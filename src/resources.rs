//! Resource quantity parsing and pod/node resource accounting.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Node, Pod};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use crate::crd::ResourceAmounts;

/// Parses a Kubernetes quantity into a value scaled by `scale`
/// (use 1000 for CPU millicores, 1 for bytes). Returns `None` for garbage.
pub fn parse_quantity(q: &str, scale: f64) -> Option<i64> {
    let q = q.trim();
    if q.is_empty() {
        return None;
    }
    const SUFFIXES: &[(&str, f64)] = &[
        ("Ki", 1024.0),
        ("Mi", 1048576.0),
        ("Gi", 1073741824.0),
        ("Ti", 1099511627776.0),
        ("Pi", 1125899906842624.0),
        ("Ei", 1152921504606846976.0),
        ("n", 1e-9),
        ("u", 1e-6),
        ("m", 1e-3),
        ("k", 1e3),
        ("M", 1e6),
        ("G", 1e9),
        ("T", 1e12),
        ("P", 1e15),
        ("E", 1e18),
    ];
    let (number, multiplier) = SUFFIXES
        .iter()
        .find_map(|(s, m)| q.strip_suffix(s).map(|n| (n, *m)))
        .unwrap_or((q, 1.0));
    // Decimal exponent form such as "1e3" is handled by f64 parsing.
    let value: f64 = number.parse().ok()?;
    Some((value * multiplier * scale).ceil() as i64)
}

fn get(map: &BTreeMap<String, Quantity>, key: &str, scale: f64) -> i64 {
    map.get(key).and_then(|q| parse_quantity(&q.0, scale)).unwrap_or(0)
}

fn amounts(map: &BTreeMap<String, Quantity>) -> ResourceAmounts {
    ResourceAmounts {
        cpu_millis: get(map, "cpu", 1000.0),
        memory_bytes: get(map, "memory", 1.0),
        pods: get(map, "pods", 1.0),
    }
}

/// Allocatable resources of a node (falls back to capacity).
pub fn node_allocatable(node: &Node) -> ResourceAmounts {
    let status = node.status.as_ref();
    status
        .and_then(|s| s.allocatable.as_ref().or(s.capacity.as_ref()))
        .map(amounts)
        .unwrap_or_default()
}

/// Effective resource request of a pod as seen by the scheduler:
/// max(sum(containers), max(init containers)) + overhead. Counts as one pod.
pub fn pod_requests(pod: &Pod) -> ResourceAmounts {
    let Some(spec) = pod.spec.as_ref() else {
        return ResourceAmounts {
            pods: 1,
            ..Default::default()
        };
    };
    let req = |c: &k8s_openapi::api::core::v1::Container| {
        c.resources
            .as_ref()
            .and_then(|r| r.requests.as_ref())
            .map(amounts)
            .unwrap_or_default()
    };
    let mut sum = ResourceAmounts::default();
    for c in &spec.containers {
        let r = req(c);
        sum.cpu_millis += r.cpu_millis;
        sum.memory_bytes += r.memory_bytes;
    }
    for c in spec.init_containers.iter().flatten() {
        let r = req(c);
        sum.cpu_millis = sum.cpu_millis.max(r.cpu_millis);
        sum.memory_bytes = sum.memory_bytes.max(r.memory_bytes);
    }
    if let Some(overhead) = spec.overhead.as_ref() {
        let o = amounts(overhead);
        sum.cpu_millis += o.cpu_millis;
        sum.memory_bytes += o.memory_bytes;
    }
    sum.pods = 1;
    sum
}

impl ResourceAmounts {
    pub fn add(&mut self, o: &ResourceAmounts) {
        self.cpu_millis += o.cpu_millis;
        self.memory_bytes += o.memory_bytes;
        self.pods += o.pods;
    }

    /// Whether `req` fits in `self` (treated as free capacity).
    pub fn fits(&self, req: &ResourceAmounts) -> bool {
        req.cpu_millis <= self.cpu_millis && req.memory_bytes <= self.memory_bytes && req.pods <= self.pods
    }

    pub fn minus(&self, o: &ResourceAmounts) -> ResourceAmounts {
        ResourceAmounts {
            cpu_millis: self.cpu_millis - o.cpu_millis,
            memory_bytes: self.memory_bytes - o.memory_bytes,
            pods: self.pods - o.pods,
        }
    }

    /// max(cpu, memory) utilization of `used` against `self`, as a percentage.
    pub fn utilization_percent(&self, used: &ResourceAmounts) -> f64 {
        let ratio = |u: i64, t: i64| if t > 0 { u as f64 / t as f64 } else { 0.0 };
        100.0 * ratio(used.cpu_millis, self.cpu_millis).max(ratio(used.memory_bytes, self.memory_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_quantities() {
        assert_eq!(parse_quantity("100m", 1000.0), Some(100));
        assert_eq!(parse_quantity("2", 1000.0), Some(2000));
        assert_eq!(parse_quantity("1.5", 1000.0), Some(1500));
        assert_eq!(parse_quantity("128Mi", 1.0), Some(134217728));
        assert_eq!(parse_quantity("1G", 1.0), Some(1_000_000_000));
        assert_eq!(parse_quantity("1e3", 1.0), Some(1000));
        assert_eq!(parse_quantity("250000n", 1000.0), Some(1));
        assert_eq!(parse_quantity("16Gi", 1.0), Some(17179869184));
        assert_eq!(parse_quantity("110", 1.0), Some(110));
        assert_eq!(parse_quantity("abc", 1.0), None);
    }

    #[test]
    fn pod_requests_use_init_container_max() {
        let pod: Pod = serde_json::from_value(serde_json::json!({
            "metadata": {"name": "p"},
            "spec": {
                "containers": [
                    {"name": "a", "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}}},
                    {"name": "b", "resources": {"requests": {"cpu": "200m"}}}
                ],
                "initContainers": [
                    {"name": "i", "resources": {"requests": {"cpu": "1", "memory": "32Mi"}}}
                ]
            }
        }))
        .unwrap();
        let r = pod_requests(&pod);
        assert_eq!(r.cpu_millis, 1000);
        assert_eq!(r.memory_bytes, 64 * 1024 * 1024);
        assert_eq!(r.pods, 1);
    }
}
