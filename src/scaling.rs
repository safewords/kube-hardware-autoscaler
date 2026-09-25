//! Pure autoscaling logic: given a snapshot of a pool and the cluster's
//! pending pods, decide which machines to power on or off.
//!
//! The scheduling model is deliberately simple (resources, node selectors,
//! required node affinity and taints/tolerations) - it only needs to be good
//! enough to decide whether powering a machine on could help, and whether the
//! pods of an idle machine would fit elsewhere.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use k8s_openapi::api::core::v1::{NodeSelectorRequirement, Pod, Taint, Toleration};

use crate::crd::{NodeScalingPoolSpec, ResourceAmounts, SAFE_TO_EVICT_ANNOTATION};
use crate::resources::pod_requests;

/// Where a member currently is in its power lifecycle, from the autoscaler's
/// point of view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberState {
    /// Powered on and Ready: capacity is available.
    Online,
    /// Powering on (or asked to): capacity will soon be available.
    Booting,
    /// Powered off and not asked to power on.
    Offline,
    /// Draining or powering off (or asked to).
    Leaving,
    /// State unknown or errored; not touched by the autoscaler.
    Unavailable,
}

/// Scheduling-relevant view of a pod.
#[derive(Clone, Debug, Default)]
pub struct PodView {
    pub namespace: String,
    pub name: String,
    pub requests: ResourceAmounts,
    pub node_selector: BTreeMap<String, String>,
    /// Required node affinity: OR of terms, each an AND of requirements.
    pub affinity_terms: Vec<Vec<NodeSelectorRequirement>>,
    pub tolerations: Vec<Toleration>,
    pub created: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct Member {
    /// NodePowerManagementConfig name.
    pub name: String,
    pub state: MemberState,
    /// Whether the autoscaler may change this member's power (`powerPolicy: Auto`).
    pub auto: bool,
    pub labels: BTreeMap<String, String>,
    pub taints: Vec<Taint>,
    pub allocatable: ResourceAmounts,
    /// Sum of requests of all non-terminated pods bound to the node.
    pub requested: ResourceAmounts,
    /// The part of `requested` that counts towards utilization (pool options can
    /// leave out DaemonSet pods and pods that do not target the pool).
    pub counted: ResourceAmounts,
    /// Pods that would have to be rescheduled if this node went away.
    pub movable_pods: Vec<PodView>,
    /// Pods that block powering the node off.
    pub blocking_pods: Vec<String>,
    pub drain_failed_at: Option<DateTime<Utc>>,
}

/// A schedulable node outside every scaling pool: somewhere evicted pods can
/// go when a member powers off.
#[derive(Clone, Debug)]
pub struct ExternalNode {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub taints: Vec<Taint>,
    /// allocatable minus the requests of the pods already running there.
    pub free: ResourceAmounts,
}

fn external_key(name: &str) -> String {
    format!("external/{name}")
}

pub struct PoolSnapshot<'a> {
    pub spec: &'a NodeScalingPoolSpec,
    pub members: Vec<Member>,
    /// All unschedulable pods in the cluster.
    pub pending: Vec<PodView>,
    pub now: DateTime<Utc>,
    pub last_scale_up: Option<DateTime<Utc>>,
    pub unneeded_since: BTreeMap<String, DateTime<Utc>>,
    /// Nodes outside every pool that can absorb evicted pods.
    pub external: Vec<ExternalNode>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Plan {
    /// (member, reason)
    pub power_on: Vec<(String, String)>,
    pub power_off: Vec<(String, String)>,
    pub unneeded_since: BTreeMap<String, DateTime<Utc>>,
    /// Pending pods (past the grace period) that this pool could host.
    pub relevant_pending: u32,
    /// Members online or booting, before applying this plan.
    pub online: u32,
    pub message: String,
}

/// Taints that result from the node being powered off or cordoned by us, and
/// therefore say nothing about whether a pod could run there once it is on.
const TRANSIENT_TAINTS: &[&str] = &[
    "node.kubernetes.io/unschedulable",
    "node.kubernetes.io/not-ready",
    "node.kubernetes.io/unreachable",
    "node.kubernetes.io/network-unavailable",
    "node.cloudprovider.kubernetes.io/shutdown",
    // Set for non-graceful node shutdown handling while the machine is off.
    "node.kubernetes.io/out-of-service",
    // Cilium taints nodes until its agent is running again after boot.
    "node.cilium.io/agent-not-ready",
];

fn tolerates(tolerations: &[Toleration], taint: &Taint) -> bool {
    tolerations.iter().any(|t| {
        let effect_ok = t.effect.as_deref().is_none_or(|e| e.is_empty() || e == taint.effect);
        let key_ok = match t.key.as_deref() {
            None | Some("") => t.operator.as_deref() == Some("Exists"),
            Some(k) => k == taint.key,
        };
        let value_ok = match t.operator.as_deref() {
            Some("Exists") => true,
            _ => t.value.as_deref().unwrap_or("") == taint.value.as_deref().unwrap_or(""),
        };
        effect_ok && key_ok && value_ok
    })
}

fn requirement_matches(req: &NodeSelectorRequirement, labels: &BTreeMap<String, String>) -> bool {
    let value = labels.get(&req.key);
    let values = req.values.as_deref().unwrap_or(&[]);
    let as_int = |s: &str| s.parse::<i64>().ok();
    match req.operator.as_str() {
        "In" => value.is_some_and(|v| values.contains(v)),
        "NotIn" => value.is_none_or(|v| !values.contains(v)),
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "Gt" => {
            matches!((value.and_then(|v| as_int(v)), values.first().and_then(|v| as_int(v))), (Some(a), Some(b)) if a > b)
        }
        "Lt" => {
            matches!((value.and_then(|v| as_int(v)), values.first().and_then(|v| as_int(v))), (Some(a), Some(b)) if a < b)
        }
        _ => false,
    }
}

/// Whether `pod` could be placed on a node with these labels and taints,
/// ignoring resources.
pub fn pod_matches_node(pod: &PodView, labels: &BTreeMap<String, String>, taints: &[Taint]) -> bool {
    let selector_ok = pod.node_selector.iter().all(|(k, v)| labels.get(k) == Some(v));
    let affinity_ok = pod.affinity_terms.is_empty()
        || pod
            .affinity_terms
            .iter()
            .any(|term| term.iter().all(|r| requirement_matches(r, labels)));
    let taints_ok = taints
        .iter()
        .filter(|t| t.effect == "NoSchedule" || t.effect == "NoExecute")
        .filter(|t| !TRANSIENT_TAINTS.contains(&t.key.as_str()))
        .all(|t| tolerates(&pod.tolerations, t));
    selector_ok && affinity_ok && taints_ok
}

fn pod_fits(pod: &PodView, m: &Member, free: &ResourceAmounts) -> bool {
    free.fits(&pod.requests) && pod_matches_node(pod, &m.labels, &m.taints)
}

impl PodView {
    /// Whether the pod explicitly targets nodes carrying all of `pool`'s
    /// `matchLabels`: through its `nodeSelector`, or through required node
    /// affinity where every term requires the label (`In` with exactly that
    /// value). A pool without `matchLabels` is never explicitly selected.
    pub fn explicitly_selects(&self, pool: &crate::crd::NodeSelector) -> bool {
        if pool.match_labels.is_empty() {
            return false;
        }
        pool.match_labels.iter().all(|(key, value)| {
            self.node_selector.get(key) == Some(value)
                || (!self.affinity_terms.is_empty()
                    && self.affinity_terms.iter().all(|term| {
                        term.iter().any(|r| {
                            r.key == *key
                                && r.operator == "In"
                                && r.values
                                    .as_deref()
                                    .is_some_and(|v| !v.is_empty() && v.iter().all(|x| x == value))
                        })
                    }))
        })
    }

    pub fn from_pod(pod: &Pod) -> Self {
        let spec = pod.spec.as_ref();
        let affinity_terms = spec
            .and_then(|s| s.affinity.as_ref())
            .and_then(|a| a.node_affinity.as_ref())
            .and_then(|na| na.required_during_scheduling_ignored_during_execution.as_ref())
            .map(|sel| {
                sel.node_selector_terms
                    .iter()
                    .map(|t| t.match_expressions.clone().unwrap_or_default())
                    .collect()
            })
            .unwrap_or_default();
        PodView {
            namespace: pod.metadata.namespace.clone().unwrap_or_default(),
            name: pod.metadata.name.clone().unwrap_or_default(),
            requests: pod_requests(pod),
            node_selector: spec.and_then(|s| s.node_selector.clone()).unwrap_or_default(),
            affinity_terms,
            tolerations: spec.and_then(|s| s.tolerations.clone()).unwrap_or_default(),
            created: pod
                .metadata
                .creation_timestamp
                .as_ref()
                .and_then(|t| DateTime::from_timestamp(t.0.as_second(), 0)),
        }
    }
}

/// Whether a pod is waiting because no node can host it.
pub fn is_unschedulable(pod: &Pod) -> bool {
    let bound = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_some();
    let gated = pod
        .spec
        .as_ref()
        .and_then(|s| s.scheduling_gates.as_ref())
        .is_some_and(|g| !g.is_empty());
    let pending = pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Pending");
    let unschedulable = pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter().any(|c| {
                c.type_ == "PodScheduled" && c.status == "False" && c.reason.as_deref() == Some("Unschedulable")
            })
        });
    !bound && !gated && pending && unschedulable && pod.metadata.deletion_timestamp.is_none()
}

/// Whether a pod is still consuming node resources.
pub fn is_active(pod: &Pod) -> bool {
    !matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some("Succeeded") | Some("Failed")
    )
}

pub fn is_daemonset_pod(pod: &Pod) -> bool {
    pod.metadata
        .owner_references
        .iter()
        .flatten()
        .any(|o| o.kind == "DaemonSet")
}

pub fn is_mirror_pod(pod: &Pod) -> bool {
    pod.metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key("kubernetes.io/config.mirror"))
}

/// Classification of a pod for draining purposes.
#[derive(Debug, PartialEq, Eq)]
pub enum DrainClass {
    /// Stays on the node (daemonset/mirror/terminated); does not block power off.
    Ignore,
    /// Must be evicted before power off.
    Evict,
    /// Blocks power off.
    Block(&'static str),
}

pub fn drain_class(pod: &Pod) -> DrainClass {
    if !is_active(pod) {
        return DrainClass::Ignore;
    }
    let annotation = |k: &str| {
        pod.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(k))
            .map(String::as_str)
    };
    let safe = annotation(SAFE_TO_EVICT_ANNOTATION).or(annotation("cluster-autoscaler.kubernetes.io/safe-to-evict"));
    // DaemonSet and mirror pods are not evicted, but an explicit
    // safe-to-evict=false still keeps their node powered on (e.g. a
    // DaemonSet worker in the middle of a job).
    if is_daemonset_pod(pod) || is_mirror_pod(pod) {
        return match safe {
            Some("false") => DrainClass::Block("annotated safe-to-evict=false"),
            _ => DrainClass::Ignore,
        };
    }
    match safe {
        Some("false") => DrainClass::Block("annotated safe-to-evict=false"),
        Some("true") => DrainClass::Evict,
        _ if pod.metadata.owner_references.as_ref().is_none_or(|o| o.is_empty()) => {
            DrainClass::Block("not managed by a controller")
        }
        _ => DrainClass::Evict,
    }
}

/// Computes the scaling plan for one pool.
pub fn plan(s: &PoolSnapshot) -> Plan {
    let mut plan = Plan::default();
    let spec = s.spec;
    let max_online = spec.max_online.map(|m| m as usize).unwrap_or(usize::MAX);

    let online_now = s
        .members
        .iter()
        .filter(|m| matches!(m.state, MemberState::Online | MemberState::Booting))
        .count();
    plan.online = online_now as u32;

    // --- scale up ----------------------------------------------------------
    let grace = Duration::seconds(spec.scale_up.pending_pod_grace_seconds as i64);
    let mut pending: Vec<&PodView> = s
        .pending
        .iter()
        .filter(|p| p.created.is_none_or(|c| s.now - c >= grace))
        .filter(|p| !spec.scale_up.require_explicit_selection || p.explicitly_selects(&spec.node_selector))
        .filter(|p| {
            s.members.iter().any(|m| {
                m.allocatable.fits(&p.requests)
                    && pod_matches_node(p, &m.labels, &m.taints)
                    && (matches!(m.state, MemberState::Booting) || (m.auto && m.state == MemberState::Offline))
            })
        })
        .collect();
    plan.relevant_pending = pending.len() as u32;
    // First-fit decreasing by CPU, then memory.
    pending.sort_by_key(|p| std::cmp::Reverse((p.requests.cpu_millis, p.requests.memory_bytes)));

    // Bins: booting members first (capacity already on its way), then members we open.
    let mut bins: Vec<(&Member, ResourceAmounts)> = s
        .members
        .iter()
        .filter(|m| m.state == MemberState::Booting)
        .map(|m| (m, m.allocatable.minus(&m.requested)))
        .collect();
    let mut offline: Vec<&Member> = s
        .members
        .iter()
        .filter(|m| m.auto && m.state == MemberState::Offline)
        .collect();
    // Prefer the largest machines so fewer are needed.
    offline.sort_by_key(|m| std::cmp::Reverse((m.allocatable.cpu_millis, m.allocatable.memory_bytes)));

    let mut opened: Vec<(String, String)> = Vec::new();
    let can_open = |opened: &Vec<(String, String)>| {
        online_now + opened.len() < max_online && (opened.len() as u32) < spec.scale_up.max_nodes_per_step.max(1)
    };
    if spec.scale_up.enabled {
        for pod in &pending {
            if let Some((_, free)) = bins.iter_mut().find(|(m, free)| pod_fits(pod, m, free)) {
                *free = free.minus(&pod.requests);
                continue;
            }
            if !can_open(&opened) {
                continue;
            }
            if let Some(pos) = offline.iter().position(|m| pod_fits(pod, m, &m.allocatable)) {
                let m = offline.remove(pos);
                opened.push((
                    m.name.clone(),
                    format!("unschedulable pod {}/{}", pod.namespace, pod.name),
                ));
                bins.push((m, m.allocatable.minus(&pod.requests)));
            }
        }
    }
    // minOnline is enforced even when scale-up is disabled.
    while online_now + opened.len() < spec.min_online as usize && online_now + opened.len() < max_online {
        let Some(m) = offline.first().copied() else { break };
        offline.remove(0);
        opened.push((m.name.clone(), format!("pool below minOnline ({})", spec.min_online)));
    }
    plan.power_on = opened;

    // --- scale down --------------------------------------------------------
    let candidates: Vec<&Member> = s
        .members
        .iter()
        .filter(|m| m.auto && m.state == MemberState::Online)
        .collect();
    let threshold = spec.scale_down.utilization_threshold_percent as f64;
    let backoff = Duration::seconds(spec.scale_down.drain_failure_backoff_seconds as i64);
    let is_unneeded = |m: &Member| {
        m.allocatable.utilization_percent(&m.counted) < threshold
            && m.blocking_pods.is_empty()
            && m.drain_failed_at.is_none_or(|t| s.now - t >= backoff)
    };
    for m in &candidates {
        if is_unneeded(m) {
            let since = s.unneeded_since.get(&m.name).copied().unwrap_or(s.now);
            plan.unneeded_since.insert(m.name.clone(), since);
        }
    }

    let over_max = online_now.saturating_sub(max_online);
    let leaving = s.members.iter().filter(|m| m.state == MemberState::Leaving).count();
    let recently_scaled_up = s
        .last_scale_up
        .is_some_and(|t| s.now - t < Duration::seconds(spec.scale_down.delay_after_scale_up_seconds as i64));
    let booting = s.members.iter().any(|m| m.state == MemberState::Booting);

    let blocked_reason = if !plan.power_on.is_empty() {
        Some("scaling up")
    } else if over_max > 0 {
        None
    } else if !spec.scale_down.enabled {
        Some("scale down disabled")
    } else if plan.relevant_pending > 0 {
        Some("pods pending")
    } else if booting {
        Some("nodes booting")
    } else if recently_scaled_up {
        Some("recently scaled up")
    } else {
        None
    };

    let step = spec.scale_down.max_nodes_per_step.max(1) as usize;
    if blocked_reason.is_none() && (leaving < step || over_max > 0) {
        let unneeded_for = Duration::seconds(spec.scale_down.unneeded_seconds as i64);
        let util = |m: &Member| m.allocatable.utilization_percent(&m.counted);
        let mut eligible: Vec<&Member> = candidates
            .iter()
            .copied()
            .filter(|m| {
                let expired = plan
                    .unneeded_since
                    .get(&m.name)
                    .is_some_and(|t| s.now - *t >= unneeded_for);
                (over_max > 0 && m.blocking_pods.is_empty()) || expired
            })
            .collect();
        eligible.sort_by(|a, b| util(a).total_cmp(&util(b)));

        let mut removed: BTreeSet<String> = BTreeSet::new();
        // Where evicted pods could go: online pool members, plus schedulable nodes
        // outside any scaling pool (e.g. an always-on base) that never power off.
        let mut free: BTreeMap<String, ResourceAmounts> = s
            .members
            .iter()
            .filter(|m| m.state == MemberState::Online)
            .map(|m| (m.name.clone(), m.allocatable.minus(&m.requested)))
            .collect();
        for e in &s.external {
            free.insert(external_key(&e.name), e.free);
        }
        let budget = if over_max > 0 {
            over_max.max(step.saturating_sub(leaving))
        } else {
            step - leaving
        };

        for m in eligible {
            if removed.len() >= budget || online_now - removed.len() <= spec.min_online as usize {
                break;
            }
            // Simulate moving this node's pods to the remaining online nodes.
            let mut trial = free.clone();
            trial.remove(&m.name);
            let mut targets: Vec<(String, &BTreeMap<String, String>, &[Taint])> = s
                .members
                .iter()
                .filter(|o| o.state == MemberState::Online && o.name != m.name && !removed.contains(&o.name))
                .map(|o| (o.name.clone(), &o.labels, o.taints.as_slice()))
                .collect();
            targets.extend(
                s.external
                    .iter()
                    .map(|e| (external_key(&e.name), &e.labels, e.taints.as_slice())),
            );
            let mut pods: Vec<&PodView> = m.movable_pods.iter().collect();
            pods.sort_by_key(|p| std::cmp::Reverse((p.requests.cpu_millis, p.requests.memory_bytes)));
            let all_fit = pods.iter().all(|p| {
                let slot = targets
                    .iter()
                    .find(|(key, labels, taints)| trial[key].fits(&p.requests) && pod_matches_node(p, labels, taints))
                    .map(|(key, _, _)| key.clone());
                match slot {
                    Some(key) => {
                        let f = trial.get_mut(&key).unwrap();
                        *f = f.minus(&p.requests);
                        true
                    }
                    None => false,
                }
            });
            if all_fit {
                free = trial;
                removed.insert(m.name.clone());
                let reason = if over_max > 0 {
                    format!("pool above maxOnline ({max_online})")
                } else {
                    format!(
                        "utilization {:.0}% below {:.0}% for {}s",
                        util(m),
                        threshold,
                        unneeded_for.num_seconds()
                    )
                };
                plan.power_off.push((m.name.clone(), reason));
            }
        }
    }

    plan.message = match (plan.power_on.len(), plan.power_off.len(), blocked_reason) {
        (0, 0, Some(r)) if r != "scaling up" && !plan.unneeded_since.is_empty() => {
            format!("steady ({} unneeded; scale down held: {r})", plan.unneeded_since.len())
        }
        (0, 0, _) => "steady".to_string(),
        (on, off, _) => format!("powering on {on}, powering off {off}"),
    };
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{ScaleDownSpec, ScaleUpSpec};

    const GI: i64 = 1 << 30;

    fn spec() -> NodeScalingPoolSpec {
        NodeScalingPoolSpec {
            node_selector: Default::default(),
            min_online: 1,
            max_online: Some(3),
            scale_up: ScaleUpSpec {
                enabled: true,
                pending_pod_grace_seconds: 30,
                max_nodes_per_step: 3,
                require_explicit_selection: false,
            },
            scale_down: ScaleDownSpec {
                enabled: true,
                utilization_threshold_percent: 50,
                unneeded_seconds: 600,
                delay_after_scale_up_seconds: 600,
                drain_failure_backoff_seconds: 1800,
                max_nodes_per_step: 1,
                ignore_daemon_set_utilization: false,
                ignore_non_selecting_pod_utilization: false,
            },
        }
    }

    fn res(cpu: i64, mem_gi: i64) -> ResourceAmounts {
        ResourceAmounts {
            cpu_millis: cpu,
            memory_bytes: mem_gi * GI,
            pods: 1,
        }
    }

    fn member(name: &str, state: MemberState, used_cpu: i64) -> Member {
        Member {
            name: name.into(),
            state,
            auto: true,
            labels: BTreeMap::from([("kubernetes.io/hostname".into(), name.into())]),
            taints: vec![],
            allocatable: ResourceAmounts {
                cpu_millis: 4000,
                memory_bytes: 16 * GI,
                pods: 110,
            },
            requested: ResourceAmounts {
                cpu_millis: used_cpu,
                memory_bytes: 0,
                pods: 0,
            },
            counted: ResourceAmounts {
                cpu_millis: used_cpu,
                memory_bytes: 0,
                pods: 0,
            },
            movable_pods: vec![],
            blocking_pods: vec![],
            drain_failed_at: None,
        }
    }

    fn pod(name: &str, cpu: i64, age_s: i64, now: DateTime<Utc>) -> PodView {
        PodView {
            namespace: "default".into(),
            name: name.into(),
            requests: res(cpu, 1),
            created: Some(now - Duration::seconds(age_s)),
            ..Default::default()
        }
    }

    fn snapshot(
        spec: &NodeScalingPoolSpec,
        members: Vec<Member>,
        pending: Vec<PodView>,
        now: DateTime<Utc>,
    ) -> PoolSnapshot<'_> {
        PoolSnapshot {
            spec,
            members,
            pending,
            now,
            last_scale_up: None,
            unneeded_since: BTreeMap::new(),
            external: vec![],
        }
    }

    #[test]
    fn powers_on_for_unschedulable_pods() {
        let now = Utc::now();
        let spec = spec();
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Offline, 0),
                member("c", MemberState::Offline, 0),
            ],
            vec![pod("p1", 1000, 60, now), pod("p2", 1000, 60, now)],
            now,
        );
        let p = plan(&s);
        // Both pods fit on one machine.
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.relevant_pending, 2);
        assert!(p.power_off.is_empty());
    }

    #[test]
    fn ignores_young_pending_pods() {
        let now = Utc::now();
        let spec = spec();
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Offline, 0),
            ],
            vec![pod("p1", 1000, 5, now)],
            now,
        );
        assert!(plan(&s).power_on.is_empty());
    }

    #[test]
    fn booting_capacity_absorbs_pending_pods() {
        let now = Utc::now();
        let spec = spec();
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Booting, 0),
                member("c", MemberState::Offline, 0),
            ],
            vec![pod("p1", 1000, 60, now)],
            now,
        );
        assert!(plan(&s).power_on.is_empty());
    }

    #[test]
    fn respects_max_online_and_selectors() {
        let now = Utc::now();
        let spec = spec();
        let mut gpu = member("gpu", MemberState::Offline, 0);
        gpu.labels.insert("gpu".into(), "true".into());
        let mut p = pod("needs-gpu", 1000, 60, now);
        p.node_selector.insert("gpu".into(), "true".into());
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Offline, 0),
                gpu,
            ],
            vec![p],
            now,
        );
        assert_eq!(
            plan(&s).power_on,
            vec![("gpu".to_string(), "unschedulable pod default/needs-gpu".to_string())]
        );

        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Online, 3900),
                member("c", MemberState::Online, 3900),
                member("d", MemberState::Offline, 0),
            ],
            vec![pod("p", 1000, 60, now)],
            now,
        );
        assert!(plan(&s).power_on.is_empty(), "max_online reached");
    }

    #[test]
    fn taints_must_be_tolerated_except_transient_ones() {
        let now = Utc::now();
        let mut m = member("a", MemberState::Offline, 0);
        m.taints = vec![Taint {
            key: "node.kubernetes.io/unreachable".into(),
            effect: "NoSchedule".into(),
            ..Default::default()
        }];
        let p = pod("p", 100, 60, now);
        assert!(pod_matches_node(&p, &m.labels, &m.taints));
        m.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("db".into()),
            effect: "NoSchedule".into(),
            ..Default::default()
        });
        assert!(!pod_matches_node(&p, &m.labels, &m.taints));
        let mut tolerant = p.clone();
        tolerant.tolerations = vec![Toleration {
            key: Some("dedicated".into()),
            operator: Some("Equal".into()),
            value: Some("db".into()),
            effect: Some("NoSchedule".into()),
            ..Default::default()
        }];
        assert!(pod_matches_node(&tolerant, &m.labels, &m.taints));
    }

    #[test]
    fn enforces_min_online() {
        let now = Utc::now();
        let mut spec = spec();
        spec.min_online = 2;
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 100),
                member("b", MemberState::Offline, 0),
            ],
            vec![],
            now,
        );
        let p = plan(&s);
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.power_on[0].0, "b");
    }

    #[test]
    fn scales_down_idle_node_after_unneeded_period() {
        let now = Utc::now();
        let spec = spec();
        let mut idle = member("b", MemberState::Online, 200);
        idle.movable_pods = vec![pod("web", 200, 3600, now)];
        let members = vec![member("a", MemberState::Online, 1000), idle];

        // First observation: starts the unneeded timer, no action yet.
        let s = snapshot(&spec, members.clone(), vec![], now);
        let p = plan(&s);
        assert!(p.power_off.is_empty());
        assert!(p.unneeded_since.contains_key("b"));

        // After the unneeded period the least utilized node goes.
        let mut s = snapshot(&spec, members, vec![], now);
        s.unneeded_since = BTreeMap::from([
            ("a".to_string(), now - Duration::seconds(700)),
            ("b".to_string(), now - Duration::seconds(700)),
        ]);
        let p = plan(&s);
        assert_eq!(p.power_off.len(), 1, "min_online=1 keeps one node");
        assert_eq!(p.power_off[0].0, "b");
    }

    #[test]
    fn does_not_scale_down_when_pods_would_not_fit() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Online, 1500);
        b.movable_pods = vec![pod("big", 1500, 3600, now)];
        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(&spec, vec![member("a", MemberState::Online, 3000), b], vec![], now)
        };
        assert!(plan(&s).power_off.is_empty());
    }

    #[test]
    fn blocking_pods_and_pending_pods_prevent_scale_down() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Online, 0);
        b.blocking_pods = vec!["default/bare".into()];
        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(&spec, vec![member("a", MemberState::Online, 0), b], vec![], now)
        };
        assert!(plan(&s).power_off.is_empty());

        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(
                &spec,
                vec![
                    member("a", MemberState::Online, 3900),
                    member("b", MemberState::Online, 3900),
                    member("c", MemberState::Offline, 0),
                ],
                vec![pod("p", 1000, 60, now)],
                now,
            )
        };
        let p = plan(&s);
        assert_eq!(p.power_on.len(), 1);
        assert!(p.power_off.is_empty());
    }

    #[test]
    fn recent_scale_up_delays_scale_down() {
        let now = Utc::now();
        let spec = spec();
        let s = PoolSnapshot {
            last_scale_up: Some(now - Duration::seconds(60)),
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(
                &spec,
                vec![member("a", MemberState::Online, 0), member("b", MemberState::Online, 0)],
                vec![],
                now,
            )
        };
        assert!(plan(&s).power_off.is_empty());
    }

    #[test]
    fn non_auto_members_are_never_touched() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Offline, 0);
        b.auto = false;
        let s = snapshot(
            &spec,
            vec![member("a", MemberState::Online, 3900), b],
            vec![pod("p", 1000, 60, now)],
            now,
        );
        let p = plan(&s);
        assert!(p.power_on.is_empty());
        assert_eq!(p.relevant_pending, 0);
    }

    #[test]
    fn powered_off_node_taints_do_not_block_scale_up() {
        // Taints seen on a powered-off k3s + Cilium node.
        let now = Utc::now();
        let mut m = member("gpu-1", MemberState::Offline, 0);
        m.taints = [
            ("node.kubernetes.io/out-of-service", "NoExecute"),
            ("node.kubernetes.io/unreachable", "NoSchedule"),
            ("node.cilium.io/agent-not-ready", "NoSchedule"),
            ("node.kubernetes.io/unreachable", "NoExecute"),
        ]
        .iter()
        .map(|(k, e)| Taint {
            key: k.to_string(),
            effect: e.to_string(),
            ..Default::default()
        })
        .collect();
        assert!(pod_matches_node(&pod("p", 100, 60, now), &m.labels, &m.taints));
    }

    #[test]
    fn daemonset_pods_can_block_scale_down() {
        let mk = |v: serde_json::Value| -> Pod { serde_json::from_value(v).unwrap() };
        let ds = serde_json::json!([{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "ds", "uid": "2"}]);
        let busy = mk(
            serde_json::json!({"metadata": {"name": "gpu-worker", "ownerReferences": ds,
            "annotations": {SAFE_TO_EVICT_ANNOTATION: "false"}}}),
        );
        assert!(matches!(drain_class(&busy), DrainClass::Block(_)));
        let idle = mk(
            serde_json::json!({"metadata": {"name": "gpu-worker", "ownerReferences": ds,
            "annotations": {SAFE_TO_EVICT_ANNOTATION: "true"}}}),
        );
        assert_eq!(drain_class(&idle), DrainClass::Ignore);
    }

    #[test]
    fn classifies_pods_for_drain() {
        let mk = |v: serde_json::Value| -> Pod { serde_json::from_value(v).unwrap() };
        let owned = serde_json::json!([{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs", "uid": "1"}]);
        let ds = serde_json::json!([{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "ds", "uid": "2"}]);
        assert_eq!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a", "ownerReferences": owned}})
            )),
            DrainClass::Evict
        );
        assert_eq!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a", "ownerReferences": ds}})
            )),
            DrainClass::Ignore
        );
        assert!(matches!(
            drain_class(&mk(serde_json::json!({"metadata": {"name": "a"}}))),
            DrainClass::Block(_)
        ));
        assert_eq!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a", "annotations": {SAFE_TO_EVICT_ANNOTATION: "true"}}})
            )),
            DrainClass::Evict
        );
        assert!(matches!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a", "ownerReferences": owned,
                "annotations": {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}}})
            )),
            DrainClass::Block(_)
        ));
        assert_eq!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a"}, "status": {"phase": "Succeeded"}})
            )),
            DrainClass::Ignore
        );
    }
}

#[cfg(test)]
mod dedicated_pool_tests {
    //! A single-machine GPU pool that only GPU work may wake, with guest pods
    //! and DaemonSets that must not keep it on (the "standby GPU" setup).
    use super::*;
    use crate::crd::{NodeSelector, ScaleDownSpec, ScaleUpSpec};

    const GI: i64 = 1 << 30;

    fn gpu_spec() -> NodeScalingPoolSpec {
        NodeScalingPoolSpec {
            node_selector: NodeSelector {
                match_labels: BTreeMap::from([("example.com/gpu".to_string(), "true".to_string())]),
                match_expressions: vec![],
            },
            min_online: 0,
            max_online: Some(1),
            scale_up: ScaleUpSpec {
                enabled: true,
                pending_pod_grace_seconds: 15,
                max_nodes_per_step: 1,
                require_explicit_selection: true,
            },
            scale_down: ScaleDownSpec {
                enabled: true,
                utilization_threshold_percent: 10,
                unneeded_seconds: 900,
                delay_after_scale_up_seconds: 900,
                drain_failure_backoff_seconds: 1800,
                max_nodes_per_step: 1,
                ignore_daemon_set_utilization: true,
                ignore_non_selecting_pod_utilization: true,
            },
        }
    }

    fn cpu(millis: i64) -> ResourceAmounts {
        ResourceAmounts {
            cpu_millis: millis,
            memory_bytes: GI,
            pods: 1,
        }
    }

    fn gpu_box(state: MemberState) -> Member {
        Member {
            name: "gpu-1".into(),
            state,
            auto: true,
            labels: BTreeMap::from([("example.com/gpu".to_string(), "true".to_string())]),
            taints: vec![],
            allocatable: ResourceAmounts {
                cpu_millis: 12000,
                memory_bytes: 64 * GI,
                pods: 110,
            },
            requested: ResourceAmounts::default(),
            counted: ResourceAmounts::default(),
            movable_pods: vec![],
            blocking_pods: vec![],
            drain_failed_at: None,
        }
    }

    fn pod(name: &str, selector: &[(&str, &str)], now: DateTime<Utc>) -> PodView {
        PodView {
            namespace: "default".into(),
            name: name.into(),
            requests: cpu(1000),
            node_selector: selector.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            created: Some(now - Duration::seconds(60)),
            ..Default::default()
        }
    }

    fn base(free_cpu: i64) -> ExternalNode {
        ExternalNode {
            name: "always-on-1".into(),
            labels: BTreeMap::new(),
            taints: vec![],
            free: ResourceAmounts {
                cpu_millis: free_cpu,
                memory_bytes: 32 * GI,
                pods: 100,
            },
        }
    }

    fn snap<'a>(
        spec: &'a NodeScalingPoolSpec,
        m: Member,
        pending: Vec<PodView>,
        now: DateTime<Utc>,
    ) -> PoolSnapshot<'a> {
        PoolSnapshot {
            spec,
            members: vec![m],
            pending,
            now,
            last_scale_up: None,
            unneeded_since: BTreeMap::new(),
            external: vec![],
        }
    }

    #[test]
    fn ci_pods_do_not_wake_the_pool_but_gpu_jobs_do() {
        let now = Utc::now();
        let spec = gpu_spec();
        let ci = pod("runner", &[], now);
        let p = plan(&snap(&spec, gpu_box(MemberState::Offline), vec![ci], now));
        assert!(
            p.power_on.is_empty(),
            "an untargeted pod must not power on a dedicated pool"
        );
        assert_eq!(p.relevant_pending, 0);

        let job = pod("transcode", &[("example.com/gpu", "true")], now);
        let p = plan(&snap(&spec, gpu_box(MemberState::Offline), vec![job], now));
        assert_eq!(p.power_on.len(), 1);
    }

    #[test]
    fn required_affinity_counts_as_explicit_selection() {
        let now = Utc::now();
        let spec = gpu_spec();
        let mut job = pod("transcode", &[], now);
        job.affinity_terms = vec![vec![NodeSelectorRequirement {
            key: "example.com/gpu".into(),
            operator: "In".into(),
            values: Some(vec!["true".into()]),
        }]];
        assert!(job.explicitly_selects(&spec.node_selector));
        // "In [true, false]" also allows non-GPU nodes: not explicit.
        job.affinity_terms[0][0].values = Some(vec!["true".into(), "false".into()]);
        assert!(!job.explicitly_selects(&spec.node_selector));
    }

    #[test]
    fn guests_move_to_the_always_on_base_so_the_pool_can_power_off() {
        let now = Utc::now();
        let spec = gpu_spec();
        let mut m = gpu_box(MemberState::Online);
        // A CI runner landed on the GPU box: it is requested load, but not counted.
        m.requested = cpu(2000);
        m.counted = ResourceAmounts::default();
        m.movable_pods = vec![pod("runner", &[], now)];
        let mut s = snap(&spec, m, vec![], now);
        s.unneeded_since = BTreeMap::from([("gpu-1".to_string(), now - Duration::seconds(1000))]);

        // Without anywhere to move the runner, the single-machine pool is stuck on.
        assert!(plan(&s).power_off.is_empty());
        // With room on the always-on base, it powers off.
        s.external = vec![base(4000)];
        assert_eq!(plan(&s).power_off.len(), 1);
        // But not when the base is full.
        s.external = vec![base(500)];
        assert!(plan(&s).power_off.is_empty());
    }

    #[test]
    fn running_gpu_job_blocks_power_off() {
        let now = Utc::now();
        let spec = gpu_spec();
        let mut m = gpu_box(MemberState::Online);
        m.blocking_pods = vec!["default/transcode (annotated safe-to-evict=false)".into()];
        let mut s = snap(&spec, m, vec![], now);
        s.unneeded_since = BTreeMap::from([("gpu-1".to_string(), now - Duration::seconds(1000))]);
        s.external = vec![base(4000)];
        let p = plan(&s);
        assert!(p.power_off.is_empty());
        assert!(
            !p.unneeded_since.contains_key("gpu-1"),
            "a node with a running job is not even unneeded"
        );
    }
}
