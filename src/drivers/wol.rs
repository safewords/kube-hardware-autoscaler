//! Wake-on-LAN driver.
//!
//! * power on: a UDP magic packet is broadcast to the machine's NIC, from the
//!   operator's own node and/or from **relay pods on neighbouring nodes**.
//!   Relays let machines on other network segments or sites (linked by a VPN,
//!   for example) be woken by nodes that share their broadcast domain;
//! * power off: a privileged pod is scheduled onto the node that runs
//!   `systemctl poweroff` in the host namespaces;
//! * power state: inferred from the Node's `Ready` condition (put a `ping`
//!   interface in front for an accurate reading).
//!
//! The shutdown pod carries the node's boot id and refuses to act if the host
//! has rebooted since, so a stale pod can never shut down a freshly booted host.

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{DeleteParams, ListParams, PostParams};
use kube::{Api, Client, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{DriverError, DriverInit, DriverKind, PowerDriver, Result};
use crate::crd::{POWERED_OFF_ANNOTATION, PowerState};
use crate::wake;

const RELAY_COMPONENT: &str = "wake-relay";
const TARGET_LABEL: &str = "hardware-autoscaler.safewords.com/target";

/// `config` of the `wakeOnLan` driver. No credentials are needed.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WakeOnLanConfig {
    /// MAC address of the NIC to wake, e.g. `aa:bb:cc:dd:ee:ff`.
    pub mac_address: String,
    /// Broadcast address the magic packet is sent to. When omitted, every
    /// sender uses the broadcast address of each of its interfaces plus
    /// `255.255.255.255`, which works across sites with different subnets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broadcast_address: Option<String>,
    /// UDP port. Defaults to 9.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Also send from the operator's own node (it must share a broadcast
    /// domain with the machine, typically via `hostNetwork: true`). Defaults
    /// to true; set false when only relays can reach the machine.
    #[serde(default = "default_true")]
    pub send_from_operator: bool,
    /// Send from neighbouring nodes as well, via short-lived relay pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay: Option<RelayConfig>,
    /// Image used for the shutdown pod; must provide `sh` and `nsenter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown_image: Option<String>,
}

/// Which nodes relay the magic packet. Candidates are Ready nodes other than
/// the target that the operator has not powered off.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RelayConfig {
    /// Only nodes whose value for this label equals the target Node's value,
    /// e.g. `topology.kubernetes.io/zone` to use nodes at the same site. When
    /// omitted, any node may relay (a fan-out across all segments).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub same_topology_as: Option<String>,
    /// Only nodes carrying all of these labels.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
    /// At most this many relays per wake. Defaults to 3.
    #[serde(default = "default_max_relays")]
    pub max_nodes: u32,
}

fn default_port() -> u16 {
    9
}
fn default_true() -> bool {
    true
}
fn default_max_relays() -> u32 {
    3
}

pub struct WakeOnLanDriver {
    config: WakeOnLanConfig,
    mac: [u8; 6],
    node_name: String,
    client: Client,
    namespace: String,
    shutdown_image: String,
    relay_image: String,
}

impl DriverKind for WakeOnLanDriver {
    const NAME: &'static str = "wakeOnLan";
    const DESCRIPTION: &'static str = "Wake-on-LAN magic packet (from the operator and/or neighbour relays) to power on; in-band shutdown to power off";
    const REQUIRES_CREDENTIALS: bool = false;
    type Config = WakeOnLanConfig;

    fn build(config: WakeOnLanConfig, init: &DriverInit<'_>) -> Result<Self> {
        let mac = wake::parse_mac(&config.mac_address)
            .ok_or_else(|| DriverError::Config(format!("invalid MAC address {:?}", config.mac_address)))?;
        if let Some(b) = &config.broadcast_address
            && b.parse::<std::net::Ipv4Addr>().is_err()
        {
            return Err(DriverError::Config(format!("invalid broadcastAddress {b:?}")));
        }
        if !config.send_from_operator && config.relay.is_none() {
            return Err(DriverError::Config(
                "sendFromOperator is false and no relay is configured".into(),
            ));
        }
        let shutdown_image = config
            .shutdown_image
            .clone()
            .unwrap_or_else(|| init.ctx.shutdown_image.clone());
        Ok(Self {
            config,
            mac,
            node_name: init.node_name.to_string(),
            client: init.ctx.client.clone(),
            namespace: init.ctx.namespace.clone(),
            shutdown_image,
            relay_image: init.ctx.relay_image.clone(),
        })
    }
}

fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.type_ == "Ready"))
        .is_some_and(|c| c.status == "True")
}

/// Picks the nodes that relay a wake for `target`: Ready, not the target,
/// not powered off by us, matching the selector and (optionally) sharing the
/// target's topology label. Sorted by name for stable choices.
pub(crate) fn pick_relays(
    target: &str,
    target_labels: &BTreeMap<String, String>,
    nodes: &[Node],
    cfg: &RelayConfig,
) -> Vec<String> {
    let topology = cfg.same_topology_as.as_ref().map(|key| (key, target_labels.get(key)));
    // A topology key the target does not carry would silently fan out
    // everywhere; select nothing instead.
    if let Some((_, None)) = topology {
        return vec![];
    }
    let mut names: Vec<String> = nodes
        .iter()
        .filter(|n| n.name_any() != target)
        .filter(|n| node_ready(n) && !n.annotations().contains_key(POWERED_OFF_ANNOTATION))
        .filter(|n| cfg.node_selector.iter().all(|(k, v)| n.labels().get(k) == Some(v)))
        .filter(|n| match topology {
            Some((key, Some(value))) => n.labels().get(key) == Some(value),
            _ => true,
        })
        .map(|n| n.name_any())
        .collect();
    names.sort();
    names.truncate(cfg.max_nodes.max(1) as usize);
    names
}

fn short_hash(parts: &[&str]) -> String {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    parts.hash(&mut h);
    format!("{:08x}", h.finish() as u32)
}

impl WakeOnLanDriver {
    fn shutdown_pod_name(&self) -> String {
        let mut name = format!("kha-shutdown-{}", self.node_name.replace('.', "-"));
        name.truncate(63);
        name.trim_end_matches('-').to_string()
    }

    async fn delete_pod(&self, name: &str) -> Result<()> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        match pods
            .delete(
                name,
                &DeleteParams {
                    grace_period_seconds: Some(0),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn relay_pod(&self, relay_node: &str) -> Result<Pod> {
        let mut prefix = format!("kha-wake-{}", self.node_name.replace('.', "-"));
        prefix.truncate(45);
        let name = format!(
            "{}-{}",
            prefix.trim_end_matches('-'),
            short_hash(&[&self.node_name, relay_node])
        );
        let mut args = vec![
            "wake".to_string(),
            "--mac".into(),
            self.config.mac_address.clone(),
            "--port".into(),
            self.config.port.to_string(),
        ];
        if let Some(b) = &self.config.broadcast_address {
            args.extend(["--broadcast".into(), b.clone()]);
        }
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": self.namespace,
                "labels": {
                    "app.kubernetes.io/name": "kube-hardware-autoscaler",
                    "app.kubernetes.io/component": RELAY_COMPONENT,
                    TARGET_LABEL: self.node_name,
                },
            },
            "spec": {
                "nodeName": relay_node,
                // The relay broadcasts on the node's own network segments.
                "hostNetwork": true,
                "restartPolicy": "Never",
                "activeDeadlineSeconds": 60,
                "terminationGracePeriodSeconds": 0,
                "automountServiceAccountToken": false,
                "tolerations": [{"operator": "Exists"}],
                "securityContext": {"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532},
                "containers": [{
                    "name": "wake",
                    "image": self.relay_image,
                    "args": args,
                    "securityContext": {
                        "allowPrivilegeEscalation": false,
                        "readOnlyRootFilesystem": true,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"memory": "64Mi"}},
                }],
            },
        }))
        .map_err(|e| DriverError::Interface(format!("building relay pod: {e}")))
    }

    /// Starts relay pods on neighbouring nodes. Returns the relay node names.
    async fn wake_via_relays(&self, cfg: &RelayConfig) -> Result<Vec<String>> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let target = nodes.get_opt(&self.node_name).await?;
        let target_labels = target.map(|n| n.labels().clone()).unwrap_or_default();
        let all = nodes.list(&ListParams::default()).await?.items;
        let relays = pick_relays(&self.node_name, &target_labels, &all, cfg);
        if relays.is_empty() {
            return Err(DriverError::Interface(match &cfg.same_topology_as {
                Some(key) if !target_labels.contains_key(key) => {
                    format!("Node {} has no {key} label to select relays by", self.node_name)
                }
                _ => "no Ready node qualifies as a Wake-on-LAN relay".into(),
            }));
        }
        // Clear out relays from earlier wakes of this machine.
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let old = pods
            .list(&ListParams::default().labels(&format!(
                "{TARGET_LABEL}={},app.kubernetes.io/component={RELAY_COMPONENT}",
                self.node_name
            )))
            .await?;
        for p in old.items {
            self.delete_pod(&p.name_any()).await?;
        }
        let mut started = Vec::new();
        for relay in &relays {
            match pods.create(&PostParams::default(), &self.relay_pod(relay)?).await {
                Ok(_) => started.push(relay.clone()),
                Err(e) => tracing::warn!(relay, error = %e, "cannot start Wake-on-LAN relay"),
            }
        }
        if started.is_empty() {
            return Err(DriverError::Interface(
                "no Wake-on-LAN relay pod could be started".into(),
            ));
        }
        Ok(started)
    }
}

const SHUTDOWN_SCRIPT: &str = r#"set -eu
current="$(cat /proc/sys/kernel/random/boot_id)"
if [ -n "${EXPECTED_BOOT_ID}" ] && [ "${current}" != "${EXPECTED_BOOT_ID}" ]; then
  echo "host rebooted since shutdown was requested (boot id ${current}); not shutting down"
  exit 0
fi
echo "powering off host"
exec nsenter -t 1 -m -u -i -n -p -- sh -c 'systemctl poweroff || poweroff'
"#;

#[async_trait]
impl PowerDriver for WakeOnLanDriver {
    async fn power_state(&self) -> Result<PowerState> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        Ok(match nodes.get_opt(&self.node_name).await? {
            Some(n) if node_ready(&n) => PowerState::On,
            _ => PowerState::Off,
        })
    }

    async fn power_on(&self) -> Result<()> {
        // Make sure no stale shutdown pod survives into the next boot.
        self.delete_pod(&self.shutdown_pod_name()).await?;

        let mut errors = Vec::new();
        let mut sent = false;
        if self.config.send_from_operator {
            let targets = wake::targets(self.config.broadcast_address.as_deref(), self.config.port)?;
            match wake::send(&self.mac, &targets, 3).await {
                Ok(n) => {
                    tracing::info!(node = %self.node_name, packets = n, "magic packet sent from the operator's node");
                    sent = true;
                }
                Err(e) => errors.push(format!("operator: {e}")),
            }
        }
        if let Some(relay) = &self.config.relay {
            match self.wake_via_relays(relay).await {
                Ok(relays) => {
                    tracing::info!(node = %self.node_name, ?relays, "magic packet relayed from neighbouring nodes");
                    sent = true;
                }
                Err(e) => errors.push(format!("relays: {e}")),
            }
        }
        if sent {
            Ok(())
        } else {
            Err(DriverError::Interface(errors.join("; ")))
        }
    }

    async fn power_off(&self, _force: bool) -> Result<()> {
        // Wake-on-LAN has no out-of-band power off; a forced request simply
        // re-issues the in-band shutdown.
        let nodes: Api<Node> = Api::all(self.client.clone());
        let node = nodes
            .get_opt(&self.node_name)
            .await?
            .ok_or_else(|| DriverError::Interface(format!("node {} not found", self.node_name)))?;
        let boot_id = node
            .status
            .as_ref()
            .and_then(|s| s.node_info.as_ref())
            .map(|i| i.boot_id.clone())
            .unwrap_or_default();

        self.delete_pod(&self.shutdown_pod_name()).await?;
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": self.shutdown_pod_name(),
                "namespace": self.namespace,
                "labels": {
                    "app.kubernetes.io/name": "kube-hardware-autoscaler",
                    "app.kubernetes.io/component": "shutdown",
                    "hardware-autoscaler.safewords.com/node": self.node_name,
                },
            },
            "spec": {
                "nodeName": self.node_name,
                "hostPID": true,
                "restartPolicy": "Never",
                "activeDeadlineSeconds": 900,
                "terminationGracePeriodSeconds": 0,
                "priorityClassName": "system-node-critical",
                "tolerations": [{"operator": "Exists"}],
                "containers": [{
                    "name": "shutdown",
                    "image": self.shutdown_image,
                    "command": ["sh", "-c", SHUTDOWN_SCRIPT],
                    "env": [{"name": "EXPECTED_BOOT_ID", "value": boot_id}],
                    "securityContext": {"privileged": true},
                    "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}},
                }],
            },
        }))
        .map_err(|e| DriverError::Interface(format!("building shutdown pod: {e}")))?;
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        pods.create(&PostParams::default(), &pod).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, labels: &[(&str, &str)], ready: bool, powered_off: bool) -> Node {
        let mut annotations = serde_json::Map::new();
        if powered_off {
            annotations.insert(POWERED_OFF_ANNOTATION.into(), "2026-01-01T00:00:00Z".into());
        }
        serde_json::from_value(json!({
            "metadata": {
                "name": name,
                "labels": labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<BTreeMap<_, _>>(),
                "annotations": annotations,
            },
            "status": {"conditions": [{"type": "Ready", "status": if ready { "True" } else { "False" }}]}
        }))
        .unwrap()
    }

    fn labels(l: &[(&str, &str)]) -> BTreeMap<String, String> {
        l.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    const ZONE: &str = "topology.kubernetes.io/zone";

    fn cluster() -> Vec<Node> {
        vec![
            node("gpu-1", &[(ZONE, "site-a")], false, true), // the target, currently off
            node("a-1", &[(ZONE, "site-a")], true, false),
            node("a-2", &[(ZONE, "site-a"), ("relay", "yes")], true, false),
            node("a-3", &[(ZONE, "site-a")], false, false), // NotReady
            node("a-4", &[(ZONE, "site-a")], true, true),   // powered off by us
            node("b-1", &[(ZONE, "site-b")], true, false),
        ]
    }

    #[test]
    fn relays_come_from_the_same_site() {
        let cfg = RelayConfig {
            same_topology_as: Some(ZONE.into()),
            max_nodes: 3,
            ..Default::default()
        };
        let target = labels(&[(ZONE, "site-a")]);
        assert_eq!(pick_relays("gpu-1", &target, &cluster(), &cfg), vec!["a-1", "a-2"]);
    }

    #[test]
    fn without_topology_relays_fan_out_everywhere() {
        let cfg = RelayConfig {
            max_nodes: 10,
            ..Default::default()
        };
        assert_eq!(
            pick_relays("gpu-1", &BTreeMap::new(), &cluster(), &cfg),
            vec!["a-1", "a-2", "b-1"]
        );
        let one = RelayConfig {
            max_nodes: 1,
            ..Default::default()
        };
        assert_eq!(pick_relays("gpu-1", &BTreeMap::new(), &cluster(), &one), vec!["a-1"]);
    }

    #[test]
    fn selector_restricts_relays() {
        let cfg = RelayConfig {
            node_selector: labels(&[("relay", "yes")]),
            max_nodes: 3,
            ..Default::default()
        };
        assert_eq!(pick_relays("gpu-1", &BTreeMap::new(), &cluster(), &cfg), vec!["a-2"]);
    }

    #[test]
    fn missing_topology_label_on_target_selects_nothing() {
        let cfg = RelayConfig {
            same_topology_as: Some(ZONE.into()),
            max_nodes: 3,
            ..Default::default()
        };
        assert!(pick_relays("gpu-1", &BTreeMap::new(), &cluster(), &cfg).is_empty());
    }

    #[test]
    fn config_validation() {
        let parse = |v: serde_json::Value| serde_json::from_value::<WakeOnLanConfig>(v);
        let c = parse(json!({"macAddress": "aa:bb:cc:dd:ee:ff"})).unwrap();
        assert!(c.send_from_operator && c.relay.is_none() && c.broadcast_address.is_none());
        let c = parse(json!({"macAddress": "aa:bb:cc:dd:ee:ff", "relay": {"sameTopologyAs": ZONE}})).unwrap();
        assert_eq!(c.relay.unwrap().max_nodes, 3);
        assert!(parse(json!({"macAddress": "aa:bb:cc:dd:ee:ff", "relay": {"bogus": 1}})).is_err());
    }
}
