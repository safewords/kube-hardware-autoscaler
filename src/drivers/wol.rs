//! Wake-on-LAN driver.
//!
//! * power on: a UDP magic packet is broadcast to the machine's NIC;
//! * power off: a privileged pod is scheduled onto the node that runs
//!   `systemctl poweroff` in the host namespaces;
//! * power state: inferred from the Node's `Ready` condition.
//!
//! The shutdown pod carries the node's boot id and refuses to act if the host
//! has rebooted since, so a stale pod can never shut down a freshly booted host.

use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::UdpSocket;

use super::{DriverError, DriverInit, DriverKind, PowerDriver, Result};
use crate::crd::PowerState;

/// `config` of the `wakeOnLan` driver. No credentials are needed; the
/// operator must be able to reach the machine's broadcast domain (typically
/// by running with `hostNetwork: true`).
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WakeOnLanConfig {
    /// MAC address of the NIC to wake, e.g. `aa:bb:cc:dd:ee:ff`.
    pub mac_address: String,
    /// Broadcast address the magic packet is sent to. Defaults to `255.255.255.255`.
    #[serde(default = "default_broadcast")]
    pub broadcast_address: String,
    /// UDP port. Defaults to 9.
    #[serde(default = "default_port")]
    pub port: u16,
    /// Image used for the shutdown pod; must provide `sh` and `nsenter`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shutdown_image: Option<String>,
}

fn default_broadcast() -> String {
    "255.255.255.255".into()
}
fn default_port() -> u16 {
    9
}

pub struct WakeOnLanDriver {
    config: WakeOnLanConfig,
    mac: [u8; 6],
    node_name: String,
    client: Client,
    namespace: String,
    image: String,
    timeout: Duration,
}

impl DriverKind for WakeOnLanDriver {
    const NAME: &'static str = "wakeOnLan";
    const DESCRIPTION: &'static str = "Wake-on-LAN magic packet to power on; in-band shutdown pod to power off";
    const REQUIRES_CREDENTIALS: bool = false;
    type Config = WakeOnLanConfig;

    fn build(config: WakeOnLanConfig, init: &DriverInit<'_>) -> Result<Self> {
        let mac = parse_mac(&config.mac_address)
            .ok_or_else(|| DriverError::Config(format!("invalid MAC address {:?}", config.mac_address)))?;
        let image = config
            .shutdown_image
            .clone()
            .unwrap_or_else(|| init.ctx.shutdown_image.clone());
        Ok(Self {
            config,
            mac,
            node_name: init.node_name.to_string(),
            client: init.ctx.client.clone(),
            namespace: init.ctx.namespace.clone(),
            image,
            timeout: init.ctx.op_timeout,
        })
    }
}

impl WakeOnLanDriver {
    fn pod_name(&self) -> String {
        let mut name = format!("kha-shutdown-{}", self.node_name.replace('.', "-"));
        name.truncate(63);
        name.trim_end_matches('-').to_string()
    }

    async fn delete_shutdown_pod(&self) -> Result<()> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        match pods
            .delete(
                &self.pod_name(),
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
}

pub(crate) fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let parts: Vec<&str> = s.split([':', '-']).collect();
    if parts.len() != 6 {
        return None;
    }
    let mut mac = [0u8; 6];
    for (i, p) in parts.iter().enumerate() {
        if p.len() != 2 {
            return None;
        }
        mac[i] = u8::from_str_radix(p, 16).ok()?;
    }
    Some(mac)
}

pub(crate) fn magic_packet(mac: &[u8; 6]) -> Vec<u8> {
    let mut packet = vec![0xFF; 6];
    for _ in 0..16 {
        packet.extend_from_slice(mac);
    }
    packet
}

fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.type_ == "Ready"))
        .is_some_and(|c| c.status == "True")
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
        self.delete_shutdown_pod().await?;
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.set_broadcast(true)?;
        let packet = magic_packet(&self.mac);
        let target = format!("{}:{}", self.config.broadcast_address, self.config.port);
        for _ in 0..3 {
            tokio::time::timeout(self.timeout, socket.send_to(&packet, &target))
                .await
                .map_err(|_| DriverError::Timeout(self.timeout))??;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
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

        self.delete_shutdown_pod().await?;
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": self.pod_name(),
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
                    "image": self.image,
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

    #[test]
    fn parses_macs() {
        assert_eq!(
            parse_mac("aa:bb:cc:dd:ee:ff"),
            Some([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff])
        );
        assert_eq!(parse_mac("00-11-22-33-44-55"), Some([0, 0x11, 0x22, 0x33, 0x44, 0x55]));
        assert_eq!(parse_mac("00:11:22:33:44"), None);
        assert_eq!(parse_mac("zz:11:22:33:44:55"), None);
    }

    #[test]
    fn builds_magic_packet() {
        let p = magic_packet(&[1, 2, 3, 4, 5, 6]);
        assert_eq!(p.len(), 102);
        assert_eq!(&p[..6], &[0xFF; 6]);
        assert_eq!(&p[6..12], &[1, 2, 3, 4, 5, 6]);
        assert_eq!(&p[96..], &[1, 2, 3, 4, 5, 6]);
    }
}
