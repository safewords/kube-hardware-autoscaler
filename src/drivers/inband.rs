//! In-band host commands: a privileged pod scheduled onto the node that runs
//! `systemctl poweroff` or `systemctl suspend` in the host namespaces.
//!
//! Used by the `wakeOnLan` driver to power off, and by the controller to put
//! machines into standby (`lifecycle.powerOffMode: Standby`), which no
//! management interface can do out-of-band.
//!
//! The pod carries the node's boot id and refuses to act if the host has
//! rebooted since, so a stale pod can never shut down a freshly booted host.
//! A suspend is additionally refused after a short deadline: suspending
//! does not change the boot id, so a pod that only starts after the machine
//! has been woken again must not put it straight back to sleep.

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use serde_json::json;

use super::{DriverError, Result};

/// What the host is asked to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostCommand {
    PowerOff,
    Suspend,
}

impl HostCommand {
    fn shell(self) -> &'static str {
        match self {
            HostCommand::PowerOff => "systemctl poweroff || poweroff",
            HostCommand::Suspend => "systemctl suspend || echo mem > /sys/power/state",
        }
    }
}

/// A suspend pod that has not run this many seconds after being requested
/// does nothing.
const SUSPEND_DEADLINE_SECS: i64 = 120;

const SCRIPT: &str = r#"set -eu
current="$(cat /proc/sys/kernel/random/boot_id)"
if [ -n "${EXPECTED_BOOT_ID}" ] && [ "${current}" != "${EXPECTED_BOOT_ID}" ]; then
  echo "host rebooted since ${ACTION} was requested (boot id ${current}); not acting"
  exit 0
fi
if [ -n "${NOT_AFTER}" ] && [ "$(date +%s)" -gt "${NOT_AFTER}" ]; then
  echo "${ACTION} request expired; not acting"
  exit 0
fi
echo "${ACTION}: host"
exec nsenter -t 1 -m -u -i -n -p -- sh -c "${HOST_COMMAND}"
"#;

/// Name of the (single) host command pod of a node.
pub fn pod_name(node_name: &str) -> String {
    let mut name = format!("kha-shutdown-{}", node_name.replace('.', "-"));
    name.truncate(63);
    name.trim_end_matches('-').to_string()
}

/// Deletes the node's host command pod, if any.
pub async fn cancel(client: &Client, namespace: &str, node_name: &str) -> Result<()> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let dp = DeleteParams {
        grace_period_seconds: Some(0),
        ..Default::default()
    };
    match pods.delete(&pod_name(node_name), &dp).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Replaces any earlier host command pod of the node with one running `cmd`.
pub async fn run(client: &Client, namespace: &str, node_name: &str, image: &str, cmd: HostCommand) -> Result<()> {
    let nodes: Api<Node> = Api::all(client.clone());
    let node = nodes
        .get_opt(node_name)
        .await?
        .ok_or_else(|| DriverError::Interface(format!("node {node_name} not found")))?;
    let boot_id = node
        .status
        .as_ref()
        .and_then(|s| s.node_info.as_ref())
        .map(|i| i.boot_id.clone())
        .unwrap_or_default();
    let (action, not_after) = match cmd {
        HostCommand::PowerOff => ("poweroff", String::new()),
        HostCommand::Suspend => (
            "suspend",
            (chrono::Utc::now().timestamp() + SUSPEND_DEADLINE_SECS).to_string(),
        ),
    };

    cancel(client, namespace, node_name).await?;
    let pod: Pod = serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod_name(node_name),
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "kube-hardware-autoscaler",
                "app.kubernetes.io/component": "shutdown",
                "hardware-autoscaler.safewords.com/node": node_name,
            },
        },
        "spec": {
            "nodeName": node_name,
            "hostPID": true,
            "restartPolicy": "Never",
            "activeDeadlineSeconds": 900,
            "terminationGracePeriodSeconds": 0,
            "priorityClassName": "system-node-critical",
            "tolerations": [{"operator": "Exists"}],
            "containers": [{
                "name": "shutdown",
                "image": image,
                "command": ["sh", "-c", SCRIPT],
                "env": [
                    {"name": "EXPECTED_BOOT_ID", "value": boot_id},
                    {"name": "NOT_AFTER", "value": not_after},
                    {"name": "ACTION", "value": action},
                    {"name": "HOST_COMMAND", "value": cmd.shell()},
                ],
                "securityContext": {"privileged": true},
                "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}},
            }],
        },
    }))
    .map_err(|e| DriverError::Interface(format!("building {action} pod: {e}")))?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    pods.create(&PostParams::default(), &pod).await?;
    Ok(())
}
