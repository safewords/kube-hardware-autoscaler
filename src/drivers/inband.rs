//! In-band host commands: a privileged pod scheduled onto the node that runs
//! `systemctl poweroff` or `systemctl suspend` in the host namespaces.
//!
//! Used by the `wakeOnLan` driver to power off, and by the controller to put
//! machines into standby (`lifecycle.powerOffMode: Standby`/`Auto`), which no
//! management interface can do out-of-band. A separate unprivileged probe pod
//! reads `/sys/power/mem_sleep` to find out whether the host can sleep in S3.
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
    /// Suspend to RAM in ACPI S3 (`deep`), not the kernel's default mode.
    SuspendS3,
}

impl HostCommand {
    fn shell(self) -> &'static str {
        match self {
            HostCommand::PowerOff => "systemctl poweroff || poweroff",
            HostCommand::Suspend => "systemctl suspend || echo mem > /sys/power/state",
            HostCommand::SuspendS3 => {
                "echo deep > /sys/power/mem_sleep && (systemctl suspend || echo mem > /sys/power/state)"
            }
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

async fn delete_pod(client: &Client, namespace: &str, name: &str) -> Result<()> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let dp = DeleteParams {
        grace_period_seconds: Some(0),
        ..Default::default()
    };
    match pods.delete(name, &dp).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Deletes the node's host command pod, if any.
pub async fn cancel(client: &Client, namespace: &str, node_name: &str) -> Result<()> {
    delete_pod(client, namespace, &pod_name(node_name)).await
}

/// Progress of a sleep capability probe.
#[derive(Debug, PartialEq, Eq)]
pub enum Probe {
    /// The probe pod was started or is still running.
    Pending,
    /// The host's boot id and the contents of its `/sys/power/mem_sleep`.
    Done {
        boot_id: String,
        mem_sleep: String,
    },
    Failed(String),
}

fn probe_pod_name(node_name: &str) -> String {
    let mut name = format!("kha-probe-{}", node_name.replace('.', "-"));
    name.truncate(63);
    name.trim_end_matches('-').to_string()
}

/// Whether a `/sys/power/mem_sleep` listing (e.g. `s2idle [deep]`) offers S3.
pub fn offers_s3(mem_sleep: &str) -> bool {
    mem_sleep
        .split_whitespace()
        .any(|m| m.trim_matches(['[', ']']) == "deep")
}

fn parse_probe(output: &str) -> Option<(String, String)> {
    let mut lines = output.lines();
    let boot_id = lines.next()?.trim().to_string();
    let mem_sleep = lines.next().unwrap_or_default().trim().to_string();
    (!boot_id.is_empty()).then_some((boot_id, mem_sleep))
}

/// Reports which sleep modes the host's kernel offers. Call repeatedly: the
/// first call starts a small unprivileged pod that reads `/sys/power/mem_sleep`
/// through a read-only hostPath; a later call collects its result from the
/// pod's termination message and removes it.
pub async fn probe_sleep(client: &Client, namespace: &str, node_name: &str, image: &str) -> Result<Probe> {
    let name = probe_pod_name(node_name);
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let Some(pod) = pods.get_opt(&name).await? else {
        let pod: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": "kube-hardware-autoscaler",
                    "app.kubernetes.io/component": "sleep-probe",
                    "hardware-autoscaler.safewords.com/node": node_name,
                },
            },
            "spec": {
                "nodeName": node_name,
                "restartPolicy": "Never",
                "activeDeadlineSeconds": 120,
                "terminationGracePeriodSeconds": 0,
                "automountServiceAccountToken": false,
                "tolerations": [{"operator": "Exists"}],
                "volumes": [{"name": "power", "hostPath": {"path": "/sys/power", "type": "Directory"}}],
                "containers": [{
                    "name": "probe",
                    "image": image,
                    // boot_id is not namespaced: it is the host's.
                    "command": ["sh", "-c", "{ cat /proc/sys/kernel/random/boot_id; cat /host/sys/power/mem_sleep 2>/dev/null || true; } > /dev/termination-log"],
                    "volumeMounts": [{"name": "power", "mountPath": "/host/sys/power", "readOnly": true}],
                    "securityContext": {
                        "allowPrivilegeEscalation": false,
                        "readOnlyRootFilesystem": true,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"memory": "32Mi"}},
                }],
            },
        }))
        .map_err(|e| DriverError::Interface(format!("building sleep probe pod: {e}")))?;
        pods.create(&PostParams::default(), &pod).await?;
        return Ok(Probe::Pending);
    };
    let status = pod.status.unwrap_or_default();
    let terminated = status
        .container_statuses
        .unwrap_or_default()
        .into_iter()
        .find_map(|c| c.state.and_then(|s| s.terminated));
    let result = match (terminated, status.phase.as_deref()) {
        (Some(t), _) if t.exit_code == 0 => match parse_probe(t.message.as_deref().unwrap_or_default()) {
            Some((boot_id, mem_sleep)) => Probe::Done { boot_id, mem_sleep },
            None => Probe::Failed("sleep probe reported nothing".into()),
        },
        (Some(t), _) => Probe::Failed(format!("sleep probe exited with {}", t.exit_code)),
        (None, Some("Failed")) => Probe::Failed(status.message.unwrap_or_else(|| "sleep probe pod failed".into())),
        (None, _) => return Ok(Probe::Pending),
    };
    delete_pod(client, namespace, &name).await?;
    Ok(result)
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
        HostCommand::Suspend | HostCommand::SuspendS3 => (
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
                // Root explicitly: the default image (the operator's own) runs as
                // a non-root user, and nsenter into PID 1 needs root.
                "securityContext": {"privileged": true, "runAsUser": 0, "runAsGroup": 0, "runAsNonRoot": false},
                "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}},
            }],
        },
    }))
    .map_err(|e| DriverError::Interface(format!("building {action} pod: {e}")))?;
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    pods.create(&PostParams::default(), &pod).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_s3() {
        assert!(offers_s3("s2idle [deep]"));
        assert!(offers_s3("[s2idle] deep"));
        assert!(!offers_s3("[s2idle]"));
        assert!(!offers_s3(""));
    }

    #[test]
    fn parses_probe_output() {
        assert_eq!(
            parse_probe("abc-123\ns2idle [deep]\n"),
            Some(("abc-123".into(), "s2idle [deep]".into()))
        );
        // No mem_sleep file: no suspend-to-RAM at all.
        assert_eq!(parse_probe("abc-123\n"), Some(("abc-123".into(), String::new())));
        assert_eq!(parse_probe(""), None);
    }
}
