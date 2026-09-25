//! Controllers and the state they share.

pub mod node_power_management_config;
pub mod node_scaling_pool;

use std::sync::Arc;

use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::{Patch, PatchParams};
use kube::runtime::events::{Event, EventType, Recorder};
use kube::runtime::reflector::Store;
use kube::{Api, Client, Resource, ResourceExt};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::crd::{CORDONED_BY_ANNOTATION, NodePowerManagementConfig, NodeScalingPool, POWERED_OFF_ANNOTATION};
use crate::drivers::DriverContext;
use crate::identity::IdentityCheck;
use crate::metrics::Metrics;

pub const FIELD_MANAGER: &str = "kube-hardware-autoscaler";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kubernetes API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
}

pub struct Context {
    pub client: Client,
    pub drivers: DriverContext,
    pub nodes: Store<Node>,
    pub pods: Store<Pod>,
    pub node_power_management_configs: Store<NodePowerManagementConfig>,
    pub pools: Store<NodeScalingPool>,
    pub recorder: Recorder,
    pub metrics: Arc<Metrics>,
    /// Log power actions instead of executing them.
    pub dry_run: bool,
    /// Identity check results per (NodePowerManagementConfig, generation), so BMCs are not
    /// queried for their UUID on every reconcile.
    pub identity_cache: IdentityCache,
}

const IDENTITY_TTL: std::time::Duration = std::time::Duration::from_secs(600);

type IdentityKey = (String, i64);
type IdentityEntry = (std::time::Instant, Vec<(String, IdentityCheck)>);

/// Cached identity results: (NodePowerManagementConfig name, generation) -> (checked at, per-interface result).
#[derive(Default)]
pub struct IdentityCache(std::sync::Mutex<std::collections::HashMap<IdentityKey, IdentityEntry>>);

impl IdentityCache {
    pub fn get(&self, key: &(String, i64)) -> Option<Vec<(String, IdentityCheck)>> {
        let map = self.0.lock().ok()?;
        map.get(key)
            .filter(|(at, _)| at.elapsed() < IDENTITY_TTL)
            .map(|(_, r)| r.clone())
    }

    pub fn put(&self, key: (String, i64), results: Vec<(String, IdentityCheck)>) {
        if let Ok(mut map) = self.0.lock() {
            // Drop entries for older generations of the same object.
            map.retain(|(name, _), _| name != &key.0);
            map.insert(key, (std::time::Instant::now(), results));
        }
    }
}

impl Context {
    /// All NodeScalingPools currently known.
    pub fn pools(&self) -> Vec<Arc<NodeScalingPool>> {
        self.pools.state()
    }

    pub fn node(&self, name: &str) -> Option<Arc<Node>> {
        self.nodes.get(&kube::runtime::reflector::ObjectRef::new(name))
    }

    pub fn pods_on(&self, node: &str) -> Vec<Arc<Pod>> {
        self.pods
            .state()
            .into_iter()
            .filter(|p| p.spec.as_ref().and_then(|s| s.node_name.as_deref()) == Some(node))
            .collect()
    }

    /// Publishes a Kubernetes event on `obj`; failures are only logged.
    pub async fn event<K: Resource<DynamicType = ()>>(&self, obj: &K, type_: EventType, reason: &str, note: String) {
        let ev = Event {
            type_,
            reason: reason.into(),
            note: Some(note),
            action: reason.into(),
            secondary: None,
        };
        if let Err(e) = self.recorder.publish(&ev, &obj.object_ref(&())).await {
            tracing::warn!(error = %e, reason, "failed to publish event");
        }
    }
}

/// Builds a top-level JSON merge patch turning `old` into `new`: changed keys
/// are set, removed keys are nulled, untouched keys are omitted (so fields
/// written concurrently by another controller are preserved).
pub fn merge_diff<T: Serialize>(old: &T, new: &T) -> Result<Map<String, Value>, Error> {
    let old = serde_json::to_value(old)?;
    let new = serde_json::to_value(new)?;
    let empty = Map::new();
    let old = old.as_object().unwrap_or(&empty);
    let new = new.as_object().unwrap_or(&empty);
    let mut patch = Map::new();
    for (k, v) in new {
        if old.get(k) != Some(v) {
            patch.insert(k.clone(), v.clone());
        }
    }
    for k in old.keys() {
        if !new.contains_key(k) {
            patch.insert(k.clone(), Value::Null);
        }
    }
    Ok(patch)
}

/// Patches the status subresource with the difference between `old` and `new`.
pub async fn patch_status_diff<K, S>(api: &Api<K>, name: &str, old: &S, new: &S) -> Result<(), Error>
where
    K: Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
    S: Serialize,
{
    let diff = merge_diff(old, new)?;
    if diff.is_empty() {
        return Ok(());
    }
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(json!({ "status": diff })))
        .await?;
    Ok(())
}

pub fn node_ready(node: &Node) -> bool {
    node.status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.type_ == "Ready"))
        .is_some_and(|c| c.status == "True")
}

fn is_cordoned(node: &Node) -> bool {
    node.spec.as_ref().and_then(|s| s.unschedulable).unwrap_or(false)
}

/// Cordons a node, remembering that the operator did it. A node that was
/// already cordoned by someone else is left alone (and won't be uncordoned).
pub async fn cordon(client: &Client, node: &Node) -> Result<(), Error> {
    if is_cordoned(node) {
        return Ok(());
    }
    let api: Api<Node> = Api::all(client.clone());
    let patch = json!({
        "metadata": {"annotations": {CORDONED_BY_ANNOTATION: "true"}},
        "spec": {"unschedulable": true},
    });
    api.patch(&node.name_any(), &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(())
}

const OUT_OF_SERVICE_TAINT: &str = "node.kubernetes.io/out-of-service";

/// Marks a Node as deliberately powered off by the operator, so node fencing
/// tools can tell it apart from a failed node.
pub async fn mark_powered_off(client: &Client, node: &str, now: chrono::DateTime<chrono::Utc>) -> Result<(), Error> {
    let api: Api<Node> = Api::all(client.clone());
    let patch = json!({"metadata": {"annotations": {POWERED_OFF_ANNOTATION: now.to_rfc3339()}}});
    api.patch(node, &PatchParams::default(), &Patch::Merge(patch)).await?;
    Ok(())
}

/// After the operator powered a machine back on: removes the powered-off
/// marker and any `out-of-service` fence taint added while it was off. Only
/// acts on Nodes carrying the marker, so genuine fences are left alone.
pub async fn clear_powered_off(client: &Client, node: &Node) -> Result<bool, Error> {
    if !node.annotations().contains_key(POWERED_OFF_ANNOTATION) {
        return Ok(false);
    }
    let api: Api<Node> = Api::all(client.clone());
    let taints = node.spec.as_ref().and_then(|s| s.taints.clone()).unwrap_or_default();
    let mut patch = json!({"metadata": {"annotations": {POWERED_OFF_ANNOTATION: null}}});
    if taints.iter().any(|t| t.key == OUT_OF_SERVICE_TAINT) {
        let kept: Vec<_> = taints.into_iter().filter(|t| t.key != OUT_OF_SERVICE_TAINT).collect();
        patch["spec"] = json!({ "taints": kept });
    }
    api.patch(&node.name_any(), &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(true)
}

/// Uncordons a node only if the operator cordoned it.
pub async fn uncordon_if_ours(client: &Client, node: &Node) -> Result<bool, Error> {
    if !node.annotations().contains_key(CORDONED_BY_ANNOTATION) {
        return Ok(false);
    }
    let api: Api<Node> = Api::all(client.clone());
    let patch = json!({
        "metadata": {"annotations": {CORDONED_BY_ANNOTATION: null}},
        "spec": {"unschedulable": null},
    });
    api.patch(&node.name_any(), &PatchParams::default(), &Patch::Merge(patch))
        .await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_diff_only_contains_changes_and_nulls_removals() {
        let old = json!({"a": 1, "b": 2, "c": 3});
        let new = json!({"a": 1, "b": 5});
        let d = merge_diff(&old, &new).unwrap();
        assert_eq!(Value::Object(d), json!({"b": 5, "c": null}));
    }
}
