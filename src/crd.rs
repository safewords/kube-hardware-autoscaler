//! Custom resource definitions.
//!
//! * [`NodePowerManagementConfig`] binds a Kubernetes `Node` to an out-of-band power interface
//!   (IPMI, Redfish, PiKVM or Wake-on-LAN) and tracks its power lifecycle.
//! * [`NodeScalingPool`] selects a class of machines by Node labels and decides, based on demand,
//!   which of them should be powered on or off.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const API_GROUP: &str = "hardware-autoscaler.safewords.com";

/// Annotation placed on a Node when the operator cordons it, so that only
/// operator-cordoned nodes are uncordoned again.
pub const CORDONED_BY_ANNOTATION: &str = "hardware-autoscaler.safewords.com/cordoned-by-operator";

/// Pod annotation that prevents the operator from evicting the pod (and hence
/// from powering off the node that runs it). The cluster-autoscaler
/// equivalent `cluster-autoscaler.kubernetes.io/safe-to-evict` is honoured too.
pub const SAFE_TO_EVICT_ANNOTATION: &str = "hardware-autoscaler.safewords.com/safe-to-evict";

/// Node annotation set (with an RFC 3339 timestamp) while the operator has
/// deliberately powered the machine off. Node fencing tools should skip nodes
/// carrying it: an intentionally powered-off node is not a failed one. Removed,
/// together with any `node.kubernetes.io/out-of-service` taint, once the
/// operator has powered the machine back on and the Node is Ready.
pub const POWERED_OFF_ANNOTATION: &str = "hardware-autoscaler.safewords.com/powered-off";

// ---------------------------------------------------------------------------
// NodePowerManagementConfig
// ---------------------------------------------------------------------------

/// A physical (or virtual) machine backing a Kubernetes node whose power state
/// can be controlled out-of-band.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "hardware-autoscaler.safewords.com",
    version = "v1alpha1",
    kind = "NodePowerManagementConfig",
    plural = "nodepowermanagementconfigs",
    shortname = "npmc",
    status = "NodePowerManagementConfigStatus",
    // One NodePowerManagementConfig per Node, enforced by the API server: object names are
    // unique, so two NodePowerManagementConfigs can never claim the same machine.
    validation = kube::core::Rule::new("self.metadata.name == self.spec.nodeName").message("metadata.name must equal spec.nodeName"),
    printcolumn = r#"{"name":"Pool","type":"string","jsonPath":".status.pool"}"#,
    printcolumn = r#"{"name":"Policy","type":"string","jsonPath":".spec.powerPolicy"}"#,
    printcolumn = r#"{"name":"Via","type":"string","jsonPath":".status.lastPowerAction.via"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Power","type":"string","jsonPath":".status.powerState"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct NodePowerManagementConfigSpec {
    /// Name of the Kubernetes `Node` object this machine registers as. Must
    /// equal `metadata.name`. Pool membership follows from this Node's labels
    /// (see `NodeScalingPool.spec.nodeSelector`).
    pub node_name: String,

    /// How the machine's power state is decided.
    #[serde(default)]
    pub power_policy: PowerPolicy,

    /// Management interfaces, in priority order. Every operation tries them in
    /// turn (each bounded by its own timeout) and the first success wins, so
    /// e.g. IPMI can be backed by Wake-on-LAN for powering on.
    #[schemars(length(min = 1))]
    pub power_interfaces: Vec<PowerInterface>,

    /// Timeouts and drain behaviour for power transitions.
    #[serde(default)]
    pub lifecycle: LifecycleSpec,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum PowerPolicy {
    /// Power state is driven by the `NodeScalingPool` whose nodeSelector matches the Node. Without a
    /// pool the machine is only observed.
    #[default]
    Auto,
    /// Keep the machine powered on (manual override).
    AlwaysOn,
    /// Drain and keep the machine powered off (manual override / maintenance).
    AlwaysOff,
}

/// Out-of-band management interface configuration.
///
/// `driver` selects an entry from the driver catalog (`kube-hardware-autoscaler drivers`
/// lists them together with the schema of their `config`). Credentials are
/// always read from a `Secret` in the operator's namespace.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PowerInterface {
    /// Optional label used in status, events and logs. Defaults to the driver name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Driver name, e.g. `ipmi`, `redfish`, `pikvm` or `wakeOnLan`.
    pub driver: String,
    /// Operations this interface is used for (`status`, `powerOn`, `powerOff`).
    /// All of them when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actions: Option<Vec<InterfaceAction>>,
    /// Upper bound for one operation on this interface before falling back to
    /// the next one. Defaults to the operator-wide timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    /// Secret holding the interface credentials (for drivers that need them).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials_secret_ref: Option<CredentialsRef>,
    /// Driver-specific configuration; validated against the driver's schema.
    #[serde(default = "empty_object")]
    #[schemars(schema_with = "free_form_object")]
    pub config: serde_json::Value,
}

impl PowerInterface {
    pub fn label(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.driver)
    }

    pub fn handles(&self, action: InterfaceAction) -> bool {
        self.actions.as_ref().is_none_or(|a| a.contains(&action))
    }
}

/// An operation a power interface can be used for.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum InterfaceAction {
    /// Reading the power state.
    Status,
    /// Powering on.
    PowerOn,
    /// Powering off (graceful and forced).
    PowerOff,
}

fn empty_object() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}

fn free_form_object(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true
    })
}

/// Reference to a `Secret` (in the operator namespace) holding credentials.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsRef {
    /// Secret name.
    pub name: String,
    /// Key holding the user name. Defaults to `username`.
    #[serde(default = "default_username_key")]
    pub username_key: String,
    /// Key holding the password. Defaults to `password`.
    #[serde(default = "default_password_key")]
    pub password_key: String,
}

fn default_username_key() -> String {
    "username".into()
}
fn default_password_key() -> String {
    "password".into()
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleSpec {
    /// Seconds to wait for the node to become `Ready` after powering on.
    #[serde(default = "default_boot_timeout")]
    pub boot_timeout_seconds: u64,
    /// Seconds to wait for pods to be evicted before giving up.
    #[serde(default = "default_drain_timeout")]
    pub drain_timeout_seconds: u64,
    /// Seconds to wait for a graceful (ACPI) shutdown before forcing power off.
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout_seconds: u64,
    /// Power off anyway when the drain times out. When false (default) the
    /// scale-down is aborted and the node is uncordoned.
    #[serde(default)]
    pub force_after_drain_timeout: bool,
    /// What "off" means for this machine: a full `Shutdown` (default) or
    /// `Standby` (suspend to RAM, which wakes in seconds). Standby is always
    /// entered in-band, through a privileged pod running `systemctl suspend`
    /// on the node; waking uses the `powerOn` interfaces as usual (Wake-on-LAN
    /// is the most reliable way to wake a suspended machine). If the machine
    /// does not go to sleep, wakes up by itself, or does not wake up when
    /// asked, standby is considered broken on it: it is shut down (or, when it
    /// will not wake, forced off and cold booted) and standby is not used again
    /// for a day (`status.standbyFailedAt`).
    #[serde(default)]
    pub power_off_mode: PowerOffMode,
}

/// How a machine is taken offline.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum PowerOffMode {
    /// Shut the operating system down and power the machine off.
    #[default]
    Shutdown,
    /// Suspend to RAM with whatever sleep mode the kernel uses by default.
    Standby,
    /// Suspend to RAM in ACPI S3 when the machine's kernel offers it (`deep`
    /// in `/sys/power/mem_sleep`, checked once per boot), otherwise shut down.
    /// Like `Standby`, falls back to shutdown for a day after sleep fails.
    Auto,
}

fn default_boot_timeout() -> u64 {
    900
}
fn default_drain_timeout() -> u64 {
    300
}
fn default_shutdown_timeout() -> u64 {
    300
}

impl Default for LifecycleSpec {
    fn default() -> Self {
        Self {
            boot_timeout_seconds: default_boot_timeout(),
            drain_timeout_seconds: default_drain_timeout(),
            shutdown_timeout_seconds: default_shutdown_timeout(),
            force_after_drain_timeout: false,
            power_off_mode: PowerOffMode::default(),
        }
    }
}

/// Lifecycle phase of a managed machine.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum Phase {
    #[default]
    Unknown,
    PoweringOn,
    On,
    Draining,
    PoweringOff,
    Off,
    /// Suspended to RAM by the operator (`lifecycle.powerOffMode: Standby`).
    Standby,
    Error,
}

/// Observed power state as reported by the management interface.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum PowerState {
    On,
    Off,
    #[default]
    Unknown,
}

impl std::fmt::Display for PowerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PowerState::On => "On",
            PowerState::Off => "Off",
            PowerState::Unknown => "Unknown",
        })
    }
}

/// Target power state.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum PowerTarget {
    On,
    Off,
}

/// A decision made by a `NodeScalingPool` autoscaler.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScalingDecision {
    /// The `NodeScalingPool` that made the decision. A decision is ignored (and
    /// cleared) once the machine is no longer a member of that pool, e.g.
    /// because the pool was deleted or the Node's labels changed.
    #[serde(default)]
    pub pool: String,
    pub target: PowerTarget,
    pub reason: String,
    pub time: DateTime<Utc>,
}

/// A status condition (the usual Kubernetes shape).
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    #[serde(rename = "type")]
    pub type_: String,
    /// `True`, `False` or `Unknown`.
    pub status: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    pub last_transition_time: DateTime<Utc>,
}

pub const COND_NODE_FOUND: &str = "NodeFound";
pub const COND_POOL_MEMBERSHIP: &str = "PoolMembership";
pub const COND_IDENTITY_VERIFIED: &str = "IdentityVerified";
pub const COND_POWER_STATE_CONSISTENT: &str = "PowerStateConsistent";

impl NodePowerManagementConfigStatus {
    /// Sets a condition, keeping `lastTransitionTime` unless the status changed.
    pub fn set_condition(
        &mut self,
        type_: &str,
        status: &str,
        reason: &str,
        message: impl Into<String>,
        now: DateTime<Utc>,
    ) {
        set_condition(&mut self.conditions, type_, status, reason, message, now);
    }

    pub fn condition_is_true(&self, type_: &str) -> bool {
        self.conditions.iter().any(|c| c.type_ == type_ && c.status == "True")
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodePowerManagementConfigStatus {
    #[serde(default)]
    pub phase: Phase,
    /// When the current phase was entered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase_since: Option<DateTime<Utc>>,
    #[serde(default)]
    pub power_state: PowerState,
    /// Whether the Kubernetes node currently reports `Ready`.
    #[serde(default)]
    pub node_ready: bool,
    /// The `NodeScalingPool` whose `nodeSelector` matches this machine's Node, if
    /// exactly one does.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// Latest decision from the pool autoscaler (only honoured with
    /// `powerPolicy: Auto`, and only while the machine is still a member of
    /// the pool that made it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling_decision: Option<ScalingDecision>,
    /// Safety checks gating power actions: `NodeFound`, `PoolMembership`,
    /// `IdentityVerified`, `PowerStateConsistent`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// Interfaces that are misconfigured or failed before a lower-priority
    /// one succeeded during the last status check, as "name: error".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interface_warnings: Vec<String>,
    /// The last power action issued to the management interface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_power_action: Option<PowerActionRecord>,
    /// When the operator last put the machine into standby. Cleared once it is
    /// back online or has been powered off instead. While set, power-on
    /// requests are repeated even if an interface reads the suspended machine
    /// as On (some BMCs do).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standby_since: Option<DateTime<Utc>>,
    /// When standby last failed on this machine (it did not go to sleep, woke
    /// up by itself, or did not wake up). For a day afterwards the machine is
    /// shut down instead of put into standby.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standby_failed_at: Option<DateTime<Utc>>,
    /// Whether the machine can sleep in S3, as found by the sleep probe for
    /// its current boot (`powerOffMode: Auto`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_support: Option<SleepSupport>,
    /// When a forced (hard) power off was issued during the current `PoweringOff` phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forced_off_at: Option<DateTime<Utc>>,
    /// When the last drain failed; the autoscaler backs off from this node for a while.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_failed_at: Option<DateTime<Utc>>,
    /// Last-known allocatable resources of the node (kept while it is off).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allocatable: Option<ResourceAmounts>,
    /// Last-known labels of the node (kept while it is off).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_labels: Option<BTreeMap<String, String>>,
    /// Human-readable details about the current state or the last error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Result of a sleep capability probe.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SleepSupport {
    /// Boot the probe ran in; a new boot is probed again.
    pub boot_id: String,
    /// Whether ACPI S3 (`deep`) is available.
    pub s3: bool,
    /// Contents of `/sys/power/mem_sleep`, or why the probe failed.
    pub detail: String,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PowerActionRecord {
    /// `PowerOn`, `PowerOff`, `ForceOff` or `Standby`.
    pub action: String,
    pub time: DateTime<Utc>,
    /// Whether a management interface accepted the request.
    pub succeeded: bool,
    /// The interface that carried the action out (or the last one tried).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<String>,
}

/// Compact resource amounts used for scheduling estimates.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceAmounts {
    pub cpu_millis: i64,
    pub memory_bytes: i64,
    pub pods: i64,
}

// ---------------------------------------------------------------------------
// NodeScalingPool
// ---------------------------------------------------------------------------

/// A class of machines, selected by the labels of their Kubernetes Nodes, that
/// are powered on and off together based on demand. Every `NodePowerManagementConfig` whose
/// Node matches `nodeSelector` is a member; a Node matched by more than one
/// pool is a conflict and belongs to none.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "hardware-autoscaler.safewords.com",
    version = "v1alpha1",
    kind = "NodeScalingPool",
    plural = "nodescalingpools",
    shortname = "nspool",
    status = "NodeScalingPoolStatus",
    validation = kube::core::Rule::new("(has(self.spec.nodeSelector.matchLabels) && size(self.spec.nodeSelector.matchLabels) > 0) || (has(self.spec.nodeSelector.matchExpressions) && size(self.spec.nodeSelector.matchExpressions) > 0)").message("spec.nodeSelector must not be empty"),
    printcolumn = r#"{"name":"Min","type":"integer","jsonPath":".spec.minOnline"}"#,
    printcolumn = r#"{"name":"Max","type":"integer","jsonPath":".spec.maxOnline"}"#,
    printcolumn = r#"{"name":"Online","type":"integer","jsonPath":".status.onlineNodes"}"#,
    printcolumn = r#"{"name":"Total","type":"integer","jsonPath":".status.totalNodes"}"#,
    printcolumn = r#"{"name":"Pending","type":"integer","jsonPath":".status.pendingPods"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct NodeScalingPoolSpec {
    /// Which Nodes belong to this pool, by label (the "node type"). Required
    /// and non-empty; only Nodes that also have a `NodePowerManagementConfig` are powered.
    pub node_selector: NodeSelector,
    /// Minimum number of machines kept powered on.
    #[serde(default)]
    pub min_online: u32,
    /// Maximum number of machines powered on. Unlimited when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_online: Option<u32>,
    #[serde(default)]
    pub scale_up: ScaleUpSpec,
    #[serde(default)]
    pub scale_down: ScaleDownSpec,
}

/// Label selector over Kubernetes Nodes (same semantics as a Kubernetes
/// `LabelSelector`: all `matchLabels` and all `matchExpressions` must hold).
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelector {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub match_labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub match_expressions: Vec<LabelRequirement>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LabelRequirement {
    pub key: String,
    pub operator: LabelOperator,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum LabelOperator {
    In,
    NotIn,
    Exists,
    DoesNotExist,
}

impl NodeSelector {
    pub fn is_empty(&self) -> bool {
        self.match_labels.is_empty() && self.match_expressions.is_empty()
    }

    /// Whether `labels` satisfy the selector. An empty selector matches
    /// nothing (never everything), so a mistake cannot enrol every node.
    pub fn matches(&self, labels: &BTreeMap<String, String>) -> bool {
        if self.is_empty() {
            return false;
        }
        self.match_labels.iter().all(|(k, v)| labels.get(k) == Some(v))
            && self.match_expressions.iter().all(|r| {
                let value = labels.get(&r.key);
                match r.operator {
                    LabelOperator::In => value.is_some_and(|v| r.values.contains(v)),
                    LabelOperator::NotIn => value.is_none_or(|v| !r.values.contains(v)),
                    LabelOperator::Exists => value.is_some(),
                    LabelOperator::DoesNotExist => value.is_none(),
                }
            })
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleUpSpec {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Unschedulable pods younger than this are ignored (lets the scheduler settle).
    #[serde(default = "default_pending_grace")]
    pub pending_pod_grace_seconds: u64,
    /// Only count pending pods that explicitly target this pool: their
    /// `nodeSelector` (or every required node-affinity term) must require all of
    /// the pool's `nodeSelector.matchLabels`. Keeps unrelated pods (CI runners,
    /// overflow from other nodes) from powering on a dedicated pool such as GPUs.
    #[serde(default)]
    pub require_explicit_selection: bool,
    /// Maximum machines powered on per autoscaler cycle.
    #[serde(default = "default_scale_up_step")]
    pub max_nodes_per_step: u32,
    /// Power on machines that can sleep in S3 before those that cannot
    /// (`powerOffMode: Auto`/`Standby`): they are back in seconds after the
    /// next idle period instead of needing a full boot. A machine counts as
    /// S3-capable while it is in `Standby`, or when its last sleep probe found
    /// S3 and standby has not failed on it in the past day. Ranks after the
    /// `preferredNodes` weight and before machine size (see `preferredNodes`).
    #[serde(default)]
    pub prefer_s3_capable: bool,
    /// Which machines this pool would rather use: a weight from 0 to 100 per
    /// machine, by Node name. Machines not listed weigh 0; each name may
    /// appear once. Machines are powered on in this order, and powered off
    /// in exactly the reverse order:
    /// 1. weight, highest first;
    /// 2. with `preferS3Capable`, machines that can sleep in S3 first;
    /// 3. size (CPU, then memory), largest first: fewer machines absorb the
    ///    pending pods, and on the way down the smallest goes first, taking
    ///    the least capacity away;
    /// 4. Node name, ascending.
    ///
    /// Scale-up gives each pending pod the first machine in this order that
    /// it fits on; `minOnline` takes the first machines that are off. When
    /// several machines are unneeded, scale-down powers off the last first.
    /// A weight never makes a machine eligible that otherwise is not, and
    /// never keeps one on that is otherwise unneeded.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "preferred_nodes_schema")]
    pub preferred_nodes: Vec<PreferredNode>,
}

/// One entry of `scaleUp.preferredNodes`.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PreferredNode {
    /// Node name (equal to the machine's `NodePowerManagementConfig` name).
    pub name: String,
    /// 0 to 100; higher wakes first and powers off last.
    pub weight: u32,
}

/// The highest `scaleUp.preferredNodes[].weight`.
pub const MAX_NODE_WEIGHT: u32 = 100;

fn preferred_nodes_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        // A list keyed by name: the API server refuses duplicate names.
        "x-kubernetes-list-type": "map",
        "x-kubernetes-list-map-keys": ["name"],
        "items": {
            "type": "object",
            "required": ["name", "weight"],
            "properties": {
                "name": {
                    "description": "Node name (equal to the machine's NodePowerManagementConfig name).",
                    "type": "string",
                    "minLength": 1
                },
                "weight": {
                    "description": "0 to 100; higher wakes first and powers off last.",
                    "type": "integer",
                    "minimum": 0,
                    "maximum": MAX_NODE_WEIGHT
                }
            }
        }
    })
}

impl ScaleUpSpec {
    /// The weight of the machine registered as Node `node`: its first
    /// `preferredNodes` entry, 0 when unlisted.
    pub fn node_weight(&self, node: &str) -> u32 {
        self.preferred_nodes
            .iter()
            .find(|p| p.name == node)
            .map_or(0, |p| p.weight)
    }
}

impl Default for ScaleUpSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            pending_pod_grace_seconds: default_pending_grace(),
            max_nodes_per_step: default_scale_up_step(),
            require_explicit_selection: false,
            prefer_s3_capable: false,
            preferred_nodes: Vec::new(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ScaleDownSpec {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// A node is considered unneeded when both its CPU and memory requests are
    /// below this percentage of allocatable.
    #[serde(default = "default_utilization")]
    pub utilization_threshold_percent: u32,
    /// How long a node must be continuously unneeded before it is powered off.
    #[serde(default = "default_unneeded")]
    pub unneeded_seconds: u64,
    /// After this pool powers any machine on (for pending pods or for
    /// `minOnline`), power no machine in the pool off for this many seconds,
    /// however idle it looks. Pool-wide, and it only holds scale-down back:
    /// scale-up is never delayed by it. It covers the time between a machine
    /// booting and its pods being scheduled and started. The equivalent of
    /// cluster-autoscaler's `--scale-down-delay-after-add`. 600 when neither
    /// this nor the deprecated `delayAfterScaleUpSeconds` is set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hold_after_power_on_seconds: Option<u64>,
    /// Deprecated: the old name of `holdAfterPowerOnSeconds`, still accepted
    /// with the same meaning. When both are set, `holdAfterPowerOnSeconds`
    /// wins (and the operator logs a warning).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delay_after_scale_up_seconds: Option<u64>,
    /// Do not retry a node whose drain failed within this many seconds.
    #[serde(default = "default_drain_backoff")]
    pub drain_failure_backoff_seconds: u64,
    /// Leave DaemonSet pods out of the utilization calculation (they run on every
    /// eligible node anyway), like cluster-autoscaler's --ignore-daemonsets-utilization.
    #[serde(default)]
    pub ignore_daemon_set_utilization: bool,
    /// Leave pods that do not explicitly target this pool out of the utilization
    /// calculation: they are guests that get drained elsewhere, not a reason to
    /// keep the machine powered on.
    #[serde(default)]
    pub ignore_non_selecting_pod_utilization: bool,
    /// Maximum machines concurrently being scaled down.
    #[serde(default = "default_scale_down_step")]
    pub max_nodes_per_step: u32,
}

impl Default for ScaleDownSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            utilization_threshold_percent: default_utilization(),
            unneeded_seconds: default_unneeded(),
            hold_after_power_on_seconds: None,
            delay_after_scale_up_seconds: None,
            drain_failure_backoff_seconds: default_drain_backoff(),
            max_nodes_per_step: default_scale_down_step(),
            ignore_daemon_set_utilization: false,
            ignore_non_selecting_pod_utilization: false,
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_pending_grace() -> u64 {
    30
}
fn default_scale_up_step() -> u32 {
    3
}
fn default_utilization() -> u32 {
    50
}
fn default_unneeded() -> u64 {
    600
}
/// `scaleDown.holdAfterPowerOnSeconds` when neither name is set.
pub const DEFAULT_HOLD_AFTER_POWER_ON_SECONDS: u64 = 600;

impl ScaleDownSpec {
    /// The effective `holdAfterPowerOnSeconds`: the new name, else the
    /// deprecated `delayAfterScaleUpSeconds`, else the default.
    pub fn hold_after_power_on(&self) -> u64 {
        self.hold_after_power_on_seconds
            .or(self.delay_after_scale_up_seconds)
            .unwrap_or(DEFAULT_HOLD_AFTER_POWER_ON_SECONDS)
    }

    /// Why the spec's use of the deprecated `delayAfterScaleUpSeconds`
    /// deserves a warning, if it does.
    pub fn deprecation_warning(&self) -> Option<String> {
        match (self.hold_after_power_on_seconds, self.delay_after_scale_up_seconds) {
            (Some(new), Some(old)) if new != old => Some(format!(
                "scaleDown.delayAfterScaleUpSeconds ({old}) is deprecated and ignored: scaleDown.holdAfterPowerOnSeconds ({new}) is also set and wins"
            )),
            (Some(_), Some(_)) => Some(
                "scaleDown.delayAfterScaleUpSeconds is deprecated and ignored: scaleDown.holdAfterPowerOnSeconds is also set and wins; remove the old name"
                    .to_string(),
            ),
            (None, Some(_)) => Some(
                "scaleDown.delayAfterScaleUpSeconds is deprecated; rename it to scaleDown.holdAfterPowerOnSeconds"
                    .to_string(),
            ),
            _ => None,
        }
    }
}
fn default_drain_backoff() -> u64 {
    1800
}
fn default_scale_down_step() -> u32 {
    1
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeScalingPoolStatus {
    /// NodePowerManagementConfigs currently in this pool.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<String>,
    /// NodePowerManagementConfigs whose Node also matches another pool; excluded from both.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<String>,
    #[serde(default)]
    pub total_nodes: u32,
    /// Machines that are on or powering on.
    #[serde(default)]
    pub online_nodes: u32,
    /// Unschedulable pods this pool could serve.
    #[serde(default)]
    pub pending_pods: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scale_up_time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scale_down_time: Option<DateTime<Utc>>,
    /// Since when each member (by NodePowerManagementConfig name) has been continuously unneeded.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub unneeded_since: BTreeMap<String, DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// `PreferredNodesValid`: whether every `scaleUp.preferredNodes` entry
    /// names an existing Node that this pool's `nodeSelector` selects.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    /// The last decisions of this pool's autoscaler, oldest first (at most
    /// `MAX_RECENT_DECISIONS`): every power action, and every change in why
    /// nothing is done. Kept here because Events expire after an hour.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_decisions: Vec<DecisionRecord>,
}

/// How many entries `NodeScalingPool.status.recentDecisions` keeps.
pub const MAX_RECENT_DECISIONS: usize = 20;

pub const COND_PREFERRED_NODES_VALID: &str = "PreferredNodesValid";

/// What a pool decided.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum DecisionAction {
    PowerOn,
    PowerOff,
    NoAction,
}

/// One entry of `NodeScalingPool.status.recentDecisions`.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct DecisionRecord {
    pub time: DateTime<Utc>,
    pub action: DecisionAction,
    /// The machine powered on or off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
    /// Why, including the rank and the rule that decided between candidates.
    pub reason: String,
    /// The ranked candidates, best first for power-on and first to go for
    /// power-off: `name(w<weight> [s3] <cpu>c/<memory>Gi) > ...`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub candidates: String,
}

/// Sets a condition in `conditions`, keeping `lastTransitionTime` unless the status changed.
pub fn set_condition(
    conditions: &mut Vec<Condition>,
    type_: &str,
    status: &str,
    reason: &str,
    message: impl Into<String>,
    now: DateTime<Utc>,
) {
    let status = status.to_string();
    let message = message.into();
    match conditions.iter_mut().find(|c| c.type_ == type_) {
        Some(c) => {
            if c.status != status {
                c.last_transition_time = now;
            }
            c.status = status;
            c.reason = reason.to_string();
            c.message = message;
        }
        None => conditions.push(Condition {
            type_: type_.to_string(),
            status,
            reason: reason.to_string(),
            message,
            last_transition_time: now,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// YAML 1.1 parsers (used by Kubernetes tooling) read bare `on`, `off`,
    /// `yes`, `no`, `y`, `n` as booleans. Values users write in manifests must
    /// never be one of those.
    #[test]
    fn user_facing_enum_values_survive_yaml_1_1() {
        const YAML11_BOOLS: &[&str] = &["y", "yes", "n", "no", "true", "false", "on", "off"];
        let values: Vec<String> = [
            serde_json::to_value([PowerPolicy::Auto, PowerPolicy::AlwaysOn, PowerPolicy::AlwaysOff]).unwrap(),
            serde_json::to_value([PowerOffMode::Shutdown, PowerOffMode::Standby, PowerOffMode::Auto]).unwrap(),
            serde_json::to_value([
                DecisionAction::PowerOn,
                DecisionAction::PowerOff,
                DecisionAction::NoAction,
            ])
            .unwrap(),
            serde_json::to_value([
                InterfaceAction::Status,
                InterfaceAction::PowerOn,
                InterfaceAction::PowerOff,
            ])
            .unwrap(),
        ]
        .iter()
        .flat_map(|v| {
            v.as_array()
                .unwrap()
                .iter()
                .map(|s| s.as_str().unwrap().to_ascii_lowercase())
        })
        .collect();
        for v in values {
            assert!(!YAML11_BOOLS.contains(&v.as_str()), "{v} is a YAML 1.1 boolean");
        }
    }

    fn scale_down(v: serde_json::Value) -> ScaleDownSpec {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn hold_after_power_on_accepts_the_deprecated_name() {
        let new = scale_down(serde_json::json!({"holdAfterPowerOnSeconds": 120}));
        assert_eq!(new.hold_after_power_on(), 120);
        assert_eq!(new.deprecation_warning(), None);

        let old = scale_down(serde_json::json!({"delayAfterScaleUpSeconds": 900}));
        assert_eq!(old.hold_after_power_on(), 900);
        assert!(old.deprecation_warning().unwrap().contains("rename"));

        // Both: the new name wins, with a warning.
        let both = scale_down(serde_json::json!({"holdAfterPowerOnSeconds": 120, "delayAfterScaleUpSeconds": 900}));
        assert_eq!(both.hold_after_power_on(), 120);
        assert!(both.deprecation_warning().unwrap().contains("wins"));

        for spec in [&old, &both] {
            let w = spec.deprecation_warning().unwrap();
            assert!(!w.contains("  "), "{w:?}");
        }
        let both_same =
            scale_down(serde_json::json!({"holdAfterPowerOnSeconds": 900, "delayAfterScaleUpSeconds": 900}));
        let w = both_same.deprecation_warning().unwrap();
        assert!(w.contains("wins") && !w.contains("  "), "{w:?}");

        let neither = scale_down(serde_json::json!({}));
        assert_eq!(neither.hold_after_power_on(), DEFAULT_HOLD_AFTER_POWER_ON_SECONDS);
        assert_eq!(neither.deprecation_warning(), None);
        // Neither name is written back when unset, so the CRD's `scaleDown`
        // default does not pin one.
        let written = serde_json::to_value(ScaleDownSpec::default()).unwrap();
        assert!(written.get("holdAfterPowerOnSeconds").is_none());
        assert!(written.get("delayAfterScaleUpSeconds").is_none());
    }

    fn pool_schema() -> serde_json::Value {
        use kube::CustomResourceExt;
        let crd = serde_json::to_value(NodeScalingPool::crd()).unwrap();
        crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"].clone()
    }

    #[test]
    fn crd_schema_keeps_both_names_without_defaults() {
        let down = &pool_schema()["properties"]["scaleDown"];
        for name in ["holdAfterPowerOnSeconds", "delayAfterScaleUpSeconds"] {
            let field = &down["properties"][name];
            assert_eq!(field["type"], "integer", "{name}");
            // A schema default would be filled in by the API server and, for the
            // new name, override an object's deprecated value.
            assert!(field.get("default").is_none(), "{name} must not have a default");
        }
        assert!(
            down["properties"]["delayAfterScaleUpSeconds"]["description"]
                .as_str()
                .unwrap()
                .starts_with("Deprecated")
        );
        assert!(down["default"].get("delayAfterScaleUpSeconds").is_none());
    }

    #[test]
    fn preferred_nodes_are_validated_and_default_to_zero() {
        let list = &pool_schema()["properties"]["scaleUp"]["properties"]["preferredNodes"];
        assert_eq!(list["type"], "array");
        assert_eq!(list["x-kubernetes-list-type"], "map", "names must be unique");
        assert_eq!(list["x-kubernetes-list-map-keys"], serde_json::json!(["name"]));
        let weight = &list["items"]["properties"]["weight"];
        assert_eq!(weight["minimum"].as_f64(), Some(0.0));
        assert_eq!(weight["maximum"].as_f64(), Some(100.0));

        let up: ScaleUpSpec = serde_json::from_value(serde_json::json!({
            "preferredNodes": [{"name": "devbox", "weight": 100}, {"name": "vulpes-zerda", "weight": 10}]
        }))
        .unwrap();
        assert_eq!(up.node_weight("devbox"), 100);
        assert_eq!(up.node_weight("vulpes-zerda"), 10);
        assert_eq!(up.node_weight("unlisted"), 0);
        assert!(
            serde_json::from_value::<ScaleUpSpec>(
                serde_json::json!({"preferredNodes": [{"name": "devbox", "weight": -1}]})
            )
            .is_err()
        );
    }
}
