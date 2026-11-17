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

    /// What to do when the machine is powered on outside the autoscaler (by
    /// hand, from its BMC, or by a power button). Overrides the pools'
    /// `manualPowerOnPolicy`; see `ManualPowerOnPolicy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_power_on_policy: Option<ManualPowerOnPolicy>,

    /// After the machine is powered off outside the autoscaler, do not wake it
    /// again for this many seconds. Overrides the pools'
    /// `manualPowerOffCooldownSeconds`; 0 (the default) disables it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_power_off_cooldown_seconds: Option<u64>,

    /// When the pools may run this machine (e.g. a loud server only in the
    /// daytime). Outside these hours no pool wakes it, and once it is on and
    /// idle it is powered off without waiting out `unneededSeconds`. Work
    /// already running is not interrupted: a machine stays on while it is busy
    /// by the pools' usual rules (utilization, blocking pods, logins), and a
    /// machine switched on by hand follows `manualPowerOnPolicy` as always.
    /// Only `powerPolicy: Auto` is affected. Always active when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_hours: Option<ActiveHours>,

    /// A periodic reachability check of the machine's management path (its
    /// BMC or KVM), recorded in `status.reachability` and as the metrics
    /// `kha_machine_probe_up` and `kha_machine_probe_rtt_seconds`. Pools with
    /// `scaleUp.strategy: Scored` would rather wake a machine whose
    /// management interface answers, and wake one that does not only when no
    /// other machine fits. When omitted, the address of the first interface
    /// that has one (a `redfish`, `pikvm` or `nanokvm` endpoint, an `ipmi`
    /// address) is probed; a machine with none (Wake-on-LAN only) is not
    /// probed. `{disabled: true}` turns it off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<ProbeSpec>,
}

/// `spec.probe`: how the machine's management path is checked.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProbeSpec {
    /// Do not probe this machine.
    #[serde(default)]
    pub disabled: bool,
    /// Host name or IP address to probe. Defaults to the first interface's
    /// management address (see `probe`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// `tcp` (connect to `port`; a refused connection still proves the host
    /// is up) or `icmp` (echo, IPv4 only; needs unprivileged ping sockets or
    /// `CAP_NET_RAW`). Defaults to `tcp`, or `icmp` for an `ipmi` address
    /// (IPMI is UDP).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<ProbeMethod>,
    /// TCP port. Defaults to the endpoint URL's port, else 443 for `https`
    /// and 80 for `http`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// Timeout per probe, in milliseconds (default 1000).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ProbeMethod {
    #[default]
    Tcp,
    Icmp,
}

/// The hours a machine may be run by the pools: active whenever any window is.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActiveHours {
    /// IANA time zone the windows are in, e.g. `America/Los_Angeles`, so
    /// they follow daylight saving. UTC when omitted. A name the operator
    /// does not know makes the machine count as outside its hours (it is not
    /// woken; a running machine is not forced off), with the error in the
    /// pools' decisions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
    #[schemars(length(min = 1))]
    pub windows: Vec<ActiveWindow>,
}

/// One daily window, `start` inclusive to `end` exclusive, in local time. An
/// `end` at or before `start` runs past midnight into the next day (`22:00` to
/// `06:00`); `00:00` to `24:00` is the whole day.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ActiveWindow {
    /// The days the window starts on. Every day when omitted or empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub days: Vec<Day>,
    /// `HH:MM`, 24-hour.
    #[schemars(regex(pattern = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"))]
    pub start: String,
    /// `HH:MM`, 24-hour, or `24:00`.
    #[schemars(regex(pattern = r"^(([01][0-9]|2[0-3]):[0-5][0-9]|24:00)$"))]
    pub end: String,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
pub enum Day {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

/// What the autoscaler does with a machine powered on outside it.
///
/// Whatever the policy, a stale scaling decision never acts on a manually
/// powered-on machine: it is cleared when the manual power-on is seen.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum ManualPowerOnPolicy {
    /// Leave it alone while it stays on: never power it off, and do not let
    /// it hold or block anything (it is not "booting", not counted towards
    /// `maxOnline`). It does count as room for pods evicted elsewhere once its
    /// Node is Ready. It returns to normal management once it is observed off
    /// again (e.g. powered off by hand), or when `status.manualPowerOn` is
    /// cleared. Keeping a machine off for maintenance is `powerPolicy:
    /// AlwaysOff`, not this.
    #[default]
    LeaveOn,
    /// Manage it as if its pool had powered it on: normal scale-down, with
    /// `holdAfterPowerOnSeconds` counted from the manual power-on.
    Adopt,
    /// Power it back off after the pool's `manualPowerOnGraceSeconds`, busy or
    /// not (pods are still drained, and safe-to-evict=false pods still block):
    /// for machines that must only ever be on when the autoscaler says so.
    PowerOff,
}

impl ManualPowerOnPolicy {
    /// How cautious the policy is; overlapping pools apply the most cautious.
    fn caution(self) -> u8 {
        match self {
            ManualPowerOnPolicy::LeaveOn => 2,
            ManualPowerOnPolicy::Adopt => 1,
            ManualPowerOnPolicy::PowerOff => 0,
        }
    }
}

/// The policy for a machine: its own `manualPowerOnPolicy`, else the most
/// cautious of its pools' (`LeaveOn` over `Adopt` over `PowerOff`), else
/// `LeaveOn`.
pub fn effective_manual_power_on_policy(
    machine: Option<ManualPowerOnPolicy>,
    pools: impl IntoIterator<Item = Option<ManualPowerOnPolicy>>,
) -> ManualPowerOnPolicy {
    machine.unwrap_or_else(|| {
        pools
            .into_iter()
            .flatten()
            .max_by_key(|p| p.caution())
            .unwrap_or_default()
    })
}

/// `lifecycle.bootTimeoutSeconds` when neither the machine nor any of its pools sets it.
pub const DEFAULT_BOOT_TIMEOUT_SECONDS: u64 = 1200;

/// The boot timeout for a machine: its own `lifecycle.bootTimeoutSeconds`,
/// else the longest of its pools' `scaleUp.bootTimeoutSeconds`, else 1200.
pub fn effective_boot_timeout(machine: Option<u64>, pools: impl IntoIterator<Item = Option<u64>>) -> u64 {
    machine
        .or_else(|| pools.into_iter().flatten().max())
        .unwrap_or(DEFAULT_BOOT_TIMEOUT_SECONDS)
}

/// What a pool does when a machine it asked to power on is still not on after
/// `powerOnTimeoutSeconds`. A machine that did turn on and is waiting for its
/// Node is the boot timeout's concern, not this.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum PowerOnFailurePolicy {
    /// Drop the power-on. The machine is recorded as a boot failure: it stops
    /// counting as capacity and is not powered on again until
    /// `bootFailureBackoffSeconds` has passed, so the next machine in the
    /// pool's order can be chosen. A forced power-off is issued only when an
    /// interface reads the machine as on. Repeating Wake-on-LAN does not keep
    /// the attempt open.
    #[default]
    Release,
    /// Keep issuing PowerOn, and keep counting the machine as capacity already
    /// on its way, until its Node is Ready or the decision is cleared.
    Retry,
}

/// The power-on failure rule for one attempt. The pool that made the decision
/// decides; its unset timeout uses `boot_timeout` (the machine's effective
/// boot timeout). With no owning pool, `Release` wins over `Retry` and the
/// shortest timeout wins.
pub fn effective_power_on_failure(
    decision_pool: Option<&str>,
    pools: &[(&str, &ScaleUpSpec)],
    boot_timeout: u64,
) -> (PowerOnFailurePolicy, u64) {
    let timeout_of = |spec: &ScaleUpSpec| spec.power_on_timeout_seconds.unwrap_or(boot_timeout);
    if let Some(name) = decision_pool
        && let Some((_, spec)) = pools.iter().find(|(pool, _)| *pool == name)
    {
        return (spec.power_on_failure_policy, timeout_of(spec));
    }
    if pools.is_empty() {
        return (PowerOnFailurePolicy::Release, boot_timeout);
    }
    let policy = if pools
        .iter()
        .any(|(_, spec)| spec.power_on_failure_policy == PowerOnFailurePolicy::Release)
    {
        PowerOnFailurePolicy::Release
    } else {
        PowerOnFailurePolicy::Retry
    };
    let timeout = pools
        .iter()
        .filter(|(_, spec)| spec.power_on_failure_policy == policy)
        .map(|(_, spec)| timeout_of(spec))
        .min()
        .unwrap_or(boot_timeout);
    (policy, timeout)
}

/// The manual power-off cooldown for a machine: its own, else the longest of
/// its pools', else 0 (none).
pub fn effective_manual_power_off_cooldown(machine: Option<u64>, pools: impl IntoIterator<Item = Option<u64>>) -> u64 {
    machine.or_else(|| pools.into_iter().flatten().max()).unwrap_or(0)
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
    /// Seconds a powered-on machine may take for its Node to become `Ready`,
    /// however it was powered on. Past it the machine counts as boot-failed
    /// (phase `BootFailed`): it no longer holds its pools' scale-down, is not
    /// counted as capacity, and is powered off (unless it was powered on by
    /// hand under `manualPowerOnPolicy: LeaveOn`) and not woken again for its
    /// pools' `scaleUp.bootFailureBackoffSeconds`. When unset, the longest of
    /// its pools' `scaleUp.bootTimeoutSeconds`, else 1200.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_timeout_seconds: Option<u64>,
    /// How long this machine must stay continuously unneeded before it is
    /// powered off, over its pools' `scaleDown.unneededSeconds` (each pool
    /// that selects it applies this instead of its own). For a machine that
    /// is cheap to wake again, or expensive to leave idle. The pools'
    /// `holdAfterPowerOnSeconds` and other holds still apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unneeded_seconds: Option<u64>,
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
    /// asked, standby is considered broken on it: it is shut down instead of
    /// suspended, and standby is not used again for a day
    /// (`status.standbyFailedAt`). A power-on that never turns the machine on
    /// is the pool's `powerOnFailurePolicy`, not another cold boot.
    #[serde(default)]
    pub power_off_mode: PowerOffMode,
    /// Never let a scale-down power off or suspend the machine while someone
    /// is logged in to it (a console or SSH login, as systemd-logind records
    /// it in `/run/systemd/sessions`). Checked by a small unprivileged pod on
    /// the node just before the Node is cordoned, and again just before the
    /// power-off. A login calls the scale-down off, records the sessions in
    /// `status.loginSessions`, and the pools keep the machine for 5 minutes
    /// before trying again. A check that cannot run also keeps it on.
    /// `powerPolicy: AlwaysOff` and boot-failure power-offs are not checked.
    #[serde(default)]
    pub keep_on_while_logged_in: bool,
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

fn default_drain_timeout() -> u64 {
    300
}
fn default_shutdown_timeout() -> u64 {
    300
}

impl Default for LifecycleSpec {
    fn default() -> Self {
        Self {
            boot_timeout_seconds: None,
            unneeded_seconds: None,
            drain_timeout_seconds: default_drain_timeout(),
            shutdown_timeout_seconds: default_shutdown_timeout(),
            force_after_drain_timeout: false,
            power_off_mode: PowerOffMode::default(),
            keep_on_while_logged_in: false,
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
    /// Powered on, but its Node did not become Ready within the boot timeout.
    BootFailed,
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
    /// The `NodeScalingPool` that made the decision (with overlapping pools,
    /// the one that woke or powered off the machine). A decision is ignored (and
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
    /// The `NodeScalingPool`s whose `nodeSelector` matches this machine's Node,
    /// comma-separated (for display; see `pools`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// The `NodeScalingPool`s whose `nodeSelector` matches this machine's
    /// Node, sorted. Pools may overlap: any of them may power the machine on,
    /// and it is powered off only when all of them agree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pools: Vec<String>,
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
    /// Who was logged in when a scale-down last found the machine in use
    /// (`lifecycle.keepOnWhileLoggedIn`). Cleared when a check finds nobody.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub login_sessions: Option<LoginSessions>,
    /// Set when the machine was seen powered on outside the autoscaler
    /// (powered off or in standby before, on now, and no PowerOn of ours).
    /// Cleared when it is next seen off. Clear it by hand to put a `LeaveOn`
    /// machine back under management while it is on:
    /// `kubectl patch npmc <name> --subresource=status --type=merge -p '{"status":{"manualPowerOn":null}}'`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_power_on: Option<ManualPowerChange>,
    /// The last time the machine was seen powered off outside the autoscaler.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_power_off: Option<ManualPowerChange>,
    /// Since when the machine has been powered on without its Node being Ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub powered_on_not_ready_since: Option<DateTime<Utc>>,
    /// The last boot failure (powered on, Node not Ready within the boot
    /// timeout). Cleared once the Node is Ready.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_failure: Option<BootFailure>,
    /// The machine's recent cold boots, newest last: how long each took from
    /// powering on to its Node being Ready, or that it failed. Pools with
    /// `scaleUp.strategy: Scored` learn from it which machines come up fast
    /// and reliably, and `scaleUp.hedge` how long a boot should take.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_history: Option<BootHistory>,
    /// The last checks of the management path (`spec.probe`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reachability: Option<Reachability>,
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
    /// Last-known non-zero allocatable of each extended resource (a device
    /// plugin's, such as `gpu.intel.com/i915`). Kept while the machine is off
    /// and while, after a boot, its device plugin has not registered the
    /// devices again: a machine woken for pods waiting for such a device is
    /// not taken for unneeded meanwhile.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extended_resources: BTreeMap<String, i64>,
    /// Human-readable details about the current state or the last error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// A power change made outside the autoscaler.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManualPowerChange {
    pub time: DateTime<Utc>,
    /// The `manualPowerOnPolicy` in effect when it was seen (power-on only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<ManualPowerOnPolicy>,
}

/// How many boots `status.bootHistory` keeps.
pub const BOOT_HISTORY_LEN: usize = 10;

/// `status.bootHistory`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BootHistory {
    /// Up to 10 boots, oldest first.
    #[serde(default)]
    pub boots: Vec<BootRecord>,
    /// Mean seconds to Ready of the successful boots listed (for display).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub average_seconds: Option<u64>,
    /// Successful and failed boots listed (for display).
    #[serde(default)]
    pub succeeded: u32,
    #[serde(default)]
    pub failed: u32,
}

/// One boot in `status.bootHistory`.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BootRecord {
    /// When the boot ended (Ready, or given up).
    pub time: DateTime<Utc>,
    /// Whether the Node became Ready.
    pub succeeded: bool,
    /// Seconds from powering on to Ready (successful boots only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<u64>,
}

impl BootHistory {
    /// Appends a boot, keeping the last `BOOT_HISTORY_LEN`, and refreshes the summary.
    pub fn record(&mut self, record: BootRecord) {
        self.boots.push(record);
        let excess = self.boots.len().saturating_sub(BOOT_HISTORY_LEN);
        self.boots.drain(..excess);
        let ok: Vec<u64> = self
            .boots
            .iter()
            .filter_map(|b| b.seconds.filter(|_| b.succeeded))
            .collect();
        self.succeeded = self.boots.iter().filter(|b| b.succeeded).count() as u32;
        self.failed = self.boots.len() as u32 - self.succeeded;
        self.average_seconds = (!ok.is_empty()).then(|| ok.iter().sum::<u64>() / ok.len() as u64);
    }
}

/// How many probes `status.reachability.recent` keeps.
pub const PROBE_HISTORY_LEN: usize = 10;

/// `status.reachability`.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Reachability {
    /// What was probed, e.g. `tcp 10.0.0.12:443`.
    pub target: String,
    /// Whether the last probe was answered.
    pub reachable: bool,
    /// Round-trip time of the last answered probe, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rtt_ms: Option<u64>,
    /// The last probes, oldest first: `1` answered, `0` not.
    #[serde(default)]
    pub recent: String,
    pub checked_at: DateTime<Utc>,
    /// Why the probe could not run (bad address, no ICMP socket), if so.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Reachability {
    /// Share of the recent probes that were answered, 0.0 to 1.0.
    pub fn answered(&self) -> f64 {
        if self.recent.is_empty() {
            return if self.reachable { 1.0 } else { 0.0 };
        }
        self.recent.chars().filter(|c| *c == '1').count() as f64 / self.recent.len() as f64
    }
}

/// A machine that was powered on but did not become Ready in time.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BootFailure {
    pub time: DateTime<Utc>,
    /// Left powered on (powered on by hand under `LeaveOn`) rather than
    /// powered off.
    #[serde(default)]
    pub left_on: bool,
}

/// Login sessions found on a machine that was about to be powered off.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoginSessions {
    pub checked_at: DateTime<Utc>,
    /// One entry per session, e.g. `alice on pts/0 from 10.0.0.5`, or why the
    /// check could not tell.
    pub sessions: Vec<String>,
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
/// Node matches `nodeSelector` is a member. Pools may overlap: a machine
/// selected by several pools is a member of each; any of them may power it on
/// for its own pending pods, and it is powered off only when every one of them
/// considers it unneeded (`status.releasable`).
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
    /// Default `manualPowerOnPolicy` for this pool's machines (a machine's own
    /// setting wins; with overlapping pools the most cautious applies).
    /// `LeaveOn` when unset everywhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_power_on_policy: Option<ManualPowerOnPolicy>,
    /// Under `manualPowerOnPolicy: PowerOff`, how long a manually powered-on
    /// machine may stay on before it is powered off again.
    #[serde(default = "default_manual_grace")]
    pub manual_power_on_grace_seconds: u64,
    /// Default `manualPowerOffCooldownSeconds` for this pool's machines: after
    /// a machine is powered off by hand, do not wake it for this long. 0 (the
    /// default) disables it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manual_power_off_cooldown_seconds: Option<u64>,
}

fn default_manual_grace() -> u64 {
    600
}

/// `scaleUp.bootFailureBackoffSeconds` default.
pub const DEFAULT_BOOT_FAILURE_BACKOFF_SECONDS: u64 = 1800;

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
    /// appear once. Without `strategies`, machines are powered on in this
    /// order, and powered off in exactly the reverse order (with
    /// `strategies`, this weight only counts through the `Priority`
    /// strategy, if it is listed):
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
    /// Default `lifecycle.bootTimeoutSeconds` for this pool's machines (a
    /// machine's own setting wins; with overlapping pools the longest
    /// applies). 1200 when unset everywhere.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_timeout_seconds: Option<u64>,
    /// After a machine fails to boot, do not power it on again for this long.
    #[serde(default = "default_boot_failure_backoff")]
    pub boot_failure_backoff_seconds: u64,
    /// How long a power-on may stay short of the machine actually being on
    /// before `powerOnFailurePolicy` applies. The clock is the scaling
    /// decision's time; repeating the PowerOn command does not restart it.
    /// Covers a machine that never turns on and a suspended machine that does
    /// not wake. A machine that is on and posting is timed by
    /// `bootTimeoutSeconds` instead. Unset: that machine's effective boot
    /// timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_on_timeout_seconds: Option<u64>,
    /// What to do when `powerOnTimeoutSeconds` elapses. `Release` (the
    /// default) gives the attempt up so another machine can be powered on.
    /// `Retry` keeps the attempt, and the capacity hold, open.
    #[serde(default)]
    pub power_on_failure_policy: PowerOnFailurePolicy,
    /// How the machine to wake is chosen among those that are off and fit:
    /// strategies, each rating every candidate from 0 to 1, combined by
    /// weight; the highest total is woken first, and scale-down powers off
    /// in the reverse order. Ties go to the larger machine, then by name.
    /// Constraints are never traded away: active hours, backoffs,
    /// `maxOnline` and `maxNodesPerStep` apply before any strategy, and a
    /// machine that is not off and `Auto`, or that the pod does not fit, is
    /// never a candidate.
    ///
    /// Empty (the default): the fixed order described under `preferredNodes`.
    ///
    /// ```yaml
    /// strategies:
    ///   - {name: Reachable, weight: 3}
    ///   - {name: FastestBoot, weight: 2}
    ///   - {name: Reliable, weight: 2}
    ///   - {name: Random, weight: 1}
    /// ```
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "strategies_schema")]
    pub strategies: Vec<WeightedStrategy>,
    /// Wake another machine when the one booting for pending pods is taking
    /// longer than it should. Off when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hedge: Option<HedgeSpec>,
}

/// One entry of `scaleUp.strategies`.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WeightedStrategy {
    pub name: Strategy,
    /// 1 to 100: how much this strategy counts against the others.
    pub weight: u32,
}

/// A way of choosing which machine to wake. Each rates a machine from 0 to 1.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
pub enum Strategy {
    /// The machine's `preferredNodes` weight, as a fraction of 100.
    Priority,
    /// Whoever is up on the network: the share of its recent management
    /// probes (`spec.probe`) answered, scaled down by round-trip time
    /// (`1 - rtt / 1000ms`, at least 0.5 when answered); 0.5 when it is not
    /// probed. With this strategy listed, a machine whose management
    /// interface did not answer its last probe is woken only when no other
    /// machine fits.
    Reachable,
    /// How fast its recent boots reached Ready (`status.bootHistory`):
    /// `120 / (120 + seconds)`, so 0.5 at two minutes; 0.5 with no history.
    FastestBoot,
    /// The share of its recent boots that succeeded, smoothed so a single
    /// boot is not certainty: `(succeeded + 1) / (boots + 2)`.
    Reliable,
    /// 1 when it is in standby or can sleep in S3 (see `preferS3Capable`):
    /// back in seconds instead of a full boot.
    Standby,
    /// CPU (then memory) relative to the largest candidate: fewer machines
    /// absorb the pending pods.
    Largest,
    /// The reverse of `Largest`: the least capacity, and power, left idle.
    Smallest,
    /// A fresh random number per candidate every time the pool decides:
    /// spreads wakes, and wear, over interchangeable machines.
    Random,
    /// How much of its `activeHours` window is left once it has booted:
    /// `(left - expected boot) / 1h`, so 1 with an hour or more to work, 0
    /// when it would close before it is Ready. The expected boot is its
    /// average (`status.bootHistory`), else 5 minutes. 1 for a machine
    /// without active hours. With this strategy listed, a machine whose
    /// window closes before it would be Ready is woken only when no other
    /// machine fits.
    ActiveHoursRemaining,
}

fn strategies_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        "x-kubernetes-list-type": "map",
        "x-kubernetes-list-map-keys": ["name"],
        "items": {
            "type": "object",
            "required": ["name", "weight"],
            "properties": {
                "name": {
                    "type": "string",
                    "enum": ["Priority", "Reachable", "FastestBoot", "Reliable", "Standby", "Largest", "Smallest", "Random", "ActiveHoursRemaining"]
                },
                "weight": {
                    "description": "1 to 100: how much this strategy counts against the others.",
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100
                }
            }
        }
    })
}

/// `scaleUp.hedge`.
#[derive(Deserialize, Serialize, Clone, Copy, Debug, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HedgeSpec {
    /// A boot is overdue at this percentage of the machine's average boot
    /// time (`status.bootHistory`). Default 150.
    #[serde(default = "default_hedge_percent")]
    #[schemars(range(min = 100))]
    pub percent_of_average: u32,
    /// Never overdue before this many seconds. Default 120.
    #[serde(default = "default_hedge_min")]
    pub min_seconds: u64,
    /// Overdue after this many seconds for a machine with no boot history.
    /// Default 300.
    #[serde(default = "default_hedge_unknown")]
    pub without_history_seconds: u64,
}

fn default_hedge_percent() -> u32 {
    150
}
fn default_hedge_min() -> u64 {
    120
}
fn default_hedge_unknown() -> u64 {
    300
}

impl Default for HedgeSpec {
    fn default() -> Self {
        Self {
            percent_of_average: default_hedge_percent(),
            min_seconds: default_hedge_min(),
            without_history_seconds: default_hedge_unknown(),
        }
    }
}

impl HedgeSpec {
    /// Seconds after which a boot is overdue, given the machine's average.
    pub fn overdue_after(&self, average_seconds: Option<f64>) -> i64 {
        match average_seconds {
            Some(avg) => ((avg * self.percent_of_average as f64 / 100.0).ceil() as i64).max(self.min_seconds as i64),
            None => self.without_history_seconds.max(self.min_seconds) as i64,
        }
    }
}

fn default_boot_failure_backoff() -> u64 {
    DEFAULT_BOOT_FAILURE_BACKOFF_SECONDS
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
            boot_timeout_seconds: None,
            boot_failure_backoff_seconds: DEFAULT_BOOT_FAILURE_BACKOFF_SECONDS,
            power_on_timeout_seconds: None,
            power_on_failure_policy: PowerOnFailurePolicy::default(),
            strategies: Vec::new(),
            hedge: None,
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
    /// Unused since pools may overlap (always empty); kept so older objects validate.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conflicts: Vec<String>,
    /// Members this pool agrees may be powered off now: unneeded for
    /// `unneededSeconds`, with nothing holding the pool's scale-down (pending
    /// pods, `holdAfterPowerOnSeconds`, machines booting, `minOnline`). Another
    /// pool selecting the same machine powers it off only if it is listed here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub releasable: Vec<String>,
    /// Why this pool keeps each other online member on ("busy: utilization
    /// 40% >= 10%", "2 pending pod(s)", "unneeded since ..."); what another pool
    /// selecting the same machine reports as "needed by pool <this>".
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub needed: BTreeMap<String, String>,
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
            serde_json::to_value([PowerOnFailurePolicy::Release, PowerOnFailurePolicy::Retry]).unwrap(),
            serde_json::to_value([PowerOffMode::Shutdown, PowerOffMode::Standby, PowerOffMode::Auto]).unwrap(),
            serde_json::to_value([
                ManualPowerOnPolicy::LeaveOn,
                ManualPowerOnPolicy::Adopt,
                ManualPowerOnPolicy::PowerOff,
            ])
            .unwrap(),
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

    #[test]
    fn machine_settings_override_pool_defaults() {
        use ManualPowerOnPolicy::*;
        assert_eq!(effective_manual_power_on_policy(None, []), LeaveOn);
        assert_eq!(effective_manual_power_on_policy(None, [Some(Adopt), None]), Adopt);
        // Overlapping pools: the most cautious wins.
        assert_eq!(
            effective_manual_power_on_policy(None, [Some(PowerOff), Some(Adopt)]),
            Adopt
        );
        assert_eq!(
            effective_manual_power_on_policy(None, [Some(Adopt), Some(LeaveOn)]),
            LeaveOn
        );
        // The machine's own setting wins over every pool.
        assert_eq!(
            effective_manual_power_on_policy(Some(PowerOff), [Some(LeaveOn)]),
            PowerOff
        );

        assert_eq!(effective_boot_timeout(None, []), 1200);
        assert_eq!(effective_boot_timeout(None, [Some(600), Some(900)]), 900);
        assert_eq!(effective_boot_timeout(Some(300), [Some(900)]), 300);

        assert_eq!(effective_manual_power_off_cooldown(None, [None]), 0);
        assert_eq!(effective_manual_power_off_cooldown(None, [Some(60)]), 60);
        assert_eq!(effective_manual_power_off_cooldown(Some(0), [Some(60)]), 0);
    }

    #[test]
    fn the_pool_that_asked_for_power_on_decides_the_failure() {
        let release = ScaleUpSpec {
            power_on_failure_policy: PowerOnFailurePolicy::Release,
            power_on_timeout_seconds: Some(600),
            ..ScaleUpSpec::default()
        };
        let retry = ScaleUpSpec {
            power_on_failure_policy: PowerOnFailurePolicy::Retry,
            power_on_timeout_seconds: None,
            ..ScaleUpSpec::default()
        };
        let pools = [("ci", &release), ("gpu", &retry)];
        assert_eq!(
            effective_power_on_failure(Some("ci"), &pools, 1200),
            (PowerOnFailurePolicy::Release, 600)
        );
        // Unset timeout: the machine's boot timeout.
        assert_eq!(
            effective_power_on_failure(Some("gpu"), &pools, 1200),
            (PowerOnFailurePolicy::Retry, 1200)
        );
        // No owning pool: Release, and the shortest of those timeouts.
        assert_eq!(
            effective_power_on_failure(None, &pools, 1200),
            (PowerOnFailurePolicy::Release, 600)
        );
        assert_eq!(
            effective_power_on_failure(None, &[], 900),
            (PowerOnFailurePolicy::Release, 900)
        );
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
            "preferredNodes": [{"name": "gpu-node-1", "weight": 100}, {"name": "gpu-node-2", "weight": 10}]
        }))
        .unwrap();
        assert_eq!(up.node_weight("gpu-node-1"), 100);
        assert_eq!(up.node_weight("gpu-node-2"), 10);
        assert_eq!(up.node_weight("unlisted"), 0);
        assert!(
            serde_json::from_value::<ScaleUpSpec>(
                serde_json::json!({"preferredNodes": [{"name": "gpu-node-1", "weight": -1}]})
            )
            .is_err()
        );
    }
}

// ---------------------------------------------------------------------------
// PowerRequest
// ---------------------------------------------------------------------------

/// A request from an application to have one machine powered on for a while:
/// the pools wake it and keep it on until the request expires or is deleted,
/// after which their usual idle rules apply.
///
/// Namespaced, so RBAC decides which applications may ask. Setting
/// `overrideActiveHours` needs `create` on the `powerrequests/overrideactivehours`
/// subresource as well (checked by the chart's ValidatingAdmissionPolicy).
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    group = "hardware-autoscaler.safewords.com",
    version = "v1alpha1",
    kind = "PowerRequest",
    plural = "powerrequests",
    shortname = "pwr",
    namespaced,
    status = "PowerRequestStatus",
    validation = kube::core::Rule::new("has(self.spec.duration) != has(self.spec.until)").message("set exactly one of spec.duration and spec.until"),
    printcolumn = r#"{"name":"Node","type":"string","jsonPath":".spec.nodeName"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Expires","type":"date","jsonPath":".status.expiresAt"}"#,
    printcolumn = r#"{"name":"Reason","type":"string","jsonPath":".spec.reason","priority":1}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct PowerRequestSpec {
    /// The machine: its Node name (and NodePowerManagementConfig name).
    #[schemars(length(min = 1))]
    pub node_name: String,
    /// How long to keep it on, counted from when its Node is Ready: `90m`,
    /// `2h`, `1h30m`, `45s`. Exactly one of `duration` and `until`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(regex(pattern = r"^([0-9]+h)?([0-9]+m)?([0-9]+s)?$"), length(min = 2))]
    pub duration: Option<String>,
    /// Keep it on until this time (RFC 3339). Exactly one of `duration` and
    /// `until`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<DateTime<Utc>>,
    /// Why, for the pools' decisions and Events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Wake and keep the machine even outside its `activeHours`. Without it,
    /// a request for a machine outside its hours is refused, and an active
    /// one ends when its hours do.
    #[serde(default)]
    pub override_active_hours: bool,
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
pub enum PowerRequestPhase {
    /// Not looked at yet.
    #[default]
    Pending,
    /// Accepted; the machine is being woken (or its Node is not Ready yet).
    PoweringOn,
    /// The machine is on and Ready, and kept on until `expiresAt`.
    Active,
    /// Over: `expiresAt` passed, or the machine's active hours ended.
    Expired,
    /// Not accepted (see `message`); the machine was not touched for it.
    Refused,
    /// Accepted, but the machine did not become Ready in time.
    Failed,
}

impl PowerRequestPhase {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            PowerRequestPhase::Expired | PowerRequestPhase::Refused | PowerRequestPhase::Failed
        )
    }
}

#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PowerRequestStatus {
    #[serde(default)]
    pub phase: PowerRequestPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// When the machine's Node was first seen Ready for this request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<DateTime<Utc>>,
    /// When the request stops holding the machine on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    /// When it reached Expired, Refused or Failed; deleted an hour later.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}
