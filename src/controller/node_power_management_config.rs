//! `NodePowerManagementConfig` controller: drives a machine towards its desired power state.
//!
//! ```text
//!             power on                 node Ready
//!   Off ─────────────────▶ PoweringOn ───────────▶ On
//!    ▲                                              │ cordon
//!    │ power reads Off                              ▼
//! PoweringOff ◀──────────────────────────────── Draining
//!    (graceful, forced after shutdownTimeout)   (evict pods)
//! ```
//!
//! With `lifecycle.powerOffMode: Standby`, `PoweringOff` suspends the machine
//! in-band instead and ends in `Standby` (rather than `Off`) once the power
//! reads Off or the Node stops being Ready. `Auto` does the same in ACPI S3 if
//! a probe finds the kernel offers it, and shuts down otherwise.
//!
//! The desired state comes from `spec.powerPolicy` (`AlwaysOn`/`AlwaysOff`), or with
//! `Auto` from `status.scalingDecision`, written by the `NodeScalingPool` controller.
//!
//! Before any power action four safety gates must pass: the Node exists, pool
//! membership is resolved from its labels (stale decisions are dropped), each
//! interface proves it controls this machine (system UUID) or is disabled, and
//! the interface's power reading does not contradict a live Node.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::api::EvictParams;
use kube::runtime::controller::Action;
use kube::runtime::events::EventType;
use kube::{Api, ResourceExt};
use tracing::{info, warn};

use super::{
    Context, Error, clear_powered_off, cordon, mark_powered_off, node_ready, patch_status_diff, uncordon_if_ours,
};
use crate::crd::{
    BootFailure, COND_IDENTITY_VERIFIED, COND_NODE_FOUND, COND_POOL_MEMBERSHIP, COND_POWER_STATE_CONSISTENT,
    ManualPowerChange, ManualPowerOnPolicy, NodePowerManagementConfig, NodePowerManagementConfigStatus,
    NodeScalingPool, Phase, PowerActionRecord, PowerOffMode, PowerPolicy, PowerState, PowerTarget, ScalingDecision,
    SleepSupport, effective_boot_timeout, effective_manual_power_on_policy,
};
use crate::drivers::inband::{self, HostCommand, Probe};
use crate::drivers::{Outcome, PowerChain};
use crate::identity::IdentityCheck;
use crate::membership::{self, Membership};
use crate::resources::node_allocatable;
use crate::scaling::{DrainClass, drain_class};

const FAST: Duration = Duration::from_secs(10);
const NORMAL: Duration = Duration::from_secs(30);
const SLOW: Duration = Duration::from_secs(60);
/// Minimum time between repeated power commands while waiting for a transition.
const RETRY_ACTION_AFTER: i64 = 90;
/// A machine still up this long after being suspended did not go to sleep
/// (or woke straight back up). Capped by `shutdownTimeoutSeconds`.
const STANDBY_SETTLE_SECS: i64 = 240;
/// After standby failed on a machine, it is shut down instead for this long.
const STANDBY_BACKOFF_SECS: i64 = 24 * 3600;
/// How long a drained machine waits for its sleep probe before shutting down
/// (`powerOffMode: Auto`).
const PROBE_WAIT_SECS: i64 = 60;

/// The state to drive towards. With `Auto`, only a decision from a pool the
/// machine currently belongs to counts (stale decisions are cleared earlier).
pub fn desired_target(policy: PowerPolicy, st: &NodePowerManagementConfigStatus) -> Option<PowerTarget> {
    match policy {
        PowerPolicy::AlwaysOn => Some(PowerTarget::On),
        PowerPolicy::AlwaysOff => Some(PowerTarget::Off),
        PowerPolicy::Auto => st
            .scaling_decision
            .as_ref()
            .filter(|d| st.pools.contains(&d.pool))
            .map(|d| d.target),
    }
}

/// Waking up this soon after being suspended counts as standby not holding
/// (spontaneous) rather than as a manual power-on.
const SPONTANEOUS_WAKE_SECS: i64 = 300;

/// A power change the autoscaler did not make.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManualChange {
    None,
    /// Was off (or in standby), is on now, and no PowerOn of ours explains it.
    PoweredOn,
    /// Was on, reads off now, and no power-off of ours explains it.
    PoweredOff,
}

/// Compares the previous status with the fresh readings in `st` (power state,
/// Node readiness, last power action) to find power changes made by someone
/// else. Our own transitions always pass through `PoweringOn`/`PoweringOff`
/// (and are recorded in `lastPowerAction`), so they are never taken for manual.
pub fn detect_manual_change(
    old: &NodePowerManagementConfigStatus,
    st: &NodePowerManagementConfigStatus,
    now: DateTime<Utc>,
    boot_timeout_secs: u64,
    shutdown_timeout_secs: u64,
) -> ManualChange {
    let age = |actions: &[&str]| {
        st.last_power_action
            .as_ref()
            .filter(|a| actions.contains(&a.action.as_str()))
            .map(|a| (now - a.time).num_seconds())
    };
    let our_on = age(&["PowerOn"]).is_some_and(|s| s <= boot_timeout_secs as i64);
    let our_off = age(&["PowerOff", "ForceOff", "Standby"]).is_some_and(|s| s <= shutdown_timeout_secs as i64 + 300);
    let on_now = st.power_state == PowerState::On || st.node_ready;
    let off_now = st.power_state == PowerState::Off && !st.node_ready;
    match old.phase {
        Phase::Off if on_now && !our_on => ManualChange::PoweredOn,
        Phase::Standby
            if st.node_ready
                && !our_on
                && old
                    .standby_since
                    .is_none_or(|t| (now - t).num_seconds() > SPONTANEOUS_WAKE_SECS) =>
        {
            ManualChange::PoweredOn
        }
        Phase::On | Phase::PoweringOn | Phase::BootFailed
            if old.power_state == PowerState::On && off_now && !our_off =>
        {
            ManualChange::PoweredOff
        }
        _ => ManualChange::None,
    }
}

/// What `track_boot` found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootEvent {
    None,
    /// Powered on, Node not Ready past the boot timeout (just now).
    Failed,
    /// The Node of a machine that had failed to boot is Ready after all.
    Recovered,
}

/// Tracks how long the machine has been powered on without its Node being
/// Ready, however it was powered on, and records a boot failure past
/// `boot_timeout_secs`. A new boot after a failure (it was off in between)
/// can fail again.
pub fn track_boot(st: &mut NodePowerManagementConfigStatus, now: DateTime<Utc>, boot_timeout_secs: u64) -> BootEvent {
    if st.node_ready {
        st.powered_on_not_ready_since = None;
        return match st.boot_failure.take() {
            Some(_) => BootEvent::Recovered,
            None => BootEvent::None,
        };
    }
    let booting = st.power_state == PowerState::On
        && st.standby_since.is_none()
        && !matches!(st.phase, Phase::PoweringOff | Phase::Draining | Phase::Standby);
    if !booting {
        if st.power_state == PowerState::Off {
            st.powered_on_not_ready_since = None;
        }
        return BootEvent::None;
    }
    // First seen booting: since our own PowerOn if one started this phase.
    let start = match st.phase {
        Phase::PoweringOn | Phase::BootFailed => st.phase_since.unwrap_or(now).min(now),
        _ => now,
    };
    let since = *st.powered_on_not_ready_since.get_or_insert(start);
    let new_boot = st.boot_failure.as_ref().is_none_or(|f| since > f.time);
    if new_boot && (now - since).num_seconds() > boot_timeout_secs as i64 {
        st.boot_failure = Some(BootFailure {
            time: now,
            left_on: false,
        });
        return BootEvent::Failed;
    }
    BootEvent::None
}

/// Whether the recorded boot failure belongs to the boot in progress: the
/// machine has been on without Ready since before the failure was recorded.
/// A later boot (it was off in between, or a person powered it on) is a new
/// boot, which the old failure must never act on.
pub fn failure_is_current(st: &NodePowerManagementConfigStatus) -> bool {
    match (&st.boot_failure, st.powered_on_not_ready_since) {
        (Some(f), Some(since)) => since <= f.time,
        _ => false,
    }
}

/// Whether a boot failure leaves the machine on: only when it was powered on
/// by hand (not by us) and the policy is `LeaveOn`; a human may be at it.
pub fn boot_failure_leaves_on(st: &NodePowerManagementConfigStatus, policy: ManualPowerOnPolicy) -> bool {
    let since = st.powered_on_not_ready_since;
    let ours = st
        .last_power_action
        .as_ref()
        .is_some_and(|a| a.action == "PowerOn" && since.is_none_or(|s| a.time >= s - chrono::Duration::seconds(120)));
    !ours && policy == ManualPowerOnPolicy::LeaveOn
}

/// The state to drive towards, after the manual power-on policy: a machine
/// powered on by hand under `LeaveOn` is only observed, whatever decision is
/// stored.
pub fn managed_target(
    policy: PowerPolicy,
    st: &NodePowerManagementConfigStatus,
    manual: ManualPowerOnPolicy,
) -> Option<PowerTarget> {
    if policy == PowerPolicy::Auto && st.manual_power_on.is_some() && manual == ManualPowerOnPolicy::LeaveOn {
        return None;
    }
    desired_target(policy, st)
}

struct Reconciler<'a> {
    mn: &'a NodePowerManagementConfig,
    ctx: &'a Context,
    chain: PowerChain,
    node: Option<Arc<Node>>,
    st: NodePowerManagementConfigStatus,
    now: DateTime<Utc>,
    /// Effective `lifecycle.bootTimeoutSeconds` (machine, else pools, else default).
    boot_timeout: u64,
    /// Effective `manualPowerOnPolicy`.
    manual_policy: ManualPowerOnPolicy,
}

impl Reconciler<'_> {
    fn set_phase(&mut self, phase: Phase) {
        if self.st.phase != phase {
            info!(nodepowermanagementconfig = %self.mn.name_any(), from = ?self.st.phase, to = ?phase, "phase change");
            self.st.phase = phase;
            self.st.phase_since = Some(self.now);
        }
    }

    fn in_phase_for(&self) -> i64 {
        self.st
            .phase_since
            .map(|t| (self.now - t).num_seconds())
            .unwrap_or(i64::MAX)
    }

    fn since_last_action(&self, action: &str) -> i64 {
        match &self.st.last_power_action {
            Some(a) if a.action == action => (self.now - a.time).num_seconds(),
            _ => i64::MAX,
        }
    }

    async fn act(&mut self, action: &str) -> bool {
        let name = self.mn.name_any();
        let result = if self.ctx.dry_run {
            info!(nodepowermanagementconfig = %name, action, "dry-run: not executing power action");
            Ok(Outcome {
                value: (),
                via: "dry-run".into(),
                failures: vec![],
            })
        } else {
            match action {
                "PowerOn" => self.chain.power_on().await,
                "PowerOff" => self.chain.power_off(false).await,
                // No management interface can suspend a machine; it is always done in-band.
                "Standby" => {
                    let d = &self.ctx.drivers;
                    let node = &self.mn.spec.node_name;
                    let cmd = match self.mn.spec.lifecycle.power_off_mode {
                        PowerOffMode::Auto => HostCommand::SuspendS3,
                        _ => HostCommand::Suspend,
                    };
                    inband::run(&d.client, &d.namespace, node, &d.shutdown_image, cmd)
                        .await
                        .map(|()| Outcome {
                            value: (),
                            via: "in-band".into(),
                            failures: vec![],
                        })
                }
                _ => self.chain.power_off(true).await,
            }
        };
        match action {
            "Standby" => self.st.standby_since = Some(self.now),
            "PowerOff" => self.st.standby_since = None,
            _ => {}
        }
        let ok = result.is_ok();
        self.ctx.metrics.power_action(&self.mn.spec.node_name, action, ok);
        self.st.last_power_action = Some(PowerActionRecord {
            action: action.into(),
            time: self.now,
            succeeded: ok,
            via: result.as_ref().ok().map(|o| o.via.clone()),
        });
        match result {
            Ok(outcome) => {
                info!(nodepowermanagementconfig = %name, action, via = %outcome.via, "power action issued");
                let mut note = format!("{action} issued via {}", outcome.via);
                if !outcome.failures.is_empty() {
                    note.push_str(&format!(" after: {}", outcome.failures.join("; ")));
                }
                self.ctx.event(self.mn, EventType::Normal, action, note).await;
            }
            Err(e) => {
                warn!(nodepowermanagementconfig = %name, action, error = %e, "power action failed");
                self.st.message = Some(format!("{action} failed: {e}"));
                self.ctx
                    .event(self.mn, EventType::Warning, &format!("{action}Failed"), e.to_string())
                    .await;
            }
        }
        ok
    }

    async fn ensure_on(&mut self) -> Result<Duration, Error> {
        let power = self.st.power_state;
        if power == PowerState::On && self.st.node_ready {
            if let Some(node) = &self.node
                && uncordon_if_ours(&self.ctx.client, node).await?
            {
                info!(node = %self.mn.spec.node_name, "uncordoned node");
            }
            if let Some(node) = &self.node
                && clear_powered_off(&self.ctx.client, node).await?
            {
                info!(node = %self.mn.spec.node_name, "cleared powered-off marker (and any out-of-service fence)");
            }
            if self.st.phase != Phase::On {
                self.ctx
                    .event(
                        self.mn,
                        EventType::Normal,
                        "NodeOnline",
                        format!("node {} is Ready", self.mn.spec.node_name),
                    )
                    .await;
            }
            self.set_phase(Phase::On);
            self.st.forced_off_at = None;
            self.st.standby_since = None;
            self.st.message = None;
            if self.mn.spec.lifecycle.power_off_mode == PowerOffMode::Auto {
                // Find out early, so a scale down does not have to wait for it.
                self.probe_sleep().await;
            }
            return Ok(SLOW);
        }
        let boot_timeout = self.boot_timeout as i64;
        match self.st.phase {
            Phase::PoweringOn => {
                if self.in_phase_for() > boot_timeout && self.st.standby_since.is_some() {
                    // Asleep but not answering the wake-up: cut the power so the
                    // next power on is a cold boot. Keep standby_since so the
                    // wake-ups continue should the force off not be possible.
                    self.standby_failed(format!("machine did not wake from standby within {boot_timeout}s"))
                        .await;
                    self.act("ForceOff").await;
                    self.set_phase(Phase::Error);
                    self.st.message = Some(format!(
                        "did not wake from standby within {boot_timeout}s; forced off for a cold boot"
                    ));
                } else if (power == PowerState::Off || self.st.standby_since.is_some())
                    && self.since_last_action("PowerOn") > RETRY_ACTION_AFTER
                {
                    // Some BMCs read a suspended machine as On, so after a standby
                    // keep asking until the Node is Ready.
                    self.act("PowerOn").await;
                }
                Ok(FAST)
            }
            // Unlike a shutdown, a standby can be called off: withdraw the
            // suspend pod if it has not run yet, and wake the machine if it has.
            Phase::PoweringOff | Phase::Standby if self.st.standby_since.is_some() => {
                if let Err(e) =
                    inband::cancel(&self.ctx.client, &self.ctx.drivers.namespace, &self.mn.spec.node_name).await
                {
                    warn!(node = %self.mn.spec.node_name, error = %e, "cannot delete suspend pod");
                }
                if self.act("PowerOn").await {
                    self.set_phase(Phase::PoweringOn);
                } else {
                    self.set_phase(Phase::Error);
                }
                Ok(FAST)
            }
            // A graceful shutdown cannot be cancelled: wait for it to finish, then power back on.
            Phase::PoweringOff
                if power == PowerState::On
                    && self.in_phase_for() <= self.mn.spec.lifecycle.shutdown_timeout_seconds as i64 =>
            {
                self.st.message = Some("waiting for shutdown to complete before powering back on".into());
                Ok(FAST)
            }
            _ if power == PowerState::On && self.st.standby_since.is_none() => {
                // Powered but not Ready yet (booting, or an aborted scale down).
                if let Some(node) = &self.node {
                    uncordon_if_ours(&self.ctx.client, node).await?;
                }
                self.set_phase(Phase::PoweringOn);
                Ok(FAST)
            }
            _ => {
                if self.st.phase == Phase::Error && self.since_last_action("PowerOn") < RETRY_ACTION_AFTER {
                    return Ok(NORMAL);
                }
                if self.act("PowerOn").await {
                    self.set_phase(Phase::PoweringOn);
                } else {
                    self.set_phase(Phase::Error);
                }
                Ok(FAST)
            }
        }
    }

    /// The action that takes this machine offline.
    fn off_action(&self) -> &'static str {
        off_action(
            self.mn.spec.lifecycle.power_off_mode,
            self.st.standby_failed_at,
            self.s3_known(),
            self.now,
        )
    }

    fn boot_id(&self) -> Option<String> {
        let id = self.node.as_ref()?.status.as_ref()?.node_info.as_ref()?.boot_id.clone();
        (!id.is_empty()).then_some(id)
    }

    /// Whether the machine can sleep in S3, if probed during its current boot.
    fn s3_known(&self) -> Option<bool> {
        let boot = self.boot_id()?;
        self.st
            .sleep_support
            .as_ref()
            .filter(|s| s.boot_id == boot)
            .map(|s| s.s3)
    }

    /// Advances the sleep probe for the current boot; `None` while pending.
    async fn probe_sleep(&mut self) -> Option<bool> {
        if let Some(known) = self.s3_known() {
            return Some(known);
        }
        let boot = self.boot_id()?;
        let d = &self.ctx.drivers;
        let (boot_id, s3, detail) =
            match inband::probe_sleep(&d.client, &d.namespace, &self.mn.spec.node_name, &d.shutdown_image).await {
                Ok(Probe::Pending) => return None,
                // Ran before a reboot: probe again.
                Ok(Probe::Done { boot_id, .. }) if boot_id != boot => return None,
                Ok(Probe::Done { boot_id, mem_sleep }) => (boot_id, inband::offers_s3(&mem_sleep), mem_sleep),
                Ok(Probe::Failed(e)) => (boot, false, format!("probe failed: {e}")),
                Err(e) => {
                    warn!(node = %self.mn.spec.node_name, error = %e, "sleep probe failed");
                    (boot, false, format!("probe failed: {e}"))
                }
            };
        let note = if s3 {
            format!("machine can sleep in S3 (mem_sleep: {detail}); idle periods use standby")
        } else {
            format!("machine cannot sleep in S3 (mem_sleep: {detail}); idle periods shut it down")
        };
        info!(nodepowermanagementconfig = %self.mn.name_any(), s3, detail = %detail, "sleep probe");
        self.ctx.event(self.mn, EventType::Normal, "SleepProbe", note).await;
        self.st.sleep_support = Some(SleepSupport { boot_id, s3, detail });
        Some(s3)
    }

    /// Records that standby does not work on this machine right now; it is
    /// shut down instead until the backoff expires.
    async fn standby_failed(&mut self, why: String) {
        self.st.standby_failed_at = Some(self.now);
        warn!(nodepowermanagementconfig = %self.mn.name_any(), reason = %why, "standby failed");
        let note = format!(
            "{why}; shutting down instead of standby for the next {}h",
            STANDBY_BACKOFF_SECS / 3600
        );
        self.ctx.event(self.mn, EventType::Warning, "StandbyFailed", note).await;
    }

    async fn ensure_off(&mut self) -> Result<Duration, Error> {
        let power = self.st.power_state;
        if self.st.phase == Phase::Standby
            && self.st.standby_since.is_some()
            && self.st.node_ready
            && power != PowerState::Off
        {
            // Nobody asked it to wake up. Re-suspending would just loop, so
            // drain it again (below) and shut it down instead.
            self.standby_failed("machine woke up from standby by itself".into())
                .await;
            self.st.standby_since = None;
        }
        let suspended = in_standby(self.st.phase, self.st.standby_since.is_some(), self.st.node_ready);
        if power == PowerState::Off || suspended {
            let (phase, reason, note) = if self.st.standby_since.is_some() {
                (Phase::Standby, "Standby", "machine is in standby")
            } else {
                (Phase::Off, "PoweredOff", "machine is powered off")
            };
            if self.st.phase != phase {
                self.ctx.event(self.mn, EventType::Normal, reason, note.into()).await;
            }
            self.set_phase(phase);
            self.st.forced_off_at = None;
            self.st.message = None;
            return Ok(SLOW);
        }
        let lc = &self.mn.spec.lifecycle;
        match self.st.phase {
            Phase::PoweringOff => {
                let shutdown_timeout = lc.shutdown_timeout_seconds as i64;
                let settle = shutdown_timeout.min(STANDBY_SETTLE_SECS);
                if self.st.standby_since.is_some() && self.in_phase_for() > settle {
                    // Still up: the suspend did not happen, or the machine woke
                    // straight back up. It is running, so shut it down gracefully.
                    self.standby_failed(format!(
                        "machine did not stay in standby (still up {settle}s after suspending)"
                    ))
                    .await;
                    self.act("PowerOff").await;
                    self.st.phase_since = Some(self.now);
                } else if self.in_phase_for() > shutdown_timeout {
                    let since_force = self.st.forced_off_at.map(|t| (self.now - t).num_seconds());
                    if since_force.is_none_or(|s| s > RETRY_ACTION_AFTER) {
                        self.st.message = Some(format!(
                            "graceful shutdown did not finish within {shutdown_timeout}s; forcing power off"
                        ));
                        self.act("ForceOff").await;
                        self.st.forced_off_at = Some(self.now);
                        self.st.standby_since = None;
                    }
                } else if self.st.last_power_action.as_ref().is_some_and(|a| !a.succeeded)
                    && self.since_last_action(self.off_action()) > RETRY_ACTION_AFTER / 3
                {
                    self.act(self.off_action()).await;
                }
                Ok(FAST)
            }
            Phase::Draining => self.drain().await,
            _ => match &self.node {
                Some(node) => {
                    cordon(&self.ctx.client, node).await?;
                    self.ctx
                        .event(
                            self.mn,
                            EventType::Normal,
                            "Draining",
                            format!("cordoned node {}; evicting pods", node.name_any()),
                        )
                        .await;
                    self.set_phase(Phase::Draining);
                    Ok(Duration::from_secs(2))
                }
                None => {
                    // Never power off a machine whose Node we cannot see: we
                    // could not drain it, and spec.nodeName may simply be wrong.
                    // (reconcile() already refuses actions without a Node; this
                    // is a second line of defence.)
                    self.st.message = Some(format!(
                        "refusing to power off: Node {} not found, cannot drain",
                        self.mn.spec.node_name
                    ));
                    Ok(NORMAL)
                }
            },
        }
    }

    async fn power_off(&mut self) {
        // Tell node fencing this is deliberate before the machine goes quiet.
        if !self.ctx.dry_run
            && let Err(e) = mark_powered_off(&self.ctx.client, &self.mn.spec.node_name, self.now).await
        {
            warn!(node = %self.mn.spec.node_name, error = %e, "cannot annotate node as powered off");
        }
        self.act(self.off_action()).await;
        // Even if the request failed we move on; PoweringOff retries and eventually forces.
        self.set_phase(Phase::PoweringOff);
    }

    async fn drain(&mut self) -> Result<Duration, Error> {
        let node_name = &self.mn.spec.node_name;
        let pods = self.ctx.pods_on(node_name);
        let mut remaining = Vec::new();
        let mut blocked = Vec::new();
        for pod in &pods {
            let id = format!("{}/{}", pod.namespace().unwrap_or_default(), pod.name_any());
            match drain_class(pod) {
                DrainClass::Ignore => {}
                DrainClass::Block(why) => blocked.push(format!("{id} ({why})")),
                DrainClass::Evict => {
                    remaining.push(id.clone());
                    if pod.metadata.deletion_timestamp.is_some() {
                        continue;
                    }
                    let api: Api<Pod> = Api::namespaced(self.ctx.client.clone(), &pod.namespace().unwrap_or_default());
                    match api.evict(&pod.name_any(), &EvictParams::default()).await {
                        Ok(_) => info!(pod = %id, node = %node_name, "evicted pod"),
                        Err(kube::Error::Api(e)) if e.code == 404 => {}
                        // 429: disruption budget does not allow it right now; retry next round.
                        Err(kube::Error::Api(e)) if e.code == 429 => {}
                        Err(e) => warn!(pod = %id, error = %e, "eviction failed"),
                    }
                }
            }
        }

        if remaining.is_empty() && blocked.is_empty() {
            if self.mn.spec.lifecycle.power_off_mode == PowerOffMode::Auto
                && self.probe_sleep().await.is_none()
                && self.in_phase_for() <= PROBE_WAIT_SECS
            {
                // Not known yet; after PROBE_WAIT_SECS it is a shutdown.
                self.st.message = Some("checking whether the machine can sleep in S3".into());
                return Ok(Duration::from_secs(2));
            }
            self.st.message = None;
            self.power_off().await;
            return Ok(FAST);
        }

        let drain_timeout = self.mn.spec.lifecycle.drain_timeout_seconds as i64;
        let summary = format!(
            "waiting for {} pod(s) to leave{}",
            remaining.len(),
            if blocked.is_empty() {
                String::new()
            } else {
                format!("; blocked by {}", blocked.join(", "))
            }
        );
        if self.in_phase_for() <= drain_timeout {
            self.st.message = Some(summary);
            return Ok(Duration::from_secs(5));
        }

        if self.mn.spec.lifecycle.force_after_drain_timeout {
            self.st.message = Some(format!("drain timed out ({summary}); powering off anyway"));
            self.ctx
                .event(
                    self.mn,
                    EventType::Warning,
                    "DrainTimeout",
                    self.st.message.clone().unwrap(),
                )
                .await;
            self.power_off().await;
            return Ok(FAST);
        }

        let note = format!("drain timed out after {drain_timeout}s: {summary}");
        self.ctx
            .event(self.mn, EventType::Warning, "DrainFailed", note.clone())
            .await;
        self.st.message = Some(note);
        self.st.drain_failed_at = Some(self.now);
        if self.mn.spec.power_policy == PowerPolicy::Auto {
            // Abort the scale down; the pool backs off from this machine for a while.
            self.st.scaling_decision = Some(ScalingDecision {
                pool: self
                    .st
                    .scaling_decision
                    .as_ref()
                    .map(|d| d.pool.clone())
                    .or_else(|| self.st.pools.first().cloned())
                    .unwrap_or_default(),
                target: PowerTarget::On,
                reason: "scale down aborted: drain failed".into(),
                time: self.now,
            });
            if let Some(node) = &self.node {
                uncordon_if_ours(&self.ctx.client, node).await?;
            }
            self.set_phase(Phase::On);
        } else {
            // Manual Off: keep trying, restarting the drain timer.
            self.st.phase_since = Some(self.now);
        }
        Ok(NORMAL)
    }

    /// Reacts to a power change made by someone else. Clearing the stored
    /// decision is what keeps a stale one from fighting the change: an old
    /// "Off" would power a manually started machine straight back off, an old
    /// "On" would wake a manually stopped one.
    async fn manual_change(&mut self, change: ManualChange) {
        let node = self.mn.spec.node_name.clone();
        match change {
            ManualChange::None => {}
            ManualChange::PoweredOn => {
                let policy = self.manual_policy;
                self.st.manual_power_on = Some(ManualPowerChange {
                    time: self.now,
                    policy: Some(policy),
                });
                self.st.scaling_decision = None;
                self.st.standby_since = None;
                // A person started a new boot: an earlier boot failure (and its
                // backoff) no longer applies, and must not be acted on.
                self.st.boot_failure = None;
                let what = match policy {
                    ManualPowerOnPolicy::LeaveOn => "not managing it while it stays on",
                    ManualPowerOnPolicy::Adopt => "managing it as if its pool had powered it on",
                    ManualPowerOnPolicy::PowerOff => "powering it off again after the pool's grace period",
                };
                let note = format!("{node} powered on outside the autoscaler; policy {policy:?}: {what}");
                info!(nodepowermanagementconfig = %self.mn.name_any(), ?policy, "manual power-on");
                self.ctx.event(self.mn, EventType::Normal, "ManualPowerOn", note).await;
            }
            ManualChange::PoweredOff => {
                self.st.manual_power_on = None;
                self.st.manual_power_off = Some(ManualPowerChange {
                    time: self.now,
                    policy: None,
                });
                self.st.scaling_decision = None;
                if self.st.boot_failure.as_ref().is_some_and(|f| f.left_on) {
                    self.st.boot_failure = None;
                }
                info!(nodepowermanagementconfig = %self.mn.name_any(), "manual power-off");
                self.ctx
                    .event(
                        self.mn,
                        EventType::Normal,
                        "ManualPowerOff",
                        format!("{node} powered off outside the autoscaler; back under management"),
                    )
                    .await;
            }
        }
    }

    /// Handles a machine that is powered on without its Node becoming Ready.
    /// Returns `Some(requeue)` while it is boot-failed and still powered (the
    /// normal state machine is skipped), `None` otherwise.
    async fn boot(&mut self, event: BootEvent) -> Option<Duration> {
        let node = self.mn.spec.node_name.clone();
        match event {
            BootEvent::Recovered => {
                info!(nodepowermanagementconfig = %self.mn.name_any(), "node Ready after a boot failure");
                self.ctx
                    .event(
                        self.mn,
                        EventType::Normal,
                        "BootRecovered",
                        format!("{node} is Ready after all; managed normally again"),
                    )
                    .await;
                return None;
            }
            BootEvent::Failed => {
                let leave = boot_failure_leaves_on(&self.st, self.manual_policy);
                if let Some(f) = self.st.boot_failure.as_mut() {
                    f.left_on = leave;
                }
                self.st.scaling_decision = None;
                let minutes = self.boot_timeout / 60;
                let what = if leave {
                    "left on: it was powered on by hand and the policy is LeaveOn".to_string()
                } else {
                    "powering it off; it is not woken again for the pools' bootFailureBackoffSeconds".to_string()
                };
                let note =
                    format!("{node} powered on {minutes}m+ without its Node becoming Ready; boot failed, {what}");
                warn!(nodepowermanagementconfig = %self.mn.name_any(), boot_timeout = self.boot_timeout, left_on = leave, "boot failed");
                self.st.message = Some(note.clone());
                self.set_phase(Phase::BootFailed);
                self.ctx.event(self.mn, EventType::Warning, "BootFailed", note).await;
                if !leave {
                    if !self.ctx.dry_run
                        && let Err(e) = mark_powered_off(&self.ctx.client, &node, self.now).await
                    {
                        warn!(node = %node, error = %e, "cannot annotate node as powered off");
                    }
                    self.act("PowerOff").await;
                }
                return Some(FAST);
            }
            BootEvent::None => {}
        }
        let failure = self.st.boot_failure.clone()?;
        if self.st.node_ready || self.st.power_state != PowerState::On || !failure_is_current(&self.st) {
            return None;
        }
        // Still powered and not Ready after a boot failure.
        self.set_phase(Phase::BootFailed);
        if !failure.left_on {
            let since_failure = (self.now - failure.time).num_seconds();
            let shutdown_timeout = self.mn.spec.lifecycle.shutdown_timeout_seconds as i64;
            let since_force = self.st.forced_off_at.map(|t| (self.now - t).num_seconds());
            if since_failure > shutdown_timeout.min(STANDBY_SETTLE_SECS)
                && since_force.is_none_or(|s| s > RETRY_ACTION_AFTER)
            {
                // The graceful request did nothing (a machine with nothing to
                // boot ignores ACPI): cut the power.
                self.act("ForceOff").await;
                self.st.forced_off_at = Some(self.now);
            }
        }
        Some(FAST)
    }

    /// Without a desired state, only mirror what the interface reports.
    fn observe(&mut self) -> Duration {
        if in_standby(self.st.phase, self.st.standby_since.is_some(), self.st.node_ready) {
            return SLOW;
        }
        match self.st.power_state {
            PowerState::On if self.st.node_ready => self.set_phase(Phase::On),
            PowerState::On => self.set_phase(Phase::PoweringOn),
            PowerState::Off => self.set_phase(Phase::Off),
            PowerState::Unknown => self.set_phase(Phase::Unknown),
        }
        SLOW
    }
}

/// `Standby` or `PowerOff`. Standby is used when configured (with `Auto`,
/// only if the machine is known to sleep in S3), unless it failed on this
/// machine within the backoff period.
pub fn off_action(
    mode: PowerOffMode,
    standby_failed_at: Option<DateTime<Utc>>,
    s3: Option<bool>,
    now: DateTime<Utc>,
) -> &'static str {
    let standby_broken = standby_failed_at.is_some_and(|t| (now - t).num_seconds() < STANDBY_BACKOFF_SECS);
    match mode {
        _ if standby_broken => "PowerOff",
        PowerOffMode::Standby => "Standby",
        PowerOffMode::Auto if s3 == Some(true) => "Standby",
        _ => "PowerOff",
    }
}

/// Whether a machine can sleep in S3, for scale-up ordering: it is asleep in
/// standby right now, or its last sleep probe found S3 and standby has not
/// failed on it within the backoff period. The probe result of an earlier boot
/// is used as-is, since a powered-off machine cannot be probed.
pub fn s3_capable(st: &NodePowerManagementConfigStatus, now: DateTime<Utc>) -> bool {
    let standby_broken = st
        .standby_failed_at
        .is_some_and(|t| (now - t).num_seconds() < STANDBY_BACKOFF_SECS);
    st.phase == Phase::Standby || (!standby_broken && st.sleep_support.as_ref().is_some_and(|s| s.s3))
}

/// Whether a standby we requested has taken effect. Most interfaces read a
/// suspended machine as Off, but some BMCs keep reporting On in S3; the
/// kubelet going quiet is the reliable signal.
pub fn in_standby(phase: Phase, standby_requested: bool, node_ready: bool) -> bool {
    standby_requested && matches!(phase, Phase::PoweringOff | Phase::Standby) && !node_ready
}

/// Seconds after our own power-off during which "interface says Off, node
/// still alive" is expected (the kubelet lease has not expired yet).
const OFF_GRACE_SECS: i64 = 90;
/// A kubelet lease renewed this recently means the node is running.
const LEASE_FRESH_SECS: i64 = 15;

/// Whether the interface's power reading agrees with the Node. The one
/// contradiction we can detect: the interface reports Off while the Node's
/// kubelet is demonstrably alive, which means the interface controls a
/// different machine (or lies). Power actions are refused while it holds.
pub fn power_state_consistent(
    reported: PowerState,
    node_ready: bool,
    lease_age_secs: Option<i64>,
    since_our_off_action_secs: Option<i64>,
) -> bool {
    let node_alive = node_ready && lease_age_secs.is_some_and(|a| a <= LEASE_FRESH_SECS);
    let recently_turned_off = since_our_off_action_secs.is_some_and(|s| s <= OFF_GRACE_SECS);
    !(reported == PowerState::Off && node_alive && !recently_turned_off)
}

async fn lease_age_secs(ctx: &Context, node: &str, now: DateTime<Utc>) -> Option<i64> {
    let leases: Api<Lease> = Api::namespaced(ctx.client.clone(), "kube-node-lease");
    let lease = leases.get_opt(node).await.ok()??;
    let renew = lease.spec?.renew_time?;
    let renewed = DateTime::from_timestamp(renew.0.as_second(), 0)?;
    Some((now - renewed).num_seconds())
}

pub async fn reconcile(mn: Arc<NodePowerManagementConfig>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = mn.name_any();
    let api: Api<NodePowerManagementConfig> = Api::all(ctx.client.clone());
    // Our own writes (the status patch, cordoning the Node) trigger the next
    // reconcile before the cache has seen the new status. Acting on that stale
    // copy would repeat the last transition, e.g. send a second suspend or
    // power on. Read the live object instead.
    let Some(mn) = api.get_opt(&name).await?.map(Arc::new) else {
        return Ok(Action::await_change());
    };
    let old = mn.status.clone().unwrap_or_default();
    let mut st = old.clone();
    let now = Utc::now();

    // --- gate 1: the Node must exist ---------------------------------------
    let node = ctx.node(&mn.spec.node_name);
    st.node_ready = node.as_deref().is_some_and(node_ready);
    match node.as_deref() {
        Some(n) => {
            // Keep the latest scheduling-relevant facts so they survive power off.
            let alloc = node_allocatable(n);
            if alloc.cpu_millis > 0 {
                st.allocatable = Some(alloc);
            }
            // Only non-zero values: right after a boot a device plugin reports 0
            // until it has registered its devices again.
            for (name, amount) in crate::resources::node_extended(n) {
                if amount > 0 {
                    st.extended_resources.insert(name, amount);
                }
            }
            st.node_labels = Some(n.labels().clone());
            st.set_condition(COND_NODE_FOUND, "True", "NodeFound", "", now);
        }
        None => st.set_condition(
            COND_NODE_FOUND,
            "False",
            "NodeNotFound",
            format!("Node {} does not exist; no power actions are taken", mn.spec.node_name),
            now,
        ),
    }

    // --- gate 2: pool membership from the Node's labels ----------------------
    let pools = ctx.pools();
    let membership = membership::resolve(node.as_deref().map(|n| n.labels()), pools.iter().map(|p| p.as_ref()));
    st.pools = membership.pools().to_vec();
    st.pool = (!st.pools.is_empty()).then(|| st.pools.join(","));
    match &membership {
        Membership::Member(ps) => st.set_condition(
            COND_POOL_MEMBERSHIP,
            "True",
            "InPool",
            match ps.len() {
                1 => format!("selected by NodeScalingPool {}", ps[0]),
                _ => format!(
                    "selected by NodeScalingPools {} (any may power it on; all must agree to power it off)",
                    ps.join(", ")
                ),
            },
            now,
        ),
        Membership::NotInPool => st.set_condition(
            COND_POOL_MEMBERSHIP,
            "False",
            "NotInPool",
            "no NodeScalingPool selects this Node",
            now,
        ),
        Membership::NoNode => st.set_condition(COND_POOL_MEMBERSHIP, "False", "NodeNotFound", "", now),
    }
    // A decision only counts while the machine is still in the pool that made it.
    if st
        .scaling_decision
        .as_ref()
        .is_some_and(|d| !st.pools.contains(&d.pool))
    {
        info!(nodepowermanagementconfig = %name, "clearing scaling decision from a pool this machine no longer belongs to");
        st.scaling_decision = None;
    }

    let mut chain = match PowerChain::build(&ctx.drivers, &mn).await {
        Ok(c) => c,
        Err(e) => {
            warn!(nodepowermanagementconfig = %name, error = %e, "cannot build any power interface");
            st.phase = Phase::Error;
            st.message = Some(e.to_string());
            st.power_state = PowerState::Unknown;
            st.interface_warnings.clear();
            patch_status_diff(&api, &name, &old, &st).await?;
            return Ok(Action::requeue(SLOW));
        }
    };

    // --- gate 3: does each interface control this machine? -------------------
    let node_uuid = node
        .as_deref()
        .and_then(|n| n.status.as_ref())
        .and_then(|s| s.node_info.as_ref())
        .map(|i| i.system_uuid.clone());
    let key = (name.clone(), mn.metadata.generation.unwrap_or_default());
    let identity: Vec<(String, IdentityCheck, bool)> = match ctx.identity_cache.get(&key) {
        Some(cached) => cached.into_iter().map(|(l, r)| (l, r, true)).collect(),
        None => {
            let fresh = chain.check_identity(node_uuid.as_deref()).await;
            if fresh.iter().all(|(_, _, cacheable)| *cacheable) {
                ctx.identity_cache
                    .put(key, fresh.iter().map(|(l, r, _)| (l.clone(), r.clone())).collect());
            }
            fresh
        }
    };
    chain.disable_mismatched(&identity);
    let mismatched: Vec<&str> = identity
        .iter()
        .filter(|(_, r, _)| matches!(r, IdentityCheck::Mismatch { .. }))
        .map(|(l, _, _)| l.as_str())
        .collect();
    let verified: Vec<&str> = identity
        .iter()
        .filter(|(_, r, _)| *r == IdentityCheck::Match)
        .map(|(l, _, _)| l.as_str())
        .collect();
    if !mismatched.is_empty() {
        if old.condition_is_true(COND_IDENTITY_VERIFIED)
            || !old
                .conditions
                .iter()
                .any(|c| c.type_ == COND_IDENTITY_VERIFIED && c.reason == "Mismatch")
        {
            ctx.event(
                mn.as_ref(),
                EventType::Warning,
                "IdentityMismatch",
                format!(
                    "interfaces {} control a different machine than Node {}; they are disabled",
                    mismatched.join(", "),
                    mn.spec.node_name
                ),
            )
            .await;
        }
        st.set_condition(
            COND_IDENTITY_VERIFIED,
            "False",
            "Mismatch",
            format!(
                "{} report a different system UUID than the Node; disabled",
                mismatched.join(", ")
            ),
            now,
        );
    } else if !verified.is_empty() {
        st.set_condition(
            COND_IDENTITY_VERIFIED,
            "True",
            "Verified",
            format!("system UUID confirmed via {}", verified.join(", ")),
            now,
        );
    } else {
        st.set_condition(
            COND_IDENTITY_VERIFIED,
            "Unknown",
            "NotVerifiable",
            "no interface can report a system UUID; relying on the power state consistency check",
            now,
        );
    }

    match chain.power_state().await {
        Ok(outcome) => {
            st.power_state = outcome.value;
            st.interface_warnings = outcome.failures;
        }
        Err(e) => {
            st.interface_warnings = chain.unavailable().to_vec();
            warn!(nodepowermanagementconfig = %name, error = %e, "cannot read power state");
            ctx.metrics.node_power(&mn.spec.node_name, PowerState::Unknown);
            st.power_state = PowerState::Unknown;
            st.message = Some(format!("cannot read power state: {e}"));
            patch_status_diff(&api, &name, &old, &st).await?;
            return Ok(Action::requeue(NORMAL));
        }
    }
    ctx.metrics.node_power(&mn.spec.node_name, st.power_state);

    // --- gate 4: interface reading must not contradict a live Node -----------
    let lease_age = if st.power_state == PowerState::Off && st.node_ready {
        lease_age_secs(&ctx, &mn.spec.node_name, now).await
    } else {
        None
    };
    let since_off = st
        .last_power_action
        .as_ref()
        .filter(|a| matches!(a.action.as_str(), "PowerOff" | "ForceOff" | "Standby"))
        .map(|a| (now - a.time).num_seconds());
    let consistent = power_state_consistent(st.power_state, st.node_ready, lease_age, since_off);
    if consistent {
        st.set_condition(COND_POWER_STATE_CONSISTENT, "True", "Consistent", "", now);
    } else {
        if old.condition_is_true(COND_POWER_STATE_CONSISTENT) || old.conditions.is_empty() {
            ctx.event(
                mn.as_ref(),
                EventType::Warning,
                "PowerStateMismatch",
                "an interface reports Off while the Node's kubelet is alive; it probably controls a different machine. Power actions are disabled.".into(),
            )
            .await;
        }
        st.set_condition(
            COND_POWER_STATE_CONSISTENT,
            "False",
            "InterfaceReportsOffButNodeIsAlive",
            "the interface reports Off, yet the Node is Ready and its kubelet lease is fresh; check that the interface points at this machine",
            now,
        );
    }

    // Settings that can come from the machine's pools.
    let member_pools: Vec<&Arc<NodeScalingPool>> = pools.iter().filter(|p| st.pools.contains(&p.name_any())).collect();
    let manual_policy = effective_manual_power_on_policy(
        mn.spec.manual_power_on_policy,
        member_pools.iter().map(|p| p.spec.manual_power_on_policy),
    );
    let boot_timeout = effective_boot_timeout(
        mn.spec.lifecycle.boot_timeout_seconds,
        member_pools.iter().map(|p| p.spec.scale_up.boot_timeout_seconds),
    );
    let change = detect_manual_change(&old, &st, now, boot_timeout, mn.spec.lifecycle.shutdown_timeout_seconds);
    // A machine seen off is under normal management again, whoever turned it off.
    if st.power_state == PowerState::Off && !st.node_ready && change != ManualChange::PoweredOn {
        st.manual_power_on = None;
    }
    let boot_event = track_boot(&mut st, now, boot_timeout);

    let node_found = node.is_some();
    let mut r = Reconciler {
        mn: &mn,
        ctx: &ctx,
        chain,
        node,
        st,
        now,
        boot_timeout,
        manual_policy,
    };
    r.manual_change(change).await;
    let boot_requeue = r.boot(boot_event).await;
    let requeue = if let Some(requeue) = boot_requeue {
        requeue
    } else if !node_found || !consistent {
        // Safety gates failed: never act, only report.
        r.st.message = Some(if !node_found {
            format!("Node {} not found: observing only, no power actions", mn.spec.node_name)
        } else {
            "power state contradicts the live Node: observing only, no power actions".to_string()
        });
        r.observe()
    } else {
        match managed_target(mn.spec.power_policy, &r.st, manual_policy) {
            None => r.observe(),
            Some(PowerTarget::On) => r.ensure_on().await?,
            Some(PowerTarget::Off) => r.ensure_off().await?,
        }
    };
    patch_status_diff(&api, &name, &old, &r.st).await?;
    Ok(Action::requeue(requeue))
}

pub fn error_policy(mn: Arc<NodePowerManagementConfig>, err: &Error, ctx: Arc<Context>) -> Action {
    warn!(nodepowermanagementconfig = %mn.name_any(), error = %err, "reconcile failed");
    ctx.metrics.reconcile_error("nodepowermanagementconfig");
    Action::requeue(NORMAL)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_reading_with_live_node_is_inconsistent() {
        // Interface says Off, Node Ready, kubelet lease renewed 5 s ago, we never turned it off.
        assert!(!power_state_consistent(PowerState::Off, true, Some(5), None));
        // ... but right after our own power off, the node is expected to look alive for a while.
        assert!(power_state_consistent(PowerState::Off, true, Some(5), Some(30)));
        assert!(!power_state_consistent(PowerState::Off, true, Some(5), Some(600)));
        // Stale lease or NotReady node: the machine may really be off.
        assert!(power_state_consistent(PowerState::Off, true, Some(60), None));
        assert!(power_state_consistent(PowerState::Off, false, Some(5), None));
        assert!(power_state_consistent(PowerState::Off, true, None, None));
        // On readings never contradict (a booting node is legitimately not Ready yet).
        assert!(power_state_consistent(PowerState::On, false, None, None));
    }

    #[test]
    fn standby_is_reached_when_the_node_goes_quiet() {
        // Suspend requested, node no longer Ready: in standby even if a BMC still reads On.
        assert!(in_standby(Phase::PoweringOff, true, false));
        assert!(in_standby(Phase::Standby, true, false));
        // Still Ready: the suspend has not happened (yet).
        assert!(!in_standby(Phase::PoweringOff, true, true));
        // A NotReady node is not in standby unless we asked for it.
        assert!(!in_standby(Phase::PoweringOff, false, false));
        assert!(!in_standby(Phase::Draining, true, false));
    }

    #[test]
    fn failed_standby_falls_back_to_shutdown_for_a_day() {
        let now = Utc::now();
        let hours_ago = |h| Some(now - chrono::Duration::hours(h));
        assert_eq!(off_action(PowerOffMode::Standby, None, None, now), "Standby");
        assert_eq!(off_action(PowerOffMode::Standby, hours_ago(1), None, now), "PowerOff");
        assert_eq!(off_action(PowerOffMode::Standby, hours_ago(25), None, now), "Standby");
        assert_eq!(off_action(PowerOffMode::Shutdown, None, Some(true), now), "PowerOff");
    }

    #[test]
    fn auto_sleeps_only_where_s3_is_available() {
        let now = Utc::now();
        assert_eq!(off_action(PowerOffMode::Auto, None, Some(true), now), "Standby");
        assert_eq!(off_action(PowerOffMode::Auto, None, Some(false), now), "PowerOff");
        // Unknown (probe did not finish): shut down.
        assert_eq!(off_action(PowerOffMode::Auto, None, None, now), "PowerOff");
        // S3 available but it failed recently: shut down.
        let failed = Some(now - chrono::Duration::hours(2));
        assert_eq!(off_action(PowerOffMode::Auto, failed, Some(true), now), "PowerOff");
    }

    fn decision(pool: &str, target: PowerTarget) -> ScalingDecision {
        ScalingDecision {
            pool: pool.into(),
            target,
            reason: String::new(),
            time: Utc::now(),
        }
    }

    #[test]
    fn only_decisions_from_the_current_pools_count() {
        let mut st = NodePowerManagementConfigStatus {
            pool: Some("ci,gpu".into()),
            pools: vec!["ci".into(), "gpu".into()],
            ..Default::default()
        };
        st.scaling_decision = Some(decision("ci", PowerTarget::On));
        assert_eq!(desired_target(PowerPolicy::Auto, &st), Some(PowerTarget::On));
        st.scaling_decision = Some(decision("gpu", PowerTarget::Off));
        assert_eq!(desired_target(PowerPolicy::Auto, &st), Some(PowerTarget::Off));
        st.scaling_decision = Some(decision("old-pool", PowerTarget::Off));
        assert_eq!(desired_target(PowerPolicy::Auto, &st), None);
        st.pool = None;
        st.pools.clear();
        st.scaling_decision = Some(decision("gpu", PowerTarget::Off));
        assert_eq!(desired_target(PowerPolicy::Auto, &st), None);
        assert_eq!(desired_target(PowerPolicy::AlwaysOn, &st), Some(PowerTarget::On));
    }

    fn status(phase: Phase, power: PowerState, ready: bool) -> NodePowerManagementConfigStatus {
        NodePowerManagementConfigStatus {
            phase,
            power_state: power,
            node_ready: ready,
            ..Default::default()
        }
    }

    fn action(name: &str, secs_ago: i64, now: DateTime<Utc>) -> Option<PowerActionRecord> {
        Some(PowerActionRecord {
            action: name.into(),
            time: now - chrono::Duration::seconds(secs_ago),
            succeeded: true,
            via: None,
        })
    }

    #[test]
    fn manual_power_changes_are_told_apart_from_ours() {
        let now = Utc::now();
        let off = status(Phase::Off, PowerState::Off, false);
        let detect = |old: &NodePowerManagementConfigStatus, new: &NodePowerManagementConfigStatus| {
            detect_manual_change(old, new, now, 1200, 300)
        };
        // Off before, on now, nothing of ours: manual.
        let mut on = status(Phase::Off, PowerState::On, false);
        assert_eq!(detect(&off, &on), ManualChange::PoweredOn);
        // Our own PowerOn a minute ago explains it.
        on.last_power_action = action("PowerOn", 60, now);
        assert_eq!(detect(&off, &on), ManualChange::None);
        // While we power on, the interface may still read Off: not a manual power-off.
        let powering_on = status(Phase::PoweringOn, PowerState::Off, false);
        assert_eq!(
            detect(&powering_on, &status(Phase::PoweringOn, PowerState::Off, false)),
            ManualChange::None
        );

        // Waking from standby: right after suspending it is standby not holding;
        // later it is someone waking it.
        let mut asleep = status(Phase::Standby, PowerState::Off, false);
        asleep.standby_since = Some(now - chrono::Duration::seconds(60));
        let awake = status(Phase::Standby, PowerState::On, true);
        assert_eq!(detect(&asleep, &awake), ManualChange::None);
        asleep.standby_since = Some(now - chrono::Duration::hours(2));
        assert_eq!(detect(&asleep, &awake), ManualChange::PoweredOn);

        // On before, off now, no power-off of ours: manual.
        let running = status(Phase::On, PowerState::On, true);
        let mut down = status(Phase::On, PowerState::Off, false);
        assert_eq!(detect(&running, &down), ManualChange::PoweredOff);
        down.last_power_action = action("ForceOff", 30, now);
        assert_eq!(detect(&running, &down), ManualChange::None);
        // A machine that failed to boot and that someone switched off.
        let failed = status(Phase::BootFailed, PowerState::On, false);
        assert_eq!(
            detect(&failed, &status(Phase::BootFailed, PowerState::Off, false)),
            ManualChange::PoweredOff
        );
    }

    #[test]
    fn a_machine_that_never_becomes_ready_fails_to_boot_once_and_recovers() {
        let t0 = Utc::now();
        let at = |m: i64| t0 + chrono::Duration::minutes(m);
        // Powered on (by anyone), Node never Ready: "no bootable device".
        let mut st = status(Phase::PoweringOn, PowerState::On, false);
        assert_eq!(track_boot(&mut st, at(0), 1200), BootEvent::None);
        assert_eq!(track_boot(&mut st, at(19), 1200), BootEvent::None);
        assert_eq!(track_boot(&mut st, at(21), 1200), BootEvent::Failed);
        assert_eq!(st.boot_failure.as_ref().unwrap().time, at(21));
        // Reported once, not on every reconcile.
        assert_eq!(track_boot(&mut st, at(40), 1200), BootEvent::None);

        // Powered off, then a new boot that fails again is a new failure.
        st.power_state = PowerState::Off;
        st.phase = Phase::Off;
        assert_eq!(track_boot(&mut st, at(41), 1200), BootEvent::None);
        assert!(st.powered_on_not_ready_since.is_none());
        st.power_state = PowerState::On;
        st.phase = Phase::PoweringOn;
        assert_eq!(track_boot(&mut st, at(80), 1200), BootEvent::None);
        assert_eq!(track_boot(&mut st, at(101), 1200), BootEvent::Failed);

        // The Node goes Ready after all: recovered.
        st.node_ready = true;
        assert_eq!(track_boot(&mut st, at(102), 1200), BootEvent::Recovered);
        assert!(st.boot_failure.is_none());
        assert_eq!(track_boot(&mut st, at(103), 1200), BootEvent::None);
    }

    #[test]
    fn a_suspended_machine_reading_on_is_not_booting() {
        let now = Utc::now();
        let mut st = status(Phase::Standby, PowerState::On, false);
        st.standby_since = Some(now - chrono::Duration::hours(5));
        assert_eq!(track_boot(&mut st, now, 60), BootEvent::None);
        assert!(st.powered_on_not_ready_since.is_none());
    }

    #[test]
    fn only_a_manual_power_on_under_leave_on_is_left_on_after_a_boot_failure() {
        let now = Utc::now();
        let mut st = status(Phase::PoweringOn, PowerState::On, false);
        st.powered_on_not_ready_since = Some(now - chrono::Duration::minutes(21));
        // Nothing of ours started this boot.
        assert!(boot_failure_leaves_on(&st, ManualPowerOnPolicy::LeaveOn));
        assert!(!boot_failure_leaves_on(&st, ManualPowerOnPolicy::Adopt));
        assert!(!boot_failure_leaves_on(&st, ManualPowerOnPolicy::PowerOff));
        // We powered it on: powered off again whatever the policy.
        st.last_power_action = action("PowerOn", 21 * 60 + 5, now);
        assert!(!boot_failure_leaves_on(&st, ManualPowerOnPolicy::LeaveOn));
    }

    #[test]
    fn a_stale_decision_never_fights_a_manual_power_on_under_leave_on() {
        let mut st = NodePowerManagementConfigStatus {
            pools: vec!["gpu".into()],
            scaling_decision: Some(decision("gpu", PowerTarget::Off)),
            manual_power_on: Some(ManualPowerChange {
                time: Utc::now(),
                policy: Some(ManualPowerOnPolicy::LeaveOn),
            }),
            ..Default::default()
        };
        assert_eq!(
            managed_target(PowerPolicy::Auto, &st, ManualPowerOnPolicy::LeaveOn),
            None
        );
        // Manual overrides still apply.
        assert_eq!(
            managed_target(PowerPolicy::AlwaysOff, &st, ManualPowerOnPolicy::LeaveOn),
            Some(PowerTarget::Off)
        );
        // Adopt / PowerOff follow the pools (whose old decision is cleared when
        // the manual power-on is seen; see manual_change).
        assert_eq!(
            managed_target(PowerPolicy::Auto, &st, ManualPowerOnPolicy::Adopt),
            Some(PowerTarget::Off)
        );
        st.manual_power_on = None;
        assert_eq!(
            managed_target(PowerPolicy::Auto, &st, ManualPowerOnPolicy::LeaveOn),
            Some(PowerTarget::Off)
        );
    }

    #[test]
    fn an_old_boot_failure_never_acts_on_a_new_boot() {
        let t0 = Utc::now();
        let at = |m: i64| t0 + chrono::Duration::minutes(m);
        // The operator's boot failed at 20 min; the machine was powered off.
        let mut st = status(Phase::PoweringOn, PowerState::On, false);
        st.powered_on_not_ready_since = Some(at(0));
        assert_eq!(track_boot(&mut st, at(21), 1200), BootEvent::Failed);
        assert!(failure_is_current(&st));
        st.power_state = PowerState::Off;
        st.phase = Phase::Off;
        track_boot(&mut st, at(22), 1200);
        assert!(!failure_is_current(&st));

        // A person powers it on at 23 min: a new boot, not the failed one.
        let old = st.clone();
        st.power_state = PowerState::On;
        st.last_power_action = action("PowerOff", 2 * 60, at(23));
        assert_eq!(
            detect_manual_change(&old, &st, at(23), 1200, 300),
            ManualChange::PoweredOn
        );
        assert_eq!(track_boot(&mut st, at(23), 1200), BootEvent::None);
        assert!(!failure_is_current(&st), "the old failure is not this boot's");
        // Even before manual_change clears it, nothing acts on it; a fresh
        // failure of the new boot is recorded only after its own timeout.
        assert_eq!(track_boot(&mut st, at(30), 1200), BootEvent::None);
        assert_eq!(track_boot(&mut st, at(44), 1200), BootEvent::Failed);
        assert!(failure_is_current(&st));
    }
}
