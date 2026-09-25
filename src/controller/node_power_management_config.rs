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
    COND_IDENTITY_VERIFIED, COND_NODE_FOUND, COND_POOL_MEMBERSHIP, COND_POWER_STATE_CONSISTENT,
    NodePowerManagementConfig, NodePowerManagementConfigStatus, Phase, PowerActionRecord, PowerPolicy, PowerState,
    PowerTarget, ScalingDecision,
};
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

/// The state to drive towards. With `Auto`, only a decision from the pool the
/// machine currently belongs to counts (stale decisions are cleared earlier).
pub fn desired_target(policy: PowerPolicy, st: &NodePowerManagementConfigStatus) -> Option<PowerTarget> {
    match policy {
        PowerPolicy::AlwaysOn => Some(PowerTarget::On),
        PowerPolicy::AlwaysOff => Some(PowerTarget::Off),
        PowerPolicy::Auto => st
            .scaling_decision
            .as_ref()
            .filter(|d| st.pool.as_deref() == Some(d.pool.as_str()))
            .map(|d| d.target),
    }
}

struct Reconciler<'a> {
    mn: &'a NodePowerManagementConfig,
    ctx: &'a Context,
    chain: PowerChain,
    node: Option<Arc<Node>>,
    st: NodePowerManagementConfigStatus,
    now: DateTime<Utc>,
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
                _ => self.chain.power_off(true).await,
            }
        };
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
            self.st.message = None;
            return Ok(SLOW);
        }
        let boot_timeout = self.mn.spec.lifecycle.boot_timeout_seconds as i64;
        match self.st.phase {
            Phase::PoweringOn => {
                if self.in_phase_for() > boot_timeout {
                    self.set_phase(Phase::Error);
                    self.st.message = Some(format!(
                        "node did not become Ready within {boot_timeout}s of powering on"
                    ));
                    self.ctx
                        .event(
                            self.mn,
                            EventType::Warning,
                            "BootTimeout",
                            self.st.message.clone().unwrap(),
                        )
                        .await;
                } else if power == PowerState::Off && self.since_last_action("PowerOn") > RETRY_ACTION_AFTER {
                    self.act("PowerOn").await;
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
            _ if power == PowerState::On => {
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

    async fn ensure_off(&mut self) -> Result<Duration, Error> {
        let power = self.st.power_state;
        if power == PowerState::Off {
            if self.st.phase != Phase::Off {
                self.ctx
                    .event(
                        self.mn,
                        EventType::Normal,
                        "PoweredOff",
                        "machine is powered off".into(),
                    )
                    .await;
            }
            self.set_phase(Phase::Off);
            self.st.forced_off_at = None;
            self.st.message = None;
            return Ok(SLOW);
        }
        let lc = &self.mn.spec.lifecycle;
        match self.st.phase {
            Phase::PoweringOff => {
                let shutdown_timeout = lc.shutdown_timeout_seconds as i64;
                if self.in_phase_for() > shutdown_timeout {
                    let since_force = self.st.forced_off_at.map(|t| (self.now - t).num_seconds());
                    if since_force.is_none_or(|s| s > RETRY_ACTION_AFTER) {
                        self.st.message = Some(format!(
                            "graceful shutdown did not finish within {shutdown_timeout}s; forcing power off"
                        ));
                        self.act("ForceOff").await;
                        self.st.forced_off_at = Some(self.now);
                    }
                } else if self.st.last_power_action.as_ref().is_some_and(|a| !a.succeeded)
                    && self.since_last_action("PowerOff") > RETRY_ACTION_AFTER / 3
                {
                    self.act("PowerOff").await;
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
        self.act("PowerOff").await;
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
                pool: self.st.pool.clone().unwrap_or_default(),
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

    /// Without a desired state, only mirror what the interface reports.
    fn observe(&mut self) -> Duration {
        match self.st.power_state {
            PowerState::On if self.st.node_ready => self.set_phase(Phase::On),
            PowerState::On => self.set_phase(Phase::PoweringOn),
            PowerState::Off => self.set_phase(Phase::Off),
            PowerState::Unknown => self.set_phase(Phase::Unknown),
        }
        SLOW
    }
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
    st.pool = membership.pool().map(String::from);
    match &membership {
        Membership::Member(p) => st.set_condition(
            COND_POOL_MEMBERSHIP,
            "True",
            "InPool",
            format!("selected by NodeScalingPool {p}"),
            now,
        ),
        Membership::NotInPool => st.set_condition(
            COND_POOL_MEMBERSHIP,
            "False",
            "NotInPool",
            "no NodeScalingPool selects this Node",
            now,
        ),
        Membership::Conflict(ps) => st.set_condition(
            COND_POOL_MEMBERSHIP,
            "False",
            "Conflict",
            format!(
                "selected by several NodeScalingPools ({}); member of none",
                ps.join(", ")
            ),
            now,
        ),
        Membership::NoNode => st.set_condition(COND_POOL_MEMBERSHIP, "False", "NodeNotFound", "", now),
    }
    // A decision only counts while the machine is still in the pool that made it.
    if st
        .scaling_decision
        .as_ref()
        .is_some_and(|d| Some(d.pool.as_str()) != st.pool.as_deref())
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
        .filter(|a| a.action == "PowerOff" || a.action == "ForceOff")
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

    let node_found = node.is_some();
    let mut r = Reconciler {
        mn: &mn,
        ctx: &ctx,
        chain,
        node,
        st,
        now,
    };
    let requeue = if !node_found || !consistent {
        // Safety gates failed: never act, only report.
        r.st.message = Some(if !node_found {
            format!("Node {} not found: observing only, no power actions", mn.spec.node_name)
        } else {
            "power state contradicts the live Node: observing only, no power actions".to_string()
        });
        r.observe()
    } else {
        match desired_target(mn.spec.power_policy, &r.st) {
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

    fn decision(pool: &str, target: PowerTarget) -> ScalingDecision {
        ScalingDecision {
            pool: pool.into(),
            target,
            reason: String::new(),
            time: Utc::now(),
        }
    }

    #[test]
    fn only_decisions_from_the_current_pool_count() {
        let mut st = NodePowerManagementConfigStatus {
            pool: Some("gpu".into()),
            ..Default::default()
        };
        st.scaling_decision = Some(decision("gpu", PowerTarget::Off));
        assert_eq!(desired_target(PowerPolicy::Auto, &st), Some(PowerTarget::Off));
        st.scaling_decision = Some(decision("old-pool", PowerTarget::Off));
        assert_eq!(desired_target(PowerPolicy::Auto, &st), None);
        st.pool = None;
        st.scaling_decision = Some(decision("gpu", PowerTarget::Off));
        assert_eq!(desired_target(PowerPolicy::Auto, &st), None);
        assert_eq!(desired_target(PowerPolicy::AlwaysOn, &st), Some(PowerTarget::On));
    }
}
