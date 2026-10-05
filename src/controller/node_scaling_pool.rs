//! `NodeScalingPool` controller: periodically evaluates demand for the machines its
//! `nodeSelector` selects and records power decisions on them
//! (`NodePowerManagementConfig.status.scalingDecision`, stamped with the pool name), which
//! the `NodePowerManagementConfig` controller then carries out.
//!
//! Membership is resolved from Node labels with the same function the
//! `NodePowerManagementConfig` controller uses, so both always agree. Pools may overlap:
//! a machine selected by several pools is a member of each. Any of them may power it on;
//! one powers it off only when every other pool selecting it lists it in
//! `status.releasable` (otherwise its decision says which pool needs it and why).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::runtime::events::EventType;
use kube::{Api, ResourceExt};
use serde_json::json;
use tracing::{debug, info, warn};

use super::node_power_management_config::s3_capable;
use super::{Context, DecisionVerbosity, Error, node_ready, patch_status_diff};
use crate::crd::{
    COND_POWER_STATE_CONSISTENT, COND_PREFERRED_NODES_VALID, DecisionAction, DecisionRecord, MAX_RECENT_DECISIONS,
    ManualPowerOnPolicy, NodePowerManagementConfig, NodePowerManagementConfigStatus, NodeScalingPool,
    NodeScalingPoolStatus, Phase, PowerPolicy, PowerTarget, ResourceAmounts, ScalingDecision, effective_boot_timeout,
    effective_manual_power_off_cooldown, effective_manual_power_on_policy, set_condition,
};
use crate::membership::{self, Membership};
use crate::resources::{node_allocatable, node_extended, pod_requests};
use crate::scaling::{
    DrainClass, ExternalNode, Member, MemberState, PlannedDecision, PodView, PoolSnapshot, drain_class, is_active,
    is_daemonset_pod, is_unschedulable, plan,
};

const INTERVAL: Duration = Duration::from_secs(15);

/// Maps a member NodePowerManagementConfig to its autoscaler state. `pools` are
/// the pools the machine belongs to: a decision by any of them counts (a
/// machine another pool woke is booting for this one too), others are ignored.
pub fn member_state(mn: &NodePowerManagementConfig, pools: &[String]) -> MemberState {
    let st = mn.status.clone().unwrap_or_default();
    // A machine failing the power-state consistency gate is never touched.
    if st
        .conditions
        .iter()
        .any(|c| c.type_ == COND_POWER_STATE_CONSISTENT && c.status == "False")
    {
        return MemberState::Unavailable;
    }
    let ready_on = st.phase == Phase::On && st.node_ready;
    match mn.spec.power_policy {
        PowerPolicy::AlwaysOn if ready_on => return MemberState::Online,
        PowerPolicy::AlwaysOn | PowerPolicy::AlwaysOff => return MemberState::Unavailable,
        PowerPolicy::Auto => {}
    }
    let decision = st
        .scaling_decision
        .as_ref()
        .filter(|d| pools.contains(&d.pool))
        .map(|d| d.target);
    match decision {
        Some(PowerTarget::Off) if matches!(st.phase, Phase::Off | Phase::Standby) => MemberState::Offline,
        Some(PowerTarget::Off) => MemberState::Leaving,
        Some(PowerTarget::On) if ready_on => MemberState::Online,
        Some(PowerTarget::On) if matches!(st.phase, Phase::Error | Phase::BootFailed) => MemberState::Unavailable,
        Some(PowerTarget::On) => MemberState::Booting,
        None => match st.phase {
            Phase::On if st.node_ready => MemberState::Online,
            Phase::On | Phase::PoweringOn => MemberState::Booting,
            Phase::Off | Phase::Standby => MemberState::Offline,
            Phase::Draining | Phase::PoweringOff => MemberState::Leaving,
            Phase::Unknown | Phase::Error | Phase::BootFailed => MemberState::Unavailable,
        },
    }
}

/// How boot failures and manual power changes shape a member, for one pool.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Special {
    /// Replaces the state from `member_state`.
    pub state: Option<MemberState>,
    /// Why, for the decision log.
    pub note: Option<String>,
    /// `Adopt`: counts as this pool's power-on at that time.
    pub adopted_at: Option<chrono::DateTime<Utc>>,
    /// `PowerOff`: powered off once this time has passed.
    pub release_after: Option<chrono::DateTime<Utc>>,
}

/// Applies boot failures, the boot failure backoff, manual power-ons (by
/// `policy`) and the manual power-off cooldown to a member whose plain state
/// is `base`. Notes use absolute times so the decision log only changes when
/// the situation does.
#[allow(clippy::too_many_arguments)]
pub fn classify(
    st: &NodePowerManagementConfigStatus,
    base: MemberState,
    policy: ManualPowerOnPolicy,
    spec: &crate::crd::NodeScalingPoolSpec,
    boot_timeout_secs: u64,
    cooldown_secs: u64,
    now: chrono::DateTime<Utc>,
) -> Special {
    let at = |t: chrono::DateTime<Utc>| t.format("%H:%M:%SZ").to_string();
    let secs = |n: u64| chrono::Duration::seconds(n as i64);
    if st.phase == Phase::BootFailed
        && let Some(f) = &st.boot_failure
    {
        let then = if f.left_on {
            "left on (powered on by hand, LeaveOn)"
        } else {
            "powering off"
        };
        return Special {
            state: Some(MemberState::Unavailable),
            note: Some(format!(
                "boot failed at {}: on {}m+ without Ready; released hold; {then}",
                at(f.time),
                boot_timeout_secs / 60
            )),
            ..Default::default()
        };
    }
    if base == MemberState::Offline {
        if let Some(f) = st.boot_failure.as_ref().filter(|f| !f.left_on)
            && now < f.time + secs(spec.scale_up.boot_failure_backoff_seconds)
        {
            return Special {
                state: Some(MemberState::Unavailable),
                note: Some(format!(
                    "boot failed at {}; not woken again until {}",
                    at(f.time),
                    at(f.time + secs(spec.scale_up.boot_failure_backoff_seconds))
                )),
                ..Default::default()
            };
        }
        if let Some(off) = &st.manual_power_off {
            if cooldown_secs > 0 && now < off.time + secs(cooldown_secs) {
                return Special {
                    state: Some(MemberState::Unavailable),
                    note: Some(format!(
                        "manually powered off at {}; not woken again until {}",
                        at(off.time),
                        at(off.time + secs(cooldown_secs))
                    )),
                    ..Default::default()
                };
            }
            if now < off.time + chrono::Duration::minutes(10) {
                return Special {
                    note: Some(format!(
                        "manually powered off at {}; back under management",
                        at(off.time)
                    )),
                    ..Default::default()
                };
            }
        }
        return Special::default();
    }
    let Some(on) = &st.manual_power_on else {
        return Special::default();
    };
    match policy {
        ManualPowerOnPolicy::LeaveOn if st.node_ready => Special {
            state: Some(MemberState::Manual),
            note: Some(format!(
                "manually powered on at {}; policy LeaveOn; not managing",
                at(on.time)
            )),
            ..Default::default()
        },
        ManualPowerOnPolicy::LeaveOn => Special {
            state: Some(MemberState::Unavailable),
            note: Some(format!(
                "manually powered on at {}, not Ready yet; policy LeaveOn; not managing",
                at(on.time)
            )),
            ..Default::default()
        },
        ManualPowerOnPolicy::Adopt => Special {
            note: Some(format!(
                "manually powered on at {}; policy Adopt; managed as if this pool woke it",
                at(on.time)
            )),
            adopted_at: Some(on.time),
            ..Default::default()
        },
        ManualPowerOnPolicy::PowerOff => {
            let after = on.time + secs(spec.manual_power_on_grace_seconds);
            Special {
                note: Some(format!(
                    "manually powered on at {}; policy PowerOff; powered off after {}",
                    at(on.time),
                    at(after)
                )),
                release_after: Some(after),
                ..Default::default()
            }
        }
    }
}

fn build_member(
    mn: &NodePowerManagementConfig,
    member_of: &[String],
    pool: &NodeScalingPool,
    all_pools: &[Arc<NodeScalingPool>],
    ctx: &Context,
) -> Option<Member> {
    let down = &pool.spec.scale_down;
    let st = mn.status.clone().unwrap_or_default();
    // Membership requires a live Node, so it exists here.
    let node = ctx.node(&mn.spec.node_name)?;
    let taints = node.spec.as_ref().and_then(|s| s.taints.clone()).unwrap_or_default();
    let allocatable = Some(node_allocatable(&node))
        .filter(|a| a.cpu_millis > 0)
        .or(st.allocatable)
        .unwrap_or_default();

    let mut requested = ResourceAmounts::default();
    let mut counted = ResourceAmounts::default();
    let mut movable_pods = Vec::new();
    let mut blocking_pods = Vec::new();
    for pod in ctx.pods_on(&mn.spec.node_name) {
        if !is_active(&pod) {
            continue;
        }
        let requests = pod_requests(&pod);
        requested.add(&requests);
        let view = PodView::from_pod(&pod);
        let ignored = (down.ignore_daemon_set_utilization && is_daemonset_pod(&pod))
            || (down.ignore_non_selecting_pod_utilization && !view.explicitly_selects(&pool.spec.node_selector));
        if !ignored {
            counted.add(&requests);
        }
        match drain_class(&pod) {
            DrainClass::Ignore => {}
            DrainClass::Evict => movable_pods.push(view),
            DrainClass::Block(why) => blocking_pods.push(format!(
                "{}/{} ({why})",
                pod.namespace().unwrap_or_default(),
                pod.name_any()
            )),
        }
    }

    // Extended resources the machine had before and reports as 0 now, within
    // its boot timeout of becoming Ready: its device plugin is still
    // registering them.
    let ready_since = node
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .and_then(|c| c.iter().find(|c| c.type_ == "Ready" && c.status == "True"))
        .and_then(|c| c.last_transition_time.as_ref())
        .and_then(|t| chrono::DateTime::from_timestamp(t.0.as_second(), 0));
    let member_pools: Vec<&Arc<NodeScalingPool>> =
        all_pools.iter().filter(|p| member_of.contains(&p.name_any())).collect();
    let boot_timeout = effective_boot_timeout(
        mn.spec.lifecycle.boot_timeout_seconds,
        member_pools.iter().map(|p| p.spec.scale_up.boot_timeout_seconds),
    );
    let manual_policy = effective_manual_power_on_policy(
        mn.spec.manual_power_on_policy,
        member_pools.iter().map(|p| p.spec.manual_power_on_policy),
    );
    let cooldown = effective_manual_power_off_cooldown(
        mn.spec.manual_power_off_cooldown_seconds,
        member_pools.iter().map(|p| p.spec.manual_power_off_cooldown_seconds),
    );
    let booting_window = chrono::Duration::seconds(boot_timeout as i64);
    let registering = match ready_since {
        Some(t) if Utc::now() - t < booting_window => {
            let current = node_extended(&node);
            st.extended_resources
                .iter()
                .filter(|(k, v)| **v > 0 && current.get(*k).copied().unwrap_or(0) == 0)
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        }
        _ => Default::default(),
    };

    let base = member_state(mn, member_of);
    let special = classify(&st, base, manual_policy, &pool.spec, boot_timeout, cooldown, Utc::now());
    Some(Member {
        name: mn.name_any(),
        state: special.state.unwrap_or(base),
        note: special.note,
        adopted_at: special.adopted_at,
        release_after: special.release_after,
        woken_by: st
            .scaling_decision
            .as_ref()
            .filter(|d| d.target == PowerTarget::On && member_of.contains(&d.pool))
            .map(|d| d.pool.clone()),
        auto: mn.spec.power_policy == PowerPolicy::Auto,
        labels: node.labels().clone(),
        taints,
        allocatable,
        requested,
        counted,
        movable_pods,
        blocking_pods,
        drain_failed_at: st.drain_failed_at,
        s3_capable: s3_capable(&st, Utc::now()),
        registering,
    })
}

/// Schedulable nodes that no scaling pool can power off: Ready, not cordoned,
/// and without an `Auto` NodePowerManagementConfig. Pods evicted from a member
/// may move there (e.g. to an always-on base) when deciding scale-down.
fn external_nodes(ctx: &Context) -> Vec<ExternalNode> {
    let auto_managed: std::collections::BTreeSet<String> = ctx
        .node_power_management_configs
        .state()
        .iter()
        .filter(|c| c.spec.power_policy == PowerPolicy::Auto)
        .map(|c| c.spec.node_name.clone())
        .collect();
    ctx.nodes
        .state()
        .iter()
        .filter(|n| !auto_managed.contains(&n.name_any()))
        .filter(|n| node_ready(n) && !n.spec.as_ref().and_then(|s| s.unschedulable).unwrap_or(false))
        .map(|n| {
            let mut used = ResourceAmounts::default();
            for pod in ctx.pods_on(&n.name_any()) {
                if is_active(&pod) {
                    used.add(&pod_requests(&pod));
                }
            }
            ExternalNode {
                name: n.name_any(),
                labels: n.labels().clone(),
                taints: n.spec.as_ref().and_then(|s| s.taints.clone()).unwrap_or_default(),
                free: node_allocatable(n).minus(&used),
            }
        })
        .collect()
}

async fn decide(
    api: &Api<NodePowerManagementConfig>,
    pool: &str,
    member: &str,
    target: PowerTarget,
    reason: &str,
) -> Result<(), Error> {
    let decision = ScalingDecision {
        pool: pool.to_string(),
        target,
        reason: reason.to_string(),
        time: Utc::now(),
    };
    api.patch_status(
        member,
        &PatchParams::default(),
        &Patch::Merge(json!({ "status": { "scalingDecision": decision } })),
    )
    .await?;
    Ok(())
}

/// The other pools selecting machine `member` that do not (yet) agree to power
/// it off, with their reason: those not listing it in `status.releasable`.
/// A pool that has not evaluated it yet keeps it on.
pub fn other_pools_holding(
    member: &str,
    member_of: &[String],
    this_pool: &str,
    pools: &[Arc<NodeScalingPool>],
) -> Vec<(String, String)> {
    member_of
        .iter()
        .filter(|p| p.as_str() != this_pool)
        .filter_map(|other| {
            let st = pools
                .iter()
                .find(|p| p.name_any() == *other)
                .and_then(|p| p.status.clone())
                .unwrap_or_default();
            if st.releasable.iter().any(|m| m == member) {
                return None;
            }
            let why = st
                .needed
                .get(member)
                .cloned()
                .unwrap_or_else(|| "not evaluated yet".into());
            Some((other.clone(), why))
        })
        .collect()
}

/// Problems with `scaleUp.preferredNodes`: entries naming no Node, and entries
/// naming a Node this pool's `nodeSelector` does not select. Such entries are
/// harmless (they never match a member) but almost certainly a mistake.
pub fn preferred_node_problems<'a>(
    pool: &crate::crd::NodeScalingPoolSpec,
    labels_of: impl Fn(&str) -> Option<&'a std::collections::BTreeMap<String, String>>,
) -> (Vec<String>, Vec<String>) {
    let (mut missing, mut outside) = (Vec::new(), Vec::new());
    for p in &pool.scale_up.preferred_nodes {
        match labels_of(&p.name) {
            None => missing.push(p.name.clone()),
            Some(labels) if !pool.node_selector.matches(labels) => outside.push(p.name.clone()),
            Some(_) => {}
        }
    }
    (missing, outside)
}

/// Sets `PreferredNodesValid` and logs (once per change) what is wrong.
fn check_preferred_nodes(pool: &NodeScalingPool, ctx: &Context, st: &mut NodeScalingPoolStatus) {
    let name = pool.name_any();
    let nodes: Vec<Arc<k8s_openapi::api::core::v1::Node>> = pool
        .spec
        .scale_up
        .preferred_nodes
        .iter()
        .filter_map(|p| ctx.node(&p.name))
        .collect();
    let (missing, outside) = preferred_node_problems(&pool.spec, |n| {
        nodes.iter().find(|node| node.name_any() == n).map(|node| node.labels())
    });
    let mut problems = Vec::new();
    if !missing.is_empty() {
        problems.push(format!("no such Node: {}", missing.join(", ")));
    }
    if !outside.is_empty() {
        problems.push(format!(
            "not selected by this pool's nodeSelector: {}",
            outside.join(", ")
        ));
    }
    let key = format!("{name}/preferredNodes");
    if problems.is_empty() {
        ctx.first_time(&key, None);
        if pool.spec.scale_up.preferred_nodes.is_empty() {
            st.conditions.retain(|c| c.type_ != COND_PREFERRED_NODES_VALID);
        } else {
            set_condition(
                &mut st.conditions,
                COND_PREFERRED_NODES_VALID,
                "True",
                "AllSelected",
                "",
                Utc::now(),
            );
        }
        return;
    }
    let message = format!(
        "scaleUp.preferredNodes entries ignored ({}); the other entries apply",
        problems.join("; ")
    );
    if ctx.first_time(&key, Some(&message)) {
        warn!(pool = %name, "{message}");
    }
    let reason = if missing.is_empty() {
        "NotInPool"
    } else {
        "UnknownNodes"
    };
    set_condition(
        &mut st.conditions,
        COND_PREFERRED_NODES_VALID,
        "False",
        reason,
        message,
        Utc::now(),
    );
}

/// Appends this cycle's decisions to `recent`: every power action, and a
/// `NoAction` only when its reason differs from the latest `NoAction`'s.
/// Returns the decisions that were new.
fn record_decisions(
    recent: &mut Vec<DecisionRecord>,
    decisions: &[PlannedDecision],
    now: chrono::DateTime<Utc>,
) -> Vec<PlannedDecision> {
    let mut new = Vec::new();
    for d in decisions {
        if d.action == DecisionAction::NoAction {
            let latest = recent.last();
            if latest.is_some_and(|r| r.action == DecisionAction::NoAction && r.reason == d.reason) {
                continue;
            }
        }
        recent.push(DecisionRecord {
            time: now,
            action: d.action,
            node: d.node.clone(),
            reason: d.reason.clone(),
            candidates: d.candidates.clone(),
        });
        new.push(d.clone());
    }
    let excess = recent.len().saturating_sub(MAX_RECENT_DECISIONS);
    recent.drain(..excess);
    new
}

pub async fn reconcile(pool: Arc<NodeScalingPool>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = pool.name_any();
    let now = Utc::now();
    let old = pool.status.clone().unwrap_or_default();
    let mut st = old.clone();

    let deprecation = pool.spec.scale_down.deprecation_warning();
    if ctx.first_time(&format!("{name}/deprecated"), deprecation.as_deref())
        && let Some(message) = &deprecation
    {
        warn!(pool = %name, "{message}");
    }
    check_preferred_nodes(&pool, &ctx, &mut st);

    let pools = ctx.pools();
    let mut managed: Vec<Arc<NodePowerManagementConfig>> = Vec::new();
    let mut members: Vec<Member> = Vec::new();
    let mut kept_by_others: std::collections::BTreeMap<String, Vec<(String, String)>> = Default::default();
    for mn in ctx.node_power_management_configs.state() {
        let node = ctx.node(&mn.spec.node_name);
        let Membership::Member(member_of) =
            membership::resolve(node.as_deref().map(|n| n.labels()), pools.iter().map(|p| p.as_ref()))
        else {
            continue;
        };
        if !member_of.contains(&name) {
            continue;
        }
        let holds = other_pools_holding(&mn.name_any(), &member_of, &name, &pools);
        if !holds.is_empty() {
            kept_by_others.insert(mn.name_any(), holds);
        }
        if let Some(m) = build_member(&mn, &member_of, &pool, &pools, &ctx) {
            members.push(m);
        }
        managed.push(mn);
    }
    let external = external_nodes(&ctx);
    let pending: Vec<PodView> = ctx
        .pods
        .state()
        .iter()
        .filter(|p| is_unschedulable(p))
        .map(|p| PodView::from_pod(p))
        .collect();

    let snapshot = PoolSnapshot {
        spec: &pool.spec,
        members,
        pending,
        now,
        last_scale_up: old.last_scale_up_time,
        unneeded_since: old.unneeded_since.clone(),
        external,
        kept_by_others,
    };
    let result = plan(&snapshot);

    let new_decisions = record_decisions(&mut st.recent_decisions, &result.decisions, now);
    let verbosity = ctx.decision_verbosity;
    if !new_decisions.is_empty() {
        for line in &result.live_details {
            if verbosity == DecisionVerbosity::Detailed {
                info!(pool = %name, "candidate {line}");
            } else {
                debug!(pool = %name, "candidate {line}");
            }
        }
    }
    let mn_api: Api<NodePowerManagementConfig> = Api::all(ctx.client.clone());
    for d in &new_decisions {
        let (target, event) = match d.action {
            DecisionAction::PowerOn => (PowerTarget::On, "ScaleUpSelected"),
            DecisionAction::PowerOff => (PowerTarget::Off, "ScaleDownSelected"),
            DecisionAction::NoAction => {
                if verbosity == DecisionVerbosity::Quiet {
                    debug!(pool = %name, reason = %d.live_reason, candidates = %d.candidates, "no scaling action");
                    continue;
                }
                info!(pool = %name, reason = %d.live_reason, candidates = %d.candidates, "no scaling action");
                let event = if d.reason.starts_with("pod ") {
                    Some("ScaleUpBlocked")
                } else if d.reason.starts_with("scale-down held") {
                    Some("ScaleDownBlocked")
                } else {
                    None
                };
                if let Some(event) = event {
                    ctx.event(pool.as_ref(), EventType::Normal, event, d.reason.clone())
                        .await;
                }
                continue;
            }
        };
        let member = d.node.as_deref().unwrap_or_default();
        info!(
            pool = %name,
            nodepowermanagementconfig = %member,
            reason = %d.reason,
            candidates = %d.candidates,
            ?target,
            "scaling decision"
        );
        decide(&mn_api, &name, member, target, &d.reason).await?;
        let note = format!("{member}: {}", d.reason);
        ctx.event(pool.as_ref(), EventType::Normal, event, note).await;
        if let Some(mn) = managed.iter().find(|m| m.name_any() == member) {
            ctx.event(
                mn.as_ref(),
                EventType::Normal,
                event,
                format!("NodeScalingPool {name}: {}", d.reason),
            )
            .await;
        }
    }

    let online_after =
        result.online as usize + result.power_on.len() - result.power_off.len().min(result.online as usize);
    let mut member_names: Vec<String> = managed.iter().map(|m| m.name_any()).collect();
    member_names.sort();
    st.members = member_names;
    st.conflicts = Vec::new();
    st.releasable = result.releasable.clone();
    st.needed = result.needed.clone();
    st.total_nodes = managed.len() as u32;
    st.online_nodes = online_after as u32;
    st.pending_pods = result.relevant_pending;
    st.unneeded_since = result.unneeded_since;
    st.message = Some(result.message);
    if !result.power_on.is_empty() {
        st.last_scale_up_time = Some(now);
    }
    if !result.power_off.is_empty() {
        st.last_scale_down_time = Some(now);
    }
    ctx.metrics
        .pool(&name, st.online_nodes, st.total_nodes, st.pending_pods);

    let api: Api<NodeScalingPool> = Api::all(ctx.client.clone());
    patch_status_diff(&api, &name, &old, &st).await?;
    Ok(Action::requeue(INTERVAL))
}

pub fn error_policy(pool: Arc<NodeScalingPool>, err: &Error, ctx: Arc<Context>) -> Action {
    warn!(pool = %pool.name_any(), error = %err, "reconcile failed");
    ctx.metrics.reconcile_error("nodescalingpool");
    Action::requeue(INTERVAL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{NodePowerManagementConfigSpec, NodePowerManagementConfigStatus, PowerInterface};

    fn mn(
        policy: PowerPolicy,
        phase: Phase,
        ready: bool,
        decision: Option<(&str, PowerTarget)>,
    ) -> NodePowerManagementConfig {
        let mut m = NodePowerManagementConfig::new(
            "n",
            NodePowerManagementConfigSpec {
                node_name: "n".into(),
                power_policy: policy,
                power_interfaces: vec![PowerInterface {
                    name: None,
                    actions: None,
                    timeout_seconds: None,
                    driver: "ipmi".into(),
                    credentials_secret_ref: None,
                    config: json!({}),
                }],
                lifecycle: Default::default(),
                manual_power_on_policy: None,
                manual_power_off_cooldown_seconds: None,
            },
        );
        m.status = Some(NodePowerManagementConfigStatus {
            phase,
            node_ready: ready,
            scaling_decision: decision.map(|(pool, target)| ScalingDecision {
                pool: pool.into(),
                target,
                reason: String::new(),
                time: Utc::now(),
            }),
            ..Default::default()
        });
        m
    }

    #[test]
    fn maps_member_states() {
        use MemberState::*;
        let s = |m: NodePowerManagementConfig| member_state(&m, &["p".to_string()]);
        assert_eq!(s(mn(PowerPolicy::Auto, Phase::On, true, None)), Online);
        assert_eq!(s(mn(PowerPolicy::Auto, Phase::Off, false, None)), Offline);
        assert_eq!(
            s(mn(PowerPolicy::Auto, Phase::Off, false, Some(("p", PowerTarget::On)))),
            Booting
        );
        assert_eq!(
            s(mn(PowerPolicy::Auto, Phase::On, true, Some(("p", PowerTarget::Off)))),
            Leaving
        );
        assert_eq!(
            s(mn(PowerPolicy::Auto, Phase::Off, false, Some(("p", PowerTarget::Off)))),
            Offline
        );
        assert_eq!(
            s(mn(
                PowerPolicy::Auto,
                Phase::Standby,
                false,
                Some(("p", PowerTarget::Off))
            )),
            Offline
        );
        assert_eq!(s(mn(PowerPolicy::Auto, Phase::Standby, false, None)), Offline);
        assert_eq!(
            s(mn(PowerPolicy::Auto, Phase::Error, false, Some(("p", PowerTarget::On)))),
            Unavailable
        );
        assert_eq!(s(mn(PowerPolicy::AlwaysOn, Phase::On, true, None)), Online);
        assert_eq!(s(mn(PowerPolicy::AlwaysOff, Phase::On, true, None)), Unavailable);
    }

    #[test]
    fn decisions_from_other_pools_are_ignored() {
        // A stale Off from a deleted pool must not make the machine look Leaving.
        let m = mn(
            PowerPolicy::Auto,
            Phase::On,
            true,
            Some(("deleted-pool", PowerTarget::Off)),
        );
        assert_eq!(member_state(&m, &["p".to_string()]), MemberState::Online);
    }

    #[test]
    fn inconsistent_machines_are_unavailable() {
        let mut m = mn(PowerPolicy::Auto, Phase::On, true, None);
        m.status.as_mut().unwrap().set_condition(
            COND_POWER_STATE_CONSISTENT,
            "False",
            "InterfaceReportsOffButNodeIsAlive",
            "",
            Utc::now(),
        );
        assert_eq!(member_state(&m, &["p".to_string()]), MemberState::Unavailable);
    }

    fn decision(action: DecisionAction, node: Option<&str>, reason: &str) -> PlannedDecision {
        PlannedDecision {
            action,
            node: node.map(String::from),
            reason: reason.into(),
            live_reason: reason.into(),
            candidates: String::new(),
        }
    }

    #[test]
    fn records_actions_always_and_no_action_on_change_only() {
        let now = Utc::now();
        let mut recent = Vec::new();
        let idle = [decision(
            DecisionAction::NoAction,
            None,
            "1 online, nothing pending, nothing unneeded",
        )];
        assert_eq!(record_decisions(&mut recent, &idle, now).len(), 1);
        assert!(
            record_decisions(&mut recent, &idle, now).is_empty(),
            "unchanged: not recorded again"
        );
        let on = [decision(
            DecisionAction::PowerOn,
            Some("gpu-node-1"),
            "unschedulable pod a/b",
        )];
        assert_eq!(record_decisions(&mut recent, &on, now).len(), 1);
        // The same reason for doing nothing after an action is a new decision.
        assert_eq!(record_decisions(&mut recent, &idle, now).len(), 1);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[1].node.as_deref(), Some("gpu-node-1"));

        // Bounded, oldest dropped first.
        for i in 0..30 {
            let d = [decision(DecisionAction::NoAction, None, &format!("reason {i}"))];
            record_decisions(&mut recent, &d, now);
        }
        assert_eq!(recent.len(), MAX_RECENT_DECISIONS);
        assert_eq!(recent.last().unwrap().reason, "reason 29");
        assert_eq!(recent[0].reason, "reason 10");
    }

    #[test]
    fn preferred_node_problems_names_missing_and_outside_nodes() {
        use crate::crd::{NodeScalingPoolSpec, PreferredNode};
        use std::collections::BTreeMap;
        let spec: NodeScalingPoolSpec = serde_json::from_value(json!({
            "nodeSelector": {"matchLabels": {"gpu": "true"}},
            "scaleUp": {"preferredNodes": [
                {"name": "gpu-node-1", "weight": 100},
                {"name": "gone", "weight": 50},
                {"name": "cpu-box", "weight": 10},
            ]}
        }))
        .unwrap();
        assert_eq!(
            spec.scale_up.preferred_nodes[0],
            PreferredNode {
                name: "gpu-node-1".into(),
                weight: 100
            }
        );
        let gpu = BTreeMap::from([("gpu".to_string(), "true".to_string())]);
        let cpu = BTreeMap::new();
        let (missing, outside) = preferred_node_problems(&spec, |n| match n {
            "gpu-node-1" => Some(&gpu),
            "cpu-box" => Some(&cpu),
            _ => None,
        });
        assert_eq!(missing, ["gone"]);
        assert_eq!(outside, ["cpu-box"]);
    }

    #[test]
    fn other_pools_hold_a_machine_until_they_release_it() {
        use crate::crd::{NodeScalingPoolSpec, NodeScalingPoolStatus};
        use std::collections::BTreeMap;
        let mk = |name: &str, status: Option<NodeScalingPoolStatus>| {
            let spec: NodeScalingPoolSpec =
                serde_json::from_value(json!({"nodeSelector": {"matchLabels": {name: "true"}}})).unwrap();
            let mut p = NodeScalingPool::new(name, spec);
            p.status = status;
            Arc::new(p)
        };
        let member_of = vec!["ci".to_string(), "gpu".to_string()];
        let busy = NodeScalingPoolStatus {
            needed: BTreeMap::from([("gpu-node-2".to_string(), "busy: utilization 50% >= 10%".to_string())]),
            ..Default::default()
        };
        let pools = vec![mk("gpu", None), mk("ci", Some(busy))];
        assert_eq!(
            other_pools_holding("gpu-node-2", &member_of, "gpu", &pools),
            vec![("ci".to_string(), "busy: utilization 50% >= 10%".to_string())]
        );
        // From ci's side, gpu has not evaluated it yet: it keeps it on too.
        assert_eq!(
            other_pools_holding("gpu-node-2", &member_of, "ci", &pools),
            vec![("gpu".to_string(), "not evaluated yet".to_string())]
        );
        let releases = NodeScalingPoolStatus {
            releasable: vec!["gpu-node-2".into()],
            ..Default::default()
        };
        let pools = vec![mk("gpu", None), mk("ci", Some(releases))];
        assert!(other_pools_holding("gpu-node-2", &member_of, "gpu", &pools).is_empty());
        // A machine in one pool only is never held by another.
        assert!(other_pools_holding("gpu-node-2", &["gpu".to_string()], "gpu", &pools).is_empty());
    }

    mod special {
        use super::super::*;
        use crate::crd::{BootFailure, ManualPowerChange, NodeScalingPoolSpec};

        fn pool() -> NodeScalingPoolSpec {
            serde_json::from_value(json!({"nodeSelector": {"matchLabels": {"example.com/gpu": "true"}}})).unwrap()
        }

        fn st(phase: Phase, ready: bool) -> NodePowerManagementConfigStatus {
            NodePowerManagementConfigStatus {
                phase,
                node_ready: ready,
                ..Default::default()
            }
        }

        #[test]
        fn a_boot_failure_is_unavailable_then_backed_off_then_wakeable() {
            let now = Utc::now();
            let spec = pool();
            let mut s = st(Phase::BootFailed, false);
            s.boot_failure = Some(BootFailure {
                time: now,
                left_on: false,
            });
            let c = classify(
                &s,
                MemberState::Booting,
                ManualPowerOnPolicy::LeaveOn,
                &spec,
                1200,
                0,
                now,
            );
            assert_eq!(c.state, Some(MemberState::Unavailable));
            assert!(
                c.note
                    .unwrap()
                    .contains("on 20m+ without Ready; released hold; powering off")
            );

            // Powered off: not woken again within the backoff...
            s.phase = Phase::Off;
            let c = classify(
                &s,
                MemberState::Offline,
                ManualPowerOnPolicy::LeaveOn,
                &spec,
                1200,
                0,
                now,
            );
            assert_eq!(c.state, Some(MemberState::Unavailable));
            assert!(c.note.unwrap().contains("not woken again until"));
            // ...and wakeable after it.
            let later = now + chrono::Duration::seconds(1801);
            let c = classify(
                &s,
                MemberState::Offline,
                ManualPowerOnPolicy::LeaveOn,
                &spec,
                1200,
                0,
                later,
            );
            assert_eq!(c, Special::default());
        }

        #[test]
        fn manual_power_on_under_each_policy() {
            let now = Utc::now();
            let spec = pool();
            let mut s = st(Phase::On, true);
            s.manual_power_on = Some(ManualPowerChange {
                time: now,
                policy: None,
            });
            let c = classify(
                &s,
                MemberState::Online,
                ManualPowerOnPolicy::LeaveOn,
                &spec,
                1200,
                0,
                now,
            );
            assert_eq!(c.state, Some(MemberState::Manual));
            assert!(c.note.unwrap().ends_with("policy LeaveOn; not managing"));
            // Not Ready yet under LeaveOn: neither booting nor capacity.
            s.node_ready = false;
            let c = classify(
                &s,
                MemberState::Booting,
                ManualPowerOnPolicy::LeaveOn,
                &spec,
                1200,
                0,
                now,
            );
            assert_eq!(c.state, Some(MemberState::Unavailable));
            s.node_ready = true;

            let c = classify(&s, MemberState::Online, ManualPowerOnPolicy::Adopt, &spec, 1200, 0, now);
            assert_eq!((c.state, c.adopted_at), (None, Some(now)));

            let c = classify(
                &s,
                MemberState::Online,
                ManualPowerOnPolicy::PowerOff,
                &spec,
                1200,
                0,
                now,
            );
            assert_eq!(c.release_after, Some(now + chrono::Duration::seconds(600)));
        }

        #[test]
        fn manual_power_off_returns_to_management_with_an_optional_cooldown() {
            let now = Utc::now();
            let spec = pool();
            let mut s = st(Phase::Off, false);
            s.manual_power_off = Some(ManualPowerChange {
                time: now,
                policy: None,
            });
            let c = classify(
                &s,
                MemberState::Offline,
                ManualPowerOnPolicy::LeaveOn,
                &spec,
                1200,
                0,
                now,
            );
            assert_eq!(c.state, None, "no cooldown: can be woken on demand");
            assert!(c.note.unwrap().ends_with("back under management"));
            let c = classify(
                &s,
                MemberState::Offline,
                ManualPowerOnPolicy::LeaveOn,
                &spec,
                1200,
                900,
                now,
            );
            assert_eq!(c.state, Some(MemberState::Unavailable));
        }
    }
}
