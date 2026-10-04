//! `NodeScalingPool` controller: periodically evaluates demand for the machines its
//! `nodeSelector` selects and records power decisions on them
//! (`NodePowerManagementConfig.status.scalingDecision`, stamped with the pool name), which
//! the `NodePowerManagementConfig` controller then carries out.
//!
//! Membership is resolved from Node labels with the same function the
//! `NodePowerManagementConfig` controller uses, so both always agree. Machines selected by
//! more than one pool are conflicts and belong to none.

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
    NodePowerManagementConfig, NodeScalingPool, NodeScalingPoolStatus, Phase, PowerPolicy, PowerTarget,
    ResourceAmounts, ScalingDecision, set_condition,
};
use crate::membership::{self, Membership};
use crate::resources::{node_allocatable, pod_requests};
use crate::scaling::{
    DrainClass, ExternalNode, Member, MemberState, PlannedDecision, PodView, PoolSnapshot, drain_class, is_active,
    is_daemonset_pod, is_unschedulable, plan,
};

const INTERVAL: Duration = Duration::from_secs(15);

/// Maps a member NodePowerManagementConfig to its autoscaler state. `pool` is this pool's
/// name: decisions made by any other pool are ignored.
pub fn member_state(mn: &NodePowerManagementConfig, pool: &str) -> MemberState {
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
        .filter(|d| d.pool == pool)
        .map(|d| d.target);
    match decision {
        Some(PowerTarget::Off) if matches!(st.phase, Phase::Off | Phase::Standby) => MemberState::Offline,
        Some(PowerTarget::Off) => MemberState::Leaving,
        Some(PowerTarget::On) if ready_on => MemberState::Online,
        Some(PowerTarget::On) if st.phase == Phase::Error => MemberState::Unavailable,
        Some(PowerTarget::On) => MemberState::Booting,
        None => match st.phase {
            Phase::On if st.node_ready => MemberState::Online,
            Phase::On | Phase::PoweringOn => MemberState::Booting,
            Phase::Off | Phase::Standby => MemberState::Offline,
            Phase::Draining | Phase::PoweringOff => MemberState::Leaving,
            Phase::Unknown | Phase::Error => MemberState::Unavailable,
        },
    }
}

fn build_member(mn: &NodePowerManagementConfig, pool: &NodeScalingPool, ctx: &Context) -> Option<Member> {
    let pool_name = pool.name_any();
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

    Some(Member {
        name: mn.name_any(),
        state: member_state(mn, &pool_name),
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
    let mut conflicts: Vec<String> = Vec::new();
    for mn in ctx.node_power_management_configs.state() {
        let node = ctx.node(&mn.spec.node_name);
        match membership::resolve(node.as_deref().map(|n| n.labels()), pools.iter().map(|p| p.as_ref())) {
            Membership::Member(p) if p == name => managed.push(mn),
            Membership::Conflict(ps) if ps.contains(&name) => conflicts.push(mn.name_any()),
            _ => {}
        }
    }
    let members: Vec<Member> = managed.iter().filter_map(|mn| build_member(mn, &pool, &ctx)).collect();
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
    conflicts.sort();
    st.members = member_names;
    st.conflicts = conflicts;
    st.total_nodes = managed.len() as u32;
    st.online_nodes = online_after as u32;
    st.pending_pods = result.relevant_pending;
    st.unneeded_since = result.unneeded_since;
    st.message = Some(if st.conflicts.is_empty() {
        result.message
    } else {
        format!(
            "{} (excluded, selected by several pools: {})",
            result.message,
            st.conflicts.join(", ")
        )
    });
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
        let s = |m: NodePowerManagementConfig| member_state(&m, "p");
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
        assert_eq!(member_state(&m, "p"), MemberState::Online);
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
        assert_eq!(member_state(&m, "p"), MemberState::Unavailable);
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
            Some("devbox"),
            "unschedulable pod a/b",
        )];
        assert_eq!(record_decisions(&mut recent, &on, now).len(), 1);
        // The same reason for doing nothing after an action is a new decision.
        assert_eq!(record_decisions(&mut recent, &idle, now).len(), 1);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[1].node.as_deref(), Some("devbox"));

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
                {"name": "devbox", "weight": 100},
                {"name": "gone", "weight": 50},
                {"name": "cpu-box", "weight": 10},
            ]}
        }))
        .unwrap();
        assert_eq!(
            spec.scale_up.preferred_nodes[0],
            PreferredNode {
                name: "devbox".into(),
                weight: 100
            }
        );
        let gpu = BTreeMap::from([("gpu".to_string(), "true".to_string())]);
        let cpu = BTreeMap::new();
        let (missing, outside) = preferred_node_problems(&spec, |n| match n {
            "devbox" => Some(&gpu),
            "cpu-box" => Some(&cpu),
            _ => None,
        });
        assert_eq!(missing, ["gone"]);
        assert_eq!(outside, ["cpu-box"]);
    }
}
