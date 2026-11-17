//! Pure autoscaling logic: given a snapshot of a pool and the cluster's
//! pending pods, decide which machines to power on or off.
//!
//! The scheduling model is deliberately simple (resources, node selectors,
//! required node affinity and taints/tolerations) - it only needs to be good
//! enough to decide whether powering a machine on could help, and whether the
//! pods of an idle machine would fit elsewhere.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use k8s_openapi::api::core::v1::{NodeSelectorRequirement, Pod, Taint, Toleration};

use crate::crd::{DecisionAction, NodeScalingPoolSpec, ResourceAmounts, SAFE_TO_EVICT_ANNOTATION, Strategy};
use crate::resources::pod_requests;

/// Where a member currently is in its power lifecycle, from the autoscaler's
/// point of view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemberState {
    /// Powered on and Ready: capacity is available.
    Online,
    /// Powering on (or asked to): capacity will soon be available.
    Booting,
    /// Powered off and not asked to power on.
    Offline,
    /// Draining or powering off (or asked to).
    Leaving,
    /// State unknown or errored, failed to boot, or held back (boot failure
    /// backoff, manual power-off cooldown); not touched by the autoscaler.
    Unavailable,
    /// Powered on by hand under `manualPowerOnPolicy: LeaveOn`, and Ready: never
    /// powered off, not counted as booting or towards `maxOnline`, but room for
    /// pods evicted elsewhere.
    Manual,
}

/// Scheduling-relevant view of a pod.
#[derive(Clone, Debug, Default)]
pub struct PodView {
    pub namespace: String,
    pub name: String,
    pub requests: ResourceAmounts,
    pub node_selector: BTreeMap<String, String>,
    /// Required node affinity: OR of terms, each an AND of requirements.
    pub affinity_terms: Vec<Vec<NodeSelectorRequirement>>,
    pub tolerations: Vec<Toleration>,
    pub created: Option<DateTime<Utc>>,
    /// Extended resource requests (e.g. `gpu.intel.com/i915`).
    pub extended_requests: BTreeMap<String, i64>,
}

#[derive(Clone, Debug)]
pub struct Member {
    /// NodePowerManagementConfig name.
    pub name: String,
    pub state: MemberState,
    /// Whether the autoscaler may change this member's power (`powerPolicy: Auto`).
    pub auto: bool,
    pub labels: BTreeMap<String, String>,
    pub taints: Vec<Taint>,
    pub allocatable: ResourceAmounts,
    /// Sum of requests of all non-terminated pods bound to the node.
    pub requested: ResourceAmounts,
    /// The part of `requested` that counts towards utilization (pool options can
    /// leave out DaemonSet pods and pods that do not target the pool).
    pub counted: ResourceAmounts,
    /// Pods that would have to be rescheduled if this node went away.
    pub movable_pods: Vec<PodView>,
    /// Pods that block powering the node off.
    pub blocking_pods: Vec<String>,
    pub drain_failed_at: Option<DateTime<Utc>>,
    /// When a scale-down last found someone logged in, and who
    /// (`lifecycle.keepOnWhileLoggedIn`).
    pub logged_in: Option<(DateTime<Utc>, Vec<String>)>,
    /// Whether the machine can sleep in S3 (see `scaleUp.preferS3Capable`).
    pub s3_capable: bool,
    /// Extended resources (e.g. `gpu.intel.com/i915`) the machine had before
    /// and reports as 0 shortly after booting, with their last-known amounts:
    /// its device plugin has not registered them yet.
    pub registering: BTreeMap<String, i64>,
    /// The pool whose power-on decision is in force for this machine, if any
    /// (with overlapping pools, possibly another pool than the one planning).
    pub woken_by: Option<String>,
    /// Why the member is in a special state (boot failure, manual power
    /// change), for the decision log.
    pub note: Option<String>,
    /// Powered on by hand under `Adopt`: counts as this pool's power-on then.
    pub adopted_at: Option<DateTime<Utc>>,
    /// Powered on by hand under `PowerOff`: powered off once this has passed.
    pub release_after: Option<DateTime<Utc>>,
    /// Outside its `activeHours`, and why: powered off as soon as it is idle,
    /// without waiting out `unneededSeconds`.
    pub outside_active_hours: Option<String>,
    /// The machine's own `lifecycle.unneededSeconds`, over the pool's.
    pub unneeded_seconds: Option<u64>,
    /// PowerRequests holding the machine on (their labels, `; `-joined): it is
    /// woken if off, and never unneeded while they last.
    pub power_requests: Option<String>,
    /// What the operator has learned about the machine (boots, probes).
    pub history: MachineHistory,
}

/// What the operator has learned about a machine, for `strategy: Scored`
/// and `hedge`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MachineHistory {
    /// Mean seconds to Ready of its recent successful boots.
    pub boot_average_seconds: Option<f64>,
    pub boots_succeeded: u32,
    pub boots_failed: u32,
    /// Whether its management path answered the last probe; `None` when it
    /// is not probed.
    pub reachable: Option<bool>,
    /// Share of its recent probes answered, 0.0 to 1.0.
    pub answered: Option<f64>,
    pub rtt_ms: Option<u64>,
    /// When the boot in progress began (members that are `Booting`).
    pub booting_since: Option<DateTime<Utc>>,
    /// This decision's draw for the `Random` strategy, 0.0 to 1.0.
    pub lottery: f64,
    /// Seconds until its active hours end, when it has them and is inside
    /// them; `None` when it has no end within a week.
    pub active_seconds_left: Option<i64>,
}

/// A schedulable node outside every scaling pool: somewhere evicted pods can
/// go when a member powers off.
#[derive(Clone, Debug)]
pub struct ExternalNode {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub taints: Vec<Taint>,
    /// allocatable minus the requests of the pods already running there.
    pub free: ResourceAmounts,
}

fn external_key(name: &str) -> String {
    format!("external/{name}")
}

pub struct PoolSnapshot<'a> {
    pub spec: &'a NodeScalingPoolSpec,
    pub members: Vec<Member>,
    /// All unschedulable pods in the cluster.
    pub pending: Vec<PodView>,
    pub now: DateTime<Utc>,
    pub last_scale_up: Option<DateTime<Utc>>,
    pub unneeded_since: BTreeMap<String, DateTime<Utc>>,
    /// Nodes outside every pool that can absorb evicted pods.
    pub external: Vec<ExternalNode>,
    /// Members also selected by other pools that do not agree to power them
    /// off: member -> [(pool, its reason)]. Such a member is never powered off.
    pub kept_by_others: BTreeMap<String, Vec<(String, String)>>,
}

#[derive(Debug, Default, PartialEq)]
pub struct Plan {
    /// (member, reason)
    pub power_on: Vec<(String, String)>,
    pub power_off: Vec<(String, String)>,
    pub unneeded_since: BTreeMap<String, DateTime<Utc>>,
    /// Pending pods (past the grace period) that this pool could host.
    pub relevant_pending: u32,
    /// Members online or booting, before applying this plan.
    pub online: u32,
    pub message: String,
    /// One decision per power action, or a single `NoAction` explaining why
    /// nothing happens.
    pub decisions: Vec<PlannedDecision>,
    /// Per machine: its rank and why it is (not) a candidate. `details` is
    /// stable (for status), `live_details` has seconds (for logs).
    pub details: Vec<String>,
    pub live_details: Vec<String>,
    /// Members this pool agrees may be powered off now (`status.releasable`).
    pub releasable: Vec<String>,
    /// Why this pool keeps each other online member on (`status.needed`).
    pub needed: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlannedDecision {
    pub action: DecisionAction,
    pub node: Option<String>,
    /// One line: why, the rank and the deciding rule. Stable while the
    /// situation is (absolute times only), so it can be compared across cycles.
    pub reason: String,
    /// `reason` with elapsed and remaining seconds, for logs.
    pub live_reason: String,
    /// The ranked candidates, compact: `a(w100 s3 16c/64Gi) > b(w10 8c/32Gi)`.
    pub candidates: String,
}

/// Taints that result from the node being powered off or cordoned by us, and
/// therefore say nothing about whether a pod could run there once it is on.
/// How long a machine someone was found logged in to is kept before a
/// scale-down checks it again (`lifecycle.keepOnWhileLoggedIn`).
pub const LOGIN_RECHECK_SECS: i64 = 300;

const TRANSIENT_TAINTS: &[&str] = &[
    "node.kubernetes.io/unschedulable",
    "node.kubernetes.io/not-ready",
    "node.kubernetes.io/unreachable",
    "node.kubernetes.io/network-unavailable",
    "node.cloudprovider.kubernetes.io/shutdown",
    // Set for non-graceful node shutdown handling while the machine is off.
    "node.kubernetes.io/out-of-service",
    // Cilium taints nodes until its agent is running again after boot.
    "node.cilium.io/agent-not-ready",
];

fn tolerates(tolerations: &[Toleration], taint: &Taint) -> bool {
    tolerations.iter().any(|t| {
        let effect_ok = t.effect.as_deref().is_none_or(|e| e.is_empty() || e == taint.effect);
        let key_ok = match t.key.as_deref() {
            None | Some("") => t.operator.as_deref() == Some("Exists"),
            Some(k) => k == taint.key,
        };
        let value_ok = match t.operator.as_deref() {
            Some("Exists") => true,
            _ => t.value.as_deref().unwrap_or("") == taint.value.as_deref().unwrap_or(""),
        };
        effect_ok && key_ok && value_ok
    })
}

fn requirement_matches(req: &NodeSelectorRequirement, labels: &BTreeMap<String, String>) -> bool {
    let value = labels.get(&req.key);
    let values = req.values.as_deref().unwrap_or(&[]);
    let as_int = |s: &str| s.parse::<i64>().ok();
    match req.operator.as_str() {
        "In" => value.is_some_and(|v| values.contains(v)),
        "NotIn" => value.is_none_or(|v| !values.contains(v)),
        "Exists" => value.is_some(),
        "DoesNotExist" => value.is_none(),
        "Gt" => {
            matches!((value.and_then(|v| as_int(v)), values.first().and_then(|v| as_int(v))), (Some(a), Some(b)) if a > b)
        }
        "Lt" => {
            matches!((value.and_then(|v| as_int(v)), values.first().and_then(|v| as_int(v))), (Some(a), Some(b)) if a < b)
        }
        _ => false,
    }
}

/// Whether `pod` could be placed on a node with these labels and taints,
/// ignoring resources.
pub fn pod_matches_node(pod: &PodView, labels: &BTreeMap<String, String>, taints: &[Taint]) -> bool {
    let selector_ok = pod.node_selector.iter().all(|(k, v)| labels.get(k) == Some(v));
    let affinity_ok = pod.affinity_terms.is_empty()
        || pod
            .affinity_terms
            .iter()
            .any(|term| term.iter().all(|r| requirement_matches(r, labels)));
    let taints_ok = taints
        .iter()
        .filter(|t| t.effect == "NoSchedule" || t.effect == "NoExecute")
        .filter(|t| !TRANSIENT_TAINTS.contains(&t.key.as_str()))
        .all(|t| tolerates(&pod.tolerations, t));
    selector_ok && affinity_ok && taints_ok
}

fn pod_fits(pod: &PodView, m: &Member, free: &ResourceAmounts) -> bool {
    free.fits(&pod.requests) && pod_matches_node(pod, &m.labels, &m.taints)
}

impl PodView {
    /// Whether the pod explicitly targets nodes carrying all of `pool`'s
    /// `matchLabels`: through its `nodeSelector`, or through required node
    /// affinity where every term requires the label (`In` with exactly that
    /// value). A pool without `matchLabels` is never explicitly selected.
    pub fn explicitly_selects(&self, pool: &crate::crd::NodeSelector) -> bool {
        if pool.match_labels.is_empty() {
            return false;
        }
        pool.match_labels.iter().all(|(key, value)| {
            self.node_selector.get(key) == Some(value)
                || (!self.affinity_terms.is_empty()
                    && self.affinity_terms.iter().all(|term| {
                        term.iter().any(|r| {
                            r.key == *key
                                && r.operator == "In"
                                && r.values
                                    .as_deref()
                                    .is_some_and(|v| !v.is_empty() && v.iter().all(|x| x == value))
                        })
                    }))
        })
    }

    pub fn from_pod(pod: &Pod) -> Self {
        let spec = pod.spec.as_ref();
        let affinity_terms = spec
            .and_then(|s| s.affinity.as_ref())
            .and_then(|a| a.node_affinity.as_ref())
            .and_then(|na| na.required_during_scheduling_ignored_during_execution.as_ref())
            .map(|sel| {
                sel.node_selector_terms
                    .iter()
                    .map(|t| t.match_expressions.clone().unwrap_or_default())
                    .collect()
            })
            .unwrap_or_default();
        PodView {
            namespace: pod.metadata.namespace.clone().unwrap_or_default(),
            name: pod.metadata.name.clone().unwrap_or_default(),
            requests: pod_requests(pod),
            node_selector: spec.and_then(|s| s.node_selector.clone()).unwrap_or_default(),
            affinity_terms,
            tolerations: spec.and_then(|s| s.tolerations.clone()).unwrap_or_default(),
            extended_requests: crate::resources::pod_extended_requests(pod),
            created: pod
                .metadata
                .creation_timestamp
                .as_ref()
                .and_then(|t| DateTime::from_timestamp(t.0.as_second(), 0)),
        }
    }
}

/// Whether a pod is waiting because no node can host it.
pub fn is_unschedulable(pod: &Pod) -> bool {
    let bound = pod.spec.as_ref().and_then(|s| s.node_name.as_ref()).is_some();
    let gated = pod
        .spec
        .as_ref()
        .and_then(|s| s.scheduling_gates.as_ref())
        .is_some_and(|g| !g.is_empty());
    let pending = pod.status.as_ref().and_then(|s| s.phase.as_deref()) == Some("Pending");
    let unschedulable = pod
        .status
        .as_ref()
        .and_then(|s| s.conditions.as_ref())
        .is_some_and(|cs| {
            cs.iter().any(|c| {
                c.type_ == "PodScheduled" && c.status == "False" && c.reason.as_deref() == Some("Unschedulable")
            })
        });
    !bound && !gated && pending && unschedulable && pod.metadata.deletion_timestamp.is_none()
}

/// Whether a pod is still consuming node resources.
pub fn is_active(pod: &Pod) -> bool {
    !matches!(
        pod.status.as_ref().and_then(|s| s.phase.as_deref()),
        Some("Succeeded") | Some("Failed")
    )
}

pub fn is_daemonset_pod(pod: &Pod) -> bool {
    pod.metadata
        .owner_references
        .iter()
        .flatten()
        .any(|o| o.kind == "DaemonSet")
}

pub fn is_mirror_pod(pod: &Pod) -> bool {
    pod.metadata
        .annotations
        .as_ref()
        .is_some_and(|a| a.contains_key("kubernetes.io/config.mirror"))
}

/// Classification of a pod for draining purposes.
#[derive(Debug, PartialEq, Eq)]
pub enum DrainClass {
    /// Stays on the node (daemonset/mirror/terminated); does not block power off.
    Ignore,
    /// Must be evicted before power off.
    Evict,
    /// Blocks power off.
    Block(&'static str),
}

/// Short-lived pods the operator itself runs on nodes (sleep probe, in-band
/// shutdown/suspend, Wake-on-LAN relay).
pub fn is_operator_helper_pod(pod: &Pod) -> bool {
    let labels = pod.metadata.labels.as_ref();
    let label = |k: &str| labels.and_then(|l| l.get(k)).map(String::as_str);
    label("app.kubernetes.io/name") == Some("kube-hardware-autoscaler")
        && matches!(
            label("app.kubernetes.io/component"),
            Some("sleep-probe" | "session-check" | "shutdown" | "wake-relay")
        )
}

pub fn drain_class(pod: &Pod) -> DrainClass {
    if !is_active(pod) || is_operator_helper_pod(pod) {
        return DrainClass::Ignore;
    }
    let annotation = |k: &str| {
        pod.metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(k))
            .map(String::as_str)
    };
    let safe = annotation(SAFE_TO_EVICT_ANNOTATION).or(annotation("cluster-autoscaler.kubernetes.io/safe-to-evict"));
    // DaemonSet and mirror pods are not evicted, but an explicit
    // safe-to-evict=false still keeps their node powered on (e.g. a
    // DaemonSet worker in the middle of a job).
    if is_daemonset_pod(pod) || is_mirror_pod(pod) {
        return match safe {
            Some("false") => DrainClass::Block("annotated safe-to-evict=false"),
            _ => DrainClass::Ignore,
        };
    }
    match safe {
        Some("false") => DrainClass::Block("annotated safe-to-evict=false"),
        Some("true") => DrainClass::Evict,
        _ if pod.metadata.owner_references.as_ref().is_none_or(|o| o.is_empty()) => {
            DrainClass::Block("not managed by a controller")
        }
        _ => DrainClass::Evict,
    }
}

/// The order in which a pool powers its machines on; scale-down uses the exact
/// reverse. Total, so the choice never depends on the order members were
/// listed. With `scaleUp.strategies` empty:
///
/// 1. `scaleUp.preferredNodes` weight, highest first (unlisted machines weigh 0);
/// 2. with `scaleUp.preferS3Capable`, machines that can sleep in S3 first;
/// 3. size (CPU, then memory), largest first: fewer machines absorb the
///    pending pods, and on the way down the smallest goes first, taking the
///    least capacity away while the larger ones absorb its pods;
/// 4. Node name, ascending (descending on the way down).
///
/// With strategies, see `Ranking::order`.
pub fn power_on_order(spec: &NodeScalingPoolSpec, a: &Member, b: &Member) -> std::cmp::Ordering {
    Ranking::new(spec, [a, b]).order(a, b)
}

/// Ranks machines for one decision: the pool's strategies, and what they
/// rate relative to (the largest candidate, for `Largest`/`Smallest`).
pub struct Ranking<'a> {
    spec: &'a NodeScalingPoolSpec,
    largest: (i64, i64),
}

/// One strategy's part of a machine's score.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Factor {
    pub strategy: Strategy,
    pub weight: u32,
    /// 0.0 to 1.0.
    pub rating: f64,
}

/// A machine's score under `scaleUp.strategies`.
#[derive(Clone, Debug, PartialEq)]
pub struct Score {
    /// The weighted mean of the ratings, 0 to 1000 (so it orders totally).
    pub total: i64,
    pub factors: Vec<Factor>,
}

/// Seconds at which a boot rates 0.5 for `FastestBoot`.
const BOOT_TIME_HALF_SECS: f64 = 120.0;
/// The boot `ActiveHoursRemaining` expects of a machine with no history.
const UNKNOWN_BOOT_SECS: f64 = 300.0;
/// Time left after booting at which `ActiveHoursRemaining` rates 1.
const WORTHWHILE_SECS: f64 = 3600.0;

/// Seconds a machine would have in its active hours once Ready; `None`
/// without an end.
fn usable_seconds(m: &Member) -> Option<f64> {
    let left = m.history.active_seconds_left? as f64;
    Some(left - m.history.boot_average_seconds.unwrap_or(UNKNOWN_BOOT_SECS))
}

impl<'a> Ranking<'a> {
    pub fn new<'m>(spec: &'a NodeScalingPoolSpec, members: impl IntoIterator<Item = &'m Member>) -> Self {
        let largest = members
            .into_iter()
            .map(|m| (m.allocatable.cpu_millis, m.allocatable.memory_bytes))
            .fold((0, 0), |(c, mem), (mc, mm)| (c.max(mc), mem.max(mm)));
        Self { spec, largest }
    }

    fn strategic(&self) -> bool {
        !self.spec.scale_up.strategies.is_empty()
    }

    fn uses(&self, strategy: Strategy) -> bool {
        self.spec.scale_up.strategies.iter().any(|s| s.name == strategy)
    }

    /// With strategies: with `Reachable` listed, machines whose management
    /// interface did not answer go last; then the highest score; then size,
    /// largest first; then name.
    pub fn order(&self, a: &Member, b: &Member) -> std::cmp::Ordering {
        let spec = self.spec;
        if !self.strategic() {
            let key = |m: &Member| {
                (
                    std::cmp::Reverse(spec.scale_up.node_weight(&m.name)),
                    spec.scale_up.prefer_s3_capable && !m.s3_capable,
                    std::cmp::Reverse((m.allocatable.cpu_millis, m.allocatable.memory_bytes)),
                )
            };
            return key(a).cmp(&key(b)).then_with(|| a.name.cmp(&b.name));
        }
        let key = |m: &Member| {
            (
                self.unreachable(m),
                std::cmp::Reverse(self.score(m).total),
                std::cmp::Reverse((m.allocatable.cpu_millis, m.allocatable.memory_bytes)),
            )
        };
        key(a).cmp(&key(b)).then_with(|| a.name.cmp(&b.name))
    }

    /// A machine woken only when no other fits: its management interface
    /// does not answer (`Reachable`), or its active hours end before it
    /// would be Ready (`ActiveHoursRemaining`).
    fn unreachable(&self, m: &Member) -> bool {
        self.last_resort(m).is_some()
    }

    fn last_resort(&self, m: &Member) -> Option<&'static str> {
        if self.uses(Strategy::Reachable) && m.history.reachable == Some(false) {
            Some("Reachable")
        } else if self.uses(Strategy::ActiveHoursRemaining) && usable_seconds(m).is_some_and(|s| s <= 0.0) {
            Some("ActiveHoursRemaining")
        } else {
            None
        }
    }

    fn size_rating(&self, m: &Member) -> f64 {
        let (cpu, mem) = self.largest;
        let frac = |v: i64, max: i64| if max > 0 { v as f64 / max as f64 } else { 1.0 };
        // CPU decides; memory only separates machines with the same CPU.
        0.9 * frac(m.allocatable.cpu_millis, cpu) + 0.1 * frac(m.allocatable.memory_bytes, mem)
    }

    /// How one strategy rates a machine, 0.0 to 1.0.
    pub fn rate(&self, strategy: Strategy, m: &Member) -> f64 {
        let h = &m.history;
        match strategy {
            Strategy::Priority => self.spec.scale_up.node_weight(&m.name) as f64 / crate::crd::MAX_NODE_WEIGHT as f64,
            Strategy::Reachable => match h.reachable {
                None => 0.5,
                Some(last) => {
                    let answered = h.answered.unwrap_or(if last { 1.0 } else { 0.0 });
                    let speed = h.rtt_ms.map_or(1.0, |ms| (1.0 - ms as f64 / 1000.0).max(0.5));
                    answered * speed
                }
            },
            Strategy::FastestBoot => match h.boot_average_seconds {
                Some(secs) => BOOT_TIME_HALF_SECS / (BOOT_TIME_HALF_SECS + secs.max(0.0)),
                None => 0.5,
            },
            Strategy::Reliable => {
                (h.boots_succeeded as f64 + 1.0) / ((h.boots_succeeded + h.boots_failed) as f64 + 2.0)
            }
            Strategy::Standby => {
                if m.s3_capable {
                    1.0
                } else {
                    0.0
                }
            }
            Strategy::Largest => self.size_rating(m),
            Strategy::Smallest => 1.0 - self.size_rating(m),
            Strategy::Random => h.lottery.clamp(0.0, 1.0),
            Strategy::ActiveHoursRemaining => match usable_seconds(m) {
                Some(s) => (s / WORTHWHILE_SECS).clamp(0.0, 1.0),
                None => 1.0,
            },
        }
    }

    /// A machine's score: the weighted mean of its strategies' ratings.
    pub fn score(&self, m: &Member) -> Score {
        let factors: Vec<Factor> = self
            .spec
            .scale_up
            .strategies
            .iter()
            .map(|s| Factor {
                strategy: s.name,
                weight: s.weight,
                rating: self.rate(s.name, m),
            })
            .collect();
        let weights: u32 = factors.iter().map(|f| f.weight).sum();
        let total = if weights == 0 {
            0
        } else {
            let sum: f64 = factors.iter().map(|f| f.weight as f64 * f.rating).sum();
            (sum / weights as f64 * 1000.0).round() as i64
        };
        Score { total, factors }
    }

    /// The traits shown for a machine in the decision log.
    pub fn traits(&self, m: &Member) -> String {
        if !self.strategic() {
            return format!(
                "weight {}, S3 {}, {}",
                self.spec.scale_up.node_weight(&m.name),
                if m.s3_capable { "yes" } else { "no" },
                size(m)
            );
        }
        format!("score {}, {}, {}", self.score(m).total, learned(m), size(m))
    }

    /// Compact tags for the candidate list: `w100 s3 16c/64Gi` or `412 boot 95s`.
    fn tags(&self, m: &Member) -> String {
        if !self.strategic() {
            let mut tags = vec![format!("w{}", self.spec.scale_up.node_weight(&m.name))];
            if m.s3_capable {
                tags.push("s3".into());
            }
            tags.push(size(m));
            return tags.join(" ");
        }
        format!("{} {}", self.score(m).total, size(m))
    }

    /// The rule that puts `chosen` ahead of `other`: for scale-up
    /// (`down == false`) `chosen` ranks before `other`; for scale-down it
    /// ranks after it.
    pub fn rule(&self, chosen: &Member, other: &Member, down: bool) -> String {
        if !self.strategic() {
            return ordered_rule(self.spec, chosen, other, down);
        }
        let (uc, uo) = (self.last_resort(chosen), self.last_resort(other));
        if uc.is_some() != uo.is_some() {
            let (who, why) = if down { (chosen, uc) } else { (other, uo) };
            return match why {
                Some("Reachable") => format!("Reachable ({}'s management interface does not answer)", who.name),
                _ => format!(
                    "ActiveHoursRemaining ({}'s active hours end in {}s, before it would be Ready)",
                    who.name,
                    who.history.active_seconds_left.unwrap_or(0)
                ),
            };
        }
        let (sc, so) = (self.score(chosen), self.score(other));
        if sc.total != so.total {
            // The strategy contributing most to the difference.
            let decisive = sc
                .factors
                .iter()
                .zip(&so.factors)
                .map(|(c, o)| (c.strategy, c.weight as f64 * (c.rating - o.rating)))
                .filter(|(_, d)| *d != 0.0)
                .max_by(|a, b| {
                    if down {
                        (-a.1).total_cmp(&-b.1)
                    } else {
                        a.1.total_cmp(&b.1)
                    }
                })
                .map(|(s, _)| s);
            let why = match decisive {
                Some(s) => format!(", mostly {:?}: {}", s, self.compare(s, chosen, other)),
                None => String::new(),
            };
            return format!("score ({} vs {}{why})", sc.total, so.total);
        }
        let size_key = |m: &Member| (m.allocatable.cpu_millis, m.allocatable.memory_bytes);
        if size_key(chosen) != size_key(other) {
            format!("size ({} vs {})", size(chosen), size(other))
        } else {
            format!("name ({} {} {})", chosen.name, if down { ">" } else { "<" }, other.name)
        }
    }

    /// One strategy's view of two machines, for the decision log.
    fn compare(&self, s: Strategy, a: &Member, b: &Member) -> String {
        let (ha, hb) = (&a.history, &b.history);
        match s {
            Strategy::Priority => format!(
                "weight {} vs {}",
                self.spec.scale_up.node_weight(&a.name),
                self.spec.scale_up.node_weight(&b.name)
            ),
            Strategy::Reachable => format!("mgmt {} vs {}", mgmt(ha), mgmt(hb)),
            Strategy::FastestBoot => format!(
                "boot {} vs {}",
                secs(ha.boot_average_seconds),
                secs(hb.boot_average_seconds)
            ),
            Strategy::Reliable => format!("boots {} vs {}", ok_of(ha), ok_of(hb)),
            Strategy::Standby => format!(
                "S3 {} vs {}",
                if a.s3_capable { "yes" } else { "no" },
                if b.s3_capable { "yes" } else { "no" }
            ),
            Strategy::Largest | Strategy::Smallest => format!("{} vs {}", size(a), size(b)),
            Strategy::Random => format!("drew {:.2} vs {:.2}", ha.lottery, hb.lottery),
            Strategy::ActiveHoursRemaining => format!("hours left {} vs {}", left(ha), left(hb)),
        }
    }
}

fn left(h: &MachineHistory) -> String {
    h.active_seconds_left
        .map_or("unlimited".into(), |s| format!("{}m", s / 60))
}

fn secs(s: Option<f64>) -> String {
    s.map_or("unknown".into(), |s| format!("{s:.0}s"))
}

fn ok_of(h: &MachineHistory) -> String {
    format!("{}/{} ok", h.boots_succeeded, h.boots_succeeded + h.boots_failed)
}

fn mgmt(h: &MachineHistory) -> String {
    match (h.reachable, h.answered, h.rtt_ms) {
        (None, ..) => "not probed".into(),
        (Some(_), Some(a), Some(ms)) => format!("{:.0}% answered, {ms}ms", a * 100.0),
        (Some(_), Some(a), None) => format!("{:.0}% answered", a * 100.0),
        (Some(r), None, _) => if r { "up" } else { "down" }.into(),
    }
}

/// A machine's learned facts, short: `boot 95s, 9/10 ok, bmc 2ms`.
fn learned(m: &Member) -> String {
    let h = &m.history;
    let mut parts = Vec::new();
    parts.push(match h.boot_average_seconds {
        Some(s) => format!("boot {s:.0}s"),
        None => "boot ?".into(),
    });
    let boots = h.boots_succeeded + h.boots_failed;
    if boots > 0 {
        parts.push(format!("{}/{} ok", h.boots_succeeded, boots));
    }
    match (h.reachable, h.rtt_ms) {
        (Some(true), Some(ms)) => parts.push(format!("mgmt {ms}ms")),
        (Some(true), None) => parts.push("mgmt up".into()),
        (Some(false), _) => parts.push("mgmt down".into()),
        (None, _) => {}
    }
    parts.join(", ")
}

/// Computes the scaling plan for one pool.
pub fn plan(s: &PoolSnapshot) -> Plan {
    let mut plan = Plan::default();
    let spec = s.spec;
    let max_online = spec.max_online.map(|m| m as usize).unwrap_or(usize::MAX);

    let online_now = s
        .members
        .iter()
        .filter(|m| matches!(m.state, MemberState::Online | MemberState::Booting))
        .count();
    plan.online = online_now as u32;

    // --- scale up ----------------------------------------------------------
    let grace = Duration::seconds(spec.scale_up.pending_pod_grace_seconds as i64);
    // Pending pods this pool cares about (past the grace period, and targeting
    // the pool where that is required).
    let selected: Vec<&PodView> = s
        .pending
        .iter()
        .filter(|p| p.created.is_none_or(|c| s.now - c >= grace))
        .filter(|p| !spec.scale_up.require_explicit_selection || p.explicitly_selects(&spec.node_selector))
        .collect();
    let mut pending: Vec<&PodView> = selected
        .iter()
        .copied()
        .filter(|p| {
            s.members.iter().any(|m| {
                m.allocatable.fits(&p.requests)
                    && pod_matches_node(p, &m.labels, &m.taints)
                    && (matches!(m.state, MemberState::Booting) || (m.auto && m.state == MemberState::Offline))
            })
        })
        .collect();
    plan.relevant_pending = pending.len() as u32;
    // First-fit decreasing by CPU, then memory.
    pending.sort_by_key(|p| std::cmp::Reverse((p.requests.cpu_millis, p.requests.memory_bytes)));

    // Bins: booting members first (capacity already on its way), then members we open.
    // With `hedge`, a boot taking longer than it should is no longer counted
    // on: the pods it was for open the next machine. Member -> since when.
    let overdue: BTreeMap<String, DateTime<Utc>> = match spec.scale_up.hedge {
        Some(h) => s
            .members
            .iter()
            .filter(|m| m.state == MemberState::Booting)
            .filter_map(|m| {
                let since = m.history.booting_since?;
                let at = since + Duration::seconds(h.overdue_after(m.history.boot_average_seconds));
                (s.now >= at).then(|| (m.name.clone(), at))
            })
            .collect(),
        None => BTreeMap::new(),
    };
    let mut bins: Vec<(&Member, ResourceAmounts)> = s
        .members
        .iter()
        .filter(|m| m.state == MemberState::Booting && !overdue.contains_key(&m.name))
        .map(|m| (m, m.allocatable.minus(&m.requested)))
        .collect();
    let hedging = if overdue.is_empty() {
        String::new()
    } else {
        format!(
            "; hedging: {} overdue",
            overdue.keys().cloned().collect::<Vec<_>>().join(", ")
        )
    };
    let mut offline: Vec<&Member> = s
        .members
        .iter()
        .filter(|m| m.auto && m.state == MemberState::Offline)
        .collect();
    // Each pod takes the first machine in `power_on_order` it fits on.
    let ranking = Ranking::new(spec, s.members.iter());
    offline.sort_by(|a, b| ranking.order(a, b));
    let ranked_up: Vec<&Member> = offline.clone();
    let rank_up = |m: &Member| ranked_up.iter().position(|o| o.name == m.name).unwrap_or(0) + 1;

    let mut opened: Vec<(String, String)> = Vec::new();
    // Why each machine was chosen, and why pods were left waiting.
    let mut up_notes: Vec<(String, String)> = Vec::new();
    let mut unplaced: Vec<(String, String)> = Vec::new();
    let can_open = |opened: &Vec<(String, String)>| {
        if online_now + opened.len() >= max_online {
            Err(format!("maxOnline ({max_online}) reached"))
        } else if opened.len() as u32 >= spec.scale_up.max_nodes_per_step.max(1) {
            Err(format!(
                "maxNodesPerStep ({}) reached this cycle",
                spec.scale_up.max_nodes_per_step.max(1)
            ))
        } else {
            Ok(())
        }
    };
    for pod in &pending {
        let pod_name = format!("{}/{}", pod.namespace, pod.name);
        if !spec.scale_up.enabled {
            unplaced.push((pod_name, "scale-up disabled".into()));
            continue;
        }
        if let Some((_, free)) = bins.iter_mut().find(|(m, free)| pod_fits(pod, m, free)) {
            *free = free.minus(&pod.requests);
            continue;
        }
        if let Err(why) = can_open(&opened) {
            unplaced.push((pod_name, why));
            continue;
        }
        if let Some(pos) = offline.iter().position(|m| pod_fits(pod, m, &m.allocatable)) {
            let m = offline.remove(pos);
            // The machine it would have had otherwise: the next one it fits on.
            let decided = match offline[pos..].iter().find(|o| pod_fits(pod, o, &o.allocatable)) {
                Some(next) => format!("over {} by {}", next.name, ranking.rule(m, next, false)),
                None => "the only machine off that fits".into(),
            };
            let reason = format!(
                "unschedulable pod {pod_name}; #{} of {} to power on, {decided}{hedging}",
                rank_up(m),
                ranked_up.len()
            );
            up_notes.push((m.name.clone(), reason.clone()));
            opened.push((m.name.clone(), reason));
            bins.push((m, m.allocatable.minus(&pod.requests)));
        } else {
            unplaced.push((pod_name, "fits no machine that is off and Auto".into()));
        }
    }
    // PowerRequests: their machines are woken whatever the pending pods,
    // maxOnline or maxNodesPerStep say (and even with scale-up disabled).
    while let Some(pos) = offline.iter().position(|m| m.power_requests.is_some()) {
        let m = offline.remove(pos);
        let reason = m.power_requests.clone().unwrap_or_default();
        up_notes.push((m.name.clone(), reason.clone()));
        opened.push((m.name.clone(), reason));
    }
    // minOnline is enforced even when scale-up is disabled.
    while online_now + opened.len() < spec.min_online as usize && online_now + opened.len() < max_online {
        let Some(m) = offline.first().copied() else { break };
        offline.remove(0);
        let decided = match offline.first() {
            Some(next) => format!("over {} by {}", next.name, ranking.rule(m, next, false)),
            None => "the only machine off".into(),
        };
        let reason = format!(
            "pool below minOnline ({}); #{} of {} to power on, {decided}",
            spec.min_online,
            rank_up(m),
            ranked_up.len()
        );
        up_notes.push((m.name.clone(), reason.clone()));
        opened.push((m.name.clone(), reason));
    }
    plan.power_on = opened;

    // --- scale down --------------------------------------------------------
    let mut candidates: Vec<&Member> = s
        .members
        .iter()
        .filter(|m| m.auto && m.state == MemberState::Online)
        .collect();
    // The exact reverse of the power-on order.
    candidates.sort_by(|a, b| ranking.order(b, a));
    let threshold = spec.scale_down.utilization_threshold_percent as f64;
    let backoff = Duration::seconds(spec.scale_down.drain_failure_backoff_seconds as i64);
    let login_recheck = Duration::seconds(LOGIN_RECHECK_SECS);
    let pool_unneeded = spec.scale_down.unneeded_seconds;
    // A member's own `lifecycle.unneededSeconds`, else the pool's.
    let unneeded_for = |m: &Member| Duration::seconds(m.unneeded_seconds.unwrap_or(pool_unneeded) as i64);
    let util = |m: &Member| m.allocatable.utilization_percent(&m.counted);
    // Per online candidate: why it is not (yet) eligible to power off, in both renderings.
    let mut down_notes: BTreeMap<String, Note> = BTreeMap::new();
    let mut waiting_notes: Vec<String> = Vec::new();
    for m in &candidates {
        let waiting = waiting_for_registration(m, &selected);
        let note = if let Some(note) = waiting {
            waiting_notes.push(format!("{} {note}", m.name));
            Note::same(note)
        } else if let Some(r) = &m.power_requests {
            Note::same(format!("busy: {r}"))
        } else if !m.blocking_pods.is_empty() {
            Note::same(format!("busy: blocked by {}", m.blocking_pods.join(", ")))
        } else if let Some((t, who)) = m.logged_in.as_ref().filter(|(t, _)| s.now - *t < login_recheck) {
            Note {
                stable: format!(
                    "busy: logged in: {}; recheck after {}",
                    who.join(", "),
                    hhmmss(*t + login_recheck)
                ),
                live: format!(
                    "busy: logged in: {}; recheck in {}s",
                    who.join(", "),
                    (*t + login_recheck - s.now).num_seconds()
                ),
            }
        } else if let Some(t) = m.drain_failed_at.filter(|t| s.now - *t < backoff) {
            Note {
                stable: format!("drain failed; retry after {}", hhmmss(t + backoff)),
                live: format!("drain failed; retry in {}s", (t + backoff - s.now).num_seconds()),
            }
        } else if util(m) >= threshold {
            Note::same(format!("busy: utilization {:.0}% >= {:.0}%", util(m), threshold))
        } else {
            let since = s.unneeded_since.get(&m.name).copied().unwrap_or(s.now);
            plan.unneeded_since.insert(m.name.clone(), since);
            if let Some(why) = &m.outside_active_hours {
                Note::same(format!("unneeded since {}; {why}", hhmmss(since)))
            } else if s.now - since >= unneeded_for(m) {
                Note::same(format!("unneeded since {}", hhmmss(since)))
            } else {
                Note {
                    stable: format!(
                        "unneeded since {}, eligible at {}",
                        hhmmss(since),
                        hhmmss(since + unneeded_for(m))
                    ),
                    live: format!(
                        "unneeded for {} of {}s",
                        (s.now - since).num_seconds(),
                        unneeded_for(m).num_seconds()
                    ),
                }
            }
        };
        down_notes.insert(m.name.clone(), note);
    }

    // Unneeded long enough to go: its own or the pool's unneededSeconds, or at
    // once outside its active hours.
    let expired_for =
        |m: &Member, since: DateTime<Utc>| m.outside_active_hours.is_some() || s.now - since >= unneeded_for(m);
    let over_max = online_now.saturating_sub(max_online);
    let leaving = s.members.iter().filter(|m| m.state == MemberState::Leaving).count();
    // `holdAfterPowerOnSeconds`: pool-wide, after this pool last powered a
    // machine on, or a machine was powered on by hand under `Adopt`.
    let last_power_on = s
        .last_scale_up
        .into_iter()
        .chain(s.members.iter().filter_map(|m| m.adopted_at))
        .max();
    let hold_until = last_power_on
        .map(|t| t + Duration::seconds(spec.scale_down.hold_after_power_on() as i64))
        .filter(|until| s.now < *until);
    let booting = s.members.iter().any(|m| m.state == MemberState::Booting);

    let blocked_reason: Option<Note> = if !plan.power_on.is_empty() {
        Some(Note::same("scaling up".into()))
    } else if over_max > 0 {
        None
    } else if !spec.scale_down.enabled {
        Some(Note::same("scale down disabled".into()))
    } else if plan.relevant_pending > 0 {
        Some(Note::same(format!("{} pending pod(s)", plan.relevant_pending)))
    } else if booting {
        Some(Note::same("nodes booting".into()))
    } else {
        hold_until.map(|until| Note {
            stable: format!("holding after power-on until {}", hhmmss(until)),
            live: format!("holding after power-on, {}s left", (until - s.now).num_seconds()),
        })
    };

    let step = spec.scale_down.max_nodes_per_step.max(1) as usize;
    let mut off_notes: BTreeMap<String, String> = BTreeMap::new();
    // `manualPowerOnPolicy: PowerOff` past its grace: powered off busy or not
    // (pods are still drained; safe-to-evict=false pods still block).
    let released = |m: &Member| {
        m.release_after.is_some_and(|t| s.now >= t) && m.blocking_pods.is_empty() && m.power_requests.is_none()
    };
    let eligible: Vec<&Member> = candidates
        .iter()
        .copied()
        .filter(|m| {
            let expired = plan.unneeded_since.get(&m.name).is_some_and(|t| expired_for(m, *t));
            (over_max > 0 && m.blocking_pods.is_empty() && m.power_requests.is_none()) || expired || released(m)
        })
        .collect();
    if blocked_reason.is_none() && leaving >= step && over_max == 0 {
        for m in &eligible {
            off_notes.insert(
                m.name.clone(),
                format!("kept: maxNodesPerStep ({step}) already powering off"),
            );
        }
    }
    if blocked_reason.is_none() && (leaving < step || over_max > 0) {
        let mut removed: BTreeSet<String> = BTreeSet::new();
        // Where evicted pods could go: online pool members, plus schedulable nodes
        // outside any scaling pool (e.g. an always-on base) that never power off.
        let mut free: BTreeMap<String, ResourceAmounts> = s
            .members
            .iter()
            .filter(|m| matches!(m.state, MemberState::Online | MemberState::Manual))
            .map(|m| (m.name.clone(), m.allocatable.minus(&m.requested)))
            .collect();
        for e in &s.external {
            free.insert(external_key(&e.name), e.free);
        }
        let budget = if over_max > 0 {
            over_max.max(step.saturating_sub(leaving))
        } else {
            step - leaving
        };

        for (i, m) in eligible.iter().enumerate() {
            if let Some(holds) = s.kept_by_others.get(&m.name).filter(|h| !h.is_empty()) {
                let by = holds
                    .iter()
                    .map(|(pool, why)| format!("needed by pool {pool}: {why}"))
                    .collect::<Vec<_>>()
                    .join("; ");
                off_notes.insert(m.name.clone(), format!("kept: {by}"));
                continue;
            }
            if removed.len() >= budget {
                off_notes.insert(m.name.clone(), format!("kept: maxNodesPerStep ({step}) reached"));
                continue;
            }
            if online_now - removed.len() <= spec.min_online as usize {
                off_notes.insert(m.name.clone(), format!("kept: minOnline ({})", spec.min_online));
                continue;
            }
            // Simulate moving this node's pods to the remaining online nodes.
            let mut trial = free.clone();
            trial.remove(&m.name);
            let mut targets: Vec<(String, &BTreeMap<String, String>, &[Taint])> = s
                .members
                .iter()
                .filter(|o| {
                    matches!(o.state, MemberState::Online | MemberState::Manual)
                        && o.name != m.name
                        && !removed.contains(&o.name)
                })
                .map(|o| (o.name.clone(), &o.labels, o.taints.as_slice()))
                .collect();
            targets.extend(
                s.external
                    .iter()
                    .map(|e| (external_key(&e.name), &e.labels, e.taints.as_slice())),
            );
            let mut pods: Vec<&PodView> = m.movable_pods.iter().collect();
            pods.sort_by_key(|p| std::cmp::Reverse((p.requests.cpu_millis, p.requests.memory_bytes)));
            let all_fit = pods.iter().all(|p| {
                let slot = targets
                    .iter()
                    .find(|(key, labels, taints)| trial[key].fits(&p.requests) && pod_matches_node(p, labels, taints))
                    .map(|(key, _, _)| key.clone());
                match slot {
                    Some(key) => {
                        let f = trial.get_mut(&key).unwrap();
                        *f = f.minus(&p.requests);
                        true
                    }
                    None => false,
                }
            });
            if all_fit {
                free = trial;
                removed.insert(m.name.clone());
                let expired = plan.unneeded_since.get(&m.name).is_some_and(|t| expired_for(m, *t));
                let why = if over_max > 0 {
                    format!("pool above maxOnline ({max_online})")
                } else if released(m) && !expired {
                    "manually powered on; policy PowerOff, grace over".to_string()
                } else if let Some(hours) = &m.outside_active_hours {
                    format!("utilization {:.0}% below {:.0}%; {hours}", util(m), threshold)
                } else {
                    format!(
                        "utilization {:.0}% below {:.0}% for {}s",
                        util(m),
                        threshold,
                        unneeded_for(m).num_seconds()
                    )
                };
                // The machine that would have gone otherwise: the next eligible one.
                let decided = match eligible[i + 1..].iter().find(|o| !removed.contains(&o.name)) {
                    Some(next) => format!("before {} by {}", next.name, ranking.rule(m, next, true)),
                    None => "the only machine eligible".into(),
                };
                let rank = candidates.iter().position(|o| o.name == m.name).unwrap_or(0) + 1;
                let reason = format!("{why}; #{rank} of {} to power off, {decided}", candidates.len());
                off_notes.insert(m.name.clone(), reason.clone());
                plan.power_off.push((m.name.clone(), reason));
            } else {
                off_notes.insert(m.name.clone(), "kept: its pods would not fit elsewhere".into());
            }
        }
    }

    plan.message = match (plan.power_on.len(), plan.power_off.len(), &blocked_reason) {
        (0, 0, Some(r)) if r.stable != "scaling up" && !plan.unneeded_since.is_empty() => {
            // The message stays stable while the hold lasts (no seconds in it).
            let r = r.stable.split(" until ").next().unwrap_or(&r.stable);
            format!("steady ({} unneeded; scale down held: {r})", plan.unneeded_since.len())
        }
        (0, 0, _) => "steady".to_string(),
        (on, off, _) => format!("powering on {on}, powering off {off}"),
    };

    // --- agreement with overlapping pools -------------------------------------
    // What this pool says to other pools selecting the same machines: which
    // members it would let go now, and why it keeps the others.
    for m in &candidates {
        let expired = plan.unneeded_since.get(&m.name).is_some_and(|t| expired_for(m, *t));
        let held = if !expired && !released(m) {
            Some(down_notes[&m.name].stable.clone())
        } else if let Some(r) = &blocked_reason {
            Some(r.stable.clone())
        } else if online_now <= spec.min_online as usize {
            Some(format!("minOnline ({})", spec.min_online))
        } else {
            None
        };
        match held {
            Some(why) => {
                plan.needed.insert(m.name.clone(), why);
            }
            None => plan.releasable.push(m.name.clone()),
        }
    }
    for m in s.members.iter().filter(|m| m.state == MemberState::Booting) {
        plan.needed.insert(m.name.clone(), "powering on".into());
    }
    plan.releasable.sort();

    // --- explanation ---------------------------------------------------------
    let compact = |list: &[&Member]| {
        list.iter()
            .map(|m| format!("{}({})", m.name, ranking.tags(m)))
            .collect::<Vec<_>>()
            .join(" > ")
    };
    let render = |live: bool| -> Vec<String> {
        let mut lines = Vec::new();
        for m in &ranked_up {
            let mut line = format!("{}: #{} to power on ({})", m.name, rank_up(m), ranking.traits(m));
            match up_notes.iter().find(|(n, _)| *n == m.name) {
                Some(_) => line.push_str("; chosen"),
                None if !pending.iter().any(|p| pod_fits(p, m, &m.allocatable)) && !pending.is_empty() => {
                    line.push_str("; no pending pod fits")
                }
                None => {}
            }
            if let Some(n) = &m.note {
                line.push_str(&format!("; {n}"));
            }
            lines.push(line);
        }
        for (i, m) in candidates.iter().enumerate() {
            let note = &down_notes[&m.name];
            let mut line = format!(
                "{}: #{} to power off ({}{}); {}",
                m.name,
                i + 1,
                ranking.traits(m),
                m.woken_by
                    .as_deref()
                    .map(|p| format!(", woken by pool {p}"))
                    .unwrap_or_default(),
                if live { &note.live } else { &note.stable }
            );
            if let Some(off) = off_notes.get(&m.name) {
                if plan.power_off.iter().any(|(n, _)| n == &m.name) {
                    line.push_str("; chosen");
                } else {
                    line.push_str(&format!("; {off}"));
                }
            }
            if let Some(n) = &m.note {
                line.push_str(&format!("; {n}"));
            }
            lines.push(line);
        }
        let mut others: Vec<&Member> = s
            .members
            .iter()
            .filter(|m| !ranked_up.iter().chain(candidates.iter()).any(|o| o.name == m.name))
            .collect();
        others.sort_by(|a, b| a.name.cmp(&b.name));
        for m in others {
            let why = match m.state {
                _ if !m.auto => "excluded: powerPolicy is not Auto".to_string(),
                _ if m.note.is_some() => m.note.clone().unwrap_or_default(),
                MemberState::Manual => "powered on by hand; not managing".to_string(),
                MemberState::Booting => {
                    let base = match &m.woken_by {
                        Some(p) => format!("powering on (woken by pool {p})"),
                        None => "powering on".to_string(),
                    };
                    match overdue.get(&m.name) {
                        Some(at) => format!("{base}; boot overdue since {}, not counted as capacity", hhmmss(*at)),
                        None => base,
                    }
                }
                MemberState::Leaving => "powering off".to_string(),
                MemberState::Unavailable => "excluded: state unknown or in error".to_string(),
                // Unreachable: Auto Online/Offline members are ranked above.
                MemberState::Online | MemberState::Offline => "not ranked".to_string(),
            };
            lines.push(format!("{}: {why}", m.name));
        }
        lines
    };
    plan.details = render(false);
    plan.live_details = render(true);

    let up_candidates = compact(&ranked_up);
    let down_candidates = compact(&candidates);
    for (name, reason) in &plan.power_on {
        plan.decisions.push(PlannedDecision {
            action: DecisionAction::PowerOn,
            node: Some(name.clone()),
            reason: reason.clone(),
            live_reason: reason.clone(),
            candidates: up_candidates.clone(),
        });
    }
    for (name, reason) in &plan.power_off {
        plan.decisions.push(PlannedDecision {
            action: DecisionAction::PowerOff,
            node: Some(name.clone()),
            reason: reason.clone(),
            live_reason: reason.clone(),
            candidates: down_candidates.clone(),
        });
    }
    if plan.decisions.is_empty() {
        let unneeded: Vec<&str> = candidates
            .iter()
            .filter(|m| plan.unneeded_since.contains_key(&m.name))
            .map(|m| m.name.as_str())
            .collect();
        let why = |live: bool| -> String {
            if let Some((pod, why)) = unplaced.first() {
                let more = match unplaced.len() {
                    1 => String::new(),
                    n => format!(" (and {} more)", n - 1),
                };
                format!("pod {pod}{more} waits: {why}")
            } else if let (Some(r), false) = (&blocked_reason, unneeded.is_empty()) {
                format!(
                    "scale-down held ({}): {}",
                    if live { &r.live } else { &r.stable },
                    unneeded
                        .iter()
                        .map(|n| format!(
                            "{n} {}",
                            if live {
                                &down_notes[*n].live
                            } else {
                                &down_notes[*n].stable
                            }
                        ))
                        .collect::<Vec<_>>()
                        .join("; ")
                )
            } else if !unneeded.is_empty() {
                unneeded
                    .iter()
                    .map(|n| {
                        let note = if live {
                            &down_notes[*n].live
                        } else {
                            &down_notes[*n].stable
                        };
                        match off_notes.get(*n) {
                            Some(kept) => format!("{n} {note}; {kept}"),
                            None => format!("{n} {note}"),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            } else if !waiting_notes.is_empty() {
                waiting_notes.join("; ")
            } else if plan.relevant_pending > 0 {
                format!(
                    "{} pending pod(s) fit capacity already powering on",
                    plan.relevant_pending
                )
            } else {
                format!("{online_now} online, nothing pending, nothing unneeded")
            }
        };
        // Members in a special state explain it in every decision about doing
        // nothing, so the log records when it starts and ends.
        let notes: Vec<String> = s
            .members
            .iter()
            .filter_map(|m| m.note.as_ref().map(|n| format!("{} {n}", m.name)))
            .collect();
        let with_notes = |reason: String| {
            if notes.is_empty() {
                reason
            } else {
                format!("{reason}; {}", notes.join("; "))
            }
        };
        plan.decisions.push(PlannedDecision {
            action: DecisionAction::NoAction,
            node: None,
            reason: with_notes(why(false)),
            live_reason: with_notes(why(true)),
            candidates: if unneeded.is_empty() && unplaced.is_empty() {
                String::new()
            } else if unplaced.is_empty() {
                down_candidates
            } else {
                up_candidates
            },
        });
    }
    plan
}

/// Why `m` is needed although it may look idle: pending pods of this pool
/// that would fit it once the extended resources its device plugin is still
/// registering (see `Member::registering`) appear. `None` when there are none.
fn waiting_for_registration(m: &Member, selected: &[&PodView]) -> Option<String> {
    if m.registering.is_empty() {
        return None;
    }
    let free = m.allocatable.minus(&m.requested);
    let mut resources: BTreeSet<&str> = BTreeSet::new();
    let mut count = 0;
    for p in selected {
        let waits_for: Vec<&str> = p
            .extended_requests
            .iter()
            .filter(|(k, v)| m.registering.get(*k).is_some_and(|have| have >= *v))
            .map(|(k, _)| k.as_str())
            .collect();
        if waits_for.is_empty() || !free.fits(&p.requests) || !pod_matches_node(p, &m.labels, &m.taints) {
            continue;
        }
        count += 1;
        resources.extend(waits_for);
    }
    (count > 0).then(|| {
        format!(
            "needed: {count} pending pod(s) waiting for its {} to register",
            resources.into_iter().collect::<Vec<_>>().join(", ")
        )
    })
}

/// Text in two renderings: `stable` for status and events (absolute times, so
/// it only changes when the situation does), `live` for logs (seconds).
#[derive(Clone, Debug)]
struct Note {
    stable: String,
    live: String,
}

impl Note {
    fn same(s: String) -> Self {
        Note {
            stable: s.clone(),
            live: s,
        }
    }
}

fn hhmmss(t: DateTime<Utc>) -> String {
    t.format("%H:%M:%SZ").to_string()
}

fn size(m: &Member) -> String {
    let cpu = m.allocatable.cpu_millis as f64 / 1000.0;
    let gib = m.allocatable.memory_bytes as f64 / (1u64 << 30) as f64;
    format!("{cpu}c/{gib:.0}Gi")
}

/// The rule of `power_on_order` that puts `chosen` ahead of `other`: for
/// scale-up (`down == false`) `chosen` ranks before `other`; for scale-down it
/// ranks after it.
pub fn deciding_rule(spec: &NodeScalingPoolSpec, chosen: &Member, other: &Member, down: bool) -> String {
    Ranking::new(spec, [chosen, other]).rule(chosen, other, down)
}

/// `deciding_rule` without strategies.
fn ordered_rule(spec: &NodeScalingPoolSpec, chosen: &Member, other: &Member, down: bool) -> String {
    let (wc, wo) = (
        spec.scale_up.node_weight(&chosen.name),
        spec.scale_up.node_weight(&other.name),
    );
    let size_key = |m: &Member| (m.allocatable.cpu_millis, m.allocatable.memory_bytes);
    if wc != wo {
        format!("weight ({wc} vs {wo})")
    } else if spec.scale_up.prefer_s3_capable && chosen.s3_capable != other.s3_capable {
        if down {
            "S3 capability (not S3-capable)"
        } else {
            "S3 capability (S3-capable)"
        }
        .to_string()
    } else if size_key(chosen) != size_key(other) {
        format!("size ({} vs {})", size(chosen), size(other))
    } else {
        format!("name ({} {} {})", chosen.name, if down { ">" } else { "<" }, other.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{PreferredNode, ScaleDownSpec, ScaleUpSpec};

    const GI: i64 = 1 << 30;

    fn spec() -> NodeScalingPoolSpec {
        NodeScalingPoolSpec {
            node_selector: Default::default(),
            min_online: 1,
            max_online: Some(3),
            scale_up: ScaleUpSpec {
                enabled: true,
                pending_pod_grace_seconds: 30,
                max_nodes_per_step: 3,
                require_explicit_selection: false,
                prefer_s3_capable: false,
                preferred_nodes: vec![],
                boot_timeout_seconds: None,
                boot_failure_backoff_seconds: 1800,
                power_on_timeout_seconds: None,
                power_on_failure_policy: Default::default(),
                strategies: vec![],
                hedge: None,
                learned_power_on_timeout: None,
                boot_failure_backoff_max_seconds: None,
            },
            scale_down: ScaleDownSpec {
                enabled: true,
                utilization_threshold_percent: 50,
                unneeded_seconds: 600,
                hold_after_power_on_seconds: Some(600),
                delay_after_scale_up_seconds: None,
                drain_failure_backoff_seconds: 1800,
                max_nodes_per_step: 1,
                ignore_daemon_set_utilization: false,
                ignore_non_selecting_pod_utilization: false,
            },
            manual_power_on_policy: None,
            manual_power_on_grace_seconds: 600,
            manual_power_off_cooldown_seconds: None,
        }
    }

    fn res(cpu: i64, mem_gi: i64) -> ResourceAmounts {
        ResourceAmounts {
            cpu_millis: cpu,
            memory_bytes: mem_gi * GI,
            pods: 1,
        }
    }

    fn member(name: &str, state: MemberState, used_cpu: i64) -> Member {
        Member {
            name: name.into(),
            state,
            auto: true,
            labels: BTreeMap::from([("kubernetes.io/hostname".into(), name.into())]),
            taints: vec![],
            allocatable: ResourceAmounts {
                cpu_millis: 4000,
                memory_bytes: 16 * GI,
                pods: 110,
            },
            requested: ResourceAmounts {
                cpu_millis: used_cpu,
                memory_bytes: 0,
                pods: 0,
            },
            counted: ResourceAmounts {
                cpu_millis: used_cpu,
                memory_bytes: 0,
                pods: 0,
            },
            movable_pods: vec![],
            blocking_pods: vec![],
            drain_failed_at: None,
            logged_in: None,
            s3_capable: false,
            registering: BTreeMap::new(),
            woken_by: None,
            note: None,
            adopted_at: None,
            release_after: None,
            outside_active_hours: None,
            unneeded_seconds: None,
            power_requests: None,
            history: Default::default(),
        }
    }

    fn pod(name: &str, cpu: i64, age_s: i64, now: DateTime<Utc>) -> PodView {
        PodView {
            namespace: "default".into(),
            name: name.into(),
            requests: res(cpu, 1),
            created: Some(now - Duration::seconds(age_s)),
            ..Default::default()
        }
    }

    fn snapshot(
        spec: &NodeScalingPoolSpec,
        members: Vec<Member>,
        pending: Vec<PodView>,
        now: DateTime<Utc>,
    ) -> PoolSnapshot<'_> {
        PoolSnapshot {
            spec,
            members,
            pending,
            now,
            last_scale_up: None,
            unneeded_since: BTreeMap::new(),
            external: vec![],
            kept_by_others: BTreeMap::new(),
        }
    }

    #[test]
    fn powers_on_for_unschedulable_pods() {
        let now = Utc::now();
        let spec = spec();
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Offline, 0),
                member("c", MemberState::Offline, 0),
            ],
            vec![pod("p1", 1000, 60, now), pod("p2", 1000, 60, now)],
            now,
        );
        let p = plan(&s);
        // Both pods fit on one machine.
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.relevant_pending, 2);
        assert!(p.power_off.is_empty());
    }

    #[test]
    fn prefers_s3_capable_machines_only_when_asked() {
        let now = Utc::now();
        let mut big = member("big", MemberState::Offline, 0);
        big.allocatable.cpu_millis = 16000;
        let mut sleeper = member("sleeper", MemberState::Offline, 0);
        sleeper.s3_capable = true;
        let members = vec![big, sleeper];
        let pods = vec![pod("p1", 1000, 60, now)];

        let mut spec = spec();
        let default = plan(&snapshot(&spec, members.clone(), pods.clone(), now));
        assert_eq!(default.power_on[0].0, "big", "size decides by default");

        spec.scale_up.prefer_s3_capable = true;
        let preferred = plan(&snapshot(&spec, members.clone(), pods.clone(), now));
        assert_eq!(preferred.power_on[0].0, "sleeper");

        // A pod only the machine without S3 can hold still gets it.
        let huge = vec![pod("p1", 12000, 60, now)];
        let fallback = plan(&snapshot(&spec, members, huge, now));
        assert_eq!(fallback.power_on[0].0, "big");
    }

    fn prefer(spec: &mut NodeScalingPoolSpec, weights: &[(&str, u32)]) {
        spec.scale_up.preferred_nodes = weights
            .iter()
            .map(|(name, weight)| PreferredNode {
                name: name.to_string(),
                weight: *weight,
            })
            .collect();
    }

    #[test]
    fn preferred_nodes_order_scale_up_before_s3_and_size() {
        let now = Utc::now();
        let mut big = member("big", MemberState::Offline, 0);
        big.allocatable.cpu_millis = 16000;
        let mut sleeper = member("sleeper", MemberState::Offline, 0);
        sleeper.s3_capable = true;
        let plain = member("plain", MemberState::Offline, 0);
        let members = vec![big, sleeper, plain];
        let pods = vec![pod("p1", 1000, 60, now)];
        let first = |spec: &NodeScalingPoolSpec, pods: Vec<PodView>| {
            plan(&snapshot(spec, members.clone(), pods, now)).power_on[0].0.clone()
        };

        let mut spec = spec();
        spec.scale_up.prefer_s3_capable = true;
        assert_eq!(first(&spec, pods.clone()), "sleeper", "no weights: S3, then size");

        // The weight comes before S3 capability and size.
        prefer(&mut spec, &[("plain", 10)]);
        assert_eq!(first(&spec, pods.clone()), "plain");
        prefer(&mut spec, &[("plain", 10), ("big", 50)]);
        assert_eq!(first(&spec, pods.clone()), "big");

        // Equal weights fall back to S3 capability, then size.
        prefer(&mut spec, &[("plain", 50), ("big", 50), ("sleeper", 50)]);
        assert_eq!(first(&spec, pods.clone()), "sleeper");
        spec.scale_up.prefer_s3_capable = false;
        assert_eq!(first(&spec, pods.clone()), "big");

        // A preferred machine the pod does not fit on is skipped, not forced.
        prefer(&mut spec, &[("plain", 100), ("sleeper", 90)]);
        assert_eq!(first(&spec, vec![pod("huge", 12000, 60, now)]), "big");
    }

    #[test]
    fn preferred_nodes_order_min_online() {
        let now = Utc::now();
        let mut spec = spec();
        spec.min_online = 1;
        prefer(&mut spec, &[("b", 100), ("a", 10)]);
        let members = vec![
            member("a", MemberState::Offline, 0),
            member("b", MemberState::Offline, 0),
            member("c", MemberState::Offline, 0),
        ];
        let p = plan(&snapshot(&spec, members, vec![], now));
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.power_on[0].0, "b");
    }

    /// Every member unneeded for long enough; returns the one powered off first.
    fn first_off(spec: &NodeScalingPoolSpec, members: Vec<Member>, now: DateTime<Utc>) -> String {
        let mut s = snapshot(spec, members, vec![], now);
        s.unneeded_since = s
            .members
            .iter()
            .map(|m| (m.name.clone(), now - Duration::seconds(700)))
            .collect();
        let p = plan(&s);
        assert_eq!(p.power_off.len(), 1, "one per step");
        p.power_off[0].0.clone()
    }

    #[test]
    fn preferred_nodes_power_off_lightest_first() {
        let now = Utc::now();
        let mut spec = spec();
        spec.min_online = 0;
        let members = vec![
            member("a", MemberState::Online, 300),
            member("b", MemberState::Online, 200),
            member("c", MemberState::Online, 100),
        ];
        // All alike: the last name goes first.
        assert_eq!(first_off(&spec, members.clone(), now), "c");
        // Unlisted machines weigh 0 and go before any listed one.
        prefer(&mut spec, &[("c", 100), ("b", 50)]);
        assert_eq!(first_off(&spec, members.clone(), now), "a");
        prefer(&mut spec, &[("c", 100), ("a", 10), ("b", 20)]);
        assert_eq!(first_off(&spec, members, now), "a");
    }

    #[test]
    fn ties_break_by_s3_then_size_then_name_and_scale_down_is_the_reverse() {
        let now = Utc::now();
        let mk = |name: &str, state, cpu: i64, s3: bool| {
            let mut m = member(name, state, 0);
            m.allocatable.cpu_millis = cpu;
            m.s3_capable = s3;
            m
        };
        let mut spec = spec();
        spec.max_online = None;
        spec.min_online = 0;
        spec.scale_up.prefer_s3_capable = true;
        prefer(&mut spec, &[("w-small", 50), ("w-big", 50)]);
        let fleet = |state| {
            vec![
                mk("z-s3-big", state, 8000, true),
                mk("plain-big", state, 8000, false),
                mk("a-s3-big", state, 8000, true),
                mk("w-small", state, 2000, false),
                mk("s3-small", state, 2000, true),
                mk("w-big", state, 8000, false),
            ]
        };
        let expected_up = ["w-big", "w-small", "a-s3-big", "z-s3-big", "s3-small", "plain-big"];

        let mut sorted = fleet(MemberState::Offline);
        sorted.sort_by(|a, b| power_on_order(&spec, a, b));
        let names: Vec<&str> = sorted.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, expected_up);

        // The plan follows it: minOnline powers machines on in this order...
        spec.min_online = 6;
        spec.scale_up.max_nodes_per_step = 6;
        let p = plan(&snapshot(&spec, fleet(MemberState::Offline), vec![], now));
        let on: Vec<&str> = p.power_on.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(on, expected_up);
        // ...and the input order does not matter.
        let mut reversed = fleet(MemberState::Offline);
        reversed.reverse();
        let p = plan(&snapshot(&spec, reversed, vec![], now));
        let on: Vec<&str> = p.power_on.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(on, expected_up);

        // Scale-down takes them in the exact reverse order.
        spec.min_online = 0;
        spec.scale_down.max_nodes_per_step = 6;
        let mut s = snapshot(&spec, fleet(MemberState::Online), vec![], now);
        s.unneeded_since = s
            .members
            .iter()
            .map(|m| (m.name.clone(), now - Duration::seconds(700)))
            .collect();
        let p = plan(&s);
        let off: Vec<&str> = p.power_off.iter().map(|(n, _)| n.as_str()).collect();
        let mut expected_down = expected_up.to_vec();
        expected_down.reverse();
        assert_eq!(off, expected_down);

        // Without preferS3Capable, S3 capability plays no part.
        spec.scale_up.prefer_s3_capable = false;
        let mut sorted = fleet(MemberState::Offline);
        sorted.sort_by(|a, b| power_on_order(&spec, a, b));
        let names: Vec<&str> = sorted.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(
            names,
            ["w-big", "w-small", "a-s3-big", "plain-big", "z-s3-big", "s3-small"]
        );
    }

    #[test]
    fn deprecated_delay_name_still_holds_scale_down() {
        let now = Utc::now();
        let mut spec = spec();
        spec.scale_down.hold_after_power_on_seconds = None;
        spec.scale_down.delay_after_scale_up_seconds = Some(30);
        let s = |spec| PoolSnapshot {
            last_scale_up: Some(now - Duration::seconds(60)),
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(
                spec,
                vec![member("a", MemberState::Online, 0), member("b", MemberState::Online, 0)],
                vec![],
                now,
            )
        };
        assert_eq!(plan(&s(&spec)).power_off.len(), 1, "a 30 s hold has passed after 60 s");
        // The new name wins over the old one.
        let mut spec = spec.clone();
        spec.scale_down.hold_after_power_on_seconds = Some(120);
        let p = plan(&s(&spec));
        assert!(p.power_off.is_empty());
        assert!(p.message.contains("holding after power-on"), "{}", p.message);
    }

    #[test]
    fn scale_up_decision_explains_rank_and_rule() {
        let now = Utc::now();
        let mut spec = spec();
        spec.scale_up.prefer_s3_capable = true;
        prefer(&mut spec, &[("gpu-node-1", 100), ("gpu-node-2", 10)]);
        let mut first = member("gpu-node-1", MemberState::Offline, 0);
        first.s3_capable = true;
        let mut fixed = member("fixed", MemberState::Offline, 0);
        fixed.auto = false;
        let members = vec![
            member("gpu-node-2", MemberState::Offline, 0),
            first,
            fixed,
            member("a", MemberState::Online, 3900),
        ];
        let p = plan(&snapshot(&spec, members, vec![pod("p1", 1000, 60, now)], now));
        assert_eq!(p.decisions.len(), 1);
        let d = &p.decisions[0];
        assert_eq!(d.action, DecisionAction::PowerOn);
        assert_eq!(d.node.as_deref(), Some("gpu-node-1"));
        assert_eq!(
            d.reason,
            "unschedulable pod default/p1; #1 of 2 to power on, over gpu-node-2 by weight (100 vs 10)"
        );
        assert_eq!(d.candidates, "gpu-node-1(w100 s3 4c/16Gi) > gpu-node-2(w10 4c/16Gi)");
        assert!(
            p.details
                .contains(&"gpu-node-1: #1 to power on (weight 100, S3 yes, 4c/16Gi); chosen".to_string())
        );
        assert!(
            p.details
                .contains(&"gpu-node-2: #2 to power on (weight 10, S3 no, 4c/16Gi)".to_string())
        );
        assert!(
            p.details
                .contains(&"fixed: excluded: powerPolicy is not Auto".to_string())
        );

        // Equal weights: S3 capability decides, then size, then name.
        prefer(&mut spec, &[]);
        let p = plan(&snapshot(
            &spec,
            vec![
                member("y", MemberState::Offline, 0),
                member("x", MemberState::Offline, 0),
            ],
            vec![pod("p1", 1000, 60, now)],
            now,
        ));
        assert_eq!(
            p.decisions[0].reason,
            "unschedulable pod default/p1; #1 of 2 to power on, over y by name (x < y)"
        );
    }

    #[test]
    fn scale_down_decision_explains_rank_and_rule() {
        let now = Utc::now();
        let mut spec = spec();
        spec.min_online = 0;
        prefer(&mut spec, &[("gpu-node-1", 100), ("gpu-node-2", 10)]);
        let mut s = snapshot(
            &spec,
            vec![
                member("gpu-node-1", MemberState::Online, 100),
                member("gpu-node-2", MemberState::Online, 100),
            ],
            vec![],
            now,
        );
        s.unneeded_since = BTreeMap::from([
            ("gpu-node-1".to_string(), now - Duration::seconds(700)),
            ("gpu-node-2".to_string(), now - Duration::seconds(700)),
        ]);
        let p = plan(&s);
        let d = &p.decisions[0];
        assert_eq!(d.action, DecisionAction::PowerOff);
        assert_eq!(d.node.as_deref(), Some("gpu-node-2"));
        assert_eq!(
            d.reason,
            "utilization 2% below 50% for 600s; #1 of 2 to power off, before gpu-node-1 by weight (10 vs 100)"
        );
        assert_eq!(d.candidates, "gpu-node-2(w10 4c/16Gi) > gpu-node-1(w100 4c/16Gi)");
        assert!(
            p.details
                .iter()
                .any(|l| l.starts_with("gpu-node-1: #2 to power off")
                    && l.ends_with("kept: maxNodesPerStep (1) reached")),
            "{:?}",
            p.details
        );
    }

    #[test]
    fn no_action_explains_the_hold_stably_and_live() {
        let now = Utc::now();
        let spec = spec();
        let scaled_up = now - Duration::seconds(100);
        let unneeded = now - Duration::seconds(700);
        let s = PoolSnapshot {
            last_scale_up: Some(scaled_up),
            unneeded_since: BTreeMap::from([("b".to_string(), unneeded)]),
            ..snapshot(
                &spec,
                vec![
                    member("a", MemberState::Online, 3000),
                    member("b", MemberState::Online, 0),
                ],
                vec![],
                now,
            )
        };
        let p = plan(&s);
        assert_eq!(p.decisions.len(), 1);
        let d = &p.decisions[0];
        assert_eq!(d.action, DecisionAction::NoAction);
        let until = hhmmss(scaled_up + Duration::seconds(600));
        assert_eq!(
            d.reason,
            format!(
                "scale-down held (holding after power-on until {until}): b unneeded since {}",
                hhmmss(unneeded)
            )
        );
        assert_eq!(
            d.live_reason,
            format!(
                "scale-down held (holding after power-on, 500s left): b unneeded since {}",
                hhmmss(unneeded)
            )
        );
        // The same situation a little later explains itself identically in status.
        let later = PoolSnapshot {
            now: now + Duration::seconds(15),
            last_scale_up: Some(scaled_up),
            unneeded_since: BTreeMap::from([("b".to_string(), unneeded)]),
            ..snapshot(
                &spec,
                vec![
                    member("a", MemberState::Online, 3000),
                    member("b", MemberState::Online, 0),
                ],
                vec![],
                now + Duration::seconds(15),
            )
        };
        assert_eq!(plan(&later).decisions[0].reason, d.reason);
        assert_eq!(plan(&later).details, p.details);
        assert!(
            p.details
                .contains(&"a: #2 to power off (weight 0, S3 no, 4c/16Gi); busy: utilization 75% >= 50%".to_string())
        );
    }

    #[test]
    fn no_action_explains_waiting_pods() {
        let now = Utc::now();
        let mut spec = spec();
        spec.max_online = Some(1);
        let p = plan(&snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Offline, 0),
            ],
            vec![pod("p1", 1000, 60, now)],
            now,
        ));
        assert_eq!(p.decisions[0].action, DecisionAction::NoAction);
        assert_eq!(p.decisions[0].reason, "pod default/p1 waits: maxOnline (1) reached");
    }

    #[test]
    fn a_node_whose_gpus_are_still_registering_is_needed_by_the_pods_waiting_for_them() {
        let now = Utc::now();
        let mut spec = spec();
        spec.min_online = 0;
        let i915 = "gpu.intel.com/i915".to_string();
        // Woken for the runner; Ready, but its device plugin reports 0 cards so far.
        let mut gpu = member("gpu-node-2", MemberState::Online, 0);
        gpu.registering = BTreeMap::from([(i915.clone(), 1)]);
        let mut runner = pod("runner", 1000, 120, now);
        runner.extended_requests = BTreeMap::from([(i915.clone(), 1)]);
        let snap = |gpu: Member, pending: Vec<PodView>| {
            let mut s = snapshot(
                &spec,
                vec![member("gpu-node-1", MemberState::Online, 3000), gpu],
                pending,
                now,
            );
            s.unneeded_since = BTreeMap::from([("gpu-node-2".to_string(), now - Duration::seconds(700))]);
            s
        };

        let p = plan(&snap(gpu.clone(), vec![runner.clone()]));
        assert!(p.power_off.is_empty(), "the node the runner waits for stays on");
        assert!(!p.unneeded_since.contains_key("gpu-node-2"), "and is not even unneeded");
        assert_eq!(
            p.decisions[0].reason,
            "gpu-node-2 needed: 1 pending pod(s) waiting for its gpu.intel.com/i915 to register"
        );
        assert!(p.details.iter().any(|l| l.starts_with("gpu-node-2: #1 to power off")
            && l.ends_with("needed: 1 pending pod(s) waiting for its gpu.intel.com/i915 to register")));

        // A pod that needs more cards than the node ever had does not hold it.
        let mut greedy = runner.clone();
        greedy.extended_requests = BTreeMap::from([(i915.clone(), 2)]);
        assert_eq!(plan(&snap(gpu.clone(), vec![greedy])).power_off.len(), 1);
        // Neither does a pod not waiting for a device.
        assert_eq!(
            plan(&snap(gpu.clone(), vec![pod("cpu-only", 1000, 120, now)]))
                .power_off
                .len(),
            1
        );
        // Once the plugin has registered the cards (nothing registering), the usual rules apply.
        let mut registered = gpu;
        registered.registering.clear();
        assert_eq!(plan(&snap(registered, vec![runner])).power_off.len(), 1);
    }

    #[test]
    fn ignores_young_pending_pods() {
        let now = Utc::now();
        let spec = spec();
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Offline, 0),
            ],
            vec![pod("p1", 1000, 5, now)],
            now,
        );
        assert!(plan(&s).power_on.is_empty());
    }

    #[test]
    fn booting_capacity_absorbs_pending_pods() {
        let now = Utc::now();
        let spec = spec();
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Booting, 0),
                member("c", MemberState::Offline, 0),
            ],
            vec![pod("p1", 1000, 60, now)],
            now,
        );
        assert!(plan(&s).power_on.is_empty());
    }

    #[test]
    fn respects_max_online_and_selectors() {
        let now = Utc::now();
        let spec = spec();
        let mut gpu = member("gpu", MemberState::Offline, 0);
        gpu.labels.insert("gpu".into(), "true".into());
        let mut p = pod("needs-gpu", 1000, 60, now);
        p.node_selector.insert("gpu".into(), "true".into());
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Offline, 0),
                gpu,
            ],
            vec![p],
            now,
        );
        assert_eq!(
            plan(&s).power_on,
            vec![(
                "gpu".to_string(),
                "unschedulable pod default/needs-gpu; #2 of 2 to power on, the only machine off that fits".to_string()
            )]
        );

        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 3900),
                member("b", MemberState::Online, 3900),
                member("c", MemberState::Online, 3900),
                member("d", MemberState::Offline, 0),
            ],
            vec![pod("p", 1000, 60, now)],
            now,
        );
        assert!(plan(&s).power_on.is_empty(), "max_online reached");
    }

    #[test]
    fn taints_must_be_tolerated_except_transient_ones() {
        let now = Utc::now();
        let mut m = member("a", MemberState::Offline, 0);
        m.taints = vec![Taint {
            key: "node.kubernetes.io/unreachable".into(),
            effect: "NoSchedule".into(),
            ..Default::default()
        }];
        let p = pod("p", 100, 60, now);
        assert!(pod_matches_node(&p, &m.labels, &m.taints));
        m.taints.push(Taint {
            key: "dedicated".into(),
            value: Some("db".into()),
            effect: "NoSchedule".into(),
            ..Default::default()
        });
        assert!(!pod_matches_node(&p, &m.labels, &m.taints));
        let mut tolerant = p.clone();
        tolerant.tolerations = vec![Toleration {
            key: Some("dedicated".into()),
            operator: Some("Equal".into()),
            value: Some("db".into()),
            effect: Some("NoSchedule".into()),
            ..Default::default()
        }];
        assert!(pod_matches_node(&tolerant, &m.labels, &m.taints));
    }

    #[test]
    fn enforces_min_online() {
        let now = Utc::now();
        let mut spec = spec();
        spec.min_online = 2;
        let s = snapshot(
            &spec,
            vec![
                member("a", MemberState::Online, 100),
                member("b", MemberState::Offline, 0),
            ],
            vec![],
            now,
        );
        let p = plan(&s);
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.power_on[0].0, "b");
    }

    #[test]
    fn scales_down_idle_node_after_unneeded_period() {
        let now = Utc::now();
        let spec = spec();
        let mut idle = member("b", MemberState::Online, 200);
        idle.movable_pods = vec![pod("web", 200, 3600, now)];
        let members = vec![member("a", MemberState::Online, 1000), idle];

        // First observation: starts the unneeded timer, no action yet.
        let s = snapshot(&spec, members.clone(), vec![], now);
        let p = plan(&s);
        assert!(p.power_off.is_empty());
        assert!(p.unneeded_since.contains_key("b"));

        // After the unneeded period the least utilized node goes.
        let mut s = snapshot(&spec, members, vec![], now);
        s.unneeded_since = BTreeMap::from([
            ("a".to_string(), now - Duration::seconds(700)),
            ("b".to_string(), now - Duration::seconds(700)),
        ]);
        let p = plan(&s);
        assert_eq!(p.power_off.len(), 1, "min_online=1 keeps one node");
        assert_eq!(p.power_off[0].0, "b");
    }

    #[test]
    fn keeps_a_machine_someone_is_logged_in_to_until_the_recheck() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Online, 0);
        b.logged_in = Some((now - Duration::seconds(60), vec!["alice on pts/0 from 10.0.0.5".into()]));
        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(
                &spec,
                vec![member("a", MemberState::Online, 1000), b.clone()],
                vec![],
                now,
            )
        };
        let p = plan(&s);
        assert!(p.power_off.is_empty());
        assert!(!p.unneeded_since.contains_key("b"), "the unneeded timer starts over");
        assert!(
            p.details.iter().any(|d| d.contains("logged in: alice on pts/0")),
            "{:?}",
            p.details
        );

        // Past the recheck it is unneeded again, and the timer starts afresh.
        b.logged_in = Some((now - Duration::seconds(LOGIN_RECHECK_SECS + 1), b.logged_in.unwrap().1));
        let s = snapshot(&spec, vec![member("a", MemberState::Online, 1000), b], vec![], now);
        let p = plan(&s);
        assert!(p.power_off.is_empty());
        assert!(p.unneeded_since.contains_key("b"));
    }

    #[test]
    fn outside_active_hours_an_idle_machine_goes_at_once_and_a_busy_one_stays() {
        let now = Utc::now();
        let spec = spec();
        let why = "outside active hours (08:00-22:00 UTC)".to_string();
        let mut b = member("b", MemberState::Online, 0);
        b.outside_active_hours = Some(why.clone());
        // Idle: powered off without waiting out unneededSeconds.
        let s = snapshot(
            &spec,
            vec![member("a", MemberState::Online, 1000), b.clone()],
            vec![],
            now,
        );
        let p = plan(&s);
        assert_eq!(p.power_off.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(), ["b"]);
        assert!(p.details.iter().any(|d| d.contains(&why)), "{:?}", p.details);
        // Busy: its work finishes first.
        let mut busy = b.clone();
        busy.counted.cpu_millis = 2000;
        let s = snapshot(&spec, vec![member("a", MemberState::Online, 1000), busy], vec![], now);
        assert!(plan(&s).power_off.is_empty());
        // Someone logged in: kept, as at any hour.
        b.logged_in = Some((now - Duration::seconds(60), vec!["alice on pts/0".into()]));
        let s = snapshot(&spec, vec![member("a", MemberState::Online, 1000), b], vec![], now);
        assert!(plan(&s).power_off.is_empty());
    }

    #[test]
    fn outside_active_hours_an_idle_machine_is_releasable_to_other_pools_at_once() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Online, 0);
        b.outside_active_hours = Some("outside active hours (08:00-20:00 UTC)".into());
        let s = snapshot(&spec, vec![member("a", MemberState::Online, 1000), b], vec![], now);
        let p = plan(&s);
        assert_eq!(p.releasable, ["b"], "needed: {:?}", p.needed);
    }

    #[test]
    fn a_machine_own_unneeded_seconds_overrides_the_pool() {
        let now = Utc::now();
        let spec = spec();
        let pool = spec.scale_down.unneeded_seconds as i64;
        assert!(pool > 300, "the test needs a pool timer over 300 s");
        let mut b = member("b", MemberState::Online, 0);
        b.unneeded_seconds = Some(300);
        let mut c = member("c", MemberState::Online, 0);
        c.unneeded_seconds = None;
        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([
                ("b".to_string(), now - Duration::seconds(301)),
                ("c".to_string(), now - Duration::seconds(301)),
            ]),
            ..snapshot(&spec, vec![member("a", MemberState::Online, 1000), b, c], vec![], now)
        };
        let p = plan(&s);
        assert_eq!(p.releasable, ["b"], "needed: {:?}", p.needed);
        assert!(
            p.power_off.iter().any(|(n, r)| n == "b" && r.contains("for 300s")),
            "{:?}",
            p.power_off
        );
        assert!(!p.power_off.iter().any(|(n, _)| n == "c"));
    }

    #[test]
    fn a_power_request_wakes_its_machine_with_nothing_pending() {
        let now = Utc::now();
        let mut spec = spec();
        spec.scale_up.enabled = false;
        spec.max_online = Some(0);
        let mut b = member("b", MemberState::Offline, 0);
        b.power_requests = Some("power request app/r until 14:00:00Z".into());
        let s = snapshot(&spec, vec![member("a", MemberState::Offline, 0), b], vec![], now);
        let p = plan(&s);
        assert_eq!(
            p.power_on,
            [("b".to_string(), "power request app/r until 14:00:00Z".to_string())]
        );
    }

    #[test]
    fn a_power_request_keeps_an_idle_machine_and_tells_the_other_pools() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Online, 0);
        b.power_requests = Some("power request app/r until 14:00:00Z".into());
        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(7200))]),
            ..snapshot(&spec, vec![member("a", MemberState::Online, 1000), b], vec![], now)
        };
        let p = plan(&s);
        assert!(p.power_off.is_empty(), "{:?}", p.power_off);
        assert!(!p.unneeded_since.contains_key("b"));
        assert_eq!(
            p.needed.get("b").map(String::as_str),
            Some("busy: power request app/r until 14:00:00Z")
        );
    }

    #[test]
    fn does_not_scale_down_when_pods_would_not_fit() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Online, 1500);
        b.movable_pods = vec![pod("big", 1500, 3600, now)];
        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(&spec, vec![member("a", MemberState::Online, 3000), b], vec![], now)
        };
        assert!(plan(&s).power_off.is_empty());
    }

    #[test]
    fn blocking_pods_and_pending_pods_prevent_scale_down() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Online, 0);
        b.blocking_pods = vec!["default/bare".into()];
        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(&spec, vec![member("a", MemberState::Online, 0), b], vec![], now)
        };
        assert!(plan(&s).power_off.is_empty());

        let s = PoolSnapshot {
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(
                &spec,
                vec![
                    member("a", MemberState::Online, 3900),
                    member("b", MemberState::Online, 3900),
                    member("c", MemberState::Offline, 0),
                ],
                vec![pod("p", 1000, 60, now)],
                now,
            )
        };
        let p = plan(&s);
        assert_eq!(p.power_on.len(), 1);
        assert!(p.power_off.is_empty());
    }

    #[test]
    fn recent_scale_up_delays_scale_down() {
        let now = Utc::now();
        let spec = spec();
        let s = PoolSnapshot {
            last_scale_up: Some(now - Duration::seconds(60)),
            unneeded_since: BTreeMap::from([("b".to_string(), now - Duration::seconds(700))]),
            ..snapshot(
                &spec,
                vec![member("a", MemberState::Online, 0), member("b", MemberState::Online, 0)],
                vec![],
                now,
            )
        };
        assert!(plan(&s).power_off.is_empty());
    }

    #[test]
    fn non_auto_members_are_never_touched() {
        let now = Utc::now();
        let spec = spec();
        let mut b = member("b", MemberState::Offline, 0);
        b.auto = false;
        let s = snapshot(
            &spec,
            vec![member("a", MemberState::Online, 3900), b],
            vec![pod("p", 1000, 60, now)],
            now,
        );
        let p = plan(&s);
        assert!(p.power_on.is_empty());
        assert_eq!(p.relevant_pending, 0);
    }

    #[test]
    fn powered_off_node_taints_do_not_block_scale_up() {
        // Taints seen on a powered-off k3s + Cilium node.
        let now = Utc::now();
        let mut m = member("gpu-1", MemberState::Offline, 0);
        m.taints = [
            ("node.kubernetes.io/out-of-service", "NoExecute"),
            ("node.kubernetes.io/unreachable", "NoSchedule"),
            ("node.cilium.io/agent-not-ready", "NoSchedule"),
            ("node.kubernetes.io/unreachable", "NoExecute"),
        ]
        .iter()
        .map(|(k, e)| Taint {
            key: k.to_string(),
            effect: e.to_string(),
            ..Default::default()
        })
        .collect();
        assert!(pod_matches_node(&pod("p", 100, 60, now), &m.labels, &m.taints));
    }

    #[test]
    fn daemonset_pods_can_block_scale_down() {
        let mk = |v: serde_json::Value| -> Pod { serde_json::from_value(v).unwrap() };
        let ds = serde_json::json!([{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "ds", "uid": "2"}]);
        let busy = mk(
            serde_json::json!({"metadata": {"name": "gpu-worker", "ownerReferences": ds,
            "annotations": {SAFE_TO_EVICT_ANNOTATION: "false"}}}),
        );
        assert!(matches!(drain_class(&busy), DrainClass::Block(_)));
        let idle = mk(
            serde_json::json!({"metadata": {"name": "gpu-worker", "ownerReferences": ds,
            "annotations": {SAFE_TO_EVICT_ANNOTATION: "true"}}}),
        );
        assert_eq!(drain_class(&idle), DrainClass::Ignore);
    }

    #[test]
    fn classifies_pods_for_drain() {
        let mk = |v: serde_json::Value| -> Pod { serde_json::from_value(v).unwrap() };
        let owned = serde_json::json!([{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs", "uid": "1"}]);
        let ds = serde_json::json!([{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "ds", "uid": "2"}]);
        assert_eq!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a", "ownerReferences": owned}})
            )),
            DrainClass::Evict
        );
        assert_eq!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a", "ownerReferences": ds}})
            )),
            DrainClass::Ignore
        );
        assert!(matches!(
            drain_class(&mk(serde_json::json!({"metadata": {"name": "a"}}))),
            DrainClass::Block(_)
        ));
        assert_eq!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a", "annotations": {SAFE_TO_EVICT_ANNOTATION: "true"}}})
            )),
            DrainClass::Evict
        );
        assert!(matches!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a", "ownerReferences": owned,
                "annotations": {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}}})
            )),
            DrainClass::Block(_)
        ));
        assert_eq!(
            drain_class(&mk(
                serde_json::json!({"metadata": {"name": "a"}, "status": {"phase": "Succeeded"}})
            )),
            DrainClass::Ignore
        );
        // The operator's own helper pods (probes, suspend/shutdown, wake relay) never hold up a drain.
        for component in ["sleep-probe", "session-check", "shutdown", "wake-relay"] {
            assert_eq!(
                drain_class(&mk(serde_json::json!({"metadata": {"name": "kha-x", "labels": {
                    "app.kubernetes.io/name": "kube-hardware-autoscaler",
                    "app.kubernetes.io/component": component}}}))),
                DrainClass::Ignore,
                "{component}"
            );
        }
    }
}

#[cfg(test)]
mod dedicated_pool_tests {
    //! A single-machine GPU pool that only GPU work may wake, with guest pods
    //! and DaemonSets that must not keep it on (the "standby GPU" setup).
    use super::*;
    use crate::crd::{NodeSelector, ScaleDownSpec, ScaleUpSpec};

    const GI: i64 = 1 << 30;

    fn gpu_spec() -> NodeScalingPoolSpec {
        NodeScalingPoolSpec {
            node_selector: NodeSelector {
                match_labels: BTreeMap::from([("example.com/gpu".to_string(), "true".to_string())]),
                match_expressions: vec![],
            },
            min_online: 0,
            max_online: Some(1),
            scale_up: ScaleUpSpec {
                enabled: true,
                pending_pod_grace_seconds: 15,
                max_nodes_per_step: 1,
                require_explicit_selection: true,
                prefer_s3_capable: false,
                preferred_nodes: vec![],
                boot_timeout_seconds: None,
                boot_failure_backoff_seconds: 1800,
                power_on_timeout_seconds: None,
                power_on_failure_policy: Default::default(),
                strategies: vec![],
                hedge: None,
                learned_power_on_timeout: None,
                boot_failure_backoff_max_seconds: None,
            },
            scale_down: ScaleDownSpec {
                enabled: true,
                utilization_threshold_percent: 10,
                unneeded_seconds: 900,
                hold_after_power_on_seconds: Some(900),
                delay_after_scale_up_seconds: None,
                drain_failure_backoff_seconds: 1800,
                max_nodes_per_step: 1,
                ignore_daemon_set_utilization: true,
                ignore_non_selecting_pod_utilization: true,
            },
            manual_power_on_policy: None,
            manual_power_on_grace_seconds: 600,
            manual_power_off_cooldown_seconds: None,
        }
    }

    fn cpu(millis: i64) -> ResourceAmounts {
        ResourceAmounts {
            cpu_millis: millis,
            memory_bytes: GI,
            pods: 1,
        }
    }

    fn gpu_box(state: MemberState) -> Member {
        Member {
            name: "gpu-1".into(),
            state,
            auto: true,
            labels: BTreeMap::from([("example.com/gpu".to_string(), "true".to_string())]),
            taints: vec![],
            allocatable: ResourceAmounts {
                cpu_millis: 12000,
                memory_bytes: 64 * GI,
                pods: 110,
            },
            requested: ResourceAmounts::default(),
            counted: ResourceAmounts::default(),
            movable_pods: vec![],
            blocking_pods: vec![],
            drain_failed_at: None,
            logged_in: None,
            s3_capable: false,
            registering: BTreeMap::new(),
            woken_by: None,
            note: None,
            adopted_at: None,
            release_after: None,
            outside_active_hours: None,
            unneeded_seconds: None,
            power_requests: None,
            history: Default::default(),
        }
    }

    fn pod(name: &str, selector: &[(&str, &str)], now: DateTime<Utc>) -> PodView {
        PodView {
            namespace: "default".into(),
            name: name.into(),
            requests: cpu(1000),
            node_selector: selector.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            created: Some(now - Duration::seconds(60)),
            ..Default::default()
        }
    }

    fn base(free_cpu: i64) -> ExternalNode {
        ExternalNode {
            name: "always-on-1".into(),
            labels: BTreeMap::new(),
            taints: vec![],
            free: ResourceAmounts {
                cpu_millis: free_cpu,
                memory_bytes: 32 * GI,
                pods: 100,
            },
        }
    }

    fn snap<'a>(
        spec: &'a NodeScalingPoolSpec,
        m: Member,
        pending: Vec<PodView>,
        now: DateTime<Utc>,
    ) -> PoolSnapshot<'a> {
        PoolSnapshot {
            spec,
            members: vec![m],
            pending,
            now,
            last_scale_up: None,
            unneeded_since: BTreeMap::new(),
            external: vec![],
            kept_by_others: BTreeMap::new(),
        }
    }

    #[test]
    fn ci_pods_do_not_wake_the_pool_but_gpu_jobs_do() {
        let now = Utc::now();
        let spec = gpu_spec();
        let ci = pod("runner", &[], now);
        let p = plan(&snap(&spec, gpu_box(MemberState::Offline), vec![ci], now));
        assert!(
            p.power_on.is_empty(),
            "an untargeted pod must not power on a dedicated pool"
        );
        assert_eq!(p.relevant_pending, 0);

        let job = pod("transcode", &[("example.com/gpu", "true")], now);
        let p = plan(&snap(&spec, gpu_box(MemberState::Offline), vec![job], now));
        assert_eq!(p.power_on.len(), 1);
    }

    #[test]
    fn required_affinity_counts_as_explicit_selection() {
        let now = Utc::now();
        let spec = gpu_spec();
        let mut job = pod("transcode", &[], now);
        job.affinity_terms = vec![vec![NodeSelectorRequirement {
            key: "example.com/gpu".into(),
            operator: "In".into(),
            values: Some(vec!["true".into()]),
        }]];
        assert!(job.explicitly_selects(&spec.node_selector));
        // "In [true, false]" also allows non-GPU nodes: not explicit.
        job.affinity_terms[0][0].values = Some(vec!["true".into(), "false".into()]);
        assert!(!job.explicitly_selects(&spec.node_selector));
    }

    #[test]
    fn guests_move_to_the_always_on_base_so_the_pool_can_power_off() {
        let now = Utc::now();
        let spec = gpu_spec();
        let mut m = gpu_box(MemberState::Online);
        // A CI runner landed on the GPU box: it is requested load, but not counted.
        m.requested = cpu(2000);
        m.counted = ResourceAmounts::default();
        m.movable_pods = vec![pod("runner", &[], now)];
        let mut s = snap(&spec, m, vec![], now);
        s.unneeded_since = BTreeMap::from([("gpu-1".to_string(), now - Duration::seconds(1000))]);

        // Without anywhere to move the runner, the single-machine pool is stuck on.
        assert!(plan(&s).power_off.is_empty());
        // With room on the always-on base, it powers off.
        s.external = vec![base(4000)];
        assert_eq!(plan(&s).power_off.len(), 1);
        // But not when the base is full.
        s.external = vec![base(500)];
        assert!(plan(&s).power_off.is_empty());
    }

    #[test]
    fn running_gpu_job_blocks_power_off() {
        let now = Utc::now();
        let spec = gpu_spec();
        let mut m = gpu_box(MemberState::Online);
        m.blocking_pods = vec!["default/transcode (annotated safe-to-evict=false)".into()];
        let mut s = snap(&spec, m, vec![], now);
        s.unneeded_since = BTreeMap::from([("gpu-1".to_string(), now - Duration::seconds(1000))]);
        s.external = vec![base(4000)];
        let p = plan(&s);
        assert!(p.power_off.is_empty());
        assert!(
            !p.unneeded_since.contains_key("gpu-1"),
            "a node with a running job is not even unneeded"
        );
    }
}

#[cfg(test)]
mod overlapping_pool_tests {
    //! Two dedicated pools over the same machines: `gpu`
    //! (gpu=true: gpu-node-1, gpu-node-2) and `ci` (ci=true: gpu-node-2,
    //! ci-node-1, gpu-node-1), each with its own pending pods, utilization rules
    //! and preferences.
    use super::*;
    use crate::crd::{NodeSelector, PreferredNode};

    const GI: i64 = 1 << 30;

    fn pool(label: &str, weights: &[(&str, u32)], max_online: u32) -> NodeScalingPoolSpec {
        let mut spec: NodeScalingPoolSpec = serde_json::from_value(serde_json::json!({
            "nodeSelector": {"matchLabels": {label: "true"}},
            "minOnline": 0,
            "maxOnline": max_online,
            "scaleUp": {"requireExplicitSelection": true, "pendingPodGraceSeconds": 15, "maxNodesPerStep": 1},
            "scaleDown": {
                "ignoreDaemonSetUtilization": true,
                "ignoreNonSelectingPodUtilization": true,
                "utilizationThresholdPercent": 10,
                "unneededSeconds": 300,
                "holdAfterPowerOnSeconds": 600
            }
        }))
        .unwrap();
        spec.scale_up.preferred_nodes = weights
            .iter()
            .map(|(n, w)| PreferredNode {
                name: n.to_string(),
                weight: *w,
            })
            .collect();
        let _: &NodeSelector = &spec.node_selector;
        spec
    }

    fn gpu() -> NodeScalingPoolSpec {
        pool("gpu", &[("gpu-node-1", 100), ("gpu-node-2", 10)], 2)
    }

    fn ci() -> NodeScalingPoolSpec {
        pool("ci", &[("gpu-node-2", 100), ("ci-node-1", 50), ("gpu-node-1", 10)], 3)
    }

    fn machine(name: &str, labels: &[&str], state: MemberState) -> Member {
        Member {
            name: name.into(),
            state,
            auto: true,
            labels: labels.iter().map(|l| (l.to_string(), "true".to_string())).collect(),
            taints: vec![],
            allocatable: ResourceAmounts {
                cpu_millis: 8000,
                memory_bytes: 32 * GI,
                pods: 110,
            },
            requested: ResourceAmounts::default(),
            counted: ResourceAmounts::default(),
            movable_pods: vec![],
            blocking_pods: vec![],
            drain_failed_at: None,
            logged_in: None,
            s3_capable: false,
            registering: BTreeMap::new(),
            woken_by: None,
            note: None,
            adopted_at: None,
            release_after: None,
            outside_active_hours: None,
            unneeded_seconds: None,
            power_requests: None,
            history: Default::default(),
        }
    }

    /// The members each pool sees (only those its selector matches).
    fn fleet(state: MemberState, pool_label: &str) -> Vec<Member> {
        [
            machine("gpu-node-1", &["gpu", "ci"], state),
            machine("gpu-node-2", &["gpu", "ci"], state),
            machine("ci-node-1", &["ci"], state),
        ]
        .into_iter()
        .filter(|m| m.labels.contains_key(pool_label))
        .collect()
    }

    fn job(name: &str, label: &str, now: DateTime<Utc>) -> PodView {
        PodView {
            namespace: "default".into(),
            name: name.into(),
            requests: ResourceAmounts {
                cpu_millis: 1000,
                memory_bytes: GI,
                pods: 1,
            },
            node_selector: BTreeMap::from([(label.to_string(), "true".to_string())]),
            created: Some(now - Duration::seconds(60)),
            ..Default::default()
        }
    }

    fn snap<'a>(
        spec: &'a NodeScalingPoolSpec,
        members: Vec<Member>,
        pending: Vec<PodView>,
        now: DateTime<Utc>,
    ) -> PoolSnapshot<'a> {
        PoolSnapshot {
            spec,
            members,
            pending,
            now,
            last_scale_up: None,
            unneeded_since: BTreeMap::new(),
            external: vec![],
            kept_by_others: BTreeMap::new(),
        }
    }

    /// What `other_pools_holding` derives from the other pool's status.
    fn holds_from(pool: &str, other: &Plan, member: &str) -> BTreeMap<String, Vec<(String, String)>> {
        if other.releasable.iter().any(|m| m == member) {
            return BTreeMap::new();
        }
        let why = other
            .needed
            .get(member)
            .cloned()
            .unwrap_or_else(|| "not evaluated yet".into());
        BTreeMap::from([(member.to_string(), vec![(pool.to_string(), why)])])
    }

    #[test]
    fn a_gpu_job_wakes_the_gpu_node_through_gpu_and_ci_ignores_it() {
        let now = Utc::now();
        let (gpu, ci) = (gpu(), ci());
        let pending = vec![job("transcode", "gpu", now)];
        let p = plan(&snap(&gpu, fleet(MemberState::Offline, "gpu"), pending.clone(), now));
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.power_on[0].0, "gpu-node-1");
        let p = plan(&snap(&ci, fleet(MemberState::Offline, "ci"), pending, now));
        assert!(p.power_on.is_empty(), "a GPU job does not target ci");
    }

    #[test]
    fn a_ci_job_wakes_the_shared_node_through_ci_and_gpu_ignores_it() {
        let now = Utc::now();
        let (gpu, ci) = (gpu(), ci());
        let pending = vec![job("runner", "ci", now)];
        let p = plan(&snap(&ci, fleet(MemberState::Offline, "ci"), pending.clone(), now));
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.power_on[0].0, "gpu-node-2");
        assert!(
            p.power_on[0].1.contains("over ci-node-1 by weight (100 vs 50)"),
            "{}",
            p.power_on[0].1
        );
        let p = plan(&snap(&gpu, fleet(MemberState::Offline, "gpu"), pending, now));
        assert!(p.power_on.is_empty(), "a CI job does not target gpu");
    }

    #[test]
    fn max_online_counts_machines_another_pool_woke() {
        let now = Utc::now();
        let mut ci = ci();
        ci.max_online = Some(1);
        // gpu-node-1 is on because gpu woke it; ci may not wake a second machine.
        let mut members = fleet(MemberState::Offline, "ci");
        members[0].state = MemberState::Online;
        members[0].woken_by = Some("gpu".into());
        members[0].requested.cpu_millis = 8000;
        let p = plan(&snap(&ci, members, vec![job("runner", "ci", now)], now));
        assert!(p.power_on.is_empty());
        assert_eq!(p.decisions[0].reason, "pod default/runner waits: maxOnline (1) reached");
    }

    #[test]
    fn a_ci_job_keeps_the_shared_node_on_through_ci_although_gpu_ignores_it_and_power_off_needs_both() {
        let now = Utc::now();
        let (gpu, ci) = (gpu(), ci());
        let long_ago = now - Duration::seconds(1000);
        // gpu-node-2 runs a CI job: requested load. ci counts it (the runner
        // selects ci=true); gpu does not (it does not select gpu=true).
        let busy = |label: &str| {
            let mut ms = fleet(MemberState::Online, label);
            for m in ms.iter_mut() {
                if m.name == "gpu-node-2" {
                    m.requested.cpu_millis = 4000;
                    if label == "ci" {
                        m.counted.cpu_millis = 4000;
                    }
                }
            }
            ms
        };
        let all_unneeded = |members: &[Member]| -> BTreeMap<String, DateTime<Utc>> {
            members.iter().map(|m| (m.name.clone(), long_ago)).collect()
        };

        let mut ci_snap = snap(&ci, busy("ci"), vec![], now);
        ci_snap.unneeded_since = all_unneeded(&ci_snap.members);
        let ci_plan = plan(&ci_snap);
        assert!(!ci_plan.releasable.contains(&"gpu-node-2".to_string()));
        assert_eq!(ci_plan.needed["gpu-node-2"], "busy: utilization 50% >= 10%");

        // gpu alone would power gpu-node-2 off (lowest weight, idle by its rules)...
        let mut gpu_snap = snap(&gpu, busy("gpu"), vec![], now);
        gpu_snap.unneeded_since = all_unneeded(&gpu_snap.members);
        gpu_snap.external = vec![ExternalNode {
            name: "always-on-1".into(),
            labels: BTreeMap::new(),
            taints: vec![],
            free: ResourceAmounts {
                cpu_millis: 16000,
                memory_bytes: 64 * GI,
                pods: 100,
            },
        }];
        assert_eq!(plan(&gpu_snap).power_off[0].0, "gpu-node-2");
        // ...but ci keeps it on, and gpu says so. gpu-node-1, which ci releases, goes instead.
        gpu_snap.kept_by_others = holds_from("ci", &ci_plan, "gpu-node-2");
        let gpu_plan = plan(&gpu_snap);
        assert_eq!(gpu_plan.power_off.len(), 1);
        assert_eq!(gpu_plan.power_off[0].0, "gpu-node-1");
        assert!(
            gpu_plan.details.iter().any(|l| l.starts_with("gpu-node-2:")
                && l.ends_with("kept: needed by pool ci: busy: utilization 50% >= 10%")),
            "{:?}",
            gpu_plan.details
        );

        // Once the job is gone, ci releases it too, and gpu powers it off.
        let mut ci_snap = snap(&ci, fleet(MemberState::Online, "ci"), vec![], now);
        ci_snap.unneeded_since = all_unneeded(&ci_snap.members);
        let ci_plan = plan(&ci_snap);
        assert!(ci_plan.releasable.contains(&"gpu-node-2".to_string()));
        gpu_snap.kept_by_others = holds_from("ci", &ci_plan, "gpu-node-2");
        assert_eq!(plan(&gpu_snap).power_off[0].0, "gpu-node-2");
    }

    #[test]
    fn a_pool_holding_after_power_on_or_with_pending_pods_releases_nothing() {
        let now = Utc::now();
        let ci = ci();
        let long_ago = now - Duration::seconds(1000);
        let mut s = snap(&ci, fleet(MemberState::Online, "ci"), vec![], now);
        s.unneeded_since = s.members.iter().map(|m| (m.name.clone(), long_ago)).collect();
        s.last_scale_up = Some(now - Duration::seconds(60));
        let p = plan(&s);
        assert!(p.releasable.is_empty());
        assert!(p.needed["gpu-node-2"].starts_with("holding after power-on until "));

        // Recently unneeded: not releasable yet, and the reason says when it will be.
        let mut s = snap(&ci, fleet(MemberState::Online, "ci"), vec![], now);
        s.unneeded_since = BTreeMap::from([("gpu-node-1".to_string(), now - Duration::seconds(10))]);
        let p = plan(&s);
        assert!(
            p.needed["gpu-node-1"].starts_with("unneeded since "),
            "{}",
            p.needed["gpu-node-1"]
        );
    }
}

#[cfg(test)]
mod boot_and_manual_tests {
    //! Boot failures and machines powered on by hand, as the planner sees them.
    use super::*;

    const GI: i64 = 1 << 30;

    fn spec() -> NodeScalingPoolSpec {
        serde_json::from_value(serde_json::json!({
            "nodeSelector": {"matchLabels": {"example.com/gpu": "true"}},
            "minOnline": 0,
            "maxOnline": 2,
            "scaleDown": {"unneededSeconds": 300, "utilizationThresholdPercent": 10, "holdAfterPowerOnSeconds": 600}
        }))
        .unwrap()
    }

    fn machine(name: &str, state: MemberState) -> Member {
        Member {
            name: name.into(),
            state,
            auto: true,
            labels: BTreeMap::from([("example.com/gpu".to_string(), "true".to_string())]),
            taints: vec![],
            allocatable: ResourceAmounts {
                cpu_millis: 8000,
                memory_bytes: 32 * GI,
                pods: 110,
            },
            requested: ResourceAmounts::default(),
            counted: ResourceAmounts::default(),
            movable_pods: vec![],
            blocking_pods: vec![],
            drain_failed_at: None,
            logged_in: None,
            s3_capable: false,
            registering: BTreeMap::new(),
            woken_by: None,
            note: None,
            adopted_at: None,
            release_after: None,
            outside_active_hours: None,
            unneeded_seconds: None,
            power_requests: None,
            history: Default::default(),
        }
    }

    fn snap(spec: &NodeScalingPoolSpec, members: Vec<Member>, now: DateTime<Utc>) -> PoolSnapshot<'_> {
        PoolSnapshot {
            spec,
            unneeded_since: members
                .iter()
                .map(|m| (m.name.clone(), now - Duration::seconds(1000)))
                .collect(),
            members,
            pending: vec![],
            now,
            last_scale_up: None,
            external: vec![],
            kept_by_others: BTreeMap::new(),
        }
    }

    #[test]
    fn a_machine_that_failed_to_boot_does_not_hold_the_pool() {
        let now = Utc::now();
        let spec = spec();
        // Still booting: the idle machine is kept while capacity is on its way.
        let booting = plan(&snap(
            &spec,
            vec![
                machine("gpu-node-1", MemberState::Online),
                machine("gpu-node-2", MemberState::Booting),
            ],
            now,
        ));
        assert!(booting.power_off.is_empty());
        assert!(booting.decisions[0].reason.contains("scale-down held (nodes booting)"));

        // Past its boot timeout it is boot-failed: the hold is released.
        let mut failed = machine("gpu-node-2", MemberState::Unavailable);
        failed.note = Some("boot failed at 12:00:00Z: on 20m+ without Ready; released hold; powering off".into());
        let p = plan(&snap(
            &spec,
            vec![machine("gpu-node-1", MemberState::Online), failed],
            now,
        ));
        assert_eq!(p.power_off.len(), 1);
        assert_eq!(p.power_off[0].0, "gpu-node-1");
        assert!(
            p.details.contains(
                &"gpu-node-2: boot failed at 12:00:00Z: on 20m+ without Ready; released hold; powering off".to_string()
            ),
            "{:?}",
            p.details
        );
    }

    #[test]
    fn the_boot_failure_is_in_the_decision_log() {
        let now = Utc::now();
        let spec = spec();
        let mut busy = machine("gpu-node-1", MemberState::Online);
        busy.counted.cpu_millis = 6000;
        let mut failed = machine("gpu-node-2", MemberState::Unavailable);
        failed.note = Some("boot failed at 12:00:00Z: on 20m+ without Ready; released hold; powering off".into());
        let p = plan(&snap(&spec, vec![busy, failed], now));
        assert_eq!(p.decisions[0].action, DecisionAction::NoAction);
        assert!(
            p.decisions[0]
                .reason
                .ends_with("; gpu-node-2 boot failed at 12:00:00Z: on 20m+ without Ready; released hold; powering off"),
            "{}",
            p.decisions[0].reason
        );
    }

    #[test]
    fn a_machine_left_on_by_hand_is_never_powered_off_and_blocks_nothing() {
        let now = Utc::now();
        let mut spec = spec();
        spec.max_online = Some(1);
        let mut manual = machine("gpu-node-1", MemberState::Manual);
        manual.note = Some("manually powered on at 12:00:00Z; policy LeaveOn; not managing".into());
        let pending = vec![PodView {
            namespace: "default".into(),
            name: "job".into(),
            requests: ResourceAmounts {
                cpu_millis: 1000,
                memory_bytes: GI,
                pods: 1,
            },
            created: Some(now - Duration::seconds(120)),
            ..Default::default()
        }];
        let mut s = snap(
            &spec,
            vec![manual.clone(), machine("gpu-node-2", MemberState::Offline)],
            now,
        );
        s.pending = pending;
        let p = plan(&s);
        // Idle, yet not powered off; not counted towards maxOnline, so the pool
        // still wakes another machine for its pod.
        assert!(p.power_off.is_empty());
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.power_on[0].0, "gpu-node-2");

        // It is room for pods evicted from a machine being powered off.
        let mut leaving = machine("gpu-node-3", MemberState::Online);
        leaving.movable_pods = vec![PodView {
            namespace: "default".into(),
            name: "web".into(),
            requests: ResourceAmounts {
                cpu_millis: 500,
                memory_bytes: GI,
                pods: 1,
            },
            ..Default::default()
        }];
        let p = plan(&snap(&spec, vec![manual, leaving], now));
        assert_eq!(p.power_off.len(), 1);
        assert_eq!(p.power_off[0].0, "gpu-node-3");
    }

    #[test]
    fn an_adopted_machine_holds_the_pool_from_its_manual_power_on() {
        let now = Utc::now();
        let spec = spec();
        let mut adopted = machine("gpu-node-1", MemberState::Online);
        adopted.adopted_at = Some(now - Duration::seconds(120));
        let p = plan(&snap(&spec, vec![adopted.clone()], now));
        assert!(p.power_off.is_empty());
        assert!(p.decisions[0].reason.contains("holding after power-on until"));
        adopted.adopted_at = Some(now - Duration::seconds(700));
        assert_eq!(plan(&snap(&spec, vec![adopted], now)).power_off.len(), 1);
    }

    #[test]
    fn power_off_policy_powers_it_off_after_the_grace_busy_or_not() {
        let now = Utc::now();
        let spec = spec();
        let mut m = machine("gpu-node-1", MemberState::Online);
        m.counted.cpu_millis = 6000; // busy by the utilization rule
        m.release_after = Some(now + Duration::seconds(60));
        let mut s = snap(&spec, vec![m.clone()], now);
        s.unneeded_since.clear();
        assert!(plan(&s).power_off.is_empty(), "within the grace");
        m.release_after = Some(now - Duration::seconds(1));
        let mut s = snap(&spec, vec![m.clone()], now);
        s.unneeded_since.clear();
        let p = plan(&s);
        assert_eq!(p.power_off.len(), 1);
        assert!(p.power_off[0].1.starts_with("manually powered on; policy PowerOff"));
        assert!(p.releasable.contains(&"gpu-node-1".to_string()));
        // Pods that must not be evicted still keep it on.
        m.blocking_pods = vec!["default/job (annotated safe-to-evict=false)".into()];
        let mut s = snap(&spec, vec![m], now);
        s.unneeded_since.clear();
        assert!(plan(&s).power_off.is_empty());
    }
}

#[cfg(test)]
mod strategy_tests {
    //! `scaleUp.strategies` and `scaleUp.hedge`.
    use super::*;

    const GI: i64 = 1 << 30;

    fn spec(extra: serde_json::Value) -> NodeScalingPoolSpec {
        let mut v = serde_json::json!({
            "nodeSelector": {"matchLabels": {"example.com/gpu": "true"}},
            "minOnline": 0,
            "scaleUp": {"maxNodesPerStep": 1, "pendingPodGraceSeconds": 0}
        });
        for (k, val) in extra.as_object().unwrap() {
            v["scaleUp"][k] = val.clone();
        }
        serde_json::from_value(v).unwrap()
    }

    fn machine(name: &str, cpu: i64) -> Member {
        Member {
            name: name.into(),
            state: MemberState::Offline,
            auto: true,
            labels: BTreeMap::from([("example.com/gpu".to_string(), "true".to_string())]),
            taints: vec![],
            allocatable: ResourceAmounts {
                cpu_millis: cpu * 1000,
                memory_bytes: 32 * GI,
                pods: 110,
            },
            requested: ResourceAmounts::default(),
            counted: ResourceAmounts::default(),
            movable_pods: vec![],
            blocking_pods: vec![],
            drain_failed_at: None,
            logged_in: None,
            s3_capable: false,
            registering: BTreeMap::new(),
            woken_by: None,
            note: None,
            adopted_at: None,
            release_after: None,
            outside_active_hours: None,
            unneeded_seconds: None,
            power_requests: None,
            history: MachineHistory {
                lottery: 0.5,
                ..Default::default()
            },
        }
    }

    fn booted(mut m: Member, avg: f64, ok: u32, failed: u32) -> Member {
        m.history.boot_average_seconds = Some(avg);
        m.history.boots_succeeded = ok;
        m.history.boots_failed = failed;
        m
    }

    fn pending(now: DateTime<Utc>) -> Vec<PodView> {
        vec![PodView {
            namespace: "transcode".into(),
            name: "worker".into(),
            requests: ResourceAmounts {
                cpu_millis: 1000,
                memory_bytes: GI,
                pods: 1,
            },
            created: Some(now - Duration::seconds(60)),
            ..Default::default()
        }]
    }

    fn woken(spec: &NodeScalingPoolSpec, members: Vec<Member>) -> Plan {
        let now = Utc::now();
        plan(&PoolSnapshot {
            spec,
            members,
            pending: pending(now),
            now,
            last_scale_up: None,
            unneeded_since: BTreeMap::new(),
            external: vec![],
            kept_by_others: BTreeMap::new(),
        })
    }

    #[test]
    fn without_strategies_the_fixed_order_is_unchanged() {
        let spec = spec(serde_json::json!({"preferredNodes": [{"name": "slow", "weight": 100}]}));
        let p = woken(
            &spec,
            vec![
                booted(machine("fast", 8), 30.0, 10, 0),
                booted(machine("slow", 8), 900.0, 2, 8),
            ],
        );
        assert_eq!(
            p.power_on[0].0, "slow",
            "preferredNodes still decide when no strategy is listed"
        );
    }

    #[test]
    fn fastest_boot_and_reliable_learn_from_history() {
        let spec = spec(serde_json::json!({
            "strategies": [{"name": "FastestBoot", "weight": 2}, {"name": "Reliable", "weight": 2}]
        }));
        let p = woken(
            &spec,
            vec![
                booted(machine("slow", 8), 900.0, 2, 8),
                booted(machine("fast", 8), 30.0, 10, 0),
            ],
        );
        assert_eq!(p.power_on[0].0, "fast");
        assert!(
            p.power_on[0].1.contains("over slow by score (")
                && p.power_on[0].1.contains("mostly FastestBoot: boot 30s vs 900s"),
            "{}",
            p.power_on[0].1
        );
        // A machine with no history sits between the two.
        let ranking = Ranking::new(&spec, []);
        let fresh = ranking.score(&machine("new", 8)).total;
        assert!(ranking.score(&booted(machine("fast", 8), 30.0, 10, 0)).total > fresh);
        assert!(ranking.score(&booted(machine("slow", 8), 900.0, 2, 8)).total < fresh);
    }

    #[test]
    fn reachable_wakes_whoever_answers_and_an_unreachable_machine_only_last() {
        let spec = spec(serde_json::json!({
            "strategies": [{"name": "Reachable", "weight": 1}, {"name": "FastestBoot", "weight": 5}]
        }));
        let mut dark = booted(machine("dark", 8), 20.0, 10, 0);
        dark.history.reachable = Some(false);
        dark.history.answered = Some(0.0);
        let mut lit = booted(machine("lit", 8), 400.0, 10, 0);
        lit.history.reachable = Some(true);
        lit.history.answered = Some(1.0);
        lit.history.rtt_ms = Some(2);
        // Faster to boot, but its management interface does not answer.
        let p = woken(&spec, vec![dark.clone(), lit]);
        assert_eq!(p.power_on[0].0, "lit");
        assert!(
            p.power_on[0]
                .1
                .contains("Reachable (dark's management interface does not answer)"),
            "{}",
            p.power_on[0].1
        );
        // Still a last resort when it is the only machine that fits.
        let p = woken(&spec, vec![dark]);
        assert_eq!(p.power_on[0].0, "dark");
    }

    #[test]
    fn largest_smallest_and_standby_rate_what_they_say() {
        let big = machine("big", 32);
        let small = machine("small", 4);
        let largest = spec(serde_json::json!({"strategies": [{"name": "Largest", "weight": 1}]}));
        assert_eq!(woken(&largest, vec![small.clone(), big.clone()]).power_on[0].0, "big");
        let smallest = spec(serde_json::json!({"strategies": [{"name": "Smallest", "weight": 1}]}));
        assert_eq!(
            woken(&smallest, vec![big.clone(), small.clone()]).power_on[0].0,
            "small"
        );
        let standby = spec(serde_json::json!({"strategies": [{"name": "Standby", "weight": 1}]}));
        let mut sleeper = small.clone();
        sleeper.s3_capable = true;
        assert_eq!(woken(&standby, vec![big, sleeper]).power_on[0].0, "small");
    }

    #[test]
    fn random_follows_the_draw() {
        let spec = spec(serde_json::json!({"strategies": [{"name": "Random", "weight": 1}]}));
        let (mut a, mut b) = (machine("a", 8), machine("b", 8));
        a.history.lottery = 0.2;
        b.history.lottery = 0.9;
        let p = woken(&spec, vec![a.clone(), b.clone()]);
        assert_eq!(p.power_on[0].0, "b");
        assert!(
            p.power_on[0].1.contains("mostly Random: drew 0.90 vs 0.20"),
            "{}",
            p.power_on[0].1
        );
        a.history.lottery = 0.95;
        assert_eq!(woken(&spec, vec![a, b]).power_on[0].0, "a");
    }

    #[test]
    fn scale_down_powers_off_the_lowest_score_first() {
        let spec = spec(serde_json::json!({"strategies": [{"name": "FastestBoot", "weight": 1}]}));
        let fast = booted(machine("fast", 8), 30.0, 10, 0);
        let slow = booted(machine("slow", 8), 900.0, 10, 0);
        let ranking = Ranking::new(&spec, [&fast, &slow]);
        let mut down = [&fast, &slow];
        down.sort_by(|a, b| ranking.order(b, a));
        // The machine quickest to bring back is kept; the slow one goes first.
        assert_eq!(down[0].name, "slow");
    }

    #[test]
    fn hedging_wakes_the_next_machine_when_a_boot_is_overdue() {
        let now = Utc::now();
        let spec = spec(serde_json::json!({
            "strategies": [{"name": "FastestBoot", "weight": 1}],
            "hedge": {}
        }));
        let mut stuck = booted(machine("stuck", 8), 60.0, 5, 0);
        stuck.state = MemberState::Booting;
        let spare = machine("spare", 8);
        let at = |booting_for: i64| {
            let mut m = stuck.clone();
            m.history.booting_since = Some(now - Duration::seconds(booting_for));
            plan(&PoolSnapshot {
                spec: &spec,
                members: vec![m, spare.clone()],
                pending: pending(now),
                now,
                last_scale_up: None,
                unneeded_since: BTreeMap::new(),
                external: vec![],
                kept_by_others: BTreeMap::new(),
            })
        };
        // Average 60s: overdue at max(150% of 60s, 120s) = 120s.
        assert!(
            at(100).power_on.is_empty(),
            "within its expected boot, the pod waits for it"
        );
        let p = at(130);
        assert_eq!(p.power_on.len(), 1);
        assert_eq!(p.power_on[0].0, "spare");
        assert!(
            p.power_on[0].1.ends_with("; hedging: stuck overdue"),
            "{}",
            p.power_on[0].1
        );
        assert!(
            p.details
                .iter()
                .any(|d| d.starts_with("stuck: powering on; boot overdue since ")),
            "{:?}",
            p.details
        );

        // Without `hedge`, the pod keeps waiting for it however long it takes.
        let mut no_hedge = spec.clone();
        no_hedge.scale_up.hedge = None;
        let mut m = stuck.clone();
        m.history.booting_since = Some(now - Duration::seconds(3600));
        let p = plan(&PoolSnapshot {
            spec: &no_hedge,
            members: vec![m, spare.clone()],
            pending: pending(now),
            now,
            last_scale_up: None,
            unneeded_since: BTreeMap::new(),
            external: vec![],
            kept_by_others: BTreeMap::new(),
        });
        assert!(p.power_on.is_empty());
    }

    #[test]
    fn active_hours_remaining_skips_a_machine_that_would_close_before_it_is_ready() {
        // Your order: lagopus, then velox, then zerda, by a dominant Priority.
        let spec = spec(serde_json::json!({
            "preferredNodes": [{"name": "velox", "weight": 50}, {"name": "zerda", "weight": 10}],
            "strategies": [{"name": "Priority", "weight": 100}, {"name": "ActiveHoursRemaining", "weight": 1}]
        }));
        let with_left = |name: &str, left: Option<i64>, boot: Option<f64>| {
            let mut m = machine(name, 8);
            m.history.active_seconds_left = left;
            m.history.boot_average_seconds = boot;
            m
        };
        // Mid-day: Priority decides.
        let p = woken(
            &spec,
            vec![
                with_left("zerda", Some(8 * 3600), Some(200.0)),
                with_left("velox", Some(8 * 3600), Some(260.0)),
            ],
        );
        assert_eq!(p.power_on[0].0, "velox");

        // 19:55: velox has 5 minutes and boots in 262s, so 38s to work; still
        // ahead, as zerda (same hours) is no better off.
        let p = woken(
            &spec,
            vec![
                with_left("zerda", Some(300), Some(200.0)),
                with_left("velox", Some(300), Some(262.0)),
            ],
        );
        assert_eq!(p.power_on[0].0, "velox");

        // 19:58: velox would close before it is Ready. A machine without hours
        // is woken instead, whatever its priority.
        let p = woken(
            &spec,
            vec![
                with_left("velox", Some(120), Some(262.0)),
                with_left("anytime", None, None),
            ],
        );
        assert_eq!(p.power_on[0].0, "anytime");
        assert!(
            p.power_on[0].1.contains(
                "over velox by ActiveHoursRemaining (velox's active hours end in 120s, before it would be Ready)"
            ),
            "{}",
            p.power_on[0].1
        );
        // And when it is the only machine that fits, it is still woken: the
        // job may run past the end of its hours (work is never interrupted).
        let p = woken(&spec, vec![with_left("velox", Some(120), Some(262.0))]);
        assert_eq!(p.power_on[0].0, "velox");

        // The rating: an hour or more after booting is 1, none is 0, no hours is 1.
        let ranking = Ranking::new(&spec, []);
        let rate = |left, boot| ranking.rate(Strategy::ActiveHoursRemaining, &with_left("m", left, boot));
        assert_eq!(rate(Some(4 * 3600), Some(60.0)), 1.0);
        assert_eq!(
            rate(Some(1800 + 300), None),
            0.5,
            "five minutes expected with no history"
        );
        assert_eq!(rate(Some(100), Some(200.0)), 0.0);
        assert_eq!(rate(None, None), 1.0);
    }

    #[test]
    fn hedge_thresholds() {
        let h = crate::crd::HedgeSpec::default();
        assert_eq!(h.overdue_after(Some(60.0)), 120, "never before minSeconds");
        assert_eq!(h.overdue_after(Some(200.0)), 300, "150% of the average");
        assert_eq!(h.overdue_after(None), 300, "withoutHistorySeconds");
    }
}
