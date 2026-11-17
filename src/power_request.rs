//! `PowerRequest`: what a request means for its machine at one instant.
//!
//! One pure function, used both by the PowerRequest controller (to write the
//! request's status) and by every pool (to decide whether the request holds
//! its machine on), so the two can never disagree.

use chrono::{DateTime, Duration, Utc};

use crate::active_hours::{Standing, standing};
use crate::crd::{
    NodePowerManagementConfig, Phase, PowerPolicy, PowerRequestPhase, PowerRequestSpec, PowerRequestStatus,
};

/// A request accepted but not Ready within this long has failed.
pub const READY_TIMEOUT_SECS: i64 = 30 * 60;
/// Finished requests are deleted this long after they finish.
pub const FINISHED_TTL_SECS: i64 = 60 * 60;
/// The longest a request may hold a machine, unless the operator is told
/// otherwise (`--max-power-request-seconds`).
pub const DEFAULT_MAX_SECS: u64 = 24 * 60 * 60;

/// What a request does to its machine now.
#[derive(Clone, Debug, PartialEq)]
pub struct Hold {
    /// For the pools' decisions: `power request ns/name until 22:00:00Z`.
    pub label: String,
    /// The request may wake and keep the machine outside its active hours.
    pub override_active_hours: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Eval {
    /// The status the request should have.
    pub status: PowerRequestStatus,
    /// Whether, and how, it holds its machine on.
    pub hold: Option<Hold>,
}

/// `90m`, `2h`, `1h30m`, `45s`.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let bad = || format!("duration {s:?} is not like 90m, 2h or 1h30m");
    let mut total: i64 = 0;
    let mut num = String::new();
    let mut seen_unit = false;
    for c in s.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        let n: i64 = num.parse().map_err(|_| bad())?;
        num.clear();
        total += match c {
            'h' => n * 3600,
            'm' => n * 60,
            's' => n,
            _ => return Err(bad()),
        };
        seen_unit = true;
    }
    if !num.is_empty() || !seen_unit {
        return Err(bad());
    }
    Ok(Duration::seconds(total))
}

fn hhmmss(t: DateTime<Utc>) -> String {
    t.format("%H:%M:%SZ").to_string()
}

/// Evaluates a request `ns/name` for its machine `mn` (None: no such machine).
#[allow(clippy::too_many_arguments)]
pub fn evaluate(
    ns: &str,
    name: &str,
    spec: &PowerRequestSpec,
    status: &PowerRequestStatus,
    created: DateTime<Utc>,
    mn: Option<&NodePowerManagementConfig>,
    now: DateTime<Utc>,
    max_secs: u64,
) -> Eval {
    let mut st = status.clone();
    // Over is over: a finished request never comes back.
    if st.phase.is_terminal() {
        return Eval { status: st, hold: None };
    }
    let finish = |mut st: PowerRequestStatus, phase: PowerRequestPhase, message: String| {
        st.phase = phase;
        st.message = Some(message);
        st.finished_at = Some(now);
        Eval { status: st, hold: None }
    };
    let max = Duration::seconds(max_secs as i64);
    let length = match (&spec.duration, spec.until) {
        (Some(d), None) => match parse_duration(d) {
            Ok(d) if d <= Duration::zero() => return finish(st, PowerRequestPhase::Refused, "duration is zero".into()),
            Ok(d) => d,
            Err(e) => return finish(st, PowerRequestPhase::Refused, e),
        },
        (None, Some(until)) => until - created,
        _ => {
            return finish(
                st,
                PowerRequestPhase::Refused,
                "set exactly one of duration and until".into(),
            );
        }
    };
    if length > max {
        return finish(
            st,
            PowerRequestPhase::Refused,
            format!("longer than the operator allows ({}s)", max.num_seconds()),
        );
    }
    let node = &spec.node_name;
    let Some(mn) = mn else {
        return finish(
            st,
            PowerRequestPhase::Refused,
            format!("no NodePowerManagementConfig named {node}"),
        );
    };
    if mn.spec.power_policy == PowerPolicy::AlwaysOff {
        return finish(
            st,
            PowerRequestPhase::Refused,
            format!("{node} has powerPolicy AlwaysOff"),
        );
    }
    if !spec.override_active_hours
        && let Some(why) = standing(mn.spec.active_hours.as_ref(), now).note()
    {
        // Refused if it never held the machine; ended if it did.
        let (phase, what) = if st.phase == PowerRequestPhase::Pending {
            (PowerRequestPhase::Refused, "refused")
        } else {
            (PowerRequestPhase::Expired, "ended")
        };
        return finish(
            st,
            phase,
            format!("{what}: {node} is {why}; set overrideActiveHours to run it anyway"),
        );
    }

    let mst = mn.status.clone().unwrap_or_default();
    let ready = mst.phase == Phase::On && mst.node_ready;
    if ready && st.ready_at.is_none() {
        st.ready_at = Some(now);
    }
    st.expires_at = match (spec.until, st.ready_at) {
        (Some(until), _) => Some(until),
        (None, Some(r)) => Some(r + length),
        (None, None) => None,
    };
    if let Some(exp) = st.expires_at.filter(|e| now >= *e) {
        return finish(st, PowerRequestPhase::Expired, format!("expired at {}", hhmmss(exp)));
    }
    if st.ready_at.is_none() && now - created >= Duration::seconds(READY_TIMEOUT_SECS) {
        return finish(
            st,
            PowerRequestPhase::Failed,
            format!("{node} was not Ready within {}m", READY_TIMEOUT_SECS / 60),
        );
    }
    let until = match st.expires_at {
        Some(e) => format!("until {}", hhmmss(e)),
        None => format!("for {}s once Ready", length.num_seconds()),
    };
    st.phase = if ready {
        PowerRequestPhase::Active
    } else {
        PowerRequestPhase::PoweringOn
    };
    st.message = Some(match st.phase {
        PowerRequestPhase::Active => format!("{node} is on; kept {until}"),
        _ => format!("waking {node}; kept {until}"),
    });
    let reason = spec.reason.as_deref().map(|r| format!(" ({r})")).unwrap_or_default();
    let mut label = format!("power request {ns}/{name}{reason} {until}");
    if spec.override_active_hours && matches!(standing(mn.spec.active_hours.as_ref(), now), Standing::Outside(_)) {
        label.push_str(", overriding active hours");
    }
    Eval {
        status: st,
        hold: Some(Hold {
            label,
            override_active_hours: spec.override_active_hours,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        ActiveHours, ActiveWindow, NodePowerManagementConfigSpec, NodePowerManagementConfigStatus, PowerInterface,
    };

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn machine(policy: PowerPolicy, ready: bool, hours: Option<(&str, &str)>) -> NodePowerManagementConfig {
        let mut m = NodePowerManagementConfig::new(
            "zerda",
            NodePowerManagementConfigSpec {
                node_name: "zerda".into(),
                power_policy: policy,
                power_interfaces: vec![PowerInterface {
                    name: None,
                    actions: None,
                    timeout_seconds: None,
                    driver: "ipmi".into(),
                    credentials_secret_ref: None,
                    config: serde_json::json!({}),
                }],
                lifecycle: Default::default(),
                manual_power_on_policy: None,
                manual_power_off_cooldown_seconds: None,
                active_hours: hours.map(|(s, e)| ActiveHours {
                    time_zone: Some("UTC".into()),
                    windows: vec![ActiveWindow {
                        days: vec![],
                        start: s.into(),
                        end: e.into(),
                    }],
                }),
                probe: None,
            },
        );
        m.status = Some(NodePowerManagementConfigStatus {
            phase: if ready { Phase::On } else { Phase::Off },
            node_ready: ready,
            ..Default::default()
        });
        m
    }

    fn req(duration: &str) -> PowerRequestSpec {
        PowerRequestSpec {
            node_name: "zerda".into(),
            duration: Some(duration.into()),
            until: None,
            reason: Some("batch".into()),
            override_active_hours: false,
        }
    }

    fn eval(
        spec: &PowerRequestSpec,
        st: &PowerRequestStatus,
        mn: Option<&NodePowerManagementConfig>,
        now: &str,
    ) -> Eval {
        evaluate(
            "app",
            "r",
            spec,
            st,
            at("2026-11-17T12:00:00Z"),
            mn,
            at(now),
            DEFAULT_MAX_SECS,
        )
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90m"), Ok(Duration::minutes(90)));
        assert_eq!(parse_duration("1h30m"), Ok(Duration::minutes(90)));
        assert_eq!(parse_duration("45s"), Ok(Duration::seconds(45)));
        assert!(parse_duration("2").is_err());
        assert!(parse_duration("2d").is_err());
        assert!(parse_duration("").is_err());
    }

    #[test]
    fn wakes_then_holds_for_the_duration_from_ready_then_expires() {
        let spec = req("2h");
        let off = machine(PowerPolicy::Auto, false, None);
        let e = eval(
            &spec,
            &PowerRequestStatus::default(),
            Some(&off),
            "2026-11-17T12:00:05Z",
        );
        assert_eq!(e.status.phase, PowerRequestPhase::PoweringOn);
        assert_eq!(e.status.expires_at, None);
        assert_eq!(
            e.hold.as_ref().unwrap().label,
            "power request app/r (batch) for 7200s once Ready"
        );

        let on = machine(PowerPolicy::Auto, true, None);
        let e = eval(&spec, &e.status, Some(&on), "2026-11-17T12:02:00Z");
        assert_eq!(e.status.phase, PowerRequestPhase::Active);
        assert_eq!(e.status.ready_at, Some(at("2026-11-17T12:02:00Z")));
        assert_eq!(e.status.expires_at, Some(at("2026-11-17T14:02:00Z")));
        assert_eq!(e.hold.unwrap().label, "power request app/r (batch) until 14:02:00Z");

        // ready_at is kept, not moved forward.
        let later = eval(&spec, &e.status, Some(&on), "2026-11-17T13:00:00Z");
        assert_eq!(later.status.expires_at, Some(at("2026-11-17T14:02:00Z")));

        let done = eval(&spec, &e.status, Some(&on), "2026-11-17T14:02:00Z");
        assert_eq!(done.status.phase, PowerRequestPhase::Expired);
        assert!(done.hold.is_none());
        // Terminal stays terminal.
        let again = eval(&spec, &done.status, Some(&on), "2026-11-17T14:03:00Z");
        assert_eq!(again.status, done.status);
    }

    #[test]
    fn until_is_absolute() {
        let spec = PowerRequestSpec {
            duration: None,
            until: Some(at("2026-11-17T13:00:00Z")),
            ..req("1h")
        };
        let off = machine(PowerPolicy::Auto, false, None);
        let e = eval(
            &spec,
            &PowerRequestStatus::default(),
            Some(&off),
            "2026-11-17T12:00:05Z",
        );
        assert_eq!(e.status.expires_at, Some(at("2026-11-17T13:00:00Z")));
        assert!(e.hold.is_some());
    }

    #[test]
    fn refusals() {
        let st = PowerRequestStatus::default();
        let now = "2026-11-17T12:00:05Z";
        let e = eval(&req("2h"), &st, None, now);
        assert_eq!(e.status.phase, PowerRequestPhase::Refused);
        let off = machine(PowerPolicy::AlwaysOff, false, None);
        assert_eq!(
            eval(&req("2h"), &st, Some(&off), now).status.phase,
            PowerRequestPhase::Refused
        );
        let auto = machine(PowerPolicy::Auto, false, None);
        assert_eq!(
            eval(&req("25h"), &st, Some(&auto), now).status.phase,
            PowerRequestPhase::Refused
        );
        assert_eq!(
            eval(&req("0m"), &st, Some(&auto), now).status.phase,
            PowerRequestPhase::Refused
        );
    }

    #[test]
    fn active_hours_refuse_unless_overridden() {
        let st = PowerRequestStatus::default();
        // Created at 12:00; its hours start at 14:00.
        let quiet = machine(PowerPolicy::Auto, false, Some(("14:00", "20:00")));
        let night = "2026-11-17T12:00:05Z";
        let e = eval(&req("2h"), &st, Some(&quiet), night);
        assert_eq!(e.status.phase, PowerRequestPhase::Refused);
        assert!(e.status.message.unwrap().contains("set overrideActiveHours"));

        let spec = PowerRequestSpec {
            override_active_hours: true,
            ..req("2h")
        };
        let e = eval(&spec, &st, Some(&quiet), night);
        assert_eq!(e.status.phase, PowerRequestPhase::PoweringOn);
        let hold = e.hold.unwrap();
        assert!(hold.override_active_hours);
        assert!(hold.label.ends_with(", overriding active hours"), "{}", hold.label);
    }

    #[test]
    fn an_active_request_ends_with_the_active_hours() {
        let spec = req("4h");
        let on = machine(PowerPolicy::Auto, true, Some(("08:00", "20:00")));
        let e = eval(&spec, &PowerRequestStatus::default(), Some(&on), "2026-11-17T19:00:00Z");
        assert_eq!(e.status.phase, PowerRequestPhase::Active);
        let e = eval(&spec, &e.status, Some(&on), "2026-11-17T20:00:00Z");
        assert_eq!(e.status.phase, PowerRequestPhase::Expired);
        assert!(e.status.message.unwrap().starts_with("ended: "));
    }

    #[test]
    fn fails_when_never_ready() {
        let off = machine(PowerPolicy::Auto, false, None);
        let e = eval(
            &req("2h"),
            &PowerRequestStatus::default(),
            Some(&off),
            "2026-11-17T12:00:05Z",
        );
        let e = eval(&req("2h"), &e.status, Some(&off), "2026-11-17T12:30:00Z");
        assert_eq!(e.status.phase, PowerRequestPhase::Failed);
        assert!(e.hold.is_none());
    }
}
