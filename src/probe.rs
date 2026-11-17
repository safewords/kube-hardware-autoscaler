//! Reachability of a machine's management path (`spec.probe`): whether its
//! BMC or KVM answers, and how quickly. Recorded in `status.reachability`
//! and weighed by pools with `scaleUp.strategy: Scored`, so a machine whose
//! management interface is gone is not the one woken while another would do.

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crate::crd::{NodePowerManagementConfigSpec, PROBE_HISTORY_LEN, ProbeMethod, Reachability};
use crate::drivers::ping::{icmp_echo, tcp_verdict};

/// How often a machine is probed, at most.
pub const PROBE_INTERVAL_SECS: i64 = 55;

const DEFAULT_TIMEOUT_MS: u64 = 1000;

/// What to probe, and how.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeTarget {
    pub host: String,
    pub method: ProbeMethod,
    pub port: u16,
    pub timeout: Duration,
}

impl std::fmt::Display for ProbeTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.method {
            ProbeMethod::Tcp if self.host.contains(':') => write!(f, "tcp [{}]:{}", self.host, self.port),
            ProbeMethod::Tcp => write!(f, "tcp {}:{}", self.host, self.port),
            ProbeMethod::Icmp => write!(f, "icmp {}", self.host),
        }
    }
}

/// Host and port of an endpoint URL (`https://bmc:8443/...`, `bmc`, `[::1]:443`).
fn split_endpoint(endpoint: &str) -> Option<(String, Option<u16>, Option<u16>)> {
    let (scheme_port, rest) = match endpoint.split_once("://") {
        Some((scheme, rest)) => (
            match scheme.to_ascii_lowercase().as_str() {
                "https" | "wss" => Some(443),
                "http" | "ws" => Some(80),
                _ => None,
            },
            rest,
        ),
        None => (None, endpoint),
    };
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit_once('@').map_or(authority, |(_, a)| a);
    let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
        let (host, after) = v6.split_once(']')?;
        (host.to_string(), after.strip_prefix(':').and_then(|p| p.parse().ok()))
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => (h.to_string(), p.parse().ok()),
            _ => (authority.to_string(), None),
        }
    };
    (!host.is_empty()).then_some((host, port, scheme_port))
}

/// The management address of the first interface that has one: a `redfish`,
/// `pikvm` or `nanokvm` endpoint, or an `ipmi` address. (method, host, port)
fn interface_address(spec: &NodePowerManagementConfigSpec) -> Option<(ProbeMethod, String, Option<u16>)> {
    spec.power_interfaces.iter().find_map(|i| {
        let field = |name: &str| i.config.get(name).and_then(|v| v.as_str()).filter(|s| !s.is_empty());
        match i.driver.as_str() {
            "redfish" | "pikvm" | "nanokvm" => {
                let (host, port, scheme_port) = split_endpoint(field("endpoint")?)?;
                Some((ProbeMethod::Tcp, host, port.or(scheme_port).or(Some(443))))
            }
            // IPMI is UDP: a TCP connect proves nothing, an echo does.
            "ipmi" => Some((ProbeMethod::Icmp, field("address")?.to_string(), None)),
            _ => None,
        }
    })
}

/// What `spec.probe` asks to probe on this machine: `None` when it is not
/// probed (disabled, or nothing to probe), `Err` when it is misconfigured.
pub fn target(spec: &NodePowerManagementConfigSpec) -> Option<Result<ProbeTarget, String>> {
    let probe = spec.probe.clone().unwrap_or_default();
    if probe.disabled {
        return None;
    }
    let derived = interface_address(spec);
    let (method, host, port) = match (&probe.address, derived) {
        (Some(a), _) if a.is_empty() => return Some(Err("probe.address must not be empty".into())),
        (Some(a), derived) => {
            let (host, port, scheme_port) = match split_endpoint(a) {
                Some(parts) => parts,
                None => return Some(Err(format!("cannot read a host from probe.address {a:?}"))),
            };
            // An explicit address keeps a derived port only when it is the same host.
            let derived_port = derived.as_ref().filter(|(_, h, _)| *h == host).and_then(|(_, _, p)| *p);
            (ProbeMethod::Tcp, host, port.or(scheme_port).or(derived_port))
        }
        (None, Some(d)) => d,
        (None, None) if spec.probe.is_some() => {
            return Some(Err(
                "probe has no address and no interface has one to probe; set probe.address".into(),
            ));
        }
        (None, None) => return None,
    };
    let method = probe.method.unwrap_or(method);
    let port = match (method, probe.port.or(port)) {
        (ProbeMethod::Tcp, None) => {
            return Some(Err(format!(
                "no port to probe {host} on; set probe.port or use method icmp"
            )));
        }
        (_, p) => p.unwrap_or(0),
    };
    let timeout = Duration::from_millis(probe.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).max(1));
    Some(Ok(ProbeTarget {
        host,
        method,
        port,
        timeout,
    }))
}

/// One probe's outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Answered(Duration),
    Silent,
    /// The probe could not run, or says nothing about the host.
    Failed(String),
}

/// Probes once.
pub async fn probe(target: &ProbeTarget) -> Outcome {
    let addr = match tokio::time::timeout(
        target.timeout,
        tokio::net::lookup_host((target.host.as_str(), target.port)),
    )
    .await
    {
        Ok(Ok(mut addrs)) => match target.method {
            // ICMP probing is IPv4-only.
            ProbeMethod::Icmp => match addrs.find(SocketAddr::is_ipv4) {
                Some(a) => a,
                None => return Outcome::Failed(format!("{} has no IPv4 address for icmp", target.host)),
            },
            ProbeMethod::Tcp => match addrs.next() {
                Some(a) => a,
                None => return Outcome::Failed(format!("cannot resolve {}", target.host)),
            },
        },
        Ok(Err(e)) => return Outcome::Failed(format!("cannot resolve {}: {e}", target.host)),
        Err(_) => return Outcome::Failed(format!("resolving {} timed out", target.host)),
    };
    let started = Instant::now();
    let answered = match target.method {
        ProbeMethod::Tcp => {
            let result = match tokio::time::timeout(target.timeout, tokio::net::TcpStream::connect(addr)).await {
                Ok(r) => r.map(|_| ()),
                Err(_) => Err(io::ErrorKind::TimedOut.into()),
            };
            match tcp_verdict(result) {
                Some(up) => up,
                None => return Outcome::Failed(format!("cannot connect to {addr}")),
            }
        }
        ProbeMethod::Icmp => {
            let IpAddr::V4(v4) = addr.ip() else {
                return Outcome::Failed("icmp probing supports IPv4 only".into());
            };
            let timeout = target.timeout;
            match tokio::task::spawn_blocking(move || icmp_echo(v4, 1, timeout)).await {
                Ok(Ok(up)) => up,
                Ok(Err(e)) => {
                    return Outcome::Failed(format!(
                        "icmp socket unavailable ({e}); allow unprivileged ping or use method tcp"
                    ));
                }
                Err(e) => return Outcome::Failed(format!("icmp worker failed: {e}")),
            }
        }
    };
    if answered {
        Outcome::Answered(started.elapsed())
    } else {
        Outcome::Silent
    }
}

/// Whether the machine is due for a probe.
pub fn due(last: Option<&Reachability>, target: &ProbeTarget, now: DateTime<Utc>) -> bool {
    last.is_none_or(|r| r.target != target.to_string() || (now - r.checked_at).num_seconds() >= PROBE_INTERVAL_SECS)
}

/// The new `status.reachability` after a probe. A probe that could not run
/// keeps the previous answer and only records the error.
pub fn record(
    last: Option<&Reachability>,
    target: &ProbeTarget,
    outcome: &Outcome,
    now: DateTime<Utc>,
) -> Reachability {
    let label = target.to_string();
    // A new target starts a new history.
    let last = last.filter(|r| r.target == label);
    let mut recent = last.map(|r| r.recent.clone()).unwrap_or_default();
    let (reachable, rtt_ms, error) = match outcome {
        Outcome::Answered(rtt) => (true, Some(rtt.as_millis() as u64), None),
        Outcome::Silent => (false, last.and_then(|r| r.rtt_ms), None),
        Outcome::Failed(e) => (
            last.is_some_and(|r| r.reachable),
            last.and_then(|r| r.rtt_ms),
            Some(e.clone()),
        ),
    };
    match outcome {
        Outcome::Answered(_) => recent.push('1'),
        Outcome::Silent => recent.push('0'),
        Outcome::Failed(_) => {}
    }
    let excess = recent.len().saturating_sub(PROBE_HISTORY_LEN);
    recent.drain(..excess);
    Reachability {
        target: label,
        reachable,
        rtt_ms,
        recent,
        checked_at: now,
        error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{PowerInterface, ProbeSpec};

    fn spec(interfaces: &[(&str, serde_json::Value)], probe: Option<ProbeSpec>) -> NodePowerManagementConfigSpec {
        NodePowerManagementConfigSpec {
            node_name: "n".into(),
            power_policy: Default::default(),
            power_interfaces: interfaces
                .iter()
                .map(|(driver, config)| PowerInterface {
                    name: None,
                    driver: driver.to_string(),
                    actions: None,
                    timeout_seconds: None,
                    credentials_secret_ref: None,
                    config: config.clone(),
                })
                .collect(),
            lifecycle: Default::default(),
            manual_power_on_policy: None,
            manual_power_off_cooldown_seconds: None,
            active_hours: None,
            probe,
        }
    }

    fn target_of(s: &NodePowerManagementConfigSpec) -> String {
        target(s).expect("probed").expect("valid").to_string()
    }

    #[test]
    fn derives_the_management_address() {
        let redfish = spec(
            &[("redfish", serde_json::json!({"endpoint": "https://bmc.lan/redfish/v1"}))],
            None,
        );
        assert_eq!(target_of(&redfish), "tcp bmc.lan:443");
        let port = spec(&[("pikvm", serde_json::json!({"endpoint": "http://kvm:8080"}))], None);
        assert_eq!(target_of(&port), "tcp kvm:8080");
        let ipmi = spec(&[("ipmi", serde_json::json!({"address": "10.0.0.9"}))], None);
        assert_eq!(target_of(&ipmi), "icmp 10.0.0.9");
        // The first interface with an address wins; Wake-on-LAN has none.
        let mixed = spec(
            &[
                ("wakeOnLan", serde_json::json!({"macAddress": "aa:bb:cc:dd:ee:ff"})),
                ("redfish", serde_json::json!({"endpoint": "https://[fd00::9]:8443"})),
            ],
            None,
        );
        assert_eq!(target_of(&mixed), "tcp [fd00::9]:8443");
    }

    #[test]
    fn a_wake_on_lan_machine_is_not_probed_unless_told_where() {
        let wol = spec(
            &[("wakeOnLan", serde_json::json!({"macAddress": "aa:bb:cc:dd:ee:ff"}))],
            None,
        );
        assert!(target(&wol).is_none());
        let told = spec(
            &[("wakeOnLan", serde_json::json!({}))],
            Some(ProbeSpec {
                address: Some("10.0.0.7".into()),
                method: Some(ProbeMethod::Icmp),
                ..Default::default()
            }),
        );
        assert_eq!(target_of(&told), "icmp 10.0.0.7");
        let no_port = spec(
            &[("wakeOnLan", serde_json::json!({}))],
            Some(ProbeSpec {
                address: Some("10.0.0.7".into()),
                ..Default::default()
            }),
        );
        assert!(target(&no_port).unwrap().is_err(), "tcp needs a port");
        let nothing = spec(&[("wakeOnLan", serde_json::json!({}))], Some(ProbeSpec::default()));
        assert!(
            target(&nothing).unwrap().is_err(),
            "an explicit probe with nothing to probe is an error"
        );
        let off = spec(
            &[("redfish", serde_json::json!({"endpoint": "https://bmc"}))],
            Some(ProbeSpec {
                disabled: true,
                ..Default::default()
            }),
        );
        assert!(target(&off).is_none());
    }

    #[test]
    fn records_answers_and_keeps_the_last_ten() {
        let t = ProbeTarget {
            host: "bmc".into(),
            method: ProbeMethod::Tcp,
            port: 443,
            timeout: Duration::from_secs(1),
        };
        let now = Utc::now();
        let mut r = record(None, &t, &Outcome::Answered(Duration::from_millis(4)), now);
        assert_eq!((r.reachable, r.rtt_ms, r.recent.as_str()), (true, Some(4), "1"));
        r = record(Some(&r), &t, &Outcome::Silent, now);
        assert_eq!((r.reachable, r.rtt_ms, r.recent.as_str()), (false, Some(4), "10"));
        // A probe that could not run records the error and nothing else.
        r = record(Some(&r), &t, &Outcome::Failed("no route".into()), now);
        assert_eq!(
            (r.reachable, r.recent.as_str(), r.error.as_deref()),
            (false, "10", Some("no route"))
        );
        for _ in 0..12 {
            r = record(Some(&r), &t, &Outcome::Answered(Duration::from_millis(2)), now);
        }
        assert_eq!(r.recent, "1111111111");
        assert_eq!(r.answered(), 1.0);
        assert!(!due(Some(&r), &t, now), "probed just now");
        assert!(due(Some(&r), &t, now + chrono::Duration::seconds(PROBE_INTERVAL_SECS)));
        // A different target starts over.
        let other = ProbeTarget {
            port: 8443,
            ..t.clone()
        };
        assert!(due(Some(&r), &other, now));
        assert_eq!(record(Some(&r), &other, &Outcome::Silent, now).recent, "0");
    }

    #[tokio::test]
    async fn a_listening_port_answers_and_a_silent_one_does_not() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let open = ProbeTarget {
            host: "127.0.0.1".into(),
            method: ProbeMethod::Tcp,
            port,
            timeout: Duration::from_millis(500),
        };
        assert!(matches!(probe(&open).await, Outcome::Answered(_)));
        // TEST-NET-1 is never routed: the connect times out.
        let silent = ProbeTarget {
            host: "192.0.2.1".into(),
            port: 9,
            timeout: Duration::from_millis(200),
            ..open
        };
        assert!(!matches!(probe(&silent).await, Outcome::Answered(_)));
    }
}
