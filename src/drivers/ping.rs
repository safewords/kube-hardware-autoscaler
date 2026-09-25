//! Reachability probe: a status-only driver that reports a machine as On when
//! it answers on the network and Off when it does not.
//!
//! Put it first in `powerInterfaces` (with `actions: [status]`, although the
//! chain skips it for power actions anyway) to get a real power reading for
//! drivers that cannot report one, such as Wake-on-LAN, whose own reading
//! only mirrors the Node's `Ready` condition and lags a shutdown by the kubelet
//! grace period.
//!
//! * `tcp` (default): connect to `port`. An accepted *or refused* connection
//!   proves the host is up; only a timeout or "unreachable" counts as off.
//!   Needs no privileges.
//! * `icmp`: ICMP echo (IPv4). Uses an unprivileged ping socket, which the
//!   kernel allows when `net.ipv4.ping_group_range` includes the operator's
//!   group (the default on many distributions), and falls back to a raw socket
//!   (needs `CAP_NET_RAW`).

use std::io;
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};

use super::{DriverError, DriverInit, DriverKind, PowerDriver, Result};
use crate::crd::{InterfaceAction, PowerState};

/// `config` of the `ping` driver.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PingConfig {
    /// Host name or IP address of the machine (not its BMC).
    pub address: String,
    #[serde(default)]
    pub method: PingMethod,
    /// TCP port for the `tcp` method. Defaults to 22 (SSH).
    #[serde(default = "default_port")]
    pub port: u16,
    /// Timeout per probe, in milliseconds.
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
    /// Probes before declaring the machine off.
    #[serde(default = "default_attempts")]
    pub attempts: u32,
}

fn default_port() -> u16 {
    22
}
fn default_timeout() -> u64 {
    1000
}
fn default_attempts() -> u32 {
    3
}

#[derive(Deserialize, Serialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum PingMethod {
    #[default]
    Tcp,
    Icmp,
}

pub struct PingDriver {
    config: PingConfig,
}

impl DriverKind for PingDriver {
    const NAME: &'static str = "ping";
    const DESCRIPTION: &'static str = "Reachability probe (TCP connect or ICMP echo); status only";
    const REQUIRES_CREDENTIALS: bool = false;
    type Config = PingConfig;

    fn build(config: PingConfig, _init: &DriverInit<'_>) -> Result<Self> {
        if config.address.is_empty() {
            return Err(DriverError::Config("ping address must not be empty".into()));
        }
        if config.attempts == 0 || config.timeout_ms == 0 {
            return Err(DriverError::Config(
                "ping attempts and timeoutMs must be positive".into(),
            ));
        }
        Ok(Self { config })
    }
}

/// Outcome of one TCP connection attempt.
fn tcp_verdict(result: io::Result<()>) -> Option<bool> {
    match result {
        Ok(()) => Some(true),
        // Something answered with a RST: the host is up, the port just closed.
        Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => Some(true),
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::HostUnreachable | io::ErrorKind::NetworkUnreachable
            ) =>
        {
            Some(false)
        }
        // Anything else (e.g. no route configured locally) says nothing about the host.
        Err(_) => None,
    }
}

impl PingDriver {
    async fn resolve(&self) -> Result<SocketAddr> {
        let mut addrs = tokio::net::lookup_host((self.config.address.as_str(), self.config.port)).await?;
        addrs
            .next()
            .ok_or_else(|| DriverError::Interface(format!("cannot resolve {}", self.config.address)))
    }

    async fn tcp_probe(&self, target: SocketAddr) -> Option<bool> {
        let timeout = Duration::from_millis(self.config.timeout_ms);
        let result = match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(target)).await {
            Ok(r) => r.map(|_| ()),
            Err(_) => Err(io::ErrorKind::TimedOut.into()),
        };
        tcp_verdict(result)
    }

    async fn icmp_probe(&self, target: Ipv4Addr, seq: u16) -> Result<bool> {
        let timeout = Duration::from_millis(self.config.timeout_ms);
        tokio::task::spawn_blocking(move || icmp_echo(target, seq, timeout))
            .await
            .map_err(|e| DriverError::Interface(format!("icmp worker failed: {e}")))?
            .map_err(|e| {
                DriverError::Interface(format!(
                    "icmp socket unavailable ({e}); allow unprivileged ping (net.ipv4.ping_group_range) or use method: tcp"
                ))
            })
    }
}

#[async_trait]
impl PowerDriver for PingDriver {
    fn supports(&self, action: InterfaceAction) -> bool {
        action == InterfaceAction::Status
    }

    async fn power_state(&self) -> Result<PowerState> {
        let target = self.resolve().await?;
        let mut inconclusive = 0;
        for attempt in 0..self.config.attempts {
            let up = match self.config.method {
                PingMethod::Tcp => self.tcp_probe(target).await,
                PingMethod::Icmp => match target.ip() {
                    IpAddr::V4(v4) => Some(self.icmp_probe(v4, attempt as u16).await?),
                    IpAddr::V6(_) => {
                        return Err(DriverError::Config(
                            "icmp probing supports IPv4 only; use method: tcp".into(),
                        ));
                    }
                },
            };
            match up {
                Some(true) => return Ok(PowerState::On),
                Some(false) => {}
                None => inconclusive += 1,
            }
        }
        if inconclusive == self.config.attempts {
            // Never a definite answer: let the chain fall through to the next interface.
            return Ok(PowerState::Unknown);
        }
        Ok(PowerState::Off)
    }

    async fn power_on(&self) -> Result<()> {
        Err(DriverError::Interface(
            "the ping driver only reports power state".into(),
        ))
    }

    async fn power_off(&self, _force: bool) -> Result<()> {
        Err(DriverError::Interface(
            "the ping driver only reports power state".into(),
        ))
    }
}

/// RFC 1071 internet checksum.
fn checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = data
        .chunks(2)
        .map(|c| u32::from(u16::from_be_bytes([c[0], *c.get(1).unwrap_or(&0)])))
        .sum();
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn echo_request(id: u16, seq: u16) -> Vec<u8> {
    let mut p = vec![8, 0, 0, 0];
    p.extend(id.to_be_bytes());
    p.extend(seq.to_be_bytes());
    p.extend_from_slice(b"kube-hardware-autoscaler");
    let c = checksum(&p);
    p[2..4].copy_from_slice(&c.to_be_bytes());
    p
}

/// Whether `icmp` (ICMP payload, without IP header) is an echo reply for `seq`.
fn is_reply(icmp: &[u8], seq: u16) -> bool {
    icmp.len() >= 8 && icmp[0] == 0 && u16::from_be_bytes([icmp[6], icmp[7]]) == seq
}

/// Sends one echo request and waits for the matching reply.
fn icmp_echo(target: Ipv4Addr, seq: u16, timeout: Duration) -> io::Result<bool> {
    let (socket, raw) = match Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::ICMPV4)) {
        Ok(s) => (s, false),
        Err(_) => (Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::ICMPV4))?, true),
    };
    let dest: socket2::SockAddr = SocketAddr::from((target, 0)).into();
    // Unprivileged ping sockets rewrite the identifier; raw sockets keep ours.
    socket.send_to(&echo_request(std::process::id() as u16, seq), &dest)?;
    let deadline = Instant::now() + timeout;
    let mut buf = [MaybeUninit::<u8>::uninit(); 1500];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        socket.set_read_timeout(Some(remaining))?;
        match socket.recv_from(&mut buf) {
            Ok((n, from)) => {
                // SAFETY: recv_from initialised the first n bytes.
                let data: Vec<u8> = buf[..n].iter().map(|b| unsafe { b.assume_init() }).collect();
                let icmp = if raw {
                    let ihl = usize::from(data.first().copied().unwrap_or(0) & 0x0f) * 4;
                    data.get(ihl..).unwrap_or(&[])
                } else {
                    &data[..]
                };
                let from_target = from.as_socket_ipv4().is_some_and(|a| *a.ip() == target);
                if from_target && is_reply(icmp, seq) {
                    return Ok(true);
                }
            }
            Err(e) if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => return Ok(false),
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn driver(address: &str, port: u16) -> PingDriver {
        PingDriver {
            config: PingConfig {
                address: address.into(),
                method: PingMethod::Tcp,
                port,
                timeout_ms: 300,
                attempts: 1,
            },
        }
    }

    #[test]
    fn echo_request_has_valid_checksum() {
        let p = echo_request(0x1234, 7);
        assert_eq!(p[0], 8);
        assert_eq!(checksum(&p), 0, "checksum over a packet including its checksum is zero");
        let mut reply = p.clone();
        reply[0] = 0;
        assert!(is_reply(&reply, 7));
        assert!(!is_reply(&reply, 8));
        assert!(!is_reply(&p, 7), "a request is not a reply");
    }

    #[test]
    fn tcp_verdicts() {
        assert_eq!(tcp_verdict(Ok(())), Some(true));
        assert_eq!(tcp_verdict(Err(io::ErrorKind::ConnectionRefused.into())), Some(true));
        assert_eq!(tcp_verdict(Err(io::ErrorKind::TimedOut.into())), Some(false));
        assert_eq!(tcp_verdict(Err(io::ErrorKind::HostUnreachable.into())), Some(false));
        assert_eq!(tcp_verdict(Err(io::ErrorKind::PermissionDenied.into())), None);
    }

    #[tokio::test]
    async fn open_and_closed_ports_both_mean_on() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        assert_eq!(driver("127.0.0.1", port).power_state().await.unwrap(), PowerState::On);
        drop(listener);
        // Now closed: the host still answers with a refusal. (Linux refuses at
        // once; Windows only after ~2 s of SYN retries, hence the long timeout.)
        let mut closed = driver("127.0.0.1", port);
        closed.config.timeout_ms = 5000;
        assert_eq!(closed.power_state().await.unwrap(), PowerState::On);
    }

    #[tokio::test]
    async fn silent_address_means_off() {
        // TEST-NET-1 is never routed; connects time out (or fail as unreachable).
        let state = driver("192.0.2.1", 22).power_state().await.unwrap();
        assert!(state == PowerState::Off || state == PowerState::Unknown, "{state:?}");
    }

    #[test]
    fn is_status_only() {
        let d = driver("127.0.0.1", 22);
        assert!(d.supports(InterfaceAction::Status));
        assert!(!d.supports(InterfaceAction::PowerOn));
        assert!(!d.supports(InterfaceAction::PowerOff));
    }
}
