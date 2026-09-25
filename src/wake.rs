//! Wake-on-LAN magic packets, shared by the `wakeOnLan` driver (sending from
//! the operator's own node) and the `wake` subcommand (sending from relay pods
//! on other nodes, so machines on other network segments or sites can be woken
//! by their neighbours).

use std::collections::BTreeSet;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::net::UdpSocket;

pub fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let parts: Vec<&str> = s.split([':', '-']).collect();
    if parts.len() != 6 {
        return None;
    }
    let mut mac = [0u8; 6];
    for (i, p) in parts.iter().enumerate() {
        if p.len() != 2 {
            return None;
        }
        mac[i] = u8::from_str_radix(p, 16).ok()?;
    }
    Some(mac)
}

pub fn magic_packet(mac: &[u8; 6]) -> Vec<u8> {
    let mut packet = vec![0xFF; 6];
    for _ in 0..16 {
        packet.extend_from_slice(mac);
    }
    packet
}

/// Broadcast addresses of this host's interfaces that are up, IPv4, not
/// loopback and broadcast-capable, plus the limited broadcast 255.255.255.255.
pub fn interface_broadcasts() -> Vec<Ipv4Addr> {
    let mut out: BTreeSet<Ipv4Addr> = BTreeSet::new();
    if let Ok(ifaces) = if_addrs::get_if_addrs() {
        for iface in ifaces {
            if iface.is_loopback() || !iface.is_oper_up() {
                continue;
            }
            if let if_addrs::IfAddr::V4(v4) = iface.addr
                && let Some(b) = v4.broadcast
            {
                out.insert(b);
            }
        }
    }
    out.insert(Ipv4Addr::BROADCAST);
    out.into_iter().collect()
}

/// Where to send: the explicit broadcast address if configured, otherwise
/// every interface's broadcast address.
pub fn targets(explicit: Option<&str>, port: u16) -> io::Result<Vec<SocketAddr>> {
    match explicit {
        Some(addr) => {
            let ip: Ipv4Addr = addr.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("invalid broadcast address {addr}"))
            })?;
            Ok(vec![SocketAddr::from((ip, port))])
        }
        None => Ok(interface_broadcasts()
            .into_iter()
            .map(|ip| SocketAddr::from((ip, port)))
            .collect()),
    }
}

/// Sends the magic packet for `mac` to every target, `repeats` times. Returns
/// the number of successful sends; fails only if none succeeded.
pub async fn send(mac: &[u8; 6], targets: &[SocketAddr], repeats: u32) -> io::Result<usize> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.set_broadcast(true)?;
    let packet = magic_packet(mac);
    let mut ok = 0;
    let mut last_err = None;
    for _ in 0..repeats.max(1) {
        for target in targets {
            match socket.send_to(&packet, target).await {
                Ok(_) => ok += 1,
                Err(e) => {
                    tracing::debug!(%target, error = %e, "magic packet send failed");
                    last_err = Some(e);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if ok == 0 {
        return Err(last_err.unwrap_or_else(|| io::Error::other("no broadcast targets")));
    }
    Ok(ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_macs() {
        assert_eq!(
            parse_mac("aa:bb:cc:dd:ee:ff"),
            Some([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff])
        );
        assert_eq!(parse_mac("00-11-22-33-44-55"), Some([0, 0x11, 0x22, 0x33, 0x44, 0x55]));
        assert_eq!(parse_mac("00:11:22:33:44"), None);
        assert_eq!(parse_mac("zz:11:22:33:44:55"), None);
    }

    #[test]
    fn builds_magic_packet() {
        let p = magic_packet(&[1, 2, 3, 4, 5, 6]);
        assert_eq!(p.len(), 102);
        assert_eq!(&p[..6], &[0xFF; 6]);
        assert_eq!(&p[6..12], &[1, 2, 3, 4, 5, 6]);
        assert_eq!(&p[96..], &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn automatic_targets_always_include_limited_broadcast() {
        let t = targets(None, 9).unwrap();
        assert!(t.contains(&SocketAddr::from((Ipv4Addr::BROADCAST, 9))));
        assert!(t.iter().all(|a| a.port() == 9));
        assert_eq!(
            targets(Some("192.168.1.255"), 7).unwrap(),
            vec![SocketAddr::from(([192, 168, 1, 255], 7))]
        );
        assert!(targets(Some("nope"), 9).is_err());
    }

    #[tokio::test]
    async fn sends_to_a_local_listener() {
        let listener = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mac = [0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0x07];
        assert_eq!(send(&mac, &[addr], 2).await.unwrap(), 2);
        let mut buf = [0u8; 200];
        let (n, _) = listener.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], magic_packet(&mac).as_slice());
    }
}
