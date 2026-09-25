//! Machine identity: checks that a management interface controls the same
//! physical machine as the Kubernetes Node, by comparing the system UUID the
//! interface reports with `Node.status.nodeInfo.systemUUID` (SMBIOS UUID, as
//! read by the kubelet).

/// Identity reported by a management interface.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemId {
    /// A textual UUID, e.g. Redfish `ComputerSystem.UUID`.
    Uuid(String),
    /// The 16 raw bytes of IPMI "Get System GUID". BMCs disagree on byte
    /// order, so every common interpretation is tried.
    RawGuid([u8; 16]),
}

impl std::fmt::Display for SystemId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemId::Uuid(s) => f.write_str(s),
            SystemId::RawGuid(b) => f.write_str(&hex(b)),
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Lower-case hex digits only; None unless it is a 128-bit value.
fn normalize(uuid: &str) -> Option<String> {
    let h: String = uuid
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .collect::<String>()
        .to_ascii_lowercase();
    (h.len() == 32).then_some(h)
}

/// Swaps the first three UUID fields between big- and little-endian (the
/// SMBIOS / Microsoft GUID encoding).
fn swap_fields(b: &[u8; 16]) -> [u8; 16] {
    let mut o = *b;
    o[0..4].reverse();
    o[4..6].reverse();
    o[6..8].reverse();
    o
}

/// All-zero or all-FF UUIDs mean "not set" on many boards and prove nothing.
fn is_placeholder(h: &str) -> bool {
    h.chars().all(|c| c == '0') || h.chars().all(|c| c == 'f')
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityCheck {
    /// The interface reports the Node's UUID.
    Match,
    /// The interface reports a different machine.
    Mismatch { reported: String, expected: String },
    /// No usable identity on one side (driver cannot report one, or the
    /// Node/BMC UUID is missing or a placeholder).
    Unverifiable,
}

pub fn check(reported: Option<&SystemId>, node_uuid: Option<&str>) -> IdentityCheck {
    let (Some(reported), Some(expected)) = (reported, node_uuid.and_then(normalize)) else {
        return IdentityCheck::Unverifiable;
    };
    if is_placeholder(&expected) {
        return IdentityCheck::Unverifiable;
    }
    let candidates: Vec<String> = match reported {
        SystemId::Uuid(s) => match normalize(s) {
            Some(h) => vec![h],
            None => return IdentityCheck::Unverifiable,
        },
        SystemId::RawGuid(b) => {
            let mut rev = *b;
            rev.reverse();
            // Canonical = inverse of each encoding BMCs use: as-is, fully
            // reversed, SMBIOS mixed-endian, and both compositions.
            let mut rev_of_swap = swap_fields(b);
            rev_of_swap.reverse();
            vec![
                hex(b),
                hex(&rev),
                hex(&swap_fields(b)),
                hex(&swap_fields(&rev)),
                hex(&rev_of_swap),
            ]
        }
    };
    if candidates.iter().all(|c| is_placeholder(c)) {
        return IdentityCheck::Unverifiable;
    }
    if candidates.contains(&expected) {
        IdentityCheck::Match
    } else {
        IdentityCheck::Mismatch {
            reported: reported.to_string(),
            expected,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NODE: &str = "4C4C4544-0036-4A10-8058-B4C04F4E4632"; // Dell-style SMBIOS UUID

    #[test]
    fn textual_uuid_matches_case_and_dash_insensitively() {
        let r = SystemId::Uuid("4c4c4544-0036-4a10-8058-b4c04f4e4632".into());
        assert_eq!(check(Some(&r), Some(NODE)), IdentityCheck::Match);
        let other = SystemId::Uuid("4c4c4544-0036-4a10-8058-b4c04f4e4633".into());
        assert!(matches!(
            check(Some(&other), Some(NODE)),
            IdentityCheck::Mismatch { .. }
        ));
    }

    #[test]
    fn raw_ipmi_guid_matches_in_any_common_byte_order() {
        let be: [u8; 16] = [
            0x4c, 0x4c, 0x45, 0x44, 0x00, 0x36, 0x4a, 0x10, 0x80, 0x58, 0xb4, 0xc0, 0x4f, 0x4e, 0x46, 0x32,
        ];
        let mut reversed = be;
        reversed.reverse();
        for raw in [be, reversed, swap_fields(&be), swap_fields(&reversed)] {
            assert_eq!(
                check(Some(&SystemId::RawGuid(raw)), Some(NODE)),
                IdentityCheck::Match,
                "{raw:02x?}"
            );
        }
        let mut wrong = be;
        wrong[15] ^= 1;
        assert!(matches!(
            check(Some(&SystemId::RawGuid(wrong)), Some(NODE)),
            IdentityCheck::Mismatch { .. }
        ));
    }

    #[test]
    fn missing_or_placeholder_ids_are_unverifiable() {
        let r = SystemId::Uuid(NODE.into());
        assert_eq!(check(None, Some(NODE)), IdentityCheck::Unverifiable);
        assert_eq!(check(Some(&r), None), IdentityCheck::Unverifiable);
        assert_eq!(
            check(Some(&r), Some("00000000-0000-0000-0000-000000000000")),
            IdentityCheck::Unverifiable
        );
        assert_eq!(
            check(Some(&SystemId::RawGuid([0; 16])), Some(NODE)),
            IdentityCheck::Unverifiable
        );
        assert_eq!(
            check(Some(&SystemId::Uuid("not-a-uuid".into())), Some(NODE)),
            IdentityCheck::Unverifiable
        );
    }
}
