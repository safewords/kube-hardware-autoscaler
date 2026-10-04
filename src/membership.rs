//! Pool membership: which `NodeScalingPool`s a machine belongs to, derived from
//! the labels of its Kubernetes Node. Used identically by both controllers so
//! they can never disagree.
//!
//! Pools may overlap: a machine belongs to every pool whose `nodeSelector`
//! matches its Node. Any of them may power it on; it is powered off only when
//! all of them agree (see `NodeScalingPool.status.releasable`). A machine still
//! has exactly one `NodePowerManagementConfig`, named after its Node.

use std::collections::BTreeMap;

use kube::ResourceExt;

use crate::crd::NodeScalingPool;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Membership {
    /// The Node object does not exist.
    NoNode,
    /// No pool selects this Node.
    NotInPool,
    /// The pools selecting this Node (at least one), sorted by name.
    Member(Vec<String>),
}

impl Membership {
    /// The pools this machine belongs to (empty unless `Member`).
    pub fn pools(&self) -> &[String] {
        match self {
            Membership::Member(p) => p,
            _ => &[],
        }
    }
}

/// Resolves membership for a Node with `labels` (None: the Node is missing).
pub fn resolve<'a>(
    labels: Option<&BTreeMap<String, String>>,
    pools: impl IntoIterator<Item = &'a NodeScalingPool>,
) -> Membership {
    let Some(labels) = labels else {
        return Membership::NoNode;
    };
    let mut matching: Vec<String> = pools
        .into_iter()
        .filter(|p| p.metadata.deletion_timestamp.is_none() && p.spec.node_selector.matches(labels))
        .map(|p| p.name_any())
        .collect();
    matching.sort();
    if matching.is_empty() {
        Membership::NotInPool
    } else {
        Membership::Member(matching)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{LabelOperator, LabelRequirement, NodeScalingPoolSpec, NodeSelector};

    fn pool(name: &str, labels: &[(&str, &str)]) -> NodeScalingPool {
        let spec: NodeScalingPoolSpec = serde_json::from_value(serde_json::json!({
            "nodeSelector": {"matchLabels": labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<BTreeMap<_, _>>()}
        }))
        .unwrap();
        NodeScalingPool::new(name, spec)
    }

    fn labels(l: &[(&str, &str)]) -> BTreeMap<String, String> {
        l.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn resolves_membership_including_overlap() {
        let gpu = pool("gpu", &[("example.com/gpu", "true")]);
        let ci = pool("ci", &[("example.com/ci", "true")]);
        let pools = [gpu, ci];
        assert_eq!(resolve(None, &pools), Membership::NoNode);
        assert_eq!(resolve(Some(&labels(&[("x", "y")])), &pools), Membership::NotInPool);
        assert_eq!(
            resolve(Some(&labels(&[("example.com/gpu", "true")])), &pools),
            Membership::Member(vec!["gpu".into()])
        );
        // Selected by both: a member of both.
        assert_eq!(
            resolve(
                Some(&labels(&[("example.com/gpu", "true"), ("example.com/ci", "true")])),
                &pools
            ),
            Membership::Member(vec!["ci".into(), "gpu".into()])
        );
    }

    #[test]
    fn empty_selector_matches_nothing() {
        let sel = NodeSelector::default();
        assert!(!sel.matches(&labels(&[("a", "b")])));
        assert!(!sel.matches(&BTreeMap::new()));
    }

    #[test]
    fn match_expressions() {
        let sel = NodeSelector {
            match_labels: BTreeMap::new(),
            match_expressions: vec![
                LabelRequirement {
                    key: "gpu".into(),
                    operator: LabelOperator::In,
                    values: vec!["nvidia".into(), "intel".into()],
                },
                LabelRequirement {
                    key: "maintenance".into(),
                    operator: LabelOperator::DoesNotExist,
                    values: vec![],
                },
            ],
        };
        assert!(sel.matches(&labels(&[("gpu", "intel")])));
        assert!(!sel.matches(&labels(&[("gpu", "amd")])));
        assert!(!sel.matches(&labels(&[("gpu", "intel"), ("maintenance", "true")])));
    }
}
