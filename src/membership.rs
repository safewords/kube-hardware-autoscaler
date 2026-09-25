//! Pool membership: which `NodeScalingPool` (if any) a machine belongs to, derived
//! from the labels of its Kubernetes Node. Used identically by both
//! controllers so they can never disagree.

use std::collections::BTreeMap;

use kube::ResourceExt;

use crate::crd::NodeScalingPool;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Membership {
    /// The Node object does not exist.
    NoNode,
    /// No pool selects this Node.
    NotInPool,
    /// Exactly one pool selects this Node.
    Member(String),
    /// Several pools select this Node; it belongs to none of them.
    Conflict(Vec<String>),
}

impl Membership {
    pub fn pool(&self) -> Option<&str> {
        match self {
            Membership::Member(p) => Some(p),
            _ => None,
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
    match matching.len() {
        0 => Membership::NotInPool,
        1 => Membership::Member(matching.remove(0)),
        _ => Membership::Conflict(matching),
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
    fn resolves_membership() {
        let gpu = pool("gpu", &[("example.com/gpu", "true")]);
        let big = pool("big", &[("size", "large")]);
        let pools = [gpu, big];
        assert_eq!(resolve(None, &pools), Membership::NoNode);
        assert_eq!(resolve(Some(&labels(&[("x", "y")])), &pools), Membership::NotInPool);
        assert_eq!(
            resolve(Some(&labels(&[("example.com/gpu", "true")])), &pools),
            Membership::Member("gpu".into())
        );
        assert_eq!(
            resolve(Some(&labels(&[("example.com/gpu", "true"), ("size", "large")])), &pools),
            Membership::Conflict(vec!["big".into(), "gpu".into()])
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
