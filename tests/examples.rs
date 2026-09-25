//! Every NodePowerManagementConfig / NodeScalingPool in `examples/` must deserialize into the CRD
//! types and every power interface config must pass its driver's validation.

use std::path::Path;

use kube_hardware_autoscaler::crd::{NodePowerManagementConfigSpec, NodeScalingPoolSpec};
use kube_hardware_autoscaler::drivers::lookup;
use serde::Deserialize;

fn yaml_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            yaml_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "yaml" || e == "yml") {
            out.push(path);
        }
    }
}

#[test]
fn examples_match_the_crds_and_driver_schemas() {
    let mut files = Vec::new();
    yaml_files(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/examples")), &mut files);
    let (mut nodes, mut pools) = (0, 0);
    for file in &files {
        let text = std::fs::read_to_string(file).unwrap();
        for doc in serde_yaml::Deserializer::from_str(&text) {
            let v = serde_yaml::Value::deserialize(doc).unwrap();
            let ctx = |what: &str| format!("{}: {what}", file.display());
            match v.get("kind").and_then(|k| k.as_str()) {
                Some("NodePowerManagementConfig") => {
                    let spec: NodePowerManagementConfigSpec =
                        serde_yaml::from_value(v["spec"].clone()).unwrap_or_else(|e| panic!("{}", ctx(&e.to_string())));
                    assert!(!spec.power_interfaces.is_empty(), "{}", ctx("no powerInterfaces"));
                    assert_eq!(
                        v["metadata"]["name"].as_str(),
                        Some(spec.node_name.as_str()),
                        "{}",
                        ctx("metadata.name must equal spec.nodeName")
                    );
                    for pi in &spec.power_interfaces {
                        let driver = lookup(&pi.driver).unwrap_or_else(|e| panic!("{}", ctx(&e.to_string())));
                        driver
                            .validate(&pi.config)
                            .unwrap_or_else(|e| panic!("{}", ctx(&e.to_string())));
                        if driver.requires_credentials() {
                            assert!(
                                pi.credentials_secret_ref.is_some(),
                                "{}",
                                ctx("missing credentialsSecretRef")
                            );
                        }
                    }
                    nodes += 1;
                }
                Some("NodeScalingPool") => {
                    let spec: NodeScalingPoolSpec =
                        serde_yaml::from_value(v["spec"].clone()).unwrap_or_else(|e| panic!("{}", ctx(&e.to_string())));
                    assert!(
                        !spec.node_selector.is_empty(),
                        "{}",
                        ctx("NodeScalingPool without nodeSelector")
                    );
                    pools += 1;
                }
                _ => {}
            }
        }
    }
    assert!(
        nodes >= 6 && pools >= 1,
        "found only {nodes} NodePowerManagementConfigs and {pools} NodeScalingPools"
    );
}
