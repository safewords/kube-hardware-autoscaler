//! Validating admission webhook for `NodeScalingPool`.
//!
//! Refuses a pool whose `scaleUp.preferredNodes` names a Node that does not
//! exist, and admits with a warning one that names a Node the pool's
//! `nodeSelector` does not select (and one that still uses the deprecated
//! `scaleDown.delayAfterScaleUpSeconds`). Duplicate names and the weight range
//! are the CRD schema's job.
//!
//! The operator runs the webhook end to end, with nothing else installed:
//! * TLS: a self-signed CA and a serving certificate for the webhook Service,
//!   generated once and kept in a Secret in the operator's namespace.
//! * Registration: the `ValidatingWebhookConfiguration` is applied only once
//!   the server is listening, so the first install never waits on a webhook
//!   that is not there yet, and deleted again on a graceful shutdown, so an
//!   operator rollout does not refuse pool changes while no pod serves. It is
//!   owned by the operator's ClusterRole, so uninstalling the chart removes it.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context as _;
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine as _;
use chrono::{Datelike, Utc};
use k8s_openapi::ByteString;
use k8s_openapi::api::admissionregistration::v1::ValidatingWebhookConfiguration;
use k8s_openapi::api::core::v1::{Node, Secret};
use k8s_openapi::api::rbac::v1::ClusterRole;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::core::DynamicObject;
use kube::core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview, Operation};
use kube::runtime::reflector::{ObjectRef, Store};
use kube::{Api, Client, ResourceExt};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::json;
use tracing::{info, warn};

use crate::controller::FIELD_MANAGER;
use crate::controller::node_scaling_pool::preferred_node_problems;
use crate::crd::{API_GROUP, NodeScalingPool, NodeScalingPoolSpec};

pub const PATH: &str = "/validate-nodescalingpool";
/// Secret annotation: when the serving certificate expires (RFC 3339).
const NOT_AFTER_ANNOTATION: &str = "hardware-autoscaler.safewords.com/not-after";
/// Secret annotation: the DNS names the serving certificate was issued for.
const DNS_NAMES_ANNOTATION: &str = "hardware-autoscaler.safewords.com/dns-names";
/// Certificates are issued for this many years and renewed (at startup) in the last 30 days.
const CERT_YEARS: i32 = 5;

#[derive(Clone, Debug)]
pub struct Settings {
    /// Address the HTTPS server listens on.
    pub addr: SocketAddr,
    /// The operator's namespace (Secret and Service).
    pub namespace: String,
    /// The Service in front of the operator that the API server calls.
    pub service: String,
    pub service_port: i32,
    /// The Secret holding the CA and the serving certificate.
    pub secret: String,
    /// The `ValidatingWebhookConfiguration`'s name.
    pub config_name: String,
    /// The ClusterRole that owns the `ValidatingWebhookConfiguration`.
    pub owner_cluster_role: Option<String>,
}

impl Settings {
    fn dns_names(&self) -> Vec<String> {
        let base = format!("{}.{}.svc", self.service, self.namespace);
        vec![base.clone(), format!("{base}.cluster.local")]
    }
}

#[derive(Clone)]
pub struct Tls {
    pub ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
}

/// Issues a fresh CA and a serving certificate for `dns_names`.
fn issue(dns_names: &[String]) -> anyhow::Result<(Tls, chrono::DateTime<Utc>)> {
    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        KeyUsagePurpose,
    };
    let now = Utc::now();
    let not_after = now
        .with_year(now.year() + CERT_YEARS)
        .unwrap_or(now + chrono::Duration::days(365 * CERT_YEARS as i64));
    let until = rcgen::date_time_ymd(not_after.year(), not_after.month() as u8, not_after.day() as u8);
    let since = rcgen::date_time_ymd(now.year() - 1, 1, 1);

    let mut ca = CertificateParams::new(Vec::<String>::new())?;
    ca.distinguished_name = DistinguishedName::new();
    ca.distinguished_name
        .push(DnType::CommonName, "kube-hardware-autoscaler webhook CA");
    ca.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    ca.not_before = since;
    ca.not_after = until;
    let ca_key = KeyPair::generate()?;
    let ca_cert = ca.self_signed(&ca_key)?;
    let issuer = Issuer::new(ca, ca_key);

    let mut leaf = CertificateParams::new(dns_names.to_vec())?;
    leaf.distinguished_name = DistinguishedName::new();
    leaf.distinguished_name.push(DnType::CommonName, dns_names[0].clone());
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature, KeyUsagePurpose::KeyEncipherment];
    leaf.not_before = since;
    leaf.not_after = until;
    let leaf_key = KeyPair::generate()?;
    let leaf_cert = leaf.signed_by(&leaf_key, &issuer)?;
    Ok((
        Tls {
            ca_pem: ca_cert.pem(),
            cert_pem: leaf_cert.pem(),
            key_pem: leaf_key.serialize_pem(),
        },
        not_after,
    ))
}

fn tls_from_secret(secret: &Secret, dns_names: &str) -> Option<Tls> {
    let annotations = secret.metadata.annotations.as_ref()?;
    if annotations.get(DNS_NAMES_ANNOTATION).map(String::as_str) != Some(dns_names) {
        return None;
    }
    let not_after = chrono::DateTime::parse_from_rfc3339(annotations.get(NOT_AFTER_ANNOTATION)?).ok()?;
    if not_after < Utc::now() + chrono::Duration::days(30) {
        return None;
    }
    let data = secret.data.as_ref()?;
    let text = |k: &str| data.get(k).and_then(|b| String::from_utf8(b.0.clone()).ok());
    Some(Tls {
        ca_pem: text("ca.crt")?,
        cert_pem: text("tls.crt")?,
        key_pem: text("tls.key")?,
    })
}

/// The CA and serving certificate: from the Secret when it holds a valid set,
/// otherwise newly issued and stored there.
pub async fn ensure_tls(client: &Client, s: &Settings) -> anyhow::Result<Tls> {
    let api: Api<Secret> = Api::namespaced(client.clone(), &s.namespace);
    let dns_names = s.dns_names();
    let joined = dns_names.join(",");
    for _ in 0..3 {
        let existing = api.get_opt(&s.secret).await?;
        if let Some(tls) = existing.as_ref().and_then(|sec| tls_from_secret(sec, &joined)) {
            return Ok(tls);
        }
        let (tls, not_after) = issue(&dns_names)?;
        let mut secret: Secret = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "type": "kubernetes.io/tls",
            "metadata": {
                "name": s.secret,
                "namespace": s.namespace,
                "labels": {"app.kubernetes.io/name": "kube-hardware-autoscaler", "app.kubernetes.io/component": "webhook"},
                "annotations": {
                    NOT_AFTER_ANNOTATION: not_after.to_rfc3339(),
                    DNS_NAMES_ANNOTATION: joined,
                },
            },
        }))?;
        secret.data = Some(BTreeMap::from([
            ("ca.crt".to_string(), ByteString(tls.ca_pem.clone().into_bytes())),
            ("tls.crt".to_string(), ByteString(tls.cert_pem.clone().into_bytes())),
            ("tls.key".to_string(), ByteString(tls.key_pem.clone().into_bytes())),
        ]));
        let result = match existing {
            Some(old) => {
                secret.metadata.resource_version = old.resource_version();
                api.replace(&s.secret, &PostParams::default(), &secret).await
            }
            None => api.create(&PostParams::default(), &secret).await,
        };
        match result {
            Ok(_) => {
                info!(secret = %s.secret, %not_after, "issued the webhook's CA and serving certificate");
                return Ok(tls);
            }
            // Someone else wrote it first: read theirs.
            Err(kube::Error::Api(e)) if e.code == 409 => continue,
            Err(e) => return Err(e.into()),
        }
    }
    anyhow::bail!("could not store the webhook certificate in Secret {}", s.secret)
}

/// Why a pool should be refused (`Err`), or the warnings to admit it with.
/// On UPDATE, a name that was already listed is only warned about when its
/// Node is gone, so a Node disappearing does not block every later edit.
pub fn review_pool<'a>(
    new: &NodeScalingPoolSpec,
    old: Option<&NodeScalingPoolSpec>,
    labels_of: impl Fn(&str) -> Option<&'a BTreeMap<String, String>>,
) -> Result<Vec<String>, String> {
    let (missing, outside) = preferred_node_problems(new, labels_of);
    let was_listed = |n: &str| old.is_some_and(|o| o.scale_up.preferred_nodes.iter().any(|p| p.name == n));
    let (kept, refused): (Vec<String>, Vec<String>) = missing.into_iter().partition(|n| was_listed(n));
    if !refused.is_empty() {
        return Err(format!(
            "spec.scaleUp.preferredNodes: no Node named {} (entries are Node names, as in `kubectl get nodes`)",
            quoted(&refused)
        ));
    }
    let mut warnings = Vec::new();
    if !kept.is_empty() {
        warnings.push(format!(
            "spec.scaleUp.preferredNodes: Node {} no longer exists; the entry is ignored",
            quoted(&kept)
        ));
    }
    if !outside.is_empty() {
        warnings.push(format!(
            "spec.scaleUp.preferredNodes: Node {} is not selected by spec.nodeSelector; the entry has no effect",
            quoted(&outside)
        ));
    }
    if let Some(w) = new.scale_down.deprecation_warning() {
        warnings.push(w);
    }
    Ok(warnings)
}

fn quoted(names: &[String]) -> String {
    names.iter().map(|n| format!("{n:?}")).collect::<Vec<_>>().join(", ")
}

async fn validate(
    State(nodes): State<Store<Node>>,
    Json(review): Json<AdmissionReview<NodeScalingPool>>,
) -> Json<AdmissionReview<DynamicObject>> {
    let req: AdmissionRequest<NodeScalingPool> = match review.try_into() {
        Ok(r) => r,
        Err(e) => return Json(AdmissionResponse::invalid(e.to_string()).into_review()),
    };
    let mut resp = AdmissionResponse::from(&req);
    if !matches!(req.operation, Operation::Create | Operation::Update) {
        return Json(resp.into_review());
    }
    let Some(pool) = req.object.as_ref() else {
        return Json(resp.into_review());
    };
    let found: Vec<Arc<Node>> = pool
        .spec
        .scale_up
        .preferred_nodes
        .iter()
        .filter_map(|p| nodes.get(&ObjectRef::new(&p.name)))
        .collect();
    let verdict = review_pool(&pool.spec, req.old_object.as_ref().map(|o| &o.spec), |n| {
        found.iter().find(|node| node.name_any() == n).map(|node| node.labels())
    });
    match verdict {
        Ok(warnings) => {
            if !warnings.is_empty() {
                resp.warnings = Some(warnings);
            }
        }
        Err(reason) => {
            info!(pool = %pool.name_any(), dry_run = req.dry_run, %reason, "refused NodeScalingPool");
            resp = resp.deny(reason);
        }
    }
    Json(resp.into_review())
}

/// Serves the webhook over TLS until the process exits.
pub async fn serve(addr: SocketAddr, tls: &Tls, nodes: Store<Node>) -> anyhow::Result<()> {
    let certs = CertificateDer::pem_slice_iter(tls.cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .context("webhook certificate")?;
    let key = PrivateKeyDer::from_pem_slice(tls.key_pem.as_bytes()).context("webhook key")?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("webhook TLS config")?;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let router = Router::new().route(PATH, post(validate)).with_state(nodes);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding the webhook to {addr}"))?;
    info!(%addr, "webhook listening");
    tokio::spawn(async move {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(s) => s,
                Err(e) => {
                    warn!(error = %e, "webhook accept failed");
                    continue;
                }
            };
            let acceptor = acceptor.clone();
            let router = router.clone();
            tokio::spawn(async move {
                let stream = match acceptor.accept(stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::debug!(%peer, error = %e, "webhook TLS handshake failed");
                        return;
                    }
                };
                let service = hyper_util::service::TowerToHyperService::new(router);
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                    .await
                {
                    tracing::debug!(%peer, error = %e, "webhook connection ended");
                }
            });
        }
    });
    Ok(())
}

/// Applies the `ValidatingWebhookConfiguration` pointing at this operator.
pub async fn register(client: &Client, s: &Settings, tls: &Tls) -> anyhow::Result<()> {
    let mut owner_references = Vec::new();
    if let Some(role) = &s.owner_cluster_role {
        match Api::<ClusterRole>::all(client.clone()).get_opt(role).await {
            Ok(Some(cr)) => owner_references.push(json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRole",
                "name": role,
                "uid": cr.uid().unwrap_or_default(),
            })),
            Ok(None) => {
                warn!(clusterrole = %role, "webhook owner ClusterRole not found; the webhook configuration is unowned")
            }
            Err(e) => {
                warn!(clusterrole = %role, error = %e, "cannot read the webhook owner ClusterRole; the webhook configuration is unowned")
            }
        }
    }
    let config = json!({
        "apiVersion": "admissionregistration.k8s.io/v1",
        "kind": "ValidatingWebhookConfiguration",
        "metadata": {
            "name": s.config_name,
            "labels": {"app.kubernetes.io/name": "kube-hardware-autoscaler", "app.kubernetes.io/component": "webhook"},
            "ownerReferences": owner_references,
        },
        "webhooks": [{
            "name": format!("nodescalingpools.{API_GROUP}"),
            "admissionReviewVersions": ["v1"],
            "sideEffects": "None",
            "failurePolicy": "Fail",
            "matchPolicy": "Equivalent",
            "timeoutSeconds": 5,
            "clientConfig": {
                "service": {
                    "namespace": s.namespace,
                    "name": s.service,
                    "port": s.service_port,
                    "path": PATH,
                },
                "caBundle": base64::engine::general_purpose::STANDARD.encode(&tls.ca_pem),
            },
            "rules": [{
                "apiGroups": [API_GROUP],
                "apiVersions": ["v1alpha1"],
                "operations": ["CREATE", "UPDATE"],
                "resources": ["nodescalingpools"],
                "scope": "Cluster",
            }],
        }],
    });
    Api::<ValidatingWebhookConfiguration>::all(client.clone())
        .patch(
            &s.config_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(config),
        )
        .await
        .with_context(|| format!("applying ValidatingWebhookConfiguration {}", s.config_name))?;
    info!(name = %s.config_name, "webhook registered");
    Ok(())
}

/// Removes the `ValidatingWebhookConfiguration`, so pool changes are not
/// refused while no operator serves the webhook.
pub async fn unregister(client: &Client, s: &Settings) {
    match Api::<ValidatingWebhookConfiguration>::all(client.clone())
        .delete(&s.config_name, &DeleteParams::default())
        .await
    {
        Ok(_) => info!(name = %s.config_name, "webhook unregistered"),
        Err(kube::Error::Api(e)) if e.code == 404 => {}
        Err(e) => warn!(name = %s.config_name, error = %e, "could not unregister the webhook"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(v: serde_json::Value) -> NodeScalingPoolSpec {
        serde_json::from_value(v).unwrap()
    }

    fn pool(preferred: serde_json::Value) -> NodeScalingPoolSpec {
        spec(json!({
            "nodeSelector": {"matchLabels": {"gpu": "true"}},
            "scaleUp": {"preferredNodes": preferred},
        }))
    }

    #[test]
    fn refuses_unknown_nodes_and_warns_about_nodes_outside_the_pool() {
        let gpu = BTreeMap::from([("gpu".to_string(), "true".to_string())]);
        let cpu = BTreeMap::new();
        let labels = |n: &str| match n {
            "gpu-node-1" => Some(&gpu),
            "cpu-box" => Some(&cpu),
            _ => None,
        };
        let ok = pool(json!([{"name": "gpu-node-1", "weight": 100}]));
        assert_eq!(review_pool(&ok, None, labels), Ok(vec![]));

        let typo = pool(json!([{"name": "devbx", "weight": 100}]));
        let err = review_pool(&typo, None, labels).unwrap_err();
        assert!(err.contains("no Node named \"devbx\""), "{err}");

        let outside = pool(json!([{"name": "gpu-node-1", "weight": 100}, {"name": "cpu-box", "weight": 5}]));
        let warnings = review_pool(&outside, None, labels).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("\"cpu-box\" is not selected"), "{}", warnings[0]);

        // A Node that disappeared after the entry was saved does not block other edits.
        let gone = pool(json!([{"name": "gpu-node-1", "weight": 100}, {"name": "retired", "weight": 5}]));
        let warnings = review_pool(&gone, Some(&gone), labels).unwrap();
        assert!(warnings[0].contains("\"retired\" no longer exists"), "{}", warnings[0]);
        // ...but adding it anew does.
        assert!(review_pool(&gone, Some(&ok), labels).is_err());
    }

    #[test]
    fn warns_about_the_deprecated_name() {
        let old_name = spec(json!({
            "nodeSelector": {"matchLabels": {"gpu": "true"}},
            "scaleDown": {"delayAfterScaleUpSeconds": 600},
        }));
        let warnings = review_pool(&old_name, None, |_| None).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("holdAfterPowerOnSeconds"));
    }

    #[test]
    fn issues_a_certificate_chain() {
        let names = vec!["svc.ns.svc".to_string(), "svc.ns.svc.cluster.local".to_string()];
        let (tls, not_after) = issue(&names).unwrap();
        assert!(tls.ca_pem.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(tls.key_pem.contains("PRIVATE KEY"));
        assert!(not_after > Utc::now() + chrono::Duration::days(365 * 4));
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let certs: Vec<_> = CertificateDer::pem_slice_iter(tls.cert_pem.as_bytes())
            .collect::<Result<_, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_slice(tls.key_pem.as_bytes()).unwrap();
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .unwrap();
    }
}
