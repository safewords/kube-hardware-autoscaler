//! PowerRequest controller: keeps each request's status current, emits an
//! Event when its phase changes, and deletes it an hour after it finishes.
//!
//! It never powers anything itself. The pools read the same requests (through
//! `Context::power_request_holds`, the same `power_request::evaluate`) and
//! wake or keep the machine.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use kube::api::DeleteParams;
use kube::runtime::controller::Action;
use kube::runtime::events::EventType;
use kube::runtime::reflector::ObjectRef;
use kube::{Api, ResourceExt};
use tracing::{info, warn};

use super::{Context, Error, patch_status_diff};
use crate::crd::{PowerRequest, PowerRequestPhase};
use crate::power_request::{FINISHED_TTL_SECS, evaluate};

const INTERVAL: Duration = Duration::from_secs(15);

pub async fn reconcile(req: Arc<PowerRequest>, ctx: Arc<Context>) -> Result<Action, Error> {
    let ns = req.namespace().unwrap_or_default();
    let name = req.name_any();
    let api: Api<PowerRequest> = Api::namespaced(ctx.client.clone(), &ns);
    let now = Utc::now();
    let old = req.status.clone().unwrap_or_default();

    if let Some(done) = old.finished_at.filter(|_| old.phase.is_terminal()) {
        let gone = done + chrono::Duration::seconds(FINISHED_TTL_SECS);
        if now >= gone {
            info!(request = %format!("{ns}/{name}"), "deleting finished power request");
            match api.delete(&name, &DeleteParams::default()).await {
                Ok(_) => {}
                Err(kube::Error::Api(status)) if status.code == 404 => {}
                Err(e) => return Err(e.into()),
            }
            return Ok(Action::await_change());
        }
        return Ok(Action::requeue((gone - now).to_std().unwrap_or(INTERVAL)));
    }

    let created = req
        .metadata
        .creation_timestamp
        .as_ref()
        .and_then(|t| chrono::DateTime::from_timestamp(t.0.as_second(), 0))
        .unwrap_or(now);
    let mn = ctx
        .node_power_management_configs
        .get(&ObjectRef::new(&req.spec.node_name));
    let e = evaluate(
        &ns,
        &name,
        &req.spec,
        &old,
        created,
        mn.as_deref(),
        now,
        ctx.max_power_request_secs,
    );
    if e.status.phase != old.phase {
        let type_ = match e.status.phase {
            PowerRequestPhase::Refused | PowerRequestPhase::Failed => EventType::Warning,
            _ => EventType::Normal,
        };
        let msg = e.status.message.clone().unwrap_or_default();
        info!(request = %format!("{ns}/{name}"), phase = ?e.status.phase, %msg, "power request");
        ctx.event(req.as_ref(), type_, &format!("{:?}", e.status.phase), msg)
            .await;
    }
    patch_status_diff(&api, &name, &old, &e.status).await?;
    Ok(Action::requeue(INTERVAL))
}

pub fn error_policy(req: Arc<PowerRequest>, err: &Error, ctx: Arc<Context>) -> Action {
    warn!(request = %req.name_any(), error = %err, "reconcile failed");
    ctx.metrics.reconcile_error("powerrequest");
    Action::requeue(INTERVAL)
}
