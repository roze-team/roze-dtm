use std::collections::BTreeMap;

use roze_error::RozeError;
use roze_grpc::transport::{Request, Response, Status};

use roze_dtm::pb::dtmgimp::{
    dtm_server::Dtm, DtmBranchRequest, DtmGidReply, DtmProgress, DtmProgressesReply, DtmRequest,
    DtmTopicRequest, DtmTransaction,
};

use crate::{CompatBranchRequest, CompatOperation, CompatTransactionRequest, ControlState};

#[derive(Clone)]
pub(crate) struct DtmGrpcService {
    state: ControlState,
}

impl DtmGrpcService {
    pub(crate) fn new(state: ControlState) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl Dtm for DtmGrpcService {
    async fn new_gid(&self, request: Request<()>) -> Result<Response<DtmGidReply>, Status> {
        let context = authorize(&self.state, &request)?;
        Ok(roze_rpc::rpc::response_with_context(
            DtmGidReply {
                gid: crate::generate_gid(),
            },
            &context,
        ))
    }

    async fn submit(&self, request: Request<DtmRequest>) -> Result<Response<()>, Status> {
        apply_transaction(&self.state, request, CompatOperation::Submit).await
    }

    async fn prepare(&self, request: Request<DtmRequest>) -> Result<Response<()>, Status> {
        apply_transaction(&self.state, request, CompatOperation::Prepare).await
    }

    async fn abort(&self, request: Request<DtmRequest>) -> Result<Response<()>, Status> {
        apply_transaction(&self.state, request, CompatOperation::Abort).await
    }

    async fn register_branch(
        &self,
        request: Request<DtmBranchRequest>,
    ) -> Result<Response<()>, Status> {
        let context = authorize(&self.state, &request)?;
        let DtmBranchRequest {
            gid,
            trans_type,
            branch_id,
            op,
            mut data,
            busi_payload,
            ..
        } = request.into_inner();
        let registration = CompatBranchRequest {
            gid: gid.clone(),
            trans_type,
            branch_id,
            data: Some(
                serde_json::to_string(&payload_value(busi_payload.clone()))
                    .map_err(|_| bad_request("invalid branch payload", &context))?,
            ),
            op: data.remove("op").or_else(|| (!op.is_empty()).then_some(op)),
            status: data.remove("status"),
            confirm: data.remove("confirm"),
            cancel: data.remove("cancel"),
            url: data.remove("url"),
            binary_data: Some(busi_payload),
        };
        let registration = crate::compat_registration_from_request(&self.state, registration)
            .map_err(|_| bad_request("invalid branch registration", &context))?;
        let transaction = crate::apply_compat_registration(&self.state, &gid, registration)
            .await
            .map_err(|_| {
                crate::audit_compat_failure(
                    &self.state.audit_history,
                    "dtm.compat.grpc.register_branch",
                    Some(&gid),
                );
                operation_failed("branch registration failed", &context)
            })?;
        crate::audit_transition(
            &self.state.audit_history,
            "dtm.compat.grpc.register_branch",
            &transaction,
        );
        success((), &context)
    }

    async fn prepare_workflow(
        &self,
        request: Request<DtmRequest>,
    ) -> Result<Response<DtmProgressesReply>, Status> {
        let context = authorize(&self.state, &request)?;
        let mut input = compat_request(request.into_inner(), &context)?;
        input.trans_type = "workflow".to_owned();
        let gid = input.gid.clone();
        let transaction = crate::compat_apply(&self.state, input, CompatOperation::Prepare)
            .await
            .map_err(|_| {
                crate::audit_compat_failure(
                    &self.state.audit_history,
                    "dtm.compat.grpc.prepare_workflow",
                    Some(&gid),
                );
                operation_failed("workflow preparation failed", &context)
            })?;
        crate::audit_transition(
            &self.state.audit_history,
            "dtm.compat.grpc.prepare_workflow",
            &transaction,
        );
        let progresses = transaction
            .workflow_progresses
            .iter()
            .map(|progress| DtmProgress {
                status: crate::workflow_progress_status_name(progress.status).to_owned(),
                bin_data: progress.data.clone(),
                branch_id: progress.branch_id.clone(),
                op: progress.operation.clone(),
            })
            .collect();
        let rollback_reason = transaction
            .metadata
            .get("rollback_reason")
            .cloned()
            .unwrap_or_default();
        let result = transaction
            .metadata
            .get("dtm.workflow.result")
            .cloned()
            .unwrap_or_default();
        success(
            DtmProgressesReply {
                transaction: Some(DtmTransaction {
                    gid: transaction.gid,
                    status: crate::compat_workflow_status_name(transaction.status).to_owned(),
                    rollback_reason,
                    result,
                }),
                progresses,
            },
            &context,
        )
    }

    async fn subscribe(&self, request: Request<DtmTopicRequest>) -> Result<Response<()>, Status> {
        let context = authorize(&self.state, &request)?;
        let input = request.into_inner();
        self.state
            .branch_url_policy
            .validate(&input.url)
            .map_err(|_| bad_request("subscriber URL is not allowed", &context))?;
        self.state
            .dtm
            .subscribe_topic(&input.topic, &input.url, &input.remark)
            .await
            .map_err(|_| {
                crate::audit_resource_operation(
                    &self.state.audit_history,
                    "dtm.compat.grpc.subscribe",
                    "topic",
                    &input.topic,
                    "failed",
                );
                operation_failed("topic subscription failed", &context)
            })?;
        crate::audit_resource_operation(
            &self.state.audit_history,
            "dtm.compat.grpc.subscribe",
            "topic",
            &input.topic,
            "success",
        );
        success((), &context)
    }

    async fn unsubscribe(&self, request: Request<DtmTopicRequest>) -> Result<Response<()>, Status> {
        let context = authorize(&self.state, &request)?;
        let input = request.into_inner();
        self.state
            .dtm
            .unsubscribe_topic(&input.topic, &input.url)
            .await
            .map_err(|_| {
                crate::audit_resource_operation(
                    &self.state.audit_history,
                    "dtm.compat.grpc.unsubscribe",
                    "topic",
                    &input.topic,
                    "failed",
                );
                operation_failed("topic unsubscription failed", &context)
            })?;
        crate::audit_resource_operation(
            &self.state.audit_history,
            "dtm.compat.grpc.unsubscribe",
            "topic",
            &input.topic,
            "success",
        );
        success((), &context)
    }

    async fn delete_topic(
        &self,
        request: Request<DtmTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let context = authorize(&self.state, &request)?;
        let topic = request.into_inner().topic;
        let deleted = self.state.dtm.delete_topic(&topic).await.map_err(|_| {
            crate::audit_resource_operation(
                &self.state.audit_history,
                "dtm.compat.grpc.delete_topic",
                "topic",
                &topic,
                "failed",
            );
            operation_failed("topic deletion failed", &context)
        })?;
        if !deleted {
            crate::audit_resource_operation(
                &self.state.audit_history,
                "dtm.compat.grpc.delete_topic",
                "topic",
                &topic,
                "not_found",
            );
            return Err(roze_rpc::rpc::status_from_error(
                RozeError::NotFound("topic not found".to_owned()),
                &context,
            ));
        }
        crate::audit_resource_operation(
            &self.state.audit_history,
            "dtm.compat.grpc.delete_topic",
            "topic",
            &topic,
            "success",
        );
        success((), &context)
    }
}

async fn apply_transaction(
    state: &ControlState,
    request: Request<DtmRequest>,
    operation: CompatOperation,
) -> Result<Response<()>, Status> {
    let context = authorize(state, &request)?;
    let input = compat_request(request.into_inner(), &context)?;
    let gid = input.gid.clone();
    let event = operation.event(input.protocol);
    let transaction = crate::compat_apply(state, input, operation)
        .await
        .map_err(|_| {
            crate::audit_compat_failure(&state.audit_history, event, Some(&gid));
            operation_failed("transaction operation failed", &context)
        })?;
    crate::audit_transition(&state.audit_history, event, &transaction);
    success((), &context)
}

fn compat_request(
    input: DtmRequest,
    context: &roze_context::Context,
) -> Result<CompatTransactionRequest, Status> {
    let steps = if input.steps.trim().is_empty() {
        Vec::new()
    } else {
        serde_json::from_str::<Vec<BTreeMap<String, String>>>(&input.steps)
            .map_err(|_| bad_request("invalid transaction steps", context))?
    };
    let options = input.trans_options.unwrap_or_default();
    Ok(CompatTransactionRequest {
        gid: input.gid,
        trans_type: input.trans_type,
        steps,
        payloads: input.bin_payloads.into_iter().map(payload_value).collect(),
        timeout_to_fail: positive_u64(options.timeout_to_fail),
        rollback_reason: (!input.rollback_reason.is_empty()).then_some(input.rollback_reason),
        custom_data: (!input.customed_data.is_empty()).then_some(input.customed_data),
        query_prepared: (!input.query_prepared.is_empty()).then_some(input.query_prepared),
        wait_result: options.wait_result,
        // The pinned upstream gRPC contract has no Concurrent field. Field 4
        // remains reserved for the deprecated passthrough-header option.
        concurrent: false,
        retry_interval: positive_u64(options.retry_interval),
        request_timeout: positive_u64(options.request_timeout),
        retry_limit: positive_u64(options.retry_limit),
        branch_headers: options.branch_headers.into_iter().collect(),
        req_extra: input.req_extra.into_iter().collect(),
        protocol: crate::CompatProtocol::Grpc,
    })
}

fn positive_u64(value: i64) -> Option<u64> {
    u64::try_from(value).ok().filter(|value| *value > 0)
}

fn payload_value(bytes: Vec<u8>) -> serde_json::Value {
    serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        serde_json::Value::Array(
            bytes
                .into_iter()
                .map(|byte| serde_json::Value::from(u64::from(byte)))
                .collect(),
        )
    })
}

fn authorize<T>(
    state: &ControlState,
    request: &Request<T>,
) -> Result<roze_context::Context, Status> {
    let context = roze_rpc::rpc::request_context(request);
    let Some(expected) = state.control_token.as_deref() else {
        return Ok(context);
    };
    let provided = request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if provided
        .is_some_and(|provided| crate::constant_time_eq(provided.as_bytes(), expected.as_bytes()))
    {
        Ok(context)
    } else {
        Err(roze_rpc::rpc::status_from_error(
            RozeError::Unauthorized,
            &context,
        ))
    }
}

fn success<T>(value: T, context: &roze_context::Context) -> Result<Response<T>, Status> {
    Ok(roze_rpc::rpc::response_with_context(value, context))
}

fn bad_request(message: &str, context: &roze_context::Context) -> Status {
    roze_rpc::rpc::status_from_error(RozeError::BadRequest(message.to_owned()), context)
}

fn operation_failed(message: &str, context: &roze_context::Context) -> Status {
    roze_rpc::rpc::status_from_error(RozeError::FailedPrecondition(message.to_owned()), context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use roze_dtm::pb;

    #[test]
    fn grpc_request_preserves_steps_payloads_and_timeout() {
        let request = pb::dtmgimp::DtmRequest {
            gid: "gid-grpc".to_owned(),
            trans_type: "saga".to_owned(),
            trans_options: Some(pb::dtmgimp::DtmTransOptions {
                timeout_to_fail: 30,
                retry_interval: 5,
                request_timeout: 2,
                retry_limit: 7,
                branch_headers: [("x-tenant".to_owned(), "tenant-a".to_owned())]
                    .into_iter()
                    .collect(),
                ..Default::default()
            }),
            bin_payloads: vec![br#"{"order_id":"42"}"#.to_vec()],
            customed_data: "opaque-data".to_owned(),
            req_extra: [("status".to_owned(), "submitted".to_owned())]
                .into_iter()
                .collect(),
            steps: r#"[{"action":"http://inventory/action","compensate":"http://inventory/compensate"}]"#.to_owned(),
            ..Default::default()
        };
        let converted =
            compat_request(request, &roze_context::Context::background()).expect("convert request");
        assert_eq!(converted.gid, "gid-grpc");
        assert_eq!(converted.timeout_to_fail, Some(30));
        assert_eq!(converted.steps.len(), 1);
        assert_eq!(converted.payloads[0]["order_id"], "42");
        assert_eq!(converted.custom_data.as_deref(), Some("opaque-data"));
        assert_eq!(converted.retry_interval, Some(5));
        assert_eq!(converted.branch_headers["x-tenant"], "tenant-a");
        assert_eq!(converted.req_extra["status"], "submitted");
        assert!(converted.protocol == crate::CompatProtocol::Grpc);
        let transaction = crate::compat_transaction(
            roze_dtm::TransactionKind::Saga,
            &converted,
            &roze_dtm::BranchUrlPolicy::allow_all(),
        )
        .expect("build transaction");
        assert_eq!(transaction.options.retry_interval_millis, Some(5_000));
        assert_eq!(transaction.options.request_timeout_millis, Some(2_000));
        assert_eq!(transaction.options.retry_limit, Some(7));
        assert_eq!(transaction.options.branch_headers["x-tenant"], "tenant-a");
    }

    #[test]
    fn opaque_grpc_payload_is_retained_as_bytes() {
        assert_eq!(
            payload_value(vec![0, 1, 255]),
            serde_json::json!([0, 1, 255])
        );
    }
}
