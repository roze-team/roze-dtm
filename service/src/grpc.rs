use std::collections::BTreeMap;

use roze_error::RozeError;
use roze_grpc::transport::{Request, Response, Status};

use roze_dtm::pb::dtmgimp::{
    dtm_server::Dtm, DtmBranchRequest, DtmGidReply, DtmProgress, DtmProgressesReply, DtmRequest,
    DtmTopicRequest, DtmTransaction,
};

use crate::{
    CompatBranchRequest, CompatOperation, CompatTransactionRequest, ControlState,
};

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
            mut data,
            busi_payload,
            ..
        } = request.into_inner();
        let branch = CompatBranchRequest {
            gid: gid.clone(),
            trans_type,
            branch_id,
            data: Some(
                serde_json::to_string(&payload_value(busi_payload))
                    .map_err(|_| bad_request("invalid branch payload", &context))?,
            ),
            confirm: data.remove("confirm"),
            cancel: data.remove("cancel"),
            url: data.remove("url"),
        };
        let branch = crate::compat_branch_from_request(&self.state, branch)
            .map_err(|_| bad_request("invalid branch registration", &context))?;
        self.state
            .dtm
            .register_branch(&gid, branch)
            .await
            .map_err(|_| operation_failed("branch registration failed", &context))?;
        success((), &context)
    }

    async fn prepare_workflow(
        &self,
        request: Request<DtmRequest>,
    ) -> Result<Response<DtmProgressesReply>, Status> {
        let context = authorize(&self.state, &request)?;
        let mut input = compat_request(request.into_inner(), &context)?;
        input.trans_type = "workflow".to_owned();
        let transaction = crate::compat_apply(&self.state, input, CompatOperation::Prepare)
            .await
            .map_err(|_| operation_failed("workflow preparation failed", &context))?;
        let progresses = transaction
            .branches
            .iter()
            .map(|branch| DtmProgress {
                status: branch_status_name(branch.status).to_owned(),
                bin_data: serde_json::to_vec(&branch.payload).unwrap_or_default(),
                branch_id: branch.id.clone(),
                op: branch_operation(branch.kind).to_owned(),
            })
            .collect();
        let rollback_reason = transaction
            .metadata
            .get("rollback_reason")
            .cloned()
            .unwrap_or_default();
        success(
            DtmProgressesReply {
                transaction: Some(DtmTransaction {
                    gid: transaction.gid,
                    status: crate::status_name(transaction.status).to_owned(),
                    rollback_reason,
                    result: String::new(),
                }),
                progresses,
            },
            &context,
        )
    }

    async fn subscribe(
        &self,
        request: Request<DtmTopicRequest>,
    ) -> Result<Response<()>, Status> {
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
            .map_err(|_| operation_failed("topic subscription failed", &context))?;
        success((), &context)
    }

    async fn unsubscribe(
        &self,
        request: Request<DtmTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let context = authorize(&self.state, &request)?;
        let input = request.into_inner();
        self.state
            .dtm
            .unsubscribe_topic(&input.topic, &input.url)
            .await
            .map_err(|_| operation_failed("topic unsubscription failed", &context))?;
        success((), &context)
    }

    async fn delete_topic(
        &self,
        request: Request<DtmTopicRequest>,
    ) -> Result<Response<()>, Status> {
        let context = authorize(&self.state, &request)?;
        let deleted = self
            .state
            .dtm
            .delete_topic(&request.into_inner().topic)
            .await
            .map_err(|_| operation_failed("topic deletion failed", &context))?;
        if !deleted {
            return Err(roze_rpc::rpc::status_from_error(
                RozeError::NotFound("topic not found".to_owned()),
                &context,
            ));
        }
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
    crate::compat_apply(state, input, operation)
        .await
        .map_err(|_| operation_failed("transaction operation failed", &context))?;
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
        retry_interval: positive_u64(options.retry_interval),
        request_timeout: positive_u64(options.request_timeout),
        retry_limit: positive_u64(options.retry_limit),
        branch_headers: options.branch_headers.into_iter().collect(),
        req_extra: input.req_extra.into_iter().collect(),
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
    if provided.is_some_and(|provided| {
        crate::constant_time_eq(provided.as_bytes(), expected.as_bytes())
    }) {
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
    roze_rpc::rpc::status_from_error(
        RozeError::FailedPrecondition(message.to_owned()),
        context,
    )
}

const fn branch_status_name(status: roze_dtm::BranchStatus) -> &'static str {
    match status {
        roze_dtm::BranchStatus::Pending => "prepared",
        roze_dtm::BranchStatus::Running => "submitted",
        roze_dtm::BranchStatus::Compensating => "submitted",
        roze_dtm::BranchStatus::Succeeded => "succeed",
        roze_dtm::BranchStatus::Failed => "failed",
        roze_dtm::BranchStatus::Skipped => "failed",
    }
}

const fn branch_operation(kind: roze_dtm::BranchKind) -> &'static str {
    match kind {
        roze_dtm::BranchKind::SagaAction
        | roze_dtm::BranchKind::WorkflowAction
        | roze_dtm::BranchKind::MessageAction => "action",
        roze_dtm::BranchKind::SagaCompensate => "compensate",
        roze_dtm::BranchKind::TccTry => "try",
        roze_dtm::BranchKind::TccConfirm => "confirm",
        roze_dtm::BranchKind::TccCancel => "cancel",
        roze_dtm::BranchKind::XaAction => "commit",
    }
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
        let converted = compat_request(request, &roze_context::Context::background())
            .expect("convert request");
        assert_eq!(converted.gid, "gid-grpc");
        assert_eq!(converted.timeout_to_fail, Some(30));
        assert_eq!(converted.steps.len(), 1);
        assert_eq!(converted.payloads[0]["order_id"], "42");
        assert_eq!(converted.custom_data.as_deref(), Some("opaque-data"));
        assert_eq!(converted.retry_interval, Some(5));
        assert_eq!(converted.branch_headers["x-tenant"], "tenant-a");
        assert_eq!(converted.req_extra["status"], "submitted");
    }

    #[test]
    fn opaque_grpc_payload_is_retained_as_bytes() {
        assert_eq!(
            payload_value(vec![0, 1, 255]),
            serde_json::json!([0, 1, 255])
        );
    }
}
