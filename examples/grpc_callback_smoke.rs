use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use roze_context::Context;
use roze_dtm::{
    grpc_client::DtmGrpcClient,
    pb::dtmgimp::{DtmRequest, DtmTransOptions},
};
use serde_json::Value;
use tokio::sync::oneshot;
use tonic::{transport::Server, Request, Response, Status};

mod callback {
    tonic::include_proto!("workflow_callback_test");
}

use callback::{
    workflow_server::{Workflow, WorkflowServer},
    Empty, WorkflowData,
};

const CALLBACK_ENDPOINT: &str = "127.0.0.1:18092";
const CALLBACK_TARGET: &str = "grpc://127.0.0.1:18092/workflow_callback_test.Workflow/Query";
const TIMEOUT_TARGET: &str = "grpc://127.0.0.1:18092/workflow_callback_test.Workflow/Timeout";

#[derive(Clone, Default)]
struct CallbackService {
    attempts: Arc<Mutex<HashMap<String, usize>>>,
}

#[tonic::async_trait]
impl Workflow for CallbackService {
    async fn query(&self, request: Request<WorkflowData>) -> Result<Response<Empty>, Status> {
        let (gid, attempt) = self.validate_and_record(&request, b"grpc-callback-payload")?;
        if attempt == 1 {
            Err(Status::failed_precondition("grpc callback ongoing"))
        } else {
            Err(Status::aborted(format!(
                "grpc callback business failure for {gid}"
            )))
        }
    }

    async fn timeout(&self, request: Request<WorkflowData>) -> Result<Response<Empty>, Status> {
        self.validate_and_record(&request, b"grpc-timeout-payload")?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(Response::new(Empty {}))
    }
}

impl CallbackService {
    fn validate_and_record(
        &self,
        request: &Request<WorkflowData>,
        expected_data: &[u8],
    ) -> Result<(String, usize), Status> {
        let metadata = request.metadata();
        let gid = metadata_value(metadata, "dtm-gid")?;
        require_metadata(metadata, "dtm-trans_type", "workflow")?;
        require_metadata(metadata, "dtm-branch_id", "00")?;
        let operation = metadata_value(metadata, "dtm-op")?;
        if operation != "grpc-error-matrix" && operation != "grpc-timeout-matrix" {
            return Err(Status::invalid_argument("unexpected dtm-op metadata"));
        }
        if request.get_ref().data != expected_data {
            return Err(Status::invalid_argument("callback payload changed"));
        }
        let mut attempts = self
            .attempts
            .lock()
            .map_err(|_| Status::internal("attempt lock poisoned"))?;
        let attempt = attempts.entry(gid.clone()).or_default();
        *attempt += 1;
        Ok((gid, *attempt))
    }

    fn attempts(&self, gid: &str) -> usize {
        self.attempts
            .lock()
            .expect("attempt lock")
            .get(gid)
            .copied()
            .unwrap_or_default()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let grpc_endpoint = std::env::var("ROZE_DTM_GRPC_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:36791".to_owned());
    let http_endpoint =
        std::env::var("ROZE_DTM_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:18090".to_owned());
    let token = std::env::var("ROZE_DTM_CONTROL_TOKEN")?;
    let callback = CallbackService::default();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let callback_task = tokio::spawn(
        Server::builder()
            .add_service(WorkflowServer::new(callback.clone()))
            .serve_with_shutdown(CALLBACK_ENDPOINT.parse()?, async {
                let _ = shutdown_rx.await;
            }),
    );
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = DtmGrpcClient::connect(grpc_endpoint)
        .await?
        .with_bearer_token(token.clone());
    let context = Context::background();
    let failed_gid = client.new_gid(&context).await?;
    client
        .prepare_workflow(
            &context,
            callback_request(
                &failed_gid,
                CALLBACK_TARGET,
                "grpc-error-matrix",
                b"grpc-callback-payload",
                DtmTransOptions {
                    retry_interval: 1,
                    retry_limit: 3,
                    ..DtmTransOptions::default()
                },
            ),
        )
        .await?;

    let timeout_gid = client.new_gid(&context).await?;
    client
        .prepare_workflow(
            &context,
            callback_request(
                &timeout_gid,
                TIMEOUT_TARGET,
                "grpc-timeout-matrix",
                b"grpc-timeout-payload",
                DtmTransOptions {
                    timeout_to_fail: 3,
                    retry_interval: 1,
                    request_timeout: 1,
                    retry_limit: 3,
                    ..DtmTransOptions::default()
                },
            ),
        )
        .await?;

    let failed = wait_for_status(&http_endpoint, &token, &failed_gid, "failed").await?;
    anyhow::ensure!(
        failed["metadata"]["rollback_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("grpc callback business failure")),
        "gRPC ABORTED reason was not persisted"
    );
    let timed_out = wait_for_status(&http_endpoint, &token, &timeout_gid, "failed").await?;
    anyhow::ensure!(
        timed_out["metadata"]["rollback_reason"] == "workflow callback timed out",
        "gRPC callback timeout did not use the stable terminal reason"
    );
    anyhow::ensure!(
        callback.attempts(&failed_gid) >= 2,
        "FAILED_PRECONDITION callback was not retried"
    );
    anyhow::ensure!(
        callback.attempts(&timeout_gid) >= 1,
        "timeout callback was not invoked"
    );

    let _ = shutdown_tx.send(());
    callback_task.await??;
    println!("grpc callback smoke passed failed_gid={failed_gid} timeout_gid={timeout_gid}");
    Ok(())
}

fn callback_request(
    gid: &str,
    target: &str,
    operation: &str,
    data: &[u8],
    options: DtmTransOptions,
) -> DtmRequest {
    use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

    DtmRequest {
        gid: gid.to_owned(),
        trans_type: "workflow".to_owned(),
        trans_options: Some(options),
        customed_data: serde_json::json!({
            "name": operation,
            "data": BASE64_STANDARD.encode(data),
        })
        .to_string(),
        query_prepared: target.to_owned(),
        ..DtmRequest::default()
    }
}

async fn wait_for_status(
    endpoint: &str,
    token: &str,
    gid: &str,
    expected: &str,
) -> anyhow::Result<Value> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
    while tokio::time::Instant::now() < deadline {
        let response = client
            .get(format!(
                "{}/v1/transactions/{}",
                endpoint.trim_end_matches('/'),
                gid
            ))
            .bearer_auth(token)
            .send()
            .await?;
        anyhow::ensure!(response.status().is_success(), "transaction query failed");
        let body: Value = response.json().await?;
        let transaction = body
            .get("data")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("transaction response omitted data"))?;
        if transaction["status"]
            .as_str()
            .is_some_and(|status| status.eq_ignore_ascii_case(expected))
        {
            return Ok(transaction);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("transaction {gid} did not reach {expected}")
}

fn metadata_value(
    metadata: &tonic::metadata::MetadataMap,
    name: &'static str,
) -> Result<String, Status> {
    metadata
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| Status::invalid_argument(format!("missing {name} metadata")))
}

fn require_metadata(
    metadata: &tonic::metadata::MetadataMap,
    name: &'static str,
    expected: &str,
) -> Result<(), Status> {
    if metadata_value(metadata, name)? == expected {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "unexpected {name} metadata"
        )))
    }
}
