use std::{collections::BTreeMap, future::Future};

use anyhow::Context as _;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::{Deserialize, Serialize};

use crate::{
    BranchKind, KvEntry, Transaction, TransactionKind, TransactionOptions,
    WorkflowProgressStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateBranchRequest {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<BranchKind>,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compensate: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel: Option<String>,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTransactionRequest {
    pub gid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<TransactionKind>,
    pub branches: Vec<CreateBranchRequest>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub options: TransactionOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackWorkflowProgress {
    pub branch_id: String,
    pub operation: String,
    pub status: WorkflowProgressStatus,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackWorkflowSnapshot {
    pub gid: String,
    pub status: String,
    pub rollback_reason: String,
    pub result: Vec<u8>,
    pub progresses: Vec<CallbackWorkflowProgress>,
}

#[derive(Deserialize)]
struct CallbackWorkflowWire {
    transaction: CallbackWorkflowTransactionWire,
    #[serde(default)]
    progresses: Vec<CallbackWorkflowProgressWire>,
}

#[derive(Deserialize)]
struct CallbackWorkflowTransactionWire {
    gid: String,
    status: String,
    #[serde(default)]
    rollback_reason: String,
    #[serde(default)]
    result: String,
}

#[derive(Deserialize)]
struct CallbackWorkflowProgressWire {
    status: String,
    #[serde(default)]
    bin_data: String,
    branch_id: String,
    op: String,
}

#[derive(Debug, Clone)]
pub struct DtmHttpClient {
    base_url: String,
    bearer_token: Option<String>,
    client: reqwest::Client,
}

impl DtmHttpClient {
    pub fn new(base_url: impl Into<String>) -> anyhow::Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        anyhow::ensure!(
            base_url.starts_with("http://") || base_url.starts_with("https://"),
            "DTM base URL must use HTTP(S)"
        );
        Ok(Self {
            base_url,
            bearer_token: None,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    pub async fn submit(
        &self,
        kind: TransactionKind,
        request: &CreateTransactionRequest,
    ) -> anyhow::Result<Transaction> {
        let path = match kind {
            TransactionKind::Tcc => "/v1/tcc",
            TransactionKind::Saga => "/v1/saga",
            TransactionKind::Workflow => "/v1/workflows",
            TransactionKind::Message => "/v1/messages",
            TransactionKind::Xa => "/v1/xa",
        };
        self.post_transaction(path, Some(request)).await
    }

    pub async fn transition(&self, path: &str) -> anyhow::Result<Transaction> {
        self.post_transaction(path, Option::<&()>::None).await
    }

    /// Creates an empty prepared XA global transaction using the upstream-compatible API.
    pub async fn prepare_xa(&self, gid: &str) -> anyhow::Result<()> {
        self.compat_xa_operation("/api/dtmsvr/prepare", gid).await
    }

    /// Commits all registered XA resource branches.
    pub async fn commit_xa(&self, gid: &str) -> anyhow::Result<()> {
        self.compat_xa_operation("/api/dtmsvr/submit", gid).await
    }

    /// Rolls back all registered XA resource branches.
    pub async fn rollback_xa(&self, gid: &str) -> anyhow::Result<()> {
        self.compat_xa_operation("/api/dtmsvr/abort", gid).await
    }

    /// Runs a global XA decision around one or more resource-branch calls.
    ///
    /// The business closure must return failures instead of panicking so the
    /// client can persist the rollback decision.
    pub async fn xa_global_transaction<T, F, Fut>(
        &self,
        gid: &str,
        work: F,
    ) -> anyhow::Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        self.prepare_xa(gid).await?;
        match work().await {
            Ok(value) => {
                self.commit_xa(gid).await?;
                Ok(value)
            }
            Err(error) => {
                self.rollback_xa(gid)
                    .await
                    .context("failed to persist XA rollback decision")?;
                Err(error.context("XA global business operation failed"))
            }
        }
    }

    /// Registers the endpoint that will resolve one prepared XA resource branch.
    pub async fn register_xa_branch(
        &self,
        gid: &str,
        branch_id: &str,
        phase2_url: &str,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !gid.is_empty() && gid.len() <= 128,
            "XA gid must contain 1 to 128 bytes"
        );
        anyhow::ensure!(
            !branch_id.is_empty() && branch_id.len() <= 128,
            "XA branch id must contain 1 to 128 bytes"
        );
        let url = reqwest::Url::parse(phase2_url).context("invalid XA phase-2 URL")?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none(),
            "XA phase-2 URL must be HTTP(S) without credentials or fragment"
        );
        let response = self
            .authorized(
                self.client
                    .post(format!("{}/api/dtmsvr/registerXaBranch", self.base_url))
                    .json(&serde_json::json!({
                        "gid": gid,
                        "trans_type": "xa",
                        "branch_id": branch_id,
                        "url": phase2_url,
                    })),
            )
            .send()
            .await?;
        decode_compat_success(response).await
    }

    pub async fn get(&self, gid: &str) -> anyhow::Result<Transaction> {
        let response = self
            .authorized(self.client.get(format!("{}/v1/transactions/{gid}", self.base_url)))
            .send()
            .await?;
        decode_transaction(response).await
    }

    pub async fn compat_new_gid(&self) -> anyhow::Result<String> {
        let response = self
            .authorized(self.client.get(format!("{}/api/dtmsvr/newGid", self.base_url)))
            .send()
            .await?;
        let status = response.status();
        let value: serde_json::Value = response.json().await?;
        anyhow::ensure!(status.is_success(), "DTM newGid failed with status {status}");
        value
            .get("gid")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned)
            .context("DTM newGid response did not contain gid")
    }

    pub async fn prepare_callback_workflow(
        &self,
        gid: &str,
        query_prepared: &str,
        custom_data: &str,
    ) -> anyhow::Result<CallbackWorkflowSnapshot> {
        let response = self
            .authorized(
                self.client
                    .post(format!("{}/api/dtmsvr/prepareWorkflow", self.base_url))
                    .json(&serde_json::json!({
                        "gid": gid,
                        "trans_type": "workflow",
                        "query_prepared": query_prepared,
                        "custom_data": custom_data,
                    })),
            )
            .send()
            .await?;
        decode_callback_workflow(response).await
    }

    pub async fn prepare_named_callback_workflow(
        &self,
        gid: &str,
        query_prepared: &str,
        workflow_name: &str,
        data: &[u8],
    ) -> anyhow::Result<CallbackWorkflowSnapshot> {
        anyhow::ensure!(
            !workflow_name.is_empty() && workflow_name.len() <= 128,
            "callback workflow name must contain 1 to 128 bytes"
        );
        anyhow::ensure!(data.len() <= 2 * 1024 * 1024, "callback workflow data exceeds 2 MiB");
        let custom_data = serde_json::json!({
            "name": workflow_name,
            "data": BASE64_STANDARD.encode(data),
        })
        .to_string();
        self.prepare_callback_workflow(gid, query_prepared, &custom_data)
            .await
    }

    pub async fn record_callback_workflow_progress(
        &self,
        gid: &str,
        branch_id: &str,
        operation: &str,
        status: WorkflowProgressStatus,
        data: &[u8],
    ) -> anyhow::Result<()> {
        let data = std::str::from_utf8(data)
            .context("HTTP callback workflow progress data must be UTF-8; use gRPC for binary data")?;
        let status = match status {
            WorkflowProgressStatus::Succeeded => "succeed",
            WorkflowProgressStatus::Failed => "failed",
        };
        let response = self
            .authorized(
                self.client
                    .post(format!("{}/api/dtmsvr/registerBranch", self.base_url))
                    .json(&serde_json::json!({
                        "gid": gid,
                        "trans_type": "workflow",
                        "branch_id": branch_id,
                        "op": operation,
                        "status": status,
                        "data": data,
                    })),
            )
            .send()
            .await?;
        decode_compat_success(response).await
    }

    pub async fn finish_callback_workflow(
        &self,
        gid: &str,
        status: WorkflowProgressStatus,
        rollback_reason: &str,
        result: &[u8],
    ) -> anyhow::Result<()> {
        let status = match status {
            WorkflowProgressStatus::Succeeded => "succeed",
            WorkflowProgressStatus::Failed => "failed",
        };
        let response = self
            .authorized(
                self.client
                    .post(format!("{}/api/dtmsvr/submit", self.base_url))
                    .json(&serde_json::json!({
                        "gid": gid,
                        "trans_type": "workflow",
                        "req_extra": {
                            "status": status,
                            "rollback_reason": rollback_reason,
                            "result": BASE64_STANDARD.encode(result),
                        },
                    })),
            )
            .send()
            .await?;
        decode_compat_success(response).await
    }

    pub async fn subscribe_topic(
        &self,
        topic: &str,
        url: &str,
        remark: &str,
    ) -> anyhow::Result<()> {
        self.compat_topic_operation(
            "/api/dtmsvr/subscribe",
            &[("topic", topic), ("url", url), ("remark", remark)],
        )
        .await
    }

    pub async fn unsubscribe_topic(&self, topic: &str, url: &str) -> anyhow::Result<()> {
        self.compat_topic_operation(
            "/api/dtmsvr/unsubscribe",
            &[("topic", topic), ("url", url)],
        )
        .await
    }

    pub async fn delete_topic(&self, topic: &str) -> anyhow::Result<()> {
        let mut url = reqwest::Url::parse(&format!("{}/api/dtmsvr/topic/", self.base_url))?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("DTM base URL cannot contain path segments"))?
            .push(topic);
        let response = self.authorized(self.client.delete(url)).send().await?;
        decode_compat_success(response).await
    }

    pub async fn query_kv(
        &self,
        category: Option<&str>,
        key: Option<&str>,
    ) -> anyhow::Result<Vec<KvEntry>> {
        let mut query = Vec::new();
        if let Some(category) = category {
            query.push(("cat", category));
        }
        if let Some(key) = key {
            query.push(("key", key));
        }
        let response = self
            .authorized(
                self.client
                    .get(format!("{}/api/dtmsvr/queryKV", self.base_url))
                    .query(&query),
            )
            .send()
            .await?;
        let status = response.status();
        let value: serde_json::Value = response.json().await?;
        anyhow::ensure!(status.is_success(), "DTM KV query failed with status {status}");
        serde_json::from_value(value.get("kv").cloned().context("KV response did not contain kv")?)
            .map_err(Into::into)
    }

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.bearer_token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    async fn compat_topic_operation(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> anyhow::Result<()> {
        let response = self
            .authorized(
                self.client
                    .get(format!("{}{}", self.base_url, path))
                    .query(query),
            )
            .send()
            .await?;
        decode_compat_success(response).await
    }

    async fn compat_xa_operation(&self, path: &str, gid: &str) -> anyhow::Result<()> {
        anyhow::ensure!(!gid.is_empty() && gid.len() <= 128, "invalid XA gid");
        let response = self
            .authorized(
                self.client
                    .post(format!("{}{}", self.base_url, path))
                    .json(&serde_json::json!({
                        "gid": gid,
                        "trans_type": "xa",
                        "wait_result": true,
                    })),
            )
            .send()
            .await?;
        decode_compat_success(response).await
    }

    async fn post_transaction<T: Serialize + ?Sized>(
        &self,
        path: &str,
        body: Option<&T>,
    ) -> anyhow::Result<Transaction> {
        let mut request = self.authorized(self.client.post(format!("{}{}", self.base_url, path)));
        if let Some(body) = body {
            request = request.json(body);
        }
        decode_transaction(request.send().await?).await
    }
}

async fn decode_compat_success(response: reqwest::Response) -> anyhow::Result<()> {
    let status = response.status();
    let value: serde_json::Value = response.json().await?;
    anyhow::ensure!(status.is_success(), "DTM request failed with status {status}");
    anyhow::ensure!(
        value.get("dtm_result").and_then(serde_json::Value::as_str) == Some("SUCCESS"),
        "DTM compatibility request failed"
    );
    Ok(())
}

async fn decode_transaction(response: reqwest::Response) -> anyhow::Result<Transaction> {
    let status = response.status();
    let value: serde_json::Value = response.json().await?;
    anyhow::ensure!(status.is_success(), "DTM request failed with status {status}");
    let data = value.get("data").cloned().context("DTM response did not contain data")?;
    Ok(serde_json::from_value(data)?)
}

async fn decode_callback_workflow(
    response: reqwest::Response,
) -> anyhow::Result<CallbackWorkflowSnapshot> {
    let status = response.status();
    let wire: CallbackWorkflowWire = response.json().await?;
    anyhow::ensure!(
        status.is_success(),
        "DTM callback workflow query failed with status {status}"
    );
    let result = if wire.transaction.result.is_empty() {
        Vec::new()
    } else {
        BASE64_STANDARD
            .decode(wire.transaction.result)
            .context("DTM callback workflow result is not valid base64")?
    };
    let progresses = wire
        .progresses
        .into_iter()
        .map(|progress| {
            let status = match progress.status.as_str() {
                "succeed" | "succeeded" => WorkflowProgressStatus::Succeeded,
                "failed" => WorkflowProgressStatus::Failed,
                _ => anyhow::bail!("DTM callback workflow progress has invalid status"),
            };
            let data = BASE64_STANDARD
                .decode(progress.bin_data)
                .context("DTM callback workflow progress is not valid base64")?;
            Ok(CallbackWorkflowProgress {
                branch_id: progress.branch_id,
                operation: progress.op,
                status,
                data,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(CallbackWorkflowSnapshot {
        gid: wire.transaction.gid,
        status: wire.transaction.status,
        rollback_reason: wire.transaction.rollback_reason,
        result,
        progresses,
    })
}
