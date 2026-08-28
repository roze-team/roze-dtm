use std::collections::BTreeMap;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::{BranchKind, KvEntry, Transaction, TransactionKind, TransactionOptions};

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
