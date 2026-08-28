use std::collections::BTreeMap;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::{BranchKind, Transaction, TransactionKind};

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

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.bearer_token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
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

async fn decode_transaction(response: reqwest::Response) -> anyhow::Result<Transaction> {
    let status = response.status();
    let value: serde_json::Value = response.json().await?;
    anyhow::ensure!(status.is_success(), "DTM request failed with status {status}");
    let data = value.get("data").cloned().context("DTM response did not contain data")?;
    Ok(serde_json::from_value(data)?)
}
