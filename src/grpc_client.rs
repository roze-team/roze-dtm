use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use roze_context::Context;
use roze_grpc::transport::{Channel, Endpoint, Request};

use crate::pb::dtmgimp::{
    dtm_client::DtmClient as ProtoDtmClient, DtmBranchRequest, DtmProgressesReply, DtmRequest,
    DtmTopicRequest,
};
use crate::WorkflowProgressStatus;

#[derive(Debug, Clone)]
pub struct DtmGrpcClient {
    inner: ProtoDtmClient<Channel>,
    bearer_token: Option<String>,
}

impl DtmGrpcClient {
    pub async fn connect(endpoint: impl Into<String>) -> anyhow::Result<Self> {
        let channel = Endpoint::from_shared(endpoint.into())?.connect().await?;
        Ok(Self {
            inner: ProtoDtmClient::new(channel),
            bearer_token: None,
        })
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: ProtoDtmClient::new(channel),
            bearer_token: None,
        }
    }

    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    pub async fn new_gid(&mut self, context: &Context) -> anyhow::Result<String> {
        let request = self.request(context, ())?;
        Ok(self
            .inner
            .new_gid(request)
            .await?
            .into_inner()
            .gid)
    }

    pub async fn submit(&mut self, context: &Context, input: DtmRequest) -> anyhow::Result<()> {
        let request = self.request(context, input)?;
        self.inner.submit(request).await?;
        Ok(())
    }

    pub async fn prepare(&mut self, context: &Context, input: DtmRequest) -> anyhow::Result<()> {
        let request = self.request(context, input)?;
        self.inner.prepare(request).await?;
        Ok(())
    }

    pub async fn abort(&mut self, context: &Context, input: DtmRequest) -> anyhow::Result<()> {
        let request = self.request(context, input)?;
        self.inner.abort(request).await?;
        Ok(())
    }

    pub async fn register_branch(
        &mut self,
        context: &Context,
        input: DtmBranchRequest,
    ) -> anyhow::Result<()> {
        let request = self.request(context, input)?;
        self.inner.register_branch(request).await?;
        Ok(())
    }

    pub async fn record_callback_workflow_progress(
        &mut self,
        context: &Context,
        gid: impl Into<String>,
        branch_id: impl Into<String>,
        operation: impl Into<String>,
        status: WorkflowProgressStatus,
        data: Vec<u8>,
    ) -> anyhow::Result<()> {
        let status = match status {
            WorkflowProgressStatus::Succeeded => "succeed",
            WorkflowProgressStatus::Failed => "failed",
        };
        self.register_branch(
            context,
            DtmBranchRequest {
                gid: gid.into(),
                trans_type: "workflow".to_owned(),
                branch_id: branch_id.into(),
                op: operation.into(),
                data: [("status".to_owned(), status.to_owned())]
                    .into_iter()
                    .collect(),
                busi_payload: data,
            },
        )
        .await
    }

    pub async fn finish_callback_workflow(
        &mut self,
        context: &Context,
        gid: impl Into<String>,
        status: WorkflowProgressStatus,
        rollback_reason: impl Into<String>,
        result: &[u8],
    ) -> anyhow::Result<()> {
        let status = match status {
            WorkflowProgressStatus::Succeeded => "succeed",
            WorkflowProgressStatus::Failed => "failed",
        };
        self.submit(
            context,
            DtmRequest {
                gid: gid.into(),
                trans_type: "workflow".to_owned(),
                req_extra: [
                    ("status".to_owned(), status.to_owned()),
                    ("rollback_reason".to_owned(), rollback_reason.into()),
                    ("result".to_owned(), BASE64_STANDARD.encode(result)),
                ]
                .into_iter()
                .collect(),
                ..DtmRequest::default()
            },
        )
        .await
    }

    pub async fn prepare_workflow(
        &mut self,
        context: &Context,
        input: DtmRequest,
    ) -> anyhow::Result<DtmProgressesReply> {
        let request = self.request(context, input)?;
        Ok(self
            .inner
            .prepare_workflow(request)
            .await?
            .into_inner())
    }

    pub async fn query_callback_workflow(
        &mut self,
        context: &Context,
        gid: impl Into<String>,
        query_prepared: impl Into<String>,
        custom_data: impl Into<String>,
    ) -> anyhow::Result<DtmProgressesReply> {
        self.prepare_workflow(
            context,
            DtmRequest {
                gid: gid.into(),
                trans_type: "workflow".to_owned(),
                customed_data: custom_data.into(),
                query_prepared: query_prepared.into(),
                ..DtmRequest::default()
            },
        )
        .await
    }

    pub async fn prepare_named_callback_workflow(
        &mut self,
        context: &Context,
        gid: impl Into<String>,
        query_prepared: impl Into<String>,
        workflow_name: &str,
        data: &[u8],
    ) -> anyhow::Result<DtmProgressesReply> {
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
        self.query_callback_workflow(context, gid, query_prepared, custom_data)
            .await
    }

    pub async fn subscribe(
        &mut self,
        context: &Context,
        input: DtmTopicRequest,
    ) -> anyhow::Result<()> {
        let request = self.request(context, input)?;
        self.inner.subscribe(request).await?;
        Ok(())
    }

    pub async fn unsubscribe(
        &mut self,
        context: &Context,
        input: DtmTopicRequest,
    ) -> anyhow::Result<()> {
        let request = self.request(context, input)?;
        self.inner.unsubscribe(request).await?;
        Ok(())
    }

    pub async fn delete_topic(
        &mut self,
        context: &Context,
        input: DtmTopicRequest,
    ) -> anyhow::Result<()> {
        let request = self.request(context, input)?;
        self.inner.delete_topic(request).await?;
        Ok(())
    }

    fn request<T>(&self, context: &Context, value: T) -> anyhow::Result<Request<T>> {
        let mut request = roze_rpc::rpc::client_request(
            value,
            context,
            roze_rpc::rpc::RpcClientOptions::default(),
            None,
        );
        if let Some(token) = &self.bearer_token {
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {token}").parse()?,
            );
        }
        Ok(request)
    }
}
