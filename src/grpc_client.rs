use roze_context::Context;
use roze_grpc::transport::{Channel, Endpoint, Request};

use crate::pb::dtmgimp::{
    dtm_client::DtmClient as ProtoDtmClient, DtmBranchRequest, DtmProgressesReply, DtmRequest,
    DtmTopicRequest,
};

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
