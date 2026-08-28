use roze_context::Context;
use roze_dtm::{
    grpc_client::DtmGrpcClient,
    pb::dtmgimp::{DtmRequest, DtmTopicRequest, DtmTransOptions},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = std::env::var("ROZE_DTM_GRPC_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:36791".to_owned());
    let token = std::env::var("ROZE_DTM_CONTROL_TOKEN")?;
    let mut client = DtmGrpcClient::connect(endpoint)
        .await?
        .with_bearer_token(token);
    let context = Context::background();

    let gid = client.new_gid(&context).await?;
    let request = DtmRequest {
        gid: gid.clone(),
        trans_type: "msg".to_owned(),
        trans_options: Some(DtmTransOptions {
            wait_result: true,
            ..DtmTransOptions::default()
        }),
        steps: "[]".to_owned(),
        ..DtmRequest::default()
    };
    client.prepare(&context, request.clone()).await?;
    client.submit(&context, request).await?;

    let topic = format!("grpc-smoke-{gid}");
    let subscription = DtmTopicRequest {
        topic: topic.clone(),
        url: "http://127.0.0.1:18091/events".to_owned(),
        remark: "local gRPC smoke".to_owned(),
    };
    client.subscribe(&context, subscription.clone()).await?;
    client.unsubscribe(&context, subscription).await?;
    client
        .delete_topic(
            &context,
            DtmTopicRequest {
                topic,
                ..DtmTopicRequest::default()
            },
        )
        .await?;

    println!("grpc smoke passed gid={gid}");
    Ok(())
}
