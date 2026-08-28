use serde::{Deserialize, Serialize};

use crate::{current_millis, TransactionStore};

pub const TOPICS_CATEGORY: &str = "topics";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvEntry {
    #[serde(rename = "cat", alias = "category")]
    pub category: String,
    #[serde(rename = "k", alias = "key")]
    pub key: String,
    #[serde(rename = "v", alias = "value")]
    pub value: String,
    pub version: u64,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
}

impl KvEntry {
    pub fn new(
        category: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let now = current_millis();
        Self {
            category: category.into(),
            key: key.into(),
            value: value.into(),
            version: 1,
            created_at_millis: now,
            updated_at_millis: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicSubscriber {
    pub url: String,
    #[serde(default)]
    pub remark: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Topic {
    pub name: String,
    pub subscribers: Vec<TopicSubscriber>,
    pub version: u64,
}

pub async fn subscribe<S: TransactionStore + ?Sized>(
    store: &S,
    topic: &str,
    url: &str,
    remark: &str,
) -> anyhow::Result<Topic> {
    validate_topic(topic)?;
    validate_subscriber_url(url)?;
    anyhow::ensure!(remark.len() <= 1_024, "topic subscriber remark is too long");

    for _ in 0..64 {
        match store.get_kv(TOPICS_CATEGORY, topic).await? {
            None => {
                let subscribers = vec![TopicSubscriber {
                    url: url.to_owned(),
                    remark: remark.to_owned(),
                }];
                let entry = KvEntry::new(
                    TOPICS_CATEGORY,
                    topic,
                    serde_json::to_string(&subscribers)?,
                );
                if store.create_kv(entry).await? {
                    return Ok(Topic {
                        name: topic.to_owned(),
                        subscribers,
                        version: 1,
                    });
                }
            }
            Some(mut entry) => {
                let mut subscribers = decode_subscribers(&entry)?;
                anyhow::ensure!(
                    !subscribers.iter().any(|subscriber| subscriber.url == url),
                    "this url exists"
                );
                subscribers.push(TopicSubscriber {
                    url: url.to_owned(),
                    remark: remark.to_owned(),
                });
                let expected_version = entry.version;
                entry.version = entry.version.saturating_add(1);
                entry.updated_at_millis = current_millis();
                entry.value = serde_json::to_string(&subscribers)?;
                if store.update_kv(entry, expected_version).await? {
                    return Ok(Topic {
                        name: topic.to_owned(),
                        subscribers,
                        version: expected_version.saturating_add(1),
                    });
                }
            }
        }
    }
    anyhow::bail!("topic {topic} subscription update is contended")
}

pub async fn unsubscribe<S: TransactionStore + ?Sized>(
    store: &S,
    topic: &str,
    url: &str,
) -> anyhow::Result<Topic> {
    validate_topic(topic)?;
    validate_subscriber_url(url)?;

    for _ in 0..64 {
        let mut entry = store
            .get_kv(TOPICS_CATEGORY, topic)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no such a topic"))?;
        let mut subscribers = decode_subscribers(&entry)?;
        let previous_len = subscribers.len();
        subscribers.retain(|subscriber| subscriber.url != url);
        anyhow::ensure!(subscribers.len() != previous_len, "no such an url");
        let expected_version = entry.version;
        entry.version = entry.version.saturating_add(1);
        entry.updated_at_millis = current_millis();
        entry.value = serde_json::to_string(&subscribers)?;
        if store.update_kv(entry, expected_version).await? {
            return Ok(Topic {
                name: topic.to_owned(),
                subscribers,
                version: expected_version.saturating_add(1),
            });
        }
    }
    anyhow::bail!("topic {topic} subscription update is contended")
}

pub async fn get_topic<S: TransactionStore + ?Sized>(
    store: &S,
    topic: &str,
) -> anyhow::Result<Option<Topic>> {
    validate_topic(topic)?;
    store
        .get_kv(TOPICS_CATEGORY, topic)
        .await?
        .map(|entry| {
            let subscribers = decode_subscribers(&entry)?;
            Ok(Topic {
                name: entry.key,
                subscribers,
                version: entry.version,
            })
        })
        .transpose()
}

pub async fn delete_topic<S: TransactionStore + ?Sized>(
    store: &S,
    topic: &str,
) -> anyhow::Result<bool> {
    validate_topic(topic)?;
    store.delete_kv(TOPICS_CATEGORY, topic).await
}

fn decode_subscribers(entry: &KvEntry) -> anyhow::Result<Vec<TopicSubscriber>> {
    serde_json::from_str(&entry.value).map_err(Into::into)
}

fn validate_topic(topic: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !topic.is_empty() && topic.len() <= 191,
        "invalid topic name"
    );
    Ok(())
}

fn validate_subscriber_url(value: &str) -> anyhow::Result<()> {
    let url = reqwest::Url::parse(value).map_err(|_| anyhow::anyhow!("invalid subscriber URL"))?;
    anyhow::ensure!(
        value.len() <= 2_048
            && matches!(url.scheme(), "http" | "https")
            && url.host().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none(),
        "invalid subscriber URL"
    );
    Ok(())
}
