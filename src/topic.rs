//! Creating the topic, on the rare occasion that is asked for.
//!
//! Separate from the loop because it is the one thing here that *changes* the
//! cluster, and it happens once at startup or never. A canary pointed at a
//! production cluster should observe it rather than write to its control plane,
//! which is why the flag that reaches this module is off by default.

use anyhow::{Context, Result};
use kafka_admin::{Admin, NewTopic};

/// The topic to create, if it turns out to be missing.
#[derive(Debug, Clone)]
pub struct TopicSpec {
    /// The topic name.
    pub name: String,
    /// Partitions to use when creating it.
    pub partitions: i32,
    /// Replication factor to use when creating it.
    ///
    /// `i16` because that is the width of the protocol field; a value that does
    /// not fit is not a replication factor.
    pub replication_factor: i16,
}

/// Create the topic if it is missing, and say which happened.
pub async fn ensure(admin: &Admin, spec: &TopicSpec) -> Result<()> {
    let described = admin.describe_topics([spec.name.clone()]).await?;
    if described.iter().any(|(_, result)| result.is_ok()) {
        tracing::info!(topic = %spec.name, "topic already exists");
        return Ok(());
    }

    let created = admin
        .create_topics([NewTopic::new(
            &spec.name,
            spec.partitions,
            spec.replication_factor,
        )])
        .await
        .context("creating the topic")?;

    for (name, result) in &created {
        match result {
            Ok(created) => tracing::info!(
                topic = %name,
                partitions = created.partitions,
                "topic created"
            ),
            Err(error) => anyhow::bail!("could not create {name}: {error}"),
        }
    }
    Ok(())
}
