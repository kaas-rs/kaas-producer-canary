//! Creating the topic, on the rare occasion that is asked for.
//!
//! Separate from the loop because it is the one thing here that *changes* the
//! cluster, and it happens once at startup or never. A canary pointed at a
//! production cluster should observe it rather than write to its control plane,
//! which is why the flag that reaches this module is off by default.

use anyhow::{Context, Result};
use kafka_admin::{Admin, NewTopic};

/// The topic to create, if it turns out to be missing.
///
/// Built the way kaas-lib's own types are — see `STYLE.md`. Only the name is
/// required: a single-partition, single-replica topic is what a canary wants
/// on a cluster that has not been told otherwise, so both of the others have a
/// defensible default and are therefore setters.
#[derive(Debug, Clone)]
pub struct TopicSpec {
    /// The topic name.
    name: String,
    /// Partitions to use when creating it.
    partitions: i32,
    /// Replication factor to use when creating it.
    ///
    /// `i16` because that is the width of the protocol field; a value that does
    /// not fit is not a replication factor.
    replication_factor: i16,
}

impl TopicSpec {
    /// A single-partition, single-replica topic called `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partitions: 1,
            replication_factor: 1,
        }
    }

    /// Create it with this many partitions.
    #[must_use]
    pub fn with_partitions(mut self, partitions: i32) -> Self {
        self.partitions = partitions;
        self
    }

    /// Create it with this replication factor.
    #[must_use]
    pub fn with_replication_factor(mut self, replication_factor: i16) -> Self {
        self.replication_factor = replication_factor;
        self
    }
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
