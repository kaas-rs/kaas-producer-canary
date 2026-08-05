//! The command line, and the only place that knows there is one.
//!
//! The conversions below are the point of the module. `Args` is shaped by what
//! is convenient to type at a shell — a comma-separated string, a pair of flags
//! that only matter together, a boolean standing in for an enum — and none of
//! that shape should reach the code that writes records. Converting once at the
//! boundary means the loop takes a [`Schedule`] it can satisfy rather than an
//! `Args` it has to interpret.

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use kafka_produce::Acks;

use crate::canary::Schedule;
use crate::topic::TopicSpec;

#[derive(Debug, Parser)]
#[command(
    name = "kaas-producer-canary",
    about = "Write a timestamped JSON heartbeat to a Kafka topic on an interval."
)]
pub struct Args {
    /// Comma-separated bootstrap servers.
    #[arg(long, env = "KAAS_CANARY_BOOTSTRAP")]
    pub bootstrap: String,

    /// The topic to write to.
    #[arg(long, env = "KAAS_CANARY_TOPIC", default_value = "kaas-canary")]
    pub topic: String,

    /// How long between heartbeats.
    #[arg(long, env = "KAAS_CANARY_INTERVAL_MS", default_value_t = 1_000)]
    pub interval_ms: u64,

    /// Create the topic if it does not exist.
    ///
    /// Off by default. A canary pointed at a production cluster should observe
    /// it, not change it — and a typo in `--topic` with creation on silently
    /// makes a new topic instead of failing.
    #[arg(long, env = "KAAS_CANARY_CREATE_TOPIC")]
    pub create_topic: bool,

    /// Partitions to use when creating the topic.
    #[arg(long, default_value_t = 1)]
    pub partitions: i32,

    /// Replication factor to use when creating the topic.
    ///
    /// `i16` because that is the width of the protocol field; a value that
    /// does not fit is not a replication factor.
    #[arg(long, default_value_t = 1)]
    pub replication_factor: i16,

    /// Write every record to this partition, rather than letting the
    /// partitioner choose.
    #[arg(long)]
    pub partition: Option<i32>,

    /// Stop after this many heartbeats. Runs forever when unset.
    #[arg(long)]
    pub count: Option<u64>,

    /// Require only the leader to acknowledge, rather than the full ISR.
    ///
    /// Faster and weaker. Note it also means a heartbeat is acknowledged
    /// before a consumer can read it, since the high watermark does not
    /// advance until the replicas have it — so a reader tailing the topic
    /// will appear to lag.
    #[arg(long)]
    pub acks_leader: bool,
}

impl Args {
    /// The bootstrap list, split and cleaned.
    ///
    /// Empty is an error rather than an empty `Vec`, because a client handed no
    /// addresses fails later, deeper, and with a message about connections
    /// instead of about the flag that was wrong.
    pub fn bootstrap_servers(&self) -> Result<Vec<String>> {
        let servers: Vec<String> = self
            .bootstrap
            .split(',')
            .map(str::trim)
            .filter(|server| !server.is_empty())
            .map(str::to_owned)
            .collect();
        if servers.is_empty() {
            anyhow::bail!("--bootstrap resolved to no addresses");
        }
        Ok(servers)
    }

    /// What the heartbeat loop needs.
    pub fn schedule(&self) -> Schedule {
        Schedule {
            topic: self.topic.clone(),
            // A zero-millisecond interval is a tight loop writing as fast as
            // the cluster will take it, which is a load generator rather than a
            // canary — and it would be one pointed at production.
            interval: Duration::from_millis(self.interval_ms.max(1)),
            partition: self.partition,
            count: self.count,
        }
    }

    /// What to create, if `--create-topic` was given.
    pub fn topic_spec(&self) -> TopicSpec {
        TopicSpec {
            name: self.topic.clone(),
            partitions: self.partitions,
            replication_factor: self.replication_factor,
        }
    }

    /// How many replicas must acknowledge a write.
    ///
    /// `acks=0` is not reachable from here because `kafka-produce` does not
    /// offer it, which is the right answer for a canary anyway: a write nobody
    /// acknowledged tells you nothing about whether it happened.
    pub fn acks(&self) -> Acks {
        if self.acks_leader {
            Acks::Leader
        } else {
            Acks::All
        }
    }

    /// The acknowledgement setting, for the log line that records it.
    pub fn acks_label(&self) -> &'static str {
        if self.acks_leader { "leader" } else { "all" }
    }
}
