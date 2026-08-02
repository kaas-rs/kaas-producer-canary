//! A heartbeat producer: one timestamped JSON record per second, forever.
//!
//! # What makes it a canary rather than a clock
//!
//! Posting the time is easy. What makes the stream *diagnostic* is the
//! `sequence` field: it increases by exactly one per record, independently of
//! the clock, so a consumer can tell three situations apart that otherwise
//! look identical in a topic full of timestamps.
//!
//! * **A gap in `sequence`** — records were lost, or a produce failed and was
//!   reported. Timestamps alone cannot show this, because the next record
//!   still carries a plausible time.
//! * **A gap in time with no gap in `sequence`** — the canary was stopped and
//!   restarted, or blocked. Nothing was lost; the writer paused.
//! * **A repeat of `sequence`** — a duplicate, which is the thing a
//!   non-idempotent producer is allowed to do on a retry and which
//!   `kafka-produce` deliberately refuses to cause. Seeing one here would be
//!   a real finding.
//!
//! `run_id` separates those further: it changes on every restart, so two
//! processes writing to the same topic never look like one with a sequence
//! anomaly.
//!
//! # Failure policy
//!
//! A failed send is logged and counted; the loop continues. A canary that
//! exits on the first error stops reporting exactly when there is something to
//! report, and its silence gets attributed to the cluster.

#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use kafka_admin::{Admin, ClusterConfig, NewTopic};
use kafka_produce::{Acks, Producer, ProducerConfig, ProducerRecord};
use serde::Serialize;

/// One heartbeat, as it appears on the topic.
#[derive(Debug, Serialize)]
struct Heartbeat {
    /// Which run of the canary wrote this. Changes on every restart.
    run_id: String,
    /// Increases by exactly one per record within a run. The diagnostic field.
    sequence: u64,
    /// Milliseconds since the epoch — what a machine should sort on.
    timestamp_ms: i64,
    /// RFC 3339 with milliseconds — what a human should read.
    timestamp: String,
    /// Where the canary is running.
    host: String,
}

#[derive(Debug, Parser)]
#[command(
    name = "kaas-producer-canary",
    about = "Write a timestamped JSON heartbeat to a Kafka topic on an interval."
)]
struct Args {
    /// Comma-separated bootstrap servers.
    #[arg(long, env = "KAAS_CANARY_BOOTSTRAP")]
    bootstrap: String,

    /// The topic to write to.
    #[arg(long, env = "KAAS_CANARY_TOPIC", default_value = "kaas-canary")]
    topic: String,

    /// How long between heartbeats.
    #[arg(long, env = "KAAS_CANARY_INTERVAL_MS", default_value_t = 1_000)]
    interval_ms: u64,

    /// Create the topic if it does not exist.
    ///
    /// Off by default. A canary pointed at a production cluster should observe
    /// it, not change it — and a typo in `--topic` with creation on silently
    /// makes a new topic instead of failing.
    #[arg(long, env = "KAAS_CANARY_CREATE_TOPIC")]
    create_topic: bool,

    /// Partitions to use when creating the topic.
    #[arg(long, default_value_t = 1)]
    partitions: i32,

    /// Replication factor to use when creating the topic.
    ///
    /// `i16` because that is the width of the protocol field; a value that
    /// does not fit is not a replication factor.
    #[arg(long, default_value_t = 1)]
    replication_factor: i16,

    /// Write every record to this partition, rather than letting the
    /// partitioner choose.
    #[arg(long)]
    partition: Option<i32>,

    /// Stop after this many heartbeats. Runs forever when unset.
    #[arg(long)]
    count: Option<u64>,

    /// Require only the leader to acknowledge, rather than the full ISR.
    ///
    /// Faster and weaker. Note it also means a heartbeat is acknowledged
    /// before a consumer can read it, since the high watermark does not
    /// advance until the replicas have it — so a reader tailing the topic
    /// will appear to lag.
    #[arg(long)]
    acks_leader: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let bootstrap: Vec<String> = args
        .bootstrap
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if bootstrap.is_empty() {
        anyhow::bail!("--bootstrap resolved to no addresses");
    }

    let admin = Admin::connect(bootstrap.clone(), ClusterConfig::default())
        .await
        .context("connecting to the cluster")?;

    if args.create_topic {
        ensure_topic(&admin, &args).await?;
    }

    let config = ProducerConfig::new().acks(if args.acks_leader {
        Acks::Leader
    } else {
        Acks::All
    });
    let producer = Producer::new(admin.cluster().clone(), config);

    let run_id = run_id();
    let host = hostname();

    tracing::info!(
        topic = %args.topic,
        interval_ms = args.interval_ms,
        %run_id,
        %host,
        acks = if args.acks_leader { "leader" } else { "all" },
        "canary starting"
    );

    let sent = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));

    let beating = beat(&producer, &args, &run_id, &host, &sent, &failed);

    tokio::select! {
        result = beating => result?,
        signal = tokio::signal::ctrl_c() => {
            signal.context("waiting for ctrl-c")?;
            tracing::info!("interrupted");
        }
    }

    tracing::info!(
        sent = sent.load(Ordering::Relaxed),
        failed = failed.load(Ordering::Relaxed),
        "canary stopped"
    );
    Ok(())
}

/// The heartbeat loop.
async fn beat(
    producer: &Producer,
    args: &Args,
    run_id: &str,
    host: &str,
    sent: &AtomicU64,
    failed: &AtomicU64,
) -> Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_millis(args.interval_ms.max(1)));
    // If a send takes longer than the interval — a leader election will do it
    // — catching up by firing the missed ticks back to back would write a
    // burst of records carrying nearly identical timestamps. Skip them: the
    // gap is the signal, and `sequence` still records that nothing was lost.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut sequence = 0u64;
    loop {
        ticker.tick().await;
        sequence = sequence.saturating_add(1);

        let now = Utc::now();
        let heartbeat = Heartbeat {
            run_id: run_id.to_owned(),
            sequence,
            timestamp_ms: now.timestamp_millis(),
            timestamp: now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            host: host.to_owned(),
        };

        let payload = match serde_json::to_vec(&heartbeat) {
            Ok(payload) => payload,
            Err(error) => {
                // Serialising our own struct cannot realistically fail, but
                // rule 2 says a library does not get to find out the hard way.
                tracing::error!(%error, sequence, "could not serialise a heartbeat");
                failed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        let mut record = ProducerRecord::new(&args.topic)
            .value(payload)
            .header("content-type", "application/json")
            .header("canary-run", run_id.to_owned())
            // The record timestamp and the one in the body are set from the
            // same reading, so a consumer can tell broker-side rewriting
            // (LogAppendTime) from clock skew by comparing them.
            .timestamp(heartbeat.timestamp_ms);
        if let Some(partition) = args.partition {
            record = record.partition(partition);
        }

        match producer.send(record).await {
            Ok(metadata) => {
                sent.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    sequence,
                    partition = metadata.partition,
                    offset = metadata.offset,
                    "beat"
                );
            }
            Err(error) => {
                // Deliberately not fatal. See the module docs: a canary that
                // exits on the first error stops reporting exactly when there
                // is something to report.
                failed.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(sequence, %error, "heartbeat failed");
            }
        }

        if let Some(limit) = args.count
            && sequence >= limit
        {
            return Ok(());
        }
    }
}

/// Create the topic if it is missing, and say which happened.
async fn ensure_topic(admin: &Admin, args: &Args) -> Result<()> {
    let described = admin.describe_topics([args.topic.clone()]).await?;
    if described.iter().any(|(_, result)| result.is_ok()) {
        tracing::info!(topic = %args.topic, "topic already exists");
        return Ok(());
    }

    let created = admin
        .create_topics([NewTopic::new(
            &args.topic,
            args.partitions,
            args.replication_factor,
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

/// An identifier for this run of the canary.
///
/// Wall-clock nanoseconds and the process id together, rather than either
/// alone: two canaries started in the same millisecond would share a
/// millisecond timestamp, and a pid is reused freely across restarts and
/// identical across containers. The pair is what makes "two writers on one
/// topic" distinguishable from "one writer with a sequence anomaly".
fn run_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}-{:x}", std::process::id())
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}
