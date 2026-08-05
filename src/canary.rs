//! The heartbeat loop, and the counters it keeps.
//!
//! The counters are plain integers behind `&mut self` rather than shared
//! atomics. Nothing here is concurrent — one loop writes them and `main` reads
//! them once the loop is over — so the sharing was never real, and an `Arc<
//! AtomicU64>` that is only ever touched from one task is a claim about the
//! program that is not true.

use std::time::Duration;

use anyhow::Result;
use kafka_produce::{Producer, ProducerRecord};

use crate::heartbeat::{self, Heartbeat};

/// How the canary beats.
///
/// Everything the loop needs and nothing it does not. Built from the command
/// line at the boundary rather than threaded through it, so this module does
/// not know that a command line exists — and so a new flag cannot start
/// influencing the loop without somebody writing the line that lets it.
#[derive(Debug, Clone)]
pub struct Schedule {
    /// The topic to write to.
    pub topic: String,
    /// How long between heartbeats.
    pub interval: Duration,
    /// Write every record to this partition, rather than letting the
    /// partitioner choose.
    pub partition: Option<i32>,
    /// Stop after this many heartbeats. Runs forever when `None`.
    pub count: Option<u64>,
}

/// A canary, mid-flight.
pub struct Canary {
    schedule: Schedule,
    run_id: String,
    host: String,
    sent: u64,
    failed: u64,
}

impl Canary {
    /// Identify this run and prepare to beat.
    pub fn new(schedule: Schedule) -> Self {
        Self {
            schedule,
            run_id: heartbeat::run_id(),
            host: heartbeat::hostname(),
            sent: 0,
            failed: 0,
        }
    }

    /// Which run this is. Changes on every restart.
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Where the canary is running.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Heartbeats the cluster acknowledged.
    pub fn sent(&self) -> u64 {
        self.sent
    }

    /// Heartbeats it did not.
    pub fn failed(&self) -> u64 {
        self.failed
    }

    /// The heartbeat loop.
    ///
    /// Returns when `Schedule::count` is reached, and otherwise not at all —
    /// the caller ends it by dropping the future.
    pub async fn beat(&mut self, producer: &Producer) -> Result<()> {
        let mut ticker = tokio::time::interval(self.schedule.interval);
        // If a send takes longer than the interval — a leader election will do
        // it — catching up by firing the missed ticks back to back would write
        // a burst of records carrying nearly identical timestamps. Skip them:
        // the gap is the signal, and `sequence` still records that nothing was
        // lost.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut sequence = 0u64;
        loop {
            ticker.tick().await;
            sequence = sequence.saturating_add(1);

            let heartbeat = Heartbeat::now(&self.run_id, &self.host, sequence);

            let payload = match serde_json::to_vec(&heartbeat) {
                Ok(payload) => payload,
                Err(error) => {
                    // Serialising our own struct cannot realistically fail, but
                    // rule 2 says a library does not get to find out the hard
                    // way.
                    tracing::error!(%error, sequence, "could not serialise a heartbeat");
                    self.failed += 1;
                    continue;
                }
            };

            let mut record = ProducerRecord::new(&self.schedule.topic)
                .value(payload)
                .header("content-type", "application/json")
                .header("canary-run", self.run_id.clone())
                // The record timestamp and the one in the body are set from the
                // same reading, so a consumer can tell broker-side rewriting
                // (LogAppendTime) from clock skew by comparing them.
                .timestamp(heartbeat.timestamp_ms);
            if let Some(partition) = self.schedule.partition {
                record = record.partition(partition);
            }

            match producer.send(record).await {
                Ok(metadata) => {
                    self.sent += 1;
                    tracing::info!(
                        sequence,
                        partition = metadata.partition,
                        offset = metadata.offset,
                        "beat"
                    );
                }
                Err(error) => {
                    // Deliberately not fatal. See the crate docs: a canary that
                    // exits on the first error stops reporting exactly when
                    // there is something to report.
                    self.failed += 1;
                    tracing::warn!(sequence, %error, "heartbeat failed");
                }
            }

            if let Some(limit) = self.schedule.count
                && sequence >= limit
            {
                return Ok(());
            }
        }
    }
}
