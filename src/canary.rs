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
use crate::registry::Encoder;

/// The build writing the heartbeats, taken from the manifest at compile time
/// so it cannot drift from the binary the way a hand-maintained constant does.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// How the canary beats.
///
/// Everything the loop needs and nothing it does not. Built from the command
/// line at the boundary rather than threaded through it, so this module does
/// not know that a command line exists — and so a new flag cannot start
/// influencing the loop without somebody writing the line that lets it.
///
/// Built the way kaas-lib's own types are — see `STYLE.md`. The topic and the
/// interval are in `new` because a schedule without either is not one; the
/// rest arrive as `Option`s from flags that may not have been given, so they
/// are `with_maybe_*` setters that relay rather than decide.
#[derive(Debug, Clone)]
pub struct Schedule {
    /// The topic to write to.
    topic: String,
    /// How long between heartbeats.
    interval: Duration,
    /// Write every record to this partition, rather than letting the
    /// partitioner choose.
    partition: Option<i32>,
    /// Stop after this many heartbeats. Runs forever when `None`.
    count: Option<u64>,
}

impl Schedule {
    /// Beat on `topic`, every `interval`.
    pub fn new(topic: impl Into<String>, interval: Duration) -> Self {
        Self {
            topic: topic.into(),
            interval,
            partition: None,
            count: None,
        }
    }

    /// Pin every record to a partition, when one was chosen for us.
    ///
    /// `None` leaves the choice to the partitioner, which for a keyless record
    /// means sticking to one partition anyway — but one the cluster picked.
    #[must_use]
    pub fn with_maybe_partition(mut self, partition: Option<i32>) -> Self {
        self.partition = partition;
        self
    }

    /// Stop after this many heartbeats, when a limit was given.
    #[must_use]
    pub fn with_maybe_count(mut self, count: Option<u64>) -> Self {
        self.count = count;
        self
    }
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
    pub async fn beat(&mut self, producer: &Producer, encoder: &Encoder) -> Result<()> {
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

            // No HTTP happens here: the encoder resolved and cached the schema
            // at startup, so this is Avro serialisation and a five-byte prefix.
            // A failure means the record does not match the schema, which is a
            // bug in this crate rather than a fault in the cluster — counted
            // as a failure all the same, because the beat did not happen and
            // `sequence` has already moved on.
            let payload = match encoder.encode(&heartbeat).await {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::error!(%error, sequence, "could not encode a heartbeat");
                    self.failed += 1;
                    continue;
                }
            };

            let record = ProducerRecord::new(&self.schedule.topic)
                .with_value(payload)
                // Avro under Confluent's framing: a zero byte, the schema id
                // big-endian, then the datum. Named rather than
                // `application/avro` because the prefix is the half a reader
                // gets wrong, and a plain `avro/binary` would not warn them
                // that the first five bytes are not part of the datum.
                .with_header("content-type", "application/vnd.confluent.avro+binary")
                .with_header("canary-run", self.run_id.clone())
                // Which build wrote this. A canary's own version changes what
                // the stream looks like — idempotence arriving in kaas-lib
                // 0.3.0 quietly removed a class of `sequence` gap — so a reader
                // comparing two periods needs to know whether it is comparing
                // two clusters or two canaries.
                .with_header("canary-version", VERSION)
                // The record timestamp and the one in the body are set from the
                // same reading, so a consumer can tell broker-side rewriting
                // (LogAppendTime) from clock skew by comparing them.
                .with_timestamp(heartbeat.timestamp_ms)
                .with_maybe_partition(self.schedule.partition);

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
