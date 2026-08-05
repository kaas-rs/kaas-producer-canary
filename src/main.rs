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
//! Reading the stream back and checking those properties is
//! [`kaas-consumer-canary`](https://github.com/kaas-rs/kaas-consumer-canary)'s
//! job. This end can only claim; an acknowledgement describes the leader's log
//! at one instant, not what a reader finds there later.
//!
//! # Failure policy
//!
//! A failed send is logged and counted; the loop continues. A canary that
//! exits on the first error stops reporting exactly when there is something to
//! report, and its silence gets attributed to the cluster.
//!
//! # Layout
//!
//! * [`cli`] — the command line, and the conversions out of it.
//! * [`heartbeat`] — the payload and the identity of whoever wrote it.
//! * [`canary`] — the loop.
//! * [`topic`] — creating the topic, when that is asked for.
//!
//! This file is the wiring and the shutdown, and nothing else.

#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
// Tests may unwrap freely: a panic there is a failed assertion, which is the
// mechanism, not a crash in a process that is meant to run for weeks.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used, clippy::panic))]

mod canary;
mod cli;
mod heartbeat;
mod topic;

use anyhow::{Context, Result};
use clap::Parser;
use kafka_admin::{Admin, ClusterConfig};
use kafka_produce::{Producer, ProducerConfig};

use crate::canary::Canary;
use crate::cli::Args;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = Args::parse();
    let bootstrap = args.bootstrap_servers()?;

    let admin = Admin::connect(bootstrap, ClusterConfig::default())
        .await
        .context("connecting to the cluster")?;

    if args.create_topic {
        topic::ensure(&admin, &args.topic_spec()).await?;
    }

    let config = ProducerConfig::new().acks(args.acks());
    let producer = Producer::new(admin.cluster().clone(), config);
    let mut canary = Canary::new(args.schedule());

    tracing::info!(
        topic = %args.topic,
        interval_ms = args.interval_ms,
        run_id = %canary.run_id(),
        host = %canary.host(),
        acks = args.acks_label(),
        "canary starting"
    );

    // Scoped so the loop's borrow of `canary` ends with the future, leaving the
    // counters readable below whether the loop returned or ctrl-c cancelled it.
    {
        let beating = canary.beat(&producer);
        tokio::select! {
            result = beating => result?,
            signal = tokio::signal::ctrl_c() => {
                signal.context("waiting for ctrl-c")?;
                tracing::info!("interrupted");
            }
        }
    }

    tracing::info!(
        sent = canary.sent(),
        failed = canary.failed(),
        "canary stopped"
    );
    Ok(())
}

/// `info` and above unless `RUST_LOG` says otherwise.
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}
