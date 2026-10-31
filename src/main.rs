//! A heartbeat producer: one timestamped Avro record per second, forever.
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
//! # Why the records are Avro
//!
//! The claim above — that a reader can tell three situations apart — assumes
//! the reader still parses the record. A JSON payload made that an assumption:
//! nothing stopped this crate renaming a field, and the only thing that
//! noticed was a test in a different repository. The schema is now registered
//! before the first beat, and the registry refuses a version that is not
//! backward compatible with the last, so the assumption is checked by the
//! thing every consumer already trusts rather than by convention.
//!
//! It is also the honest configuration for a canary that watches a Kafka
//! cluster this one is a client of: schema-registry-framed Avro is what the
//! traffic here actually looks like, and a canary writing something simpler
//! than production traffic exercises less than it appears to.
//!
//! # Failure policy
//!
//! A failed send is logged and counted; the loop continues. A canary that
//! exits on the first error stops reporting exactly when there is something to
//! report, and its silence gets attributed to the cluster.
//!
//! Startup is the exception, and the registry is part of it: see
//! [`registry`]'s module docs for why a schema that cannot be registered stops
//! the process, and why a registry that goes down afterwards does not.
//!
//! # Layout
//!
//! * [`cli`] — the command line, and the conversions out of it.
//! * [`identity`] — workload identity, read from the environment rather than
//!   asked for, and why it is the one thing here with no flag.
//! * [`token`] — the EntraID token exchange behind it, and why that lives in
//!   this crate rather than in kaas-lib.
//! * [`heartbeat`] — the payload, its schema, and the identity of whoever
//!   wrote it.
//! * [`registry`] — registering that schema, and encoding against it.
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
mod identity;
mod registry;
mod token;
mod topic;

use anyhow::{Context, Result};
use clap::Parser;
use kafka_admin::{Admin, ClusterConfig};
use kafka_conn::ConnectionConfig;
use kafka_produce::{Producer, ProducerConfig};

use crate::canary::Canary;
use crate::cli::Args;
use crate::identity::WorkloadIdentity;
use crate::registry::Encoder;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = Args::parse();
    // Both conversions before either connection, so that a malformed flag is
    // reported as a malformed flag. Checking the registry URL after
    // `Admin::connect` would mean a typo in it surfaced only once the cluster
    // was reachable — and on an unreachable cluster, never at all.
    let bootstrap = args.bootstrap_servers()?;
    let registry_url = args.schema_registry_url()?;
    let registry_trust = args.schema_registry_trust()?;
    // Also before the connection, and for the same reason: a half-set identity
    // is a configuration fault, and finding it here means the message names the
    // variable rather than the handshake it eventually broke.
    let identity = WorkloadIdentity::from_env()?;

    let mut cluster = ClusterConfig::default();
    if let Some(identity) = &identity {
        // TLS is not optional once there is a bearer token to send. OAUTHBEARER
        // over a plaintext listener puts an access token on the wire in the
        // clear, and unlike a password it is accepted by the broker from
        // whoever replays it until it expires. So the presence of an identity
        // decides the transport too — there is no flag to get this wrong with.
        cluster.connection = ConnectionConfig::new()
            .with_tls(args.tls_config()?)
            .with_sasl(identity.sasl()?);
        tracing::info!(identity = %identity.describe(), "workload identity detected");
    }

    let admin = Admin::connect(bootstrap, cluster)
        .await
        .context("connecting to the cluster")?;

    if args.create_topic {
        topic::ensure(&admin, &args.topic_spec()).await?;
    }

    // Before the producer, and fatal: see `registry`'s module docs. A canary
    // that cannot encode has nothing to say, and finding that out now is
    // cheaper than finding it out once a second for a week. The token, when
    // the registry wants one, is fatal on the same terms.
    let registry_token = match &identity {
        Some(identity) => identity.registry_token().await?,
        None => None,
    };
    let encoder = Encoder::register(
        registry_url,
        &args.topic,
        registry_token.as_deref(),
        &registry_trust,
    )
    .await?;

    let config = ProducerConfig::new().acks(args.acks());
    let producer = Producer::new(admin.cluster().clone(), config);
    let mut canary = Canary::new(args.schedule());

    tracing::info!(
        topic = %args.topic,
        interval_ms = args.interval_ms,
        run_id = %canary.run_id(),
        host = %canary.host(),
        acks = args.acks_label(),
        subject = encoder.subject(),
        // Worth a field of its own: "which listener did this actually reach"
        // is the first question about any heartbeat gap, and the answer is not
        // recoverable from the bootstrap address alone.
        auth = if identity.is_some() {
            "oauthbearer"
        } else {
            "none"
        },
        "canary starting"
    );

    // Scoped so the loop's borrow of `canary` ends with the future, leaving the
    // counters readable below whether the loop returned or ctrl-c cancelled it.
    {
        let beating = canary.beat(&producer, &encoder);
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
