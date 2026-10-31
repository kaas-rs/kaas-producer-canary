//! The command line, and the only place that knows there is one.
//!
//! The conversions below are the point of the module. `Args` is shaped by what
//! is convenient to type at a shell — a comma-separated string, a pair of flags
//! that only matter together, a boolean standing in for an enum — and none of
//! that shape should reach the code that writes records. Converting once at the
//! boundary means the loop takes a [`Schedule`] it can satisfy rather than an
//! `Args` it has to interpret.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kafka_conn::TlsConfig;
use kafka_produce::Acks;

use crate::canary::Schedule;
use crate::registry::RegistryTrust;
use crate::topic::TopicSpec;

#[derive(Debug, Parser)]
#[command(
    name = "kaas-producer-canary",
    about = "Write a timestamped Avro heartbeat to a Kafka topic on an interval."
)]
pub struct Args {
    /// Comma-separated bootstrap servers.
    #[arg(long, env = "KAAS_CANARY_BOOTSTRAP")]
    pub bootstrap: String,

    /// The topic to write to.
    #[arg(long, env = "KAAS_CANARY_TOPIC", default_value = "kaas-canary")]
    pub topic: String,

    /// Base URL of the Confluent-compatible schema registry.
    ///
    /// Required, and without a default on purpose. Every record is Avro framed
    /// with a schema id, so there is no format to fall back to when this is
    /// missing — a canary that started anyway would be writing bytes nothing
    /// can read. Apicurio serves this API under `/apis/ccompat/v7`.
    #[arg(long, env = "KAAS_CANARY_SCHEMA_REGISTRY")]
    pub schema_registry: String,

    /// PEM bundle to trust for an `https://` schema registry.
    ///
    /// Its own flag rather than `--ca-file`, because the two are usually
    /// different CAs: Strimzi signs its listeners with a per-cluster CA, and
    /// the registry's certificate comes from wherever the platform issues
    /// service certificates. Nothing joins the two.
    ///
    /// Unset means the system roots — of which a scratch image has none, so
    /// an in-cluster registry over TLS needs this. Naming it for an `http://`
    /// registry is refused rather than ignored: the likelier story is a URL
    /// that lost its `s`.
    #[arg(long, env = "KAAS_CANARY_SCHEMA_REGISTRY_CA_FILE")]
    pub schema_registry_ca_file: Option<PathBuf>,

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

    /// PEM bundle to trust for the Kafka connection.
    ///
    /// The one piece of the TLS story the environment cannot supply, which is
    /// why it is a flag when nothing else about authentication is. A broker
    /// behind a private CA — Strimzi signs its listeners with a per-cluster CA
    /// of its own — is not in any system trust store, and there is no
    /// convention that says where its certificate was mounted.
    ///
    /// Unset means the system roots, which is right for a broker with a
    /// publicly-issued certificate and wrong for Strimzi. The symptom of
    /// forgetting it is a TLS handshake failure at connect, not a later
    /// authentication error.
    #[arg(long, env = "KAAS_CANARY_CA_FILE")]
    pub ca_file: Option<PathBuf>,
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

    /// The registry URL, checked for a scheme the client can speak.
    ///
    /// Anything but `http://` or `https://` would fail inside reqwest, at
    /// registration, with a message about URL schemes. Catching it at the
    /// boundary instead means the error names the flag that was wrong, which
    /// is the same reason `bootstrap_servers` rejects an empty list here rather
    /// than letting a client fail deeper.
    pub fn schema_registry_url(&self) -> Result<&str> {
        let url = self.schema_registry.trim();
        if url.is_empty() {
            anyhow::bail!("--schema-registry is empty");
        }
        if url.starts_with("http://") {
            if self.schema_registry_ca_file.is_some() {
                anyhow::bail!(
                    "--schema-registry-ca-file is set but --schema-registry is \
                     plain http ({url:?}); did the URL lose its https?"
                );
            }
        } else if !url.starts_with("https://") {
            anyhow::bail!("--schema-registry must be an http:// or https:// URL, got {url:?}");
        }
        Ok(url)
    }

    /// What to verify an `https://` registry against.
    ///
    /// Read and parsed here, for the reason [`Args::tls_config`] reads its
    /// bundle here: an unreadable or empty file is reported as the flag that
    /// named it, at startup, rather than as a handshake failure at
    /// registration.
    pub fn schema_registry_trust(&self) -> Result<RegistryTrust> {
        let Some(path) = &self.schema_registry_ca_file else {
            return Ok(RegistryTrust::System);
        };
        let pem = std::fs::read(path)
            .with_context(|| format!("reading --schema-registry-ca-file {}", path.display()))?;
        RegistryTrust::from_pem(&pem)
            .with_context(|| format!("parsing --schema-registry-ca-file {}", path.display()))
    }

    /// What to trust for the Kafka connection.
    ///
    /// Read here rather than inside the connection so that an unreadable bundle
    /// is reported as the flag that named it, at startup, alongside the other
    /// boundary checks — and not as a handshake failure once the cluster is
    /// reachable.
    pub fn tls_config(&self) -> Result<TlsConfig> {
        match &self.ca_file {
            Some(path) => {
                let pem = std::fs::read(path)
                    .with_context(|| format!("reading --ca-file {}", path.display()))?;
                Ok(TlsConfig::with_ca_pem(pem))
            }
            None => Ok(TlsConfig::system()),
        }
    }

    /// What the heartbeat loop needs.
    pub fn schedule(&self) -> Schedule {
        // A zero-millisecond interval is a tight loop writing as fast as the
        // cluster will take it, which is a load generator rather than a canary
        // — and it would be one pointed at production.
        Schedule::new(&self.topic, Duration::from_millis(self.interval_ms.max(1)))
            .with_maybe_partition(self.partition)
            .with_maybe_count(self.count)
    }

    /// What to create, if `--create-topic` was given.
    pub fn topic_spec(&self) -> TopicSpec {
        TopicSpec::new(&self.topic)
            .with_partitions(self.partitions)
            .with_replication_factor(self.replication_factor)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(extra: &[&str]) -> Args {
        let mut argv = vec!["kaas-producer-canary", "--bootstrap", "broker:9092"];
        argv.extend_from_slice(extra);
        Args::try_parse_from(argv).unwrap()
    }

    #[test]
    fn both_http_schemes_are_accepted() {
        for url in [
            "http://registry:8080/apis/ccompat/v7",
            "https://registry:8443/apis/ccompat/v7",
        ] {
            assert_eq!(
                args(&["--schema-registry", url])
                    .schema_registry_url()
                    .unwrap(),
                url
            );
        }
    }

    #[test]
    fn any_other_scheme_is_refused() {
        let err = args(&["--schema-registry", "registry:8080"])
            .schema_registry_url()
            .unwrap_err();
        assert!(err.to_string().contains("http:// or https://"), "{err}");
    }

    /// A CA for a plaintext registry configures nothing. Ignoring it would
    /// hide the likelier mistake: the URL was meant to be `https://`.
    #[test]
    fn a_registry_ca_beside_an_http_url_is_refused() {
        let args = args(&[
            "--schema-registry",
            "http://registry:8080",
            "--schema-registry-ca-file",
            "/etc/trust-bundle/ca.crt",
        ]);
        let err = args.schema_registry_url().unwrap_err();
        assert!(err.to_string().contains("lose its https"), "{err}");
    }

    #[test]
    fn no_registry_ca_means_the_system_store() {
        let trust = args(&["--schema-registry", "https://registry:8443"])
            .schema_registry_trust()
            .unwrap();
        assert!(matches!(trust, RegistryTrust::System));
    }

    #[test]
    fn an_unreadable_registry_ca_names_the_flag() {
        let err = args(&[
            "--schema-registry",
            "https://registry:8443",
            "--schema-registry-ca-file",
            "/nonexistent/ca.crt",
        ])
        .schema_registry_trust()
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("--schema-registry-ca-file"),
            "{err:#}"
        );
    }
}
