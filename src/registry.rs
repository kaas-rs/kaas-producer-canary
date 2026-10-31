//! The schema registry, and the encoder that turns a heartbeat into bytes.
//!
//! Separate from the loop for the same reason [`crate::topic`] is: it is the
//! other thing here that *writes* somewhere other than the topic. Registering
//! a schema creates a version in Apicurio, and it happens once at startup or
//! never — so it belongs at the boundary, next to the other startup effect,
//! rather than inside a function that runs every second.
//!
//! # What goes on the wire
//!
//! Confluent's framing, which is what `schema_registry_converter` produces and
//! what every Kafka tool in this cluster expects to find: a zero magic byte,
//! the schema id as four big-endian bytes, then the Avro datum. A consumer
//! reads the id out of the prefix and asks the registry for the schema, which
//! is why the record does not have to carry one.
//!
//! # Registering at startup is deliberate, and so is failing there
//!
//! [`Encoder::register`] posts the schema before the first beat. Apicurio
//! returns the existing id when the schema is unchanged, so this is idempotent
//! across restarts and does not pile up versions.
//!
//! A failure here is fatal, which looks like a contradiction of this crate's
//! failure policy and is not. That policy is about *sends*: a canary that
//! exits on the first send error stops reporting exactly when there is
//! something to report. This is a startup check, and it sits beside
//! `Admin::connect`, which is fatal for the same reason — a canary that cannot
//! encode a record has nothing to report either way, and a pod in
//! CrashLoopBackOff is a louder, more accurate signal than a process that
//! runs for weeks logging a warning a second.
//!
//! The fatality does not extend past startup. `AvroEncoder` caches the
//! resolved schema and its id, so once [`Encoder::register`] has succeeded the
//! encode path makes no HTTP calls at all: the registry can go down for the
//! rest of the run without costing a single heartbeat.
//!
//! # Authentication
//!
//! A registry that requires a token gets one bearer token, attached to the
//! registration and used for nothing else. The same property that makes a
//! registry outage harmless after startup makes token refresh unnecessary: an
//! encoder that never calls the registry again never presents an expired
//! token to it. Where the token comes from is [`crate::identity`]'s concern.
//!
//! Over an `http://` registry it travels in the clear, which is the exposure
//! `main.rs` refuses for Kafka. That stays possible because the token's
//! audience is the registry alone and it expires within the hour, whereas an
//! OAUTHBEARER token is accepted by every broker in the cluster — but an
//! `https://` registry, verified against `--schema-registry-ca-file`, is the
//! configuration to run.
//!
//! # TLS
//!
//! The registry client is reqwest, handed a finished rustls `ClientConfig`
//! that names `ring`, for the reason [`crate::token`]'s connector does: a
//! process-wide default provider is global state any dependency could install
//! first. reqwest is built without a provider of its own (`Cargo.toml`), so it
//! is always handed one here, `http://` registry or not — without it, building
//! the client panics.

use std::sync::Arc;

use anyhow::{Context, Result};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::avro_common::get_supplied_schema;
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common::{SubjectNameStrategy, SuppliedSchema};

use crate::heartbeat::{self, Heartbeat};

/// What an `https://` registry's certificate is verified against.
#[derive(Debug)]
pub enum RegistryTrust {
    /// The system trust store. Empty in a scratch image, which is why
    /// registering against an `https://` registry with it fails at startup
    /// rather than at the handshake.
    System,
    /// These roots and no others: `--schema-registry-ca-file`.
    Roots(Vec<CertificateDer<'static>>),
}

impl RegistryTrust {
    /// Every certificate in a PEM bundle. Other PEM sections are skipped; a
    /// bundle with no certificate in it is an error, because an empty trust
    /// store verifies nothing and would fail every handshake.
    pub fn from_pem(pem: &[u8]) -> Result<Self> {
        let roots = CertificateDer::pem_slice_iter(pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("not a PEM bundle: {e}"))?;
        if roots.is_empty() {
            anyhow::bail!("no certificates in the PEM bundle");
        }
        Ok(Self::Roots(roots))
    }

    /// The root store these name. A certificate rustls cannot use is skipped,
    /// as [`crate::token`] skips one in the system bundle; a bundle of nothing
    /// but those is an empty store, which [`http_client`] reports.
    fn root_store(&self) -> rustls::RootCertStore {
        let mut roots = rustls::RootCertStore::empty();
        match self {
            Self::System => {
                roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            }
            Self::Roots(certs) => {
                roots.add_parsable_certificates(certs.iter().cloned());
            }
        }
        roots
    }
}

/// The registry client's HTTP client, with `ring` named rather than assumed.
///
/// An empty root store is refused for an `https://` URL only: an `http://`
/// registry never reaches the TLS configuration, and in a scratch image the
/// system store is empty, which must not stop a plaintext registry working.
fn http_client(url: &str, trust: &RegistryTrust) -> Result<reqwest::ClientBuilder> {
    let roots = trust.root_store();
    if url.starts_with("https://") && roots.is_empty() {
        anyhow::bail!(
            "no usable roots to verify the schema registry at {url} with: \
             set --schema-registry-ca-file"
        );
    }
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .context("building the schema registry's tls configuration")?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(reqwest::Client::builder().tls_backend_preconfigured(tls))
}

/// The heartbeat schema, in the shape the registry client posts.
///
/// `get_supplied_schema` derives all of this from a parsed [`Schema`], and the
/// one field it derives badly is the schema itself: it re-serialises, and
/// apache-avro's `Serialize` for a record does not emit field-level `doc`. The
/// subject in Apicurio is the closest thing this stream has to
/// documentation — it is what somebody finds when they discover an unfamiliar
/// topic — so the raw literal goes out instead, and only the derived *name* is
/// kept from the round trip.
///
/// [`Schema`]: apache_avro::Schema
pub fn supplied_schema() -> anyhow::Result<SuppliedSchema> {
    let mut supplied = get_supplied_schema(&heartbeat::schema()?);
    supplied.schema = heartbeat::SCHEMA.to_owned();
    Ok(supplied)
}

/// A registered schema, and the means to encode against it.
///
/// `'static` on the inner encoder because the futures it caches borrow only
/// its own settings, which it owns — the lifetime parameter exists for
/// callers who hand it borrowed configuration, and this one does not.
pub struct Encoder {
    inner: AvroEncoder<'static>,
    strategy: SubjectNameStrategy,
    subject: String,
}

impl Encoder {
    /// Register the heartbeat schema for `topic`, and return an encoder bound
    /// to it.
    ///
    /// The subject is `<topic>-value`, the TopicNameStrategy every other
    /// client in this cluster assumes, and the one kafbat-ui resolves with. It
    /// also means one schema per topic, which is what makes a compatibility
    /// rule on the subject meaningful — see [`crate::heartbeat`] for why that
    /// rule is worth setting and is not set today.
    ///
    /// Registering the same schema twice returns the existing version rather
    /// than making a new one, so restarts do not accumulate versions. That was
    /// checked against Apicurio rather than taken from the documentation.
    ///
    /// `bearer` is sent as `Authorization: Bearer` when present; `None` calls
    /// the registry anonymously, as this canary always has. `trust` is what an
    /// `https://` registry's certificate is checked against.
    pub async fn register(
        url: &str,
        topic: &str,
        bearer: Option<&str>,
        trust: &RegistryTrust,
    ) -> Result<Self> {
        let mut builder = SrSettings::new_builder(url.to_owned());
        if let Some(token) = bearer {
            builder.set_token_authorization(token);
        }
        let settings = builder
            .build_with(http_client(url, trust)?)
            .map_err(anyhow::Error::new)
            .with_context(|| format!("configuring the schema registry client for {url}"))?;

        // `TopicNameStrategyWithSchema` is what makes registration automatic:
        // the plain `TopicNameStrategy` only ever *fetches* the latest version
        // and fails when there is none, which would mean a fresh cluster
        // needed somebody to POST a schema by hand before the canary could
        // start. Supplying the schema turns that first lookup into a POST.
        let strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            topic.to_owned(),
            false,
            supplied_schema()?,
        );
        let subject = strategy
            .get_subject()
            .map_err(anyhow::Error::new)
            .context("deriving the registry subject")?;

        let inner = AvroEncoder::new(settings);
        let registered = inner
            .get_schema_and_id(&subject, strategy.clone())
            .await
            .map_err(anyhow::Error::new)
            .with_context(|| format!("registering {subject} with the schema registry at {url}"))?;

        tracing::info!(
            subject = %subject,
            schema_id = registered.id,
            registry = %url,
            // The registry-side counterpart of `auth` on "canary starting": a
            // 401 from a registry that started enforcing is the first thing
            // this answers.
            registry_auth = if bearer.is_some() { "oauthbearer" } else { "none" },
            "schema registered"
        );

        Ok(Self {
            inner,
            strategy,
            subject,
        })
    }

    /// The subject the schema is registered under. For the startup log line.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Encode one heartbeat, framed for the registry.
    ///
    /// Cannot fail against a registry that is merely unreachable — see the
    /// module docs — so a failure here means the record does not match the
    /// schema, which is a bug in this crate rather than a fault in the
    /// cluster. The caller still treats it as it treats a failed send.
    pub async fn encode(&self, heartbeat: &Heartbeat) -> Result<Vec<u8>, SRCError> {
        self.inner.encode_struct(heartbeat, &self.strategy).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A self-signed root with nothing else to it, generated for these tests.
    const ROOT_PEM: &str = "\
-----BEGIN CERTIFICATE-----
MIIBjTCCATOgAwIBAgIUbSEnRzKPlAYmBr5zPnRLC3yLh7cwCgYIKoZIzj0EAwIw
GzEZMBcGA1UEAwwQY2FuYXJ5LXRlc3Qtcm9vdDAgFw0yNjEwMzEwNjQ5NDNaGA8y
MTI2MTAwNzA2NDk0M1owGzEZMBcGA1UEAwwQY2FuYXJ5LXRlc3Qtcm9vdDBZMBMG
ByqGSM49AgEGCCqGSM49AwEHA0IABHUJZMqmXihQ1akrEcIaFO34ShSRlkKSHWIf
jZYhowPPAQ79BSorTLnZ1mW5HibVwDHpmGDT6bjryQPMD/P0dPajUzBRMB0GA1Ud
DgQWBBT3eSGaCwDr8UQGf+Vj6XfdaMxygzAfBgNVHSMEGDAWgBT3eSGaCwDr8UQG
f+Vj6XfdaMxygzAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0gAMEUCIQD1
ZWHSEaEjFc/53Z/TGhaHMyy4CSxZBxNLHNTngx4WLQIgBitEkiBY8Ny43xxu4P2O
4a95O14YWy2M0nHXvvyNnvY=
-----END CERTIFICATE-----
";

    /// The regression `Cargo.toml` warns about: reqwest is built without a
    /// crypto provider and panics building a client unless it is handed one.
    /// Nothing in these tests installs a process-wide default, so a client
    /// that builds here was given its provider by [`http_client`].
    #[test]
    fn a_client_builds_without_a_process_wide_crypto_provider() {
        let trust = RegistryTrust::from_pem(ROOT_PEM.as_bytes()).unwrap();
        http_client("https://registry:8443", &trust)
            .unwrap()
            .build()
            .unwrap();
        http_client("http://registry:8080", &RegistryTrust::Roots(Vec::new()))
            .unwrap()
            .build()
            .unwrap();
    }

    /// An https registry with nothing to verify it against would fail every
    /// handshake; saying so at startup names the flag that fixes it.
    #[test]
    fn an_https_registry_with_no_roots_is_refused_up_front() {
        let err = http_client("https://registry:8443", &RegistryTrust::Roots(Vec::new()))
            .err()
            .unwrap();
        assert!(
            err.to_string().contains("--schema-registry-ca-file"),
            "{err}"
        );
    }

    #[test]
    fn a_pem_bundle_yields_its_certificates() {
        let twice = format!("{ROOT_PEM}{ROOT_PEM}");
        match RegistryTrust::from_pem(twice.as_bytes()).unwrap() {
            RegistryTrust::Roots(roots) => assert_eq!(roots.len(), 2),
            RegistryTrust::System => panic!("a bundle is not the system store"),
        }
    }

    #[test]
    fn a_file_with_no_certificate_is_refused() {
        let err = RegistryTrust::from_pem(b"not a certificate\n").unwrap_err();
        assert!(err.to_string().contains("no certificates"), "{err}");
    }

    /// The subject is the half of the registry contract that is not in the
    /// schema. Every reader of this topic resolves the same string, and it is
    /// derived rather than written down, so this is where a change to the
    /// derivation would show.
    #[test]
    fn the_subject_is_the_topic_with_a_value_suffix() {
        let strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(
            "kaas-canary-v1".to_owned(),
            false,
            supplied_schema().unwrap(),
        );
        assert_eq!(strategy.get_subject().unwrap(), "kaas-canary-v1-value");
    }

    /// `get_supplied_schema` derives the name the registry stores from the
    /// schema itself. A namespace change would be invisible in the schema pin
    /// next door — it is a rename of the record, not of a field — and would
    /// still be a different type to every consumer resolving by name.
    #[test]
    fn the_registered_name_is_fully_qualified() {
        assert_eq!(
            supplied_schema().unwrap().name.as_deref(),
            Some("rs.kaas.canary.Heartbeat")
        );
    }

    /// The reason `supplied_schema` exists at all. If a future apache-avro
    /// learns to round-trip field docs this test still passes and the override
    /// becomes redundant; if this crate ever drops the override, it fails, and
    /// the subject in Apicurio quietly loses its documentation — which nothing
    /// else here would notice.
    #[test]
    fn the_field_documentation_survives_into_the_registry() {
        let posted = supplied_schema().unwrap().schema;
        assert!(
            posted.contains("Increases by exactly one per record within a run."),
            "field docs were dropped on the way to the registry: {posted}"
        );
    }
}
