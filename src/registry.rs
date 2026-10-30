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
//! It travels over plain HTTP — `--schema-registry` accepts `http://` only, for
//! the reasons in `Cargo.toml` — which is the exposure `main.rs` refuses for
//! Kafka. It is tolerated here because the token's audience is the registry
//! alone and it expires within the hour, whereas an OAUTHBEARER token is
//! accepted by every broker in the cluster. TLS on the registry is the fix.

use anyhow::{Context, Result};
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use schema_registry_converter::avro_common::get_supplied_schema;
use schema_registry_converter::error::SRCError;
use schema_registry_converter::schema_registry_common::{SubjectNameStrategy, SuppliedSchema};

use crate::heartbeat::{self, Heartbeat};

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
    /// the registry anonymously, as this canary always has.
    pub async fn register(url: &str, topic: &str, bearer: Option<&str>) -> Result<Self> {
        let mut builder = SrSettings::new_builder(url.to_owned());
        if let Some(token) = bearer {
            builder.set_token_authorization(token);
        }
        let settings = builder
            .build()
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
