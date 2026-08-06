//! The payload, its schema, and who wrote it.
//!
//! One type and two identifiers, kept together because they are one thing: the
//! record on the wire. Something downstream parses these fields by name —
//! `kaas-consumer-canary` reads them back and checks them — so renaming one is
//! a breaking change to every reader, and it is the kind of change that looks
//! harmless in a diff.
//!
//! # The schema is the contract now
//!
//! It used to be a JSON string, pinned by a test here so that changing it was
//! loud. It is now [`SCHEMA`], published to the registry at startup, which
//! makes it something a reader can resolve rather than something a reader has
//! to have been told.
//!
//! What that does **not** buy, as the registry is currently deployed, is
//! enforcement. Apicurio's compatibility level for these subjects is `NONE` —
//! verified against the cluster, not assumed — so an incompatible edit here is
//! accepted and breaks readers at parse time exactly as a renamed JSON field
//! used to. Setting the level to `BACKWARD` turns the registry into the check
//! this file would like it to be; until somebody does, the obligation below is
//! carried by review.
//!
//! **A new field needs a `default`.** That is what makes it readable by a
//! consumer holding the older schema — Avro resolution fills the gap from the
//! default, and has nothing to fill it with otherwise. Removing a field needs
//! the same on the other side.

use anyhow::{Context, Result};
use apache_avro::Schema;
use chrono::Utc;
use serde::Serialize;

/// The Avro schema for [`Heartbeat`], registered under the topic's subject at
/// startup and referenced by id in every record after that.
///
/// Posted to the registry as this exact string rather than as the parsed
/// schema re-serialised — see [`crate::registry::supplied_schema`]. That is
/// not a formatting preference: apache-avro does not round-trip field-level
/// `doc`, so the per-field prose below would be dropped on the way out and
/// nobody reading the subject in Apicurio would ever see it.
///
/// `sequence` is a `long` because Avro has no unsigned types. The Rust side
/// keeps `u64` — it counts, so it cannot go backwards — and the conversion is
/// lossless for any value below `i64::MAX`, which at one beat per second is
/// about 292 billion years away. A canary that survives that long has earned
/// the overflow.
///
/// The two timestamps are plain `long` and `string` rather than the
/// `timestamp-millis` logical type. The logical type would be the better
/// modelling, and it is deliberately not used: a reader that does not
/// understand the annotation gets a `long` either way, and the point of these
/// two fields is that they are the *same instant in two renderings* — one to
/// sort on and one to read — which a logical type would tempt a consumer to
/// collapse back into one.
pub const SCHEMA: &str = r#"{
  "type": "record",
  "name": "Heartbeat",
  "namespace": "rs.kaas.canary",
  "doc": "One heartbeat from kaas-producer-canary.",
  "fields": [
    {"name": "run_id", "type": "string", "doc": "Which run of the canary wrote this. Changes on every restart."},
    {"name": "sequence", "type": "long", "doc": "Increases by exactly one per record within a run. The diagnostic field."},
    {"name": "timestamp_ms", "type": "long", "doc": "Milliseconds since the epoch - what a machine should sort on."},
    {"name": "timestamp", "type": "string", "doc": "RFC 3339 with milliseconds - what a human should read."},
    {"name": "host", "type": "string", "doc": "Where the canary is running."}
  ]
}"#;

/// [`SCHEMA`], parsed.
///
/// Fallible because the constant is a string until something parses it, and a
/// crate that denies `expect` does not get to assume its own literal is valid.
/// The failure is caught at startup, before a record is written, which is the
/// only place it can be caught at all.
pub fn schema() -> Result<Schema> {
    Schema::parse_str(SCHEMA).context("parsing the heartbeat schema")
}

/// One heartbeat, as it appears on the topic.
///
/// Declaration order does not have to match [`SCHEMA`] — Avro resolution
/// matches fields by name — but it does, because two orderings of one record
/// is a diff nobody can read.
#[derive(Debug, Serialize)]
pub struct Heartbeat {
    /// Which run of the canary wrote this. Changes on every restart.
    pub run_id: String,
    /// Increases by exactly one per record within a run. The diagnostic field.
    pub sequence: u64,
    /// Milliseconds since the epoch — what a machine should sort on.
    pub timestamp_ms: i64,
    /// RFC 3339 with milliseconds — what a human should read.
    pub timestamp: String,
    /// Where the canary is running.
    pub host: String,
}

impl Heartbeat {
    /// Stamp a heartbeat from a single reading of the clock.
    ///
    /// A constructor rather than a struct literal in the loop, so that "one
    /// reading" is a property of the type instead of a habit of its caller. The
    /// two timestamps are one instant in two renderings; taking them from two
    /// `Utc::now()` calls would let them straddle a millisecond and disagree,
    /// and a consumer comparing them would report a fault nobody could explain.
    pub fn now(run_id: &str, host: &str, sequence: u64) -> Self {
        let now = Utc::now();
        Self {
            run_id: run_id.to_owned(),
            sequence,
            timestamp_ms: now.timestamp_millis(),
            timestamp: now.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            host: host.to_owned(),
        }
    }
}

/// An identifier for this run of the canary.
///
/// Wall-clock nanoseconds and the process id together, rather than either
/// alone: two canaries started in the same millisecond would share a
/// millisecond timestamp, and a pid is reused freely across restarts and
/// identical across containers. The pair is what makes "two writers on one
/// topic" distinguishable from "one writer with a sequence anomaly".
pub fn run_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or(0);
    format!("{nanos:x}-{:x}", std::process::id())
}

/// Where the canary is running, as far as it can tell.
pub fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Heartbeat {
        Heartbeat {
            run_id: "abc-1".to_owned(),
            sequence: 7,
            timestamp_ms: 1_785_686_674_238,
            timestamp: "2026-08-02T16:04:34.238Z".to_owned(),
            host: "canary-0".to_owned(),
        }
    }

    /// The schema is a contract: something downstream parses these fields by
    /// name. Renaming one is a breaking change to every consumer, and it is
    /// the kind of change that looks harmless in a diff.
    ///
    /// Pinned as the *parsed* schema re-serialised rather than as the literal
    /// above, so that reformatting the constant is free and changing what it
    /// means is not. Field-level `doc` is absent here because apache-avro
    /// drops it on the way back out — which is exactly why the literal is what
    /// gets posted, and `registry` has the test that says so.
    #[test]
    fn the_schema_shape_is_stable() {
        let canonical = serde_json::to_string(&schema().unwrap()).unwrap();
        assert_eq!(
            canonical,
            r#"{"type":"record","namespace":"rs.kaas.canary","name":"Heartbeat","doc":"One heartbeat from kaas-producer-canary.","fields":[{"name":"run_id","type":"string"},{"name":"sequence","type":"long"},{"name":"timestamp_ms","type":"long"},{"name":"timestamp","type":"string"},{"name":"host","type":"string"}]}"#
        );
    }

    /// The struct and the schema are two descriptions of one record, and
    /// nothing in the compiler ties them together. This is the tie: a rename
    /// on either side alone fails to resolve here, and a rename on both fails
    /// the pin above.
    ///
    /// Asserted through a round trip rather than against pinned bytes, because
    /// pinned Avro bytes would say the encoding of `long` had not changed —
    /// which is apache-avro's contract to keep, not this crate's.
    #[test]
    fn the_payload_resolves_against_the_schema() {
        let schema = schema().unwrap();
        let value = apache_avro::to_value(sample())
            .unwrap()
            .resolve(&schema)
            .unwrap();
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();

        let decoded = apache_avro::from_avro_datum(&schema, &mut bytes.as_slice(), None).unwrap();
        let apache_avro::types::Value::Record(fields) = decoded else {
            panic!("a heartbeat is a record");
        };
        let named: Vec<(&str, &apache_avro::types::Value)> =
            fields.iter().map(|(n, v)| (n.as_str(), v)).collect();

        assert_eq!(
            named,
            vec![
                ("run_id", &apache_avro::types::Value::String("abc-1".into())),
                ("sequence", &apache_avro::types::Value::Long(7)),
                (
                    "timestamp_ms",
                    &apache_avro::types::Value::Long(1_785_686_674_238)
                ),
                (
                    "timestamp",
                    &apache_avro::types::Value::String("2026-08-02T16:04:34.238Z".into())
                ),
                (
                    "host",
                    &apache_avro::types::Value::String("canary-0".into())
                ),
            ]
        );
    }

    #[test]
    fn the_two_timestamps_agree() {
        // One is for sorting and one is for reading; they are set from a
        // single clock reading and must not drift apart. Asserted against a
        // real construction rather than a hand-written literal, which would
        // only prove the literal was consistent with itself.
        let beat = Heartbeat::now("abc-1", "canary-0", 7);
        let parsed = chrono::DateTime::parse_from_rfc3339(&beat.timestamp).unwrap();
        assert_eq!(parsed.timestamp_millis(), beat.timestamp_ms);
    }

    #[test]
    fn a_run_id_is_not_reused() {
        // Two runs must be distinguishable, or "two canaries on one topic" and
        // "one canary with a sequence anomaly" become the same observation.
        assert_ne!(run_id(), run_id());
    }
}
