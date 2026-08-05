//! The payload, and who wrote it.
//!
//! One type and two identifiers, kept together because they are one thing: the
//! record on the wire. Something downstream parses these fields by name —
//! `kaas-consumer-canary` reads them back and checks them — so renaming one is
//! a breaking change to every reader, and it is the kind of change that looks
//! harmless in a diff.

use chrono::Utc;
use serde::Serialize;

/// One heartbeat, as it appears on the topic.
///
/// Declaration order is serialisation order, and the test below pins the whole
/// string rather than the field names, so reordering these is as loud as
/// renaming them.
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

    /// The payload is a contract: something downstream parses these fields by
    /// name. Renaming one is a breaking change to every consumer, and it is
    /// the kind of change that looks harmless in a diff.
    #[test]
    fn the_payload_shape_is_stable() {
        let json = serde_json::to_string(&sample()).unwrap();
        assert_eq!(
            json,
            r#"{"run_id":"abc-1","sequence":7,"timestamp_ms":1785686674238,"timestamp":"2026-08-02T16:04:34.238Z","host":"canary-0"}"#
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
