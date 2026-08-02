# kaas-producer-canary

Writes a timestamped JSON heartbeat to a Kafka topic once a second, using
[kaas-lib](https://github.com/kaas-rs/kaas-lib)'s `kafka-produce`.

```sh
cargo run -- --bootstrap localhost:9092 --topic kaas-canary
```

```json
{"run_id":"18c80891ba0e656b-3dbdc8","sequence":1,"timestamp_ms":1785686674238,"timestamp":"2026-08-02T16:04:34.238Z","host":"canary-0"}
```

Each record also carries two headers: `content-type: application/json` and
`canary-run: <run_id>`, so a consumer can filter by run without parsing the
body.

## Why `sequence` is the point

Posting the time is easy. What makes the stream diagnostic is that `sequence`
increases by exactly one per record **independently of the clock**, which
separates three situations that look identical in a topic full of timestamps:

| what you see                      | what happened                                                       |
|-----------------------------------|---------------------------------------------------------------------|
| gap in `sequence`                 | records were lost, or a send failed and was reported                |
| gap in time, no gap in `sequence` | the canary was paused or restarted — nothing lost                   |
| repeated `sequence`               | a duplicate, which `kafka-produce` refuses to cause; a real finding |

`run_id` changes on every restart, so two canaries writing to one topic never
look like a single one with a sequence anomaly. It is wall-clock nanoseconds
plus the pid, because two processes started in the same millisecond would
share a millisecond timestamp, and pids repeat across restarts and containers.

## Options

| flag                                    | env                        | default       |                                             |
|-----------------------------------------|----------------------------|---------------|---------------------------------------------|
| `--bootstrap`                           | `KAAS_CANARY_BOOTSTRAP`    | —             | comma-separated `host:port`                 |
| `--topic`                               | `KAAS_CANARY_TOPIC`        | `kaas-canary` |                                             |
| `--interval-ms`                         | `KAAS_CANARY_INTERVAL_MS`  | `1000`        |                                             |
| `--create-topic`                        | `KAAS_CANARY_CREATE_TOPIC` | off           | create the topic if missing                 |
| `--partitions` / `--replication-factor` |                            | `1` / `1`     | used only with `--create-topic`             |
| `--partition`                           |                            | —             | pin every record to one partition           |
| `--count`                               |                            | —             | stop after N beats; runs forever when unset |
| `--acks-leader`                         |                            | off           | require only the leader, not the full ISR   |

`--create-topic` is off by default on purpose: a canary pointed at a
production cluster should observe it rather than change it, and a typo in
`--topic` with creation enabled quietly makes a new topic instead of failing.

## Two behaviours worth knowing before you read the output

**Unkeyed records stick to one partition.** That is KIP-480, and it is
deliberate — round-robining per record would produce a one-record batch per
partition. For a canary it is also convenient, since the whole heartbeat
stream stays in offset order in one partition. Use `--partition` to choose
which one.

**`--acks-leader` acknowledges before the record is readable.** With
`acks=1` the leader has written the record but the high watermark does not
advance until the replicas have it, so a consumer tailing the topic will
appear to lag behind the canary's own log. The default (`acks=all`) does not
have this property.

## Failure policy

A failed send is logged and counted, and the loop continues. A canary that
exits on the first error stops reporting exactly when there is something to
report, and its silence gets attributed to the cluster rather than to itself.
`unwrap`, `expect` and `panic!` are denied at the crate root for the same
reason — this process is meant to run for weeks.

## Building

```sh
cargo build --release
```

Everything resolves from crates.io — `kafka-produce` ships in kaas-lib 0.2.1.

## Container

```sh
docker run --rm ghcr.io/kaas-rs/kaas-producer-canary:latest \
  --bootstrap kafka:9092 --topic kaas-canary
```

Static musl binary on distroless nonroot, so there is no shell in the image
and nothing to exec into. Built and published by
[`.github/workflows/release.yml`](.github/workflows/release.yml) on a `v*` tag;
`main` builds publish `main` and `sha-<short>` tags for testing, and only a
semver tag moves `latest`.

## Deployment

Deployed by ArgoCD as **one instance per Kafka cluster**, from
`apps/kaas/canary/` and `apps/strimzi/canary/` in the cluster repo
(`Woestebanaan/k3s-cluster`). It gets no namespace of its own: each canary
sits in the namespace of the broker it watches, so it is scheduled, scaled and
deleted with that cluster rather than alongside it.

The manifests live there rather than here because that is the repository
ArgoCD reads; duplicating them would give two sources of truth for one
Deployment. They **pin an image digest**, not a tag — a canary that silently
changes version is a canary that cannot be trusted to report anything.

Sync is manual in this cluster, deliberately: a merged commit is not evidence
that anything changed. Check `argocd app get kaas`, not the git log.

## Licence

Apache-2.0
