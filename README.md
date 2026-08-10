# kaas-producer-canary

Writes a timestamped Avro heartbeat to a Kafka topic once a second, using
[kaas-lib](https://github.com/kaas-rs/kaas-lib)'s `kafka-produce`, with the
schema registered in a Confluent-compatible registry.

```sh
cargo run -- --bootstrap localhost:9092 --topic kaas-canary \
  --schema-registry http://localhost:8080/apis/ccompat/v7
```

```
{"run_id": "18c80891ba0e656b-3dbdc8", "sequence": 1, "timestamp_ms": 1785686674238,
 "timestamp": "2026-08-02T16:04:34.238Z", "host": "canary-0"}
```

On the wire that is Confluent framing — a zero byte, the schema id as four
big-endian bytes, then the Avro datum:

```
00 00 00 00 01  2e 31 38 63 38 ...
│  └────┬────┘  └── datum
│       └── schema id 1
└── magic byte
```

Each record also carries three headers, so a consumer can filter without
decoding the body:

| header           | value                                     |                                        |
|------------------|-------------------------------------------|----------------------------------------|
| `content-type`   | `application/vnd.confluent.avro+binary`   |                                        |
| `canary-run`     | `<run_id>`                                | which run wrote it; changes on restart |
| `canary-version` | the crate version                         | which build wrote it                   |

`canary-version` is there because the canary's own version changes what the
stream looks like — idempotence arriving in kaas-lib 0.3.0 quietly removed a
class of `sequence` gap. A reader comparing two periods needs to know whether
it is comparing two clusters or two canaries.

This crate's **0.3.0 is the sharpest example yet**: it is where the payload
stopped being JSON. A topic written across that boundary holds both formats,
split at whatever offset the rollout happened to land on, and this header is
the only thing in the record that says which side of it you are reading —
the body cannot tell you, because deciding how to parse the body is the
question.

## The schema, and how it gets there

The schema is `SCHEMA` in [`src/heartbeat.rs`](src/heartbeat.rs), a record
called `rs.kaas.canary.Heartbeat`. Nothing has to put it in the registry by
hand: the canary posts it at startup under the subject `<topic>-value`
(TopicNameStrategy), and the registry returns the existing id when the schema
has not changed, so restarts do not pile up versions.

Registration is the one startup step that is **fatal** on failure, which reads
like a contradiction of the failure policy below and is not. That policy is
about sends. This is a configuration check, and it sits beside
`Admin::connect`: a canary that cannot encode a record has nothing to report,
and a pod in `CrashLoopBackOff` is a louder and more accurate signal than a
process that runs for weeks logging one warning a second.

It does not extend past startup. The resolved schema and its id are cached, so
**after the first beat the canary makes no HTTP calls at all** — the registry
can be down for the rest of the run without costing a heartbeat.

Two things worth knowing before you edit the schema:

**A new field needs a `default`.** That is what lets a consumer holding the
older schema read the new records; Avro resolution fills the gap from the
default and has nothing to fill it with otherwise.

**The registry is not currently enforcing that.** Apicurio's compatibility
level for these subjects is `NONE`, so an incompatible edit is accepted and
breaks readers at parse time. Setting it to `BACKWARD` makes the registry the
check this design wants; until then the check is review.

`sequence` is an Avro `long` because Avro has no unsigned types. The Rust side
stays `u64` — it counts, so it cannot go backwards — and the conversion is
lossless below `i64::MAX`, which at one beat per second is about 292 billion
years away.

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

| flag                                    | env                            | default       |                                             |
|-----------------------------------------|--------------------------------|---------------|---------------------------------------------|
| `--bootstrap`                           | `KAAS_CANARY_BOOTSTRAP`        | —             | comma-separated `host:port`                 |
| `--schema-registry`                     | `KAAS_CANARY_SCHEMA_REGISTRY`  | —             | ccompat base URL; `http://` only            |
| `--topic`                               | `KAAS_CANARY_TOPIC`            | `kaas-canary` |                                             |
| `--interval-ms`                         | `KAAS_CANARY_INTERVAL_MS`  | `1000`        |                                             |
| `--create-topic`                        | `KAAS_CANARY_CREATE_TOPIC` | off           | create the topic if missing                 |
| `--partitions` / `--replication-factor` |                            | `1` / `1`     | used only with `--create-topic`             |
| `--partition`                           |                            | —             | pin every record to one partition           |
| `--count`                               |                            | —             | stop after N beats; runs forever when unset |
| `--acks-leader`                         |                            | off           | require only the leader, not the full ISR   |
| `--ca-file`                             | `KAAS_CANARY_CA_FILE`      | system roots  | PEM bundle to trust for the Kafka connection |

`--create-topic` is off by default on purpose: a canary pointed at a
production cluster should observe it rather than change it, and a typo in
`--topic` with creation enabled quietly makes a new topic instead of failing.

`--schema-registry` has no default and is required, because there is no format
to fall back to: every record is Avro framed with a schema id, so a canary that
started without a registry would be writing bytes nothing can read. It takes
`http://` only, and rejects anything else at the boundary — the *registry
client* is built with no TLS backend, for the reason spelled out in
[`Cargo.toml`](Cargo.toml): both of `schema_registry_converter`'s TLS features
reach for a C project (OpenSSL, or `aws-lc-sys` and cmake), and either one
would cost the release image its "static musl, one build package" property and
put a second crypto provider into the same `rustls` kaas-lib uses with `ring`.

That restriction is the registry's alone. **The Kafka connection does TLS**, via
kaas-lib's own `rustls`/`ring` stack — which is why `--ca-file` exists and why
talking to a TLS listener needs no feature flag here.

## Workload identity

There are no flags for it. If the environment describes a federated identity,
this canary authenticates with SASL/OAUTHBEARER; if it does not, it connects the
way it always has. Nothing in between has to be configured:

| variable | | |
|---|---|---|
| `AZURE_CLIENT_ID` | required | the **application (client) id** — not the service principal's object id |
| `AZURE_TENANT_ID` | required | selects the issuer |
| `AZURE_FEDERATED_TOKEN_FILE` | required | path to the assertion, re-read on every token fetch |
| `AZURE_AUTHORITY_HOST` | `https://login.microsoftonline.com/` | for sovereign clouds |
| `AZURE_KAFKA_SCOPE` | `api://<client-id>/.default` | when the broker wants a token about some other resource |

The first three are exactly what AKS's `azure-wi-webhook` injects, so a pod that
runs here runs on AKS unchanged — and the reverse. Off-cluster the same three
can be set by hand. **Setting some but not all three is an error at startup**,
not a quiet fall back to anonymous: a canary that silently connects to a
listener nobody meant to test reports on the wrong thing for as long as nobody
notices.

The token endpoint and scope are derived rather than asked for, so the common
case needs no configuration at all beyond the identity itself.

Two things follow automatically from an identity being present:

- **TLS is turned on**, with no way to opt out. OAUTHBEARER over a plaintext
  listener puts an access token on the wire in the clear, and unlike a password
  a stolen bearer token is honoured from whoever replays it until it expires.
- **`--ca-file` is usually needed with it.** Strimzi signs its listeners with a
  per-cluster CA that is in no system trust store; without the flag the
  handshake fails at connect.

```sh
# nothing about the canary's own arguments changes
AZURE_CLIENT_ID=... AZURE_TENANT_ID=... \
AZURE_FEDERATED_TOKEN_FILE=/var/run/spiffe/svid/azure-token \
kaas-producer-canary --bootstrap broker:9094 --topic kaas-canary \
  --schema-registry http://registry:8080/apis/ccompat/v7 \
  --ca-file /etc/kafka/ca.crt
```

Where the assertion comes from is not this crate's business — a spiffe-helper
sidecar writes it in the homelab, the kubelet writes it on AKS. It is a file
that changes, which is why it is read afresh for each token rather than captured
at startup: SPIRE's JWT-SVIDs default to a five minute life, well below the
access token they buy and far below the KIP-368 re-authentication interval.

The exchange itself is [`src/token.rs`](src/token.rs), in this crate rather than
in kaas-lib. kaas-lib's job ends at `TokenProvider` — "give me a bearer token,
and a fresh one when I ask again" — and an `AADSTS` error taxonomy is not
something a Kafka client library should carry for every one of its consumers.
That module's header records why the official `azure_identity` crate is not the
dependency here, which is a fact about this image rather than about the crate.

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

Startup is the exception, and schema registration is part of it; see above.

## Style

Optional configuration is a consuming `with_*` builder on the type itself, the
convention kaas-lib uses. [`STYLE.md`](STYLE.md) is the whole of it, including
the one place this crate deviates: it is a binary, so a speculative setter is
a `dead_code` build failure rather than unused API.

## Building

```sh
cargo build --release
```

Everything resolves from crates.io — `kafka-produce` and `kafka-admin` are
pinned at kaas-lib 0.7.

## Container

```sh
docker run --rm ghcr.io/kaas-rs/kaas-producer-canary:latest \
  --bootstrap kafka:9092 --topic kaas-canary \
  --schema-registry http://apicurio:8080/apis/ccompat/v7
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

Both instances point `--schema-registry` at the one Apicurio in the `apicurio`
namespace. They write to same-named topics on two different clusters and so
share the subject `kaas-canary-v1-value`, which is fine precisely because they
publish an identical schema — two writers agreeing on one subject is cheaper
than two subjects somebody has to compare.

Because the digest is pinned, `--schema-registry` and the image bump have to
land in **one commit**: the pre-Avro binary does not know the flag and would
exit on an unknown argument.

That registry currently runs on in-memory H2, which is a test configuration
and not a durable one. It is empty after any restart, and a canary that
registered at startup keeps using its cached schema id without noticing — so
records reference an id the registry no longer knows and consumers get a 404
resolving it. The repair is to restart the producers after the registry:

```sh
kubectl rollout restart deployment/kaas-producer-canary -n kaas
kubectl rollout restart deployment/kaas-producer-canary -n strimzi
```

## Licence

Apache-2.0
