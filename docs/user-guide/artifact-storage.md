# Artifact storage

GPUStack stores large private JSON artifacts in an S3 compatible bucket. Benchmark
monitoring archives use this layer; browser clients never receive storage
credentials. PostgreSQL/MySQL metadata and object storage must be backed up together.

## Modes

- `builtin`: the packaged Linux container starts **SeaweedFS 4.54** on the same node.
  This is the default for a packaged server. Persist the entire GPUStack data volume
  (`/var/lib/gpustack` by default), including `artifacts/builtin`.
- `external`: use a shared S3 service and an existing bucket. GPUStack does not
  create the bucket or change its policy, encryption, lifecycle, or public access.
- `disabled`: stop new artifact writes. Explicit retained read profiles remain
  available to readers. This is the default for source/wheel installs and workers.

A multi-server deployment must select `external` and share a bucket and namespace.
The packaged single-node service has no storage replication or HA guarantee.
Credentials are generated randomly once, saved as private files on the persistent
volume, and reused after restarts. Its S3 and internal endpoints bind to loopback;
no additional container port needs to be published.

## Configuration

Configure the server with a YAML file, `GPUSTACK_` environment variables, or CLI
flags (for example, `--artifact-storage-mode external`). YAML keys use underscores,
flags use hyphens, and environment names use uppercase underscores.

```yaml
artifact_storage_mode: external
artifact_storage_profile_id: monitoring-eu-v1
artifact_storage_endpoint: https://s3.example.com # omit for AWS endpoint discovery
artifact_storage_bucket: gpustack-private
artifact_storage_prefix: installations/production
artifact_storage_region: eu-west-1
artifact_storage_addressing_style: path # auto, path, virtual
artifact_storage_tls_verify: true
# artifact_storage_ca_file: /etc/ssl/private/s3-ca.pem
```

The SDK's default AWS credential chain is used when explicit credentials are absent.
This supports environment credentials, shared AWS configuration, and SDK-supported
instance, container, or web identity credentials. Actual cloud identity chains need
verification in the target deployment. Explicit credentials use
`artifact_storage_access_key_id`, `artifact_storage_secret_access_key`, and optional
`artifact_storage_session_token`. Supply these through protected configuration or
secret environment injection. They are excluded from GPUStack Config dumps and
representation. Do not place credentials inside the endpoint URL.

TLS verification is enabled by default, including for custom endpoints. Set a custom
CA with `artifact_storage_ca_file`. Disabling certificate verification requires the
explicit `artifact_storage_tls_verify: false` setting or
`--no-artifact-storage-tls-verify` flag.

| Setting | Default | Meaning |
| --- | --- | --- |
| `artifact_storage_connect_timeout` | 5 seconds | SDK connection timeout |
| `artifact_storage_read_timeout` | 30 seconds | SDK read timeout |
| `artifact_storage_max_attempts` | 3 | Total SDK attempts, including the first |
| `artifact_storage_max_connections` | 8 | SDK connection pool size |
| `artifact_storage_max_concurrency` | 4 | In-flight calls per profile and server event loop |
| `artifact_storage_max_object_bytes` | 16777216 | Maximum decoded bytes for one document |
| `artifact_storage_max_total_bytes` | 0 (unlimited) | Initial installation-wide compressed-object capacity; initializes the shared database policy once |
| `artifact_storage_builtin_port` | 18333 | Loopback S3 port in builtin mode |
| `benchmark_archive_retention_days` | unset | Follow Benchmark retention; optional archive expiry in days |
| `benchmark_archive_sample_step_seconds` | 15 | Archive collector sample grid |
| `benchmark_archive_flush_interval_seconds` | 60 | Archive flush cadence |
| `benchmark_archive_settle_seconds` | 30 | Allow source metrics to settle |
| `benchmark_archive_max_concurrency` | 2 | Archive job concurrency budget |
| `benchmark_archive_max_run_bytes` | 268435456 | Maximum archive bytes per execution |

Retention and collector settings are consumed by the Benchmark archive domain;
the S3 adapter never silently truncates an oversized object. Configure the bucket's
own retention consistently with application retention so a bucket lifecycle does
not delete objects that remain indexed.

## Monitoring object names

New monitoring objects describe their scope before an immutable UUID:

```text
resources-<uuid>.json.gz
runtime-aggregate-<uuid>.json.gz
runtime-prefill-instances-<uuid>.json.gz
phase-1024-1-runtime-decode-instances-<uuid>.json.gz
```

The directory already identifies the installation, owner, Benchmark, execution,
and monitoring format, so filenames do not repeat those fields. The prefix
identifies runtime/resource data, specific roles, and aggregate/instance views.
Recorded phases add their phase ID; complete-run files omit `run`. The default
`all` role is omitted, as is the resource scope's default aggregate view.
One file contains multiple metrics. Components
are normalized to bounded ASCII labels; the database scope remains authoritative.
The UUID prevents different captures from overwriting each other.

Existing UUID-only names remain unchanged and readable. Readers, migration and
cleanup use the complete indexed object key; they never reconstruct paths from
the UUID. A filename does not identify the active revision or guarantee complete
samples. Those properties remain in the shared archive index.

## Readable revision manifests

After an archive is sealed as `ready`, `partial`, or `no_data`, a separate
background loop creates a directory for each published revision:

```text
installations/<installation>/owners/<owner>/benchmarks/<id>/runs/<run>/
  monitoring/v1/manifests/revision-000001.json
```

These are actual, uncompressed UTF-8 JSON files with indentation, so an S3 or
Filer browser can display them. Monitoring curves remain independent gzip objects.
Old sealed archives are backfilled from their published database indexes without
querying Prometheus or changing the monitoring data. Running captures wait until
sealing; optional directory writes do not block the ongoing capture loop.

Each document identifies its immutable revision and lists scopes, phases, roles,
views, metric names/units, sample coverage, object keys, sizes and SHA-256 values.
Supported metrics have detailed metadata; `unavailable_metrics` compactly groups
unsupported and not-applicable keys and reasons. Supported metrics with no samples
remain distinct. Times retain Unix seconds and include readable UTC timestamps.
`revision_state` describes that saved revision, rather than the current job state.
No raw prompts, credentials, launch commands, environment variables or private
source addresses are copied into the directory.

`stored_bytes` values are frozen when the directory is first prepared. A later
profile migration may use another gzip encoding while preserving the data.
For curve objects, `checksum_scope: uncompressed_json_bytes` means the SHA-256 is
of the decoded JSON bytes, not the `.json.gz` download. For a manifest itself,
the checksum covers its actual UTF-8 JSON bytes.

The database determines the active revision and whether its manifest has been
committed; there is no mutable `latest.json` pointer. An upload can precede a failed
database acknowledgement, so the presence of a file alone is not proof that its
manifest reference is committed. The normal authenticated
`GET /v2/benchmarks/<id>/monitoring-archive` response includes a `manifest` object:

```json
{
  "state": "ready",
  "archive_revision": 1,
  "filename": "revision-000001.json",
  "size_bytes": 12345,
  "sha256": "<sha256>",
  "checksum_scope": "utf8_json_bytes"
}
```

The same field reports `pending`, `failed` with a safe `last_error`, or
`unavailable`. It does not expose private bucket/profile configuration or full
object keys. A directory failure, including capacity rejection, does not change
the capture state or invalidate already saved charts; background retries can
complete it later.

Manifests use the shared byte ledger and participate in profile migration,
integrity audits, Benchmark deletion and archive expiry. They do not consume the
monitoring per-run byte budget. Unknown upload results retain their indexed intent
and reservation and are reconciled by a healthy retry. Fixed manifest keys are
excluded from ordinary orphan/capacity cleanup to prevent a stale cleaner from
deleting a retried publication; explicit archive deletion or expiry still removes
all of them. A rejected, never-attempted upload leaves no orphan object.

## Global capacity

Capacity applies to this GPUStack installation across all Servers and configured
storage profiles. It counts compressed bytes in managed objects, including retained
archive revisions, unpublished objects, and both copies during profile migration.
It also counts space reserved for writes whose S3 outcome is not yet confirmed.
It excludes other files in the bucket, object-store replication overhead, retained
S3 object versions, and filesystem space that SeaweedFS has not yet reclaimed.

The default is **unlimited**. Set `artifact_storage_max_total_bytes` in YAML,
`GPUSTACK_ARTIFACT_STORAGE_MAX_TOTAL_BYTES` in the environment, or
`--artifact-storage-max-total-bytes` on server startup to seed the policy on first
initialization. Values are integer bytes; `0` means unlimited. The largest supported
configured limit is `9007199254740991` bytes. All Servers should use the same initial
configuration. After initialization, the shared database policy is authoritative:
restarting a Server with a different seed does not overwrite an administrator's
saved policy.

Platform administrators can inspect or change the active policy without restarting
Servers through `GET` / `PUT /v2/artifact-storage/capacity`; PUT accepts
`{"max_bytes": 0}` for unlimited, or a positive byte limit. The response contains
`used_bytes`, `reserved_bytes`, `total_bytes`, `remaining_bytes`, `usage_percent`,
and `state`. Unlimited mode returns null remaining bytes and percentage. Limited
mode warns at 80% and becomes `full` at or above the cap.

The CLI provides the same shared policy:

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml capacity
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  capacity --max-bytes 107374182400
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  capacity --max-bytes 0
```

Reservations are serialized in the shared database before uploading. A write that
does not fit reports `storage_capacity_exceeded`; it cannot silently evict history
or truncate the archive. Existing archives remain readable, and Benchmark request
execution and client statistics continue independently. Raising the cap, choosing
unlimited, or deleting managed archives can allow pending capture to resume. A
missed source interval can only be recovered while Prometheus still retains it.
Lowering the cap below existing usage is allowed and blocks additional allocation;
it does not delete existing data.

Space is released only after S3 acknowledges deletion. Failed or interrupted
uploads conservatively retain their reservation until reconciliation or cleanup
confirms the outcome. Existing objects are imported into the ledger on first use;
legacy intended objects or migration copies with unknown compressed size use a
conservative reservation based on their uncompressed size. This may temporarily
report more allocated space than the actual stored bytes. It is not a whole-bucket
usage scan or an S3 lifecycle-policy controller. Bucket versioning, object lock and
physical disk reclamation remain the storage administrator's responsibility.

## Capture status and source coverage

`capture_complete` describes the pinned archive revision. It is true for `ready`
and `no_data` revisions, and false for a `partial` revision with capture errors,
invalidated identities, or unavailable measured scopes. `archive_state` reports
the current job state, which can change while a client keeps reading an older
revision.

`data_coverage` describes the saved source observations separately. A short phase
can be `ready` with `capture_complete: true` and `data_coverage: partial` when its
first sampling tick precedes the first scrape, or its rolling latency window is
too short. These conditions retain their metric statuses, nulls, zero values,
and coverage statistics; they do not require retrying a successful capture.
Query failures remain retryable capture errors. A successful retry publishes a
new revision and preserves the earlier revision for pinned readers.

## Storage profiles and changes

A profile ID permanently identifies an endpoint, bucket, prefix, and region. The
adapter stores a fingerprint binding under `artifacts/profiles` and refuses to reuse
that ID for a different target. Archive indexes also retain profile identity.
Credential rotation for the same target does not require a new ID.

When changing the destination, choose a new write profile ID and retain the old
connection under `artifact_storage_read_profiles`. Profile entries use the same
settings without the `artifact_storage_` prefix. Configure this mapping through YAML
or the JSON environment variable `GPUSTACK_ARTIFACT_STORAGE_READ_PROFILES`.

```yaml
artifact_storage_profile_id: monitoring-eu-v2
artifact_storage_bucket: new-private-bucket
artifact_storage_read_profiles:
  monitoring-eu-v1:
    endpoint: https://old-s3.example.com
    bucket: gpustack-private
    prefix: installations/production
    region: eu-west-1
    addressing_style: path
    # Credentials may be omitted to use the SDK credential chain.
```

Keep old profiles and their objects reachable until every dependent archive is
migrated or deleted. Switching to external or disabled mode stops the packaged
SeaweedFS service; migrate local objects first or retain a separately reachable
service for old reads. Copying files to a new bucket alone does not rewrite archive
indexes. Never remove profile bindings merely to make a changed target pass checks.

## Required S3 operations

The adapter uses SigV4 `HeadObject`, `PutObject`, `GetObject`, and `DeleteObject`.
Give the runtime access only to its configured namespace. Conditional `PutObject` with `If-None-Match: *` is used when supported.
`artifact_storage_conditional_writes: false` selects portable S3 explicitly; the
adapter also falls back on an explicit unsupported-condition response. Permission
errors and concurrent conflicts never trigger fallback. Portable writes check for
existing content but cannot make HEAD and PUT atomic: callers must use unique
object keys and database-fenced publication, as the Benchmark archiver does.
Identical retries verify the stored checksum. ETags are opaque and are never treated as MD5 checksums.

The read-only connection check uses `HeadBucket`, which on AWS requires
`s3:ListBucket`. A denied check can therefore indicate missing health-check
permission even if object operations are permitted. The check never creates a
bucket or changes data. Builtin initialization alone uses `CreateBucket` for its
managed bucket.

Each document is independently gzip-compressed and carries a SHA-256 of canonical
uncompressed UTF-8 JSON. Reads verify stored metadata and the expected index
checksum, limit both compressed and decoded bytes, and close the response stream.
All SDK I/O and compression run outside the event loop. Errors expose stable safe
reasons, without SDK exception bodies or credentials.

Storage connection flags, environment variables, and YAML settings are documented
in the [CLI configuration reference](../cli-reference/start.md#artifact-storage-options).
External mode connects to an existing S3-compatible service; GPUStack does not
deploy or manage that service's replicas, metadata database, or availability.

The packaged service uses HTTP ports 18333 (S3), 19333 (master), 18080 (volume),
18888 (filer), and internal gRPC ports 29333, 28080, 28888. All bind to loopback.
Only the S3 port is configurable. Its master data, object data, Filer LevelDB, and
credentials all reside beneath `artifacts/builtin`; a partial volume backup is
insufficient.

## Verification scope

Unit tests run without networks and cover byte bounds, checksums, immutable retries
and racing writers, safe errors, configuration, credential persistence, disabled
mode, read profiles, and SDK offloading. A local smoke test of SeaweedFS 4.54 verifies
S3 upload, download, restart persistence, checksum validation, and deletion. Linux
image builds on both architectures and AWS/other vendor identity, TLS, permission,
performance, and failure scenarios require deployment-level verification.

The pinned binary flags and checksums follow the [official 4.54 release](https://github.com/seaweedfs/seaweedfs/releases/tag/4.54),
[server command](https://github.com/seaweedfs/seaweedfs/blob/4.54/weed/command/server.go),
and [S3 configuration documentation](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API).

## Profile migration and integrity audit

The administrator CLI uses the configured database connection directly. It opens
an existing schema; it does not create tables, run migrations, initialize auth
providers, or start storage services. Configure both the old and destination
profiles before running it. Keep a coordinated database and object backup.

Preview a selected execution without uploads or database changes:

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  migrate-profile --source-id monitoring-eu-v1 --target-id monitoring-eu-v2 \
  --benchmark-id 7 --run-id RUN_UUID --dry-run --manifest /secure/migration-plan.json
```

Apply that same selection with `--apply`. Omitting both action flags also performs
a dry run. `--archive-id`, `--benchmark-id`, and `--run-id` can narrow the selection;
`--limit` caps one invocation (default 100, maximum 1000 archives). The command
processes only archives in `ready`, `partial`, or `no_data` states, and refuses to
claim one with an active lease. It does not backfill data from Prometheus or
migrate unfinished/failed captures.

For each archive, migration acquires the existing database lease, increments its
fencing token, and persists intended source/destination object references before
uploading. It reuses stable object keys in the destination profile, verifies source
and destination SHA-256, checkpoints progress per object, and changes the archive
and its published chunk profile IDs in one fenced transaction after every object
passes. Scope IDs, archive revisions, timestamps, and payload checksums remain
unchanged. Published revisions therefore replay the same data. Different archives
commit independently; one failed batch can already contain completed archives.

Rerun the same command after interruption. Completed archives no longer match the
source profile, and unfinished copies resume from their durable progress. A lost
lease prevents publication, and another process may resume after the lease expires
(up to three minutes). A missing target copy is recreated from its verified source;
a corrupt/conflicting copy stops migration for administrator investigation. Source
copies remain intact throughout, including after successful migration.

Retained source and unfinished destination copies stay indexed in the archive's
migration history. Normal Benchmark deletion or configured archive expiry cleans
them up through the same retryable GC job. Keep their storage profiles configured
until that cleanup completes. There is no implicit eager source purge or bucket-wide
cleanup command. Plan for temporary duplicate storage capacity.

Audit the currently indexed objects without changing storage or database rows:

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  audit --profile-id monitoring-eu-v2 --manifest /secure/integrity-report.json
```

`check` is an alias for `audit`. The same archive/run/Benchmark filters and limit
apply. The report identifies missing objects, checksum mismatches, unavailable
profiles, and safe storage failure reasons; it does not contain payloads or
credentials. Exit status is 0 for success, 2 when audit found issues, and 1 for an
operation failure. Reports are written with private permissions when `--manifest`
is supplied. The audit checks published indexed objects, rather than scanning an
entire bucket or guessing which unindexed objects may be deleted.
