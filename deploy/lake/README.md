# Lake isolation and Dagster deployment candidate

These templates describe an **isolated** deployment. Nothing in this directory
installs or starts services automatically. Approval must cover paths, ports,
users, databases, sample data and resource budgets before using them on Linux.
They do not modify the existing API, jobs service, auth database or data.

## Environment and ownership

Use a separately installed checkout/venv under `/opt/qrp-lake`. Install the
project's lake extra and matching `dagster-webserver` and `dagster-postgres`
versions. The adapter is tested against Dagster 1.13.28; its companion packages
are 1.13.28 and 0.29.28 respectively. Pin the resolved environment for deployment.

Use separate `qrp-lake-core`, `qrp-lake-extension`, and `qrp-lake-daemon` users.
The core and extension users may share a **read-only** `qrp-lake-readers` group.
Prepare directories before starting units; `ReadWritePaths` does not create
them. Proposed layout:

| Path | Writer | Readers |
| --- | --- | --- |
| `/var/lib/qrp-lake/core` | core user | extension user through read group |
| `/var/lib/qrp-lake/raw` | core user | administrator; no extension access |
| `/var/lib/qrp-lake/core-runtime` | core user | administrator |
| `/var/lib/qrp-lake/extension` | extension user | intended consumers |
| `/var/lib/qrp-lake/extension-runtime` | extension user | administrator |
| `/var/lib/qrp-lake/dagster` | Dagster service users | administrator |

Set directories to deny group write to core files. Core files must remain
group-readable when newly published (the core unit uses `UMask=0027`). Apply
filesystem quotas or an isolated bounded volume to staging/spill. Worker disk
polling alone cannot prevent a fast write from temporarily exceeding a budget.
Precreate `raw` with core ownership and mode 0700. The core unit explicitly
allows writes there under `ProtectSystem=strict`; do not add it to extension
`ReadWritePaths`. Verify this inside the service mount namespace.

Create **three new PostgreSQL databases**, following `catalog-roles.sql.example`:
core DuckLake, extension DuckLake, and Dagster run/event metadata. Use distinct
roles. After core metadata initialization, grant the core reader SELECT on
existing and future metadata tables. Validate actual DuckLake schema names.
The extension role must fail attempts to modify core catalog metadata, and its
Linux identity must fail writes to core data files. Core read-only access needs
both catalog SELECT and data-file read permissions.
Set the new Dagster metadata database timezone to UTC **before its first
write**, as in the SQL template. Its naive metadata timestamps otherwise
inherit a server timezone such as Asia/Shanghai, while monitoring interprets
them as UTC; the Linux probe reproduced slots failing to expire. Business
schedule timezone remains Asia/Shanghai. Do not blindly change an existing
metadata database with historical timestamps; that needs a separate audit.

Use restricted configuration files under `/etc/qrp-lake` (not committed):

- `core.env`: `QRP_ENV_FILE` pointing to itself; `QRP_LAKE_ROOT`; core writer
  catalog; Tushare configuration; explicit worker limits and absolute QRP paths.
- `extension.env`: `QRP_ENV_FILE` pointing to itself; same lake root;
  `QRP_LAKE_CORE_READ_CATALOG` and extension writer catalog. **No core writer
  catalog or provider secrets.** Set `QRP_LAKE_PLUGINS_FILE` to an operator-owned
  enabled-plugin JSON. `QRP_LAKE_ALLOW_OBSERVED` defaults to false: observed/legacy
  inputs without historical PIT proof are rejected. Setting it true explicitly
  opts into observed-data research outputs; it does not make them strict PIT.
- `dagster.env`: `DAGSTER_HOME` and `DAGSTER_POSTGRES_URL`; no lake writer or
  provider credentials.
  Use the explicit SQLAlchemy driver URL `postgresql+psycopg2://USER:SECRET@HOST:PORT/ISOLATED_DB`.
  Dagster 1.13.28's notification query is incompatible with psycopg 3 server-side
  parameter binding; SQLAlchemy 2.1 changes the driver selected by plain
  `postgresql://`. The explicit driver was exercised by the restart test.
  See [SQLAlchemy's PostgreSQL driver change](https://docs.sqlalchemy.org/en/21/dialects/postgresql.html).
- Both code-location environments also require the separate Dagster metadata
  connection and common `DAGSTER_HOME` for run/event coordination.

Core and extension code locations have separate environment files and users.
The daemon uses the DefaultRunLauncher, which launches runs in their associated
gRPC code locations. Do not replace these with one shared credential-bearing
process and claim that schema names alone provide isolation.

Preinstall the pinned DuckDB extensions under a service-readable extension
cache for each execution identity. Do not depend on a developer's home cache;
the units protect `/home`. Normal task execution does not install extensions.

## Definition loading and trigger ownership

Copy the reviewed templates to the separately approved locations. The
workspace connects to core on loopback port 4261 and extension on 4262; UI is
loopback port 3260. Do not expose unauthenticated Dagster services publicly.

The core location loads only core declarations. The extension location declares
external core asset dependencies without importing core execution or plugin
implementations into the core process. Missing same-date materializations fail
closed. Each downstream invocation receives a JSON manifest with the upstream
publication and snapshot IDs; it never silently substitutes the previous day.

Definitions have **no automatically enabled schedule**. Enable schedules or
submit backfills only after shadow verification. A backfill uses one date per
run, and failures/retries remain local to that partition. An administrative
review must ensure only one scheduler has production trigger authority for a
dataset before any cutover. Historical stock snapshots cannot be reconstructed
by requesting today's provider snapshot with a historical target date.
The real code-location entry point now creates a STOPPED schedule by default
at 18:00 Asia/Shanghai. Set `QRP_LAKE_SCHEDULE_HOUR` independently in each
location environment (for example, core 18 and extension 19). Enabling a
schedule is a separate cutover action; an empty plugin allowlist has no job or
schedule to enable.

Core CLI invocations receive a stable identity derived from the Dagster root run,
dataset and partition. Retain `core-runtime/invocations/` alongside the Raw
batches it references: these files freeze the first upstream manifest, Raw
batch and CAS baseline for retries. Do not treat them as disposable attempt
logs. A manual CLI caller can supply `--invocation-id`; use a new ID for an
intentional acquisition/revision. This binding owns no retry/lease/job state.
Run-level reexecution keeps the root binding; receipts still record the actual
current Dagster run ID for audit.

Set conservative cross-run pools in the isolated Dagster instance:

```bash
dagster instance concurrency set qrp_core 1
dagster instance concurrency set qrp_extension 1
```

These limits permit one core and one extension asset at a time. Extension runs
also have a run-tag limit of one. The starting budget deliberately leaves a
core execution slot independent of extension occupancy. This is a bounded
starting configuration, not a measured throughput optimum. No custom
claim/lease/retry database is created; Dagster owns those lifecycles.

`DefaultRunLauncher` in this pinned Dagster version does not implement worker
health checks. Monitoring alone therefore does not detect a disappeared gRPC
run worker. The template adds a 7,200-second run deadline: after unexpected
code-location loss, a run/slot can remain occupied until that deadline and
monitoring cleanup. Tune the deadline to the whole job's budget (not one asset)
before deployment. The Linux recovery probe uses a 30-second deadline to
exercise this same path; it does not claim instant crash detection/resumption.

## Acceptance before production

1. Load both code locations independently; disable/break extension imports and
   confirm core discovery and a real core partition still work.
2. Materialize one partition, repeat it and confirm the same visible publication
   is reconciled rather than duplicate rows appended.
3. Cancel an extension with a child process, then exceed its memory budget;
   verify the whole cgroup is empty and core publication remains possible.
4. Restart the daemon/code location during a run. Check the run is terminal or
   explicitly resumed, and concurrency slots are released. Run monitoring and
   slot cleanup are configured; the configuration alone is not test evidence.
5. Attempt core-file and core-catalog writes under the extension identity and
   confirm rejection. Check that the core writer credentials cannot be read.
6. Run same-date competing publication and independent-date backfill tests,
   then verify exact input receipts and data visible at each pinned snapshot.
7. Compare core-alone versus extension-concurrent P50/P95, RSS, spill, queue
   wait and end-to-end timings using equal inputs and recorded thresholds.

The worker enforces timeout/cancellation and samples process-tree RSS and local
temporary/log bytes. On Windows this is not a cgroup or filesystem security
boundary. Linux unit `MemoryMax`, `KillMode=control-group`, credentials and
filesystem permissions must be verified on the target node. Deliberately
escaping process groups is outside the worker's polling guarantee.

No production installation, role creation, service restart, trigger switch or
data migration has been performed by adding these templates.

## Opt-in Linux verification entry points

`tests/pipeline/lake/linux_acceptance.py` exercises transient user units and
kernel memory limits; it explicitly reports a failure if the user manager does
not enforce filesystem restrictions. A property being accepted by systemd is
not enough evidence. `linux_system_acceptance.sh` is the reviewed privileged
supplement for the current isolated review path: transient system units only,
real Raw writes as `claire`, and a denied write as `nobody`. Inspect its fixed
paths before reusing it on another machine. These probes use
`ProtectHome=read-only` for isolated source under home; the final `/opt` layout
with `ProtectHome=true` and production service accounts still needs deployment
acceptance.

`tests/pipeline/lake/linux_dagster_recovery.py --root <isolated-review-root>`
uses a new metadata database on the explicit loopback test instance, restarts
the daemon, kills the code location during a real worker run, checks terminal
failure/child cleanup/slot recovery, then completes a second run. It does not
enable schedules. Logs and test databases are retained as evidence; transient
code-location and daemon services are stopped in its `finally` block.

## API references used

- [Dagster assets](https://dagster.io/docs/api/dagster/assets)
- [Concurrency pools](https://dagster.io/docs/guides/operate/managing-concurrency/concurrency-pools)
- [Instance configuration and run monitoring](https://dagster.io/docs/deployment/oss/oss-instance-configuration)
