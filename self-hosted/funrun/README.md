# funrun compose stack

Conductor (`convex-local-backend`, `FUNCTION_RUNNER=remote`) + N stateless
`funrun_worker`s + Postgres + RustFS (S3-compatible, replaces MinIO) + the
Convex dashboard on http://127.0.0.1:6791 (log in with the admin key below) + an
optional Envoy proxy for `FUNRUN_ROUTING=proxy`.

## Architecture

![funrun topology](docs/architecture.png)

The conductor owns every piece of durable state — Postgres, S3, the scheduler,
the HTTP routes, subscriptions and the whole transaction lifecycle. Workers own
none of it. They receive a request, run JavaScript, and hand back an outcome;
every read they need travels back to the conductor over gRPC, and only the
conductor commits. That is what makes a pool safe to scale by adding containers
and safe to lose mid-request.

Three pieces live inside the conductor process and are easy to confuse:

| Piece               | Crate                    | Job                                                          |
| ------------------- | ------------------------ | ------------------------------------------------------------ |
| `WorkerPool` client | `remote_function_runner` | Picks a worker, dispatches, retries, tracks health           |
| `FunctionHost`      | `function_host`          | gRPC **server** answering worker callbacks (reads, syscalls) |
| Isolate host        | `function_runner`        | The real V8 host — runs inside the _worker_, not here        |

`FUNCTION_RUNNER=local` collapses the entire right-hand side back into the
conductor and is byte-for-byte upstream behaviour: no pool, no callbacks, no
extra process. Everything below is remote mode only.

Each diagram below is also a self-contained interactive page under `docs/` —
open the `.html` next to the image for guided views, search and relationship
tracing.

### Conductor: how a call reaches a worker

[![conductor routing](docs/conductor.png)](docs/conductor.html)

`pick()` uses rendezvous hashing on the module path, so one module tends to keep
landing on the same worker and reusing its warm isolate. It spills to another
worker when the first is at capacity or measurably busier (`LOAD_SPILL_MARGIN`,
20%). Health and load come from a background `watch_load` stream plus a 5s DNS
refresh loop — never from the request path.

Retries are keyed on a `RequestKind`, not on the connection:

| Kind                  | Before `Started` | After `Started` | Refused budget |
| --------------------- | ---------------- | --------------- | -------------- |
| `Run(Query/Mutation)` | retry            | retry (pure)    | 1s             |
| `Run(Action)`         | retry            | **no**          | 30s            |
| `Run(HttpAction)`     | retry            | **no**          | 1s             |
| `Deploy`, `NodePure`  | retry            | retry (pure)    | 1s             |
| `NodeExecute`         | retry            | **no**          | 30s            |

A `Refused` outcome means the worker was overloaded _before_ user code ran, so
it is always safe to re-send: the conductor backs off 50ms → 2s with ±50% jitter
until the budget for that kind is spent. Actions get 30s because failing one
strands a committed run; a user-facing read fails fast after 1s. When a pool has
no healthy worker at all, `FUNRUN_FALLBACK` decides between failing the call and
running it in-process.

### Isolate worker: one request

[![isolate worker request](docs/isolate-worker.png)](docs/isolate-worker.html)

`FUNRUN_KIND=isolate` (the default) serves queries, mutations, actions, HTTP
actions and deploy-time evaluation — `analyze`, schema validation and component
push all run here under the real per-evaluation limits, so the conductor creates
**zero** isolates in remote mode. The worker loads user modules straight from
S3, which is why remote mode requires shared storage rather than the conductor's
local filesystem.

Each attempt is one fresh bidirectional `Execute` stream. Log lines and HTTP
response chunks stream back while user code is still running. `Started` is the
retry boundary: once that frame is sent, the conductor knows user code may have
had side effects.

### Node worker: one `"use node"` action

[![node worker action](docs/node-worker.png)](docs/node-worker.html)

`"use node"` actions need a real Node.js runtime (npm packages, node builtins),
which a V8 isolate cannot provide. Setting `FUNRUN_NODE_WORKERS=host:port`
routes them to a separate `FUNRUN_KIND=node` pool; leave it unset and they stay
on the conductor exactly as upstream. Each worker holds
`FUNRUN_NODE_MAX_CONCURRENT` slots (4 by default).

Two origins do two different jobs, and mixing them up is the most common
misconfiguration:

- **`FUNRUN_NODE_CALLBACK_ORIGIN`** is embedded as the action's
  `backendAddress`. Node syscalls — `ctx.runQuery`, `ctx.runMutation`,
  `ctx.scheduler` — go back to the conductor through it. These do _not_ route
  through `FunctionHost`, which is the isolate-only callback path.
- **`CONVEX_CLOUD_ORIGIN`** is baked into the `ctx.storage` URLs the action
  fetches itself. A loopback value works on the conductor and fails on a worker,
  so the conductor warns about it at startup.

See [Node workers](#node-workers) below for the operational knobs (drain
timeouts, fallback, Railway).

## Run it

```sh
cd self-hosted/funrun
export INSTANCE_SECRET=$(openssl rand -hex 32)
docker compose up -d --build --scale worker=2
docker compose logs conductor | grep -i "function_host\|error" | head
docker compose exec conductor ./generate_admin_key.sh
```

`FUNCTION_RUNNER`, `FUNRUN_WORKERS`, `FUNRUN_ROUTING`, `FUNCTION_HOST_LISTEN`,
`FUNRUN_LISTEN`, `CONDUCTOR_CPUS`, `WORKER_CPUS`, `FUNRUN_NODE_WORKERS`,
`FUNRUN_FALLBACK`, `FUNRUN_NODE_CALLBACK_ORIGIN`, `FUNRUN_KIND`,
`FUNRUN_NODE_MAX_CONCURRENT`, `FUNRUN_NODE_DRAIN_TIMEOUT_SECS` are all
overridable env vars (defaults: `remote`, `worker:7400`, `direct`,
`0.0.0.0:7401`, `0.0.0.0:7400`, `4`, `4`, unset, `fail`,
`http://conductor:3210`, `isolate`, `4`, `NODE_ACTION_USER_TIMEOUT + 30s`).
`FUNRUN_KIND` is fixed per service in compose (`worker` takes the default and
`node-worker` sets `node`); it only matters when running the binary directly.

To route through Envoy instead of direct DNS-based routing:

```sh
docker compose --profile proxy up -d --build --scale worker=2
FUNRUN_ROUTING=proxy FUNRUN_WORKERS=envoy:7400 docker compose up -d conductor
```

Bring it down: `docker compose down` (add `-v` to also drop the Postgres/RustFS
volumes).

## Node workers

`"use node"` actions run in-process on the conductor (via `LocalNodeExecutor`)
unless `FUNRUN_NODE_WORKERS` is set, in which case they run on a separate,
stateless `node-worker` pool -- same idea as the isolate `worker` pool, but for
Node.

```sh
FUNRUN_NODE_WORKERS=node-worker:7400 \
  docker compose --profile node up -d --build --scale worker=2
```

- Unset (the default) means Node runs on the conductor, exactly like upstream.
- `FUNRUN_FALLBACK` controls what happens when the Node pool has no healthy
  worker: `fail` (default) rejects the action; `local` runs it in-process on the
  conductor instead and increments `funrun_fallback_total{kind="node"}`.
- Railway (or any host with a separate Node service): the Node service's
  draining time must be at least 11 minutes (`stop_grace_period` /
  `NODE_ACTION_USER_TIMEOUT` + drain), and `FUNRUN_NODE_CALLBACK_ORIGIN` must
  point back at the conductor, e.g. `http://conductor.railway.internal:3210`,
  not a public/loopback origin.
- `CONVEX_CLOUD_ORIGIN` must also resolve on the Node workers. File storage
  inside a `"use node"` action (`ctx.storage.store` / `.get`) fetches URLs built
  from that origin, so the conductor's own loopback address fails there, and the
  conductor warns at startup when a Node pool is configured with one. Isolate
  workers are unaffected: their storage calls go through the conductor as
  syscalls and never fetch that URL.

## Health

- `GET /api/funrun/status` -- admin-only JSON
  (`Authorization: Convex <admin_key>`) with the isolate and Node pool state
  (`pools.isolate`, `pools.node`) and the fallback counters.
- `GET /funrun/status` -- a self-contained HTML page that polls the JSON
  endpoint above; open http://127.0.0.1:3210/funrun/status after logging in.
- The dashboard's Workers page (http://127.0.0.1:6791/workers) renders the same
  data.
- Conductor Prometheus metrics are on `:9101`
  (`FUNRUN_CONDUCTOR_METRICS_LISTEN`), separate from each worker's own `:9100`
  (`FUNRUN_METRICS_LISTEN`). The conductor exposes, per pool it routes to
  (`isolate`/`node`): `funrun_worker_load_info{pool,addr}` (last reported load
  per worker), `funrun_pool_healthy_info{pool}` (healthy worker count), and
  `funrun_fallback_total{kind="isolate"|"deploy"|"node"}` (in-process
  fallbacks). The exporter prefixes each of these with `convex_local_backend_`,
  so scrape e.g. `convex_local_backend_funrun_fallback_total`.

## knobs.env

Pins the transaction/function/isolate limits (`TRANSACTION_MAX_*`,
`FUNCTION_MAX_*`, `ISOLATE_MAX_USER_HEAP_SIZE`,
`DATABASE_UDF_USER_TIMEOUT_SECONDS`, `V8_ACTION_USER_TIMEOUT_SECS`) to the
upstream defaults in `crates/common/src/knobs.rs` on both the conductor and
every worker, so the two sides can't silently drift apart. It's loaded via
`env_file:` on both services; it doesn't tune anything.

## RustFS storage round trip

If a deploy's `ctx.storage.store`/`ctx.storage.get` action fails against RustFS,
try disabling S3 features one at a time in `x-common-env`:
`AWS_S3_DISABLE_SSE=true`, then `AWS_S3_DISABLE_CHECKSUMS=true`.

## Credentials

`RUSTFS_ACCESS_KEY`/`RUSTFS_SECRET_KEY` default to `rustfsadmin`/`rustfsadmin`
and are overridable env vars; **defaults are for local development only.**

## Operational notes

- `conductor.command` is `["./run_backend.sh"]` (not a direct
  `convex-local-backend` invocation) because `run_backend.sh` is the existing
  env->CLI-flag translator for `--instance-name`/`--instance-secret`/
  `--s3-storage`/Postgres, which `LocalConfig`'s clap doesn't read from the
  environment directly.
- The Dockerfile has no `ENTRYPOINT` because the same image serves both the
  conductor (`./run_backend.sh`) and worker (`./funrun_worker`) services, each
  supplying its own `command:`.
- Listeners default to `0.0.0.0` because containers without IPv6 (Docker
  Desktop) can't bind `[::]`. On IPv6 private networks (Railway) set
  `FUNRUN_LISTEN=[::]:7400` and `FUNCTION_HOST_LISTEN=[::]:7401`. When a
  worker's DNS name returns both families, the conductor routes within the
  family the resolver returns first, so each worker counts once, and falls back
  to the other family while none of the preferred addresses is healthy.
- `AWS_S3_DISABLE_SSE: "true"` is set because RustFS rejects multipart uploads
  without a KMS/SSE-S3 key configured.

## E2E equivalence suite

`e2e/run.sh [local|direct|proxy|node ...]` brings the stack up once per config
(default: all four), deploys `e2e/convex/`, runs `e2e/tests/` with vitest,
checks that remote configs (`direct`/`proxy`/`node`) create zero isolates on the
conductor -- including at deploy time -- and only on workers, and tears the
stack down (`-v`). Uses the existing images; `BUILD=1` rebuilds first
(conductor, worker, node-worker and dashboard).

The `node` config additionally brings up `node-worker`, sets
`FUNRUN_NODE_WORKERS=node-worker:7400`, and runs `e2e/tests/node.test.ts` (a
real `"use node"` action using `node:crypto`/`node:os`), which checks the action
ran on the worker's container hostname, not the conductor's. It then checks the
conductor spawned no local Node subprocess, that `/api/funrun/status` reports a
healthy `node` pool and 403s without an admin key, and exercises the `node`
fallback path (stop `node-worker`, restart the conductor with
`FUNRUN_FALLBACK=local`, rerun the Node test, and check
`funrun_fallback_total{kind="node"}` on `:9101/metrics`).
