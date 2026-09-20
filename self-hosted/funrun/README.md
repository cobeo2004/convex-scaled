# funrun compose stack

Conductor (`convex-local-backend`, `FUNCTION_RUNNER=remote`) + N stateless
`funrun_worker`s + Postgres + RustFS (S3-compatible, replaces MinIO) + the
Convex dashboard on http://127.0.0.1:6791 (log in with the admin key below) + an
optional Envoy proxy for `FUNRUN_ROUTING=proxy`.

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
  worker: `fail` (default) rejects the action; `local` runs it in-process on
  the conductor instead and increments `funrun_fallback_total{kind="node"}`.
- Railway (or any host with a separate Node service): the Node service's
  draining time must be at least 11 minutes (`stop_grace_period` /
  `NODE_ACTION_USER_TIMEOUT` + drain), and `FUNRUN_NODE_CALLBACK_ORIGIN` must
  point back at the conductor, e.g.
  `http://conductor.railway.internal:3210`, not a public/loopback origin.

## Health

- `GET /api/funrun/status` -- admin-only JSON (`Authorization: Convex
  <admin_key>`) with the isolate and Node pool state (`pools.isolate`,
  `pools.node`) and the fallback counters.
- `GET /funrun/status` -- a self-contained HTML page that polls the JSON
  endpoint above; open http://127.0.0.1:3210/funrun/status after logging in.
- The dashboard's Workers page (http://127.0.0.1:6791/workers) renders the
  same data.
- Conductor Prometheus metrics are on `:9101`
  (`FUNRUN_CONDUCTOR_METRICS_LISTEN`), separate from each worker's own `:9100`
  (`FUNRUN_METRICS_LISTEN`). The conductor exposes, per pool it routes to
  (`isolate`/`node`): `funrun_worker_load_info{pool,addr}` (last reported load
  per worker), `funrun_pool_healthy_info{pool}` (healthy worker count), and
  `funrun_fallback_total{kind="isolate"|"deploy"|"node"}` (in-process
  fallbacks).

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
checks that remote configs (`direct`/`proxy`/`node`) create zero isolates on
the conductor -- including at deploy time -- and only on workers, and tears
the stack down (`-v`). Uses the existing images; `BUILD=1` rebuilds first
(conductor, worker, node-worker and dashboard).

The `node` config additionally brings up `node-worker`, sets
`FUNRUN_NODE_WORKERS=node-worker:7400`, and runs `e2e/tests/node.test.ts`
(a real `"use node"` action using `node:crypto`/`node:os`), which checks the
action ran on the worker's container hostname, not the conductor's. It then
checks the conductor spawned no local Node subprocess, that
`/api/funrun/status` reports a healthy `node` pool and 403s without an admin
key, and exercises the `node` fallback path (stop `node-worker`, restart the
conductor with `FUNRUN_FALLBACK=local`, rerun the Node test, and check
`funrun_fallback_total{kind="node"}` on `:9101/metrics`).
