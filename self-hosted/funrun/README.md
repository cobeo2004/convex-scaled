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

`FUNCTION_RUNNER`, `FUNRUN_WORKERS`, `FUNRUN_ROUTING`, `CONDUCTOR_CPUS`,
`WORKER_CPUS` are all overridable env vars (defaults: `remote`, `worker:7400`,
`direct`, `4`, `4`).

To route through Envoy instead of direct DNS-based routing:

```sh
docker compose --profile proxy up -d --build --scale worker=2
FUNRUN_ROUTING=proxy FUNRUN_WORKERS=envoy:7400 docker compose up -d conductor
```

Bring it down: `docker compose down` (add `-v` to also drop the Postgres/RustFS
volumes).

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
- `AWS_S3_DISABLE_SSE: "true"` is set because RustFS rejects multipart uploads
  without a KMS/SSE-S3 key configured.

## E2E equivalence suite

`e2e/run.sh [local|direct|proxy ...]` brings the stack up once per config
(default: all three), deploys `e2e/convex/`, runs `e2e/tests/` with vitest,
checks that remote configs created isolates only on workers, and tears the stack
down (`-v`). Uses the existing images; `BUILD=1` rebuilds first.
