# funrun compose stack

Conductor (`convex-local-backend`, `FUNCTION_RUNNER=remote`) + N stateless
`funrun_worker`s + Postgres + RustFS (S3-compatible, replaces MinIO) + an
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
`FUNCTION_MAX_*`, `ISOLATE_MAX_USER_HEAP_SIZE`, `DATABASE_UDF_USER_TIMEOUT_SECONDS`,
`V8_ACTION_USER_TIMEOUT_SECS`) to the upstream defaults in
`crates/common/src/knobs.rs` on both the conductor and every worker, so the
two sides can't silently drift apart. It's loaded via `env_file:` on both
services; it doesn't tune anything.

## RustFS storage round trip

If a deploy's `ctx.storage.store`/`ctx.storage.get` action fails against
RustFS, try disabling S3 features one at a time in `x-common-env`:
`AWS_S3_DISABLE_SSE=true`, then `AWS_S3_DISABLE_CHECKSUMS=true`.

## Deviations from the brief

- **conductor `command`**: the brief's literal
  `["convex-local-backend", "--s3-storage", "--instance-name", "convex-self-hosted"]`
  can't actually authenticate or reach Postgres: `crates/local_backend/src/config.rs`'s
  `LocalConfig` only has `env = "..."` on the four new remote-runner flags
  (`FUNCTION_RUNNER`, `FUNRUN_WORKERS`, `FUNRUN_ROUTING`, `FUNCTION_HOST_LISTEN`);
  `instance_name`/`instance_secret`/`db`/`db_spec`/`s3_storage` are CLI-only, so
  the given command would run without `--instance-secret` (clap requires it once
  `--instance-name` is passed) and would default to SQLite (no `POSTGRES_URL`
  reader on the Rust side). `self-hosted/docker-build/run_backend.sh` is exactly
  the existing env->flag translator for those fields (it already reads
  `INSTANCE_NAME`, `INSTANCE_SECRET`, `POSTGRES_URL`, `DO_NOT_REQUIRE_SSL`, the
  `S3_*` vars) and is already copied into the image by the reused Dockerfile
  stages, so `conductor.command` is `["./run_backend.sh"]` instead. The new
  remote-runner flags are still read straight from the environment by clap
  (they have `env = "..."` attrs), so no change to `run_backend.sh` was needed.
- **Dockerfile has no `ENTRYPOINT`** (`Dockerfile.backend` sets
  `ENTRYPOINT ["./run_backend.sh"]`): this image is shared by both the
  conductor and worker services, and the worker must exec `./funrun_worker`
  directly (its `WorkerConfig` *is* fully env-wired). Each service supplies
  its own full command instead.
- **`cargo chef prepare` has no `--bin` filter** (brief said
  `--bin convex-local-backend`, add `-p funrun_worker --bin funrun_worker` to
  the build): `cargo-chef prepare --bin` can only be passed once (confirmed via
  `cargo chef prepare --help`), so scoping to one binary isn't possible when
  building two. The recipe now covers the whole workspace instead; `cargo chef
  cook`/`cargo build` are unaffected (they already build everything needed for
  both binaries).
- **`V8_ACTION_USER_TIMEOUT_SECS` in `knobs.env` is `1800`**, not the brief's
  placeholder `600` — `crates/common/src/knobs.rs` defines the upstream
  default as `1800` (`NODE_ACTION_USER_TIMEOUT_SECS` is the one defaulting to
  `600`). The brief said to replace every placeholder with the real upstream
  default, so `1800` is used. Envoy's `timeout: 2100s` (35 min) is kept as
  specified — a buffer above the 30-minute V8 action timeout.
