#!/usr/bin/env bash
# Failure injection against the ../ compose stack in remote-direct mode:
# tests/failures.test.ts (worker kill, no re-run, drain, rolling restart, no
# workers), then a worker with a wrong INSTANCE_SECRET.
# Uses the already-built images; set BUILD=1 to rebuild them first.
set -euo pipefail
cd "$(dirname "$0")"
export INSTANCE_SECRET=${INSTANCE_SECRET:-$(openssl rand -hex 32)}
export FUNCTION_RUNNER=remote FUNRUN_ROUTING=direct FUNRUN_WORKERS=worker:7400
if command -v pnpm >/dev/null; then pm=pnpm; else pm=npm; fi
$pm install --silent

compose() { (cd .. && docker compose "$@"); }
trap 'compose down -v >/dev/null 2>&1 || true' EXIT
[[ ${BUILD:-} == 1 ]] && compose build conductor worker
compose down -v >/dev/null 2>&1 || true
compose up -d --wait --scale worker=2 conductor worker
admin_key=$(compose exec -T conductor ./generate_admin_key.sh | tail -1)
CONVEX_SELF_HOSTED_URL=http://127.0.0.1:3210 CONVEX_SELF_HOSTED_ADMIN_KEY=$admin_key npx convex deploy --yes
$pm test tests/failures.test.ts

echo "=== bad token: worker with a wrong INSTANCE_SECRET ==="
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
# The secret must be hex (a literal "wrong" crashes the worker at startup
# with "Couldn't hexdecode key"), just not the conductor's. --use-aliases
# puts it behind the `worker` DNS name, so the conductor discovers it.
bad=$(compose run -d --use-aliases -e INSTANCE_SECRET="$(openssl rand -hex 32)" worker)
trap 'docker rm -f "$bad" >/dev/null 2>&1; compose down -v >/dev/null 2>&1 || true' EXIT
sleep 10 # > one DNS refresh (5 s) + the first WatchLoad
bad_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$bad")
[[ -n $bad_ip ]] || { echo "FAIL: bad-token worker is not running" >&2; docker logs "$bad" >&2; exit 1; }
conductor_log=$(compose logs --since "$since" conductor)
worker_log=$(docker logs "$bad" 2>&1)
# Conductor: its WatchLoad to that worker is rejected (Unauthenticated).
grep -m3 "$bad_ip.*invalid funrun bearer token" <<<"$conductor_log" \
  || { echo "FAIL: conductor did not log the rejected WatchLoad for $bad_ip" >&2; exit 1; }
# Worker: it rejected the conductor's calls.
grep -m3 'Unauthenticated funrun call rejected' <<<"$worker_log" \
  || { echo "FAIL: bad-token worker did not log Unauthenticated" >&2; tail -20 <<<"$worker_log" >&2; exit 1; }
# And it never got work: the other replicas still serve.
grep -q 'isolate worker' <<<"$worker_log" && { echo "FAIL: bad-token worker ran a function" >&2; exit 1; }
echo "bad token: PASS"
