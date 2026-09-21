#!/usr/bin/env bash
# Load balancing across both pools, against the ../ compose stack in
# remote-direct mode with two of each worker: tests/balance.test.ts checks that
# a module keeps to its home isolate worker, that distinct modules reach both,
# that a saturated worker spills to its neighbour, and that node actions spread
# across both node workers.
# Uses the already-built images; set BUILD=1 to rebuild them first.
set -euo pipefail
cd "$(dirname "$0")"
export INSTANCE_SECRET=${INSTANCE_SECRET:-$(openssl rand -hex 32)}
export FUNCTION_RUNNER=remote FUNRUN_ROUTING=direct FUNRUN_WORKERS=worker:7400
export FUNRUN_NODE_WORKERS=node-worker:7400
if command -v pnpm >/dev/null; then pm=pnpm; else pm=npm; fi
$pm install --silent

compose() { (cd .. && docker compose --profile node "$@"); }
trap 'compose down -v >/dev/null 2>&1 || true' EXIT
[[ ${BUILD:-} == 1 ]] && compose build conductor worker node-worker
compose down -v >/dev/null 2>&1 || true
# Two of each: one worker per pool can't show a split, and the spill test needs
# somewhere for the overflow to go.
compose up -d --wait --scale worker=2 --scale node-worker=2 conductor worker node-worker

admin_key=$(compose exec -T conductor ./generate_admin_key.sh | tail -1)
CONVEX_SELF_HOSTED_URL=http://127.0.0.1:3210 CONVEX_SELF_HOSTED_ADMIN_KEY=$admin_key npx convex deploy --yes

# The tests read each replica's metrics, so fail early and clearly if a pool
# did not come up with two.
for svc in worker node-worker; do
  n=$(compose ps -q "$svc" | grep -c . || true)
  [[ $n == 2 ]] || { echo "FAIL: expected 2 $svc replicas, got $n" >&2; exit 1; }
done

$pm test tests/balance.test.ts
