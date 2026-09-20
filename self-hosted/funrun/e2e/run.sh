#!/usr/bin/env bash
# Runs the equivalence suite against the ../ compose stack in each config:
#   local  - FUNCTION_RUNNER=local (conductor runs UDFs itself)
#   direct - FUNCTION_RUNNER=remote, FUNRUN_ROUTING=direct (DNS -> workers)
#   proxy  - FUNCTION_RUNNER=remote, FUNRUN_ROUTING=proxy (via Envoy)
#   node   - direct routing plus a node-worker pool for "use node" actions
# Usage: ./run.sh [config...]   (default: local direct proxy node)
# Uses the already-built images; set BUILD=1 to rebuild them first.
set -euo pipefail
cd "$(dirname "$0")"
export INSTANCE_SECRET=${INSTANCE_SECRET:-$(openssl rand -hex 32)}
if command -v pnpm >/dev/null; then pm=pnpm; else pm=npm; fi
$pm install --silent

compose() { (cd .. && docker compose --profile proxy --profile node "$@"); }
trap 'compose down -v >/dev/null 2>&1 || true' EXIT
[[ ${BUILD:-} == 1 ]] && compose build conductor worker node-worker dashboard

for cfg in ${@:-local direct proxy node}; do
  case $cfg in
    local)  export FUNCTION_RUNNER=local FUNRUN_ROUTING=direct FUNRUN_WORKERS=worker:7400 FUNRUN_NODE_WORKERS= ;;
    direct) export FUNCTION_RUNNER=remote FUNRUN_ROUTING=direct FUNRUN_WORKERS=worker:7400 FUNRUN_NODE_WORKERS= ;;
    proxy)  export FUNCTION_RUNNER=remote FUNRUN_ROUTING=proxy FUNRUN_WORKERS=envoy:7400 FUNRUN_NODE_WORKERS= ;;
    node)   export FUNCTION_RUNNER=remote FUNRUN_ROUTING=direct FUNRUN_WORKERS=worker:7400 FUNRUN_NODE_WORKERS=node-worker:7400 ;;
    *) echo "unknown config $cfg" >&2; exit 2 ;;
  esac
  echo "=== $cfg: FUNCTION_RUNNER=$FUNCTION_RUNNER FUNRUN_ROUTING=$FUNRUN_ROUTING FUNRUN_WORKERS=$FUNRUN_WORKERS FUNRUN_NODE_WORKERS=$FUNRUN_NODE_WORKERS ==="
  compose down -v >/dev/null 2>&1 || true
  extra=()
  [[ $cfg == proxy ]] && extra+=(envoy)
  [[ $cfg == node ]] && extra+=(node-worker)
  compose up -d --wait --scale worker=2 conductor worker "${extra[@]}"
  admin_key=$(compose exec -T conductor ./generate_admin_key.sh | tail -1)
  CONVEX_SELF_HOSTED_URL=http://127.0.0.1:3210 CONVEX_SELF_HOSTED_ADMIN_KEY=$admin_key npx convex deploy --yes
  # Where did the user code run? Isolate threads log "Created <pool> isolate worker <n>".
  # For a remote config the conductor must create none, including at deploy.
  isolates() { compose logs "$1" | grep -c 'isolate worker' || true; }
  if [[ $cfg == node ]]; then
    export EXPECT_NODE_HOST_PREFIX=$(compose ps -q node-worker | cut -c1-12)
    # An empty prefix makes node.test.ts skip the "ran on the node worker" check.
    [[ -n $EXPECT_NODE_HOST_PREFIX ]] || { echo "FAIL: no node-worker container id" >&2; exit 1; }
    $pm test tests/equivalence.test.ts tests/node.test.ts
  else
    $pm test tests/equivalence.test.ts
  fi
  conductor_isolates=$(isolates conductor)
  worker_isolates=$(isolates worker)
  echo "isolate creations: conductor=$conductor_isolates workers=$worker_isolates"
  compose logs conductor worker | grep 'isolate worker' || true
  if [[ $FUNCTION_RUNNER == remote && ( $worker_isolates == 0 || $conductor_isolates != 0 ) ]]; then
    echo "FAIL: remote config but tests did not run only on workers" >&2; exit 1
  fi
  if [[ $FUNCTION_RUNNER == local && $worker_isolates != 0 ]]; then
    echo "FAIL: local config but a worker created an isolate" >&2; exit 1
  fi

  if [[ $cfg == node ]]; then
    # The conductor must not have spawned its own Node subprocess for the
    # "use node" action; it must have run entirely on node-worker.
    if compose exec -T conductor pgrep -f local.cjs >/dev/null; then
      echo "FAIL: conductor ran a Node process for a use-node action" >&2; exit 1
    fi
    curl -sf -H "Authorization: Convex $admin_key" localhost:3210/api/funrun/status \
      | jq -e '.pools.node | length > 0 and all(.healthy)' >/dev/null \
      || { echo "FAIL: node pool not healthy in /api/funrun/status" >&2; exit 1; }
    # Verified once against this admin check: unauthenticated requests get 403.
    status_code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3210/api/funrun/status)
    if [[ $status_code != 403 ]]; then
      echo "FAIL: unauthenticated /api/funrun/status returned $status_code, want 403" >&2; exit 1
    fi

    # Fallback: kill the node-worker pool, restart the conductor so it falls
    # back to the in-process LocalNodeExecutor, and check the counter.
    compose stop node-worker
    FUNRUN_FALLBACK=local compose up -d --wait conductor
    sleep 2
    unset EXPECT_NODE_HOST_PREFIX
    $pm test tests/node.test.ts
    fallback=$(curl -s localhost:9101/metrics | grep 'funrun_fallback_total{kind="node"}' | awk '{print $NF}')
    if [[ -z ${fallback:-} || ! $fallback =~ ^[0-9]+$ || $fallback -lt 1 ]]; then
      echo "FAIL: funrun_fallback_total{kind=\"node\"} not >= 1 (got '${fallback:-<none>}')" >&2
      exit 1
    fi
  fi
done
