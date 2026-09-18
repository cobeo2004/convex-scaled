#!/usr/bin/env bash
# Runs the equivalence suite against the ../ compose stack in each config:
#   local  - FUNCTION_RUNNER=local (conductor runs UDFs itself)
#   direct - FUNCTION_RUNNER=remote, FUNRUN_ROUTING=direct (DNS -> workers)
#   proxy  - FUNCTION_RUNNER=remote, FUNRUN_ROUTING=proxy (via Envoy)
# Usage: ./run.sh [config...]   (default: local direct proxy)
# Uses the already-built images; set BUILD=1 to rebuild them first.
set -euo pipefail
cd "$(dirname "$0")"
export INSTANCE_SECRET=${INSTANCE_SECRET:-$(openssl rand -hex 32)}
if command -v pnpm >/dev/null; then pm=pnpm; else pm=npm; fi
$pm install --silent

compose() { (cd .. && docker compose --profile proxy "$@"); }
trap 'compose down -v >/dev/null 2>&1 || true' EXIT
[[ ${BUILD:-} == 1 ]] && compose build conductor worker

for cfg in ${@:-local direct proxy}; do
  case $cfg in
    local)  export FUNCTION_RUNNER=local FUNRUN_ROUTING=direct FUNRUN_WORKERS=worker:7400 ;;
    direct) export FUNCTION_RUNNER=remote FUNRUN_ROUTING=direct FUNRUN_WORKERS=worker:7400 ;;
    proxy)  export FUNCTION_RUNNER=remote FUNRUN_ROUTING=proxy FUNRUN_WORKERS=envoy:7400 ;;
    *) echo "unknown config $cfg" >&2; exit 2 ;;
  esac
  echo "=== $cfg: FUNCTION_RUNNER=$FUNCTION_RUNNER FUNRUN_ROUTING=$FUNRUN_ROUTING FUNRUN_WORKERS=$FUNRUN_WORKERS ==="
  compose down -v >/dev/null 2>&1 || true
  compose up -d --wait --scale worker=2 conductor worker $([[ $cfg == proxy ]] && echo envoy)
  admin_key=$(compose exec -T conductor ./generate_admin_key.sh | tail -1)
  CONVEX_SELF_HOSTED_URL=http://127.0.0.1:3210 CONVEX_SELF_HOSTED_ADMIN_KEY=$admin_key npx convex deploy --yes
  # Where did the user code run? Isolate threads log "Created <pool> isolate worker <n>".
  # The conductor always creates some while deploying (push-time analyze runs locally).
  isolates() { compose logs "$1" | grep -c 'isolate worker' || true; }
  conductor_before=$(isolates conductor)
  $pm test
  conductor_isolates=$(isolates conductor)
  worker_isolates=$(isolates worker)
  echo "isolate creations: conductor=$conductor_isolates (at deploy: $conductor_before) workers=$worker_isolates"
  compose logs conductor worker | grep 'isolate worker' || true
  if [[ $FUNCTION_RUNNER == remote && ( $worker_isolates == 0 || $conductor_isolates != "$conductor_before" ) ]]; then
    echo "FAIL: remote config but tests did not run only on workers" >&2; exit 1
  fi
  if [[ $FUNCTION_RUNNER == local && $worker_isolates != 0 ]]; then
    echo "FAIL: local config but a worker created an isolate" >&2; exit 1
  fi
done
