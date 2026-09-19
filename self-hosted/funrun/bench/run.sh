#!/usr/bin/env bash
# B0-B4 benchmark of the funrun compose stack (../). Results: RESULTS.md.
#   B0    FUNCTION_RUNNER=local, conductor 8 CPUs, 0 workers
#   B1-N  remote, conductor 2 CPUs, N workers x 2 CPUs (N = 1, 2, 3)
#   B2    remote, conductor 2 + 3 workers x 2 (same layout as B1-3, run again)
#   B3    B1-3 + 10 concurrent actions:burnCpu {ms: 30000}
#   B4    B2 via Envoy (FUNRUN_ROUTING=proxy)
# Usage: ./run.sh [run...]   (default: all of the above)
# Knobs: WARMUP/DURATION seconds (30/120), SCALE (multiplies every scenario's
# thread count in workload.json), OUT (results dir, default ./out).
# Needs the images built (docker compose build), node, pnpm/npm, python3.
set -euo pipefail
cd "$(dirname "$0")"
bench=$PWD
repo=$(cd ../../.. && pwd)
out=${OUT:-$bench/out}
WARMUP=${WARMUP:-30} DURATION=${DURATION:-120} SCALE=${SCALE:-1}
export INSTANCE_SECRET=${INSTANCE_SECRET:-$(openssl rand -hex 32)}
compose() { (cd .. && docker compose --profile proxy "$@"); }
# macOS: keep the host awake (a lid-close on battery still sleeps it; summarize.py
# reports max_sample_gap_s so a run that slept is visible).
command -v caffeinate >/dev/null && { caffeinate -dimsu -w $$ & }
trap 'compose down -v >/dev/null 2>&1 || true; jobs -p | xargs kill 2>/dev/null || true' EXIT

lg=$repo/target/release/load-generator
[[ -x $lg ]] || (cd "$repo" && cargo build --release -p load_generator --bin load-generator)
[[ -f $repo/npm-packages/scenario-runner/dist/scenario-runner.js ]] ||
  (cd "$repo/npm-packages/scenario-runner" && npm run build)
if command -v pnpm >/dev/null; then pm=pnpm; else pm=npm; fi
(cd ../e2e && $pm install --silent)
mkdir -p "$out"
workload=$out/workload.json
python3 -c 'import json,sys; w=json.load(open(sys.argv[1]))
for s in w["scenarios"]: s["benchmark"]*=int(sys.argv[2])
json.dump(w,open(sys.argv[3],"w"))' workload.json "$SCALE" "$workload"

# Every worker's /metrics (FUNRUN_METRICS_LISTEN) into $1; $2 = worker count.
scrape_workers() {
  : >"$1"
  for ((i = 1; i <= $2; i++)); do
    compose exec -T --index "$i" worker curl -s localhost:9100/metrics >>"$1"
  done
}

run() {
  local name=$1 workers=$2 dir=$out/$1
  echo "=== $name: FUNCTION_RUNNER=$FUNCTION_RUNNER CONDUCTOR_CPUS=$CONDUCTOR_CPUS workers=$workers x $WORKER_CPUS routing=$FUNRUN_ROUTING ==="
  mkdir -p "$dir"
  # Other work on the host skews every number: wait (up to 30 min) for the
  # 1-minute load average to drop below 3. summarize.py reports the load seen.
  for _ in $(seq 180); do
    awk '{exit !($2 < 3)}' <(sysctl -n vm.loadavg) && break
    sleep 10
  done
  compose down -v >/dev/null 2>&1 || true
  compose up -d --wait conductor \
    $([[ $workers != 0 ]] && echo "--scale worker=$workers worker") $([[ $FUNRUN_ROUTING == proxy ]] && echo envoy) >"$dir/up.log" 2>&1
  local key
  key=$(compose exec -T conductor ./generate_admin_key.sh | tail -1)
  (cd ../e2e && CONVEX_SELF_HOSTED_URL=http://127.0.0.1:3210 CONVEX_SELF_HOSTED_ADMIN_KEY=$key \
    npx convex deploy --yes >"$dir/deploy.log" 2>&1)
  local burners=()
  if [[ $name == B3 ]]; then
    for _ in $(seq 10); do
      (while :; do
        curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:3210/api/action \
          -H 'content-type: application/json' \
          -d '{"path":"actions:burnCpu","args":{"ms":30000},"format":"json"}' >>"$dir/burners.txt"
      done) &
      burners+=($!)
    done
  fi
  lgrun() { (cd "$repo" && "$lg" --existing-instance-url http://127.0.0.1:3210 \
    --existing-instance-admin-key "$key" --skip-build --once "$@" "$workload"); }
  lgrun --duration "$WARMUP" >"$dir/warmup.log" 2>&1 || true
  local since
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  curl -s http://127.0.0.1:3210/metrics >"$dir/conductor.before.prom"
  scrape_workers "$dir/workers.before.prom" "$workers"
  (while :; do
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' | sed "s/^/$(date +%s),/"
    sysctl -n vm.loadavg | awk -v t="$(date +%s)" '{print t ",hostload," $2}'
    pgrep -f scenario-runner.js | head -1 | xargs -I{} ps -o %cpu= -p {} | sed "s/^/$(date +%s),scenario-runner,/"
  done >"$dir/stats.csv" 2>/dev/null) &
  local sampler=$!
  local rc=0
  lgrun --duration "$DURATION" --stats-report >"$dir/loadgen.log" 2>&1 || rc=$?
  kill "$sampler" ${burners[@]+"${burners[@]}"} 2>/dev/null || true
  echo "load-generator exit $rc" >>"$dir/loadgen.log"
  curl -s http://127.0.0.1:3210/metrics >"$dir/conductor.after.prom" || true
  scrape_workers "$dir/workers.after.prom" "$workers" || true
  compose logs --since "$since" conductor >"$dir/conductor.log" 2>&1 || true
  [[ $workers == 0 ]] || compose logs --since "$since" worker >"$dir/worker.log" 2>&1 || true
  python3 "$bench/summarize.py" "$dir" "$DURATION" | tee "$dir/summary.txt"
}

for r in ${@:-B0 B1-1 B1-2 B1-3 B2 B3 B4}; do
  export FUNCTION_RUNNER=remote FUNRUN_ROUTING=direct FUNRUN_WORKERS=worker:7400 CONDUCTOR_CPUS=2 WORKER_CPUS=2
  case $r in
    B0) FUNCTION_RUNNER=local CONDUCTOR_CPUS=8 run B0 0 ;;
    B1-1 | B1-2 | B1-3) run "$r" "${r#B1-}" ;;
    B2 | B3) run "$r" 3 ;;
    B4) FUNRUN_ROUTING=proxy FUNRUN_WORKERS=envoy:7400 run B4 3 ;;
    *) echo "unknown run $r" >&2; exit 2 ;;
  esac
done
