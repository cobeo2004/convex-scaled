"""Summarizes one run.sh run dir: load-generator report, Prometheus deltas,
docker stats and conductor log counts. Usage: summarize.py <dir> <seconds>"""
import collections, json, re, sys
from pathlib import Path

d, secs = Path(sys.argv[1]), float(sys.argv[2])


def dur_ms(s):
    m = re.match(r"([\d.]+)(ns|µs|us|ms|s)$", s.strip())
    return float(m[1]) * {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1, "s": 1e3}[m[2]]


# load-generator --stats-report
lg = (d / "loadgen.log").read_text()
fns, cur = {}, None
for line in lg.splitlines():
    if m := re.match(r"Stats for (\S+) in scenario \S+ with path Some\(\"(.+)\"\):", line):
        cur = fns.setdefault(m[2], {"type": m[1], "errors": 0})
    elif cur is not None and (m := re.match(r"(Total|p50|p99)\s*: (.+)", line)):
        cur[m[1]] = int(m[2]) if m[1] == "Total" else dur_ms(m[2])
    elif m := re.match(r"Errors for (\S+) in scenario \S+ with path (?:Some\(\"(.+)\"\)|None): (\d+)", line):
        fns.setdefault(m[2] or m[1], {"type": m[1], "errors": 0, "Total": 0})["errors"] += int(m[3])
calls = sum(f.get("Total", 0) for f in fns.values())
errors = sum(f["errors"] for f in fns.values())


def prom(path):
    """{(name, labels-without-le): value} plus histogram buckets."""
    vals, buckets = collections.Counter(), collections.defaultdict(collections.Counter)
    if not path.exists():
        return vals, buckets
    for line in path.read_text().splitlines():
        m = re.match(r"([a-zA-Z_:][\w:]*)(\{[^}]*\})? ([^ ]+)", line)
        if not m or line.startswith("#"):
            continue
        name, labels, v = m[1], m[2] or "", float(m[3])
        # VictoriaMetrics histograms: non-cumulative buckets keyed by vmrange="lo...hi".
        if name.endswith("_bucket") and (hi := re.search(r'vmrange="[^.]+(?:\.\d+)?(?:e[-+]?\d+)?\.\.\.([^"]+)"', labels)):
            buckets[name[:-7]][float(hi[1])] += v
        else:
            vals[name] += v
    return vals, buckets


def delta(kind):
    (va, ba), (vb, bb) = prom(d / f"{kind}.after.prom"), prom(d / f"{kind}.before.prom")
    return {k: va[k] - vb[k] for k in va}, {k: {le: ba[k][le] - bb[k][le] for le in ba[k]} for k in ba}


def hist(vals, buckets, name):
    n = vals.get(f"{name}_count", 0)
    if not n:
        return None
    b, acc = [], 0
    for hi, c in sorted(buckets.get(name, {}).items()):
        acc += c
        b.append((hi, acc))
    q = lambda p: round(next((hi for hi, c in b if c >= p * n), float("inf")) * 1e3, 2)
    return {"n": int(n), "mean_ms": round(vals[f"{name}_sum"] / n * 1e3, 2), "p50_le_ms": q(0.5), "p99_le_ms": q(0.99)}


cv, cb = delta("conductor")
wv, _ = delta("workers")
timers = {t: hist(cv, cb, t) for t in [n[:-6] for n in cv if n.endswith("_count")]
          if re.search(r"database_commit(_queue|_persistence_write)?_seconds$", t)}
rpcs = sum(v for k, v in wv.items() if "funrun_index_page_rpcs_total" in k)
rejected = {k: v for k, v in {**cv, **wv}.items() if "rejected" in k and v}

cpu, mem = collections.defaultdict(list), collections.defaultdict(float)
for line in (d / "stats.csv").read_text().splitlines():
    p = line.split(",")
    if len(p) < 3:
        continue
    role = re.sub(r"^convex-funrun-|-\d+$", "", p[1])
    cpu[(role, p[1])].append(float(p[2].rstrip("%")))
    if len(p) > 3:
        m = re.match(r"([\d.]+)(\w+)", p[3])
        mem[role] = max(mem[role], float(m[1]) * {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024}.get(m[2], 0))
ts = sorted({int(line.split(",")[0]) for line in (d / "stats.csv").read_text().splitlines() if line[:1].isdigit()})
max_gap = max((b - a for a, b in zip(ts, ts[1:])), default=None)
avg_cpu = collections.defaultdict(float)
for (role, _), xs in cpu.items():
    avg_cpu[role] += sum(xs) / len(xs)

log = (d / "conductor.log").read_text()
burn = (d / "burners.txt").read_text().split() if (d / "burners.txt").exists() else []
print(json.dumps({
    "throughput_rps": round(calls / secs, 1),
    # docker stats samples every few seconds; a gap much larger means the host slept.
    "max_sample_gap_s": max_gap,
    "calls": calls, "errors": errors,
    "error_rate": round(errors / (calls + errors), 5) if calls + errors else None,
    "per_function": {k: {"rps": round(f.get("Total", 0) / secs, 1), "p50_ms": f.get("p50"),
                         "p99_ms": f.get("p99"), "errors": f["errors"]} for k, f in sorted(fns.items())},
    # hostload = mean host 1-minute load average during the run (10 CPUs).
    "avg_cpu_pct": {k: round(v) for k, v in avg_cpu.items() if k != "hostload"},
    "host_load_avg": round(avg_cpu["hostload"], 1) if "hostload" in avg_cpu else None,
    "max_mem_mib": {k: round(v) for k, v in mem.items()},
    "commit_timers": timers,
    "index_page_rpcs": int(rpcs), "index_page_rpcs_per_call": round(rpcs / calls, 3) if calls else None,
    "rejected_counters": rejected,
    "log_overloaded": log.count("Overloaded"),
    "log_funrun_retries": len(re.findall(r"retrying \w+ in .*: funrun worker", log)),
    "log_occ_failures": log.count("Optimistic concurrency control failed"),
    "burners_http_codes": dict(collections.Counter(burn)),
}, indent=1))
