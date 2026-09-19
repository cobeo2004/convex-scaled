# funrun B0–B4 benchmark results (2026-09-18)

**These are rough numbers from a single laptop. Do not treat them as capacity figures.**
Everything ran on one laptop: a 10-CPU Apple Silicon MacBook with 8 GB of RAM for Docker (OrbStack/Docker Desktop VM). The
following all ran on that host at the same time:

- the stack under test: conductor, workers, Postgres 17, RustFS, and Envoy (B4 only);
- the load generator: `target/release/load-generator` plus its Node
  `scenario-runner` child process, which used 10–35% of one CPU (see `scenario-runner` in the table below);
- **work from another session that I did not control:** a local iOS build (Xcode/clang) and later
  `eas build --platform android --local` plus an Android emulator. These builds pushed
  the host load average as high as 68 while some runs were in progress (see "Host noise").

## Layout (ruling R21)

| Run | FUNCTION_RUNNER | Conductor CPUs | Workers | Notes |
|---|---|---|---|---|
| B0 | local | 8 | 0 | |
| B1-N | remote | 2 | N × 2 CPUs (N = 1, 2, 3) | |
| B2 | remote | 2 | 3 × 2 | same 8-CPU budget as B0 |
| B3 | remote | 2 | 3 × 2 | + 10 concurrent `actions:burnCpu {ms: 30000}` (a curl loop through `/api/action`) |
| B4 | remote, `FUNRUN_ROUTING=proxy` | 2 | 3 × 2 | through Envoy (`--profile proxy`) |

CPU limits come from the existing `CONDUCTOR_CPUS` / `WORKER_CPUS` compose variables. Memory is not capped.
Peak memory stayed below 1.2 GB for the conductor and below 0.9 GB per role otherwise, and nothing ran out of memory.

## Workload

`workload.json` drives `crates/load_generator` in **benchmark (closed-loop) mode**. There are 40 client threads,
each sending one request at a time:

| Scenario | Threads | Function |
|---|---|---|
| query | 10 | `bench:byAuthor`: `messages.by_author` for one of 10 seeded reader authors (50 rows each, never written to, so the read size stays constant) |
| query | 10 | `messages:cpuQuery`: about 20 ms of math (calibrated at about 22 ms per call on B0) |
| mutation | 8 | `bench:send`: inserts into `messages` for a random writer author (1 of 100) |
| mutation | 8 | `bench:increment`: `messages:increment` on 1 of 100 counters |
| http | 2 | `POST /echo` |
| action | 2 | `bench:burnCpu50`: `actions:burnCpu {ms: 50}` |

By thread count the mix is 50% queries, 40% mutations, 5% HTTP and 5% actions. The number of requests of each type differs because faster functions complete more calls.

**Deviation from the brief (open-loop rate):** `load_generator`'s `rate` mode is not
open-loop. Each scenario loop waits for its own request to finish and opens a new WebSocket client per
request, so it cannot hold a fixed offered rate. I used benchmark mode instead. To choose the
offered load, I saturated B0 first, then fixed it: probes at 20/40/80/160 threads gave
B0 872/791/661/583 rps with the conductor at 705–841% CPU (8 CPUs). Throughput peaks at 20 threads and then falls as queueing grows. I kept 40 threads, which saturates the
conductor at 770–810%. It was not changed after the results came in.
`load_generator` also needs zero-argument functions plus `setup:setupMessages`,
`setup:setupVectors` and `http:siteUrl`. These live in `e2e/convex/{bench,setup,http}.ts`.

Timing: **30 s warmup + 120 s measured** per run, each on a fresh stack (`down -v`, deploy).
The query cache is bypassed by the random `cacheBreaker` argument.

## Measurement

- Throughput, p50/p99 and client errors come from `load-generator --stats-report` over the 120 s window.
- Conductor `/metrics` is scraped before and after the measured window. `database_commit_queue_seconds`,
  `database_commit_seconds` and `database_commit_persistence_write_seconds` are
  VictoriaMetrics histograms. The mean is exact; the p99 is a bucket upper bound (`p99≤`).
- IndexPage RPCs: `funrun_worker_funrun_index_page_rpcs_total` is counted on the **workers**, which now
  serve `/metrics` on `FUNRUN_METRICS_LISTEN`. The before/after delta is summed over all workers and divided by
  client calls (all types). No Overloaded counter exists, so I counted `Overloaded` and funrun
  `retrying …: funrun worker` lines in the conductor log.
- CPU is the mean of `docker stats` samples. Host load is the mean 1-minute load average across 10 CPUs, and it **includes
  the stack itself** (the conductor alone keeps about 8 CPUs busy in B0).

## Results (the table and every pass/fail verdict use the final set of runs)

| Run | rps | errors (rate) | host load | conductor CPU% | worker CPU% (sum) | IndexPage RPC/call | Overloaded / funrun retries |
|---|---|---|---|---|---|---|---|
| B0 | **589.2** | 0 (0) | 19.2 | 809 | – | – | 0 / 0 |
| B1-1 | **174.0** | 4 (0.019%) | 12.0 (Android build running) | 44 | 204 | 0.44 | 0 / 0 |
| B1-2 | **425.1** | 0 (0) | 18.0 | 80 | 398 | 0.49 | 0 / 0 |
| B1-3 | **518.8** | 0 (0) | 8.6 | 89 | 472 | 0.49 | 0 / 0 |
| B2 | **531.6** | 0 (0) | 15.1 | 96 | 444 | 0.51 | 0 / 0 |
| B3 | **353.3** | 0 (0) | 10.3 | 63 | 598 | 0.48 | 0 / 0 |
| B4 | **556.0** | 0 (0) | 8.9 | 84 (Envoy 14) | 437 | 0.42 | 0 / 0 |

p50 / p99 per function, in ms:

| Run | byAuthor | cpuQuery | send | increment | echo | burnCpu50 |
|---|---|---|---|---|---|---|
| B0 | 36 / 127 | 86 / 217 | 84 / 257 | 95 / 325 | 22 / 87 | 65 / 135 |
| B1-1 | 203 / 840 | 381 / 1090 | 106 / 589 | 199 / 820 | 130 / 576 | 193 / 606 |
| B1-2 | 88 / 241 | 163 / 390 | 84 / 217 | 97 / 321 | 64 / 177 | 101 / 216 |
| B1-3 | 69 / 158 | 127 / 291 | 52 / 160 | 71 / 273 | 35 / 115 | 98 / 174 |
| B2 | 53 / 157 | 189 / 317 | 44 / 154 | 62 / 258 | 25 / 109 | 100 / 171 |
| B3 | 98 / 289 | 187 / 505 | 95 / 227 | 101 / 391 | 74 / 212 | 101 / 251 |
| B4 | 76 / 121 | 167 / 272 | 80 / 125 | 96 / 221 | 8 / 43 | 100 / 148 |

Commit timers (mean ms / p99≤ ms):

| Run | commit_queue | commit | commit_persistence_write |
|---|---|---|---|
| B0 | 17.9 / 88.0 | 44.5 / 166.8 | 27.3 / 129.2 |
| B1-1 | 2.5 / 16.7 | 20.4 / 189.6 | 18.3 / 189.6 |
| B1-2 | 1.8 / 11.4 | 14.9 / 88.0 | 10.8 / 68.1 |
| B1-3 | 2.3 / 19.0 | 16.3 / 68.1 | 11.8 / 52.8 |
| B2 | 2.3 / 12.9 | 17.0 / 60.0 | 12.1 / 46.4 |
| B3 | 2.1 / 24.5 | 17.2 / 60.0 | 13.6 / 52.8 |
| B4 | 3.1 / 16.7 | 17.8 / 46.4 | 12.7 / 35.9 |

In B0 the commit queue wait is about 8× higher than in any remote run. In local mode the conductor's 8 CPUs are
saturated by V8, which starves the committer. In remote mode the conductor stays at 45–96% of its 2 CPUs.

## Targets

| Target | Measured | Verdict |
|---|---|---|
| (b) B1 1→2 ≥ 1.7× | 425.1 / 174.0 = **2.44×** in the final set; 336.8 / 220.2 = **1.53×** in the earlier set | **Inconclusive.** The final set passes only because B1-1 ran during an Android build. The earlier set misses (see below). |
| (b) B1 1→3 ≥ 2.4× | 518.8 / 174.0 = **2.98×** in the final set; 451.3 / 220.2 = **2.05×** in the earlier set | **Inconclusive**, for the same reason |
| (c) B2 ≥ 0.85 × B0 | 531.6 / 589.2 = **0.90×** | **PASS** |
| (d) B3 query p99 ≤ 2 × B1-3 p99 | byAuthor 289 / 158 = **1.82×**; cpuQuery 505 / 291 = **1.73×** | **PASS** |
| B4 (record only) | 556.0 rps, 0 errors, 1.05× B2 | recorded |

On (b), I trust neither set:
- **Earlier set.** B1-1/2/3 ran back to back overnight, before host load was recorded (`out/prev/`, 220.2 / 336.8 / 451.3 rps,
  0 errors). The 1→2 ratio is 1.53× and 1→3 is 2.05×, both **misses**.
- **Final set.** B1-1 ran just as a local Android build started. That run had a host load of 12 and was the only one with errors, which
  inflates both ratios.
- **The closed-loop client caps the ceiling.** 40 threads at a mean latency of about 75 ms caps
  throughput at roughly 520–550 rps, so B1-3, B2 and B4 sit near the workload's concurrency limit.
  The workers are not saturated: 472/600% in B1-3 and 444/600% in B2. So 1→3 scaling is limited by the load generator
  as well as by the stack.

Overall: throughput clearly grows with the number of workers (both sets increase monotonically). Nothing measured here
shows the 1.7× / 2.4× ratios holding up. Checking them properly needs a quiet host and more client threads than
B0 needs, and the load generator should run on a separate machine.

IndexPage RPCs per call are 0.42–0.51, well under one round trip per call. That does **not** trigger
the M1.5 index-read batching, and (c) passed anyway.

## Errors and stress

- All valid runs: **0 client errors and 0 Overloaded**, and no funrun retries were logged.
- B1-1: 4 `messages:cpuQuery` "Server Error" responses (0.019%). The run was CPU-starved by the Android build (cpuQuery p99 was
  1090 ms). The conductor logged nothing for these request IDs. My best guess is the 1 s
  `DATABASE_UDF_USER_TIMEOUT_SECONDS` query timeout, but I did not verify it.
- B3: all 50 of the 30-second burns returned HTTP 200 while the workers ran at 598% of 600% CPU.
- Every run logged OCC conflicts on `counters` from `bench:increment`, which picks 1 of 100 counters with 8 threads:
  B0 472, B1-1 247, B1-2 551, B1-3 774, B2 785, B3 467, B4 517. None reached the client as an
  error, so the load generator saw 0 mutation errors. The conductor also logged `WebSocket … Client disconnected` warnings when
  load-generator shut down (47 in B1-1).

## Discarded runs (kept locally in `out/`, not committed)

- `B0-slept-invalid`: the host slept for 47 minutes during the measured window. That run gave a p99 of 1,973 s and 7 cpuQuery errors.
- `B1-3-conductor-exit`: the host slept for about 1.5 hours during warmup. Eleven seconds after the host woke, the
  conductor exited with **code 0**. `docker events` shows no `oom` event and no `kill`, and the conductor's logs were lost when the stack
  was torn down. Exiting with code 0 without being killed matches `local_backend`'s "Received a fatal error. Shutting
  down immediately" (preempt) path. That path is upstream code, which this branch does not touch.
- `B0-host-busy-invalid`: ran during the iOS build, with host load up to 68. That run gave 329 rps with the conductor at 891% CPU.

## Host noise

The earlier B1 set and the final set differ by as much as 26% on the same layout (B1-2: 336.8 vs 425.1).
To reduce this, `run.sh` now waits up to 30 minutes for the 1-minute load average to drop below 3 before each run, keeps
the host awake with `caffeinate`, and `summarize.py` reports `host_load_avg` and `max_sample_gap_s`, where a large gap means the host slept.

## Reproduce

```sh
cd self-hosted/funrun && docker compose build conductor worker
bench/run.sh            # all runs; or e.g. bench/run.sh B0 B2
# WARMUP=30 DURATION=120 SCALE=1 by default; results in bench/out/<run>/summary.txt
```
