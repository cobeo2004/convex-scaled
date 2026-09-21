import { exec } from "node:child_process";
import { promisify } from "node:util";
import { ConvexHttpClient } from "convex/browser";
import { beforeEach, expect, test } from "vitest";
import { api } from "../convex/_generated/api";

// Load balancing across both pools, against the stack balance.sh brings up:
// 2 isolate workers and 2 node workers.
//
// Attribution comes from each worker's own
// funrun_worker_grpc_server_started_total{method="Execute"} -- one Execute
// stream per request the conductor sent to that worker. The conductor exports
// no per-worker request counter, and the "Created <pool> isolate worker" log
// line that run.sh greps is no use here: it fires once per V8 thread at
// warm-up and never again, so it says a worker ran something, not what.

const URL = process.env.CONVEX_URL ?? "http://127.0.0.1:3210";
const http = new ConvexHttpClient(URL);
const sh = promisify(exec);

// The conductor spills a module off its home worker at this many in-flight
// requests (FUNRUN_CLIENT_MAX_REQUESTS_PER_UPSTREAM).
const SPILL_AT = 15;

// Every probe call needs args no earlier call used, or the backend's query
// cache answers it without dispatching to a worker and the test measures
// nothing. Seeded from the clock so a re-run against a live backend does not
// collide with the previous run's cache entries.
const RUN = Date.now();
let seq = 0;
const nonce = () => RUN + seq++;

const MODULES = [
  api.balance.m0.probe,
  api.balance.m1.probe,
  api.balance.m2.probe,
  api.balance.m3.probe,
  api.balance.m4.probe,
  api.balance.m5.probe,
  api.balance.m6.probe,
  api.balance.m7.probe,
  api.balance.m8.probe,
  api.balance.m9.probe,
];

// balance.sh runs these against the stack it just brought up. Override to
// point at an already-running one, e.g.
// COMPOSE_CMD="docker compose -p convex-funrun -f ../docker-compose.yml".
const COMPOSE =
  process.env.COMPOSE_CMD ?? "docker compose -f ../docker-compose.yml";

async function executeCount(service: string, index: number): Promise<number> {
  const { stdout } = await sh(
    `${COMPOSE} exec -T --index ${index} ${service} curl -s localhost:9100/metrics`,
  );
  const line = stdout
    .split("\n")
    .find((l) =>
      l.startsWith('funrun_worker_grpc_server_started_total{method="Execute"}'),
    );
  // A worker that has served nothing yet has not emitted the series at all.
  return line ? Number(line.split(" ").pop()) : 0;
}

const counts = (service: string, replicas: number): Promise<number[]> =>
  Promise.all(
    Array.from({ length: replicas }, (_, i) => executeCount(service, i + 1)),
  );

// Requests each worker served while `body` ran.
async function served<T>(
  service: string,
  replicas: number,
  body: () => Promise<T>,
): Promise<{ result: T; per: number[] }> {
  const before = await counts(service, replicas);
  const result = await body();
  const after = await counts(service, replicas);
  return { result, per: after.map((n, i) => n - before[i]) };
}

beforeEach(async () => {
  // Warm every worker's module cache and let any in-flight work from a
  // previous test drain, so a delta measures only what its own test sent.
  await Promise.all(MODULES.map((m) => http.query(m, { nonce: nonce() })));
  await new Promise((r) => setTimeout(r, 500));
});

test("a module keeps to one isolate worker", { timeout: 120_000 }, async () => {
  const CALLS = 12;
  const { per } = await served("worker", 2, async () => {
    // Sequential: in-flight never reaches SPILL_AT, so nothing spills and the
    // only thing under test is the (module, addr) hash.
    for (let i = 0; i < CALLS; i++)
      await http.query(MODULES[0], { nonce: nonce() });
  });

  const busiest = Math.max(...per);
  expect(busiest).toBeGreaterThanOrEqual(CALLS);
  // Everything went to one worker; nothing leaked to the other.
  expect(per.reduce((a, b) => a + b, 0) - busiest).toBe(0);
});

test(
  "distinct modules reach both isolate workers",
  { timeout: 120_000 },
  async () => {
    const { per } = await served("worker", 2, async () => {
      for (const m of MODULES) {
        for (let i = 0; i < 3; i++) await http.query(m, { nonce: nonce() });
      }
    });

    // Each module picks its home independently, so this is 10 coin flips: the
    // chance all ten land on one worker, failing this, is 2 * 2^-10 = 0.2%.
    expect(per.filter((n) => n > 0)).toHaveLength(2);
    expect(per.reduce((a, b) => a + b, 0)).toBe(MODULES.length * 3);
  },
);

test(
  "a saturated isolate worker spills to its neighbour",
  { timeout: 120_000 },
  async () => {
    // One module, so without spill every one of these would go to one worker.
    // Far more than SPILL_AT at once, each holding its slot long enough for the
    // conductor to see them overlap.
    const CALLS = SPILL_AT * 2 + 10;
    const { result, per } = await served("worker", 2, () =>
      Promise.allSettled(
        Array.from({ length: CALLS }, () =>
          http.query(MODULES[0], { nonce: nonce(), reads: 30 }),
        ),
      ),
    );

    const failed = result.flatMap((r) =>
      r.status === "rejected" ? [String(r.reason)] : [],
    );
    expect(failed).toEqual([]);
    // Isolate workers accept MAX_ISOLATE_WORKERS (300) at once, so nothing here
    // is refused: the split is pick() spilling, not a worker pushing back.
    expect(per.filter((n) => n > 0)).toHaveLength(2);
    expect(per.reduce((a, b) => a + b, 0)).toBe(CALLS);
  },
);

test(
  "node actions spread across both node workers",
  { timeout: 180_000 },
  async () => {
    // 2 workers x FUNRUN_NODE_MAX_CONCURRENT (4) = 8 slots. 16 actions holding
    // 2s each is 4s of pool time, well inside the 30s refused-wait budget for
    // NodeExecute, so none of these may fail.
    const CALLS = 16;
    const results = await Promise.allSettled(
      Array.from({ length: CALLS }, () =>
        http.action(api.nodeActions.nodeSleep, { ms: 2000 }),
      ),
    );

    const failed = results.flatMap((r) =>
      r.status === "rejected" ? [String(r.reason)] : [],
    );
    expect(failed).toEqual([]);

    const hosts = new Map<string, number>();
    for (const r of results) {
      if (r.status === "fulfilled") {
        hosts.set(r.value.host, (hosts.get(r.value.host) ?? 0) + 1);
      }
    }
    expect(hosts.size).toBe(2);
    // Both workers did real work. A lopsided split still produces two hosts, so
    // assert a share: with 8 slots filled from one queue, neither worker can
    // legitimately end up under a quarter of the load.
    for (const [host, n] of hosts) {
      expect(n, `${host} served ${n} of ${CALLS}`).toBeGreaterThanOrEqual(
        CALLS / 4,
      );
    }
  },
);
