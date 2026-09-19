import { ConvexHttpClient } from "convex/browser";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { setTimeout as sleep } from "node:timers/promises";
import { api } from "../convex/_generated/api";
import { describe, expect, test } from "vitest";

// Failure injection against the ../ compose stack (started by failures.sh).
// Async exec so load keeps flowing while docker commands run.
const http = new ConvexHttpClient(process.env.CONVEX_URL ?? "http://127.0.0.1:3210");
const sh = async (cmd: string) => (await promisify(exec)(cmd)).stdout.trim();
const compose = (args: string) => sh(`docker compose -f ../docker-compose.yml ${args}`);
const workers = async () => (await compose("ps -q worker")).split("\n").filter(Boolean);
const run = Date.now().toString(36);
const counter = (name: string) => http.query(api.messages.getCounter, { name });
const message = (r: PromiseSettledResult<unknown>) =>
  r.status === "rejected" ? String((r.reason as Error)?.message ?? r.reason) : "";

// Brings both replicas back and waits until the conductor routes to them:
// a query succeeds, then one DNS refresh (5 s) + WatchLoad reconnect (1 s).
async function restoreWorkers() {
  await compose("up -d --no-recreate --scale worker=2 worker");
  await expect.poll(() => counter("ready").then(() => true, () => false), { timeout: 60_000 }).toBe(true);
  await sleep(7_000);
}

describe.runIf(process.env.FUNCTION_RUNNER === "remote")("funrun failures", () => {
  test("mutations survive every worker being killed mid-load (committed at most once)", async () => {
    const name = `kill-${run}`;
    const writes = Array.from({ length: 200 }, () => http.mutation(api.messages.increment, { name }));
    // Kills every replica mid-load: in-flight mutations retry and then fail
    // (lost call / NoFunrunWorker) until the pool is back. None commits twice.
    const kill = sleep(200).then(() => compose("kill worker"));
    const results = await Promise.allSettled(writes);
    await kill;
    await restoreWorkers();
    const ok = results.filter((r) => r.status === "fulfilled").length;
    const errors = results.filter((r) => r.status === "rejected").map(message);
    console.log(`kill mid-load: ${ok} committed, ${errors.length} failed`, [...new Set(errors)]);
    for (const e of errors) expect(e).toMatch(/overloaded|NoFunrunWorker|funrun worker/i);
    expect(await counter(name)).toBe(ok);
    await sleep(3_000);
    expect(await counter(name)).toBe(ok);
  }, 120_000);

  test("an action on a killed worker fails and is not re-run", async () => {
    const marker = `marker-${run}`;
    const p = http.action(api.actions.markedBurn, { marker, ms: 15_000 });
    const settled = p.then(() => "resolved", (e: Error) => e.message);
    // The action has committed its marker and is now burning CPU.
    await expect.poll(() => counter(marker), { timeout: 10_000 }).toBe(1);
    // Kill only the replica running it (the busy one), so a wrong retry would
    // land on the healthy one and bump the marker to 2.
    const stats = await sh(`docker stats --no-stream --format '{{.ID}} {{.CPUPerc}}' ${(await workers()).join(" ")}`);
    const busy = stats
      .split("\n")
      .map((l) => l.split(" "))
      .sort((a, b) => parseFloat(b[1]) - parseFloat(a[1]))[0][0];
    await sh(`docker kill ${busy}`);
    const outcome = await settled;
    console.log(`killed ${busy}; action ->`, outcome);
    expect(outcome).not.toBe("resolved");
    // A retry would re-run within milliseconds; give it well over that.
    for (let i = 0; i < 5; i++) {
      expect(await counter(marker)).toBe(1);
      await sleep(1_000);
    }
    await restoreWorkers();
    expect(await counter(marker)).toBe(1);
  }, 120_000);

  test("SIGTERM drains an in-flight action before the worker exits", async () => {
    const marker = `drain-${run}`;
    const p = http.action(api.actions.markedBurn, { marker, ms: 4_000 });
    await expect.poll(() => counter(marker), { timeout: 10_000 }).toBe(1);
    await compose("stop -t 30 worker");
    await expect(p).resolves.toBe(true);
    await restoreWorkers();
    expect(await counter(marker)).toBe(1);
  }, 120_000);

  test("graceful rolling restart under load: zero failed calls, actions drain", async () => {
    const name = `roll-${run}`;
    const actions: { marker: string; done: Promise<boolean> }[] = [];
    let stop = false;
    let writes = 0;
    const failures: string[] = [];
    const lane = async () => {
      while (!stop) {
        try {
          await http.mutation(api.messages.increment, { name });
          writes++;
          await http.query(api.messages.getCounter, { name });
        } catch (e) {
          failures.push((e as Error).message);
        }
      }
    };
    const lanes = Array.from({ length: 8 }, lane);
    await sleep(1_000);
    // One replica at a time, so at least one worker is always up.
    for (const id of await workers()) {
      // A long action in flight while a replica drains: whichever worker
      // runs it, it must finish (drain flushes its result) and run once.
      const marker = `roll-action-${run}-${actions.length}`;
      actions.push({ marker, done: http.action(api.actions.markedBurn, { marker, ms: 3_000 }) });
      await expect.poll(() => counter(marker), { timeout: 10_000 }).toBe(1);
      await sh(`docker stop -t 30 ${id}`);
      await sleep(1_000);
      await sh(`docker start ${id}`);
      await sleep(8_000); // rediscovered (DNS refresh 5 s) + WatchLoad healthy
    }
    stop = true;
    await Promise.all(lanes);
    console.log(`rolling restart: ${writes} writes, ${failures.length} failures`, [...new Set(failures)]);
    expect(failures).toEqual([]);
    expect(writes).toBeGreaterThan(0);
    expect(await counter(name)).toBe(writes);
    for (const { marker, done } of actions) {
      await expect(done).resolves.toBe(true);
      expect(await counter(marker)).toBe(1);
    }
  }, 120_000);

  test("no workers -> Overloaded", async () => {
    await compose("stop worker");
    await expect(counter("x")).rejects.toThrow(/overloaded|NoFunrunWorker/i);
    await restoreWorkers();
  }, 120_000);
});
