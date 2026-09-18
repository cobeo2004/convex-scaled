import { ConvexHttpClient, ConvexClient } from "convex/browser";
import { api } from "../convex/_generated/api";
import { describe, expect, test } from "vitest";
import { randomBytes } from "node:crypto";

const URL = process.env.CONVEX_URL ?? "http://127.0.0.1:3210";
const SITE = process.env.CONVEX_SITE_URL ?? "http://127.0.0.1:3211";
const run = Date.now().toString(36);
const http = new ConvexHttpClient(URL);

describe(`funrun equivalence (${process.env.FUNCTION_RUNNER ?? "unknown"})`, () => {
  test("mutation then indexed query", async () => {
    await http.mutation(api.messages.send, { author: `a-${run}`, body: "hello" });
    const rows = await http.query(api.messages.byAuthor, { author: `a-${run}` });
    expect(rows.map((r) => r.body)).toEqual(["hello"]);
  });

  test("concurrent increments are serialized by OCC", async () => {
    const name = `c-${run}`;
    await Promise.all(Array.from({ length: 25 }, () => http.mutation(api.messages.increment, { name })));
    expect(await http.query(api.messages.getCounter, { name })).toBe(25);
  });

  test("user errors surface unchanged", async () => {
    await expect(http.mutation(api.messages.throws, {})).rejects.toThrow(/boom/);
  });

  test("action callbacks: runMutation, runQuery, scheduler, storage", async () => {
    const author = `act-${run}`;
    const res = await http.action(api.actions.roundTrip, { author });
    expect(res).toEqual({ count: 1, text: "hello funrun" });
    await expect.poll(async () => (await http.query(api.messages.byAuthor, { author })).length, { timeout: 10_000 }).toBe(2);
  });

  test("subscription updates after a write", async () => {
    const client = new ConvexClient(URL);
    const author = `sub-${run}`;
    const seen: number[] = [];
    const unsub = client.onUpdate(api.messages.byAuthor, { author }, (rows) => seen.push(rows.length));
    await expect.poll(() => seen.at(-1)).toBe(0);
    await http.mutation(api.messages.send, { author, body: "live" });
    await expect.poll(() => seen.at(-1), { timeout: 10_000 }).toBe(1);
    unsub();
    await client.close();
  });

  test("text search", async () => {
    await http.mutation(api.messages.send, { author: `s-${run}`, body: `needle${run}` });
    await expect.poll(async () => (await http.query(api.search.find, { term: `needle${run}` })).length, { timeout: 15_000 }).toBe(1);
  });

  test("http action request body and status", async () => {
    const res = await fetch(`${SITE}/echo`, { method: "POST", body: "ping" });
    expect(res.status).toBe(201);
    expect(await res.text()).toBe("ping");
  });

  test("http action streamed response", async () => {
    const res = await fetch(`${SITE}/stream`);
    expect(await res.text()).toBe("abc");
  });

  test("http action >1 MiB request body round-trips byte-for-byte", async () => {
    // base64 keeps the payload ASCII so /echo's req.text() is lossless;
    // ~1.5 MiB forces the body to reach the worker in multiple chunks.
    const payload = Buffer.from(randomBytes(1_200_000).toString("base64"));
    expect(payload.length).toBeGreaterThan(1024 * 1024);
    const res = await fetch(`${SITE}/echo`, { method: "POST", body: payload });
    expect(res.status).toBe(201);
    const echoed = Buffer.from(await res.arrayBuffer());
    expect(echoed.length).toBe(payload.length);
    expect(echoed.equals(payload)).toBe(true);
  });
});
