import { createHash } from "node:crypto";
import { ConvexHttpClient } from "convex/browser";
import { expect, test } from "vitest";
import { api } from "../convex/_generated/api";

const URL = process.env.CONVEX_URL ?? "http://127.0.0.1:3210";
const http = new ConvexHttpClient(URL);

test("use node action round-trips through callbacks and storage", async () => {
  const author = `node-${Date.now()}`;
  const r = await http.action(api.nodeActions.nodeRoundTrip, { author });
  expect(r.count).toBe(1);
  expect(r.sha256).toBe(
    createHash("sha256").update("hello node").digest("hex"),
  );
  if (process.env.EXPECT_NODE_HOST_PREFIX) {
    expect(r.host.startsWith(process.env.EXPECT_NODE_HOST_PREFIX)).toBe(true);
  }
});
