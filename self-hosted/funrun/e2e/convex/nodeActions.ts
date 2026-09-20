"use node";
import { createHash } from "node:crypto";
import { hostname } from "node:os";
import { action } from "./_generated/server";
import { api } from "./_generated/api";
import { v } from "convex/values";

// Imports Node built-ins, so it can only run in the Node executor. Returns
// the host it ran on so the e2e can tell a node-worker from the conductor.
export const nodeRoundTrip = action({
  args: { author: v.string() },
  handler: async (
    ctx,
    { author },
  ): Promise<{ count: number; sha256: string; host: string }> => {
    await ctx.runMutation(api.messages.send, { author, body: "from node" });
    const rows = await ctx.runQuery(api.messages.byAuthor, { author });
    // A typeless Blob makes the node executor send an empty content-type, which
    // the upload endpoint rejects. Same upstream; not a funrun behaviour.
    const id = await ctx.storage.store(
      new Blob(["hello node"], { type: "text/plain" }),
    );
    const text = await (await ctx.storage.get(id))!.text();
    const sha256 = createHash("sha256").update(text).digest("hex");
    console.log("nodeRoundTrip done");
    return { count: rows.length, sha256, host: hostname() };
  },
});

// Holds a node worker slot for `ms`, so the stress test can saturate the pool
// (FUNRUN_NODE_MAX_CONCURRENT) and check that refused requests wait instead of
// failing. setTimeout, not a busy loop: this tests admission, not CPU.
export const nodeSleep = action({
  args: { ms: v.number() },
  handler: async (_ctx, { ms }): Promise<{ host: string }> => {
    await new Promise((r) => setTimeout(r, ms));
    return { host: hostname() };
  },
});
