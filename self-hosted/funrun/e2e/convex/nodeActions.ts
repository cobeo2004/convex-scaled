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
    const id = await ctx.storage.store(new Blob(["hello node"]));
    const text = await (await ctx.storage.get(id))!.text();
    const sha256 = createHash("sha256").update(text).digest("hex");
    console.log("nodeRoundTrip done");
    return { count: rows.length, sha256, host: hostname() };
  },
});
