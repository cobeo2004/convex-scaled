import { action } from "./_generated/server";
import { api, internal } from "./_generated/api";
import { v } from "convex/values";

export const roundTrip = action({
  args: { author: v.string() },
  // Explicit return type breaks the api -> roundTrip -> api type cycle (TS7022).
  handler: async (ctx, { author }): Promise<{ count: number; text: string }> => {
    await ctx.runMutation(api.messages.send, { author, body: "from action" });
    const rows = await ctx.runQuery(api.messages.byAuthor, { author });
    await ctx.scheduler.runAfter(0, internal.messages.scheduledWrite, { author });
    const blob = new Blob(["hello funrun"]);
    const id = await ctx.storage.store(blob);
    const text = await (await ctx.storage.get(id))!.text();
    console.log("roundTrip done");
    return { count: rows.length, text };
  },
});

export const burnCpu = action({
  args: { ms: v.number() },
  handler: async (_ctx, { ms }) => {
    const end = Date.now() + ms;
    let x = 0;
    while (Date.now() < end) x += Math.sqrt(x + 1);
    return x > 0;
  },
});
