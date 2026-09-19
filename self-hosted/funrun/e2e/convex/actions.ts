import { action } from "./_generated/server";
import { api, internal } from "./_generated/api";
import { v } from "convex/values";

export const roundTrip = action({
  args: { author: v.string() },
  // Explicit return type breaks the api -> roundTrip -> api type cycle (TS7022).
  handler: async (
    ctx,
    { author },
  ): Promise<{ count: number; text: string }> => {
    await ctx.runMutation(api.messages.send, { author, body: "from action" });
    const rows = await ctx.runQuery(api.messages.byAuthor, { author });
    await ctx.scheduler.runAfter(0, internal.messages.scheduledWrite, {
      author,
    });
    const blob = new Blob(["hello funrun"]);
    const id = await ctx.storage.store(blob);
    const text = await (await ctx.storage.get(id))!.text();
    console.log("roundTrip done");
    return { count: rows.length, text };
  },
});

// Stores `text` as a blob and returns a fetchable URL for it (R? file
// storage over HTTP compat check).
export const storeText = action({
  args: { text: v.string() },
  handler: async (ctx, { text }): Promise<string> => {
    const id = await ctx.storage.store(new Blob([text]));
    const url = await ctx.storage.getUrl(id);
    if (url === null) throw new Error("storage url missing");
    return url;
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

// Bumps counter `marker` once (via a committed mutation), then burns CPU, so a
// test can tell whether the action ran more than once.
export const markedBurn = action({
  args: { marker: v.string(), ms: v.number() },
  handler: async (ctx, { marker, ms }): Promise<boolean> => {
    await ctx.runMutation(api.messages.increment, { name: marker });
    const end = Date.now() + ms;
    let x = 0;
    while (Date.now() < end) x += Math.sqrt(x + 1);
    return x > 0;
  },
});
