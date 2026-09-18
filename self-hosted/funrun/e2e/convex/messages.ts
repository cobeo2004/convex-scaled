import { mutation, query, internalMutation } from "./_generated/server";
import { v } from "convex/values";
import { paginationOptsValidator, PaginationResult } from "convex/server";
import { Doc } from "./_generated/dataModel";

export const send = mutation({
  args: { author: v.string(), body: v.string() },
  handler: async (ctx, args) => ctx.db.insert("messages", args),
});

export const byAuthor = query({
  args: { author: v.string() },
  handler: async (ctx, { author }) =>
    ctx.db.query("messages").withIndex("by_author", (q) => q.eq("author", author)).collect(),
});

export const byAuthorPage = query({
  args: { author: v.string(), paginationOpts: paginationOptsValidator },
  // Explicit return type breaks the api -> byAuthorPage -> api type cycle (TS7022).
  handler: async (ctx, { author, paginationOpts }): Promise<PaginationResult<Doc<"messages">>> =>
    ctx.db
      .query("messages")
      .withIndex("by_author", (q) => q.eq("author", author))
      .paginate(paginationOpts),
});

export const increment = mutation({
  args: { name: v.string() },
  handler: async (ctx, { name }) => {
    const row = await ctx.db.query("counters").withIndex("by_name", (q) => q.eq("name", name)).unique();
    if (row) {
      await ctx.db.patch(row._id, { value: row.value + 1 });
      return row.value + 1;
    }
    await ctx.db.insert("counters", { name, value: 1 });
    return 1;
  },
});

export const getCounter = query({
  args: { name: v.string() },
  handler: async (ctx, { name }) =>
    (await ctx.db.query("counters").withIndex("by_name", (q) => q.eq("name", name)).unique())?.value ?? 0,
});

export const throws = mutation({ args: {}, handler: async () => { throw new Error("boom"); } });

export const scheduledWrite = internalMutation({
  args: { author: v.string() },
  handler: async (ctx, { author }) => { await ctx.db.insert("messages", { author, body: "scheduled" }); },
});
