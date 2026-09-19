// load_generator calls setup:setupMessages and setup:setupVectors before every
// run (see ../../bench). Seeds the bench:byAuthor readers once.
import { mutation } from "./_generated/server";
import { v } from "convex/values";
import { READERS } from "./bench";

export const setupMessages = mutation({
  args: { rows: v.number(), channel: v.string() },
  handler: async (ctx, { rows }) => {
    if (await ctx.db.query("messages").withIndex("by_author", (q) => q.eq("author", "reader0")).first()) return;
    for (let i = 0; i < rows; i++) {
      await ctx.db.insert("messages", { author: `reader${i % READERS}`, body: `seed ${i}` });
    }
  },
});

// The bench has no vector scenarios.
export const setupVectors = mutation({ args: { rows: v.number() }, handler: async () => {} });
