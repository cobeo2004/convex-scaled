import { query } from "./_generated/server";
import { v } from "convex/values";

export const find = query({
  args: { term: v.string() },
  handler: async (ctx, { term }) =>
    ctx.db
      .query("messages")
      .withSearchIndex("search_body", (q) => q.search("body", term))
      .collect(),
});
