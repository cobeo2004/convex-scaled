// Zero-arg entry points for crates/load_generator (see ../../bench): its
// RunFunction scenario calls queries with `{ cacheBreaker }` and
// mutations/actions with `{}`, so these pick the arguments themselves.
import { action, mutation, query } from "./_generated/server";
import { v } from "convex/values";

// Readers are seeded by setup:setupMessages and never written to, so each
// byAuthor call reads the same number of rows for the whole run.
export const READERS = 10;

export const byAuthor = query({
  args: { cacheBreaker: v.optional(v.number()) },
  handler: async (ctx, { cacheBreaker }) =>
    ctx.db
      .query("messages")
      .withIndex("by_author", (q) =>
        q.eq("author", `reader${(cacheBreaker ?? 0) % READERS}`),
      )
      .collect(),
});

export const send = mutation({
  args: {},
  handler: async (ctx) =>
    ctx.db.insert("messages", {
      author: `writer${Math.floor(Math.random() * 100)}`,
      body: "bench",
    }),
});

// Same body as messages:increment, over 100 counters to keep OCC conflicts rare.
export const increment = mutation({
  args: {},
  handler: async (ctx) => {
    const name = `counter${Math.floor(Math.random() * 100)}`;
    const row = await ctx.db
      .query("counters")
      .withIndex("by_name", (q) => q.eq("name", name))
      .unique();
    if (row) {
      await ctx.db.patch(row._id, { value: row.value + 1 });
      return row.value + 1;
    }
    await ctx.db.insert("counters", { name, value: 1 });
    return 1;
  },
});

// actions:burnCpu {ms: 50}.
export const burnCpu50 = action({
  args: {},
  handler: async () => {
    const end = Date.now() + 50;
    let x = 0;
    while (Date.now() < end) x += Math.sqrt(x + 1);
    return x > 0;
  },
});
