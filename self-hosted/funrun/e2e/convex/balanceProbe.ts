import { v } from "convex/values";
import { QueryCtx } from "./_generated/server";

// The conductor routes on the *module* path -- pick() scores (module, addr)
// and takes the argmax -- so the balance tests need many distinct modules that
// do the same thing. balance/m0..m9 are those modules; the work lives here so
// there is one copy of it.
// `nonce` exists only to defeat the query cache: two calls with identical args
// are served from it and never reach a worker at all, so without this the
// balance tests measure zero executions. pick() hashes the module path, not the
// arguments, so varying it cannot perturb the routing under test.
export const probeArgs = {
  nonce: v.number(),
  reads: v.optional(v.number()),
};

// An indexed read, repeated `reads` times so a caller can hold a worker's
// in-flight slot long enough to push pick() past
// FUNRUN_CLIENT_MAX_REQUESTS_PER_UPSTREAM and force a spill. Each read is a
// round trip to the conductor's FunctionHost, so the hold costs latency rather
// than CPU -- a busy-wait would blow the 1s query limit once enough of these
// run at once on the same worker.
export async function probeHandler(
  ctx: QueryCtx,
  { reads }: { nonce: number; reads?: number },
) {
  let rows = 0;
  for (let i = 0; i < (reads ?? 1); i++) {
    const page = await ctx.db
      .query("messages")
      .withIndex("by_author", (q) => q.eq("author", "reader0"))
      .collect();
    rows = page.length;
  }
  return rows;
}
