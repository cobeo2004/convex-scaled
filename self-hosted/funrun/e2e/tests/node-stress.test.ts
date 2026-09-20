import { ConvexHttpClient } from "convex/browser";
import { expect, test } from "vitest";
import { api } from "../convex/_generated/api";

const URL = process.env.CONVEX_URL ?? "http://127.0.0.1:3210";
const http = new ConvexHttpClient(URL);
const CONCURRENCY = Number(process.env.NODE_STRESS_CONCURRENCY ?? 12);
const HOLD_MS = Number(process.env.NODE_STRESS_HOLD_MS ?? 3000);

// The pool admits FUNRUN_NODE_MAX_CONCURRENT (4) at a time, so most of these
// are refused on arrival. They must wait for a slot, not fail: an action is
// already committed when it is dispatched, so a refusal marks it Failed.
test(
  `${CONCURRENCY} concurrent node actions all finish`,
  { timeout: 180_000 },
  async () => {
    const results = await Promise.allSettled(
      Array.from({ length: CONCURRENCY }, () =>
        http.action(api.nodeActions.nodeSleep, { ms: HOLD_MS }),
      ),
    );
    const failed = results.flatMap((r) =>
      r.status === "rejected" ? [String(r.reason)] : [],
    );
    expect(failed).toEqual([]);
    if (process.env.EXPECT_NODE_HOST_PREFIX) {
      for (const r of results) {
        if (r.status === "fulfilled") {
          expect(
            r.value.host.startsWith(process.env.EXPECT_NODE_HOST_PREFIX!),
          ).toBe(true);
        }
      }
    }
  },
);
