// One of ten identical modules. Ten distinct module paths is what makes the
// spread test meaningful: pick() hashes (module, addr), so each module lands
// on its own home worker independently.
import { query } from "../_generated/server";
import { probeArgs, probeHandler } from "../balanceProbe";

export const probe = query({ args: probeArgs, handler: probeHandler });
