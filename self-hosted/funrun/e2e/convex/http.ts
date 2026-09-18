import { httpRouter } from "convex/server";
import { httpAction } from "./_generated/server";

const http = httpRouter();
http.route({
  path: "/echo",
  method: "POST",
  handler: httpAction(async (_ctx, req) => new Response(await req.text(), { status: 201 })),
});
http.route({
  path: "/stream",
  method: "GET",
  handler: httpAction(async () => {
    const body = new ReadableStream({
      start(c) { for (const p of ["a", "b", "c"]) c.enqueue(new TextEncoder().encode(p)); c.close(); },
    });
    return new Response(body);
  }),
});
export default http;
