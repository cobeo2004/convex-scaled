import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  messages: defineTable({ author: v.string(), body: v.string() })
    .index("by_author", ["author"])
    .searchIndex("search_body", {
      searchField: "body",
      filterFields: ["author"],
    }),
  counters: defineTable({ name: v.string(), value: v.number() }).index(
    "by_name",
    ["name"],
  ),
});
