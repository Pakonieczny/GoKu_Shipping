// Tests for netlify/functions/_etsyMailPrompts.js: the short instructions
// both inbox AIs use, the switch back to the long ones, and the owner's
// fixed wording enforced after the model answers.
//
//   node tests/etsy-mail/prompts.cjs
"use strict";
const assert = require("node:assert/strict");
const path = require("node:path");
const P = require(path.resolve(__dirname, "../../netlify/functions/_etsyMailPrompts.js"));

let passed = 0;
function test(name, fn) { fn(); passed++; console.log("ok -", name); }

const NG = "Unfortunately we can't guarantee delivery dates, whichever shipping option is chosen.";

test("short instructions are the default; legacy only when set", () => {
  assert.equal(P.useShortPrompts(null), true);
  assert.equal(P.useShortPrompts({}), true);
  assert.equal(P.useShortPrompts({ promptVersion: "short" }), true);
  assert.equal(P.useShortPrompts({ promptVersion: "legacy" }), false);
  assert.equal(P.useShortPrompts({ promptVersion: "Legacy" }), false);
});
test("each AI's instructions stay short", () => {
  assert.ok(P.CORE.length + P.SUPPORT.length < 25000, "support");
  assert.ok(P.CORE.length + P.SALES.length < 25000, "sales");
});
test("the system prompts carry the fact sheet last and the shop's announcement clipped", () => {
  const sys = P.buildSupportSystem({ config: null, shopEnrichment: { announcement: "SALE NOW\n\nlong boilerplate" }, knowledgeBlock: "FACTS" });
  assert.ok(sys.startsWith(P.CORE));
  assert.ok(sys.endsWith("FACTS"));
  assert.ok(sys.includes("SALE NOW") && !sys.includes("long boilerplate"));
  assert.ok(P.buildSalesSystem({ knowledgeBlock: "FACTS" }).includes("OUTPUT: ONE JSON OBJECT"));
});
test("the sales contract names every state and action the validator accepts, and no abandon", () => {
  for (const w of ["discovery", "spec", "quote", "revision", "pending_close_approval", "abandoned", "completed", "non_sales",
                   "compute_quote", "attach_collateral", "ask_one_question", "confirm_acceptance_and_create_listing",
                   "escalate_to_human", "acknowledge", "review_decision", "items_quoted", "quoted_total_usd"]) {
    assert.ok(P.SALES.includes(w), w);
  }
  assert.ok(!/\babandon"/.test(P.SALES));
});
test("an English reply with a business-day range gets the no-guarantee sentence before the sign-off", () => {
  const out = P.finishReplyText("Hi Ann,\n\nIt should ship within the next 2-4 business days.\n\nMany Thanks,\nCustomBrites");
  assert.equal(out, "Hi Ann,\n\nIt should ship within the next 2-4 business days. " + NG + "\n\nMany Thanks,\nCustomBrites");
});
test("the sentence is not added twice, nor to a reply without timing", () => {
  const once = "Hi Ann, it ships in 1-3 business days. " + NG + "\n\nMany Thanks,\nCustomBrites";
  assert.equal(P.finishReplyText(once), once);
  assert.equal(P.finishReplyText("Hi Ann,\n\nYes, we can make it in gold.\n\nMany Thanks,\nCustomBrites"),
    "Hi Ann,\n\nYes, we can make it in gold.\n\nMany Thanks,\nCustomBrites");
});
test("a missing sign-off is added and a run-on one moved to its own lines", () => {
  assert.equal(P.finishReplyText("Hi Ann,\n\nYes, we can."), "Hi Ann,\n\nYes, we can.\n\nMany Thanks,\nCustomBrites");
  assert.equal(P.finishReplyText("Hi Ann, yes. Many Thanks, CustomBrites"), "Hi Ann, yes.\n\nMany Thanks,\nCustomBrites");
  assert.equal(P.finishReplyText(""), "");
});
test("short tool descriptions keep every schema", () => {
  const spec = [{ name: "lookup_order_details", description: "long", input_schema: { type: "object",
    properties: { receiptId: { type: "string", description: "x".repeat(200) } }, required: ["receiptId"] } }];
  const out = P.shortenTools(spec, P.SUPPORT_TOOL_TEXT);
  assert.equal(out[0].description, P.SUPPORT_TOOL_TEXT.lookup_order_details);
  assert.deepEqual(out[0].input_schema.required, ["receiptId"]);
  assert.equal(spec[0].description, "long", "the original is untouched");
});

console.log(`\n${passed} passed`);
