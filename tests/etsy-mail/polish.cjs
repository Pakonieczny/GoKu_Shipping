// Tests for the composer's Polish button: netlify/functions/etsyMailPolish.js
// and the learning mark on a polished send (_etsyMailLearning.recordOutcome).
//
//   node tests/etsy-mail/polish.cjs
//
// The model is a stub (no paid call, no network); Firestore is a fake.
"use strict";
const assert = require("node:assert/strict");
const path = require("node:path");
const fs = require("node:fs");
const Module = require("node:module");

const FN = path.resolve(__dirname, "../../netlify/functions");
const calls = [];
let reply = () => ({ stop_reason: "end_turn", content: [{ type: "text", text: "Hi Sue,\n\nYour order ships tomorrow.\n\nMany Thanks,\nCustomBrites" }] });
const realLoad = Module._load;
Module._load = function (req, parent, ...rest) {
  if (/(^|\/)_etsyMailAnthropic(\.js)?$/.test(req)) {
    return { callClaudeRaw: async (args) => { calls.push(args); return reply(args); } };
  }
  return realLoad.call(this, req, parent, ...rest);
};
const P = require(path.join(FN, "etsyMailPolish.js"));
const { POLISH } = require(path.join(FN, "_etsyMailPrompts.js"));
const L = require(path.join(FN, "_etsyMailLearning.js"));
Module._load = realLoad;

const post = (body, headers = {}) => P.handler({ httpMethod: "POST", headers, body: JSON.stringify(body) });
const parse = (r) => JSON.parse(r.body);

let passed = 0;
async function test(name, fn) {
  calls.length = 0;
  await fn();
  passed++;
  console.log("ok -", name);
}

(async () => {
  await test("an empty or blank box is refused without a model call", async () => {
    for (const text of ["", "   \n\t "]) {
      const r = await post({ text });
      assert.equal(r.statusCode, 400);
      assert.equal(parse(r).ok, false);
      assert.match(parse(r).error, /Type a reply first/);
    }
    assert.equal(calls.length, 0);
  });

  await test("one call with the AI Draft writer, low effort, a small budget and a timeout", async () => {
    const r = await post({ text: "hi sue ur order ships tmrw thx", customerName: "Sue", customerMessages: ["When does it ship?"] });
    assert.equal(r.statusCode, 200);
    assert.deepEqual(parse(r), { ok: true, text: "Hi Sue,\n\nYour order ships tomorrow.\n\nMany Thanks,\nCustomBrites" });
    assert.equal(calls.length, 1);
    const c = calls[0];
    const draftSrc = fs.readFileSync(path.join(FN, "etsyMailDraftReply.js"), "utf8");
    const draftModel = draftSrc.match(/const AI_MODEL\s*=\s*process\.env\.ETSYMAIL_AI_MODEL\s*\|\|\s*"([^"]+)"/)[1];
    assert.equal(c.model, draftModel);
    assert.equal(c.effort, "low");
    assert.ok(c.maxTokens <= 4000);
    assert.ok(c.timeoutMs > 0 && c.timeoutMs < 26000);
    assert.equal(c.system, POLISH);
    const user = c.messages[0].content[0].text;
    assert.match(user, /<staff_message>\nhi sue ur order ships tmrw thx\n<\/staff_message>/);
    assert.match(user, /<customer_messages name="Sue">\n- When does it ship\?/);
  });

  await test("the customer context is the latest few messages, a few hundred characters", async () => {
    const long = "x".repeat(700);
    const ctx = P._internals.customerContext(["old one", "second", long, "newest"]);
    assert.equal(ctx.length, 3);
    assert.equal(ctx[ctx.length - 1], "newest");
    assert.ok(ctx.join("").length <= 900);
    assert.deepEqual(P._internals.customerContext(null), []);
  });

  await test("the rules keep facts, add nothing, and never introduce the owner's hard rules", () => {
    for (const rx of [/as short as it can be without losing any meaning/, /Keep exactly as written: every fact, number, price/,
      /tracking number, link, code and promise/, /Add nothing/, /never answer anything the staff message doesn't answer/,
      /delivery or arrival date, or any timeline/, /refund, remake, replacement, reship or discount/, /Buffalo, NY/,
      /keep it as written \(it is their decision\)/, /language the staff wrote in/, /Many Thanks,\nCustomBrites/,
      /let us know if you have any questions/, /Return only the message text/]) {
      assert.match(POLISH, rx);
    }
  });

  await test("wrapping quotes and fences are taken off the model's text", () => {
    const out = (t) => P._internals.cleanOutput({ content: [{ type: "text", text: t }] });
    assert.equal(out('"Hi Sue, thanks!"'), "Hi Sue, thanks!");
    assert.equal(out("```\nHi Sue\n```"), "Hi Sue");
    assert.equal(out('Hi "Sue"'), 'Hi "Sue"');
  });

  await test("a failed, cut-off or empty answer is a clear error for the toast", async () => {
    reply = () => { throw new Error("overloaded"); };
    let r = await post({ text: "hello" });
    assert.equal(r.statusCode, 502);
    assert.equal(parse(r).ok, false);
    assert.match(parse(r).error, /didn't answer/);
    reply = () => ({ stop_reason: "max_tokens", content: [{ type: "text", text: "Hi" }] });
    r = await post({ text: "hello" });
    assert.match(parse(r).error, /cut off/);
    reply = () => ({ stop_reason: "end_turn", content: [] });
    r = await post({ text: "hello" });
    assert.match(parse(r).error, /empty/);
  });

  await test("guarded like the other inbox AI endpoints", async () => {
    process.env.ETSYMAIL_EXTENSION_SECRET = "s3cret";
    try {
      const r = await post({ text: "hello" });
      assert.equal(r.statusCode, 401);
      assert.equal(calls.length, 0);
    } finally { delete process.env.ETSYMAIL_EXTENSION_SECRET; }
    const r = await P.handler({ httpMethod: "GET", headers: {} });
    assert.equal(r.statusCode, 405);
  });

  await test("a polished send is recorded but never learned from", async () => {
    const written = [];
    const db = {
      collection: () => ({ doc: () => ({ get: async () => ({ exists: false }) }) }),
      batch: () => ({ set: (ref, doc) => written.push(doc), commit: async () => {} })
    };
    const admin = { firestore: { FieldValue: { serverTimestamp: () => "ts", increment: (n) => n } } };
    const prev = { generatedByAI: true, status: "draft", text: "Hi Sue, sorry for the delay with your order, it will ship soon. Many Thanks" };
    const sentText = "Hi Sue, your necklace is being engraved now and leaves our studio this week, with tracking to follow.";
    const plain = await L.recordOutcome({ db, admin, draftId: "d1", threadId: "t1", prev, sentText, sendOrigin: "manual" });
    assert.equal(plain.learnStatus, "pending");
    assert.equal(plain.polished, undefined);
    const pol = await L.recordOutcome({ db, admin, draftId: "d2", threadId: "t1", prev, sentText, sendOrigin: "manual", polished: true });
    assert.equal(pol.learnStatus, "skip");
    assert.equal(pol.polished, true);
  });

  await test("the send path and the learner both honour the mark", () => {
    const send = fs.readFileSync(path.join(FN, "etsyMailDraftSend.js"), "utf8");
    assert.match(send, /polished: body\.polished === true/);
    const learn = fs.readFileSync(path.join(FN, "etsyMailLearn-background.js"), "utf8");
    assert.match(learn, /polished !== true\)\.map/);
    const entries = require("../../scripts/netlify-function-entries.json");
    assert.ok(entries.endpoints.includes("etsyMailPolish.js"));
  });

  console.log(`\n${passed} passed`);
})().catch((e) => { console.error(e); process.exit(1); });
