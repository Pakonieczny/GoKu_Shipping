// Adversarial checks of the inbox AI server rules (wave 3, area 18).
// Pure functions only: no model, no Firestore, no network.
//
//   node tests/etsy-mail/adv-inbox-ai.cjs
"use strict";
const assert = require("node:assert/strict");
const path = require("node:path");
const FN = path.resolve(__dirname, "../../netlify/functions");
const V = require(path.join(FN, "_etsyMailVetoes.js"));
const K = require(path.join(FN, "_etsyMailKnowledge.js"));

let passed = 0, failed = 0;
const pending = [];
function test(name, fn) {
  const ok = () => { passed++; console.log("ok -", name); };
  const bad = e => { failed++; console.log("FAIL -", name, "\n   ", e.message); };
  try { const r = fn(); if (r && typeof r.then === "function") pending.push(r.then(ok, bad)); else ok(); }
  catch (e) { bad(e); }
}

const AWAY = "Hi and Thanks so much for your message! We are away from the shop at the moment, but will be more than happy to answer your questions when we return! Our regular business hours are Monday-Friday 8am-4pm EST.";

// Since the away reply stopped counting as our latest reply (9d01300), the
// pipeline drafts the question before it. The vetoes must still see it.
test("a refund or damage claim before the shop's away reply still vetoes auto-send", () => {
  const newestFirst = [
    { direction: "outbound", text: AWAY },
    { direction: "inbound", text: "My necklace arrived broken, I want a refund." },
    { direction: "outbound", text: "Your order has shipped!" }
  ];
  const txt = V.unansweredInboundText(newestFirst);
  assert.ok(txt && /arrived broken/.test(txt), "unanswered text: " + txt);
  const veto = V.applyDeterministicVetoes({ inboundText: txt, draftText: "So sorry! We'll make you a new one.", draftToolCalls: [] });
  assert.equal(veto.vetoed, true);
});

// The fact sheet now states the business hours, so a real reply quotes them.
test("a real staff reply that states the business hours is not the away reply", () => {
  assert.equal(K.isAwayMessage("Hi Ann, our regular business hours are Monday to Friday, 8am to 4pm Eastern time. Your order ships tomorrow."), false);
  assert.equal(K.isAwayMessage("Sorry for the wait, we were away from the shop last week. Your order is in production now."), false);
  assert.equal(K.isAwayMessage(AWAY), true);
  assert.equal(K.isAwayMessage("Thanks for your message! We're out of the office until Monday."), true);
});

// Owner's rule: no concrete delivery dates (the drafter's date guard).
test("a reply that says when the customer will receive it is held; lost-package and range wording are not", () => {
  assert.ok(V.deliveryDateSentence("Hi Ann,\n\nYou should receive it by Friday.\n\nMany Thanks,\nCustomBrites"));
  assert.ok(V.deliveryDateSentence("You'll have your necklace by October 3."));
  assert.ok(V.deliveryDateSentence("It should be at your door by 10/3."));
  assert.ok(V.deliveryDateSentence("It should arrive by Oct 3."));
  assert.equal(V.deliveryDateSentence("If it hasn't arrived by Oct 3, let us know and we'll look into it."), null);
  assert.equal(V.deliveryDateSentence("It ships in 4-6 business days, then 2-5 business days in transit."), null);
  assert.equal(V.deliveryDateSentence("We received your photo on Monday, thank you!"), null);
  assert.equal(V.deliveryDateSentence("We'll have your order ready to ship by Friday."), null);
});

// ── Paul's hard rules: gaps found by the inbox AI check ──────────────
// Refund, remake, reship, replacement and discount drafts always wait for
// a person. The support drafter's check missed an offer to send or make a
// new item or part.
test("an offer to send or make a new item or part waits for a person", () => {
  assert.equal(typeof V.remedyOfferSentence, "function", "remedyOfferSentence exported");
  for (const t of [
    "So sorry about that! We'll send you a new clasp.",
    "We'll make you a new one right away.",
    "We'd be happy to send you a new extender piece.",
    "I'll ship out a new pair this week.",
    "We can make you another necklace.",
    "We will mail you a fresh chain.",
    "Happy to send another one out to you!",
    "We'll send you a replacement clasp.",
    "We'll refund the shipping.",
    "We'll remake it with the correct spelling."
  ]) assert.ok(V.remedyOfferSentence("Hi Ann,\n\n" + t + "\n\nMany Thanks,\nCustomBrites"), "should hold: " + t);
});
test("a new listing, link, proof or photo is sales work and does not wait", () => {
  for (const t of [
    "We'll make you a new listing with the 18 inch chain.",
    "I'll create a new custom listing for you now.",
    "We'll send you a new link once it's ready.",
    "We'll send you a new proof tomorrow.",
    "We'll send you another photo of the charm.",
    "We'll send you another message when it ships.",
    "We'll get you a new tracking number.",
    "Your order ships in 4-6 business days.",
    "We make each piece by hand in our studio.",
    "We make a new batch every week.",
    "You can get another chain length at checkout."
  ]) assert.equal(V.remedyOfferSentence(t), null, "should not hold: " + t);
  assert.ok(V.remedyOfferSentence("We'll get a new clasp out to you."), "a shop's get-a-new-part offer still holds");
});

// The sales AI had only a prompt rule against delivery dates. The pipeline's
// sales auto-send runs applyDeterministicVetoes with "custom" excluded.
test("a sales draft that gives a delivery date is held by the code, not only the prompt", () => {
  const dated = V.applyDeterministicVetoes({
    inboundText: "Can I get the necklace for my mom's birthday?",
    draftText: "Hi Ann,\n\nYes! If you order today it will arrive by Friday.\n\nMany Thanks,\nCustomBrites",
    draftToolCalls: [], excludePatternIds: ["custom"]
  });
  assert.equal(dated.vetoed, true);
  assert.ok(dated.reasons.some(r => /^outbound_delivery_date/.test(r)), dated.reasons.join(";"));
  const range = V.applyDeterministicVetoes({
    inboundText: "How long does it take?",
    draftText: "Hi Ann,\n\nIt ships within 4-6 business days, then 2-5 business days in transit.\n\nMany Thanks,\nCustomBrites",
    draftToolCalls: [], excludePatternIds: ["custom"]
  });
  assert.equal(range.vetoed, false, range.reasons.join(";"));
  // The sales agent marks such a draft for a person too (readyForHumanApproval).
  const src = require("node:fs").readFileSync(path.join(FN, "etsyMailSalesAgent-background.js"), "utf8");
  assert.ok(/deliveryDateSentence\(replyText\)/.test(src), "sales agent checks its reply");
  assert.ok(/salesHoldForPerson\s*=[^;]*salesDated/.test(src), "a dated sales reply is held for a person");
});

// Learning: a lesson marked direct went live after one conversation even
// with a calendar date or a delivery promise in it. Fake Firestore and a
// canned learner answer: no model call.
test("a learned rule or fact with a date or delivery promise waits for a person", async () => {
  const Module = require("node:module");
  const store = {
    "EtsyMail_DraftOutcomes/o1": { learnStatus: "pending", threadId: "T1", route: "support", kind: "rewrite",
      customerAsked: "Will it come before Christmas?", aiText: "It ships in 4-6 business days.", sentText: "Order by Dec 10 and it arrives by Christmas." }
  };
  const clone = v => v === undefined ? undefined : JSON.parse(JSON.stringify(v));
  const ref = (c, id) => ({ id,
    get: async () => ({ exists: !!store[c + "/" + id], id, ref: ref(c, id), data: () => clone(store[c + "/" + id]) }),
    set: async (d, o) => { store[c + "/" + id] = o && o.merge ? { ...(store[c + "/" + id] || {}), ...clone(d) } : clone(d); },
    update: async d => { store[c + "/" + id] = { ...(store[c + "/" + id] || {}), ...clone(d) }; },
    delete: async () => { delete store[c + "/" + id]; } });
  const query = (c, f, op, v) => ({ limit: () => ({ get: async () => {
    const docs = Object.keys(store).filter(k => k.startsWith(c + "/")).map(k => k.slice(c.length + 1))
      .filter(id => op === "==" ? store[c + "/" + id][f] === v : store[c + "/" + id][f] < v)
      .map(id => ({ id, ref: ref(c, id), data: () => clone(store[c + "/" + id]) }));
    return { docs, empty: !docs.length, size: docs.length };
  } }) });
  const db = { collection: c => ({ doc: id => ref(c, id), where: (f, op, v) => query(c, f, op, v) }),
    getAll: async (...rs) => Promise.all(rs.map(r => r.get())),
    batch: () => { const ops = []; return { set: (r, d, o) => ops.push(() => r.set(d, o)), update: (r, d) => ops.push(() => r.update(d)),
      delete: r => ops.push(() => r.delete()), commit: async () => { for (const op of ops) await op(); } }; } };
  const answer = { rules: [
      { text: "Tell customers who order before Dec 10 that it will arrive by Christmas.", scope: "all", direct: true, outcomes: ["o1"] },
      { text: "Tell customers their order will be delivered within 3 days.", scope: "all", direct: true, outcomes: ["o1"] },
      { text: "Offer the gift box when a customer says the order is a birthday present.", scope: "all", direct: true, outcomes: ["o1"] }],
    facts: [
      { family: "Shipping", text: "Orders placed by December 15 arrive before Christmas.", direct: true, outcomes: ["o1"] },
      { family: "Shipping", text: "Priority Mail takes 1-3 business days in transit, and we can't guarantee delivery dates.", direct: true, outcomes: ["o1"] }],
    ignored: [] };
  const fakes = { firebaseAdmin: { firestore: () => db },
    _etsyMailAnthropic: { callClaudeRaw: async () => ({ content: [{ type: "text", text: JSON.stringify(answer) }], usage: null }) } };
  const real = Module._load;
  Module._load = function (req, ...rest) {
    const m = /[\\/](firebaseAdmin|_etsyMailAnthropic)(\.js)?$/.exec(req);
    return m ? fakes[m[1]] : real.call(this, req, ...rest);
  };
  let learn;
  try { learn = require(path.join(FN, "etsyMailLearn-background.js")); } finally { Module._load = real; }
  await learn.runLearning({ force: true });
  const rules = () => store["EtsyMail_Config/learnedRules"].rules;
  const facts = () => store["EtsyMail_Config/productFacts"].added;
  const find = (list, re) => { const x = list.find(r => re.test(r.text)); assert.ok(x, "learned: " + re); return x; };
  assert.equal(find(rules(), /Dec 10/).status, "suggested", "a dated rule waits");
  assert.ok(find(rules(), /Dec 10/).heldReason, "and says why");
  assert.equal(find(rules(), /within 3 days/).status, "suggested", "a delivery promise waits");
  assert.equal(find(rules(), /gift box/).status, "active", "a plain direct rule still goes live");
  assert.equal(find(facts(), /December 15/).status, "suggested", "a dated fact waits");
  assert.equal(find(facts(), /Priority Mail/).status, "active", "a business-day range with the no-guarantee wording goes live");
  // The owner's own "on" is never overridden.
  Object.assign(find(rules(), /Dec 10/), { status: "active", ownerSet: true });
  store["EtsyMail_DraftOutcomes/o2"] = { ...store["EtsyMail_DraftOutcomes/o1"], learnStatus: "pending", threadId: "T2" };
  answer.rules = [{ text: "Tell customers who order before Dec 10 that it will arrive by Christmas.", direct: true, outcomes: ["o2"] }];
  answer.facts = [];
  await learn.runLearning({ force: true });
  assert.equal(find(rules(), /Dec 10/).status, "active", "the owner's on is kept");
});

Promise.all(pending).then(() => {
  console.log(`\n${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
});
