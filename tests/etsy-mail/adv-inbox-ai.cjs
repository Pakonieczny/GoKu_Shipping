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
function test(name, fn) {
  try { fn(); passed++; console.log("ok -", name); }
  catch (e) { failed++; console.log("FAIL -", name, "\n   ", e.message); }
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

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
