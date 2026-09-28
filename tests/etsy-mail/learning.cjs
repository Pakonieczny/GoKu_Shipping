// Tests for the inbox AI's fact sheet and correction learning:
// netlify/functions/_etsyMailKnowledge.js and _etsyMailLearning.js.
//
//   node tests/etsy-mail/learning.cjs
//
// Pure functions only; nothing touches the network or Firestore.
"use strict";
const assert = require("node:assert/strict");
const path = require("node:path");
const fs = require("node:fs");
const FN = path.resolve(__dirname, "../../netlify/functions");
const K = require(path.join(FN, "_etsyMailKnowledge.js"));
const L = require(path.join(FN, "_etsyMailLearning.js"));

let passed = 0;
function test(name, fn) {
  fn();
  passed++;
  console.log("ok -", name);
}

test("an unchanged draft reads as unchanged, sign-off aside", () => {
  const ai = "Hi Sue, your charms ship with Priority Mail, 2-4 business days.\n\nMany Thanks,\nCustomBrites";
  const sent = "Hi Sue, your charms ship with Priority Mail, 2-4 business days.\n\nKind Regards,\nCustomBrites";
  const c = L.compareTexts(ai, sent);
  assert.equal(c.kind, "unchanged");
  assert.equal(c.numbersChanged, false);
});

test("a small edit is light, and a changed number is flagged", () => {
  const ai = "Hi Wayne, choose the $5.50 option on the re-shipping listing and add your house number in the note at checkout please.";
  const sent = "Hi Wayne, choose the $9 option on the re-shipping listing and add your house number in the note at checkout please.";
  const c = L.compareTexts(ai, sent);
  assert.equal(c.kind, "light");
  assert.equal(c.numbersChanged, true);
});

test("a rewrite and a replacement are told apart", () => {
  const ai = "Hi Luz-Adriana, sorry the extender came loose! We'd be happy to send you a new extender piece. Just confirm your address and we'll get one on its way.";
  const rewrite = "Hi Luz-Adriana, sorry the extender came loose! The extender was a free extra, and the necklace is still the 14 inches you ordered. If you'd like a new extender piece, confirm your address and we'll get one on its way.";
  const replaced = "Yes the earrings come as a pair with standard posts and silicone backings.";
  assert.equal(L.compareTexts(ai, rewrite).kind, "rewrite");
  assert.equal(L.compareTexts(ai, replaced).kind, "replaced");
  assert.ok(L.compareTexts(ai, rewrite).added.length >= 1);
});

test("ISO weeks roll over at the year boundary", () => {
  assert.equal(L.isoWeek(Date.UTC(2026, 8, 27)), "2026-W39");
  assert.equal(L.isoWeek(Date.UTC(2027, 0, 1)), "2026-W53");
  assert.equal(L.gapId("Do the swan studs have silicone backs?"), L.gapId("do the SWAN studs have silicone backs"));
});

test("the default fact sheet has stable ids, known families and no banned claims", () => {
  const facts = K.cleanFacts(K.DEFAULT_FACTS);
  assert.ok(facts.length >= 40, "at least 40 default facts");
  const ids = new Set();
  for (const f of facts) {
    assert.ok(/^[a-z][a-z0-9-]+$/.test(f.id), "id " + f.id);
    assert.ok(!ids.has(f.id), "duplicate id " + f.id);
    ids.add(f.id);
    assert.ok(K.FAMILY_ORDER.includes(f.family), "family " + f.family);
    assert.ok(!/niagara|chit\s*chats|border/i.test(f.text), "banned word in " + f.id);
    assert.ok(!/\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.? \d{1,2}\b/i.test(f.text), "calendar date in " + f.id);
  }
  const text = facts.map(f => f.text).join("\n");
  assert.ok(/4-6 business days/.test(text), "production 4-6 business days");
  assert.ok(/silicone/i.test(text), "stud backs");
  assert.ok(/bracelet/i.test(text), "bracelets");
});

test("owner edits override a default fact; added facts join; bad input is ignored", () => {
  const first = K.cleanFacts(K.DEFAULT_FACTS)[0];
  const eff = K.effectiveFacts({
    overrides: { [first.id]: { text: "Edited by the owner.", status: "active" }, nope: { text: "x" } },
    added: [{ id: "f123", family: "Rings", text: "Rings come in sizes 5 to 8.", status: "active", source: "owner" },
            { id: first.id, family: "Rings", text: "must not replace a default" }, null, { text: "" }]
  });
  assert.equal(eff.find(f => f.id === first.id).text, "Edited by the owner.");
  assert.equal(eff.filter(f => f.id === first.id).length, 1);
  assert.ok(eff.some(f => f.id === "f123"));
  assert.equal(K.effectiveFacts(null).length, K.cleanFacts(K.DEFAULT_FACTS).length);
});

test("the knowledge block is deterministic, grouped, and shows only active rules for the audience", () => {
  const facts = [
    { id: "b", family: "Shipping", text: "US shipping is free.", status: "active" },
    { id: "a", family: "What we make", text: "We make charms.", status: "active" },
    { id: "c", family: "Shipping", text: "Hidden.", status: "off" }
  ];
  const rules = [
    { id: "r2", text: "Sales only rule.", status: "active", scope: "sales" },
    { id: "r1", text: "Everyone rule.", status: "active", scope: "all" },
    { id: "r3", text: "Suggested rule.", status: "suggested", scope: "all" }
  ];
  const a = K.renderKnowledgeBlock({ facts, rules }, "support");
  const b = K.renderKnowledgeBlock({ facts: facts.slice().reverse(), rules: rules.slice().reverse() }, "support");
  assert.equal(a, b);
  assert.ok(a.indexOf("What we make:") < a.indexOf("Shipping:"));
  assert.ok(!/Hidden\./.test(a));
  assert.ok(/Everyone rule\./.test(a) && !/Sales only rule\./.test(a) && !/Suggested rule\./.test(a));
  assert.ok(/Sales only rule\./.test(K.renderKnowledgeBlock({ facts, rules }, "sales")));
  assert.ok(!/LEARNED FROM/.test(K.renderKnowledgeBlock({ facts, rules: [] }, "support")));
});

test("the stored option sheet's guessed earring back is taken out", () => {
  const out = K.scrubStyleFacts({ silhouette: { description: "The stud face is a die-cut silhouette, mounted on a sterling silver post + butterfly back. Size codes...", postType: "sterling silver post + butterfly back" } });
  assert.ok(!/butterfly/i.test(JSON.stringify(out)));
  assert.equal(out.silhouette.postType, undefined);
  assert.equal(K.scrubStyleFacts(null), null);
});

test("the automatic away reply is recognised, real replies are not", () => {
  assert.ok(K.isAwayMessage("Hi and Thanks so much for your message! We are away from the shop at the moment, but will be more than happy to answer your questions when we return! Our regular business hours are Monday-Friday 8am-4pm EST."));
  assert.ok(!K.isAwayMessage("Yes the earrings come as a pair and not just 1 earring."));
  assert.ok(!K.isAwayMessage("We'll be closed next week for the holiday, so it ships Monday."));
});

test("the stored sales prompt's contradicting lines are corrected", () => {
  const src = fs.readFileSync(path.join(FN, "etsyMailSalesAgent-background.js"), "utf8");
  const m = src.match(/const SALES_PROMPT_PATCHES = (\[[\s\S]*?\n\]);/);
  assert.ok(m, "patch list present");
  const patches = eval(m[1]);
  const stored = "If a customer wants a ring, bracelet, body jewelry, or anything else, gently say custom isn't available for that and offer to point them to existing Etsy listings.\n"
    + "  - **Off-catalog request** — ring, bracelet, body jewelry, etc.\nUSPS Priority Mail (+$18, 1-3 days)\n"
    + "in the standard escalation form: \"Thanks for sending this over. I need to look at this carefully before I can speak to specifics.\" followed by";
  let out = stored;
  for (const [from, to] of patches) out = out.split(from).join(to);
  assert.ok(!/ring, bracelet/.test(out));
  assert.ok(/2-4 days/.test(out) && !/1-3 days/.test(out));
  assert.ok(!/I need to look at this carefully/.test(out));
});

console.log(`${passed} learning and fact sheet tests passed`);
