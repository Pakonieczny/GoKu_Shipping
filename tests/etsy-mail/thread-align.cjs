// Tests for netlify/functions/_etsyMailThreadAlign.js: matching a scrape of
// an Etsy conversation to the stored messages by order, not by time.
//
//   node tests/etsy-mail/thread-align.cjs
//
// Synthetic threads only; nothing touches the network or Firestore.
"use strict";
const assert = require("node:assert/strict");
const path = require("node:path");
const A = require(path.resolve(__dirname, "../../netlify/functions/_etsyMailThreadAlign.js"));

let passed = 0;
function test(name, fn) {
  fn();
  passed++;
  console.log("ok -", name);
}

// Times as the scraper stores them: the minute read off the page plus the
// message's page position in ms.
const at = (day, hh, mm, pos) => Date.UTC(2026, 7, day, hh, mm) + pos;
const C1 = Date.UTC(2026, 7, 27, 15, 0);   // first full scrape
const C2 = Date.UTC(2026, 7, 28, 16, 0);   // later partial scrape
const IMG = "https://i.etsystatic.com/icm/aa11bb/915555555/icm_fullxfull.915555555_x.jpg";

const msg = (id, role, text, tsMs, createdMs, imageUrls) => {
  const m = { senderRole: role, text, imageUrls: imageUrls || [] };
  return { id, fp: A.fingerprint(m), tsMs, createdMs };
};
const a = msg("a", "customer", "Can this be made in gold?", at(26, 14, 0, 0), C1);
const b = msg("b", "staff", "Yes, the original is fine in gold.", at(26, 14, 5, 1), C1);
const c = msg("c", "customer", "Great, thank you", at(27, 13, 0, 2), C1);
const d = msg("d", "staff", "Here are two options as requested.", at(27, 13, 10, 3), C1);
// What the old time-based dedupe left: the partial scrape stamped b with the
// next day's first time, missed it, and stored it again.
const bCopy = msg("b2", "staff", "Yes, the original is fine in gold.", at(27, 13, 0, 0), C2);
const e = msg("e", "customer", "One more question", at(28, 15, 0, 3), C2);

test("text and image keys ignore page noise", () => {
  assert.equal(A.normalizeText("Message:  Hi “there”  Translate to English"), 'hi "there"');
  assert.equal(A.imageId(IMG), "915555555");
  assert.equal(A.imageId(IMG.replace("fullxfull", "150x150") + "?v=2"), "915555555");
  const small = A.fingerprint({ senderRole: "staff", text: "", imageUrls: [IMG.replace("fullxfull", "150x150")] });
  assert.equal(small, A.fingerprint({ senderRole: "shop_owner", imageUrls: [IMG] }));
  assert.notEqual(small, A.fingerprint({ senderRole: "customer", imageUrls: [IMG] }));
});

test("stored copies from the old dedupe are found", () => {
  assert.deepEqual(A.findLegacyDuplicates([a, b, c, d, bCopy, e]), ["b2"]);
});

test("a message really sent twice is kept", () => {
  const C3 = Date.UTC(2026, 7, 30, 16, 0);
  const again = msg("a2", "customer", "Can this be made in gold?", at(30, 14, 0, 0), C3);
  assert.deepEqual(A.findLegacyDuplicates([a, b, c, d, e, again]), []);
});

// A partial scrape: b sits above the first day header and borrows c's time.
const page = [
  { senderRole: "staff", text: "Yes, the original is fine in gold.", tsMs: at(27, 13, 0, 0) },
  { senderRole: "customer", text: "Great, thank you", tsMs: at(27, 13, 0, 1) },
  { senderRole: "staff", text: "Here are two options as requested.", tsMs: at(27, 13, 10, 2) },
  { senderRole: "staff", text: "", imageUrls: [IMG], tsMs: at(28, 15, 0, 3) },   // missed before, late time
  { senderRole: "customer", text: "One more question", tsMs: at(28, 15, 0, 4) },
];
const incoming = page.map(p => ({ fp: A.fingerprint(p), tsMs: p.tsMs }));

test("a re-scrape stores nothing twice and places the missed image in order", () => {
  const r = A.alignScrape([a, b, c, d, e], incoming, { scrapedAt: C2 + 3600e3 });
  const fresh = r.matchOf.map((j, i) => (j < 0 ? i : -1)).filter(i => i >= 0);
  assert.deepEqual(fresh, [3]);
  assert.ok(r.newTs[3] > d.tsMs && r.newTs[3] < e.tsMs, "image lands between its neighbours");
  assert.deepEqual(r.duplicates, []);
  assert.equal(r.gap, false);
  assert.equal(r.sorted[r.matchOf[0]].id, "b");
});

test("a re-scrape proves and removes the stored copy", () => {
  const r = A.alignScrape([a, b, c, d, bCopy, e], incoming, { scrapedAt: C2 + 3600e3 });
  assert.equal(r.sorted[r.matchOf[0]].id, "b", "the first-saved copy is the one kept");
  assert.deepEqual(r.duplicates, ["b2"]);
});

test("the same words twice on the page stay twice", () => {
  const t1 = msg("t1", "customer", "Thank you!", at(26, 14, 0, 0), C1);
  const t2 = msg("t2", "staff", "You're welcome", at(26, 14, 1, 1), C1);
  const t3 = msg("t3", "customer", "Thank you!", at(26, 14, 2, 2), C1);
  const inc = [t1, t2, t3].map(x => ({ fp: x.fp, tsMs: x.tsMs }));
  const r = A.alignScrape([t1, t2, t3], inc);
  assert.deepEqual(r.matchOf, [0, 1, 2]);
  assert.deepEqual(r.duplicates, []);
});

test("a scrape that shares nothing with storage asks for a full scrape", () => {
  const r = A.alignScrape([a, b], [{ fp: e.fp, tsMs: e.tsMs }]);
  assert.equal(r.gap, true);
  assert.equal(A.alignScrape([], incoming).gap, false);
});

console.log(`\n${passed} tests passed`);
