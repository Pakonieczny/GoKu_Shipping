// RU S6 (Firestore reads): the AI's shop-listing search (searchListings in etsyMailListingsCatalog.js) scores every active listing.
// Each function instance that searches (the drafter, the sales agent, the catalog function) used to read the whole catalog
// (one document per active listing: 5,762 on 26 Sep) for its own copy, on its first search and again 30 minutes later. The first
// instance now also leaves a compact shared copy in EtsyMail_ListingsSync, tagged with the sync state's lastIncrementalAt, and
// another instance that finds that tag reads those few documents instead.
// Measured against the cost meter's in-memory Firestore (tests/cost/meter.cjs), a fresh "instance" = a fresh load of the module.
//   node tests/cost/ru-S6-listing-search-copy.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path"), Module = require("module");
const ROOT = path.join(__dirname, "../..");
const FN = path.join(ROOT, "netlify/functions");
const meter = require("./meter.cjs");
const { Timestamp } = meter;

const m = meter.create(); m.install();
const realLoad = Module._load;
Module._load = function (req, ...rest) {
  if (/(^|[\\/])_etsyMailEtsy(\.js)?$/.test(req)) return { etsyFetch: async () => { throw new Error("no Etsy in this test"); }, SHOP_ID: "1" };
  if (/(^|[\\/])_etsyApiMeter(\.js)?$/.test(req)) return { bump: () => ({ failNet() {}, fromHttp() {} }), bumpSimple() {}, wrapHandler: fn => fn };
  return realLoad.call(this, req, ...rest);
};
process.env.SHOP_ID = "1";
const quiet = () => { console.log = () => {}; console.warn = () => {}; console.error = () => {}; };
const realLog = console.log, realWarn = console.warn, realErr = console.error;
const loud = () => { console.log = realLog; console.warn = realWarn; console.error = realErr; };

/* ── a shop-sized catalog: 2,300 active listings with real-sized descriptions, 200 inactive ── */
const NOW = Date.now(), ts = ms => Timestamp.fromMillis(ms);
const N_ACTIVE = 2300, N_INACTIVE = 200;
const WORDS = ["anklet", "locket", "sunflower", "zodiac", "initial", "birthstone", "huggie", "hoop", "pendant", "charm", "engraved", "disc", "bracelet", "gold", "silver", "rose"];
const pick = (i, k) => WORDS[(i * 7 + k * 3) % WORDS.length];
const filler = n => "handmade in small batches, shipped from our studio, made to order with care and a gift box included; ".repeat(Math.ceil(n / 100)).slice(0, n);
const listing = (i, active) => ({
  listingId: String(1000000000 + i), title: ("Personalized " + pick(i, 1) + " " + pick(i, 2) + " " + pick(i, 3) + " necklace for her " + i).trim(),
  description: pick(i, 4) + " " + filler(1500 + (i % 7) * 200), tags: Array.from({ length: 13 }, (_, k) => pick(i, k + 5) + (k > 8 ? " gift" : "")),
  priceUsd: 30 + (i % 40), images: [{ url: "https://i.etsystatic.com/x/" + i + ".jpg", altText: "" }], listingUrl: "https://www.etsy.com/listing/" + (1000000000 + i),
  state: active ? "active" : "inactive", quantity: 5, active, lastSyncedAt: ts(NOW - 3600e3 + (i % 50) * 1000)
});
const seed = {
  "EtsyMail_Config/autoPipeline": { listingsMirrorEnabled: true },
  "EtsyMail_ListingsSync/global": { inFlight: false, lastIncrementalAt: ts(NOW - 6 * 3600e3), lastFullSyncAt: ts(NOW - 6 * 3600e3), totalListings: N_ACTIVE }
};
for (let i = 0; i < N_ACTIVE; i++) seed["EtsyMail_Listings/" + (1000000000 + i)] = listing(i, true);
for (let i = 0; i < N_INACTIVE; i++) seed["EtsyMail_Listings/" + (2000000000 + i)] = listing(i + 5000, false);
m.db.seed(seed);
const raw = m._raw;
const doc = p => raw.docs.get(p);

/* the algorithm as it was written (every active listing scored from its stored fields), for the answer to be compared with */
function brute(query, limit) {
  const tokens = String(query).trim().toLowerCase().split(/\s+/).filter(t => t.length >= 2);
  const ids = [...raw.docs.keys()].filter(p => /^EtsyMail_Listings\/\d+$/.test(p)).filter(p => doc(p).active === true).sort();
  const scored = [];
  for (const p of ids) {
    const d = doc(p), title = String(d.title || "").toLowerCase(), desc = String(d.description || "").toLowerCase().slice(0, 500), tags = (d.tags || []).map(t => String(t).toLowerCase());
    let score = 0;
    for (const t of tokens) { if (title.includes(t)) score += 3; if (tags.includes(t)) score += 2; if (desc.includes(t)) score += 1; }
    if (score > 0) scored.push({ score, id: p.split("/")[1], syncedMs: d.lastSyncedAt && d.lastSyncedAt.toMillis ? d.lastSyncedAt.toMillis() : 0 });
  }
  scored.sort((a, b) => (b.score - a.score) || (b.syncedMs - a.syncedMs));
  return { ids: scored.slice(0, Math.max(1, Math.min(limit, 25))).map(s => s.id), total: scored.length };
}

/* a new function instance: the module loaded afresh (its caches are module variables) */
function instance() {
  delete require.cache[require.resolve(path.join(FN, "etsyMailListingsCatalog.js"))];
  return require(path.join(FN, "etsyMailListingsCatalog.js"));
}
async function search(cat, q, limit) {
  const a = m.snapshot(); quiet(); let r;
  try { r = await m.op("search " + q, () => cat.searchListings(q, limit)); } finally { loud(); }
  const d = m.since(a);
  return { r, reads: d.reads + d.aggs, writes: d.writes, bytes: d.bytes };
}
const QUERIES = [["anklet", 8], ["sunflower zodiac", 8], ["birthstone huggie hoop", 10], ["zzzz-no-such-word", 8], ["rose gold engraved", 25]];
const same = (a, q, limit) => { const b = brute(q, limit); assert.deepEqual(a.matches.map(x => x.listingId), b.ids, "the same matches in the same order for " + JSON.stringify(q)); assert.equal(a.totalScored, b.total, "the same count scored for " + JSON.stringify(q)); };
const sharedDocs = () => [...raw.docs.keys()].filter(p => p.startsWith("EtsyMail_ListingsSync/searchCopy"));
let passed = 0;
const ok = name => { passed++; realLog("ok  " + name); };

(async () => {
  /* 1. the first instance reads the catalog as before (it has to), leaves the shared copy, and answers exactly as the algorithm does */
  const A = instance();
  const first = await search(A, "anklet", 8);
  same(first.r, "anklet", 8);
  assert(first.reads >= N_ACTIVE && first.reads <= N_ACTIVE + 30, "the first instance reads the active catalog: " + first.reads);
  const meta = doc("EtsyMail_ListingsSync/searchCopy");
  assert(meta && meta.v === 1 && meta.count === N_ACTIVE && meta.parts >= 1 && meta.parts <= 40, "the shared copy is described: " + JSON.stringify(meta));
  assert.equal(first.writes, meta.parts + 1, "its writes: the parts and the document that names them");
  for (let i = 0; i < meta.parts; i++) { const p = doc("EtsyMail_ListingsSync/searchCopy_" + i); assert(p && p.gen === meta.gen && Buffer.byteLength(p.json, "utf8") < 800000, "part " + i + " is whole and fits a document"); }
  for (const [q, l] of QUERIES.slice(1)) same((await search(A, q, l)).r, q, l);
  ok("first instance: reads the catalog once (" + first.reads + " reads), leaves the shared copy (" + meta.parts + " parts, " + first.writes + " writes), answers exactly as scoring every active listing does");

  /* 2. a new instance (the drafter after an idle spell, the sales agent) uses the shared copy: a handful of reads, the same answers */
  const B = instance();
  const second = await search(B, "anklet", 8);
  same(second.r, "anklet", 8);
  assert.equal(second.writes, 0, "using the copy writes nothing");
  assert(second.reads <= 3 + meta.parts + 8 + 2, "the second instance reads the sync state, the copy's documents and the matches: " + second.reads);
  assert(second.reads * 50 < first.reads, "at least 50 times fewer reads than reading the catalog (" + second.reads + " vs " + first.reads + ")");
  for (const [q, l] of QUERIES.slice(1)) same((await search(B, q, l)).r, q, l);
  const again = await search(B, "anklet", 8);
  assert(again.reads <= 8, "a search within the instance's 30 minutes reads only the matches (" + again.reads + ")");
  ok("second instance: " + second.reads + " reads instead of " + first.reads + " (" + (first.reads / second.reads).toFixed(0) + "x fewer), the same answers, no writes");

  /* 3. a sync completed since: the tag moved, the copy is not used, the pages are read again and a title changed by the sync is found */
  m.db.seed({
    "EtsyMail_ListingsSync/global": Object.assign({}, doc("EtsyMail_ListingsSync/global"), { lastIncrementalAt: ts(NOW - 60e3) }),
    "EtsyMail_Listings/1000000007": Object.assign({}, doc("EtsyMail_Listings/1000000007"), { title: "Brand new moonstone wanderlust necklace" })
  });
  const C = instance();
  const third = await search(C, "wanderlust", 8);
  assert(third.reads >= N_ACTIVE, "a new tag: the catalog is read again (" + third.reads + ")");
  assert.deepEqual(third.r.matches.map(x => x.listingId), ["1000000007"], "the listing the sync changed is found");
  same(third.r, "wanderlust", 8);
  assert.equal(doc("EtsyMail_ListingsSync/searchCopy").tag, doc("EtsyMail_ListingsSync/global").lastIncrementalAt.toMillis(), "the copy is rewritten for the new tag");
  const D = instance();
  const fourth = await search(D, "wanderlust", 8);
  assert(fourth.reads < 40 && fourth.r.matches.length === 1, "...and the next instance uses the new copy (" + fourth.reads + " reads)");
  ok("after a sync the copy is rebuilt once, and the next instance uses it");

  /* 3b. a sync completes while an instance is still reading the pages: its copy is already out of date and is not left as the shared one */
  {
    const tagBefore = doc("EtsyMail_ListingsSync/searchCopy").tag;
    m.db.seed({ "EtsyMail_ListingsSync/global": Object.assign({}, doc("EtsyMail_ListingsSync/global"), { lastIncrementalAt: ts(NOW - 30e3) }) });   // a newer tag: this instance has to read the pages
    const newTag = doc("EtsyMail_ListingsSync/global").lastIncrementalAt.toMillis();
    let moved = false;
    m.queryGuard = info => { if (!moved && info.col === "EtsyMail_Listings") { moved = true; m.db.seed({ "EtsyMail_ListingsSync/global": Object.assign({}, doc("EtsyMail_ListingsSync/global"), { lastIncrementalAt: ts(NOW - 10e3) }) }); } };
    const F = instance();
    const mid = await search(F, "anklet", 8);
    m.queryGuard = null;
    same(mid.r, "anklet", 8);
    assert(moved && mid.reads >= N_ACTIVE && mid.writes === 0, "the tag moved during the read: nothing is written (" + mid.reads + " reads, " + mid.writes + " writes)");
    assert.notEqual(doc("EtsyMail_ListingsSync/searchCopy").tag, doc("EtsyMail_ListingsSync/global").lastIncrementalAt.toMillis(), "no copy is left for a tag that is already out of date");
    assert.equal(doc("EtsyMail_ListingsSync/searchCopy").tag, tagBefore, "the copy that was there is untouched (" + newTag + ")");
    ok("a sync that completes during the read leaves no stale copy as the shared one");
  }

  /* 4. a sync under way: no shared copy is used or written (the pages are read as before) */
  const meta4 = JSON.stringify(doc("EtsyMail_ListingsSync/searchCopy"));
  m.db.seed({ "EtsyMail_ListingsSync/global": Object.assign({}, doc("EtsyMail_ListingsSync/global"), { inFlight: true, inFlightStartedAt: ts(NOW - 30e3) }) });
  const E = instance();
  const fifth = await search(E, "anklet", 8);
  same(fifth.r, "anklet", 8);
  assert(fifth.reads >= N_ACTIVE && fifth.writes === 0, "a sync is running: the pages are read, nothing is written (" + fifth.reads + " reads, " + fifth.writes + " writes)");
  assert.equal(JSON.stringify(doc("EtsyMail_ListingsSync/searchCopy")), meta4, "the shared copy is left as it was");
  // a sync that died and left its lock: the same (the old way), not a day of stale answers
  m.db.seed({ "EtsyMail_ListingsSync/global": Object.assign({}, doc("EtsyMail_ListingsSync/global"), { inFlight: true, inFlightStartedAt: ts(NOW - 3 * 3600e3) }) });
  const E2 = instance();
  const fifthB = await search(E2, "anklet", 8);
  assert(fifthB.reads >= N_ACTIVE && fifthB.writes === 0, "a lock left behind: still the old way (" + fifthB.reads + " reads)");
  m.db.seed({ "EtsyMail_ListingsSync/global": Object.assign({}, doc("EtsyMail_ListingsSync/global"), { inFlight: false, inFlightStartedAt: null }) });
  ok("while a sync is running (or its lock was left behind) the shared copy is neither used nor written");

  /* 5. a damaged copy is never trusted: a part of another generation, bad JSON, a wrong count, too old, no sync state */
  const reset = async () => { const X = instance(); await search(X, "anklet", 8); assert.equal(doc("EtsyMail_ListingsSync/searchCopy").tag, doc("EtsyMail_ListingsSync/global").lastIncrementalAt.toMillis()); };
  const expectFallback = async (label, damage) => {
    await reset();
    // make sure the copy is current, then damage it
    damage();
    const X = instance();
    const r = await search(X, "anklet", 8);
    same(r.r, "anklet", 8);
    assert(r.reads >= N_ACTIVE, label + ": the pages are read instead (" + r.reads + " reads)");
  };
  await expectFallback("a part of another generation", () => m.db.seed({ "EtsyMail_ListingsSync/searchCopy_0": Object.assign({}, doc("EtsyMail_ListingsSync/searchCopy_0"), { gen: "other" }) }));
  await expectFallback("unreadable JSON", () => m.db.seed({ "EtsyMail_ListingsSync/searchCopy_0": Object.assign({}, doc("EtsyMail_ListingsSync/searchCopy_0"), { json: "{not json" }) }));
  await expectFallback("a wrong count", () => m.db.seed({ "EtsyMail_ListingsSync/searchCopy": Object.assign({}, doc("EtsyMail_ListingsSync/searchCopy"), { count: 5 }) }));
  await expectFallback("a missing part", () => raw.docs.delete("EtsyMail_ListingsSync/searchCopy_0"));
  await expectFallback("older than 36 hours", () => m.db.seed({ "EtsyMail_ListingsSync/searchCopy": Object.assign({}, doc("EtsyMail_ListingsSync/searchCopy"), { builtAtMs: Date.now() - 37 * 3600e3 }) }));
  {
    await reset();
    const saved = doc("EtsyMail_ListingsSync/global"); raw.docs.delete("EtsyMail_ListingsSync/global");
    const X = instance(); const r = await search(X, "anklet", 8);
    same(r.r, "anklet", 8);
    assert(r.reads >= N_ACTIVE && r.writes === 0, "no sync state: the pages are read, nothing is written");
    m.db.seed({ "EtsyMail_ListingsSync/global": saved });
  }
  // and a damaged copy is mended by the instance that found it: the next one is cheap again
  await expectFallback("a damaged copy (mended afterwards)", () => m.db.seed({ "EtsyMail_ListingsSync/searchCopy_0": Object.assign({}, doc("EtsyMail_ListingsSync/searchCopy_0"), { gen: "other" }) }));
  const healed = await search(instance(), "anklet", 8);
  assert(healed.reads < 40, "the copy was written again by the instance that found it damaged (" + healed.reads + " reads)");
  ok("a damaged, partial, wrong-count, too-old or unreadable copy (or no sync state) is never used: the pages are read as before, and the copy is mended");

  /* 6. the search switch off still answers 'disabled' without touching the catalog */
  m.db.seed({ "EtsyMail_Config/autoPipeline": { listingsMirrorEnabled: false } });
  const off = await search(instance(), "anklet", 8);
  assert(off.r.error && off.reads <= 2, "mirror disabled: no catalog read (" + off.reads + ")");
  m.db.seed({ "EtsyMail_Config/autoPipeline": { listingsMirrorEnabled: true } });
  ok("listingsMirrorEnabled=false still reads next to nothing");

  const scale = 5762 / N_ACTIVE, partsAt = Math.ceil(meta.parts * scale);
  realLog("\n" + "per-instance rebuild of the search copy".padEnd(52) + "reads".padStart(7) + "KB".padStart(9));
  realLog(("before: every instance, every 30 min of use (" + N_ACTIVE + " listings)").padEnd(52) + String(first.reads).padStart(7) + String(Math.round(first.bytes / 1024)).padStart(9));
  realLog("after: shared copy current".padEnd(52) + String(second.reads).padStart(7) + String(Math.round(second.bytes / 1024)).padStart(9));
  realLog("at the 5,762 active listings of 26 Sep: about 5,774 reads (~14 MB) before, about " + (3 + partsAt + 8) + " reads (~" + Math.round(second.bytes * scale / 1048576) + " MB) after");
  realLog("\n" + passed + " checks passed");
})().catch(e => { loud(); realLog(e && e.stack || e); process.exit(1); });
