// FC4 (Firebase cost): the order window's timeline poll. An open order window asked the whole timeline every 2.5 s (1,440 calls an hour).
// This test measures one call before (whole answer) and after (the change probe: ifRev -> { unchanged }), and proves the probe cannot miss a
// change in any document the answer is made of. Own in-memory Firestore (tests/cost/meter.cjs); no network, no real service.
// COST (Oct 2026): the probe itself read ~36 documents. The cheap answer now comes from ONE document, Order_Rev/{orderId}, which every
// writer of the order's timeline raises; the full digest (verify, or once the net of 60 s has passed) still sees every document.
//   node tests/cost/fc4-timeline-probe.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const meter = require("./meter.cjs");
const m = meter.create(); m.install();
const Timeline = require(path.join(__dirname, "../../netlify/functions/_orderTimeline.js"));
const db = m.db, sleep = ms => new Promise(r => setTimeout(r, ms));
const O = "4200000001", NOW = Date.now(), pad = (n, c = "x") => c.repeat(n);
const seed = {};
// recorded events: 40, about 700 bytes each (text, data, names)
for (let i = 0; i < 40; i++) seed[`Order_Timeline/${O}~${i % 3 ? "scan" : "note"}~k${i}`] = { orderId: O, type: i % 3 ? "scan" : "note", at: NOW - (60 - i) * 60e3, by: "Tess", source: "station", station: "sorting", device: "sorting-1", lineKey: `${O}_90`, transactionId: "90", sheetId: "", sheet: "", setId: "", text: "Seen at Sorting " + pad(60), data: { note: pad(300) }, milestone: false, createdAt: meter.Timestamp.fromMillis(NOW - (60 - i) * 60e3) };
// 5 pieces; their rows carry fields the derivation does not read (placement notes)
for (let i = 0; i < 5; i++) seed[`Charm_Pool/${O}_9${i}_1`] = { poolId: `${O}_9${i}_1`, orderId: O, transactionId: "9" + i, lineKey: `${O}_9${i}`, setId: "set-1", sheetId: "sheet-A", sheetName: "GF_Sep.28.26_Set-1_Sheet-1", sku: "CH-1", material: "gold", state: "written", createdAt: meter.Timestamp.fromMillis(NOW - 3600e3), updatedAt: meter.Timestamp.fromMillis(NOW - 3000e3), svg: pad(6000, "s"), outline: pad(3000, "o") };
// 2 sheets with the heavy fields a sheet record carries (placements, previews)
for (const sid of ["sheet-A", "sheet-B"]) seed[`Charm_Nest_Sheets/${sid}`] = { id: sid, metal: "gold", sheetIndex: sid === "sheet-A" ? 1 : 2, orders: [O], poolIds: [`${O}_90_1`, `${O}_91_1`], setId: "set-1", createdAt: meter.Timestamp.fromMillis(NOW - 3600e3), updatedAt: meter.Timestamp.fromMillis(NOW - 3000e3), placements: pad(150000, "p"), outputs: { preview: { url: "https://x/y.png" } } };
seed["Charm_Nest_Sets/set-1"] = { setId: "set-1", seq: 1, name: "Set 1", sheetIds: ["sheet-A", "sheet-B"], committedAt: meter.Timestamp.fromMillis(NOW - 1000e3), committed: [O], orders: { [O]: { held: false } } };
for (let i = 0; i < 12; i++) seed[`Brites_Orders/${O}/messages/m${i}`] = { text: "Please check the clasp " + pad(80), timestamp: meter.Timestamp.fromMillis(NOW - (30 - i) * 60e3), senderName: "Ana", senderRole: "design" };
seed[`Charm_Custom_Orders/${O}_90`] = { key: `${O}_90`, receiptId: O, transactionId: "90", sku: "CUS-1", kind: "custom", stamps: [{ how: "print", at: NOW - 900e3, by: "Tess" }], prints: 1, notes: pad(2000) };
seed[`Charm_Nest_CustomRead/${O}_90`] = { order: O, reads: { r1: { at: NOW - 5000e3, kind: "custom", confidence: 0.9, summary: pad(1500) } }, latest: "r1" };
seed[`Charm_Nest_Arrivals/${O}`] = { firstSeenAt: NOW - 7200e3, createTs: NOW - 7300e3, junk: pad(800) };
seed[`Design_Order_Archive/${O}`] = { completedAtMs: NOW - 900e3, completedBy: "Ana", setId: "set-1", sheetIds: ["sheet-A"], status: { createdTs: NOW - 7300e3 }, big: pad(8000) };
seed[`EtsyMail_Receipts/${O}`] = { created_timestamp: Math.round((NOW - 7300e3) / 1000), updated_timestamp: Math.round((NOW - 100e3) / 1000), status: "paid", is_shipped: false, raw: { shipments: [], transactions: pad(12000) } };
for (let i = 0; i < 5; i++) seed[`Charm_Pool_Back/${O}_9${i}_1`] = { poolId: `${O}_9${i}_1`, sheetId: "sheet-A", setId: "set-1", approvedAt: NOW - 800e3, approvedBy: "Ana", text: "Love you", preview: pad(5000, "b") };
// a cancel's step on a sheet (cancelSteps writes it, and later rewrites it IN PLACE under the same key as the step goes on: RV2)
seed[`Order_Timeline/${O}~cancelStep~cx-1700000000000-GF_Sheet_1`] = { orderId: O, type: "cancelStep", at: NOW - 600e3, by: "Tess", source: "sorter", station: "sorter", sheet: "GF Sheet 1", text: "On a cut sheet: set aside (GF Sheet 1)", data: { outcome: "setAside", done: false, sheet: "GF Sheet 1" }, createdAt: meter.Timestamp.fromMillis(NOW - 600e3) };
db.seed(seed);

const run = (name, opts) => m.op(name, () => Timeline.get(db, O, Object.assign({ prefix: "", sandboxed: Timeline.SANDBOXED_DEFAULT }, opts)));
const delta = async (name, opts) => { const a = m.snapshot(); const out = await run(name, opts); return { out, d: m.since(a) }; };
// (the full digest: what a poke (verify) or the 60 s net asks; the one-read answer is checked on its own below)
const mut = async (label, fn, expect = true) => {
  await sleep(4);   // (the in-memory update time has millisecond resolution)
  const before = (await run("probe", { wantRev: true })).rev;
  await fn(); await sleep(4);
  const after = await run("probe", { ifRev: before, verify: true });
  if (expect) { assert(!after.unchanged, label + ": the probe must see this change"); assert(after.rev && after.rev.split(".")[0] !== before.split(".")[0], label + ": a new revision"); }
  else assert(after.unchanged === true, label + ": a rewrite in place is the known blind spot (the page's full read, at least once a minute, covers it)");
  console.log(`  ${expect ? "seen " : "blind"}  ${label}`);
};

(async () => {
  // ── before: what every poll cost ──
  const before = await delta("get (before: whole answer every poll)", {});
  assert(before.out.events.length >= 40);
  // ── after: the first read keeps a revision; the next polls send it back ──
  const first = await delta("get (first read, asks wantRev)", { wantRev: true });
  assert(Timeline.REV_TOKEN.test(first.out.rev) && first.out.rev.split(".").length === 3, "a revision comes with the answer (digest.orderRev.probedAt): " + first.out.rev);
  assert.deepEqual(JSON.parse(JSON.stringify(Object.assign({}, first.out, { rev: undefined, now: 0 }))), JSON.parse(JSON.stringify(Object.assign({}, before.out, { now: 0 }))), "the answer itself is the same with or without wantRev");
  const full = await delta("get (probe: the full digest)", { ifRev: first.out.rev, verify: true });
  assert.equal(full.out.unchanged, true);
  const probe = await delta("get (after: Order_Rev, unchanged)", { ifRev: full.out.rev });
  assert.equal(probe.out.unchanged, true); assert.equal(probe.out.rev, full.out.rev);
  assert.equal(probe.out.events, undefined, "an unchanged answer carries no events");
  assert.equal(probe.d.reads + probe.d.aggs, 1, "an unchanged poll is ONE read (Order_Rev): " + (probe.d.reads + probe.d.aggs));
  const per = d => meter.perHour(d, 1440);
  console.log(`\none poll of an order window (40 recorded events, 5 pieces, 2 sheets, 1 set, 12 messages):`);
  const row = (n, x) => console.log(`  ${n.padEnd(34)} reads ${String(x.d.reads + x.d.aggs).padStart(4)}   bytes ${String(x.d.bytes).padStart(8)}   per hour at 2.5 s: reads ${String(per(x.d).reads).padStart(7)}  MB ${(per(x.d).bytes / 1e6).toFixed(1).padStart(6)}  USD ${per(x.d).usd.toFixed(4)}`);
  row("before (whole answer)", before); row("first read (answer + digest)", first); row("probe (full digest, Oct 7)", full); row("after (Order_Rev, unchanged)", probe);
  assert(full.d.reads + full.d.aggs > 20, "the full digest is the dear one (the net, at most once a minute)");
  assert(probe.d.bytes < before.d.bytes / 50, "the probe carries about nothing");
  assert(probe.d.reads + probe.d.aggs < before.d.reads + before.d.aggs, "and reads fewer documents");

  console.log("\nthe probe sees a change in every document the answer is made of:");
  await mut("a recorded event added", () => db.collection("Order_Timeline").doc(`${O}~shipped~new`).set({ orderId: O, type: "note", at: Date.now(), text: "x" }));
  await mut("a recorded event deleted (a sandbox reset)", () => db.collection("Order_Timeline").doc(`${O}~scan~k1`).delete());
  await mut("a piece's row changed", () => db.collection("Charm_Pool").doc(`${O}_90_1`).update({ state: "abandoned" }));
  await mut("a piece added to the order", () => db.collection("Charm_Pool").doc(`${O}_95_1`).set({ poolId: `${O}_95_1`, orderId: O, transactionId: "95", state: "pooled" }));
  await mut("a sheet record rewritten", () => db.collection("Charm_Nest_Sheets").doc("sheet-A").update({ poolIds: [`${O}_90_1`] }));
  await mut("the order put on another sheet", () => db.collection("Charm_Nest_Sheets").doc("sheet-C").set({ id: "sheet-C", metal: "silver", orders: [O], poolIds: [] }));
  await mut("a set committed (its record)", () => db.collection("Charm_Nest_Sets").doc("set-1").update({ committedAt: meter.Timestamp.fromMillis(Date.now()) }));
  await mut("a Team message added", () => db.collection("Brites_Orders").doc(O).collection("messages").doc("m99").set({ text: "DESIGNED :)", timestamp: meter.Timestamp.fromMillis(Date.now()), senderName: "Ana" }));
  await mut("a custom order pressed again", () => db.collection("Charm_Custom_Orders").doc(`${O}_90`).update({ completedAt: Date.now() }));
  await mut("a custom reading (found by its query)", () => db.collection("Charm_Nest_CustomRead").doc(`${O}_90`).update({ decided: { kind: "custom", at: Date.now() } }));
  await mut("a custom reading (found by a line's key only)", () => db.collection("Charm_Nest_CustomRead").doc(`${O}_91`).set({ order: "other", reads: {} }));
  await mut("a back engraving approved", () => db.collection("Charm_Pool_Back").doc(`${O}_92_1`).update({ approvedAt: Date.now() }));
  await mut("the order cancelled", () => db.collection("Charm_Nest_Cancelled").doc(O).set({ orderId: O, by: "Paul", at: Date.now(), why: "buyer asked" }));
  await mut("the arrival record", () => db.collection("Charm_Nest_Arrivals").doc(O).update({ seenAt: Date.now() }));
  await mut("the design archive", () => db.collection("Design_Order_Archive").doc(O).update({ completedBy: "Bo" }));
  await mut("the design station's plain completion", () => db.collection("Design_Completed Orders").doc(O).set({ completedAt: Date.now() }));
  await mut("the Etsy mirror's receipt", () => db.collection("EtsyMail_Receipts").doc(O).update({ status: "completed" }));
  await mut("a Rose Gold sheet's cut", async () => { await db.collection("Charm_Nest_Sheets").doc("sheet-R").set({ id: "sheet-R", metal: "rose", orders: [O], rosePlanHash: "h", roseCutAt: Date.now(), roseStockId: "stock-1", poolIds: [] }); }, true);
  await mut("the cut's own record (who cut it)", () => db.collection("Charm_Nest_Rose_Stock").doc("stock-1").collection("cuts").doc("sheet-R").set({ at: Date.now(), by: "Tess" }));
  await mut("another order's records (must NOT move this order)", async () => { await db.collection("Order_Timeline").doc("999~note~z").set({ orderId: "999", type: "note", at: Date.now() }); await db.collection("Charm_Pool").doc("999_1_1").set({ poolId: "999_1_1", orderId: "999" }); await db.collection("Charm_Nest_Sheets").doc("sheet-Z").set({ id: "sheet-Z", orders: ["999"] }); }, false);
  await mut("a cancel's step rewritten in place (same key: set aside, then done)", () => db.collection("Order_Timeline").doc(`${O}~cancelStep~cx-1700000000000-GF_Sheet_1`).set({ at: Date.now(), text: "On a cut sheet: set aside by Tess (GF Sheet 1)", data: { outcome: "setAside", done: true } }, { merge: true }));
  await mut("a recorded event rewritten in place", () => db.collection("Order_Timeline").doc(`${O}~scan~k2`).set({ text: "changed" }, { merge: true }), false);

  // ── the one-read answer (COST, Oct 2026): every writer of the timeline moves Order_Rev, so a change shows at the next poll; a record no writer
  //    raises it for waits for the net (60 s) or a poke (verify), never longer ──
  console.log("\nthe one-read answer (Order_Rev) sees what the timeline's writers wrote:");
  const FV = meter.FieldValue;
  const cheap = async (label, fn, expect = true) => {
    await sleep(4);
    const held = (await run("probe", { wantRev: true })).rev;
    assert.equal((await run("probe", { ifRev: held })).unchanged, true, label + ": quiet before");
    await fn(); await sleep(4);
    const a = m.snapshot(), after = await run("probe", { ifRev: held }), d = m.since(a);
    if (expect) assert(!after.unchanged && after.events, label + ": seen at the next poll, without verify");
    else { assert.equal(after.unchanged, true, label + ": not raised, so not seen before the net"); assert.equal(d.reads + d.aggs, 1, "and that cost one read"); }
    console.log(`  ${expect ? "seen " : "net  "}  ${label}`);
    return held;
  };
  await cheap("an event added (Timeline.add)", () => Timeline.add(db, FV, [{ orderId: O, type: "note", text: "hello", id: "cheap-1" }], { prefix: "" }));
  await cheap("the same event rewritten in place (Timeline.add, same key: the digest's blind spot)", () => Timeline.add(db, FV, [{ orderId: O, type: "note", text: "hello again", id: "cheap-1" }], { prefix: "" }));
  await cheap("a raise on its own (Timeline.touchNow: a Team message)", () => Timeline.touchNow(db, FV, [O], ""));
  const held = await cheap("a piece's row written directly (nothing raises Order_Rev)", () => db.collection("Charm_Pool").doc(`${O}_90_1`).update({ state: "written" }), false);
  // the net: a revision whose digest is older than 60 s takes the full digest again, and sees it
  const realNow = Date.now; Date.now = () => realNow() + 61000;
  try { const late = await run("probe", { ifRev: held }); assert(!late.unchanged && late.events, "past the net the full digest sees what nothing raised"); }
  finally { Date.now = realNow; }
  // a window unused for long may let the one-read answer stand up to 10 minutes (netMs), never longer, never under 60 s
  const h2 = (await run("probe", { wantRev: true })).rev;
  Date.now = () => realNow() + 61000;
  try {
    assert.equal((await run("probe", { ifRev: h2, netMs: 5 * 60000 })).unchanged, true, "netMs 5 min: still the one-read answer at 61 s");
    assert.notEqual((await run("probe", { ifRev: h2, netMs: 1000 })).rev, h2, "netMs under 60 s is 60 s: the digest is taken again");
    Date.now = () => realNow() + 11 * 60000;
    const a = m.snapshot(); await run("probe", { ifRev: h2, netMs: 60 * 60000 }); assert(m.since(a).reads > 5, "netMs is capped at 10 minutes");
  } finally { Date.now = realNow; }
  // another order's events do not move this order's revision
  const h3 = (await run("probe", { wantRev: true })).rev;
  await Timeline.add(db, FV, [{ orderId: "999", type: "note", text: "x", id: "other" }], { prefix: "" });
  assert.equal((await run("probe", { ifRev: h3 })).unchanged, true, "another order's event leaves this one's revision");
  console.log("  ok     the net (60 s; netMs up to 10 min) and other orders");

  // sandbox: the same order's sandbox records are separate, and the Etsy mirror is not read
  const sbSeed = { [`Sandbox_Order_Timeline/${O}~note~a`]: { orderId: O, type: "note", at: NOW, text: "s" } }; db.seed(sbSeed);
  const sb = await m.op("sb", () => Timeline.get(db, O, { prefix: "Sandbox_", sandboxed: Timeline.SANDBOXED_DEFAULT, wantRev: true }));
  assert(Timeline.REV_TOKEN.test(sb.rev)); const sb2 = await Timeline.get(db, O, { prefix: "Sandbox_", sandboxed: Timeline.SANDBOXED_DEFAULT, ifRev: sb.rev }); assert.equal(sb2.unchanged, true);
  const real = await Timeline.get(db, O, { prefix: "", sandboxed: Timeline.SANDBOXED_DEFAULT, ifRev: sb.rev }); assert(!real.unchanged, "a sandbox revision never matches the real order's");
  // a digest that is not one (a stale page, a bad value) reads in full
  const bad = await Timeline.get(db, O, { prefix: "", sandboxed: Timeline.SANDBOXED_DEFAULT, ifRev: "000000000000" }); assert(!bad.unchanged && bad.events.length);
  // a sandbox event moves the sandbox's Order_Rev only
  const pr = (await run("probe", { wantRev: true })).rev;
  await Timeline.add(db, meter.FieldValue, [{ orderId: O, type: "note", text: "sb", id: "sb-1" }], { prefix: "Sandbox_" });
  assert.equal((await run("probe", { ifRev: pr })).unchanged, true, "a sandbox event leaves production's revision");
  assert(db.docs.get(`Sandbox_Order_Rev/${O}`), "the sandbox raises Sandbox_Order_Rev");
  console.log("\nfc4-timeline-probe: all checks passed");
  m.uninstall();
})().catch(e => { console.error(e); process.exit(1); });
