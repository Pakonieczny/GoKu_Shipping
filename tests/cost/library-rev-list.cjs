// COST (Oct 2026): the Library's revision says WHAT moved. The live read (laserStatus with ifRev + ifRevs) cost ONE read when the Library's revision
// (Charm_Nest_Rev/library) had not moved, and one read per document of the answer (~59 for a screen of cards) whenever it had, which was nearly always:
// runPut is in REV_OPS and a nesting run saves every few seconds. The revision now keeps which documents the recent writes touched (changed, at most 200),
// so a reader whose revision moved for somebody else's write is answered { unchanged } for TWO reads; a runPut that changes nothing the Library reads
// moves nothing; a write that does not say what it touched ('*'), a reader older than the list, or a count moved without its list still probe the
// documents (the old cost, never a missed change). The slow check that records seals asks the same way.
// The real charmNestLibrary handler over the cost meter's in-memory Firestore; no network.   node tests/cost/library-rev-list.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const meter = require("./meter.cjs");
const m = meter.create(); m.install();
const realLog = console.log; console.log = () => {}; console.warn = () => {};
delete process.env.EDIT_PASSCODE; process.env.CHARM_NEST_HOLDERS_MS = "0";
const lib = require(path.join(__dirname, "../../netlify/functions/charmNestLibrary.js"));
const db = m.db, sleep = ms => new Promise(r => setTimeout(r, ms));
const call = async body => { const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return JSON.parse(r.body || "{}"); };
const S = "Charm_Nest_Sheets", SET = "Charm_Nest_Sets", RUN = "Charm_Nest_Runs";
const ts = meter.Timestamp.fromMillis(Date.now()), seed = {}, sheetIds = [], setIds = [], runs = {};
// the screen: 12 sets of 4 sheets in view, 3 runs; and one more run (run-other) that is nesting elsewhere
for (let k = 0; k < 12; k++) {
  const setId = `set-${k}`, runId = `run-${k % 3}`, ids = [];
  runs[runId] = runs[runId] || { runId, lines: {}, orders: [] };
  for (let i = 0; i < 4; i++) {
    const id = `sheet-${k}-${i}`, orders = Array.from({ length: 6 }, (_, n) => String(4100000000 + k * 100 + i * 10 + n)), pool = orders.map(o => o + "_1_1");
    orders.forEach((o, n) => { runs[runId].lines[o + "_1"] = { orderId: o, state: "written", quantity: 1, poolIds: [pool[n]], engraveCandidate: false }; runs[runId].orders.push(o); });
    seed[`${S}/${id}`] = { id, setId, setSeq: k + 1, sheetIndex: i + 1, runId, metal: i % 2 ? "silver" : "gold", day: "2026-10-03", status: "complete", placedCount: 6, charmCount: 6, density: 0.7, stock: { wIn: 6, hIn: 4.5 }, poolIds: pool, orders, verification: { ok: true }, outputs: { ai: { path: id + ".ai", url: "u" }, preview: { path: id + ".png", url: "u" } }, label: { files: [{ path: id + "-qr.png", url: "u", payload: "p", orders }] }, updatedAt: ts, createdAt: ts };
    ids.push(id); sheetIds.push(id);
  }
  seed[`${SET}/${setId}`] = { setId, seq: k + 1, day: "2026-10-03", runId, sheetIds: ids, materials: ["gold", "silver"], orders: {}, status: "labelled", updatedAt: ts, createdAt: ts };
  setIds.push(setId);
}
for (const [id, r] of Object.entries(runs)) seed[`${RUN}/${id}`] = r;
seed[`${RUN}/run-other`] = { runId: "run-other", lines: { "4199999999_1": { orderId: "4199999999", state: "pooled", quantity: 1, poolIds: [] } }, orders: ["4199999999"], step: "nest", status: "open" };
db.seed(seed);

(async () => {
  const view = { sheetIds, setIds }, delta = async fn => { const a = m.snapshot(); const out = await fn(); return { out, d: m.since(a) }; };
  const ask = extra => call(Object.assign({ op: "laserStatus", recordSeals: false, wantRevs: true }, view, extra));
  await call({ op: "putSheet", sheet: Object.assign({}, seed[`${S}/sheet-0-0`], { updatedAt: undefined, createdAt: undefined }) });   // (the revision document exists, with its list)
  let full = await ask({}); const keys = Object.keys(full.revs);
  assert(full.rev && /-\d+$/.test(full.rev), "the revision names its count");
  const probe = () => delta(() => ask({ ifRevs: full.revs, ifRev: full.rev }));
  const rows = [];
  const row = (name, x) => { rows.push([name, x.d.reads]); realLog(`  ${name.padEnd(70)} reads ${String(x.d.reads).padStart(3)}   bytes ${String(x.d.bytes).padStart(6)}`); };
  realLog(`\nthe Library's live read, a screen of ${sheetIds.length} sheets in ${setIds.length} sets (the answer names ${keys.length} documents):`);

  // 1. nothing moved: one read, as before
  let x = await probe(); assert.equal(x.out.unchanged, true); assert.equal(x.d.reads, 1); row("idle (the revision has not moved)", x);
  // 2. before: the revision moved for somebody's write = the probe of every document (what verify still does, the safety net)
  x = await delta(() => ask({ ifRevs: full.revs, ifRev: full.rev, verify: true })); assert.equal(x.out.unchanged, true); assert(x.d.reads >= keys.length); row("BEFORE: the revision moved (= the safety-net probe, verify)", x); const before = x.d.reads;
  // 3. a nesting run elsewhere saves its lines: the revision moves, the list says none of ours: TWO reads
  const other = (await db.collection(RUN).doc("run-other").get()).data();
  await call({ op: "runPut", run: Object.assign({}, other, { lines: { "4199999999_1": { orderId: "4199999999", state: "placed", quantity: 1, poolIds: ["4199999999_1_1"] } } }) });
  x = await probe(); assert.equal(x.out.unchanged, true, "another run's save is not ours"); assert.equal(x.out.via, "list"); assert.equal(x.d.reads, 2); row("AFTER: another run saved its lines (the list says: none of ours)", x);
  const after = x.d.reads; full.rev = x.out.rev;
  // 4. a run of OURS saves its step and status alone: the revision does not move at all
  const r0 = (await db.collection(RUN).doc("run-0").get()).data(); const revDoc = () => db.collection("Charm_Nest_Rev").doc("library").get().then(s => s.data().n);
  await call({ op: "runPut", run: r0 }); x = await probe(); full = x.out.unchanged ? Object.assign(full, { rev: x.out.rev }) : x.out;   // (its first save moves its lines out to live parts: a change of record, once)
  let n0 = await revDoc(); await call({ op: "runPut", run: Object.assign({}, r0, { step: "label", status: "open", touchedAt: Date.now() }) });
  assert.equal(await revDoc(), n0, "a runPut that changes nothing the Library reads moves no revision");
  x = await probe(); assert.equal(x.out.unchanged, true); assert.equal(x.d.reads, 1); row("our run saved its step / status alone (no revision moved)", x);
  // ... the same run with a line changed: ours, a full answer
  await sleep(3); await call({ op: "runPut", run: Object.assign({}, r0, { lines: Object.assign({}, r0.lines, { [Object.keys(r0.lines)[0]]: Object.assign({}, Object.values(r0.lines)[0], { state: "gone" }) }) }) });
  assert.equal(await revDoc(), n0 + 1, "a runPut that changes a line moves it");
  x = await probe(); assert(!x.out.unchanged && x.out.sheets, "our run's line changed: answered in full"); full = x.out;
  // 5. a sheet of ours saved (by anyone): a full answer; a sheet not on screen: two reads
  await sleep(3); await call({ op: "putSheet", sheet: { id: "sheet-elsewhere", metal: "gold", day: "2026-10-03", poolIds: [], orders: [] } });
  x = await probe(); assert.equal(x.out.unchanged, true); assert.equal(x.d.reads, 2); full.rev = x.out.rev;
  await sleep(3); await call({ op: "putSheet", sheet: Object.assign({}, seed[`${S}/sheet-5-2`], { updatedAt: undefined, createdAt: undefined, density: 0.8 }) });
  x = await probe(); assert(!x.out.unchanged && x.out.sheets.find(s => s.id === "sheet-5-2").density === 0.8, "our sheet saved elsewhere: answered in full"); full = x.out;
  // 6. a Complete Order of a piece that is not ours: two reads; a set of ours: a full answer
  await sleep(3); await call({ op: "customPut", key: "4199999999_1", receiptId: "4199999999", transactionId: "1", sku: "X", how: "button", by: "Paul" });
  x = await probe(); assert.equal(x.out.unchanged, true); assert.equal(x.d.reads, 2); row("someone's Complete Order of a piece not on screen", x); full.rev = x.out.rev;
  await sleep(3); await call({ op: "setUpdate", setId: "set-7", patch: { note: "x" } });
  x = await probe(); assert(!x.out.unchanged, "our set changed: answered in full"); full = x.out;
  // 7. a write that does not say what it touched ('*'): the documents are probed (the old cost), never a miss
  await sleep(3); await call({ op: "noDesignPut", sku: "NOPE-1", by: "Paul" });
  x = await probe(); assert.equal(x.out.unchanged, true); assert.equal(x.out.via, "docs"); assert(x.d.reads >= keys.length); row("a write that does not name what it touched ('*'): the probe as before", x); full.rev = x.out.rev;
  // 8. an older instance's plain +1 (no list): the documents are probed; the next named write starts the list again
  const older = full.rev;
  await sleep(3); await db.collection("Charm_Nest_Rev").doc("library").set({ n: meter.FieldValue.increment(1), at: Date.now() }, { merge: true });
  x = await probe(); assert.equal(x.out.via, "docs"); full.rev = x.out.rev;
  await sleep(3); await call({ op: "putSheet", sheet: { id: "sheet-elsewhere", metal: "gold", day: "2026-10-03", poolIds: [], orders: [] } });
  x = await delta(() => ask({ ifRevs: full.revs, ifRev: older })); assert.equal(x.out.via, "docs", "a reader from before the unlisted write cannot be told by the list");
  x = await probe(); assert.equal(x.out.via, "list", "a reader that probed after it can"); assert.equal(x.d.reads, 2); full.rev = x.out.rev;
  // 9. the list holds 200: a reader older than the oldest it dropped probes the documents
  const stale = full.rev;
  for (let i = 0; i < 205; i++) await call({ op: "setUpdate", setId: `set-far-${i}`, patch: { note: "x" } });
  const list = (await db.collection("Charm_Nest_Rev").doc("library").get()).data();
  assert(Object.keys(list.changed).length <= 200 && list.floor > 0, "the list is bounded");
  x = await delta(() => ask({ ifRevs: full.revs, ifRev: stale })); assert.equal(x.out.via, "docs", "older than the list: the documents");
  // 10. a change in one of ours is never missed, whichever way it was written: a '*' write, a named write
  await sleep(3); full = await ask({}); await sleep(3); await db.collection(S).doc("sheet-3-1").update({ verification: { ok: false } }); await call({ op: "noDesignDelete", sku: "NOPE-1", by: "Paul" });
  assert(!(await ask({ ifRevs: full.revs, ifRev: full.rev })).unchanged, "a '*' write: the documents show our change");

  // the slow check that records seals: before, a moved revision meant the whole pass; now the list first
  realLog(`\nthe slow check (recordSeals), the same screen:`);
  const seal = extra => call(Object.assign({ op: "laserStatus", recordSeals: true, wantRev: true, by: "T" }, view, extra));
  await seal({}); let s2 = await seal({}); full = await ask({});
  x = await delta(() => seal({ ifRev: s2.rev })); assert.equal(x.out.unchanged, true); assert.equal(x.d.reads, 1); row("idle (the revision has not moved)", x);
  await sleep(3); await call({ op: "customPut", key: "4199999999_1", receiptId: "4199999999", transactionId: "1", sku: "X", how: "print", by: "Paul" });
  x = await delta(() => seal({ ifRev: s2.rev })); assert(!x.out.unchanged); row("BEFORE: the revision moved (the whole pass)", x); const sealBefore = x.d.reads; s2 = x.out;
  await sleep(3); await call({ op: "customPut", key: "4199999999_1", receiptId: "4199999999", transactionId: "1", sku: "X", how: "print", by: "Paul" });
  x = await delta(() => seal({ ifRev: s2.rev, ifRevs: full.revs })); assert.equal(x.out.unchanged, true); assert.equal(x.out.via, "list"); assert.deepEqual(x.out.added, []); assert.equal(x.d.reads, 2); row("AFTER: moved for somebody else's write (the list says: none of ours)", x);
  await sleep(3); await call({ op: "putSheet", sheet: Object.assign({}, seed[`${S}/sheet-2-0`], { updatedAt: undefined, createdAt: undefined, density: 0.9 }) });
  x = await delta(() => seal({ ifRev: s2.rev, ifRevs: full.revs })); assert(!x.out.unchanged && Array.isArray(x.out.added), "one of ours changed: the whole pass");
  realLog(`\nper hour, a Library shown at a 3 s beat while the revision keeps moving for a run nesting elsewhere: before ${before * 1200} reads, after ${after * 1200}; the slow check each minute: before ${sealBefore * 60}, after ${2 * 60}`);
  realLog("library-rev-list: all checks passed");
  m.uninstall(); process.exit(0);
})().catch(e => { realLog(e); process.exit(1); });
