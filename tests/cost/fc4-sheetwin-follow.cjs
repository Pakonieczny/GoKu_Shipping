// FC4 (Firebase cost): the sheet window's beat. An open sheet window asked laserStatus every 2 s with ifRevs = every document the full answer was made
// of (its set, every other sheet of the set, the runs ...), to learn whether ONE sheet record moved. It now asks after that record alone.
// The real charmNestLibrary handler over the cost meter's in-memory Firestore; no network.   node tests/cost/fc4-sheetwin-follow.cjs
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
const meter = require("./meter.cjs");
const m = meter.create(); m.install();
const realLog = console.log; console.log = () => {}; console.warn = () => {};
delete process.env.EDIT_PASSCODE;
const lib = require(path.join(__dirname, "../../netlify/functions/charmNestLibrary.js"));
const db = m.db, sleep = ms => new Promise(r => setTimeout(r, ms));
const call = async body => { const r = await lib.handler({ httpMethod: "POST", headers: {}, body: JSON.stringify(body) }); return JSON.parse(r.body || "{}"); };
const S = "Charm_Nest_Sheets", SET = "Charm_Nest_Sets", RUN = "Charm_Nest_Runs";
const at = Date.now(), ts = meter.Timestamp.fromMillis(at), seed = {};
const sheets = 12, setId = "set-1", runId = "run-1", ids = [], run = { runId, lines: {} };
for (let i = 0; i < sheets; i++) {
  const id = `sheet-${i}`, orders = Array.from({ length: 25 }, (_, n) => String(4100000000 + i * 30 + n)), pool = orders.map(o => o + "_1_1");
  orders.forEach((o, n) => { run.lines[o + "_1"] = { orderId: o, state: "written", quantity: 1, poolIds: [pool[n]], engraveCandidate: false }; });
  seed[`${S}/${id}`] = { id, setId, setSeq: 1, sheetIndex: i + 1, runId, metal: "gold", day: "2026-10-03", status: "complete", placedCount: 25, charmCount: 25, density: 0.7, stock: { wIn: 6, hIn: 4.5 }, poolIds: pool, orders, verification: { ok: true }, outputs: { ai: { path: id + ".ai", url: "u" }, preview: { path: id + ".png", url: "u" } }, label: { files: [{ path: id + "-qr.png", url: "u", payload: "p", orders }] }, placements: "p".repeat(120000), updatedAt: ts, createdAt: ts };
  ids.push(id);
}
seed[`${RUN}/${runId}`] = run;
seed[`${SET}/${setId}`] = { setId, seq: 1, day: "2026-10-03", runId, sheetIds: ids, materials: ["gold"], orders: {}, status: "labelled", updatedAt: ts, createdAt: ts };
db.seed(seed);

(async () => {
  const id = "sheet-3", key = "s:" + id, ask = extra => m.op("follow", () => call(Object.assign({ op: "laserStatus", sheetIds: [id], setIds: [setId], recordSeals: false, wantRevs: true }, extra)));
  const delta = async fn => { const a = m.snapshot(); const out = await fn(); return { out, d: m.since(a) }; };
  const full = await delta(() => ask());
  assert(full.out.sheets && full.out.sheets.some(x => x.id === id) && full.out.revs[key], "the full answer holds the sheet and names its revision");
  const keys = Object.keys(full.out.revs);
  // before: every revision of the full answer is sent back
  const oldProbe = await delta(() => ask({ ifRevs: full.out.revs }));
  assert.equal(oldProbe.out.unchanged, true);
  // after: the sheet's own revision alone, no set asked
  const newProbe = await delta(() => ask({ ifRevs: { [key]: full.out.revs[key] }, setIds: [] }));
  assert.equal(newProbe.out.unchanged, true, "the one-record probe answers unchanged");
  const per = d => meter.perHour(d, 1800);
  const row = (n, x) => realLog(`  ${n.padEnd(40)} reads ${String(x.d.reads + x.d.aggs).padStart(3)}   bytes ${String(x.d.bytes).padStart(6)}   per hour at 2 s: reads ${String(per(x.d).reads).padStart(6)}  USD ${per(x.d).usd.toFixed(4)}`);
  realLog(`\none beat of an open sheet window (a set of ${sheets} sheets, 1 run; the answer names ${keys.length} documents):`);
  row("before (every revision sent back)", oldProbe); row("after (the sheet record alone)", newProbe);
  assert.equal(newProbe.d.reads, 1); assert(oldProbe.d.reads >= keys.length, "the old probe read every document it was sent");
  // a change in THIS sheet record makes the one-record probe answer in full
  const base = full.out.revs[key]; await sleep(4);
  await db.collection(S).doc(id).update({ poolIds: ["x_1_1"], orders: ["4100000003"] });
  const moved = await ask({ ifRevs: { [key]: base }, setIds: [] });
  assert(!moved.unchanged && moved.sheets.some(x => x.id === id) && moved.revs[key] !== base, "this sheet changed: answered in full, with its new revision");
  assert.deepEqual(moved.sheets.find(x => x.id === id).poolIds, ["x_1_1"]);
  // a change in the set or a neighbour sheet does not move the probe (the window shows this sheet's membership alone: memberSig)
  await sleep(4); await db.collection(SET).doc(setId).update({ status: "again" }); await db.collection(S).doc("sheet-4").update({ verification: { ok: false } });
  assert.equal((await ask({ ifRevs: { [key]: moved.revs[key] }, setIds: [] })).unchanged, true);
  // the sheet deleted: the probe sees it (the revision of a missing document is "0")
  await sleep(4); await db.collection(S).doc(id).delete();
  const gone = await ask({ ifRevs: { [key]: moved.revs[key] }, setIds: [] });
  assert(!gone.unchanged, "a sheet deleted is a change");
  realLog("fc4-sheetwin-follow: all checks passed");
  m.uninstall();
})().catch(e => { realLog(e); process.exit(1); });
