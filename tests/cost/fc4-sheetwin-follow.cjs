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
  // COST (Oct 2026): the window's beat follows the person (charm-nest-sheetwin.js followGap, read from the page's own source): 2 s while used and focused,
  // about 15 s otherwise, about 60 s after 10 minutes untouched; at the window's 2 s tick, the reads an hour of a window left open
  {
    const src = require("node:fs").readFileSync(path.join(__dirname, "../../charm-nest-sheetwin.js"), "utf8"), from = src.indexOf("const followGap = () => {"), to = src.indexOf("\n  };", from) + 4;
    assert(from > 0 && to > from, "followGap is in the sheet window");
    let now = 1.8e12, focus = true; const FOLLOW = { touch: now, moved: now - 3600000 };
    const ctx = require("node:vm").createContext({ Date: { now: () => now }, document: { hasFocus: () => focus }, FOLLOW });
    const gap = require("node:vm").runInContext(src.slice(from, to) + "\nfollowGap", ctx);
    const reads = mins => { let at = -Infinity, n = 0; for (let t = 0; t < mins * 60000; t += 2000) { now = FOLLOW.t0 + t; if (now - at >= gap()) { at = now; n++; } } return n; };
    FOLLOW.t0 = now; assert.equal(gap(), 0, "used, focused: every beat (2 s)");
    focus = false; assert(gap() >= 13000 && gap() < 15000, "not focused: about 15 s"); focus = true;
    now += 121000; assert(gap() >= 13000 && gap() < 15000, "untouched for 2 minutes: about 15 s");
    now += 600000; assert(gap() >= 58000 && gap() < 60000, "untouched for 10 minutes: about 60 s");
    FOLLOW.moved = now; assert(gap() < 15000, "the sheet moved: back to about 15 s");
    FOLLOW.touch = now; assert.equal(gap(), 0, "a touch: every beat again");
    FOLLOW.touch = FOLLOW.moved = FOLLOW.t0 = now = 1.9e12 - 3600000;
    realLog(`  a sheet window open for an hour, used for its first 2 minutes, then left: ${reads(60)} reads (was 1,800 at a 2 s beat)`);
    assert(reads(60) < 150, "an hour of a window nobody uses reads far less than the 1,800 of a 2 s beat");
  }
  realLog("fc4-sheetwin-follow: all checks passed");
  m.uninstall();
})().catch(e => { realLog(e); process.exit(1); });
