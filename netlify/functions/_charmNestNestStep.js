/*  netlify/functions/_charmNestNestStep.js — the run's "nest" step in the cloud driver (cloud plan v2 §1.6 item 6 and §1.7 "nest", build step 6). DORMANT: the driver runs
 *  a step only while Charm_Nest_Open/drive is standby or on, the cloud carries the run, and the step is in drive.cloudSteps; otherwise it parks the run with words.
 *
 *  The port of RunCtl.nestAll and onSheetDone (charm-nest-bridge.js) onto records, with no computer:
 *    1. each sheet of the run owed a nest (Nest.owedNest: pieces placed on it that wait, a nest marked owed, a change since its nest) and with no job at its revisions:
 *       a Rose Gold, 10K or 14K sheet first claims its physical sheet, ONCE, by the driver, before any slot runs (m:P1: roseClaim, idempotent by the sheet that owns
 *       the stock, its unit keyed on the sheet's revisions so a replay is written once); then nestStart (Lib.call, fenced on the driver's lease and the run's carrier):
 *       one job per sheet and revision, made once (P2);
 *    2. the lanes: one lane per run and metal (Nest.pumpLanes): the oldest queued job of a free lane starts (its slot 0), a running job's slots are kept alive and
 *       one cut off is started again from its checkpoint;
 *    3. the step waits ({ wait: "jobs" }) while a job of the run is queued or running, or a job's layout waits for the sheet writer (build step 7a), and says why;
 *       the cursor keeps the jobs' ids, so a Stop press stops them (stopJob, _charmNestDrive.js control);
 *    4. a sheet whose nest a person stopped, or that failed AUTO_ERROR_ATTEMPTS times, is held (run.sheetHolds, as the page's holdSheet): the run goes on with the
 *       others, and the sheet waits for its Nest press (a page's nestStart makes the next attempt; the hold is lifted when a job of the sheet runs again).
 *  A resting run whose sheet changed is poked back to this step by the driver (workSignature: dirty, contentRev), which is the page's dirty-sheet re-nest (bridge onSheetDone).
 *  It never calls Etsy and never builds the world.                                                                                                                  */
"use strict";
const Nest = require("./_charmNestNest");
const CutLine = require("../../charm-nest-rose.js");
const { num, str } = Nest;
const LIVE = new Set(["queued", "running"]);
const FINISH = "Charm_Nest_Finish";   // (the sheet writer's task of each sheet, _charmNestFinish.js: its lastDone names the nest it finished)
/** An answer that passes by itself (the store busy, a time-out, the network): the sheet is asked again at the next pass, never held for a person. */
const PASSING = /ABORTED|DEADLINE|UNAVAILABLE|RESOURCE_EXHAUSTED|contention|timed out|answer in time|ECONNRESET|ETIMEDOUT|socket hang up|network|HTTP 5\d\d/i;
const passing = res => !res || (res.status >= 500 && PASSING.test(String(res.error || "")));

/** What the page's RoseStock.fitHint says of a sheet about to nest (PAIRPARTIAL): the pieces of its smallest order and an average piece, when every order has two or more. */
function fitHint(rec, S) {
  try {
    const list = Nest.activeCharms(rec); if (!list.length) return {};
    const by = new Map(); for (const c of list) { const k = c.order || c.id; by.set(k, (by.get(k) || 0) + 1); }
    const unit = Math.min(...by.values()); if (!(unit >= 1)) return {};   // (LEFTOVERNOROOM: a sheet whose smallest order is one piece asks for room for that one piece too)
    const MM = 25.4 / 72, area = list.reduce((n, c) => n + Nest.inflatedArea(S, c), 0) / list.length * MM * MM, sides = list.map(c => [num(c.widthPt) * MM, num(c.heightPt) * MM]).filter(x => x[0] > 0 && x[1] > 0);
    return { fit: { unit, areaMm2: +area.toFixed(1), ...(sides.length ? { minMm: +(sides.reduce((n, x) => n + Math.min(...x), 0) / sides.length).toFixed(2), maxMm: +(sides.reduce((n, x) => n + Math.max(...x), 0) / sides.length).toFixed(2) } : {}) } };
  } catch (_) { return {}; }
}
/** The page's RoseStock.nestClaim, by the driver, before a cut-line sheet's job is made: Rose Gold always takes its physical sheet (a fresh one under the metal's policy
    'new'); 10K and 14K take one only onto a leftover that fits or when the sheet already holds one, and none under 'new'. → null | { error } */
async function claimStock(ctx, rec, S, policy) {
  if (!CutLine.cuts(rec.metal) || rec.roseCutAt) return null;
  const pol = policy[rec.metal] || { mode: "auto" }, rose = rec.metal === "rose", has = !!(rec.roseStockId || rec.roseProtectedJson || rec.rosePlanJson);
  const never = !(rec.placements || []).length && !rec.roseStockId;
  const stock = rec.roseStockId ? await Nest.roseStockOf(ctx.db, rec) : null, plate = Nest.plateOf(rec, S, stock, policy), st = Nest.stockFor(S, rec.metal, plate);
  let extra;
  if (pol.mode === "new" && never && !has) { if (!rose) return null; extra = { fresh: true }; }
  else if (rose || has) extra = fitHint(rec, S);
  else extra = Object.assign({ onlyRemnant: true }, fitHint(rec, S));
  /* RGLINK (Paul, 23 Nov 2026: the next Rose Gold draft goes on the leftover of the green line just cut, the same plate; at 00:07 a draft opened on a fresh plate): a draft the placement
     opened on a green line's leftover (greenLeftover, charm-nest-place.js greenPage) claims THAT plate, as the page claims the exact partial sheet (RoseStock.nestClaim). Refused (taken
     meanwhile, another size, deleted): the automatic choice, as before. Rose Gold only; Options -> Partial Sheet "new" keeps a fresh plate. */
  const hint = rose && !rec.roseStockId && pol.mode !== "new" && rec.greenLeftover && rec.greenLeftover.stockId ? String(rec.greenLeftover.stockId) : null;
  const skip = Array.isArray(rec.greenNoRoom) && rec.greenNoRoom.length ? { skip: rec.greenNoRoom.map(String).slice(-10) } : {};   // (LEFTOVERNOROOM: the leftovers this sheet found without room)
  const ask = (sid, tag) => ctx.call(Object.assign({ op: "roseClaim", sheetId: rec.id, metal: rec.metal, wPt: st.wPt, hPt: st.hPt, nesting: true }, sid ? { stockId: sid } : {}, extra, skip),
    "nestclaim|" + rec.id + "|" + num(rec.layoutRev) + "|" + num(rec.contentRev) + tag);
  let res = await ask(rec.roseStockId || hint, hint ? "|left" : "");
  if (hint && res && res.status >= 400 && !passing(res)) res = await ask(null, "");
  if (passing(res)) return { again: str((res && res.error) || "no answer", 200) };
  if (res.status >= 400) return { error: str(res.error || "the physical sheet could not be claimed", 300) };
  return null;
}

/** The nest step's one unit. ctx: the driver's (db, run, runId, call, cursor, budgetEnd). */
/** NESTBUSY: the nest unit, then the placement's wait (cursor.placeWait, _charmNestPlaceUnit.js): order lines that wait for a sheet being nested make the step wait here, after the
 *  nest moved along this pass, and send the step back to the placement (cursor.i 0) so those lines go on their sheets once the nest ends. */
async function nestUnit(ctx) {
  const res = await nestCore(ctx), pw = ctx.cursor && ctx.cursor.placeWait;
  if (!pw || (res && (res.stop || res.stopped || res.more || res.goto || res.overBudget))) return res;
  const base = res && res.wait ? res : { wait: "jobs", ids: [], why: "" };
  return Object.assign({}, base, { ids: [...new Set([].concat(base.ids || [], pw.ids || []))].slice(0, 50), why: [base.why, pw.why].filter(Boolean).join(" · "),
    cursor: Object.assign({ jobIds: [] }, (res && res.cursor) || {}, { i: 0, placeWait: null }) });
}
async function nestCore(ctx) {
  const { db, run, runId } = ctx, now = Date.now();
  const [S, policy, sheets] = await Promise.all([Nest.readSettings(db), Nest.readPolicy(db), Nest.sheetsOfRun(db, runId)]);
  const oldHolds = sheets.filter(r => run.sheetHolds && run.sheetHolds[r.id] && !(run.sheetHoldRevs && run.sheetHoldRevs[r.id])).map(r => r.id);   // (HOLDTAKES: an old hold looks at every job of its sheet)
  let jobs = await Nest.jobsOfRun(db, runId, sheets, { sheetIds: oldHolds });   // (READCUT: the live jobs and each sheet's latest, not every nest the run ever asked)
  // (a hold is written onto the run record only when one changes: run.sheetHolds, which the driver saves with the record; a lifted hold is null, the save merges)
  const holds = run.sheetHolds && typeof run.sheetHolds === "object" ? run.sheetHolds : {}, waits = [], held = [];
  /* HOLDTAKES (Paul, 21 Nov 2026): a hold names the revisions it was made at (run.sheetHoldRevs); a sheet that changed since (pieces placed on it, moved onto it, taken off) is
     nested again with its hold lifted, as a person's Nest press would: its nest is a new ask at new revisions, with its own attempts */
  const revs = run.sheetHoldRevs && typeof run.sheetHoldRevs === "object" ? run.sheetHoldRevs : {}, revOf = rec => num(rec.layoutRev) + "/" + num(rec.contentRev);
  const byId = new Map(sheets.map(r => [r.id, r]));
  const hold = (id, why) => { holds[id] = why; run.sheetHolds = holds; revs[id] = why && byId.get(id) ? revOf(byId.get(id)) : null; run.sheetHoldRevs = revs; if (why) held.push(why); };
  let asked = 0;
  for (const rec of sheets) {
    if (Date.now() > ctx.budgetEnd) return { more: true, cursor: { jobIds: jobs.filter(j => LIVE.has(j.status)).map(j => j.id).slice(0, 60) } };
    const mine = jobs.filter(j => j.sheetId === rec.id);
    // (a held sheet that a person nested again: its hold is lifted, as the page's startNestReady lifts runHold)
    if (holds[rec.id] && mine.some(j => LIVE.has(j.status) && !j.auto)) hold(rec.id, null);   // (null, not a deleted key: the driver's record save merges)
    if (holds[rec.id] && revs[rec.id] && revs[rec.id] !== revOf(rec)) hold(rec.id, null);   // (HOLDTAKES: it changed since it was held)
    // (HOLDTAKES: a hold made before holds kept their revisions is lifted once a PERSON's nest of the sheet ended done: a Nest press that finished between two passes lifted nothing)
    if (holds[rec.id] && !revs[rec.id] && mine.some(j => !j.auto && j.status === "done")) hold(rec.id, null);
    // (build step 7b, C4 (d): the sheet writer found that the oldest order cannot fit on an empty sheet (problemStop): the run stops with those words once, as the page's overflowToNextSheet
    //  stopped it; after a person's Resume the sheet is held (the run goes on with the others) until a person nests it again)
    if (rec.problemStop && rec.problemStop.why && !(rec.placements || []).length && !mine.some(j => LIVE.has(j.status))) {
      const why = str(rec.problemStop.why, 300), said = (run.errors || []).some(e => e && e.why === why && num(e.t) >= num(rec.problemStop.at));
      if (!said && run.status === "running") return { stop: { why, fix: "Review the order's size or the fill ceiling, then press Nest on its sheet and Resume", kind: "error" }, cursor: { jobIds: jobs.filter(j => LIVE.has(j.status)).map(j => j.id).slice(0, 60) } };
      if (!holds[rec.id]) hold(rec.id, `${rec.fileBase || rec.metalLabel || rec.id}: ${why}`);
      continue;
    }
    const owed = Nest.owedNest(rec, run, now);
    if (!owed.owed) { if (owed.wait) waits.push(owed.why); continue; }
    const at = mine.filter(j => num(j.layoutBase) === num(rec.layoutRev) && num(j.contentBase) === num(rec.contentRev));
    /* LEFTOVERNOROOM (Paul, 27 Nov 2026, RG Draft 1 of Nov 26: "Why is this RG sheet preview not visible"): its 22 pieces stood on a leftover with no room (the nest ended "no-room", the
       writer had nothing to write: no files, no preview, the card "Updating…" for good, and the nest was never asked again at those revisions). A green-line sheet with NO piece placed,
       in no set, whose nest at these revisions ended with no room on the LEFTOVER it holds, gives that leftover back (roseRelease), remembers it (greenNoRoom: never claimed for this
       sheet again) and claims another plate (roseClaim's room check: a leftover that holds its pieces, else a new sheet), then is nested again on it at once. A sheet with pieces placed
       keeps its plate (what does not fit goes on, as always); a NEW plate is never given back here (an order too big for a whole sheet is the writer's problemStop). */
    if (CutLine.cuts(rec.metal) && !rec.roseCutAt && (rec.draft || !rec.setId) && !(rec.placements || []).length && rec.roseStockId && !mine.some(j => LIVE.has(j.status))
      && at.some(j => j.status === "done" && j.endedBy === "no-room" && (!j.roseStockId || String(j.roseStockId) === String(rec.roseStockId)))) {
      const stock = await Nest.roseStockOf(db, rec);
      if (stock && (num(stock.revision) > 0 || stock.profileJson)) {
        const name = rec.fileBase || rec.metalLabel || rec.id, sid = String(rec.roseStockId);
        const rel = await ctx.call({ op: "roseRelease", stockId: sid, sheetId: rec.id }, "noroom-release|" + rec.id + "|" + sid);
        if (passing(rel)) { waits.push(`${name}: its leftover with no room is given back at the next pass (${str((rel && rel.error) || "no answer", 160)})`); continue; }
        if (rel && rel.status >= 400) { hold(rec.id, `${name}: its leftover has no room for its pieces and could not be given back (${str(rel.error || "refused", 200)})`); continue; }
        const skip = [...new Set([].concat(Array.isArray(rec.greenNoRoom) ? rec.greenNoRoom : [], [sid]).map(String))].slice(-10);
        await ctx.call({ op: "putSheet", sheet: { id: rec.id, greenNoRoom: skip, greenLeftover: null } }, "noroom-mark|" + rec.id + "|" + sid);
        const r2 = Object.assign({}, rec, { roseStockId: null, roseRevision: null, greenLeftover: null, greenNoRoom: skip });
        const claim2 = await claimStock(ctx, r2, S, policy);
        if (claim2 && claim2.again) { waits.push(`${name}: its new plate is claimed at the next pass (${claim2.again})`); continue; }
        if (claim2 && claim2.error) { hold(rec.id, `${name}: ${claim2.error}`); continue; }
        const res2 = await ctx.call({ op: "nestStart", sheetId: rec.id, auto: true, why: `${name}: its leftover had no room for its pieces; nested on another plate` }, "nest|" + rec.id + "|" + num(rec.layoutRev) + "|" + num(rec.contentRev) + "|" + at.map(j => j.id).sort().join(",") + "|replate|" + sid);
        if (passing(res2) || (res2 && res2.status === 409 && res2.retry)) { waits.push(`${name}: its nest on the new plate is asked at the next pass`); continue; }
        if (res2 && res2.held) { hold(rec.id, str(res2.why, 300)); continue; }
        if (!res2 || res2.status >= 400) { hold(rec.id, `${name}: ${str((res2 && res2.error) || "its nest could not be asked", 260)}`); continue; }
        asked++; continue;
      }
    }
    if (at.some(j => LIVE.has(j.status) || j.status === "done")) continue;   // (its job runs, or its layout waits for the writer)
    if (mine.some(j => LIVE.has(j.status))) continue;   // (a job of an older revision still runs: it ends stale or its layout is refused; the next one is asked then)
    const claim = await claimStock(ctx, rec, S, policy);
    if (claim && claim.again) { waits.push(`${rec.fileBase || rec.id}: its physical sheet is claimed at the next pass (${claim.again})`); continue; }
    if (claim && claim.error) { hold(rec.id, `${rec.fileBase || rec.metalLabel || rec.id}: ${claim.error}`); continue; }
    // (the unit's key names the attempts that ended at these revisions: an ask after one ended is a new ask, never the replay of the answer that made the ended one)
    const res = await ctx.call({ op: "nestStart", sheetId: rec.id, auto: true, why: str(owed.why, 160) }, "nest|" + rec.id + "|" + num(rec.layoutRev) + "|" + num(rec.contentRev) + "|" + at.map(j => j.id).sort().join(","));
    if (res && res.status === 409 && res.retry) { waits.push(`${rec.fileBase || rec.id}: it changed while its nest was asked`); continue; }
    if (passing(res)) { waits.push(`${rec.fileBase || rec.id}: its nest is asked again at the next pass (${str((res && res.error) || "no answer", 160)})`); continue; }
    if (res && res.held) { hold(rec.id, str(res.why, 300)); continue; }
    if (!res || res.status >= 400) { hold(rec.id, `${rec.fileBase || rec.metalLabel || rec.id}: ${str((res && res.error) || "its nest could not be asked", 260)}`); continue; }
    asked++;
  }
  if (asked) jobs = await Nest.jobsOfRun(db, runId, sheets, { sheetIds: oldHolds });   // (READCUT: a job just asked is queued: the live read finds it)
  // the lanes: one per run and metal; the oldest queued job starts, a running job's slots are kept alive (a slot cut off starts again from its checkpoint)
  const live = jobs.filter(j => LIVE.has(j.status));
  await Nest.pumpLanes(db, live, { by: "drive" });
  // the layouts made and not yet applied to their sheet (the sheet writer, build step 7a: it applies a job's layout with layoutBase and contentBase and marks nestJob "applied")
  const bySheet = new Map(sheets.map(s => [s.id, s]));
  let unapplied = jobs.filter(j => j.status === "done" && (rec => rec && num(j.layoutBase) === num(rec.layoutRev) && num(j.contentBase) === num(rec.contentRev) && !(rec.nestJob && rec.nestJob.id === j.id && rec.nestJob.status === "applied"))(bySheet.get(j.sheetId)));
  /* NESTSETTLED (Paul, 22 Nov 2026: every sheet stood at "Nesting · step 1 of 5"; the run waited at its nest step from 11:55 on GF_working_d-134110ec148dde3f): nothing marks a
     nest "applied". A nest that changed the layout moves the sheet's layoutRev past its base, so it leaves this list; a nest that changed NOTHING (the writer's shortcut: "the layout
     and its saved files stand as they are") leaves the revisions as they were, and the run waited for it for good. A nest the sheet writer has finished (its task's lastDone names it,
     the very sign its own lane reads, _charmNestFinishLane.js finishedNest) is settled: it is never waited for again. */
  if (unapplied.length) {
    const ids = [...new Set(unapplied.map(j => j.sheetId))], done = new Set();
    for (let i = 0; i < ids.length; i += 100) for (const d of await db.getAll(...ids.slice(i, i + 100).map(id => db.collection(FINISH).doc(id)))) {
      const t = d.exists ? d.data() || {} : null, ld = t && t.lastDone;
      if (ld && ld.nestJobId) done.add(String(ld.nestJobId));
    }
    unapplied = unapplied.filter(j => !done.has(String(j.id)));
  }
  const jobIds = live.concat(unapplied).map(j => j.id).slice(0, 60);
  if (live.length || unapplied.length) {
    const names = list => [...new Set(list.map(j => j.sheetName || j.sheetId))].slice(0, 4).join(", ");
    const why = [live.length ? `Nesting ${live.length} sheet(s) in the cloud: ${names(live)}` : "", unapplied.length ? `${unapplied.length} nested sheet(s) wait for the cloud's sheet writer: ${names(unapplied)}` : "", ...waits.slice(0, 2), held.length ? `${held.length} sheet(s) held: ${held[0]}` : ""].filter(Boolean).join(" · ");
    return { wait: "jobs", ids: jobIds, why, cursor: { jobIds } };
  }
  if (waits.length) return { wait: "jobs", ids: [], why: waits.slice(0, 3).join(" · "), cursor: { jobIds: [] } };
  return held.length ? { cursor: { jobIds: [] }, note: `${held.length} sheet(s) held: ${held[0]}` } : { cursor: { jobIds: [] } };
}

/** The run's "plan" step (RunCtl's doStep "plan", bridge ~16667; cloud plan v2 §1.7: "a saturation note only"): for each metal, what its pieces made and not yet on a sheet need
    of one plate, said in words (over the ceiling: the overflow goes to a second sheet). It reads the pieces' sizes and writes nothing: placement, in the nest step, decides. */
async function planUnit(ctx) {
  const { db, run } = ctx, say = typeof ctx.say === "function" ? ctx.say : () => {};
  const ids = [...new Set(Object.values(run.lines || {}).filter(l => l && l.state === "pooled" && Array.isArray(l.poolIds)).flatMap(l => l.poolIds.map(String)))].slice(0, 3000);
  if (!ids.length) return undefined;
  const S = await Nest.readSettings(db), by = new Map(), MM2 = (25.4 / 72) ** 2, rows = [];
  for (let i = 0; i < ids.length; i += 300) for (const d of await db.getAll(...ids.slice(i, i + 300).map(id => db.collection(Nest.POOL).doc(id)), { fieldMask: ["material", "metal", "sheetId", "state", "sku", "size", "aiPath", "areaPt2", "widthPt", "heightPt"] })) {
    const p = d.exists ? d.data() || {} : null; if (p && (p.material || p.metal) && !p.sheetId && !["abandoned", "superseded"].includes(p.state)) rows.push(p);
  }
  // (a piece's size: its row's, else the library's for the design it is cut from, as the sheet choice reads it: _charmNestPlaceUnit.js geometryOf)
  const { geometryOf } = require("./_charmNestPlaceUnit"), key = r => String(r.sku || "").trim().toUpperCase(), skus = [...new Set(rows.filter(r => !(+r.areaPt2 > 0)).map(key).filter(s => s && !s.includes("/")))], entries = new Map();
  for (let i = 0; i < skus.length; i += 100) for (const s of await db.getAll(...skus.slice(i, i + 100).map(k => db.collection("Charm_Master_Index").doc(k)), { fieldMask: ["sku", "sizes", "aiPath", "widthPt", "heightPt", "areaPt2"] })) if (s.exists) entries.set(s.id, s.data() || {});
  for (const p of rows) { const m = p.material || p.metal, g = geometryOf(entries.get(key(p)), p) || {}, t = by.get(m) || { n: 0, need: 0 }; t.n++; t.need += Nest.inflatedArea(S, g); by.set(m, t); }
  const cap = +S.settings.maxFill > 0 ? +S.settings.maxFill : 0.8;
  for (const [m, t] of by) { const usable = Nest.usableArea(S, { metal: m }), share = usable > 0 ? t.need / usable : 0; say(`[plan] ${m}: ${t.n} piece(s) need ${Math.round(share * 100)}% of the plate (${Math.round(t.need * MM2)} of ${Math.round(usable * MM2)} mm²) — ${share > cap ? `over the ${Math.round(cap * 100)}% ceiling, the overflow goes to a second sheet` : `under the ${Math.round(cap * 100)}% ceiling`}`); }
  return undefined;
}

/** The steps this module gives the driver: { plan, nest: [units] }. (Placement, build step 5, goes before the nest unit in the same step.) */
function steps() { return { plan: [planUnit], nest: [nestUnit] }; }

module.exports = { steps, planUnit, nestUnit, claimStock, fitHint };
