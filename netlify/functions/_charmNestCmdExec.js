/*  netlify/functions/_charmNestCmdExec.js — WHAT EACH REQUEST DOES IN THE CLOUD (cloud plan v2, build step 13). One executor per type of CMD_TYPES (charmNestLibrary.js), the
 *  cloud's port of charm-nest-run-follow.js execCmd: the same answers in the same words, made from the records (sheets, sets, pool rows, run lines, the world's lines), never from
 *  a page's memory, never from Etsy (no table is asked for, no interpretAll: a reading comes from the world build, E4).
 *
 *  An executor is async (x) -> { done: { ok, text, freed? } } | { wait: { text, pct?, work } }, x = { ctx, cmd, a: cmd.args, work, now }. A request that needs more than one pass
 *  (a nest, a reading of the orders, a placement) keeps its stage in `work` (Charm_Nest_Cmd_Work/<cmdId>, written by the commands unit) and is asked again next pass. Each one is
 *  answered: done, or refused in words, or, past its own time limit, answered with what it did and what is left (a request never waits silently).
 *
 *    record only   reorder · delete · leaveset · setrebuild · unkeep · repicture
 *    pieces        placewaiting · pairnow · sendhome · remakeline · keepsync · remake · skuonce · release
 *    nest          renest · consolidate · empty · seat   (consolidate / empty: 10K / 14K sheets, and Gold Filled / Sterling Silver drafts of the run: COMBINEDRAFTS)
 *    pieces        customsend (CUSTOMSEND, 21 Nov 2026: a card's sent designs made into pieces by the cloud's intake and placed by its placement)
 *    nest          restorenest (RGRESTORE, 22 Nov 2026: a nest's verified layout put back on its sheet; its cut set aside first, the draft its pieces went to emptied and removed)
 *
 *  What it needs of the other steps, by op name (an op that is not the cloud's in this build makes the request answer in words, never wait): nestStart (build step 6),
 *  placeOnSheet (step 5), releaseAsk (Release Hold in the cloud), roseClaim (step 6), roseRelease, runLinesRetire (step 10a). The lanes a request needs must be in
 *  Charm_Nest_Open/drive.cloudSteps: "nest" for anything that places or nests, "pool" as well for a piece made again, "files" for a picture.                              */
"use strict";
const OpClass = require("./_charmNestOpClass");
const SheetName = require("../../charm-nest-sheet-name.js");
const SetEdit = require("../../charm-nest-set-edit.js");
let SetRepair = null; try { SetRepair = require("../../charm-nest-set-repair.js"); } catch (_) { SetRepair = null; }
let SetRules = null; try { SetRules = require("../../charm-nest-set-rules.js"); } catch (_) { SetRules = null; }
const CutLine = require("../../charm-nest-rose.js");
const { busyRec, WORK } = require("./_charmNestCmdOps");

const SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets", POOL = "Charm_Pool", WORLD_LINES = "Charm_World_Lines", MASTER = "Charm_Master_Index", STOCK = "Charm_Nest_Rose_Stock", INTAKE = "Charm_Nest_Intake_Lines";
const SOLID = new Set(["gold10k", "gold14k"]); const FAST = new Set(["gold", "silver"]);   // (COMBINEDRAFTS: Gold Filled / Sterling Silver drafts are combined and emptied too)
const OPEN_LINE = l => !!l && !["gone", "committed", "skipped"].includes(String(l.state || ""));
const NEST_MS = 25 * 60000, PLACE_MS = 15 * 60000, WORLD_MS = 15 * 60000, REMADE_MS = 25 * 60000, FILES_MS = 20 * 60000, RELEASE_PLACE_MS = 20 * 60000;
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const ms = v => (v && v.toMillis ? v.toMillis() : +v || 0);
const plural = (n, one, more) => `${n} ${n === 1 ? one : more || one + "s"}`;
const and = l => (l.length < 2 ? l.join("") : `${l.slice(0, -1).join(", ")} and ${l[l.length - 1]}`);
const nameOf = r => { try { return (SheetName && SheetName.name && SheetName.name(r)) || r.fileBase || r.id; } catch (_) { return (r && (r.fileBase || r.id)) || "the sheet"; } };
const cutRec = r => !!r && (!!r.roseCutAt || num(r.laserDoneAt) > 0);
const lineDrawn = r => !!r && !!(r.rosePlanJson || r.roseProtectedJson);
const ridOf = k => (/^(\d{1,30})_/.exec(String(k || "")) || [])[1] || "";
const activeCharms = r => (r && Array.isArray(r.charms) ? r.charms : []).filter(c => c && !c.excluded);
const ok = text => ({ done: { ok: true, text: str(text, 400) || "Done" } });
const no = text => ({ done: { ok: false, text: str(text, 400) || "Not done" } });
const wait = (text, work, pct) => ({ wait: Object.assign({ text: str(text, 160), work: work || {} }, pct == null ? {} : { pct }) });

/* ── reading the records ── */
const has = op => OpClass.isCloudOp(op);
const lane = (ctx, l) => (ctx.cloudSteps || []).includes(l);
async function runSheets(ctx, metal) {
  let q = ctx.db.collection(SHEETS).where("runId", "==", ctx.runId); if (metal) q = q.where("metal", "==", metal);
  const s = await q.get(); return s.docs.map(d => Object.assign({ id: d.id }, d.data())).filter(r => !r.archived);
}
async function sheetRec(ctx, id) { if (!id) return null; const s = await ctx.db.collection(SHEETS).doc(String(id)).get(); return s.exists ? Object.assign({ id: s.id }, s.data()) : null; }
async function runSets(ctx) { const s = await ctx.db.collection(SETS).where("runId", "==", ctx.runId).get(); return s.docs.map(d => Object.assign({ setId: d.id, id: d.id }, d.data())); }
async function setRec(ctx, id) { if (!id) return null; const s = await ctx.db.collection(SETS).doc(String(id)).get(); return s.exists ? Object.assign({ setId: s.id, id: s.id }, s.data()) : null; }
async function pools(ctx, ids) {
  const out = new Map(), list = [...new Set((ids || []).map(String))];
  for (let i = 0; i < list.length; i += 100) for (const s of await ctx.db.getAll(...list.slice(i, i + 100).map(id => ctx.db.collection(POOL).doc(id)))) if (s.exists) out.set(s.id, s.data());
  return out;
}
async function worldLine(ctx, key) { const s = await ctx.db.collection(WORLD_LINES).doc(String(key)).get(); if (!s.exists) return null; const d = s.data() || {}; if (d.gone) return { key, gone: true }; try { return Object.assign(JSON.parse(d.data || "{}"), { key, at: num(d.at) }); } catch (_) { return null; } }
async function worldAt(ctx) { const s = await ctx.db.collection("Charm_World").doc("summary").get(); return s.exists ? ms((s.data() || {}).at) : 0; }
const lines = ctx => (ctx.run && ctx.run.lines) || {};
/** Where each piece is: the sheet whose record lists it (Placement's rule: the record decides). → Map poolId -> sheet */
function listedBy(sheets) { const m = new Map(); for (const r of sheets) for (const id of r.poolIds || []) m.set(String(id), r); return m; }
/** Whether a piece a sheet lists is seated (in its placements or feedWait) — the C4 rule. */
function seated(r, poolId) {
  const c = (r.charms || []).find(x => x && String(x.poolId) === String(poolId)); if (!c) return false;
  return (r.placements || []).some(p => p && p.id === c.id) || (Array.isArray(r.feedWait) && r.feedWait.includes(c.id));
}
const committedSheet = (r, sets) => { const s = sets.find(x => x.setId === r.setId || (x.sheetIds || []).includes(r.id)); return !!(s && (SetEdit.committedSet(s) || s.processReady)); };
const inAnySet = (r, sets) => !!(r && ((r.setId && !r.draft) || sets.some(s => (s.sheetIds || []).includes(r.id))));
const orderOf = r => (Number.isFinite(+r.draftOrder) ? +r.draftOrder : num(r.page));
const inOrder = list => list.slice().sort((x, y) => (orderOf(x) - orderOf(y)) || (ms(x.createdAt) - ms(y.createdAt)) || (x.id < y.id ? -1 : 1));
/** The cloud's own write, with a unit key of its own (the mid: a replay writes once). */
const put = (x, body, key) => x.ctx.call(body, `${x.ctx.runId}|cmd|${x.cmd.id}|${key}`);
const err = r => (r && r.status >= 400 ? str(r.error || "refused", 300) : "");

/* ── a sheet's nest, waited for: started by nestStart (append, or fresh), settled when its record has a newer layout, no nest owed, nothing running, and (with the cloud's files
   lane on) its files written. → { settled, timedOut, rec } ── */
async function startNest(x, sheetId, fresh, why) {
  if (!has("nestStart")) return { started: false, queued: true };   // (the nest step's own unit starts every sheet that owes a nest: nestOwed)
  const r = await put(x, { op: "nestStart", runId: x.ctx.runId, sheetId, fresh: !!fresh, why: str(why, 120) }, `nest|${sheetId}|${fresh ? "fresh" : "append"}|${x.work.nestAsk || 0}`);
  if (err(r)) return { started: false, error: err(r) };
  return { started: true, jobId: r.jobId || null };
}
async function nestSettled(x, w) {
  const rec = await sheetRec(x.ctx, w.id), now = Date.now();
  if (!rec || rec.archived) return { settled: true, rec: null, gone: true };
  const files = lane(x.ctx, "files"), moved = num(rec.layoutRev) > num(w.layoutRev);
  const settled = moved && !busyRec(rec, now) && !rec.nestOwed && (!files || !rec.filesOwed);
  return { settled, rec, timedOut: !settled && now - num(w.at) > NEST_MS };
}
/* FIRSTNOW (Paul, 22 Nov 2026: "No waiting, this must always take priority"; a Back on its sheet was refused twice in a minute: "GF Draft 21 is having its files written: try again",
   then "is nesting: try again"): a person's request never gives up because a sheet it must change is busy. A live nest of that sheet is stopped (a person's stop, once a job, as Move all
   onto does: the nest is made again with the request's change), files being written are waited for, and the request goes on the moment the sheet is still: the driver looks at it again
   between units, every few seconds. */
const liveNest = r => !!(r && r.nestJob && ["queued", "running", "starting"].includes(String(r.nestJob.status || "")));
async function stopFor(x, recs, why) {
  for (const r of recs || []) { const id = r && r.nestJob && (r.nestJob.id || r.nestJob.jobId); if (id && liveNest(r)) await put(x, { op: "stopJob", id: String(id), for: "person", by: x.cmd.by || "", why: str(why, 120) }, `firstnow|stop|${id}`); }
}
const FIRST_MS = 15 * 60000;
const placedWords = rec => { const all = activeCharms(rec).length, n = (rec.placements || []).length, v = rec.verification; return `${n} of ${all} placed${v ? (v.ok ? ", verified" : ", verification flagged") : ""}`; };

/* ═══ record only ═══ */
async function reorder(x) {
  const m = x.a.metal; if (!SOLID.has(m)) return no("Sheets are arranged by hand for 10K and 14K Gold only.");
  const sets = await runSets(x.ctx), pages = inOrder(await runSheets(x.ctx, m)), have = pages.map(p => p.id);
  const want = [...new Set((x.a.ids || []).filter(id => have.includes(id)))]; have.forEach((id, i) => { if (!want.includes(id)) want.splice(Math.min(i, want.length), 0, id); });
  const movable = p => !cutRec(p) && !committedSheet(p, sets);
  const byId = new Map(pages.map(p => [p.id, p])), free = want.map(id => byId.get(id)).filter(movable), seats = pages.map((p, i) => (movable(p) ? i : -1)).filter(i => i >= 0);
  const placed = pages.slice(); seats.forEach((at, k) => { placed[at] = free[k]; });
  const values = seats.map(i => +pages[i].page).filter(Number.isFinite).sort((a, b) => a - b), usable = values.length === seats.length && new Set(values).size === values.length, val = k => (usable ? values[k] : k + 1);
  let n = 0;
  for (const [k, at] of seats.entries()) {
    const p = placed[at]; if (p.draftOrder === val(k) && p.page === val(k)) continue;
    const r = await put(x, { op: "putSheet", sheet: { id: p.id, draftOrder: val(k), page: val(k) } }, `order|${p.id}|${val(k)}`); if (err(r)) return no(`The order was not changed: ${err(r)}`);
    p.draftOrder = val(k); p.page = val(k); n++;
  }
  return ok(n ? "Sheet order changed: " + placed.filter(movable).map(nameOf).join(", ") : "The sheets were already in that order");
}
async function del(x) {
  const p = await sheetRec(x.ctx, x.a.sheetId); if (!p || p.runId !== x.ctx.runId) return no("That sheet is not on the run the cloud carries.");
  const sets = await runSets(x.ctx), name = nameOf(p), now = Date.now();
  if (activeCharms(p).length) return no(`Move its ${plural(activeCharms(p).length, "piece")} onto another sheet first`);
  if (cutRec(p)) return no(`${name} was already cut`);
  if (inAnySet(p, sets)) return no(`${name} is in a set`);
  if (lineDrawn(p)) return no(`${name} has a cut line drawn`);
  const busy = busyRec(p, now); if (busy) return no(`${name} ${busy}`);
  let stockNote = "";
  if (p.roseStockId && CutLine.cuts(p.metal) && p.metal !== "rose" && !num(p.roseRevision)) {
    if (has("roseRelease")) { const r = await put(x, { op: "roseRelease", stockId: p.roseStockId, sheetId: p.id, metal: p.metal }, `rose|${p.id}`); if (err(r)) stockNote = `; its physical sheet stays reserved (${err(r)})`; }
    else stockNote = "; its physical sheet stays reserved until it is let go in the Partial sheets list";
  }
  const r = await put(x, { op: "archiveEmptySheet", id: p.id, runId: x.ctx.runId }, `archive|${p.id}`); if (err(r)) return no(`${name} could not be removed: ${err(r)}`);
  // the later sheets of the metal move up one place (the browser's removePage), each draft named again from its place
  const left = inOrder(await runSheets(x.ctx, p.metal)).filter(q => q.id !== p.id && !cutRec(q) && !committedSheet(q, sets) && num(q.page) > num(p.page));
  for (const q of left) { const pg = Math.max(1, num(q.page) - 1); await put(x, { op: "putSheet", sheet: { id: q.id, page: pg, draftOrder: pg } }, `order|${q.id}|${pg}`); }
  return ok(`${name} deleted (it was empty)${stockNote}`);
}
async function leaveset(x) {
  const p = await sheetRec(x.ctx, x.a.sheetId); if (!p || p.runId !== x.ctx.runId) return no("That sheet is not on the run the cloud carries.");
  const setId = SetEdit.inSetOf(p); if (!setId) return no(`${nameOf(p)} is not in a set.`);
  const was = nameOf(p), r = await put(x, { op: "cmdLeaveSet", runId: x.ctx.runId, setId, sheetIds: [p.id], by: x.cmd.by || "" }, `leave|${p.id}|${setId}`);
  if (err(r)) return no(err(r));
  return ok(`${was} was taken out of its set; it is a draft now and waits for the next set`);
}
async function setrebuild(x) {
  if (!SetRepair || typeof SetRepair.plan !== "function") return no("The set rules are not loaded in the cloud: nothing was changed");
  const set = await setRec(x.ctx, x.a.setId); if (!set) return no("That set could not be found: refresh the Library");
  if (set.runId && set.runId !== x.ctx.runId) return no(`${SetEdit.setWord(set)} is made by a run the cloud does not carry: nothing was changed`);
  const sets = await runSets(x.ctx), sheets = await runSheets(x.ctx), byId = new Map(sheets.map(r => [r.id, r]));
  for (const id of set.sheetIds || []) if (!byId.has(id)) { const r = await sheetRec(x.ctx, id); if (r && !r.archived) byId.set(id, r); }
  const now = Date.now(), run = x.ctx.run || {};
  const p = SetRepair.plan(set, [...byId.values()], sets, { cloud: true, runHere: true, committing: !!run.commitRequested, busy: r => busyRec(r, now) });
  if (!p.ok) return p.noop ? ok(p.refused.reason) : no(p.refused ? p.refused.reason : "The set was not rebuilt");
  if (!p.expel.length) return ok((p.words && p.words.summary) || "The set already follows the rules");
  const family = p.expel.filter(id => { const r = byId.get(id); const k = SetRules && r ? SetRules.metalClass(r) : ""; return ["RG", "10K", "14K"].includes(k); });
  const r = await put(x, { op: "cmdLeaveSet", runId: x.ctx.runId, setId: set.setId, sheetIds: p.expel, whole: true, mark: family, by: x.cmd.by || "" }, `rebuild|${set.setId}|${p.signature || p.expel.join(",")}`);
  if (err(r)) return no(`The set was not rebuilt: ${err(r)}`);
  return ok((p.words && p.words.summary) || `${SetEdit.setWord(set)} was rebuilt: ${plural(p.expel.length, "sheet")} left it; the next sets form by the usual rules`);
}
async function unkeep(x) {
  const want = new Set((x.a.ids || x.a.orders || []).map(v => String(v).replace(/\D/g, "")).filter(Boolean)); if (!want.size) return no("No order was named.");
  const run = x.ctx.run || {}, was = (run.keepShipped || []).map(String), letGo = was.filter(v => want.has(v));
  if (letGo.length) { const r = await put(x, { op: "runPut", run: { runId: x.ctx.runId, keepShipped: was.filter(v => !want.has(v)) }, merge: true }, `unkeep|${[...want].sort().join(",")}`); if (err(r)) return no(`The orders were not let go: ${err(r)}`); run.keepShipped = was.filter(v => !want.has(v)); }
  // which of them are shipped at our stations or cancelled: our own records (never Etsy). The take-off lane takes their pieces off the open sheets.
  let gone = 0;
  try {
    const sh = await x.ctx.read({ op: "shippedByUs", orders: [...want] }), cc = await x.ctx.read({ op: "cancelCheck", orderIds: [...want] });
    const shipped = sh && sh.shipped, cancelled = cc && (cc.cancelled || cc.orders || cc);
    for (const id of want) { const s = Array.isArray(shipped) ? shipped.map(String).includes(id) : !!(shipped && shipped[id]); const c = Array.isArray(cancelled) ? cancelled.map(String).includes(id) : !!(cancelled && typeof cancelled === "object" && cancelled[id]); if (s || c) gone++; }
  } catch (_) { /* said as not known */ }
  const off = lane(x.ctx, "takeoff") ? "the cloud takes their pieces off every sheet that is still open" : "their pieces come off every sheet that is still open at the next take-off";
  return ok(`${letGo.length} shipped order(s) let go; ${gone} of them were shipped at our stations or cancelled: ${off}`);
}
async function repicture(x) {
  if (!lane(x.ctx, "files")) return no("A sheet's picture is drawn by the cloud's sheet writer, which is not on yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (!x.work.stage) {
    const p = await sheetRec(x.ctx, x.a.sheetId); if (!p || p.archived) return no("That sheet is not in the cloud.");
    if (cutRec(p)) return no(`${nameOf(p)} was cut: its picture stays as it was cut`);
    if (num(p.previewRev) >= num(p.layoutRev) && p.preview) return ok(`${nameOf(p)}'s picture already shows the sheet as it is now`);
    const at = Date.now(), r = await put(x, { op: "putSheet", sheet: { id: p.id, filesOwed: at } }, `picture|${p.id}|${num(p.layoutRev)}`); if (err(r)) return no(err(r));
    return wait("Drawing the sheet's picture again…", { stage: "files", id: p.id, at, layoutRev: num(p.layoutRev) });
  }
  const rec = await sheetRec(x.ctx, x.work.id); if (!rec) return no("That sheet is no longer in the cloud.");
  if (!rec.filesOwed && num(rec.previewRev) >= num(rec.layoutRev)) return ok(`${nameOf(rec)}: its picture was drawn again as the sheet is now`);
  if (Date.now() - num(x.work.at) > FILES_MS) return no(`${nameOf(rec)}: its files are still owed; the cloud writes them when the sheet is free`);
  return wait("Drawing the sheet's picture again…", x.work);
}

/* ═══ pieces ═══ */
/** STRANDED (C4): the run's lines counted as pooled whose pieces stand on no sheet, put right on the records so the placement places them: a piece record that names a sheet
 *  which does not list it lets that sheet go; a piece a sheet lists but never seated (a reject the overflow did not move on) makes that sheet owe a nest again; a line refused
 *  because another run claimed it ("contended") is asked for again. Never a piece a sheet seats, one that is nesting, held, gone or cancelled. → { keys, pieces, writes }
 *  o.tried (the stranded sweep's own journal, _charmNestSweeps.js): a line it tried less than 30 minutes ago waits. A person's request passes no journal, so every line is looked at
 *  now, as the page's placeStranded did; no guard above is ever skipped for it (cloud plan v2 E1: the cloud passes no force). */
async function strandedFix(x, o = {}) {
  const ctx = x.ctx, L = lines(ctx), now = Date.now(), tried = o.tried || {}, out = { keys: [], pieces: 0, writes: 0, contended: [], resat: [] };
  const sheets = await runSheets(ctx), on = listedBy(sheets);
  const cand = Object.entries(L).filter(([k, l]) => OPEN_LINE(l) && !l.hold && !l.vacated && ["pooled", "written"].includes(l.state) && (l.poolIds || []).length && now - num(tried[k]) > 30 * 60000);
  const stranded = [], resit = new Map();
  for (const [k, l] of cand) {
    let any = false;
    for (const id of l.poolIds) {
      const s = on.get(String(id));
      if (!s) { any = true; continue; }
      if (seated(s, id) || busyRec(s, now) || s.nestOwed || s.inplaceHold) continue;
      if (cutRec(s)) continue;   // (a cut sheet keeps what it holds)
      any = true; resit.set(s.id, s);
    }
    if (any) stranded.push([k, l]);
  }
  if (stranded.length) {
    const cc = await ctx.read({ op: "cancelCheck", orderIds: [...new Set(stranded.map(([k]) => ridOf(k)))] }).catch(() => null), cancelled = cc && (cc.cancelled || cc.orders);
    const isCancelled = rid => (Array.isArray(cancelled) ? cancelled.map(String).includes(rid) : !!(cancelled && typeof cancelled === "object" && cancelled[rid]));
    const loose = [], toPooled = {};
    const rows = await pools(ctx, stranded.flatMap(([, l]) => l.poolIds));
    for (const [k, l] of stranded) {
      if (isCancelled(ridOf(k))) continue;
      out.keys.push(k); out.pieces += l.poolIds.length;
      for (const id of l.poolIds) { const r = rows.get(String(id)); if (r && r.sheetId && !on.has(String(id)) && !["abandoned", "superseded", "committed"].includes(r.state)) loose.push(String(id)); }
      if (l.state === "written") toPooled[k] = { state: "pooled" };
    }
    for (let i = 0; i < loose.length; i += 300) { const part = loose.slice(i, i + 300), r = await put(x, { op: "poolUpdate", poolIds: part, patch: { sheetId: null } }, `strand|loose|${part[0]}|${Math.floor(now / 120000)}`); if (!err(r)) out.writes++; }
    if (Object.keys(toPooled).length) { const r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: toPooled }, `strand|lines|${Object.keys(toPooled).sort().join(",").slice(0, 400)}|${Math.floor(now / 120000)}`); if (!err(r)) out.writes++; }
    // (a piece a sheet lists but never seated, the sheet at rest: its turned-away list is cleared and the sheet marked changed, so its nest tries it again (the nest step's owedNest);
    //  what still does not fit is moved on by the writer's overflow, step 7b. A cut sheet keeps what it holds)
    for (const s of resit.values()) { const r = await put(x, { op: "putSheet", sheet: { id: s.id, rejects: [], dirty: true } }, `strand|resit|${s.id}|${num(s.contentRev)}|${num(s.layoutRev)}`); if (!err(r)) { out.writes++; out.resat.push(s.id); } }
  }
  // CONTENDEDBACK: a line another run had claimed is asked for again (the server answers afresh)
  const again = {}; for (const [k, l] of Object.entries(L)) if (l && l.state === "contended" && !l.hold) { again[k] = { state: "pulled", reason: null, wait: null }; out.contended.push(k); }
  if (Object.keys(again).length) { const r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: again }, `strand|contended|${Object.keys(again).sort().join(",").slice(0, 400)}`); if (!err(r)) out.writes++; }
  return out;
}
/** How many of these lines' pieces a sheet seats now. → { pieces, placed, names, waiting:[reason], open } */
async function placedNow(ctx, keys) {
  const run = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), L = (run && run.run && run.run.lines) || lines(ctx);
  const sheets = await runSheets(ctx), on = listedBy(sheets), out = { pieces: 0, placed: 0, names: new Set(), waiting: [], open: 0 };
  for (const k of keys) {
    const l = L[k]; if (!l || !OPEN_LINE(l)) continue;
    if (["pulled", "pooled", "waiting", "contended"].includes(l.state) && !(l.poolIds || []).length) { out.open++; if (l.reason) out.waiting.push(String(l.reason)); continue; }
    for (const id of l.poolIds || []) { out.pieces++; const s = on.get(String(id)); if (s && seated(s, id)) { out.placed++; out.names.add(nameOf(s)); } else out.open++; }
    if (l.state === "waiting" && l.reason) out.waiting.push(String(l.reason));
  }
  out.names = [...out.names]; return out;
}
async function placewaiting(x) {
  if (!lane(x.ctx, "nest")) return no("Placing pieces is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if ((x.a.ids || []).length) return customPlace(x);   // (CESEND: a custom piece a viewer sent to a sheet)
  if (!x.work.stage) {
    const fix = await strandedFix(x);   // (a person's request: no journal of earlier tries, so nothing waits out the sweep's 30 minutes)
    if (!fix.keys.length && !fix.contended.length) return ok("No piece stands outside a sheet.");
    return wait("Placing the pieces that stand on no sheet…", { stage: "placing", keys: fix.keys.concat(fix.contended).slice(0, 300), pieces: fix.pieces, lines: fix.keys.length + fix.contended.length, at: Date.now() });
  }
  const v = await placedNow(x.ctx, x.work.keys || []);
  if (!v.open || Date.now() - num(x.work.at) > PLACE_MS) return ok(v.pieces ? `${v.placed} of ${Math.max(v.pieces, x.work.pieces || 0)} stranded piece(s) (${x.work.lines} order line(s)) went onto a sheet${v.open && v.waiting.length ? " · " + v.waiting[0] : ""}` : "No piece stands outside a sheet.");
  return wait(`Placing the pieces that stand on no sheet… ${v.placed} of ${v.pieces || "?"} placed`, x.work, v.pieces ? Math.round(100 * v.placed / v.pieces) : null);
}
/* CESEND (21 Nov 2026: the Charm Editor's Send to sheet on a staff computer in mode "on": "Replace on the order…", another order, no order, a custom line). The page wrote the piece's rows
   (customPiecePut, cloudPlace, this run) and asks: placewaiting { ids: the new pool ids, and "off:<pool id>" for the piece it replaces }. The cloud takes the replaced piece off its sheet
   (cloudTakeOff sheetOff, that one piece: a cut sheet keeps it, and says so), then places the new piece by the placement unit's own rules (only those rows), and answers in words where
   it went, or why not. The nest step nests the sheet that took it (nestOwed). */
async function customPlace(x) {
  const ctx = x.ctx, all = (x.a.ids || []).map(String), off = (all.find(s => s.startsWith("off:")) || "").slice(4), ids = all.filter(s => !s.startsWith("off:"));
  if (!ids.length) return no("No piece was named");
  const w = x.work;
  if (!w.stage) {
    const rows = await pools(ctx, ids);
    const bad = ids.filter(id => { const r = rows.get(id); return !r || !r.custom || r.cloudPlace !== true || r.runId !== ctx.runId; });
    if (bad.length) return no(`The piece ${bad[0]} is not a custom piece sent for the run the cloud carries: nothing was placed`);
    w.stage = "off"; w.at = Date.now(); w.offText = "";
  }
  if (w.stage === "off") {
    if (off) {
      const r = (await pools(ctx, [off])).get(off), sheets = await runSheets(ctx), on = listedBy(sheets).get(off), sid = on ? on.id : (r && r.sheetId) || null, rid = ridOf(off) || String((r && r.orderId) || "");
      if (!r) w.offText = `the piece it replaces (${off}) is not in the cloud`;
      else if (!lane(ctx, "takeoff") || !has("cloudTakeOff")) w.offText = `the piece it replaces stays on ${on ? nameOf(on) : "its sheet"} (the cloud's take-off is not on): take it off in the sheet window`;
      else {
        const t = await put(x, { op: "cloudTakeOff", orderId: rid, sheetId: sid, ids: [off], kind: "sheetOff", at: Date.now(), who: x.cmd.by || "someone", note: "Replaced: made again from the Charm Editor (Send to sheet)" }, `ceoff|${off}`);
        if (err(t)) { if (t && t.waiting && Date.now() - num(w.at) < PLACE_MS) return wait(`Taking the replaced piece off ${on ? nameOf(on) : "its sheet"}… ${err(t)}`, w); w.offText = `the piece it replaces stays on ${on ? nameOf(on) : "its sheet"}: ${err(t)}`; }
        else if (t && (t.kept || []).length) w.offText = `the piece it replaces stays on ${on ? nameOf(on) : "its sheet"} (${t.keptWhy || "kept"})`;
        else w.offText = `the piece it replaces was taken off ${on ? nameOf(on) : "the pool"}`;
      }
    }
    w.stage = "place";
  }
  const res = await require("./_charmNestPlaceUnit").placeAll(Object.assign({}, ctx, { customIds: ids }));
  const on = listedBy(await runSheets(ctx)), names = [...new Set(ids.map(id => on.get(id)).filter(Boolean).map(nameOf))], n = ids.filter(id => on.has(id)).length;
  const tail = w.offText ? ` · ${w.offText}` : "";
  if (n === ids.length) return ok(`${plural(n, "custom piece")} placed on ${and(names)} (the cloud nests it there)${tail}`);
  if (Date.now() - num(w.at) > PLACE_MS) return n ? ok(`${n} of ${ids.length} custom pieces placed on ${and(names)}; the rest is not placed: ${res.why || "no sheet took it"}${tail}`) : no(`Not placed: ${res.why || "no sheet took it"}${tail}`);
  return wait(`Placing the custom piece… ${res.why || ""}`.trim(), w, Math.round(100 * n / ids.length));
}
async function pairnow(x) {
  if (!lane(x.ctx, "nest")) return no("Placing pieces is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (!x.work.stage) {
    const L = lines(x.ctx), keys = [...new Set((x.a.ids || []).map(String))].filter(k => { const l = L[k]; return l && l.state === "waiting" && l.wait && l.wait.kind === "pair" && !l.hold; });
    if (!keys.length) return no("That line does not wait for a sheet to fill.");
    const run = x.ctx.run || {}, free = [...new Set((run.pairFree || []).map(String).concat(keys.map(ridOf)))].slice(-400);
    let r = await put(x, { op: "runPut", run: { runId: x.ctx.runId, pairFree: free }, merge: true }, `pairfree|${keys.sort().join(",")}`); if (err(r)) return no(`Not placed: ${err(r)}`);
    const patch = {}; for (const k of keys) patch[k] = { state: (L[k].poolIds || []).length ? "pooled" : "pulled", wait: null, reason: null };
    r = await put(x, { op: "runLinesPatch", runId: x.ctx.runId, lines: patch }, `pairnow|${keys.join(",")}`); if (err(r)) return no(`Not placed: ${err(r)}`);
    return wait("Placing the waiting piece…", { stage: "placing", keys, at: Date.now() });
  }
  const v = await placedNow(x.ctx, x.work.keys || []);
  if (v.pieces && !v.open) return ok(`${v.placed} piece(s) placed on ${and(v.names)}`);
  if (Date.now() - num(x.work.at) > PLACE_MS) return v.placed ? ok(`${v.placed} piece(s) placed on ${and(v.names)}; the rest waits: ${v.waiting[0] || "for the next placement"}`) : no(`Not placed: ${v.waiting[0] || "the intake did not take it"}`);
  return wait("Placing the waiting piece…", x.work);
}
async function sendhome(x) {
  if (!lane(x.ctx, "nest") || !has("placeOnSheet")) return no("Moving a piece between sheets is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  const ctx = x.ctx;
  if (!x.work.stage) {
    const L = lines(ctx), sheets = await runSheets(ctx), on = listedBy(sheets), why = [], moves = [];
    for (const k of [...new Set((x.a.ids || []).map(String))].slice(0, 12)) {
      const l = L[k]; if (!l || !OPEN_LINE(l) || !(l.poolIds || []).length) { why.push(`${k}: no piece of it is on a sheet`); continue; }
      const rows = await pools(ctx, l.poolIds), homeNames = [...new Set([...rows.values()].map(r => String(r.sheetName || "")).filter(Boolean))];
      const nowOn = [...new Set(l.poolIds.map(id => on.get(String(id))).filter(Boolean))], home = homeNames.map(nm => sheets.find(s => s.fileBase === nm)).filter(s => s && !nowOn.includes(s));
      const rid = ridOf(k);
      if (!nowOn.length) { why.push(`${rid}: on no sheet`); continue; }
      if (!home.length) { why.push(`${rid}: ${homeNames.length ? "already on " + homeNames.join(", ") : "the sheet it came off is not known"}`); continue; }
      const cut = nowOn.concat(home).find(cutRec); if (cut) { why.push(`${rid}: ${nameOf(cut)} was already cut`); continue; }
      for (const src of nowOn) {
        const ids = l.poolIds.filter(id => on.get(String(id)) === src).map(String), charms = (src.charms || []).filter(c => c && ids.includes(String(c.poolId)));
        const r = await put(x, { op: "placeOnSheet", runId: ctx.runId, metal: home[0].metal, lineKey: k, rid, poolIds: ids, charms, target: { sheetId: home[0].id, layoutBase: num(home[0].layoutRev), contentBase: num(home[0].contentRev) }, from: { sheetId: src.id, layoutBase: num(src.layoutRev), contentBase: num(src.contentRev), reason: "homeward" } }, `home|${k}|${src.id}|${home[0].id}|${num(home[0].layoutRev)}`);
        if (err(r)) { why.push(`${rid}: ${err(r)}`); continue; }
        moves.push({ key: k, home: home[0].id });
      }
    }
    if (!moves.length) return no((why.join("; ") || "Nothing was moved.").slice(0, 380));
    const homes = [...new Set(moves.map(m => m.home))], waitOn = [];
    for (const id of homes) { const rec = await sheetRec(ctx, id); waitOn.push({ id, layoutRev: num(rec && rec.layoutRev), at: Date.now() }); await startNest(x, id, false, `pieces sent home, asked by ${x.cmd.by || "a person"}`); }
    return wait("Putting the piece back on its own sheet…", { stage: "nest", sheets: waitOn, keys: moves.map(m => m.key), why });
  }
  return settleNests(x, "Putting the piece back on its own sheet…", async () => {
    const v = await placedNow(ctx, x.work.keys || []);
    return v.placed ? ok(`${(x.work.keys || []).length} order line(s) back on ${and(v.names)}${(x.work.why || []).length ? " · " + x.work.why.join("; ") : ""}`) : no(`${(x.work.keys || []).length} order line(s) were moved but are not seated yet: ${v.waiting[0] || "the sheet had no room; the overflow takes them on"}`);
  });
}
/** Every sheet named in work.sheets has settled (or ran past its time): then fin() answers. */
async function settleNests(x, text, fin) {
  let open = 0, late = 0;
  for (const w of x.work.sheets || []) { const s = await nestSettled(x, w); if (!s.settled) { open++; if (s.timedOut) late++; } }
  if (open && late < open) return wait(text, x.work);
  return fin(late);
}

/* INPLACE (REMAKELINE, KEEPREMOTE, REMAKESTALE, ANYWHERE): order lines made up again, each in the place its pieces had. The stages, one or more passes each:
     world    a request that changes what a line reads as waits for the next reading of the orders (the world build after the request: summary.at), so the reading is the cloud's
     pick     the lines (remakeline: the ones named; keepsync / remake: open lines of these SKUs whose pieces hold an older drawing or another SKU; skuonce: the one line)
     vacate   each line's pieces leave their sheet, which keeps their place (cmdVacate); the line goes back to the intake (pulled, repoolChanged, vacated)
     remade   the intake has made its pieces again (the line is pooled, its new pieces exist and stand on no sheet)
     back     the new pieces go back on the sheet that kept their place (cmdPutBack), which owes a nest that seats them in its free room; with no place kept (the sheet was
              cut or closed meanwhile) the line is placed as any line is
     nest     the sheets settle. A sheet that is cut, behind a green line or in a committed set keeps its pieces as they are (said in the answer). */
async function inplace(x, label) {
  const ctx = x.ctx, w = x.work, now = Date.now();
  if (!lane(ctx, "nest") || !lane(ctx, "pool")) return no(`${label} is not the cloud's yet (in this build pieces are made and placed by a computer): with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again`);
  // (KEEPNOW, Paul 22 Nov 2026: "This is taking too long ... This type of update must happen in real-time": a save to the charm repository changes a drawing, never what an order reads as,
  //  so its pieces are picked at once, not after the next reading of the orders, a minute or two)
  if (!w.stage && x.noWorld) Object.assign(w, { stage: "pick", asked: num(x.cmd.at) || now, at: now });
  if (!w.stage) return wait("Waiting for the next reading of the orders…", { stage: "world", asked: num(x.cmd.at) || now, at: now });
  if (w.stage === "world") {
    const at = await worldAt(ctx);
    if (at > num(w.asked)) return wait("Reading the lines again…", Object.assign({}, w, { stage: "pick", worldAt: at }));
    if (now - num(w.at) > WORLD_MS) return no("The orders were not read again in 15 minutes (the world build): nothing was made again. Ask again in a moment");
    return wait("Waiting for the next reading of the orders…", w);
  }
  if (w.stage === "pick") {
    const keys = await x.pick();
    if (!keys.length) return ok(x.none || "Nothing to make again: every open piece already holds the drawing the library has now");
    return wait("Taking the old pieces off…", Object.assign({}, w, { stage: "vacate", keys: keys.slice(0, 40), done: [], why: [], vac: {} }));
  }
  if (w.stage === "vacate") {
    const L = lines(ctx), sheets = await runSheets(ctx), sets = await runSets(ctx), on = listedBy(sheets), vac = Object.assign({}, w.vac || {}), why = (w.why || []).slice(), patch = {};
    let busy = 0;
    for (const k of w.keys) {
      if (vac[k]) continue; const l = L[k]; if (!l || !OPEN_LINE(l)) { vac[k] = { skip: true }; continue; }
      const bySheet = new Map(); for (const id of l.poolIds || []) { const s = on.get(String(id)); if (s) (bySheet.get(s) || bySheet.set(s, []).get(s)).push(String(id)); }
      // (a line is made again whole or not at all: a sheet that keeps what it holds (cut, behind a green line, in a set sent to the station) keeps the whole line as it is)
      const keeps = [...bySheet.keys()].find(s => cutRec(s) || lineDrawn(s) || committedSheet(s, sets));
      if (keeps) { why.push(`${ridOf(k)}: ${nameOf(keeps)} ${cutRec(keeps) ? "was already cut" : lineDrawn(keeps) ? "is behind a green line" : "is in a set sent to the station"}: its pieces stay as they are`); vac[k] = { skip: true }; continue; }
      if ([...bySheet.keys()].some(s => busyRec(s, now))) { await stopFor(x, [...bySheet.keys()], `${label} (a person's request goes first)`); busy++; continue; }   // (FIRSTNOW)
      /* ORIGINHOME (Paul, 22 Nov 2026: order 4231992262's discs, made again on a draft, "should have gone back to the original sheet"): pieces that came onto a draft after being made
         again from a sheet (their records' vacatedFrom, the Nov 18 set's sheet) keep that sheet as their home: taken off the draft, their records still name it, and once made again they
         go back to it (cmdMovePieces lets a set's sheet take its own order back), not to the draft. */
      const before = await pools(ctx, l.poolIds || []), vfs = new Set([...before.values()].map(r => String(r.vacatedFrom || "")));
      const origin0 = vfs.size === 1 && !vfs.has("") ? [...vfs][0] : "", origin = origin0 && [...bySheet.keys()].every(s => s.id !== origin0 && s.draft && !s.setId) ? origin0 : "";
      const left = [], kept = [];
      for (const [s, ids] of bySheet) {
        const r = await put(x, Object.assign({ op: "cmdVacate", runId: ctx.runId, lineKey: k, rid: ridOf(k), sheetId: s.id, poolIds: ids, layoutBase: num(s.layoutRev), contentBase: num(s.contentRev), by: x.cmd.by || "", why: label }, origin ? { origin } : {}), `vacate|${k}|${s.id}|${num(s.contentRev)}`);
        if (err(r)) { if (r.keep) { kept.push(...ids); why.push(`${ridOf(k)}: ${err(r)}`); } else busy++; continue; }
        left.push(s.id);
      }
      if (busy && !left.length) continue;
      if (kept.length && !left.length) { vac[k] = { skip: true }; continue; }
      vac[k] = Object.assign({ sheets: left }, origin ? { origin } : {});
      // (the line goes back to the intake whole: its pieces keep their ids (O.poolId), so a piece a sheet kept after all (cut a moment ago) is still listed there and is not placed twice)
      patch[k] = Object.assign({ state: "pulled", poolIds: [], repoolChanged: true, wait: null, reason: null }, left.length ? { vacated: { sheetIds: left, at: now } } : {});
    }
    if (Object.keys(patch).length) { const r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: patch }, `inplace|pulled|${Object.keys(patch).sort().join(",").slice(0, 400)}`); if (err(r)) return no(`Not made again: ${err(r)}`); }
    const pending = w.keys.filter(k => !vac[k]);
    if (pending.length && now - num(w.at) < NEST_MS) return wait("Taking the old pieces off… (a sheet is busy)", Object.assign({}, w, { vac, why }));
    const todo = w.keys.filter(k => vac[k] && !vac[k].skip);
    if (!todo.length) return no((why.join("; ") || "Nothing was made again").slice(0, 380));
    return wait("Making the pieces again…", Object.assign({}, w, { stage: "remade", vac, why, todo, remadeAt: now }));
  }
  if (w.stage === "remade") {
    /* NOWCMD (Paul 21 Nov 2026: a Change SKU sat 15 minutes at "Making the pieces again… 0 of 1 made": the line went back to the intake, whose gate opens only at the run's checkpoint or
       when it rests, and the run waited at commit for the Etsy freshness gate). The request makes its own lines' pieces now, as a person's Send to Sheet does (CUSTOMSEND): the intake's
       pool with only these lines, past the run-step gate, every other rule kept (a blocked SKU, a design missing, a size its design lacks: the line is held with its words) */
    const got0 = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), fresh = got0 && got0.run;
    if (fresh && fresh.lines) {
      const pend = (w.todo || []).filter(k => { const l = fresh.lines[k]; return l && OPEN_LINE(l) && ["pulled", "pooled", "waiting", "contended"].includes(String(l.state)) && !(l.poolIds || []).length; });
      if (pend.length) {
        try { await require("./_charmNestIntake").pool({ db: ctx.db, run: Object.assign({}, fresh, { runId: ctx.runId }), runId: ctx.runId, call: ctx.call, read: ctx.read, drive: ctx.drive || {}, cloudSteps: ctx.cloudSteps || [], fence: ctx.fence, say: ctx.say || (() => {}), budgetEnd: num(ctx.budgetEnd) || Date.now() + 45000, settings: null, stockPreset: null }, { step: true, only: new Set(pend) }); }
        catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; if (ctx.say) ctx.say(`[cmd] ${label}: the pieces were not made now: ${str((e && e.message) || e, 200)}`); }
      }
    }
    const run = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), L = (run && run.run && run.run.lines) || lines(ctx), ready = [], waiting = [], why = (w.why || []).slice();
    for (const k of w.todo) {
      const l = L[k];
      if (l && l.state === "pooled" && (l.poolIds || []).length) ready.push(k);
      else if (l && ["held", "oversize", "unmatched", "noDesign"].includes(String(l.state)) && !(l.poolIds || []).length) why.push(`${ridOf(k)}: not made again: ${str(l.reason || (l.hold && l.hold.why) || l.state, 160)}`);   // (said exactly, never waited on)
      else waiting.push(k);
    }
    if (waiting.length && now - num(w.remadeAt) < REMADE_MS) return wait(`Making the pieces again… ${ready.length} of ${w.todo.length} made`, w, Math.round(100 * ready.length / w.todo.length));
    return wait("Putting the new pieces in place…", Object.assign({}, w, { stage: "back", ready, late: waiting, why }));
  }
  if (w.stage === "back") {
    const run = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), L = (run && run.run && run.run.lines) || lines(ctx), placedOn = new Map(), why = (w.why || []).slice(), unvac = {}, loose = [];
    for (const k of w.ready || []) {
      const l = L[k], home = ((w.vac[k] || {}).sheets || [])[0]; unvac[k] = { vacated: null };
      if (!l) continue;
      // (NEWLINE, Paul 22 Nov 2026: Michelle's Change SKU SHEEP3-CO → SHEEP 3 made the 2 pieces and answered "made again: 0 piece(s)", failed: the order was new, on no sheet, so it had no
      //  place to go back to and was skipped here, never placed. Such a line is placed now as any new piece is)
      if (!home) { loose.push(k); continue; }
      const jr = await ctx.db.collection(INTAKE).doc(k).get(), charms = jr.exists ? (jr.data() || {}).charms : null, s = await sheetRec(ctx, home);
      let mine = Array.isArray(charms) ? charms.filter(c => c && (l.poolIds || []).map(String).includes(String(c.poolId)) && (!s || !c.metal || c.metal === s.metal)) : [];
      // (FIXBACK, Paul 22 Nov 2026: order 4231992262's new discs went to a new draft, not back to the Nov 18 set sheet they came from: the intake's ledger never holds a line's pieces
      //  once it is pooled, so they were never "described" and every put-back gave up. They are described now from their own records, as the placement describes a piece)
      if (!mine.length && s) mine = (await describeLine(ctx, l.poolIds)).filter(c => (!c.metal || c.metal === s.metal));
      if (!s || !mine.length) { why.push(`${ridOf(k)}: placed as a new piece (${!s ? "its sheet is gone" : "its new pieces are not described yet"})`); loose.push(k); continue; }
      const r = await put(x, { op: "cmdPutBack", runId: ctx.runId, lineKey: k, rid: ridOf(k), sheetId: home, poolIds: mine.map(c => String(c.poolId)), charms: mine, keepBacks: !!x.keepBacks, layoutBase: num(s.layoutRev), contentBase: num(s.contentRev), by: x.cmd.by || "" }, `back|${k}|${home}|${num(s.contentRev)}`);
      if (err(r) && r.busy && now - num(w.backAt || now) < FIRST_MS) { await stopFor(x, [s], `${label} (a person's request goes first)`); return wait(`Putting the new pieces back as soon as ${nameOf(s)} is still…`, Object.assign({}, w, { backAt: num(w.backAt) || now, why })); }   // (FIRSTNOW)
      if (err(r)) { why.push(`${ridOf(k)}: ${err(r)}; it is placed as any piece is`); loose.push(k); continue; }
      if (!r.noop) placedOn.set(home, { id: home, layoutRev: num(r.layoutRev), at: now });
      const origin = (w.vac[k] || {}).origin;   // (ORIGINHOME: then on to the sheet they first came from)
      if (origin && origin !== home) {
        const f = await sheetRec(ctx, home), t = await sheetRec(ctx, origin);
        const m = f && t ? await put(x, { op: "cmdMovePieces", runId: ctx.runId, from: { sheetId: home, layoutBase: num(f.layoutRev), contentBase: num(f.contentRev) }, to: { sheetId: origin, layoutBase: num(t.layoutRev), contentBase: num(t.contentRev) },
          poolIds: mine.map(c => String(c.poolId)), by: x.cmd.by || "", why: "back on the sheet it was first made on" }, `origin|${k}|${origin}|${num(t.contentRev)}`) : { status: 404, error: "its first sheet is gone" };
        if (err(m) && /try again once it is done/.test(err(m)) && f && t && now - num(w.backAt || now) < FIRST_MS) { await stopFor(x, [f, t], `${label} (a person's request goes first)`); return wait(`Putting the new pieces back on ${nameOf(t)} as soon as it is still…`, Object.assign({}, w, { backAt: num(w.backAt) || now, why })); }   // (FIRSTNOW)
        if (err(m)) why.push(`${ridOf(k)}: kept on ${nameOf(f || { id: home })} (${err(m)})`);
        else { placedOn.delete(home); placedOn.set(origin, { id: origin, layoutRev: num(m.to && m.to.layoutRev), at: now }); }
      }
    }
    // every place still kept for these lines is let go (a line not put back is placed as any line), then the lines are placed by the cloud's placement as usual
    for (const k of (w.ready || []).concat(w.late || [])) for (const sid of (w.vac[k] || {}).sheets || []) await put(x, { op: "cmdPutBack", runId: ctx.runId, lineKey: k, sheetId: sid, release: true }, `release|${k}|${sid}`);
    for (const k of w.late || []) { unvac[k] = { vacated: null }; why.push(`${ridOf(k)}: its pieces were not made again in time; the intake makes them and they are placed as new pieces`); }
    if (Object.keys(unvac).length) await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: unvac }, `inplace|unvac|${Object.keys(unvac).sort().join(",").slice(0, 400)}`);
    /* NOWCMD: a line that could not go back to its kept place is placed now by the placement unit's own rules (only these lines; SETSHUT and the rest kept), not at the run's next nest step,
       and every sheet that took one is asked to nest now, so the request answers only once the new pieces are on a sheet */
    if (loose.length) {
      try {
        const g2 = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), fr = g2 && g2.run;
        if (fr && fr.lines) await require("./_charmNestPlaceUnit").placeAll(Object.assign({}, ctx, { run: Object.assign({}, fr, { runId: ctx.runId }), only: new Set(loose) }));
        const g3 = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), L3 = (g3 && g3.run && g3.run.lines) || {}, on = listedBy(await runSheets(ctx));
        for (const k of loose) for (const id of (L3[k] || {}).poolIds || []) { const s = on.get(String(id)); if (s && !placedOn.has(s.id) && (s.nestOwed || !seated(s, id))) placedOn.set(s.id, { id: s.id, layoutRev: num(s.layoutRev), at: now }); }
      } catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; why.push(`the new pieces were not placed now: ${str((e && e.message) || e, 160)}`); }
    }
    for (const s of placedOn.values()) await startNest(x, s.id, false, `${label}, asked by ${x.cmd.by || "a person"}`);
    /* SKUFAST (Paul, 22 Nov 2026: "updating SKUs … needs to be near instant and all the computers need to be updated near instantly"): the request is answered as soon as every new piece is on
       its sheet (listed there, the sheet's nest asked): the nest and the sheet's files go on by themselves, as after any placement, and every computer sees them through the changes feed. It
       no longer waits a minute or two more for that nest. A piece not on a sheet yet keeps the request at its nest step, which says how it ended. */
    { const g = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), L2 = (g && g.run && g.run.lines) || {}, on2 = listedBy(await runSheets(ctx)), names = new Set(); let pieces = 0, listed = 0;
      for (const k of w.ready || []) for (const id of (L2[k] || {}).poolIds || []) { pieces++; const sh = on2.get(String(id)); if (sh) { listed++; names.add(nameOf(sh)); } }
      if ((w.ready || []).length && pieces && listed === pieces && !(w.late || []).length)
        return ok(`${(w.ready || []).length} order line(s) made again: ${listed} piece(s) on ${and([...names])}, being nested${why.length ? " · " + why.join("; ") : ""}`); }
    return wait("Seating the new pieces…", Object.assign({}, w, { stage: "nest", sheets: [...placedOn.values()], why }));
  }
  if (w.stage === "nest") {
    return settleNests(x, "Seating the new pieces…", async () => {
      const v = await placedNow(ctx, w.todo || []), made = (w.ready || []).length;
      const text = `${made} order line(s) made again: ${v.placed} piece(s)${v.names.length ? " on " + and(v.names) : ""}${(w.why || []).length ? " · " + w.why.join("; ") : ""}`;
      return made && (v.placed || !v.pieces) ? ok(text) : no(text);
    });
  }
  return no("The request lost its place: ask again");
}
/** The open lines whose pieces hold an older drawing of these SKUs, or another SKU than the line reads as now (the world's line). */
/* FASTPICK (Paul, 22 Nov 2026: a save to the charm repository on Michelle's computer stayed at "Reading the lines again…" and failed; the request after it waited behind it): this read every
   open line of the run one by one, two reads a line (its world reading, its pieces), about 1,400 lines: two minutes, past the unit's one-minute limit. The unit was cut off when it went to save
   what it found, so every pass read them all again from the start, for good, and every request after it waited. The world readings and the pieces are read now in a few batched reads
   (300 a read), a few seconds for the whole run. */
async function staleLines(ctx, skus) {
  const want = new Set(skus.map(s => String(s).trim().toUpperCase()).filter(Boolean)), L = lines(ctx), out = [], entries = new Map();
  const entryOf = async sku => { if (!entries.has(sku)) { const s = await ctx.db.collection(MASTER).doc(sku).get(); entries.set(sku, s.exists ? s.data() : null); } return entries.get(sku); };
  const keys = Object.keys(L).filter(k => { const l = L[k]; return OPEN_LINE(l) && (l.poolIds || []).length && !l.hold; });
  const world = new Map(), rowsAll = new Map();
  for (let i = 0; i < keys.length; i += 300) for (const s of await ctx.db.getAll(...keys.slice(i, i + 300).map(k => ctx.db.collection(WORLD_LINES).doc(k)))) {
    if (!s.exists) continue; const d = s.data() || {}; if (d.gone) continue; try { world.set(s.id, JSON.parse(d.data || "{}")); } catch (_) { /* a reading that does not parse: the run line's own SKU is used */ }
  }
  const ids = [...new Set(keys.flatMap(k => (L[k].poolIds || []).map(String)))];
  for (let i = 0; i < ids.length; i += 300) for (const s of await ctx.db.getAll(...ids.slice(i, i + 300).map(id => ctx.db.collection(POOL).doc(id)))) if (s.exists) rowsAll.set(s.id, s.data());
  for (const k of keys) {
    const l = L[k];
    const wl = world.get(k) || null, design = String((wl && wl.spec && wl.spec.designSku) || l.sku || "").toUpperCase(), rows = new Map((l.poolIds || []).map(String).filter(id => rowsAll.has(id)).map(id => [id, rowsAll.get(id)]));
    const skusHere = new Set([design].concat([...rows.values()].map(r => String(r.sku || "").toUpperCase())).filter(Boolean));
    if (![...skusHere].some(s => want.has(s))) continue;
    const e = design ? await entryOf(design) : null;
    const stale = [...rows.values()].some(r => !r.custom && ((design && String(r.sku || "").toUpperCase() !== design) || (e && e.masterHash && r.masterHash && r.masterHash !== e.masterHash) || redrawn(e, r)));
    if (stale) out.push(k);
    if (out.length >= 40) break;
  }
  return out;
}
/* REDRAWN (Paul, 22 Nov 2026: 9MM_DISC_8391 updated to 9.0 mm in the Charm Editor with "Make the open pieces again", and the cloud answered "Nothing to make again" while its 4 discs
   stood on a sheet at the old size): a Charm Editor update replaces the design's drawing (charmHash, its edited.from → edited.to) and keeps the master file's hash (masterHash), the only
   hash this looked at. A piece made from the drawing the update replaced (its charmHash, a fitted size's "~10MM" aside, is edited.from) holds an older drawing. */
function redrawn(e, r) {
  const ed = e && e.edited, from = ed && ed.from != null ? String(ed.from) : "", now = String((ed && ed.to) || (e && e.charmHash) || "");
  return !!(from && from !== now && r && r.charmHash && String(r.charmHash).split("~")[0] === from);
}
/** FIXBACK: a line's live pieces as a sheet lists them, from their own records and the library's index (the placement's own description: charm-nest-place.js charmOfRow). */
async function describeLine(ctx, ids) {
  const PU = require("./_charmNestPlaceUnit"), Place = require("../../charm-nest-place.js"), rows = [...(await pools(ctx, ids || [])).values()].filter(r => r && r.poolId && !["abandoned", "superseded"].includes(String(r.state || "")));
  const entries = new Map();
  for (const sku of [...new Set(rows.map(r => String(r.sku || "").trim().toUpperCase()).filter(Boolean))]) { const d = await ctx.db.collection(MASTER).doc(sku).get(); if (d.exists) entries.set(sku, d.data()); }
  const now = Date.now();
  return rows.map(r => PU.customCharm(Place.charmOfRow(Object.assign({}, r, { material: r.material || r.metal }), PU.geometryOf(entries.get(String(r.sku || "").trim().toUpperCase()), r), now), r));
}
/* BACKHOME (Paul, 22 Nov 2026: "that should've gone back to the original sheet"): an order line made again whose new pieces went onto another sheet goes back to the sheet its pieces were
   taken from (each piece's record names it: vacatedFrom), even a sheet of a set (it is its own order coming back: cmdMovePieces lets that sheet take it), into the room they left; that sheet
   is nested again around what it holds. A sheet that was cut, has a green line, or whose set went to the station keeps what it has (cmdMovePieces refuses it, in words). */
async function backhome(x) {
  const ctx = x.ctx, key = String((x.a.ids || [])[0] || "");
  if (x.work.stage === "nest") {
    // (SEATEDNOW: answered the moment every piece of the order is seated on its sheet, as a Change SKU is (SKUFAST), never after the sheet's last marks settle: the 18:18 request said
    //  "its nest did not finish" 25 minutes after its pieces were seated)
    const rec0 = await sheetRec(ctx, (x.work.sheets[0] || {}).id), want = (x.work.ids || []).map(String);
    if (rec0 && want.length && want.every(id => seated(rec0, id))) return ok(`The order's pieces are back on ${nameOf(rec0)}: ${placedWords(rec0)}`);
    return settleNests(x, "Nesting the order on its sheet…", async late => {
    const rec = await sheetRec(ctx, (x.work.sheets[0] || {}).id); if (!rec) return no("That sheet is no longer in the cloud.");
    return late ? no(`The order's pieces are back on ${nameOf(rec)}, but its nest did not finish (${rec.status || "waiting"})`) : ok(`The order's pieces are back on ${nameOf(rec)}: ${placedWords(rec)}`);
  }); }
  const l = lines(ctx)[key]; if (!l || !OPEN_LINE(l) || !(l.poolIds || []).length) return no("That order line has no pieces in the run");
  const live = [...(await pools(ctx, l.poolIds)).values()].filter(r => r && r.sheetId && !["abandoned", "superseded"].includes(String(r.state || "")));
  if (!live.length) return no("The order's pieces are on no sheet yet: ask again once they are placed");
  const homes = new Set(live.map(r => String(r.vacatedFrom || ""))), on = new Set(live.map(r => String(r.sheetId)));
  if (homes.size !== 1 || homes.has("")) return no("The order's pieces do not name one sheet they were taken from");
  if (on.size !== 1) return no("The order's pieces are on more than one sheet");
  const home = [...homes][0], from = [...on][0];
  if (from === home) return ok("The order's pieces are on their sheet already");
  const f = await sheetRec(ctx, from), t = await sheetRec(ctx, home); if (!f || !t) return no("That sheet is no longer in the cloud");
  const first = num(x.work.firstNowAt) || Date.now(), held = (r0, why0) => {   // (FIRSTNOW)
    if (Date.now() - first > FIRST_MS) return no(`Not moved back: ${nameOf(r0)} ${why0} after 15 minutes; ask again`);
    return wait(`Moving the order back as soon as ${nameOf(r0)} is still (${why0})…`, Object.assign({}, x.work, { stage: "move", firstNowAt: first }));
  };
  { const nowB = Date.now(), who = [f, t].find(r0 => liveNest(r0) || busyRec(r0, nowB));
    if (who) { await stopFor(x, [f, t], `${ridOf(key)}: back on its own sheet (a person's request goes first)`); return held(who, busyRec(who, nowB) || "nesting"); } }
  const r = await put(x, { op: "cmdMovePieces", runId: ctx.runId, from: { sheetId: from, layoutBase: num(f.layoutRev), contentBase: num(f.contentRev) }, to: { sheetId: home, layoutBase: num(t.layoutRev), contentBase: num(t.contentRev) },
    poolIds: live.map(p => String(p.poolId)), by: x.cmd.by || "", why: "back on the sheet it was made again from" }, `backhome|${key}|${home}|${num(t.contentRev)}`);
  if (err(r) && /try again once it is done|changed since this move was planned/.test(err(r))) return held(/files/.test(err(r)) ? f : t, err(r).replace(/^.*? (is|was|changed)/, "$1").replace(/: try again.*$/, ""));
  if (err(r)) return no(`Not moved back: ${err(r)}`);
  await startNest(x, home, false, `${ridOf(key)}: back on the sheet it came from`);
  return wait("Nesting the order on its sheet…", { stage: "nest", sheets: [{ id: home, layoutRev: num(r.to && r.to.layoutRev), at: Date.now() }], ids: live.map(p => String(p.poolId)) });
}
async function remakeline(x) {
  x.keepBacks = true;
  x.pick = async () => [...new Set((x.a.ids || []).map(String))].filter(k => OPEN_LINE(lines(x.ctx)[k]));
  x.none = "Those lines are not open lines of the run";
  return inplace(x, "Making the order's pieces again");
}
async function keepsync(x) {
  x.noWorld = true;   // (KEEPNOW)
  x.pick = () => staleLines(x.ctx, x.a.ids || []);
  return inplace(x, "Making the other orders again");
}
async function remake(x) {
  x.pick = () => staleLines(x.ctx, x.a.ids || []);
  return inplace(x, "Making the pieces of the older drawing again");
}
async function skuonce(x) {
  const ctx = x.ctx, key = String((x.a.ids || [])[0] || ""), sku = String(x.a.sku || "").trim().toUpperCase();
  if (!x.work.stage) {
    const l = lines(ctx)[key]; if (!key || !sku) return no("not ready");
    if (!l || !OPEN_LINE(l)) return no(`The piece ${key} is not in the run the cloud carries`);
    const e = await ctx.db.collection(MASTER).doc(sku).get(); if (!e.exists) return no(`${sku} is not in the charm repo`);
    if ((e.data() || {}).blocked || (e.data() || {}).block) return no(`${sku} is blocked in the master, so it cannot be cut`);
    const was = String((l.skuOverride && l.skuOverride.sku) || l.sku || ""); if (was.toUpperCase() === sku) return ok(`This piece is ${sku} already`);
    if ((l.poolIds || []).length && (!lane(ctx, "nest") || !lane(ctx, "pool"))) return no("Changing a placed piece is not the cloud's yet (in this build pieces are made and placed by a computer): with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
    const at = Date.now(), who = String(x.cmd.by || "").trim() || "a person", say = `Just this order: ${was || "no SKU"} → ${sku} (the listing and Etsy are not changed)`;
    let r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: { [key]: { skuOverride: { sku, by: who, at, was }, repoolChanged: (l.poolIds || []).length > 0 } } }, `once|${key}|${sku}`); if (err(r)) return no(err(r));
    r = await put(x, { op: "timelineAdd", events: [{ orderId: ridOf(key), type: "decided", id: `${key}.once.${at}`, at, by: who, station: "sorter", text: `Decided by ${who} · ${say}`.slice(0, 200), data: { sku, once: true, was: was || null } }] }, `once|tl|${key}|${sku}`);
    if (!(l.poolIds || []).length) return ok(`${sku} for order ${ridOf(key)} only: its pieces are made at the next intake`);
    x.work = { stage: "world", asked: at, at, once: { key, sku } };
    return wait(`${sku} for this order only… waiting for the order to be read again`, x.work);
  }
  x.pick = async () => [key].filter(k => OPEN_LINE(lines(ctx)[k]));
  const res = await inplace(x, `${sku} for this order only`);
  if (res.done && res.done.ok) res.done.text = `${sku} for order ${ridOf(key)} only · ${res.done.text}`.slice(0, 380);
  return res;
}
async function release(x) {
  const ctx = x.ctx, rid = String((x.a.ids || [])[0] || "").replace(/\D/g, "");
  if (!rid) return no("not ready");
  if (!has("releaseAsk")) return no("Release Hold is not in the cloud in this build: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (!x.work.stage) {
    const r = await put(x, { op: "releaseAsk", orderId: rid, runId: ctx.runId, note: x.cmd.by ? `asked by ${x.cmd.by}` : "" }, `release|${rid}`);
    if (err(r)) return no(err(r));
    const keys = Object.keys(r.lines || {}).filter(k => k.startsWith(rid + "_"));
    if (!lane(ctx, "nest") || !keys.length) return ok(`Order ${rid} released from hold, at the front of the queue${r.already ? " (it was not on hold in the cloud)" : ""}`);
    return wait(`Order ${rid}: its place at the front is saved`, { stage: "placing", keys, at: Date.now() });
  }
  const v = await placedNow(ctx, x.work.keys || []);
  if (v.pieces && !v.open) return ok(`Order ${rid} released from hold and placed on ${and(v.names)}`);
  if (Date.now() - num(x.work.at) > RELEASE_PLACE_MS) return ok(`Order ${rid} released from hold, at the front of the queue${v.placed ? `; ${v.placed} piece(s) placed on ${and(v.names)}` : ""}${v.waiting[0] ? " · " + v.waiting[0] : ": its pieces are placed at the next intake"}`);
  return wait(v.names.length ? `Placed on ${and(v.names)}…` : "Moving its pieces…", x.work);
}

/* ═══ nest ═══ */
/* RESTORE (Paul, 21 Nov 2026: "I don't want this nesting … fix that"): Re-nest with sourceId = a nest job that finished on this sheet puts that nest's saved layout back on the sheet,
   every piece where that nest placed it (only pieces still on the sheet); no new nest is asked and no file is written by this request. */
async function restoreLayout(x) {
  const p = await sheetRec(x.ctx, x.a.sheetId); if (!p || p.archived || p.runId !== x.ctx.runId) return no("That sheet is not on the run the cloud carries.");
  if (cutRec(p)) return no("That sheet was already cut: its layout is not changed.");
  let j = null, res = null;
  if (Array.isArray(x.a.poses) && x.a.poses.length) {   // (a layout a person's computer kept: each pose names its piece by pool id)
    const idOf = new Map(activeCharms(p).map(c => [String(c.poolId), String(c.id)]));
    res = { placements: x.a.poses.filter(q => idOf.has(String(q.poolId))).map(q => ({ id: idOf.get(String(q.poolId)), cxPt: +q.cxPt, cyPt: +q.cyPt, angle: +q.angle || 0, wPt: num(q.wPt), hPt: num(q.hPt), scale: num(q.scale) || 1 })), density: num(p.density), freePt2: num(p.freePt2), verification: null };
    j = { finishedMs: Date.now() };
  } else {
    const js = await x.ctx.db.collection("Charm_Nest_Jobs").doc(String(x.a.sourceId)).get(); j = js.exists ? js.data() || {} : null; res = j && j.result;
    if (!j || j.sheetId !== p.id || j.status !== "done" || !res || !Array.isArray(res.placements)) return no("That nest has no finished layout saved for this sheet.");
  }
  const on = new Set(activeCharms(p).map(c => String(c.id))), placements = res.placements.filter(q => q && on.has(String(q.id)));
  if (!placements.length) return no("None of the pieces that nest placed are on the sheet now.");
  const r = await put(x, { op: "putSheet", layoutBase: num(p.layoutRev), sheet: { id: p.id, placements, rejects: [], feedWait: null, placedCount: placements.length, density: num(res.density), freePt2: num(res.freePt2),
    verification: res.verification || null, endedBy: "kept", status: "complete", dirty: false, nestOwed: null } }, `restore|${p.id}|${x.a.sourceId}|${num(p.layoutRev)}`);
  if (err(r)) return no(`Not restored: ${err(r)}`);
  return ok(`${nameOf(p)}: its layout is back, ${placements.length} of ${on.size} pieces placed where the nest of ${new Date(num(j.finishedMs)).toLocaleTimeString("en-CA", { timeZone: "America/Toronto" })} left them`);
}
async function renest(x) {
  if (!x.work.stage && (x.a.sourceId || (Array.isArray(x.a.poses) && x.a.poses.length))) return restoreLayout(x);
  if (!lane(x.ctx, "nest") || !has("nestStart")) return no("Nesting is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (!x.work.stage) {
    const p = await sheetRec(x.ctx, x.a.sheetId), sets = await runSets(x.ctx), now = Date.now();
    if (!p || p.archived || p.runId !== x.ctx.runId) return no("That sheet is not on the run the cloud carries.");
    if (cutRec(p)) return no("That sheet was already cut: it is not nested again.");
    if (busyRec(p, now)) return no("That sheet is being nested right now.");
    if (committedSheet(p, sets)) return no("That sheet is in a committed set: it is not nested again.");
    const s = await startNest(x, p.id, true, `Re-nest asked by ${x.cmd.by || "a person"}`); if (s.error) return no(s.error);
    return wait("Starting the nest…", { stage: "nest", sheets: [{ id: p.id, layoutRev: num(p.layoutRev), at: now }], name: nameOf(p) });
  }
  return settleNests(x, "Nesting…", async late => {
    const rec = await sheetRec(x.ctx, x.work.sheets[0].id); if (!rec) return no("The sheet is no longer in the cloud.");
    if (late) return no(`the nest did not finish (${rec.status || "waiting"})`);
    return rec.problem ? no(String(rec.problem.message || rec.problem)) : ok(`${x.work.name} was nested again: ${placedWords(rec)}`);
  });
}
/* o.draft (COMBINEDRAFTS, 21 Nov 2026: two Gold Filled / Sterling Silver drafts of the run): the emptied draft is archived (archiveEmptySheet) and the target nested again from scratch (fresh) */
async function moveAndNest(x, src, t, words, o = {}) {
  const r = await put(x, { op: "cmdMovePieces", runId: x.ctx.runId, from: { sheetId: src.id, layoutBase: num(src.layoutRev), contentBase: num(src.contentRev) }, to: { sheetId: t.id, layoutBase: num(t.layoutRev), contentBase: num(t.contentRev) }, by: x.cmd.by || "", why: words }, `move|${src.id}|${t.id}|${num(src.contentRev)}|${num(t.contentRev)}`);
  if (err(r)) return no(err(r));
  let gone = "";
  if (o.draft && num(r.from && r.from.left) === 0) { const a = await put(x, { op: "archiveEmptySheet", id: src.id, runId: x.ctx.runId }, `archive|${src.id}`); gone = err(a) ? `${nameOf(src)} stays, empty (${err(a)})` : `${nameOf(src)} was left empty and archived`; if (!err(a)) { const re = await require("./_charmNestCombine").renumberAfter(x.ctx, src, k => `${x.ctx.runId}|cmd|${x.cmd.id}|${k}`); if (re.length) gone += `; ${re.join(", ")}`; } }
  /* NOWHOLE (Paul, 21 Nov 2026: "nothing can rewrite an entire sheet"): after a merge the pieces already placed on the target stay where they lie; the moved ones are added around them */
  const s = await startNest(x, t.id, false, `${words}, asked by ${x.cmd.by || "a person"}`); if (s.error) return no(`The pieces were moved onto ${nameOf(t)}, but its nest could not start: ${s.error}`);
  return wait(`Nesting ${r.moved} piece(s) on ${nameOf(t)}…`, { stage: "nest", sheets: [{ id: t.id, layoutRev: num(r.to.layoutRev), at: Date.now() }], moved: (src.poolIds || []).map(String), from: nameOf(src), to: nameOf(t), n: r.moved, gone });
}
async function moveSettled(x, empty) {
  return settleNests(x, `Nesting on ${x.work.to}…`, async () => {
    const sheets = await runSheets(x.ctx), on = listedBy(sheets), ids = x.work.moved || [], at = new Map();
    let onT = 0; for (const id of ids) { const s = on.get(String(id)); if (!s) continue; if (nameOf(s) === x.work.to && seated(s, id)) onT++; else at.set(nameOf(s), (at.get(nameOf(s)) || 0) + 1); }
    const rest = [...at].filter(([nm]) => nm !== x.work.to).map(([nm, n]) => `${n} on ${nm}`);
    return ok(`${x.work.n} piece(s) of ${x.work.from} moved onto ${x.work.to}: ${onT} placed there${rest.length ? `; ${empty ? "the rest could not be placed there and went on" : "the rest went on"}: ${rest.join(", ")}` : ""}${x.work.gone ? `; ${x.work.gone}` : ""}`);
  });
}
async function consolidate(x) {
  /* COMBINE10K14K (Paul, 21 Nov 2026: "allow that but only for the 10k and 14 K under the manual options menu … Nothing can engage this automatically … only the user can do this inside
     the options modal"): the ONLY combine is a person's drag of one 10K / 14K run sheet onto another in the options window (via "options", a person's name). Gold Filled / Sterling Silver
     sheets are never combined by a request (NOMERGE), and nothing in the cloud sends this request */
  if (!SOLID.has(x.a.metal) || x.a.via !== "options" || !String(x.cmd.by || "").trim() || /^cloud$/i.test(String(x.cmd.by).trim())) return no("Combining sheets is a person's choice, for 10K and 14K Gold only, in the options window");
  if (!lane(x.ctx, "nest")) return no("Nesting is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (x.work.stage) return moveSettled(x, false);
  const src = await sheetRec(x.ctx, x.a.sourceId), t = await sheetRec(x.ctx, x.a.targetId);
  if (!src || !t || src.id === t.id || src.metal !== t.metal) return no("Pick two sheets of one metal.");
  if (!SOLID.has(src.metal) || src.metal !== x.a.metal) return no("Combining sheets is for 10K and 14K Gold only.");
  if (FAST.has(src.metal)) {   // (COMBINEDRAFTS: two Gold Filled / Sterling Silver drafts of the run, judged as the cloud's own combine judges them: all the pieces fit, orders whole, pairs kept, no pin)
    if (src.runId !== x.ctx.runId || t.runId !== x.ctx.runId) return no("Both sheets must be drafts of the run the cloud carries.");
    const j = await require("./_charmNestCombine").judgeMove(x.ctx, src.id, t.id);
    /* MERGEALL (Paul, 21 Nov 2026: "take draft 2, 3 and 4 and merge them into one sheet and then whatever's left over you put on the next draft"): a PERSON's Move all onto of two
       Gold Filled / Sterling Silver drafts never waits on a nest, a hold the run left after old nest failures, or the room: both sheets' nests are stopped (a person's stop), every
       piece goes onto the target, the target is nested whole, and the pieces that do not fit go on to the next draft (the sheet writer's overflow). The hard rules stay: cut, a cut
       line, approved, in a set, released full, Manual Control, a person's pin, an order on a third sheet. */
    const SOFT = /is being nested or written|is having its files written|is nesting or saving|is being saved|waits for its next nest|is held by the run|is nesting|has no room|turned pieces away|already had no room/;
    if (!j.ok && !SOFT.test(String(j.why || ""))) return no(`Not combined: ${j.why}`);
    const live = r => !!(r && r.nestJob && ["queued", "running", "starting"].includes(String(r.nestJob.status || "")));
    /* (MERGEWAIT, 21 Nov 2026: a sheet whose files are being written, or a nest started after the first stop (another request's), is waited for too, never refused: every live
       nest is stopped at each pass (once per job), and the move goes once both sheets are still) */
    const still = r => !live(r) && !busyRec(r, Date.now());
    if (!still(src) || !still(t)) {
      const now = Date.now(), first = num(x.work.mergeStopAt) || now;
      for (const r of [src, t]) { const id = r.nestJob && (r.nestJob.id || r.nestJob.jobId); if (id && live(r)) await put(x, { op: "stopJob", id: String(id), for: "person", by: x.cmd.by || "", why: "Move all onto: the sheets are merged, then nested whole" }, `mergestop|${id}`); }
      const who = !still(src) ? src : t;
      if (now - first > 15 * 60000) return no(`Not combined: ${nameOf(who)} ${busyRec(who, now) || "is still nesting"} after 15 minutes; press Move all onto again in a moment`);
      return wait(`Waiting for ${nameOf(who)} to stop (${busyRec(who, now) || "nesting"}), then moving every piece…`, Object.assign({}, x.work, { mergeStopAt: first }));
    }
    const moved = await moveAndNest(x, src, t, j.ok ? "Combine drafts" : "Move all onto (what does not fit goes on to the next draft)", { draft: true });
    // (MERGEWAIT: a nest or a file write that began between the look above and the move is waited for too, within the same 15 minutes, never a refusal)
    const busyNow = moved && moved.done && moved.done.ok === false && /having its files written|is nesting|is being saved|is nesting or saving/.test(String(moved.done.text || ""));
    if (busyNow && Date.now() - (num(x.work.mergeStopAt) || Date.now()) <= 15 * 60000) return wait("Waiting for the sheets to be still, then moving every piece…", Object.assign({}, x.work, { mergeStopAt: num(x.work.mergeStopAt) || Date.now() }));
    return moved;
  }
  if (!SOLID.has(src.metal)) return no("Sheets are combined by hand for 10K and 14K Gold and for Gold Filled / Sterling Silver drafts only.");
  return moveAndNest(x, src, t, "Merge sheets");
}
async function empty(x) {
  if (!lane(x.ctx, "nest")) return no("Nesting is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (x.work.waitingMove) return settleNests(x, "Nesting the moved pieces on " + (x.work.toName || "the next draft") + "…", async late => {
    const rec = await sheetRec(x.ctx, x.work.sheets[0].id); if (!rec) return no("That draft is no longer in the cloud.");
    return late ? no(`${x.work.n} piece(s) were moved onto ${nameOf(rec)}, but its nest did not finish (${rec.status || "waiting"})`) : ok(`${x.work.n} waiting piece(s) moved onto ${nameOf(rec)}: ${placedWords(rec)}`);
  });
  if (x.work.stage) return moveSettled(x, true);
  if (Array.isArray(x.a.ids) && x.a.ids.length && x.a.targetId) return moveWaiting(x);
  const src = await sheetRec(x.ctx, x.a.sheetId); if (!src || src.runId !== x.ctx.runId) return no("That sheet is not here.");
  if (FAST.has(src.metal)) {   // (COMBINEDRAFTS: a Gold Filled / Sterling Silver draft's pieces go whole onto the oldest other draft of the run that takes them all)
    if (!activeCharms(src).length) return no(`${nameOf(src)} has no pieces`);
    const f = await require("./_charmNestCombine").targetsFor(x.ctx, src.id);
    if (f.source) return no(`Not emptied: ${f.why[0] || "that draft takes no part"}`);
    if (!f.targets.length) return no(`${nameOf(src)} has no other draft that can take all its pieces${f.why.length ? `: ${f.why[0]}` : ""}`);
    return moveAndNest(x, src, f.targets[0], "Empty draft", { draft: true });
  }
  if (!SOLID.has(src.metal)) return no("Sheets are emptied by hand for 10K and 14K Gold and for Gold Filled / Sterling Silver drafts only.");
  if (!activeCharms(src).length) return no(`${nameOf(src)} has no pieces`);
  const sets = await runSets(x.ctx), ord = inOrder(await runSheets(x.ctx, src.metal)), i = ord.findIndex(p => p.id === src.id), now = Date.now();
  const cand = ord.slice(0, Math.max(0, i)).concat(ord.slice(i + 1)).filter(p => p.id !== src.id && !cutRec(p) && !lineDrawn(p) && !inAnySet(p, sets) && !busyRec(p, now) && !(p.manualEdit && now - num(p.manualEdit.at) < 90000));
  if (!cand.length) return no(`${nameOf(src)} has no other sheet that can take its pieces.`);
  /* (MOVETO, Paul 21 Nov 2026: "move the 10K piece from Draft 7 to Draft 4"): a target the person names is used, when it may take the pieces; none named: the first, as before */
  if (x.a.targetId) {
    const named = cand.find(p => p.id === x.a.targetId), tn = ord.find(p => p.id === x.a.targetId);
    if (!named) return no(`${tn ? nameOf(tn) : "That sheet"} cannot take ${nameOf(src)}'s pieces now (cut, a green line drawn, in a set, busy or open by hand)`);
    return moveAndNest(x, src, named, "Empty sheet");
  }
  return moveAndNest(x, src, cand[0], "Empty sheet");
}
/* MOVEWAITING (Paul, 21 Nov 2026: "Put the 15 pieces on the next draft"): Empty with pool ids and a target: those pieces of the sheet that wait UNPLACED (never one already placed)
   go onto the target draft of the same metal, whole orders only (an order with a piece placed on the source, or a piece not named, stays), by the sheet writer's own move
   (finishOverflow: one transaction, the target's own checks), and the target nests them around its placed pieces. */
async function moveWaiting(x) {
  const src = await sheetRec(x.ctx, x.a.sheetId), t = await sheetRec(x.ctx, x.a.targetId), now = Date.now();
  if (!src || src.runId !== x.ctx.runId || !t || t.runId !== x.ctx.runId) return no("Pick two sheets of the run.");
  if (src.metal !== t.metal || src.id === t.id) return no("Pick two sheets of one metal.");
  const named = new Set(x.a.ids.map(String)), placed = new Set((src.placements || []).map(p => p && String(p.id))), ordOf = c => String((c && c.order) || "").split("/")[0];
  const live = activeCharms(src), placedOrders = new Set(live.filter(c => placed.has(String(c.id))).map(ordOf));
  /* REUNITE (Paul, 22 Nov 2026: "check all earrings … ensure all of them have the required pairs"): named pieces that are each one half of a pair whose other half lies on the
     target go to it, placed or not (cmdMovePieces with those pool ids: their place on this sheet is let go), and the target nests them beside their halves */
  const tGroups = new Set((t.charms || []).filter(c => c && !c.excluded && c.groupKey).map(c => String(c.groupKey)));
  const halves = live.filter(c => named.has(String(c.poolId)) && c.groupKey && tGroups.has(String(c.groupKey)));
  if (halves.length && halves.length === named.size) {
    const r = await put(x, { op: "cmdMovePieces", runId: x.ctx.runId, from: { sheetId: src.id, layoutBase: num(src.layoutRev), contentBase: num(src.contentRev) }, to: { sheetId: t.id, layoutBase: num(t.layoutRev), contentBase: num(t.contentRev) },
      poolIds: halves.map(c => String(c.poolId)), by: x.cmd.by || "", why: "a pair put back together" }, `reunite|${src.id}|${num(src.contentRev)}|${t.id}|${num(t.contentRev)}`);
    if (err(r)) return no(`Not moved: ${err(r)}`);
    const s = await startNest(x, t.id, false, `the other half of a pair moved here by ${x.cmd.by || "a person"}`);
    if (s.error) return no(`${r.moved} piece(s) were moved onto ${nameOf(t)}, but its nest could not start: ${s.error}`);
    return wait(`Nesting the pair's other half on ${nameOf(t)}…`, { waitingMove: true, stage: "nest", n: r.moved, toName: nameOf(t), sheets: [{ id: t.id, layoutRev: num(t.layoutRev) + 1, at: now }] });
  }
  const waiting = live.filter(c => c.poolId && !placed.has(String(c.id)));
  const unnamed = new Set(waiting.filter(c => !named.has(String(c.poolId))).map(ordOf));
  const going = waiting.filter(c => named.has(String(c.poolId)) && !placedOrders.has(ordOf(c)) && !unnamed.has(ordOf(c)));
  if (!going.length) return no(`None of those pieces can move: they are placed, or their order has a piece that stays on ${nameOf(src)}`);
  const r = await put(x, { op: "finishOverflow", id: src.id, runId: x.ctx.runId, layoutBase: num(src.layoutRev), contentBase: num(src.contentRev), pieceIds: going.map(c => String(c.id)),
    why: `moved by ${x.cmd.by || "a person"}`, to: { sheetId: t.id, layoutBase: num(t.layoutRev), contentBase: num(t.contentRev) } }, `movewaiting|${src.id}|${num(src.contentRev)}|${t.id}|${num(t.contentRev)}`);
  if (err(r)) return no(`Not moved: ${err(r)}`);
  if (!r.moved) return no("Nothing was moved (those pieces were already moved or taken off)");
  const s = await startNest(x, t.id, false, `${r.moved} waiting piece(s) moved here by ${x.cmd.by || "a person"}`);
  if (s.error) return no(`${r.moved} piece(s) were moved onto ${nameOf(t)}, but its nest could not start: ${s.error}`);
  return wait(`Nesting ${r.moved} moved piece(s) on ${nameOf(t)}…`, { waitingMove: true, stage: "nest", n: r.moved, toName: nameOf(t), sheets: [{ id: t.id, layoutRev: num(t.layoutRev) + 1, at: now }] });
}
async function seat(x) {
  if (!lane(x.ctx, "nest") || !has("nestStart") || !has("roseClaim")) return no("Moving a sheet onto a partial sheet is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (x.work.stage) return settleNests(x, "Nesting the pieces…", async late => {
    const rec = await sheetRec(x.ctx, x.work.sheets[0].id); if (!rec) return no("The sheet is no longer in the cloud.");
    const n = x.work.n, k = (rec.placements || []).length;
    const text = `${x.work.name}'s ${k === n ? `${n} ${n === 1 ? "piece now lies" : "pieces now lie"}` : `${k} of ${n} pieces now lie`} on ${x.work.label}.${k < n ? ` ${n - k} went on to the next sheet or stay to be placed.` : ""}${x.work.freed ? ` The sheet ${x.work.name} lay on before is back in the repository as Available: keep it or delete it.` : ""}`;
    return late ? no(`the nest did not finish: ${text}`) : { done: Object.assign({ ok: true, text }, x.work.freed ? { freed: x.work.freed } : {}) };
  });
  const p = await sheetRec(x.ctx, x.a.sheetId), now = Date.now(); if (!p || p.runId !== x.ctx.runId) return no("That sheet is not here.");
  if (!CutLine.cuts(p.metal)) return no("Partial sheets are for Rose Gold, 10K Gold and 14K Gold sheets.");
  if (num(p.laserDoneAt) > 0) return no("This sheet is Completed (laser cut): its metal has been cut, so its pieces cannot move to another partial sheet.");
  if (busyRec(p, now)) return no("This sheet is nesting or saving right now. Wait a moment, then choose the partial sheet.");
  const n = activeCharms(p).length; if (!n) return no("This sheet has no pieces to move yet.");
  const ids = (Array.isArray(x.a.partialIds) && x.a.partialIds.length ? x.a.partialIds : [x.a.partialId]).filter(Boolean).map(String).slice(0, 8);
  if (!ids.length) return no("Choose the sheet to move onto.");
  // (Paul 21 Nov 2026, item 11: the id a person picks is the PARTIAL SHEET's record (Charm_Nest_Remnants/<stockId>-<revision>), not its physical sheet: the stock is the one that
  //  record names, claimed with the record, as the page's partialClaim does; an id with no record is taken as a physical sheet, as before)
  const rs = await x.ctx.db.collection("Charm_Nest_Remnants").doc(ids[0]).get(), rem = rs.exists ? rs.data() || {} : null, stockId = rem && rem.stockId ? String(rem.stockId) : ids[0];
  if (rem ? rem.status === "inUse" && rem.inUseBySheetId === p.id : ids[0] === p.roseStockId) return no("This sheet already sits on that partial sheet.");
  const ss = await x.ctx.db.collection(STOCK).doc(stockId).get(), st = ss.exists ? ss.data() || {} : null;
  if (!st || (st.metal && st.metal !== p.metal) || (st.status && st.status !== "available" && st.owner !== p.id)) return no("That partial sheet is not available any more. Choose another.");
  const r = await put(x, { op: "roseClaim", sheetId: p.id, metal: p.metal, wPt: st.wPt, hPt: st.hPt, stockId, revision: rem ? +rem.revision : st.revision, nesting: true, exact: true, partialId: ids[0], swap: !!(p.roseStockId || p.roseCutAt || p.rosePlanJson || p.roseProtectedJson) }, `seat|${p.id}|${ids[0]}`);
  if (err(r) || !r.stock) return no("That partial sheet could not be reserved: " + (err(r) || "no answer"));
  if (ids.length > 1) await put(x, { op: "putSheet", sheet: { id: p.id, partialChain: ids.slice(1) } }, `chain|${p.id}|${ids.join(",")}`);
  const s = await startNest(x, p.id, true, `moved onto a partial sheet, asked by ${x.cmd.by || "a person"}`); if (s.error) return no(`The partial sheet is reserved, but the nest could not start: ${s.error}`);
  const mm = v => Math.round(+v / 72 * 25.4 * 10) / 10, label = st.wPt && st.hPt ? `the partial sheet ${mm(st.wPt)} × ${mm(st.hPt)} mm` : "the partial sheet you chose";
  return wait(`${nameOf(p)}'s ${n} pieces are being nested on the sheet you chose`, { stage: "nest", sheets: [{ id: p.id, layoutRev: num(p.layoutRev), at: now }], name: nameOf(p), n, label, freed: p.roseStockId && p.roseStockId !== stockId ? p.roseStockId : null });
}
/* ═══ by hand: the sheet hop (ITEM 13, Paul 21 Nov 2026) ═══ a charm dragged onto the next or previous 10K / 14K sheet on any computer: ONE request. Its pieces go to the target
   where they were put (cmdHopPieces: pinned there, every other piece of both sheets where it is, both owing their files, no nest); a green line on either sheet is set aside first
   (roseReopen) and drawn again once the files are written (roseRedraw). A dry run first, so no line is set aside for a move that would be refused. */
async function hop(x) {
  const a = x.a, by = x.cmd.by || "", froms = [...new Set([a.sourceId].concat(a.sourceIds || []).filter(Boolean))].slice(0, 4);
  const body = { op: "cmdHopPieces", runId: x.ctx.runId, from: froms, to: Object.assign({ sheetId: a.targetId }, typeof a.layoutBase === "number" ? { layoutBase: a.layoutBase } : {}), poses: a.poses || [], free: a.free === true, ...(a.fill === true ? { fill: true } : {}), by, person: by };   // (fill: GAP G1, the sheet window's Fill the freed room / Place from a viewer)
  const dry = await put(x, Object.assign({}, body, { dry: true }), "hop-dry"); if (err(dry)) return no(`Not moved: ${err(dry)}`);
  const redraw = [];
  for (const id of [a.targetId].concat(froms)) {
    const r = await sheetRec(x.ctx, id); if (!r || !SOLID.has(r.metal) || !(r.roseCutAt || lineDrawn(r) || r.rosePlanHash)) continue;
    const g = await put(x, { op: "roseReopen", sheetId: id, by: by || undefined }, `hop-reopen|${id}`); if (err(g)) return no(`Not moved: ${nameOf(r)}'s green line could not be set aside (${err(g)})`);
    redraw.push({ sheetId: id, cut: !!r.roseCutAt });
  }
  const r = await put(x, Object.assign({}, body, { redraw }), "hop");
  if (err(r)) { for (const z of redraw) await put(x, { op: "putSheet", sheet: { id: z.sheetId, roseRedraw: { at: Date.now(), by, line: true, cut: z.cut }, filesOwed: Date.now() } }, `hop-back|${z.sheetId}`); return no(`Not moved: ${err(r)}`); }
  const refiled = await hopRefile(x, [a.targetId].concat(froms));
  const rid = [...new Set((a.poses || []).map(p => ridOf(p.poolId)).filter(Boolean))].join(", "), n = r.moved, src = (r.from || []).map(f => f.name);
  const sets = refiled.length ? ` ${and(refiled.map(z => z.name))} ${refiled.length === 1 ? "was" : "were"} already approved: ${refiled.every(z => z.ok) ? `${refiled.length === 1 ? "its" : "their"} QR labels, labels PDF, manifest and set.json are written again${lane(x.ctx, "labels") ? "" : " once the cloud's labels lane is on"}` : `not all asked again (${refiled.filter(z => !z.ok).map(z => `${z.name}: ${z.why}`).join("; ")})`}.` : "";
  // (GAP G1: the freed room filled from a viewer: the same move, said as what the person pressed)
  if (a.fill === true) return ok(`${rid.includes(",") ? "Orders" : "Order"} ${rid}: ${n === 1 ? "the charm" : n + " pieces"} moved from ${and(src)} into the freed room on ${r.to.name}, where ${n === 1 ? "it was" : "they were"} planned; every other charm stayed where it was and no sheet was nested again. ${lane(x.ctx, "files") ? "The cloud writes the sheets' files again" : "The sheets owe their files"}${redraw.length ? "; the green line is drawn again once they are written" : ""}.${sets}`);
  return ok(`Order ${rid}: ${n === 1 ? "the charm" : n + " pieces"} moved from ${and(src)} to ${r.to.name} where ${n === 1 ? "it was" : "they were"} put; every other charm stayed where it was. ${lane(x.ctx, "files") ? "The cloud writes both sheets' files again" : "Both sheets owe their files"}${redraw.length ? "; the green line is drawn again once they are written" : ""}.${sets}${r.free ? ` ${n === 1 ? "It does" : "They do"} not fit there yet: red until resized or moved clear.` : ""}`);
}
/* GAP G2 (Paul 21 Nov 2026): after a hop, a set already approved (approved for laser cutting, its labels made: released) whose sheet changed has its files written again, as the page's
   hop did (sheetwin goldAfter → Review.inPlaceRefile). The cloud's way: ONE labelsAsk { kind: "setFiles" } a set, answered by the labels pass item (_charmNestLabels.js doAsk: each
   sheet's QR label that no longer says its orders is drawn again, then the labels PDF, manifest and set.json, status kept), behind the Etsy freshness gate as every setFiles ask.
   The sheet's own card says its set changed once its files are written (the files lane, _charmNestFinish.js S8 releasedChange). A set sent to the laser station never gets here
   (cmdHopPieces refuses a hop on its sheets). → [{ setId, name, ok, why }] */
const releasedSet = (s, rec) => !!(s && (s.committedAt || s.processReady || s.laserSetPending || s.status === "labelled" || (s.labels && s.labels.pdf) || (rec && (rec.processReady || rec.laserSetPending))));
async function hopRefile(x, ids) {
  const out = [];
  let sets = []; try { sets = (await runSets(x.ctx)).filter(s => !/^superseded/.test(String(s.status || ""))); } catch (_) { sets = []; }
  for (const id of [...new Set(ids.filter(Boolean).map(String))]) {
    const rec = await sheetRec(x.ctx, id).catch(() => null); if (!rec || rec.archived) continue;
    const s = sets.find(z => (z.sheetIds || []).map(String).includes(id)) || (rec.setId && !rec.draft ? sets.find(z => z.setId === rec.setId) : null);
    if (!s || out.some(z => z.setId === s.setId) || !releasedSet(s, rec)) continue;
    const name = SetEdit && SetEdit.setWord ? SetEdit.setWord(s) : s.name || (s.seq ? `Set ${s.seq}` : "Its set");
    const q = await put(x, { op: "labelsAsk", kind: "setFiles", setId: s.setId, by: x.cmd.by || "Cloud" }, `hop-setfiles|${s.setId}`);
    out.push({ setId: s.setId, name, ok: !err(q) && !(q && q.error), why: err(q) || str(q && q.error, 200) });
  }
  return out;
}
/* CUSTOMSEND (Paul, 21 Nov 2026: Send to Sheet and Monogram Approve on a staff computer in mode "on"): a card's sent designs made into pieces and placed BY THE CLOUD, the port of the
   carrier's applyRemote (charm-nest-bridge.js). The send is read from the cloud's own record (Charm_Custom_Sheet, phase "sent"); each of its open lines whose pieces are not these designs
   yet has its old pieces taken off their sheet (cmdVacate: the pieces' records say superseded; the kept place is let go at once, cmdPutBack release) and goes back to the intake
   (pulled, its hold lifted: the send IS the release), which makes its pieces from the card's designs (_charmNestCustomMake.js), and the placement unit puts them on a sheet. Answered
   done once every new piece is listed on a sheet; refused in words when a sheet keeps what it holds (cut, behind a green line, in a set sent to the station) or a design cannot be made.
   Asking twice changes nothing: a line whose pieces already are these designs is left as it is. */
const CUSTOM_MS = 12 * 60000, TAKEN = new Set(["abandoned", "superseded"]);
async function customsend(x) {
  const ctx = x.ctx, w = x.work, now = Date.now(), ck = str(x.a.ck, 200);
  if (!lane(ctx, "nest") || !lane(ctx, "pool")) return no("A card's sent designs are made and placed by the cloud's intake and placement, which are not on yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then send it again");
  if (!w.stage) {
    if (!ck) return no("No card was named.");
    const got = await ctx.read({ op: "customSheetGet", keys: [ck] }).catch(() => null), rec = got && got.records && got.records[ck];
    if (!rec || !rec.sent || !rec.sent.lines || rec.phase !== "sent") return no("The send is not recorded in the cloud yet: send it again");
    const L = lines(ctx), keys = Object.keys(rec.sent.lines).filter(k => (rec.sent.lines[k] || []).some(pc => pc && !pc.removed)), rid = String(rec.rid || ridOf(keys[0]) || "");
    const cc = await ctx.read({ op: "cancelCheck", orderIds: [rid] }).catch(() => null), cx = cc && (cc.cancelled || cc.orders);
    if (Array.isArray(cx) ? cx.map(String).includes(rid) : !!(cx && typeof cx === "object" && cx[rid])) return no(`Order ${rid} was cancelled.`);
    const open = keys.filter(k => OPEN_LINE(L[k]));
    if (!open.length) return no(`Order ${rid} is not in the run the cloud carries.`);
    const paths = new Set((rec.files || []).map(F => F && F.cloud && F.cloud.path).filter(Boolean));
    const sheets = await runSheets(ctx), sets = await runSets(ctx), on = listedBy(sheets), patch = {};
    let swapped = 0;
    for (const k of open) {
      const l = L[k], ids = (l.poolIds || []).map(String), rows = ids.length ? await pools(ctx, ids) : new Map();
      if (ids.length && ids.every(id => { const r = rows.get(id); return r && r.custom && !TAKEN.has(r.state) && paths.has(String(r.aiPath || "")); })) continue;   // (already these designs)
      const bySheet = new Map(); for (const id of ids) { const s = on.get(id); if (s) (bySheet.get(s) || bySheet.set(s, []).get(s)).push(id); }
      const keeps = [...bySheet.keys()].find(s => cutRec(s) || lineDrawn(s) || committedSheet(s, sets));
      if (keeps) return no(`Order ${rid}: ${nameOf(keeps)} ${cutRec(keeps) ? "was already cut" : lineDrawn(keeps) ? "is behind a green line" : "is in a set sent to the station"}: its piece stays as it is and the new design was not placed`);
      const busy = [...bySheet.keys()].find(s => busyRec(s, now));
      if (busy) { if (now - (num(x.cmd.at) || now) > CUSTOM_MS) return no(`Order ${rid}: ${nameOf(busy)} ${busyRec(busy, now)}; nothing was changed`); return wait(`Waiting for ${nameOf(busy)} to be free…`, {}); }
      for (const [s, part] of bySheet) {
        const r = await put(x, { op: "cmdVacate", runId: ctx.runId, lineKey: k, rid, sheetId: s.id, poolIds: part, layoutBase: num(s.layoutRev), contentBase: num(s.contentRev), by: x.cmd.by || "", why: "Send to Sheet: the sent design replaces it" }, `cs-vacate|${k}|${s.id}|${num(s.contentRev)}`);
        if (err(r)) return no(`Order ${rid}: the old piece could not be taken off ${nameOf(s)}: ${err(r)}`);
        await put(x, { op: "cmdPutBack", runId: ctx.runId, lineKey: k, sheetId: s.id, release: true }, `cs-release|${k}|${s.id}`);
        swapped += part.length;
      }
      patch[k] = { state: "pulled", poolIds: [], repoolChanged: true, wait: null, reason: null, hold: null, vacated: null };
    }
    if (Object.keys(patch).length) { const r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: patch }, `cs-lines|${Object.keys(patch).sort().join(",").slice(0, 400)}`); if (err(r)) return no(`Order ${rid}: not sent: ${err(r)}`); }
    return wait("Making the new pieces from the sent designs…", { stage: "placing", keys: open, rid, swapped, at: now });
  }
  if (w.stage === "placing") {
    // (its own lines only, now: a person's send is not a new order waiting at the intake's gate (run mode, the run's step); the intake makes them from the card's designs and the placement puts them on a sheet)
    const got = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), fresh = got && got.run;
    if (fresh && fresh.lines) {
      const only = new Set(w.keys || []), run = Object.assign({}, fresh, { runId: ctx.runId });
      const Intake = require("./_charmNestIntake"), PlaceUnit = require("./_charmNestPlaceUnit");
      await Intake.pool({ db: ctx.db, run, runId: ctx.runId, call: ctx.call, read: ctx.read, drive: ctx.drive || {}, cloudSteps: ctx.cloudSteps || [], fence: ctx.fence, say: ctx.say || (() => {}), budgetEnd: num(ctx.budgetEnd) || Date.now() + 45000, settings: null, stockPreset: null }, { step: true, only });
      await PlaceUnit.placeAll(Object.assign({}, ctx, { run, only }));
    }
    const back = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), L = (back && back.run && back.run.lines) || lines(ctx), on = listedBy(await runSheets(ctx));
    let pieces = 0, listed = 0; const names = new Set(), stuck = [];
    for (const k of w.keys || []) {
      const l = L[k]; if (!l || !OPEN_LINE(l)) continue;
      if (["held", "oversize", "unmatched", "noDesign"].includes(l.state) && !(l.poolIds || []).length) { stuck.push(String(l.reason || l.state)); continue; }
      for (const id of l.poolIds || []) { pieces++; const s = on.get(String(id)); if (s) { listed++; names.add(nameOf(s)); } }
      if (!(l.poolIds || []).length) pieces++;
    }
    const off = w.swapped ? ` (the old piece${w.swapped === 1 ? " was" : "s were"} taken off)` : "";
    if (stuck.length) return no(`Order ${w.rid}: the new piece is not on a sheet: ${stuck[0]}`.slice(0, 380));
    if (pieces && listed === pieces) return ok(`${pieces === 1 ? "its new piece is" : `its ${pieces} new pieces are`} on ${and([...names])}${off}`);
    if (now - num(w.at) > CUSTOM_MS) return no(`Order ${w.rid}: ${listed} of ${pieces} new piece(s) on a sheet so far; the rest go on with the run's next placement`);
    return wait(listed ? `Placing the new pieces… ${listed} of ${pieces} on a sheet` : "Making the new pieces from the sent designs…", w, pieces ? Math.round(100 * listed / pieces) : null);
  }
  return no("The request lost its place: send it again");
}

/* ═══ RGRESTORE (Paul, 22 Nov 2026) ═══ restorenest { sheetId, sourceId: the nest (Charm_Nest_Jobs id) whose layout goes back, fromId: the draft its pieces wait on now, targetId?: the sheet
   that takes the draft's other pieces (the ones that nest did not place) }. RG Sheet 1 held 29 pieces after nest-8462e548578a82c3789dfc62; the writer's ONCEWHOLE bug lifted 11 of them onto a
   new draft (RG Draft 2) with the remade BULLSEYE (which truly has no room there); the draft took the leftover of Sheet 1's automatic cut, and its "oldest order cannot fit" stopped the run.
   ONE request, each step looking at the records first (a pass cut off midway, or the request asked twice, does each thing once), said in the answer:
     check  cmdRestoreNest dry: every refusal in words BEFORE anything is written (laser cut, the set sent to the station, the sheet's pieces not where that nest left them (they could overlap),
            the physical sheet held or cut by a third sheet, a piece taken off or committed, a pair split, Manual Control). A sheet's live nest is stopped and a busy sheet waited for (FIRSTNOW)
     rest   the draft's pieces that nest did not place go to targetId (cmdMovePieces) and are nested there round what it holds (nestStart, append)
     stock  the draft gives the physical sheet back (roseRelease)
     aside  the sheet's recorded cut is set aside (roseReopen, replace): the stock is the sheet's again at the outline it had before that cut, the cut's leftover is discarded
     back   cmdRestoreNest: the pieces back exactly where the nest left them; the sheet owes its files, then its green line (roseRedraw, the rosecut lane), then its cut (its own automatic
            cut, or a person's Cut Sheet); the draft emptied whole (its problem and the run stop it asked go)
     gone   the empty draft archived (archiveEmptySheet), the later drafts of its metal numbered again; the lines brought back are "written" when the sheet is in a set (as its set's others)
   Then it waits for the target's nest and answers. Never Etsy; the run's stop is left as it is (its Resume is the owner's press). */
const RESTORE_MS = 15 * 60000;
async function restorenest(x) {
  const ctx = x.ctx, a = x.a, by = x.cmd.by || "", now = Date.now(), w = Object.assign({}, x.work, { did: (x.work.did || []).slice(), why: (x.work.why || []).slice() });
  const NEED = ["cmdRestoreNest", "cmdMovePieces", "roseRelease", "roseReopen", "archiveEmptySheet", "nestStart", "runLinesPatch"];
  if (!lane(ctx, "nest") || !NEED.every(has)) return no("Putting a nest's layout back is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (w.stage === "nest") return restoreSettled(x, w);
  const sid = str(a.sheetId, 80), fid = str(a.fromId, 80), tid = str(a.targetId, 80), job = str(a.sourceId, 80), tries = `t${num(w.tries)}`;
  let S = await sheetRec(ctx, sid), D = fid ? await sheetRec(ctx, fid) : null, T = tid ? await sheetRec(ctx, tid) : null;
  if (!S || S.archived || S.runId !== ctx.runId) return no("That sheet is not on the run the cloud carries.");
  if (!D || D.runId !== ctx.runId) return no("The draft its pieces wait on is not on the run the cloud carries.");
  const js = job ? await ctx.db.collection("Charm_Nest_Jobs").doc(job).get() : null, J = js && js.exists ? js.data() || {} : null, P = J && J.result && Array.isArray(J.result.placements) ? J.result.placements : [];
  if (!J || J.sheetId !== S.id || J.status !== "done" || !P.length) return no(`That nest has no finished layout of ${nameOf(S)}: nothing was put back`);
  const placedIds = new Set(P.map(p => String(p && p.id))), onS = new Set(activeCharms(S).map(c => String(c.id)));
  const pending = [...placedIds].filter(id => !onS.has(id)), rest = D.archived ? [] : activeCharms(D).filter(c => c.poolId && !placedIds.has(String(c.id)));
  const said = () => (w.did.length ? ` (done so far: ${w.did.join("; ")})` : "");
  const again = (why, rec) => {   // (FIRSTNOW: a busy sheet is waited for, 15 minutes at most; nothing that cannot wait is left half done)
    const first = num(w.firstNowAt) || now;
    if (now - first > RESTORE_MS) return no(`Not put back: ${nameOf(rec)} ${why} after 15 minutes; ask again${said()}`);
    return wait(`Putting ${nameOf(S)}'s pieces back as soon as ${nameOf(rec)} is still (${why})…`, Object.assign({}, w, { firstNowAt: first, tries: num(w.tries) + 1 }));
  };
  if (pending.length || rest.length) {
    const live = [S, D, T].filter(r => r && !r.archived), busy = live.find(r => liveNest(r) || busyRec(r, now));
    if (busy) { await stopFor(x, live, `${nameOf(S)}: its layout put back (a person's request goes first)`); return again(busyRec(busy, now) || "is nesting", busy); }
  }
  if (rest.length && (!T || T.archived || T.id === D.id || T.id === S.id)) return no(`${nameOf(D)} also holds ${plural(rest.length, "piece")} that nest did not place (${rest.slice(0, 3).map(c => c.name || c.poolId).join(", ")}): name the sheet that takes ${rest.length === 1 ? "it" : "them"}. Nothing was changed`);
  // check: every refusal before anything is written
  if (pending.length) {
    const dry = await put(x, { op: "cmdRestoreNest", runId: ctx.runId, sheetId: S.id, jobId: job, from: [D.id], by, person: by, dry: true }, `restore-dry|${S.id}|${num(S.layoutRev)}.${num(S.contentRev)}|${D.id}|${num(D.layoutRev)}.${num(D.contentRev)}|${S.roseCutAt ? 1 : 0}|${tries}`);
    if (err(dry)) return dry.busy ? again(err(dry).replace(/^.*? (is|has) /, "$1 ").replace(/: try again.*$/, ""), S) : no(`Not put back: ${err(dry)}${said()}`);
  }
  // rest: the draft's other pieces to the target, nested there
  if (rest.length) {
    const m = await put(x, { op: "cmdMovePieces", runId: ctx.runId, from: { sheetId: D.id, layoutBase: num(D.layoutRev), contentBase: num(D.contentRev) }, to: { sheetId: T.id, layoutBase: num(T.layoutRev), contentBase: num(T.contentRev) },
      poolIds: rest.map(c => String(c.poolId)), by, why: `not part of ${nameOf(S)}'s nest` }, `restore-rest|${D.id}|${num(D.contentRev)}|${T.id}|${num(T.contentRev)}`);
    if (err(m)) return /try again once it is done|changed since this move was planned/.test(err(m)) ? again("is busy", T) : no(`Not put back: ${err(m)}${said()}`);
    w.rest = { n: num(m.moved), to: T.id, toName: nameOf(T), layoutRev: num(m.to && m.to.layoutRev), ids: rest.map(c => String(c.poolId)), names: rest.map(c => String(c.name || c.poolId).replace(/^\d+ · /, "")) };
    w.did.push(`${and(w.rest.names)} moved to ${w.rest.toName}`);
    D = await sheetRec(ctx, D.id);
  }
  if (pending.length) {
    // stock: the draft gives the physical sheet back
    const stockId = str(S.roseStockId, 120), ss = stockId ? await ctx.db.collection(STOCK).doc(stockId).get() : null, stock = ss && ss.exists ? ss.data() || {} : null;
    if (stock && stock.owner === D.id) {
      const r = await put(x, { op: "roseRelease", stockId, sheetId: D.id, metal: D.metal }, `restore-release|${D.id}|${stockId}|${num(stock.revision)}`);
      if (err(r)) return no(`Not put back: ${nameOf(D)} could not give ${nameOf(S)}'s physical sheet back (${err(r)})${said()}`);
      w.did.push(`${nameOf(D)} gave ${nameOf(S)}'s physical sheet back`);
    }
    // aside: the sheet's recorded cut (and any line) set aside
    if (S.roseCutAt || lineDrawn(S) || S.rosePlanHash) {
      const r = await put(x, { op: "roseReopen", sheetId: S.id, replace: true, by: by || undefined }, `restore-aside|${S.id}|${num(S.roseCutAt)}|${S.rosePlanHash || ""}`);
      if (err(r)) return no(`Not put back: ${nameOf(S)}'s cut could not be set aside (${err(r)})${said()}`);
      w.aside = { cut: !!(r && r.cut), revision: r && r.cut ? r.cut.revision : null, leftover: r && r.cut ? r.cut.leftover : null };
      w.did.push(`${nameOf(S)}'s ${w.aside.cut ? "automatic cut" : "green line"} set aside`);
    }
    // back: the pieces where the nest left them
    S = await sheetRec(ctx, S.id); D = await sheetRec(ctx, D.id);
    const r = await put(x, { op: "cmdRestoreNest", runId: ctx.runId, sheetId: S.id, jobId: job, from: [D.id], by, person: by }, `restore-back|${S.id}|${num(S.layoutRev)}.${num(S.contentRev)}|${D.id}|${num(D.layoutRev)}.${num(D.contentRev)}|${tries}`);
    if (err(r)) return r.busy ? again(err(r).replace(/^.*? (is|has) /, "$1 ").replace(/: try again.*$/, ""), S) : no(`Not put back: ${err(r)}${said()}`);
    w.back = { n: num(r.restored), orders: r.orders || [], pieces: r.pieces || [], inSet: !!r.inSet };
    if (w.back.n) w.did.push(`${plural(w.back.n, "piece")} back on ${nameOf(S)}`);
  }
  // gone: the empty draft archived, the later drafts numbered again
  D = await sheetRec(ctx, D.id);
  if (D && !D.archived && !activeCharms(D).length && !(D.poolIds || []).length) {
    const r = await put(x, { op: "archiveEmptySheet", id: D.id, runId: ctx.runId }, `restore-archive|${D.id}`);
    if (err(r)) w.why.push(`${nameOf(D)} is empty but stays (${err(r)})`);
    else { w.gone = nameOf(D); let re = []; try { re = await require("./_charmNestCombine").renumberAfter(ctx, D, k => `${ctx.runId}|cmd|${x.cmd.id}|${k}`); } catch (_) { re = []; } if (re.length) w.why.push(re.join(", ")); }
  }
  // the lines brought back: "written" when the sheet is in a set (every piece of the line on it), as the set's other pieces
  if (w.back && w.back.inSet && w.back.pieces.length && !w.linesDone) {
    const got = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), L = (got && got.run && got.run.lines) || lines(ctx), S2 = await sheetRec(ctx, S.id), on = new Set(((S2 && S2.poolIds) || []).map(String)), mine = new Set(w.back.pieces.map(String));
    const patch = {}; for (const [k, l] of Object.entries(L)) if (l && l.state === "pooled" && (l.poolIds || []).length && l.poolIds.some(id => mine.has(String(id))) && l.poolIds.every(id => on.has(String(id)))) patch[k] = { state: "written" };
    if (Object.keys(patch).length) { const r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: patch }, `restore-lines|${S.id}|${Object.keys(patch).sort().join(",").slice(0, 400)}`); if (err(r)) w.why.push(`its order lines are marked written at the next set look (${err(r)})`); }
    w.linesDone = true;
  }
  // the words, then the target's nest
  const parts = [];
  if (w.back && w.back.n) parts.push(`${nameOf(S)}: ${plural(w.back.n, "piece")} back where ${job} left ${w.back.n === 1 ? "it" : "them"} (${placedIds.size} on it, nothing nested again)${w.aside && w.aside.cut ? "; its automatic cut set aside, its physical sheet its own at its pre-cut outline" : ""}; files, green line and cut follow`);
  else if (!pending.length) parts.push(`${nameOf(S)} holds every piece of ${job} already: it was not changed`);
  if (w.gone) parts.push(`${w.gone} emptied${w.did.some(d => /physical sheet back/.test(d)) ? ", its stock given back" : ""}, removed: the run is no longer stopped by it (press Resume)`);
  if (w.why.length) parts.push(w.why.join("; "));
  w.said = parts.join(". ");
  if (w.rest && w.rest.n && !w.rest.asked) {
    const s2 = await startNest(x, w.rest.to, false, `${plural(w.rest.n, "piece")} moved from ${nameOf(D)}, asked by ${by || "a person"}`);
    w.rest.asked = true;
    if (s2.error) return ok(`${w.said}. ${and(w.rest.names)} ${w.rest.n === 1 ? "waits" : "wait"} on ${w.rest.toName}; its nest could not start yet (${s2.error}): the run nests it`);
    return wait(`Nesting ${and(w.rest.names)} on ${w.rest.toName}…`, Object.assign({}, w, { stage: "nest", sheets: [{ id: w.rest.to, layoutRev: w.rest.layoutRev, at: Date.now() }] }));
  }
  return ok(w.said || "Nothing to put back");
}
async function restoreSettled(x, w) {
  const R = w.rest || {}, want = (R.ids || []).map(String), rec0 = await sheetRec(x.ctx, R.to);
  const tail = (rec, late) => { const n = rec ? want.filter(id => seated(rec, id)).length : 0; return `${w.said}. ${and(R.names || [])} ${n === want.length ? `nested on ${R.toName}: ${placedWords(rec)}` : `${want.length === 1 ? "waits" : "wait"} on ${R.toName}${late ? ` (its nest did not finish: ${(rec && rec.status) || "waiting"})` : ""}`}`; };
  if (rec0 && want.length && want.every(id => seated(rec0, id))) return ok(tail(rec0, false));
  return settleNests(x, `Nesting ${and(R.names || [])} on ${R.toName || "the sheet"}…`, async late => ok(tail(await sheetRec(x.ctx, R.to), late)));
}

const EXEC = { reorder, delete: del, leaveset, setrebuild, unkeep, repicture, placewaiting, pairnow, sendhome, remakeline, keepsync, remake, skuonce, release, renest, consolidate, empty, seat, customsend, hop, backhome, restorenest };
module.exports = { EXEC, strandedFix, placedNow, staleLines, runSheets, runSets, sheetRec, nameOf, cutRec, lineDrawn, listedBy, seated, has, lane, WORK };
