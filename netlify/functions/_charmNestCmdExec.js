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
 *    record only   rgmerge (RGMERGE, 23 Nov 2026: two Rose Gold sheets of one set and one physical sheet made one, every piece where it lies, the green line between them gone)
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
/* NESTOWEDREAD (Paul, 23 Nov 2026: RG Draft 2's Empty onto RG Draft 1 said "Nesting on RG Draft 7…" for its whole 25 minutes although the nest had placed all 21 pieces, verified, two
   minutes in): a sheet's nestOwed mark is the reason a nest was asked, and no nest clears it (_charmNestNest.owedNest reads it as the reason only, so a mark nobody clears never nests a
   sheet over and over). A request read the mark as "a nest is still owed": it waited until it ran out of time, and the requests behind it waited too. What owes a nest is the nest's own
   rule: a piece not placed and not turned away, or a change since the last nest. */
const owesNest = (rec, now) => !!(rec && require("./_charmNestNest").owedNest(rec, null, now || Date.now()).owed);

const SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets", POOL = "Charm_Pool", WORLD_LINES = "Charm_World_Lines", MASTER = "Charm_Master_Index", STOCK = "Charm_Nest_Rose_Stock";
const SOLID = new Set(["gold10k", "gold14k"]); const FAST = new Set(["gold", "silver"]);   // (COMBINEDRAFTS: Gold Filled / Sterling Silver drafts are combined and emptied too)
const OPEN_LINE = l => !!l && !["gone", "committed", "skipped"].includes(String(l.state || "")), LIVE_LINE = l => OPEN_LINE(l) || (!!l && l.state === "committed" && (l.poolIds || []).length > 0);   // (a piece of a set sent to the station is changed as any other until the laser's mark)
const NEST_MS = 25 * 60000, PLACE_MS = 15 * 60000, FILES_MS = 20 * 60000, RELEASE_PLACE_MS = 20 * 60000;
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
/* CMDFAST (Paul, 23 Nov 2026: "You have a budget of 30 sec", "I cannot wait. This is taking too long"): a person's change of one order read every sheet of the run, whole (50 records of hundreds of KB, seconds), at each of
   its steps, only to find the one or two sheets that list the order's pieces. Those sheets are asked for by their pieces (the query placeOnSheet already runs). */
async function sheetsWith(ctx, ids) {
  const list = [...new Set((ids || []).map(String).filter(Boolean))], out = new Map(), parts = [];
  for (let i = 0; i < list.length; i += 30) parts.push(list.slice(i, i + 30));
  for (const s of await Promise.all(parts.map(p => ctx.db.collection(SHEETS).where("poolIds", "array-contains-any", p).get()))) for (const d of s.docs) { const r = Object.assign({ id: d.id }, d.data()); if (r.runId === ctx.runId && !r.archived) out.set(d.id, r); }
  return [...out.values()];
}
/** The run as the cloud holds it, read again after 8 s or after a write of its lines by this request (runChanged): the steps of one request follow each other within a second and read it four times. */
const RUN_FRESH_MS = 8000;
async function runNow(ctx) {
  const c = ctx._runNow; if (c && Date.now() - c.at < RUN_FRESH_MS && Date.now() >= c.at) return c.run;
  const r = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), run = (r && r.run) || null;
  if (run) ctx._runNow = { at: Date.now(), run };
  return run;
}
const runChanged = ctx => { ctx._runNow = null; };
async function sheetRec(ctx, id) { if (!id) return null; const s = await ctx.db.collection(SHEETS).doc(String(id)).get(); return s.exists ? Object.assign({ id: s.id }, s.data()) : null; }
async function runSets(ctx) { const s = await ctx.db.collection(SETS).where("runId", "==", ctx.runId).get(); return s.docs.map(d => Object.assign({ setId: d.id, id: d.id }, d.data())); }
async function setRec(ctx, id) { if (!id) return null; const s = await ctx.db.collection(SETS).doc(String(id)).get(); return s.exists ? Object.assign({ setId: s.id, id: s.id }, s.data()) : null; }
async function pools(ctx, ids) {
  const out = new Map(), list = [...new Set((ids || []).map(String))];
  for (let i = 0; i < list.length; i += 100) for (const s of await ctx.db.getAll(...list.slice(i, i + 100).map(id => ctx.db.collection(POOL).doc(id)))) if (s.exists) out.set(s.id, s.data());
  return out;
}
async function worldLine(ctx, key) { const s = await ctx.db.collection(WORLD_LINES).doc(String(key)).get(); if (!s.exists) return null; const d = s.data() || {}; if (d.gone) return { key, gone: true }; try { return Object.assign(JSON.parse(d.data || "{}"), { key, at: num(d.at) }); } catch (_) { return null; } }
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
/* STEPSAID (Paul, 27 Nov 2026: "I did not see any feedback to the user when a sheet is being altered"; a Change SKU's panel said "The cloud is doing it… Started" for minutes): a request says each
   stage of its work as it reaches it, through its own step (runCmdStep: what the pressing computer's panel and every sheet card that follows the request show). One write a stage, and only when
   the words change; never waited for (a process's calls go one at a time, so a stage's words land before the request's next write and before its answer); a write that fails is only words lost.
   x.quiet (a sweep, which says its own progress at each turn): nothing is said. → say(text), and say.done() to wait for the words already sent. */
function stepper(x) {
  const none = Object.assign(() => {}, { done: async () => {} });
  if (!x || x.quiet || !x.cmd || !x.cmd.id || !x.ctx) return none;
  const call = typeof x.ctx.callNow === "function" ? x.ctx.callNow : x.ctx.call; if (typeof call !== "function") return none;
  let last = String(x.cmd.step || ""), chain = Promise.resolve();
  const say = text => { text = str(text, 160); if (!text || text === last) return; last = text; chain = chain.then(() => call({ op: "runCmdStep", runId: x.ctx.runId, id: x.cmd.id, text }, `${x.ctx.runId}|cmd|${x.cmd.id}|step|${Math.floor(Date.now() / 60000)}`)).catch(() => null); };
  return Object.assign(say, { done: () => chain });
}

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
  const settled = moved && !busyRec(rec, now) && !owesNest(rec, now) && (!files || !rec.filesOwed);
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
      if (seated(s, id) || busyRec(s, now) || owesNest(s, now) || s.inplaceHold) continue;
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
    for (const s of resit.values()) { const r = await put(x, { op: "putSheet", sheet: { id: s.id, rejects: [], dirty: true }, contentBase: num(s.contentRev) }, `strand|resit|${s.id}|${num(s.contentRev)}|${num(s.layoutRev)}`); if (!err(r)) { out.writes++; out.resat.push(s.id); } }   // (FRESHWRITE, 27 Nov 2026: a nest that wrote the sheet since this read keeps its turned-away list)
  }
  // CONTENDEDBACK: a line another run had claimed is asked for again (the server answers afresh)
  const again = {}; for (const [k, l] of Object.entries(L)) if (l && l.state === "contended" && !l.hold) { again[k] = { state: "pulled", reason: null, wait: null }; out.contended.push(k); }
  if (Object.keys(again).length) { const r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: again }, `strand|contended|${Object.keys(again).sort().join(",").slice(0, 400)}`); if (!err(r)) out.writes++; }
  return out;
}
/** How many of these lines' pieces a sheet seats now. → { pieces, placed, names, waiting:[reason], open } */
async function placedNow(ctx, keys) {
  const run = await ctx.read({ op: "runGet", runId: ctx.runId }).catch(() => null), L = (run && run.run && run.run.lines) || lines(ctx);
  const sheets = await sheetsWith(ctx, keys.flatMap(k => (L[k] && L[k].poolIds) || [])), on = listedBy(sheets), out = { pieces: 0, placed: 0, names: new Set(), waiting: [], open: 0 };
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

/** The open lines whose pieces hold an older drawing of these SKUs, or another SKU than the line reads as now (the world's line). */
/* FASTPICK (Paul, 22 Nov 2026: a save to the charm repository on Michelle's computer stayed at "Reading the lines again…" and failed; the request after it waited behind it): this read every
   open line of the run one by one, two reads a line (its world reading, its pieces), about 1,400 lines: two minutes, past the unit's one-minute limit. The unit was cut off when it went to save
   what it found, so every pass read them all again from the start, for good, and every request after it waited. The world readings and the pieces are read now in a few batched reads
   (300 a read), a few seconds for the whole run. */
/** Many documents, 300 a read, the reads side by side. */
async function getMany(ctx, coll, ids) { const parts = []; for (let i = 0; i < ids.length; i += 300) parts.push(ids.slice(i, i + 300)); return (await Promise.all(parts.map(p => ctx.db.getAll(...p.map(id => ctx.db.collection(coll).doc(String(id))))))).flat(); }
async function staleLines(ctx, skus) {
  const want = new Set(skus.map(s => String(s).trim().toUpperCase()).filter(Boolean)), L = lines(ctx), out = [], own = new Set(), entries = new Map();
  const entryOf = async sku => { if (!entries.has(sku)) { const s = await ctx.db.collection(MASTER).doc(sku).get(); entries.set(sku, s.exists ? s.data() : null); } return entries.get(sku); };
  const keys = Object.keys(L).filter(k => LIVE_LINE(L[k]) && (L[k].poolIds || []).length);
  const world = new Map(), rowsAll = new Map();
  for (const s of await getMany(ctx, WORLD_LINES, keys)) {
    if (!s.exists) continue; const d = s.data() || {}; if (d.gone) continue; try { world.set(s.id, JSON.parse(d.data || "{}")); } catch (_) { /* a reading that does not parse: the run line's own SKU is used */ }
  }
  const ids = [...new Set(keys.flatMap(k => (L[k].poolIds || []).map(String)))];
  for (const s of await getMany(ctx, POOL, ids)) if (s.exists) rowsAll.set(s.id, s.data());
  for (const k of keys) {
    const l = L[k];
    const wl = world.get(k) || null, design = String((wl && wl.spec && wl.spec.designSku) || l.sku || "").toUpperCase(), rows = new Map((l.poolIds || []).map(String).filter(id => rowsAll.has(id)).map(id => [id, rowsAll.get(id)]));
    const skusHere = new Set([design].concat([...rows.values()].map(r => String(r.sku || "").toUpperCase())).filter(Boolean));
    if (![...skusHere].some(s => want.has(s))) continue;
    const e = design ? await entryOf(design) : null;
    const stale = [...rows.values()].some(r => !r.custom && ((design && String(r.sku || "").toUpperCase() !== design) || (e && e.masterHash && r.masterHash && r.masterHash !== e.masterHash) || redrawn(e, r)));
    if (stale) { out.push(k); if ([...rows.values()].some(r => r.artEdit && r.artEdit.front)) own.add(k); }
  }
  return Object.assign(out, { own });   // (own: lines with a piece a person changed in the Charm Editor for that order alone)
}
/* REDRAWN (Paul, 22 Nov 2026: 9MM_DISC_8391 updated to 9.0 mm in the Charm Editor with "Make the open pieces again", and the cloud answered "Nothing to make again" while its 4 discs
   stood on a sheet at the old size): a Charm Editor update replaces the design's drawing (charmHash, its edited.from → edited.to) and keeps the master file's hash (masterHash), the only
   hash this looked at. A piece made from the drawing the update replaced (its charmHash, a fitted size's "~10MM" aside, is edited.from) holds an older drawing. */
// (EVERYOLD, Paul 27 Nov 2026: "those changes do not always propagate throughout the system": after a second update edited.from names only the drawing the LAST update replaced, so pieces still cut from the first
//  one were never made again. The record keeps every drawing an update replaced (edited.was, _charmNestMaster.js editorPut): a piece cut from any of them holds an older drawing)
function redrawn(e, r) {
  const ed = e && e.edited, now = String((ed && ed.to) || (e && e.charmHash) || ""), old = new Set([].concat(ed && Array.isArray(ed.was) ? ed.was : [], ed && ed.from != null ? [ed.from] : []).map(String).filter(h => h && h !== now));
  return !!(old.size && r && r.charmHash && old.has(String(r.charmHash).split("~")[0]));
}
/** Pieces as a sheet lists them, from their own rows and the library's index (the placement's own description: charm-nest-place.js charmOfRow). */
async function describe(ctx, rows) {
  const PU = require("./_charmNestPlaceUnit"), Place = require("../../charm-nest-place.js"), key = r => String(r.sku || "").trim().toUpperCase(), entries = new Map(), now = Date.now();
  for (const s of await getMany(ctx, MASTER, [...new Set(rows.map(key).filter(Boolean))])) if (s.exists) entries.set(s.id, s.data());
  return rows.map(r => PU.customCharm(Place.charmOfRow(Object.assign({}, r, { material: r.material || r.metal }), PU.geometryOf(entries.get(key(r)), r), now), r));
}
/* STRAIGHT (Paul, 23 Nov 2026: "Everything the user does is primary and must be immediate including the cloud ... no more waiting", "you've just been piling on rule after rule ... instead of fixing the problem"): the ONE way
   a person's change makes order lines' pieces again (Change SKU, Just this order, Make it again, a saved link or drawing, Send to Sheet). In one go, in one process: the piece maker makes the new pieces, they are seated
   where the old ones lay (or in the nearest free room of their sheet: _charmNestSeatNow.js), and their records, their entries and their places are written in ONE write (cmdSwapLine); then it is answered. Nothing is taken
   off first, no place is kept, no step is written down between, nothing waits for a pass, a nest or a reading of the orders. A piece on no sheet yet is placed as any new piece. Paul's rules (23 Nov 2026): "Unless a sheet is
   marked as laser cut including RG 10k, 14k the user should be able to update/remove/cancel a given charm ... nothing should prevent that"; "The only reason a charm/charms would not be placed on the original sheet is if
   that charm/charms were updated and it not longer fit onto the available space left behind by the old charm. Then the charm would go to the next avaliable Draft sheet of the same colour": its sheet is never nested again
   for it. A line is made again whole or not at all; a line on hold is not made; what cannot be made leaves its sheet, so the old piece is never cut. */
const NOT_MADE = new Set(["held", "oversize", "unmatched", "noDesign", "skipped"]);
async function again(x, keys, label) {
  const ctx = x.ctx, w = x.work, now = Date.now();
  if (!lane(ctx, "nest") || !lane(ctx, "pool")) return no(`${label} is not the cloud's yet (in this build pieces are made and placed by a computer): with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again`);
  const run = await runNow(ctx), L = (run && run.lines) || lines(ctx), why = (w.why || []).slice(), go = new Map(), names = new Set(w.names || []), patch = {}, waits = (w.waits || []).slice(), say = stepper(x);   // (STEPSAID; TRUEDONE: waits)
  let n = num(w.n), pieces = num(w.pieces), nests = num(w.nests);
  const [sheets, sets] = await Promise.all([sheetsWith(ctx, keys.flatMap(k => (L[k] && L[k].poolIds) || [])), runSets(ctx)]), on = listedBy(sheets), touched = new Set(), notOn = new Set();
  for (const k of keys) {
    const l = L[k]; if (!LIVE_LINE(l)) continue;
    if (l.hold && !x.lift) { why.push(`${ridOf(k)}: on hold: its pieces are made when it is released`); continue; }
    const homes = [...new Set((l.poolIds || []).map(id => on.get(String(id))).filter(Boolean))], keeps = homes.find(s => [s].concat(sets.filter(z => (z.sheetIds || []).includes(s.id))).some(z => num(z.laserDoneAt) > 0));
    if (keeps) why.push(`${ridOf(k)}: ${nameOf(keeps)} is marked laser cut: its pieces stay as they are`);
    else go.set(k, homes);
  }
  if (!go.size) { const res = n ? ok(`${n} order line(s) made again`) : no((why.join("; ") || "Nothing was made again").slice(0, 380)); return Object.assign(res.done, { n, names: [...names], why }), res; }
  const held = [...go.keys()].filter(k => L[k].hold || L[k].state === "held"); if (x.lift && held.length) { const free = { state: "pulled", hold: null, reason: null, wait: null }, r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: Object.fromEntries(held.map(k => [k, free])) }, `lift|${held.sort().join(",").slice(0, 400)}`); if (err(r)) return no(err(r)); for (const k of held) L[k] = Object.assign({}, L[k], free); }   // (Send to Sheet: the send is the release)
  const swap = (k, s, b, key) => put(x, Object.assign({ op: "cmdSwapLine", runId: ctx.runId, lineKey: k, rid: ridOf(k), sheetId: s.id, by: x.cmd.by || "", keepBacks: !!x.keepBacks }, b), `swap|${k}|${s.id}|${key}`);
  { const ks = [...go.keys()]; say(ks.length === 1 ? `Making the new pieces of order ${ridOf(ks[0])}…` : `Making the new pieces of ${ks.length} order lines…`); }   // (STEPSAID: the piece maker's stage)
  /** One line's new pieces onto the sheet that holds its old ones (a sheet that changed meanwhile is looked at again); a line of two sheets leaves the other one. No room there: gone, the whole line goes on the next Draft. */
  const onto = async (k, rows, homes) => {
    for (let t = 0; t < 6; t++) {
      const s = t ? await sheetRec(ctx, homes[0].id) : homes[0]; if (!s) return { error: "its sheet is gone", status: 404, gone: true };
      const charms = (await describe(ctx, rows)).filter(c => !c.metal || c.metal === s.metal); if (charms.length !== rows.length) return { error: "its metal changed", status: 409, gone: true };
      say(`Seating on ${nameOf(s)}…`);   // (STEPSAID)
      const got = await require("./_charmNestSeatNow").seat({ db: ctx.db, rec: s, lineKey: k, charms, rows, say: ctx.say }).catch(e => ({ broke: str((e && e.message) || e, 160) }));
      if (got.broke) return { error: `its new piece could not be drawn (${got.broke})`, status: 500 };
      if (!got.poses && !got.open) { for (const h of homes) notOn.add(h.id); return { error: got.why, status: 409, gone: true, noRoom: nameOf(s) }; }
      for (const o of t ? [] : homes.slice(1)) { await swap(k, o, { pools: [], charms: [] }, `off|${num(o.contentRev)}`); touched.add(o.id); }
      say(`Writing ${nameOf(s)}…`);   // (STEPSAID)
      // (FRESHWRITE, 27 Nov 2026: the poses were seated on the sheet as read: its content revision too, so a move by hand since (contentRev, not layoutRev) has it seated again)
      const r = await swap(k, s, { pools: rows, charms, ...(got.poses ? { poses: got.poses, layoutBase: num(s.layoutRev), contentBase: num(s.contentRev) } : {}) }, `${num(s.layoutRev)}|${num(s.contentRev)}|${got.poses ? 1 : 0}|${t}`);
      if (r && r.conflict) continue;   // (the sheet changed while it was seated: seated again on it as it is now)
      if (err(r)) return r;
      names.add(nameOf(s)); touched.add(s.id); n++; pieces += rows.length; if (r.size) why.push(`${ridOf(k)}: resized to ${Math.round(r.size * 100)}% to fit its place`); if (!r.seated) { nests++; await startNest(x, s.id, false, `${label}, asked by ${x.cmd.by || "a person"}`); }
      const l = L[k]; if ((r.backsDropped || []).length) { if (l.engrave && typeof l.engrave === "object") patch[k] = { engrave: Object.assign({}, l.engrave, { invalidatedBy: "charm changed", approved: false, state: "reclassify" }) }; why.push(`${ridOf(k)}: its engraving is approved again on the new charm`); }
      return r;
    }
    return { error: "its sheet kept changing while its new piece was seated: ask again", status: 409 };
  };
  // the piece maker, with these lines as lines to make; the pieces of a line that lies on a sheet are written with their sheet, the others as any new piece
  const loose = [], given = Object.assign({}, run, { runId: ctx.runId, lines: Object.assign({}, L, Object.fromEntries([...go.keys()].map(k => [k, Object.assign({}, L[k], { state: "pulled", poolIds: [], repoolChanged: true, wait: null, reason: null, hold: null })]))) });
  const call = async (b, u) => {
    if (b.op === "runLinesPatch") {   // (a line that reads as it did is not written again: every write of a line writes the run's whole list of lines)
      const body = {}; for (const [k, v] of Object.entries(b.lines)) { const d = Object.fromEntries(Object.entries(v).filter(([f, z]) => JSON.stringify(z) !== JSON.stringify((L[k] || {})[f] === undefined ? null : L[k][f]))); if ((L[k] || {}).state === "committed" && !loose.includes(k)) delete d.state; if (Object.keys(d).length) body[k] = d; }
      const r = Object.keys(body).length ? await ctx.call(Object.assign({}, b, { lines: body }), u) : { ok: true, status: 200, at: Date.now() }; return err(r) ? r : Object.assign({}, r, { applied: Object.keys(b.lines) });
    }
    if (b.op !== "poolPut") return ctx.call(b, u);
    const byLine = new Map(), rest = [], out = { ok: true, status: 200, placed: [] }, was = x.bare && b.pools.some(p => String(p.poolId).startsWith(x.bare + "_")) ? await pools(ctx, b.pools.map(p => p.poolId)) : new Map(); for (const p of b.pools) { if (was.has(String(p.poolId)) && String(p.poolId).startsWith(x.bare + "_") && (was.get(String(p.poolId)).artEdit || {}).front) p.artEdit = Object.assign({}, was.get(String(p.poolId)).artEdit, { front: null }); const k = String(p.poolId).replace(/_\d+$/, ""); (byLine.get(k) || byLine.set(k, []).get(k)).push(p); }
    for (const [k, rows] of byLine) {
      const homes = go.get(k) || []; if (!homes.length) { rest.push(...rows); loose.push(k); continue; }
      const r = await onto(k, rows, homes);
      if (err(r) && r.gone) { say(`${r.noRoom ? `No room on ${r.noRoom}` : `${ridOf(k)}: ${str(r.error, 80)}`}: placing on the next Draft…`); for (const s of homes) { await swap(k, s, { pools: [], charms: [] }, `off|${num(s.contentRev)}`); touched.add(s.id); } rest.push(...rows); loose.push(k); }   // (no room on its sheet, its metal changed, or its sheet let it go: new pieces, on the next Draft)   // (STEPSAID)
      else if (err(r)) { why.push(`${ridOf(k)}: ${err(r)}`); out.placed.push(...rows.map(p => ({ poolId: p.poolId, sheetId: homes[0].id, sheetName: homes[0].fileBase || null, state: "ready" }))); }
    }
    if (!rest.length) return out;
    const r = await ctx.call({ op: "poolPut", pools: rest }, u); return err(r) ? r : Object.assign({}, r, { placed: (r.placed || []).concat(out.placed) });
  };
  await require("./_charmNestIntake").pool({ db: ctx.db, run: given, runId: ctx.runId, call, read: ctx.read, drive: ctx.drive || {}, cloudSteps: ctx.cloudSteps || [], fence: ctx.fence, say: ctx.say || (() => {}), budgetEnd: num(ctx.budgetEnd) || Date.now() + 45000, settings: null, stockPreset: null }, { step: true, only: new Set(go.keys()) });
  runChanged(ctx);
  const late = [];
  for (const [k, homes] of go) {
    const l = given.lines[k];
    if ((l.poolIds || []).length) continue;
    if (!NOT_MADE.has(String(l.state))) { late.push(k); continue; }
    for (const s of homes) { await swap(k, s, { pools: [], charms: [] }, `off|${num(s.contentRev)}`); touched.add(s.id); }
    patch[k] = { poolIds: [] }; why.push(`${ridOf(k)}: not made again: ${str(l.reason || l.state, 160)}`);
  }
  for (const k of go.keys()) if (!late.includes(k) && (L[k].changePending || L[k].repoolChanged)) patch[k] = Object.assign({ changePending: false, repoolChanged: false }, patch[k]);
  if (Object.keys(patch).length) await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: patch }, `again|${Object.keys(patch).sort().join(",").slice(0, 400)}`);
  if (loose.length) {
    const pr = (await require("./_charmNestPlaceUnit").placeAll(Object.assign({}, ctx, { run: given, only: new Set(loose), notOn }))) || {};
    const g = await runNow(ctx), L2 = (g && g.lines) || given.lines, ids = loose.flatMap(k => (L2[k] && L2[k].poolIds) || []), by = listedBy(await sheetsWith(ctx, ids));
    for (const s of new Set(by.values())) { names.add(nameOf(s)); nests++; await startNest(x, s.id, false, `${label}, asked by ${x.cmd.by || "a person"}`); }
    /* TRUEDONE (Paul, 27 Nov 2026: a Change SKU answered "made again … on GF Draft 4" while the placing had said "0 piece(s) placed so far"): only the pieces a sheet's record lists now are said placed;
       a line's pieces that no sheet lists yet are said to wait, with the placing's own reason (the line's, a sheet being nested, or the placing's time spent): they go on at the next placing */
    n += loose.length; pieces += ids.filter(id => by.has(String(id))).length;
    const later = (pr.cursor && pr.cursor.placeWait && pr.cursor.placeWait.why) || pr.why || (pr.more ? "the placing goes on in its next turn" : "");
    for (const k of loose) { const left = ((L2[k] && L2[k].poolIds) || []).map(String).filter(id => !by.has(id)); if (!left.length) continue; const whyNot = str((L2[k] && L2[k].reason) || later || "the next placing puts them on a sheet", 160); waits.push(`${ridOf(k)}: ${plural(left.length, "new piece")} ${left.length === 1 ? "waits" : "wait"} to be placed (${whyNot})`); }
    const went = loose.flatMap(k => [...new Set(((L2[k] && L2[k].poolIds) || []).map(id => by.get(String(id))).filter(Boolean))].map(s => ({ orderId: ridOf(k), type: "placed", at: Date.now(), by: x.cmd.by || "Cloud", station: "sorter", device: "cloud", sheetId: s.id, sheet: nameOf(s).slice(0, 80), lineKey: k, id: `swap-${x.cmd.id}-${k}-${s.id}`, text: `Went onto ${nameOf(s)}`.slice(0, 200) })));
    if (went.length) await put(x, { op: "timelineAdd", events: went }, `went|${loose.slice().sort().join(",").slice(0, 400)}`);
  }
  if (touched.size) await hopRefile(x, [...touched]);   // (a set already released: its labels, manifest and files are made again)
  await say.done();   // (STEPSAID: the words said so far land before the answer)
  if (late.length && now - (num(w.at) || now) < 5 * 60000) return wait(`Making the pieces again… ${n} of ${n + late.length} made`, { stage: "again", keys: late, at: num(w.at) || now, n, pieces, nests, names: [...names], why, waits });
  for (const k of late) why.push(`${ridOf(k)}: not made again: ${str((given.lines[k] || {}).reason || "its order could not be read", 160)}`);
  const tail = why.length ? " · " + why.join("; ") : "";
  // (TRUEDONE: what is placed, then what still waits; nothing placed is never said placed)
  const onWords = pieces ? `: ${plural(pieces, "piece")} placed${names.size ? " on " + and([...names]) : ""}${nests ? ", being nested" : ""}` : "", waitWords = waits.length ? (pieces ? " · " : ": ") + waits.join("; ") : "";
  const res = n ? ok(`${n} order line(s) made again${onWords}${waitWords}${tail}`) : no((why.join("; ") || "Nothing was made again").slice(0, 380));
  return Object.assign(res.done, { n, names: [...names], why, waits }), res;
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
/** The lines asked for read again at once, and only them (_charmNestWorld.js readNow): a Change SKU never waits for every open order to be read. null when they could not be. */
async function readLinesNow(x, keys, extra) {
  try {
    const ops = {}; for (const op of ["masterList", "optionMapGet", "aliasGet", "noDesignGet", "customGet", "listingSkus", "customSheetGet"]) ops[op] = b => x.ctx.read(Object.assign({ op }, b || {}));
    const got = await require("./_charmNestWorld").readNow({ db: x.ctx.db, ops, keys, lines: Object.assign({}, lines(x.ctx), extra || {}) });
    return got && got.size === keys.length ? got : null;
  } catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; if (x.ctx.say) x.ctx.say(`[cmd] the lines were not read again now: ${str((e && e.message) || e, 160)}`); return null; }
}
async function remakeline(x) {
  x.keepBacks = true;
  let ask = x.work.keys || [...new Set((x.a.ids || []).map(String))].filter(k => LIVE_LINE(lines(x.ctx)[k]));
  if (!ask.length) return ok("Those lines are not open lines of the run");
  // (Accept the new order, S2: a line whose change does not touch what it is made of keeps its pieces as they are and only stops waiting)
  const L = lines(x.ctx), calm = x.a.label === "Accept the new order" && !x.work.stage ? ask.filter(k => L[k].changePending && !L[k].repoolChanged && (L[k].poolIds || []).length) : [];
  if (calm.length) { const r = await put(x, { op: "runLinesPatch", runId: x.ctx.runId, lines: Object.fromEntries(calm.map(k => [k, { changePending: false }])) }, `calm|${calm.join(",")}`); if (err(r)) return no(err(r)); ask = ask.filter(k => !calm.includes(k)); if (!ask.length) return ok("Accepted: its pieces stay as they are"); }
  if (!x.work.stage) await readLinesNow(x, ask);
  return again(x, ask, "Making the order's pieces again");
}
/* LINKLATE (Paul, 23 Nov 2026, order 4236768670: "the items on the sheet and the thumbnails did not get updated again ... I need this properly fixed once and for all"): KEEPNOW picks a design's pieces at once, since a save to
   the repository "changes a drawing, never what an order reads as". A saved LINK does change it: "Wrong charm?", a charm picked in Review or the Charm Editor links a listing to the charm and asks this very request seconds
   later, before the orders have been read again with the link. The old reading still said LEMON, nothing was found for LEMON_7271, "Nothing to make again" was the answer for good and the sheets kept the old charm. A request
   for a charm that a listing (or an option of one) was linked to in the last ten minutes waits for a reading of the orders begun after that link, as Change SKU's does. Not known (the links could not be read): it waits. */
const LINK_MS = 10 * 60000, LINKS = ["Charm_Sku_Aliases", "Charm_Option_Map"];
function linkedSkus(d) {
  const out = new Set(), walk = (v, n) => { if (n > 6 || v == null) return; if (typeof v === "string") { const s = v.trim().toUpperCase(); if (s && s.length <= 80) out.add(s); return; } if (Array.isArray(v)) { for (const x of v) walk(x, n + 1); return; }
    if (typeof v === "object" && typeof v.toMillis !== "function" && !("_seconds" in v)) for (const x of Object.values(v)) walk(x, n + 1); };
  for (const k of ["sku", "huggie", "bySku", "pairBySku", "map"]) walk(d && d[k], 0);
  return out;
}
async function linkedLately(ctx, skus, since, asked, lids) {   // (lids: the listings linked, CMDFAST)
  const want = new Set((skus || []).map(s => String(s).trim().toUpperCase()).filter(Boolean)); let at = 0; if (!want.size) return 0;
  try {
    for (const name of LINKS) for (const s of (await ctx.db.collection(name).where("updatedAt", ">=", new Date(since)).limit(100).get()).docs) { const d = s.data() || {}, got = linkedSkus(d); if ([...want].some(w => got.has(w))) { at = Math.max(at, ms(d.updatedAt) || asked); if (lids) { const lid = String(d.listingId || s.id || "").replace(/\D/g, ""); lids.add(lid || "?"); } } }
    return at;
  } catch (_) { return asked; }
}
/** CMDFAST: the open lines of these listings read again now (readNow), so a saved link is seen at once and not at the next reading of all orders, minutes later. true when every one was (or there is none). */
async function readListingsNow(x, lids) {
  if (!lids || !lids.size || lids.has("?")) return false;
  try {
    const L = lines(x.ctx), keys = Object.keys(L).filter(k => OPEN_LINE(L[k])), mine = [];
    for (const s of await getMany(x.ctx, WORLD_LINES, keys)) {
      if (!s.exists) continue; const d = s.data() || {}; if (d.gone) continue;
      const r = JSON.parse(d.data || "{}"); if (!r || !r.line) return false;
      if (lids.has(String(r.line.listingId || "").replace(/\D/g, ""))) mine.push(s.id);
    }
    if (!mine.length) return true;
    return mine.length <= 80 && !!(await readLinesNow(x, mine));
  } catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; return false; }
}
async function keepsync(x) {
  if (!x.work.stage) { const asked = num(x.cmd.at) || Date.now(), lids = new Set(); if (await linkedLately(x.ctx, x.a.ids || [], asked - LINK_MS, asked, lids)) await readListingsNow(x, lids); }   // (LINKLATE: the linked listings' own lines are read again now)
  return remake(x);
}
/* REPOALL (Paul, 23 Nov 2026: "if the update repository button is clicked on a given charm that is part of an order or several orders, then all those orders must be updated as well not only the repository ... all orders
   should be updated on the same sheet where they reside"): every open line that holds the older drawing, a few at a time until none is left, each said as it goes (it made 40 of them, once). A line on a sheet marked laser
   cut keeps its piece; a held one is made again when it is released; what could not be made is tried twice more. A piece a person changed for its order alone keeps that change and its drawing (it cannot be laid over
   another drawing), said; the piece the drawing was saved from (sourceId) has it in the drawing now. */
const BATCH = 4, ROUNDS = 3;
async function remake(x) {
  const ctx = x.ctx, w = x.work, did = num(w.did), from = String(x.a.sourceId || "").replace(/_\d+$/, ""); let todo = w.todo || [], kept = num(w.kept), held = num(w.held), own = num(w.own), round = num(w.round); const why = (w.why || []).slice(), names = new Set(w.names || []); let n = num(w.n), fresh = !w.todo;
  while (!todo.length && round < ROUNDS) {
    const L = lines(ctx), st = await staleLines(ctx, x.a.ids || []), sets = await runSets(ctx), on = listedBy(await sheetsWith(ctx, st.flatMap(k => L[k].poolIds || []))), cut = k => (L[k].poolIds || []).some(id => { const s = on.get(String(id)); return s && [s].concat(sets.filter(z => (z.sheetIds || []).includes(s.id))).some(z => num(z.laserDoneAt) > 0); });
    const later = st.filter(k => L[k].hold && !cut(k)), alone = k => st.own.has(k) && k !== from; kept = st.filter(cut).length; held = later.length; own = st.filter(k => !cut(k) && alone(k)).length; todo = st.filter(k => !L[k].hold && !cut(k) && !alone(k)); round++;
    if (fresh && later.length) { const r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: Object.fromEntries(later.map(k => [k, { repoolChanged: true }])) }, `held|${later.slice().sort().join(",").slice(0, 400)}`); if (err(r)) return no(err(r)); runChanged(ctx); }
    if (!todo.length) break; if (!fresh) why.length = 0; fresh = false;
  }
  const tail = [kept ? `${kept} on a sheet marked laser cut stay as they are` : "", held ? `${held} on hold are made again when released` : "", own ? `${own} with their own Charm Editor change keep it` : "", ...why.slice(0, 4)].filter(Boolean).join("; ");
  if (!todo.length) return n ? ok(`${n} order line(s) made again${names.size ? " on " + and([...names]) : ""}${tail ? " · " + tail : ""}`) : kept || held || own || why.length ? (why.length ? no(tail) : ok(`Nothing to make again now: ${tail}`)) : ok("Nothing to make again: every open piece already holds the drawing the library has now");
  const now = todo.slice(0, BATCH), res = await again(Object.assign({}, x, { work: { at: 1 }, bare: from, quiet: true }), now, "Making the orders again"), r = (res.done || {}); n += num(r.n); for (const z of r.names || []) names.add(z); why.push(...(r.why || []), ...(r.waits || []));   // (STEPSAID: the sweep says its own progress each turn; TRUEDONE: pieces still to be placed are said)
  return wait(`Making the open orders again… ${did + now.length} of ${did + todo.length}`, { stage: "all", todo: todo.slice(BATCH), did: did + now.length, n, names: [...names].slice(0, 60), why: why.slice(0, 20), kept, held, own, round }, Math.min(99, Math.round((did + now.length) / (did + todo.length) * 100)));
}
async function skuonce(x) {
  const ctx = x.ctx, key = String((x.a.ids || [])[0] || ""), sku = String(x.a.sku || "").trim().toUpperCase(), l = lines(ctx)[key], say = t => `${sku} for order ${ridOf(key)} only${t}`.slice(0, 380);
  if (!x.work.stage) {
    if (!key || !sku) return no("not ready");
    if (!LIVE_LINE(l)) return no(`The piece ${key} is not in the run the cloud carries`);
    const e = await ctx.db.collection(MASTER).doc(sku).get(); if (!e.exists) return no(`${sku} is not in the charm repo`);
    if ((e.data() || {}).blocked || (e.data() || {}).block) return no(`${sku} is blocked in the master, so it cannot be cut`);
    const was = String((l.skuOverride && l.skuOverride.sku) || l.sku || ""), saved = was.toUpperCase() === sku, rows = saved && (l.poolIds || []).length ? await pools(ctx, l.poolIds) : null, at = Date.now(), who = String(x.cmd.by || "").trim() || "a person", pick = saved && l.skuOverride ? l.skuOverride : { sku, by: who, at, was };
    if (rows && rows.size === l.poolIds.length && [...rows.values()].every(r => String(r.sku || "").toUpperCase() === sku)) return ok(`This piece is ${sku} already`);
    if (!saved) {   // (ONCEMADE: the choice written once; the pieces are made whether it was saved now or before)
      const r = await put(x, { op: "runLinesPatch", runId: ctx.runId, lines: { [key]: { skuOverride: pick, repoolChanged: (l.poolIds || []).length > 0 } } }, `once|${key}|${sku}`); if (err(r)) return no(err(r));
      await put(x, { op: "timelineAdd", events: [{ orderId: ridOf(key), type: "decided", id: `${key}.once.${at}`, at, by: who, station: "sorter", text: `Decided by ${who} · Just this order: ${was || "no SKU"} → ${sku} (the listing and Etsy are not changed)`.slice(0, 200), data: { sku, once: true, was: was || null } }] }, `once|tl|${key}|${sku}`);
      runChanged(ctx);
    }
    const read = await readLinesNow(x, [key], { [key]: Object.assign({}, l, { skuOverride: pick }) });
    if (!(l.poolIds || []).length || l.hold) return ok(say(l.hold ? " is saved: its pieces are made when the order is released" : ": its pieces are made at the next intake"));
    if (!read) return no(say(" is saved, but the order could not be read again: press it again"));
  }
  const res = await again(x, [key], `${sku} for this order only`);
  if (res.done && res.done.ok) res.done.text = say(" · " + res.done.text);
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
  const r = await put(x, { op: "putSheet", layoutBase: num(p.layoutRev), contentBase: num(p.contentRev) /* (FRESHWRITE: a move by hand since this read moves contentRev, not layoutRev) */, sheet: { id: p.id, placements, rejects: [], feedWait: null, placedCount: placements.length, density: num(res.density), freePt2: num(res.freePt2),
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
  let gone = "", freed = "";
  // (RGEMPTY: a Rose Gold draft emptied gives its physical sheet back before it is removed)
  if (o.release && o.draft && num(r.from && r.from.left) === 0 && src.roseStockId && has("roseRelease")) { const rr = await put(x, { op: "roseRelease", stockId: src.roseStockId, sheetId: src.id, metal: src.metal }, `release|${src.id}|${src.roseStockId}`); freed = err(rr) ? `; its physical sheet stays reserved (${err(rr)})` : "; its physical sheet given back"; }
  if (o.draft && num(r.from && r.from.left) === 0) { const a = await put(x, { op: "archiveEmptySheet", id: src.id, runId: x.ctx.runId }, `archive|${src.id}`); gone = err(a) ? `${nameOf(src)} stays, empty (${err(a)})` : `${nameOf(src)} was left empty and archived${freed}`; if (!err(a)) { const re = await require("./_charmNestCombine").renumberAfter(x.ctx, src, k => `${x.ctx.runId}|cmd|${x.cmd.id}|${k}`); if (re.length) gone += `; ${re.join(", ")}`; } }
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
  /* RGEMPTY (Paul, 23 Nov 2026: "Why did this rose gold draft not get used further and now it started another draft 2"): a Rose Gold DRAFT is emptied onto the Rose Gold draft a person
     names (both uncut, no green line, in no set, not busy), its pieces nested there around what that draft holds (nothing it holds moves); the emptied draft is removed and its physical
     sheet given back (roseRelease). Rose Gold only: 10K / 14K below are unchanged. */
  if (src.metal === "rose") {
    if (!activeCharms(src).length) return no(`${nameOf(src)} has no pieces`);
    const t = x.a.targetId ? await sheetRec(x.ctx, x.a.targetId) : null, rsets = await runSets(x.ctx), rnow = Date.now();
    if (!t || t.id === src.id || t.metal !== "rose" || t.runId !== x.ctx.runId) return no("Name the Rose Gold draft of the run that takes its pieces.");
    for (const r of [src, t]) {
      if (cutRec(r)) return no(`${nameOf(r)} was already cut`);
      if (lineDrawn(r)) return no(`${nameOf(r)} has a green line drawn`);
      if ((r.setId && !r.draft) || inAnySet(r, rsets)) return no(`${nameOf(r)} is in a set`);
      const b = busyRec(r, rnow); if (b) return no(`${nameOf(r)} ${b}: ask again once it is done`);
    }
    return moveAndNest(x, src, t, "Empty Rose Gold draft", { draft: true, release: true });
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
const { released: releasedSet } = require("./_charmNestTakeOffOps");
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
   yet is made again the one straight way (again, above): its hold lifted (the send IS the release), its new pieces in the old ones' place, or placed as new pieces. Refused in words when a sheet
   is marked laser cut or a design cannot be made. Asking twice changes nothing. */
const TAKEN = new Set(["abandoned", "superseded"]);
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
    const open = keys.filter(k => LIVE_LINE(L[k]));
    if (!open.length) return no(`Order ${rid} is not in the run the cloud carries.`);
    const paths = new Set((rec.files || []).map(F => F && F.cloud && F.cloud.path).filter(Boolean));
    const todo = [];
    for (const k of open) {
      const ids = (L[k].poolIds || []).map(String), rows = ids.length ? await pools(ctx, ids) : new Map();
      if (ids.length && ids.every(id => { const r = rows.get(id); return r && r.custom && !TAKEN.has(r.state) && paths.has(String(r.aiPath || "")); })) continue;   // (already these designs)
      todo.push(k);
    }
    if (!todo.length) return ok("These designs are its pieces already");
    x.work = { stage: "again", keys: todo, rid };
  }
  x.lift = true;
  const res = await again(x, x.work.keys, "Send to Sheet"); if (res.wait) res.wait.work.rid = x.work.rid;
  if (res.done && !res.done.ok) res.done.text = `Order ${x.work.rid}: ${res.done.text}`.slice(0, 380);
  return res;
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

/* ═══ RGMERGE (Paul, 23 Nov 2026: "Those pieces should've been part of sheet one without the green cut line") ═══ rgmerge { sheetId: the sheet that empties, targetId }: two Rose Gold sheets of one
   open set that a green line divided (cut one after the other from one physical sheet, nothing on the laser) become one sheet. The whole change is ONE write (cmdRoseMerge: the cuts set aside, the
   pieces moved where they lie, the empty sheet out of its set and archived), so a pass that ends midway leaves the sheets as they were or as they are to be, never between. Then: the emptied
   sheet's own files go (archiveEmptySheet), a set whose labels were made has them made again (labelsAsk), and a draft that was on the last leftover is seated again once the target's new line is
   cut (its pieces stay where they lie when that line is clear of them; else it is nested again). The files, the QR label, the line and its cut are the cloud's, as after any change. */
const MERGE_MS = 15 * 60000, MERGE_CUT_MS = 25 * 60000;
/** Every piece of a layout lies outside an outline (each piece's own box, as it is turned): the metal under it is not the line's. */
const clearOf = (rec, p) => !!p && (rec.placements || []).length > 0 && (rec.placements || []).every(q => num(q.wPt) > 0 && num(q.hPt) > 0 && !CutLine.intersects(p, num(q.cxPt) - q.wPt / 2, num(q.cyPt) - q.hPt / 2, +q.wPt, +q.hPt, 0));
const insideOf = (now, was) => !!(now && was && now.axis === was.axis && now.step === was.step && now.values.length === was.values.length && now.values.every((v, i) => v <= was.values[i] + 0.5));
async function rgmerge(x) {
  const ctx = x.ctx, a = x.a, by = x.cmd.by || "", now = Date.now(), w = Object.assign({}, x.work);
  if (!lane(ctx, "nest") || !["cmdRoseMerge", "archiveEmptySheet", "nestStart", "roseClaim"].every(has)) return no("Making two Rose Gold sheets one is not the cloud's yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again");
  if (w.stage === "cut") return mergeSeated(x, w);
  const S = await sheetRec(ctx, str(a.sheetId, 80)), T = await sheetRec(ctx, str(a.targetId, 80)), tries = `t${num(w.tries)}`;
  if (!S || !T || S.runId !== ctx.runId || T.runId !== ctx.runId) return no("Pick two sheets of the run the cloud carries.");
  const merged = S.archived && S.mergedInto && S.mergedInto.to === T.id;
  if (!merged) {
    const again = (why, rec) => { const first = num(w.firstNowAt) || now; return now - first > MERGE_MS ? no(`Not moved: ${nameOf(rec)} ${why} after 15 minutes; ask again`) : wait(`Moving ${nameOf(S)}'s pieces as soon as ${nameOf(rec)} is still (${why})…`, Object.assign({}, w, { firstNowAt: first, tries: num(w.tries) + 1 })); };
    const held = T.roseStockId ? (await runSheets(ctx, "rose")).filter(r => r.roseStockId === T.roseStockId && r.id !== S.id && r.id !== T.id && !cutRec(r)) : [], live = [S, T].concat(held), busy = live.find(r => liveNest(r) || busyRec(r, now));
    const body = { op: "cmdRoseMerge", runId: ctx.runId, sheetId: S.id, targetId: T.id, by, person: by }, at = `${S.id}|${num(S.layoutRev)}.${num(S.contentRev)}|${T.id}|${num(T.layoutRev)}.${num(T.contentRev)}|${tries}`;
    const dry = await put(x, Object.assign({}, body, { dry: true }), `rgmerge-dry|${at}`);
    if (err(dry) && !dry.busy) return no(`Not moved: ${err(dry)}`);   // (a refusal is said before anything is stopped for it)
    if (busy || dry.busy) { await stopFor(x, live, `${nameOf(S)} and ${nameOf(T)} become one sheet (a person's request goes first)`); return busy ? again(busyRec(busy, now) || "is nesting", busy) : again(err(dry).replace(/^.*? (is|has|keeps) /, "$1 ").replace(/: try again.*$/, ""), T); }
    const r = await put(x, body, `rgmerge|${at}`);
    if (err(r)) return r.busy ? again(err(r).replace(/^.*? (is|has|keeps) /, "$1 ").replace(/: try again.*$/, ""), T) : no(`Not moved: ${err(r)}`);
  }
  const E = await sheetRec(ctx, S.id), m = (E && E.mergedInto) || {}, lift = Array.isArray(m.lift) ? m.lift : [];
  if (!w.cleared) { await put(x, { op: "archiveEmptySheet", id: S.id, runId: ctx.runId }, `rgmerge-files|${S.id}`); w.cleared = true; }   // (its own files; its record stays, archived)
  if (!w.asked) { await hopRefile(x, [T.id]); w.asked = true; }
  w.said = `${plural(num(m.n), "piece")} of ${m.fromName || nameOf(S)} moved onto ${nameOf(T)} where ${num(m.n) === 1 ? "it lies" : "they lie"} (nothing was nested), the green line between them is gone and ${m.fromName || nameOf(S)} was removed from ${m.set || "the set"}; ${nameOf(T)}'s files, QR label and one green line are made again`;
  if (!lift.length) return ok(w.said);
  return wait(`Waiting for ${nameOf(T)}'s new green line, then ${and(lift.map(l => l.name))} goes on its leftover…`, Object.assign({}, w, { stage: "cut", at: num(w.at) || now }));
}
async function mergeSeated(x, w) {
  const ctx = x.ctx, now = Date.now(), T = await sheetRec(ctx, str(x.a.targetId, 80)), E = await sheetRec(ctx, str(x.a.sheetId, 80)), m = (E && E.mergedInto) || {}, notes = [];
  const left = []; for (const l of Array.isArray(m.lift) ? m.lift : []) { const r = await sheetRec(ctx, l.sheetId); if (r && !r.archived && !r.roseStockId && activeCharms(r).length) left.push(r); }   // (one that found a seat of its own meanwhile keeps it)
  if (!left.length) return ok(w.said);
  if (!T || !T.roseCutAt) {
    if (T && now - num(w.at) <= MERGE_CUT_MS) return wait(`Waiting for ${nameOf(T)}'s new green line…`, w);
    return ok(`${w.said}. Its green line is not cut yet${T && T.autoCutWhy ? ` (${str(T.autoCutWhy, 120)})` : ""}: ${and(left.map(nameOf))} ${left.length === 1 ? "takes its" : "take their"} plate at the next nest`);
  }
  const stockId = str(T.roseStockId, 120), ss = await ctx.db.collection(STOCK).doc(stockId).get(), stock = ss.exists ? ss.data() || {} : null, parse = s => { try { return s ? JSON.parse(s) : null; } catch (_) { return null; } };
  const cq = await ctx.db.collection(STOCK).doc(stockId).collection("cuts").where("revision", "==", num(m.fromRevision)).limit(1).get(), was = cq.docs[0] ? (parse(cq.docs[0].data().planJson) || {}).profile : null, p = stock && parse(stock.profileJson);
  for (const L of left) {
    const b = busyRec(L, now); if (b) return wait(`Waiting for ${nameOf(L)} (${b})…`, w);
    let seat = false;
    if (stock && !stock.owner && (insideOf(p, was) || clearOf(L, p))) { const c = await put(x, { op: "roseClaim", sheetId: L.id, sheetName: nameOf(L), stockId, revision: num(stock.revision), exact: true, metal: "rose", wPt: num(stock.wPt), hPt: num(stock.hPt), by: x.cmd.by || "Cloud" }, `rgmerge-seat|${L.id}|${stockId}|${num(stock.revision)}`); seat = !err(c); if (seat) { stock.owner = L.id; notes.push(`${nameOf(L)} is on its leftover, its pieces where they were`); } }
    if (!seat) { const s = await startNest(x, L.id, true, `${nameOf(T)}'s green line was drawn again`); notes.push(s.error ? `${nameOf(L)} takes its plate at its next nest (${s.error})` : `${nameOf(L)} is nested again on its leftover`); }
  }
  return ok(`${w.said}. ${notes.join("; ")}`);
}

const EXEC = { reorder, delete: del, leaveset, setrebuild, unkeep, repicture, placewaiting, pairnow, sendhome, remakeline, keepsync, remake, skuonce, release, renest, consolidate, empty, seat, customsend, hop, backhome, restorenest, rgmerge };
module.exports = { EXEC, strandedFix, placedNow, staleLines, runSheets, runSets, sheetRec, nameOf, cutRec, lineDrawn, listedBy, seated, has, lane, WORK };
