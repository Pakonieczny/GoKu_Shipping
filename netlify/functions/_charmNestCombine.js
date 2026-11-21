/*  netlify/functions/_charmNestCombine.js — COMBINEDRAFTS (Paul, 21 Nov 2026: "one sheet at a time": a new Gold Filled / Sterling Silver draft is never started while an older draft of
 *  that metal still has room; drafts left underfilled must be combined). This morning the cloud's nests failed and the placement offered only the first filling draft (both fixed today:
 *  charm-nest-place.js ONEDRAFTFILL), so drafts were split that nothing combined again: SS Draft 2 (6 pieces) beside SS Draft 3 (5), GF Draft 4 (12) beside an older GF draft with room.
 *
 *  The pass item "combine" (lane "nest", order 55: after the commands, before the files and the sweeps), every pass of a run the cloud carries:
 *    · per Gold Filled / Sterling Silver colour, the run's DRAFTS that may take part (judged on their records: not cut, not approved, in no set, not released full, not being nested
 *      or written, not held by a person (Manual Control, a keep), no problem, no run hold, no place kept for an order made again);
 *    · the oldest draft as the target, a NEWER draft as the source (newest first), and the source's pieces go onto the target only when ALL of them fit there (an order is never
 *      broken across sheets): the room estimate of the placement (the fill ceiling of the target's usable area less what it holds, NOSPREAD's admission order by order when the policy
 *      is on: charm-nest-nospread.js admits), the pairing rule (PAIR / PAIR2: pairFits, the source's pieces counted as moved), R16 (an order of the source that also lies on a third
 *      sheet of its colour stays where it is), no piece a person pinned (a manual mark: the whole source stays; a fresh nest keeps the target's pins, handLocks), no unit of an order bigger than a sheet (chunk);
 *    · then ONE move (cmdMovePieces: one transaction, its own checks again), the emptied draft archived (archiveEmptySheet, the cloud's way: refused in its transaction if a piece
 *      came onto it meanwhile), the later drafts numbered again (renumberAfter: Draft 1, 2, 3 with no gap) and a FRESH nest of the target asked (nestStart fresh: it packs the newcomers with what it held; a pinned piece keeps its place), said in the run's log.
 *  At most ONE combine a pass (the target then nests: the next one waits for it). Nothing is done while the sorter's onesheet setting is "off". DORMANT with the drive: the driver
 *  never runs while Charm_Nest_Open/drive is absent or "off" (mode "off" unchanged).
 *
 *  judgeMove(ctx, srcId, tId) is the same judgement for a person's Combine / Empty of two Gold Filled / Sterling Silver drafts (_charmNestCmdExec.js consolidate / empty), and
 *  targetsFor(ctx, srcId) the drafts that may take a draft's pieces, oldest first. Never calls Etsy; writes only through the driver's fenced call.                           */
"use strict";
const Place = require("../../charm-nest-place.js");
const SheetName = require("../../charm-nest-sheet-name.js");
const OpClass = require("./_charmNestOpClass");
const { busyRec } = require("./_charmNestCmdOps");

const SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets";
const FAST = ["gold", "silver"], LABEL = { gold: "Gold Filled", silver: "Sterling Silver" };
const MANUAL_MS = 90000, KEEP_MS = 600000;
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const nameOf = r => { try { return (SheetName && SheetName.name && SheetName.name(r)) || r.fileBase || r.id; } catch (_) { return (r && (r.fileBase || r.id)) || "the sheet"; } };
const orderOf = c => String(c && c.order != null ? c.order : "").split("/")[0];
const active = r => (Array.isArray(r && r.charms) ? r.charms : []).filter(c => c && !c.excluded);
const pinnedOf = c => !!(c && c.manual && typeof c.manual === "object" && !c.arrivalPin);   // (a person's mark: pinned or sized by hand in Manual Control, as charm-nest-nest-plan.js holds it; the run's own arrival pin is no person's)
const err = r => (r && r.status >= 400 ? str(r.error || "refused", 300) : r && r.error && !r.ok ? str(r.error, 300) : "");
const plural = (n, one) => `${n} ${one}${n === 1 ? "" : "s"}`;

/** Why a sheet RECORD of the run is not a draft that may take part in a combine, in words ("" when it may). */
function notDraft(rec, sets, run, now) {
  if (!rec || rec.archived) return "is gone";
  if (!FAST.includes(rec.metal)) return "is not a Gold Filled or Sterling Silver sheet";
  if (run && run.runId && rec.runId !== run.runId) return "is not a sheet of the run";
  if (rec.roseCutAt || num(rec.laserDoneAt) > 0) return "was cut";
  if (rec.rosePlanJson || rec.rosePlanHash || rec.roseProtectedJson) return "has a cut line drawn";
  if (rec.processReady || rec.laserSetPending || num(rec.committedAt) > 0) return "is approved";
  if ((rec.setId && !rec.draft) || (sets || []).some(s => s && (s.sheetIds || []).map(String).includes(rec.id))) return "is in a set";
  if (rec.cardinalPull) return "is being pulled into a set";
  if (rec.releaseFull) return "was released full";
  if (rec.topup && !rec.topup.closedAt) return "is filling its last gaps";
  if (rec.intakeFinalized) return "takes no new pieces (its intake was finalized)";
  if (rec.problem || rec.problemStop) return "needs a look";
  if (rec.runHold || (run && run.sheetHolds && run.sheetHolds[rec.id])) return "is held by the run";
  if (rec.label && rec.label.own) return "has a label of its own";
  if (rec.keepInSet || rec.ownerSet) return "was put in its set by a person";
  if (rec.manualEdit && now - num(rec.manualEdit.at) < MANUAL_MS) return "is open in Manual Control";
  if (rec.keepRelease && now - num(rec.keepRelease.at) < KEEP_MS) return "is being held right now";
  if (rec.inplaceHold) return "keeps a place for an order being made again";
  const busy = busyRec(rec, now); if (busy) return busy;
  if (Place.jobRunning(rec.nestJob, now) || Place.jobRunning(rec.filesJob, now)) return "is being nested or written";
  if (Array.isArray(rec.feedWait) && rec.feedWait.length) return "waits for its next nest";
  return "";
}

/** The run's records, its sets and the shared settings, as the placement reads them (PlaceUnit.readWorld, full records: the hold marks are read too). */
async function look(ctx) {
  const PlaceUnit = require("./_charmNestPlaceUnit");
  const sq = await ctx.db.collection(SHEETS).where("runId", "==", ctx.runId).get();
  const records = sq.docs.map(d => Object.assign({}, d.data(), { id: d.id })).filter(r => !r.archived);
  const world = await PlaceUnit.readWorld(ctx, { records });
  const run = Object.assign({}, ctx.run || {}, { runId: ctx.runId }), now = Date.now();
  const W = Place.recordHost({ run, records: world.records, sets: world.sets, settings: world.settings, preset: world.preset, partial: world.partial, now });
  return { ctx, run, now, world, W, R: W.rules, env: W.env, settings: W.settings, sets: world.sets, recs: new Map(world.records.map(r => [r.id, r])), totals: Place.pieceTotalsRec(run.lines || {}, W.views()) };
}
const viewOf = (L, id) => L.W.views().find(v => v.sheetId === id) || null;
/** The area a piece takes on a sheet (the placement's inflatedArea; a piece of unknown area counts its box), or 0 when nothing is known of its size. */
const areaOf = (L, c) => (+c.areaPt2 > 0 ? L.env.inflatedArea(c) : +c.widthPt > 0 && +c.heightPt > 0 ? +c.widthPt * +c.heightPt : 0);

/** May every piece of draft `src` go onto draft `t`? → { ok, why, n, need, room }. Never a write. */
function judge(L, src, t) {
  const S = viewOf(L, src.id), T = viewOf(L, t.id), sn = nameOf(src), tn = nameOf(t);
  if (!S || !T) return { ok: false, why: "That sheet is not a sheet of the run" };
  if (src.id === t.id || src.metal !== t.metal) return { ok: false, why: "Pick two drafts of one metal" };
  for (const [r, n] of [[src, sn], [t, tn]]) { const w = notDraft(L.recs.get(r.id) || r, L.sets, L.run, L.now); if (w) return { ok: false, why: `${n} ${w}` }; }
  const moving = active(S), held = active(T);
  if (!moving.length) return { ok: false, source: true, why: `${sn} has no pieces` };
  if (moving.some(pinnedOf)) return { ok: false, source: true, why: `${sn} holds a piece a person pinned: its pieces stay where they are` };
  if (moving.some(c => c.chunk)) return { ok: false, source: true, why: `${sn} holds part of an order bigger than one sheet: it fills its sheets in turn` };
  const rejected = (Array.isArray(L.recs.get(t.id) && L.recs.get(t.id).rejects) ? L.recs.get(t.id).rejects : []).length;
  if (rejected) return { ok: false, why: `${tn} turned pieces away at its last nest: it has no room` };
  if (moving.some(c => (c.missed || []).map(String).includes(t.id))) return { ok: false, why: `${tn} already had no room for a piece of ${sn}` };
  const oids = [...new Set(moving.map(orderOf).filter(Boolean))];
  // R16: an order of the source that also lies on a third sheet of its colour stays where it is (moving it would not bring it together)
  for (const rid of oids) {
    const third = L.W.pagesOf(src.metal).find(p => p.sheetId && p.sheetId !== src.id && p.sheetId !== t.id && active(p).some(c => orderOf(c) === rid));
    if (third) return { ok: false, why: `order ${rid} also lies on ${nameOf(third)}: its pieces on ${sn} stay where they are` };
  }
  // PAIR / PAIR2: the target with the source's orders keeps the pairing rule (the source's pieces counted as gone from it)
  if (!L.R.pairFits(T, oids, L.run, { skip: new Set(S.charms) })) return { ok: false, why: `${tn} would share orders with too many ${src.metal === "gold" ? "Sterling Silver" : "Gold Filled"} sheets (the pairing rule)` };
  // the room: the fill ceiling of the target's usable area, less what it holds now (seated or waiting)
  if ([...moving, ...held].some(c => !(areaOf(L, c) > 0))) return { ok: false, why: `the size of a piece on ${sn} or ${tn} is not known: the room cannot be judged` };
  const seatedIds = new Set((T.placements || []).map(x => x && x.id)), sum = list => list.reduce((n, c) => n + areaOf(L, c), 0);
  const sheet = { usablePt2: L.env.usableArea(T), placedPt2: sum(held.filter(c => seatedIds.has(c.id))), pendingPt2: sum(held.filter(c => !seatedIds.has(c.id))), maxFill: +L.settings.maxFill || 0.8, gapFilling: false };
  const cap = sheet.maxFill * sheet.usablePt2, need = sum(moving);
  const room = cap - sheet.placedPt2 - sheet.pendingPt2;
  if (need > room + 1e-9) return { ok: false, why: `${tn} has no room for the ${plural(moving.length, "piece")} of ${sn} (${Math.round(100 * (sheet.placedPt2 + sheet.pendingPt2) / Math.max(1, sheet.usablePt2))}% full)`, need, room };
  // NOSPREAD (when the policy is on): each order of the source admitted whole, in turn, as the placement admits an order onto a sheet
  const NS = L.env.NS();
  if (NS) {
    let pending = sheet.pendingPt2;
    for (const rid of oids) {
      const pieces = moving.filter(c => orderOf(c) === rid), key = rid + "|" + src.metal;
      const expected = (L.totals.expected.get(key) || 0) + (L.totals.expected.get(rid + "|") || 0), onSheets = L.totals.placed.get(key) || 0;
      const order = NS.orderOf({ pieces, placed: Math.max(0, onSheets - pieces.length), more: Math.max(0, expected - onSheets) }, c => areaOf(L, c));
      const a = NS.admits(Object.assign({}, sheet, { pendingPt2: pending }), order, L.env.cfg());
      if (!a.take) return { ok: false, why: `${tn} does not take order ${rid} whole (${a.why})`, need, room };
      pending += order.footprint;
    }
  }
  return { ok: true, n: moving.length, need, room, orders: oids };
}

/** The drafts of a metal that may take part, oldest first (the line order: page, then when started). */
function draftsOf(L, metal) {
  const line = L.W.pagesOf(metal).filter(v => v.sheetId);
  return line.map((v, i) => [v, i]).filter(([v]) => !notDraft(L.recs.get(v.sheetId), L.sets, L.run, L.now)).sort((a, b) => (num(a[0].page) - num(b[0].page)) || (a[1] - b[1])).map(([v]) => L.recs.get(v.sheetId));
}
/** The first combine the rules allow: { src, t, j, metal } | null, with why each pair was left (o.why). */
/* (GOLDONLY, Paul 21 Nov 2026: "I said GOLD ONLY"): the cloud's own combine, with no person asking, acts on Gold Filled drafts only; Sterling Silver drafts are combined only when a
   person presses Move all onto */
const AUTO_METALS = ["gold"];
function plan(L, why) {
  for (const metal of AUTO_METALS) {
    const ds = draftsOf(L, metal); if (ds.length < 2) continue;
    for (let ti = 0; ti < ds.length - 1; ti++) {
      const t = ds[ti];
      for (let si = ds.length - 1; si > ti; si--) { const src = ds[si], j = judge(L, src, t); if (j.ok) return { src, t, j, metal }; if (why) why.push(`${nameOf(src)} → ${nameOf(t)}: ${j.why}`); }
    }
  }
  return null;
}

/* ONE combine a pass, whoever asks (this pass item, or the gather sweep, _charmNestSweeps.js): the pass is known by its lease nonce (ctx.fence.nonce). */
const PASSES = new Map();
const passKey = ctx => (ctx && ctx.fence && ctx.fence.nonce ? String(ctx.fence.nonce) : "");
const combinedThisPass = ctx => !!passKey(ctx) && PASSES.has(passKey(ctx));
function markPass(ctx) { const k = passKey(ctx); if (!k) return; PASSES.set(k, Date.now()); if (PASSES.size > 50) PASSES.delete(PASSES.keys().next().value); }

/* The drafts are numbered again once one is archived, as a person's Delete numbers them (_charmNestCmdExec.js del, the page's removePage): every later sheet of its metal in the
   run that is not cut and not in a committed set moves up one place (page; its draftOrder too when it has one), so the names read Draft 1, 2, 3 with no gap ("Draft N" is the
   sheet's page, charm-nest-sheet-name.js draftNo). Only page / draftOrder are written (putSheet with no layout: no nest, no layout change). → the names that changed */
async function renumberAfter(ctx, gone, keyOf) {
  const out = [];
  try {
    const sq = await ctx.db.collection(SHEETS).where("runId", "==", ctx.runId).get(), tq = await ctx.db.collection(SETS).where("runId", "==", ctx.runId).get();
    const sets = tq.docs.map(d => Object.assign({ setId: d.id }, d.data())), fixed = r => sets.some(x => x && (x.committedAt || x.processReady || x.laserSetPending) && ((x.sheetIds || []).includes(r.id) || x.setId === r.setId));
    const later = sq.docs.map(d => Object.assign({}, d.data(), { id: d.id })).filter(q => !q.archived && q.id !== gone.id && q.metal === gone.metal && num(q.page) > num(gone.page) && !(q.roseCutAt || num(q.laserDoneAt) > 0) && !fixed(q))
      .sort((a, b) => num(a.page) - num(b.page));
    for (const q of later) {
      const pg = Math.max(1, num(q.page) - 1), sheet = { id: q.id, page: pg };
      if (q.draftOrder != null && Number.isFinite(+q.draftOrder) && gone.draftOrder != null && Number.isFinite(+gone.draftOrder) && +q.draftOrder > +gone.draftOrder) sheet.draftOrder = +q.draftOrder - 1;
      const was = nameOf(q), r = await ctx.call({ op: "putSheet", sheet }, keyOf(`renumber|${q.id}|${pg}`));
      if (!err(r)) out.push(`${was} is now ${nameOf(Object.assign({}, q, sheet))}`);
    }
  } catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; out.push(`the later drafts keep their numbers for now (${str((e && e.message) || e, 120)})`); }
  return out;
}

/** The pass item. ctx: the driver's. → { cursor: { at, last, moved? } } | undefined */
async function combineUnit(ctx) {
  const run = ctx.run || {}, say = t => { try { ctx.say && ctx.say("[drive] combine: " + t); } catch (_) { /* a note only */ } };
  if (combinedThisPass(ctx)) return undefined;
  if (["complete", "abandoned", "archived", "stopped"].includes(String(run.status || ""))) return undefined;
  if (run.settings && run.settings.onesheet === "off") return undefined;
  if (!(ctx.cloudSteps || []).includes("nest")) return undefined;
  // (a cheap look first: no colour with two drafts, nothing else is read)
  const q = await ctx.db.collection(SHEETS).where("runId", "==", ctx.runId).select("metal", "archived", "draft", "setId").get(), n = { gold: 0, silver: 0 };
  for (const d of q.docs) { const r = d.data() || {}; if (!r.archived && FAST.includes(r.metal) && !(r.setId && !r.draft)) n[r.metal]++; }
  if (n.gold < 2 && n.silver < 2) return undefined;
  const L = await look(ctx);
  if (L.settings.onesheet === "off") return undefined;
  const p = plan(L); if (!p) return undefined;
  const { src, t, j } = p, sn = nameOf(src), tn = nameOf(t), by = "Cloud", words = `${sn} combined onto ${tn} (one ${LABEL[p.metal]} sheet at a time)`;
  const mv = await ctx.call({ op: "cmdMovePieces", runId: ctx.runId, from: { sheetId: src.id, layoutBase: num(src.layoutRev), contentBase: num(src.contentRev) }, to: { sheetId: t.id, layoutBase: num(t.layoutRev), contentBase: num(t.contentRev) }, by, why: "combined: one sheet at a time" },
    `${ctx.runId}|combine|move|${src.id}|${t.id}|${num(src.contentRev)}|${num(t.contentRev)}`);
  if (!err(mv) && mv && mv.ok) markPass(ctx);
  if (err(mv) || !mv || !mv.ok) { say(`${sn} was not combined onto ${tn}: ${err(mv) || "no answer"}`); return { cursor: { at: Date.now(), last: `not combined: ${err(mv) || "no answer"}` } }; }
  const done = [`${plural(mv.moved, "piece")} of ${sn} moved onto ${tn}`];
  if (num(mv.from && mv.from.left) === 0) {
    const a = await ctx.call({ op: "archiveEmptySheet", id: src.id, runId: ctx.runId }, `${ctx.runId}|combine|archive|${src.id}`);
    done.push(err(a) ? `${sn} stays, empty (${err(a)})` : `${sn} was left empty and archived`);
    if (!err(a)) { const re = await renumberAfter(ctx, src, k => `${ctx.runId}|combine|${src.id}|${k}`); if (re.length) done.push(re.join(", ")); }
  } else done.push(`${sn} kept ${plural(num(mv.from && mv.from.left), "piece")}`);
  if (OpClass.isCloudOp("nestStart")) {
    const s = await ctx.call({ op: "nestStart", runId: ctx.runId, sheetId: t.id, fresh: true, why: str(words, 120) }, `${ctx.runId}|combine|nest|${t.id}|${num(mv.to && mv.to.layoutRev)}|${num(mv.to && mv.to.contentRev)}`);
    done.push(err(s) ? `its nest waits (${err(s)}); the nest step nests it` : `${tn} is nested again from scratch`);
  } else done.push(`the nest step nests ${tn}`);
  const text = `${done.join("; ")} · ${j.orders.length} order(s) moved whole`;
  say(text);
  return { cursor: { at: Date.now(), last: str(text, 300), moved: true } };
}

/** A person's Combine of two Gold Filled / Sterling Silver drafts: the same judgement. → { ok, why, n } */
async function judgeMove(ctx, srcId, tId) {
  const L = await look(ctx), src = L.recs.get(String(srcId)), t = L.recs.get(String(tId));
  if (!src || !t) return { ok: false, why: "That sheet is not a sheet of the run" };
  return judge(L, src, t);
}
/** A person's Empty of a Gold Filled / Sterling Silver draft: the drafts that take ALL its pieces, oldest first. → { targets: [record], why: [words] } */
async function targetsFor(ctx, srcId) {
  const L = await look(ctx), src = L.recs.get(String(srcId)), why = [];
  if (!src) return { targets: [], why: ["That sheet is not a sheet of the run"] };
  const w = notDraft(src, L.sets, L.run, L.now); if (w) return { targets: [], source: true, why: [`${nameOf(src)} ${w}`] };
  const targets = [];
  for (const t of draftsOf(L, src.metal)) { if (t.id === src.id) continue; const j = judge(L, src, t); if (j.ok) targets.push(t); else if (j.source) return { targets: [], source: true, why: [j.why] }; else why.push(j.why); }
  return { targets, why };
}

module.exports = { AUTO_METALS, units: { passItems: [{ name: "combine", lane: "nest", order: 55, unit: combineUnit }] }, combineUnit, combinedThisPass, renumberAfter, judgeMove, targetsFor, notDraft, judge, look, plan, FAST };
