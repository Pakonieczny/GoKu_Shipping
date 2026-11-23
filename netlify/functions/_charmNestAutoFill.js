/*  netlify/functions/_charmNestAutoFill.js — GAPFILL (Paul, 23 Nov 2026, GF_Nov.23.26_Set-1_Sheet-2: two orders were cancelled, three pieces came off, the gaps stayed: "Whenever this happens, you must
 *  automatically fill the empty gaps from orders in unfinished draft sheets"). A Gold Filled or Sterling Silver sheet that is finished and in a set nobody approved for cutting yet, and that lost pieces
 *  since its last nest (takenOffAt newer than the nest's end: judged by the record), takes WHOLE orders of the unfinished drafts of its metal into the free room, by itself.
 *    · every piece already on it stays exactly where it is and nothing is nested: an append nest cannot promise that (it may lift charms to make room, "unjam", and a piece it turns away is passed on to
 *      another draft), so the room is planned here with the nest's own pieces, grid, angles, fill ceiling and check (charm-nest-solver.js makeSheetGrid, prepareVariant, verify), as the sheet window's
 *      "Fill the freed room" plans it, and moved by the same op (cmdHopPieces fill, with its own narrow exception to SETSHUT: gap); what does not fit stays on its draft, untouched;
 *    · an order goes only whole: every piece of it on unfinished drafts of this metal, or already in this very set; never one on hold, cancelled, with a question, closed, already engraved or cut, pinned by
 *      a person, part of an order bigger than a sheet; a pair never in halves; a draft is never emptied. Biggest first, the oldest first when equal;
 *    · never a sheet cut, marked by the laser, approved, sent, open in Manual Control, busy, keeping a place for a piece made again, or whose orders are already sorted, assembled or shipped;
 *    · one try a take-off, and again (at most every 20 minutes, 12 times) only when the drafts changed. Never Rose Gold, 10K or 14K.
 *  Units: passUnit (pass item "autofill", lane "takeoff", after the take-offs; only while "files" is the cloud's too) finds the sheets and starts a process of its own for one (charmNestDrive-background
 *  { fill, runId, key }: nowFill): making the pieces' outlines takes a minute or more and never holds a pass. Its one write is fenced on the running pass's lease, as a person's request is (CMDFAST).
 *  State: Charm_Nest_Fill/<sheetId>. No Etsy call, no world build. DORMANT: the driver runs nothing while the drive is off. */
"use strict";
const crypto = require("crypto");
const NestPlan = require("../../charm-nest-nest-plan.js");
const Place = require("../../charm-nest-place.js");
const Placement = require("./_charmNestPlacement");
const SheetName = require("../../charm-nest-sheet-name.js");
const SetEdit = require("../../charm-nest-set-edit.js");
const { busyRec } = require("./_charmNestCmdOps");

const COLL = "Charm_Nest_Fill", SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets", POOL = "Charm_Pool", WORLD_LINES = "Charm_World_Lines", TIMELINE = "Order_Timeline";
const METALS = ["gold", "silver"], LABEL = { gold: "Gold Filled", silver: "Sterling Silver" };
const FRESH_MS = 72 * 3600000, AGAIN_MS = 20 * 60000, MAX_AGAIN = 12, MAX_KICKS = 6, ALIVE_MS = 4 * 60000, MANUAL_MS = 90000, KEEP_MS = 600000, PLAN_MS = 150000, MAX_ORDERS = 60, MAX_MOVE = 60, MAX_FROM = 4;
const DEAD = new Set(["abandoned", "superseded"]), MADE = new Set(["committed", "engraved", "labelled"]);
const num = v => (Number.isFinite(+v) ? +v : 0);
const ms = v => (v && typeof v.toMillis === "function" ? v.toMillis() : num(v));
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const ridOf = k => { const s = String(k || ""), i = s.indexOf("_"); return i > 0 ? s.slice(0, i) : s; };
const lineOf = id => String(id).replace(/_\d+$/, "");
const nameOf = r => { try { return SheetName.name(r) || r.fileBase || r.id; } catch (_) { return (r && (r.fileBase || r.id)) || "the sheet"; } };
const active = r => (Array.isArray(r && r.charms) ? r.charms : []).filter(c => c && !c.excluded && c.poolId);
const err = r => (r && r.status >= 400 ? str(r.error || "refused", 300) : r && r.error && !r.ok ? str(r.error, 300) : "");
const sha = s => crypto.createHash("sha256").update(String(s)).digest("hex").slice(0, 16);
const and = l => (l.length < 2 ? l.join("") : l.slice(0, -1).join(", ") + " and " + l[l.length - 1]);

/* NOTCUT (23 Nov 2026, 18:25): a sheet a person approved for cutting carries the seal laserReady (processReady is not set), a sheet whose engraving and order check are done stands at Laser cutting, and a set cut days ago is
   often not marked: none of them is ever filled. */
const laserReady = r => (Array.isArray(r && r.processSeals) ? r.processSeals : []).some(x => x && x.how === "laserReady"), atLaser = r => laserReady(r) || !!(r && r.stepState && r.stepState.engraving && r.stepState.orders);
/* FORWARD (Paul, 23 Nov 2026, 18:28: "I only want you to fill this one sheet don't fill any of the other historical sheets. All this is just moving forward"): only sets made from 18:30 that day on, and the one sheet he named (GF_Nov.23.26_Set-1_Sheet-2). */
const FROM = 1795476600000, ONE_SHEET = "d-346bcfde8d3776d7";
/** A set a sheet's gap may still be filled in: made, not approved for cutting, not sent, not cut. */
const openSet = s => !!s && !SetEdit.committedSet(s) && !s.processReady && !s.laserSetPending && !(num(s.laserDoneAt) > 0) && !/^superseded/.test(String(s.status || ""));
/** Why a sheet's gaps are not filled now, in words ("" when they may be). `sets`: the sets that list it. */
function targetWhy(rec, sets, now) {
  if (!rec || rec.archived) return "it is gone";
  if (!METALS.includes(rec.metal)) return "only Gold Filled and Sterling Silver sheets are filled";
  if (!(num(rec.takenOffAt) > 0) || now - num(rec.takenOffAt) > FRESH_MS) return "no piece came off it lately";
  if (num(rec.takenOffAt) <= Math.max(num(rec.nestJob && rec.nestJob.at), 0)) return "it was nested after its pieces came off";
  if (rec.status !== "complete" || !(rec.placements || []).length) return "it is not finished";
  if (rec.roseCutAt || num(rec.laserDoneAt) > 0) return "it was cut";
  if (rec.processReady || rec.laserSetPending || num(rec.committedAt) > 0) return "it is approved for cutting";
  if (rec.rosePlanJson || rec.rosePlanHash || rec.roseProtectedJson) return "it has a cut line";
  const list = (sets || []).filter(s => (s.sheetIds || []).map(String).includes(String(rec.id)));
  if (rec.draft || !rec.setId || list.length !== 1 || String(list[0].setId) !== String(rec.setId)) return "it is not in one set";
  if (!openSet(list[0])) return "its set is approved, sent or cut";
  if (atLaser(rec) || laserReady(list[0])) return "it is approved for cutting";
  const made = num(list[0].formedAt) || ms(list[0].createdAt); if (rec.id !== ONE_SHEET && !(made >= FROM)) return "its set is approved, sent or cut";   // (FORWARD: a set made before the rule)
  if (rec.recalled) return "it was recalled";
  if (rec.problem || rec.problemStop) return "it needs a look";
  if (rec.inplaceHold) return "it keeps a place for a piece being made again";
  if (rec.manualEdit && now - num(rec.manualEdit.at) < MANUAL_MS) return "a person is moving its pieces";
  if (rec.keepRelease && now - num(rec.keepRelease.at) < KEEP_MS) return "it is being held";
  const busy = busyRec(rec, now); if (busy) return busy;
  if (Place.jobRunning(rec.nestJob, now) || Place.jobRunning(rec.filesJob, now)) return "it is being nested or written";
  return "";
}
/** Why a sheet is not an unfinished draft orders may be taken from ("" when it is). */
function sourceWhy(rec, sets, run, now) {
  if (!rec || rec.archived) return "gone";
  if (!rec.draft || (sets || []).some(s => (s.sheetIds || []).map(String).includes(String(rec.id)))) return "in a set";
  if (run && run.runId && rec.runId !== run.runId) return "another run's";
  if (rec.roseCutAt || num(rec.laserDoneAt) > 0 || rec.processReady || rec.laserSetPending || num(rec.committedAt) > 0) return "cut or approved";
  if (rec.rosePlanJson || rec.rosePlanHash || rec.roseProtectedJson) return "a cut line is drawn";
  if (rec.releaseFull || rec.intakeFinalized || rec.cardinalPull) return "finished";
  if (rec.problem || rec.problemStop || rec.inplaceHold) return "needs a look, or keeps a place";
  if (rec.keepInSet || rec.ownerSet || (rec.label && rec.label.own)) return "a person decided about it";
  if (run && run.sheetHolds && run.sheetHolds[rec.id]) return "held";
  if (rec.manualEdit && now - num(rec.manualEdit.at) < MANUAL_MS) return "open in Manual Control";
  if (rec.keepRelease && now - num(rec.keepRelease.at) < KEEP_MS) return "being held";
  if (busyRec(rec, now) || Place.jobRunning(rec.nestJob, now) || Place.jobRunning(rec.filesJob, now)) return "busy";
  return "";
}
/** The whole orders of the unfinished drafts that may go into this set's sheet. o: { target, set, sources, run, rows: Map(poolId → row, every row of these orders), world: Map(lineKey → {problems}), cancelled: Set }
    → [{ rid, pieces: [{ c, src }], area, at }] biggest first, the oldest first when equal */
function candidates(o) {
  const inSet = new Set((o.set.sheetIds || []).map(String)), bySrc = new Map(o.sources.map(s => [String(s.id), s])), lines = (o.run && o.run.lines) || {}, orders = new Map(), bad = new Set();
  for (const src of o.sources) {
    const placed = new Set((src.placements || []).map(p => p && String(p.id)));
    for (const c of active(src)) {
      const rid = ridOf(c.poolId); if (!/^\d{4,20}$/.test(rid)) continue;
      if (!orders.has(rid)) orders.set(rid, []);
      orders.get(rid).push({ c, src });
      if (!placed.has(String(c.id)) || c.chunk || c.frozen || (c.manual && typeof c.manual === "object" && !c.arrivalPin)) bad.add(rid);   // (waiting to be nested, part of a bigger order, or pinned by a person)
    }
  }
  const rowsOf = new Map(); for (const [id, r] of o.rows) { const k = ridOf(id); if (!rowsOf.has(k)) rowsOf.set(k, []); rowsOf.get(k).push([id, r]); }
  const linesOf = new Map(); for (const [k, l] of Object.entries(lines)) { const r = ridOf(k); if (!linesOf.has(r)) linesOf.set(r, []); linesOf.get(r).push([k, l]); }
  const out = [];
  for (const [rid, pieces] of orders) {
    if (bad.has(rid) || (o.cancelled && o.cancelled.has(rid)) || pieces.length > MAX_MOVE) continue;
    const mine = new Set(pieces.map(z => String(z.c.poolId))), rows = rowsOf.get(rid) || [], live = rows.filter(([, r]) => r && !DEAD.has(String(r.state || "")));
    if (pieces.some(z => !live.some(([id]) => id === String(z.c.poolId)))) continue;
    // every piece of the order on these drafts, or already on a sheet of this set (an order is never split over two sets)
    if (!live.every(([id, r]) => !Placement.takenOff(r) && (mine.has(id) ? bySrc.has(String(r.sheetId || "")) && !MADE.has(String(r.state || "")) : inSet.has(String(r.sheetId || ""))))) continue;
    // every line of it made: none on hold, changed, waiting, with a question, or closed
    const ls = linesOf.get(rid) || []; if (!ls.length) continue;
    const fine = ([k, l]) => { const st = String((l && l.state) || ""); if (["noDesign", "skipped", "gone"].includes(st)) return true;
      const w = o.world && o.world.get(k); return (st === "pooled" || st === "written") && (l.poolIds || []).length > 0 && !l.hold && !l.changePending && !l.designHold && !l.vacated && !l.etsyClosed && !l.wait && !(w && (w.problems || []).length); };
    if (!ls.every(fine)) continue;
    const at = Math.min(...pieces.map(z => num((o.rows.get(String(z.c.poolId)) || {}).orderDate)).filter(v => v > 0), Infinity);
    out.push({ rid, pieces, area: pieces.reduce((n, z) => n + num(z.c.areaPt2), 0), at: isFinite(at) ? at : 9e15 });
  }
  return out.sort((a, b) => b.area - a.area || a.at - b.at || a.rid.localeCompare(b.rid)).slice(0, MAX_ORDERS);
}

/* ── the plan: where they lie, with the nest's own grid ── */
/** o: { Solver, job (NestPlan.buildJob of the sheet with every candidate's piece on it, unplaced), placed (the saved placements), cands, left: Map(sheetId → pieces it holds), budgetMs, verifyRes }
    → { orders: [{ rid, spots: [{ poolId, id, src, cxPt, cyPt, angle, wPt, hPt }] }], placements, why } */
async function plan(o) {
  const Sv = o.Solver, job = o.job, res = 2, ringR = 4, end = Date.now() + (o.budgetMs || PLAN_MS), pieces = new Map(job.pieces.map(p => [String(p.id), p])), clear = job.clearancePt;
  const angles = [...new Set((job.angles && job.angles.length && job.angles.length <= 36 ? job.angles : Array.from({ length: 36 }, (_, i) => i * 10)).map(a => ((a % 360) + 360) % 360))];
  const VARS = new Map(), variant = (p, a) => { const k = p.id + "@" + a; if (!VARS.has(k)) VARS.set(k, Sv.prepareVariant({ bits: p.bits, w: p.w, h: p.h, scale: p.scale }, a, clear, res)); return VARS.get(k); };
  const fixed = [], unknown = []; let used = 0;
  for (const p of o.placed) { const q = pieces.get(String(p.id)); if (!q) { unknown.push(p.id); continue; } fixed.push({ piece: { bits: q.bits, w: q.w, h: q.h, scale: q.scale }, placement: p }); const v = variant(q, +p.angle || 0); used += v ? v.fine.pm.cells : 0;   // (what a piece occupies on the grid: the measure of the nest's own fill limit)
    }
  if (unknown.length) return { orders: [], placements: [], why: `${unknown.length} piece(s) on it could not be drawn` };
  const usable = Sv.makeSheetGrid(Object.assign({}, job.sheet), clear, res).usableCells, cap = (job.maxFill > 0 && job.maxFill < 1 ? job.maxFill : 1) * usable;
  const grid0 = Sv.makeSheetGrid(Object.assign({}, job.sheet, { fixedPieces: fixed }), clear, res), used0 = used, breathe = async () => { await new Promise(r => setImmediate(r)); if (o.beat) await o.beat(); };
  /* one way of filling: each order in turn at its tightest legal spot (most contact with its neighbours), `lean` breaking the near ties toward one side: pieces pushed together leave the rest of the room whole */
  const way = async (lean, until) => {
  let grid = grid0.clone(), used = used0, chosen = []; const left = new Map(o.left);
  for (const k of o.cands) {
    if (Date.now() > until || chosen.reduce((n, x) => n + x.spots.length, 0) + k.pieces.length > MAX_MOVE) break;
    const from = new Map(); for (const z of k.pieces) from.set(String(z.src.id), (from.get(String(z.src.id)) || 0) + 1);
    if (new Set([...chosen.flatMap(x => x.spots.map(s => s.src)), ...from.keys()]).size > MAX_FROM) continue;
    if ([...from].some(([id, n]) => (left.get(id) || 0) - n < 1)) continue;   // (a draft is never emptied)
    const list = k.pieces.map(z => Object.assign({ q: pieces.get(String(z.c.id)) }, z)); if (list.some(z => !z.q)) continue;
    let g = grid.clone(), cells = used, spots = [];
    for (const z of list.sort((a, b) => num(b.q.areaPt2) - num(a.q.areaPt2))) {
      g.buildSAT(); let best = null;
      const score = (v, x, y) => { if (!g.fits(v.fine.pm, x, y)) return null; return g.overlap(v._ring, x - ringR, y - ringR, 1e9) / Math.max(1, v._ring.cells) + lean[0] * (x / g.W) + lean[1] * (y / g.H); };
      for (const a of angles) {
        const v = variant(z.q, a); if (!v || cells + v.fine.pm.cells > cap) continue;
        if (!v._ring) { const rg = Sv.ring(v.fine.bits, v.fine.w, v.fine.h, ringR); v._ring = Sv.packShifted(rg.bits, rg.w, rg.h); }
        const w = v.fine.w, h = v.fine.h, full = w * h - v.fine.pm.cells;
        for (let y = 0; y + h <= g.H; y += 2) for (let x = 0; x + w <= g.W; x += 2) { if (g.boxSum(x, y, x + w, y + h) > full) continue; const s = score(v, x, y); if (s != null && (!best || s > best.s + 1e-9)) best = { v, x, y, s }; }
        await breathe();
      }
      if (!best) { spots = null; break; }
      for (let dy = -1; dy <= 1; dy++) for (let dx = -1; dx <= 1; dx++) { const s = score(best.v, best.x + dx, best.y + dy); if (s != null && s > best.s + 1e-9) best = Object.assign({}, best, { x: best.x + dx, y: best.y + dy, s }); }
      g.stamp(best.v.fine.bits, best.v.fine.w, best.v.fine.h, best.x, best.y); cells += best.v.fine.pm.cells;
      spots.push({ poolId: String(z.c.poolId), id: String(z.c.id), src: String(z.src.id), cxPt: (best.x + best.v.solid.cx) / res, cyPt: (best.y + best.v.solid.cy) / res, angle: best.v.angle, wPt: best.v.solid.w / res, hPt: best.v.solid.h / res });
    }
    if (!spots) continue;
    grid = g; used = cells; for (const [id, n] of from) left.set(id, left.get(id) - n);
    chosen.push({ rid: k.rid, spots });
  }
  return { chosen, used };
  };
  // the ways, the plain one first; the one that covers the most of the plate is kept (the plain one when equal)
  let best = null, chosen = [];
  for (const lean of [[0, 0], [-.6, -.1], [.6, -.1], [-.1, .6], [-.1, -.6]]) { if (best && Date.now() > end - (o.budgetMs || PLAN_MS) / 3) break; const w = await way(lean, end); if (!best || w.used > best.used) best = w; }
  chosen = best.chosen; used = best.used;
  // the check every nest has (Solver.verify), on the whole sheet: the pieces on it as they lie, and the new ones; an order the check does not pass is left where it is
  const lay = list => o.placed.concat(list.flatMap(x => x.spots.map(s => ({ id: s.id, angle: s.angle, cxPt: s.cxPt, cyPt: s.cyPt, wPt: s.wPt, hPt: s.hPt, scale: NestPlan.CHARM_SCALE }))));
  const vjob = Object.assign({}, job, { lockedPlacements: o.placed.map(p => Object.assign({}, p)) }), was = Sv.verify(vjob, o.placed, o.verifyRes || 6);
  const check = list => Sv.verify(vjob, lay(list), o.verifyRes || 6), newIds = list => new Set(list.flatMap(x => x.spots.map(s => s.id)));
  while (chosen.length) {
    const v = check(chosen), ids = newIds(chosen), hit = new Set([...(v.overlappingPairs || []).flat(), ...(v.cornerIds || [])].map(String).filter(id => ids.has(id)));
    if (!hit.size && v.outsidePx <= was.outsidePx && v.removedPx <= was.removedPx && v.cornerPx <= was.cornerPx && !v.protectedMoved) break;
    const drop = hit.size ? chosen.filter(x => x.spots.some(s => hit.has(s.id))) : [chosen[chosen.length - 1]];
    chosen = chosen.filter(x => !drop.includes(x));
  }
  return { orders: chosen, placements: lay(chosen), fill: usable ? used / usable : 0, cap: usable ? cap / usable : 0, why: chosen.length ? "" : "no waiting order fits the room" };
}

/* ── the pass item: which sheets are due, and their process started ── */
const FIELDS = ["processSeals", "stepState", "metal", "takenOffAt", "archived", "draft", "setId", "status", "nestJob", "filesJob", "roseCutAt", "laserDoneAt", "processReady", "laserSetPending", "committedAt", "inplaceHold", "manualEdit", "keepRelease", "problem", "problemStop", "recalled", "runId", "saving", "fileBase", "sheetIndex", "rosePlanHash", "placedCount"];
/** What the drafts of a metal hold now, as a word that changes when they do. */
async function draftSig(db, runId, metal) {
  const q = await db.collection(SHEETS).where("runId", "==", runId).where("draft", "==", true).select("metal", "contentRev", "layoutRev", "setId", "releaseFull", "archived").get();
  return sha(q.docs.map(d => [d.id, d.data() || {}]).filter(([, r]) => r.metal === metal && !r.archived).map(([id, r]) => `${id}:${num(r.contentRev)}:${r.releaseFull ? 1 : 0}`).sort().join("|"));
}
async function kick(body, o) {
  if (o && typeof o.kick === "function") return o.kick(body);
  const fetch = require("node-fetch"), origin = String(process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app").replace(/\/+$/, "");
  try { const r = await Promise.race([fetch(`${origin}/.netlify/functions/charmNestDrive-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }), new Promise(r => setTimeout(r, 4000))]); return !!(r && r.status >= 200 && r.status < 300); } catch (_) { return false; }
}
async function passUnit(ctx) {
  const now = Date.now(), steps = (ctx.cloudSteps || []).map(String), run = ctx.run || {};
  if (!steps.includes("files") || ["abandoned", "complete"].includes(String(run.status || ""))) return undefined;   // (a sheet that changed needs its files written)
  const q = await ctx.db.collection(SHEETS).where("takenOffAt", ">", now - FRESH_MS).select(...FIELDS).get();
  const recs = q.docs.map(d => Object.assign({}, d.data() || {}, { id: d.id })).filter(r => r.runId === ctx.runId && METALS.includes(r.metal) && r.setId && !r.draft && !r.archived);
  if (!recs.length) return undefined;
  const [fills, sq] = [await ctx.db.getAll(...recs.map(r => ctx.db.collection(COLL).doc(r.id))), await ctx.db.collection(SETS).where("runId", "==", ctx.runId).select("setId", "sheetIds", "committedAt", "processReady", "processSeals", "laserSetPending", "laserDoneAt", "status", "formedAt", "createdAt").get()];
  const sets = sq.docs.map(d => Object.assign({ setId: d.id }, d.data() || {})).filter(s => !/^superseded/.test(String(s.status || ""))), sigs = new Map(); let started = 0;
  for (let i = 0; i < recs.length && started < 2; i++) {
    const r = recs[i], f = fills[i].exists ? fills[i].data() || {} : {}, same = num(f.takenOffAt) === num(r.takenOffAt);
    if (targetWhy(Object.assign({ placements: [1] }, r), sets, now)) continue;
    if (same && ["asked", "working"].includes(f.state) && now - Math.max(num(f.kickedAt), num(f.beatAt)) < ALIVE_MS) continue;
    if (same && f.state === "refused") continue;
    if (!sigs.has(r.metal)) sigs.set(r.metal, await draftSig(ctx.db, ctx.runId, r.metal));
    const sig = sigs.get(r.metal), ended = same && ["done", "none"].includes(f.state);
    if (ended && (f.sig === sig || num(f.again) >= MAX_AGAIN || now - num(f.endedAt) < (/nesting|saving|written|busy/i.test(String(f.text || "")) ? 15000 : AGAIN_MS))) continue;   // (one try a take-off; again only when the drafts changed)
    if (same && !ended && num(f.kicks) >= MAX_KICKS) continue;
    const key = crypto.randomBytes(16).toString("hex");
    await ctx.db.collection(COLL).doc(r.id).set({ sheetId: r.id, runId: ctx.runId, key, state: "asked", kickedAt: now, beatAt: 0, takenOffAt: num(r.takenOffAt), sig, kicks: same && !ended ? num(f.kicks) + 1 : 1, again: ended ? num(f.again) + 1 : same ? num(f.again) : 0, expireAt: new Date(now + 14 * 86400000) }, { merge: true });
    const went = await kick({ fill: r.id, runId: ctx.runId, key }, ctx); started++;
    if (!went) await ctx.db.collection(COLL).doc(r.id).set({ kickedAt: 0 }, { merge: true });
    if (ctx.say) ctx.say(`[fill] ${nameOf(r)}: its gaps are being filled (${went ? "started" : "not started yet"})`);
  }
  return undefined;
}

/* ── one sheet, by a process of its own ── */
/** The pool rows of these orders, every one: Map(poolId → row). */
async function rowsOfOrders(db, rids) {
  const out = new Map();
  for (let i = 0; i < rids.length; i += 15) { const part = rids.slice(i, i + 15), vals = part.concat(part.filter(r => Number.isSafeInteger(+r)).map(Number)); const q = await db.collection(POOL).where("orderId", "in", vals).get(); for (const d of q.docs) out.set(d.id, d.data() || {}); }
  return out;
}
async function worldOf(db, keys) {
  const out = new Map();
  for (let i = 0; i < keys.length; i += 100) for (const d of await db.getAll(...keys.slice(i, i + 100).map(k => db.collection(WORLD_LINES).doc(k)))) { if (!d.exists) continue; let x = null; try { x = JSON.parse((d.data() || {}).data || "{}"); } catch (_) { x = null; } if (x) out.set(d.id, { problems: Array.isArray(x.problems) ? x.problems : [] }); }
  return out;
}
/** Whether a station has worked on one of these orders since the set was made (sorted, assembled, shipped): its sheet is cut, whatever its mark says. */
async function atStation(db, rids, since) {
  for (let i = 0; i < rids.length; i += 10) { const q = await db.collection(TIMELINE).where("orderId", "in", rids.slice(i, i + 10)).where("type", "in", ["sorted", "assembled", "shipped"]).limit(200).get(), hit = q.docs.map(d => d.data() || {}).find(e => ms(e.at) > num(since)); if (hit) return String(hit.orderId || "an order"); }
  return "";
}
async function nowFill(o) {
  const { db, Lib, sheetId, runId, key } = o, say = o.say || (() => {}), ref = db.collection(COLL).doc(String(sheetId)), s0 = await ref.get(), f = s0.exists ? s0.data() || {} : {}, Nest = require("./_charmNestNest"), Solver = require("./_charmNestSolver");
  if (!f.key || f.key !== key || f.runId !== runId || f.state !== "asked") return { skipped: "not this sheet's turn" };
  await ref.set({ state: "working", beatAt: Date.now(), startedAt: Date.now() }, { merge: true });
  const end = (state, text, more) => ref.set(Object.assign({ state, text: str(text, 400), endedAt: Date.now() }, more || {}), { merge: true }).then(() => ({ [state]: text }));
  let beatAt = Date.now(); const beat = async () => { if (Date.now() - beatAt > 15000) { beatAt = Date.now(); await ref.set({ beatAt }, { merge: true }).catch(() => {}); } };
  try {
    const read = b => Lib.call(b, {}), now = Date.now();
    const [got, dv, all, tq] = await Promise.all([read({ op: "runGet", runId }), db.collection("Charm_Nest_Open").doc("drive").get(), db.collection(SHEETS).where("runId", "==", runId).get(), db.collection(SETS).where("runId", "==", runId).get()]);
    const run = got && got.run ? Object.assign({}, got.run, { runId }) : null, drive = dv.exists ? dv.data() || {} : {};
    if (!run || ["abandoned", "complete"].includes(String(run.status || ""))) return end("refused", "the run is closed");
    if (drive.mode !== "on" && drive.mode !== "standby") return end("none", "the drive is off");
    const recs = all.docs.map(d => Object.assign({}, d.data() || {}, { id: d.id })).filter(r => !r.archived), sets = tq.docs.map(d => Object.assign({ setId: d.id }, d.data() || {})).filter(s => !/^superseded/.test(String(s.status || "")));
    const rec = recs.find(r => r.id === sheetId), why = targetWhy(rec, sets, now); if (why) return end(/^(it was cut|it is approved|its set is|it is gone)/.test(why) ? "refused" : "none", why);
    if (recs.some(r => String(r.setId || "") === String(rec.setId) && !r.draft && laserReady(r))) return end("refused", "it is approved for cutting");   // (NOTCUT: any sheet of its set)
    const name = nameOf(rec), set = sets.find(s => String(s.setId) === String(rec.setId)), mine = [...new Set(active(rec).map(c => ridOf(c.poolId)))];
    const worked = await atStation(db, mine, num(set.formedAt) || ms(set.createdAt)); if (worked) return end("refused", `order ${worked} on it is already being sorted or assembled: the sheet is cut`);
    // a piece taken off to be made again comes back to its own place: the room is its
    const back = await db.collection(POOL).where("vacatedFrom", "==", sheetId).get();
    if (back.docs.some(d => { const r = d.data() || {}; return !DEAD.has(String(r.state || "")) && !recs.some(x => (x.poolIds || []).map(String).includes(d.id)); }) || Object.values(run.lines || {}).some(l => l && l.vacated && JSON.stringify(l.vacated).includes(String(sheetId)))) return end("none", "a piece made again comes back to its place on it first", { sig: null });
    const sources = recs.filter(r => r.metal === rec.metal && r.id !== rec.id && ["", "busy"].includes(sourceWhy(r, sets, run, now)));   // (DRAFTBUSY: a draft being nested gives its orders once it is still)
    if (!sources.length) return end("none", `no unfinished ${LABEL[rec.metal]} draft to take an order from`);
    const rids = [...new Set(sources.flatMap(s => active(s).map(c => ridOf(c.poolId))))].filter(r => /^\d{4,20}$/.test(r)), keys = Object.keys(run.lines || {}).filter(k => rids.includes(ridOf(k)));
    const [rows, world, cx] = await Promise.all([rowsOfOrders(db, rids), worldOf(db, keys), read({ op: "cancelCheck", orderIds: rids.slice(0, 400) })]);
    if (!cx || cx.error) return end("none", "the cancelled orders could not be read", { sig: null });
    const cc = cx.cancelled || cx.orders || {}, cancelled = new Set(Array.isArray(cc) ? cc.map(String) : Object.keys(cc).filter(k => cc[k]));
    const cands = candidates({ target: rec, set, sources, run, rows, world, cancelled });
    if (!cands.length) return end("none", `no whole order on an unfinished ${LABEL[rec.metal]} draft may go into ${set.name || "its set"}`);
    // the pieces, made again as a nest makes them
    const [S, policy, tRows] = await Promise.all([Nest.readSettings(db), Nest.readPolicy(db), Nest.poolRowsOf(db, rec)]), plate = Nest.plateOf(rec, S, null, policy);
    const Pieces = (o.pieces || require("./_charmNestNestPieces").create)({ db, bucket: o.bucket || require("./firebaseAdmin").storage().bucket(), S, log: t => say(t), budgetMs: 9 * 60000 });
    const built = await Pieces.rebuild(rec, tRows, beat), have = new Set(built.charms.map(c => String(c.id))), saved = (rec.placements || []).filter(p => p && [p.cxPt, p.cyPt].every(v => Number.isFinite(+v))).map(p => Object.assign({ id: String(p.id), angle: +p.angle || 0, cxPt: +p.cxPt, cyPt: +p.cyPt, wPt: num(p.wPt), hPt: num(p.hPt), scale: num(p.scale) || NestPlan.CHARM_SCALE }, num(p.fixScale) > 0 ? { fixScale: num(p.fixScale) } : {}));
    const lost = saved.filter(p => !have.has(p.id)); if (lost.length) return end("none", `${lost.length} piece(s) on ${name} could not be drawn in the cloud: nothing was moved`, { sig: null });
    const ids = new Set(cands.flatMap(k => k.pieces.map(z => String(z.c.id)))), cCharms = [];
    for (const src of sources) { const part = Object.assign({}, src, { charms: active(src).filter(c => ids.has(String(c.id))) }); if (!part.charms.length) continue; const b = await Pieces.rebuild(part, new Map(part.charms.map(c => [String(c.poolId), rows.get(String(c.poolId))])), beat); cCharms.push(...b.charms); }
    const drawn = new Map(cCharms.map(c => [String(c.id), c])), ok = cands.filter(k => k.pieces.every(z => drawn.has(String(z.c.id)))).map(k => Object.assign({}, k, { area: k.pieces.reduce((n, z) => n + num(drawn.get(String(z.c.id)).areaPt2), 0) })).sort((a, b) => b.area - a.area || a.at - b.at || a.rid.localeCompare(b.rid));
    for (const c of built.charms) { const p = saved.find(x => x.id === String(c.id)); c.pinned = p ? Object.assign({ cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle }, c.manual && c.manual.size ? { size: c.manual.size } : {}) : null; if (p && !c.manual) c.arrivalPin = true; }
    for (const c of cCharms) { c.pinned = null; delete c.arrivalPin; c.metal = rec.metal; }
    const onIt = new Set(saved.map(p => p.id)), view = Object.assign({ metal: rec.metal, runId, sheetId: rec.id, charms: built.charms.filter(c => onIt.has(String(c.id))).concat(cCharms), placements: saved.slice(), rejects: [], topup: null, roseCutAt: null, roseProtected: null, nestFlow: "standard", nestInitial: saved.map(p => Object.assign({}, p)), appendOnly: true, intakeAppend: true, runSeed: 1 }, plate);
    const env = Object.assign(Nest.planEnv(S), { nestItems: sh => NestPlan.activeCharms(sh) }), job = NestPlan.buildJob(view, env);
    const got2 = await plan({ Solver, job, placed: saved, cands: ok, left: new Map(sources.map(s => [String(s.id), active(s).length])), budgetMs: o.planMs || PLAN_MS, beat, verifyRes: [4, 6, 8].includes(+S.settings.verifyRes) ? +S.settings.verifyRes : 6 });
    if (!got2.orders.length) return end("none", got2.why || "no waiting order fits the room");
    // the one write, under the running pass's lease (as CMDFAST): a pass that is not running is asked for
    const spots = got2.orders.flatMap(x => x.spots), from = [...new Set(spots.map(s => s.src))], text = `${spots.length} piece${spots.length === 1 ? "" : "s"} of order${got2.orders.length === 1 ? "" : "s"} ${and(got2.orders.map(x => x.rid))} from ${and(from.map(id => nameOf(recs.find(r => r.id === id))))} fill the room pieces left on ${name}`;
    const nap = t => new Promise(r => setTimeout(r, t)), lease = async t => { for (const e = Date.now() + t; ;) { const l = await db.collection("Charm_Nest_Drive").doc("lease").get(), L = l.exists ? l.data() || {} : {}; if (L.state === "running" && L.nonce) return L.nonce; if (Date.now() > e) return null; await nap(400); } };
    let nonce = await lease(o.leaseMs == null ? 20000 : o.leaseMs); if (!nonce && Lib.driveSoon) { await Lib.driveSoon("fill waits").catch(() => null); nonce = await lease(o.leaseMs == null ? 60000 : o.leaseMs); }
    if (!nonce) { await ref.set({ state: "asked", kickedAt: 0, beatAt: 0 }, { merge: true }); return { again: "no pass runs" }; }
    /* DRAFTBUSY (23 Nov 2026, 18:41, GF Sheet 2: "GF Draft 22 is nesting: try again once it is done"): the draft orders come from is nested again at every new order, minutes of every quarter hour: the plan was made and thrown away, and the
       next try was 20 minutes later. The write waits for a moment the draft is still (its pieces' places on the draft do not matter: they leave it), six minutes at most; then the next pass asks again. */
    const write = n => Lib.call({ op: "cmdHopPieces", runId, from, to: { sheetId: rec.id, layoutBase: num(rec.layoutRev) }, poses: spots.map(s => ({ poolId: s.poolId, cxPt: s.cxPt, cyPt: s.cyPt, angle: s.angle, wPt: s.wPt, hPt: s.hPt })), fill: true, gap: { setId: String(set.setId), takenOffAt: num(rec.takenOffAt), note: text }, by: "Cloud", person: "Cloud" }, { unit: `gapfill|${rec.id}|${num(rec.takenOffAt)}|${num(rec.layoutRev)}|${sha(spots.map(s => s.poolId).sort().join(","))}|${n}`, fence: { path: "Charm_Nest_Drive/lease", nonce, runId } });
    let r = null; for (const until = Date.now() + (o.busyMs == null ? 6 * 60000 : o.busyMs), step = o.busyStep == null ? 2500 : o.busyStep, t0 = { n: 0 }; ;) {
      r = await write(t0.n++);
      if (!(r && (r.busy || r.fenced)) || Date.now() > until) break;
      await ref.set({ beatAt: Date.now() }, { merge: true }).catch(() => {}); await nap(step);
      const fresh = await lease(o.leaseMs == null ? 60000 : o.leaseMs); if (!fresh) break; nonce = fresh;
    }
    if (r && (r.busy || r.fenced)) { say(`[fill] ${name}: ${err(r)}: asked again at the next pass`); await ref.set({ state: "asked", kickedAt: 0, beatAt: 0, kicks: 0 }, { merge: true }); return { again: err(r) || "busy" }; }
    if (err(r) || !r) { const w = err(r) || "no answer"; say(`[fill] ${name}: ${w}`); return r && (r.conflict || r.busy || r.gone || r.fenced || r.taken) ? end("none", w, { sig: null }) : end("none", w); }
    await Lib.call({ op: "timelineAdd", events: got2.orders.map(x => ({ orderId: x.rid, type: "note", by: "Cloud", station: "sorter", sheetId: rec.id, sheet: str(name, 80), id: `gapfill-${rec.id}-${num(rec.takenOffAt)}`, at: Date.now(), text: `Moved to ${name} to fill the room an order left there`.slice(0, 200), data: { gapFill: true, from } })) }, { unit: `gapfill-note|${rec.id}|${num(rec.takenOffAt)}|${sha(text)}`, fence: { path: "Charm_Nest_Drive/lease", nonce, runId } }).catch(() => null);
    say(`[fill] ${text} (${Math.round(100 * got2.fill)}% full)`);
    return end("done", text, { moved: spots.map(s => s.poolId), orders: got2.orders.map(x => x.rid), fill: got2.fill, sig: await draftSig(db, runId, rec.metal) });
  } catch (e) { await ref.set({ state: "asked", kickedAt: 0, beatAt: 0, error: str((e && e.message) || e, 300) }, { merge: true }).catch(() => {}); throw e; }
}

module.exports = { units: { passItems: [{ name: "autofill", lane: "takeoff", order: 35, unit: passUnit }] }, passUnit, nowFill, plan, candidates, targetWhy, sourceWhy, openSet, draftSig, COLL };
