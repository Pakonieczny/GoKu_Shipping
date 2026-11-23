/*  netlify/functions/_charmNestNestSlot.js — one SLOT of a cloud nest job: charmNestSolve-background { nonce, jobId, slot } (cloud plan v2 §1.1 a, build step 6).
 *  DORMANT: a slot begins only with the nonce of its own lease (Charm_Nest_Jobs/<jobId>/slots/<slot>, _charmNestLease.js, heartbeat on), which only the driver's nest
 *  step and the job's own slot 0 hand out, and its lease refuses while Charm_Nest_Open/drive is absent or "off", or while a computer carries the job's run.
 *
 *  A slot, in one invocation (12 minutes at most of Netlify's 15):
 *    begin    the lease, reading in the same transaction the job (not ended), the drive (standby or on) and the run's carrier (the cloud).
 *    prepare  (slot 0, once per job) the solver's input, from the sheet's record at the job's two revisions (layoutBase, contentBase: a record that moved since makes
 *             the job "stale", and the driver asks for a new one): the shared Settings, the plate and its physical sheet, each live piece made again with its
 *             silhouette (_charmNestNestPieces.js), the turn of orders (NestPlan.feedTurn), the solver job (NestPlan.buildJob), the number of slots
 *             (NestPlan.searchCount) and the KEEPSEARCH plan (NestPlan.keepPlan). Kept once at charmnest/jobs/<jobId>/input.json.gz, so every slot and every
 *             attempt searches the very same pieces. Then the other slots are started.
 *    search   the slot's searches (NestPlan.slotJob, then KEEPSEARCH's next placing orders while the job's count and the slot's time last: job.keep.next), seed
 *             hash(jobId) + variant, each with the time left; the best layout so far is kept on the slot's document at most every 5 s (its checkpoint). A slot
 *             cut off (its invocation killed, its beat stale) is taken up again by a new holder FROM that checkpoint: every piece it had seated stays exactly where
 *             it is (NestPlan.resumeJob: lockedPlacements) and the rest are placed around them.
 *    pick     the slot that finds every slot finished takes the pick (job.picking, once): NestPlan.finalPick over the slots' best layouts, the orders kept whole
 *             (NestPlan.keepOrdersWhole), the layout checked (Solver.verify), job.result written and the sheet's nestJob marked "nested". A layout is never written
 *             to the sheet here: the sheet writer (build step 7a) applies it with layoutBase and contentBase, so a hand move or an artwork edit made during the nest
 *             refuses it (P2).
 *  Every write is fenced: a transaction that first reads the slot's lease (its nonce, running) and the run's carrier (the cloud). A slot's beat (every 20 s) renews
 *  the lease, the job's beatAt and the run's carrier.at (C6); it reads the stop asked for (stopJob) every 4 s, and a stop ends the search within one step of it.
 *  The nonce is read again before every Storage upload (C7). At the job's end the driver is told (driveSoon("task")). No Etsy, no world build, no page.            */
"use strict";
const zlib = require("zlib");
const Nest = require("./_charmNestNest");
const Lease = require("./_charmNestLease");
const NestPlan = require("../../charm-nest-nest-plan.js");
const CutLine = require("../../charm-nest-rose.js");
const O = require("../../charm-nest-orders.js");
const NoSpread = require("../../charm-nest-nospread.js");
const Solver = require("./_charmNestSolver");

const BUDGET_MS = 12 * 60000, TAIL_MS = 60000, BEAT_MS = 20000, POLL_MS = 4000, CHECKPOINT_MS = 5000, PREPARE_MS = 10 * 60000, PICK_STALE_MS = 2 * 60000, MIN_SEARCH_MS = 60000;
const { num, str, ENDED } = Nest;
class Fenced extends Error { constructor(why) { super(why || "fenced"); this.fenced = true; } }
const parse = t => { try { return t ? JSON.parse(t) : null; } catch (_) { return null; } };

/* ── the input: the solver job with each piece's silhouette packed one bit a cell (as the page sends its server nest), gzipped ── */
function packBits(bits) { const out = Buffer.alloc((bits.length + 7) >> 3); for (let i = 0; i < bits.length; i++) if (bits[i]) out[i >> 3] |= 1 << (i & 7); return out.toString("base64"); }
function unpackBits(b64, n) { const buf = Buffer.from(String(b64 || ""), "base64"), out = new Uint8Array(n); for (let i = 0; i < n; i++) out[i] = (buf[i >> 3] >> (i & 7)) & 1; return out; }
const packJob = job => Object.assign({}, job, { pieces: job.pieces.map(p => Object.assign({}, p, { bits: packBits(p.bits), packed: true })) });
const unpackJob = job => Object.assign({}, job, { pieces: job.pieces.map(p => { const n = (p.w | 0) * (p.h | 0), q = Object.assign({}, p, { bits: p.packed ? unpackBits(p.bits, n) : Uint8Array.from(p.bits || []) }); delete q.packed; return q; }) });
async function loadInput(bucket, path) { const [buf] = await bucket.file(path).download(); return JSON.parse(zlib.gunzipSync(buf).toString("utf8")); }
/** A layout as text (its nested lists never reach a Firestore field) and a placement as a flat map (numbers, words and flags only). */
const layoutText = l => JSON.stringify(Solver.publicLayout(l));
const flat = o => { const out = {}; for (const [k, v] of Object.entries(o || {})) if (v === null || ["number", "string", "boolean"].includes(typeof v)) out[k] = v; return out; };
/** What the pick reads of each piece (keepOrdersWhole, and the writer after it): no bitmap. */
const metaOf = c => flat({ id: c.id, poolId: c.poolId || null, order: c.order || null, orderDate: num(c.orderDate), frontAt: num(c.frontAt) || null, chunk: c.chunk || null, groupKey: c.groupKey || null,
  groupSize: c.groupSize == null ? null : num(c.groupSize), side: c.side || null, bodyIndex: c.bodyIndex == null ? null : num(c.bodyIndex), mirror: c.mirror == null ? null : !!c.mirror, hash: c.hash || null, name: c.name || null,
  sku: c.sku || null, areaPt2: num(c.areaPt2), widthPt: num(c.widthPt), heightPt: num(c.heightPt), arrivalPin: !!c.arrivalPin, excluded: false });
const metaCharm = (c, orderInfo, pinned) => Object.assign(metaOf(c), { orderInfo: orderInfo || null, pinned: pinned || null });

/* ── the protected green-line layout of a cut-line sheet (the page's RoseStock.protect: a saved plan, or the lines its record keeps), checked against its saved placements ── */
function protectedOf(rec, saved, word) {
  if (!CutLine.cuts(rec.metal) || rec.roseCutAt) return null;
  const plan = parse(rec.rosePlanJson);
  if (plan && plan.profile) {
    const ids = new Set((plan.shapes || []).map(s => s.id)), pl = saved.filter(p => ids.has(p.id));
    if (pl.length !== ids.size) throw Object.assign(new Error(`the saved ${word} layout does not match the sheet's placements`), { words: true });
    return { profile: plan.profile, lines: plan.lines, placements: pl.map(p => Object.assign({}, p)), shapes: plan.shapes, stages: plan.stages };
  }
  const g = parse(rec.roseProtectedJson); if (!g) return null;
  for (const p of g.placements || []) {
    const q = saved.filter(x => x.id === p.id);
    if (q.length !== 1 || ["cxPt", "cyPt", "angle"].some(k => !Number.isFinite(q[0][k]) || Math.abs(q[0][k] - p[k]) > 0.001)) throw Object.assign(new Error(`the saved ${word} layout does not match the sheet's placements`), { words: true });
  }
  return g;
}

/* ── NQ (Paul, 21 Nov 2026: "the GF and SS sheets nested by the cloud leave big empty spaces ... the 10K / 14K nest packs much better"): a Gold Filled or Sterling Silver sheet was
   nested three orders at a time (NestPlan.feedTurn), every nest after the first pinned each piece already on the sheet where it lay (the run's own pin, arrivalPin) and placed the
   next few around it, in ONE search; each little group was seated against the last and the gaps between them were never filled again (44 pieces: eight nests, eight walls).
   10K and 14K see their whole batch in one search, several careful searches side by side (multiStart, KEEPSEARCH) and keep the least wasteful layout (tight).
   Now a GF / SS sheet that is still being formed (a draft of the run, or a sheet in no set; never one cut, laser cut, released full, committed or in a set) is laid out whole by every
   nest, "Nest the rest" as much as "Nest": the whole batch, oldest orders first, the 10K / 14K search, the least wasteful layout kept; only a person's pin (Manual Control: c.manual)
   holds its piece where it is. Beside those searches one slot appends to the saved layout as before (altJob), and the pick prefers the layout that keeps every piece already on the
   sheet (wholePick): laying the sheet out again never seats fewer of them. ── */
/* NOWHOLE (Paul, 21 Nov 2026: "You must remove that rule. You cannot reinstate every part on a sheet after one part comes"): NQ's whole layout is OFF for every metal. A Gold Filled or
   Sterling Silver sheet is nested as before NQ: the pieces already on it stay where they lie and the new ones are placed around them. (The list is kept, empty, so the code above reads as it did.) */
const WHOLE_METALS = [];
/** Whether the cloud lays this sheet out whole (see NQ above). */
function wholeOf(rec) {
  if (!rec || !WHOLE_METALS.includes(rec.metal)) return false;
  if (rec.roseCutAt || num(rec.laserDoneAt) > 0 || rec.releaseFull || num(rec.committedAt) > 0 || rec.approvedAt) return false;
  if (rec.setId && !rec.draft) return false;   // (a sheet of a set keeps its layout: only a draft of the run, or a sheet in no set, is formed again)
  if (rec.appendOnly === true) return false;   // (a Merge by hand or a piece made again in place promised the sheet's charms stay where they are: _charmNestCmdOps.js)
  return true;
}
/** NQ's pick among the slots' layouts: the one that keeps the most of the pieces already on the sheet (`seated`), then the solver's own judgement with the dead space weighed
    (tight, as 10K / 14K are picked). → { pick, k } (k: the index in `results`), or null. */
function wholePick(results, seated, sheet) {
  const ids = new Set((seated || []).map(String)), keptOf = r => r.placements.reduce((n, p) => n + (ids.has(String(p.id)) ? 1 : 0), 0);
  let best = null, k = -1;
  results.forEach((r, i) => { if (!r) return; if (!best) { best = r; k = i; return; } const a = keptOf(r), b = keptOf(best); if (a !== b ? a > b : Solver.betterLayout(r, best, sheet, { tight: true, reachFirst: false })) { best = r; k = i; } });
  return best ? { pick: best, k } : null;
}

/* ── prepare (slot 0): the job's input from the sheet's record ── */
async function prepare(ctx) {
  const { db, bucket, job } = ctx;
  const snap = await db.collection(Nest.SHEETS).doc(job.sheetId).get();
  if (!snap.exists) return { stale: "the sheet is no longer in the cloud" };
  const rec = Object.assign({}, snap.data() || {}, { id: job.sheetId });
  if (num(rec.layoutRev) !== num(job.layoutBase) || num(rec.contentRev) !== num(job.contentBase)) return { stale: "the sheet changed since its nest was asked" };
  const no = Nest.refusal(rec); if (no) return { error: no };
  const [S, policy, rows, roseStock] = await Promise.all([Nest.readSettings(db), Nest.readPolicy(db), Nest.poolRowsOf(db, rec), Nest.roseStockOf(db, rec)]);
  const plate = Nest.plateOf(rec, S, roseStock, policy);
  if (Nest.inputKeyOf(rec, rows, S, plate, job.mode) !== job.inputKey) return { stale: "the sheet's settings, plate or pieces changed since its nest was asked" };
  const name = rec.fileBase || rec.metalLabel || "This sheet";
  // every live piece, made again with its silhouette (in node: the page's own code and its canvas)
  const Pieces = require("./_charmNestNestPieces").create({ db, bucket, S, log: ctx.log, budgetMs: PREPARE_MS });
  const built = await Pieces.rebuild(rec, rows, () => ctx.tick());
  if (ctx.stop) return { stopped: true };
  const charms = built.charms, missing = built.missing, have = new Set(charms.map(c => c.id));
  const placedIds = new Set((rec.placements || []).map(p => p && String(p.id)).filter(Boolean)), lost = missing.filter(m => placedIds.has(m.id));
  // (a piece already seated that cannot be made again would lose its place: the sheet is not nested in the cloud, in words)
  if (lost.length) return { error: `${name}: ${lost.length} placed piece(s) could not be made again in the cloud (${lost[0].why}); nothing was nested` };
  if (!charms.length) return { error: `${name}: none of its pieces could be made in the cloud (${(missing[0] && missing[0].why) || "no piece"})` };
  // the saved layout: every placed piece stays where it is (the run's own pin; a person's Manual Control pin is held for good: handLocks)
  const fresh = job.mode === "fresh";
  const saved = (rec.placements || []).filter(p => p && have.has(String(p.id)) && [p.cxPt, p.cyPt].every(v => Number.isFinite(+v)))
    .map(p => ({ id: String(p.id), angle: +p.angle || 0, cxPt: +p.cxPt, cyPt: +p.cyPt, wPt: num(p.wPt), hPt: num(p.hPt), scale: num(p.scale) || NestPlan.CHARM_SCALE, layerName: p.layer || p.layerName || null }));
  const byId = new Map(charms.map(c => [c.id, c]));
  // (NQ: a Gold Filled or Sterling Silver draft is laid out whole, its person pins held: wholeOf; its saved layout is kept only by the contender that appends to it)
  const whole = wholeOf(rec);
  const pinSaved = keepAll => { for (const c of charms) { c.pinned = null; delete c.arrivalPin; }
    for (const p of saved) { const c = byId.get(p.id); if (!c) continue; if (!keepAll && !c.manual) continue; c.pinned = { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle }; if (c.manual && c.manual.size) c.pinned.size = c.manual.size; if (!c.manual) c.arrivalPin = true; } };
  pinSaved(!fresh && !whole);
  const word = CutLine.cutWord ? CutLine.cutWord(rec.metal) : "Rose Gold";
  let roseProtected = null; try { roseProtected = protectedOf(rec, saved, word); } catch (e) { return { error: `${name}: ${e.message}: reload it and nest again` }; }
  const view = Object.assign({ metal: rec.metal, runId: rec.runId || null, sheetId: rec.id, charms, placements: fresh || whole ? (roseProtected ? roseProtected.placements.map(p => Object.assign({}, p)) : []) : saved.slice(), rejects: [],
    topup: rec.topup || null, roseCutAt: null, roseProtected, nestFlow: "standard", runSeed: num(job.seed) || Nest.seedOf(job.id) }, plate);
  const env = Nest.planEnv(S, { agent: (scope, kind, text) => ctx.log(`[nest ${job.id}] ${kind}: ${text}`) });
  // the turn of orders this nest places (a big batch on a Gold or Silver sheet: three orders at a time, oldest first; the rest wait their turn on the sheet)
  // (NQ: a draft laid out whole takes its whole batch in one search, as 10K and 14K do: no turn of three orders)
  const all = NestPlan.activeCharms(view);
  /* OWNTOGETHER (Paul, 21 Nov 2026: Set-2's GF Sheet 1, put back with 56 of its 60 pieces): a sheet released full takes no new piece, so what waits on it is its OWN orders' pieces; they are
     nested together in one nest around the pieces already placed (which stay where they lie), never one at a time: a released sheet passes waiting pieces on (feedNext), which would
     split those orders off the sheet */
  const released = !!rec.releaseFull;
  let items = whole || released ? all : NestPlan.feedTurn(view, all, env);
  /* ONEPIECE (Paul, 21 Nov 2026: "they go on one piece at a time"): every nest places exactly ONE waiting piece next to the pieces already on the sheet (they stay where they lie); the others
     wait for the next nests, one at a time (feedWait). One piece is a search of seconds: no nest ever runs out of time or loses its work. A piece of an order already on the sheet goes first. */
  { const placedIds = new Set((view.placements || []).map(p => String(p.id))), key = c => String((c && c.order) || "").split("/")[0], onSheet = new Set(all.filter(c => placedIds.has(String(c.id))).map(key));
    const waiting = items.filter(c => !placedIds.has(String(c.id))).map((c, i) => [c, i]).sort((a, b) => (onSheet.has(key(b[0])) ? 1 : 0) - (onSheet.has(key(a[0])) ? 1 : 0) || a[1] - b[1]).map(x => x[0]);
    if (!whole && !released && waiting.length > 1) {
      /* PAIRTOGETHER (Paul, 22 Nov 2026: "why are earrings only nesting a single piece"; the Dachshund pair 4243337038 was split, its Right on GF Draft 3 and its Left alone on a new
         draft): the unit of a nest is ONE ORDER, never one piece: the first waiting piece and every other waiting piece of its order (a pair's two earrings, every piece of a
         multi-piece order) are nested together, so they are placed together or turned away together (keepOrdersWhole moves them on as one) */
      const one = waiting[0], unit = new Set(waiting.filter(c => key(c) === key(one)).map(c => String(c.id))), later = waiting.filter(c => !unit.has(String(c.id))).map(c => String(c.id));
      items = items.filter(c => placedIds.has(String(c.id)) || unit.has(String(c.id)));
      view.feedWait = [...new Set((view.feedWait || []).map(String).concat(later))];
    } }
  const onSheet = new Set(items.map(c => c.id)), kept = view.placements.filter(p => onSheet.has(p.id));
  view.nestInitial = kept.length ? kept.map(p => Object.assign({}, p)) : null;
  view.appendOnly = view.intakeAppend = !!view.nestInitial;   // (a saved layout that takes new pieces places them around itself, as the page's intake appends)
  const solverJob = NestPlan.buildJob(view, env);
  // NQ: the whole layout searches as 10K and 14K do (several careful searches side by side, each its own placing order, KEEPSEARCH, the least wasteful kept); a saved layout
  // that would lose none of its pieces by appending is searched too, in a slot of its own (altJob), so the whole layout never seats fewer of the pieces already on the sheet
  let alt = null, altSlot = -1, seated = [];
  if (whole) {
    solverJob.tight = true; solverJob.multiStart = NestPlan.multiStartFor(env.cores);
    seated = fresh ? [] : saved.map(p => p.id).filter(id => onSheet.has(id));
    if (seated.length) {
      pinSaved(true);
      const av = Object.assign({}, view, { placements: saved.filter(p => onSheet.has(p.id)).map(p => Object.assign({}, p)), feedWait: null });
      av.nestInitial = av.placements.map(p => Object.assign({}, p)); av.appendOnly = av.intakeAppend = true;
      alt = Object.assign(NestPlan.buildJob(av, env), { tight: true, altAppend: true });
      pinSaved(false);
    }
  }
  let n = Math.max(1, Math.min(Nest.MAX_SLOTS, NestPlan.searchCount(solverJob, items.length, () => 1)));
  if (alt) { n = Math.min(Nest.MAX_SLOTS, n + 1); altSlot = n - 1; }   // (a slot of its own beside the whole layout's searches; at six, five search the whole layout)
  const keep = NestPlan.keepPlan(solverJob, n, items.length, 0);
  const infoOf = new Map(charms.map(c => [c.id, c.orderInfo]));
  const meta = { charms: all.map(c => metaCharm(c, infoOf.get(c.id) ? flat(infoOf.get(c.id)) : null, c.pinned ? flat(c.pinned) : null)), feedWait: (view.feedWait || []).map(String), appendOnly: view.appendOnly,
    nestInitial: view.nestInitial ? view.nestInitial.map(flat) : null, protectedIds: roseProtected ? (roseProtected.placements || []).map(p => String(p.id)) : [], topup: view.topup || null,
    stock: flat(Nest.stockFor(S, rec.metal, view)), usablePt2: Nest.usableArea(S, view), clearancePt: +S.settings.clearancePt || 0, maxFill: +S.settings.maxFill || 0.8, nospread: S.settings.nospread !== "off",
    missing, verifyRes: [4, 6, 8].includes(+S.settings.verifyRes) ? +S.settings.verifyRes : 6 };
  if (whole) Object.assign(meta, { whole: true, seated, altSlot, altNestInitial: alt ? alt.lockedPlacements.filter(p => seated.includes(p.id)).map(flat) : null });
  return { input: Object.assign({ v: 1, jobId: job.id, sheetId: rec.id, metal: rec.metal, n, keepMax: keep ? keep.max : 0, job: packJob(solverJob), meta }, alt ? { alt: packJob(alt), altSlot } : {}), n, keep, items: items.length, missing, feedWait: meta.feedWait, solverJob, altJob: alt };
}

/* ── the slot's fenced writes ── */
function fencedTx(ctx, fn) {
  return ctx.db.runTransaction(async tx => {
    const s = await tx.get(ctx.sref), d = s.exists ? s.data() || {} : {};
    if (d.nonce !== ctx.nonce || d.state !== "running") throw new Fenced("another holder runs this slot now: this write was not made");
    if (ctx.cref) { const cs = await tx.get(ctx.cref), c = cs.exists ? (cs.data() || {}).carrier : null; if (!c || c.dev !== Nest.CLOUD_DEV) throw new Fenced(`${(c && c.by) || "a computer"} carries the run now: the cloud's write was not made`); }
    return fn(tx);
  });
}
/** The beat (every 20 s): the slot's lease, the job's beatAt and the run's carrier.at (C6), in one transaction that also reads the job (not ended), the drive (not off)
    and the carrier (the cloud's). A beat that is refused stops the search (a person took the run back, the drive was turned off, the job ended); one that finds
    another holder fences the slot. */
async function beat(ctx) {
  const { db, FV } = ctx;
  const r = await Lease.renew(db, ctx.sref, ctx.nonce, { check: async tx => {
    const js = await tx.get(ctx.jref), j = js.exists ? js.data() || {} : {}, dv = await tx.get(db.collection(Nest.OPEN).doc("drive")), d = dv.exists ? dv.data() || {} : null;
    let c = null; if (ctx.cref) { const cs = await tx.get(ctx.cref); c = cs.exists ? (cs.data() || {}).carrier || null : null; }
    if (ENDED.has(j.status)) return { ok: false, why: "the job has ended" };
    if (!d || (d.mode !== "standby" && d.mode !== "on")) return { ok: false, why: "the drive was turned off" };
    if (ctx.cref && (!c || c.dev !== Nest.CLOUD_DEV)) return { ok: false, why: `${(c && c.by) || "a computer"} carries the run now` };
    const now = Date.now(), writes = [[ctx.jref, { state: "running", beatAt: now, updatedAt: FV.serverTimestamp(), progress: { [String(ctx.i)]: flat(ctx.progress()) } }, { merge: true }]];
    if (c) writes.push([ctx.cref, { carrier: Object.assign({}, c, { at: now }) }, { merge: true }]);
    return { ok: true, writes };
  } }).catch(e => ({ ok: true, failed: String((e && e.message) || e) }));   // (a beat that could not be committed is tried again at the next step)
  if (r && r.lost) { ctx.lost = true; ctx.stop = true; } else if (r && r.ok === false) { ctx.handed = r.why; ctx.stop = true; }
}
/** Between two steps of the work: the stop asked for (every 4 s, one field), the beat (every 20 s). */
async function tick(ctx) {
  const now = Date.now();
  if (now - ctx.polledAt >= POLL_MS) {
    ctx.polledAt = now;
    try { const [s] = await ctx.db.getAll(ctx.jref, { fieldMask: ["stopRequested", "status"] }), d = s.exists ? s.data() || {} : {}; if (d.stopRequested || ENDED.has(d.status)) ctx.stop = true; } catch (_) { /* read at the next step */ }
  }
  if (now - ctx.beatAt >= BEAT_MS) { ctx.beatAt = now; await beat(ctx); }
  if (ctx.lost) throw new Fenced("another holder runs this slot now");
}

/** The job ends without a layout (stopped before it searched, stale, an error): the job and the sheet's nestJob say so, then the driver is told. */
async function endJob(ctx, status, why) {
  const { FV } = ctx, now = Date.now(), shref = ctx.db.collection(Nest.SHEETS).doc(ctx.job.sheetId);
  const wrote = await fencedTx(ctx, async tx => {
    const js = await tx.get(ctx.jref), j = js.exists ? js.data() || {} : null, ss = await tx.get(shref), sd = ss.exists ? ss.data() || {} : null;
    if (!j || ENDED.has(j.status)) return false;
    tx.set(ctx.jref, { status, state: status, endedBy: status === "stopped" ? "stopped" : status, error: status === "error" || status === "stale" ? str(why, 300) : null, note: str(why, 300), finishedMs: now, finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, { merge: true });
    if (sd && sd.nestJob && sd.nestJob.id === ctx.job.id) tx.set(shref, { nestJob: Object.assign({}, sd.nestJob, { status, at: now, why: str(why, 200) }) }, { merge: true });
    return true;
  });
  if (wrote) await Nest.kickDriver(ctx.db, "nest", { kick: ctx.kickDriver });
  return wrote;
}
/** This slot's search has finished: its best layout and how it ended (the pick reads every slot's). */
const writeFinal = (ctx, best, final) => fencedTx(ctx, async tx => { tx.set(ctx.sref, Object.assign({ final: flat(Object.assign({ at: Date.now() }, final)) }, best ? { bestJson: layoutText(best), placed: best.placements.length } : {}), { merge: true }); });

/* ── search ── */
async function search(ctx, input, sd) {
  // (NQ: the slot that appends to the saved layout runs its own job, once, judged as before; every other slot searches the whole layout)
  const altSlot = !!input.alt && ctx.i === num(input.altSlot);
  const base = altSlot ? (ctx.altJob || (ctx.altJob = unpackJob(input.alt))) : ctx.solverJob || (ctx.solverJob = unpackJob(input.job)), metal = input.metal, sheet = base.sheet, keep = !altSlot && num(input.keepMax) > 0, opts = keep ? NestPlan.keepOpts(metal) : {};
  const total = num(base.timeBudgetMs) || 180000;
  let startMs = num(sd.searchStartMs);
  if (!startMs) { startMs = Date.now(); await fencedTx(ctx, async tx => { tx.set(ctx.sref, { searchStartMs: startMs }, { merge: true }); }); }
  let best = parse(sd.bestJson), dirty = false, lastWrite = Date.now(), trials = num(sd.trials), last = null, v = sd.v != null ? num(sd.v) : ctx.i, runs = 0;
  if (best && !Array.isArray(best.placements)) best = null;
  const left = () => Math.min(total - (Date.now() - startMs), ctx.deadline - TAIL_MS - Date.now());
  ctx.progress = () => ({ placed: best ? best.placements.length : 0, total: base.pieces.length, trials, v, at: Date.now() });
  const checkpoint = async () => {
    if (!dirty || !best) return; dirty = false; lastWrite = Date.now();
    await fencedTx(ctx, async tx => { tx.set(ctx.sref, { bestJson: layoutText(best), placed: best.placements.length, trials, v, bestAt: Date.now() }, { merge: true }); });
  };
  const runOne = async job => {
    const budget = Math.max(5000, Math.min(num(job.timeBudgetMs) || total, left()));
    let seen = 0; runs++;
    const res = await Solver.solve(Object.assign({}, job, { timeBudgetMs: budget }), {
      shouldStop: () => ctx.stop || ctx.lost || Date.now() > ctx.deadline - TAIL_MS,
      yield: async () => { await tick(ctx); if (dirty && Date.now() - lastWrite >= CHECKPOINT_MS) await checkpoint(); },
      onBest: b => { if (b && Array.isArray(b.placements) && Solver.betterLayout(b, best, sheet, opts)) { best = Solver.publicLayout(b); dirty = true; } },
      onTrial: s => { const t = s && (s.completedTrials != null ? +s.completedTrials : +s.trial + 1); if (Number.isFinite(t)) seen = t; }
    });
    trials += num(res && res.trials) || seen;
    if (res && Array.isArray(res.placements)) { best = Solver.bestResult(res, best, sheet, opts); dirty = true; }
    last = res || last;
    await checkpoint();
    return res;
  };
  const finalOf = extra => Object.assign({ endedBy: ctx.stop ? "stopped" : (last && last.endedBy) || (best && best.endedBy) || "budget", trials, elapsedMs: Date.now() - startMs, placed: best ? best.placements.length : 0,
    timedOut: !!(last && last.timedOut), runs, v }, extra || {});
  // a slot taken up again: every piece its checkpoint seated stays where it is; the rest are placed around them, with the time this slot has left
  if (best && best.placements.length && num(sd.tries) > 1) {
    if (left() > 5000 && !ctx.stop) await runOne(NestPlan.resumeJob(NestPlan.slotJob(base, ctx.i, { keep, useGPU: false }), best));
    return { best, final: finalOf({ resumed: true }) };
  }
  let job = NestPlan.slotJob(base, ctx.i, { keep, useGPU: false });
  for (;;) {
    const t0 = Date.now();
    await runOne(job);
    if (!keep || ctx.stop || ctx.lost || (last && last.endedBy === "stopped")) break;
    // (KEEPSEARCH: the search starts again in the job's next placing order while the job's count and this slot's time last; NestPlan.keepAgain's rule, across invocations)
    const pass = Date.now() - t0; if (left() < pass + 5000) break;
    const next = await fencedTx(ctx, async tx => {
      const js = await tx.get(ctx.jref), j = js.exists ? js.data() || {} : {}, k = j.keep;
      if (j.stopRequested || ENDED.has(j.status) || !k || num(k.next) >= num(k.max)) return null;
      tx.set(ctx.jref, { keep: { next: num(k.next) + 1, max: num(k.max) } }, { merge: true });
      return num(k.next);
    });
    if (next == null) break;
    v = next; job = NestPlan.keepJob(base, v);
  }
  return { best, final: finalOf() };
}

/* ── the pick: once every slot has finished ── */
async function finishCheck(ctx) {
  const { db, FV } = ctx, n = Math.max(1, Math.min(Nest.MAX_SLOTS, num(ctx.job.slots) || 1)), refs = Array.from({ length: n }, (_, k) => Nest.slotRef(db, ctx.jobId, k));
  const [js, ...ss] = await db.getAll(ctx.jref, ...refs), j = js.exists ? js.data() || {} : {};
  if (ENDED.has(j.status)) return { ended: j.status };
  if (!Nest.slotsEnded(Object.assign({}, ctx.job, j), ss, Date.now(), ctx.i)) return { waiting: true };   // (every slot finished; or, long after the job was prepared, none but this one has a holder)
  const mine = await fencedTx(ctx, async tx => {
    const s = await tx.get(ctx.jref), d = s.exists ? s.data() || {} : {};
    if (ENDED.has(d.status)) return false;
    if (d.picking && d.picking.nonce !== ctx.nonce && Date.now() - num(d.picking.at) < PICK_STALE_MS) return false;
    tx.set(ctx.jref, { picking: { nonce: ctx.nonce, slot: ctx.i, at: Date.now() } }, { merge: true });
    return true;
  });
  if (!mine) return { picking: "another slot" };
  const input = ctx.input || (ctx.input = await loadInput(ctx.bucket, j.inputPath || ctx.job.inputPath));
  const base = ctx.solverJob || (ctx.solverJob = unpackJob(input.job)), meta = input.meta || {}, metal = input.metal, stopped = !!j.stopRequested;
  const finals = ss.map(s => (s.data() || {}).final || {}), bySlot = ss.map(s => { const r = parse((s.data() || {}).bestJson); return r && Array.isArray(r.placements) ? r : null; }), results = bySlot.filter(Boolean);
  const now = Date.now(), notes = [];
  let status = "done", error = null, result = null;
  if (!results.length) { status = stopped ? "stopped" : "error"; error = stopped ? null : `no search of ${input.sheetId} ended with a layout (${finals.map(f => f.endedBy || "?").join(", ")})`; }
  else {
    // (GOLDOPT, TIGHT, KEEPSEARCH: the least wasteful of the slots' layouts for 10K, 14K and Rose Gold; the page's betterLayout reduce for every metal)
    // (NQ: a Gold Filled / Sterling Silver sheet laid out whole: the layout that keeps every piece already on the sheet, then the least wasteful: wholePick; appended, the saved layout is held)
    const wp = meta.whole ? wholePick(bySlot, meta.seated, base.sheet) : null, appended = !!(wp && input.alt && wp.k === num(input.altSlot));
    const pick = wp ? Solver.bestResult(wp.pick, null, base.sheet) : NestPlan.finalPick(results, null, null, metal, () => base.sheet, Solver);
    const best = finals.reduce((a, f) => (a && num(a.placed) >= num(f.placed) ? a : f), null) || {};
    const endedBy = stopped ? "stopped" : pick.endedBy || best.endedBy || "budget";
    // the orders kept whole (charm-nest-nest-plan.js keepOrdersWhole, as finishNest runs it): an order a piece of which did not fit goes to the next sheet whole
    const byId = new Map(meta.charms.map(c => [c.id, c])), turn = new Map(base.pieces.filter(p => byId.has(p.id)).map(p => [p.id, byId.get(p.id)]));
    const sh = { metal, sheetId: input.sheetId, placements: pick.placements.map(p => Object.assign({}, p)), rejects: (pick.rejects || []).map(String), charms: meta.charms, feedWait: meta.feedWait || [],
      topup: meta.topup || null, roseProtected: (meta.protectedIds || []).length ? { placements: meta.protectedIds.map(id => ({ id })) } : null, appendOnly: appended || !!meta.appendOnly, nestInitial: (appended ? meta.altNestInitial : meta.nestInitial) || null };
    const S = { settings: { clearancePt: meta.clearancePt, maxFill: meta.maxFill } };
    const whole = NestPlan.keepOrdersWhole(sh, turn, new Set(pick.pocketFilled || []), { groupOf: Nest.groupOf, Orders: O, usableArea: () => num(meta.usablePt2), inflatedArea: c => Nest.inflatedArea(S, c),
      settings: S.settings, nospread: () => (meta.nospread ? NoSpread : null), agent: (scope, kind, text) => notes.push(String(text).slice(0, 400)) });
    // the check (the solver's verifier, as the page's geomCheck: the pieces held where they were must still be there; one the unjam moved, or a lifted one, is no longer held)
    const keptIds = new Set(sh.placements.map(p => p.id)), moved = new Set(((pick.unjam && pick.unjam.moved) || []).map(String));
    const vbase = appended ? unpackJob(input.alt) : base, vjob = Object.assign({}, vbase, { lockedPlacements: (vbase.lockedPlacements || []).filter(p => keptIds.has(p.id) && !moved.has(String(p.id))) });
    let verification; try { const v = Solver.verify(vjob, sh.placements, meta.verifyRes || 6); verification = Object.assign(flat(v), Array.isArray(v.cornerIds) ? { cornerIds: v.cornerIds.slice(0, 50).map(String) } : {}, Array.isArray(v.overlappingPairs) ? { overlapPairs: v.overlappingPairs.length } : {}); }
    catch (e) { verification = { ok: false, error: str(e && e.message, 200) }; }
    const pieces = base.pieces.length, last = finals.find(f => f.timedOut) || best;
    // (a search that ran out of time before it placed a single charm shows nothing about whether they fit: the nest is tried again, once: html finishNest's rule)
    if (!stopped && !sh.placements.length && pieces && endedBy === "budget" && (last.timedOut || !num(last.trials))) { status = "error"; error = "Ran out of time before placing a charm: the sheet is searched again"; }
    if (stopped) status = "stopped";
    let text = layoutText(pick); if (text.length > 400000) text = null;
    result = { placements: sh.placements.map(flat), rejects: sh.rejects.map(String), endedBy, trials: finals.reduce((n, f) => n + num(f.trials), 0), elapsedMs: Math.max(...finals.map(f => num(f.elapsedMs)), 0),
      density: num(pick.density), usablePt2: num(pick.usablePt2), freePt2: num(pick.freePt2), placedPt2: num(pick.placedPt2), pieces, slots: n,
      appendOnly: !!sh.appendOnly,   // (ONCEWHOLE: whether the nest held the sheet's saved layout where it lay, for the writer)
      ordersWhole: { done: true, lifted: whole.lifted.map(p => String(p.id)), mates: whole.mates.map(p => String(p.id)), wholeMates: whole.wholeMates.map(p => String(p.id)), behind: (whole.behind || []).map(String) },
      verification, feedWait: (meta.feedWait || []).map(String), missing: (meta.missing || []).map(flat), notes: notes.slice(0, 20), layoutJson: text, retainedBest: !!pick.retainedBest, ...(meta.whole ? { whole: true, appended } : {}) };
  }
  const shref = db.collection(Nest.SHEETS).doc(ctx.job.sheetId);
  await fencedTx(ctx, async tx => {
    const s = await tx.get(ctx.jref), d = s.exists ? s.data() || {} : {}, sv = await tx.get(shref), sd = sv.exists ? sv.data() || {} : null;
    if (ENDED.has(d.status) || !d.picking || d.picking.nonce !== ctx.nonce) return;
    tx.set(ctx.jref, Object.assign({ status, state: status, endedBy: result ? result.endedBy : status, error, finishedMs: now, finishedAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp(), beatAt: now }, result ? { result } : {}), { merge: true });
    if (sd && sd.nestJob && sd.nestJob.id === ctx.job.id) tx.set(shref, { nestJob: Object.assign({}, sd.nestJob, { status: status === "done" ? "nested" : status, at: now }) }, { merge: true });
  });
  ctx.log(`[nest] ${ctx.jobId}: ${status}${result ? ` · ${result.placements.length}/${result.pieces} placed · ${result.endedBy} · ${n} slot(s)` : ""}${error ? " · " + error : ""}`);
  await Nest.kickDriver(db, "nest", { kick: ctx.kickDriver });
  return { status, placed: result ? result.placements.length : 0 };
}

/* ── one invocation ── */
async function run(o) {
  const { db, FV, bucket, nonce, jobId } = o, i = Math.floor(+o.slot), log = o.log || (() => {}), t0 = Date.now();
  const jref = Nest.jobRef(db, jobId), sref = Nest.slotRef(db, jobId, i);
  const began = await Lease.begin(db, sref, nonce, { heartbeat: true, patch: { tries: FV.increment(1), slot: i, jobId }, check: async tx => {
    const js = await tx.get(jref), j = js.exists ? js.data() || {} : null, dv = await tx.get(db.collection(Nest.OPEN).doc("drive")), d = dv.exists ? dv.data() || {} : null;
    let c = null; if (j && j.runId) { const cs = await tx.get(db.collection(Nest.CARRIER).doc(j.runId)); c = cs.exists ? (cs.data() || {}).carrier || null : null; }
    if (!j || j.kind !== "nest") return { ok: false, why: "no such nest job" };
    if (ENDED.has(j.status)) return { ok: false, why: `the job has ended (${j.status})` };
    if (!d || (d.mode !== "standby" && d.mode !== "on")) return { ok: false, why: "the drive is off" };
    if (j.runId && (!c || c.dev !== Nest.CLOUD_DEV)) return { ok: false, why: `${(c && c.by) || "a computer"} carries the run` };
    return { ok: true, info: { job: j } };
  } });
  if (!began) return { skipped: "not this slot's turn" };
  if (began.refused) { await Lease.end(db, sref, nonce, { refused: began.why }).catch(() => {}); return { refused: began.why }; }
  const job = began.info.job;
  const ctx = { db, FV, bucket, nonce, jobId, i, jref, sref, job, log, deadline: t0 + (o.budgetMs || BUDGET_MS), cref: job.runId ? db.collection(Nest.CARRIER).doc(job.runId) : null,
    stop: false, lost: false, handed: null, polledAt: 0, beatAt: 0 /* (the first step beats at once: the job reads "running" while its pieces are made, so a hand-back waits for it) */, input: null, solverJob: null, kickDriver: o.kickDriver, kickSlot: o.kickSlot, progress: () => ({ phase: "begin" }) };
  ctx.tick = () => tick(ctx);
  let out = { slot: i };
  try {
    const sd = (await sref.get()).data() || {};
    if (job.stopRequested && !job.inputPath) { await endJob(ctx, "stopped", job.stopFor === "run" ? "the run was stopped before this nest began" : "stopped before it began"); out.ended = "stopped"; }
    else if (num(sd.tries) > Nest.MAX_SLOT_TRIES && !o.pick && !sd.final) {
      // (a slot started MAX_SLOT_TRIES times without finishing: it gives up in words; the job ends, or the pick goes on with the other slots)
      const why = `slot ${i} stopped answering ${Nest.MAX_SLOT_TRIES} times`;
      if (!job.inputPath) { await endJob(ctx, "error", `its input could not be prepared (${why})`); out.ended = "error"; }
      else { await writeFinal(ctx, null, { endedBy: "failed", why }); out.pick = await finishCheck(ctx); }
    } else {
      if (!job.inputPath) {
        if (i !== 0) { await Lease.end(db, sref, nonce, { waiting: "the input is prepared by slot 0" }); return { waiting: true }; }
        ctx.progress = () => ({ phase: "prepare" });
        const p = await prepare(ctx);
        if (p.stopped) { await endJob(ctx, "stopped", "stopped while its pieces were made"); out.ended = "stopped"; }
        else if (p.stale) { await endJob(ctx, "stale", p.stale); out.ended = "stale"; }
        else if (p.error) { await endJob(ctx, "error", p.error); out.ended = "error"; }
        else {
          const path = Nest.inputPathOf(jobId), gz = zlib.gzipSync(Buffer.from(JSON.stringify(p.input), "utf8"));
          await fencedTx(ctx, async () => null);   // (C7: the nonce read again right before the upload)
          await bucket.file(path).save(gz, { contentType: "application/gzip", resumable: false, metadata: { cacheControl: "no-store" } });
          const set = { inputPath: path, slots: p.n, keep: p.keep ? { next: p.n, max: p.keep.max } : null, timeBudgetMs: num(p.solverJob.timeBudgetMs), pieceCount: p.items, feedWait: p.feedWait, missing: p.missing.map(flat), preparedMs: Date.now(), beatAt: Date.now() };
          const go = await fencedTx(ctx, async tx => {
            const s = await tx.get(jref), d = s.exists ? s.data() || {} : {};
            if (ENDED.has(d.status)) return "ended";
            tx.set(jref, Object.assign({}, set, d.stopRequested ? {} : { status: "running", state: "running", startedMs: Date.now() }, { updatedAt: FV.serverTimestamp() }), { merge: true });
            return d.stopRequested ? "stop" : "go";
          });
          ctx.job = Object.assign({}, job, set, { status: "running" }); ctx.input = p.input; ctx.solverJob = p.solverJob;
          if (go === "ended") out.ended = "ended";
          else if (go === "stop") { await endJob(ctx, "stopped", "stopped while its pieces were made"); out.ended = "stopped"; }
          else if (go === "go") {
            log(`[nest] ${jobId}: input ready · ${p.items} piece(s) this turn · ${p.n} slot(s)${p.missing.length ? ` · ${p.missing.length} piece(s) could not be made` : ""}`);
            for (let k = 1; k < p.n; k++) await Nest.ensureSlot(db, ctx.job, k, { by: "slot0", kick: o.kickSlot }, { dispatched: [] });
            // (slot 0's own search starts here, or in a fresh invocation when the pieces took most of this one: the driver starts it again)
            if (ctx.deadline - Date.now() < TAIL_MS + MIN_SEARCH_MS) { await Lease.end(db, sref, nonce, { prepared: true }); return { prepared: true, searchLater: true }; }
          }
        }
      }
      if (!out.ended) {
        const sd2 = (await sref.get()).data() || {};
        if (!sd2.final && !o.pick) {
          const input = ctx.input || (ctx.input = await loadInput(bucket, ctx.job.inputPath));
          ctx.progress = () => ({ phase: "search" });
          const r = await search(ctx, input, sd2);
          if (ctx.handed && !ctx.lost) { await Lease.end(db, sref, nonce, { handed: ctx.handed }).catch(() => {}); return { handed: ctx.handed }; }
          await writeFinal(ctx, r.best, r.final);
          out.final = r.final;
        }
        out.pick = await finishCheck(ctx);
      }
    }
    await Lease.end(db, sref, nonce, flat({ ended: out.ended || null, picked: out.pick && out.pick.status ? out.pick.status : null }));
    return out;
  } catch (e) {
    if (e && e.fenced) { log(`[nest] ${jobId} slot ${i}: ${e.message}`); return { fenced: true }; }   // (another holder, or a computer, has it now: nothing more is written, not even the lease's end)
    const why = String((e && e.message) || e).slice(0, 300);
    log(`[nest] ${jobId} slot ${i} failed: ${why}`);
    try {
      if (!ctx.job.inputPath) await endJob(ctx, "error", why);
      else { await writeFinal(ctx, null, { endedBy: "error", why }); await finishCheck(ctx); }
    } catch (_) { /* the driver starts the slot again; its tries are counted */ }
    await Lease.end(db, sref, nonce, null, why).catch(() => {});
    return { error: why };
  }
}

/** charmNestSolve-background's slot mode: { nonce, jobId, slot, pick? }. */
async function handler(body, deps) {
  const nonce = String(body.nonce || ""), jobId = String(body.jobId || ""), slot = Math.floor(+body.slot);
  if (!nonce || !Nest.isId(jobId) || !(slot >= 0 && slot < Nest.MAX_SLOTS)) return { statusCode: 400, body: "bad payload" };
  const admin = deps.admin, db = deps.db || admin.firestore(), FV = admin.firestore.FieldValue, bucket = deps.bucket || admin.storage().bucket();
  let out;
  try { out = await run({ db, FV, bucket, nonce, jobId, slot, pick: body.pick === true, log: t => console.log(t) }); }
  catch (e) { console.error("[nest] slot", jobId, slot, e); out = { error: String((e && e.message) || e) }; }
  return { statusCode: 200, body: JSON.stringify(out).slice(0, 800) };
}

module.exports = { handler, run, prepare, search, finishCheck, protectedOf, wholeOf, wholePick, WHOLE_METALS, packBits, unpackBits, packJob, unpackJob, loadInput, BUDGET_MS };
