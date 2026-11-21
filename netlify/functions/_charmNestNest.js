/*  netlify/functions/_charmNestNest.js — THE CLOUD NEST LANE (cloud plan v2 §1.1 a, build step 6: nesting in the cloud). DORMANT: nothing here runs while
 *  Charm_Nest_Open/drive is absent or "off": a page's nestStart is refused in words and writes nothing, the driver never calls it (it does not pass), and a
 *  worker slot refuses at its lease.
 *
 *  What a nest in the cloud is (no computer runs a solver; a page only sends the Nest press and shows the job):
 *    nestStart { sheetId, fresh?, auto?, why }   one JOB per (sheet, layoutRev, contentRev, input): Charm_Nest_Jobs/<jobId>, jobId = "nest-" + sha24(sheetId|layoutRev|
 *                 contentRev|inputKey) (P2), made with create() in one transaction, so two presses (or a replayed unit) give ONE job. A job at that key that ended in
 *                 error or was stopped is tried again under the next attempt's id (<id>-a2, -a3 ... at most MAX_ATTEMPTS; m:P2): a person can retry without
 *                 changing the layout. The cloud's own ask (auto) retries an error AUTO_ERROR_ATTEMPTS times in all and never a job a PERSON stopped (that sheet
 *                 waits for its Nest press, as the page's holdSheet "This sheet was stopped; retry it when ready"). The sheet's record says which job is its
 *                 own (nestJob { id, status, at, attempt }): a sheet has one heavy task at a time.
 *    the lanes    one lane per run and metal (job.lane = runId~metal): the metals nest side by side, one metal's sheets one after another (the overflow and
 *                 feed-turn order). pumpLanes (the driver's nest step) starts the oldest queued job of a free lane and keeps a running job's slots alive.
 *    the slots    charmNestSolve-background { nonce, jobId, slot } (_charmNestNestSlot.js): each slot is an invocation of its own with its own lease
 *                 (Charm_Nest_Jobs/<jobId>/slots/<i>, _charmNestLease.js, heartbeat on), fenced on that nonce and on the run's carrier being the cloud.
 *                 Slot 0 prepares the job's input once (charmnest/jobs/<jobId>/input.json.gz: the solver job made by charm-nest-nest-plan.js buildJob from the
 *                 sheet's record, its pieces' silhouettes made in node), then the slots search (10K, 14K and Rose Gold: up to six, KEEPSEARCH restarts shared
 *                 through job.keep), each checkpointing its best layout; a slot cut off is taken up again from that checkpoint (NestPlan.resumeJob). The
 *                 last slot to finish picks the layout (NestPlan.finalPick), checks it (Solver.verify), keeps the orders whole (NestPlan.keepOrdersWhole)
 *                 and writes job.result. The sheet writer (build step 7a) applies it with layoutBase and contentBase.
 *    the switch   only while the drive is standby or on, and only for a run the cloud carries.
 *  It never calls Etsy and never builds the world (E1, E4): every read is of our own Firestore and Storage.                                                     */
"use strict";
const crypto = require("crypto");
const NestPlan = require("../../charm-nest-nest-plan.js");
const CutLine = require("../../charm-nest-rose.js");
const O = require("../../charm-nest-orders.js");
const Pair = require("../../charm-nest-pair.js");
const NoSpread = require("../../charm-nest-nospread.js");
const Solver = require("./_charmNestSolver");
const Lease = require("./_charmNestLease");

const JOBS = "Charm_Nest_Jobs", SHEETS = "Charm_Nest_Sheets", POOL = "Charm_Pool", SHARE = "Charm_Nest_Shared", STOCKS = "Charm_Nest_Rose_Stock", CARRIER = "Charm_Nest_Run_Carrier", OPEN = "Charm_Nest_Open";
const CLOUD_DEV = "cloud";
const MAX_ATTEMPTS = 5, AUTO_ERROR_ATTEMPTS = 3, MAX_SLOTS = 6, MAX_SLOT_TRIES = 3;
/** The processors the cloud's search is sized for (NestPlan.multiStartFor): six slots for a fresh 10K, 14K or Rose Gold nest, as the strongest computer ran. */
const CLOUD_CORES = 7;
const MANUAL_STALE_MS = 90000;   // (charmNestLibrary.js MANUAL_STALE_MS: a person's Manual Control claim on a sheet counts for 90 s after its last beat)
const ENDED = new Set(["done", "error", "stopped", "stale"]);
const sha = (s, n) => crypto.createHash("sha256").update(String(s)).digest("hex").slice(0, n || 24);
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const isId = s => /^[\w\-]{4,80}$/.test(String(s || ""));

/* ── ids and paths ── */
/** The job's key (P2): the sheet, the revision of its layout and of its content, and what the nest is made of (inputKeyOf). */
const jobBase = (sheetId, layoutRev, contentRev, inputKey) => "nest-" + sha([sheetId, layoutRev, contentRev, inputKey].join("|"), 24);
/** The id of the a-th attempt at a key (m:P2): the first is the key itself. */
const attemptId = (base, a) => (a <= 1 ? base : base + "-a" + a);
/** The seed of a job's search: a hash of its id (never a random number), so a job run again searches the same way and a new attempt searches anew. */
const seedOf = jobId => (parseInt(sha("seed|" + jobId, 8), 16) & 0x7fffffff) || 1;
const inputPathOf = jobId => "charmnest/jobs/" + jobId + "/input.json.gz";
const jobRef = (db, jobId) => db.collection(JOBS).doc(jobId);
const slotRef = (db, jobId, i) => db.collection(JOBS).doc(jobId).collection("slots").doc(String(i));

/* ── the shared Settings (Charm_Nest_Shared/sorter-settings and sorter-stock, charm-nest-cloud-share.js) over the page's defaults (charm-nest-1.html DEFAULTS,
   STOCK_PRESETS, stockFor, cornerPtSetting, angleSet, usableArea, inflatedArea, topupRoom, the NOSPREAD helpers): the values a nest reads, read the way the page
   reads them. The settings that stay on each computer (SETTINGS_HERE) are not shared: the cloud is a computer of its own, with the standard flow and the solver. ── */
const IN = 25.4;
const DEFAULTS = { stock: { gold: [100 / IN, 50 / IN], silver: [100 / IN, 50 / IN], rose: [100 / IN, 50 / IN], gold10k: [100 / IN, 50 / IN], gold14k: [100 / IN, 50 / IN] },
  clearancePt: -0.5, insetPt: 1.5, cornerPt: 1.5 * 72 / 25.4, angleStep: 10, budgetS: 180, seed: 1, maxFill: 0.80, silhouetteRes: 6, minPt: 6, verifyRes: 6,
  engine: "solver", nestFlow: "standard", skuPattern: "^[A-Z0-9][A-Z0-9 _.,'&()+\\-]{1,60}$", labelGapMm: 6.4 };
const STOCK_PRESETS = [{ id: "A", w: 100 / IN, h: 50 / IN }, { id: "B", w: 100 / IN, h: 100 / IN }, { id: "custom", w: 0, h: 0 }];
const SETTINGS_HERE = new Set(["v", "sandbox", "sandboxSeed", "sandboxStream", "sandboxSpeed", "computePipeline", "parallel", "serverAbove", "masterServerAbove", "rail", "orderView", "notify", "sound", "dsOrigin",
  "heartbeatS", "heartbeatMiss", "runMode", "pollOrders", "pollMinutes", "autoEvery", "etsyHourlyCap", "nestFlow", "learnedBudgetS", "learnedStallS"]);
const SOLID = ["gold10k", "gold14k", "rose"], NO_CORNER = ["gold10k", "gold14k"];
const TOPUP = { orders: 35, target: .75 }, GAP_PACK = .5;
/** The Settings as every computer shares them, over the page's defaults. A read that fails throws (a nest is never made with settings it did not read). → S { settings, stockPreset, sig } */
async function readSettings(db) {
  const [s, k] = await db.getAll(db.collection(SHARE).doc("sorter-settings"), db.collection(SHARE).doc("sorter-stock"));
  const f = s.exists ? (s.data() || {}).f || {} : {}, settings = Object.assign({}, DEFAULTS, { stock: Object.assign({}, DEFAULTS.stock) });
  for (const [key, v] of Object.entries(f)) { if (!v || v.x || SETTINGS_HERE.has(key)) continue; try { settings[key] = JSON.parse(v.j); } catch (_) { /* a value that is not JSON is left at its default */ } }
  settings.stock = Object.assign({}, DEFAULTS.stock, settings.stock && typeof settings.stock === "object" && !Array.isArray(settings.stock) ? settings.stock : {});
  settings.engine = "solver"; settings.nestFlow = "standard";   // (the learned flow needs a laptop runner and the AI engine a person's page: the cloud nests with the solver, the standard way)
  let stockPreset = "A"; const kp = k.exists ? ((k.data() || {}).f || {}).preset : null;
  if (kp && !kp.x) { try { const v = JSON.parse(kp.j); if (typeof v === "string" && v) stockPreset = v; } catch (_) { /* default */ } }
  return { settings, stockPreset };
}
/** The Partial Sheet policy per cut-line metal (config/charmNestPartials, _charmNestRemnants.js): { rose|gold10k|gold14k: { mode: auto|new, wMm, hMm } }. */
async function readPolicy(db) {
  const s = await db.collection("config").doc("charmNestPartials").get(), d = s.exists ? s.data() || {} : {}, out = {};
  for (const m of ["rose", "gold10k", "gold14k"]) { const p = (d.policies && d.policies[m]) || {}, ok = v => Number.isFinite(+v) && +v >= 5 && +v <= 500; out[m] = { mode: p.mode === "new" ? "new" : "auto", wMm: ok(p.wMm) ? +p.wMm : 100, hMm: ok(p.hMm) ? +p.hMm : 50 }; }
  return out;
}
/** The plate of a metal for a sheet (the page's stockFor): the physical sheet it holds, the size it was cut at or the size its policy gave it, else the metal's. */
function stockFor(S, metal, sh) {
  const saved = sh && (sh.roseStock || sh.keptStock || sh.newStock);
  if (saved && +saved.wPt > 0 && +saved.hPt > 0) return { wPt: +saved.wPt, hPt: +saved.hPt, wIn: saved.wPt / 72, hIn: saved.hPt / 72 };
  const p = STOCK_PRESETS.find(x => x.id === S.stockPreset), inches = p && p.id !== "custom" && !SOLID.includes(metal) ? [p.w, p.h] : (S.settings.stock[metal] || DEFAULTS.stock[metal] || DEFAULTS.stock.gold);
  return { wIn: +inches[0], hIn: +inches[1], wPt: inches[0] * 72, hPt: inches[1] * 72 };
}
function cornerPt(S, sh) { const metal = typeof sh === "string" ? sh : sh && sh.metal; if (metal && NO_CORNER.includes(metal)) return 0; const v = S.settings.cornerPt; return v == null || v === "" || !Number.isFinite(+v) ? DEFAULTS.cornerPt : Math.max(0, +v); }
function angleSet(S) { const st = [2, 5, 10].includes(+S.settings.angleStep) ? +S.settings.angleStep : 10, a = []; for (let d = 0; d < 360; d += st) a.push(d); return a; }
function usableArea(S, sh) {
  const st = stockFor(S, sh.metal, sh), inset = +S.settings.insetPt || 0, clearance = +S.settings.clearancePt || 0, ins = inset + Math.max(0, clearance) / 2;
  if (sh.roseStock && sh.roseStock.profileJson) return Solver.makeSheetGrid({ ...st, insetPt: inset, cornerPt: cornerPt(S, sh), remnant: JSON.parse(sh.roseStock.profileJson) }, clearance, 2).usableCells / 4;
  return Math.max(0, st.wPt - 2 * ins) * Math.max(0, st.hPt - 2 * ins);
}
function inflatedArea(S, c) { const g = (+S.settings.clearancePt || 0) / 2; return (+c.areaPt2 || 0) + g * 2 * ((+c.widthPt || 0) + (+c.heightPt || 0)) + Math.PI * g * g; }
const activeCharms = sh => (sh.charms || []).filter(c => c && !c.excluded);
/** The tries a sheet filling its gaps has left (the page's topupRoom). */
function topupRoom(sh, waiting = true) {
  const t = sh.topup; if (!t || t.closedAt || !["gold", "silver"].includes(sh.metal)) return Infinity;
  const tried = new Set((t.tried || []).map(String)); let room = TOPUP.orders - tried.size;
  if (waiting) { const placed = new Set((sh.placements || []).map(p => p.id)); room -= new Set(activeCharms(sh).filter(c => !placed.has(c.id)).map(NestPlan.orderKey).filter(o => !tried.has(o))).size; }
  return room;
}
const nospreadOf = S => (S.settings.nospread !== "off" ? NoSpread : null);
const nospreadCfg = S => ({ target: TOPUP.target, fitAtTarget: GAP_PACK, ...(S.settings.nospreadMargin != null && S.settings.nospreadMargin !== "" && Number.isFinite(+S.settings.nospreadMargin) ? { nearMargin: +S.settings.nospreadMargin } : {}) });
function nospreadSheet(S, sh, skip, o = {}) {
  const placed = new Set((sh.placements || []).map(p => p.id)); let placedPt2 = 0, pendingPt2 = 0;
  for (const c of activeCharms(sh)) { if (skip && skip.has(c.id)) continue; const a = inflatedArea(S, c); if (placed.has(c.id)) placedPt2 += a; else if (!o.nestedOnly) pendingPt2 += a; }
  return { usablePt2: usableArea(S, sh), placedPt2, pendingPt2, maxFill: +S.settings.maxFill || .8, gapFilling: NestPlan.gapFilling(sh) };
}
/** The page's nospreadAdmitted: the orders a sheet filling its gaps may still try (a multi-piece order that does not fit whole goes on, whole, to the next sheet). */
function nospreadAdmitted(S, sh, byOrder) {
  const NS = nospreadOf(S), keys = [...byOrder.keys()]; if (!NS || !["gold", "silver"].includes(sh.metal)) return new Set(keys);
  const all = new Set(); for (const ps of byOrder.values()) for (const c of ps) all.add(c.id);
  const base = nospreadSheet(S, sh, all), cfg = nospreadCfg(S), out = new Set();
  for (const k of keys) { const o = NS.orderOf({ pieces: byOrder.get(k), placed: 0, more: 0 }, c => inflatedArea(S, c)); if (!o.multi || NS.admits(base, o, cfg).take) { out.add(k); base.pendingPt2 += o.footprint; } }
  return out;
}
/** The line a piece belongs to when it is in a group (an earring pair, a necklace of counted discs: the page's groupOf / lineKeyOf); null for a plain piece. */
function groupOf(c) {
  if (!c || !Pair.inGroup(c)) return null;
  let k = Pair.groupKey ? Pair.groupKey(c) : "";
  if (!k || /^:/.test(k)) { const oi = c.orderInfo, m = /^(\d{4,20})_([^_]*)_\d{1,3}$/.exec(String(c.poolId || "")); k = oi && oi.receiptId != null && oi.transactionId != null ? `${oi.receiptId}:${oi.transactionId}` : m ? `${m[1]}:${m[2]}` : ""; }
  return k && k.length > 1 && !/^:/.test(k) ? k : null;
}
/** What charm-nest-nest-plan.js reads of its host, for the cloud. */
function planEnv(S, o = {}) {
  return { settings: S.settings, stockFor: (m, sh) => stockFor(S, m, sh), cornerPt: sh => cornerPt(S, sh), angleSet: () => angleSet(S), cores: CLOUD_CORES,
    Solver, Rose: CutLine, Orders: O, groupOf, nospread: () => nospreadOf(S), nospreadAdmitted: (sh, by) => nospreadAdmitted(S, sh, by), topupRoom: (sh, w) => topupRoom(sh, w),
    usableArea: sh => usableArea(S, sh), inflatedArea: c => inflatedArea(S, c), agent: o.agent || (() => null) };
}
/** The sheet's own plate, as the page reads it after a reload (restoreRunSheets, keptStock), with its physical sheet (Rose Gold, 10K, 14K) and its Partial Sheet policy. */
function plateOf(rec, S, roseStock, policy) {
  const v = { metal: rec.metal };
  if (roseStock && +roseStock.wPt > 0 && +roseStock.hPt > 0) v.roseStock = { id: roseStock.id, revision: num(roseStock.revision), wPt: +roseStock.wPt, hPt: +roseStock.hPt, profileJson: roseStock.profileJson || null };
  if (["gold10k", "gold14k"].includes(rec.metal)) {
    const was = rec.stock || {}, now = stockFor(S, rec.metal, null), wPt = +was.wPt || +was.wIn * 72, hPt = +was.hPt || +was.hIn * 72;
    if (wPt > 0 && hPt > 0 && (Math.abs(wPt - now.wPt) > 0.01 || Math.abs(hPt - now.hPt) > 0.01)) v.keptStock = { wPt, hPt };
  }
  // (a cut-line sheet never nested, under its metal's Partial Sheet policy 'new': the size the owner set: charm-nest-partial-nest.js newSheet)
  const p = policy && policy[rec.metal];
  if (p && p.mode === "new" && CutLine.cuts(rec.metal) && !v.roseStock && !v.keptStock && !(rec.placements || []).length && !rec.roseStockId) v.newStock = { wPt: p.wMm * 72 / 25.4, hPt: p.hMm * 72 / 25.4 };
  return v;
}

/* ── reads ── */
async function getMany(db, refs, mask) { const out = []; for (let i = 0; i < refs.length; i += 100) out.push(...await db.getAll(...refs.slice(i, i + 100), ...(mask ? [{ fieldMask: mask }] : []))); return out; }
/** The pool rows of a sheet's pieces: Map poolId → row. */
async function poolRowsOf(db, rec, mask) {
  const ids = [...new Set([...(rec.poolIds || []), ...(rec.charms || []).map(c => c && c.poolId)].filter(Boolean).map(String))].slice(0, 600);
  const snaps = ids.length ? await getMany(db, ids.map(id => db.collection(POOL).doc(id)), mask) : [], out = new Map();
  snaps.forEach(s => { if (s.exists) out.set(s.id, s.data() || {}); });
  return out;
}
/** The physical sheet a sheet holds (Rose Gold, 10K, 14K), or null. */
async function roseStockOf(db, rec) {
  if (!rec || !CutLine.cuts(rec.metal) || !isId(rec.roseStockId)) return null;
  const s = await db.collection(STOCKS).doc(rec.roseStockId).get(); return s.exists ? Object.assign({ id: s.id }, s.data() || {}) : null;
}
async function sheetsOfRun(db, runId) { const q = await db.collection(SHEETS).where("runId", "==", runId).get(); return q.docs.map(d => Object.assign({ id: d.id }, d.data() || {})).filter(s => !s.archived); }
/** The run's nest jobs, their bookkeeping only (a job's result is read by the writer, never by every pass of the driver). */
const JOB_FIELDS = ["kind", "status", "sheetId", "sheetName", "layoutBase", "contentBase", "lane", "createdMs", "stopRequested", "stopFor", "auto", "slots", "attempt", "inputPath", "error", "preparedMs"];
async function jobsOfRun(db, runId) {
  const q = db.collection(JOBS).where("runId", "==", runId), s = await (typeof q.select === "function" ? q.select(...JOB_FIELDS) : q).get();
  return s.docs.map(d => Object.assign({ id: d.id }, d.data() || {})).filter(j => j.kind === "nest");
}

/** What the nest of a sheet is made of, beyond its two revisions (P2): the plate, the physical sheet, the Settings it reads, the protected green-line layout, the gap fill,
    and each piece's rank and artwork (a pool row's release from hold moves its rank, an artwork edit its outline, and neither moves the sheet's revisions). */
function inputKeyOf(rec, rows, S, plate, mode) {
  const st = stockFor(S, rec.metal, plate), set = S.settings;
  const charms = (rec.charms || []).filter(c => c && !c.excluded).map(c => [String(c.id), String(c.hash || ""), String(c.poolId || ""), num(c.areaPt2), c.side || "", !!c.mirror, c.bodyIndex == null ? null : num(c.bodyIndex), c.manual ? [num(c.manual.size), c.manual.fit || ""] : null, !!c.frozen, c.chunk || null]).sort((a, b) => a[0].localeCompare(b[0]));
  const pieces = charms.map(c => { const r = rows.get(c[2]); return r ? [c[2], num(r.frontAt), num(r.orderDate), r.artEdit ? [num(r.artEdit.front), num(r.artEdit.back)] : null, String(r.charmHash || ""), String(r.sku || ""), String(r.size || "")] : [c[2]]; });
  return sha(JSON.stringify([1, mode || "auto", rec.metal, [+st.wPt.toFixed(3), +st.hPt.toFixed(3)], plate.roseStock ? [plate.roseStock.id, plate.roseStock.revision, sha(plate.roseStock.profileJson || "", 12)] : null,
    [set.clearancePt, set.insetPt, set.maxFill, cornerPt(S, rec), set.angleStep, set.budgetS, set.silhouetteRes, set.minPt, set.nospread || "on", set.nospreadMargin == null ? null : set.nospreadMargin],
    sha(String(rec.roseProtectedJson || "") + "|" + String(rec.rosePlanJson || ""), 12), rec.topup || null, charms, pieces]), 32);
}

/** Why a sheet cannot be nested at all, in words, or "". */
function refusal(rec) {
  if (!rec) return "That sheet is not in the cloud";
  const name = rec.fileBase || rec.metalLabel || "This sheet";
  if (rec.archived) return `${name} is archived: it is not nested`;
  if (rec.roseCutAt) return `${name} has been cut: it is not nested again (start a new sheet to use its remainder)`;
  if (num(rec.laserDoneAt) > 0) return `${name} is laser cut: it is not nested again`;
  if (!activeCharms(rec).length) return `${name} has nothing to nest`;
  return "";
}
/** Whether the run's nest step owes a sheet a nest, from its record (the page's nestAll and onSheetDone: a sheet with pieces still to place, one placeOnSheet marked
    (nestOwed), or one that changed since it was nested and owes no files written as it stands). → { owed, why } | { owed: false, wait?, why? } */
function owedNest(rec, run, now) {
  if (refusal(rec)) return { owed: false };
  if (rec.releaseFull) return { owed: false };   // (a sheet released full takes no new piece: one taken off it is written as it stands, the files lane)
  const holds = (run && run.sheetHolds) || {}; if (holds[rec.id]) return { owed: false, held: String(holds[rec.id]) };
  // (Gate.nestable: a 10K or 14K sheet in a set nests only when that solid metal is picked for the run, or the sheet's own Include tick; a draft of the run always nests)
  if (["gold10k", "gold14k"].includes(rec.metal) && rec.setId && !rec.draft && rec.solidIncluded !== true && !(run && run.solidIncluded && run.solidIncluded[rec.metal] === true)) return { owed: false };
  const name = rec.fileBase || rec.metalLabel || rec.id;
  if (rec.manualEdit && now - num(rec.manualEdit.at) < MANUAL_STALE_MS) return { owed: false, wait: true, why: `${name}: a person is moving its pieces by hand` };
  const placed = new Set((rec.placements || []).map(p => p && String(p.id))), rejected = new Set((rec.rejects || []).map(r => String(r && typeof r === "object" ? r.id : r)));
  const fresh = activeCharms(rec).filter(c => !placed.has(String(c.id)) && !rejected.has(String(c.id)));
  // (placeOnSheet's nestOwed mark is read as the reason only: what owes the nest is a piece not placed and not turned away, which no write of a layout leaves behind,
  //  and a change since the last nest (dirty), which nestStart clears as the page's startNestReady does: a mark nobody clears never nests a sheet over and over)
  if (fresh.length) return { owed: true, why: rec.nestOwed ? `${name}: pieces were placed on it` : `${name}: ${fresh.length} piece(s) wait to be nested` };
  if (rec.dirty === true && !rec.filesOwed) return { owed: true, why: `${name}: it changed since it was nested` };
  return { owed: false };
}

/* ── nestStart (an op of charmNestLibrary: OPS.nestStart; class PRESS, and one of the cloud's own writes). L: { db, FV, PREFIX, MARKED, fenceTx, driveMode, revTouch } ── */
async function start(L, b) {
  const db = L.db, FV = L.FV;
  if (L.PREFIX) return { error: "The cloud nests production runs only: a sandbox sheet nests on its page", status: 400, sandbox: true };
  const sheetId = str(b.sheetId, 80); if (!isId(sheetId)) return { error: "bad sheet id", status: 400 };
  // (DORMANT: a page's press is taken only while the drive is standby or on, and only for a run the cloud carries; the cloud's own call is fenced on both already)
  if (!L.MARKED && (await L.driveMode()) === "off") return { error: "The cloud does not nest sheets yet (the drive is off): this computer nests it. Nothing was changed", status: 409, driveOff: true };
  const fresh = b.fresh === true, auto = !!L.MARKED && b.auto === true, mode = fresh ? "fresh" : "auto";
  const sref = db.collection(SHEETS).doc(sheetId), snap = await sref.get();
  if (!snap.exists) return { error: "That sheet is not in the cloud", status: 404 };
  const rec = Object.assign({}, snap.data() || {}, { id: sheetId }), no = refusal(rec);
  if (no) return { error: no, status: 409, refused: true };
  if (!L.MARKED) {
    if (!isId(rec.runId)) return { error: `${rec.fileBase || "This sheet"} belongs to no run: it nests on a computer`, status: 409, notCarrier: true };
    const cs = await db.collection(CARRIER).doc(rec.runId).get(), c = cs.exists ? (cs.data() || {}).carrier : null;
    if (!c || c.dev !== CLOUD_DEV) return { error: `${(c && c.by) || "A computer"} carries this sheet's run: it nests there. Nothing was changed`, status: 409, notCarrier: true };
  }
  const [S, policy, rows, roseStock] = await Promise.all([readSettings(db), readPolicy(db), poolRowsOf(db, rec, ["frontAt", "orderDate", "artEdit", "charmHash", "sku", "size"]), roseStockOf(db, rec)]);
  const plate = plateOf(rec, S, roseStock, policy), layoutRev = num(rec.layoutRev), contentRev = num(rec.contentRev), inputKey = inputKeyOf(rec, rows, S, plate, mode);
  const base = jobBase(sheetId, layoutRev, contentRev, inputKey), now = Date.now();
  const out = await db.runTransaction(async tx => {
    if (L.fenceTx) await L.fenceTx(tx);
    const cur = await tx.get(sref), d = cur.exists ? cur.data() || {} : null;
    if (!d || num(d.layoutRev) !== layoutRev || num(d.contentRev) !== contentRev || String(d.roseStockId || "") !== String(rec.roseStockId || "")) return { error: "The sheet changed while its nest was asked: it is asked again", status: 409, conflict: true, retry: true };
    let pick = null, last = null, errors = 0;
    for (let a = 1; a <= MAX_ATTEMPTS && !pick; a++) {
      const id = attemptId(base, a), s = await tx.get(jobRef(db, id));
      if (!s.exists) { pick = { id, attempt: a }; break; }
      const j = s.data() || {}; last = { id, j, a };
      if (j.status === "error") errors++;
      if (!["error", "stopped", "stale"].includes(j.status)) return { ok: true, jobId: id, created: false, status: j.status || "queued", attempt: a };
      if (auto && j.status === "stopped" && j.stopFor !== "run") return { ok: true, jobId: id, created: false, status: "stopped", attempt: a, held: true, why: `${rec.fileBase || "This sheet"}: its nest was stopped by ${j.stopBy || "a person"}; it waits for its Nest press` };
      if (auto && j.status === "error" && errors >= AUTO_ERROR_ATTEMPTS) return { ok: true, jobId: id, created: false, status: "error", attempt: a, held: true, why: `${rec.fileBase || "This sheet"}: its nest failed ${errors} times (${str(j.error, 160) || "no reason given"}); press Nest when the cause is fixed` };
    }
    if (!pick) return { error: `${rec.fileBase || "This sheet"} was nested ${MAX_ATTEMPTS} times at this layout without a layout to keep (${str(last && last.j && last.j.error, 160) || "stopped"}): change the sheet, or ask for help`, status: 409, gaveUp: true, jobId: last && last.id };
    const metal = d.metal || rec.metal, runId = d.runId || null;
    const job = { id: pick.id, kind: "nest", cloud: true, status: "queued", state: "queued", sheetId, runId, metal, lane: (runId || "none") + "~" + metal, baseId: base, attempt: pick.attempt,
      layoutBase: layoutRev, contentBase: contentRev, roseStockId: d.roseStockId || null, inputKey, mode, why: str(b.why, 160), by: str(b.by, 80) || null, auto, seed: seedOf(pick.id),
      sheetName: d.fileBase || d.metalLabel || null, pieceCount: activeCharms(d).length, createdMs: now, createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() };
    tx.create(jobRef(db, pick.id), job);
    // (the nest takes up whatever changed the sheet: its dirty mark is cleared here, as the page's startNestReady clears it; a change made during the nest marks it again)
    tx.set(sref, { nestJob: { id: pick.id, status: "queued", at: now, attempt: pick.attempt }, dirty: false }, { merge: true });
    return { ok: true, jobId: pick.id, created: true, status: "queued", attempt: pick.attempt };
  });
  if (out && out.ok && L.revTouch) L.revTouch(["s:" + sheetId]);
  return out;
}

/* ── the lanes and the slots: started by the driver (its nest step) and by a slot that fans the search out; each slot through its own lease ── */
/** POST charmNestSolve-background { nonce, jobId, slot } (best-effort: a slot not started is dispatched again by the driver once its lease goes stale). */
async function kickSlot(nonce, jobId, slot, o = {}) {
  if (typeof o.kick === "function") return o.kick({ nonce, jobId, slot, pick: !!o.pick });
  const fetch = require("node-fetch"), origin = String(process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app").replace(/\/+$/, "");
  try { await Promise.race([fetch(`${origin}/.netlify/functions/charmNestSolve-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ nonce, jobId, slot, ...(o.pick ? { pick: true } : {}) }) }), new Promise(r => setTimeout(r, 4000))]); }
  catch (e) { console.warn("[nest] slot not started:", jobId, slot, e && e.message); }
}
/** Slot i of a job gets a holder unless it has a live one: a single-use nonce (_charmNestLease dispatch), then the kick. */
const SLOT_LEASE_FIELDS = ["nonce", "state", "at", "startedAt", "beatAt", "heartbeat", "final", "tries"];   // (what a slot's holder is judged by: never its checkpoint, which can be large)
async function ensureSlot(db, job, i, o, out, snap) {
  const s = snap || (await db.getAll(slotRef(db, job.id, i), { fieldMask: SLOT_LEASE_FIELDS }))[0], d = s.exists ? s.data() || {} : null;
  if (d && Lease.held(d, Date.now())) return false;
  const got = await Lease.dispatch(db, slotRef(db, job.id, i), (o.by || "drive") + (o.pick ? ":pick" : ""), { heartbeat: true });
  if (!got || !got.nonce) return false;
  out.dispatched.push(job.id + "/" + i);
  await kickSlot(got.nonce, job.id, i, o);
  return true;
}
/** A slot with no holder, of a job prepared longer ago than this, is taken as finished with its checkpoint (a slot whose invocation never started again): the pick goes on. */
const DEAD_MS = 20 * 60000;
/** Whether every slot of a job has finished (`final`), or, the job prepared more than DEAD_MS ago, has no live holder (`self`: the slot that asks, which holds its own lease). */
function slotsEnded(job, snaps, now, self) {
  const old = num(job.preparedMs) > 0 && now - num(job.preparedMs) > DEAD_MS;
  return snaps.every((s, k) => { const d = s.exists ? s.data() || {} : null; return !!(d && d.final) || (old && (k === self || !(d && Lease.held(d, now)))); });
}
/** A running job: each slot that has not finished keeps a live holder; once every slot has finished, slot 0 is asked to pick the layout. */
async function keepSlots(db, job, o, out) {
  const n = Math.max(1, Math.min(MAX_SLOTS, num(job.slots) || 1)), snaps = await db.getAll(...Array.from({ length: n }, (_, i) => slotRef(db, job.id, i)), { fieldMask: SLOT_LEASE_FIELDS });
  if (slotsEnded(job, snaps, Date.now(), -1)) return ensureSlot(db, job, 0, Object.assign({}, o, { pick: true }), out, snaps[0]);
  for (let i = 0; i < n; i++) { const d = snaps[i].exists ? snaps[i].data() || {} : null; if (!(d && d.final)) await ensureSlot(db, job, i, o, out, snaps[i]); }
  return true;
}
/** One lane per run and metal: the oldest queued job of a free lane starts; a running job's slots are kept alive. A queued job asked to stop is let start, so it closes itself. */
async function pumpLanes(db, jobs, o = {}) {
  const out = { dispatched: [] }, lanes = new Map();
  for (const j of jobs || []) if (j && (j.status === "queued" || j.status === "running")) { const k = j.lane || j.id; if (!lanes.has(k)) lanes.set(k, []); lanes.get(k).push(j); }
  for (const list of lanes.values()) {
    list.sort((a, b) => num(a.createdMs) - num(b.createdMs) || String(a.id).localeCompare(String(b.id)));
    for (const j of list) if (j.status === "queued" && j.stopRequested) await ensureSlot(db, j, 0, o, out);
    const running = list.filter(j => j.status === "running");
    if (running.length) { for (const j of running) await keepSlots(db, j, o, out); continue; }
    const next = list.find(j => j.status === "queued" && !j.stopRequested);
    if (next) await ensureSlot(db, next, 0, o, out);
  }
  return out;
}
/** The driver is told a task ended (charmNestLibrary driveSoon, for a worker): never while the drive is off; best-effort. */
async function kickDriver(db, by, o = {}) {
  try {
    const dv = await db.collection(OPEN).doc("drive").get(), d = dv.exists ? dv.data() || {} : null;
    if (!d || (d.mode !== "standby" && d.mode !== "on")) return { off: true };
    const got = await Lease.dispatch(db, db.collection("Charm_Nest_Drive").doc("lease"), "task:" + str(by, 40), { heartbeat: true });
    if (!got || !got.nonce) return { queued: true };
    if (typeof o.kick === "function") await o.kick(got.nonce); else await require("./_charmNestDrive").kick(got.nonce);
    return { dispatched: true };
  } catch (e) { console.warn("[nest] the driver was not told:", e && e.message); return { error: true }; }
}

module.exports = { start, pumpLanes, keepSlots, ensureSlot, kickSlot, kickDriver, slotsEnded, DEAD_MS, readSettings, readPolicy, stockFor, cornerPt, angleSet, usableArea, inflatedArea, topupRoom, groupOf, planEnv, plateOf,
  poolRowsOf, roseStockOf, sheetsOfRun, jobsOfRun, inputKeyOf, owedNest, refusal, activeCharms, jobBase, attemptId, seedOf, inputPathOf, jobRef, slotRef,
  JOBS, SHEETS, POOL, CARRIER, OPEN, CLOUD_DEV, MAX_ATTEMPTS, AUTO_ERROR_ATTEMPTS, MAX_SLOTS, MAX_SLOT_TRIES, CLOUD_CORES, ENDED, DEFAULTS, num, str, isId, sha };
