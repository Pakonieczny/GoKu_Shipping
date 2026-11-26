/*  netlify/functions/_charmNestWorld.js — THE CLOUD WORLD: every open order line read ONCE, here, and kept in Firestore
 *  (Paul, 20 Nov 2026: "Everything that was production related was supposed to be in the cloud ... the computers are never allowed to store their own
 *  information ... Only the cloud can decide." Plan: CLOUD-MIGRATION-EXECUTION.md, phases 1 and 2).
 *
 *  What it does, with no browser open:
 *    1. the open orders: the receipts the mail system already copies from Etsy every 7 to 10 minutes (EtsyMail_Receipts, whole, with their items).
 *       Etsy itself is asked ONLY for a receipt that copy does not hold whole, at most 500 calls in a Toronto day (Paul, 20 Nov 2026), counted in
 *       the cloud; a receipt read that way is kept (Charm_World_Etsy/<receipt>) until it changes.
 *    2. the shape: the Design Station's own shaping (charm-nest-etsy-shape.js, ported from design-1.html and tested against it), the staff note
 *       (the staff-note index, _staffNotes.js), the Team messages (Brites_Orders/<rid>/messages, newest 5), the completed ledger (_designCompleted.js).
 *    3. the inputs of the reading, all from the cloud, through the same ops the pages call (so they are exactly what a page gets): the master index
 *       (masterList), the option maps, SKU aliases and no-design list, the custom-order records and Review marks (customGet), Claude's custom readings
 *       and the decisions on them (customReadGet), the listings' SKU tables (listingSkus, stored copies only), cancels (cancelList), sends to sheet
 *       (customSheetGet), the open run's line records (runGet: what the run did with each line and every person's decision on it).
 *    4. the reading: charm-nest-orders.js interpretLine, with the context the sorter builds (charm-nest-bridge.js Orders.ctx / readLine /
 *       interpretAll, Master.entryFor / looseFor / likeFor / sharedFor), and the pure part of interpretAll after it (overrides, Release Anyway, sends).
 *    5. Charm_World_Lines/<lineKey>: the full line (the order, the Etsy line, the reading, the problems with every detail, the run's state and every
 *       decision) written only when it changed; a line that left is written `gone`. Charm_World/summary: the counts, the open keys' hash, when.
 *    6. Review: charm-nest-review-core.js builds the cards and every count from those lines and records; kept in Charm_World/review. In drive mode "on" its Resolved
 *       decisions are the cloud's list (Charm_Nest_Review_Settled, _charmNestReviewSettled.js: S2, Paul 21 Nov 2026).
 *  Production only for now (the sandbox's orders come from Charm_Sandbox: a later step). Nothing a page shows is changed by this file yet: the pages
 *  are compared with it first (Settings → Computers), then switched to draw from it.                                                              */
"use strict";

const O = require("../../charm-nest-orders.js");
let Shape = null, ReviewCore = null;
try { Shape = require("../../charm-nest-etsy-shape.js"); } catch (_) { Shape = null; }
try { ReviewCore = require("../../charm-nest-review-core.js"); } catch (_) { ReviewCore = null; }

const LINES = "Charm_World_Lines", WORLD = "Charm_World", ETSY = "Charm_World_Etsy";
const SWEEP_MS = 30 * 60000, HYDRATE_MAX = 40, HYDRATE_PAR = 2, MSG_TTL = 10 * 60000, MSG_MAX = 200, VERSION = 1, FULL_MS = 10 * 60000;
/* ETSY (Paul, 20 Nov 2026: "1500 a day is unacceptable. I'm giving you a limit of 500"): the orders come from the receipts the mail system already copies from Etsy
   every 7 to 10 minutes (EtsyMail_Receipts, etsyMailReceiptsMirrorCron: whole receipts with their items, paid for already). This file calls Etsy itself ONLY for a
   receipt that copy does not hold whole, and never more than ETSY_DAY_CAP times in a Toronto day, counted in the cloud (Charm_World/etsy, a transaction per call). */
const MIRROR = "EtsyMail_Receipts", ETSY_DAY_CAP = 500;
const torontoDay = t => { try { return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(t)); } catch (_) { return new Date(t).toISOString().slice(0, 10); } };
/** One Etsy call, if today's count allows it: true (counted) or false (the day's 500 are spent). */
async function etsySpend(db) {
  const ref = db.collection("Charm_World").doc("etsy"), day = torontoDay(Date.now());
  return db.runTransaction(async tx => {
    const s = await tx.get(ref), d = s.exists ? s.data() || {} : {}, used = d.day === day ? +d.used || 0 : 0;
    if (used >= ETSY_DAY_CAP) return false;
    tx.set(ref, { day, used: used + 1, cap: ETSY_DAY_CAP, at: Date.now() });
    return true;
  });
}
async function etsyUsed(db) { try { const s = await db.collection("Charm_World").doc("etsy").get(), d = s.exists ? s.data() || {} : {}; return d.day === torontoDay(Date.now()) ? +d.used || 0 : 0; } catch (_) { return null; } }
const stampOf = r => Number((r && (r.updated_timestamp || r.update_timestamp)) || 0) || 0;
const wholeRaw = r => !!(r && r.receipt_id && Array.isArray(r.transactions) && r.transactions.length);
/* COST: a warm instance keeps the master index it read, under the index's signature (masterList answers it with its first part): read again only when it moved */
const meterTag = t => { try { return global.__readMeter && global.__readMeter.setTag ? global.__readMeter.setTag(t) : ""; } catch (_) { return ""; } };   // (READMETER: each part of the build is counted under its own name)
const CACHE = { masterSig: null, masterEntries: null, mirror: new Map(), etsy: null };
/* READCUT (Paul, 25 Nov 2026: Firestore cost; the read meter: each build read every open receipt twice, ~1,550 copied receipts and ~1,420 kept ones, every two minutes, changed or not):
   · CACHE.mirror: a copied receipt (EtsyMail_Receipts) as this instance last read it, under its own updated_timestamp: a receipt whose stamp the build already knows (open: the sweep's
     and the watermark's reading of the very same field) is not read again while the stamp stands. A receipt not open (a run's line only) is read as before.
   · CACHE.etsy: the kept receipts (Charm_World_Etsy, written by the build alone) as this instance holds them after its own build, under that build's sequence (state.buildSeq): reused only
     when the build just before was this instance's own (no other build wrote in between); any other case reads them all, as before. */
/* COST: what moved in the cloud since the last build, for four document reads: the changes feed's shards (every write a page makes moves one: a decision, a
   map, a custom record, a cancel, a run's lines, a piece's place). A build with no Etsy change and no feed move, less than FULL_MS after the last, is skipped. */
async function feedToken(db) { const ds = await db.getAll(...['library', 'feed-p', 'feed-o', 'feed-x'].map(id => db.collection('Charm_Nest_Rev').doc(id))); return ds.map(d => (d.exists ? String((d.data() || {}).n || 0) : '-')).join('.'); }
/* Etsy's daily limit, as the mail sync records it when Etsy refuses for the day (EtsyMail_Config/syncState): no Etsy call until it resets */
async function etsyLocked(db) { try { const s = await db.collection('EtsyMail_Config').doc('syncState').get(), d = s.exists ? s.data() || {} : {}, t = d.etsyDailyLimitResetAt; const ms = t && t.toMillis ? t.toMillis() : +t || 0; return ms > Date.now() ? ms : 0; } catch (_) { return 0; } }
/* (CLOUDDRIVE: whether the cloud drive is in standby or on: Charm_Nest_Open/drive; absent, "off" or unreadable is off) */
async function driveMode(db) { try { const s = await db.collection("Charm_Nest_Open").doc("drive").get(), m = s.exists ? (s.data() || {}).mode : null; return m === "standby" || m === "on" ? m : "off"; } catch (_) { return "off"; } }
async function driveOn(db) { try { const s = await db.collection("Charm_Nest_Open").doc("drive").get(), m = s.exists ? (s.data() || {}).mode : null; return m === "standby" || m === "on"; } catch (_) { return false; } }
const CANCEL_STATUS = /cancel|fully refunded/i;

const sha = s => require("crypto").createHash("sha256").update(String(s)).digest("hex").slice(0, 24);
const J = v => { try { return JSON.stringify(v); } catch (_) { return ""; } };
const lineKeyOf = (rid, tid) => `${rid}_${tid}`;   // (charm-nest-orders.js lineKey(order, line): the same key, from the ids)
const isOpenReceipt = r => !!r && (r.is_paid !== false) && !r.is_shipped && !CANCEL_STATUS.test(String(r.status || ""));

/* ── the master index, as Master (charm-nest-bridge.js §19) keeps it ── */
function masterIndex(entries) {
  const m = new Map((entries || []).map(e => [String(e.sku || "").toUpperCase(), e]));
  const loose = new Map(), like = new Map(), shared = new Map();
  for (const k of m.keys()) { const lk = O.looseKey(k); loose.set(lk, loose.has(lk) ? "" : k); const a = like.get(lk); if (a) a.push(k); else like.set(lk, [k]); }
  for (const [k, v] of m) { const f = O.designFile ? O.designFile(v) : ""; if (!f) continue; const a = shared.get(f); if (a) a.push(k); else shared.set(f, [k]); }
  const entryFor = sku => m.get(String(sku || "").toUpperCase()) || null;
  return {
    entries: m, entryFor,
    looseFor: sku => loose.get(O.looseKey(sku)) || "",
    likeFor: sku => (like.get(O.looseKey(sku)) || []).slice(),
    sharedFor: sku => { const own = m.get(String(sku || "").toUpperCase()), f = own && O.designFile ? O.designFile(own) : ""; return f ? shared.get(f) || [] : []; }
  };
}

/* ── the reading of one line: charm-nest-bridge.js readLine (the fallback for a line the reader cannot read included) ── */
function readLine(row, ctx) {
  try { const c = Object.assign({}, ctx); if (row.blockOverride) c.blockOverride = row.blockOverride; if (row.skuOverride) c.skuOverride = row.skuOverride; return O.interpretLine(row.order, row.line, c); }
  catch (e) {
    const l = row.line && typeof row.line === "object" ? row.line : {}, o = row.order || {}, say = String((e && e.message) || e).slice(0, 140), sku = String(l.sku == null ? "" : l.sku).trim().toUpperCase().slice(0, 80);
    const bare = { transactionId: String(l.transactionId == null ? "" : l.transactionId), listingId: String(l.listingId == null ? "" : l.listingId), productId: "", sku, title: "", quantity: Number(l.quantity) || 1, metalKey: String(l.metalKey == null ? "" : l.metalKey), metalLabel: String(l.metalLabel == null ? "" : l.metalLabel), personalization: [], buyerMessage: "", variations: [] };
    const plain = Object.assign({}, o, { buyerMessage: "", staffNote: "", messages: [] });
    let sp; try { sp = O.interpretLine(plain, bare, ctx); } catch (_) { sp = O.interpretLine(plain, Object.assign({}, bare, { sku: "" }), ctx); }
    const reason = `this piece could not be read (${say}): it waits for a person, the other pieces go on`;
    if (sp.noDesign || sp.ownDesign) return sp;
    sp.problems = [{ kind: "unmatchedSku", reason, sku, listingId: bare.listingId, title: String(l.title == null ? "" : l.title).slice(0, 160), formHold: true, readFailed: true, lineForm: "unknown", designForm: "unknown", siblings: [] }].concat(sp.problems.filter(p => p.kind === "needsMaterial"));
    sp.formHold = { sku, design: "", lineForm: "unknown", designForm: "unknown", reason, siblings: [], readFailed: true }; sp.siblings = [];
    sp.designSku = null; sp.skuSource = null; sp.sources = Object.assign({}, sp.sources, { sku: null });
    return sp;
  }
}

/** The decisions and the run's facts a line carries, from the run's line record (Orders.lineRecord): kept on the cloud line as they are. */
const RUN_FIELDS = ["state", "poolIds", "reason", "hold", "heldAt", "holdSeen", "wait", "engrave", "engraveCandidate", "noDesign", "groupSize", "sides", "handDone", "activityAt", "changePending", "repoolChanged", "arrivedAt",
  "materialOverride", "sizeOverride", "engravingOverride", "skuOverride", "blockOverride", "releaseAnyway", "releaseHold", "frontAt", "releasing", "etsyClosed", "crh", "_lp", "material", "quantity", "pieceCount", "sku",
  // (CLOUDDRIVE §1.9: the fields only the cloud writes on a run's line; no line has them while the drive is off, so nothing here changes until then)
  "poolProblems", "changedAt", "changedWhy", "designHold"];
function runFacts(rec) { const out = {}; if (!rec || typeof rec !== "object") return out; for (const k of RUN_FIELDS) if (rec[k] !== undefined) out[k] = rec[k]; return out; }

/** The pure part of interpretAll after the reading (charm-nest-bridge.js ~1243-1270): the state a reading moves, a send to sheet, the overrides and Release Anyway. */
function afterRead(row, prev, sent) {
  row.problems = (row.spec.problems || []).slice();
  if (row.spec.noDesign) row.state = row.state === "pulled" ? "noDesign" : row.state;
  if (O.questionGone && prev && O.questionGone(row, prev)) { row.state = "pulled"; row.reason = null; }
  if ((row.spec.customDone || (row.spec.special && row.spec.special.notCut)) && !row.hold && !(row.poolIds || []).length && ["held", "unmatched", "waiting", "oversize"].includes(row.state)) { row.state = "noDesign"; row.reason = null; }
  else if (row.spec.ownDesign && !row.spec.customDone && !row.hold && !(row.poolIds || []).length && ["held", "unmatched", "waiting", "oversize"].includes(row.state)) { row.state = "held"; row.reason = O.specialOf && O.specialOf.addOn ? O.specialOf.addOn.wait : row.reason; }
  if (sent && !row.spec.customDone) {
    row.problems = []; row.spec.engraveCandidate = false; if (sent.metal) row.spec.material = sent.metal;
    // (CUSTOMBACK, as the page's interpretAll: a line whose file drew its charms' backs engraves them, from the drawing, approved in Design & Engrave)
    if (+sent.backs > 0) { row.spec.customBack = +sent.backs; row.spec.engraveCandidate = true; } else delete row.spec.customBack;
    // (MULTICHARM, 25 Nov 2026, as the page's interpretAll: the line is the pieces its card sent it, not its Etsy quantity; an earring pair keeps its own Left + Right count)
    if (sent.pieces > 0 && !(row.spec.pair && row.spec.pair.earring === true) && O.pinPieces) { row.spec.customPieces = sent.pieces; O.pinPieces(row.spec, sent.pieces); delete row.spec.pieceNote; }
    if (sent.pieces && !row.hold && !(row.poolIds || []).length && ["held", "unmatched", "oversize", "noDesign"].includes(row.state)) { row.state = "pulled"; row.reason = null; }
  }
  if (row.materialOverride) { row.spec.material = row.materialOverride; row.problems = row.problems.filter(p => p.kind !== "needsMaterial"); }
  if (row.sizeOverride) { row.spec.size = row.sizeOverride; row.problems = row.problems.filter(p => p.kind !== "missingSize"); }
  row.material = row.spec.material;
  if (row.releaseAnyway && Array.isArray(row.releaseAnyway.kinds) && row.problems.length) row.problems = row.problems.filter(p => !row.releaseAnyway.kinds.includes(p.kind));
  return row;
}

/* ── the Review core's snapshot (charm-nest-review-core.js, THE SNAPSHOT): the lines as its rules read them, the sends, the custom records ── */
function coreLine(r) {
  const o = r.order || {}, l = r.line || {}, sp = r.spec || null;
  return { key: r.key, state: r.state || "pulled", reason: r.reason || null, hold: r.hold || null, heldAt: +r.heldAt || 0, holdSeen: !!r.holdSeen, placement: null,
    poolIds: (r.poolIds || []).slice(), frontAt: +r.frontAt || 0, releasing: r.releasing ? { at: +r.releasing.at || 0, by: r.releasing.by || "" } : null,
    arrivedAt: +r.arrivedAt || 0, activityAt: +r.activityAt || 0, changePending: !!r.changePending, changedAt: +r.changedAt || 0, changedWhy: String(r.changedWhy || ""),
    order: { receiptId: String(o.receiptId || String(r.key).split("_")[0]), orderNumber: String(o.orderNumber || o.receiptId || ""), createTs: +o.createTs || 0, updateTs: +o.updateTs || 0, shipBy: +o.shipBy || 0, buyer: (o.buyer && o.buyer.name) || "" },
    line: { transactionId: String(l.transactionId || String(r.key).split("_")[1] || ""), listingId: String(l.listingId || ""), sku: String(l.sku || ""), title: String(l.title || "") },
    spec: sp ? { designSku: sp.designSku || null, special: sp.special ? { label: sp.special.label || "", monogram: !!sp.special.monogram, notCut: !!sp.special.notCut } : null, material: sp.material || null } : null,
    problems: JSON.parse(J(r.problems || [])) };
}
function coreSends(customSheet) {
  const out = [];
  for (const [k, e] of Object.entries(customSheet || {})) { if (!e) continue; const s = e.sent || e.sendIntent; if (!s || !s.lines) continue; out.push({ ck: e.ck || k, rid: String(e.rid || ""), phase: e.sent ? "sent" : "pending", sent: { at: +s.at || 0, by: s.by || "", id: s.id || "", lines: Object.fromEntries(Object.keys(s.lines).map(x => [x, []])) } }); }
  return out;
}

/* ── THE FLOW: the page's rules (charm-nest-flow-stages.js count / orderRank / lineRank), with every line's placement made as the page makes it (charm-nest-order-pieces.js resolve over the
   order's own read, op getOrderPieces; then the page's factsOf and charm-nest-piece-placement.js resolve), from ALL the records, never from what one computer happened to have loaded ── */
let FlowStages = null, OPieces = null, PPlace = null, PairM = null;
try { const m = require("../../charm-nest-flow-stages.js"); FlowStages = m && m.CNFlow ? m.CNFlow : null; } catch (_) { FlowStages = null; }
try { OPieces = require("../../charm-nest-order-pieces.js"); } catch (_) { OPieces = null; }
try { PPlace = require("../../charm-nest-piece-placement.js"); } catch (_) { PPlace = null; }
try { PairM = require("../../charm-nest-pair.js"); } catch (_) { PairM = null; }
const FLOW_GONE = new Set(["abandoned", "superseded"]);
const cut$ = s => !!(s && (+s.laserDoneAt > 0 || +s.roseCutAt > 0));
/** charm-nest-order-pieces.js lineOfRow: the order line as OrderPieces reads it. */
function pieceLine(r) {
  const sp = r.spec || {}, ln = r.line || {};
  return { key: r.key, receiptId: r.order && r.order.receiptId, form: sp.form || "", transactionId: ln.transactionId, sku: sp.designSku || ln.sku || "", title: ln.title || "", material: r.material || sp.material || r.metal || null,
    quantity: sp.quantity || ln.quantity || 1, state: r.state, poolIds: r.poolIds || [], hold: r.hold || null, changePending: !!r.changePending, problems: r.problems || [], spec: { noDesign: sp.noDesign, customDone: sp.customDone || null, pieceCount: sp.pieceCount, pair: sp.pair, ...(sp.customPieces > 0 ? { customPieces: sp.customPieces } : {}) }, reason: r.reason || "", listingId: ln.listingId || "" };
}
/** The line's sheet facts (what the page's factsOf takes from the line's copies): what a cached reading keeps between builds. A line's own hold, hand and cancel are added fresh at each build. */
function sheetFactsOf(rid, rows, answer, entryFor, setCut) {
  if (!OPieces) return {};
  const pairArg = l => ({ receiptId: l.receiptId, transactionId: l.transactionId, sku: l.sku, quantity: Math.max(1, Math.round(+(l.spec && l.spec.quantity || l.quantity) || 1)), form: l.form || "", key: l.key, spec: l.spec || {}, line: l });
  const entryOf = l => { try { return l && l.sku && entryFor ? entryFor(l.sku) || null : null; } catch (_) { return null; } };
  const earring = l => { try { return !!(PairM && l && (typeof PairM.isEarringPair === "function" ? PairM.isEarringPair(pairArg(l), entryOf(l)) : PairM.isMismatched(entryOf(l)))); } catch (_) { return false; } };
  const pairOf = l => { if (!earring(l)) return null; try { const x = PairM.piecesFor(pairArg(l), entryOf(l)); return Array.isArray(x) && x.length > 1 && x.some(p => p.side) ? x : null; } catch (_) { return null; } };
  const groupLine = l => { try { return !!(PairM && l && typeof PairM.isGroupLine === "function" && PairM.isGroupLine(pairArg(l), entryOf(l))); } catch (_) { return false; } };
  const kindOf = l => { if (!earring(l) || typeof PairM.kindOf !== "function") return null; try { return PairM.kindOf(pairArg(l), entryOf(l)) || null; } catch (_) { return null; } };
  const countOf = l => { try { const c = typeof O.countedPieces === "function" ? O.countedPieces(l) : null; return c ? c.total : 0; } catch (_) { return 0; } };
  const pieces = OPieces.resolve({ orderId: rid, lines: rows.map(pieceLine), pools: (answer && answer.pools) || [], sheets: (answer && answer.sheets) || [], sheetsKnown: !!answer, failed: !answer, pairOf, kindOf, groupLine, countOf });
  const out = {};
  for (const r of rows) {
    const mine = pieces.filter(c => c.lineKey === r.key), sheets = [], seen = new Set();
    for (const c of mine) if (c && c.nested) { const k = c.sheetId || c.sheetLabel; if (!k || seen.has(k)) continue; seen.add(k); sheets.push({ id: c.sheetId || null, label: c.sheetLabel || "", setId: c.setId || null, cut: c.state === "cut" || !!(c.sheetId && setCut(c.sheetId, c.setId)) }); }
    const ch = mine.find(c => c && c.hold);
    out[r.key] = { q: mine.length, on: mine.filter(c => c && c.nested).length, sh: sheets, ch: ch ? { reason: String(ch.hold.reason || "").slice(0, 200), at: +ch.hold.at || 0, by: String(ch.hold.by || "").slice(0, 80) } : null, ld: mine.some(c => c && c.loading) ? 1 : 0, un: mine.some(c => c && c.unsure) ? 1 : 0 };
  }
  return out;
}
/** The page's factsOf + PiecePlacement.resolve for one line, from its sheet facts and the line itself. */
function placeOfLine(r, f, cancelled) {
  if (!PPlace) return null;
  const waits = !r.hold && !!(r.spec && r.spec.special && r.spec.special.monogram);
  const own = r.hold || (r.state === "held" && !waits) ? { reason: String(typeof r.hold === "string" ? r.hold : (r.hold && (r.hold.reason || r.hold.text)) || r.reason || "").slice(0, 200), at: +(r.hold && r.hold.at) || 0, by: String((r.hold && r.hold.by) || "").slice(0, 80) } : null;
  const hold = own || (f && f.ch) || null;
  const hc = !hold ? (() => { const l = pieceLine(r), c = !(l.poolIds && l.poolIds.length) && (r.state === "skipped" ? { state: "completed", how: "skip" } : r.handDone || (l.spec && l.spec.customDone)); return c && typeof c === "object" && c.state !== "open" && c.how !== "sheet" ? c : null; })() : null;
  return PPlace.resolve({ key: r.key, qty: Math.max((f && f.q) || 0, 1), copies: Math.max((f && f.q) || 0, 1), copiesOn: (f && f.on) || 0, sheets: (f && f.sh) || [], hold, hand: hc, cancelled: cancelled || r.state === "gone", loading: !!(f && f.ld), unsure: !!(f && f.un) || !f });
}
function flowOf(rows, facts, x) {
  if (!FlowStages || !PPlace) return null;
  const memo = new Map(), place = r => { if (!memo.has(r.key)) { const p = placeOfLine(r, facts[r.key], x.cancelled.has(String(r.key).split("_")[0])); memo.set(r.key, p && p.state !== "loading" ? p : null); } return memo.get(r.key); };
  const learned = rid => { const v = x.learnedMap[String(rid)]; return Array.isArray(v) ? +v[0] || 0 : 0; };
  const ctx = { place, held: r => !!r.hold || (place(r) || {}).state === "hold", nested: () => false, review: r => x.reviewLines.has(r.key),
    cut: (r, pl) => { const on = pl && pl.sheets ? pl.sheets : []; return on.length > 0 && on.every(s => s && s.cut); }, learned,
    reviewOrder: rid => x.openOrders.has(String(rid)), engraving: rid => x.engraving.has(String(rid)) };
  const out = FlowStages.count(rows, ctx, Object.entries(x.completedMap || {}).map(([rid, e]) => Object.assign({ rid }, e && typeof e === "object" ? e : {})));
  return { counts: out.counts, byRid: Object.fromEntries(out.byRid) };
}

async function pool(items, n, fn) { const out = new Array(items.length); let i = 0; await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const k = i++; try { out[k] = await fn(items[k], k); } catch (e) { out[k] = { error: e }; } } })); return out; }

/* ── the build ── */
async function build(opts) {
  const { db, admin, Lib } = opts, FV = admin.firestore.FieldValue, now = opts.now || Date.now(), log = opts.log || (() => {}), budgetEnd = now + (opts.budgetMs || 11 * 60000);
  const etsy = opts.etsy || require("./_etsyMailEtsy");
  const ops = Lib.ops, notes = [], errors = [];
  const say = t => { notes.push(t); log(t); };
  if (!Shape) throw new Error("charm-nest-etsy-shape.js is not deployed");
  const stateRef = db.collection(WORLD).doc("state"), st = (await stateRef.get()).data() || {};
  const open = Object.assign({}, st.open || {});   // receipt id → its update stamp, every receipt open on Etsy

  /* (CLOUDDRIVE build step 9, P11 / E7; an additive field pages ignore): when the mail system last finished a copy of Etsy's receipts, read BEFORE the copy is read below, so a
     step that waits for a sync after it started knows this build read that sync's copy (the driver's freshness gate: summary.mirrorBefore). mirrorAt below stays as it is. */
  let mirrorBefore = null; try { const ms0 = await db.doc("EtsyMail_Config/receiptsMirrorState").get(), m0 = ms0.exists ? ms0.data() || {} : {}, t0 = m0.lastSyncCompletedAt; mirrorBefore = t0 && t0.toMillis ? t0.toMillis() : +t0 || null; } catch (_) { mirrorBefore = null; }
  meterTag("world:receipts");
  /* 1 · which receipts are open: from the receipts the mail system copies from Etsy (no Etsy call here) */
  const sweep = opts.sweep || !st.sweptAt || now - st.sweptAt > SWEEP_MS || !Object.keys(open).length;
  let watermark = Number(st.mirrorMark) || 0, etsyMoved = 0;
  const mirrored = new Map();   // receipt id → the copied receipt (raw), read in this build
  try {
    if (sweep) {
      // every receipt the copy holds as not shipped, filtered as the station's list is (paid, not shipped, not cancelled)
      const snap = await db.collection(MIRROR).where("is_shipped", "==", false).select("receipt_id", "status", "is_paid", "is_shipped", "updated_timestamp").get();
      for (const k of Object.keys(open)) delete open[k];
      for (const d of snap.docs) { const x = d.data() || {}; if (isOpenReceipt(x)) open[String(x.receipt_id || d.id)] = stampOf(x); watermark = Math.max(watermark, stampOf(x)); }
      st.sweptAt = now; etsyMoved = snap.size;
      say(`[world] the copied receipts: ${Object.keys(open).length} open of ${snap.size} not shipped`);
    } else {
      const snap = await db.collection(MIRROR).where("updated_timestamp", ">=", Math.max(0, watermark - 600)).orderBy("updated_timestamp").limit(2000).get();
      for (const d of snap.docs) { const x = d.data() || {}, id = String(x.receipt_id || d.id), was = open[id]; if (isOpenReceipt(x)) open[id] = stampOf(x); else delete open[id]; if (was !== open[id]) etsyMoved++; watermark = Math.max(watermark, stampOf(x)); if (wholeRaw(x.raw)) { mirrored.set(id, x.raw); CACHE.mirror.set(id, { stamp: stampOf(x), raw: x.raw }); } }
    }
  } catch (e) { errors.push("receipts copy: " + String(e.message || e).slice(0, 200)); say("[world] the copied receipts were not read: " + (e.message || e)); if (!Object.keys(open).length) throw e; }
  let mirrorAt = null; try { const ms = await db.doc("EtsyMail_Config/receiptsMirrorState").get(), m = ms.exists ? ms.data() || {} : {}, t = m.lastSyncCompletedAt; mirrorAt = t && t.toMillis ? t.toMillis() : +t || null; } catch (_) { mirrorAt = null; }
  if (mirrorAt && now - mirrorAt > 30 * 60000) errors.push("the mail system's copy of Etsy's receipts was last updated " + Math.round((now - mirrorAt) / 60000) + " min ago");

  /* (nothing moved: no new build; one every FULL_MS all the same, for what moves no counter: a staff note, a Team message, a receipt read whole later) */
  let token = ""; try { token = await feedToken(db); } catch (_) { token = ""; }
  if (!opts.force && !sweep && !etsyMoved && token && token === st.token && now - (+st.lastBuildAt || 0) < FULL_MS && !(+st.toRead > 0)) {
    await stateRef.set({ open, mirrorMark: watermark, checkedAt: now }, { merge: true });
    return { skipped: true, at: now, token, errors };
  }

  meterTag("world:run");
  /* 2 · the open run: its lines are in production until it says otherwise (a line Etsy closed stays until our own shipping ships it) */
  let run = null;
  try { const o = await ops.runOpen({ only: "get" }); if (o && o.runId) { const g = await ops.runGet({ runId: o.runId }); run = g && g.run ? g.run : null; } } catch (e) { errors.push("run: " + e.message); }
  const runLines = (run && run.lines) || {};
  const runReceipts = new Set(Object.entries(runLines).filter(([, l]) => l && l.state !== "gone").map(([k]) => String(k).split("_")[0]));

  meterTag("world:etsy");
  /* 3 · each receipt whole: the copy's own (it holds the items), else one Etsy read, counted against the day's 500 and kept until the receipt changes */
  const want = [...new Set([...Object.keys(open), ...runReceipts])];
  const kept = new Map(), seq = +st.buildSeq || 0, ec = CACHE.etsy && CACHE.etsy.seq === seq ? CACHE.etsy.docs : null;   // (READCUT: this instance's own copy, when the last build was its own)
  const keptAsk = ec ? want.filter(id => !ec.has(id)) : want;
  if (ec) for (const id of want) { const v = ec.get(id); if (v) kept.set(id, v); }
  for (let i = 0; i < keptAsk.length; i += 300) { const docs = await db.getAll(...keptAsk.slice(i, i + 300).map(id => db.collection(ETSY).doc(id))); docs.forEach(d => { if (d.exists) kept.set(d.id, d.data()); if (ec) ec.set(d.id, d.exists ? d.data() : null); }); }
  const etsyDocs = ec || new Map(want.map(id => [id, kept.get(id) || null]));   // (what this build knows of every kept receipt it read: carried to the next build of this instance)
  CACHE.etsy = null;   // (until this build has saved its state: a build that fails half-way leaves no copy behind)
  for (const id of want) { const c = CACHE.mirror.get(id); if (!mirrored.has(id) && c && open[id] != null && Number(c.stamp) === Number(open[id])) mirrored.set(id, c.raw); }   // (READCUT: the copy as read under this very stamp)
  const needCopy = want.filter(id => !mirrored.has(id));
  for (let i = 0; i < needCopy.length; i += 300) { const docs = await db.getAll(...needCopy.slice(i, i + 300).map(id => db.collection(MIRROR).doc(id))); docs.forEach(d => { const x = d.exists ? d.data() || {} : null; if (x && wholeRaw(x.raw)) { mirrored.set(d.id, x.raw); CACHE.mirror.set(d.id, { stamp: stampOf(x), raw: x.raw }); } }); }
  if (CACHE.mirror.size > 6000) { const live = new Set(want); for (const k of [...CACHE.mirror.keys()]) if (!live.has(k)) CACHE.mirror.delete(k); }
  for (const id of want) { const raw = mirrored.get(id); if (raw) kept.set(id, Object.assign({}, kept.get(id), { rid: id, raw, stamp: stampOf(raw), via: "copy" })); }
  const stale = want.filter(id => { const k = kept.get(id); return !k || !k.raw || (open[id] != null && k.via !== "copy" && Number(k.stamp) !== Number(open[id])); });
  const locked = stale.length ? await etsyLocked(db) : 0;   // (Etsy refused the shop for the day: the mail sync recorded it; nothing is asked until it resets)
  let doNow = locked ? [] : stale.slice(0, HYDRATE_MAX); let etsyCalls = 0, etsyRefused = 0;
  if (locked) errors.push("Etsy's daily limit for the shop: no Etsy call until " + new Date(locked).toISOString());
  /* (CLOUDDRIVE E2, cloud plan v2 step 1a; DORMANT): while the cloud drive is in standby or on, the shop's Etsy switch (_etsyStop.js) is read strictly before the day's calls are
     counted: when it is on, or cannot be read, nothing is asked of Etsy and nothing is counted (each call would only be answered by the switch, and spend the day's 500).
     The receipts stay to be read (toRead), so the first build after Resume Etsy reads them. With the drive absent or "off" this is today's build exactly. */
  if (doNow.length && (await driveOn(db))) {
    let stopped = true; try { const s = await db.doc(require("./_etsyStop").DOC).get(); stopped = !!(s.exists && (s.data() || {}).stopped === true); } catch (_) { stopped = true; }
    if (stopped) { errors.push(`Etsy is stopped for the whole shop: ${stale.length} receipt(s) wait for Resume Etsy`); doNow = []; }
  }
  if (stale.length) say(`[world] ${stale.length} receipt(s) the copy does not hold whole: ${doNow.length} asked of Etsy now (at most ${ETSY_DAY_CAP} calls a day)`);
  const readNow = new Set();
  await pool(doNow, HYDRATE_PAR, async id => {
    if (Date.now() > budgetEnd || etsyRefused) return;
    if (!(await etsySpend(db).catch(() => false))) { etsyRefused++; return; }
    etsyCalls++;
    const raw = await etsy.getShopReceiptFull(id);
    if (!raw || !raw.receipt_id) return;
    readNow.add(id);   // (CLOUDDRIVE: read whole now; summary.toReadIds lists the ones still to read)
    const rec = { rid: id, stamp: stampOf(raw), raw, via: "etsy", readAt: Date.now(), open: isOpenReceipt(raw) };
    kept.set(id, Object.assign({}, kept.get(id), rec));
    if (!rec.open) delete open[id];
    await db.collection(ETSY).doc(id).set(Object.assign({}, JSON.parse(J(rec)), { updatedAt: FV.serverTimestamp() }), { merge: true });
    etsyDocs.set(id, Object.assign({}, etsyDocs.get(id) || {}, JSON.parse(J(rec))));   // (READCUT: this instance's copy says what it wrote)
  });
  if (etsyRefused) errors.push(`Etsy: today's ${ETSY_DAY_CAP} calls are spent; ${stale.length - etsyCalls} receipt(s) wait for tomorrow or for the mail system's copy`);

  meterTag("world:notes");
  /* 4 · notes, messages, completed */
  const rids = want.filter(id => kept.get(id) && kept.get(id).raw);
  let noteMap = new Map();
  try { const n = await require("./_staffNotes").lookup(db, FV, rids, { now }); if (n && n.notes) noteMap = n.notes; } catch (e) { errors.push("notes: " + e.message); }
  for (const [id, t] of noteMap) if (t === true) { try { const s = await db.collection("Brites_Orders").doc(id).get(); noteMap.set(id, s.exists ? String((s.data() || {})["Staff Note"] || "") : ""); } catch (_) { noteMap.set(id, ""); } }
  let completed = new Set();
  try { const c = await require("./_designCompleted").list(db, FV, { now }); completed = new Set((c && c.ids) || []); } catch (e) { errors.push("completed: " + e.message); }
  const msgDue = rids.filter(id => { const k = kept.get(id); return !k.messagesAt || now - k.messagesAt > MSG_TTL; }).slice(0, MSG_MAX);
  await pool(msgDue, 8, async id => {
    const snap = await db.collection("Brites_Orders").doc(id).collection("messages").orderBy("timestamp", "desc").limit(5).get();
    const rows = snap.docs.map(d => { const m = d.data() || {}; const t = m.timestamp; return { senderName: String(m.senderName || "Staff"), text: String(m.text || ""), at: t && t.toMillis ? t.toMillis() : (t && t.seconds ? t.seconds * 1000 : null), imageUrl: m.imageUrl || null }; }).reverse();
    const k = kept.get(id); k.messages = rows; k.messagesAt = now;
    await db.collection(ETSY).doc(id).set({ messages: rows, messagesAt: now }, { merge: true });
    etsyDocs.set(id, Object.assign({}, etsyDocs.get(id) || {}, { messages: rows, messagesAt: now }));   // (READCUT)
  });

  meterTag("world:inputs");
  /* 5 · the inputs of the reading, from the cloud's own ops */
  let entries = null;
  /* READCUT (Paul, 25 Nov 2026: Firestore cost; Query Insights: the library read 500 records at a time, ~725 whole readings a day, ~2.6M reads): the build asked for the first 3,000 records BEFORE it
     compared the library's signature, so even a warm instance holding the library as it stands read it all again every two minutes. The signature alone is read first (masterList limit 1:
     a count and two one-record reads); then the copy this instance holds, else the library's saved file of that very signature (Storage, no database read), else the whole library side by
     side (masterNow) and saved for the next build; the old reading in parts only if all of that fails. A copy of another signature is never used. */
  { const sg = await ops.masterList({ limit: 1 }), sig = J(sg.index || null);
    if (CACHE.masterEntries && CACHE.masterSig === sig && sig !== "null") entries = CACHE.masterEntries;
    else {
      const bucket = bucketOf(opts), kept = await masterKept(bucket, sig);
      if (kept) entries = kept;
      else { const quick = await masterNow(db, sg.index).catch(() => null); if (quick) { entries = quick; keepMaster(bucket, sig, quick); } }
      if (!entries) { const first = await ops.masterList({ limit: 3000 }); entries = (first.entries || []).slice(); let cursor = first.next; for (let i = 0; cursor && i < 20; i++) { const r = await ops.masterList({ limit: 3000, cursor }); entries.push(...(r.entries || [])); cursor = r.next; } }
      CACHE.masterSig = sig; CACHE.masterEntries = entries;
    } }
  const M = masterIndex(entries);
  const [om, al, nd, cl, cancels] = await Promise.all([ops.optionMapGet({}), ops.aliasGet({}), ops.noDesignGet({}), ops.customGet({}), ops.cancelList({ idsOnly: true }).catch(e => { errors.push("cancels: " + e.message); return { ids: [] }; })]);
  const maps = { optionMaps: (om && om.maps) || {}, aliases: (al && al.aliases) || {}, noDesign: (nd && nd.list) || { patterns: [], skus: [], rows: [] } };
  try { O.cleanMaps(maps.aliases, maps.noDesign); } catch (_) {}
  const customDone = {}, customKept = {}, reviewMarks = {};
  const putCustom = (k, v) => { if (v && v.kind === "reviewMark") reviewMarks[k] = v; else if (v && v.state === "open") customKept[k] = v; else if (v) customDone[k] = v; };
  for (const [k, v] of Object.entries((cl && cl.records) || {})) putCustom(k, v);
  const cancelled = new Set(((cancels && cancels.ids) || []).map(String).filter(Boolean));

  /* 6 · the rows, shaped as the station shapes them */
  const rows = [];
  for (const id of rids) {
    if (cancelled.has(id)) continue;
    const k = kept.get(id), inRun = runReceipts.has(id);
    if (completed.has(id) && !inRun) continue;           // (the station drops an order the Design ledger completed)
    if (!open[id] && !inRun) continue;
    // (the station's path, hydrateOrder + applyHydration: the receipt as Etsy's list gives it, items included, hydrated with the same receipt read whole)
    const r = Shape.prepareReceipt(k.raw, { detail: k.raw });
    const order = Shape.shapeOrder(r, { staffNote: noteMap.get(id) || "", messages: k.messages || [], hasNote: noteMap.has(id), hasMessages: (k.messages || []).length > 0, completed: completed.has(id) });
    O.cleanOrders({ order });
    for (const line of order.lines || []) {
      const key = lineKeyOf(order.receiptId, line.transactionId), rec = runLines[key];
      rows.push(Object.assign({ key, order, line, state: "pulled", reason: null, poolIds: [] }, runFacts(rec), { etsyOpen: !!open[id], inRun: !!rec, _run: runFacts(rec) }));
    }
  }
  { const seenKey = new Set(); for (let i = rows.length - 1; i >= 0; i--) { if (seenKey.has(rows[i].key)) rows.splice(i, 1); else seenKey.add(rows[i].key); } }   // (one record per line, whatever Etsy repeats)
  // the run's own lines whose receipt could not be read now: kept as the run has them (never dropped for a read that failed)
  const have = new Set(rows.map(r => r.key));
  for (const [key, rec] of Object.entries(runLines)) if (rec && rec.state !== "gone" && !have.has(key) && !cancelled.has(String(key).split("_")[0])) rows.push(Object.assign({ key, order: null, line: null, fromRecord: true }, runFacts(rec), { etsyOpen: false, inRun: true, _run: runFacts(rec) }));

  meterTag("world:custom");
  /* 7 · readings: Claude's (custom) and the listings' SKU tables, from the stored copies */
  const readable = rows.filter(r => r.order);
  // Claude's custom readings (Charm_Nest_CustomRead/<line>: reads by hash, latest, the person's decision): the one the run names for the line (crh), else the latest stored,
  // as a page that holds no reading of its own takes the stored one (charm-nest-bridge.js CustomRead); a person's decision always
  const customRead = {}, customDecided = {};
  /* READCUT (Paul, 25 Nov 2026: Firestore cost): the line records below (Claude's readings, the sends to sheet, the engraving jobs) were read for EVERY live line at every build, ~1,600 each,
     most of them not even there (a missing document is a read too). When the build just before was this instance's own (state.buildSeq), only what changed since is read: the readings and
     the engraving jobs written since that build began (their own stamps, updatedAt and at: every write sets one; neither is ever deleted), the sends to sheet only if their revision moved
     (customSheetGet ifRev), and the lines that build did not have. Any doubt (another instance built, a query fails, too much moved) reads them all, as before. */
  const LC = CACHE.lines && CACHE.lines.seq === seq ? CACHE.lines : null, NL = { customRead: new Map(), crAt: now, engrave: new Map(), enAt: now, sheet: null, sheetRev: null, sheetLines: null };
  CACHE.lines = null;   // (until this build has saved its state)
  const SLACK = 5 * 60000, MOVED_MAX = 1500;
  try {
    const keys = readable.map(r => r.key), byKey = new Map(readable.map(r => [r.key, r])), crDocs = NL.customRead;
    let ask = keys;
    if (LC && LC.customRead) {
      try {
        const q = await db.collection("Charm_Nest_CustomRead").where("updatedAt", ">", admin.firestore.Timestamp.fromMillis(Math.max(0, LC.crAt - SLACK))).limit(MOVED_MAX).get();
        if (q.size < MOVED_MAX) { const moved = new Map(q.docs.map(d => [d.id, d.data() || {}])); ask = []; for (const k of keys) { if (moved.has(k)) crDocs.set(k, moved.get(k)); else if (LC.customRead.has(k)) crDocs.set(k, LC.customRead.get(k)); else ask.push(k); } }
      } catch (e) { ask = keys; crDocs.clear(); }
    }
    for (let i = 0; i < ask.length; i += 300) { const docs = await db.getAll(...ask.slice(i, i + 300).map(k => db.collection("Charm_Nest_CustomRead").doc(k))); for (const d of docs) crDocs.set(d.id, d.exists ? d.data() || {} : null); }
    for (const k of keys) { const x = crDocs.get(k); if (!x) continue; const r = byKey.get(k), reads = x.reads || {}, h = r && r.crh && reads[r.crh] ? r.crh : x.latest; if (h && reads[h]) customRead[k] = reads[h]; if (x.decided && x.decided.kind) customDecided[k] = x.decided; }
  } catch (e) { errors.push("customRead: " + e.message); NL.customRead = new Map(); }
  const listingIds = [...new Set(readable.map(r => String(r.line.listingId || "")).filter(Boolean))];
  const listingSkus = {};
  for (let i = 0; i < listingIds.length; i += 100) { try { const g = await ops.listingSkus({ listingIds: listingIds.slice(i, i + 100), cacheOnly: true }); Object.assign(listingSkus, (g && (g.tables || g.listings)) || {}); } catch (e) { errors.push("listingSkus: " + e.message); break; } }
  // sends to sheet (CustomSheet in the bridge: B.customDesigns, each card's record with sent / sendIntent lines → pieces, and its files with their metal)
  let sentOf = () => null, customSheet = {};
  try {
    const keys = readable.map(r => r.key);
    let same = false;
    if (LC && LC.sheetRev && LC.sheet && LC.sheetLines) {   // (READCUT: the sends to sheet as this instance read them, when their revision has not moved; only the new lines are read)
      const fresh = keys.filter(k => !LC.sheetLines.has(k));
      if (fresh.length <= 1000) { const g = await ops.customSheetGet({ lineKeys: fresh, ifRev: LC.sheetRev, wantRev: true }); if (g && !g.error && g.rev === LC.sheetRev && !g.full) { customSheet = Object.assign({}, LC.sheet, g.records || {}); NL.sheetRev = g.rev; same = true; } }
    }
    if (!same) for (let i = 0; i < keys.length; i += 1000) { const g = await ops.customSheetGet(Object.assign({ lineKeys: keys.slice(i, i + 1000) }, i === 0 ? { wantRev: true } : {})); Object.assign(customSheet, (g && g.records) || {}); if (i === 0) NL.sheetRev = (g && g.rev) || null; }
    NL.sheet = customSheet; NL.sheetLines = new Set(keys);
    const idx = new Map(); for (const e of Object.values(customSheet)) if (e && (e.sent || e.sendIntent)) for (const k of Object.keys((e.sent || e.sendIntent).lines || {})) idx.set(k, e);
    sentOf = row => { const e = idx.get(row.key); if (!e) return null; const list = (((e.sent || e.sendIntent).lines || {})[row.key] || []).filter(pc => !(pc && pc.removed)), pc = list[0], F = pc && (e.files || []).find(x => x.id === pc.f); return { metal: (F && F.metal) || null, pieces: list.length, sent: !!e.sent, backs: list.filter(q => { const G = q && (e.files || []).find(x => x.id === q.f); return !!G && Array.isArray(G.charmBacks) && G.charmBacks.includes(Number.isInteger(q.i) ? q.i : 0); }).length }; };
  } catch (e) { errors.push("customSheet: " + e.message); }

  meterTag("world:pool");
  /* 7b · where each line's pieces are (Charm_Pool: the pieces' own records, which every placement writes): a line whose live pieces are all on a written
     (or committed) set is written (or committed), whatever a run record saved earlier says (RVSAME: the run record of order 4228567858 said "pooled" while its
     piece was on RG_Nov.13.26_Set-1_Sheet-1). Only lines still before that are looked at. */
  try {
    const ask = rows.filter(r => Array.isArray(r.poolIds) && r.poolIds.length && !["written", "committed", "gone", "skipped"].includes(r.state));
    const ids = [...new Set(ask.flatMap(r => r.poolIds.map(String)))], pool = new Map();
    // (READCUT: the pieces' records as this instance read them at its last build, while the placement counter has not moved since (every op that may write a piece raises it, after it lands); else all)
    let pgen = null; try { const g = await db.collection("Charm_Nest_Rev").doc("placement").get(); pgen = g.exists ? Number((g.data() || {}).n) || 0 : null; } catch (_) { pgen = null; }
    const pc = LC && LC.pool && pgen != null && LC.poolGen === pgen ? LC.pool : null, pAsk = pc ? ids.filter(id => !pc.has(id)) : ids, seen = new Map();
    if (pc) for (const id of ids) { const v = pc.get(id); if (v !== undefined) { seen.set(id, v); if (v) pool.set(id, v); } }
    for (let i = 0; i < pAsk.length; i += 300) for (const d of await db.getAll(...pAsk.slice(i, i + 300).map(id => db.collection("Charm_Pool").doc(id)))) { seen.set(d.id, d.exists ? d.data() || {} : null); if (d.exists) pool.set(d.id, d.data() || {}); }
    NL.pool = seen; NL.poolGen = pgen;
    for (const r of ask) {
      const live = r.poolIds.map(id => pool.get(String(id))).filter(p => p && !["abandoned", "superseded"].includes(String(p.state || "")));
      if (!live.length || live.length !== r.poolIds.length) continue;
      if (live.every(p => p.state === "committed")) r.state = "committed";
      else if (live.every(p => ["written", "committed"].includes(String(p.state)) && p.sheetId)) r.state = "written";
    }
  } catch (e) { errors.push("pieces: " + e.message); }

  meterTag("world:read");
  /* 8 · read every line */
  const seen = O.seenSkus ? O.seenSkus(readable.map(r => r.line)) : {};
  const ctx = { optionMaps: maps.optionMaps, aliases: maps.aliases, noDesign: maps.noDesign, customDone, customRead, customDecided, listingSkus, seen,
    masterEntry: M.entryFor, masterLoose: M.looseFor, masterShared: M.sharedFor, masterLike: M.likeFor, masterSkus: () => [...M.entries.keys()] };
  for (const row of readable) {
    if (row.state === "gone") continue;
    row.spec = readLine(row, ctx);
    afterRead(row, null, sentOf(row));
  }

  meterTag("world:write");
  /* 9 · write what changed */
  const prevSnap = await db.collection(WORLD).doc("summary").get(), prev = (prevSnap.exists && prevSnap.data()) || {};
  const prevSigs = Object.assign({}, prev.sigs || {});
  const live = rows.filter(r => r.state !== "gone"), sigs = {};
  let writes = 0, batch = db.batch(), inBatch = 0;
  const flush = async () => { if (inBatch) { await batch.commit(); batch = db.batch(); inBatch = 0; } };
  const moved = [];   // (the lines whose record changed, made first and written after the change marks below have read their stored copies)
  for (const r of rows) {
    const doc = { v: VERSION, key: r.key, receiptId: String(r.key).split("_")[0], order: r.order, line: r.line, spec: r.spec || null, problems: r.problems || [], state: r.state || "pulled", reason: r.reason || null,
      run: r._run || runFacts(r), etsyOpen: !!r.etsyOpen, inRun: !!r.inRun, fromRecord: !!r.fromRecord, runId: run ? run.runId : null };
    const data = J(doc), sig = sha(data); sigs[r.key] = sig;
    if (prevSigs[r.key] === sig) continue;
    moved.push({ r, doc, data, sig });
  }
  /* 9a · (CLOUDDRIVE build step 9, revalidate read from the world; DORMANT: only while the drive is standby or on) a line of the run whose order Etsy updated after it was
     read (its update time moved) and whose making changed (material, design, size, count) or whose customer's words changed: a change mark (Charm_World/changed), kept until
     the run's revalidate step has taken it (the line's changedAt), read by nothing else. The comparison is the page's own (bridge Orders.revalidate). */
  if (run && (await driveOn(db))) { try { await changeMarks(db, rows, moved, prevSigs, runLines, M, now); } catch (e) { errors.push("changes: " + String(e.message || e).slice(0, 160)); } }
  for (const { r, doc, data, sig } of moved) {
    // (the line is kept as the exact JSON text of what was read, beside the fields a query needs: Firestore refuses an empty field and an array inside an array, and the reader's spec may hold either)
    batch.set(db.collection(LINES).doc(r.key), { v: VERSION, key: r.key, receiptId: doc.receiptId, state: doc.state, runId: doc.runId, sig, at: now, gone: false, data }, { merge: false }); inBatch++; writes++;
    if (inBatch >= 400) await flush();
  }
  for (const k of Object.keys(prevSigs)) if (!(k in sigs)) { batch.set(db.collection(LINES).doc(k), { gone: true, goneAt: now, at: now, state: "gone" }, { merge: true }); inBatch++; writes++; if (inBatch >= 400) await flush(); }
  await flush();

  meterTag("world:engrave");
  /* 9b · the orders with engraving still to settle (Charm_Nest_Engrave: each line's engraving jobs as every computer saves them, charm-nest-engrave-sync.js), for the Design & Engrave badge:
     a job words / review / fitting / ready / classify, not archived, of a live line (charm-nest-bridge.js Engrave.pendingJobs) */
  let engraving = null, engraveKeys = null, engravePending = null, engraveJobs = null, engraveAt = null;
  try {
    // (each line's latest activity for the Design & Engrave list, the same on every computer: its order's Etsy times, its pieces' approvals and decisions; seconds or ms or text, as they come)
    const tms = v => { if (v == null || v === "") return 0; if (typeof v === "number") return v > 1e12 ? v : v > 1e9 ? v * 1000 : 0; const n = /^\d+(\.\d+)?$/.test(String(v)) ? tms(+v) : Date.parse(v); return Number.isFinite(n) && n > 0 ? n : 0; };
    const rowOf = new Map(live.map(r => [r.key, r]));
    const ENG_PENDING = new Set(["words", "review", "fitting", "ready", "classify"]), keys = live.filter(r => r.order && r.key).map(r => r.key), set = new Set(), all = [], pend = [];
    // (READCUT: the engraving records written since this instance's last build, by their own `at`, and the lines it did not have; else all of them)
    const enDocs = NL.engrave; let enAsk = keys;
    if (LC && LC.engrave) {
      try {
        const q = await db.collection("Charm_Nest_Engrave").where("at", ">", Math.max(0, LC.enAt - SLACK)).orderBy("at").limit(MOVED_MAX).get();
        if (q.size < MOVED_MAX) { const moved = new Map(q.docs.map(d => [d.id, d.data() || {}])); enAsk = []; for (const k of keys) { if (moved.has(k)) enDocs.set(k, moved.get(k)); else if (LC.engrave.has(k)) enDocs.set(k, LC.engrave.get(k)); else enAsk.push(k); } }
      } catch (e) { enAsk = keys; enDocs.clear(); }
    }
    for (let i = 0; i < enAsk.length; i += 300) for (const d of await db.getAll(...enAsk.slice(i, i + 300).map(k => db.collection("Charm_Nest_Engrave").doc(k)))) enDocs.set(d.id, d.exists ? d.data() || {} : null);
    for (const d of keys.map(k => ({ id: k, x: enDocs.get(k) })).filter(e => e.x).map(e => ({ id: e.id, exists: true, data: () => e.x }))) {
      if (!d.exists) continue; let rec = null; try { rec = JSON.parse((d.data() || {}).rec || "null"); } catch (_) { rec = null; }
      if (!rec || !Array.isArray(rec.jobs) || !rec.jobs.some(j => j && !j.archived)) continue;
      all.push(d.id);   // (CLOUDWORLD: the lines whose engraving work the cloud holds: the Design & Engrave tab of every computer lists these, and no line one computer prepared alone)
      // (each piece's state as the cloud holds it: a computer whose piece says otherwise takes the cloud's record of the line, charm-nest-cloud-world.js engraveCheck)
      (engraveJobs || (engraveJobs = {}))[d.id] = Object.fromEntries(rec.jobs.filter(j => j && j.key && !j.archived).map(j => [String(j.key), String(j.state || "") + (j.editingBack ? "!" : "")]));
      { const o = ((rowOf.get(d.id) || {}).order) || {}; (engraveAt || (engraveAt = {}))[d.id] = Math.max(tms(o.updateTs), tms(o.createTs), ...rec.jobs.filter(j => j && !j.archived).map(j => Math.max(tms(j.approvedAt), tms(j.decidedAt)))); }
      if (rec.jobs.some(j => j && ENG_PENDING.has(String(j.state)) && !j.archived && !j.editingBack)) { set.add(String(d.id).split("_")[0]); pend.push(d.id); }
    }
    engraving = [...set]; engraveKeys = all.sort(); engravePending = pend.sort();
  } catch (e) { errors.push("engraving: " + e.message); }

  meterTag("world:flow");
  /* 9c · THE FLOW (the left rail and the Orders tab's stages): where each line's pieces are, read as an order's own window reads them (op getOrderPieces: its pool rows and every saved
     sheet that lists them, one snapshot) for EVERY open order, then the page's rules. The reading is kept (Charm_World/place) while nothing a placement is made from moved (the cloud's
     placement counter, Charm_Nest_Rev/placement) and for FULL_MS at most; a line not in it, or whose pieces changed, is read afresh. Plus the steps after the laser every computer learned
     (Charm_Nest_Shared flowstages-production) and the Completed pile (completed-production). */
  let flowIn = null;
  if (!FlowStages || !OPieces || !PPlace) errors.push("flow: the page's rules did not load in the cloud (" + [!FlowStages && "flow-stages", !OPieces && "order-pieces", !PPlace && "piece-placement"].filter(Boolean).join(", ") + ")");
  else try {
    const sharedOf = async (base, shards) => { const out = {}; const docs = await db.getAll(...Array.from({ length: shards }, (_, i) => db.collection("Charm_Nest_Shared").doc(`${base}-${i}`))); for (const d of docs) { if (!d.exists) continue; const f = (d.data() || {}).f || {}; for (const [k, x] of Object.entries(f)) { if (!x || x.x) continue; let v = null; try { v = typeof x.j === "string" ? JSON.parse(x.j) : x.v; } catch (_) { v = null; } if (v != null) out[k] = v; } } return out; };
    const [learnedMap, completedMap] = await Promise.all([sharedOf("flowstages-production", 4), sharedOf("completed-production", 16)]);
    const genDoc = await db.collection("Charm_Nest_Rev").doc("placement").get(), gen = genDoc.exists ? Number((genDoc.data() || {}).n) || 0 : 0;
    const placeRef = db.collection(WORLD).doc("place"), pd = await placeRef.get(), prevPlace = pd.exists ? pd.data() || {} : {};
    let kept = {}; try { kept = prevPlace.data ? JSON.parse(prevPlace.data) : {}; } catch (_) { kept = {}; }
    const fresh = !opts.force && prevPlace.gen === gen && now - (+prevPlace.at || 0) < FULL_MS;
    const sigOf = r => [(r.poolIds || []).join(","), (r.spec && r.spec.quantity) || (r.line && r.line.quantity) || 1, r.state === "gone" ? 0 : 1].join("|");
    const byRid = new Map(); for (const r of live) if (r.order && r.key) { const rid = String(r.key).split("_")[0]; if (!byRid.has(rid)) byRid.set(rid, []); byRid.get(rid).push(r); }
    const need = [...byRid.keys()].filter(rid => !fresh || byRid.get(rid).some(r => !kept[r.key] || kept[r.key].g !== sigOf(r)));
    const facts = {}; for (const [rid, rs] of byRid) if (!need.includes(rid)) for (const r of rs) facts[r.key] = kept[r.key];
    const answers = new Map(), batches = []; for (let i = 0; i < need.length; i += 40) batches.push(need.slice(i, i + 40));
    await pool(batches, 3, async ids => { const a = await ops.getOrderPieces({ orderIds: ids }); if (!a || a.error || !a.orders) throw new Error((a && a.error) || "no answer"); for (const id of ids) answers.set(id, a.orders[id] || { pools: [], sheets: [] }); });
    // (a set marked laser done cuts every sheet it lists, as the page's replaceCutWhy reads the Library's sets)
    const setIds = [...new Set([...answers.values()].flatMap(a => (a.sheets || []).map(s => s.setId)).filter(Boolean).map(String))], setCutOf = new Map();
    for (let i = 0; i < setIds.length; i += 100) for (const d of await db.getAll(...setIds.slice(i, i + 100).map(id => db.collection("Charm_Nest_Sets").doc(id)), { fieldMask: ["laserDoneAt", "sheetIds"] })) if (d.exists) { const x = d.data() || {}; if (+x.laserDoneAt > 0) setCutOf.set(d.id, new Set((x.sheetIds || []).map(String))); }
    const setCut = (sheetId, setId) => !!(setId && setCutOf.has(String(setId)) && setCutOf.get(String(setId)).has(String(sheetId)));
    let read = 0;
    for (const rid of need) { if (!answers.has(rid)) continue; read++; const f = sheetFactsOf(rid, byRid.get(rid), answers.get(rid), M.entryFor, setCut); for (const r of byRid.get(rid)) { if (f[r.key]) { f[r.key].g = sigOf(r); facts[r.key] = f[r.key]; } } }
    if (need.length && read < need.length) errors.push(`flow: the pieces of ${need.length - read} order(s) were not read`);
    say(`[world] flow: ${byRid.size} orders, pieces read for ${read}${fresh ? " (the rest kept: nothing placed moved)" : ""}`);
    const keepData = J(facts);
    if (Buffer.byteLength(keepData) < 900000) await placeRef.set({ v: VERSION, at: fresh ? +prevPlace.at || now : now, gen, data: keepData });
    flowIn = { learnedMap, completedMap, facts };
  } catch (e) { errors.push("flow: " + String(e.message || e).slice(0, 160)); }

  meterTag("world:review");
  /* 10 · Review, built once, here */
  let review = null;
  if (ReviewCore && typeof ReviewCore.build === "function") {
    try {
      const customSent = {}; for (const [k, v] of Object.entries(Object.assign({}, customKept, customDone))) if (v && v.sent && typeof v.sent === "object") customSent[k] = v.sent;
      // (S2, Paul 21 Nov 2026: Review's Resolved decisions are the cloud's in mode "on" (_charmNestReviewSettled.js, written where Review.remove answers a decision), so every computer lists the
      //  same; in any other mode the list stays the carrying page's own, as before)
      let settled = [];
      if (ops.reviewSettledGet && await driveMode(db) === "on") { try { const g = await ops.reviewSettledGet({}); settled = (g && Array.isArray(g.settled) ? g.settled : []).slice(0, 500); } catch (e) { errors.push("settled: " + String(e.message || e).slice(0, 160)); } }
      review = ReviewCore.build({ now, lines: rows.map(coreLine), customDone, customKept, reviewMarks, customSheet: coreSends(customSheet), customSent, cancelled: [...cancelled], settled, engraving });
      // (the flow, with Review's Open and the engraving still to settle as the page's flowCtx reads them)
      let flow = null;
      if (flowIn) {
        try {
          // (the page's flowNow: the open orders' lines, not shipped (the Completed pile), not cancelled; its Review set: every line of an Open card that is no engraving and no hold card)
          const shipped = new Set(Object.keys(flowIn.completedMap || {}).map(String)), ridOf = r => String(r.key).split("_")[0];
          const flowRows = live.filter(r => r.order && r.state !== "gone" && !shipped.has(ridOf(r)) && !cancelled.has(ridOf(r)));
          const reviewLines = new Set((review.cards || []).filter(c => c && c.state === "Open" && !/^(eng|held):/.test(String(c.key || ""))).flatMap(c => c.lineKeys || []));
          flow = flowOf(flowRows, flowIn.facts, { cancelled, learnedMap: flowIn.learnedMap, completedMap: flowIn.completedMap, openOrders: new Set((review.openOrders || []).map(String)), engraving: new Set((engraving || []).map(String)), reviewLines });
        } catch (e) { errors.push("flow: " + String(e.message || e).slice(0, 160)); }
      }
      if (flow) {
        review.counts.flow = flow.counts;
        // (CNT1, Paul 9 Nov 2026: the top nav's Review and Design badges say the very count of the rail's row they sit on: the cloud's flow, the same on every computer)
        review.counts.reviewOrdersCore = review.counts.reviewOrders; review.counts.designOrdersCore = review.counts.designOrders;
        review.counts.reviewOrders = flow.counts.review || 0; review.counts.designOrders = flow.counts.design || 0;
      }
      if (engraveKeys) { review.counts.engraveLines = engravePending.length; review.counts.engraveOrders = (engraving || []).length; review.counts.engraveHeld = engraveKeys.length; }
      await db.collection(WORLD).doc("review").set({ v: VERSION, at: now, counts: JSON.parse(J(review.counts || {})), data: J({ cards: (review.cards || []).slice(0, 1500), openOrders: review.openOrders || [], openKeys: review.openKeys || [], resolvedKeys: review.resolvedKeys || [], itemKeys: review.itemKeys || [], flowByRid: flow ? flow.byRid : null, engraveKeys: engraveKeys || null, engravePending: engravePending || null, engraveJobs: engraveJobs || null, engraveAt: engraveAt || null }) });
    } catch (e) { errors.push("review: " + String(e.message || e).slice(0, 200)); }
  }

  meterTag("world:totals");
  /* 10b · the Library's totals (op ping's own count: saved sheets not archived, charms in the library), so the rail's "Library · N sheets · M charms" is the cloud's on every computer, kept current */
  let library = null;
  try { const [sc, cc] = await Promise.all([db.collection("Charm_Nest_Sheets").where("archived", "==", false).count().get(), db.collection("Charm_Nest_Library").count().get()]); library = { sheets: sc.data().count, charms: cc.data().count, at: now }; }
  catch (e) { errors.push("library: " + String(e.message || e).slice(0, 120)); }

  const states = live.reduce((m, r) => (m[r.state] = (m[r.state] || 0) + 1, m), {});
  const openKeys = live.map(r => r.key).sort();
  const summary = { v: VERSION, at: now, runId: run ? run.runId : null, lines: rows.length, live: live.length, states, receipts: rids.length, openOnEtsy: Object.keys(open).length,
    keysHash: sha(openKeys.join(",")), held: live.filter(r => r.state === "held").map(r => r.key).sort(), unmatched: live.filter(r => r.state === "unmatched").map(r => r.key).sort(),
    counts: review ? review.counts : null, writes, toRead: Math.max(0, stale.length - etsyCalls), etsyCalls, etsyToday: await etsyUsed(db), etsyCap: ETSY_DAY_CAP, mirrorAt, library, errors: errors.slice(0, 20), notes: notes.slice(-20), sigs,
    // (CLOUDDRIVE build step 9, P11 / E7: additive, pages ignore them) the mail system's sync this build read, and the receipts still to be read whole (the first 500)
    mirrorBefore, toReadIds: stale.filter(id => !readNow.has(id)).slice(0, 500) };
  await db.collection(WORLD).doc("summary").set(JSON.parse(J(summary)));
  await stateRef.set({ open, mirrorMark: watermark, sweptAt: st.sweptAt || null, lastBuildAt: now, checkedAt: now, token, toRead: Math.max(0, stale.length - etsyCalls), buildSeq: seq + 1 }, { merge: false });
  CACHE.etsy = { seq: seq + 1, docs: etsyDocs };
  CACHE.lines = Object.assign(NL, { seq: seq + 1 });   // (READCUT: the line records as this build read them, for this instance's next build)   // (READCUT: the next build reuses it only if it is this instance's and nothing built in between)
  if (etsyDocs.size > 6000) { const live = new Set(want); for (const k of [...etsyDocs.keys()]) if (!live.has(k)) etsyDocs.delete(k); }
  return Object.assign({}, summary, { sigs: undefined });
}

/* ── (CLOUDDRIVE build step 9) the change marks: what Etsy changed in an order of the run after its line was read (the page's Orders.revalidate, bridge, without the Etsy reads) ── */
/** The change between the stored line (prev) and the line as read now (r), when Etsy updated the order since (its update time moved): its making (material, design, size,
    count) or the customer's words. null: no change a line is made from (an address, a gift note, another line of the order). */
function changeOf(prev, r, entryFor, now) {
  const po = prev && prev.order, ps = prev && prev.spec, no = r.order, ns = r.spec;
  if (!po || !ps || !no || !ns || !(+no.updateTs > 0) || +po.updateTs === +no.updateTs) return null;
  const wordsKey = sp => { const c = sp && O.decodeWords ? O.decodeWords(Object.assign({}, sp)) : sp; return JSON.stringify([c && c.personalization, c && c.buyerMessage, c && c.staffNote]); };
  const words = wordsKey(ps) !== wordsKey(ns), material = ps.material !== ns.material, sku = !O.sameDesign(ps.designSku, ns.designSku, k => entryFor(k));
  const shape = material || sku || ps.size !== ns.size || ps.quantity !== ns.quantity;
  if (!shape && !words) return null;
  const rid = String(r.key).split("_")[0];
  return { at: now, ts: +no.updateTs, shape, words, why: `Etsy updated order ${rid} after the pull${material ? " · material changed" : ""}${sku ? " · SKU changed" : ""}${words ? " · the customer's words changed" : ""}` };
}
/** Charm_World/changed { marks: { lineKey: { at, ts, shape, words, why } }, ts: { lineKey: the order's update time as last read } }: written only when it changes. Only the
    run's lines with pieces are followed; a line whose update time moved is compared with its stored copy (read before this build writes it). A mark goes once the run's
    revalidate step took it (the line's changedAt) or the line left the run. */
async function changeMarks(db, rows, moved, prevSigs, runLines, M, now) {
  const ref = db.collection(WORLD).doc("changed"), snap = await ref.get(), was = snap.exists ? snap.data() || {} : {};
  const marks = Object.assign({}, was.marks || {}), ts = Object.assign({}, was.ts || {}), movedKeys = new Set(moved.map(x => x.r.key)), ask = [];
  for (const r of rows) {
    if (!r.inRun || !r.order || !r.spec || r.fromRecord || !(r.poolIds || []).length) continue;
    const t = +r.order.updateTs || 0, before = +ts[r.key] || 0;
    if (t && before && t !== before && prevSigs[r.key] && movedKeys.has(r.key)) ask.push(r);
    if (t) ts[r.key] = t;
  }
  for (let i = 0; i < ask.length; i += 300) {
    const part = ask.slice(i, i + 300), docs = await db.getAll(...part.map(r => db.collection(LINES).doc(r.key)));
    docs.forEach((d, j) => { const prev = d.exists ? lineOf(d.data()) : null, m = prev && !prev.gone ? changeOf(prev, part[j], M.entryFor, now) : null; if (m) marks[part[j].key] = m; });
  }
  for (const [k, m] of Object.entries(marks)) { const l = runLines[k]; if (!l || ["gone", "committed", "skipped"].includes(String(l.state || "")) || +l.changedAt >= +m.at) delete marks[k]; }
  for (const k of Object.keys(ts)) if (!runLines[k]) delete ts[k];
  if (J(marks) !== J(was.marks || {}) || J(ts) !== J(was.ts || {})) await ref.set({ v: VERSION, at: now, marks, ts });
  return marks;
}

/** A stored line as a page reads it: the JSON text opened, the query fields beside it. */
function lineOf(d) { if (!d) return null; if (d.gone) return { key: d.key, gone: true, at: d.at }; let x = {}; try { x = JSON.parse(d.data || "{}"); } catch (_) { x = {}; } return Object.assign(x, { key: d.key, sig: d.sig, at: d.at }); }
function reviewOf(d) { if (!d) return null; let x = {}; try { x = JSON.parse(d.data || "{}"); } catch (_) { x = {}; } return Object.assign({ v: d.v, at: d.at, counts: d.counts || {} }, x); }

/* ── READNOW (Paul, 23 Nov 2026: "You have a budget of 30 sec to make the update ... This should be a simple process"): a Change SKU waited for the next reading of EVERY open order, a minute or more.
   The lines named here are read again at once, and only them: each line's order as it was last read (no Etsy, no mail), with the saved links, options and the charm repository as they are NOW, by the very
   reading the build makes (readLine, afterRead). Written where the build writes them (Charm_World_Lines), so whoever makes the pieces reads it; the next build writes the same. Whether one line's choices mean
   what they do is also told by the listing's other lines (seenSkus): the stored lines of the same listing are read for it. ── */
/** The library's records as masterList gives them, read side by side by the first letter of their SKU; null unless every record its signature counts was read. */
const MASTER_AT = [null].concat("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ".split(""), ["["]);
async function masterNow(db, index) {
  const n = index && Number.isFinite(+index.count) ? +index.count : -1; if (n < 0) return null;
  const Master = require("./_charmNestMaster"), ID = require("./firebaseAdmin").firestore.FieldPath.documentId(), out = []; let docs = 0;
  const parts = await Promise.all(MASTER_AT.map((lo, i) => { let q = db.collection(Master.INDEX); if (lo != null) q = q.where(ID, ">=", lo); const hi = MASTER_AT[i + 1]; if (hi != null) q = q.where(ID, "<", hi); return q.get(); }));
  for (const s of parts) for (const d of s.docs) { docs++; const e = d.data(), r = e && e.sku ? Master.slimEntry(e) : null; if (r) out.push(r); }
  return docs === n ? out.sort((x, y) => x.sku.localeCompare(y.sku)) : null;
}
/* PERSONFIRST (Paul, 23 Nov 2026): a process that has just started read all 9,900 library records again (seconds of every person's request). The library as it was last read is kept as one file with its signature (the
   records' count and newest change): the reading of all orders writes it when the library moved, and so does the first request that finds it older. A file of another signature is never used. */
const MASTER_FILE = "charmnest/cache/master-slim.json.gz";
const bucketOf = o => { if (o && o.bucket) return o.bucket; try { return require("./firebaseAdmin").storage().bucket(); } catch (_) { return null; } };
async function masterKept(bucket, sig) { try { if (!bucket || !sig || sig === "null") return null; const [buf] = await bucket.file(MASTER_FILE).download(), d = JSON.parse(require("zlib").gunzipSync(buf).toString("utf8")); return d && d.v === 1 && d.sig === sig && Array.isArray(d.entries) && d.entries.length ? d.entries : null; } catch (_) { return null; } }
async function keepMaster(bucket, sig, entries) { try { if (!bucket || !sig || sig === "null" || !(entries || []).length) return; await bucket.file(MASTER_FILE).save(require("zlib").gzipSync(Buffer.from(JSON.stringify({ v: 1, sig, entries }), "utf8")), { contentType: "application/gzip", resumable: false, metadata: { cacheControl: "no-store" } }); } catch (_) { /* read from the records next time */ } }
async function readNow(o) {
  const { db, ops } = o, keys = [...new Set((o.keys || []).map(String))].slice(0, 80), runLines = o.lines || {}, now = Date.now(), out = new Map(); if (!keys.length) return out;
  const docs = new Map(); for (let i = 0; i < keys.length; i += 300) for (const d of await db.getAll(...keys.slice(i, i + 300).map(k => db.collection(LINES).doc(k)))) if (d.exists) docs.set(d.id, d.data() || {});
  const rows = []; for (const k of keys) { const d = docs.get(k); if (!d || d.gone) continue; let x = null; try { x = JSON.parse(d.data || "{}"); } catch (_) { x = null; } if (!x || !x.order || !x.line) continue; const facts = runFacts(runLines[k]); rows.push({ d, x, row: Object.assign({ key: k, order: x.order, line: x.line, state: "pulled", reason: null, poolIds: [] }, facts, { etsyOpen: !!x.etsyOpen, inRun: !!runLines[k], _run: facts }) }); }
  if (!rows.length) return out;
  const lids = [...new Set(rows.map(r => String(r.row.line.listingId || "")).filter(Boolean))];
  // (CMDFAST: the library's signature alone first, then, only when it moved, the whole library side by side; it was read 500 records at a time, 20 reads one after another, seconds of every Change SKU)
  // (PERSONFIRST: every input in one call, read side by side (readMany): they were seven calls one after another; the custom records of these lines only, never all)
  const rowKeys = rows.map(r => r.row.key), all = ops.many ? await ops.many([{ op: "masterList", limit: 1 }, { op: "optionMapGet" }, { op: "aliasGet" }, { op: "noDesignGet" }, { op: "customGet", keys: rowKeys }, { op: "listingSkus", listingIds: lids, cacheOnly: true }, { op: "customSheetGet", lineKeys: rowKeys }]).catch(() => null) : null;
  const many = Array.isArray(all) && all.length === 7 && all.every(a => a && !a.error) ? all : null;
  const entriesNow = async () => { const sg = many ? many[0] : await ops.masterList({ limit: 1 }), sig = J(sg.index || null); if (CACHE.masterEntries && CACHE.masterSig === sig && sig !== "null") return CACHE.masterEntries;
    const bucket = bucketOf(o), kept = await masterKept(bucket, sig); if (kept) { CACHE.masterSig = sig; CACHE.masterEntries = kept; return kept; }
    const quick = await masterNow(db, sg.index).catch(() => null); if (quick) { CACHE.masterSig = sig; CACHE.masterEntries = quick; keepMaster(bucket, sig, quick); return quick; }
    const first = await ops.masterList({ limit: 3000 }), list = (first.entries || []).slice(); let cursor = first.next; for (let i = 0; cursor && i < 20; i++) { const r = await ops.masterList({ limit: 3000, cursor }); list.push(...(r.entries || [])); cursor = r.next; } CACHE.masterSig = sig; CACHE.masterEntries = list; return list; };
  const [entries, om, al, nd, cl, ls, cs, reads] = await Promise.all([entriesNow(), many ? many[1] : ops.optionMapGet({}), many ? many[2] : ops.aliasGet({}), many ? many[3] : ops.noDesignGet({}), many ? many[4] : ops.customGet({}), many ? many[5] : ops.listingSkus({ listingIds: lids, cacheOnly: true }), many ? many[6] : ops.customSheetGet({ lineKeys: rowKeys }), db.getAll(...rows.map(r => db.collection("Charm_Nest_CustomRead").doc(r.row.key)))]);
  const M = CACHE.index && CACHE.indexOf === entries ? CACHE.index : (CACHE.indexOf = entries, CACHE.index = masterIndex(entries));
  const maps = { optionMaps: (om && om.maps) || {}, aliases: (al && al.aliases) || {}, noDesign: (nd && nd.list) || { patterns: [], skus: [], rows: [] } }; try { O.cleanMaps(maps.aliases, maps.noDesign); } catch (_) { /* as read */ }
  const customDone = {}; for (const [k, v] of Object.entries((cl && cl.records) || {})) if (v && v.kind !== "reviewMark" && v.state !== "open") customDone[k] = v;
  const byKey = new Map(rows.map(r => [r.row.key, r.row])), customRead = {}, customDecided = {};
  for (const d of reads) { if (!d.exists) continue; const x = d.data() || {}, r = byKey.get(d.id), rd = x.reads || {}, h = r && r.crh && rd[r.crh] ? r.crh : x.latest; if (h && rd[h]) customRead[d.id] = rd[h]; if (x.decided && x.decided.kind) customDecided[d.id] = x.decided; }
  const idx = new Map(); for (const e of Object.values((cs && cs.records) || {})) if (e && (e.sent || e.sendIntent)) for (const k of Object.keys((e.sent || e.sendIntent).lines || {})) idx.set(k, e);
  const sentOf = row => { const e = idx.get(row.key); if (!e) return null; const list = (((e.sent || e.sendIntent).lines || {})[row.key] || []).filter(pc => !(pc && pc.removed)), pc = list[0], F = pc && (e.files || []).find(f => f.id === pc.f); return { metal: (F && F.metal) || null, pieces: list.length, sent: !!e.sent, backs: list.filter(q => { const G = q && (e.files || []).find(x => x.id === q.f); return !!G && Array.isArray(G.charmBacks) && G.charmBacks.includes(Number.isInteger(q.i) ? q.i : 0); }).length }; };
  const ctx = { optionMaps: maps.optionMaps, aliases: maps.aliases, noDesign: maps.noDesign, customDone, customRead, customDecided, listingSkus: Object.assign({}, (ls && (ls.tables || ls.listings)) || {}), seen: O.seenSkus ? O.seenSkus((o.seenLines || []).concat(rows.map(r => r.row.line))) : {},
    masterEntry: M.entryFor, masterLoose: M.looseFor, masterShared: M.sharedFor, masterLike: M.likeFor, masterSkus: () => [...M.entries.keys()] };
  const batch = db.batch(); let n = 0;
  for (const { d, x, row } of rows) {
    row.spec = readLine(row, ctx); afterRead(row, null, sentOf(row));
    const doc = Object.assign({}, x, { v: VERSION, key: row.key, receiptId: String(row.key).split("_")[0], order: row.order, line: row.line, spec: row.spec || null, problems: row.problems || [], state: row.state || "pulled", reason: row.reason || null, run: row._run });
    const data = J(doc); out.set(row.key, { spec: row.spec, problems: row.problems || [], state: row.state });
    if (data && data !== d.data) { batch.set(db.collection(LINES).doc(row.key), Object.assign({}, d, { state: doc.state, sig: sha(data), at: now, gone: false, data }), { merge: false }); n++; }
  }
  if (n) await batch.commit();
  return out;
}

/** RELNOW (23 Nov 2026, item 23: a page opened seconds after a Release Hold still showed the order ON HOLD, until the next reading of all orders, up to a minute later): what the run holds of these lines NOW is put on
 *  their stored reading; nothing else of it is read again (the next build reads it whole). lines: { lineKey: the run's line }. → how many lines were put right */
async function runNow(o) {
  const { db } = o, L = o.lines || {}; let n = 0;
  for (const k of Object.keys(L).slice(0, 80)) n += await db.runTransaction(async tx => {
    const ref = db.collection(LINES).doc(k), s = await tx.get(ref), d = s.exists ? s.data() || {} : null; if (!d || d.gone) return 0;
    let x = null; try { x = JSON.parse(d.data || "{}"); } catch (_) { x = null; } if (!x || !x.order || !x.line) return 0;
    const run = runFacts(L[k]), kinds = run.releaseAnyway && Array.isArray(run.releaseAnyway.kinds) ? run.releaseAnyway.kinds : [], was = String(run.state || "pulled"), state = x.spec && x.spec.noDesign && was === "pulled" ? "noDesign" : was;
    const data = J(Object.assign({}, x, { run, state, reason: run.reason || null, problems: (Array.isArray(x.problems) ? x.problems : []).filter(p => !(p && kinds.includes(p.kind))) })); if (data === d.data) return 0;
    tx.set(ref, Object.assign({}, d, { state, sig: sha(data), at: Date.now(), gone: false, data }), { merge: false });
    return 1;
  });
  return n;
}

module.exports = { readNow, runNow, masterNow, flowOf, sheetFactsOf, placeOfLine, build, lineOf, reviewOf, masterIndex, readLine, afterRead, runFacts, isOpenReceipt, LINES, WORLD, ETSY };
