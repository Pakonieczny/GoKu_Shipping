/*  netlify/functions/_charmNestPieceHost.js — THE PIECE MAKER'S HOST IN THE CLOUD (cloud plan v2, build steps 3 and 4; Paul, 20 Nov 2026: no production work runs on any computer).
 *  charm-nest-pool-make.js makes an order line's pieces (pool rows and their charms) from ONE source on the page and here. The page hands it its own state; this file hands it the same
 *  things read from the cloud, so the cloud makes the very pieces a page made:
 *    S          the shop's shared settings (Charm_Nest_Shared/sorter-settings over the page's DEFAULTS, charm-nest-1.html) and its plate preset (sorter-stock), with stockFor
 *    B          the pool rows of the lines in hand (Charm_Pool, read by the caller), the designs read in this pass (B.pool.sources), the library records asked (B.master.entries)
 *    Master     the library's records read straight from Charm_Master_Index (never through an op that adds a download link), keepOutOf and the SKU pattern, as the page's Master
 *    a design   read from Storage itself (bucket.file(path).download()): never through a URL, never through charmNestOutput's door, never from Etsy
 *    P          charm-nest-pdf.js with the page's canvas for node (_charmNestPdf.js withCanvas(), _charmNestCanvas.js), loaded the first time a design is drawn
 *    the rest   the page's own helpers ported as they are: pinPooled (bridge Pool.pinPooled, less its "Order changed" notice: that is Review's, never the intake's), releaseAway
 *               (bridge Orders.releaseAway), a piece already on a sheet (charmOf: the sheet records of the run), a green-line wait (greenHeld), a cancelled order
 *    cloud      true: an Etsy stop or a 429 never passes by itself here, each row says which body it is cut from (bodyIndex, oneBody), and a body is never told by a hash (BODYID)
 *    bodyOf     a record a page made says no body: the line's own reading says it (its one body, or the body its listing's profile cuts for that copy), never a silhouette's hash
 *  Nothing here calls Etsy, a listing SKU table, the network or an op of the library: it reads Firestore and Storage only.                                                          */
"use strict";
const O = require("../../charm-nest-orders.js");
const PoolPieces = require("../../charm-nest-pool-pieces.js");
const Pair = require("../../charm-nest-pair.js");
const PoolMake = require("../../charm-nest-pool-make.js");
const Master = require("./_charmNestMaster");
let ReviewCore = null; try { ReviewCore = require("../../charm-nest-review-core.js"); } catch (_) { ReviewCore = null; }

const PT_PER_IN = 72, MM = 25.4 / 72;
/* charm-nest-1.html METALS (label): what the piece maker says of a metal */
const METAL_LABEL = { gold: "GF 14/20", silver: "SS", rose: "RG 14/20", gold10k: "10K Gold", gold14k: "14K Gold" };
/* charm-nest-1.html DEFAULTS: the fields the piece maker and the pull rule read (a field the shop's shared settings hold wins, as on every page) */
const DEFAULTS = Object.freeze({
  stock: { gold: [100 / 25.4, 50 / 25.4], silver: [100 / 25.4, 50 / 25.4], rose: [100 / 25.4, 50 / 25.4], gold10k: [100 / 25.4, 50 / 25.4], gold14k: [100 / 25.4, 50 / 25.4] },
  insetPt: 1.5, maxFill: 0.80, minPt: 6, silhouetteRes: 6, pullMode: "all", pullDueBy: "", pullCount: 40, nospread: undefined, skuPattern: "^[A-Z0-9][A-Z0-9 _.,'&()+\\-]{1,60}$"
});
/* charm-nest-1.html STOCK_PRESETS / DEFAULT_STOCK / stockFor: the plate a metal is cut on */
const STOCK_PRESETS = [{ id: "A", w: 100 / 25.4, h: 50 / 25.4 }, { id: "B", w: 100 / 25.4, h: 100 / 25.4 }, { id: "custom", w: 0, h: 0 }], DEFAULT_STOCK = "A";
/* charm-nest-cloud-share.js SETTINGS_HERE: what stays on each computer and is never shared (runMode lives on the drive document, Charm_Nest_Open/drive) */
const SETTINGS_HERE = new Set(["v", "sandbox", "sandboxSeed", "sandboxStream", "sandboxSpeed", "computePipeline", "parallel", "serverAbove", "masterServerAbove", "rail", "orderView", "notify", "sound", "dsOrigin",
  "heartbeatS", "heartbeatMiss", "runMode", "pollOrders", "pollMinutes", "autoEvery", "etsyHourlyCap", "nestFlow", "learnedBudgetS", "learnedStallS"]);
const SHARE = "Charm_Nest_Shared";

const ms = v => (v && v.toMillis ? v.toMillis() : +v || 0);
/** A shared store's fields as values (charmNestLibrary sharePut: f[field] = { j: JSON text, at, dev, by } or { x: 1 } when removed), from the store's `f` as a document or op shareGet
    gives it. Every cloud reader of the shop's shared settings reads them through this (a field record is never a setting's value). */
function sharedValues(f) {
  const out = {};
  for (const [k, v] of Object.entries(f && typeof f === "object" ? f : {})) { if (!v || v.x || typeof v.j !== "string") continue; try { out[k] = JSON.parse(v.j); } catch (_) { /* a field that does not read is left at its default */ } }
  return out;
}
/** A shared store's fields, from its document. */
function sharedFields(snap) { return sharedValues(snap && snap.exists ? (snap.data() || {}).f : null); }
/** The shop's settings and plate preset as every page reads them: { settings, stockPreset, read: { settings, stock } } (read: whether each store was there). */
async function readSettings(db) {
  const [s, k] = await db.getAll(db.collection(SHARE).doc("sorter-settings"), db.collection(SHARE).doc("sorter-stock"));
  const shared = sharedFields(s), settings = JSON.parse(JSON.stringify(DEFAULTS));
  for (const [key, v] of Object.entries(shared)) if (!SETTINGS_HERE.has(key) && key !== "stock") settings[key] = v;
  settings.stock = Object.assign({}, DEFAULTS.stock, shared.stock && typeof shared.stock === "object" && !Array.isArray(shared.stock) ? shared.stock : {});
  const pf = sharedFields(k), stockPreset = STOCK_PRESETS.some(p => p.id === pf.preset) ? pf.preset : DEFAULT_STOCK;
  return { settings, stockPreset, read: { settings: !!(s && s.exists), stock: !!(k && k.exists) } };
}
/** charm-nest-1.html stockFor(metal): the plate of a metal ({ wIn, hIn, wPt, hPt }). */
function stockFor(settings, stockPreset, metal) {
  const p = STOCK_PRESETS.find(x => x.id === stockPreset);
  const inches = p && p.id !== "custom" && !["gold10k", "gold14k", "rose"].includes(metal) ? [p.w, p.h] : ((settings.stock || {})[metal] || DEFAULTS.stock[metal]);
  if (!Array.isArray(inches) || !(+inches[0] > 0) || !(+inches[1] > 0)) throw new Error(`no plate size is known for the metal ${metal == null ? "(none)" : metal}`);
  return { wIn: +inches[0], hIn: +inches[1], wPt: +inches[0] * PT_PER_IN, hPt: +inches[1] * PT_PER_IN };
}

/* ── the library's records: Charm_Master_Index read straight (slimEntry, plus updatedAt, which trySig reads), every download link taken off, so a design is read from Storage itself ── */
const upper = s => String(s == null ? "" : s).trim().toUpperCase();
function bare(d) {
  const e = Master.slimEntry(d); if (!e) return null;
  e.updatedAt = ms(d.updatedAt) || null; e.aiUrl = null; e.thumbUrl = null;
  if (e.sizes && typeof e.sizes === "object") { const z = {}; for (const [k, v] of Object.entries(e.sizes)) z[k] = v && typeof v === "object" ? Object.assign({}, v, { aiUrl: null, thumbUrl: null }) : v; e.sizes = z; }
  return e;
}
/** The library's records of these SKUs: Map(SKU → record) of those that exist. */
async function readEntries(db, skus) {
  const want = [...new Set((skus || []).map(upper).filter(k => k && Master.isSku(k)))], out = new Map();
  for (let i = 0; i < want.length; i += 100) {
    const docs = await db.getAll(...want.slice(i, i + 100).map(k => db.collection(Master.INDEX).doc(k)));
    for (const d of docs) if (d.exists) { const e = bare(d.data() || {}); if (e) out.set(d.id, e); }
  }
  return out;
}
/** How many records the library holds (the page's B.master.entries.size, which trySig reads for a line whose SKU is in no master file: it is tried again when the library grows). */
async function libraryCount(db) { try { return (await db.collection(Master.INDEX).count().get()).data().count || 0; } catch (_) { return 0; } }

/* ── the designs already read and traced by this instance (the page's keptMaster / keepMaster): by path, under the drawing's hash, the last 24 ── */
const KEPT = new Map(), KEPT_MAX = 24;
const keptMaster = (path, hash) => { const k = String(path || ""), s = KEPT.get(k); if (!s) return null; if (hash && String(s.hash || "") !== String(hash)) return null; KEPT.delete(k); KEPT.set(k, s); s.usedAt = Date.now(); return s; };
const keepMaster = (path, src) => { const k = String(path || ""); if (!k || !src) return; KEPT.delete(k); KEPT.set(k, src); while (KEPT.size > KEPT_MAX) KEPT.delete(KEPT.keys().next().value); };

let PDF = null;
/** charm-nest-pdf.js with the page's canvas for node, loaded once, the first time a design is drawn (nothing of it loads for a pass that makes no piece). */
function pdf() { if (!PDF) PDF = require("./_charmNestPdf").withCanvas().CharmNestPDF; return PDF; }

/** The design file at a Storage path, as bytes (Uint8Array): read from the bucket itself. Only "gs:<path>" is a design's address here: no URL is ever fetched. */
async function bytesAt(bucket, url) {
  const u = String(url || "");
  if (!u.startsWith("gs:")) throw new Error("the cloud reads a design only from its own Storage, never through a link");
  const path = u.slice(3).replace(/^\/+/, "");
  if (!/^charmnest\//.test(path)) throw new Error(`the cloud reads a design only from the library's own folder (charmnest/), not ${path.slice(0, 80)}`);
  const [buf] = await bucket.file(path).download();
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
}

/** The Map of library records the maker reads, whose size is the library's count (the page's B.master.entries.size) rather than how many were asked. */
class Library extends Map { constructor(count) { super(); this.count = +count || 0; } get size() { return Math.max(this.count, super.size); } }

/**
 * host(o) → { make, S, B, Master, entries, readSkus(skus), specOf } — the piece maker for one pass of the cloud.
 *   o: { db, bucket (Storage; else the project's own bucket), settings, stockPreset (readSettings), rows (Map poolId → pool row of the lines in hand), listed (Set of pool ids a live
 *        sheet record of the run lists), sheets (Map sheetId → sheet record of the run), cancelled (Set of receipt ids), specs (Map lineKey → the line's reading), libCount,
 *        budgetMs (the pace: 45 s), log }
 */
function host(o) {
  const db = o.db, log = typeof o.log === "function" ? o.log : () => {};
  let bucket = o.bucket || null;
  const theBucket = () => bucket || (bucket = require("./firebaseAdmin").storage().bucket());
  const settings = o.settings || JSON.parse(JSON.stringify(DEFAULTS)), stockPreset = o.stockPreset || DEFAULT_STOCK;
  const entries = new Library(o.libCount);
  const S = { settings, poolSources: {}, cloud: { ok: true } };
  const B = { pool: { rows: o.rows instanceof Map ? o.rows : new Map(), sources: new Map() }, master: { entries }, run: o.run || null, maps: { customRead: o.customRead || {} } };
  const listed = o.listed instanceof Set ? o.listed : new Set(), sheets = o.sheets instanceof Map ? o.sheets : new Map(), cancelled = o.cancelled instanceof Set ? o.cancelled : new Set();
  const specs = o.specs instanceof Map ? o.specs : new Map();
  const missing = new Set();
  /* CUSTOMSEND (21 Nov 2026): a line sent from its card (o.sends: lineKey → its Charm_Custom_Sheet record) is made from the card's own designs, as the page's CustomSheet.prepare */
  const custom = o.sends instanceof Map ? require("./_charmNestCustomMake").create({ sends: o.sends, pdf, bytes: p => bytesAt(theBucket(), "gs:" + String(p || "")), settings, stockFor: m => stockFor(settings, stockPreset, m) }) : null;
  const M = {
    entryFor: sku => entries.get(upper(sku)) || null,
    /* the page's fetchEntry: a SKU no master file can hold is no design (not a failed read); one asked once and missing is not asked again in this pass */
    fetchEntry: async sku => {
      const k = upper(sku); if (!k) return null;
      const have = entries.get(k); if (have) return have;
      if (!/^[A-Z0-9][A-Z0-9 _.,'&()+\-]{1,60}$/.test(k) || missing.has(k)) return null;
      const got = await readEntries(db, [k]); const e = got.get(k) || null;
      if (e) entries.set(k, e); else missing.add(k);
      return e;
    },
    keepOutOf: (c, face) => ((c && c.members) || []).filter(m => m.layer && (face === "front" ? /front\s*keep-?out/i : /back\s*keep-?out/i).test(m.layer)),
    skuRegex: () => { try { return new RegExp(settings.skuPattern || DEFAULTS.skuPattern); } catch (_) { return pdf().SKU_PATTERN_DEFAULT; } }
  };
  const Orders = { releaseAway };
  const Review = { problemText: p => (ReviewCore && typeof ReviewCore.problemText === "function" ? ReviewCore.problemText(p) : String((p && (p.reason || p.kind)) || "a question")) };
  /** bridge Orders.releaseAway: why a line released anyway has nothing to place, or "" */
  function releaseAway(row) {
    const sp = (row && row.spec) || {}; if (!row || sp.noDesign || sp.ownDesign || sp.customDone) return "";
    const ps = [].concat(sp.problems || [], row.problems || []).filter(p => p && p.kind), one = k => ps.find(p => p.kind === k);
    const un = one("unmatchedSku"); if (un) return un.formHold && un.reason ? String(un.reason) : `SKU ${un.sku || sp.designSku || "?"} has no design in any master file`;
    if (ps.some(p => p.kind === "blockedSku" && p.noOutline)) return "its design has no outline to cut";
    if (one("needsMaterial") && !row.materialOverride) return "its metal is not read, so it cannot be put on a sheet";
    const msz = one("missingSize"); if (msz) return `${msz.sku || "its design"} has no size ${msz.size || "(none)"}`;
    if (one("oversize")) return "it does not fit the plate";
    if (!ps.length) return "";
    if (!sp.designSku) return "it has no design";
    try { if (entries.size && !M.entryFor(sp.designSku)) return `${sp.designSku} has no design in any master file`; } catch (_) { /* the maker asks */ }
    return "";
  }
  const live = r => !!r && !["abandoned", "superseded"].includes(r.state);
  /** bridge Pool.pinPooled: a line already pooled keeps the pieces it was pooled with (the count its rows were made as, and the group they say they are). BODYID: whether a live piece of a
   *  line read as ONE body was cut whole is what the cloud's row says (oneBody false); a page's row that says nothing is checked against the hashes the LIBRARY recorded for the whole design
   *  (Chrome's own, never one the cloud drew). The "Order changed" notice (noticeSet) is Review's: the intake never raises it. */
  function pinPooled(row) {
    const sp = row.spec; if (!sp) return;
    const lv = (row.poolIds || []).map(id => B.pool.rows.get(id)).filter(live);
    if (lv.length) { const g = Math.max(0, ...lv.map(r => +r.groupSize || 0)); if (g > 1) row.groupSize = g; else delete row.groupSize; }
    if (sp.pair && sp.pair.oneBody && lv.length) {
      const said = lv.map(r => PoolPieces.cleanBody(r)).filter(x => typeof x.oneBody === "boolean");
      if (said.length) { if (said.some(x => !x.oneBody)) O.solo(sp, null); }
      else {
        const e = M.entryFor(sp.designSku), g = e ? maker.sizeEntry(e, sp.size) : null, whole = new Set(maker.entryHashes(e).concat(g && g.charmHash ? [String(g.charmHash)] : []));
        if (whole.size && lv.some(r => whole.has(String(r.charmHash)))) O.solo(sp, null);
      }
    }
    if (row.repoolChanged || row.changePending) return;
    const had = Math.max(0, ...(row.poolIds || []).map(id => { const r = B.pool.rows.get(id) || {}; return ["abandoned", "superseded"].includes(r.state) ? 0 : +r.quantity || 0; }));
    const off = (row.poolIds || []).every(id => { const r = B.pool.rows.get(id); return !!r && ["abandoned", "superseded"].includes(r.state); });
    if (had) O.pinPieces(sp, had); else if (sp.pieceRule && off) O.pinPieces(sp, sp.pieceRule);
  }
  /** RGC1 (bridge Pool.greenHeld): a line waiting for an open layout's green line is not made up again until that line is cut. Here the layout is the sheet record the wait names. */
  function greenHeld(row) {
    if (!(row && row.state === "waiting" && row.wait && row.wait.kind === "green")) return false;
    const s = row.wait.sheetId ? sheets.get(String(row.wait.sheetId)) : null;
    return !!s && !s.archived && !(+s.roseCutAt > 0) && !(+s.laserDoneAt > 0);
  }
  /** BODYID: which body a record a page made (it says none) is cut from, from the line's own reading: its listing's profile (the body of each copy), else the one body it reads as, else the whole design. */
  function bodyOf(rec) {
    if (!rec || typeof rec !== "object") return null;
    const key = rec.lineKey ? String(rec.lineKey) : (() => { const p = PoolPieces.parsePoolId(rec.poolId); return p ? p.lineKey : ""; })(); if (!key) return null;
    const sp = specs.get(key); if (!sp) return null;
    if (rec.side === "L" || rec.side === "R") return null;   // (an ear says its body with its side: ensureBase)
    const copy = +rec.copy || (PoolPieces.parsePoolId(rec.poolId) || {}).copy || 0;
    const per = O.pieceBodiesOf ? O.pieceBodiesOf(sp) : null;
    if (Array.isArray(per) && per.length && copy >= 1 && Number.isInteger(per[copy - 1])) return { oneBody: true, bodyIndex: per[copy - 1] };
    const ob = sp.pair && sp.pair.oneBody; if (ob && Number.isInteger(ob.index) && ob.index >= 0) return { oneBody: true, bodyIndex: ob.index };
    return { oneBody: false };
  }
  const H = {
    window: Object.assign({ CharmNestPair: Pair }, custom ? { CustomSheet: true } : {}),
    get S() { return S; }, get B() { return B; }, get O() { return O; }, get P() { return pdf(); }, get MM() { return MM; }, get Master() { return M; }, get PoolPieces() { return PoolPieces; },
    agent: (_where, kind, text) => { try { log(`[pieces] ${kind}: ${String(text).slice(0, 300)}`); } catch (_) { /* a note only */ } return null; },
    api: async (fn, body) => { if (fn === "charmNestOutput" && body && body.op === "url" && body.path) return { url: "gs:" + String(body.path) }; throw new Error("the cloud's piece maker calls no function: " + String(fn)); },
    stockFor: m => stockFor(settings, stockPreset, m), labelOf: m => METAL_LABEL[m] || m, now: () => Date.now(),
    CharmNestAssets: () => ({ bytes: url => bytesAt(theBucket(), url) }), Orders: () => Orders, Review: () => Review, CustomPrint: () => null, CustomSheet: () => custom, ArtEdit: () => undefined,
    keptMaster, keepMaster, pinPooled, charmOf: id => (listed.has(String(id)) ? { poolId: String(id) } : null), greenHeld, cancelled: rid => cancelled.has(String(rid)),
    cloud: true, pace: typeof o.pace === "function" ? o.pace : PoolMake.pace({ budgetMs: o.budgetMs || 45000 }), bodyOf
  };
  const maker = PoolMake.create(H);
  /** The library's records of these SKUs read now, together (the maker asks one at a time otherwise). */
  async function readSkus(skus) { const want = [...new Set((skus || []).map(upper))].filter(k => k && !entries.has(k) && !missing.has(k)); if (!want.length) return 0; const got = await readEntries(db, want); for (const [k, e] of got) entries.set(k, e); for (const k of want) if (!got.has(k)) missing.add(k); return got.size; }
  return { make: maker, S, B, H, Master: M, entries, readSkus, specs, releaseAway, pinPooled, greenHeld, bodyOf, stock: m => stockFor(settings, stockPreset, m) };
}

module.exports = { host, readSettings, readEntries, libraryCount, stockFor, sharedFields, sharedValues, DEFAULTS, STOCK_PRESETS, DEFAULT_STOCK, SETTINGS_HERE, METAL_LABEL };
