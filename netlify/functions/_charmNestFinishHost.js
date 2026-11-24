/*  netlify/functions/_charmNestFinishHost.js — what the cloud's one sheet writer (charmNestSheet-background.js, _charmNestFinish.js; cloud plan v2 build step 7a) needs of
 *  "where it runs" that a page keeps in its own memory: the nesting settings, a sheet's stock, a metal's name and the sheet's pieces made again from the library's files.
 *  Nothing here writes anything. It reads Firestore and Storage directly (reads only), never through a page global, never the network, never Etsy:
 *    settings(db)            the shared Settings (Charm_Nest_Shared/sorter-settings and sorter-stock, written by every page's charm-nest-cloud-share.js) on top of the page's
 *                            DEFAULTS (charm-nest-1.html :4080; the values the writer reads are copied below and must move with them)
 *    stockFor(metal, sh, S)  the page's stockFor (charm-nest-1.html :4258): a saved sheet's own size first (its Rose Gold / 10K / 14K stock, the size its record was written at),
 *                            then the shop's plate (sorter-stock), then the metal's own size from Settings
 *    labelOf, METAL_TAG, cornerPtSetting, cornerSheet, insetFor, inflatedArea, shapeSignature   the page's helpers of the same names (charm-nest-1.html), on records
 *    makePieces(env)         the sheet's pieces made again from the library's design files, as the page's Recall.rebuildOnce makes them (charm-nest-bridge.js :28531), with the
 *                            page's own piece maker (charm-nest-pool-make.js) and its silhouettes drawn by the node canvas (_charmNestPdf.js withCanvas, _charmNestCanvas.js).
 *                            A piece keeps the id, name, order and hash its record holds (a node-made hash is never written over the browser's: design risk "Piece hash");
 *                            its own artwork edit is put over it (charm-nest-art-edit.js piece, read through the cloud's artList).                                         */
"use strict";
const { CharmNestPDF: P, withCanvas } = require("./_charmNestPdf");
const O = require("../../charm-nest-orders.js");
const PoolMake = require("../../charm-nest-pool-make.js");
const PoolPieces = require("../../charm-nest-pool-pieces.js");
const Pair = require("../../charm-nest-pair.js");
const MasterIndex = require("./_charmNestMaster");

const PT_PER_IN = 72, MM = 25.4 / 72;
/* The page's DEFAULTS (charm-nest-1.html :4080), the values the sheet writer reads. */
const DEFAULTS = {
  stock: { gold: [100 / 25.4, 50 / 25.4], silver: [100 / 25.4, 50 / 25.4], rose: [100 / 25.4, 50 / 25.4], gold10k: [100 / 25.4, 50 / 25.4], gold14k: [100 / 25.4, 50 / 25.4] },
  clearancePt: -0.5, insetPt: 1.5, cornerPt: 1.5 * 72 / 25.4, angleStep: 10, budgetS: 180, seed: 1, minPt: 6, verifyRes: 6, silhouetteRes: 6, maxFill: 0.80, nestFlow: "standard"
};
/* The page's STOCK_PRESETS (charm-nest-1.html :4094): the plate in inches. */
const STOCK_PRESETS = { A: [100 / 25.4, 50 / 25.4], B: [100 / 25.4, 100 / 25.4] }, DEFAULT_STOCK = "A";
const METAL_LABEL = { gold: "GF 14/20", silver: "SS", rose: "RG 14/20", gold10k: "10K Gold", gold14k: "14K Gold" };
const METAL_TAG = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
const NO_CORNER_METALS = ["gold10k", "gold14k"];
const labelOf = m => METAL_LABEL[m] || String(m || "");

/** One shared store's fields ({ f: { key: { j: JSON } | { x: 1 } } }, op_sharePut) as plain values. */
function shareFields(d) {
  const out = {}, f = (d && d.f) || {};
  for (const [k, v] of Object.entries(f)) { if (!v || v.x || typeof v.j !== "string") continue; try { out[k] = JSON.parse(v.j); } catch (_) { /* a field that does not read is left out */ } }
  return out;
}
/** The writer's Settings: the page's DEFAULTS with the shared ones over them, and the shop's plate. → { settings, stockPreset } */
async function settings(db) {
  let shared = {}, preset = DEFAULT_STOCK;
  try {
    const [a, b] = await db.getAll(db.collection("Charm_Nest_Shared").doc("sorter-settings"), db.collection("Charm_Nest_Shared").doc("sorter-stock"));
    if (a && a.exists) shared = shareFields(a.data());
    if (b && b.exists) { const p = shareFields(b.data()).preset; if (typeof p === "string" && p) preset = p; }
  } catch (_) { /* the defaults, as a page that never read the cloud's */ }
  const out = Object.assign({}, DEFAULTS, shared);
  out.stock = Object.assign({}, DEFAULTS.stock, shared.stock && typeof shared.stock === "object" ? shared.stock : {});
  return { settings: out, stockPreset: preset };
}
/** The page's stockFor on a record-made sheet (S: { settings, stockPreset }). */
function stockFor(metal, sh, S) {
  const saved = (sh && (sh.roseStock || sh.recordStock || sh.keptStock || sh.newStock)) || null;
  if (saved && saved.wPt && saved.hPt) return { wPt: +saved.wPt, hPt: +saved.hPt, wIn: saved.wPt / 72, hIn: saved.hPt / 72 };
  const p = STOCK_PRESETS[S && S.stockPreset];
  const inches = p && !["gold10k", "gold14k", "rose"].includes(metal) ? p : ((S && S.settings && S.settings.stock && S.settings.stock[metal]) || DEFAULTS.stock[metal] || DEFAULTS.stock.gold);
  return { wIn: inches[0], hIn: inches[1], wPt: inches[0] * PT_PER_IN, hPt: inches[1] * PT_PER_IN };
}
function cornerPtSetting(sh, S) { const metal = typeof sh === "string" ? sh : sh && sh.metal; if (metal && NO_CORNER_METALS.includes(metal)) return 0; const v = S && S.settings ? S.settings.cornerPt : null; return v == null || v === "" || !Number.isFinite(+v) ? DEFAULTS.cornerPt : Math.max(0, +v); }
function cornerSheet(sh, st, S) {
  let remnant = null;
  try { remnant = sh && sh.roseStock && sh.roseStock.profileJson ? JSON.parse(sh.roseStock.profileJson) : null; } catch (_) { remnant = null; }
  return Object.assign({ wPt: +st.wPt, hPt: +st.hPt, cornerPt: cornerPtSetting(sh, S) }, remnant && Math.abs(remnant.wPt - st.wPt) < .01 && Math.abs(remnant.hPt - st.hPt) < .01 ? { remnant } : {});
}
/** (the page's handPlacedSheet: a 10K / 14K sheet with a piece a person placed by hand keeps no inset) */
const handPlacedSheet = sh => !!sh && ["gold10k", "gold14k"].includes(sh.metal) && (sh.charms || []).some(c => c && c.manual && c.pinned && !c.arrivalPin);
const insetFor = (sh, S) => (handPlacedSheet(sh) ? 0 : +(S && S.settings && S.settings.insetPt) || 0);
/** (the page's inflatedArea and shapeSignature, charm-nest-1.html :5348 and :5724) */
const inflatedArea = (c, S) => { const g = (+(S && S.settings && S.settings.clearancePt) || 0) / 2; return c.areaPt2 + g * 2 * (c.widthPt + c.heightPt) + Math.PI * g * g; };
function shapeSignature(items, S) {
  const a = items.map(c => inflatedArea(c, S)); const n = a.length; if (!n) return { count: 0, cv: 0, largestFrac: 0 };
  const mean = a.reduce((s, v) => s + v, 0) / n, sd = Math.sqrt(a.reduce((s, v) => s + (v - mean) ** 2, 0) / n);
  return { count: n, cv: mean ? sd / mean : 0, largestFrac: Math.max(...a) / a.reduce((s, v) => s + v, 0) };
}

/* ── Storage, read in-process (never through the page's door, charmNestOutput.js; never a fetch of a link) ── */
const STORE = "cloud-storage:";
/** The Storage path a link names: our own "cloud-storage:<path>", or a Firebase download link (…/o/<path>?alt=media&token=…). null for anything else (no foreign link is ever read). */
function pathOfLink(url) {
  const s = String(url || "");
  if (s.startsWith(STORE)) return s.slice(STORE.length);
  const m = /^https:\/\/firebasestorage\.googleapis\.com\/v0\/b\/[^/]+\/o\/([^?#]+)/.exec(s);
  if (m) { try { return decodeURIComponent(m[1]); } catch (_) { return null; } }
  if (/^charmnest\//.test(s)) return s;
  return null;
}
async function bytesOf(bucket, url) {
  const path = pathOfLink(url);
  if (!path || !/^charmnest\//.test(path)) throw new Error(`the cloud reads design files from its own storage only: ${String(url).slice(0, 120)}`);
  const [buf] = await bucket.file(path).download();
  return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
}

/* ── the pieces ── */
/** The record's placements, each with the record's charm it was written for (charm-nest-pool-pieces.js placedCharms, the page's placedOfRecord). */
const placedOfRecord = rec => (PoolPieces.placedCharms ? PoolPieces.placedCharms(rec) : ((rec && rec.placements) || []).map(p => ({ p, c: ((rec && rec.charms) || []).find(c => c.id === p.id) || null })));
/** A saved charm's hand mark (the page's CNManual.fromRecord, charm-nest-sheetwin.js :6933). */
function manualOf(rc) {
  const m = rc && rc.manual; if (!m || typeof m !== "object") return null;
  const size = +m.size;
  return Object.assign({ by: String(m.by || "").slice(0, 80), at: +m.at || 0 }, size >= 0.8 && size <= 1.2 && size !== 1 ? { size } : {}, m.fit === "inplace" ? { fit: "inplace" } : {});
}
const AE = /~ae[0-9a-z]+$/;
const CHARM_SCALE = 0.971;   // (charm-nest-nest-plan.js CHARM_SCALE, the page's CHARM_SCALE)

/**
 * The sheet's pieces made again from the library (the page's Recall.rebuildOnce, on the cloud). env: { db, bucket, rec (the sheet record), S ({ settings, stockPreset }),
 *   read(body) (a cloud read: Lib.call for artList), log(text), pace?() }. → { charms: [charm], sources: { [sourceId]: src }, missing: [words] }
 * Each charm keeps its record's id, name, order and hash; it carries the silhouette (bits, w, h, scale, areaPt2) node drew for the checks and the members the file is written from.
 */
async function makePieces(env) {
  withCanvas({ backend: process.env.CHARM_NEST_CANVAS || "auto" });
  const { db, bucket, rec, S } = env, say = env.log || (() => {});
  const SS = { settings: S.settings, poolSources: {}, cloud: { ok: true } };
  const BB = { pool: { rows: new Map(), sources: new Map() }, master: { entries: new Map() }, run: null, orders: { byKey: new Map() } };
  const entries = new Map();
  const Master = {
    entryFor: sku => entries.get(String(sku || "").trim().toUpperCase()) || null,
    fetchEntry: async sku => {
      const k = String(sku || "").trim().toUpperCase(); if (!MasterIndex.isSku(k)) return null;
      if (entries.has(k)) return entries.get(k);
      const s = await db.collection(MasterIndex.INDEX).doc(k).get(), e = s.exists ? MasterIndex.slimEntry(s.data()) : null;
      entries.set(k, e); return e;
    },
    keepOutOf: () => null, skuRegex: () => P.SKU_PATTERN_DEFAULT
  };
  const maker = PoolMake.create({
    window: { CharmNestPair: Pair }, S: SS, B: BB, O, P, MM, Master, PoolPieces,
    agent: (who, kind, text) => say(`[pieces] ${kind}: ${text}`),
    api: async (fn, body) => { if (fn === "charmNestOutput" && body && body.op === "url") return { ok: true, url: STORE + String(body.path || "") }; throw new Error(`the cloud's piece maker does not call ${fn} ${body && body.op}`); },
    stockFor: m => stockFor(m, null, S), labelOf, now: () => Date.now(),
    CharmNestAssets: () => ({ bytes: url => bytesOf(bucket, url) }),
    keptMaster: async () => null, keepMaster: () => {}, charmOf: () => null, pinPooled: () => {}, greenHeld: () => false, cancelled: () => false,
    Orders: () => null, Review: () => null, CustomPrint: () => null, CustomSheet: () => null, ArtEdit: () => null, cloud: true, pace: env.pace
  });
  // the piece's own artwork edit (charm-nest-art-edit.js piece): read through the cloud's own read of Charm_Pool_Art (artList); a piece with no pool row is never asked here
  const ArtEdit = require("../../charm-nest-art-edit.js");
  ArtEdit.setHost({
    api: async (fn, body) => {
      if (fn === "charmNestLibrary" && body && body.op === "artList" && typeof env.read === "function") return env.read({ op: "artList", keys: body.keys });
      if (fn === "charmNestLibrary" && body && body.op === "artPrepoolList") return { ids: {} };
      throw new Error(`the cloud's piece maker does not call ${fn} ${body && body.op}`);
    },
    bytes: url => bytesOf(bucket, url)
  });
  // every piece of the record: each placed one with its placement (DUPID: the one its placement was written for), then the ones still waiting their turn on the sheet (no placement)
  const placed = placedOfRecord(rec), named = new Set(placed.map(x => x.c).filter(Boolean));
  for (const c of rec.charms || []) if (c && !c.excluded && !named.has(c) && !placed.some(x => x.p && String(x.p.id) === String(c.id))) placed.push({ p: null, c });
  // the pool rows of the record's pieces, read once
  const ids = [...new Set(placed.map(x => x.c && x.c.poolId).filter(Boolean).map(String))];
  for (let i = 0; i < ids.length; i += 100) {
    const snaps = await db.getAll(...ids.slice(i, i + 100).map(id => db.collection("Charm_Pool").doc(id)));
    for (const s of snaps) if (s.exists) BB.pool.rows.set(s.id, Object.assign({ poolId: s.id }, s.data()));
  }
  /* ONEBAD (Paul, 23 Nov 2026): a piece that waits on the sheet (no place yet) and cannot be made never stops the sheet from being written: it keeps waiting, said by name (waiting), and every
     other piece is placed. Only a piece that HAS a place and cannot be made stops the write (missing): writing without it would cut the sheet without it. The record's own list of pieces is never
     written here, so nothing leaves the sheet. */
  const charms = [], missing = [], waiting = [];
  for (const { p, c: rc } of placed) {
    if (env.pace) await env.pace();
    const miss = t => (p ? missing : waiting).push(t);
    if (!rc) { miss(`placement ${p && p.id} names no piece of the sheet`); continue; }
    if (rc.excluded) continue;
    if (!rc.poolId) { miss(`${rc.name || rc.id} is no order's piece (a file dropped on a card): the cloud cannot make it again`); continue; }
    // (TAKENRELOAD: a piece taken off the sheet leaves its list of pieces at once while its place is still in the layout: it is not drawn back)
    if (rec.dirty === true && Array.isArray(rec.poolIds) && !rec.poolIds.map(String).includes(String(rc.poolId))) continue;
    const pool = BB.pool.rows.get(String(rc.poolId));
    if (!pool) { miss(`${rc.poolId}: its piece record is not in the cloud`); continue; }
    try {
      let own = pool.custom || rc.custom ? PoolPieces.ownSource(rec, rc, pool) : null;
      const entry = own ? null : await Master.fetchEntry(pool.sku);
      if (!own && !entry && (pool.custom || rc.custom) && pool.aiPath) own = { path: String(pool.aiPath).replace(/#fit-.*$/, "") };   // (OWNFILE: _charmNestNestPieces.js)
      if (!entry && !(own && (own.url || own.path))) { miss(`${pool.orderId || rc.poolId} · ${pool.sku || rc.sku}: no design in the library`); continue; }
      /* ONEDRAWING (Paul, 23 Nov 2026, GF Sheet 1 of Nov 18: its pandas, made at 7.4 mm, were written at 11 mm, the hoop's size, over their neighbours): a sheet's piece is drawn from the file its record names, as it was made and
         nested, never made up again from the library and a size. The library's entry only when the record names no file. */
      const ofLib = () => (entry ? maker.masterCharm(entry, pool.size) : maker.masterCharm({ sku: rc.sku || pool.sku, aiPath: own.path || own.url, aiUrl: own.url }, null));
      const src = !own && rc.aiPath ? await maker.masterCharm({ sku: rc.sku || pool.sku, aiPath: rc.aiPath, aiUrl: null, charmHash: rc.hash, upAngle: entry && entry.upAngle, facing: entry && entry.facing, facings: entry && entry.facings }, null).catch(e => { if (!entry) throw e; return ofLib(); }) : await ofLib();
      const piece = PoolPieces.cleanFields(pool).side ? pool : PoolPieces.cleanFields(rc).side ? rc : null;
      const base = await maker.ensureBase(src, piece, rc.hash ? rc : pool).catch(() => null);
      if (!base) { miss(`${pool.orderId || rc.poolId} · ${pool.sku}: its ${piece && piece.side === "L" ? "left" : "right"} body is no longer in the design`); continue; }
      // (the page's rebuildOnce: the piece's charm, its own artwork edit over it, then the record's and the pool row's facts; it is held where its record placed it)
      const charm = await ArtEdit.piece(maker.cloneCharm(base, String(rc.id)), String(rc.poolId), pool);
      const ae = AE.exec(String(charm.hash || "")), keptHash = String(rc.hash || "").replace(AE, "");
      Object.assign(charm, {
        id: String(rc.id), name: rc.name || charm.name, order: pool.orderId || rc.order || charm.order, orderDate: +pool.orderDate || 0, arrivedAt: +pool.arrivedAt || 0, poolId: String(rc.poolId), metal: rec.metal,
        lineKey: pool.lineKey, orderInfo: { receiptId: pool.orderId, transactionId: pool.transactionId, sku: pool.sku, copy: pool.copy, quantity: pool.quantity, form: pool.form, size: pool.size },
        sku: rc.sku || pool.sku || charm.sku, hash: keptHash ? keptHash + (ae ? ae[0] : "") : charm.hash, layer: rc.layer || null, qty: rc.qty || 1,
        pinned: p ? { cxPt: +p.cxPt, cyPt: +p.cyPt, angle: +p.angle || 0 } : null
      });
      if (piece) Object.assign(charm, PoolPieces.fieldsOf(piece));
      for (const k of ["frozen", "chunk"]) if (rc[k] !== undefined && rc[k] !== null) charm[k] = rc[k];
      if (own) charm.custom = true;
      const man = manualOf(rc); if (man) { charm.manual = man; if (man.size && charm.pinned) charm.pinned.size = man.size; }
      // (OWNSCALE: a piece saved a little smaller than the standard with no size mark of its own takes its size from its placement, so the checks measure it as it lies)
      if (p) { const ratio = Number.isFinite(+p.scale) && +p.scale > 0 ? +(+p.scale / CHARM_SCALE).toFixed(5) : 1; if (!(man && man.size) && Math.abs(ratio - 1) > 0.0005 && ratio >= 0.8 && ratio <= 1.2) { charm.manual = Object.assign({ by: "", at: 0 }, charm.manual || {}, { size: ratio, fit: "inplace" }); charm.pinned.size = ratio; } }
      charms.push(charm);
    } catch (e) { miss(`${pool.orderId || rc.poolId} · ${pool.sku || rc.sku}: ${String((e && e.message) || e).slice(0, 200)}`); }
  }
  return { charms, sources: SS.poolSources, missing, waiting };
}

module.exports = { DEFAULTS, STOCK_PRESETS, METAL_LABEL, METAL_TAG, CHARM_SCALE, labelOf, settings, shareFields, stockFor, cornerPtSetting, cornerSheet, handPlacedSheet, insetFor, inflatedArea, shapeSignature,
  pathOfLink, bytesOf, placedOfRecord, manualOf, makePieces, STORE };
