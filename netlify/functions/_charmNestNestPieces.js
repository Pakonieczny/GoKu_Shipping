/*  netlify/functions/_charmNestNestPieces.js — a sheet record's pieces made again in node, with their silhouettes, for the cloud's nest (cloud plan v2, build step 6).
 *
 *  A sheet record keeps what each piece IS (its pool id, its design's file, its hash, its side, its size by hand) but not its silhouette: a page made that from the design
 *  file when it pooled the piece, and kept it in memory. The cloud's nest needs the same bitmap the page would hand its solver. This is the page's own way of putting a
 *  run's sheets back (charm-nest-bridge.js restoreRunSheets: Pool.masterCharm, Pool.ensureBase, Pool.cloneCharm, CharmNestArtEdit.piece), run in node through the ONE
 *  piece maker the page uses (charm-nest-pool-make.js create(host)) with a host of the cloud's own:
 *    · the library's entries are read from Charm_Master_Index (_charmNestMaster.js slimEntry), never through a page;
 *    · every design file and every artwork file is read from our own bucket by its path (bucket.file().download()): a link the record keeps is read back to its path, and no
 *      URL is ever fetched (no network, no Etsy: E4);
 *    · the silhouette is the page's buildSilhouettes, drawn by the node canvas (_charmNestPdf.js withCanvas, _charmNestCanvas.js): at most one edge pixel (1/6 pt, 0.059 mm)
 *      from the page's (the owner's 0.06 mm), held there by tests/charm-nest/cloud-silhouette-parity.cjs;
 *    · a piece's own artwork edit (Charm_Pool_Art) is put over its master as the page puts it (charm-nest-art-edit.js piece), read from Firestore;
 *    · the host says it is the cloud's (cloud: true: the pool's retry rules never take an Etsy stop as passing, m:E2) and paces itself (C7: a turn of the event loop
 *      between pieces, and a time budget).
 *  create({ db, bucket, S, log, budgetMs, beat }) → { rebuild(rec, rows) → { charms, missing } }: each live charm of the record, made again with bits, w, h, scale, areaPt2,
 *  widthPt, heightPt and hash, and the fields the nest reads (order, orderDate, frontAt, poolId, orderInfo, the pair fields, manual, frozen, chunk). A piece that cannot be
 *  made (no pool row, no design, a body gone from the design) is listed in `missing` with the reason, in words.                                                     */
"use strict";
const O = require("../../charm-nest-orders.js");
const PoolMake = require("../../charm-nest-pool-make.js");
const PoolPieces = require("../../charm-nest-pool-pieces.js");
const Pair = require("../../charm-nest-pair.js");
const Master = require("./_charmNestMaster");

const ART = "Charm_Pool_Art", MC_MIN = 0.8, MC_MAX = 1.2;
const num = v => (Number.isFinite(+v) ? +v : 0);
const ms = v => (v && typeof v.toMillis === "function" ? v.toMillis() : num(v));
/** A piece's manual mark from its record (the sheet window's CNManual.fromRecord, charm-nest-sheetwin.js): { by, at, size?, fit? } | null. */
function manualOf(rc) {
  const m = rc && rc.manual; if (!m || typeof m !== "object") return null;
  const size = +m.size;
  return Object.assign({ by: String(m.by || "").slice(0, 80), at: num(m.at) }, size >= MC_MIN && size <= MC_MAX && size !== 1 ? { size } : {}, m.fit === "inplace" ? { fit: "inplace" } : {});
}
/** The bucket path a link names: "bucket:<path>" (this host's own), a Firebase download link (…/o/<path>?…) or a storage.googleapis.com link; null for anything else. */
function storagePath(url) {
  const s = String(url || "");
  if (s.startsWith("bucket:")) return s.slice(7) || null;
  let m = /^https:\/\/firebasestorage\.googleapis\.com\/v0\/b\/[^/]+\/o\/([^?#]+)/.exec(s); if (m) return decodeURIComponent(m[1]);
  m = /^https:\/\/storage\.googleapis\.com\/[^/]+\/([^?#]+)/.exec(s); if (m) return decodeURIComponent(m[1]);
  return null;
}
/** Of the charms a design can give, the one whose size is the record's (a piece whose stored hash, made on a page, is not the one node draws: the closest outline box within 3 %). */
function byShape(cands, rc) {
  const w = num(rc && rc.widthPt), h = num(rc && rc.heightPt); if (!(w > 0 && h > 0)) return null;
  let best = null, err = Infinity;
  for (const c of cands || []) { if (!c) continue; const e = Math.max(Math.abs(num(c.widthPt) - w) / w, Math.abs(num(c.heightPt) - h) / h); if (e < err) { err = e; best = c; } }
  return err <= 0.03 ? best : null;
}

function create(o) {
  const { db, bucket, S } = o, log = o.log || (() => {});
  const P = require("./_charmNestPdf").withCanvas().CharmNestPDF;
  const ArtEdit = require("../../charm-nest-art-edit.js");
  const entries = new Map(), rows = new Map();
  const bytes = async url => {
    const p = storagePath(url); if (!p) throw new Error("a file the cloud cannot read from its own storage: " + String(url).slice(0, 80));
    const [buf] = await bucket.file(p).download(); return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  };
  async function fetchEntry(sku) {
    const k = String(sku || "").trim().toUpperCase(); if (!k || !Master.isSku(k)) return null;
    if (entries.has(k)) return entries.get(k);
    const s = await db.collection(Master.INDEX).doc(k).get(), e = s.exists ? Master.slimEntry(s.data()) : null;
    entries.set(k, e); return e;
  }
  let skuPat = null;
  const MasterHost = {
    entryFor: sku => entries.get(String(sku || "").trim().toUpperCase()) || null,
    fetchEntry,
    keepOutOf: (c, face) => ((c && c.members) || []).filter(m => m.layer && (face === "front" ? /front\s*keep-?out/i : /back\s*keep-?out/i).test(m.layer)),
    skuRegex: () => skuPat || (skuPat = (() => { try { return new RegExp(S.settings.skuPattern); } catch (_) { return P.SKU_PATTERN_DEFAULT; } })())
  };
  /* the artwork edits of a piece (charmNestLibrary op_artList, op_artPrepoolList), read from Firestore; a picture's paths are read from our bucket by the path its record keeps */
  const artApi = async (fn, body) => {
    if (fn !== "charmNestLibrary" || !body) throw new Error("the cloud's artwork reader reads only the artwork records");
    if (body.op === "artList") {
      const keys = (Array.isArray(body.keys) ? body.keys : []).map(String).slice(0, 400), arts = {};
      for (let i = 0; i < keys.length; i += 100) (await db.getAll(...keys.slice(i, i + 100).map(k => db.collection(ART).doc(k)))).forEach((s, j) => {
        if (!s.exists) return; const r = Object.assign({}, s.data() || {}); r.updatedAt = ms(r.updatedAt);
        if (r.blob && r.blob.path) r.blob = Object.assign({}, r.blob, { url: "bucket:" + r.blob.path });
        arts[keys[i + j]] = r;
      });
      return { arts };
    }
    if (body.op === "artPrepoolList") { const s = await db.collection(ART).doc("_prepool").get(), d = s.exists ? s.data() || {} : {}; return { ids: d.ids && typeof d.ids === "object" ? d.ids : {} }; }
    throw new Error("the cloud's artwork reader reads only the artwork records");
  };
  ArtEdit.setHost({ api: artApi, bytes, rowOf: id => rows.get(String(id)) || null });
  const pace = PoolMake.pace({ budgetMs: o.budgetMs || 8 * 60000 });
  const B = { pool: { rows, sources: new Map() }, master: { entries } };
  const St = { settings: S.settings, poolSources: {}, cloud: { ok: true } };
  const Make = PoolMake.create({
    window: { CharmNestPair: Pair }, S: St, B, O, P, MM: 25.4 / 72, Master: MasterHost, PoolPieces,
    agent: (scope, kind, text) => log(`[nest pieces] ${kind}: ${text}`),
    api: async (fn, body) => { if (fn === "charmNestOutput" && body && body.op === "url" && body.path) return { url: /^https?:/.test(String(body.path)) ? String(body.path) : "bucket:" + body.path }; throw new Error("the cloud's piece maker reads only its own storage"); },
    stockFor: () => null, labelOf: m => m, now: () => Date.now(),
    CharmNestAssets: () => ({ bytes }), Orders: () => null, Review: () => null, CustomPrint: () => null, CustomSheet: () => null, ArtEdit: () => ArtEdit,
    keptMaster: async () => null, keepMaster: () => {}, pinPooled: () => {}, charmOf: () => null, greenHeld: () => false, cancelled: () => false,
    cloud: true, pace
  });

  /** One charm of the record, made again (restoreRunSheets, per placement; here for every live piece of the sheet, placed or still to place). */
  async function charmOf(rc, pool, rec) {
    if (!rc.poolId) throw new Error("a piece that is no order's (dropped on a page by hand) is nested on a computer");
    if (!pool) throw new Error(`its piece record ${rc.poolId} is not in the cloud`);
    // (a custom design is in no master file: it is made from its own file, the one its sheet was cut from: the record's sources)
    let own = pool.custom || rc.custom ? PoolPieces.ownSource(rec, rc, pool) : null;
    const sku = String(pool.sku || rc.sku || "").trim().toUpperCase();
    const entry = own ? null : await fetchEntry(sku);
    if (!own && !entry && (pool.custom || rc.custom) && pool.aiPath) own = { path: String(pool.aiPath).replace(/#fit-.*$/, "") };   // (OWNFILE, Paul 23 Nov 2026: "why would you stop a sheet from being fully filled just because there's a custom order?": SS Draft 11 placed nothing from the evening of 22 Nov on: order 4241324766's custom drawing was not in the sheet's own list of files (empty), so it was "not in the library", and every nest of the sheet was thrown away. A custom piece's record names its drawing: it is made from that)
    if (!entry && !(own && (own.path || own.url))) throw new Error(`its design ${sku || "(no SKU)"} is not in the library`);
    const ofLib = () => (entry ? Make.masterCharm(entry, pool.size || null) : Make.masterCharm({ sku: sku || "CUSTOM", aiPath: own.path || own.url, aiUrl: null }, null));
    const src = !own && rc.aiPath ? await Make.masterCharm({ sku, aiPath: rc.aiPath, aiUrl: null, charmHash: rc.hash, upAngle: entry && entry.upAngle, facing: entry && entry.facing, facings: entry && entry.facings }, null).catch(e => { if (!entry || (e && e.budget)) throw e; return ofLib(); }) : await ofLib();   // (ONEDRAWING: nested from the file the record names, as _charmNestFinishHost.js writes it)
    // a piece of a pair is cut from its own body, turned over when it is the mirror image (the pool row says which; the record too)
    const piece = PoolPieces.cleanFields(pool).side ? pool : PoolPieces.cleanFields(rc).side ? rc : null;
    let base = null;
    if (!piece && pool.oneBody === true && Number.isInteger(+pool.bodyIndex)) { const solo = await Make.soloBodies(src).catch(() => null); base = (solo && solo[+pool.bodyIndex]) || null; }   // (a cloud row says which body alone it is cut from)
    let baseWhy = null;   // (BODYWHY: the maker's own words when it cannot tell the body, kept for the refusal below)
    if (!base) base = await Make.ensureBase(src, piece, rc.hash ? rc : pool).catch(e => { if (e && e.budget) throw e; baseWhy = e; return null; });   // (a spent time budget is the slot's to take up again, never "no such body")
    // (a piece cut from ONE body of a design drawn twice, whose hash a page drew: node's hash differs by an edge pixel, so the body is found by its size)
    if (base && !piece && base === src.charms[0]) { const solo = (await Make.soloBodies(src).catch(() => null)) || []; if (solo.length) { const m = byShape([src.charms[0], ...solo], rc); if (m) base = m; } }
    if (!base) throw new Error(piece ? `its ${piece.side === "L" ? "left" : "right"} body is no longer in the design` : (baseWhy && baseWhy.message ? String(baseWhy.message).slice(0, 180) : "its design has no such body"));
    let c = Make.cloneCharm(base, String(rc.id));
    if (ArtEdit && typeof ArtEdit.piece === "function") c = await ArtEdit.piece(c, String(rc.poolId), pool);   // (ART EDIT: the piece's own artwork, when its pool row names one)
    c = Object.assign({}, c, { id: String(rc.id), name: rc.name || c.name || sku, sku: rc.sku || pool.sku || c.sku || null, order: pool.orderId != null ? String(pool.orderId) : String(rc.order || rc.id),
      orderDate: num(pool.orderDate), arrivedAt: num(pool.arrivedAt), poolId: String(rc.poolId), metal: rec.metal, lineKey: pool.lineKey || null, excluded: false, pinned: null,
      orderInfo: { receiptId: pool.orderId, transactionId: pool.transactionId, sku: pool.sku, copy: pool.copy, quantity: pool.quantity, form: pool.form, size: pool.size } });
    delete c.frontAt; if (num(pool.frontAt) > 0) c.frontAt = num(pool.frontAt);   // (an order released from hold goes first: CharmNestOrders.rankDate)
    if (piece) Object.assign(c, PoolPieces.fieldsOf(piece));
    delete c.manual; const man = manualOf(rc); if (man) c.manual = man;
    if (rc.frozen) c.frozen = rc.frozen; else delete c.frozen;
    if (rc.chunk || pool.chunk) c.chunk = rc.chunk || pool.chunk;
    if (own) c.custom = true;
    if (!(c.bits && c.w > 0 && c.h > 0 && c.bits.length === c.w * c.h)) throw new Error("its silhouette could not be drawn");
    return c;
  }
  /** Every live piece of the record, made again. `poolRows`: Map poolId → row (the whole row). `beat`: awaited between pieces (the slot's heartbeat). */
  async function rebuild(rec, poolRows, beat) {
    for (const [id, r] of poolRows || []) rows.set(String(id), r);
    const charms = [], missing = [];
    for (const rc of rec.charms || []) {
      if (!rc || rc.excluded) continue;
      try { charms.push(await charmOf(rc, rows.get(String(rc.poolId)), rec)); }
      catch (e) { if (e && e.budget) throw e; missing.push({ id: String(rc.id), poolId: rc.poolId || null, why: String((e && e.message) || e).slice(0, 200) }); }
      await pace(); if (beat) await beat();
    }
    return { charms, missing };
  }
  return { rebuild, charmOf, fetchEntry };
}

module.exports = { create, manualOf, storagePath, byShape };
