/*  netlify/functions/_charmEngravePiece.js — the charm of ONE piece, made in the cloud the way a page makes it (cloud plan v2, build step 11: the fit and the back file of a piece
 *  need its exact outline, holes, keep-outs, the parts a person added to it, and its mirror image for a Right earring).
 *
 *  The page holds a piece's charm on its sheet in memory (Pool.charmOf); a sheet record keeps only the piece's numbers. So the cloud makes it again from the records, with the page's own
 *  code and in the page's own order (charm-nest-bridge.js restoreRunSheets, the take-over rebuild: TAKEOVERSAFE2):
 *    1. the design: the library's index entry of the pool row's SKU (Charm_Master_Index, read only), or, for a custom piece, the source its sheet record names;
 *    2. CharmNestPoolMake's masterCharm (read, parsed, grouped, traced with the page's buildSilhouettes through _charmNestPdf.withCanvas) and ensureBase (the body of a mismatched pair,
 *       its mirror image for a Right ear, the one body a solo piece is cut from), then cloneCharm under the piece's id;
 *    3. the piece's own artwork edit (Charm_Pool_Art, read through artList; charm-nest-art-edit.js piece) when its pool row says it has one;
 *    4. CharmNestEngraveSides.pieceCharm: the charm as THIS piece is cut (never mirrored twice).
 *  Design files are read with bucket.file().download(), never through charmNestOutput's door. Nothing here writes anything, and nothing reaches Etsy.
 *
 *    create({ db, read, bucket, settings, log }) → { charmOf(poolId, o), poolRows(ids), entryOf(sku), sourceOf(charm), sidesCtx(rows) }
 *      charmOf(poolId, { pool, sheet, slot, side }) → { charm, src, entry, pool, rc } or throws ({ engravingPending } while the piece is not on a sheet or its design cannot be read yet)
 *      read: the cloud's read call (Lib.call for a READ op: artList)                                                                                                                  */
"use strict";
const Pdf = require("./_charmNestPdf");
const MasterIdx = require("./_charmNestMaster");
const O = require("../../charm-nest-orders.js");
const Pair = require("../../charm-nest-pair.js");
const PoolPieces = require("../../charm-nest-pool-pieces.js");
const PoolMake = require("../../charm-nest-pool-make.js");
const Sides = require("../../charm-nest-engrave-sides.js");
const ArtEdit = require("../../charm-nest-art-edit.js");

const MM = 25.4 / 72;
const pending = (why, more) => Object.assign(new Error(why), { engravingPending: true }, more || {});
/** The library's keep-out layers of a charm (the page's Master.keepOutOf). */
const keepOutOf = (c, face) => c.members.filter(m => m.layer && (face === "front" ? /front\s*keep-?out/i : /back\s*keep-?out/i).test(m.layer));
/** A Storage object's path from a download link (firebasestorage.googleapis.com/v0/b/<bucket>/o/<path>?...), a gs:// address, or the "storage:" form the host gives. */
function pathOfUrl(url) {
  const u = String(url || "");
  if (u.startsWith("storage:")) return u.slice(8);
  const m = /\/o\/([^?#]+)/.exec(u); if (m) { try { return decodeURIComponent(m[1]); } catch (_) { return null; } }
  const g = /^gs:\/\/[^/]+\/(.+)$/.exec(u); return g ? g[1] : null;
}

function create(env) {
  const { db } = env, log = env.log || (() => {});
  const { CharmNestPDF: P } = Pdf.withCanvas({ backend: "auto" });   // (the page's canvas for node: masterCharm traces the silhouettes as the page does)
  const settings = () => (typeof env.settings === "function" ? env.settings() : env.settings) || {};
  const entries = new Map(), rows = new Map();
  const bucket = () => env.bucket || require("./firebaseAdmin").storage().bucket();
  async function download(url) {
    const p = pathOfUrl(url); if (!p) throw new Error("this design file's address cannot be read in the cloud");
    if (!/^charmnest\//.test(p)) throw new Error("the cloud reads design files under charmnest/ only");
    const [buf] = await bucket().file(p).download();
    return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  const skuRegex = () => { try { return new RegExp(settings().skuPattern || "^[A-Z0-9][A-Z0-9 _.,'&()+\\-]{1,60}$"); } catch (_) { return P.SKU_PATTERN_DEFAULT; } };
  async function fetchEntry(sku) {
    const k = String(sku || "").trim().toUpperCase(); if (!k) return null;
    if (entries.has(k)) return entries.get(k);
    const s = await db.collection(MasterIdx.INDEX).doc(k).get(), e = s.exists ? MasterIdx.slimEntry(s.data()) : null;
    if (e) entries.set(k, e);
    return e;
  }
  const Master = { entryFor: sku => entries.get(String(sku || "").trim().toUpperCase()) || null, fetchEntry, keepOutOf, skuRegex };
  const S = { get settings() { return Object.assign({ minPt: 6, silhouetteRes: 6 }, settings()); }, poolSources: {}, cloud: { ok: true } };
  const B = { pool: { rows, sources: new Map() }, master: { entries } };
  const H = {
    window: { CharmNestPair: Pair }, S, B, O, P, MM, Master, PoolPieces, cloud: true,
    agent: (meta, kind, text) => log(`[engrave piece] ${kind}: ${text}`),
    api: async (fn, body) => { if (fn === "charmNestOutput" && body && body.op === "url") return { url: "storage:" + String(body.path || "") }; throw new Error(`the cloud's piece maker does not call ${fn} ${body && body.op}`); },
    stockFor: () => ({ wPt: 1e9, hPt: 1e9 }), labelOf: m => String(m || ""), now: () => Date.now(),
    CharmNestAssets: () => ({ bytes: download }),
    keptMaster: async () => null, keepMaster: () => {}, pinPooled: () => {}, charmOf: () => null, greenHeld: () => false, cancelled: () => false,
    Orders: () => ({}), Review: () => ({}), CustomPrint: () => null, CustomSheet: () => ({ sentOf: () => null }), ArtEdit: () => ArtEdit
  };
  const maker = PoolMake.create(H);
  // the piece's own artwork edits, read through the cloud's read call (artList); a picture's paths from its file in Storage
  ArtEdit.setHost({ api: (fn, body) => (fn === "charmNestLibrary" && body && (body.op === "artList" || body.op === "artPrepoolList") ? env.read(body) : Promise.reject(new Error("not a read the cloud makes here"))), bytes: download, rowOf: id => rows.get(id) || null,
    parsedOf: c => ((c && S.poolSources[c.sourceId]) || {}).parsed || null });   // (an edited cut line's silhouette is traced again on the design it was read from, as the page does)

  /** The pool rows of these pieces (Charm_Pool, through poolGet), kept for this unit. */
  async function poolRows(ids) {
    const want = [...new Set((ids || []).filter(id => id && !rows.has(id)))];
    for (let i = 0; i < want.length; i += 300) { const got = await env.read({ op: "poolGet", poolIds: want.slice(i, i + 300) }); for (const [id, r] of Object.entries((got && got.pools) || {})) rows.set(id, r); }
    return new Map((ids || []).map(id => [id, rows.get(id) || null]));
  }
  /** The context CharmNestEngraveSides reads a line's pieces through (the page's sidesCtx). */
  const sidesCtx = () => ({ poolRow: id => rows.get(id) || null, charmOf: () => null, entryFor: sku => Master.entryFor(sku), pair: Pair, mergeSeals: (...r) => require("../../charm-nest-engraving-seals.js").merge(...r) });

  /** The charm of one piece, as its page holds it on its sheet and as Engrave's charmFor gives it (the mirror of a Right ear, its own artwork). */
  async function charmOf(poolId, o = {}) {
    const pool = o.pool || rows.get(poolId) || (await poolRows([poolId])).get(poolId);
    if (!pool) throw pending("its piece is not in the pool yet");
    rows.set(poolId, pool);
    const sheet = o.sheet || null, rc = sheet && Array.isArray(sheet.charms) ? sheet.charms.find(c => c && c.poolId === poolId) || {} : {};
    // a custom design is in no master file: it is made from its own file, the one its sheet was cut from (the sheet record's sources)
    let own = pool.custom || rc.custom ? ((sheet && sheet.sources) || []).find(s => s && (s.id === rc.sourceId || s.name === rc.sourceName)) : null;
    const entry = own ? null : await fetchEntry(pool.sku);
    if (!own && !entry && (pool.custom || rc.custom) && pool.aiPath) own = { path: String(pool.aiPath).replace(/#fit-.*$/, "") };   // (OWNFILE: _charmNestNestPieces.js)
    if (!entry && !(own && (own.url || own.path))) throw pending(`the design ${pool.sku || "of this piece"} is not in the library`);
    let src;
    try { src = entry ? await maker.masterCharm(entry, pool.size || null) : await maker.masterCharm({ sku: rc.sku || pool.sku, aiPath: own.path || pathOfUrl(own.url), aiUrl: own.url }, null); }
    catch (e) { throw pending(`its design file could not be read (${(e && e.message) || e})`); }
    // a piece of a mismatched pair is cut from its own body, never from both (the pool row says which; the sheet's record says so too)
    const piece = PoolPieces.cleanFields(pool).side ? pool : PoolPieces.cleanFields(rc).side ? rc : null;
    const base = await maker.ensureBase(src, piece, rc.hash ? rc : pool).catch(() => null);
    if (!base) throw Object.assign(new Error(`its ${piece && piece.side === "L" ? "left" : "right"} body is no longer in the design — fit the words again`), { engravingInvalid: true });
    let charm = maker.cloneCharm(base, `${src.id}:${poolId}`);
    charm = await ArtEdit.piece(charm, poolId, pool);   // (ART EDIT: the piece's own edit, when its pool row names one; it throws when the edit cannot be read: never cut without it)
    charm.poolId = poolId; charm.order = pool.orderId; charm.lineKey = pool.lineKey; charm.orderInfo = { receiptId: pool.orderId, transactionId: pool.transactionId, sku: pool.sku, copy: pool.copy, quantity: pool.quantity, form: pool.form, size: pool.size };
    if (own) charm.custom = true;
    const hint = { slot: o.slot || null, side: o.side || null };
    const cut = Sides.pieceCharm(sidesCtx(), poolId, charm, hint);
    return { charm: cut, src, entry, pool, rc };
  }
  /** The parsed design a charm's file is built from (buildBackFile's `parsed`). */
  const sourceOf = src => src;
  return { charmOf, poolRows, fetchEntry, entryOf: sku => Master.entryFor(sku), sidesCtx, sourceOf, rows, maker, P, download };
}

module.exports = { create, keepOutOf, pathOfUrl };
