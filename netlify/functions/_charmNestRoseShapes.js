/*  netlify/functions/_charmNestRoseShapes.js — the green line's charm outlines in node (cloud plan v2 step 10b; hard-parts §4.2).
 *  A sheet record does not keep a charm's vectors (outline, members, centerPt): the page draws the contour from the charms it holds in memory (RoseStock.plan →
 *  CharmNestRose.shapes(charms, placements), charm-nest-rose-ui.js ~238). The cloud makes the same charms again from the designs, with the page's own code:
 *    · the pieces' pool rows (sku, size, side, bodyIndex, mirror, oneBody, charmHash, masterHash, aiPath);
 *    · each design read from the bucket and made up by charm-nest-pool-make.js (create(host): masterCharm → readMasterCharm, the same parse, grouping, rings, size fit;
 *      ensureBase: a pair's own body and the Right's mirror image, CharmNestPair.pieceGeometry; a one-body piece: soloBodies) with a node host made here, and a piece's own
 *      artwork edit put over it (charm-nest-art-edit.js piece(), its records read through the cloud's read call);
 *    · when Part 3's netlify/functions/_charmNestPieceBits.js is present, its loadCharms is used instead (the same pieces, one maker for the cloud).
 *  Then CharmNestRose.shapes(charms, placements), the pure function the page runs, with scale = placement.scale || 0.971 (the page's CHARM_SCALE).
 *  THE OWNER'S 0.06 mm RULE: the shapes are vectors (the design's own Béziers flattened), so node and Chrome compute the same numbers; the server's guard (_charmNestRoseStock
 *  sameOutline, 0.1 pt = 0.035 mm) holds a protected outline. A rebuilt charm whose extent differs from the one the sheet record kept (widthPt / heightPt) by more than
 *  0.06 mm (0.17 pt) is NOT used: the line is refused with words ("the design of <piece> changed since it was placed: nest the sheet again"), nothing is drawn. The same when the
 *  design's file changed since the piece was made (its masterHash), or a piece's design cannot be read.                                                                           */
"use strict";
const O = require("../../charm-nest-orders.js");
const Rose = require("../../charm-nest-rose.js");
const PoolPieces = require("../../charm-nest-pool-pieces.js");
const TOL_PT = 0.06 / (25.4 / 72);   // 0.06 mm in points
const POOL = "Charm_Pool", INDEX = "Charm_Master_Index";
const num = v => (Number.isFinite(+v) ? +v : 0);

class ShapeError extends Error { constructor(why) { super(why); this.shape = true; } }

/** The path of a stored file from the link the page kept (a firebase download URL, a gs:// path, or the "bucket:" path this host hands out). */
function pathOfUrl(url) {
  const s = String(url || "");
  if (s.startsWith("bucket:")) return s.slice(7);
  const m = /\/o\/([^?]+)/.exec(s); if (m) return decodeURIComponent(m[1]);
  const g = /^gs:\/\/[^/]+\/(.+)$/.exec(s); if (g) return g[1];
  return "";
}
/** A node host for the page's piece maker (charm-nest-pool-make.js create(host)): what it reads of the world, from the bucket and the cloud's reads. */
function makeHost(ctx, cache) {
  const bucket = ctx.bucket || require("./firebaseAdmin").storage().bucket();
  const Pdf = require("./_charmNestPdf").withCanvas();   // (the page's canvas in node, for the silhouettes the maker traces: _charmNestCanvas.js)
  const bytes = async url => { const p = pathOfUrl(url); if (!p || !/^charmnest\//.test(p)) throw new Error("a design file the cloud cannot read: " + String(url).slice(0, 80)); const [b] = await bucket.file(p).download(); return new Uint8Array(b.buffer, b.byteOffset, b.byteLength); };
  let ArtEdit; try { ArtEdit = require("../../charm-nest-art-edit.js"); ArtEdit.setHost({ api: (fn, body) => ctx.read(body), rowOf: id => cache.rows.get(String(id)) || null, bytes }); } catch (_) { ArtEdit = undefined; }
  const keepOutOf = (c, face) => (c.members || []).filter(m => m.layer && (face === "front" ? /front\s*keep-?out/i : /back\s*keep-?out/i).test(m.layer));
  return {
    window: { CharmNestPair: require("../../charm-nest-pair.js") },
    S: { settings: { silhouetteRes: 6, minPt: 6 }, poolSources: {}, cloud: { ok: true } },
    B: { pool: { rows: cache.rows, sources: cache.sources }, master: { entries: {} } },
    O, P: Pdf.CharmNestPDF, MM: 25.4 / 72, PoolPieces: require("../../charm-nest-pool-pieces.js"),
    Master: { entryFor: sku => cache.entries.get(String(sku || "").trim().toUpperCase()) || null, fetchEntry: async sku => cache.entries.get(String(sku || "").trim().toUpperCase()) || null, keepOutOf, skuRegex: () => /^[A-Z0-9][A-Z0-9_\-]{1,60}$/i, patch: () => {} },
    agent: (meta, level, text) => { try { console.log(`[rose-shapes] ${level}: ${String(text).slice(0, 300)}`); } catch (_) { /* a note only */ } },
    api: async (fn, body) => (fn === "charmNestOutput" && body && body.op === "url" ? { ok: true, url: "bucket:" + String(body.path || "") } : ctx.read(body)),
    stockFor: () => ({}), labelOf: m => m, CharmNestAssets: () => ({ bytes }),
    keptMaster: async () => null, keepMaster: () => {}, now: () => Date.now(),
    Orders: () => null, Review: () => null, CustomPrint: () => undefined, CustomSheet: () => undefined, ArtEdit: () => ArtEdit,
    pinPooled: () => {}, charmOf: () => null, greenHeld: () => false, cancelled: () => false, cloud: true
  };
}
/** The charms of a sheet's placed pieces, made again from their designs, with the vectors the contour reads: Map(charmId -> charm). */
async function charmsOf(ctx, rec) {
  const db = ctx.db, placed = new Set((rec.placements || []).map(p => p.id)), recCharms = (rec.charms || []).filter(c => c && placed.has(c.id));
  if (recCharms.length !== placed.size) throw new ShapeError("A placement of this sheet names no piece: nest the sheet again");
  // Part 3's PieceBits when it is here (one maker for the cloud), else the page's piece maker with this file's node host
  // (read by a name the bundler does not follow: a missing module never fails the build; a deploy that bundles it names it in netlify.toml included_files)
  let PieceBits = null; try { const name = "./_charmNestPieceBits"; PieceBits = require(name); } catch (_) { PieceBits = null; }
  if (PieceBits && typeof PieceBits.loadCharms === "function") {
    const list = await PieceBits.loadCharms(db, ctx.bucket || require("./firebaseAdmin").storage().bucket(), recCharms.map(c => c.poolId), { charmsOf: () => recCharms });
    const byPool = new Map((list || []).map(c => [String(c.poolId), c])), out = new Map();
    for (const rc of recCharms) { const c = byPool.get(String(rc.poolId)); if (!c || !c.outline) throw new ShapeError(`the design of ${rc.name || rc.poolId} could not be read: nest the sheet again`); out.set(rc.id, Object.assign({}, c, { id: rc.id })); }
    return out;
  }
  const cache = { rows: new Map(), sources: new Map(), entries: new Map() };
  const ids = [...new Set(recCharms.map(c => c.poolId).filter(Boolean).map(String))];
  if (ids.length !== recCharms.length) throw new ShapeError("A piece of this sheet has no pool record (a custom piece of no order): its green line is drawn on a computer");
  for (let i = 0; i < ids.length; i += 100) for (const s of await db.getAll(...ids.slice(i, i + 100).map(id => db.collection(POOL).doc(id)))) if (s.exists) cache.rows.set(s.id, Object.assign({ poolId: s.id }, s.data()));
  const skus = [...new Set([...cache.rows.values()].map(r => String(r.sku || "").trim().toUpperCase()).filter(Boolean))];
  for (let i = 0; i < skus.length; i += 30) for (const s of await db.getAll(...skus.slice(i, i + 30).map(k => db.collection(INDEX).doc(k)))) if (s.exists) cache.entries.set(s.id, Object.assign({ sku: s.id }, s.data()));
  const maker = require("../../charm-nest-pool-make.js").create(makeHost(ctx, cache)), H = maker;
  const out = new Map();
  for (const rc of recCharms) {
    await new Promise(r => (typeof setImmediate === "function" ? setImmediate(r) : setTimeout(r, 0)));
    const row = cache.rows.get(String(rc.poolId)), name = rc.name || rc.poolId;
    if (!row) throw new ShapeError(`${name}: its pool record is gone: nest the sheet again`);
    /* MULTIDRAW (Paul, 28 Nov 2026, GF Draft 2: the pieces of one custom file of several charms drawn as its first charm): a custom piece is drawn from its own file, the one it was made, nested and
       written from (as _charmNestFinishHost.js draws it), never from a library entry of the same SKU, and as the charm of that file its row names (customIndex: charm-nest-pool-make.js ensureBase);
       the record and the row of a master piece are read as before */
    const ci = row.custom === true || rc.custom === true ? PoolPieces.customIndexOf(rc, row) : null;
    let entry = (row.custom === true || rc.custom === true) && row.aiPath ? null : cache.entries.get(String(row.sku || "").trim().toUpperCase()) || null;
    if (entry && row.masterHash && entry.masterHash && String(row.masterHash) !== String(entry.masterHash)) throw new ShapeError(`the design of ${name} changed since it was placed: nest the sheet again`);
    if (!entry) { if (!row.aiPath) throw new ShapeError(`${name}: its design is not in the library`); entry = { sku: String(row.sku || "CUSTOM"), aiPath: String(row.aiPath).replace(/#fit-.*$/, ""), charmHash: row.charmHash || null }; }
    let src; try { src = await H.masterCharm(entry, row.size || null); } catch (e) { throw new ShapeError(`the design of ${name} could not be read (${(e && e.message) || e}): nest the sheet again`); }
    const piece = { side: row.side, bodyIndex: row.bodyIndex, mirror: row.mirror };
    const oneBody = typeof row.oneBody === "number" && Number.isInteger(row.oneBody) ? row.oneBody : typeof rc.oneBody === "number" && Number.isInteger(rc.oneBody) ? rc.oneBody : null;
    let base;
    if (oneBody != null && !H.sidedPiece(piece)) { const bodies = await H.soloBodies(src); base = bodies && bodies[oneBody]; }
    else base = await H.ensureBase(src, piece, Object.assign({ charmHash: row.charmHash }, ci != null ? { customIndex: ci } : {}));
    if (!base || !base.outline) throw new ShapeError(`${name}: the body it is cut from could not be made again from its design: nest the sheet again`);
    let charm = H.cloneCharm(base, rc.id); charm.poolId = rc.poolId;
    const AE = require("../../charm-nest-art-edit.js");
    try { charm = await AE.piece(charm, rc.poolId, row); } catch (e) { throw new ShapeError(`${name}: ${(e && e.message) || e}`); }
    // (the owner's rule: a cloud outline may differ from the page's by at most 0.06 mm; its extent is measured against the record's)
    const w = num(charm.widthPt), h = num(charm.heightPt);
    if (num(rc.widthPt) > 0 && num(rc.heightPt) > 0 && (Math.abs(w - num(rc.widthPt)) > TOL_PT || Math.abs(h - num(rc.heightPt)) > TOL_PT)) throw new ShapeError(`the design of ${name} changed since it was placed (${w.toFixed(2)} × ${h.toFixed(2)} pt now, ${num(rc.widthPt).toFixed(2)} × ${num(rc.heightPt).toFixed(2)} pt on the sheet): nest the sheet again`);
    out.set(rc.id, charm);
  }
  return out;
}
/** The contour's shapes for a sheet record (hard-parts §4.2 shapesForSheet): CharmNestRose.shapes over every placement, slimmed. o.noMembers: the outlines only (the full test). */
async function forSheet(ctx, rec, o = {}) {
  const map = await charmsOf(ctx, rec), charms = [...map.values()].map(c => (o.noMembers ? Object.assign({}, c, { members: [] }) : c));
  const placements = (o.placements || rec.placements || []).map(p => Object.assign({}, p));
  return Rose.shapes(charms, placements);
}
/** RoseStock.full (rose-ui.js sheetFull): the saved plan says so, else a plan made now of the charms past the protected ones says it leaves no room. stock: the physical sheet's record. */
async function full(ctx, rec, stock) {
  const plan = rec.rosePlanJson ? (() => { try { return JSON.parse(rec.rosePlanJson); } catch (_) { return null; } })() : null;
  if (plan) return !!plan.full;
  const guard = rec.roseProtectedJson ? (() => { try { return JSON.parse(rec.roseProtectedJson); } catch (_) { return null; } })() : null;
  const fixed = new Set(((guard && guard.placements) || []).map(p => p.id)), fresh = (rec.placements || []).filter(p => !fixed.has(p.id));
  if (!fresh.length) return false;
  const wPt = num(stock && stock.wPt) || num(rec.stock && rec.stock.wPt), hPt = num(stock && stock.hPt) || num(rec.stock && rec.stock.hPt);
  if (!(wPt > 0 && hPt > 0)) return false;
  try { const shapes = await forSheet(ctx, rec, { noMembers: true, placements: fresh }); return !!Rose.plan(shapes, wPt, hPt, (guard && guard.profile) || (stock && stock.profileJson ? JSON.parse(stock.profileJson) : null), num(rec.roseAllowanceMm) || 0.2).full; }
  catch (_) { return false; }
}

module.exports = { forSheet, charmsOf, full, makeHost, pathOfUrl, ShapeError, TOL_PT };
