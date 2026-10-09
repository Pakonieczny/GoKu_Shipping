/*  charm-nest-pool-pieces.js — the shape of a PIECE record (pool row, charm, sheet-record charm) for pairs.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Paul (9 Oct 2026): a pair of earrings, a mismatched pair (a left charm and a different right charm under one listing) and a
 *  disc necklace must be tracked as several pieces of ONE order line everywhere. charm-nest-pair.js says WHAT a line makes
 *  (piecesFor); this file says how a piece is RECORDED and how an old record that has no such fields is read.
 *
 *  Loads three ways, one source:  window.CharmNestPoolPieces (the sorter page)
 *                                 module.exports            (node tests and Netlify functions: require("../../charm-nest-pool-pieces.js"))
 *
 *  THE RECORD. A piece keeps its id: `${receiptId}_${transactionId}_${n}` (n = 1..number of pieces; the server accepts that shape only).
 *  Scalar fields are added to every piece of an EARRING PAIR (stud, hoop, huggie; matching or mismatched: contract.md amendment 2, Paul
 *  18:47), and to no other piece (a single charm, a necklace's discs, letters and charms stay as they were, so none of these is written
 *  for them; their group is read from the pool id and their `quantity`):
 *      side       "L" | "R"                 the ear
 *      mirror     true | false              true: the piece is the MIRROR IMAGE of the as-drawn master design (a Left faces left, a Right faces right)
 *      bodyIndex  0 | 1                     which of the design's bodies this piece is cut from (a matching pair: 0; a mismatched pair: 0 = left body, 1 = right body)
 *      groupKey   "receiptId:transactionId" every piece of one order line (contract.md)
 *      groupSize  number of pieces in the group (2 x the Etsy quantity)
 *  They are written in the pool row (poolPut), on the page's charm, and in the sheet record's `charms[]` entry. side, bodyIndex, groupKey and
 *  groupSize come all four or none (a record with some of them is read as having none); `mirror` is a boolean or absent (an older sided
 *  record has none: read as the as-drawn piece). Scalars only (Firestore refuses arrays in arrays; nothing here stores a list).
 *  Piece numbering: unit u = 1..q makes piece 2u-1 (LEFT) then piece 2u (RIGHT); a mismatched pair's Left is cut from body 0, its Right from body 1.
 *
 *  OLD RECORDS (no fields) are read, never rewritten: metaOf() derives the group from the pool id, the size from `quantity` (the
 *  number of copies a row counts up to), side null. A row of a design known to be mismatched that carries no fields is a GLUED
 *  piece: it was made before this change as ONE charm holding both bodies; it stays one piece (kind "glued").
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestPoolPieces = api;
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";

  const FIELDS = ["side", "mirror", "bodyIndex", "groupKey", "groupSize"];
  const MAX_PIECES = 400;                         // the most pool rows one poolPut call carries (charmNestLibrary poolPut slice)
  const POOL_ID = /^(\d{4,20})_([^_]*)_(\d{1,3})$/;   // receiptId_transactionId_copy (charm-nest-orders.js poolId)
  const num = v => (Number.isFinite(+v) && +v > 0 ? Math.floor(+v) : 0);

  // The shared definition of a pair (charm-nest-pair.js). Looked up lazily; a page or a function that has not got it still reads records.
  let pairMod = null;
  const Pair = () => {
    if (pairMod) return pairMod;
    if (root && root.CharmNestPair) return (pairMod = root.CharmNestPair);
    try { if (typeof require === "function") pairMod = require("./charm-nest-pair.js"); } catch (_) { /* optional */ }
    return pairMod;
  };

  /** A pool id taken apart, or null. */
  function parsePoolId(id) {
    const m = POOL_ID.exec(String(id == null ? "" : id));
    return m ? { receiptId: m[1], transactionId: m[2], copy: +m[3], lineKey: m[1] + "_" + m[2], groupKey: m[1] + ":" + m[2] } : null;
  }

  const keyOk = k => typeof k === "string" && /^\d{1,30}:[^:\s]{1,40}$/.test(k);
  /** "receiptId:transactionId" of a pool id, a pool row, a charm, a placement or an order row/line ("" when it cannot be told). */
  function groupKeyOf(x) {
    if (x == null) return "";
    if (typeof x === "string" || typeof x === "number") { const p = parsePoolId(x); return p ? p.groupKey : keyOk(String(x)) ? String(x) : ""; }
    if (keyOk(x.groupKey)) return x.groupKey;
    const id = x.poolId || (typeof x.id === "string" ? x.id : ""), p = parsePoolId(id);
    if (p) return p.groupKey;
    const P = Pair(); const k = P && P.groupKey ? P.groupKey(x) : "";
    if (keyOk(k)) return k;
    const lk = typeof x.lineKey === "string" ? /^(\d{1,30})_([^_]+)$/.exec(x.lineKey) : null;
    return lk ? lk[1] + ":" + lk[2] : "";
  }

  /** The fields of a piece that has side, bodyIndex, groupKey and groupSize ALL valid (and `mirror` when it is a boolean), else {}: what a record may carry. A plain piece carries none. */
  function cleanFields(o) {
    if (!o || typeof o !== "object") return {};
    const side = o.side, bodyIndex = o.bodyIndex, size = o.groupSize, pid = typeof o.poolId === "string" ? parsePoolId(o.poolId) : null;
    const key = pid ? pid.groupKey : o.groupKey;   // a pool id names its group by itself: a wrong groupKey on a row is repaired from it
    if (side !== "L" && side !== "R") return {};
    if (!Number.isInteger(bodyIndex) || bodyIndex < 0 || bodyIndex > 9) return {};
    if (!Number.isInteger(size) || size < 2 || size > MAX_PIECES) return {};
    if (!keyOk(key)) return {};
    return typeof o.mirror === "boolean" ? { side, mirror: o.mirror, bodyIndex, groupKey: key, groupSize: size } : { side, bodyIndex, groupKey: key, groupSize: size };
  }
  /** The record fields of one piece from CharmNestPair.piecesFor ({ side, mirror, bodyIndex, groupKey, n, of }), or from a stored piece: {} unless it is a left or right piece. */
  function fieldsOf(piece) {
    if (!piece) return {};
    return cleanFields({ side: piece.side, mirror: piece.mirror, bodyIndex: piece.bodyIndex, groupKey: piece.groupKey, groupSize: piece.groupSize != null ? piece.groupSize : piece.of });
  }
  /** What a sheet record's charms[] entry adds for a charm: the four fields of a left or right piece, nothing for any other charm. */
  const sheetCharmFields = charm => fieldsOf(charm);

  /** One piece, with everything a reader needs. `x`: a pool id, a pool row, a charm, or a sheet-record charm. ctx: { mismatchedDesign, quantity }.
   *  kind: "single" | "pair" | "multi" (matching pieces) | "mismatched" (a left and a right) | "glued" (an old row of a mismatched design). */
  function metaOf(x, ctx) {
    ctx = ctx || {};
    const row = x && typeof x === "object" ? x : { poolId: String(x == null ? "" : x) };
    const id = String(row.poolId || (typeof row.id === "string" && POOL_ID.test(row.id) ? row.id : "") || "");
    const p = parsePoolId(id), f = cleanFields(row);
    const n = p ? p.copy : num(row.copy) || num(row.orderInfo && row.orderInfo.copy) || 1;
    if (f.side) {
      // (a mismatched pair's Right is cut from body 1; a matching pair has one body. A Left of a mismatched pair says so only with ctx.mismatchedDesign: the master entry's `pair`)
      const mis = !!ctx.mismatchedDesign || f.bodyIndex > 0;
      return { poolId: id, groupKey: f.groupKey, n, groupSize: f.groupSize, side: f.side, mirror: typeof f.mirror === "boolean" ? f.mirror : null, bodyIndex: f.bodyIndex, unit: Math.ceil(n / 2), kind: f.groupSize === 2 ? (mis ? "mismatched" : "pair") : "multi", glued: false, legacy: false };
    }
    const size = num(row.quantity) || num(row.orderInfo && row.orderInfo.quantity) || num(ctx.groupSize) || num(ctx.quantity) || 1;
    const glued = !!ctx.mismatchedDesign;
    return { poolId: id, groupKey: groupKeyOf(row) || (p ? p.groupKey : ""), n, groupSize: size, side: null, mirror: null, bodyIndex: 0, unit: n, kind: glued ? "glued" : size > 2 ? "multi" : size === 2 ? "pair" : "single", glued, legacy: glued };
  }

  /** The groups (order lines) of a list of pieces, each with where its pieces are. `sheetOf(row)` gives a piece's sheet id or null.
   *  Only groups of two or more pieces: [{ groupKey, size, have, missing, sided, ids, sheets: { sheetId: [poolId] }, off: [poolId], split }].
   *  split: the pieces are on more than one sheet, or some are on a sheet and others are not (or not made). Pure: it reads nothing. */
  function groupsOf(rows, sheetOf) {
    const where = typeof sheetOf === "function" ? sheetOf : () => null, by = new Map();
    for (const r of rows || []) {
      const m = metaOf(r); if (!m.groupKey) continue;
      if (!by.has(m.groupKey)) by.set(m.groupKey, { groupKey: m.groupKey, size: 0, have: 0, sided: false, mismatched: false, ids: [], sheets: {}, off: [] });
      const g = by.get(m.groupKey); g.size = Math.max(g.size, m.groupSize); g.have++; g.sided = g.sided || !!m.side; g.mismatched = g.mismatched || m.bodyIndex > 0; g.ids.push(m.poolId);
      const s = where(r);
      if (s) (g.sheets[s] || (g.sheets[s] = [])).push(m.poolId); else g.off.push(m.poolId);
    }
    const out = [];
    for (const g of by.values()) {
      if (g.size < 2 && g.have < 2) continue;
      g.missing = Math.max(0, g.size - g.have);
      const onN = Object.keys(g.sheets).length;
      g.split = onN > 1 || (onN === 1 && (g.off.length > 0 || g.missing > 0));
      out.push(g);
    }
    return out;
  }

  /** An old row of a design known to be mismatched: no fields, so it is one glued piece. A line whose pieces are live on a sheet stays glued
   *  (its sheet holds both bodies in one charm); a line taken off may be made up again as two pieces. `rows`: the line's pool rows. */
  function legacyGlued(rows, live) {
    const mine = (rows || []).filter(r => r && !["abandoned", "superseded"].includes(r.state));
    if (!mine.length || mine.some(r => cleanFields(r).side)) return false;
    return typeof live === "function" ? mine.some(r => live(r)) : mine.some(r => !!r.sheetId);
  }

  return { FIELDS, MAX_PIECES, parsePoolId, groupKeyOf, cleanFields, fieldsOf, sheetCharmFields, metaOf, groupsOf, legacyGlued };
});
