/*  charm-nest-pair-labels.js — what a label, a set manifest and a sticker say about a PAIR (Paul, 9 Oct 2026).
 *  ═══════════════════════════════════════════════════════════════════════
 *  Pairs, mismatched pairs and multi-piece orders (plan: plans/pairs-1009/plan.md, rules R1 to R5). Every place that prints, writes or
 *  names a piece (the sheet's QR label, the set's manifest, the back files, the 1 x 1 in sticker) asks THIS file how to say it, so
 *  a mismatched pair is always "Left" and "Right" of the same order, a group split over sheets is always said, and everything else is
 *  worded exactly as before (a line with no side and no split gives back the old text, byte for byte).
 *
 *  Pure data in, plain strings out; no DOM, no network. Sides, groups and pair designs are read from the fields the shared module
 *  defines (charm-nest-pair.js: side "L" | "R" | null, groupKey, a master entry's `pair`); nothing here measures geometry.
 *
 *    sideOf(x)                      "L" | "R" | null     a charm, pool row, set copy, back record, piece (its own `side`)
 *    wordOf(side | x)               "Left" | "Right" | ""
 *    copyWord(sku, copy, count, x)  one copy's name in the manifest: "SKU#2" (count > 1), "SKU" (alone), plus " (Left)" for a side
 *    manifestEntries(lines, nameOf, arrow) ["SKU#1 (Left)->GF Sheet 1", ...] for the lines of ONE order
 *    piecesOfOrders(orders)         the flat piece list of a set's `orders` map (page shape or the server's)
 *    piecesOfCharms(charms)         the pieces of placed charms (order info, else the pool id "receipt_transaction_copy")
 *    reconcile(base, live)          base pieces, with each live sheet's own pieces in place of the ones it was recorded with
 *    splitNotes(sheetId, pieces, only?)  the lines a sheet's QR label carries for the groups that sit on more than one sheet ([] when none); only: the order numbers of one label's part
 *    backWord(b)                    " · Left" for a back record with a side, else ""
 *    labelPieces(line, entry)       the stickers a mismatched pair line needs: [{ side, n, of }], else null
 *    stickerPieces(rows, entryOf)   the same for a card's lines (what QR Printer.html prints one page for)
 *    pieceCount(rows, entryOf)      pieces behind a card's lines (a mismatched pair makes two per unit), else null
 *  ═══════════════════════════════════════════════════════════════════════ */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory(root);
  else root.CharmNestPairLabels = factory(root);
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";
  const str = v => (v == null ? "" : String(v));
  const pair = () => {
    if (root && root.CharmNestPair) return root.CharmNestPair;
    try { if (typeof require === "function") return require("./charm-nest-pair.js"); } catch (_) { /* the plain words below are enough */ }
    return null;
  };

  /** The side a stored piece has: its own `side` ("L" or "R"), nothing else (old records have none: they are not a mismatched pair). */
  function sideOf(x) {
    if (!x || typeof x !== "object") return null;
    for (const v of [x.side, x.orderInfo && x.orderInfo.side, x.piece && x.piece.side, x.pool && x.pool.side]) if (v === "L" || v === "R") return v;
    return null;
  }
  function wordOf(x) {
    const side = typeof x === "string" ? x : sideOf(x);
    const P = pair();
    if (P && typeof P.sideLabel === "function") return P.sideLabel(side);
    return side === "L" ? "Left" : side === "R" ? "Right" : "";
  }

  /** "SKU#2" for one of several copies, "SKU" for a lone one, and " (Left)" / " (Right)" after it for a piece with a side (the words the set editor's sideTag uses). */
  function copyWord(sku, copy, count, x) {
    const w = wordOf(x);
    return `${str(sku)}${count > 1 ? "#" + str(copy) : ""}${w ? " (" + w + ")" : ""}`;
  }

  /** "Sheet 2" from a sheet's file base ("GF_Oct-9_Set-3_Sheet-2"); anything else is said as it is. */
  const shortSheet = name => { const m = /_Sheet-(\d+)/.exec(str(name)); return m ? "Sheet " + m[1] : str(name); };
  const sheetKey = c => str(c && (c.sheetId || c.sheet));

  /** The manifest words of ONE order's lines: every copy "SKU#2 (Left)->sheet" (a copy without a side: "SKU#2->sheet", as before).
   *  lines: the order's lines, as an array or as the page's { transactionId: line }; nameOf(copy): the sheet's name; arrow: "->" (set editor) or "→" (bridge). */
  function manifestEntries(lines, nameOf, arrow) {
    const list = Array.isArray(lines) ? lines : Object.values(lines || {});
    const out = [];
    for (const l of list) {
      const copies = (l && l.copies) || [];
      for (const c of copies) out.push(`${copyWord(l.sku, c.copy, copies.length, c)}${arrow || "->"}${nameOf ? nameOf(c) : str(c.sheet)}`);
    }
    return out;
  }

  /** The flat pieces of a set's `orders` map: { rid: { lines: { tid: line } | [line] } } → [{ rid, tid, sku, copy, poolId, sheetId, sheet, side }]. */
  function piecesOfOrders(orders) {
    const out = [];
    for (const [rid, o] of Object.entries(orders || {})) {
      const lines = Array.isArray(o && o.lines) ? o.lines : Object.values((o && o.lines) || {});
      for (const l of lines) for (const c of (l && l.copies) || []) out.push({ rid: str(rid), tid: str(l.transactionId), sku: str(l.sku), copy: c.copy, poolId: str(c.poolId), sheetId: str(c.sheetId), sheet: str(c.sheet), side: sideOf(c) });
    }
    return out;
  }

  const POOL_PARTS = /^(\d{4,20})_([^_]*)_(\d{1,3})$/;   // receiptId_transactionId_copy (Orders.poolId)
  /** The pieces of a list of placed charms: [{ rid, tid, sku, copy, poolId, side }]. A charm with no order info is read from its pool id; one with neither is no piece here. */
  function piecesOfCharms(charms) {
    const out = [];
    for (const c of charms || []) {
      if (!c) continue;
      const oi = c.orderInfo || {}, m = POOL_PARTS.exec(String(c.poolId || ""));
      const rid = str(c.order || oi.receiptId || (m && m[1])).split("/")[0], tid = str(oi.transactionId || (m && m[2]));
      if (!rid || !tid) continue;
      out.push({ rid, tid, sku: str(oi.sku), copy: oi.copy || (m && +m[3]) || 1, poolId: str(c.poolId), side: sideOf(c) });
    }
    return out;
  }

  /** The pieces as the sheets say they are now: `live` is [{ sheetId, sheet, pieces:[piece] }] (sheets this page holds or has just read). A live
   *  sheet is the truth about itself: the pieces recorded on it are dropped and its own put in their place; sheets not listed keep what was recorded. */
  function reconcile(base, live) {
    const truth = new Set((live || []).map(s => str(s.sheetId)).filter(Boolean));
    const out = (base || []).filter(p => !truth.has(str(p.sheetId)));
    const seen = new Set(out.map(p => p.poolId).filter(Boolean));
    for (const s of live || []) for (const p of s.pieces || []) {
      if (p.poolId && seen.has(p.poolId)) continue;   // (a piece stands on one sheet: the first live one that says so)
      if (p.poolId) seen.add(p.poolId);
      out.push(Object.assign({}, p, { sheetId: str(s.sheetId), sheet: str(s.sheet) }));
    }
    return out;
  }

  /** "Left", "Right", "2 Left" for pieces that have sides; "1 of 2" / "1" for pieces that have none. */
  function wordsOf(pieces, total, here) {
    if (pieces.length && pieces.every(p => p.side)) {
      const n = { L: 0, R: 0 }; for (const p of pieces) n[p.side]++;
      return ["L", "R"].filter(k => n[k]).map(k => (n[k] > 1 ? n[k] + " " : "") + wordOf(k)).join(" + ");
    }
    return here ? `${pieces.length} of ${total}` : String(pieces.length);
  }
  const MAX_NOTES = 4;

  /** The lines a sheet's QR label carries (6.5 pt under its title) for every group (order line) that has pieces on this sheet AND on another:
   *  "4171450075 Left here, Right on Sheet 3" (a mismatched pair), "4171450075 1 of 2 here, 1 on Sheet 3" (a matching pair, discs).
   *  Deterministic (order, then line); more than four groups: the first three and "+N more". [] for a sheet that holds no split group. */
  function splitNotes(sheetId, pieces, only) {
    const id = str(sheetId), groups = new Map(), keep = only ? new Set([...only].map(str)) : null;
    for (const p of pieces || []) {
      if (!p || !p.rid || !p.tid || !p.sheetId || (keep && !keep.has(p.rid))) continue;
      const k = p.rid + ":" + p.tid;
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(p);
    }
    const lines = [];
    for (const k of [...groups.keys()].sort()) {
      const g = groups.get(k);
      if (g.length < 2) continue;
      const here = g.filter(p => p.sheetId === id);
      if (!here.length || here.length === g.length) continue;
      const elsewhere = new Map();
      for (const p of g) if (p.sheetId !== id) { if (!elsewhere.has(p.sheetId)) elsewhere.set(p.sheetId, []); elsewhere.get(p.sheetId).push(p); }
      const rest = [...elsewhere.entries()].sort((a, b) => str(a[1][0].sheet).localeCompare(str(b[1][0].sheet))).map(([, ps]) => `${wordsOf(ps, g.length, false)} on ${shortSheet(ps[0].sheet) || "another sheet"}`);
      lines.push(`${g[0].rid} ${wordsOf(here, g.length, true)} here, ${rest.join(", ")}`);
    }
    return lines.length > MAX_NOTES ? lines.slice(0, MAX_NOTES - 1).concat(`+${lines.length - (MAX_NOTES - 1)} more split orders`) : lines;
  }

  /** " · Left" for a back (an engraving) of a piece with a side, else "": appended to the back's line in files people read. */
  const backWord = b => { const w = wordOf(b); return w ? " · " + w : ""; };

  /** Is this master entry (or charm) a mismatched pair design? */
  function mismatchedDesign(entry) {
    if (!entry || typeof entry !== "object") return false;
    const P = pair();
    if (P && typeof P.designPair === "function" && !entry.outline) { const d = P.designPair(entry); return !!(d && d.mismatched && d.bodies === 2); }
    if (P && typeof P.isMismatched === "function") { try { return !!P.isMismatched(entry); } catch (_) { return false; } }
    return !!(entry.pair && entry.pair.mismatched && +entry.pair.bodies === 2);
  }
  const quantityOf = line => { const q = Math.floor(+(line && ((line.spec && line.spec.quantity) || line.quantity || line.qty)) || 1); return q > 0 ? Math.min(q, 20) : 1; };

  /** The stickers one order line needs: a mismatched pair line makes, for each unit bought, a LEFT sticker then a RIGHT sticker of the same order
   *  ([{ side, n, of }]); a line that is not a mismatched pair makes none of its own (null: the order's one sticker, as before). */
  function labelPieces(line, entry) {
    if (!mismatchedDesign(entry)) return null;
    const q = quantityOf(line), out = [];
    for (let n = 1; n <= q; n++) { out.push({ side: "L", n, of: q }); out.push({ side: "R", n, of: q }); }
    return out;
  }
  /** The stickers a card's lines need together (rows: the sorter's rows { spec, line }); entryOf(row) → the master entry of the row's design. null when none. */
  function stickerPieces(rows, entryOf) {
    const out = [];
    for (const r of rows || []) { let e = null; try { e = entryOf ? entryOf(r) : null; } catch (_) { e = null; } const p = labelPieces(r && (r.line || r), e); if (p) out.push(...p); }
    return out.length ? out : null;
  }
  /** Pieces behind a card's lines: a mismatched pair line makes two per unit, every other line what its quantity says (as the page counted before). null when no line is a pair. */
  function pieceCount(rows, entryOf) {
    let n = 0, any = false;
    for (const r of rows || []) {
      let e = null; try { e = entryOf ? entryOf(r) : null; } catch (_) { e = null; }
      const q = quantityOf(r && (r.line || r));
      if (mismatchedDesign(e)) { any = true; n += 2 * q; } else n += q;
    }
    return any ? n : null;
  }

  return { sideOf, wordOf, copyWord, manifestEntries, piecesOfOrders, piecesOfCharms, reconcile, splitNotes, shortSheet, backWord, mismatchedDesign, labelPieces, stickerPieces, pieceCount, MAX_NOTES };
});
