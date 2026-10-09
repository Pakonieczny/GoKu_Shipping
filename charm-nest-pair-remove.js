/* Charm Nest · pairs and removal (window.PairRemove; Paul, 9 Oct 2026: "when it comes to removing an order from a sheet the system must
   recognize that some orders have multiple pieces some matching some not matching ... and both need to be always fully tracked").

   What it is. Hold, Cancel, Take off the sheet, Release hold, the fill of freed room and Delete sheet all remove or move PIECES of an order.
   A line (one receipt id + transaction id) can make several pieces: a matching pair, a mismatched pair (a left and a right charm, side "L" and
   "R"), n discs. This file answers, for those removals, three questions the pages used to answer per order line only:
     1. Which pieces go together?            PairRemove.expand(ids, src)       every member of every group the ids touch, wherever it sits
     2. Who is left behind?                  PairRemove.partnersOutside(ids, src, whereOf)   the members NOT in ids, and where each is
     3. How is it said in plain words?       PairRemove.describe(items)        sentences naming the pair, the sides and the sheets
   It reads, it never writes. The definitions (group key, sides, kinds) come from the shared module charm-nest-pair.js (CharmNestPair);
   where that file is not loaded, the same group key is read from the pool id or the receipt and transaction, so old pages keep working.
   A single piece, and a line that is not a pair, produces no sentence at all: every old message stays exactly as it was.

   src (every field optional, each an array, an iterable, a Map (its values) or a function returning one):
     rows    the Orders rows ({ poolIds, order:{receiptId}, line:{transactionId}, spec })
     pools   the pool rows ({ poolId, orderId, transactionId, state, side?, copy?, sku? })
     charms  the pieces on loaded sheets ({ poolId, order, orderInfo }), or { charm, sheet } pairs
   Piece states that are gone for good (abandoned, superseded) are never members.

   Loads three ways: window.PairRemove (page), module.exports (node tests). */
(function (root, factory) {
  let pair = null;
  if (typeof module === "object" && module.exports) { try { pair = require("./charm-nest-pair.js"); } catch (_) { pair = null; } module.exports = factory(() => pair || (root && root.CharmNestPair) || null, () => (root && root.Master) || null); }
  else root.PairRemove = factory(() => root.CharmNestPair || null, () => root.Master || null);
})(typeof self !== "undefined" ? self : this, function (CP, MASTER) {
  "use strict";
  const GONE = new Set(["abandoned", "superseded"]);
  const str = v => (v == null ? "" : String(v));
  const POOL_ID = /^(\d{4,20})_([^_]*)_(\d{1,3})$/;   // receiptId_transactionId_copy (Orders.poolId): the same shape charm-nest-pair.js reads
  const arrOf = v => { const x = typeof v === "function" ? v() : v; if (!x) return []; if (x instanceof Map) return [...x.values()]; return Array.isArray(x) ? x : typeof x[Symbol.iterator] === "function" ? [...x] : []; };

  /** receiptId:transactionId of anything a piece can be read from (a pool id, a pool row, a charm, an Orders row); "" when it has no usable key. */
  function keyOf(x) {
    const P = CP();
    let k = "";
    if (P && typeof P.groupKey === "function") k = P.groupKey(x);
    else if (typeof x === "string") { const m = POOL_ID.exec(x); k = m ? m[1] + ":" + m[2] : ""; }
    else if (x && typeof x === "object") {
      const oi = x.orderInfo || {}, o = x.order || {}, l = x.line || {};
      const rid = x.receiptId != null ? x.receiptId : x.orderId != null ? x.orderId : oi.receiptId != null ? oi.receiptId : o.receiptId;
      const tid = x.transactionId != null ? x.transactionId : oi.transactionId != null ? oi.transactionId : l.transactionId;
      if (rid != null && rid !== "") k = str(rid) + ":" + str(tid);
      else { const m = POOL_ID.exec(str(x.poolId || x.id)); if (m) k = m[1] + ":" + m[2]; }
    }
    return /^[^:\s]+:[^:\s]+$/.test(k) ? k : "";   // (a line with no transaction id has no group: it would pull in the whole order)
  }
  // an Orders row has `order` as an object (the order) and no receiptId of its own: read through it. A charm's `order` is the receipt id as a string.
  const keyOfCharm = c => {
    if (!c || typeof c !== "object") return "";
    const oi = c.orderInfo;
    if (oi && oi.receiptId != null) return keyOf({ receiptId: oi.receiptId, transactionId: oi.transactionId });
    return keyOf(c.poolId ? c.poolId : c);
  };
  const keyOfRow = r => (r && r.order && r.order.receiptId != null ? keyOf({ receiptId: r.order.receiptId, transactionId: r.line && r.line.transactionId }) : "");

  /* Only a GROUP is filed (Paul 9 Oct, after ADVCOMPAT 1 and 2): an earring pair, a necklace of counted discs / letters / charms, a line the intake marks multi. The copies of a plain
     quantity-N line each stand alone: a removal naming one takes one, and no pair words are said of them. A piece says it is in a group with groupSize 2 or more (a pool row, a charm,
     an item), or, when it says no size, with an ear (L / R). An Orders row is a group when its pieces say so (groupSize) or its line makes several pieces per unit (isGroupLine). */
  const pieceGrouped = x => {
    if (!x || typeof x !== "object") return false;
    const P = CP();
    if (x.grouped === true || x.grouped === false) return x.grouped;
    if (P && typeof P.inGroup === "function") return P.inGroup(x);
    const n = +x.groupSize; return Number.isFinite(n) && n > 0 ? n >= 2 : x.side === "L" || x.side === "R";
  };
  const rowGrouped = r => {
    if (!r || typeof r !== "object") return false;
    if (+r.groupSize >= 2) return true;
    const P = CP(); if (!P || typeof P.isGroupLine !== "function") return false;
    try { const spec = r.spec || {}, line = r.line || {}; return !!P.isGroupLine({ spec, quantity: Math.max(1, Math.round(+spec.quantity || +line.quantity || 1)), sku: spec.designSku || line.sku || "", form: spec.form || "", poolIds: arrOf(r.poolIds).filter(Boolean) }); } catch (_) { return false; }
  };
  /** The pieces the page knows of, filed by group: { byKey: Map(groupKey → Set(piece ids)), idKey: Map(id → groupKey) }. Built once from the sources (rows, pools,
   *  charms; see the top); pass the result to expand / partnersOutside / splitBy instead of the sources when many questions are asked of the same state. */
  function index(src) {
    if (src && src.byKey instanceof Map && src.idKey instanceof Map) return src;
    src = src || {};
    const byKey = new Map(), idKey = new Map();
    const note = (k, id) => { if (!k || !id) return; let set = byKey.get(k); if (!set) byKey.set(k, set = new Set()); set.add(id); if (!idKey.has(id)) idKey.set(id, k); };
    for (const r of arrOf(src.rows)) { if (!rowGrouped(r)) continue; const k = keyOfRow(r); for (const id of arrOf(r && r.poolIds)) { const x = str(id); if (x) note(k || keyOf(x), x); } }
    for (const p of arrOf(src.pools)) { if (!p || !p.poolId || GONE.has(p.state) || !pieceGrouped(p)) continue; const x = str(p.poolId); note(keyOf(p) || keyOf(x), x); }
    for (const c0 of arrOf(src.charms)) { const c = (c0 && c0.charm) || c0; if (!c || !c.poolId || !pieceGrouped(c)) continue; const x = str(c.poolId); note(keyOfCharm(c), x); }
    return { byKey, idKey };
  }

  /** Every piece id of every group the given ids touch: { ids: Set, added: Set (members that were not asked for), groups: Map(key → Set(ids)) }.
   *  A group is found by the key of an id (its pool id, an Orders row that lists it, its pool row, its charm on a sheet). */
  function expand(ids, src) {
    const idx = index(src), want = new Set(arrOf(ids).map(str).filter(Boolean)), out = new Set(want), groups = new Map(), added = new Set();
    for (const id of want) {
      const k = idx.idKey.get(id) || keyOf(id); if (!k) continue;
      if (!groups.has(k)) groups.set(k, new Set(idx.byKey.get(k) || []));
      groups.get(k).add(id);
    }
    for (const set of groups.values()) for (const id of set) if (!out.has(id)) { out.add(id); added.add(id); }
    return { ids: out, added, groups };
  }

  /** The members of the groups `ids` touch that are NOT in `ids`: [{ id, groupKey, where }]. whereOf(id) says where each one is (a plain
   *  sheet label, "" when on no sheet); a member the page cannot place is "" too. Gone pieces (abandoned, superseded) are not members. */
  function partnersOutside(ids, src, whereOf) {
    const have = new Set(arrOf(ids).map(str)), ex = expand(have, src), out = [];
    for (const [k, set] of ex.groups) for (const id of set) if (!have.has(id)) out.push({ id, groupKey: k, where: typeof whereOf === "function" ? str(whereOf(id)) : "" });
    return out;
  }

  /** Does this design (a SKU) draw two different bodies, a mismatched pair, by the master's `pair` field? false when the master or CharmNestPair is not here. */
  function isMismatchedSku(sku) {
    try { const P = CP(), M = MASTER(); return !!(sku && P && M && typeof M.entryFor === "function" && P.isMismatched(M.entryFor(sku))); } catch (_) { return false; }
  }

  /* ── sides and kinds ── */
  const sideLabel = s => (s === "L" ? "Left" : s === "R" ? "Right" : "");
  const sideWord = s => (s === "L" ? "left earring" : s === "R" ? "right earring" : "");
  /** The side of a piece: its own `side` (every piece of an earring pair has one: Paul, 9 Oct 18:47, a left and a right, matching or not), else (a mismatched design, or a row
   *  of earrings that says it is one of two or more) from its copy number as the pieces are made (copy 1 left, copy 2 right, 3 left ...). Any other piece: null. */
  function sideOfPiece(p, mismatched) {
    if (!p || typeof p !== "object") return null;
    if (p.side === "L" || p.side === "R") return p.side;
    if (!mismatched && !(PAIR_FORMS.has(str(p.form).toLowerCase()) && +p.groupSize >= 2)) return null;
    const copy = +p.copy || (() => { const m = POOL_ID.exec(str(p.poolId || p.id)); return m ? +m[3] : 0; })();
    return copy > 0 ? (copy % 2 === 1 ? "L" : "R") : null;
  }
  const PAIR_FORMS = new Set(["earrings", "earring", "stud", "studs", "hoop", "hoops", "huggie", "huggies", "pair"]);
  /** single | pair | mismatched | multi, from the group's pieces ([{ side, form? }]). "pair" is a pair of EARRINGS (the pieces say their form is earrings,
   *  studs, hoops or huggies); two copies of a pendant are "multi", and so are n discs: the words then say "pieces", never "pair". */
  function kindOfPieces(list) {
    list = arrOf(list);
    const n = list.length;
    if (n < 2) return "single";
    // every earring of a pair carries its side: as many left as right (a pair, or several pairs of one line) is a pair of earrings; "mismatched" when the pieces say it is
    const sides = list.map(p => p && p.side).filter(Boolean), nl = sides.filter(x => x === "L").length, nr = sides.filter(x => x === "R").length;
    if (n % 2 === 0 && nl === n / 2 && nr === n / 2) return list.some(p => p && p.mismatched) ? "mismatched" : "pair";
    const forms = list.map(p => str(p && p.form).toLowerCase()).filter(Boolean);
    if (n === 2 && forms.length === 2 && forms.every(f => PAIR_FORMS.has(f))) return "pair";
    return "multi";
  }

  /** Items: [{ id, groupKey, side, form?, where }] (where: the sheet's plain label or ""). The groups with more than one piece, each as
   *  { key, kind, size, items, sheets:[label], text, phrase }:
   *    phrase  "the left earring on GF Sheet 1 and the right earring on GF Sheet 3" · "the left and right earrings on GF Sheet 2" · "2 left and 2 right earrings on GF Sheet 1" (several pairs in a line)
   *            · "3 pieces: 2 on GF Sheet 1 and 1 on GF Sheet 2" (discs, copies)
   *    text    "Its pair: " + phrase + "." (two earrings), "Its 4 earrings: " + phrase + "." (more); a pair always says it; several copies or discs only when they sit on more than one sheet: "" otherwise
   *  A group of one piece is left out, so an order whose groups are all single gets no new words at all. */
  function groupsOf(items) {
    const by = new Map();
    for (const it of arrOf(items)) { if (!it) continue; const k = pieceGrouped(it) ? (it.groupKey || keyOf(it.id) || ("alone:" + str(it.id))) : ("alone:" + str(it.id)); if (!by.has(k)) by.set(k, []); by.get(k).push(it); }
    const join = l => l.length < 2 ? l.join("") : l.length === 2 ? l.join(" and ") : l.slice(0, -1).join(", ") + " and " + l[l.length - 1];
    const out = [];
    for (const [key, list] of by) {
      if (list.length < 2) continue;
      const kind = kindOfPieces(list), sheets = [...new Set(list.map(i => i.where).filter(Boolean))], at = i => i.where || "no sheet";
      let phrase, text = "";
      const sided = list.every(i => i.side === "L" || i.side === "R");
      if ((kind === "mismatched" || kind === "pair") && sided) {
        // earrings: every piece is a left or a right. Two pieces: "the left earring on A and the right earring on B"; more (several pairs in a line): counts per sheet
        if (list.length === 2) {
          const l = list.find(i => i.side === "L"), r = list.find(i => i.side === "R");
          phrase = at(l) === at(r) ? `the left and right earrings on ${at(l)}` : `the left earring on ${at(l)} and the right earring on ${at(r)}`;
          text = `Its pair: ${phrase}.`;
        } else {
          const per = new Map(); for (const i of list) { if (!per.has(at(i))) per.set(at(i), []); per.get(at(i)).push(i); }
          const part = its => { const nl = its.filter(i => i.side === "L").length, nr = its.length - nl; return `${[nl ? nl + " left" : "", nr ? nr + " right" : ""].filter(Boolean).join(" and ")} earring${its.length === 1 ? "" : "s"}`; };
          phrase = join([...per].map(([w, its]) => `${part(its)} on ${w}`));
          text = `Its ${list.length} earrings: ${phrase}.`;
        }
      } else if (kind === "pair" || kind === "mismatched") {
        phrase = new Set(list.map(at)).size <= 1 ? `both pieces on ${at(list[0])}` : `one piece on ${at(list[0])} and one on ${at(list[1])}`;
        text = `Its pair: ${phrase}.`;
      } else {
        const per = new Map(); for (const i of list) per.set(at(i), (per.get(at(i)) || 0) + 1);
        phrase = `${list.length} pieces: ${join([...per].map(([w, n]) => `${n} on ${w}`))}`;
        if (per.size > 1) text = `Its ${phrase}.`;
      }
      out.push({ key, kind, size: list.length, items: list, sheets, phrase, text });
    }
    return out;
  }
  /** Plain sentences for a removal: [] when no group has anything to say. (The popups add them after their own first sentence.) */
  const describe = items => groupsOf(items).map(g => g.text).filter(Boolean);

  /** A short label for a piece in a list: the design name with its side for a mismatched pair ("MITTENS · left earring"), else the old "name · 1 of 2" text. */
  function pieceLabel(name, side, copy, qty) {
    const w = sideWord(side);
    if (w) return `${name} · ${w}`;
    return +qty > 1 && +copy > 0 ? `${name} · ${+copy} of ${+qty}` : name;
  }

  /** Items for a list of piece ids, from the sources: side from the pool row, where from whereOf(id). mismatched(id) says a design draws two bodies. */
  function itemsFor(ids, src, whereOf, mismatched) {
    const pools = new Map(); for (const p of arrOf((src || {}).pools)) if (p && p.poolId) pools.set(str(p.poolId), p);
    return arrOf(ids).map(str).filter(Boolean).map(id => {
      const p = pools.get(id) || { poolId: id };
      const mis = typeof mismatched === "function" ? !!mismatched(id, p) : !!mismatched;
      return Object.assign({ id, groupKey: keyOf(p) || keyOf(id), side: sideOfPiece(p, mis), form: str(p.form), where: typeof whereOf === "function" ? str(whereOf(id)) : "" }, +p.groupSize > 0 ? { groupSize: +p.groupSize } : null, mis ? { mismatched: true } : null);
    });
  }

  /** What a cancel record keeps of one Orders row: { pieces, kind }. pieces = how many pieces the line made (its piece ids while it has them, else what the line
   *  would make: CharmNestPair.pieceCountOf with the design's master entry, so a mismatched pair counts two); kind single | pair | mismatched | multi (a "pair" is a
   *  pair of earrings; two copies of a pendant are "multi"). ctx.entryFor(sku) gives the master entry (its `pair` says mismatched). Without CharmNestPair: pieces
   *  from the ids or the quantity, kind "" (nothing is claimed). Never throws. */
  function lineInfo(row, ctx) {
    try {
      row = row || {}; ctx = ctx || {};
      const spec = row.spec || {}, line = row.line || {}, ids = arrOf(row.poolIds).filter(Boolean), P = CP();
      const q = Math.max(1, Math.round(+spec.quantity || +line.quantity || 1));
      if (!P || typeof P.pieceCountOf !== "function") return { pieces: ids.length || q, kind: "" };
      const sku = spec.designSku || line.sku || "", entry = typeof ctx.entryFor === "function" ? ctx.entryFor(sku) : (MASTER() && typeof MASTER().entryFor === "function" ? MASTER().entryFor(sku) : null);
      const like = { spec, quantity: q, title: line.title, sku, variations: line.variations, poolIds: ids };
      const pieces = P.pieceCountOf(like, entry || undefined) || ids.length || q;
      let kind = P.kindOf(like, entry || undefined);
      if (kind === "pair" && !PAIR_FORMS.has(str(spec.form).toLowerCase())) kind = "multi";   // two copies of a pendant are not a pair of earrings
      return { pieces, kind };
    } catch (_) { return { pieces: 1, kind: "" }; }
  }

  /** The groups of `ids` split by a removal that does not take all of a group: { key, inside:[id], outside:[{id, where}] } for each group with a member in ids and a member out. */
  function splitBy(ids, src, whereOf) {
    const have = new Set(arrOf(ids).map(str)), ex = expand(have, src), out = [];
    for (const [k, set] of ex.groups) {
      const inside = [...set].filter(id => have.has(id)), outside = [...set].filter(id => !have.has(id)).map(id => ({ id, where: typeof whereOf === "function" ? str(whereOf(id)) : "" }));
      if (inside.length && outside.length) out.push({ key: k, inside, outside });
    }
    return out;
  }

  /** Plain words for the groups a deleted sheet split (the server's answer `splits`: [{ groupKey, here, of }], pieces that were on the sheet and pieces the group has):
   *  "Order 4190000009: 1 of its 2 pieces was on that sheet and is on no sheet now; the rest stay on their sheets." [] when none. */
  function deletedSplits(splits) {
    return arrOf(splits).filter(x => x && x.groupKey && +x.of > +x.here).slice(0, 10).map(x => {
      const rid = str(x.groupKey).split(":")[0], here = +x.here || 0, rest = (+x.of || 0) - here;
      return `Order ${rid}: ${here} of its ${x.of} pieces ${here === 1 ? "was" : "were"} on that sheet and ${here === 1 ? "is" : "are"} on no sheet now; the other ${rest === 1 ? "one stays" : rest + " stay"} on ${rest === 1 ? "its" : "their"} sheet${rest === 1 ? "" : "s"}.`;
    });
  }

  return { GONE, pieceGrouped, rowGrouped, deletedSplits, keyOf, keyOfCharm, keyOfRow, index, expand, isMismatchedSku, partnersOutside, splitBy, lineInfo, sideLabel, sideWord, sideOfPiece, kindOfPieces, groupsOf, describe, pieceLabel, itemsFor };
});
