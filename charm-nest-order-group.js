/* charm-nest-order-group.js — ONE ORDER, ONE ROW in the Orders tab (Paul, 13 Oct 2026, a necklace of two metals drawn as two separate order rows, each with its own number, time,
 * personalisation, pictures and Send to Review: "This is the same order and should be shown as one order with 2 pieces").
 *
 * The Orders tab draws one row per LINE (an Etsy transaction): an order of two lines (one Silver, one Gold) was two rows. This file holds what the tab needs to draw them as ONE card
 * that is not the tab's own business, so the rules can be tested alone:
 *
 *   CharmNestOrderGroup.groupRows(rows, lineIndex)   rows (the list's lines, already filtered and sorted) → [{ rid, rows }] in the order the list shows them: an order stands where its FIRST
 *                                                     line stood, its lines in the order's own line order (lineIndex(row): the line's place in the order, as the order window lists its pieces).
 *                                                     A line with no order number is its own group. A group of one is drawn exactly as a row always was.
 *   CharmNestOrderGroup.sameOf(list)                  the one value every entry has, or '' (an empty or differing entry means nothing is shared: it is said on each piece instead)
 *   CharmNestOrderGroup.countWord(n)                  "2 pieces" | "1 piece": the order window's own words for an order's piece count
 *   CharmNestOrderGroup.pieceKeys(group)              the line keys of a group, in order
 * Pure. Loads in the page (window.CharmNestOrderGroup) and in node. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestOrderGroup = api;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const ridOf = r => { const id = r && r.order && r.order.receiptId; return id == null || id === '' ? '' : String(id); };

  function groupRows(rows, lineIndex) {
    const out = [], at = new Map();
    for (const r of Array.isArray(rows) ? rows : []) {
      if (!r) continue;
      const rid = ridOf(r), id = rid || 'line:' + r.key;
      let g = at.get(id);
      if (!g) { g = { rid, rows: [] }; at.set(id, g); out.push(g); }
      g.rows.push(r);
    }
    if (typeof lineIndex === 'function') for (const g of out) if (g.rows.length > 1) {
      const idx = new Map(g.rows.map((r, i) => [r, [Number(lineIndex(r)), i]]));
      g.rows.sort((a, b) => { const x = idx.get(a), y = idx.get(b); return (Number.isFinite(x[0]) && Number.isFinite(y[0]) ? x[0] - y[0] : 0) || x[1] - y[1]; });
    }
    return out;
  }

  /** The one value every entry holds, '' when they differ or an entry is empty. */
  function sameOf(list) {
    const l = (Array.isArray(list) ? list : []).map(x => String(x == null ? '' : x));
    return l.length > 1 && l[0] && l.every(x => x === l[0]) ? l[0] : '';
  }
  const countWord = n => `${n} piece${n === 1 ? '' : 's'}`;
  const pieceKeys = g => (g && g.rows ? g.rows : []).map(r => r.key);
  return { groupRows, sameOf, countWord, pieceKeys, ridOf };
});
