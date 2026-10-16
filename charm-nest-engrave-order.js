/* The ORDER's engraved pieces, in one list (Paul, 13 Oct 2026: "no easy way to tell how many pieces a given order has in the detailed engraving modal ... the back and forward arrow buttons are not working, and I can't
 * cycle through the different charms, even though this order has 2 charms ... every engraved piece needs a dedicated Approve button ... the user must never be allowed to skip pieces in the multipiece order";
 * and, 04:27 UTC: "This is the same order and should be shown as one order with 2 pieces").
 *
 * ORDER-FIRST. An order (the Etsy receipt, `rid`) owns an ordered list of PIECES, whatever their metal or line: an SS necklace and a GF necklace of the same design are two lines of ONE order and two pieces
 * of it. Nothing is stored here and nothing is read from the network: every answer is computed from the order's live lines and the engraving card model (charm-nest-engrave-card.js piecesOf, one Piece per
 * engraved piece: a charm, one ear, one disc, each with its Front and/or Back job). The page's own surfaces (the Engrave detail's header strip and Back / Next, the order window, the guide that says
 * what is left) all read THIS list, so they cannot disagree about how many pieces an order has or which one is open.
 *
 *   piecesOf(rid, o)             [P]    the ENGRAVED pieces of order `rid`, in a fixed order (the order's line order, then slot: Left, Right, Disc 1..n). A line that engraves nothing, an earring ("never
 *                                       engrave") and a face nobody asked for give no entry. P = { pieceKey (<lineKey>[#slot]), rid, lineKey, slot, label ("SS", "GF · Disc 2": metal first when the order has
 *                                       more than one line), title (the card's own title), metal ("SS" | "GF" | ".."), faces (['front','back'] subset, only the faces engraved), jobKeys, state, piece (the card model's Piece) }
 *                                       state: 'to-approve' (a face is in review: the piece has its own Approve) | 'approved' (every engraved face approved) | 'held' (cannot be approved yet: words to confirm,
 *                                       or being prepared; no Approve press is possible) | 'none' (cut plain; only with o.withNone).
 *                                       o: { rows (the order's lines; default: every live line of `rid` from window.Orders.rows()), nameOf, isWorking, seen, stage, withNone }  (nameOf / isWorking / seen as the card's piecesOf)
 *   countOf(rid, o)              { n, approved, left, toApprove, held }       left = n - approved; the order is done when n > 0 and left === 0
 *   indexOf(rid, pieceKey, o)    0-based place in piecesOf, or -1
 *   nextUnapproved(rid, pieceKey, o)  the first piece AFTER pieceKey (wrapping, never pieceKey itself) that is not approved; null when every other piece is approved (or there is no other piece)
 *   step(rid, pieceKey, dir, o)  the neighbouring piece (dir 1 | -1), NO wrap: null past either end (a press that would leave the order)
 *   MONOGRAM (MONOUI, Paul, 16 Oct 2026: "the monogram ... as part of the Engraving Tab ... in place of the currently existing back/front engraving"): a custom MONOGRAM line is a piece of its order too, with no Front and no Back and no Engrave
 *                                job: { pieceKey: the line's key, kind: 'monogram', label ("Monogram", metal first when the order has more lines), faces: [], jobKeys: [], state, piece: { kind: 'monogram', row, faces: {front:null, back:null}, jobs: [] } }.
 *                                Its state is the page's answer for the line (useMonogram(fn) once, or o.monoState(row) per call): null (not a monogram line: nothing is added) | 'to-approve' | 'held' | 'approved'. Its Approve is
 *                                that piece's approval in the order's sequence: countOf, nextUnapproved, step, the strip, the guide and Back / Next treat it like any other piece. Without a function nothing changes.
 *   jobOf(P, face)               the Engrave job of that face of the piece; without `face`: the face still to settle (Front before Back), else the first job (null for a monogram piece)
 *   groupRows(rows, o)           the Engrave tab's queue rows (charm-nest-engrave-rows.js) put in ORDER order: orders by first appearance, each order's rows together in the order's line order. Back / Next walk this
 *                                sequence, so every piece of an order is reached before the next order. o: { ridOf(row), lineIndex(row) }
 *   orderCounts(queueRows, decidedRows, o)   { decided, remaining, n, of, text }   "N of M · K done" counted in ORDERS: an order is done only when none of its pieces is still queued (o.queued: every rid with a piece
 *                                       still in the queue, filters or not). Single-line orders count exactly as rows did.
 *   stripHtml(pieces, key, o)    the piece strip drawn in the Engrave detail's header (only for 2 or more pieces): "Piece 1 of 2" and one small button per piece (number, metal dot, label, a tick when approved), the one
 *                                shown pressed. data-a="piece" data-piece=<pieceKey>. o: { titleOf(P, on) }
 *   metalOf(row)                 "SS" | "GF" | ".." from the line's own metal
 * Pure but for reading the page's lines when o.rows is not given. Loads in the page (window.CharmNestEngraveOrder) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveOrder = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Card = () => dep('CharmNestEngraveCard', './charm-nest-engrave-card.js');
  const Sides = () => dep('CharmNestEngraveSides', './charm-nest-engrave-sides.js');
  const tryDo = (f, d) => { try { return f(); } catch (_) { return d; } };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const STATE = { approve: 'to-approve', approved: 'approved', words: 'held', busy: 'held', plain: 'none' };
  const lineOf = row => (row && row.parentRow) || row || null;
  const ridOfRow = row => { const l = lineOf(row); return String((l && l.order && l.order.receiptId) || ''); };
  const metalOf = row => { const l = lineOf(row) || {}, m = String(l.material || (l.spec && (l.spec.material || l.spec.metal)) || ''); return CODE[m] || (m ? m.toUpperCase().slice(0, 4) : ''); };
  const slotRank = s => (s == null || s === '' ? 0 : s === 'L' ? 1 : s === 'R' ? 2 : /^D(\d+)$/.test(s) ? 10 + +RegExp.$1 : 99);
  const slotWord = s => (s === 'L' ? 'Left' : s === 'R' ? 'Right' : /^D\d+$/.test(s || '') ? 'Disc ' + s.slice(1) : '');
  const faceList = piece => ['front', 'back'].filter(f => piece.faces && piece.faces[f]);
  let monoFn = null;   // the page's answer for a monogram line (null | 'to-approve' | 'held' | 'approved'), set once by the bridge: a module with no page adds no monogram piece
  const useMonogram = fn => { monoFn = typeof fn === 'function' ? fn : null; };
  const MONO_STATE = { 'to-approve': 1, held: 1, approved: 1 };

  /** The live lines of an order, in the order they came. */
  function rowsOf(rid, o) {
    const O = root && root.Orders, all = o && Array.isArray(o.rows) ? o.rows : ((O && O.rows ? tryDo(() => O.rows(), []) : []) || []).filter(r => r && ridOfRow(r) === String(rid));
    const seen = new Set();   // (a row that belongs to a line, a pair's half, is the line's piece: each line once)
    return all.filter(r => { const l = lineOf(r); if (!r || !l || l.state === 'gone' || seen.has(l.key)) return false; seen.add(l.key); return true; });
  }

  function piecesOf(rid, o) {
    o = o || {}; rid = String(rid == null ? '' : rid);
    const C = Card(); if (!C || !C.piecesOf) return [];
    const rows = rowsOf(rid, o), many = new Set(rows.map(r => (lineOf(r) || {}).key)).size > 1, out = [];
    const popt = Object.assign({}, o); delete popt.rows; delete popt.withNone; delete popt.monoState;
    const monoState = o.monoState === undefined ? monoFn : o.monoState;
    rows.forEach((r, li) => {
      const ms = monoState ? tryDo(() => monoState(r), null) : null;
      if (ms && MONO_STATE[ms]) {   // (a monogram line: one piece, no faces, no job)
        const L = lineOf(r), metal = metalOf(L), label = (many ? [metal, 'Monogram'] : ['Monogram']).filter(Boolean).join(' · ');
        out.push({ pieceKey: L.key, rid, lineKey: L.key, slot: null, label, title: 'Monogram', metal, faces: [], jobKeys: [], state: ms, kind: 'monogram', line: li,
          piece: { kind: 'monogram', pieceKey: L.key, rid, lineKey: L.key, slot: null, row: L, title: 'Monogram', faces: { front: null, back: null }, jobs: [], state: ms } });
        return;
      }
      let ps = []; try { ps = C.piecesOf([r], popt) || []; } catch (_) { ps = []; }
      ps = ps.map((p, i) => ({ p, i })).sort((a, b) => slotRank(a.p.slot) - slotRank(b.p.slot) || a.i - b.i).map(x => x.p);
      for (const p of ps) {
        const state = STATE[p.state] || 'held'; if (state === 'none' && !o.withNone) continue;
        const S = Sides(), sps = S && S.setPieceOf ? tryDo(() => S.setPieceOf(p.row || r, p.slot), null) : null;   // (a piece of a set of different designs is "Small bar", not "Disc 2": charm-nest-engrave-sides.js)
        const metal = p.metal || metalOf(p.row || r), sw = sps ? S.pieceName(sps) : slotWord(p.slot);
        const label = (many ? [metal, sw] : [sw || metal]).filter(Boolean).join(' · ') || p.name || p.title || 'Piece';
        out.push({ pieceKey: p.pieceKey, rid: p.rid || rid, lineKey: p.lineKey, slot: p.slot || null, label, title: p.title || '', metal, faces: faceList(p), jobKeys: (p.jobs || []).map(j => j.key), state, piece: p, line: li });
      }
    });
    return out;
  }
  function countOf(rid, o) {
    const ps = piecesOf(rid, o), approved = ps.filter(p => p.state === 'approved').length;
    return { n: ps.length, approved, left: ps.length - approved, toApprove: ps.filter(p => p.state === 'to-approve').length, held: ps.filter(p => p.state === 'held').length };
  }
  const indexOf = (rid, pieceKey, o) => piecesOf(rid, o).findIndex(p => p.pieceKey === pieceKey);
  function nextUnapproved(rid, pieceKey, o) {
    const ps = piecesOf(rid, o), n = ps.length, i = ps.findIndex(p => p.pieceKey === pieceKey);
    for (let k = 1; k <= n; k++) { const p = ps[((i < 0 ? -1 : i) + k) % n]; if (p && p.pieceKey !== pieceKey && p.state !== 'approved') return p; }
    return null;
  }
  function step(rid, pieceKey, dir, o) {
    const ps = piecesOf(rid, o), n = ps.length; if (!n) return null;
    const i = ps.findIndex(p => p.pieceKey === pieceKey);
    if (i < 0) return dir < 0 ? ps[n - 1] : ps[0];
    const j = i + (dir < 0 ? -1 : 1);
    return j < 0 || j >= n ? null : ps[j];
  }
  const OPEN = ['review', 'words', 'blocked', 'classify', 'ready', 'fitting', 'reclassify'];
  function jobOf(P, face) {
    const faces = (P && P.piece && P.piece.faces) || {};
    if (face === 'front' || face === 'back') return (faces[face] && faces[face].job) || null;
    const js = ['front', 'back'].map(f => faces[f] && faces[f].job).filter(Boolean);
    return js.find(j => OPEN.includes(j.state)) || js[0] || null;
  }

  /* ── the queue in order order ── */
  const rowRid = row => ridOfRow(row && row.jobs && row.jobs[0] && row.jobs[0].row);
  function groupRows(rows, o) {
    o = o || {}; const list = (rows || []).slice(), ridOf = o.ridOf || rowRid, lineIx = o.lineIndex || (() => 0);
    const order = [], by = new Map();
    list.forEach((r, i) => { const k = ridOf(r) || ('~' + i); if (!by.has(k)) { by.set(k, []); order.push(k); } by.get(k).push({ r, i }); });
    const out = [];
    for (const k of order) out.push(...by.get(k).sort((a, b) => (lineIx(a.r) - lineIx(b.r)) || (a.i - b.i)).map(x => x.r));
    return out;
  }
  function orderCounts(queueRows, decidedRows, o) {
    o = o || {}; const ridOf = o.ridOf || rowRid;
    const q = new Set((queueRows || []).map(r => ridOf(r))), still = new Set([...q, ...(o.queued || [])]);
    const d = new Set((decidedRows || []).map(r => ridOf(r)).filter(k => !still.has(k)));
    const decided = d.size, remaining = q.size;
    return { decided, remaining, n: decided + 1, of: decided + remaining, text: `${decided + 1} of ${decided + remaining} · ${decided} done` };
  }

  /* ── the strip ── */
  function stripHtml(pieces, key, o) {
    o = o || {}; const ps = pieces || []; if (ps.length < 2) return '';
    const at = ps.findIndex(p => p.pieceKey === key), word = { 'to-approve': 'to approve', approved: 'approved', held: 'words or placement not ready', none: 'no engraving' };
    return `<span class="egPcStrip" role="group" aria-label="The ${ps.length} pieces of this order, one by one" data-n="${ps.length}">`
      + `<span class="egPcOf">${at < 0 ? `${ps.length} pieces` : `Piece ${at + 1} of ${ps.length}`}</span>`
      + ps.map((p, i) => {
        const on = i === at, t = o.titleOf ? o.titleOf(p, on) : `${on ? 'Shown now' : 'Show'}: piece ${i + 1} of ${ps.length}, ${p.label} (${p.kind === 'monogram' && p.state === 'held' ? 'the monogram is being made, or its letters need confirming' : word[p.state] || p.state})`;
        return `<button type="button" class="egPcTab" data-a="piece" data-piece="${esc(p.pieceKey)}" data-state="${esc(p.state)}" aria-pressed="${on}"${on ? ' aria-current="true"' : ''} title="${esc(t)}">`
          + `<b class="egPcNo">${i + 1}</b><i class="egPcDot" data-metal="${esc(p.metal)}" aria-hidden="true"></i><span class="egPcLbl">${esc(p.label)}</span>${p.state === 'approved' ? '<b class="egPcTick" aria-label="approved">✓</b>' : ''}</button>`;
      }).join('') + '</span>';
  }

  return { piecesOf, countOf, indexOf, nextUnapproved, step, jobOf, groupRows, orderCounts, stripHtml, metalOf, rowsOf, useMonogram };
});
