/* ONE answer to "where is this piece now" (Paul, 5 Oct 2026, order window: the hold card said "Not on a sheet yet" for both pieces while their
 * rows said "Waiting · next: Engraved" and "Waiting · next: Laser cut", which a piece that is not on a sheet cannot be: "I can't have issues like this
 * type of simple logic mess up ... there's no mismatch in orders being on sheets, not being on sheets ... anywhere in any application").
 *
 * Why it broke. Two independent sources answered the same question:
 *   1. the CHIPS, the Sheet tab and the Sheet button read the CURRENT placement (OrderPieces: the sheets' own records, the live pages, the hold marker, the
 *      hand record): "not on a sheet" the moment the piece is taken off;
 *   2. the row's six dots, the header rail, the pill, the Timeline's NOW and "next: ..." read the permanent HISTORY (timeline-ui whereOf / derive), where
 *      `step` is a high-water mark (Math.max over every event, `placed` included) that no event ever lowers: a piece placed on a sheet and later taken off
 *      (held, removed, its sheet deleted, its set undone) kept Nested solid and said "next: Laser cut".
 * History is a record of what happened; it is not where the piece is. This module is the one place that turns the facts of NOW into the answer; every
 * surface of the order window reads it (timeline-ui's derive clamps the steps to it, the rows, the hold card, the rail, the pill, the Sheet tab, the
 * Timeline's NOW marker), and any other surface may.
 *
 * THE RULE (stated once; the default Paul approved by not objecting to it, said in every report): the dots, the next step and the words follow the CURRENT
 * state. A piece that is on no sheet now (never placed, released but not yet placed, held, taken off, its sheet deleted, its set undone) has Nested and every
 * later step hollow, and its next step is "be placed on a sheet". The permanent history is NEVER touched: the ON SHEET seal and every other seal stay on the
 * Timeline forever, and nothing here invents or removes one; the Nested step's hover says "Not on a sheet now" and, from the history, "Was on SS Sheet 1 until
 * Paul took it off, 5 Oct 12:44 AM". A piece the history shows past the laser (cut, sorted, welded, assembled, shipped) keeps that history: it is not
 * clamped (a piece cannot un-cut), and its sheet record is not what says it was cut.
 *
 *   PiecePlacement.resolve(facts)            pure (also required by node tests) -> Placement
 *   PiecePlacement.roll(placements)          pure: one Placement for a set of pieces (the order)
 *   PiecePlacement.history(events)           pure: what the permanent timeline says of this piece's placement: { on, wasOn, held, at, by, text }
 *   PiecePlacement.withHistory(place, events)  pure: the same Placement with since / by / wasOn filled from the piece's events
 *   PiecePlacement.of(orderId, lineKey?)     page: the Placement of one piece (a line of the order); the order's roll-up when lineKey is left out
 *   PiecePlacement.ofOrder(orderId)          page: { order: Placement (rolled up), pieces: [Placement], byKey }
 *   PiecePlacement.ofRow(row)                page: the Placement of the piece an order row (the pull's, the order window's) is
 *   PiecePlacement.subscribe(fn)             page: fn() when anything it reads changed (OrderPieces' reads); returns the unsubscribe
 *   PiecePlacement.STATES                    ['sheet','waiting','hold','hand','cancelled','loading']
 *   PiecePlacement.groups(pieces)            pure: the pieces of an order (OrderPieces entries, or a Placement's `parts`) folded into their GROUPS (one order line: receipt:transaction),
 *                                            each { key, n, kind, known, pieces, sheets, on, off, split, setState, hasSides, pairs, ... } (pairs, mismatched pairs, discs; `known`: the pieces said their kind or their ears, it was not guessed from the count)
 *   PiecePlacement.pairWords(group, here)    pure: what a person should read of a group from one sheet's side: { text, lines, away, here, set, sheets }, "Right piece is on RG Sheet 2, in the same set"; `sheets` = the other sheets to link to ({ id, label, key, side })
 *   PiecePlacement.placeWords(group)         pure: the same without a viewpoint: "Left on GF Sheet 1, Right on RG Sheet 2"
 *   PiecePlacement.sideWord(side)            "Left" | "Right" | ""
 *
 * facts (every field optional; nothing is read from the page here):
 *   { key, name, metal, qty,
 *     cancelled: false | true | { at, by, reason },   the order or this piece is cancelled
 *     hold:      false | true | string | { reason, at, by },   a person's stop is on it (a Hold press: it is off its sheet unless that sheet is cut)
 *     hand:      null | the custom order's own completion record { state, how: 'button'|'print', completedAt, completedBy }   completed by hand
 *     sheets:    [{ id, label, metal, setId, cut, sent, pools }]   the sheets that hold a copy of it now (the sheets' own records first: OrderPieces)
 *     copies, copiesOn: how many copies it has and how many sit on a sheet (omit: all, or none, by `sheets`)
 *     loading, unsure: the sheet records are not read yet / could not be read: nothing is said about a missing sheet
 *     parts:     [{ key, side, groupKey, copy, on, sheetId, sheetLabel, setId, cut, hold, hand, gone, loading }] one per piece (copy) of the line, when it has two or more
 *     why:       plain reason it is on no sheet ("it has no SKU")  }
 *
 * Placement (the answer):
 *   { key, name, metal, qty,
 *     state:   'sheet'     on a sheet now (and nothing stops it)
 *              'waiting'   on no sheet, nothing stops it: it waits for the sorter to place it (never placed, or released/taken off and not placed again)
 *              'hold'      a person's stop is on it (onSheet says whether a cut sheet still holds it)
 *              'hand'      completed by hand, by the Complete Order press or by its QR label printed (how: 'button' | 'print'): it needs no sheet
 *              'cancelled' the order or the piece is cancelled
 *              'loading'   the sheet records are not read yet: no surface may say "not on a sheet" (nor on one)
 *     onSheet: true when a sheet record or live page holds it now (so `sheets` is not empty); partial: some copies are on a sheet and some are not;
 *     sheets, copies, copiesOn, more (other sheets than the first),
 *     text:    the row's words: "GF Sheet 1" | "Waiting for a sheet" | "On hold" | "Completed by hand" | "Cancelled" | ""
 *     say:     the same as a sentence: "On GF Sheet 1" | "Waiting for a sheet" | "On hold, off its sheet" ...
 *     why:     the plain reason it is on no sheet ("it is on hold", "it is waiting to be placed", "it was completed by hand and needs no sheet"); '' while on one
 *     next:    the step key the piece waits for: 'sheet' while it is on no sheet (nextText "be placed on a sheet"), else null (its history says what is after the sheet)
 *     fence:   true when the steps from Nested on must be hollow (waiting, or held and on no sheet): timeline-ui's derive clamps to it unless the history is past the laser
 *     floor:   1 when on a sheet (Nested is done at least), else 0
 *     since, by, wasOn   from the history (withHistory): when and by whom it came to be in this state, and the sheet it was taken off ({ label, sheetId, at, until, by, how })
 *     sig:     a short string that changes whenever the answer does (every surface repaints on it)
 *     parts, groups, sides, split, splitGroups, words   (only for a line of two or more pieces; additive: `text` and every field above are the same as without them)
 *              parts   the pieces, normalised; groups  PiecePlacement.groups(parts); sides  { L: part | null, R: part | null } when the pieces know their ear;
 *              split   true when the pieces of the group are on different sheets (or some on a sheet and some on none): the R3 fact;
 *              splitGroups  how many groups are split; words  placeWords of the split group ("Left on GF Sheet 1, Right on RG Sheet 2"), '' when none is
 *              For a line whose pieces know their ear, `say` and `why` name the piece ("Right piece is not on a sheet yet") instead of "1 of 2 copies". }
 *
 * Never throws; never writes anything; synchronous and cheap. */
(function (root, factory) { const api = factory(); if (typeof module === 'object' && module.exports) module.exports = api; else { root.CharmNestPiecePlacement = api; root.PiecePlacement = api.makePage(root); } })(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const STATES = ['sheet', 'waiting', 'hold', 'hand', 'cancelled', 'loading'];
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const str = (v, n) => String(v == null ? '' : v).trim().slice(0, n || 400);
  const WAIT_TEXT = 'Waiting for a sheet';
  const NEXT_TEXT = 'be placed on a sheet';

  // ── pairs, mismatched pairs, discs (Paul, 9 Oct 2026): the pieces of one order line are ONE group, and it must be possible to say plainly where each piece is ──
  // (a piece is a pool id `receipt_transaction_copy`; its group is the line `receipt:transaction` (CharmNestPair.groupKey); its side is "L" | "R" when the design
  //  draws two different bodies, else null. A record that carries none of these still works: the group is read from the pool id or the line key, the side is absent.)
  const SIDE_WORD = { L: 'Left', R: 'Right' };
  const sideWord = s => SIDE_WORD[s] || '';
  const POOL_RE = /^(\d{4,20})_([^_]*)_(\d{1,3})$/, LINE_RE = /^(\d{4,20})_([^_]*)$/;
  function groupKeyOf(p) {
    if (!p) return '';
    if (p.groupKey && String(p.groupKey) !== ':') return String(p.groupKey);
    if (+p.groupSize === 1) return '';                                   // (a piece its line says is alone: no group)
    const m = LINE_RE.exec(String(p.lineKey || '')) || POOL_RE.exec(String(p.poolId || p.key || ''));
    return m ? m[1] + ':' + m[2] : '';
  }
  /** One piece as the group helpers read it, from an OrderPieces entry (`nested`, `sheetLabel`) or from a Placement's `parts` (`on`). */
  function partOf(p) {
    p = p || {};
    return { key: str(p.key != null ? p.key : p.poolId, 80), gk: groupKeyOf(p), side: p.side === 'L' || p.side === 'R' ? p.side : null, copy: +p.copy || 0, kind: str(p.kind, 12),
      on: p.on != null ? !!p.on : !!p.nested, sheetId: p.sheetId || null, sheetLabel: str(p.sheetLabel, 80), setId: p.setId || null, cut: !!p.cut || p.state === 'cut',
      hold: !!p.hold || p.problem === 'held', hand: !!p.hand, gone: !!p.gone, loading: !!p.loading };
  }
  const placeKey = p => p.sheetId || p.sheetLabel || '';
  function describeGroup(key, ps) {
    const n = ps.length, hasSides = ps.some(p => p.side), sheets = [], seen = new Map();
    for (const p of ps) if (p.on && placeKey(p)) {
      let s = seen.get(placeKey(p)); if (!s) { s = { id: p.sheetId, label: p.sheetLabel || 'a sheet', setId: p.setId || null, keys: [], sides: [] }; seen.set(placeKey(p), s); sheets.push(s); }
      s.keys.push(p.key); if (p.side) s.sides.push(p.side);
    }
    const on = ps.filter(p => p.on), off = ps.filter(p => !p.on && !p.hand && !p.loading);
    const told = ps.find(p => p.kind); const kind = n < 2 ? 'single' : told ? told.kind : hasSides ? 'pair' : n === 2 ? 'pair' : 'multi';   // (amendment 2: a piece that knows its ear belongs to an earring pair, matching or mismatched)
    const lefts = ps.filter(p => p.side === 'L').length, rights = ps.filter(p => p.side === 'R').length;
    const split = n >= 2 && (sheets.length > 1 || (sheets.length >= 1 && off.length > 0));
    // do the sheets a group sits on share a set? (R3: "an order split across sheets says whether the sheets are in one set")
    let setState = ''; if (sheets.length > 1) { const ids = sheets.map(s => s.setId); setState = ids.every(x => x && x === ids[0]) ? 'same' : ids.every(x => !x) ? 'none' : 'other'; }
    return { key, n, kind, known: !!told || hasSides, hasSides, pieces: ps, sheets, on, off, hold: ps.filter(p => p.hold), split, setState, lefts, rights, pairs: (told || hasSides) && (kind === 'pair' || kind === 'mismatched') ? (hasSides ? Math.min(lefts, rights) : Math.floor(n / 2)) : 0, halves: (told || hasSides) && (kind === 'pair' || kind === 'mismatched') ? (hasSides ? Math.abs(lefts - rights) : n % 2) : 0 };
  }
  /** The groups of a list of pieces (see the header). A piece with no group is its own group of one. Pure. */
  function groups(list) {
    const by = new Map();
    for (const raw of Array.isArray(list) ? list : []) {
      if (!raw) continue; const p = partOf(raw); if (p.gone && !p.on) continue;
      const k = p.gk || 'alone:' + p.key; (by.get(k) || by.set(k, []).get(k)).push(p);
    }
    return [...by].map(([k, ps]) => describeGroup(k, ps.map((p, i) => [p, i]).sort((a, b) => (a[0].copy || 0) - (b[0].copy || 0) || a[1] - b[1]).map(x => x[0])));
  }
  const capital = t => t ? t[0].toUpperCase() + t.slice(1) : t;
  const pieceName = (g, p) => sideWord(p.side) ? sideWord(p.side) + ' piece' : g.n === 2 ? 'The other piece' : 'A piece';
  const where = p => p.hold && !p.on ? 'on hold' : p.on ? 'on ' + (p.sheetLabel || 'a sheet') : p.hand ? 'completed by hand' : p.loading ? 'being read' : 'not on a sheet yet';
  /** What to say of a group standing on one sheet (hereSheetId): the pieces that are NOT on it, each told plainly, with whether their sheet shares a set with this one.
   *  { text, lines, away: [part], here: [part], set: 'same' | 'other' | 'none' | '' }. A group wholly here says nothing (text ''). Pure. */
  function pairWords(g, hereSheetId) {
    const out = { text: '', lines: [], away: [], here: [], set: '', sheets: [] };
    if (!g || g.n < 2) return out;
    const here = g.pieces.filter(p => p.on && hereSheetId && p.sheetId === hereSheetId), away = g.pieces.filter(p => !here.includes(p) && !p.hand && !p.loading);   // (a piece whose sheets are not read yet is neither here nor away: nothing is said of it)
    out.here = here; out.away = away; if (!away.length) return out;
    for (const p of away) if (p.on && p.sheetId && !out.sheets.some(x => x.id === p.sheetId)) out.sheets.push({ id: p.sheetId, label: p.sheetLabel || 'a sheet', key: p.key, side: p.side });   // (the other sheets to open, one link each)
    const mine = g.sheets.find(s => s.id && s.id === hereSheetId), shares = s => !mine ? '' : s.id === mine.id ? '' : mine.setId && s.setId === mine.setId ? 'same' : !mine.setId && !s.setId ? 'none' : 'other';
    const SET = { same: ', in the same set', other: ', in another set', none: ', in no set' };
    if (g.n === 2 || g.hasSides) {
      for (const p of away) {
        const s = p.on && g.sheets.find(x => placeKey(p) === (x.id || x.label)), rel = s ? shares(s) : '';
        out.lines.push(`${pieceName(g, p)} is ${where(p)}${rel ? SET[rel] : ''}`);
        if (rel && !out.set) out.set = rel;
      }
    } else {
      // (a group of three or more: told by place, "2 of its 3 pieces are on RG Sheet 2")
      const byPlace = new Map(); for (const p of away) { const k = p.on ? placeKey(p) : p.hold ? '~hold' : '~off'; (byPlace.get(k) || byPlace.set(k, []).get(k)).push(p); }
      for (const [k, ps] of byPlace) {
        const s = ps[0].on && g.sheets.find(x => k === (x.id || x.label)), rel = s ? shares(s) : '';
        out.lines.push(`${ps.length === 1 ? '1' : ps.length} of its ${g.n} pieces ${ps.length === 1 ? 'is' : 'are'} ${where(ps[0])}${rel ? SET[rel] : ''}`);
        if (rel && !out.set) out.set = rel;
      }
    }
    out.text = out.lines.map(capital).join('. ') + '.';
    return out;
  }
  /** The same group told with no sheet in mind: "Left on GF Sheet 1, Right on RG Sheet 2" (a pair with no sides: "one piece on GF Sheet 1, one on RG Sheet 2"). Pure. */
  function placeWords(g) {
    if (!g || g.n < 2) return '';
    if (g.hasSides || g.n === 2) return g.pieces.map(p => (sideWord(p.side) || (p === g.pieces[0] ? 'One piece' : 'the other')) + ' ' + (p.on ? 'on ' + (p.sheetLabel || 'a sheet') : p.hold ? 'on hold' : p.hand ? 'completed by hand' : 'not on a sheet yet')).join(', ').replace(/^([a-z])/, c => c.toUpperCase());
    const by = new Map(); for (const p of g.pieces) { const k = where(p); by.set(k, (by.get(k) || 0) + 1); }
    return [...by].map(([k, c]) => `${c} ${k}`).join(', ');
  }

  /** What stands on ONE place (a sheet, or the sheets of a set), counted from the groups of the orders on it (PiecePlacement.groups of each WHOLE order, so a pair knows its other piece is elsewhere):
   *  { pieces, pairs, halves }. A pair counts when both its ears are on the place; one ear whose other is elsewhere is a half pair. A group that is no earring pair (discs, a single) counts only its pieces. Pure. */
  function sheetCounts(gs, sheetIds) {
    const ids = new Set([].concat(sheetIds == null ? [] : sheetIds).filter(Boolean)), out = { pieces: 0, pairs: 0, halves: 0 };
    for (const g of Array.isArray(gs) ? gs : []) {
      const mine = g.pieces.filter(p => p.on && ids.has(p.sheetId)); if (!mine.length) continue;
      out.pieces += mine.length;
      if (!(g.kind === 'pair' || g.kind === 'mismatched') || !g.known) continue;
      if (g.hasSides) { const l = mine.filter(p => p.side === 'L').length, r = mine.filter(p => p.side === 'R').length; out.pairs += Math.min(l, r); out.halves += Math.abs(l - r) + (mine.length - l - r > 0 ? 1 : 0); }   // (an ear is a pair only with its other ear on the same place)
      else if (mine.length === g.n) out.pairs += Math.floor(g.n / 2); else out.halves += 1;
    }
    return out;
  }
  /** "24 pieces, 10 pairs, 3 half pairs" (pairs and half pairs only when there are some); '' for nothing. Pure. */
  function countWords(c) {
    if (!c || !c.pieces) return '';
    const n = (k, one, many) => `${k} ${k === 1 ? one : many}`;
    return [n(c.pieces, 'piece', 'pieces'), c.pairs ? n(c.pairs, 'pair', 'pairs') : '', c.halves ? n(c.halves, 'half pair', 'half pairs') : ''].filter(Boolean).join(', ');
  }

  function sheetsOf(list) {
    const out = [], seen = new Set();
    for (const s of Array.isArray(list) ? list : []) {
      if (!s) continue;
      const id = s.id || s.sheetId || null, label = str(s.label || s.sheetLabel || (s.metal && s.n ? `${CODE[s.metal] || ''} Sheet ${s.n}`.trim() : '') || (s.page ? String(s.page) : ''), 80), k = id || label || ('#' + out.length);
      if (seen.has(k)) continue; seen.add(k);
      out.push({ id, label, metal: s.metal || null, setId: s.setId || null, cut: !!s.cut, sent: !!s.sent, pools: Array.isArray(s.pools) ? s.pools.slice() : [] });
    }
    return out;
  }
  const holdOf = h => {
    if (!h) return null;
    if (typeof h === 'string') return { reason: str(h, 200), at: 0, by: '' };
    if (typeof h === 'object') return { reason: str(h.reason || h.text || h.why || '', 200), at: +h.at || 0, by: str(h.by, 80) };
    return { reason: '', at: 0, by: '' };
  };
  const sigOf = p => [p.key || '', p.state, p.onSheet ? 1 : 0, p.partial ? 1 : 0, p.sheets.map(s => (s.id || s.label) + (s.cut ? '!' : '')).join('+'), p.text, p.why, p.how || '', p.since || 0, p.wasOn ? (p.wasOn.sheetId || p.wasOn.label) + '@' + (p.wasOn.until || 0) : ''].join('~') + (p.pairSig ? '~' + p.pairSig : '');

  /** The answer for a piece a person's stop is on (a hold takes it off its sheet; a sheet that is already cut keeps it, and then it is both: held, and physically there). */
  function holdAnswer(hold, onSheet, on) {
    const reason = str(hold.reason, 200).replace(/^(on )?hold\s*[:·-]\s*/i, '');
    return { state: 'hold', text: onSheet ? on : 'On hold', say: onSheet ? `On hold, still on ${on}` : 'On hold, off its sheet', reason: /^(hold|on hold|held)$/i.test(reason) ? '' : reason, why: 'it is on hold' + (reason && !/^(hold|on hold|held)$/i.test(reason) ? ': ' + reason : ''),
      next: onSheet ? null : 'sheet', nextText: onSheet ? '' : `be released, then ${NEXT_TEXT}`, fence: !onSheet, floor: onSheet ? 1 : 0, since: hold.at, by: hold.by };
  }

  /** The answer for one piece, from the facts of now. */
  function resolve(f) {
    f = f && typeof f === 'object' ? f : {};
    const sheets = sheetsOf(f.sheets), copies = Math.max(1, Math.round(+f.copies || +f.qty || 1));
    const copiesOn = sheets.length ? Math.min(copies, Math.max(1, Math.round(f.copiesOn != null ? +f.copiesOn : copies))) : 0;
    const onSheet = sheets.length > 0, partial = onSheet && copiesOn < copies;
    const hold = holdOf(f.hold), hand = f.hand && typeof f.hand === 'object' ? f.hand : null, cx = f.cancelled ? (typeof f.cancelled === 'object' ? f.cancelled : {}) : null;
    const loading = !!f.loading && !onSheet && !hold && !hand && !cx, unsure = !!f.unsure && !onSheet && !hold && !hand && !cx;
    const base = { key: f.key == null ? null : f.key, name: str(f.name, 60), metal: f.metal || null, qty: copies, sheets, copies, copiesOn, onSheet, partial, more: Math.max(0, sheets.length - 1), how: '', since: 0, by: '', wasOn: null };
    const first = sheets[0] || null, on = first ? first.label || 'a sheet' : '';
    let p;
    if (cx) {
      // (a cancelled piece may still sit on a cut sheet: it is still said to be cancelled)
      p = { state: 'cancelled', text: 'Cancelled', say: 'Cancelled' + (onSheet ? `, still on ${on}` : ''), why: 'it was cancelled', next: null, nextText: '', fence: false, floor: 0, since: +cx.at || 0, by: str(cx.by, 80) };
    } else if (hold) {
      p = holdAnswer(hold, onSheet, on);
    } else if (hand) {
      const how = hand.how === 'button' ? 'button' : 'print';
      p = { state: 'hand', text: 'Completed by hand', say: how === 'button' ? 'Completed by hand with Complete Order' : 'Completed by hand, its QR label printed', why: 'it was completed by hand and needs no sheet', next: null, nextText: '', fence: false, floor: 0, how, since: +hand.completedAt || 0, by: str(hand.completedBy, 80) };
    } else if (onSheet) {
      p = { state: 'sheet', text: on, say: `On ${on}` + (sheets.length > 1 ? ` and ${sheets.length - 1} more` : '') + (partial ? `; ${copies - copiesOn} of ${copies} copies not on a sheet yet` : ''), why: partial ? `${copies - copiesOn} of ${copies} copies are not on a sheet yet` : '', next: partial ? 'sheet' : null, nextText: partial ? NEXT_TEXT : '', fence: false, floor: 1 };
    } else if (loading || unsure) {
      p = { state: 'loading', text: '', say: unsure ? 'Its sheets could not be read just now' : 'Reading its sheets', why: '', next: null, nextText: '', fence: false, floor: 0, unsure };
    } else {
      p = { state: 'waiting', text: WAIT_TEXT, say: WAIT_TEXT, why: str(f.why, 200) || 'it is waiting to be placed', next: 'sheet', nextText: NEXT_TEXT, fence: true, floor: 0 };
    }
    const out = Object.assign(base, p, { since: p.since || 0, by: p.by || '' });
    pairFacts(out, f.parts);
    out.sig = sigOf(out);
    return out;
  }
  /** The pair facts of a line of two or more pieces, added to its Placement (nothing existing changes, except the words of a line whose pieces know their ear). */
  function pairFacts(out, rawParts) {
    const parts = Array.isArray(rawParts) ? rawParts.filter(Boolean).map(partOf) : [];
    if (parts.length < 2) return out;
    const gs = groups(parts), bad = gs.filter(g => g.split), sides = { L: null, R: null }, hasSides = parts.some(x => x.side);
    for (const x of parts) if (x.side && !sides[x.side]) sides[x.side] = x;
    out.parts = parts; out.groups = gs; out.split = bad.length > 0; out.splitGroups = bad.length; out.words = bad.length ? placeWords(bad[0]) : '';
    if (hasSides) out.sides = sides;
    if (hasSides && out.state === 'sheet') {
      // a pair that knows its ears says which ear is where, and which is not on a sheet yet, instead of "1 of 2 copies"
      const off = parts.filter(x => !x.on && !x.hand && !x.loading);
      if (out.partial && off.length) { const w = off.map(x => (sideWord(x.side) || 'A') + ' piece').join(' and '); out.why = `${w} ${off.length === 1 ? 'is' : 'are'} not on a sheet yet`; out.say = `On ${out.sheets[0] ? out.sheets[0].label || 'a sheet' : 'a sheet'}${out.sheets.length > 1 ? ` and ${out.sheets.length - 1} more` : ''}; ${out.why}`; }
      else if (bad.length) out.say = out.words;
    }
    if (out.split || hasSides) out.pairSig = [out.split ? 'x' : '', parts.map(x => (x.side || '-') + (x.on ? ':' + (x.sheetId || x.sheetLabel || '?') : '') + (x.hold ? 'h' : '')).join(',')].join('');
    return out;
  }

  /** The order's one answer from its pieces': cancelled pieces and pieces completed by hand are not what the order waits on. */
  function roll(list) {
    const all = (Array.isArray(list) ? list : []).filter(Boolean), live = all.filter(p => p.state !== 'cancelled' && p.state !== 'hand');
    const n = s => all.filter(p => p.state === s).length;
    const sheets = []; for (const p of all) for (const s of p.sheets) if (!sheets.some(x => (x.id && s.id ? x.id === s.id : x.label === s.label))) sheets.push(s);
    const held = live.filter(p => p.state === 'hold'), waiting = live.filter(p => p.state === 'waiting'), off = live.filter(p => !p.onSheet && p.state !== 'loading'), loading = live.filter(p => p.state === 'loading');
    const counts = { pieces: all.length, sheet: n('sheet'), waiting: n('waiting'), hold: n('hold'), hand: n('hand'), cancelled: n('cancelled'), loading: n('loading'), onSheet: all.filter(p => p.onSheet).length };
    let state, text, why = '', say;
    if (!all.length) { state = 'loading'; text = ''; say = ''; }
    else if (!live.length) { state = counts.cancelled === all.length ? 'cancelled' : 'hand'; text = state === 'cancelled' ? 'Cancelled' : 'Completed by hand'; say = text; }
    else if (held.length) { state = 'hold'; text = 'On hold'; why = held[0].why; say = held.length === live.length ? 'On hold' : `${held.length} of ${live.length} pieces on hold`; }
    else if (waiting.length) { state = 'waiting'; text = live.every(p => !p.onSheet) ? WAIT_TEXT : `${waiting.length} of ${live.length} not on a sheet yet`; why = waiting[0].why; say = text; }
    else if (loading.length && !live.some(p => p.onSheet)) { state = 'loading'; text = ''; say = 'Reading its sheets'; }
    else { state = 'sheet'; text = sheets.map(s => s.label).filter(Boolean).slice(0, 3).join(' + ') || 'On a sheet'; say = 'On ' + text; }
    const out = { key: null, name: '', metal: null, qty: all.reduce((a, p) => a + (p.qty || 1), 0), state, text, say, why, onSheet: live.length > 0 && live.every(p => p.onSheet || p.state === 'loading') && live.some(p => p.onSheet), anyOnSheet: all.some(p => p.onSheet), partial: live.some(p => p.onSheet) && off.length > 0,
      sheets, more: Math.max(0, sheets.length - 1), copies: all.reduce((a, p) => a + p.copies, 0), copiesOn: all.reduce((a, p) => a + p.copiesOn, 0), how: '', since: 0, by: '', wasOn: null,
      next: off.length ? 'sheet' : null, nextText: off.length ? NEXT_TEXT : '', fence: off.length > 0, floor: live.length && live.every(p => p.onSheet) ? 1 : 0, counts, pieces: all };
    // (pairs, mismatched pairs and discs: how many of the order's lines have their pieces on different sheets; only said when there is one)
    const splitN = all.reduce((a, p) => a + (p.splitGroups || 0), 0); if (splitN) { out.splitPairs = splitN; out.words = all.map(p => p.split && p.words).filter(Boolean).join('; '); }
    out.sig = [state, text, why, counts.pieces, all.map(p => p.sig).join('|')].join('~');
    return out;
  }

  // ── the history: what the permanent timeline says of where this piece was (only the sheet events; never a step) ──
  const ON = new Set(['placed', 'moved', 'renested', 'included', 'merged']);
  const PEOPLE_OUT = new Set(['', 'system', 'etsy', 'operator', 'someone']);
  const personOf = e => { const b = str(e && (e.by || (e.data && e.data.by)), 80); return PEOPLE_OUT.has(b.toLowerCase()) ? '' : b; };
  const sheetOfEvent = e => str(e.sheet || (e.data && (e.data.sheet || e.data.sheetName)) || '', 80);
  /** { on: the sheet the last placement event put it on (or null), wasOn: the sheet it was last taken off, held: the hold nobody released (or null), at, by, text }
   *  from one piece's events (any order). Reads only the events about sheets and holds; the timeline itself is never changed. */
  function history(events) {
    const list = (Array.isArray(events) ? events : []).filter(e => e && typeof e === 'object' && e.type).slice().sort((a, b) => (+a.at || 0) - (+b.at || 0));
    let on = null, wasOn = null, held = null, at = 0, by = '', text = '';
    for (const e of list) {
      const t = e.type;
      if (ON.has(t)) { on = { label: sheetOfEvent(e), sheetId: e.sheetId || null, at: +e.at || 0, by: personOf(e) }; at = +e.at || at; by = personOf(e) || by; text = e.text || ''; if (t === 'placed' || t === 'moved' || t === 'renested') wasOn = null; }
      else if (t === 'removed') {
        if (on && (!e.sheetId || !on.sheetId || e.sheetId === on.sheetId)) { wasOn = { label: on.label || sheetOfEvent(e), sheetId: on.sheetId || e.sheetId || null, at: on.at, until: +e.at || 0, by: personOf(e), how: /hold/i.test(str((e.data && e.data.reason) || e.text, 200)) ? 'hold' : 'removed', reason: str((e.data && e.data.reason) || '', 160) }; on = null; }
        else if (!on) wasOn = wasOn || { label: sheetOfEvent(e), sheetId: e.sheetId || null, at: 0, until: +e.at || 0, by: personOf(e), how: /hold/i.test(str((e.data && e.data.reason) || e.text, 200)) ? 'hold' : 'removed', reason: str((e.data && e.data.reason) || '', 160) };
        at = +e.at || at; by = personOf(e) || by; text = e.text || '';
      } else if (t === 'held') { held = { at: +e.at || 0, by: personOf(e), text: str((e.data && e.data.reason) || e.text, 200) }; at = +e.at || at; by = personOf(e) || by; }
      else if (t === 'released' || t === 'restored') { held = null; at = +e.at || at; by = personOf(e) || by; }
    }
    return { on, wasOn, held, at, by, text };
  }
  /** The Placement with when / who / the sheet it was taken off, from this piece's own events. Nothing about the state changes. */
  function withHistory(place, events) {
    if (!place) return place;
    const h = history(events); let out = Object.assign({}, place);
    // (a hold nobody released, in the permanent timeline, on a piece no sheet holds: it is held, whatever the line's own marker still says; the marker, when set, already said so)
    if (out.state === 'waiting' && h.held && !out.onSheet) out = Object.assign(out, holdAnswer({ reason: h.held.text, at: h.held.at, by: h.held.by }, false, ''));
    if (!out.since && (out.state === 'waiting' || out.state === 'hold') && h.at) { out.since = h.at; out.by = out.by || h.by; }
    // (taken off by an event: that; or the history still says it was placed, and no record holds it now: its sheet was deleted or its set undone behind the timeline.
    //  A piece on hold with no event that took it off says its hold alone: the hold is why it is on no sheet, and nothing in the history tells when it came off)
    if (!out.onSheet) out.wasOn = h.wasOn || (h.on && out.state !== 'hold' ? Object.assign({}, h.on, { until: 0, by: '', how: 'gone', reason: '' }) : null);
    out.sig = sigOf(out);
    return out;
  }

  // ── the page's face: reads what the page already holds, learns nothing of its own, never writes ──
  function makePage(root) {
    const ridOf = x => String(x == null ? '' : x).replace(/\D/g, '');
    const subs = new Set();
    let hooked = false;
    const hook = () => { if (hooked) return; const OP = root.OrderPieces; if (!OP || !OP.subscribe) return; hooked = true; OP.subscribe(() => { for (const f of [...subs]) { try { f(); } catch (e) { console.warn('PiecePlacement subscriber', e); } } }); };
    const rowsOf = rid => { try { return (root.Orders && root.Orders.rows ? root.Orders.rows() || [] : []).filter(r => r && r.order && ridOf(r.order.receiptId) === rid && r.state !== 'gone'); } catch (_) { return []; } };
    const cancelled = rid => { try { return !!(root.Cancelled && root.Cancelled.has && root.Cancelled.has(rid)); } catch (_) { return false; } };
    const handOfRow = row => { try { return root.CharmNestReadiness && root.CharmNestReadiness.handOf ? root.CharmNestReadiness.handOf(row) : null; } catch (_) { return null; } };
    // (a row's own hold counts until the cloud has confirmed it (row.holdSeen, set by PlacementFeed.adoptHolds): from then on the cloud's pool rows are the hold's one record, so a Release made on
    //  another computer releases it here too. o.cloudOnly asks what the cloud alone says: the feed uses it to tell a hold the cloud confirms from one still being written)
    const holdOfRow = (row, o) => row && !row.holdSeen && !(o && o.cloudOnly) && (row.hold || row.state === 'held') ? { reason: str(typeof row.hold === 'string' ? row.hold : (row.hold && (row.hold.reason || row.hold.text)) || row.reason || '', 200), at: +(row.hold && row.hold.at) || 0, by: str(row.hold && row.hold.by, 80) } : null;
    /** The facts of one line of an order, from the line's row and its copies (OrderPieces entries: records first, live pages next, the pool row only a hint). */
    function factsOf(row, copies, o) {
      const mine = copies || [], sheets = [], seen = new Set();
      for (const c of mine) if (c && c.nested) { const k = c.sheetId || c.sheetLabel; if (!k || seen.has(k)) continue; seen.add(k); sheets.push({ id: c.sheetId || null, label: c.sheetLabel, metal: c.metal, setId: c.setId, cut: c.state === 'cut', pools: [c.poolId || c.key] }); }
      for (const s of sheets) for (const c of mine) if (c && c.nested && (c.sheetId || c.sheetLabel) === (s.id || s.label) && !s.pools.includes(c.poolId || c.key)) s.pools.push(c.poolId || c.key);
      // (a hold the cloud keeps, a pool row taken off for a hold: OrderPieces says so on the piece as `hold`, so a page whose own row never heard of it (another computer pressed Hold) says it too)
      const cloudHold = mine.find(c => c && c.hold), lineHold = holdOfRow(row, o) || (cloudHold ? { reason: str(cloudHold.hold.reason, 200), at: +cloudHold.hold.at || 0, by: str(cloudHold.hold.by, 80) } : !row && mine.some(c => c && c.problem === 'held') ? { reason: (mine.find(c => c.problem === 'held') || {}).why || '' } : null);
      const sk = mine[0] || {}, hand = !lineHold && row ? handOfRow(row) : (!lineHold && mine.length && mine.every(c => c && c.hand) ? mine[0].hand : null);
      return { key: row ? row.key : sk.lineKey || null, name: sk.label || '', metal: (row && (row.material || (row.spec && row.spec.material))) || sk.metal || null, qty: Math.max(mine.length, 1),
        cancelled: (o && o.cancelled) || (row && row.state === 'gone') || (!row && mine.length > 0 && mine.every(c => c && c.gone)), hold: lineHold, hand,
        parts: mine.length > 1 ? mine.map(c => ({ key: c.poolId || c.key, lineKey: c.lineKey, groupKey: c.groupKey, groupSize: c.groupSize, kind: c.kind, side: c.side, copy: c.copy, on: !!c.nested, sheetId: c.sheetId || null, sheetLabel: c.sheetLabel || '', setId: c.setId || null, cut: c.state === 'cut', hold: !!(c.hold || c.problem === 'held'), hand: !!c.hand, gone: !!c.gone, loading: !!c.loading })) : undefined,
        sheets, copies: Math.max(mine.length, 1), copiesOn: mine.filter(c => c && c.nested).length, loading: mine.some(c => c && c.loading), unsure: mine.some(c => c && c.unsure), why: (mine.find(c => c && !c.nested && c.reason) || {}).reason || '' };
    }
    function ofOrder(orderId, opts) {
      hook();
      const rid = ridOf(orderId), out = { orderId: rid, order: null, pieces: [], byKey: new Map() };
      if (!rid) { out.order = roll([]); return out; }
      let copies = []; try { copies = root.OrderPieces && root.OrderPieces.of ? root.OrderPieces.of(rid) || [] : []; } catch (_) { copies = []; }
      const rows = rowsOf(rid), cx = cancelled(rid), byLine = new Map();
      for (const c of copies) { const k = c.lineKey; (byLine.get(k) || byLine.set(k, []).get(k)).push(c); }
      const keys = rows.length ? rows.map(r => r.key) : [...byLine.keys()];
      for (const k of keys) {
        const p = resolve(factsOf(rows.find(r => r.key === k) || null, byLine.get(k) || [], { cancelled: cx, cloudOnly: !!(opts && opts.cloudOnly) }));
        out.pieces.push(p); out.byKey.set(k, p);
      }
      out.order = roll(out.pieces);
      return out;
    }
    const of = (orderId, lineKey, opts) => { const o = ofOrder(orderId, opts); return lineKey ? o.byKey.get(lineKey) || null : o.order; };
    function ofRow(row, opts) { const rid = row && row.order ? ridOf(row.order.receiptId) : ''; return rid && row.key ? of(rid, row.key, opts) : null; }
    /** The words a sheet's (or a set's) piece count carries on hover: "24 pieces, 10 pairs, 3 half pairs", read from OrderPieces (every order on those sheets, whole, so a half pair is known).
     *  '' when the page cannot count them all (an order's record not read yet) or when nothing is a pair: a card that shows its numbers needs no other words. `shown` are the numbers the card prints
     *  (the words are given only when they count the same pieces). */
    function sheetWords(sheetIds, shown) {
      const OP = root.OrderPieces; if (!OP || typeof OP.onSheet !== 'function' || typeof OP.of !== 'function') return '';
      const ids = [].concat(sheetIds || []).filter(Boolean); if (!ids.length) return '';
      try {
        const rids = new Set(); for (const id of ids) for (const x of OP.onSheet(id) || []) rids.add(x.orderId);
        const gs = []; for (const rid of rids) gs.push(...groups(OP.of(rid) || []));
        const c = sheetCounts(gs, ids);
        if (!c.pairs && !c.halves) return '';
        if (shown != null && ![].concat(shown).some(k => +k === c.pieces)) return '';
        return countWords(c);
      } catch (_) { return ''; }
    }
    return { of, ofOrder, ofRow, factsOf, subscribe: fn => { hook(); subs.add(fn); return () => subs.delete(fn); }, resolve, roll, history, withHistory, groups, pairWords, placeWords, sideWord, sheetCounts, countWords, sheetWords, STATES };
  }
  return { resolve, roll, history, withHistory, groups, pairWords, placeWords, sideWord, sheetCounts, countWords, makePage, STATES, WAIT_TEXT, NEXT_TEXT };
});
