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
 *
 * facts (every field optional; nothing is read from the page here):
 *   { key, name, metal, qty,
 *     cancelled: false | true | { at, by, reason },   the order or this piece is cancelled
 *     hold:      false | true | string | { reason, at, by },   a person's stop is on it (a Hold press: it is off its sheet unless that sheet is cut)
 *     hand:      null | the custom order's own completion record { state, how: 'button'|'print', completedAt, completedBy }   completed by hand
 *     sheets:    [{ id, label, metal, setId, cut, sent, pools }]   the sheets that hold a copy of it now (the sheets' own records first: OrderPieces)
 *     copies, copiesOn: how many copies it has and how many sit on a sheet (omit: all, or none, by `sheets`)
 *     loading, unsure: the sheet records are not read yet / could not be read: nothing is said about a missing sheet
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
 *     sig:     a short string that changes whenever the answer does (every surface repaints on it) }
 *
 * Never throws; never writes anything; synchronous and cheap. */
(function (root, factory) { const api = factory(); if (typeof module === 'object' && module.exports) module.exports = api; else { root.CharmNestPiecePlacement = api; root.PiecePlacement = api.makePage(root); } })(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const STATES = ['sheet', 'waiting', 'hold', 'hand', 'cancelled', 'loading'];
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const str = (v, n) => String(v == null ? '' : v).trim().slice(0, n || 400);
  const WAIT_TEXT = 'Waiting for a sheet';
  const NEXT_TEXT = 'be placed on a sheet';

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
  const sigOf = p => [p.key || '', p.state, p.onSheet ? 1 : 0, p.partial ? 1 : 0, p.sheets.map(s => (s.id || s.label) + (s.cut ? '!' : '')).join('+'), p.text, p.why, p.how || '', p.since || 0, p.wasOn ? (p.wasOn.sheetId || p.wasOn.label) + '@' + (p.wasOn.until || 0) : ''].join('~');

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
    out.sig = sigOf(out);
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
    const holdOfRow = row => row && (row.hold || row.state === 'held') ? { reason: str(typeof row.hold === 'string' ? row.hold : (row.hold && (row.hold.reason || row.hold.text)) || row.reason || '', 200), at: +(row.hold && row.hold.at) || 0, by: str(row.hold && row.hold.by, 80) } : null;
    /** The facts of one line of an order, from the line's row and its copies (OrderPieces entries: records first, live pages next, the pool row only a hint). */
    function factsOf(row, copies, o) {
      const mine = copies || [], sheets = [], seen = new Set();
      for (const c of mine) if (c && c.nested) { const k = c.sheetId || c.sheetLabel; if (!k || seen.has(k)) continue; seen.add(k); sheets.push({ id: c.sheetId || null, label: c.sheetLabel, metal: c.metal, setId: c.setId, cut: c.state === 'cut', pools: [c.poolId || c.key] }); }
      for (const s of sheets) for (const c of mine) if (c && c.nested && (c.sheetId || c.sheetLabel) === (s.id || s.label) && !s.pools.includes(c.poolId || c.key)) s.pools.push(c.poolId || c.key);
      const lineHold = holdOfRow(row) || (!row && mine.some(c => c && c.problem === 'held') ? { reason: (mine.find(c => c.problem === 'held') || {}).why || '' } : null);
      const sk = mine[0] || {}, hand = !lineHold && row ? handOfRow(row) : (!lineHold && mine.length && mine.every(c => c && c.hand) ? mine[0].hand : null);
      return { key: row ? row.key : sk.lineKey || null, name: sk.label || '', metal: (row && (row.material || (row.spec && row.spec.material))) || sk.metal || null, qty: Math.max(mine.length, 1),
        cancelled: (o && o.cancelled) || (row && row.state === 'gone') || (!row && mine.length > 0 && mine.every(c => c && c.gone)), hold: lineHold, hand,
        sheets, copies: Math.max(mine.length, 1), copiesOn: mine.filter(c => c && c.nested).length, loading: mine.some(c => c && c.loading), unsure: mine.some(c => c && c.unsure), why: (mine.find(c => c && !c.nested && c.reason) || {}).reason || '' };
    }
    function ofOrder(orderId) {
      hook();
      const rid = ridOf(orderId), out = { orderId: rid, order: null, pieces: [], byKey: new Map() };
      if (!rid) { out.order = roll([]); return out; }
      let copies = []; try { copies = root.OrderPieces && root.OrderPieces.of ? root.OrderPieces.of(rid) || [] : []; } catch (_) { copies = []; }
      const rows = rowsOf(rid), cx = cancelled(rid), byLine = new Map();
      for (const c of copies) { const k = c.lineKey; (byLine.get(k) || byLine.set(k, []).get(k)).push(c); }
      const keys = rows.length ? rows.map(r => r.key) : [...byLine.keys()];
      for (const k of keys) {
        const p = resolve(factsOf(rows.find(r => r.key === k) || null, byLine.get(k) || [], { cancelled: cx }));
        out.pieces.push(p); out.byKey.set(k, p);
      }
      out.order = roll(out.pieces);
      return out;
    }
    const of = (orderId, lineKey) => { const o = ofOrder(orderId); return lineKey ? o.byKey.get(lineKey) || null : o.order; };
    function ofRow(row) { const rid = row && row.order ? ridOf(row.order.receiptId) : ''; return rid && row.key ? of(rid, row.key) : null; }
    return { of, ofOrder, ofRow, factsOf, subscribe: fn => { hook(); subs.add(fn); return () => subs.delete(fn); }, resolve, roll, history, withHistory, STATES };
  }
  return { resolve, roll, history, withHistory, makePage, STATES, WAIT_TEXT, NEXT_TEXT };
});
