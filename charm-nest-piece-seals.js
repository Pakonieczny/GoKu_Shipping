/* Charm Nest · the seals of a piece's milestones (window.PieceSeals): what the small card over each of a piece's progress dots shows of its seal.
   Paul, 5 Oct 2026: "Enable each of these solid and hollow green dots into an active hover state like the other milestone timeline dots that showcase
   a bit of info regarding each milestone ... If a given step requires a seal, then you can show an unfinished version of that seal. That means the seal
   will be visible but slightly transparent, so it's easy to understand that it's still missing; and on a solid dot, if a seal was present, it would be
   shown in the hover expanded view."

   Which step has which seal (one answer, read from the timeline's own table, OrderTimelineUI.STAGES / faceModel, never a second list):
       Order in   ORDER RECEIVED (arrived)            Nested     ON SHEET (placed, renested)       Engraved  BACK ENGRAVING (engraveApproved)
       Laser cut  LASER CUT / PARTIAL CUT (laserDone, roseCut)   Sorted    SORTED (sorted)          Welded    WELDED (welded)
       Assembled  ASSEMBLED (assembled)               Shipped    SHIPPED / ETSY COMPLETE (shipped, etsyCompleted)
   plus two seals that are not steps of the rail but complete a piece by hand: 'complete' (ORDER COMPLETE) and 'print' (QR LABEL PRINTED). Every other
   thing a piece has (a scan, a packed note, a sheet QR label, a hold) has no seal in the app and gets none here. The whole table, with where each one's
   data comes from, is in plans/order-window-pieces/seal-steps.md.

   render(stepKey, ctx, { done, size, caption }) -> an element (span.pgSeals), or null
       stepKey  a step of the rail: 'arrived' | 'sheet' | 'engraved' | 'laser' | 'sorted' | 'welded' | 'assembled' | 'shipped' (the names the header rail and the
                dots use; 'nested', 'order', 'laserDone', 'roseCut' and the event types work too), or 'complete' | 'print'.
       ctx      { events, piece, pieces?, D?, steps?, cancelled?, context?, rec? } what the order window already holds of the piece:
                events     the order's permanent timeline (the window's W.events, or one piece's events from OrderTimelineUI.summary); an array, [] when it
                           has none, anything else (the timeline not read yet): nothing is claimed and the answer is null
                piece      the piece as the window knows it (W.pieces[i], OrderTimelineUI.summary().each[i].p: { key (the line key), tid, pools, sheets, line });
                           only that piece's events count (OrderTimelineUI.ofPiece), the order's own events (arrived, a station's scan) count for every piece
                pieces     all of the order's pieces (ofPiece needs them to tell which sheet's events are this piece's); D / steps: summary().each[i].D / .steps
                rec        the piece's custom record (B.maps.customDone[key] ...), for 'complete' and 'print' only
       done     true: the dot is solid (the step is done). false: it is hollow. Left out: worked out from the timeline.
       size     the seal's width in px (default 40, 16 to 96)
       caption  true (default): a quiet line beside a real seal: who, where, when ("Marco R. · Sorting · Mon 12:41 PM"); false: the seal alone (the card says it)
   What comes back:
       a step that is DONE and has a recorded seal: the real seal (the very Seal.face every other seal is drawn with, the same .seal element, so Seal.zoom grows
           it in place on a rest, a click, a tap or Tab, exactly as elsewhere), the first one the step has: the one the header rail draws for it (a second sorting
           scan or a re-nesting is the same step: the Timeline draws one seal per step and piece, and so does this; seals() lists them all). Its aria-label says
           what, who, where and when. The Complete Order and QR label seals ('complete', 'print') are different: every press and every reprint is a seal of its
           own, so they come as one seal and "+N" (Seal.compactRow, the order overview's own component).
       a step that is NOT done and still to come ('now' or 'later'): the UNFINISHED seal, Seal.face(..., { ghost: true }): the same seal outline, words and icon,
           dashed, grey and about half transparent, with no date, time or person (they are not known). Not a .seal: no tab stop, nothing to zoom, no click.
       anything else: null. A step with no seal in the app; a done step the timeline holds no event of ("done before the timeline was kept": nothing is invented);
           a step the order will never take (cancelled, completed by hand, a weld on a necklace). A ghost never shows beside a real seal: once the step has one,
           the real one is all there is. A real seal is permanent (Reopen and Undo never take one away, they are not read here).
   ghost(kind, size) -> the unfinished seal alone (kind: a step key as above, or an event type such as 'laserDone'; null for a kind that has no seal)
   has(stepKey) -> whether the step has a seal at all
   seals(stepKey, ctx) -> the recorded seals of the step as data: [{ id, type, at, by, person, station, place, sheet }], oldest first (no DOM)
   info(stepKey, ctx) -> the card's data from the same sources: { key, label, state: done|now|later|stopped|gone|skipped|none, done: [{t, sub}], need: [{kind, t}],
           facts, seals, ghost } (OrderTimelineUI.requirementsOf: what is done and what is still missing, in the shop's words)
   Nothing here writes, fetches or records anything; it only reads what the window was handed. Never throws: a failure answers null (and one console note). */
(function (root) {
  'use strict';
  const doc = root.document;
  if (!doc || root.PieceSeals) return;

  const ZONE = 'America/Toronto', DEFAULT = 40, MIN = 16, MAX = 96;
  const warned = new Set();
  const warn = (what, e) => { if (warned.has(what)) return; warned.add(what); try { console.warn('Piece seals: ' + what, e); } catch (_) {} };
  const hash = s => { let h = 2166136261; for (const c of String(s)) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); } return h >>> 0; };
  const fmt = o => { let f = null; return t => { try { return (f = f || new Intl.DateTimeFormat('en-US', o)).format(new Date(+t)); } catch (_) { return ''; } }; };
  const wk = fmt({ timeZone: ZONE, weekday: 'short' }), tm = fmt({ timeZone: ZONE, hour: 'numeric', minute: '2-digit' }), day = fmt({ timeZone: ZONE, weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' });
  const shortWhen = t => +t > 0 ? `${wk(t)} ${tm(t)}` : '';
  const longWhen = t => +t > 0 ? `${day(t)} · ${tm(t)}` : '';
  const sizeOf = n => Math.max(MIN, Math.min(MAX, Math.round(+n) || DEFAULT));
  const UI = () => root.OrderTimelineUI || null;
  const SEAL = () => root.Seal && typeof root.Seal.face === 'function' ? root.Seal : null;

  /* the one table: a step's own key, the words people and code call it, and the two seals that are not steps */
  const ALIAS = {
    arrived: 'arrived', order: 'arrived', orderin: 'arrived', received: 'arrived', orderreceived: 'arrived',
    sheet: 'sheet', nested: 'sheet', placed: 'sheet', renested: 'sheet', onsheet: 'sheet',
    engraved: 'engraved', engraving: 'engraved', engraveapproved: 'engraved', backengraving: 'engraved',
    laser: 'laser', lasercut: 'laser', laserdone: 'laser', rosecut: 'laser', cut: 'laser',
    sorted: 'sorted', welded: 'welded', weld: 'welded', assembled: 'assembled', assembly: 'assembled',
    shipped: 'shipped', etsycompleted: 'shipped',
    complete: 'complete', ordercomplete: 'complete', sealcompleted: 'complete', completed: 'complete',
    print: 'print', printed: 'print', qrlabelprinted: 'print', sealprinted: 'print', qrlabel: 'print'
  };
  const keyOf = name => ALIAS[String(name == null ? '' : name).toLowerCase().replace(/[^a-z]/g, '')] || '';
  const stageOf = (ui, key) => ui && ui.STAGES ? ui.STAGES.find(s => s.k === key) || null : null;
  const HAND = { complete: 'sealCompleted', print: 'sealPrinted' };
  /** The event type a kind is drawn as (its step's own kind, 'sealCompleted' / 'sealPrinted', or an event type the timeline seals). */
  function typeOf(ui, kind) {
    const k = keyOf(kind);
    if (k && HAND[k]) return HAND[k];
    if (k) { const s = stageOf(ui, k); return s ? s.kind : ''; }
    const t = String(kind || '');
    try { return t && ui && ui.sealed && ui.sealed({ type: t }) ? t : ''; } catch (_) { return ''; }
  }

  function css() {
    if (doc.getElementById('pgSealCss')) return;
    const s = doc.createElement('style'); s.id = 'pgSealCss';
    s.textContent = `.pgSeals{display:inline-flex;align-items:center;gap:7px;min-width:0;max-width:100%;vertical-align:middle}
.pgSeals .seal{flex:0 0 var(--seal-fit,40px);width:var(--seal-fit,40px);height:var(--seal-fit,40px);margin:2px}
.pgGhost{position:relative;display:inline-block;flex:none;box-sizing:border-box;width:var(--seal-fit,40px);height:var(--seal-fit,40px);margin:2px;pointer-events:none;user-select:none;-webkit-user-select:none;cursor:default}
.pgGhost svg{display:block;width:100%;height:100%;overflow:visible;pointer-events:none}
.pgSeals .sealPlus{font:700 10.5px var(--sans,system-ui,sans-serif);color:var(--ink45,#938c80);white-space:nowrap}
.pgBy{min-width:0;font:10px/1.35 var(--sans,system-ui,sans-serif);color:var(--ink45,#938c80);overflow-wrap:anywhere}`;
    (doc.head || doc.documentElement).appendChild(s);
  }
  const node = (tag, cls) => { const n = doc.createElement(tag); if (cls) n.className = cls; return n; };
  const svgOf = html => { const t = doc.createElement('template'); t.innerHTML = String(html).trim(); return t.content.firstElementChild; };

  /* ═══ the unfinished seal ═══ */
  /** The seal of `kind` as it is still to come: Seal.face's own ghost (dashed, grey, no date, time or person), in a wrapper that is NOT a .seal, so no
   *  zoom, no tab stop and no click ever treat it as a stamp. null when the kind has no seal. */
  function ghost(kind, size) {
    try {
      const ui = UI(), S = SEAL(), type = ui && S ? typeOf(ui, kind) : ''; if (!type) return null;
      css();
      // (the two hand seals are the Seal module's own faces, as the cards draw them; a step's seal is the timeline's, as the header rail draws it)
      const hand = keyOf(kind), model = HAND[hand] && typeof S.modelOf === 'function' ? S.modelOf({ how: hand === 'complete' ? 'button' : 'print', at: 0, by: '' }) : ui.faceModel({ key: 'pg-ghost-' + type, type, at: 0, by: '', ghost: true });
      const label = model.action ? String(model.action).toLowerCase().replace(/^./, c => c.toUpperCase()) : 'Seal';
      const g = node('span', 'pgSeal pgGhost'); g.style.setProperty('--seal-fit', sizeOf(size) + 'px');
      g.setAttribute('role', 'img'); g.setAttribute('aria-label', `${label} seal: not stamped yet`);
      g.dataset.pgGhost = keyOf(kind) || type;
      g.append(svgOf(S.face(model, { ghost: true })));
      return g;
    } catch (e) { warn('ghost', e); return null; }
  }

  /* ═══ what the window handed over ═══ */
  function readCtx(ctx) {
    const u = UI(); if (!u || !ctx || typeof ctx !== 'object' || !Array.isArray(ctx.events)) return null;
    const piece = ctx.piece || ctx.p || null, pieces = Array.isArray(ctx.pieces) && ctx.pieces.length ? ctx.pieces : piece ? [piece] : [];
    const events = ctx.events.filter(e => e && typeof e === 'object' && e.type && (!piece || u.ofPiece(e, piece, pieces)));
    return { u, events, piece, pieces, D: ctx.D || null, steps: Array.isArray(ctx.steps) && ctx.steps.length ? ctx.steps : null, cancelled: ctx.cancelled || null, context: ctx.context || null, rec: ctx.rec || null, lineKey: String((piece && (piece.lineKey || piece.key)) || (ctx.rec && ctx.rec.key) || ctx.lineKey || '') };
  }
  const atOf = e => +e.at || 0;
  const byAt = (a, b) => atOf(a) - atOf(b);
  /** The recorded seals of a step (timeline events), oldest first, each press once. */
  function eventsOf(c, key) {
    const s = stageOf(c.u, key); if (!s) return [];
    const seen = new Set(), out = [];
    for (const e of c.events.filter(x => s.types.includes(x.type)).sort(byAt)) {
      const id = String(e.id || e.key || `${e.type}@${atOf(e)}`); if (seen.has(id)) continue; seen.add(id); out.push(e);
    }
    return out;
  }
  const dataOf = (c, e) => {
    const person = (() => { try { return c.u.personOf(e) || ''; } catch (_) { return String(e.by || ''); } })(), place = (() => { try { return c.u.placeOf(e) || ''; } catch (_) { return ''; } })();
    return { id: String(e.id || e.key || ''), type: e.type, at: atOf(e), by: String(e.by || ''), person, station: String(e.station || ''), place, sheet: String(e.sheet || '') };
  };
  /** The recorded seals of a step as data (no DOM). 'complete' and 'print' are read from the piece's record and the timeline (Seal.ofPiece). */
  function seals(stepKey, ctx) {
    try {
      const key = keyOf(stepKey), c = key ? readCtx(ctx) : null; if (!c) return [];
      if (HAND[key]) return handSeals(c, key).map(s => ({ id: String(s.id || ''), type: HAND[key], at: +s.at || 0, by: String(s.by || ''), person: String(s.by || '').trim(), station: '', place: 'Charm Sorter', sheet: '' }));
      return eventsOf(c, key).map(e => dataOf(c, e));
    } catch (e) { warn('seals', e); return []; }
  }
  /** The piece's Complete Order presses ('complete') or QR label prints ('print'): the record's stamps, what an older record kept, the timeline (Seal.ofPiece). */
  function handSeals(c, key) {
    const S = root.Seal; if (!S || typeof S.ofPiece !== 'function') return [];
    const all = S.ofPiece(c.rec || null, { events: c.events, lineKey: c.lineKey });
    return all.filter(s => key === 'complete' ? s.how === 'button' : s.how === 'print');
  }
  const stagesFor = c => c.steps || (c.u.stagesFor ? c.u.stagesFor(c.piece && c.piece.line ? c.piece.line : undefined) : undefined);
  /** The step as requirementsOf reads it: state (done | now | later | stopped | gone | skipped | none), what is done, what is missing. */
  function stepInfo(c, key) {
    if (!c.u.requirementsOf) return null;
    return c.u.requirementsOf(key, { events: c.events, D: c.D ? Object.assign({}, c.D) : undefined, stages: stagesFor(c), cancelled: c.cancelled || undefined, context: c.context || undefined });
  }

  /* ═══ the real seal ═══ */
  const rotOf = (c, e) => { const h = hash(String(e.key || e.id || '') + e.type), sh = c.u.KIND && c.u.KIND[e.type] ? c.u.KIND[e.type].sh : 'e'; return sh === 'm' ? 4 + (h % 8) : -(4 + (h % 9)); };
  const titleOf = (c, e, d, label) => [label, d.person || d.by, d.place, d.sheet, longWhen(d.at)].filter(Boolean).join(' · ');
  /** One timeline event as a seal: the same Seal.face as the timeline's, in a .seal element (Seal.zoom grows it in place). */
  function sealEl(c, e, label, size) {
    const S = SEAL(), model = c.u.faceModel(e), d = dataOf(c, e);
    const s = node('span', 'seal seal-step pgSeal'); s.style.setProperty('--rot', rotOf(c, e) + 'deg'); s.style.setProperty('--seal-fit', size + 'px');
    s.dataset.at = String(d.at || 0); s.dataset.pgBy = d.person; s.dataset.pgPlace = [d.place, d.sheet].filter(Boolean).join(' · ');
    s.tabIndex = 0; s.setAttribute('role', 'img'); s.setAttribute('aria-label', titleOf(c, e, d, label));
    s.append(svgOf(S.face(model)));
    return { s, d };
  }
  const captionOf = d => [d.person || d.by, d.place, d.sheet, shortWhen(d.at)].filter(Boolean).join(' · ');
  function plus(more) { const p = node('span', 'sealPlus'); p.textContent = '+' + more; p.setAttribute('role', 'img'); p.setAttribute('aria-label', `${more} more seal${more > 1 ? 's' : ''}`); return p; }
  function wrap(key, state, count) {
    const w = node('span', 'pgSeals'); w.dataset.pgStep = key; w.dataset.pgState = state; w.dataset.pgCount = String(count || 0); return w;
  }
  /** A step's recorded seal as one element: its first seal (the one the header rail draws for the step; a step the timeline records again for the same piece, a second
   *  sorting scan or a re-nesting, keeps that one seal, as the Timeline does, and the rest stay in seals()), and a quiet line (who, where, when). */
  function realOf(c, key, size, caption) {
    const evs = eventsOf(c, key); if (!evs.length) return null;
    const stage = stageOf(c.u, key), label = stage ? stage.l : key, { s, d } = sealEl(c, evs[0], label, size);
    const w = wrap(key, 'done', evs.length); w.append(s);
    if (caption) { const t = captionOf(d); if (t) { const by = node('span', 'pgBy'); by.textContent = t; w.append(by); } }
    return w;
  }
  /** 'complete' / 'print': the Complete Order press that completed the piece (else the latest one), or the latest QR label print, and "+N" for the rest:
   *  Seal.compactRow, the order overview's own one-seal-and-"+N" component (the card's own seal face, so it looks and zooms exactly like the ones in the rows). */
  function handOf(c, key, size, caption) {
    const S = root.Seal, all = handSeals(c, key); if (!all.length || !S) return null;
    const done = key === 'complete' && c.rec && typeof S.completionOf === 'function' ? S.completionOf(c.rec, { events: c.events, lineKey: c.lineKey }) : null;
    const lead = done && done.how === 'button' ? done : all[all.length - 1], more = all.length - 1;
    const html = typeof S.compactRow === 'function' ? S.compactRow(lead, more, size) : S.row({ stamps: [lead], prints: lead.how === 'print' ? lead.n || 0 : 0 }, { size });
    if (!html) return null;
    const w = wrap(key, 'done', all.length); w.insertAdjacentHTML('beforeend', html);
    if (caption) { const t = captionOf({ person: String(lead.by || '').trim(), by: String(lead.by || ''), place: 'Charm Sorter', sheet: '', at: +lead.at || 0 }); if (t) { const by = node('span', 'pgBy'); by.textContent = t; w.append(by); } }
    return w;
  }

  /* ═══ render ═══ */
  function render(stepKey, ctx, o) {
    try {
      o = o || {};
      const key = keyOf(stepKey), S = SEAL(); if (!key || !S) return null;
      const c = readCtx(ctx); if (!c) return null;
      css();
      const size = sizeOf(o.size), caption = o.caption !== false;
      // the two seals that complete a piece by hand are not steps: a real one, or nothing (a ghost would say the piece must be completed by hand)
      if (HAND[key]) return handOf(c, key, size, caption);
      if (!stageOf(c.u, key)) return null;
      // a recorded seal is permanent and wins over everything: it is shown whatever `done` says
      const real = realOf(c, key, size, caption); if (real) return real;
      if (o.done === true) return null;   // (done, but the timeline holds no event of it: nothing is invented)
      const q = stepInfo(c, key);
      if (!q || (q.state !== 'now' && q.state !== 'later')) return null;   // (never taken: cancelled, completed by hand, not this piece's step; or done with no event)
      const g = ghost(key, size); if (!g) return null;
      const w = wrap(key, 'missing', 0); w.append(g);
      return w;
    } catch (e) { warn('render', e); return null; }
  }

  /** The card's data from the same sources: what is done (who, where, when), what is still missing, and whether it has a seal or would show a ghost. */
  function info(stepKey, ctx) {
    try {
      const key = keyOf(stepKey), c = key ? readCtx(ctx) : null; if (!c || HAND[key] || !stageOf(c.u, key)) return null;
      const q = stepInfo(c, key); if (!q) return null;
      const list = seals(key, ctx);
      return { key, label: q.label, state: q.state, done: q.done, need: q.need, facts: q.facts, seals: list, ghost: !list.length && (q.state === 'now' || q.state === 'later') };
    } catch (e) { warn('info', e); return null; }
  }
  const has = stepKey => { const k = keyOf(stepKey), u = UI(); return !!k && (!!HAND[k] || !!stageOf(u, k)); };

  root.PieceSeals = { render, ghost, has, seals, info, keyOf, DEFAULT_SIZE: DEFAULT };
})(typeof self !== 'undefined' ? self : this);
