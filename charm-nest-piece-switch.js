/* The PIECE SWITCH: one component for "which piece of this order am I looking at" (Paul, 10 Oct 2026: "adapt the entire application to be able to cycle through all the
 * chosen discs one by one in the engraving tab and everywhere the engraving option is presented to the user (including all popup modals)").
 *
 * A necklace with "2 Disc" is ONE order line with two pieces; an earring pair is one line with a Left and a Right. Each piece has its own words, font, state, approval,
 * seal and back file (charm-nest-engrave-sides.js: one job per piece, slot L, R or D1..Dn). The Engrave tab built a "Left | Right" switch for the two ears
 * (charm-nest-engrave-rows.js, charm-nest-bridge.js). This is that same switch for any number of pieces, so that every place that shows the engraving of a counted
 * line (the editor, the order window, the sheet window, Review, pop-ups) draws THE SAME control and moves the same way: nothing is restyled, nothing is a second
 * implementation.
 *
 * It needs the pieces of a line as a list of engraving jobs ({ key, slot, lines, state, copies, fit, verify, row }) or of anything that carries the same fields
 * (a record from the order window may give { key, slot, lines, state } and its own `stage`, `font`). It reads nothing, writes nothing, draws nothing by itself:
 *
 *   list(jobs, o)               [piece]   the jobs of ONE line, in the order given, told as pieces (each also carries `record`: CharmNestEngraveSides.pieceRecord, the one per-piece
 *                                         record: index, of, slot, tag, words, lines, poolIds, font, fontAsked, state, approved, sealed; nothing here is stored or invented):
 *                                         { key, job, slot, kind ("ear" | "disc" | "piece"), n (disc number, else 0), label ("Left" | "Right" | "Disc 2"),
 *                                           name ("Left ear" | "Disc 2"), tag ("LEFT EAR" | "DISC 2 of 3" + " ×2" when it holds several copies), words ("J" | "Anna / Ben"),
 *                                           font ({ name, asked }), stage ("Placement to check"), state, busy }
 *                                         o: { of (how many pieces the line has in all, decided ones included; default: the list), isWorking(job), stageOf(job, busy),
 *                                              fontOf(job) -> { name, asked } | string }
 *   cursor(pieces, key)         { pieces, count, index, current, prev, next, hasPrev, hasNext, at(key) }   where the person is; prev/next wrap round the line
 *   step(pieces, key, dir, wrap)  the piece after (dir 1) or before (dir -1) `key`; null at either end unless wrap
 *   chipHtml(piece)             the little tag ("DISC 2 of 3"), as the lists and the switch draw it
 *   html(pieces, key, o)        the switch itself, the markup of the Left | Right switch for any number of pieces (class egEarSwitch / egEarTab, data-a="ear",
 *                               data-ear=<job key>; egMany besides on a switch of discs or other pieces, which may wrap on a narrow window); o.fonts: add each piece's font by name to its button (the discs do; the ears keep the markup they always had)
 *   bind(host, onPick)          press handler for every button of a switch inside `host`: onPick(key, button); returns a function that takes it off again
 *   kindOf(pieces)              "ears" | "discs" | "pieces"
 *   fontText(font)              "Special Elite" (the font the piece is engraved in), "Typewriter (asked)" when the buyer's font has no match yet, "" when none is known
 *   fontNote(font)              the same in a sentence for a tooltip ("asked Typewriter")
 *   allSameWords(pieces)        every piece carries the very same (non-empty) words: the one condition under which one press may approve them all (charm-nest-engrave-rows.js approveAll)
 *
 * FACES (plans/front-back-1011/CONTRACT.md): a piece may have a FRONT job beside its back job (charm-nest-engrave-faces.js). list() then also tells each piece's `face` ("front" |
 * "back"), `faceLabel`, `pieceKey` (the piece, shared by its faces) and `faces` (how many faces this piece has here); a front's `tag` gets " · FRONT" when the piece has a back too.
 *   groupPieces(pieces)         [{ slot, pieceKey, kind, n, label, name, tag, front, back, pieces:[front?, back?], words, stage, key }]   the pieces of list() grouped by piece (Front first)
 *   faceHtml(group, key, o)     the Front | Back switch of ONE piece: the very markup of the Left | Right switch (egEarSwitch / egEarTab, data-a="ear", data-ear=<job key>); bind() handles it
 *   html(pieces, key, o)        with a front job in the list (and o.faces !== false) it draws the PIECE switch (one button per piece; none for a single piece) and, right under it, the
 *                               Front | Back switch of the piece shown; a list with no front job draws exactly what it always drew
 *   kindOf(pieces)              also "faces": ONE piece with a front and a back
 * Pure but for html()/bind(): no network, no clock. Loads in the page (window.CharmNestPieceSwitch) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestPieceSwitch = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Rows = () => dep('CharmNestEngraveRows', './charm-nest-engrave-rows.js');
  const Sides = () => dep('CharmNestEngraveSides', './charm-nest-engrave-sides.js');
  const Faces = () => dep('CharmNestEngraveFaces', './charm-nest-engrave-faces.js');
  const faceOfJob = job => { const F = Faces(); return F ? F.faceOf(job) : 'back'; };
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const slotOf = job => { if (!job) return ''; if (job.slot) return String(job.slot); const m = /#(L|R|D\d{1,2})(?:~front)?$/.exec(String(job.key == null ? '' : job.key)); return m ? m[1] : ''; };
  const kindOfSlot = slot => (slot === 'L' || slot === 'R' ? 'ear' : /^D\d{1,2}$/.test(slot) ? 'disc' : 'piece');
  const labelOf = slot => (slot === 'L' ? 'Left' : slot === 'R' ? 'Right' : /^D\d+$/.test(slot) ? 'Disc ' + slot.slice(1) : '');
  const nameOf = slot => (slot === 'L' ? 'Left ear' : slot === 'R' ? 'Right ear' : labelOf(slot));
  const tagOf = (slot, of) => (slot === 'L' ? 'LEFT EAR' : slot === 'R' ? 'RIGHT EAR' : /^D\d+$/.test(slot) ? 'DISC ' + slot.slice(1) + (of > 1 ? ' of ' + of : '') : '');

  /** What a piece is engraved in, by name. A piece's own record first (a job or a record: `font`, a name or { name, asked }), then the one the line asked for (row.spec.font), else the app's font. */
  function fontOfJob(job) {
    const norm = f => (f == null || f === '' ? null : typeof f === 'string' ? { name: f, asked: '' } : { name: String(f.name || f.installed || ''), asked: String(f.asked || '') });
    const own = norm(job && job.font) || norm(job && job.piece && job.piece.font), line = job && job.row && job.row.spec && norm(job.row.spec.font);
    const f = own || line; if (!f) return { name: '', asked: '' };
    return { name: f.name, asked: f.asked };
  }
  const fontParts = f => { const x = typeof f === 'string' ? { name: f, asked: '' } : f || {}; return { name: String(x.name || '').trim(), asked: String(x.asked || '').trim() }; };
  const fontText = f => { const x = fontParts(f); return x.name || (x.asked ? x.asked + ' (asked)' : ''); };
  const fontNote = f => { const x = fontParts(f); return x.name ? (x.asked && x.asked.toLowerCase() !== x.name.toLowerCase() ? `${x.name} (the buyer asked for ${x.asked})` : x.name) : x.asked ? `the buyer asked for ${x.asked}; no matching font yet` : ''; };

  function list(jobs, o) {
    o = o || {}; const R = Rows(), F = Faces(), arr = (jobs || []).filter(Boolean), withFront = !!F && arr.some(j => F.faceOf(j) === 'front');
    const of = o.of > 0 ? o.of : withFront ? F.pieceCount(arr) : arr.length;
    const per = new Map(); if (withFront) for (const j of arr) { const k = slotOf(j); per.set(k, (per.get(k) || 0) + 1); }
    return arr.map(job => {
      const slot = slotOf(job), busy = !!(o.isWorking && o.isWorking(job)), face = faceOfJob(job), faces = withFront ? per.get(slot) : 1;
      const copies = (job.copies || []).length, S = Sides(), rec = S && S.pieceRecord ? S.pieceRecord(job, { of, faces }) : null;
      const f = o.fontOf ? o.fontOf(job) : rec && rec.font ? rec.font : fontOfJob(job), font = fontParts(f);
      const base = tagOf(slot, of), many = copies > 1 ? ' ×' + copies : '';
      const pieceKey = rec ? rec.pieceKey : (F ? F.parseKey(job.key).pieceKey : String(job.key == null ? '' : job.key));
      return { key: job.key, job, record: rec, slot, kind: kindOfSlot(slot), n: /^D(\d+)$/.test(slot) ? +slot.slice(1) : 0, label: labelOf(slot), name: nameOf(slot),
        tag: (face === 'front' && faces > 1 ? [base, 'FRONT'].filter(Boolean).join(' · ') : base) + many, baseTag: base + many, words: R ? R.wordsOf(job) : '', font,
        face, faceLabel: face === 'front' ? 'Front' : 'Back', pieceKey, faces,
        stage: o.stageOf ? o.stageOf(job, busy) : R ? R.stageOf(job, busy) : '', state: job.state, busy };
    });
  }
  function cursor(pieces, key) {
    const ps = pieces || [], i = ps.findIndex(p => p.key === key), n = ps.length;
    return { pieces: ps, count: n, index: i, current: i < 0 ? null : ps[i], hasPrev: i > 0, hasNext: i >= 0 && i < n - 1,
      prev: n ? ps[i < 0 ? n - 1 : (i + n - 1) % n] : null, next: n ? ps[i < 0 ? 0 : (i + 1) % n] : null, at: k => ps.find(p => p.key === k) || null };
  }
  function step(pieces, key, dir, wrap) {
    const ps = pieces || [], n = ps.length; if (!n) return null;
    const i = ps.findIndex(p => p.key === key);
    if (i < 0) return dir < 0 ? ps[n - 1] : ps[0];
    const j = i + (dir < 0 ? -1 : 1);
    if (j < 0 || j >= n) return wrap ? ps[(j + n) % n] : null;
    return ps[j];
  }
  const kindOf = pieces => {
    const ps = pieces || [], ks = new Set(ps.map(p => p.kind));
    if (ps.length >= 2 && new Set(ps.map(p => p.pieceKey || p.slot)).size === 1 && ps.some(p => p.face === 'front')) return 'faces';
    return ks.size === 1 && ks.has('disc') ? 'discs' : ks.size === 1 && ks.has('ear') ? 'ears' : 'pieces';
  };
  const chipHtml = p => `<span class="egPiece" data-slot="${esc(p.slot)}">${esc(p.tag)}</span>`;
  /** Every piece carries the very same words. With front jobs in the list the rule is per face (a front and a back carry different words by nature): the pieces of each face must agree. */
  const allSameWords = pieces => {
    const ps = pieces || [];
    if (!ps.some(p => p.face === 'front')) return ps.length >= 2 && !!ps[0].words && ps.every(p => p.words === ps[0].words);
    return ps.length >= 2 && ps.every(p => !!p.words) && ['front', 'back'].every(f => { const of = ps.filter(p => p.face === f); return of.length < 2 || of.every(p => p.words === of[0].words); });
  };

  /** The switch: one button for each piece (its tag, its words, where it stands), the one shown pressed. Presentation only: each piece is its own card, with its own words, fit and approval.
   *  For two ears this is, character for character, the Left | Right switch of the Engrave editor. */
  function flatHtml(pieces, key, o) {
    o = o || {}; const ps = pieces || [], kind = kindOf(ps);
    const aria = kind === 'ears' ? 'The left and the right ear of this line' : kind === 'discs' ? 'The discs of this order, one by one' : 'The pieces of this line, one by one';
    return `<span class="egEarSwitch${kind === 'ears' ? '' : ' egMany'}" role="group" aria-label="${aria}">${ps.map(p => {
      const on = p.key === key, ft = o.fonts ? fontText(p.font) : '', fn = o.fonts ? fontNote(p.font) : '';
      const what = kind === 'ears' ? 'the ' + p.name.toLowerCase() : p.name.toLowerCase();
      return `<button type="button" class="egEarTab" data-a="ear" data-ear="${esc(p.key)}" aria-pressed="${on}" title="${on ? 'Shown now' : 'Show'}: ${esc(what)} · ${esc(p.words || 'words not settled')}${fn ? ' · ' + esc(fn) : ''} · ${esc(p.stage)}">${chipHtml(p)}<span class="egEarWords">${esc(p.words || '…')}</span>${ft ? `<span class="egEarFont">${esc(ft)}</span>` : ''}<span class="egEarStage">${esc(p.stage)}</span></button>`;
    }).join('')}</span>`;
  }
  /** The pieces of list() grouped by piece (a piece = a slot of the line), in the order given; each group has its Front and its Back. */
  function groupPieces(pieces) {
    const by = new Map(), out = [];
    for (const p of pieces || []) {
      const k = p.pieceKey || p.slot || '';
      if (!by.has(k)) { const g = { slot: p.slot, pieceKey: k, kind: p.kind, n: p.n, label: p.label, name: p.name, tag: p.baseTag != null ? p.baseTag : p.tag, front: null, back: null, pieces: [], words: '', stage: '', key: p.key }; by.set(k, g); out.push(g); }
      by.get(k)[p.face === 'front' ? 'front' : 'back'] = p;
    }
    const open = p => !['approved', 'written', 'skipped', 'none'].includes(p.state);
    for (const g of out) {
      g.pieces = [g.front, g.back].filter(Boolean);
      const first = g.pieces.find(open) || g.back || g.front;
      g.key = first.key; g.stage = first.stage;
      const ws = g.pieces.map(p => p.words).filter(Boolean); g.words = new Set(ws).size === 1 ? ws[0] : ws.join(' · ');
    }
    return out;
  }
  /** The Front | Back switch of ONE piece (group from groupPieces): the markup of the Left | Right switch, a button for each face with its words and where it stands. */
  function faceHtml(group, key, o) {
    o = o || {}; const g = group || { pieces: [] };
    return `<span class="egEarSwitch egFaces" role="group" aria-label="The front and the back engraving of this piece">${g.pieces.map(p => {
      const on = p.key === key, ft = o.fonts ? fontText(p.font) : '', fn = o.fonts ? fontNote(p.font) : '', what = (p.face === 'front' ? 'the front' : 'the back') + (g.name ? ' of ' + g.name.toLowerCase() : '');
      return `<button type="button" class="egEarTab" data-a="ear" data-ear="${esc(p.key)}" data-face="${esc(p.face)}" aria-pressed="${on}" title="${on ? 'Shown now' : 'Show'}: ${esc(what)} · ${esc(p.words || 'words not settled')}${fn ? ' · ' + esc(fn) : ''} · ${esc(p.stage)}"><span class="egPiece" data-slot="${esc(p.slot)}" data-face="${esc(p.face)}">${esc(p.faceLabel.toUpperCase())}</span><span class="egEarWords">${esc(p.words || '…')}</span>${ft ? `<span class="egEarFont">${esc(ft)}</span>` : ''}<span class="egEarStage">${esc(p.stage)}</span></button>`;
    }).join('')}</span>`;
  }
  function html(pieces, key, o) {
    o = o || {}; const ps = pieces || [];
    if (o.faces === false || !ps.some(p => p.face === 'front')) return flatHtml(ps, key, o);
    const groups = groupPieces(ps), shown = groups.find(g => g.pieces.some(p => p.key === key)) || groups[0];
    // one button for each piece (it presses to the face still to settle, or to the face shown when the piece is the shown one), then the shown piece's Front | Back
    const top = groups.length > 1 ? flatHtml(groups.map(g => ({ key: g === shown ? (g.pieces.some(p => p.key === key) ? key : g.key) : g.key, slot: g.slot, kind: g.kind, n: g.n, label: g.label, name: g.name, tag: g.tag, words: g.words, font: (g.back || g.front).font, stage: g.stage })), g0(shown, key), o) : '';
    return top + (shown.pieces.length > 1 ? faceHtml(shown, key, o) : '');
  }
  const g0 = (shown, key) => (shown.pieces.some(p => p.key === key) ? key : shown.key);
  function bind(host, onPick) {
    if (!host || typeof host.addEventListener !== 'function') return () => {};
    const on = e => { const b = e.target && e.target.closest && e.target.closest('[data-a="ear"][data-ear]'); if (b && host.contains(b)) onPick(b.dataset.ear, b); };
    host.addEventListener('click', on);
    return () => host.removeEventListener('click', on);
  }
  void Sides;
  return { list, cursor, step, kindOf, chipHtml, html, groupPieces, faceHtml, bind, fontText, fontNote, fontOfJob, allSameWords, tagOf, labelOf, nameOf };
});
