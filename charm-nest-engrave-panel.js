/* The ENGRAVING PANEL of the Engrave tab's detail card (Paul, 12 Oct 2026: "This UI is totally broken everywhere ... it should only be part of the detailed Engraving modal popup within the
 * Engraving tab ... do NOT crowd the menu" and "the user should be able just to click on either one and then update it with the text font options and positional update of the text on the
 * charm, and the actual text itself. Everything has to be linked to its engraving, whether it be front or back on the same charm or multiple charms in the same order").
 * Plan: plans/engrave-ui-1012 (CONTRACT.md section D). It replaces the UI half of charm-nest-engrave-setup.js for the Engrave tab; the model (read, patchOf, consequences, buildOverride,
 * fontChoices) and the save path (Engrave.saveSetup -> CharmNestEngraveSetup.apply -> seedWords) are the ones that file already has.
 *
 * ONE column, 232px minimum, always open, nothing explained in words:
 *   None | Front | Back | Front + back    one row (an earring / never line draws nothing at all: CharmNestEngraveFaces.neverOf; a line that only ever allows a front offers None | Front)
 *   [words]                               a full-width text area (2 rows, grows to 4, then scrolls) for the selected face
 *   [font]                                a full-width drop-down (never beside the words, so it cannot overlap them); each option in its own font once the preview loaded it
 *   Position  < ^ v > centre - +          acts at once on THAT face's job (CharmNestEngravePosition); Cancel puts it back
 *   Save  Cancel                          both off until something is staged
 * The face being edited is chosen by the Front / Back thumbnails above the panel (CharmNestEngraveCard.mountPiece, `onPick`), or `setFace(face)`. Every draft belongs to ONE piece:
 * its key is the piece key `<lineKey>[#slot]`, and inside it each face has its own words and font, so the Front never changes the Back, disc 1 never shares a draft with disc 2.
 * Typing, picking a font or moving the text repaints that face's thumbnail at once (CharmNestEngravePreview.update), before anything is saved.
 *
 *   mount(slot, row, opts?)     draws the panel into `slot` for the piece of `row` and wires it -> { destroy(), refresh(), setFace(face), draw() }. Safe to call again after every redraw of the host
 *                               (the staged edit, the face, the caret and the position snapshot come back). Draws nothing when the line has nothing to edit.
 *                               opts: { job (the job the card shows: gives the piece and the face), face, slot, jobs(), onFace(face), onChange({ face, kind }), thumb(face) -> element of that face's preview,
 *                                       save(patch) -> Promise, scope }
 *   mountPiece(slot, job, o?)   the slot of the Engrave detail card: the Front / Back thumbnails (CharmNestEngraveCard.mountPiece, when the page has it) above the panel, linked both ways
 *   html(vm, d, ctx)            the panel as a string (for tests)
 *
 * Loads in the page (window.CharmNestEngravePanel) and in node (pure parts only). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngravePanel = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Faces = () => dep('CharmNestEngraveFaces', './charm-nest-engrave-faces.js');
  const Setup = () => dep('CharmNestEngraveSetup', './charm-nest-engrave-setup.js');
  const Pos = () => (root && root.CharmNestEngravePosition) || null;
  const Prev = () => (root && root.CharmNestEngravePreview) || null;
  const Eng = () => (root && root.Engrave) || null;
  const Why = () => dep('CharmNestEngraveWhy', './charm-nest-engrave-why.js');   // (ENGWHY: the reasons, what counts as an override, the record)
  const str = x => String(x == null ? '' : x);
  const esc = s => str(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const cap1 = s => s.charAt(0).toUpperCase() + s.slice(1);
  const linesOf = v => (Array.isArray(v) ? v : str(v).split(/\r?\n/)).map(x => str(x).replace(/\s+/g, ' ').trim()).filter(Boolean);
  const lineOf = row => (row && row.parentRow) || row || null;
  const tryDo = (f, d) => { try { return f(); } catch (_) { return d; } };
  const MODES = [['none', 'None'], ['front', 'Front'], ['back', 'Back'], ['both', 'Front + back']];
  const facesOf = mode => (mode === 'both' ? ['front', 'back'] : mode === 'front' ? ['front'] : mode === 'back' ? ['back'] : []);
  const hasPatch = p => !!(p && (p.mode || p.front !== undefined || p.back !== undefined));
  const slotOfJob = j => { if (!j) return null; const F = Faces(); return j.slot || (F && j.key ? F.parseKey(j.key).slot : null) || null; };
  const pieceKeyOf = (line, slot) => str(line && line.key) + (slot ? '#' + slot : '');
  /** The modes a piece may be given. A line that never engraves is not drawn at all; a line that only ever allows a front (none today) would get None | Front. */
  function modesFor(line) {
    const F = Faces(); if (F && F.neverOf && F.neverOf(line)) return [];
    const sp = line && line.spec, only = sp && sp.engraveFrontOnly;
    return only ? MODES.filter(([m]) => m === 'none' || m === 'front') : MODES;
  }

  /* ── the staged edit ───────────────────────────────────────────────────────── */
  const DRAFTS = new Map();    // piece key -> { mode, face, faces: { front: { text, font }, back: ... }, dirty, posTouched, snaps: { jobKey: snapshot }, busy, error, confirm, ... }
  const LIVE = new Map();      // piece key -> the bindings on screen (a save that ends tells every one of them)
  let uid = 0;
  /* ── the life of a staged edit (ENGAUTO, round 2: "a staged draft dies with its panel session") ─────────────────────────────────────────────────────────────────
   * An edit that is staged and not saved belongs to the card it was made in. It lives through every redraw of that card (the Engrave tab draws the detail again on each change, even for the other
   * face of the same piece), and ends with Cancel, with Save, when another order or piece opens, and when the detail is closed (the tab draws a list, another tab, nothing): the next time the card
   * opens it shows what the system chose (or what a person saved), never an old draft. "Live" = a panel of that piece is on the page. */
  const GRACE = 250;       // ms of quiet after the page has drawn again before a piece with no panel on the page loses its draft (a redraw swaps the card in the same task)
  let sweepT = 0, watching = null;
  const isLive = key => { const set = LIVE.get(key); if (!set) return false; for (const b of set) if (b.el && b.el.isConnected) return true; return false; };
  /** Forget the staged edit of every piece that has no panel on the page; `except` is the piece being opened. */
  function sweep(except) { for (const k of [...DRAFTS.keys()]) if (k !== except && !isLive(k)) DRAFTS.delete(k); }
  const sweepSoon = () => { try { clearTimeout(sweepT); sweepT = setTimeout(() => sweep(), GRACE); } catch (_) { /* a missed sweep ends at the next opening */ } };
  /** One observer on the Engrave tab's own view: every drawing of it (a card gone, a list, another piece) wakes one quiet sweep. Nothing is read or written. */
  function watch(doc) {
    try {
      const v = doc && doc.getElementById && doc.getElementById('engraveView'); if (!v || watching === v || typeof MutationObserver !== 'function') return;
      if (watching && watching._engpMo) watching._engpMo.disconnect();
      const mo = new MutationObserver(() => { if (DRAFTS.size) sweepSoon(); }); mo.observe(v, { childList: true, subtree: true }); v._engpMo = mo; watching = v;
    } catch (_) { /* the draft then ends at the next piece opened */ }
  }

  /** What the card must show while a piece is being edited: the mode the person staged and the words and font typed so far (null when nothing is staged: the card shows what is saved).
   *  The faces of the card follow it at once (None draws no face, Front + back draws both). */
  function stageOf(key) {
    const d = DRAFTS.get(key); if (!d || !(d.dirty || d.posTouched)) return null;
    const x = f => ({ text: str(d.faces[f] && d.faces[f].text), font: str(d.faces[f] && d.faces[f].font) });
    return { mode: d.mode, faces: { front: x('front'), back: x('back') } };
  }

  function draftOf(vm, key) {
    let d = DRAFTS.get(key);
    if (d && d.sig !== vm.sig && !d.dirty && !d.posTouched && !d.busy) d = null;   // what the piece says changed under an untouched draft: start again from it
    if (!d) {
      d = { key, sig: vm.sig, mode: vm.mode, face: null, shown: '', faces: {}, dirty: false, posTouched: false, snaps: {}, moved: {}, noRoom: {}, busy: '', error: '', confirm: '', confirmLines: null, focus: null, ask: null, why: null };
      for (const f of ['front', 'back']) { const r = vm.rows.find(x => x.face === f); d.faces[f] = { text: r ? r.text : '', font: r ? r.fontWord : '' }; }
      d.init = { front: Object.assign({}, d.faces.front), back: Object.assign({}, d.faces.back) };   // (the faces as the panel opened them: what the person has not touched is the system's own, CharmNestEngraveWhy.changes)
      DRAFTS.set(key, d);
    }
    return d;
  }
  /** EDITOR WORDS (Paul, 27 Oct 2026: "the system should not force the user to also add text on top of already added text from within the Charm Editor"): is this face of the piece engraved by
   *  parts the charm editor added? (CharmNestEngraveSetup.editorArtOn: the page's one test) */
  const artOn = (vm, face) => { const S = Setup(); return !!(face && S && S.editorArtOn && tryDo(() => S.editorArtOn(vm, face), false)); };
  /** The words a face starts with when its mode is switched on and nothing was typed: the line's own, else the buyer's labelled words, else (one face and no labels) the other face's. Never a guess between two faces. */
  function seedText(vm, face, mode) {
    const S = Setup(), here = vm.rows.find(r => r.face === face); if (here && here.text) return here.text;
    if (artOn(vm, face)) return '';   // (EDITOR WORDS: a face the charm editor drew is engraved already: the buyer's words stay an offer, never filled in)
    const lab = S.labelled(vm.raw); if (lab[face]) return lab[face].join('\n');
    if (mode !== 'both' && !lab.any) {
      const other = vm.rows.find(r => r.face !== face && r.text), rd = vm.setup && vm.setup[face === 'front' ? 'back' : 'front'];
      if (other) return other.text; if (rd) return rd.text;
      return linesOf(vm.raw).join('\n');
    }
    return '';
  }
  /** The mode changes: the faces it asks for start with their words; two faces never end on the same copied words. */
  function setMode(vm, d, mode) {
    d.mode = mode; d.confirm = ''; d.error = ''; d.dirty = true;
    for (const f of facesOf(mode)) { const x = d.faces[f] || (d.faces[f] = { text: '', font: '' }); if (!str(x.text).trim() && !vm.rows.find(r => r.face === f && r.text)) x.text = seedText(vm, f, mode); }
    if (mode === 'both') { const a = d.faces.front, b = d.faces.back, same = linesOf(a.text).join('\n') === linesOf(b.text).join('\n') && linesOf(a.text).length; if (same) { const keepFront = vm.rows.find(r => r.face === 'front' && r.text); (keepFront ? b : a).text = ''; } }
    const on = facesOf(mode); if (!on.includes(d.face)) d.face = on[0] || d.face;
  }
  /** What a save would make of the piece's setup (for the consequences). */
  function nextSetupOf(vm, patch) {
    const F = Faces(), S = Setup(), line = vm.line, rd = tryDo(() => { const sp = line.spec, e = sp && sp.engraving; return e && typeof e === 'object' && F.MODES.includes(e.mode) ? e : S.derivedReading(line); }, null);
    const spec = rd && rd !== line.spec.engraving ? Object.assign({}, line.spec, { engraving: rd }) : line.spec;
    return F.setupOf({ spec }, S.buildOverride(line, patch, 'x', 0), vm.slot || undefined);
  }

  /* ── the position of a face (CharmNestEngravePosition, ENGPOS) ─────────────── */
  const faceJobOf = (vm, face, slot) => { const r = vm.rows.find(x => x.face === face); return r && r.jobs ? (r.jobs.find(j => (slotOfJob(j) || null) === (slot || null)) || (slot ? null : r.jobs[0]) || null) : null; };
  const snapOf = job => { const P = Pos(); return job && P && P.snapshot ? tryDo(() => P.snapshot(job), null) : null; };
  /** The snapshot of a face's placement, taken right before the panel's first move of it (what the placement was when the person began; kept with the draft, so it survives every redraw of the card, until Save or Cancel). */
  function snapBeforeMove(d, job) { if (job && job.key && !d.moved[job.key]) d.snaps[job.key] = snapOf(job); }
  const positionOf = job => { const V = Prev(); if (V && V.positionOf) return job ? tryDo(() => V.positionOf(job), null) : null; const P = Pos(); return job && P && P.of ? tryDo(() => P.of(job), null) : null; };   // (ENGTHUMB: the thumbnails draw the work area's frame, where the job's words sit is the preview's own positionOf; ENGPOS's `of` is the as-drawn picture)

  /* ── the font drop-down ────────────────────────────────────────────────────── */
  function fontOptions(vm, value) {
    const S = Setup(), list = S.fontChoices ? S.fontChoices() : [], lf = vm.lineFont || {}, P = Prev();
    const lineWord = str(lf.word || lf.name || 'Default'), lineKey = str(lf.word).toLowerCase();
    const fam = word => { try { if (!P || !P.fontFamilyOf || !P.fontIdOf) return ''; const id = P.fontIdOf(vm.line, 'back', word); return id ? ` style="font-family:${esc(P.fontFamilyOf(id))}"` : ''; } catch (_) { return ''; } };
    const have = list.some(c => c.word === value);
    const opts = [`<option value=""${fam(lf.word)}${!value || str(value).toLowerCase() === lineKey ? ' selected' : ''}${lf.name ? ` title="${esc(lf.name)}"` : ''}>${esc(lineWord)}</option>`];
    for (const c of list) { if (lineKey && c.word.toLowerCase() === lineKey) continue; opts.push(`<option value="${esc(c.word)}"${fam(c.word)}${c.word === value ? ' selected' : ''}${c.name ? ` title="${esc(c.name)}"` : ''}>${esc(c.word)}</option>`); }
    if (value && !have && str(value).toLowerCase() !== lineKey) opts.push(`<option value="${esc(value)}" selected>${esc(value)}</option>`);
    return opts.join('');
  }

  /* ── the system's own choice, kept in sight (ENGHILITE; Paul, 12 Oct 2026, point 7: "Highlight the AI chosen engraving options, so they're clearly visible to the user for a particular order/listing,
   *    unless the user decides to override those selections") ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   * What the system chose is CharmNestEngraveSetup.autoChoice(line, slot) (ENGAUTO) when the page has it, else the reading under the person's override (vm.base). It is MARKED, never moved: a small gold sparkle on the
   * system's mode segment, on the system's font (its entry in the drop-down list, and a ghost beside the closed drop-down once another font is picked) and on the words box of the face shown; the control whose value IS the
   * system's takes a quiet gold tint too. A person's pick moves the selection (the ring, the words, the font), never the marker, so the system's choice and the person's change are both on the screen. The only words are
   * titles: "Chosen by the system" (+ what the system chose, and after an override the recorded reason, read-only: engravingOverride.reason). It decorates the panel after it is drawn (hiDecorate), so the panel's own markup,
   * its widths (the sparkle takes no room: the mode control stays one row at 232px) and its handlers are untouched. */
  const HI_TITLE = 'Chosen by the system';
  const HI_SPARK = '<svg viewBox="0 0 12 12" aria-hidden="true" focusable="false"><path d="M6 .4C6.4 3.8 8.2 5.6 11.6 6 8.2 6.4 6.4 8.2 6 11.6 5.6 8.2 3.8 6.4 .4 6 3.8 5.6 5.6 3.8 6 .4Z" fill="currentColor"/></svg>';
  const HI_GLYPH = '✦';   // (the same four-point star, as the text mark in front of the system's entry of the font list: an <option> can only hold text)
  const normWords = v => linesOf(v).join('\n');
  const sameWord = (a, b) => str(a).trim().toLowerCase() === str(b).trim().toLowerCase();
  /** The same words? What the engine does to a face's words is not a person's change: a capitals font (Ace) engraves "Name One" as "NAME ONE" and the automatic line split puts it on two lines ("NAME" / "ONE"), so
   *  the staged words are compared as one run of words (line breaks as spaces) and, for a capitals font, in capitals. Any other difference (a letter, a digit, a space between words) is the person's. */
  function sameWords(a, b, fontId) {
    const O = dep('CharmNestOrders', './charm-nest-orders.js'), run = v => linesOf(v).join(' '), caps = t => { try { return O && O.capsText && fontId ? str(O.capsText(t, fontId)) : t; } catch (_) { return t; } };
    return caps(run(a)) === caps(run(b));
  }
  const rowFor = (vm, slot) => (slot || vm.slot ? Object.assign({}, vm.line, { slot: slot || vm.slot }) : vm.line);
  /** The installed font id of a font word on a face of this piece ('' when the page cannot say). */
  function fontIdFor(vm, face, word, slot) {
    const P = Prev(); try { if (P && P.fontIdOf) return str(P.fontIdOf(rowFor(vm, slot), face, word)); } catch (_) { /* below */ }
    const O = dep('CharmNestOrders', './charm-nest-orders.js'); try { return O && O.fontOfPhrase && word ? str(O.fontOfPhrase(word)) : ''; } catch (_) { return ''; }
  }
  /** The drop-down value that stands for a font on a face: '' = the line's own font (the first entry), else the shop's word. Fonts are compared by the installed font they resolve to, never by the word. */
  function fontValueOf(vm, face, f, slot) {
    const S = Setup(), list = S && S.fontChoices ? S.fontChoices() : [], lineKey = str(vm.lineFont && vm.lineFont.word).toLowerCase(), word = str(f && f.fontWord).trim();
    const values = [''].concat(list.filter(c => c.word.toLowerCase() !== lineKey).map(c => c.word));
    if (word) { if (word.toLowerCase() === lineKey) return ''; const hit = values.find(v => v && sameWord(v, word)); if (hit) return hit; }
    const id = str(f && f.fontId) || (word ? fontIdFor(vm, face, word, slot) : '');
    if (id) { const hit = values.find(v => fontIdFor(vm, face, v, slot) === id); if (hit !== undefined) return hit; }
    return word;
  }
  /** What the system chose for the piece: { mode, faces: { front: {text, fontWord, fontId}|null, back: ... } } (a face the system's mode does not engrave is null). */
  function systemChoice(vm, slot) {
    const S = Setup(); let a = null;
    if (S && typeof S.autoChoice === 'function') a = tryDo(() => S.autoChoice(vm.line, slot || vm.slot || undefined, { jobs: vm.jobs }), null);
    if (!a || !MODES.some(([m]) => m === a.mode)) { const b = vm.base || vm.setup || {}; a = { mode: b.mode, front: b.front, back: b.back }; }
    const faces = {};
    for (const f of ['front', 'back']) {
      const x = a[f], on = facesOf(a.mode).includes(f);
      faces[f] = on && x ? { text: normWords(x.lines && x.lines.length ? x.lines : x.text), fontWord: str(x.fontWord).trim(), fontId: str(x.fontId) } : on ? { text: '', fontWord: '', fontId: '' } : null;
    }
    return { mode: a.mode, faces };
  }
  /** The reason a person gave for the saved override (ENGWHY: engravingOverride.reason { code, text, by, at, slot? }), as one read-only line for a title, or ''. ENGWHY's own reader says it when the page has it
   *  ("Changed: Customer asked · Name One · Oct 12, 3:04 PM"); else the same words are made here from the record. */
  function reasonLine(line, slot) {
    const W = root && root.CharmNestEngraveWhy;
    if (W && typeof W.reasonOf === 'function') { const r = tryDo(() => W.reasonOf(line, slot || null), null); return r && str(r.text).trim() ? str(r.title || r.line) : ''; }
    const ov = line && line.engravingOverride, r = ov && typeof ov === 'object' ? ov.reason : null; if (!r || typeof r !== 'object') return '';
    if (slot && r.slot && r.slot !== slot) return '';
    const t = str(r.text).trim() || cap1(str(r.code).replace(/[_-]+/g, ' ').trim()), by = str(r.by).trim();
    return t ? 'Changed: ' + t + (by ? ' \u00b7 ' + by : '') : '';
  }
  /** The pure model of the highlight for the face shown: { sys, mode: {sys, same}, words: {sys, same}|null, font: {sys (drop-down value), label, same}|null, reason }. `same` = what is on the panel now (the staged edit) IS the system's. */
  function hiModel(vm, d, face, slot) {
    const sys = systemChoice(vm, slot), on = facesOf(d.mode).includes(face), sf = face ? sys.faces[face] : null, x = (face && d.faces[face]) || { text: '', font: '' };
    const m = { sys, mode: { sys: sys.mode, same: d.mode === sys.mode }, words: null, font: null, reason: reasonLine(vm.line, slot || vm.slot) };
    if (sf && sf.text) m.words = { sys: sf.text, same: on && sameWords(x.text, sf.text, fontIdFor(vm, face, x.font, slot)) };
    if (sf) {
      const sv = fontValueOf(vm, face, sf, slot), lf = vm.lineFont || {};
      m.font = { sys: sv, label: sv || str(lf.word || lf.name), same: on && fontValueOf(vm, face, { fontWord: x.font }, slot) === sv };
    }
    return m;
  }
  const HI_CSS = `.engpSys{display:inline-flex;align-items:center;justify-content:center;width:14px;height:14px;color:var(--gold,#a9823f);cursor:default}
.engpSys svg{display:block;width:10px;height:10px}
.engpSys[data-st=changed]{opacity:.6}
.engpSeg[data-sys]{position:relative;background:var(--goldSoft,#f0e6cd);box-shadow:inset 0 0 0 1px var(--goldLine,#e3d3a6)}
.engpSeg[data-sys][aria-checked=true]{background:var(--goldSoft,#f0e6cd);box-shadow:0 0 0 1px rgba(28,26,23,.42)}
.engpSeg>.engpSys{position:absolute;top:-4px;right:-3px;width:12px;height:12px;pointer-events:none}
.engpSeg>.engpSys svg{width:9px;height:9px}
.engpBox[data-sys]>.engpWords{padding-right:24px}
.engpBox>.engpSys{position:absolute;top:4px;right:5px}
.engpBox[data-sys=same]>.engpWords{background:var(--goldSoft,#f0e6cd);background:color-mix(in srgb,var(--goldSoft,#f0e6cd) 60%,var(--card,#fffefb))}
.engpFontBox{display:block;position:relative;min-width:0}
.engpFontBox>.engpSys{position:absolute;top:50%;right:30px;transform:translateY(-50%)}
.engpFontBox[data-sys=same]>.engpFont{background:var(--goldSoft,#f0e6cd);background:color-mix(in srgb,var(--goldSoft,#f0e6cd) 60%,var(--card,#fffefb))}`;
  function ensureHiCss(doc) {
    try { if (!doc || doc.getElementById('engpHiCss')) return; const s = doc.createElement('style'); s.id = 'engpHiCss'; s.textContent = HI_CSS; (doc.head || doc.documentElement).appendChild(s); } catch (_) { /* unmarked is still usable */ }
  }
  /** Put the system's marks on a drawn panel (`el` holds the panel), or bring them up to date after a typed word / a picked font (every step finds what it made before). Never moves a control, never throws. */
  function hiDecorate(el, vm, d, slot) {
    try {
      const doc = el && el.ownerDocument, p = el && el.querySelector('.engp'); if (!doc || !p || !vm || !d) return;
      ensureHiCss(doc);
      const face = p.dataset.face || '', m = hiModel(vm, d, face, slot), why = m.reason ? '\n' + m.reason : '';
      const put = (host, st, title) => { let k = host.querySelector(':scope > .engpSys'); if (!k) { k = doc.createElement('i'); k.className = 'engpSys'; k.innerHTML = HI_SPARK; host.appendChild(k); } k.dataset.st = st; k.title = title; return k; };
      const drop = host => { const k = host && host.querySelector(':scope > .engpSys'); if (k) k.remove(); };
      // the mode: the sparkle and the tint stay on the system's segment whatever is selected (the ring is the selection)
      for (const seg of p.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg[data-engp="mode"]')) {
        if (seg.dataset.mode === m.mode.sys) { const t = HI_TITLE + (m.mode.same ? '' : why); seg.dataset.sys = m.mode.same ? 'same' : 'changed'; seg.title = t; put(seg, 'same', t); }
        else { delete seg.dataset.sys; seg.removeAttribute('title'); drop(seg); }
      }
      // the words of the face shown
      const box = p.querySelector('.engpBox');
      if (box) {
        if (m.words) { const t = HI_TITLE + (m.words.same ? '' : ': ' + m.words.sys.replace(/\n/g, ' / ') + why); box.dataset.sys = m.words.same ? 'same' : 'changed'; put(box, m.words.same ? 'same' : 'changed', t); }
        else { delete box.dataset.sys; drop(box); }
      }
      // the font of the face shown: its entry in the list always carries the star; beside the closed drop-down a ghost star once another font is picked
      const sel = p.querySelector('.engpFont');
      if (sel) {
        let fb = sel.parentElement; if (!fb || !fb.classList.contains('engpFontBox')) { fb = doc.createElement('span'); fb.className = 'engpFontBox'; sel.parentNode.insertBefore(fb, sel); fb.appendChild(sel); }
        for (const o of sel.options) { const mine = !!m.font && o.value === m.font.sys; if (mine) { o.label = HI_GLYPH + ' ' + o.text; o.dataset.sys = '1'; } else if (o.dataset.sys) { o.label = ''; o.removeAttribute('label'); delete o.dataset.sys; } }
        if (m.font) { fb.dataset.sys = m.font.same ? 'same' : 'changed'; if (m.font.same) { drop(fb); sel.title = HI_TITLE; } else { put(fb, 'changed', HI_TITLE + ': ' + m.font.label + why); sel.removeAttribute('title'); } }
        else { delete fb.dataset.sys; drop(fb); sel.removeAttribute('title'); }
      }
    } catch (_) { /* the panel works without its marks */ }
  }

  /* ── the markup ────────────────────────────────────────────────────────────── */
  const ICON = {
    left: '<path d="M10 3 5 8l5 5"/>', right: '<path d="m6 3 5 5-5 5"/>', up: '<path d="m3 10 5-5 5 5"/>', down: '<path d="m3 6 5 5 5-5"/>',
    centre: '<circle cx="8" cy="8" r="2.2"/><path d="M8 2v2.2M8 11.8V14M2 8h2.2M11.8 8H14"/>', smaller: '<path d="M4 8h8"/>', larger: '<path d="M4 8h8M8 4v8"/>'
  };
  const POS_BUTTONS = [['left', 'Move left'], ['up', 'Move up'], ['down', 'Move down'], ['right', 'Move right'], ['centre', 'Centre'], ['smaller', 'Smaller'], ['larger', 'Larger']];
  const posButton = (k, t, off) => `<button type="button" class="engpPb" data-engp="pos" data-act="${k}" title="${esc(off || t)}" aria-label="${esc(t)}"${off ? ' disabled' : ''}><svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${ICON[k]}</svg></button>`;
  const NOFIT = 'Placed once the words are fitted', NOROOM = 'No room that way';

  function faceHtml(vm, d, face, id, slot) {
    const cur = vm.rows.find(r => r.face === face), x = d.faces[face] || { text: '', font: '' }, locked = !!(cur && cur.locked), per = !!(cur && cur.perPiece), dis = d.busy ? ' disabled' : '';
    const words = `<div class="engpBox"><textarea class="engpWords" id="${id}" data-engp="words" data-face="${face}" rows="2" placeholder="Words for the ${face}" aria-label="${cap1(face)} words"${locked || per || d.busy ? ' disabled' : ''}>${esc(per ? '' : x.text)}</textarea>${locked || per ? '' : `<button type="button" class="btn ghost xs engpEmoji emBtn" data-emoji${d.busy ? ' disabled' : ''} title="Choose an emoji the laser can engrave and put it in the words">Emoji</button>`}</div>`;
    const font = `<select class="engpFont" data-engp="font" data-face="${face}" aria-label="${cap1(face)} font"${locked || d.busy ? ' disabled' : ''}>${fontOptions(vm, x.font)}</select>`;
    const job = faceJobOf(vm, face, slot), P = Pos();
    const can = !job || locked ? false : P && P.canMove ? tryDo(() => P.canMove(job), false) : !!(job.fit && job.state === 'review');
    const off = job && job.fit && job.fit.artOnly ? 'No words to move' : locked || (job && !can && job.state && job.state !== 'review' && job.state !== 'approved' && job.state !== 'written') ? 'Approved: nothing moves' : !can ? NOFIT : '';   // (EDITOR WORDS: a face engraved by the charm editor's parts alone has no words to move)   // (NODECIDED: an approved piece's position buttons work: the first press reopens it for Approve)
    const stuck = (job && d.noRoom && d.noRoom[job.key]) || {};
    const pos = P ? `<div class="engpPos"><span class="engpLbl">Position</span><span class="engpRow" role="group" aria-label="${cap1(face)} position">${POS_BUTTONS.map(([k, t]) => posButton(k, t, d.busy ? NOFIT : off || (stuck[k] ? NOROOM : ''))).join('')}</span></div>` : '';
    const lock = cur && cur.decided ? `<div class="engpLock" role="status">${cur.lockState === 'skipped' ? 'Cut plain' : cur.lockState === 'written' ? 'Saved to sheet' : 'Approved'}${cur.lockBy ? ' by ' + esc(cur.lockBy) : ''}</div>` : per ? `<div class="engpLock" role="status">Differs per piece</div>` : '';
    return `<div class="engpFace" data-face="${face}">${words}${font}${pos}${lock}</div>`;
  }
  /** The reason prompt (ENGWHY, Paul point 7): shown IN PLACE of the Save / Cancel row when a Save would override the system's choice. Four quick buttons (a press saves at once), one small text box for
   *  "something else" with its own Save, and Cancel (back to the panel, the edit still staged; Esc does the same). */
  function askHtml(d) {
    const W = Why(), text = str(d.ask && d.ask.text);
    return `<div class="engpWhy" role="group" aria-label="Why change it?"><div class="engpWhyQ">Why change it?</div><div class="engpChips">${((W && W.REASONS) || []).map(r => `<button type="button" class="engpChip" data-engp="why" data-code="${esc(r.code)}">${esc(r.label)}</button>`).join('')}</div>`
      + `<div class="engpWhyRow"><input type="text" class="engpWhyText" data-engp="whyText" maxlength="120" placeholder="Something else" aria-label="Another reason" autocomplete="off" value="${esc(text)}"><button type="button" class="btn gold xs" data-engp="whySave"${text.trim() ? '' : ' disabled'}>Save</button><button type="button" class="btn ghost xs" data-engp="whyCancel">Cancel</button></div></div>`;
  }
  /** The panel for a view model (`vm` = CharmNestEngraveSetup.read) and its staged edit (`d`). ctx: { slot } */
  function html(vm, d, ctx) {
    ctx = ctx || {};
    const S = Setup(), id = 'engp' + (++uid), slot = ctx.slot === undefined ? vm.slot || null : ctx.slot, on = facesOf(d.mode), face = on.includes(d.face) ? d.face : on[0] || '';
    const modes = modesFor(vm.line), dis = d.busy ? ' disabled' : '';
    const seg = `<div class="engpMode" role="radiogroup" aria-label="Engraving">${modes.map(([m, t]) => `<button type="button" role="radio" class="engpSeg" data-engp="mode" data-mode="${m}" aria-checked="${d.mode === m}"${dis}>${t}</button>`).join('')}</div>`;
    const sw = ctx.faceSwitch && on.length > 1 ? `<div class="engpMode engpFaceSw" role="radiogroup" aria-label="Face">${on.map(f => `<button type="button" role="radio" class="engpSeg" data-engp="face" data-face="${f}" aria-checked="${f === face}"${dis}>${cap1(f)}</button>`).join('')}</div>` : '';
    // (EDITOR WORDS: a face the charm editor drew is never asked for words: one calm line says they are optional while its box is empty)
    const art = artOn(vm, face), typed = !!(face && d.faces[face] && linesOf(d.faces[face].text).length);
    const ask = art ? (typed ? '' : `<div class="engpOffer" role="status">No words needed: the charm editor's parts are the ${esc(face)}'s engraving. Type words only to add them.</div>`)
      : vm.needsPerson && d.mode === vm.mode ? `<div class="engpAsk" role="status">${esc(vm.prompt)}</div>${vm.raw ? `<div class="engpRaw" aria-label="The buyer's own words">${esc(vm.raw)}</div>` : ''}` : '';
    const patch = S.patchOf(vm, d), dirty = hasPatch(patch), staged = dirty || d.posTouched;
    const cons = dirty ? S.consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup) : null;
    const warn = cons && cons.blocked ? `<div class="engpErr" role="alert">${esc(cons.blocked)}</div>` : '';
    const err = d.error ? `<div class="engpErr" role="alert">${esc(d.error)}</div>` : '';
    const confirm = d.confirm && d.confirmLines && d.confirmLines.length ? `<div class="engpConfirm" role="status">${d.confirmLines.map(t => `<span>${esc(t)}</span>`).join('')}<span class="engpActs"><button type="button" class="btn gold xs" data-engp="saveYes"${dis}>Cut it plain and save</button><button type="button" class="btn ghost xs" data-engp="keep">Keep it as it is</button></span></div>` : '';
    const spin = d.busy ? `<span class="engpStat" role="status"><span class="owSpin" aria-hidden="true"></span>${esc(d.busy)}</span>` : '';
    const canSave = staged && !d.busy && !(cons && cons.blocked) && !d.confirm;
    const acts = `<div class="engpActs"><button type="button" class="btn gold xs" data-engp="save"${canSave ? '' : ' disabled'}>Save</button><button type="button" class="btn ghost xs" data-engp="cancel"${(staged || d.dirty) && !d.busy ? '' : ' disabled'}>Cancel</button>${spin}</div>`;
    const W = Why(), rs = !d.ask && !d.busy && W && W.reasonOf ? W.reasonOf(vm.line, vm.slot || null) : null;   // (the reason of the saved override, read-only, one quiet line)
    const why = rs ? `<div class="engpWhyLine" title="${esc(rs.title)}">${esc(rs.line)}</div>` : '';
    return `<div class="engp" data-key="${esc(d.key)}" data-mode="${esc(d.mode)}" data-face="${esc(face)}" role="group" aria-label="Engraving">${seg}${sw}${face ? faceHtml(vm, d, face, id, slot) : ''}${ask}${warn}${err}${confirm}${d.ask ? askHtml(d) : acts}${why}</div>`;
  }

  /* ── the look ──────────────────────────────────────────────────────────────── */
  const CSS = `.engp{display:grid;gap:10px;min-width:0;container-type:inline-size;font:12px var(--sans,system-ui);color:var(--ink,#1c1a17)}
.engp[hidden]{display:none!important}
.engpCard{margin-bottom:10px}
.engpCard:empty{display:none}
.engpMode{display:flex;flex-wrap:nowrap;gap:2px;padding:2px;min-width:0;border:1px solid var(--line,#e4ddd0);border-radius:9px;background:var(--paper2,#ebe5d9)}
.engpSeg{flex:1 1 auto;min-width:0;white-space:nowrap;padding:5px 8px;border:0;border-radius:7px;background:transparent;font:600 11px var(--sans,system-ui);color:inherit;cursor:pointer}
.engpSeg[aria-checked=true]{background:var(--card,#fffefb);box-shadow:0 0 0 1px rgba(28,26,23,.42)}
.engpSeg:disabled{opacity:.55;cursor:default}
@container (max-width:239px){.engpSeg{padding:5px 4px;font-size:10.5px}}
.engpFace{display:grid;gap:8px;min-width:0}
.engpWords,.engpFont{display:block;width:100%;min-width:0;box-sizing:border-box;border:1px solid var(--line,#e4ddd0);border-radius:8px;background:var(--card,#fffefb);color:inherit}
.engpBox{position:relative;min-width:0}
.engpWords{min-height:64px;resize:none;padding:6px 8px 26px;font:13px/1.4 var(--sans,system-ui);overflow-y:auto}
.engpEmoji{position:absolute;right:5px;bottom:5px;padding:2px 8px}
.engpFont{padding:6px 8px;font:13px var(--sans,system-ui)}
.engpWords:disabled,.engpFont:disabled{background:var(--paper2,#ebe5d9);color:var(--ink70,#5b554c)}
.engpPos{display:grid;gap:4px}
.engpLbl{font:600 10.5px var(--sans,system-ui);letter-spacing:.06em;text-transform:uppercase;color:var(--ink45,#938c80)}
.engpRow{display:flex;gap:4px;min-width:0}
.engpPb{flex:0 0 28px;width:28px;height:28px;padding:0;display:inline-flex;align-items:center;justify-content:center;border:1px solid var(--line,#e4ddd0);border-radius:7px;background:var(--card,#fffefb);color:inherit;cursor:pointer}
.engpPb:hover:not(:disabled){background:var(--paper2,#ebe5d9)}
.engpPb:disabled{opacity:.4;cursor:default}
.engpLock{font-size:11.5px;color:var(--ink45,#938c80)}
.engpAsk{font-size:12px;font-weight:600;color:var(--clay,#b0563f)}
.engpOffer{font-size:12px;line-height:1.4;color:var(--ink70,#5b554c)}
.engpRaw{padding:5px 8px;border-radius:8px;background:var(--card2,#faf7f1);border:1px solid var(--line,#e4ddd0);font:600 12.5px var(--serif,Georgia,serif);white-space:pre-line;overflow-wrap:anywhere}
.engpErr{font-size:11.5px;color:var(--clay,#b0563f)}
.engpConfirm{display:grid;gap:4px;font-size:11.5px;color:var(--clay,#b0563f)}
.engpActs{display:flex;align-items:center;flex-wrap:wrap;gap:4px 6px}
.engpStat{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;color:var(--ink70,#5b554c)}
.engpWhy{display:grid;gap:6px;min-width:0}
.engpWhyQ{font:600 11.5px var(--sans,system-ui);color:var(--ink70,#5b554c)}
.engpChips{display:flex;flex-wrap:wrap;gap:4px}
.engpChip{padding:5px 9px;border:1px solid var(--line,#e4ddd0);border-radius:999px;background:var(--card,#fffefb);font:600 11px var(--sans,system-ui);color:inherit;white-space:nowrap;cursor:pointer}
.engpChip:hover{background:var(--paper2,#ebe5d9)}
.engpWhyRow{display:flex;align-items:center;gap:4px;min-width:0}
.engpWhyText{flex:1 1 70px;min-width:0;box-sizing:border-box;padding:5px 8px;border:1px solid var(--line,#e4ddd0);border-radius:8px;background:var(--card,#fffefb);font:12px var(--sans,system-ui);color:inherit}
.engpWhyLine{font-size:11px;line-height:1.35;color:var(--ink45,#938c80);overflow-wrap:anywhere}`;
  function ensureCss(doc) {
    try { if (!doc || doc.getElementById('engpCss')) return; const s = doc.createElement('style'); s.id = 'engpCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); } catch (_) { /* unstyled is still usable */ }
  }
  const growBox = ta => { try { const cs = ta.ownerDocument.defaultView.getComputedStyle(ta), lh = parseFloat(cs.lineHeight) || 18, pad = (parseFloat(cs.paddingTop) || 0) + (parseFloat(cs.paddingBottom) || 0), max = Math.round(lh * 4 + pad + 2), min = parseFloat(cs.minHeight) || 0; ta.style.height = 'auto'; ta.style.height = Math.max(min, Math.min(ta.scrollHeight + 2, max)) + 'px'; } catch (_) { /* the box keeps its rows */ } };

  /* ── wiring ────────────────────────────────────────────────────────────────── */
  const hostSave = (line, patch) => { const E = Eng(); return E && E.saveSetup ? E.saveSetup(line, patch) : Promise.resolve({ ok: false, message: 'The engraving is not loaded yet.' }); };
  /** Wire the panel inside `el` for the piece of `line`. */
  function bind(el, line, opts) {
    ensureCss(el.ownerDocument);
    let dead = false;
    const slot = opts.slot !== undefined ? opts.slot : opts.job ? slotOfJob(opts.job) : null, key = pieceKeyOf(line, slot);
    const jobs = () => (opts.jobs ? opts.jobs() : (Eng() && Eng().jobsOf ? Eng().jobsOf(line) : [])) || [];
    const me = { el, key, draw: null };
    sweep(key); watch(el.ownerDocument);   // (another piece was open before: its staged edit ends here)
    const track = () => { let set = LIVE.get(key); if (!set) LIVE.set(key, set = new Set()); for (const b of [...set]) if (b !== me && b.el && !b.el.isConnected) set.delete(b); set.add(me); };   // (a card drawn again leaves its old panel behind: forgotten here)
    const untrack = () => { const set = LIVE.get(key); if (set) { set.delete(me); if (!set.size) LIVE.delete(key); } };
    const model = () => {
      const S = Setup(), vm = S && S.read ? S.read(line, { slot: slot || undefined, jobs: jobs() }) : null; if (!vm) return null;
      const d = draftOf(vm, key), shownKey = opts.job ? str(opts.job.key) : '';
      if (shownKey && d.shown !== shownKey) { d.shown = shownKey; if (opts.face) d.face = opts.face; }   // (the card was drawn for another job: its face is the one to edit)
      const on = facesOf(d.mode); if (!on.includes(d.face)) d.face = on[0] || d.face;
      return { vm, d };
    };
    const focusState = () => { const a = el.ownerDocument.activeElement; if (!a || !el.contains(a) || !a.dataset || !a.dataset.engp) return null; return { engp: a.dataset.engp, face: a.dataset.face || '', mode: a.dataset.mode || '', act: a.dataset.act || '', start: a.selectionStart, end: a.selectionEnd }; };
    const restoreFocus = want => {
      if (!want || dead) return;
      const sel = `[data-engp="${want.engp}"]${want.face ? `[data-face="${want.face}"]` : ''}${want.mode ? `[data-mode="${want.mode}"]` : ''}${want.act ? `[data-act="${want.act}"]` : ''}`, t = el.querySelector(sel);
      if (t && !t.disabled && el.ownerDocument.activeElement !== t) { try { t.focus({ preventScroll: true }); if (want.start != null && t.setSelectionRange) t.setSelectionRange(want.start, want.end); } catch (_) { /* no caret to give back */ } }
    };
    /** ENGWORK: what the big work-area drawing shows for a face while words, font or mode are staged: the staged edit of that face's job goes to Engrave.draftFit (a draft: never saved, approved or sent anywhere),
     *  or null when nothing is staged (Cancel, after a Save, a panel just opened) so the fitted engraving is drawn again. `opts.onDraft(faceKey, staged)` replaces the default. The ONE place it is called is `paint`
     *  below, which every input, change, mode, position, Cancel and Save already ends in. */
    const draftOut = (vm, d, face) => {
      try {
        const job = faceJobOf(vm, face, slot); if (!job || !job.key) return;
        const x = d.faces[face] || { text: '', font: '' }, staged = hasPatch(Setup().patchOf(vm, d)) ? { mode: d.mode, face, text: x.text, fontWord: x.font || '' } : null;
        if (typeof opts.onDraft === 'function') opts.onDraft(job.key, staged); else { const E = Eng(); if (E && E.draftFit) E.draftFit(job.key, staged); }
      } catch (_) { /* the work area keeps what it drew */ }
    };
    /** What a face's thumbnail shows right now: the staged words and font, the face's own position. */
    const paint = (vm, d, face) => {
      draftOut(vm, d, face);
      const P = Prev(); if (!P || !P.update) return;
      const host = opts.thumb ? tryDo(() => opts.thumb(face), null) : tryDo(() => (opts.scope || el.ownerDocument).querySelector(`.engFace[data-face="${face}"] .engThumb`), null);
      if (!host) return;
      const on = facesOf(d.mode).includes(face), x = d.faces[face] || { text: '', font: '' }, job = faceJobOf(vm, face, slot);
      const part = { text: on ? linesOf(x.text).join('\n') : '', face };
      try { const id = P.fontIdOf ? P.fontIdOf(vm.line, face, x.font) : null; if (id) part.fontId = id; } catch (_) { /* the preview keeps its font */ }
      const pos = positionOf(job); if (pos) part.position = { x: pos.x, y: pos.y, size: pos.size, angle: pos.angle };
      tryDo(() => P.update(host, part));
    };
    const paintAll = (vm, d) => { for (const f of ['front', 'back']) paint(vm, d, f); };
    /* While words, font or mode are staged and not saved, the card's Approve buttons (and its A key) must not approve the OLD fitted words: they are dimmed with a title and a press is stopped before the card sees it. */
    const GATE = '[data-a="approve"],[data-a="approveBoth"],[data-a="approveFaces"]', GATE_WHY = 'Save or Cancel the edit first';
    let gateCard = null, gateOn = false;
    /* APPROVEFIX (Paul, 28 Oct 2026, order 4212020155: "The 'Approve' button is not working after I made adjustments to this charm's engraving."): the gate never stops a press without a word, and never
     * stops one for an edit that is not staged any more. A press it stops says why in the card, under the button (the same line the card uses for a press that did not approve: .egApproveWhy), and while the
     * gate is on that line is there with the dimmed button (a tooltip alone said nothing). A press arriving with nothing staged (the draft ended without this panel being drawn again) lifts the gate and goes on. */
    const stagedNow = () => { const m = dead ? null : tryDo(() => model(), null); return !!m && hasPatch(Setup().patchOf(m.vm, m.d)); };
    const gateSay = on => {
      const host = gateCard && gateCard.querySelector('.pvApproval'); if (!host) return;
      let n = host.querySelector('.egApproveWhy');
      if (!on) { if (n && n.textContent === GATE_WHY) n.remove(); return; }
      if (!n) { n = gateCard.ownerDocument.createElement('div'); n.className = 'egApproveWhy'; n.setAttribute('role', 'status'); host.appendChild(n); }
      if (n.textContent !== GATE_WHY) n.textContent = GATE_WHY;
    };
    const stop = e => { if (!stagedNow()) { applyGate(false); return false; } e.preventDefault(); e.stopImmediatePropagation(); gateSay(true); return true; };
    const blockClick = e => { if (gateOn && e.target && e.target.closest && e.target.closest(GATE)) stop(e); };
    const blockKey = e => { if (gateOn && e.key && e.key.toLowerCase() === 'a' && !e.metaKey && !e.ctrlKey && !e.altKey && e.target && e.target.closest && !e.target.closest('input,textarea,select,[contenteditable]')) stop(e); };
    const hookCard = () => {
      const card = el.closest ? el.closest('.rvItem') : null; if (!card || card === gateCard) return;
      if (gateCard) { gateCard.removeEventListener('click', blockClick, true); gateCard.removeEventListener('keydown', blockKey, true); }
      card.addEventListener('click', blockClick, true); card.addEventListener('keydown', blockKey, true); gateCard = card;
    };
    const applyGate = on => {
      gateOn = !!on; hookCard(); if (!gateCard) return;
      for (const b of gateCard.querySelectorAll(GATE)) {
        if (on) { if (!b.dataset.engpWas) b.dataset.engpWas = JSON.stringify([b.disabled, b.title]); b.disabled = true; b.title = GATE_WHY; }
        else if (b.dataset.engpWas) {
          const [dis, ttl] = tryDo(() => JSON.parse(b.dataset.engpWas), [b.disabled, b.title]); delete b.dataset.engpWas;
          if (b.title === GATE_WHY) { b.disabled = !!dis; b.title = ttl; }   // (APPRFIX-A: only if the button is still as the gate left it. The card brings "Approve both faces" up to date a frame after it is drawn (syncBoth), and a card drawn with an edit staged was gated BEFORE that: the old "Look at the front placement first" came back after Cancel and the button stayed dead)
        }
      }
      gateSay(gateOn && !!gateCard.querySelector(GATE));   // (APPROVEFIX: the reason in words in the card while the gate is on)
    };
    /** ENGFRONT: the face being edited has NO job to draw (the front is asked for in the mode and not saved yet, or its job is "no engraving"): the work area would go on drawing the OTHER face's engraving
     *  while this face's thumbnail is marked. It says so in one plain line instead (the card's own empty-box markup; the drawing under it is hidden while the line is there), and the line goes with the next draw. */
    const noJob = (vm, d) => {
      const card = el.closest ? el.closest('.rvItem') : null, host = card ? card.querySelector('.backHost') : null; if (!card || !host) return;
      const job = facesOf(d.mode).includes(d.face) ? faceJobOf(vm, d.face, slot) : undefined, old = host.querySelector('.engpNoJob');
      const want = job === null ? `Save to see the ${d.face} engraving here.` : job && job.state === 'none' ? `No ${d.face} engraving.` : '';
      if (!want) { if (old) old.remove(); delete card.dataset.engpNojob; return; }
      card.dataset.engpNojob = d.face;
      const box = old || host.appendChild(el.ownerDocument.createElement('div'));
      box.className = 'noBack engpNoJob'; box.setAttribute('role', 'status'); if (box.textContent !== want) box.textContent = want;
    };
    const draw = () => {
      if (dead) return;
      const m = model(), fs = focusState();
      if (!m || !modesFor(line).length) { untrack(); el.innerHTML = ''; el.hidden = true; if (opts.onVisible) tryDo(() => opts.onVisible(false)); return; }
      track(); el.hidden = false; if (opts.onVisible) tryDo(() => opts.onVisible(true));
      const { vm, d } = m;
      el.innerHTML = html(vm, d, { slot, faceSwitch: !!opts.faceSwitch });
      const ta = el.querySelector('.engpWords'); if (ta) growBox(ta);
      { const fs = el.querySelector('.engpFont'); if (fs && fs.selectedOptions[0] && fs.selectedOptions[0].style.fontFamily) fs.style.fontFamily = fs.selectedOptions[0].style.fontFamily; }
      { const eb = el.querySelector('[data-emoji]'), tb = el.querySelector('.engpWords'), CE = el.ownerDocument.defaultView && el.ownerDocument.defaultView.CNEmojiPicker;   // (Paul's own Emoji button, as the words box of the card had it: it types into the box, so the panel sees an ordinary input)
        if (eb && !(CE && tb && tryDo(() => CE.attach({ textarea: tb, button: eb, key: 'engp:' + key + ':' + d.face }), false))) eb.remove(); }
      hiDecorate(el, vm, d, slot);   // (ENGHILITE: the system's own choice stays marked)
      hookCard(); applyGate(hasPatch(Setup().patchOf(vm, d)));
      noJob(vm, d);
      const P = Prev(); if (P && P.warm && !draw.warmed) { draw.warmed = true; tryDo(() => P.warm()); }
      const want = fs || (d.focus && el.ownerDocument.activeElement === el.ownerDocument.body ? d.focus : null);
      restoreFocus(want);
      if (want && !el.isConnected) Promise.resolve().then(() => restoreFocus(want));   // (a card is built before it is put in the page)
    };
    me.draw = draw;
    const redrawLive = () => { const set = LIVE.get(key); for (const b of [...(set || [])]) { if (b !== me && b.el && !b.el.isConnected) { set.delete(b); continue; } tryDo(() => b.draw()); } };
    const changed = (kind, face) => { try { if (typeof opts.onChange === 'function') opts.onChange({ face, kind, key }); } catch (_) { /* the host's own */ } };
    const setFace = face => {
      const m = model(); if (!m || !['front', 'back'].includes(face) || modesFor(line).length === 0) return;
      const { vm, d } = m;
      if (!facesOf(d.mode).includes(face)) return;   // (a face the engraving does not need is not drawn, so it cannot be picked; the mode control turns a face on)
      d.face = face; d.focus = null; draw(); paintAll(vm, d);
      try { if (typeof opts.onFace === 'function') opts.onFace(face); } catch (_) { /* the host's own */ }
    };

    const done = (m, res) => {
      const { vm, d } = m; let ok = false;
      d.busy = '';
      if (res && res.cancelled) { /* the name was put away: calm, nothing said */ }
      else if (!res || !res.ok) d.error = (res && res.message) || 'Not saved. Press Save to try again.';
      else { ok = true; DRAFTS.delete(key); }
      redrawLive();
      if (ok) { const m2 = model(); if (m2) paintAll(m2.vm, m2.d); changed('saved', d.face); try { if (res.pending) { /* the host already said it */ } } catch (_) { /* nothing */ } }
    };
    const run = async (m, patch) => {
      m.d.error = ''; m.d.confirm = ''; m.d.busy = 'Saving…'; draw();
      let res; try { res = await (opts.save ? opts.save(patch) : hostSave(line, Object.assign({}, patch, slot ? { slot } : {}))); } catch (e) { res = { ok: false, message: `Not saved: ${e && e.message ? e.message : e}` }; }
      done(m, res);
    };
    const cancel = m => {
      const { vm, d } = m, P = Pos();
      if (P && P.restore) for (const [k, snap] of Object.entries(d.snaps)) { if (!snap || !d.moved[k]) continue; const job = ['front', 'back'].map(f => faceJobOf(vm, f, slot)).find(j => j && j.key === k); if (job) tryDo(() => P.restore(job, snap)); }
      DRAFTS.delete(key); draw(); const m2 = model(); if (m2) paintAll(m2.vm, m2.d); changed('cancel', d.face);
    };
    /* ── the reason (ENGWHY) ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
     * A Save that would override what the SYSTEM chose (CharmNestEngraveSetup.diffFromAuto: the mode, a face's words or font; the position when the person moved the words with these buttons) asks
     * "Why change it?" first, in place of the Save / Cancel row. A quick button saves at once with that reason; the text box has its own Save; Cancel (or Esc) goes back with the edit still staged.
     * A Save equal to the system's choice asks nothing. The reason and the system's choice go in the SAME patch, so the existing single save writes them (no extra write, read or listener). */
    const posMoved = d => !!d.posTouched && Object.keys(d.moved || {}).some(k => jobs().some(j => j && j.key === k && j.nudged));
    const changesOf = (vm, d) => { const W = Why(); return W && W.changes ? tryDo(() => W.changes(vm, d, { jobs: jobs(), positionMoved: posMoved(d) }), null) : null; };
    const trySave = (m, confirmed) => {
      const { vm, d } = m, S = Setup(), patch = S.patchOf(vm, d), ch = changesOf(vm, d), W = Why();
      const posOnly = !hasPatch(patch) && !!ch && ch.any && ch.of.includes('position');   // (only the position was moved, and it is an override: it too gets its reason)
      if (!hasPatch(patch) && !posOnly) { DRAFTS.delete(key); draw(); changed('saved', d.face); return; }   // (only the position was moved: it is kept as it is)
      const cons = hasPatch(patch) ? S.consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup) : null;
      if (cons && cons.blocked) { d.error = cons.blocked; draw(); return; }
      if (ch && ch.any && !d.why) {   // (asked once: the answer stays with the draft until the person edits again)
        d.ask = d.ask || { text: '' }; d.confirm = ''; d.error = ''; draw();
        const first = el.querySelector('[data-engp="why"]'); if (first) tryDo(() => first.focus({ preventScroll: true }));
        const box = el.querySelector('.engpWhy'); if (box && box.scrollIntoView) tryDo(() => box.scrollIntoView({ block: 'nearest' }));   // (the question is taller than the row it replaces: bring all of it into view)
        return;
      }
      if (cons && !confirmed && cons.plain.length) { d.confirm = 'save'; d.confirmLines = cons.lines.filter(l => /already/.test(l)); draw(); return; }
      run(m, W && W.patchWith ? W.patchWith(patch, d.why, ch) : patch);
    };
    const pickWhy = (m, why) => { const d = m.d, W = Why(); d.why = { code: why.code, text: str(why.text) || (W && W.labelOf ? W.labelOf(why.code) : '') }; d.ask = null; trySave(m, false); };
    const closeAsk = m => { m.d.ask = null; draw(); const sv = el.querySelector('[data-engp="save"]'); if (sv && !sv.disabled) tryDo(() => sv.focus({ preventScroll: true })); };
    const click = e => {
      const b = e.target && e.target.closest && e.target.closest('[data-engp]'); if (!b || !el.contains(b) || b.disabled || dead) return;
      const m = model(); if (!m) return; const { vm, d } = m, a = b.dataset.engp;
      if (d.busy) return;
      if (a === 'face') setFace(b.dataset.face);
      else if (a === 'mode') { if (b.dataset.mode === d.mode) return; d.why = null; setMode(vm, d, b.dataset.mode); draw(); paintAll(vm, d); changed('mode', d.face); }
      else if (a === 'cancel') cancel(m);
      else if (a === 'keep') { d.confirm = ''; d.confirmLines = null; draw(); }
      else if (a === 'save' || a === 'saveYes') trySave(m, a === 'saveYes');
      else if (a === 'why') pickWhy(m, { code: b.dataset.code });
      else if (a === 'whySave') { const t = str(d.ask && d.ask.text).replace(/\s+/g, ' ').trim(); if (t) pickWhy(m, { code: 'other', text: t }); }
      else if (a === 'whyCancel') closeAsk(m);
      else if (a === 'pos') {
        const P = Pos(), job = faceJobOf(vm, d.face, slot); if (!P || !job || !job.fit) return;
        const step = e.shiftKey ? 2 : 0.5, act = b.dataset.act; snapBeforeMove(d, job); d.why = null;
        let r = null;
        if (act === 'left') r = P.nudge(job, -step, 0); else if (act === 'right') r = P.nudge(job, step, 0);
        else if (act === 'up') r = P.nudge(job, 0, step); else if (act === 'down') r = P.nudge(job, 0, -step);
        else if (act === 'centre') r = P.centre(job); else if (act === 'smaller') r = P.size(job, 0.95); else if (act === 'larger') r = P.size(job, 1.05);
        // no room (refused, or stopped at the edge of the safe area): that button looks off, with its title; any other move that works gives the room back
        const stuck = !!r && (r.ok === false || r.clamped === true);
        if (stuck) d.noRoom[job.key] = { [act]: true }; else delete d.noRoom[job.key];
        if (r && r.ok === false) { draw(); return; }          // (the text stays; the page already said so)
        d.posTouched = true; d.moved[job.key] = true; draw(); paintAll(vm, d); changed('position', d.face);
      }
    };
    const input = e => {
      const t = e.target; if (!t || !t.dataset || !t.dataset.engp || dead) return;
      if (t.dataset.engp === 'whyText') { const m0 = model(); if (m0 && m0.d.ask) { m0.d.ask.text = t.value; const ws = el.querySelector('[data-engp="whySave"]'); if (ws) ws.disabled = !t.value.trim(); } return; }   // (the "something else" box of the reason prompt)
      const m = model(); if (!m) return; const { vm, d } = m, f = t.dataset.face; if (!f) return;
      const x = d.faces[f] || (d.faces[f] = { text: '', font: '' });
      if (t.dataset.engp === 'words') { x.text = t.value; growBox(t); } else if (t.dataset.engp === 'font') x.font = t.value; else return;
      d.dirty = true; d.confirm = ''; d.error = ''; d.noRoom = {}; d.why = null;
      const S = Setup(), p = S.patchOf(vm, d), sv = el.querySelector('[data-engp="save"]'), cn = el.querySelector('[data-engp="cancel"]');
      if (sv) sv.disabled = !hasPatch(p) && !d.posTouched;
      applyGate(hasPatch(p));
      if (cn) cn.disabled = false;
      if (t.dataset.engp === 'font' && t.tagName === 'SELECT') t.style.fontFamily = (t.selectedOptions[0] && t.selectedOptions[0].style.fontFamily) || '';
      hiDecorate(el, vm, d, slot);   // (ENGHILITE: the tint follows the staged words / font; the marks stay)
      paint(vm, d, f); changed(t.dataset.engp, f);
    };
    const key_ = e => {
      const t = e.target; if (!t || !t.dataset || !t.dataset.engp) return;
      if (e.key === 'Escape') { const m0 = model(); if (m0 && m0.d.ask) { e.preventDefault(); e.stopPropagation(); closeAsk(m0); return; } }   // (Esc leaves the reason prompt: the edit stays staged)
      if (t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.tagName === 'INPUT') e.stopPropagation();   // (the card's own keys, S for "No engraving", are for the card, not for words)
      if (t.dataset.engp === 'whyText' && e.key === 'Enter') { e.preventDefault(); const ws = el.querySelector('[data-engp="whySave"]'); if (ws && !ws.disabled) ws.click(); return; }
      if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && t.tagName === 'TEXTAREA') { e.preventDefault(); const s = el.querySelector('[data-engp="save"]'); if (s && !s.disabled) s.click(); }
      else if (e.key === 'Escape' && t.tagName === 'TEXTAREA') { e.preventDefault(); const c = el.querySelector('[data-engp="cancel"]'); if (c && !c.disabled) c.click(); }
    };
    const focusin = e => { const t = e.target; if (!t || !t.dataset || !t.dataset.engp || dead) return; const m = model(); if (m) m.d.focus = { engp: t.dataset.engp, face: t.dataset.face || '', mode: t.dataset.mode || '', act: t.dataset.act || '', start: t.selectionStart, end: t.selectionEnd }; };
    const focusout = e => { if (e.relatedTarget) { const m = model(); if (m) m.d.focus = null; } };
    const ev = [['click', click], ['input', input], ['change', input], ['keydown', key_], ['focusin', focusin], ['focusout', focusout]];
    for (const [t, f] of ev) el.addEventListener(t, f);
    // the first paint of the thumbnails: the face's own words and position (the card drew them from the job; this makes them the staged ones)
    /* ROTBAR: a turn of the WHOLE piece made with the work area's degree box (the bottom row of the card, charm-nest-bridge.js turnTo) is a position change like the Position buttons: `beforeMove(job)` keeps the placement as it was
     * (once, until Save or Cancel), `moved(job, kind)` marks the face moved, so Save and Cancel come on (Save asks "Why change it?" as for any position change; Cancel turns the piece back). It changes no word, font or mode. */
    const beforeMove = job => { const m = model(); if (!m || !job || !job.key) return false; snapBeforeMove(m.d, job); return true; };
    const moved = (job, kind) => {
      const m = model(); if (!m || !job || !job.key) return false; const { vm, d } = m;
      d.posTouched = true; d.moved[job.key] = true; d.why = null; d.noRoom = {}; draw(); paintAll(vm, d); changed(kind || 'position', d.face); return true;
    };
    return { draw, refresh: draw, setFace, beforeMove, moved, destroy() { dead = true; untrack(); sweepSoon(); for (const [t, f] of ev) el.removeEventListener(t, f); gateOn = false; if (gateCard) { gateCard.removeEventListener('click', blockClick, true); gateCard.removeEventListener('keydown', blockKey, true); } }, paintAll: () => { const m = model(); if (m) paintAll(m.vm, m.d); } };
  }

  const BOUND = new WeakMap();
  /** THE ONE-LINE MOUNT of the panel. */
  function mount(slot, row, opts) {
    const none = { destroy() {}, refresh() {}, setFace() {}, draw() {} };
    try {
      const line = lineOf(row);
      if (!slot || typeof slot.addEventListener !== 'function' || !line || !line.spec) return none;
      const was = BOUND.get(slot); if (was) was.destroy();
      const b = bind(slot, line, opts || {});
      BOUND.set(slot, b); b.draw(); if (b.paintAll) tryDo(() => b.paintAll());
      return b;
    } catch (e) { if (typeof console !== 'undefined') console.warn('engraving panel', e); return none; }
  }

  /** A line's piece name, as the order window names it (a necklace of saved charms by all of them, else the design's SKU, the line's SKU or title). */
  function lineName(x) {
    const O = dep('CharmNestOrders', './charm-nest-orders.js'), set = O && O.setDesignsOf ? tryDo(() => O.setDesignsOf(x && x.spec), null) : null;
    return str(set && set.length ? set.join(' + ') : (x && x.spec && x.spec.designSku) || (x && x.line && (x.line.sku || x.line.title)) || '').replace(/_+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, set && set.length ? 80 : 32);
  }

  /** The slot of the Engrave detail card: the Front / Back thumbnails (when the page has CharmNestEngraveCard) above the panel, each click on a thumbnail picking the face the panel edits. */
  function mountPiece(slot, job, o) {
    const none = { destroy() {}, refresh() {}, setFace() {}, panel: null, card: null };
    try {
      if (!slot || !job || !job.row) return none;
      o = o || {};
      const doc = slot.ownerDocument, F = Faces(), face = F ? F.faceOf(job) : 'back', line = lineOf(job.row);
      slot.textContent = '';
      const cardHost = doc.createElement('div'); cardHost.className = 'engpCard'; const panelHost = doc.createElement('div'); panelHost.className = 'engpHost';
      slot.appendChild(cardHost); slot.appendChild(panelHost);
      const C = root.CharmNestEngraveCard; let card = null, panel = null;
      const pk = pieceKeyOf(line, slotOfJob(job)), stage = () => stageOf(pk);
      const pick = f => { if (panel) panel.setFace(f); };
      if (C && C.mountPiece) card = tryDo(() => C.mountPiece(cardHost, { row: line, key: job.key, face, stage: stage(), onPick: pick, approve: false, nameOf: o.nameOf || lineName, isWorking: Eng() && Eng().isWorking ? Eng().isWorking : undefined }), null);
      if (!card) cardHost.remove();
      /** The card follows the STAGED edit: the faces it draws are the ones the staged mode asks for (the same tick), the face being edited is marked, and the thumbnails get the staged words back. */
      const follow = f => { if (!card || !card.update) return; tryDo(() => card.update({ face: f, stage: stage() })); tryDo(() => panel.paintAll()); };
      panel = mount(panelHost, line, {
        job, face, scope: slot, faceSwitch: !card, onDraft: o.onDraft, onVisible: v => { slot.hidden = !v && !card; }, thumb: f => (card && card.thumb ? card.thumb(f) : null),
        onFace: f => {
          follow(f);   // (the card draws its thumbnails from the saved jobs: the staged words go back on them)
          // the card in front is for the face being edited: another face that has its own placement is shown by the tab itself (its card is drawn again; the staged edit stays)
          const E = Eng(), other = E && E.jobsOf ? E.jobsOf(line).find(j => (slotOfJob(j) || null) === (slotOfJob(job) || null) && F && F.faceOf(j) === f) : null;
          // (ENGFRONT: a face that is already decided lives in the Decided list, where the app opens it (EngraveLink): the Placements card cannot show it, so the press used to snap back to the other face)
          if (other && other !== job && ['approved', 'written', 'skipped'].includes(other.state) && root.EngraveLink && root.EngraveLink.open && typeof o.refocus !== 'function') { tryDo(() => { Promise.resolve(root.EngraveLink.open({ piece: other.key, face: f })).catch(() => {}); }); }
          else if (other && other !== job && E.restoreView && E.render && typeof o.refocus !== 'function') { tryDo(() => { E.restoreView({ focus: other.key }); E.render(); }); }
          else if (other && other !== job && typeof o.refocus === 'function') tryDo(() => o.refocus(other));
          if (typeof o.onFace === 'function') tryDo(() => o.onFace(f));
        },
        onChange: ev => {
          if (card && ev) {
            if (ev.kind === 'mode' || ev.kind === 'saved' || ev.kind === 'cancel' || ev.kind === 'turn') follow(ev.face);   // (a turn of the whole piece: the thumbnails draw the charm turned, so they are drawn again)
            else if (ev.kind === 'words' && card.label) { const d = DRAFTS.get(ev.key), x = d && d.faces[ev.face]; tryDo(() => card.label(ev.face, x ? x.text : '')); }   // (the words under the picture are the words being typed)
          }
          if (typeof o.onChange === 'function') tryDo(() => o.onChange(ev));
        }
      });
      return { destroy() { tryDo(() => panel.destroy()); if (card && card.destroy) tryDo(() => card.destroy()); }, refresh() { tryDo(() => panel.refresh()); }, setFace: f => panel.setFace(f), panel, card };
    } catch (e) { if (typeof console !== 'undefined') console.warn('engraving panel slot', e); return none; }
  }

  return { mount, mountPiece, html, bind, modesFor, seedText, stageOf, sweep, isLive, DRAFTS, MODES, hilite: { model: hiModel, decorate: hiDecorate, systemChoice, reasonLine, HI_TITLE } };
});
