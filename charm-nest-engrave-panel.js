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

  function draftOf(vm, key) {
    let d = DRAFTS.get(key);
    if (d && d.sig !== vm.sig && !d.dirty && !d.posTouched && !d.busy) d = null;   // what the piece says changed under an untouched draft: start again from it
    if (!d) {
      d = { key, sig: vm.sig, mode: vm.mode, face: null, shown: '', faces: {}, dirty: false, posTouched: false, snaps: {}, moved: {}, noRoom: {}, busy: '', error: '', confirm: '', confirmLines: null, focus: null };
      for (const f of ['front', 'back']) { const r = vm.rows.find(x => x.face === f); d.faces[f] = { text: r ? r.text : '', font: r ? r.fontWord : '' }; }
      DRAFTS.set(key, d);
    }
    return d;
  }
  /** The words a face starts with when its mode is switched on and nothing was typed: the line's own, else the buyer's labelled words, else (one face and no labels) the other face's. Never a guess between two faces. */
  function seedText(vm, face, mode) {
    const S = Setup(), here = vm.rows.find(r => r.face === face); if (here && here.text) return here.text;
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
  const positionOf = job => { const P = Pos(); return job && P && P.of ? tryDo(() => P.of(job), null) : null; };

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
    const off = locked || (job && !can && job.state && job.state !== 'review') ? 'Approved: nothing moves' : !can ? NOFIT : '';
    const stuck = (job && d.noRoom && d.noRoom[job.key]) || {};
    const pos = P ? `<div class="engpPos"><span class="engpLbl">Position</span><span class="engpRow" role="group" aria-label="${cap1(face)} position">${POS_BUTTONS.map(([k, t]) => posButton(k, t, d.busy ? NOFIT : off || (stuck[k] ? NOROOM : ''))).join('')}</span></div>` : '';
    const lock = locked ? `<div class="engpLock" role="status">${cur.lockState === 'skipped' ? 'Cut plain' : 'Approved'}${cur.lockBy ? ' by ' + esc(cur.lockBy) : ''}</div>` : per ? `<div class="engpLock" role="status">Differs per piece</div>` : '';
    return `<div class="engpFace" data-face="${face}">${words}${font}${pos}${lock}</div>`;
  }
  /** The panel for a view model (`vm` = CharmNestEngraveSetup.read) and its staged edit (`d`). ctx: { slot } */
  function html(vm, d, ctx) {
    ctx = ctx || {};
    const S = Setup(), id = 'engp' + (++uid), slot = ctx.slot === undefined ? vm.slot || null : ctx.slot, on = facesOf(d.mode), face = on.includes(d.face) ? d.face : on[0] || '';
    const modes = modesFor(vm.line), dis = d.busy ? ' disabled' : '';
    const seg = `<div class="engpMode" role="radiogroup" aria-label="Engraving">${modes.map(([m, t]) => `<button type="button" role="radio" class="engpSeg" data-engp="mode" data-mode="${m}" aria-checked="${d.mode === m}"${dis}>${t}</button>`).join('')}</div>`;
    const sw = ctx.faceSwitch && on.length > 1 ? `<div class="engpMode engpFaceSw" role="radiogroup" aria-label="Face">${on.map(f => `<button type="button" role="radio" class="engpSeg" data-engp="face" data-face="${f}" aria-checked="${f === face}"${dis}>${cap1(f)}</button>`).join('')}</div>` : '';
    const ask = vm.needsPerson && d.mode === vm.mode ? `<div class="engpAsk" role="status">${esc(vm.prompt)}</div>${vm.raw ? `<div class="engpRaw" aria-label="The buyer's own words">${esc(vm.raw)}</div>` : ''}` : '';
    const patch = S.patchOf(vm, d), dirty = hasPatch(patch), staged = dirty || d.posTouched;
    const cons = dirty ? S.consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup) : null;
    const warn = cons && cons.blocked ? `<div class="engpErr" role="alert">${esc(cons.blocked)}</div>` : '';
    const err = d.error ? `<div class="engpErr" role="alert">${esc(d.error)}</div>` : '';
    const confirm = d.confirm && d.confirmLines && d.confirmLines.length ? `<div class="engpConfirm" role="status">${d.confirmLines.map(t => `<span>${esc(t)}</span>`).join('')}<span class="engpActs"><button type="button" class="btn gold xs" data-engp="saveYes"${dis}>Cut it plain and save</button><button type="button" class="btn ghost xs" data-engp="keep">Keep it as it is</button></span></div>` : '';
    const spin = d.busy ? `<span class="engpStat" role="status"><span class="owSpin" aria-hidden="true"></span>${esc(d.busy)}</span>` : '';
    const canSave = staged && !d.busy && !(cons && cons.blocked) && !d.confirm;
    const acts = `<div class="engpActs"><button type="button" class="btn gold xs" data-engp="save"${canSave ? '' : ' disabled'}>Save</button><button type="button" class="btn ghost xs" data-engp="cancel"${(staged || d.dirty) && !d.busy ? '' : ' disabled'}>Cancel</button>${spin}</div>`;
    return `<div class="engp" data-key="${esc(d.key)}" data-mode="${esc(d.mode)}" data-face="${esc(face)}" role="group" aria-label="Engraving">${seg}${sw}${face ? faceHtml(vm, d, face, id, slot) : ''}${ask}${warn}${err}${confirm}${acts}</div>`;
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
.engpRaw{padding:5px 8px;border-radius:8px;background:var(--card2,#faf7f1);border:1px solid var(--line,#e4ddd0);font:600 12.5px var(--serif,Georgia,serif);white-space:pre-line;overflow-wrap:anywhere}
.engpErr{font-size:11.5px;color:var(--clay,#b0563f)}
.engpConfirm{display:grid;gap:4px;font-size:11.5px;color:var(--clay,#b0563f)}
.engpActs{display:flex;align-items:center;flex-wrap:wrap;gap:4px 6px}
.engpStat{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;color:var(--ink70,#5b554c)}`;
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
    const blockClick = e => { if (gateOn && e.target && e.target.closest && e.target.closest(GATE)) { e.preventDefault(); e.stopImmediatePropagation(); } };
    const blockKey = e => { if (gateOn && e.key && e.key.toLowerCase() === 'a' && !e.metaKey && !e.ctrlKey && !e.altKey && e.target && e.target.closest && !e.target.closest('input,textarea,select,[contenteditable]')) { e.preventDefault(); e.stopImmediatePropagation(); } };
    const hookCard = () => {
      const card = el.closest ? el.closest('.rvItem') : null; if (!card || card === gateCard) return;
      if (gateCard) { gateCard.removeEventListener('click', blockClick, true); gateCard.removeEventListener('keydown', blockKey, true); }
      card.addEventListener('click', blockClick, true); card.addEventListener('keydown', blockKey, true); gateCard = card;
    };
    const applyGate = on => {
      gateOn = !!on; hookCard(); if (!gateCard) return;
      for (const b of gateCard.querySelectorAll(GATE)) {
        if (on) { if (!b.dataset.engpWas) b.dataset.engpWas = JSON.stringify([b.disabled, b.title]); b.disabled = true; b.title = GATE_WHY; }
        else if (b.dataset.engpWas) { const [dis, ttl] = tryDo(() => JSON.parse(b.dataset.engpWas), [b.disabled, b.title]); delete b.dataset.engpWas; b.disabled = !!dis; b.title = ttl; }
      }
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
      if (!facesOf(d.mode).includes(face)) setMode(vm, d, d.mode === 'none' ? face : 'both');   // (an empty slot was picked: the panel turns that face on)
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
    const click = e => {
      const b = e.target && e.target.closest && e.target.closest('[data-engp]'); if (!b || !el.contains(b) || b.disabled || dead) return;
      const m = model(); if (!m) return; const { vm, d } = m, a = b.dataset.engp;
      if (d.busy) return;
      if (a === 'face') setFace(b.dataset.face);
      else if (a === 'mode') { if (b.dataset.mode === d.mode) return; setMode(vm, d, b.dataset.mode); draw(); paintAll(vm, d); changed('mode', d.face); }
      else if (a === 'cancel') cancel(m);
      else if (a === 'keep') { d.confirm = ''; d.confirmLines = null; draw(); }
      else if (a === 'save' || a === 'saveYes') {
        const S = Setup(), patch = S.patchOf(vm, d);
        if (!hasPatch(patch)) { DRAFTS.delete(key); draw(); changed('saved', d.face); return; }   // (only the position was moved: it is kept as it is)
        const cons = S.consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup);
        if (cons.blocked) { d.error = cons.blocked; draw(); return; }
        if (a === 'save' && cons.plain.length) { d.confirm = 'save'; d.confirmLines = cons.lines.filter(l => /already/.test(l)); draw(); return; }
        run(m, patch);
      } else if (a === 'pos') {
        const P = Pos(), job = faceJobOf(vm, d.face, slot); if (!P || !job || !job.fit) return;
        const step = e.shiftKey ? 2 : 0.5, act = b.dataset.act; snapBeforeMove(d, job);
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
      const m = model(); if (!m) return; const { vm, d } = m, f = t.dataset.face; if (!f) return;
      const x = d.faces[f] || (d.faces[f] = { text: '', font: '' });
      if (t.dataset.engp === 'words') { x.text = t.value; growBox(t); } else if (t.dataset.engp === 'font') x.font = t.value; else return;
      d.dirty = true; d.confirm = ''; d.error = ''; d.noRoom = {};
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
      if (t.tagName === 'TEXTAREA' || t.tagName === 'SELECT') e.stopPropagation();   // (the card's own keys, S for "No engraving", are for the card, not for words)
      if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && t.tagName === 'TEXTAREA') { e.preventDefault(); const s = el.querySelector('[data-engp="save"]'); if (s && !s.disabled) s.click(); }
      else if (e.key === 'Escape' && t.tagName === 'TEXTAREA') { e.preventDefault(); const c = el.querySelector('[data-engp="cancel"]'); if (c && !c.disabled) c.click(); }
    };
    const focusin = e => { const t = e.target; if (!t || !t.dataset || !t.dataset.engp || dead) return; const m = model(); if (m) m.d.focus = { engp: t.dataset.engp, face: t.dataset.face || '', mode: t.dataset.mode || '', act: t.dataset.act || '', start: t.selectionStart, end: t.selectionEnd }; };
    const focusout = e => { if (e.relatedTarget) { const m = model(); if (m) m.d.focus = null; } };
    const ev = [['click', click], ['input', input], ['change', input], ['keydown', key_], ['focusin', focusin], ['focusout', focusout]];
    for (const [t, f] of ev) el.addEventListener(t, f);
    // the first paint of the thumbnails: the face's own words and position (the card drew them from the job; this makes them the staged ones)
    return { draw, refresh: draw, setFace, destroy() { dead = true; untrack(); for (const [t, f] of ev) el.removeEventListener(t, f); gateOn = false; if (gateCard) { gateCard.removeEventListener('click', blockClick, true); gateCard.removeEventListener('keydown', blockKey, true); } }, paintAll: () => { const m = model(); if (m) paintAll(m.vm, m.d); } };
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
      const pick = f => { if (panel) panel.setFace(f); };
      if (C && C.mountPiece) card = tryDo(() => C.mountPiece(cardHost, { row: line, key: job.key, face, onPick: pick, approve: false, nameOf: o.nameOf || lineName, isWorking: Eng() && Eng().isWorking ? Eng().isWorking : undefined }), null);
      if (!card) cardHost.remove();
      panel = mount(panelHost, line, {
        job, face, scope: slot, faceSwitch: !card, onDraft: o.onDraft, onVisible: v => { slot.hidden = !v && !card; }, thumb: f => (card && card.thumb ? card.thumb(f) : null),
        onFace: f => {
          if (card && card.update) { tryDo(() => card.update({ face: f })); tryDo(() => panel.paintAll()); }   // (the card draws its thumbnails from the saved jobs: the staged words go back on them)
          // the card in front is for the face being edited: another face that has its own placement is shown by the tab itself (its card is drawn again; the staged edit stays)
          const E = Eng(), other = E && E.jobsOf ? E.jobsOf(line).find(j => (slotOfJob(j) || null) === (slotOfJob(job) || null) && F && F.faceOf(j) === f) : null;
          if (other && other !== job && E.restoreView && E.render && typeof o.refocus !== 'function') { tryDo(() => { E.restoreView({ focus: other.key }); E.render(); }); }
          else if (other && other !== job && typeof o.refocus === 'function') tryDo(() => o.refocus(other));
          if (typeof o.onFace === 'function') tryDo(() => o.onFace(f));
        },
        onChange: ev => { if (card && card.update && ev && (ev.kind === 'saved' || ev.kind === 'cancel')) { tryDo(() => card.update({ face: ev.face })); tryDo(() => panel.paintAll()); } if (typeof o.onChange === 'function') tryDo(() => o.onChange(ev)); }
      });
      return { destroy() { tryDo(() => panel.destroy()); if (card && card.destroy) tryDo(() => card.destroy()); }, refresh() { tryDo(() => panel.refresh()); }, setFace: f => panel.setFace(f), panel, card };
    } catch (e) { if (typeof console !== 'undefined') console.warn('engraving panel slot', e); return none; }
  }

  return { mount, mountPiece, html, bind, modesFor, seedText, DRAFTS, MODES, hilite: { model: hiModel, decorate: hiDecorate, systemChoice, reasonLine, HI_TITLE } };
});
