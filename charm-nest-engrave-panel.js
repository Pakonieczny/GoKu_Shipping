/* The ENGRAVING PANEL of the Engrave tab's detail card (Paul, 12 Oct 2026: "This UI is totally broken everywhere ... it should only be part of the detailed Engraving modal popup within the
 * Engraving tab ... do NOT crowd the menu" and "the user should be able just to click on either one and then update it with the text font options and positional update of the text on the
 * charm, and the actual text itself. Everything has to be linked to its engraving, whether it be front or back on the same charm or multiple charms in the same order").
 * Plan: plans/engrave-ui-1012 (CONTRACT.md section D). It replaces the UI half of charm-nest-engrave-setup.js for the Engrave tab; the model (read, patchOf, consequences, buildOverride,
 * fontChoices) and the save path (Engrave.saveSetup -> CharmNestEngraveSetup.apply -> seedWords) are the ones that file already has.
 *
 * LIVEWORDS (Paul, 29 Oct 2026: "1. When I click the Back/Front buttons the system does not display the Back preview. 2. ... when I click on the Back/Front vector preview thumbnails the main engraving preview
 * area does not get toggled ... 3. Remove the Back+Front option ... 4. Remove the process that ask the user why the made text engraving changes including asking the user to click save ... As soon as the user
 * types a letter into the text box, that letter must immediately appear in all charm vector preview images and main Engraving preview in real-time. The user just needs to press the green approve"):
 *
 * ONE column, 232px minimum, always open, nothing explained in words:
 *   None | Front | Back                   one row of toggles: which faces are ENGRAVED (an earring / never line draws nothing at all: CharmNestEngraveFaces.neverOf; a front-only line offers None | Front).
 *                                         A press on a face that is not engraved engraves it too (Front while the Back is engraved = both faces, the old "Front + back") and SHOWS it; a press on a face that is
 *                                         engraved shows it (never takes it away: None takes every face away). Each press is STAGED (STAGEALL, 29 Oct 2026): Approve keeps it, leaving the card discards it.
 *   [words]                               a full-width text area (2 rows, grows to 4, then scrolls) for the face shown
 *   [font]                                a full-width drop-down (never beside the words, so it cannot overlap them); each option in its own font once the preview loaded it
 *   (no Position row: BIGPANEL, Paul 29 Oct 2026, "remove the complete "Position" title and all of the below buttons completely": the words are moved in the work area (drag, the arrow keys, Centre))
 * There is no Save, no Cancel and no "Why change it?": a typed letter is drawn at once on that face's thumbnail (CharmNestEngravePreview.update) and, a frame later, in the work area (Engrave.draftFit); the green
 * Approve of the card writes the words (the panel catches the press, saves the staged words through Engrave.saveSetup, waits for them to be fitted, then presses Approve again). Anything staged and never approved is
 * DISCARDED when the piece's card is left (STAGEALL); on an approved piece the green Approve lights up while anything is staged. Who changed the words is still recorded (seedWords: the job's decision and the order timeline's "Engraving edited by ..." line), nothing is asked.
 * The face shown is chosen by the Front / Back thumbnails above the panel (CharmNestEngraveCard.mountPiece, `onPick`), by the face buttons, or `setFace(face)`; the work area follows through
 * Engrave.showFace(job, face, { animate: true }) when the page has it. Every draft belongs to ONE piece: its key is the piece key `<lineKey>[#slot]`, and inside it each face has its own words and font, so the
 * Front never changes the Back, disc 1 never shares a draft with disc 2.
 *
 *   mount(slot, row, opts?)     draws the panel into `slot` for the piece of `row` and wires it -> { destroy(), refresh(), setFace(face), draw(), commit() }. Safe to call again after every redraw of the host
 *                               (the staged edit, the face and the caret come back). Draws nothing when the line has nothing to edit.
 *                               opts: { job (the job the card shows: gives the piece and the face), face, slot, jobs(), onFace(face), onChange({ face, kind }), thumb(face) -> element of that face's preview,
 *                                       save(patch) -> Promise, show(face) (the work area shows that face), scope }
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
  const MODES = [['none', 'None'], ['front', 'Front'], ['back', 'Back']];   // (LIVEWORDS: no "Front + back" choice: Front and Back are toggles, both pressed = both faces engraved, mode 'both')
  const facesOf = mode => (mode === 'both' ? ['front', 'back'] : mode === 'front' ? ['front'] : mode === 'back' ? ['back'] : []);
  const faceOf = j => { const F = Faces(); return F && F.faceOf ? F.faceOf(j) : (j && j.face === 'front' ? 'front' : 'back'); };
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
  const DRAFTS = new Map();    // piece key -> { mode, face, faces: { front: { text, font }, back: ... }, dirty, moved, busy, error, confirm, flush(), ... }
  const LIVE = new Map();      // piece key -> the bindings on screen (a save that ends tells every one of them)
  const APPROVING = new Set(); // piece keys whose staged words are being kept before the Approve that was pressed (LIVEWORDS: a second press meanwhile is ignored)
  let uid = 0;
  /* ── the life of a staged edit (ENGAUTO, round 2: "a staged draft dies with its panel session") ─────────────────────────────────────────────────────────────────
   * An edit that is staged and not kept yet belongs to the card it was made in. It lives through every redraw of that card (the Engrave tab draws the detail again on each change, even for the other
   * face of the same piece), and ends with Approve (which keeps it first), when another order or piece opens, and when the detail is closed (the tab draws a list, another tab, nothing). LIVEWORDS (there is
   * no Save any more): words or a font typed and not approved are KEPT quietly when the draft ends that way (d.flush: the same save, never a face cut plain unasked), so nothing typed is lost; the next time
   * the card opens it shows what was kept, never an old draft. "Live" = a panel of that piece is on the page. */
  const GRACE = 250;       // ms of quiet after the page has drawn again before a piece with no panel on the page loses its draft (a redraw swaps the card in the same task)
  let sweepT = 0, watching = null;
  const isLive = key => { const set = LIVE.get(key); if (!set) return false; for (const b of set) if (b.el && b.el.isConnected) return true; return false; };
  /** Forget the staged edit of every piece that has no panel on the page; `except` is the piece being opened. */
  // STAGEALL (Paul, 29 Oct 2026: "it's not approved until the Approve button is pressed again by the user after any updates, otherwise those updates to the engraving are discarded"): a piece left with no Approve
  // loses what was staged on it (words, font, None / Front / Back): nothing is kept quietly any more, and the work area's draft of each of its faces goes with it.
  function sweep(except) { for (const k of [...DRAFTS.keys()]) if (k !== except && !isLive(k)) { const d = DRAFTS.get(k); DRAFTS.delete(k); if (d && typeof d.flush === 'function' && !d.busy) tryDo(() => d.flush()); } }
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
    return { mode: d.mode, faces: { front: x('front'), back: x('back') }, ...(d.mode === 'none' && d.wasMode ? { plain: facesOf(d.wasMode) } : {}) };   // (NONEPLAIN 2: None staged: the faces it takes away are still drawn, as the charm alone)
  }

  function draftOf(vm, key) {
    let d = DRAFTS.get(key);
    if (d && d.sig !== vm.sig && !d.dirty && !d.posTouched && !d.busy) d = null;   // what the piece says changed under an untouched draft: start again from it
    if (!d) {
      d = { key, sig: vm.sig, mode: vm.mode, face: null, shown: '', faces: {}, dirty: false, posTouched: false, moved: {}, noRoom: {}, busy: '', error: '', confirm: '', confirmLines: null, focus: null };
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
    const m = { sys, mode: { sys: sys.mode, same: d.mode === sys.mode }, words: null, font: null, reason: '' };   // (LIVEWORDS: no reason is asked or shown any more; reasonLine stays for an old record's reader)
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
.engpSeg[data-sys][aria-checked=true],.engpSeg[data-sys][aria-pressed=true]{background:var(--goldSoft,#f0e6cd);box-shadow:0 0 0 1px rgba(28,26,23,.42)}
.engpSeg>.engpSys{position:absolute;top:-4px;right:-3px;width:12px;height:12px;pointer-events:none}
.engpSeg>.engpSys svg{width:9px;height:9px}
.engpBox>.engpSys{position:absolute;top:8px;right:66px}
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
      // the mode: the sparkle and the tint stay on the system's segment(s) whatever is selected (the ring is the selection; a system "both" marks Front and Back)
      const sysSegs = m.mode.sys === 'none' ? ['none'] : facesOf(m.mode.sys);
      for (const seg of p.querySelectorAll('.engpMode:not(.engpFaceSw) .engpSeg[data-engp="mode"]')) {
        if (sysSegs.includes(seg.dataset.mode)) { const t = HI_TITLE + (m.mode.same ? '' : why); seg.dataset.sys = m.mode.same ? 'same' : 'changed'; seg.title = t; put(seg, 'same', t); }
        else { delete seg.dataset.sys; drop(seg); }   // (its own title stays: what a press on it does)
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

  function faceHtml(vm, d, face, id, slot) {
    const cur = vm.rows.find(r => r.face === face), x = d.faces[face] || { text: '', font: '' }, locked = !!(cur && cur.locked), per = !!(cur && cur.perPiece), dis = d.busy ? ' disabled' : '';
    const words = `<div class="engpBox"><textarea class="engpWords" id="${id}" data-engp="words" data-face="${face}" rows="3" placeholder="Words for the ${face}" aria-label="${cap1(face)} words"${locked || per || d.busy ? ' disabled' : ''}>${esc(per ? '' : x.text)}</textarea>${locked || per ? '' : `<span class="engpTools"><button type="button" class="btn ghost xs engpAlpha emBtn" data-alpha${d.busy ? ' disabled' : ''} title="Other alphabets: letters, words and phrases in Greek, Hebrew, Arabic, Chinese, Japanese, Korean, Latin and more, found by their English" aria-label="Other alphabets">Aα</button><button type="button" class="btn ghost xs engpEmoji emBtn" data-emoji${d.busy ? ' disabled' : ''} title="Choose an emoji the laser can engrave and put it in the words">Emoji</button></span>`}</div>`;
    const font = `<select class="engpFont" data-engp="font" data-face="${face}" aria-label="${cap1(face)} font"${locked || d.busy ? ' disabled' : ''}>${fontOptions(vm, x.font)}</select>`;
    const lock = cur && cur.decided ? `<div class="engpLock" role="status">${cur.lockState === 'skipped' ? 'Cut plain' : cur.lockState === 'written' ? 'Saved to sheet' : 'Approved'}${cur.lockBy ? ' by ' + esc(cur.lockBy) : ''}</div>` : per ? `<div class="engpLock" role="status">Differs per piece</div>` : '';
    return `<div class="engpFace" data-face="${face}">${words}${font}${lock}</div>`;
  }
  /** The panel for a view model (`vm` = CharmNestEngraveSetup.read) and its staged edit (`d`). ctx: { slot } */
  function html(vm, d, ctx) {
    ctx = ctx || {};
    const S = Setup(), id = 'engp' + (++uid), slot = ctx.slot === undefined ? vm.slot || null : ctx.slot, on = facesOf(d.mode), face = on.includes(d.face) ? d.face : on[0] || '';
    const modes = modesFor(vm.line), dis = d.busy ? ' disabled' : '';
    // (LIVEWORDS: toggles, not a radio: Front and Back are each pressed while that face is engraved, None while nothing is; the face shown carries data-shown)
    const segTitle = m => m === 'none' ? 'No engraving on this piece' : on.includes(m) ? (m === face ? `The ${m} is engraved and shown` : `The ${m} is engraved: press to show it`) : on.length ? `Engrave the ${m} too, and show it` : `Engrave the ${m}, and show it`;
    const seg = `<div class="engpMode" role="group" aria-label="Engraved faces">${modes.map(([m, t]) => `<button type="button" class="engpSeg" data-engp="mode" data-mode="${m}" aria-pressed="${m === 'none' ? d.mode === 'none' : on.includes(m)}"${m !== 'none' && m === face ? ' data-shown="1"' : ''} title="${esc(segTitle(m))}"${dis}>${t}</button>`).join('')}</div>`;
    const sw = ctx.faceSwitch && on.length > 1 ? `<div class="engpMode engpFaceSw" role="radiogroup" aria-label="Face">${on.map(f => `<button type="button" role="radio" class="engpSeg" data-engp="face" data-face="${f}" aria-checked="${f === face}"${dis}>${cap1(f)}</button>`).join('')}</div>` : '';
    // (EDITOR WORDS: a face the charm editor drew is never asked for words: one calm line says they are optional while its box is empty)
    const art = artOn(vm, face), typed = !!(face && d.faces[face] && linesOf(d.faces[face].text).length);
    const ask = art ? (typed ? '' : `<div class="engpOffer" role="status">No words needed: the charm editor's parts are the ${esc(face)}'s engraving. Type words only to add them.</div>`)
      : vm.needsPerson && d.mode === vm.mode ? `<div class="engpAsk" role="status">${esc(vm.prompt)}</div>${vm.raw ? `<div class="engpRaw" aria-label="The buyer's own words">${esc(vm.raw)}</div>` : ''}` : '';
    const patch = S.patchOf(vm, d), dirty = hasPatch(patch), staged = dirty || d.posTouched;
    const cons = dirty ? S.consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup) : null;
    const warn = cons && cons.blocked ? `<div class="engpErr" role="alert">${esc(cons.blocked)}</div>` : '';
    const err = d.error ? `<div class="engpErr" role="alert">${esc(d.error)}</div>` : '';
    const spin = d.busy ? `<span class="engpStat" role="status"><span class="owSpin" aria-hidden="true"></span>${esc(d.busy)}</span>` : '';
    const acts = spin ? `<div class="engpActs">${spin}</div>` : '';   // (LIVEWORDS: no Save, no Cancel, no reason: only the small spinner while a change is kept)
    return `<div class="engp" data-key="${esc(d.key)}" data-mode="${esc(d.mode)}" data-face="${esc(face)}"${staged ? ' data-staged="1"' : ''} role="group" aria-label="Engraving">${seg}${sw}${face ? faceHtml(vm, d, face, id, slot) : ''}${ask}${warn}${err}${acts}</div>`;
  }

  /* ── the look ──────────────────────────────────────────────────────────────── */
  const CSS = `.engp{display:grid;gap:10px;min-width:0;container-type:inline-size;font:12px var(--sans,system-ui);color:var(--ink,#1c1a17)}
.engp[hidden]{display:none!important}
.engpCard{margin-bottom:10px}
.engpCard:empty{display:none}
.engpCard .engFaces[data-n="1"]{grid-template-columns:repeat(2,minmax(0,1fr))}
.engpCard .engFaces[data-n="1"] .engFace{display:flex}
.engpCard .engFaces[data-n="1"] .engLbl{order:-1}
.engpCard .engFaces[data-n="1"] .engTxt{display:block;white-space:nowrap;font-size:12px;line-height:14px}
.engpMode{display:flex;flex-wrap:nowrap;gap:3px;padding:3px;min-width:0;border:1px solid var(--line,#e4ddd0);border-radius:10px;background:var(--paper2,#ebe5d9)}
.engpSeg{flex:1 1 0;min-width:0;min-height:40px;white-space:nowrap;padding:6px 10px;border:0;border-radius:8px;background:transparent;font:600 13.5px var(--sans,system-ui);color:inherit;cursor:pointer}
.engpSeg[aria-checked=true],.engpSeg[aria-pressed=true]{background:var(--card,#fffefb);box-shadow:0 0 0 1px rgba(28,26,23,.42)}
.engpSeg[data-shown]{text-decoration:underline;text-decoration-thickness:2px;text-underline-offset:3px}
.engpSeg:disabled{opacity:.55;cursor:default}
@container (max-width:239px){.engpSeg{padding:6px 4px;font-size:13px}}
.engpFace{display:grid;gap:10px;min-width:0}
.engpWords,.engpFont{display:block;width:100%;min-width:0;box-sizing:border-box;border:1px solid var(--line,#e4ddd0);border-radius:8px;background:var(--card,#fffefb);color:inherit}
.engpBox{position:relative;min-width:0}
/* BIGPANEL (Paul, 29 Oct 2026: "Enlarge this yellow shaded areas including the text box and buttons and remove the complete "Position" title and all of the below buttons ... does not overpower the other
   surrounding UI"): the toggles 40 px, the words 17 px with three lines in view, Aα and Emoji 34 px stacked in the box's right edge (beside the words, never over them; the ✦ of ENGHILITE between them and the words), the font picker 44 px; the Position
   row is gone, and its room pays for the rest: the column is no taller than it was. */
.engpWords{min-height:86px;resize:none;padding:8px 80px 8px 11px;font:17px/1.3 var(--sans,system-ui);overflow-y:auto}
.engpEmoji{position:absolute;right:6px;top:6px;padding:2px 8px}
.engpTools{position:absolute;right:6px;top:6px;display:flex;flex-direction:column;align-items:stretch;gap:5px;width:58px}.engpTools>.engpEmoji,.engpTools>.engpAlpha{position:static;min-height:34px;margin:0;padding:0 8px;font-size:13px;border-radius:8px}.engpAlpha{font-weight:600}
.engpFont{min-height:44px;padding:8px 11px;font:15px var(--sans,system-ui);border-radius:9px}
.engpWords:disabled,.engpFont:disabled{background:var(--paper2,#ebe5d9);color:var(--ink70,#5b554c)}
.engpLock{font-size:11.5px;color:var(--ink45,#938c80)}
.engpAsk{font-size:12px;font-weight:600;color:var(--clay,#b0563f)}
.engpOffer{font-size:12px;line-height:1.4;color:var(--ink70,#5b554c)}
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
    sweep(key); watch(el.ownerDocument);   // (another piece was open before: its staged edit ends here)
    const track = () => { let set = LIVE.get(key); if (!set) LIVE.set(key, set = new Set()); for (const b of [...set]) if (b !== me && b.el && !b.el.isConnected) set.delete(b); set.add(me); };   // (a card drawn again leaves its old panel behind: forgotten here)
    const untrack = () => { const set = LIVE.get(key); if (set) { set.delete(me); if (!set.size) LIVE.delete(key); } };
    const model = () => {
      const S = Setup(), vm = S && S.read ? S.read(line, { slot: slot || undefined, jobs: jobs() }) : null; if (!vm) return null;
      const d = draftOf(vm, key), shownKey = opts.job ? str(opts.job.key) : '';
      if (shownKey && d.shown !== shownKey) { d.shown = shownKey; if (opts.face) d.face = opts.face; }   // (the card was drawn for another job: its face is the one to edit)
      const on = facesOf(d.mode); if (!on.includes(d.face)) d.face = on[0] || d.face;
      if (!d.flush) d.flush = () => keepQuietly(d);
      return { vm, d };
    };
    /** STAGEALL: the draft ended with no Approve (the card was left): it is DISCARDED (Paul: "otherwise those updates to the engraving are discarded"); the work area stops drawing it. Nothing is saved. */
    const keepQuietly = d => {
      const E = Eng(); for (const k of d.drafted || []) tryDo(() => { if (typeof opts.onDraft === 'function') opts.onDraft(k, null); else if (E && E.draftFit) E.draftFit(k, null); });
      if (E && E.stageFace && opts.job && opts.job.key) tryDo(() => E.stageFace(opts.job.key, null, null));   // (FRONTSTAGE: a staged face shown in the work area goes with the discarded draft)
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
        if (staged) (d.drafted || (d.drafted = new Set())).add(job.key);
        if (typeof opts.onDraft === 'function') opts.onDraft(job.key, staged); else { const E = Eng(); if (E && E.draftFit) E.draftFit(job.key, staged); }
      } catch (_) { /* the work area keeps what it drew */ }
    };
    /** FRONTSTAGE: the face being edited is staged on and has no job yet: the work area shows that face with its staged words (Engrave.stageFace); anything else takes such a showing away. -> shown */
    const stagedOut = (vm, d) => {
      const E = Eng(), cj = opts.job; if (!E || typeof E.stageFace !== 'function' || !cj || !cj.key) return false;
      const f = d.face, on = !dead && DRAFTS.get(key) === d && facesOf(d.mode).includes(f) && faceJobOf(vm, f, slot) === null && !!vm.rows.length;
      if (!on) { tryDo(() => E.stageFace(cj.key, null, null)); return false; }
      const x = d.faces[f] || { text: '', font: '' };
      const r = tryDo(() => E.stageFace(cj.key, f, { text: x.text, fontWord: x.font || '' }), null);
      return !!(r && r.ok);
    };
    /** What a face's thumbnail shows right now: the staged words and font, the face's own position. */
    const paint = (vm, d, face) => {
      draftOut(vm, d, face);
      if (face === d.face) stagedOut(vm, d);
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
    /* LIVEWORDS: the card's green Approve is the ONE commit of typed words. A press on it (or the A key) while words or a font are staged is caught before the card sees it: the staged edit is kept
     * (Engrave.saveSetup: no question, no Save), the face is fitted with the new words, and then the same button of the card as it is drawn by then is pressed again, so the approval approves the NEW words.
     * APPROVEFIX (Paul, 28 Oct 2026: never a press that does nothing without a word): the press says what it is waiting for under the button (.egApproveWhy), and a second press meanwhile is ignored. */
    const GATE = '[data-a="approve"],[data-a="approveBoth"],[data-a="approveFaces"]', WAIT_WHY = 'Placing the new words · the approval follows';
    let gateCard = null;
    const stagedNow = () => { const m = dead ? null : tryDo(() => model(), null); return !!m && !m.d.busy && hasPatch(Setup().patchOf(m.vm, m.d)); };
    const say = (card, text) => {
      const host = card && card.querySelector('.pvApproval'); if (!host) return;
      let n = host.querySelector('.egApproveWhy:not(.engpNoneHint)');
      if (!text) { if (n && n.dataset.engp) n.remove(); return; }
      if (!n) { n = card.ownerDocument.createElement('div'); n.className = 'egApproveWhy'; n.setAttribute('role', 'status'); host.appendChild(n); }
      n.dataset.engp = '1'; if (n.textContent !== text) n.textContent = text;
    };
    /** STAGEALL (Paul, 29 Oct 2026: "if they choose to change something than the approved button needs to light up again"): while anything is staged on an approved piece (None, Front, Back, words, a font), its calm
     *  "Approved" makes way for the green Approve (the press is caught here: approveAfterCommit keeps what is staged and approves it); the calm one is back the moment nothing is staged (None, then Back again).
     *  No line explains anything (Paul: "The user is allowed to choose none front or back without any explanations"). */
    const stageGate = (vm, d) => {
      const card = el.closest ? el.closest('.rvItem') : null, host = card && card.querySelector('.pvApproval'), doc = el.ownerDocument; if (!host) return;
      const want = !dead && hasPatch(tryDo(() => Setup().patchOf(vm, d), {}));
      let b = host.querySelector('[data-engp-none]');
      const calm = host.querySelector('[data-a="approved"]'), real = host.querySelector('[data-a="approve"]:not([data-engp-none])');
      const quiet = [calm && (calm.closest('.egApproveWrap') || calm), host.querySelector('.egWrittenNote')].filter(Boolean);   // (the calm "Approved" and its "Saved to sheet" note make way while something is staged)
      for (const n of host.querySelectorAll('.engpNoneHint')) n.remove();
      if (!want) { if (b) b.remove(); for (const q of quiet) if (q.dataset.engpHid) { q.style.display = ''; delete q.dataset.engpHid; } return; }
      if (calm && (!real || real.disabled)) {   // (only an approved piece: a piece whose own Approve is off for its own reason keeps it off)
        if (!b) { b = doc.createElement('button'); b.type = 'button'; b.className = 'btn sage sm egApproveButton'; b.dataset.a = 'approve'; b.dataset.engpNone = '1'; b.title = 'approve this change'; b.textContent = 'Approve'; host.insertBefore(b, host.firstChild); }
        for (const q of quiet) if (!q.dataset.engpHid) { q.style.display = 'none'; q.dataset.engpHid = '1'; }
      }
      b = b || real; if (b) b.disabled = !!d.busy;
    };
    const cardOf = jobKey => { if (gateCard && gateCard.isConnected) return gateCard; try { return el.ownerDocument.querySelector(`.rvItem[data-key="${(root.CSS && root.CSS.escape ? root.CSS.escape(jobKey) : jobKey)}"]`); } catch (_) { return null; } };
    const wait = ms => new Promise(r => setTimeout(r, ms));
    const approveAfterCommit = async act => {
      const m = model(); if (!m) return;
      const jobKey = str(opts.job && opts.job.key) || str((faceJobOf(m.vm, m.d.face, slot) || {}).key);
      APPROVING.add(key); let pressed = false;
      // (NONEPLAIN 2: Approve (the button or the A key) with None staged IS the commit: every face goes, with no seal (Engrave.saveSetup -> takeAway), the page says so and moves on as after any Approve; nothing is
      //  left to approve, so the press ends there and never approves the old job. With the "Cut it plain" row showing, Approve is the go-ahead too.)
      const noneNow = m.d.mode === 'none';
      const newFace = !noneNow && facesOf(m.d.mode).includes(m.d.face) && faceJobOf(m.vm, m.d.face, slot) === null ? m.d.face : null;   // (FRONTSTAGE: the face edited was staged on and has no job yet)
      const otherTouched = !!newFace && (() => { const p = tryDo(() => Setup().patchOf(m.vm, m.d), {}) || {}, o = newFace === 'front' ? 'back' : 'front'; return p[o] !== undefined; })();   // (ONEAPPROVE: the other face changed in this press too)
      try {
        say(cardOf(jobKey), noneNow ? '' : WAIT_WHY);
        const res = await commit(m);
        if (!res || !res.ok) { if (!(res && res.cancelled)) say(cardOf(jobKey), 'Not approved: ' + str((res && res.message) || 'the new words were not kept') .replace(/^Not saved:\s*/, '')); return; }
        if (noneNow) return;
        const E = Eng(), until = Date.now() + 60000 * Math.max(0.001, +(root && root.CN_SPIN_SCALE) || 1);
        // ONEAPPROVE (Paul, 29 Oct 2026: the green Approve is the commit: one press is enough): the staged face is made now, its words fitted and moved to where they were dragged (the job's _placeAt, by the
        // bridge), and approved in this same press; the other face keeps its state unless this press changed it too (then it is approved as well, as below)
        if (newFace) {
          const nj = () => tryDo(() => ((E && E.jobsOf ? E.jobsOf(line) : jobs()) || []).find(j => j && faceOf(j) === newFace && (slotOfJob(j) || null) === (slot || null)), null);
          const ready = x => !!x && x.state === 'review' && !!x.fit && !x._placeAt && !(E && E.isWorking && tryDo(() => E.isWorking(x), false));
          let j2 = nj(); while (Date.now() < until && !(ready(j2) || (j2 && ['words', 'blocked'].includes(j2.state)))) { await wait(60); j2 = nj(); }
          say(cardOf(jobKey), '');
          if (ready(j2) && E && E.approve) { pressed = true; APPROVING.delete(key); await Promise.resolve(tryDo(() => E.approve(j2), null)).catch(() => {}); }
          else if (j2 && E && E.restoreView && E.render) { tryDo(() => { E.restoreView({ tab: 'place', list: false, chosen: true, focus: j2.key }); E.render(); }); return; }   // (its words still need a person: its own card)
          if (!otherTouched) return;
        }
        const jobNow = () => tryDo(() => ((E && E.jobsOf ? E.jobsOf(line) : jobs()) || []).find(j => j && j.key === jobKey), null);
        let j = jobNow();
        const settled = x => (x.state === 'review' && x.fit && !(E && E.isWorking && tryDo(() => E.isWorking(x), false))) || ['words', 'blocked', 'approved', 'written', 'skipped', 'none'].includes(x.state)
          || (x.state === 'ready' && E && E.canFit && !tryDo(() => E.canFit(x), true));   // (a face that cannot be fitted here is not waited for)
        while (j && Date.now() < until && !settled(j)) { await wait(60); j = jobNow(); }
        await wait(0);   // (the card of the fitted words is drawn first)
        const card = cardOf(jobKey), b = card && (card.querySelector(`[data-a="${act}"]`) || card.querySelector('[data-a="approve"]'));
        if (b && !b.disabled) { APPROVING.delete(key); say(card, ''); pressed = true; b.click(); }
        else if (j && ['approved', 'written'].includes(j.state)) say(card, '');   // (STAGEALL: the face shown kept its approval (a face added has its own Approve on its own card): nothing to say)
        else say(card, j && j.state === 'review' ? 'Not approved: look at the new words, then press Approve.' : 'Not approved: the new words are still being placed. Press Approve once they are drawn.');
      } finally { if (!pressed) APPROVING.delete(key); }
    };
    const stop = (e, act) => {
      if (APPROVING.has(key)) { e.preventDefault(); e.stopImmediatePropagation(); return; }   // (the approval that follows the new words is on its way)
      // (NONEPLAIN: a change being kept right now (None, a face) is never overtaken by an Approve of the job as it was: the press waits for it and says so)
      { const m = dead ? null : tryDo(() => model(), null); if (m && m.d.busy) { e.preventDefault(); e.stopImmediatePropagation(); say(gateCard, m.d.mode === 'none' ? 'No engraving is being kept · nothing to approve' : 'Your change is being kept · press Approve once it is drawn'); return; } }
      if (!stagedNow()) return;   // (nothing staged: the card approves as it always did)
      e.preventDefault(); e.stopImmediatePropagation(); approveAfterCommit(act || 'approve');
    };
    const blockClick = e => { const b = e.target && e.target.closest ? e.target.closest(GATE) : null; if (b && !b.disabled) stop(e, b.dataset.a); };
    const blockKey = e => {
      const k = e.key ? e.key.toLowerCase() : '', plain = !e.metaKey && !e.ctrlKey && !e.altKey && e.target && e.target.closest && !e.target.closest('input,textarea,select,[contenteditable]');
      if (k === 'a' && plain) stop(e, 'approve');
      // NONEPRESS (Paul, 29 Oct 2026: "Remove this "No Engraving" button from the UI"): the S key is the None toggle: it STAGES None (nothing is kept until Approve), on any piece, approved or not
      else if (k === 's' && plain && !e.repeat) { e.preventDefault(); e.stopImmediatePropagation(); stageNone(); }
    };
    /** S, or a host: the None toggle pressed (staged only; Approve keeps it). false when this piece cannot be set to None here (busy, a line that never engraves). */
    const stageNone = () => { if (dead) return false; const m = tryDo(() => model(), null); if (!m || m.d.busy || !modesFor(line).some(([x]) => x === 'none')) return false; pressMode(m, 'none'); return true; };
    const hookCard = () => {
      const card = el.closest ? el.closest('.rvItem') : null; if (!card || card === gateCard) return;
      if (gateCard) { gateCard.removeEventListener('click', blockClick, true); gateCard.removeEventListener('keydown', blockKey, true); }
      card.addEventListener('click', blockClick, true); card.addEventListener('keydown', blockKey, true); gateCard = card;
    };
    /** ENGFRONT: the face being edited has NO job to draw (the front is asked for in the mode and not saved yet, or its job is "no engraving"): the work area would go on drawing the OTHER face's engraving
     *  while this face's thumbnail is marked. It says so in one plain line instead (the card's own empty-box markup; the drawing under it is hidden while the line is there), and the line goes with the next draw. */
    const noJob = (vm, d) => {
      const card = el.closest ? el.closest('.rvItem') : null, host = card ? card.querySelector('.backHost') : null; if (!card || !host) return;
      const job = facesOf(d.mode).includes(d.face) ? faceJobOf(vm, d.face, slot) : undefined, old = host.querySelector('.engpNoJob');
      // (FRONTSTAGE, Paul 29 Oct 2026, "without any explanations": a face staged on that has no job yet is SHOWN in the work area with its staged words (stagedOut), never "being prepared")
      const shown = job === null && !d.busy && stagedOut(vm, d);
      const want = job === null ? (shown ? '' : d.busy ? `Preparing the ${d.face} engraving…` : '') : job && job.state === 'none' ? `No ${d.face} engraving.` : '';
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
        if (eb && !(CE && tb && tryDo(() => CE.attach({ textarea: tb, button: eb, key: 'engp:' + key + ':' + d.face }), false))) eb.remove();
        // (ALPHAFIND, Paul 28 Oct 2026: the letters, words and phrases of other alphabets, found by their English, typed into the box beside Emoji: charm-nest-alphabet-picker.js)
        const ab = el.querySelector('[data-alpha]'), AP = el.ownerDocument.defaultView && el.ownerDocument.defaultView.CNAlphabetPicker;
        if (ab && !(AP && tb && tryDo(() => AP.attach({ textarea: tb, button: ab, key: 'engpa:' + key + ':' + d.face }), false))) ab.remove(); }
      hiDecorate(el, vm, d, slot);   // (ENGHILITE: the system's own choice stays marked)
      hookCard();
      noJob(vm, d);
      stageGate(vm, d);
      const P = Prev(); if (P && P.warm && !draw.warmed) { draw.warmed = true; tryDo(() => P.warm()); }
      const want = fs || (d.focus && el.ownerDocument.activeElement === el.ownerDocument.body ? d.focus : null);
      restoreFocus(want);
      if (want && !el.isConnected) Promise.resolve().then(() => restoreFocus(want));   // (a card is built before it is put in the page)
    };
    me.draw = draw;
    const redrawLive = () => { const set = LIVE.get(key); for (const b of [...(set || [])]) { if (b !== me && b.el && !b.el.isConnected) { set.delete(b); continue; } tryDo(() => b.draw()); } };
    const changed = (kind, face) => { try { if (typeof opts.onChange === 'function') opts.onChange({ face, kind, key }); } catch (_) { /* the host's own */ } };
    const show = face => { if (typeof opts.show === 'function') tryDo(() => opts.show(face)); };   // (the work area shows that face: Engrave.showFace through mountPiece)
    /** The face shown and edited. Returns true when the panel moved to it (a face nothing engraves is not edited here: the card still shows it). */
    const setFace = face => {
      const m = model(); if (!m || !['front', 'back'].includes(face) || modesFor(line).length === 0) return false;
      const { vm, d } = m;
      if (!facesOf(d.mode).includes(face)) return false;   // (a face the engraving does not need has no words to edit; the Front / Back toggles engrave it)
      { const cur = el.querySelector('.engp'); if (d.face === face && cur && cur.dataset.face === face) return true; }   // (already this face: nothing is drawn again, so a box being typed in keeps its caret, e.g. when Engrave.showFace's turn lands after the press already switched the panel)
      d.face = face; d.focus = null; draw(); paintAll(vm, d);
      try { if (typeof opts.onFace === 'function') opts.onFace(face); } catch (_) { /* the host's own */ }
      return true;
    };

    const done = (m, res) => {
      const { vm, d } = m; let ok = false;
      d.busy = '';
      if (res && res.cancelled) { /* the name was put away: calm, nothing said */ }
      else if (!res || !res.ok) d.error = (res && res.message) || 'Not kept. Try again.';
      else { ok = true; DRAFTS.delete(key); }
      redrawLive();
      if (ok) { const m2 = model(); if (m2) paintAll(m2.vm, m2.d); changed('saved', d.face); }
      void vm; return ok;
    };
    const run = async (m, patch) => {
      m.d.error = ''; m.d.confirm = ''; m.d.busy = 'Saving…'; draw();
      let res; try { res = await (opts.save ? opts.save(patch) : hostSave(line, Object.assign({}, patch, slot ? { slot } : {}))); } catch (e) { res = { ok: false, message: `Not saved: ${e && e.message ? e.message : e}` }; }
      done(m, res);
      return res || { ok: false };
    };
    /** LIVEWORDS / STAGEALL: KEEP what is staged (words, fonts, the faces engraved), with no question and no warning (Paul, 29 Oct 2026: "I don't ever want to see this warning"): the one save path
     *  (Engrave.saveSetup), called by Approve alone. A change the line cannot take (a piece already cut) is said and the faces engraved go back to what is kept. -> { ok, same?, message? } */
    const commit = async m => {
      const { vm, d } = m, S = Setup(), patch = S.patchOf(vm, d);
      if (!hasPatch(patch)) return { ok: true, same: true };
      const cons = S.consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup);
      if (cons && cons.blocked) { d.error = cons.blocked; if (d.mode !== vm.mode) { d.mode = vm.mode; const on = facesOf(d.mode); if (!on.includes(d.face)) d.face = on[0] || d.face; } draw(); paintAll(vm, d); return { ok: false, message: cons.blocked }; }
      return run(m, patch);
    };
    /** The faces engraved change (None / Front / Back): STAGED, like typed words (STAGEALL, Paul 29 Oct 2026: "The user is allowed to choose none front or back without any explanations ... it's not approved until the
     *  Approve button is pressed again"): the toggle and the previews change at once, Approve keeps it, leaving the card lets it go. NONEPLAIN 2: None shows the charm alone; Front or Back pressed again brings back
     *  exactly what was there (nothing was saved, so an approval is still an approval). */
    const pressMode = (m, want) => {
      const { vm, d } = m, on = facesOf(d.mode);
      if (want !== 'none' && on.includes(want)) { if (!setFace(want)) d.face = want; show(want); return; }   // (engraved already: a press shows it, never takes it away)
      if (want === 'none' && d.mode === 'none') return;
      d.error = '';
      if (want === 'none') { d.wasMode = d.mode; setMode(vm, d, 'none'); }
      else if (d.mode === 'none' && d.wasMode && facesOf(d.wasMode).includes(want)) { d.mode = d.wasMode; d.wasMode = ''; d.face = want; }
      else { setMode(vm, d, on.length ? 'both' : want); d.face = want; }
      draw(); paintAll(vm, d); changed('mode', d.face);
      if (want !== 'none' && faceJobOf(vm, want, slot)) show(want);
    };
    const click = e => {
      const b = e.target && e.target.closest && e.target.closest('[data-engp]'); if (!b || !el.contains(b) || b.disabled || dead) return;
      const m = model(); if (!m) return; const { vm, d } = m, a = b.dataset.engp;
      if (d.busy) return;
      if (a === 'face') { setFace(b.dataset.face); show(b.dataset.face); }
      else if (a === 'mode') pressMode(m, b.dataset.mode);
    };
    const input = e => {
      const t = e.target; if (!t || !t.dataset || !t.dataset.engp || dead) return;
      const m = model(); if (!m) return; const { vm, d } = m, f = t.dataset.face; if (!f) return;
      const x = d.faces[f] || (d.faces[f] = { text: '', font: '' });
      if (t.dataset.engp === 'words') { if (e.type === 'change' || !t.isConnected) return; x.text = t.value;   // (every letter arrives as an input; a words box's own "change" fires when it loses the focus, also when a redraw takes it away, and carries nothing new)
        growBox(t); } else if (t.dataset.engp === 'font') { if (x.font === t.value || !t.isConnected) return; x.font = t.value; } else return;
      d.dirty = true; d.confirm = ''; d.error = ''; d.noRoom = {};
      if (t.selectionStart != null) d.focus = { engp: t.dataset.engp, face: f, mode: '', act: '', start: t.selectionStart, end: t.selectionEnd };   // (the caret as it is now: a card drawn again under the typing (the other face's own card opening) gives it back here)
      if (t.dataset.engp === 'font' && t.tagName === 'SELECT') t.style.fontFamily = (t.selectedOptions[0] && t.selectedOptions[0].style.fontFamily) || '';
      const p = el.querySelector('.engp'); if (p) { if (hasPatch(Setup().patchOf(vm, d))) p.dataset.staged = '1'; else delete p.dataset.staged; }
      hiDecorate(el, vm, d, slot);   // (ENGHILITE: the tint follows the staged words / font; the marks stay)
      stageGate(vm, d);   // (STAGEALL: Approve lights up with the first letter, and goes calm again when the words are as approved)
      paint(vm, d, f); changed(t.dataset.engp, f);   // (LIVEWORDS: the face's thumbnail at once, the work area's draft in the same tick: Engrave.draftFit)
    };
    const key_ = e => {
      const t = e.target; if (!t || !t.dataset || !t.dataset.engp) return;
      if (t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.tagName === 'INPUT') e.stopPropagation();   // (the card's own keys, S for "No engraving", A for Approve, are for the card, not for words)
    };
    const focusin = e => { const t = e.target; if (!t || !t.dataset || !t.dataset.engp || dead) return; const m = model(); if (m) m.d.focus = { engp: t.dataset.engp, face: t.dataset.face || '', mode: t.dataset.mode || '', act: t.dataset.act || '', start: t.selectionStart, end: t.selectionEnd }; };
    const focusout = e => { if (e.relatedTarget) { const m = model(); if (m) m.d.focus = null; } };
    const ev = [['click', click], ['input', input], ['change', input], ['keydown', key_], ['focusin', focusin], ['focusout', focusout]];
    for (const [t, f] of ev) el.addEventListener(t, f);
    /* ROTBAR: a turn of the WHOLE piece made with the work area's degree box (charm-nest-bridge.js turnTo) is a position change like the Position buttons: it is the job's at once (LIVEWORDS: nothing to save
     * or cancel); `beforeMove(job)` / `moved(job, kind)` stay for the bridge, `moved` draws the thumbnails again. It changes no word, font or mode. */
    const beforeMove = job => !!(job && job.key && model());
    const moved = (job, kind) => {
      const m = model(); if (!m || !job || !job.key) return false; const { vm, d } = m;
      d.moved[job.key] = true; d.noRoom = {}; draw(); paintAll(vm, d); changed(kind || 'position', d.face); return true;
    };
    /** Keep what is staged now, quietly (a test, a host that closes the card itself). -> Promise<{ ok, ... }> */
    const commitNow = () => { const m = dead ? null : model(); return m ? commit(m) : Promise.resolve({ ok: false }); };
    return { draw, refresh: draw, setFace, beforeMove, moved, commit: commitNow, stageNone, destroy() { dead = true; untrack(); sweepSoon(); for (const [t, f] of ev) el.removeEventListener(t, f); if (gateCard) { gateCard.removeEventListener('click', blockClick, true); gateCard.removeEventListener('keydown', blockKey, true); } }, paintAll: () => { const m = model(); if (m) paintAll(m.vm, m.d); } };
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

  /** The slot of the Engrave detail card: the Front / Back thumbnails (when the page has CharmNestEngraveCard) above the panel. LIVEWORDS (Paul, 29 Oct 2026, points 2 and 5: "the user can now click on the
   *  actual back and front thumbnail"): the thumbnails ARE the face switch: a press marks that thumbnail, the panel edits that face (when it is engraved) and the work area shows it, through
   *  Engrave.showFace(job, face, { animate: true }) (the work area's own animated turn; until the page has it, the work area's FACEVIEW switch or the card of that face's own job, as before). */
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
      const hasShow = () => { const E = Eng(); return !!(E && typeof E.showFace === 'function'); };
      /** The tab shows the card of the job of face `f` of this piece, when that face has a job of its own that is not the one on screen. */
      const jobNav = f => {
        const E = Eng(), other = E && E.jobsOf ? (E.jobsOf(line) || []).find(j => (slotOfJob(j) || null) === (slotOfJob(job) || null) && F && F.faceOf(j) === f) : null;
        if (!other || other.key === job.key) return;
        // (ENGFRONT: a face that is already decided lives in the Decided list, where the app opens it (EngraveLink): the Placements card cannot show it, so the press used to snap back to the other face)
        if (typeof o.refocus === 'function') tryDo(() => o.refocus(other));
        else if (['approved', 'written', 'skipped'].includes(other.state) && root.EngraveLink && root.EngraveLink.open) tryDo(() => { Promise.resolve(root.EngraveLink.open({ piece: other.key, face: f })).catch(() => {}); });
        else if (E.restoreView && E.render) tryDo(() => { E.restoreView({ focus: other.key }); E.render(); });
      };
      /** The work area shows `f` (one call, whoever asked: a thumbnail, a face button, a face just engraved): Engrave.showFace turns it as the sheets turn and, when the turn lands, gives the
       *  panel that face (this mount's setFace below: the panel, the marked thumbnail, the card of that face's own job). Until the page has it: FACEVIEW's switch, then the face's own job's card. */
      const navigate = f => {
        const E = Eng(), cur = tryDo(() => ((E && E.jobsOf ? E.jobsOf(line) : []) || []).find(j => j && j.key === job.key), null) || job;
        if (hasShow()) { tryDo(() => E.showFace(cur, f, { animate: true })); return; }
        if (typeof o.onView === 'function') tryDo(() => o.onView(f));
        jobNav(f);
      };
      /** A press on a thumbnail: that thumbnail is marked and the panel edits that face at once (a face nothing engraves keeps the panel where it is); the work area turns to it. */
      const pick = f => {
        // (the panel's face, words box, font and position switch in THIS tick, so typing right after the press lands in that face's box; the work area's turn is only the animation)
        if (panel) tryDo(() => panel.setFace(f));
        if (card && card.update) tryDo(() => card.update({ face: f }));   // (marked: drawn again only when the mark moved)
        navigate(f);
      };
      // (FACEVIEW, Paul 28 Oct 2026: the card also shows a face the Charm Editor drew on that nothing engraves (o.artFaces); ENGSMART: a face nothing engraves is not drawn, so a piece engraved on both
      //  faces has both thumbnails, one engraved on one face has that one)
      if (C && C.mountPiece) card = tryDo(() => C.mountPiece(cardHost, { row: line, key: job.key, face: o.view || face, stage: stage(), onPick: pick, artFaces: o.artFaces, approve: false, nameOf: o.nameOf || lineName, isWorking: Eng() && Eng().isWorking ? Eng().isWorking : undefined }), null);
      if (!card) cardHost.remove();
      /** The card follows the STAGED edit: the faces it draws are the ones the staged mode asks for (the same tick), the face being edited is marked, and the thumbnails get the staged words back. */
      const follow = f => { if (!card || !card.update) return; tryDo(() => card.update({ face: f, stage: stage() })); tryDo(() => panel.paintAll()); };
      panel = mount(panelHost, line, {
        job, face, scope: slot, faceSwitch: !card, onDraft: o.onDraft, onVisible: v => { slot.hidden = !v && !card; }, thumb: f => (card && card.thumb ? card.thumb(f) : null), show: navigate,
        onFace: f => {
          follow(f);   // (the card draws its thumbnails from the saved jobs: the staged words go back on them)
          if (typeof o.onFace === 'function') tryDo(() => o.onFace(f));
        },
        onChange: ev => {
          if (card && ev) {
            if (ev.kind === 'mode' || ev.kind === 'saved' || ev.kind === 'turn') follow(ev.face);   // (a turn of the whole piece: the thumbnails draw the charm turned, so they are drawn again)
            else if (ev.kind === 'words' && card.label) { const d = DRAFTS.get(ev.key), x = d && d.faces[ev.face]; tryDo(() => card.label(ev.face, x ? x.text : '')); }   // (the words under the picture are the words being typed)
          }
          if (typeof o.onChange === 'function') tryDo(() => o.onChange(ev));
        }
      });
      return { destroy() { tryDo(() => panel.destroy()); if (card && card.destroy) tryDo(() => card.destroy()); }, refresh() { tryDo(() => panel.refresh()); }, setFace: f => { const moved = !!tryDo(() => panel.setFace(f), false); if (!moved && card && card.update) tryDo(() => card.update({ face: f })); jobNav(f); return moved; }, show: pick, panel, card };   // (setFace: what Engrave.showFace calls when its turn lands)
    } catch (e) { if (typeof console !== 'undefined') console.warn('engraving panel slot', e); return none; }
  }

  return { mount, mountPiece, html, bind, modesFor, seedText, stageOf, sweep, isLive, DRAFTS, MODES, hilite: { model: hiModel, decorate: hiDecorate, systemChoice, reasonLine, HI_TITLE } };
});
