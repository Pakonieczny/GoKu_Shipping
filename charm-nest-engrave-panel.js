/* The ENGRAVING PANEL of the Engrave tab's detail card (Paul, 12 Oct 2026: "This UI is totally broken everywhere ... it should only be part of the detailed Engraving modal popup within the
 * Engraving tab ... do NOT crowd the menu" and "the user should be able just to click on either one and then update it with the text font options and positional update of the text on the
 * charm, and the actual text itself. Everything has to be linked to its engraving, whether it be front or back on the same charm or multiple charms in the same order").
 * Plan: plans/engrave-ui-1012 (CONTRACT.md section D). The model (read, patchOf, consequences, buildOverride, fontChoices) and the save path are charm-nest-engrave-setup.js's.
 *
 * ENGSTAGE (Paul, 30 Oct 2026: "every letter typed into the text box immediately updates itself in BOTH the Thumbnail image and in the main charm preview ... they should be able to toggle back and in between any of
 * the four options and everything should be seamless ... users will be clicking here very quickly and clicking the approved button and expecting things to happen immediately"; "When Engraving setting (None or Front
 * or Back) has already been chosen and "Approved" and then the user updates their original choice then the "Approved" button must become immediately clickable again ... If the user does not click "Approve" again
 * then the changes will be lost"; "We also need a clean and elegant solution to choose both Back & Front"). Spec: stage-spec/SPEC.md.
 *
 * TWO independent things, both STAGED in the piece's draft (DRAFTS, by piece key `<lineKey>[#slot]`), never kept until Approve:
 *   choice   none | front | back | both: which faces are engraved (a line that never engraves draws nothing: CharmNestEngraveFaces.neverOf; a front-only line offers None | Front)
 *   active   front | back: the face the words box edits and the work area shows (a thumbnail press; Front / Back pressed in the choice; the face the card was opened on)
 *   per face { text, font, pos } (pos: { charm: [x, y], centre: [x, y] } dragged in the work area, CharmNestEngravePanel.stagePos, or null for the automatic place)
 * dirty = what is staged differs from what is kept (the jobs, the setup, the approvals); while an Approve is being kept, from what that press staged.
 *
 * ONE column, nothing explained in words:
 *   [ None | Front | Back | Front & Back ]  one segmented control; ONE white raised pill shows the choice and slides between the options (Back looks like every other option: the ✦ of the system's own
 *                                         reading is a small mark, never a tint). A press stages at once: None draws both faces plain, Front / Back makes that face active and engraved and the other plain,
 *                                         Front & Back engraves both and keeps the face shown.
 *   [words]                               the active face's words (placeholder "Front words" / "Back words"). Typing on a face that is not engraved engraves it too (back + front typed = both, none + front = front).
 *   [font]                                the active face's font
 * Every change paints at once through ONE call, Engrave.showStage(job, stage) (the thumbnails and the work area; until the page has it the old previews: CharmNestEngravePreview.update, Engrave.draftFit,
 * Engrave.stageFace, Engrave.showFace). The green Approve of the card is the panel's: enabled while something is staged and every engraved face has words (or None is staged), calm "Approved" when nothing
 * is staged and the piece is approved, off with a soft gold pulse on an engraved face with no words. One press paints "Approved ✓" in the same frame and keeps the staged state in the background
 * (Engrave.commitStage, one chain per piece: a press meanwhile with nothing new is ignored, edits meanwhile are staged on top and light Approve again); a failure gives Approve back with the staged state
 * and one short toast. Leaving the piece's card without Approve discards the draft: everything shows what is kept.
 *
 *   mount(slot, row, opts?)     draws the panel into `slot` for the piece of `row` and wires it -> { destroy(), refresh(), setFace(face), draw(), commit(), press() }. Safe to call again after every redraw of the host.
 *                               opts: { job (the job the card shows), face, slot, jobs(), onFace(face), onChange({ face, kind, key }), onStage(stage, kind), thumb(face), show(face), scope }
 *   mountPiece(slot, job, o?)   the slot of the Engrave detail card: the Front / Back thumbnails (CharmNestEngraveCard.mountPiece) above the panel, linked both ways
 *   stagePos(key, face, pos)    a drag in the work area: the face's staged place (the work area's own report)
 *   stageOf(key) / stateOf(key) the staged stage (SPEC's showStage shape) / { choice, active, dirty, gate } of a piece, or null
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
  const CHOICES = [['none', 'None'], ['front', 'Front'], ['back', 'Back'], ['both', 'Front & Back']];   // (ENGSTAGE: four options, one selected; "Both" when the column is too narrow for its words)
  const MODES = CHOICES;
  const FACES = ['front', 'back'];
  const facesOf = mode => (mode === 'both' ? ['front', 'back'] : mode === 'front' ? ['front'] : mode === 'back' ? ['back'] : []);
  const withFace = (choice, f) => { const on = new Set(facesOf(choice)); on.add(f); return on.has('front') && on.has('back') ? 'both' : on.has('front') ? 'front' : on.has('back') ? 'back' : 'none'; };
  const faceOf = j => { const F = Faces(); return F && F.faceOf ? F.faceOf(j) : (j && j.face === 'front' ? 'front' : 'back'); };
  const hasPatch = p => !!(p && (p.mode || p.front !== undefined || p.back !== undefined));
  const slotOfJob = j => { if (!j) return null; const F = Faces(); return j.slot || (F && j.key ? F.parseKey(j.key).slot : null) || null; };
  const pieceKeyOf = (line, slot) => str(line && line.key) + (slot ? '#' + slot : '');
  const normWords = v => linesOf(v).join('\n');
  /** The choices a piece may be given. A line that never engraves is not drawn at all; a front-only line gets None | Front. */
  function choicesFor(line) {
    const F = Faces(); if (F && F.neverOf && F.neverOf(line)) return [];
    const sp = line && line.spec, only = sp && sp.engraveFrontOnly;
    return only ? CHOICES.filter(([m]) => m === 'none' || m === 'front') : CHOICES;
  }
  const modesFor = choicesFor;
  const facesFor = line => (choicesFor(line).some(([m]) => m === 'back') ? FACES : choicesFor(line).length ? ['front'] : []);

  /* ── the staged state ─────────────────────────────────────────────────────── */
  const DRAFTS = new Map();    // piece key -> { key, sig, choice, active, faces: { front: { text, font, pos }, back }, seq, pillAt, copied, jobKey, shown, focus, error }
  const LIVE = new Map();      // piece key -> the bindings on screen
  const COMMITS = new Map();   // piece key -> { seq, draft, stage } of the Approve being kept now (the baseline the draft is compared with until it lands)
  const CHAINS = new Map();    // piece key -> the promise of the last Approve queued (ENGSTAGE: one chain per piece)
  let uid = 0;
  /* ── the life of a draft ────────────────────────────────────────────────────
   * A draft lives through every redraw of its card (the tab draws the detail again on each change) and ends with Approve (kept), or when the piece's card is left (another piece, the list, another tab, closed): then
   * it is DISCARDED (STAGEALL / ENGSTAGE item 3: "If the user does not click "Approve" again then the changes will be lost"), and the piece shows what is kept everywhere. "Live" = a panel of that piece is on the page. */
  const GRACE = 250;       // ms of quiet after the page has drawn again before a piece with no panel on the page loses its draft (a redraw swaps the card in the same task)
  let sweepT = 0, watching = null;
  const isLive = key => { const set = LIVE.get(key); if (!set) return false; for (const b of set) if (b.el && b.el.isConnected) return true; return false; };
  function sweep(except) { for (const k of [...DRAFTS.keys()]) if (k !== except && !isLive(k)) { const d = DRAFTS.get(k); DRAFTS.delete(k); if (d && typeof d.flush === 'function') tryDo(() => d.flush()); } }
  const sweepSoon = () => { try { clearTimeout(sweepT); sweepT = setTimeout(() => sweep(), GRACE); } catch (_) { /* a missed sweep ends at the next opening */ } };
  /** One observer on the Engrave tab's own view: every drawing of it wakes one quiet sweep. Nothing is read or written. */
  function watch(doc) {
    try {
      const v = doc && doc.getElementById && doc.getElementById('engraveView'); if (!v || watching === v || typeof MutationObserver !== 'function') return;
      if (watching && watching._engpMo) watching._engpMo.disconnect();
      const mo = new MutationObserver(() => { if (DRAFTS.size) sweepSoon(); }); mo.observe(v, { childList: true, subtree: true }); v._engpMo = mo; watching = v;
    } catch (_) { /* the draft then ends at the next piece opened */ }
  }

  const posOk = p => !!p && Array.isArray(p.charm) && p.charm.length >= 2 && p.charm.every(Number.isFinite);
  const posOf = p => (posOk(p) ? { charm: [p.charm[0], p.charm[1]], ...(Array.isArray(p.centre) && p.centre.length >= 2 ? { centre: [p.centre[0], p.centre[1]] } : {}) } : null);
  const samePos = (a, b) => (!posOk(a) && !posOk(b)) || (posOk(a) && posOk(b) && Math.abs(a.charm[0] - b.charm[0]) < 1e-6 && Math.abs(a.charm[1] - b.charm[1]) < 1e-6);
  const cloneFaces = faces => { const o = {}; for (const f of FACES) { const x = (faces && faces[f]) || {}; o[f] = { text: str(x.text), font: str(x.font), pos: posOf(x.pos) }; } return o; };
  /** The stage of a draft as Engrave.showStage takes it: { key, choice, active, faces: { front: { text, font, pos }, back } } (+ mode and plain, for a card that still reads the old shape). */
  function stageFrom(d, line) {
    const allowed = line ? facesFor(line) : FACES, on = facesOf(d.choice);
    return { key: d.key, choice: d.choice, active: d.active, faces: cloneFaces(d.faces), seq: d.seq, mode: d.choice, plain: allowed.filter(f => !on.includes(f)) };
  }
  /** What the card must show for a piece: its staged stage, or null when nothing of it is staged (the card shows what is kept). */
  function stageOf(key) { const d = DRAFTS.get(key); return d ? stageFrom(d, d.line) : null; }

  function draftOf(vm, key, face) {
    let d = DRAFTS.get(key);
    let sv = null;
    let typed = null;
    if (d && d.sig !== vm.sig && !COMMITS.has(key) && !d.touched) { const keep = d.active; sv = { svStage: d.svStage, svDragged: d.svDragged, why: d.why }; typed = d.faces; d = null; face = keep || face; }   // (svStage / svDragged: stage-view's own fields on the draft, carried over)   // (what the piece says changed under a draft nobody changed: start again from it, on the face shown)
    if (!d) {
      d = { key, sig: vm.sig, line: vm.line, choice: vm.mode, active: '', faces: {}, seq: 0, touched: false, dirty: false, pillAt: null, copied: {}, error: '', focus: null, shown: '', jobKey: '' };
      for (const f of FACES) { const r = vm.rows.find(x => x.face === f); d.faces[f] = { text: r ? r.text : '', font: r ? r.fontWord : '', pos: null }; }
      // (CAPSWORDS, coordinator 30 Oct 2026: "never replace the box's contents with the job's normalized text while the card is open": the words as the person typed them stay in the box when they are
      //  the words kept (an approval of "Name Oneo" keeps "NAME" / "ONEO" in Ace: the box keeps "Name Oneo"))
      if (typed) { const S = Setup(); for (const f of FACES) { const t = typed[f] && str(typed[f].text), r = vm.rows.find(x => x.face === f); if (t && r && r.text && t !== r.text && S && S.sameKept && tryDo(() => S.sameKept(vm, f, t, r.fontWord), false)) d.faces[f].text = t; } }
      d.active = firstActive(vm, d, face);
      if (sv) for (const k of ['svStage', 'svDragged', 'why']) if (sv[k] !== undefined) d[k] = sv[k];   // (and why the last Approve did not approve: said until the next change or press)
      DRAFTS.set(key, d);
    }
    return d;
  }
  /** The face shown first: the face the card was opened on when it is engraved, else the engraved face that has words, else the back (a front-only line: the front). */
  function firstActive(vm, d, face) {
    const allowed = facesFor(vm.line), on = facesOf(d.choice);
    if (face && allowed.includes(face) && (on.includes(face) || !on.length)) return face;
    if (on.length === 1) return on[0];
    const worded = on.find(f => linesOf(d.faces[f] && d.faces[f].text).length); if (worded && on.length) return face && on.includes(face) ? face : worded;
    return allowed.includes('back') ? 'back' : allowed[0] || 'back';
  }
  /** EDITOR WORDS (Paul, 27 Oct 2026: "the system should not force the user to also add text on top of already added text from within the Charm Editor"): is this face engraved by the charm editor's parts? */
  const artOn = (vm, face) => { const S = Setup(); return !!(face && S && S.editorArtOn && tryDo(() => S.editorArtOn(vm, face), false)); };
  /** The words a face starts with when it is engraved and nothing was typed: the line's own, else the buyer's labelled words, else (one face and no labels) the other face's. Never a guess between two faces. */
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
  /** The choice changes. Front or Back alone: that face keeps its words, or starts with the words the order gives it (the line's own, the buyer's labelled words, else the other face's: Paul's case, Back "Ruby",
   *  Front pressed: the front gets "Ruby"); words taken from the other face MOVE (that face, no longer engraved, is left with none). Front & Back: a face added starts empty unless the buyer labelled words for
   *  it; a face that has its words keeps them (None, then Front & Back again, brings back what was there). */
  function setChoice(vm, d, choice) {
    const S = Setup(); d.choice = choice; d.error = '';
    for (const f of facesOf(choice)) {
      const x = d.faces[f] || (d.faces[f] = { text: '', font: '', pos: null }), of = f === 'front' ? 'back' : 'front', o = d.faces[of] || (d.faces[of] = { text: '', font: '', pos: null });
      if (str(x.text).trim()) continue;
      if (choice === 'both') { const lab = tryDo(() => S.labelled(vm.raw), null); x.text = lab && lab[f] ? lab[f].join('\n') : ''; d.copied[f] = false; continue; }
      const own = vm.rows.find(r => r.face === f && r.text), t = seedText(vm, f, choice);
      x.text = t; d.copied[f] = !!t && !own && normWords(t) === normWords(o.text);
      if (d.copied[f]) { o.text = ''; d.copied[of] = false; }   // (moved: the face left behind keeps no copy of them)
    }
  }
  /** What a save would make of the piece's setup (for the consequences). */
  function nextSetupOf(vm, patch) {
    const F = Faces(), S = Setup(), line = vm.line, rd = tryDo(() => { const sp = line.spec, e = sp && sp.engraving; return e && typeof e === 'object' && F.MODES.includes(e.mode) ? e : S.derivedReading(line); }, null);
    const spec = rd && rd !== line.spec.engraving ? Object.assign({}, line.spec, { engraving: rd }) : line.spec;
    return F.setupOf({ spec }, S.buildOverride(line, patch, 'x', 0), vm.slot || undefined);
  }
  const viewOf = d => ({ mode: d.choice, faces: d.faces });
  /** Does the draft differ from what is kept: what the jobs and the setup hold NOW (read again at every look, a commit under way or not)? (coordinator, 30 Oct 2026: never against a press's snapshot) */
  function dirtyOf(vm, d, key) {
    void key; const S = Setup(); if (hasPatch(tryDo(() => S.patchOf(vm, viewOf(d)), null))) return true;
    if (facesOf(d.choice).some(f => posOk(d.faces[f] && d.faces[f].pos))) return true;
    // (coordinator, 30 Oct 2026: a piece with an approved face whose other engraved face is not approved (it went back to review: its words changed, it was moved) is staged, never calm: Approve stays lit for it)
    const jobsOfFace = f => (vm.rows.find(r => r.face === f) || { jobs: [] }).jobs || [];
    const ok = j => j.state === 'approved' || j.state === 'written';
    return FACES.some(f => jobsOfFace(f).some(ok)) && facesOf(d.choice).some(f => jobsOfFace(f).some(j => !ok(j) && j.state !== 'none' && j.state !== 'skipped'));
  }
  /** Is the draft exactly the stage of a press (`st`: its choice, and the words, font and place of every face it engraves)? */
  function sameStage(d, st) {
    if (!st || d.choice !== st.choice) return false;
    return facesOf(d.choice).every(f => normWords(d.faces[f].text) === normWords(st.faces[f].text) && str(d.faces[f].font) === str(st.faces[f].font) && samePos(d.faces[f].pos, st.faces[f].pos));
  }
  /** Is what is kept approved? Every face it engraves has its piece's job approved or saved to its sheet (None: a regular order, nothing to approve). */
  function keptApproved(vm) {
    return facesOf(vm.mode).every(f => { const r = vm.rows.find(x => x.face === f); return !!r && r.jobs.length > 0 && r.jobs.every(j => j.state === 'approved' || j.state === 'written'); });
  }
  /** The engraved faces with no words (a face the charm editor drew needs none; a line's face whose words differ per piece is each piece's). */
  const emptyFaces = (vm, d) => facesOf(d.choice).filter(f => !linesOf(d.faces[f] && d.faces[f].text).length && !artOn(vm, f) && !(vm.rows.find(r => r.face === f) || {}).perPiece);
  /** THE GATE of Approve: { st: 'approve' | 'need' (an engraved face has no words: faces) | 'blocked' (why) | 'busy' (a face is being read or placed) | 'approved' (calm) | 'ok' (the press is being kept: "Approved ✓") , dirty } */
  function gateOf(vm, d, key) {
    const c = COMMITS.get(key), dirty = dirtyOf(vm, d, key);
    if (c && sameStage(d, c.stage) && (RUNNING.has(key) || PENDING.has(key))) return { st: 'ok', dirty };   // (the press of exactly this stage is being kept, or waits its turn: "Approved ✓"; data-dirty still says the jobs do not hold it yet)
    const empty = emptyFaces(vm, d);
    if (dirty) {
      if (empty.length) return { st: 'need', faces: empty, dirty };
      { const S = Setup(), patch = tryDo(() => S.patchOf(vm, viewOf(d)), {}); if (hasPatch(patch)) { const cons = tryDo(() => S.consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup), null); if (cons && cons.blocked) return { st: 'blocked', why: cons.blocked, dirty }; } }
      return { st: 'approve', dirty };
    }
    if (keptApproved(vm)) return { st: 'approved', dirty: false };
    // (nothing staged and a face of the piece is being read or placed (the order changed, words just kept), or holds words the laser cannot engrave: Approve waits, off, until the card is drawn again for it)
    const pj = facesOf(d.choice).flatMap(f => (vm.rows.find(r => r.face === f) || { jobs: [] }).jobs || []);
    if (pj.some(j => ['classify', 'reclassify', 'ready', 'fitting'].includes(j.state) || j.state === 'blocked' || (j.state === 'words' && j.missing && j.missing.length))) return { st: 'busy', dirty: false };
    return empty.length ? { st: 'need', faces: empty, dirty: false } : { st: 'approve', dirty: false };
  }

  /* ── the position of a face (CharmNestEngravePosition, ENGPOS) ─────────────── */
  const faceJobOf = (vm, face, slot) => { const r = vm.rows.find(x => x.face === face); return r && r.jobs ? (r.jobs.find(j => (slotOfJob(j) || null) === (slot || null)) || (slot ? null : r.jobs[0]) || null) : null; };
  const positionOf = job => { const V = Prev(); if (V && V.positionOf) return job ? tryDo(() => V.positionOf(job), null) : null; const P = Pos(); return job && P && P.of ? tryDo(() => P.of(job), null) : null; };

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
   * What the system chose is CharmNestEngraveSetup.autoChoice(line, slot) (ENGAUTO) when the page has it, else the reading under the person's override (vm.base). It is MARKED, never moved: a small gold ✦ on the
   * system's option (ENGSTAGE: a mark only, never a tint, so the selected option looks the same on every option, Back included), on the system's font and on the words box of the face shown. */
  const HI_TITLE = 'Chosen by the system';
  const HI_SPARK = '<svg viewBox="0 0 12 12" aria-hidden="true" focusable="false"><path d="M6 .4C6.4 3.8 8.2 5.6 11.6 6 8.2 6.4 6.4 8.2 6 11.6 5.6 8.2 3.8 6.4 .4 6 3.8 5.6 5.6 3.8 6 .4Z" fill="currentColor"/></svg>';
  const HI_GLYPH = '✦';
  const sameWord = (a, b) => str(a).trim().toLowerCase() === str(b).trim().toLowerCase();
  /** The same words? The engine's capitals (Ace) and its line split are not a person's change: compared as one run of words, in capitals for a capitals font. */
  function sameWords(a, b, fontId) {
    const O = dep('CharmNestOrders', './charm-nest-orders.js'), run = v => linesOf(v).join(' '), caps = t => { try { return O && O.capsText && fontId ? str(O.capsText(t, fontId)) : t; } catch (_) { return t; } };
    return caps(run(a)) === caps(run(b));
  }
  const rowFor = (vm, slot) => (slot || vm.slot ? Object.assign({}, vm.line, { slot: slot || vm.slot }) : vm.line);
  function fontIdFor(vm, face, word, slot) {
    const P = Prev(); try { if (P && P.fontIdOf) return str(P.fontIdOf(rowFor(vm, slot), face, word)); } catch (_) { /* below */ }
    const O = dep('CharmNestOrders', './charm-nest-orders.js'); try { return O && O.fontOfPhrase && word ? str(O.fontOfPhrase(word)) : ''; } catch (_) { return ''; }
  }
  function fontValueOf(vm, face, f, slot) {
    const S = Setup(), list = S && S.fontChoices ? S.fontChoices() : [], lineKey = str(vm.lineFont && vm.lineFont.word).toLowerCase(), word = str(f && f.fontWord).trim();
    const values = [''].concat(list.filter(c => c.word.toLowerCase() !== lineKey).map(c => c.word));
    if (word) { if (word.toLowerCase() === lineKey) return ''; const hit = values.find(v => v && sameWord(v, word)); if (hit) return hit; }
    const id = str(f && f.fontId) || (word ? fontIdFor(vm, face, word, slot) : '');
    if (id) { const hit = values.find(v => fontIdFor(vm, face, v, slot) === id); if (hit !== undefined) return hit; }
    return word;
  }
  function systemChoice(vm, slot) {
    const S = Setup(); let a = null;
    if (S && typeof S.autoChoice === 'function') a = tryDo(() => S.autoChoice(vm.line, slot || vm.slot || undefined, { jobs: vm.jobs }), null);
    if (!a || !MODES.some(([m]) => m === a.mode)) { const b = vm.base || vm.setup || {}; a = { mode: b.mode, front: b.front, back: b.back }; }
    const faces = {};
    for (const f of FACES) {
      const x = a[f], on = facesOf(a.mode).includes(f);
      faces[f] = on && x ? { text: normWords(x.lines && x.lines.length ? x.lines : x.text), fontWord: str(x.fontWord).trim(), fontId: str(x.fontId) } : on ? { text: '', fontWord: '', fontId: '' } : null;
    }
    return { mode: a.mode, faces };
  }
  function reasonLine(line, slot) {
    const W = root && root.CharmNestEngraveWhy;
    if (W && typeof W.reasonOf === 'function') { const r = tryDo(() => W.reasonOf(line, slot || null), null); return r && str(r.text).trim() ? str(r.title || r.line) : ''; }
    const ov = line && line.engravingOverride, r = ov && typeof ov === 'object' ? ov.reason : null; if (!r || typeof r !== 'object') return '';
    if (slot && r.slot && r.slot !== slot) return '';
    const t = str(r.text).trim() || cap1(str(r.code).replace(/[_-]+/g, ' ').trim()), by = str(r.by).trim();
    return t ? 'Changed: ' + t + (by ? ' · ' + by : '') : '';
  }
  /** The pure model of the highlight for the active face: { sys, mode: {sys, same}, words: {sys, same}|null, font: {sys, label, same}|null, reason }. */
  function hiModel(vm, d, face, slot) {
    const mode = d.choice != null ? d.choice : d.mode, sys = systemChoice(vm, slot), on = facesOf(mode).includes(face), sf = face ? sys.faces[face] : null, x = (face && d.faces[face]) || { text: '', font: '' };
    const m = { sys, mode: { sys: sys.mode, same: mode === sys.mode }, words: null, font: null, reason: '' };
    if (sf && sf.text) m.words = { sys: sf.text, same: on && sameWords(x.text, sf.text, fontIdFor(vm, face, x.font, slot)) };
    if (sf) { const sv = fontValueOf(vm, face, sf, slot), lf = vm.lineFont || {}; m.font = { sys: sv, label: sv || str(lf.word || lf.name), same: on && fontValueOf(vm, face, { fontWord: x.font }, slot) === sv }; }
    return m;
  }
  const HI_CSS = `.engpSys{display:inline-flex;align-items:center;justify-content:center;width:14px;height:14px;color:var(--gold,#a9823f);cursor:default}
.engpSys svg{display:block;width:10px;height:10px}
.engpSys[data-st=changed]{opacity:.6}
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
  /** Put the system's marks on a drawn panel, or bring them up to date (every step finds what it made before). Never moves a control, never throws. */
  function hiDecorate(el, vm, d, slot) {
    try {
      const doc = el && el.ownerDocument, p = el && el.querySelector('.engp'); if (!doc || !p || !vm || !d) return;
      ensureHiCss(doc);
      const face = p.dataset.active || p.dataset.face || '', m = hiModel(vm, d, face, slot);
      const put = (host, st, title) => { let k = host.querySelector(':scope > .engpSys'); if (!k) { k = doc.createElement('i'); k.className = 'engpSys'; k.innerHTML = HI_SPARK; host.appendChild(k); } k.dataset.st = st; k.title = title; return k; };
      const drop = host => { const k = host && host.querySelector(':scope > .engpSys'); if (k) k.remove(); };
      // the choice: the ✦ stays on the system's option whatever is selected (the pill is the selection)
      for (const seg of p.querySelectorAll('.engpChoice .engpSeg[data-engp="choice"]')) {
        if (seg.dataset.choice === m.mode.sys) { seg.dataset.sys = m.mode.same ? 'same' : 'changed'; seg.title = HI_TITLE; put(seg, 'same', HI_TITLE); }
        else { delete seg.dataset.sys; drop(seg); seg.title = CHOICE_TITLE[seg.dataset.choice] || ''; }
      }
      const box = p.querySelector('.engpBox');
      if (box) {
        if (m.words) { const t = HI_TITLE + (m.words.same ? '' : ': ' + m.words.sys.replace(/\n/g, ' / ')); box.dataset.sys = m.words.same ? 'same' : 'changed'; put(box, m.words.same ? 'same' : 'changed', t); }
        else { delete box.dataset.sys; drop(box); }
      }
      const sel = p.querySelector('.engpFont');
      if (sel) {
        let fb = sel.parentElement; if (!fb || !fb.classList.contains('engpFontBox')) { fb = doc.createElement('span'); fb.className = 'engpFontBox'; sel.parentNode.insertBefore(fb, sel); fb.appendChild(sel); }
        for (const o of sel.options) { const mine = !!m.font && o.value === m.font.sys; if (mine) { o.label = HI_GLYPH + ' ' + o.text; o.dataset.sys = '1'; } else if (o.dataset.sys) { o.label = ''; o.removeAttribute('label'); delete o.dataset.sys; } }
        if (m.font) { fb.dataset.sys = m.font.same ? 'same' : 'changed'; if (m.font.same) { drop(fb); sel.title = HI_TITLE; } else { put(fb, 'changed', HI_TITLE + ': ' + m.font.label); sel.removeAttribute('title'); } }
        else { delete fb.dataset.sys; drop(fb); sel.removeAttribute('title'); }
      }
    } catch (_) { /* the panel works without its marks */ }
  }

  /* ── the markup ────────────────────────────────────────────────────────────── */
  const CHOICE_TITLE = { none: 'No engraving on this piece', front: 'Engrave the front only', back: 'Engrave the back only', both: 'Engrave the front and the back' };
  function faceHtml(vm, d, face, id) {
    const cur = vm.rows.find(r => r.face === face), x = d.faces[face] || { text: '', font: '' }, per = !!(cur && cur.perPiece), on = facesOf(d.choice).includes(face);
    const words = `<div class="engpBox"><textarea class="engpWords" id="${id}" data-engp="words" data-face="${face}" rows="3" placeholder="${cap1(face)} words" aria-label="${cap1(face)} words"${per ? ' disabled' : ''}>${esc(per || !on ? '' : x.text)}</textarea>${per ? '' : `<span class="engpTools"><button type="button" class="btn ghost xs engpAlpha emBtn" data-alpha title="Other alphabets: letters, words and phrases in Greek, Hebrew, Arabic, Chinese, Japanese, Korean, Latin and more, found by their English" aria-label="Other alphabets">Aα</button><button type="button" class="btn ghost xs engpEmoji emBtn" data-emoji title="Choose an emoji the laser can engrave and put it in the words">Emoji</button></span>`}</div>`;
    const font = `<select class="engpFont" data-engp="font" data-face="${face}" aria-label="${cap1(face)} font">${fontOptions(vm, x.font)}</select>`;
    const lock = per ? `<div class="engpLock" role="status">Differs per piece</div>` : '';
    return `<div class="engpFace" data-face="${face}">${words}${font}${lock}</div>`;
  }
  /** The panel for a view model (`vm` = CharmNestEngraveSetup.read) and its draft (`d`). ctx: { faceSwitch, gate } */
  function html(vm, d, ctx) {
    ctx = ctx || {};
    if (d.choice == null && d.mode != null) d.choice = d.mode;   // (an old-shaped draft from a test)
    if (!d.active) d.active = d.face || firstActive(vm, d, null);
    const id = 'engp' + (++uid), choices = choicesFor(vm.line), idx = choices.findIndex(([c]) => c === d.choice), at = d.pillAt != null && d.pillAt >= 0 && d.pillAt < choices.length ? d.pillAt : idx;
    const seg = `<div class="engpMode engpChoice" role="group" aria-label="Engraved faces" style="--engp-n:${choices.length};--engp-i:${Math.max(0, at)}"><span class="engpPill" aria-hidden="true"${idx < 0 ? ' hidden' : ''}></span>${choices.map(([c, t]) => `<button type="button" class="engpSeg" data-engp="choice" data-choice="${c}" data-mode="${c}" aria-pressed="${c === d.choice}" title="${esc(CHOICE_TITLE[c])}">${esc(t)}</button>`).join('')}</div>`;
    const allowed = facesFor(vm.line), face = allowed.includes(d.active) ? d.active : allowed[0] || '';
    const sw = ctx.faceSwitch && allowed.length > 1 ? `<div class="engpMode engpFaceSw" role="radiogroup" aria-label="Face">${allowed.map(f => `<button type="button" role="radio" class="engpSeg" data-engp="face" data-face="${f}" aria-checked="${f === face}">${cap1(f)}</button>`).join('')}</div>` : '';
    const g = ctx.gate || null, warn = g && g.st === 'blocked' ? `<div class="engpErr" role="alert">${esc(g.why)}</div>` : '';
    return `<div class="engp" data-key="${esc(d.key)}" data-choice="${esc(d.choice)}" data-active="${esc(face)}" data-mode="${esc(d.choice)}" data-face="${esc(face)}"${g && g.dirty ? ' data-dirty="1" data-staged="1"' : ''} role="group" aria-label="Engraving">${seg}${sw}${face ? faceHtml(vm, d, face, id) : ''}${warn}</div>`;
  }

  /* ── the look ──────────────────────────────────────────────────────────────── */
  /* HOVER (Paul, 30 Oct 2026: "Also the hover states for everything here are inadequate"): every control of the column has the same quiet states: on hover (pointer devices) a tint a step darker and a border a step
     toward the gold, in 120 ms; a 2 px gold ring 2 px out on keyboard focus, never removed; a 1 px press-down (the options: scale .98) in 80 ms; disabled: no hover, dimmed, the plain cursor. Reduced motion keeps
     the colours and drops the movement (the pill's slide too). */
  const CSS = `.engp{display:grid;gap:10px;min-width:0;container-type:inline-size;font:12px var(--sans,system-ui);color:var(--ink,#1c1a17);--engpGold:var(--gold,#a9823f);--engpHover:color-mix(in srgb,var(--line,#e4ddd0) 55%,var(--gold,#a9823f))}
.engp[hidden]{display:none!important}
.engpCard{margin-bottom:10px}
.engpCard:empty{display:none}
.engpCard .engFaces[data-n="1"]{grid-template-columns:repeat(2,minmax(0,1fr))}
.engpCard .engFaces[data-n="1"] .engFace{display:flex}
.engpCard .engFaces[data-n="1"] .engLbl{order:-1}
.engpCard .engFaces[data-n="1"] .engTxt{display:block;white-space:nowrap;font-size:12px;line-height:14px}
.engpMode{position:relative;display:flex;flex-wrap:nowrap;gap:3px;padding:3px;min-width:0;border:1px solid var(--line,#e4ddd0);border-radius:10px;background:var(--paper2,#ebe5d9)}
.engpPill{position:absolute;top:3px;bottom:3px;left:3px;width:calc((100% - 6px - (var(--engp-n,4) - 1) * 3px) / var(--engp-n,4));border-radius:8px;background:var(--card,#fffefb);box-shadow:0 1px 2px rgba(28,26,23,.16),0 0 0 1px rgba(28,26,23,.30);transform:translateX(calc(var(--engp-i,0) * (100% + 3px)));transition:transform .18s cubic-bezier(.2,.75,.25,1),box-shadow .12s ease-out;pointer-events:none;z-index:0}
.engpPill[hidden]{display:none}
.engpSeg{position:relative;z-index:1;flex:1 1 0;min-width:0;min-height:40px;white-space:nowrap;padding:6px 4px;border:0;border-radius:8px;background:transparent;font:600 13.5px var(--sans,system-ui);color:var(--ink70,#5b554c);cursor:pointer;transition:background-color .12s ease-out,color .12s ease-out,transform .08s ease-out}
.engpSeg[aria-pressed=true],.engpSeg[aria-checked=true]{color:var(--ink,#1c1a17)}
.engpFaceSw .engpSeg[aria-checked=true]{background:var(--card,#fffefb);box-shadow:0 1px 2px rgba(28,26,23,.16),0 0 0 1px rgba(28,26,23,.30)}
.engpSeg:disabled{opacity:.55;cursor:default}
@media (hover:hover){.engpSeg:not(:disabled):not([aria-pressed=true]):not([aria-checked=true]):hover{background:rgba(28,26,23,.07);color:var(--ink,#1c1a17)}
.engpChoice:has(.engpSeg[aria-pressed=true]:hover) .engpPill{box-shadow:0 2px 6px rgba(28,26,23,.22),0 0 0 1px rgba(28,26,23,.38)}
.engpWords:not(:disabled):hover,.engpFont:not(:disabled):hover{border-color:var(--engpHover);background:var(--card2,#faf7f1);transition:border-color .12s ease-out,background-color .12s ease-out}
.engpTools>button:not(:disabled):hover{background:var(--paper2,#ebe5d9);border-color:var(--engpHover)}
.pvApproval .engpApprove:not(:disabled):hover{background:#52694e;border-color:#52694e}}
.engpSeg:not(:disabled):active{transform:scale(.98)}
.engpTools>button:not(:disabled):active,.pvApproval .engpApprove:not(:disabled):active{transform:translateY(1px)}
.engp :is(.engpSeg,.engpWords,.engpFont,.engpTools>button):focus-visible,.pvApproval .engpApprove:focus-visible{outline:2px solid var(--engpGold,#a9823f);outline-offset:2px}
.engpFace{display:grid;gap:10px;min-width:0}
.engpWords,.engpFont{display:block;width:100%;min-width:0;box-sizing:border-box;border:1px solid var(--line,#e4ddd0);border-radius:8px;background:var(--card,#fffefb);color:inherit;transition:border-color .12s ease-out,box-shadow .12s ease-out}   /* (the fill fades only into a hover: the system's tint follows a typed letter at once) */
.engpFont{cursor:pointer}
.engpBox{position:relative;min-width:0}
/* BIGPANEL (Paul, 29 Oct 2026): the options 40 px, the words 17 px with three lines in view, Aα and Emoji 34 px stacked in the box's right edge, the font picker 44 px; no Position row. */
.engpWords{min-height:86px;resize:none;padding:8px 80px 8px 11px;font:17px/1.3 var(--sans,system-ui);overflow-y:auto}
.engpEmoji{position:absolute;right:6px;top:6px;padding:2px 8px}
.engpTools{position:absolute;right:6px;top:6px;display:flex;flex-direction:column;align-items:stretch;gap:5px;width:58px}.engpTools>.engpEmoji,.engpTools>.engpAlpha{position:static;min-height:34px;margin:0;padding:0 8px;font-size:13px;border-radius:8px;cursor:pointer;transition:background-color .12s ease-out,border-color .12s ease-out,transform .08s ease-out}.engpAlpha{font-weight:600}
.engpFont{min-height:44px;padding:8px 11px;font:15px var(--sans,system-ui);border-radius:9px}
.engpWords:disabled,.engpFont:disabled{background:var(--paper2,#ebe5d9);color:var(--ink70,#5b554c);cursor:default;opacity:.8}
.engpLock{font-size:11.5px;color:var(--ink45,#938c80)}
.engpErr{font-size:11.5px;color:var(--clay,#b0563f)}
/* ENGSTAGE: an engraved face with no words: its words box and its thumbnail breathe a soft gold outline (no text); Approve waits for words */
@keyframes engpNeed{0%,100%{box-shadow:0 0 0 0 rgba(169,130,63,0)}50%{box-shadow:0 0 0 3px rgba(169,130,63,.42)}}
.engp[data-need~=front] .engpWords[data-face=front],.engp[data-need~=back] .engpWords[data-face=back]{border-color:var(--gold2,#caa861);animation:engpNeed 1.4s ease-in-out 3}
[data-engp-need~=front] :is([data-eng-thumb=front],.engFace[data-face=front] .engThumb),[data-engp-need~=back] :is([data-eng-thumb=back],.engFace[data-face=back] .engThumb){outline:1.5px solid var(--gold2,#caa861);outline-offset:1px;animation:engpNeed 1.4s ease-in-out 3}
/* ENGSTAGE: the panel's one Approve in the card head: green while something is staged, calm "Approved" when nothing is, "Approved ✓" the moment it is pressed */
.pvApproval .engpApprove{transition:background-color .12s ease-out,border-color .12s ease-out,color .12s ease-out,opacity .12s ease-out,transform .08s ease-out}
.pvApproval .engpApprove:disabled{cursor:default}
.pvApproval .engpApprove[data-gate=need]:disabled,.pvApproval .engpApprove[data-gate=blocked]:disabled,.pvApproval .engpApprove[data-gate=busy]:disabled{opacity:.5}
.pvApproval .engpApproveWrap{position:relative}.engpStampAt{position:absolute;inset:0;visibility:hidden;pointer-events:none;overflow:hidden;font-size:0}
.engpTick{display:inline-block;margin-left:6px;transform-origin:50% 60%;animation:engpTick .32s cubic-bezier(.2,.9,.3,1.3) both}
@keyframes engpTick{0%{opacity:0;transform:scale(.4)}100%{opacity:1;transform:scale(1)}}
@media (prefers-reduced-motion:reduce){.engpPill{transition:box-shadow .12s ease-out}.engpSeg:not(:disabled):active,.engpTools>button:not(:disabled):active,.pvApproval .engpApprove:not(:disabled):active{transform:none}.engpTick{animation:none}.engp[data-need] .engpWords,[data-engp-need] :is([data-eng-thumb],.engThumb){animation:none}}`;
  function ensureCss(doc) {
    try { if (!doc || doc.getElementById('engpCss')) return; const s = doc.createElement('style'); s.id = 'engpCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); } catch (_) { /* unstyled is still usable */ }
  }
  const growBox = ta => { try { const cs = ta.ownerDocument.defaultView.getComputedStyle(ta), lh = parseFloat(cs.lineHeight) || 18, pad = (parseFloat(cs.paddingTop) || 0) + (parseFloat(cs.paddingBottom) || 0), max = Math.round(lh * 4 + pad + 2), min = parseFloat(cs.minHeight) || 0; ta.style.height = 'auto'; ta.style.height = Math.max(min, Math.min(ta.scrollHeight + 2, max)) + 'px'; } catch (_) { /* the box keeps its rows */ } };
  const toast = (msg, kind, ms) => { try { const w = root || {}, t = typeof w.toast === 'function' ? w.toast : w.CN && w.CN.toast; if (t) t(msg, kind || 'bad', ms || 6000, 'engrave-stage'); } catch (_) { /* the button says it */ } };
  const shortWhy = m => { const t = str(m).replace(/^Not (?:approved|saved):\s*/i, '').replace(/\s+/g, ' ').trim(); const one = t.split(/(?<=\.)\s/)[0] || t; return 'Not approved: ' + (one.length > 110 ? one.slice(0, 107) + '…' : one || 'try again'); };

  /* ── Approve: one press, at once (ENGSTAGE) ───────────────────────────────── */
  const liveBindings = key => [...(LIVE.get(key) || [])].filter(b => b.el && b.el.isConnected);
  /** The press: the staged state is what will be kept; "Approved ✓" is painted in this same frame and the commit runs behind it, after any commit of this piece still running. -> true when a commit was queued */
  const RUNNING = new Set();   // piece keys whose commit runs now
  const PENDING = new Map();   // piece key -> the press waiting for the running commit: the LATEST one (a press meanwhile replaces it)
  function pressCommit(ctx) {
    const { key, line, slot, d } = ctx;
    const snap = { seq: d.seq, draft: d, stage: { choice: d.choice, active: d.active, faces: cloneFaces(d.faces) } };
    COMMITS.set(key, snap);
    const job = { snap, line, slot, jobKey: ctx.jobKey };
    if (RUNNING.has(key)) PENDING.set(key, job);   // (kept right after the commit running now, from the jobs as they are then)
    else startRun(key, job);
    const E = Eng(); if (E && E.showStage && typeof E.showStage.flush === 'function') tryDo(() => E.showStage.flush());
    for (const b of liveBindings(key)) tryDo(() => b.gate());   // ("Approved ✓", now: its commit runs or waits)
    return true;
  }
  function startRun(key, job) {
    RUNNING.add(key);
    const go = () => runCommit({ key, line: job.line, slot: job.slot, snap: job.snap, jobKey: job.jobKey });
    const run = (CHAINS.get(key) || Promise.resolve()).then(go, go).catch(e => { toast(shortWhy(e && e.message ? e.message : e)); }).then(() => {   // (a commit before it that failed in any way never holds this one back)
      RUNNING.delete(key);
      const next = PENDING.get(key); PENDING.delete(key);
      if (next) { startRun(key, next); return; }
      if (COMMITS.get(key) === job.snap) COMMITS.delete(key);
      unbusy(key);   // (the chain is drained: Approve is drawn from the jobs as they are now: lit when what is staged differs from them)
    });
    CHAINS.set(key, run); run.then(() => { if (CHAINS.get(key) === run) CHAINS.delete(key); });
  }
  /* ENGSTAGE (coordinator, 30 Oct 2026: a 60-action burst left "Approved" stuck, aria-busy, with edits staged on top): a commit is never left pending. It reports each step it makes (o.step); 20 s with no step
   * (CN_SPIN_SCALE: a test's clock) ends the wait for it: Approve is given back with the staged state as it is and one short reason. The commit itself may still land later: the next commit of the piece waits for it
   * first (HUNG), so two never run at once. The stamp of an approval is pressed on the panel's own anchor beside the button (stampAt), never on the live button the person may be pressing again. */
  const WATCH_MS = 8000;
  /** Every Approve of a piece and its stamp's anchor say busy no more, and Approve is drawn again from the gate (the end of a commit, kept or not). */
  function unbusy(key) {
    try { const doc = root && root.document; if (doc) for (const n of doc.querySelectorAll('[data-engp-stamp][aria-busy], [data-engp-approve][aria-busy]')) { const p = n.closest('.rvItem'), e = p && p.querySelector('.engp[data-key]'); if (!e || e.dataset.key === key) n.removeAttribute('aria-busy'); } } catch (_) { /* drawn below */ }
    for (const b of liveBindings(key)) tryDo(() => { for (const n of [b.button(), b.stampAt()]) if (n) n.removeAttribute('aria-busy'); b.gate(); });
  }
  const HUNG = new Map();   // piece key -> the commit still running after its watchdog let go of it
  /* KEYS (stage-view's trace, 30 Oct 2026: "A fast typist's "a" or "s" can then approve or skip mid-word"): the moment a key was last typed in a words box. For 800 ms after it a key that lands on the card
   * (a rebuild took the focus from the box) goes back into the box, never to the card's own shortcuts (A approves, S stages None, the arrows move the words). */
  const TYPED = { at: 0, key: '' };
  const TYPE_MS = 800;
  /* OWNINPUT (coordinator 30 Oct 2026, the input tracer: "While an approval's seal stamp is pressed ... this document-level capture listener swallows EVERY click and keydown on the page"; "Never
   * swallow input inside the Engrave detail card's panel and work area"): what the panel tells the stamp's guard (Seal.ownInput) is its own. A press in the panel or on its Approve, the work area, a thumbnail or
   * the panel's pickers, and on the card of a panel A (it presses Approve: queued), S (it stages None) or a key typed mid-word (it goes back into the box), never dismisses the card under the stamp.
   * Escape, Next, Back, close, another card or row and the tabs are still held for the stamp. */
  const OWN_SEL = '.engp, [data-engp], [data-engp-approve], [data-engp-stamp], .backHost, [data-eng-thumb], .emPop, .cnAp';   // (the panel's Approve sits in the card's approval row, outside .engp)
  function ownInput(e) {
    const t = e && e.target; if (!t || !t.closest || e.type === 'keydown' && e.key === 'Escape') return false;
    if (t.closest(OWN_SEL)) return !(t.closest('.emPop, .cnAp') && !t.ownerDocument.querySelector('.engp'));   // (the pickers: while a panel is on the page)
    if (e.type !== 'keydown' || e.metaKey || e.ctrlKey || e.altKey) return false;
    const card = t.closest('.rvItem'), p = card && card.querySelector('.engp[data-key]'); if (!p) return false;
    const k = (e.key || '').toLowerCase();
    return (TYPED.key === p.dataset.key && Date.now() - TYPED.at < TYPE_MS) || (!e.repeat && (k === 'a' || k === 's'));
  }
  let ownInputOn = false;
  const ownInputHook = () => { if (ownInputOn) return; const S = root && root.Seal; if (S && typeof S.ownInput === 'function') { ownInputOn = true; tryDo(() => S.ownInput(ownInput)); } };
  async function runCommit(ctx) {
    const { key, line, slot, snap } = ctx, E = Eng(), S = Setup();
    let res, settled = false, last = Date.now(), timer = 0;
    try {
      { const h = HUNG.get(key); if (h) { await Promise.race([h, new Promise(r => setTimeout(r, WATCH_MS * Math.max(0.001, +(root && root.CN_SPIN_SCALE) || 1)))]); if (HUNG.get(key) === h) HUNG.delete(key); } }   // (the one let go is waited for, at most as long again)
      let work;
      if (typeof ctx.save === 'function') work = Promise.resolve().then(() => ctx.save());
      else if (!E || typeof E.commitStage !== 'function' || !S) work = Promise.resolve({ ok: false, message: 'The engraving is not loaded yet.' });
      else {
        const vm = S.read(line, { slot: slot || undefined, jobs: (E.jobsOf ? E.jobsOf(line) : []) || [] });
        const patch = vm ? S.patchOf(vm, { mode: snap.stage.choice, faces: snap.stage.faces }) : {};
        const button = () => (liveBindings(key).map(b => b.stampAt()).find(Boolean)) || null;
        work = Promise.resolve().then(() => E.commitStage(line, { choice: snap.stage.choice, slot, faces: snap.stage.faces, patch }, Object.assign({ button, step: () => { last = Date.now(); }, overtaken: () => PENDING.has(key) }, ctx.keepOnly ? { approve: false } : {})));   // (overtaken: a newer press waits: it approves, this one only keeps)
      }
      last = Date.now();
      const scale = Math.max(0.001, +(root && root.CN_SPIN_SCALE) || 1);
      const jobSig = () => tryDo(() => (((E && E.jobsOf ? E.jobsOf(line) : []) || []).map(j => [j.key, j.state, j.fit ? 1 : 0, +j.approvedAt || 0, j.approvalPreparing ? 1 : 0, j.stamping ? 1 : 0, j.backSaving ? 1 : 0, (j.backs || []).length].join(':')).join('|')), '');
      let seen = jobSig();   // (progress: a step of the commit, or any change in the piece's jobs)
      const dog = new Promise(r => { const tick = () => { if (settled) return; { const now = jobSig(); if (now !== seen) { seen = now; last = Date.now(); } } if (Date.now() - last >= WATCH_MS * scale) { r({ ok: false, hung: true, message: 'Not approved: it took too long. Press Approve to try again.' }); return; } timer = setTimeout(tick, Math.min(1000, WATCH_MS * scale / 4)); }; timer = setTimeout(tick, Math.min(1000, WATCH_MS * scale / 4)); });
      res = await Promise.race([work.then(v => v || { ok: false }, e => ({ ok: false, message: str(e && e.message ? e.message : e) })), dog]);
      settled = true; clearTimeout(timer);
      if (res && res.hung) { const h = work.catch(() => {}).then(() => { if (HUNG.get(key) === h) HUNG.delete(key); for (const b of liveBindings(key)) tryDo(() => b.draw()); unbusy(key); }); HUNG.set(key, h); }
    } catch (e) { res = { ok: false, message: str(e && e.message ? e.message : e) }; }
    finally {
      settled = true; if (COMMITS.get(key) === snap && !PENDING.has(key)) COMMITS.delete(key);   // (the "Approved ✓" of this press ends here, whatever happened; a press waiting after it keeps its own)
      unbusy(key);   // (nothing of this press says busy any more: its stamp's anchor and the button, then Approve drawn again)
    }
    const d = DRAFTS.get(key), clean = !!(res && res.ok) && d === snap.draft && d.seq === snap.seq && !COMMITS.has(key) && !PENDING.has(key);
    if (clean) { d.touched = false; d.dirty = false; d.sig = ''; for (const f of FACES) d.faces[f].pos = null; }
    else if (res && res.ok && d === snap.draft) { for (const f of FACES) if (posOk(d.faces[f].pos) && samePos(d.faces[f].pos, snap.stage.faces[f].pos)) d.faces[f].pos = null; }   // (the place this commit kept is kept: only what differs from it stays staged)   // (what was staged is what is kept now: nothing is staged; the draft starts again from what is kept at the next look, on the face shown)
    else if (!(res && res.ok) && !(res && res.cancelled)) {
      if (!(res && res.said)) toast(shortWhy(res && res.message));   // (one short toast; an approval that said why itself is not said twice)
      const m = str(res && res.message).trim() || 'Not approved: try again.', dn = DRAFTS.get(key); if (dn) dn.why = /^Not approved/i.test(m) ? m : 'Not approved: ' + m.replace(/^Not saved:\s*/i, '');   // (APPROVEFIX: the card says it in words too, under Approve, until the next change or press)
    }
    // (the pictures: what is kept, or what is staged on top of it now: never the state of the press. A binding draws from the draft as it is NOW: the sequence guard)
    const live = liveBindings(key);
    for (const b of live) tryDo(() => b.draw());
    if (clean || !DRAFTS.has(key)) { if (E && typeof E.showStage === 'function') tryDo(() => E.showStage(ctx.jobKey || (live[0] && live[0].jobKey) || '', null)); for (const b of live) tryDo(() => b.paintLegacy()); }
    else for (const b of live.slice(0, 1)) tryDo(() => b.paint('commit'));
    return res;
  }

  /* ── wiring ────────────────────────────────────────────────────────────────── */
  /** Wire the panel inside `el` for the piece of `line`. */
  function bind(el, line, opts) {
    ensureCss(el.ownerDocument); ownInputHook();
    let dead = false, gateCard = null;
    const doc = el.ownerDocument, slot = opts.slot !== undefined ? opts.slot : opts.job ? slotOfJob(opts.job) : null, key = pieceKeyOf(line, slot);
    const jobs = () => (opts.jobs ? opts.jobs() : (Eng() && Eng().jobsOf ? Eng().jobsOf(line) : [])) || [];
    const me = { el, key, draw: null, jobKey: str(opts.job && opts.job.key) };
    sweep(key); watch(doc);   // (another piece was open before: its draft ends here)
    const track = () => { let set = LIVE.get(key); if (!set) LIVE.set(key, set = new Set()); for (const b of [...set]) if (b !== me && b.el && !b.el.isConnected) set.delete(b); set.add(me); };
    const untrack = () => { const set = LIVE.get(key); if (set) { set.delete(me); if (!set.size) LIVE.delete(key); } };
    let adopted = false;   // (this binding took its card's face once: two panels of one piece (a card and a test's slot) never take the face from each other at every look)
    const model = () => {
      const S = Setup(), vm = S && S.read ? S.read(line, { slot: slot || undefined, jobs: jobs() }) : null; if (!vm) return null;
      const d = draftOf(vm, key, opts.face), shownKey = str(opts.job && opts.job.key);
      if (!adopted) { adopted = true; if (shownKey && d.shown !== shownKey) { const first = !d.shown; d.shown = shownKey; if (opts.face && !first && facesFor(line).includes(opts.face)) d.active = opts.face; } }   // (the card was drawn for another job of this piece: its face is the one shown)
      if (!d.jobKey) d.jobKey = shownKey;
      if (!facesFor(line).includes(d.active)) d.active = facesFor(line)[0] || d.active;
      if (!d.flush) d.flush = () => discard(d);
      return { vm, d };
    };
    /** STAGEALL / ENGSTAGE: the draft ended with no Approve (the card was left): it is DISCARDED; the work area and the thumbnails stop drawing it. Nothing is saved. */
    const discard = d => {
      const E = Eng(); if (!E) return;
      if (typeof E.showStage === 'function') { tryDo(() => E.showStage(d.jobKey || me.jobKey, null)); return; }
      for (const k of d.drafted || []) tryDo(() => { if (typeof opts.onDraft === 'function') opts.onDraft(k, null); else if (E.draftFit) E.draftFit(k, null); });
      if (E.stageFace && me.jobKey) tryDo(() => E.stageFace(me.jobKey, null, null));
    };
    const focusState = () => { const a = doc.activeElement; if (!a || !el.contains(a) || !a.dataset || !a.dataset.engp) return null; return { engp: a.dataset.engp, face: a.dataset.face || '', choice: a.dataset.choice || '', start: a.selectionStart, end: a.selectionEnd }; };
    const restoreFocus = want => {
      if (!want || dead) return;
      const sel = `[data-engp="${want.engp}"]${want.face ? `[data-face="${want.face}"]` : ''}${want.choice ? `[data-choice="${want.choice}"]` : ''}`;
      let t = el.querySelector(sel), moved = false;
      if (!t && (want.engp === 'words' || want.engp === 'font')) { t = el.querySelector(`[data-engp="${want.engp}"]`); moved = !!t; }   // (the face changed under the box in focus: the new face's box takes it, the caret at its end)
      if (t && !t.disabled && doc.activeElement !== t) { try { t.focus({ preventScroll: true }); if (t.setSelectionRange) { if (moved) { const n = str(t.value).length; t.setSelectionRange(n, n); } else if (want.start != null) t.setSelectionRange(want.start, want.end); } } catch (_) { /* no caret to give back */ } }
    };

    /* ── the pictures: ONE call (Engrave.showStage), else the old previews ── */
    const showFaceOf = () => { if (typeof opts.show === 'function') return opts.show; return null; };
    /** ENGWORK (legacy): the work area's draft of a face that has a job (Engrave.draftFit), what the thumbnail shows (CharmNestEngravePreview.update), a face staged on with no job (Engrave.stageFace). */
    const draftOut = (vm, d, face) => {
      try {
        const job = faceJobOf(vm, face, slot); if (!job || !job.key) return;
        const on = facesOf(d.choice).includes(face), x = d.faces[face] || { text: '', font: '' }, staged = dirtyOf(vm, d, key) ? { mode: d.choice, face, text: on ? x.text : '', fontWord: x.font || '' } : null;
        if (staged) (d.drafted || (d.drafted = new Set())).add(job.key);
        if (typeof opts.onDraft === 'function') opts.onDraft(job.key, staged); else { const E = Eng(); if (E && E.draftFit) E.draftFit(job.key, staged); }
      } catch (_) { /* the work area keeps what it drew */ }
    };
    const stagedOut = (vm, d) => {
      const E = Eng(), cj = opts.job; if (!E || typeof E.stageFace !== 'function' || !cj || !cj.key) return false;
      const f = d.active, on = !dead && DRAFTS.get(key) === d && facesOf(d.choice).includes(f) && faceJobOf(vm, f, slot) === null && !!vm.rows.length;
      if (!on) { tryDo(() => E.stageFace(cj.key, null, null)); return false; }
      const x = d.faces[f] || { text: '', font: '' }, r = tryDo(() => E.stageFace(cj.key, f, { text: x.text, fontWord: x.font || '' }), null);
      return !!(r && r.ok);
    };
    const thumbOut = (vm, d, face) => {
      const P = Prev(); if (!P || !P.update) return;
      const host = opts.thumb ? tryDo(() => opts.thumb(face), null) : tryDo(() => (opts.scope || doc).querySelector(`.engFace[data-face="${face}"] .engThumb`), null);
      if (!host) return;
      const on = facesOf(d.choice).includes(face), x = d.faces[face] || { text: '', font: '' }, job = faceJobOf(vm, face, slot);
      const part = { text: on ? linesOf(x.text).join('\n') : '', face };
      try { const id = P.fontIdOf ? P.fontIdOf(vm.line, face, x.font) : null; if (id) part.fontId = id; } catch (_) { /* the preview keeps its font */ }
      const pos = positionOf(job); if (pos) part.position = { x: pos.x, y: pos.y, size: pos.size, angle: pos.angle };
      tryDo(() => P.update(host, part));
    };
    const legacy = (vm, d, kind) => { for (const f of FACES) { draftOut(vm, d, f); thumbOut(vm, d, f); } stagedOut(vm, d); if (kind === 'active' || kind === 'choice') { const sh = showFaceOf(); if (sh) tryDo(() => sh(d.active)); } };
    /** Every staged change ends here: the thumbnails and the work area show the stage at once (Engrave.showStage coalesces to one paint a frame; the last state wins). */
    const paint = (kind, m) => {
      m = m || (dead ? null : tryDo(() => model(), null)); if (!m) return;
      const { vm, d } = m, E = Eng(), st = stageFrom(d, line);
      if (typeof opts.onStage === 'function') tryDo(() => opts.onStage(st, kind));
      if (E && typeof E.showStage === 'function') tryDo(() => E.showStage(opts.job || d.jobKey, st, typeof opts.thumb === 'function' ? { thumb: opts.thumb } : undefined));   // (a host's own thumbnails: painted by showStage too)   // (the card's own job, every change, approved pieces too: the thumbnails and the work area are stage-view's)
      else legacy(vm, d, kind);
    };
    const paintLegacy = () => { const E = Eng(); if (E && typeof E.showStage === 'function') return; const m = dead ? null : tryDo(() => model(), null); if (m) legacy(m.vm, m.d, ''); };

    /* ── Approve: the card's one green Approve is the panel's ── */
    const GATE = '[data-a="approve"],[data-a="approveBoth"],[data-a="approveFaces"]';
    const hostOf = () => { const card = el.closest ? el.closest('.rvItem') : null; return card ? card.querySelector('.pvApproval') : null; };
    const button = () => { const h = hostOf(); const b = h && h.querySelector('[data-engp-approve]'); return b && b.isConnected ? b : null; };
    const stampAt = () => { const h = hostOf(); const a = h && h.querySelector('[data-engp-stamp]'); return a && a.isConnected ? a : null; };
    /** The Approve of the card, drawn from the gate: green Approve / off with the gold pulse / calm "Approved" / "Approved ✓" the moment it was pressed. The card's own approval buttons make way for it. */
    const drawGate = (vm, d) => {
      const g = gateOf(vm, d, key), p = el.querySelector('.engp'), scope = opts.scope || el.parentElement;
      d.dirty = !!g.dirty;
      if (p) {
        if (g.dirty) { p.dataset.dirty = '1'; p.dataset.staged = '1'; } else { delete p.dataset.dirty; delete p.dataset.staged; }
        if (g.st === 'need') p.dataset.need = g.faces.join(' '); else delete p.dataset.need;
      }
      if (scope && scope.dataset) { if (g.st === 'need') scope.dataset.engpNeed = g.faces.join(' '); else delete scope.dataset.engpNeed; }
      const host = hostOf(); if (!host) return g;
      for (const n of host.querySelectorAll('[data-a="approve"]:not([data-engp-approve]),[data-a="approved"]:not([data-engp-approve]),[data-a="approveBoth"],[data-a="approveFaces"]')) { const w = n.parentElement && n.parentElement.classList.contains('egApproveWrap') && n.parentElement.children.length === 1 ? n.parentElement : n; w.remove(); }
      for (const n of host.querySelectorAll('.egWrittenNote')) n.hidden = g.st !== 'approved';   // ("Saved to sheet": said only while nothing is staged)
      { const why = g.st === 'approve' || g.st === 'need' ? str(d.why) : ''; let n = host.querySelector('.egApproveWhy[data-engp]') || (why ? host.querySelector('.egApproveWhy') : null);   // (the approval's own line, when it wrote one, is the same line: never two)
        if (!why) { if (n) n.remove(); } else { if (n) n.dataset.engp = '1'; if (!n) { n = doc.createElement('div'); n.className = 'egApproveWhy'; n.dataset.engp = '1'; n.setAttribute('role', 'status'); host.appendChild(n); } if (n.textContent !== why) n.textContent = why; } }
      let b = host.querySelector('[data-engp-approve]');
      if (!b) { const w = doc.createElement('span'); w.className = 'egApproveWrap engpApproveWrap'; b = doc.createElement('button'); b.type = 'button'; b.dataset.engpApprove = '1'; w.appendChild(b); host.insertBefore(w, host.firstChild); }
      if (b.parentElement && !b.parentElement.querySelector(':scope > [data-engp-stamp]')) { const a = doc.createElement('span'); a.className = 'engpStampAt'; a.dataset.engpStamp = '1'; a.setAttribute('aria-hidden', 'true'); b.after(a); }   // (the approval's stamp is pressed here, never on the button itself)
      if (b.hasAttribute('aria-busy')) b.removeAttribute('aria-busy');
      { const a = b.parentElement && b.parentElement.querySelector(':scope > [data-engp-stamp]'); if (a && a.hasAttribute('aria-busy') && !COMMITS.has(key) && !HUNG.has(key)) a.removeAttribute('aria-busy'); }   // (the stamp's anchor says busy only while an approval of this piece runs)
      const calm = g.st === 'approved' || g.st === 'ok', want = g.st === 'ok' ? 'Approved ✓' : calm ? 'Approved' : 'Approve';
      b.className = 'btn sage sm egApproveButton engpApprove' + (calm ? ' egApproved' : '');
      b.dataset.a = calm ? 'approved' : 'approve'; b.dataset.gate = g.st;
      b.disabled = g.st !== 'approve'; b.setAttribute('aria-disabled', String(b.disabled));
      b.title = g.st === 'approve' ? 'Approve this engraving' : g.st === 'blocked' ? g.why : calm ? 'Approved: change anything and press Approve again' : '';
      if (b.textContent !== want) b.innerHTML = g.st === 'ok' ? 'Approved<span class="engpTick" aria-hidden="true">✓</span>' : want;
      return g;
    };
    const gate = () => { const m = dead ? null : tryDo(() => model(), null); if (m) drawGate(m.vm, m.d); };
    /** A press of Approve (the button, the card's other approval buttons, the A key). */
    const press = () => {
      if (dead) return false; const m = tryDo(() => model(), null); if (!m) return false; const { vm, d } = m;
      const g = gateOf(vm, d, key);
      if (g.st !== 'approve') { if (g.st === 'need') { const p = el.querySelector('.engp'); if (p) { delete p.dataset.need; void p.offsetWidth; p.dataset.need = g.faces.join(' '); } } return false; }   // (pressed while it is kept with nothing new: ignored)
      d.why = ''; return pressCommit({ key, line, slot, d, jobKey: me.jobKey || d.jobKey });
    };
    const stop = e => { e.preventDefault(); e.stopImmediatePropagation(); press(); };
    const blockClick = e => { const b = e.target && e.target.closest ? e.target.closest(GATE) : null; if (b && !b.disabled && b.closest('.rvItem') === gateCard) stop(e); };
    const blockKey = e => {
      const k = e.key ? e.key.toLowerCase() : '', t = e.target, plain = !e.metaKey && !e.ctrlKey && !e.altKey && t && t.closest && !t.closest('input,textarea,select,[contenteditable]');
      if (!plain) return;
      // (KEYS: a key typed while the words were being typed, that landed on the card because a rebuild moved the focus: it goes back into the words box, where it was meant to go)
      if (TYPED.key === key && Date.now() - TYPED.at < TYPE_MS) { if (retype(e)) { e.preventDefault(); e.stopImmediatePropagation(); } return; }
      const onCard = t === gateCard || t === doc.body || t === doc.documentElement;   // (the card itself in focus, or nothing: never a key meant for another control)
      if (k === 'a' && !e.repeat) { if (onCard) stop(e); else { e.preventDefault(); e.stopImmediatePropagation(); } }
      // NONEPRESS (Paul, 29 Oct 2026: "Remove this "No Engraving" button from the UI"): the S key stages None (nothing is kept until Approve)
      else if (k === 's' && !e.repeat) { e.preventDefault(); e.stopImmediatePropagation(); if (onCard) stageNone(); }
    };
    /** KEYS: a key that landed on the card mid-typing, typed into the words box: the box takes the focus back (the caret where it was) and the key acts there (a letter, Backspace, Delete, the arrows). -> true when taken */
    const retype = e => {
      const ta = el.querySelector('.engpWords'); if (!ta || ta.disabled || dead) return false;
      const m = tryDo(() => model(), null), f = m && m.d.focus, k = e.key || '';
      tryDo(() => { ta.focus({ preventScroll: true }); if (f && f.engp === 'words' && f.start != null && ta.setSelectionRange) ta.setSelectionRange(Math.min(f.start, ta.value.length), Math.min(f.end, ta.value.length)); });
      const a = ta.selectionStart, b = ta.selectionEnd;
      if (k.length === 1) ta.setRangeText(k, a, b, 'end');
      else if (k === 'Backspace') { if (a !== b) ta.setRangeText('', a, b, 'end'); else if (a > 0) ta.setRangeText('', a - 1, a, 'end'); else return true; }
      else if (k === 'Delete') { if (a !== b) ta.setRangeText('', a, b, 'end'); else if (a < ta.value.length) ta.setRangeText('', a, a + 1, 'end'); else return true; }
      else if (k === 'Enter') ta.setRangeText('\n', a, b, 'end');
      else if (k === 'ArrowLeft' || k === 'ArrowRight') { const n = Math.max(0, Math.min(ta.value.length, (k === 'ArrowLeft' ? Math.min(a, b) - (a === b ? 1 : 0) : Math.max(a, b) + (a === b ? 1 : 0)))); ta.setSelectionRange(n, n); return true; }
      else return ['ArrowUp', 'ArrowDown', 'Escape', 'Tab'].includes(k) ? k !== 'Tab' : false;   // (the other arrows and Escape are the box's own; Tab moves on as ever)
      ta.dispatchEvent(new Event('input', { bubbles: true }));
      return true;
    };
    const stageNone = () => { if (dead) return false; const m = tryDo(() => model(), null); if (!m || !choicesFor(line).some(([x]) => x === 'none')) return false; pressChoice(m, 'none'); return true; };
    const hookCard = () => {
      const card = el.closest ? el.closest('.rvItem') : null; if (!card || card === gateCard) return;
      if (gateCard) { gateCard.removeEventListener('click', blockClick, true); gateCard.removeEventListener('keydown', blockKey, true); }
      card.addEventListener('click', blockClick, true); card.addEventListener('keydown', blockKey, true); gateCard = card;
    };

    /* ── drawing ── */
    const syncPill = d => {
      const box = el.querySelector('.engpChoice'); if (!box) return;
      const choices = choicesFor(line), i = choices.findIndex(([c]) => c === d.choice), pill = box.querySelector('.engpPill');
      if (pill) pill.hidden = i < 0;
      if (i >= 0 && str(box.style.getPropertyValue('--engp-i')).trim() !== String(i)) { if (pill) void pill.offsetWidth; box.style.setProperty('--engp-i', String(i)); }
      d.pillAt = i;
      for (const b of box.querySelectorAll('.engpSeg[data-engp="choice"]')) b.setAttribute('aria-pressed', String(b.dataset.choice === d.choice));
      const p = el.querySelector('.engp'); if (p) { p.dataset.choice = d.choice; p.dataset.mode = d.choice; }
    };
    /** "Front & Back" when it fits the column, else "Both" (SPEC: the four options fit the 382 px column at 40 px). */
    const fitLabels = () => {
      const b = el.querySelector('.engpSeg[data-choice="both"]'); if (!b || !b.isConnected || !b.clientWidth) return;
      try {
        const cs = doc.defaultView.getComputedStyle(b), room = b.clientWidth - (parseFloat(cs.paddingLeft) || 0) - (parseFloat(cs.paddingRight) || 0), g = (fitLabels.cv || (fitLabels.cv = doc.createElement('canvas'))).getContext('2d');
        g.font = `${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`; const want = g.measureText('Front & Back').width <= room - 2 ? 'Front & Back' : 'Both';
        const t = [...b.childNodes].find(n => n.nodeType === 3); if (t) { if (t.nodeValue !== want) t.nodeValue = want; } else b.insertBefore(doc.createTextNode(want), b.firstChild);
      } catch (_) { /* the words it has */ }
    };
    const draw = () => {
      if (dead) return;
      const m = model(), fs = focusState();
      if (!m || !choicesFor(line).length) { untrack(); el.innerHTML = ''; el.hidden = true; if (opts.onVisible) tryDo(() => opts.onVisible(false)); return; }
      track(); el.hidden = false; if (opts.onVisible) tryDo(() => opts.onVisible(true));
      const { vm, d } = m, g = gateOf(vm, d, key);
      el.innerHTML = html(vm, d, { faceSwitch: !!opts.faceSwitch, gate: g });
      syncPill(d);   // (the pill slides from where it was drawn last to the choice: one transform, 180 ms)
      fitLabels(); { const w = doc.defaultView; if (w && w.requestAnimationFrame) w.requestAnimationFrame(() => { if (!dead) fitLabels(); }); }
      const ta = el.querySelector('.engpWords'); if (ta) growBox(ta);
      { const fsel = el.querySelector('.engpFont'); if (fsel && fsel.selectedOptions[0] && fsel.selectedOptions[0].style.fontFamily) fsel.style.fontFamily = fsel.selectedOptions[0].style.fontFamily; }
      { const eb = el.querySelector('[data-emoji]'), tb = el.querySelector('.engpWords'), CE = doc.defaultView && doc.defaultView.CNEmojiPicker;   // (Paul's own Emoji button: it types into the box, so the panel sees an ordinary input)
        if (eb && !(CE && tb && tryDo(() => CE.attach({ textarea: tb, button: eb, key: 'engp:' + key + ':' + d.active }), false))) eb.remove();
        const ab = el.querySelector('[data-alpha]'), AP = doc.defaultView && doc.defaultView.CNAlphabetPicker;   // (ALPHAFIND, Paul 28 Oct 2026: other alphabets, beside Emoji)
        if (ab && !(AP && tb && tryDo(() => AP.attach({ textarea: tb, button: ab, key: 'engpa:' + key + ':' + d.active }), false))) ab.remove(); }
      hiDecorate(el, vm, d, slot);
      hookCard();
      drawGate(vm, d);
      const P = Prev(); if (P && P.warm && !draw.warmed) { draw.warmed = true; tryDo(() => P.warm()); }
      const ae = doc.activeElement, typing = TYPED.key === key && Date.now() - TYPED.at < 4 * TYPE_MS;
      const lost = ae => !ae || ae === doc.body || ae === doc.documentElement || !ae.isConnected || (typing && ae.matches && ae.matches('.rvItem')) || (ae.closest && ae.closest('.engp') && ae.closest('.engp') !== el.querySelector('.engp') && ae.closest('.engp').dataset.key === key);   // (nothing, a card, or the same piece's panel of a card being replaced)
      const want = fs || (d.focus && lost(ae) ? d.focus : null);
      restoreFocus(want);
      if (want) {   // (a card is built before it is put in the page, and the tab focuses a card it drew 30 ms later: the box takes the focus back once it is there)
        const again = () => { if (dead || !el.isConnected) return; const a2 = doc.activeElement; if (a2 && el.contains(a2)) return; if (lost(a2)) restoreFocus(want); };   // (never taken from a control the person moved to)
        Promise.resolve().then(again); const w = doc.defaultView; if (w && w.requestAnimationFrame) w.requestAnimationFrame(again); setTimeout(again, 45);
      }
    };
    me.draw = draw; me.gate = gate; me.button = button; me.stampAt = stampAt; me.paint = kind => paint(kind); me.paintLegacy = paintLegacy;
    const redrawLive = () => { const set = LIVE.get(key); for (const b of [...(set || [])]) { if (b !== me && b.el && !b.el.isConnected) { set.delete(b); continue; } tryDo(() => b.draw()); } };
    const changed = (kind, face) => { try { if (typeof opts.onChange === 'function') opts.onChange({ face, kind, key }); } catch (_) { /* the host's own */ } };

    /* ── the staged changes ── */
    const bump = (d, touched) => { d.seq = (d.seq || 0) + 1; d.error = ''; d.why = ''; if (touched) d.touched = true; };
    /** The active face (a thumbnail, the face switch, Engrave.showFace landing). Any face the piece has: one that is not engraved shows plain, and its words box waits empty. -> true when it moved */
    const setActive = (face, from) => {
      const m = model(); if (!m || !facesFor(line).includes(face)) return false;
      const { d } = m; if (d.active === face) { const cur = el.querySelector('.engp'); if (cur && cur.dataset.active === face) { if (from === 'thumb') paint('active', m); return true; } }
      d.active = face; bump(d);
      if (from === 'thumb') d.focus = { engp: 'words', face, choice: '', start: null, end: null }; else d.focus = null;   // (a thumbnail pressed: the words box of that face takes the caret, so typing goes there at once)
      draw();
      if (from === 'thumb') { const ta = el.querySelector('.engpWords'); if (ta && !ta.disabled && doc.activeElement !== ta) tryDo(() => { ta.focus({ preventScroll: true }); const n = str(ta.value).length; ta.setSelectionRange(n, n); }); }
      paint('active', m);
      try { if (typeof opts.onFace === 'function') opts.onFace(face, from); } catch (_) { /* the host's own */ }
      changed('active', face);
      return true;
    };
    /** A press of one of the four options: staged at once (the pill slides; the faces it engraves get their words; Front / Back is shown). */
    const pressChoice = (m, choice) => {
      const { vm, d } = m; if (!choicesFor(line).some(([c]) => c === choice)) return;
      if (d.choice === choice && (choice === 'none' || choice === 'both' || d.active === choice)) return;
      setChoice(vm, d, choice);
      if (choice === 'front' || choice === 'back') d.active = choice;
      bump(d, true); draw(); paint('choice', m); changed('choice', d.active);
    };
    const click = e => {
      const b = e.target && e.target.closest && e.target.closest('[data-engp]'); if (!b || !el.contains(b) || b.disabled || dead) return;
      const m = model(); if (!m) return; const a = b.dataset.engp;
      if (a === 'face') setActive(b.dataset.face, 'switch');
      else if (a === 'choice' || a === 'mode') pressChoice(m, b.dataset.choice || b.dataset.mode);
    };
    const input = e => {
      const t = e.target; if (!t || !t.dataset || !t.dataset.engp || dead) return;
      const m = model(); if (!m) return; const { vm, d } = m, f = FACES.includes(d.active) ? d.active : t.dataset.face; if (!f) return;   // (the face the keystroke is for is the draft's active face, read now: the box on screen is always that face's)
      const x = d.faces[f] || (d.faces[f] = { text: '', font: '', pos: null });
      if (t.dataset.engp === 'words') {
        if (e.type === 'change' || !t.isConnected) return;   // (every letter arrives as an input; the box's own "change" carries nothing new)
        x.text = t.value; d.copied[f] = false; growBox(t);
        if (str(t.value).trim() && !facesOf(d.choice).includes(f)) d.choice = withFace(d.choice, f);   // (typing on a face that is not engraved engraves it: the pill slides to the new choice)
      } else if (t.dataset.engp === 'font') { if (x.font === t.value || !t.isConnected) return; x.font = t.value; if (t.tagName === 'SELECT') t.style.fontFamily = (t.selectedOptions[0] && t.selectedOptions[0].style.fontFamily) || ''; }
      else return;
      bump(d, true);
      if (t.selectionStart != null) d.focus = { engp: t.dataset.engp, face: f, choice: '', start: t.selectionStart, end: t.selectionEnd };
      syncPill(d);
      hiDecorate(el, vm, d, slot);
      drawGate(vm, d);   // (Approve lights up with the first letter, and goes calm again when the words are as kept)
      paint(t.dataset.engp, m); changed(t.dataset.engp, f);
    };
    const key_ = e => {
      const t = e.target; if (!t || !t.dataset || !t.dataset.engp) return;
      if (t.dataset.engp === 'words') { TYPED.at = Date.now(); TYPED.key = key; }   // (KEYS)
      if (t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.tagName === 'INPUT') e.stopPropagation();   // (the card's own keys, S and A, are for the card, not for words)
    };
    const focusin = e => { const t = e.target; if (!t || !t.dataset || !t.dataset.engp || dead) return; const m = model(); if (m) m.d.focus = { engp: t.dataset.engp, face: t.dataset.face || '', choice: t.dataset.choice || '', start: t.selectionStart, end: t.selectionEnd }; };
    const focusout = e => {
      if (!e.relatedTarget) return;
      if (e.relatedTarget === (el.closest ? el.closest('.rvItem') : null) && TYPED.key === key && Date.now() - TYPED.at < 4 * TYPE_MS) return;   // (KEYS: the tab took the focus to the card mid-typing: the box keeps its claim, a rebuild gives it back)
      const m = model(); if (m) m.d.focus = null;
    };
    const ev = [['click', click], ['input', input], ['change', input], ['keydown', key_], ['focusin', focusin], ['focusout', focusout]];
    for (const [t, f] of ev) el.addEventListener(t, f);
    /** A drag in the work area: the face's staged place (pos = { charm: [x, y], centre? } or null). */
    const stagePosHere = (face, pos) => {
      const m = dead ? null : tryDo(() => model(), null); if (!m || !FACES.includes(face)) return false;
      const { vm, d } = m, x = d.faces[face] || (d.faces[face] = { text: '', font: '', pos: null }), p = posOf(pos);
      if (samePos(x.pos, p) && !!x.pos === !!p) return true;
      x.pos = p; bump(d, true); drawGate(vm, d); paint('pos', m); changed('position', face); return true;
    };
    me.stagePos = stagePosHere; me.setActive = setActive;   // (ENGSTAGE: Engrave.showFace gives the active face to the panel: setActive below)
    /* ROTBAR: a turn of the WHOLE piece made with the work area's degree box is the job's at once; `moved` draws the thumbnails again. It changes no word, font or choice. */
    const beforeMove = job => !!(job && job.key && model());
    const moved = (job, kind) => { const m = model(); if (!m || !job || !job.key) return false; drawGate(m.vm, m.d); paint(kind || 'position', m); changed(kind || 'position', m.d.active); return true; };
    /** Keep what is staged now WITHOUT approving it (a test, a host that closes the card itself): the faces, their words and fonts (Engrave.commitStage, approve: false), on this piece's chain. -> Promise<{ ok, ... }> */
    const commitNow = () => {
      const m = dead ? null : model(); if (!m) return Promise.resolve({ ok: false }); const { vm, d } = m;
      if (!dirtyOf(vm, d, key)) return Promise.resolve({ ok: true, same: true });
      const snap = { seq: d.seq, draft: d, stage: { choice: d.choice, active: d.active, faces: cloneFaces(d.faces) } };
      const save = typeof opts.save === 'function' ? (() => { const patch = Setup().patchOf(vm, viewOf(d)); return () => opts.save(patch); })() : null;   // (a host's own save(patch): a test)
      const go = () => runCommit({ key, line, slot, snap, jobKey: me.jobKey || d.jobKey, keepOnly: true, save }), run = (CHAINS.get(key) || Promise.resolve()).then(go, go);
      CHAINS.set(key, run.catch(() => {})); return run;
    };
    return { draw, refresh: draw, setFace: f => setActive(f, 'host'), setActive, beforeMove, moved, commit: commitNow, press, stageNone, stagePos: stagePosHere, paint: k => paint(k || 'refresh'), key,
      destroy() { dead = true; untrack(); sweepSoon(); for (const [t, f] of ev) el.removeEventListener(t, f); if (gateCard) { gateCard.removeEventListener('click', blockClick, true); gateCard.removeEventListener('keydown', blockKey, true); } },
      paintAll: () => paint('refresh') };
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

  /** ENGSTAGE (stage-view): the active face of a piece, given from outside (Engrave.showFace): every live panel of the piece takes it as a press of the host would (its draft, the thumbnails' mark and the
   *  work area follow through showStage). -> true when a panel took it (false: no panel of the piece on the page) */
  function setActive(key, face) { let took = false; for (const b of liveBindings(str(key))) if (b.setActive && tryDo(() => b.setActive(face, 'host'), false)) took = true; return took; }
  /** A drag in the work area reports the face's staged place here (ENGSTAGE seam: pos = { charm: [x, y], centre: [x, y] } or null). -> true when a draft took it */
  function stagePos(key, face, pos) {
    for (const b of liveBindings(str(key))) if (b.stagePos) return !!tryDo(() => b.stagePos(face, pos), false);
    const d = DRAFTS.get(str(key)); if (!d || !FACES.includes(face)) return false;
    d.faces[face].pos = posOf(pos); d.seq = (d.seq || 0) + 1; d.touched = true; return true;
  }
  /** What a piece has staged, for a test or a host: { choice, active, dirty, gate, seq, committing } or null. */
  /** Is a commit of a piece running or waiting? (with or without a draft on the page) */
  const busyOf = key => ({ running: RUNNING.has(str(key)), pending: PENDING.has(str(key)), committing: COMMITS.has(str(key)) });
  function stateOf(key) {
    const d = DRAFTS.get(str(key)); if (!d) return null;
    const S = Setup(), vm = d.line && S ? tryDo(() => S.read(d.line, { slot: (str(key).split('#')[1]) || undefined }), null) : null, g = vm ? gateOf(vm, d, str(key)) : null;
    return { choice: d.choice, active: d.active, seq: d.seq, dirty: !!(g && g.dirty), gate: g ? g.st : '', committing: COMMITS.has(str(key)), running: RUNNING.has(str(key)), pending: PENDING.has(str(key)), faces: cloneFaces(d.faces) };
  }

  /** A line's piece name, as the order window names it. */
  function lineName(x) {
    const O = dep('CharmNestOrders', './charm-nest-orders.js'), set = O && O.setDesignsOf ? tryDo(() => O.setDesignsOf(x && x.spec), null) : null;
    return str(set && set.length ? set.join(' + ') : (x && x.spec && x.spec.designSku) || (x && x.line && (x.line.sku || x.line.title)) || '').replace(/_+/g, ' ').replace(/\s+/g, ' ').trim().slice(0, set && set.length ? 80 : 32);
  }

  /** The slot of the Engrave detail card: the Front / Back thumbnails (CharmNestEngraveCard.mountPiece) above the panel. ENGSTAGE: a press on a thumbnail makes that face the ACTIVE face (the words box edits it, the
   *  work area turns to it: Engrave.showStage); until the page has showStage, Engrave.showFace(job, face, { animate: true }) turns it, as LIVEWORDS did. */
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
      const staged = () => !!(Eng() && typeof Eng().showStage === 'function');
      const hasShow = () => { const E = Eng(); return !!(E && typeof E.showFace === 'function'); };
      /** (legacy, no showStage) the tab shows the card of the job of face `f` of this piece, when that face has a job of its own that is not the one on screen. */
      const jobNav = f => {
        if (staged()) return;   // (ENGSTAGE: the work area draws any face of the piece in this card: no other card is opened)
        const E = Eng(), other = E && E.jobsOf ? (E.jobsOf(line) || []).find(j => (slotOfJob(j) || null) === (slotOfJob(job) || null) && F && F.faceOf(j) === f) : null;
        if (!other || other.key === job.key || ['none', 'skipped'].includes(other.state)) return;   // (a face taken away has no card of its own: the work area shows it plain here)
        { const st = stage(); if (st && !facesOf(st.choice).includes(f)) return; }   // (a face not engraved in the stage: shown plain in this card)
        if (typeof o.refocus === 'function') tryDo(() => o.refocus(other));
        else if (['approved', 'written', 'skipped'].includes(other.state) && root.EngraveLink && root.EngraveLink.open) tryDo(() => { Promise.resolve(root.EngraveLink.open({ piece: other.key, face: f })).catch(() => {}); });
        else if (E.restoreView && E.render) tryDo(() => { E.restoreView({ focus: other.key }); E.render(); });
      };
      /** (legacy, no showStage) the work area shows `f`: Engrave.showFace turns it and, when the turn lands, gives the panel that face (setFace below). */
      const navigate = f => {
        if (staged()) return;
        const E = Eng(), cur = tryDo(() => ((E && E.jobsOf ? E.jobsOf(line) : []) || []).find(j => j && j.key === job.key), null) || job;
        if (hasShow()) { tryDo(() => E.showFace(cur, f, { animate: true })); return; }
        if (typeof o.onView === 'function') tryDo(() => o.onView(f));
        jobNav(f);
      };
      /** A press on a thumbnail: that face is the active face at once (the words box, the font, the work area). */
      const pick = f => { if (panel && panel.setActive) tryDo(() => panel.setActive(f, 'thumb')); else if (card && card.update) tryDo(() => card.update({ face: f })); };
      if (C && C.mountPiece) card = tryDo(() => C.mountPiece(cardHost, { row: line, key: job.key, face: (stage() && stage().active) || o.view || face, stage: stage(), onPick: pick, artFaces: o.artFaces, approve: false, nameOf: o.nameOf || lineName, isWorking: Eng() && Eng().isWorking ? Eng().isWorking : undefined }), null);
      if (!card) cardHost.remove();
      let lastShape = '';
      /** The card follows the stage: the faces engraved and the face shown at once (a new choice or face redraws its thumbnails; words typed are painted by showStage, or by the old preview below). */
      const follow = (st, kind) => {
        if (!card || !card.update) return;
        const shape = st ? st.choice + '|' + st.active : '';
        if (kind === 'words' || kind === 'font' || kind === 'pos') { if (!staged() && card.label && st && kind === 'words') tryDo(() => card.label(st.active, (st.faces[st.active] || {}).text || '')); if (shape === lastShape) return; }
        lastShape = shape; tryDo(() => card.update({ face: st ? st.active : face, stage: st }));
      };
      panel = mount(panelHost, line, {
        job, face: o.view || face, scope: slot, faceSwitch: !card, onDraft: o.onDraft, onVisible: v => { slot.hidden = !v && !card; }, thumb: f => (card && card.thumb ? card.thumb(f) : null), show: navigate,
        onStage: (st, kind) => follow(st, kind),
        onFace: f => { if (typeof o.onFace === 'function') tryDo(() => o.onFace(f)); },
        onChange: ev => { if (typeof o.onChange === 'function') tryDo(() => o.onChange(ev)); }
      });
      // (Engrave.showFace's turn lands: the panel already has the face; the card of that face's own job opens only where the page has no showStage)
      return { destroy() { tryDo(() => panel.destroy()); if (card && card.destroy) tryDo(() => card.destroy()); }, refresh() { tryDo(() => panel.refresh()); }, setFace: f => { const moved = !!tryDo(() => panel.setActive ? panel.setActive(f, 'turn') : panel.setFace(f), false); if (!moved && card && card.update) tryDo(() => card.update({ face: f })); jobNav(f); return moved; }, show: pick, panel, card };
    } catch (e) { if (typeof console !== 'undefined') console.warn('engraving panel slot', e); return none; }
  }

  return { mount, mountPiece, html, bind, modesFor, choicesFor, seedText, stageOf, stateOf, busyOf, stagePos, setActive, sweep, isLive, DRAFTS, COMMITS, MODES, CHOICES, gateOf, dirtyOf, hilite: { model: hiModel, decorate: hiDecorate, systemChoice, reasonLine, HI_TITLE } };
});
