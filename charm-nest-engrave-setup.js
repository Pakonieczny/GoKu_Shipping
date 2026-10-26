/* The LINE'S ENGRAVING SETUP editor (Paul, 11 Oct 2026: "I should be able to also verify the choices the system made in the engraving panel and update them as needed").
 * Plan: plans/front-back-1011 (CONTRACT.md section 3, SURFACES.md row 6, ENGSETUP-API.md).
 *
 * ONE small control for the whole app. It says what the system decided for a LINE: "Engraves: Front + back" (None / Front only / Back only / Front + back), the words of each
 * face (seeded from spec.engraving, with the system's one plain "why" line, where it came from and how sure it is), the font of each face (the line's font unless a face names
 * its own). Every edit is saved as row.engravingOverride (by = the signed-in login, never
 * typed) and always wins over the reading. It is drawn INLINE in a card (never a pop-up; a question is a plain line inside the card), a wait is a small labelled spinner,
 * a failure is one plain line.
 *
 *   mount(slot, row, opts?)         THE ONE-LINE MOUNT: draws the control into `slot` for the line of `row` (the line row or any side row / job.row of it) and wires it; safe to call
 *                                   again after every redraw of the host (the staged edit, whether it is open, and the caret come back). Draws nothing when the line has nothing to say.
 *                                   opts: { compact, open, onSaved(result), jobs(), save(patch) }  -> { destroy(), refresh() }
 *   html(row, opts) / bind(el, row, handlers)   the same in two steps (el holds ONLY the control). handlers: { save(patch), jobs() } (defaults: the bridge's Engrave.saveSetup /
 *                                   jobsOf)
 *   read(row, { jobs, slot })       pure view model of the control (setup, per-face rows, why, source, edited, raw words, locks); `slot` ('L' | 'R' | 'D2' ...): ONE piece's view (its own words, its own jobs, vm.jobKeys)
 *   setupFor(row, slot?, noOverride?)  the line's effective setup (CharmNestEngraveFaces.setupOf over the reading, TAGSKU's tag option when the intake has not read the line yet, and the override); with a slot: that piece's words
 *                                   (engravingOverride.pieces[slot][face], else the line's). Legacy setupFor(row, true) still means noOverride.
 *   (PIECES) patch.all: with the line's words (no slot), the words of every piece for that face are dropped: "the same on every piece", one quiet control for the views
 *   buildOverride(line, patch, by, now)   pure: the row.engravingOverride of CONTRACT 3 (merges the previous one; a face the mode does not engrave is dropped). patch.slot: the words go to pieces[slot][face] (one piece),
 *                                   the mode and the line-level words stay the line's; an empty text on a piece face removes that piece's entry (it inherits the line's again)
 *   sig(row)                        what the control would show, as a short string (a host that caches its card draws it again when this changes)
 *   consequences(line, jobs, nextSetup, prevSetup)   pure: what a save does to the jobs, in plain lines + the jobs it removes / cuts plain / opens / adds
 *   apply(line, patch, env)   the save path (name -> override -> ensureJobs -> reconcile -> seed), all side effects through `env` (the bridge's Engrave.saveSetup builds it)
 *   labelled(text)                  the words of each face found by its label in the buyer's note ("front - W" / "back - 1. 27. 24")
 *
 * Pure but for mount / html / bind (DOM). Loads in the page (window.CharmNestEngraveSetup) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveSetup = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const dep = (name, file) => { const g = root && root[name]; if (g) return g; try { return typeof require === 'function' ? require(file) : null; } catch (_) { return null; } };
  const Faces = () => dep('CharmNestEngraveFaces', './charm-nest-engrave-faces.js');
  const Orders = () => dep('CharmNestOrders', './charm-nest-orders.js');
  const str = x => String(x == null ? '' : x);
  const esc = s => str(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const lineOf = row => (row && row.parentRow) || row || null;
  const DECIDED = ['approved', 'written', 'skipped', 'none'];
  const OPEN_STATES = ['classify', 'reclassify'];
  const CUT = ['written', 'labelled', 'committed'];
  const MODE_TABS = [['none', 'None'], ['front', 'Front only'], ['back', 'Back only'], ['both', 'Front + back']];
  const FACE_NAME = { front: 'front', back: 'back' };
  const cap1 = s => s.charAt(0).toUpperCase() + s.slice(1);
  const linesOf = v => (Array.isArray(v) ? v : str(v).split(/\r?\n/)).map(x => str(x).replace(/\s+/g, ' ').trim()).filter(Boolean);
  /** One run of words: line breaks as spaces, and (for a font with no lowercase, Ace) in capitals: the same words however the fit laid them out. */
  const runOf = (v, fontId) => { let t = linesOf(v).join(' ').replace(/\s+/g, ' ').trim(); try { const O = Orders(); if (fontId && O && O.fontCaps && O.fontCaps(fontId)) t = O.capsText(t, fontId); } catch (_) { /* as written */ } return t; };
  /** The words of a job as they were GIVEN: the fit lays them out again (job.lines is the cut's layout: "NAME" / "ONE", capitals for Ace), but job.lineInput keeps what went in. A job a person decided, or laid out by hand (lineMode "preserve"), shows its lines as they are. */
  const wordsOfJob = j => {
    const ls = linesOf((j && (j.lines && j.lines.length ? j.lines : j.text)) || '');
    if (j && !DECIDED.includes(j.state) && j.lineMode !== 'preserve' && Array.isArray(j.lineInput) && j.lineInput.length) { const li = linesOf(j.lineInput); if (li.length && runOf(li).toLowerCase() === runOf(ls).toLowerCase()) return li.join('\n'); }
    return ls.join('\n');
  };
  const faceOfJob = j => { const F = Faces(); return F ? F.faceOf(j) : 'back'; };

  /* ── the buyer's words ─────────────────────────────────────────────────────── */
  /** The words of each face found by its label: "front - W" / "back - 1. 27. 24", "Front: A, Back: B" (also on one line). { front: [lines]|null, back: [lines]|null, any }. Never guesses: no label, no words. */
  function labelled(text) {
    const s = str(text), out = { front: null, back: null, any: false }, rx = /(^|[\s,;|·])(front|back)(?:\s+side)?\s*[:=\-–—]\s*/gi, hits = []; let m;
    while ((m = rx.exec(s))) hits.push({ face: m[2].toLowerCase(), at: m.index + m[1].length, end: rx.lastIndex });
    hits.forEach((h, i) => {
      const val = s.slice(h.end, i + 1 < hits.length ? hits[i + 1].at : s.length).replace(/[\s\/,;|·]+$/, '').trim();
      out.any = true; if (val && !out[h.face]) out[h.face] = linesOf(val);
    });
    return out;
  }
  /** The buyer's own words of a line (what a person must read when nothing says which words go where). */
  function rawOf(line) {
    const sp = line && line.spec || {}, e = sp.engraving && typeof sp.engraving === 'object' ? sp.engraving : null;
    if (e && str(e.raw).trim()) return str(e.raw).trim();
    const O = Orders(), vis = O && O.visible ? x => O.visible(x) : x => x;
    return (Array.isArray(sp.personalization) ? sp.personalization : [sp.personalization]).map(x => str(vis(x)).trim()).filter(Boolean).join('\n');
  }
  /** While the intake has not read the line (spec.engraving, ENGINTAKE), the Mini Tag choice TAGSKU read (spec.tagOption: "Tag1 (front+back)") and the labelled note give the reading. */
  function derivedReading(line) {
    const sp = line && line.spec, t = sp && sp.tagOption;
    if (!t || !['front', 'both', 'back'].includes(t.mode)) return null;
    const raw = rawOf(line), lab = labelled(raw), whole = linesOf(raw), mk = ls => (ls && ls.length ? { text: ls.join('\n'), lines: ls.slice(), fontWord: null } : null);
    const o = { v: 1, mode: t.mode, source: 'option', front: null, back: null, textSource: 'none', needsPerson: false, confidence: 0.9, option: { name: str(t.name), value: str(t.value) } };
    const asks = t.mode === 'both' ? 'asks for both faces' : `asks for the ${t.mode} only`, ov = optView(o.option.name, o.option.value), said = ov && ov.paired ? `“${ov.line}” ${asks}` : `${(ov && ov.label) || o.option.name || 'The option'} “${(ov && ov.text) || o.option.value}” ${asks}`;
    if (t.mode === 'both') {
      if (lab.front || lab.back) { o.front = mk(lab.front); o.back = mk(lab.back); o.textSource = 'labelled'; o.needsPerson = !(lab.front && lab.back); o.confidence = lab.front && lab.back ? 0.95 : 0.6; }
      else if (whole.length) { o.textSource = 'unlabelled'; o.needsPerson = true; o.confidence = 0.5; }
    } else if (lab[t.mode]) { o[t.mode] = mk(lab[t.mode]); o.textSource = 'labelled'; }
    else if (!lab.any && whole.length) { o[t.mode] = mk(whole); o.textSource = 'single'; }
    o.why = o.textSource === 'labelled' ? `${said}; the note is labelled ${[lab.front && 'front - ' + lab.front.join(' / '), lab.back && 'back - ' + lab.back.join(' / ')].filter(Boolean).join(' / ')}`
      : o.needsPerson ? `${said}, but the note does not say which words go where` : o.textSource === 'single' ? `${said}; the buyer's words go to the ${t.mode}` : `${said}; the buyer wrote no words for it`;
    return o;
  }
  /** The line's reading as the control tells it: spec.engraving when the intake wrote it, else the tag option's. */
  function readingOf(line) {
    const sp = line && line.spec, F = Faces();
    if (sp && sp.engraving && typeof sp.engraving === 'object' && F && F.MODES.includes(sp.engraving.mode)) return sp.engraving;
    return derivedReading(line);
  }
  /** The line's effective setup (CharmNestEngraveFaces.setupOf over the reading), or null for a row with no spec. */
  function setupFor(row, slot, noOverride) {
    if (typeof slot === 'boolean') { noOverride = slot; slot = null; }   // (the old call: setupFor(row, true) = without the person's override)
    const F = Faces(), line = lineOf(row); if (!F || !line || !line.spec) return null;
    const read = readingOf(line), own = line.spec.engraving;
    const spec = read && read !== own ? Object.assign({}, line.spec, { engraving: read }) : line.spec;
    return F.setupOf({ spec }, noOverride ? null : (line.engravingOverride || null), slot || null);
  }

  /* ── the system's own choice (ENGAUTO, Paul 12 Oct 2026, point 5) ───────────────────────────────────────────────────────────────────────
   * "All of these options default state is to be dynamically auto-selected intelligently by the system based on the rules of the system, order details, inclusive of customer
   * instructions and choices made by the customer." autoChoice is that choice, ignoring what any person saved (row.engravingOverride) or decided at a card (job.decision):
   * the intake's reading (spec.engraving: the sides policy, the front / back words, the option, the title, the notes, the font reader), the Mini Tag option, the earring rule, and, for a
   * back the intake leaves to the reader, the reader's own words (a job nobody edited), else the buyer's own one inscription. */
  const slotIx = slot => { const m = /^D(\d{1,2})(?:\.\d{1,3})?$/.exec(str(slot)); return m ? +m[1] - 1 : null; };   // (disc n of any unit: the font words of a disc repeat for every unit)
  /** The installed font id a face is engraved in: the shop's word for it (fontWord), else the line's font for that face (the same rule as CharmNestEngravePreview.fontIdOf). Pure. */
  function fontIdOf(row, face, fontWord, slot) {
    const O = Orders(), line = lineOf(row); let id = '';
    try { if (fontWord && O && O.fontOfPhrase) id = O.fontOfPhrase(fontWord) || ''; } catch (_) { /* below */ }
    if (!id && O && O.pieceFont && line && line.spec) { try { id = O.pieceFont(line.spec, slotIx(slot || (row && row.slot)), face === 'front' ? 'front' : 'back').id || ''; } catch (_) { /* the app's own font */ } }
    return id || (O && O.DEFAULT_FONT) || '';
  }
  const fontNameOf = id => { const O = Orders(); try { const f = O && O.fontById && id ? O.fontById(id) : null; return f ? f.name : ''; } catch (_) { return ''; } };
  /** A system job: its words came from the reader or the intake, not from a person (seedWords marks source "person" and a decision; decideWords and a person's "no engraving" a decision). */
  const systemJob = j => !!j && !j.decision && j.source !== 'person' && !j.editingBack;
  /** The words ONE face has from the reader (the jobs of that face and piece nobody edited), as lines; null when there are none or they differ from piece to piece. */
  function readerLines(face, slot, jobs) {
    const mine = (jobs || []).filter(j => j && faceOfJob(j) === face && (!slot || slotOfJob(j) === slot) && systemJob(j)), words = [...new Set(mine.map(wordsOfJob).filter(Boolean))];
    return words.length === 1 ? words[0].split('\n') : null;
  }
  /** One plain inscription in the buyer's own box (what the reader will give back for a back nobody labelled): a line or three, short, no instruction in it, font requests taken out. */
  function plainOwnWords(line) {
    const raw = rawOf(line); if (!raw) return null;
    const O = Orders(); let ls = linesOf(raw);
    try { const fr = O && O.fontReadText ? O.fontReadText(raw, { cueOnly: true }) : null; if (fr && fr.changed) ls = linesOf(fr.lines || []); } catch (_) { /* the words as written */ }
    if (!ls.length || ls.length > 3 || ls.some(l => l.length > 40 || /[()\[\]]/.test(l))) return null;
    if (/\b(?:please|pls|thanks?|thank\s*you|capital|capitals|uppercase|lowercase|all\s*caps|cent(?:er|re)d?|font|typeface|size|bold|italics?|cursive|script|engrav\w*|hello|hi|hey|dear|would\s+like|i\s+want|i'd\s+like|make\s+it|same\s+as|front|back|both|sides?)\b/i.test(ls.join(' '))) return null;
    return ls;
  }
  /** `{ mode, faces, front, back, position:'auto', source, why, needsPerson, never, confidence, slot, option, textFrom }`: the choice of the SYSTEM for a line (and one piece of it: `slot` 'L' | 'R' | 'D2'), whatever a person saved.
   *    mode      'none' | 'front' | 'back' | 'both'
   *    front/back  { text, lines, fontWord (the face's own font word or null), fontId (the installed font it is engraved in), fontName, provisional? } or null (the face is not engraved, or its words are not known)
   *    source    which rule decided the mode: 'rule' (an earring is never engraved), 'option' (a listing option: "Tag1 (front+back)", "Both Sides", "Blank"), 'sides' (a side count on a design with its own front art),
   *              'tag' (the Mini Tag choice read before the intake), 'title' (the listing title), 'note' (labels or a phrase in the personalisation / a message / the staff note), 'reader' (the reader found no engraving),
   *              'default' (no front / back word anywhere: the back, as every line always was)
   *    why       the intake's one plain line (the words it read, where from, the font); needsPerson: the system could not tell which words go where (a person is asked).
   *  o: { jobs } (default Engrave.jobsOf(line) in the page): the reader's words for a back the intake leaves to it. Pure; compare fonts by fontId, never by fontWord. */
  function autoChoice(row, slot, o) {
    const F = Faces(), line = lineOf(row); if (!F || !line || !line.spec) return null;
    const sl = slot && SLOT_RX.test(String(slot)) ? String(slot) : null;
    const setup = setupFor(line, sl, true); if (!setup) return null;
    const out = { mode: setup.mode, faces: setup.faces.slice(), front: null, back: null, position: 'auto', source: '', why: str(setup.why), needsPerson: !!setup.needsPerson, never: !!setup.never, confidence: setup.confidence, slot: sl, option: setup.option || null, textFrom: '' };
    if (setup.never) { out.source = 'rule'; return out; }
    const jobs = ((o && o.jobs) || (root.Engrave && root.Engrave.jobsOf ? root.Engrave.jobsOf(line) : []) || []).filter(Boolean);
    const rd = readingOf(line), own = line.spec.engraving;
    out.source = setup.sides ? 'sides' : rd && rd !== own ? 'tag' : ['option', 'title', 'note'].includes(setup.source) ? setup.source : 'default';
    // the reader found nothing to engrave (a job it put away as "no engraving", nobody decided otherwise): the system's choice is none
    const mineJobs = jobs.filter(j => j && (!sl || slotOfJob(j) === sl));
    if (!rd && setup.mode !== 'none' && mineJobs.length && mineJobs.every(j => j.state === 'none' && !j.decision)) { out.mode = 'none'; out.faces = []; out.source = 'reader'; out.why = str(mineJobs[0].reason) || 'The reader found no engraving'; return out; }
    for (const face of out.faces) {
      const w = setup[face]; let lines = w && w.lines && w.lines.length ? w.lines.slice() : null, provisional = false, from = lines ? 'intake' : '';
      if (!lines && !setup.needsPerson && face === 'back') {
        lines = readerLines(face, sl, jobs); if (lines) from = 'reader';
        else if (setup.mode === 'back') {
          const lab = labelled(rawOf(line)); lines = lab.back && lab.back.length ? lab.back.slice() : plainOwnWords(line);   // ("back: 21": the label is no part of the words)
          if (lines) { provisional = true; from = 'personalization'; }
        }
      }
      const fontWord = (w && w.fontWord) || null, id = fontIdOf(line, face, fontWord, sl);
      out[face] = { text: lines ? lines.join('\n') : '', lines: lines || [], fontWord, fontId: id, fontName: fontNameOf(id), ...(provisional ? { provisional: true } : {}), from };
      if (from && !out.textFrom) out.textFrom = from;
    }
    return out;
  }
  const sameText = (a, b) => linesOf(a).join('\n') === linesOf(b).join('\n');
  /** What a person's choice `now` ({ mode, front: { text, fontWord }, back: {...} }, e.g. the panel's staged edit or a saved override) changes against the system's: { changed, parts: ['mode', 'front.text', 'back.font', ...], auto }.
   *  Only the faces the person's mode engraves are compared; fonts by the installed font they resolve to. */
  function diffFromAuto(row, slot, now, o) {
    const auto = autoChoice(row, slot, o), parts = []; if (!auto || !now) return { changed: false, parts, auto };
    const F = Faces(), faces = mode => (mode === 'both' ? ['front', 'back'] : mode === 'front' ? ['front'] : mode === 'back' ? ['back'] : []), mode = F.MODES.includes(now.mode) ? now.mode : auto.mode;
    if (mode !== auto.mode) parts.push('mode');
    for (const face of faces(mode)) {
      const a = auto[face], n = now[face] || { text: '', fontWord: null }, nt = n.text != null ? n.text : n.lines, text = linesOf(nt).join('\n');
      if (!a) { if (text) parts.push(face + '.text'); continue; }
      const nid = fontIdOf(row, face, n.fontWord || n.font || null, slot);
      if (runOf(text, nid) !== runOf(a.text, nid) && !(a.provisional && !text)) parts.push(face + '.text');   // (the same words in the fit's line split, or in capitals for Ace, are no change)
      if (nid !== a.fontId) parts.push(face + '.font');
    }
    return { changed: parts.length > 0, parts, auto };
  }

  /* ── fonts ─────────────────────────────────────────────────────────────────── */
  /** The font words a face may name: the shop's own (Stylish, Pristina, ... Ace, Britannic Bold), each with the installed font it engraves in. */
  function fontChoices() {
    const O = Orders(), out = [];
    for (const g of (O && O.FONT_WORDS) || []) { const w = g.words && g.words[0]; if (!w) continue; const f = O.fontById && O.fontById(g.id); out.push({ word: w, name: f ? f.name : '' }); }
    return out;
  }
  /** The font a face is engraved in: { word (what the buyer's word was), name (the installed font) }. A face's own word wins over the line's. */
  function faceFont(line, fontWord) {
    const O = Orders(), sp = line && line.spec;
    if (fontWord && O && O.fontOfPhrase) { const f = O.fontById && O.fontById(O.fontOfPhrase(fontWord)); return { word: fontWord, name: f ? f.name : '', own: true }; }
    if (O && O.pieceFont && sp) { try { const p = O.pieceFont(sp, null); return { word: p.asked || '', name: p.name || '', own: false, listing: !!p.listing, via: p.via || '', sub: !!p.substituted }; } catch (_) { /* below */ } }
    return { word: '', name: '', own: false };
  }

  /* ── the view model ────────────────────────────────────────────────────────── */
  /* OPTTIDY (Paul, 13 Oct 2026): an option named in these words is named as the app's one display reader says it ("the Length · Font option", not "the Necklace Length&middot;FONT option"); display only. */
  const optView = (name, value) => { const O = Orders(); try { return O && typeof O.optionView === 'function' ? O.optionView(name, value) : null; } catch (_) { return null; } };
  // (the system's reason, decoded to text; and the option it quotes, "the option “Metal &middot; #Characters?: GOLD·Set·ENGRAVED”", said as the display reader says it. Only that quote: the buyer's own words are never touched. A clipped quote stays.)
  const shownWhy = t => {
    const O = Orders();
    try {
      if (!O || typeof O.shownText !== 'function') return str(t);
      const s = O.shownText(t);
      return typeof O.optionView !== 'function' ? s : s.replace(/(\bthe option )\u201c([^\u201d]+)\u201d/g, (m, lead, q) => { const i = q.indexOf(': '); if (i <= 0 || /\u2026$/.test(q)) return m; const v = O.optionView(q.slice(0, i), q.slice(i + 2)); return v.changed ? lead + '\u201c' + v.line + '\u201d' : m; });
    } catch (_) { return str(t); }
  };
  const SRC = { option: o => { const v = o && o.name ? optView(o.name, o.value) : null; return o && o.name ? `the ${(v && (v.names || v.label)) || o.name} option` : 'the option'; }, title: () => 'the listing title', note: () => "the buyer's note", person: () => 'a person', profile: () => "this listing's profile", default: () => '' };   // (BARS, Paul 19 Oct 2026: a bar listing's profile, charm-nest-orders.js PIECE_LISTINGS)
  /** What the control shows for a line. null when the line has nothing to say (no spec, left the pull, done by hand, mode none with no edit). */
  const SLOT_RX = /^(?:C\d{1,3}|(?:L|R|D\d{1,2})(?:\.\d{1,3})?)$/;   // (a slot: L, R, D<n>; a later unit of a line of quantity >= 2 "<slot>.<unit>"; copy n "C<n>": charm-nest-engrave-sides.js)
  const slotOfJob = j => (j && (j.slot || (Faces() && Faces().parseKey(j.key).slot))) || null;
  function read(row, o) {
    const F = Faces(), line = lineOf(row); if (!F || !line || !line.spec) return null;
    if (F.neverOf && F.neverOf(line)) return null;   // (NO EARRING IS EVER ENGRAVED, charm-nest-orders.js section 3d: nothing to verify and nothing to edit, so the control is not drawn)
    const slot = o && o.slot && SLOT_RX.test(String(o.slot)) ? String(o.slot) : null;
    const setup = setupFor(line, slot); if (!setup) return null;
    if (line.state === 'gone' || (line.spec.customDone && line.spec.customDone.state !== 'open')) return null;
    const jobs = ((o && o.jobs) || (root.Engrave && root.Engrave.jobsOf ? root.Engrave.jobsOf(line) : []) || []).filter(Boolean);
    const pieceJobs = slot ? jobs.filter(j => slotOfJob(j) === slot) : jobs;   // (ONE piece asked for: only its own jobs are read, locked or shown)
    if (setup.mode === 'none' && !setup.edited && !jobs.length && !readingOf(line)) return null;
    const base = setup.edited ? setupFor(line, slot, true) : setup, lineFont = faceFont(line, null);
    const rows = setup.faces.map(face => {
      const mine = pieceJobs.filter(j => faceOfJob(j) === face), wordsList = mine.map(wordsOfJob).filter(Boolean), distinct = [...new Set(wordsList)];
      const perPiece = !slot && distinct.length > 1, w = setup[face];
      const locked = mine.find(j => DECIDED.includes(j.state) && j.state !== 'none') || null;
      let lines = perPiece ? [] : distinct.length ? distinct[0].split('\n') : (w ? w.lines.slice() : []);
      // (the words the system or the person gave, not the fit's own line split and capitals: the panel opens on exactly the choice, and a font-only save never sends the cut's layout as "words")
      if (!perPiece && !locked && distinct.length && w && w.lines && w.lines.length) { const fid = fontIdOf(line, face, w.fontWord, slot); if (runOf(lines, fid) === runOf(w.lines, fid)) lines = w.lines.slice(); }
      const by = locked ? str(locked.approvedBy || locked.decidedBy || (locked.decision && locked.decision.by)) : '';
      const ft = faceFont(line, w && w.fontWord);
      return { face, label: cap1(face), key: mine.length === 1 ? mine[0].key : (slot || !mine.length ? F.keyOf(line, face, slot) : ''), lines, text: lines.join('\n'), fontWord: (w && w.fontWord) || '', font: ft, perPiece, locked: false, decided: !!locked, lockState: locked ? locked.state : '', lockBy: by, state: mine.length ? mine.map(j => j.state)[0] : '', jobs: mine };
    });
    const e = readingOf(line) || {}, pct = Math.round((+setup.confidence || 0) * 100);
    const src = setup.edited ? 'person' : setup.source, srcText = (SRC[src] || (() => ''))(setup.option || e.option);
    return { ok: true, line, key: line.key, slot, pieceKey: F.pieceKeyOf(F.keyOf(line, 'back', slot)), jobKeys: { front: F.keyOf(line, 'front', slot), back: F.keyOf(line, 'back', slot) },
      setup, base, mode: setup.mode, modeLabel: F.modeLabel(setup.mode), rows, jobs, pieceJobs, lineFont, edited: setup.edited, pieceEdited: !!setup.pieceEdited, by: setup.by, at: setup.at,
      why: setup.edited ? `Set by ${setup.by || 'a person'}${setup.at ? ' on ' + new Date(setup.at).toLocaleDateString() : ''}` : shownWhy(setup.why),
      reading: setup.edited ? F.describe(base) : '', from: setup.edited ? '' : srcText ? `from ${srcText}${pct > 0 && setup.source !== 'default' ? ` · ${pct}% sure` : ''}` : (setup.source === 'default' ? 'nothing in the listing or the note says otherwise' : ''),
      raw: rawOf(line), needsPerson: setup.needsPerson, textSource: setup.textSource,
      prompt: setup.needsPerson ? (setup.mode === 'both' ? 'Say which words go on the front and which on the back' : `Say which words go on the ${setup.faces[0] || 'charm'}`) : '',
      cut: CUT.includes(line.state), sig: JSON.stringify([slot, setup.mode, rows.map(r => [r.text, r.fontWord, r.decided, r.lockState, r.perPiece]), setup.edited, setup.pieceEdited, setup.needsPerson, CUT.includes(line.state)]) };
  }

  /* ── the override ───────────────────────────────────────────────────────────── */
  /** row.engravingOverride (CONTRACT 3). patch: { mode?, front?: {text, fontWord}|null, back?: ... }. Parts not in the patch are kept from the previous override; a face the final mode
   *  does not engrave is dropped (the reading's words come back if the mode comes back). `by` is the signed-in login. */
  function buildOverride(line, patch, by, now) {
    const F = Faces(), prev = line && line.engravingOverride && typeof line.engravingOverride === 'object' ? line.engravingOverride : {}, p = patch || {};
    const slot = p.slot && SLOT_RX.test(String(p.slot)) ? String(p.slot) : null;
    const mode = F.MODES.includes(p.mode) ? p.mode : (F.MODES.includes(prev.mode) ? prev.mode : undefined);
    const out = {}; if (mode) out.mode = mode;
    const live = mode === 'both' ? ['front', 'back'] : mode === 'front' ? ['front'] : mode === 'back' ? ['back'] : mode === 'none' ? [] : ['front', 'back'];
    // the line's own words: a patch for a PIECE (patch.slot) never touches them
    for (const face of live) {
      if (!slot && p[face] !== undefined) out[face] = F.normFace(p[face]);
      else if (prev[face] !== undefined) out[face] = prev[face];
    }
    // one piece's own words (ENGLINK): pieces[slot][face]; the other pieces' entries are kept exactly as they were (disc 2 is never disc 1)
    const pieces = {};
    if (prev.pieces && typeof prev.pieces === 'object') for (const sl of Object.keys(prev.pieces)) if (SLOT_RX.test(sl) && prev.pieces[sl] && typeof prev.pieces[sl] === 'object') pieces[sl] = Object.assign({}, prev.pieces[sl]);
    if (slot) {
      const q = pieces[slot] || (pieces[slot] = {});
      for (const face of ['front', 'back']) if (p[face] !== undefined) { const n = F.normFace(p[face]); if (n) q[face] = n; else delete q[face]; }   // (no words = this piece goes back to the line's)
    }
    // (PIECES: patch.all, one quiet "same on every piece": the line's words for a face replace the words each piece was given for that face)
    if (!slot && p.all) for (const face of ['front', 'back']) if (p[face] !== undefined) for (const sl of Object.keys(pieces)) delete pieces[sl][face];
    for (const sl of Object.keys(pieces)) { const q = pieces[sl]; for (const face of ['front', 'back']) if (!live.includes(face)) delete q[face]; if (!Object.keys(q).length) delete pieces[sl]; }
    if (Object.keys(pieces).length) out.pieces = pieces;
    out.by = str(by); out.at = now == null ? Date.now() : now;
    { const W = dep('CharmNestEngraveWhy', './charm-nest-engrave-why.js'); if (W && W.fold) W.fold(out, prev, p); }   // (ENGWHY: the reason for a person's override and the system's choice it replaced ride in THIS record: no extra write)
    return out;
  }

  /* ── what a save does ──────────────────────────────────────────────────────── */
  /** The jobs a change of setup (`prev` -> `next`) removes (undecided), cuts plain (decided), opens again (cut plain / no engraving, asked for again) and the faces that get a new placement. */
  function consequences(line, jobs, next, prev) {
    const F = Faces(), list = (jobs || []).filter(Boolean), out = { remove: [], plain: [], reopen: [], add: [], none: [], lines: [], blocked: '' };
    if (!next) return out;
    const decidedWas = j => DECIDED.includes(j.state) && j.state !== 'none';
    if (line && CUT.includes(line.state) && list.some(j => faceOfJob(j) && !next.faces.includes(faceOfJob(j)) && decidedWas(j))) out.blocked = 'This piece is already cut: an engraving that is decided on it can no longer be changed here.';
    if (!next.faces.length) {
      for (const j of list) (decidedWas(j) ? (j.state === 'skipped' ? [] : out.plain) : j.state === 'none' ? [] : out.none).push(j);
    } else {
      const rc = F.reconcile(next, list, [null]);
      out.remove = rc.drop.slice(); out.plain = rc.keep.filter(j => j.state !== 'skipped' && j.state !== 'none');
      out.add = rc.add.map(a => a.face).filter((f, i, a) => a.indexOf(f) === i);
      // (a face the person asks for AGAIN: its job was cut plain / set to no engraving while the setup did not ask for it. A cut-plain job of a face that was wanted all along is a decision, not reopened.)
      out.reopen = list.filter(j => next.faces.includes(faceOfJob(j)) && !(prev && prev.faces.includes(faceOfJob(j))) && (j.state === 'none' || j.state === 'skipped'));
    }
    // (a piece of a SET of different designs is "the small bar", not "disc 2": charm-nest-engrave-sides.js setPieceOf)
    const setWord = j => { const Sd = dep('CharmNestEngraveSides', './charm-nest-engrave-sides.js'), sp = Sd && Sd.setPieceOf ? Sd.setPieceOf(j) : null; return sp ? Sd.pieceName(sp).toLowerCase() : ''; };
    const nounOfJob = j => { const Sd = dep('CharmNestEngraveSides', './charm-nest-engrave-sides.js'); return Sd && Sd.nounOf ? Sd.nounOf(j) : ''; };   // (BARS, Paul 19 Oct 2026: "the front of bar 2", not "disc 2", on a profiled listing's line)
    const slotWordOf = sl => { const Fc = Faces(); return Fc && Fc.slotLabel ? Fc.slotLabel(sl).toLowerCase() : str(sl); };   // (a piece of a line of several units: "piece 2", "left · pair 2")
    const nm = j => FACE_NAME[faceOfJob(j)] + (j.slot ? ' of the ' + (setWord(j) || (j.slot === 'L' ? 'left ear' : j.slot === 'R' ? 'right ear' : /^D\d+$/.test(str(j.slot)) ? (nounOfJob(j) || 'disc') + ' ' + str(j.slot).slice(1) : slotWordOf(j.slot))) : '');
    for (const j of out.plain) { const by = str(j.approvedBy || j.decidedBy || (j.decision && j.decision.by)); out.lines.push(`The ${nm(j)} is already ${j.state === 'written' ? 'approved and saved' : 'approved'}${by ? ' by ' + by : ''}: saving cuts it plain (its approval seal stays, a "cut plain" seal is added and its file is withdrawn).`); }
    for (const j of out.remove) out.lines.push(`The ${nm(j)} placement that was still to check is removed.`);
    for (const j of out.none) out.lines.push(`The ${nm(j)} is set to no engraving.`);
    for (const f of out.add) out.lines.push(`The ${f} gets its own placement to check.`);
    for (const j of out.reopen) out.lines.push(`The ${nm(j)} was ${j.state === 'none' ? 'set to no engraving' : 'cut plain'}: it is opened again for engraving (the old seals stay).`);
    return out;
  }

  /** The same words? (lines of a job against a seed) */
  const sameWords = (job, seed) => wordsOfJob(job) === linesOf(seed.lines).join('\n');

  /** THE SAVE PATH. Everything that touches the page goes through `env`:
   *    who() -> name | Promise<name> ('' = put away: nothing happens)   now()   store(line, override|null) (sets/deletes line.engravingOverride and persists)
   *    jobsOf(line)   ensureJobs(line)   slots(line)   remove(job)   skip(job, by) (cut plain + seal)   setNone(job, why)   reopen(job)   seed(job, seed)   changed(line, result)
   *  Returns { ok, cancelled?, message?, pending?, removed, plain, none, reopened, seeded, added }. A decided face is NEVER deleted; nothing asks Etsy or the paid reader. */
  async function apply(line, patch, env) {
    const F = Faces();
    const res = { ok: false, removed: [], plain: [], none: [], reopened: [], seeded: [], added: [], pending: false };
    if (!line || !line.spec) { res.message = 'This piece is not loaded.'; return res; }
    if (line.state === 'gone') { res.message = 'This order has left the pull, so its engraving is not changed here.'; return res; }
    // NO EARRING IS EVER ENGRAVED (charm-nest-orders.js section 3d): a person's pick for an earring is dropped quietly (nothing is stored, no job is made, nothing asks)
    if (F.neverOf && F.neverOf(line)) { res.ok = true; res.never = true; return res; }
    let by = env.who(); if (by && typeof by.then === 'function') by = await by;
    by = str(by).trim(); if (!by) { res.cancelled = true; return res; }
    const prev = line.engravingOverride || null, now = env.now ? env.now() : Date.now();
    const override = buildOverride(line, patch, by, now);
    const whyText = (() => { const W = dep('CharmNestEngraveWhy', './charm-nest-engrave-why.js'); return W && W.say && patch && patch.reason && override.reason ? W.say(override.reason) : ''; })();   // (the words the order timeline's existing "edited by" line carries)
    // a decided face on a line that is already cut stays as it is: the check is made on the setup this change would give, before anything is stored
    const next = F.setupOf({ spec: (() => { const r = readingOf(line); return r && r !== line.spec.engraving ? Object.assign({}, line.spec, { engraving: r }) : line.spec; })() }, override);
    const before = setupFor(line), pre = consequences(line, env.jobsOf(line), next, before);
    if (pre.blocked) { res.message = pre.blocked; return res; }
    try { env.store(line, override); } catch (e) { try { env.store(line, prev); } catch (_) { /* the page keeps what it had */ } res.message = `Not saved: ${e && e.message ? e.message : e}`; return res; }
    res.ok = true;
    try {
      env.ensureJobs(line);
      let jobs = env.jobsOf(line);
      const setup = setupFor(line), slots = env.slots ? env.slots(line) : [null];
      const rc = setup.faces.length ? F.reconcile(setup, jobs, slots) : { add: [], drop: [], keep: [] };
      // the page makes every job the setup asks for; if it cannot (the front engine is not in this page yet) only the choice is kept and no job is touched
      if (rc.add.length) { res.pending = true; res.added = rc.add.map(a => a.face).filter((f, i, a) => a.indexOf(f) === i); }
      else {
        const plainWhy = `no engraving - decided by ${by}`;
        if (!setup.faces.length) {
          for (const j of jobs) { if (j.state === 'none' || j.state === 'skipped') continue; if (DECIDED.includes(j.state)) { await env.skip(j, by); res.plain.push(j.key); } else { env.setNone(j, plainWhy, by, whyText); res.none.push(j.key); } }
        } else {
          for (const j of rc.drop) { env.remove(j); res.removed.push(j.key); }
          for (const j of rc.keep) { if (j.state === 'skipped' || j.state === 'none') continue; await env.skip(j, by); res.plain.push(j.key); }
          for (const j of env.jobsOf(line)) if (setup.faces.includes(faceOfJob(j)) && !before.faces.includes(faceOfJob(j)) && (j.state === 'none' || j.state === 'skipped') && !CUT.includes(line.state)) { env.reopen(j); res.reopened.push(j.key); }
        }
      }
      // an undecided face job the setup still asks for whose words differ from the person's words takes them (no paid reading; also while a new face is still pending, since nothing is removed or cut here);
      // a job still being classified takes its words from the setup itself
      const only = patch && patch.slot && SLOT_RX.test(String(patch.slot)) ? String(patch.slot) : null;
      for (const j of env.jobsOf(line)) {
        const face = faceOfJob(j); if (!setup.faces.includes(face) || OPEN_STATES.includes(j.state)) continue;
        // (NODECIDED: an approved, saved or cut-plain piece takes the words and font a person changed on ITS face, and goes back to needing its Approve: env.reopenForEdit, the transition a Reopen always made; a 'none' job never)
        const wasDecided = j.state === 'approved' || j.state === 'written' || j.state === 'skipped';
        if (DECIDED.includes(j.state) && !wasDecided) continue;
        const at = slotOfJob(j);
        if (only && at !== only) continue;   // (a piece's own edit re-seeds that piece's jobs only: disc 2 never reaches disc 1)
        const seed = F.seedFor(setup, face, at); if (!seed) continue;
        const touched = !!(patch && patch[face] !== undefined);
        if (wasDecided && !touched) continue;
        // a face the person did not touch keeps its job when the words are the same words however the fit laid them out ("NAME" / "ONE" in capitals is "Name One");
        // a face the person edited re-seeds when the words, or the font picked for this face, differ from the job's
        if (touched ? (sameWords(j, seed) && str(j.faceFontWord) === str(seed.fontWord)) : (sameWords(j, seed) || runOf(wordsOfJob(j), fontIdOf(line, face, j.faceFontWord || seed.fontWord || null, at)) === runOf(seed.lines, fontIdOf(line, face, seed.fontWord || j.faceFontWord || null, at)))) continue;
        if (wasDecided && !(env.reopenForEdit && env.reopenForEdit(j))) { res.kept = (res.kept || []).concat(j.key); continue; }   // (busy, or a recalled set not rebuilt yet: it stays as it is, said by the card)
        await env.seed(j, Object.assign({}, seed, { by, ...(whyText ? { why: whyText } : {}) })); res.seeded.push(j.key);
      }
    } catch (e) {
      res.ok = false; res.message = `Your choice is saved, but the placements could not all be updated: ${e && e.message ? e.message : e}. Press Save again to finish.`;
    }
    try { env.changed(line, res); } catch (_) { /* a redraw that failed is not a failed save */ }
    return res;
  }

  /* ── what the person sees ───────────────────────────────────────────────────── */
  const CSS = `.egSetup{display:grid;gap:6px;min-width:0;font:12px var(--sans,system-ui);color:var(--ink,#1c1a17)}
.egSetup[hidden],[data-engsetup][hidden]{display:none!important}
.egSuTop{display:flex;align-items:center;flex-wrap:wrap;gap:4px 8px;min-width:0}
.egSuLbl{font:600 10.5px var(--sans,system-ui);letter-spacing:.06em;text-transform:uppercase;color:var(--ink45,#938c80)}
.egSuMode{font-weight:600}
.egSuMeta{font-size:11px;color:var(--ink45,#938c80)}
.egSuFaces{display:grid;gap:3px;min-width:0}
.egSuFace{display:flex;align-items:baseline;gap:6px;min-width:0}
.egSuFace .fl{flex:0 0 38px;font:600 10.5px var(--sans,system-ui);letter-spacing:.04em;text-transform:uppercase;color:var(--ink45,#938c80)}
.egSuFace .w{font:600 13px var(--serif,Georgia,serif);min-width:0;overflow-wrap:anywhere;white-space:pre-line}
.egSuFace .w.none{font:italic 12px var(--sans,system-ui);color:var(--ink45,#938c80)}
.egSuFace .f{margin-left:auto;font:10.5px var(--sans,system-ui);color:var(--ink45,#938c80);white-space:nowrap}
.egSuWhy{font-size:11px;line-height:1.35;color:var(--ink70,#5b554c)}
.egSuWhy small{color:var(--ink45,#938c80);font-size:11px}
.egSuAsk{font-size:12px;font-weight:600;color:var(--clay,#b0563f)}
.egSuRaw{padding:5px 8px;border-radius:8px;background:var(--card2,#faf7f1);border:1px solid var(--line,#e4ddd0);font:600 12.5px var(--serif,Georgia,serif);white-space:pre-line;overflow-wrap:anywhere}
.egSuTabT{padding:2px 4px 2px 9px;font:600 12px var(--sans,system-ui);white-space:nowrap}
.egSuTab:disabled{opacity:.55;cursor:default}
.egSuEdit{display:grid;gap:4px;min-width:0}
.egSuFe{display:grid;grid-template-columns:44px minmax(0,1fr) minmax(0,150px);gap:4px 6px;align-items:start}
.egSuFe .fl{padding-top:6px;font:600 10.5px var(--sans,system-ui);letter-spacing:.04em;text-transform:uppercase;color:var(--ink45,#938c80)}
.egSuFe textarea,.egSuFe select{min-width:0;width:100%;box-sizing:border-box;font:13px var(--sans,system-ui);padding:5px 7px;border:1px solid var(--line,#e4ddd0);border-radius:8px;background:var(--card,#fffefb);color:inherit}
.egSuFe textarea{resize:vertical;min-height:30px}
.egSuFe select{font-size:11.5px;padding:6px 5px}
.egSuFe .lock{grid-column:2/-1;font-size:11px;color:var(--ink45,#938c80)}
.egSuFe textarea:disabled,.egSuFe select:disabled{background:var(--paper2,#ebe5d9);color:var(--ink70,#5b554c)}
@media (max-width:520px){.egSuFe{grid-template-columns:44px minmax(0,1fr)}.egSuFe select{grid-column:2}}
.egSuNote{display:grid;gap:2px;font-size:11.5px;color:var(--ink70,#5b554c)}
.egSuNote.warn{color:var(--clay,#b0563f)}
.egSuActs{display:flex;align-items:center;flex-wrap:wrap;gap:4px 6px}
.egSuStat{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;color:var(--ink70,#5b554c)}
.egSuErr{font-size:11.5px;color:var(--clay,#b0563f)}
.egSuCompact .egSuWhy{font-size:10.5px}`;
  function ensureCss(doc) {
    try { if (!doc || doc.getElementById('cnEngSetupCss')) return; const s = doc.createElement('style'); s.id = 'cnEngSetupCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s); } catch (_) { /* unstyled is still usable */ }
  }
  const LIVE = new Map();     // line key -> the bindings that are on screen for it (a save that ends tells every one of them, whichever slot started it)
  const DRAFTS = new Map();   // line key -> the staged edit (so a host that draws its card again gets it back)
  const NOTES = new Map();    // line key -> the last result line ("Saved. ...")
  let uid = 0;

  const draftOf = vm => {
    let d = DRAFTS.get(vm.key);
    if (d && d.sig !== vm.sig && !d.dirty) d = null;                 // what the line says changed under an untouched draft: start from the new reading
    if (!d) { d = { sig: vm.sig, open: vm.needsPerson && !vm.cut, mode: vm.mode, faces: {}, dirty: false, busy: '', error: '', confirm: '', focus: null }; for (const f of ['front', 'back']) { const r = vm.rows.find(x => x.face === f); d.faces[f] = { text: r ? r.text : '', font: r ? r.fontWord : '' }; } DRAFTS.set(vm.key, d); }
    return d;
  };
  /** The words a face starts with when the mode asks for it and the person has typed nothing: the line's own (reading / job), else the buyer's labelled words, else (a single face and
   *  no labels at all) the words of the face it replaces. Never a guess between two faces. */
  function seedText(vm, face, mode) {
    const here = vm.rows.find(r => r.face === face); if (here && here.text) return here.text;
    const lab = labelled(vm.raw); if (lab[face]) return lab[face].join('\n');
    if (mode !== 'both' && !lab.any) {
      const other = vm.rows.find(r => r.face !== face && r.text), rd = vm.setup && vm.setup[face === 'front' ? 'back' : 'front'];
      if (other) return other.text; if (rd) return rd.text;
      return linesOf(vm.raw).join('\n');   // (one face and a note with no labels: all of it is that face's words)
    }
    return '';
  }
  const facesOf = mode => (mode === 'both' ? ['front', 'back'] : mode === 'front' ? ['front'] : mode === 'back' ? ['back'] : []);
  function patchOf(vm, d) {
    const patch = {}; if (vm.slot) patch.slot = vm.slot;   // (one piece's edit says which piece: buildOverride / apply keep it to that piece)
    if (d.mode !== vm.mode) patch.mode = d.mode;
    for (const f of facesOf(d.mode)) {
      const cur = vm.rows.find(r => r.face === f), x = d.faces[f] || { text: '', font: '' }, text = linesOf(x.text).join('\n');
      if (cur && cur.locked) continue;
      if (!cur) { if (text || x.font) patch[f] = { text: x.text, fontWord: x.font || null }; continue; }
      if (cur.perPiece) { if ((x.font || '') !== cur.fontWord) patch[f] = { text: cur.text, fontWord: x.font || null }; continue; }
      if (text !== cur.text || (x.font || '') !== cur.fontWord) patch[f] = { text: x.text, fontWord: x.font || null };
    }
    return patch;
  }
  const hasPatch = p => !!(p.mode || p.front !== undefined || p.back !== undefined);
  function nextSetupOf(vm, patch) {
    const F = Faces(), line = vm.line, r = readingOf(line), spec = r && r !== line.spec.engraving ? Object.assign({}, line.spec, { engraving: r }) : line.spec;
    return F.setupOf({ spec }, buildOverride(line, patch, 'x', 0), vm.slot || null);
  }

  /** "Ace → Jost": the word the buyer picked and the installed font it engraves in (just the one when they are the same). */
  const fontText = f => (f.word && f.name && f.word.toLowerCase() !== f.name.toLowerCase() ? `${f.word} → ${f.name}` : f.word || f.name || '') + (f.via === 'message' ? (f.sub ? ' (nearest we have, from the message)' : ' (from the message)') : '');
  function fontSelect(vm, face, value, disabled) {
    const list = fontChoices(), have = list.some(c => c.word === value), lf = vm.lineFont;
    const first = `The piece’s font${fontText(lf) ? ` (${esc(fontText(lf))})` : ''}`;
    return `<select data-su="font" data-face="${face}" aria-label="${cap1(face)} font"${disabled ? ' disabled' : ''}><option value="">${first}</option>${list.map(c => `<option value="${esc(c.word)}"${c.word === value ? ' selected' : ''}>${esc(c.word)}${c.name ? ' → ' + esc(c.name) : ''}</option>`).join('')}${value && !have ? `<option value="${esc(value)}" selected>${esc(value)}</option>` : ''}</select>`;
  }
  function viewHtml(vm, d, o) {
    const showFaces = vm.rows.length > 1 || vm.mode === 'front' || vm.edited || vm.needsPerson;
    const faces = showFaces ? `<div class="egSuFaces">${vm.rows.map(r => `<div class="egSuFace" data-face="${r.face}"><span class="fl">${r.label}</span> ${r.perPiece ? '<span class="w none">differs per piece</span>' : r.lines.length ? `<span class="w">${esc(r.text)}</span>` : '<span class="w none">words to confirm</span>'} ${r.font.word || r.font.name ? `<span class="f" title="${esc(r.font.name ? 'engraved in ' + r.font.name : '')}">${esc(fontText(r.font))}</span>` : ''}</div>`).join('')}</div>` : '';
    const ask = vm.prompt ? `<div class="egSuAsk" role="status">${esc(vm.prompt)}</div>${vm.raw ? `<div class="egSuRaw" aria-label="The buyer's own words">${esc(vm.raw)}</div>` : ''}` : '';
    const whyBits = vm.edited ? `<div class="egSuWhy">${esc(vm.why)}${vm.reading ? ` <small>The system read: ${esc(vm.reading)}</small>` : ''}</div>` : (vm.why || vm.from) ? `<div class="egSuWhy">${esc(vm.why)}${vm.from ? ` <small>${vm.why ? '· ' : ''}${esc(vm.from)}</small>` : ''}</div>` : '';
    const edit = vm.cut ? '' : `<button type="button" class="btn ghost xs" data-su="edit">${vm.needsPerson ? 'Say which' : 'Edit'}</button>`;
    return `<div class="egSuTop"><span class="egSuLbl">Engraves</span> <b class="egSuMode">${esc(vm.modeLabel)}</b> ${edit} ${statusHtml(d)}</div>${faces}${ask}${whyBits}${confirmHtml(d)}${noteHtml(vm)}`;
  }
  const statusHtml = d => (d && d.busy ? `<span class="egSuStat" role="status"><span class="owSpin" aria-hidden="true"></span>${esc(d.busy)}</span>` : '');
  const confirmHtml = d => (d && d.error ? `<div class="egSuErr" role="alert">${esc(d.error)}</div>` : '') + (d && d.confirmLines && d.confirmLines.length && d.confirm ? `<div class="egSuNote warn" role="status">${d.confirmLines.map(t => `<span>${esc(t)}</span>`).join('')}<span class="egSuActs"><button type="button" class="btn gold xs" data-su="saveYes"${d.busy ? ' disabled' : ''}>Cut it plain and save</button><button type="button" class="btn ghost xs" data-su="keep">Keep it as it is</button></span></div>` : '');
  const noteHtml = vm => { const n = NOTES.get(vm.key); return n ? `<div class="egSuNote" role="status">${esc(n)}</div>` : ''; };
  function editHtml(vm, d) {
    const dis = d.busy ? ' disabled' : '', id = 'egSu' + (++uid), next = facesOf(d.mode);
    const tabs = MODE_TABS.map(([m, t]) => `<button type="button" class="egEarTab egSuTab" data-su="mode" data-mode="${m}" aria-pressed="${d.mode === m}"${dis}><span class="egSuTabT">${t}</span></button>`).join('');
    const faces = next.map(f => {
      const cur = vm.rows.find(r => r.face === f), x = d.faces[f] || { text: '', font: '' }, locked = !!(cur && cur.locked), per = !!(cur && cur.perPiece);
      const words = per ? `<div class="egSuWhy">The words differ from piece to piece: change them on each piece's own card.</div>`
        : `<textarea id="${id}${f}" data-su="words" data-face="${f}" rows="${Math.max(1, Math.min(4, linesOf(x.text).length || 1))}" placeholder="Words for the ${f}" aria-label="Words for the ${f}"${locked || d.busy ? ' disabled' : ''}>${esc(x.text)}</textarea>`;
      return `<div class="egSuFe" data-face="${f}"><label class="fl" for="${id}${f}">${cap1(f)}</label>${words}${fontSelect(vm, f, x.font, locked || !!d.busy)}${cur && cur.decided ? `<span class="lock">${esc(cur.lockState === 'skipped' ? 'cut plain' : 'approved')}${cur.lockBy ? ' by ' + esc(cur.lockBy) : ''}: a change asks for a new approval</span>` : ''}</div>`;
    }).join('');
    const prompt = vm.needsPerson && d.mode === vm.mode ? `<div class="egSuAsk" role="status">${esc(vm.prompt)}</div>${vm.raw ? `<div class="egSuRaw" aria-label="The buyer's own words">${esc(vm.raw)}</div>` : ''}` : '';
    const patch = patchOf(vm, d), dirty = hasPatch(patch), cons = dirty ? consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup) : null;
    const lines = cons && !d.confirm ? cons.lines.filter(l => !/already (approved|approved and saved)/.test(l)) : [];
    const warn = cons && cons.blocked ? `<div class="egSuErr" role="alert">${esc(cons.blocked)}</div>` : '';
    const why = vm.why || vm.from ? `<div class="egSuWhy">${esc(vm.edited ? vm.reading ? 'The system read: ' + vm.reading : '' : vm.why)}${!vm.edited && vm.from ? ` <small>${vm.why ? '· ' : ''}${esc(vm.from)}</small>` : ''}</div>` : '';
    return `<div class="egSuTop"><span class="egSuLbl">Engraves</span><span class="egEarSwitch egMany" role="group" aria-label="What this piece engraves">${tabs}</span>${statusHtml(d)}</div><div class="egSuEdit">${prompt}${faces}</div>${why}${lines.length ? `<div class="egSuNote">${lines.map(t => `<span>${esc(t)}</span>`).join('')}</div>` : ''}${warn}${confirmHtml(d)}<div class="egSuActs"><button type="button" class="btn gold xs" data-su="save"${dirty && !d.busy && !(cons && cons.blocked) ? '' : ' disabled'}>Save</button><button type="button" class="btn ghost xs" data-su="cancel"${dis}>Cancel</button></div>`;
  }
  /** The control as a string (the line's `row`). opts: { compact, open, vm, draft } */
  function html(row, opts) {
    opts = opts || {};
    const vm = opts.vm || read(row, opts); if (!vm) return '';
    const d = opts.draft || draftOf(vm);
    if (opts.open != null && !d.touchedOpen) d.open = !!opts.open;
    return `<div class="egSetup${opts.compact ? ' egSuCompact' : ''}" data-setup data-key="${esc(vm.key)}" data-engraves="${esc(vm.mode)}" data-edited="${vm.edited ? 1 : 0}" data-open="${d.open && !vm.cut ? 1 : 0}" role="group" aria-label="What this piece engraves">${d.open && !vm.cut ? editHtml(vm, d) : viewHtml(vm, d, opts)}</div>`;
  }

  /* ── wiring ─────────────────────────────────────────────────────────────────── */
  const hostSave = (line, patch) => (root.Engrave && root.Engrave.saveSetup ? root.Engrave.saveSetup(line, patch) : Promise.resolve({ ok: false, message: 'The engraving is not loaded yet.' }));
  const resultNote = res => {
    const parts = [];
    if (res.plain && res.plain.length) parts.push(`${res.plain.length === 1 ? 'One face was' : res.plain.length + ' faces were'} cut plain`);
    if (res.removed && res.removed.length) parts.push('the placement that was still to check was removed');
    if (res.reopened && res.reopened.length) parts.push('opened again for engraving');
    if (res.pending) parts.push(`the ${(res.added || []).join(' and ') || 'new'} placement cannot be made in this version of the page yet`);
    return 'Saved' + (parts.length ? '. ' + cap1(parts.join('; ')) : '') + '.';
  };
  /** Wire the control inside `el` (which holds only html()'s output) for the line of `row`. Returns { destroy(), refresh() }. */
  function bind(el, row, handlers, opts) {
    if (!el || typeof el.addEventListener !== 'function') return { destroy() {}, refresh() {} };
    ensureCss(el.ownerDocument);
    handlers = handlers || {}; opts = opts || {};
    const line = lineOf(row);
    const H = { save: handlers.save || (p => hostSave(line, p)), jobs: handlers.jobs || (() => (root.Engrave && root.Engrave.jobsOf ? root.Engrave.jobsOf(line) : [])) };
    let dead = false;
    const me = { el, key: '', draw: null };
    const untrack = () => { if (!me.key) return; const set = LIVE.get(me.key); if (set) { set.delete(me); if (!set.size) LIVE.delete(me.key); } me.key = ''; };
    const track = key => { if (me.key === key) return; untrack(); me.key = key; let set = LIVE.get(key); if (!set) LIVE.set(key, set = new Set()); set.add(me); };
    const model = () => { const vm = read(line, { jobs: H.jobs() }); return vm ? { vm, d: draftOf(vm) } : null; };
    const focusState = () => { const a = el.ownerDocument.activeElement; if (!a || !el.contains(a) || !a.dataset || !a.dataset.su) return null; return { su: a.dataset.su, face: a.dataset.face || '', mode: a.dataset.mode || '', start: a.selectionStart, end: a.selectionEnd }; };
    const draw = () => {
      if (dead) return;
      const m = model(), fs = focusState();
      if (!m) { untrack(); el.innerHTML = ''; el.hidden = true; return; }
      track(m.vm.key); el.hidden = false;
      el.innerHTML = html(line, { vm: m.vm, draft: m.d, compact: opts.compact, open: opts.open });
      const want = fs || (m.d.focus && el.ownerDocument.activeElement === el.ownerDocument.body ? m.d.focus : null);
      if (want) {
        const sel = `[data-su="${want.su}"]${want.face ? `[data-face="${want.face}"]` : ''}${want.mode ? `[data-mode="${want.mode}"]` : ''}`, t = el.querySelector(sel);
        if (t && !t.disabled) { try { t.focus({ preventScroll: true }); if (want.start != null && t.setSelectionRange) t.setSelectionRange(want.start, want.end); } catch (_) { /* no caret to give back */ } }
      }
    };
    // a save ends on whichever slot is on screen for the line: the card that started it may have been drawn again (and this binding put away) while it waited
    const done = (m, res) => {
      const key = m.vm.key; let ok = false;
      m.d.busy = '';
      if (res && res.cancelled) { /* the name was put away: calm, nothing said */ }
      else if (!res || !res.ok) m.d.error = (res && res.message) || 'Not saved. Press Save to try again.';
      else { ok = true; DRAFTS.delete(key); NOTES.set(key, resultNote(res)); }
      const live = [...(LIVE.get(key) || [])];
      for (const b of live) { try { b.draw(); } catch (_) { /* one card that fails to draw does not stop the rest */ } }
      if (!ok) return;
      const target = !dead && el.isConnected ? el : (live.find(b => b.el && b.el.isConnected) || {}).el;
      try { if (target) target.dispatchEvent(new (target.ownerDocument.defaultView.CustomEvent)('cn-engrave-setup', { bubbles: true, detail: { key, result: res } })); } catch (_) { /* a host that does not listen */ }
      try { if (typeof opts.onSaved === 'function') opts.onSaved(res); } catch (_) { /* the host's own */ }
    };
    const run = async (m, patch) => {
      m.d.error = ''; m.d.confirm = ''; m.d.busy = 'Saving…'; NOTES.delete(m.vm.key); draw();
      let res; try { res = await H.save(patch); } catch (e) { res = { ok: false, message: `Not saved: ${e && e.message ? e.message : e}` }; }
      done(m, res);
    };
    const click = e => {
      const b = e.target && e.target.closest && e.target.closest('[data-su]'); if (!b || !el.contains(b) || b.disabled || dead) return;
      const m = model(); if (!m) return; const { vm, d } = m, a = b.dataset.su;
      if (d.busy) return;
      if (a === 'edit') { d.open = true; d.touchedOpen = true; d.confirm = ''; d.error = ''; NOTES.delete(vm.key); draw(); const w = el.querySelector('textarea:not(:disabled)'); if (w) w.focus({ preventScroll: true }); }
      else if (a === 'cancel') { DRAFTS.delete(vm.key); const d2 = draftOf(vm); d2.open = false; d2.touchedOpen = true; draw(); }
      else if (a === 'mode') {
        const mode = b.dataset.mode; if (mode === d.mode) return;
        d.mode = mode; d.confirm = ''; d.error = ''; d.dirty = true;
        for (const f of facesOf(mode)) { const x = d.faces[f] || (d.faces[f] = { text: '', font: '' }); if (!str(x.text).trim() && !vm.rows.find(r => r.face === f && r.text)) x.text = seedText(vm, f, mode); }
        // (two faces never end on the same copied words: the one that only got them by following the note from the other face gives them back; a person who wants the same words twice types them)
        if (mode === 'both') { const a = d.faces.front, b = d.faces.back, same = linesOf(a.text).join('\n') === linesOf(b.text).join('\n') && linesOf(a.text).length; if (same) { const keepFront = vm.rows.find(r => r.face === 'front' && r.text); (keepFront ? b : a).text = ''; } }
        draw();
      } else if (a === 'save' || a === 'saveYes') {
        const patch = patchOf(vm, d); if (!hasPatch(patch)) { d.open = false; d.touchedOpen = true; draw(); return; }
        const cons = consequences(vm.line, vm.jobs, nextSetupOf(vm, patch), vm.setup);
        if (cons.blocked) { d.error = cons.blocked; draw(); return; }
        if (a === 'save' && cons.plain.length) { d.confirm = 'save'; d.confirmLines = cons.lines.filter(l => /already/.test(l)); draw(); return; }
        run(m, patch);
      } else if (a === 'keep') { d.confirm = ''; d.confirmLines = null; draw(); }
    };
    const input = e => {
      const t = e.target; if (!t || !t.dataset || !t.dataset.su || dead) return;
      const m = model(); if (!m) return; const { vm, d } = m, f = t.dataset.face; if (!f) return;
      const x = d.faces[f] || (d.faces[f] = { text: '', font: '' });
      if (t.dataset.su === 'words') x.text = t.value; else if (t.dataset.su === 'font') x.font = t.value; else return;
      d.dirty = true; d.confirm = ''; d.error = '';
      const save = el.querySelector('[data-su="save"]'); if (save) { const p = patchOf(vm, d); save.disabled = !hasPatch(p); }
    };
    const key = e => {
      const t = e.target; if (!t || !t.dataset || !t.dataset.su) return;
      if (t.tagName === 'TEXTAREA' || t.tagName === 'SELECT') e.stopPropagation();   // (the card's own keys, S for "No engraving", are for the card, not for words)
      if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && t.tagName === 'TEXTAREA') { e.preventDefault(); const s = el.querySelector('[data-su="save"]'); if (s && !s.disabled) s.click(); }
      else if (e.key === 'Escape' && t.tagName === 'TEXTAREA') { e.preventDefault(); const c = el.querySelector('[data-su="cancel"]'); if (c) c.click(); }
    };
    const focusin = e => { const t = e.target; if (!t || !t.dataset || !t.dataset.su || dead) return; const m = model(); if (m) m.d.focus = { su: t.dataset.su, face: t.dataset.face || '', mode: t.dataset.mode || '', start: t.selectionStart, end: t.selectionEnd }; };
    const focusout = e => { const m = model(); if (!m || !m.d.focus) return; if (e.relatedTarget) { m.d.focus = null; return; } setTimeout(() => { if (!dead && !el.contains(el.ownerDocument.activeElement) && e.target && !e.target.isConnected === false) { const mm = model(); if (mm) mm.d.focus = null; } }, 0); };
    el.addEventListener('click', click); el.addEventListener('input', input); el.addEventListener('change', input); el.addEventListener('keydown', key); el.addEventListener('focusin', focusin); el.addEventListener('focusout', focusout);
    me.draw = draw;
    return { destroy() { dead = true; untrack(); for (const [t, f] of [['click', click], ['input', input], ['change', input], ['keydown', key], ['focusin', focusin], ['focusout', focusout]]) el.removeEventListener(t, f); }, refresh: draw, draw };
  }

  const BOUND = new WeakMap();
  /** THE ONE-LINE MOUNT. */
  function mount(slot, row, opts) {
    const none = { destroy() {}, refresh() {} };
    try {
      if (!slot || typeof slot.addEventListener !== 'function' || !lineOf(row)) return none;
      const was = BOUND.get(slot); if (was) was.destroy();
      opts = opts || {};
      const b = bind(slot, row, { save: opts.save, jobs: opts.jobs }, opts);
      BOUND.set(slot, b); b.draw();
      return b;
    } catch (e) { if (typeof console !== 'undefined') console.warn('engraving setup', e); return none; }
  }
  const wants = (row, o) => !!read(row, o);
  /** A short signature of what the control would show (hosts that cache their card draw it again when it changes). */
  const sig = (row, o) => { const vm = read(row, o); return vm ? vm.sig : ''; };

  return { mount, html, bind, read, wants, sig, setupFor, buildOverride, consequences, apply, labelled, patchOf, fontChoices, derivedReading, autoChoice, diffFromAuto, fontIdOf, DRAFTS, NOTES, MODE_TABS };
});
