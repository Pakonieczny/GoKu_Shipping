/* The FACE of an engraving (Paul, 11 Oct 2026: "this particular listing has a new ones where it has engraving on the front and back so you will need to
 * update the entire engraving process flow, including all the pop-ups ... I should be able to also verify the choices the system made in the engraving panel
 * and update them as needed"). The contract is plans/front-back-1011/CONTRACT.md; this file is the model it describes.
 *
 * Until now an engraving job was the BACK engraving of one piece of an order line (the whole line, an ear L / R, or a disc D1..Dn). A face sits next to the
 * piece: `face` is "back" or "front"; ABSENT MEANS "back" everywhere (jobs, records, seals, events) and nothing is migrated. A piece engraved on both faces has
 * two independent jobs (own words, font, fit, card, approval, seals, file, record) tied together only by the line: the line is engraved when EVERY face it
 * expects is done. A line with no `spec.engraving`, or with mode "back" and nothing said, runs the code it ran before this file existed.
 *
 *   identity    faceOf(x) · isFront(x) · jobKey(lineKey, slot, face) · parseKey(key) -> { key, lineKey, rowKey, slot, face, pieceKey } · pieceKeyOf(key | x)
 *               keyOf(x, face?, slot?) -> THE job key of (piece, face), from a job / row / side row / piece record / key (ENGLINK: one key for cards, thumbs, panel, previews, position)
 *               facesOfPiece(piece, jobs?, setup?) -> { pieceKey, slot, never, faces: [{ face, key, job, expected, state }] }   (Front first; an earring never has a Back slot)
 *               recKey(poolId, face) · parseRecKey(k) -> { poolId, face } · label(face) · title(face) · tag(face)
 *   the line    setupOf(rowOrSpec, override?, slot?) -> { mode, faces, front, back, needsPerson, source, why, edited, multi, textSource, confidence, option, pieces, slot, pieceEdited }
 *               (slot given: the words of THAT piece: engravingOverride.pieces[slot][face], else the line's; no slot: the line's, with `pieces` also carrying every person's per-piece words)
 *               modeLabel(mode) · describe(setup) · seedFor(setup, face) -> { lines, text, fontWord, how } | null (null: ask the reader (back) / a person)
 *   the plan    planFaces(setup, slots) -> [{ slot, face }]   (slot major, Front before Back) · reconcile(setup, jobs, slots) -> { add, drop, keep }
 *   pieces      groupPieces(jobs) -> [{ slot, pieceKey, label, front, back, jobs }] · pieceCount(jobs) · faceJobs(jobs, slot) -> { front, back }
 *   status      faceStatus(jobs) -> { faces, expected, done, state } · bothFacesDone(jobs, o) · bothFacesWritten(jobs, o) · combine(recs) -> { needed, state, approved, faces? }
 *   gates       approveAllGate(jobs, o) -> { ok, why }   (the same three conditions as charm-nest-engrave-rows.js approveBoth, for every job of a row, faces included)
 *   records     pickRecord(records, poolId, face) · dedupe(records) -> Map(recKey -> newest record) · recordsOf(records, poolId) -> { front, back }
 *   the panel   panelRows(setup, jobs) -> [{ face, label, words, from, state, job }]  (one row per face the line has, Front first)
 *
 * Pure: no page, no network, no clock. Loads in the page (window.CharmNestEngraveFaces) and in node. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveFaces = api;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const FRONT = 'front', BACK = 'back', FACES = [FRONT, BACK], MODES = ['none', 'front', 'back', 'both'];
  const SUFFIX = '~front';
  // (PIECES: a slot is L, R, D<n> (unit 1), <slot>.<unit> (a later unit of a line of quantity >= 2) or C<n> (copy n of such a line): charm-nest-engrave-sides.js slotInfo)
  const SLOT_SRC = 'C\\d{1,3}|(?:L|R|D\\d{1,2})(?:\\.\\d{1,3})?';
  const KEY = new RegExp('^(.*?)(?:#(' + SLOT_SRC + '))?(~front)?$');
  const STATE_ORDER = ['reclassify', 'classify', 'words', 'blocked', 'ready', 'fitting', 'review', 'approved', 'written'];
  const DECIDED = ['approved', 'written', 'skipped', 'none'];
  const str = x => String(x == null ? '' : x);
  const isDecided = s => DECIDED.includes(s);

  /* ── identity ─────────────────────────────────────────────────────────────── */
  /** "front" | "back" of a job, a record, a seal, an event or a key. Anything that does not say "front" is the back. */
  function faceOf(x) {
    if (x == null) return BACK;
    if (typeof x === 'string') return x.endsWith(SUFFIX) || x === FRONT ? FRONT : BACK;
    if (x.face === FRONT) return FRONT;
    if (x.face === BACK) return BACK;
    if (typeof x.key === 'string' && x.key.endsWith(SUFFIX)) return FRONT;
    if (x.data && x.data.face === FRONT) return FRONT;
    return BACK;
  }
  const isFront = x => faceOf(x) === FRONT;
  const label = face => (face === FRONT ? 'Front' : 'Back');
  const title = face => (face === FRONT ? 'Front engraving' : 'Back engraving');
  const tag = face => (face === FRONT ? 'FRONT' : 'BACK');
  /** THE one place a job key is built: <lineKey>[#slot][~front]. A back key is exactly what it always was. */
  function jobKey(lineKey, slot, face) { return str(lineKey) + (slot ? '#' + slot : '') + (face === FRONT ? SUFFIX : ''); }
  /** THE one place a job key is split. A key that matches nothing is a whole line: { lineKey: key, slot: null, face: 'back' }. */
  function parseKey(key) {
    const k = str(key), m = KEY.exec(k);
    const lineKey = m ? m[1] : k, slot = m && m[2] ? m[2] : null, face = m && m[3] ? FRONT : BACK;
    return { key: k, lineKey, rowKey: lineKey, slot, face, pieceKey: lineKey + (slot ? '#' + slot : '') };
  }
  const SLOT_RX = new RegExp('^(?:' + SLOT_SRC + ')$'), POOL_RX = /^(\d{4,20})_([^_]*)_(\d{1,3})$/;
  /** What a job, a line row, a side row, a piece record or a key string names: { lineKey, slot, face } (face = what x itself says; a bare line row says back). */
  function identOf(x) {
    if (x == null) return { lineKey: '', slot: null, face: BACK };
    if (typeof x === 'string') { const p = parseKey(x); return { lineKey: p.lineKey, slot: p.slot, face: p.face }; }
    const k = typeof x.key === 'string' ? parseKey(x.key) : typeof x.pieceKey === 'string' ? parseKey(x.pieceKey) : null;
    // (a piece of OrderPieces has key = its pool id and its line in `lineKey`; a side row has key = its job key and its line in parentRow; a job has rowKey)
    const lineKey = str(x.rowKey || x.lineKey || (x.parentRow && x.parentRow.key) || (k ? k.lineKey : ''));
    const slot = (SLOT_RX.test(str(x.slot)) ? x.slot : null) || (k && k.slot) || (x.side === 'L' || x.side === 'R' ? x.side : null) || null;
    return { lineKey, slot, face: faceOf(x) };
  }
  /** THE one key of an engraving: the job key of (piece, face) = Faces.jobKey(lineKey, slot, face). Everything that shows, edits, approves, previews or positions an engraving carries THIS key, so a face of one
   *  piece can never be taken for another face or another piece. `face` defaults to the face x itself says (a back for a bare line row); `slot` names the piece for a bare line row (an ear, a disc). */
  function keyOf(x, face, slot) {
    const id = identOf(x), f = face === FRONT || face === BACK ? face : id.face;
    return jobKey(id.lineKey, slot !== undefined && slot !== null ? (SLOT_RX.test(str(slot)) ? slot : null) : id.slot, f);
  }
  /** The key of the piece (its faces share it): `<lineKey>[#slot]`. Takes a key as before, or anything keyOf takes. */
  const pieceKeyOf = x => { const id = identOf(x); return id.lineKey + (id.slot ? '#' + id.slot : ''); };
  /** The key of a saved record: the poolId for a back (unchanged), "<poolId>~front" for a front. */
  const recKey = (poolId, face) => str(poolId) + (face === FRONT ? SUFFIX : '');
  function parseRecKey(k) { k = str(k); return k.endsWith(SUFFIX) ? { poolId: k.slice(0, -SUFFIX.length), face: FRONT } : { poolId: k, face: BACK }; }
  const recKeyOf = rec => recKey(rec && rec.poolId, faceOf(rec));

  /* ── the line's setup ─────────────────────────────────────────────────────── */
  const lines = v => (Array.isArray(v) ? v : str(v).split(/\r?\n/)).map(x => str(x).replace(/\s+/g, ' ').trim()).filter(Boolean);
  /** One face's words, normalised: { text, lines, fontWord } or null when there are no words. */
  function normFace(f) {
    if (f == null) return null;
    if (typeof f === 'string') f = { text: f };
    if (typeof f !== 'object') return null;
    const ls = lines(Array.isArray(f.lines) && f.lines.length ? f.lines : f.text);
    if (!ls.length) return null;
    const fw = str(f.fontWord).trim();
    return { text: ls.join('\n'), lines: ls, fontWord: fw || null };
  }
  const validMode = m => MODES.includes(m);
  /** NO EARRING IS EVER ENGRAVED (ZODNOBACK, Paul 12 Oct 2026; the one rule is charm-nest-orders.js section 3d, earringNeverBackEngraves). The intake wrote its verdict on the line's spec (spec.engraveNever:
   *  { rule, form, why, note }); everything else in the app asks THIS, never the buyer's words or the form again. `x` is a row, a spec or a bare reading. → the verdict or null. */
  function neverOf(x) {
    const row = x && typeof x === 'object' ? (x.parentRow || x) : null;
    const spec = row && row.spec && typeof row.spec === 'object' ? row.spec : row;
    const n = spec && spec.engraveNever;
    return n && typeof n === 'object' && n.rule ? n : null;
  }
  const NEVER_REASON = "Earrings are never engraved (the shop's rule)";
  /** The line's effective engraving setup: spec.engraving (the intake's reading) under the row's engravingOverride (a person's word, which always wins).
   *  `x` is a row ({ spec, engravingOverride }), a spec ({ engraving, engraveCandidate }) or a bare engraving object. No reading at all: today's setup (back when the line has
   *  anything to engrave, none otherwise). */
  function setupOf(x, override, slot) {
    const row = x && typeof x === 'object' ? x : {};
    const spec = row.spec && typeof row.spec === 'object' ? row.spec : (row.engraving !== undefined || row.engraveCandidate !== undefined ? row : null);
    // an earring is never engraved, whatever the reading says and whatever a person picked later: the pick is not in force (it is kept as it was saved, never rewritten), nothing asks
    const never = neverOf(spec ? { spec } : null);
    if (never) return { v: 1, mode: 'none', faces: [], front: null, back: null, needsPerson: false, source: 'rule', edited: false, multi: false, pieces: null, reviewKind: null, raw: '', textSource: 'none', confidence: 1, why: str(never.why) || NEVER_REASON, option: null, sides: null, by: '', at: 0, never: true, slot: slot || null, pieceEdited: false };
    let base = spec && spec.engraving && typeof spec.engraving === 'object' ? spec.engraving : (!spec && validMode(row.mode) ? row : null);
    if (base && !validMode(base.mode)) base = null;
    const ov0 = override !== undefined ? override : row.engravingOverride;
    // a person's words for ONE piece (ENGLINK, Paul 12 Oct 2026 point 6): engravingOverride.pieces[slot][face]; the line-level front / back stay the fallback for every piece without its own entry (old records have none)
    const ovp = ov0 && typeof ov0 === 'object' && ov0.pieces && typeof ov0.pieces === 'object' ? ov0.pieces : null;
    const hasPieceWords = !!ovp && Object.keys(ovp).some(sl => SLOT_RX.test(sl) && ovp[sl] && typeof ovp[sl] === 'object' && (ovp[sl].front !== undefined || ovp[sl].back !== undefined));
    const ov = ov0 && typeof ov0 === 'object' && (validMode(ov0.mode) || ov0.front !== undefined || ov0.back !== undefined || hasPieceWords) ? ov0 : null;
    const candidate = spec ? spec.engraveCandidate !== false : true;
    let mode = ov && validMode(ov.mode) ? ov.mode : base ? base.mode : (candidate ? BACK : 'none');
    // a line the intake calls "none" while a person (or the line itself) has words stays what the intake said unless a person overrides it
    const pick = face => {
      if (mode === 'none' || (mode === FRONT && face === BACK) || (mode === BACK && face === FRONT)) return null;
      const mine = ov && ov[face] !== undefined ? normFace(ov[face]) : undefined;
      if (mine !== undefined) return mine;
      return normFace(base && base[face]);
    };
    const front = pick(FRONT), back = pick(BACK);
    const faces = mode === 'both' ? [FRONT, BACK] : mode === FRONT ? [FRONT] : mode === BACK ? [BACK] : [];
    const personalWords = !!(ov && (ov.front !== undefined || ov.back !== undefined));
    // words that differ from one ear / disc to the next ({ L: { front, back }, R: ... }): kept as the intake read them, until a person gives the line words of their own; a person's words for one piece lay over them
    let pieces = null;
    if (base && base.pieces && typeof base.pieces === 'object' && !personalWords && mode !== 'none') {
      for (const sl of Object.keys(base.pieces)) { const q = base.pieces[sl]; if (!q || typeof q !== 'object') continue; const f = mode === BACK ? null : normFace(q.front), b = mode === FRONT ? null : normFace(q.back); if (f || b) (pieces = pieces || {})[sl] = { front: f, back: b }; }
    }
    if (hasPieceWords && mode !== 'none') {
      for (const sl of Object.keys(ovp)) {
        const q = ovp[sl]; if (!SLOT_RX.test(sl) || !q || typeof q !== 'object') continue;
        // (a piece's own words keep the face's font word when the person named none for the piece: editing the words of disc 2 does not reset the font the face was given)
        const inh = (n, line) => (n && !n.fontWord && line && line.fontWord ? Object.assign({}, n, { fontWord: line.fontWord }) : n);
        const f = mode === BACK ? null : inh(normFace(q.front), front), b = mode === FRONT ? null : inh(normFace(q.back), back), was = pieces && pieces[sl];
        if (f || b) (pieces = pieces || {})[sl] = { front: f || (was && was.front) || null, back: b || (was && was.back) || null };
      }
    }
    // the piece asked for: its own words, else the line's (a face the piece has none for keeps the line's words)
    const sl0 = slot && SLOT_RX.test(str(slot)) ? slot : null, own = sl0 && pieces ? pieces[sl0] : null;
    const pieceEdited = !!(sl0 && hasPieceWords && ovp[sl0] && typeof ovp[sl0] === 'object' && (ovp[sl0].front !== undefined || ovp[sl0].back !== undefined));
    const frontS = (own && own.front) || front, backS = (own && own.back) || back;
    const lacking = faces.some(f => !(f === FRONT ? frontS : backS));
    // a reading the intake marked for a person (an unlabelled "both", a conflict, one set of words for each ear) stays so until a person speaks; a person who changed only the mode keeps the flag for a face still without words;
    // for ONE piece it clears when that piece (or the whole line) has words from a person
    const spoken = personalWords || pieceEdited;
    const needsPerson = !!(base && base.needsPerson) && !spoken && (!ov || lacking || (hasPieceWords && !personalWords)) ? true : false;   // (another piece's words from a person do not settle this piece's question)
    const source = ov ? 'person' : base ? str(base.source) || 'default' : 'default';
    return { v: 1, mode, faces, front: frontS, back: backS, needsPerson, source, edited: !!ov, multi: faces.length > 1, pieces, reviewKind: base && base.reviewKind ? str(base.reviewKind) : null, raw: base && base.raw ? str(base.raw) : '',
      textSource: base ? str(base.textSource) || 'none' : 'none', confidence: base && Number.isFinite(+base.confidence) ? +base.confidence : (base ? 0.5 : 1),
      why: ov ? `Set by ${str(ov.by) || 'a person'}` : base ? str(base.why) : '', option: base && base.option ? base.option : null, sides: base && base.sides && typeof base.sides === 'object' ? base.sides : null, by: ov ? str(ov.by) : '', at: ov ? +ov.at || 0 : 0, slot: sl0, pieceEdited };
  }
  const modeLabel = mode => ({ none: 'No engraving', front: 'Front only', back: 'Back only', both: 'Front + back' }[mode] || '');
  const joinWords = f => (f && f.lines ? f.lines.join(' / ') : '');
  /** "Front: W · Back: 1. 27. 24" (a face that has no words yet says so). */
  function describe(setup) {
    if (!setup || !setup.faces.length) return modeLabel('none');
    if (setup.pieces) return Object.keys(setup.pieces).map(sl => `${slotLabel(sl) || sl} (${setup.faces.map(f => `${label(f)}: ${joinWords(setup.pieces[sl][f]) || 'words to confirm'}`).join(' · ')})`).join(' · ');
    return setup.faces.map(f => `${label(f)}: ${joinWords(setup[f]) || 'words to confirm'}`).join(' · ');
  }
  /** What the intake wants a person to look at on this line, or null: { kind: "nowords" | "texts" | "conflict" | "mode" | "pieces" | "review", why, raw, mode }. A person's own words for the line clear it (setupOf). The panel shows `why`
   *  (and `raw`, the buyer's own words) even for a line whose mode is "none" (a blank option with words written), which has no face to hang it on. */
  function review(setup) { return setup && setup.needsPerson ? { kind: setup.reviewKind || 'review', why: setup.why || '', raw: setup.raw || '', mode: setup.mode } : null; }
  /** The words a face job starts with, or null. null means: the reader decides (a back with nothing from the intake: today's path) or a person does (a face the intake could not fill, or a front). */
  function seedFor(setup, face, slot) {
    if (!setup || !setup.faces.includes(face)) return null;
    const own = slot && setup.pieces && setup.pieces[slot] ? setup.pieces[slot][face] : null;   // (words the buyer gave for THIS ear / disc)
    const w = own && own.lines && own.lines.length ? own : setup[face];
    if (!w || !w.lines.length) return null;
    // the intake saying "back" with nothing but its own default is the old reading: the reader (and its paid call) decides, as it always did
    if (face === BACK && setup.source === 'default' && !setup.edited) return null;
    return { lines: w.lines.slice(), text: w.text, fontWord: w.fontWord, how: setup.edited ? 'person' : (setup.textSource || 'labelled'), source: setup.edited ? 'person' : setup.source, confidence: setup.edited ? 1 : setup.confidence };
  }

  /* ── the plan: which (piece, face) jobs a line has ───────────────────────── */
  const SLOT_PARTS = /^(?:C(\d{1,3})|(L|R|D(\d{1,2}))(?:\.(\d{1,3}))?)$/;
  /** unit-major place of a slot: Left 0, Right 1, Disc n 1+n, then the same again 100 on for each later unit; a copy slot is its unit; no slot first, an unknown one last (the same ranks as Sides.slotRank). */
  const slotOrder = s => { const m = SLOT_PARTS.exec(str(s)); if (!m) return s ? 9999 : -1; if (m[1]) return (+m[1] - 1) * 100; return ((m[4] ? +m[4] : 1) - 1) * 100 + (m[3] ? 1 + (+m[3]) : m[2] === 'L' ? 0 : 1); };
  const faceOrder = f => (f === FRONT ? 0 : 1);
  const byPlan = (a, b) => slotOrder(a.slot) - slotOrder(b.slot) || faceOrder(a.face) - faceOrder(b.face);
  /** The jobs a line should have: every slot, every face its setup asks for (slot major, Front before Back). */
  function planFaces(setup, slots) {
    const sl = slots && slots.length ? slots : [null], faces = setup && setup.faces && setup.faces.length ? setup.faces : [BACK];
    return sl.flatMap(slot => faces.map(face => ({ slot: slot || null, face }))).sort(byPlan);
  }
  /** What a change of setup does to the jobs a line already has: `add` = (slot, face) pairs without a job; `drop` = existing jobs of a face the setup no longer asks for that nobody has decided
   *  (removed); `keep` = existing jobs of such a face that a person decided (approved, written, skipped, none): they stay, with their seals and files, and count in the line's verdict. */
  function reconcile(setup, jobs, slots) {
    const have = (jobs || []).filter(Boolean), slotList = [...new Set([...(slots && slots.length ? slots : [null]), ...have.map(j => parseKey(j.key).slot || j.slot || null)].map(s => s || null))];
    const want = planFaces(setup, slotList), id = (slot, face) => (slot || '') + '|' + face;
    const wantIds = new Set(want.map(w => id(w.slot, w.face))), haveIds = new Set(have.map(j => id(j.slot || parseKey(j.key).slot, faceOf(j))));
    const add = want.filter(w => !haveIds.has(id(w.slot, w.face)));
    const extra = have.filter(j => !wantIds.has(id(j.slot || parseKey(j.key).slot, faceOf(j))));
    return { add, drop: extra.filter(j => !isDecided(j.state)), keep: extra.filter(j => isDecided(j.state)) };
  }

  /* ── pieces and their faces ──────────────────────────────────────────────── */
  const slotOfJob = j => (j && (j.slot || parseKey(j.key).slot)) || null;
  /** The short name of a slot ("Left", "Disc 2", "Piece 2"; a later unit says so: "Left · pair 2", "Disc 2 · set 2"): the same words as Sides.labelOf without a shape. */
  const slotLabel = s => {
    const m = SLOT_PARTS.exec(str(s)); if (!m) return '';
    if (m[1]) return 'Piece ' + (+m[1]);
    const unit = m[4] ? +m[4] : 1, base = m[3] ? 'Disc ' + (+m[3]) : m[2] === 'L' ? 'Left' : 'Right';
    return unit > 1 ? base + ' · ' + (m[3] ? 'set ' : 'pair ') + unit : base;
  };
  /** How many pieces a list of jobs covers (distinct slots; a line with no slot is one piece): the number "Disc 2 of 3" and "Approve all discs" count, never jobs.length. */
  function pieceCount(jobs) { return new Set((jobs || []).filter(Boolean).map(slotOfJob)).size; }
  /** The jobs of ONE piece by face. */
  function faceJobs(jobs, slot) {
    const out = { front: null, back: null };
    for (const j of jobs || []) if (j && (slotOfJob(j) || null) === (slot || null)) out[faceOf(j)] = j;
    return out;
  }
  /** The jobs of one line grouped by piece, in piece order; each group has its Front and Back job (or null). */
  function groupPieces(jobs) {
    const by = new Map();
    for (const j of (jobs || []).filter(Boolean)) {
      const slot = slotOfJob(j), k = slot || '';
      if (!by.has(k)) by.set(k, { slot, pieceKey: parseKey(j.key).pieceKey, label: slotLabel(slot), front: null, back: null, jobs: [], faces: [] });
      const g = by.get(k), f = faceOf(j); g[f] = j;
    }
    const out = [...by.values()].sort((a, b) => slotOrder(a.slot) - slotOrder(b.slot));
    for (const g of out) { g.jobs = [g.front, g.back].filter(Boolean); g.faces = [g.front && FRONT, g.back && BACK].filter(Boolean); }
    return out;
  }

  /** The jobs of the page (the Engrave module's), for a caller that hands none: nothing in node. */
  const pageJobs = () => { try { const g = typeof self !== 'undefined' ? self : globalThis, E = g && g.Engrave; return E && E.items ? [...E.items().values()] : []; } catch (_) { return []; } };
  /** The faces of ONE piece (ENGLINK): what a card shows as its Front and Back slots, each with the key every handler carries. `piece`: a piece key (`<lineKey>[#slot]`), a pool id (`<line>_<n>`: found through the
   *  copies a job holds), a job, a row / side row, or { lineKey, slot }. `jobs`: the page's jobs (default: Engrave.items()). `setup`: the line's setupOf (marks a face the setup asks for before its job exists).
   *  → { pieceKey, slot, never, faces: [{ face, key, job|null, expected, state }], legacy: [jobs hidden by the earring rule] }, Front first.
   *  A normal piece shows both slots (a face nobody engraves is a quiet empty slot: expected false). An earring (spec.engraveNever) never has a Back slot and nothing is expected on it; a Front slot is shown only
   *  for a front job that already exists (a decided legacy one); a back job it still holds is listed under `legacy`, never as a slot. The copies of a plain quantity-N line share one job: one slot, `job.copies` says how many. */
  function facesOfPiece(piece, jobs, setup) {
    const list = (Array.isArray(jobs) ? jobs : pageJobs()).filter(j => j && !j.editingBack);
    let mine = [], pk = '', slot = null;
    if (typeof piece === 'string' && POOL_RX.test(piece)) {
      mine = list.filter(j => (j.copies || []).includes(piece));
      if (mine.length) { const p0 = parseKey(mine[0].key); pk = p0.pieceKey; slot = p0.slot; mine = list.filter(j => parseKey(j.key).pieceKey === pk); }
    } else {
      const id = identOf(piece); pk = id.lineKey + (id.slot ? '#' + id.slot : ''); slot = id.slot;
      mine = list.filter(j => parseKey(j.key).pieceKey === pk);
    }
    const rowOf = piece && typeof piece === 'object' ? piece : (mine[0] && mine[0].row) || null;
    const never = !!(neverOf(rowOf) || mine.some(j => neverOf(j.row)));
    const lineKey = pk ? parseKey(pk).lineKey : '';
    const faces = [], legacy = [];
    for (const f of FACES) {
      const job = mine.find(j => faceOf(j) === f) || null;
      if (never && (f === BACK || !job)) { if (job) legacy.push(job); continue; }
      const expected = !never && ((!!job && job.state !== 'none') || !!(setup && setup.faces && setup.faces.includes(f)));
      faces.push({ face: f, key: jobKey(lineKey, slot, f), job, expected, state: job ? str(job.state) : '' });
    }
    return { pieceKey: pk, slot, never, faces, legacy };
  }

  /* ── status ─────────────────────────────────────────────────────────────── */
  const rank = s => { const i = STATE_ORDER.indexOf(s); return i < 0 ? 99 : i; };
  /** Where one piece stands on each face: { faces: { front: state|null, back: state|null }, expected: ['front','back'] present, done: every present job decided, state: the least advanced }. */
  function faceStatus(jobs) {
    const faces = { front: null, back: null }, present = [];
    for (const j of (jobs || []).filter(Boolean)) { const f = faceOf(j); faces[f] = str(j.state) || null; if (!present.includes(f)) present.push(f); }
    const expected = FACES.filter(f => present.includes(f));
    const open = expected.map(f => faces[f]).filter(s => !isDecided(s));
    const state = open.length ? open.slice().sort((a, b) => rank(a) - rank(b))[0] : expected.length && expected.every(f => faces[f] === 'none') ? 'none' : expected.length && expected.every(f => ['none', 'skipped'].includes(faces[f])) ? 'skipped' : expected.length ? 'approved' : '';
    return { faces, expected, done: expected.length > 0 && !open.length, state };
  }
  /** Every face the piece expects is decided (approved, written, skipped or none). `o.expected`: the faces the line's setup asks for, so a face whose job is not made yet counts as not done. */
  function bothFacesDone(jobs, o) {
    const st = faceStatus(jobs), expected = o && Array.isArray(o.expected) && o.expected.length ? o.expected : st.expected;
    return expected.length > 0 && expected.every(f => isDecided(st.faces[f]));
  }
  /** Every face the piece expects has its file saved ('written') or needs none ('skipped', 'none'): the piece may be marked engraved. */
  function bothFacesWritten(jobs, o) {
    const st = faceStatus(jobs), expected = o && Array.isArray(o.expected) && o.expected.length ? o.expected : st.expected;
    return expected.length > 0 && expected.every(f => ['written', 'skipped', 'none'].includes(st.faces[f]));
  }
  /** The one decision of a piece from its faces' decisions [{ face, needed, state, approved }]: the old three fields (so every old reader stays right) and, only when a front is there, `faces`. */
  function combine(recs) {
    const list = (recs || []).filter(Boolean);
    if (!list.length) return undefined;
    const settled = s => s === 'none' || s === 'skipped';
    const need = list.filter(r => !settled(r.state) && r.needed !== false);
    const out = { needed: list.some(r => !!r.needed) };
    out.state = need.length ? need.map(r => r.state).sort((a, b) => rank(a) - rank(b))[0] : list.every(r => r.state === 'none') ? 'none' : 'skipped';
    out.approved = list.every(r => !!r.approved);
    if (list.some(r => faceOf(r) === FRONT)) {
      out.faces = {};
      for (const r of list) out.faces[faceOf(r)] = { needed: !!r.needed, state: r.state, approved: !!r.approved };
    }
    return out;
  }

  /* ── one press for every job of a row ───────────────────────────────────── */
  const words = j => ((j && j.lines) || []).map(s => str(s).trim()).filter(Boolean).join(' / ');
  /** "Approve both ears" / "Approve all discs" for a row whose jobs may have two faces: every job must have a placement to check, nothing running, the same words on the pieces of a face (a
   *  person never approves words that differ without looking at them) and its placement put in front of the person (o.shown). Different faces carry different words by nature: the "same words"
   *  rule is per face. `o`: { busy(job), shown(job), settling(job) }. */
  function approveAllGate(jobs, o) {
    o = o || {};
    const list = (jobs || []).filter(Boolean);
    if (list.length < 2) return { ok: false, why: 'This piece has one placement to approve.' };
    const name = j => (slotLabel(slotOfJob(j)) ? slotLabel(slotOfJob(j)) + ' ' : '') + label(faceOf(j)).toLowerCase();
    if (!list.every(j => j.state === 'review' && j.fit && j.verify && j.verify.geometry && j.verify.geometry.ok)) return { ok: false, why: 'Every placement must be ready to check first.' };
    if (list.some(j => j._approvalTask || j._backTask || j.approvalPreparing || j.stamping || j.backSaving || (o.busy && o.busy(j)))) return { ok: false, why: 'A placement is still being prepared or approved.' };
    for (const f of FACES) {
      const of = list.filter(j => faceOf(j) === f);
      if (of.length > 1) { const w = words(of[0]); if (!w || !of.every(j => words(j) === w)) return { ok: false, why: `The ${label(f).toLowerCase()} words differ between the pieces: approve each one on its own.` }; }
    }
    for (const j of list) if (o.shown && !o.shown(j)) return { ok: false, why: `Look at the ${name(j)} placement first (it is not on screen yet).` };
    return { ok: true, why: '' };
  }

  /* ── saved records (a backPool entry / a Charm_Pool_Back doc) ───────────── */
  /** The newest record of each (piece, face): a Map keyed by recKey. Invalidated records are left out. */
  function dedupe(records) {
    const found = new Map();
    for (const b of records || []) {
      if (!b || !b.poolId || b.invalidated) continue;
      const k = recKeyOf(b), prev = found.get(k);
      if (!prev || (+b.approvedAt || 0) >= (+prev.approvedAt || 0)) found.set(k, b);
    }
    return found;
  }
  function pickRecord(records, poolId, face) {
    let best = null;
    for (const b of records || []) if (b && b.poolId === poolId && !b.invalidated && faceOf(b) === (face === FRONT ? FRONT : BACK) && (!best || (+b.approvedAt || 0) >= (+best.approvedAt || 0))) best = b;
    return best;
  }
  const recordsOf = (records, poolId) => ({ front: pickRecord(records, poolId, FRONT), back: pickRecord(records, poolId, BACK) });

  /* ── what the panel shows: one row per face the line has ────────────────── */
  /** [{ face, label, title, words, from, state, job }] Front first. `from`: "person" (a person's words or decision), "order" (the buyer's own labelled words), "reader" (the paid reading),
   *  "unread" (nothing yet). The words are the job's when it has them, else the setup's. */
  function panelRows(setup, jobs) {
    const out = [], by = {};
    for (const j of jobs || []) if (j) by[faceOf(j)] = j;
    for (const f of FACES) {
      const job = by[f] || null, want = setup && setup.faces.includes(f);
      if (!job && !want) continue;
      const w = job && words(job) ? words(job) : joinWords(setup && setup[f]);
      const how = job && job.decision && job.decision.by ? 'person' : job && job.wordsSource === 'spec.engraving' ? (setup && setup.edited ? 'person' : 'order') : job && (job.source === 'personalization' || job.claudeJob || job.confidence != null) && job.text ? 'reader' : w ? (setup && setup.edited ? 'person' : 'order') : 'unread';
      out.push({ face: f, label: label(f), title: title(f), words: w, from: how, state: job ? job.state : '', job });
    }
    return out;
  }

  return { FRONT, BACK, FACES, MODES, SUFFIX, STATE_ORDER, DECIDED, faceOf, isFront, label, title, tag, jobKey, parseKey, pieceKeyOf, recKey, parseRecKey, recKeyOf,
    normFace, setupOf, modeLabel, describe, seedFor, planFaces, reconcile, pieceCount, faceJobs, groupPieces, slotLabel, faceStatus, bothFacesDone, bothFacesWritten, combine,
    approveAllGate, dedupe, pickRecord, recordsOf, panelRows, isDecided, review, neverOf, NEVER_REASON, keyOf, facesOfPiece };
});
