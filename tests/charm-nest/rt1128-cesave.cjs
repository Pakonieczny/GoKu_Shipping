/* CESAVE1128 (Paul, 28 Nov 2026: "This should never ever be allowed to happen. A user never added a second text in the charm editor ... the user changed the same engraving that was already there
   using the charm editor"). Order 4242919793, piece 4242919793_5306158681_1 (SKINNY_2952) on GF_Nov.23.26_Set-1_Sheet-1: its front "Fiona" approved 22 Nov (Michelle_R); 26 Nov 18:31 Ana_M saved
   the piece in the Charm Editor and its artwork record got added [{ id: "cemwxw1q90b", kind: "path", role: "hatch", d: <Fiona's outlines> }] (no words, an id the editor made at 18:30:57, nine
   seconds before the save), the front job untouched: the sheet and its .ai drew Fiona twice.
   The bridge's own open and save (charm-nest-bridge.js, its Charm Editor block run as it stands, over stubs; the real editor model, charm-nest-ce-ops.js, charm-nest-ce-words.js and the real
   CharmNestArtEdit.encode, which makes the record the cloud keeps), with a person in the editor doing what the editor's own commands do. Node only; nothing is sent anywhere.
     A  every way into the editor for a placed piece hands the face's approved engraving in as ONE Type linked to its job: the engraving card's Open in Charm Editor; the order window's Open in
        Charm Editor / Edit Design and the charm picked on its Sheet tab (both { row, poolId, from: "orderWindow" }); a written face the upkeep shelved; a job that names no piece yet (a line
        this computer did not pool). The sheet window has no door of its own for a plain piece (it goes to the engraving card or the order window)
     R  THE CAUSE, as it happened: the words handed in, Offset Path's "Add new path" (the editor's default; the words stay, their outlines grown go behind them under a new id): the Save must not
        write that copy into the drawing (it did: one added part, kind path, role hatch, a "ce…" id, no words, the job untouched: the record of 26 Nov), and the engraving keeps its words
     B  the words retyped and saved: the job's words change (Engrave.saveSetup), nothing is added to the drawing; also from the job that named no piece
     C  Save with nothing changed: nothing written, nothing changed
     D  the real case afterwards: the piece's art holds Fiona's outlines with no words (cemwxw1q90b) beside the approved front: opened and saved, ONE copy is left (the engraving): the copy is
        not handed to the editor and the Save takes it out of the record; outlines of OTHER words, and a part that is not exactly the words, are kept
     E  words the person really adds (a new Type, its own words) are kept as drawn; a copy of the words retyped with other words is the person's own and kept
     H  ONE editor, centrally connected (CEONE; the owner 28 Nov 2026: "Why isn't there just one editor ... it doesn't matter where I open it from" · "You need to standardize the editor ... exactly
        the same for every case scenario" · "amalgamate all the connections ... all the repository saves all the charm saves everything"): every entry opens through CharmEditorHub.open (by
        what is opened: a piece, a repository charm, a design, a new design), every editor is CharmEditorHub.editor, every Save CharmEditorHub.save; the same piece from the card, the order
        window and its Sheet tab gets the same open, the same editor (content and controls) and the same outputs of Save (a piece of two copies, with a front and a back job); the controls are
        the same for every content but what the content decides, never where it was opened from: the repository charm's greyed Save (the owner: "you're just greying out a button") and the cut
        line locked at open for a placed piece or a custom order's drawing, free for a repository charm, a design and a new design
     F  the page loads the changed files under new tokens (-ce1)
   node tests/charm-nest/rt1128-cesave.cjs */
'use strict';
const fs = require('fs'), path = require('path'), vm = require('vm');
const ROOT = path.join(__dirname, '../..');
let pass = 0, fail = 0; const ok = (c, m, d) => { if (c) { pass++; console.log('  ok   ' + m); } else { fail++; console.log('  FAIL ' + m + (d === undefined ? '' : ' :: ' + JSON.stringify(d).slice(0, 700))); } };
const CE = require(path.join(ROOT, 'charm-nest-charm-editor.js')), M = CE.model;
const W = require(path.join(ROOT, 'charm-nest-ce-words.js'));
const AE = require(path.join(ROOT, 'charm-nest-art-edit.js'));
const OPS = require(path.join(ROOT, 'charm-nest-ce-ops.js'));
const BRIDGE = fs.readFileSync(path.join(ROOT, 'charm-nest-bridge.js'), 'utf8');
const SRC = BRIDGE.split('\n');
const A0 = SRC.findIndex(l => l.startsWith('  const CHARM_EDIT_ICON = ')), A1 = SRC.findIndex(l => l.startsWith('  function charmEditLog(kind, data)'));
const BLOCK = SRC.slice(A0, A1 + 1).join('\n');
const I = [1, 0, 0, 1, 0, 0], POOL = '4242919793_5306158681_1', POOL2 = '4242919793_5306158681_2', SHEET = { sheetId: 'd-1ff008d1c436435d', fileBase: 'GF_Nov.23.26_Set-1_Sheet-1' };
const engine = (t, k, s, o) => CE.builtinTextToPaths(t, k, s, o);   // (the font engine: the editor's own stand-in, the same for the Type, its copies and the comparison)
const rect = (x0, y0, x1, y1) => [[['m', [x0, y0]], ['l', [x1, y0]], ['l', [x1, y1]], ['l', [x0, y1]], ['h']]];
const toSource = subs => subs.map(sp => sp.map(op => (op[0] === 'h' ? ['h'] : [op[0]].concat(op.slice(1).map(p => [p[0], -p[1]])))));   // (the identity view: an item point is (x, -y) of the charm's)

/** The page around the Charm Editor block: one SKINNY bar on its sheet, its front engraved "Fiona" (approved); o: { copies, rowPoolIds, shelved, artFront, stamp } */
function world(o) {
  o = o || {};
  const outline = { kind: 'path', subpaths: rect(0, 0, 80, 20), stroke: true, strokeRGB: [0, 0, 0], fill: false, artRole: 'cut' };
  const charm = { id: 'c1', poolId: POOL, name: 'SKINNY_2952', outline, members: [outline], bbox: [0, 0, 80, 20] };
  if (o.artFront) { charm.artFront = o.artFront; charm.artEdit = { front: { at: o.stamp || 1795717866216, key: 'k1' } }; }
  const view = { M: I.slice(), R: I.slice(), face: 'front', upAngle: 0 };
  const row = { key: 'L4242919793', order: { receiptId: '4242919793' }, line: { sku: 'SKINNY_2952', transactionId: '5306158681' }, spec: { designSku: 'SKINNY_2952' }, poolIds: o.rowPoolIds || (o.two ? [POOL, POOL2] : [POOL]) };
  const lines = ['Fiona'], fit = () => ({ lines, size: 7, angle: 0, centre: [40, 10], weight: 'Regular', glyphs: [{ cmds: [] }] });
  const job = { key: 'front:L4242919793', face: 'front', row, state: o.shelved ? 'written' : 'approved', copies: o.copies || (o.two ? [POOL, POOL2] : [POOL]), lines: lines.slice(), lineInput: lines.slice(), text: 'Fiona', fontKey: 'ace',
    approvedBy: 'Michelle_R', approvedAt: 1795461992819, fit: o.shelved ? null : fit(), view: o.shelved ? null : view, writtenFit: o.shelved ? Object.assign(fit(), { approvedAt: 1795461992819 }) : null };
  const jobs = new Map([[job.key, job]]), saved = [], setups = [], opened = [], told = [];
  if (o.back) jobs.set('back:L4242919793', { key: 'back:L4242919793', row, state: 'none', copies: job.copies.slice(), lines: [], lineInput: [], text: '', fontKey: 'ace', fit: null, view: null, writtenFit: null });   // (a back job with no words: the piece has two faces' jobs)
  const font = { id: 'ace' };
  const toCmds = subs => { const out = []; for (const sp of subs) for (const op of sp) { if (op[0] === 'm') out.push({ type: 'M', x: op[1][0], y: -op[1][1] }); else if (op[0] === 'l') out.push({ type: 'L', x: op[1][0], y: -op[1][1] }); else if (op[0] === 'h') out.push({ type: 'Z' }); } return out; };
  const G = {
    capPerEm: () => 0.7, pairedUp: u => u, viewOptionsFor: () => ({}), backView: () => ({ M: I.slice(), R: I.slice(), face: 'front', upAngle: 0 }),
    layoutLines: (ls, f, s) => { const r = engine(ls.join('\n'), 'ace', s, { align: 'center' }), bb = M.bboxOf(r.subpaths); return { cmds: toCmds(M.transformSubpaths(r.subpaths, [1, 0, 0, 1, -(bb[0] + bb[2]) / 2, -(bb[1] + bb[3]) / 2])) }; }
  };
  const ctx = {
    console: { log() {}, warn() {}, error() {} }, Promise, Map, Set, WeakMap, WeakSet, Object, Array, JSON, Math, Date, String, Number, Boolean, Error, RegExp, Symbol, parseInt, parseFloat, isFinite, setTimeout, clearTimeout, Uint8Array,
    CustomEvent: class { constructor(t, d) { this.type = t; this.detail = d && d.detail; } },
    document: { querySelectorAll: () => [], querySelector: () => null, dispatchEvent() {}, getElementById: () => null, head: { appendChild() {} } },
    CharmNestCeWords: W,
    CharmNestCeText: { textToPaths: engine, prepare: async () => ({ fonts: [{ key: 'ace', name: 'Ace' }] }), fonts: async () => [{ key: 'ace', name: 'Ace' }] },
    // (CharmNestArtEdit as it is, but its cloud: a save is the record encode makes, kept here)
    CharmNestArtEdit: Object.assign({}, AE, { sync: async () => 0, load: async () => {}, piece: async c => c,
      save: async (pid, face, changes, c) => { const art = AE.encode(changes, { charm: c.charm, view: c.view, face, placed: c.placed }); saved.push({ pid, face, art }); return { record: art }; } }),
    G, O: { DEFAULT_FONT: 'ace', FONT_WORDS: [], fontById: () => null, fontOfPhrase: () => '' }, MM: 25.4 / 72, PT: 72 / 25.4,
    Pool: { charmOf: id => (id === POOL || id === POOL2 ? charm : null), sheetOf: id => (id === POOL || id === POOL2 ? SHEET : null), previewCharmOf: () => null, sizeEntry: () => null },
    B: { pool: { rows: new Map([[POOL, { poolId: POOL, sheetId: SHEET.sheetId, engraveFront: true, artEdit: o.artFront ? { front: o.stamp || 1795717866216 } : null }]]) } },
    Master: { entryFor: () => null, fetchEntry: async () => null },
    items: () => jobs, lineOf: r => (r && r.parentRow) || r, SIDES: () => ({ slotOfId: () => null }), sidesCtx: {},
    jobsOfPiece: id => { const out = { front: null, back: null }; for (const j of jobs.values()) if (!j.editingBack && (j.copies || []).includes(id)) { const f = j.face === 'front' ? 'front' : 'back'; out[f] = out[f] || j; } return out; },
    jobsOf: r => [...jobs.values()].filter(j => j.row && r && j.row.key === r.key),
    faceOf: j => (j && j.face === 'front' ? 'front' : 'back'), isFront: j => !!j && j.face === 'front', otherFaceOf: j => (j.face === 'front' ? 'back' : 'front'), viewFaceOf: j => (j.face === 'front' ? 'front' : 'back'),
    fontIdOf: () => 'ace', setOf: () => ({ Regular: font }), fontFor: () => font, fitOpts: () => ({ lineGap: 0.216 }), fontSetOf: () => ({ Regular: font }), loadFontSet: async () => {}, loadFonts: async () => {}, F_: { ok: true, Regular: font },
    engravedWords: j => ((j && (j.lineInput && j.lineInput.length ? j.lineInput : j.lines)) || []).map(s => String(s).trim()).filter(Boolean),
    listState: j => (j && j.keptState) || (j && j.state), reviveShelved: () => {}, recalledJob: () => false,   // (a shelved face this page cannot build again: it keeps its writtenFit)
    employeeName: () => 'Ana_M', needEmployee: async () => 'Ana_M', decidedBusy: () => false, charmFor: () => charm,
    faceHasEditorArt: (j, f) => !!((f || 'back') === 'front' ? charm.artFront && charm.artFront.length : charm.artBack && charm.artBack.length),
    saveSetup: async (r, patch) => { setups.push(patch); return { ok: true }; },
    fitTasks: new WeakMap(), startEdit: () => true, refit: () => true, refresh: () => {}, Session: { schedule() {} }, fitJob: async () => {}, artOnlyPlacement: () => false,
    pieceKeyOfJob: j => (j && j.row ? j.row.key : ''), svStageNow: () => null, svNorm: s => s, svChoiceFaces: c => (c === 'both' ? ['front', 'back'] : c ? [c] : []), draftOf: () => null, svPlaceOf: () => null,
    api: async () => ({ ok: true }), toast: t => told.push(t), agent: () => {}, render: () => {}, Review: { items: () => [], remove() {}, render() {} },
    setViewFace: () => {}, FRONT_ART: new Map(), Orders: { rows: () => [] }, sheetName: () => '', allSheets: () => [], renderCard: () => {}, ListMedia: { refresh() {} },
    S: { mode: 'engrave' }, EG: {}, setMode: () => {}, focusOn: () => {}, svPieceKeyOf: k => k, svCardsOf: () => ({ on: [] }), SV_STAGES: new Map(), svShowStage: () => {}
  };
  ctx.window = ctx; ctx.self = ctx;
  ctx.CharmEditor = { model: M, current: null, release() {}, raise() {},
    open: async opts => { opened.push(opts); return opened.length > 1 ? null : o.person ? o.person(opts) : null; } };   // (a second open is a save refused: put away, said by the test)
  vm.createContext(ctx);
  const api = vm.runInContext(`(function(){\n${BLOCK}\n;return { openCharmEditor };})()`, ctx, { filename: 'charm-nest-bridge.js (Charm Editor block)' });
  return { api, ctx, hub: ctx.CharmEditorHub || null, charm, job, row, jobs, saved, setups, opened, told, view };
}
/** The person in the editor: the faces as the editor keeps them (prepIn), `act` changes them as the editor's commands do, then the editor's own result (exportItems, diffFace) and its check. */
function person(act, seen) {
  return async opts => {
    const geo = M.createGeo(opts.textToPaths), S = {};
    for (const f of Object.keys(opts.faces)) { const base = M.prepIn(opts.faces[f].items, opts.lockRoles); S[f] = { base, items: base, docIn: opts.faces[f] }; }
    if (seen) seen.front = S.front.items.slice();
    if (act) act(S, geo);
    const faces = {}, changes = {};
    for (const f of Object.keys(S)) { const out = M.exportItems(S[f].items, geo); faces[f] = Object.assign({}, S[f].docIn, { face: f, items: out }); changes[f] = M.diffFace(M.exportItems(S[f].base, geo), out); }
    const r = { faces, changes };
    if (opts.check) { const no = await opts.check(r); if (no) r.refusedBy = no; }
    return r;
  };
}
const wordsOf = items => (items || []).filter(i => i && i.kind === 'text' && i.source === 'engrave');
const brief = it => it && { id: it.id, kind: it.kind, text: it.text, source: it.source, engraving: it.engraving };
const linked = (w, items) => { const t = wordsOf(items); return t.length === 1 && t[0].text === 'Fiona' && !!t[0].engraving && t[0].engraving.job === w.job.key && t[0].engraving.face === 'front'; };
const addedOf = w => [].concat(...w.saved.filter(s => s.face === 'front').map(s => s.art.added || []));
const WAYS = [['the engraving card\'s Open in Charm Editor', 'card'], ['the order window\'s Open in Charm Editor / Edit Design', 'ow']];
const door = (w, k) => (k === 'card' ? w.job : { row: w.row, poolId: POOL, from: 'orderWindow' });
/** An entry as its code calls the editor: through the one hub when the page has it (CEONE), else as origin/main called it (the card: openCharmEditor(job); the order window: { row, poolId, from, say }). */
const enter = (w, k) => (w.hub ? (k === 'card' ? w.hub.open({ piece: String(w.job.copies[0]), row: w.job.row }, { from: 'card', job: w.job }) : w.hub.open({ piece: POOL, row: w.row }, { from: 'orderWindow', say: () => {} })) : w.api.openCharmEditor(k === 'card' ? w.job : { row: w.row, poolId: POOL, from: 'orderWindow', say: () => {} }));
/** The Type as the editor draws it (its outlines in the face, as a Save writes them): for the record of 26 Nov. */
async function fionaOutlines() {
  let out = null;
  const w = world({ person: person((S, geo) => { const t = wordsOf(S.front.items)[0]; out = M.exportItems([t], geo)[0].subpaths; }) });
  await w.api.openCharmEditor(w.job);
  return out;
}

(async () => {
  /* ── A · every way in hands the approved front in as ONE Type linked to its job ── */
  console.log('A · the words handed in');
  for (const [name, x] of [['the engraving card', 'card'], ['the order window (Open in Charm Editor / Edit Design)', 'ow'], ['the order window\'s Sheet tab, the charm picked on the sheet', 'sheet']]) {
    const seen = {}, w = world({ person: person(null, seen) });
    await w.api.openCharmEditor(x === 'card' ? w.job : { row: w.row, poolId: POOL, from: 'orderWindow' });
    ok(linked(w, seen.front), `A1 ${name}: the approved front "Fiona" is ONE Type of the editor linked to its job`, (seen.front || []).map(brief));
  }
  { const seen = {}, w = world({ shelved: true, person: person(null, seen) });
    await w.api.openCharmEditor({ row: w.row, poolId: POOL, from: 'orderWindow' });
    ok(linked(w, seen.front), 'A2 a written front the upkeep shelved (its writtenFit only): handed in as its linked Type all the same', (seen.front || []).map(brief)); }
  { const seen = {}, w = world({ copies: [], rowPoolIds: [], person: person(null, seen) });
    await w.api.openCharmEditor({ row: w.row, poolId: POOL, from: 'orderWindow' });
    ok(linked(w, seen.front), 'A3 a front job that names no piece yet (its line not pooled on this computer), opened from the order window: its words are handed in as its linked Type', { handed: (seen.front || []).map(brief), copies: w.job.copies }); }
  { const sw = fs.readFileSync(path.join(ROOT, 'charm-nest-sheetwin.js'), 'utf8');
    ok(!/openCharmEditor\s*\(/.test(sw) && /EngraveLink\.open\(/.test(sw), 'A4 the sheet window has no Charm Editor door of its own for a plain piece: it goes to the engraving card (EngraveLink) or the order window'); }

  /* ── R · the cause: Offset Path's "Add new path" on the words, then Save ── */
  console.log('R · the copy the editor made of the words (the record of 26 Nov)');
  const fiona = await fionaOutlines();
  for (const [name, k] of WAYS) {
    let made = null;
    const w = world({ person: person((S, geo) => {
      const t = wordsOf(S.front.items)[0], r = OPS.ops.offsetItems(S.front.items, [t.id], geo, { mm: 0.2, join: 'miter', miter: 4, mode: 'add' });
      S.front.items = r.items; made = S.front.items.find(i => (r.sel || []).includes(i.id));
    }) });
    await w.api.openCharmEditor(door(w, k));
    const added = addedOf(w);
    ok(!!made && made.kind === 'path' && /^ce/.test(made.id) && made.text == null && made.role === 'hatch' && !!made.engraving && made.engraving.job === w.job.key, `R0 ${name}: Offset Path ▸ Add new path makes what the record of 26 Nov holds: a new "ce…" id, kind path, role hatch, no words (the link to the job carried with its paint)`, brief(made));
    ok(added.length === 0, `R1 ${name}: the Save writes NO copy of the words into the drawing (it wrote one: kind path, no words, a "ce…" id: Fiona twice on the sheet)`, added.map(a => ({ id: a.id, kind: a.kind, role: a.role, text: a.text })));
    ok(w.setups.length === 0 && w.job.lines.join() === 'Fiona', `R2 ${name}: the engraving keeps its words "Fiona", untouched (one copy: the engraving)`, w.setups);
  }

  /* ── B · the words retyped ── */
  console.log('B · the words changed in the editor go back to the engraving');
  for (const o of [{}, { copies: [], rowPoolIds: [] }]) {
    const w = world(Object.assign({ person: person((S, geo) => { const t = wordsOf(S.front.items)[0]; if (t) S.front.items = S.front.items.map(i => (i === t ? M.retext(t, { text: 'Fiona Rose' }, geo) : i)); }) }, o));
    await w.api.openCharmEditor({ row: w.row, poolId: POOL, from: 'orderWindow' });
    const p = w.setups.find(s => s.front), added = addedOf(w);
    ok(!!p && p.front.text === 'Fiona Rose' && added.length === 0, `B1 ${o.copies ? 'a job that named no piece: ' : ''}"Fiona" retyped "Fiona Rose" and saved: the job's words are "Fiona Rose" (Engrave.saveSetup), nothing is added to the drawing`, { setups: w.setups, added: added.map(a => ({ id: a.id, kind: a.kind, text: a.text })) });
  }

  /* ── C · nothing changed ── */
  console.log('C · Save with nothing changed');
  for (const [name, k] of WAYS) {
    const w = world({ person: person(null) });
    await w.api.openCharmEditor(door(w, k));
    ok(w.saved.length === 0 && w.setups.length === 0, `C1 ${name}: nothing written, nothing changed`, { saved: w.saved.length, setups: w.setups });
  }

  /* ── D · the record of 26 Nov, opened again ── */
  console.log('D · the piece whose art already holds Fiona\'s outlines');
  const part = (id, subs) => ({ kind: 'path', subpaths: toSource(subs), fill: true, hatchBlue: true, stroke: false, artRole: 'hatch', artAdded: true, artMemberId: id });
  ok(!!fiona && fiona.length > 3, 'D0 Fiona\'s outlines as the editor draws the Type handed in (the record\'s d)', fiona && fiona.length);
  for (const [name, card] of [['the engraving card', true], ['the order window', false]]) {
    const seen = {}, w = world({ artFront: [part('cemwxw1q90b', fiona)], person: person(null, seen) });
    await w.api.openCharmEditor(card ? w.job : { row: w.row, poolId: POOL, from: 'orderWindow' });
    const shown = (seen.front || []).filter(i => String(i.id) === 'cemwxw1q90b');
    const rec = w.saved.find(s => s.face === 'front');
    ok(linked(w, seen.front) && shown.length === 0, `D1 ${name}: the editor is handed ONE Fiona, the engraving's Type; the outline copy cemwxw1q90b is not among its parts`, (seen.front || []).map(brief));
    ok(!!rec && !(rec.art.added || []).some(a => a.id === 'cemwxw1q90b') && w.setups.length === 0, `D2 ${name}: Save takes the copy out of the piece's record (one copy left: the engraving, untouched)`, { saved: w.saved.map(s => (s.art.added || []).map(a => a.id)), setups: w.setups });
  }
  { const other = (await (async () => { let out = null; const w = world({ person: person((S, geo) => { out = M.exportItems([{ id: 'x', kind: 'text', role: 'hatch', text: 'Rose', fontKey: 'ace', sizePt: 7, align: 'center', tx: 40, ty: -10, sx: 1, sy: 1, rot: 0 }], geo)[0].subpaths; }) }); await w.api.openCharmEditor(w.job); return out; })());
    const grown = OPS.kernel.offsetPath({ subpaths: fiona }, 0.2 * 72 / 25.4, 'miter', 4, false, false);
    const seen = {}, w = world({ artFront: [part('ceother1', other), part('cegrown1', grown)], person: person(null, seen) });
    await w.api.openCharmEditor(w.job);
    const ids = (seen.front || []).map(i => String(i.id));
    ok(ids.includes('ceother1') && ids.includes('cegrown1') && w.saved.length === 0, 'D3 outlines of OTHER words ("Rose"), and outlines that are not exactly the words in their font (Fiona grown 0.2 mm), are kept as they are: nothing proves them the engraving\'s', { ids, saved: w.saved.length }); }

  /* ── E · the person's own words ── */
  console.log('E · words the person adds are kept');
  { const w = world({ person: person(S => { S.front.items = S.front.items.concat([{ id: M.newId(), kind: 'text', role: 'hatch', source: 'added', name: 'Text', text: '2026', fontKey: 'ace', sizePt: 5, align: 'center', tx: 20, ty: -6, sx: 1, sy: 1, rot: 0 }]); }) });
    await w.api.openCharmEditor(w.job);
    const added = addedOf(w);
    ok(added.length === 1 && added[0].kind === 'text' && added[0].text === '2026' && w.setups.length === 0, 'E1 a new Type "2026" typed beside the engraving: kept in the drawing as drawn; the engraving untouched', added.map(a => ({ id: a.id, kind: a.kind, text: a.text }))); }
  { const w = world({ person: person((S, geo) => { const t = wordsOf(S.front.items)[0], c = M.retext(M.reid(t), { text: 'Love', ty: -4 }, geo); S.front.items = S.front.items.concat([c]); }) });
    await w.api.openCharmEditor(w.job);
    const added = addedOf(w);
    ok(added.length === 1 && added[0].text === 'Love' && w.setups.length === 0, 'E2 the words duplicated and the copy retyped "Love": the person\'s own words, kept as drawn; the engraving keeps "Fiona"', added.map(a => ({ id: a.id, kind: a.kind, text: a.text }))); }
  { const w = world({ person: person((S, geo) => { const t = wordsOf(S.front.items)[0], c = M.retext(M.reid(t), { ty: -4 }, geo); S.front.items = S.front.items.concat([c]); }) });
    await w.api.openCharmEditor(w.job);
    ok(addedOf(w).length === 0 && w.setups.length === 0, 'E3 the words duplicated with their very words: a second copy of the engraving, not added to the drawing', addedOf(w).map(a => ({ id: a.id, kind: a.kind, text: a.text }))); }

  /* ── H · ONE editor: every entry the same open, the same editor, the same save ── */
  console.log('H · one editor, centrally connected (CEONE)');
  { const want = [
      ['charm-nest-bridge.js', /CharmEditorHub\.open\(\{ piece: String\(\(j\.copies \|\| \[\]\)\[0\] \|\| ""\), row: j\.row \|\| null \}, \{ from: "card", job: j \}\)/, 'the engraving card\'s Open in Charm Editor'],
      ['charm-nest-custom-piece.js', /H\.open\(\{ piece: pc\.poolId, row: pc\.row \}, \{ from: inWin \? 'orderWindow' : 'piece', say: tell \}\)/, 'the order window\'s Open in Charm Editor / Edit Design (and the charm picked on its Sheet tab)'],
      ['charm-nest-bridge.js', /CharmEditorHub\.open\(\{ piece: t\.poolIds\[0\], row: t\.row \}/, 'Keep editing after a Save'],
      ['charm-nest-bridge.js', /CharmEditorHub\.open\(\{ master: b\.dataset\.ceopen \}\)/, 'Master files Edit (a repository charm)'],
      ['charm-nest-bridge.js', /CharmEditorHub\.open\(\{ empty: o \}\)/, 'the tabs\' Charm Editor button (a new design)'],
      ['charm-nest-sheetwin.js', /CharmEditorHub\.open\(\{ design: f\.designId, version: f\.version \|\| undefined \}\)/, 'the sheet window\'s custom piece (its design)']];
    for (const [f, re, name] of want) ok(re.test(fs.readFileSync(path.join(ROOT, f), 'utf8')), `H1 ${name} opens through the one open (CharmEditorHub.open, by what is opened)`);
    const ed = [['charm-nest-bridge.js', /res = await CharmEditorHub\.editor\(Object\.assign\(\{ title: sku, subtitle,/, 'a piece'], ['charm-nest-bridge.js', /CharmEditorHub\.editor\(Object\.assign\(\{ fonts, textToPaths: charmEditText \}, opts\)/, 'the design mode of the page'],
      ['charm-nest-ce-masters.js', /HUB\.editor\(opts, content\)/, 'a repository charm, a design, a new design (charm-nest-ce-masters.js)'], ['charm-nest-ce-library.js', /HUB\.editor\(next, content\)/, 'a design or charm opened from the library panel'],
      ['charm-nest-ce-library.js', /HUB\.editor\(opts, content\)/, 'a saved design (charm-nest-ce-library.js openDesign)'], ['charm-nest-custom-piece.js', /HUB\.editor\(o, \{ custom: rid \}\)/, 'a custom order\'s design']];
    for (const [f, re, name] of ed) ok(re.test(fs.readFileSync(path.join(ROOT, f), 'utf8')), `H2 ${name}: the editor is the one editor (CharmEditorHub.editor)`);
    const sv = [['charm-nest-bridge.js', /await CharmEditorHub\.save\(content, res, savePiece\)/, 'a piece'], ['charm-nest-ce-masters.js', /HUB\.save\(content, res, keep\)/, 'a design, a new design'], ['charm-nest-ce-library.js', /HUB\.save\(content, res, r => keepVersion\(design, r\)\)/, 'the library panel\'s designs'], ['charm-nest-custom-piece.js', /HUB\.save\(\{ custom: rid \}, res, keep\)/, 'a custom order\'s design']];
    for (const [f, re, name] of sv) ok(re.test(fs.readFileSync(path.join(ROOT, f), 'utf8')), `H3 ${name}: its Save is the one save (CharmEditorHub.save)`);
  }
  // the same piece from every entry: the same open, the same editor (content and controls), the same outputs of Save
  { const canon = o => o && { title: o.title, subtitle: o.subtitle, startFace: o.startFace, lockRoles: o.lockRoles, back: o.back && [o.back.label, o.back.title], hold: !!o.hold, saveOff: !!o.saveOff, autosave: !!o.autosave, check: typeof o.check === 'function',
      faces: Object.fromEntries(Object.entries(o.faces || {}).map(([f, d]) => [f, (d.items || []).map(i => [i.source === 'engrave' ? 'WORDS' : i.id, i.kind, i.text || '', i.locked ? 'locked' : ''])])) };
    const outOf = w => ({ saved: w.saved.map(s => [s.pid, s.face, JSON.stringify(s.art.added.map(a => [a.kind, a.role, a.text || '', a.d])), JSON.stringify(s.art.removed), JSON.stringify(s.art.changed)]), setups: JSON.stringify(w.setups) });
    for (const [what, wo, act] of [['opened and closed', { two: true, back: true }, null], ['"Fiona" retyped "Fiona Rose"', { two: true, back: true }, (S, geo) => { const t = wordsOf(S.front.items)[0]; S.front.items = S.front.items.map(i => (i === t ? M.retext(t, { text: 'Fiona Rose' }, geo) : i)); }],
      ['Offset Path ▸ Add new path on the words (26 Nov)', { two: true, back: true }, (S, geo) => { const t = wordsOf(S.front.items)[0], r = OPS.ops.offsetItems(S.front.items, [t.id], geo, { mm: 0.2, join: 'miter', miter: 4, mode: 'add' }); S.front.items = r.items; }],
      ['a new Type "2026" added', { two: true, back: true }, S => { S.front.items = S.front.items.concat([{ id: 'ce-new-2026', kind: 'text', role: 'hatch', source: 'added', name: 'Text', text: '2026', fontKey: 'ace', sizePt: 5, align: 'center', tx: 20, ty: -6, sx: 1, sy: 1, rot: 0 }]); }]]) {
      const res = {};
      for (const k of ['card', 'ow', 'sheet']) { const w = world(Object.assign({ person: person(act) }, wo)); await enter(w, k === 'card' ? 'card' : 'ow'); res[k] = { opts: canon(w.opened[0]), out: outOf(w), hub: w.hub ? w.hub.calls().filter(c => c.fn !== 'editor').map(c => [c.fn, c.kind, c.key]) : null }; }
      const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
      ok(!!res.card.hub && same(res.card.hub, res.ow.hub) && same(res.ow.hub, res.sheet.hub) && same(res.card.hub[0], ['open', 'piece', POOL]), `H4 ${what}: the card, the order window and its Sheet tab call the same open with the same content (piece ${POOL}) and the same save`, { card: res.card.hub, ow: res.ow.hub });
      ok(same(res.card.opts, res.ow.opts) && same(res.ow.opts, res.sheet.opts), `H5 ${what}: the editor each entry opens is the same: the same content (title, subtitle, faces, first face) and the same controls (locks, Back, held, draft, checks)`, { card: res.card.opts, ow: res.ow.opts });
      ok(same(res.card.out, res.ow.out) && same(res.ow.out, res.sheet.out), `H6 ${what}: Save gives the same outputs from every entry (the same records for the same copies, the same words given back to the engraving)`, { card: res.card.out, ow: res.ow.out });
    }
  }
  // the editor's controls the same for every content, apart from what the content decides (like the greyed Save): the cut line locked at open for a placed piece and a custom order's drawing,
  // free for a repository charm, a saved design and a new design; and the same content gives the same controls whoever opens it (a design opened from the library panel of a piece's editor used
  // to take that editor's locks)
  { const w = world({ person: person(null) }); await enter(w, 'card');
    const H = w.hub, ui = [];
    if (H) { const seen = []; w.ctx.CharmEditor.open = async o => { seen.push(o); return null; };
      for (const [content, opts] of [[{ design: 'd-1', version: 2 }, { title: 'D', faces: {}, startFace: 'front', lockRoles: [] }], [{ design: 'd-1', version: 2 }, { title: 'D', faces: {}, startFace: 'front', lockRoles: ['cut'] }],
        [{ empty: true }, { title: 'New', faces: {}, startFace: 'front', lockRoles: [], back: { label: 'Back to Orders', title: 'Back to the Orders tab' } }],
        [{ custom: '4250000001' }, { title: 'C', faces: {}, startFace: 'front', lockRoles: ['cut'] }], [{ custom: '4250000002' }, { title: 'C blank', faces: {}, startFace: 'front', lockRoles: [] }],
        [{ master: 'SKINNY_2952' }, { title: 'M', faces: {}, startFace: 'front', lockRoles: [], saveOff: true }]]) { await H.editor(opts, content); }
      for (const o of [w.opened[0]].concat(seen)) { const u = H.uiOf(o); ui.push([u.lockRoles.join(','), u.back, u.hold, u.saveOff]); } }
    const same = ui.length && JSON.stringify(ui[0].slice(1, 3));
    ok(ui.length === 7 && ui.every(u => JSON.stringify(u.slice(1, 3)) === same),
      'H7 the editor shows the same controls for a piece, a design, a new design, a custom order\'s design and a repository charm: the same Back, held after Save', ui);
    ok(ui.length === 7 && ui.map(u => u[0]).join('|') === 'cut||||cut||' && ui.map(u => u[3]).join() === 'false,false,false,false,false,false,true',
      'H8 what is opened decides, like the repository\'s greyed Save: the cut line opens locked for a placed piece and a custom order\'s drawing, free for a repository charm, a design (whoever opens it) and a new design; only the repository charm\'s Save is greyed out', ui.map(u => u[0] + ':' + u[3]));
  }
  /* ── F · the page loads the changed files ── */
  const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
  for (const f of ['charm-nest-bridge.js', 'charm-nest-ce-words.js', 'charm-nest-ce-masters.js', 'charm-nest-ce-library.js', 'charm-nest-custom-piece.js', 'charm-nest-sheetwin.js']) ok(new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-ce1"').test(html), `F1 charm-nest-1.html loads the new ${f} (token -ce1)`);

  console.log(`\nrt1128-cesave: ${pass} passed, ${fail} failed`); process.exit(fail ? 1 : 0);
})().catch(e => { console.log('  FAIL the test ran to its end :: ' + ((e && e.stack) || e)); process.exit(1); });
