// ENGUI3 (Paul, 11 Oct 2026, plans/front-back-1011): front + back engraving on the sheet window, the shelves, the sheet cards, the Library wording, the Orders chip and the search.
// Offline and pure: the sheet window's own engraving helpers are read from its source and run over contract-shaped fixtures (jobs keyed <line>[#slot][~front], records flat in
// sheet.backPool with face:'front' for a front); the shelf, the show/hide pill and the readiness wording are the real modules (jsdom for the pill). Nothing is written anywhere.
//   NODE_PATH=<jsdom node_modules> node tests/charm-nest/engrave-ui-sheet.cjs
// Cases: order 4195966039 (front "W" / back "1. 27. 24"), an earring pair with front + back (2 pieces, 4 engravings), a 3-disc necklace with front + back (3 pieces, 6 engravings),
// and the back-only cases, which must read EXACTLY as they did before this change.
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const src = f => fs.readFileSync(path.join(root, f), 'utf8');
const Faces = require(path.join(root, 'charm-nest-engrave-faces.js'));
const Backs = require(path.join(root, 'charm-nest-backs.js'));
const Ready = require(path.join(root, 'charm-nest-readiness.js'));
let n = 0; const t = (name, fn) => Promise.resolve().then(fn).then(() => { n++; console.log('ok   ' + name); }, e => { console.error('FAIL ' + name + '\n' + (e.stack || e)); process.exitCode = 1; });

/* ── the sheet window's helpers, run as written ── */
const sw = src('charm-nest-sheetwin.js');
const cut = (a, b) => { const i = sw.indexOf(a), j = sw.indexOf(b, i); assert(i > 0 && j > i, 'markers ' + a); return sw.slice(i, j); };
const disc = cut('  function discOfPiece(x) {', '  const copyText = ');
const block = cut('  const FACES = () =>', '  /* ── the sheet pane: its orders ── */');
function world(jobs, rec, extra) {
  const items = new Map(jobs.map(j => [j.key, j]));
  const win = { CharmNestEngraveFaces: Faces, Engrave: { items: () => items, isWorking: () => false, canFit: () => true }, OrderEngraving: extra && extra.OE };
  const ctx = vm.createContext({ window: win, Engrave: win.Engrave, W: { rec: rec || null }, tryDo: (f, d) => { try { return f(); } catch (_) { return d; } }, rowOf: () => ({}) });
  return vm.runInContext(disc + block + '\n;({ FACES, faceOfJob, unitsOf, anyFront, engWord, jobsOfPiece, engOf, engSame, shownFace, shownEng, faceTextOf, discOfPiece, recKeyOf })', ctx);
}
const LINE = '4195966039_5000000011', P = c => `${LINE}_${c}`;
const job = (slot, face, state, copy, text, o) => Object.assign({ key: Faces.jobKey(LINE, slot, face), face, slot: slot || null, state, copies: [P(copy)], text, rowKey: LINE }, o || {});
const piece = copy => ({ poolId: P(copy), id: 'c' + copy, qty: 1, copy });
const rec = (copy, face, text) => Object.assign({ poolId: P(copy), sheetId: 's1', text, approvedAt: 5, approvedBy: 'Ana' }, face === 'front' ? { face: 'front' } : {});

(async () => {
  await t('order 4195966039: front "W" and back "1. 27. 24" are one piece with two engravings; the least settled face speaks for the piece', () => {
    const w = world([job(null, 'front', 'review', 1, 'W'), job(null, 'back', 'review', 1, '1. 27. 24')]);
    const e = w.engOf(piece(1));
    assert.equal(e.kind, 'approve'); assert.deepEqual(Array.from(e.units.map(u => u.face)), ['front', 'back']);
    assert.equal(e.text, 'Front: W · Back: 1. 27. 24');
    assert.equal(w.faceTextOf({ eng: e }, 'front'), 'W'); assert.equal(w.faceTextOf({ eng: e }, 'back'), '1. 27. 24');
    assert.equal(w.shownFace({ eng: e }), 'front', 'the Front is shown first');
    assert.equal(w.shownEng({ eng: e, _face: 'back' }).text, '1. 27. 24');
  });
  await t('front approved, back still to approve: the piece is still to approve, and the card shows the back (the face still to settle)', () => {
    const w = world([job(null, 'front', 'approved', 1, 'W', { approvedBy: 'Ana', approvedAt: 9 }), job(null, 'back', 'review', 1, '1. 27. 24')]);
    const x = { poolId: P(1) }; x.eng = w.engOf(x);
    assert.equal(x.eng.kind, 'approve'); assert.equal(x.eng.faces.front.kind, 'approved'); assert.equal(w.shownFace(x), 'back');
    assert.equal(w.engSame(x.eng, w.engOf(x)), true);
  });
  await t('both faces approved: approved; one face decided "cut plain" and the other approved: approved too, the plain face is kept', () => {
    const a = world([job(null, 'front', 'written', 1, 'W'), job(null, 'back', 'approved', 1, '1. 27. 24')]).engOf(piece(1));
    assert.equal(a.kind, 'approved');
    const b = world([job(null, 'front', 'skipped', 1, ''), job(null, 'back', 'approved', 1, '1. 27. 24')]).engOf(piece(1));
    assert.equal(b.kind, 'approved'); assert.equal(b.faces.front.kind, 'skipped'); assert.equal(b.units.length, 2);
  });
  await t('a sheet read from the Library (no jobs): the two saved records are the two engravings, front first', () => {
    const r = { backPool: [rec(1, 'back', '1. 27. 24'), rec(1, 'front', 'W')], engraving: { [P(1)]: { needed: true, state: 'written', approved: true, faces: { front: { needed: true, state: 'written', approved: true }, back: { needed: true, state: 'written', approved: true } } } } };
    const w = world([], r); const e = w.engOf(piece(1), r);
    assert.equal(e.kind, 'approved'); assert.deepEqual(Array.from(e.units.map(u => u.face)), ['front', 'back']); assert.equal(e.text, 'Front: W · Back: 1. 27. 24');
  });
  await t('an earring pair, front + back: 2 pieces and 4 engravings (the count of engravings is units, the count of pieces is pieces)', () => {
    const jobs = [job('L', 'front', 'review', 1, 'A'), job('L', 'back', 'review', 1, 'a'), job('R', 'front', 'approved', 2, 'A'), job('R', 'back', 'approved', 2, 'a')];
    const w = world(jobs);
    const xs = [piece(1), piece(2)].map(p => { p.eng = w.engOf(p); return p; });
    assert.equal(xs.length, 2); assert.equal(xs.flatMap(w.unitsOf).length, 4);
    assert.equal(Faces.pieceCount(jobs), 2);
    assert.equal(xs.flatMap(w.unitsOf).filter(u => ['approve', 'words', 'preparing'].includes(u.kind)).length, 2, 'two engravings still to approve (the left ear)');
    assert.equal(w.anyFront(xs), true);
    assert.equal(w.engWord(2, true), 'engravings'); assert.equal(w.engWord(1, true), 'engraving');
    assert.equal(xs[0].eng.kind, 'approve'); assert.equal(xs[1].eng.kind, 'approved');
  });
  await t('a 3-disc necklace, front + back: 3 pieces and 6 engravings; "Disc n of m" counts pieces', () => {
    const jobs = []; for (let d = 1; d <= 3; d++) jobs.push(job('D' + d, 'front', 'review', d, 'F' + d), job('D' + d, 'back', 'review', d, 'B' + d));
    const OE = { cycleOf: (row, key) => { const ps = jobs.map((j, i) => ({ key: j.key, job: j, n: +j.slot.slice(1), name: 'Disc ' + j.slot.slice(1) })); return { pieces: ps, key }; } };
    const w = world(jobs, null, { OE });
    const xs = [1, 2, 3].map(piece); for (const p of xs) p.eng = w.engOf(p);
    assert.equal(xs.flatMap(w.unitsOf).length, 6); assert.equal(Faces.pieceCount(jobs), 3);
    const d = w.discOfPiece(xs[1]); assert.deepEqual({ n: d.n, of: d.of }, { n: 2, of: 3 }, 'Disc 2 of 3, not of 6');
  });
  await t('INERT: a back-only piece reads exactly as before (the same keys, no faces, no units; one unit)', () => {
    const w = world([job(null, 'back', 'review', 1, '1. 27. 24')]);
    const x = piece(1); x.eng = w.engOf(x);
    assert.deepEqual(Object.keys(x.eng).sort(), ['job', 'kind', 'label', 'text']); assert.equal(x.eng.kind, 'approve'); assert.equal(x.eng.label, 'to approve');
    assert.equal(x.eng.faces, undefined); assert.equal(x.eng.units, undefined);
    assert.equal(w.unitsOf(x).length, 1); assert.equal(w.anyFront([x]), false); assert.equal(w.engWord(2, false), 'backs');
    assert.equal(w.shownFace(x), 'back'); assert.equal(w.shownEng(x), x.eng); assert.equal(w.faceTextOf(x, 'back'), '1. 27. 24'); assert.equal(w.faceTextOf(x, 'front'), '');
    const none = world([job(null, 'back', 'none', 1, '')]).engOf(piece(1)); assert.equal(none.kind, 'none');
    const saved = world([], { backPool: [rec(1, 'back', 'x')] }).engOf(piece(1), { backPool: [rec(1, 'back', 'x')] }); assert.equal(saved.kind, 'approved'); assert.equal(saved.faces, undefined);
  });

  await t('take off / move / remove: the sheet window carries a piece\'s records by poolId, so both faces travel together (row 46)', () => {
    const lines = sw.split('\n').filter(l => /backPool\s*=\s*.*\.filter\(|\.backPool \|\| \[\]\)\.filter\(/.test(l));
    assert(lines.length >= 4, 'the take-off / move / undo filters are there: ' + lines.length);
    for (const l of lines) { assert.match(l, /\.poolId/, l); assert.doesNotMatch(l, /\.face|isFrontRec|recKey/, 'a filter by face would leave one face behind: ' + l); }
    // and the same filter, run on a piece with both records, takes both
    const both = [rec(1, 'back', 'a'), rec(1, 'front', 'A'), rec(2, 'back', 'b')], ids = new Set([P(1)]);
    assert.deepEqual(both.filter(b => !ids.has(b.poolId)).map(b => b.poolId), [P(2)]); assert.equal(both.filter(b => ids.has(b.poolId)).length, 2);
  });

  /* ── the back shelf ── */
  const shelfBacks = [Object.assign(rec(1, 'back', '1. 27. 24'), { order: '4195966039', sku: 'S', preview: 'https://x/b.png' }), Object.assign(rec(1, 'front', 'W'), { order: '4195966039', sku: 'S', preview: 'https://x/f.png' })];
  await t('shelf: a front + back sheet names each picture\'s face, and the section says "Engravings"', () => {
    const h = Backs.markup(shelfBacks, {});
    assert.match(h, /aria-label="Engravings"/); assert.equal((h.match(/class="backFace"/g) || []).length, 2); assert.match(h, />Front<\/span>/); assert.match(h, />Back<\/span>/);
    assert.equal((h.match(/<figure /g) || []).length, 2); assert.match(h, /data-face="front"/);
  });
  await t('INERT shelf: a back-only sheet is drawn exactly as before (no caption, "Back engravings")', () => {
    const h = Backs.markup([shelfBacks[0]], {});
    assert.match(h, /aria-label="Back engravings"/); assert.doesNotMatch(h, /backFace/); assert.doesNotMatch(h, /data-face/);
  });

  /* ── the show/hide pill (jsdom) ── */
  let JSDOM; try { ({ JSDOM } = require('jsdom')); } catch (_) { console.log('  – no jsdom (set NODE_PATH): the pill checks were not run'); }
  if (JSDOM) await t('pill: "Engravings, 4 engravings" with a front; "Back engraving, 4 pieces" without; the library card follows', () => {
    const dom = new JSDOM('<!doctype html><body><span id="h" hidden></span><span id="g" hidden></span>'
      + '<div id="mixed"><section class="sheetBacks"><div class="backPieces"><figure data-face="front"></figure><figure></figure><figure data-face="front"></figure><figure></figure></div></section></div>'
      + '<div id="plain"><section class="sheetBacks"><div class="backPieces"><figure></figure><figure></figure><figure></figure><figure></figure></div></section></div></body>', { url: 'http://localhost/', runScripts: 'outside-only' });
    const w = dom.window, d = w.document; w.eval(src('charm-nest-engraving-toggle.js')); const ET = w.EngravingToggle;
    assert.equal(ET.countIn(d.getElementById('mixed')), 4); assert.equal(ET.mixedIn(d.getElementById('mixed')), true); assert.equal(ET.mixedIn(d.getElementById('plain')), false);
    const a = ET.mount(d.getElementById('h'), { sheetKey: 'a', count: 4, mixed: true });
    assert.equal(a.button.getAttribute('aria-label'), 'Engravings, 4 engravings'); assert.match(a.button.title, /^Show the engravings of this sheet · 4 engravings$/);
    const b = ET.mount(d.getElementById('g'), { sheetKey: 'b', count: 4 });
    assert.equal(b.button.getAttribute('aria-label'), 'Back engraving, 4 pieces'); assert.match(b.button.title, /^Show the back engraving of this sheet · 4 pieces$/);
    b.update(2, 'b', true); assert.equal(b.button.getAttribute('aria-label'), 'Engravings, 2 engravings');
    b.update(1, 'b'); assert.equal(b.button.getAttribute('aria-label'), 'Engravings, 1 engraving', 'an update without the flag keeps what it was');
    b.update(1, 'b', false); assert.equal(b.button.getAttribute('aria-label'), 'Back engraving, 1 piece');
    assert.equal(a.button.querySelector('.engN').textContent, '4');
  });

  /* ── readiness: the same sheet counted by pieces and by engravings ── */
  const sheet = (faces) => {
    const ok = id => ({ poolId: id, sheetId: 'sh', approvedAt: 5, approvedBy: 'Ana', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://x/' + id + '.ai' } } });
    const s = { id: 'sh', setId: 'set', status: 'complete', poolIds: [P(1), P(2)], placedCount: 2, verification: { ok: true }, outputs: { ai: { url: 'https://x/f.ai' }, preview: { url: 'https://x/p.png' } }, label: { files: [{ path: 'q', url: 'https://x/q', payload: 'p', orders: ['4195966039'] }], orders: ['4195966039'] }, orders: ['4195966039'], backPool: [], engraving: {} };
    const dec = (state, ap) => ({ needed: true, state, approved: ap });
    if (faces) {
      s.engraving[P(1)] = Object.assign(dec('approved', true), { faces: { front: dec('written', true), back: dec('review', false) } });
      s.engraving[P(2)] = Object.assign(dec('written', true), { faces: { front: dec('written', true), back: dec('written', true) } });
      s.backPool = [Object.assign(ok(P(1)), { face: 'front' }), Object.assign(ok(P(2)), { face: 'front' }), ok(P(2))];
    } else {
      s.engraving[P(1)] = dec('review', false); s.engraving[P(2)] = dec('written', true); s.backPool = [ok(P(2))];
    }
    return s;
  };
  await t('readiness: 2 ears x 2 faces = 4 engravings, 2 pieces; 3 of 4 approved while 1 of 2 pieces is', () => {
    const r = Ready.sheet(sheet(true));
    assert.equal(r.required, 2); assert.equal(r.approved, 1); assert.equal(r.waiting, 1);
    assert.deepEqual(r.faces, { required: 4, approved: 3, waiting: 1, saved: 3 });
    const s = Ready.laserSheet(sheet(true)); assert.deepEqual(s.faces, r.faces);
  });
  await t('INERT readiness: a back-only sheet has no `faces` and says what it always said', () => {
    const s = sheet(false), r = Ready.sheet(s);
    assert.equal(r.faces, undefined); assert.equal(r.required, 2); assert.equal(r.approved, 1);
    const x = Ready.explain ? Ready.explain(s, { kind: 'sheet', rows: [] }) : null;
    if (x) { const eng = x.steps.find(q => q.key === 'engraving'); assert(eng); assert.match(eng.detail, /1 back engraving still needs approval \(1 of 2 approved\)\./); }
  });
  await t('readiness wording: a sheet with a front says "engraving" and counts engravings; the set sums the faces', () => {
    const x = Ready.explain(sheet(true), { kind: 'sheet', rows: [] }); const eng = x.steps.find(q => q.key === 'engraving');
    assert.match(eng.detail, /^1 engraving still needs approval \(3 of 4 approved\)\.$/);
    const g = Ready.set({ sheetIds: ['sh'] }, [sheet(true)]); assert.deepEqual(g.faces, { required: 4, approved: 3, waiting: 1, saved: 3 });
    const mix = Ready.set({ sheetIds: ['sh', 'b'] }, [sheet(true), (() => { const b = sheet(false); b.id = 'b'; b.backPool.forEach(r => { r.sheetId = 'b'; }); return b; })()]);
    assert.deepEqual(mix.faces, { required: 6, approved: 4, waiting: 2, saved: 4 }, 'a back-only sheet counts one engraving per piece');
    const done = sheet(true); done.engraving[P(1)].faces.back = { needed: true, state: 'written', approved: true }; done.engraving[P(1)].approved = true; done.engraving[P(1)].state = 'written'; done.backPool.push({ poolId: P(1), sheetId: 'sh', approvedAt: 6, approvedBy: 'Ana', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: 'b.ai', url: 'https://x/b.ai' } } });
    assert.deepEqual(Ready.sheet(done).faces, { required: 4, approved: 4, waiting: 0, saved: 4 });
    assert.match(Ready.explain(done, { kind: 'sheet', rows: [] }).steps.find(q => q.key === 'engraving').detail, /^Every engraving is approved \(4 of 4\)\.$/);
  });
  await t('the issues label: "Saving files" only for a sheet with a front; the library list keeps the readiness\' own words', () => {
    const s = sheet(true); s.engraving[P(1)].faces.back = { needed: true, state: 'written', approved: true }; s.engraving[P(1)].approved = true; s.engraving[P(1)].state = 'written';   // all approved, the back file of P1 is not saved
    const mine = Ready.issues ? Ready.issues({ rows: [], allSheets: [s], sheets: [s] }) : null;
    if (mine) { const all = [].concat(mine.issues || mine || []); const own = all.find(i => i && i.key === 'backFilesMissing'); if (own) assert.equal(own.label, 'Saving files'); }
    const li = src('charm-nest-library-issues.js'); assert.match(li, /it\.key === 'backFilesMissing' && it\.label === 'Saving files' \? it\.label : o\.label/);
  });

  /* ── the Orders chip and the search wording (source-level: both are inside big page modules) ── */
  await t('Orders chip: "Front + back" / "Front only" from the line\'s setup; nothing for a back-only line', () => {
    const br = src('charm-nest-bridge.js'); const i = br.indexOf('function faceChip(r)'), fn = br.slice(i, br.indexOf('const OV = {', i));
    const ctx = vm.createContext({ window: { CharmNestEngraveFaces: Faces } }); const faceChip = vm.runInContext('(' + fn.replace(/^function faceChip/, 'function') + ')', ctx);
    const row = (mode, extra) => ({ spec: { engraveCandidate: true, engraving: mode ? { mode, front: mode !== 'back' ? { text: 'W' } : undefined, back: mode !== 'front' ? { text: '1. 27. 24' } : undefined } : undefined }, ...(extra || {}) });
    assert.deepEqual(Array.from(faceChip(row('both'))), ['info', 'Front + back']); assert.deepEqual(Array.from(faceChip(row('front'))), ['info', 'Front only']);
    assert.deepEqual(Array.from(faceChip(row('back'))), ['', '']); assert.deepEqual(Array.from(faceChip(row(null))), ['', ''], 'no reading at all: today\'s back');
    assert.deepEqual(Array.from(faceChip(row('back', { engravingOverride: { mode: 'both', by: 'Ana', at: 1, front: { text: 'W' } } }))), ['info', 'Front + back'], 'a person\'s word wins');
    assert.deepEqual(Array.from(faceChip({})), ['', '']);
    assert.match(br, /fc\[1\] \? `<span class="ost \$\{fc\[0\]\} engFaces"/);
  });
  await t('search: the sentence names the face that waits; the disc count is by pieces; back-only reads as ever', () => {
    const s = src('charm-nest-search.js');
    assert.match(s, /"Front \+ back engraving"/); assert.match(s, /"Front engraving"/); assert.match(s, /: "Back engraving"\)\(new Set/);
    assert(s.indexOf('const faceOfJob =') > 0 && s.indexOf('const faceOfJob =') < s.indexOf('const engName ='), 'declared before it is used (a const read early throws)');
    assert.match(s, /now = eng && reached < 2 \? `\$\{engName\}\$\{discSay\(eng\)\} · /);
    assert.match(s, /of = mine\.some\(x => faceOfJob\(x\) === "front"\) \? new Set\(mine\.map\(x => String\(x\.slot\)\)\)\.size : mine\.length/);
  });
  await t('downloads and plate wording: "engravings" for a sheet with a front, the old words for a back-only sheet', () => {
    assert.match(sw, /hasFront\(rec\) \? "Download the sheet with its front and back engravings" : "Download the sheet with its back engravings"/);
    assert.match(sw, /hasFront\(rec\) \? "with engravings" : "with backs"/); assert.match(sw, /: fr \? "Turn the sheet over: every charm's back engraving, mirrored/);
    const br = src('charm-nest-bridge.js'); assert.match(br, /const groupFront = g => /); assert.match(br, /with their \$\{groupFront\(g\) \? "front and back " : "back "\}engravings…/); assert.match(br, /with \$\{groupFront\(g\) \? "engravings" : "back engravings"\}<\/small>/);
    assert.match(br, /\$\{job\.face === "front" \? "front" : "back"\} engraving still needs to be finished/);
  });
  await t('laser review card: "Waiting on N engravings" counts engravings when the sheet has a front, "back engravings" otherwise', () => {
    const br = src('charm-nest-bridge.js'); assert.match(br, /Waiting on \$\{plural\(E\.waiting,L\.faces\?'engraving':'back engraving'\)\}/);
    const s = sheet(true); const L = Ready.laserSheet(s), E = L.faces || L; assert.equal(E.waiting, 1);
  });

  console.log(`\n${n} pure checks passed`);
  if (process.argv.includes('--pure') || process.exitCode) return;
  if (!process.env.EU3_NEST_ONLY) await browser();
  await nestCard();
})();

/* ── the real sheet window (headless Chromium over the fake site: bridge-server.cjs, the real charmNestLibrary ops over an in-memory store, nothing live) ── */
async function browser() {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), noNested = require('./_noNestedArrays.cjs');
  const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', BACKS = 'Charm_Pool_Back', SETS = 'Charm_Nest_Sets';
  const EARS = '4300000101', NECK = '4195966039', PLAIN = '4300000103';
  const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const S1 = [[EARS, 1, 1], [EARS, 1, 2], [NECK, 2, 1], [PLAIN, 3, 1]], S2 = [['4300000104', 4, 1]];
  const sheet = (id, n, list) => ({ id, metal: 'gold', setId: 'set-1', charms: list.map(([r, t, c], i) => ({ id: id + i, poolId: pid(r, t, c), order: r, sku: 'TEST-' + t, name: `${r} · TEST-${t}`, ...(r === EARS ? { side: c === 1 ? 'L' : 'R', mirror: c === 2, bodyIndex: c - 1, groupKey: `${r}:${5000000000 + t}` } : {}) })),
    placements: list.map((x, i) => ({ id: id + i, cxPt: 40 + i * 50, cyPt: 50, angle: 0, wPt: 30, hPt: 30 })), poolIds: list.map(([r, t, c]) => pid(r, t, c)), orders: [...new Set(list.map(x => x[0]))], sheetIndex: n, fileBase: `GF_2026-10-11_Set-1_Sheet-${n}`, runId: 'run-test-1', placedCount: list.length, charmCount: list.length, stock: { wPt: 300, hPt: 150 } });
  const rec = (sheetId, rid, tx, copy, face, lines) => Object.assign({ poolId: pid(rid, tx, copy), sheetId, order: rid, sku: 'TEST-' + tx, copy, text: lines.join('\n'), lines, sizePt: 7, capMm: 1.7, weight: 'Regular', lineGap: .216, centre: [15, 15], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' }, face === 'front' ? { face: 'front' } : {});
  const recs = [rec('gold-open-1', EARS, 1, 1, 'front', ['A']), rec('gold-open-1', EARS, 1, 1, 'back', ['a']), rec('gold-open-1', EARS, 1, 2, 'front', ['A']), rec('gold-open-1', EARS, 1, 2, 'back', ['a']),
    rec('gold-open-1', NECK, 2, 1, 'front', ['W']), rec('gold-open-1', NECK, 2, 1, 'back', ['1. 27. 24']), rec('gold-open-1', PLAIN, 3, 1, 'back', ['Ada']), rec('gold-open-2', '4300000104', 4, 1, 'back', ['Bo'])];
  const dec = { needed: true, state: 'written', approved: true };
  for (const [d, mine] of [[sheet('gold-open-1', 1, S1), recs.slice(0, 7)], [sheet('gold-open-2', 2, S2), recs.slice(7)]]) {
    d.backPool = mine; d.engraving = {};
    for (const r of mine) d.engraving[r.poolId] = r.face === 'front' || mine.some(q => q.poolId === r.poolId && q.face === 'front') ? Object.assign({}, dec, { faces: { front: dec, back: dec } }) : dec;
    noNested(d); st.put(SHEETS, d.id, d);
    for (const r of mine) st.put(BACKS, r.face === 'front' ? r.poolId + '~front' : r.poolId, r);
  }
  st.put(SETS, 'set-1', { id: 'set-1', seq: 1, sheetIds: ['gold-open-1', 'gold-open-2'], runId: 'run-test-1' });
  for (const [sid, list] of [['gold-open-1', S1], ['gold-open-2', S2]]) for (const [r, tx, c] of list) st.put(POOL, pid(r, tx, c), { poolId: pid(r, tx, c), orderId: r, sheetId: sid, setId: 'set-1', state: 'placed', material: 'gold', lineKey: `${r}_${5000000000 + tx}`, sku: 'TEST-' + tx });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1500, height: 950 } });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.OrderPieces, null, { timeout: 60000 });
    await page.evaluate(([e, nk, pl, o4]) => {
      const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
      const row = (rid, tx, n) => ({ key: `${rid}_${5000000000 + tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer' }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + tx), listingId: '', sku: 'TEST-' + tx, title: 'Test ' + tx, quantity: 1, variations: [], personalization: [] }, spec: { designSku: 'TEST-' + tx, quantity: 1, material: 'gold', problems: [] },
        problems: [], state: 'written', reason: null, poolIds: Array.from({ length: n }, (_, i) => pid(rid, tx, i + 1)), engrave: null, material: 'gold', arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = [row(e, 1, 2), row(nk, 2, 1), row(pl, 3, 1), row(o4, 4, 1)]; window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
    }, [EARS, NECK, PLAIN, '4300000104']);
    await page.evaluate(id => SheetWin.open('gold-open-1', { select: id }), pid(NECK, 2, 1));
    const strip = () => page.evaluate(() => (document.querySelector('.swStrip, [data-r2=strip]') || {}).innerText || '');
    await until(async () => /engravings? approved/.test(await strip()), 30000, 'the strip counts the engravings');
    let s = await strip();
    assert.match(s, /7 engravings approved/, '2 ears x 2 faces + 2 faces + 1 back = 7 engravings (on 4 pieces): ' + s);
    assert.match(s, /Front side, front engraving as laid/, 'the sheet says the front side carries front engraving: ' + s);
    assert.doesNotMatch(s, /to approve/, s);
    console.log('ok   sheet window strip: "7 engravings approved" (4 pieces: a pair x 2 faces, 4195966039 front W / back 1. 27. 24, a back-only charm)');
    // the card of the piece: Front | Back on the very same switch, the words of the face shown
    await until(() => page.evaluate(() => document.querySelectorAll('[data-r2=eng] .egEarTab').length >= 2), 20000, 'the face switch on the piece card');
    const tabs = await page.evaluate(() => [...document.querySelectorAll('[data-r2=eng] .egEarTab')].map(b => b.innerText.replace(/\s+/g, ' ').trim()));
    assert(tabs.some(x => /front/i.test(x)) && tabs.some(x => /back/i.test(x)), 'Front and Back on the card: ' + JSON.stringify(tabs));
    const card = await page.evaluate(() => (document.querySelector('[data-r2=eng]') || {}).innerText || '');
    assert.match(card, /W/, 'the card shows the front words first: ' + card);
    await page.evaluate(() => { const b = [...document.querySelectorAll('[data-r2=eng] .egEarTab')].find(x => /back/i.test(x.innerText)); b.click(); });
    await until(() => page.evaluate(() => /1\. 27\. 24/.test((document.querySelector('[data-r2=eng]') || {}).innerText || '')), 10000, 'the back words after pressing Back');
    console.log('ok   piece card: Front | Back switch; Front shows W, Back shows 1. 27. 24');
    // the plate turned over: every engraved charm's back, no page error
    await page.evaluate(() => document.querySelector('[data-r2=face]').click());
    await until(async () => /Back side, mirrored/.test(await strip()), 15000, 'the plate on its back');
    await page.waitForTimeout(500);
    await page.evaluate(() => document.querySelector('[data-r2=face]').click());
    await until(async () => /Front side/.test(await strip()), 15000, 'the plate on its front');
    console.log('ok   plate: turned to Back and to Front again');
    // a back-only sheet reads as ever
    await page.evaluate(() => SheetWin.close()); await page.waitForTimeout(700);
    await page.evaluate(() => SheetWin.open('gold-open-2'));
    await until(async () => /1 back approved/.test(await strip()), 30000, 'a back-only strip');
    s = await strip();
    assert.match(s, /1 back approved/); assert.doesNotMatch(s, /engravings|Front side/, 'a back-only sheet never says engravings or front side: ' + s); assert.match(s, /Hover a charm for its order/);
    console.log('ok   back-only sheet: "1 back approved", no front wording');
    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    console.log('\nbrowser checks passed');
  } finally { await browser.close(); srv.close(); }
}


/* ── a Nest sheet card with a front + back piece: the plate's Front shows the FRONT words unmirrored, its Back the back words mirrored, the shelf names the faces, the pill says "Engravings" ── */
async function nestCard() {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { return; }
  const { start } = require('./bridge-server.cjs');
  const SH = 'sheet-nest-card-front', PA = '4195966039_41959660391_1', PB = '4173711093_41737110931_1';
  const base = { sheetId: SH, order: '4195966039', sku: 'TINY_TAG', copy: 1, sizePt: 8, capMm: 2, weight: 'Regular', lineGap: .216, centre: [17, 17], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' };
  const BACK = Object.assign({ poolId: PA, text: '1. 27. 24', lines: ['1. 27. 24'] }, base), FRONT = Object.assign({ poolId: PA, face: 'front', text: 'W', lines: ['W'] }, base, { centre: [17, 17] });
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.confirm = () => true; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.SheetWin && SheetWin.cardBack && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(({ SH, PA, PB, BACK, FRONT }) => {
      CN.setMode('nest');
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      const charm = (id, poolId) => ({ id, name: poolId.split('_')[0] + ' · TAG', poolId, order: poolId.split('_')[0], metal: 'gold', centerPt: [17, 17], widthPt: 34, heightPt: 34, areaPt2: 34 * 34, outline: sq, members: [sq], thumb: '' });
      Object.assign(S.sheets.gold, { sheetId: SH, status: 'complete', charms: [charm('n1', PA), charm('n2', PB)], backPool: [BACK, FRONT],
        placements: [{ id: 'n1', cxPt: 60, cyPt: 60, angle: 0, wPt: 34, hPt: 34 }, { id: 'n2', cxPt: 150, cyPt: 60, angle: 0, wPt: 34, hPt: 34 }] });
      CN.showPage('gold', 0);
    }, { SH, PA, PB, BACK, FRONT });
    const card = '.sheetCard[data-m="gold"]';
    await page.waitForFunction(c => { const s = document.querySelector(c + ' [data-r="face"]'); return s && !s.hidden; }, card);
    const inkAt = () => page.evaluate(c => {
      const cv = document.querySelector(c + ' [data-r="canvas"]'), sh = S.sheets.gold, v = sh._view, ctx = cv.getContext('2d');
      const W = cv._face === 'back', sheetW = stockFor('gold', sh).wPt;
      const at = p => { const x = v.R + (W ? sheetW - p.cxPt : p.cxPt) * v.k, y = v.R + p.cyPt * v.k, h = Math.round(9 * v.k);
        const d = ctx.getImageData(Math.round(x) - h, Math.round(y) - h, 2 * h + 1, 2 * h + 1).data; let n = 0; for (let j = 0; j < d.length; j += 4) if (d[j] + d[j + 1] + d[j + 2] < 330) n++; return n; };
      return { face: cv._face || 'front', a: at(sh.placements[0]), b: at(sh.placements[1]) };
    }, card);
    // the plate's FRONT carries the front words ("W") in ink once the font is in; the plain charm stays plain
    const until = async (fn, ms, what) => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 150)); } };
    const f = await until(async () => { const v = await inkAt(); return v.a >= 4 ? v : null; }, 90000, 'front words in ink on the Front of the plate').catch(async e => {
      console.error('diagnostics', JSON.stringify(await page.evaluate(() => { const sh = S.sheets.gold; return { fonts: !!(window.Engrave && Engrave.fonts && Engrave.fonts.ok), ff: sh._frontFont, fh: sh._frontHas, backs: !!sh._backs, hasFront: SheetWin.cardBack.hasFront(sh), face: (document.querySelector('.sheetCard[data-m="gold"] [data-r="canvas"]') || {})._face || 'front' }; })));
      throw e; });
    assert.equal(f.b, 0, 'the plain charm stays plain from the front: ' + f.b);
    // the shelf and the pill
    const shelf = await page.evaluate(c => ({ faces: [...document.querySelectorAll(c + ' .backFace')].map(e => e.textContent), figures: document.querySelectorAll(c + ' .backPieces figure').length, label: (document.querySelector(c + ' [data-r="eng"] button') || document.body).getAttribute('aria-label') }), card);
    assert.deepEqual(shelf.faces.sort(), ['Back', 'Front'], 'the shelf names each picture\'s face: ' + JSON.stringify(shelf)); assert.equal(shelf.figures, 2);
    assert.equal(shelf.label, 'Engravings, 2 engravings', 'the pill counts engravings: ' + JSON.stringify(shelf));
    // and turned over: the back words, mirrored, in the same place of the same charm
    await page.click(`${card} [data-r="face"] [data-face="back"]`);
    await page.waitForFunction(c => { const cv = document.querySelector(c + ' [data-r="canvas"]'), seg = document.querySelector(c + ' [data-r="face"]');
      return cv._face === 'back' && !cv._turning && seg.querySelector('.spin').hidden && window.Engrave && Engrave.fonts && Engrave.fonts.ok; }, card, { timeout: 30000 });
    const b = await until(async () => { const v = await inkAt(); return v.a >= 4 ? v : null; }, 60000, 'back words in ink on the Back of the plate');
    assert.equal(b.b, 0, 'the plain charm stays plain from behind: ' + b.b);
    assert.deepEqual(errors, [], 'no page errors');
    console.log('ok   Nest card: Front of the plate shows the front words, Back the back words; shelf names Front/Back; pill "Engravings, 2 engravings"');
  } finally { await browser.close(); srv.close(); }
}
