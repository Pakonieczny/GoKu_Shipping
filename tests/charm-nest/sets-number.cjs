// How a sheet is numbered and named (Paul, 10 Oct 2026, sandbox Library):
//   "There are lot's of issues here with how the sheets are names ... there are 2 SS sheets both names #1"
//   the in-progress GF sheet read "Sheet 1" next to Set-1's "Sheet 1, 2, 3" and another read "Sheet 5" with no 4 anywhere.
// A. the sandbox as read (a fixture of the ten sheets: metal, set, draft, sheetIndex, page, file name) under the old reading and the one rule
// B. charm-nest-sheet-name.js alone: the name, a join, a leave, a return, the retired numbers, the qualified names, partial records
// C. the real charmNestLibrary handler over bridge-server.cjs's in-memory shop: a sheet that joins a committed set takes the next NUMBER IN THE SET
//    (never its page), nobody else is renumbered, a number that left is not given again, a stale writer cannot lower what is retired
// D. the staged dry run (scripts/sets-number-dryrun.cjs) over the same fixture writes nothing and says what the app would do
// E. every surface reads the one file (a source check)
// A fixture only: nothing here touches the live site, Etsy or a paid service.
//   node tests/charm-nest/sets-number.cjs
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path');
const { start } = require('./bridge-server.cjs');
const SN = require('../../charm-nest-sheet-name.js');
const SE = require('../../charm-nest-set-edit.js');
const DR = require('../../scripts/sets-number-dryrun.cjs');
const root = p => path.join(__dirname, '../..', p);
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs', TL = 'Order_Timeline';

// ── A. the sandbox as the Library listed it on 10 Oct 2026 (the fields that name a sheet; nothing else is kept) ──
const DAY = '2026-10-03', RUNID = 'run-2026-10-03-hnbk453eg5', SET1 = 'set-2026-10-03-1';
const row = (id, metal, o) => Object.assign({ id, metal, day: DAY, runId: RUNID, setId: null, draft: true, solidIncluded: null, sheetIndex: null, setSeq: null, laserDoneAt: null, roseCutAt: null, archived: false,
  fileBase: `${{ gold: 'GF', silver: 'SS', rose: 'RG', gold14k: '14K' }[metal]}_working_${id}` }, o);
const inSet1 = (id, metal, page, no, tag) => row(id, metal, { setId: SET1, draft: false, setSeq: 1, page, sheetIndex: no, fileBase: `${tag}_Oct.03.26_Set-1_Sheet-${no}` });
const SANDBOX = [
  row('silver-mv1n51oa', 'silver', { page: 1 }), row('rose-mv1n4r78-ppixrd4r78', 'rose', { page: 1 }), row('gold-mv1n4r6z', 'gold', { page: 1 }), row('gold-s5-mv1pbxaq', 'gold', { page: 5 }),
  inSet1('silver-s2-mv1o02uo', 'silver', 2, 1, 'SS'), inSet1('gold-s4-mv1oyjd9', 'gold', 4, 3, 'GF'), inSet1('gold-s3-mv1oh2q9', 'gold', 3, 2, 'GF'), inSet1('gold-s2-mv1nyiv7', 'gold', 2, 1, 'GF'),
  row('gold14k-mv1pwbh0-2nl6zvwbh0', 'gold14k', { page: 1, solidIncluded: false }), row('gold14k-mv1q06qf-oioj4z06qf', 'gold14k', { page: 2, solidIncluded: false })];
const SET_ONE = { setId: SET1, seq: 1, day: DAY, name: 'Set-1', committedAt: null, sheetIds: ['gold-s2-mv1nyiv7', 'gold-s3-mv1oh2q9', 'gold-s4-mv1oyjd9', 'silver-s2-mv1o02uo'] };

(async () => {
  // ═══ A. why the sandbox read "SS Sheet 1" twice, "GF Sheet 1" twice and "GF Sheet 5" with no 4 ═══
  const dupes = f => { const by = {}; for (const r of SANDBOX) (by[f(r)] = by[f(r)] || []).push(r.id); return Object.entries(by).filter(([, v]) => v.length > 1).map(([k]) => k).sort(); };
  assert.deepEqual(dupes(DR.legacyName), ['GF Sheet 1', 'SS Sheet 1'], 'before: sheetIndex || file number || page: two sheets of one metal read alike');
  assert.equal(DR.legacyName(SANDBOX[3]), 'GF Sheet 5', 'before: the draft read by its page in the run (5), a number Set-1 never had (it has 1, 2, 3)');
  assert.deepEqual(dupes(SN.name), [], 'after: no two sheets of the sandbox share a name');
  assert.deepEqual(Object.fromEntries(SANDBOX.map(r => [r.id, SN.name(r)])), {
    'silver-mv1n51oa': 'SS Draft 1', 'rose-mv1n4r78-ppixrd4r78': 'RG Draft 1', 'gold-mv1n4r6z': 'GF Draft 1', 'gold-s5-mv1pbxaq': 'GF Draft 5',
    'silver-s2-mv1o02uo': 'SS Sheet 1', 'gold-s4-mv1oyjd9': 'GF Sheet 3', 'gold-s3-mv1oh2q9': 'GF Sheet 2', 'gold-s2-mv1nyiv7': 'GF Sheet 1',
    'gold14k-mv1pwbh0-2nl6zvwbh0': '14K Draft 1', 'gold14k-mv1q06qf-oioj4z06qf': '14K Draft 2' });
  // the sheets of Set-1 keep the very numbers their file names and QR labels carry: nothing in the set is renumbered or relabelled
  for (const r of SANDBOX.filter(SN.inSet)) assert.equal(SN.numberOf(r), (/_Sheet-(\d+)/.exec(r.fileBase) || [])[1] * 1, r.id + ': the number is the file name\'s');
  // the page is NOT a number in the set: the run counted five GF pages, the set holds GF 1, 2, 3
  assert.deepEqual(SANDBOX.filter(r => r.metal === 'gold').map(r => r.page).sort(), [1, 2, 3, 4, 5]); assert.deepEqual(SANDBOX.filter(r => r.metal === 'gold' && SN.inSet(r)).map(SN.numberOf).sort(), [1, 2, 3]);
  assert.deepEqual([...SN.uniqueNames(SANDBOX).values()].sort(), SANDBOX.map(SN.name).sort(), 'no clash, so no name needs more words');

  // ═══ B. the module alone ═══
  const set = (id, no, o) => Object.assign({ id, metal: 'gold', setId: 'set-2026-10-03-1', setSeq: 1, draft: false, sheetIndex: no, page: no + 1, fileBase: `GF_Oct.03.26_Set-1_Sheet-${no}` }, o);
  assert.equal(SN.name(set('a', 3)), 'GF Sheet 3'); assert.equal(SN.short(set('a', 3)), 'Sheet 3');
  assert.equal(SN.name({ metal: 'silver', setId: null, draft: true, page: 1 }), 'SS Draft 1'); assert.equal(SN.name({ metal: 'gold14k', setId: null, draft: true, page: 2 }), '14K Draft 2');
  assert.equal(SN.name(set('a', 3, { solidIncluded: false })), 'GF Draft 4', 'a solid sheet that is not ticked in has no number in the set');
  assert.equal(SN.name(set('a', 2, { draft: true, sheetIndex: 2 })), 'GF Draft 3', 'a stale sheetIndex on a draft is not a number');
  // taken out of Set 1: a draft by its page, and it remembers where it was (its file name keeps that)
  const left = set('a', 3, { setId: null, draft: true, sheetIndex: null, page: 4 });
  assert.equal(SN.name(left), 'GF Draft 4'); assert.equal(SN.was(left), 'GF Sheet 3 of Set 1'); assert.equal(SN.was(set('a', 3)), ''); assert.equal(SN.was({ metal: 'gold', draft: true, setId: null, page: 1, fileBase: 'GF_working_x' }), '');
  // a cut sheet is a physical thing with the name on it: it keeps its number in a set or out of one
  assert.equal(SN.name(set('a', 2, { setId: null, draft: true, sheetIndex: null, laserDoneAt: 5 })), 'GF Sheet 2'); assert.equal(SN.name({ metal: 'rose', setId: null, draft: true, page: 1, roseCutAt: 7, fileBase: 'RG_Oct.03.26_Set-1_Sheet-1' }), 'RG Sheet 1');
  // a partial read (no setId, no draft: the masked sheet read of the remnants and the timeline) is read by the number it keeps, as it always was
  assert.equal(SN.name({ metal: 'gold', sheetIndex: 3, page: 5 }), 'GF Sheet 3'); assert.equal(SN.name({ metal: 'gold', fileBase: 'GF_Oct.03.26_Set-1_Sheet-2', page: 4 }), 'GF Sheet 2'); assert.equal(SN.name({ metal: 'gold', page: 4 }), 'GF Draft 4');
  assert.equal(SN.name({ metalLabel: 'GF', setId: null, draft: true, page: 2 }), 'GF Draft 2', 'the metal code of a record that has no metal: its label'); assert.equal(SN.name(null), '');
  // the file name's tag
  assert.deepEqual(SN.fileTag({ fileBase: 'GF_Oct.03.26_Set-12_Sheet-4' }), { code: 'GF', set: 12, no: 4 }); assert.equal(SN.fileTag({ fileBase: 'GF_working_gold-s5-mv1pbxaq' }), null);

  // join: the next number after every number in use AND every number the set ever gave (retired), per metal; the page is never used
  const members = [set('a', 1), set('b', 2), set('c', 3), { id: 's', metal: 'silver', setId: 'x', draft: false, sheetIndex: 1 }];
  assert.equal(SN.nextNumber(members, 'gold', null), 4); assert.equal(SN.nextNumber(members, 'silver', null), 2); assert.equal(SN.nextNumber(members, 'rose', null), 1, 'each metal counts alone');
  assert.equal(SN.nextNumber(members, 'gold', { gold: 6 }), 7, 'a number that left the set is retired: a new sheet does not take it'); assert.equal(SN.nextNumber([], 'gold', {}), 1);
  const draft5 = { id: 'd', metal: 'gold', setId: null, draft: true, page: 5, fileBase: 'GF_working_d' };
  assert.equal(SN.claim(draft5, members, null, n => `GF_Oct.03.26_Set-1_Sheet-${n}`), 4, 'a draft that was never in the set: page 5 is not its number, 4 is');
  // return: a sheet that left takes its own number back when nobody holds it; if the number was given on, it takes the next
  const gone = set('g', 3, { setId: null, draft: true, sheetIndex: null, page: 4 }), without = members.filter(m => m.id !== 'c');
  assert.equal(SN.claim(gone, without, SN.raise({ gold: 3 }, []), n => `GF_Oct.03.26_Set-1_Sheet-${n}`), 3, 'it comes back as GF Sheet 3');
  assert.equal(SN.claim(gone, without.concat(set('n', 3)), null, n => `GF_Oct.03.26_Set-1_Sheet-${n}`), 4, 'sheet 3 is held by another: it takes 4');
  assert.equal(SN.claim(gone, without, null, n => `GF_Oct.03.26_Set-9_Sheet-${n}`), 3, 'its file name is from another set (or another day): it is a new sheet here, so the next number (2 is the highest) is 3');
  assert.equal(SN.claim(Object.assign({}, gone, { fileBase: 'GF_Oct.03.26_Set-9_Sheet-3' }), without, { gold: 3 }, n => `GF_Oct.03.26_Set-1_Sheet-${n}`), 4, 'from another set it never reclaims; 3 is retired here');
  assert.equal(SN.claim(gone, without, null), 3, 'without a file-name rule a sheet always takes the next number');
  // leave: the highest only goes up, whoever writes it
  assert.deepEqual(SN.raise({ gold: 2 }, [set('x', 5)]), { gold: 5 }); assert.deepEqual(SN.raise({ gold: 9 }, [set('x', 5)]), { gold: 9 }); const given = { gold: 4 }; SN.raise(given, [set('x', 8)]); assert.deepEqual(given, { gold: 4 }, 'the input is left as it was');
  assert.deepEqual(SN.mergeNos({ gold: 5, silver: 2 }, { gold: 1, rose: 3 }), { gold: 5, silver: 2, rose: 3 }, 'a stale copy cannot lower a retired number'); assert.deepEqual(SN.mergeNos(undefined, { gold: 'x', silver: 0, rose: 2 }), { rose: 2 }, 'junk is dropped');
  // one board of several sets or runs: only the names that would read alike get more words
  const two = [set('p', 1), set('q', 1, { setId: 'set-2026-10-03-2', setSeq: 2 }), set('r', 2)], un = SN.uniqueNames(two);
  assert.deepEqual([un.get('p'), un.get('q'), un.get('r')], ['GF Sheet 1 · Set 1', 'GF Sheet 1 · Set 2', 'GF Sheet 2'], 'the clash is told apart by its set; the sheet that is alone keeps its short name');
  const dr = SN.uniqueNames([{ id: 'x', metal: 'gold', setId: null, draft: true, page: 1, day: '2026-10-03' }, { id: 'y', metal: 'gold', setId: null, draft: true, page: 1, day: '2026-10-04' }]);
  assert.deepEqual([dr.get('x'), dr.get('y')], ['GF Draft 1 · Oct 3', 'GF Draft 1 · Oct 4'], 'two runs\' drafts: by the day');
  const same = SN.uniqueNames([set('m', 1), set('n', 1)]); assert.deepEqual([same.get('m'), same.get('n')], ['GF Sheet 1 · Set 1 #1', 'GF Sheet 1 · Set 1 #2'], 'a damaged set that holds two alike is still told apart');

  // ═══ C. the real handler: a sheet that joins a committed set takes its next number in the SET ═══
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now };
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const edit = moves => post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'setMember', moves: moves.map(([sheetId, to]) => ({ sheetId, to })) }] });
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const TAG = { gold: 'GF', silver: 'SS' }, fileFor = (m, n) => `${TAG[m]}_Oct.03.26_Set-1_Sheet-${n}`;
  const mk = (id, metal, piece, o = {}) => st.put(S, id, { id, runId: 'run-x', metal, day: DAY, status: 'complete', draft: !o.setId, placedCount: 1, charmCount: 1, density: .36, stock: { wIn: 6, hIn: 4.5 }, poolIds: [piece], orders: [piece.split('_')[0]],
    verification: { ok: true }, outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } }, solidIncluded: null, sheetIndex: null, updatedAt: ts, createdAt: ts, ...o });
  const inSet = (id, metal, piece, no, page) => mk(id, metal, piece, { setId: 'set-1', setSeq: 1, sheetIndex: no, page, fileBase: fileFor(metal, no), folder: fileFor(metal, no), processSeals: [{ how: 'approved', by: 'Ann', at: now - 6000 }], processReady: true });
  const working = (id, metal, piece, page) => mk(id, metal, piece, { page, fileBase: `${TAG[metal]}_working_${id}`, folder: `${TAG[metal]}_working_${id}` });
  const file = sheetId => ({ sheetId, sheet: sheetId, path: `set-1/${sheetId}-qr.png`, url: image, payload: 'p-' + sheetId, orders: [] });
  // Set 1 (committed): GF 1, 2, 3 (pages 2, 3, 4 of the run) and SS 1 (page 2); the drafts beside it: GF page 5, GF page 1, SS page 1
  st.put(SET, 'set-1', { setId: 'set-1', seq: 1, day: DAY, runId: 'run-x', sheetIds: ['gf-1', 'gf-2', 'gf-3', 'ss-1'], materials: ['gold', 'silver'], orders: {}, status: 'complete', committedAt: now - 9000, committed: [], labelFiles: ['gf-1', 'gf-2', 'gf-3', 'ss-1'].map(file),
    labels: { pdf: { path: 'x.pdf', url: 'u' } }, processSeals: [{ how: 'approved', by: 'Ann', at: now - 5000 }], updatedAt: ts, createdAt: ts });
  inSet('gf-1', 'gold', '5100000001_1_1', 1, 2); inSet('gf-2', 'gold', '5100000002_1_1', 2, 3); inSet('gf-3', 'gold', '5100000003_1_1', 3, 4); inSet('ss-1', 'silver', '5100000004_1_1', 1, 2);
  working('dr-gf5', 'gold', '5100000010_1_1', 5); working('dr-gf1', 'gold', '5100000011_1_1', 1); working('dr-ss1', 'silver', '5100000012_1_1', 1); working('dr-gf6', 'gold', '5100000013_1_1', 6);
  st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines: {} });
  const sheet = id => st.doc(S, id), setDoc = () => st.doc(SET, 'set-1'), nameOf = id => SN.name(sheet(id));
  const kept = ids => JSON.stringify(ids.map(i => { const x = sheet(i); return [x.sheetIndex, x.fileBase, x.page, x.processSeals, x.label]; }));
  const noteOf = (sheetId, re) => st.list(TL).some(e => e.sheetId === sheetId && re.test(e.text || ''));
  assert.deepEqual(['gf-1', 'gf-2', 'gf-3', 'ss-1', 'dr-gf5', 'dr-gf1', 'dr-ss1'].map(nameOf), ['GF Sheet 1', 'GF Sheet 2', 'GF Sheet 3', 'SS Sheet 1', 'GF Draft 5', 'GF Draft 1', 'SS Draft 1'], 'the scene reads as the sandbox does under the rule');
  const untouched = kept(['gf-1', 'gf-2', 'gf-3', 'ss-1']);

  // the draft that is page 5 of the run joins: it is GF Sheet 4 of the set, not GF Sheet 5, and the file name says so
  let r = await edit([['dr-gf5', 'set-1']]); assert.equal(r.status, 200, JSON.stringify(r));
  assert.equal(sheet('dr-gf5').sheetIndex, 4); assert.equal(sheet('dr-gf5').fileBase, fileFor('gold', 4)); assert.equal(sheet('dr-gf5').page, 5, 'its page in the run is as it was: a page is not a number in the set'); assert.equal(nameOf('dr-gf5'), 'GF Sheet 4');
  assert.equal(sheet('dr-gf5').draft, false); assert.equal(sheet('dr-gf5').label, null, 'its QR label is made again by the page (the number is on it)');
  assert(noteOf('dr-gf5', /^put in Set 1 · GF Sheet 4$/), 'the order timeline says GF Sheet 4, not 5: ' + JSON.stringify(st.list(TL).map(e => e.text)));
  assert.equal(kept(['gf-1', 'gf-2', 'gf-3', 'ss-1']), untouched, 'nobody else in the set is renumbered, renamed or relabelled when a sheet joins');
  assert.deepEqual(setDoc().sheetNos, { gold: 4, silver: 1 }, 'the set remembers the highest number it gave to each metal');
  // the draft that is page 1 of the run: GF Sheet 5 (its page is 1, which is a number the set already holds). A silver sheet counts silver alone.
  r = await edit([['dr-gf1', 'set-1']]); assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(sheet('dr-gf1').sheetIndex, 5); assert.equal(sheet('dr-gf1').fileBase, fileFor('gold', 5)); assert.equal(nameOf('dr-gf1'), 'GF Sheet 5');
  r = await edit([['dr-ss1', 'set-1']]); assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(sheet('dr-ss1').sheetIndex, 2); assert.equal(nameOf('dr-ss1'), 'SS Sheet 2'); assert.equal(sheet('dr-ss1').fileBase, fileFor('silver', 2));
  const names = ['gf-1', 'gf-2', 'gf-3', 'dr-gf5', 'dr-gf1', 'ss-1', 'dr-ss1'].map(nameOf); assert.deepEqual(names, ['GF Sheet 1', 'GF Sheet 2', 'GF Sheet 3', 'GF Sheet 4', 'GF Sheet 5', 'SS Sheet 1', 'SS Sheet 2'], 'in the set: GF 1 to 5 and SS 1, 2, no clash, no gap'); assert.equal(new Set(names).size, names.length);
  assert.deepEqual(setDoc().sheetNos, { gold: 5, silver: 2 });
  // GF Sheet 5 leaves: it is a draft again (GF Draft 1 by its page), remembers where it was, and its number is retired
  r = await edit([['dr-gf1', null]]); assert.equal(r.status, 200, JSON.stringify(r)); const out = sheet('dr-gf1');
  assert.equal(out.draft, true); assert.equal(out.sheetIndex, null); assert.equal(nameOf('dr-gf1'), 'GF Draft 1'); assert.equal(SN.was(out), 'GF Sheet 5 of Set 1', 'it remembers: ' + out.fileBase);
  assert.equal(kept(['gf-1', 'gf-2', 'gf-3']), kept(['gf-1', 'gf-2', 'gf-3']));
  assert.deepEqual(['gf-1', 'gf-2', 'gf-3', 'dr-gf5'].map(nameOf), ['GF Sheet 1', 'GF Sheet 2', 'GF Sheet 3', 'GF Sheet 4'], 'nobody renumbers when a sheet leaves');
  assert.deepEqual(setDoc().sheetNos, { gold: 5, silver: 2 }, 'the highest number stays, so 5 is not given to anyone else');
  // a different draft joins now: GF Sheet 6, not 5 (a printed QR label or laser file may still say 5)
  r = await edit([['dr-gf6', 'set-1']]); assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(sheet('dr-gf6').sheetIndex, 6); assert.equal(nameOf('dr-gf6'), 'GF Sheet 6'); assert.equal(sheet('dr-gf6').fileBase, fileFor('gold', 6));
  // the sheet that left comes back: it takes ITS number again when free (5 is free: nobody took it)
  r = await edit([['dr-gf1', 'set-1']]); assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(sheet('dr-gf1').sheetIndex, 5); assert.equal(nameOf('dr-gf1'), 'GF Sheet 5'); assert.equal(sheet('dr-gf1').fileBase, fileFor('gold', 5), 'the same file name, the same QR label text');
  assert.equal(new Set(['gf-1', 'gf-2', 'gf-3', 'dr-gf5', 'dr-gf1', 'dr-gf6'].map(nameOf)).size, 6, 'no two GF sheets of the set share a name');
  // the highest leaves, and a NEW sheet joins: it does not take the number of the one that left
  r = await edit([['dr-gf6', null]]); assert.equal(r.status, 200, JSON.stringify(r)); st.put(S, 'dr-gf7', { ...sheet('dr-gf6'), id: 'dr-gf7', poolIds: ['5100000014_1_1'], orders: ['5100000014'], page: 7, fileBase: 'GF_working_d7', folder: 'GF_working_d7' });
  r = await edit([['dr-gf7', 'set-1']]); assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(sheet('dr-gf7').sheetIndex, 7, 'GF Sheet 6 left: 6 stays retired'); assert.equal(nameOf('dr-gf7'), 'GF Sheet 7');
  assert.equal(sheet('dr-gf6').draft, true); assert.equal(SN.was(sheet('dr-gf6')), 'GF Sheet 6 of Set 1');
  // a page holding an older copy of the set cannot lower what is retired (setUpdate max-merges `sheetNos`)
  const high = JSON.stringify(setDoc().sheetNos);
  r = await post({ op: 'setUpdate', setId: 'set-1', patch: { note: 'saved by an old page', sheetNos: { gold: 1 } } }); assert.equal(r.status, 200, JSON.stringify(r));
  assert.equal(JSON.stringify(setDoc().sheetNos), high, 'a stale writer cannot lower the numbers a set gave out'); assert.equal(setDoc().note, 'saved by an old page', 'the rest of what it saved is kept');
  r = await post({ op: 'setUpdate', setId: 'set-1', patch: { sheetNos: { gold: 20 } } }); assert.equal(setDoc().sheetNos.gold, 20, 'a higher number is kept');
  // the order timeline, the sheet search and the readiness words call the sheet by the same rule (the server's labelOf/sheetLabel are the module)
  const lib = fs.readFileSync(root('netlify/functions/charmNestLibrary.js'), 'utf8');
  assert(/require\("\.\.\/\.\.\/charm-nest-sheet-name\.js"\)/.test(lib), 'the Library handler requires the one file'); assert(/sheetLabel[\s\S]{0,400}SheetName\.name\(d\)/.test(lib));
  assert(noteOf('dr-gf7', /^put in Set 1 · GF Sheet 7$/)); assert(noteOf('dr-gf6', /^taken out of Set 1 · GF Sheet 6$/), 'a sheet that left is named as it was in the set when it left: ' + JSON.stringify(st.list(TL).filter(e => e.sheetId === 'dr-gf6').map(e => e.text)));
  // nothing sealed or approved was touched by any of it
  for (const id of ['gf-1', 'gf-2', 'gf-3', 'ss-1']) assert.deepEqual(sheet(id).processSeals, [{ how: 'approved', by: 'Ann', at: now - 6000 }], id + ': the seal is permanent');
  assert.deepEqual(setDoc().processSeals, [{ how: 'approved', by: 'Ann', at: now - 5000 }]); assert.equal(setDoc().committedAt, now - 9000);
  // no nested array reached the shop (Firestore refuses them)
  const nested = (v, p = '') => Array.isArray(v) ? v.flatMap((x, i) => Array.isArray(x) ? [p + '[' + i + ']'] : nested(x, p + '[' + i + ']')) : v && typeof v === 'object' ? Object.entries(v).flatMap(([k, x]) => nested(x, p + '.' + k)) : [];
  assert.deepEqual(nested(setDoc()), []);
  await srv.close();

  // ═══ D. the staged dry run writes nothing and says what the app would do ═══
  const before = JSON.stringify(SANDBOX), plan = DR.plan(SANDBOX, [SET_ONE], ['gold-s5-mv1pbxaq:' + SET1, 'gold-mv1n4r6z:' + SET1]);
  assert.equal(JSON.stringify(SANDBOX), before, 'the plan leaves the read as it was');
  assert.deepEqual(plan.clashesBefore.map(c => c.name).sort(), ['GF Sheet 1', 'SS Sheet 1']); assert.deepEqual(plan.clashesAfter, []);
  const rowOf = id => plan.rows.find(x => x.id === id);
  assert.deepEqual([rowOf('silver-mv1n51oa').before, rowOf('silver-mv1n51oa').after, rowOf('silver-mv1n51oa').changes], ['SS Sheet 1', 'SS Draft 1', true]); assert.equal(rowOf('gold-s2-mv1nyiv7').changes, false, 'Set-1\'s own sheets are not renamed');
  assert.deepEqual(plan.rows.filter(x => x.changes).map(x => x.id).sort(), ['gold-mv1n4r6z', 'gold-s5-mv1pbxaq', 'gold14k-mv1pwbh0-2nl6zvwbh0', 'gold14k-mv1q06qf-oioj4z06qf', 'rose-mv1n4r78-ppixrd4r78', 'silver-mv1n51oa'], 'only the drafts are read differently');
  assert(plan.rows.every(x => /^none/.test(x.stored)), 'no stored number changes: the name is read from the record'); assert(plan.rows.every(x => x.why.length > 10));
  const j = plan.joins[0]; assert.deepEqual([j.number, j.name, j.fileBase], [4, 'GF Sheet 4', 'GF_Oct.03.26_Set-1_Sheet-4'], 'the draft of page 5 would be GF Sheet 4'); assert.equal(plan.joins[1].number, 4);
  assert.equal(j.allowed, false, 'Set-1 is the run\'s own open set: the Library move refuses it (the run page includes the sheet there)'); assert.match(j.why, /still being made by its run/);
  assert.match(fs.readFileSync(root('scripts/sets-number-dryrun.cjs'), 'utf8'), /^(?![\s\S]*(fetch\(|https\.request|http\.request|writeFile|appendFile|firebase))/, 'the script has no network and writes no file');

  // ═══ E. every surface reads the one file ═══
  const page = fs.readFileSync(root('charm-nest-1.html'), 'utf8'), build = fs.readFileSync(root('scripts/build-public.cjs'), 'utf8');
  assert(/<script src="charm-nest-sheet-name\.js\?v=[^"]+"><\/script>/.test(page), 'the page loads the file'); assert(page.indexOf('charm-nest-sheet-name.js') < page.indexOf('charm-nest-readiness.js'), 'before the files that read it');
  assert(/charm-nest-sheet-name\.js/.test(build), 'the Netlify build publishes it');
  for (const f of ['charm-nest-set-edit.js', 'charm-nest-flow.js', 'charm-nest-sheetwin.js', 'charm-nest-search.js', 'charm-nest-library.js', 'charm-nest-bridge.js', 'charm-nest-flow-rose.js', 'charm-nest-laser-act.js', 'netlify/functions/_charmNestPlacement.js', 'netlify/functions/_orderTimeline.js'])
    assert(/CharmNestSheetName|charm-nest-sheet-name/.test(fs.readFileSync(root(f), 'utf8')), f + ' names a sheet with the one file');
  // the modern join writers never use the page as the number
  const set0 = fs.readFileSync(root('charm-nest-set-edit.js'), 'utf8'), idx = set0.slice(set0.indexOf('function indexFor'), set0.indexOf('function indexFor') + 900); assert(!/\.page/.test(idx), 'indexFor does not read the page');
  console.log('sets-number: ok');
})().catch(e => { console.error(e); process.exit(1); });
