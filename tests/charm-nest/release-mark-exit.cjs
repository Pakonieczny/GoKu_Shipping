// RELEASEMARK (Paul, 20 Nov 2026: "Figure out why none of these sheets have been made into a set. There's already three full Gold Filled sheets and a full silver sheet ...").
// The live records of the Nov 12 run: GF Draft 1 (gold-s15, 78%) and GF Draft 2 (gold-s16, 73%) were drafts whose records named Set 5 (set-2026-11-12-1: setId, setSeq 5, sheetIndex 6 / 11,
// the day 2026-11-12 and Set 5's folder for their files) although Set 5 never listed them, and they held no release mark. The stamp came from a computer that does not hold the run (a follower:
// B.run is none, so Gate.modern() is false): nextSheetSeq's old branch asked Sets.ensure for the run's dispatch set, which is Set 5's record (key "<run>|dispatch"), and numbered the draft into it;
// persistSheet then filed its files under Set 5 and membershipOf (old rules) saved the set on a draft. A save from such a copy also recomputed the release mark from that copy.
// Each scene reproduces one step of it, then checks the fix:
//   A  the server (putSheet, in the in-memory cloud of _cloudFakes.cjs): a draft names no set; a layout save naming a set that does not list the sheet keeps the record's membership (the save is
//      not refused, the release mark and files go in); a released sheet stays released unless a person reopens it (reopen) or the sheet is emptied; a join and a person's take-out go as before
//   B  charm-nest-finish-core.js membershipOf: a draft is saved with no set, number or index (a take-out of that very set under way excepted)
//   C  the page's nextSheetSeq (charm-nest-1.html, in a vm): a computer that does not hold the run numbers no set; the run's carrier of an old-rules run still does
//   D  the page's setFiled / keptAnswer: files go to a set's folder only when the set lists the sheet; the cloud's keptFull / membershipKept are taken back
//   E  charm-nest-place.js passedOver and the page's nospreadSettle: the turn-away is read from the sheets, so a reload or another carrier does not lose it, once per piece count
//   F  charm-nest-assembly-core.js on the records: with its release mark kept, GF Draft 1 forms a set with GF Draft 3 (2 completed GF sheets), its old Set 5 stamp notwithstanding
// Offline: no network, no Etsy, no live cloud.   node tests/charm-nest/release-mark-exit.cjs
'use strict';
process.env.EDIT_PASSCODE = 'test-pass';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const ROOT = path.join(__dirname, '../..');
const T0 = Date.UTC(2026, 10, 20, 15, 0, 0);
const F = require('./_cloudFakes.cjs')({ now: T0 });
let fails = 0;
const check = async (id, fn) => { try { await fn(); console.log('PASS  ' + id); } catch (e) { fails++; console.log('FAIL  ' + id + '  ' + ((e && e.message) || e)); } };
const handlerOf = lib => async body => { const r = await lib.handler({ httpMethod: 'POST', headers: { 'x-edit-passcode': 'test-pass' }, body: JSON.stringify(body) }); let o = {}; try { o = JSON.parse(r.body || '{}'); } catch (_) {} return Object.assign({ status: r.statusCode }, o); };
const html = fs.readFileSync(path.join(ROOT, 'charm-nest-1.html'), 'utf8');
const part = (src, a, b) => { const i = src.indexOf(a); assert(i >= 0, 'not found: ' + a); const j = src.indexOf(b, i); assert(j > i, 'not found: ' + b); return src.slice(i, j); };

const RUN = 'run-2026-11-12-wrtgl8fukc', SET5 = 'set-2026-11-12-1', S15 = 'gold-s15-mwmg3fo9';
const charms = (n, suf = '') => Array.from({ length: n }, (_, i) => ({ id: 'c' + i, poolId: `42375${i}${suf}_1_1`, order: `42375${i}${suf}`, areaPt2: 500, widthPt: 25, heightPt: 25 }));
const placed = n => Array.from({ length: n }, (_, i) => ({ id: 'c' + i, cxPt: 10 + i, cyPt: 10, angle: 0 }));
const layout = (n, suf) => ({ charms: charms(n, suf), placements: placed(n), poolIds: charms(n, suf).map(c => c.poolId) });

(async () => {
  /* ═══ A · the server ═══ */
  const seedWorld = () => {
    F.reset();
    F.seed('Charm_Nest_Runs/' + RUN, { runId: RUN, releasePolicy: 2, status: 'processed', step: 'complete', day: '2026-11-18', sheets: {}, holds: {}, errors: [], orders: [] });
    F.seed('Charm_Nest_Runs/run-old', { runId: 'run-old', status: 'processed', day: '2026-09-20', sheets: {}, holds: {}, errors: [], orders: [] });
    F.seed('Charm_Nest_Sets/' + SET5, { setId: SET5, runId: RUN, day: '2026-11-12', seq: 5, name: 'Set-5', group: 'dispatch', key: RUN + '|dispatch', status: 'awaiting review', formedAt: T0 - 4 * 86400000, sheetIds: ['gold-s8-mwdb8jkv', 'gold-s9-mweaiu9f'], materials: ['gold'], orders: {}, sheetNos: { gold: 6 } });
    F.seed('Charm_Nest_Sets/set-new', { setId: 'set-new', runId: RUN, day: '2026-11-20', seq: 1, name: 'Set-1', group: 'dispatch', status: 'open', sheetIds: [], materials: [], orders: {} });
    F.seed('Charm_Nest_Sheets/' + S15, Object.assign({ id: S15, runId: RUN, metal: 'gold', page: 15, day: '2026-11-18', draft: true, setId: null, setSeq: null, sheetIndex: null, releaseFull: true, density: 0.7768, status: 'complete', fileBase: 'GF_working_' + S15, layoutRev: 60, archived: false }, layout(3)));
  };
  const lib = F.load('charmNestLibrary.js', { fresh: true }), P = handlerOf(lib), rec = id => F.doc('Charm_Nest_Sheets/' + id);

  await check('A1 the stamp, as the follower saved it (a draft naming Set 5, its number and index, the release mark recomputed false): the record names no set and stays released', async () => {
    seedWorld();
    const r = await P({ op: 'putSheet', sheet: Object.assign({ id: S15, runId: RUN, metal: 'gold', draft: true, setId: SET5, setSeq: 5, sheetIndex: 6, releaseFull: false, fileBase: 'GF_working_' + S15, day: '2026-11-12' }, layout(3)), layoutBase: 60 });
    assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(r.keptFull, true, 'answer says keptFull');
    const d = rec(S15); assert.equal(d.draft, true); assert.equal(d.setId, null); assert.equal(d.setSeq, null); assert.equal(d.sheetIndex, null); assert.equal(d.releaseFull, true);
  });
  await check('A2 an older page that named the draft "Set-5 Sheet-4" (draft false, Set 5 does not list it): the save is not refused; the record keeps its own membership (a draft of no set), the layout and the release mark go in', async () => {
    seedWorld();
    const r = await P({ op: 'putSheet', sheet: Object.assign({ id: S15, runId: RUN, metal: 'gold', draft: false, setId: SET5, setSeq: 5, sheetIndex: 4, releaseFull: true, fileBase: 'GF_Nov.12.26_Set-5_Sheet-4' }, layout(4)), layoutBase: 60 });
    assert.equal(r.status, 200, JSON.stringify(r)); assert.deepEqual(r.membershipKept, { asked: SET5, kept: null });
    const d = rec(S15); assert.equal(d.draft, true); assert.equal(d.setId, null); assert.equal(d.placements.length, 4); assert.equal(d.releaseFull, true);
    assert.deepEqual(F.doc('Charm_Nest_Sets/' + SET5).sheetIds, ['gold-s8-mwdb8jkv', 'gold-s9-mweaiu9f'], 'Set 5 untouched');
  });
  await check('A3 a person reopening it (reopen: true, the sheet window\'s undo of its own Make QR label, a Merge) takes the mark off; a save that empties the sheet does too', async () => {
    seedWorld();
    let r = await P({ op: 'putSheet', sheet: { id: S15, releaseFull: false, topup: null }, reopen: true });
    assert.equal(r.status, 200); assert.equal(r.keptFull, undefined); assert.equal(rec(S15).releaseFull, false);
    seedWorld();
    r = await P({ op: 'putSheet', sheet: { id: S15, runId: RUN, metal: 'gold', releaseFull: false, charms: [], placements: [], poolIds: [] }, layoutBase: 60 });
    assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(rec(S15).releaseFull, false);
  });
  await check('A4 a join is its own patch (no layout) and goes through the set rules as before: a full draft joins the open set', async () => {
    seedWorld();
    const r = await P({ op: 'putSheet', sheet: { id: S15, draft: false, setId: 'set-new', setSeq: 1, sheetIndex: 1, fileBase: 'GF_Nov.20.26_Set-1_Sheet-1', folder: 'GF_Nov.20.26_Set-1_Sheet-1', solidIncluded: null } });
    assert.equal(r.status, 200, JSON.stringify(r)); assert.equal(r.membershipKept, undefined);
    const d = rec(S15); assert.equal(d.draft, false); assert.equal(d.setId, 'set-new'); assert.equal(d.releaseFull, true);
  });
  await check('A5 a person\'s take-out of that very set under way (draft, leftSet names the set) keeps the set\'s name until the run lets it go (PERSONOUT)', async () => {
    seedWorld();
    F.seed('Charm_Nest_Sheets/' + S15, Object.assign(rec(S15), { draft: false, setId: 'set-new', setSeq: 1, sheetIndex: 1 }));
    const r = await P({ op: 'putSheet', sheet: { id: S15, draft: true, setId: 'set-new', leftSet: 'set-new' } });
    assert.equal(r.status, 200, JSON.stringify(r)); const d = rec(S15); assert.equal(d.draft, true); assert.equal(d.setId, 'set-new');
  });
  await check('A6 a run of the old rules (no releasePolicy 2) saves a layout naming a set as before: it goes through the set rules (no membership kept back)', async () => {
    seedWorld();
    F.seed('Charm_Nest_Sheets/old-1', Object.assign({ id: 'old-1', runId: 'run-old', metal: 'gold', page: 1, draft: true, setId: null, releaseFull: true, status: 'complete', archived: false, layoutRev: 1 }, layout(2)));
    const r = await P({ op: 'putSheet', sheet: Object.assign({ id: 'old-1', runId: 'run-old', metal: 'gold', draft: false, setId: 'set-new', setSeq: 1, sheetIndex: 1, releaseFull: true }, layout(2)), layoutBase: 1 });
    assert.equal(r.membershipKept, undefined, JSON.stringify(r));
  });

  /* ═══ B · membershipOf ═══ */
  const FC = require(path.join(ROOT, 'charm-nest-finish-core.js')), SR = require(path.join(ROOT, 'charm-nest-set-rules.js'));
  const host = (sets, modern) => ({ SetRules: SR, setsOfRun: () => sets, modern: () => modern });
  const page = o => Object.assign({ sheetId: S15, metal: 'gold', runId: RUN, draft: true, setId: SET5, seq: 5, sheetIndex: 6, placements: [{ id: 'a' }], releaseFull: true }, o || {});
  const NONE = { draft: true, setId: null, setSeq: null, sheetIndex: null };
  await check('B1 a draft that names Set 5 (full or not, old rules or new) is saved with no set, number or index', async () => {
    for (const modern of [true, false]) for (const releaseFull of [true, false]) assert.deepEqual(FC.membershipOf(page({ releaseFull }), host([{ setId: SET5, sheetIds: ['gold-s8-mwdb8jkv'] }], modern)), NONE, `modern ${modern} full ${releaseFull}`);
  });
  await check('B2 a member stays a member (the set lists it); a take-out under way (leftSet = that set) keeps the name; a plain draft keeps its own number', async () => {
    assert.equal(FC.membershipOf(page({ draft: false }), host([{ setId: SET5, sheetIds: [S15] }], true)).setId, SET5);
    assert.equal(FC.membershipOf(page({ leftSet: SET5 }), host([], true)).setId, SET5);
    assert.deepEqual(FC.membershipOf(page({ setId: null, seq: 15, sheetIndex: null }), host([], true)), { draft: true, setId: null, setSeq: 15, sheetIndex: null });
  });

  /* ═══ C · the page's nextSheetSeq ═══ */
  const seqCtx = (run, sets) => {
    const c = { console, ensured: [], RUN, SET5 }; vm.createContext(c);
    c.__run = run; c.__sets = sets;
    vm.runInContext(`
      const window = { B: { run: __run }, Gate: { modern: id => !!window.B.run && window.B.run.releasePolicy === 2 && (!id || id === window.B.run.runId), solidSelected: () => true, committedSheet: () => false, keepsPlace: () => false }, CharmNestSheetName: null };
      const B = window.B, Gate = window.Gate, S = { cloud: { ok: true } };
      const today = () => '2026-11-20', uid = () => 'u1';
      const Sets = { ensure: async (runKey, group) => { ensured.push(runKey + '|' + group); return __sets[runKey + '|' + group] || null; } };
      const pagesOf = () => [];
    `, c);
    vm.runInContext(part(html, 'async function nextSheetSeq(sh) {', '/** The listings a sheet\'s pieces were bought from'), c);
    return c;
  };
  const set5 = { setId: SET5, seq: 5, day: '2026-11-12', sheetNos: { gold: 5 } };
  await check('C1 a follower (no run of its own) numbers no set: its new page and its draft stay drafts of no set, Sets.ensure is never asked', async () => {
    const c = seqCtx(null, { [RUN + '|dispatch']: set5 });
    for (const draft of [undefined, true]) {
      c.__sh = { metal: 'gold', page: 15, runId: RUN, sheetId: S15, group: 'dispatch', draft, setId: draft ? SET5 : undefined, seq: draft ? 5 : undefined };
      const seq = await vm.runInContext('nextSheetSeq(__sh)', c);
      assert.equal(seq, null); assert.equal(c.__sh.draft, true); assert.equal(c.__sh.setId, null); assert.equal(c.__sh.sheetIndex, null);
    }
    assert.deepEqual(c.ensured, []);
  });
  await check('C2 a computer holding another run is a follower of this one too; a sheet in a set keeps its place there', async () => {
    const c = seqCtx({ runId: 'run-other', releasePolicy: 2 }, { [RUN + '|dispatch']: set5 });
    c.__sh = { metal: 'gold', page: 8, runId: RUN, sheetId: 'gold-s8', group: 'dispatch', draft: false, setId: SET5, seq: 5, sheetIndex: 1 };
    assert.equal(await vm.runInContext('nextSheetSeq(__sh)', c), 5); assert.equal(c.__sh.setId, SET5); assert.deepEqual(c.ensured, []);
  });
  await check('C3 the carrier of a run of the old rules still numbers its sheet into the run\'s set (unchanged)', async () => {
    const c = seqCtx({ runId: 'run-old', releasePolicy: 1 }, { 'run-old|dispatch': { setId: 'set-old', seq: 2, day: '2026-09-20' } });
    c.__sh = { metal: 'gold', page: 1, runId: 'run-old', sheetId: 'old-1', group: 'dispatch' };
    assert.equal(await vm.runInContext('nextSheetSeq(__sh)', c), 2); assert.equal(c.__sh.setId, 'set-old'); assert.deepEqual(c.ensured, ['run-old|dispatch']);
  });

  /* ═══ D · the page's setFiled and keptAnswer ═══ */
  await check('D1 setFiled: a set\'s folder only for a member its set lists; a draft or an unlisted sheet is filed as a sheet of its own day; an old-rules carrier files by its set', async () => {
    const c = { console }; vm.createContext(c);
    vm.runInContext(`const B = { run: { runId: '${RUN}', releasePolicy: 2 } }, Sets = { ofRun: () => [{ setId: '${SET5}', sheetIds: ['gold-s8'] }] }, window = { B, Sets };`, c);
    vm.runInContext(part(html, 'function setFiled(sh) {', '/** RELEASEMARK: what the cloud kept'), c);
    c.__t = (o) => vm.runInContext(`setFiled(${JSON.stringify(o)})`, c);
    assert.equal(c.__t({ runId: RUN, sheetId: S15, setId: SET5, draft: true }), false);
    assert.equal(c.__t({ runId: RUN, sheetId: S15, setId: SET5, draft: false }), false);
    assert.equal(c.__t({ runId: RUN, sheetId: 'gold-s8', setId: SET5, draft: false }), true);
    vm.runInContext(`B.run = { runId: 'run-old', releasePolicy: 1 };`, c);
    assert.equal(c.__t({ runId: 'run-old', sheetId: 'x', setId: 'set-old', draft: false }), true);
  });
  await check('D2 keptAnswer: the cloud\'s keptFull puts the release mark back on the page; membershipKept makes the page\'s copy the draft of no set the record is', async () => {
    const c = { console, logs: [] }; vm.createContext(c);
    vm.runInContext(`const log = (sh, m) => logs.push(m);`, c);
    vm.runInContext(part(html, 'function keptAnswer(sh, a) {', 'async function persistSheet('), c);
    c.__sh = { releaseFull: false, topup: { at: 1, tried: [] }, draft: false, setId: SET5, seq: 5, sheetIndex: 4, label: { files: [] } };
    vm.runInContext(`keptAnswer(__sh, { keptFull: true, membershipKept: { asked: '${SET5}', kept: null } })`, c);
    assert.equal(c.__sh.releaseFull, true); assert.equal(c.__sh.topup, null); assert.equal(c.__sh.draft, true); assert.equal(c.__sh.setId, null); assert.equal(c.__sh.sheetIndex, null);
  });

  /* ═══ E · NOSPREAD's turn-away read from the sheets ═══ */
  const PL = require(path.join(ROOT, 'charm-nest-place.js'));
  const gf = (id, page, n, born, o) => Object.assign({ sheetId: id, id, metal: 'gold', page, runId: RUN, cardStartedAt: born, charms: charms(n), placements: placed(n) }, o || {});
  await check('E1 passedOver: GF Draft 1 is passed over once a Gold Filled sheet started after it holds pieces; a later empty sheet, another metal or another run does not count', async () => {
    const s15 = gf(S15, 15, 3, 100), s16 = gf('gold-s16', 16, 2, 200);
    assert.equal(PL.passedOver(s15, [s15, s16]), true);
    assert.equal(PL.passedOver(s16, [s15, s16]), false);
    assert.equal(PL.passedOver(s15, [s15, gf('gold-s16', 16, 0, 200)]), false);
    assert.equal(PL.passedOver(s15, [s15, gf('silver-s9', 9, 3, 300, { metal: 'silver' })]), false);
    assert.equal(PL.passedOver(s15, [s15, gf('gold-x', 2, 3, 300, { runId: 'run-other' })]), false);
    assert.equal(PL.passedOver(gf('a', 1, 2, 0), [gf('a', 1, 2, 0), gf('b', 2, 1, 0)]), true, 'no start times: the page number');
  });
  await check('E2 nospreadSettle on a page that never saw a refusal (a reload, a new carrier): GF Draft 1 is brought to its one nest after 30 quiet minutes, and only once for that piece count', async () => {
    const c = { console, nests: [], logs: [] }; vm.createContext(c);
    c.__pages = [gf(S15, 15, 3, 100, { status: 'complete', persistedDone: true, verification: { ok: true }, fileBase: 'GF_working_' + S15, density: 0.7768, rejects: [] }), gf('gold-s16', 16, 2, 200, { status: 'complete' })];
    vm.runInContext(`
      let __now = 1000000; const topupNow = () => __now;
      const window = { B: { run: { runId: '${RUN}', status: 'processed' } }, CharmNestPlace: __PL, LiveNest: { closed: () => false } }, LiveNest = window.LiveNest;
      const S = { settings: {} }, fmt = { pct: v => Math.round(v * 100) + '%' };
      const nospread = () => ({}), allSheets = () => __pages, activeCharms = sh => sh.charms, heldForResume = () => false, NS_QUIET_MS = 30 * 60 * 1000;
      const log = (sh, m) => logs.push(m), startNest = sh => nests.push(sh.sheetId);
    `, Object.assign(c, { __PL: PL }));
    vm.runInContext(part(html, 'function nospreadSettle() {', '/** The order as the policy reads it.'), c);
    vm.runInContext('nospreadSettle()', c);
    assert.deepEqual(c.nests, [], 'the quiet starts');
    vm.runInContext('__now += 31 * 60 * 1000; nospreadSettle()', c);
    assert.deepEqual(c.nests, [S15]); const s = c.__pages[0]; assert.equal(s._nsCheck, true); assert.equal(s._nsPassed, 1);
    // its nest ends (as finishNest leaves it) and the sheet is still not released: no second nest for the same pieces
    Object.assign(s, { status: 'complete', dirty: false, appendOnly: false, intakeAppend: false });
    vm.runInContext('__now += 31 * 60 * 1000; nospreadSettle(); nospreadSettle()', c);
    assert.deepEqual(c.nests, [S15]);
  });
  await check('E3 sheetFull counts that turn-away as the miss at the release fill: GF Draft 1 (78%) is released, GF Draft 2 (73%) is not', async () => {
    const c = { console }; vm.createContext(c);
    vm.runInContext(`const S = { settings: { maxFill: .8 } }; const untriedIds = () => new Set(); const inflatedArea = c => c.areaPt2 || 0;`, c);
    vm.runInContext(part(html, 'function sheetFull(sh, result, items) {', '/* Gap fill (Paul, 24 Sep)'), c);
    const sheet = d => ({ rejects: [], _nsCheck: true, _nsPassed: 1, verification: { ok: true }, placements: [{ id: 'a' }] }), res = d => ({ endedBy: 'kept', density: d, placedPt2: d * 38841, usablePt2: 38841, smallRoom: true });
    c.__a = sheet(); c.__b = sheet(); c.__ra = res(0.7768); c.__rb = res(0.7317);
    assert.equal(vm.runInContext('sheetFull(__a, __ra, [])', c), true);
    assert.equal(vm.runInContext('sheetFull(__b, __rb, [])', c), false);
  });

  /* ═══ F · the set assembly on the records ═══ */
  const Core = require(path.join(ROOT, 'charm-nest-assembly-core.js'));
  const look = async recs => {
    const run = { runId: RUN, releasePolicy: 2, solidIncluded: {}, status: 'processed', lines: {} };
    const sets = [Core.asSet({ setId: SET5, runId: RUN, day: '2026-11-12', seq: 5, group: 'dispatch', status: 'awaiting review', formedAt: 1, sheetIds: ['gold-s8', 'gold-s9'] })], setsById = new Map(sets.map(s => [s.setId, s]));
    const pages = recs.map(r => Core.asPage(r, { run, setsById, now: T0 })), V = { run, pages, sets, now: T0, today: '2026-11-20', journal: {}, joinRefused: new Map(), clock: { completion: 0, join: 0 }, fileBase: p => Core.sheetFileBase(p, '2026-11-20') };
    const joins = [], io = { say() {}, pace: async () => {}, async ensureSet() { const set = Core.asSet({ setId: 'set-next', seq: 1, day: '2026-11-20', runId: RUN, group: 'dispatch', sheetIds: [], status: 'open' }); V.sets.push(set); return set; },
      async stampReleased() {}, async join(p, set) { joins.push([p.sheetId, set.setId]); return { ok: true }; }, async joined() { return false; }, async leave() {}, async saveSet() {} };
    const out = await Core.gate(V).assemble(io); return { out, joins };
  };
  const recOf = (id, page, suf, o) => Object.assign({ id, runId: RUN, metal: 'gold', page, day: '2026-11-20', status: 'complete', verification: { ok: true }, outputs: { ai: { path: 'x/' + id + '.ai' } }, density: 0.75 }, layout(3, suf), o || {});   // (suf: each sheet its own orders)
  const members = [recOf('gold-s8', 7, '8', { draft: false, setId: SET5, setSeq: 5, sheetIndex: 1, releaseFull: true }), recOf('gold-s9', 9, '9', { draft: false, setId: SET5, setSeq: 5, sheetIndex: 2, releaseFull: true })];
  const s17 = recOf('gold-s17-mwoedhs2', 17, '17', { draft: true, setId: null, releaseFull: true });
  const s15 = mark => recOf(S15, 15, '15', { draft: true, setId: SET5, setSeq: 5, sheetIndex: 6, releaseFull: mark });
  await check('F1 as the live records were (GF Draft 1 a draft naming Set 5, no release mark): no set forms; GF Draft 3 waits for a completed SS sheet or a second completed GF sheet', async () => {
    const r = await look(members.concat([s15(false), s17])); assert.equal(r.out.formed, 0); assert.deepEqual(r.joins, []);
  });
  await check('F2 with the release mark kept (the fix), GF Draft 1 and GF Draft 3 form the next set (2 completed GF sheets), its old Set 5 stamp notwithstanding; Set 5 is not touched', async () => {
    const r = await look(members.concat([s15(true), s17]));
    assert.equal(r.out.formed, 2); assert.deepEqual(r.joins.map(j => j[0]).sort(), ['gold-s17-mwoedhs2', S15].sort()); assert(r.joins.every(j => j[1] === 'set-next'), JSON.stringify(r.joins));
  });

  assert.deepEqual(F.net.tripwire || [], [], 'no Etsy address was asked');
  console.log(fails ? `FAIL release-mark-exit: ${fails}` : 'PASS release-mark-exit');
  process.exit(fails ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
