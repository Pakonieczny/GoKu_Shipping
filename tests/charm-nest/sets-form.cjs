// Set formation (Paul, 10 Oct 2026): "In order for a Set of Sheets to be allowed to exist there must be at minimum 1 Completed GF Sheet and 1 Completed SS Sheet."
// One offline suite: the shared definition (charm-nest-set-rules.js) read by the page's assembly (settle), the readiness gate (the Approve button, the card line,
// the set's place), the Library plan (a New set drop, taking a sheet out), SetEdit.verifyMoves, the server's completion of a set (op setUpdate, fake Firestore that
// refuses nested arrays), and the staged repair of the sandbox state of image 1 (scripts/set-repair-1010.cjs). Nothing here touches the network or a real record.
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const Rules = require('../../charm-nest-set-rules.js'), R = require('../../charm-nest-readiness.js'), SE = require('../../charm-nest-set-edit.js');
const SO = require('../../charm-nest-shared-orders.js').core, LF = require('../../charm-nest-flow.js'), Repair = require('../../scripts/set-repair-1010.cjs');
const clone = x => JSON.parse(JSON.stringify(x));

/* ── the sandbox Library of image 1, as the live read showed it (ids, flags, the three shared orders; everything else trimmed) ───────────────── */
const back = (p, sid) => ({ poolId: p, sheetId: sid, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: p + '.ai', url: 'https://example.com/' + p + '.ai' } } });
const mk = (id, metal, o = {}) => {
  const pool = o.pool || [`${4170000000 + Math.floor(Math.random() * 1e6)}_${id.length}_1`], orders = [...new Set(pool.map(p => p.split('_')[0]))];
  return { id, runId: 'run1', metal, metalLabel: metal === 'gold' ? 'GF 14/20' : metal === 'silver' ? 'SS' : 'RG 14/20', day: '2026-10-03', status: 'complete', draft: true, releaseFull: false, setId: null, sheetIndex: null,
    poolIds: pool, placedCount: pool.length, charmCount: pool.length, density: .7, verification: { ok: true }, outputs: { ai: { url: 'https://example.com/f.ai' }, preview: { url: 'https://example.com/f.png' } },
    orders, orderReadiness: Object.fromEntries(orders.map(x => [x, { ready: true }])), label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: orders[0], orders }] }, backPool: pool.map(p => back(p, id)), ...o, pool: undefined };
};
const inSet1 = { draft: false, setId: 'set-1', setSeq: 1 };
const image1 = () => ({
  gf1: mk('gold-mv1n4r6z', 'gold', { page: 1, releaseFull: true, status: 'partial', density: .754, pool: ['4170252963_5213392567_1', '4170837249_5213435316_1', '4170000011_1_1'] }),          // GF Sheet 1, 75%, 71/308: full, outside every set
  ss1: mk('silver-mv1n51oa', 'silver', { page: 1, releaseFull: true, status: 'partial', density: .76, pool: ['4170252963_5212609072_1', '4170000021_1_1'] }),                              // SS Sheet 1, 76%, 71/122: full, outside every set
  rg1: mk('rose-mv1n4r78', 'rose', { page: 1, density: .153, pool: ['4170837249_5213435318_1', '4170000031_1_1'] }),                                                                          // RG Sheet 1, 15%
  gf5: mk('gold-s5-mv1pbxaq', 'gold', { page: 5, density: .268, pool: ['4170000041_1_1'] }),                                                                                                   // GF Sheet 5, 27%: still filling
  g2: mk('gold-s2-mv1nyiv7', 'gold', { ...inSet1, page: 2, sheetIndex: 1, releaseFull: true, density: .77, pool: ['4170000051_1_1'] }),                                                       // Set-1: GF Sheet 1
  g3: mk('gold-s3-mv1oh2q9', 'gold', { ...inSet1, page: 3, sheetIndex: 2, releaseFull: true, status: 'partial', density: .707, pool: ['4170000061_1_1'] }),                                    // Set-1: GF Sheet 2
  g4: mk('gold-s4-mv1oyjd9', 'gold', { ...inSet1, page: 4, sheetIndex: 3, releaseFull: true, density: .746, pool: ['4174601819_5218691212_1', '4174601819_5218691212_2'] }),                  // Set-1: GF Sheet 3
  sp: mk('silver-s2-mv1o02uo', 'silver', { ...inSet1, page: 2, sheetIndex: 1, density: .573, pool: ['4174601819_5218850093_1', '4174601819_5218850093_2'] })                                 // Set-1: SS Sheet 1, 57%: partial, pulled in by the cardinal rule
});
const set1 = () => ({ setId: 'set-1', seq: 1, name: 'Set-1', group: 'dispatch', status: 'awaiting review', committedAt: null, sheetIds: ['gold-s2-mv1nyiv7', 'gold-s3-mv1oh2q9', 'gold-s4-mv1oyjd9', 'silver-s2-mv1o02uo'] });

(async () => {
  const S = image1(), members = [S.g2, S.g3, S.g4, S.sp], all = Object.values(S);

  /* 1 · why each sheet of image 1 is where it is: the cardinal rule over the stored pieces, and the definition of Completed */
  const core = all.map(s => SO.sheetOf(s, { label: `${Rules.metalClass(s)} ${s.id}` }));
  const groups = SO.groups(core).map(g => g.ids.slice().sort());
  assert.deepEqual(groups.sort(), [[S.g4.id, S.sp.id].sort(), [S.gf1.id, S.rg1.id, S.ss1.id].sort()].sort(), 'two groups: GF3+SS (Set-1), and GF1+SS1+RG1 (outside)');
  for (const k of ['gf1', 'ss1', 'g2', 'g3', 'g4']) assert.equal(Rules.isCompleted(S[k]), true, k + ' is full');
  for (const k of ['sp', 'gf5', 'rg1']) assert.equal(Rules.isCompleted(S[k]), false, k + ' is still filling');
  assert.equal(Rules.isCompleted(S.sp), false, 'status complete + in a set is not completed: the 57% SS of Set-1');
  assert.equal(Rules.isCompleted(S.gf1), true, 'status partial + draft is a full sheet: the 75% GF');

  /* 2 · the page's assembly (Gate.assembleNow): SetRules.settle decides who joins now */
  let st = Rules.settle([], [S.g2, S.sp]);                                                   // no set yet, a completed GF and a partial SS: no set is made
  assert.deepEqual([st.formed, st.join.length, st.wait.length], [false, 0, 2]); assert.match(st.wait[0].why, /^Waits for a completed SS sheet: a set needs at least 1 completed GF sheet and 1 completed SS sheet/);
  st = Rules.settle([], [S.gf1, S.ss1]); assert.deepEqual([st.formed, st.join.length, st.wait.length], [true, 2, 0]);   // a completed GF and SS: the set is made, with both
  st = Rules.settle([], [S.gf1, S.ss1, S.sp]); assert.equal(st.join.length, 3, 'a partial partner comes with a valid batch (the cardinal rule)');
  st = Rules.settle([], [S.ss1]); assert.equal(st.join.length, 0, 'a lone completed SS makes no set');
  st = Rules.settle(members, [S.gf1]); assert.deepEqual([st.join.length, st.wait.length], [0, 1], 'Set-1 exists but lacks a completed SS: another GF does not help, it waits');
  st = Rules.settle(members, [S.gf1, S.ss1]); assert.equal(st.join.length, 2, 'the batch that completes Set-1 joins');
  st = Rules.settle([S.g2, S.ss1], [S.gf1, S.gf5]); assert.equal(st.join.length, 2, 'a valid set takes more, partial sheets too');
  assert.deepEqual(Rules.settle([], []).join, []);

  /* 3 · the readiness gate: Set-1 is flagged, not dissolved; it is not ready, says so in one line, and a completed SS makes it right */
  const gate = Rules.gate(set1(), members);
  assert.deepEqual([gate.applies, gate.ok, gate.missing], [true, false, ['SS']]); assert.match(gate.line, /^Waiting for a completed SS sheet: a set needs at least 1 completed GF sheet and 1 completed SS sheet before it can go to laser cutting\.$/);
  const flagged = R.laserGroup(set1(), clone(members));
  assert.equal(flagged.ready, false, 'every sheet is ready, the set is not: it has no completed SS');
  assert.equal(R.laserSheet(clone(S.g2)).ready, true, 'a sheet of it is ready on its own');
  const g = R.setGate(set1(), clone(members));
  assert.equal(g.ready, false); assert.equal(g.reason, 'Set-1 · needs a completed SS sheet'); assert.equal(g.blockers[0].rule, 'setPrinciple');
  assert.equal(R.explain(set1(), { kind: 'set', sheets: clone(members) }).nextText, gate.line, 'the card says it in the one plain line');
  const right = members.concat([{ ...S.ss1, ...inSet1, sheetIndex: 2 }]), setRight = { ...set1(), sheetIds: set1().sheetIds.concat(S.ss1.id) };
  assert.equal(R.laserGroup(setRight, clone(right)).ready, true, 'with a completed SS sheet the same set is ready'); assert.equal(R.setGate(setRight, clone(right)).ready, true);
  assert.equal(R.laserGroup(set1(), clone(members.map(m => m === S.sp ? { ...m, releaseFull: true } : m))).ready, true, 'the partial SS sheet reaching its fill (released) is enough as well');
  const committed = { ...set1(), status: 'complete', committedAt: 5 };
  assert.deepEqual([Rules.gate(committed, members).applies, R.laserGroup(committed, clone(members)).ready], [false, true], 'a set committed before is never blocked afterwards');
  assert.equal(Rules.gate(set1(), members.map(({ releaseFull, ...m }) => m)).applies, false, 'records without a completion mark are not judged');

  /* 4 · the Library plan: a New set drop is refused unless the sheets in it make a valid set; the last completed sheet of a set cannot be taken out */
  const state = { sheets: Object.fromEntries(all.map(s => [s.id, clone(s)])), sets: { 'set-1': { ...set1(), status: 'complete', committedAt: 5 } }, runs: { run1: { open: false } }, live: {} };
  const live = (id, o = {}) => ({ runHere: true, draft: true, dispatchSetId: null, can: { ok: true, byHand: true }, split: [], ...o });
  for (const k of ['gf1', 'ss1', 'gf5']) state.live[S[k].id] = live();
  const env = { by: 'Paul', canJoin: true, canRelabel: true };
  const keys = p => p.needs.map(n => n.key);
  let p = LF.core.planMove(state, { kind: 'sheet', id: S.gf1.id, to: { newSet: true } }, env);
  assert(keys(p).includes('setPrinciple'), 'a completed GF alone cannot start a set: ' + JSON.stringify(keys(p))); assert.match(p.needs.find(n => n.key === 'setPrinciple').detail, /no completed SS sheet/);
  p = LF.core.planMove(state, { kind: 'sheet', id: S.gf1.id, to: { sheet: S.ss1.id } }, { ...env, dest: { newSet: true } });
  assert(!keys(p).includes('setPrinciple'), 'a completed GF dropped on a completed SS starts a valid set: ' + JSON.stringify(p.needs));
  // taking sheets out of a committed set (rule A and B are SetEdit's; the principle is one more)
  const cs = { ...set1(), status: 'complete', committedAt: 5 }, cstate = { ...state, sets: { 'set-1': cs } };
  for (const k of ['g2', 'g3', 'g4', 'sp']) cstate.sheets[S[k].id] = clone({ ...S[k], poolIds: [S[k].id + '_x_1'], orders: [] });   // (no shared order: only the principle is looked at here)
  cstate.sheets[S.sp.id].releaseFull = true; cstate.sheets[S.sp.id].metal = 'silver';                                              // a committed set that holds one completed SS sheet
  p = LF.core.planMove(cstate, { kind: 'sheet', id: S.sp.id, to: { area: 'progress' } }, { ...env, sharedItems: [] });
  assert(keys(p).includes('setPrinciple'), 'the only completed SS of a valid set stays: ' + JSON.stringify(keys(p))); assert.match(p.needs.find(n => n.key === 'setPrinciple').label, /would have no completed SS sheet/);
  p = LF.core.planMove(cstate, { kind: 'sheet', id: S.g3.id, to: { area: 'progress' } }, { ...env, sharedItems: [] });
  assert(!keys(p).includes('setPrinciple'), 'one of three completed GF sheets may leave: ' + JSON.stringify(keys(p)));

  /* 5 · SetEdit.verifyMoves (the server's setMember step reads it inside its transaction) */
  const mem = (o = {}) => ({ ...clone(S.sp), ...o });
  const recs = { a: mem({ id: 'a', metal: 'gold', releaseFull: true, setId: 'sx', draft: false, poolIds: ['9000000001_1_1'], orders: [] }), b: mem({ id: 'b', metal: 'silver', releaseFull: true, setId: 'sx', draft: false, poolIds: ['9000000002_1_1'], orders: [] }), c: mem({ id: 'c', metal: 'gold', releaseFull: true, setId: 'sx', draft: false, poolIds: ['9000000003_1_1'], orders: [] }) };
  const sx = { doc: { setId: 'sx', seq: 5, committedAt: 5, status: 'complete', sheetIds: ['a', 'b', 'c'] }, members: [recs.a, recs.b, recs.c] };
  let v = SE.verifyMoves({ moves: [{ id: 'b', to: null }], recs, sets: { sx }, others: [] });
  assert.equal(v.ok, false); assert.equal(v.reasons[0].key, 'setPrinciple'); assert.match(SE.sayWhy(v), /Set 5 would have no completed SS sheet/);
  v = SE.verifyMoves({ moves: [{ id: 'a', to: null }], recs, sets: { sx }, others: [] }); assert.equal(v.ok, true, JSON.stringify(v.reasons));
  const legacy = { doc: sx.doc, members: [recs.a, { ...recs.b, releaseFull: false }, recs.c] };
  v = SE.verifyMoves({ moves: [{ id: 'a', to: null }], recs, sets: { sx: legacy }, others: [] }); assert.equal(v.ok, true, 'a set that was already short is flagged elsewhere, a move out of it is not blocked here');

  /* 6 · the server: a set is completed (committed) only with a completed GF and a completed SS sheet; fake Firestore, no nested arrays */
  const source = fs.readFileSync(require('node:path').join(__dirname, '../../netlify/functions/charmNestLibrary.js'), 'utf8');
  const data = new Map(); let writes = 0;
  const put = (path, doc) => data.set(path, refuseNestedArrays(clone(doc), path));
  const snap = ref => ({ id: ref.path.split('/')[1], exists: data.has(ref.path), data: () => clone(data.get(ref.path)) });
  const noRuns = { isQuery: true, select: () => noRuns, get: async () => ({ docs: [] }) };
  const ctxOf = () => vm.createContext({ splitOfSet: async () => [], Readiness: R, process, console, str: String, num: Number, isId: () => true, PREFIX: '', SETS: 'sets', SHEETS: 'sheets', RUNS: 'runs', RUN_LINES: 'run_lines', col: n => ({ where: () => noRuns, doc: id => ({ path: n + '/' + id, get: async () => snap({ path: n + '/' + id }) }) }), FV: { serverTimestamp: () => 100 },
    db: { runTransaction: async fn => fn({ get: async ref => ref.isQuery ? ref.get() : snap(ref), set: (ref, patch) => { writes++; put(ref.path, { ...data.get(ref.path), ...patch }); }, update: (ref, patch) => { writes++; put(ref.path, { ...data.get(ref.path), ...patch }); } }), getAll: async (...refs) => refs.filter(x => x.path).map(snap) } });
  const ctx = ctxOf();
  vm.runInContext(source.slice(source.indexOf('async function op_setUpdate'), source.indexOf('async function op_setGet')), ctx);
  vm.runInContext(source.slice(source.indexOf('async function filingRecords'), source.indexOf('async function op_laserStatus')), ctx);
  const line = (sheet, i) => [`${sheet.id}-l${i}`, { orderId: sheet.poolIds[i].split('_')[0], state: 'written', poolIds: [sheet.poolIds[i]] }];
  const setup = (a, b, setDoc) => {
    data.clear(); writes = 0;
    const list = [a, b].map(s => ({ ...clone(s), setId: 'set1', draft: false }));
    list.forEach(s => put('sheets/' + s.id, s));
    put('runs/run1', { lines: Object.fromEntries(list.flatMap(s => s.poolIds.map((_, i) => line(s, i)))) });
    put('sets/set1', { setId: 'set1', sheetIds: list.map(s => s.id), status: 'open', ...setDoc });
  };
  setup(S.g2, S.sp);                                                                    // a completed GF and the partial SS: refused, nothing written
  await assert.rejects(ctx.op_setUpdate({ setId: 'set1', patch: { status: 'complete' } }), /Set cannot be completed: .* needs a completed SS sheet\. A set needs at least 1 completed GF sheet and 1 completed SS sheet\./);
  assert.equal(writes, 0, 'a refused completion writes nothing');
  setup(S.g2, { ...S.ss1, releaseFull: true });                                         // a completed GF and a completed SS: recorded
  await ctx.op_setUpdate({ setId: 'set1', patch: { status: 'complete', sheetIds: ['gold-s2-mv1nyiv7', 'silver-mv1n51oa'] } }); assert.equal(writes, 1); assert.equal(data.get('sets/set1').status, 'complete');
  setup(S.g2, S.sp, { status: 'complete', committedAt: 5 });                            // a set committed before the principle is saved again, never refused for it
  await ctx.op_setUpdate({ setId: 'set1', patch: { status: 'complete' } }); assert.equal(writes, 1);

  /* 7 · the staged repair of image 1: a dry run that writes nothing, from the same rules */
  const plan = Repair.plan(all.map(clone), [set1()]);
  assert.deepEqual(plan.writes, [], 'the script has no write path');
  assert.equal(plan.before.ok, false); assert.deepEqual(plan.before.missing, ['SS']);
  assert.deepEqual(plan.moves.map(m => [m.label.replace(' (not in a set)', ''), m.asLabel]).sort(), [['GF Sheet 1', 'GF Sheet 4'], ['RG Sheet 1', 'RG Sheet 1'], ['SS Sheet 1', 'SS Sheet 2']], 'the completed GF is GF Sheet 4 of Set-1 (Paul\'s option A), the completed SS joins, the Rose Gold sheet comes with them');
  assert(plan.moves.every(m => m.needs.length), 'all three wait for the Rose Gold sheet\'s own Cut Sheet press (Paul\'s explicit yes), which no script gives');
  assert.deepEqual(plan.stays.map(s => s.label.replace(' (not in a set)', '')), ['GF Sheet 5'], 'the 27% GF sheet is not touched: it starts the next set');
  assert.equal(plan.afterIfAllMove.ok, true); assert.deepEqual(plan.afterIfAllMove.have, { GF: 4, SS: 1 });
  assert.equal(plan.afterNow.ok, false, 'without that yes nothing can move and Set-1 stays flagged, never dissolved');
  const text = Repair.say(plan).join('\n'); assert.match(text, /GF Sheet 1 \(not in a set\) moves from no set to Set-1 as GF Sheet 4/); assert.match(text, /never dissolved/);
  // the same rule when nothing ties the completed sheets to a Rose Gold sheet: they join by themselves
  const free = all.filter(s => s.metal !== 'rose').map(clone); free.forEach(s => { s.poolIds = s.poolIds.filter(x => !x.startsWith('4170837249_')); });
  const plan2 = Repair.plan(free, [set1()]); assert.deepEqual(plan2.moves.map(m => m.needs.length), [0, 0]); assert.equal(plan2.afterNow.ok, true);

  console.log('PASS: sets-form: a set exists only with a completed GF and a completed SS sheet (page assembly, readiness, Library plan, SetEdit, server completion), Set-1 is flagged and never dissolved, and the staged repair of image 1 writes nothing');
})().catch(e => { console.error(e); process.exit(1); });
