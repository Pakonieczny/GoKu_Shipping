// Combining two sheets that are not in Laser cutting by drag and drop (Paul, 7 Oct 2026: "I can not drag/drop these to combine them.
// Neither of them are in Laser cutting so there should be nothing preventing them from being combined").
// LibraryFlow over a FAKE backend only (bridge-server.cjs: the real charmNestLibrary handler over an in-memory Firestore); the sheet
// window's own Include path is a fake with its exact shape. Nothing here touches the live site, Etsy or a paid service.
//
// The scene is the screenshot: a draft 14K sheet and a draft GF sheet, no open set, one committed set. It walks every place the
// move bar offers for such a sheet and says what each answers, then the new place: a draft sheet dropped on another draft sheet.
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const LF = require('../../charm-nest-flow.js');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  // a draft sheet of the open run holding `pieces` (pool ids "order_line_copy")
  const mk = (id, metal, pieces, extra = {}) => st.put(S, id, { id, runId: 'run-live', metal, day, status: 'complete', draft: true, placedCount: pieces.length, charmCount: pieces.length, density: .36, stock: { wIn: 6, hIn: 4.5 },
    poolIds: pieces, orders: [...new Set(pieces.map(p => p.split('_')[0]))], verification: { ok: true }, outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
    page: +id.split('-').pop() || 1, sheetIndex: null, updatedAt: ts, createdAt: ts, ...extra });
  const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, '') || 1, day, runId: 'run-live', sheetIds, materials: ['gold'], orders: {}, status: 'open', updatedAt: ts, createdAt: ts, ...extra });

  // ── the scene ──
  mkSet('set-1', ['gf-c1'], { status: 'complete', committedAt: now - 5000 });                      // Set 1 · Oct 3: committed
  mk('gf-c1', 'gold', ['3800000001_1_1'], { draft: false, setId: 'set-1', setSeq: 1, sheetIndex: 1 });
  mk('k14-1', 'gold14k', ['3800000011_1_1']);                                                         // 14K Sheet 1: a draft
  mk('gf-2', 'gold', ['3800000021_1_1', '3800000022_1_1']);                                           // GF Sheet 2: a draft
  mk('ss-3', 'silver', ['3800000031_1_1']);                                                           // SS Sheet 3: a draft, shares an order with nobody (yet)
  mk('gf-4', 'gold', ['3800000041_1_1']);
  mk('ss-5', 'silver', ['3800000051_1_1', '3800000041_2_1']);                                         // shares order 3800000041 with gf-4
  mk('gf-6', 'gold', ['3800000061_1_1']);
  mk('rg-7', 'rose', ['3800000071_1_1'], { roseStockId: 'stock-1' });
  mk('gf-8', 'gold', ['3800000081_1_1'], { laserDoneAt: now - 1000 });                                // completed
  mk('gf-9', 'gold', ['3800000091_1_1'], { roseCutAt: now - 1000 });                                  // cut (permanent)
  mk('gf-10', 'gold', ['3800000101_1_1']);
  mk('in-set', 'gold', ['3800000111_1_1'], { draft: false, setId: 'set-9', setSeq: 9, sheetIndex: 1 });
  mkSet('set-9', ['in-set'], { status: 'open' });                                                     // the run's open set, once it exists
  st.put(RUN, 'run-live', { runId: 'run-live', status: 'review', lines: {} });

  const L = {};                          // what the sheet window knows of each sheet (SheetWin.joinInfo), changed by the fake Include below
  const draft = (id, extra = {}) => { L[id] = { runHere: true, draft: true, dispatchSetId: null, can: { ok: true, byHand: !/^k14/.test(id) }, split: [], ...extra }; };
  for (const id of ['k14-1', 'gf-2', 'ss-3', 'gf-4', 'ss-5', 'gf-6', 'gf-10']) draft(id);
  draft('rg-7', { rose: { full: false }, can: { ok: true, byHand: false } });
  draft('gf-8', { can: { ok: false, reason: 'It is already cut.' } });
  draft('gf-9', { can: { ok: false, reason: 'It is already cut.' } });
  L['in-set'] = { runHere: true, draft: false, dispatchSetId: 'set-9', can: { ok: false, reason: 'It is already in a set.' }, split: [] };
  const joins = [], pulls = {}, down = new Set();     // pulls: { sheet: [sheets the cardinal rule brings in with it] } as Gate.assemble would
  const opened = id => { st.put(S, id, { draft: false, setId: 'set-8', setSeq: 8 }); L[id] = { ...L[id], draft: false }; const d8 = st.doc(SET, 'set-8'); if (d8) st.put(SET, 'set-8', { sheetIds: [...new Set([...(d8.sheetIds || []), id])] }); else mkSet('set-8', [id]); for (const k of Object.keys(L)) if (k !== 'in-set') L[k].dispatchSetId = 'set-8'; };
  LF.configure({
    api: async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; },
    employee: () => 'Paul', rows: () => [], remakeLabel: async () => {}, rose: () => null, roseJoin: async () => ({ ok: true, sheets: [], warnings: [] }),
    live: id => L[id] || null,
    areaOf: () => 'progress', setOf: () => null,
    sets: () => [{ setId: 'set-1', seq: 1, committedAt: now - 5000, status: 'complete' }].concat(L['k14-1'].dispatchSetId === 'set-8' ? [{ setId: 'set-8', seq: 8 }] : []),
    include: async (id, o) => {
      if (down.has(id)) throw new Error('Waits for GF Sheet 2 (it is still being nested or saved)');
      joins.push({ id, ...o }); opened(id); for (const x of pulls[id] || []) { opened(x); }
    }
  });
  const plan = (id, to) => LF.plan({ kind: 'sheet', id, to });
  const keys = p => p.needs.map(n => n.key);
  const zs = id => LF.explainTargets({ kind: 'sheet', id });
  const z = (list, f) => list.find(f);
  try {
    // ═══ A. what the move bar offers a draft 14K sheet (the screenshot), and what each place answers ═══
    let list = zs('k14-1');
    assert.equal(z(list, x => x.area === 'progress').ok, false, 'In progress: it is already there'); assert.match(z(list, x => x.area === 'progress').reason, /already in In progress/);
    assert.equal(z(list, x => x.area === 'laser').ok, true); assert.equal(z(list, x => x.area === 'completed').ok, true);
    assert.equal(z(list, x => x.set === 'set-1').ok, false); assert.match(z(list, x => x.set === 'set-1').reason, /already committed/, 'the greyed Set 1 chip');
    assert.equal(z(list, x => x.newSet).ok, true, 'New set is open to a draft when no set is open');
    // Laser cutting and Completed are armed but refused with the reason that it is not in a set yet (it is a draft)
    for (const area of ['laser', 'completed']) { const p = await plan('k14-1', { area }); assert.equal(p.ok, false); assert(keys(p).includes('membership'), JSON.stringify(keys(p))); assert.match(p.needs.find(n => n.key === 'membership').label, /14K Sheet 1 is not in a set yet/); }
    // the committed set: refused by name
    let p = await plan('k14-1', { set: 'set-1' }); assert.equal(p.ok, false); assert(keys(p).includes('setCommitted'), JSON.stringify(keys(p)));
    // New set: a plan that can be committed, nothing written by planning
    p = await plan('k14-1', { newSet: true }); assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.steps.map(x => x.type), ['include']); assert.equal(joins.length, 0);

    // ═══ B. a draft sheet dropped on another draft sheet: the place, in plain words ═══
    let sz = LF.sheetZone({ kind: 'sheet', id: 'k14-1' }, 'gf-2', { self: '14K Sheet 1', name: 'GF Sheet 2' });
    assert.deepEqual({ ok: sz.ok, name: sz.name, sub: sz.sub, sheet: sz.sheet }, { ok: true, name: 'Combine with GF Sheet 2', sub: 'both start a new set', sheet: 'gf-2' });
    assert.equal(LF.sheetZone({ kind: 'sheet', id: 'k14-1' }, 'k14-1'), null, 'a sheet is not a place for itself'); assert.equal(LF.sheetZone({ kind: 'set', id: 'set-1' }, 'gf-2'), null, 'a set takes no sheet this way');
    const refused = (a, b, re) => { const r = LF.sheetZone({ kind: 'sheet', id: a }, b, { self: a, name: b }); assert.equal(r.ok, false, a + ' on ' + b); assert.match(r.reason, re, r.reason); return r; };
    refused('k14-1', 'rg-7', /Rose Gold.*Cut Sheet press/); refused('rg-7', 'k14-1', /Rose Gold.*Cut Sheet press/);
    refused('k14-1', 'in-set', /already in a set: drop k14-1 on that set/); refused('in-set', 'k14-1', /already in a set/);
    refused('k14-1', 'gf-9', /not ready to join a set\. It is already cut/); refused('k14-1', 'nope', /not on this page/);
    L['gf-6'].can = { ok: false, reason: 'Its layout has not been verified yet.' }; refused('k14-1', 'gf-6', /not ready to join a set\. Its layout has not been verified yet/); L['gf-6'].can = { ok: true, byHand: true };
    L['gf-6'].runHere = false; refused('k14-1', 'gf-6', /not on a page of the open run/); L['gf-6'].runHere = true;

    // ═══ C. the drop: both join one new set, in one plan, with no yes (nothing is shared) ═══
    p = await plan('k14-1', { sheet: 'gf-2' });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.confirm, [], 'two sheets that share nothing: the drop is the yes');
    assert.deepEqual(p.steps.map(x => [x.type, x.sheetId, !!x.also, x.newSet]), [['include', 'k14-1', false, true], ['include', 'gf-2', true, false]]);
    assert(p.auto.some(a => a.key === 'membership' && /14K Sheet 1 added to a new set/.test(a.label)) && p.auto.some(a => a.key === 'membership:gf-2' && /GF Sheet 2 added to a new set/.test(a.label)), JSON.stringify(p.auto.map(a => a.label)));
    assert.deepEqual(p.move.to, { sheet: 'gf-2' }, 'commit plans again from the drop'); assert.equal(joins.length, 0, 'planning writes nothing');
    let r = await LF.commit(p, { by: 'Paul' });
    assert.equal(r.ok, true, JSON.stringify(r)); assert.deepEqual(joins.map(j => [j.id, j.newSet, j.setId]), [['k14-1', true, null], ['gf-2', false, null]], 'the first starts the set, the second joins it');
    assert(r.applied.some(a => /14K Sheet 1 added/.test(a.label)) && r.applied.some(a => /GF Sheet 2 added/.test(a.label)), JSON.stringify(r.applied));
    assert.equal(st.doc(S, 'k14-1').draft, false); assert.equal(st.doc(S, 'gf-2').draft, false);
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true, JSON.stringify(r)); assert.equal(joins.length, 2, 'a repeat finds both in the set and writes nothing more');

    // ═══ D. a set is open now: the drop puts both into it (never a second open set) ═══
    st.put(SET, 'set-8', { setId: 'set-8', seq: 8, day, runId: 'run-live', sheetIds: ['k14-1', 'gf-2'], materials: ['gold'], orders: {}, status: 'open', updatedAt: ts, createdAt: ts });
    assert.equal(z(zs('ss-3'), x => x.newSet).ok, false); assert.match(z(zs('ss-3'), x => x.newSet).reason, /open set takes it/);
    sz = LF.sheetZone({ kind: 'sheet', id: 'ss-3' }, 'gf-10', { self: 'SS Sheet 3', name: 'GF Sheet 10' }); assert.equal(sz.ok, true); assert.equal(sz.sub, 'both join Set 8');
    joins.length = 0; p = await plan('ss-3', { sheet: 'gf-10' }); assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.equal(p.to.setId, 'set-8'); assert(p.auto.some(a => /SS Sheet 3 added to Set 8/.test(a.label)));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true, JSON.stringify(r)); assert.deepEqual(joins.map(j => [j.id, j.newSet, j.setId]), [['ss-3', false, 'set-8'], ['gf-10', false, 'set-8']]);
    // the other sets: an uncommitted set that is not the open one is dimmed, said in plain words, before any plan is asked for
    const sets0 = LF.hooks.sets; LF.configure({ sets: () => sets0().concat([{ setId: 'set-7', seq: 7 }]) });
    assert.match(z(zs('gf-4'), x => x.set === 'set-7').reason, /Set 7 is not the open set\. Drop it on Set 8/); assert.equal(z(zs('gf-4'), x => x.set === 'set-8').ok, true);
    p = await plan('gf-4', { set: 'set-8' }); assert.equal(p.ok, true, JSON.stringify(p.needs)); LF.configure({ sets: sets0 });

    // ═══ E. sheets that share a multi-piece order: the cardinal rule, with its one yes ═══
    for (const id of Object.keys(L)) if (id !== 'in-set') L[id].dispatchSetId = null;     // (back to no open set, for the next scenes)
    LF.configure({ sets: () => [{ setId: 'set-1', seq: 1, committedAt: now - 5000, status: 'complete' }] });
    joins.length = 0; for (const k of Object.keys(pulls)) delete pulls[k];
    // gf-4 and ss-5 share an order. Dropping gf-4 on gf-6: ss-5 must come too, so the one yes is asked, naming both sheets that travel
    p = await plan('gf-4', { sheet: 'gf-6' }); assert.equal(p.ok, true, JSON.stringify(p.needs));
    const tog = p.confirm.find(c => c.key === 'together'); assert(tog && /SS Sheet 5 joins a new set with GF Sheet 4 and GF Sheet 6/.test(tog.label), JSON.stringify(p.confirm));
    assert.deepEqual(p.steps.map(x => [x.type, x.sheetId, !!x.also]), [['include', 'gf-4', false], ['include', 'gf-6', true]]); assert.deepEqual(p.steps[0].with, ['ss-5']);
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, false); assert.match(r.error, /Needs your yes first/, 'no yes, nothing done'); assert.equal(joins.length, 0);
    pulls['gf-4'] = ['ss-5'];                                                          // (Gate.assemble pulls the sheet that shares the order along)
    r = await LF.commit(p, { by: 'Paul', confirmed: ['together'] }); assert.equal(r.ok, true, JSON.stringify(r));
    assert.deepEqual(joins.map(j => [j.id, j.split, j.with]), [['gf-4', 'all', ['ss-5']], ['gf-6', 'all', undefined]]); assert.equal(st.doc(S, 'ss-5').draft, false, 'the sheet that shares the order came with it');
    // the sheet dropped on shares the order itself: both go, and no yes is asked about a sheet the drop already names
    for (const id of ['gf-4', 'ss-5', 'gf-6']) { st.put(S, id, { draft: true, setId: null }); draft(id); }
    p = await plan('gf-4', { sheet: 'ss-5' }); assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.confirm, [], 'the orders only tie the two sheets that were dropped together');
    for (const k of Object.keys(pulls)) delete pulls[k]; joins.length = 0; pulls['gf-4'] = ['ss-5'];                // (ss-5 comes along with gf-4: the second step finds it in and does nothing)
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true, JSON.stringify(r)); assert.deepEqual(joins.map(j => j.id), ['gf-4'], 'one include: the other was already in'); assert(r.applied.some(a => /SS Sheet 5 added/.test(a.label)), JSON.stringify(r.applied));
    for (const id of ['gf-4', 'ss-5']) { st.put(S, id, { draft: true, setId: null }); draft(id); } for (const k of Object.keys(pulls)) delete pulls[k];

    // ═══ F. what blocks a drop on a sheet, each said in plain words, and nothing is written ═══
    for (const id of ['k14-1', 'gf-2', 'gf-6', 'gf-10']) { st.put(S, id, { draft: true, setId: null }); draft(id); }
    const blocked = async (a, b, key, re) => { joins.length = 0; const q = await plan(a, { sheet: b }); assert.equal(q.ok, false, a + ' on ' + b); assert(keys(q).includes(key), a + ' on ' + b + ': ' + key + ' in ' + JSON.stringify(q.needs)); assert.match(q.needs.find(n => n.key === key).label + ' ' + q.needs.find(n => n.key === key).detail, re); const c = await LF.commit(q, { by: 'Paul' }); assert.equal(c.ok, false); assert.equal(joins.length, 0, 'nothing written'); return q; };
    await blocked('k14-1', 'rg-7', 'roseCombine', /Rose Gold.*Cut Sheet press/);
    await blocked('rg-7', 'k14-1', 'roseCombine', /Rose Gold.*Cut Sheet press/);
    await blocked('gf-6', 'gf-9', 'sheetCut', /was already cut|permanent/);
    await blocked('gf-6', 'gf-8', 'sheetCompleted', /is completed/);
    st.put(S, 'in-set', { draft: false, setId: 'set-9' }); await blocked('gf-6', 'in-set', 'alsoInSet', /already in/);
    L['gf-10'].can = { ok: false, reason: 'It is still being nested or saved.' }; await blocked('gf-6', 'gf-10', 'include', /GF Sheet 10 is not ready to join a set.*nested or saved/); L['gf-10'].can = { ok: true, byHand: true };
    delete L['gf-10']; await blocked('gf-6', 'gf-10', 'runElsewhere', /GF Sheet 10: its run is open on another screen/);      // (its run is open, and this page does not hold the sheet)
    st.put(RUN, 'run-old', { runId: 'run-old', status: 'complete', lines: {} }); st.put(S, 'gf-10', { runId: 'run-old' }); await blocked('gf-6', 'gf-10', 'fixedSet', /GF Sheet 10 is closed.*run is finished/); st.put(S, 'gf-10', { runId: 'run-live' }); draft('gf-10');
    L['gf-6'].can = { ok: false, reason: 'No charms are placed on it.' }; await blocked('gf-6', 'gf-2', 'include', /Not ready to join a set.*No charms/); L['gf-6'].can = { ok: true, byHand: true };
    // the sheet dropped on could not join after the first did: the first is in, and the answer says so
    st.put(S, 'k14-1', { draft: true, setId: null }); st.put(S, 'gf-2', { draft: true, setId: null }); draft('k14-1'); draft('gf-2');
    joins.length = 0; down.add('gf-2'); p = await plan('k14-1', { sheet: 'gf-2' }); r = await LF.commit(p, { by: 'Paul' }); down.delete('gf-2');
    assert.equal(r.ok, false); assert.match(r.error, /14K Sheet 1 added to a new set, but GF Sheet 2 could not join: Waits for GF Sheet 2/, r.error); assert.deepEqual(joins.map(j => j.id), ['k14-1']);

    // ═══ G. the old ways are as they were: a sheet on New set or on the open set, one at a time ═══
    st.put(S, 'k14-1', { draft: true, setId: null }); draft('k14-1'); joins.length = 0;
    p = await plan('k14-1', { newSet: true }); assert.equal(p.ok, true); r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true, JSON.stringify(r)); assert.deepEqual(joins.map(j => [j.id, j.newSet]), [['k14-1', true]]);
    console.log('PASS: combine sheets by drag and drop: every place the move bar offers a draft sheet answers in plain words; a draft sheet dropped on another joins both to the open set or a new one; the cardinal rule keeps its one yes; Rose Gold, cut, completed, in-a-set and not-ready sheets are refused whole, writing nothing');
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
