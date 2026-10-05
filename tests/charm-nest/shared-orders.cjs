// The cardinal rule of a Set of Sheets (Paul, 5 Oct 2026), over a FAKE backend only (bridge-server.cjs: the in-memory shop running the
// real charmNestLibrary handler). Nothing here touches the live site, a real set or a real order.
//   every sheet that shares a multi-piece order with another is in the SAME set:
//   · the pure core (groups, between, composition) · the server's twin (flowState with a move, sharedOrders, setUpdate refuses)
//   · LibraryFlow.plan: a blocked move says which orders and writes nothing, commit refuses, the whole group moving together is allowed
//   · a three-sheet chain, removal unblocks, a repeat commit, a stale page, Rose Gold and 10K/14K as before, a failed write loses nothing
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const SO = require('../../charm-nest-shared-orders.js');
const LF = require('../../charm-nest-flow.js');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';
const C = SO.core;

const X = '3900000001', Y = '3900000002', Z = '3900000003', W = '3900000004', P = '3900000005', Q = '3900000006', K = '3900000007', R = '3900000008', T = '3900000009', M = '3900000010', N = '3900000011', U = '3900000012', V = '3900000013';

(async () => {
  // ── 0. the pure core ─────────────────────────────────────────────────────────────────────────────────────────────
  {
    const sh = (id, setId, keys, extra = {}) => C.sheetOf({ id, metal: 'gold', page: +id.replace(/\D/g, '') || 1, setId, poolIds: keys, ...extra });
    const a = sh('a1', 'set-1', [`${X}_1_1`, `${P}_1_1`]), b = sh('b2', null, [`${X}_1_2`]), c = sh('c3', null, [`${Q}_1_1`]), d = sh('d4', 'set-1', [`${Y}_1_1`, `${Y}_1_2`]);
    assert.deepEqual(C.groups([a, b, c, d]).map(g => g.ids.sort()), [['a1', 'b2']], 'one order on two sheets ties them; one sheet with all its pieces, and a lone sheet, tie nothing');
    // one piece listed on two sheets (a stale record) is ONE piece: not shared
    assert.deepEqual(C.groups([sh('e1', null, [`${X}_1_1`]), sh('e2', null, [`${X}_1_1`])]), []);
    // an order's single piece is never shared however many sheets we look at
    assert.deepEqual(C.groups([sh('f1', null, [`${Q}_1_1`]), sh('f2', null, [`${P}_1_1`])]), []);
    const items = C.between([a, b, c, d], ['b2'], 'set-1'); assert.deepEqual(items, [], 'b2 moving INTO the set its mate is in: nothing is split');
    const out = C.between([a, b, c, d], ['b2'], 'new'); assert.equal(out.length, 1); assert.equal(out[0].orderId, X); assert.deepEqual(out[0].hereIds, ['b2']); assert.deepEqual(out[0].thereIds, ['a1']);
    assert.equal(out[0].total, 2); assert.equal(out[0].pieces.length, 2); assert.deepEqual(out[0].here, 'GF Sheet 2'); assert.deepEqual(out[0].there, ['GF Sheet 1']);
    assert.deepEqual(C.between([a, b, c, d], ['a1', 'b2'], 'new'), [], 'the whole group moving together splits nothing');
    assert.deepEqual(C.between([a, b, c, d], ['c3'], 'set-1'), [], 'a sheet that shares nothing moves freely');
    // a three-sheet chain: x1-x2 share Y, x2-x3 share Z: one group of three
    const x1 = sh('x1', null, [`${Y}_1_1`, `${W}_1_1`]), x2 = sh('x2', null, [`${Y}_1_2`, `${Z}_1_1`]), x3 = sh('x3', null, [`${Z}_1_2`]);
    assert.deepEqual(C.groupOf([x1, x2, x3], 'x1').ids.sort(), ['x1', 'x2', 'x3']);
    assert.equal(C.between([x1, x2, x3], ['x1'], 'new').length, 1, 'x1 alone splits Y'); assert.deepEqual(C.between([x1, x2, x3], ['x1', 'x2'], 'new').map(i => i.orderId), [Z], 'x1+x2 without x3 splits Z'); assert.deepEqual(C.between([x1, x2, x3], ['x1', 'x2', 'x3'], 'new'), []);
    // composition after nesting: a sheet in no set that now shares an order with a sheet in set-1 is pulled in; two sets spread = a conflict told exactly
    const comp = C.composition([a, b, c, d]); assert.deepEqual(comp.pull.map(x => [x.id, x.setId, x.because]), [['b2', 'set-1', [X]]]); assert.deepEqual(comp.conflicts, []);
    const e2 = sh('e2', 'set-2', [`${X}_1_3`]); const comp2 = C.composition([a, e2]); assert.equal(comp2.conflicts.length, 1); assert.deepEqual(comp2.conflicts[0].sets.sort(), ['set-1', 'set-2']); assert.deepEqual(comp2.pull, []);
    // a draft sheet, or a solid left out, is in no set whatever its setId says
    assert.equal(C.effectiveSet({ setId: 'set-1', draft: true }), null); assert.equal(C.effectiveSet({ setId: 'set-1', solidIncluded: false }), null); assert.equal(C.effectiveSet({ setId: 'set-1' }), 'set-1');
    // an old record that lists orders and no pieces: one unknown piece per order and sheet, so two sheets with the same order still tie
    const o1 = C.sheetOf({ id: 'o1', metal: 'gold', orders: [X] }), o2 = C.sheetOf({ id: 'o2', metal: 'gold', orders: [X] }); assert.equal(C.groups([o1, o2]).length, 1);
    assert(/Order .* has pieces on/.test(C.sentence(out, 'GF Sheet 2')));
  }

  // ── the fake shop ────────────────────────────────────────────────────────────────────────────────────────────────
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-05';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const orderOf = k => k.split('_')[0];
  // a sheet holding these pieces (pool ids), ready for Laser cutting
  const mk = (id, keys, extra = {}) => {
    const orders = [...new Set(keys.map(orderOf))];
    st.put(S, id, { id, runId: 'run-live', metal: 'gold', day, status: 'complete', placedCount: keys.length, charmCount: keys.length, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: keys, orders, verification: { ok: true },
      outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
      label: { files: [{ path: id + '-qr.png', url: image, payload: orders[0], orders }], orders }, page: 1, sheetIndex: 1, updatedAt: ts, createdAt: ts, ...extra });
  };
  const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, '') || 1, day, runId: 'run-live', sheetIds, materials: ['gold'], orders: {}, status: 'open', updatedAt: ts, createdAt: ts, ...extra });
  const piece = (o, n) => `${o}_1_${n}`;

  mkSet('set-3', ['opn-a']); mk('opn-a', [piece(X, 1), piece(P, 1)], { setId: 'set-3', setSeq: 3, page: 1, sheetIndex: 1 });
  mk('live-a', [piece(X, 2)], { draft: true, page: 2, sheetIndex: 2 });            // shares X with opn-a (in set-3)
  mk('live-b', [piece(Q, 1)], { draft: true, page: 3, sheetIndex: 3 });            // shares nothing
  mk('ch-1', [piece(Y, 1), piece(W, 1)], { draft: true, page: 4, sheetIndex: 4 });  // the chain: ch-1 – ch-2 share Y, ch-2 – ch-3 share Z
  mk('ch-2', [piece(Y, 2), piece(Z, 1)], { draft: true, page: 5, sheetIndex: 5 });
  mk('ch-3', [piece(Z, 2)], { draft: true, page: 6, sheetIndex: 6 });
  mkSet('set-2', ['cm-a'], { status: 'complete', committedAt: now - 1000 }); mk('cm-a', [piece(K, 1)], { setId: 'set-2', setSeq: 2, page: 1, sheetIndex: 1 });
  mk('cm-d', [piece(K, 2)], { draft: true, page: 7, sheetIndex: 7 });              // shares K with a sheet in a committed set
  mk('rg-a', [piece(R, 1)], { metal: 'rose', draft: true, page: 1, sheetIndex: 1, roseStockId: 'stock-1', rosePlanHash: 'h-1' });
  mk('gold-r', [piece(R, 2)], { draft: true, page: 8, sheetIndex: 8 });            // shares R with a Rose Gold sheet
  mk('k10-a', [piece(T, 1)], { metal: 'gold10k', draft: true, page: 1, sheetIndex: 1 }); mk('k14-b', [piece(T, 2)], { metal: 'gold14k', draft: true, page: 1, sheetIndex: 1 });
  mkSet('set-1', ['mem-1', 'mem-2'], { status: 'labelled', runId: 'run-x' });      // a set of an older run: its sheets share M
  mk('mem-1', [piece(M, 1)], { setId: 'set-1', setSeq: 1, runId: 'run-x', page: 1, sheetIndex: 1 }); mk('mem-2', [piece(M, 2)], { setId: 'set-1', setSeq: 1, runId: 'run-x', page: 2, sheetIndex: 2 });
  mk('rg-solo', [piece(N, 1)], { metal: 'rose', draft: true, page: 2, sheetIndex: 2, roseStockId: 'stock-1', noLabel: true });
  mk('chn-a', [piece(U, 1)], { draft: true, page: 9, sheetIndex: 9 }); mk('chn-b', [piece(U, 2)], { draft: true, page: 10, sheetIndex: 10 });   // stale-page twin
  mkSet('set-6', ['sx-1'], { status: 'labelled' }); mk('sx-1', [piece(V, 1)], { setId: 'set-6', setSeq: 6, page: 11, sheetIndex: 11 }); mkSet('set-7', ['sx-2'], { status: 'labelled' }); mk('sx-2', [piece(V, 2)], { setId: 'set-7', setSeq: 7, page: 12, sheetIndex: 12 });   // the same order in two different sets, both ready
  // the lines of the run for the one set the server is asked to complete (what Readiness reads for engraving)
  const lines = {}; for (const [o, n] of [[V, 1], [V, 2]]) lines[`${o}_1`] = { orderId: o, state: 'written', quantity: 2, poolIds: [piece(o, 1), piece(o, 2)], engraveCandidate: false };
  st.put(RUN, 'run-live', { runId: 'run-live', status: 'review', lines }); st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines: {} });

  const joins = [], draftOf = id => !!(st.doc(S, id) || {}).draft;
  const draftLive = id => ({ runHere: true, draft: true, dispatchSetId: 'set-3', can: { ok: true, byHand: true }, split: [] });
  const liveHook = id => {
    const d = st.doc(S, id); if (!d) return null;
    if (d.metal === 'rose') return { runHere: true, draft: true, dispatchSetId: 'set-3', can: { ok: true }, rose: true, split: [] };
    if (id === 'cm-a') return { runHere: false, draft: false, dispatchSetId: 'set-3', can: { ok: false, reason: 'It is not on a page of the open run.' }, split: [] };
    return d.draft ? draftLive(id) : { runHere: true, draft: false, dispatchSetId: 'set-3', can: { ok: true }, split: [] };
  };
  LF.configure({
    api: post, employee: () => 'Paul', rows: () => [],
    remakeLabel: async () => {},
    include: async (id, o) => { joins.push({ id, ...o }); for (const x of [id, ...(o.with || [])]) st.put(S, x, { draft: false, setId: o.setId || 'set-9', setSeq: 3 }); },
    live: liveHook
  });
  const sheetsSnapshot = () => JSON.stringify([...st.docs.entries()].filter(([k]) => k.startsWith(S + '/') || k.startsWith(SET + '/')).sort());
  const writes = () => st.calls.filter(c => ['flowApply', 'putSheet', 'setUpdate', 'laserDone', 'laserStatus'].includes(c.op));
  try {
    // ── A. the server's twin: the records alone say which orders a move would split ──
    let r = await post({ op: 'sharedOrders', kind: 'sheet', id: 'live-a', to: { newSet: true } });
    assert.equal(r.status, 200); assert.deepEqual(r.shared.map(i => i.orderId), [X]); assert.deepEqual(r.shared[0].there, ['GF Sheet 1']); assert.deepEqual(r.shared[0].here, 'GF Sheet 2'); assert.deepEqual(r.group.ids.sort(), ['live-a', 'opn-a']);
    r = await post({ op: 'sharedOrders', kind: 'sheet', id: 'live-a', to: { set: 'set-3' } }); assert.deepEqual(r.shared, [], 'joining the set its mate is in splits nothing');
    r = await post({ op: 'sharedOrders', kind: 'sheet', id: 'live-b', to: { set: 'set-3' } }); assert.deepEqual(r.shared, [], 'a sheet that shares nothing');
    r = await post({ op: 'sharedOrders', kind: 'sheet', id: 'ch-1', to: { newSet: true } }); assert.deepEqual(r.shared.map(i => i.orderId), [Y], 'ch-1 alone splits Y'); assert.deepEqual(r.group.ids.sort(), ['ch-1', 'ch-2', 'ch-3'], 'the chain reaches ch-3 through ch-2');
    r = await post({ op: 'sharedOrders', kind: 'sheet', id: 'ch-1', to: { newSet: true }, together: true }); assert.deepEqual(r.shared, [], 'the whole chain together splits nothing');
    r = await post({ op: 'sharedOrders', kind: 'sheet', id: 'cm-d', to: { set: 'set-3' } }); assert.deepEqual(r.shared.map(i => i.orderId), [K]); assert(r.shared[0].locked.some(l => l.sheetId === 'cm-a' && /committed to the Design Station/.test(l.why)), JSON.stringify(r.shared[0].locked));
    r = await post({ op: 'flowState', sheetIds: ['live-a'], setIds: ['set-3'], move: { kind: 'sheet', id: 'live-a', to: { newSet: true } } }); assert.deepEqual(r.shared.map(i => i.orderId), [X], 'flowState carries it with the rest of the state');
    r = await post({ op: 'flowState', sheetIds: ['live-a'], setIds: ['set-3'] }); assert.equal(r.shared, undefined, 'a move with no membership change asks nothing more');

    // ── B. dragging a sheet out of / into a set: blocked with the exact orders, writes nothing ──
    const before = sheetsSnapshot(); let callsBefore = writes().length;
    let p = await LF.plan({ kind: 'sheet', id: 'live-a', to: { newSet: true } });
    assert.equal(p.ok, false); assert.equal(p.needs[0].key, 'sharedOrders', JSON.stringify(p.needs.map(n => n.key)));
    assert.deepEqual(p.needs[0].items.map(i => i.orderId), [X], 'exactly the order that prevents it'); assert.deepEqual(p.shared.map(i => i.orderId), [X]);
    assert(/pieces on another sheet/.test(p.needs[0].label)); assert(/GF Sheet 2/.test(p.needs[0].detail)); assert(p.group.includes('GF Sheet 1') && p.group.includes('GF Sheet 2'), JSON.stringify(p.group));
    assert.equal(p.needs[0].items[0].pieces.length, 2); assert.equal(p.needs[0].items[0].pieces[0].sheetLabel === 'GF Sheet 1' || p.needs[0].items[0].pieces[0].sheetLabel === 'GF Sheet 2', true);
    assert.equal(sheetsSnapshot(), before, 'a plan writes nothing'); assert.equal(writes().length, callsBefore);
    let c = await LF.commit(p, { by: 'Paul' }); assert.equal(c.ok, false); assert(/pieces on another sheet/.test(c.error)); assert.equal(sheetsSnapshot(), before, 'a refused commit writes nothing'); assert.equal(joins.length, 0);
    // joining the set its mate is already in is allowed, and nothing else is asked
    p = await LF.plan({ kind: 'sheet', id: 'live-a', to: { set: 'set-3' } }); assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.equal(p.shared, undefined); assert(!p.confirm.some(x => x.key === 'together'));
    // a sheet without shared orders: the plan is the plain one
    p = await LF.plan({ kind: 'sheet', id: 'live-b', to: { set: 'set-3' } }); assert.equal(p.ok, true); assert.equal(p.shared, undefined); assert(!p.needs.some(x => x.key === 'sharedOrders')); assert(p.steps.some(x => x.type === 'include' && !x.with));
    // out of a set: the sheet that stays behind is named, and the base reasons come after the cardinal one
    p = await LF.plan({ kind: 'sheet', id: 'mem-1', to: { set: 'set-3' } }); assert.equal(p.ok, false); assert.equal(p.needs[0].key, 'sharedOrders'); assert.deepEqual(p.needs[0].items.map(i => i.orderId), [M]);
    assert(/cannot leave its set/.test(p.needs[0].detail) && /GF Sheet 2/.test(p.needs[0].detail), p.needs[0].detail); assert(p.needs.length > 1, 'the round-1 reasons are still told');
    // a mate in a committed set can never join: the exact reason, no way through
    p = await LF.plan({ kind: 'sheet', id: 'cm-d', to: { set: 'set-3' } }); assert.equal(p.ok, false); assert.equal(p.needs[0].key, 'sharedOrders'); assert(/Set 2 was committed to the Design Station/.test(p.needs[0].detail), p.needs[0].detail); assert.deepEqual(p.confirm, []);
    c = await LF.commit(p, { by: 'Paul', confirmed: ['together', 'splitOrders'] }); assert.equal(c.ok, false, 'no yes key lifts it'); assert.equal(sheetsSnapshot(), before);

    // ── C. a three-sheet chain moves as one: one yes, and every sheet joins ──
    p = await LF.plan({ kind: 'sheet', id: 'ch-1', to: { set: 'set-3' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.confirm.map(x => x.key), ['together']); assert.deepEqual(p.shared.map(i => i.orderId), [Y], 'the order that ties ch-1 to the rest is named');
    assert(/GF Sheet 5/.test(p.confirm[0].label) && /GF Sheet 6/.test(p.confirm[0].label), p.confirm[0].label); assert(p.auto.some(a => a.key === 'membership:ch-2') && p.auto.some(a => a.key === 'membership:ch-3'));
    assert.deepEqual(p.steps.find(x => x.type === 'include').with.sort(), ['ch-2', 'ch-3']);
    assert.equal(sheetsSnapshot(), before, 'planning the chain writes nothing');
    c = await LF.commit(p, { by: 'Paul' }); assert.equal(c.ok, false); assert(/yes first/.test(c.error)); assert.equal(joins.length, 0, 'no yes, no move'); assert.equal(sheetsSnapshot(), before);
    // ── D. removal unblocks: the order comes off one sheet (a record edit here; the page does it with SheetWin.takeOffOrder) ──
    //      ch-3 loses Z: the chain is ch-1 – ch-2 only
    st.put(S, 'ch-3', { poolIds: [], orders: [] });
    p = await LF.plan({ kind: 'sheet', id: 'ch-1', to: { set: 'set-3' } }); assert.equal(p.ok, true); assert.deepEqual(p.steps.find(x => x.type === 'include').with, ['ch-2'], 'ch-3 no longer ties to the group');
    assert(/GF Sheet 5/.test(p.confirm[0].label) && !/GF Sheet 6/.test(p.confirm[0].label), p.confirm[0].label);
    // ch-2 loses Y: ch-1 shares nothing now and moves alone, with no extra yes
    st.put(S, 'ch-2', { poolIds: [piece(Z, 1)], orders: [Z] });
    p = await LF.plan({ kind: 'sheet', id: 'ch-1', to: { set: 'set-3' } }); assert.equal(p.ok, true); assert.deepEqual(p.confirm.map(x => x.key), []); assert.equal(p.shared, undefined); assert(p.steps.some(x => x.type === 'include' && !x.with));
    // and the blocked move from B is free once its order comes off the sheet it shared with: live-a loses X
    st.put(S, 'live-a', { poolIds: [], orders: [] }); p = await LF.plan({ kind: 'sheet', id: 'live-a', to: { newSet: true } }); assert(!p.needs.some(x => x.key === 'sharedOrders'), JSON.stringify(p.needs.map(n => n.key)));
    st.put(S, 'live-a', { poolIds: [piece(X, 2)], orders: [X] });
    // put the chain back for the move itself
    st.put(S, 'ch-2', { poolIds: [piece(Y, 2), piece(Z, 1)], orders: [Y, Z] }); st.put(S, 'ch-3', { poolIds: [piece(Z, 2)], orders: [Z] });
    p = await LF.plan({ kind: 'sheet', id: 'ch-1', to: { set: 'set-3' } });
    c = await LF.commit(p, { by: 'Paul', confirmed: ['together'] }); assert.equal(c.ok, true, JSON.stringify(c));
    assert.deepEqual(joins, [{ id: 'ch-1', setId: 'set-3', newSet: false, split: 'all', with: ['ch-2', 'ch-3'] }]);
    for (const id of ['ch-1', 'ch-2', 'ch-3']) assert.equal(st.doc(S, id).setId, 'set-3', id + ' is in the set');
    assert(c.applied.some(a => a.key === 'membership:ch-2') && c.applied.some(a => a.key === 'membership:ch-3'), 'the applied list names every sheet that joined');
    // ── E. a repeat commit changes nothing ──
    const after = sheetsSnapshot(); c = await LF.commit(p, { by: 'Paul', confirmed: ['together'] }); assert.equal(c.ok, true); assert.equal(c.noop, true); assert.deepEqual(c.applied, []); assert.equal(joins.length, 1); assert.equal(sheetsSnapshot(), after);

    // ── F. a stale page: the page's own answer is empty (it still shows the sheet alone), the records say otherwise ──
    st.put(S, 'chn-a', {}); const pageSays = { between: () => [], groupOf: () => ({ ids: ['chn-a'], labels: ['GF Sheet 9'], members: [{ id: 'chn-a', label: 'GF Sheet 9', setId: null }], orders: [], sets: [] }), enrich: () => {} };
    LF.configure({ shared: pageSays });
    p = await LF.plan({ kind: 'sheet', id: 'chn-a', to: { newSet: true } }); assert.equal(p.ok, false); assert.equal(p.needs[0].key, 'sharedOrders'); assert.deepEqual(p.needs[0].items.map(i => i.orderId), [U], 'the server twin reads the records');
    // …and the page that sees what the records do not yet (the sheet nested a moment ago) blocks too
    mk('fresh', [piece('3900000099', 1)], { draft: true, page: 13, sheetIndex: 13 });
    LF.configure({ shared: { between: () => [{ orderId: '3900000099', label: 'Order 3900000099', customer: 'A. Buyer', thumb: null, total: 2, ids: [], here: 'GF Sheet 13', there: ['GF Sheet 14'], hereIds: ['fresh'], thereIds: ['gone'], pieces: [], locked: [] }], groupOf: () => null, enrich: () => {} } });
    p = await LF.plan({ kind: 'sheet', id: 'fresh', to: { newSet: true } }); assert.equal(p.needs[0].key, 'sharedOrders'); assert.deepEqual(p.needs[0].items.map(i => i.orderId), ['3900000099']); assert.equal(p.needs[0].items[0].customer, 'A. Buyer');
    LF.configure({ shared: null }); LF.hooks.shared = null;
    // the server refuses a set that would be completed without a sheet that shares its order (a page with stale data cannot do it)
    const completeBefore = JSON.stringify(st.doc(SET, 'set-6'));
    r = await post({ op: 'setUpdate', setId: 'set-6', patch: { status: 'complete' } });
    assert.notEqual(r.status, 200, JSON.stringify(r)); assert(new RegExp(`order ${V} also has pieces on GF Sheet 12, which is not in this set`).test(r.error || ''), 'it names the order and the sheet: ' + r.error);
    assert.equal(JSON.stringify(st.doc(SET, 'set-6')), completeBefore, 'refused: the set is as it was');

    // ── G. composition after nesting: a sheet that now shares an order with a sheet of a set is pulled together ──
    {
      const list = [...st.docs.entries()].filter(([k]) => k.startsWith(S + '/')).map(([k, v]) => C.sheetOf(v)).filter(Boolean);
      const comp = C.composition(list);
      assert(comp.pull.some(x => x.id === 'live-a' && x.setId === 'set-3' && x.because.includes(X)), JSON.stringify(comp.pull));
      assert(comp.conflicts.length === 0 || comp.conflicts.every(g => g.sets.length > 1));
    }

    // ── H. Rose Gold and 10K / 14K ──
    // a Rose Gold sheet that shares nothing: the plan is exactly the one it had (no cardinal need, no extra confirm)
    LF.configure({ roseJoin: async () => ({ ok: true, sheets: [] }) });
    p = await LF.plan({ kind: 'sheet', id: 'rg-solo', to: { set: 'set-3' } }); assert(!p.needs.some(x => x.key === 'sharedOrders'), JSON.stringify(p.needs)); assert.equal(p.shared, undefined); assert.deepEqual(p.confirm.map(x => x.key), ['roseSet', 'roseLine'], 'the plain Rose Gold plan, as in library-flow.cjs');
    LF.configure({ roseJoin: null }); LF.hooks.roseJoin = null;
    // a gold sheet that shares an order with a Rose Gold one: Rose Gold is never pulled in on its own, the person is told why
    p = await LF.plan({ kind: 'sheet', id: 'gold-r', to: { set: 'set-3' } }); assert.equal(p.ok, false); assert.equal(p.needs[0].key, 'sharedOrders'); assert(/Rose Gold sheet joins a set only by its own Cut Sheet press/.test(p.needs[0].detail), p.needs[0].detail); assert.deepEqual(p.needs[0].items.map(i => i.orderId), [R]);
    // 10K and 14K: the sheets of one order go in together (the old "only this sheet" choice is gone), through one yes
    p = await LF.plan({ kind: 'sheet', id: 'k10-a', to: { set: 'set-3' } }); assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.confirm.map(x => x.key), ['together']); assert(/10K Sheet 1/.test(p.confirm[0].detail) && /14K Sheet 1/.test(p.confirm[0].detail), p.confirm[0].detail);
    assert(!p.confirm.some(x => x.key === 'splitOrders'));
    // a page whose live read still reports the old per-sheet split (an older page) gets the same plan: both join
    LF.configure({ live: id => id === 'k10-a' ? { runHere: true, draft: true, dispatchSetId: 'set-3', can: { ok: true }, split: [{ label: '14K Sheet 1', orders: [T] }] } : liveHook(id) });
    p = await LF.plan({ kind: 'sheet', id: 'k10-a', to: { set: 'set-3' } }); assert.deepEqual(p.confirm.map(x => x.key), ['together']); assert(!p.confirm.some(x => x.key === 'splitOrders'));
    c = await LF.commit(p, { by: 'Paul', confirmed: ['together'] }); assert.equal(c.ok, true, JSON.stringify(c)); assert.deepEqual(joins.at(-1), { id: 'k10-a', setId: 'set-3', newSet: false, split: 'all', with: ['k14-b'] }); assert.equal(st.doc(S, 'k14-b').setId, 'set-3');
    LF.configure({ live: liveHook });

    // ── I. nothing is lost when the write fails ──
    mk('boom-a', [piece('3900000050', 1)], { draft: true, page: 20, sheetIndex: 20 }); mk('boom-b', [piece('3900000050', 2)], { draft: true, page: 21, sheetIndex: 21 });
    const snap = sheetsSnapshot(); LF.configure({ include: async () => { throw new Error('the cloud said no'); } });
    p = await LF.plan({ kind: 'sheet', id: 'boom-a', to: { set: 'set-3' } }); assert.equal(p.ok, true, JSON.stringify(p.needs));
    c = await LF.commit(p, { by: 'Paul', confirmed: ['together'] }); assert.equal(c.ok, false); assert(/cloud said no/.test(c.error)); assert.equal(sheetsSnapshot(), snap, 'a failed move changes nothing');
    // the whole cloud failing while it plans: refused, nothing written
    st.fail.charmNestLibrary = true; c = await LF.commit({ move: { kind: 'sheet', id: 'boom-a', to: { set: 'set-3' } } }, { by: 'Paul', confirmed: ['together'] }); st.fail.charmNestLibrary = false; assert.equal(c.ok, false); assert.equal(sheetsSnapshot(), snap);

    console.log('PASS: cardinal rule: core groups/between/composition, server twin (flowState, sharedOrders, setUpdate), blocked drag names the orders and writes nothing, three-sheet chain moves as one, removal unblocks, repeat commit idempotent, stale page, Rose Gold and 10K/14K, failure loses nothing');
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
