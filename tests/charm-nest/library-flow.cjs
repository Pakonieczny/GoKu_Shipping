// LibraryFlow over a FAKE backend only (the in-memory shop of bridge-server.cjs running the real charmNestLibrary handler):
// plan and commit for every move forwards and backwards, a sheet into a set, Rose Gold with and without its confirm, hard
// blockers, a repeat commit, a failed write that leaves everything as it was. Nothing here touches the live site.
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const R = require('../../charm-nest-readiness.js');
const LF = require('../../charm-nest-flow.js');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs', TL = 'Order_Timeline';

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const lines = {};
  let n = 0;
  // a sheet of one order, ready for Laser cutting unless `extra` says otherwise
  const mk = (id, extra = {}, order) => {
    order = order || String(3800000000 + ++n);
    const pool = order + '_1_1';
    lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: [pool], ...(extra.needsBack ? { engrave: { needed: true, state: 'review', approved: false } } : { engraveCandidate: false }) };
    const { needsBack, noLabel, ...rest } = extra;
    st.put(S, id, { id, runId: 'run-x', metal: 'gold', day, status: 'complete', placedCount: 1, charmCount: 1, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: [pool], orders: [order], verification: { ok: true },
      outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
      ...(noLabel ? {} : { label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders: [order] }], orders: [order] } }), sheetIndex: 1, updatedAt: ts, createdAt: ts, ...rest });
    return order;
  };
  const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, '') || 1, day, runId: 'run-x', sheetIds, materials: ['gold'], orders: {}, status: 'labelled', updatedAt: ts, createdAt: ts, ...extra });

  // ── fixtures ──
  mkSet('set-1', ['mem-1', 'mem-2']); mkSet('set-2', ['cmt-1'], { status: 'complete', committedAt: now - 1000 }); mkSet('set-3', ['opn-1'], { status: 'open', runId: 'run-live' });
  mk('solo', {}); mk('noqr', { noLabel: true }); mk('noback', { needsBack: true }); mk('draft', { draft: true });
  mk('mem-1', { setId: 'set-1', setSeq: 1, sheetIndex: 1 }); mk('mem-2', { setId: 'set-1', setSeq: 1, sheetIndex: 2, verification: { ok: false } });
  mk('cmt-1', { setId: 'set-2', setSeq: 2 });
  mk('rose', { metal: 'rose', roseStockId: 'stock-1', noLabel: false });
  mk('live1', { draft: true, runId: 'run-live' });
  mk('opn-1', { setId: 'set-3', setSeq: 3, runId: 'run-live' });
  mk('rg-new', { metal: 'rose', draft: true, runId: 'run-live', roseStockId: 'stock-1', noLabel: true });                       // no line yet
  mk('rg-line', { metal: 'rose', draft: true, runId: 'run-live', roseStockId: 'stock-1', noLabel: true, rosePlanHash: 'h-1' });   // has its line
  mk('rg-full', { metal: 'rose', draft: true, runId: 'run-live', roseStockId: 'stock-1', noLabel: true, rosePlanHash: 'h-2' });   // full: no line, no cut
  mk('rg-cut', { metal: 'rose', setId: 'set-1', setSeq: 1, roseStockId: 'stock-1', roseCutAt: now - 5000, rosePlanHash: 'h-3' });
  mkSet('set-4', ['cmt-2'], { status: 'complete', committedAt: now - 2000 }); mk('cmt-2', { setId: 'set-4', setSeq: 4, runId: 'run-live' });   // committed, not cut, its run still open
  st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines }); st.put(RUN, 'run-live', { runId: 'run-live', status: 'review', lines: {} });

  const labels = [], joins = [], roseCalls = [], marks = [];
  LF.configure({
    api: post, employee: () => 'Paul', rows: () => [],
    remakeLabel: async id => { labels.push(id); const rec = st.doc(S, id); st.put(S, id, { label: { files: [{ path: id + '-qr.png', url: image, payload: rec.orders[0], orders: rec.orders }], orders: rec.orders } }); },
    include: async (id, o) => { joins.push({ id, ...o }); st.put(S, id, { draft: false, setId: o.setId || 'set-9', setSeq: 3 }); },
    live: id => id === 'live1' ? { runHere: true, draft: true, dispatchSetId: 'set-3', can: { ok: true, byHand: true }, split: [] } : id === 'opn-1' ? { runHere: true, draft: false, dispatchSetId: 'set-3', can: { ok: true } }
      : /^rg-(new|line)$/.test(id) ? { runHere: true, draft: true, dispatchSetId: 'set-3', can: { ok: true }, rose: { full: false }, split: [] } : id === 'rg-full' ? { runHere: true, draft: true, dispatchSetId: 'set-3', can: { ok: true }, rose: { full: true }, split: [] }
      : id === 'cmt-2' ? { runHere: false, draft: false, dispatchSetId: 'set-3', can: { ok: false, reason: 'It is not on a page of the open run.' }, split: [] } : null
  });
  const held = id => !!(st.doc(S, id).laserHold && st.doc(S, id).laserHold.at);
  const area = async (kind, id) => (await LF.plan({ kind, id, to: { area: 'nowhere' } })).from.area;
  const seals = id => (st.doc(S, id).processSeals || []).map(x => x.how + ':' + x.by);
  try {
    // ── A. a sheet alone: Laser cutting → Completed → Laser cutting → In progress → Laser cutting, every seal kept ──
    assert.equal(await area('sheet', 'solo'), 'laser');
    let p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'completed' } });
    assert.equal(p.from.area, 'laser'); assert.equal(p.to.area, 'completed'); assert(p.ok); assert.deepEqual(p.needs, []);
    assert(p.auto.some(a => a.key === 'mark' && /Paul/.test(a.label)), 'it says who marks it');
    assert.equal(st.doc(S, 'solo').laserDoneAt, undefined, 'a plan writes nothing');
    assert.equal(st.calls.filter(c => c.op === 'laserDone' || c.op === 'flowApply').length, 0);
    let r = await LF.commit(p, { by: 'Paul' });
    assert.equal(r.ok, true, JSON.stringify(r)); assert(+st.doc(S, 'solo').laserDoneAt > 0); assert(r.applied.some(a => a.key === 'mark'));
    assert.equal(await area('sheet', 'solo'), 'completed');
    const callsBefore = st.calls.length;
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true); assert.equal(r.noop, true); assert.deepEqual(r.applied, [], 'a repeat commit does nothing');
    assert(!st.calls.slice(callsBefore).some(c => c.op === 'laserDone' || c.op === 'flowApply'), 'and writes nothing');
    const cutSeals = seals('solo'); assert(cutSeals.includes('laserDone:Paul'));
    // backwards: Completed → Laser cutting (Undo path), seals stay
    p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'laser' } }); assert(p.ok); assert(p.auto.some(a => a.key === 'reopen'));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true); assert(!st.doc(S, 'solo').laserDoneAt, 'reopened');
    assert.deepEqual(seals('solo').slice(0, cutSeals.length), cutSeals, 'every seal is kept');
    // Laser cutting → In progress: a hold, nothing else touched
    const approvals = JSON.stringify([st.doc(S, 'solo').label, st.doc(S, 'solo').verification, st.doc(S, 'solo').outputs]);
    p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'progress' } }); assert(p.ok); assert(p.auto.some(a => a.key === 'hold')); assert(!held('solo'), 'planning holds nothing');
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true); assert(held('solo'));
    assert.equal(await area('sheet', 'solo'), 'progress'); assert.equal(JSON.stringify([st.doc(S, 'solo').label, st.doc(S, 'solo').verification, st.doc(S, 'solo').outputs]), approvals, 'approvals untouched');
    assert.deepEqual(seals('solo').slice(0, cutSeals.length), cutSeals);
    assert.equal(st.doc(S, 'solo').flowHistory.at(-1).type, 'hold'); assert(st.list(TL).some(e => e.sheetId === 'solo' && /held back/.test(e.text || '')), 'the orders get a note');
    assert(!R.laserSheet(st.doc(S, 'solo')).ready, 'the readiness policy honours the hold');
    assert.equal((await post({ op: 'laserDone', kind: 'sheet', id: 'solo', by: 'Paul', stage: 'laser' })).status, 409, 'a held sheet cannot be completed');
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true); assert.equal(r.noop, true);   // repeat: nothing more
    assert.equal(st.doc(S, 'solo').flowHistory.filter(x => x.type === 'hold').length, 1);
    // forwards again: the hold lifts, the person's seal is recorded
    p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'laser' } }); assert(p.ok, JSON.stringify(p.needs)); assert(p.auto.some(a => /released from its hold/.test(a.label)));
    assert(p.auto.some(a => a.check && /Layout verified/.test(a.label)), 'the checks that passed are shown');
    r = await LF.commit(p, { by: 'Seth' }); assert.equal(r.ok, true, JSON.stringify(r)); assert(!held('solo')); assert.equal(await area('sheet', 'solo'), 'laser');
    assert(seals('solo').includes('laserReady:Seth'), 'a ready seal by the signed-in person: ' + seals('solo'));
    assert.equal(st.doc(S, 'solo').flowHistory.at(-1).type, 'release');
    // Completed → In progress (reopen, then hold) and the way back
    await LF.commit(await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'completed' } }), { by: 'Paul' });
    p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'progress' } }); assert.equal(p.from.area, 'completed'); assert(p.auto.some(a => a.key === 'reopen') && p.auto.some(a => a.key === 'hold'));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true); assert(!st.doc(S, 'solo').laserDoneAt && held('solo'));
    await LF.commit(await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'laser' } }), { by: 'Paul' }); assert(!held('solo'));

    // ── B. what can be done by itself is done and said; what is missing blocks ──
    assert.equal(await area('sheet', 'noqr'), 'progress');
    p = await LF.plan({ kind: 'sheet', id: 'noqr', to: { area: 'laser' } });
    assert(p.ok, JSON.stringify(p.needs)); assert(p.auto.some(a => /QR label made/.test(a.label)) && p.auto.some(a => a.key === 'seal'));
    // what the approval screen reads: the kind, plain names of both ends, and real stamps flagged as such
    assert.equal(p.kind, 'sheet'); assert.equal(p.from.label, 'In progress'); assert.equal(p.to.label, 'Laser cutting');
    assert(p.auto.find(a => a.key === 'seal').stamp === true && p.auto.find(a => /^qrLabel/.test(a.key)).stamp === true, 'a seal and a remade QR label are stamps'); assert(!p.auto.find(a => a.check && a.stamp));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true, JSON.stringify(r)); assert.deepEqual(labels, ['noqr']); assert.equal(await area('sheet', 'noqr'), 'laser'); assert(seals('noqr').includes('laserReady:Paul'));
    assert(r.applied.find(a => a.key === 'seal').stamp === true && r.applied.find(a => a.key === 'seal').detail, 'applied lines carry the keys of the auto lines'); assert(r.applied.every(a => a.key && a.label));
    p = await LF.plan({ kind: 'sheet', id: 'noback', to: { area: 'laser' } });
    assert.equal(p.ok, false); const eng = p.needs.find(x => x.key === 'engraving'); assert(eng, JSON.stringify(p.needs)); assert(/back engraving/.test(eng.label)); assert(eng.items.length, 'it says which charms');
    const before = JSON.stringify(st.doc(S, 'noback'));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, false); assert(/engraving/i.test(r.error)); assert.equal(JSON.stringify(st.doc(S, 'noback')), before, 'a blocked commit changes nothing');
    let a = await LF.approve({ kind: 'sheet', id: 'noback', by: 'Paul' }); assert.equal(a.ok, false); assert(a.needs.some(x => x.key === 'engraving')); assert.equal(a.approved, false);
    // approve does the safe steps and is safe to call twice
    st.put(S, 'noqr', { label: null, laserDoneAt: undefined }); st.doc(S, 'noqr').label = undefined; st.put(S, 'noqr', { processSeals: [] });
    labels.length = 0; a = await LF.approve({ kind: 'sheet', id: 'noqr', by: 'Paul' });
    assert.equal(a.approved, true, JSON.stringify(a)); assert.deepEqual(labels, ['noqr']); assert(a.applied.some(x => /QR label/.test(x.label)));
    assert.equal(a.mode, 'approve'); assert(a.auto.every(x => x.done), 'an approval lists only what is done'); assert(a.applied.find(x => /^qrLabel/.test(x.key)).stamp === true);
    const a2 = await LF.approve({ kind: 'sheet', id: 'noqr', by: 'Paul' }); assert.equal(a2.approved, true); assert.deepEqual(labels, ['noqr'], 'the second press finds everything done'); assert.deepEqual(a2.applied, []);
    p = await LF.plan({ kind: 'sheet', id: 'draft', to: { area: 'laser' } }); assert(p.needs.some(x => x.key === 'membership'), 'a held-back sheet has to join a set first');
    p = await LF.plan({ kind: 'sheet', id: 'noqr', to: { area: 'completed' } });

    // ── C. a set: its sheets travel together ──
    p = await LF.plan({ kind: 'set', id: 'set-1', to: { area: 'laser' } });
    assert.equal(p.from.area, 'progress'); assert.equal(p.ok, false); assert(p.needs.some(x => x.key === 'nesting'), JSON.stringify(p.needs));
    p = await LF.plan({ kind: 'sheet', id: 'mem-1', to: { area: 'laser' } }); assert.equal(p.ok, false);
    const mem = p.needs.find(x => x.key === 'members'); assert(mem && /Set 1/.test(mem.detail) && mem.items.some(i => /Sheet 2/.test(i.label)), JSON.stringify(p.needs));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, false);
    st.put(S, 'mem-2', { verification: { ok: true } });
    assert.equal(await area('set', 'set-1'), 'laser'); p = await LF.plan({ kind: 'set', id: 'set-1', to: { area: 'laser' } }); assert.equal(p.noop, true);
    p = await LF.plan({ kind: 'set', id: 'set-1', to: { area: 'completed' } }); assert(p.ok);
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true, JSON.stringify(r)); assert(+st.doc(SET, 'set-1').laserDoneAt > 0); assert(+st.doc(S, 'mem-1').laserDoneAt > 0 && +st.doc(S, 'mem-2').laserDoneAt > 0);
    assert.equal(await area('set', 'set-1'), 'completed');
    p = await LF.plan({ kind: 'set', id: 'set-1', to: { area: 'progress' } }); assert(p.ok); assert(p.auto.some(a => a.key === 'hold') && p.auto.some(a => a.key === 'reopen'));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true); assert(held('mem-1') && held('mem-2') && !st.doc(SET, 'set-1').laserDoneAt); assert.equal(await area('set', 'set-1'), 'progress');
    assert(seals('mem-1').some(x => /laserDone/.test(x)), 'the cut seal of every sheet stays');
    p = await LF.plan({ kind: 'set', id: 'set-1', to: { area: 'laser' } }); assert(p.ok); r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true); assert(!held('mem-1') && !held('mem-2')); assert.equal(await area('set', 'set-1'), 'laser');
    // a sheet of a set completes on its own and stays with its set
    p = await LF.plan({ kind: 'sheet', id: 'mem-1', to: { area: 'completed' } }); assert(p.notes.some(x => /stays with Set 1/.test(x)));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true); assert(!st.doc(SET, 'set-1').laserDoneAt, 'its set is not complete yet');

    // ── D. Rose Gold: never a green line without the press ──
    assert.equal(await area('sheet', 'rose'), 'progress');
    p = await LF.plan({ kind: 'sheet', id: 'rose', to: { area: 'laser' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.confirm.map(c => c.key), ['roseLine'], 'the fallback confirm without the Rose Gold module'); assert(/green dash line/i.test(p.confirm[0].label));
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, false); assert(/yes/i.test(r.error)); assert.equal(st.doc(S, 'rose').rosePlanHash, undefined);
    r = await LF.commit(p, { by: 'Paul', confirmed: ['roseLine'] }); assert.equal(r.ok, false, 'confirmed, but nothing can calculate it'); assert(/cannot be calculated/.test(r.error)); assert.equal(st.doc(S, 'rose').rosePlanHash, undefined); assert(!held('rose'));
    let roseSheet = { sheetId: 'rose', label: 'RG Sheet 1', needsLine: true, source: 'live', blocked: '', why: 'No green dash line yet' }, roseRefuse = '';
    LF.configure({ rose: () => ({
      check: async item => { roseCalls.push('check:' + item.id); return { needsLine: true, sheets: [roseSheet], confirm: { key: 'roseLine', label: 'Add the green dash line to RG Sheet 1?', detail: 'This calculates the cut contour for these charms' } }; },
      calculate: async (item, o) => { roseCalls.push('calculate:' + item.id + ':' + o.by); if (roseRefuse) return { ok: false, lines: 0, error: roseRefuse, sheets: [] }; o.onStep({ key: 'calculating', label: 'Calculating' }); st.put(S, item.id, { rosePlanHash: 'hash-1' }); o.onStep({ key: 'saved', label: 'Saved' }); return { ok: true, lines: 4, sheets: [{ sheetId: 'rose', label: 'RG Sheet 1', lineAdded: true, lines: 4 }] }; }
    }) });
    // a sheet the page does not have open, or still saving, cannot be given a line from a move: a hard need, no yes is asked for
    roseSheet = { ...roseSheet, source: 'record' }; p = await LF.plan({ kind: 'sheet', id: 'rose', to: { area: 'laser' } });
    assert.equal(p.ok, false); assert(p.needs.some(x => x.key === 'roseOpen' && /Cut Sheet/.test(x.detail) && /Nest tab/.test(x.detail) && x.items[0].label === 'RG Sheet 1'), JSON.stringify(p.needs)); assert.deepEqual(p.confirm, []);
    r = await LF.commit(p, { by: 'Paul', confirmed: ['roseLine'] }); assert.equal(r.ok, false); assert(/Cut Sheet/.test(r.error)); assert(!roseCalls.some(c => /calculate/.test(c)));
    roseSheet = { ...roseSheet, source: 'live', blocked: 'Its layout is still being nested or saved' }; p = await LF.plan({ kind: 'sheet', id: 'rose', to: { area: 'laser' } });
    assert(p.needs.some(x => x.key === 'roseBusy') && !p.confirm.length, JSON.stringify(p)); roseSheet = { ...roseSheet, blocked: '' }; roseCalls.length = 0;
    // calculate itself refuses (cloud off, layout saving, not open): nothing was written, the hold/release it took is put back
    p = await LF.plan({ kind: 'sheet', id: 'rose', to: { area: 'laser' } }); roseRefuse = 'RG Sheet 1 is not open on this page, so its green dash line cannot be worked out here. Open it on the Nest tab and press Cut Sheet there';
    r = await LF.commit(p, { by: 'Paul', confirmed: ['roseLine'] }); assert.equal(r.ok, false); assert(/Nest tab/.test(r.error)); assert.equal(st.doc(S, 'rose').rosePlanHash, undefined); assert.equal(await area('sheet', 'rose'), 'progress'); roseRefuse = ''; roseCalls.length = 0;
    p = await LF.plan({ kind: 'sheet', id: 'rose', to: { area: 'laser' } }); assert.equal(p.confirm[0].label, 'Add the green dash line to RG Sheet 1?'); assert.deepEqual(roseCalls, ['check:rose']);
    r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, false); assert(!roseCalls.some(c => /calculate/.test(c)), 'no calculation without its press');
    const steps = []; r = await LF.commit(p, { by: 'Paul', confirmed: ['roseLine'], onStep: x => steps.push(x.key + ':' + (x.state || '')) });
    assert.equal(r.ok, true, JSON.stringify(r)); assert(r.applied.some(a => a.key === 'roseLine' && /Green dash line added to RG Sheet 1/.test(a.label)), JSON.stringify(r.applied)); assert.equal(roseCalls.filter(c => /calculate/.test(c)).length, 1); assert(roseCalls.includes('calculate:rose:Paul')); assert.equal(st.doc(S, 'rose').rosePlanHash, 'hash-1');
    assert(steps.some(x => /^roseLine:start/.test(x)) && steps.some(x => /^calculating/.test(x)), steps.join());
    assert.equal(await area('sheet', 'rose'), 'laser');
    // moving it elsewhere (back, completed) adds no line either: from Laser cutting there is nothing to calculate
    roseCalls.length = 0; p = await LF.plan({ kind: 'sheet', id: 'rose', to: { area: 'completed' } }); assert.deepEqual(p.confirm, []); assert(!roseCalls.length);
    LF.configure({ rose: () => null });
    // a Rose Gold sheet gets its QR label with Cut Sheet: a move does not make it, and says where it is made
    mk('rose-nl', { metal: 'rose', roseStockId: 'stock-1', noLabel: true }); labels.length = 0;
    p = await LF.plan({ kind: 'sheet', id: 'rose-nl', to: { area: 'laser' } }); assert(p.needs.some(x => x.key === 'qr' && /Cut Sheet/.test(x.detail)), JSON.stringify(p.needs)); assert(!labels.length);

    // ── E. a failed write changes nothing ──
    mk('boom', {}); let q = await LF.plan({ kind: 'sheet', id: 'boom', to: { area: 'progress' } }); assert(q.ok);
    LF.configure({ mark: async () => { throw new Error('the cloud said no'); } });
    await LF.commit(await LF.plan({ kind: 'sheet', id: 'boom', to: { area: 'completed' } }), { by: 'Paul' }).then(x => assert.equal(x.ok, false));
    LF.configure({ mark: null }); Object.assign(LF.hooks, { mark: null }); delete require.cache; // (the default path calls the server)
    // completed → In progress whose reopen fails: the hold it took first is lifted again
    await LF.commit(await LF.plan({ kind: 'sheet', id: 'boom', to: { area: 'completed' } }), { by: 'Paul' }).then(x => { /* the server path works */ });
    LF.configure({ mark: async (kind, id, done) => { if (!done) throw new Error('reopen refused'); return post({ op: 'laserDone', kind, id, done, by: 'Paul', stage: 'laser' }); } });
    const wasDone = st.doc(S, 'boom').laserDoneAt; assert(wasDone > 0);
    r = await LF.commit(await LF.plan({ kind: 'sheet', id: 'boom', to: { area: 'progress' } }), { by: 'Paul' });
    assert.equal(r.ok, false); assert(/reopen refused/.test(r.error)); assert(!held('boom'), 'its hold was lifted again'); assert.equal(st.doc(S, 'boom').laserDoneAt, wasDone, 'still completed, as it was');
    LF.configure({ mark: null }); LF.hooks.mark = null;
    // a stale plan is refused by the cloud: the sheet was held meanwhile
    mk('race', {}); const stale = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'hold', sheetIds: ['race'] }], expect: { race: { held: true } } });
    assert.equal(stale.status, 409); assert(!held('race')); const hold1 = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'hold', sheetIds: ['race'], note: 'x' }], expect: { race: { held: false } } });
    assert.equal(hold1.status, 200); assert.equal((await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'hold', sheetIds: ['race'] }] })).applied.length, 0, 'asked twice, held once'); assert.equal(st.doc(S, 'race').flowHistory.length, 1);
    assert.equal((await post({ op: 'flowApply', steps: [{ type: 'hold', sheetIds: ['race'] }] })).status, 400, 'a change needs a name');
    assert.equal((await post({ op: 'flowApply', by: 'P', steps: [{ type: 'hold', sheetIds: ['nope-nope'] }] })).status, 404);
    st.put(S, 'race', { archived: true }); assert.equal((await post({ op: 'flowApply', by: 'P', steps: [{ type: 'release', sheetIds: ['race'] }] })).status, 409);
    // the whole cloud failing: nothing is applied
    mk('down', {}); const snap = JSON.stringify(st.doc(S, 'down')); st.fail.charmNestLibrary = true;
    r = await LF.commit({ move: { kind: 'sheet', id: 'down', to: { area: 'progress' } } }, { by: 'Paul' }); st.fail.charmNestLibrary = false;
    assert.equal(r.ok, false); assert.equal(JSON.stringify(st.doc(S, 'down')), snap);
    // a page save cannot clear a hold or the history
    await post({ op: 'putSheet', sheet: { id: 'solo', laserHold: null, flowHistory: [] } }); assert(st.doc(S, 'solo').flowHistory.length >= 2);
    await LF.commit(await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'progress' } }), { by: 'Paul' }); await post({ op: 'putSheet', sheet: { id: 'solo', laserHold: null } }); assert(held('solo'), 'putSheet leaves a hold alone');
    await LF.commit(await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'laser' } }), { by: 'Paul' });

    // ── F. into a set ──
    p = await LF.plan({ kind: 'sheet', id: 'live1', to: { set: 'set-3' } }); assert.equal(p.ok, true, JSON.stringify(p.needs));
    assert(p.auto.some(a => a.key === 'membership' && /Set 3/.test(a.label)) && p.auto.some(a => /QR label made for Set 3/.test(a.label)));
    assert.equal(joins.length, 0, 'a plan joins nothing'); r = await LF.commit(p, { by: 'Paul' }); assert.equal(r.ok, true, JSON.stringify(r)); assert.deepEqual(joins, [{ id: 'live1', setId: 'set-3', newSet: false, split: null }]);
    p = await LF.plan({ kind: 'sheet', id: 'solo', to: { set: 'set-2' } }); assert.equal(p.ok, false); assert(p.needs.some(x => x.key === 'setCommitted'), JSON.stringify(p.needs));
    p = await LF.plan({ kind: 'sheet', id: 'solo', to: { set: 'set-1' } }); assert.equal(p.ok, false); assert(p.needs.some(x => x.key === 'fixedSet'), JSON.stringify(p.needs)); { const fx = p.needs.find(x => x.key === 'fixedSet'); assert(fx.label.split(' ').length <= 7 && /finished/.test(fx.label + ' ' + fx.detail) && !/next save|open run builds/.test(fx.detail) && fx.detail.split(/[.!?]\s/).length === 1, 'plain words, one short sentence: ' + fx.label + ' / ' + fx.detail); }
    p = await LF.plan({ kind: 'sheet', id: 'opn-1', to: { set: 'set-1' } }); assert.equal(p.ok, false); p = await LF.plan({ kind: 'sheet', id: 'live1', to: { newSet: true } }); assert(p.needs.some(x => x.key === 'openSetExists') || p.needs.some(x => x.key === 'notOpenSet') || p.noop || !p.ok);
    p = await LF.plan({ kind: 'set', id: 'set-1', to: { set: 'set-3' } }); assert(p.needs.some(x => x.key === 'wholeSet'));
    p = await LF.plan({ kind: 'sheet', id: 'solo', to: { set: 'set-1' } }); assert.equal(p.noop, undefined);

    // ── G. where it may be dropped ──
    LF.configure({ areaOf: it => it.id === 'solo' ? 'laser' : 'progress', sets: () => [{ setId: 'set-1', seq: 1 }, { setId: 'set-2', seq: 2, status: 'complete', committedAt: 1 }, { setId: 'set-3', seq: 3 }], setOf: () => null });
    assert.deepEqual(LF.targets({ kind: 'sheet', id: 'solo' }).map(t => t.area || t.set || 'new'), ['progress', 'completed', 'set-1', 'set-3']);
    assert.deepEqual(LF.targets({ kind: 'set', id: 'set-1' }).map(t => t.area), ['laser', 'completed']);
    const z = LF.explainTargets({ kind: 'sheet', id: 'solo' }); assert(/committed/.test(z.find(x => x.set === 'set-2').reason)); assert(/already/.test(z.find(x => x.area === 'laser').reason));
    assert.equal(LF.targets({ kind: 'sheet', id: 'solo', area: 'completed' }).some(t => t.set), false, 'a completed sheet goes back to Laser cutting first');
    // ── H. Rose Gold into a set (Paul: "dragging and dropping a rose gold sheet between sets"): its own yes, never assumed ──
    const joinsRose = [], roseAsk = { 'rg-new': true, 'rg-line': false, 'rg-full': false }, rr = [];
    const roseMod = (o = {}) => ({
      check: async item => { rr.push('check:' + item.id); const need = !!roseAsk[item.id]; return { needsLine: need, sheets: [{ sheetId: item.id, label: 'RG Sheet 1', needsLine: need, source: o.source || 'live', blocked: o.blocked || '' }], confirm: need ? { key: 'roseLine', label: 'Add the green dash line to RG Sheet 1?', detail: 'This calculates the cut contour for these charms' } : null }; },
      calculate: async (item, x) => { rr.push('calculate:' + item.id + ':' + !!x.recordCut); return { ok: true, lines: 1 }; }
    });
    LF.configure({ rose: () => roseMod(), roseJoin: async (id, o) => { joinsRose.push({ id, line: o.line, full: o.full, by: o.by }); st.put(S, id, { draft: false, setId: 'set-3', setSeq: 3, ...(o.full ? {} : { roseCutAt: Date.now(), rosePlanHash: 'h-new' }) }); return { ok: true, sheets: o.full ? [] : [{ sheetId: id, label: 'RG Sheet 1', lineAdded: !!o.line, lines: o.line ? 1 : 0, cut: true }], warnings: [] }; } });
    // a sheet with no line yet: both yes keys, the line is added only in the same press
    p = await LF.plan({ kind: 'sheet', id: 'rg-new', to: { set: 'set-3' } });
    assert.equal(p.ok, true, JSON.stringify(p.needs)); assert.deepEqual(p.confirm.map(c => c.key), ['roseSet', 'roseLine']);
    assert.equal(p.confirm[0].label, 'Add RG Sheet 1 to Set 3?'); assert(/Rose Gold joins a set the way Cut Sheet does, with its green dash line/.test(p.confirm[0].detail)); assert(/permanent/.test(p.confirm[0].detail));
    assert(/Cut Sheet press/.test(p.auto.find(a => a.key === 'membership').detail), 'the auto line says it is the Cut Sheet press'); assert.equal(joinsRose.length, 0, 'plan writes nothing'); assert.equal(st.doc(S, 'rg-new').draft, true);
    for (const keys of [[], ['roseSet'], ['roseLine']]) { r = await LF.commit(p, { by: 'Paul', confirmed: keys }); assert.equal(r.ok, false, keys.join()); assert(/yes first/.test(r.error)); }
    assert.equal(joinsRose.length, 0); assert.equal(st.doc(S, 'rg-new').draft, true, 'nothing without both yes keys');
    r = await LF.commit(p, { by: 'Paul', confirmed: ['roseSet', 'roseLine'] }); assert.equal(r.ok, true, JSON.stringify(r));
    assert.deepEqual(joinsRose, [{ id: 'rg-new', line: true, full: false, by: 'Paul' }]); assert(r.applied.some(a => a.key === 'membership' && /RG Sheet 1 added to Set 3/.test(a.label)) && r.applied.some(a => a.key === 'roseLine') && r.applied.some(a => a.key === 'roseCut'), JSON.stringify(r.applied));
    assert.equal(st.doc(S, 'rg-new').draft, false); r = await LF.commit(p, { by: 'Paul', confirmed: ['roseSet', 'roseLine'] }); assert.equal(r.noop, true, 'asked again: already in the set'); assert.equal(joinsRose.length, 1);
    // a sheet that already has its line: only roseSet is asked
    p = await LF.plan({ kind: 'sheet', id: 'rg-line', to: { set: 'set-3' } }); assert.deepEqual(p.confirm.map(c => c.key), ['roseSet']); assert(!p.steps.some(x => x.type === 'roseLine'));
    r = await LF.commit(p, { by: 'Paul', confirmed: ['roseSet'] }); assert.equal(r.ok, true, JSON.stringify(r)); assert.deepEqual(joinsRose[1], { id: 'rg-line', line: false, full: false, by: 'Paul' });
    // a full sheet: it only joins, and no cut is said to be recorded
    p = await LF.plan({ kind: 'sheet', id: 'rg-full', to: { set: 'set-3' } }); assert.deepEqual(p.confirm.map(c => c.key), ['roseSet']); assert(/no cut is recorded/.test(p.confirm[0].detail));
    r = await LF.commit(p, { by: 'Paul', confirmed: ['roseSet'] }); assert.equal(r.ok, true); assert.equal(joinsRose[2].full, true); assert(!r.applied.some(a => a.key === 'roseCut'));
    // the Rose Gold module cannot reach the sheet: a hard need, no yes is asked for
    st.put(S, 'rg-new', { draft: true, setId: null, roseCutAt: null, rosePlanHash: null }); st.doc(S, 'rg-new').roseCutAt = undefined; st.doc(S, 'rg-new').rosePlanHash = undefined; joinsRose.length = 0;
    LF.configure({ rose: () => roseMod({ source: 'record' }) }); p = await LF.plan({ kind: 'sheet', id: 'rg-new', to: { set: 'set-3' } });
    assert.equal(p.ok, false); assert(p.needs.some(x => x.key === 'roseOpen' && /Cut Sheet/.test(x.detail)), JSON.stringify(p.needs)); assert.deepEqual(p.confirm, []);
    LF.configure({ rose: () => roseMod({ blocked: 'Its layout is still being nested or saved' }) }); p = await LF.plan({ kind: 'sheet', id: 'rg-new', to: { set: 'set-3' } }); assert(p.needs.some(x => x.key === 'roseBusy'));
    // without the page's roseJoin the join says where it can be done; without the Rose Gold module a line cannot be asked for from here
    LF.configure({ rose: () => roseMod(), roseJoin: null }); LF.hooks.roseJoin = null; p = await LF.plan({ kind: 'sheet', id: 'rg-new', to: { set: 'set-3' } });
    assert(p.needs.some(x => x.key === 'join' && /Nest tab/.test(x.detail)), JSON.stringify(p.needs));
    const calc = []; LF.configure({ rose: () => null, roseJoin: async () => { calc.push('join'); return { ok: true, sheets: [] }; } }); p = await LF.plan({ kind: 'sheet', id: 'rg-new', to: { set: 'set-3' } });
    assert.deepEqual(p.confirm.map(c => c.key), ['roseSet', 'roseLine'], 'the fallback words stand when the module is absent');
    r = await LF.commit(p, { by: 'Paul', confirmed: ['roseSet', 'roseLine'] }); assert.equal(r.ok, false); assert(/cannot be calculated/.test(r.error)); assert(!calc.length && st.doc(S, 'rg-new').draft === true, 'checked before anything is done');
    // the page's own hook: the Cut Sheet press, the yes-gated order of things (fakes for the page's parts)
    const parts = { log: [], sh: { sheetId: 'rg-x', page: 2, metal: 'rose', draft: true, setId: null } };
    const page = { RoseStock: { record: async (sh, x) => { parts.log.push('record:' + x.by); sh.draft = false; sh.setId = 'set-3'; sh.roseCutAt = 1; }, render() {} }, Gate: { changeMembership: async () => { parts.log.push('include'); parts.sh.draft = false; parts.sh.setId = 'set-3'; }, policy: () => ({ reason: 'x' }) },
      CN: { allSheets: () => [parts.sh] }, LibraryFlowRose: { calculate: async (item, x) => { parts.log.push('calculate:' + x.recordCut + ':' + x.by); parts.sh.draft = false; parts.sh.setId = 'set-3'; parts.sh.roseCutAt = 1; return { ok: true, sheets: [{ sheetId: 'rg-x', label: 'RG Sheet 2', lineAdded: true, lines: 1, cut: true }] }; } } };
    const hook = LF.core.makeRoseJoin(() => page);
    let h = await hook('rg-x', { by: 'Paul', line: true }); assert.equal(h.ok, true); assert.deepEqual(parts.log, ['calculate:true:Paul'], 'a line goes through calculate with recordCut: the one cut a move records');
    parts.log.length = 0; Object.assign(parts.sh, { draft: true, setId: null, roseCutAt: 0 }); h = await hook('rg-x', { by: 'Paul', line: false }); assert.deepEqual(parts.log, ['record:Paul'], 'a sheet with its line is pressed as its Cut Sheet button does');
    parts.log.length = 0; Object.assign(parts.sh, { draft: true, setId: null, roseCutAt: 0 }); h = await hook('rg-x', { by: 'Paul', full: true }); assert.deepEqual(parts.log, ['include'], 'a full sheet only joins'); assert.equal(h.sheets[0].cut, false);
    parts.log.length = 0; Object.assign(parts.sh, { draft: true, setId: null, roseCutAt: 0 }); page.LibraryFlowRose.calculate = async () => ({ ok: false, error: 'RG Sheet 2 is not open on this page' }); h = await hook('rg-x', { by: 'Paul', line: true }); assert.equal(h.ok, false); assert(!parts.log.length && parts.sh.draft === true, 'refused before anything was written');
    page.RoseStock.record = async () => { throw new Error('Not cut: Nest and verify first'); }; h = await hook('rg-x', { by: 'Paul', line: false }); assert.equal(h.ok, false); assert(/Not cut/.test(h.error)); assert.equal(parts.sh._roseAction, false, 'the busy flag is let go');
    h = await LF.core.makeRoseJoin(() => ({ CN: { allSheets: () => [] } }))('nope', { by: 'P' }); assert.equal(h.ok, false); assert(/Cut Sheet/.test(h.error));
    LF.configure({ rose: () => null, roseJoin: null }); LF.hooks.roseJoin = null;

    // ── I. a sheet out of a committed or finished set: the exact reason, and the way through ──
    p = await LF.plan({ kind: 'sheet', id: 'cmt-2', to: { set: 'set-3' } }); assert.equal(p.ok, false);
    const lc = p.needs.find(x => x.key === 'leaveCommitted'); assert(lc && /Set 4 is committed to the station/.test(lc.label) && lc.label.split(' ').length <= 7 && /Undo set/.test(lc.detail) && /undo that commit/.test(lc.detail), JSON.stringify(p.needs)); assert.deepEqual(p.confirm, []);
    r = await LF.commit(p, { by: 'Paul', confirmed: ['leaveSet', 'roseSet'] }); assert.equal(r.ok, false, 'no yes key lifts a block'); assert(/committed to the station/.test(r.error) && /Undo set/.test(r.error));
    p = await LF.plan({ kind: 'sheet', id: 'cmt-1', to: { set: 'set-3' } }); assert(p.needs.some(x => x.key === 'leaveCommitted'), 'a sheet of a committed set of a finished run: the same plain reason');
    p = await LF.plan({ kind: 'sheet', id: 'rg-cut', to: { set: 'set-3' } }); assert(p.needs.some(x => x.key === 'sheetCut' && /permanent/.test(x.detail) && /stays in the set it was cut in/.test(x.detail)), JSON.stringify(p.needs));
    st.put(S, 'solo', { laserDoneAt: now - 100 }); p = await LF.plan({ kind: 'sheet', id: 'solo', to: { set: 'set-3' } }); assert(p.needs.some(x => x.key === 'sheetCompleted' && /cut record/.test(x.detail)), JSON.stringify(p.needs)); st.doc(S, 'solo').laserDoneAt = undefined;

    console.log('PASS: library flow: plan/commit forwards and backwards, holds keep every seal, a sheet into a set, Rose Gold only on its press, hard blockers, idempotent repeat, failed writes change nothing');
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exitCode = 1; });
