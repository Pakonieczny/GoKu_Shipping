// AUDIT (R15 · a set advances as ONE, one Approve per set; R1 / R10 behind it): the page's gate (CharmNestReadiness.setGate, with the set rules ON
// as in the app) against the server's refusal (charmNestLibrary approveGate, reached through flowApply / flowState, the real handler over the
// in-memory shop of tests/charm-nest/bridge-server.cjs). Required: whatever the page keeps grey, the server refuses; the page is never green
// where the server refuses. No live endpoint, no paid call.
//   node tests/charm-nest/audit-number-approve-gate.cjs
'use strict';
const assert = require('node:assert/strict');
const R = require('../../charm-nest-readiness.js');
const SR = require('../../charm-nest-set-rules.js'); SR.enforce(true);
const I = require('../../charm-nest-set-invariants.js');
(async () => {
  const { start } = require('./bridge-server.cjs');
  const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs';
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-20';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const lines = {}; let n = 0;
  // one ready sheet: nothing to engrave, verified, files and QR label; `order` shares an order with another sheet (a pair split over two sheets)
  const mk = (id, o = {}) => {
    const order = o.order || String(3900000000 + ++n), copy = o.copy || 1, pool = `${order}_${copy}_1`;
    lines[`${order}_${copy}`] = { orderId: order, state: 'written', quantity: 1, poolIds: [pool], engraveCandidate: false };
    const { order: _o, copy: _c, metal, ...rest } = o;
    st.put(S, id, { id, runId: 'run-a', metal: metal || 'gold', day, status: 'complete', placedCount: 1, charmCount: 1, density: .8, releaseFull: true, stock: { wIn: 6, hIn: 4.5 }, poolIds: [pool], orders: [order], verification: { ok: true },
      outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } },
      label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders: [order] }], orders: [order] }, sheetIndex: 1, updatedAt: ts, createdAt: ts, ...rest });
    return order;
  };
  const mkSet = (setId, sheetIds, extra = {}) => st.put(SET, setId, { setId, seq: +setId.replace(/\D/g, '') || 1, day, runId: 'run-a', sheetIds, materials: [], orders: {}, status: 'nesting', updatedAt: ts, createdAt: ts, ...extra });
  // the records as the page reads them (the Library's entries carry each sheet's engraving report: nothing to engrave here)
  const recs = ids => ids.map(id => { const d = st.doc(S, id); return { ...d, id, engraving: Object.fromEntries((d.poolIds || []).map(p => [p, { needed: false, state: 'none', approved: true }])) }; });
  const LF = require('../../charm-nest-flow.js');
  LF.configure({ api: post, employee: () => 'Paul', rows: () => [], remakeLabel: async () => {}, rose: () => null });
  const fails = [];
  const check = async (name, fn) => { try { await fn(); console.log('ok   ' + name); } catch (e) { fails.push(name); console.log('FAIL ' + name + '\n     ' + (e && e.message)); } };
  try {
    // set-1: a proper set (completed GF + completed SS), every sheet ready -> both say ready (the control)
    mkSet('set-1', ['gf-1', 'ss-1']); mk('gf-1', { setId: 'set-1', setSeq: 1 }); mk('ss-1', { setId: 'set-1', setSeq: 1, metal: 'silver' });
    // set-2: an uncommitted set of ONE completed GF sheet (no SS: R1 says it is not a set) -> the page keeps it grey
    mkSet('set-2', ['gf-2']); mk('gf-2', { setId: 'set-2', setSeq: 2 });
    // set-3: GF + SS, but the GF sheet shares a multi-piece order with an UNCUT Rose Gold sheet outside every set: a partner to add (R10) -> the page keeps it grey
    mkSet('set-3', ['gf-3', 'ss-3']); const shared = mk('gf-3', { setId: 'set-3', setSeq: 3 }); mk('ss-3', { setId: 'set-3', setSeq: 3, metal: 'silver' });
    mk('rg-3', { metal: 'rose', order: shared, copy: 2, draft: true, setId: null, sheetIndex: null, page: 1 });
    st.put(RUN, 'run-a', { runId: 'run-a', status: 'complete', lines });

    await check('control · a right set of two ready sheets: the page and the server both let it be approved', async () => {
      const page = R.setGate(st.doc(SET, 'set-1'), recs(['gf-1', 'ss-1']));
      assert.equal(page.ready, true, page.reason);
      const r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-1' }] });
      assert.equal(r.status, 200, r.error);
    });

    await check('R1/R15 · a set of ONE completed GF sheet: grey on the page, so the server refuses its approval too', async () => {
      const page = R.setGate(st.doc(SET, 'set-2'), recs(['gf-2']));
      assert.equal(page.ready, false, 'the page keeps it grey'); assert(/completed SS/.test(page.reason), page.reason);
      const plan = await LF.plan({ kind: 'set', id: 'set-2', to: { area: 'laser' } });
      const r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'sheet', id: 'gf-2' }] });
      const sealed = (st.doc(S, 'gf-2').processSeals || []).some(x => x.how === 'laserReady');
      const wrong = [];
      if (plan.ok) wrong.push(`the Library plan (LibraryFlow.plan, a drag of Set 2 to Laser cutting) goes ahead with steps ${JSON.stringify(plan.steps.map(x => x.type))}`);
      if (r.status !== 409) wrong.push(`the server's flowApply seal answered ${r.status} (laserReady seal written: ${sealed})`);
      assert.deepEqual(wrong, [], `setGate keeps it grey ("${page.reason}") but: ${wrong.join('; ')}`);
    });

    await check('R10/R15 · a set whose GF sheet shares an order with an uncut Rose Gold sheet outside it: grey on the page (partner to add), so the server refuses too', async () => {
      const sheets = recs(['gf-3', 'ss-3', 'rg-3']).map(x => ({ ...x, pieces: (x.poolIds || []).map(p => ({ poolId: p, order: p.split('_')[0] })) }));
      const res = I.check([{ id: 'set-3', name: 'Set 3', seq: 3, sheetIds: ['gf-3', 'ss-3'] }], sheets, { skipExempt: true, partialPartners: 'pending' });
      const row = res.sets.find(s => s.id === 'set-3');
      const page = R.setGate({ ...st.doc(SET, 'set-3'), cardinalWait: [{ id: 'rg-3', label: 'RG Draft 1' }] }, recs(['gf-3', 'ss-3']));   // (what the Library's card derives, partnersOf)
      assert.equal(page.ready, false, 'the page keeps it grey');
      const plan = await LF.plan({ kind: 'set', id: 'set-3', to: { area: 'laser' } });
      const r = await post({ op: 'flowApply', by: 'Paul', steps: [{ type: 'seal', kind: 'set', id: 'set-3' }] });
      const wrong = [];
      if (plan.ok) wrong.push(`the Library plan (LibraryFlow.plan, a drag of Set 3 to Laser cutting) goes ahead (it reads no cardinalWait)`);
      if (r.status !== 409) wrong.push(`the server's flowApply seal answered ${r.status}`);
      assert.deepEqual(wrong, [], `the card keeps it grey ("${page.reason}"; checker canApprove=${row && row.canApprove}, partners ${JSON.stringify(res.partners.map(p => p.sheet || p.sheets || p))}) but: ${wrong.join('; ')}`);
    });
  } finally { srv.close(); LF.configure({ api: null, remakeLabel: null, rose: () => null }); }
  if (fails.length) { console.log(`FAIL: audit-number-approve-gate: ${fails.length} case(s): ${fails.join(' | ')}`); process.exit(1); }
  console.log('PASS: audit-number-approve-gate');
})().catch(e => { console.error(e); process.exit(1); });
