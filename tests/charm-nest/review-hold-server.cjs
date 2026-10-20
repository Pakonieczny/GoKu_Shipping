// An order in Review holds its sheet at the SERVER's write gates too (real charmNestLibrary handler over the in-memory shop of bridge-server.cjs; nothing live):
// the cut (mark complete) of a ready sheet is refused while one of its orders is open in Review, nothing is written, and it goes through by itself once the review is done.
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const R = require('../../charm-nest-readiness.js');
const LF = require('../../charm-nest-flow.js');
const H = require('../../charm-nest-review-hold.js');
const S = 'Charm_Nest_Sheets', RUN = 'Charm_Nest_Runs', CUSTOM = 'Charm_Custom_Orders';

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const order = '3800000777', pool = order + '_1_1', lines = {};
  lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: [pool], engrave: { needed: false, state: 'none', approved: true } };
  st.put(S, 'solo', { id: 'solo', runId: 'run-x', metal: 'gold', day, status: 'complete', placedCount: 1, charmCount: 1, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: [pool], orders: [order], verification: { ok: true },
    outputs: { ai: { path: 'solo.ai', url: srv.sorterOrigin + '/solo.ai' }, preview: { path: 'solo.png', url: image } }, label: { files: [{ path: 'solo-qr.png', url: image, payload: order, orders: [order] }], orders: [order] }, sheetIndex: 1, updatedAt: ts, createdAt: ts });
  st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines });
  LF.configure({ api: post, employee: () => 'Name One', rows: () => [] });

  // ── baseline: ready, the plan to Completed is ok ──
  let p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'completed' } }); assert(p.ok, JSON.stringify(p.needs));

  // ── the order is sent to Review (the real op writes the real record) ──
  const put = await post({ op: 'reviewMarkPut', action: 'mark', rid: order, by: 'Name One' }); assert.equal(put.status, 200, JSON.stringify(put));
  assert(st.doc(CUSTOM, order + '_review'), 'the review record exists'); assert.equal(st.doc(CUSTOM, order + '_review').review.state, 'open');

  // a page WITHOUT the hold (an older page, a second computer): the list read does not know, the plan is ok, but the server's WRITE gate refuses and writes nothing
  p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'completed' } });
  const r = await LF.commit(p, { by: 'Name One' });
  assert.equal(r.ok, false, 'the server refuses the cut: ' + JSON.stringify(r).slice(0, 300)); assert.equal(st.doc(S, 'solo').laserDoneAt, undefined, 'nothing written');
  assert.match(JSON.stringify(r), /In Review: Order held by Name One/, 'the refusal names the review');

  // a page WITH the hold: the plan itself refuses, in words
  globalThis.CharmNestReviewHold = H;
  globalThis.CharmNestReviewMark = { isMarked: x => x === order && st.doc(CUSTOM, order + '_review').review.state === 'open', markedRids: () => st.doc(CUSTOM, order + '_review').review.state === 'open' ? [order] : [], reasonOf: () => 'Order held by Name One', onChange: () => () => {} };
  globalThis.OrderPieces = { of: () => [], subscribe() {}, load: () => Promise.resolve() };
  p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'completed' } });
  assert.equal(p.ok, false); const need = p.needs.find(n => n.key === 'orders'); assert.ok(need, JSON.stringify(p.needs.map(n => n.key))); assert.match(need.label, /in Review/); assert.ok(need.items.some(i => i.id === order));

  // ── resolved: the same press goes through by itself ──
  const res = await post({ op: 'reviewMarkPut', action: 'resolve', rid: order, by: 'Name One', how: 'ok' }); assert.equal(res.status, 200); assert.equal(st.doc(CUSTOM, order + '_review').review.state, 'done');
  p = await LF.plan({ kind: 'sheet', id: 'solo', to: { area: 'completed' } }); assert(p.ok, JSON.stringify(p.needs));
  const r2 = await LF.commit(p, { by: 'Name One' }); assert.equal(r2.ok, true, JSON.stringify(r2).slice(0, 300)); assert(+st.doc(S, 'solo').laserDoneAt > 0);
  await srv.close();
  console.log('review-hold-server: ok');
})().catch(e => { console.error(e); process.exit(1); });
