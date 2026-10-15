// ADV-REVIEW (Paul, 15 Oct 2026): the server's Approve gate (charmNestLibrary flowApply -> approveGate) holds a sheet whose order is in Review, and its whole set with it, whatever the page says.
// review-hold-server.cjs pins the cut (mark complete); nothing pinned Approve for laser cutting (the release and the laser seal, sheet or set). Real handler over the in-memory shop of
// bridge-server.cjs; nothing live; every name is made up.
//   an older page / a second bench presses Approve (release, laser seal of the sheet, laser seal of the set) -> 409 naming the order, NOTHING written
//   a sheet of another set, with no order in Review, is not stopped by it (the control)
//   resolved in Review -> the same press goes through
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
const S = 'Charm_Nest_Sheets', RUN = 'Charm_Nest_Runs', SETS = 'Charm_Nest_Sets';

(async () => {
  const srv = await start({ receipts: [] }), { st } = srv, now = Date.now(), ts = { toMillis: () => now }, day = '2026-10-03';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const image = 'data:image/svg+xml,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="30" height="20"><rect width="30" height="20"/></svg>');
  const held = '3800000777', mate = '3800000778', other = '3800000779', lines = {};
  const sheet = (id, order, set, n) => {
    const pool = order + '_1_1'; lines[order + '_1'] = { orderId: order, state: 'written', quantity: 1, poolIds: [pool], engrave: { needed: false, state: 'none', approved: true } };
    st.put(S, id, { id, runId: 'run-x', metal: 'gold', day, status: 'complete', placedCount: 1, charmCount: 1, density: .7, stock: { wIn: 6, hIn: 4.5 }, poolIds: [pool], orders: [order], verification: { ok: true }, setId: set, setSeq: set === 'set-x' ? 1 : 2, sheetIndex: n,
      outputs: { ai: { path: id + '.ai', url: srv.sorterOrigin + '/' + id + '.ai' }, preview: { path: id + '.png', url: image } }, label: { files: [{ path: id + '-qr.png', url: image, payload: order, orders: [order] }], orders: [order] }, updatedAt: ts, createdAt: ts });
  };
  sheet('held', held, 'set-x', 1); sheet('mate', mate, 'set-x', 2); sheet('apart', other, 'set-y', 1);
  st.put(SETS, 'set-x', { setId: 'set-x', seq: 1, day, runId: 'run-x', sheetIds: ['held', 'mate'], materials: ['gold'], orders: {}, status: 'labelled', updatedAt: ts, createdAt: ts });
  st.put(SETS, 'set-y', { setId: 'set-y', seq: 2, day, runId: 'run-x', sheetIds: ['apart'], materials: ['gold'], orders: {}, status: 'labelled', updatedAt: ts, createdAt: ts });
  st.put(RUN, 'run-x', { runId: 'run-x', status: 'complete', lines });
  const write = (id) => { const d = st.doc(S, id); return JSON.stringify([d.processSeals || null, d.laserHold || null, d.processReady || null]); };
  const before = ['held', 'mate'].map(write);

  const put = await post({ op: 'reviewMarkPut', action: 'mark', rid: held, by: 'Name One' }); assert.equal(put.status, 200, JSON.stringify(put));

  // the order sits on 'held'; its set-mate 'mate' is stopped with it (a set advances as one), by each way of approving
  for (const [what, steps] of [['release', [{ type: 'release', sheetIds: ['mate'] }]], ['laser seal of the sheet', [{ type: 'seal', kind: 'sheet', id: 'mate' }]], ['laser seal of the set', [{ type: 'seal', kind: 'set', id: 'set-x' }]], ['laser seal of the sheet it is on', [{ type: 'seal', kind: 'sheet', id: 'held' }]]]) {
    const r = await post({ op: 'flowApply', steps, by: 'Name One' });
    assert.equal(r.status, 409, `${what}: refused: ` + JSON.stringify(r).slice(0, 200));
    assert.match(r.error, new RegExp(`Order ${held} is in Review`), `${what}: names the order`);
    assert.equal(r.orderId, held);
  }
  assert.deepEqual(['held', 'mate'].map(write), before, 'nothing was written by any refused press (no seal, no hold, no readiness)');

  // the control: a sheet of another set whose orders are not in Review goes through
  const far = await post({ op: 'flowApply', steps: [{ type: 'seal', kind: 'sheet', id: 'apart' }], by: 'Name One' });
  assert.equal(far.status, 200, 'another set is not stopped: ' + JSON.stringify(far).slice(0, 200));

  // resolved: the same press goes through, set and all
  const res = await post({ op: 'reviewMarkPut', action: 'resolve', rid: held, by: 'Name One', how: 'ok' }); assert.equal(res.status, 200);
  const ok = await post({ op: 'flowApply', steps: [{ type: 'seal', kind: 'set', id: 'set-x' }], by: 'Name One' });
  assert.equal(ok.status, 200, 'once resolved the press goes through: ' + JSON.stringify(ok).slice(0, 200));
  await srv.close();
  console.log('adv-review-gate: ok');
})().catch(e => { console.error(e); process.exit(1); });
