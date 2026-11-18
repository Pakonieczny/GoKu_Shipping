// SETNOOP (Paul, 18 Nov 2026: "We can't have an unintentional rewrite"): a computer saved every set of the run about every 19 s with the very record the cloud already held (only updatedAt moved). Each save
// moved the set's updatedAt and the change counters (and the placement counter every computer reads), so every computer read again and redrew.
//   1  a first save makes the set; the same save again writes nothing: the record's updatedAt and every change counter are as they were
//   2  a save that changes anything (the name, the sheets, their order, an order's held word) still writes, moves the counters and reaches the record
//   3  a field a save leaves out stays as it was (a save is a patch), and the same patch again is still a no-op
// Real handler over the repo's fake site (bridge-server.cjs). Nothing live.   node tests/charm-nest/set-update-noop.cjs
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs');
(async () => {
  const srv = await start({ receipts: [] }); const { st } = srv;
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, ...(await r.json()) }; };
  const SET = 'Charm_Nest_Sets', ID = 'set-2026-11-18-1';
  const patch = (o = {}) => Object.assign({ runId: 'run-x', day: '2026-11-18', seq: 1, name: 'Set-1', folder: 'charmnest/sets/2026-11-18/Set-1', materials: ['gold', 'silver'], sheetIds: ['gold-1', 'silver-1'], orders: { 111: { held: null, lines: [{ transactionId: 't1', copies: [] }] } }, status: 'open', labels: null }, o);
  const feed = async () => (await call({ op: 'changes', since: '' })).n;
  let fails = 0; const ok = (c, m) => { if (!c) fails++; console.log((c ? '  ✓ ' : '  ✗ ') + m); };
  try {
    let r = await call({ op: 'setUpdate', setId: ID, patch: patch() }); assert.equal(r.status, 200);
    const t1 = st.doc(SET, ID); ok(!!t1 && t1.name === 'Set-1', '1a a first save makes the set');
    const n1 = await feed(), u1 = JSON.stringify(st.doc(SET, ID).updatedAt);
    r = await call({ op: 'setUpdate', setId: ID, patch: patch() }); const n2 = await feed();
    ok(r.status === 200 && r.same === true, '1b the same save again answers "same"');
    ok(JSON.stringify(st.doc(SET, ID).updatedAt) === u1, '1c and writes nothing: updatedAt is as it was');
    ok(n1 === n2, '1d and moves no change counter: ' + n1 + ' / ' + n2);
    await call({ op: 'setUpdate', setId: ID, patch: patch() }); await call({ op: 'setUpdate', setId: ID, patch: patch() });
    ok((await feed()) === n1, '1e ten of them in a row (here three) move nothing either');

    for (const [what, p] of [['the name', { name: 'Set-1b' }], ['the sheets', { sheetIds: ['gold-1', 'silver-1', 'gold-2'] }], ['their order', { sheetIds: ['silver-1', 'gold-1'] }], ['an order\'s held word', { orders: { 111: { held: 'on hold', lines: [{ transactionId: 't1', copies: [] }] } } }]]) {
      const before = await feed(); r = await call({ op: 'setUpdate', setId: ID, patch: patch(p) }); const after = await feed();
      const doc = st.doc(SET, ID); const field = Object.keys(p)[0];
      ok(r.status === 200 && r.same !== true && after !== before && JSON.stringify(doc[field]) === JSON.stringify(p[field]), '2 a save that changes ' + what + ' is written and moves the counters');
      await call({ op: 'setUpdate', setId: ID, patch: patch() });   // (back to the base record for the next case)
    }
    // 3 · a patch that leaves a field out
    await call({ op: 'setUpdate', setId: ID, patch: patch() });
    const part = { name: 'Set-1' }; const nb = await feed(); r = await call({ op: 'setUpdate', setId: ID, patch: part }); const na = await feed();
    ok(r.same === true && na === nb && st.doc(SET, ID).sheetIds.length === 2, '3 a partial patch that names only what is already so is a no-op, and the fields it leaves out stay');
  } finally { srv.close && srv.close(); }
  if (fails) { console.log('set-update-noop: ' + fails + ' failed'); process.exit(1); }
  console.log('set-update-noop: ok');
})().catch(e => { console.error(e); process.exit(1); });
