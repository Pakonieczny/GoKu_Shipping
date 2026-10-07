/* The Library's live read (laserStatus) reads a sheet for the fields its entry and its readiness are made of, not the whole record:
   a sheet record is mostly its charms (each with two download links) and its placements, and none of that is in the answer.
   The real handler over the in-memory Firestore with field masks honoured (st.strictMasks) against the same shop with them
   ignored: the answers are identical, the bytes read are a fraction. Offline, no production record. */
const assert = require('node:assert/strict');
const { start } = require('./bridge-server.cjs'), { seed } = require('./laser-workflow.cjs');
const S = 'Charm_Nest_Sheets';
const url = (p, t) => 'https://firebasestorage.googleapis.com/v0/b/gokudatabase.firebasestorage.app/o/' + encodeURIComponent(p) + '?alt=media&token=' + t;
const fat = (id, n) => {
  const charms = Array.from({ length: n }, (_, i) => ({ id: id + ':' + i, hash: 'h' + i.toString(16).padStart(12, '0'), name: 'Charm design number ' + i, sourceId: 'pool:SKU' + i, sourceName: 'SKU-' + i + ' (master)', index: i, qty: 1, order: String(3700000000 + i), layer: null, poolId: null, custom: false, sku: 'SKU-' + i, areaPt2: 123.4, widthPt: 21.33, heightPt: 18.5, thumbUrl: url('charmnest/master/SKU-' + i + '.png', '2f9d1c7e-5a3b-4c1d-9e8f-0123456789ab'), aiUrl: url('charmnest/master/SKU-' + i + '.ai', '7a1c3e5f-9b2d-4f6a-8c0e-ba9876543210'), pngPath: 'charmnest/master/SKU-' + i + '.png', aiPath: 'charmnest/master/SKU-' + i + '.ai', excluded: false, open: false }));
  const placements = charms.map((c, i) => ({ id: c.id, angle: 90, cxPt: 10.123456 + i, cyPt: 20.123456 + i, xPt: 5.123456, yPt: 6.123456, wPt: 21.33, hPt: 18.5, n: i + 1, name: c.order + ' · ' + c.name, layer: null }));
  return { charms, placements, rejects: [], params: { engine: 'solver', budgetS: 12, gap: 1.2, rotations: 36 }, trials: 120, elapsedMs: 11000, totalMs: 12000, seq: 1, restored: false };
};
(async () => {
  const srv = await start({ receipts: [] }); seed(srv); const { st } = srv;
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, ...await r.json() }; };
  try {
    for (const id of ['cut-sheet', 'ready-sheet', 'pending-sheet']) st.put(S, id, fat(id, 100));
    st.put(S, 'seq-only-sheet', { id: 'seq-only-sheet', metal: 'gold', day: '2026-09-30', status: 'complete', placedCount: 1, charmCount: 1, poolIds: ['3700000999_1_1'], orders: ['3700000999'], seq: 7, setSeq: null, ...fat('seq-only-sheet', 100) });
    // a record from before placedCount was kept: readiness counts its placements, so these are still read
    st.put(S, 'old-sheet', { id: 'old-sheet', metal: 'gold', day: '2026-09-30', status: 'complete', poolIds: [], orders: [], ...fat('old-sheet', 30), placedCount: undefined });
    const ids = ['cut-sheet', 'ready-sheet', 'pending-sheet', 'seq-only-sheet', 'old-sheet'];
    const ask = extra => call({ op: 'laserStatus', sheetIds: ids, setIds: ['set-fixture'], ...extra });
    const strip = a => JSON.parse(JSON.stringify(a, (k, v) => (k === 'checkedAt' ? undefined : v)));
    st.strictMasks = false; const whole = strip(await ask({ wantRevs: true }));
    st.strictMasks = true; st.readBytes = 0; const masked = strip(await ask({ wantRevs: true })); const maskedBytes = st.readBytes;
    assert.deepEqual(masked, whole, 'the answer is the same with the sheets read by their fields as with them read whole');
    assert(masked.sheets.length >= 5 && masked.sheets.some(s => s.id === 'seq-only-sheet') && masked.sheets.some(s => s.id === 'old-sheet'));
    // the bytes: a whole read of the five sheets against the masked read of them
    const sizeWhole = ids.reduce((n, id) => n + JSON.stringify(st.doc(S, id)).length, 0);
    console.log(`sheets read whole ${sizeWhole} bytes; read by their fields (answer ${JSON.stringify(masked).length} bytes) ${maskedBytes} bytes in all`);
    assert(maskedBytes < sizeWhole / 3, 'the sheets are read for a fraction of their bytes');
    // the slow, seal-recording check (it writes, so its answers carry their own times) still answers for the same sheets and sets
    st.strictMasks = false; const a = strip(await ask({ recordSeals: true, by: 'Test' })); st.strictMasks = true; const b = strip(await ask({ recordSeals: true, by: 'Test' }));
    assert.deepEqual(b.sheets.map(x => [x.id, x.processReady, x.laser && x.laser.ready]), a.sheets.map(x => [x.id, x.processReady, x.laser && x.laser.ready])); assert.deepEqual(b.sets.map(x => x.setId), a.sets.map(x => x.setId));
    console.log('Laser status mask OK: identical answers, sheets read for their fields only');
  } finally { srv.close && srv.close(); }
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
