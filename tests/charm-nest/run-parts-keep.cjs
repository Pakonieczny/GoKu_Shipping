/* The parts of a run's lines are named by their content, so the text of a part never changes; a function instance keeps what it read of
   one (charmNestLibrary.js keepPart). The Library's full read of a run, made again whenever anything it watches changed and by every
   open page, then reads a part's text once per instance and not once per call. The real handler over the in-memory Firestore with
   field masks honoured (st.strictMasks); offline, no production record. */
const assert = require('node:assert/strict'), crypto = require('node:crypto');
const { start } = require('./bridge-server.cjs');
const S = 'Charm_Nest_Sheets', SET = 'Charm_Nest_Sets', RUN = 'Charm_Nest_Runs', LIVE = 'Charm_Nest_Run_Live', LINES = 'Charm_Nest_Run_Lines';
const line = (o, n) => ({ orderId: String(o), state: 'written', quantity: 1, poolIds: [o + '_1_1'], engraveCandidate: false, sku: 'SKU-' + n, listingId: String(1000000000 + n), title: 'A charm title that is about as long as a real listing title is, number ' + n });
const part = (from, n) => { const lines = {}; for (let i = 0; i < n; i++) lines[(from + i) + '_1'] = line(from + i, i); return JSON.stringify(lines); };
const sha = t => crypto.createHash('sha256').update(t).digest('hex').slice(0, 40);
(async () => {
  const srv = await start({ receipts: [] }); const { st } = srv; st.strictMasks = true;
  const call = async b => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(b) }); return { status: r.status, ...await r.json() }; };
  const strip = a => JSON.parse(JSON.stringify(a, (k, v) => (k === 'checkedAt' ? undefined : v)));
  try {
    const liveText = part(3800000000, 400), archText = part(3700000000, 400), liveId = 'run-keep~' + sha(liveText), archId = 'run-keep~' + sha(archText);
    st.put(LIVE, liveId, { runId: 'run-keep', json: liveText, bytes: liveText.length, lines: 400, seq: 0 });
    st.put(LINES, archId, { runId: 'run-keep', lines: 400, bytes: archText.length, json: archText, keys: Object.keys(JSON.parse(archText)), orders: Object.values(JSON.parse(archText)).map(l => l.orderId), decisions: '{}', decisionsVersion: 'old', at: 1000, seq: 0 });
    st.put(RUN, 'run-keep', { runId: 'run-keep', liveLines: { ids: [liveId] }, lineArchive: true });
    const sheets = ['3800000001', '3700000002', '3800000003'];
    sheets.forEach((o, i) => st.put(S, 'keep-' + i, { id: 'keep-' + i, setId: 'keep-set', setSeq: 1, sheetIndex: i + 1, runId: 'run-keep', metal: 'gold', day: '2026-10-06', status: 'complete', placedCount: 1, charmCount: 1, density: .7, poolIds: [o + '_1_1'], orders: [o], verification: { ok: true } }));
    st.put(SET, 'keep-set', { setId: 'keep-set', seq: 1, day: '2026-10-06', runId: 'run-keep', sheetIds: ['keep-0', 'keep-1', 'keep-2'], materials: ['gold'], orders: {}, status: 'labelled' });
    const ask = () => call({ op: 'laserStatus', sheetIds: ['keep-0', 'keep-1', 'keep-2'], setIds: ['keep-set'] });
    st.readBytes = 0; const first = strip(await ask()), firstBytes = st.readBytes;
    assert.equal(first.status, 200); assert.equal(first.sheets.length, 3);
    st.readBytes = 0; const second = strip(await ask()), secondBytes = st.readBytes;
    assert.deepEqual(second, first, 'the answer is the same when the parts come from the instance');
    console.log(`full read of a run with a live part (${liveText.length} bytes) and an archive part (${archText.length} bytes): first call ${firstBytes} bytes, next ${secondBytes} bytes`);
    assert(firstBytes > liveText.length + archText.length, 'the first call reads the parts');
    assert(secondBytes < firstBytes - (liveText.length + archText.length) * 0.9, 'the next call reads neither part\'s text');
    // a save that replaces the live part names a new one: its lines are what the next read answers with
    const newer = part(3800000000, 401), newerId = 'run-keep~' + sha(newer);
    st.put(LIVE, newerId, { runId: 'run-keep', json: newer, bytes: newer.length, lines: 401, seq: 0 }); st.put(RUN, 'run-keep', { liveLines: { ids: [newerId] } });
    st.put(S, 'keep-2', { orders: ['3800000400'], poolIds: ['3800000400_1_1'] });
    const third = await ask(); assert.equal(third.status, 200);
    assert(JSON.stringify(third).includes('3800000400') || third.sheets.some(x => x.id === 'keep-2'), 'a new part is read, not the one kept under another name');
    console.log('Run parts keep OK: a part is read once per instance, a part of a new name is read when it appears');
  } finally { srv.close && srv.close(); }
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
