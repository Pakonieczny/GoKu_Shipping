/* CEAUTOSAVE (Paul, 15 Nov 2026): the Charm Editor's draft in the cloud — charmNestLibrary.js artDraftPut / artDraftGet / artDraftDrop, the REAL handler over the in-memory Firestore
   of tests/charm-nest/bridge-server.cjs. No live service, no network.
     A refused: bad pool id, nobody named, a draft that is no gzip / no JSON / no faces, over the size cap
     B kept: who and when, baseAt, a second put replaces the first (last writer wins, the other person named), read back; a small draft in the record, a big one as a file (the one before deleted)
     C a draft is not a saved edit: the pool row (artEdit stamp, state) and the art collection are untouched; the ops move no counter
     D drop; the sandbox: its own collection (Sandbox_ prefix), production unseen
   node tests/charm-nest/art-draft-server.cjs */
'use strict';
const assert = require('node:assert/strict'), zlib = require('node:zlib');
const { start } = require('./bridge-server.cjs');
let n = 0; const ok = (c, m) => { assert.ok(c, m); n++; }, eq = (a, b, m) => { assert.deepEqual(a, b, m); n++; };
(async () => {
  const srv = await start({ receipts: [], strictArrays: true }), st = srv.st;
  try {
    const call = async body => { const out = await st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body), queryStringParameters: {} }); return { status: out.statusCode, body: JSON.parse(out.body || '{}') }; };
    const ID = '4170000001_10010_1';
    const row0 = { poolId: ID, orderId: '4170000001', sku: 'CALENDAR', state: 'pooled', sheetId: null, artEdit: { back: 111 } };
    st.put('Charm_Pool', ID, row0);
    const faces = (extra) => ({ v: 1, faces: { front: { face: 'front', unitPt: true, frame: { x0: 0, y0: 0, wPt: 72, hPt: 72 }, items: [{ id: 'a', role: 'art', d: 'M0 0L10 10', fill: [0, 0, 0], pad: extra || '' }] } } });
    const gz = o => zlib.gzipSync(Buffer.from(JSON.stringify(o))).toString('base64');
    const put = (b) => call(Object.assign({ op: 'artDraftPut', poolId: ID, by: 'Ana_M', baseAt: 111, draft: gz(faces()) }, b || {}));

    /* A */
    for (const [b, re, m] of [[{ poolId: 'nope' }, /bad pool id/, 'bad pool id'], [{ by: ' ' }, /signed-in person/, 'nobody named'], [{ draft: 'not base64 gzip' }, /gzip of JSON/, 'not a gzip'],
      [{ draft: Buffer.from('hello').toString('base64') }, /gzip of JSON/, 'not gzip bytes'], [{ draft: zlib.gzipSync('not json').toString('base64') }, /gzip of JSON/, 'not JSON'],
      [{ draft: gz({ v: 1 }) }, /faces/, 'no faces'], [{ draft: gz({ faces: [] }) }, /faces/, 'faces is an array'], [{ draft: '' }, /gzip|at most/, 'empty']]) {
      const r = await put(b); ok(r.status === 400 && re.test(r.body.error || ''), 'A refused: ' + m + ' · ' + JSON.stringify(r.body).slice(0, 120));
    }
    ok(!st.list('Charm_Pool_Art_Draft').length, 'A nothing written by the refused');
    const huge = Buffer.alloc(4300000, 7).toString('base64');   // over the 4.2 MB cap
    let r = await put({ draft: huge }); ok(r.status === 400 && /at most/.test(r.body.error), 'A over the size cap refused: ' + JSON.stringify(r.body).slice(0, 100));

    /* B */
    const gen0 = JSON.stringify(st.list('Charm_Nest_Order_Rev') || []);
    r = await put(); ok(r.status === 200 && r.body.ok && r.body.stored === 'record' && r.body.baseAt === 111, 'B kept in the record');
    let d = st.doc('Charm_Pool_Art_Draft', ID); ok(d && d.by === 'Ana_M' && d.at > 0 && d.baseAt === 111 && typeof d.data === 'string', 'B who, when, baseAt, data');
    ok(!/\[\s*\[/.test(JSON.stringify(d)), 'B no array in an array in the record');
    r = await call({ op: 'artDraftGet', poolId: ID }); ok(r.body.draft && r.body.draft.by === 'Ana_M' && r.body.draft.data === undefined && r.body.draft.baseAt === 111, 'B get: who and when, no data without full');
    r = await call({ op: 'artDraftGet', poolId: ID, full: true }); eq(JSON.parse(zlib.gunzipSync(Buffer.from(r.body.draft.data, 'base64')).toString()), faces(), 'B full get hands the document back');
    r = await put({ by: 'Bo_K', baseAt: 222, draft: faces('x') }); ok(r.status === 200, 'B a plain object is gzipped by the server');
    d = st.doc('Charm_Pool_Art_Draft', ID); ok(d.by === 'Bo_K' && d.baseAt === 222, 'B last writer wins, the other person named');
    // a big draft (random-ish text: does not gzip small) → a file
    let seed = 7; const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff).toString(36);
    const bigDoc = faces(Array.from({ length: 120000 }, rnd).join(''));
    r = await put({ draft: gz(bigDoc) }); ok(r.status === 200 && r.body.stored === 'file' && r.body.gz > 440000, 'B a big draft is a file: ' + JSON.stringify(r.body).slice(0, 120));
    d = st.doc('Charm_Pool_Art_Draft', ID); ok(d.blob && d.blob.path.startsWith('charmnest/art-draft/' + ID) && d.data === undefined && JSON.stringify(d).length < 2000, 'B the record names the file, holds no data');
    const path1 = d.blob.path; ok(st.blobs.has(path1), 'B the file is in Storage');
    r = await call({ op: 'artDraftGet', poolId: ID, full: true }); eq(JSON.parse(zlib.gunzipSync(Buffer.from(r.body.draft.data, 'base64')).toString()), bigDoc, 'B the big draft read back whole');
    r = await put({ draft: gz(bigDoc).slice(0, -8) + 'AAAA' }); // a corrupt gzip: refused, the file before stays
    ok(r.status === 400 && st.blobs.has(path1), 'B a refused put keeps the draft before');
    r = await put(); ok(r.status === 200 && r.body.stored === 'record' && !st.blobs.has(path1), 'B a small draft replaces the big one and the file is deleted');

    /* C */
    eq(st.doc('Charm_Pool', ID), row0, 'C the pool row is untouched (artEdit stamp, state)');
    ok(!st.list('Charm_Pool_Art').length, 'C no art record written');
    eq(JSON.stringify(st.list('Charm_Nest_Order_Rev') || []), gen0, 'C no feed counter moved');

    /* D */
    r = await call({ op: 'artDraftDrop', poolId: ID, by: ' ' }); ok(r.status === 400, 'D drop needs a name');
    r = await call({ op: 'artDraftDrop', poolId: ID, by: 'Ana_M' }); ok(r.body.ok && r.body.dropped && !st.doc('Charm_Pool_Art_Draft', ID), 'D dropped');
    r = await call({ op: 'artDraftDrop', poolId: ID, by: 'Ana_M' }); ok(r.body.ok && r.body.dropped === false, 'D dropping nothing is fine');
    r = await call({ op: 'artDraftGet', poolId: ID }); ok(r.body.draft === null, 'D get after drop is null');
    r = await put({ sandbox: true }); ok(r.status === 200 && st.doc('Sandbox_Charm_Pool_Art_Draft', ID) && !st.doc('Charm_Pool_Art_Draft', ID), 'D the sandbox keeps its own');
    r = await call({ op: 'artDraftGet', poolId: ID }); ok(r.body.draft === null, 'D production does not see the sandbox draft');
    r = await call({ op: 'artDraftGet', poolId: ID, sandbox: true }); ok(r.body.draft && r.body.draft.by === 'Ana_M', 'D the sandbox reads its own');
    console.log(`PASS ${n} checks: CEAUTOSAVE server — artDraftPut / Get / Drop`);
  } finally { srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
