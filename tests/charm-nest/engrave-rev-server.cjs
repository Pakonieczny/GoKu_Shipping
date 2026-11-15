// ENGREV (Paul, 15 Nov 2026: "I wanna make sure they're absolutely rock solid between the cloud and all the computers"). Offline: the real charmNestLibrary handler over the in-memory Firestore.
//   1  engraveSyncPut: a line carries a rev; a stale writer is refused and reported (stale), the fresh one wins, a put with no baseRev still works, the others of the same call are written
//   2  engraveSyncGet returns each line's rev
//   3  backPut with artAt: older refused (staleArt, nothing stored), equal / newer accepted, no artAt accepted; the stamp is never stored
//   node tests/charm-nest/engrave-rev-server.cjs
'use strict';
const assert = require('assert/strict');
const { start } = require('./bridge-server.cjs');
const NOW = Date.now(), RID = '4195966039', TID = '9100000099', POOL1 = `${RID}_${TID}_1`, SHEET = 'sheet-rev-1', K1 = `${RID}_${TID}`, K2 = `${RID}_9100000100`;
const back = (o = {}) => Object.assign({ poolId: POOL1, sheetId: SHEET, order: RID, transactionId: TID, sku: 'MINI_0445', copy: 1, text: 'AB', lines: ['AB'], approvedAt: NOW, approvedBy: 'Paul' }, o);
(async () => {
  const srv = await start(); const st = srv.st; st.atomic = true;
  st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', sheetIndex: 1, page: 1, day: '2026-10-11', status: 'written', folder: SHEET, fileBase: SHEET, poolIds: [POOL1], orders: [RID], charmCount: 1, placedCount: 1 });
  st.put('Charm_Pool', POOL1, { poolId: POOL1, orderId: RID, transactionId: TID, lineKey: K1, sku: 'MINI_0445', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SHEET, artEdit: { back: 5000 } });
  const call = async body => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, body: await r.json().catch(() => ({})) }; };
  const line = (key, words, o) => Object.assign({ key, rec: { v: 1, jobs: [{ key: key + '_b', words }] }, sig: words }, o || {});
  let ok = 0; const t = async (n, f) => { await f(); ok++; console.log('  ok  ' + n); };
  try {
    await t('1 two writers of one line: stale refused and reported, fresh wins, no baseRev accepted, others written', async () => {
      let r = await call({ op: 'engraveSyncPut', lines: [line(K1, 'one')], by: 'A', dev: '' }); assert.equal(r.status, 200, JSON.stringify(r.body));
      assert.equal(r.body.lines[0].rev, 1); assert.equal(r.body.stale, undefined);
      // computer A and B both read rev 1; A writes (rev 2); B's write from rev 1 is stale
      r = await call({ op: 'engraveSyncPut', lines: [line(K1, 'A-words', { baseRev: 1 })], by: 'A' }); assert.equal(r.body.lines[0].rev, 2);
      r = await call({ op: 'engraveSyncPut', lines: [line(K1, 'B-words', { baseRev: 1 }), line(K2, 'other', { baseRev: 0 })], by: 'B' }); assert.equal(r.status, 200, JSON.stringify(r.body));
      assert.deepEqual(r.body.stale, [{ key: K1, rev: 2 }]); assert.deepEqual(r.body.lines.map(x => x.key), [K2]);
      assert.equal(JSON.parse(st.doc('Charm_Nest_Engrave', K1).rec).jobs[0].words, 'A-words', 'the fresh write stays'); assert.equal(st.doc('Charm_Nest_Engrave', K1).rev, 2);
      assert.equal(JSON.parse(st.doc('Charm_Nest_Engrave', K2).rec).jobs[0].words, 'other');
      r = await call({ op: 'engraveSyncPut', lines: [line(K1, 'old-page')], by: 'C' }); assert.equal(r.body.lines[0].rev, 3); assert.equal(r.body.stale, undefined);
      r = await call({ op: 'engraveSyncPut', lines: [line(K1, 'B-again', { baseRev: 3 })], by: 'B' }); assert.equal(r.body.lines[0].rev, 4);
    });
    await t('2 engraveSyncGet returns the rev', async () => {
      const r = await call({ op: 'engraveSyncGet', keys: [K1, K2] }); const m = Object.fromEntries(r.body.lines.map(l => [l.key, l.rev])); assert.deepEqual(m, { [K1]: 4, [K2]: 1 });
    });
    await t('3 backPut artAt: older refused, equal/newer accepted, none accepted', async () => {
      let r = await call({ op: 'backPut', backs: [back({ artAt: 4000 })] }); assert.equal(r.status, 200, JSON.stringify(r.body)); assert.deepEqual(r.body.staleArt, [POOL1]); assert.equal(r.body.written, 0);
      assert.equal(st.doc('Charm_Pool_Back', POOL1), undefined, 'nothing stored'); assert.equal(st.list('Order_Timeline').filter(x => x.type === 'engraveApproved').length, 0);
      r = await call({ op: 'backPut', backs: [back({ face: 'front', artAt: 1 })] }); assert.equal(r.body.written, 1, 'the front has no stamp: the back stamp does not count'); assert.equal(r.body.staleArt, undefined);
      r = await call({ op: 'backPut', backs: [back({ artAt: 5000 })] }); assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(r.body.written, 1); assert.equal(r.body.staleArt, undefined);
      assert.equal('artAt' in st.doc('Charm_Pool_Back', POOL1), false, 'the stamp is not stored');
      r = await call({ op: 'backPut', backs: [back({ artAt: 9000, approvedAt: NOW + 5, text: 'CD' })] }); assert.equal(r.body.written, 1);
      r = await call({ op: 'backPut', backs: [back({ approvedAt: NOW + 9, text: 'EF' })] }); assert.equal(r.body.written, 1, 'no artAt: as before');
      r = await call({ op: 'backPut', backs: [back({ artAt: 0, approvedAt: NOW + 20 })] }); assert.deepEqual(r.body.staleArt, [POOL1], 'artAt 0 with a stamp on the row is stale');
    });
    console.log('engrave-rev-server: ' + ok + '/3 ok');
  } catch (e) { console.log('  FAIL ' + String(e && e.stack || e).split('\n').slice(0, 5).join('\n')); process.exitCode = 1; }
  finally { try { await srv.close(); } catch (_) {} setTimeout(() => process.exit(process.exitCode || 0), 100); }
})();
