// ENGMAP (front-back-1011): the SERVER keeps a piece's FRONT engraving beside its back. Offline: the real charmNestLibrary handler over the in-memory
// Firestore (bridge-server.cjs). Nothing live, no Etsy, no paid call.
//
//   1  a back sent as it always was (no `face`) is stored under its poolId, exactly as before, and the sheet lists it
//   2  a front (face "front") is a second record: doc "<poolId>~front", the sheet lists BOTH, the back's doc is untouched
//   3  backList returns both docs; the front one says face "front"
//   4  putSheet by a page that only knows the back keeps the front (and the other way round); a stale copy never drops a face
//   5  backInvalidate with no face takes only the back (as before); face "front" takes only the front; "all" takes both
//   6  the order's timeline has one event for each face, ids "<poolId>-<at>" and "<poolId>~front-<at>" (the timeline turns the "~" into "_"), the front one says so
//   7  an older back is superseded by a newer back of the same face only; a front never replaces a back
//   8  backPreview names the face it asks for
//   9  no document the fake stores has an array inside an array
//   node tests/charm-nest/engrave-faces-server.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const { start } = require('./bridge-server.cjs');

const results = [];
const t = async (name, fn) => { try { await fn(); results.push([name, null]); console.log('  ok  ' + name); } catch (e) { results.push([name, e]); console.log('  FAIL ' + name + '\n      ' + String(e && e.stack || e).split('\n').slice(0, 4).join('\n      ')); } };
const NOW = Date.now(), LATER = NOW + 600000, RID = '4195966039', TID = '9100000099', POOL1 = `${RID}_${TID}_1`, SHEET = 'sheet-fb-1', SHEET2 = 'sheet-fb-2';
const back = (o = {}) => Object.assign({ poolId: POOL1, sheetId: SHEET, order: RID, transactionId: TID, sku: 'MINI_0445', copy: 1, text: '1. 27. 24', lines: ['1. 27. 24'], approvedAt: NOW, approvedBy: 'Paul' }, o);
const front = (o = {}) => back(Object.assign({ face: 'front', text: 'W', lines: ['W'], approvedAt: NOW + 10 }, o));

(async () => {
  const srv = await start(); const st = srv.st; st.atomic = true;
  st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, metal: 'gold', sheetIndex: 1, page: 1, day: '2026-10-11', status: 'written', folder: SHEET, fileBase: SHEET, poolIds: [POOL1], orders: [RID], charmCount: 1, placedCount: 1 });
  st.put('Charm_Pool', POOL1, { poolId: POOL1, orderId: RID, transactionId: TID, lineKey: `${RID}_${TID}`, sku: 'MINI_0445', material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SHEET });
  const call = async body => { const r = await fetch(`${srv.sorterOrigin}/.netlify/functions/charmNestLibrary`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, body: await r.json().catch(() => ({})) }; };
  const doc = id => st.doc('Charm_Pool_Back', id), sheet = id => st.doc('Charm_Nest_Sheets', id);
  const listed = (id = SHEET) => (sheet(id).backPool || []).map(b => `${b.poolId}${b.face === 'front' ? '~front' : ''}:${b.text}`).sort();
  try {
    await t('1 a back with no face is stored under its poolId, as it always was, and the sheet lists it', async () => {
      const r = await call({ op: 'backPut', backs: [back()] }); assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(r.body.written, 1);
      assert.equal(doc(POOL1).text, '1. 27. 24'); assert.equal('face' in doc(POOL1), false, 'a back record carries no face'); assert.equal(doc(POOL1 + '~front'), undefined);
      assert.deepEqual(listed(), [`${POOL1}:1. 27. 24`]);
      const e = st.list('Order_Timeline').filter(x => x.orderId === RID && x.type === 'engraveApproved'); assert.equal(e.length, 1); assert.equal(e[0]._id, `${RID}~engraveApproved~${POOL1}-${NOW}`); assert.equal('face' in (e[0].data || {}), false); assert.equal(/^Front/.test(e[0].text), false);
    });
    await t('2 a front is a second record: doc "<poolId>~front"; the sheet lists both; the back is untouched', async () => {
      const before = JSON.stringify(doc(POOL1));
      const r = await call({ op: 'backPut', backs: [front()] }); assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(r.body.written, 1);
      assert.equal(doc(POOL1 + '~front').face, 'front'); assert.equal(doc(POOL1 + '~front').poolId, POOL1); assert.equal(doc(POOL1 + '~front').text, 'W');
      assert.equal(JSON.stringify(doc(POOL1)), before, 'the back doc did not change');
      assert.deepEqual(listed(), [`${POOL1}:1. 27. 24`, `${POOL1}~front:W`].sort());
      assert.equal(sheet(SHEET).backPool.find(b => b.face === 'front').approvedAt, NOW + 10);
    });
    await t('3 backList returns both records; the front one says face "front"', async () => {
      const r = await call({ op: 'backList', sheetId: SHEET }); assert.equal(r.status, 200); const bs = r.body.backs;
      assert.equal(bs.length, 2); assert.equal(bs.filter(b => b.face === 'front').length, 1); assert.equal(bs.filter(b => !b.face).length, 1);
    });
    await t('4 a page that only knows the back saves the sheet: the front stays (and a page that only knows the front keeps the back)', async () => {
      const s = sheet(SHEET), onlyBack = s.backPool.filter(b => b.face !== 'front'), onlyFront = s.backPool.filter(b => b.face === 'front');
      let r = await call({ op: 'putSheet', sheet: { id: SHEET, poolIds: [POOL1], backPool: onlyBack } }); assert.equal(r.status, 200, JSON.stringify(r.body));
      assert.deepEqual(listed(), [`${POOL1}:1. 27. 24`, `${POOL1}~front:W`].sort());
      r = await call({ op: 'putSheet', sheet: { id: SHEET, poolIds: [POOL1], backPool: onlyFront } }); assert.equal(r.status, 200, JSON.stringify(r.body));
      assert.deepEqual(listed(), [`${POOL1}:1. 27. 24`, `${POOL1}~front:W`].sort());
      // getSheet puts the whole records back (the front from its own doc)
      const g = (await call({ op: 'getSheet', id: SHEET })).body.sheet; assert.equal(g.backPool.length, 2);
    });
    await t('5 backInvalidate: no face = the back only; "front" = the front only; "all" = both', async () => {
      let r = await call({ op: 'backInvalidate', poolIds: [POOL1] }); assert.equal(r.status, 200);
      assert.equal(doc(POOL1).invalidated, true); assert.notEqual(doc(POOL1 + '~front').invalidated, true); assert.deepEqual(listed(), [`${POOL1}~front:W`]);
      // approve the back again (newer), then take only the front
      r = await call({ op: 'backPut', backs: [back({ approvedAt: LATER + 100 })] }); assert.equal(r.status, 200, JSON.stringify(r.body)); assert.equal(doc(POOL1).invalidated, false);
      r = await call({ op: 'backInvalidate', poolIds: [POOL1], face: 'front' }); assert.equal(r.status, 200);
      assert.equal(doc(POOL1 + '~front').invalidated, true); assert.equal(doc(POOL1).invalidated, false); assert.deepEqual(listed(), [`${POOL1}:1. 27. 24`]);
      r = await call({ op: 'backPut', backs: [front({ approvedAt: LATER + 200 })] }); assert.equal(r.status, 200, JSON.stringify(r.body));
      assert.deepEqual(listed(), [`${POOL1}:1. 27. 24`, `${POOL1}~front:W`].sort());
      r = await call({ op: 'backInvalidate', poolIds: [POOL1], face: 'all' }); assert.equal(r.status, 200);
      assert.equal(doc(POOL1).invalidated, true); assert.equal(doc(POOL1 + '~front').invalidated, true); assert.deepEqual(listed(), []);
    });
    await t('6 the order\'s timeline: one event for each face, the front one says so', async () => {
      const ev = st.list('Order_Timeline').filter(x => x.orderId === RID && x.type === 'engraveApproved'), ids = ev.map(x => x._id.split('~engraveApproved~')[1]).sort();
      assert.deepEqual(ids, [`${POOL1}-${NOW}`, `${POOL1}-${LATER + 100}`, `${POOL1}_front-${NOW + 10}`, `${POOL1}_front-${LATER + 200}`].sort());
      const f = ev.find(x => x._id.endsWith(`~${POOL1}_front-${NOW + 10}`)); assert.equal(f.data.face, 'front'); assert.match(f.text, /^Front: /); assert.equal(f.data.poolId, POOL1);
      assert.equal(ev.filter(x => x.data && x.data.face === 'front').length, 2); assert.equal(ev.filter(x => x.data && !x.data.face).length, 2);
    });
    await t('7 an older back of a face never replaces a newer one; a front never replaces a back', async () => {
      let r = await call({ op: 'backPut', backs: [back({ approvedAt: LATER + 1000, text: 'newest' })] }); assert.equal(r.status, 200, JSON.stringify(r.body));
      r = await call({ op: 'backPut', backs: [front({ approvedAt: LATER + 2000, text: 'F2' })] }); assert.equal(r.status, 200, JSON.stringify(r.body));
      assert.equal(doc(POOL1).text, 'newest'); assert.equal(doc(POOL1 + '~front').text, 'F2');
      r = await call({ op: 'backPut', backs: [back({ approvedAt: LATER + 500, text: 'older' })] }); assert.notEqual(r.status, 200); assert.equal(doc(POOL1).text, 'newest');
      assert.equal(doc(POOL1 + '~front').text, 'F2', 'the front did not move');
      assert.deepEqual(listed(), [`${POOL1}:newest`, `${POOL1}~front:F2`].sort());
    });
    await t('8 backPreview names the face it asks for', async () => {
      const a = await call({ op: 'backPreview', sheetId: SHEET, poolId: POOL1, face: 'front', approvedAt: NOW + 1 }); assert.match(JSON.stringify(a.body), /changed|unavailable|no longer|storage|bucket/i);   // (no storage here: it finds the record, then cannot read the file)
      const bad = await call({ op: 'backPreview', sheetId: SHEET2, poolId: POOL1, face: 'front' }); assert.match(JSON.stringify(bad.body), /front engraving no longer belongs/);
      const bad2 = await call({ op: 'backPreview', sheetId: SHEET2, poolId: POOL1 }); assert.match(JSON.stringify(bad2.body), /This back no longer belongs/);
    });
    await t('9 every document stored by every case above passes the Firestore nested-array check', () => {
      let n = 0; for (const [k, v] of st.docs) { refuseNestedArrays(v, k); n++; } assert(n >= 4, 'documents checked: ' + n);
    });
  } finally { srv.close(); }
  const failed = results.filter(([, e]) => e);
  console.log(`\n${results.length - failed.length}/${results.length} passed`);
  if (failed.length) process.exit(1);
})().catch(e => { console.error(e); process.exit(1); });
