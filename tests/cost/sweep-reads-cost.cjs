// COST (Oct 2026): what ONE sweep of the Design Station's open orders (the sorter's order check, every ten minutes in every sorter tab, and its
// snapshots) reads from Firestore through firebaseOrders, the old way and the new way, on an in-memory Firestore shaped like production
// (~800 open orders, 3,500 noted orders on record, stale selection locks left by closed tabs). Real handler, tests/cost/meter.cjs.
//   old: staffNotesFor per order (ten-id "in" queries) + dcFor on every open order + rt=1 (every lock and claim held) + a read per noted order for its text
//   new: staffNotesFor from the staff-note index (_staffNotes.js) + dcFor only on orders not asked about yet + dcSince delta + rtSince delta
// Also checked: the index answers exactly what the per-order asks answered (texts included), follows every note written, ignores the scanner
// relay documents, keeps a long note as "has a note", is reconciled weekly; an un-complete is seen by dcSince; a selection lock not written for
// 24 hours is released by the realtime sweep.
//   node tests/cost/sweep-reads-cost.cjs
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const say = s => process.stdout.write(s + '\n');
const root = path.join(__dirname, '../..');
const T = ms => meter.Timestamp.fromMillis(ms), NOW = Date.now(), H = 3600e3;

const OPEN = 800, rid = i => String(4300000000 + i), old = i => String(3900000000 + i);
const open = Array.from({ length: OPEN }, (_, i) => rid(i));
(async () => {
  const m = meter.create(), seed = {};
  for (let i = 0; i < OPEN; i++) seed['Brites_Orders/' + rid(i)] = Object.assign({ 'Order Number': rid(i), 'Client Name': 'Customer ' + i, 'Brites Messages': 'x'.repeat(800) }, i % 10 === 0 ? { 'Staff Note': 'use the 14k chain ' + i } : {});
  seed['Brites_Orders/' + rid(5)]['Staff Note'] = 'L'.repeat(4000);                                       // one note too long for the index
  for (let i = 0; i < 3400; i++) seed['Brites_Orders/' + old(i)] = { 'Order Number': old(i), 'Staff Note': 'old note ' + i };   // the notes of orders long gone
  seed['Brites_Orders/weld-scan-1'] = { 'Staff Note': JSON.stringify({ v: 1, id: 'a', at: NOW, sent: NOW }) };                  // a scanner relay document
  for (let i = 0; i < 5000; i++) seed['Design_Completed Orders/' + old(i)] = { orderId: old(i), completed: true, completedAt: T(NOW - 30 * H - i * 60e3) };
  for (let i = 0; i < 30; i++) seed['Design_Completed Orders/' + rid(700 + i)] = { orderId: rid(700 + i), completed: true, completedAt: T(NOW - 2 * H) };
  for (let i = 0; i < 150; i++) seed['Design_RealTime_Selected_Orders/' + old(i)] = { selected: true, selectedBy: 'closed-tab', page: 'design', at: T(NOW - 3 * 24 * H - i * 60e3) };   // left by tabs that never released
  for (let i = 0; i < 10; i++) seed['Design_RealTime_Selected_Orders/' + rid(100 + i)] = { selected: true, selectedBy: 'bench-' + i, page: 'design-1', at: T(NOW - H) };
  for (let i = 0; i < 20; i++) seed['Design_RealTime_Selected_Orders/' + rid(200 + i)] = { claimed: true, claimedBy: 'sorter', claimRun: 'run1', claimAt: T(NOW - H), at: T(NOW - H) };
  for (let i = 0; i < 400; i++) seed['Design_RealTime_Selected_Orders/' + old(1000 + i)] = { selected: false, selectedBy: null, page: null, at: T(NOW - (i + 2) * H), expireAt: new Date(NOW + 20 * 86400e3) };
  seed['Staff_Notes_Index/n0'] = { buildingAt: NOW };   // (a build "in progress": the first sweep below is answered the old way, per order)
  m.db.seed(seed);
  m.install();
  const fo = require(path.join(root, 'netlify/functions/firebaseOrders.js')), SN = require(path.join(root, 'netlify/functions/_staffNotes.js'));
  const get = async q => JSON.parse((await fo.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: q })).body);
  const post = async b => JSON.parse((await fo.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(b) })).body);
  const chunks = (a, n) => { const o = []; for (let i = 0; i < a.length; i += n) o.push(a.slice(i, i + n)); return o; };
  const measure = async (name, fn) => { const s = m.snapshot(); const out = await m.op(name, fn); return { d: m.since(s), out }; };
  const notesOf = async ids => { const found = new Set(), notes = {}; for (const c of chunks(ids, 100)) { const j = await get({ staffNotesFor: c.join(',') }); j.orderNumbers.forEach(id => found.add(id)); Object.assign(notes, j.notes || {}); } return { found, notes }; };
  const noted = open.filter((_, i) => i % 10 === 0 || i === 5);

  /* ── BEFORE: one sweep the old way ── */
  const bNotes = await measure('before.staffNotesFor', () => notesOf(open));
  const bDc = await measure('before.dcFor', async () => { for (const c of chunks(open, 100)) await get({ dcFor: c.join(',') }); });
  const bRt = await measure('before.rt=1', () => get({ rt: '1' }));
  const bText = await measure('before.staffNoteOf', async () => { for (const id of noted) await get({ orderId: id }); });   // the snapshot read each noted order for its text
  const before = bNotes.d.reads + bDc.d.reads + bRt.d.reads + bText.d.reads;
  say('one sweep of ' + OPEN + ' open orders, BEFORE:');
  say(`  staffNotesFor (per order)        ${bNotes.d.reads} reads`);
  say(`  dcFor (every open order)         ${bDc.d.reads} reads`);
  say(`  rt=1 (every lock and claim held) ${bRt.d.reads} reads (${Object.keys(bRt.out.locks).length} locks, 150 of them stale)`);
  say(`  staffNoteOf (a read per note)    ${bText.d.reads} reads`);
  say(`  total                            ${before} reads`);

  /* ── the index: built once (the old whole read), then 4 reads ── */
  await m.db.collection('Staff_Notes_Index').doc('n0').set({ buildingAt: null }, { merge: true });
  const build = await measure('index.build', () => get({ staffNotesFor: open.slice(0, 100).join(',') }));
  assert.equal(build.out.via, 'built');
  say(`staff-note index built once: ${build.d.reads} reads, ${build.d.writes} writes`);
  const shards = (await m.db.collection('Staff_Notes_Index').get()).docs.map(d => d.data());
  assert.equal(shards.length, SN.SHARDS); assert(shards.every(s => s.ready === true && s.v === 1));
  const indexed = Object.assign({}, ...shards.map(s => s.notes || {}));
  assert(!('weld-scan-1' in indexed), 'a scanner relay document is not a note'); assert.equal(indexed[rid(5)], true, 'a long note is kept as "has a note"');
  assert.equal(Object.keys(indexed).length, noted.length + 3400, 'every receipt with a note');

  /* ── AFTER: one sweep the new way (within the hour: nothing new to ask dcFor about) ── */
  SN._reset();
  const since = NOW - 10 * 60e3;
  const aNotes = await measure('after.staffNotesFor', () => notesOf(open));
  const aDc = await measure('after.dcSince', () => get({ dcSince: String(since) }));
  const aRt = await measure('after.rtSince', () => get({ rtSince: String(since) }));
  const aText = await measure('after.staffNoteOf', async () => { for (const id of noted) if (!(id in aNotes.out.notes)) await get({ orderId: id }); });
  const after = aNotes.d.reads + aDc.d.reads + aRt.d.reads + aText.d.reads;
  say('one sweep, AFTER:');
  say(`  staffNotesFor (index, ${Math.ceil(OPEN / 100)} chunks)  ${aNotes.d.reads} reads`);
  say(`  dcSince (delta)                  ${aDc.d.reads} reads`);
  say(`  rtSince (delta)                  ${aRt.d.reads} reads`);
  say(`  staffNoteOf (only the long note) ${aText.d.reads} reads`);
  say(`  total                            ${after} reads (${(before / after).toFixed(0)} times fewer)`);
  assert.deepEqual([...aNotes.out.found].sort(), [...bNotes.out.found].sort(), 'the index answers which orders have a note exactly as the per-order asks did');
  for (const id of noted) if (id !== rid(5)) assert.equal(aNotes.out.notes[id], bNotes.out.notes[id], 'and the same text');
  assert(!(rid(5) in aNotes.out.notes) && aNotes.out.found.has(rid(5)), 'a long note: listed, its text read from the order');
  assert(aNotes.d.reads <= 4 * Math.ceil(OPEN / 100), 'at most 4 reads a chunk'); assert(after * 20 < before, 'at least twenty times fewer reads a sweep');

  /* ── a note written, changed, cleared: the index follows in the same batch; a relay document never enters it ── */
  await post({ orderNumber: rid(1), staffNote: 'call the buyer' });
  assert.equal((await get({ staffNotesFor: rid(1) })).notes[rid(1)], 'call the buyer');
  await post({ orderNumber: rid(10), staffNote: '' });
  assert.deepEqual((await get({ staffNotesFor: rid(10) })).orderNumbers, [], 'a cleared note leaves the index');
  await post({ orderNumber: 'weld-scan-1', orderNumField: '1', staffNote: '{"v":1}' });
  assert(!('weld-scan-1' in Object.assign({}, ...(await m.db.collection('Staff_Notes_Index').get()).docs.map(d => d.data().notes || {}))));
  /* ── a note changed outside firebaseOrders: the weekly reconcile corrects it, each difference verified against its order ── */
  await m.db.collection('Brites_Orders').doc(rid(20)).set({ 'Staff Note': 'changed in the console' }, { merge: true });
  SN._reset();
  const rec = await SN.lookup(m.db, meter.FieldValue || require(path.join(root, 'netlify/functions/firebaseAdmin')).firestore.FieldValue, [rid(20)], { now: NOW + 8 * 24 * H });
  assert.equal(rec.via, 'reconciled'); assert.equal(rec.notes.get(rid(20)), 'changed in the console');
  SN._reset(); assert.equal((await get({ staffNotesFor: rid(20) })).notes[rid(20)], 'changed in the console', 'and the index holds it from then on');
  /* ── the sandbox keeps the per-order asks ── */
  assert.equal((await fo.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: { staffNotesFor: rid(0), sandbox: '1' } })).statusCode, 200);
  /* ── ?staffNotes=1 is gone ── */
  assert.notEqual((await fo.handler({ httpMethod: 'GET', headers: {}, queryStringParameters: { staffNotes: '1' } })).statusCode, 200, '?staffNotes=1 no longer answers');

  /* ── an un-complete is seen by the delta; a completion again clears its mark ── */
  const t0 = Date.now() - 1000;
  await post({ uncompleteIds: [rid(700)] });
  let dj = await get({ dcSince: String(t0) });
  assert.deepEqual(dj.undone, [rid(700)]); assert(!dj.orderNumbers.includes(rid(700)));
  await post({ completedIds: [rid(700)] });
  dj = await get({ dcSince: String(t0) });
  assert.deepEqual(dj.undone, [], 'completed again: no un-complete mark'); assert(dj.orderNumbers.includes(rid(700)));

  /* ── a selection not written for 24 hours is released by the realtime sweep (a tombstone the delta polls see) ── */
  const t1 = Date.now() - 1000;
  await post({ rtUnlockIds: [rid(100)], clientId: 'bench-0' });
  const rt = await get({ rt: '1' });
  assert.equal(Object.keys(rt.locks).length, 9, 'only the live locks are left (' + Object.keys(rt.locks).length + ')');
  const delta = await get({ rtSince: String(t1) });
  assert(delta.unlocks.includes(old(0)) && delta.unlocks.includes(old(149)), 'the delta polls see the stale locks released');
  assert.equal(Object.keys(rt.claims).length, 20, 'claims are untouched');
  m.uninstall();
  say('sweep-reads-cost: passed');
})().catch(e => { console.error(e); process.exit(1); });
