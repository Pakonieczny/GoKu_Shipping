// AUDIT (R14 · set numbers on the server): op setAllocate (netlify/functions/charmNestLibrary.js), the real handler over the in-memory shop of
// tests/charm-nest/bridge-server.cjs with Firestore's optimistic transactions simulated (st.atomic: a transaction whose reads changed before
// its commit runs again). Required: one number per set per day across every run, group and "after" key, none twice, even when two screens ask
// at once; a set asked for again is the same set with ITS day; a follow-on set names a set of the same run that holds sheets or is committed.
//   node tests/charm-nest/audit-number-allocate-server.cjs
'use strict';
const assert = require('node:assert/strict');
(async () => {
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), { st } = srv; st.atomic = true;
  const SET = 'Charm_Nest_Sets';
  const post = async body => { const r = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) }); return { status: r.status, ...await r.json() }; };
  const alloc = (day, runId, extra = {}) => post({ op: 'setAllocate', day, runId, group: 'dispatch', ...extra });
  const fails = [];
  const check = async (name, fn) => { try { await fn(); console.log('ok   ' + name); } catch (e) { fails.push(name); console.log('FAIL ' + name + '\n     ' + (e && e.message)); } };
  const D1 = '2026-10-21', D2 = '2026-10-22';
  try {
    await check('two runs and their follow-on sets on one day: 1, 2, 3, 4 in the order asked, idempotent per key', async () => {
      const a1 = await alloc(D1, 'run-a'), b1 = await alloc(D1, 'run-b');
      assert.deepEqual([a1.seq, b1.seq], [1, 2]);
      st.put(SET, a1.setId, { sheetIds: ['gf-a', 'ss-a'] });
      const a2 = await alloc(D1, 'run-a', { after: a1.setId }); assert.equal(a2.seq, 3);
      assert.equal((await alloc(D1, 'run-a', { after: a1.setId })).setId, a2.setId, 'asked again after the same set: the same set');
      assert.equal((await alloc(D1, 'run-a')).setId, a1.setId, 'the run\'s plain dispatch key is its first set');
      st.put(SET, b1.setId, { committedAt: 5, sheetIds: ['gf-b'] });
      assert.equal((await alloc(D1, 'run-b', { after: b1.setId })).seq, 4);
    });
    await check('a follow-on set names a set of the same run that holds sheets or is committed (else refused)', async () => {
      const c1 = await alloc(D1, 'run-c');
      assert(/not a committed set of the run/.test((await alloc(D1, 'run-c', { after: c1.setId })).error || ''), 'an empty, open set cannot be followed');
      const a1 = st.list(SET).find(s => s.runId === 'run-a' && s.seq === 1);
      assert(/not a committed set of the run/.test((await alloc(D1, 'run-c', { after: a1.setId })).error || ''), 'a set of another run cannot be followed');
    });
    await check('past midnight: the follow-on set is Set 1 of the new day; asked again on either day it is the same set with ITS day', async () => {
      const a = st.list(SET).filter(s => s.runId === 'run-a').sort((x, y) => x.seq - y.seq), a2 = a[a.length - 1];
      st.put(SET, a2.setId, { sheetIds: ['gf-a2', 'ss-a2'] });
      const n1 = await alloc(D2, 'run-a', { after: a2.setId });
      assert.deepEqual([n1.seq, n1.day], [1, D2]);
      const again = await alloc(D1, 'run-a', { after: a2.setId });
      assert.deepEqual([again.setId, again.day], [n1.setId, D2], 'the day answered is the set\'s own');
    });
    st.tick = () => new Promise(r => setTimeout(r, Math.floor(Math.random() * 4)));   // (each read and commit yields: two requests interleave as on two servers)
    await check('two screens ask at once (different runs, same day): every number once', async () => {
      const day = '2026-10-23', out = await Promise.all(Array.from({ length: 8 }, (_, i) => alloc(day, 'run-p' + i)));
      const seqs = out.map(r => r.seq).sort((x, y) => x - y);
      assert.deepEqual(seqs, [1, 2, 3, 4, 5, 6, 7, 8], 'numbers ' + JSON.stringify(out.map(r => r.seq || r.error)));
      assert.equal(new Set(out.map(r => r.setId)).size, 8);
    });
    await check('two screens of ONE run ask for its set at once: one set, one number', async () => {
      const day = '2026-10-24', out = await Promise.all([alloc(day, 'run-q'), alloc(day, 'run-q'), alloc(day, 'run-q')]);
      assert.equal(new Set(out.map(r => r.setId)).size, 1, JSON.stringify(out.map(r => r.setId)));
      assert.equal(st.list(SET).filter(s => s.runId === 'run-q').length, 1, 'one set record');
    });
    await check('two screens of ONE run ask for its set at once across midnight: the answer carries the set\'s own day', async () => {
      // the first commits while the second is inside its transaction: the second runs again and finds the allocation (its own day is the next day)
      let held = null; const gate = new Promise(r => { held = r; });
      st.tick = async what => { if (/tx commit/.test(what) && !held.done) { held.done = true; await gate; } };
      const p1 = alloc('2026-10-26', 'run-m'); await new Promise(r => setTimeout(r, 60));
      const p2 = alloc('2026-10-25', 'run-m'); await new Promise(r => setTimeout(r, 60)); held(); st.tick = null;
      const out = await Promise.all([p1, p2]);
      const recs = st.list(SET).filter(s => s.runId === 'run-m');
      assert.equal(recs.length, 1, 'one set record'); 
      for (const r of out) assert.equal(r.day, recs[0].day, `answered ${r.setId} (seq ${r.seq}) with day ${r.day}, but that set is of ${recs[0].day}: the page would name its folder and its sheets' files for the wrong day`);
    });
  } finally { srv.close(); }
  console.log(`(transactions run again on conflict: ${st.txRetries || 0})`);
  if (fails.length) { console.log(`FAIL: audit-number-allocate-server: ${fails.length} case(s): ${fails.join(' | ')}`); process.exit(1); }
  console.log('PASS: audit-number-allocate-server');
})().catch(e => { console.error(e); process.exit(1); });
