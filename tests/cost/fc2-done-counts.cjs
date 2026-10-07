// The Completed tab's count (doneCounts) is kept in one document (Charm_Nest_Rev/done) and read for one read while nothing that
// decides it was written (FC2). Real handler, the meter's in-memory Firestore. Checks that the number is always the one a
// full count gives, after each kind of write that can change it, and what a read costs before and after.
//   node tests/cost/fc2-done-counts.cjs
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const W = path.join(__dirname, '..', '..');
const T = 1.7e12;

(async () => {
  const m = meter.create(); m.install();
  const docs = {};
  // 40 sets of 3 completed sheets (the set completed, so all 120 sheets are filed), 6 sheets of 2 sets completed one by one
  // (their sets are not: not filed yet), 4 completed sheets that are in no set, 1 draft sheet completed, 2 open sets
  for (let s = 0; s < 40; s++) {
    const ids = [1, 2, 3].map(k => `sheet-${s}-${k}`);
    docs[`Charm_Nest_Sets/set-${s}`] = { setId: `set-${s}`, sheetIds: ids, laserDoneAt: T + s, status: 'complete', updatedAt: T };
    ids.forEach(id => { docs[`Charm_Nest_Sheets/${id}`] = { id, setId: `set-${s}`, laserDoneAt: T + s, metal: 'gold', archived: false, updatedAt: T }; });
  }
  for (let s = 0; s < 2; s++) {
    const ids = [1, 2, 3].map(k => `part-${s}-${k}`);
    docs[`Charm_Nest_Sets/part-${s}`] = { setId: `part-${s}`, sheetIds: ids, status: 'open', updatedAt: T };
    ids.forEach((id, k) => { docs[`Charm_Nest_Sheets/${id}`] = { id, setId: `part-${s}`, metal: 'gold', archived: false, updatedAt: T, ...(k < 2 ? { laserDoneAt: T + 99 } : {}) }; });
  }
  for (let k = 0; k < 4; k++) docs[`Charm_Nest_Sheets/alone-${k}`] = { id: `alone-${k}`, metal: 'gold', laserDoneAt: T + 50 + k, archived: false, updatedAt: T };
  docs['Charm_Nest_Sheets/draft-1'] = { id: 'draft-1', setId: 'part-0', draft: true, metal: 'gold', laserDoneAt: T + 70, archived: false, updatedAt: T };
  m.db.seed(docs);
  const fn = require(path.join(W, 'netlify/functions/charmNestLibrary.js'));
  const post = body => fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) });
  const countOnly = async (sandbox) => { const r = await post({ op: 'laserDoneList', countOnly: true, ...(sandbox ? { sandbox: true } : {}) }); assert.strictEqual(r.statusCode, 200, r.body); return JSON.parse(r.body).counts; };
  // the definition, worked out from the records themselves (what a full count is)
  const truth = () => {
    const sheets = [...m.db.docs.entries()].filter(([k]) => /^Charm_Nest_Sheets\//.test(k)).map(([, v]) => v), sets = new Map([...m.db.docs.entries()].filter(([k]) => /^Charm_Nest_Sets\//.test(k)).map(([k, v]) => [k.split('/')[1], v]));
    const pending = s => !!(s.setId && !s.draft && s.solidIncluded !== false && !(+(sets.get(s.setId) || {}).laserDoneAt > 0));
    return { sheets: sheets.filter(s => +s.laserDoneAt > 0 && !s.archived && !pending(s)).length, sets: [...sets.values()].filter(s => +s.laserDoneAt > 0).length };
  };
  const cost = async f => { const a = m.snapshot(); const r = await f(); const d = m.since(a); return { r, reads: d.reads + d.aggs, writes: d.writes }; };
  const rev = () => m.db.docs.get('Charm_Nest_Rev/done') || {};
  const ok = (name, got, want) => { assert.deepStrictEqual(got, want, `${name}: ${JSON.stringify(got)} vs ${JSON.stringify(want)}`); process.stdout.write(`ok   ${name} ${JSON.stringify(got)}\n`); };

  // 1. the first read counts (every sheet), keeps the answer; the next ones read one document
  const first = await cost(() => countOnly());
  ok('first read is the full count', first.r, truth()); assert.ok(first.reads > 100, 'the first read counted: ' + first.reads);
  assert.strictEqual(first.writes, 1, 'and kept it (one write)');
  const again = await cost(() => countOnly());
  ok('second read is the kept number', again.r, truth()); assert.strictEqual(again.reads, 1, 'one document read, got ' + again.reads); assert.strictEqual(again.writes, 0);
  process.stdout.write(`     full count ${first.reads} reads, kept answer ${again.reads} read\n`);

  // 2. every write that can change the count raises the revision, and the next read is exact again
  // 2a. a completion taken back (laserDone done:false on one sheet of a completed set: the set is taken back with it)
  let r = await post({ op: 'laserDone', kind: 'sheet', id: 'sheet-3-2', done: false, by: 'Test' }); assert.strictEqual(r.statusCode, 200, r.body);
  ok('2a laserDone (undo) answers the exact count', JSON.parse(r.body).counts, truth());
  ok('2a and so does the next read', await countOnly(), truth());
  assert.strictEqual(m.db.docs.get('Charm_Nest_Sets/set-3').laserDoneAt, undefined, 'the set was taken back');
  // 2b. a completed sheet deleted
  r = await post({ op: 'deleteSheet', id: 'alone-0', code: '975311' }); assert.strictEqual(r.statusCode, 200, r.body);
  ok('2b deleteSheet', await countOnly(), truth());
  // 2c. a completed sheet archived (the mark is kept aside): nothing marked is archived
  m.db.seed({ 'Charm_Nest_Runs/run-x': { runId: 'run-x', status: 'running' }, 'Charm_Nest_Sheets/alone-1': { ...m.db.docs.get('Charm_Nest_Sheets/alone-1'), runId: 'run-x' } });
  await countOnly();
  r = await post({ op: 'archiveEmptySheet', id: 'alone-1', runId: 'run-x' }); assert.strictEqual(r.statusCode, 200, r.body);
  ok('2c archiveEmptySheet', await countOnly(), truth());
  // 2d. a completed sheet saved with another set / as a draft (putSheet): the count moves, and a save that changes nothing does not raise the revision
  const n0 = rev().n;
  r = await post({ op: 'putSheet', sheet: { id: 'alone-2', metal: 'gold', day: '2026-10-01' } }); assert.strictEqual(r.statusCode, 200, r.body);
  assert.strictEqual(rev().n, n0, 'a save that changes nothing that decides the count leaves the revision alone');
  await countOnly(); const kept = rev().n;
  r = await post({ op: 'putSheet', sheet: { id: 'alone-2', metal: 'gold', day: '2026-10-01', setId: 'part-1' } }); assert.strictEqual(r.statusCode, 200, r.body);
  assert.ok(rev().n > kept, 'a completed sheet put into an open set raises the revision');
  ok('2d putSheet into an open set (no longer filed)', await countOnly(), truth());
  r = await post({ op: 'putSheet', sheet: { id: 'alone-2', metal: 'gold', day: '2026-10-01', setId: 'part-1', draft: true } }); assert.strictEqual(r.statusCode, 200, r.body);
  ok('2d putSheet as a draft (filed again)', await countOnly(), truth());
  // 2e. an open run saving a sheet that is not completed does not touch the count or its revision
  const n1 = rev().n;
  r = await post({ op: 'putSheet', sheet: { id: 'part-0-3', metal: 'gold', day: '2026-10-01', setId: 'part-0' } }); assert.strictEqual(r.statusCode, 200, r.body);
  assert.strictEqual(rev().n, n1, 'saving a sheet that is not completed leaves the revision alone');

  // 3. a counter raised while a read counted leaves the kept numbers behind: the next read counts again
  await countOnly();
  const k = rev(); m.db.seed({ 'Charm_Nest_Rev/done': { ...k, n: k.n + 1 } });
  m.db.docs.get('Charm_Nest_Sheets/sheet-5-1').laserDoneAt = undefined;   // (a write by hand, then the counter raised by whoever wrote it)
  ok('3 a raised revision is counted again', await countOnly(), truth());
  // 4. a write nothing raised the counter for shows within the time limit
  m.db.docs.get('Charm_Nest_Sheets/sheet-6-1').laserDoneAt = undefined;
  const stale = await countOnly(); assert.notDeepStrictEqual(stale, truth(), 'inside the time limit a hand edit is not seen (that is what the limit is for)');
  m.db.seed({ 'Charm_Nest_Rev/done': { ...rev(), at: Date.now() - 21 * 60000 } });
  ok('4 after the time limit it is counted again', await countOnly(), truth());
  // 5. the sandbox counts every time and keeps no counter document
  const sbBefore = [...m.db.docs.keys()].filter(x => /^Sandbox_/.test(x)).length;
  m.db.seed({ 'Sandbox_Charm_Nest_Sheets/s1': { id: 's1', laserDoneAt: T, metal: 'gold', archived: false }, 'Sandbox_Charm_Nest_Sets/z': { setId: 'z', sheetIds: ['s1'] } });
  const sb = await cost(() => countOnly(true)); ok('5 sandbox', sb.r, { sheets: 1, sets: 0 }); assert.strictEqual(sb.writes, 0);
  assert.ok(![...m.db.docs.keys()].some(x => /^Sandbox_Charm_Nest_Rev/.test(x)), 'no counter document in the sandbox');
  void sbBefore;
  // 6. a failed read of the kept answer is never an error of the op
  const real = m.db.collection; let broke = 0;
  m.db.collection = function (name) { const c = real.call(this, name); if (name === 'Charm_Nest_Rev') { return { doc() { broke++; return { get: async () => { throw new Error('boom'); }, set: async () => { throw new Error('boom'); } }; } }; } return c; };
  try { ok('6 a failing counter document still answers', await countOnly(), truth()); } finally { m.db.collection = real; }
  assert.ok(broke >= 1);
  process.stdout.write('done counts OK\n');
  m.uninstall(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
