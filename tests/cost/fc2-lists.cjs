// The Library's lists read what decides their rows first and the rows they answer with after (FC2): same answers as reading every
// record whole, a fraction of the bytes. Real handler over the meter's in-memory Firestore.
//   node tests/cost/fc2-lists.cjs
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs');
const W = path.join(__dirname, '..', '..');
const meter = require('./meter.cjs');
process.env.N_DONE_SHEETS = '600';
// the fixture of the cost table (tests/cost/fc2-library-cost.cjs), without running it
const src = fs.readFileSync(path.join(__dirname, 'fc2-library-cost.cjs'), 'utf8').replace(/\(async \(\) => \{[\s\S]*$/, 'module.exports = { build, DAY };');
const tmp = path.join(__dirname, '.fc2-build.tmp.cjs');
fs.writeFileSync(tmp, src);
const { build } = require(tmp); fs.unlinkSync(tmp);

(async () => {
  const m = meter.create(); m.install(); m.db.seed(build());
  const docs = m.db.docs;
  // variety: a sheet of a completed set that is not marked itself, an archived sheet, a draft, another metal, a set marked complete by hand, a set with a completion day
  docs.get('Charm_Nest_Sheets/sheet-d599').laserDoneAt = undefined; docs.get('Charm_Nest_Sheets/sheet-d598').archived = true; docs.get('Charm_Nest_Sheets/sheet-c1-2').draft = true;
  docs.get('Charm_Nest_Sheets/sheet-c2-1').metal = 'silver'; docs.get('Charm_Nest_Sets/set-cur-3').status = 'completed'; docs.get('Charm_Nest_Sets/set-cur-4').completionDay = '2026-10-05';
  // a legacy run record that holds its lines and holds itself, a newer one that keeps them in parts
  docs.set('Charm_Nest_Runs/run-legacy', { runId: 'run-legacy', day: '2026-10-02', step: 'review', status: 'paused', lines: { a_1: {}, b_1: {}, c_1: {} }, holds: { x: 1 }, errors: [{ why: 'e' }], updatedAt: 1.8e12, createdAt: 1, huge: 'z'.repeat(50000), sheets: { s1: {} } });
  const fn = require(path.join(W, 'netlify/functions/charmNestLibrary.js'));
  const post = async body => { const r = await fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) }); assert.strictEqual(r.statusCode, 200, r.body.slice(0, 300)); return JSON.parse(r.body); };
  const bytesOf = async f => { const a = m.snapshot(); const out = await f(); const d = m.since(a); return { out, reads: d.reads + d.aggs, bytes: d.bytes }; };
  const ok = s => process.stdout.write('ok   ' + s + '\n');

  // 1. Current list = the full read, filtered by the same rule, in the same order
  const cur = await bytesOf(() => post({ op: 'listSheets', limit: 300, excludeDone: true }));
  const whole = await bytesOf(() => post({ op: 'listSheets', limit: 300 }));
  const filed = s => s.laserDoneAt > 0 && !s.laserSetPending;
  const expected = whole.out.sheets.filter(s => !filed(s));
  assert.ok(expected.length > 20 && expected.length < whole.out.sheets.length / 2, 'the fixture has done and current sheets: ' + expected.length + ' of ' + whole.out.sheets.length);
  assert.deepStrictEqual(cur.out.sheets, expected); ok(`listSheets Current: ${expected.length} of ${whole.out.sheets.length} sheets, the same entries in the same order`);
  assert.ok(!cur.out.sheets.some(s => s.id === 'sheet-d598') && !whole.out.sheets.some(s => s.id === 'sheet-d598'), 'an archived sheet is in no list');
  assert.ok(cur.out.sheets.some(s => s.id === 'sheet-d599'), 'a sheet of a completed set that is not marked itself is listed');
  process.stdout.write(`     Current read ${cur.reads} reads ${cur.bytes} bytes; the whole read of the same newest sheets ${whole.reads} reads ${whole.bytes} bytes\n`);
  assert.ok(cur.bytes < whole.bytes / 3, 'Current reads a fraction of the bytes');
  const silver = await post({ op: 'listSheets', limit: 300, excludeDone: true, metal: 'silver' });
  assert.deepStrictEqual(silver.sheets, expected.filter(s => s.metal === 'silver')); ok('listSheets Current with a metal');
  const part = await post({ op: 'listSheets', limit: 300, excludeDone: true });
  assert.strictEqual(part.next, null); ok('no part left');

  // 2. setList: the sets it answers with are the stored records, whole, the open ones when excludeDone
  const sl = await bytesOf(() => post({ op: 'setList', includeSheets: true, limit: 200, excludeDone: true }));
  const open = [...docs.entries()].filter(([k, v]) => /^Charm_Nest_Sets\//.test(k) && !(+v.laserDoneAt > 0));
  assert.deepStrictEqual(new Set(sl.out.sets.map(s => s.setId)), new Set(open.map(([k]) => k.split('/')[1])));
  for (const s of sl.out.sets) { const stored = docs.get('Charm_Nest_Sets/' + s.setId); assert.deepStrictEqual(s.orders, stored.orders); assert.deepStrictEqual(s.sheetIds, stored.sheetIds); assert.strictEqual(s.runId, stored.runId); }
  assert.ok(sl.out.sheets.length >= 40, 'their sheets come with them: ' + sl.out.sheets.length);
  ok(`setList: ${sl.out.sets.length} open sets, whole, with ${sl.out.sheets.length} sheets`);
  const all50 = await post({ op: 'setList', limit: 50 });
  assert.strictEqual(all50.sets.length, 50); assert.ok(all50.sets.every(s => s.orders && s.sheetIds), 'every set comes whole');
  const done = await post({ op: 'setList', limit: 30, status: 'complete' });
  assert.ok(done.sets.length === 30 && done.sets.every(s => s.status === 'complete')); ok('setList with a status and without excludeDone');
  process.stdout.write(`     setList read ${sl.reads} reads ${sl.bytes} bytes\n`);

  // 3. runList: the same rows from the fields a row is made of
  const rl = await bytesOf(() => post({ op: 'runList', limit: 200 }));
  const legacy = rl.out.runs.find(r => r.runId === 'run-legacy');
  assert.deepStrictEqual({ lines: legacy.lines, holds: legacy.holds, errors: legacy.errors, status: legacy.status, step: legacy.step }, { lines: 3, holds: 1, errors: 1, status: 'paused', step: 'review' });
  const live = rl.out.runs.find(r => r.runId === 'run-open-0'); assert.strictEqual(live.lines, 300);
  const olds = rl.out.runs.find(r => r.runId === 'run-old-299'); assert.strictEqual(olds.lines, 80);
  assert.ok(rl.bytes < 120000, 'runList no longer reads the whole records: ' + rl.bytes); ok(`runList: counts of lines, holds and errors as before, ${rl.reads} reads ${rl.bytes} bytes`);

  // 4. ping: the calibration rows are read once per instance for a while, the counts every time
  const p1 = await bytesOf(() => post({ op: 'ping' })), p2 = await bytesOf(() => post({ op: 'ping' }));
  assert.ok(p1.out.calibration.length === 200 && p2.out.calibration.length === 200 && p1.reads > 200 && p2.reads < 20, `ping ${p1.reads} then ${p2.reads} reads`);
  assert.deepStrictEqual(p2.out, p1.out); ok(`ping: ${p1.reads} reads, then ${p2.reads}`);
  await post({ op: 'putCalibration', row: { sheetId: 'sheet-new', metal: 'gold', count: 30, density: 0.7, cv: 0.2, largestFrac: 0.1, placedAll: true } });
  const p3 = await post({ op: 'ping' }); assert.strictEqual(p3.calibration[0].sheetId, 'sheet-new'); ok('a calibration row written here shows at the next ping');
  const p4 = await post({ op: 'ping', calibration: false }); assert.ok(!p4.calibration && p4.sheets > 0 && p4.charms === 3000); ok('ping without calibration');
  process.stdout.write('lists OK\n'); m.uninstall(); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
