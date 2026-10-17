// COST (Oct 2026): the whole list of hand-finished Custom Orders (charmNestLibrary customGet with no key, keys or since: every sorter
// tab at load and every 30 minutes, Orders.loadMaps) read 180 days of records, up to 1000, at every call. A warm instance now keeps the
// list it read and tops it up from the changes feed (updatedAtMs >= what it held less a margin): the same answer, at about one read.
// Offline: FC1's meter over its in-memory Firestore, the REAL handler, fixtures only.
//   1 · the first call reads the whole list; the next ones read only what changed (an empty query is 1 read)
//   2 · the answer is the one a fresh read gives: a new record, a changed one (reopen), the order newest first, the 1000 cap and truncated
//   3 · the sandbox never uses the memo (Reset deletes its records), nor a call with its own `days`
//   COST_REPORT=1 node tests/cost/custom-list-memo.cjs   (prints the reads; CUSTOM_MEMO=0 skips the warm budgets, to measure the code before the memo)
'use strict';
const assert = require('assert'), path = require('path');
const meter = require('./meter.cjs');
const W = path.join(__dirname, '..', '..');
const N = +process.env.N_CUSTOM || 600, DAY = 86400000;
const post = (fn, body) => fn.handler({ httpMethod: 'POST', headers: {}, queryStringParameters: {}, body: JSON.stringify(body) });
const say = s => process.stdout.write(s + '\n');

(async () => {
  const realNow = Date.now; let NOW = Date.UTC(2026, 9, 17, 15, 0, 0); Date.now = () => NOW;
  const m = meter.create(); m.install();
  const docs = {};
  for (let i = 0; i < N; i++) {
    const rid = String(4100000000 + i), key = rid + '_' + rid + '1', at = NOW - (i * 170 + 5) * 60000;   // spread over ~70 days
    docs['Charm_Custom_Orders/' + key] = { key, receiptId: rid, transactionId: rid + '1', sku: 'RE_' + i, title: 'Rework ' + i, category: 'Custom', kind: 'custom', state: 'completed', completedAt: at, completedBy: 'Ana', how: 'button', stamps: [{ how: 'button', at, by: 'Ana' }], updatedAtMs: at, hasLabel: false, label: '{"big":"' + 'x'.repeat(400) + '"}' };
  }
  docs['Charm_Custom_Orders/old_1'] = { key: 'old_1', receiptId: '1', state: 'completed', updatedAtMs: NOW - 200 * DAY };   // older than 180 days: never listed
  docs['Sandbox_Charm_Custom_Orders/4100000000_41000000001'] = { key: '4100000000_41000000001', receiptId: '4100000000', state: 'completed', updatedAtMs: NOW - 60000 };
  m.db.seed(docs);
  const fn = require(path.join(W, 'netlify/functions/charmNestLibrary.js'));
  const list = async (body, label) => { const a = m.snapshot(); const r = await m.op(label, () => post(fn, Object.assign({ op: 'customGet' }, body || {}))); const d = m.since(a); assert.equal(r.statusCode, 200, label + ': ' + r.body.slice(0, 200)); return { j: JSON.parse(r.body), reads: d.reads, bytes: d.bytes }; };
  const keysOf = j => Object.keys(j.records);
  const first = await list(null, 'customGet list (cold)');
  assert.equal(keysOf(first.j).length, N, 'every record of the last 180 days, none older');
  assert(!('old_1' in first.j.records), 'a record older than 180 days is not listed');
  assert(!first.j.records[keysOf(first.j)[0]].label, 'no sticker in the list');
  assert.equal(first.j.truncated, false);
  NOW += 30 * 60000;
  const warm = await list(null, 'customGet list (warm, nothing changed)');
  assert.deepEqual(warm.j.records, first.j.records, 'the same records');
  assert.deepEqual(keysOf(warm.j), keysOf(first.j), 'in the same order (newest first)');
  assert(warm.j.at === NOW, 'the server time of this answer');
  // a completion and a reopen written meanwhile (through the real ops) are in the next answer
  NOW += 1000;
  let r = await post(fn, { op: 'customPut', key: '4199999999_41999999991', receiptId: '4199999999', transactionId: '41999999991', sku: 'NEW', title: 'New', how: 'button', by: 'Bo' }); assert.equal(r.statusCode, 200, r.body);
  NOW += 1000;
  r = await post(fn, { op: 'customReopen', key: '4100000005_41000000051', by: 'Bo' }); assert.equal(r.statusCode, 200, r.body);
  NOW += 1000;
  const after = await list(null, 'customGet list (warm, 2 changed)');
  assert.equal(keysOf(after.j).length, N + 1, 'the new record joins');
  assert.equal(keysOf(after.j)[0], '4100000005_41000000051', 'newest first: the reopened one');
  assert.equal(after.j.records['4100000005_41000000051'].state, 'open', 'the reopen is in the list');
  assert.equal(after.j.records['4199999999_41999999991'].state, 'completed');
  // the memo's answer equals a fresh read (the `days` form skips the memo and reads the store)
  const cold = await list({ days: 180 }, 'customGet list days:180 (never memo)');
  assert.deepEqual(after.j.records, cold.j.records, 'the memo answers what a fresh read answers');
  assert.deepEqual(keysOf(after.j), keysOf(cold.j), 'in the same order');
  assert(cold.reads >= N, 'an explicit days reads the store: ' + cold.reads);
  // the sandbox reads its own store every time
  const sb1 = await list({ sandbox: true }, 'sandbox list'), sb2 = await list({ sandbox: true }, 'sandbox list again');
  assert.deepEqual(keysOf(sb1.j), ['4100000000_41000000001']); assert.equal(sb2.reads, sb1.reads, 'the sandbox is read afresh each time');
  if (process.env.COST_REPORT) for (const x of [['cold', first], ['warm, nothing changed', warm], ['warm, 2 changed', after], ['days:180 (fresh read)', cold]]) say(`  customGet list ${x[0]}: ${x[1].reads} reads, ${x[1].bytes} bytes`);
  assert(first.reads >= N, 'the first call reads the list: ' + first.reads);
  if (process.env.CUSTOM_MEMO !== '0') {
    assert(warm.reads <= 2, 'a warm call reads only the changes (none): ' + warm.reads);
    assert(after.reads <= 4, 'a warm call reads only the changes (2): ' + after.reads);
  }
  m.uninstall(); Date.now = realNow;
  say('custom-list-memo: ok (cold ' + first.reads + ' reads, warm ' + warm.reads + ', warm with 2 changes ' + after.reads + ')');
})().catch(e => { console.error(e); process.exit(1); });
