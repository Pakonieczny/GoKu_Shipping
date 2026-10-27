// KNOWN SPLIT (Paul, 27 Oct 2026, of 14K Sheet 1 held back at Order check by "Pair split from GF Sheet 1" for orders 4206987902 and 4210212342, whose 14K pieces were on it:
// "I cannot have false positives and false negatives in this platform"). A Rose Gold / 10K / 14K sheet is in ONE set and ties no two sets (Paul, 26 Oct 2026): an order with pieces on
// a green-line sheet and on a GF / SS sheet of another set is split by design, each side cut with its own set. CharmNestReadiness (page and server, one file):
//   1  Paul's screenshot: 14K Sheet 1 (Set 2) with the 14K hoops / the 14K map, GF Sheet 1 (Set 1, its engravings not approved) with the GF pairs: no order issue on 14K Sheet 1, its
//      Order check is done; the Order check of GF Sheet 1 says nothing about 14K Sheet 1 either, the other way round
//   2  a split between a GF sheet and an SS sheet of two sets stays the real issue it is ("Split from SS Sheet 1")
//   3  a green-line sheet in NO set keeps its wait (nothing is decided for a sheet that has no set yet), and a mate of the SAME set stays the set's wait as before
//   4  a report saved before the block carried its metal is read the same (the asking sheet's metal, or the other sheet's name)
//   node tests/charm-nest/green-split-orders.cjs      Offline: plain objects, no browser, no network.
'use strict';
const assert = require('node:assert/strict');
const R = require('../../charm-nest-readiness.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ✓ ' + m); };
const back = (id, sheetId) => ({ poolId: id, sheetId, approvedAt: 10, approvedBy: 'Paul', verified: { geometry: { ok: true }, file: { ok: true } }, outputs: { ai: { path: id + '.ai', url: 'https://example.com/' + id + '.ai' } } });
function sheet(id, metal, pool, { approved = true, setId = 'set1', draft = false } = {}) {
  const orders = [...new Set(pool.map(p => p.split('_')[0]))];
  return { id, metal, setId: draft ? null : setId, draft, setSeq: +(String(setId).match(/\d+$/) || [1])[0], runId: 'run1', sheetIndex: 1, status: 'complete', poolIds: pool, placedCount: pool.length, verification: { ok: true }, preview: 'https://example.com/p.png', outputs: { ai: 'https://example.com/f.ai' },
    orders, label: { files: [{ path: 'qr.png', url: 'https://example.com/qr.png', payload: 'x', orders }] }, backPool: approved ? pool.map(p => back(p, id)) : [],
    engraving: Object.fromEntries(pool.map(p => [p, approved ? { needed: true, state: 'written', approved: true } : { needed: true, state: 'review', approved: false }])) };
}
function row(order, txn, q, title) {
  const key = `${order}_${txn}`;
  return { key, order: { receiptId: order, buyer: { name: 'Customer ' + order } }, line: { title, listingId: 'L' + order, transactionId: txn }, state: 'written', poolIds: [...Array(q)].map((_, i) => `${key}_${i + 1}`), spec: { quantity: q, designSku: title, noDesign: false }, problems: [], hold: null, changePending: false };
}
const A = '4206987902', B = '4210212342';
// Elliott Jensen: HUGGIE HOOPS- STAR OF DAVID (14K, a pair) + DAVID STAR (HUGGIE) (GF, a pair); Aseel Quraishi: PALESTINE MAP (14K) + HUGGIE HOOPS- PALESTINE (GF, a pair)
const rows = [row(A, '1', 2, 'HUGGIE HOOPS- STAR OF DAVID'), row(A, '2', 2, 'DAVID STAR (HUGGIE)'), row(B, '1', 1, 'PALESTINE MAP'), row(B, '2', 2, 'HUGGIE HOOPS- PALESTINE')];
const k14Pool = [`${A}_1_1`, `${A}_1_2`, `${B}_1_1`], gfPool = [`${A}_2_1`, `${A}_2_2`, `${B}_2_1`, `${B}_2_2`];
const orderIssues = (s, all) => R.issues(s, { rows, allSheets: all }).filter(i => i.step === 'orders' && i.orderId);

// 1 · Paul's screenshot
{
  const k14 = sheet('k14', 'gold14k', k14Pool, { setId: 'set2' }), gf = sheet('gf', 'gold', gfPool, { setId: 'set1', approved: false });
  const reps = R.orderReports(rows, [k14, gf]);
  ok(!reps[A].ready && reps[A].blocks.every(b => b.key === 'otherSheetNotReady' && b.green !== true), 'the whole-order reading still knows the GF pairs sit on a sheet that is not ready (GF: no green mark)');
  ok(orderIssues(k14, [k14, gf]).length === 0, '14K Sheet 1 lists no order issue: its orders\' GF pieces are in another set by design (no "Pair split from GF Sheet 1")');
  const read = { ...k14, orderReadiness: Object.fromEntries(R.orderIds(k14).map(o => [o, R.forSheet(reps[o], 'k14', R.setOf(k14), 'gold14k')])) };
  ok(R.sheet(read).stages.orders === true, 'its Order check is done (the step is no longer red)');
  ok(R.issues(k14, { rows, allSheets: [k14, gf] }).every(i => i.key !== 'otherSheetNotReady'), 'and nothing else of it names GF Sheet 1');
  // the other way round: 14K Sheet 1 not ready, GF Sheet 1 asks
  const k14b = sheet('k14', 'gold14k', k14Pool, { setId: 'set2', approved: false }), gfb = sheet('gf', 'gold', gfPool, { setId: 'set1' });
  const reps2 = R.orderReports(rows, [k14b, gfb]);
  ok(reps2[A].blocks.some(b => b.green === true), 'a piece on a not-ready 14K sheet carries its green mark');
  ok(orderIssues(gfb, [k14b, gfb]).length === 0, 'GF Sheet 1 lists no order issue for the 14K pieces in another set');
}
// 2 · GF and SS of two sets: the real split stays
{
  const gf = sheet('gf', 'gold', [`${A}_1_1`, `${A}_1_2`], { setId: 'set1' }), ss = sheet('ss', 'silver', [`${A}_2_1`, `${A}_2_2`], { setId: 'set2', approved: false });
  const rows2 = [row(A, '1', 2, 'GF hoops'), row(A, '2', 2, 'SS charm')];
  const is = R.issues(gf, { rows: rows2, allSheets: [gf, ss] }).filter(i => i.step === 'orders' && i.orderId);
  ok(is.length === 1 && is[0].key === 'otherSheetNotReady' && is[0].split === true && is[0].pieces[0].sheetLabel === 'SS Sheet 1', 'a GF / SS order split between two sets is still the real issue: ' + JSON.stringify(is.map(i => [i.key, i.split, i.pieces[0].sheetLabel])));
}
// 3 · no set, same set
{
  const k14 = sheet('k14', 'gold14k', k14Pool, { draft: true }), gf = sheet('gf', 'gold', gfPool, { setId: 'set1', approved: false });
  ok(orderIssues(k14, [k14, gf]).length === 2, 'a 14K sheet in no set keeps its wait for the GF pieces (nothing is decided for a sheet with no set)');
  const k14s = sheet('k14', 'gold14k', k14Pool, { setId: 'set1' });
  ok(orderIssues(k14s, [k14s, gf]).length === 0, 'in the SAME set it is the set\'s wait, as before (said under Approve)');
}
// 4 · a report saved before the green mark
{
  const k14 = sheet('k14', 'gold14k', k14Pool, { setId: 'set2', approved: false }), gf = sheet('gf', 'gold', gfPool, { setId: 'set1', approved: false });
  const old = JSON.parse(JSON.stringify(R.orderReports(rows, [k14, gf]))); for (const r of Object.values(old)) for (const b of r.blocks || []) delete b.green;
  ok(R.forSheet(old[A], 'k14', 'set2', 'gold14k').ready === true, 'an old report read by the 14K sheet: its own metal says it is a known split');
  ok(R.forSheet(old[A], 'gf', 'set1', 'gold').ready === true, 'read by the GF sheet: the other sheet\'s name ("14K Sheet 1") says it');
  ok(R.forSheet(old[A], 'gf', 'set1').ready === true, 'and without the asking metal too');
}
console.log(`green-split-orders: ${n} checks OK`);
