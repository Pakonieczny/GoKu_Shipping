// KEYONE (flag-fix-1011): an EARRING PAIR is exactly what it was. "One charm of a design that draws more than one body" is for lines that are not an earring pair (key-one.cjs); this proves the
// earring side did not move. The same scenarios (earring pairs matching and mismatched, a single earring that names its ear, a necklace of a one-body design and of two different charms) were run
// on the app BEFORE the change (the checkout of main at 5b9b8a6) and their pool records, sheet charms and notes kept in key-ears-same.golden.json; this runs them on this checkout and compares.
// Offline: no browser, no network.   node tests/charm-nest/key-ears-same.cjs          (to write the golden from another checkout: node key-ears-same.cjs --write <checkout>)
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const here = path.join(__dirname, '../..'), goldenFile = path.join(__dirname, 'key-ears-same.golden.json');
const J = x => (x === undefined ? null : JSON.parse(JSON.stringify(x)));
async function scenarios(root) {
  const K = require('./_keyWorld.cjs')(root), out = {};
  const cases = [
    // [name, design, title, extra spec, quantity, form]
    ['mismatched pair of two keys, unequal', 'KEYS_UNEQUAL', 'Key Stud Earrings', 1, 'earrings'],
    ['mismatched pair of two keys x2', 'KEYS_UNEQUAL', 'Key Stud Earrings', 2, 'earrings'],
    ['mismatched pair, two keys of one shape', 'KEY_5598', 'Key Stud Earrings', 1, 'earrings'],
    ['mismatched pair, a ball and a racket', 'TENNIS_MIS', 'Tennis Ball and Racket Earrings', 1, 'earrings'],
    ['single earring naming the Right ear', 'KEYS_UNEQUAL', 'Single Key Earring, Right Ear', 1, 'single'],
    ['matching stud pair', 'PAIR_STUD', 'Star Stud Earrings', 1, 'earrings'],
    ['matching hoop pair x2', 'PAIR_HOOP', 'Hoop Earrings', 2, 'earrings'],
    ['necklace of a one-body design', 'ONE_PENDANT', 'Pendant Necklace', 1, 'necklace'],
    ['necklace of a ball and a racket', 'TENNIS_MIS', 'Tennis Ball and Racket Necklace', 1, 'necklace'],
  ];
  let n = 0;
  for (const [name, sku, title, qty, form] of cases) {
    const w = K.world([sku]); w.use(sku); n++;
    const rid = String(4196000000 + n), tid = String(5000000000 + n);
    const sp0 = K.specOf(rid, tid, sku, title, [], w.entries[sku]);
    const row = K.lineRow(rid, tid, sku, qty, { pair: sp0.pair, pieceCount: sp0.pieceCount * qty, form: sp0.form || form });
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut');
    out[name] = {
      pools: put ? J(put.body.pools) : null, state: row.state, poolIds: J(row.poolIds), notes: J(row.spec.pair.notes), glued: !!row.spec.pair.glued, mismatched: !!row.spec.pair.mismatched,
      charms: w.pages.gold.charms.map(c => ({ id: c.id, name: c.name, side: c.side, mirror: c.mirror, bodyIndex: c.bodyIndex, groupKey: c.groupKey, groupSize: c.groupSize, members: c.members.length, bbox: J(c.bbox), hash: c.hash, mirrored: !!(c.outline && c.outline.mirrored), topIndices: J(c.topIndices) })),
      logs: w.logs.slice(),
    };
  }
  return out;
}
module.exports = { scenarios };
if (require.main === module) (async () => {
  if (process.argv[2] === '--write') { const g = await scenarios(path.resolve(process.argv[3])); fs.writeFileSync(goldenFile, JSON.stringify(g, null, 1) + '\n'); console.log('golden written:', Object.keys(g).length, 'scenarios'); return; }
  const golden = JSON.parse(fs.readFileSync(goldenFile, 'utf8')), now = J(await scenarios(here));
  assert.deepStrictEqual(Object.keys(now), Object.keys(golden));
  for (const k of Object.keys(golden)) { assert.deepStrictEqual(now[k], golden[k], 'unchanged: ' + k); console.log('  ✓', k); }
  assert.strictEqual(golden['mismatched pair of two keys, unequal'].pools.length, 2, 'the golden is a real earring pair (a Left and a Right)');
  assert.strictEqual(golden['matching stud pair'].pools.length, 2); assert.strictEqual(golden['matching hoop pair x2'].pools.length, 4);
  console.log('key-ears-same: ok');
})().catch(e => { console.error(e); process.exit(1); });
