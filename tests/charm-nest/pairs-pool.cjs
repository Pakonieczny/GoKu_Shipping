// PAIRPOOL (pairs-1009, area 4): the pool and piece records for pairs, mismatched pairs and multi-piece lines. Offline: no browser, no network.
//   node tests/charm-nest/pairs-pool.cjs
// 1 · charm-nest-pool-pieces.js (pure): the four fields, old records derived, groups, legacy glued
// 2 · the bridge's Pool (the real source, run in a vm with small stubs): makePool through poolAdd for a single charm, a matching pair, discs
//     (records byte-identical to what the pre-pairs code wrote) and for a mismatched pair (two pieces per unit, each its own body)
// 3 · the real netlify function over the fake Firestore (no-nested-arrays check on every stored document): poolPut sanitises, the legacy
//     guard refuses to half-migrate a glued line, getOrderPieces says a split pair
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const F = require('./pairs-fixtures.cjs');
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const PP = require(path.join(root, 'charm-nest-pool-pieces.js'));
const O = require(path.join(root, 'charm-nest-orders.js'));
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const pass = name => console.log('  ✓', name);
const J = x => JSON.parse(JSON.stringify(x));      // the vm's arrays and objects come from another realm: compare by value

(async () => {
  /* ── 1 · the module ── */
  {
    assert.deepStrictEqual(PP.parsePoolId('4190000001_5000000010_2'), { receiptId: '4190000001', transactionId: '5000000010', copy: 2, lineKey: '4190000001_5000000010', groupKey: '4190000001:5000000010' });
    assert.strictEqual(PP.parsePoolId('nonsense'), null);
    const good = { side: 'L', bodyIndex: 0, groupKey: '4190000001:5000000010', groupSize: 2 };
    assert.deepStrictEqual(PP.cleanFields(good), good);
    for (const bad of [{ ...good, side: 'X' }, { ...good, bodyIndex: 1.5 }, { ...good, groupSize: 1 }, { ...good, groupSize: 401 }, { ...good, groupKey: 'abc' }, { side: 'L' }, { groupKey: good.groupKey, groupSize: 2 }, null, 5])
      assert.deepStrictEqual(PP.cleanFields(bad), {}, 'all four valid or none: ' + JSON.stringify(bad));
    assert.deepStrictEqual(PP.fieldsOf({ side: 'R', bodyIndex: 1, groupKey: good.groupKey, n: 2, of: 4 }), { side: 'R', bodyIndex: 1, groupKey: good.groupKey, groupSize: 4 }, 'a piecesFor piece (of) becomes the record fields');
    assert.deepStrictEqual(PP.fieldsOf({ side: null, bodyIndex: 0, groupKey: good.groupKey, n: 1, of: 2 }), {}, 'a matching piece carries none');
    assert.deepStrictEqual(PP.sheetCharmFields({ id: 'x', side: 'L', bodyIndex: 0, groupKey: good.groupKey, groupSize: 2, hash: 'h' }), good);
    assert.deepStrictEqual(PP.sheetCharmFields({ id: 'x', hash: 'h' }), {});
    // an old row: derived, never glued unless the design is a mismatched one
    const old = { poolId: '4190000001_5000000010_1', orderId: '4190000001', quantity: 2, copy: 1 };
    assert.deepStrictEqual(PP.metaOf(old), { poolId: old.poolId, groupKey: '4190000001:5000000010', n: 1, groupSize: 2, side: null, bodyIndex: 0, unit: 1, kind: 'pair', glued: false, legacy: false });
    assert.strictEqual(PP.metaOf({ poolId: '4190000001_5000000010_1', quantity: 1 }).kind, 'single');
    assert.strictEqual(PP.metaOf({ poolId: '4190000001_5000000010_1', quantity: 3 }).kind, 'multi');
    const glued = PP.metaOf(old, { mismatchedDesign: true });
    assert.strictEqual(glued.kind, 'glued'); assert.strictEqual(glued.side, null); assert.strictEqual(glued.glued, true);
    const right = PP.metaOf({ poolId: '4190000001_5000000010_4', ...good, side: 'R', bodyIndex: 1, groupSize: 4 });
    assert.deepStrictEqual([right.side, right.bodyIndex, right.n, right.unit, right.groupSize, right.kind], ['R', 1, 4, 2, 4, 'multi']);
    assert.strictEqual(PP.metaOf('4190000001_5000000010_2').groupKey, '4190000001:5000000010', 'a bare pool id works');
    // legacy glued: no side anywhere and a piece live on a sheet
    assert.strictEqual(PP.legacyGlued([{ poolId: 'a', state: 'written', sheetId: 's1' }]), true);
    assert.strictEqual(PP.legacyGlued([{ poolId: 'a', state: 'abandoned', sheetId: null }]), false, 'a line taken off may be made up again as two pieces');
    assert.strictEqual(PP.legacyGlued([{ poolId: 'a', state: 'ready' }]), false, 'not on a sheet yet: free to be two pieces');
    assert.strictEqual(PP.legacyGlued([{ poolId: 'a', state: 'written', sheetId: 's1', ...good }]), false, 'a row that has a side is not an old one');
    // groups
    const rows = [{ poolId: '4190000001_5000000010_1', ...good, groupSize: 2 }, { poolId: '4190000001_5000000010_2', ...good, side: 'R', bodyIndex: 1 }, { poolId: '4190000002_5000000020_1', quantity: 1 }];
    const where = { '4190000001_5000000010_1': 'sA', '4190000001_5000000010_2': 'sB', '4190000002_5000000020_1': 'sA' };
    let g = PP.groupsOf(rows, r => where[r.poolId]);
    assert.strictEqual(g.length, 1, 'a single piece is no group of two'); assert.strictEqual(g[0].split, true); assert.strictEqual(g[0].sided, true); assert.deepStrictEqual(Object.keys(g[0].sheets).sort(), ['sA', 'sB']);
    where['4190000001_5000000010_2'] = 'sA'; g = PP.groupsOf(rows, r => where[r.poolId]); assert.strictEqual(g[0].split, false, 'both on one sheet: whole');
    where['4190000001_5000000010_2'] = null; g = PP.groupsOf(rows, r => where[r.poolId]); assert.strictEqual(g[0].split, true, 'one on a sheet, one waiting: split'); assert.deepStrictEqual(g[0].off, ['4190000001_5000000010_2']);
    g = PP.groupsOf([rows[0]], r => 'sA'); assert.strictEqual(g[0].missing, 1); assert.strictEqual(g[0].split, true, 'a piece that was never made: the group is not whole');
    pass('the piece fields: all four or none, old records derived, groups and legacy glued');
  }

  /* ── 2 · the bridge's Pool, real source, small stubs ── */
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const a = src.indexOf('const Pool = window.Pool = (() => {'), b = src.indexOf('\n/* Carry-forward is keyed', a);
  assert(a > 0 && b > a, 'the Pool module is where the test expects it');
  const MM = 25.4 / 72;
  function world(designs, rowsSeed) {
    const logs = [], calls = [], pages = { gold: { metal: 'gold', charms: [], placements: [], status: 'idle', sheetId: null } };
    const entries = {};
    for (const sku of designs) entries[sku] = Object.assign(F.entryOf(sku), { sku, aiPath: 'charmnest/master/sku/' + sku + '.ai', aiUrl: 'https://x/' + sku, masterHash: 'mh-' + sku, upAngle: 0 });
    const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, '0'); };
    const c = {
      console, Date, JSON, Map, Set, Math, Number, String, Array, Object, Promise, Error, Uint8Array, setTimeout, clearTimeout, performance: { now: () => Date.now() }, structuredClone,
      window: { CharmNestPair: Pair, CharmNestPoolPieces: PP }, O, MM, labelOf: m => m, stockFor: () => ({ wPt: 1400, hPt: 700 }),
      S: { cloud: { ok: true }, settings: { insetPt: 0, maxFill: .8, silhouetteRes: 6, minPt: 6 }, poolSources: {}, sheets: { gold: { active: 0 } } },
      B: { pool: { rows: new Map(rowsSeed || []), sources: new Map() }, orders: { byKey: new Map(), rows: [] }, run: null },
      agent: (...x) => { logs.push(x.slice(1).join(' ')); }, api: async (fn, body) => { calls.push({ fn, body: JSON.parse(JSON.stringify(body)) }); refuseNestedArrays(body, 'poolPut'); return {}; },
      Master: { entryFor: sku => entries[sku] || null, fetchEntry: async () => null, keepOutOf: () => [], skuRegex: () => /^$/ },
      CharmNestAssets: { bytes: async () => new Uint8Array(1) },
      P: {
        parseSource: async () => ({}), groupCharmsAsync: null, integrateRings: () => ({ left: [], welded: 0 }), parseSkuLabel: () => null,
        buildSilhouettes: async (_p, charms) => { for (const ch of charms) { const w = ch.bbox[2] - ch.bbox[0], h = ch.bbox[3] - ch.bbox[1]; Object.assign(ch, { bits: new Uint8Array(4), w: 2, h: 2, scale: 6, areaPt2: Math.round(w * h * .8), widthPt: w, heightPt: h, centerPt: [(ch.bbox[0] + ch.bbox[2]) / 2, (ch.bbox[1] + ch.bbox[3]) / 2], thumb: 't', hash: fnv(JSON.stringify(ch.bbox) + ch.members.length + (ch.outline.id || '')) }); } return charms; }
      },
      allSheets: () => Object.values(pages), pagesOf: m => [pages[m]], sheetDirty: () => {}, renderCard: () => {}, Orders: { rows: () => [], interpretAll() {}, render() {} },
      CN: {}, Gate: {}, Review: {}, RunCtl: {}, Cleanups: null, CustomPrint: null, CustomSheet: null, LiveNest: null, addPage: () => { throw new Error('no new page expected'); }
    };
    c.window.Cleanups = null;
    vm.createContext(c);
    // each master read gives the same drawing, but a fresh object (the Pool writes into it)
    c.P.groupCharmsAsync = async () => ({ charms: [structuredClone(F.charmOf(c.__sku))], orphans: [] });
    const realLoad = null; void realLoad;
    vm.runInContext(src.slice(a, b), c);
    return { c, logs, calls, pages, Pool: c.window.Pool, entries, use(sku) { c.__sku = sku; } };
  }
  const lineRow = (rid, tid, sku, qty, extra) => ({ key: `${rid}_${tid}`, state: 'pulled', order: { receiptId: rid, createTs: 1791500000, updateTs: 1791500001 }, line: { transactionId: tid, listingId: '1', sku }, spec: Object.assign({ designSku: sku, material: 'gold', quantity: qty, size: null, form: 'earrings', chain: null }, extra || {}), problems: [], poolIds: [], engrave: null, arrivedAt: 0 });
  // the record the code before pairs wrote for a plain line (same keys, same order)
  const was = (rid, tid, sku, copy, q, charmHash) => JSON.stringify({ poolId: `${rid}_${tid}_${copy}`, runId: null, setId: null, sheetId: null, orderId: rid, orderDate: 1791500000, arrivedAt: 0, transactionId: tid, sku, material: 'gold', size: null, form: 'earrings', chain: null, copy, quantity: q, charmHash, masterHash: 'mh-' + sku, aiPath: 'charmnest/master/sku/' + sku + '.ai', engrave: false, state: 'ready', lineKey: `${rid}_${tid}`, updateTs: 1791500001 });

  // plain lines: single charm, matching pair (two copies), discs: byte-identical, and no pair field anywhere
  for (const [sku, qty] of [['ONE-PENDANT', 1], ['PAIR-STUD', 2], ['DISC-14', 1], ['PAIR-HOOP', 3]]) {
    const w = world([sku]); w.use(sku);
    const row = lineRow('4190000101', '5000000101', sku, qty);
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'); assert(put, sku + ': recorded');
    assert.strictEqual(put.body.pools.length, qty, sku + ': quantity pieces, as before');
    const charmHash = put.body.pools[0].charmHash;
    put.body.pools.forEach((p, i) => assert.strictEqual(JSON.stringify(p), was('4190000101', '5000000101', sku, i + 1, qty, charmHash), sku + ' copy ' + (i + 1) + ': the record is byte-identical to the one written before pairs'));
    assert(!put.body.pools.some(p => 'side' in p || 'bodyIndex' in p || 'groupKey' in p || 'groupSize' in p));
    assert.deepStrictEqual(J(row.poolIds), put.body.pools.map(p => p.poolId));
    const charms = w.pages.gold.charms; assert.strictEqual(charms.length, qty);
    for (const ch of charms) { assert(!('side' in ch) && !('groupKey' in ch), sku + ': no pair field on a plain charm'); assert.strictEqual(ch.orderInfo.quantity, qty); }
    assert.strictEqual(charms[0].name, qty > 1 ? `4190000101 · ${sku} · 1/${qty}` : `4190000101 · ${sku}`);
    assert(!w.logs.some(l => /bod(y|ies)/i.test(l)), sku + ': nothing said about bodies');
  }
  pass('a single charm, a matching pair (quantity 2), discs and a hoop: the pool rows and charms are exactly what they were');

  // a mismatched pair: two bodies, two pieces per unit
  for (const qty of [1, 2]) {
    const w = world(['TENNIS-MIS']); w.use('TENNIS-MIS');
    const row = lineRow('4190000102', '5000000102', 'TENNIS-MIS', qty);
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'), pools = put.body.pools, n = 2 * qty;
    assert.strictEqual(pools.length, n, `quantity ${qty} makes ${n} pieces`);
    pools.forEach((p, i) => {
      assert.strictEqual(p.poolId, `4190000102_5000000102_${i + 1}`); assert(/^\d{5,20}_\d{5,20}_\d{1,3}$/.test(p.poolId), 'the server accepts the id');
      assert.strictEqual(p.copy, i + 1); assert.strictEqual(p.quantity, n); assert.strictEqual(p.groupSize, n);
      assert.strictEqual(p.side, i % 2 ? 'R' : 'L'); assert.strictEqual(p.bodyIndex, i % 2); assert.strictEqual(p.groupKey, '4190000102:5000000102');
      assert.deepStrictEqual(J(PP.cleanFields(p)), { side: p.side, bodyIndex: p.bodyIndex, groupKey: p.groupKey, groupSize: n }, 'the server keeps all four');
      assert.strictEqual(p.lineKey, '4190000102_5000000102');
    });
    assert.strictEqual(pools[0].charmHash === pools[1].charmHash, false, 'the left and the right body are two different shapes');
    if (qty === 2) { assert.strictEqual(pools[0].charmHash, pools[2].charmHash, 'two lefts are one shape'); assert.strictEqual(pools[1].charmHash, pools[3].charmHash); }
    const charms = w.pages.gold.charms; assert.strictEqual(charms.length, n);
    const bodies = Pair.bodiesOf(F.charmOf('TENNIS-MIS'));
    charms.forEach((ch, i) => {
      assert.strictEqual(ch.side, i % 2 ? 'R' : 'L'); assert.strictEqual(ch.groupSize, n); assert.strictEqual(ch.poolId, pools[i].poolId);
      assert.strictEqual(ch.members.length, bodies[i % 2].members.length, 'each piece is cut from its own body: its members only');
      assert.deepStrictEqual(J(ch.bbox), J(bodies[i % 2].bbox));
      assert.strictEqual(ch.name, `4190000102 · TENNIS-MIS · ${i + 1}/${n}`, 'the layer name keeps its shape; the sheet writer adds Left/Right');
      assert.strictEqual(ch.orderInfo.copy, i + 1); assert.strictEqual(ch.orderInfo.quantity, n); assert.strictEqual(ch.lineKey, '4190000102_5000000102');
    });
    assert(charms[0].areaPt2 !== charms[1].areaPt2 || charms[0].widthPt !== charms[1].widthPt, 'the two bodies are different sizes');
    const dot = charms[0].members.filter(m => bodies[1].members.includes(m)); assert.strictEqual(dot.length, 0, 'no member is in both bodies');
    assert.deepStrictEqual(J(row.poolIds), pools.map(p => p.poolId)); assert.strictEqual(row.state, 'pooled');
    // the sheet record's charm entry carries the fields for a sided piece only
    assert.deepStrictEqual(J(PP.sheetCharmFields(charms[1])), { side: 'R', bodyIndex: 1, groupKey: '4190000102:5000000102', groupSize: n });
    for (const p of pools) refuseNestedArrays(p, 'pool row');
    // one source, traced once
    assert.strictEqual(w.c.B.pool.sources.size, 1);
  }
  pass('a mismatched pair makes two pieces per unit, left then right, each its own charm from its own body, one group, ids valid');

  // pieceOf / groupOf / pieceCountOf / baseFor on the pool's own rows
  {
    const w = world(['TENNIS-MIS', 'PAIR-STUD']); w.use('TENNIS-MIS');
    const row = lineRow('4190000103', '5000000103', 'TENNIS-MIS', 1);
    await w.Pool.poolAdd(row, null);
    const P0 = w.Pool.pieceOf(row.poolIds[0]), P1 = w.Pool.pieceOf(row.poolIds[1]);
    assert.deepStrictEqual(J([P0.side, P1.side, P0.kind, P0.groupSize, P1.unit]), ['L', 'R', 'mismatched', 2, 1]);
    assert.strictEqual(w.Pool.pieceCountOf(row), 2, 'its pool ids are the fact');
    assert.strictEqual(w.Pool.pieceCountOf(lineRow('1', '2', 'TENNIS-MIS', 3)), 6, 'before pooling a mismatched design makes two per unit (the entry says pair)');
    assert.strictEqual(w.Pool.pieceCountOf(lineRow('1', '2', 'PAIR-STUD', 3)), 3, 'every other design makes quantity pieces, as the app does today');
    const g = w.Pool.groupOf(row.poolIds[0]); assert.strictEqual(g.size, 2); assert.strictEqual(g.split, false, 'neither is on a sheet yet: nothing is split'); assert.strictEqual(g.sided, true); assert.strictEqual(g.off.length, 2);
    // an old glued row of a mismatched design
    w.c.B.pool.rows.set('4190000104_5000000104_1', { poolId: '4190000104_5000000104_1', sku: 'TENNIS-MIS', quantity: 1, copy: 1, sheetId: 's1' });
    const old = w.Pool.pieceOf('4190000104_5000000104_1'); assert.deepStrictEqual(J([old.kind, old.side, old.glued]), ['glued', null, true]);
    // baseFor
    const s0 = w.c.B.pool.sources.values().next().value;
    assert.strictEqual(w.Pool.baseFor(s0, null), s0.charms[0]); assert.strictEqual(w.Pool.baseFor(s0, { side: 'R', bodyIndex: 1 }), s0.bodies[1]);
    assert.strictEqual(w.Pool.baseFor({ charms: [1], bodies: null }, { side: 'L', bodyIndex: 0 }), null, 'a sided piece with no body is never drawn as both bodies');
    pass('pieceOf, groupOf, pieceCountOf and baseFor read the new rows and the old ones');
  }

  // an older glued line stays glued while a piece of it is on a sheet; a line taken off is made up as two pieces
  {
    const w = world(['TENNIS-MIS']); w.use('TENNIS-MIS');
    const id1 = '4190000105_5000000105_1';
    const row = lineRow('4190000105', '5000000105', 'TENNIS-MIS', 1); row.poolIds = [id1];
    w.c.B.pool.rows.set(id1, { poolId: id1, sku: 'TENNIS-MIS', quantity: 1, copy: 1, sheetId: 'sh-1', state: 'written' });
    row.state = 'pulled'; row.poolIds = [id1];
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'); assert.strictEqual(put.body.pools.length, 1, 'still the one glued piece');
    assert(!('side' in put.body.pools[0]), 'and no pair field on it');
    assert.strictEqual(w.pages.gold.charms[0].members.length, F.charmOf('TENNIS-MIS').members.length, 'the glued charm: both bodies');
    const w2 = world(['TENNIS-MIS']); w2.use('TENNIS-MIS');
    const row2 = lineRow('4190000106', '5000000106', 'TENNIS-MIS', 1); row2.poolIds = ['4190000106_5000000106_1'];
    w2.c.B.pool.rows.set(row2.poolIds[0], { poolId: row2.poolIds[0], sku: 'TENNIS-MIS', quantity: 1, copy: 1, sheetId: null, state: 'abandoned', heldAt: 5 });
    await w2.Pool.poolAdd(row2, null);
    assert.strictEqual(w2.calls.find(x => x.body.op === 'poolPut').body.pools.length, 2, 'a line taken off is made up again as a left and a right piece');
    // the cloud's answer says a glued piece is still on a saved sheet: the line is made again as it was
    const w3 = world(['TENNIS-MIS']); w3.use('TENNIS-MIS');
    const row3 = lineRow('4190000107', '5000000107', 'TENNIS-MIS', 1);
    let n3 = 0; w3.c.api = async (fn, body) => { w3.calls.push({ fn, body: JSON.parse(JSON.stringify(body)) }); return ++n3 === 1 ? { legacy: body.pools.map(p => p.poolId) } : {}; };
    await w3.Pool.poolAdd(row3, null);
    const puts = w3.calls.filter(x => x.body.op === 'poolPut'); assert.strictEqual(puts.length, 2); assert.strictEqual(puts[0].body.pools.length, 2); assert.strictEqual(puts[1].body.pools.length, 1, 'second call: the glued piece');
    assert(!('side' in puts[1].body.pools[0])); assert.strictEqual(row3.poolIds.length, 1);
    pass('a glued line stays glued while a piece is on a sheet; taken off it becomes two; the cloud\'s legacy answer is honoured');
  }

  // oversize is judged on each body
  {
    const w = world(['TENNIS-MIS']); w.use('TENNIS-MIS'); w.c.stockFor = () => ({ wPt: 6, hPt: 6 });
    const row = lineRow('4190000108', '5000000108', 'TENNIS-MIS', 1);
    await w.Pool.poolAdd(row, null);
    assert.strictEqual(row.state, 'oversize'); assert(!w.calls.length, 'nothing recorded for a body that does not fit');
    pass('a body that does not fit the plate holds the line');
  }

  // a design whose two bodies sit in one drawing group of the master stays one glued piece (the sheet writer copies whole groups)
  {
    const w = world(['TENNIS-MIS']); w.use('TENNIS-MIS');
    w.c.P.groupCharmsAsync = async () => { const ch = structuredClone(F.charmOf('TENNIS-MIS')); ch.members.forEach(m => { m.parent = 7; m.index = 1; }); return { charms: [ch], orphans: [] }; };
    const row = lineRow('4190000109', '5000000109', 'TENNIS-MIS', 1);
    await w.Pool.poolAdd(row, null);
    assert.strictEqual(w.calls.find(x => x.body.op === 'poolPut').body.pools.length, 1, 'one glued piece');
    assert(w.logs.some(l => /one group of the master/.test(l)), 'said in words');
    pass('two bodies in one group of the master: kept as one glued piece, said in the log');
  }

  /* ── 3 · the real server over the fake Firestore ── */
  {
    const fsx = F.fakeFirestore(), fns = F.functions(fsx, ['charmNestLibrary']);
    try {
      const rid = '4190000201', tid = '5000000201', gk = rid + ':' + tid;
      const row = (copy, over) => Object.assign({ poolId: `${rid}_${tid}_${copy}`, orderId: rid, transactionId: tid, sku: 'TENNIS-MIS', material: 'gold', copy, quantity: 2, state: 'ready', lineKey: `${rid}_${tid}`, runId: 'run-x', sheetId: null, setId: null }, over || {});
      const fields = (side, i) => ({ side, bodyIndex: i, groupKey: gk, groupSize: 2 });
      let r = await fns.lib('poolPut', { pools: [row(1, fields('L', 0)), row(2, Object.assign(fields('R', 1), { groupSize: 'two' }))] });
      assert.strictEqual(r.ok, true); assert.strictEqual(r.written, 2); assert.strictEqual(r.legacy, undefined, 'a plain answer has no legacy');
      const one = fsx.get('Charm_Pool', row(1).poolId), two = fsx.get('Charm_Pool', row(2).poolId);
      assert.deepStrictEqual([one.side, one.bodyIndex, one.groupKey, one.groupSize], ['L', 0, gk, 2]);
      assert.deepStrictEqual([two.side, two.bodyIndex, two.groupKey, two.groupSize], [undefined, undefined, undefined, undefined], 'a half-valid set of fields is not stored');
      // a plain row stays as it is
      const plain = '4190000202_5000000202_1';
      await fns.lib('poolPut', { pools: [{ poolId: plain, orderId: '4190000202', sku: 'ONE-PENDANT', copy: 1, quantity: 1, state: 'ready', side: 'L' }] });
      const pl = fsx.get('Charm_Pool', plain); assert(!('side' in pl), 'a lone side with no group is dropped'); assert(!('groupKey' in pl));
      // the legacy guard: an old glued row is on a saved sheet: new left and right rows are not written
      const lrid = '4190000203', ltid = '5000000203', glued = `${lrid}_${ltid}_1`;
      fsx.put('Charm_Pool', glued, { poolId: glued, orderId: lrid, transactionId: ltid, sku: 'TENNIS-MIS', copy: 1, quantity: 1, state: 'written', sheetId: 'sheet-old', setId: 'set-old' });
      const lk = lrid + ':' + ltid, lrow = (c, side, i) => ({ poolId: `${lrid}_${ltid}_${c}`, orderId: lrid, transactionId: ltid, sku: 'TENNIS-MIS', copy: c, quantity: 2, state: 'ready', runId: 'run-y', side, bodyIndex: i, groupKey: lk, groupSize: 2 });
      r = await fns.lib('poolPut', { pools: [lrow(1, 'L', 0), lrow(2, 'R', 1)] });
      assert.deepStrictEqual(r.legacy.sort(), [`${lrid}_${ltid}_1`, `${lrid}_${ltid}_2`], 'told to make the line as it was');
      assert.strictEqual(r.written, 0); assert.strictEqual(fsx.get('Charm_Pool', glued).side, undefined, 'the glued row is untouched');
      assert.strictEqual(fsx.get('Charm_Pool', `${lrid}_${ltid}_2`), undefined, 'and no right piece appeared');
      // taken off (abandoned): it may be made up again as two
      fsx.put('Charm_Pool', glued, { state: 'abandoned', sheetId: null, heldAt: 5, heldBy: 'x' });
      r = await fns.lib('poolPut', { pools: [lrow(1, 'L', 0), lrow(2, 'R', 1)] });
      assert.strictEqual(r.written, 2); assert.strictEqual(fsx.get('Charm_Pool', glued).side, 'L'); assert(fsx.get('Charm_Pool', glued).repooledAt, 'made live again, as always');
      pass('server poolPut: the four fields all valid or none, a plain row untouched, a glued line on a saved sheet is not half-migrated');

      // getOrderPieces: a pair split over two sheets is said; a plain order's answer has no groups
      const w = F.cases.mismatchedSplit(); const fs2 = F.fakeFirestore(), fns2 = F.functions(fs2, ['charmNestLibrary']); fs2.seed(w);
      const oid = Object.keys(w.pieces.reduce((m, p) => (m[String(p.poolId).split('_')[0]] = 1, m), {}));
      let sawGroups = 0;
      for (const id of oid) { const ans = await fns2.lib('getOrderPieces', { orderIds: [id] }); const o = ans.orders && ans.orders[id]; if (o && o.summary && o.summary.groups) { sawGroups++; const g0 = o.summary.groups[0]; assert.strictEqual(g0.size, 2); assert.strictEqual(Object.keys(g0.sheets).length, 2); } }
      assert.strictEqual(sawGroups, 1, 'exactly the mismatched pair order says it is split');
      const fs3 = F.fakeFirestore(), fns3 = F.functions(fs3, ['charmNestLibrary']); fs3.seed(F.cases.pairOneSheet());
      const w3 = F.cases.pairOneSheet(); for (const id of new Set(w3.pieces.map(p => String(p.poolId).split('_')[0]))) { const ans = await fns3.lib('getOrderPieces', { orderIds: [id] }); assert(!(ans.orders[id] && ans.orders[id].summary && ans.orders[id].summary.groups), 'no groups on a plain order'); }
      pass('getOrderPieces: a split mismatched pair is reported from the rows already read; a plain order\'s answer is unchanged');
    } finally { fns.restore(); }
  }
  console.log('pairs-pool: all passed');
})().catch(e => { console.error(e); process.exit(1); });
