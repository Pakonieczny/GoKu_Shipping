// PAIRPOOL (pairs-1009, area 4): the pool and piece records for pairs, mismatched pairs and multi-piece lines. Offline: no browser, no network.
//   node tests/charm-nest/pairs-pool.cjs
// 1 · charm-nest-pool-pieces.js (pure): the four fields, old records derived, groups, legacy glued
// 2 · the bridge's Pool (the real source, run in a vm with small stubs): makePool through poolAdd for a single charm, discs and any line the pair
//     module calls plain (records byte-identical to what the pre-pairs code wrote), and for an earring pair, matching or mismatched (a Left and a
//     Right per unit, the Right the mirror image of the drawing, a mismatched pair's pieces each cut from their own body)
// 3 · the real netlify function over the fake Firestore (no-nested-arrays check on every stored document): poolPut sanitises, the legacy
//     guard refuses to half-migrate a glued line (the split-pair answer of getOrderPieces is PAIRSERVER's: tests/charm-nest/pairs-server.cjs)
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const F = require('./pairs-fixtures.cjs');
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const PP = require(path.join(root, 'charm-nest-pool-pieces.js'));
const O = require(path.join(root, 'charm-nest-orders.js'));
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const pass = name => console.log('  ✓', name);
// the real charm-nest-pair.js (amendment 2: an earring pair always makes a Left and a Right per unit, copies alternating; the Right is the mirror image
// of the drawing; a mismatched pair's pieces are its two bodies) and, below, a module that calls every line plain (what the app did before pairs)
// a pair module that calls every line plain (what the app did before pairs)
const PairPlain = Object.assign({}, Pair, { piecesFor: line => { const q = Math.max(1, Math.round(+(line.spec && line.spec.quantity) || 1)); return Array.from({ length: q }, (_, i) => ({ side: null, bodyIndex: 0, groupKey: Pair.groupKey(line), n: i + 1, of: q, mirror: false })); }, pieceCountOf: line => Math.max(1, Math.round(+(line.spec && line.spec.quantity) || 1)), isGroupLine: () => false });
const J = x => JSON.parse(JSON.stringify(x));      // the vm's arrays and objects come from another realm: compare by value

(async () => {
  /* ── 1 · the module ── */
  {
    assert.deepStrictEqual(PP.parsePoolId('4190000001_5000000010_2'), { receiptId: '4190000001', transactionId: '5000000010', copy: 2, lineKey: '4190000001_5000000010', groupKey: '4190000001:5000000010' });
    assert.strictEqual(PP.parsePoolId('nonsense'), null);
    const good = { side: 'L', bodyIndex: 0, groupKey: '4190000001:5000000010', groupSize: 2 };
    assert.deepStrictEqual(PP.cleanFields(good), good);
    for (const bad of [{ ...good, side: 'X' }, { ...good, bodyIndex: 1.5 }, { ...good, groupSize: 0 }, { ...good, groupSize: 401 }, { ...good, groupKey: 'abc' }, { side: 'L' }, { groupKey: good.groupKey, groupSize: 2 }, null, 5])
      assert.deepStrictEqual(PP.cleanFields(bad), {}, 'all four valid or none: ' + JSON.stringify(bad));
    assert.deepStrictEqual(PP.cleanFields({ ...good, groupSize: 1 }), { ...good, groupSize: 1 }, 'a single earring that names its ear is a sided piece of a group of ONE');
    assert.strictEqual(PP.metaOf({ poolId: '4190000001_5000000010_1', ...good, groupSize: 1 }).kind, 'single');
    assert.deepStrictEqual(PP.fieldsOf({ side: null, bodyIndex: 0, groupKey: good.groupKey, n: 1, of: 1 }), {}, 'a single that names no ear carries none of the fields');
    assert.deepStrictEqual(PP.fieldsOf({ side: 'R', bodyIndex: 1, groupKey: good.groupKey, n: 2, of: 4 }), { side: 'R', bodyIndex: 1, groupKey: good.groupKey, groupSize: 4 }, 'a piecesFor piece (of) becomes the record fields');
    assert.deepStrictEqual(PP.fieldsOf({ side: null, bodyIndex: 0, groupKey: good.groupKey, n: 1, of: 2 }), {}, 'a piece with no side (a single charm, a disc) carries none');
    assert.deepStrictEqual(PP.fieldsOf({ side: 'R', mirror: true, bodyIndex: 0, groupKey: good.groupKey, n: 2, of: 2 }), { side: 'R', mirror: true, bodyIndex: 0, groupKey: good.groupKey, groupSize: 2 }, 'a matching pair\'s Right: sided, mirrored, body 0');
    assert.deepStrictEqual(PP.cleanFields({ ...good, mirror: 'yes' }), good, 'mirror is a boolean or it is left out (the rest stands)');
    assert.deepStrictEqual(PP.cleanFields({ ...good, mirror: false }), { ...good, mirror: false });
    assert.deepStrictEqual(PP.cleanFields({ poolId: '4190000001_5000000010_2', ...good, groupKey: 'zzz' }).groupKey, good.groupKey, 'a pool id names its group: a wrong groupKey is repaired from it');
    assert.deepStrictEqual(PP.FIELDS, ['side', 'mirror', 'bodyIndex', 'groupKey', 'groupSize']);
    assert.deepStrictEqual(PP.sheetCharmFields({ id: 'x', side: 'L', bodyIndex: 0, groupKey: good.groupKey, groupSize: 2, hash: 'h' }), good);
    assert.deepStrictEqual(PP.sheetCharmFields({ id: 'x', hash: 'h' }), {});
    // an old row: derived, never glued unless the design is a mismatched one
    const old = { poolId: '4190000001_5000000010_1', orderId: '4190000001', quantity: 2, copy: 1 };
    assert.deepStrictEqual(PP.metaOf(old), { poolId: old.poolId, groupKey: '4190000001:5000000010', n: 1, groupSize: 2, side: null, mirror: null, bodyIndex: 0, unit: 1, kind: 'pair', glued: false, legacy: false });
    assert.strictEqual(PP.metaOf({ poolId: '4190000001_5000000010_1', quantity: 1 }).kind, 'single');
    assert.strictEqual(PP.metaOf({ poolId: '4190000001_5000000010_1', quantity: 3 }).kind, 'multi');
    const glued = PP.metaOf(old, { mismatchedDesign: true });
    assert.strictEqual(glued.kind, 'glued'); assert.strictEqual(glued.side, null); assert.strictEqual(glued.glued, true);
    const right = PP.metaOf({ poolId: '4190000001_5000000010_4', ...good, side: 'R', bodyIndex: 1, groupSize: 4 });
    assert.deepStrictEqual([right.side, right.bodyIndex, right.n, right.unit, right.groupSize, right.kind, right.mirror], ['R', 1, 4, 2, 4, 'multi', null]);
    const mp = PP.metaOf({ poolId: '4190000001_5000000010_2', ...good, side: 'R', mirror: true, bodyIndex: 0 });
    assert.deepStrictEqual([mp.kind, mp.mirror, mp.side, mp.unit], ['pair', true, 'R', 1], 'a matching pair: two sided pieces of one body');
    assert.strictEqual(PP.metaOf({ poolId: '4190000001_5000000010_2', ...good, side: 'R', bodyIndex: 1 }).kind, 'mismatched', 'a piece cut from body 1 belongs to a mismatched pair');
    assert.strictEqual(PP.metaOf({ poolId: '4190000001_5000000010_1', ...good }, { mismatchedDesign: true }).kind, 'mismatched', 'and its Left says so with the master entry\'s word');
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
  function world(designs, rowsSeed, PairMod) {
    const logs = [], calls = [], pages = { gold: { metal: 'gold', charms: [], placements: [], status: 'idle', sheetId: null } };
    const entries = {};
    for (const sku of designs) entries[sku] = Object.assign(F.entryOf(sku), { sku, aiPath: 'charmnest/master/sku/' + sku + '.ai', aiUrl: 'https://x/' + sku, masterHash: 'mh-' + sku, upAngle: 0 });
    const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, '0'); };
    const c = {
      console, Date, JSON, Map, Set, Math, Number, String, Array, Object, Promise, Error, Uint8Array, setTimeout, clearTimeout, performance: { now: () => Date.now() }, structuredClone,
      window: { CharmNestPair: PairMod || Pair, CharmNestPoolPieces: PP }, O, MM, labelOf: m => m, stockFor: () => ({ wPt: 1400, hPt: 700 }),
      S: { cloud: { ok: true }, settings: { insetPt: 0, maxFill: .8, silhouetteRes: 6, minPt: 6 }, poolSources: {}, sheets: { gold: { active: 0 } } },
      B: { pool: { rows: new Map(rowsSeed || []), sources: new Map() }, orders: { byKey: new Map(), rows: [] }, run: null },
      agent: (...x) => { logs.push(x.slice(1).join(' ')); }, api: async (fn, body) => { calls.push({ fn, body: JSON.parse(JSON.stringify(body)) }); refuseNestedArrays(body, 'poolPut'); return {}; },
      Master: { entryFor: sku => entries[sku] || null, fetchEntry: async () => null, keepOutOf: () => [], skuRegex: () => /^$/ },
      CharmNestAssets: { bytes: async () => new Uint8Array(1) },
      P: {
        parseSource: async () => ({}), groupCharmsAsync: null, integrateRings: () => ({ left: [], welded: 0 }), parseSkuLabel: () => null,
        buildSilhouettes: async (_p, charms) => { for (const ch of charms) { const w = ch.bbox[2] - ch.bbox[0], h = ch.bbox[3] - ch.bbox[1]; Object.assign(ch, { bits: new Uint8Array(4), w: 2, h: 2, scale: 6, areaPt2: Math.round(w * h * .8), widthPt: w, heightPt: h, centerPt: [(ch.bbox[0] + ch.bbox[2]) / 2, (ch.bbox[1] + ch.bbox[3]) / 2], thumb: 't', hash: fnv(JSON.stringify(ch.bbox) + ch.members.length + (ch.outline.id || '') + (ch.outline.mirrored ? 'm' : '')) }); } return charms; }
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
  const was = (rid, tid, sku, copy, q, charmHash, form) => JSON.stringify({ poolId: `${rid}_${tid}_${copy}`, runId: null, setId: null, sheetId: null, orderId: rid, orderDate: 1791500000, arrivedAt: 0, transactionId: tid, sku, material: 'gold', size: null, form: form || 'earrings', chain: null, copy, quantity: q, charmHash, masterHash: 'mh-' + sku, aiPath: 'charmnest/master/sku/' + sku + '.ai', engrave: false, state: 'ready', lineKey: `${rid}_${tid}`, updateTs: 1791500001 });

  // plain lines: single charm, matching pair (two copies), discs: byte-identical, and no pair field anywhere
  for (const [sku, qty, mod, form] of [['ONE-PENDANT', 1, Pair, 'pendant'], ['DISC-14', 3, Pair, 'necklace'], ['ONE-PENDANT', 1, PairPlain, 'earrings'], ['PAIR-STUD', 2, PairPlain, 'earrings'], ['DISC-14', 1, PairPlain, 'necklace'], ['PAIR-HOOP', 3, PairPlain, 'earrings']]) {
    const w = world([sku], null, mod); w.use(sku);
    const row = lineRow('4190000101', '5000000101', sku, qty, { form });
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'); assert(put, sku + ': recorded');
    assert.strictEqual(put.body.pools.length, qty, sku + ': quantity pieces, as before');
    const charmHash = put.body.pools[0].charmHash;
    put.body.pools.forEach((p, i) => assert.strictEqual(JSON.stringify(p), was('4190000101', '5000000101', sku, i + 1, qty, charmHash, form), sku + ' copy ' + (i + 1) + ': the record is byte-identical to the one written before pairs'));
    assert(!put.body.pools.some(p => 'side' in p || 'bodyIndex' in p || 'groupKey' in p || 'groupSize' in p));
    assert.deepStrictEqual(J(row.poolIds), put.body.pools.map(p => p.poolId));
    const charms = w.pages.gold.charms; assert.strictEqual(charms.length, qty);
    for (const ch of charms) { assert(!('side' in ch) && !('groupKey' in ch), sku + ': no pair field on a plain charm'); assert.strictEqual(ch.orderInfo.quantity, qty); }
    assert.strictEqual(charms[0].name, qty > 1 ? `4190000101 · ${sku} · 1/${qty}` : `4190000101 · ${sku}`);
    assert(!w.logs.some(l => /bod(y|ies)/i.test(l)), sku + ': nothing said about bodies');
  }
  pass('a single charm, discs, and any line the pair module calls plain: the pool rows and charms are exactly what they were');

  // counted-option necklaces (3 discs, 5 letters: the intake's pieceCount above the quantity) ARE a group of that many pieces: every row carries the group and its size, no ear;
  // the same piece count as a plain quantity (above, 'DISC-14' x 3) is not (Paul 9 Oct, after ADVCOMPAT 1: a plain quantity-N line is not a group)
  for (const [qty, count] of [[1, 3], [2, 6], [1, 5]]) {
    const w = world(['DISC-14']); w.use('DISC-14');
    const row = lineRow('4190000103', '5000000103', 'DISC-14', qty, { form: 'necklace', pieceCount: count });
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'), pools = put.body.pools, gk = '4190000103:5000000103';
    assert.strictEqual(pools.length, count, 'a necklace of ' + count + ' discs makes ' + count + ' pieces');
    pools.forEach(pl => { assert.strictEqual(pl.groupKey, gk); assert.strictEqual(pl.groupSize, count); assert(!('side' in pl) && !('mirror' in pl) && !('bodyIndex' in pl), 'no ear on a disc'); });
    const charms = w.pages.gold.charms; assert.strictEqual(charms.length, count);
    for (const ch of charms) { assert.strictEqual(ch.groupKey, gk, 'the sheet charm carries the group'); assert.strictEqual(ch.groupSize, count); assert(!('side' in ch)); }
    assert.deepStrictEqual(J(PP.cleanGroupFields(pools[0])), { groupKey: gk, groupSize: count }, 'the server keeps exactly these two fields');
    assert.deepStrictEqual(J(PP.cleanFields(pools[0])), {}, 'and a sideless piece is not a sided one');
  }
  pass('counted discs / letters (pieceCount above the quantity): a group of that many pieces, each row with the group key and size and no ear');

  // a mismatched pair: two bodies, two pieces per unit
  for (const qty of [1, 2]) {
    const w = world(['TENNIS-MIS']); w.use('TENNIS-MIS'); w.entries['TENNIS-MIS'].facings = ['L', 'R'];   // a person set it: the left body faces left, the right body faces right
    const row = lineRow('4190000102', '5000000102', 'TENNIS-MIS', qty);
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'), pools = put.body.pools, n = 2 * qty;
    assert.strictEqual(pools.length, n, `quantity ${qty} makes ${n} pieces`);
    pools.forEach((p, i) => {
      assert.strictEqual(p.poolId, `4190000102_5000000102_${i + 1}`); assert(/^\d{5,20}_\d{5,20}_\d{1,3}$/.test(p.poolId), 'the server accepts the id');
      assert.strictEqual(p.copy, i + 1); assert.strictEqual(p.quantity, n); assert.strictEqual(p.groupSize, n);
      assert.strictEqual(p.side, i % 2 ? 'R' : 'L'); assert.strictEqual(p.bodyIndex, i % 2); assert.strictEqual(p.groupKey, '4190000102:5000000102'); assert.strictEqual(p.mirror, false, 'each body already faces its own ear');
      assert.deepStrictEqual(J(PP.cleanFields(p)), { side: p.side, mirror: false, bodyIndex: p.bodyIndex, groupKey: p.groupKey, groupSize: n }, 'the server keeps all of them');
      assert.strictEqual(p.lineKey, '4190000102_5000000102');
    });
    assert.strictEqual(pools[0].charmHash === pools[1].charmHash, false, 'the left and the right body are two different shapes');
    if (qty === 2) { assert.strictEqual(pools[0].charmHash, pools[2].charmHash, 'two lefts are one shape'); assert.strictEqual(pools[1].charmHash, pools[3].charmHash); }
    const charms = w.pages.gold.charms; assert.strictEqual(charms.length, n);
    const bodies = Pair.bodiesOf(F.charmOf('TENNIS-MIS'));
    charms.forEach((ch, i) => {
      assert.strictEqual(ch.side, i % 2 ? 'R' : 'L'); assert.strictEqual(ch.mirror, false); assert.strictEqual(ch.groupSize, n); assert.strictEqual(ch.poolId, pools[i].poolId);
      assert.strictEqual(ch.members.length, bodies[i % 2].members.length, 'each piece is cut from its own body: its members only');
      assert.deepStrictEqual(J(ch.bbox), J(bodies[i % 2].bbox));
      assert.strictEqual(ch.name, `4190000102 · TENNIS-MIS · ${i + 1}/${n}`, 'the layer name keeps its shape; the sheet writer adds Left/Right');
      assert.strictEqual(ch.orderInfo.copy, i + 1); assert.strictEqual(ch.orderInfo.quantity, n); assert.strictEqual(ch.lineKey, '4190000102_5000000102');
    });
    assert(charms[0].areaPt2 !== charms[1].areaPt2 || charms[0].widthPt !== charms[1].widthPt, 'the two bodies are different sizes');
    const dot = charms[0].members.filter(m => bodies[1].members.includes(m)); assert.strictEqual(dot.length, 0, 'no member is in both bodies');
    assert.deepStrictEqual(J(row.poolIds), pools.map(p => p.poolId)); assert.strictEqual(row.state, 'pooled');
    // the sheet record's charm entry carries the fields for a sided piece only
    assert.deepStrictEqual(J(PP.sheetCharmFields(charms[1])), { side: 'R', mirror: false, bodyIndex: 1, groupKey: '4190000102:5000000102', groupSize: n });
    for (const p of pools) refuseNestedArrays(p, 'pool row');
    assert.strictEqual(w.c.B.pool.sources.size, 1, 'one source, traced once');
  }
  // a mismatched pair whose right body is drawn facing the wrong way: that piece is the mirror image of its body
  {
    const w = world(['TENNIS-MIS']); w.use('TENNIS-MIS'); w.entries['TENNIS-MIS'].facings = ['L', 'L'];   // a person set it: both bodies face left, so the Right is the second body turned over
    const row = lineRow('4190000110', '5000000110', 'TENNIS-MIS', 1);
    await w.Pool.poolAdd(row, null);
    const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools, charms = w.pages.gold.charms;
    assert.deepStrictEqual(J(pools.map(p => [p.side, p.mirror, p.bodyIndex])), [['L', false, 0], ['R', true, 1]]);
    assert.strictEqual(charms[1].mirror, true); assert.strictEqual(charms[1].outline.mirrored, true, 'the Right is cut from its body, reflected'); assert.strictEqual(charms[0].outline.mirrored, undefined);
    assert.strictEqual(charms[1].members.length, Pair.bodiesOf(F.charmOf('TENNIS-MIS'))[1].members.length, 'still its own body only');
  }
  pass('a mismatched pair makes two pieces per unit, left then right, each its own charm from its own body (mirrored when it faces the wrong way), one group, ids valid');

  // a MATCHING earring pair (stud, hoop): a Left and a Right per unit too; the Right is the mirror image of the drawing (amendment 2)
  for (const [sku, qty] of [['PAIR-STUD', 1], ['PAIR-STUD', 2], ['PAIR-HOOP', 1]]) {
    const w = world([sku]); w.use(sku);
    const row = lineRow('4190000111', '5000000111', sku, qty);
    await w.Pool.poolAdd(row, null);
    const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools, charms = w.pages.gold.charms, n = 2 * qty, gk = '4190000111:5000000111';
    assert.strictEqual(pools.length, n, sku + ' ×' + qty + ': a Left and a Right per unit'); assert.strictEqual(charms.length, n);
    pools.forEach((p, i) => {
      assert.deepStrictEqual(J([p.side, p.mirror, p.bodyIndex, p.groupKey, p.groupSize, p.copy, p.quantity]), [i % 2 ? 'R' : 'L', i % 2 === 1, 0, gk, n, i + 1, n], `${sku} piece ${i + 1}`);
      refuseNestedArrays(p, 'pool row'); assert.strictEqual(charms[i].side, p.side); assert.strictEqual(charms[i].mirror, p.mirror);
      assert.strictEqual(charms[i].name, `4190000111 · ${sku} · ${i + 1}/${n}`);
      assert.strictEqual(!!charms[i].outline.mirrored, i % 2 === 1, 'a Right is cut from the mirrored outline, a Left from the drawing');
    });
    assert.notStrictEqual(pools[0].charmHash, pools[1].charmHash, 'the Left and its mirror image are two shapes to the nester');
    if (qty === 2) { assert.strictEqual(pools[0].charmHash, pools[2].charmHash); assert.strictEqual(pools[1].charmHash, pools[3].charmHash); assert.strictEqual(charms[1].outline, charms[3].outline, 'the mirrored charm is made once per design'); }
    assert.strictEqual(charms[0].members.length, F.charmOf(sku).members.length);
  }
  // a line that is not an earring pair keeps its record, whatever the module says about earrings
  {
    const w = world(['ONE-PENDANT']); w.use('ONE-PENDANT');
    await w.Pool.poolAdd(lineRow('4190000112', '5000000112', 'ONE-PENDANT', 1, { form: 'pendant' }), null);
    assert.strictEqual(JSON.stringify(w.calls.find(x => x.body.op === 'poolPut').body.pools[0]), was('4190000112', '5000000112', 'ONE-PENDANT', 1, 1, w.calls[0].body.pools[0].charmHash, 'pendant'));
  }
  // the module cannot mirror: the line is held and said so, never drawn the wrong way round
  {
    const NoMirror = Object.assign({}, Pair, { pieceGeometry: undefined, mirrorOf: undefined });
    const w = world(['PAIR-STUD'], null, NoMirror); w.use('PAIR-STUD');
    const row = lineRow('4190000113', '5000000113', 'PAIR-STUD', 1);
    await w.Pool.poolAdd(row, null);
    assert.strictEqual(row.state, 'held'); assert(/mirror/.test(row.reason)); assert(!w.calls.length, 'nothing recorded'); assert.strictEqual(w.pages.gold.charms.length, 0);
  }
  pass('a matching earring pair makes a Left and a Right per unit (the Right mirrored); a line that is not an earring pair is unchanged; no mirror means the line is held');

  // a SINGLE earring that names its ear (Paul, 9 Oct: left and right are always kept; ADVCOUNT F12): ONE piece with that side, the Right the mirror image by the same facing rule as a pair
  {
    const entryFor = sku => Object.assign(F.entryOf(sku), { sku });
    const specOf = (rid, tid, sku, title, vars) => {
      const order = { receiptId: rid, updateTs: 1791500001 }, line = { transactionId: tid, listingId: '1', sku, title, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Gold' }].concat(vars || []) };
      const sp = O.interpretLine(order, line, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: entryFor });
      return { designSku: sku, material: 'gold', quantity: 1, size: null, form: sp.form || 'earrings', chain: null, pair: sp.pair, pieceCount: sp.pieceCount };
    };
    const single = async (n, sku, title, want) => {
      const rid = '41900001' + n, tid = '50000001' + n, w = world([sku]); w.use(sku);
      const row = lineRow(rid, tid, sku, 1, specOf(rid, tid, sku, title));
      await w.Pool.poolAdd(row, null);
      const put = w.calls.find(x => x.body.op === 'poolPut'), pools = put.body.pools, charms = w.pages.gold.charms, p = pools[0];
      assert.strictEqual(pools.length, 1, title + ': one piece'); assert.strictEqual(charms.length, 1);
      if (!want) { assert(!('side' in p || 'bodyIndex' in p || 'groupKey' in p || 'groupSize' in p || 'mirror' in p), title + ': no ear named: no pair field, as drawn'); assert(!('side' in charms[0]) && !charms[0].outline.mirrored); return { w, row, p }; }
      assert.deepStrictEqual(J([p.side, p.mirror, p.bodyIndex, p.groupKey, p.groupSize, p.copy, p.quantity]), [want.side, want.mirror, want.body || 0, rid + ':' + tid, 1, 1, 1], title);
      assert.deepStrictEqual(J(PP.cleanFields(p)), J(PP.fieldsOf(p)), 'the server keeps every field'); refuseNestedArrays(p, 'pool row');
      assert.strictEqual(charms[0].side, want.side); assert.strictEqual(charms[0].mirror, want.mirror); assert.strictEqual(charms[0].groupSize, 1);
      assert.strictEqual(!!charms[0].outline.mirrored, want.mirror, 'the mirrored outline is cut for the piece that faces the other way');
      assert.strictEqual(charms[0].name, rid + ' · ' + sku, 'the layer name has no 1/1');
      assert.deepStrictEqual(J(PP.sheetCharmFields(charms[0])), J(PP.fieldsOf(p)));
      return { w, row, p };
    };
    await single('31', 'PAIR-FACE-L', 'Custom Single Replacement Silver Cat Huggie Earring Left Ear', { side: 'L', mirror: false });
    await single('32', 'PAIR-FACE-L', 'Single Star Earring, Right Ear', { side: 'R', mirror: true });
    await single('33', 'PAIR-FACE-R', 'Single Star Earring, Left Ear', { side: 'L', mirror: true });
    await single('34', 'PAIR-FACE-R', 'Single Star Earring Right Earring only', { side: 'R', mirror: false });
    await single('35', 'PAIR-FACE-L', 'Single Star Earring');                                  // no ear named: the plain record
    await single('36', 'TENNIS-MIS', 'Single Mittens Earring Right Ear', { side: 'R', mirror: true, body: 1 });   // a mismatched design: the body of that ear, not both (the fixture draws both bodies facing left, so its Right is turned over, as in a pair)
    // the plain single's record is exactly what it was before pairs
    const plain = await single('37', 'PAIR-FACE-L', 'Single Star Earring');
    assert.strictEqual(JSON.stringify(plain.p), was('4190000137', '5000000137', 'PAIR-FACE-L', 1, 1, plain.p.charmHash, plain.row.spec.form));
    // a pair is still a pair
    const pair = world(['PAIR-FACE-L']); pair.use('PAIR-FACE-L'); const prow = lineRow('4190000138', '5000000138', 'PAIR-FACE-L', 1, specOf('4190000138', '5000000138', 'PAIR-FACE-L', 'Star Stud Earrings'));
    await pair.Pool.poolAdd(prow, null); assert.deepStrictEqual(J(pair.calls.find(x => x.body.op === 'poolPut').body.pools.map(p => [p.side, p.mirror, p.groupSize])), [['L', false, 2], ['R', true, 2]]);
  }
  pass('a single earring that names its ear is ONE piece with that side (the Right mirrored by the facing rule, a mismatched design cut from that ear\'s body); one that names none is cut as drawn; a pair is still two');

  // the real module: whatever it says, the records it produces are valid for the server and the fields come straight from its pieces
  {
    for (const sku of ['TENNIS-MIS', 'PAIR-STUD', 'ONE-PENDANT']) {
      const w = world([sku]); w.use(sku);
      const row = lineRow('4190000114', '5000000114', sku, 2);
      await w.Pool.poolAdd(row, null);
      const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools;
      pools.forEach((p, i) => { refuseNestedArrays(p, 'pool row'); const f = PP.cleanFields(p); if (p.side) assert.deepStrictEqual(J(f), J(PP.fieldsOf(p)), sku + ': what is recorded is what the server keeps'); else assert.strictEqual(Object.keys(f).length, 0); });
      assert.strictEqual(row.poolIds.length, pools.length);
    }
  }
  pass('the real charm-nest-pair.js: its pieces become valid records');

  // pieceOf / groupOf / pieceCountOf / baseFor on the pool's own rows
  {
    const w = world(['TENNIS-MIS', 'PAIR-STUD']); w.use('TENNIS-MIS');
    const row = lineRow('4190000103', '5000000103', 'TENNIS-MIS', 1);
    await w.Pool.poolAdd(row, null);
    const P0 = w.Pool.pieceOf(row.poolIds[0]), P1 = w.Pool.pieceOf(row.poolIds[1]);
    assert.deepStrictEqual(J([P0.side, P1.side, P0.kind, P0.groupSize, P1.unit]), ['L', 'R', 'mismatched', 2, 1]);
    assert.strictEqual(w.Pool.pieceCountOf(row), 2, 'its pool ids are the fact');
    assert.strictEqual(w.Pool.pieceCountOf(lineRow('1', '2', 'TENNIS-MIS', 3)), 6, 'before pooling a mismatched design makes two per unit (the entry says pair)');
    assert.strictEqual(w.Pool.pieceCountOf(lineRow('1', '2', 'PAIR-STUD', 3)), 6, 'an earring pair makes two per unit');
    assert.strictEqual(w.Pool.pieceCountOf(lineRow('1', '2', 'PAIR-STUD', 3, { form: 'necklace' })), 3, 'every other line makes quantity pieces, as the app does today');
    assert.strictEqual(w.Pool.pieceCountOf(lineRow('1', '2', 'PAIR-STUD', 3, { form: 'necklace', pieceCount: 5 })), 5, 'the count the intake set wins');
    const g = w.Pool.groupOf(row.poolIds[0]); assert.strictEqual(g.size, 2); assert.strictEqual(g.split, false, 'neither is on a sheet yet: nothing is split'); assert.strictEqual(g.sided, true); assert.strictEqual(g.off.length, 2);
    // an old glued row of a mismatched design
    w.c.B.pool.rows.set('4190000104_5000000104_1', { poolId: '4190000104_5000000104_1', sku: 'TENNIS-MIS', quantity: 1, copy: 1, sheetId: 's1' });
    const old = w.Pool.pieceOf('4190000104_5000000104_1'); assert.deepStrictEqual(J([old.kind, old.side, old.glued]), ['glued', null, true]);
    // baseFor
    const s0 = w.c.B.pool.sources.values().next().value;
    assert.strictEqual(w.Pool.baseFor(s0, null), s0.charms[0]); assert.strictEqual(w.Pool.baseFor(s0, { side: 'R', bodyIndex: 1 }), s0.bodies[1]);
    assert.strictEqual(w.Pool.baseFor({ charms: [1], bodies: null }, { side: 'R', bodyIndex: 1 }), null, 'a sided piece with no body is never drawn as both bodies');
    assert.strictEqual(w.Pool.baseFor({ charms: [1], bodies: null }, { side: 'L', bodyIndex: 0 }), 1, 'a matching pair has one body');
    assert.strictEqual(w.Pool.baseFor({ charms: [1], bodies: null }, { side: 'R', mirror: true, bodyIndex: 0 }), null, 'a mirrored piece waits for its mirrored charm (ensureBase)');
    assert.strictEqual(w.Pool.baseFor({ charms: [1], bodies: null, mirrors: { 0: 'M' } }, { side: 'R', mirror: true, bodyIndex: 0 }), 'M');
    pass('pieceOf, groupOf, pieceCountOf and baseFor read the new rows and the old ones');
  }

  // a source recovered from a checkpoint has its traced charm and bytes but not the derived bodies or mirrored charm: ensureBase makes them again
  {
    const w = world(['TENNIS-MIS']); w.use('TENNIS-MIS');
    w.entries['TENNIS-MIS'].facings = ['L', 'L']; await w.Pool.poolAdd(lineRow('4190000115', '5000000115', 'TENNIS-MIS', 1), null);
    const s0 = w.c.B.pool.sources.values().next().value;
    delete s0.bodies; delete s0.bodiesChecked; delete s0.mirrors;
    assert.strictEqual(w.Pool.baseFor(s0, { side: 'R', mirror: true, bodyIndex: 1 }), null);
    const [a, b] = await Promise.all([w.Pool.ensureBase(s0, { side: 'R', mirror: true, bodyIndex: 1 }), w.Pool.ensureBase(s0, { side: 'R', mirror: true, bodyIndex: 1 })]);
    assert.strictEqual(a, b, 'two lines of one design made up side by side make it once'); assert.strictEqual(a.mirror, true); assert.strictEqual(a.outline.mirrored, true);
    assert.strictEqual(s0.bodies.length, 2); assert.strictEqual(w.Pool.baseFor(s0, { side: 'R', mirror: true, bodyIndex: 1 }), a);
    assert.strictEqual(await w.Pool.ensureBase(s0, null), s0.charms[0], 'an ordinary piece is the one charm');
    pass('a recovered source makes its bodies and its mirrored charm again, once');
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
    // an older matching pair (rows made before pieces had a side, one still on a sheet) is not rewritten as a Left and a Right: it stays as it was
    const wm = world(['PAIR-STUD']); wm.use('PAIR-STUD');
    const rowm = lineRow('4190000116', '5000000116', 'PAIR-STUD', 2), ids = ['4190000116_5000000116_1', '4190000116_5000000116_2'];
    rowm.poolIds = ids; ids.forEach((id, i) => wm.c.B.pool.rows.set(id, { poolId: id, sku: 'PAIR-STUD', quantity: 2, copy: i + 1, sheetId: i ? null : 'sh-9', state: i ? 'ready' : 'written' }));
    await wm.Pool.poolAdd(rowm, null);
    { const ps = wm.calls.find(x => x.body.op === 'poolPut').body.pools; assert.strictEqual(ps.length, 2, 'the pieces it had'); assert(!ps.some(p => 'side' in p || 'mirror' in p || 'groupKey' in p), 'and no pair field on them'); }
    assert.strictEqual(wm.Pool.pieceCountOf(lineRow('1', '2', 'PAIR-STUD', 3)), 6, 'before pooling an earring pair counts two per unit (the pair module\'s word)');
    assert.strictEqual(wm.Pool.pieceCountOf(lineRow('1', '2', 'ONE-PENDANT', 3, { form: 'pendant' })), 3);
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
      assert.deepStrictEqual([two.side, two.bodyIndex, two.groupKey, two.groupSize], [undefined, undefined, gk, undefined], 'a half-valid set is not stored (the group key alone is what the pool id says, PAIRSERVER cleanPiece)');
      // mirror: a boolean is kept, anything else is left out and the rest stands
      const mrid = '4190000204', mtid = '5000000204', mk = mrid + ':' + mtid, mrow = (c, side, m) => ({ poolId: `${mrid}_${mtid}_${c}`, orderId: mrid, transactionId: mtid, sku: 'PAIR-STUD', copy: c, quantity: 2, state: 'ready', runId: 'run-m', side, mirror: m, bodyIndex: 0, groupKey: mk, groupSize: 2 });
      r = await fns.lib('poolPut', { pools: [mrow(1, 'L', false), mrow(2, 'R', true)] }); assert.strictEqual(r.written, 2);
      assert.deepStrictEqual([fsx.get('Charm_Pool', `${mrid}_${mtid}_1`).mirror, fsx.get('Charm_Pool', `${mrid}_${mtid}_2`).mirror, fsx.get('Charm_Pool', `${mrid}_${mtid}_2`).side], [false, true, 'R'], 'a matching pair\'s Left and mirrored Right are stored');
      await fns.lib('poolPut', { pools: [Object.assign(mrow(2, 'R', 'yes'), { runId: 'run-m' })] });
      assert.strictEqual(fsx.get('Charm_Pool', `${mrid}_${mtid}_2`).mirror, true, 'a mirror that is not a boolean is not written (the stored one stands)');
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

    } finally { fns.restore(); }
  }
  console.log('pairs-pool: all passed');
})().catch(e => { console.error(e); process.exit(1); });
