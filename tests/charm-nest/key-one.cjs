// KEYONE (flag-fix-1011): "This listing is not an earring so it should NOT have duplicate charms" (Paul, 11 Oct 2026, order 4196617547: a Key Necklace, quantity 1, whose design KEY_5598
// draws TWO keys). A line that is not an earring pair makes ONE charm of a design that draws the same outline twice: one body on the sheet, one in the thumbnail, no Left / Right, one piece.
// Offline: no browser, no network.   node tests/charm-nest/key-one.cjs
//  1 · charm-nest-pair.js: the shape test (same outline twice → one body; different charms → both bodies kept), the rule for the body (the left one), which lines choose
//  2 · the intake (charm-nest-orders.js): the Key necklace says ONE charm, the Key earrings keep their Left and Right, a single earring that names no ear is one body too
//  3 · the bridge's Pool (the real source, run in a vm with small stubs): the necklace makes ONE piece cut from the left key alone, with no side, no group, a body's hash; the pieces of a
//      line pooled before this stay whole; a stored hash is read back (ensureBase / soloBase); the placement picture and the Review picture are one key; the count is 1
//  4 · the real master files of the 14 Sep indexing, when they are there (KEY_5598, MISMATCHED_7134): the shape test on real geometry
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const PP = require(path.join(root, 'charm-nest-pool-pieces.js'));
const O = require(path.join(root, 'charm-nest-orders.js'));
const Labels = require(path.join(root, 'charm-nest-pair-labels.js'));
const { DESIGNS, hashOf, entryOf, world, specOf, lineRow, pairSpec } = require('./_keyWorld.cjs')(root);   // (the designs and the Pool in a vm: shared with key-ears-same.cjs)
const pass = name => console.log('  ✓', name);
const J = x => JSON.parse(JSON.stringify(x));

(async () => {
  /* ── 1 · the module ── */
  {
    const key = DESIGNS.KEY_5598(), bodies = Pair.bodiesOf(key);
    assert.strictEqual(bodies.length, 2); assert.strictEqual(Pair.isMismatched(key), true, 'two keys with different letters are a mismatched design (the 42 flagged ones are such)');
    const d = Pair.bodiesAlike(key); assert.strictEqual(d.n, 2); assert.strictEqual(d.alike, true, 'the same outline twice'); assert.strictEqual(d.parts[0].how, 'as drawn'); assert(d.parts[0].same >= .99);
    assert.strictEqual(Pair.bodiesAlike(DESIGNS.TENNIS_MIS()).alike, false, 'a ball and a racket are different charms: they keep today\'s one piece holding both');
    assert.strictEqual(Pair.bodiesAlike(DESIGNS.ONE_PENDANT()), null, 'a one-body design has nothing to compare');
    assert.strictEqual(Pair.sameOutline(bodies[0], bodies[1]).alike, true);
    // a key turned over still has the key's outline: the second body drawn as the first one's mirror image counts too, at the stricter bar
    const mir = Pair.mirrorOf(Pair.charmOfBody(key, bodies[0])), mb = Pair.bodiesOf(mir)[0];
    assert.strictEqual(Pair.sameOutline(bodies[0], mb).how === 'as drawn' || Pair.sameOutline(bodies[0], mb).how === 'turned over', true);
    // a design the entry names (the four measured SKUs), before any file is read
    for (const s of ['KEY_5598', 'KEY_6641', 'MISMATCHED_7134', 'SCHR_1215']) assert.strictEqual(Pair.alikeDesign({ sku: s, pair: { v: 1, bodies: 2, mismatched: true } }), true, s);
    assert.strictEqual(Pair.alikeDesign({ sku: 'CAT_1111', pair: { v: 1, bodies: 2, mismatched: true } }), null, 'any other design: the file has to be read');
    assert.strictEqual(Pair.alikeDesign({ sku: 'CAT_1111', pair: { v: 1, bodies: 2, mismatched: true, alike: false } }), false);
    assert.strictEqual(Pair.alikeDesign(key), true); assert.strictEqual(Pair.alikeDesign(DESIGNS.TENNIS_MIS()), false);
    // which lines choose ONE: not an earring pair, whatever the design draws
    const necklace = { receiptId: '4196617547', transactionId: '1', quantity: 1, form: 'necklace', spec: { quantity: 1, pair: { earring: false, single: false, mismatched: false, twoBodies: true } } };
    const ears = { receiptId: '4196617548', transactionId: '1', quantity: 1, form: 'earrings', spec: { quantity: 1, pieceCount: 2, pair: { earring: true, single: false, mismatched: true, kind: 'mismatched', sides: ['L', 'R'] } } };
    const oneEar = { receiptId: '4196617549', transactionId: '1', quantity: 1, form: 'single', spec: { quantity: 1, pair: { earring: false, single: true, mismatched: true, sideSaid: null } } };
    const namedEar = { receiptId: '4196617550', transactionId: '1', quantity: 1, form: 'single', spec: { quantity: 1, pair: { earring: false, single: true, mismatched: true, sideSaid: 'R', sides: ['R'] } } };
    const s = Pair.soloOf(necklace, key); assert.deepStrictEqual(J(s), { index: 0, count: 2, rule: 'the left body', how: 'as drawn' }, 'the rule: always the LEFT body (bodyIndex 0), the first in the drawing');
    assert.strictEqual(Pair.soloOf(ears, key), null, 'an earring pair keeps its Left and its Right');
    assert.strictEqual(Pair.soloOf(namedEar, key), null, 'a single earring that names its ear keeps that ear\'s own body');
    assert.strictEqual(Pair.soloOf(oneEar, key).index, 0, 'a single earring that names no ear is not a pair: one body');
    assert.strictEqual(Pair.soloOf(necklace, DESIGNS.TENNIS_MIS()), null, 'two different charms of one design: the line keeps both (listed for Paul)');
    assert.strictEqual(Pair.soloOf(necklace, DESIGNS.ONE_PENDANT()), null);
    assert.strictEqual(Pair.soloOf(Object.assign({}, necklace, { spec: { quantity: 1, pair: { earring: false, glued: true } } }), key), null, 'an old glued line stays as it was');
    assert.strictEqual(Pair.soloOf(Object.assign({}, necklace, { spec: { quantity: 1, pair: { earring: false, legacy: true } } }), key), null);
    const asked = Pair.soloOf(Object.assign({}, necklace, { spec: { quantity: 1, pair: { earring: false, useBody: 1 } } }), key);
    assert.deepStrictEqual([asked.index, asked.rule], [1, "a person's choice"], 'a person\'s word (spec.pair.useBody) picks the body');
    // the pieces and the count: a necklace of this design is ONE piece per unit, an earring pair two
    assert.strictEqual(Pair.pieceCountOf(necklace, key), 1); assert.strictEqual(Pair.pieceCountOf(Object.assign({}, necklace, { quantity: 3, spec: { quantity: 3, pair: necklace.spec.pair } }), key), 3);
    assert.strictEqual(Pair.pieceCountOf(ears, key), 2);
    assert.strictEqual(Pair.piecesFor(necklace, key).length, 1); assert(!Pair.piecesFor(necklace, key)[0].side, 'no Left or Right on a necklace');
    pass('the shape test (the same outline twice → one body, the left one; two different charms → both kept) and which lines choose it');
  }

  /* ── 2 · the intake ── */
  {
    const entry = Object.assign(entryOf('KEY_5598'), { sku: 'KEY_5598' });
    const read = (title, vars) => { const order = { receiptId: '4196617547', updateTs: 1 }, line = { transactionId: '5000000001', listingId: '1099225598', sku: 'KEY_5598', title, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Gold' }].concat(vars || []) }; return O.interpretLine(order, line, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => (s === 'KEY_5598' ? entry : null) }); };
    const neck = read('Personalized Key Necklace, Initial Key Charm', [{ name: 'Letter', value: 'A' }]);
    assert.strictEqual(neck.pieceCount, 1, 'counted as 1 piece'); assert.strictEqual(neck.pair.earring, false); assert.deepStrictEqual(J(neck.pair.oneBody), { index: 0, count: 2, rule: 'the left body' });
    assert(neck.pair.notes.some(n => /made as ONE charm, the left body only, no Left or Right/.test(n)), 'the order window says one charm');
    assert(!neck.pair.notes.some(n => /both bodies/.test(n)), 'and no longer says both bodies');
    assert.deepStrictEqual(J(neck.pair.sides), [null], 'no Left or Right');
    const ears = read('Key Stud Earrings'); assert.strictEqual(ears.pieceCount, 2); assert.deepStrictEqual(J(ears.pair.sides), ['L', 'R']); assert.strictEqual(ears.pair.oneBody, undefined, 'the earring pair keeps its Left and its Right');
    assert.strictEqual(ears.pair.kind, 'mismatched'); assert.strictEqual(ears.pair.earring, true);
    const single = read('Single Key Earring'); assert.strictEqual(single.pieceCount, 1); assert.deepStrictEqual(J(single.pair.oneBody), { index: 0, count: 2, rule: 'the left body' }, 'a single earring that names no ear is not a pair: one body');
    // the notes can be taken back (the geometry says otherwise) and put again
    const sp = JSON.parse(JSON.stringify(neck)); O.solo(sp, null); assert.strictEqual(sp.pair.oneBody, undefined); assert(sp.pair.notes.some(n => /made as one piece with both bodies/.test(n))); assert(!sp.pair.notes.some(n => /ONE charm/.test(n)));
    O.solo(sp, { index: 0, count: 2, rule: 'the left body' }); assert(sp.pair.notes.some(n => /ONE charm/.test(n))); assert(!sp.pair.notes.some(n => /both bodies/.test(n)));
    O.solo(sp, { index: 0, count: 2, rule: 'the left body' }); assert.strictEqual(sp.pair.notes.filter(n => /ONE charm/.test(n)).length, 1, 'said once');
    // a design that is not known to repeat its outline keeps the old words at the intake
    const other = O.interpretLine({ receiptId: '4196617551', updateTs: 1 }, { transactionId: '5000000002', listingId: '2', sku: 'CAT_1111', title: 'Cat and Fish Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Gold' }] }, { optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => (s === 'CAT_1111' ? { sku: 'CAT_1111', pair: { v: 1, bodies: 2, mismatched: true }, members: 4 } : null) });
    assert.strictEqual(other.pair.oneBody, undefined); assert(other.pair.notes.some(n => /made as one piece with both bodies/.test(n)), 'two different charms: today\'s words');
    pass('the intake: the Key necklace says ONE charm (1 piece, no side); the Key earrings keep Left and Right; a different-charms design keeps its old words');
  }

  /* ── 3 · the bridge's Pool ── */

  // the Key necklace of order 4196617547: ONE piece, the left key alone
  for (const [sku, qty] of [['KEY_5598', 1], ['KEY_5598', 2], ['KEYS_UNEQUAL', 1]]) {
    const w = world([sku]); w.use(sku);
    const entry = w.entries[sku], sp0 = specOf('4196617547', '5000000001', sku, 'Personalized Key Necklace', [{ name: 'Letter', value: 'A' }], entry);
    const row = lineRow('4196617547', '5000000001', sku, qty, { pair: sp0.pair, pieceCount: sp0.pieceCount * qty });
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'), pools = put.body.pools, charms = w.pages.gold.charms, whole = DESIGNS[sku](), bodies = Pair.bodiesOf(whole), wholeHash = hashOf(whole);
    assert.strictEqual(pools.length, qty, `${sku} ×${qty}: ONE piece per unit, never two keys`); assert.strictEqual(charms.length, qty);
    assert(!pools.some(p => 'side' in p || 'mirror' in p || 'bodyIndex' in p || 'groupKey' in p || 'groupSize' in p), 'no ear, no body number, no group: the same record a plain charm has');
    for (const [i, ch] of charms.entries()) {
      assert.strictEqual(ch.members.length, bodies[0].members.length, 'cut from the left body only: its members, not the other key\'s');
      assert.deepStrictEqual(J(ch.bbox), J(bodies[0].bbox), 'the sheet gets one key\'s box'); assert(ch.bbox[2] - ch.bbox[0] < whole.bbox[2] - whole.bbox[0] - 20, 'half the width of the two keys');
      assert(!('side' in ch) && !('groupKey' in ch) && !('mirror' in ch), 'no Left / Right on the sheet charm'); assert.strictEqual(ch.oneBody, 0);
      assert.deepStrictEqual(J(ch.topIndices), [0], 'the sheet writer copies the left key\'s own group only');
      assert.strictEqual(ch.name, qty > 1 ? `4196617547 · ${sku} · ${i + 1}/${qty}` : `4196617547 · ${sku}`);
      assert.strictEqual(pools[i].charmHash, ch.hash); assert.notStrictEqual(ch.hash, wholeHash, 'its hash is a body\'s, the one mark that tells it from the whole design');
    }
    assert.strictEqual(new Set(charms.map(c => c.id)).size, qty, 'every piece is its own object'); const srcCharm = w.c.B.pool.sources.get('charmnest/master/sku/' + sku + '.ai').charms[0];
    assert.strictEqual(srcCharm.poolId, undefined, 'the design\'s own charm is never written into'); assert.strictEqual(srcCharm.members.length, whole.members.length, 'and still holds both keys');
    assert.deepStrictEqual(J(row.spec.pair.oneBody), { index: 0, count: 2, rule: 'the left body' }); assert(row.spec.pair.notes.some(n => /ONE charm/.test(n)) && !row.spec.pair.notes.some(n => /both bodies|glued/.test(n)));
    assert.strictEqual(row.spec.pair.glued, false, 'not glued'); assert.deepStrictEqual(J(row.poolIds), pools.map(p => p.poolId)); assert.strictEqual(row.state, 'pooled');
    assert.strictEqual(w.Pool.pieceCountOf(row), qty, 'counted as ' + qty + ' piece(s)');
  }
  pass('a Key necklace makes ONE piece per unit, cut from the left key alone: no side, no group, a body\'s hash, the design\'s own charm untouched, noted as ONE charm');

  // two different charms of one design (a ball and a racket) on the same kind of line: today\'s one piece holding both
  {
    const w = world(['TENNIS_MIS']); w.use('TENNIS_MIS');
    const sp0 = specOf('4196617560', '5000000060', 'TENNIS_MIS', 'Tennis Ball and Racket Necklace', [], w.entries.TENNIS_MIS);
    const row = lineRow('4196617560', '5000000060', 'TENNIS_MIS', 1, { pair: sp0.pair, pieceCount: sp0.pieceCount });
    await w.Pool.poolAdd(row, null);
    const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools, ch = w.pages.gold.charms[0], whole = DESIGNS.TENNIS_MIS();
    assert.strictEqual(pools.length, 1); assert.strictEqual(ch.members.length, whole.members.length, 'both bodies, as before'); assert.strictEqual(pools[0].charmHash, hashOf(whole)); assert.strictEqual(ch.oneBody, undefined);
    assert.strictEqual(row.spec.pair.oneBody, undefined);
    assert.strictEqual(w.Pool.pieceCountOf(Object.assign({}, row, { poolIds: [] })), 1, 'a necklace is one piece per unit even where the design draws two bodies (it was counted 2)');
  }
  pass('two clearly different charms of one design keep what they made: one piece holding both bodies');

  // an earring pair of the very same design: UNCHANGED: a Left and a Right per unit, each its own key
  {
    const w = world(['KEYS_UNEQUAL']); w.use('KEYS_UNEQUAL');
    const sp0 = specOf('4196617570', '5000000070', 'KEYS_UNEQUAL', 'Key Stud Earrings', [], w.entries.KEYS_UNEQUAL);
    const row = lineRow('4196617570', '5000000070', 'KEYS_UNEQUAL', 1, { pair: sp0.pair, pieceCount: sp0.pieceCount, form: 'earrings' });
    await w.Pool.poolAdd(row, null);
    const pools = w.calls.find(x => x.body.op === 'poolPut').body.pools, charms = w.pages.gold.charms, bodies = Pair.bodiesOf(DESIGNS.KEYS_UNEQUAL());
    assert.strictEqual(pools.length, 2); assert.deepStrictEqual(J(pools.map(p => [p.side, p.bodyIndex, p.groupSize, p.mirror])), [['L', 0, 2, false], ['R', 1, 2, true]]);
    assert.notStrictEqual(pools[0].charmHash, pools[1].charmHash); assert.strictEqual(charms[0].members.length, bodies[0].members.length); assert.strictEqual(charms[1].members.length, bodies[1].members.length);
    assert(charms.every(c => c.oneBody === undefined), 'no piece of an earring pair is a "one body" piece'); assert.strictEqual(row.spec.pair.oneBody, undefined);
    assert.strictEqual(w.Pool.pieceCountOf(Object.assign({}, row, { poolIds: [] })), 2, 'two pieces per unit');
  }
  pass('the same design as an earring pair: a Left and a Right (the Right turned over, no facing set), each its own key, exactly as before');

  // a line pooled BEFORE this (its pool row holds the whole design's hash): made again, it is cut whole as it was; the words about one body are taken back
  {
    const w = world(['KEY_5598']); w.use('KEY_5598');
    const entry = w.entries.KEY_5598, whole = DESIGNS.KEY_5598(), wholeHash = hashOf(whole);
    const sp0 = specOf('4196617580', '5000000080', 'KEY_5598', 'Personalized Key Necklace', [], entry);
    const row = lineRow('4196617580', '5000000080', 'KEY_5598', 1, { pair: sp0.pair, pieceCount: 1 });
    row.poolIds = ['4196617580_5000000080_1']; w.c.B.pool.rows.set(row.poolIds[0], { poolId: row.poolIds[0], state: 'ready', charmHash: wholeHash, quantity: 1, sku: 'KEY_5598' });
    w.Pool.pinPooled(row);
    assert.strictEqual(row.spec.pair.oneBody, undefined, 'an old line: the words about one body are taken back'); assert(row.spec.pair.notes.some(n => /both bodies/.test(n)));
    assert.strictEqual(w.oneRow(row), -1, 'and its picture is the whole design it was cut as');
    await w.Pool.poolAdd(row, null);
    const ch = w.pages.gold.charms[0]; assert.strictEqual(ch.members.length, whole.members.length, 'cut whole, both keys, exactly as it was'); assert.strictEqual(ch.hash, wholeHash); assert.strictEqual(row.spec.pair.oneBody, undefined);
    // a new line with the same design is the opposite
    const fresh = lineRow('4196617581', '5000000081', 'KEY_5598', 1, { pair: specOf('4196617581', '5000000081', 'KEY_5598', 'Personalized Key Necklace', [], entry).pair, pieceCount: 1 });
    assert.strictEqual(w.oneRow(fresh), 0, 'a fresh Key necklace is drawn as the left key');
  }
  pass('a line pooled before this keeps its whole charm and its words; a fresh line is the one body');

  // what is stored reads back: the piece's charm hash says whole or one body (the pool row, the sheet record)
  {
    const w = world(['KEY_5598']); w.use('KEY_5598');
    const entry = w.entries.KEY_5598; const s = await w.Pool.masterCharm(entry, null), whole = s.charms[0], bodies = Pair.bodiesOf(whole);
    const solo = await w.Pool.soloBodies(s); assert.strictEqual(solo.length, 2); assert.strictEqual(solo[0].members.length, bodies[0].members.length);
    const left = await w.Pool.ensureBase(s, null, { charmHash: solo[0].hash }); assert.strictEqual(left, solo[0], 'a pool row\'s hash of the left key: the left key');
    const fromSheet = await w.Pool.ensureBase(s, null, { hash: solo[0].hash }); assert.strictEqual(fromSheet, solo[0], 'a sheet record\'s hash: the same');
    assert.strictEqual(await w.Pool.ensureBase(s, null, { charmHash: whole.hash }), whole, 'an old record (the whole design\'s hash): the whole design');
    assert.strictEqual(await w.Pool.ensureBase(s, null, { charmHash: 'zzzzzzzz' }), whole, 'a hash it does not know: the whole design, as before');
    assert.strictEqual(await w.Pool.ensureBase(s, null), whole, 'no record: the whole design');
    // the sheet window's way (charm-nest-sheetwin.js pieceBase): a piece with no ear and a body's hash is that body; an ear piece goes through ensureBase; everything else the whole design
    const swSrc = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8'), pa = swSrc.indexOf('async function pieceBase(g, x) {'), pb = swSrc.indexOf('async function sourceGeom', pa);
    assert(pa > 0 && pb > pa); const sw = { Pool: w.Pool, console, window: null }; sw.window = sw; vm.createContext(sw); vm.runInContext(swSrc.slice(pa, pb) + '\nthis.pieceBase = pieceBase;', sw);
    const g = { base: whole, src: s };
    assert.strictEqual(await sw.pieceBase(g, { poolId: 'p', hash: solo[0].hash, side: null }), solo[0], 'the sheet window draws the one key');
    assert.strictEqual(await sw.pieceBase(g, { poolId: 'p', hash: whole.hash, side: null }), whole, 'an old sheet: both keys, as cut');
    assert.strictEqual(await sw.pieceBase(g, { poolId: 'p', side: null }), whole, 'a record with no hash: as before');
    // the pictures: Master tab and the Review row
    const sku = 'KEY_5598'; const row = lineRow('4196617547', '5000000001', sku, 1, { pair: specOf('4196617547', '5000000001', sku, 'Personalized Key Necklace', [], entry).pair, pieceCount: 1 });
    assert.strictEqual(w.oneRow(row), 0); assert.strictEqual(w.pairRow(row), false, 'the Review row draws no Left + Right for a necklace of two keys');
    const pic = await w.Pool.masterPreview(entry, null, true, { one: 0 });
    assert.strictEqual(pic, 'picture'); const shown = w.shown.at(-1).charm;
    assert.strictEqual(Pair.bodiesOf(shown).length, 1, 'the picture is handed ONE key'); assert.strictEqual(Pair.isMismatched(shown), false, 'so the thumbnail plan draws no Left / Right chips'); assert.deepStrictEqual(J(shown.bbox), J(bodies[0].bbox));
    await w.Pool.masterFront(entry, null, 600, { one: 0 }); assert.strictEqual(Pair.bodiesOf(w.shown.at(-1).charm).length, 1);
    await w.Pool.masterPreview(entry, null, true); assert.strictEqual(Pair.bodiesOf(w.shown.at(-1).charm).length, 2, 'the Master tab and an earring line still draw both keys');
    const earRow = lineRow('4196617548', '5000000002', sku, 1, { pair: specOf('4196617548', '5000000002', sku, 'Key Stud Earrings', [], entry).pair, pieceCount: 2, form: 'earrings' });
    assert.strictEqual(w.oneRow(earRow), -1); assert.strictEqual(w.pairRow(earRow), true, 'an earring line of the design draws its Left + Right');
    // the efficiency tiles, the order window and the sheet window ask for the same key
    const place = await w.Pool.ensureBase(s, null, { charmHash: solo[0].hash }); assert.strictEqual(Pair.bodiesOf(place).length, 1);
  }
  pass('the stored hash is read back (pool row, sheet record, sheet window); the picture of the line is one key; an earring line still draws Left + Right');

  // the stickers and the ear of a single earring that names no ear of such a design: none
  {
    const entry = Object.assign(entryOf('KEY_5598'), { sku: 'KEY_5598' }), spec = specOf('4196617590', '5000000090', 'KEY_5598', 'Single Key Earring', [], entry);
    const row = { spec: Object.assign({}, spec, { quantity: 1 }), line: { sku: 'KEY_5598', title: 'Single Key Earring' } };
    assert.strictEqual(Labels.earPieces(row, entry), null, 'one charm, no Left sticker and no Right sticker');
    const ear = specOf('4196617591', '5000000091', 'KEY_5598', 'Key Stud Earrings', [], entry);
    assert.deepStrictEqual(J(Labels.earPieces({ spec: Object.assign({}, ear, { quantity: 1 }), line: { sku: 'KEY_5598', title: 'Key Stud Earrings' } }, entry)).map(p => p.side), ['L', 'R'], 'the earring pair keeps its stickers');
  }
  pass('stickers: a single earring that names no ear prints none; an earring pair prints Left and Right');

  /* ── 4 · the real master files, when they are there ── */
  const samples = '/mnt/project-files/plans/pairs-1009/PAIRMASTER-samples';
  if (fs.existsSync(path.join(samples, 'KEY_5598.ai')) && fs.existsSync(path.join(root, 'vendor/pdf-lib-1.17.1.min.js'))) {
    const g = globalThis; g.window = g; g.self = g; g.PDFLib = require(path.join(root, 'vendor/pdf-lib-1.17.1.min.js'));
    require(path.join(root, 'charm-nest-geom.js')); require(path.join(root, 'charm-nest-vector.js')); require(path.join(root, 'charm-nest-pdf.js'));
    const P = g.CharmNestPDF;
    const charmOf = async f => { const parsed = await P.parseSource(new Uint8Array(fs.readFileSync(path.join(samples, f))), f), gr = P.groupCharms(parsed, { minPt: 6 }); const charm = gr.charms.reduce((x, y) => (y.bbox[2] - y.bbox[0]) * (y.bbox[3] - y.bbox[1]) > (x.bbox[2] - x.bbox[0]) * (x.bbox[3] - x.bbox[1]) ? y : x); for (const c of gr.charms) if (c !== charm) { for (const m of c.members) if (!charm.members.includes(m)) charm.members.push(m); charm.bbox = [Math.min(charm.bbox[0], c.bbox[0]), Math.min(charm.bbox[1], c.bbox[1]), Math.max(charm.bbox[2], c.bbox[2]), Math.max(charm.bbox[3], c.bbox[3])]; } P.integrateRings(charm); return charm; };
    const key = await charmOf('KEY_5598.ai'), mit = await charmOf('MISMATCHED_7134.ai'), heart = await charmOf('HEART_2206.ai');
    const dk = Pair.bodiesAlike(key), dm = Pair.bodiesAlike(mit);
    assert(dk && dk.alike && dk.parts[0].same >= .9, 'the real KEY_5598: two keys of one outline (' + (dk && dk.parts[0].same) + ')');
    assert(dm && dm.alike && dm.parts[0].same >= .99, 'the real MISMATCHED_7134: two mittens of one outline');
    assert(!(Pair.bodiesAlike(heart) && Pair.bodiesAlike(heart).alike) || Pair.bodiesOf(heart).length < 2, 'the real HEART_2206 (a matching design or two different halves) is not turned into one body by the test');
    assert.strictEqual(Pair.soloOf({ receiptId: '4196617547', transactionId: '1', quantity: 1, form: 'necklace', spec: { quantity: 1, pair: { earring: false } } }, key).index, 0);
    pass('real master geometry: KEY_5598 and MISMATCHED_7134 are the same outline twice (one body on a necklace)');
  } else console.log('  - the real master samples are not here: skipped');
  console.log('key-one: ok');
})().catch(e => { console.error(e); process.exit(1); });
