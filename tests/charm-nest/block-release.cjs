// BLOCKREL + RELTOQ (Paul, 17 Oct 2026, orders 4193111740 SKU DRAGON11 stud earrings and 4191826593 SKU FLORALS2 earrings: "rejected incorrectly", "SKU DRAGON11 blocked: The charm outline is front artwork. Reload its
// CUT geometry before fitting engraving."; "I asked you to add a button so that I can release orders that are flagged for review back to the production/nesting queue").
//   The indexer runs the back-view (flip) check on every charm and writes whatever it throws into entry.blocked; every reader of .blocked then refused to cut ANY order of that SKU. But that check is about ENGRAVING a back,
//   and an earring is never engraved. So a block that is ONLY the flip check's does not stop cutting (O.entryBlock); a genuine block (an open outline, a ring left loose, two master files, no outline) still does, and a person
//   may release those lines anyway ("Release to nesting queue": row.blockOverride = { sku, by, at }, for that SKU on that line only, kept with the run's lines like materialOverride / sizeOverride).
// Offline, node only; the real charm-nest-orders.js, the bridge's real Pool (makePool) and line record in a vm with stand-ins, the real charm-nest-order-release.js:   node tests/charm-nest/block-release.cjs
//   A  entryBlock: the exact message of the two orders, every flip message, and every genuine block (open, ring, two master files, a clash, an unknown text) the way the three indexers write them
//   B  the reading (interpretLine): a flip-only block raises no blockedSku; a genuine one does; a person's release of that SKU sets it aside, a release of another SKU does not; the sizes are still checked
//   C  the pool (makePool): flip-only → pooled; genuine → held with the blockedSku problem; released → pooled; a release of another SKU / of a design with no outline → still held; a pair of two designs and a set read it too
//   D  the line record: blockOverride round-trips through lineRecord / rowFromRecord, and the resume keep-list and Carry.adopt carry it
//   E  the engine (OrderHold.release): a line held only by a flip-only block is released in SUCCESS; a genuine block ends in the old failure (with the hint to the Review button); a released line ends in success
//   F  the Review button's logic (releaseOf / releaseToQueue, the page's own code sliced over stand-ins): NOBLOCK (Paul, 19 Oct 2026: nothing may stop the user pressing Release Hold): every card with an order behind it is pressable, a
//      question still open is said in the title and after the press, only an order that left the run and a Custom Order have none; what each kind does, the timeline answer
'use strict';
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

const EXACT = 'The charm outline is front artwork. Reload its CUT geometry before fitting engraving.';   // what the two orders said
const FLIP = 'flip check failed — the flipped back does not match the front; a cut-out does not stay open when the charm is flipped';   // BackViewError, charm-nest-geom.js

(async () => {
  /* ── A · entryBlock ── */
  const flipOnly = [
    ['the message of the two orders', { blocked: EXACT }],
    ['a BackViewError', { blocked: FLIP }],
    ['a BackViewError, the flipped-back words alone', { blocked: 'flip check failed — the flipped back covers a different area' }],
    ['front-only detail', { blocked: 'flip check failed — front-only detail would show on the back' }],
    ['a worker that timed out', { blocked: 'Background calculation timed out. Retry this item.' }],
    ['a worker that stopped', { blocked: 'Background calculation stopped. Retry this item.' }],
    ['written by the browser indexer, with its own flag', { blocked: EXACT, flipOk: false, open: false }],
    ['a flip failure of a kind nobody has seen, from the in-page indexer (flipOk false, no genuine word)', { blocked: "Cannot read properties of undefined (reading 'x')", flipOk: false }],
    ['a pair design built from two bodies whose back view failed', { blocked: EXACT, pair: { v: 1, bodies: 2, mismatched: true } }]
  ];
  for (const [what, e] of flipOnly) { eq(O.entryBlock(e), '', 'cutting is not held by ' + what); eq(O.blockKind(e), 'flip', 'it is a flip-only block: ' + what); }
  const genuine = [
    ['an open outline', { blocked: 'open outline', open: true }],
    ['an open outline, text only (the server indexers store the flag too, but the text stands alone)', { blocked: 'open outline' }],
    ['an open outline AND the flip message (the indexer writes both, joined by "; ")', { blocked: 'open outline; ' + EXACT }],
    ['an open outline whose text lost its words but whose flag is set', { blocked: EXACT, open: true }],
    ['a flip message and a ring left loose (appended last)', { blocked: EXACT + '; detached ring not merged' }],
    ['a ring left loose alone', { blocked: 'detached ring not merged' }],
    ['a BackViewError and a ring', { blocked: FLIP + '; detached ring not merged' }],
    ['two master files (putIndex)', { blocked: 'SKU present in two master files: A.ai and B.ai' }],
    ['a clash record on an entry whose text is only the flip message', { blocked: EXACT, conflict: { with: 'abc' } }],
    ['a design saved from an order', { blocked: 'this name is a design saved from an order: it is kept as it is' }],
    ['the script indexer failing on a charm', { blocked: 'not written: boom' }],
    ['a reason nobody wrote (a hand edit)', { blocked: 'needs checking' }],
    ['the in-page flag of a flip failure with an open outline', { blocked: EXACT, flipOk: false, open: true }],
    ['the in-page flag with a genuine word', { blocked: 'open outline; ' + EXACT, flipOk: false }]
  ];
  for (const [what, e] of genuine) { eq(O.entryBlock(e), e.blocked, 'cutting is held by ' + what); eq(O.blockKind(e), 'genuine', 'it is a genuine block: ' + what); }
  for (const e of [null, undefined, {}, { blocked: null }, { blocked: '' }]) { eq(O.entryBlock(e), '', 'no block: ' + JSON.stringify(e)); eq(O.blockKind(e), '', 'no kind: ' + JSON.stringify(e)); }
  // a person's release
  ok(O.blockReleased({ sku: 'dragon11' }, ' Dragon11 ') && !O.blockReleased({ sku: 'DRAGON11' }, 'FLORALS2') && !O.blockReleased(null, 'DRAGON11') && !O.blockReleased({}, 'DRAGON11') && !O.blockReleased({ sku: 'DRAGON11' }, ''), 'a release names a SKU, upper case or not, and only that SKU');
  ok(O.blockReleased({ sku: 'A', skus: ['A', 'B'] }, 'B') && O.blockReleased([{ sku: 'A' }, { sku: 'B' }], 'B') && !O.blockReleased({ sku: 'A', skus: ['A', 'B'] }, 'C'), 'a release of several SKUs of one line');
  eq([O.blockFor({ blocked: 'open outline' }, 'X', { sku: 'X' }), O.blockFor({ blocked: 'open outline' }, 'X', { sku: 'Y' }), O.blockFor({ blocked: 'open outline' }, 'X'), O.blockFor({ blocked: EXACT }, 'X')], ['', 'open outline', 'open outline', ''], 'blockFor: the cutting block unless released');
  // the three indexers compose the reasons from exactly these parts: the real texts, as they are written, classify as the table above says
  for (const f of ['scripts/index-master.cjs', 'netlify/functions/charmMaster-background.js', 'charm-nest-bridge.js']) {
    const src = fs.readFileSync(path.join(root, f), 'utf8');
    ok(/"open outline"/.test(src) && /detached ring not merged/.test(src), f + ' still writes the reasons the classifier knows');
  }
  ok(/The charm outline is front artwork\. Reload its CUT geometry before fitting engraving\./.test(fs.readFileSync(path.join(root, 'charm-nest-geom.js'), 'utf8')), 'the geometry still throws the message the classifier knows');
  ok(/"flip check failed — "/.test(fs.readFileSync(path.join(root, 'charm-nest-geom.js'), 'utf8')), 'and BackViewError still starts so');
  ok(/SKU present in two master files/.test(fs.readFileSync(path.join(root, 'netlify/functions/_charmNestMaster.js'), 'utf8')), 'and putIndex still writes the clash so');
  pass('A · entryBlock: only the flip check\'s own messages stop being a block; every genuine block still is one');

  /* ── B · the reading ── */
  const ENT = {};
  const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: s => ENT[String(s || '').toUpperCase()] || null, masterLoose: () => '' }, extra || {});
  const ORDER = { receiptId: '4193111740', updateTs: 1791839563 };
  const dragon = over => Object.assign({ transactionId: '5247356127', listingId: '1700000001', sku: 'DRAGON11', title: 'Dragon Stud Earrings, Personalized Charm Earrings', quantity: 1, metalKey: 'rose', metalLabel: 'Rose Gold Filled', personalization: [], variations: [{ name: 'Metal', value: 'Rose Gold Filled' }] }, over || {});
  const kinds = sp => (sp.problems || []).map(p => p.kind);
  const design = over => Object.assign({ sku: 'DRAGON11', members: 3, holes: 0, backKeepOut: [], widthPt: 20, heightPt: 24, areaPt2: 400, engravable: false }, over || {});
  ENT.DRAGON11 = design({ blocked: EXACT });
  let sp = O.interpretLine(ORDER, dragon(), ctx());
  eq([kinds(sp), sp.designSku, sp.skuSource], [[], 'DRAGON11', 'transaction'], 'DRAGON11: the flip check\'s message no longer raises a blockedSku question; the line is its design');
  ok(sp.engraveNever, 'and it is an earring: never engraved (the block was about engraving)');
  ENT.DRAGON11 = design({ blocked: 'open outline', open: true });
  sp = O.interpretLine(ORDER, dragon(), ctx());
  eq(kinds(sp), ['blockedSku'], 'an open outline still raises it'); eq(sp.problems[0], { kind: 'blockedSku', reason: 'open outline', sku: 'DRAGON11' }, 'with the entry\'s own reason');
  ENT.DRAGON11 = design({ blocked: 'open outline; ' + EXACT });
  sp = O.interpretLine(ORDER, dragon(), ctx()); eq([kinds(sp), sp.problems[0].reason], [['blockedSku'], 'open outline; ' + EXACT], 'open outline AND the flip message: held, and the reason says both');
  ENT.DRAGON11 = design({ blocked: 'SKU present in two master files: A and B', conflict: { with: 'x' } });
  eq(kinds(O.interpretLine(ORDER, dragon(), ctx())), ['blockedSku'], 'two master files still raise it');
  // a person's release
  ENT.DRAGON11 = design({ blocked: 'open outline', open: true });
  const rel = { sku: 'DRAGON11', by: 'Paul', at: 1 };
  sp = O.interpretLine(ORDER, dragon(), ctx({ blockOverride: rel })); eq([kinds(sp), sp.designSku], [[], 'DRAGON11'], 'a person\'s release of DRAGON11 sets its block aside for the line');
  sp = O.interpretLine(ORDER, dragon(), ctx({ blockOverride: { sku: 'FLORALS2', by: 'Paul', at: 1 } })); eq(kinds(sp), ['blockedSku'], 'a release of another SKU sets nothing aside');
  sp = O.interpretLine(ORDER, dragon(), ctx({ blockOverride: { sku: 'x', skus: ['x', 'dragon11'], by: 'Paul', at: 1 } })); eq(kinds(sp), [], 'a release of several SKUs, one of them this one');
  // the size is still read for a released (or flip-only) design: the blocked branch used to skip it
  ENT.DRAGON11 = design({ blocked: EXACT, sizes: { S: { aiPath: 'a' }, M: { aiPath: 'b' } } });
  sp = O.interpretLine(ORDER, dragon({ variations: [{ name: 'Metal', value: 'Rose Gold Filled' }, { name: 'Size', value: 'XL' }] }), ctx());
  ok(kinds(sp).includes('missingSize') && !kinds(sp).includes('blockedSku'), 'a flip-only SKU is checked for size like any design: ' + JSON.stringify(kinds(sp)));
  // an exact match that is flip-only blocked is a match like any other: a saved link to another design still wins (the person's word), none changes the SKU
  ENT.DRAGON11 = design({ blocked: EXACT }); ENT.OTHER = design({ sku: 'OTHER' });
  sp = O.interpretLine(ORDER, dragon(), ctx({ aliases: { '1700000001': { v: 2, bySku: { DRAGON11: 'OTHER' } } } })); eq([sp.designSku, sp.skuSource, kinds(sp)], ['OTHER', 'alias', []], 'a person\'s saved link still wins');
  // FLORALS2 (the second order): silver earrings
  ENT.FLORALS2 = design({ sku: 'FLORALS2', blocked: 'flip check failed — the flipped back does not match the front' });
  sp = O.interpretLine({ receiptId: '4191826593', updateTs: 1 }, dragon({ sku: 'FLORALS2', title: 'Floral Earrings, Silver Dangle Earrings', metalKey: 'silver', metalLabel: 'Sterling Silver', variations: [{ name: 'Metal', value: 'Sterling Silver' }] }), ctx());
  eq([kinds(sp), sp.designSku], [[], 'FLORALS2'], 'FLORALS2: the same');
  // the lookalike rule treats a flip-only entry as the design it is (a blocked one was skipped)
  const look = O.lookalikeRead('DRAGON11', { form: 'necklace' }, { masterEntry: s => ENT[s] || null, masterLike: () => ['DRAGON11', 'DRAGON 11'] });
  ok(look === null || typeof look === 'object', 'lookalikeRead still answers');
  pass('B · the reading: a flip-only block raises no question; a genuine one does; a person\'s release sets it aside for its SKU only');

  /* ── C · the pool ── */
  const F = require('./pairs-fixtures.cjs');
  const keyWorld = require('./_keyWorld.cjs')(root);
  const { DESIGNS, world, lineRow } = keyWorld;
  DESIGNS.DRAGON11 = () => F.charmOf('ONE-PENDANT'); DESIGNS.SECOND = () => F.charmOf('PAIR-STUD'); DESIGNS.THIRD = () => F.charmOf('PAIR-HOOP');
  const worldOf = (list, patch) => { const w = world(list || ['DRAGON11']); w.c.P.parseSource = async (_b, name) => ({ name }); w.c.P.groupCharmsAsync = async parsed => ({ charms: [structuredClone(DESIGNS[String(parsed.name).replace(/\.ai$/, '').replace(/^.*\//, '')]())], orphans: [] }); for (const [k, v] of Object.entries(patch || {})) Object.assign(w.entries[k], v); return w; };
  const rowOf = over => Object.assign(lineRow('4193111740', '5247356127', 'DRAGON11', 1, { material: 'gold' }), { line: { transactionId: '5247356127', listingId: '1700000001', sku: 'DRAGON11', title: 'Dragon Stud Earrings', quantity: 1 } }, over || {});
  const poolCalls = w => w.calls.filter(c => c.body && c.body.op === 'poolPut').length;
  // (the intake's own pass, Pool.addAll: it also writes the line's retry signature, which reads the release)
  const addAll = async (w, rows) => { const c = w.c; c.Orders.rows = () => rows; c.Orders.lineRecord = r => [r.key, {}]; c.Gate.plan = async () => ({ wait: new Map() }); c.Gate.afterPool = async () => {}; c.Review.syncOrderItems = () => {}; for (const f of ['renderRail', 'updateTopSub', 'refreshAllCards']) c[f] = () => {}; c.RunCtl.save = async () => {}; return w.Pool.addAll({ runId: 'r', lines: {} }); };
  const tryPool = async (patch, over) => { const w = worldOf(['DRAGON11'], { DRAGON11: patch }), row = rowOf(over); await addAll(w, [row]); return { w, row }; };
  { const { w, row } = await tryPool({ blocked: EXACT });
    eq([row.state, row.problems.length, poolCalls(w)], ['pooled', 0, 1], 'makePool: a flip-only block does not hold the line: it is pooled'); }
  { const { w, row } = await tryPool({ blocked: 'open outline', open: true });
    eq([row.state, row.problems.map(p => p.kind + ':' + p.sku + ':' + p.reason), poolCalls(w)], ['held', ['blockedSku:DRAGON11:open outline'], 0], 'an open outline holds it, as ever, with the entry\'s reason');
    ok(row.reason === 'SKU blocked: open outline' && typeof row.poolTry === 'string' && row.poolTry.length > 10, 'and the line\'s reason is the old one, not an error of the pool: ' + row.reason);
    ok(/DRAGON11/.test(row.poolTry) && /open outline/.test(row.poolTry), 'with its retry signature written'); }
  { const { w, row } = await tryPool({ blocked: 'open outline', open: true }, { blockOverride: { sku: 'DRAGON11', by: 'Paul', at: 1 } });
    eq([row.state, row.problems.length, poolCalls(w)], ['pooled', 0, 1], 'a person\'s release of DRAGON11 pools the line although the master still blocks it'); }
  { const { row } = await tryPool({ blocked: 'open outline', open: true }, { blockOverride: { sku: 'FLORALS2', by: 'Paul', at: 1 } });
    eq([row.state, row.problems.map(p => p.kind)], ['held', ['blockedSku']], 'a release of another SKU does not'); }
  { // a line held for a genuine block is tried again the moment a person releases it: the signature of its last try changes with the release
    const w = worldOf(['DRAGON11'], { DRAGON11: { blocked: 'open outline', open: true } }), row = rowOf();
    await addAll(w, [row]); const was = row.poolTry; eq([row.state, typeof was], ['held', 'string'], 'held, with the signature of this try');
    row.blockOverride = { sku: 'DRAGON11', by: 'Paul', at: 7 }; Object.assign(row.spec, { problems: [] }); row.problems = [];
    await addAll(w, [row]); eq(row.state, 'pooled', 'the next intake pass pools it: the release changed what the line is tried on'); }
  { const { w, row } = await tryPool({ blocked: 'detached ring not merged; ' + EXACT });
    eq([row.state, poolCalls(w)], ['held', 0], 'a ring left loose holds it even with the flip message beside it'); }
  { // the entry says it has area, but the traced design has none: nothing to cut, whoever asks
    const w = worldOf(['DRAGON11']), row = rowOf({ blockOverride: { sku: 'DRAGON11', by: 'Paul', at: 1 } });
    w.c.P.buildSilhouettes = async (_p, charms) => { for (const ch of charms) Object.assign(ch, { areaPt2: 0, widthPt: 0, heightPt: 0 }); return charms; };
    await w.Pool.poolAdd(row, null);
    eq([row.state, row.problems.map(p => p.kind), row.problems[0] && row.problems[0].noOutline, poolCalls(w)], ['held', ['blockedSku'], true, 0], 'a design with no outline to cut stays held whatever a person says, and the question says so (noOutline)'); }
  { // a pair of two designs (the Left ear from the first design, the Right ear from the second): the second design's block is read the same way
    const pairOf = () => ({ earring: true, mismatched: true, members: [{ sku: 'DRAGON11', side: 'L' }, { sku: 'SECOND', side: 'R' }], notes: [], asks: [], sides: ['L', 'R'] });
    const mk2 = patch => { const w = worldOf(['DRAGON11', 'SECOND'], { SECOND: patch }); const row = rowOf(); row.spec = Object.assign({}, row.spec, { form: 'earrings', pair: pairOf(), pieceCount: 2 }); return { w, row }; };
    let t = mk2({ blocked: 'open outline', open: true }); await t.w.Pool.poolAdd(t.row, null);
    eq([t.row.state, t.row.problems.map(p => p.kind + ':' + p.sku), poolCalls(t.w)], ['held', ['blockedSku:SECOND'], 0], 'a pair of two designs: the genuine block of the second design holds the line');
    t = mk2({ blocked: EXACT }); await t.w.Pool.poolAdd(t.row, null);
    eq([t.row.state, t.row.problems.length, poolCalls(t.w)], ['pooled', 0, 1], 'and its flip-only block holds nothing');
    t = mk2({ blocked: 'open outline', open: true }); t.row.blockOverride = { sku: 'SECOND', by: 'Paul', at: 1 }; await t.w.Pool.poolAdd(t.row, null);
    eq([t.row.state, t.row.problems.length], ['pooled', 0], 'and a release of the second design pools the pair');
    // a set of two designs on a necklace: each design is read the same way
    const mk3 = patch => { const w = worldOf(['DRAGON11', 'THIRD'], { THIRD: patch }); const row = rowOf(); row.spec = Object.assign({}, row.spec, { form: 'necklace', pair: { earring: false, set: { designs: ['DRAGON11', 'THIRD'], source: 'alias' }, notes: [], asks: [], sides: [null, null] }, pieceCount: 2 }); return { w, row }; };
    t = mk3({ blocked: 'open outline', open: true }); await t.w.Pool.poolAdd(t.row, null);
    eq([t.row.state, t.row.problems.map(p => p.kind + ':' + p.sku)], ['held', ['blockedSku:THIRD']], 'a set of two designs: the genuine block of the second holds the line');
    t = mk3({ blocked: FLIP }); await t.w.Pool.poolAdd(t.row, null);
    ok(!t.row.problems.some(p => p.kind === 'blockedSku'), 'and its flip-only block does not (' + t.row.state + ')'); }
  pass('C · the pool: flip-only → pooled; genuine → held; released → pooled; no outline → held whatever is said');

  /* ── D · the line record ── */
  {
    const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
    const cut = (from, to) => { const a = src.indexOf(from), b = src.indexOf(to, a); assert.ok(a > 0 && b > a, 'slice ' + from.slice(0, 50)); return src.slice(a, b); };
    const SL = cut('  const cap = (v, n) =>', '  /* The claim is a courtesy');
    const c = { console, JSON, Number, String, Array, Object, Date, Math, labelOf: m => m, Master: { entryFor: () => null }, O: { cleanLine() {} } };
    vm.createContext(c); vm.runInContext(SL + '\nthis.lineRecord = lineRecord; this.rowFromRecord = rowFromRecord;', c);
    const base = { key: 'k_1', order: { receiptId: '4193111740', updateTs: 7, createTs: 1 }, line: { transactionId: '1', listingId: '9', title: 't', variations: [] }, state: 'held', poolIds: [], problems: [], spec: { designSku: 'DRAGON11', quantity: 1 } };
    const none = c.lineRecord(Object.assign({}, base))[1]; ok(!('blockOverride' in none), 'a line with no release carries none in its record');
    const rec = c.lineRecord(Object.assign({}, base, { blockOverride: { sku: 'DRAGON11', by: 'Paul', at: 5 } }))[1];
    eq(rec.blockOverride, { sku: 'DRAGON11', by: 'Paul', at: 5 }, 'the record keeps the release');
    const many = c.lineRecord(Object.assign({}, base, { blockOverride: { sku: 'A', skus: ['A', 'B'], by: 'Paul', at: 5 } }))[1]; eq(many.blockOverride.skus, ['A', 'B'], 'and a release of several SKUs');
    const back = c.rowFromRecord('k_1', JSON.parse(JSON.stringify(rec)));
    eq(back.blockOverride, { sku: 'DRAGON11', by: 'Paul', at: 5 }, 'a row made from the record has it again (a reload, the order window of another order)');
    ok(!('blockOverride' in c.rowFromRecord('k_1', JSON.parse(JSON.stringify(none)))), 'and a row made from a record without it has none');
    ok(!('blockOverride' in c.rowFromRecord('k_1', { orderId: '1', blockOverride: 'junk' })), 'junk is not a release');
    // the same list the materialOverride / sizeOverride sit in (resume), and Carry.adopt
    ok(/"materialOverride", "sizeOverride", "blockOverride", ("releaseHold", )?"engravingOverride"/.test(src), 'the resume keep-list carries it beside the other overrides');   // (RELHOLD, 18 Oct 2026: the Release Hold press's who and when sits beside it)
    ok(/if \(row && row\.blockOverride && !fresh\.blockOverride\) fresh\.blockOverride = row\.blockOverride;/.test(src), 'Carry.adopt carries it to a changed order');
    ok(/row\.sizeOverride, row\.blockOverride \? JSON\.stringify\(row\.blockOverride\) : "", m\.optionMaps/.test(src) && /row\.sizeOverride \|\| "", row\.blockOverride \? JSON\.stringify\(row\.blockOverride\) : "",/.test(src), 'the reading (inputsOf) and the pool\'s retry signature (trySig) depend on it, with no helper outside their own modules to be missing from a slice');
    // the whole cycle on the real reader: a restored row, read again, is not asked
    ENT.DRAGON11 = design({ blocked: 'open outline', open: true });
    const restored = c.rowFromRecord('k_1', JSON.parse(JSON.stringify(rec)));
    sp = O.interpretLine(ORDER, dragon(), ctx({ blockOverride: restored.blockOverride })); eq(kinds(sp), [], 'a restored release still sets the block aside');
  }
  pass('D · the line record: blockOverride is saved with the run\'s lines and comes back');

  /* ── E · the engine ── */
  {
    const rid = '4193111740';
    const rows = [], repooled = [];
    const mk = (key, o) => Object.assign({ key, order: { receiptId: rid, createTs: 1 }, line: { transactionId: '1', quantity: 1, sku: 'DRAGON11', title: 'Dragon Stud Earrings', listingId: '1700000001' }, state: 'held', reason: 'SKU blocked: ' + EXACT, hold: null, problems: [{ kind: 'blockedSku', sku: 'DRAGON11', reason: EXACT }], spec: { designSku: 'DRAGON11', quantity: 1 }, poolIds: [] }, o || {});
    globalThis.Orders = { rows: () => rows, heldNow: r => !!r.hold || r.state === 'held', lineRecord: r => [r.key, { state: r.state }], render() {} };
    // Review.repool, as the page does it: the line is read again (with the person's release, if any) and pooled when nothing is asked
    globalThis.Review = { syncOrderItems() {}, repool: async r => {
      repooled.push(r.key); r.hold = null;
      const sp = O.interpretLine({ receiptId: rid, updateTs: 1 }, dragon(), Object.assign(ctx(), r.blockOverride ? { blockOverride: r.blockOverride } : {}));
      r.spec = sp; r.problems = sp.problems.slice(); r.state = r.problems.length ? 'held' : 'pooled'; r.reason = r.problems.length ? 'SKU blocked: ' + r.problems[0].reason : null; r.poolIds = r.problems.length ? [] : [rid + '_1_1'];
    } };
    globalThis.B = { run: null };
    require(path.join(root, 'charm-nest-order-release.js'));
    const OH = globalThis.OrderHold;
    // the owner's orders: the flip message only. The line was held by the OLD code (its state is "held", problems from the old reading); the page now reads it with no question.
    ENT.DRAGON11 = design({ blocked: EXACT });
    rows.length = 0; repooled.length = 0; rows.push(mk('dragon'));
    let plan = await OH.releasePlan(rid); ok(plan.canRelease === true && plan.held === true, 'the plan: it can be released');
    let res = await OH.release(rid, { name: 'Paul' });
    ok(res.ok === true && res.released === true && !res.error, 'Release hold on a line held only by the flip message ends in SUCCESS: ' + JSON.stringify({ ok: res.ok, e: res.error }));
    eq([repooled, rows[0].state, rows[0].problems.length], [['dragon'], 'pooled', 0], 'the line was made up again, is pooled and asks nothing'); ok(rows[0].releasing === undefined, 'and the release mark is cleared');
    ok(rows[0].frontAt > 0, 'at the front of the queue');
    ok(res.steps.some(s => s.type === 'released') && res.steps.some(s => s.type === 'done') && !res.steps.some(s => s.type === 'error'), 'the steps end in released and done, with no error');
    // a line a re-reading has already turned "held" → "pulled" (the page stopped holding it) is made up again too
    rows.length = 0; repooled.length = 0; rows.push(mk('dragon', { state: 'held' }));
    const mid = Object.getOwnPropertyDescriptor(rows[0], 'state'); void mid;
    let flipped = false; const wasRepool = globalThis.Review.repool;
    globalThis.Orders.heldNow = r => { if (!flipped && r.releasing) { flipped = true; r.state = 'pulled'; r.problems = []; return false; } return !!r.hold || r.state === 'held'; };
    res = await OH.release(rid, { name: 'Paul' });
    ok(res.ok === true && repooled.includes('dragon'), 'a line that stopped being "held" while the release saved its place is made up again all the same: ' + JSON.stringify({ ok: res.ok, e: res.error, repooled }));
    globalThis.Orders.heldNow = r => !!r.hold || r.state === 'held'; globalThis.Review.repool = wasRepool;
    // a genuine block: the old failure, and the hint to the button that cuts it anyway
    ENT.DRAGON11 = design({ blocked: 'open outline', open: true });
    rows.length = 0; repooled.length = 0; rows.push(mk('dragon', { problems: [{ kind: 'blockedSku', sku: 'DRAGON11', reason: 'open outline' }], reason: 'SKU blocked: open outline' }));
    res = await OH.release(rid, { name: 'Paul' });
    ok(res.ok === false && res.released === true && /needs a look in Review: SKU blocked: open outline\.? Its Review row has “Release Hold” to cut it anyway\.$/.test(res.error), 'an open outline still ends in "needs a look in Review", and points at the Review button: ' + res.error);
    ok(!/\.\./.test(res.error), 'with no doubled full stop: ' + res.error);
    // ...and with the person's release of that SKU: success
    rows[0].blockOverride = { sku: 'DRAGON11', by: 'Paul', at: 1 }; repooled.length = 0;
    res = await OH.release(rid, { name: 'Paul' });
    ok(res.ok === true && rows[0].state === 'pooled', 'released by a person: the same line is pooled and the release ends in success: ' + JSON.stringify({ ok: res.ok, e: res.error, s: rows[0].state }));
    // a design with no outline: no hint (nothing can release it)
    rows.length = 0; rows.push(mk('none', { problems: [{ kind: 'blockedSku', sku: 'DRAGON11', reason: 'its design has no outline to cut', noOutline: true }], reason: 'SKU blocked: its design has no outline to cut' }));
    globalThis.Review.repool = async r => { r.hold = null; r.state = 'held'; r.problems = [{ kind: 'blockedSku', sku: 'DRAGON11', reason: 'its design has no outline to cut', noOutline: true }]; };
    res = await OH.release(rid, { name: 'Paul' });
    ok(res.ok === false && /no outline to cut\.$/.test(res.error) && !/Release to nesting queue/.test(res.error), 'a design with no outline gets no hint: ' + res.error);
    // the toast's text: HoldUI strips the reason's own full stop
    const ui = fs.readFileSync(path.join(root, 'charm-nest-hold-ui.js'), 'utf8');
    ok(/msgOf\(out\.res\.error\)\.replace\(\/\[\.\\s\]\+\$\/, ''\)\}\. What was done is kept, and Release hold can be pressed again/.test(ui), 'and the toast does not print ".."');
  }
  pass('E · the engine: a line held only by the flip message is released in success; a real block ends as before, with the hint; a released one succeeds');

  /* ── F · the Review button's logic ── */
  {
    const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
    // (RELHOLD, Paul 18 Oct 2026: the button is the card's "Release Hold" now; its block ends where the button is drawn)
    const a = src.indexOf('  const RELEASE_LABEL = "Release Hold";'), b = src.indexOf('  /** RELHOLD: the one Release Hold button', a);
    assert.ok(a > 0 && b > a, 'the RELTOQ block is where the test expects it');
    const calls = { toasts: [], answers: [], repooled: [], released: [], otherMarked: [], resolved: [], accepted: [], saved: 0, synced: 0, agent: [] };
    const rowsAll = [];
    const mkRow = (key, rid, o) => Object.assign({ key, order: { receiptId: rid, createTs: +rid.slice(-3) }, line: { sku: 'DRAGON11' }, state: 'held', hold: null, problems: [{ kind: 'blockedSku', sku: 'DRAGON11', reason: 'open outline' }], spec: { designSku: 'DRAGON11' }, poolIds: [] }, o || {});
    const c = {
      console, Date, JSON, Set, Map, Array, Object, String, Number, Promise, Error, window: {},
      B: { run: { lines: {} } }, RunCtl: { save: async () => { calls.saved++; }, poke() {} },
      Orders: { rows: () => rowsAll, heldNow: r => !!r.hold || r.state === 'held', lineRecord: r => [r.key, { state: r.state, blockOverride: r.blockOverride || null }], render() {} },
      toast: (m, k) => { calls.toasts.push([k || '', m]); }, agent: (...x) => { calls.agent.push(x.slice(1).join(' ')); },
      by: () => 'Paul', answered: (it, type, say, detail, who) => { calls.answers.push({ kind: it.kind, type, say, detail, who }); },
      problemText: p => (p.kind === 'blockedSku' ? `SKU ${p.sku} blocked: ${p.reason}` : p.kind), syncOrderItems: () => { calls.synced++; },
      repool: async r => { calls.repooled.push(r.key); r.state = 'pooled'; r.problems = []; }, acceptChange: async it => { calls.accepted.push(it.key); },
      items: () => c.__items || [], rowsOf: it => (it.rows && it.rows.length ? it.rows : it.row ? [it.row] : []).filter(r => r.state !== 'gone'), document: {},
      mkeyOf: it => String(it.key), DONE_SW: '#done', TL: { rec: e => { (calls.tl = calls.tl || []).push(e); } }   // (RELHOLD: the press's flight and its timeline step)
    };
    c.window.OrderHold = { release: async (rid, o) => { calls.released.push([rid, o.name, rowsAll.filter(r => r.order.receiptId === rid).map(r => !!r.blockOverride)]); calls.otherMarked.push(rowsAll.filter(r => r.order.receiptId !== rid && /^o[12]_/.test(r.key)).map(r => !!r.blockOverride)); for (const r of rowsAll.filter(x => x.order.receiptId === rid)) { r.state = 'pooled'; r.problems = []; } return { ok: true, released: true, placed: true, sheets: [{ label: 'RG Sheet 2' }] }; } };
    c.window.CharmNestReviewMark = { resolve: async (rid, o) => { calls.resolved.push([rid, o.how]); return { ok: true }; } };
    vm.createContext(c); vm.runInContext(src.slice(a, b) + '\nthis.releaseOf = releaseOf; this.releaseToQueue = releaseToQueue; this.liftOrder = liftOrder;', c);
    const item = (kind, row, extra) => Object.assign({ kind, key: 'ord:' + kind + ':' + (row ? row.key : 'x'), row, rows: row ? [row] : [], problem: row && row.problems[0] || {}, why: 'w' }, extra || {});
    const R = it => { const r = c.releaseOf(it); return [r.how, r.why, r.title]; };
    // what each kind says
    const dragon1 = mkRow('a_1', '4193111740'); rowsAll.push(dragon1);
    eq(R(item('blockedSku', dragon1))[0], 'override', 'a blocked SKU can be released'); ok(/Cut DRAGON11 although the master blocks it \(open outline\)/.test(c.releaseOf(item('blockedSku', dragon1)).title) && /this piece goes to the front of the queue and onto the next sheet/.test(c.releaseOf(item('blockedSku', dragon1)).title), 'and its title says what it does: ' + c.releaseOf(item('blockedSku', dragon1)).title);
    const none = mkRow('n_1', '4193111741', { problems: [{ kind: 'blockedSku', sku: 'DRAGON11', reason: 'its design has no outline to cut', noOutline: true }] });
    ok(R(item('blockedSku', none))[0] === 'queue' && R(item('blockedSku', none))[1] === '' && /no outline to cut, so nothing can be nested/.test(R(item('blockedSku', none))[2]), 'a design with no outline can still be pressed (back in line, nothing is overridden), and its title says what is open: ' + R(item('blockedSku', none))[2]);
    eq([R(item('reviewMark', dragon1, { rid: '4193111740' }))[0], R(item('heldOrder', null, { rid: '4193111740' }))[0], R(item('orderChanged', dragon1))[0]], ['mark', 'hold', 'change'], 'a mark, a held notice and a changed order can be released');
    const asks = (kind, p) => item(kind, mkRow('q_1', '4193111742', { problems: [Object.assign({ kind }, p)] }), { problem: Object.assign({ kind }, p) });
    for (const [it, re] of [
      [asks('unmatchedSku', { sku: 'NOPE', reason: 'not in any master file' }), /Still open: SKU NOPE has no design in any master file yet: pick its charm/],
      [asks('needsMaterial', {}), /Still open: its metal is not read yet: pick the metal\./],
      [asks('needsMapping', { optionName: 'Style', optionValue: 'Odd' }), /Still open: the option “Style: Odd” is not answered yet/],
      [asks('needsMapping', { pairSecond: { why: 'x' } }), /Still open: say which design goes on the other ear/],
      [asks('missingSize', { sku: 'DRAGON11', size: 'XL' }), /Still open: DRAGON11 has no size XL: pick one of its sizes/],
      [asks('oversize', {}), /Still open: it does not fit the plate under the ceiling/]
    ]) { const r = R(it); ok(r[0] === 'queue' && r[1] === '' && re.test(r[2]), `${it.kind}: NOT greyed, pressable, its title says what is still open: ${r[2]}`); }
    // a Custom Order keeps its own Complete Order button instead (it never goes through the nesting queue)
    { const r = R(item('customOrder', mkRow('c_1', '4193111743', { problems: [] }))); ok(r[0] === '' && /A custom order is made by hand/.test(r[1]), 'a custom order has no Release Hold: ' + r[1]); }
    // a piece whose cut is in the queue and which waits only on its engraving
    const queued = mkRow('e_1', '4193111744', { problems: [], state: 'pooled', poolIds: ['p1'] });
    ok(R(item('engraveWords', queued, { problem: {} }))[0] === 'queue' && /already in the nesting queue \(or on a sheet\) and is not placed again: this card waits only on its engraving/.test(R(item('engraveWords', queued, { problem: {} }))[2]), 'an engraving card can be pressed too, and its title says the cut piece is queued and not placed again');
    ok(R(item('engraveWords', mkRow('e_2', '4193111745', { problems: [{ kind: 'blockedSku', sku: 'DRAGON11', reason: 'open outline' }] }), { problem: {} }))[0] === 'queue' && /Still open: its first question is not answered yet: SKU DRAGON11 blocked/.test(R(item('engraveWords', mkRow('e_2', '4193111745', { problems: [{ kind: 'blockedSku', sku: 'DRAGON11', reason: 'open outline' }] }), { problem: {} }))[2]), 'an engraving card of a line that is itself held says what it waits on first, and can be pressed');
    eq(R(item('engraveWords', mkRow('e_4', '4193111747', { problems: [], state: 'pulled' }), { problem: {} }))[0], 'queue', 'a card with nothing open still has a Release Hold that puts the order back in line');
    eq(R(item('engraveWords', mkRow('e_3', '4193111746', { problems: [], state: 'pulled', hold: 'held by Paul' }), { problem: {} }))[0], 'hold', 'a person\'s hold on a line that asks nothing else is released as Release hold');
    ok(/no longer in this run/.test(R(item('blockedSku', null))[1]), 'an item whose lines left the run says so');

    // the press: a blocked SKU card over two orders (oldest first), one with its pieces on hold
    rowsAll.length = 0;
    const r1 = mkRow('o1_1', '4193111740'), r2 = mkRow('o2_1', '4191826593', { line: { sku: 'DRAGON11' } }); r2.order.createTs = 0;
    rowsAll.push(r1, r2);
    const card = item('blockedSku', r1, { rows: [r1, r2], key: 'ord:blocked:DRAGON11' }); c.__items = [card];
    const okAll = await c.releaseToQueue(card);
    ok(okAll === true, 'the press ends in success');
    eq(calls.released.map(x => x.slice(0, 2)), [['4191826593', 'Paul'], ['4193111740', 'Paul']], 'both orders are released through OrderHold.release, the oldest first');
    eq(calls.released.map(x => x[2]), [[true], [true]], 'and each was marked with the release BEFORE the engine read it');
    eq(calls.otherMarked, [[false], [true]], 'an order is marked just before its own turn: while the engine is on the first order the second is not marked yet, and it is by the time of its own');
    eq([r1.blockOverride.sku, r1.blockOverride.by, r1.blockOverride.at > 0, r2.blockOverride.sku], ['DRAGON11', 'Paul', true, 'DRAGON11'], 'every line of the card carries { sku, by, at }');
    eq(calls.answers.length, 2, 'the card\'s answer is noted for each order, as it is released (the timeline writes it for each line)'); eq(calls.answers.map(a => [a.type, a.kind, a.who, a.detail.sku, a.detail.blockOverride]), [['decided', 'blockedSku', 'Paul', 'DRAGON11', true], ['decided', 'blockedSku', 'Paul', 'DRAGON11', true]], 'each as "decided" by Paul');
    ok(/^Released to the nesting queue: DRAGON11 is cut although it is blocked \(open outline\)$/.test(calls.answers[0].say), 'in words: ' + calls.answers[0].say);
    ok(calls.saved >= 1 && c.B.run.lines['o1_1'] && c.B.run.lines['o1_1'].blockOverride.sku === 'DRAGON11', 'and the run\'s lines are saved with it');
    ok(calls.toasts.some(t => t[0] === 'ok' && /Order 4193111740 released to the nesting queue: first in line, placed on RG Sheet 2/.test(t[1])), 'the page says where it was placed: ' + JSON.stringify(calls.toasts.map(t => t[1])));
    ok(['4191826593', '4193111740'].every(rid => calls.agent.some(x => new RegExp(rid + ' · DRAGON11: Paul released it to the nesting queue although the master blocks it \\(open outline\\)').test(x))), 'and the agent log keeps who did it, for each order');
    // a second SKU on a line that already carries a release of another keeps both
    const r3 = mkRow('o3_1', '4190000001', { blockOverride: { sku: 'FLORALS2', by: 'Paul', at: 1 }, problems: [{ kind: 'blockedSku', sku: 'DRAGON11', reason: 'open outline' }] }); rowsAll.push(r3);
    await c.releaseToQueue(item('blockedSku', r3));
    eq([r3.blockOverride.sku, r3.blockOverride.skus], ['FLORALS2', ['FLORALS2', 'DRAGON11']], 'a line keeps its earlier release when another SKU is released');
    // a failing engine ends in the plain failure and a bad toast; nothing is lost
    calls.toasts.length = 0; const r4 = mkRow('o4_1', '4190000002'); rowsAll.push(r4);
    c.window.OrderHold.release = async () => ({ ok: false, released: true, error: 'Order 4190000002 is released and at the front of the queue, but it needs a look in Review: x.' });
    const bad = await c.releaseToQueue(item('blockedSku', r4));
    ok(bad === false && calls.toasts.some(t => t[0] === 'bad' && /needs a look in Review: x\. What was done is kept, and Release Hold can be pressed again/.test(t[1])), 'a failure is said, and the button can be pressed again: ' + JSON.stringify(calls.toasts));
    ok(r4.blockOverride && r4.blockOverride.sku === 'DRAGON11', 'and the release stays on the line');
    // no engine (or nothing held): the lines are made up again in line, first
    calls.toasts.length = 0; calls.repooled.length = 0; delete c.window.OrderHold; const r5 = mkRow('o5_1', '4190000003', { state: 'pulled' }); rowsAll.push(r5);
    const was = Date.now(); const fine = await c.releaseToQueue(item('blockedSku', r5));
    ok(fine === true && calls.repooled.includes('o5_1') && r5.frontAt >= was, 'without the engine the line is made up again, at the front: ' + JSON.stringify(calls.toasts));
    // a mark
    calls.resolved.length = 0; calls.toasts.length = 0; calls.released.length = 0; c.window.OrderHold = { release: async rid => { calls.released.push([rid]); return { ok: true, placed: false, sheets: [] }; } };
    const m1 = mkRow('m_1', '4192000001', { problems: [], state: 'pooled', hold: 'held by Paul' }); rowsAll.push(m1);
    await c.releaseToQueue(item('reviewMark', m1, { rid: '4192000001' }));
    eq(calls.resolved, [['4192000001', 'other']], 'a Send to Review mark is resolved ("other": released to the nesting queue)'); eq(calls.released, [['4192000001']], 'and a line of it that was on hold is released'); ok(calls.toasts.some(t => /left Review/.test(t[1])), 'and it says so');
    // a change
    await c.releaseToQueue(item('orderChanged', m1)); eq(calls.accepted, ['ord:orderChanged:m_1'], 'a changed order is accepted');
    // a card with a question still open: the press is not refused, it acts (the line is made up again, first in line) and no card is "answered" by it
    calls.toasts.length = 0; calls.answers.length = 0; calls.repooled.length = 0; const g = await c.releaseToQueue(asks('needsMaterial', {}));
    ok(g === true && calls.repooled.includes('q_1') && calls.answers.length === 0, 'pressing Release Hold on a card with an open question is carried out, not refused: ' + JSON.stringify([g, calls.repooled, calls.toasts]));
    // and when the line is made up again but its question is still open, the page says so plainly instead of refusing beforehand (liftOrder, no engine: the line is made up again)
    calls.toasts.length = 0; calls.repooled.length = 0; delete c.window.OrderHold;
    const open1 = mkRow('q_9', '4193111749', { state: 'pulled', problems: [{ kind: 'needsMaterial' }] }); rowsAll.push(open1); c.repool = async r => { calls.repooled.push(r.key); };
    const g2 = await c.releaseToQueue(item('needsMaterial', open1, { problem: { kind: 'needsMaterial' } }));
    ok(g2 === false && calls.repooled.includes('q_9') && calls.toasts.some(t => t[0] === 'bad' && /back in line, but it still needs a look/.test(t[1])), 'a question still open after the press is said after it, and the line was put back in line first: ' + JSON.stringify(calls.toasts));
  }
  pass('F · the Review button: never greyed for an order in the run (what is still open is in its title and said after the press); the release, its note on the timeline and the order of the engine');

  console.log(`block-release: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
