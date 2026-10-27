// SIBLINGS (Paul, 12 Oct 2026, order 4198409468): the Etsy SKU "SCHRODINGER CAT" names no design a necklace may take (the library holds only a huggie-sized
// "SCHRODINGER CAT" and the numbered "SCHRODINGER CAT 1" and "SCHRODINGER CAT 2"), and the listing wears TWO cats. The Review row offers ONE quiet tap, "Use both: X 1 + X 2",
// that saves the pair for the listing and its SKU for good and makes the line two pieces of one group.
//   node tests/charm-nest/siblings-suggest.cjs
// Offline: no browser, no network, no Etsy call, no paid call, nothing live. Fake data only (the real master NAMES the order touches, no buyer data); the saved-link round trip runs the real
// netlify/functions/charmNestLibrary.js over the in-memory Firestore of tests/charm-nest/bridge-server.cjs.
//  1 · the suggestion (charm-nest-siblings.js): necklaces only, never earrings or a huggie, never an indexed family, the pair is the two lowest numbers, 3 or more list the rest
//  2 · the saved link (charm-nest-orders.js): a necklace line with a saved pair is two pieces of one group, each its own design, no Left or Right, nothing mirrored; an earring line keeps its Left and Right
//  3 · the round trip: aliasPut pair, aliasGet, the next order of that listing is already linked (no question), another SKU or listing is not
//  4 · the pool (the real Pool in a vm): piece 1 is the first design and piece 2 the second, one group, quantity 2 alternates; a second design the master cannot give holds the line
//  5 · the pictures: the line's picture is its two designs side by side, a plain key of its own
//  6 · wiring: the script tag, the build list, the Review row's controls
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const S = require(path.join(root, 'charm-nest-siblings.js'));
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const PP = require(path.join(root, 'charm-nest-pool-pieces.js'));
const F = require('./pairs-fixtures.cjs');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);
const V = (name, value) => ({ name, value });

/* ── the master: the real SKU names of the order (the huggie-sized plain design and the numbered ones), and a few look-alikes ───────────────────────────── */
const NAMES = ['SCHRODINGER CAT', 'SCHRODINGER CAT 1', 'SCHRODINGER CAT 2', 'SCHRODINGER CAT 1 (HUGGIE)', 'SCHRODINGER CAT 2 (HUGGIE)', 'HUGGIE HOOPS- SCHRONDINGER CAT', 'SCHR_1215',
  'DRAGON 1', 'DRAGON 2', 'DRAGON 3', 'DRAGON 4', 'DRAGON 1 (HUGGIE)', 'DRAGON 2 (HUGGIE)', 'LETTER-1', 'LETTER-2', 'LETTER-3', 'MOON 1', 'MOON 1 (HUGGIE)', 'MOON 2 (HUGGIE)', 'STAR 1', 'STAR 2 EARRING', 'STAR 3 EARRING'];
const sib = (sku, n2) => ({ sku, n: n2, form: 'necklace', why: 'numbered version ' + n2 + ' of X' });
const master = (names, notOwn) => {
  const m = new Map(names.map(k => [k, {}])), loose = new Map(); for (const k of m.keys()) { const key = O.looseKey(k); loose.set(key, loose.has(key) ? '' : k); }
  return { masterEntry: s => (notOwn && notOwn.includes(String(s).toUpperCase()) ? null : m.get(String(s || '').toUpperCase()) || null), masterLoose: s => loose.get(O.looseKey(s)) || '' };
};
const ctx = (extra, names, notOwn) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] } }, master(names || NAMES, notOwn), extra);
const order = { receiptId: '4198409468', updateTs: 1 };
const LID = '1190000001';
const necklace = over => Object.assign({ transactionId: '5300000001', listingId: LID, sku: 'SCHRODINGER CAT', title: 'Schrodinger Cat Necklace Dainty Charm Necklace', quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [],
  variations: [V('Metal Choice', 'Silver'), V('Necklace Length in inches', '18"'), V('Personalization', 'I did not select + engraving')] }, over || {});
const earrings = over => necklace(Object.assign({ title: 'Schrodinger Cat Stud Earrings', variations: [V('Style', 'Stud earrings'), V('Metal Choice', 'Silver')] }, over || {}));
const SKUS = NAMES.filter(k => k !== 'SCHRODINGER CAT');   // (the plain design is the huggie-sized one the guard refuses on a necklace)

/* ── 1 · the suggestion ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{
  const r = S.suggest(necklace(), { skus: SKUS });
  ok(r, 'the necklace line gets the offer');
  eq(r.pair, { L: 'SCHRODINGER CAT 1', R: 'SCHRODINGER CAT 2' }, 'the pair is the two numbered designs');
  eq(r.siblings.map(x => x.sku), ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2'], 'and they are the single choices too, in number order');
  eq(r.rest, [], 'nothing else to list');
  eq(r.ask, 'SCHRODINGER CAT has a 1 and a 2 in the library. Is this necklace both charms?', 'plain words, the ones Paul asked for');
  ok(!r.siblings.some(x => /HUGGIE/.test(x.sku)) && !JSON.stringify(r).includes('HUGGIE'), 'a huggie is never offered to a necklace');
  // the real master's own list (all 6,856 SKU names, when the shared copy is on this machine)
  const copy = '/mnt/project-files/plans/flag-fix-1011/master-skus-1011.json';
  if (fs.existsSync(copy)) {
    const all = JSON.parse(fs.readFileSync(copy, 'utf8')).map(e => e.sku), real = S.suggest(necklace(), { skus: all });
    eq(real.pair, { L: 'SCHRODINGER CAT 1', R: 'SCHRODINGER CAT 2' }, 'the real master: the same pair'); ok(real.siblings.every(x => !/HUGGIE|EARRING|STUD/.test(x.sku)), 'and no huggie among the choices');
  }
  { const ix = S.index(SKUS); eq(S.suggest(necklace(), { index: ix }), r, 'the master listed once (index) gives the very same offer as the plain scan'); eq(S.suggest(necklace({ sku: 'DRAGON' }), { index: ix }), S.suggest(necklace({ sku: 'DRAGON' }), { skus: SKUS }), 'and for a family of four'); ok(!ix.has('SCHRODINGERCATHUGGIE'), 'a (HUGGIE) design is not in it'); }
  pass('SCHRODINGER CAT on a necklace: ONE offer, both numbered designs, in plain words, never the huggie');
}
{ // the guard's own list is read (spec.siblings / problem.siblings) and is not worked out again
  const given = [sib('SCHRODINGER CAT 1', 1), sib('SCHRODINGER CAT 2', 2)];
  eq(S.suggest(necklace(), { siblings: given, skus: [] }).pair, { L: 'SCHRODINGER CAT 1', R: 'SCHRODINGER CAT 2' }, 'the guard\'s list is used as given (no master list needed)');
  eq(S.suggest(necklace(), { spec: { siblings: given } }).pair.R, 'SCHRODINGER CAT 2', 'spec.siblings too');
  eq(S.suggest(necklace(), { siblings: [], skus: SKUS }), null, 'an EMPTY list from the guard is an answer: nothing is offered (no second guess from the master)');
  eq(S.suggest(necklace(), { siblings: given, spec: { lineForm: 'huggie' } }), null, 'the guard says the line is huggie: no offer');
  eq(S.suggest(necklace(), { siblings: given, spec: { lineForm: 'earring' } }), null, 'the guard says the line is an earring: no offer');
  ok(S.suggest(earrings(), { siblings: given, spec: { lineForm: 'necklace' } }), 'the guard says necklace: it is one, whatever the words');
  pass('the form guard\'s candidate list and form are read, never recomputed');
}
{ // negatives
  eq(S.suggest(earrings(), { skus: SKUS }), null, 'an earring listing (siblings are alternatives): no both chip');
  eq(S.suggest(earrings({ title: 'Schrodinger Cat Huggie Hoop Earrings', variations: [V('Hoop Size', '8.5mm'), V('Metal Choice', 'Silver')] }), { skus: SKUS }), null, 'huggie hoops: none');
  eq(S.suggest(earrings({ title: 'Dual Charm Stud Earrings, two charms' }), { skus: SKUS }), null, 'even an earring title that says two charms: a pair of earrings has its own Left and Right answers');
  eq(S.suggest(necklace({ title: 'Schrodinger Cat Bracelet', variations: [V('Metal Choice', 'Silver')] }), { skus: SKUS }), null, 'a bracelet that does not say two charms: none');
  eq(S.suggest(necklace({ title: 'Schrodinger Cat Bracelet, Dual Charm Bracelet', variations: [V('Metal Choice', 'Silver')] }), { skus: SKUS }), null, 'a bracelet is not a necklace, even one that says dual charm: none (necklace-form lines only)');
  eq(S.suggest(necklace({ title: 'Schrodinger Cat Charm', variations: [V('Metal Choice', 'Silver'), V('Charm Type', 'Charm only')] }), { skus: SKUS }), null, 'a loose charm that does not say two: none');
  eq(S.suggest(necklace({ title: 'Schrodinger Cat Charm Set of 2 charms', variations: [V('Metal Choice', 'Silver'), V('Charm Type', 'Charm only')] }), { skus: SKUS }), null, 'a loose charm, even one that says 2 charms: none (necklace-form lines only)');
  eq(S.suggest(necklace({ title: 'Cat Jewelry', variations: [V('Metal Choice', 'Silver')] }), { skus: SKUS }), null, 'a line that is not known to be a necklace: none');
  eq(S.suggest(necklace({ sku: 'MOON' }), { skus: SKUS }), null, 'only ONE numbered design (the other numbers are huggie): not a pair');
  eq(S.suggest(necklace({ sku: 'LETTER' }), { skus: SKUS }), null, 'LETTER-1, LETTER-2, LETTER-3 are an indexed family (the option picks the member), never a pair');
  eq(S.suggest(necklace({ sku: 'STAR' }), { skus: SKUS }), null, 'designs of another family (an earring in the name) are left out: one left, no pair');
  eq(S.suggest(necklace({ sku: '' }), { skus: SKUS }), null, 'no SKU, no offer');
  eq(S.suggest(necklace(), { skus: SKUS, usable: k => k === 'SCHRODINGER CAT' }), null, 'a SKU whose own design the page accepts is not second-guessed');
  eq(S.suggest(necklace(), { skus: SKUS, blocked: k => k === 'SCHRODINGER CAT 2' }), null, 'a blocked design is never offered (one left: no pair)');
  eq(S.suggest(necklace({ sku: 'Schrodinger  cat' }), { skus: SKUS }).pair, { L: 'SCHRODINGER CAT 1', R: 'SCHRODINGER CAT 2' }, 'case and spacing do not matter');
  eq(S.suggest(necklace({ sku: 'SCHRODINGER&#39;S CAT' }), { skus: SKUS }), null, 'another name is another name (no fuzzy match)');
  // the audit's nearest real case (SCHROAUDIT, order 4171756052): SKU "GRAD CAP 1", the master has only "GRAD CAP 1 (HUGGIE)" and "GRAD CAP 2": a correct hold, nothing is offered
  eq(S.suggest(necklace({ sku: 'GRAD CAP 1' }), { skus: ['GRAD CAP 1 (HUGGIE)', 'GRAD CAP 2'] }), null, 'GRAD CAP 1 (huggie only) beside GRAD CAP 2: no offer, the hold stands');
  eq(S.suggest(necklace({ sku: 'GRAD CAP 1' }), { siblings: [], skus: ['GRAD CAP 1 (HUGGIE)', 'GRAD CAP 2'] }), null, 'and the guard\'s empty list for it is the same answer');
  // never a huggie beside a plain one, in the guard's list or in a plain scan
  eq(S.suggest(necklace({ sku: 'X' }), { skus: ['X 1', 'X 2 (HUGGIE)'] }), null, 'a plain 1 and a huggie 2 are not a pair of necklace charms');
  eq(S.suggest(necklace({ sku: 'X' }), { siblings: [{ sku: 'X 1', n: 1, form: 'necklace' }, { sku: 'X 2', n: 2, form: 'huggie' }] }), null, 'the guard\'s list with a design it calls huggie: that design is left out, one is not a pair');
  eq(S.suggest(necklace({ sku: 'X' }), { siblings: [{ sku: 'X 1', n: 1, form: 'necklace' }, { sku: 'X 2 (HUGGIE)', n: 2, form: 'unknown' }, { sku: 'X 3', n: 3, form: 'unknown' }] }).pair, { L: 'X 1', R: 'X 3' }, 'a "(HUGGIE)" name is left out whatever form it is marked; the plain ones pair (1 and 3)');
  // the base plus 1 and 2 (KILLER WHALE, PALM TREE, SCHRODINGER CAT): the plain base is not a numbered design; the offer is about 1 and 2 only
  eq(S.suggest(necklace({ sku: 'PALM TREE' }), { skus: ['PALM TREE', 'PALM TREE 1', 'PALM TREE 2'] }).pair, { L: 'PALM TREE 1', R: 'PALM TREE 2' }, 'a base with a 1 and a 2: the two numbered designs, never the base');
  pass('no offer on earrings or huggie hoops, a bracelet or loose charm, indexed families, one design, blocked designs');
}
{ // sets of three or more: the pair and the rest listed, never "all N"
  const r = S.suggest(necklace({ sku: 'DRAGON' }), { skus: SKUS });
  eq(r.pair, { L: 'DRAGON 1', R: 'DRAGON 2' }, '3 or more: the pair is the two lowest numbers'); eq(r.rest.map(x => x.sku), ['DRAGON 3', 'DRAGON 4'], 'the rest are single choices');
  eq(r.siblings.map(x => x.sku), ['DRAGON 1', 'DRAGON 2', 'DRAGON 3', 'DRAGON 4'], 'every numbered design is one'); ok(!JSON.stringify(r).includes('HUGGIE'), 'no huggie');
  ok(/numbers 1, 2, 3 and 4 in the library\. Is this necklace two of them \(1 and 2\)\?/.test(r.ask), 'plain words: ' + r.ask); ok(!/all/i.test(r.ask), 'never "all"');
  pass('three or more numbered designs: the pair is offered and the rest are listed, never "all N"');
}

/* ── 2 · the saved link on the line ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const LINK = { [LID]: { pairBySku: { 'SCHRODINGER CAT': { L: 'SCHRODINGER CAT 1', R: 'SCHRODINGER CAT 2', by: 'Test', at: 1 } } } };
const read = (line, c) => O.interpretLine(order, line, c || ctx({ aliases: LINK }, null, ['SCHRODINGER CAT']));
{
  const before = read(necklace(), ctx({}, null, ['SCHRODINGER CAT']));
  ok(before.problems.some(p => p.kind === 'unmatchedSku') && !before.pair.set, 'with no saved link the line waits (unmatched) and is no set');
  const sp = read(necklace());
  eq(sp.problems.filter(p => p.kind !== 'needsMaterial'), [], 'with the saved link nothing is asked'); eq(sp.designSku, 'SCHRODINGER CAT 1', 'pooled from the first design');
  eq(sp.pair.set, { designs: ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2'], source: 'alias' }, 'the set is the two designs the person saved');
  eq([sp.pieceCount, sp.pair.perUnit, sp.pair.kind, sp.pair.earring, sp.pair.mismatched, sp.pair.glued], [2, 2, 'multi', false, false, false], 'two pieces, one group, kind multi, not an earring pair, not mismatched');
  eq(sp.pair.sides, [null, null], 'no side: a necklace is never a Left and a Right'); eq(O.piecesOf({ spec: sp, line: necklace() }).map(p => (p.side || '-') + p.bodyIndex), ['-0', '-0'], 'piecesOf: no side, whole designs');
  ok(sp.pair.notes.some(x => /two designs a person saved.*SCHRODINGER CAT 1 and SCHRODINGER CAT 2.*no Left or Right, nothing mirrored/.test(x)), 'a plain note says so');
  eq(O.countedPieces({ spec: sp, line: necklace() }), { total: 2, qty: 1, noun: 'piece' }, 'every place that says how many pieces says 2');
  const q2 = read(necklace({ quantity: 2 })); eq([q2.pieceCount, q2.pair.sides], [4, [null, null, null, null]], 'quantity 2: 4 pieces (the order\'s quantity rule is unchanged: units times pieces)');
  ok(Pair.isGroupLine({ spec: sp, quantity: 1, form: sp.form }, null), 'it is a group line (travels together)');
  eq(Pair.piecesFor(Object.assign({ receiptId: order.receiptId }, necklace(), { quantity: 1, spec: sp }), null).map(p => [p.side, p.mirror, p.of]), [[null, false, 2], [null, false, 2]], 'charm-nest-pair.js agrees: no side, no mirror, 2 pieces');
  // an earring line keeps what it always made
  const ear = read(earrings());
  eq([ear.pair.earring, ear.pair.mismatched, ear.pair.source, ear.pair.set || null, ear.pair.sides.join('')], [true, true, 'alias', null, 'LR'], 'an earring line with the same saved link is still a Left and a Right (unchanged)');
  // a link naming a design the master does not hold does nothing new
  const gone = read(necklace(), ctx({ aliases: LINK }, NAMES.filter(k => k !== 'SCHRODINGER CAT 2'), ['SCHRODINGER CAT']));
  ok(!gone.pair.set && gone.problems.some(p => p.kind === 'unmatchedSku'), 'a saved design that left the master: no set, the line asks again');
  // words alone never make a set: a necklace SKU or title that names two designs is still one piece of the first
  const words = O.interpretLine(order, necklace({ sku: 'SCHRODINGER CAT 1/SCHRODINGER CAT 2' }), ctx());
  ok(!words.pair.set && words.pieceCount === 1, 'words alone never make a set: one piece');
  // another SKU or another listing is not linked
  const other = read(necklace({ sku: 'DRAGON' }), ctx({ aliases: LINK }, null, ['SCHRODINGER CAT'])); ok(!other.pair.set, 'another SKU of the same listing: not linked');
  const other2 = read(necklace({ listingId: '1190000002' })); ok(!other2.pair.set, 'another listing with the same SKU: not linked');
  // a single earring or a line sold as Single is not a set
  const single = read(necklace({ variations: [V('Style', 'Single earring'), V('Metal Choice', 'Silver')], title: 'Schrodinger Cat Earring' })); ok(!single.pair.set, 'a single earring is not a set');
  pass('a saved pair on a necklace: two pieces of one group, no Left or Right; earrings, words and other SKUs unchanged');
}

/* ── 3 · the round trip: aliasPut pair, aliasGet, the next order is already linked ──────────────────────────────────────────────────────────────── */
(async () => {
  const { start } = require('./bridge-server.cjs'), srv = await start({ receipts: [] });
  try {
    const call = async body => { const out = await srv.st.handlers.charmNestLibrary.handler({ httpMethod: 'POST', headers: {}, body: JSON.stringify(body) }); return Object.assign({ statusCode: out.statusCode }, JSON.parse(out.body || '{}')); };
    for (const k of ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2', 'DRAGON 1']) srv.st.put('Charm_Master_Index', k, { sku: k, widthPt: 20, heightPt: 20, members: 3 });
    const put = await call({ op: 'aliasPut', listingId: LID, fromSku: 'SCHRODINGER CAT', pair: { L: 'SCHRODINGER CAT 1', R: 'SCHRODINGER CAT 2' }, by: 'Test Operator', title: 'Schrodinger Cat Necklace' });
    ok(put.ok === true && put.statusCode === 200, 'aliasPut pair is saved: ' + JSON.stringify(put));
    const got = await call({ op: 'aliasGet' }), rec = got.aliases && got.aliases[LID];
    ok(rec && rec.pairBySku && rec.pairBySku['SCHRODINGER CAT'] && rec.pairBySku['SCHRODINGER CAT'].L === 'SCHRODINGER CAT 1' && rec.pairBySku['SCHRODINGER CAT'].R === 'SCHRODINGER CAT 2', 'aliasGet returns it under the listing and the SKU');
    ok(!('sku' in rec) && !rec.bySku, 'it is the pair link only: no single-charm alias is written beside it');
    // the NEXT order of that listing (another receipt, another transaction) reads it and asks nothing
    const next = O.interpretLine({ receiptId: '4199999999', updateTs: 2 }, necklace({ transactionId: '5399999999' }), ctx({ aliases: got.aliases }, null, ['SCHRODINGER CAT']));
    eq(next.problems.filter(p => p.kind !== 'needsMaterial'), [], 'the next order of the listing is already linked, with no question'); eq([next.designSku, next.pair.set.designs, next.pieceCount], ['SCHRODINGER CAT 1', ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2'], 2], 'two pieces, both designs');
    // another SKU on the listing, another listing: not linked
    ok(!O.interpretLine(order, necklace({ sku: 'DRAGON' }), ctx({ aliases: got.aliases }, null, ['SCHRODINGER CAT'])).pair.set, 'another SKU of the listing is not linked');
    // a pair names two different master designs, both in the index: the server refuses anything else and writes nothing
    const same = await call({ op: 'aliasPut', listingId: '1190000009', fromSku: 'X', pair: { L: 'DRAGON 1', R: 'DRAGON 1' }, by: 'T' }); ok(same.statusCode === 400 && /two different master SKUs/.test(same.error), 'the same design twice is refused');
    const missing = await call({ op: 'aliasPut', listingId: '1190000009', fromSku: 'X', pair: { L: 'DRAGON 1', R: 'NOT IN MASTER' }, by: 'T' }); ok(missing.statusCode === 400 && /not in the master index/.test(missing.error), 'a design that is not in the index is refused');
    const nofrom = await call({ op: 'aliasPut', listingId: '1190000009', pair: { L: 'DRAGON 1', R: 'SCHRODINGER CAT 1' }, by: 'T' }); ok(nofrom.statusCode === 400, 'no SKU, no pair');
    ok(!(await call({ op: 'aliasGet' })).aliases['1190000009'], 'a refused answer wrote nothing');
    pass('the saved pair round trip: kept per listing and SKU for good, read back by the next order, refused when it names a design the index lacks');
  } finally { srv.close(); }

  /* ── 4 · the pool: each piece is its own design ───────────────────────────────────────────────────────────────────────────────────────────────── */
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const a = src.indexOf('const Pool = window.Pool = (() => {'), b = src.indexOf('\n/* Carry-forward is keyed', a);
  assert(a > 0 && b > a, 'the Pool module is where the test expects it');
  const MM = 25.4 / 72;
  function world(designs, extra) {
    const logs = [], calls = [], pages = { silver: { metal: 'silver', charms: [], placements: [], status: 'idle', sheetId: null } }, entries = {};
    for (const sku of designs) entries[sku] = Object.assign(F.entryOf(sku), { sku, aiPath: 'charmnest/master/sku/' + sku + '.ai', aiUrl: 'https://x/' + sku, masterHash: 'mh-' + sku, upAngle: 0 });
    const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, '0'); };
    const c = Object.assign({
      console, Date, JSON, Map, Set, Math, Number, String, Array, Object, Promise, Error, Uint8Array, setTimeout, clearTimeout, performance: { now: () => Date.now() }, structuredClone,
      window: { CharmNestPair: Pair, CharmNestPoolPieces: PP }, O, MM, labelOf: m => m, stockFor: () => ({ wPt: 1400, hPt: 700 }),
      S: { cloud: { ok: true }, settings: { insetPt: 0, maxFill: .8, silhouetteRes: 6, minPt: 6 }, poolSources: {}, sheets: { silver: { active: 0 } } },
      B: { pool: { rows: new Map(), sources: new Map() }, orders: { byKey: new Map(), rows: [] }, run: null },
      agent: (...x) => { logs.push(x.slice(1).join(' ')); }, api: async (fn, body) => { calls.push({ fn, body: JSON.parse(JSON.stringify(body)) }); refuseNestedArrays(body, 'poolPut'); return {}; },
      Master: { entryFor: sku => entries[sku] || null, fetchEntry: async () => null, keepOutOf: () => [], skuRegex: () => /^$/ },
      CharmNestAssets: { bytes: async () => new Uint8Array(1) },
      P: {
        parseSource: async (_bytes, name) => ({ name }), groupCharmsAsync: async parsed => ({ charms: [structuredClone(F.charmOf(String(parsed.name).replace(/\.ai$/, '')))], orphans: [] }), integrateRings: () => ({ left: [], welded: 0 }), parseSkuLabel: () => null,
        buildSilhouettes: async (_p, charms) => { for (const ch of charms) { const w = ch.bbox[2] - ch.bbox[0], h = ch.bbox[3] - ch.bbox[1]; Object.assign(ch, { bits: new Uint8Array(4), w: 2, h: 2, scale: 6, areaPt2: Math.round(w * h * .8), widthPt: w, heightPt: h, centerPt: [(ch.bbox[0] + ch.bbox[2]) / 2, (ch.bbox[1] + ch.bbox[3]) / 2], thumb: 't', hash: fnv(JSON.stringify(ch.bbox) + ch.members.length + (ch.outline.id || '') + (ch.outline.mirrored ? 'm' : '')) }); } return charms; }
      },
      allSheets: () => Object.values(pages), pagesOf: m => [pages[m]], sheetDirty: () => {}, renderCard: () => {}, Orders: { rows: () => [], interpretAll() {}, render() {} },
      CN: {}, Gate: {}, Review: {}, RunCtl: {}, Cleanups: null, CustomPrint: null, CustomSheet: null, LiveNest: null, addPage: () => { throw new Error('no new page expected'); }
    }, extra || {});
    c.window.Cleanups = null; vm.createContext(c); vm.runInContext(src.slice(a, b), c);
    return { c, logs, calls, pages, Pool: c.window.Pool, entries };
  }
  // the line as the intake reads it (the real interpretLine over the real saved link), the design files the fixtures draw
  const D1 = 'ONE-PENDANT', D2 = 'PAIR-STUD';
  const fixtureLine = (qty, over) => {
    const al = { [LID]: { pairBySku: { 'SCHRODINGER CAT': { L: D1, R: D2 } } } }, nm = [D1, D2, 'SCHRODINGER CAT'];
    const line = necklace({ quantity: qty, transactionId: '5300000777' });
    const sp = O.interpretLine({ receiptId: '4190000401', createTs: 1791500000, updateTs: 1791500001 }, line, ctx({ aliases: al }, nm, ['SCHRODINGER CAT']));
    if (over) over(sp);
    return { key: '4190000401_5300000777', state: 'pulled', order: { receiptId: '4190000401', createTs: 1791500000, updateTs: 1791500001 }, line, spec: Object.assign(sp, { material: 'silver', size: null }), problems: [], poolIds: [], engrave: null, arrivedAt: 0 };
  };
  for (const qty of [1, 2]) {
    const w = world([D1, D2]), row = fixtureLine(qty); ok(row.spec.pair.set && row.spec.designSku === D1, 'the fixture line is a set of ' + D1 + ' + ' + D2);
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'); assert(put, 'recorded'); const pools = put.body.pools, nn = 2 * qty, want = Array.from({ length: nn }, (_, i) => (i % 2 ? D2 : D1));
    eq(pools.map(p => p.sku), want, `quantity ${qty}: piece 1 is the first design, piece 2 the second, alternating`);
    ok(pools.every(p => p.side == null && p.mirror == null), 'no side and nothing mirrored on any piece'); ok(pools.every(p => p.groupKey === '4190000401:5300000777' && p.groupSize === nn), 'one group, all of its pieces');
    eq(pools.map(p => p.aiPath), want.map(k => 'charmnest/master/sku/' + k + '.ai'), 'each row points at its own design file'); eq(pools.map(p => p.masterHash), want.map(k => 'mh-' + k), 'and its own master hash');
    eq(pools.map(p => p.copy), Array.from({ length: nn }, (_, i) => i + 1), 'copies 1 to ' + nn); ok(pools.every(p => p.quantity === nn), 'quantity says the piece count');
    const charms = w.pages.silver.charms; eq(charms.length, nn, 'the sheet gets every piece');
    charms.forEach((ch, i) => { eq([ch.orderInfo.sku, ch.side == null, !!ch.mirror, ch.poolId], [want[i], true, false, pools[i].poolId], `piece ${i + 1}: its own design, no side, not mirrored`); ok(ch.name === `4190000401 · ${want[i]} · ${i + 1}/${nn}`, 'named by its own design: ' + ch.name);
      const own = F.charmOf(want[i]); ok(ch.members.length === own.members.length, 'cut from its own design only'); });
    ok(new Set(charms.map(c => c.hash)).size === 2, 'two designs, two charm hashes'); ok(row.state === 'pooled' && row.poolIds.length === nn, 'pooled'); ok(!w.logs.some(l => /glued/.test(l)), 'never the one glued piece');
  }
  pass('the pool cuts piece 1 from the first design and piece 2 from the second (quantity 2: A B A B), one group, no side, no mirror');
  { // a second design the master cannot give: held, never made as the first alone
    const w = world([D1]), row = fixtureLine(1);
    await w.Pool.poolAdd(row, null);
    ok(!w.calls.some(x => x.body && x.body.op === 'poolPut') && w.pages.silver.charms.length === 0, 'nothing is written or put on a sheet'); ok(row.state === 'unmatched' && row.problems.some(p => p.kind === 'unmatchedSku' && p.sku === D2), 'it asks about the missing design: ' + row.state);
  }
  { // an old line pinned to the one piece it was pooled with, that piece taken off (no pool row left): made up again at the set (KITEPAIR, Paul 13 Oct 2026: "a line taken off is made up new
    // at the rule's count"), the first design and then the second; never two pieces of the first design (27 Oct 2026: the set was read while the line was still pinned)
    const w = world([D1, D2]), row = fixtureLine(1); O.pinPieces(row.spec, 1); ok(row.spec.pair.legacy, 'pinned to the one piece it had');
    await w.Pool.poolAdd(row, null); const put = w.calls.find(x => x.body.op === 'poolPut'); eq(put.body.pools.map(p => p.sku), [D1, D2], 'its piece taken off, the line is made up again as the set: the first design, then the second');
    ok(!row.spec.pair.legacy, 'and the pin is taken back');
  }
  { // a line that is not a set is unchanged: a plain necklace of one design, and an earring pair of two saved designs
    const w = world([D1]), line = necklace({ transactionId: '5300000778' }), sp = O.interpretLine(order, Object.assign({}, line, { sku: D1 }), ctx({}, [D1]));
    const row = { key: '4190000401_5300000778', state: 'pulled', order: { receiptId: '4190000401', createTs: 1791500000, updateTs: 1791500001 }, line: Object.assign({}, line, { sku: D1 }), spec: Object.assign(sp, { material: 'silver', size: null }), problems: [], poolIds: [], engrave: null, arrivedAt: 0 };
    await w.Pool.poolAdd(row, null); eq(w.calls.find(x => x.body.op === 'poolPut').body.pools.map(p => [p.sku, p.groupSize == null]), [[D1, true]], 'a plain necklace of one design: one piece, no group');
  }
  pass('a missing second design holds the line; an old line and a plain necklace are unchanged');

  /* ── 5 · the pictures ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  {
    const lm = src, x = lm.indexOf('  const oneRow = row => { try {'), y = lm.indexOf('  /** A line\'s vector design drawn large on white'), z = lm.indexOf('  /** What makes two lines\' vector designs one picture'), z2 = lm.indexOf('  /** A line\'s vector design into one box');
    assert(x > 0 && y > x && z > y && z2 > z, 'the picture code is where the test expects it');
    const w = world([D1, D2]), sets = [], one = [];
    const c2 = vm.createContext({ console, Object, String, Array, Promise, Map, Set, JSON, Number, window: { CharmNestPair: Pair }, P: {}, Engrave: {}, catalog: new Map(), Pool: { charmOf: () => null, masterSet: (es, size, px) => { sets.push(es.map(e => e.sku)); return 'set-image'; }, masterTwo: () => 'two-image', masterFront: (e) => { one.push(e.sku); return 'one-image'; }, masterPreview: (e) => { one.push(e.sku); return 'one-image'; }, sizeEntry: () => null } });
    c2.Master = { entryFor: sku => w.entries[sku] || null, fetchEntry: async () => null };
    const dv = lm.indexOf('  const drawnPiece=row=>'); assert(dv > y && dv < z, 'the picture key\'s version readers (WRONGCHARM2: pieceSig, designVer) sit before vectorKey');
    vm.runInContext(lm.slice(x, y) + '\n' + lm.slice(dv, z2) + '\n;this.__v = { vector, vectorKey, setOf };', c2);
    const V2 = c2.__v, row = fixtureLine(1);
    eq(V2.setOf(row), [D1, D2], 'setOf: the two designs'); const img = await V2.vector(row, 120); eq([img, sets], ['set-image', [[D1, D2]]], 'the line\'s picture is its two designs side by side');
    ok(/\|set:ONE-PENDANT\+PAIR-STUD$/.test(V2.vectorKey(row)), 'its key names both designs: ' + V2.vectorKey(row)); ok(V2.vectorKey(row, { piece: true }) === 'sku:ONE-PENDANT||v:@', 'one piece asked for on its own is its design as before (WRONGCHARM2: with the library entry version, none here): ' + V2.vectorKey(row, { piece: true }));
    one.length = 0; sets.length = 0; await V2.vector(row, 120, { piece: true }); ok(!sets.length, 'a piece asked for alone is never the set picture');
    const plain = fixtureLine(1, sp => { delete sp.pair.set; }); eq(V2.setOf(plain), [], 'a line that is no set has no set picture'); ok(!/\|set:/.test(V2.vectorKey(plain)), 'and no set key');
    const gone = fixtureLine(1, sp => { sp.pair.legacy = true; }); eq(V2.setOf(gone), [], 'an old line (legacy) keeps its one picture');
    const missing = world([D1]); c2.Master = { entryFor: sku => missing.entries[sku] || null, fetchEntry: async () => null }; sets.length = 0; one.length = 0; await V2.vector(row, 120); eq([sets, one], [[], [D1]], 'a design the page cannot read: the one picture the line always had');
    pass('the line\'s picture is its two designs side by side, with a key of its own; a piece, an old line and a missing design fall back to the one picture');
  }

  /* ── 6 · wiring ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  {
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), tags = [...html.matchAll(/<script src="([^"?]+)(\?v=[^"]*)?"/g)], at = f => tags.findIndex(m => m[1] === f);
    ok(at('charm-nest-siblings.js') > 0 && tags.filter(m => m[1] === 'charm-nest-siblings.js').length === 1, 'the page loads the sibling module once'); ok(at('charm-nest-siblings.js') < at('charm-nest-bridge.js'), 'before the bridge');
    ok(/^\?v=2026\d{4}-[a-z0-9-]+$/.test(tags[at('charm-nest-siblings.js')][2] || ''), 'with a ?v= token'); ok(/-sg1$/.test(tags[at('charm-nest-bridge.js')][2]) || /-sg1-/.test(tags[at('charm-nest-bridge.js')][2]), 'the bridge token keeps its suffixes and carries sg1');
    ok(/-sg1/.test(tags[at('charm-nest-orders.js')][2]), 'the orders token carries sg1');
    ok(/"charm-nest-siblings\.js"/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')), 'scripts/build-public.cjs serves it');
    const card = src.slice(src.indexOf('} else if (it.kind === "unmatchedSku" || it.kind === "blockedSku") {'), src.indexOf('} else if (it.kind === "missingSize") {'));
    const rr = src.slice(src.indexOf('  function reviewRow(it) {'), src.indexOf('  /** A card\'s custom buttons (cx: what they act on')), helpers = src.slice(src.indexOf('  const answerKeysOf = (it, rows) =>'), src.indexOf('  /** One list of the master\'s SKUs for every field that picks a charm'));
    ok(/function saveBoth\(it, p, sib\)/.test(helpers) && /op: "aliasPut", listingId: q\.lid, fromSku: q\.from, pair: \{ L, R \}/.test(helpers) && /pairs\.some\(q => !q\.from \|\| !\/\^\\d\+\$\/\.test\(q\.lid\)\)/.test(helpers), 'one shared save: aliasPut with pair for each listing and SKU, refused without a listing number or SKU');
    ok(/data-both="1"/.test(helpers) && /Use both: /.test(helpers) && /dsPick/.test(helpers) && /Master\.pictureTag/.test(helpers) && /data-dsku=/.test(helpers), 'ONE "Use both" button with the two thumbnails, then the single choices, the SKU picker\'s own buttons and picture tag');
    // the Review ROW, which is what the Review tab draws (the decision card is no longer mounted: 28-29 Sep), and the card the tests and older callers still use
    ok(/\(it\.kind==='unmatchedSku'\|\|it\.kind==='blockedSku'\)\?similarOffer\(row,pk,group,it\)/.test(rr) && /class="sibOffer"/.test(rr) && /sibButtons\(sib\)/.test(rr) && /wireSib\(node,it,pk,sib/.test(rr) && /Master\.mountPictures\(node\)/.test(rr), 'the Review row (reviewRow) shows the question and the choices, wired, with their pictures');
    ok(/!cu&&!it\.done&&!it\.decided&&row&&\(it\.kind==='unmatchedSku'/.test(rr), 'the offer is for an open unmatched-SKU row only (never a custom, completed or sent one; REVSUGGEST: or a blocked SKU)');
    ok(/answerKeysOf\(it, sib\.rows\)/.test(helpers) && /answered\(\{ kind: it\.kind, rows: sib\.rows \}/.test(helpers) && /pickSimilar\(it, p, b\.dataset\.dsku, sib && \(b\.dataset\.sim && sib\.rowsFor \? sib\.rowsFor\(b\.dataset\.dsku\) : sib\.rows\)\)/.test(helpers) && /x === row \|\|/.test(src.slice(src.indexOf('function siblingOffer'), src.indexOf('const answerKeysOf'))), 'the tap covers only the lines of the card that are offered the same pair (a huggie or earring listing with the same SKU is never linked)');
    ok(/siblingStamp\(it\),cancelRidOf\(it\)\]\);/.test(src), 'the row is drawn again when the offer or its pictures change (stampOf)');
    ok(/similarOffer\(r, p, group, it\)/.test(card) && /sibButtons\(sib\)/.test(card) && /wireSib\(c, it, p, sib/.test(card) && /it\.kind === "unmatchedSku" \|\| it\.kind === "blockedSku" \? similarOffer/.test(card), 'the decision card shares the same offer and save');
    // (NOHOLDREV, Paul 17 Oct 2026: no Hold on a Review row or card)
    ok(/data-a="alias"[^`]*Use this charm/.test(card) && /data-a="nodesign"/.test(card) && !/data-a="hold"/.test(card) && !/Hold order/.test(card), 'the card\'s own buttons are all still there, and none is Hold order');
    ok(/CustomPrint\.keptButtonHtml\(x,'print'/.test(rr) && /CustomPrint\.releaseButtonHtml\(x,'sm'\)/.test(rr) && /CustomSheet\.buttonsHtml\(x,true\)/.test(rr) && !/HoldUI|holdSlotFor|holdStamp/.test(rr), 'the row\'s own buttons (Print QR label, Release Hold where Complete Order stood: RELORDER, Paul 20 Oct 2026; Send to Sheet) are drawn as before, with no Hold');
    const fx0 = helpers.indexOf('  /** "Wrong charm?" under the Vector design'), fx1 = fx0 < 0 ? -1 : helpers.indexOf('\n  }\n', helpers.indexOf('function fixMount', fx0));   // (WRONGCHARM: an Orders card's own door into the order window, not the offer's; it lies between the helpers read here)
    const helpersOwn = fx0 < 0 ? helpers : helpers.slice(0, fx0) + helpers.slice(fx1 + 4);
    ok(!/prompt\(|confirm\(|openModal|modal|showModal|\.open\(/.test(helpersOwn + rr.slice(rr.indexOf('const pk=it.problem'), rr.indexOf('const summary='))), 'no pop-up on a pop-up');
    pass('wiring: script tag, tokens, build list, the Review row\'s chip and buttons');
  }
  console.log(`siblings-suggest: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
