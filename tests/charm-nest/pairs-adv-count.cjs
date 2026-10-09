// ADVCOUNT (pairs-1009, phase 2): adversarial review of how many pieces an Etsy order line makes. Offline: no browser, no network, nothing live.
//   node tests/charm-nest/pairs-adv-count.cjs               strict checks must pass; OPEN lines are findings that belong to another worker's file (printed, not failing)
//   ADV_STRICT=1 node tests/charm-nest/pairs-adv-count.cjs   every OPEN line is a hard failure (what the owner runs to see it fixed)
// Sections: 0 the real Sep 17 sandbox snapshot lines (fixtures/adv-count-lines.json: the 106 lines whose count changed, lines with a separator in the SKU, specials,
//           singles) against the golden counts; 1-7 the open findings of ADVCOUNT-findings.md (charm-nest-orders.js, PAIRINTAKE); 8 the station pages (never Left/Right on a
//           line that is not an earring pair); 9 the sorter's live card lists every piece of a multi-piece line.
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const STRICT = process.env.ADV_STRICT === '1';
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const open = [];
function known(id, label, fn) {                                  // a finding in a file this test's author may not edit: reported, and hard in ADV_STRICT mode
  try { fn(); n++; console.log('  fixed  ' + id + ' ' + label); } catch (e) { if (STRICT) throw new Error(id + ' ' + label + ': ' + e.message); open.push(id); console.log('  OPEN   ' + id + ' ' + label + ' :: ' + String(e.message).split('\n')[0].slice(0, 160)); }
}

const order = { receiptId: '4170000001', updateTs: 1789100000 };
const V = (name, value) => ({ name, value });
const MASTER = { A: {}, 'A (HUGGIE)': {}, STAR: {}, MOON: {}, MISMATCHED_7134: {}, DUAL: { pair: { v: 1, bodies: 2, mismatched: true } } };
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, masterEntry: s => MASTER[s] || null }, extra);
const mk = over => Object.assign({ transactionId: '5200000001', listingId: '1718', sku: 'A', title: 'Moon Charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [], buyerMessage: '', variations: [V('Metal Choice', 'Gold')] }, over);
const read = (over, c) => { const line = mk(over); const spec = O.interpretLine(order, line, c || ctx()); return { line, spec, asks: spec.problems.filter(p => p.count) }; };
const S = (title, vars, extra) => Object.assign({ title, variations: [V('Metal Choice', 'Gold')].concat(vars || []) }, extra || {});
const answer = (name, value, k) => ({ optionMaps: { 1718: { [O.norm(name)]: { [O.norm(value)]: { field: 'count', value: String(k) } } } } });
const pieces = (over, c) => read(over, c).spec.pieceCount;

/* ── 0 · the real snapshot lines ──────────────────────────────────────────────────────────────────────────────────────── */
{
  const fx = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/adv-count-lines.json'), 'utf8'));
  let changed = 0, was = 0, now = 0;
  for (const f of fx.lines) {
    const line = mk({ transactionId: String(f.tid), listingId: String(f.listingId), sku: f.sku, title: f.title, quantity: f.q, variations: f.vars.map(([a, b]) => V(a, b)) });
    const o = { receiptId: String(f.rid), updateTs: 0 };
    const spec = O.interpretLine(o, line, ctx({ masterEntry: s => (MASTER[s] || (s ? {} : null)) }));
    const tag = `${f.rid}/${f.tid} ${f.title.slice(0, 30)}`;
    eq(spec.quantity, f.q, tag + ': the Etsy quantity is untouched');
    eq(spec.pieceCount, f.now, tag + ': pieces ' + f.old + ' -> ' + f.now);
    // the three readers of the one count agree: the spec, the raw Etsy line (what a station page holds) and charm-nest-pair.js
    eq(O.pieceCountOf(line), f.now, tag + ': pieceCountOf(line)');
    eq(Pair.pieceCountOf(Object.assign({}, line, { receiptId: f.rid, spec }), null), f.now, tag + ': CharmNestPair.pieceCountOf agrees');
    eq(Pair.piecesFor(Object.assign({}, line, { receiptId: f.rid, spec }), null).map(p => p.side || '-').join(''), spec.pair.sides.map(s => s || '-').join(''), tag + ': CharmNestPair.piecesFor sides agree');
    was += f.old; now += f.now; if (f.old !== f.now) changed++;
  }
  ok(changed === 105 && now - was === 108, `105 lines change by +108 pieces (106 and +109 until 754e6265 read "Single ... Earring" in a title) (got ${changed} lines, +${now - was})`);
}

/* ── 1 · "Just 1" on an earring line must not turn the pair into one piece ─────────────────────────────────────────────────── */
known('F1', 'Just 1 on an earring line keeps the pair (2 pieces)', () => {
  for (const [title, vars] of [['Star Studs', [V('Qty', '2 studs')]], ['Huggie Hoops Earrings', [V('Charms', '2 charms')]]]) {
    const o = vars[0], r = read(S(title, vars), ctx(answer(o.name, o.value, 1)));
    eq([r.spec.pieceCount, r.spec.pair.sides.join('')], [2, 'LR'], title + ': a person said "this option is not a count"');
    eq(read(S(title, vars, { quantity: 2 }), ctx(answer(o.name, o.value, 1))).spec.pieceCount, 4, title + ' x2');
  }
  eq(pieces(S('Star Studs', [V('Qty', '2 studs')]), ctx(answer('Qty', '2 studs', 4))), 4, 'a real number above 1 still counts (2 pieces on each ear)');
  eq(pieces(S('Charm Necklace', [V('Pack', 'Set of 3')]), ctx(answer('Pack', 'Set of 3', 1))), 1, 'Just 1 on a necklace is 1');
});

/* ── 2 · a piece word in the option NAME and a bare number in the value ─────────────────────────────────────────────────────── */
known('F2', 'Discs: 2 / Disc Count: 2 / Charms: 3 are counts', () => {
  for (const [name, value, want] of [['Discs', '2', 2], ['Disc Count', '2', 2], ['Disc Quantity', '2', 2], ['Charms', '3', 3], ['# of discs', '2', 2], ['Discs (qty)', '2', 2], ['Select Discs', 'Two', 2], ['Choose number of discs', '2', 2]])
    eq(pieces(S('Initial Disc Necklace', [V(name, value)])), want, `${name}: ${value}`);
});
known('F2b', 'a letters count by name is asked, a single disc is 1', () => {
  ok(read(S('Initial Necklace', [V('Initials', '2')])).asks.length === 1, 'Initials: 2 is asked');
  eq(pieces(S('Initial Disc Necklace', [V('Discs', '1')])), 1, 'Discs: 1');
});

/* ── 3 · other ways to write a count ────────────────────────────────────────────────────────────────────────────────────────── */
known('F3', 'adjectives between the number and the unit, Double / Triple / Trio / Duo', () => {
  for (const [value, want] of [['3 Gold Discs', 3], ['Double Disc', 2], ['Triple Disc', 3], ['Trio of discs', 3], ['Duo discs', 2]]) {
    const r = read(S('Initial Necklace', [V('Option', value)]));
    ok(r.spec.pieceCount === want || r.asks.length === 1, `${value}: ${want} pieces or a question (got ${r.spec.pieceCount}, ${r.asks.length} asks)`);
  }
});

/* ── 4 · options that add a piece, "N of M" ──────────────────────────────────────────────────────────────────────────────────── */
known('F4', 'add a second charm / Disc 2 of 3 are asked, never silently 1', () => {
  for (const [name, value] of [['Add a second charm', 'Yes +$12'], ['Extras', 'Add Extra Charm'], ['Extras', 'Add another disc +$14'], ['Extras', '2 extra charms +$20'], ['Disc', 'Disc 2 of 3']]) {
    const r = read(S('Charm Necklace', [V(name, value)]));
    ok(r.spec.pieceCount > 1 || r.asks.length === 1, `${name}: ${value} makes more than 1 or waits (got ${r.spec.pieceCount}, ${r.asks.length} asks)`);
  }
});

/* ── 5 · the note names several discs and no option gives a count ───────────────────────────────────────────────────────────── */
known('F5', 'a note that names 2+ discs/tags with no option count waits for a person', () => {
  for (const personalization of [['Tag 1: J, Tag 2: Q'], ['I want three discs: A B C'], ['Disc 1: A; Disc 2: B']]) {
    const r = read(S('Initial Disc Necklace', [], { personalization }));
    ok(r.spec.pieceCount > 1 || r.spec.problems.some(p => p.kind === 'needsMapping' || p.count || p.kind === 'needsCount'), `${personalization[0]}: counted or asked (got ${r.spec.pieceCount}, problems ${r.spec.problems.map(p => p.kind)})`);
  }
});

/* ── 6 · a single earring, however it is written ────────────────────────────────────────────────────────────────────────────── */
{
  // fixed by PAIRINTAKE 754e6265 (a title that says Single near the earring word is one piece and names its ear)
  const single = (over, sideWant) => { const r = read(over); eq([r.spec.pieceCount, r.spec.pair.single], [1, true], JSON.stringify(over).slice(0, 80)); if (sideWant !== undefined) eq(r.spec.pair.sideSaid, sideWant, 'side'); };
  single(S('Custom Single Replacement Silver Cat Huggie Earring Left Ear'), 'L');
  single(S('Single Cat Huggie Earring'));
  single(S('Single Star Earring'));
  single(S('Star Stud Earring (single)'));
  eq(pieces(S('Star Stud Earrings, Single or Pair', [V('Type', 'Pair')])), 2, 'a title that offers both, with Pair chosen, stays a pair');
  eq(pieces(S('Star Stud Earrings')), 2, 'plain studs are still a pair');
}
known('F6', 'a side-only option, or a "Replacement ... Left Ear" title, is a single earring', () => {
  const single = (over, sideWant) => { const r = read(over); eq([r.spec.pieceCount, r.spec.pair.single], [1, true], JSON.stringify(over).slice(0, 90)); if (sideWant !== undefined) eq(r.spec.pair.sideSaid, sideWant, 'side'); };
  single(S('Star Stud Earrings', [V('Side', 'Left ear only')]), 'L');
  single(S('Star Stud Earrings', [V('Ear', 'Right')]), 'R');
  single(S('Star Stud Earrings', [V('Choose side', 'Right')]), 'R');
  single(S('Replacement Stud Earring for Left Ear'), 'L');
  single(S('Right Earring Replacement'), 'R');
});

/* ── 7 · a line that says two different designs but names one waits ─────────────────────────────────────────────────────────── */
known('F7', 'mismatched in the words, one SKU: held for a person', () => {
  for (const over of [S('Mismatched Star Stud Earrings'), S('Mix Match Earrings', [V('Left Ear Charm', 'Star'), V('Right Ear Charm', 'Moon')]), S('Zodiac Studs', [V('Metal Choice', 'Silver • 2 symbols')])]) {
    const r = read(over);
    ok(r.spec.problems.some(p => /pairSecond|needsPair|needsMapping/.test(p.kind) && (p.pair || p.pairSecond || p.count || p.kind !== 'needsMapping')), JSON.stringify(over.title) + ': a problem holds the line (got ' + r.spec.problems.map(p => p.kind) + ')');
  }
});

/* ── 8 · the station pages: Left / Right only for an earring pair ───────────────────────────────────────────────────────────── */
{
  const src = fs.readFileSync(path.join(root, 'station-live-order.js'), 'utf8');
  const win = { CharmNestOrders: O, StationActivity: null };
  vm.runInContext(src, vm.createContext({ window: win, document: { addEventListener() {} }, console, Math, Date, String, Number, Array, Object, JSON, RegExp, Set, Map, Promise }));
  const L = win.StationLiveOrder, J = x => JSON.parse(JSON.stringify(x));
  const tx = (title, vars, over) => Object.assign({ transaction_id: 5200000001, listing_id: 1718, sku: 'A', title, quantity: 1, receipt_id: 4170000001, variations: (vars || []).map(([a, b]) => ({ formatted_name: a, formatted_value: b })) }, over || {});
  const notEars = (label, t, kind) => {
    eq(J(L.ears([t])), [], label + ': no Left / Right sticker');
    eq(L.tag(t), '', label + ': no "Pair: Left + Right"');
    eq(L.kind(t).mismatched, false, label + ': not mismatched');
    eq(J(L.sides(t, '4170000001')).filter(s => s.side || s.both), [], label + ': no side and no glued tile');
    if (kind) eq(L.kind(t).kind, kind, label + ': kind');
  };
  notEars('necklace charm with a "+" in its SKU (4172944386 CAT(+FISH))', tx('Cat Add On Charm Dainty Cat Charm Gold Animal Charm', [['Metal Choice', 'Gold Filled'], ['Charm Type', 'Necklace CHARM']], { sku: 'CAT(+FISH) - Cat Only' }), 'single');
  notEars('necklace with a "/" SKU', tx('Moon Charm Necklace', [['Metal', 'Gold']], { sku: 'Moon/Star' }), 'single');
  notEars('necklace with "2 symbols"', tx('Zodiac Necklace', [['Metal Choice', 'Silver • 2 symbols']]), 'single');
  notEars('necklace whose note says "not mismatched"', tx('Charm Necklace', [['Personalization', 'not mismatched, just one']]), 'single');
  notEars('necklace with Left / Right letter options', tx('Initial Necklace', [['Left Charm Initial', 'A'], ['Right Charm Initial', 'B']]), 'single');
  notEars('single earring of a MISMATCHED design', tx('Star Stud Earrings', [['Type', 'Single']], { sku: 'MISMATCHED_7134' }));
  // a real earring pair is told as before, whatever its SKU looks like
  for (const sku of ['A', 'Cute Triceratops w/ hearts', 'Bowling Pin + Ball']) {
    const t = tx('Star Stud Earrings', [['Metal Choice', 'Gold']], { sku });
    eq(J(L.ears([t])).map(e => e.side + e.n + '/' + e.of), ['L1/1', 'R1/1'], sku + ': an earring pair prints a Left and a Right sticker');
    eq(L.tag(t), 'Pair: Left + Right', sku + ': tag'); eq(L.units([t]), 2, sku + ': two parts'); eq(L.pieces([t], '4170000001').pieceCount, 2, sku + ': two pieces');
  }
  const mis = tx('Mittens Studs', [['Metal Choice', 'Gold']], { sku: 'MISMATCHED_7134' });
  eq([L.kind(mis).kind, L.tag(mis), J(L.ears([mis])).length], ['mismatched', 'Pair: Left + Right', 2], 'a MISMATCHED_ design is still told as a mismatched pair');
  const word = tx('Mismatched Tennis Ball and Raquet Huggie Hoops Charm', [['Metal Choice', 'Gold']], { sku: 'Huggie Hoops-Tennis Ball/Racket3' });
  eq([L.kind(word).sided, L.units([word]), J(L.ears([word])).length], [true, 2, 2], 'a title that says Mismatched on an earring line is still an earring pair (2 pieces, 2 stickers)');
  const opt = tx('Zodiac Stud Earrings', [['Metal Choice', 'Silver • 2 symbols']], { sku: 'Zodiac REVAMP' });
  eq([L.kind(opt).mismatched, L.tag(opt)], [true, 'Pair: Left + Right'], 'an earring option that says 2 symbols is a mismatched pair');
  ok(true, 'stations');
}

/* ── 9 · the sorter's own live card lists every piece of a multi-piece line ─────────────────────────────────────────────────── */
{
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const a = src.indexOf('const livePairOf = (r, rid) => {'), b = src.indexOf('const CNLive = window.CNLive', a);
  ok(a > 0 && b > a, 'livePairOf is in the bridge');
  const win = { CharmNestPair: Pair, Master: { entryFor: () => null } };
  const c = vm.createContext({ window: win, String, Math, Number, Array, Object, JSON });
  vm.runInContext(src.slice(a, b) + ';this.livePairOf = livePairOf;', c);
  const J = x => JSON.parse(JSON.stringify(x));
  const rowOf = over => { const line = mk(over); return { order, line, spec: O.interpretLine(order, line, ctx()), poolIds: [] }; };
  const disc3 = c.livePairOf(rowOf(S('Initial Disc Necklace', [V('Number of Discs / Metal', '3 discs • gold')])), '4170000001');
  eq(J(disc3).map(p => [p.side, p.of, p.n, p.grp]), [0, 1, 2].map(i => [undefined, 3, i + 1, '4170000001:5200000001']), 'a 3-disc necklace is 3 pieces of one group on the live card, no side');
  eq(J(c.livePairOf(rowOf(S('Charm Necklace')), '4170000001')), [], 'a single charm is told exactly as before');
  eq(J(c.livePairOf(rowOf(S('Star Stud Earrings')), '4170000001')).map(p => p.side), ['L', 'R'], 'an earring pair is still a Left and a Right');
  eq(J(c.livePairOf(rowOf(S('Star Charm', [], { quantity: 2 })), '4170000001')), [], 'a plain quantity-2 charm is told exactly as before (the count did not change)');
  eq(J(c.livePairOf(rowOf(S('Initial Disc Necklace', [V('Necklace Options', '2 Disc')], { quantity: 2 })), '4170000001')).map(p => [p.of, p.n]), [[4, 1], [4, 2], [4, 3], [4, 4]], 'two 2-disc necklaces are 4 pieces of one group');
}

/* ── 10 · the sticker module and the efficiency record count with the pool's count ──────────────────────────────────────────── */
{
  const PL = require(path.join(root, 'charm-nest-pair-labels.js'));
  global.CharmNestPair = Pair;   // (the module reads the globals the page has)
  const rowOf = (over, c) => { const line = mk(over); return { order, line, spec: O.interpretLine(order, line, c || ctx()) }; };
  const pairRow = rowOf(S('Star Stud Earrings')), disc3 = rowOf(S('Initial Disc Necklace', [V('Number of Discs', '3')])), many = rowOf(S('Star Charm', [], { quantity: 25 }));
  eq(PL.pieceCount([pairRow, disc3], () => null), 5, 'a card with a pair and a 3-disc necklace is 2 + 3 pieces, as the pool counts it (it said 3)');
  eq(PL.pieceCount([pairRow, many], () => null), 27, 'a quantity of 25 charms is 25 pieces, not the 20 the sticker module capped it at');
  eq(PL.pieceCount([disc3], () => null), null, 'no pair on the card: null, the page counts as it always did');
  eq(PL.stickerPieces([pairRow, disc3], () => null).map(s => s.side), ['L', 'R'], 'the necklace beside a pair prints no sticker of its own');
  const sticky = (over, c) => { const r = rowOf(over, c); return { stickers: (PL.stickerPieces([r], () => null) || []).length, pool: O.pieceCountOf(r.spec) }; };
  eq(sticky(S('Star Stud Earrings', [], { quantity: 3 })), { stickers: 6, pool: 6 }, 'sticker count = pool count: 3 pairs');
  eq(sticky(S('Star Stud Earrings', [V('Qty', '2 studs')]), ctx(answer('Qty', '2 studs', 4))), { stickers: 4, pool: 4 }, 'sticker count = pool count: an answered 4');
}

console.log(`\npairs-adv-count: ${n} checks passed${open.length ? `, ${open.length} OPEN findings (${open.join(' ')}) for other owners` : ''}`);
