// OPTTWO (options-1010): the lines that say TWO designs but name one: "Mismatched Tennis Ball and Raquet Huggie Hoops" (orders 4171010675, 4173373368) and
// ZODIAC REVAMP "Silver • 2 symbols" with the Zodiac Sign Libra and the note "balance et lion" (order 4175402612). Paul, 10 Oct 2026: "I thought you were supposed to
// check all of the drop down menu SKU links ... none correctly checked and matched."   node tests/charm-nest/options-two.cjs
// Offline: no browser, no network, no Etsy call, nothing live. The three lines are the real Sep 17 snapshot lines (titles, SKUs, options, the note: no buyer data); the master is a
// list of the real master SKU names (and, when /tmp/reindex-stage/idx-final.json is there, the whole real index of 6,856 designs is read as well).
//  1 · the Tennis rows: each name is exactly one master design, so the line is a mismatched pair (Left Tennis Ball, Right Tennis Racket), never a look-alike, a plain note when one is missing
//  2 · a person's saved answer (the same on both ears / a second design) is never overridden
//  3 · the Zodiac line: the second sign is read from the line (any language the shop sells in), each sign's charm is that value of the option on this listing; a missing one is asked once
//  4 · the pool: each ear of a pair of two designs is cut from its own design, facing its own side (the real Pool in a vm, as tests/charm-nest/pairs-pool.cjs)
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), vm = require('vm');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const Pair = require(path.join(root, 'charm-nest-pair.js'));
const PP = require(path.join(root, 'charm-nest-pool-pieces.js'));
const F = require('./pairs-fixtures.cjs');
const refuseNestedArrays = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);

/* ── the master: the real SKU names these lines touch ─────────────────────────────────────────────────────────────────────────────────────────────────────── */
const NAMES = ['TENNIS BALL (HUGGIE)', 'TENNIS RACKET (HUGGIE)', 'TENNIS RACKET', 'TENNIS_73243', 'TENNIS_73243 (HUGGIE)', 'SPORTS 15 - PIN PONG RACKET', 'SPORTS 15 - PIN PONG RACKET (HUGGIE)', 'HUGGIE HOOPS- BASEBALL', 'HUGGIE HOOPS- FOOTBALL',
  'HUGGIE HOOPS- CAT + FISH', 'CAT (HUGGIE)', 'FISH (HUGGIE)', 'STAR', 'MOON', 'ZODIAC ICONS LIBRA', 'ZODIAC ICONS LEO', 'ZODIAC ICONS ARIES', 'ZODIAC ICONS LIBRA (HUGGIE)', 'ZODIAC ICONS LEO (HUGGIE)', 'ZODIAC_ICONS_EARRING_5', 'ZODIAC_ICONS_EARRING_6',
  'ZODIAC_ICONS_EARRING_0', 'ZODIAC_ICONS_EARRING_1', 'PISCES_68933', 'A'];
function master(names) {
  const m = new Map(names.map(k => [k, {}])), loose = new Map(); for (const k of m.keys()) { const key = O.looseKey(k); loose.set(key, loose.has(key) ? '' : k); }
  return { masterEntry: s => m.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose.get(O.looseKey(s)) || '' };
}
const ctx = (extra, names) => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] } }, master(names || NAMES), extra);

/* ── the three real lines ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const V = (name, value, ids) => Object.assign({ name, value }, ids ? { propertyId: ids[0], valueId: ids[1] } : {});
const TENNIS_TITLE = 'Mismatched Tennis Ball and Raquet Huggie Hoops Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold | Handcrafted Sports Jewelry';
const REAL = {
  4171010675: { order: { receiptId: '4171010675', updateTs: 1 }, line: { transactionId: '5213678588', listingId: '1744372161', productId: '27738389707', sku: 'Huggie Hoops-Tennis Ball/Racket3', title: TENNIS_TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: [], buyerMessage: '',
    variations: [V('METAL CHOICE', 'Silver', ['514', '55196991045']), V('HOOP SIZE', '8.5mm', ['513', '110638330075'])] } },
  4173373368: { order: { receiptId: '4173373368', updateTs: 1 }, line: { transactionId: '5215153110', listingId: '1744372161', productId: '28104491116', sku: 'Huggie Hoops-Tennis Ball/Racket3', title: TENNIS_TITLE, quantity: 1, metalKey: '14k', metalLabel: '14K Gold', personalization: [], buyerMessage: '',
    variations: [V('METAL CHOICE', '14k Solid Gold', ['514', '148615583071']), V('HOOP SIZE', '11mm', ['513', '65252824563'])] } },
  4175402612: { order: { receiptId: '4175402612', updateTs: 1 }, line: { transactionId: '5218016559', listingId: '1706155793', productId: '26814960626', sku: 'Zodiac REVAMP', title: 'Pisces Zodiac Stud Earrings, Astrological Symbol Earrings, Zodiac Jewelry, 14k Gold Filled Sterling Silver Earrings, Zodiac Sign Earrings', quantity: 2, metalKey: 'silver', metalLabel: 'Silver',
    personalization: ['balance et lion'], buyerMessage: '', variations: [V('Metal Choice :', 'Silver • 2 symbols', ['513', '1403828249254']), V('Zodiac Sign', 'Libra', ['514', '107267507881']), V('Personalization', 'balance et lion')] } }
};
const read = (rid, over, c, ordOver) => { const r = REAL[rid]; const line = Object.assign({}, r.line, over || {}); return { line, spec: O.interpretLine(Object.assign({}, r.order, ordOver || {}), line, c || ctx()) }; };
const members = sp => (sp.pair.members || []).map(m => m.side + ':' + m.sku);
const kinds = sp => sp.problems.map(p => p.kind);
const second = sp => sp.problems.find(p => p.pairSecond);
const optionKey = (name, value) => ({ name: O.norm(name), value: O.norm(value) });
const answerFor = (listingId, name, value, map) => ({ [String(listingId)]: { [O.norm(name)]: { [O.norm(value)]: map } } });
const deep = (a, b) => { for (const [k, v] of Object.entries(b)) a[k] = v && typeof v === 'object' && !Array.isArray(v) && a[k] && typeof a[k] === 'object' ? deep(a[k], v) : JSON.parse(JSON.stringify(v)); return a; };
const merge = (...maps) => maps.reduce((a, m) => deep(a, m), {});

/* ── 1 · the Tennis rows ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
for (const rid of ['4171010675', '4173373368']) {
  const { spec: sp } = read(rid);
  eq(members(sp), ['L:TENNIS BALL (HUGGIE)', 'R:TENNIS RACKET (HUGGIE)'], rid + ': the Left is the Tennis Ball, the Right the Tennis Racket (each the one master design that name reads as)');
  ok(sp.pair.mismatched && sp.pair.earring, rid + ': a mismatched earring pair'); eq(sp.designSku, 'TENNIS BALL (HUGGIE)', 'pooled from its Left design first');
  eq(sp.problems, [], rid + ': nothing is asked (no unmatched SKU, no second-design card)'); eq([sp.pieceCount, sp.pair.sides.join(''), sp.pair.kind], [2, 'LR', 'mismatched'], rid + ': 2 pieces, a Left and a Right');
  ok(sp.size === '8.5MM' || sp.size === '11MM', 'the hoop size is still read (' + sp.size + ')');
}
ok(read('4171010675').spec.pair.source === 'skus', 'the SKU and the title agree: evidence "skus"');
{ const two = read('4173373368', { quantity: 2 }).spec;
  eq([two.pieceCount, two.pair.sides.join('')], [4, 'LRLR'], 'quantity 2: L R L R'); eq(O.piecesOf(two).map(p => p.bodyIndex), [0, 1, 0, 1], 'bodies 0 1 0 1'); }
pass('4171010675 and 4173373368 resolve to a Left Tennis Ball and a Right Tennis Racket, 2 pieces, nothing asked');

// each source on its own
{ const t = read('4171010675', { sku: 'A' }).spec;   // the title alone ("Tennis Ball and Raquet": Raquet is the shop's spelling of Racket, and the Racket of Tennis)
  eq([members(t), t.pair.source, kinds(t)], [['L:TENNIS BALL (HUGGIE)', 'R:TENNIS RACKET (HUGGIE)'], 'title', []], 'the title alone names the pair');
  const s = read('4171010675', { title: 'Huggie Hoops Tennis', sku: 'Huggie Hoops-Tennis Ball/Racket' }).spec;   // the SKU alone, no "mismatched" in the title, no number
  eq([members(s), s.pair.source, kinds(s)], [['L:TENNIS BALL (HUGGIE)', 'R:TENNIS RACKET (HUGGIE)'], 'skus', []], 'the SKU alone names the pair');
  const swapped = read('4171010675', { sku: 'Huggie Hoops-Racket/Tennis Ball', title: 'Mismatched Tennis Racket and Tennis Ball Huggie Hoops' }).spec;
  eq(members(swapped), ['L:TENNIS RACKET (HUGGIE)', 'R:TENNIS BALL (HUGGIE)'], 'the first named is the Left, the second the Right (the buyer\'s order, not the alphabet)'); }
pass('the title alone, the SKU alone, and the other order each name the pair');

// a name never becomes a look-alike, and the one missing design is told in ONE line
{ const noBall = ctx({}, NAMES.filter(k => k !== 'TENNIS BALL (HUGGIE)'));
  const sp = read('4171010675', null, noBall).spec, q = second(sp);
  ok(!sp.pair.mismatched && !sp.pair.members, 'no Tennis Ball in the master: no pair is made'); ok(q && q.pairSecond.why.includes('“Tennis Ball” is') === false && /no single master design reads as “Tennis Ball”/.test(q.pairSecond.why), 'one plain line names the missing design: ' + (q && q.pairSecond.why));
  ok(/“Raquet” is TENNIS RACKET \(HUGGIE\)/.test(q.pairSecond.why) || /“Tennis Racket” is TENNIS RACKET/.test(q.pairSecond.why) || /TENNIS RACKET \(HUGGIE\)/.test(q.pairSecond.why), 'and says what the other name is');
  eq(sp.problems.filter(p => p.pairSecond).length, 1, 'one question, not two'); ok(sp.problems.some(p => p.kind === 'unmatchedSku'), 'the SKU as written is still not in the master (asked as before)');
  const plainOnly = ctx({}, ['TENNIS RACKET', 'TENNIS_73243', 'SPORTS 15 - PIN PONG RACKET', 'A']);
  const sp2 = read('4171010675', null, plainOnly).spec;
  ok(!sp2.pair.mismatched, 'the necklace-size TENNIS RACKET, TENNIS_73243 and a ping pong racket are no stand-in for a huggie hoop\'s Tennis Racket and Tennis Ball');
  ok(!sp2.pair.members && !!second(sp2), 'the line waits'); }
{ // a stud pair (not huggie hoops): the plain design names are the designs
  const studs = read('4171010675', { sku: 'A', title: 'Mismatched Star and Moon Stud Earrings', variations: [V('Metal Choice', 'Silver')] }).spec;
  eq([members(studs), studs.pair.source, kinds(studs)], [['L:STAR', 'R:MOON'], 'title', []], 'studs: STAR and MOON, the plain designs');
  const huggieOnly = read('4171010675', { sku: 'A', title: 'Mismatched Cat and Fish Stud Earrings', variations: [V('Metal Choice', 'Silver')] }).spec;
  ok(!huggieOnly.pair.mismatched, 'a stud line takes no "(HUGGIE)" design: CAT and FISH exist only as huggie charms, so it waits'); }
{ // the SKU and the title name different designs: nothing is guessed
  const clash = read('4171010675', { title: 'Mismatched Star and Moon Huggie Hoops Charm' }, ctx({}, NAMES.concat(['STAR (HUGGIE)', 'MOON (HUGGIE)']))).spec;
  ok(!clash.pair.mismatched && !!second(clash), 'the SKU says Tennis, the title says Star and Moon: it waits'); ok(/different designs/.test(second(clash).pairSecond.why), 'and says why: ' + second(clash).pairSecond.why); }
{ // a necklace that names two charms is not two ears
  const neck = read('4171010675', { sku: 'A', title: 'Mismatched Star and Moon Necklace', variations: [V('Metal Choice', 'Silver')] }).spec;
  ok(!neck.pair.mismatched && !neck.pair.earring && neck.pieceCount === 1, 'a necklace is never turned into an earring pair by its words'); }
{ // a line that does not name two designs at all
  const one = read('4171010675', { sku: 'A', title: 'Mismatched Studs', variations: [V('Metal Choice', 'Silver')] }).spec;
  ok(!one.pair.mismatched && !!second(one) && /two different designs/.test(second(one).pairSecond.why), 'no two names to read: the old plain question'); }
pass('a missing design is told in one line, a look-alike never stands in, a clash and a necklace wait or stay as they were');

/* ── 2 · a person's saved answer is kept ─────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{ const lid = '1744372161', lineId = '4171010675/5213678588', K = 'Two designs on this line';
  const same = read('4171010675', null, ctx({ optionMaps: answerFor(lid, K, lineId, { field: 'ignore', value: null }) })).spec;
  ok(!same.pair.mismatched && !same.pair.members, 'the same on both ears, said by a person for this line: the names do not override it'); eq(same.pair.second.answered, 'same', 'and it is read as answered'); ok(!second(same), 'nothing is asked again');
  eq(same.pair.sides, ['L', 'R'], 'a matching pair: a Left and a Right');
  const other = read('4173373368', null, ctx({ optionMaps: answerFor(lid, K, lineId, { field: 'ignore', value: null }) })).spec;
  ok(other.pair.mismatched, 'the answer was for that line only: the other order still resolves');
  const named = read('4171010675', { sku: 'A', title: 'Mismatched Studs', variations: [V('Metal Choice', 'Silver')] }, ctx({ optionMaps: answerFor(lid, K, lineId, { field: 'design', value: 'MOON' }) })).spec;
  eq(members(named), ['L:A', 'R:MOON'], 'a second design a person named still makes the pair (Left the line\'s design)'); eq(named.pair.source, 'answer', 'by their answer');
  const both = read('4171010675', null, ctx({ optionMaps: answerFor(lid, K, lineId, { field: 'design', value: 'MOON' }) })).spec;
  eq(members(both), ['L:TENNIS BALL (HUGGIE)', 'R:TENNIS RACKET (HUGGIE)'], 'the SKU\'s own two names are the evidence when they are exact (a second design is asked only where none is)'); }
pass('a person\'s "the same on both ears" and a second design they named are kept');

/* ── 3 · the Zodiac line ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
const LID = '1706155793', SIGN = 'Zodiac Sign';
const mapOf = (...ps) => merge(...ps.map(([v, sku]) => answerFor(LID, SIGN, v, { field: 'design', value: sku })));
{ const sp = read('4175402612').spec;   // as it stands today: nothing is mapped for this listing
  eq(sp.problems.map(p => p.optionName + '=' + p.optionValue), ['Zodiac Sign=Libra', 'Zodiac Sign=Leo'], 'the line\'s own sign and the second sign (from "balance et lion") are each asked, as option values, once');
  const leo = sp.problems.find(p => p.optionValue === 'Leo'); ok(/second symbol of a 2-symbols line: Leo/.test(leo.note) && /balance et lion/.test(leo.note) && /Libra/.test(leo.note), 'the question says where Leo came from: ' + leo.note);
  ok(!second(sp) && sp.pair.second && sp.pair.second.viaOption && !sp.pair.second.answered, 'no second "Two designs?" card: the question is the option value\'s'); ok(!sp.pair.mismatched && sp.pair.earring, 'still an earring pair, waiting'); eq([sp.pieceCount, sp.pair.sides.join('')], [4, 'LRLR'], 'quantity 2 is 4 pieces');
  ok(!sp.problems.some(p => p.kind === 'unmatchedSku'), 'the listing\'s own SKU "Zodiac REVAMP" is not asked about while an option is open (as before)'); }
{ // Libra and Leo each answered for this listing (what the Review card keeps): the pair
  const sp = read('4175402612', null, ctx({ optionMaps: mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5'], ['Leo', 'ZODIAC_ICONS_EARRING_6']) })).spec;
  eq(members(sp), ['L:ZODIAC_ICONS_EARRING_5', 'R:ZODIAC_ICONS_EARRING_6'], 'Libra the Left, Leo the Right');
  eq([sp.pair.source, sp.pair.mismatched, sp.problems.length, sp.pieceCount, sp.pair.sides.join(''), sp.designSku], ['signs', true, 0, 4, 'LRLR', 'ZODIAC_ICONS_EARRING_5'], 'resolved: no question, 4 pieces at quantity 2'); eq(O.piecesOf(sp).map(p => p.bodyIndex), [0, 1, 0, 1], 'bodies 0 1 0 1');
  // only Libra answered: Leo is asked, nothing is picked for it
  const half = read('4175402612', null, ctx({ optionMaps: mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5']) })).spec;
  eq(half.problems.map(p => p.optionName + '=' + p.optionValue), ['Zodiac Sign=Leo'], 'Libra is answered: only Leo is asked'); ok(!half.pair.mismatched, 'and the line waits');
  // the answer for Leo is the listing's: a one-symbol Leo line on the same listing reads it too
  const leoOne = read('4175402612', { quantity: 1, variations: [V('Metal Choice :', 'Silver • 1 symbol', ['513', '1403828248908']), V('Zodiac Sign', 'Leo', ['514', '107267507999'])], personalization: [] }, ctx({ optionMaps: mapOf(['Leo', 'ZODIAC_ICONS_EARRING_6']) })).spec;
  eq([leoOne.designSku, kinds(leoOne), leoOne.pair.mismatched, leoOne.pieceCount], ['ZODIAC_ICONS_EARRING_6', [], false, 2], 'the same answer makes a 1-symbol Leo line (a matching pair, 2 pieces)'); }
{ // the SKU Etsy ties to the value of the option (the listing's inventory): Libra needs no answer
  const table = { at: 1, n: 3, products: [{ id: '26814960626', sku: 'ZODIAC_ICONS_EARRING_5', d: 0, pv: [['513', '1403828249254'], ['514', '107267507881']] }, { id: '26814960560', sku: 'ZODIAC_ICONS_EARRING_6', d: 0, pv: [['513', '1403828248908'], ['514', '107267507841']] }, { id: '1', sku: 'ZODIAC_ICONS_EARRING_0', d: 0, pv: [['513', '1403828248908'], ['514', '107267507900']] }] };
  const line = { sku: 'ZODIAC_ICONS_EARRING_5' };
  const sp = read('4175402612', line, ctx({ listingSkus: { [LID]: table }, optionMaps: mapOf(['Leo', 'ZODIAC_ICONS_EARRING_6']) })).spec;
  eq(members(sp), ['L:ZODIAC_ICONS_EARRING_5', 'R:ZODIAC_ICONS_EARRING_6'], 'Libra from its own SKU (tied to the value by the inventory), Leo from the answer');
  const asks = read('4175402612', line, ctx({ listingSkus: { [LID]: table } })).spec;
  eq(asks.problems.map(p => p.optionName + '=' + p.optionValue), ['Zodiac Sign=Leo'], 'with only the inventory: Libra is answered by its SKU, Leo is asked'); }
{ // the second sign is not invented
  const noNote = read('4175402612', { personalization: [], variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', 'Libra')] }, ctx({ optionMaps: mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5']) })).spec, q = second(noNote);
  ok(q && /two different designs/.test(q.pairSecond.why) && /names one \(Libra\)/.test(q.pairSecond.why) && /second symbol/.test(q.pairSecond.why), 'only one sign anywhere: ONE plain line asks for the second: ' + (q && q.pairSecond.why));
  ok(!noNote.pair.mismatched && noNote.problems.every(p => p.optionValue !== 'Leo'), 'nothing is made up'); eq(noNote.pieceCount, 4, 'it still counts its pair');
  const sameSign = read('4175402612', { personalization: ['balance'], variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', 'Libra'), V('Personalization', 'balance')] }, ctx({ optionMaps: mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5']) })).spec;
  ok(!!second(sameSign) && !sameSign.pair.mismatched, 'the note repeats Libra: still one sign, the line waits');
  const three = read('4175402612', { personalization: ['lion, aries'], variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', 'Libra'), V('Personalization', 'lion, aries')] }, ctx({ optionMaps: mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5']) })).spec;
  ok(!!second(three) && /3 signs/.test(second(three).pairSecond.why) && !three.pair.mismatched, 'three signs named: not guessed which two');
  const same = read('4175402612', null, ctx({ optionMaps: merge(mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5'], ['Leo', 'ZODIAC_ICONS_EARRING_5'])) })).spec;
  ok(!same.pair.mismatched && !!second(same) && /same charm/.test(second(same).pairSecond.why), 'two signs on one charm are not a mismatched pair'); }
{ // the person said "the same on both ears" for the line: Libra on both, no second sign is asked
  const lineId = '4175402612/5218016559';
  const sp = read('4175402612', null, ctx({ optionMaps: merge(mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5']), answerFor(LID, 'Two designs on this line', lineId, { field: 'ignore', value: null })) })).spec;
  ok(!sp.pair.mismatched && sp.problems.length === 0 && sp.pair.second.answered === 'same', 'kept: the same on both ears, nothing asked'); eq(sp.pair.sides.join(''), 'LRLR', 'a matching pair, 4 pieces'); }
{ // names in the shop's languages, and a second dropdown
  const sg = (opt, note, over) => read('4175402612', Object.assign({ personalization: note ? [note] : [], variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', opt)].concat(note ? [V('Personalization', note)] : []) }, over || {}), ctx({ optionMaps: mapOf(['Aries', 'ZODIAC_ICONS_EARRING_0'], ['Taurus', 'ZODIAC_ICONS_EARRING_1'], ['Libra', 'ZODIAC_ICONS_EARRING_5'], ['Leo', 'ZODIAC_ICONS_EARRING_6']) })).spec;
  for (const [opt, note, want] of [['Aries', 'Tauro', ['ZODIAC_ICONS_EARRING_0', 'ZODIAC_ICONS_EARRING_1']], ['Aries', 'taureau', ['ZODIAC_ICONS_EARRING_0', 'ZODIAC_ICONS_EARRING_1']], ['Aries', 'Stier', ['ZODIAC_ICONS_EARRING_0', 'ZODIAC_ICONS_EARRING_1']], ['Libra', 'Löwe', ['ZODIAC_ICONS_EARRING_5', 'ZODIAC_ICONS_EARRING_6']], ['Libra', 'Leone', ['ZODIAC_ICONS_EARRING_5', 'ZODIAC_ICONS_EARRING_6']], ['Libra', 'Leo', ['ZODIAC_ICONS_EARRING_5', 'ZODIAC_ICONS_EARRING_6']], ['Libra', 'Libra and Leo please', ['ZODIAC_ICONS_EARRING_5', 'ZODIAC_ICONS_EARRING_6']]])
    eq((sg(opt, note).pair.members || []).map(m => m.sku), want, `${opt} + "${note}"`);
  const second2 = sg('Libra', '', { variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', 'Libra'), V('Second Zodiac Sign', 'Leo')] }).pair;
  ok(!second2.mismatched, 'a second dropdown is asked under its own name until it is answered (it is another option)');
  const both = read('4175402612', { personalization: [], variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', 'Libra'), V('Second Zodiac Sign', 'Leo')] }, ctx({ optionMaps: merge(mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5']), answerFor(LID, 'Second Zodiac Sign', 'Leo', { field: 'design', value: 'ZODIAC_ICONS_EARRING_6' })) })).spec;
  eq((both.pair.members || []).map(m => m.sku), ['ZODIAC_ICONS_EARRING_5', 'ZODIAC_ICONS_EARRING_6'], 'two dropdowns, each answered: the pair'); }
{ // lines that must stay exactly as they were
  const one = read('4175402612', { quantity: 1, personalization: [], variations: [V('Metal Choice :', 'Silver • 1 symbol'), V('Zodiac Sign', 'Pisces', ['514', '107267507841'])], title: 'Pisces Zodiac Stud Earrings' }).spec;
  eq([kinds(one), one.pair.says, one.pair.mismatched, one.pieceCount], [['needsMapping'], false, false, 2], '"1 symbol": the same sign on both ears, only the option is asked');
  ok(!read('4175402612', { title: 'Zodiac Charm Necklace', variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', 'Libra'), V('Personalization', 'lion')] }, ctx({ optionMaps: mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5'], ['Leo', 'ZODIAC_ICONS_EARRING_6']) })).spec.pair.mismatched, 'a necklace is not an earring pair: no sign makes it one');
  // an old option answer that named a design for the SIGN still decides the line when there is no second sign
  const old = read('4175402612', { quantity: 1, personalization: [], variations: [V('Metal Choice :', 'Silver • 1 symbol'), V('Zodiac Sign', 'Libra')] }, ctx({ optionMaps: mapOf(['Libra', 'ZODIAC_ICONS_EARRING_5']) })).spec;
  eq([old.designSku, kinds(old), old.pieceCount], ['ZODIAC_ICONS_EARRING_5', [], 2], 'a plain 1-symbol line with its answer: as before'); }
pass('Zodiac "2 symbols": the second sign from the line, each sign\'s charm from the listing, a missing one asked once, nothing invented, the old lines unchanged');

/* ── the real master, when it is on this machine ─────────────────────────────────────────────────────────────────────────────────────────────────────── */
const IDX = '/tmp/reindex-stage/idx-final.json';
if (fs.existsSync(IDX)) {
  const idx = JSON.parse(fs.readFileSync(IDX, 'utf8')), names = Object.values(idx.entries).map(e => String(e.sku).toUpperCase());
  const c = ctx({}, names);
  for (const rid of ['4171010675', '4173373368']) { const sp = read(rid, null, c).spec; eq([members(sp), kinds(sp)], [['L:TENNIS BALL (HUGGIE)', 'R:TENNIS RACKET (HUGGIE)'], []], rid + ' against the real 6,856-design master'); }
  // every other mismatched-looking snapshot line keeps its answer: run the whole snapshot when it is there
  const snap = '/tmp/advcount-data/snapshot-min.json';
  if (fs.existsSync(snap)) {
    const orders = JSON.parse(fs.readFileSync(snap, 'utf8')); let lines = 0, pairs = [], waits = [];
    for (const r of orders) for (const t of r.transactions) {
      const line = { transactionId: String(t.transaction_id), listingId: String(t.listing_id), productId: String(t.product_id), sku: t.sku, title: t.title, quantity: t.quantity, metalKey: /silver/i.test(JSON.stringify(t.variations)) ? 'silver' : 'gold', personalization: t.personalization || [], buyerMessage: r.message_from_buyer || '',
        variations: t.variations.map(v => ({ name: v.formatted_name, value: v.formatted_value, propertyId: String(v.property_id), valueId: v.value_id ? String(v.value_id) : '' })) };
      const sp = O.interpretLine({ receiptId: String(r.receipt_id), updateTs: 1 }, line, c); lines++;
      if (sp.pair.mismatched && sp.pair.members) pairs.push(r.receipt_id + ' ' + members(sp).join(' '));
      if (sp.pair.second && !sp.pair.second.answered) waits.push(r.receipt_id + ' ' + (sp.pair.second.why || '').slice(0, 90));
    }
    console.log(`  · real snapshot (${lines} lines): ${pairs.length} pair(s) of two designs: ${pairs.join(' | ') || 'none'}; ${waits.length} waiting for a second design: ${waits.join(' | ') || 'none'}`);
    ok(pairs.length === 2 && pairs.every(p => /TENNIS BALL \(HUGGIE\) R:TENNIS RACKET \(HUGGIE\)$/.test(p)), 'on the real snapshot exactly the two Tennis lines became pairs of two designs');
    ok(waits.length === 1 && /^4175402612 /.test(waits[0]), 'and only the Zodiac line waits (for its second symbol\'s charm)');
  }
  pass('against the real master index: the Tennis pair, and the whole Sep 17 snapshot');
} else console.log('  · (the real master index is not on this machine: the named subset above stands)');

/* ── 4 · the pool: each ear is cut from its own design ──────────────────────────────────────────────────────────────────────────────────────────────── */
(async () => {
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const a = src.indexOf('const Pool = window.Pool = (() => {'), b = src.indexOf('\n/* Carry-forward is keyed', a);
  assert(a > 0 && b > a, 'the Pool module is where the test expects it');
  const MM = 25.4 / 72, J = x => JSON.parse(JSON.stringify(x));
  function world(designs, extra) {
    const logs = [], calls = [], pages = { gold: { metal: 'gold', charms: [], placements: [], status: 'idle', sheetId: null } }, entries = {};
    for (const sku of designs) entries[sku] = Object.assign(F.entryOf(sku), { sku, aiPath: 'charmnest/master/sku/' + sku + '.ai', aiUrl: 'https://x/' + sku, masterHash: 'mh-' + sku, upAngle: 0 });
    const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(16).padStart(8, '0'); };
    const c = Object.assign({
      console, Date, JSON, Map, Set, Math, Number, String, Array, Object, Promise, Error, Uint8Array, setTimeout, clearTimeout, performance: { now: () => Date.now() }, structuredClone,
      window: { CharmNestPair: Pair, CharmNestPoolPieces: PP }, O, MM, labelOf: m => m, stockFor: () => ({ wPt: 1400, hPt: 700 }),
      S: { cloud: { ok: true }, settings: { insetPt: 0, maxFill: .8, silhouetteRes: 6, minPt: 6 }, poolSources: {}, sheets: { gold: { active: 0 } } },
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
  const lineRow = (rid, tid, sku, qty, members) => {
    const line = { transactionId: tid, listingId: '1', sku };
    const sp = Object.assign({ designSku: sku, material: 'gold', quantity: qty, size: null, form: 'earrings', chain: null }, {});
    const pair = { earring: true, mismatched: true, source: 'skus', members: members.map((m, i) => ({ side: i ? 'R' : 'L', sku: m })), glued: false, perUnit: 2, sides: [], kind: qty === 1 ? 'mismatched' : 'multi' };
    for (let i = 0; i < 2 * qty; i++) pair.sides.push(i % 2 ? 'R' : 'L');
    return { key: `${rid}_${tid}`, state: 'pulled', order: { receiptId: rid, createTs: 1791500000, updateTs: 1791500001 }, line, spec: Object.assign(sp, { pair, pieceCount: 2 * qty }), problems: [], poolIds: [], engrave: null, arrivedAt: 0 };
  };
  for (const [left, right, qty, wantMirror] of [['PAIR-FACE-L', 'PAIR-FACE-R', 1, [false, false]], ['PAIR-FACE-R', 'PAIR-FACE-L', 1, [true, true]], ['PAIR-FACE-L', 'PAIR-STUD', 2, [false, true, false, true]], ['PAIR-STUD', 'PAIR-FACE-R', 1, [false, false]]]) {
    const w = world([left, right]), row = lineRow('4190000201', '5000000201', left, qty, [left, right]);
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut'); assert(put, `${left} + ${right}: recorded`); const pools = put.body.pools, nn = 2 * qty;
    eq(pools.map(p => p.sku), Array.from({ length: nn }, (_, i) => (i % 2 ? right : left)), `${left} + ${right}: the Left ear is cut from the first design, the Right from the second (the row says so)`);
    eq(pools.map(p => p.side), Array.from({ length: nn }, (_, i) => (i % 2 ? 'R' : 'L')), 'sides L R'); eq(pools.map(p => p.mirror), wantMirror, `each ear faces its own side: mirrored only when its own drawing faces the wrong way (${left} + ${right})`);
    eq(pools.map(p => p.bodyIndex), Array.from({ length: nn }, (_, i) => i % 2), 'the Left is body 0 and the Right body 1 of the pair');
    ok(pools.every(p => p.groupKey === '4190000201:5000000201' && p.groupSize === nn), 'one group, all of its pieces');
    eq(pools.map(p => p.aiPath), Array.from({ length: nn }, (_, i) => 'charmnest/master/sku/' + (i % 2 ? right : left) + '.ai'), 'each row points at its own design file');
    eq(pools.map(p => p.masterHash), Array.from({ length: nn }, (_, i) => 'mh-' + (i % 2 ? right : left)), 'and its own master hash');
    const charms = w.pages.gold.charms; eq(charms.length, nn, 'the sheet gets every piece');
    charms.forEach((ch, i) => { const sku = i % 2 ? right : left; eq([ch.sku, ch.orderInfo.sku, ch.side, ch.mirror, ch.poolId], [sku, sku, i % 2 ? 'R' : 'L', wantMirror[i], pools[i].poolId], `piece ${i + 1}: its own design, side and mirror`);
      ok(ch.name === `4190000201 · ${sku} · ${i + 1}/${nn}`, 'named by its own design: ' + ch.name);
      const own = F.charmOf(sku); ok(ch.members.length === own.members.length || ch.mirror, 'cut from its own design only'); });
    ok(row.state === 'pooled' && row.poolIds.length === nn, 'pooled'); ok(!w.logs.some(l => /glued/.test(l)), 'never made as the one glued piece of the first design');
  }
  pass('the pool cuts the Left ear from the first design and the Right ear from the second, each facing its own side, one group');

  { // a second design the master cannot give: the line is held, never made as the first alone
    const w = world(['PAIR-FACE-L']), row = lineRow('4190000202', '5000000202', 'PAIR-FACE-L', 1, ['PAIR-FACE-L', 'NOT-IN-MASTER']);
    const prep = await w.Pool.poolAdd(row, null);
    ok(!w.calls.some(x => x.body && x.body.op === 'poolPut') && w.pages.gold.charms.length === 0, 'nothing is written or put on a sheet'); ok(row.state === 'unmatched' && row.problems.some(p => p.kind === 'unmatchedSku' && p.sku === 'NOT-IN-MASTER'), 'it is asked about the missing design: ' + row.state);
    void prep; }
  { // a line that is not a pair of two designs is exactly what it was (one design, one source)
    const w = world(['PAIR-FACE-L']), row = lineRow('4190000203', '5000000203', 'PAIR-FACE-L', 1, ['PAIR-FACE-L', 'PAIR-FACE-L']);
    row.spec.pair.mismatched = false; row.spec.pair.members = null; row.spec.pair.kind = 'pair';
    await w.Pool.poolAdd(row, null); const put = w.calls.find(x => x.body.op === 'poolPut');
    eq(put.body.pools.map(p => [p.sku, p.side, p.mirror, p.bodyIndex]), [['PAIR-FACE-L', 'L', false, 0], ['PAIR-FACE-L', 'R', true, 0]], 'a matching pair: both from the one design, the Right the mirror, body 0 (unchanged)'); }
  pass('a missing second design holds the line; a matching pair is unchanged');
  /* ── 5 · the pictures: each ear of a pair of two designs is drawn from its own design ─────────────────────────────────────────────────────────────────── */
  { const lm = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), x = lm.indexOf('  const pairRow = row => { try { const CP = window.CharmNestPair'), y = lm.indexOf('  /** A line\'s vector design drawn large on white'), z = lm.indexOf('  /** What makes two lines\' vector designs one picture'), z2 = lm.indexOf('  /** A line\'s vector design into one box');
    assert(x > 0 && y > x && z > y && z2 > z, 'the picture code is where the test expects it');
    const w = world(['PAIR-FACE-L', 'PAIR-FACE-R']), drawn = [];
    const c2 = vm.createContext({ console, Object, String, Array, Promise, Map, Set, JSON, Number, window: { CharmNestPair: Pair }, P: {}, Engrave: {}, catalog: new Map(), Pool: { charmOf: () => null, masterFront: (e, size, px, ro) => { drawn.push([e.sku, 'front', !!(ro && ro.mirror)]); return 'img'; }, masterPreview: (e, size, big, ro) => { drawn.push([e.sku, 'preview', !!(ro && ro.mirror)]); return 'img'; } } });
    c2.Master = { entryFor: sku => w.entries[sku] || null, fetchEntry: async () => null };
    vm.runInContext(lm.slice(x, y) + '\n' + lm.slice(z, z2) + '\n;this.__v = { vector, vectorKey, earDesign };', c2);
    const V2 = c2.__v, row = Object.assign(lineRow('4190000301', '5000000301', 'PAIR-FACE-L', 1, ['PAIR-FACE-L', 'PAIR-FACE-R']), { poolIds: ['p1'] });
    await V2.vector(row, 120, { side: 'L', highlight: 'L', mirror: false }); await V2.vector(row, 120, { side: 'R', highlight: 'R', mirror: false });
    eq(drawn.map(d => d[0]), ['PAIR-FACE-L', 'PAIR-FACE-R'], 'the Left row draws the Left design and the Right row the Right design (not the first design twice)');
    ok(V2.vectorKey(row, { side: 'L', highlight: 'L' }) !== V2.vectorKey(row, { side: 'R', highlight: 'R' }), 'and they are two pictures, never shared');
    ok(/PAIR-FACE-R/.test(V2.vectorKey(row, { side: 'R', highlight: 'R' })) && /PAIR-FACE-L/.test(V2.vectorKey(row, { side: 'L' })), 'the key names the ear\'s own design');
    eq(V2.earDesign(row, 'R'), 'PAIR-FACE-R', 'earDesign'); eq(V2.earDesign(row, 'X'), '', 'no ear, no design');
    const one = lineRow('4190000302', '5000000302', 'PAIR-FACE-L', 1, ['PAIR-FACE-L', 'PAIR-FACE-L']); one.spec.pair.mismatched = false; one.spec.pair.members = null;
    drawn.length = 0; await V2.vector(one, 120, { side: 'R', mirror: true }); eq(drawn, [['PAIR-FACE-L', 'front', true]], 'a matching pair is drawn as before: its one design, the Right turned over'); eq(V2.earDesign(one, 'R'), '', 'no second design on a matching pair'); }
  pass('the Left and Right rows draw their own designs (a matching pair is drawn as it was)');
  console.log(`options-two: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
