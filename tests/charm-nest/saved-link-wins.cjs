// DACHMAP (Paul, 12 Oct 2026 18:39 UTC, order 4197850084, Etsy listing 1801980526 "Dachshund Dog Charm", Etsy SKU "DACHSHUND", Charm Type "CHARM + Engraving", GF 14/20):
// "Update the Mapping of this etsy listing to be matched with the Repo charm SKU". The library holds an entry named DACHSHUND whose drawing is a white cat on a blue disc (one design
// file, SITTING_0096.ai, that SITTING_0096 and DOGS hold too), and the real dachshund is DACHSHUND_36943 (14.4 x 7.5 mm).
//
// THE CAUSE (charm-nest-orders.js resolveSku): a transaction SKU that is a master SKU as it stands was returned at once (source "transaction"), BEFORE the person's saved link for this
// listing and this SKU (Charm_Sku_Aliases/{listing}.bySku, "Use this charm") was looked at. Only a form clash (HUGGIEGUARD) let a saved link win, and a necklace charm line on a cat disc
// clashes with nothing, so a saved link for "DACHSHUND" could not have changed the line.
// THE RULE: a person's saved link for THIS listing and THIS transaction SKU beats an exact master match, when the design it names is in the master and not blocked. Never the reverse:
// no link, a link for another listing or SKU, the listing-wide answer, a link to a design the master does not hold, a blocked design, or a link naming the SKU itself all read as before.
// Offline: no network, no Etsy, no AI.   node tests/charm-nest/saved-link-wins.cjs
const assert = require('assert/strict'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);

// the real entries, read from the live public masterGet / masterList on 12 Oct 2026 (design file and size)
const CAT = 'charmnest/sandbox/master/SITTING_0096.ai';
const E = [
  ['DACHSHUND', CAT, 36.51, 41.615], ['DOGS', CAT, 36.51, 41.615], ['SITTING_0096', CAT, 36.51, 41.615],
  ['DACHSHUND_36943', 'charmnest/master/DACHSHUND_36943.ai', 40.724, 21.347], ['DACHSHUND_36943 (HUGGIE)', 'charmnest/master/DACHSHUND_36943_HUGGIE_.ai', 30.759, 19.182], ['DACHSHUND (HUGGIE)', 'charmnest/sandbox/master/DACHSHUND_HUGGIE_.ai', 26.72, 19.983],
  ['DACHSHUND_88528', 'charmnest/master/DACHSHUND_88528.ai', 40.688, 24.783],
  ['OLD_BLOCKED', 'charmnest/master/OLD_BLOCKED.ai', 30, 30]
];
const lib = new Map(E.map(([sku, aiPath, widthPt, heightPt]) => [sku, { sku, aiPath, widthPt, heightPt, members: 3, sizes: null, blocked: sku === 'OLD_BLOCKED' ? 'SKU present in two master files' : null }]));
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const byFile = new Map(); for (const [k, e] of lib) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null, sharedFor = s => { const e = entryFor(s); return e ? byFile.get(e.aiPath) || [] : []; };
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: sharedFor }, extra || {});
let tx = 8000000000;
const LISTING = '1801980526';
const mk = (sku, listing) => ({ transactionId: String(++tx), listingId: String(listing || LISTING), sku, title: 'Dachshund Dog Charm Animal Jewelry Sausage Dog Charm', quantity: 1, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Pet'], buyerMessage: '',
  variations: [['Charm Type', 'CHARM + Engraving'], ['Metal Choice', 'Gold Filled']].map(([name, value]) => ({ name, value })) });
const read = (l, extra) => O.interpretLine({ receiptId: 'R' + l.transactionId, updateTs: 1 }, JSON.parse(JSON.stringify(l)), ctx(extra));
const alias = (v, listing) => ({ aliases: { [listing || LISTING]: v } });
const resolve = (l, aliases) => O.resolveSku(l, aliases || {}, entryFor, { patterns: [], skus: [] }, s => loose[O.looseKey(s)] || '');

function before() {
  const sp = read(mk('DACHSHUND'));
  eq([sp.designSku, sp.skuSource], ['DACHSHUND', 'transaction'], 'no saved link: the exact master SKU is the design, exactly as before');
  eq(entryFor('DACHSHUND').aiPath, entryFor('SITTING_0096').aiPath, 'DACHSHUND is the very drawing SITTING_0096 is (a cat on a disc)');
  step('no saved link: the exact master match "DACHSHUND" stands, as before');
}

function linkWins() {
  const saved = { v: 2, bySku: { DACHSHUND: 'DACHSHUND_36943' }, by: 'DACHMAP' };
  const r = resolve(mk('DACHSHUND'), { [LISTING]: saved });
  eq(r, { sku: 'DACHSHUND_36943', source: 'alias' }, 'resolveSku: the saved link beats the exact master match');
  const sp = read(mk('DACHSHUND'), alias(saved));
  eq([sp.designSku, sp.skuSource], ['DACHSHUND_36943', 'alias'], 'the line reads as the real dachshund');
  ok(!sp.formHold, 'not held'); eq(sp.problems.filter(p => /sku|map|design/i.test(String(p.kind))).length, 0, 'no SKU question: ' + JSON.stringify(sp.problems.map(p => p.kind)));
  eq(sp.boughtSku, 'DACHSHUND', 'the saved link is keyed by the SKU as bought, which stays on the line');
  // the same SKU written another way on the transaction
  for (const s of ['Dachshund', 'dachshund ']) eq(read(mk(s), alias(saved)).designSku, 'DACHSHUND_36943', 'the same SKU written another way: ' + JSON.stringify(s));
  step('a saved link for this listing and SKU beats the exact master match: DACHSHUND reads as DACHSHUND_36943');
}

function neverTheReverse() {
  const saved = { v: 2, bySku: { DACHSHUND: 'DACHSHUND_36943' } };
  // another listing, another SKU of the same listing: nothing changes
  eq(read(mk('DACHSHUND', '999'), alias(saved)).designSku, 'DACHSHUND', 'the link is for ONE listing: another listing with the same SKU is read as before');
  eq(read(mk('DOGS'), alias(saved)).designSku, 'DOGS', 'the link is for ONE SKU: another SKU of the listing is read as before');
  // the listing-wide answer (no per-SKU link) is not a link for this SKU
  eq(read(mk('DACHSHUND'), alias({ v: 2, sku: 'DACHSHUND_36943' })).designSku, 'DACHSHUND', 'the listing-wide answer never overrides an exact master SKU');
  eq(read(mk('DACHSHUND'), alias({ sku: 'DACHSHUND_36943' })).designSku, 'DACHSHUND', 'nor does an older listing-wide answer (no v)');
  // a link to a design the master does not hold, a blocked one, or to the SKU itself
  eq(read(mk('DACHSHUND'), alias({ v: 2, bySku: { DACHSHUND: 'NOT_IN_THE_MASTER' } })).designSku, 'DACHSHUND', 'a link to a design the master does not hold never replaces a design it does');
  eq(read(mk('DACHSHUND'), alias({ v: 2, bySku: { DACHSHUND: 'OLD_BLOCKED' } })).designSku, 'DACHSHUND', 'a link to a blocked design never replaces a good one');
  eq(resolve(mk('DACHSHUND'), { [LISTING]: { v: 2, bySku: { DACHSHUND: 'DACHSHUND' } } }), { sku: 'DACHSHUND', source: 'transaction' }, 'a link that names the SKU itself changes nothing');
  // a SKU the master does not hold is still answered by the link, as before
  eq(resolve(mk('NO_SUCH_SKU'), { [LISTING]: { v: 2, bySku: { NO_SUCH_SKU: 'DACHSHUND_36943' } } }), { sku: 'DACHSHUND_36943', source: 'alias' }, 'an unknown SKU is answered by its link, as before');
  // a blocked exact entry still gives way to the link (the order that already existed)
  eq(resolve(mk('OLD_BLOCKED'), { [LISTING]: { v: 2, bySku: { OLD_BLOCKED: 'DACHSHUND_36943' } } }), { sku: 'DACHSHUND_36943', source: 'alias' }, 'a blocked exact entry gives way to the link, as before');
  // the no-design list is a person's word too: it is not changed here
  eq(O.resolveSku(mk('DACHSHUND'), { [LISTING]: { v: 2, bySku: { DACHSHUND: 'DACHSHUND_36943' } } }, entryFor, { patterns: [], skus: ['DACHSHUND'] }, s => loose[O.looseKey(s)] || ''), { sku: 'DACHSHUND', source: 'transaction' }, 'a SKU on the no-design list stays as it was');
  step('never the reverse: no link, another listing or SKU, the listing-wide answer, an unknown or blocked design, the same SKU, the no-design list: all read as before');
}

function otherLinksStillWin() {
  // a saved pair link (two charms) still wins over the exact match, as before
  const sp = read(mk('DACHSHUND'), alias({ pairBySku: { DACHSHUND: { L: 'DACHSHUND_36943', R: 'DACHSHUND_88528' } } }));
  ok(sp.designSku === 'DACHSHUND_36943' || sp.skuSource === 'pair', 'a saved pair link still reads: ' + sp.designSku + ' ' + sp.skuSource);
  // a saved option answer (a person picked the charm for an option) still stands over the SKU
  const l = mk('DACHSHUND'); l.variations.push({ name: 'Breed', value: 'Long' });
  eq(read(l, { optionMaps: { [LISTING]: { breed: { long: { field: 'design', value: 'DACHSHUND_88528' } } } }, aliases: { [LISTING]: { v: 2, bySku: { DACHSHUND: 'DACHSHUND_36943' } } } }).designSku, 'DACHSHUND_88528', 'a person\'s option answer is still read after the SKU');
  step('a saved pair link and a saved option answer keep their own place');
}

// the back engraving of a charm is read from the line's own words and options, never from which design the SKU names: the link changes the design and where it came from, nothing else of the reading
function engravingUntouched() {
  const a = read(mk('DACHSHUND')), b = read(mk('DACHSHUND'), alias({ v: 2, bySku: { DACHSHUND: 'DACHSHUND_36943' } }));
  ok(a.engraveCandidate === true && b.engraveCandidate === true, 'the personalised charm is an engraving candidate before and after');
  const strip = sp => { const c = JSON.parse(JSON.stringify(sp)); delete c.designSku; delete c.skuSource; delete c.sources; return c; };
  eq(strip(b), strip(a), 'every other part of the reading (form, metal, engraving words, pieces, problems) is exactly the same before and after the link');
  eq([b.form, b.problems.length], ['charm', 0], 'still a charm line with nothing to ask');
  step('the back engraving reading of the charm line is unchanged by the link');
}

before(); linkWins(); neverTheReverse(); otherLinksStillWin(); engravingUntouched();
console.log(`saved-link-wins: ${n} checks ok`);
