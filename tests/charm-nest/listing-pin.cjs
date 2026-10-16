// A listing pinned to one design (Paul, 16 Oct 2026, order 4200443660, listing 1816178147 "Dove of Peace Charm Gold Pendant Bird Charm ... Add On Charm", Etsy SKU "DOVE 4"):
// "Manually overwrite the Etsy Listing SKU matching for this particular listing, and ensure our system always matches it with" BEADY DOVE 4 (the dove with its branch and its ring).
// "DOVE 4" is itself a design of the library (the dove with no loop), so the order read as that one. The listing is now read as BEADY DOVE 4 whatever SKU the transaction carries, over a
// person's saved link, as a person's own answer (source "alias": no automatic guard takes it back); a design the library does not hold is never swapped for another; every other listing is read as before.
// Offline, node only:  node tests/charm-nest/listing-pin.cjs
const path = require('path'), assert = require('assert/strict');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 20, 17) / 1000);
const order = rid => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Brenda Aragon' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [] });
const TITLE = 'Dove of Peace Charm Gold Pendant Bird Charm Wedding Jewelry Bridesmaid Gift Swallow Bird Charm Animal Jewelry Confirmation Gift Add On Charm';
const line = (o = {}) => Object.assign({ transactionId: '7001', listingId: '1816178147', sku: 'DOVE 4', title: TITLE, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal choice', value: 'Gold' }], personalization: ['B'], metalKey: 'gold', metalLabel: 'GF 14/20' }, o);
const LIB = new Set(['DOVE 4', 'BEADY DOVE 4', 'DOVE 4 — DISC', 'DOVE 4 WITH BRANCH']);
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: s => (LIB.has(String(s).toUpperCase()) ? { sku: String(s).toUpperCase() } : null), masterLoose: () => '' }, extra || {});
const read = (l, c) => O.interpretLine(order('4200443660'), l, c || ctx());
const unmatched = sp => (sp.problems || []).some(p => p.kind === 'unmatchedSku');

// the order as it is
{
  const sp = read(line());
  ok(sp.designSku === 'BEADY DOVE 4' && sp.skuSource === 'alias', 'order 4200443660 (listing 1816178147, Etsy SKU "DOVE 4"): read as BEADY DOVE 4, as a saved answer: ' + JSON.stringify({ d: sp.designSku, s: sp.skuSource }));
  ok(!unmatched(sp) && !sp.special, 'no unknown-SKU question, and it is an ordinary charm line (not a special kind)');
  ok(O.pinnedDesign('1816178147') === 'BEADY DOVE 4' && O.pinnedDesign(1816178147) === 'BEADY DOVE 4' && O.pinnedDesign(' 1816178147 ') === 'BEADY DOVE 4', 'the table answers for the listing number however it is written');
  ok(JSON.stringify(O.PINNED_LISTING_DESIGNS) === JSON.stringify({ '1816178147': 'BEADY DOVE 4', '1819190283': 'BEADY SQUIRREL3' }), 'the table holds these two listings and nothing else');
}
// whatever the transaction carries
for (const sku of ['DOVE 4 WITH BRANCH', 'DOVE 4 — DISC', 'BEADY DOVE 4', 'NOT_A_DESIGN_1', 'dove 4', '']) {
  const sp = read(line({ sku }));
  ok(sp.designSku === 'BEADY DOVE 4' && sp.skuSource === 'alias' && !unmatched(sp), `Etsy SKU ${JSON.stringify(sku)}: still BEADY DOVE 4`);
}
// over a person's saved link for the listing
{
  const sp = read(line(), ctx({ aliases: { 1816178147: { sku: 'DOVE 4 — DISC', bySku: { 'DOVE 4': 'DOVE 4 — DISC' } } } }));
  ok(sp.designSku === 'BEADY DOVE 4', 'a saved link for this listing and SKU to another design does not change it: ' + sp.designSku);
  const sp2 = read(line(), ctx({ aliases: { 1816178147: { listingKind: { kind: 'regular', by: 'Paul' } } } }));
  ok(sp2.designSku === 'BEADY DOVE 4', 'nor does a person calling the listing regular');
}
// a design the library does not hold is not swapped for another
{
  LIB.delete('BEADY DOVE 4');
  const sp = read(line());
  ok(sp.designSku !== 'DOVE 4' && (unmatched(sp) || sp.designSku === 'BEADY DOVE 4' || sp.designSku == null), 'BEADY DOVE 4 not in the library: never quietly read as "DOVE 4": ' + JSON.stringify({ d: sp.designSku, p: (sp.problems || []).map(p => p.kind) }));
  ok(unmatched(sp) || sp.designSku == null, 'it waits like any unknown SKU: ' + JSON.stringify((sp.problems || []).map(p => p.kind)));
  LIB.add('BEADY DOVE 4');
}
// the squirrel add-on charm (order 4192064411, listing 1819190283, Etsy SKU "SQUIRREL3 W/ NUT"): BEADY SQUIRREL3
{
  LIB.add('BEADY SQUIRREL3');
  const sq = (o = {}) => line(Object.assign({ transactionId: '7002', listingId: '1819190283', sku: 'SQUIRREL3 W/ NUT', title: 'Squirrel Charm Necklace Woodland Animal Charm Add On Charm Forest Animal Jewelry Birthday Gift for Nature Lover Woodland Creature Gift', metalKey: 'silver', metalLabel: 'SS' }, o));
  for (const sku of ['SQUIRREL3 W/ NUT', 'squirrel3 w/ nut', '']) {
    const sp = O.interpretLine(order('4192064411'), sq({ sku }), ctx());
    ok(sp.designSku === 'BEADY SQUIRREL3' && sp.skuSource === 'alias' && !unmatched(sp), `order 4192064411, Etsy SKU ${JSON.stringify(sku)}: read as BEADY SQUIRREL3, no unknown-SKU question: ` + JSON.stringify({ d: sp.designSku, s: sp.skuSource }));
  }
  const other = O.interpretLine(order('4192064411'), sq({ listingId: '1819190284' }), ctx());
  ok(other.designSku !== 'BEADY SQUIRREL3', 'another listing with that SKU is not pinned');
  LIB.delete('BEADY SQUIRREL3');
  const missing = O.interpretLine(order('4192064411'), sq(), ctx());
  ok(unmatched(missing) || missing.designSku == null || missing.designSku === 'BEADY SQUIRREL3', 'a design the library does not hold is never swapped for another');
}
// every other listing is read as before
{
  const other = read(line({ listingId: '1816178148' })), plain = read(line({ listingId: '555000111' }));
  ok(other.designSku === 'DOVE 4' && other.skuSource === 'transaction' && plain.designSku === 'DOVE 4' && plain.skuSource === 'transaction', 'the same Etsy SKU on another listing (even one number off) is still "DOVE 4" by the transaction, as before');
  ok(O.pinnedDesign('') === '' && O.pinnedDesign(null) === '' && O.pinnedDesign('abc') === '' && O.pinnedDesign('constructor') === '', 'no listing, junk and prototype names pin nothing');
}
// the save-to-repo flow reads the same
{
  const r = O.resolveSku(line({ sku: 'DOVE 4' }), {}, ctx().masterEntry, { patterns: [], skus: [] }, () => '');
  ok(r.sku === 'BEADY DOVE 4' && r.source === 'alias' && r.pinned === true, 'resolveSku itself (the one the save-to-repo flow calls) answers BEADY DOVE 4: ' + JSON.stringify(r));
  const r2 = O.resolveSku(line({ listingId: '1', sku: 'DOVE 4' }), {}, ctx().masterEntry, { patterns: [], skus: [] }, () => '');
  ok(r2.sku === 'DOVE 4' && r2.source === 'transaction' && !r2.pinned, 'and another listing is answered as before');
}
console.log('listing-pin OK ·', n, 'checks');
