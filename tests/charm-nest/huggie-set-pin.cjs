// HUGGIEPIN (Paul, 17 Oct 2026, order 4192003305, a charm-only listing, Charm Type "Huggie CHARM SET", Etsy SKU "OCTOPUS 2", read as "OCTOPUS 2 (HUGGIE)" and found in no master file):
//   "The Huggie Set option for this charm only listing needs to be associated with SKU from our charm repo" ... "Here is the Etsy listing SKU for the Huggie Octopus earrings: Huggie Hoops- Octopus2".
// The huggie set of OCTOPUS 2 is the library's "HUGGIE HOOPS- OCTOPUS2" (the master's own spelling of the dash and case is found by the loose key). Only the HUGGIE SET: the same listing's
// necklace charm option is read as it was; a design the library does not hold is never swapped for another (the line waits as an unknown SKU); every other huggie set is read as before.
// Offline, node only:  node tests/charm-nest/huggie-set-pin.cjs
const path = require('path'), assert = require('assert/strict');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; };

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 20, 17) / 1000);
const order = rid => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Test Buyer' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [] });
const TITLE = 'Octopus Charm Necklace Ocean Animal Charm Add On Charm Beach Jewelry Gift';
const line = (type, o = {}) => Object.assign({ transactionId: '7101', listingId: '1700000001', sku: 'OCTOPUS 2', title: TITLE, quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Charm Type', value: type }, { name: 'Metal choice', value: 'Gold' }], personalization: [], metalKey: 'gold', metalLabel: 'GF 14/20' }, o);
const HUGGIE = 'Huggie CHARM SET', NECK = 'Necklace CHARM';
const lib = new Set(['HUGGIE HOOPS- OCTOPUS2', 'OCTOPUS 2', 'BUNNY_42980', 'BUNNY_42980 (HUGGIE)']);
const upKey = s => String(s || '').toUpperCase();
const ctx = extra => {
  const loose = {}; for (const k of lib) loose[O.looseKey(k)] = k;
  return Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: s => (lib.has(upKey(s)) ? { sku: upKey(s) } : null), masterLoose: s => loose[O.looseKey(s)] || '' }, extra || {});
};
const read = (l, c) => O.interpretLine(order('4192003305'), l, c || ctx());
const asks = sp => (sp.problems || []).filter(p => p.kind === 'unmatchedSku' || p.kind === 'needsMapping' || p.kind === 'blockedSku');

// the order: the huggie set of "OCTOPUS 2" is HUGGIE HOOPS- OCTOPUS2
{
  const sp = read(line(HUGGIE));
  ok(sp.designSku === 'HUGGIE HOOPS- OCTOPUS2', 'the Huggie CHARM SET of Etsy SKU "OCTOPUS 2" is HUGGIE HOOPS- OCTOPUS2: ' + JSON.stringify({ d: sp.designSku, s: sp.skuSource, p: (sp.problems || []).map(p => p.kind) }));
  ok(sp.skuSource === 'alias' && asks(sp).length === 0, 'it is a settled answer: no unknown-SKU question: ' + JSON.stringify(asks(sp)));
  ok(O.pinnedHuggieSet('OCTOPUS 2 (HUGGIE)', ctx().masterEntry, ctx().masterLoose) === 'HUGGIE HOOPS- OCTOPUS2', 'pinnedHuggieSet answers for the huggie name as the intake writes it');
}
// however the SKU is written, whichever listing carries it
for (const sku of ['OCTOPUS 2', 'OCTOPUS2', 'octopus 2', 'OCTOPUS 2-CO', 'OCTOPUS2 CO', 'OCTOPUS 2 (HUGGIE)', ' Octopus  2 ']) {
  const sp = read(line(HUGGIE, { sku, listingId: '1700000002' }));
  ok(sp.designSku === 'HUGGIE HOOPS- OCTOPUS2' && asks(sp).length === 0, `Etsy SKU ${JSON.stringify(sku)}: its huggie set is HUGGIE HOOPS- OCTOPUS2: ` + JSON.stringify({ d: sp.designSku, a: asks(sp).map(p => p.kind) }));
}
// the master's own spelling of the name is found (the dash the library holds, the case, underscores)
for (const spelt of ['HUGGIE HOOPS– OCTOPUS2', 'Huggie Hoops- Octopus2', 'HUGGIE_HOOPS-_OCTOPUS2', 'HUGGIE HOOPS - OCTOPUS 2']) {
  lib.delete('HUGGIE HOOPS- OCTOPUS2'); lib.add(upKey(spelt));
  const sp = read(line(HUGGIE));
  ok(sp.designSku === upKey(spelt) && asks(sp).length === 0, `the library holds ${JSON.stringify(spelt)}: that is the design: ` + sp.designSku);
  lib.delete(upKey(spelt)); lib.add('HUGGIE HOOPS- OCTOPUS2');
}
// only the huggie set: the same listing's necklace charm is read as it always was
{
  const sp = read(line(NECK));
  ok(sp.designSku === 'OCTOPUS 2' && asks(sp).length === 0, 'the Necklace CHARM of the same listing is still the charm "OCTOPUS 2": ' + sp.designSku);
}
// a design the library does not hold is not swapped for another
{
  lib.delete('HUGGIE HOOPS- OCTOPUS2');
  const sp = read(line(HUGGIE));
  ok(sp.designSku !== 'OCTOPUS 2' && sp.designSku !== 'HUGGIE HOOPS- OCTOPUS2', 'no HUGGIE HOOPS- OCTOPUS2 in the library: never quietly read as another design: ' + sp.designSku);
  ok(asks(sp).length >= 1 && O.pinnedHuggieSet('OCTOPUS 2 (HUGGIE)', ctx().masterEntry, ctx().masterLoose) === '', 'it waits like any unknown SKU: ' + JSON.stringify(asks(sp).map(p => p.kind)));
  lib.add('HUGGIE HOOPS- OCTOPUS2');
}
// every other huggie set is read as before
{
  const sp = read(line(HUGGIE, { sku: 'BUNNY_42980', listingId: '1700000003' }));
  ok(sp.designSku === 'BUNNY_42980 (HUGGIE)' && asks(sp).length === 0, 'BUNNY_42980: its huggie set is "BUNNY_42980 (HUGGIE)", as ever: ' + sp.designSku);
  const other = read(line(HUGGIE, { sku: 'OCTOPUS 3', listingId: '1700000004' }));
  ok(other.designSku !== 'HUGGIE HOOPS- OCTOPUS2', 'OCTOPUS 3 is not OCTOPUS 2: ' + other.designSku);
  ok(JSON.stringify(O.PINNED_HUGGIE_SETS) === JSON.stringify({ OCTOPUS2: 'HUGGIE HOOPS- OCTOPUS2' }), 'the table holds this one huggie set and nothing else');
}
// a person's saved answer for the line does not take the pin back (a pinned design is a person's own answer)
{
  const sp = read(line(HUGGIE), ctx({ aliases: { 1700000001: { huggie: 'BUNNY_42980 (HUGGIE)', bySku: { 'OCTOPUS 2 (HUGGIE)': 'BUNNY_42980 (HUGGIE)' } } } }));
  ok(sp.designSku === 'HUGGIE HOOPS- OCTOPUS2', 'a saved link to another design does not change it: ' + sp.designSku);
}
console.log('huggie-set-pin: ' + n + ' checks passed');
