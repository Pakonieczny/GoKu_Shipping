// WRONGCHARM, the planner (Paul, 18 Oct 2026; order 4193072538, Etsy listing 1819191541, Etsy SKU "TINY MOON & STARS 2" matched exactly to the star + moon pair; it is MOON_3288):
// charm-nest-charm-fix.js over the REAL reader (charm-nest-orders.js interpretLine) and the REAL link planner (charm-nest-review-link.js planOf / linkOf). No page, no network, no Etsy.
//   node tests/charm-nest/wrong-charm.cjs
const assert = require('assert/strict'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
const RL = require(path.join(root, 'charm-nest-review-link.js'));
const F = require(path.join(root, 'charm-nest-charm-fix.js'));
let n = 0; const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; }, ok = (c, m) => { assert(c, m); n++; };
const step = name => console.log('  ✓', name);

const OLD = 'TINY MOON & STARS 2', NEW = 'MOON_3288', LID = '1819191541', OTHER = '1819199999';
const lib = new Map([OLD, NEW, 'TINY MOON & STARS 1', 'STAR_1'].map(sku => [sku, { sku, aiPath: 'test/' + sku.replace(/\W+/g, '_') + '.ai', widthPt: 28, heightPt: 28, members: 1, sizes: null, blocked: null }]));
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null;
const ctx = maps => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => (entryFor(s) ? [String(s).toUpperCase()] : []) }, maps || {});
let tx = 7000000000;
const row = (rid, sku, listing, maps, more) => {
  const line = { transactionId: String(++tx), listingId: listing == null ? LID : listing, sku, title: 'Tiny Moon Charm Celestial Charm Moon Jewelry Add On Charm', quantity: 1, metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '', variations: [{ name: 'Metal', value: 'Gold Filled' }] };
  const order = { receiptId: rid, updateTs: 1 };
  return Object.assign({ key: rid + '_' + line.transactionId, order, line, spec: O.interpretLine(order, line, ctx(maps)), state: 'pooled', poolIds: [] }, more || {});
};
const h = { RL, O };

{ // the line Paul named
  const a = row('4193072538', OLD);
  eq([a.spec.designSku, a.spec.skuSource], [OLD, 'transaction'], 'the exact master SKU is the design');
  const off = F.offerOf(a, h);
  eq([off.ok, off.listingId, off.cur, off.from, off.key, off.via], [true, LID, OLD, OLD, `${LID}|${OLD}`, 'alias'], 'offered, keyed as the matcher keys it: ' + JSON.stringify(off));
  step('a line matched by its exact Etsy SKU is offered "Wrong charm?", keyed listing + SKU');
}
{ // never offered
  const asks = row('R1', 'NO SUCH CHARM 77');
  ok(asks.spec.problems.some(p => p.kind === 'unmatchedSku'), 'an unknown SKU asks'); eq(F.offerOf(asks, h).ok, false, 'a line that still asks is answered on its Review card');
  eq(F.offerOf(Object.assign(row('R2', OLD), { fromRecord: true }), h).ok, false, 'a line read from the records');
  eq(F.offerOf(Object.assign(row('R3', OLD), { state: 'gone' }), h).ok, false, 'a gone line');
  eq(F.offerOf(row('R4', OLD), Object.assign({ cancelled: () => true }, h)).ok, false, 'a cancelled order');
  const own = row('R5', OLD); own.spec = Object.assign({}, own.spec, { ownDesign: true }); eq(F.offerOf(own, h).ok, false, 'a line cut from its own artwork');
  const nl = row('R6', OLD, ''); eq(F.offerOf(nl, h).ok, false, 'no listing number: no key to keep it under');
  step('never offered: a line that asks, from the records, gone, cancelled, its own artwork, no listing');
}
{ // the planned link, read BEFORE it is written: it beats the exact match for this listing only; the maps themselves are untouched
  const a = row('4193072538', OLD), plan = RL.planOf(a, NEW), maps = { aliases: {}, optionMaps: {} };
  const next = F.mapsWith(maps, plan, O);
  eq(maps, { aliases: {}, optionMaps: {} }, 'the maps are not changed');
  eq(next.aliases[LID].bySku, { [OLD]: NEW }, 'the copy holds bySku[the SKU as bought]');
  const again = row('NEXT', OLD, LID, next), other = row('X', OLD, OTHER, next), sib = row('S', 'STAR_1', LID, next);
  eq([again.spec.designSku, again.spec.skuSource], [NEW, 'alias'], 'a new order of the listing reads the pick, by the saved link');
  eq([other.spec.designSku, other.spec.skuSource], [OLD, 'transaction'], 'the same SKU on another listing reads as before');
  eq([sib.spec.designSku, sib.spec.skuSource], ['STAR_1', 'transaction'], 'another SKU of the same listing reads as before');
  eq(RL.linkOf(again, next), { sku: NEW, via: 'alias', key: `${LID}|${OLD}` }, 'linkOf reads it under the same key');
  ok(RL.proof(a, NEW, ctx(next)).designOk, 'the proof a new order reads as the pick');
  step('the planned link wins over the exact SKU match for THIS listing and SKU only (read before anything is written)');
}
{ // who the pick reaches, and the one line
  const a = row('4193072538', OLD, LID, null, { poolIds: ['p1'] }), c = row('4193072539', OLD, LID, null, { poolIds: ['p2'] }), b = row('4193072540', OLD), d = row('4193072541', OLD, LID, null, { poolIds: ['p4'] });
  const x = row('4193079999', OLD, OTHER, null, { poolIds: ['p5'] }), s = row('4193070000', 'STAR_1'), g = Object.assign(row('4193070001', OLD), { state: 'skipped' }), k = row('4193070002', OLD);
  const aff = F.affectedOf([a, c, b, d, x, s, g, k], a, NEW, Object.assign({ cancelled: r => r.order.receiptId === '4193070002' }, h));
  eq(aff.orders, ['4193072538', '4193072539', '4193072540', '4193072541'], 'the open orders of the listing under the same key: not another listing, another SKU, a skipped or a cancelled line');
  eq([aff.key, aff.made], [`${LID}|${OLD}`, 3], 'three already made');
  eq(F.askText(NEW, LID, aff), `Use ${NEW} for every order of listing ${LID}? 4 open orders of it change (3 already made: each new piece goes into the old one's spot, or onto the latest unfinished sheet when it does not fit; a laser-cut piece is held for a person); the same SKU on other listings stays as it is.`, 'the confirming line');
  eq(F.askText(NEW, LID, F.affectedOf([b], b, NEW, h)), `Use ${NEW} for every order of listing ${LID}? 1 open order of it changes; the same SKU on other listings stays as it is.`, 'one order, none made');
  step('the pick reaches the open orders of the listing under the same key; one confirming line with their count');
}
console.log(`wrong-charm: ${n} checks passed`);
