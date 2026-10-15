// REVDROP (Paul, 15 Oct 2026, order 4198686344, listing "MODIFICATION REWORK FREE SHIPPING", SKU RE_5460): "These custom listing should not have any associated SKU ... it's strange that there is already an
// associated vector thumbnail with this particular custom listing". The shop's own code for a rework or custom listing (RE_5460, CUSTOM_6673) is a name, not a design. When the master happens to hold a
// design with the same name and the line was matched to it by the SKU as Etsy wrote it, that design is NOT the order's: the line is the buyer's own (own / ownDesign: no vector thumbnail, no SKU
// question, nothing cut from the master design), exactly as an add-on listing's is. A person's own link or answer still stands; a line with no such design is as it always was.
//   node tests/charm-nest/custom-sku-collision.cjs
'use strict';
const path = require('path'), assert = require('assert/strict');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
const order = { receiptId: '4198686344', updateTs: 1, staffNote: '', buyerMessage: '' };
const line = (sku, title, extra) => Object.assign({ transactionId: '41765762721', listingId: '1800062721', sku, title, quantity: 1, metalKey: '', metalLabel: '', personalization: [], variations: [{ name: 'Price', value: '15' }] }, extra || {});
const master = names => { const m = {}; for (const n of names) m[n] = { sku: n }; return m; };
const ctx = (m, aliases) => ({ optionMaps: {}, aliases: aliases || {}, noDesign: {}, masterEntry: s => m[String(s).toUpperCase()] || null });
const REWORK = 'MODIFICATION REWORK FREE SHIPPING';
let n = 0; const ok = (c, why) => { assert(c, why); n++; };

// 1 · the screenshot: RE_5460 is in the master (a design that shares the shop's code), the line was matched to it automatically
{ const sp = O.interpretLine(order, line('RE_5460', REWORK), ctx(master(['RE_5460'])));
  ok(sp.skuSource === 'transaction' && sp.designSku === 'RE_5460', 'the line is matched to the master design by its SKU: ' + sp.skuSource + ' ' + sp.designSku);
  ok(sp.special && sp.special.kind === 'rework' && sp.special.own === true && sp.special.collides === 'RE_5460', 'it is a rework line and its design is its own: ' + JSON.stringify(sp.special && { k: sp.special.kind, own: sp.special.own, c: sp.special.collides }));
  ok(sp.ownDesign === true, 'spec.ownDesign: the Review card draws no vector thumbnail for it');
  ok(!sp.problems.some(p => p.kind === 'needsMaterial' || p.kind === 'unknownSku' || p.kind === 'needsMapping'), 'nothing is asked about a master design that is not its own: ' + JSON.stringify(sp.problems.map(p => p.kind))); }

// 2 · no master design of that name: as it always was (a rework line that asks for its metal, no own flag)
{ const sp = O.interpretLine(order, line('RE_5460', REWORK), ctx(master([])));
  ok(sp.special && sp.special.kind === 'rework' && !sp.special.own && !sp.ownDesign && sp.problems.some(p => p.kind === 'needsMaterial'), 'unchanged without a colliding design: ' + JSON.stringify(sp.special && sp.special.kind) + ' ' + sp.problems.map(p => p.kind)); }

// 3 · a person's link for the listing stands (the "Save to the charm repository and link this listing" switch, or Use this charm): never turned into "own"
{ const m = master(['RE_5460', 'FOX_4821']), al = { 1800062721: { listingId: '1800062721', bySku: { RE_5460: 'FOX_4821' }, by: 'Paul' } };
  const sp = O.interpretLine(order, line('RE_5460', REWORK), ctx(m, al));
  ok(sp.skuSource === 'alias' && sp.designSku === 'FOX_4821', 'the saved link wins over the exact master name (DACHMAP): ' + sp.skuSource + ' ' + sp.designSku);
  ok(!(sp.special && sp.special.own) && sp.ownDesign !== true, 'a person\'s saved link is a person\'s word, never turned into "own": ' + JSON.stringify(sp.special && { k: sp.special.kind, own: sp.special.own })); }

// 4 · the custom family (CUSTOM_6673 with a master design of that name) is the same
{ const sp = O.interpretLine(order, line('CUSTOM_6673', 'Custom Charm + Necklace'), ctx(master(['CUSTOM_6673'])));
  ok(sp.special && /^custom/.test(sp.special.kind) && sp.special.own === true && sp.ownDesign === true, 'CUSTOM_6673 that the master also holds is still the buyer\'s own design: ' + JSON.stringify(sp.special && { k: sp.special.kind, own: sp.special.own })); }

// 5 · a catalogue SKU is untouched, and so is chain only and a SKU that is not one of the two families
{ const reg = O.interpretLine(order, line('HEART_9491', 'Heart Charm Necklace', { variations: [], metalKey: 'gold', metalLabel: 'Gold' }), ctx(master(['HEART_9491'])));
  ok(!reg.special && reg.designSku === 'HEART_9491' && !reg.ownDesign, 'a catalogue charm is a catalogue charm');
  const ch = O.interpretLine(order, line('CHAIN_8941', 'Chain only', { variations: [] }), ctx(master(['CHAIN_8941'])));
  ok(!ch.special || !ch.special.own, 'a chain-only SKU in the master is not turned into an own design: ' + JSON.stringify(ch.special && ch.special.kind));
  const card = O.interpretLine(order, line('CARD_1234', 'Card Charm', { variations: [], metalKey: 'gold', metalLabel: 'Gold' }), ctx(master(['CARD_1234'])));
  ok(!card.special && card.designSku === 'CARD_1234', 'CARD_ / BAG_ / BOX_ names keep their "a catalogue SKU is its design" rule (only the custom and rework codes yield)'); }

// 6 · RW_ and a dash or dot as the separator are the same family; an unrelated name that merely starts with RE is not
{ for (const s of ['RW_5460', 'RE-5460', 'RE.5460', 'RE5460']) { const sp = O.interpretLine(order, line(s, REWORK), ctx(master([s])));
    ok(sp.special && sp.special.kind === 'rework' && sp.special.own === true, s + ' is read like RE_5460: ' + JSON.stringify(sp.special && { k: sp.special.kind, own: sp.special.own })); }
  const rein = O.interpretLine(order, line('REINDEER_4821', 'Reindeer Necklace', { variations: [], metalKey: 'gold', metalLabel: 'Gold' }), ctx(master(['REINDEER_4821'])));
  ok(!rein.special && rein.designSku === 'REINDEER_4821', 'REINDEER_4821 is a catalogue charm'); }

console.log('custom SKU collision OK: ' + n + ' checks');
