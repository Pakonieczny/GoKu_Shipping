// REVIEWLIST (Paul, 23 Nov 2026): "whenever this listing (718177133) gets purchased, make sure you automatically place it in Review. Also do not auto match it to any SKU/Charm moving forward",
// and the same for listing 628970499. The reader (charm-nest-orders.js interpretLine, used by every page and by the cloud) and the Review wiring (charm-nest-bridge.js, charm-nest-review-core.js):
//   1  a line of either listing is held in Review with no design, whatever matches it (its own SKU, a saved link, an option mapped to a charm)
//   2  Claude's reading and the no-design list do not take it out of Review
//   3  a person's charm for THIS order ("Just this order") is read; the next order of the listing asks again
//   4  another listing reads as before; a line a person completed by hand asks nothing; an order placed before the rule keeps its charm
//   5  each order is its own Review decision; a pick in Review and Change SKU are "Just this order" for these listings (nothing saved for the listing)
//   node tests/charm-nest/review-listings.cjs      Offline: no browser, no network.
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js')), RC = require(path.join(root, 'charm-nest-review-core.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ✓ ' + m); };

const lib = new Map(), put = sku => lib.set(sku.toUpperCase(), { sku: sku.toUpperCase(), aiPath: 'charmnest/master/' + sku.replace(/\W+/g, '_') + '.ai', widthPt: 20, heightPt: 33, members: 2, holes: 1, sizes: null, blocked: null });
['EXTENDER', 'MAPLE_8065', 'COMET_9427'].forEach(put);
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const W = { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: () => [] };
const ctxOf = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {} }, W, extra || {});
let tx = 7700000000, rid = 4250000000;
const LINE = (listingId, sku, vars) => ({ transactionId: String(++tx), listingId, sku, title: 'Charm listing', quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: [], buyerMessage: '',
  variations: [{ name: 'Metal', value: 'Gold Filled' }].concat(vars || []) });
const NEW = 1795460000, OLD = 1795400000;   // (an order placed after the rule went live, and one placed before it)
const read = (line, c, r, at) => O.interpretLine({ receiptId: String(r || ++rid), createTs: at || NEW, updateTs: 1, buyerMessage: '', staffNote: '' }, JSON.parse(JSON.stringify(line)), c || ctxOf());
const sku = sp => (sp.problems || []).filter(p => p.kind === 'unmatchedSku');
const held = (sp, lid) => !sp.designSku && sp.formHold && sp.formHold.everyOrder === lid && sku(sp).length === 1 && sku(sp)[0].everyOrder === lid && (sp.problems || [])[0].kind === 'unmatchedSku';
const once = s => ({ skuOverride: { sku: s, by: 'Paul K.', at: 1795000000000, was: 'EXTENDER' } });

ok(O.reviewListing('628970499') === '628970499' && O.reviewListing(718177133) === '718177133' && O.reviewListing('1700000500') === '', 'the two listings are the always-Review listings, no other');

// 1 · held whatever matches it
{
  const a = read(LINE('628970499', 'EXTENDER'));
  ok(held(a, '628970499'), `628970499 with a SKU the library holds: no design, held in Review (${a.designSku}, ${JSON.stringify(sku(a).map(p => p.reason))})`);
  ok(/always goes to Review/.test(sku(a)[0].reason) && !/not in any master file/.test(sku(a)[0].reason), 'its one line says why, never "Unknown SKU"');
  const b = read(LINE('718177133', 'MAPLE_8065'), ctxOf({ aliases: { 718177133: { sku: 'COMET_9427', bySku: { MAPLE_8065: 'COMET_9427' } } } }));
  ok(held(b, '718177133'), '718177133 with a saved link for the listing: the link is not used, held in Review');
  const c = read(LINE('718177133', '', [{ name: 'Charm', value: 'Maple' }]), ctxOf({ optionMaps: { 718177133: { charm: { maple: { field: 'design', value: 'MAPLE_8065' } } } } }));
  ok(held(c, '718177133'), 'an option mapped to a charm is not used either');
  ok(!a.formHold.design && a.formHold.siblings.length === 0, 'no design and no suggested sibling on the hold');
}
// 2 · Claude's reading and the no-design list do not take it out of Review
{
  const line = LINE('628970499', 'EXTENDER');
  const k = O.lineKey({ receiptId: '4250099999' }, line);
  const r1 = read(line, ctxOf({ customRead: { [k]: { kind: 'chainOnly', confidence: 0.99, summary: 'an extender, chain only' } } }), '4250099999');
  ok(held(r1, '628970499') && !r1.noDesign && !(r1.special && r1.special.notCut), 'Claude reading it as chain only: still held in Review');
  const r2 = read(LINE('628970499', 'EXTENDER'), ctxOf({ noDesign: { patterns: [], skus: ['EXTENDER'] } }));
  ok(held(r2, '628970499') && !r2.noDesign, 'its SKU on the no-design list: still held in Review');
}
// 3 · a person's charm for this order is read; the next order asks again
{
  const sp = read(LINE('628970499', 'EXTENDER'), ctxOf(once('COMET_9427')));
  ok(sp.designSku === 'COMET_9427' && sp.skuSource === 'once' && !sku(sp).length && !sp.formHold, `"Just this order" COMET_9427: cut as it, no question (${sp.designSku}, ${sp.skuSource})`);
  ok(held(read(LINE('628970499', 'EXTENDER')), '628970499'), 'the next order of the listing is held in Review again');
}
// 4 · another listing as before; completed by hand asks nothing
{
  const sp = read(LINE('1700000500', 'MAPLE_8065'));
  ok(sp.designSku === 'MAPLE_8065' && !sp.formHold && !sku(sp).length, 'another listing with the same SKU reads as before: ' + sp.designSku);
  const line = LINE('718177133', 'MAPLE_8065'), k = O.lineKey({ receiptId: '4250088888' }, line);
  const d = read(line, ctxOf({ customDone: { [k]: { state: 'done', by: 'Paul K.' } } }), '4250088888');
  ok(!sku(d).length && !d.formHold, 'a line a person completed by hand asks nothing');
}
// 4b · an order bought before the rule went live keeps its charm (no piece taken off a sheet)
{
  const sp = read(LINE('628970499', 'EXTENDER'), null, null, OLD);
  ok(sp.designSku === 'EXTENDER' && !sp.formHold && !sku(sp).length, 'an order placed before the rule: read as before, ' + sp.designSku);
  ok(!O.reviewAsks('628970499', { createTs: 0 }) && O.reviewAsks('628970499', { createTs: NEW }) && !O.reviewAsks('1700000500', { createTs: NEW }), 'reviewAsks: only the two listings, only orders from the rule\'s time');
}
// 5 · Review: one decision per order; the picks are this order's only
{
  const p = { kind: 'unmatchedSku', sku: 'EXTENDER', everyOrder: '628970499', listingId: '628970499', formHold: true };
  const k1 = RC.decisionKey({ key: '4250000001_1' }, p), k2 = RC.decisionKey({ key: '4250000002_2' }, p);
  ok(k1 !== k2 && k1 === 'ord:once:4250000001_1', 'cloud Review: each order is its own decision (' + k1 + ', ' + k2 + ')');
  ok(RC.decisionKey({ key: 'x' }, { kind: 'unmatchedSku', sku: 'EXTENDER' }) === 'ord:sku:EXTENDER', 'any other unknown SKU stays one decision for every order');
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  ok(/function decisionKey\(row, p\) \{\n    if \(p\.kind === "unmatchedSku" && p\.everyOrder\) return `ord:once:\$\{row\.key\}`;/.test(src), 'the page: the same decision key');
  const save = /async function saveCharm\(it, p, sku, rows\) \{[\s\S]*?\n  \}/.exec(src)[0];
  ok(/O\.reviewListing\(x\.line\.listingId\)[\s\S]*editSkuOnce\(x, sku, \{ by: who \}\)/.test(save) && save.indexOf('editSkuOnce') < save.indexOf('aliasPut'), 'a pick in Review is "Just this order" for these listings, before any listing link is saved');
  ok(/async function editSku\(row, sku, opt\) \{\n    if \(row && row\.line && O\.reviewListing && O\.reviewListing\(row\.line\.listingId\)\) return editSkuOnce\(row, sku, opt\);/.test(src), 'Change SKU for the listing is "Just this order" for these listings');
  ok(/let one = askEvery;/.test(src) && /once\.disabled = askEvery;[\s\S]*once\.disabled = askEvery;/.test(src), 'the Change SKU window: "Just this order" on and locked for these listings');
  ok(/!\(O\.reviewListing && O\.reviewListing\(off\.lid\)\)/.test(src), 'Update repository from an order of these listings saves no link for the listing');
}
console.log(`review-listings: ${n} checks OK`);
