// ONESKU (Paul, 27 Oct 2026): the Change SKU window's "Just this order": "a no consequence, temporary one time changed to the matching of the SKU and charm ... the chosen matching SKU and charm are only
// used this one time and all subsequent times a user purchases that given listing it will revert back to the original SKU". The reader's half (charm-nest-orders.js interpretLine, ctx.skuOverride):
//   1  the number listing of Paul's screenshot (1871838409, NECKLACE_CHARM-1 bought, a 7 wanted): the line reads the person's charm, from "once", with no question; the next order of the listing reads as before
//   2  it wins over everything a listing says: a saved link (bySku), a listing-wide answer, an option mapped to a charm
//   3  an unknown SKU ("Unknown SKU" in Review) with the person's charm: no question left; an option nobody could read is answered by the choice, a font still asks
//   4  a charm no longer in the repo is not used: the line reads as Etsy has it
//   node tests/charm-nest/sku-once-read.cjs      Offline: no browser, no network.
'use strict';
const assert = require('assert'), path = require('path');
const O = require(path.join(__dirname, '../../charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; console.log('  ✓ ' + m); };

const lib = new Map(), put = (sku, holes = 1) => lib.set(sku.toUpperCase(), { sku: sku.toUpperCase(), aiPath: 'charmnest/master/' + sku.replace(/\W+/g, '_') + '.ai', widthPt: 20, heightPt: 33, members: 2, holes, sizes: null, blocked: null });
for (let i = 0; i < 10; i++) { put('Necklace_Charm-' + i); put('Necklace_Charm-' + i + ' (HUGGIE)'); }
put('COMIC NUMBER'); put('MAPLE_8065'); put('COMET_9427'); put('REPTILE 2-IGUANA (HUGGIE)');
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const byFile = new Map(); for (const [k, e] of lib) byFile.set(e.aiPath, (byFile.get(e.aiPath) || []).concat([k]));
const W = { masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: s => { const e = lib.get(String(s || '').toUpperCase()); return e ? byFile.get(e.aiPath) || [] : []; }, masterSkus: () => [...lib.keys()] };
const ctxOf = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {} }, W, extra || {});
let tx = 7600000000;
const read = (line, c) => O.interpretLine({ receiptId: '4202674150', updateTs: 1, buyerMessage: '', staffNote: '' }, JSON.parse(JSON.stringify(line)), c || ctxOf());
const kinds = sp => (sp.problems || []).map(p => p.kind);
const once = (sku, extra) => ({ skuOverride: Object.assign({ sku, by: 'Paul K.', at: 1793070000000, was: 'NECKLACE_CHARM-1' }, extra || {}) });
const NUM = (o = {}) => ({ transactionId: String(++tx), listingId: '1871838409', sku: o.sku != null ? o.sku : 'Necklace_Charm-1', title: 'Number Charm Gold Number Pendant Add On Charm Personalized Jewelry Team Number Charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled',
  personalization: [], buyerMessage: '', variations: [{ name: 'Metal Choice', value: 'Gold Filled' }, { name: 'Charm Type', value: 'Necklace CHARM' }].concat(o.num != null ? [{ name: 'Numbers', value: String(o.num) }] : []) });

// 1 · Paul's screenshot: the listing's number 1, a 7 wanted, just this once
{
  const base = read(NUM({ num: 1 }));
  ok(base.designSku === 'NECKLACE_CHARM-1', 'the number listing reads its own member as before: ' + base.designSku);
  const sp = read(NUM({ num: 1 }), ctxOf(once('NECKLACE_CHARM-7')));
  ok(sp.designSku === 'NECKLACE_CHARM-7' && sp.skuSource === 'once' && !kinds(sp).length, `"Just this order": the line is cut as NECKLACE_CHARM-7, from the person's choice, with no question (${sp.designSku}, ${sp.skuSource}, [${kinds(sp)}])`);
  ok(sp.once && sp.once.sku === 'NECKLACE_CHARM-7' && sp.once.etsy === 'NECKLACE_CHARM-1' && sp.once.was === 'NECKLACE_CHARM-1' && sp.once.by === 'Paul K.', 'the line says what it was, what Etsy says and who chose: ' + JSON.stringify(sp.once));
  ok(!sp.numberProfile && !sp.viaInventory, 'the number rule did not read it again');
  const next = read(NUM({ num: 1 }));
  ok(next.designSku === 'NECKLACE_CHARM-1' && next.skuSource !== 'once', 'the next order of the listing reads as it always did: ' + next.designSku);
}
// 2 · it wins over what the listing says: a saved link for the SKU, a listing-wide answer, an option mapped to a charm
{
  const by = ctxOf(Object.assign({ aliases: { 1700000500: { bySku: { 'COMET_9427': 'MAPLE_8065' } } } }, once('NECKLACE_CHARM-3')));
  const line = { transactionId: String(++tx), listingId: '1700000500', sku: 'COMET_9427', title: 'Comet charm', quantity: 1, metalKey: 'gold', metalLabel: 'Gold Filled', personalization: [], buyerMessage: '', variations: [{ name: 'Metal', value: 'Gold Filled' }] };
  ok(read(line, ctxOf({ aliases: { 1700000500: { bySku: { 'COMET_9427': 'MAPLE_8065' } } } })).designSku === 'MAPLE_8065', 'without it, the listing\'s saved link reads the line as MAPLE_8065');
  const sp = read(line, by);
  ok(sp.designSku === 'NECKLACE_CHARM-3' && sp.skuSource === 'once' && !kinds(sp).length, 'with it, the person\'s charm for this order wins over the saved link: ' + sp.designSku);
  const wide = read(line, ctxOf(Object.assign({ aliases: { 1700000500: { sku: 'MAPLE_8065' } } }, once('COMET_9427'))));
  ok(wide.designSku === 'COMET_9427' && wide.skuSource === 'once', 'and over a listing-wide answer: ' + wide.designSku);
  const opt = Object.assign({}, line, { transactionId: String(++tx), variations: [{ name: 'Metal', value: 'Gold Filled' }, { name: 'Charm Type', value: 'Necklace charm' }] });
  const om = { optionMaps: { 1700000500: { 'charm type': { 'necklace charm': { field: 'design', value: 'MAPLE_8065' } } } } };
  ok(read(opt, ctxOf(om)).designSku === 'MAPLE_8065', 'without it, an option mapped to a charm decides');
  const so = read(opt, ctxOf(Object.assign({}, om, once('COMET_9427'))));
  ok(so.designSku === 'COMET_9427' && so.skuSource === 'once' && !kinds(so).length, 'with it, the person\'s charm wins over the option\'s: ' + so.designSku);
}
// 3 · an unknown SKU, an option nobody could read, a font
{
  const line = { transactionId: String(++tx), listingId: '1700000600', sku: 'REPTILE 2-IGUANA', title: 'Iguana earrings', quantity: 1, metalKey: 'silver', metalLabel: 'Sterling Silver', personalization: [], buyerMessage: '', variations: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Style', value: 'Lizard B' }] };
  const before = read(line);
  ok(kinds(before).length > 0, 'without it, the line asks: [' + kinds(before) + ']');
  const sp = read(line, ctxOf(once('REPTILE 2-IGUANA (HUGGIE)', { was: 'REPTILE 2-IGUANA' })));
  ok(sp.designSku === 'REPTILE 2-IGUANA (HUGGIE)' && !kinds(sp).includes('unmatchedSku') && !kinds(sp).includes('needsMapping'), `with it, the unknown SKU and the unread option are answered by the choice (${sp.designSku}, [${kinds(sp)}])`);
  ok((sp.options || []).some(o => o.name === 'Style' && o.mapped && o.mapped.source === 'once'), 'the option is marked as answered by the choice');
  const font = Object.assign({}, line, { transactionId: String(++tx), variations: [{ name: 'Metal', value: 'Sterling Silver' }, { name: 'Font', value: 'Wiggly Script 9' }] });
  const sf = read(font, ctxOf(once('REPTILE 2-IGUANA (HUGGIE)')));
  ok(sf.designSku === 'REPTILE 2-IGUANA (HUGGIE)' && !(sf.options || []).some(o => o.name === 'Font' && o.mapped && o.mapped.source === 'once'), 'a font option is never answered by a charm choice: [' + kinds(sf) + ']');
}
// 4 · a charm gone from the repo is not used
{
  const sp = read(NUM({ num: 1 }), ctxOf(once('NO_SUCH_CHARM_42')));
  ok(sp.designSku === 'NECKLACE_CHARM-1' && sp.skuSource !== 'once' && !sp.once, 'a chosen charm no longer in the repo is not used: the line reads as Etsy has it (' + sp.designSku + ')');
}
console.log(`sku-once-read: ${n} checks OK`);
