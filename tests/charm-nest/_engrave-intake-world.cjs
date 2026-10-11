// The offline order data the front / back engraving golden replays (ENGINTAKE, 11 Oct 2026): every line of the Sep 17 sandbox snapshot (411), the real lines of the options studies
// (18), the font-option lines (17), the piece-count lines (118) and one line for every font-bearing option value of the 49 font listings (191). No Etsy, no Firestore, no buyer data
// (the fixtures carry titles, SKUs and option names and values only; their personalisation text was never kept), no AI.
//   lines(variant) -> [{ key, order, line }]    variant "plain": the line as the fixture has it (no words);  "words": the same line with one plain inscription ("ANNA")
//   ctx()          -> the context interpretLine is given (the SKUs the master held on 9 Oct 2026)
'use strict';
const fs = require('fs'), path = require('path');
const FX = f => JSON.parse(fs.readFileSync(path.join(__dirname, f), 'utf8'));
const SNAP = FX('fixtures/addon-snapshot-lines.json'), LINKS = FX('options-link-data.json'), FONTS = FX('fixtures/font-option-lines.json'), COUNT = FX('fixtures/adv-count-lines.json'), AUDIT = FX('fixtures/font-audit-values.json');
const KNOWN = SNAP.known;
const entryOf = s => { const k = KNOWN[String(s).toUpperCase()]; return k ? Object.assign({ sku: s }, k.blocked ? { blocked: k.blocked } : {}, k.sizes ? { sizes: Object.fromEntries(k.sizes.map(z => [z, {}])) } : {}, k.pair ? { pair: k.pair } : {}, k.facing ? { facing: k.facing } : {}, k.sym ? { sym: k.sym } : {}) : null; };
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryOf, masterLoose: s => SNAP.loose[String(s)] || '' }, extra || {});
const SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
const order = (rid, msg) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 432000, updateTs: SHIP - 431940, shipBy: SHIP, buyer: { name: 'Buyer' }, buyerMessage: msg ? 'x' : '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [] });
const mk = (variant, rid, tid, listingId, sku, title, q, vars, metalKey, msg) => ({ order: order(rid, msg), line: { transactionId: String(tid), listingId: String(listingId), sku, title, quantity: q || 1, expectedShipDate: SHIP, variations: (vars || []).map(([name, value]) => ({ name, value })), metalKey: metalKey || '', metalLabel: metalKey || '', personalization: variant === 'words' ? ['ANNA'] : [] } });
function lines(variant) {
  const out = [];
  SNAP.lines.forEach((f, i) => out.push(Object.assign({ key: 'snap:' + i }, mk(variant, f.rid, f.tid, f.listing, f.sku, f.title, f.q, f.vars, f.mk, f.msg))));
  LINKS.lines.forEach((f, i) => out.push(Object.assign({ key: 'link:' + i }, mk(variant, f.receipt, f.tx, f.listing, f.sku, f.title, f.quantity, f.variations.map(v => [v.name, v.value]), 'silver', 0))));
  FONTS.lines.forEach((f, i) => out.push(Object.assign({ key: 'font:' + i }, mk(variant, f.rid, f.tid, f.listingId, f.sku, f.title, f.q, f.vars, 'gold', 0))));
  COUNT.lines.forEach((f, i) => out.push(Object.assign({ key: 'count:' + i }, mk(variant, f.rid, f.tid, f.listingId, f.sku, f.title, f.q, f.vars, 'gold', 0))));
  AUDIT.rows.forEach((f, i) => out.push(Object.assign({ key: 'audit:' + i }, mk(variant, '4190000' + String(i).padStart(3, '0'), '5190000' + String(i).padStart(3, '0'), f.listing, 'CURB', 'Font audit line', 1, [['Metal Choice', 'Gold Filled'], [f.name, f.value]], 'gold', 0))));
  return out;
}
module.exports = { lines, ctx, SNAP };
