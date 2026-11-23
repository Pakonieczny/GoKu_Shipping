// LISTMETAL (Paul, 23 Nov 2026, order 4235923653: "When buyer chooses this Gold Filled option ... our system interprets it as Rose Gold"; 4230787700 and 4238855149 the same): a metal named in a "Personalization" drop-down
// is the metal, before the listing's title. The real charm-nest-etsy-shape.js, the orders' own choices and titles.   LISTMETAL_ROOT=<another checkout> node tests/charm-nest/listing-metal.cjs
'use strict';
const path = require('path'), root = process.env.LISTMETAL_ROOT ? path.resolve(process.env.LISTMETAL_ROOT) : path.join(__dirname, '../..'), S = require(path.join(root, 'charm-nest-etsy-shape.js'));
let n = 0, failed = 0; const ok = (m, c, d) => { n++; console.log((c ? 'OK   ' : 'FAIL ') + m + (c || d === undefined ? '' : ' :: ' + JSON.stringify(d))); if (!c) failed++; };
const TITLE = 'Personalized Disc Bracelet, Handwriting Engraved Bracelet - YOUR HANDWRITING - or Image, Sterling Silver, Gold or Rose Gold, Jewelry For Her';
const tx = (lid, choice, o) => Object.assign({ transaction_id: 5295960022, listing_id: lid, sku: 'Personalized_6464', title: TITLE, quantity: 1, personalization: [choice],
  variations: [{ formatted_name: 'Personalization', formatted_value: choice, property_id: 514, value_id: 476853435367 }, { formatted_name: 'Metal/Length', formatted_value: '6.5 inches', property_id: 513 }] }, o || {});
const key = t => S.shapeLine(t, {}).metalKey;
ok('order 4235923653: GOLD·Engraved (listing 581466464) is Gold Filled', key(tx(581466464, 'GOLD·Engraved')) === 'gold', key(tx(581466464, 'GOLD·Engraved')));
ok('order 4230787700: the same choice', key(tx('581466464', 'GOLD·Engraved')) === 'gold');
ok('order 4238855149: GOLD·Front is Gold Filled, SILVER·Front is Silver (listing 522898333)', key(tx(522898333, 'GOLD·Front')) === 'gold' && key(tx(522898333, 'SILVER·Front')) === 'silver', [key(tx(522898333, 'GOLD·Front')), key(tx(522898333, 'SILVER·Front'))]);
ok('ROSE GOLD · Engraved and ROSEGOLD·Front stay Rose Gold', key(tx(581466464, 'ROSE GOLD · Engraved')) === 'rose' && key(tx(522898333, 'ROSEGOLD·Front')) === 'rose');
ok('gold filled · back is Gold Filled', key(tx(1, 'Gold Filled · Back')) === 'gold');
ok('words the buyer typed (property 54) are never read as the metal', (() => { const t = tx(581466464, 'GOLD·Engraved'); t.variations[0].property_id = 54; return key(t) === 'rose'; })());
ok('a drop-down choice that is no metal and engraving word reads as before', key(tx(1, 'GOLD FOREVER')) === 'rose' && key(tx(1, 'Golden·Front')) === 'rose' && key(tx(1, 'Engraved')) === 'rose');
{ const t = tx(581466464, 'GOLD·Engraved'); t.variations.push({ formatted_name: 'Metal', formatted_value: 'Silver', property_id: 513 }); ok('a Metal option that names a metal still comes first', key(t) === 'silver'); }
{ const t = tx(581466464, 'GOLD·Engraved'); t.variations.push({ formatted_name: 'Chain', formatted_value: 'Rose Gold chain', property_id: 515 }); ok('any other option that names a metal still comes first', key(t) === 'rose'); }
{ const t = tx(1234567, 'x'); t.variations = [{ formatted_name: 'Metal', formatted_value: 'Silver' }]; ok('a listing with a Metal choice reads as before', key(t) === 'silver'); }
console.log(`${failed ? 'FAIL' : 'PASS'} listing-metal (${n - failed} of ${n})`); process.exit(failed ? 1 : 0);
