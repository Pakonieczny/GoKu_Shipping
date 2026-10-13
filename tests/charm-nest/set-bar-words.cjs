// SETBAR integration (Paul, 13 Oct 2026, order 4202291826): the set rule (SETBAR-A) + the personalisation reader (SETBAR-C) + the bar-on-chain rule meet in ONE place: the reader's slotInfo hook, wired in
// charm-nest-bridge.js. "The thinner bar always sits on the shorter chain unless the user special requests a different layout."
// The buyer's words: 16"= 33*0'45" N 96*45'53" W (the asterisk is supposed to be the little degree circle-I just don't have that on my keyboard) 18"= Ginger
// Expected: the thinner small bar (SKINNY_0191, engraving slot D2) hangs on the 16" chain and is engraved with the coordinates (degree signs); the thicker large bar (BAR_1548, slot D1) hangs on the 18" chain
// and is engraved "Ginger". A different layout asked for = nothing is assigned (the person sees every wording).
// Offline only: the real intake (charm-nest-orders.js), the real reader (charm-nest-engrave-read.js), the real wiring text taken from the bridge source. node tests/charm-nest/set-bar-words.cjs
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = process.env.SETBAR_ROOT ? path.resolve(process.env.SETBAR_ROOT) : path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js')), R = require(path.join(root, 'charm-nest-engrave-read.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a === undefined ? null : a)), b, m); n++; };

const guard = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/huggie-guard-world.json'), 'utf8'));
const masters = ['BAR_1548', 'SKINNY_0191'].map(s => Object.assign({ holes: 2 }, guard.master.find(e => e.sku === s)));
const m = new Map(masters.map(e => [e.sku.toUpperCase(), e])), loose = new Map(); for (const k of m.keys()) loose.set(O.looseKey(k), k);
const ctx = { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: s => m.get(String(s || '').toUpperCase()) || null, masterLoose: k => (loose.has(k) ? m.get(loose.get(k)) : null), masterSkus: () => [...m.keys()] };
const TEXT = '16"= 33*0\'45" N 96*45\'53" W (the asterisk is supposed to be the little degree circle-I just don\'t have that on my keyboard) 18"= Ginger';
const V = (name, value) => ({ name, value });
const line = {
  transactionId: '5300000001', listingId: '532401548', sku: 'BAR_1548', title: 'Bar Necklace SET of 2, Gold, Rose Gold, Silver, Custom Gold Bar Set, Engraved Necklace, Customized Name Bar Set, Personalized Bar Necklace',
  quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: [TEXT],
  variations: [V('Metal &middot; #Characters?', 'GOLD·Set·ENGRAVED'), V('Necklace Length&middot;FONT', '16"&18"·Set·ANGELINA'), V('Personalization', TEXT)]
};
const spec = O.interpretLine({ receiptId: '4999999999', updateTs: 1791826702 }, line, ctx);
const row = { spec, line };

// the wiring, exactly as the bridge has it (taken from the source, not re-typed)
const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const mm = /R\.slotInfo = \(row, slot\) => \{[\s\S]*?\n    \};/.exec(src);
ok(mm, 'charm-nest-bridge.js wires CharmNestEngraveRead.slotInfo to the set piece model (the base commit does not)');
const lineRowOf = r => (r && r.parentRow) || r;
const wire = new Function('O', 'lineRowOf', 'R', mm[0] + '\nreturn R.slotInfo;');

const R2 = R; R2.slotInfo = null;
R2.slotInfo = wire(O, lineRowOf, R2);   // (the wiring sets it on the module it is given: the reader's own hook)
const jobs = [{ slot: 'D1' }, { slot: 'D2' }];

// 1 · the set is two pieces: D1 the large thicker BAR_1548, D2 the small thinner SKINNY_0191
const pcs = O.setPiecesOf(row);
eq(pcs.map(p => [p.slotKey, p.sku, p.thickness, p.length]), [['D1', 'BAR_1548', 'thicker', '18"'], ['D2', 'SKINNY_0191', 'thinner', '16"']], 'D1 = large bar on the longer chain, D2 = small bar on the shorter chain');

// 2 · the reader's slots come from that rule: 18 for D1, 16 for D2
const slots = R2.slotsOf(row, jobs);
eq(slots, [{ slot: 'D1', length: 18, rank: null }, { slot: 'D2', length: 16, rank: null }], 'the hook gives each piece its chain length');

// 3 · the buyer's words, read, then joined: D2 (16") the coordinates with degree signs, D1 (18") Ginger
const reading = R2.readPersonalisation(TEXT, { lengths: R2.lengthsOfLine(line) });
const res = R2.assign(reading, slots);
ok(res.ok, 'every piece has words of its own: ' + res.why);
eq(res.bySlot.D2.words, '33°0\'45" N 96°45\'53" W', 'the thinner bar on the 16" chain is engraved with the coordinates, the asterisks read as degree signs, the note to the shop left out');
eq(res.bySlot.D1.words, 'Ginger', 'the thicker bar on the 18" chain is engraved Ginger');
ok(!/asterisk|keyboard/i.test(JSON.stringify(res.bySlot)), 'the explanation to the shop is never engraved');

// 4 · a buyer who asked for ANOTHER layout: nothing is assigned, the person sees every wording
const asked = JSON.parse(JSON.stringify(spec)); asked.pair.set.layout = { asked: true, answered: false, words: ['swap the bars'] };
const rowAsked = { spec: asked, line };
eq(R2.slotsOf(rowAsked, jobs), null, 'a different layout asked for and not looked at: no chain is assumed');
const res2 = R2.assign(reading, R2.slotsOf(rowAsked, jobs));
ok(!res2.ok && Object.keys(res2.bySlot).length === 0, 'nothing is given to a bar by a guess: ' + res2.why);

// 5 · a line that is no set: no mapping
const plain = O.interpretLine({ receiptId: '4999999998', updateTs: 1791826702 }, Object.assign({}, line, { listingId: '111', sku: 'BAR_1548' }), ctx);
eq(R2.slotsOf({ spec: plain, line: Object.assign({}, line, { listingId: '111' }) }, jobs), null, 'another listing is no set: the hook says nothing');

console.log('set-bar-words: ' + n + ' checks passed');
