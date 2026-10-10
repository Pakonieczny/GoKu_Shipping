// ZODIACTWO (Paul, 10 Oct 2026, 16:17 UTC, order 4175402612, ZODIAC_EARRINGS-6, Stud earrings, "Metal Choice :: Silver • 2 symbols", Zodiac Sign Libra:
// "I don't understand the problem here."): a line that says N symbols on a zodiac listing reads the OTHER sign from the buyer's note, deterministically.
//
//  what the data showed:
//   · the buyer's note of order 4175402612 is "balance et lion" (French: Libra and Leo). OPTTWO already read it as the signs Libra and Leo, but the CHARM of a sign
//     named in words had only two sources: a person's saved answer for that option value (nobody can give one any more) or the SKU Etsy ties to the value the buyer
//     bought. Leo was never bought, so the row asked "Zodiac Sign: Leo not mapped" for ever;
//   · Etsy's inventory knows it: each value of the Zodiac Sign drop-down has its own SKU (Leo = Zodiac_Earrings-4). But the stored table kept value ids only, never
//     Etsy's text for them, so no sign could be looked up by name. The table now keeps the names (the same one GET), and a sign is the one value whose text names it
//     (the same closed list of twelve signs the note is read with), its SKU the one SKU of that value and of no other value.
//   · a note that names the same sign as the drop-down means both ears are that sign (no question); a note that names none, or more signs than the option's
//     count, still waits, and the row now says what the buyer's note says.
//
//   node tests/charm-nest/zodiac-two.cjs      Offline: no browser, no network, no Etsy call, nothing live, no AI.
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const root = path.join(__dirname, '../..'), fnDir = path.join(root, 'netlify/functions');
const O = require(path.join(root, 'charm-nest-orders.js'));
const T = require(path.join(fnDir, '_charmNestListingSkus.js'));
const refuseNestedArrays = require('./_noNestedArrays.cjs');
const DATA = JSON.parse(fs.readFileSync(path.join(__dirname, 'options-link-data.json'), 'utf8'));   // the real Etsy tables of 10 listings (value ids and SKUs, no buyer data) and 130 real master SKUs
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(JSON.parse(JSON.stringify(a)), b, m); n++; };
const pass = name => console.log('  ✓', name);
const clone = x => JSON.parse(JSON.stringify(x));

/* ── the world: the real master SKUs, the real table of the Zodiac REVAMP stud listing, Etsy's inventory in Etsy's own shape ───────────────────────────────── */
const SIGNS = ['Aries', 'Taurus', 'Gemini', 'Cancer', 'Leo', 'Virgo', 'Libra', 'Scorpio', 'Sagittarius', 'Capricorn', 'Aquarius', 'Pisces'];   // ZODIAC_EARRINGS-0..11 (the twelve master drawings, looked at: Aries 0 … Pisces 11)
const LID = '1706155793', REAL_TABLE = DATA.tables[LID];
const lib = new Map(DATA.master.map(s => [s.toUpperCase(), {}])), loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: s => lib.get(String(s || '').toUpperCase()) || null, masterLoose: s => loose[O.looseKey(s)] || '' }, extra);
// Etsy's getListingInventory shape (v3): products[{ product_id, sku, is_deleted, property_values[{ property_id, property_name, scale_id, scale_name, value_ids[], values[] }] }].
// REAL_NAMES: Etsy's own text for every value of this listing's two options, read ONCE from Etsy on 10 Oct 2026 (the one GET this change spends; no buyer data). Aries … Pisces are
// the Zodiac Sign values with SKUs Zodiac_Earrings-0 … -11 (the twelve master drawings agree), and the 13th value, with no SKU, is called "2 symbols-leave note".
const REAL_NAMES = {"props":{"513":"Metal Choice :","514":"Zodiac Sign"},"names":{"513:108315083144":"Silver - Pair","513:110477679703":"Gold - Pair","513:116562319191":"Silver - Single","513:116562328355":"Gold - Single","513:1403828246974":"Gold \u2022 2 symbols","513:1403828247690":"RoseGold \u2022 1 symbol","513:1403828248908":"Silver \u2022 1 symbol","513:1403828249254":"Silver \u2022 2 symbols","513:1404106460519":"Gold \u2022 1 symbol","513:1404106461785":"RoseGold \u2022 2 symbols","513:931064598856":"RoseGold - Single","513:952730170907":"RoseGold - Pair","514:104130158864":"Capricorn","514:104130158888":"Taurus","514:104130158892":"Gemini","514:104130158906":"Virgo","514:104130158924":"Sagittarius","514:107267507837":"Aquarius","514:107267507841":"Pisces","514:107267507851":"Aries","514:107267507863":"Leo","514:107267507881":"Libra","514:107267507889":"Scorpio","514:1403828245856":"2 symbols-leave note","514:71051789161":"Cancer"}};
function etsyInventory(table, o) {
  o = o || {};
  return { count: table.products.length, products: table.products.map(p => ({ product_id: +p.id, sku: p.sku, is_deleted: !!p.d, offerings: [], property_values: p.pv.map(([pid, vid]) => Object.assign({ property_id: +pid, property_name: REAL_NAMES.props[pid], scale_id: null, scale_name: null, value_ids: [+vid] }, o.noNames ? {} : { values: [REAL_NAMES.names[pid + ':' + vid]] })) })) };
}
const NOW = 1791598245784;
const TABLE = T.compact(etsyInventory(REAL_TABLE), NOW);   // the table the page holds after the cloud's one GET with the names
const NAMELESS = clone(REAL_TABLE);                         // the table as it was stored on 10 Oct (ids and SKUs only)

/* ── the real line of order 4175402612 (the Sep 17 snapshot: title, SKU, ids, options, the note; no buyer data) ───────────────────────────────────────────── */
const V = (name, value, ids) => Object.assign({ name, value }, ids ? { propertyId: ids[0], valueId: ids[1] } : {});
const REAL = { order: { receiptId: '4175402612', updateTs: 1 }, line: { transactionId: '5218016559', listingId: LID, productId: '26814960626', sku: 'Zodiac REVAMP', title: 'Pisces Zodiac Stud Earrings, Astrological Symbol Earrings, Zodiac Jewelry, 14k Gold Filled Sterling Silver Earrings, Zodiac Sign Earrings', quantity: 2, metalKey: 'silver', metalLabel: 'Silver',
  personalization: ['balance et lion'], buyerMessage: '', variations: [V('Metal Choice :', 'Silver • 2 symbols', ['513', '1403828249254']), V('Zodiac Sign', 'Libra', ['514', '107267507881']), V('Personalization', 'balance et lion')] } };
const read = (over, c) => O.interpretLine(REAL.order, Object.assign({}, REAL.line, over || {}), c || ctx({ listingSkus: { [LID]: TABLE } }));
const noteLine = (note, sign, extra) => Object.assign({ personalization: note ? [note] : [], variations: [V('Metal Choice :', 'Silver • 2 symbols', ['513', '1403828249254']), V('Zodiac Sign', sign || 'Libra', ['514', sign ? ({ Libra: '107267507881', Pisces: '107267507841', Leo: '107267507863', Aries: '107267507851' })[sign] : '107267507881'])].concat(note ? [V('Personalization', note)] : []) }, extra || {});
const members = sp => (sp.pair.members || []).map(m => m.side + ':' + m.sku);
const open = sp => sp.problems.filter(p => p.kind === 'needsMapping');
const text = p => (p.pairSecond ? p.pairSecond.why : (p.note || '') + (p.why ? ' — ' + p.why : ''));

/* ── 1 · the table keeps Etsy's own text for each value, packed for Firestore, read back as it was ─────────────────────────────────────────────────────────── */
{ eq(Object.keys(TABLE).sort(), ['at', 'n', 'names', 'products', 'props'], 'a table of options now carries names and props');
  eq([TABLE.names['514:107267507881'], TABLE.names['514:107267507863'], TABLE.names['514:107267507841'], TABLE.props['514'], TABLE.props['513']], ['Libra', 'Leo', 'Pisces', 'Zodiac Sign', 'Metal Choice :'], 'Libra, Leo and Pisces are named, with the option\'s own name');
  eq(Object.keys(TABLE.names).filter(k => k.startsWith('514:')).length, 13, 'all 13 values of the drop-down are named (12 signs and the unnamed 13th)');
  const stored = refuseNestedArrays(T.toStored(TABLE), 'Charm_Listing_Skus/' + LID);
  ok(Array.isArray(stored.nm) && stored.nm.every(x => typeof x === 'string') && !('names' in stored) && !('props' in stored), 'stored as lists of strings, no map of names and no array in an array');
  eq(T.fromStored(clone(stored)), clone(TABLE), 'read back exactly (names and props, the pairs as pairs)');
  eq(T.fromStored(clone(REAL_TABLE)), clone(REAL_TABLE), 'a table stored before the names read back as it always did (no names key)');
  const none = T.compact(etsyInventory(REAL_TABLE, { noNames: true }), NOW); eq([none.names, none.props], [{}, { '514': 'Zodiac Sign', '513': 'Metal Choice :' }], 'Etsy gave no value texts: names is {} (asked for once, never again)');
  eq(T.fromStored(T.toStored(none)).names, {}, 'an empty names list survives the round trip');
  const uni = T.compact({ products: [{ product_id: 1, sku: 'A', property_values: [{ property_id: 1, value_ids: [2], values: ['x'] }] }, { product_id: 2, sku: 'A', property_values: [] }] }, NOW); ok(uni.uni === 'A' && !uni.names, 'a listing with one SKU for every product keeps no names'); }
pass('the stored table keeps Etsy\'s value names (packed as strings, read back as a map) and older tables read as before');

/* ── 2 · order 4175402612, the real line, the real table, the real master names ───────────────────────────────────────────────────────────────────────────── */
{ const sp = read();
  eq(members(sp), ['L:ZODIAC_EARRINGS-6', 'R:ZODIAC_EARRINGS-4'], 'Libra (the drop-down, the Left ear) and Leo (the note\'s second sign, "lion", the Right ear), each by Etsy\'s own SKU for that value');
  eq([sp.pair.source, sp.pair.mismatched, sp.pair.earring, sp.problems.length, sp.pair.second], ['signs', true, true, 0, null], 'no question of any kind');
  eq([sp.designSku, sp.pieceCount, sp.pair.sides.join(''), sp.pair.kind], ['ZODIAC_EARRINGS-6', 4, 'LRLR', 'multi'], 'quantity 2 is 4 pieces: a Left Libra and a Right Leo, twice');
  eq(O.piecesOf(sp).map(p => p.bodyIndex), [0, 1, 0, 1], 'the Left is the first design (body 0), the Right the second (body 1)');
  const one = read({ quantity: 1 }); eq([one.pieceCount, one.pair.kind, one.pair.sides.join('')], [2, 'mismatched', 'LR'], 'quantity 1: two pieces, a Left and a Right');
  const nm = read({ title: 'Aries Zodiac Stud Earrings' }); eq(members(nm), ['L:ZODIAC_EARRINGS-6', 'R:ZODIAC_EARRINGS-4'], 'the title\'s sign (Pisces, Aries…) is never read: only the options and the buyer\'s note');
  const viaTx = read({ sku: 'Zodiac_Earrings-6' }); eq(members(viaTx), ['L:ZODIAC_EARRINGS-6', 'R:ZODIAC_EARRINGS-4'], 'a transaction that already carries the option\'s own SKU reads the same');
  const noIds = read({ productId: '', variations: REAL.line.variations.map(v => ({ name: v.name, value: v.value })) }); eq(members(noIds), ['L:ZODIAC_EARRINGS-6', 'R:ZODIAC_EARRINGS-4'], 'a line restored from a record (no product or value ids) finds both signs by name too'); }
pass('order 4175402612 reads as a Left Libra (ZODIAC_EARRINGS-6) and a Right Leo (ZODIAC_EARRINGS-4), no question');

/* ── 3 · what the buyer's note may say ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
{ const pair = (note, sign) => { const sp = read(noteLine(note, sign)); return members(sp).join(' '); };
  eq(pair('Leo', 'Libra'), 'L:ZODIAC_EARRINGS-6 R:ZODIAC_EARRINGS-4', 'the note names exactly one other sign (English): the pair');
  eq(pair('lion', 'Libra'), 'L:ZODIAC_EARRINGS-6 R:ZODIAC_EARRINGS-4', '"lion" alone');
  eq(pair('  LEO!! ', 'Libra'), 'L:ZODIAC_EARRINGS-6 R:ZODIAC_EARRINGS-4', 'capitals and punctuation do not matter');
  eq(pair('Please Aries for the second one, thank you', 'Libra'), 'L:ZODIAC_EARRINGS-6 R:ZODIAC_EARRINGS-0', 'a sentence: the one sign word in it');
  eq(pair('Libra and Leo', 'Libra'), 'L:ZODIAC_EARRINGS-6 R:ZODIAC_EARRINGS-4', 'the drop-down\'s sign repeated in the note is not a second sign');
}
{ // two other signs besides the drop-down's: 3 signs for a 2-symbols line: not guessed, and the row says what the note says
  const sp = read(noteLine('balance et lion', 'Pisces')), q = sp.problems.find(p => p.pairSecond);
  ok(!sp.pair.mismatched && q && /names 3 signs \(Pisces, Libra, Leo\)/.test(q.pairSecond.why) && /the buyer's note says “balance et lion”/.test(q.pairSecond.why), 'more signs than the 2 symbols: it waits and says what the note says: ' + (q && q.pairSecond.why));
  const sp2 = read(noteLine('Leo and Aries', 'Libra')), q2 = sp2.problems.find(p => p.pairSecond);
  ok(!sp2.pair.mismatched && /names 3 signs/.test(q2.pairSecond.why) && /“Leo and Aries”/.test(q2.pairSecond.why), 'drop-down + two named in the note: waits, note shown');
  const none = read(noteLine('please gift wrap', 'Libra')), q3 = none.problems.find(p => p.pairSecond);
  ok(!none.pair.mismatched && /names one \(Libra\)/.test(q3.pairSecond.why) && /the buyer's note says “please gift wrap”/.test(q3.pairSecond.why), 'a note that names no sign: waits, and shows the note: ' + q3.pairSecond.why);
  const empty = read(noteLine('', 'Libra')), q4 = empty.problems.find(p => p.pairSecond);
  ok(!empty.pair.mismatched && /there is no buyer's note/.test(q4.pairSecond.why), 'no note at all: waits, and says there is none: ' + q4.pairSecond.why);
  eq([none.pieceCount, empty.pieceCount], [4, 4], 'a waiting line still counts its pair (2 units, 4 pieces)'); }
{ // the note names the SAME sign as the drop-down: both ears are that sign, a Left and a Right (the Right the mirror), nothing asked
  for (const note of ['balance', 'Libra', 'libra libra', 'Waage']) {
    const sp = read(noteLine(note, 'Libra'));
    eq([sp.pair.second.answered, sp.pair.second.by, sp.pair.second.sign, sp.problems.length, sp.pair.mismatched, sp.pair.sides.join(''), sp.pieceCount, sp.designSku], ['same', 'note', 'Libra', 0, false, 'LRLR', 4, 'ZODIAC_EARRINGS-6'], `note "${note}": the same sign on both ears, nothing asked`);
  }
  const sp = read(noteLine('balance', 'Libra')); ok(/repeats Libra/.test(sp.pair.notes.join(' ')), 'and the line says why: ' + sp.pair.notes.join(' | ')); }
pass('one other sign makes the pair; the same sign again means both ears; none, or more than the option\'s count, waits and shows the note');

/* ── 3b · the listing's 13th Zodiac Sign value, "2 symbols-leave note" (its real name, read from Etsy on 10 Oct): the buyer writes BOTH signs in the note ─────────────────────────────────── */
{ const leave = (note, extra) => read(Object.assign({ productId: '26814959766', personalization: note ? [note] : [], variations: [V('Metal Choice :', 'Silver • 2 symbols', ['513', '1403828249254']), V('Zodiac Sign', '2 symbols-leave note', ['514', '1403828245856'])].concat(note ? [V('Personalization', note)] : []) }, extra || {}));
  const m = sp => members(sp).join(' ');
  for (const [note, want] of [['Leo and Aries', 'L:ZODIAC_EARRINGS-4 R:ZODIAC_EARRINGS-0'], ['balance et lion', 'L:ZODIAC_EARRINGS-6 R:ZODIAC_EARRINGS-4'], ['Pisces / Gemini', 'L:ZODIAC_EARRINGS-11 R:ZODIAC_EARRINGS-2'], ['1) Scorpio 2) Taurus please', 'L:ZODIAC_EARRINGS-7 R:ZODIAC_EARRINGS-1']]) {
    const sp = leave(note); eq([m(sp), sp.problems.length, sp.pair.source, sp.pieceCount, sp.pair.sides.join('')], [want, 0, 'signs', 4, 'LRLR'], `"2 symbols-leave note" + note "${note}": the two signs in the order named, a Left and a Right, nothing asked`);
    ok(sp.options.some(o => o.value === '2 symbols-leave note' && o.mapped && o.mapped.source === 'rule:signs-in-note'), 'the drop-down value itself is answered by them'); }
  const one = leave('Leo'), q1 = one.problems.find(p => p.pairSecond); ok(!one.pair.mismatched && q1 && /names one \(Leo\)/.test(q1.pairSecond.why) && /the buyer's note says “Leo”/.test(q1.pairSecond.why), 'one sign in the note: the line says 2 symbols, so it waits and shows the note: ' + (q1 && q1.pairSecond.why));
  ok(!leave('leo leo').pair.mismatched && !leave('leo leo').pair.second.answered, 'the same sign twice is not guessed to mean both ears when the drop-down names none');
  const none = leave(''), q0 = none.problems.find(p => p.pairSecond); ok(!none.pair.mismatched && /no sign is named/.test(q0.pairSecond.why) && /there is no buyer's note/.test(q0.pairSecond.why), 'no note: waits and says there is none: ' + q0.pairSecond.why);
  const three = leave('Leo Libra Aries'), q3 = three.problems.find(p => p.pairSecond); ok(!three.pair.mismatched && /names 3 signs/.test(q3.pairSecond.why) && /“Leo Libra Aries”/.test(q3.pairSecond.why), 'three signs for 2 symbols: waits, note shown');
  const old = read(Object.assign({ productId: '26814959766', personalization: ['Leo and Aries'], variations: [V('Metal Choice :', 'Silver • 2 symbols', ['513', '1403828249254']), V('Zodiac Sign', '2 symbols-leave note', ['514', '1403828245856']), V('Personalization', 'Leo and Aries')] }), ctx({ listingSkus: { [LID]: NAMELESS } }));
  ok(!old.pair.mismatched && old.pair.second.needNames === true && /not loaded yet/.test(old.problems.map(text).join(' ')), 'without the names it waits, says why, and asks for them');
  const metal = read({ title: 'Zodiac Stud Earrings', personalization: ['Leo and Aries'], variations: [V('Metal Choice :', 'Silver • 2 symbols', ['513', '1403828249254']), V('Personalization', 'Leo and Aries')] }); ok(!metal.pair.mismatched, 'a note with two signs and no Zodiac Sign option at all is not read (only a drop-down value that says 2 symbols leaves the signs to the note)'); }
pass('"2 symbols-leave note": the note\'s two signs, in the order named, make the pair; anything else waits and shows the note');

/* ── 4 · exact words only: the twelve names, whole words, never fuzzy, never the title ────────────────────────────────────────────────────────────────────── */
{ const names = t => O.signsOfText(t).join(',');
  eq(names('Leo'), 'Leo', 'Leo'); eq(names('Sagittarius Capricorn Aquarius Pisces'), 'Sagittarius,Capricorn,Aquarius,Pisces', 'four more'); eq(names('Scorpio'), 'Scorpio', 'Scorpio');
  for (const w of ['Lionel', 'Leonardo', 'Leopard', 'Libre', 'Scorpius', 'Sagitarius', 'Aquarus', 'Pisceses', 'Virgin', 'Cancel', 'Taurusx', 'lio', 'Gem', 'Ari', 'zodiac']) eq(names(w), '', `"${w}" is no sign (whole words of the closed list only)`);
  eq(names('Leo-Libra'), 'Leo,Libra', 'a hyphen is a word break'); eq(names('Löwe'), 'Leo', 'the shop\'s languages: Löwe is Leo'); eq(names('Zodiac Leo ♌'), 'Leo', 'a symbol next to the name');
  const ln = O.signsOfLine(REAL.line); eq([ln.signs.map(x => x.sign + ':' + x.from), ln.propertyId, ln.noteText], [['Libra:option', 'Leo:note'], '514', 'balance et lion'], 'signsOfLine: Libra from the option, Leo from the note, the sign option\'s property id, the note\'s own words'); }
pass('only the twelve exact names (whole words, any case) are signs; look-alikes are not');

/* ── 5 · what stops the lookup is told in one plain line, and nothing is invented ──────────────────────────────────────────────────────────────────────────── */
{ const waits = (table, over) => read(over || {}, ctx({ listingSkus: table === undefined ? {} : { [LID]: table } }));
  const none = waits(undefined), leoQ0 = open(none).find(p => p.optionValue === 'Leo');
  ok(!none.pair.mismatched && leoQ0 && /not loaded yet/.test(leoQ0.why), 'no table on the page yet: Leo waits ("' + leoQ0.why + '")'); eq(none.pair.second.needNames, false, 'and asks for nothing special (the table is asked for as always)');
  const old = waits(NAMELESS), leoQ = open(old).find(p => p.optionValue === 'Leo');
  ok(!old.pair.mismatched && /names for this listing's “Zodiac Sign” values are not loaded yet/.test(leoQ.why) && /balance et lion/.test(leoQ.note), 'the table stored on 10 Oct (no names): Leo waits, says why, and shows the note: ' + text(leoQ));
  eq(old.pair.second.needNames, true, 'and the line asks for the names (spec.pair.second.needNames: the page asks the cloud once more)');
  eq(open(old).map(p => p.optionValue), ['Leo'], 'Libra is not asked about: its own SKU is tied to it');
  const noLeo = clone(TABLE); for (const k of Object.keys(noLeo.names)) if (noLeo.names[k] === 'Leo') noLeo.names[k] = 'Lion';   // (a listing whose value is not a sign's name at all; "Lion" IS in the closed list, so make it plain)
  const plain = clone(TABLE); for (const k of Object.keys(plain.names)) if (plain.names[k] === 'Leo') plain.names[k] = 'Roar';
  const r1 = waits(plain), q1 = open(r1).find(p => p.optionValue === 'Leo'); ok(!r1.pair.mismatched && /no value of “Zodiac Sign” on this listing is named Leo/.test(q1.why), 'no value of the option is named Leo: ' + q1.why);
  eq(members(waits(noLeo)).join(' '), 'L:ZODIAC_EARRINGS-6 R:ZODIAC_EARRINGS-4', '"Lion" as Etsy\'s own text for the value is Leo too (the same closed list)');
  const twice = clone(TABLE); const some = Object.keys(twice.names).find(k => /^514:/.test(k) && twice.names[k] === 'Aries'); twice.names[some] = 'Leo';   // (two values both named Leo)
  const r2 = waits(twice); ok(!r2.pair.mismatched && /more than one value of “Zodiac Sign” on this listing is named Leo/.test(open(r2).find(p => p.optionValue === 'Leo').why), 'two values named Leo: not guessed');
  const shared = clone(TABLE); const leoV = Object.keys(shared.names).find(k => shared.names[k] === 'Leo').slice(4);
  for (const p of shared.products) if (p.pv.some(a => a[0] === '514' && a[1] !== leoV) && /-5$/.test(p.sku)) { p.sku = 'Zodiac_Earrings-4'; }   // (another value's products given Leo's SKU)
  const r3 = waits(shared); ok(!r3.pair.mismatched && /shared by other values/.test(open(r3).find(p => p.optionValue === 'Leo').why), 'a SKU Etsy also gives another value says nothing about Leo: waits');
  const blank = clone(TABLE); for (const p of blank.products) if (p.pv.some(a => a[1] === leoV)) p.sku = '';
  const r4 = waits(blank); ok(!r4.pair.mismatched && /keeps no SKU on the Leo value/.test(open(r4).find(p => p.optionValue === 'Leo').why), 'Leo with no SKU on Etsy: waits');
  const unk = clone(TABLE); for (const p of unk.products) if (p.pv.some(a => a[1] === leoV)) p.sku = 'Zodiac_Earrings-99';
  const r5 = waits(unk); ok(!r5.pair.mismatched && /Zodiac_Earrings-99|ZODIAC_EARRINGS-99/i.test(open(r5).find(p => p.optionValue === 'Leo').why) && /not in any master file/.test(open(r5).find(p => p.optionValue === 'Leo').why), 'a SKU no master file holds: waits, never a look-alike');
  const neck = clone(TABLE); for (const p of neck.products) if (p.pv.some(a => a[1] === leoV)) p.sku = 'ZODIAC_NECKLACE_CHARM-4';
  const r6 = waits(neck); ok(!r6.pair.mismatched && /is a necklace design and this line is earrings/.test(open(r6).find(p => p.optionValue === 'Leo').why), 'a necklace design on an earrings line is never the Right ear');
  const sameSku = clone(TABLE); for (const p of sameSku.products) if (p.pv.some(a => a[1] === leoV)) p.sku = 'Zodiac_Earrings-6';
  const r7 = waits(sameSku); ok(!r7.pair.mismatched && !!r7.problems.length, 'two signs on one SKU are not a pair of two designs (it waits)'); }
{ // a person's saved answer for Leo stays above the inventory; Libra answered by a person too
  const lid = (name, value, map) => ({ [LID]: { [O.norm(name)]: { [O.norm(value)]: map } } });
  const sp = read({}, ctx({ listingSkus: { [LID]: TABLE }, optionMaps: lid('Zodiac Sign', 'Leo', { field: 'design', value: 'ZODIAC_EARRINGS-5' }) }));
  eq(members(sp), ['L:ZODIAC_EARRINGS-6', 'R:ZODIAC_EARRINGS-5'], 'a person\'s answer for the option value beats the inventory (as it always did)');
  const ign = read({}, ctx({ listingSkus: { [LID]: TABLE }, optionMaps: { [LID]: { 'two designs on this line': { '4175402612/5218016559': { field: 'ignore', value: null } } } } }));
  ok(!ign.pair.mismatched && ign.pair.second.answered === 'same' && !ign.pair.second.by, 'a person\'s "the same on both ears" for the line is the last word (no note reading)'); }
{ // lines that must stay exactly as they were
  const one = read({ quantity: 1, personalization: [], variations: [V('Metal Choice :', 'Silver • 1 symbol', ['513', '1403828248908']), V('Zodiac Sign', 'Pisces', ['514', '107267507841'])], productId: '26814960560' });
  eq([one.pair.says, one.pair.mismatched, one.pieceCount, one.designSku, one.problems.length], [false, false, 2, 'ZODIAC_EARRINGS-11', 0], '"1 symbol": the same sign on both ears, as before');
  const neck = read({ title: 'Zodiac Charm Necklace', variations: [V('Metal Choice :', 'Silver • 2 symbols'), V('Zodiac Sign', 'Libra'), V('Personalization', 'lion')] }); ok(!neck.pair.mismatched, 'a necklace is not an earring pair');
  const three = read({ title: 'Zodiac Stud Earrings', variations: [V('Metal Choice :', 'Silver • 3 symbols', ['513', '1']), V('Zodiac Sign', 'Libra', ['514', '107267507881']), V('Personalization', 'leo, aries')], personalization: ['leo, aries'] });
  ok(!three.pair.mismatched, '"3 symbols" is no pair of two designs'); }
pass('every way the lookup can fail is told in one line and picks nothing; a person\'s answer stays above it; the other lines read as before');

/* ── 6 · the cloud's cache: one GET fills the names, a table without them is asked again once, the sandbox never asks Etsy ────────────────────────────────── */
(async () => {
  const mk = (stored, o) => {
    const docs = new Map(Object.entries(stored || {}).map(([k, v]) => [k, clone(v)])); const calls = { get: 0, set: [] }, etsy = [];
    const refOf = (id) => ({ id, async get() { calls.get++; const d = docs.get(id); return { exists: !!d, id, data: () => (d ? clone(d) : undefined) }; }, async set(d) { refuseNestedArrays(d, id); calls.set.push(id); docs.set(id, clone(d)); } });
    const db = { collection: () => ({ doc: refOf }), async getAll(...refs) { return Promise.all(refs.map(r => r.get())); } };
    return { db, docs, calls, etsy, env: Object.assign({ db, now: NOW + 1000, fetchInventory: async id => { etsy.push(id); return etsyInventory(REAL_TABLE, o); } }, {}) };
  };
  const stored0 = { [LID]: T.toStored(Object.assign({}, clone(NAMELESS), { at: NOW - 3600000 })) };   // fresh (1 hour old), stored before the names
  { // asked WITHOUT nameIds: a fresh table is answered as before, no Etsy call
    const w = mk(stored0), r = await T.lookup([LID], w.env); eq([w.etsy.length, r.pending, r.etsyCalls, !!r.tables[LID].names], [0, [], 0, false], 'no names asked for: the fresh table is answered from the cache, no Etsy call (unchanged)'); }
  { // asked WITH nameIds (a line needs the names): one GET, the names stored, the table answered, the budget counted
    const w = mk(stored0), r = await T.lookup([LID], Object.assign(w.env, { nameIds: [LID] }));
    eq([w.etsy, r.etsyCalls, r.pending, r.cap.used], [[LID], 1, [], 1], 'asked for names: ONE Etsy GET for this one listing, counted in the day\'s budget');
    ok(r.tables[LID].names && r.tables[LID].names['514:107267507863'] === 'Leo', 'the answered table carries the names'); const doc = w.docs.get(LID); ok(Array.isArray(doc.nm) && doc.nm.length > 20 && Array.isArray(doc.pn), 'and the stored document keeps them as lists of strings (no nested arrays)');
    ok(w.calls.set.includes('_budget'), 'the day\'s budget document was written');
    const again = await T.lookup([LID], Object.assign(mk(Object.fromEntries(w.docs)).env, { nameIds: [LID] })); eq(again.etsyCalls, 0, 'asked again with the names stored: no second call'); }
  { // the sandbox (cacheOnly) never asks Etsy: the old table comes back, pending, so the page waits and tries again later
    const w = mk(stored0), r = await T.lookup([LID], Object.assign(w.env, { nameIds: [LID], cacheOnly: true }));
    eq([w.etsy.length, r.etsyCalls, r.pending, r.why, !!r.tables[LID].products, !!r.tables[LID].names], [0, 0, [LID], 'cache', true, false], 'the sandbox: no Etsy call, the stored table (without names) answered, listed as pending');
    ok(w.calls.set.length === 0, 'nothing is written'); }
  { // Etsy gave no value texts: names {} stored once; the next ask for names is a cache hit
    const w = mk(stored0, { noNames: true }), r = await T.lookup([LID], Object.assign(w.env, { nameIds: [LID] })); eq([w.etsy.length, r.tables[LID].names], [1, {}], 'no texts from Etsy: names {} stored');
    const again = await T.lookup([LID], Object.assign(mk(Object.fromEntries(w.docs)).env, { nameIds: [LID] })); eq(again.etsyCalls, 0, 'and not asked for again'); }
  { // the day's cap and a stale table still hold: 60 used = no call, the old table answered
    const w = mk(Object.assign({ _budget: { day: new Date(NOW + 1000).toISOString().slice(0, 10), n: 60, at: NOW } }, stored0)), r = await T.lookup([LID], Object.assign(w.env, { nameIds: [LID] }));
    eq([w.etsy.length, r.pending, r.why], [0, [LID], 'day'], 'the day\'s 60 are used: no call, the old table answered, pending'); }
  { // an Etsy failure is never stored and never repeated within the request
    const w = mk(stored0); w.env.fetchInventory = async id => { w.etsy.push(id); const e = new Error('Etsy 503'); e.status = 503; throw e; };
    const r = await T.lookup([LID], Object.assign(w.env, { nameIds: [LID] })); eq([w.etsy.length, r.pending, r.why, w.calls.set.filter(x => x !== '_budget').length], [1, [LID], 'error', 0], 'a failed GET: no table stored (only the day\'s count), pending, said "error"'); }
  pass('the cache asks Etsy once for the names (never in the sandbox, never past the day\'s cap), a table without names is the only one asked again');

  /* ── 7 · the page: a held line re-reads when the names arrive, and keeps its pieces ─────────────────────────────────────────────────────────────────────── */
  const src = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), a = src.indexOf('  const TABLES_LS = "cn.listingSkus.v1"'), b = src.indexOf('  async function pull(');
  assert(a > 0 && b > a, 'the SKU-table code is where the test expects it');
  const vm = require('vm');
  async function page(rows, apiAnswer) {
    const B = { maps: { listingSkus: { [LID]: clone(NAMELESS) } } }, calls = [], repooled = [], reread = [];
    const store = {}; const localStorage = { getItem: k => store[k] || null, setItem: (k, v) => { store[k] = v; } };
    const C = ctx; let current = ctx({ listingSkus: B.maps.listingSkus });
    const sandbox = { B, S: { cloud: { ok: true } }, localStorage, console, Date, JSON, Object, Array, Map, Set, Number, String, Promise, Math, setTimeout: f => { f(); return 1; }, clearTimeout() {},
      rowsOf: () => rows, api: async (fn, body) => { calls.push(body); return apiAnswer(body, B); },
      interpretAll: () => { for (const r of rows) { r.spec = O.interpretLine(r.order, r.line, Object.assign({}, current, { listingSkus: B.maps.listingSkus })); r.problems = r.spec.problems.slice(); reread.push(r.key); } },
      Review: { repool: async r => { repooled.push(r.key); r.state = 'pooled'; r.poolIds = ['x1', 'x2', 'x3', 'x4']; } } };
    vm.createContext(sandbox);
    vm.runInContext(src.slice(a, b) + '\n;this.__t = { askTables, takeTables, needsTable, needsNames, wantTables, tableTry };', sandbox);
    return Object.assign({ B, calls, repooled, reread, C }, sandbox.__t);
  }
  const rowOf = (key, extra) => { const line = Object.assign({}, REAL.line), r = Object.assign({ key, order: REAL.order, line, state: 'held', reason: 'x', poolIds: [], hold: null, spec: null, problems: [] }, extra || {}); r.spec = O.interpretLine(r.order, r.line, ctx({ listingSkus: { [LID]: NAMELESS } })); r.problems = r.spec.problems.slice(); return r; };
  { // the held line: the page asks once more, with nameIds, the cloud answers with names, the line is read again, made up again (never one with pieces or a hold)
    const held = rowOf('4175402612_5218016559'), pooled = rowOf('4175402612_5218016559b', { state: 'pooled', poolIds: ['p1', 'p2'] }), onHold = rowOf('4175402612_5218016559c', { hold: { by: 'x' } });
    ok(held.problems.length === 1 && held.spec.pair.second.needNames, 'the held line asks for the names');
    const P = await page([held, pooled, onHold], (body, B) => ({ tables: { [LID]: Object.assign({}, TABLE) }, pending: [], etsyCalls: 0 }));
    await P.askTables();
    eq([P.calls.length, P.calls[0].listingIds, P.calls[0].nameIds, P.calls[0].op], [1, [LID], [LID], 'listingSkus'], 'ONE ask for the listing, naming it in nameIds (a table the page already holds is asked for again only for this)');
    ok(P.B.maps.listingSkus[LID].names && P.B.maps.listingSkus[LID].names['514:107267507863'] === 'Leo', 'the page keeps the table with the names');
    eq([held.problems.length, members(held.spec).join(' '), held.state, held.poolIds.length], [0, 'L:ZODIAC_EARRINGS-6 R:ZODIAC_EARRINGS-4', 'pooled', 4], 'the held line reads as the pair and is made up again (4 pieces)');
    eq([P.repooled], [['4175402612_5218016559']], 'only the held line is made up again: a line with pieces or on hold is never touched');
    eq([pooled.poolIds, onHold.state, onHold.poolIds], [['p1', 'p2'], 'held', []], 'its pieces and the hold stand'); }
  { // the same ask, the sandbox answering from its cache (no names yet): the page waits, tries again later, never in a loop
    const held = rowOf('4175402612_5218016559'); const P = await page([held], () => ({ tables: { [LID]: clone(NAMELESS) }, pending: [LID], why: 'cache', etsyCalls: 0 }));
    await P.askTables(); await P.askTables(); await P.askTables();
    eq(P.calls.length, 1, 'a table that is still without names: asked once, then not again before the backoff'); ok(P.tableTry.get(LID) && P.tableTry.get(LID).until > Date.now(), 'the next try is later (10 minutes, then 30…)'); eq(held.state, 'held', 'the line keeps waiting, untouched');
    // a server that answers the table without names and without pending (an older cloud): still no loop
    const P2 = await page([rowOf('k')], () => ({ tables: { [LID]: clone(NAMELESS) }, pending: [], etsyCalls: 0 })); await P2.askTables(); await P2.askTables(); eq(P2.calls.length, 1, 'an answer with no names and no pending is not asked for again at once either'); }
  { // a line that needs nothing new asks for nothing: the table the page holds is fresh and has names
    const P = await page([rowOf('k2')], () => ({ tables: {}, pending: [] })); P.B.maps.listingSkus[LID] = Object.assign({}, TABLE, { at: Date.now() }); await P.askTables(); eq(P.calls.length, 0, 'a fresh table with names: no ask'); }
  pass('the page asks the cloud once more for the names, re-reads the held line when they arrive, makes it up with no pieces lost, and never loops');

  /* ── 8 · golden: every line of the Sep 17 snapshot reads as before except the one this change is for ────────────────────────────────────────────────────── */
  // The golden file holds, for each of the 411 lines in three worlds, a hash of the line's READING (SKU, material, size, form, chain, piece count, the questions it asks and the pair it makes) as the code
  // before this change read it. It is a summary, not the whole spec, so an unrelated field another change adds to every line does not break it.
  const GOLD = path.join(__dirname, 'zodiac-two-golden.json'), SNAPS = ['/tmp/EARWORDS-data/snapshot-min.json', '/tmp/advcount-data/snapshot-min.json', '/tmp/OPTAUDIT/data/snapshot.json'].filter(p => fs.existsSync(p)), IDX = '/tmp/reindex-stage/idx-final.json';
  if (SNAPS.length && fs.existsSync(IDX) && fs.existsSync(GOLD)) {
    const gold = JSON.parse(fs.readFileSync(GOLD, 'utf8')).digests, orders = JSON.parse(fs.readFileSync(SNAPS[0], 'utf8')), idx = JSON.parse(fs.readFileSync(IDX, 'utf8')).entries;
    const ent = new Map(idx.map(e => [String(e.sku).toUpperCase(), e])), lo = new Map(); for (const k of ent.keys()) { const key = O.looseKey(k); lo.set(key, lo.has(key) ? '' : k); }
    const base = { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, masterEntry: s => ent.get(String(s || '').toUpperCase()) || null, masterLoose: s => lo.get(O.looseKey(s)) || '' };
    const withNames = clone(DATA.tables); withNames[LID] = TABLE;
    const summary = sp => ({ designSku: sp.designSku, skuSource: sp.skuSource, noDesign: !!sp.noDesign, material: sp.material, size: sp.size, form: sp.form, chain: sp.chain, pieceCount: sp.pieceCount,
      problems: sp.problems.map(p => [p.kind, p.optionName || '', p.optionValue || '', p.sku || '', p.pairSecond ? p.pairSecond.why : '']), pair: { mismatched: sp.pair.mismatched, source: sp.pair.source, members: (sp.pair.members || []).map(m => m.side + m.sku), sides: sp.pair.sides, second: sp.pair.second ? [sp.pair.second.answered, sp.pair.second.by || '', sp.pair.second.why || ''] : null } });
    const scen = { A_noTables: [{}, []], B_realTables: [DATA.tables, []], C_zodiacNames: [withNames, ['4175402612/5218016559']] };   // (no table, or the table without names: the zodiac line still waits, only its words say more; with the names: it resolves)
    for (const [name, [lt, expectChanged]] of Object.entries(scen)) {
      const want = gold[name]; let lines = 0; const changed = [];
      for (const r of orders) for (const t of r.transactions) {
        const line = { transactionId: String(t.transaction_id), listingId: String(t.listing_id), productId: String(t.product_id), sku: t.sku, title: t.title, quantity: t.quantity, metalKey: /silver/i.test(JSON.stringify(t.variations)) ? 'silver' : 'gold', metalLabel: '', personalization: t.personalization || [], buyerMessage: t.message_from_buyer || r.message_from_buyer || '',
          variations: (t.variations || []).map(v => ({ name: v.formatted_name, value: v.formatted_value, propertyId: v.property_id != null ? String(v.property_id) : '', valueId: v.value_id ? String(v.value_id) : '' })) };
        const sp = O.interpretLine({ receiptId: String(r.receipt_id), updateTs: 1 }, line, Object.assign({}, base, { listingSkus: lt })), key = r.receipt_id + '/' + t.transaction_id; lines++;
        if (crypto.createHash('sha1').update(JSON.stringify(summary(sp))).digest('hex').slice(0, 12) !== want[key]) changed.push(key);
      }
      eq([lines, changed], [Object.keys(want).length, expectChanged], `${name}: all ${lines} lines of the snapshot read as before the change` + (expectChanged.length ? ', except order 4175402612' : ''));
    }
    pass('golden: 411 lines x 3 worlds (no tables, the 10 real tables, the zodiac table with names): only order 4175402612 changes, and only when the names are there');
  } else console.log('  · (the Sep 17 snapshot, the master index or the golden file is not on this machine: the golden part was not run)');
  /* ── 9 · the page asks for the new files with a new cache token (other workers add suffixes after it) ───────────────────────────────────────────────────── */
  { const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    for (const f of ['charm-nest-orders.js', 'charm-nest-bridge.js']) ok(new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-zt\\d(?:-[a-z0-9]+)*"').test(html), f + ' carries the -zt cache token');
    ok(/names?:\s*true|nameIds/.test(fs.readFileSync(path.join(fnDir, 'charmNestLibrary.js'), 'utf8')), 'the library op passes nameIds on'); }
  pass('cache tokens (-zt) and the library op\'s nameIds');
  console.log(`zodiac-two: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
