// BARS (Paul, 19 Oct 2026, order 4204710975): a PROFILE for Etsy listing 982791277, "Skinny MINI Vertical Bar Necklace with Heart" (SKU VERTICAL BAR_1277): 1 to 5 separate bars on ONE necklace,
// one engraving for each bar, on the FRONT of the bar unless the order says otherwise. Offline: no Etsy, no live read, no write, no paid call.
//   node tests/charm-nest/bar-necklace-profile.cjs
// What it proves (charm-nest-orders.js PIECE_LISTINGS / pieceValueRead / pieceProfileOf, charm-nest-engrave-piece-words.js, charm-nest-engrave-sides.js, -faces.js, -rows.js):
//   · THE REAL ORDER of the screenshot (listing 982791277, SKU VERTICAL BAR_1277, "Engraving & # of Bars?" = "gold • 3 bars ($45.60)", "Bar names:\nSophie\nSam\nEddy") resolves with NO decision, no table of Etsy's:
//     metal Gold (GF), 3 pieces of one necklace (slots D1..D3), Sophie / Sam / Eddy to bars 1 / 2 / 3, every bar engraved on the FRONT
//   · all 15 values of the listing's drop-down (gold / rosegold / silver, 1 to 5 bars) read, and the many ways a bullet, dot, dash, number word and price can be written
//   · a value the profile cannot read (6 bars, another metal, no count, two counts, a placeholder) still asks a person, with one plain line; another listing's line with the same drop-down is not touched
//   · fewer names than bars, more names than bars, an N/A name, one name for several bars, a message beside the names: flagged for a person (never a blank bar, never another bar's name), the names read so far kept as the proposal
//   · a side the buyer chose still wins; a line bought twice is two necklaces of N bars; the words say "Bar 2 of 3" where a disc necklace says "Disc 2 of 3"
//   · a disc necklace, a plain single charm and the old readings of the pieces' words are exactly what they were; nothing in the profile reads, writes, listens or polls
const assert = require('assert');
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require('../../charm-nest-orders.js');
const P = require('../../charm-nest-pair.js');
const S = require('../../charm-nest-engrave-sides.js');
const F = require('../../charm-nest-engrave-faces.js');
const R = require('../../charm-nest-engrave-rows.js');
const PW = require('../../charm-nest-engrave-piece-words.js');
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const MASTER = new Set(['VERTICAL BAR_1277', 'INITIAL_8391', 'COMET_9427']);
const entryFor = s => (MASTER.has(String(s || '').toUpperCase()) ? { sku: s, widthPt: 73.4, heightPt: 14 } : null);
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' }, extra || {});
const V = (name, value) => ({ name, value });

// ── THE REAL ORDER (the order window and the option's drop-down of the shop owner's screenshots) ─────────────────────────────────────────────────────────────────────────────────────────
const RID = '4204710975', LISTING = '982791277', SKU = 'VERTICAL BAR_1277', OPT = 'Engraving & # of Bars?';
const TITLE = 'Skinny MINI Vertical Bar Necklace with Heart, Personalized Long Name Gold Bar Necklace, Personalized Bar Necklace, Gold Bar, Vertical Bar';
const NAMES3 = 'Bar names:\nSophie\nSam\nEddy';
const DROPDOWN = ['gold', 'rosegold', 'silver'].flatMap(m => [1, 2, 3, 4, 5].map(k => `${m} • ${k} bar${k > 1 ? 's' : ''} ($${{ gold: [33.6, 40.8, 45.6, 52.8, 60], rosegold: [33.6, 40.8, 45.6, 52.8, 60], silver: [29.6, 36.8, 44, 51.2, 58.4] }[m][k - 1].toFixed(2)})`));
const METALKEY = { gold: 'gold', rosegold: 'rose', silver: 'silver' };
const mk = (value, pers, o) => Object.assign({ transactionId: '5004710975', listingId: LISTING, productId: '12345', sku: SKU, title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: pers == null ? [NAMES3] : pers, variations: [V(OPT, value)] }, o || {});
// one line read the way the sorter reads it, its pieces, its engraving slots and its faces
function world(line, c) {
  const order = { receiptId: RID, updateTs: 0 }, spec = O.interpretLine(order, line, c || ctx());
  noNested(spec, 'spec');
  const row = { key: `${RID}_${line.transactionId}`, order, line, spec, state: 'pulled' };
  const pieces = P.piecesFor(Object.assign({}, line, { receiptId: RID, quantity: spec.quantity, form: spec.form, spec }), null);
  row.poolIds = pieces.map((p, i) => `${row.key}_${i + 1}`);
  const SC = { pair: P, entryFor, poolRow: () => null, charmOf: () => null }, plan = S.plan(SC, row, []), setup = F.setupOf(row);
  return { order, spec, row, pieces, plan, setup, SC, faceJobs: F.planFaces(setup, plan.slots) };
}
const asks = spec => (spec.problems || []).filter(p => p.kind === 'needsMapping');

// ── 1 · the real order resolves by itself ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
{ const w = world(mk('gold • 3 bars ($45.60)')), sp = w.spec;
  eq(sp.problems, [], 'no decision is needed: the option is read without Etsy\'s list ("list for listing 982791277 is not loaded yet" is gone)');
  eq([sp.designSku, sp.skuSource, sp.material, sp.materialKey, O.CARD_TAG[sp.material]], [SKU, 'transaction', 'gold', 'gold', 'GF'], 'the design is the SKU\'s, the metal is Gold (GF)');
  eq(sp.options.map(o => [o.name, o.value, o.mapped && o.mapped.field, o.mapped && o.mapped.value, o.mapped && o.mapped.source]), [[OPT, 'gold • 3 bars ($45.60)', 'count', '3', 'rule:count:profile']], 'the option is answered by the profile: a count of 3');
  eq([sp.pieceCount, sp.pair.kind, sp.pair.perUnit, sp.pair.noun, sp.pair.profile, sp.pair.earring, sp.pair.sides], [3, 'multi', 3, 'bar', SKU, false, [null, null, null]], '3 pieces of ONE necklace, no ear, no side, the noun "bar"');
  eq(O.countedPieces({ spec: sp, line: w.row.line }), { total: 3, qty: 1, noun: 'bar' }, 'a counted line of 3 bars (every surface that says how many pieces says "3 bars")');
  ok(O.countedFact({ spec: sp, line: w.row.line }) === ' · 3 bars', 'Quantity 1 · 3 bars');
  eq(w.pieces.map(p => [p.n, p.of, p.side, p.mirror, p.groupKey]), [1, 2, 3].map(k => [k, 3, null, false, RID + ':5004710975']), 'three pool pieces n of 3, one group, never mirrored');
  eq([w.plan.split, w.plan.slots, w.row.poolIds.map(id => w.plan.of.get(id))], [true, ['D1', 'D2', 'D3'], ['D1', 'D2', 'D3']], 'one engraving job slot for each bar: copy 1 is bar 1 (D1), copy 2 bar 2, copy 3 bar 3');
  eq(sp.pieceProfile.words, { D1: 'Sophie', D2: 'Sam', D3: 'Eddy' }, 'the names go to bars 1, 2, 3 in the order written');
  eq([sp.pieceProfile.ok, sp.pieceProfile.sure, sp.pieceProfile.how, sp.pieceProfile.face, sp.pieceProfile.metal], [true, true, 'lines', 'front', 'gold'], 'nothing is asked of a person');
  eq([sp.engraving.mode, sp.engraving.source, sp.engraving.needsPerson, sp.engraving.textSource], ['front', 'profile', false, 'labelled'], 'the engraving is on the FRONT of the bars by default (the profile: the order says nothing about a side)');
  eq([w.setup.mode, w.setup.faces, w.setup.needsPerson], ['front', ['front'], false], 'the faces model plans the front only');
  eq(w.faceJobs, [{ slot: 'D1', face: 'front' }, { slot: 'D2', face: 'front' }, { slot: 'D3', face: 'front' }], 'three engraving jobs, one front for each bar, no back');
  eq(w.plan.slots.map(sl => (F.seedFor(w.setup, 'front', sl) || {}).text), ['Sophie', 'Sam', 'Eddy'], 'each front job starts with ITS OWN bar\'s name (no paid reading)');
  ok(sp.engraveCandidate && !sp.engraveNever, 'it has words to engrave');
  ok(sp.pair.notes.some(t => /3 bars on one necklace/.test(t)), 'the order\'s reading says it in one line');
  // the words every surface uses for a piece
  const jobs = w.plan.slots.map(sl => ({ key: `${w.row.key}#${sl}~front`, slot: sl, face: 'front', lines: [], row: Object.assign(Object.create(w.row), { slot: sl }) }));
  eq(S.pieceRecords(jobs, { of: 3 }).map(r => [r.tag, r.label, r.index, r.of]), [['BAR 1 of 3', 'Bar 1', 1, 3], ['BAR 2 of 3', 'Bar 2', 2, 3], ['BAR 3 of 3', 'Bar 3', 3, 3]], 'the chips say "BAR 2 of 3", not "DISC 2 of 3"');
  eq([S.slotWord(jobs[1]), S.slotWord(jobs[1], true), S.pieceTag(jobs[2], 3)], ['Bar 2', 'Bar 2', 'BAR 3 of 3'], 'slotWord / pieceTag (the one wording every list, card and message uses)');
  // the Engrave tab: the three front jobs of one line are ONE row of pieces, as the discs of a necklace are
  const rows = R.group(jobs, jobs, { discs: true });
  eq([rows.length, rows[0].jobs.length, rows[0].discs, R.kindOf(rows[0])], [1, 3, true, 'discs'], 'the Engrave tab lists the three bars as one row of three pieces, Back / Next and Approve all go bar by bar');
}

// ── 1a · the order that is held today (screenshot 1: NEEDS A DECISION, "list for listing 982791277 is not loaded yet") is released by the same reading, nothing is asked again ───────────────────────
{ const w = world(mk('gold • 3 bars ($45.60)')), prev = { problems: [{ kind: 'needsMapping', optionName: OPT, optionValue: 'gold • 3 bars ($45.60)' }], designSku: SKU };
  const held = Object.assign({}, w.row, { state: 'held', reason: 'Options: option "Engraving & # of Bars?: gold • 3 bars ($45.60)" Etsy\'s SKU list for listing 982791277 is not loaded yet', problems: [], poolIds: [] });
  ok(O.questionGone(held, prev), 'a line held for that question, read clean now, goes back to the intake (questionGone)');
  ok(O.staleQuestionHold(Object.assign({}, held, { fromRecord: true })), 'and so does its stored record (the order window of an order outside the pull)');
  ok(!O.questionGone(Object.assign({}, held, { hold: 'held by Paul' }), prev) && !O.questionGone(Object.assign({}, held, { poolIds: ['x_1'] }), prev), 'a person\'s hold and pieces already made stay'); }

// ── 1b · the same listing, another listing id: still asks a person (the profile is one row of a table) ───────────────────────────────────────────────────────────────────────────────────
{ const w = world(mk('gold • 3 bars ($45.60)', null, { listingId: '982791278', sku: 'OTHER_1277' })), sp = w.spec;
  eq([asks(sp).length, sp.pieceCount, sp.pair.noun, sp.pieceProfile, sp.engraving], [1, 1, undefined, undefined, undefined], 'another listing\'s line is not read by this profile: it asks, as it did');
  ok(/not loaded yet/.test(asks(sp)[0].why), 'with Etsy\'s own words for it (list for listing 982791278 is not loaded yet)'); }
// the SKU alone (a stored line that lost its listing id) and the SKU's other spellings: the profile is found as every SKU is (case, spacing, punctuation set aside)
for (const sku of ['VERTICAL BAR_1277', 'vertical_bar_1277', 'Vertical Bar 1277', 'VERTICAL-BAR-1277']) { const w = world(mk('silver • 2 bars ($36.80)', ['A\nB'], { listingId: '', sku, metalKey: 'silver' }));
  eq([asks(w.spec).length, w.spec.pieceCount, w.spec.pair.noun], [0, 2, 'bar'], `SKU "${sku}" with no listing id is the profile's`); }
ok(O.pieceListing({ listingId: LISTING, sku: 'ANYTHING' }) === O.PIECE_LISTINGS[0] && O.pieceListing({ listing_id: LISTING }) === O.PIECE_LISTINGS[0] && O.pieceListing({ listingId: '1', sku: 'COMET_9427' }) === null && O.pieceListing(null) === null, 'pieceListing: by listing id (either spelling) or SKU, null for every other line');
// a line of the SKU on ANOTHER listing with no bars drop-down is not touched (no face default, no pieces)
{ const w = world({ transactionId: '9', listingId: '555', sku: SKU, title: TITLE, quantity: 1, metalKey: 'gold', personalization: ['Anna'], variations: [V('Metal', 'Gold')] });
  eq([w.spec.problems.length, w.spec.pieceCount, w.spec.pieceProfile, w.spec.engraving], [0, 1, undefined, undefined], 'the SKU on a line with no bars option reads exactly as before'); }

// ── 2 · every value of the listing's drop-down (screenshot: 15 values) ────────────────────────────────────────────────────────────────────────────────────────────────────────────────
eq(DROPDOWN.length, 15, 'fifteen values');
for (const v of DROPDOWN) {
  const metal = v.split(' ')[0], k = +/(\d) bar/.exec(v)[1], names = ['Sophie', 'Sam', 'Eddy', 'Mia', 'Zoe'].slice(0, k);
  const w = world(mk(v, ['Bar names:\n' + names.join('\n')], { metalKey: METALKEY[metal], metalLabel: metal }));
  eq([w.spec.problems.length, w.spec.pieceCount, w.spec.material, w.spec.pieceProfile.metal], [0, k, METALKEY[metal], METALKEY[metal]], `${v}: ${k} piece(s), ${metal}, nothing asked`);
  eq(w.plan.slots, k > 1 ? Array.from({ length: k }, (_, i) => 'D' + (i + 1)) : [null], `${v}: ${k > 1 ? 'one slot for each bar' : 'one bar, no slot'}`);
  eq(Object.values(w.spec.pieceProfile.words), names, `${v}: the names in order`);
  eq([w.spec.engraving.mode, w.spec.engraving.needsPerson, w.faceJobs.length, w.faceJobs.every(j => j.face === 'front')], ['front', false, k, true], `${v}: ${k} front engraving job(s)`);
  if (k > 1) eq(w.plan.slots.map(sl => (F.seedFor(w.setup, 'front', sl) || {}).text), names, `${v}: each front job has its own name`);
  else eq([w.spec.pair.kind, w.spec.pair.perUnit, F.seedFor(w.setup, 'front', null).text], ['single', 1, 'Sophie'], `${v}: one bar is the ordinary single piece, its one name on the front`);
}
// the metal comes from the drop-down when the station gave none (and the station's own reading stands when it has one)
for (const [v, key, tag] of [['gold • 2 bars ($40.80)', 'gold', 'GF'], ['rosegold • 2 bars ($40.80)', 'rose', 'RG'], ['silver • 2 bars ($36.80)', 'silver', 'SS']]) {
  const sp = world(mk(v, ['A\nB'], { metalKey: '', metalLabel: '' })).spec;
  eq([sp.problems.length, sp.materialKey, sp.material, O.CARD_TAG[sp.material]], [0, key, key, tag], `${v}: no station metal, the drop-down names it (${tag})`); }
eq(world(mk('gold • 2 bars ($40.80)', ['A\nB'], { metalKey: 'rose', metalLabel: 'Rose' })).spec.materialKey, 'rose', 'the station\'s own reading stands when it has one');

// ── 3 · the many ways the value may be written; and what is NOT read ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
const read = v => O.pieceValueRead(v, O.PIECE_LISTINGS[0]);
for (const [v, want] of [
  ['gold • 3 bars ($45.60)', [3, 'gold']], ['rosegold • 1 bar ($33.60)', [1, 'rose']], ['silver • 5 bars ($58.40)', [5, 'silver']],
  ['Gold . 3 bars', [3, 'gold']], ['gold · 3 bars', [3, 'gold']], ['gold - 3 bars', [3, 'gold']], ['Gold – 3 Bars', [3, 'gold']], ['gold / 3 bars', [3, 'gold']], ['gold, 3 bars', [3, 'gold']], ['gold|3 bars', [3, 'gold']], ['gold 3 bars', [3, 'gold']],
  ['3 bars • silver', [3, 'silver']], ['3 BARS - ROSEGOLD', [3, 'rose']], ['Rose Gold • 2 bars', [2, 'rose']], ['rose-gold • 2 bars', [2, 'rose']], ['gold filled • 2 bars', [2, 'gold']], ['sterling silver • 4 bars', [4, 'silver']],
  ['three bars • gold', [3, 'gold']], ['gold • Three Bars', [3, 'gold']], ['gold • 3-bars', [3, 'gold']], ['gold • 3bars', [3, 'gold']], ['gold • bars x 3', [3, 'gold']], ['gold • x3 bars', [3, 'gold']], ['gold &bull; 3 bars', [3, 'gold']], ['gold &#8226; 3 bars ($45.60)', [3, 'gold']], ['gold &middot; 3 bars', [3, 'gold']], ['gold \u2022 3 bars \u2022 gift box', null],
  ['gold • 3 bars (+$12.50)', [3, 'gold']], ['gold • 3 bars ($45.60 USD)', [3, 'gold']], ['gold • 3 bars (€45,60)', [3, 'gold']], ['  GOLD • 3 BARS  ', [3, 'gold']], ['3 bars', [3, '']]]) {
  const r = read(v);
  if (want) eq([r.ok, r.n, r.metal], [true, want[0], want[1]], `reads ${JSON.stringify(v)}`); else ok(r.ok === false, `${JSON.stringify(v)} is not read`);
}
for (const [v, why] of [['gold • 6 bars ($66.00)', /6 bars.*1 to 5/], ['gold • 0 bars', /0 bars/], ['gold • 10 bars', /10 bars.*1 to 5/], ['platinum • 3 bars', /not one of this listing's choices.*platinum/], ['gold • 2 bars • 3 bars', /more than one count/], ['gold', /1 to 5 bars/],
  ['Select an option', /not one of this listing's choices/], ['', /not one of/], ['gold • 3.5 bars', /not one of/], ['gold • 3 discs', /not one of/], ['gold • 3 bars • engraved', /engraved.*no metal/], ['14k solid gold • 3 bars', /no metal this shop sells here/]]) {
  const r = read(v); ok(r.ok === false && why.test(r.why), `${JSON.stringify(v)} is not read: ${r.why}`); }
// a value that cannot be read ASKS a person, once, with the plain line (not "Etsy's list is not loaded yet"); the line is one piece until a person says
for (const v of ['gold • 6 bars ($66.00)', 'platinum • 3 bars', 'gold', 'Select an option', 'gold • 3 bars • 2 bars']) {
  const w = world(mk(v)), a = asks(w.spec);
  eq([a.length, a[0].optionName, a[0].optionValue, w.spec.pieceCount, w.spec.pair.noun], [1, OPT, v, 1, 'bar'], `${JSON.stringify(v)}: a person is asked about the option, nothing is pooled as bars`);
  ok(a[0].why && !/not loaded yet/.test(a[0].why), `${JSON.stringify(v)}: one plain line says why (${a[0].why})`); }
// a person's own saved answer for a value is read first and wins
{ const maps = { [LISTING]: { [OPT.toLowerCase()]: { 'gold • 6 bars ($66.00)': { field: 'count', value: '6' } } } };
  const w = world(mk('gold • 6 bars ($66.00)', ['A\nB\nC\nD\nE\nF']), ctx({ optionMaps: maps }));
  eq([asks(w.spec).length, w.spec.pieceCount, w.plan.slots], [0, 6, ['D1', 'D2', 'D3', 'D4', 'D5', 'D6']], 'a person said "6 pieces" for the value: six bars, as they said');
  eq(w.spec.pieceProfile.words.D6, 'F', 'and the names follow'); }
// two drop-downs of one line that disagree are not read
{ const w = world(mk('gold • 3 bars ($45.60)', null, { variations: [V(OPT, 'gold • 3 bars ($45.60)'), V('Number of Bars', '2 bars')] }));
  eq([asks(w.spec).map(a => a.optionName), w.spec.pieceCount, w.spec.engraving.needsPerson, w.spec.engraving.reviewKind, w.spec.pieceProfile.ok], [['Number of Bars'], 1, true, 'texts', false], 'two options naming different numbers of bars (3 and 2) are never settled by guessing: a person is asked, no name is given to a bar meanwhile'); }

// the drop-down is found by its NAME (a bars count), not by any option that says bar; the raw Etsy transaction (the stations) is counted the same
{ const pl = O.PIECE_LISTINGS[0];
  for (const nm of ['Engraving & # of Bars?', 'Engraving &amp; # of Bars?', 'Number of Bars', 'Number of Bar(s)', 'Bars']) ok(pl.option.test(nm), `"${nm}" names the count`);
  for (const nm of ['Bar Length', 'Bar Size', 'Metal', 'Necklace Length']) ok(!pl.option.test(nm), `"${nm}" does not`);
  eq(world(mk('gold \u2022 3 bars', ['A\nB\nC'], { variations: [V(OPT, 'gold \u2022 3 bars'), V('Bar Length', '2 inch')] })).spec.pieceCount, 3, 'another option that says "bar" is no count');
  const raw = { transaction_id: 5004710975, listing_id: 982791277, sku: SKU, title: TITLE, quantity: 1, variations: [{ formatted_name: OPT, formatted_value: 'gold \u2022 3 bars ($45.60)' }], message_from_buyer: '' };
  eq([O.pieceCountOf(raw), O.pieceCountOf(Object.assign({}, raw, { quantity: 2 })), O.piecesOf(raw).map(p => [p.n, p.of, p.side])], [3, 6, [[1, 3, null], [2, 3, null], [3, 3, null]]], 'the stations count a raw Etsy transaction (listing_id, formatted_name / formatted_value) as three pieces, six for two necklaces'); }

// the reading is the ROW's, not the bar listing's: another noun, another range (how a second listing is added: one row of PIECE_LISTINGS)
{ const row = { id: 'TEST', noun: 'tag', min: 1, max: 3, face: 'front', option: /tags/i };
  eq([O.pieceValueRead('silver \u2022 2 tags ($10.00)', row).n, O.pieceValueRead('silver \u2022 2 tags ($10.00)', row).metal, O.pieceValueRead('3 Tags - gold', row).n, O.pieceValueRead('gold \u2022 4 tags', row).ok, O.pieceValueRead('gold \u2022 2 bars', row).ok], [2, 'silver', 3, false, false], 'a row for another noun and range reads its own values (2 tags ok, 4 tags outside 1 to 3, bars are not tags)');
  eq([O.pieceSlots(3, 1), O.pieceSlots(2, 2), O.pieceSlots(1, 2), O.pieceSlots(1, 1)], [['D1', 'D2', 'D3'], ['D1', 'D2', 'D1.2', 'D2.2'], ['C1', 'C2'], []], 'pieceSlots: the engraving slots of N pieces on one or more necklaces'); }

// ── 4 · the words of each bar ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
const bars = (value, pers, o) => world(mk(value, pers, o));
{ // the written forms, in order
  for (const [pers, k, want, how] of [
    [['Bar names:\nSophie\nSam\nEddy'], 3, ['Sophie', 'Sam', 'Eddy'], 'lines'], [['Bar names: Sophie, Sam, Eddy'], 3, ['Sophie', 'Sam', 'Eddy'], 'list'], [['Sophie\nSam\nEddy'], 3, ['Sophie', 'Sam', 'Eddy'], 'lines'],
    [['Bar names:', 'Sophie', 'Sam', 'Eddy'], 3, ['Sophie', 'Sam', 'Eddy'], 'lines'], [['Bar 1: A, Bar 2: B'], 2, ['A', 'B'], 'numbered'], [['Bar 2: B\nBar 1: A'], 2, ['A', 'B'], 'numbered'], [['Bar names:\nBar 1: A\nBar 2: B'], 2, ['A', 'B'], 'numbered'],
    [['Sophie / Sam / Eddy'], 3, ['Sophie', 'Sam', 'Eddy'], 'list'], [['Bar names: Anna Maria; Ben; Carl'], 3, ['Anna Maria', 'Ben', 'Carl'], 'list'], [['Anna on all bars'], 3, ['Anna', 'Anna', 'Anna'], 'same'], [['Bar names: Sophie & Sam'], 2, ['Sophie', 'Sam'], 'list']]) {
    const w = bars(`gold • ${k} bars`, pers), words = w.plan.slots.map(sl => w.spec.pieceProfile.words[sl]);
    eq([words, w.spec.pieceProfile.how, w.spec.pieceProfile.sure, w.spec.engraving.needsPerson], [want, how, true, false], `${JSON.stringify(pers)} for ${k} bars: ${want.join(' / ')} (${how})`); }
  // one bar, one name: the ordinary single piece
  for (const pers of [['Sophie'], ['Bar names:\nSophie'], ['Bar names: Sophie'], ['Bar 1: Sophie']]) { const w = bars('silver • 1 bar ($29.60)', pers);
    eq([w.spec.pieceProfile.words, w.spec.pieceProfile.sure, w.spec.engraving.front.text, w.spec.engraving.needsPerson, w.plan.slots], [{ '*': 'Sophie' }, true, 'Sophie', false, [null]], `one bar, ${JSON.stringify(pers)}: its one name, on the front`); }
  // fewer names than bars: the missing bar is flagged, never engraved blank; the names read so far are the proposal
  { const w = bars('gold • 3 bars ($45.60)', ['Bar names:\nSophie\nSam']), pp = w.spec.pieceProfile, e = w.spec.engraving;
    eq([pp.ok, pp.sure, e.needsPerson, e.reviewKind, e.mode, e.source], [false, false, true, 'pieces', 'front', 'profile'], 'two names for three bars: flagged for a person');
    ok(/2 names for 3 bars: bar 3 has none/.test(e.why), 'the reason says which bar has none: ' + e.why);
    eq([w.setup.needsPerson, w.setup.reviewKind], [true, 'pieces'], 'the faces model carries the question');
    eq(w.plan.slots.map(sl => (F.seedFor(w.setup, 'front', sl) || {}).text || null), ['Sophie', 'Sam', null], 'the third bar has NO words (never a blank engraving, never another bar\'s name)'); }
  // more names than bars
  { const w = bars('gold • 2 bars', ['Bar names:\nSophie\nSam\nEddy']), e = w.spec.engraving;
    eq([e.needsPerson, e.reviewKind, w.spec.pieceProfile.sure], [true, 'pieces', false], 'three names for two bars: flagged for a person');
    ok(/3 names for 2 bars \(more names than bars\)/.test(e.why), 'the reason: ' + e.why);
    eq(e.pieces, undefined, 'no name is given to a bar by a guess'); }
  for (const pers of [['Bar names: Sophie, Sam, Eddy, Zed'], ['Sophie\nSam\nEddy\nZed']]) { const w = bars('gold • 3 bars', pers); eq([w.spec.engraving.needsPerson, w.spec.pieceProfile.sure], [true, false], `${JSON.stringify(pers)} for 3 bars: more names than bars, flagged`); }
  // a name that says N/A, nothing at all, one name for several bars, a message beside the names
  { const w = bars('gold • 3 bars', ['Bar names: Sophie, N/A, Eddy']), e = w.spec.engraving;
    eq([e.needsPerson, e.reviewKind, w.plan.slots.map(sl => (F.seedFor(w.setup, 'front', sl) || {}).text || null)], [true, 'pieces', ['Sophie', null, 'Eddy']], 'an N/A name: that bar is flagged, the others keep theirs');
    ok(/bar 2 has no name/.test(e.why), e.why); }
  { const w = bars('gold • 3 bars', ['N/A']), e = w.spec.engraving; eq([e.needsPerson, e.reviewKind, e.pieces], [true, 'nowords', undefined], 'the buyer wrote N/A for a listing that is engraved: a person says'); }
  { const w = bars('gold • 3 bars', ['Sophie']), e = w.spec.engraving; eq([e.needsPerson, e.reviewKind, e.pieces], [true, 'pieces', undefined], 'one name for three bars is not copied onto all three: a person says'); }
  { const w = bars('gold • 3 bars', [NAMES3], { buyerMessage: 'Can the second bar be longer? Thanks!' }), e = w.spec.engraving;
    eq([e.needsPerson, e.reviewKind, w.plan.slots.map(sl => (F.seedFor(w.setup, 'front', sl) || {}).text)], [true, 'texts', ['Sophie', 'Sam', 'Eddy']], 'another voice on the order (a buyer message): a person confirms, the names stay as the proposal'); }
  { const w = bars('gold • 3 bars', [], { buyerMessage: 'Please engrave Sophie, Sam and Eddy' }), e = w.spec.engraving; eq([e.needsPerson, e.reviewKind], [true, 'texts'], 'words only in a message: a person says'); }
  // a font the buyer asks for in the box is the font, not a name; a request riding on the last name is never engraved with it
  { const w = bars('gold • 3 bars', ['Bar names:\nSophie\nSam\nEddy\nTypewriter font']); eq([w.plan.slots.map(sl => w.spec.pieceProfile.words[sl]), w.spec.engraving.needsPerson, w.spec.fontText && w.spec.fontText.asked], [['Sophie', 'Sam', 'Eddy'], false, 'Typewriter'], 'a "Typewriter font" line is the line\'s font, not a fourth name');
    const x = bars('gold • 3 bars', ['Bar names: Sophie, Sam, Eddy in Pristina font']); eq([x.spec.engraving.needsPerson, x.spec.engraving.reviewKind], [true, 'texts'], '"Eddy in Pristina font" is not a name: a person checks'); ok(/reads like a font request/.test(x.spec.engraving.why), x.spec.engraving.why); }
  // nothing written at all (no personalisation, no message): no engraving reading, as every listing without words
  { const w = bars('gold • 3 bars', []); eq([w.spec.engraveCandidate, w.spec.engraving, w.spec.problems.length, w.spec.pieceCount], [false, undefined, 0, 3], 'no words anywhere: three plain bars, nothing to engrave (as before)'); }
}

// ── 5 · the face ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
{ // a side the buyer chose wins exactly as on any other listing
  for (const [opt, mode] of [['Back', 'back'], ['Both', 'both'], ['Front', 'front']]) {
    const w = world(mk('gold • 3 bars ($45.60)', ['front: W\nback: Anna'], { variations: [V(OPT, 'gold • 3 bars ($45.60)'), V('Engraving Side', opt)] }), ctx({ optionMaps: { [LISTING]: { 'engraving side': { [opt.toLowerCase()]: { field: 'ignore', value: null } } } } }));
    eq([w.spec.engraving.mode, w.spec.engraving.source], [mode, 'option'], `"Engraving Side: ${opt}" is the buyer's choice: ${mode}, not the profile's front`); }
  const w = world(mk('gold • 3 bars ($45.60)', ['Bar names:\nSophie\nSam\nEddy'], { buyerMessage: '' }));
  eq(w.spec.engraving.source, 'profile', 'with nothing said the profile\'s face stands'); }
ok(O.PIECE_LISTINGS[0].face === 'front', 'the listing\'s row says front');

// ── 6 · 1, 2, 3, 4, 5 bars all work; a sixth is not the profile's ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────
for (let k = 1; k <= 5; k++) {
  const names = ['Ava', 'Ben', 'Cy', 'Dee', 'Eli'].slice(0, k), w = bars(`gold • ${k} bar${k > 1 ? 's' : ''}`, [k > 1 ? 'Bar names:\n' + names.join('\n') : 'Ava']);
  eq([w.spec.problems.length, w.spec.pieceCount, w.faceJobs.length, w.pieces.length], [0, k, k, k], `${k} bar(s): ${k} pieces, ${k} front jobs`); }
{ const w = bars('gold • 6 bars ($66.00)', ['Bar names:\n1\n2\n3\n4\n5\n6']); ok(asks(w.spec).length === 1 && w.spec.pieceCount === 1 && /1 to 5/.test(asks(w.spec)[0].why), 'a sixth bar is flagged for a person: ' + asks(w.spec)[0].why); }

// ── 7 · bought twice: two necklaces of 3 bars ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
{ const w = bars('gold • 3 bars', ['Bar names:\nA\nB\nC\nD\nE\nF'], { quantity: 2 });
  eq([w.spec.pieceCount, w.plan.slots, w.spec.pieceProfile.units], [6, ['D1', 'D2', 'D3', 'D1.2', 'D2.2', 'D3.2'], 2], 'two necklaces: six pieces, bar 1 of the second necklace is D1.2');
  eq(w.plan.slots.map(sl => (F.seedFor(w.setup, 'front', sl) || {}).text), ['A', 'B', 'C', 'D', 'E', 'F'], 'six names, in the order of the pieces');
  const w2 = bars('gold • 3 bars', [NAMES3], { quantity: 2 });
  eq([w2.spec.engraving.needsPerson, w2.spec.engraving.reviewKind], [true, 'pieces'], 'three names for two necklaces of three bars: which necklace gets which is for a person');
  const w3 = bars('gold • 1 bar', ['Ava\nBen'], { quantity: 2 }); eq([w3.plan.slots, w3.spec.pieceProfile.words], [['C1', 'C2'], { C1: 'Ava', C2: 'Ben' }], 'one bar bought twice is two plain pieces (C1, C2), each its own name'); }

// ── 8 · the pieces' own words reader: the label rules are the bars' alone ──────────────────────────────────────────────────────────────────────────────────────────────────────────
{ const t = (x, k, o) => PW.read(x, k, o);
  eq([t(['Bar names:\nSophie\nSam\nEddy'], 3).how, t(['Bar names: Sophie, Sam, Eddy'], 3).words], ['whole', ['Bar names: Sophie', 'Sam', 'Eddy']], 'without a noun every reading is what it was ("Bar names:" is a word of the list)');
  eq([t(['Names:\nAnna\nBen'], 2).words, t(['Names: Anna, Ben'], 2).words, t(['Anna'], 1).words], [['Anna', 'Ben'], ['Anna', 'Ben'], ['Anna']], 'the plain labels still work');
  eq([t(['Bar names:\nSophie\nSam'], 3, { noun: 'bar' }).split, t(['Bar names:'], 3, { noun: 'bar' }).none, t(['Bar names:\nSophie'], 1, { noun: 'bar' }).words], [false, true, ['Sophie']], 'with the noun the label is a label, never a name');
  eq([t(['Bars: Sophie, Sam, Eddy'], 3, { noun: 'bar' }).words, t(['Bars:\nSophie\nSam\nEddy'], 3, { noun: 'bar' }).words, t(['Bar: Sophie'], 1, { noun: 'bar' }).words, t(['Bar 1: Sophie'], 1, { noun: 'bar' }).words], [['Sophie', 'Sam', 'Eddy'], ['Sophie', 'Sam', 'Eddy'], ['Sophie'], ['Sophie']], '"Bars:" and "Bar:" are labels too'); }

// ── 9 · what must not change ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
{ const disc = (value, pers, o) => world({ transactionId: '77', listingId: '234758391', sku: 'INITIAL_8391', title: 'Initial Disc Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: pers, variations: [V('Number of Discs / Metal', value)] });
  const d = disc('3 discs • gold', ['Tag 1: A, Tag 2: B, Tag 3: C']);
  eq([d.spec.problems.length, d.spec.pieceCount, d.spec.pair.noun, d.spec.pair.profile, d.spec.pieceProfile, d.spec.engraving, d.plan.slots], [0, 3, undefined, undefined, undefined, undefined, ['D1', 'D2', 'D3']], 'a disc necklace: 3 pieces, discs, no profile, no face default (the back, as always)');
  eq(O.countedPieces({ spec: d.spec, line: d.row.line }), { total: 3, qty: 1, noun: 'disc' }, 'still "3 discs"');
  const jobs = d.plan.slots.map(sl => ({ key: `${d.row.key}#${sl}`, slot: sl, lines: [], row: Object.assign(Object.create(d.row), { slot: sl }) }));
  eq([S.pieceRecords(jobs, { of: 3 }).map(r => r.tag), S.slotWord(jobs[1]), S.nounOf(jobs[0]), S.nounWord('Disc 2', jobs[0])], [['DISC 1 of 3', 'DISC 2 of 3', 'DISC 3 of 3'], 'Disc 2', '', 'Disc 2'], 'the discs say "Disc 2 of 3"');
  eq(S.splitWords(d.spec.personalization, 3).words, ['A', 'B', 'C'], 'and read their words as before');
  eq(S.wordsForDiscs(d.spec, d.plan.slots, {}).words, ['A', 'B', 'C'], 'wordsForDiscs, as before');
  const one = world({ transactionId: '78', listingId: '555', sku: 'COMET_9427', title: 'Comet Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['Anna'], variations: [V('Metal choice', 'Gold')] });
  eq([one.spec.problems.length, one.spec.pieceCount, one.spec.pair.kind, one.spec.pair.noun, one.spec.pieceProfile, one.spec.engraving, one.plan.split], [0, 1, 'single', undefined, undefined, undefined, false], 'an ordinary single charm: one piece, no profile, no face reading, the back as always'); }
// the profile adds fields to the pair facts of a bar line only
eq(Object.keys(world(mk('gold • 3 bars ($45.60)')).spec.pair).filter(k => !Object.keys(world({ transactionId: '1', listingId: '555', sku: 'COMET_9427', title: 'x', quantity: 1, metalKey: 'gold', personalization: [], variations: [V('Number of Discs', '3')] }).spec.pair).includes(k)).sort(), ['noun', 'profile'], 'a bar line\'s pair facts are a disc line\'s plus the noun and the profile id');

// ── 10 · the profile reads nothing, writes nothing, listens to nothing (the whole project is about cutting cloud cost) ────────────────────────────────────────────────────────────
{ const src = fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8'), a = src.lastIndexOf('/*', src.indexOf('3c3 · a LISTING that sells N separate bars')), b = src.indexOf('/* ═══ 3c2 · a LISTING that sells a SET');
  ok(a > 0 && b > a, 'the profile section is found');
  const section = src.slice(a, b).replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '');
  ok(!/\b(?:fetch|XMLHttpRequest|WebSocket|EventSource|onSnapshot|firestore|Firestore|setInterval|setTimeout|localStorage|sessionStorage|indexedDB|navigator|document|window)\b|\bapi\s*\(|\.collection\(|\.doc\(|\.set\(\{|\.update\(\{/.test(section), 'no read, write, listener, poll or storage in the profile code');
  const nests = O.PIECE_LISTINGS.every(x => Object.isFrozen(x) && Object.isFrozen(x.listings) && Object.isFrozen(x.skus)); ok(nests, 'the table is frozen: one row for each listing'); }
// the page: every file this changed has its cache token bumped (charm-nest-1.html), and no new public file is needed
{ const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  for (const f of ['charm-nest-orders.js', 'charm-nest-engrave-piece-words.js', 'charm-nest-engrave-sides.js', 'charm-nest-engrave-setup.js', 'charm-nest-engrave-order.js', 'charm-nest-engrave-card.js', 'charm-nest-engraving-seals.js', 'charm-nest-piece-switch.js', 'charm-nest-order-rows.js', 'charm-nest-sheetwin.js', 'charm-nest-bridge.js']) {
    const m = new RegExp('<script src="' + f.replace(/[.]/g, '\\.') + '\\?v=([^"]*)"').exec(html);
    if (m) ok(/-br1(-|$)/.test(m[1]), `${f}: cache token carries -br1 (${m[1].slice(-24)})`); else ok(false, `${f}: script tag not found`); } }

console.log(`bar-necklace-profile: ${n} checks passed`);
