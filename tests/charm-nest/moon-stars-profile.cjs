// MOONSTARS (Paul, 19 Oct 2026, order 4203628477): a PROFILE for Etsy listing 1095123288, "Moon Necklace Gold Crescent ..." (Etsy SKU MOON_3288, a drawing of the moon alone: the wrong design). ONE moon and 1 to 5 stars on
// ONE necklace, made from the master design "TINY MOON & STARS 2" (one file, two bodies: the LEFT body is the star, the RIGHT body is the moon); every star engraved on its FRONT with its own words, the moon never.
// Offline: no Etsy, no live read, no write, no paid call.
//   node tests/charm-nest/moon-stars-profile.cjs
// What it proves (charm-nest-orders.js PIECE_LISTINGS design / body / companion, pieceBodiesOf, countedPieces; charm-nest-engrave-sides.js blankSlots / plan / pieceList / summary; charm-nest-engrave-piece-words.js with the noun
// "star"; the bridge's real Pool, run in a vm, cutting each piece from ITS body):
//   · THE REAL ORDER (listing 1095123288, SKU MOON_3288, "Necklace Options" = "Silver • 2 stars ($43.20)", "First Star: M\nSecond Star: T") resolves with NO decision: design TINY MOON & STARS 2 (not MOON_3288), metal SS, 3 pieces of
//     one necklace (2 stars D1, D2 + the moon), the stars engraved FRONT "M" and "T", the moon with no engraving job and a decision that waits on nothing
//   · all 15 values of the drop-down read; "Gold • 5 stars" is 6 pieces; bought twice: 2 moons and 2N stars; an unreadable value still asks a person
//   · the words: "First Star:", "Star 1:", one a line, a list; fewer or more texts than stars, N/A, a message beside them: flagged for a person
//   · the pool: the stars are cut from the left body, the moon from the right body, all one group (one necklace); a design that cannot give two bodies holds the line
//   · the bar profile, a disc necklace and a plain line are what they were
const assert = require('assert');
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require('../../charm-nest-orders.js');
const P = require('../../charm-nest-pair.js');
const S = require('../../charm-nest-engrave-sides.js');
const F = require('../../charm-nest-engrave-faces.js');
const R = require('../../charm-nest-engrave-rows.js');
const PW = require('../../charm-nest-engrave-piece-words.js');
const RD = require('../../charm-nest-readiness.js');
const OR = require('../../charm-nest-order-rows.js');
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const DESIGN = 'TINY MOON & STARS 2';
const ENTRY = { sku: DESIGN, widthPt: 51.6, heightPt: 39.1, holes: 2, pair: { v: 1, bodies: 2, mismatched: true } };   // (the master's "Left + Right pair" entry: 18.2 x 13.8 mm, 2 holes)
const MASTER = { [DESIGN]: ENTRY, MOON_3288: { sku: 'MOON_3288', widthPt: 30, heightPt: 30 }, 'VERTICAL BAR_1277': { sku: 'VERTICAL BAR_1277', widthPt: 73.4, heightPt: 14 }, INITIAL_8391: { sku: 'INITIAL_8391', widthPt: 34, heightPt: 34 }, OTHER_MOON: { sku: 'OTHER_MOON', widthPt: 30, heightPt: 30 } };
const entryFor = s => MASTER[String(s || '').toUpperCase()] || null;
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' }, extra || {});
const V = (name, value) => ({ name, value });

// ── THE REAL ORDER (the shop owner's screenshots) ─────────────────────────────────────────────────────────────────────────────────────────
const RID = '4203628477', LISTING = '1095123288', SKU = 'MOON_3288', OPT = 'Necklace Options', TX = '5003628477';
const TITLE = 'Moon Necklace Gold Crescent Wedding Gift For Her Initial Necklace Celestial Jewelry Friendship Necklace Personalized Jewelry Gift For Mom';
const WORDS2 = 'First Star: M\nSecond Star: T';
const PRICES = { Silver: [37.6, 43.2, 46.4, 51.2, 55.2], Gold: [42.4, 46.4, 51.2, 55.2, 60], RoseGold: [44, 48.8, 53.6, 58.4, 62.4] };
const DROPDOWN = ['Silver', 'Gold', 'RoseGold'].flatMap(m => [1, 2, 3, 4, 5].map(k => `${m} • ${k} star${k > 1 ? 's' : ''} ($${PRICES[m][k - 1].toFixed(2)})`));
const METALKEY = { Silver: 'silver', Gold: 'gold', RoseGold: 'rose' }, CODE = { Silver: 'SS', Gold: 'GF', RoseGold: 'RG' };
const ORD = ['First', 'Second', 'Third', 'Fourth', 'Fifth', 'Sixth', 'Seventh', 'Eighth', 'Ninth', 'Tenth'];
const mk = (value, pers, o) => Object.assign({ transactionId: TX, listingId: LISTING, productId: '12345', sku: SKU, title: TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: pers == null ? [WORDS2] : pers, variations: [V(OPT, value)] }, o || {});
// one line read the way the sorter reads it: its pieces, its engraving slots, its faces
const SC = { pair: P, entryFor, poolRow: () => null, charmOf: () => null };
function world(line, c) {
  const order = { receiptId: RID, updateTs: 0 }, spec = O.interpretLine(order, line, c || ctx());
  noNested(spec, 'spec');
  const row = { key: `${RID}_${line.transactionId}`, order, line, spec, state: 'pulled' };
  const pieces = P.piecesFor(Object.assign({}, line, { receiptId: RID, quantity: spec.quantity, form: spec.form, spec }), null);
  row.poolIds = pieces.map((p, i) => `${row.key}_${i + 1}`);
  const plan = S.plan(SC, row, []), setup = F.setupOf(row);
  return { order, spec, row, pieces, plan, setup, faceJobs: F.planFaces(setup, plan.slots) };
}
const asks = spec => (spec.problems || []).filter(p => p.kind === 'needsMapping');
const seeds = w => w.plan.slots.map(sl => (F.seedFor(w.setup, 'front', sl) || {}).text || null);
// the face jobs of a line as the Engrave tab holds them (a side row each, the piece's pool ids)
const jobsOf = w => w.faceJobs.map(fj => { const rec = { needed: true, state: 'approved', approved: true }, j = { key: `${w.row.key}#${fj.slot}~front`, slot: fj.slot, face: 'front', lines: [], engraveRec: rec }; j.row = S.sideRow(SC, w.row, fj.slot, j); j.copies = j.row.poolIds.slice(); return j; });

// ── 1 · the real order resolves by itself ─────────────────────────────────────────────────────────────────────────────────────────
{ const w = world(mk('Silver • 2 stars ($43.20)')), sp = w.spec;
  eq(sp.problems, [], 'no decision is needed (the "Necklace Options ... not mapped" hold is gone)');
  eq([sp.designSku, sp.skuSource, sp.material, sp.materialKey, O.CARD_TAG[sp.material]], [DESIGN, 'alias', 'silver', 'silver', 'SS'], 'the design is TINY MOON & STARS 2 (never MOON_3288), the metal Silver (SS)');
  eq(sp.viaProfile, { profile: SKU, design: DESIGN, etsy: SKU, was: SKU, wasSource: 'transaction' }, 'the line says which design the profile put over which Etsy SKU');
  eq(sp.options.map(o => [o.name, o.value, o.mapped && o.mapped.field, o.mapped && o.mapped.value, o.mapped && o.mapped.source]), [[OPT, 'Silver • 2 stars ($43.20)', 'count', '3', 'rule:count:profile']], 'the drop-down is read by the profile: 3 pieces (2 stars and the moon)');
  eq([sp.pieceCount, sp.pair.kind, sp.pair.perUnit, sp.pair.noun, sp.pair.profile, sp.pair.earring, sp.pair.sides], [3, 'multi', 3, 'star', SKU, false, [null, null, null]], '3 pieces of ONE necklace, no ear, no side');
  eq([sp.pair.companion, sp.pair.pieceBodies, O.pieceBodiesOf(sp)], [{ noun: 'moon', n: 1, body: 1, engraved: false }, [0, 0, 1], [0, 0, 1]], 'the two stars are the left body (0), the moon the right body (1)');
  ok(!sp.pair.twoBodies && !sp.pair.oneBody && !sp.pair.glued && !sp.pair.notes.some(t => /both bodies/.test(t)), 'never one piece holding both bodies, never the left body alone');
  ok(sp.pair.notes.some(t => /1 moon and 2 stars on one necklace/.test(t)), 'the order\'s reading says it in one line: ' + sp.pair.notes.join(' / '));
  eq(O.countedPieces({ spec: sp, line: w.row.line }), { total: 3, qty: 1, noun: 'star', counted: 2, companion: { noun: 'moon', n: 1 } }, 'a counted line of 3 pieces: 2 stars and 1 moon');
  eq(O.countedFact({ spec: sp, line: w.row.line }), ' · 2 stars + 1 moon', 'Quantity 1 · 2 stars + 1 moon (never "3 stars")');
  eq(w.pieces.map(p => [p.n, p.of, p.side, p.mirror, p.groupKey]), [1, 2, 3].map(k => [k, 3, null, false, RID + ':' + TX]), 'three pool pieces of ONE group (one necklace: they go on one sheet), never mirrored');
  eq([w.plan.split, w.plan.slots, w.row.poolIds.map(id => w.plan.of.get(id))], [true, ['D1', 'D2'], ['D1', 'D2', 'D3']], 'an engraving slot for each STAR (D1, D2); the moon is piece D3 of the necklace and has NO job');
  eq(sp.pieceProfile.words, { D1: 'M', D2: 'T' }, '"First Star: M" goes on star 1, "Second Star: T" on star 2 (the labels are not engraved)');
  eq([sp.pieceProfile.ok, sp.pieceProfile.sure, sp.pieceProfile.how, sp.pieceProfile.face, sp.pieceProfile.metal, sp.pieceProfile.count, sp.pieceProfile.companion], [true, true, 'numbered', 'front', 'silver', 2, { noun: 'moon', n: 1, engraved: false }], 'nothing is asked of a person');
  eq([sp.engraving.mode, sp.engraving.source, sp.engraving.needsPerson, sp.engraving.textSource, Object.keys(sp.engraving.pieces)], ['front', 'profile', false, 'labelled', ['D1', 'D2']], 'the engraving is on the FRONT of each star (no words at all for the moon)');
  eq(w.faceJobs, [{ slot: 'D1', face: 'front' }, { slot: 'D2', face: 'front' }], 'two engraving jobs: the front of star 1 and the front of star 2; none for the moon, no back');
  eq(seeds(w), ['M', 'T'], 'each front job starts with ITS star\'s own words (no paid reading)');
  // the words every surface uses for a piece
  const jobs = jobsOf(w);
  eq(S.pieceRecords(jobs).map(r => [r.tag, r.label, r.index, r.of]), [['STAR 1 of 2', 'Star 1', 1, 2], ['STAR 2 of 2', 'Star 2', 2, 2]], 'the chips say "STAR 1 of 2", "STAR 2 of 2" (the moon is no star of the count)');
  eq(jobs.map(j => [S.pieceRecord(j).tag, S.slotWord(j), S.pieceTag(j, 2)]), [['STAR 1 of 2', 'Star 1', 'STAR 1 of 2'], ['STAR 2 of 2', 'Star 2', 'STAR 2 of 2']], 'pieceRecord without a count, slotWord and pieceTag say the same');
  eq(S.pieceList(SC, w.row).map(p => [p.slot, p.label, p.tag, !!p.blank]), [['D1', 'Star 1', 'STAR 1 of 2', false], ['D2', 'Star 2', 'STAR 2 of 2', false], ['D3', 'Moon', 'MOON', true]], 'the order\'s piece list: Star 1, Star 2 and the Moon (blank: never engraved)');
  eq([S.companionWord(w.row), S.isBlankSlot(w.row, 'D3'), S.isBlankSlot(w.row, 'D2'), [...S.blankSlots(w.row)]], ['Moon', true, false, ['D3']], 'blankSlots / isBlankSlot / companionWord');
  eq([OR.subLabel('counted', 0, 2, { noun: 'star' }), OR.subLabel('counted', 1, 3, { noun: 'bar' }), OR.subLabel('counted', 1, 3, { noun: 'disc' })], ['Star 1 of 2', 'Bar 2 of 3', 'Disc 2 of 3'], 'the order window drawer says "Star 1 of 2"');
  // the Engrave tab: the two front jobs of one line are ONE row of pieces
  const rows = R.group(jobs, jobs, { discs: true });
  eq([rows.length, rows[0].jobs.length, R.kindOf(rows[0])], [1, 2, 'discs'], 'the Engrave tab lists the two stars as one row of two pieces');
  // the line's decision: the moon waits on nothing (needed false, approved), the stars are their own jobs'
  S.linkParent(SC, w.row, jobs);
  const eng = w.row.engrave, [s1, s2, moon] = w.row.poolIds;
  eq([eng.approved, eng.pieces[moon]], [true, { needed: false, state: 'none', approved: true }], 'the moon\'s own decision: nothing to engrave, so it never waits');
  ok(eng.pieces[s1].needed && eng.pieces[s2].needed && eng.pieces[s1].faces.front && !eng.pieces[moon].faces, 'the stars carry their FRONT decisions');
  const dec = RD.decisions([w.row]);
  eq([dec[moon], dec[s1].needed], [{ needed: false, state: 'none', approved: true }, true], 'the readiness check reads the moon as a plain piece and the stars as engraved ones');
  jobs[1].engraveRec = { needed: true, state: 'review', approved: false };
  eq([w.row.engrave.approved, w.row.engrave.pieces[moon].approved, w.row.engrave.pieces[s2].approved], [false, true, false], 'a star still to approve keeps the line waiting; the moon never does');
  S.unlinkParent(SC, w.row);
  eq(w.row.engrave.pieces[moon], { needed: false, state: 'none', approved: true }, 'and a line taken back to a plain record keeps the moon\'s decision');
}

// ── 1a · the order held today ("NEEDS A DECISION · option not mapped") is released by the same reading ─────────────────────────────────
{ const w = world(mk('Silver • 2 stars ($43.20)')), prev = { problems: [{ kind: 'needsMapping', optionName: OPT, optionValue: 'Silver • 2 stars ($43.20)' }], designSku: SKU };
  const held = Object.assign({}, w.row, { state: 'held', reason: `Options: option "${OPT}: Silver • 2 stars ($43.20)" not mapped — Etsy keeps one SKU (MOON_3288) for every choice of this listing`, problems: [], poolIds: [] });
  ok(O.questionGone(held, prev), 'a line held for that question, read clean now, goes back to the intake (questionGone)');
  ok(!O.questionGone(Object.assign({}, held, { hold: 'held by Paul' }), prev), 'a person\'s own hold stays'); }

// ── 2 · every value of the drop-down (15) ─────────────────────────────────────────────────────────────────────────────────────────
eq(DROPDOWN.length, 15, 'fifteen values');
for (const v of DROPDOWN) {
  const metal = v.split(' ')[0], k = +/(\d) star/.exec(v)[1], names = ['A', 'B', 'C', 'D', 'E'].slice(0, k);
  const w = world(mk(v, [names.map((x, i) => `${ORD[i]} Star: ${x}`).join('\n')], { metalKey: METALKEY[metal], metalLabel: metal }));
  eq([w.spec.problems.length, w.spec.designSku, w.spec.pieceCount, w.spec.material, O.CARD_TAG[w.spec.material], w.spec.pieceProfile.metal], [0, DESIGN, k + 1, METALKEY[metal], CODE[metal], METALKEY[metal]], `${v}: ${k} star(s) + 1 moon = ${k + 1} pieces, ${CODE[metal]}, nothing asked`);
  eq([w.plan.slots, w.spec.pair.pieceBodies], [Array.from({ length: k }, (_, i) => 'D' + (i + 1)), Array.from({ length: k + 1 }, (_, i) => (i < k ? 0 : 1))], `${v}: a slot for each star, the moon the last piece (right body)`);
  eq([Object.values(w.spec.pieceProfile.words), seeds(w)], [names, names], `${v}: each star its own words, in order`);
  eq([w.spec.engraving.mode, w.spec.engraving.needsPerson, w.faceJobs.length, w.faceJobs.every(j => j.face === 'front')], ['front', false, k, true], `${v}: ${k} front engraving job(s), none for the moon`);
  eq(O.countedFact({ spec: w.spec, line: w.row.line }), ` · ${k} star${k > 1 ? 's' : ''} + 1 moon`, `${v}: "${k} star(s) + 1 moon"`);
}
// the metal comes from the drop-down when the station gave none
for (const [v, key, tag] of [['Gold • 2 stars ($46.40)', 'gold', 'GF'], ['RoseGold • 2 stars ($48.80)', 'rose', 'RG'], ['Silver • 2 stars ($43.20)', 'silver', 'SS']]) {
  const sp = world(mk(v, [WORDS2], { metalKey: '', metalLabel: '' })).spec;
  eq([sp.problems.length, sp.materialKey, O.CARD_TAG[sp.material]], [0, key, tag], `${v}: no station metal, the drop-down names it (${tag})`); }
// "Gold • 5 stars" is 6 pieces
{ const w = world(mk('Gold • 5 stars ($60.00)', ['Star 1: A\nStar 2: B\nStar 3: C\nStar 4: D\nStar 5: E'], { metalKey: 'gold' }));
  eq([w.spec.problems.length, w.spec.pieceCount, w.pieces.length, w.plan.slots.length, w.faceJobs.length, w.spec.pair.pieceBodies], [0, 6, 6, 5, 5, [0, 0, 0, 0, 0, 1]], 'Gold • 5 stars: 6 pieces (5 stars + 1 moon), 5 front jobs');
  eq(seeds(w), ['A', 'B', 'C', 'D', 'E'], '"Star 1: A" .. "Star 5: E" go to stars 1..5');
  eq(w.row.poolIds.map(id => w.plan.of.get(id)).slice(-1), ['D6'], 'the moon is the sixth piece'); }
// one star: 2 pieces, still a counted necklace (the star is D1, its words its own)
{ const w = world(mk('Gold • 1 star ($42.40)', ['First Star: Z'], { metalKey: 'gold' }));
  eq([w.spec.problems.length, w.spec.pieceCount, w.plan.slots, w.faceJobs, seeds(w), w.spec.pieceProfile.words], [0, 2, ['D1'], [{ slot: 'D1', face: 'front' }], ['Z'], { D1: 'Z' }], 'one star: the star (D1, front "Z") and the moon, no job for the moon');
  for (const pers of [['Z'], ['Star 1: Z'], ['Star: Z'], ['1st Star: Z']]) eq(world(mk('Gold • 1 star ($42.40)', pers, { metalKey: 'gold' })).spec.pieceProfile.words, { D1: 'Z' }, `one star, ${JSON.stringify(pers)}: its words are "Z"`);
  eq(S.pieceList(SC, w.row).map(p => [p.label, p.tag, !!p.blank]), [['Star 1', 'STAR 1', false], ['Moon', 'MOON', true]], 'its pieces: Star 1 and the Moon'); }

// ── 3 · bought twice: two necklaces, 2 moons and 2N stars ─────────────────────────────────────────────────────────────────────────────────
{ const w = world(mk('Silver • 2 stars ($43.20)', ['First Star: M\nSecond Star: T\nThird Star: A\nFourth Star: B'], { quantity: 2 }));
  eq([w.spec.problems.length, w.spec.pieceCount, w.pieces.length, O.pieceBodiesOf(w.spec)], [0, 6, 6, [0, 0, 1, 0, 0, 1]], 'quantity 2: 6 pieces, 2 moons and 4 stars');
  eq([w.plan.slots, w.row.poolIds.map(id => w.plan.of.get(id))], [['D1', 'D2', 'D1.2', 'D2.2'], ['D1', 'D2', 'D3', 'D1.2', 'D2.2', 'D3.2']], 'star slots D1, D2 / D1.2, D2.2; the moons D3 and D3.2 have no job');
  eq(seeds(w), ['M', 'T', 'A', 'B'], 'four words, in the order of the stars');
  eq(O.countedPieces({ spec: w.spec, line: w.row.line }), { total: 6, qty: 2, noun: 'star', counted: 4, companion: { noun: 'moon', n: 2 } }, '4 stars + 2 moons');
  eq(S.pieceList(SC, w.row).map(p => p.label), ['Star 1 · set 1', 'Star 2 · set 1', 'Moon · set 1', 'Star 1 · set 2', 'Star 2 · set 2', 'Moon · set 2'], 'each necklace: its stars and its moon');
  const jobs = jobsOf(w); S.linkParent(SC, w.row, jobs);
  eq([w.row.poolIds[2], w.row.poolIds[5]].map(id => w.row.engrave.pieces[id]), [{ needed: false, state: 'none', approved: true }, { needed: false, state: 'none', approved: true }], 'both moons wait on nothing');
  const w2 = world(mk('Silver • 2 stars ($43.20)', [WORDS2], { quantity: 2 }));
  eq([w2.spec.engraving.needsPerson, w2.spec.engraving.reviewKind], [true, 'pieces'], 'two words for two necklaces of two stars: which necklace gets which is for a person'); }

// ── 4 · what is NOT read: a person is asked ─────────────────────────────────────────────────────────────────────────────────────────
for (const v of ['Silver • 6 stars ($66.00)', 'Platinum • 2 stars', 'Silver', 'Select an option', 'Silver • 2 stars • 3 stars', 'Silver • 0 stars']) {
  const w = world(mk(v)), a = asks(w.spec);
  eq([a.length, a[0].optionName, a[0].optionValue, w.spec.pieceCount, w.spec.pair.companion, w.spec.pair.pieceBodies], [1, OPT, v, 1, undefined, undefined], `${JSON.stringify(v)}: a person is asked about the option, nothing is pooled as stars and a moon`);
  ok(a[0].why && !/not loaded yet/.test(a[0].why), `${JSON.stringify(v)}: one plain line says why (${a[0].why})`);
  eq(w.spec.designSku, DESIGN, `${JSON.stringify(v)}: the design is still the listing's (TINY MOON & STARS 2), never MOON_3288`); }
eq([O.pieceValueRead('RoseGold • 3 stars ($53.60)', O.PIECE_LISTINGS[1]).metal, O.pieceValueRead('Rose Gold - 3 Stars', O.PIECE_LISTINGS[1]).n, O.pieceValueRead('Gold • three stars', O.PIECE_LISTINGS[1]).n, O.pieceValueRead('Gold • 3 bars', O.PIECE_LISTINGS[1]).ok], ['rose', 3, 3, false], 'the values are read by the row\'s own noun (stars, never bars)');
// a person's own count for a value is the number of STARS (the moon is added)
{ const maps = { [LISTING]: { [OPT.toLowerCase()]: { 'Silver • 6 stars ($66.00)': { field: 'count', value: '6' } } } };
  const w = world(mk('Silver • 6 stars ($66.00)', ['1\n2\n3\n4\n5\n6']), ctx({ optionMaps: maps }));
  eq([asks(w.spec).length, w.spec.pieceCount, w.plan.slots.length, w.spec.pair.pieceBodies], [0, 7, 6, [0, 0, 0, 0, 0, 0, 1]], 'a person said "6" for the value: six stars and the moon'); }

// ── 5 · the design: only this listing (or its SKU with its drop-down); a person's pick of another design stands ──────────────────────────────────
{ const other = world(mk('Silver • 2 stars ($43.20)', null, { listingId: '1095123299', sku: 'OTHER_3288' }));
  eq([asks(other.spec).length, other.spec.designSku, other.spec.pieceCount, other.spec.pair.companion], [1, 'OTHER_3288', 1, undefined], 'another listing\'s line with the same drop-down is not this profile\'s: it asks, as it did');
  const skuOnly = world(mk('Silver • 2 stars ($43.20)', null, { listingId: '' }));
  eq([skuOnly.spec.problems.length, skuOnly.spec.designSku, skuOnly.spec.pieceCount], [0, DESIGN, 3], 'the SKU MOON_3288 with the drop-down and no listing id is the profile\'s');
  const plain = world({ transactionId: '9', listingId: '555', sku: SKU, title: 'Crescent Moon Necklace', quantity: 1, metalKey: 'gold', personalization: ['Anna'], variations: [V('Metal', 'Gold')] });
  eq([plain.spec.problems.length, plain.spec.designSku, plain.spec.pieceCount, plain.spec.viaProfile, plain.spec.pair.companion], [0, SKU, 1, undefined, undefined], 'the SKU MOON_3288 on another listing with no stars drop-down reads exactly as before (the moon drawing)');
  const noDrop = world({ transactionId: '10', listingId: LISTING, sku: 'CURB', title: 'Font audit line', quantity: 1, metalKey: 'gold', personalization: [], variations: [V('Metal Choice', 'Gold Filled'), V('Font Selection', '18"/Stylish')] });
  eq([noDrop.spec.designSku, noDrop.spec.viaProfile, noDrop.spec.pieceCount, noDrop.spec.pair.companion, noDrop.spec.pieceProfile], ['CURB', undefined, 1, undefined, undefined], 'a line of the listing WITHOUT the stars drop-down is some other product: its own SKU, one piece, as before');
  const nomaster = world(mk('Silver • 2 stars ($43.20)'), ctx({ masterEntry: s => (String(s).toUpperCase() === SKU ? MASTER.MOON_3288 : null) }));
  eq([nomaster.spec.designSku, nomaster.spec.problems.map(p => p.kind)], [DESIGN, ['unmatchedSku']], 'a library without TINY MOON & STARS 2: the line waits "not in any master file", it is never made as MOON_3288');
  const viaInv = world(mk('Silver • 2 stars ($43.20)'), ctx({ listingSkus: { [LISTING]: { products: [{ sku: 'MOON_3288', values: [] }] } } }));
  eq(viaInv.spec.designSku, DESIGN, 'Etsy\'s inventory table naming MOON_3288 changes nothing');
  // a person's answer for the option naming ANOTHER design stands (the profile does not take it back); one naming the Etsy SKU itself does not
  const pick = d => ctx({ optionMaps: { [LISTING]: { [OPT.toLowerCase()]: { 'Silver • 2 stars ($43.20)': { field: 'design', value: d } } } } });
  eq(world(mk('Silver • 2 stars ($43.20)'), pick('OTHER_MOON')).spec.designSku, 'OTHER_MOON', 'a person picked another design for the value: it stands');
  eq(world(mk('Silver • 2 stars ($43.20)'), pick('MOON_3288')).spec.designSku, DESIGN, 'a pick of the Etsy SKU itself (the moon alone) is the wrong design: the profile\'s stands');
  eq([O.profileDesignOf(mk('x'), O.PIECE_LISTINGS[1], SKU, 'transaction', entryFor), O.profileDesignOf(mk('x'), O.PIECE_LISTINGS[0], SKU, 'transaction', entryFor), O.profileDesignOf(mk('x', null, { listingId: '7', variations: [] }), O.PIECE_LISTINGS[1], SKU, 'transaction', entryFor)], [DESIGN, '', ''], 'profileDesignOf: the row\'s design for its listing, nothing for a row with no design or a line without the drop-down'); }

// ── 6 · the words of each star ─────────────────────────────────────────────────────────────────────────────────────────
{ for (const [pers, k, want, how] of [
    [[WORDS2], 2, ['M', 'T'], 'numbered'], [['Second Star: T\nFirst Star: M'], 2, ['M', 'T'], 'numbered'], [['Star 1: M, Star 2: T'], 2, ['M', 'T'], 'numbered'], [['Star names:\nM\nT'], 2, ['M', 'T'], 'lines'],
    [['M\nT'], 2, ['M', 'T'], 'lines'], [['M', 'T'], 2, ['M', 'T'], 'lines'], [['First star - M\nSecond star - T\nThird star - J'], 3, ['M', 'T', 'J'], 'numbered'], [['1st Star: Anna\n2nd Star: Ben'], 2, ['Anna', 'Ben'], 'numbered'],
    [['Stars: M, T'], 2, ['M', 'T'], 'list']]) {
    const w = world(mk(`Silver • ${k} stars`, pers)), words = w.plan.slots.map(sl => w.spec.pieceProfile.words[sl]);
    eq([words, w.spec.pieceProfile.how, w.spec.pieceProfile.sure, w.spec.engraving.needsPerson], [want, how, true, false], `${JSON.stringify(pers)} for ${k} stars: ${want.join(' / ')} (${how})`); }
  // fewer texts than stars: the star with none is flagged, never engraved blank; more texts than stars: flagged; N/A; one text for several; a message beside them
  { const w = world(mk('Silver • 3 stars', [WORDS2])), e = w.spec.engraving;
    eq([w.spec.pieceProfile.sure, e.needsPerson, e.reviewKind, seeds(w)], [false, true, 'pieces', ['M', 'T', null]], 'two texts for three stars: star 3 has no words, a person says');
    ok(/2 names for 3 stars: star 3 has none/.test(e.why), e.why); }
  { const w = world(mk('Silver • 2 stars', ['First Star: M\nSecond Star: T\nThird Star: J'])), e = w.spec.engraving;
    eq([e.needsPerson, e.reviewKind, e.pieces], [true, 'pieces', undefined], 'three texts for two stars: flagged, no star given a text by a guess'); }
  { const w = world(mk('Silver • 2 stars', ['First Star: M\nSecond Star: N/A'])), e = w.spec.engraving;
    eq([e.needsPerson, seeds(w)], [true, ['M', null]], 'an N/A text: that star is flagged, the other keeps its words'); }
  { const w = world(mk('Silver • 2 stars', ['M'])); eq([w.spec.engraving.needsPerson, w.spec.engraving.pieces], [true, undefined], 'one text for two stars is not copied onto both: a person says'); }
  { const w = world(mk('Silver • 2 stars', [WORDS2], { buyerMessage: 'Could the second star be bigger?' })); eq([w.spec.engraving.needsPerson, w.spec.engraving.reviewKind, seeds(w)], [true, 'texts', ['M', 'T']], 'a message beside the words: a person confirms, the words stay as the proposal'); }
  { const w = world(mk('Silver • 2 stars', [])); eq([w.spec.engraveCandidate, w.spec.engraving, w.spec.problems.length, w.spec.pieceCount], [false, undefined, 0, 3], 'nothing written at all: two plain stars and the moon'); }
  // the reader itself: the noun "star" makes "First Star:" and "Star 2:" labels; without it the text reads as it always did
  eq([PW.read([WORDS2], 2, { noun: 'star' }).words, PW.read(['Star 2: T\nStar 1: M'], 2, { noun: 'star' }).words, PW.read([WORDS2], 2).words, PW.read(['First Star: M'], 1, { noun: 'star' }).words], [['M', 'T'], ['M', 'T'], ['First Star: M', 'Second Star: T'], ['M']], 'the words reader with the noun "star"');
  eq([PW.read(['First bar: A\nSecond bar: B'], 2, { noun: 'bar' }).words, PW.read(['First: A, Second: B'], 2).words], [['A', 'B'], ['A', 'B']], 'the bar labels and the plain ordinals read as before'); }

// ── 7 · the face: a side the buyer chose still wins ─────────────────────────────────────────────────────────────────────────────────────────
{ const w = world(mk('Silver • 2 stars ($43.20)', ['front: M\nback: Anna'], { variations: [V(OPT, 'Silver • 2 stars ($43.20)'), V('Engraving Side', 'Both')] }), ctx({ optionMaps: { [LISTING]: { 'engraving side': { both: { field: 'ignore', value: null } } } } }));
  eq([w.spec.engraving.mode, w.spec.engraving.source, w.plan.slots], ['both', 'option', ['D1', 'D2']], '"Engraving Side: Both" is the buyer\'s choice; the moon still has no slot'); }

// ── 8 · the pool (the bridge's real Pool in a vm): each piece from ITS body ─────────────────────────────────────────────────────────────────────
const K = require('./_keyWorld.cjs')(root);
const bboxOf = pts => [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))];
const cut = pts => ({ kind: 'path', closed: true, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: bboxOf(pts), subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat([['l', pts[0]]])] });
const starPts = (cx, cy, Ro, Ri) => Array.from({ length: 10 }, (_, i) => { const a = Math.PI / 2 + i * Math.PI / 5, q = i % 2 ? Ri : Ro; return [cx + q * Math.cos(a), cy + q * Math.sin(a)]; });
const moonPts = (cx, cy, Rr) => { const out = []; for (let i = 0; i <= 12; i++) { const a = -Math.PI / 2 + i * Math.PI / 12; out.push([cx + Rr * Math.cos(a), cy + Rr * Math.sin(a)]); } for (let i = 12; i >= 0; i--) { const a = -Math.PI / 2 + i * Math.PI / 12; out.push([cx + Rr * 0.45 * Math.cos(a), cy + Rr * Math.sin(a)]); } return out; };
const twoBody = (sku, bodies) => { const members = bodies.map((b, i) => Object.assign(b, { parent: i })); const bb = members.reduce((u, m) => [Math.min(u[0], m.bbox[0]), Math.min(u[1], m.bbox[1]), Math.max(u[2], m.bbox[2]), Math.max(u[3], m.bbox[3])], [1e9, 1e9, -1e9, -1e9]); return { sku, outline: members[0], members, bbox: bb, widthPt: bb[2] - bb[0], heightPt: bb[3] - bb[1], areaPt2: (bb[2] - bb[0]) * (bb[3] - bb[1]) }; };
K.DESIGNS[DESIGN] = () => twoBody(DESIGN, [cut(starPts(10, 14, 10, 4)), cut(moonPts(34, 20, 18))]);   // (the star on the left, the moon on the right, as the master draws them)
K.DESIGNS.ONE_MOON = () => twoBody('ONE_MOON', [cut(moonPts(18, 20, 18))]);
(async () => {
  const pool = async (value, qty, design) => {
    const w = K.world([design || DESIGN]); w.use(design || DESIGN);
    const entry = w.entries[design || DESIGN], line = mk(value, [WORDS2], { quantity: qty || 1, metalKey: 'gold' });
    const sp = O.interpretLine({ receiptId: RID, updateTs: 1 }, line, ctx({ masterEntry: s => (String(s).toUpperCase() === DESIGN ? entry : null) }));
    const row = { key: `${RID}_${TX}`, state: 'pulled', order: { receiptId: RID, createTs: 1, updateTs: 1 }, line, spec: Object.assign({}, sp, { designSku: design || sp.designSku, material: 'gold', form: 'necklace' }), problems: [], poolIds: [], engrave: { needed: true, state: 'classify', approved: false } };
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut');
    return { w, row, pools: put ? put.body.pools : [], charms: w.pages.gold.charms, bodies: P.bodiesOf(K.DESIGNS[design || DESIGN]()) };
  };
  { const t = await pool('Silver • 2 stars ($43.20)'), J = x => JSON.parse(JSON.stringify(x));
    eq([t.row.state, t.pools.length, t.charms.length], ['pooled', 3, 3], 'the real order makes three pool pieces');
    eq(t.charms.map(c => J(c.bbox)), [J(t.bodies[0].bbox), J(t.bodies[0].bbox), J(t.bodies[1].bbox)], 'star 1 and star 2 are the LEFT body (the star) alone, the third piece the RIGHT body (the moon) alone');
    eq(t.charms.map(c => c.oneBody), [0, 0, 1], 'each charm is ONE body of the design');
    eq(t.charms.map(c => c.members.length), [1, 1, 1], 'never both bodies in one piece');
    ok(t.pools[0].charmHash === t.pools[1].charmHash && t.pools[0].charmHash !== t.pools[2].charmHash && t.pools.every((p, i) => p.charmHash === t.charms[i].hash), 'the stored hash is each piece\'s own body (the stars share one, the moon has its own)');
    eq(t.pools.map(p => [p.groupKey, p.groupSize]), [0, 1, 2].map(() => [RID + ':' + TX, 3]), 'one group of three: the moon goes on the sheet with its stars');
    eq(t.pools.map(p => p.engrave), [true, true, false], 'the moon\'s pool record says it is not engraved');
    eq(t.pools.map(p => p.sku), [DESIGN, DESIGN, DESIGN], 'every piece is the design TINY MOON & STARS 2');
    const place = await t.w.Pool.ensureBase(t.w.c.B.pool.sources.get('charmnest/master/sku/' + DESIGN + '.ai'), null, { charmHash: t.pools[2].charmHash });
    eq(J(place.bbox), J(t.bodies[1].bbox), 'a reload reads the moon\'s stored hash back as the moon body (soloBase)'); }
  { const t = await pool('Gold • 5 stars ($60.00)');
    eq([t.pools.length, t.charms.map(c => c.oneBody)], [6, [0, 0, 0, 0, 0, 1]], 'Gold • 5 stars: five stars and the moon'); }
  { const t = await pool('Silver • 2 stars ($43.20)', 2);
    eq([t.pools.length, t.charms.map(c => c.oneBody), t.pools.map(p => p.engrave)], [6, [0, 0, 1, 0, 0, 1], [true, true, false, true, true, false]], 'bought twice: two necklaces, each two stars and its moon'); }
  { const t = await pool('Silver • 2 stars ($43.20)', 1, 'ONE_MOON');
    eq([t.row.state, t.pools.length], ['held', 0], 'a design that does not draw a star and a moon apart holds the line: never three copies of the whole drawing');
    ok(/separate bodies this listing's profile cuts/.test(t.row.reason), t.row.reason); }

  // ── 9 · what must not change ─────────────────────────────────────────────────────────────────────────────────────────
  { const bar = world({ transactionId: '7', listingId: '982791277', sku: 'VERTICAL BAR_1277', title: 'Skinny MINI Vertical Bar Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['Bar names:\nSophie\nSam\nEddy'], variations: [V('Engraving & # of Bars?', 'gold • 3 bars ($45.60)')] });
    eq([bar.spec.problems.length, bar.spec.designSku, bar.spec.pieceCount, bar.spec.pair.noun, bar.spec.pair.companion, bar.spec.pair.pieceBodies, bar.plan.slots, bar.spec.pieceProfile.words, bar.spec.viaProfile], [0, 'VERTICAL BAR_1277', 3, 'bar', undefined, undefined, ['D1', 'D2', 'D3'], { D1: 'Sophie', D2: 'Sam', D3: 'Eddy' }, undefined], 'the bar profile: 3 bars, its own SKU, no companion, no body plan');
    eq([O.countedFact({ spec: bar.spec, line: bar.row.line }), S.blankSlots(bar.row).size, S.pieceList(SC, bar.row).map(p => p.label)], [' · 3 bars', 0, ['Disc 1', 'Disc 2', 'Disc 3']], 'its words and its piece list are what they were');
    const disc = world({ transactionId: '77', listingId: '234758391', sku: 'INITIAL_8391', title: 'Initial Disc Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['Tag 1: A, Tag 2: B, Tag 3: C'], variations: [V('Number of Discs / Metal', '3 discs • gold')] });
    eq([disc.spec.problems.length, disc.spec.pieceCount, disc.spec.pair.noun, disc.spec.pair.companion, disc.plan.slots, O.countedFact({ spec: disc.spec, line: disc.row.line }), O.pieceBodiesOf(disc.spec)], [0, 3, undefined, undefined, ['D1', 'D2', 'D3'], ' · 3 discs', null], 'a disc necklace: as before');
    const jobs = disc.plan.slots.map(sl => ({ key: `${disc.row.key}#${sl}`, slot: sl, lines: [], row: Object.assign(Object.create(disc.row), { slot: sl }) }));
    eq([S.pieceRecords(jobs, { of: 3 }).map(r => r.tag), S.companionWord(disc.row), S.isBlankSlot(disc.row, 'D3')], [['DISC 1 of 3', 'DISC 2 of 3', 'DISC 3 of 3'], '', false], 'the discs say "Disc 2 of 3"; no companion');
    const one = world({ transactionId: '78', listingId: '555', sku: 'INITIAL_8391', title: 'Initial Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['Anna'], variations: [V('Metal choice', 'Gold')] });
    eq([one.spec.problems.length, one.spec.pieceCount, one.spec.pair.kind, one.spec.pair.companion, one.plan.split], [0, 1, 'single', undefined, false], 'a plain single charm: one piece'); }

  // ── 10 · the table, the page ─────────────────────────────────────────────────────────────────────────────────────────
  { const row = O.PIECE_LISTINGS.find(x => x.id === SKU);
    ok(row && Object.isFrozen(row) && Object.isFrozen(row.companion) && row.design === DESIGN && row.body === 0 && row.companion.body === 1 && row.companion.n === 1 && row.face === 'front' && row.noun === 'star' && row.min === 1 && row.max === 5, 'one frozen row: design, the stars the left body, one moon the right body, front, 1 to 5');
    ok(O.pieceListing({ listingId: LISTING, sku: 'x' }) === row && O.pieceListing({ listingId: '', sku: 'moon_3288' }) === row && O.pieceListing({ listingId: '982791277' }) === O.PIECE_LISTINGS[0], 'pieceListing: by listing id or SKU');
    const src = fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8');
    ok(/MOONSTARS \(Paul, 19 Oct 2026, order 4203628477\)/.test(src) && /TO ADD such a listing/.test(src) && /TO UNDO: delete the row/.test(src), 'the rule is written in the code, with how to add and remove a row');
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    for (const f of ['charm-nest-orders.js', 'charm-nest-engrave-piece-words.js', 'charm-nest-engrave-sides.js', 'charm-nest-bridge.js', 'charm-nest-order-rows.js', 'charm-nest-engrave-card.js', 'charm-nest-sheetwin.js', 'charm-nest-order-facts.js']) {
      const m = new RegExp('<script src="' + f.replace(/[.]/g, '\\.') + '\\?v=([^"]*)"').exec(html);
      ok(m && /-ms1(-|$)/.test(m[1]), `${f}: cache token carries -ms1`); } }

  console.log(`moon-stars-profile: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
