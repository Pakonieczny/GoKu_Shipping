// BROKENHEART (Paul, 19 Oct 2026, order 4204452679): a PROFILE for the "Best Friends Necklace Set of 2" listing (Etsy SKU Best_4003, the library design BEST 4003 / BEST_4003; the listing id is not known, the row matches
// the SKU). The design is a heart broken in two along a zigzag: the LEFT half and the RIGHT half, each its own piece on its own chain, each with its own engraving. One drop-down, named "Personalization" by Etsy, of
// 9 values "<METAL>·<faces>" (SILVER·BLANK, SILVER·Front, SILVER·Front&Back, GOLD·BLANK, GOLD·Front, GOLD· Front&Back, ROSEGOLD·BLANK, ROSEGOLD·Front, ROSEGOLD·Front&Back).
// Offline: no Etsy, no live read, no write, no paid call.
//   node tests/charm-nest/broken-heart-profile.cjs
// What it proves (charm-nest-orders.js PIECE_LISTINGS `pieces` / `value`, piecesFixed, pieceFacesRead, countRead, pairInfo, pieceProfileOf; charm-nest-engrave-sides.js labelsOf / nounWord / pieceList / pieceRecord;
// charm-nest-engrave-piece-words.js "Left half:"; the bridge's real Pool, run in a vm, cutting each half from ITS body):
//   · THE REAL ORDER (SILVER·Front, "Sisters"): 2 pieces of one heart (Left half D1 = body 0, Right half D2 = body 1), never one heart; "Sisters" is ONE text for two halves: never copied onto both, never cut into
//     "Sis" / "ters": the engraving waits for a person ("Say which words go on each half") with no proposal on either half
//   · two texts (two lines, "Mama / Daughter", "Mama & Daughter", "Left: … Right: …", "Left half: …", "Half 1: …", "BFF BFF") go one per half, left first, front of each half, nothing asked
//   · a note to the shop in brackets ("(Can you use the same font as pictured?)") is never engraved
//   · the 9 values: the metal, and the faces: Front = front jobs, Front&Back = front and back jobs (backs for a person), BLANK = no engraving at all (no job, nothing waits)
//   · quantity 2 = 4 pieces (2 hearts); the pool cuts the left half from the left body, the right half from the right body, one group; a master that draws ONE heart outline with the zigzag as a line inside it holds
//     the line with one plain line (never the whole heart twice, never one heart)
//   · the bars, the moon and stars and a plain line are what they were
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
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const DESIGN = 'BEST_4003';
const ENTRY = { sku: DESIGN, widthPt: 38.4, heightPt: 26.4, holes: 2, pair: { v: 1, bodies: 2, mismatched: true } };   // (the master's two-body entry: the left half and the right half)
const MASTER = { [DESIGN]: ENTRY, 'VERTICAL BAR_1277': { sku: 'VERTICAL BAR_1277', widthPt: 73.4, heightPt: 14 }, 'TINY MOON & STARS 2': { sku: 'TINY MOON & STARS 2', widthPt: 51.6, heightPt: 39.1, holes: 2, pair: { v: 1, bodies: 2, mismatched: true } }, MOON_3288: { sku: 'MOON_3288', widthPt: 30, heightPt: 30 }, HEART_1111: { sku: 'HEART_1111', widthPt: 30, heightPt: 26 } };
const entryFor = s => MASTER[String(s || '').toUpperCase()] || null;
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' }, extra || {});
const V = (name, value) => ({ name, value, valueId: '4417' });   // (a drop-down: Etsy gives its value an id; the personalisation box has none)

// ── THE REAL ORDER (the shop owner's report) ─────────────────────────────────────────────────────────────────────────────────────────
const RID = '4204452679', SKU = 'Best_4003', OPT = 'Personalization', TX = '5004452679';
const TITLE = 'Best Friends Necklace Set of 2, Set of 2 Necklaces, Best Friends Forever Personalized Jewelry, BFF Jewelry, Friendship Necklace';
const DROPDOWN = ['SILVER·BLANK', 'SILVER·Front', 'SILVER·Front&Back', 'GOLD·BLANK', 'GOLD·Front', 'GOLD· Front&Back', 'ROSEGOLD·BLANK', 'ROSEGOLD·Front', 'ROSEGOLD·Front&Back'];
const METALKEY = { SILVER: 'silver', GOLD: 'gold', ROSEGOLD: 'rose' }, CODE = { SILVER: 'SS', GOLD: 'GF', ROSEGOLD: 'RG' };
const mk = (value, pers, o) => Object.assign({ transactionId: TX, listingId: '', productId: '12345', sku: SKU, title: TITLE, quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: pers == null ? ['Sisters'] : pers, variations: value == null ? [] : [V(OPT, value)] }, o || {});
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
const seeds = (w, face) => w.plan.slots.map(sl => (F.seedFor(w.setup, face || 'front', sl) || {}).text || null);
const jobsOf = w => w.faceJobs.map(fj => { const rec = { needed: true, state: 'approved', approved: true }, j = { key: `${w.row.key}#${fj.slot}${fj.face === 'front' ? '~front' : ''}`, slot: fj.slot, face: fj.face, lines: [], engraveRec: rec }; j.row = S.sideRow(SC, w.row, fj.slot, j); j.copies = j.row.poolIds.slice(); return j; });
const words = w => w.plan.slots.map(sl => (w.spec.pieceProfile.words || {})[sl] || null);

// ── 1 · the real order: two halves, and "Sisters" is not guessed ─────────────────────────────────────────────────────────────────────────
{ const w = world(mk('SILVER·Front')), sp = w.spec;
  eq(sp.problems, [], 'no decision about the line itself (design, metal, count)');
  eq([sp.designSku, sp.material, O.CARD_TAG[sp.material]], [DESIGN, 'silver', 'SS'], 'the design is BEST_4003 (matched by the SKU, as before), Silver');
  eq([sp.pieceCount, sp.pair.kind, sp.pair.perUnit, sp.pair.noun, sp.pair.nouns, sp.pair.profile, sp.pair.earring, sp.pair.sides], [2, 'multi', 2, 'half', 'halves', DESIGN, false, [null, null]], 'TWO pieces of one heart: no ear, no side, never one heart piece');
  eq([sp.pair.pieceBodies, O.pieceBodiesOf(sp), sp.pair.labels], [[0, 1], [0, 1], ['Left half', 'Right half']], 'the left half is the left body (0), the right half the right body (1)');
  ok(!sp.pair.twoBodies && !sp.pair.oneBody && !sp.pair.glued && !sp.pair.companion && !sp.pair.notes.some(t => /both bodies/.test(t)), 'never one piece holding both halves, never the left half alone, no companion');
  ok(sp.pair.notes.some(t => /one heart in 2 halves/.test(t) && /the left half and the right half/.test(t)), 'the reading says it in one line: ' + sp.pair.notes.join(' / '));
  eq(O.countedPieces({ spec: sp, line: w.row.line }), { total: 2, qty: 1, noun: 'half', nouns: 'halves', labels: ['Left half', 'Right half'] }, 'a counted line of 2 halves');
  eq(O.countedFact({ spec: sp, line: w.row.line }), ' · 2 halves', 'Quantity 1 · 2 halves (never "2 halfs")');
  eq(w.pieces.map(p => [p.n, p.of, p.side, p.mirror, p.groupKey]), [1, 2].map(k => [k, 2, null, false, RID + ':' + TX]), 'two pool pieces of ONE group (one heart: they go on one sheet), never mirrored');
  eq([w.plan.split, w.plan.slots, w.row.poolIds.map(id => w.plan.of.get(id))], [true, ['D1', 'D2'], ['D1', 'D2']], 'an engraving slot for each half: D1 the left half, D2 the right half');
  const e = sp.engraving;
  eq([e.mode, e.source, e.needsPerson, e.reviewKind, e.pieces, e.front, e.raw], ['front', 'profile', true, 'pieces', undefined, null, 'Sisters'], 'the FRONT of each half, and "Sisters" waits for a person: no proposal on either half (never "Front: Sisters" across the heart)');
  ok(/Say which words go on each half/.test(e.why) && /one text \(“Sisters”\)/.test(e.why), e.why);
  eq([sp.pieceProfile.sure, sp.pieceProfile.ok, sp.pieceProfile.words, sp.pieceProfile.face, sp.pieceProfile.metal], [false, false, {}, 'front', 'silver'], 'the profile says it is not sure and gives no words');
  eq(w.faceJobs, [{ slot: 'D1', face: 'front' }, { slot: 'D2', face: 'front' }], 'two engraving jobs: the front of the left half and the front of the right half, no back');
  eq(seeds(w), [null, null], 'neither half starts with "Sisters" (nor "Sis" / "ters")');
  // the words every surface uses for a piece
  const jobs = jobsOf(w);
  eq(S.pieceRecords(jobs).map(r => [r.tag, r.label, r.index, r.of, r.face]), [['LEFT HALF', 'Left half', 1, 2, 'front'], ['RIGHT HALF', 'Right half', 2, 2, 'front']], 'the chips say "LEFT HALF", "RIGHT HALF" (the Engrave tab adds " · FRONT")');
  eq(jobs.map(j => [S.pieceRecord(j).tag, S.slotWord(j), S.slotWord(j, true), S.pieceTag(j, 2)]), [['LEFT HALF', 'Left half', 'Left half', 'LEFT HALF'], ['RIGHT HALF', 'Right half', 'Right half', 'RIGHT HALF']], 'pieceRecord, slotWord and pieceTag say the same');
  ok(jobs.every(j => !/disc|ear|piece \d/i.test(S.slotWord(j) + S.pieceTag(j, 2))), 'never "Disc 1 of 2", never "Left earring"');
  eq(S.pieceList(SC, w.row).map(p => [p.slot, p.label, p.tag, !!p.blank]), [['D1', 'Left half', 'LEFT HALF', false], ['D2', 'Right half', 'RIGHT HALF', false]], 'the order\'s piece list: the Left half and the Right half, both engraved');
  eq([S.labelsOf(w.row), S.blankSlots(w.row).size, S.companionWord(w.row), S.nounWord('Disc 2 of 2 · set 2', w.row), S.nounWord('DISC 1 of 2', w.row)], [['Left half', 'Right half'], 0, '', 'Right half · set 2', 'LEFT HALF'], 'labelsOf / nounWord');
  // the Engrave tab: the two front jobs of one line are ONE row of pieces
  const rows = R.group(jobs, jobs, { discs: true });
  eq([rows.length, rows[0].jobs.length, R.kindOf(rows[0])], [1, 2, 'discs'], 'the Engrave tab lists the two halves as one row of two pieces');
  S.linkParent(SC, w.row, jobs);
  const eng = w.row.engrave, [l, r] = w.row.poolIds;
  ok(eng.pieces[l].needed && eng.pieces[r].needed && eng.pieces[l].faces.front && eng.pieces[r].faces.front, 'both halves carry their own FRONT decision');
  const dec = RD.decisions([w.row]); eq([dec[l].needed, dec[r].needed], [true, true], 'the readiness check reads both halves as engraved pieces');
  jobs[1].engraveRec = { needed: true, state: 'review', approved: false };
  eq([w.row.engrave.approved, w.row.engrave.pieces[l].approved, w.row.engrave.pieces[r].approved], [false, true, false], 'the right half still to approve keeps the line waiting; the left half has its own approval');
  S.unlinkParent(SC, w.row); }

// ── 1b · the station hands the drop-down Etsy calls "Personalization" over as buyer text too: its value is the choice, never words for a half ──
{ const w = world(mk('SILVER·Front', ['SILVER·Front', 'Sisters'])), e = w.spec.engraving;
  eq([e.needsPerson, e.pieces, e.raw, seeds(w)], [true, undefined, 'Sisters', [null, null]], '["SILVER·Front", "Sisters"]: still ONE text ("Sisters"), the drop-down\'s value is no word for a half');
  const w2 = world(mk('GOLD·Front', ['GOLD·Front\nMama\nDaughter']));
  eq([words(w2), w2.spec.engraving.needsPerson], [['Mama', 'Daughter'], false], '"GOLD·Front / Mama / Daughter": Mama and Daughter');
  const w3 = world(mk('ROSEGOLD·BLANK', ['ROSEGOLD·BLANK']));
  eq([w3.spec.engraveCandidate, w3.spec.engraving, w3.spec.problems.length, w3.spec.pieceCount], [false, undefined, 0, 2], 'BLANK with only its own value as "text": nothing to engrave, nothing waits, two plain halves');
  // (a line whose drop-down comes with no value id, so the faces reader takes it for the text box: the profile still reads it as the drop-down)
  const bare = (value, pers) => world(mk(null, pers, { variations: [{ name: OPT, value }] }));
  const b1 = bare('SILVER·Front', ['Mama\nDaughter']), b2 = bare('SILVER·Front', ['Sisters']), b3 = bare('GOLD·BLANK', []);
  eq([b1.spec.engraving.mode, b1.spec.engraving.needsPerson, seeds(b1), b2.spec.engraving.needsPerson, seeds(b2), b3.spec.engraveCandidate, b3.spec.engraving], ['front', false, ['Mama', 'Daughter'], true, [null, null], false, undefined], 'no value id on the drop-down: the same readings (Mama / Daughter; "Sisters" waits; BLANK nothing)'); }

// ── 2 · a note to the shop in brackets is never engraved ─────────────────────────────────────────────────────────────────────────
{ const w = world(mk('SILVER·Front', ['Sisters (Can you use the same font as pictured?)'])), e = w.spec.engraving;
  eq([e.needsPerson, e.reviewKind, e.pieces, seeds(w)], [true, 'pieces', undefined, [null, null]], '"Sisters (Can you use the same font as pictured?)": still one text for two halves, a person says');
  ok(/Say which words go on each half/.test(e.why) && /“Sisters”/.test(e.why) && !/pictured/.test(e.why.split('Say which')[0]), 'the one text is "Sisters": the note is not words: ' + e.why);
  const w2 = world(mk('SILVER·Front', ['Mama / Daughter (Can you use the same font as pictured?)'])), e2 = w2.spec.engraving;
  eq([words(w2), seeds(w2), e2.needsPerson, e2.reviewKind], [['Mama', 'Daughter'], ['Mama', 'Daughter'], true, 'texts'], '"Mama / Daughter (… font as pictured?)": Mama on the left half, Daughter on the right, the note never engraved; a person confirms');
  ok(/note to the shop/.test(e2.why) && /pictured/.test(e2.why), e2.why);
  ok(!seeds(w2).some(t => /font|pictured|\(/.test(t)), 'no half carries the note'); }

// ── 3 · two texts: one per half, in order ─────────────────────────────────────────────────────────────────────────
for (const [pers, want, how] of [
  [['Mama\nDaughter'], ['Mama', 'Daughter'], 'lines'], [['Mama', 'Daughter'], ['Mama', 'Daughter'], 'lines'], [['Mama / Daughter'], ['Mama', 'Daughter'], 'list'], [['Mama & Daughter'], ['Mama', 'Daughter'], 'list'],
  [['Left: Mama Right: Daughter'], ['Mama', 'Daughter'], 'labelled'], [['Right: Daughter\nLeft: Mama'], ['Mama', 'Daughter'], 'labelled'], [['Left half: Mama, Right half: Daughter'], ['Mama', 'Daughter'], 'labelled'],
  [['Half 1: Sis\nHalf 2: ters'], ['Sis', 'ters'], 'numbered'], [['BFF BFF'], ['BFF', 'BFF'], 'list'], [['BFF\nBFF'], ['BFF', 'BFF'], 'lines'], [['Sis / ters'], ['Sis', 'ters'], 'list'], [['BFF on both'], ['BFF', 'BFF'], 'same']]) {
  const w = world(mk('SILVER·Front', pers)), e = w.spec.engraving;
  eq([words(w), seeds(w), w.spec.pieceProfile.how, w.spec.pieceProfile.sure, e.needsPerson, e.mode, Object.keys(e.pieces || {})], [want, want, how, true, false, 'front', ['D1', 'D2']], `${JSON.stringify(pers)}: ${want.join(' / ')} on the left / right half (${how}), nothing asked`);
  ok(/“.+” on the left half, “.+” on the right half/.test(e.why), e.why);
}
// what is NOT read: a person is asked, nothing is guessed
for (const [pers, why] of [[['Sisters'], /one text/], [['BFF'], /one text/], [['Mama\nDaughter\nSister'], /3 names for 2 halves/], [['Left: Mama\nRight: N/A'], /right half has no name/]]) {
  const w = world(mk('SILVER·Front', pers)), e = w.spec.engraving;
  eq([e.needsPerson, w.spec.pieceProfile.sure], [true, false], `${JSON.stringify(pers)}: a person says what goes on each half`);
  ok(why.test(e.why), `${JSON.stringify(pers)}: ${e.why}`);
  if (pers.length === 1 && !/[\n:]/.test(pers[0])) eq(seeds(w), [null, null], `${JSON.stringify(pers)}: one text is never copied onto both halves, nor onto one of them`); }
{ const w = world(mk('SILVER·Front', ['Mama\nDaughter'], { buyerMessage: 'Could the left half be bigger?' })); eq([w.spec.engraving.needsPerson, w.spec.engraving.reviewKind, seeds(w)], [true, 'texts', ['Mama', 'Daughter']], 'a message beside the words: a person confirms, the words stay as the proposal'); }
// the reader itself: "Left half:" is a side label now; "Left ear:" as before
eq([PW.read(['Left half: A, Right half: B'], 2, { noun: 'half' }).words, PW.read(['Left ear: A\nRight ear: B'], 2).words, PW.read(['Left: A Right: B'], 2).words], [['A', 'B'], ['A', 'B'], ['A', 'B']], 'the words reader: "Left half:" and "Left ear:" are side labels');

// ── 4 · the 9 values of the drop-down ─────────────────────────────────────────────────────────────────────────
eq(DROPDOWN.length, 9, 'nine values');
for (const v of DROPDOWN) {
  const metal = v.split('·')[0].trim(), face = /front\s*&\s*back/i.test(v) ? 'both' : /front/i.test(v) ? 'front' : 'none', blank = face === 'none';
  eq(O.pieceFacesRead(v, O.PIECE_LISTINGS.find(x => x.id === DESIGN)), { ok: true, n: 2, metal: METALKEY[metal], mode: face, why: '' }, `${v}: read as ${METALKEY[metal]}, ${face}`);
  const w = world(mk(v, blank ? [] : ['Mama\nDaughter'], { metalKey: '', metalLabel: '' }));
  eq([w.spec.problems.length, w.spec.designSku, w.spec.pieceCount, w.spec.material, O.CARD_TAG[w.spec.material], w.spec.pair.pieceBodies], [0, DESIGN, 2, METALKEY[metal], CODE[metal], [0, 1]], `${v}: 2 halves, ${CODE[metal]} from the drop-down (no station metal), nothing asked`);
  if (face === 'front') eq([w.spec.engraving.mode, w.spec.engraving.needsPerson, w.faceJobs, seeds(w)], ['front', false, [{ slot: 'D1', face: 'front' }, { slot: 'D2', face: 'front' }], ['Mama', 'Daughter']], `${v}: the front of each half, its own words`);
  if (face === 'both') {
    eq([w.spec.engraving.mode, w.spec.engraving.needsPerson, w.spec.engraving.reviewKind, w.faceJobs.length, seeds(w), seeds(w, 'back')], ['both', true, 'texts', 4, ['Mama', 'Daughter'], [null, null]], `${v}: the front and the back of each half (4 jobs); the words go on the fronts, a person says what goes on each back`);
    ok(/a person says what goes on each back/.test(w.spec.engraving.why), w.spec.engraving.why); }
  if (blank) eq([w.spec.engraveCandidate, w.spec.engraving, w.spec.pieceProfile.face, w.spec.pieceProfile.sure, w.spec.problems.length], [false, undefined, 'none', true, 0], `${v}: nothing written, nothing engraved, nothing waits`);
}
{ const w = world(mk('GOLD·BLANK', ['Mama\nDaughter'])), e = w.spec.engraving;
  eq([e.mode, e.needsPerson, e.reviewKind, e.pieces, w.spec.pieceProfile.face], ['none', true, 'conflict', undefined, 'none'], 'BLANK with words written beside it: the conflict a person decides (as on every listing), no words on any half'); }
for (const v of ['SILVER·Engraved', 'PLATINUM·Front', 'SILVER·Front·Back·BLANK']) eq(O.pieceFacesRead(v, O.PIECE_LISTINGS.find(x => x.id === DESIGN)).ok, false, `${v}: not one of the choices`);
{ const w = world(mk('SILVER·Engraved', ['Mama\nDaughter'])); eq([w.spec.pieceCount, w.spec.pair.pieceBodies, w.spec.problems.filter(p => p.kind === 'needsMapping').length], [2, [0, 1], 0], 'a value the profile does not read: still two halves (the count is the design\'s), the faces are read as on any line'); }

// ── 5 · quantity 2: two hearts, 4 halves ─────────────────────────────────────────────────────────────────────────
{ const w = world(mk('SILVER·Front', ['Mama\nDaughter\nSis\nters'], { quantity: 2 }));
  eq([w.spec.problems.length, w.spec.pieceCount, w.pieces.length, O.pieceBodiesOf(w.spec)], [0, 4, 4, [0, 1, 0, 1]], 'quantity 2: 4 pieces, 2 left halves and 2 right halves');
  eq([w.plan.slots, seeds(w)], [['D1', 'D2', 'D1.2', 'D2.2'], ['Mama', 'Daughter', 'Sis', 'ters']], 'four words, in the order of the halves');
  eq(O.countedFact({ spec: w.spec, line: w.row.line }), ' · 4 halves', '4 halves');
  eq(S.pieceList(SC, w.row).map(p => p.label), ['Left half · set 1', 'Right half · set 1', 'Left half · set 2', 'Right half · set 2'], 'each heart: its left half and its right half');
  const w2 = world(mk('SILVER·Front', ['Mama\nDaughter'], { quantity: 2 }));
  eq([w2.spec.engraving.needsPerson, w2.spec.engraving.reviewKind], [true, 'pieces'], 'two words for two hearts: which heart gets which is for a person'); }

// ── 6 · which lines the row covers ─────────────────────────────────────────────────────────────────────────
{ for (const s of ['Best_4003', 'BEST_4003', 'BEST 4003', 'best-4003']) ok(O.pieceListing({ listingId: '', sku: s }) === O.PIECE_LISTINGS.find(x => x.id === DESIGN), `pieceListing: ${s} is the row's (looseKey)`);
  const noDrop = world(mk(null, ['Mama\nDaughter'], { variations: [] }));
  eq([noDrop.spec.pieceCount, noDrop.spec.pair.pieceBodies, noDrop.spec.engraving.mode, seeds(noDrop)], [2, [0, 1], 'front', ['Mama', 'Daughter']], 'a line of the SKU with no drop-down is still the two halves of the design, front by the row\'s default');
  const other = world({ transactionId: '9', listingId: '555', sku: 'HEART_1111', title: 'Heart Necklace', quantity: 1, metalKey: 'silver', personalization: ['Mama\nDaughter'], variations: [V(OPT, 'SILVER·Front')] });
  eq([other.spec.designSku, other.spec.pieceCount, other.spec.pair.pieceBodies, other.spec.pair.labels, other.spec.pieceProfile], ['HEART_1111', 1, undefined, undefined, undefined], 'another SKU with the same drop-down is not this profile\'s: one piece, as before');
  eq([O.piecesFixed(O.PIECE_LISTINGS[0]), O.piecesFixed(O.PIECE_LISTINGS[1]), O.pieceNouns(O.PIECE_LISTINGS[0]), O.pieceNouns(O.PIECE_LISTINGS.find(x => x.id === DESIGN))], [null, null, 'bars', 'halves'], 'piecesFixed / pieceNouns: the bars and the stars are counted, not fixed'); }

// ── 7 · the pool (the bridge's real Pool in a vm): each half from ITS body ─────────────────────────────────────────────────────────────────────
const K = require('./_keyWorld.cjs')(root);
const bboxOf = pts => [Math.min(...pts.map(p => p[0])), Math.min(...pts.map(p => p[1])), Math.max(...pts.map(p => p[0])), Math.max(...pts.map(p => p[1]))];
const cut = (pts, closed) => ({ kind: 'path', closed: closed !== false, stroke: true, fill: false, layer: 'CUT', strokeRGB: [0, 0, 0], lwPt: .25, bbox: bboxOf(pts), subpaths: [pts.map((p, i) => [i ? 'l' : 'm', p]).concat(closed === false ? [] : [['l', pts[0]]])] });
// a heart (x = 16 sin^3 t, y = 13 cos t - 5 cos 2t - 2 cos 3t - cos 4t), 1.2 pt a unit, centred at (24, 22): t 0..pi is its RIGHT side (top notch to the tip), pi..2pi its LEFT side
const HX = t => 24 + 1.2 * 16 * Math.pow(Math.sin(t), 3), HY = t => 22 + 1.2 * (13 * Math.cos(t) - 5 * Math.cos(2 * t) - 2 * Math.cos(3 * t) - Math.cos(4 * t));
const arc = (a, b, k) => Array.from({ length: k + 1 }, (_, i) => { const t = a + (b - a) * i / k; return [HX(t), HY(t)]; });
// the zigzag from the top notch (y 28) down to the tip (y 1.6), its teeth 1.4 pt wide, a little right of the middle (ZC 25: the halves are not mirror images, as on the real charm); `dx` moves it (the kerf between the halves)
const ZC = 25, zig = (dx, down) => { const out = []; for (let i = 0; i <= 10; i++) { const y = 28 - (28 - 1.6) * i / 10, a = i === 0 || i >= 9 ? 0 : (i % 2 ? 1.4 : -1.4); out.push([(i === 0 || i === 10 ? 24 : ZC) + a + dx, y]); } return down ? out : out.reverse(); };
const hole = (cx, cy) => Array.from({ length: 12 }, (_, i) => [cx + 1.3 * Math.cos(i * Math.PI / 6), cy + 1.3 * Math.sin(i * Math.PI / 6)]);
const charmOf = (sku, members) => { const bb = members.reduce((u, m) => [Math.min(u[0], m.bbox[0]), Math.min(u[1], m.bbox[1]), Math.max(u[2], m.bbox[2]), Math.max(u[3], m.bbox[3])], [1e9, 1e9, -1e9, -1e9]); return { sku, outline: members[0], members, bbox: bb, widthPt: bb[2] - bb[0], heightPt: bb[3] - bb[1], areaPt2: (bb[2] - bb[0]) * (bb[3] - bb[1]) }; };
// (a) the master draws the two halves as TWO closed outlines meeting at the zigzag (a hair of kerf between them), each with its own hole, each its own drawing group
K.DESIGNS[DESIGN] = () => {
  const left = cut(arc(Math.PI, 2 * Math.PI, 40).map(([x, y]) => [Math.min(x, 23.6), y]).concat(zig(-0.4, true))), right = cut(arc(0, Math.PI, 40).map(([x, y]) => [Math.max(x, 24.4), y]).concat(zig(0.4, false)));   // (the left half: its side of the heart, then down the zigzag; the right half: its side, then up the zigzag)
  const hl = cut(hole(14, 29)), hr = cut(hole(34, 29));
  return charmOf(DESIGN, [Object.assign(left, { parent: 0 }), Object.assign(right, { parent: 1 }), Object.assign(hl, { parent: 0 }), Object.assign(hr, { parent: 1 })]);
};
// (b) the master draws ONE heart outline with the zigzag as an open line inside it
K.DESIGNS.BEST_ONE = () => charmOf('BEST_ONE', [Object.assign(cut(arc(0, 2 * Math.PI, 80)), { parent: 0 }), Object.assign(cut(zig(0, true), false), { parent: 0 }), Object.assign(cut(hole(14, 29)), { parent: 0 }), Object.assign(cut(hole(34, 29)), { parent: 0 })]);
(async () => {
  { const b = P.bodiesOf(K.DESIGNS[DESIGN]()), one = P.bodiesOf(K.DESIGNS.BEST_ONE());
    eq([b.length, P.isMismatched(K.DESIGNS[DESIGN]()), b[0].bbox[0] < b[1].bbox[0], one.length], [2, true, true, 1], 'the test drawings: (a) two different bodies, the left one first by x; (b) one body'); }
  const pool = async (value, qty, design) => {
    const d = design || DESIGN, w = K.world([d]); w.use(d);
    const entry = w.entries[d], line = mk(value, ['Mama\nDaughter'], { quantity: qty || 1, metalKey: 'silver' });
    const sp = O.interpretLine({ receiptId: RID, updateTs: 1 }, line, ctx({ masterEntry: s => (O.looseKey(s) === 'BEST4003' ? entry : null) }));
    const row = { key: `${RID}_${TX}`, state: 'pulled', order: { receiptId: RID, createTs: 1, updateTs: 1 }, line, spec: Object.assign({}, sp, { designSku: d, material: 'gold', form: 'necklace' }), problems: [], poolIds: [], engrave: { needed: true, state: 'classify', approved: false } };
    await w.Pool.poolAdd(row, null);
    const put = w.calls.find(x => x.body.op === 'poolPut');
    return { w, row, sp, pools: put ? put.body.pools : [], charms: w.pages.gold.charms, bodies: P.bodiesOf(K.DESIGNS[d]()) };
  };
  { const t = await pool('SILVER·Front'), J = x => JSON.parse(JSON.stringify(x));
    eq([t.sp.pair.pieceBodies, t.row.state, t.pools.length, t.charms.length], [[0, 1], 'pooled', 2, 2], 'the real order makes two pool pieces');
    eq(t.charms.map(c => J(c.bbox)), [J(t.bodies[0].bbox), J(t.bodies[1].bbox)], 'piece 1 is the LEFT half alone, piece 2 the RIGHT half alone');
    eq([t.charms.map(c => c.oneBody), t.charms.map(c => c.members.length)], [[0, 1], [2, 2]], 'each charm is ONE body of the design (its outline and its own hole), never both halves');
    ok(t.pools[0].charmHash !== t.pools[1].charmHash && t.pools.every((p, i) => p.charmHash === t.charms[i].hash), 'the stored hash is each half\'s own body');
    eq(t.pools.map(p => [p.groupKey, p.groupSize]), [0, 1].map(() => [RID + ':' + TX, 2]), 'one group of two: both halves go on the same sheet');
    eq([t.pools.map(p => p.engrave), t.pools.map(p => p.sku)], [[true, true], [DESIGN, DESIGN]], 'both halves are engraved pieces of BEST_4003');
    const place = await t.w.Pool.ensureBase(t.w.c.B.pool.sources.get('charmnest/master/sku/' + DESIGN + '.ai'), null, { charmHash: t.pools[1].charmHash });
    eq(J(place.bbox), J(t.bodies[1].bbox), 'a reload reads the right half\'s stored hash back as the right body (soloBase)'); }
  { const t = await pool('GOLD·Front', 2);
    eq([t.pools.length, t.charms.map(c => c.oneBody), t.pools.map(p => p.groupSize)], [4, [0, 1, 0, 1], [4, 4, 4, 4]], 'bought twice: two hearts, each a left half and a right half'); }
  { const t = await pool('SILVER·Front', 1, 'BEST_ONE');
    eq([t.row.state, t.pools.length, t.charms.length], ['held', 0, 0], '(b) one heart outline with the zigzag inside: the line waits, never two copies of the whole heart, never one heart');
    ok(/separate bodies this listing's profile cuts \(the left half and the right half, each its own closed outline\)[^:]*?(?: — [^:]*?\([^)]*\))?: a person checks the design/.test(t.row.reason), t.row.reason); }

  // ── 8 · what must not change ─────────────────────────────────────────────────────────────────────────
  { const bar = world({ transactionId: '7', listingId: '982791277', sku: 'VERTICAL BAR_1277', title: 'Skinny MINI Vertical Bar Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['Bar names:\nSophie\nSam\nEddy'], variations: [{ name: 'Engraving & # of Bars?', value: 'gold • 3 bars ($45.60)' }] });
    eq([bar.spec.problems.length, bar.spec.pieceCount, bar.spec.pair.noun, bar.spec.pair.labels, bar.spec.pair.nouns, bar.spec.pair.pieceBodies, bar.spec.pieceProfile.words], [0, 3, 'bar', undefined, undefined, undefined, { D1: 'Sophie', D2: 'Sam', D3: 'Eddy' }], 'the bar profile: 3 bars, no labels, no bodies');
    eq([O.countedFact({ spec: bar.spec, line: bar.row.line }), S.pieceList(SC, bar.row).map(p => p.label), S.labelsOf(bar.row)], [' · 3 bars', ['Disc 1', 'Disc 2', 'Disc 3'], null], 'its words and its piece list are what they were');
    const moon = world({ transactionId: '8', listingId: '1095123288', sku: 'MOON_3288', title: 'Moon Necklace', quantity: 1, metalKey: 'silver', metalLabel: 'Silver', personalization: ['First Star: M\nSecond Star: T'], variations: [{ name: 'Necklace Options', value: 'Silver • 2 stars ($43.20)' }] });
    eq([moon.spec.problems.length, moon.spec.pieceCount, moon.spec.pair.pieceBodies, moon.spec.pair.labels, S.pieceList(SC, moon.row).map(p => p.label), O.countedFact({ spec: moon.spec, line: moon.row.line })], [0, 3, [0, 0, 1], undefined, ['Star 1', 'Star 2', 'Moon'], ' · 2 stars + 1 moon'], 'the moon and stars: as before');
    const one = world({ transactionId: '78', listingId: '555', sku: 'HEART_1111', title: 'Heart Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['Anna'], variations: [{ name: 'Metal choice', value: 'Gold' }] });
    eq([one.spec.problems.length, one.spec.pieceCount, one.spec.pair.kind, one.spec.pair.labels, one.plan.split], [0, 1, 'single', undefined, false], 'a plain single charm: one piece'); }

  // ── 9 · the table, the page ─────────────────────────────────────────────────────────────────────────
  { const row = O.PIECE_LISTINGS.find(x => x.id === DESIGN);
    ok(row && Object.isFrozen(row) && Object.isFrozen(row.pieces) && row.pieces.every(Object.isFrozen) && row.pieces.map(p => p.label).join() === 'Left half,Right half' && row.pieces.map(p => p.body).join() === '0,1' && row.face === 'front' && row.noun === 'half' && row.nouns === 'halves' && !row.design && !row.companion && row.listings.length === 0, 'one frozen row: the left half body 0, the right half body 1, front, no design swap, no listing id yet');
    for (const v of DROPDOWN) ok(row.value.test(v), `the row's value pattern finds “${v}”`);
    ok(!row.value.test('Sisters') && !row.value.test('Mama & Daughter') && !row.value.test('Front: Sisters'), 'and never the buyer\'s words');
    const src = fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8');
    ok(/BROKENHEART \(Paul, 19 Oct 2026, order 4204452679\)/.test(src) && /WHEN THE LISTING ID IS KNOWN, add it to the row's `listings`/.test(src) && /TO UNDO: delete the row/.test(src), 'the rule is written in the code, with where to add the listing id and how to undo it');
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
    for (const f of ['charm-nest-orders.js', 'charm-nest-engrave-piece-words.js', 'charm-nest-engrave-sides.js', 'charm-nest-bridge.js', 'charm-nest-sheetwin.js', 'charm-nest-order-facts.js']) {
      const m = new RegExp('<script src="' + f.replace(/[.]/g, '\\.') + '\\?v=([^"]*)"').exec(html);
      ok(m && /-bh1(-|$)/.test(m[1]), `${f}: cache token carries -bh1`); } }

  console.log(`broken-heart-profile: ${n} checks passed`);
})().catch(e => { console.error(e); process.exit(1); });
