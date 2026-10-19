// FRONTDEFAULT (Paul, 19 Oct 2026, order 4205982652): Etsy listing 1830130714, "Tiny Bar Necklace Custom Engraved Jewelry ..." (SKU BEADY), is engraved on the FRONT by default:
// "This order should always have engraving by default as the Front option. Make sure this listing gets profiled and correctly shows the engraving on the Front by default."
// Offline: no Etsy, no live read, no write, no paid call.
//   node tests/charm-nest/front-default-listing.cjs
// What it proves (charm-nest-orders.js LISTING_FACES / listingFaceOf, charm-nest-engrave-faces.js setupOf / planFaces / seedFor, charm-nest-engrave-sides.js plan):
//   · the order's line ("Layla 3HEARTS Skye", options "METAL - ENGRAVING" / "LENGTH / FONT / ALIGNMENT", neither naming a face) is FRONT only, the words exactly as written, no question
//   · many phrasings (several names, lines, symbols, a font request, "... to the moon and back") stay FRONT, no question
//   · a face the order names (a back label, front and back labels, a Front / Back drop-down) is read as on any other listing; another listing's identical line is unchanged (the shop's usual back)
const assert = require('assert');
const O = require('../../charm-nest-orders.js');
const P = require('../../charm-nest-pair.js');
const S = require('../../charm-nest-engrave-sides.js');
const F = require('../../charm-nest-engrave-faces.js');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const RID = '4205982652', LISTING = '1830130714', SKU = 'BEADY';
const TITLE = 'Tiny Bar Necklace Custom Engraved Jewelry Dainty Necklace Gift for Women Personalized Necklace for Everyday Wear Jewelry for her';
const entryFor = s => (String(s || '').toUpperCase() === SKU ? { sku: s, widthPt: 73.4, heightPt: 14 } : null);
const ctx = { optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: () => '' };
const VARS = [{ name: 'METAL - ENGRAVING', value: 'GOLD·6-10·Characters' }, { name: 'LENGTH / FONT / ALIGNMENT', value: '16"·PRISTINA·Center' }];
const mk = (pers, o) => Object.assign({ transactionId: '5005982652', listingId: LISTING, sku: SKU, title: TITLE, quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: pers, variations: VARS }, o || {});
function world(line) {
  const order = { receiptId: RID, updateTs: 0 }, spec = O.interpretLine(order, line, ctx);
  const row = { key: `${RID}_${line.transactionId}`, order, line, spec, state: 'pulled' };
  const pieces = P.piecesFor(Object.assign({}, line, { receiptId: RID, quantity: spec.quantity, form: spec.form, spec }), null);
  row.poolIds = pieces.map((p, i) => `${row.key}_${i + 1}`);
  const plan = S.plan({ pair: P, entryFor, poolRow: () => null, charmOf: () => null }, row, []), setup = F.setupOf(row);
  return { spec, row, plan, setup, jobs: F.planFaces(setup, plan.slots) };
}
const frontOnly = (w, words, m) => {
  eq([w.spec.engraving && w.spec.engraving.mode, w.spec.engraving && w.spec.engraving.source], ['front', 'profile'], m + ': the engraving is the listing profile\'s front');
  eq([w.setup.mode, w.setup.faces, w.setup.needsPerson, F.modeLabel(w.setup.mode)], ['front', ['front'], false, 'Front only'], m + ': Front only, no question');
  eq(w.jobs.map(j => j.face), ['front'], m + ': one front job, no back');
  eq((F.seedFor(w.setup, 'front') || {}).lines, words, m + ': the front starts with the buyer\'s words as read');
  ok(/front only from this listing's profile/.test(w.setup.why), m + ': the why says the profile');
};

// ── 1 · the order of the screenshot ─────────────────────────────────────────────────────────────────────────────────────────
{ const w = world(mk(['Layla 3HEARTS Skye']));
  frontOnly(w, ['Layla 3HEARTS Skye'], 'order 4205982652');
  eq(w.spec.problems.filter(p => p.kind === 'needsMapping'), [], 'no option is asked of a person');
  ok(w.spec.engraveCandidate && !w.spec.engraveNever, 'it has words to engrave');
  // "3HEARTS" is engraved as written: no rule of the app turns a word into heart symbols
  eq(w.spec.engraving.front.text, 'Layla 3HEARTS Skye', '"3HEARTS" stays as the buyer wrote it');
}

// ── 2 · a variety of phrasings: always the front, no question ─────────────────────────────────────────────────────────────────
for (const [pers, words] of [
  [['Layla'], ['Layla']],
  [['Layla\nSkye\nMia'], ['Layla', 'Skye', 'Mia']],
  [['Layla, Skye & Mia'], ['Layla, Skye & Mia']],
  [['Layla / Skye'], ['Layla / Skye']],
  [['Mom ❤ ♥♥♥'], ['Mom ❤ ♥♥♥']],
  [['L ♡ S 3HEARTS'], ['L ♡ S 3HEARTS']],
  [['12.05.2019'], ['12.05.2019']],
  [['Typewriter font\nLayla'], ['Layla']],
  [['Love you to the moon and back'], ['Love you to the moon and back']]
]) frontOnly(world(mk(pers)), words, JSON.stringify(pers[0]));
{ const w = world(mk(['Layla'], { quantity: 2 })); eq([w.setup.mode, w.setup.needsPerson], ['front', false], 'a line bought twice: front, no question'); }

// ── 3 · words the shop must confirm keep a person on the WORDS, never on the face ───────────────────────────────────────────────
{ const w = world(mk(['Emma (please make it big)']));
  eq([w.setup.mode, w.setup.faces, w.setup.needsPerson, w.setup.reviewKind], ['front', ['front'], true, 'texts'], 'a note to the shop in brackets: front, a person confirms the words');
  eq((w.setup.front || {}).lines, ['Emma'], 'the note is not engraved'); }
{ const w = world(mk([], { buyerMessage: 'Please engrave Layla' }));
  ok(w.setup.faces.includes('front') && !w.setup.faces.includes('back'), 'words only in a message: still the front'); }

// ── 4 · a face the order names is read as on any other listing ─────────────────────────────────────────────────────────────────
{ const w = world(mk(['On the back: Love you'])); eq([w.setup.mode, w.spec.engraving.source], ['back', 'note'], '"On the back:" stays the back (as today)'); }
{ const w = world(mk(['Back: love you to the moon and back'])); eq(w.setup.mode, 'back', 'a back label with the idiom: the back'); }
{ const w = world(mk(['front: A back: B'])); eq([w.setup.mode, (w.setup.front || {}).text, (w.setup.back || {}).text], ['both', 'A', 'B'], 'front and back labels: both, as today'); }
{ const w = world(mk(['Both sides: Layla'])); eq(w.setup.mode, 'both', '"Both sides" stays both (as today)'); }
{ const w = world(mk(['Layla'], { variations: VARS.concat([{ name: 'Engraving Side', value: 'Back', valueId: '777' }]) }));
  eq([w.setup.mode, w.spec.engraving.source], ['back', 'option'], 'a Front / Back / Both drop-down still decides: Back'); }
{ const w = world(mk(['Layla'], { variations: VARS.concat([{ name: 'Engraving Side', value: 'Front & Back', valueId: '778' }]) }));
  eq([w.setup.mode, w.spec.engraving.source], ['both', 'option'], 'a Front / Back / Both drop-down still decides: Both'); }

// ── 4b · listing 1008014571, the Initial Disc Necklace (order 4204843889: its card said "Back: B"): front by default, each disc its own letter on its own front ─────────────
{ const DTITLE = 'Initial Disc Necklace Gold Disc Personalized Jewelry Gift for Mom Letter Necklace Gold Initial Charm Personalized Disc Pendant';
  const disc = (n, pers, extra) => mk(pers, Object.assign({ transactionId: '5004843889', listingId: '1008014571', sku: 'nitial_Disc_4571', title: DTITLE, metalKey: 'rose', variations: [{ name: 'Necklace Options', value: `ROSEGOLD - ${n} Disc` }, { name: 'Fonts', value: '16"/ Typewriter' }] }, extra || {}));
  const one = world(disc(1, ['B']));
  frontOnly(one, ['B'], 'order 4204843889 (1 disc, "B")');
  for (const pers of [['A\nB'], ['A, B'], ['Disc 1: A\nDisc 2: B']]) {
    const w = world(disc(2, pers));
    eq([w.spec.pieceCount, w.setup.mode, w.setup.faces, w.setup.needsPerson], [2, 'front', ['front'], false], `2 discs ${JSON.stringify(pers[0])}: front only, no question`);
    eq([w.setup.pieces.D1.front.text, w.setup.pieces.D2.front.text, w.setup.pieces.D1.back, w.setup.pieces.D2.back], ['A', 'B', null, null], `2 discs ${JSON.stringify(pers[0])}: A on the front of disc 1, B on the front of disc 2`);
    eq([F.seedFor(w.setup, 'front', 'D1').text, F.seedFor(w.setup, 'front', 'D2').text], ['A', 'B'], 'each disc\'s front job starts with its own letter');
  }
  { const w = world(disc(2, ['B'])); eq([w.setup.mode, w.setup.faces, w.setup.needsPerson, w.setup.reviewKind], ['front', ['front'], true, 'pieces'], '2 discs, one letter: still the front, a person says what goes on each disc'); }
  { const w = world(disc(1, ['Back: B'])); eq(w.setup.mode, 'back', 'an explicit back label on the disc listing still decides'); }
  { const w = world(disc(1, ['B'], { listingId: '2222222222' })); eq(w.setup.mode, 'back', 'the same disc line on another listing: the usual back'); }
}

// ── 5 · another listing is unchanged ───────────────────────────────────────────────────────────────────────────────────────────
for (const pers of [['Layla 3HEARTS Skye'], ['Love you to the moon and back']]) {
  const w = world(mk(pers, { listingId: '1111111111' }));
  eq([w.setup.mode, w.setup.source === 'profile'], ['back', false], `another listing ${JSON.stringify(pers[0])}: the shop's usual back, as before`);
}

console.log(`front-default-listing: ${n} checks passed`);
