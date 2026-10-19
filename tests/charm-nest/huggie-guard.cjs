// HUGGIEGUARD (Paul, 12 Oct 2026 15:08 UTC, order 4198409468, Etsy SKU "SCHRODINGER CAT", Jewellery: Necklace, Silver, 18"): "This order is a dual piece/charm necklace and the SKU you
// currently have associated with is for the (Huggie) version of this charm. Since this is a necklace piece how did this get past your safeguards? This should be an obvious one."
//
// THE CAUSE (charm-nest-orders.js, read with the real code, section 1 below): resolveSku takes a transaction SKU that is a master SKU as it stands. The master's plain "SCHRODINGER CAT" is
// the same drawing (one design file, charmnest/master/SCHRODINGER_CAT.ai, 21.2 x 24.3 pt) as "SCHRODINGER CAT 2 (HUGGIE)": the library keeps one drawing under several names. Nothing in the
// NAME says huggie, and the only form check there was (skuFamilyConflict) reads the words NECKLACE / EARRING / STUD / HUGGIE / HOOP in the SKU text, on the paths that come from Etsy's inventory
// table. So the necklace line took the huggie drawing with no question. The necklace charms are the numbered SCHRODINGER CAT 1 and SCHRODINGER CAT 2 (29 to 31 pt).
//
// THE FIX (section 3b of charm-nest-orders.js, the form guard): every master entry and every line gets a FORM (huggie | earring | necklace | unknown) from signals that already exist; an automatic
// match never crosses forms; a clash is held with ONE plain line and the siblings that fit (spec.siblings / spec.formHold); a person's saved answer always wins. Offline: no network, no Etsy, no AI.
//   node tests/charm-nest/huggie-guard.cjs
const assert = require('assert/strict'), path = require('path'), fs = require('fs');
const root = path.join(__dirname, '../..');
const O = require(path.join(root, 'charm-nest-orders.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const step = name => console.log('  ✓', name);

/* ── the world: the real SCHRODINGER entries (design file, size: read from the live public masterGet on 12 Oct 2026) and the kinds of entries around them ── */
const SCHR = 'charmnest/master/SCHRODINGER_CAT.ai', SCHR_H1 = 'charmnest/sandbox/master/HUGGIE_HOOPS-_SCHRONDINGER_CAT.ai';
const E = [
  ['SCHRODINGER CAT', SCHR, 21.1688, 24.286], ['SCHRODINGER CAT 2 (HUGGIE)', SCHR, 21.1688, 24.286],                                   // the huggie skull cat, under two names
  ['SCHRODINGER CAT 1', 'charmnest/sandbox/master/SCHRODINGER_CAT_1.ai', 29.42435, 28.3021], ['SCHRODINGER CAT 2', 'charmnest/sandbox/master/SCHRODINGER_CAT_2.ai', 31.0699, 29.2093],   // the necklace charms (10.4 x 10.0 and 11.0 x 10.3 mm)
  ['SCHRODINGER CAT 1 (HUGGIE)', SCHR_H1, 20.121, 24.426], ['HUGGIE HOOPS- SCHRONDINGER CAT', SCHR_H1, 20.121, 24.426],
  ['BUNNY_42980', 'charmnest/master/BUNNY_42980.ai', 31, 33], ['BUNNY_42980 (HUGGIE)', 'charmnest/master/BUNNY_42980_HUGGIE_.ai', 24, 27],   // a design drawn twice: the necklace charm and, apart, its huggie
  ['CROWN 2', 'charmnest/sandbox/master/CROWN_2.ai', 34.8, 30.7], ['QUEEN_7155', 'charmnest/sandbox/master/CROWN_2.ai', 34.8, 30.7], ['REGAL_64774', 'charmnest/sandbox/master/CROWN_2.ai', 34.8, 30.7], ['ROYAL_77500 (HUGGIE)', 'charmnest/sandbox/master/CROWN_2.ai', 34.8, 30.7],   // six plain names and one huggie name on one drawing: not a huggie
  ['HEART_5193', 'charmnest/master/HEART_CUPID_HUGGIE_.ai', 25, 28.3], ['HEART CUPID (HUGGIE)', 'charmnest/master/HEART_CUPID_HUGGIE_.ai', 25, 28.3],
  ['GREEKGODDESS_NECKLACE_4', 'charmnest/master/GG_N4.ai', 30, 34], ['GREEKGODDESS_EARRING_4', 'charmnest/master/GG_E4.ai', 20, 24],
  ['MAPLE_8065', 'charmnest/master/MAPLE_8065.ai', 30, 33], ['MAPLE_8065 (HUGGIE)', 'charmnest/master/MAPLE_8065_HUGGIE_.ai', 22, 25],
  ['HUGGIE_3722', 'charmnest/master/HUGGIE_HOOPS-_LADYBUG_1.ai', 23, 25], ['PLAIN_ONE', 'charmnest/master/PLAIN_ONE.ai', 30, 30],
  ['ZODIAC_EARRINGS-0', 'z0.ai', 20, 20], ['ZODIAC_EARRINGS-1', 'z1.ai', 20, 20], ['ZODIAC_EARRINGS-2', 'z2.ai', 20, 20], ['ZODIAC_EARRINGS-3', 'z3.ai', 20, 20],
  ['PEARL_STUD_5', 'charmnest/master/PEARL_STUD_5.ai', 15, 15], ['HOOPS-CELESTIAL (MOON+STARS)', 'charmnest/master/HOOPS_CEL.ai', 24, 28]
];
const lib = new Map(E.map(([sku, aiPath, widthPt, heightPt]) => [sku, { sku, aiPath, widthPt, heightPt, members: 3, sizes: null, blocked: null }]));
for (let i = 0; i < 26; i++) lib.set('LETTER_EARRING-' + i, { sku: 'LETTER_EARRING-' + i, aiPath: 'l' + i + '.ai', sizes: null, blocked: null });
const byFile = new Map(); for (const [k, e] of lib) if (e.aiPath) { if (!byFile.has(e.aiPath)) byFile.set(e.aiPath, []); byFile.get(e.aiPath).push(k); }
const loose = {}; for (const k of lib.keys()) { const l = O.looseKey(k); loose[l] = l in loose ? '' : k; }
const entryFor = s => lib.get(String(s || '').toUpperCase()) || null, sharedFor = s => { const e = entryFor(s); return e && e.aiPath ? byFile.get(e.aiPath) || [] : []; };
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: s => loose[O.looseKey(s)] || '', masterShared: sharedFor }, extra || {});
let tx = 7000000000;
const line = (listing, sku, title, vars, extra) => Object.assign({ transactionId: String(++tx), listingId: String(listing), sku, title, quantity: 1, metalKey: /silver/i.test(JSON.stringify(vars)) ? 'silver' : 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: vars.map(([name, value]) => ({ name, value })) }, extra || {});
const read = (l, extra) => O.interpretLine({ receiptId: 'R' + l.transactionId, updateTs: 1 }, JSON.parse(JSON.stringify(l)), ctx(extra));
const held = sp => sp.problems.find(p => p.formHold);
const NECK = [['Metal Choice', 'Silver'], ['Necklace Length in inches', '18"']];
// Paul's order: the listing photo is a necklace with two cat charms; the title is not on the screenshot (the row shows only "SCHRODINGER CAT"), the app reads Jewellery: Necklace from the title and the length option
const PAUL = () => line('1000000001', 'SCHRODINGER CAT', 'Schrodinger\'s Cat Necklace, Dual Cat Charm Necklace, Silver or Gold Filled', NECK);

/* ── 1 · the cause, told with the real code: the NAME carries no form; the library's design file does ── */
function cause() {
  ok(!O.formOfText('SCHRODINGER CAT'), 'no word of the SKU says huggie, earring or necklace');
  // without the library's facts about design files (an older caller, or the SKU text alone) the line reads exactly as before: the plain SKU is the design (the old reading)
  const old = read(PAUL(), { masterShared: undefined });
  eq([old.designSku, old.skuSource, old.problems.length], ['SCHRODINGER CAT', 'transaction', 0], 'text alone cannot tell: this is the old reading');
  // the library's own facts: the same drawing is named "SCHRODINGER CAT 2 (HUGGIE)" too
  eq(O.masterForm('SCHRODINGER CAT', { masterEntry: entryFor, masterShared: sharedFor }), { form: 'huggie', by: 'shared' });
  eq(O.masterForm('SCHRODINGER CAT 2 (HUGGIE)', { masterEntry: entryFor }), { form: 'huggie', by: 'text' });
  ok(O.designFile(entryFor('SCHRODINGER CAT')) === O.designFile(entryFor('SCHRODINGER CAT 2 (HUGGIE)')), 'one design file under two names');
  eq(O.masterForm('SCHRODINGER CAT 1', { masterEntry: entryFor, masterShared: sharedFor }), { form: 'necklace', by: 'twin', soft: true }, 'the numbered necklace charm is no huggie (it has a huggie drawn apart)');
  step('cause: nothing in the SKU name says huggie; "SCHRODINGER CAT" is the drawing "SCHRODINGER CAT 2 (HUGGIE)" is (one design file), and no form check looked at the design');
}

/* ── 2 · Paul's order now waits, with one plain line and both numbered charms offered ── */
function paulsOrder() {
  const sp = read(PAUL()), h = held(sp);
  eq([sp.lineForm, sp.designForm], ['necklace', 'huggie']);
  ok(h, 'the line is held'); eq(h.kind, 'unmatchedSku', 'the existing kind: Review, readiness and the activity log already know it');
  eq(sp.designSku, null, 'no vector of the huggie drawing on the row'); eq(sp.skuSource, null); eq(sp.boughtSku, 'SCHRODINGER CAT', 'the saved answer is keyed by the SKU as bought');
  eq(h.reason, 'SKU “SCHRODINGER CAT” is a huggie design in the library, but this listing is a necklace', 'ONE plain line');
  eq(sp.problems[0], h, 'the plain line leads');
  eq(sp.formHold.siblings.map(s => s.sku), ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2'], 'both numbered charms, ascending');
  eq(sp.siblings, sp.formHold.siblings); eq(h.siblings, sp.siblings);
  eq(sp.siblings.map(s => [s.n, s.widthPt > 29]), [[1, true], [2, true]], 'numbers and sizes ride along (the necklace charms are 29 to 31 pt, the huggie skull cat 21 pt)');
  ok(sp.siblings.every(s => s.form !== 'huggie' && !/HUGGIE/.test(s.sku)), 'never a (HUGGIE) twin for a necklace');
  ok(sp.readable === false, 'a held line is never sent to the paid custom-order reading');
  eq(sp.formHold.sku, 'SCHRODINGER CAT'); eq(sp.formHold.design, 'SCHRODINGER CAT'); eq([sp.formHold.lineForm, sp.formHold.designForm], ['necklace', 'huggie']);
  // the Etsy SKU may be written another way: the same
  for (const sku of ['Schrodinger Cat', 'SCHRODINGER_CAT', 'schrodinger cat ']) { const q = read(line('1000000002', sku, 'Cat Necklace', NECK)); eq([!!q.formHold, q.designSku], [true, null], 'the same SKU written another way: ' + JSON.stringify(sku)); }
  step('Paul\'s order (SCHRODINGER CAT, Necklace): held, one plain line, no huggie vector on the row, SCHRODINGER CAT 1 and SCHRODINGER CAT 2 offered');
}

/* ── 3 · a person's saved answer always wins; the guard never touches it ── */
function personWins() {
  const base = PAUL(), alias = v => ({ aliases: { [base.listingId]: v } });
  let sp = read(base, alias({ v: 2, bySku: { 'SCHRODINGER CAT': 'SCHRODINGER CAT 2' } }));
  eq([sp.designSku, sp.skuSource, !!sp.formHold, sp.problems.length], ['SCHRODINGER CAT 2', 'alias', false, 0], '"Use this charm: SCHRODINGER CAT 2" releases the held line (and wins over the exact master SKU)');
  sp = read(base, alias({ v: 2, bySku: { 'SCHRODINGER CAT': 'SCHRODINGER CAT' } }));
  eq([sp.designSku, !!sp.formHold, sp.problems.length], ['SCHRODINGER CAT', false, 0], 'a person who says the huggie IS right is believed: never held again');
  sp = read(base, alias({ sku: 'SCHRODINGER CAT 1' }));
  eq([sp.designSku, sp.skuSource, !!sp.formHold], ['SCHRODINGER CAT 1', 'alias', false], 'an older listing-wide answer (no v) is read too');
  // a saved option map that picks the charm
  sp = read(line('1000000003', 'SCHRODINGER CAT', 'Cat Necklace', NECK.concat([['Cat', 'Skull']])), { optionMaps: { 1000000003: { cat: { skull: { field: 'design', value: 'SCHRODINGER CAT 2' } } } } });
  eq([sp.designSku, sp.skuSource, !!sp.formHold], ['SCHRODINGER CAT 2', 'option', false], 'a person\'s option answer wins');
  // a saved pair link (aliasPut pair): both designs are the person's
  sp = read(base, alias({ pairBySku: { 'SCHRODINGER CAT': { L: 'SCHRODINGER CAT 1', R: 'SCHRODINGER CAT 2' } } }));
  eq([sp.designSku, sp.skuSource, !!sp.formHold], ['SCHRODINGER CAT 1', 'pair', false], 'a saved pair link is never held');
  // a person's answer for ANOTHER SKU of the listing does not release this one
  sp = read(base, alias({ v: 2, bySku: { 'SOME OTHER SKU': 'SCHRODINGER CAT 2' } })); ok(!!sp.formHold, 'one SKU\'s answer never stands in for another');
  // a person said "form: necklace" for a listing option: still a necklace; a person said "earrings": then the line is no necklace
  sp = read(line('1000000004', 'SCHRODINGER CAT', 'Cat Charm', [['Style', 'Pendant Necklace']])); eq([sp.lineForm, !!sp.formHold], ['necklace', true], 'a Type option that says necklace');
  step('a saved alias (other design, or the same one confirmed), an option map and a saved pair link all win and are never held');
}

/* ── 4 · the matrix: line form x design form ── */
function matrix() {
  const forms = { huggie: 'SCHRODINGER CAT', earring: 'ZODIAC_EARRINGS-3', necklace: 'GREEKGODDESS_NECKLACE_4', unknown: 'PLAIN_ONE' };
  const lines = {
    necklace: s => line('1100000001', s, 'Charm Necklace', NECK), earring: s => line('1100000002', s, 'Stud Earrings', [['Metal Choice', 'Silver - Pair']]),
    huggie: s => line('1100000003', s, 'Huggie Hoop Earrings', [['Metal Choice', 'Silver - Pair']]), unknown: s => line('1100000004', s, 'Charm', [['Metal Choice', 'Silver']])
  };
  // exactly the three crossings of GUARD-API.md: necklace x huggie, huggie x necklace, earring x huggie. A necklace SKU on an earrings listing (and the reverse) stays with the SKU-word check that already existed.
  const want = { necklace: { huggie: true, earring: false, necklace: false, unknown: false }, earring: { huggie: true, earring: false, necklace: false, unknown: false }, huggie: { huggie: false, earring: false, necklace: true, unknown: false }, unknown: { huggie: false, earring: false, necklace: false, unknown: false } };
  for (const lf of Object.keys(lines)) for (const df of Object.keys(forms)) {
    const sp = read(lines[lf](forms[df])); if (lf !== 'unknown') eq(sp.lineForm, lf, `${lf} line`);   // (an unknown line carries no form words, but a SKU like ZODIAC_EARRINGS-3 is itself the shop's earring SKU: its text is a signal the line already read before the guard)
    eq(!!sp.formHold, want[lf][df], `${lf} line x ${df} design (${forms[df]}) held=${want[lf][df]}`);
    eq(O.formClash(lf, df), want[lf][df], `formClash(${lf}, ${df})`);
  }
  // the reverse: a huggie listing whose SKU is the plain name of a design that has its own huggie drawing is held, the huggie twin offered first
  let sp = read(line('1100000005', 'BUNNY_42980', 'Bunny Huggie Hoop Earrings', [['Metal Choice', 'Silver - Pair']]));
  eq([!!sp.formHold, sp.siblings.map(s => s.sku)], [true, ['BUNNY_42980 (HUGGIE)']], 'a huggie line never takes the necklace-size charm of a design that has its own huggie');
  eq(sp.formHold.reason, 'SKU “BUNNY_42980” is a necklace design in the library, but this listing is huggie hoops');
  // ... and an earring (stud) line may use the plain design that has a huggie twin (the necklace read is only soft); a necklace line takes it too
  sp = read(line('1100000006', 'BUNNY_42980', 'Bunny Stud Earrings', [['Metal Choice', 'Silver - Pair']])); eq([sp.lineForm, !!sp.formHold], ['earring', false]);
  sp = read(line('1100000007', 'BUNNY_42980', 'Bunny Necklace', NECK)); eq([sp.lineForm, !!sp.formHold, sp.designSku], ['necklace', false, 'BUNNY_42980']);
  // an earring line on a huggie text SKU, and the genuine huggie ones
  for (const [sku, title] of [['HUGGIE HOOPS- SCHRONDINGER CAT', 'Schrodinger\'s Cat Huggie Hoop Earrings'], ['SCHRODINGER CAT 1 (HUGGIE)', 'Cat Huggie Charm Earrings'], ['SCHRODINGER CAT', 'Schrodinger Cat Huggie Hoops'], ['HOOPS-CELESTIAL (MOON+STARS)', 'Celestial Huggie Hoops']]) {
    sp = read(line('1100000008', sku, title, [['Metal Choice', 'Silver - Pair']])); eq([sp.lineForm, !!sp.formHold, sp.designSku], ['huggie', false, sku], `a genuine huggie listing keeps its master: ${sku}`);
  }
  // a plain design on six names and one (HUGGIE) name is not a huggie; HUGGIE_3722 (named Huggie, drawn as a huggie ladybug) is the huggie ladybug
  eq(O.masterForm('CROWN 2', { masterEntry: entryFor, masterShared: sharedFor }).form, 'unknown'); eq(O.masterForm('QUEEN_7155', { masterEntry: entryFor, masterShared: sharedFor }).form, 'unknown');
  eq(O.masterForm('HEART_5193', { masterEntry: entryFor, masterShared: sharedFor }), { form: 'huggie', by: 'shared' }, 'a plain name on a drawing whose file is named for a huggie and a (HUGGIE) SKU');
  eq(O.formOfText('HUGGIE_3722'), '', 'Huggie_3722 is a numbered design that happens to be named Huggie: the text says nothing');
  sp = read(line('1100000009', 'PLAIN_ONE', 'Necklace', NECK)); eq(!!sp.formHold, false);
  // SCHROAUDIT's shared-drawing cases: a drawing that six plain names and one (HUGGIE) name hold is the big charm (never held on a necklace); a drawing with one plain name and one huggie name (or a huggie-named file) is the small
  // huggie, and a necklace line on it waits with ONE plain line until a person says "use this one anyway" (the same SKU saved for the listing: the card's existing "Use this charm"), which is final
  for (const sku of ['CROWN 2', 'QUEEN_7155', 'REGAL_64774']) eq(!!read(line('1100000010', sku, 'Crown Necklace', NECK)).formHold, false, sku + ' is a big charm that shares its drawing with a huggie name: a necklace line takes it');
  const hc = line('1100000011', 'HEART_5193', 'Heart Necklace', NECK); sp = read(hc); eq([!!sp.formHold, sp.designSku], [true, null], 'one plain name on a huggie-named drawing: the necklace line waits');
  sp = read(hc, { aliases: { [hc.listingId]: { v: 2, bySku: { 'HEART_5193': 'HEART_5193' } } } }); eq([!!sp.formHold, sp.designSku], [false, 'HEART_5193'], '"use this one anyway" is saved for the listing and is never asked again');
  step('matrix: necklace x huggie, huggie x necklace and earring x huggie are held; every other pairing and unknown never is; genuine huggie listings keep their (HUGGIE) master');
}

/* ── 5 · Huggie CHARM SET, -CO and the charm-only options ── */
function charmOnly() {
  const ct = (sku, value) => line('1200000001', sku, 'Cat Charm Necklace Pendant', [['Metal Choice', 'Silver'], ['Charm Type', value]]);
  let sp = read(ct('SCHRODINGER CAT 2', 'Huggie CHARM SET')); eq([sp.designSku, sp.form, !!sp.formHold, sp.problems.length, sp.lineForm], ['SCHRODINGER CAT 2 (HUGGIE)', 'huggie', false, 0, 'huggie'], 'Huggie CHARM SET still resolves to "X (HUGGIE)"');
  sp = read(ct('BUNNY_42980', 'Huggie CHARM SET')); eq([sp.designSku, !!sp.formHold], ['BUNNY_42980 (HUGGIE)', false]);
  sp = read(ct('Dragon 11', 'Huggie CHARM SET')); eq([sp.designSku, sp.problems.map(p => p.kind)], ['DRAGON 11 (HUGGIE)', ['unmatchedSku']], 'no huggie design: asked under its own name, never the necklace charm');
  // the necklace choices of such a listing use the plain SKU: when that plain SKU is the huggie drawing, the line waits
  for (const v of ['Necklace CHARM', 'CHARM + Engraving']) { sp = read(ct('SCHRODINGER CAT', v)); eq([sp.lineForm, !!sp.formHold], ['necklace', true], v + ' on a SKU that is the huggie drawing'); sp = read(ct('BUNNY_42980', v)); eq([sp.designSku, !!sp.formHold], ['BUNNY_42980', false], v + ' on a design that is the necklace charm'); }
  // X-CO is the charm-only (necklace) charm of X
  sp = read(line('1200000002', 'MAPLE_8065-CO', 'Maple Leaf Charm', [['Metal Choice', 'Silver']])); eq([sp.designSku, sp.skuSource, sp.lineForm, !!sp.formHold], ['MAPLE_8065', 'variation', 'necklace', false], '-CO still maps to the plain design');
  sp = read(line('1200000003', 'SCHRODINGER CAT-CO', 'Schrodinger Cat Charm', [['Metal Choice', 'Silver']])); eq([sp.designSku, !!sp.formHold, sp.lineForm], [null, true, 'necklace'], '-CO is the necklace charm: the huggie drawing is held');
  step('Huggie CHARM SET, -CO, Necklace CHARM and CHARM + Engraving: the huggie set is untouched; a necklace charm on the huggie drawing waits');
}

/* ── 6 · the siblings API ── */
function siblings() {
  const c = ctx(), S = (sku, form) => O.siblingsOf(sku, { masterEntry: c.masterEntry, masterLoose: c.masterLoose, masterShared: c.masterShared }, { form }).map(s => s.sku);
  eq(S('SCHRODINGER CAT', 'necklace'), ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']);
  eq(S('SCHRODINGER CAT', 'unknown'), ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2'], 'an unknown line is offered the plain numbers only');
  eq(S('SCHRODINGER CAT', 'huggie'), ['SCHRODINGER CAT 1 (HUGGIE)', 'SCHRODINGER CAT 2 (HUGGIE)'], 'a huggie line is offered the numbered huggies');
  eq(S('SCHRODINGER CAT (HUGGIE)', 'huggie'), ['SCHRODINGER CAT 1 (HUGGIE)', 'SCHRODINGER CAT 2 (HUGGIE)'], 'the base of an (HUGGIE) SKU is the name');
  eq(S('SCHRODINGER CAT 2 (HUGGIE)', 'huggie'), [], 'a numbered huggie has no numbered siblings, and the SKU itself is never offered back');
  eq(S('BUNNY_42980', 'huggie'), ['BUNNY_42980 (HUGGIE)'], 'the huggie twin');
  eq(S('NO SUCH CAT', 'necklace'), [], 'none');
  eq(S('GREEKGODDESS_NECKLACE_4', 'earring'), ['GREEKGODDESS_EARRING_4'], 'the same name written for the line\'s family');
  // a SKU the master does not hold at all but whose numbered siblings it has: the unmatched question carries them (SIBLINGSUGGEST)
  lib.delete('SCHRODINGER CAT'); lib.delete('SCHRODINGER CAT 2 (HUGGIE)');
  const sp = read(PAUL()), q = sp.problems.find(p => p.kind === 'unmatchedSku');
  eq([!!q, !!sp.formHold, q && q.reason], [true, false, 'not in any master file'], 'with no exact entry the line is unmatched, not held');
  eq([sp.siblings.map(s => s.sku), q.siblings.map(s => s.sku)], [['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2'], ['SCHRODINGER CAT 1', 'SCHRODINGER CAT 2']], 'and its siblings are told');
  lib.set('SCHRODINGER CAT', { sku: 'SCHRODINGER CAT', aiPath: SCHR, widthPt: 21.1688, heightPt: 24.286, members: 7 }); lib.set('SCHRODINGER CAT 2 (HUGGIE)', { sku: 'SCHRODINGER CAT 2 (HUGGIE)', aiPath: SCHR, widthPt: 21.1688, heightPt: 24.286, members: 7 });
  step('siblings: numbered siblings by number, the huggie ones for a huggie line, the huggie twin, the family twin; an unmatched SKU carries them too');
}

/* ── 7 · nothing else changes: the in-repo order corpora, read by clean main (e87289a) and by this code ── */
function corpora() {
  const W = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/huggie-guard-world.json'), 'utf8')), TD = __dirname;
  const sl = new Map(W.master.map(e => [e.sku.toUpperCase(), Object.assign({ sizes: null, blocked: null }, e)])), sByFile = new Map();
  for (const [k, e] of sl) if (e.aiPath) { if (!sByFile.has(e.aiPath)) sByFile.set(e.aiPath, []); sByFile.get(e.aiPath).push(k); }
  const OL = JSON.parse(fs.readFileSync(path.join(TD, 'options-link-data.json'), 'utf8')), J = f => JSON.parse(fs.readFileSync(path.join(TD, f), 'utf8'));
  const c = tables => ({ optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [] }, customDone: {}, customRead: {}, customDecided: {}, listingSkus: tables || {}, masterEntry: s => sl.get(String(s).toUpperCase()) || null, masterLoose: s => W.loose[O.looseKey(s)] || '', masterShared: s => { const e = sl.get(String(s).toUpperCase()); return e && e.aiPath ? sByFile.get(e.aiPath) || [] : []; } });
  const lines = [];
  for (const l of J('fixtures/addon-snapshot-lines.json').lines) lines.push(['A:' + l.rid + '/' + l.tid, { transactionId: l.tid, listingId: l.listing, sku: l.sku, title: l.title, quantity: l.q, metalKey: l.mk, metalLabel: '', personalization: [], buyerMessage: '', variations: l.vars.map(v => ({ name: v[0], value: v[1] })) }, { receiptId: l.rid, updateTs: 1 }, false]);
  for (const l of OL.lines) lines.push(['C:' + l.receipt + '/' + l.tx, { transactionId: l.tx, listingId: l.listing, productId: l.product, sku: l.sku, title: l.title, quantity: l.quantity, metalKey: /silver/i.test(JSON.stringify(l.variations)) ? 'silver' : 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: l.variations }, { receiptId: l.receipt, updateTs: 1 }, true]);
  for (const l of J('fixtures/adv-count-lines.json').lines) lines.push(['D:' + l.rid + '/' + l.tid, { transactionId: l.tid, listingId: l.listingId, sku: l.sku, title: l.title, quantity: l.q, metalKey: 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: l.vars.map(v => ({ name: v[0], value: v[1] })) }, { receiptId: l.rid, updateTs: 1 }, false]);
  for (const l of J('fixtures/font-option-lines.json').lines) lines.push(['E:' + l.rid + '/' + l.tid, { transactionId: l.tid, listingId: l.listingId, sku: l.sku, title: l.title, quantity: l.q, metalKey: 'gold', metalLabel: '', personalization: [], buyerMessage: '', variations: l.vars.map(v => ({ name: v[0], value: v[1] })) }, { receiptId: l.rid, updateTs: 1 }, false]);
  for (const g of W.gg) lines.push([g.key, g.line, g.order, false]);
  const rec = sp => ({ design: sp.designSku || null, src: sp.skuSource || null, problems: (sp.problems || []).map(p => p.kind + (p.optionName ? ':' + p.optionName : '')), pieces: sp.pieceCount });
  // FALSEHOLD (Paul, 19 Oct 2026, orders 4207131438 / 4204710975): an option made only of a chain length ("Price: 20"-21"" on the longer-chain add-on) is no design question any more: the line waits on
  // what it really lacks, its SKU in a master file, instead of "option not mapped". The one corpus line this changes, read as it is now:
  const FALSEHOLD_NOW = { 'A:4171409060/5213382667': { design: 'LONGER_CHAIN_5682', src: 'transaction', problems: ['unmatchedSku'], pieces: 1 } };
  const changed = [];
  for (const [key, line, order, tables] of lines) {
    const sp = O.interpretLine(order, JSON.parse(JSON.stringify(line)), c(tables ? OL.tables : null)), b = FALSEHOLD_NOW[key] || W.before[key]; ok(b, 'a before for ' + key);
    if (JSON.stringify(rec(sp)) !== JSON.stringify(b)) changed.push({ key, sku: line.sku, held: !!sp.formHold, why: sp.formHold && sp.formHold.reason, before: b.design });
  }
  ok(lines.length >= 560, 'the corpora: ' + lines.length + ' lines (411 of the Sep 17 snapshot, the 18 option-link, 118 count, 17 font lines, 5 Greek Goddess earrings)');
  // NO line of any corpus changes: no real line crosses forms (SCHROAUDIT: Paul's order is the only one). The five Greek Goddess EARRINGS lines (a necklace pendant SKU on an earrings listing, FLAGAUDIT C069-C073) are in the corpus
  // as the proof that the earring / necklace SKU-word check is left alone: they read as clean main read them.
  eq(changed, [], 'every line reads exactly as clean main (e87289a) read it: ' + JSON.stringify(changed.map(x => x.sku)));
  ok(lines.filter(l => l[0].startsWith('G:')).length === 5, 'the five Greek Goddess earrings lines are among them');
  step(`${lines.length} real order lines (Sep 17 snapshot, option-link tables, count and font lines, the five Greek Goddess earrings lines): all ${lines.length} read exactly as clean main read them; 0 change`);
}

/* ── 8 · the neighbours: letter and zodiac families, numbered earring SKUs, KEYONE's one body, mismatched pairs, fixed flagged rows ── */
function neighbours() {
  let sp = read(line('1300000001', 'ZODIAC_EARRINGS-3', 'Zodiac Stud Earrings', [['Metal Choice', 'Gold - Pair'], ['Zodiac Sign', 'Cancer']])); eq([sp.designSku, !!sp.formHold, sp.problems.length], ['ZODIAC_EARRINGS-3', false, 0], 'a numbered zodiac earring on earrings: the option settles itself (FLAGWHY)');
  sp = read(line('1300000002', 'LETTER_EARRING-19', 'Initial Letter Stud Earrings', [['Color', 'Sterling Silver'], ['Letter', 'T']])); eq([sp.designSku, !!sp.formHold, sp.problems.length], ['LETTER_EARRING-19', false, 0], 'a letter family member on earrings');
  sp = read(line('1300000003', 'ZODIAC_EARRINGS-3', 'Zodiac Necklace', NECK.concat([['Zodiac Sign', 'Cancer']]))); eq([!!sp.formHold, sp.designSku], [false, 'ZODIAC_EARRINGS-3'], 'an earring SKU on a NECKLACE listing is not this guard\'s crossing: the SKU-word check that already existed keeps it, as before');
  sp = read(line('1300000004', 'PEARL_STUD_5', 'Pearl Stud Earrings', [['Metal Choice', 'Silver - Pair']])); eq([sp.designSku, !!sp.formHold, sp.pieceCount], ['PEARL_STUD_5', false, 2], 'a stud earring pair is still two pieces');
  // a line that is not cut, a custom order, an add-on: never held
  sp = read(line('1300000005', 'CUSTOM_6673', 'Custom Necklace', NECK)); ok(!sp.formHold, 'a custom order is the custom order\'s business');
  sp = read(PAUL(), { noDesign: { patterns: [], skus: ['SCHRODINGER CAT'] } }); eq([!!sp.formHold, sp.noDesign], [false, true], 'a SKU on the no-design list is never held');
  // a guard that fails never stops a line from being read
  const boom = Object.assign(ctx(), { masterShared: () => { throw new Error('x'); } }); sp = O.interpretLine({ receiptId: 'R1', updateTs: 1 }, PAUL(), boom); ok(sp && sp.designSku === 'SCHRODINGER CAT', 'a failure of the form facts leaves the line as it always was');
  // callers without a master (the employee profile reads lines with none)
  sp = O.interpretLine({ receiptId: 'R1', updateTs: 1 }, PAUL(), { optionMaps: {}, masterEntry: null }); eq([sp.designSku, !!sp.formHold, sp.lineForm], ['SCHRODINGER CAT', false, 'necklace']);
  step('neighbours: zodiac / letter families, stud pairs, custom orders, the no-design list and a failing guard read as before');
}

/* ── 9 · the page: the guard's wiring in the bridge, readiness and the cache tokens ── */
function wiring() {
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8'), ready = fs.readFileSync(path.join(root, 'charm-nest-readiness.js'), 'utf8'), html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  ok(/masterShared: sku => Master\.sharedFor\(sku\)/.test(bridge), 'the page hands the orders module the library\'s design-file facts');
  ok(/function sharedFor\(sku\)/.test(bridge) && /return \{ entryFor, looseFor, sharedFor,/.test(bridge), 'Master.sharedFor exists and is exported');
  ok(/if \(sp\.formHold\) \{[^}]*row\.state = "unmatched"/.test(bridge), 'the pool attempt re-raises a held line: it is never pooled');
  ok(/formFacts\(l\.sku\)/.test(bridge), 'a line is read again when the library changes what its design is');
  ok(/p\.formHold \? p\.reason :/.test(bridge) && /sp\.formHold\) return sp\.formHold\.reason/.test(bridge), 'the row says the plain line');
  ok(/p\.formHold && p\.reason/.test(ready), 'readiness says it too');
  for (const name of ['charm-nest-orders.js', 'charm-nest-bridge.js', 'charm-nest-readiness.js']) { const m = new RegExp('src="' + name.replace('.', '\\.') + '\\?v=([^"]*)"').exec(html); ok(m && /-hg1(?:-|$)/.test(m[1]) && /^2026100/.test(m[1]), name + ' token carries -hg1 (other workers append their own suffix; the old ones are kept)'); }
  // the bridge's reading of the sandbox / station stays pure: nothing in the guard asks the network
  const orders = fs.readFileSync(path.join(root, 'charm-nest-orders.js'), 'utf8'), sec = orders.slice(orders.indexOf('3b · THE FORM GUARD'), orders.indexOf('function resolveSku('));
  ok(sec.length > 3000 && !/\bfetch\s*\(|XMLHttpRequest|require\(|Date\.now|Math\.random|api\(/.test(sec), 'the form guard is pure: no network, no clock, no Etsy, no AI');
  step('wiring: the page passes the design-file facts, the pool attempt re-raises the hold, readiness and the row say the plain line, the three script tokens carry -hg1');
}

const run = [cause, paulsOrder, personWins, matrix, charmOnly, siblings, corpora, neighbours, wiring]; let failed = 0;
for (const f of run) { try { f(); } catch (e) { failed++; console.log('  ✗ ' + f.name + ': ' + (e && e.stack ? e.stack.split('\n').slice(0, 7).join('\n') : e)); } }
console.log(failed ? `huggie-guard: ${failed} failed` : `huggie-guard: all passed (${n} checks)`); process.exit(failed ? 1 : 0);
