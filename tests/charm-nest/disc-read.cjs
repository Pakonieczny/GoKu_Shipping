// DISCREAD (10 Oct 2026; Paul's point 1: "did the system recognize that the buyer chose the two disc option, so it adds two discs, each through the engraving pipeline, so we can
// verify the font style and the engraving the buyer chose per individual disc"). Offline: no Etsy, no live read, no write, no paid call.
//   node tests/charm-nest/disc-read.cjs
// What it proves (charm-nest-orders.js, charm-nest-pair.js, charm-nest-engrave-sides.js; the bridge's two call sites are checked in its source):
//   · the two real lines of the Sep 17 snapshot read as ONE necklace group of n discs with no question: 4175370240 (SKU typo, "ROSEGOLD - 2 Disc", Fonts 16"/ Typewriter) = 2 discs, 4172791262
//     ("3 discs . gold", Font Stylish) = 3 discs; the font asked is kept on the line and never holds it; pieces carry n of n, group key, group size; the engraving slots are D1..Dn
//   · every counted wording makes n slots (the intake's count, not the word "disc"): "Number of Discs: 3", "3 Tags", "Set of 3 charms", "2-Disc", quantity 2 of "2 Disc" (D1 D2 D1 D2); "1 Disc" asks nothing
//   · the buyer's words are split per disc (splitWords): "Tag 1: J, Tag 2: Q" = J, Q; the other wordings; what it will NOT guess; wordsForDiscs stands aside for a buyer message / staff note
//   · each disc has its own engraving record (own pool ids, own approval, own summary), and pieceRecord(s) say index / of / words / font per disc
//   · a held line whose question is gone is released (questionGone / staleQuestionHold); a person's hold, pieces, a record row, a still-asked question are left alone
const assert = require('assert');
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const O = require('../../charm-nest-orders.js');
const P = require('../../charm-nest-pair.js');
const S = require('../../charm-nest-engrave-sides.js');
const noNested = require('./_noNestedArrays.cjs');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };

const DATA = JSON.parse(fs.readFileSync(path.join(__dirname, 'options-link-data.json'), 'utf8'));
const SNAP = JSON.parse(fs.readFileSync(path.join(__dirname, 'pairs-words-snapshot.json'), 'utf8'));
const MASTER = new Set(['INITIAL_DISC_4571', 'INITIAL_8391', 'NECK_1']);
const entryFor = s => (MASTER.has(String(s || '').toUpperCase()) ? { sku: s } : null);
const looseFor = s => { const k = O.looseKey(s); for (const m of MASTER) if (O.looseKey(m) === k) return m; return ''; };
const ctx = extra => Object.assign({ optionMaps: {}, aliases: {}, noDesign: {}, listingSkus: {}, masterEntry: entryFor, masterLoose: looseFor }, extra || {});
const V = (name, value) => ({ name, value });

// one line read the way the sorter reads it, its pieces and its engraving slots
function world(rid, line, c) {
  const order = { receiptId: rid, updateTs: 0 }, spec = O.interpretLine(order, line, c || ctx());
  noNested(spec, 'spec');
  const row = { key: `${rid}_${line.transactionId}`, order, line, spec, state: 'pulled' };
  const pieces = P.piecesFor(Object.assign({}, line, { receiptId: rid, quantity: spec.quantity, form: spec.form, spec }), null);
  row.poolIds = pieces.map((p, i) => `${row.key}_${i + 1}`);
  const SC = { pair: P, entryFor, poolRow: () => null, charmOf: () => null };
  return { order, spec, row, pieces, plan: S.plan(SC, row, []), SC };
}
const realLine = (rid, metalKey, personalization) => {
  const r = DATA.lines.find(l => l.receipt === rid);
  return { transactionId: r.tx, listingId: r.listing, productId: '', sku: r.sku, title: r.title, quantity: 1, metalKey, metalLabel: metalKey, personalization, variations: r.variations };
};

// ── 1 · the two real lines ───────────────────────────────────────────────────────────────────────────────────────────────────────────────
{ const w = world('4175370240', realLine('4175370240', 'rose', ['Tag 1: J, Tag 2: Q']));
  eq([w.spec.designSku, w.spec.skuSource, w.spec.problems.length], ['INITIAL_DISC_4571', 'typo', 0], '4175370240: the typo SKU reads as the master design, nothing is asked (not held)');
  eq([w.spec.pieceCount, w.spec.pair.kind, w.spec.pair.discs, w.spec.pair.earring, w.spec.pair.sides], [2, 'multi', 2, false, [null, null]], '"ROSEGOLD - 2 Disc" is 2 pieces of one necklace, no side');
  eq([w.spec.font.asked, w.spec.font.source, w.spec.chain], ['Typewriter', 'rule:font', '16"'], 'the Fonts option is the font the buyer ASKED for (an installed font since FONTMAP) plus a length: it never holds the line');
  ok(w.spec.engraveCandidate, 'it has words to engrave');
  eq(w.pieces.map(p => [p.n, p.of, p.side, p.mirror, p.groupKey]), [[1, 2, null, false, '4175370240:5217980219'], [2, 2, null, false, '4175370240:5217980219']], 'two pieces, n of 2, one group, never mirrored');
  eq([w.plan.split, w.plan.slots], [true, ['D1', 'D2']], 'two engraving slots: disc 1 and disc 2');
  eq(w.row.poolIds.map(id => w.plan.of.get(id)), ['D1', 'D2'], 'copy 1 is disc 1, copy 2 is disc 2');
  eq(S.splitWords(w.spec.personalization, 2).words, ['J', 'Q'], 'disc 1 = J, disc 2 = Q'); }
{ const w = world('4172791262', realLine('4172791262', 'gold', ['1: A, 2: B, 3: C']));
  eq([w.spec.designSku, w.spec.problems.length, w.spec.pieceCount, w.spec.pair.discs], ['INITIAL_8391', 0, 3, 3], '4172791262: "3 discs . gold" is 3 pieces, nothing is asked');
  eq([w.spec.font.asked, w.spec.font.source], ['Stylish', 'rule:font'], 'Font: Stylish is read, not held');
  eq(w.plan.slots, ['D1', 'D2', 'D3'], 'three slots');
  eq(S.splitWords(w.spec.personalization, 3).words, ['A', 'B', 'C'], 'one word per disc'); }
// the old reading kept in a stored line: the question is gone with the current reader
{ const w = world('4175370240', realLine('4175370240', 'rose', ['Tag 1: J, Tag 2: Q']));
  const oldSpec = { problems: [{ kind: 'needsMapping', optionName: 'Fonts', optionValue: '16"/ Typewriter' }], designSku: 'NITIAL_DISC_4571' };
  const held = Object.assign({}, w.row, { state: 'held', reason: 'option "Fonts: 16"/ Typewriter" not mapped', problems: [], poolIds: [] });
  ok(O.questionGone(held, oldSpec), 'a line held for the old "not mapped" question, read clean now, is released');
  ok(O.staleQuestionHold(Object.assign({}, held, { fromRecord: true })), 'and so is the stored record of it (the order window of an order outside the pull)');
  for (const [why, over] of [['a person\'s hold', { hold: 'held by Paul' }], ['pieces already made', { poolIds: ['x_1'] }], ['a record row (the run\'s own copy)', { fromRecord: true }], ['a line in progress', { state: 'pulled' }], ['a line whose question is still asked', { problems: [{ kind: 'needsMapping' }] }]])
    ok(!O.questionGone(Object.assign({}, held, over), oldSpec), `not released: ${why}`);
  ok(!O.questionGone(held, { problems: [] }) && !O.questionGone(held, null), 'a line never read with a question is not released by this rule');
  ok(!O.staleQuestionHold(Object.assign({}, held, { reason: 'the design could not be loaded' })), 'a pooling failure is not a question: its record is left as it is');
  ok(!O.staleQuestionHold(Object.assign({}, held, { hold: 'held by Paul' })) && !O.staleQuestionHold(Object.assign({}, held, { poolIds: ['x_1'] })), 'a hold and made pieces stay'); }

// ── 2 · every counted wording makes n slots (the intake's count, not the word "disc") ────────────────────────────────────────────────────────
{ const go = (vars, q, title) => world('4170000001', { transactionId: '55', listingId: '9', sku: 'NECK_1', title: title || 'Initial Necklace', quantity: q || 1, metalKey: 'gold', metalLabel: 'Gold', personalization: ['A, B, C'], variations: vars.map(([a, b]) => V(a, b)) });
  for (const [label, vars, count] of [['Necklace Options: GOLD - 2 Disc', [['Necklace Options', 'GOLD - 2 Disc']], 2], ['Number of Discs: 3', [['Number of Discs', '3']], 3], ['Number of Discs: Three', [['Number of Discs', 'Three']], 3],
    ['Tags: 3 Tags', [['Tags', '3 Tags']], 3], ['Number of Tags: 3', [['Number of Tags', '3']], 3], ['Set of 3 charms', [['Set', 'Set of 3 charms']], 3], ['2-Disc', [['Style', '2-Disc']], 2], ['x4 Charms', [['Options', 'x4 Charms']], 4]]) {
    const w = go(vars);
    eq([w.spec.problems.length, w.spec.pieceCount, w.plan.slots], [0, count, Array.from({ length: count }, (_, i) => 'D' + (i + 1))], `${label}: ${count} pieces, slots D1..D${count}, nothing asked`); }
  const q2 = go([['Necklace Options', 'GOLD - 2 Disc']], 2);
  eq([q2.spec.pieceCount, q2.plan.slots, q2.row.poolIds.map(id => q2.plan.of.get(id))], [4, ['D1', 'D2'], ['D1', 'D2', 'D1', 'D2']], 'two necklaces of 2 discs: four pieces, disc 1 and disc 2 of each (two jobs of two copies)');
  const one = go([['Necklace Options', 'GOLD - 1 Disc']]);
  eq([one.spec.problems.length, one.spec.pieceCount, one.plan.split, one.spec.options[0].mapped.field], [0, 1, false, 'count'], '"GOLD - 1 Disc" is one piece and asks nothing (it used to be held "not mapped"), no slot');
  const one2 = go([['Number of Discs', '1']]); eq([one2.spec.problems.length, one2.spec.pieceCount], [0, 1], '"Number of Discs: 1" is one piece too');
  // what is NOT a count stays what it was
  for (const v of [['Metal Choice', '14K SOLID GOLD'], ['Necklace Length', '16"'], ['Charm Size', '14mm']]) eq(go([v]).spec.pieceCount, 1, `${v.join(': ')} is not a count`);
  ok(S.perUnitOf({ spec: { pieceCount: 4, quantity: 2, pair: { kind: 'multi', earring: false } } }) === 2 && S.perUnitOf({ spec: { pieceCount: 4, quantity: 2, pair: { kind: 'pair', earring: true } } }) === 0 && S.perUnitOf({ spec: { pieceCount: 3, quantity: 2, pair: { kind: 'multi' } } }) === 0, 'perUnitOf: per unit, never for an earring pair, never for a count that is no multiple of the quantity'); }

// the six counted-disc listings that carry a font drop-down (FONTAUDIT: option names and values as the shop's orders show them; the font is ONE value for the whole line, shared by every disc)
{ const LISTINGS = [
    ['1008014571', 'nitial_Disc_4571', 'Necklace Options', ['GOLD - 2 Disc', 'GOLD - 3 Disc', 'GOLD - 4 Disc', 'ROSEGOLD - 2 Disc', 'SILVER - 2 Disc'], 'Fonts', ['16"/ Typewriter', '20"/ Stylish', '18"/ Angelina', '18"/ Pristina', '16"/ Comic'], true],
    ['234758391', 'Initial_8391', 'Number of Discs / Metal', ['1 disc \u2022 gold', '2 discs \u2022 gold', '3 discs \u2022 gold', '5 discs \u2022 silver'], 'Font', ['Angelina', 'Stylish', 'Typewriter', 'Comic', 'Pristina'], true],
    ['880672858', 'TEST1', 'Necklace Options', ['GOLD - 1 Disc', 'GOLD - 3 Discs', 'ROSEGOLD - 1 Disc', 'SILVER- 2 Discs'], 'Font Selection', ['18"/Typewriter', '17"/Angelina', '18"/Stylish'], true],
    ['479938139', 'Initial_Disc_8139', 'Necklace Options', ['GOLD - 1 Disc'], 'Font Selection', ['16"/Angelina', '16"/Comic'], true],
    ['1002723802', 'Beady Disc', 'Metal Choice \u00b7 Engraving Options?', ['GOLD \u2022 3 DISCS', 'SILVER \u2022 2 DISCS'], 'Necklace Length and Font', ['18"\u00b7TYPEWRITER\u00b7', '16"\u00b7ANGELINA\u00b7'], false],
    ['1025856932', 'Disc_Bracelet_6932', 'Material', ['1 Disc \u2022 Gold', '3 Disc \u2022 Gold', '3 Disc \u2022 Silver'], 'Chain Length', ['6 inch \u2022 Comic', '7 inch \u2022 Angelina'], false]];
  let lines = 0;
  for (const [listing, sku, cname, counts, fname, fonts, fontReads] of LISTINGS) for (const c of counts) for (const f of fonts) {
    const k = +/(\d)\s*disc/i.exec(c)[1], words = k > 1 ? [Array.from({ length: k }, (_, i) => `Tag ${i + 1}: ${'JQRZX'[i]}`).join(', ')] : ['J'];
    const w = world('4170000010', { transactionId: '58', listingId: listing, sku, title: 'Initial Disc Necklace', quantity: 1, metalKey: 'gold', metalLabel: 'Gold', personalization: words, variations: [V(cname, c), V(fname, f)] }, ctx({ masterEntry: s => (s ? { sku: s } : null), masterLoose: () => '' }));
    const label = `${listing} ${cname}: ${c} + ${fname}: ${f}`;
    eq([w.spec.problems.length, w.spec.pieceCount, w.plan.slots], [0, k, k > 1 ? Array.from({ length: k }, (_, i) => 'D' + (i + 1)) : [null]], `${label}: ${k} piece(s), ${k > 1 ? 'one slot per disc' : 'no slot'}, nothing asked`);
    if (fontReads) ok(w.spec.font && w.spec.font.asked.toLowerCase() === f.replace(/^[\d"\/ ]+/, '').toLowerCase(), `${label}: the font word is read (${w.spec.font && w.spec.font.asked})`);
    if (k > 1) { const jobs = w.plan.slots.map(s => ({ key: `k#${s}`, slot: s, lines: [], row: { spec: w.spec } })), recs = S.pieceRecords(jobs, {});
      eq([new Set(recs.map(r => r.fontAsked)).size, recs.map(r => r.of)], [1, Array(k).fill(k)], `${label}: every disc has the same font (the line's) and the same of`);
      eq(S.splitWords(w.spec.personalization, k).words, Array.from({ length: k }, (_, i) => 'JQRZX'[i]), `${label}: the words go disc by disc`); }
    lines++; }
  eq(lines, 69, "lines of the six listings"); }

// ── 3 · the buyer's words, one disc at a time ─────────────────────────────────────────────────────────────────────────────────────────────
{ const words = (t, k) => { const r = S.splitWords(t, k); return r.ok ? r.words : null; };
  for (const [text, k, want, how] of [
    [['Tag 1: J, Tag 2: Q'], 2, ['J', 'Q'], 'numbered'], [['Disc 1: A; Disc 2: B'], 2, ['A', 'B'], 'numbered'], [['Initial 1 - J, Initial 2 - Q'], 2, ['J', 'Q'], 'numbered'], [['1: J, 2: Q, 3: Z'], 3, ['J', 'Q', 'Z'], 'numbered'],
    [['1) J 2) Q'], 2, ['J', 'Q'], 'numbered'], [['First disc: J, second: Q'], 2, ['J', 'Q'], 'numbered'], [['Charm #2 = Q, Charm #1 = J'], 2, ['J', 'Q'], 'numbered'], [['Tag 1: J', 'Tag 2: Q'], 2, ['J', 'Q'], 'numbered'],
    [['Tag 1: J\nTag 2: Q'], 2, ['J', 'Q'], 'numbered'], [['Tag 1: Mom and Tag 2: Dad'], 2, ['Mom', 'Dad'], 'numbered'], [['Please engrave Tag 1: J, Tag 2: Q'], 2, ['J', 'Q'], 'numbered'], [['Tag 1: ❤, Tag 2: Q'], 2, ['❤', 'Q'], 'numbered'],
    [['J', 'Q'], 2, ['J', 'Q'], 'lines'], [['J\nQ'], 2, ['J', 'Q'], 'lines'], [['J, Q'], 2, ['J', 'Q'], 'list'], [['Anna Maria, Ben'], 2, ['Anna Maria', 'Ben'], 'list'], [['A, B, C'], 3, ['A', 'B', 'C'], 'list'], [['J Q'], 2, ['J', 'Q'], 'letters'],
    [['All discs: J'], 2, ['J', 'J'], 'same'], [['J on each disc'], 2, ['J', 'J'], 'same'], [['Same on all: J'], 3, ['J', 'J', 'J'], 'same']]) {
    const r = S.splitWords(text, k); eq([r.ok, r.words, r.how], [true, want, how], `${JSON.stringify(text)} for ${k} discs`); noNested(r, 'split'); }
  for (const [text, k, why] of [[['J'], 2, /one word/], [['Tag 1: J, Tag 2: Q, Tag 3: R'], 2, /numbers 1, 2, 3/], [['Tag 1: J, Tag 1: Q'], 2, /numbers 1,/], [['Anna 1. Ben 2.'], 2, /before the numbered/], [['Tag 1:, Tag 2: Q'], 2, /no words/],
    [['16.5'], 2, /numbers 16/], [[], 2, /no words/], [['J, Q, Z'], 2, /does not say/], [['Anna Maria Ben'], 2, /does not say/], [['Tag 1: J'], 1, /fewer/]]) {
    const r = S.splitWords(text, k); ok(!r.ok && why.test(r.why), `${JSON.stringify(text)} for ${k}: not guessed (${r.why})`); }
  // wordsForDiscs: the personalisation field alone may speak
  const slots2 = ['D1', 'D2'], spec = o => Object.assign({ personalization: ['Tag 1: J, Tag 2: Q'], buyerMessage: '', staffNote: '', messages: [] }, o);
  eq(S.wordsForDiscs(spec(), slots2, { engravingNote: O.engravingNote }).words, ['J', 'Q'], 'the field says it: split');
  for (const [why, over] of [['a buyer message', { buyerMessage: 'Please make the J bigger' }], ['a staff note', { staffNote: 'Engrave (Paul): J / Q' }], ['an engraving-like staff message', { messages: [{ text: 'she wants the letters on the back of each disc' }] }]])
    eq(S.wordsForDiscs(spec(over), slots2, { engravingNote: O.engravingNote }), null, `${why}: the reader weighs it, no split here`);
  ok(S.wordsForDiscs(spec({ messages: [{ text: 'ok' }] }), slots2, { engravingNote: O.engravingNote }), 'a chat word that is no engraving note does not stop it');
  eq(S.wordsForDiscs(spec(), ['L', 'R'], {}), null, 'never for earrings'); eq(S.wordsForDiscs(spec(), ['D1'], {}), null, 'never for one disc'); }

// ── 4 · each disc its own engraving record; the per-piece view ───────────────────────────────────────────────────────────────────────────────
{ const w = world('4175370240', realLine('4175370240', 'rose', ['Tag 1: J, Tag 2: Q']));
  const SC = w.SC, parent = w.row;
  const mk = (slot, over) => { const j = Object.assign({ key: `${parent.key}#${slot}`, slot, rowKey: parent.key, groupKey: '4175370240:5217980219', lines: [], text: '', state: 'ready', copies: [], backs: [] }, over || {}); j.row = S.sideRow(SC, parent, slot, j); j.copies = j.row.poolIds; return j; };
  const d1 = mk('D1', { lines: ['J'], text: 'J', state: 'approved' }), d2 = mk('D2', { lines: ['Q'], text: 'Q' });
  eq([d1.row.poolIds, d2.row.poolIds, d1.row.key, d2.row.slot], [[parent.poolIds[0]], [parent.poolIds[1]], `${parent.key}#D1`, 'D2'], 'each disc owns its pool id, its key and its slot');
  d1.engraveRec = { needed: true, state: 'approved', approved: true, text: 'J', approvedBy: 'Paul', approvedAt: 5, seals: [{ id: 's1' }] }; d2.engraveRec = { needed: true, state: 'review', approved: false, text: 'Q' };
  S.linkParent(SC, parent, [d1, d2]);
  const sum = parent.engrave;
  eq([sum.approved, sum.state, sum.text, sum.pieces[parent.poolIds[0]].approved, sum.pieces[parent.poolIds[1]].approved], [false, 'review', 'Disc 1: J · Disc 2: Q', true, false], 'the line is approved only when EVERY disc is: disc 1 approved, disc 2 still to check');
  noNested(sum, 'summary');
  d2.engraveRec = Object.assign({}, d2.engraveRec, { state: 'approved', approved: true, approvedBy: 'Paul', approvedAt: 6 });
  eq(parent.engrave.approved, true, 'both approved: the line is');
  // the per-piece view
  const r1 = S.pieceRecord(d1, { of: 2 }), r2 = S.pieceRecord(Object.assign(d2, { state: 'review', wordsSource: 'personalization:numbered' }), { of: 2 });
  eq([r1.index, r1.of, r1.slot, r1.tag, r1.label, r1.words, r1.lines, r1.poolIds, r1.state, r1.approved, r1.approvedBy, r1.sealed, r1.groupKey], [1, 2, 'D1', 'DISC 1 of 2', 'Disc 1', 'J', ['J'], [parent.poolIds[0]], 'approved', true, 'Paul', true, '4175370240:5217980219'], 'disc 1 as a record (state is the job\'s own)');
  eq([r2.index, r2.of, r2.tag, r2.words, r2.wordsSource, r2.approved, r2.sealed], [2, 2, 'DISC 2 of 2', 'Q', 'personalization:numbered', false, false], 'disc 2 as a record');
  eq([r1.font, r1.fontAsked], [{ asked: 'Typewriter', id: 'crimson-text', name: 'Crimson Text', source: 'rule:font' }, 'Typewriter'], 'the font the buyer asked for is on every disc (the line\'s, until a font is set on the job)');
  d2.font = { asked: 'Typewriter', id: 'special-elite', name: 'Special Elite', source: 'font-map' };
  eq([S.pieceRecord(d2, { of: 2 }).font.name, S.pieceRecord(d2, { of: 2 }).fontAsked], ['Special Elite', 'Typewriter'], 'a font set on the job wins (FONTMAP)');
  const recs = S.pieceRecords([d2, d1], {}); eq(recs.map(r => [r.index, r.of]), [[1, 2], [2, 2]], 'pieceRecords: D1..Dn, the same `of`'); noNested(recs, 'records'); ok(!Array.isArray(recs[0].lines[0]), 'strings only'); }

// ── 5 · the font never holds a line: every font-like option of the Sep 17 snapshot (384 distinct lines) ───────────────────────────────────
{ let font = 0, held = 0;
  SNAP.rows.forEach((r, i) => {
    const line = { transactionId: 't' + i, listingId: 'L' + i, sku: r[1], title: r[0], quantity: r[2], metalKey: 'gold', metalLabel: 'Gold', personalization: [], variations: r[3].map(([a, b]) => V(a, b.replace(/&quot;/g, '"').replace(/&middot;/g, '·'))) };
    const sp = O.interpretLine({ receiptId: '4170000' + i, updateTs: 0 }, line, ctx({ masterEntry: s => (s ? { sku: s } : null), masterLoose: () => '' }));
    if (line.variations.some(v => O.isFontOption(v.name))) { font++; if (sp.problems.some(p => p.kind === 'needsMapping' && O.isFontOption(p.optionName))) held++; }
  });
  eq([font > 0, held], [true, 0], `${font} lines with an option named for a font (Font, Fonts, Font Choice): none is held for it`); }

// ── 6 · the bridge uses all of it ────────────────────────────────────────────────────────────────────────────────────────────────────────
{ const b = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  const split = b.indexOf('const discSplit = discWordsOf(row, jobs);'), paid = b.indexOf('agentCall("engraveIntent"');
  ok(split > 0 && paid > split, 'a counted line whose note is plain is read BEFORE the paid reader (no paid call for it)');
  ok(/if \(O\.questionGone && O\.questionGone\(row, prev\)\) \{ row\.state = "pulled"; row\.reason = null; delete row\.poolTry; \}/.test(b), 'interpretAll releases a line held for a question that is gone');
  ok(/O\.staleQuestionHold && O\.staleQuestionHold\(row\)/.test(b), "the order window's record row does too");
  const h = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
  for (const f of ['charm-nest-engrave-sides.js', 'charm-nest-orders.js', 'charm-nest-bridge.js']) ok(new RegExp(f.replace(/\./g, '\\.') + '\\?v=[^"]*-dr1(?:-[a-z0-9]+)*"').test(h), `${f}: the cache token carries -dr1 (other workers add theirs after it)`); }

console.log(`disc-read: ${n} checks passed`);
