// SETBAR-C (Paul, 13 Oct 2026, order 4202291826, Etsy listing 532401548 "Bar Necklace SET of 2"): "The system should intelligently read and understand the meaning and context of the users requirement, and based on
// that make a decision as to engraving details whether engraving should be there or not, and what the user actually wants engraved."
// The buyer wrote   16"= 33*0'45" N 96*45'53" W (the asterisk is supposed to be the little degree circle-I just don't have that on my keyboard) 18"= Ginger
// and meant         the 16" necklace: 33°0'45" N 96°45'53" W     the 18" necklace: Ginger      (every * is a degree sign; the bracket talks to the shop and is NOT engraving). The app glued it all into one text for every piece.
// Offline, no network, no browser, no paid call, nothing written:   node tests/charm-nest/engrave-read.cjs
// What it proves (charm-nest-engrave-read.js, the bridge's classifyOnce, netlify/functions/_charmNestAgent.js):
//   1  the exact buyer text reads as two wordings by length, the coordinates with real degree signs, the note left out (and said), nothing glued
//   2  five other realistic variants: one length only, Left / Right, names only (untouched), the entities &#39; &amp; &quot;, a note on a BLANK line
//   3  the asterisk is a degree sign only inside a coordinate; a name with an asterisk is left alone; minus signs, minute and second marks stay as typed
//   4  a different layout asked for (swap the bars, thick bar on the 16", both on one chain) is flagged with the buyer's own clause; the exact text is NOT flagged
//   5  to engrave or not: ENGRAVED = engrave, BLANK = no engraving (a note on it waits for a person), words and no BLANK = engrave, ENGRAVED with nothing written asks
//   6  the join (assign): by chain length, or by rank (the thinner bar on the shorter chain, Paul's rule), never a guess
//   7  every installed engraving font draws U+00B0 (all 11 files, Regular and Semibold), so no font needs the page's "Unsupported engraving characters" fallback for it; the prime marks are NOT used (four fonts lack them)
//   8  the model request: the extra rules and the "ignored" answer are added only for words that look like a note to the shop / a label / a coordinate; every other request is byte for byte what it was (cache keys stand)
//   9  the bridge (classifyOnce, vm over the real source): the exact text makes NO paid call and each piece waits for a person to confirm; an unclear text still goes to the reader, which the degree fix then corrects
//  10  the page loads the file before the bridge, with a cache token, and the public build lists it
'use strict';
const assert = require('assert/strict'), fs = require('fs'), path = require('path'), vm = require('vm'), crypto = require('crypto');
const root = path.join(__dirname, '../..');
const R = require(path.join(root, 'charm-nest-engrave-read.js'));
let pass = 0, fail = 0;
const t = (name, fn) => { try { fn(); pass++; console.log('  ok   ' + name); } catch (e) { fail++; console.log('  FAIL ' + name + '\n       ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n       ')); } };
const ta = async (name, fn) => { try { await fn(); pass++; console.log('  ok   ' + name); } catch (e) { fail++; console.log('  FAIL ' + name + '\n       ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n       ')); } };

const BUYER = `16"= 33*0'45" N 96*45'53" W (the asterisk is supposed to be the little degree circle-I just don't have that on my keyboard) 18"= Ginger`;
const COORDS = `33°0'45" N 96°45'53" W`;
const LEN = lengthsText => R.lengthsOf(lengthsText);

console.log('1 · the exact buyer text');
t('two wordings by length, degree signs, the note left out', () => {
  const r = R.readPersonalisation([BUYER], { lengths: [16, 18] });
  assert.equal(r.labelled, true);
  assert.deepEqual(r.pieces.map(p => [p.label, p.length, p.kind, p.words]), [['16"', 16, 'length', COORDS], ['18"', 18, 'length', 'Ginger']]);
  assert.equal(r.whole, '', 'no glued text is handed out for a labelled reading');
  assert.equal(r.unsure, false); assert.equal(r.layoutRequest, false);
  assert.equal(r.dropped.length, 1); assert.match(r.dropped[0].text, /^\(the asterisk is supposed to be the little degree circle-I just don't have that on my keyboard\)$/);
  assert.deepEqual(r.fixes, [{ kind: 'degree', count: 2 }]);
  assert.ok(!r.pieces.some(p => /[*()]/.test(p.words)), 'no asterisk and no bracket in any wording');
  assert.ok(r.pieces[0].words.includes('°') && r.pieces[0].words.codePointAt(2) === 0xB0, 'U+00B0');
});
t('the lengths come from the option when the caller does not know them', () => {
  const r = R.readPersonalisation([BUYER], {});
  assert.deepEqual(r.pieces.map(p => p.words), [COORDS, 'Ginger']);
});
t('say() is one plain line: what was read, the degree signs, what was left out', () => {
  const line = R.say(R.readPersonalisation([BUYER], { lengths: [16, 18] }));
  assert.match(line, /Read “16"” as “33°0'45" N 96°45'53" W” and “18"” as “Ginger”/);
  assert.match(line, /2 asterisks read as the degree sign °/);
  assert.match(line, /left out as a note to the shop: “\(the asterisk is supposed to be the little degree circle-I just don't have that on my keyboard\)”/);
});
t('the buyer\'s spelling and case stay (no capitals added, no punctuation changed)', () => {
  const r = R.readPersonalisation([`16"= gInGeR   18"= mY nAmE`], { lengths: [16, 18] });
  assert.deepEqual(r.pieces.map(p => p.words), ['gInGeR', 'mY nAmE']);
});

console.log('2 · five more variants');
t('one length only: the wording is for that length, the other piece is not given it', () => {
  const r = R.readPersonalisation(['16"= Ginger'], { lengths: [16, 18] });
  assert.deepEqual(r.pieces.map(p => [p.label, p.words]), [['16"', 'Ginger']]);
  const a = R.assign(r, [{ slot: 'D1', rank: 0 }, { slot: 'D2', rank: 1 }]);
  assert.equal(a.ok, false); assert.deepEqual(a.missing, ['D2']); assert.deepEqual(Object.keys(a.bySlot), ['D1']);
});
t('Left / Right', () => {
  const r = R.readPersonalisation(['Left: Anna Right: Ben'], { lengths: [16, 18] });
  assert.deepEqual(r.pieces.map(p => [p.label, p.kind, p.side, p.words]), [['Left', 'side', 'L', 'Anna'], ['Right', 'side', 'R', 'Ben']]);
  assert.deepEqual(R.assign(r, [{ slot: 'L' }, { slot: 'R' }]).bySlot, { L: { label: 'Left', words: 'Anna', length: null }, R: { label: 'Right', words: 'Ben', length: null } });
});
t('names only: not labelled, untouched (the existing disc / reader path decides, as before)', () => {
  for (const w of ['Ginger', 'Ginger & Catalina', 'Mom\nDad', 'I love you to the moon & back']) {
    const r = R.readPersonalisation([w], { lengths: [16, 18] });
    assert.equal(r.labelled, false); assert.equal(r.changed, false, w); assert.equal(r.whole, w.replace(/\n/g, '\n')); assert.equal(r.dropped.length, 0); assert.equal(r.unsure, false);
  }
});
t('entities: &#39; &amp; &quot; are read as the characters they are', () => {
  const r = R.readPersonalisation(['16&quot;= Mom&#39;s &amp; Dad 18&quot;= Ginger&#39;s'], { lengths: [16, 18] });
  assert.deepEqual(r.pieces.map(p => p.words), ["Mom's & Dad", "Ginger's"]);
  assert.deepEqual(R.readPersonalisation(['Mom&#39;s &amp;amp; Dad']).whole, "Mom's & Dad", 'double-escaped too');
});
t('multi-line wording keeps the buyer\'s own line breaks', () => {
  const r = R.readPersonalisation([`16" = 33*0'45" N\n96*45'53" W\n18" = Ginger`], { lengths: [16, 18] });
  assert.deepEqual(r.pieces.map(p => p.words), [`33°0'45" N\n96°45'53" W`, 'Ginger']);
});
t('other ways to label: no space, inch, bare number with a colon, slash between, "16\" Anna 18\" Ben"', () => {
  for (const [txt, want] of [[`16"=Ginger 18"=Catalina`, ['Ginger', 'Catalina']], ['16 inch: Anna, 18 inch: Ben', ['Anna', 'Ben']], ['16: Anna 18: Ben', ['Anna', 'Ben']], ['16" - Anna / 18" - Ben', ['Anna', 'Ben']], ['16" Anna 18" Ben', ['Anna', 'Ben']], ['Necklace 1 = Anna, Necklace 2 = Ben', ['Anna', 'Ben']]]) {
    const r = R.readPersonalisation([txt], { lengths: [16, 18] });
    assert.equal(r.labelled, true, txt); assert.deepEqual(r.pieces.map(p => p.words), want, txt);
  }
});
t('a lone "Long - Island" or a coordinate\'s 53" is never read as a label', () => {
  assert.equal(R.readPersonalisation(['Long - Island']).labelled, false);
  assert.equal(R.readPersonalisation([`33*0'45" N 96*45'53" W`], { lengths: [16, 18, 53] }).labelled, false);
});
t('a label used twice, or words before the first label, is "not sure": the reading is not used to decide', () => {
  assert.equal(R.readPersonalisation(['16"= A 16"= B'], { lengths: [16, 18] }).unsure, true);
  assert.equal(R.readPersonalisation(['for my dog 16"= A 18"= B'], { lengths: [16, 18] }).unsure, true);
});
t('a request that may carry the words is never cut out (it is "not sure"); a note that only explains is', () => {
  const ask = R.readPersonalisation(['Please engrave Ginger'], {}); assert.equal(ask.unsure, true); assert.equal(ask.whole, 'Please engrave Ginger');
  const expl = R.readPersonalisation([`Ginger. I just don't have the degree sign on my keyboard`], {}); assert.equal(expl.whole, 'Ginger'); assert.equal(expl.dropped.length, 1); assert.equal(expl.unsure, false);
  const only = R.readPersonalisation(['(please engrave Ginger)'], {}); assert.equal(only.unsure, true, 'never lets the notes take every word'); assert.equal(only.whole, '');
  assert.equal(R.readPersonalisation(['Ginger (my dog)'], {}).whole, 'Ginger (my dog)', 'a bracket that is no note to the shop stays the buyer\'s own');
});

console.log('3 · the degree sign');
t('an asterisk is a degree sign in a coordinate (before minutes, before a compass letter, after a decimal)', () => {
  for (const [from, to] of [[`33*0'45" N 96*45'53" W`, COORDS], ['33.4567* N 96.7654* W', '33.4567° N 96.7654° W'], ['33*N 96*W', '33°N 96°W'], ['33.4567*, -96.7654*', '33.4567°, -96.7654°'], [`33 * 0' 45"`, `33° 0' 45"`]])
    assert.equal(R.fixDegrees(from).text, to, from);
});
t('º and ˚ typed in a coordinate read as ° too (Marck Script has no º)', () => assert.equal(R.fixDegrees(`33º0'45" N 96˚45'53" W`).text, COORDS));
t('an asterisk anywhere else is the buyer\'s own', () => {
  for (const w of ['Mom*Dad', '*Ginger*', 'Anna * Ben', '5*S is a code', 'Class of 2*24']) assert.equal(R.fixDegrees(w).text, w, w);
});
t('minus signs, minute and second marks stay as typed (U+2032 / U+2033 are missing from four fonts)', () => {
  const r = R.readPersonalisation([`18"= -96.7654 33*0'45"`], { lengths: [16, 18] });
  assert.equal(r.pieces[0].words, `-96.7654 33°0'45"`); assert.ok(!/[′″]/.test(r.pieces[0].words));
});

console.log('4 · a different layout asked for');
t('the exact buyer text asks for no layout', () => { assert.equal(R.readPersonalisation([BUYER], { lengths: [16, 18] }).layoutRequest, false); assert.equal(R.layoutClause(BUYER), ''); });
t('swap / thick bar on the 16" / both on one chain / "which bar": flagged with the buyer\'s own clause', () => {
  for (const [txt, clause] of [
    [`16"= Anna 18"= Ben. Please swap the bars so the thin one is on the 18"`, 'Please swap the bars so the thin one is on the 18"'],
    ['Thick bar on the 16 inch chain. 16"= Anna 18"= Ben', 'Thick bar on the 16 inch chain.'],
    ['Ginger and Anna, both on the same chain please', 'Ginger and Anna, both on the same chain please'],
    ['Anna 16", Ben 18" (put the other way round)', 'put the other way round'],
    ['the small bar on the longer chain', 'the small bar on the longer chain']]) {
    const r = R.readPersonalisation([txt], { lengths: [16, 18] });
    assert.equal(r.layoutRequest, true, txt); assert.equal(r.layoutNote, clause, txt); assert.equal(R.layoutClause(txt), clause);
  }
});
t('a layout sentence is never engraved: it is left out and said, the wordings stay', () => {
  const r = R.readPersonalisation([`16"= Anna 18"= Ben. Please swap the bars so the thin one is on the 18"`], { lengths: [16, 18] });
  assert.deepEqual(r.pieces.map(p => p.words), ['Anna', 'Ben']); assert.equal(r.dropped[0].why, 'a request about the layout, not engraving');
});
t('ordinary words are no layout request', () => { for (const w of ['Ginger', 'Mom & Dad', 'Long Island', 'My little bar', 'Chain of love', 'Switch']) assert.equal(R.layoutClause(w) === '' || /^Switch$/.test(w), true, w); });

console.log('5 · to engrave or not');
const opt = (metal, extra) => [{ name: 'Metal · #Characters?', value: metal }, { name: 'Necklace Length·FONT', value: '16"&18"·Set·ANGELINA' }].concat(extra || []);
t('ENGRAVED + words = engrave; BLANK + nothing = no engraving; no token + words = engrave (the general rule)', () => {
  assert.equal(R.decide({ options: opt('GOLD·Set·ENGRAVED'), words: true }).engrave, 'yes');
  assert.equal(R.decide({ options: opt('GOLD·Set·BLANK'), words: false }).engrave, 'no');
  assert.equal(R.decide({ options: [{ name: 'Metal', value: 'Gold' }], words: true }).engrave, 'yes');
  assert.equal(R.decide({ options: [{ name: 'Metal', value: 'Gold' }], words: false }).engrave, 'no');
});
t('BLANK with words (or a note that asks for engraving) waits for a person; any other note on it does not ask', () => {
  const a = R.decide({ options: opt('ROSE·Set·BLANK'), words: true }); assert.equal(a.engrave, 'ask'); assert.equal(a.token, 'BLANK'); assert.match(a.why, /BLANK \(no engraving\), but the buyer wrote words: nothing is engraved until a person decides/);
  assert.equal(R.decide({ options: opt('ROSE·Set·BLANK'), words: false, note: true }).engrave, 'ask');
  assert.equal(R.decide({ options: opt('ROSE·Set·BLANK'), words: false, note: false }).engrave, 'no');
  assert.equal(R.noteAsksEngraving('Hi, could you please engrave Ginger on the bar?'), true); assert.equal(R.noteAsksEngraving('Thanks! Can you ship by Friday?'), false);
});
t('ENGRAVED with nothing written anywhere asks (the buyer paid for engraving); two options that disagree ask', () => {
  const a = R.decide({ options: opt('SILVER·Set·ENGRAVED'), words: false, note: false }); assert.equal(a.engrave, 'ask'); assert.equal(a.token, 'ENGRAVED');
  assert.equal(R.decide({ options: opt('GOLD·Set·ENGRAVED', [{ name: 'Extra', value: 'BLANK' }]), words: true }).engrave, 'ask');
});
t('ENGRAVED with the words in a message ("Please put Ginger on the bar") goes on to the reader as before: anything written is "something written"', () => {
  assert.equal(R.decide({ options: opt('GOLD·Set·ENGRAVED'), words: false, note: false, anyNote: true }).engrave, 'yes');
});
t('every real value of listing 532401548 reads right', () => {
  for (const [v, tok] of [['GOLD·Set·ENGRAVED', 'ENGRAVED'], ['ROSE·Set·ENGRAVED', 'ENGRAVED'], ['SILVER·Set·ENGRAVED', 'ENGRAVED'], ['GOLD·Set·BLANK', 'BLANK'], ['ROSE·Set·BLANK', 'BLANK'], ['SILVER·Set·BLANK', 'BLANK']])
    assert.equal(R.decide({ options: opt(v), words: false, note: false }).token, tok, v);
  assert.equal(R.decide({ options: [{ name: 'Metal', value: 'Engraved Name Plate' }], words: false }).token, '', 'only the whole token ENGRAVED counts');
});
t('the personalisation box itself is never read as an option', () => assert.equal(R.decide({ options: [{ name: 'Personalization', value: 'blank' }], words: true }).engrave, 'yes'));

console.log('6 · the join: which wording goes on which piece');
t('lengths of a set from the option value', () => {
  assert.deepEqual(LEN('16"&18"·Set·ANGELINA'), [16, 18]); assert.deepEqual(LEN('15"&17"·Set·COMIC'), [15, 17]); assert.deepEqual(LEN('18"& 20"·Set·COMIC'), [18, 20]); assert.deepEqual(LEN('17"&19"·Set·TYPEWRTE'), [17, 19]);
  assert.deepEqual(LEN('16"'), [16]); assert.deepEqual(LEN('12 & 14'), [], 'a date is no length');
  assert.deepEqual(R.lengthsOfLine({ variations: [{ name: 'Personalization', value: '12" & 14"' }, { name: 'Necklace Length·FONT', value: '17"&19"·Set·PRISTINA' }] }), [17, 19]);
});
t('by chain length (A\'s mapping hands each piece its chain), in either order of the pieces', () => {
  const r = R.readPersonalisation([BUYER], { lengths: [16, 18] });
  const a = R.assign(r, [{ slot: 'D1', length: 18 }, { slot: 'D2', length: 16 }]);
  assert.equal(a.ok, true); assert.equal(a.bySlot.D2.words, COORDS); assert.equal(a.bySlot.D1.words, 'Ginger');
});
t('by rank: the thinner bar (rank 0) on the shorter chain — the shorter label goes to rank 0 (Paul, 13 Oct)', () => {
  const r = R.readPersonalisation([BUYER], { lengths: [16, 18] });
  const a = R.assign(r, [{ slot: 'D1', rank: 0 }, { slot: 'D2', rank: 1 }]);
  assert.equal(a.ok, true); assert.equal(a.bySlot.D1.words, COORDS); assert.equal(a.bySlot.D2.words, 'Ginger');
});
t('never a guess: no slots, a label that fits no piece, or two pieces on one length assigns nothing', () => {
  const r = R.readPersonalisation([BUYER], { lengths: [16, 18] });
  assert.equal(R.assign(r, null).ok, false); assert.equal(R.assign(r, []).ok, false);
  assert.equal(R.assign(r, [{ slot: 'D1', length: 16 }, { slot: 'D2', length: 17 }]).ok, false);
  assert.deepEqual(Object.keys(R.assign(r, [{ slot: 'D1', length: 16 }, { slot: 'D2', length: 16 }]).bySlot), []);
});
t('THE HOOK: slotInfo (until set, slotsOf says nothing; set, it hands the pieces over)', () => {
  const jobs = [{ slot: 'D1' }, { slot: 'D2' }];
  assert.equal(R.slotInfo, null); assert.equal(R.slotsOf({}, jobs), null);
  R.slotInfo = (row, slot) => ({ rank: slot === 'D1' ? 0 : 1 });
  try { assert.deepEqual(R.slotsOf({}, jobs), [{ slot: 'D1', length: null, rank: 0 }, { slot: 'D2', length: null, rank: 1 }]); R.slotInfo = (row, slot) => (slot === 'D1' ? 16 : 18); assert.deepEqual(R.slotsOf({}, jobs).map(x => x.length), [16, 18]); R.slotInfo = () => { throw new Error('x'); }; assert.equal(R.slotsOf({}, jobs), null, 'a hook that throws says nothing'); }
  finally { R.slotInfo = null; }
});

console.log('7 · the fonts draw the degree sign');
const FONT_DIR = path.join(root, 'vendor/fonts');
t('all 11 installed engraving fonts (17 files) have U+00B0, the apostrophe and the quotation mark', () => {
  const O = require(path.join(root, 'charm-nest-orders.js')), ot = require(path.join(root, 'vendor/opentype-1.3.4.min.js')), G = require(path.join(root, 'charm-nest-geom.js'));
  let files = 0; const primeless = new Set();
  for (const f of O.ENGRAVING_FONTS) for (const [weight, rel] of Object.entries(f.files)) {
    const b = fs.readFileSync(path.join(root, rel)), font = ot.parse(b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength));
    const cov = G.glyphCoverage(font, `33°0'45" N 96°45'53" W Ginger`);   // (the page's own coverage check, setReady)
    assert.equal(cov.ok, true, `${f.id} ${weight} lacks ${JSON.stringify(cov.missing)}`); files++;
    if (!font.charToGlyphIndex('′') || !font.charToGlyphIndex('″')) primeless.add(f.id);
  }
  assert.equal(O.ENGRAVING_FONTS.length, 11); assert.equal(files, 17);
  // the prime marks are why the minute and second marks stay ASCII
  assert.deepEqual([...primeless].sort(), ['jost', 'jost-ace', 'marck-script', 'mynerve', 'vibur']);
});
t('the lookalikes of the set listing\'s fonts (ANGELINA, COMIC, STYLISH, TYPEWRTE, PRISTINA) are among them and draw °', () => {
  const O = require(path.join(root, 'charm-nest-orders.js'));
  const map = { ANGELINA: 'mynerve', COMIC: 'source-sans-3', STYLISH: 'playwrite-us-trad', TYPEWRTE: 'crimson-text', PRISTINA: 'marck-script' };
  for (const [w, id] of Object.entries(map)) assert.equal(O.fontOfPhrase(w), id, w);
});
t('a character a font lacks is the page\'s own fallback, never a stop: the job waits as "Unsupported engraving characters" for a person', () => {
  const G = require(path.join(root, 'charm-nest-geom.js')), ot = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
  const b = fs.readFileSync(path.join(FONT_DIR, 'Mynerve-Regular.ttf')), font = ot.parse(b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength));
  const cov = G.glyphCoverage(font, `33′ N`); assert.equal(cov.ok, false); assert.deepEqual(cov.missing, ['′']);
  const bridge = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
  assert.ok(/job\.reason = `Unsupported engraving characters: /.test(bridge) && /kind: "notRepresentable"/.test(bridge), 'setReady turns a missing glyph into a words card (Review notRepresentable), the order goes on');
});

console.log('8 · the model request');
const agentMain = fs.readFileSync(path.join(root, 'netlify/functions/_charmNestAgent.js'), 'utf8');
const server = { module: { exports: {} }, process: { env: {} }, console, require: n => (n === './_charmNestAuth' ? { str: (v, n) => String(v || '').slice(0, n), num: v => +v || 0 } : {}) };
vm.createContext(server); vm.runInContext(agentMain, server);
const A = server.module.exports, sha = x => crypto.createHash('sha256').update(JSON.stringify(x)).digest('hex').slice(0, 16);
const BASE = { order: '', sku: 'COMET_9427', title: 'Comet Necklace', form: 'necklace', quantity: 1, engravable: true, buyerMessage: '', staffNote: '', messages: [] };
t('an ordinary request is byte for byte what origin/main sent (the sandbox\'s cache of paid readings keeps answering it)', () => {
  const ask = body => { const r = A.buildRequest('engraveIntent', body); return sha([r.system, r.schema, r.content]); };
  assert.equal(ask(Object.assign({}, BASE, { personalization: ['Ginger'] })), 'd33a70fee0592b85');
  assert.equal(ask(Object.assign({}, BASE, { personalization: ['Mom & Dad', '2024'], buyerMessage: 'Hello, thanks!' })), '1d4af1581916248f');
  assert.equal(ask(Object.assign({}, BASE, { sku: 'INITIAL', title: 'Discs', personalization: ['A, B'], slots: ['D1', 'D2'] })), 'f82e108d7f3f7ca0');
});
t('words that look like a note to the shop / a label / a coordinate get the extra rules and an "ignored" answer', () => {
  for (const p of [[BUYER], ["33*0'45\" N"], ['Ginger (my dog, please)'], ['Ginger please'], ['16"= A 18"= B']]) {
    const r = A.buildRequest('engraveIntent', Object.assign({}, BASE, { personalization: p }));
    assert.match(r.system, /Words that talk to the shop are never engraved/); assert.match(r.system, /degree sign: write ° \(U\+00B0\)/);
    assert.ok(r.schema.properties.ignored && r.schema.required.includes('ignored'));
  }
  const pieces = A.buildRequest('engraveIntent', Object.assign({}, BASE, { personalization: [BUYER], slots: ['D1', 'D2'], pieceLengths: { D1: 16, D2: 18 } }));
  assert.match(pieces.system, /Never join the wordings of two labels/); assert.match(pieces.content[0].text, /Chain length of each piece, in inches: \{"D1":16,"D2":18\}/);
  assert.ok(pieces.schema.properties.pieces && pieces.schema.properties.ignored);
});
t('the schema stays inside what Claude structured output supports', () => {
  const forbidden = new Set(['maxItems', 'maxLength', 'minLength', 'maximum', 'minimum', 'multipleOf', 'uniqueItems']);
  const check = (s, p) => { for (const [k, v] of Object.entries(s)) { assert.ok(!forbidden.has(k), p + '.' + k); if (s.type === 'object') assert.equal(s.additionalProperties, false, p); if (v && typeof v === 'object' && !Array.isArray(v)) check(v, p + '.' + k); } };
  check(A.buildRequest('engraveIntent', Object.assign({}, BASE, { personalization: [BUYER] })).schema, 'schema'); check(A.buildRequest('engraveIntent', Object.assign({}, BASE, { personalization: [BUYER], slots: ['D1', 'D2'] })).schema, 'schema');
});

console.log('9 · the bridge (classifyOnce over the real source)');
const bridgeSrc = fs.readFileSync(path.join(root, 'charm-nest-bridge.js'), 'utf8');
const slice = bridgeSrc.slice(bridgeSrc.indexOf('  const classifyTasks ='), bridgeSrc.indexOf('  function setNone(job'));
function bridgeWorld(hook) {
  const E = require(path.join(root, 'charm-nest-engraving-seals.js')), Sides = require(path.join(root, 'charm-nest-engrave-sides.js'));
  const reader = R; reader.slotInfo = hook || null;   // (the module's own hook: bridgeWorld calls run one after the other)
  const calls = { agent: 0, ready: [], words: [], none: [] }, map = new Map();
  const ctx = { O: require(path.join(root, 'charm-nest-orders.js')), CharmNestEngraveRead: reader, FACES: () => null, neverEngraves: () => null, NEVER_OPEN: () => true, NEVER_WHY: 'never', CNEngravingSeals: E, render() {}, S: { settings: { engraveConfidence: 0.65 } },
    Master: { entryFor: () => ({ engravable: true }) }, items: () => map, faceOf: () => 'back', SIDES: () => Sides, agent() {}, console,
    ensureJobs: row => { if (!row.jobs) { row.jobs = (row.slots || [null]).map(sl => ({ key: row.key + (sl ? '#' + sl : ''), row, slot: sl, state: 'classify', text: null, lines: [], copies: [] })); for (const j of row.jobs) map.set(j.key, j); } return row.jobs; },
    ensureJob: row => ctx.ensureJobs(row)[0],
    agentCall: async (mode, ask) => { calls.agent++; calls.asked = ask; return ctx.answer ? ctx.answer(ask) : { skipped: 'offline' }; },
    setReady: async j => { j.state = 'ready'; calls.ready.push(j.slot); return j; }, toWords: (j, why) => { j.state = 'words'; j.reason = why; calls.words.push(j.slot); return j; }, setNone: (j, why) => { j.state = 'none'; j.reason = why; calls.none.push(j.slot); return j; } };
  vm.createContext(ctx); vm.runInContext(slice, ctx);
  return { ctx, calls, map };
}
const V = (name, value) => ({ name, value });
const rowOf = (extra) => Object.assign({ key: 'k' + Math.random().toString(36).slice(2, 7), order: { receiptId: '4202291826' }, state: 'pooled', slots: ['D1', 'D2'],
  line: { title: 'Bar Necklace SET of 2', variations: [V('Metal · #Characters?', 'GOLD·Set·ENGRAVED'), V('Necklace Length·FONT', '16"&18"·Set·ANGELINA')] },
  spec: { engraveCandidate: true, designSku: 'BAR_1548', form: 'necklace', quantity: 1, personalization: [BUYER], buyerMessage: '', staffNote: '', messages: [] } }, extra);
(async () => {
  await ta('the exact text: NO paid call; each piece waits for a person ("Words to confirm"), the coordinates on the thinner bar, Ginger on the other', async () => {
    const w = bridgeWorld((row, slot) => ({ rank: slot === 'D1' ? 0 : 1 })), row = rowOf();
    await w.ctx.classify(row);
    assert.equal(w.calls.agent, 0, 'the paid reader was not asked'); assert.deepEqual(w.calls.ready, [], 'nothing goes straight to the fit');
    const [d1, d2] = row.jobs; assert.deepEqual([d1.state, d2.state], ['words', 'words']);
    assert.equal(d1.text, COORDS); assert.equal(d2.text, 'Ginger'); assert.ok(!/Ginger/.test(d1.text) && !/33/.test(d2.text), 'never glued');
    assert.match(d1.reason, /16" piece/); assert.match(d2.reason, /18" piece/); assert.match(d1.reason, /left out as a note to the shop/);
    assert.equal(d1.quote, BUYER, 'the card keeps the buyer\'s own words beside the reading');
    assert.deepEqual(Array.from(d1.questions), [d1.reason], 'and the reading stays on the card as its note after the preview is drawn and the reason line is gone');
  });
  await ta('the hook not set yet: neither piece is given the other\'s wording; both wordings are on the card for a person', async () => {
    const w = bridgeWorld(null), row = rowOf(); await w.ctx.classify(row);
    assert.equal(w.calls.agent, 0);
    for (const j of row.jobs) { assert.equal(j.state, 'words'); assert.equal(j.text, ''); assert.match(j.reason, /Say which words go on this piece/); assert.match(j.reason, /33°0'45" N 96°45'53" W/); assert.match(j.reason, /“18"” as “Ginger”/); }
  });
  await ta('one length only: that piece gets its words, the other is asked (never given them)', async () => {
    const w = bridgeWorld((row, slot) => ({ length: slot === 'D1' ? 16 : 18 })), row = rowOf({ spec: Object.assign(rowOf().spec, { personalization: ['16"= Ginger'] }) }); await w.ctx.classify(row);
    assert.equal(w.calls.agent, 0); assert.equal(row.jobs[0].text, 'Ginger'); assert.equal(row.jobs[1].text, ''); assert.match(row.jobs[1].reason, /words for 16" only/);
  });
  await ta('a different layout asked for: nothing is assigned, a person decides, the wordings stay beside it', async () => {
    const w = bridgeWorld((row, slot) => ({ rank: slot === 'D1' ? 0 : 1 })), row = rowOf({ spec: Object.assign(rowOf().spec, { personalization: [`16"= Anna 18"= Ben. Please swap the bars so the thin one is on the 18"`] }) }); await w.ctx.classify(row);
    assert.equal(w.calls.agent, 0);
    for (const j of row.jobs) { assert.equal(j.state, 'words'); assert.equal(j.text, ''); assert.match(j.reason, /The buyer asks for a different layout: “Please swap the bars so the thin one is on the 18"”/); assert.match(j.reason, /16" → Anna · 18" → Ben/); }
  });
  await ta('a layout request in the buyer\'s message waits for a person too', async () => {
    const w = bridgeWorld(null), row = rowOf({ spec: Object.assign(rowOf().spec, { personalization: [], buyerMessage: 'Can the thick bar go on the 16 inch chain instead?' }) }); await w.ctx.classify(row);
    for (const j of row.jobs) { assert.equal(j.state, 'words'); assert.match(j.reason, /different layout/); }
  });
  await ta('BLANK with a note that asks for engraving: nothing is engraved, a person decides; ENGRAVED with nothing written asks', async () => {
    let w = bridgeWorld(null), row = rowOf({ line: { title: 'Bar Necklace SET of 2', variations: [V('Metal · #Characters?', 'ROSE·Set·BLANK'), V('Necklace Length·FONT', '17"&19"·Set·COMIC')] }, spec: Object.assign(rowOf().spec, { personalization: [], buyerMessage: 'Hi! Could you please engrave Ginger on the bar?' }) });
    await w.ctx.classify(row); assert.equal(w.calls.agent, 0); for (const j of row.jobs) { assert.equal(j.state, 'words'); assert.match(j.reason, /BLANK \(no engraving\), but a note asks for engraving/); }
    w = bridgeWorld(null); row = rowOf({ spec: Object.assign(rowOf().spec, { personalization: [], engraveCandidate: false }) });
    await w.ctx.classify(row); assert.equal(w.calls.agent, 0);
    // (engraveCandidate false stops the line before the rules, exactly as before: raiseCandidate raises it on the page, see the browser test)
    assert.deepEqual(w.calls.none, ['D1', 'D2']);
  });
  await ta('ENGRAVED with the words only in a message: the reader reads the message as it always did (nothing is asked in its place)', async () => {
    const w = bridgeWorld(null), row = rowOf({ slots: [null], spec: Object.assign(rowOf().spec, { personalization: [], buyerMessage: 'Hi! Please put Ginger on the bar, thanks' }) });
    w.ctx.answer = () => ({ engrave: true, text: 'Ginger', source: 'buyerMessage', sourceQuote: 'Please put Ginger', requests: { side: 'back', font: null, handwriting: false, image: false }, questions: [], confidence: 0.9 });
    await w.ctx.classify(row); assert.equal(w.calls.agent, 1); assert.equal(row.jobs[0].text, 'Ginger');
  });
  await ta('names only: the rules stand aside and the line goes to the reader exactly as before; the reader\'s asterisk is then a degree sign', async () => {
    const w = bridgeWorld(null), row = rowOf({ slots: [null], spec: Object.assign(rowOf().spec, { personalization: ['Please engrave 33*0\'45" N 96*45\'53" W'] }) });
    w.ctx.answer = () => ({ engrave: true, text: '33*0\'45" N 96*45\'53" W', source: 'personalization', sourceQuote: 'x', requests: { side: 'back', font: null, handwriting: false, image: false }, questions: [], confidence: 0.9, ignored: ['Please engrave'] });
    await w.ctx.classify(row); assert.equal(w.calls.agent, 1, 'unclear words are still read by the reader');
    assert.equal(row.jobs[0].text, COORDS, 'the degree sign, not the asterisk'); assert.deepEqual(row.jobs[0].ignored, ['Please engrave']);
    const w2 = bridgeWorld(null), r2 = rowOf({ slots: [null], spec: Object.assign(rowOf().spec, { personalization: ['Ginger'] }) });
    w2.ctx.answer = () => ({ engrave: true, text: 'Ginger', source: 'personalization', sourceQuote: 'Ginger', requests: { side: 'back', font: null, handwriting: false, image: false }, questions: [], confidence: 0.99 });
    await w2.ctx.classify(r2); assert.equal(w2.calls.agent, 1); assert.deepEqual(w2.calls.ready, [null], 'a plain name goes the usual way');
  });
  await ta('a single necklace whose words only need the note and the degree signs put right: no paid call, the words wait to be confirmed', async () => {
    const w = bridgeWorld(null), row = rowOf({ slots: [null], line: { title: 'Coordinates necklace', variations: [] }, spec: Object.assign(rowOf().spec, { personalization: [`33*0'45" N 96*45'53" W (I don't have the degree sign on my keyboard)`] }) });
    await w.ctx.classify(row); assert.equal(w.calls.agent, 0); assert.equal(row.jobs[0].state, 'words'); assert.equal(row.jobs[0].text, COORDS); assert.match(row.jobs[0].reason, /confirm the words/);
  });
  await ta('someone else has spoken (a staff note): the rules read nothing, the line goes on as before', async () => {
    const w = bridgeWorld(null), row = rowOf({ spec: Object.assign(rowOf().spec, { staffNote: 'Engrave (Paul): Ginger' }) }); w.ctx.answer = () => ({ skipped: 'offline' });
    await w.ctx.classify(row); assert.equal(w.calls.agent, 1);
  });
  await ta('a job a person already decided is never touched', async () => {
    const w = bridgeWorld(null), row = rowOf(); w.ctx.ensureJobs(row); row.jobs[0].state = 'approved'; row.jobs[1].state = 'approved';
    w.ctx.answer = () => ({ skipped: 'offline' }); await w.ctx.classify(row);
    assert.deepEqual(w.calls.words, [], 'the rules did not put an approved job back to words');
  });

  console.log('10 · the page');
  t('charm-nest-1.html loads the reader before the bridge, with a cache token on both; the public build lists it', () => {
    const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), a = html.indexOf('charm-nest-engrave-read.js?v='), b = html.indexOf('<script src="charm-nest-bridge.js?v=');
    assert.ok(a > 0 && b > a, 'loaded before the bridge'); assert.match(html, /charm-nest-bridge\.js\?v=[^"]*-sbc1(?:-[a-z0-9]+)*"/);
    assert.ok(/charm-nest-engrave-read\.js"/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')));
  });
  t('the file is pure: it touches no network, storage, clock or DOM', () => {
    const src = fs.readFileSync(path.join(root, 'charm-nest-engrave-read.js'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
    for (const w of ['fetch(', 'XMLHttpRequest', 'localStorage', 'sessionStorage', 'Date.now', 'document.', 'window.', 'indexedDB', 'firebase']) assert.ok(!src.includes(w), w);
  });
  console.log(fail ? `\nFAILED: ${pass} passed, ${fail} failed` : `\nPASSED: ${pass} passed, 0 failed`);
  process.exitCode = fail ? 1 : 0;
})();
