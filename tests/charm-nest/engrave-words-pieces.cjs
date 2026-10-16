// PIECES · WORDS PER PIECE (Paul, 16 Oct 2026, order 4195059483 "Mandy Bellamy", line "CORGI 34728 · SS · x2": "Customer ordered two of the same Charm, but the engraving only shows one, sometimes customers will
// request different engraving for each piece, even though the two pieces are the same.") Offline, node only, no page, no network, no paid call, nothing written:
//   node tests/charm-nest/engrave-words-pieces.cjs
// What it proves (charm-nest-engrave-piece-words.js: read(texts, n) and forSlots(spec, slots), the deterministic default of what each piece of a quantity-N line starts with):
//   1  the buyer's text clearly holds N parts (lines, / & + , ; | and, "Anna & Ben", "Mom, Dad, Me"): piece 1 gets the first, piece 2 the second ...   N = 2, 3, 4, 6
//   2  numbered and labelled parts (1) 2) · 1. 2. · Piece 1: · #1 · First: · Left: Right:) are assigned by their number or side, in any order
//   3  fewer or more parts than N, or a text that is not a list (one name, an address, a date, coordinates, "Mom & Dad forever", "Jack and Jill" for one piece): the WHOLE text on every piece, `same` true, the calm note
//   4  empty, "N/A", "none", "see my message", the real order 4195059483's personalisation: no words; the real "Test" back text: the whole text on both pieces
//   5  the buyer said "the same on both": said true; "Anna / N/A": the second piece has no words (blank), a proposal for a person
//   6  unicode, emoji, entities, newlines of every kind, invisible characters, very long text, a pathological text: the words are the buyer's own, never lost, never silently duplicated, never slow, never a throw
//   7  forSlots: slot names are only keys (exactly as passed), another voice on the order defers to the reader, a quantity-1 line passes through byte for byte
//   8  nothing else moves: the ears/discs words (splitWords, wordsForDiscs) answer as before; the file is pure; the page and the public build load it
'use strict';
const assert = require('assert/strict'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const W = require(path.join(root, 'charm-nest-engrave-piece-words.js'));
let pass = 0, fail = 0;
const t = (name, fn) => { try { fn(); pass++; console.log('  ok   ' + name); } catch (e) { fail++; console.log('  FAIL ' + name + '\n       ' + String(e && e.stack || e).split('\n').slice(0, 6).join('\n       ')); } };
const show = x => String(JSON.stringify(x)).slice(0, 90);

/* every row: [label, texts, n, expected]. expected: words (exact), how, and any of split / same / said / none / sure / blank / found, why (RegExp) */
const KEYS = ['split', 'same', 'said', 'none', 'sure', 'blank', 'found', 'how'];
function row(label, texts, n, want) {
  t(`${label}  [${show(texts)} for ${n}]`, () => {
    const r = W.read(texts, n);
    assert.equal(r.n, Math.min(Math.max(Math.floor(n), 1), 400));
    assert.equal(r.words.length, r.n, 'always exactly n words');
    if (want.words) assert.deepEqual(r.words, want.words);
    for (const k of KEYS) if (k in want) assert.deepEqual(r[k], want[k], k);
    if (want.why) assert.match(r.why, want.why);
    // the invariants every answer keeps
    assert.ok(r.words.every(w => typeof w === 'string'));
    if (r.same) assert.ok(r.words.every(w => w && w === r.words[0]), 'same: one text on every piece');
    if (r.split) assert.deepEqual(r.parts, r.words, 'split: the parts are the words');
    if (!r.split && !r.none && r.n >= 2) { assert.ok(r.same, 'not split: the whole text on every piece'); assert.equal(r.note, W.SAME_NOTE); }
    if (r.none) assert.ok(r.words.every(w => w === '') && !r.split && !r.same);
    if (!r.same || r.split) assert.equal(r.note, '');
  });
}
const split = (label, texts, n, words, how, sure) => row(label, texts, n, { words, how, split: true, same: false, sure: !!sure });
const whole = (label, texts, n, why) => { const w = [].concat(texts).join('\n'); row(label, texts, n, Object.assign({ words: Array(n).fill(w), how: 'whole', split: false, same: true, said: false, none: false, sure: false }, why ? { why } : {})); };
const none = (label, texts, n) => row(label, texts, n, { words: Array(n).fill(''), how: 'none', none: true, split: false, same: false, sure: false });

console.log('1 · the text holds N parts in one line: assigned in order (a proposal for a person to confirm)');
split('two names with &', 'Anna & Ben', 2, ['Anna', 'Ben'], 'list');
split('two names with "and"', 'Anna and Ben', 2, ['Anna', 'Ben'], 'list');
split('two names with a slash', 'Anna / Ben', 2, ['Anna', 'Ben'], 'list');
split('two names with a comma', 'Anna, Ben', 2, ['Anna', 'Ben'], 'list');
split('two names with a semicolon', 'Anna; Ben', 2, ['Anna', 'Ben'], 'list');
split('two names with a plus', 'Anna + Ben', 2, ['Anna', 'Ben'], 'list');
split('two names with a bar', 'Anna | Ben', 2, ['Anna', 'Ben'], 'list');
split('tight ampersand', 'Anna&Ben', 2, ['Anna', 'Ben'], 'list');
split('tight slash', 'Anna/Ben', 2, ['Anna', 'Ben'], 'list');
split('tight comma between words', 'Anna,Ben', 2, ['Anna', 'Ben'], 'list');
split('capitals stay as typed', 'ANNA AND BEN', 2, ['ANNA', 'BEN'], 'list');
split('lower case stays as typed', 'anna & ben', 2, ['anna', 'ben'], 'list');
split('Mom and Dad', 'Mom and Dad', 2, ['Mom', 'Dad'], 'list');
split('spaces collapse, edges trimmed', '   Anna     &    Ben   ', 2, ['Anna', 'Ben'], 'list');
split('a trailing comma is no third part', 'Anna, Ben,', 2, ['Anna', 'Ben'], 'list');
split('full names of equal length around "&"', 'Mary Jane & Ann Marie', 2, ['Mary Jane', 'Ann Marie'], 'list');
split('full names around a comma', 'Anna Smith, Ben Jones', 2, ['Anna Smith', 'Ben Jones'], 'list');
split('a slash between two groups beats the commas inside them', 'Smith, Anna / Jones, Ben', 2, ['Smith, Anna', 'Jones, Ben'], 'list');
split('Spanish "y"', 'Ana y Luis', 2, ['Ana', 'Luis'], 'list');
split('German "und"', 'Hans und Greta', 2, ['Hans', 'Greta'], 'list');
split('French "et"', 'Marie et Jean', 2, ['Marie', 'Jean'], 'list');
split('accents', 'José & María', 2, ['José', 'María'], 'list');
split('diaeresis and acute', 'Zoë, Chloé', 2, ['Zoë', 'Chloé'], 'list');
split('Chinese with a slash', '李明 / 王芳', 2, ['李明', '王芳'], 'list');
split('Chinese enumeration comma', '李明、王芳', 2, ['李明', '王芳'], 'list');
split('Chinese comma', '李明，王芳', 2, ['李明', '王芳'], 'list');
split('Arabic comma', 'علي، سارة', 2, ['علي', 'سارة'], 'list');
split('full-width ampersand', 'Anna＆Ben', 2, ['Anna', 'Ben'], 'list');
split('emoji are words', '❤️ & 🐶', 2, ['❤️', '🐶'], 'list');
split('a family emoji keeps its joiners', '👨‍👩‍👧 & 🐶', 2, ['👨‍👩‍👧', '🐶'], 'list');
split('combining accents stay as typed', 'José & Mariá', 2, ['José', 'Mariá'], 'list');
split('Etsy hands the words escaped: decoded', 'Mom&#39;s &amp; Dad', 2, ["Mom's", 'Dad'], 'list');
split('the box as an array of one', ['Anna, Ben'], 2, ['Anna', 'Ben'], 'list');
split('invisible characters are not words', 'Anna​ & Ben﻿', 2, ['Anna', 'Ben'], 'list');
split('no-break spaces are spaces', 'Anna & Ben', 2, ['Anna', 'Ben'], 'list');
split('tabs are spaces', 'Anna\t/\tBen', 2, ['Anna', 'Ben'], 'list');
split('N=3 comma list (the brief)', 'Mom, Dad, Me', 3, ['Mom', 'Dad', 'Me'], 'list');
split('N=3 comma and ampersand', 'Mom, Dad & Me', 3, ['Mom', 'Dad', 'Me'], 'list');
split('N=3 Oxford comma', 'Mom, Dad, and Me', 3, ['Mom', 'Dad', 'Me'], 'list');
split('N=3 slashes', 'Mom/Dad/Me', 3, ['Mom', 'Dad', 'Me'], 'list');
split('N=3 "and" twice', 'Mom and Dad and Me', 3, ['Mom', 'Dad', 'Me'], 'list');
split('N=4 commas', 'Anna, Ben, Cleo, Dan', 4, ['Anna', 'Ben', 'Cleo', 'Dan'], 'list');
split('N=4 two groups of two', 'Anna & Ben / Cleo & Dan', 4, ['Anna', 'Ben', 'Cleo', 'Dan'], 'list');
split('N=6 semicolons', 'A1; B2; C3; D4; E5; F6', 6, ['A1', 'B2', 'C3', 'D4', 'E5', 'F6'], 'list');
split('N=6 names', 'Amy, Bob, Cat, Dee, Eli, Fay', 6, ['Amy', 'Bob', 'Cat', 'Dee', 'Eli', 'Fay'], 'list');
row('N=2 the same word twice is the buyer\'s own choice: split, and the words happen to be the same', 'Anna, Anna', 2, { words: ['Anna', 'Anna'], how: 'list', split: true, same: true, said: false, sure: false });

console.log('2 · separate lines or boxes: assigned in order (ready)');
split('two lines', 'Anna\nBen', 2, ['Anna', 'Ben'], 'lines', true);
split('two boxes of the personalisation field', ['Anna', 'Ben'], 2, ['Anna', 'Ben'], 'lines', true);
split('Windows line breaks', 'Anna\r\nBen', 2, ['Anna', 'Ben'], 'lines', true);
split('an old Mac line break', 'Anna\rBen', 2, ['Anna', 'Ben'], 'lines', true);
split('a Unicode line separator', 'Anna Ben', 2, ['Anna', 'Ben'], 'lines', true);
split('empty lines are no parts', '\nAnna\n\n\nBen\n\n', 2, ['Anna', 'Ben'], 'lines', true);
split('lines trimmed', '  Anna \n   Ben  ', 2, ['Anna', 'Ben'], 'lines', true);
split('long phrases on their own lines are fine', 'Happy Birthday Mom, love you always\nMerry Christmas Dad, miss you every day', 2, ['Happy Birthday Mom, love you always', 'Merry Christmas Dad, miss you every day'], 'lines', true);
split('a date on its own line is that piece\'s words', 'June 5, 2024\nJuly 7, 2025', 2, ['June 5, 2024', 'July 7, 2025'], 'lines', true);
split('a coordinate on its own line is that piece\'s words', '33°0\'45" N 96°45\'53" W\n34°2\'10" N 97°1\'1" W', 2, ['33°0\'45" N 96°45\'53" W', '34°2\'10" N 97°1\'1" W'], 'lines', true);
split('N=3 lines', 'Mom\nDad\nMe', 3, ['Mom', 'Dad', 'Me'], 'lines', true);
split('N=4 boxes', ['W', 'X', 'Y', 'Z'], 4, ['W', 'X', 'Y', 'Z'], 'lines', true);
split('N=6 lines', 'a\nb\nc\nd\ne\nf', 6, ['a', 'b', 'c', 'd', 'e', 'f'], 'lines', true);
row('three lines for two pieces: nothing is dropped, nothing is guessed', 'Anna\nBen\nCarl', 2, { words: Array(2).fill('Anna\nBen\nCarl'), how: 'whole', same: true, split: false, why: /3 lines for 2 pieces/ });
row('a box that says N/A leaves that piece blank (a proposal)', ['Anna', 'N/A'], 2, { words: ['Anna', ''], how: 'lines', split: true, same: false, sure: false, blank: [2] });

console.log('3 · numbered and labelled parts: assigned by number or side (ready)');
split('1) 2)', '1) Anna 2) Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('1. 2. on lines', '1. Anna\n2. Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('1: 2: with a comma', '1: Anna, 2: Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('(1) (2)', '(1) Anna (2) Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('#1 #2', '#1 Anna #2 Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('Piece 1: Piece 2:', 'Piece 1: Anna Piece 2: Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('Piece 1 - on lines', 'Piece 1 - Anna\nPiece 2 - Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('Charm 1: Charm 2:', 'Charm 1: Anna, Charm 2: Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('Tag #1 Tag #2', 'Tag #1 Anna Tag #2 Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('Earring 1: Earring 2:', 'Earring 1: Anna, Earring 2: Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('First: Second:', 'First: Anna, Second: Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('1st piece: 2nd piece:', '1st piece: Anna / 2nd piece: Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('numbers out of order go to their own piece', '2) Ben 1) Anna', 2, ['Anna', 'Ben'], 'numbered', true);
split('Left: Right: (labelled)', 'Left: Anna Right: Ben', 2, ['Anna', 'Ben'], 'labelled', true);
split('Right first, Left second', 'Right: Ben, Left: Anna', 2, ['Anna', 'Ben'], 'labelled', true);
split('L: R:', 'L: Anna, R: Ben', 2, ['Anna', 'Ben'], 'labelled', true);
split('Left ear: Right ear:', 'Left ear: Anna / Right ear: Ben', 2, ['Anna', 'Ben'], 'labelled', true);
split('a lead-in word before the marks', 'Please engrave: 1) Anna 2) Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('an ampersand inside a part stays in it', 'Piece 1: Anna & Co Piece 2: Ben', 2, ['Anna & Co', 'Ben'], 'numbered', true);
split('long phrases per number', '1) Happy Birthday Mom 2) Merry Christmas Dad', 2, ['Happy Birthday Mom', 'Merry Christmas Dad'], 'numbered', true);
split('numeric words per number', '1) 2020 2) 2021', 2, ['2020', '2021'], 'numbered', true);
split('N=3 numbered', '1) A 2) B 3) C', 3, ['A', 'B', 'C'], 'numbered', true);
split('N=3 Piece n:', 'Piece 1: A, Piece 2: B, Piece 3: C', 3, ['A', 'B', 'C'], 'numbered', true);
split('N=4 numbered on lines', '1. W\n2. X\n3. Y\n4. Z', 4, ['W', 'X', 'Y', 'Z'], 'numbered', true);
split('N=6 numbered', '1) a 2) b 3) c 4) d 5) e 6) f', 6, ['a', 'b', 'c', 'd', 'e', 'f'], 'numbered', true);
split('N=6 numbered backwards', '6) f 5) e 4) d 3) c 2) b 1) a', 6, ['a', 'b', 'c', 'd', 'e', 'f'], 'numbered', true);
row('a numbered piece that says N/A is blank (a proposal)', '1) Anna\n2) N/A', 2, { words: ['Anna', ''], how: 'numbered', split: true, sure: false, blank: [2] });
whole('a numbered piece without words', '1) Anna 2)', 2, /no words/);
whole('three numbers for two pieces', '1) A 2) B 3) C', 2, /do not fit the 2 pieces/);
whole('two numbers for three pieces', '1) A 2) B', 3, /do not fit the 3 pieces/);
whole('a number used twice', '1) A 1) B', 2, /do not fit/);
whole('a number skipped', '1) A 3) B', 2, /do not fit/);
whole('Left / Right for three pieces', 'Left: A Right: B', 3, /Left \/ Right/);
whole('Left / Right for four pieces', 'Left: A Right: B', 4, /Left \/ Right/);
whole('Left twice', 'Left: A Left: B', 2, /Left \/ Right/);
whole('labels of two kinds', 'Left: A, 2) B', 2, /different kinds/);
whole('other words before the numbered ones', 'Hello! 1) Anna 2) Ben', 2, /before/);
whole('a lone marker is no numbering', 'Piece 1: Anna', 2, null);
whole('"Front:" "Back:" are faces, not pieces', 'Front: Anna, Back: Ben', 2, /front and the back/);
whole('"Front -" "Back -" are faces too', 'Front - Anna Back - Ben', 2, /front and the back/);

console.log('4 · not clearly N parts: the WHOLE text on every piece, said calmly');
whole('one name, two pieces', 'Anna', 2, /one text/);
whole('one name, three pieces', 'Anna', 3, /one text/);
whole('one name, four pieces', 'Anna', 4, /one text/);
whole('one name, six pieces', 'Anna', 6, /one text/);
whole('the real order\'s back text', 'Test', 2, /one text/);
whole('three parts for two pieces', 'Mom, Dad, Me', 2, /3 parts for 2 pieces/);
whole('two parts for three pieces', 'Mom, Dad', 3, /2 parts for 3 pieces/);
whole('four parts for three pieces', 'Mom, Dad, Me, Sis', 3, /4 parts for 3 pieces/);
whole('"Jack and Jill" for three pieces', 'Jack and Jill', 3, /2 parts for 3 pieces/);
whole('a US address', '123 Main St, Springfield, IL 62704', 3, /address/);
whole('an address with a flat', 'Flat 4, 10 Downing Street, London', 3, /address/);
whole('an address with a house number first', '221B Baker Street, London', 2, /address/);
whole('a P.O. box', 'PO Box 123, Austin, TX 78701', 3, /address/);
whole('a UK postcode', 'SW1A 1AA, London', 2, /address/);
whole('a date with a month name', 'June 5, 2024', 2, /date/);
whole('a day first date', '5 June, 2024', 2, /date/);
whole('a numeric date with slashes', '05/06/2024', 3, /date/);
whole('a numeric date with dots', '5.6.24', 3, /date/);
whole('an ISO date', '2024-06-05', 3, /date/);
whole('a short date', '12/25', 2, null);
whole('the real fixture back text "1. 27. 24" for two pieces', '1. 27. 24', 2, null);
whole('the real fixture back text "1. 27. 24" for three pieces', '1. 27. 24', 3, null);
whole('a time pair', '10:30 / 11:45', 2, /date/);
whole('coordinates with degree signs', '33°0\'45" N, 96°45\'53" W', 2, /coordinates/);
whole('a decimal coordinate pair', '33.4567, -96.7654', 2, /coordinates/);
whole('"Mom & Dad forever" is one phrase', 'Mom & Dad forever', 2, /differ in length/);
whole('"Mary Jane and John" is not two equal names', 'Mary Jane and John', 2, /differ in length/);
whole('a greeting and a name', 'Love, Mom', 2, /greeting/);
whole('a name with a suffix', 'John Smith, Jr', 2, /greeting/);
whole('long phrases in one line are no list', 'Happy birthday to you Mom, Merry Christmas to Dad', 2, /long phrases/);
whole('"AT&T" is one word', 'AT&T', 2, null);
whole('"R&D" is one word', 'R&D', 2, null);
whole('"24/7" is one word', '24/7', 2, null);
whole('"w/ love" is not two parts', 'w/ love', 2, null);
whole('"1,000" is one number', '1,000', 2, null);
whole('an e-mail address list', 'a@b.com, c@d.com', 2, /contact/);
whole('a phone number and a name', '555 123 4567, Anna', 2, /contact/);
whole('two web addresses', 'www.x.com / www.y.com', 2, /contact/);
whole('a name with a hyphen', 'Anna-Ben', 2, null);
whole('a dash between words is no separator', 'Anna - Ben', 2, null);
whole('a heart between names is the buyer\'s own glyph', 'Anna ❤️ Ben', 2, null);
whole('"x" between names is no separator', 'Anna x Ben', 2, null);
whole('single letters with spaces are initials of one thing, not N parts', 'J R', 2, null);
whole('"All-Star" is a name, not "all: Star"', 'All-Star', 2, null);
whole('a long text with many commas', 'word, '.repeat(200) + 'end', 2, /long text/);
whole('a sentence of many parts', 'This, that, and the other thing about us all', 2, /parts for 2 pieces/);

console.log('5 · nothing to write');
for (const x of ['N/A', 'n/a', 'NA', 'N.A.', 'N / A', 'N/A.', '"N/A"', 'None', 'none', 'No', 'nil', 'Nothing', 'nada', 'Blank', 'TBD', 'No personalization', 'No personalisation needed', 'no engraving', 'None needed',
  'Not requested on this item.', 'Not applicable', 'see message', 'Please see my message', 'see notes', 'see the photo', 'as per my message', 'check message', 'will message you', '-', '--', '...', '?', '   ', '', '\n\n', '(none)', '*']) none(`"${x.replace(/\n/g, '\\n')}" says nothing`, x, 2);
none('null', null, 2); none('undefined', undefined, 3); none('an empty list', [], 2); none('empty boxes', ['', '  '], 2); none('every box says N/A', ['N/A', 'n/a'], 3);
none('the REAL order 4195059483: personalisation "N/A" for its 2 pieces', ['N/A'], 2);
none('both parts blank', 'N/A / none', 2);
for (const x of ['Nana', 'Nathan', 'No Way', 'None of your business', 'Naomi', 'Nil Desperandum', 'NA NA', '0', 'X', '❤️']) t(`"${x}" is a word, not "nothing"`, () => assert.equal(W.read(x, 2).none, false));
t('isNoWords answers the same question', () => { assert.equal(W.isNoWords('N/A'), true); assert.equal(W.isNoWords(['', ' ']), true); assert.equal(W.isNoWords('Anna'), false); assert.equal(W.isNoWords('🐶'), false); });

console.log('6 · "the same on every piece", said by the buyer');
row('Anna on both', 'Anna on both', 2, { words: ['Anna', 'Anna'], how: 'same', said: true, same: true, split: false, sure: true, note: W.SAME_NOTE });
row('Anna on each piece (N=3)', 'Anna on each piece', 3, { words: ['Anna', 'Anna', 'Anna'], how: 'same', said: true, sure: true });
row('Same on all: Anna', 'Same on all: Anna', 3, { words: Array(3).fill('Anna'), how: 'same', said: true });
row('All: Anna', 'All: Anna', 2, { words: ['Anna', 'Anna'], how: 'same', said: true });
row('Both - Anna', 'Both - Anna', 2, { words: ['Anna', 'Anna'], how: 'same', said: true });
row('Same: Anna', 'Same: Anna', 2, { words: ['Anna', 'Anna'], how: 'same', said: true });
row('Anna, same on both', 'Anna, same on both', 2, { words: ['Anna', 'Anna'], how: 'same', said: true });
row('Anna (on both)', 'Anna (on both)', 2, { words: ['Anna', 'Anna'], how: 'same', said: true });
row('a list said to be the same: it is ONE text on both', 'Mom & Dad on both', 2, { words: ['Mom & Dad', 'Mom & Dad'], how: 'same', said: true, split: false });
row('N=6 same on all', 'Same on all: Love', 6, { words: Array(6).fill('Love'), how: 'same', said: true });

whole('"Love for all" is a message, not "same on all"', 'Love for all', 2, null);
whole('"Anna on all" is not "same on all pieces"', 'Anna on all', 2, null);
row('Anna on all pieces', 'Anna on all pieces', 3, { words: Array(3).fill('Anna'), how: 'same', said: true });
row('Anna for each', 'Anna for each', 2, { words: ['Anna', 'Anna'], how: 'same', said: true });
row('Anna, the same on each piece', 'Anna, the same on each piece', 2, { words: ['Anna', 'Anna'], how: 'same', said: true });
whole('"Same on all" without words is no words to give', 'Same on all', 2, null);

console.log('6b · a label in front of the words');
split('"Names:" in front of a list', 'Names: Anna, Ben', 2, ['Anna', 'Ben'], 'list');
split('"Names:" in front of an "and" list', 'Names: Anna and Ben', 2, ['Anna', 'Ben'], 'list');
split('"Names:" on a line of its own', 'Names:\nAnna\nBen', 2, ['Anna', 'Ben'], 'lines', true);
whole('"Names: Anna" for two pieces keeps the label: the buyer\'s own text, whole', 'Names: Anna', 2, null);
split('"Name 1: .. Name 2: .."', 'Name 1: Anna Name 2: Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('"No. 1 .. No. 2 .."', 'No. 1 Anna No. 2 Ben', 2, ['Anna', 'Ben'], 'numbered', true);
split('"Left - .. / Right - .."', 'Left - Anna / Right - Ben', 2, ['Anna', 'Ben'], 'labelled', true);

console.log('7 · quantity 1 and out-of-range counts');
t('quantity 1: the text passes through byte for byte (no decoding, no N/A, no trim)', () => {
  for (const x of ['Anna & Ben', 'N/A', 'Mom&#39;s', '  Anna  ', 'Anna\nBen', '1) Anna 2) Ben', '', 'Jack and Jill']) {
    const r = W.read(x, 1); assert.deepEqual(r.words, [x]); assert.equal(r.how, 'single'); assert.equal(r.split, false); assert.equal(r.same, false); assert.equal(r.none, false); assert.equal(r.note, '');
  }
  assert.deepEqual(W.read(['A', 'B'], 1).words, ['A\nB']); assert.deepEqual(W.read(null, 1).words, ['']);
});
t('"Jack and Jill" for one piece is one name', () => { assert.deepEqual(W.read('Jack and Jill', 1).words, ['Jack and Jill']); });
t('a count under 1, not a number or a fraction reads as one piece', () => { for (const n of [0, -3, NaN, undefined, null, 'x', 0.5, 1.9]) { const r = W.read('Anna & Ben', n); assert.equal(r.n, 1); assert.deepEqual(r.words, ['Anna & Ben']); } });
t('a count given as text or with decimals', () => { assert.deepEqual(W.read('Anna & Ben', '2').words, ['Anna', 'Ben']); assert.deepEqual(W.read('Anna & Ben', 2.9).words, ['Anna', 'Ben']); });
t('400 pieces is the most', () => { const r = W.read('x', 1000); assert.equal(r.n, 400); assert.equal(r.words.length, 400); });
t('400 lines for 400 pieces', () => { const lines = Array.from({ length: 400 }, (_, i) => 'w' + i); const r = W.read(lines.join('\n'), 400); assert.equal(r.split, true); assert.deepEqual(r.words, lines); });

console.log('8 · never lost, never silently duplicated, never slow, never a throw');
const rng = (seed => () => (seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0) / 4294967296)(20261016);
const pick = a => a[Math.floor(rng() * a.length)];
t('300 generated lists of N names (N 2..6, every separator, unicode): every name on its own piece, in order', () => {
  const names = ['Anna', 'Ben', 'José', 'Zoë', 'Алёна', '李明', 'Mom', 'Dad', 'Me', 'Sis', 'Bro', 'Nana', 'Opa', 'Émile', 'Søren', 'Ña', 'Sam', 'Lee', 'Cleo', 'Dot'];
  const seps = [', ', ' / ', '; ', ' | ', ' & ', ' + ', ' and ', ',', '/', '\n'];
  for (let i = 0; i < 300; i++) {
    const n = 2 + Math.floor(rng() * 5), list = []; while (list.length < n) { const x = pick(names); if (!list.includes(x)) list.push(x); }
    const sep = pick(seps); if (/^[,/]$/.test(sep) && list.some(x => !/\p{L}{2}/u.test(x))) continue;
    const text = list.join(sep), r = W.read(text, n);
    assert.deepEqual(r.words, list, `${show(text)} for ${n}`); assert.equal(r.split, true); assert.equal(r.same, false);
    const bad = W.read(text, n + 1); assert.equal(bad.split, false, 'one piece more than the parts: not split'); assert.ok(bad.words.every(w => w === text), 'the whole text on every piece');
    if (n > 2) { const few = W.read(text, n - 1); assert.equal(few.split, false, 'one piece fewer: not split'); }
  }
});
t('2000 random texts, n 1..8: never throws, always n words, the invariants hold, the answer is repeatable', () => {
  const pool = ['Anna', 'Ben', ' ', ' ', ',', ';', '/', '&', '+', '|', ' and ', '\n', '1)', '2)', '3.', '#1', 'Piece 2:', 'Left:', 'Right:', 'N/A', 'none', '-', ':', '(', ')', '12', '2024', '.', "'", '"', '°', 'é', '李', '🐶', '‍', '​', '&amp;', '&#39;', 'Front:', 'same on all', 'on both', 'Jun 5', 'St', '@', '\t', '\r'];
  for (let i = 0; i < 2000; i++) {
    const len = Math.floor(rng() * 14); let text = ''; for (let k = 0; k < len; k++) text += pick(pool);
    const n = 1 + Math.floor(rng() * 8), r = W.read(text, n), again = W.read(text, n);
    assert.deepEqual(r, again, 'repeatable'); assert.equal(r.words.length, n);
    if (n >= 2) {
      if (r.split) { assert.equal(r.parts.length, n); assert.ok(r.words.some(Boolean)); }
      else if (!r.none) { assert.ok(r.same); assert.ok(r.words.every(w => w === r.words[0] && w)); }
      else assert.ok(r.words.every(w => w === ''));
      // the whole text is only ever given as the buyer's own words (decoded, trimmed), never something made up
      if (!r.split && !r.none && !r.said) assert.equal(r.words[0], W.linesOf(text).join('\n'));
    }
  }
});
t('a split loses no word: the parts hold every letter and digit of the text, except the numbering marks', () => {
  const alnum = s => (s.match(/[\p{L}\p{N}]/gu) || []).join('');
  for (const [text, n] of [['Anna & Ben', 2], ['Mom, Dad, Me', 3], ['Anna\nBen', 2], ['Smith, Anna / Jones, Ben', 2], ['Amy, Bob, Cat, Dee, Eli, Fay', 6], ['José / 李明', 2]]) {
    const r = W.read(text, n); assert.ok(r.split); assert.equal(alnum(r.words.join('')), alnum(text).replace(/(?<=\P{L})and(?=\P{L})/gu, ''));
  }
});
t('100,000 characters of one word for two pieces: whole, fast', () => { const x = 'A'.repeat(100000), t0 = Date.now(), r = W.read(x, 2); assert.equal(r.how, 'whole'); assert.equal(r.words[1].length, 100000); assert.ok(Date.now() - t0 < 1500); });
t('100,000 characters of "a, a, a, ..." for two pieces: whole (a long text), fast', () => { const x = 'a, '.repeat(33000), t0 = Date.now(), r = W.read(x, 2); assert.equal(r.how, 'whole'); assert.match(r.why, /long text/); assert.ok(Date.now() - t0 < 1500); });
t('20,000 numbering marks for two pieces: whole, fast', () => { const x = '1) '.repeat(20000), t0 = Date.now(), r = W.read(x, 2); assert.equal(r.split, false); assert.ok(Date.now() - t0 < 2500); });
t('50,000 lines for two pieces: whole, fast', () => { const x = 'word\n'.repeat(50000), t0 = Date.now(), r = W.read(x, 2); assert.equal(r.split, false); assert.match(r.why, /50000 lines for 2 pieces/); assert.ok(Date.now() - t0 < 2500); });
t('100,000 spaces and one letter', () => { const t0 = Date.now(), r = W.read(' '.repeat(100000) + 'x' + ' '.repeat(100000), 2); assert.deepEqual(r.words, ['x', 'x']); assert.ok(Date.now() - t0 < 1500); });
t('1,000 separators in a row', () => { const t0 = Date.now(), r = W.read(',;/&+|'.repeat(1000), 2); assert.equal(r.n, 2); assert.ok(Date.now() - t0 < 1500); });
t('a very long list of N parts that fits in 600 characters still splits', () => { const list = Array.from({ length: 40 }, (_, i) => 'N' + i), r = W.read(list.join(', '), 40); assert.equal(r.split, true); assert.deepEqual(r.words, list); });
t('a buyer text with every line break of the world', () => { const r = W.read('Anna\r\nBen\rCleo\nDan Eli Fay', 6); assert.deepEqual(r.words, ['Anna', 'Ben', 'Cleo', 'Dan', 'Eli', 'Fay']); });
t('the result is a fresh object each time (a caller may edit it)', () => { const a = W.read('Anna & Ben', 2); a.words[0] = 'X'; a.parts.length = 0; assert.deepEqual(W.read('Anna & Ben', 2).words, ['Anna', 'Ben']); });

console.log('9 · forSlots: the piece slots, another voice, quantity 1');
t('slots are keys exactly as passed (C1..Cn)', () => { const r = W.forSlots({ personalization: ['Anna / Ben'] }, ['C1', 'C2']); assert.deepEqual(r.bySlot, { C1: 'Anna', C2: 'Ben' }); assert.equal(r.deferred, false); assert.equal(r.how, 'list'); assert.equal(r.split, true); });
t('slot names it does not know are fine: only the count and the order matter', () => { const r = W.forSlots({ personalization: 'Mom, Dad, Me' }, ['x', 'L.2', 'D1.3']); assert.deepEqual(r.bySlot, { x: 'Mom', 'L.2': 'Dad', 'D1.3': 'Me' }); });
t('slots that are not strings are keys too', () => { const r = W.forSlots({ personalization: 'A / B' }, [1, 2]); assert.deepEqual(r.bySlot, { 1: 'A', 2: 'B' }); });
t('not split: the whole text is under every slot, same true', () => { const r = W.forSlots({ personalization: ['Test'] }, ['C1', 'C2']); assert.deepEqual(r.bySlot, { C1: 'Test', C2: 'Test' }); assert.equal(r.same, true); assert.equal(r.split, false); assert.equal(r.note, W.SAME_NOTE); });
t('numbered words under their slots', () => { const r = W.forSlots({ personalization: ['1) Anna 2) Ben 3) Cleo'] }, ['C1', 'C2', 'C3']); assert.deepEqual(r.bySlot, { C1: 'Anna', C2: 'Ben', C3: 'Cleo' }); assert.equal(r.sure, true); });
t('the REAL order 4195059483: "N/A" for C1, C2 is no words (the caller reads the line as it always did)', () => { const r = W.forSlots({ quantity: 2, personalization: ['N/A'] }, ['C1', 'C2']); assert.equal(r.none, true); assert.equal(r.deferred, false); assert.deepEqual(r.bySlot, { C1: '', C2: '' }); });
t('a buyer message defers: nothing is read here', () => { const r = W.forSlots({ personalization: ['Anna / Ben'], buyerMessage: 'please engrave their names' }, ['C1', 'C2']); assert.equal(r.deferred, true); assert.equal(r.how, 'deferred'); assert.deepEqual(r.words, ['', '']); assert.deepEqual(r.bySlot, { C1: '', C2: '' }); assert.equal(r.split, false); assert.equal(r.same, false); });
t('a staff note defers', () => { assert.equal(W.forSlots({ personalization: ['Anna / Ben'], staffNote: 'ask the buyer' }, ['C1', 'C2']).deferred, true); });
t('a blank buyer message or staff note does not', () => { assert.equal(W.forSlots({ personalization: ['Anna / Ben'], buyerMessage: '  ', staffNote: '' }, ['C1', 'C2']).deferred, false); });
t('a message about the engraving defers, one that is not does not (o.engravingNote decides); with no judge every message counts', () => {
  const sp = { personalization: ['Anna / Ben'], messages: [{ text: 'thanks!' }] };
  assert.equal(W.forSlots(sp, ['C1', 'C2'], { engravingNote: x => /engrav/i.test(x) }).deferred, false);
  assert.equal(W.forSlots({ personalization: ['Anna / Ben'], messages: [{ text: 'engrave it small' }] }, ['C1', 'C2'], { engravingNote: x => /engrav/i.test(x) }).deferred, true);
  assert.equal(W.forSlots(sp, ['C1', 'C2']).deferred, true);
});
t('one slot (or none) passes the text through untouched', () => { const r = W.forSlots({ personalization: ['Anna & Ben'] }, ['C1']); assert.deepEqual(r.bySlot, { C1: 'Anna & Ben' }); assert.equal(r.how, 'single'); assert.deepEqual(W.forSlots({ personalization: ['Anna'] }, []).bySlot, {}); });
t('a missing spec or missing personalisation is no words', () => { for (const sp of [null, undefined, {}, { personalization: null }]) { const r = W.forSlots(sp, ['C1', 'C2']); assert.equal(r.none, true); assert.deepEqual(r.bySlot, { C1: '', C2: '' }); } });
t('slots listed twice do not throw', () => { const r = W.forSlots({ personalization: ['A / B'] }, ['C1', 'C1']); assert.deepEqual(Object.keys(r.bySlot), ['C1']); });
t('the buyer\'s other fields never leak into the words', () => { const r = W.forSlots({ personalization: ['Anna / Ben'], font: { asked: 'Typewriter' }, pair: { kind: 'multi' } }, ['C1', 'C2']); assert.deepEqual(r.words, ['Anna', 'Ben']); });

console.log('10 · nothing else moves');
t('ears and discs read their words as they did (sides.js splitWords / wordsForDiscs untouched by this file)', () => {
  const S = require(path.join(root, 'charm-nest-engrave-sides.js'));
  assert.deepEqual(S.splitWords(['Tag 1: J, Tag 2: Q'], 2).words, ['J', 'Q']);
  assert.deepEqual(S.splitWords('1: A, 2: B, 3: C', 3).words, ['A', 'B', 'C']);
  assert.equal(S.splitWords('Anna', 2).ok, false);
  const r = S.wordsForDiscs({ personalization: ['J / Q'] }, ['D1', 'D2']); assert.equal(r.ok, true); assert.deepEqual(r.words, ['J', 'Q']);
  assert.equal(S.wordsForDiscs({ personalization: ['J / Q'], buyerMessage: 'hi' }, ['D1', 'D2']), null);
  assert.equal(S.wordsForDiscs({ personalization: ['J / Q'] }, ['C1', 'C2']), null, 'copy slots are not discs here: they read through this file');
});
t('the file is pure: it touches no network, storage, clock, randomness or DOM', () => {
  const src = fs.readFileSync(path.join(root, 'charm-nest-engrave-piece-words.js'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
  for (const w of ['fetch(', 'XMLHttpRequest', 'localStorage', 'sessionStorage', 'Date.now', 'Math.random', 'document.', 'window.', 'indexedDB', 'firebase', 'require(']) assert.ok(!src.includes(w), w);
});
t('charm-nest-1.html loads it before the bridge, with a cache token; the public build lists it', () => {
  const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8'), a = html.indexOf('<script src="charm-nest-engrave-piece-words.js?v='), b = html.indexOf('<script src="charm-nest-bridge.js?v=');
  assert.ok(a > 0 && b > a, 'loaded before the bridge'); assert.match(html, /charm-nest-engrave-piece-words\.js\?v=[0-9a-z-]+"/);
  assert.ok(/charm-nest-engrave-piece-words\.js"/.test(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8')));
});
t('in the page the module is window.CharmNestEngravePieceWords', () => {
  const vm = require('vm'), src = fs.readFileSync(path.join(root, 'charm-nest-engrave-piece-words.js'), 'utf8'), ctx = { self: null }; ctx.self = ctx;
  vm.runInNewContext(src, ctx); assert.equal(typeof ctx.CharmNestEngravePieceWords.read, 'function'); assert.equal(JSON.stringify(ctx.CharmNestEngravePieceWords.read('A / B', 2).words), '["A","B"]');
});

console.log(fail ? `\nFAILED: ${pass} passed, ${fail} failed` : `\nPASSED: ${pass} passed, 0 failed`);
process.exitCode = fail ? 1 : 0;
