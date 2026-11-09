// SYMWORD (Paul, 9 Nov 2026, order 4218451815, personalisation "D (cross) E" on the TINY bar): a sign the buyer writes as a bracketed name is that sign in the engraved words, "D ✝ E".
// charm-nest-engrave-read.js fixSymbols (one table, SYMBOL_WORDS), run by readPersonalisation, on the words of every Engrave job (charm-nest-bridge.js, beside the degree rule) and on the intake's face words
// (charm-nest-orders.js spec.engraving). Every sign of the table must be drawn by DejaVu Sans, the engraver's symbol fallback, so it engraves in every font the app has.
//   node tests/charm-nest/symbol-words.cjs      Offline: no browser, no network.
'use strict';
const assert = require('assert'), path = require('path'), fs = require('fs');
const R = require(path.join(__dirname, '../../charm-nest-engrave-read.js'));
const opentype = require(path.join(__dirname, '../../vendor/opentype-1.3.4.min.js'));
let n = 0; const ok = (c, m) => { assert(c, m); n++; }, eq = (a, b, m) => { assert.deepStrictEqual(a, b, m); n++; };
const pass = name => console.log('  ✓', name);

/* ── 1 · the order's words ── */
{ const r = R.readPersonalisation('D (cross) E', {});
  eq([r.whole, r.lines, r.changed], ['D ✝ E', ['D ✝ E'], true], '"D (cross) E" reads as D ✝ E');
  ok(R.say(r) === 'Read “D ✝ E”; “(cross)” read as the sign ✝', 'the reason line says it: ' + R.say(r)); }
pass('order 4218451815: "D (cross) E" is engraved D ✝ E, and the card says why');

/* ── 2 · the table, in the ways buyers write it ── */
for (const [t, want] of [['D (cross) E', 'D ✝ E'], ['D(cross)E', 'D✝E'], ['A [Cross] B', 'A ✝ B'], ['(cross symbol)', '✝'], ['Mom (heart) Dad', 'Mom ♥ Dad'], ['Mom (heart symbol) Dad', 'Mom ♥ Dad'],
  ['J {star} K', 'J ★ K'], ['(open heart)', '♡'], ['A (infinity sign) B', 'A ∞ B'], ['(moon) (star)', '☾ ★'], ['(anchor)', '⚓'], ['(peace sign)', '☮'], ['(music note)', '♪'], ['(shamrock)', '☘'],
  ['(star of david)', '✡'], ['(lightning bolt)', '⚡'], ['(small cross)', '✝'], ['(crucifix)', '✝']])
  eq(R.fixSymbols(t).text, want, `${t} -> ${want}`);
// (what is not a sign name stays exactly as typed)
for (const t of ['Cross Family', 'my heart', '(cross on the back) Ann', '(Bob)', 'Star', 'D cross E', '(crossed)', '(heartbeat)'])
  eq(R.fixSymbols(t).text, t, `${t} stays as typed`);
// (a bracket that talks to the shop is still a note: only a whole sign name is a sign)
{ const r = R.readPersonalisation('Ann (please add a cross if possible)', {}); ok(r.whole === 'Ann' && r.dropped.length === 1, 'a request in brackets is still a note to the shop: ' + JSON.stringify(r)); }
pass('every sign name in brackets, any case or bracket, "symbol" / "sign" after it; nothing else is touched');

/* ── 3 · every sign of the table is drawn by the symbol fallback font ── */
{ const b = fs.readFileSync(path.join(__dirname, '../../vendor/fonts/DejaVuSans.ttf')), font = opentype.parse(b.buffer.slice(b.byteOffset, b.byteOffset + b.length));
  for (const [sign] of R.SYMBOL_WORDS) ok(font.charToGlyphIndex(sign) > 0, `DejaVu Sans draws ${sign} (U+${sign.codePointAt(0).toString(16).toUpperCase()})`);
  const text = fs.readFileSync(path.join(__dirname, '../../charm-nest-text.js'), 'utf8');
  ok(/DejaVu/.test(text), 'charm-nest-text.js uses DejaVu Sans as the symbol fallback'); }
pass('every sign is in DejaVu Sans, the engraver\'s symbol fallback');

/* ── 4 · the page runs it on every job's words and the intake on the faces ── */
{ const bridge = fs.readFileSync(path.join(__dirname, '../../charm-nest-bridge.js'), 'utf8'), orders = fs.readFileSync(path.join(__dirname, '../../charm-nest-orders.js'), 'utf8'), html = fs.readFileSync(path.join(__dirname, '../../charm-nest-1.html'), 'utf8');
  ok((bridge.match(/R\.fixSymbols\(job\.lines\.join\("\\n"\)\)/g) || []).length === 2, 'both places the degree rule runs on a job\'s words run the sign rule too');
  ok(/R\.fixSymbols\(f\.text\)/.test(orders), 'the intake reads the faces\' words with it');
  for (const f of ['charm-nest-engrave-read.js', 'charm-nest-orders.js', 'charm-nest-bridge.js']) { const m = new RegExp('src="' + f.replace(/\./g, '\\.') + '\\?v=([^"]*)"').exec(html); ok(m && /-fh5(?:-|$)/.test(m[1]), f + ' cache token carries -fh5'); } }
pass('the Engrave jobs and the intake use the one table; the cache tokens carry -fh5');
console.log(`symbol-words: ${n} checks passed`);
