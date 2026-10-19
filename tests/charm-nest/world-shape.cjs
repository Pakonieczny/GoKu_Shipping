/* WORLD-SHAPE (Paul, 19 Oct 2026: "add the compatibility with the Arabic, Israeli, Chinese, Japanese, Korean, Indian letters, characters, words/symbols depending on the language").
 * charm-nest-shape.js: HarfBuzz (WebAssembly) shaping + UAX #9 bidi, offline, node only (a browser section runs when Playwright is found: PW_DIR or argv[2]).
 *  1  what ships: the wasm and the embedded bidi-js are byte-for-byte the audited ones, licences present, no network / DOM / eval in the code
 *  2  runs the same in Node, in a vm sandbox like the engraving worker's (cross-realm bytes, minimal globals), in a real OS thread and (Playwright) in a page and a dedicated Worker; init is idempotent and a bad init can be retried
 *  3  shaping against ground truth: the OFFICIAL harfbuzzjs 1.6.3 wrapper's glyph ids/clusters/advances/offsets (frozen below, valid while the font files are the ones recorded), and the font's own glyph names
 *     read by opentype.js: Arabic joins and lam-alef, Hebrew niqqud as zero-advance positioned marks, Devanagari i-matra reordering and conjuncts
 *  4  outlines: same geometry as opentype.js for an unshaped glyph, y down / pen at the origin, scale linear in size, closed contours, nothing outside the advance
 *  5  bidi: UAX #9 on hand-written lines (runs, levels, visual order, mirrored brackets, numbers inside RTL, L1, paragraphs, astral) and on sampled rows of Unicode's BidiCharacterTest.txt
 *     (BIDI_CHAR_TEST=/path/BidiCharacterTest.txt runs the whole 91,000-row file: 0 failures with the 13.0 and the 18.0 file)
 *  6  speed: init < 300 ms, a 30-character line < 20 ms
 *  7  every other script font F has shipped (vendor/fonts/script-fonts.json): covered, shaped, outlined, no .notdef
 *  Golden data: tests/charm-nest/world-shape.cjs GOLDEN, made by  node gen-golden.mjs  = hb.Blob/Face/Font/Buffer.addText/setDirection/setScript/setLanguage/hb.shape of harfbuzzjs 1.6.3 (index.mjs) on the fonts of vendor/fonts/scripts.
 */
'use strict';
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto'), vm = require('node:vm');
const { Worker } = require('node:worker_threads');
const root = path.join(__dirname, '../..');
const S = require(path.join(root, 'charm-nest-shape.js')), ot = require(path.join(root, 'vendor/opentype-1.3.4.min.js'));
const read = p => fs.readFileSync(path.join(root, p));
const ab = b => b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength);
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
// (CPU time of the process, not the wall clock: the same on a busy machine)
const now = () => { const u = process.cpuUsage(); return (u.user + u.system) / 1000; };
let checks = 0;
const ok = (c, m) => { assert(c, m); checks++; };
const same = (a, b, m) => { assert.deepEqual(a, b, m); checks++; };
const near = (a, b, tol, m) => { assert(Math.abs(a - b) <= tol, `${m}: ${a} vs ${b}`); checks++; };
const cps = t => [...t].map(c => c.codePointAt(0));

const SHAPE_SRC = read('charm-nest-shape.js').toString('utf8');
const WASM = read('vendor/hb.wasm');
const FONT_FILES = { arabic: 'NotoSansArabic-Regular.ttf', hebrew: 'NotoSansHebrew-Regular.ttf', devanagari: 'NotoSansDevanagari-Regular.ttf' };
const fontBytes = k => read('vendor/fonts/scripts/' + FONT_FILES[k]);

/* 3 · ground truth from the official harfbuzzjs wrapper: [glyph id, cluster, x advance, x offset, y offset] per glyph, in the order HarfBuzz returns (visual, left to right) */
const GOLDEN = {"harfbuzz":"14.6.0","fonts":{"arabic":{"file":"NotoSansArabic-Regular.ttf","sha256":"122807099463fe963e5f8c0712f7c15beb99bc28bdcd6f95e1c56cd53962b683","script":"Arab","rtl":true},"hebrew":{"file":"NotoSansHebrew-Regular.ttf","sha256":"90697f8bbcd9da11e627d75ed73b3df8d55ae4b092143c8d5338ceb93bcb9f83","script":"Hebr","rtl":true},"devanagari":{"file":"NotoSansDevanagari-Regular.ttf","sha256":"94a484de117ffea1784dc71c2aa1a96f3f554d8f90fa99de66783dbcab9c25dc","script":"Deva","rtl":false}},"cases":[{"font":"arabic","text":"لا","lang":"ar","glyphs":[[10,1,363,0,0],[73,0,219,0,0]]},{"font":"arabic","text":"محمد","lang":"ar","glyphs":[[29,3,532,0,0],[78,2,578,0,0],[26,1,637,0,0],[79,0,525,0,0]]},{"font":"arabic","text":"مرحبا","lang":"ar","glyphs":[[9,4,291,0,0],[317,3,0,94,-3],[16,3,343,0,0],[27,2,589,0,0],[31,1,356,-40,0],[79,0,525,0,0]]},{"font":"arabic","text":"پ چ ژ گ","lang":"fa","glyphs":[[305,6,0,467,0],[62,6,877,0,0],[3,5,260,0,0],[293,4,0,34,-112],[30,4,367,0,0],[3,3,260,0,0],[341,2,0,207,-66],[23,2,659,0,0],[3,1,260,0,0],[324,0,0,341,-28],[14,0,993,0,0]]},{"font":"arabic","text":"سلام دنیا","lang":"fa","glyphs":[[9,8,291,0,0],[319,7,0,39,-5],[18,7,373,0,0],[283,6,0,52,-59],[19,6,269,0,0],[28,5,477,0,0],[3,4,260,0,0],[74,3,484,0,0],[11,2,379,0,0],[71,1,220,0,0],[37,0,784,0,0]]},{"font":"arabic","text":"اردو زبان","lang":"ur","glyphs":[[283,8,0,256,-168],[80,8,685,0,0],[9,7,291,0,0],[317,6,0,31,-3],[19,6,269,0,0],[283,5,0,71,-112],[30,5,337,-30,0],[3,4,260,0,0],[98,3,447,0,0],[28,2,477,0,0],[30,1,337,-30,0],[8,0,238,0,0]]},{"font":"arabic","text":"مَرْحَبًا","lang":"ar","glyphs":[[9,8,291,0,0],[375,6,0,-7,-174],[317,6,0,94,-3],[16,6,343,0,0],[371,4,0,4,-106],[27,4,589,0,0],[379,2,0,-5,-107],[31,2,356,-40,0],[371,0,0,142,-100],[79,0,525,0,0]]},{"font":"arabic","text":"لله","lang":"ar","glyphs":[[85,2,486,0,0],[70,1,300,0,0],[72,0,260,0,0]]},{"font":"hebrew","text":"שלום","lang":"he","glyphs":[[23,3,684,0,0],[124,2,291,-10,0],[55,1,522,0,0],[96,0,730,0,0]]},{"font":"hebrew","text":"שָׁלוֹם","lang":"he","glyphs":[[23,6,684,0,0],[46,4,0,82,0],[124,4,291,-10,0],[55,3,522,0,0],[79,0,0,227,0],[100,0,0,539,0],[96,0,730,0,0]]},{"font":"hebrew","text":"עברית","lang":"he","glyphs":[[107,4,685,0,0],[138,3,275,-20,0],[86,2,523,0,0],[12,1,547,-25,0],[10,0,593,0,0]]},{"font":"devanagari","text":"नमस्ते","lang":"hi","glyphs":[[75,0,555,0,0],[80,1,598,0,0],[256,2,389,0,0],[71,4,570,0,0],[40,4,0,0,0]]},{"font":"devanagari","text":"क्षत्रिय","lang":"hi","glyphs":[[90,0,717,0,0],[545,3,259,0,0],[304,3,552,0,0],[81,7,580,0,0]]},{"font":"devanagari","text":"हिन्दी","lang":"hi","glyphs":[[544,0,259,0,0],[88,0,531,0,0],[245,2,309,0,0],[73,4,531,0,0],[33,4,259,0,0]]},{"font":"devanagari","text":"संस्कृत","lang":"sa","glyphs":[[87,0,676,0,0],[100,0,0,0,0],[256,2,389,0,0],[56,4,768,0,0],[36,4,0,-221,0],[71,6,570,0,0]]},{"font":"devanagari","text":"ज्ञान","lang":"hi","glyphs":[[91,0,641,0,0],[31,0,259,0,0],[75,4,555,0,0]]},{"font":"devanagari","text":"कर्म","lang":"hi","glyphs":[[56,0,768,0,0],[80,1,598,0,0],[506,1,0,0,0]]},{"font":"devanagari","text":"क़िताब","lang":"hi","glyphs":[[545,0,259,0,0],[221,0,768,0,0],[71,3,570,0,0],[31,3,259,0,0],[78,5,571,0,0]]},{"font":"devanagari","text":"श्री","lang":"hi","glyphs":[[317,0,708,0,0],[33,0,259,0,0]]}]};
/* 5 · rows of BidiCharacterTest-13.0.0.txt (every 655th): code points; paragraph direction (0 ltr, 1 rtl, 2 auto); paragraph level; resolved levels; visual order of the characters */
const BIDI_ROWS = `05D0 05D1 0028 05D2 05D3 005B 0026 0065 0066 005D 002E 0029 0067 0068;0;0;1 1 0 1 1 0 0 0 0 0 0 0 0 0;1 0 2 4 3 5 6 7 8 9 10 11 12 13
0061 0028 2680 0029 2681 05D0;1;1;2 1 1 1 1 1;5 4 3 2 1 0
2680 0028 0029 0061 2681 0028 0029;1;1;1 1 1 2 1 1 1;6 5 4 3 2 1 0
05D0 0028 2680 0029 0028 2681 0029;1;1;1 1 1 1 1 1 1;6 5 4 3 2 1 0
2680 0028 0029 2681 0028 05D0 0029 0061;1;1;1 1 1 1 1 1 1 2;7 6 5 4 3 2 1 0
2680 0028 05D0 2681 0029 0028 0061 0029;1;1;1 1 1 1 1 1 2 1;7 6 5 4 3 2 1 0
0028 0029 0061 2680 0028 2681 0029 05D0;1;1;1 1 2 1 1 1 1 1;7 6 5 4 3 2 1 0
0061 0028 0029 0062 0028 0029 2680 05D0;1;1;2 2 2 2 1 1 1 1;7 6 5 4 0 1 2 3
0028 0029 0061 05D0 0062 05D1 0028 0029;1;1;1 1 2 1 2 1 1 1;7 6 5 4 3 2 1 0
05D0 0028 2680 05D1 0029 0028 0061 0029;1;1;1 1 1 1 1 1 2 1;7 6 5 4 3 2 1 0
05D0 0028 0029 0028 0061 05D1 0029 05D2;1;1;1 1 1 1 2 1 1 1;7 6 5 4 3 2 1 0
2680 0028 0028 0029 2681 0029 0061;1;1;1 1 1 1 1 1 2;6 5 4 3 2 1 0
0028 0061 05D0 0028 0062 0029 0029;1;1;1 2 1 1 2 1 1;6 5 4 3 2 1 0
2680 0028 2681 05D0 0028 0029 2682 0029;1;1;1 1 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 2680 0028 0061 05D0 0029 05D1 0029;1;1;1 1 1 2 1 1 1 1;7 6 5 4 3 2 1 0
0028 0028 0061 2680 0029 2681 0062 0029;1;1;1 1 2 1 1 1 2 1;7 6 5 4 3 2 1 0
0061 0028 0028 0062 2680 0063 0029 0029;1;1;2 2 2 2 2 2 2 2;0 1 2 3 4 5 6 7
0028 0028 0061 05D0 0062 0029 0029 0063;1;1;1 1 2 1 2 1 1 2;7 6 5 4 3 2 1 0
0028 05D0 2680 05D1 2681 0028 0029 0029;1;1;1 1 1 1 1 1 1 1;7 6 5 4 3 2 1 0
05D0 0028 0028 0029 0061 05D1 0062 0029;1;1;1 1 1 1 2 1 2 1;7 6 5 4 3 2 1 0
0028 05D0 0029 0061 005B 005D;1;1;1 1 1 2 1 1;5 4 3 2 1 0
0028 0029 0061 005B 0062 005D 05D0;1;1;1 1 2 2 2 2 1;6 2 3 4 5 1 0
2680 0028 0029 2681 0061 005B 0062 005D;1;1;1 1 1 1 2 2 2 2;4 5 6 7 3 2 1 0
0028 2680 0061 0029 05D0 005B 0062 005D;1;1;1 1 2 1 1 1 2 1;7 6 5 4 3 2 1 0
0061 0028 0029 2680 005B 2681 005D 2682;1;1;2 1 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 0061 2680 0029 05D0 005B 005D 05D1;1;1;1 2 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 0061 0029 05D0 0062 005B 2680 005D;1;1;1 2 1 1 2 1 1 1;7 6 5 4 3 2 1 0
0028 05D0 0029 005B 2680 0061 05D1 005D;1;1;1 1 1 1 1 2 1 1;7 6 5 4 3 2 1 0
0028 05D0 0061 05D1 0029 005B 005D 2680;1;1;1 1 2 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 0061 005B 005D 0029 0062;1;1;1 2 1 1 1 2;5 4 3 2 1 0
0061 0028 005B 0062 005D 2680 0029;1;1;2 2 2 2 2 2 2;0 1 2 3 4 5 6
0028 2680 005B 2681 005D 2682 05D0 0029;1;1;1 1 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 2680 005B 0061 05D0 005D 2681 0029;1;1;1 1 1 2 1 1 1 1;7 6 5 4 3 2 1 0
0028 005B 2680 05D0 005D 05D1 2681 0029;1;1;1 1 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 0061 005B 2680 05D0 0062 005D 0029;1;1;1 2 1 1 1 2 1 1;7 6 5 4 3 2 1 0
0028 005B 0061 05D0 2680 005D 0029 05D1;1;1;1 1 2 1 1 1 1 1;7 6 5 4 3 2 1 0
05D0 2680 0028 0061 005B 005D 0029 05D1;1;1;1 1 1 2 1 1 1 1;7 6 5 4 3 2 1 0
0028 005B 05D0 0061 005D 0062 0029 0063;1;1;1 1 1 2 1 2 1 2;7 6 5 4 3 2 1 0
0028 005B 0029 2680 005D 0061;1;1;1 1 1 1 1 2;5 4 3 2 1 0
0061 2680 0028 005B 0029 05D0 005D;1;1;2 1 1 1 1 1 1;6 5 4 3 2 1 0
05D0 0028 005B 05D1 0029 005D 05D2;1;1;1 1 1 1 1 1 1;6 5 4 3 2 1 0
2680 0028 005B 0061 0029 0062 005D 05D0;1;1;1 1 1 2 1 2 1 1;7 6 5 4 3 2 1 0
0028 2680 05D0 0061 005B 0029 005D 05D1;1;1;1 1 1 2 1 1 1 1;7 6 5 4 3 2 1 0
0028 005B 0029 0061 2680 05D0 2681 005D;1;1;1 1 1 2 1 1 1 1;7 6 5 4 3 2 1 0
0061 0028 05D0 005B 2680 05D1 0029 005D;1;1;2 1 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 005B 0029 05D0 2680 0061 2681 005D;1;1;1 1 1 1 1 2 1 1;7 6 5 4 3 2 1 0
0028 005B 05D0 0061 2680 05D1 0029 005D;1;1;1 1 1 2 1 1 1 1;7 6 5 4 3 2 1 0
0028 2680;1;1;1 1;1 0
0061 0028 05D0 0028 05D1 0029;1;1;2 1 1 1 1 1;5 4 3 2 1 0
0028 0028 2680 05D0 0061 0029 2681;1;1;1 1 1 1 2 1 1;6 5 4 3 2 1 0
0028 0028 0061 05D0 0062 0029 05D1;1;1;1 1 2 1 2 1 1;6 5 4 3 2 1 0
2680 0028 0029 2681 0028;1;1;1 1 1 1 1;4 3 2 1 0
05D0 0028 05D1 2680 0029 0028;1;1;1 1 1 1 1 1;5 4 3 2 1 0
0028 0061 2680 0062 0029 2681 0028;1;1;1 2 2 2 1 1 1;6 5 4 1 2 3 0
05D0 0028 2680 0061 05D1 0029 0028;1;1;1 1 1 2 1 1 1;6 5 4 3 2 1 0
2680 005B 2681 0028 05D0 0029;1;1;1 1 1 1 1 1;5 4 3 2 1 0
2680 005B 0061 2681 0062 0028 0029;1;1;1 1 2 2 2 1 1;6 5 2 3 4 1 0
0061 2680 005B 05D0 0028 0029 05D1;1;1;2 1 1 1 1 1 1;6 5 4 3 2 1 0
05D0 005B 0061 0028 0029 2680 0062;1;1;1 1 2 2 2 2 2;2 3 4 5 6 1 0
0061 0028 2680 0062 005B 0029;1;1;2 2 2 2 2 2;0 1 2 3 4 5
2680 0028 0061 05D0 005B 05D1 0029;1;1;1 1 2 1 1 1 1;6 5 4 3 2 1 0
0061 0028 005B 05D0 2680 0029 0062;1;1;2 1 1 1 1 1 2;6 5 4 3 2 1 0
0028 05D0 0061 05D1 005B 0029 05D2;1;1;1 1 2 1 1 1 1;6 5 4 3 2 1 0
05D0 2680 0028 0029 005B 2681;1;1;1 1 1 1 1 1;5 4 3 2 1 0
0028 0029 2680 05D0 0061 005B 0062;1;1;1 1 1 1 2 2 2;4 5 6 3 2 1 0
0061 0028 05D0 0029 05D1 005B 05D2;1;1;2 1 1 1 1 1 1;6 5 4 3 2 1 0
0029 2680 0028 0061 0029;1;1;1 1 1 2 1;4 3 2 1 0
05D0 0029 05D1 0028 0029 0061;1;1;1 1 1 1 1 2;5 4 3 2 1 0
0061 2680 0029 0062 0028 0029 0063;1;1;2 2 2 2 2 2 2;0 1 2 3 4 5 6
0029 05D0 2680 0061 0028 0029 05D1;1;1;1 1 1 2 1 1 1;6 5 4 3 2 1 0
2680 0028 0061 2681 0029 0029;1;1;1 1 2 1 1 1;5 4 3 2 1 0
0028 2680 0061 2681 0062 0029 0029;1;1;1 1 2 2 2 1 1;6 5 2 3 4 1 0
0028 0061 0029 2680 05D0 0029 05D1;1;1;1 2 1 1 1 1 1;6 5 4 3 2 1 0
0028 05D0 0061 0029 0029 2680 0062;1;1;1 1 2 1 1 1 2;6 5 4 3 2 1 0
005D 0061 0028 2680 0062 0029;1;1;1 2 2 2 2 2;1 2 3 4 5 0
2680 005D 05D0 2681 0028 2682 0029;1;1;1 1 1 1 1 1 1;6 5 4 3 2 1 0
005D 0061 0028 05D0 2680 0029 0062;1;1;1 2 1 1 1 1 2;6 5 4 3 2 1 0
05D0 005D 05D1 2680 0061 0028 0029;1;1;1 1 1 1 2 1 1;6 5 4 3 2 1 0
05D0 2680 0028 005D 0061 0029;1;1;1 1 1 1 2 1;5 4 3 2 1 0
0028 2680 05D0 0061 005D 05D1 0029;1;1;1 1 1 2 1 1 1;6 5 4 3 2 1 0
05D0 2680 0028 2681 005D 0029 0061;1;1;1 1 1 1 1 1 2;6 5 4 3 2 1 0
0028 2680 0029 05D0 005D;1;1;1 1 1 1 1;4 3 2 1 0
2680 0028 2681 0029 2682 005D 2683;1;1;1 1 1 1 1 1 1;6 5 4 3 2 1 0
0028 0061 0029 2680 0062 005D 0063;1;1;1 2 1 1 2 2 2;4 5 6 3 2 1 0
05D0 2680 0028 05D1 0029 005D 2681;1;1;1 1 1 1 1 1 1;6 5 4 3 2 1 0
0028 0061 0028 0029 05D0 0028 0029;1;1;1 2 1 1 1 1 1;6 5 4 3 2 1 0
2680 0028 0028 0029 05D0 0061 0028 0029;1;1;1 1 1 1 1 2 1 1;7 6 5 4 3 2 1 0
0028 0028 0029 0061 05D0 0028 0029 2680;1;1;1 1 1 2 1 1 1 1;7 6 5 4 3 2 1 0
0028 0028 05D0 0061 0029 0028 0029 05D1;1;1;1 1 1 2 1 1 1 1;7 6 5 4 3 2 1 0
2680 0028 2681 05D0 0028 0029 0028 0029 2682;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
2680 0028 0028 0029 0028 0061 2681 0029 05D0;1;1;1 1 1 1 1 2 1 1 1;8 7 6 5 4 3 2 1 0
0028 0028 2680 0061 0029 0028 05D0 0029 0062;1;1;1 1 1 2 1 1 1 1 2;8 7 6 5 4 3 2 1 0
2680 0028 05D0 0061 0028 0029 2681 0028 0029;1;1;1 1 1 2 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0028 2680 05D0 0029 0028 05D1 0029 05D2;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0028 0061 2680 0062 0029 0063 0028 0029;1;1;1 1 2 2 2 1 2 1 1;8 7 6 5 2 3 4 1 0
0061 0028 2680 0028 05D0 0029 0028 05D1 0029;1;1;2 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0028 0061 0029 0062 05D0 0028 2680 0029;1;1;1 1 2 1 2 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0028 0061 05D0 0029 0028 2680 05D1 0029;1;1;1 1 2 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0028 0061 0029 05D0 0028 05D1 0062 0029;1;1;1 1 2 1 1 1 1 2 1;8 7 6 5 4 3 2 1 0
05D0 0028 0028 2680 0029 0061 0028 0029 0062;1;1;1 1 1 1 1 2 2 2 2;5 6 7 8 4 3 2 1 0
0028 0028 05D0 0029 0028 2680 05D1 0061 0029;1;1;1 1 1 1 1 1 1 2 1;8 7 6 5 4 3 2 1 0
0028 0028 0029 0028 05D0 0061 2680 0029 05D1;1;1;1 1 1 1 1 2 1 1 1;8 7 6 5 4 3 2 1 0
0028 0028 05D0 0029 0061 0028 05D1 0029 05D2;1;1;1 1 1 1 2 1 1 1 1;8 7 6 5 4 3 2 1 0
05D0 0028 0028 05D1 0029 05D2 0028 2680 0029;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
2680 0028 0061 2681 0028 005B 005D 0029;1;1;1 1 2 1 1 1 1 1;7 6 5 4 3 2 1 0
0061 2680 0028 0028 005B 05D0 005D 0029;1;1;2 1 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 0028 05D0 005B 005D 2680 0061 0029;1;1;1 1 1 1 1 1 2 1;7 6 5 4 3 2 1 0
2680 0028 0028 2681 005B 005D 2682 05D0 0029;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
2680 0028 0028 0061 005B 005D 2681 0029 2682;1;1;1 1 1 2 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 2680 0061 0028 005B 005D 0062 05D0 0029;1;1;1 1 2 1 1 1 2 1 1;8 7 6 5 4 3 2 1 0
2680 0028 0028 005B 05D0 005D 2681 0029 0061;1;1;1 1 1 1 1 1 1 1 2;8 7 6 5 4 3 2 1 0
2680 0028 0028 05D0 005B 0061 05D1 005D 0029;1;1;1 1 1 1 1 2 1 1 1;8 7 6 5 4 3 2 1 0
0028 0061 0028 2680 005B 005D 2681 05D0 0029;1;1;1 2 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0061 0028 0028 005B 005D 2680 05D0 2681 0029;1;1;2 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0061 0028 0062 0028 2680 005B 05D0 005D 0029;1;1;2 2 2 1 1 1 1 1 1;8 7 6 5 4 3 0 1 2
0061 0028 05D0 2680 0028 005B 005D 0062 0029;1;1;2 1 1 1 1 1 1 2 1;8 7 6 5 4 3 2 1 0
0028 0028 0061 005B 05D0 0062 005D 0029 0063;1;1;1 1 2 1 1 2 1 1 2;8 7 6 5 4 3 2 1 0
05D0 0028 2680 0028 005B 005D 2681 05D1 0029;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
05D0 0028 2680 05D1 0028 005B 2681 005D 0029;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
05D0 0028 0028 005B 0061 2680 0062 005D 0029;1;1;1 1 1 1 2 2 2 1 1;8 7 4 5 6 3 2 1 0
0028 05D0 0028 0061 05D1 005B 005D 0029 2680;1;1;1 1 1 2 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 05D0 0028 005B 05D1 005D 0029 2680 05D2;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0061 0028 005B 0029 0062 005D;1;1;1 2 2 2 2 2 1;6 1 2 3 4 5 0
0028 0028 005B 2680 05D0 0029 2681 005D;1;1;1 1 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 0061 0028 005B 05D0 0029 2680 005D;1;1;1 2 1 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 05D0 0061 0028 005B 05D1 0029 005D;1;1;1 1 2 1 1 1 1 1;7 6 5 4 3 2 1 0
0028 2680 0028 005B 0029 2681 0061 05D0 005D;1;1;1 1 1 1 1 1 2 1 1;8 7 6 5 4 3 2 1 0
2680 0028 0028 0061 2681 005B 0029 05D0 005D;1;1;1 1 1 2 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 2680 0028 005B 0061 05D0 0062 0029 005D;1;1;1 1 1 1 2 1 2 1 1;8 7 6 5 4 3 2 1 0
0028 0028 2680 005B 0029 05D0 005D 2681 05D1;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
2680 0028 0028 05D0 005B 05D1 0029 005D 05D2;1;1;1 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0061 2680 0028 0062 005B 0029 0063 005D;1;1;1 2 2 2 2 2 2 2 1;8 1 2 3 4 5 6 7 0
0061 2680 0028 05D0 0028 05D1 005B 0029 005D;1;1;2 1 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0061 0028 0062 05D0 005B 0029 2680 005D;1;1;1 2 1 2 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0061 0028 005B 05D0 2680 0029 05D1 005D;1;1;1 2 1 1 1 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 0061 05D0 0028 005B 05D1 0029 005D 0062;1;1;1 2 1 1 1 1 1 1 2;8 7 6 5 4 3 2 1 0
05D0 2680 0028 0028 005B 0029 0061 005D 0062;1;1;1 1 1 1 1 1 2 2 2;6 7 8 5 4 3 2 1 0
0028 05D0 0028 005B 0029 2680 05D1 005D 0061;1;1;1 1 1 1 1 1 1 1 2;8 7 6 5 4 3 2 1 0
0028 0028 05D0 005B 0061 0029 2680 005D 05D1;1;1;1 1 1 1 2 1 1 1 1;8 7 6 5 4 3 2 1 0
0028 05D0 0028 0061 005B 05D1 0029 05D2 005D;1;1;1 1 1 2 1 1 1 1 1;8 7 6 5 4 3 2 1 0`;

(async () => {
  /* ───────────── 1 · what ships ───────────── */
  ok(sha(WASM) === '5f67e3bd1c4f40ffd07c12d7faa30ebc40cc65998ce058fde5b3c6504607bfb4', 'vendor/hb.wasm is harfbuzzjs 1.6.3 dist/harfbuzz.wasm');
  ok(WASM.slice(0, 4).equals(Buffer.from([0, 0x61, 0x73, 0x6d])) && WASM.length < 600e3, 'a wasm binary, under 600 KB');
  const bidiBlock = /const bidiUmd=function\(module,exports,define\)\{\n([\s\S]*?)\n  \};\n  \/\/ END bidi-js/.exec(SHAPE_SRC);
  ok(bidiBlock && sha(bidiBlock[1]) === 'd7e4f22272988879f8f419c9788f970ead19da460bfc3435573986a0d3470610', 'the embedded bidi-js is bidi-js 1.1.0 dist/bidi.min.js, verbatim');
  for (const f of ['harfbuzz', 'bidi-js']) ok(/Permission is hereby granted/.test(read(`vendor/${f}-LICENSE.txt`).toString()), `${f} licence shipped`);
  ok(/HarfBuzz is licensed under the so-called "Old MIT" license/.test(read('vendor/harfbuzz-LICENSE.txt').toString()), 'the HarfBuzz licence text itself is in the licence file');
  const code = SHAPE_SRC.replace(/^\/\*[\s\S]*?\*\/\n/, '').split('\n').filter(l => !/^\s*\/\//.test(l)).join('\n');
  for (const bad of [/\bfetch\s*\(/, /XMLHttpRequest/, /\bdocument\./, /\bwindow\./, /\bnew Function\b/, /\beval\s*\(/, /\bimportScripts\b/, /https?:\/\//, /\blocalStorage\b/, /\bFirebase|firestore/i])
    ok(!bad.test(code), `charm-nest-shape.js has no ${bad}`);
  ok(!/Math\.random|Date\.now|new Date/.test(code), 'deterministic: no clock, no randomness');

  /* ───────────── 2 · environments ───────────── */
  ok(S.ready === false && !S.stub, 'not ready before init; the real file, not the stub');
  assert.throws(() => S.addFont('x', ab(fontBytes('hebrew'))), /init/, 'addFont before init says so');
  assert.throws(() => S.shapeRun('x', { fontId: 'x' }), /init/);
  // a fresh sandbox like the engraving worker's: minimal globals, bytes from another realm; a bad init rejects and can be retried
  const sandbox = () => { const c = vm.createContext({ console, Intl, TextEncoder, TextDecoder, setTimeout, clearTimeout }); c.self = c; vm.runInContext(SHAPE_SRC, c, { filename: 'charm-nest-shape.js' }); return c; };
  const sb = sandbox();
  ok(vm.runInContext('typeof CharmNestShape', sb) === 'object' && vm.runInContext('typeof module + typeof require + typeof window + typeof document', sb) === 'undefinedundefinedundefinedundefined', 'loads with nothing but self, and no DOM');
  sb.bad = new Uint8Array(40); sb.wasm = WASM; sb.hebrew = fontBytes('hebrew'); sb.arabic = fontBytes('arabic');
  const sbJson = await vm.runInContext(`(async () => {
    let first = 'resolved'; try { await CharmNestShape.init({ wasm: bad }); } catch (e) { first = 'rejected'; }
    const t0 = Date.now(); const p1 = CharmNestShape.init({ wasm }), p2 = CharmNestShape.init({ wasm }); await p1;
    const idem = p1 === p2 && (await p2) === CharmNestShape && CharmNestShape.ready;
    CharmNestShape.addFont('he', hebrew); CharmNestShape.addFont('ar', arabic);
    const a = CharmNestShape.shapeRun('שָׁלוֹם', { fontId: 'he', size: 64, rtl: true, lang: 'he' }), b = CharmNestShape.shapeRun('محمد', { fontId: 'ar', size: 64, rtl: true, lang: 'ar' });
    return JSON.stringify({ first, idem, a, b, bidi: CharmNestShape.bidi('Anna שלום 2024') });
  })()`, sb);
  const sbRes = JSON.parse(sbJson);
  ok(sbRes.first === 'rejected' && sbRes.idem, 'a bad wasm rejects, init is idempotent (one promise) and works afterwards in the same sandbox');
  await S.init({ wasm: ab(WASM) });
  const p1 = S.init(), p2 = S.init({ wasm: new Uint8Array(3) });
  ok(p1 === p2 && (await p1) === S && S.ready === true, 'init: ready, idempotent, a second call with other arguments is the same promise');
  const HE = S.addFont('hebrew', ab(fontBytes('hebrew'))), AR = S.addFont('arabic', fontBytes('arabic')), DE = S.addFont('devanagari', new Uint8Array(fontBytes('devanagari')));
  const sameRes = (mine, theirs, m) => same(JSON.parse(JSON.stringify(mine)), theirs, m);
  sameRes(S.shapeRun('שָׁלוֹם', { fontId: 'hebrew', size: 64, rtl: true, lang: 'he' }), sbRes.a, 'the sandbox (another realm, minimal globals) shapes byte-for-byte like Node: Hebrew with niqqud');
  sameRes(S.shapeRun('محمد', { fontId: 'arabic', size: 64, rtl: true, lang: 'ar' }), sbRes.b, 'the sandbox shapes Arabic like Node');
  sameRes(S.bidi('Anna שלום 2024'), sbRes.bidi, 'the sandbox orders bidi like Node');
  // a real OS thread (the engraving worker is one)
  const threadJson = await new Promise((res, rej) => {
    const w = new Worker(`
      const { parentPort, workerData } = require('node:worker_threads'); const S = require(workerData.shape);
      (async () => { await S.init({ wasm: workerData.wasm }); S.addFont('d', workerData.font);
        parentPort.postMessage(JSON.stringify(S.shapeRun('क्षत्रिय हिन्दी', { fontId: 'd', size: 48, script: 'Deva', lang: 'hi' }))); })().catch(e => parentPort.postMessage('ERR ' + e.stack));
    `, { eval: true, workerData: { shape: path.join(root, 'charm-nest-shape.js'), wasm: ab(WASM), font: ab(fontBytes('devanagari')) } });
    w.on('message', m => { res(m); w.terminate(); }); w.on('error', rej);
  });
  sameRes(S.shapeRun('क्षत्रिय हिन्दी', { fontId: 'devanagari', size: 48, script: 'Deva', lang: 'hi' }), JSON.parse(threadJson), 'a worker thread shapes byte-for-byte like the main thread');

  /* ───────────── 3 · API ───────────── */
  for (const [k, i] of [['hebrew', HE], ['arabic', AR], ['devanagari', DE]]) {
    ok(i.id === k && i.unitsPerEm >= 1000 && i.ascender > 0 && i.descender < 0 && i.capHeight > 0 && i.capHeight < i.ascender && i.xHeight > 0 && i.xHeight < i.capHeight + 1, `${k}: metrics ${JSON.stringify(i)}`);
    same(S.addFont(k, new ArrayBuffer(8)), i, `${k}: addFont is idempotent per id (the same record, the new bytes ignored)`);
    ok(S.hasFont(k), `${k}: hasFont`);
  }
  ok(!S.hasFont('nope'), 'hasFont of an unknown id');
  assert.throws(() => S.shapeRun('x', { fontId: 'nope', size: 10 }), /unknown font/, 'an unknown font id throws');
  assert.throws(() => S.addFont('junk', new Uint8Array(100)), /font/, 'junk bytes are not a font');
  ok(!S.hasFont('junk'), 'a rejected font is not registered');
  ok(S.covers('hebrew', 'שלום') && S.covers('hebrew', 'שָׁלוֹם') && S.covers('hebrew', 'שלום עולם'), 'covers: Hebrew with niqqud and a space');
  ok(!S.covers('hebrew', '中') && !S.covers('hebrew', 'שלום\t') && !S.covers('arabic', '日'), 'covers: refuses what the font lacks, and a tab');
  ok(S.covers('hebrew', 'שלום\u200f\u200d\ufe0f\n'), 'covers: default-ignorables (RLM, ZWJ, VS16) and line breaks are not missing');
  ok(S.covers('arabic', 'می\u200cخواهم') && S.covers('arabic', 'پ چ ژ گ') && S.covers('arabic', 'مَرْحَبًا'), 'covers: Persian with ZWNJ, the Persian letters, Arabic with harakat');
  ok(S.covers('devanagari', 'नमस्ते क्षत्रिय हिन्दी ॐ ।'), 'covers: Devanagari with conjuncts, om, danda');
  ok(S.covers('devanagari', ''), 'covers: the empty text');
  same([S.scriptOf('שלום'), S.scriptOf('Anna שלום 2024'), S.scriptOf('مرحبا'), S.scriptOf('नमस्ते'), S.scriptOf('日本語のテキスト'), S.scriptOf('한국어'), S.scriptOf('你好'), S.scriptOf('বাংলা'), S.scriptOf('தமிழ்')],
    ['Hebr', 'Hebr', 'Arab', 'Deva', 'Kana', 'Hang', 'Hani', 'Beng', 'Taml'], 'scriptOf: the script that needs a font (Japanese: the most frequent of Hani/Hira/Kana, ask scriptsOf)');
  same([S.scriptOf('Hello, world 123 ♡ 😀'), S.scriptOf('Привет'), S.scriptOf('Γειά σου'), S.scriptOf(''), S.scriptOf(null), S.scriptOf('1 + 2 = 3 !?')], [null, null, null, null, null, null], 'scriptOf: Latin, Greek, Cyrillic, Common, emoji: null (the base face draws them)');
  same(S.scriptsOf('日本語のテキスト abc'), { Hani: 3, Hira: 1, Kana: 4, Latn: 3 }, 'scriptsOf tells Japanese from Chinese');
  same([S.scriptOfCodePoint(0x41), S.scriptOfCodePoint(0x5d0), S.scriptOfCodePoint(0x627), S.scriptOfCodePoint(0x915), S.scriptOfCodePoint(0xac00), S.scriptOfCodePoint(0x4e2d), S.scriptOfCodePoint(0x3042), S.scriptOfCodePoint(0x30a2), S.scriptOfCodePoint(0x20), S.scriptOfCodePoint(0x301), S.scriptOfCodePoint(0x1f600), S.scriptOfCodePoint(0x10ffff)],
    ['Latn', 'Hebr', 'Arab', 'Deva', 'Hang', 'Hani', 'Hira', 'Kana', 'Zyyy', 'Zinh', 'Zyyy', 'Zzzz'], 'scriptOfCodePoint');

  /* ───────────── 3 · shaping against ground truth ───────────── */
  const goldenFontsMatch = Object.entries(GOLDEN.fonts).every(([k, f]) => sha(fontBytes(k)) === f.sha256);
  const otFonts = Object.fromEntries(Object.keys(FONT_FILES).map(k => [k, ot.parse(ab(fontBytes(k)))]));
  const nameOf = (k, gid) => otFonts[k].glyphs.get(gid).name || '';
  const cmapGid = (k, cp) => otFonts[k].charToGlyphIndex(String.fromCodePoint(cp));
  let compared = 0;
  if (goldenFontsMatch) {
    for (const c of GOLDEN.cases) {
      const f = GOLDEN.fonts[c.font], r = S.shapeRun(c.text, { fontId: c.font, size: 1000, rtl: f.rtl, script: f.script, lang: c.lang });
      same(r.glyphs.map(g => [g.id, g.cluster, g.adv]), c.glyphs.map(g => [g[0], g[1], g[2]]), `${c.font} "${c.text}": glyph ids, clusters and advances equal the official harfbuzzjs result`);
      // positions: pen + HarfBuzz x offset; y offset up is negative y
      let pen = 0; c.glyphs.forEach((g, i) => { near(r.glyphs[i].x, pen + g[3], 1e-9, `${c.font} "${c.text}" glyph ${i}: x = pen + x_offset`); near(r.glyphs[i].y, -g[4], 1e-9, `${c.font} "${c.text}" glyph ${i}: y = -y_offset`); pen += g[2]; });
      near(r.advance, pen, 1e-9, `${c.font} "${c.text}": advance is the sum of the glyph advances`);
      same(r.missing, [], `${c.font} "${c.text}": nothing missing`);
      compared++;
    }
  } else console.log('  note: the script fonts changed since the golden data was made; the exact glyph-id comparison is skipped (the structural checks below still run)');
  // run again through the font's own glyph names (opentype.js reads the post table): independent of the binding
  const run = (k, text, o = {}) => S.shapeRun(text, { fontId: k, size: 100, rtl: k !== 'devanagari', lang: { arabic: 'ar', hebrew: 'he', devanagari: 'hi' }[k], ...o });
  {
    // Arabic: lam + alef is the ligature pair, drawn alef (left) then lam (right); every letter of "محمد" has its joined form, none the isolated one
    const lam = run('arabic', 'لا'), names = lam.glyphs.map(g => nameOf('arabic', g.id));
    ok(lam.glyphs.length <= 2 && lam.glyphs.every(g => g.id !== cmapGid('arabic', 0x644) && g.id !== cmapGid('arabic', 0x627)), 'lam-alef: neither the isolated lam nor the isolated alef');
    if (names.every(Boolean)) ok(/0627/.test(names[0]) && /0644/.test(names[1]) && names.every(n => /rlig|lig|fina|init/.test(n)), `lam-alef forms by name: ${names}`);
    ok(lam.glyphs[0].cluster === 1 && lam.glyphs[1].cluster === 0, 'lam-alef: drawn right to left (the alef, character 1, at the left)');
    const mhmd = run('arabic', 'محمد'), mn = mhmd.glyphs.map(g => nameOf('arabic', g.id));
    ok(mhmd.glyphs.length === 4 && mhmd.glyphs.map(g => g.cluster).join() === '3,2,1,0', 'محمد: four glyphs, visual order = reverse of the characters');
    ok([0x645, 0x62d, 0x645, 0x62f].every(cp => !mhmd.glyphs.some(g => g.id === cmapGid('arabic', cp))), 'محمد: no isolated letter forms: it is joined');
    if (mn.every(Boolean)) ok(/0645.*init/.test(mn[3]) && /062D.*medi/i.test(mn[2]) && /0645.*medi/.test(mn[1]) && /062F.*fina/.test(mn[0]), `محمد: initial, medial, medial, final in reading order: ${mn}`);
    const iso = run('arabic', 'ب'), joined = run('arabic', 'بب'), isoIds = new Set(iso.glyphs.map(g => g.id));
    ok(joined.glyphs.filter(g => !isoIds.has(g.id)).length === 2, 'ب alone is isolated; in ببّ the two letters take their final and initial forms (the dot is its own glyph in Noto Sans Arabic)');
    const spaced = run('arabic', 'پ چ ژ گ', { lang: 'fa' });
    ok(spaced.glyphs.filter(g => g.adv > 0).length >= 7 && spaced.missing.length === 0, 'Persian پ چ ژ گ: all four letters and the spaces shaped');
    const zw = run('arabic', 'می\u200cخواهم', { lang: 'fa' }), nz = run('arabic', 'میخواهم', { lang: 'fa' });
    ok(zw.missing.length === 0 && zw.glyphs.every(g => g.id !== 0) && zw.glyphs.length >= nz.glyphs.length - 1, 'Persian with a zero-width non-joiner shapes with no .notdef');
    ok(zw.glyphs.map(g => g.id).join() !== nz.glyphs.map(g => g.id).join(), 'the ZWNJ breaks the join: mim-ya-final differs from the joined word');
    const rl = run('arabic', 'لا', { features: '-rlig' });
    ok(rl.glyphs.map(g => g.id).join() !== lam.glyphs.map(g => g.id).join(), 'features: -rlig turns the lam-alef forms off');
    ok(run('arabic', 'لا', { features: ['-rlig'] }).glyphs.map(g => g.id).join() === rl.glyphs.map(g => g.id).join(), 'features: an array of strings is the same');
    const harakat = run('arabic', 'مَرْحَبًا');
    ok(harakat.glyphs.filter(g => g.adv === 0).length >= 3 && harakat.missing.length === 0 && harakat.glyphs.some(g => g.y !== 0), 'Arabic with harakat: marks are zero-advance glyphs raised/lowered by GPOS');
  }
  {
    // Hebrew with niqqud: 7 code points, 7 glyphs, the marks are zero-advance and positioned on their letters
    const r = run('hebrew', 'שָׁלוֹם'), base = run('hebrew', 'שלום');
    ok(r.glyphs.length === 7 && base.glyphs.length === 4, 'שָׁלוֹם: 7 glyphs (4 letters + shin dot + qamats + holam), שלום: 4');
    const marks = r.glyphs.filter(g => g.adv === 0);
    ok(marks.length === 3, 'three niqqud marks have zero advance');
    ok(marks.every(m => r.glyphs.some(b => b.adv > 0 && b.cluster === m.cluster)), 'each mark shares the cluster of its letter');
    ok(r.glyphs.some(g => g.adv === 0 && Math.abs(g.x - r.glyphs.filter(b => b.cluster === g.cluster && b.adv > 0).map(b => b.x)[0]) > 1), 'the marks are shifted by mark positioning (GPOS), not left at the pen');
    near(r.advance, base.advance, base.advance * .02, 'niqqud do not widen the word');
    ok(r.glyphs[0].cluster === 6 && r.glyphs[r.glyphs.length - 1].cluster === 0, 'Hebrew: the first glyph drawn is the LAST letter (ם at the left), the last drawn is ש (at the right)');
    ok(r.commands.length > base.commands.length, 'the marks add outlines');
  }
  {
    const cp = t => [...t].length;
    const nm = run('devanagari', 'नमस्ते'), kshatriya = run('devanagari', 'क्षत्रिय'), hindi = run('devanagari', 'हिन्दी');
    ok(nm.glyphs.length < cp('नमस्ते') && kshatriya.glyphs.length < cp('क्षत्रिय') && hindi.glyphs.length < cp('हिन्दी'), `Devanagari: fewer glyphs than code points (${nm.glyphs.length}/6, ${kshatriya.glyphs.length}/8, ${hindi.glyphs.length}/6)`);
    ok(kshatriya.glyphs.length === 4, 'क्षत्रिय: क्ष and त्र are conjunct glyphs, four glyphs');
    ok(hindi.glyphs.slice(0, 2).every(g => g.cluster === 0) && hindi.glyphs[0].id !== hindi.glyphs[1].id && hindi.glyphs[0].x < hindi.glyphs[1].x, 'हिन्दी: the short-i matra (typed after ह) is drawn BEFORE, left of, the consonant ह; both share cluster 0');
    ok(nm.glyphs.every(g => g.id !== 0) && [nm, kshatriya, hindi].every(r => r.missing.length === 0), 'no .notdef in the Devanagari samples');
    same([run('devanagari', 'नमस्ते').dotted, hindi.dotted, run('devanagari', 'ि').dotted, run('devanagari', 'ि', { noDottedCircle: true }).dotted, run('devanagari', 'ि', { noDottedCircle: true }).glyphs.length], [[], [], [0], [], 1],
      'an orphan vowel sign gets the dotted circle like in a browser and says so in `dotted`; noDottedCircle suppresses it');
    const ligR = run('devanagari', 'कर्म');
    ok(ligR.glyphs.length === 3 && ligR.glyphs.some(g => g.adv === 0), 'कर्म: the reph is a zero-advance mark placed above');
  }
  // an unknown character: reported, drawn as nothing, no throw
  {
    const r = S.shapeRun('ש中ל', { fontId: 'hebrew', size: 100, rtl: true });
    same(r.missing, [0x4e2d], 'a code point the font lacks is listed in missing');
    ok(r.glyphs.length === 3 && r.glyphs.some(g => g.id === 0), 'the .notdef glyph keeps its place in the run');
    const noMiddle = S.shapeRun('של', { fontId: 'hebrew', size: 100, rtl: true });
    ok(r.commands.length === noMiddle.commands.length, 'and draws nothing (no box in the cut file)');
    ok(S.shapeRun('', { fontId: 'hebrew', size: 10 }).glyphs.length === 0 && S.shapeRun('', { fontId: 'hebrew', size: 10 }).advance === 0, 'the empty run is empty');
    ok(S.shapeRun('שלום', { fontId: 'hebrew', size: 100 }).advance > 0, 'rtl defaults from the script');
  }

  {
    // A run is itemized by script like a browser does (HarfBuzz picks the font's GSUB/GPOS script table by it); opts.script forces one script for everything.
    S.addFont('dejavu', ab(read('vendor/fonts/DejaVuSans.ttf')));
    const dv = text => S.shapeRun(text, { fontId: 'dejavu', size: 100, rtl: true });
    const mixed = dv('שלום مرحبا'), he = dv('שלום '), ar = dv('مرحبا');
    same(mixed.glyphs.map(g => g.id), [...ar.glyphs, ...he.glyphs].map(g => g.id), 'a right-to-left run of two scripts: shaped item by item, the LAST item drawn first (leftmost)');
    ok(mixed.glyphs[0].cluster >= 5 && mixed.glyphs[mixed.glyphs.length - 1].cluster === 0, 'its clusters are offsets into the whole text');
    near(mixed.advance, ar.advance + he.advance, 1e-9, 'and the advance is the sum of the items');
    const forced = S.shapeRun('שלום مرحبا', { fontId: 'dejavu', size: 100, rtl: true, script: 'Arab' });
    ok(forced.glyphs.length === mixed.glyphs.length, 'script: forces one script for the whole run (same glyph count here)');
  }
  /* ───────────── 4 · outlines ───────────── */
  const flatten = commands => {
    const polys = []; let cur = null, px = 0, py = 0;
    for (const c of commands) {
      if (c.type === 'M') { cur = [[c.x, c.y]]; polys.push(cur); px = c.x; py = c.y; }
      else if (c.type === 'L') { cur.push([c.x, c.y]); px = c.x; py = c.y; }
      else if (c.type === 'Q') { for (let i = 1; i <= 12; i++) { const t = i / 12, u = 1 - t; cur.push([u * u * px + 2 * u * t * c.x1 + t * t * c.x, u * u * py + 2 * u * t * c.y1 + t * t * c.y]); } px = c.x; py = c.y; }
      else if (c.type === 'C') { for (let i = 1; i <= 12; i++) { const t = i / 12, u = 1 - t; cur.push([u ** 3 * px + 3 * u * u * t * c.x1 + 3 * u * t * t * c.x2 + t ** 3 * c.x, u ** 3 * py + 3 * u * u * t * c.y1 + 3 * u * t * t * c.y2 + t ** 3 * c.y]); } px = c.x; py = c.y; }
    }
    return polys;
  };
  const measure = commands => {
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity, area = 0;
    for (const p of flatten(commands)) { let a = 0; for (let i = 0; i < p.length; i++) { const [x, y] = p[i], [u, v] = p[(i + 1) % p.length]; a += x * v - u * y; x0 = Math.min(x0, x); x1 = Math.max(x1, x); y0 = Math.min(y0, y); y1 = Math.max(y1, y); } area += a / 2; }
    return { x0, y0, x1, y1, area };
  };
  for (const t of ['ש', 'ם', 'ל', 'שלום']) {
    const mine = measure(S.shapeRun(t, { fontId: 'hebrew', size: 100, rtl: true }).commands);
    // opentype.js lays the same letters out left to right, in the same units: for a single glyph it is the same drawing
    if ([...t].length === 1) {
      const theirs = measure(otFonts.hebrew.getPath(t, 0, 0, 100).commands);
      for (const k of ['x0', 'y0', 'x1', 'y1']) near(mine[k], theirs[k], .02, `"${t}": ${k} equals opentype.js's`);
      near(Math.abs(mine.area), Math.abs(theirs.area), Math.abs(theirs.area) * .002, `"${t}": same area as opentype.js`);
      near(S.shapeRun(t, { fontId: 'hebrew', size: 100, rtl: true }).advance, otFonts.hebrew.charToGlyph(t).advanceWidth * 100 / HE.unitsPerEm, 1e-9, `"${t}": same advance as opentype.js`);
    }
  }
  {
    const a = S.shapeRun('محمد مرحبا', { fontId: 'arabic', size: 100, rtl: true, lang: 'ar' }), b = S.shapeRun('محمد مرحبا', { fontId: 'arabic', size: 50, rtl: true, lang: 'ar' });
    near(a.advance, b.advance * 2, 1e-9, 'advance is linear in size');
    ok(a.commands.length === b.commands.length && a.commands.every((c, i) => ['x', 'y', 'x1', 'y1', 'x2', 'y2'].every(k => c[k] === undefined || Math.abs(c[k] - 2 * b.commands[i][k]) < 1e-9)), 'every coordinate is linear in size');
    same(S.shapeRun('محمد مرحبا', { fontId: 'arabic', size: 100, rtl: true, lang: 'ar' }), a, 'deterministic: the same call gives the same result, object for object');
    ok(a.commands.every(c => 'MLQCZ'.includes(c.type) && ['x', 'y', 'x1', 'y1', 'x2', 'y2'].every(k => c[k] === undefined || Number.isFinite(c[k]))), 'only opentype.js path commands with finite numbers');
    ok(a.commands.filter(c => c.type === 'M').length === a.commands.filter(c => c.type === 'Z').length, 'every contour is closed');
    ok(!a.commands.some(c => Object.is(c.x, -0) || Object.is(c.y, -0)), 'no negative zero');
    // the opentype.js Path accepts it as is
    const path = new ot.Path(); path.extend(a.commands.map(c => ({ ...c, x: c.x === undefined ? c.x : c.x + 10, y: c.y === undefined ? c.y : c.y + 150 })));
    const bb = path.getBoundingBox();
    ok(bb.x1 >= 10 - 30 && bb.x2 <= 10 + a.advance + 30 && bb.y1 > 150 - 200 && bb.y2 < 150 + 90, `Path.extend works; ink ${bb.x1.toFixed(1)}..${bb.x2.toFixed(1)} inside the advance 0..${a.advance.toFixed(1)}`);
  }
  for (const [k, t, o] of [['arabic', 'مرحبا بكم في متجرنا', { rtl: true, lang: 'ar' }], ['hebrew', 'שָׁלוֹם עולם', { rtl: true, lang: 'he' }], ['devanagari', 'नमस्ते क्षत्रिय हिन्दी', { rtl: false, lang: 'hi' }]]) {
    const r = S.shapeRun(t, { fontId: k, size: 100, ...o }), m = measure(r.commands);
    ok(r.commands.length > 20 && r.glyphs.length > 5, `${k}: non-empty outlines`);
    ok(m.x0 > -40 && m.x1 < r.advance + 40, `${k}: ink ${m.x0.toFixed(1)}..${m.x1.toFixed(1)} sits inside the advance 0..${r.advance.toFixed(1)}`);
    near(r.advance, r.glyphs.reduce((s, g) => s + g.adv, 0), 1e-6, `${k}: advance = sum of the glyph advances`);
    ok(m.y0 < -30 && m.y0 > -150 && m.y1 < 60, `${k}: y is down: ink above the baseline is negative (${m.y0.toFixed(0)}..${m.y1.toFixed(0)})`);
    if (k !== 'devanagari') ok(r.glyphs[0].x <= r.glyphs[r.glyphs.length - 1].x + r.glyphs[r.glyphs.length - 1].adv, `${k}: rtl run is in visual order`);
  }
  // Devanagari/Hebrew/Arabic fonts have no Latin: that is what the caller's covers() is for; a Latin letter in the Hebrew font is reported, not drawn
  ok(!S.covers('hebrew', 'Anna') ? S.shapeRun('Anna', { fontId: 'hebrew', size: 20 }).missing.length > 0 : true, 'a Latin word in a font without Latin is reported missing');

  /* ───────────── 5 · bidi ───────────── */
  const B = (t, o) => S.bidi(t, o), texts = b => b.runs.map(r => r.text), levels = b => b.runs.map(r => r.level);
  {
    let b = B('Anna שלום 2024');
    same([b.baseRtl, texts(b), levels(b), b.visual], [false, ['Anna ', 'שלום ', '2024'], [0, 1, 2], [0, 2, 1]], 'Anna שלום 2024: LTR paragraph; Anna, then the number (left of the Hebrew), then שלום');
    same(b.runs.map(r => [r.start, r.end, r.rtl]), [[0, 5, false], [5, 10, true], [10, 14, false]], 'run offsets are UTF-16 offsets into the line, rtl = odd level');
    b = B('שלום Anna');
    same([b.baseRtl, texts(b), levels(b), b.visual], [true, ['שלום ', 'Anna'], [1, 2], [1, 0]], 'שלום Anna: RTL paragraph; Anna stands at the LEFT, שלום at the right');
    b = B('שלום 2024');
    same([b.baseRtl, texts(b), levels(b), b.visual], [true, ['שלום ', '2024'], [1, 2], [1, 0]], 'numbers inside an RTL line stay left-to-right (digits left of the word)');
    b = B('סכום 12.50 ₪');
    ok(b.baseRtl && b.runs.some(r => r.text.startsWith('12.50') && r.level === 2), 'a decimal number inside RTL is one left-to-right run');
    b = B('عدد ١٢٣ كتب');
    ok(b.baseRtl && b.runs.some(r => /١٢٣/.test(r.text) && r.level === 2) && b.visual.length === b.runs.length, 'Arabic-Indic digits in an Arabic line are a level-2 run');
    b = B('abc ١٢٣');
    same([texts(b), levels(b)], [['abc ', '١٢٣'], [0, 2]], 'Arabic-Indic digits after Latin text (rule W2/W7): left-to-right run at level 2');
    b = B('שלום (abc)');
    same([texts(b), levels(b), b.visual], [['שלום )', 'abc', '('], [1, 2, 1], [2, 1, 0]], 'brackets in RTL are mirrored (L4): drawn "(abc) םולש" — the first ( of the text becomes ), the last ) becomes (');
    b = B('שלום (עולם)');
    same([texts(b), levels(b), b.visual], [['שלום )עולם('], [1], [0]], 'Hebrew in Hebrew brackets: one RTL run, both brackets mirrored');
    b = B('abc (שלום) def');
    same([texts(b), levels(b), b.visual], [['abc (', 'שלום', ') def'], [0, 1, 0], [0, 1, 2]], 'Hebrew inside brackets inside an LTR line: the brackets stay left-to-right and are NOT mirrored (N0)');
    b = B('abc שלום ');
    same([texts(b), levels(b), b.visual], [['abc ', 'שלום', ' '], [0, 1, 0], [0, 1, 2]], 'L1: the trailing space takes the paragraph level');
    b = B('שלום abc ');
    same([texts(b), levels(b), b.visual], [['שלום ', 'abc', ' '], [1, 2, 1], [2, 1, 0]], 'L1 in an RTL paragraph: the trailing space is level 1 and ends at the LEFT');
    b = B('Hello world 123');
    same([b.baseRtl, texts(b), levels(b), b.visual], [false, ['Hello world 123'], [0], [0]], 'plain Latin is one level-0 run');
    same(B(''), { baseRtl: false, runs: [], visual: [] }, 'the empty line');
    b = B('abc', { dir: 'rtl' });
    same([b.baseRtl, levels(b), b.visual], [true, [2], [0]], 'dir: rtl forces the paragraph direction');
    b = B('שלום', { dir: 'ltr' });
    same([b.baseRtl, levels(b)], [false, [1]], 'dir: ltr forces it the other way');
    b = B('abc\nשלום');
    same([b.baseRtl, texts(b), levels(b), b.visual], [false, ['abc\n', 'שלום'], [0, 1], [0, 1]], 'two paragraphs: each takes its own first-strong direction; visual order per paragraph');
    b = B('שלום 😀');
    same([texts(b), levels(b), b.runs[0].end], [['שלום 😀'], [1], 7], 'an astral character (emoji, a surrogate pair) is neutral and stays in the RTL run; offsets count UTF-16 units');
    b = B('שלום 😀🎉 עולם');
    same([texts(b), levels(b)], [['שלום 😀🎉 עולם'], [1]], 'two emoji between Hebrew words are neutrals, drawn right to left with the rest (bidi-js alone would call them left-to-right letters)');
    b = B('a \u{1E900}\u{1E901} b');
    same([texts(b), levels(b)], [['a ', '\u{1E900}\u{1E901}', ' b'], [0, 1, 0]], 'Adlam letters (supplementary plane) are right-to-left');
    b = B('שלום \u{1D7CE}\u{1D7CF} x');
    same(levels(b), [1, 2, 1, 2], 'mathematical digits (supplementary plane) after Hebrew are numbers: left-to-right at level 2');
    b = B('abc \u{10800}\u{10801}');
    same([texts(b), levels(b)], [['abc ', '\u{10800}\u{10801}'], [0, 1]], 'Cypriot syllables are right-to-left');
    b = B('שלום \u202babc\u202c');
    ok(b.runs.length >= 2 && b.runs.some(r => r.level >= 1), 'explicit embeddings are honoured');
    b = B('a \u2067שלום\u2069 b');
    ok(b.runs.some(r => r.rtl) && !b.baseRtl, 'isolates are honoured');
    for (const t of ['Anna שלום 2024', 'שלום (abc)', 'abc (שלום) def', 'שלום 😀 x']) ok(B(t).runs.map(r => r.text).join('').length === t.length, `runs of "${t}" cover the whole line exactly once`);
    ok(B('abc').visual.length === 1 && B('a'.repeat(5000)).runs.length === 1, 'a long Latin line is one run');
  }
  {
    // bidi() mirrors the brackets of its run texts (L4); HarfBuzz would mirror them again in a right-to-left run. shapeRun draws the characters it is given.
    const dv = ot.parse(ab(read('vendor/fonts/DejaVuSans.ttf'))), cm = c => dv.charToGlyphIndex(c);
    S.addFont('dejavu', ab(read('vendor/fonts/DejaVuSans.ttf')));
    const ids = (t, o) => S.shapeRun(t, { fontId: 'dejavu', size: 50, ...o }).glyphs.map(g => g.id);
    same([ids('(', { rtl: true }), ids(')', { rtl: true }), ids('[', { rtl: true }), ids('<', { rtl: true })], [[cm('(')], [cm(')')], [cm('[')], [cm('<')]], 'shapeRun draws the bracket it is given in a right-to-left run');
    same(ids('(', { rtl: true, mirror: true }), [cm(')')], 'mirror: true gives HarfBuzz\'s own mirroring back for logical text');
    const b = S.bidi('שלום (עולם)'), g = ids(b.runs[0].text, { rtl: true });
    ok(g[0] === cm('(') && g[g.length - 6] === cm(')'), 'a bidi() run through shapeRun: the visual left end is "(" (the mirrored last ")"), the visual right end of the group is ")"');
    const lines = S.bidi('abc (שלום) def'); ok(lines.runs.every(r => r.text === 'abc (' || r.text === 'שלום' || r.text === ') def'), 'brackets not mirrored where the paragraph is LTR');
    ok(/^[\x00-\x7f]*$/.test(SHAPE_SRC), 'charm-nest-shape.js is plain ASCII: it decodes the same whatever charset a server announces');
  }
  {
    const check = (line, tag) => {
      const f = line.split(';'); if (f.length < 5) return;
      const codes = f[0].trim().split(' ').map(h => parseInt(h, 16)), text = String.fromCodePoint(...codes);
      const dir = f[1] === '0' ? 'ltr' : f[1] === '1' ? 'rtl' : undefined, ref = f[3].trim().split(' '), refVisual = f[4].trim() ? f[4].trim().split(' ').map(Number) : [];
      const b = S.bidi(text, { dir }), at = []; for (let i = 0; i < text.length; i += text.codePointAt(i) > 0xffff ? 2 : 1) at.push(i);
      const lv = new Map(); for (const r of b.runs) for (let i = r.start; i < r.end; i++) lv.set(i, r.level);
      assert.equal(b.baseRtl ? '1' : '0', f[2].trim(), `${tag}: paragraph level of ${f[0]}`);
      codes.forEach((_, k) => { if (ref[k] !== 'x') assert.equal(String(lv.get(at[k])), ref[k], `${tag}: level of character ${k} of ${f[0]}`); });
      const order = []; for (const ri of b.visual) { const r = b.runs[ri], ks = []; codes.forEach((_, k) => { if (at[k] >= r.start && at[k] < r.end) ks.push(k); }); if (r.rtl) ks.reverse(); order.push(...ks); }
      assert.deepEqual(order.filter(k => ref[k] !== 'x'), refVisual, `${tag}: visual order of ${f[0]}`);
      checks++;
    };
    const rows = BIDI_ROWS.split('\n').filter(Boolean); rows.forEach(l => check(l, 'BidiCharacterTest')); ok(rows.length >= 100, `${rows.length} sampled BidiCharacterTest rows pass`);
    if (process.env.BIDI_CHAR_TEST) { let n = 0; for (const l of fs.readFileSync(process.env.BIDI_CHAR_TEST, 'utf8').split('\n')) if (l && l[0] !== '#') { check(l, 'BidiCharacterTest (full)'); n++; } console.log(`  full BidiCharacterTest: ${n} rows pass`); }
  }

  /* ───────────── 6 · speed ───────────── */
  {
    const median = a => a.slice().sort((x, y) => x - y)[a.length >> 1];
    const line30 = { arabic: ['مرحبا بكم في متجرنا الجميل جدا', { rtl: true, lang: 'ar' }], hebrew: ['שלום וברוכים הבאים לחנות שלנו', { rtl: true, lang: 'he' }], devanagari: ['नमस्ते आपका स्वागत है क्षत्रिय हिन्दी', { rtl: false, lang: 'hi' }] };
    // (init is timed in fresh sandboxes, best of 3)
    let initMs = Infinity, addMs = Infinity;
    for (let i = 0; i < 3; i++) {
      const fresh = sandbox(); fresh.wasm = WASM; fresh.bytes = fontBytes('devanagari');
      const t0 = now(); await vm.runInContext('CharmNestShape.init({ wasm })', fresh); initMs = Math.min(initMs, now() - t0);
      const t1 = now(); vm.runInContext("CharmNestShape.addFont('d', bytes)", fresh); addMs = Math.min(addMs, now() - t1);
    }
    ok(initMs < 300, `init (compile + instantiate) ${initMs.toFixed(1)} ms CPU < 300`);
    let out = `  init ${initMs.toFixed(0)} ms, addFont ${addMs.toFixed(0)} ms (Devanagari 218 KB)`;
    for (const [k, [text, o]] of Object.entries(line30)) {
      S.shapeRun(text, { fontId: k, size: 40, ...o });   // (the first shape of a font builds HarfBuzz's tables and the outline cache)
      const times = []; for (let i = 0; i < 25; i++) { const t = now(); S.shapeRun(text, { fontId: k, size: 40 + i, ...o }); times.push(now() - t); }
      ok(median(times) < 20, `${k}: a ${[...text].length}-character line shapes in ${median(times).toFixed(2)} ms CPU (median of 25) < 20`);
      out += `; ${k} ${median(times).toFixed(2)} ms`;
    }
    const tb = []; for (let i = 0; i < 25; i++) { const t = now(); S.bidi('Anna שלום 2024 (x) مرحبا'); tb.push(now() - t); }
    ok(median(tb) < 2, `bidi of a mixed line ${median(tb).toFixed(3)} ms`);
    console.log(out + `; bidi ${median(tb).toFixed(3)} ms`);
  }

  /* ───────────── 7 · every other script font that shipped ───────────── */
  {
    const SAMPLES = { arabic: ['مرحبا', 'Arab', 'ar'], hebrew: ['שלום', 'Hebr', 'he'], devanagari: ['नमस्ते', 'Deva', 'hi'], bengali: ['বাংলা ক্ষ', 'Beng', 'bn'], gurmukhi: ['ਪੰਜਾਬੀ', 'Guru', 'pa'], gujarati: ['ગુજરાતી', 'Gujr', 'gu'],
      tamil: ['தமிழ் கொ', 'Taml', 'ta'], telugu: ['తెలుగు', 'Telu', 'te'], kannada: ['ಕನ್ನಡ', 'Knda', 'kn'], malayalam: ['മലയാളം', 'Mlym', 'ml'], odia: ['ଓଡ଼ିଆ', 'Orya', 'or'], sinhala: ['සිංහල', 'Sinh', 'si'],
      'cjk-sc': ['你好，世界', 'Hani', 'zh-Hans'], 'cjk-tc': ['漢字，世界', 'Hani', 'zh-Hant'], 'cjk-jp': ['日本語のテキスト', 'Hani', 'ja'], 'cjk-kr': ['한국어 안녕하세요', 'Hang', 'ko'] };
    let manifest = null; try { manifest = JSON.parse(read('vendor/fonts/script-fonts.json').toString()); } catch (_) { }
    const done = [];
    for (const m of (manifest && manifest.fonts) || []) {
      if (FONT_FILES[m.id] || !SAMPLES[m.id] || !fs.existsSync(path.join(root, m.file))) continue;
      const [text, script, lang] = SAMPLES[m.id], info = S.addFont(m.id, ab(read(m.file)));
      const r = S.shapeRun(text, { fontId: m.id, size: 100, script, lang, rtl: !!m.rtl });
      ok(S.covers(m.id, text), `${m.id}: covers "${text}"`);
      ok(r.commands.length > 10 && r.missing.length === 0 && r.glyphs.every(g => g.id !== 0) && r.advance > 0, `${m.id}: "${text}" shapes with outlines and no .notdef (${r.glyphs.length} glyphs for ${[...text].length} characters)`);
      ok(info.unitsPerEm > 0 && info.capHeight > 0 && info.ascender > 0, `${m.id}: metrics`);
      if (m.id === 'tamil') { const ko = S.shapeRun('கொ', { fontId: m.id, size: 100, script, lang }); ok(ko.glyphs.length >= 2 && ko.glyphs[0].cluster === 0, 'Tamil கொ: the two-part vowel is split around the consonant'); }
      if (m.id === 'bengali') { const ksh = S.shapeRun('ক্ষ', { fontId: m.id, size: 100, script, lang }); ok(ksh.glyphs.length < 3, 'Bengali ক্ষ is a conjunct'); }
      if (/^cjk/.test(m.id)) { const mix = S.shapeRun('ABC 日本 abc 123', { fontId: m.id, size: 100, lang }); ok(mix.missing.length === 0 && mix.glyphs.length === 14 && mix.glyphs.every(g => g.id), `${m.id}: Latin, digits and Han in one run (itemized by script)`); }
      if (m.id === 'cjk-kr') ok(r.glyphs.length === [...text].length, 'Hangul: one glyph per syllable');
      S.removeFont(m.id); done.push(m.id);
    }
    console.log(`  other script fonts checked: ${done.length ? done.join(', ') : '(none shipped yet)'}`);
    ok(S.removeFont('devanagari') && !S.hasFont('devanagari') && !S.removeFont('devanagari'), 'removeFont frees a font and says so');
    S.addFont('devanagari', ab(fontBytes('devanagari')));
    sameRes(S.shapeRun('हिन्दी', { fontId: 'devanagari', size: 64, script: 'Deva', lang: 'hi' }), JSON.parse(JSON.stringify(S.shapeRun('हिन्दी', { fontId: 'devanagari', size: 64, script: 'Deva', lang: 'hi' }))), 're-adding a font gives the same shaping');
  }

  /* ───────────── 8 · a real browser: classic script in a cross-origin-isolated page and in a dedicated Worker ───────────── */
  let browserNote = 'skipped (no Playwright: pass its directory as argv[2] or PW_DIR)';
  const pwDir = process.argv[2] || process.env.PW_DIR;
  if (pwDir) {
    let chromium = null; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require(path.join(pwDir, 'playwright'))); } catch (__) { } }
    if (chromium) {
      const http = require('node:http');
      const server = http.createServer((req, res) => {
        const u = req.url.split('?')[0], hdr = { 'Cross-Origin-Opener-Policy': 'same-origin', 'Cross-Origin-Embedder-Policy': 'require-corp', 'Cross-Origin-Resource-Policy': 'same-origin' };
        if (u === '/test.html') { res.writeHead(200, { ...hdr, 'Content-Type': 'text/html' }); return res.end('<!doctype html><meta charset=utf-8><script src="/charm-nest-shape.js"></script><body>shape'); }
        if (u === '/shape-worker.js') { res.writeHead(200, { ...hdr, 'Content-Type': 'text/javascript' }); return res.end(`importScripts('/charm-nest-shape.js');onmessage=async e=>{try{const d=e.data;await CharmNestShape.init({wasm:d.wasm});CharmNestShape.addFont('d',d.font);postMessage(JSON.stringify([CharmNestShape.shapeRun('क्षत्रिय हिन्दी',{fontId:'d',size:48,script:'Deva',lang:'hi'}),typeof document,typeof window,self.crossOriginIsolated]));}catch(err){postMessage('ERR '+err.stack)}}`); }
        const file = { '/charm-nest-shape.js': 'charm-nest-shape.js', '/hb.wasm': 'vendor/hb.wasm', '/devanagari.ttf': 'vendor/fonts/scripts/' + FONT_FILES.devanagari, '/hebrew.ttf': 'vendor/fonts/scripts/' + FONT_FILES.hebrew }[u];
        if (!file) { res.writeHead(404); return res.end(); }
        res.writeHead(200, { ...hdr, 'Content-Type': /js$/.test(file) ? 'text/javascript' : 'application/octet-stream' }); res.end(read(file));
      });
      await new Promise(r => server.listen(0, '127.0.0.1', r));
      const origin = 'http://127.0.0.1:' + server.address().port;
      let browser = null;
      try {
        browser = await chromium.launch();
        const page = await (await browser.newContext()).newPage(); const errors = []; page.on('pageerror', e => errors.push(String(e))); page.on('console', m => m.type() === 'error' && errors.push(m.text()));
        await page.goto(origin + '/test.html');
        const res = await page.evaluate(async () => {
          const S = window.CharmNestShape, wasm = await (await fetch('/hb.wasm', { cache: 'force-cache' })).arrayBuffer(), font = await (await fetch('/devanagari.ttf')).arrayBuffer(), heb = await (await fetch('/hebrew.ttf')).arrayBuffer();
          const t0 = performance.now(); await S.init({ wasm }); const initMs = performance.now() - t0;
          S.addFont('d', font); S.addFont('h', heb);
          const r = S.shapeRun('क्षत्रिय हिन्दी', { fontId: 'd', size: 48, script: 'Deva', lang: 'hi' });
          const times = []; for (let i = 0; i < 15; i++) { const t = performance.now(); S.shapeRun('नमस्ते आपका स्वागत है क्षत्रिय हिन्दी', { fontId: 'd', size: 40 + i, script: 'Deva', lang: 'hi' }); times.push(performance.now() - t); }
          return { json: JSON.stringify([r, 'undefined', 'object', true]), initMs, iso: self.crossOriginIsolated, median: times.sort((a, b) => a - b)[7], bidi: JSON.stringify(S.bidi('Anna שלום 2024')), heb: JSON.stringify(S.shapeRun('שָׁלוֹם', { fontId: 'h', size: 64, rtl: true, lang: 'he' })) };
        });
        ok(res.iso === true, 'the test page is cross-origin isolated (COEP require-corp) like the shop pages');
        sameRes(S.shapeRun('क्षत्रिय हिन्दी', { fontId: 'devanagari', size: 48, script: 'Deva', lang: 'hi' }), JSON.parse(res.json)[0], 'Chromium (page, classic <script>, wasm fetched same-origin) shapes byte-for-byte like Node');
        sameRes(S.shapeRun('שָׁלוֹם', { fontId: 'hebrew', size: 64, rtl: true, lang: 'he' }), JSON.parse(res.heb), 'Chromium shapes Hebrew with niqqud like Node');
        same(JSON.parse(res.bidi), S.bidi('Anna שלום 2024'), 'Chromium orders bidi like Node');
        const w = await page.evaluate(async () => {
          const wasm = await (await fetch('/hb.wasm')).arrayBuffer(), font = await (await fetch('/devanagari.ttf')).arrayBuffer();
          return await new Promise((resolve, reject) => { const worker = new Worker('/shape-worker.js'); worker.onmessage = e => { resolve(e.data); worker.terminate(); }; worker.onerror = e => reject(new Error(e.message)); worker.postMessage({ wasm, font }, [wasm, font]); });
        });
        ok(!/^ERR/.test(w), 'the dedicated Worker (importScripts) runs it: ' + String(w).slice(0, 120));
        const wj = JSON.parse(w); same([wj[1], wj[2]], ['undefined', 'undefined'], 'the Worker has no document and no window');
        sameRes(S.shapeRun('क्षत्रिय हिन्दी', { fontId: 'devanagari', size: 48, script: 'Deva', lang: 'hi' }), wj[0], 'the Worker shapes byte-for-byte like Node');
        ok(!errors.length, 'no page errors or console errors: ' + errors.join(' | '));
        ok(res.initMs < 1000 && res.median < 50, `Chromium wall clock (the machine may be busy): init ${res.initMs.toFixed(0)} ms, a 30-character line ${res.median.toFixed(2)} ms`);
        browserNote = `Chromium page and Worker agree with Node (init ${res.initMs.toFixed(0)} ms, 30-char line ${res.median.toFixed(2)} ms)`;
      } catch (e) { if (/Executable doesn't exist|Failed to launch|browserType\.launch/.test(String(e))) browserNote = 'skipped (Chromium not launchable here)'; else throw e; }
      finally { if (browser) await browser.close(); server.close(); }
    }
  }
  console.log(`  browser: ${browserNote}`);
  console.log(`WORLD-SHAPE: ${checks} checks passed (${compared} golden cases compared)`);
})().catch(e => { console.error(e); process.exit(1); });
