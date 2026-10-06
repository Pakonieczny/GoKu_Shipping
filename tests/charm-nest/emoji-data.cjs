/* The emoji list behind the Emoji button (charm-nest-emoji-data.js, built by scripts/build-emoji-picker-data.cjs):
   every offered emoji, tone variant and pair goes through the REAL engraver path (the fonts loaded the way charm-nest-bridge.js
   loadFonts loads them, CharmNestText.withEmoji, CharmNestGeom.glyphCoverage), nothing unsupported is offered, nothing the laser
   can cut is missing, names are there, search ranks sensibly and the file is what the generator writes. Node only, no network. */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm'), crypto = require('node:crypto');
const root = path.resolve(__dirname, '../..');
const ot = require(root + '/vendor/opentype-1.3.4.min.js'), Text = require(root + '/charm-nest-text.js'), G = require(root + '/charm-nest-geom.js');
const map = require(root + '/vendor/fonts/emoji-sequences.json');
const file = path.join(root, 'charm-nest-emoji-data.js'), source = fs.readFileSync(file, 'utf8');
const D = require(file);

// the fonts, as loadFonts loads them (the emoji font must match the hash of its shape map)
const buf = p => { const b = fs.readFileSync(path.join(root, p)); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
assert.equal(crypto.createHash('sha256').update(Buffer.from(buf('vendor/fonts/NotoEmoji-Regular.ttf'))).digest('hex'), map.fontSha256, 'emoji font matches its shape map');
const font = Text.withEmoji(ot.parse(buf('vendor/fonts/SourceSans3-Regular.otf')), ot.parse(buf('vendor/fonts/NotoEmoji-Regular.ttf')), map, ot.Path);
const seqs = map.sequences, unsupported = new Set(map.unsupported);
const hex = s => [...s].map(c => c.codePointAt(0).toString(16)).join(' ');
const engravable = c => {
  assert(c in seqs, 'in sequences: ' + hex(c));
  assert(!unsupported.has(c), 'not unsupported: ' + hex(c));
  assert.equal(Text.graphemes(c).length, 1, 'one character for the engraver: ' + hex(c));
  const cov = G.glyphCoverage(font, c); assert(cov.ok, 'glyphCoverage: ' + hex(c) + ' ' + cov.missing);
  assert(font.getPath(c, 0, 0, 20).commands.length >= 4, 'has an outline: ' + hex(c));
  assert(font.getAdvanceWidth(c, 20) > 0, 'has width: ' + hex(c));
};
const RGI = /^\p{RGI_Emoji}$/v, ink = k => JSON.stringify(seqs[k].filter(r => r[0] !== 3));

// shape of the data
assert.equal(typeof D.version, 'string'); assert(/^\d{4}-\d{2}-\d{2}$/.test(D.version));
assert.deepEqual(D.groups.map(g => g.id), ['smileys', 'people', 'animals', 'food', 'travel', 'activities', 'objects', 'symbols', 'flags'], 'group order follows emoji-test');
assert.deepEqual(D.groups.map(g => g.name), ['Smileys', 'People', 'Animals and nature', 'Food and drink', 'Travel', 'Activities', 'Objects', 'Symbols', 'Flags']);
assert.deepEqual(D.tones.map(t => t.name), ['Default', 'Light', 'Medium-light', 'Medium', 'Medium-dark', 'Dark']);
assert.equal(D.tones[0].id, ''); assert.equal(D.tones[0].c, '✋'); assert(D.tones.slice(1).every(t => t.c === '✋' + t.id && t.id.length === 2));
const items = D.groups.flatMap(g => g.items);
assert.equal(D.count, items.length, 'count is the number of selectable cells');
for (const g of D.groups) { assert(g.items.length > 0); const icon = items.find(i => i.c === g.icon); assert(icon || g.icon in seqs, 'group icon is a real emoji: ' + g.id); engravable(g.icon); }
assert(Buffer.byteLength(source) < 250 * 1024, 'compact: ' + Buffer.byteLength(source) + ' bytes');
assert(!/glyph|\bpath\b|commands/.test(source.replace(/\/\*[\s\S]*?\*\//, '')), 'no shape data in the file');

// every cell, tone variant and pair: real engraver, in the map, fully qualified, never twice
const seen = new Map(), claim = (c, why) => { assert(!seen.has(c), 'listed twice: ' + hex(c) + ' (' + why + ' and ' + seen.get(c) + ')'); seen.set(c, why); };
let variants = 0, pairs = 0;
for (const it of items) {
  assert(typeof it.c === 'string' && typeof it.n === 'string' && typeof it.k === 'string' && (it.t === 0 || it.t === 1), 'item shape ' + hex(it.c));
  engravable(it.c); claim(it.c, 'cell');
  assert(RGI.test(it.c), 'fully qualified emoji: ' + hex(it.c));
  assert(!/[\u{1F3FB}-\u{1F3FF}]/u.test(it.c), 'a tone variant is never a cell: ' + hex(it.c));
  assert.equal(Boolean(it.t), Boolean(D.toned[it.c]), 'toned map agrees with t: ' + hex(it.c));
  if (it.t) {
    assert.deepEqual(Object.keys(D.toned[it.c]), D.tones.slice(1).map(t => t.id), 'five tones: ' + hex(it.c));
    for (const [tone, v] of Object.entries(D.toned[it.c])) { engravable(v); claim(v, 'tone ' + hex(it.c)); assert(v.includes(tone)); assert(RGI.test(v)); variants++; }
  }
  if (it.p) {
    for (const a of D.tones.slice(1)) for (const b of D.tones.slice(1)) {
      const v = D.pair(it.c, a.id, b.id);
      if (a.id === b.id) { assert.equal(v, '', 'a pair needs two different tones'); continue; }
      engravable(v); claim(v, 'pair ' + hex(it.c)); assert(RGI.test(v)); pairs++;
    }
  } else assert.equal(D.pair(it.c, D.tones[1].id, D.tones[2].id), '');
  for (const s of it.s || []) { engravable(s); claim(s, 'folded into ' + hex(it.c)); assert.equal(ink(s), ink(it.c), 'a folded cell engraves with the same outline: ' + hex(s)); assert.equal(D.find(s), it); }
  assert.equal(D.find(it.c), it);
  if (it.t) for (const v of Object.values(D.toned[it.c])) assert.equal(D.find(v), it);
}
for (const k of seen.keys()) assert(k in seqs);

// nothing the laser can cut is missing: every fully qualified emoji of the map (components aside) has the outline of something offered
const reach = new Set([...seen.keys()].map(ink));
const comp = new Set(['\u{1F3FB}', '\u{1F3FC}', '\u{1F3FD}', '\u{1F3FE}', '\u{1F3FF}', '\u{1F9B0}', '\u{1F9B1}', '\u{1F9B2}', '\u{1F9B3}']);
const lost = Object.keys(seqs).filter(k => RGI.test(k) && !comp.has(k) && !reach.has(ink(k)));
assert.deepEqual(lost.map(hex), [], 'every compatible emoji engraves with an offered outline');
// and no two cells (flags excepted: each is a different place) engrave with the same outline
const inkOwner = new Map();
for (const g of D.groups) if (g.id !== 'flags') for (const it of g.items) { const i = ink(it.c); assert(!inkOwner.has(i), 'same outline offered twice: ' + hex(it.c) + ' and ' + hex(inkOwner.get(i) || '')); inkOwner.set(i, it.c); }
// all offered tone variants but the few that change the outline engrave exactly like the plain emoji
const toneChanging = items.filter(i => i.ts).map(i => i.n);
for (const it of items) if (it.t && !it.ts) for (const v of Object.values(D.toned[it.c])) assert.equal(ink(v), ink(it.c), 'tones do not change ' + it.n);
assert(toneChanging.length > 0 && toneChanging.length < 10, 'tone-changing items: ' + toneChanging.join(', '));

// group anchors: the first emoji of each group is the one emoji-test starts it with
const first = { smileys: '😀', people: '👋', animals: '🐵', food: '🍇', travel: '🌍', activities: '🎃', objects: '👓', symbols: '🏧', flags: '🏁' };
for (const g of D.groups) assert.equal(g.items[0].c, first[g.id], g.id + ' starts where emoji-test starts it');
// flags: every country and region flag of the map, with its name
const regionFlags = Object.keys(seqs).filter(k => /^[\u{1F1E6}-\u{1F1FF}]{2}$/u.test(k) && !unsupported.has(k));
const flagCells = new Set(D.groups.find(g => g.id === 'flags').items.map(i => i.c));
for (const f of regionFlags) assert(flagCells.has(f), 'flag offered: ' + hex(f));
assert(D.groups.find(g => g.id === 'flags').items.find(i => i.c === '🇨🇦').n === 'Canada');
// keycaps and symbols
for (const c of ['#️⃣', '*️⃣', '0️⃣', '9️⃣', '🔟', '©️', '♻️', '❤️']) assert(items.some(i => i.c === c || (i.s || []).includes(c)), 'offered: ' + hex(c));
// only supported keys, none from `unsupported`
for (const k of unsupported) assert(!seen.has(k), 'unsupported key offered: ' + hex(k));

// names: more than 99 % named, the rest listed
const unnamed = items.filter(i => !i.n.trim());
console.log('named', items.length - unnamed.length, 'of', items.length, unnamed.length ? 'unnamed: ' + unnamed.map(i => hex(i.c)).join(' | ') : '');
assert((items.length - unnamed.length) / items.length > 0.99, 'name coverage over 99%');
for (const i of items) assert(!/[A-Z]{3,}.*[A-Z]{3,}/.test(i.n) || /^[A-Z]/.test(i.n), 'not a raw Unicode name: ' + i.n);

// search
const top = (q, n = 1) => D.search(q).slice(0, n);
assert.equal(D.search('').length, 0); assert.equal(D.search('   ').length, 0); assert.equal(D.search('zzzzqqq').length, 0);
assert.equal(top('heart')[0].c, '❤️', 'heart -> red heart first'); assert(D.search('heart').length > 15);
assert(['🐈', '🐱'].includes(top('cat')[0].c), 'cat -> a cat first'); assert(top('cat', 3).every(i => /cat/.test(i.n)));
assert.equal(top('fr')[0].c, '🇫🇷', 'fr -> the flag of France first');
assert.equal(top('1f600')[0].c, '😀', 'hex code point'); assert.equal(top('U+1F600')[0].c, '😀'); assert.equal(top('1F600')[0].c, '😀');
assert.equal(top('2764')[0].c, '❤️', 'hex ignores the emoji selector');
assert.equal(top('FRANCE')[0].c, '🇫🇷', 'case folded'); assert.equal(top('curacao')[0].c, '🇨🇼', 'accents folded'); assert.equal(top('cote divoire')[0].c, '🇨🇮'); assert.equal(top("Côte d'Ivoire")[0].c, '🇨🇮');
assert.equal(top('pizza')[0].c, '🍕'); assert.equal(top('star')[0].c, '⭐'); assert.equal(top('sun')[0].c, '☀️'); assert.equal(top('dog')[0].c, '🐕');
assert.equal(top('car')[0].c, '🚗'); assert.equal(top('ring')[0].c, '💍'); assert.equal(top('crown')[0].c, '👑'); assert.equal(top('gem')[0].c, '💎');
assert(['❌', '✝️'].includes(top('cross')[0].c), 'cross');
assert(/smil/.test(top('smile')[0].n), 'smile -> a smiling face first');
assert.equal(top('thumbs up')[0].c, '👍'); assert.equal(top('woman frowning')[0].c, '🙍', 'folded names are searchable'); assert.equal(top('man health worker')[0].c, '🧑‍⚕️');
assert.equal(top('❤️')[0].c, '❤️', 'a pasted emoji finds itself'); assert.equal(top('canada')[0].c, '🇨🇦'); assert.equal(top('ca')[0].c, '🇨🇦');
assert.equal(top('red heart')[0].c, '❤️', 'two words'); assert.equal(top('red  heart')[0].c, '❤️');
// ranking: exact, then prefix, then contains
{ const r = D.search('cat'); const exact = r.findIndex(i => i.n === 'cat'), prefix = r.findIndex(i => i.n.startsWith('cat ')), contains = r.findIndex(i => !i.n.startsWith('cat') && i.n.includes('cat'));
  assert(exact === 0 && prefix > exact && (contains < 0 || contains > prefix), 'exact < prefix < contains'); }
assert(D.search('face').every(i => (i.n + ' ' + i.k).toLowerCase().includes('face')));
// tones and pairs
assert.equal(D.toned['👋']['\u{1F3FB}'], '👋\u{1F3FB}'); assert.equal(D.toned['👋']['\u{1F3FF}'], '👋\u{1F3FF}');
assert.equal(D.toned['☝️']['\u{1F3FD}'], '☝\u{1F3FD}', 'the emoji selector gives way to the tone (as in the map)');
assert.equal(D.toned['🧑‍🤝‍🧑']['\u{1F3FC}'], '🧑\u{1F3FC}‍🤝‍🧑\u{1F3FC}');
assert.equal(D.pair('🤝', '\u{1F3FB}', '\u{1F3FC}'), '🫱\u{1F3FB}‍🫲\u{1F3FC}'); assert.equal(D.pair('🤝', '\u{1F3FB}', '\u{1F3FB}'), ''); assert.equal(D.pair('😀', '\u{1F3FB}', '\u{1F3FC}'), '');
assert.equal(D.pair('💑', '\u{1F3FB}', '\u{1F3FC}'), '🧑\u{1F3FB}‍❤️‍🧑\u{1F3FC}', 'couple with heart, mixed tones');
// the file is what the generator writes (when this Node's ICU is the one the file was built with)
const built = (source.match(/Built with ICU ([\d.]+)/) || [])[1];
if (built === process.versions.icu) assert.equal(require(root + '/scripts/build-emoji-picker-data.cjs').build().text, source, 'charm-nest-emoji-data.js is what scripts/build-emoji-picker-data.cjs writes');
else console.log('(generator output not compared: file built with ICU ' + built + ', this Node has ' + process.versions.icu + ')');

// a plain browser script: no module, a window, no other globals
{ const win = {}, ctx = vm.createContext({ window: win }); vm.runInContext(source, ctx);
  assert.equal(win.CNEmojiData.count, D.count); assert.equal(win.CNEmojiData.search('heart')[0].c, '❤️'); assert.equal(Object.keys(ctx).sort().join(), 'window', 'no stray globals'); }
// wired in
const html = fs.readFileSync(path.join(root, 'charm-nest-1.html'), 'utf8');
assert(/<script src="charm-nest-emoji-data\.js\?v=[^"]+"><\/script>/.test(html), 'charm-nest-1.html loads the emoji data');
assert(html.indexOf('charm-nest-emoji-data.js') < html.indexOf('charm-nest-bridge.js?v='), 'before the bridge');
assert(JSON.parse(fs.readFileSync(path.join(root, 'scripts/build-public.cjs'), 'utf8').match(/const assets = (\[[\s\S]*?\]);/)[1]).includes('charm-nest-emoji-data.js'), 'ships in the public build');

const perGroup = D.groups.map(g => g.id + ' ' + g.items.length).join(', ');
console.log(`emoji-data OK: ${D.count} cells (${perGroup}); ${variants} tone variants, ${pairs} mixed-tone pairs, ${items.reduce((n, i) => n + (i.s || []).length, 0)} folded; all engravable`);
