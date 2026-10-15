/* ADV-ENGRAVE (Paul, 15 Oct 2026, final test: "resilient against lost internet connections, interruptions and unexpected user behaviour").
 * Offline, node only: the real text wrapper with the shop's real fonts, and the real loadFonts / font-blocked wiring of charm-nest-bridge.js. No Etsy, no cloud.
 *  1  a TAB inside the words: the font has no glyph for it, so its .notdef box used to be cut between the words (coverage let a tab through). It is "not in this font" now, and a person decides.
 *  2  an accent typed apart from its letter ("e" + U+0301, pasted from a Mac file name): it was drawn after the letter, not over it. It is set composed ("é") now; an accent with no composed form is "not in this font".
 *  3  the font files could not be had (a bad connection at page load): every line read meanwhile was blocked "font files missing" and each waited for a person to press Retry. When the files come, they are read again by themselves.
 */
const assert = require('node:assert/strict'), fs = require('node:fs'), vm = require('node:vm');
const G = require('../../charm-nest-geom.js'), Text = require('../../charm-nest-text.js'), ot = require('../../vendor/opentype-1.3.4.min.js');
const base = ot.loadSync('vendor/fonts/SourceSans3-Regular.otf'), emoji = ot.loadSync('vendor/fonts/NotoEmoji-Regular.ttf'), data = require('../../vendor/fonts/emoji-sequences.json');
const font = Text.withEmoji(base, emoji, data, ot.Path);

// 1 · a tab
assert(!G.glyphCoverage(font, 'Anna\t2024').ok, 'a tab between words is not in the font');
assert.deepEqual(G.glyphCoverage(font, 'Anna\t2024').missing, ['\t']);
assert.throws(() => font.getPath('Anna\t2024', 0, 0, 20), /Unsupported engraving characters/, 'a tab can never reach the cut file as the font\'s empty box');
assert(G.glyphCoverage(font, 'Anna 2024').ok && G.glyphCoverage(font, 'Anna Tom').ok, 'a space (or a no-break space) is still fine');
assert(G.glyphCoverage(font, 'Anna\nTom').ok, 'a line break is a line break, never a character');

// 2 · a loose accent
const loose = 'Café', composed = 'Café';
assert(G.glyphCoverage(font, loose).ok, 'a letter + accent that compose is fine');
assert.deepEqual(font.getPath(loose, 0, 0, 20).commands, font.getPath(composed, 0, 0, 20).commands, 'it is set exactly as the composed letter: the accent over the e');
assert.equal(font.getAdvanceWidth(loose, 20), font.getAdvanceWidth(composed, 20));
assert(!G.glyphCoverage(font, 'q́').ok, 'an accent with no composed letter cannot sit on its letter here: a person decides');
for (const t of ['Łukasz Żółć', 'Zoë', 'Việt', 'Анна', 'Ελένη', 'Anna’s “Tom”', '33°0′45″N', '❤️ Hecht!', '🇨🇦', '👨‍👩‍👧‍👦', 'ß']) assert(G.glyphCoverage(font, t).ok, t + ' still engraves');

// 3 · the font files come back
const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
const start = source.indexOf('  const FONT_FILES =', source.indexOf('const Engrave ='));
const end = source.indexOf('  const fontFor =', start);
const turn = async () => { for (let i = 0; i < 40; i++) await Promise.resolve(); };
function fixture(fetch, jobs) {
  const readied = [], F_ = {};
  const ctx = {
    AbortController, Promise, Date, Uint8Array, F_, fetch, opentype: { parse: () => ({ names: { fullName: { en: 'Test font' } } }), Path: function () {} }, CN: { sha256: async () => 'test' },
    window: { CharmNestText: { withEmoji: f => f } }, document: { getElementById: () => null }, agent() {}, esc: x => x,
    setTimeout, clearTimeout, items: () => jobs, setReady: job => { readied.push(job); return Promise.resolve(job); }, G: { glyphCoverage: () => ({ ok: true }) },
  };
  vm.createContext(ctx); vm.runInContext(source.slice(start, end), ctx);
  return { ctx, readied, F_ };
}
(async () => {
  const why = (() => { const f = fixture(async () => { throw new Error('offline'); }, new Map()); return vm.runInContext('FONT_MISSING_WHY', f.ctx); })();
  assert.match(why, /font files are missing/);
  assert(source.includes('job.reason = FONT_MISSING_WHY;'), 'setReady blocks a line with exactly the reason loadFonts looks for');
  assert(source.includes('if (!F_.ok || F_.emojiError) loadFonts(true)'), 'the network coming back asks for the missing font files, not only the emoji');
  const mk = (key, state, reason) => ({ key, state, reason });
  const jobs = new Map([['a', mk('a', 'blocked', why)], ['b', mk('b', 'blocked', why)], ['placement', mk('placement', 'blocked', 'flip does not match')], ['c', mk('c', 'review', null)], ['d', mk('d', 'approved', null)]]);
  let online = false, requests = 0;
  const fx = fixture(async () => { requests++; if (!online) throw new Error('offline'); return { ok: true, arrayBuffer: async () => new ArrayBuffer(2000), json: async () => ({ fontSha256: 'test' }) }; }, jobs);
  await fx.ctx.loadFonts(); await turn();
  assert.equal(fx.F_.ok, false, 'offline: no font'); assert.equal(fx.readied.length, 0, 'offline: nothing to read again');
  online = true;
  await fx.ctx.loadFonts(true); await turn();
  assert.equal(fx.F_.ok, true);
  assert.deepEqual(fx.readied.map(j => j.key).sort(), ['a', 'b'], 'only the lines blocked for the missing fonts are read again; a failed placement, a placement under review and an approval are left alone');
  const before = fx.readied.length; await fx.ctx.loadFonts(); await turn();
  assert.equal(fx.readied.length, before, 'fonts that are already here read nothing twice');
  console.log('PASS: a tab never becomes a box; a loose accent is set composed or asked about; lines blocked for missing fonts are read again when the fonts come back (' + requests + ' font requests)');
})().catch(e => { console.error(e); process.exitCode = 1; });
