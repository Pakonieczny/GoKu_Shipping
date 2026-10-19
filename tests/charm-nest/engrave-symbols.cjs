/* ENGRAVE-SYMBOLS (Paul, 18 Oct 2026: "Unsupported engraving characters: ♡ (U+2661)" — every character a person can text must engrave).
 * Offline, node only: the real text wrapper with the shop's real fonts, the real fit, and the real fitting worker in an OS thread.
 *  1  the refused order: "♡ IX.V.MMXXV" and "Trust yourself, love yourself! ♡" engrave in the default face and in a script face, the heart drawn from the symbol font at the face's capital height
 *  2  what a phone keyboard appends (variation selectors, zero-width characters, bidi marks) draws nothing and is never "unsupported"; exotic spaces are spaces
 *  3  a letter the script face lacks is set in Source Sans 3; a styled character with no glyph anywhere (fullwidth, math script, circled) is set as its plain form
 *  4  what must stay a person's decision stays refused and never reaches the cut file as a .notdef box: a tab, a loose accent, and scripts that need shaping or run right to left (Hebrew, Arabic, Thai, Devanagari), CJK
 *  5  nothing that engraved before changes by a single outline command
 *  6  the page and the worker set a line the same way; the symbol font ships (manifest, licence, bridge wiring) and a missing file never holds the emoji back
 */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const { Worker } = require('node:worker_threads');
const G = require('../../charm-nest-geom.js'), Fit = require('../../charm-nest-engrave-fit.js'), T = require('../../charm-nest-text.js'), ot = require('../../vendor/opentype-1.3.4.min.js');
const data = require('../../vendor/fonts/emoji-sequences.json');
const bytes = p => { const b = fs.readFileSync(p); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
const emoji = ot.parse(bytes('vendor/fonts/NotoEmoji-Regular.ttf')), sym = ot.parse(bytes('vendor/fonts/DejaVuSans.ttf'));
const sans = ot.parse(bytes('vendor/fonts/SourceSans3-Regular.otf')), myn = ot.parse(bytes('vendor/fonts/Mynerve-Regular.ttf'));
const wrap = (base, fb) => T.withEmoji(base, emoji, data, ot.Path, fb);
const font = wrap(sans, [sym]), script = wrap(myn, [sans, sym]), bare = wrap(sans, []);
const cmds = (f, t, size = 40) => f.getPath(t, 0, 0, size).commands;
const cap = f => (f.tables.os2.sCapHeight || f.charToGlyph('H').getBoundingBox().y2 || f.ascender * .7) / f.unitsPerEm;   // (DejaVu's OS/2 table has no capital height: measured on its H)
let checks = 0; const ok = (c, m) => { assert(c, m); checks++; };
const same = (a, b, m) => { assert.deepEqual(a, b, m); checks++; };

// 1 · the order that was refused
for (const [name, f] of [['Source Sans 3', font], ['a script face', script]]) for (const t of ['♡ IX.V.MMXXV', 'Trust yourself, love yourself! ♡']) {
  ok(G.glyphCoverage(f, t).ok, `${name}: "${t}" engraves`);
  ok(f.getPath(t, 0, 0, 30).commands.length > 0, `${name}: "${t}" has outlines`);
}
{
  const k = cap(sans) / cap(sym);
  same(cmds(font, '♡'), sym.getPath('♡', 0, 0, 40 * k, { kerning: true }).commands, 'the heart is the symbol font\'s own outline, at the face\'s capital height');
  const bb = font.getPath('♡', 0, 0, 40).getBoundingBox(), capPx = cap(sans) * 40, h = bb.y2 - bb.y1;
  ok(h > capPx * 0.6 && h < capPx * 1.3, `the heart stands beside the letters (${h.toFixed(1)} px against a capital of ${capPx.toFixed(1)} px)`);
  ok(font.getAdvanceWidth('A♡A', 40) > font.getAdvanceWidth('AA', 40), 'the heart takes room in the line');
}
// the real fit, on a skinny bar and a disc, with the real policy
{
  const rect = (x, y, w, h) => [['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']];
  const charmOf = (w, h) => { const o = { kind: 'path', layer: 'CUT', closed: true, stroke: true, strokeRGB: [1, 0, 0], subpaths: [rect(0, 0, w, h)], bbox: [0, 0, w, h] }; return { outline: o, members: [o], bbox: o.bbox }; };
  for (const [w, h] of [[40, 6], [25, 25]]) for (const lines of [['♡ IX.V.MMXXV'], ['Trust yourself, love yourself! ♡']]) {
    const r = Fit.calculate({ charm: charmOf(w, h), lines, lineMode: 'auto', viewOptions: { res: 6, upAngle: 90 }, maskOptions: { marginMm: .8, keepOut: [] }, opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: false } }, { Regular: font, Semibold: wrap(ot.parse(bytes('vendor/fonts/SourceSans3-Semibold.otf')), [sym]) }, G);
    ok(r && r.fit, `a ${w}x${h} mm charm fits ${JSON.stringify(lines)}`);
  }
}

// 2 · invisible characters and spaces
for (const sel of ['\uFE0F', '\uFE0E']) { ok(G.glyphCoverage(font, '♡' + sel).ok, '♡ + variation selector engraves'); same(cmds(font, '♡' + sel), cmds(font, '♡'), 'the selector draws nothing'); }
same(cmds(font, 'A\u200BB\u200CC\u200D\u2060D\uFEFF'), cmds(font, 'ABCD'), 'zero-width space, joiners, word joiner and a byte-order mark draw nothing');
ok(G.glyphCoverage(script, 'A\u00ADB').ok, 'a soft hyphen is never "unsupported" (Source Sans 3 has always drawn it as a hyphen; a face without the glyph draws nothing)');
same(cmds(font, '\u200EAnna\u200F'), cmds(font, 'Anna'), 'bidi marks draw nothing');
ok(G.glyphCoverage(font, '\u200B').ok && cmds(font, '\u200B').length === 0, 'a line of only zero-width characters is covered and empty (the words step decides what an empty line means)');
for (const sp of ['\u00A0', '\u2009', '\u2003', '\u202F', '\u3000']) { ok(G.glyphCoverage(script, 'A' + sp + 'B').ok, `U+${sp.codePointAt(0).toString(16).toUpperCase()} is a space`); }
same(cmds(script, 'A\u2009B'), cmds(script, 'A B'), 'a thin space is set as a space');

// 3 · a letter the script face lacks; a styled character with no glyph
{
  let lack = null; for (let cp = 0xA1; cp < 0x250 && !lack; cp++) { const c = String.fromCodePoint(cp); if (sans.charToGlyphIndex(c) && !myn.charToGlyphIndex(c) && !/\p{M}/u.test(c)) lack = c; }
  ok(lack, 'the script face lacks some Latin letter that Source Sans 3 has (' + lack + ')');
  ok(G.glyphCoverage(script, 'Anna ' + lack).ok, `"${lack}" in a script face engraves`);
  same(cmds(script, lack).length > 0, true, 'and is drawn');
  ok(!G.glyphCoverage(wrap(myn, []), lack).ok, 'without a fallback face the old behaviour is kept (a page whose font file is missing)');
}
for (const [styled, plain] of [['ＡＢＣ ｈｅｌｌｏ', 'ABC hello'], ['𝓗𝓮𝓵𝓵𝓸', 'Hello'], ['𝐀𝐁', 'AB'], ['Ⓐ ⓐ', 'A a'], ['㎏', 'kg']]) {
  ok(G.glyphCoverage(font, styled).ok, `"${styled}" engraves`);
  same(cmds(font, styled), cmds(font, plain), `"${styled}" is set as "${plain}"`);
}
for (const t of ['★ ☆ ✦ ✧ ✿ ❀', '✓ ✗ • … – — †', '← → ➤', '♪ ♫', '☮ ☯ ☠ ♀ ♂', '€ £ ¥ ₹ ™ © ® ° ℃', 'Łukasz Żółć Việt Ελένη Анна', '❤\uFE0F 💖 🇨🇦 👨\u200D👩\u200D👧\u200D👦']) ok(G.glyphCoverage(font, t).ok && G.glyphCoverage(script, t).ok, `${t} engraves in both faces`);

// 4 · what stays a person's decision
for (const [what, t] of [['a tab', 'A\tB'], ['a loose accent', 'q\u0301'], ['Hebrew', 'שלום'], ['Arabic', 'مرحبا'], ['Thai', 'สว\u0E31สด\u0E35'], ['Devanagari', 'नमस\u094Dत\u0947'], ['Chinese', '愛'], ['katakana', 'ツ']]) {
  ok(!G.glyphCoverage(font, t).ok && !G.glyphCoverage(script, t).ok, `${what} is still not engraved by itself`);
  assert.throws(() => font.getPath(t, 0, 0, 20), /Unsupported engraving characters/, `${what} can never reach the cut file as a box`); checks++;
}
ok(G.glyphCoverage(font, 'Anna שלום').missing.length > 0 && !G.glyphCoverage(font, 'Anna שלום').missing.includes('A'), 'a line with one refused word names only what it refused');

// 5 · nothing that engraved before changes
for (const t of ['Anna Tom', 'Fluffy & Hammy 2024', 'Café', 'Zoë Łukasz', '❤\uFE0F Hecht!', '🇨🇦', '33°0′45″N', 'ß']) {
  same(cmds(font, t), cmds(bare, t), `"${t}" is set exactly as without fallbacks`);
  same(font.getAdvanceWidth(t, 40), bare.getAdvanceWidth(t, 40), `"${t}" has the same width`);
}
same(cmds(bare, 'Anna Tom'), sans.getPath('Anna Tom', 0, 0, 40, { kerning: true }).commands, 'plain words are the face\'s own kerned outlines');
ok(!G.glyphCoverage(bare, '♡').ok, 'with no symbol font a heart is still refused (a page whose symbol file is missing behaves as before)');

// 6 · the wiring
const bridge = fs.readFileSync('charm-nest-bridge.js', 'utf8'), worker = fs.readFileSync('charm-nest-engrave-worker.js', 'utf8'), pub = fs.readFileSync('scripts/build-public.cjs', 'utf8'), html = fs.readFileSync('charm-nest-1.html', 'utf8');
ok(fs.existsSync('vendor/fonts/DejaVuSans.ttf') && fs.statSync('vendor/fonts/DejaVuSans.ttf').size > 100000, 'the symbol font is in vendor/fonts');
ok(/Bitstream/.test(fs.readFileSync('vendor/fonts/DejaVuSans-LICENSE.txt', 'utf8')) && pub.includes('"vendor/fonts/DejaVuSans-LICENSE.txt"'), 'its licence is shipped with it (public manifest)');
ok(bridge.includes('const SYMBOL_FILE = "vendor/fonts/DejaVuSans.ttf"') && bridge.includes('/HTTP 4\\d\\d/.test(e.message)'), 'the bridge loads it, and only a missing file (HTTP 4xx) is tolerated: a bad connection retries like the emoji font');
ok(bridge.includes('opentype.Path, symbols ? [symbols] : [])') && bridge.includes('[F_.sansRaw, F_.symbolFont].filter(Boolean)'), 'the default face falls back to the symbols; every other face to Source Sans 3 and the symbols');
ok(bridge.includes('F_.workerFonts.symbol = symbolBytes') && worker.includes('data.fonts.symbol') && worker.includes('[sansRaw,symbolFont].filter(Boolean)'), 'the worker gets the same fallbacks');
ok(/charm-nest-engrave-worker\.js\?v=[^"]*-sy1"/.test(bridge) && /charm-nest-bridge\.js\?v=[^"]*-sy1"/.test(html), 'browsers fetch the new bridge and worker (cache tokens)');
const loadFontsChecks = (async () => { // the page's loadFonts with a symbol file that is not on the site (404) and with one that cannot be fetched (offline)
  const vm = require('node:vm');
  const start = bridge.indexOf('  const FONT_FILES =', bridge.indexOf('const Engrave =')), end = bridge.indexOf('  const fontFor =', start);
  const run = async fetch => {
    const F_ = {}, wrapped = [];
    const ctx = { AbortController, Promise, Date, Uint8Array, F_, fetch, opentype: { parse: () => ({ names: { fullName: { en: 'Test' } } }), Path: function () {} }, CN: { sha256: async () => 'test' },
      window: { CharmNestText: { withEmoji: (f, e, m, P, fb) => { wrapped.push(fb); return f; } } }, document: { getElementById: () => null }, agent() {}, esc: x => x, setTimeout, clearTimeout, items: () => new Map(), setReady: () => Promise.resolve(), G: { glyphCoverage: () => ({ ok: true }) } };
    vm.createContext(ctx); vm.runInContext(bridge.slice(start, end), ctx); await ctx.loadFonts(); for (let i = 0; i < 40; i++) await Promise.resolve(); return { F_, wrapped };
  };
  const ans = path => ({ ok: true, arrayBuffer: async () => new ArrayBuffer(2000), json: async () => ({ fontSha256: 'test' }) });
  const good = await run(async p => ans(p)); ok(good.F_.emoji && good.F_.symbolFont && good.wrapped.every(fb => fb.length === 1), 'both files fetched: emoji on, symbols on');
  const gone = await run(async p => /DejaVu/.test(p) ? { ok: false, status: 404 } : ans(p)); ok(gone.F_.emoji && !gone.F_.symbolFont && gone.F_.symbolError && gone.wrapped.every(fb => fb.length === 0), 'a symbol file the site lacks (404): emoji still on, no symbols');
  const off = await run(async p => { if (/DejaVu/.test(p)) throw new Error('offline'); return ans(p); }); ok(!off.F_.emoji && /offline/.test(off.F_.emojiError || ''), 'a symbol file that cannot be fetched is retried like the emoji font (not silently dropped)');
})();

// 6 · the fitting worker sets a line as the page does
const workers = [];
class Adapter {
  constructor() {
    this.w = new Worker(`
      const {parentPort}=require('node:worker_threads'),fs=require('node:fs'),vm=require('node:vm');
      const context=vm.createContext({console,Intl,TextEncoder,TextDecoder,setTimeout,clearTimeout});
      context.self=context; context.postMessage=data=>parentPort.postMessage(data);
      context.importScripts=(...files)=>files.forEach(file=>vm.runInContext(fs.readFileSync(file.split('?')[0],'utf8'),context,{filename:file}));
      context.importScripts('charm-nest-engrave-worker.js');
      parentPort.on('message',data=>context.onmessage({data}));
    `, { eval: true });
    this.w.on('message', d => this.onmessage?.({ data: d })); this.w.on('error', e => this.onerror?.(e)); workers.push(this);
  }
  postMessage(d) { this.w.postMessage(d); } terminate() { this.w.terminate(); }
}
(async () => {
  await loadFontsChecks;
  const rect = (x, y, w, h) => [['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']];
  const o = { kind: 'path', layer: 'CUT', closed: true, stroke: true, strokeRGB: [1, 0, 0], subpaths: [rect(0, 0, 40, 6)], bbox: [0, 0, 40, 6] }, charm = { outline: o, members: [o], bbox: o.bbox };
  const mk = (lines, fontKey) => ({ charm, lines, lineMode: 'auto', viewOptions: { res: 6, upAngle: 90 }, maskOptions: { marginMm: .8, keepOut: [] }, opts: { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: false }, ...(fontKey ? { fontKey } : {}) });
  const semi = ot.parse(bytes('vendor/fonts/SourceSans3-Semibold.otf'));
  const fontBytes = { Regular: bytes('vendor/fonts/SourceSans3-Regular.otf'), Semibold: bytes('vendor/fonts/SourceSans3-Semibold.otf'), emoji: bytes('vendor/fonts/NotoEmoji-Regular.ttf'), emojiMap: data, symbol: bytes('vendor/fonts/DejaVuSans.ttf') };
  const mynBytes = { Regular: bytes('vendor/fonts/Mynerve-Regular.ttf') };
  const client = Fit.createClient({ WorkerClass: Adapter, url: 'charm-nest-engrave-worker.js', fonts: fontBytes, fontsFor: id => id === 'mynerve' ? mynBytes : null });
  const pageDefault = { Regular: font, Semibold: wrap(semi, [sym]) }, pageScript = { Regular: script };
  const heart = mk(['Trust yourself, love yourself! ♡']);
  same((await client.run(heart)).fit, Fit.calculate(heart, pageDefault, G).fit, 'the worker fits the heart line exactly as the page does');
  let lack = null; for (let cp = 0xA1; cp < 0x250 && !lack; cp++) { const c = String.fromCodePoint(cp); if (sans.charToGlyphIndex(c) && !myn.charToGlyphIndex(c) && !/\p{M}/u.test(c)) lack = c; }
  const own = mk(['Anna ' + lack + ' ♡'], 'mynerve');
  same((await client.run(own)).fit, Fit.calculate(own, pageScript, G).fit, 'a script face in the worker falls back to Source Sans 3 and the symbols exactly as the page does');
  const noSymbol = Fit.createClient({ WorkerClass: Adapter, url: 'charm-nest-engrave-worker.js', fonts: { ...fontBytes, symbol: undefined } });
  await assert.rejects(noSymbol.run(heart), /Unsupported engraving characters/, 'a worker that was not sent the symbol font refuses the heart (as before), never draws a box'); checks++;
  workers.forEach(a => a.terminate());
  console.log(`engrave-symbols.cjs: passed (${checks} checks)`);
})().catch(e => { console.error(e); workers.forEach(a => a.terminate()); process.exitCode = 1; });
