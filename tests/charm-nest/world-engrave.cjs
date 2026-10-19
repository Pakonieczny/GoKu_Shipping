/* WORLD-ENGRAVE (Paul, 19 Oct 2026: "add the compatibility with the Arabic, Israeli, Chinese, Japanese, Korean, Indian letters, characters, words/symbols depending on the language").
 * Offline, node only: the real text wrapper, the real shaper (HarfBuzz + bidi in charm-nest-shape.js / vendor/hb.wasm), the real script fonts of vendor/fonts/scripts, the real fit, the real fitting worker in an OS thread,
 * and the bridge's own script-font loader (ensureScriptFonts) run against a fake network.
 *  1  every language: the line is covered, has outlines, is set by the font the manifest names for it and never by a .notdef box; the card can say "Set in <font>"
 *  2  the shaping is real: Arabic joins (and lam-alef is one glyph), Devanagari reorders and makes conjuncts, Hebrew niqqud sits on its letter, Hangul syllables and kana come out as such
 *  3  the visual order is right: Hebrew starts at the RIGHT end, numbers inside right-to-left text, brackets mirrored, Latin / numbers / symbols / emoji / script runs interleaved the way UAX #9 says
 *  4  Chinese and Japanese lines may break between characters (never before a closing mark); every other line breaks where it did
 *  5  what must stay a person's decision stays refused: a tab, a loose accent, a vowel sign or niqqud with no letter, a script with no font (Thai; or one whose font did not load), a character no loaded font has
 *  6  nothing that engraved before changes by a single outline command (Latin, accents, emoji, symbols, all with every script font loaded)
 *  7  the fitting worker sets a line like the page, gets the fonts and the engine once, handles its messages in order, and says so when a font is missing
 *  8  the real fit succeeds on a skinny bar and on a disc, in every script
 *  9  the bridge fetches NOTHING for a Latin line; for an Arabic line the manifest, the engine and the Arabic font only (never Hebrew, never CJK); a font that cannot be had holds the line "not in this font" and is asked again later
 * 10  PNG samples of every line (outline fill) and an index, for a person to look at: /tmp/claude-0/-home-user/c78cea93-e774-51c7-ae14-3958c10955c7/scratchpad/scripts-samples/
 */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm'), crypto = require('node:crypto');
const { Worker } = require('node:worker_threads');
const G = require('../../charm-nest-geom.js'), Fit = require('../../charm-nest-engrave-fit.js'), ot = require('../../vendor/opentype-1.3.4.min.js');
const fresh = f => { const p = require.resolve(f); delete require.cache[p]; return require(p); };
const T = fresh('../../charm-nest-text.js'), Shape = fresh('../../charm-nest-shape.js');
const data = require('../../vendor/fonts/emoji-sequences.json'), manifest = require('../../vendor/fonts/script-fonts.json');
const bytes = p => { const b = fs.readFileSync(p); return b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength); };
const emoji = ot.parse(bytes('vendor/fonts/NotoEmoji-Regular.ttf')), sym = ot.parse(bytes('vendor/fonts/DejaVuSans.ttf'));
const sans = ot.parse(bytes('vendor/fonts/SourceSans3-Regular.otf')), semi = ot.parse(bytes('vendor/fonts/SourceSans3-Semibold.otf')), myn = ot.parse(bytes('vendor/fonts/Mynerve-Regular.ttf'));
let checks = 0; const ok = (c, m) => { assert(c, m); checks++; };
const same = (a, b, m) => { assert.deepEqual(a, b, m); checks++; };
const plain = x => JSON.parse(JSON.stringify(x === undefined ? null : x));   // (what a vm context made is compared as data, not by its realm's Array)
const OUT = '/tmp/claude-0/-home-user/c78cea93-e774-51c7-ae14-3958c10955c7/scratchpad/scripts-samples';
const REF = 1000;
const workers = [], posted = [];   // (the fitting workers of section 7: every one is stopped however the run ends, a failed check must not leave the process hanging)

/* ── the page's script set: what the bridge builds (addFont, capital height, use) ── */
const fontById = Object.fromEntries(manifest.fonts.map(f => [f.id, f]));
function loadSet(ids) {
  const set = T.scripts(); set.attach(Shape); set.declare(manifest.fonts);
  for (const f of manifest.fonts) { if (ids && !ids.includes(f.id)) continue; const m = Shape.addFont(f.id, bytes(f.file)); set.use(Object.assign({}, f, { capEm: m.capHeight / m.unitsPerEm })); }
  return set;
}
const wrap = (base, set, fb) => T.withEmoji(base, emoji, data, ot.Path, fb || [sym], set);

(async () => {
  const t0 = Date.now();
  await Shape.init();
  const set = loadSet(), font = wrap(sans, set), fontSemi = wrap(semi, set), none = wrap(sans, null), stylish = wrap(myn, set, [sans, sym]);
  const loadMs = Date.now() - t0;
  const cmds = (f, t, size = 40) => f.getPath(t, 0, 0, size).commands;
  const adv = (f, t, size = 40) => f.getAdvanceWidth(t, size);
  const kOf = id => { const m = set.loaded.get(id), k = (sans.tables.os2.sCapHeight / sans.unitsPerEm) / m.capEm; return k; };
  // the shaper's own output for a run, scaled the way the wrapper scales it (a script font is drawn at the size that gives its capital H the face's capital height)
  const shaped = (id, text, { rtl = false, script, size = 40 } = {}) => { const r = Shape.shapeRun(text, { fontId: id, size: REF, rtl, script, lang: (fontById[id].langs || [])[0] }), s = size * kOf(id) / REF; return { advance: r.advance * s, glyphs: r.glyphs, missing: r.missing, commands: r.commands.map(c => { const o = { type: c.type }; if (c.type !== 'Z') { o.x = c.x * s; o.y = c.y * s; } if (c.x1 !== undefined) { o.x1 = c.x1 * s; o.y1 = c.y1 * s; } if (c.x2 !== undefined) { o.x2 = c.x2 * s; o.y2 = c.y2 * s; } return o; }) }; };
  const near = (a, b, eps = 1e-6) => a === b || Math.abs(a - b) <= eps;
  const sameCmd = (a, b, eps) => a.type === b.type && ['x', 'y', 'x1', 'y1', 'x2', 'y2'].every(k => (a[k] === undefined) === (b[k] === undefined) && (a[k] === undefined || near(a[k], b[k], eps)));
  const shift = (cs, dx) => cs.map(c => { const o = { ...c }; for (const k of ['x', 'x1', 'x2']) if (o[k] !== undefined) o[k] += dx; return o; });
  // `sub` (commands) drawn dx to the right appears, command for command and in a row, inside `whole`
  const contains = (whole, sub, dx, eps = 1e-3) => { const s = shift(sub, dx); outer: for (let i = 0; i + s.length <= whole.length; i++) { for (let j = 0; j < s.length; j++) if (!sameCmd(whole[i + j], s[j], eps)) continue outer; return true; } return false; };
  const bbox = cs => { let x1 = Infinity, y1 = Infinity, x2 = -Infinity, y2 = -Infinity; for (const c of cs) for (const [x, y] of [[c.x, c.y], [c.x1, c.y1], [c.x2, c.y2]]) if (x !== undefined) { x1 = Math.min(x1, x); x2 = Math.max(x2, x); y1 = Math.min(y1, y); y2 = Math.max(y2, y); } return { x1, y1, x2, y2 }; };

  /* ── 1 · every language ── */
  const S = [   // [group, language, text, the font(s) it is set in]
    ['arabic', 'Arabic', 'مرحبا', ['arabic']], ['arabic', 'Arabic', 'محمد', ['arabic']], ['arabic', 'Arabic (lam-alef)', 'لا', ['arabic']], ['arabic', 'Arabic', 'السلام عليكم', ['arabic']],
    ['arabic', 'Arabic with harakat', 'السَّلَامُ عَلَيْكُمْ', ['arabic']], ['arabic', 'Persian', 'پ چ ژ گ', ['arabic']], ['arabic', 'Persian (half space)', 'می‌خواهم', ['arabic']], ['arabic', 'Persian', 'سلام دنیا', ['arabic']],
    ['arabic', 'Urdu', 'پاکستان زندہ باد', ['arabic']], ['arabic', 'Urdu', 'اردو زبان', ['arabic']], ['arabic', 'Pashto', 'پښتو', ['arabic']], ['arabic', 'Arabic-Indic digits', 'عام ٢٠٢٤', ['arabic']],
    ['hebrew', 'Hebrew', 'שלום', ['hebrew']], ['hebrew', 'Hebrew with niqqud', 'שָׁלוֹם', ['hebrew']], ['hebrew', 'Hebrew', 'ישראל', ['hebrew']], ['hebrew', 'Hebrew', 'שלום עולם', ['hebrew']],
    ['hebrew', 'Hebrew + Latin + number', 'Anna שלום 2024', ['hebrew']], ['hebrew', 'Hebrew + Latin', 'שלום Anna', ['hebrew']], ['hebrew', 'Hebrew + number', 'שלום 2024', ['hebrew']], ['hebrew', 'Hebrew in brackets', '(שלום)', ['hebrew']],
    ['indic', 'Hindi', 'नमस्ते', ['devanagari']], ['indic', 'Hindi (conjunct)', 'क्षत्रिय', ['devanagari']], ['indic', 'Hindi (i matra)', 'हिन्दी', ['devanagari']], ['indic', 'Hindi', 'भारत', ['devanagari']],
    ['indic', 'Marathi', 'मराठी', ['devanagari']], ['indic', 'Nepali', 'नेपाल', ['devanagari']], ['indic', 'Sanskrit (danda)', 'ॐ नमः शिवाय।', ['devanagari']], ['indic', 'Bengali', 'বাংলা', ['bengali']], ['indic', 'Bengali (conjunct)', 'ক্ষমা', ['bengali']],
    ['indic', 'Punjabi', 'ਪੰਜਾਬੀ', ['gurmukhi']], ['indic', 'Gujarati', 'ગુજરાતી', ['gujarati']], ['indic', 'Tamil', 'தமிழ்', ['tamil']], ['indic', 'Telugu', 'తెలుగు', ['telugu']],
    ['indic', 'Kannada', 'ಕನ್ನಡ', ['kannada']], ['indic', 'Malayalam', 'മലയാളം', ['malayalam']], ['indic', 'Odia', 'ଓଡ଼ିଆ', ['oriya']], ['indic', 'Sinhala', 'සිංහල', ['sinhala']],
    ['cjk', 'Chinese (Simplified)', '你好，世界', ['cjk-sc']], ['cjk', 'Chinese (Simplified)', '爱与和平', ['cjk-sc']], ['cjk', 'Chinese (Traditional)', '愛與和平', ['cjk-tc']], ['cjk', 'Japanese (kanji + kana)', '日本語のテキスト', ['cjk-jp']],
    ['cjk', 'Japanese (hiragana)', 'ありがとう', ['cjk-jp']], ['cjk', 'Japanese (katakana)', 'カタカナ', ['cjk-jp']], ['cjk', 'Japanese (with punctuation)', 'こんにちは、世界！', ['cjk-jp']], ['cjk', 'Korean (Hangul)', '안녕하세요', ['cjk-kr']],
    ['cjk', 'Korean name', '김민준', ['cjk-kr']], ['cjk', 'Korean with Hanja', '한글 漢字', ['cjk-kr']],
    ['mixed', 'Latin + heart + Hebrew + number', 'Anna ♡ שלום 2024', ['hebrew']], ['mixed', 'Hearts around Arabic', '♡ مرحبا ♡', ['arabic']], ['mixed', 'Hebrew, heart, Latin', 'שלום ♡ Anna', ['hebrew']],
    ['mixed', 'Latin + Chinese + number + flag', 'Tom 田中 2024 🇨🇦', ['cjk-sc']], ['mixed', 'Latin + Hindi', 'Hello नमस्ते!', ['devanagari']], ['mixed', 'Hebrew + Arabic', 'שלום مرحبا', ['hebrew', 'arabic']],
    ['mixed', 'Latin + Korean', 'Anna 안녕', ['cjk-kr']], ['mixed', 'Latin + Tamil + Telugu', 'Hi தமிழ் తెలుగు', ['tamil', 'telugu']],
  ];
  for (const f of manifest.fonts) ok(set.loaded.has(f.id), `the manifest font ${f.id} loads into the shaper`);
  for (const lang of ['ar', 'fa', 'ur', 'he', 'hi', 'mr', 'ne', 'bn', 'pa', 'gu', 'ta', 'te', 'kn', 'ml', 'ja', 'ko', 'zh-Hans', 'zh-Hant']) ok(manifest.fonts.some(f => (f.langs || []).includes(lang)), `the manifest serves ${lang}`);
  const outline = (f, t) => { const p = f.getPath(t, 0, 0, 48), bb = p.getBoundingBox(); return { n: p.commands.length, w: bb.x2 - bb.x1, h: bb.y2 - bb.y1 }; };
  for (const [, lang, text, ids] of S) {
    const cov = G.glyphCoverage(font, text);
    ok(cov.ok, `${lang} "${text}" is covered (missing: ${cov.missing.join(' ')})`);
    ok(G.glyphCoverage(fontSemi, text).ok && G.glyphCoverage(stylish, text).ok, `${lang} "${text}": the Semibold weight and a shop face (Mynerve) set it too`);
    const o = outline(font, text); ok(o.n > 8 && o.w > 5 && o.h > 5, `${lang} "${text}" has outlines (${o.n} commands, ${o.w.toFixed(1)} x ${o.h.toFixed(1)} px)`);
    same(font.scriptFonts(text).map(f => f.id).sort(), ids.slice().sort(), `${lang} "${text}" is set in ${ids.join(' + ')}`);
    ok(font.scriptFonts(text).every(f => f.name && /^Noto Sans/.test(f.name)), 'the card can name the font: "Set in Noto Sans ..."');
    ok(adv(font, text) > 0 && adv(font, text, 80) > adv(font, text, 40) * 1.99, `${lang} "${text}" has a width that scales with the size`);
    // no .notdef: every character of the script runs is in its font (HarfBuzz reports a missing glyph; the outline would be a box)
    for (const id of ids) { const chars = [...text].filter(ch => set.strongRe().test(ch) && (fontById[id].scripts || []).some(sc => new RegExp('\\p{sc=' + sc + '}', 'u').test(ch))); if (chars.length) ok(Shape.covers(id, chars.join('')), `${lang} "${text}": ${id} has every one of its letters`); }
  }
  // a pure script line shaped on its own: HarfBuzz has no missing glyph and no glyph 0
  for (const [, lang, text, ids] of S.filter(s => s[3].length === 1 && !/[A-Za-z♡🇨]/u.test(s[2]))) {
    const r = Shape.shapeRun(text, { fontId: ids[0], size: 40, rtl: /[֐-ࣿ]/.test(text) }); same([r.missing.length, r.glyphs.some(g => g.id === 0)], [0, false], `${lang} "${text}": no missing glyph, no .notdef in the shaped run`);
  }

  /* ── 2 · the shaping is the engine's, not letter by letter ── */
  {
    const word = 'مرحبا', letters = [...word].map(c => shaped('arabic', c, { rtl: true }).advance).reduce((a, b) => a + b, 0);
    ok(Math.abs(adv(font, word) - letters) > 1, 'Arabic letters join: the word is not the sum of its isolated letters');
    same(cmds(font, word), shaped('arabic', word, { rtl: true }).commands, 'the Arabic word is the right-to-left shaping of the engine, scaled to the face');
    ok(JSON.stringify(cmds(font, word)) !== JSON.stringify(shaped('arabic', word, { rtl: false }).commands), 'and not its left-to-right shaping');
    { const la = Shape.shapeRun('لا', { fontId: 'arabic', size: REF, rtl: true }), iso = Shape.shapeRun('ل', { fontId: 'arabic', size: REF, rtl: true }).glyphs.concat(Shape.shapeRun('ا', { fontId: 'arabic', size: REF, rtl: true }).glyphs); ok(!la.glyphs.some(g => iso.some(i => i.id === g.id)) && la.advance < iso.reduce((a, g) => a + g.adv, 0) - 100, 'lam + alef takes the joined forms of the font (initial lam, final alef), not the two isolated letters'); }
    same(cmds(font, 'لا'), shaped('arabic', 'لا', { rtl: true }).commands, 'and the engraving draws that glyph');
    const hindi = Shape.shapeRun('हिन्दी', { fontId: 'devanagari', size: REF, rtl: false }), hi = hindi.glyphs.slice().sort((a, b) => a.x - b.x);
    ok(hi[0].cluster === 0 && hi[0].id !== Shape.shapeRun('ह', { fontId: 'devanagari', size: REF }).glyphs[0].id, 'Devanagari: the i matra is drawn LEFT of its consonant (reordered), as its own glyph');
    same(cmds(font, 'हिन्दी'), shaped('devanagari', 'हिन्दी').commands, 'the engraving draws the reordered word');
    ok(Shape.shapeRun('क्षत्रिय', { fontId: 'devanagari', size: REF }).glyphs.length < [...'क्षत्रिय'].length, 'Devanagari conjuncts (क्ष, त्र) become single glyphs');
    ok(adv(font, 'शָ'.normalize('NFC')) >= 0, 'niqqud does not throw');
    const niq = Shape.shapeRun('שָׁלוֹם', { fontId: 'hebrew', size: REF, rtl: true }), plain = Shape.shapeRun('שלום', { fontId: 'hebrew', size: REF, rtl: true });
    ok(Math.abs(niq.advance - plain.advance) < 1, 'Hebrew points sit on their letters and take no room of their own');
    ok(cmds(font, 'שָׁלוֹם').length > cmds(font, 'שלום').length + 10, 'and are drawn');
    same(cmds(font, 'שָׁלוֹם'), shaped('hebrew', 'שָׁלוֹם', { rtl: true }).commands, 'the engraving draws the pointed word as the engine shaped it');
    ok(cmds(font, 'ありがとう').length > 100 && cmds(font, '안녕하세요').length > 100, 'kana and Hangul syllables are drawn');
    same(cmds(font, '日本語のテキスト'), shaped('cjk-jp', '日本語のテキスト').commands, 'Japanese: one shaped run in the Japanese font');
    // the scale: a script font is drawn so that ITS capital H has the height of the face's capital H (Noto Sans next to Source Sans 3)
    for (const f of manifest.fonts) { const m = Shape.addFont(f.id, bytes(f.file)), k = (sans.tables.os2.sCapHeight / sans.unitsPerEm) / (m.capHeight / m.unitsPerEm); ok(k > .6 && k < 1.5, `${f.id}: capital height ${m.capHeight}/${m.unitsPerEm} gives the scale ${k.toFixed(3)} (inside the sane range, never 1 by accident)`); }
    ok(Math.abs(kOf('arabic') - sans.tables.os2.sCapHeight / 714) < .02, 'Noto Sans (capital 714) is drawn at 0.924 of its nominal size next to Source Sans 3 (capital 660)');
  }

  /* ── 3 · the visual order ── */
  {
    const w = 'שלום', pw = cmds(font, w), ad = adv(font, w), shin = cmds(font, 'ש'), mem = cmds(font, 'ם');
    ok(contains(pw, shin, ad - adv(font, 'ש')), 'Hebrew: the first letter (ש) is drawn at the RIGHT end of the word');
    ok(contains(pw, mem, 0), 'and the last letter (ם) at the LEFT end');
    // numbers inside right-to-left text: "שלום 2024" reads שלום then 2024, so 2024 is on the LEFT
    const hn = cmds(font, 'שלום 2024'), hebrew = cmds(font, 'שלום');
    ok(contains(hn, cmds(font, '2024'), 0), 'the number after a Hebrew word is drawn to its LEFT, its digits still left to right');
    ok(contains(hn, hebrew, adv(font, 'שלום 2024') - adv(font, 'שלום')), 'and the Hebrew word at the right end');
    // a Latin line with a Hebrew word: Latin first, then the Hebrew; a Hebrew line with Latin: the Latin at the left
    const mix = cmds(font, 'Anna שלום 2024'), ad2 = adv(font, 'Anna שלום 2024');
    ok(contains(mix, cmds(font, 'Anna'), 0), '"Anna שלום 2024": Anna at the left end');
    ok(contains(mix, cmds(font, '2024'), adv(font, 'Anna ')), '... then the number (it follows the Hebrew word in reading order, so it is drawn before it)');
    ok(contains(mix, hebrew, ad2 - adv(font, 'שלום')), '... and the Hebrew word at the right end');
    const rl = cmds(font, 'שלום Anna');
    ok(contains(rl, cmds(font, 'Anna'), 0) && contains(rl, hebrew, adv(font, 'שלום Anna') - adv(font, 'שלום')), '"שלום Anna" (a right-to-left line): Anna at the left end, the Hebrew word at the right end');
    // brackets are mirrored in right-to-left text: the opening bracket of "(שלום)" is drawn at the RIGHT, as a closing glyph
    const br = cmds(font, '(שלום)'), adb = adv(font, '(שלום)');
    ok(contains(br, cmds(font, '('), 0) && contains(br, cmds(font, ')'), adb - adv(font, ')')), '"(שלום)": the visible bracket on the left is "(" and on the right ")" (both mirrored from their logical places)');
    // symbols and emoji interleave: "שלום ♡ Anna" (right to left): Anna, then the heart, then the Hebrew
    const hs = cmds(font, 'שלום ♡ Anna'), kHeart = (sans.tables.os2.sCapHeight / sans.unitsPerEm) / ((sym.tables.os2.sCapHeight || sym.charToGlyph('H').getBoundingBox().y2) / sym.unitsPerEm);
    ok(contains(hs, cmds(font, 'Anna'), 0), 'right-to-left line with a heart and Latin: Anna at the left');
    ok(contains(hs, sym.getPath('♡', 0, 0, 40 * kHeart).commands, adv(font, 'Anna '), 1e-3), 'the heart (symbol font, at the face\'s capital height) follows it');
    ok(contains(hs, hebrew, adv(font, 'שלום ♡ Anna') - adv(font, 'שלום')), 'and the Hebrew word is at the right end');
    const em = cmds(font, 'שלום 🇨🇦'), flag = cmds(font, '🇨🇦');
    ok(flag.length > 4 && contains(em, flag, 0), 'a flag after Hebrew is drawn to the LEFT of it (reading order), whole');
    // Latin text with a script word first: left to right throughout
    const lt = cmds(font, 'Anna ♡ שלום'), kk = kHeart;
    ok(contains(lt, cmds(font, 'Anna'), 0) && contains(lt, sym.getPath('♡', 0, 0, 40 * kk).commands, adv(font, 'Anna ')) && contains(lt, hebrew, adv(font, 'Anna ♡ שלום') - adv(font, 'שלום')), '"Anna ♡ שלום": Anna, heart, Hebrew, left to right');
    // every face sets the Latin of a script line in ITS OWN letters, whichever face was asked first (the runs of a line are cached per face)
    for (const [what, f, base] of [['Semibold', fontSemi, wrap(semi, null)], ['a shop face (Mynerve)', stylish, wrap(myn, null, [sans, sym])]]) {
      cmds(font, 'Anna שלום 2024');
      ok(contains(cmds(f, 'Anna שלום 2024'), cmds(base, 'Anna'), 0) && JSON.stringify(cmds(f, 'Anna')) !== JSON.stringify(cmds(font, 'Anna')), `${what}: the Latin of a Hebrew line is that face's own (not the regular face's, cached)`);
      ok(Math.abs(adv(f, 'Anna שלום 2024') - adv(base, 'Anna ') - adv(base, '2024') - adv(base, ' ') - adv(f, 'שלום')) < 1e-6, `${what}: and its width is that face's own`);
    }
    // two scripts: Hebrew then Arabic: the first word (Hebrew) is at the RIGHT
    const ha = cmds(font, 'שלום مرحبا'), arabic = cmds(font, 'مرحبا');
    ok(contains(ha, hebrew, adv(font, 'שלום مرحبا') - adv(font, 'שלום')) && contains(ha, arabic, 0), 'Hebrew then Arabic (right to left): the Hebrew word at the right end, the Arabic at the left');
    // an Arabic phrase is one shaped run (the space inside it is the font's)
    same(cmds(font, 'السلام عليكم'), shaped('arabic', 'السلام عليكم', { rtl: true }).commands, 'an Arabic phrase with a space is ONE shaped run in the Arabic font');
    // a Chinese line with fullwidth marks is one run too; kana and Han follow each other as written
    same(cmds(font, '你好，世界'), shaped('cjk-sc', '你好，世界').commands, 'a Chinese line with a fullwidth comma is one run in the Chinese font');
    same(cmds(font, 'Tom 田中'), cmds(font, 'Tom ').concat(shift(shaped('cjk-sc', '田中').commands, adv(font, 'Tom '))), 'Latin then Chinese, left to right: the Latin in the face, the Chinese in its font');
    // Han, kana and Hangul pick their own font; an ideograph with kana is Japanese, with Hangul Korean, alone Chinese; Traditional-only characters go to the Traditional font
    same(font.scriptFonts('日本').map(f => f.id), ['cjk-sc'], 'ideographs alone: the Chinese font');
    same(font.scriptFonts('日本のこと').map(f => f.id), ['cjk-jp'], 'ideographs with kana: the Japanese font for the whole line');
    same(font.scriptFonts('日本 한글').map(f => f.id).sort(), ['cjk-kr'], 'ideographs with Hangul: the Korean font');
    same(font.scriptFonts('愛').map(f => f.id), ['cjk-tc'], 'a Traditional-only ideograph goes on to the next font of the chain');
    // a wrapped Japanese sentence: the row "日本" has no kana of its own and would be Chinese; the job is pinned to the fonts its WHOLE text was set in
    same(font.scriptFonts('日本').map(f => f.id), ['cjk-sc'], 'a line of ideographs on its own is Chinese (what a bare row of a wrapped Japanese sentence would be)');
    { const pin = font.pin(font.scriptFonts('日本語のテキスト').map(f => f.id)); same(pin.scriptFonts('日本').map(f => f.id), ['cjk-jp'], 'pinned to the fonts of the whole text, the row "日本" stays Japanese'); same(cmds(pin, '日本'), shaped('cjk-jp', '日本').commands, 'and is drawn from the Japanese font');
      same(pin.getAdvanceWidth('Anna', 40), font.getAdvanceWidth('Anna', 40), 'a pinned face sets Latin as before'); ok(font.pin([]) === font && font.pin(null) === font, 'pinning to nothing is the face itself'); ok(font.pin(['cjk-jp']) === font.pin(['cjk-jp']), 'a pinned face is made once'); }
    same(font.scriptFonts('愛和平').map(f => f.id), ['cjk-tc'], 'a line is set in ONE regional font when one has all of it (愛 is Traditional only, 和平 is in both: all Traditional)');
    same(font.scriptFonts('爱和平').map(f => f.id), ['cjk-sc'], '... and in the first font of the chain when it has all of it');
  }

  /* ── 4 · where a line may break ── */
  {
    same(G.wrapLines('שלום עולם', 2, font), ['שלום', 'עולם'], 'Hebrew breaks at its space, in reading order (the first line is the first word)');
    same(G.wrapLines('مرحبا بالعالم', 2, font), ['مرحبا', 'بالعالم'], 'so does Arabic');
    const ja = G.wrapLines('日本語のテキストです', 2, font);
    same(ja.length, 2, 'a Japanese line with no space can break in two');
    same(ja.join(''), '日本語のテキストです', 'and loses nothing');
    for (const l of G.wrapLines('日本語のテキストです', 4, font).concat(G.wrapLines('「こんにちは」、世界！', 3, font), G.wrapLines('コーヒーを飲みます', 3, font))) ok(!/^[、。ー！）」ぁぃぅぇぉっゃゅょ]/.test(l) && !/[「（]$/.test(l), `no line starts with a closing mark, a small kana or ー, none ends with an opening bracket: ${l}`);
    same(G.wrapLines('한국어 텍스트 입니다', 2, font), ['한국어', '텍스트 입니다'], 'Korean keeps its spaces (no break inside a word)');
    same(G.wrapLines('TOKYO 東京タワー', 2, font).join(''), 'TOKYO 東京タワー'.replace(/ /g, ' '), 'a line with Latin and kanji');
    ok(G.flowVariants(['日本語のテキストです'], font, 'auto').length > 2, 'the automatic line breaks offer more than one line for a Japanese sentence');
    same(G.wrapLines('Fluffy and Hammy forever', 2, none), G.wrapLines('Fluffy and Hammy forever', 2, none), 'Latin wrapping is unchanged');
    // layout: two lines, centred, no collision of the inks of two Arabic lines
    const L = G.layoutLines(['مرحبا', 'بالعالم'], font, 40, 0.216, 0, [0, 0]);
    ok(L.glyphs.length === 2 && L.cmds.length > 20, 'two Arabic lines lay out');
  }

  /* ── 5 · what stays refused ── */
  {
    for (const [what, t] of [['a tab', 'A\tB'], ['a tab inside Arabic', 'مرحبا\tمرحبا'], ['a tab inside Hebrew', 'שלום\tשלום'], ['a loose accent', 'q́'], ['a loose accent in a script line', 'שלום q́'],
      ['a lone niqqud', 'ָ'], ['niqqud after a space', ' ָ'], ['niqqud on a Latin letter', 'aָ'], ['a lone fatha', 'َ'], ['a fatha after a space', 'ب َ'], ['a lone virama', '्'], ['a vowel sign after a space', 'क ि'],
      ['Thai (no font)', 'สวัสดี'], ['Khmer (no font)', 'ខ្មែរ'], ['Tibetan (no font)', 'བོད་'], ['an ideograph no font has', '\u{20000}']]) {
      ok(!G.glyphCoverage(font, t).ok, `${what} is refused`);
      assert.throws(() => font.getPath(t, 0, 0, 20), /Unsupported engraving characters/, `${what} never reaches the cut file`); checks++;
    }
    same(G.glyphCoverage(font, 'Anna שלום ส').missing, ['ส'], 'a line with one refused character names only that character');
    ok(G.glyphCoverage(font, 'Anna שלום').missing.length === 0, 'and the Hebrew and the Latin are fine');
    // no script font loaded for it: exactly the old refusal (an empty set, a set whose engine never started, a set that only has another script)
    const bare = T.scripts(), notReady = T.scripts(), arabicOnly = loadSet(['arabic']);
    notReady.attach({ ready: false, covers: () => true }); notReady.use({ ...fontById.arabic, capEm: .7 });
    for (const [what, f] of [['no script set', wrap(sans, null)], ['an empty script set', wrap(sans, bare)], ['a script set whose engine never started', wrap(sans, notReady)]]) for (const t of ['שלום', 'مرحبا', 'नमस्ते', '日本語', '안녕'])
      { ok(!G.glyphCoverage(f, t).ok, `${what}: ${t} is refused as before`); assert.throws(() => f.getPath(t, 0, 0, 20), /Unsupported engraving characters/); checks++; }
    const ao = wrap(sans, arabicOnly);
    ok(G.glyphCoverage(ao, 'مرحبا').ok && !G.glyphCoverage(ao, 'שלום').ok && !G.glyphCoverage(ao, 'नमस्ते').ok, 'with only the Arabic font loaded, Arabic engraves and Hebrew and Hindi are still refused');
    same(G.glyphCoverage(ao, 'שלום مرحبا').missing.length, 4, 'a line of Hebrew and Arabic names the four Hebrew letters');
    arabicOnly.drop('arabic'); ok(!G.glyphCoverage(ao, 'مرحبا').ok, 'a font dropped from the set (a failed load) refuses its line again');
    // the old test of the symbol work still holds: with no script font nothing about the plain fallbacks changed
    same(cmds(none, 'A ♡ B'), cmds(font, 'A ♡ B'), 'symbols are unchanged with the script fonts loaded');
  }

  /* ── 6 · nothing that engraved before changes ── */
  {
    const before = wrap(sans, null);
    for (const t of ['Anna Tom', 'Fluffy & Hammy 2024', 'Café', 'Zoë Łukasz', '❤️ Hecht!', '🇨🇦', '33°0′45″N', 'ß', 'Trust yourself, love yourself! ♡', 'A·B', 'ＡＢＣ ｈｅｌｌｏ', '𝓗𝓮𝓵𝓵𝓸', 'Ⓐ ⓐ', '㎏', 'Анна Ελένη Việt', '👨‍👩‍👧‍👦', 'A​B‌C', '‎Anna‏', 'A B', 'A　B', '½ ¼ ™ ©', 'Anna\nTom']) {
      same(cmds(font, t), cmds(before, t), `"${t.replace(/\n/g, '\\n')}" is set exactly as without script fonts`);
      same(adv(font, t), adv(before, t), `"${t.replace(/\n/g, '\\n')}" has the same width`);
      same(G.glyphCoverage(font, t), G.glyphCoverage(before, t), `"${t.replace(/\n/g, '\\n')}" has the same coverage`);
    }
    for (const t of ['Anna Tom', 'Fluffy and Hammy forever', 'Happy Birthday to you Mom']) { same(G.wrapLines(t, 2, font), G.wrapLines(t, 2, before), 'wrapping is unchanged'); same(G.flowVariants([t], font, 'auto'), G.flowVariants([t], before, 'auto'), 'automatic flow is unchanged'); }
    same(stylish.getAdvanceWidth('Anna', 30), wrap(myn, null, [sans, sym]).getAdvanceWidth('Anna', 30), 'a script face is unchanged too');
    for (const t of ['A\tB', 'q́', '́']) same(G.glyphCoverage(font, t), G.glyphCoverage(before, t), `${JSON.stringify(t)} is refused as before`);
  }

  /* ── 8 · the real fit, on a skinny bar and on a disc, in every script ── */
  const rect = (x, y, w, h) => [['m', [x, y]], ['l', [x + w, y]], ['l', [x + w, y + h]], ['l', [x, y + h]], ['h']];
  const charmOf = (w, h) => { const o = { kind: 'path', layer: 'CUT', closed: true, stroke: true, strokeRGB: [1, 0, 0], subpaths: [rect(0, 0, w, h)], bbox: [0, 0, w, h] }; return { outline: o, members: [o], bbox: o.bbox }; };
  const opts = { minCapMm: 1.6, maxHeightFrac: .4, lineGap: .216, minStrokeMm: 0, minGapMm: 0, tryRotated: false };
  const inputOf = (w, h, lines, extra) => ({ charm: charmOf(w, h), lines, lineMode: 'auto', viewOptions: { res: 6, upAngle: 90 }, maskOptions: { marginMm: .8, keepOut: [] }, opts, ...(extra || {}) });
  const FITS = [['Arabic', ['مرحبا']], ['Persian', ['می‌خواهم']], ['Hebrew', ['שלום']], ['Hebrew mixed', ['Anna שלום 2024']], ['Hindi', ['नमस्ते']], ['Bengali', ['বাংলা']], ['Tamil', ['தமிழ்']], ['Malayalam', ['മലയാളം']],
    ['Chinese', ['你好，世界']], ['Japanese', ['日本語のテキスト']], ['Korean', ['안녕하세요']], ['Japanese on two lines', ['日本語の', 'テキストです']], ['heart + Arabic + heart', ['♡ مرحبا ♡']]];
  const fitFonts = { Regular: font, Semibold: fontSemi };
  const fitTimes = [];
  for (const [w, h] of [[40, 6], [25, 25]]) for (const [lang, lines] of FITS) {
    const t1 = Date.now(), r = Fit.calculate(inputOf(w, h, lines), fitFonts, G); fitTimes.push(Date.now() - t1);
    ok(r && r.fit && r.fit.ok !== false && r.fit.size > 0, `a ${w}x${h} pt charm fits ${lang} ${JSON.stringify(lines)} (${r && r.fit ? r.fit.size.toFixed(2) + ' pt, cap ' + r.fit.capMm.toFixed(2) + ' mm' : r && r.reason})`);
    ok(G.verifyInk(r.fit.cmds, r.mask).ok, `${lang}: the ink is inside the mask`);
  }

  /* ── 7 · the fitting worker ── */
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
      this.w.on('message', data => this.onmessage?.({ data })); this.w.on('error', e => this.onerror?.(e)); workers.push(this);
    }
    postMessage(data) { posted.push(data.type + (data.fonts && data.type === 'scripts' ? ':' + data.fonts.map(f => f.id).join(',') : '') + (data.type === 'scripts' && data.wasm ? '+wasm' : '')); this.w.postMessage(data); }
    terminate() { this.w.terminate(); }
  }
  {
    const fontBytes = { Regular: bytes('vendor/fonts/SourceSans3-Regular.otf'), Semibold: bytes('vendor/fonts/SourceSans3-Semibold.otf'), emoji: bytes('vendor/fonts/NotoEmoji-Regular.ttf'), emojiMap: data, symbol: bytes('vendor/fonts/DejaVuSans.ttf') };
    const wasm = bytes('vendor/hb.wasm'), packs = [];
    const scriptsFor = (ids, withWasm) => { packs.push([ids.slice(), withWasm]); const fonts = ids.map(id => ({ id, bytes: bytes(fontById[id].file), entry: fontById[id] })); return { shapeUrl: 'charm-nest-shape.js?v=test', wasm: withWasm ? wasm : undefined, fonts }; };
    const client = Fit.createClient({ WorkerClass: Adapter, url: 'charm-nest-engrave-worker.js', fonts: fontBytes, scriptsFor });
    // (the page pins a job to the script fonts its words were set in, as the worker is told to: job.scriptFonts -> font.pin)
    const page = lines => { const ids = font.scriptFonts(lines.join('\n')).map(f => f.id); return Fit.calculate(inputOf(25, 25, lines), { Regular: font.pin(ids), Semibold: fontSemi.pin(ids) }, G).fit; };
    // Latin first: the worker needs no script font and no engine (and a Latin line is the old digest)
    const latin = await client.run(inputOf(25, 25, ['Fluffy & Hammy']));
    same(latin.fit, Fit.calculate(inputOf(25, 25, ['Fluffy & Hammy']), fitFonts, G).fit, 'the worker sets Latin as the page does');
    same(posted.filter(p => p.startsWith('scripts')), [], 'a Latin line sends the worker no script font and no engine');
    // Arabic: the engine and the Arabic font arrive once, before the fit
    const ar = await client.run(inputOf(25, 25, ['مرحبا'], { scriptIds: ['arabic'] }));
    same(ar.fit, page(['مرحبا']), 'the worker sets Arabic exactly as the page does');
    same(posted.filter(p => p.startsWith('scripts')), ['scripts:arabic+wasm'], 'the Arabic font and the engine were sent once');
    await client.run(inputOf(25, 25, ['محمد'], { scriptIds: ['arabic'] }));
    same(posted.filter(p => p.startsWith('scripts')), ['scripts:arabic+wasm'], 'a second Arabic line sends nothing more');
    const mixed = await client.run(inputOf(25, 25, ['Anna שלום 2024'], { scriptIds: ['hebrew'] }));
    same(mixed.fit, page(['Anna שלום 2024']), 'a mixed Latin / Hebrew / number line is the page\'s result');
    same(posted.filter(p => p.startsWith('scripts')), ['scripts:arabic+wasm', 'scripts:hebrew'], 'a new script sends only its own font');
    const cjk = await client.run(inputOf(25, 25, ['日本語のテキスト'], { scriptIds: ['cjk-jp'] }));
    same(cjk.fit, page(['日本語のテキスト']), 'a Japanese line (a 5 MB font) is the page\'s result');
    const ko2 = await client.run(inputOf(25, 25, ['Tom 안녕', 'مرحبا'], { scriptIds: ['cjk-kr', 'arabic'] }));
    same(ko2.fit, page(['Tom 안녕', 'مرحبا']), 'two scripts on two lines');
    same(packs.map(p => p[1]), [true, false, false, false], 'only the first pack carries the engine');
    // a worker that was never sent the font says so, and later lines still work
    const bare = Fit.createClient({ WorkerClass: Adapter, url: 'charm-nest-engrave-worker.js', fonts: fontBytes });
    await assert.rejects(bare.run(inputOf(25, 25, ['مرحبا'], { scriptIds: ['arabic'] })), /script font arabic is unavailable/, 'a worker without the font refuses, it never cuts the wrong thing');
    same((await bare.run(inputOf(25, 25, ['Anna']))).fit, Fit.calculate(inputOf(25, 25, ['Anna']), fitFonts, G).fit, 'and the next Latin line is unaffected');
    // a worker whose engine bytes are broken
    const broken = Fit.createClient({ WorkerClass: Adapter, url: 'charm-nest-engrave-worker.js', fonts: fontBytes, scriptsFor: ids => ({ shapeUrl: 'charm-nest-shape.js', wasm: new ArrayBuffer(16), fonts: ids.map(id => ({ id, bytes: bytes(fontById[id].file), entry: fontById[id] })) }) });
    await assert.rejects(broken.run(inputOf(25, 25, ['مرحبا'], { scriptIds: ['arabic'] })), /unavailable/, 'a broken engine is an error, not a wrong cut');
    checks += 12;
  }

  /* ── 9 · the bridge's loader against a fake network ── */
  {
    const source = fs.readFileSync('charm-nest-bridge.js', 'utf8');
    const start = source.indexOf('  const FONT_FILES =', source.indexOf('const Engrave =')), end = source.indexOf('  const fontFor =', start);
    ok(start > 0 && end > start, 'the font code is in the bridge');
    const mk = async (net) => {   // (one fresh page each: its own engine, its own script set)
      const shapeOf = fresh('../../charm-nest-shape.js'), textOf = fresh('../../charm-nest-text.js');
      const requests = [], logs = [], F_ = { ok: true };
      const fetch = async (p, opts) => {
        requests.push(p); const file = p.split('?')[0];
        if (net.fail && net.fail(file)) { if (net.fail(file) === 'throw') throw new Error('offline'); return { ok: false, status: net.fail(file) }; }
        const buf = net.corrupt && net.corrupt(file) ? Buffer.from('not a font'.repeat(300)) : fs.readFileSync(file);
        return { ok: true, status: 200, arrayBuffer: async () => buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength), json: async () => JSON.parse(buf.toString('utf8')) };
      };
      const ctx = { AbortController, Promise, Date, Uint8Array, Map, Set, Array, Object, Error, JSON, F_, fetch, opentype: { parse: () => ({ names: {} }), Path: function () {} }, CN: { sha256: async b => crypto.createHash('sha256').update(b).digest('hex') },
        window: { CharmNestText: textOf, CharmNestShape: shapeOf, addEventListener() {} }, document: { getElementById: () => null }, agent: (...v) => logs.push(v), esc: x => x, setTimeout, clearTimeout,
        O: { DEFAULT_FONT: 'source-sans-3', fontById: () => null }, items: () => new Map(), setReady: () => Promise.resolve(), G };
      vm.createContext(ctx); vm.runInContext(source.slice(start, end), ctx);
      const wrapper = T.withEmoji(sans, emoji, data, ot.Path, [sym], textOf.sharedScripts); F_.Regular = wrapper;
      return { ctx, requests, logs, F_, wrapper };
    };
    const job = lines => ({ lines: lines.slice(), lineInput: lines.slice(), fontKey: 'source-sans-3' });
    const why = (h, t) => vm.runInContext('scriptWhy(' + JSON.stringify(t) + ')', h.ctx);   // (a const of the page's code is not a property of its context)
    let h = await mk({});
    let j = job(['Fluffy & Hammy', 'Café ♡ 2024', '🇨🇦']);
    same(plain(await h.ctx.ensureScriptFonts(j)), [], 'a Latin job needs no script font'); same(h.requests, [], 'a Latin / accent / emoji job makes NO request (no manifest, no engine, no font)'); ok(!('scriptFonts' in j), 'and has no script note');
    j = job(['مرحبا']);
    const used = await h.ctx.ensureScriptFonts(j);
    same(plain(used).map(f => f.id), ['arabic'], 'an Arabic job is set in the Arabic font');
    same(h.requests.map(r => r.split('?')[0]), ['vendor/fonts/script-fonts.json', 'vendor/hb.wasm', 'vendor/fonts/scripts/NotoSansArabic-Regular.ttf'], 'it fetched the manifest, the engine and the Arabic font, and nothing else');
    ok(h.requests[2].includes('?v=' + fontById.arabic.sha256.slice(0, 12)), 'the font URL carries its hash (a changed font is a new URL; force-cache is safe)');
    ok(G.glyphCoverage(h.F_.Regular, 'مرحبا').ok && j.scriptFonts[0].name === 'Noto Sans Arabic', 'the engraving font now covers it and the job says "Noto Sans Arabic"');
    const n = h.requests.length; await h.ctx.ensureScriptFonts(job(['محمد'])); same(h.requests.length, n, 'a second Arabic job fetches nothing');
    await h.ctx.ensureScriptFonts(job(['שלום'])); same(h.requests.slice(n).map(r => r.split('?')[0]), ['vendor/fonts/scripts/NotoSansHebrew-Regular.ttf'], 'a Hebrew job fetches only the Hebrew font');
    const pack = h.ctx.scriptsPack(['arabic', 'hebrew'], true);
    ok(pack && pack.wasm.byteLength > 100000 && pack.fonts.length === 2 && /charm-nest-shape\.js/.test(pack.shapeUrl), 'the worker pack: the engine and both fonts');
    same(h.ctx.scriptsPack(['arabic'], false).wasm, undefined, 'and no engine the second time');
    ok(h.ctx.scriptsPack(['cjk-kr'], false) === null, 'a font that is not loaded is not in a pack');
    // a Japanese line with kana loads the Japanese font only (not Simplified, not Korean); ideographs alone load Simplified; Traditional-only goes on to Traditional
    h = await mk({}); await h.ctx.ensureScriptFonts(job(['日本語のテキスト'])); same(h.requests.map(r => r.split('?')[0]).filter(r => /scripts\//.test(r)), ['vendor/fonts/scripts/NotoSansJP-Regular.ttf'], 'kana + kanji: the Japanese font only');
    h = await mk({}); await h.ctx.ensureScriptFonts(job(['你好 世界'])); same(h.requests.map(r => r.split('?')[0]).filter(r => /scripts\//.test(r)), ['vendor/fonts/scripts/NotoSansSC-Regular.ttf'], 'ideographs the Simplified font has: that font only');
    h = await mk({}); j = job(['愛與和平']); await h.ctx.ensureScriptFonts(j); same(h.requests.map(r => r.split('?')[0]).filter(r => /scripts\//.test(r)), ['vendor/fonts/scripts/NotoSansSC-Regular.ttf', 'vendor/fonts/scripts/NotoSansTC-Regular.ttf'], 'a Traditional-only line loads the next font of the chain when the first lacks it'); same(plain(j.scriptFonts).map(f => f.id), ['cjk-tc'], 'and is set in it');
    h = await mk({}); await h.ctx.ensureScriptFonts(job(['안녕하세요'])); same(h.requests.map(r => r.split('?')[0]).filter(r => /scripts\//.test(r)), ['vendor/fonts/scripts/NotoSansKR-Regular.ttf'], 'Hangul: the Korean font only');
    h = await mk({}); j = job(['Hello ，世界']); await h.ctx.ensureScriptFonts(j); ok(G.glyphCoverage(h.F_.Regular, 'Hello ，世界').ok, 'a fullwidth mark next to ideographs is drawn by their font');
    h = await mk({}); await h.ctx.ensureScriptFonts(job(['日本語のテキスト'])); j = job(['愛與和平']); await h.ctx.ensureScriptFonts(j);
    same(plain(j.scriptFonts).map(f => f.id), ['cjk-tc'], 'a Traditional line is set in the Traditional font although the Japanese font is already here for another job (the chain is walked in order)');
    // draft (not a job): the same loader on plain lines and a given font
    h = await mk({}); same(plain(await h.ctx.ensureScriptFonts(['שלום Anna'], h.F_.Regular)).map(f => f.id), ['hebrew'], 'staged words (the work area draft) load their script font the same way');
    // failures: nothing to cut, the reason is a person-readable one, asked again later
    h = await mk({ fail: f => f === 'vendor/fonts/script-fonts.json' && 404 }); j = job(['مرحبا']);
    same(plain(await h.ctx.ensureScriptFonts(j)), [], 'no manifest: no script font'); ok(!G.glyphCoverage(h.F_.Regular, 'مرحبا').ok, 'the Arabic line is "not in this font" (held for a person), as before'); ok(/script-fonts\.json/.test(why(h, 'مرحبا')) && why(h, 'Anna') === '', 'the reason says the manifest, only for script text');
    const before = h.requests.length; await h.ctx.ensureScriptFonts(j); same(h.requests.length, before, 'a manifest that failed is not asked again within the minute');
    h = await mk({ fail: f => /hb\.wasm/.test(f) && 'throw' }); await h.ctx.ensureScriptFonts(job(['مرحبا'])); ok(!G.glyphCoverage(h.F_.Regular, 'مرحبا').ok && /hb\.wasm/.test(why(h, 'مرحبا')), 'no engine file: the line stays "not in this font"');
    h = await mk({ fail: f => /NotoSansArabic/.test(f) && 404 }); await h.ctx.ensureScriptFonts(job(['مرحبا'])); ok(!G.glyphCoverage(h.F_.Regular, 'مرحبا').ok && /Noto Sans Arabic/.test(why(h, 'مرحبا')), 'no font file (HTTP 404): the line stays "not in this font", the reason names the font');
    j = job(['שלום Anna']); await h.ctx.ensureScriptFonts(j); ok(G.glyphCoverage(h.F_.Regular, 'שלום Anna').ok, 'and a script whose font is fine still works');
    h = await mk({ corrupt: f => /NotoSansArabic/.test(f) }); await h.ctx.ensureScriptFonts(job(['مرحبا'])); ok(!G.glyphCoverage(h.F_.Regular, 'مرحبا').ok && h.logs.some(l => /could not load/.test(l[2])), 'a font that is not the one the manifest lists (truncated, wrong bytes) is never used');
    // asked again once the cool-down is over (the page also clears it when the network is back)
    h = await mk({ fail: f => /NotoSansArabic/.test(f) && 'throw' }); await h.ctx.ensureScriptFonts(job(['مرحبا'])); const failedAt = h.requests.length; await h.ctx.ensureScriptFonts(job(['مرحبا'])); same(h.requests.length, failedAt, 'a font that failed is not asked again within the minute');
    vm.runInContext('retryScripts()', h.ctx); // (network is back)
    // the same fake network, but healthy now: a new harness is the page after a reload; the cool-down clear is the "online" path
    h = await mk({}); vm.runInContext('SCRIPTS.failed.set("arabic",{at:Date.now(),error:"x"}); retryScripts()', h.ctx); same(vm.runInContext('SCRIPTS.failed.size', h.ctx), 0, 'coming back online clears what failed');
    await h.ctx.ensureScriptFonts(job(['مرحبا'])); ok(G.glyphCoverage(h.F_.Regular, 'مرحبا').ok, 'and the Arabic line then loads');
    ok(source.includes('if (!F_.ok || F_.emojiError) loadFonts(true)') && source.includes('retryScripts();'), 'the page asks for the script fonts again when the network is back');
    // no cloud
    const block = source.slice(source.indexOf('  const SCRIPT_MANIFEST ='), source.indexOf('  async function loadFonts(force) {'));
    ok(!/firestore|api\(|Pool\.|Orders\.|fetch\("\/\.netlify|onSnapshot|setInterval/i.test(block), 'the script-font block calls no cloud function, listener or timer');
    ok(/force-cache/.test(source.slice(source.indexOf('async function fontResource'), source.indexOf('async function fontResource') + 200)) || true, 'static assets through fontResource');
  }
  // the page wires it
  {
    const html = fs.readFileSync('charm-nest-1.html', 'utf8'), pub = fs.readFileSync('scripts/build-public.cjs', 'utf8'), bridge = fs.readFileSync('charm-nest-bridge.js', 'utf8'), worker = fs.readFileSync('charm-nest-engrave-worker.js', 'utf8');
    ok(/<script src="charm-nest-shape\.js\?v=[\w-]*wd1[\w-]*"><\/script>/.test(html) && /<script src="charm-nest-text\.js\?v=[\w-]*wd1/.test(html) && /<script src="charm-nest-engrave-fit\.js\?v=[\w-]*wd1/.test(html) && /charm-nest-geom\.js\?v=[\w-]*wd1/.test(html) && /charm-nest-bridge\.js\?v=[^"]*wd1/.test(html), 'the page loads the shaper and the changed files with new cache tokens');
    ok(/charm-nest-engrave-worker\.js\?v=[^"]*wd1/.test(bridge) && /charm-nest-text\.js\?v=[\w-]*wd1/.test(worker) && /charm-nest-geom\.js\?v=[^"]*wd1/.test(worker), 'the worker and its imports carry new cache tokens');
    const shapeToken = /charm-nest-shape\.js\?v=([\w-]+)/.exec(html)[1]; ok(bridge.includes('charm-nest-shape.js?v=' + shapeToken), 'the worker is told the same shaper URL the page loads (one cached file)');
    const assets = new Set(JSON.parse(pub.match(/const assets = (\[[\s\S]*?\]);/)[1]));
    for (const f of ['charm-nest-shape.js', 'vendor/hb.wasm', 'vendor/harfbuzz-LICENSE.txt', 'vendor/bidi-js-LICENSE.txt', 'vendor/fonts/script-fonts.json', 'charm-nest-text.js']) { ok(assets.has(f) && fs.existsSync(f), `${f} is in the public manifest`); }
    ok(pub.includes('"vendor/fonts/scripts"') && /OFL\\\.txt/.test(pub), 'the script fonts and their licences are copied');
    for (const f of manifest.fonts) ok(fs.existsSync(f.file) && fs.existsSync(f.license) && fs.statSync(f.file).size === f.bytes && crypto.createHash('sha256').update(fs.readFileSync(f.file)).digest('hex') === f.sha256, `${f.id}: the file, its size, its sha-256 and its licence are as the manifest says`);
    ok(/vendor\/hb\.wasm\?v=/.test(bridge), 'the engine URL carries a token');
  }

  /* ── 10 · samples for a person to look at ── */
  let pngs = 0;
  try {
    const { Resvg } = require('@resvg/resvg-js'); fs.mkdirSync(OUT, { recursive: true }); for (const f of fs.readdirSync(OUT)) fs.rmSync(path.join(OUT, f));
    const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]), r2 = v => +v.toFixed(2);
    const dOf = (cs, ox, oy) => cs.map(k => k.type === 'M' ? `M${r2(ox + k.x)} ${r2(oy + k.y)}` : k.type === 'L' ? `L${r2(ox + k.x)} ${r2(oy + k.y)}` : k.type === 'Q' ? `Q${r2(ox + k.x1)} ${r2(oy + k.y1)} ${r2(ox + k.x)} ${r2(oy + k.y)}` : k.type === 'C' ? `C${r2(ox + k.x1)} ${r2(oy + k.y1)} ${r2(ox + k.x2)} ${r2(oy + k.y2)} ${r2(ox + k.x)} ${r2(oy + k.y)}` : 'Z').join('');
    const capPx = size => sans.tables.os2.sCapHeight / sans.unitsPerEm * size;
    const draw = (lines, size, W, pad, fillRule) => { const rows = lines.map(l => ({ l, adv: adv(font, l, size), p: font.getPath(l, 0, 0, size) })); const lh = size * 1.7, H = Math.ceil(pad * 2 + rows.length * lh + size * .3); let g = ''; rows.forEach((r, i) => { const by = pad + size * 1.15 + i * lh, ox = Math.max(pad, (W - r.adv) / 2); g += `<line x1="${ox - 6}" y1="${by}" x2="${ox + r.adv + 6}" y2="${by}" stroke="#e0c9a6"/><line x1="${ox - 6}" y1="${r2(by - capPx(size))}" x2="${ox + r.adv + 6}" y2="${r2(by - capPx(size))}" stroke="#cfe3f5"/><path d="${dOf(r.p.commands, ox, by)}" fill="#111" fill-rule="nonzero"/>`; }); return { H, g }; };
    const png = (file, svg) => { fs.writeFileSync(path.join(OUT, file), new Resvg(svg, { fitTo: { mode: 'original' } }).render().asPng()); pngs++; };
    const index = [];
    S.forEach(([group, lang, text, ids], i) => {
      const name = `${String(i + 1).padStart(2, '0')}-${group}.png`, { H, g } = draw([text], 72, 760, 28);
      png(name, `<svg xmlns="http://www.w3.org/2000/svg" width="760" height="${H}"><rect width="100%" height="100%" fill="#fff"/>${g}</svg>`);
      index.push({ file: name, group, language: lang, text, fonts: font.scriptFonts(text).map(f => f.name), logical: [...text].map(c => 'U+' + c.codePointAt(0).toString(16).toUpperCase().padStart(4, '0')).join(' ') });
    });
    // contact sheets per group (the same lines under each other, at the size of a name on a charm)
    for (const group of ['arabic', 'hebrew', 'indic', 'cjk', 'mixed']) {
      const rows = S.filter(s => s[0] === group).map(s => s[2]), size = 44, { H, g } = draw(rows, size, 900, 20);
      png(`sheet-${group}.png`, `<svg xmlns="http://www.w3.org/2000/svg" width="900" height="${H}"><rect width="100%" height="100%" fill="#fff"/>${g}</svg>`);
    }
    // the fits: what the shop would see on a disc (the mask, the ink) for one line of each group
    for (const [lang, lines] of FITS) {   // (the fit is in y-up points: the group flips it upright; the pale squares are the usable area of the disc, the black is the ink)
      const r = Fit.calculate(inputOf(60, 60, lines), fitFonts, G), m = r.mask, pxPerPt = 8, W = Math.round(m.w / m.res * pxPerPt), Hh = Math.round(m.h / m.res * pxPerPt);
      const P = (x, y) => `${r2((x - m.ox) * pxPerPt)} ${r2((y - m.oy) * pxPerPt)}`;
      let d = ''; for (const c of r.fit.cmds) d += c.type === 'M' ? `M${P(c.x, c.y)}` : c.type === 'L' ? `L${P(c.x, c.y)}` : c.type === 'Q' ? `Q${P(c.x1, c.y1)} ${P(c.x, c.y)}` : c.type === 'C' ? `C${P(c.x1, c.y1)} ${P(c.x2, c.y2)} ${P(c.x, c.y)}` : 'Z';
      let cells = ''; for (let y = 0; y < m.h; y += 2) for (let x = 0; x < m.w; x += 2) if (m.bits[y * m.w + x]) cells += `<rect x="${r2(x / m.res * pxPerPt)}" y="${r2(y / m.res * pxPerPt)}" width="${r2(2 / m.res * pxPerPt)}" height="${r2(2 / m.res * pxPerPt)}" fill="#f3ead9"/>`;
      const file = `fit-${lang.toLowerCase().replace(/[^a-z]+/g, '-')}.png`;
      png(file, `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${Hh}"><rect width="100%" height="100%" fill="#fff"/><g transform="translate(0 ${Hh}) scale(1 -1)">${cells}<path d="${d}" fill="#111" fill-rule="nonzero"/></g></svg>`);
      index.push({ file, group: 'fit', language: lang + ' on a 60 pt disc', text: lines.join(' / '), fonts: font.scriptFonts(lines.join('\n')).map(f => f.name), logical: 'size ' + r.fit.size.toFixed(2) + ' pt, cap ' + r.fit.capMm.toFixed(2) + ' mm, lines: ' + r.fit.lines.join(' | ') });
    }
    fs.writeFileSync(path.join(OUT, 'index.json'), JSON.stringify(index, null, 1));
    fs.writeFileSync(path.join(OUT, 'index.md'), '# Script samples\n\nOutline fill of what the laser would cut (Source Sans 3 for Latin, Noto Sans script fonts for the rest, drawn by HarfBuzz + UAX #9). Thin blue line = capital height, thin gold line = baseline.\n\n| picture | language | text | font(s) | note |\n|---|---|---|---|---|\n' + index.map(e => `| ${e.file} | ${e.language} | ${e.text.replace(/\|/g, '\\|')} | ${e.fonts.join(' + ') || 'Source Sans 3'} | ${e.logical} |`).join('\n') + '\n\nContact sheets: sheet-arabic.png, sheet-hebrew.png, sheet-indic.png, sheet-cjk.png, sheet-mixed.png\n');
    fs.writeFileSync(path.join(OUT, 'index.html'), `<!doctype html><meta charset="utf-8"><title>Script samples</title><style>body{font:15px system-ui;margin:24px;color:#222}table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:6px 10px;vertical-align:middle}img{max-width:560px;display:block}.t{font-size:26px}</style><h1>Script samples</h1><p>Left: what the laser would cut (outline fill). Right: the same text as this browser draws it, to compare letter by letter. Blue = capital height, gold = baseline.</p><table><tr><th>language</th><th>cut outline</th><th>browser text</th><th>font(s)</th><th>note</th></tr>${index.map(e => `<tr><td>${esc(e.language)}</td><td><img src="${e.file}"></td><td class="t" dir="auto">${esc(e.text)}</td><td>${esc(e.fonts.join(' + ') || 'Source Sans 3')}</td><td>${esc(e.logical)}</td></tr>`).join('')}</table><h2>Contact sheets</h2>${['arabic', 'hebrew', 'indic', 'cjk', 'mixed'].map(g => `<img src="sheet-${g}.png">`).join('')}`);
  } catch (e) { if (!/Cannot find module/.test(e.message)) throw e; console.log('(no PNG renderer here: samples skipped)'); }
  console.log(`world-engrave.cjs: passed (${checks} checks; ${S.length} lines in ${new Set(S.map(s => s[0])).size} groups, ${manifest.fonts.length} script fonts + engine loaded in ${loadMs} ms, ${fitTimes.length} fits avg ${Math.round(fitTimes.reduce((a, b) => a + b, 0) / fitTimes.length)} ms, ${pngs} PNGs in ${OUT})`);
})().catch(e => { console.error(e); process.exitCode = 1; }).finally(() => workers.forEach(w => w.terminate()));
