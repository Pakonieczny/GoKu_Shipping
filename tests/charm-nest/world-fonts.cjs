/* WORLD-FONTS (Paul, 19 Oct 2026: "add the compatibility with the Arabic, Israeli, Chinese, Japanese, Korean, Indian letters, characters, words/symbols depending on the language").
 * Offline, node only: the script fonts the engraving shaper draws with (vendor/fonts/scripts + vendor/fonts/script-fonts.json), checked as shipped.
 *  1  the manifest is well formed (ids, scripts, languages, ranges, chain, direction) and lists every font file and every licence that is in the folder, and nothing else
 *  2  each file is the one the manifest names: size and sha256 match, the SIL OFL text sits beside it, the CJK faces stay within the size budget (1.5 MB each, 6 MB together)
 *  3  opentype.js 1.3.4 parses every file (the engraving page and worker read the cmap with it); static TrueType, weight 400, family name as listed
 *  4  the layout tables a script needs are inside (GSUB/GPOS with joining, conjunct, reordering and mark features), so HarfBuzz can shape with the font's own rules
 *  5  a real sentence of every language is 100 % covered by its font's cmap and lies inside the ranges the manifest assigns to that font
 *  6  the CJK subsets (common characters only) cover their source code tables as the manifest claims, measured here with Node's own decoders; the coverage table is printed
 */
const assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto');
const ot = require('../../vendor/opentype-1.3.4.min.js');
const ROOT = path.resolve(__dirname, '..', '..');
const manifest = JSON.parse(fs.readFileSync(path.join(ROOT, 'vendor/fonts/script-fonts.json'), 'utf8'));
let checks = 0; const ok = (c, m) => { assert(c, m); checks++; };
const same = (a, b, m) => { assert.deepEqual(a, b, m); checks++; };
const hex = cp => 'U+' + cp.toString(16).toUpperCase().padStart(4, '0');
const CJK_EACH = 1500000, CJK_TOTAL = 6000000;   // (Paul, 20 Oct 2026: the font weight is far too big: 1.5 MB per CJK face, 6 MB together)

// 1 · the manifest
same(manifest.version, 1, 'manifest version 1');
const fonts = manifest.fonts, byId = new Map(fonts.map(f => [f.id, f]));
same(byId.size, fonts.length, 'font ids are unique');
const WANT = ['arabic', 'hebrew', 'devanagari', 'bengali', 'gurmukhi', 'gujarati', 'tamil', 'telugu', 'kannada', 'malayalam', 'cjk-sc', 'cjk-tc', 'cjk-jp', 'cjk-kr'];
for (const id of WANT) ok(byId.has(id), `the manifest lists ${id}`);
for (const f of fonts) {
  const w = `${f.id}: `;
  ok(/^[a-z][a-z-]*$/.test(f.id), w + 'id is a lowercase slug');
  ok(typeof f.name === 'string' && f.name.startsWith('Noto Sans'), w + 'a Noto Sans family name');
  ok(Array.isArray(f.scripts) && f.scripts.length && f.scripts.every(s => /^[A-Z][a-z]{3}$/.test(s)), w + 'scripts are ISO 15924 codes');
  ok(Array.isArray(f.langs) && f.langs.length && f.langs.every(l => /^[a-z]{2,3}(-[A-Za-z]{2,4})?$/.test(l)), w + 'langs are language tags');
  ok(typeof f.rtl === 'boolean', w + 'rtl is a boolean');
  same(f.rtl, f.scripts.includes('Arab') || f.scripts.includes('Hebr'), w + 'rtl is true exactly for Arabic and Hebrew script fonts');
  ok(typeof f.chain === 'string' && f.chain, w + 'has a chain');
  ok(Array.isArray(f.ranges) && f.ranges.length > 0, w + 'has ranges');
  let prev = -1;
  for (const r of f.ranges) { ok(Array.isArray(r) && r.length === 2 && Number.isInteger(r[0]) && Number.isInteger(r[1]) && r[0] <= r[1] && r[1] <= 0x10FFFF, w + `range ${JSON.stringify(r)} is [first, last]`); ok(r[0] > prev, w + 'ranges are sorted and do not overlap'); prev = r[1]; }
  ok(typeof f.coverage === 'string' && f.coverage.length > 20, w + 'carries a coverage note');
  ok(f.source && /^[0-9a-f]{40}$/.test(f.source.commit) && /^[0-9a-f]{64}$/.test(f.source.sha256) && f.source.path, w + 'records where the font came from');
  ok(f.file === `vendor/fonts/scripts/${path.basename(f.file)}` && f.license === `vendor/fonts/scripts/${path.basename(f.license)}`, w + 'file and licence live in vendor/fonts/scripts');
}
// the CJK faces form one chain; every other family is its own
same(fonts.filter(f => f.chain === 'cjk').map(f => f.id), ['cjk-sc', 'cjk-tc', 'cjk-jp', 'cjk-kr'], 'the cjk chain is SC, TC, JP, KR in fallback order');
for (const f of fonts.filter(f => f.chain !== 'cjk')) same(fonts.filter(g => g.chain === f.chain).length, 1, f.id + ' is alone in its chain');
// outside the chain, no two faces claim the same code point (the range alone picks the font)
{
  const solo = fonts.filter(f => f.chain !== 'cjk');
  for (let i = 0; i < solo.length; i++) for (let j = i + 1; j < solo.length; j++) for (const a of solo[i].ranges) for (const b of solo[j].ranges)
    ok(a[1] < b[0] || b[1] < a[0], `${solo[i].id} and ${solo[j].id} do not share ${JSON.stringify(a)} / ${JSON.stringify(b)}`);
}
ok(fonts.filter(f => f.chain === 'cjk').reduce((n, f) => n + f.bytes, 0) <= CJK_TOTAL, 'the four CJK faces together stay within 6 MB');
// nothing in the folder is unlisted
{
  const dir = path.join(ROOT, 'vendor/fonts/scripts'), listed = new Set(fonts.flatMap(f => [path.basename(f.file), path.basename(f.license)]));
  for (const n of fs.readdirSync(dir)) ok(listed.has(n), `${n} is in the manifest`);
  same(fs.readdirSync(dir).length, listed.size, 'the folder holds exactly the listed fonts and licences');
}

// 2 · the files, 3 · parsed by opentype.js
const parsed = new Map();
for (const f of fonts) {
  const w = `${f.id}: `, file = path.join(ROOT, f.file), buf = fs.readFileSync(file);
  same(buf.length, f.bytes, w + 'size matches the manifest');
  same(crypto.createHash('sha256').update(buf).digest('hex'), f.sha256, w + 'sha256 matches the manifest');
  if (f.chain === 'cjk') ok(f.bytes <= CJK_EACH, w + `stays within 1.5 MB (${(f.bytes / 1e6).toFixed(2)} MB)`);
  else ok(f.bytes < 300 * 1024, w + `a non-CJK face is under 300 KB (${(f.bytes / 1024).toFixed(0)} KB)`);
  const lic = fs.readFileSync(path.join(ROOT, f.license), 'utf8');
  ok(/SIL OPEN FONT LICENSE Version 1\.1/i.test(lic) && /Copyright/.test(lic) && /PERMISSION & CONDITIONS/i.test(lic), w + 'the OFL 1.1 text with its copyright line sits beside the font');
  const t0 = Date.now(), font = ot.parse(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength)); const ms = Date.now() - t0;
  parsed.set(f.id, font);
  ok(font.numGlyphs === f.glyphs && font.numGlyphs > 400, w + `opentype.js parses it (${font.numGlyphs} glyphs in ${ms} ms)`);
  same(font.unitsPerEm, 1000, w + '1000 units per em');
  same(font.outlinesFormat, 'truetype', w + 'TrueType outlines');
  ok(!font.tables.fvar && !font.tables.gvar, w + 'a static font (the variable axes are pinned)');
  same(font.tables.os2.usWeightClass, 400, w + 'Regular weight');
  same(font.names.fontFamily.en, f.name, w + 'family name is the listed one');
  ok(font.tables.cmap && font.glyphs.length === font.numGlyphs, w + 'cmap and glyphs are read');
  for (let cp = 0x20; cp < 0x7F; cp++) ok(font.charToGlyphIndex(String.fromCodePoint(cp)) > 0, w + `Latin ${hex(cp)} is in the face (digits and punctuation match the line around it)`);
}

// 4 · layout tables: the features each script needs are in the file
const NEED = {
  arabic:     { gsub: ['ccmp', 'locl', 'init', 'medi', 'fina', 'rlig'], gpos: ['kern', 'mark', 'mkmk'], scripts: ['arab'] },
  hebrew:     { gsub: ['ccmp', 'locl'], gpos: ['kern', 'mark', 'mkmk'], scripts: ['hebr'] },
  devanagari: { gsub: ['ccmp', 'nukt', 'akhn', 'rphf', 'blwf', 'half', 'cjct', 'pres', 'abvs', 'blws', 'psts'], gpos: ['kern', 'dist', 'abvm', 'blwm', 'mark', 'mkmk'], scripts: ['dev2', 'deva'] },
  bengali:    { gsub: ['ccmp', 'nukt', 'akhn', 'rphf', 'blwf', 'half', 'pstf', 'vatu', 'cjct', 'pres', 'abvs', 'blws', 'psts'], gpos: ['kern', 'dist', 'abvm', 'blwm', 'mark', 'mkmk'], scripts: ['bng2'] },
  gurmukhi:   { gsub: ['ccmp', 'nukt', 'blwf', 'pstf', 'abvs', 'blws'], gpos: ['kern', 'dist', 'abvm', 'mark', 'mkmk'], scripts: ['gur2'] },
  gujarati:   { gsub: ['ccmp', 'nukt', 'akhn', 'rphf', 'blwf', 'half', 'cjct', 'haln', 'pres', 'abvs', 'blws', 'psts'], gpos: ['kern', 'dist', 'abvm', 'blwm', 'mark', 'mkmk'], scripts: ['gjr2'] },
  tamil:      { gsub: ['ccmp', 'akhn', 'haln', 'abvs', 'psts'], gpos: ['kern', 'dist', 'abvm', 'mark', 'mkmk'], scripts: ['tml2'] },
  telugu:     { gsub: ['ccmp', 'akhn', 'rphf', 'blwf', 'haln', 'abvs', 'blws', 'psts'], gpos: ['kern', 'dist', 'abvm', 'blwm', 'mark', 'mkmk'], scripts: ['tel2'] },
  kannada:    { gsub: ['ccmp', 'nukt', 'akhn', 'rphf', 'blwf', 'half', 'haln', 'abvs', 'blws', 'psts'], gpos: ['kern', 'dist', 'abvm', 'mark', 'mkmk'], scripts: ['knd2'] },
  malayalam:  { gsub: ['ccmp', 'akhn', 'blwf', 'half', 'haln', 'pref', 'pstf', 'psts', 'rlig'], gpos: ['kern', 'dist', 'abvm', 'blwm', 'mark', 'mkmk'], scripts: ['mlm2', 'mlym'] },
  oriya:      { gsub: ['ccmp', 'nukt', 'akhn', 'rphf', 'blwf', 'cjct', 'haln', 'pstf', 'pres', 'abvs', 'blws', 'psts'], gpos: ['kern', 'abvm', 'blwm', 'mark', 'mkmk'], scripts: ['ory2'] },
  sinhala:    { gsub: ['ccmp', 'akhn', 'rphf', 'vatu', 'pstf', 'pres', 'abvs', 'blws', 'psts'], gpos: ['kern', 'dist', 'abvm', 'mark'], scripts: ['sinh'] },
};
const tagsOf = list => new Set((list || []).map(x => x.tag));
for (const f of fonts) {
  const w = `${f.id}: `, t = parsed.get(f.id).tables;
  ok(t.gsub && t.gpos, w + 'GSUB and GPOS are present');
  const gs = tagsOf(t.gsub.features), gp = tagsOf(t.gpos.features), sc = tagsOf(t.gsub.scripts);
  ok(gs.size > 0 && gp.size > 0, w + `layout features survive (GSUB ${gs.size}, GPOS ${gp.size})`);
  ok(t.gdef, w + 'GDEF (glyph classes for mark handling) is present');
  const need = NEED[f.id];
  if (need) {
    for (const x of need.gsub) ok(gs.has(x), w + `GSUB feature '${x}' kept`);
    for (const x of need.gpos) ok(gp.has(x), w + `GPOS feature '${x}' kept`);
    ok(need.scripts.some(s => sc.has(s)), w + `GSUB script '${need.scripts.join("'/'")}' kept`);
  } else {
    ok(gs.has('locl') || gs.has('vert') || gs.has('ccmp') || gs.has('ljmo') || gs.has('liga') || gs.has('aalt'), w + 'CJK GSUB features kept: ' + [...gs].join(' '));
    ok(gp.has('kern') || gp.has('palt') || gp.has('halt') || gp.has('vkrn'), w + 'CJK GPOS features kept: ' + [...gp].join(' '));
  }
}

// 5 · a real sentence of every language is fully covered by its font, and inside the ranges that font is assigned
const inRanges = (f, cp) => f.ranges.some(r => cp >= r[0] && cp <= r[1]);
const COMMON = cp => /[\p{Script=Common}\p{Script=Inherited}]/u.test(String.fromCodePoint(cp));
const SAMPLES = [
  ['Arabic', 'arabic', ['مرحبا بالعالم', 'محمد', 'مُحَمَّد', 'لا', '١٢٣ ٤٥٦ ٧٨٩', 'كلمة، جملة؟']],
  ['Persian', 'arabic', ['سلام دنیا', 'پ چ ژ گ']],
  ['Urdu', 'arabic', ['السلام علیکم', 'اردو ٹ ڈ ڑ ں ھ ے']],
  ['Pashto', 'arabic', ['پښتو ټ ډ ړ ځ څ']],
  ['Hebrew', 'hebrew', ['שלום עולם', 'שָׁלוֹם', 'בְּרֵאשִׁית']],
  ['Hindi', 'devanagari', ['नमस्ते दुनिया', 'हिन्दी', 'क्षत्रिय', 'ज्ञान श्री कृष्ण ॐ १२३']],
  ['Bengali', 'bengali', ['বাংলা ভাষা', 'ক্ষত্রিয় র‍্যাব']],
  ['Punjabi', 'gurmukhi', ['ਪੰਜਾਬੀ', 'ਸਤਿ ਸ੍ਰੀ ਅਕਾਲ']],
  ['Gujarati', 'gujarati', ['ગુજરાતી', 'ક્ષત્રિય']],
  ['Tamil', 'tamil', ['தமிழ்', 'கொக்கோ ஸ்ரீ']],
  ['Telugu', 'telugu', ['తెలుగు', 'క్షత్రియ']],
  ['Kannada', 'kannada', ['ಕನ್ನಡ', 'ಕ್ಷತ್ರಿಯ']],
  ['Malayalam', 'malayalam', ['മലയാളം', 'ക്ഷത്രിയ']],
  ['Odia', 'oriya', ['ଓଡ଼ିଆ']],
  ['Sinhala', 'sinhala', ['සිංහල']],
  ['Chinese (simplified)', 'cjk-sc', ['你好，世界 爱', '中华人民共和国', '谢谢 欢迎光临 ￥１２３']],
  ['Chinese (traditional)', 'cjk-tc', ['你好，世界 愛', '臺灣 學校 謝謝', '嘅 啲 喺 冇']],
  ['Japanese', 'cjk-jp', ['こんにちは世界 日本語のテキスト カタカナ ツ', '高橋 山崎 辻 渡辺', 'ｱｲｳｴｵ']],
  ['Korean', 'cjk-kr', ['안녕하세요 세계 사랑', '漢字 大韓 山水', 'ㄱㄴㄷ']],
];
for (const [lang, id, lines] of SAMPLES) {
  const f = byId.get(id), font = parsed.get(id);
  ok(f, `${lang}: font ${id} exists`);
  for (const line of lines) for (const ch of line) {
    const cp = ch.codePointAt(0);
    ok(font.charToGlyphIndex(ch) > 0, `${lang} "${line}": ${hex(cp)} (${ch}) is in ${f.name}`);
    if (!/[ -~]/.test(ch) && !/\s/.test(ch)) ok(inRanges(f, cp) || COMMON(cp), `${lang} "${line}": ${hex(cp)} lies inside the ranges of ${id}`);
  }
}
// each declared range holds at least one covered character (a typo in a range would send text to a font that cannot draw it)
for (const f of fonts) for (const r of f.ranges) {
  let hit = false; const font = parsed.get(f.id);
  for (let cp = r[0]; cp <= r[1] && !hit; cp++) hit = font.charToGlyphIndex(String.fromCodePoint(cp)) > 0;
  ok(hit, `${f.id}: range ${hex(r[0])}-${hex(r[1])} holds covered characters`);
}
// the danda shared by every Indian script, and the joiners Indic and Arabic text depend on, exist in the faces that need them
for (const id of ['devanagari', 'bengali', 'gurmukhi', 'gujarati', 'tamil', 'telugu', 'kannada', 'malayalam', 'oriya', 'sinhala'])
  for (const cp of [0x0964, 0x0965, 0x200C, 0x200D, 0x25CC]) ok(parsed.get(id).charToGlyphIndex(String.fromCodePoint(cp)) > 0, `${id}: ${hex(cp)} is in the face`);
for (const id of ['arabic', 'hebrew']) for (const cp of [0x200C, 0x200D, 0x25CC]) ok(parsed.get(id).charToGlyphIndex(String.fromCodePoint(cp)) > 0, `${id}: ${hex(cp)} is in the face`);

// 6 · CJK subsets against their source tables (independent of the Python build: Node's WHATWG decoders)
{
  const table = (label, lead, trail) => {
    const d = new TextDecoder(label), out = new Set();
    for (const l of lead) for (const t of trail) {
      let x; try { x = d.decode(new Uint8Array([l, t])); } catch (e) { continue; }
      for (const ch of x) { const cp = ch.codePointAt(0); if (cp !== 0xFFFD && cp > 0x7F) out.add(cp); }
    }
    return out;
  };
  const R = (a, b) => Array.from({ length: b - a + 1 }, (_, i) => a + i);
  const isIdeo = cp => (cp >= 0x3400 && cp <= 0x9FFF) || (cp >= 0xF900 && cp <= 0xFAFF) || (cp >= 0xAC00 && cp <= 0xD7A3);
  const pct = (id, set, onlyIdeo) => { const font = parsed.get(id); let n = 0, m = 0; for (const cp of set) { if (onlyIdeo && !isIdeo(cp)) continue; m++; if (font.charToGlyphIndex(String.fromCodePoint(cp)) > 0) n++; } return { n, m, pct: 100 * n / m }; };
  const T = {
    gbL1: table('gbk', R(0xB0, 0xD7), R(0xA1, 0xFE)), gbSym: new Set([...table('gbk', [0xA1, 0xA3], R(0xA1, 0xFE))]),
    b5L1: table('big5', R(0xA4, 0xC5), [...R(0x40, 0x7E), ...R(0xA1, 0xFE)]),
    jisL1: table('euc-jp', R(0xB0, 0xCF), R(0xA1, 0xFE)), jisL2: table('euc-jp', R(0xD0, 0xF4), R(0xA1, 0xFE)), jisSym: table('euc-jp', R(0xA1, 0xA8), R(0xA1, 0xFE)),
    ksHangul: table('euc-kr', R(0xB0, 0xC8), R(0xA1, 0xFE)), ksHanja: table('euc-kr', R(0xCA, 0xFD), R(0xA1, 0xFE)),
  };
  const rows = [];
  const claim = (id, label, set, min, onlyIdeo, key) => {
    const c = pct(id, set, onlyIdeo); rows.push(`${id.padEnd(7)} ${label.padEnd(34)} ${String(c.n).padStart(5)}/${String(c.m).padEnd(5)} ${c.pct.toFixed(2).padStart(6)} %`);
    ok(c.m > 100 && c.pct >= min, `${id}: ${label} covered ${c.n}/${c.m} (${c.pct.toFixed(2)} %, at least ${min} %)`);
    if (key) { const st = byId.get(id).coverageStats[key]; ok(st, `${id}: the manifest records ${key}`); ok(Math.abs(st.ideographPct - c.pct) < 1.5 || !onlyIdeo, `${id}: the manifest's ${key} figure (${st.ideographPct} %) agrees with the measure here (${c.pct.toFixed(2)} %)`); }
    return c;
  };
  // the common core is fully there
  claim('cjk-sc', 'GB 2312 level-1 hanzi (3,755)', T.gbL1, 100, true, 'gb2312-l1');
  claim('cjk-sc', 'GB 2312 punctuation + fullwidth, rows 1,3', T.gbSym, 99, false);
  claim('cjk-tc', 'Big5 level-1 hanzi (5,401, frequent)', T.b5L1, 60, true, 'big5-l1');
  claim('cjk-jp', 'JIS X 0208 level-1 kanji (2,965)', T.jisL1, 100, true, 'jisx0208-l1');
  claim('cjk-jp', 'JIS X 0208 symbols, kana, rows 1-8', T.jisSym, 99, false);
  claim('cjk-jp', 'JIS X 0208 level-2 kanji (3,390)', T.jisL2, 5, true);
  claim('cjk-kr', 'KS X 1001 Hangul (2,350)', T.ksHangul, 100, true, 'ksx1001-hangul');
  claim('cjk-kr', 'KS X 1001 Hanja (4,888)', T.ksHanja, 30, true, 'ksx1001-hanja');
  // what the manifest claims is what is in the file
  for (const id of ['cjk-sc', 'cjk-tc', 'cjk-jp', 'cjk-kr']) {
    const f = byId.get(id), cs = f.coverageStats; ok(cs && Object.keys(cs).length >= 3, `${id}: coverageStats are recorded`);
    for (const [k, v] of Object.entries(cs)) ok(v.total > 0 && v.covered <= v.total && v.pct === +(100 * v.covered / v.total).toFixed(2), `${id}/${k}: ${v.covered}/${v.total} = ${v.pct} %`);
    const font = parsed.get(id); let ideo = 0; for (const [cp] of Object.entries(font.tables.cmap.glyphIndexMap)) if (isIdeo(+cp)) ideo++;
    ok(ideo > 2500 && ideo < 6000, `${id}: ${ideo} ideographs / Hangul syllables in the face (common characters only)`);
    rows.push(`${id.padEnd(7)} ${(f.bytes / 1e6).toFixed(2)} MB, ${f.glyphs} glyphs, ${ideo} ideographs/syllables`);
  }
  // the contract of "common characters only": a rarer character is not in the face, so the engraving line is held and a person decides (never a box)
  for (const [id, ch, what] of [['cjk-sc', '堃', 'a rare name character'], ['cjk-tc', '燚', 'a rare name character'], ['cjk-jp', '邊', 'a variant form of 辺'], ['cjk-kr', '뷁', 'a syllable outside KS X 1001']])
    same(parsed.get(id).charToGlyphIndex(ch), 0, `${id}: ${ch} (${what}) is not in the face`);
  console.log('  ' + rows.join('\n  '));
}
console.log(`world-fonts.cjs: passed (${checks} checks)`);
