#!/usr/bin/env node
'use strict';
/*
 * (WORLD, Paul 19 Oct 2026: the engraving fonts for Arabic, Hebrew, the Indian scripts and Chinese / Japanese / Korean.)
 *
 * Rebuilds vendor/fonts/scripts/** (the font binaries and their OFL licences) and vendor/fonts/script-fonts.json (the manifest the
 * page reads: which font serves which Unicode ranges / scripts / languages) from their upstream sources. Nothing in
 * vendor/fonts/scripts is edited by hand: this file is the provenance.
 *
 *   node scripts/build-script-fonts.cjs                  rebuild everything (about 10-20 minutes, the four CJK faces take most of it)
 *   node scripts/build-script-fonts.cjs --only arabic,hebrew,devanagari      rebuild some entries, keep the others in the manifest
 *   node scripts/build-script-fonts.cjs --check          rebuild into the work dir only and compare the sha256 with the manifest
 *   node scripts/build-script-fonts.cjs --verify-only    no rebuild: shape the COMMITTED files against the upstream variable fonts with HarfBuzz
 *   node scripts/build-script-fonts.cjs --format         only re-write vendor/fonts/script-fonts.json in its canonical layout
 *   options: --work <dir> (default <tmp>/script-fonts-build: venv, downloads and intermediate files, reused between runs)
 *            --no-verify (skip the HarfBuzz comparison)   --help
 *
 * NEEDS: network (raw.githubusercontent.com, registry.npmjs.org, pypi.org), python3 with venv + pip, curl, tar. The run itself creates
 * a venv in the work dir and installs the pinned fonttools / brotli / uharfbuzz there; the repo's node_modules is not involved.
 *
 * SOURCES (fonts: SIL Open Font License 1.1; the licence text of each family is copied from the same commit as the font):
 *   fonts     github.com/google/fonts, commit GF_COMMIT, ofl/<family>/<File>[wdth,wght].ttf (variable TrueType, the Noto Sans families
 *             built by the Noto project; the four CJK ones are the Google Fonts builds of Source Han Sans: [wght] only)
 *   lists     small published character lists, used ONLY to choose which CJK glyphs to keep (never copied into the repo):
 *             npm togscc@1.3.1 (MIT)      the 8,105 characters of the 2013 Chinese 通用规范汉字表; its first 3,500 are the level 1
 *             npm joyo-kanji@0.2.1 (MIT)  the 2,136 常用漢字 (2010)
 *             npm jinmeiyo-kanji@1.0.0 (MIT) the 863 人名用漢字 (names)
 *             pip opencc-python-reimplemented==0.1.7 (Apache-2.0, OpenCC dictionaries): traditional -> simplified map, to rank Big5 level 1
 *             by the commonness of the simplified form (the commonest 3,500 simplified characters)
 *
 * PIPELINE per font (fontTools, see the Python helper below): pin the variable axes to wght=400 / wdth=100 with
 * fontTools.varLib.instancer (names refreshed from STAT: "Noto Sans Arabic Regular"), then subset with the equivalent of
 *   pyftsubset --layout-features='*' --layout-scripts='*' --glyph-names --notdef-outline --name-IDs='*' --name-legacy
 *              --name-languages='*' --legacy-cmap --symbol-cmap          (hinting kept, head.modified untouched, so the output is
 *                                                                          byte-for-byte reproducible with the pinned fontTools)
 * so every GSUB / GPOS / GDEF lookup stays whole (init/medi/fina/rlig/ccmp/locl/akhn/rphf/blwf/half/pstf/cjct/vatu/abvs/blws/psts/
 * haln/dist/kern/mark/mkmk ...). Output: static TrueType (glyf) .ttf, which HarfBuzz and opentype.js 1.3.4 both parse.
 *   small fonts (Arabic, Hebrew, the Indian scripts): every glyph and every cmap entry of the upstream font is kept (nothing dropped);
 *   CJK faces (20 Oct 2026: at most 1.5 MB each, 6 MB together): only COMMON characters are kept, the text engraved is short names and
 *   phrases. Per face: the `ranges` (Latin, digits, punctuation, kana, fullwidth forms ... never ideographs), the non-ideograph characters of the
 *   `symbols` tables, and the `ideographs` sets (SETS in the Python helper: code tables decoded with Python's own codecs: gb2312, big5,
 *   shift_jis, euc_kr, and the published lists above). Every horizontal GSUB/GPOS feature stays (locl, ccmp, liga, kern, palt ...); the vertical
 *   ones (vert, vrt2, vkrn, vpal, vhal) and vhea/vmtx/VORG are dropped, hinting and glyph names are off (the faces have none).
 *   A rarer character stays "not in this font" and a person decides; the coverage of every source table is recorded in the manifest.
 * Afterwards each output is shaped against the original variable font at wght=400 with HarfBuzz (uharfbuzz) for sample sentences of
 * its language: glyph names, clusters, advances and offsets must be identical (that is what "layout kept intact" means).
 */
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const cp = require('child_process');

const ROOT = path.resolve(__dirname, '..');
const OUT_DIR = path.join(ROOT, 'vendor', 'fonts', 'scripts');
const MANIFEST = path.join(ROOT, 'vendor', 'fonts', 'script-fonts.json');
const REL = 'vendor/fonts/scripts/';

const GF_COMMIT = '47d54ca1ebb14fd864695f1d39dc734f39017c33'; // google/fonts main, 19 Oct 2026
const GF_RAW = 'https://raw.githubusercontent.com/google/fonts/' + GF_COMMIT + '/ofl/';
const PY_PACKAGES = ['fonttools==4.67.0', 'brotli==1.2.0', 'uharfbuzz==0.56.3', 'opencc-python-reimplemented==0.1.7'];
// published character lists (npm tarballs, sha256 of the tarball and of the file used); `file` is written to <work>/lists/<name>.txt
const LISTS = {
  tgh: { url: 'https://registry.npmjs.org/togscc/-/togscc-1.3.1.tgz', sha256: '8507c668c0e3f643c3a03a8b85bb79313a3a49e428de80e933a41f7d7555f8db',
    member: 'package/data/characters.txt', text: (dir) => fs.readFileSync(path.join(dir, 'package/data/characters.txt'), 'utf8'), pkg: 'togscc@1.3.1' },
  joyo: { url: 'https://registry.npmjs.org/joyo-kanji/-/joyo-kanji-0.2.1.tgz', sha256: '2d871d64d73baaecbba9dd82d6c4e6a704eedf2b8806c22158f8bab81bd4b3aa',
    member: 'package/index.js', text: (dir) => require(path.join(dir, 'package/index.js')).kanji.join(''), pkg: 'joyo-kanji@0.2.1' },
  jinmeiyo: { url: 'https://registry.npmjs.org/jinmeiyo-kanji/-/jinmeiyo-kanji-1.0.0.tgz', sha256: 'a44e0373b632401435a9b2151ab9072abd93ef32f4c2ed97e7f5e0376106a833',
    member: 'package/index.js', text: (dir) => require(path.join(dir, 'package/index.js')).kanji.join(''), pkg: 'jinmeiyo-kanji@1.0.0' },
};

// Unicode ranges each face is meant to serve (inclusive, as [first, last]); the manifest stores them as decimal numbers.
const R_HAN = [[0x3000, 0x303F], [0x4E00, 0x9FFF], [0xFF00, 0xFFEF]];
const R_COMPAT = [[0xF900, 0xFAFF]]; // CJK compatibility ideographs (only a face that keeps some)
const R_BOPO = [[0x3100, 0x312F]];
const R_KANA = [[0x3040, 0x309F], [0x30A0, 0x30FF], [0x31F0, 0x31FF]];
const R_HANGUL = [[0x3130, 0x318F], [0xAC00, 0xD7AF]];
// glyphs kept besides the ideographs (CJK `ranges` of the helper): Basic Latin, Latin-1 / Extended-A, modifier letters, general punctuation (incl. the
// zero-width characters), euro, degree Celsius, numero, arrows, circled digits, geometric shapes, stars, male/female, CJK symbols and punctuation,
// fullwidth ASCII (U+FF01-FF5E) and fullwidth signs
const KEEP_COMMON = [[0x20, 0x7E], [0xA0, 0x17F], [0x2C6, 0x2DD], [0x2000, 0x206F], [0x20AC, 0x20AC], [0x2103, 0x2103], [0x2116, 0x2116], [0x2190, 0x2199],
  [0x2460, 0x2473], [0x25A0, 0x25CF], [0x2605, 0x2606], [0x2640, 0x2642], [0x3000, 0x303F], [0xFF01, 0xFF5E], [0xFFE0, 0xFFE6]];

// shaping samples (HarfBuzz comparison of the output against the variable source at wght=400): t=text s=ISO 15924 l=language d=direction
const S = (t, s, l, d) => ({ t, s, l, d: d || 'ltr' });
const FONTS = [
  { id: 'arabic', name: 'Noto Sans Arabic', dir: 'notosansarabic', file: 'NotoSansArabic', mode: 'small', scripts: ['Arab'],
    langs: ['ar', 'fa', 'ur', 'ps', 'sd', 'ug', 'ckb'], rtl: true, chain: 'arabic',
    ranges: [[0x0600, 0x06FF], [0x0750, 0x077F], [0x0870, 0x08FF], [0xFB50, 0xFDFF], [0xFE70, 0xFEFF], [0x10EC0, 0x10EFF]],
    srcSha256: '63111b5b2e074dd48cc67692e0a2726d86ee94c1c37fe8598257b7b4e87e869e',
    verify: [S('مرحبا بالعالم', 'Arab', 'ar', 'rtl'), S('محمد', 'Arab', 'ar', 'rtl'), S('لا', 'Arab', 'ar', 'rtl'), S('سلام دنیا پ چ ژ گ', 'Arab', 'fa', 'rtl'),
      S('السلام علیکم', 'Arab', 'ur', 'rtl'), S('مُحَمَّد', 'Arab', 'ar', 'rtl'), S('١٢٣ ٤٥٦ 2024', 'Arab', 'ar', 'rtl')] },
  { id: 'hebrew', name: 'Noto Sans Hebrew', dir: 'notosanshebrew', file: 'NotoSansHebrew', mode: 'small', scripts: ['Hebr'],
    langs: ['he', 'yi'], rtl: true, chain: 'hebrew', ranges: [[0x0590, 0x05FF], [0xFB1D, 0xFB4F]],
    srcSha256: '7ef36a2c3593758cdb622e1bdef4f84523e92fbc3ccc667438dd80ff54c2de88',
    verify: [S('שלום עולם', 'Hebr', 'he', 'rtl'), S('שָׁלוֹם', 'Hebr', 'he', 'rtl'), S('בְּרֵאשִׁית 2024', 'Hebr', 'he', 'rtl')] },
  { id: 'devanagari', name: 'Noto Sans Devanagari', dir: 'notosansdevanagari', file: 'NotoSansDevanagari', mode: 'small', scripts: ['Deva'],
    langs: ['hi', 'mr', 'ne', 'sa', 'kok', 'mai', 'bho'], rtl: false, chain: 'devanagari',
    ranges: [[0x0900, 0x097F], [0x1CD0, 0x1CFF], [0xA8E0, 0xA8FF], [0x11B00, 0x11B5F]],
    srcSha256: '14ec4af41f27482216d1c2229f417ff9b1425e1babb014e57d1d40d03229853e',
    verify: [S('नमस्ते दुनिया', 'Deva', 'hi'), S('हिन्दी', 'Deva', 'hi'), S('क्षत्रिय', 'Deva', 'hi'), S('कर्म श्रीमान् ज्ञान', 'Deva', 'hi'), S('प्रेम किताब ३४५', 'Deva', 'mr')] },
  { id: 'bengali', name: 'Noto Sans Bengali', dir: 'notosansbengali', file: 'NotoSansBengali', mode: 'small', scripts: ['Beng'],
    langs: ['bn', 'as'], rtl: false, chain: 'bengali', ranges: [[0x0980, 0x09FF]],
    srcSha256: 'dcd42978094e584a849c84a51450eeac40c8826057d566ea6d4b9627a403a05a',
    verify: [S('বাংলা ভাষা', 'Beng', 'bn'), S('ক্ষত্রিয় কৌশল শ্রী', 'Beng', 'bn'), S('কিছু র‍্যাব', 'Beng', 'bn')] },
  { id: 'gurmukhi', name: 'Noto Sans Gurmukhi', dir: 'notosansgurmukhi', file: 'NotoSansGurmukhi', mode: 'small', scripts: ['Guru'],
    langs: ['pa'], rtl: false, chain: 'gurmukhi', ranges: [[0x0A00, 0x0A7F]],
    srcSha256: '1e6f728fa620e566f842d81e220265813faa12771214765d289c98e035adc5f2',
    verify: [S('ਪੰਜਾਬੀ', 'Guru', 'pa'), S('ਸਤਿ ਸ੍ਰੀ ਅਕਾਲ', 'Guru', 'pa')] },
  { id: 'gujarati', name: 'Noto Sans Gujarati', dir: 'notosansgujarati', file: 'NotoSansGujarati', mode: 'small', scripts: ['Gujr'],
    langs: ['gu'], rtl: false, chain: 'gujarati', ranges: [[0x0A80, 0x0AFF]],
    srcSha256: '9901d8552f1dd5d2c50dbd4caa6f6e174e74e8264f06594ab259ae6e7b1ac428',
    verify: [S('ગુજરાતી', 'Gujr', 'gu'), S('ક્ષત્રિય કર્મ', 'Gujr', 'gu')] },
  { id: 'tamil', name: 'Noto Sans Tamil', dir: 'notosanstamil', file: 'NotoSansTamil', mode: 'small', scripts: ['Taml'],
    langs: ['ta'], rtl: false, chain: 'tamil', ranges: [[0x0B80, 0x0BFF]],
    srcSha256: 'aa3a9b321f4b0bb2c40203ffbde9af89713227866e0e13f76e5b9eeea727cf88',
    verify: [S('தமிழ்', 'Taml', 'ta'), S('கொக்கோ ஸ்ரீ', 'Taml', 'ta')] },
  { id: 'telugu', name: 'Noto Sans Telugu', dir: 'notosanstelugu', file: 'NotoSansTelugu', mode: 'small', scripts: ['Telu'],
    langs: ['te'], rtl: false, chain: 'telugu', ranges: [[0x0C00, 0x0C7F]],
    srcSha256: 'e618af7bf999df192ed4f388eba2e563f2b5015034e9cbb317b5bd793bd7334d',
    verify: [S('తెలుగు', 'Telu', 'te'), S('క్షత్రియ శ్రీ', 'Telu', 'te')] },
  { id: 'kannada', name: 'Noto Sans Kannada', dir: 'notosanskannada', file: 'NotoSansKannada', mode: 'small', scripts: ['Knda'],
    langs: ['kn'], rtl: false, chain: 'kannada', ranges: [[0x0C80, 0x0CFF]],
    srcSha256: 'cca4f3b3a8cb12fb261f1b43baf5d2f7f59d90fe123d41f0065ed3a183997ec9',
    verify: [S('ಕನ್ನಡ', 'Knda', 'kn'), S('ಕ್ಷತ್ರಿಯ ಶ್ರೀ', 'Knda', 'kn')] },
  { id: 'malayalam', name: 'Noto Sans Malayalam', dir: 'notosansmalayalam', file: 'NotoSansMalayalam', mode: 'small', scripts: ['Mlym'],
    langs: ['ml'], rtl: false, chain: 'malayalam', ranges: [[0x0D00, 0x0D7F]],
    srcSha256: '312e0e7c3cc15fa09eb42a8f749eeb246b593ed420e3c81aafe8d910c3a6fb56',
    verify: [S('മലയാളം', 'Mlym', 'ml'), S('ക്ഷത്രിയ ശ്രീ', 'Mlym', 'ml')] },
  { id: 'oriya', name: 'Noto Sans Oriya', dir: 'notosansoriya', file: 'NotoSansOriya', mode: 'small', scripts: ['Orya'],
    langs: ['or'], rtl: false, chain: 'oriya', ranges: [[0x0B00, 0x0B7F]],
    srcSha256: '910342275d619081b896b4ed25fd9ad6ff320c25591b810de1fcc4beb153ba82',
    verify: [S('ଓଡ଼ିଆ', 'Orya', 'or'), S('କ୍ଷତ୍ରିୟ', 'Orya', 'or')] },
  { id: 'sinhala', name: 'Noto Sans Sinhala', dir: 'notosanssinhala', file: 'NotoSansSinhala', mode: 'small', scripts: ['Sinh'],
    langs: ['si'], rtl: false, chain: 'sinhala', ranges: [[0x0D80, 0x0DFF], [0x111E0, 0x111FF]],
    srcSha256: '9bd93e407a278075be403324063bc94a7e306c44de4df81214e932330c22eecf',
    verify: [S('සිංහල', 'Sinh', 'si'), S('ක්‍රිකට් ශ්‍රී', 'Sinh', 'si')] },
  // ---- CJK: COMMON characters only (see the header); the cjk chain lets the integrator fall from one face to the next per character
  { id: 'cjk-sc', name: 'Noto Sans SC', dir: 'notosanssc', file: 'NotoSansSC', mode: 'cjk', scripts: ['Hani', 'Bopo'],
    langs: ['zh', 'zh-Hans', 'zh-CN', 'zh-SG'], rtl: false, chain: 'cjk', ranges: [...R_HAN, ...R_BOPO],
    srcPattern: '[wght]', srcSha256: 'a3041811a78c361b1de50f953c805e0244951c21c5bd412f7232ef0d899af0da',
    sets: { ranges: [...KEEP_COMMON, [0x3105, 0x312F]], symbols: ['gb2312'], ideographs: ['gb2312-l1', 'tgh-l1'],
      report: ['gb2312', 'gb2312-l1', 'tgh', 'tgh-l1'], lists: ['tgh'],
      note: 'Simplified Chinese, common characters only: the 3,755 GB 2312 level-1 hanzi + the 3,500 of the level 1 of the 2013 通用规范汉字表, plus all GB 2312 symbols and Bopomofo' },
    verify: [S('你好，世界 爱', 'Hani', 'zh-Hans'), S('中华人民共和国 谢谢 欢迎 ABC 123', 'Hani', 'zh-Hans'), S('（王）「李」—… ￥１２３', 'Hani', 'zh-Hans')] },
  { id: 'cjk-tc', name: 'Noto Sans TC', dir: 'notosanstc', file: 'NotoSansTC', mode: 'cjk', scripts: ['Hani', 'Bopo'],
    langs: ['zh-Hant', 'zh-TW', 'zh-HK', 'zh-MO', 'yue'], rtl: false, chain: 'cjk', ranges: [...R_HAN, ...R_BOPO],
    srcPattern: '[wght]', srcSha256: '864727d210d54f2537bbe23b3a839436c3992af72de9322af5270897246bd44f',
    sets: { ranges: [...KEEP_COMMON, [0x3105, 0x312F]], symbols: [], ideographs: ['big5-l1-common', 'cantonese'],
      report: ['big5', 'big5-l1', 'big5-l1-common', 'cantonese'], lists: ['tgh'],
      note: 'Traditional Chinese, common characters only: the Big5 level-1 hanzi (5,401 frequently used) whose simplified form is among the 3,500 commonest, + 25 everyday Hong Kong Cantonese characters, Bopomofo' },
    verify: [S('你好，世界 愛', 'Hani', 'zh-Hant'), S('嘅 啲 喺 冇 嘢 ABC 123', 'Hani', 'zh-HK'), S('台灣 學校 謝謝', 'Hani', 'zh-Hant')] },
  { id: 'cjk-jp', name: 'Noto Sans JP', dir: 'notosansjp', file: 'NotoSansJP', mode: 'cjk', scripts: ['Hani', 'Hira', 'Kana'],
    langs: ['ja'], rtl: false, chain: 'cjk', ranges: [...R_HAN, ...R_KANA],
    srcPattern: '[wght]', srcSha256: 'c2f3b4d463500a2ddcd3849cded1fceeb9fd6d1c32e6cbecd568453ba50fc68f',
    sets: { ranges: [...KEEP_COMMON, [0x3040, 0x30FF], [0x31F0, 0x31FF], [0xFF61, 0xFF9F]], symbols: ['jisx0208'], ideographs: ['joyo', 'jisx0208-l1', 'jinmeiyo'],
      report: ['jisx0208', 'jisx0208-l1', 'joyo', 'jinmeiyo'], lists: ['joyo', 'jinmeiyo'],
      note: 'Japanese, common characters only: all kana, the 2,136 Joyo kanji, the 2,965 JIS X 0208 level-1 kanji and the 863 jinmeiyo (name) kanji, plus all JIS X 0208 symbols' },
    verify: [S('こんにちは世界 日本語のテキスト カタカナ ツ', 'Hani', 'ja'), S('高橋 山崎 辻 ABC 123', 'Hani', 'ja'), S('ｱｲｳｴｵ ー！', 'Kana', 'ja')] },
  { id: 'cjk-kr', name: 'Noto Sans KR', dir: 'notosanskr', file: 'NotoSansKR', mode: 'cjk', scripts: ['Hang', 'Hani'],
    langs: ['ko'], rtl: false, chain: 'cjk', ranges: [...R_HANGUL, ...R_HAN],
    srcPattern: '[wght]', srcSha256: '194018e6b2b293a7964f037b25c0249ce1418bc9ab3c971060a03aa57861e252',
    sets: { ranges: [...KEEP_COMMON, [0x3131, 0x318E]], symbols: [], ideographs: ['ksx1001-hangul', 'ksx1001-hanja-common'],
      report: ['ksx1001', 'ksx1001-hangul', 'ksx1001-hanja', 'ksx1001-hanja-common', 'hangul'], lists: [],
      note: 'Korean, common characters only: the 2,350 KS X 1001 Hangul syllables + the KS X 1001 Hanja that are also in the level 1 of the Japanese and Taiwanese standards (2,312), compatibility Jamo' },
    verify: [S('안녕하세요 세계 사랑', 'Hang', 'ko'), S('漢字 大韓 山水 ABC 123', 'Hani', 'ko'), S('ㄱㄴㄷ', 'Hang', 'ko')] },
];

// ----------------------------------------------------------------------------------------------------------------------------
const PY_SOURCE = String.raw`# fontbuild.py -- helper of scripts/build-script-fonts.cjs (run as: python -I fontbuild.py <command> ...)
#   small <src.ttf> <dst.ttf>                  instance wght=400/wdth=100, keep every glyph / cmap entry / layout feature
#   cjk   <src.ttf> <dst.ttf> <spec.json>      instance wght=400, keep the listed ranges / symbol tables / COMMON ideograph sets only
#   verify <var.ttf> <static.ttf> <cases.json> HarfBuzz: static output must shape exactly like the variable source at wght=400
import sys, os, json
from fontTools.ttLib import TTFont
from fontTools import subset
from fontTools.varLib import instancer

def options():
    # == pyftsubset --layout-features='*' --layout-scripts='*' --glyph-names --notdef-outline --name-IDs='*'
    #                --name-legacy --name-languages='*' --legacy-cmap --symbol-cmap   (hinting kept, timestamp untouched)
    o = subset.Options()
    o.layout_features = ['*']; o.layout_scripts = ['*']; o.glyph_names = True; o.notdef_outline = True
    o.name_IDs = ['*']; o.name_legacy = True; o.name_languages = ['*']; o.legacy_cmap = True; o.symbol_cmap = True
    return o

def pin(f):
    axes = {a.axisTag: (400 if a.axisTag == 'wght' else 100) for a in f['fvar'].axes}
    return axes

def version(f):
    n = f['name'].getDebugName(5)
    return n if n else ''

def build_small(src, dst):
    f = TTFont(src, recalcTimestamp=False)
    f = instancer.instantiateVariableFont(f, pin(f), inplace=False, updateFontNames=True)
    s = subset.Subsetter(options()); s.populate(glyphs=f.getGlyphOrder(), unicodes=list(f.getBestCmap().keys())); s.subset(f)
    f.save(dst)
    return {'bytes': os.path.getsize(dst), 'glyphs': len(f.getGlyphOrder()), 'cmap': len(f.getBestCmap()), 'version': version(f)}

# ---- CJK character sets: Python's own codecs (+ small published lists, see build-script-fonts.cjs) ----
R = lambda a, b: range(a, b + 1)
def is_han(c):   # ideographs only (CJK radicals 2E80-2FDF and symbols are "other")
    return (0x3400 <= c <= 0x4DBF) or (0x4E00 <= c <= 0x9FFF) or (0xF900 <= c <= 0xFAFF) or (0x20000 <= c <= 0x3FFFF) or (0x2F800 <= c <= 0x2FA1F)
def is_syl(c): return 0xAC00 <= c <= 0xD7A3
def han(s): return {c for c in s if is_han(c)}
def dec2(codec, leads, trails, prefix=b''):
    out = set()
    for l in leads:
        for t in trails:
            try: s = (prefix + bytes([l, t])).decode(codec)
            except Exception: continue
            out.update(ord(ch) for ch in s)
    return out
T40FE = [t for t in R(0x40, 0xFE) if t != 0x7F]
B5_T = list(R(0x40, 0x7E)) + list(R(0xA1, 0xFE))
JIS_L = list(R(0x81, 0x9F)) + list(R(0xE0, 0xFC)); JIS_T = [t for t in R(0x40, 0xFC) if t != 0x7F]
FILES = {}
def chars_of(name): return [ord(ch) for ch in open(FILES[name], encoding='utf-8').read() if not ch.isspace()]
def t2s():   # OpenCC traditional -> simplified character map (the first variant of each entry)
    import opencc
    out = {}
    for line in open(os.path.join(os.path.dirname(opencc.__file__), 'dictionary', 'TSCharacters.txt'), encoding='utf-8'):
        a, b = line.rstrip('\n').split('\t'); out[ord(a)] = ord(b.split(' ')[0])
    return out
SETS = {
  'gb2312':    lambda: dec2('gb2312', R(0xA1, 0xF7), R(0xA1, 0xFE)),
  'gb2312-l1': lambda: han(dec2('gb2312', R(0xB0, 0xD7), R(0xA1, 0xFE))),               # the 3,755 level-1 hanzi
  'tgh':       lambda: set(chars_of('tgh')),                                              # 通用规范汉字表: 8,105 characters, in the table's own order
  'tgh-l1':    lambda: set(chars_of('tgh')[:3500]),                                       # its level 1: the 3,500 commonest
  'big5':      lambda: dec2('big5', R(0xA1, 0xF9), B5_T),
  'big5-l1':   lambda: han(dec2('big5', R(0xA4, 0xC5), B5_T) | dec2('big5', [0xC6], R(0x40, 0x7E))),   # the 5,401 frequently used
  'big5-l1-common': lambda: {c for c in SETS['big5-l1']() if c in SETS['tgh-l1']() or t2s().get(c) in SETS['tgh-l1']()},
  'jisx0208':  lambda: dec2('shift_jis', JIS_L, JIS_T),
  'jisx0208-l1': lambda: han(dec2('shift_jis', R(0x88, 0x97), JIS_T) | dec2('shift_jis', [0x98], R(0x40, 0x72))),   # the 2,965 level-1 kanji
  'joyo':      lambda: set(chars_of('joyo')),                                             # 常用漢字 (2010): 2,136
  'jinmeiyo':  lambda: set(chars_of('jinmeiyo')),                                         # 人名用漢字: names
  'ksx1001':   lambda: dec2('euc_kr', R(0xA1, 0xFD), R(0xA1, 0xFE)),
  'ksx1001-hangul': lambda: {c for c in SETS['ksx1001']() if is_syl(c)},                  # 2,350
  'ksx1001-hanja':  lambda: han(SETS['ksx1001']()),                                       # 4,888
  'ksx1001-hanja-common': lambda: SETS['ksx1001-hanja']() & SETS['jisx0208-l1']() & SETS['big5-l1'](),   # also in the level 1 of the Japanese and Taiwanese standards
  'hangul':    lambda: set(R(0xAC00, 0xD7A3)),
  'cantonese': lambda: {ord(ch) for ch in '嘅啲喺冇嘢咗唔哋佢嚟咁乜嗰咩啱睇搵嘩啫囉嘥掂攞咪噉'},   # everyday Hong Kong Cantonese characters (HKSCS)
}
VERTICAL = {'vert', 'vrt2', 'vkrn', 'vpal', 'vhal', 'valt', 'vchw', 'vkna'}
_cache = {}
def table(name):
    if name not in _cache: _cache[name] = SETS[name]()
    return _cache[name]

def build_cjk(src, dst, spec):
    FILES.update(spec.get('files', {}))
    f = TTFont(src, recalcTimestamp=False)
    cm = f.getBestCmap()
    keep = set()
    for lo, hi in spec['ranges']: keep |= {c for c in cm if lo <= c <= hi}                           # Latin, punctuation, kana, fullwidth ... (never ideographs)
    for n in spec['symbols']: keep |= {c for c in table(n) if c in cm and not is_han(c) and not is_syl(c)}
    for n in spec['ideographs']: keep |= {c for c in table(n) if c in cm}
    tags = set()
    for t in ('GSUB', 'GPOS'): tags |= {fr.FeatureTag for fr in f[t].table.FeatureList.FeatureRecord}
    o = options()
    o.layout_features = sorted(tags - VERTICAL)                                                     # every horizontal feature (locl, ccmp, liga, kern, palt ...), no vertical ones
    o.hinting = False; o.glyph_names = False                                                         # the faces are unhinted and nameless anyway; keeps the subset lean
    o.drop_tables = list(o.drop_tables) + ['vhea', 'vmtx', 'VORG']
    s = subset.Subsetter(o); s.populate(unicodes=sorted(keep)); s.subset(f)
    f = instancer.instantiateVariableFont(f, pin(f), inplace=False, updateFontNames=True)
    f.save(dst)
    out = set(f.getBestCmap().keys()); up = set(cm.keys())
    stats = {}
    for n in spec['report']:
        t = table(n)                                                                                 # always the FULL source table
        ideo = {c for c in t if is_han(c) or is_syl(c)}
        stats[n] = {'total': len(t), 'ideographs': len(ideo), 'inUpstream': len(t & up), 'covered': len(t & out),
                    'ideographsCovered': len(ideo & out), 'pct': round(100.0 * len(t & out) / len(t), 2),
                    'pctOfUpstream': round(100.0 * len(t & out) / max(1, len(t & up)), 2),
                    'ideographPct': round(100.0 * len(ideo & out) / max(1, len(ideo)), 2)}
    return {'bytes': os.path.getsize(dst), 'glyphs': len(f.getGlyphOrder()), 'cmap': len(out), 'upstreamCmap': len(up), 'tables': stats,
            'ideographs': len({c for c in out if is_han(c) or is_syl(c)}), 'version': version(f)}

def verify(var_path, static_path, cases):
    import uharfbuzz as hb
    def shape(path, c, variations):
        font = hb.Font(hb.Face(hb.Blob.from_file_path(path)))
        if variations: font.set_variations(variations)
        buf = hb.Buffer(); buf.add_str(c['t']); buf.direction = c.get('d', 'ltr'); buf.script = c['s']; buf.language = c.get('l', 'en')
        hb.shape(font, buf, {})
        out = []
        for i, p in zip(buf.glyph_infos, buf.glyph_positions):
            name = font.glyph_to_string(i.codepoint)
            e = font.get_glyph_extents(i.codepoint)                        # the outline's box stands in for the glyph id (the CJK faces have no glyph names)
            out.append(('' if name.startswith('gid') else name, i.cluster, p.x_advance, p.x_offset, p.y_offset,
                        (e.x_bearing, e.y_bearing, e.width, e.height) if e else None))
        return out
    vf = TTFont(var_path)
    var = {a.axisTag: (400 if a.axisTag == 'wght' else 100) for a in vf['fvar'].axes}
    bad = []
    for c in cases:
        a = shape(var_path, c, var); b = shape(static_path, c, None)
        if a != b: bad.append(c['t'])
    return {'cases': len(cases), 'mismatches': bad}

if __name__ == '__main__':
    cmd = sys.argv[1]
    if cmd == 'small': r = build_small(sys.argv[2], sys.argv[3])
    elif cmd == 'cjk': r = build_cjk(sys.argv[2], sys.argv[3], json.load(open(sys.argv[4])))
    elif cmd == 'verify': r = verify(sys.argv[2], sys.argv[3], json.load(open(sys.argv[4])))
    else: raise SystemExit('unknown command ' + cmd)
    print(json.dumps(r))
`;
// ----------------------------------------------------------------------------------------------------------------------------

function parseArgs(argv) {
  const a = { work: path.join(os.tmpdir(), 'script-fonts-build'), only: null, check: false, verify: true, verifyOnly: false, format: false };
  for (let i = 0; i < argv.length; i++) {
    const k = argv[i];
    if (k === '--work') a.work = path.resolve(argv[++i]);
    else if (k === '--only') a.only = String(argv[++i]).split(',').filter(Boolean);
    else if (k === '--check') a.check = true;
    else if (k === '--no-verify') a.verify = false;
    else if (k === '--verify-only') a.verifyOnly = true;
    else if (k === '--format') a.format = true;
    else if (k === '--help' || k === '-h') { console.log(fs.readFileSync(__filename, 'utf8').split('*/')[0]); process.exit(0); }
    else throw new Error('unknown argument ' + k);
  }
  return a;
}
// sorted, with overlapping / touching ranges merged
const norm = (rs) => rs.map((r) => r.slice()).sort((a, b) => a[0] - b[0]).reduce((o, r) => { const l = o[o.length - 1]; if (l && r[0] <= l[1] + 1) l[1] = Math.max(l[1], r[1]); else o.push(r); return o; }, []);
const sha256 = (file) => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
function run(cmd, args, opts) {
  const r = cp.spawnSync(cmd, args, Object.assign({ encoding: 'utf8', maxBuffer: 256 * 1024 * 1024 }, opts));
  if (r.status !== 0) throw new Error(cmd + ' ' + args.join(' ') + ' failed (' + (r.status === null ? r.signal : r.status) + ')\n' + (r.stderr || '') + (r.stdout || ''));
  return r.stdout;
}
function download(url, dest) {
  if (fs.existsSync(dest)) return;
  console.log('  download ' + url);
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  run('curl', ['-fsSL', '--retry', '3', '--retry-delay', '2', '-o', dest + '.part', url]);
  fs.renameSync(dest + '.part', dest);
}
function ensureVenv(work) {
  const py = path.join(work, 'venv', 'bin', 'python');
  const want = PY_PACKAGES.map((p) => p.toLowerCase());
  if (fs.existsSync(py)) {
    try {
      const have = run(py, ['-I', '-m', 'pip', 'freeze']).toLowerCase().split('\n');
      if (want.every((w) => have.includes(w))) return py;
    } catch (e) { /* rebuild the venv below */ }
  }
  console.log('creating venv in ' + work);
  fs.mkdirSync(work, { recursive: true });
  run('python3', ['-m', 'venv', path.join(work, 'venv')]);
  run(py, ['-I', '-m', 'pip', 'install', '--quiet', '--disable-pip-version-check'].concat(PY_PACKAGES));
  return py;
}
function ensureList(work, name) {
  const L = LISTS[name], dir = path.join(work, 'lists', name), txt = path.join(work, 'lists', name + '.txt');
  if (fs.existsSync(txt)) return txt;
  const tgz = path.join(dir, name + '.tgz');
  download(L.url, tgz);
  if (sha256(tgz) !== L.sha256) throw new Error(L.pkg + ' tarball sha256 mismatch');
  run('tar', ['-xzf', tgz, '-C', dir, L.member]);
  fs.writeFileSync(txt, L.text(dir));
  return txt;
}
// JSON with the range pairs on one line each (readable diffs)
function stringify(m) {
  return JSON.stringify(m, null, 2).replace(/\[\s+(\d+),\s+(\d+)\s+\]/g, '[$1, $2]').replace(/\[\s+("[^"\]]*"(?:,\s+"[^"\]]*")*)\s+\]/g, (m0, items) => '[' + items.replace(/,\s+/g, ', ') + ']');
}
function coverageNote(f, r) {
  if (f.mode === 'small') return 'Complete upstream font: all ' + r.glyphs + ' glyphs / ' + r.cmap + ' cmap entries kept (Latin, digits and punctuation included), GSUB/GPOS/GDEF untouched.';
  const t = r.tables;
  const parts = Object.keys(t).map((k) => k + ' ' + t[k].covered + '/' + t[k].total + ' (' + t[k].pct + '%)');
  return f.sets.note + '. Coverage of each source table (characters in the face / characters in the table): ' + parts.join('; ') +
    '. Kept besides the ideographs: Latin, digits, punctuation, fullwidth forms, CJK symbols (and kana / Bopomofo / compatibility Jamo / the symbol rows of the national table where the language uses them); a rarer character is "not in this font" and a person decides.';
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.format) {
    const m = JSON.parse(fs.readFileSync(MANIFEST, 'utf8'));
    for (const e of m.fonts) { const f = FONTS.find((x) => x.id === e.id); if (f && f.mode === 'cjk') { e.ranges = norm(f.ranges); e.coverage = coverageNote(f, { tables: e.coverageStats }); } }
    fs.writeFileSync(MANIFEST, stringify(m) + '\n'); return;
  }
  const selected = FONTS.filter((f) => !args.only || args.only.includes(f.id));
  if (!selected.length) throw new Error('no font matches --only ' + args.only);
  const work = args.work;
  const py = ensureVenv(work);
  const helper = path.join(work, 'fontbuild.py');
  fs.writeFileSync(helper, PY_SOURCE);
  const old = fs.existsSync(MANIFEST) ? JSON.parse(fs.readFileSync(MANIFEST, 'utf8')) : { version: 1, fonts: [] };
  const entries = new Map(old.fonts.map((e) => [e.id, e]));
  let failed = 0;
  for (const f of selected) {
    console.log('== ' + f.id + ' (' + f.name + ')');
    const upstream = f.file + (f.srcPattern || '[wdth,wght]') + '.ttf';
    const src = path.join(work, 'src', f.file + '.ttf');
    download(GF_RAW + f.dir + '/' + encodeURIComponent(upstream).replace(/%2C/g, ','), src);
    if (sha256(src) !== f.srcSha256) throw new Error(f.id + ': upstream file differs from the pinned sha256 (' + sha256(src) + ')');
    const lic = path.join(work, 'src', f.dir + '-OFL.txt');
    download(GF_RAW + f.dir + '/OFL.txt', lic);
    if (args.verifyOnly) {
      const committed = path.join(OUT_DIR, f.file + '-Regular.ttf');
      const casesFile = path.join(work, 'cases-' + f.id + '.json');
      fs.writeFileSync(casesFile, JSON.stringify(f.verify));
      const v = JSON.parse(run(py, ['-I', helper, 'verify', src, committed, casesFile]).trim().split('\n').pop());
      console.log('  committed file vs the variable source at wght=400 (HarfBuzz): ' + (v.cases - v.mismatches.length) + '/' + v.cases + ' identical');
      if (v.mismatches.length) { failed++; console.error('  MISMATCH: ' + v.mismatches.join(' | ')); }
      continue;
    }
    const out = path.join(work, 'out', f.file + '-Regular.ttf');
    fs.mkdirSync(path.dirname(out), { recursive: true });
    let r;
    if (f.mode === 'small') r = JSON.parse(run(py, ['-I', helper, 'small', src, out]).trim().split('\n').pop());
    else {
      const files = {};
      for (const l of f.sets.lists) files[l] = ensureList(work, l);
      const spec = Object.assign({ files }, f.sets);
      const specFile = path.join(work, 'spec-' + f.id + '.json');
      fs.writeFileSync(specFile, JSON.stringify(spec));
      r = JSON.parse(run(py, ['-I', helper, 'cjk', src, out, specFile]).trim().split('\n').pop());
    }
    if (args.verify) {
      const casesFile = path.join(work, 'cases-' + f.id + '.json');
      fs.writeFileSync(casesFile, JSON.stringify(f.verify));
      const v = JSON.parse(run(py, ['-I', helper, 'verify', src, out, casesFile]).trim().split('\n').pop());
      console.log('  HarfBuzz vs the variable source at wght=400: ' + (v.cases - v.mismatches.length) + '/' + v.cases + ' identical');
      if (v.mismatches.length) { failed++; console.error('  MISMATCH: ' + v.mismatches.join(' | ')); }
    }
    const hash = sha256(out);
    const bytes = fs.statSync(out).size;
    console.log('  ' + bytes + ' bytes, ' + r.glyphs + ' glyphs, sha256 ' + hash.slice(0, 16) + '...' + (r.tables ? '\n  ' + JSON.stringify(r.tables) : ''));
    const prev = entries.get(f.id);
    if (args.check) {
      if (!prev) { failed++; console.error('  not in the manifest'); } else if (prev.sha256 !== hash) { failed++; console.error('  DIFFERS from the manifest (' + prev.sha256.slice(0, 16) + '...)'); } else console.log('  matches the manifest');
      continue;
    }
    fs.mkdirSync(OUT_DIR, { recursive: true });
    fs.copyFileSync(out, path.join(OUT_DIR, f.file + '-Regular.ttf'));
    fs.copyFileSync(lic, path.join(OUT_DIR, f.file + '-OFL.txt'));
    const entry = {
      id: f.id, name: f.name, file: REL + f.file + '-Regular.ttf', bytes, sha256: hash, license: REL + f.file + '-OFL.txt',
      scripts: f.scripts, langs: f.langs, rtl: f.rtl, ranges: norm(f.ranges), chain: f.chain,
      weight: 400, glyphs: r.glyphs, version: r.version, coverage: coverageNote(f, r),
      source: { repo: 'https://github.com/google/fonts', commit: GF_COMMIT, path: 'ofl/' + f.dir + '/' + upstream, sha256: f.srcSha256, license: 'SIL OFL 1.1' },
    };
    if (r.tables) {
      entry.coverageStats = r.tables;
      const lists = f.sets.lists.map((l) => LISTS[l].pkg).concat(f.sets.ideographs.includes('big5-l1-common') ? ['opencc-python-reimplemented@0.1.7'] : []);
      if (lists.length) entry.source.lists = { packages: lists, use: 'choice of the glyphs to keep only; the lists are not redistributed' };
      entry.keep = { ranges: norm(f.sets.ranges), symbols: f.sets.symbols, ideographs: f.sets.ideographs };
    }
    entries.set(f.id, entry);
  }
  if (args.check || args.verifyOnly) { if (failed) process.exit(1); return; }
  // re-read right before writing: several `--only` runs may go in parallel
  const latest = fs.existsSync(MANIFEST) ? JSON.parse(fs.readFileSync(MANIFEST, 'utf8')).fonts : [];
  for (const e of latest) if (!selected.some((f) => f.id === e.id)) entries.set(e.id, e);
  const fonts = FONTS.map((f) => entries.get(f.id)).filter(Boolean);
  const manifest = { version: 1, fonts };
  fs.writeFileSync(MANIFEST, stringify(manifest) + '\n');
  console.log('wrote ' + path.relative(ROOT, MANIFEST) + ' (' + fonts.length + ' fonts, ' + fonts.reduce((n, e) => n + e.bytes, 0) + ' bytes of fonts)');
  if (failed) { console.error(failed + ' HarfBuzz comparison(s) failed'); process.exit(1); }
}

if (require.main === module) {
  try { main(); } catch (e) { console.error(e.message || e); process.exit(1); }
}
module.exports = { FONTS, GF_COMMIT };
