/* charm-nest-alphabet-data.js — THE LETTERS, WORDS AND PHRASES OF OTHER ALPHABETS, FOUND BY THEIR ENGLISH (ALPHAFIND; Paul, 28 Oct 2026: "I also don't see the option to choose a
   alphabet/language letters/characters. These alternate alphabets needs to be searchable for equivalent english letters and words as in some languages a single sign can have several
   meanings and entire words or phrases depending on context. I don't want this solution to turn into a massive translation system. I just want away for users to easily find equivalent
   characters, words, and meanings and phrases in other languages.")

   A small, fixed reference, not a translator: nothing is worked out, nothing is asked of a service. Two lists:
     LETTERS  every letter of the alphabets the engraving can set (charm-nest-ce-text.js: Greek, Cyrillic, Hebrew, Arabic, Devanagari, Japanese kana, Korean letters), each with its name and
              the Latin letter(s) it stands for;
     WORDS    the words and phrases people engrave (love, mother, forever, "I love you", a Latin motto ...), each with ALL its meanings (a Chinese or Japanese sign, "shalom", "aloha" mean
              several things), and how it is said in each language that has it, with a note where it changes with who says it or to whom.
   Languages: Spanish, French, Italian, Portuguese, German, Latin, Irish, Hawaiian, Greek, Russian, Hebrew, Arabic, Hindi, Chinese (simplified; traditional where it differs), Japanese, Korean.
   CNAlphabetData = { LANGS, SCRIPTS, letters(), words(), entries() }: entries() is one flat list, each { id, kind: 'letter' | 'word', lang, script, text, roman, meanings: [..], note, en,
   hay } (hay: what a search reads, lower case). Read by charm-nest-alphabet-picker.js. UMD: module.exports in node (the tests). */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  if (root) root.CNAlphabetData = api;
})(typeof self !== 'undefined' ? self : typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';
  // code → [name, script]
  const LANGS = {
    es: ['Spanish', 'Latin'], fr: ['French', 'Latin'], it: ['Italian', 'Latin'], pt: ['Portuguese', 'Latin'], de: ['German', 'Latin'], la: ['Latin', 'Latin'], ga: ['Irish', 'Latin'], haw: ['Hawaiian', 'Latin'],
    el: ['Greek', 'Greek'], ru: ['Russian', 'Cyrillic'], he: ['Hebrew', 'Hebrew'], ar: ['Arabic', 'Arabic'], hi: ['Hindi', 'Devanagari'], sa: ['Sanskrit', 'Devanagari'],
    zh: ['Chinese', 'Chinese'], ja: ['Japanese', 'Japanese'], ko: ['Korean', 'Korean']
  };
  const SCRIPTS = ['Greek', 'Cyrillic', 'Hebrew', 'Arabic', 'Devanagari', 'Chinese', 'Japanese', 'Korean', 'Latin'];

  /* ── LETTERS: [letter(s), name, Latin equivalent(s)] ── */
  const GREEK = [['Α α', 'alpha', 'a'], ['Β β', 'beta', 'b, v'], ['Γ γ', 'gamma', 'g'], ['Δ δ', 'delta', 'd'], ['Ε ε', 'epsilon', 'e'], ['Ζ ζ', 'zeta', 'z'], ['Η η', 'eta', 'e, i'], ['Θ θ', 'theta', 'th'],
    ['Ι ι', 'iota', 'i'], ['Κ κ', 'kappa', 'k'], ['Λ λ', 'lambda', 'l'], ['Μ μ', 'mu', 'm'], ['Ν ν', 'nu', 'n'], ['Ξ ξ', 'xi', 'x, ks'], ['Ο ο', 'omicron', 'o'], ['Π π', 'pi', 'p'], ['Ρ ρ', 'rho', 'r'],
    ['Σ σ ς', 'sigma (ς at the end of a word)', 's'], ['Τ τ', 'tau', 't'], ['Υ υ', 'upsilon', 'u, y'], ['Φ φ', 'phi', 'ph, f'], ['Χ χ', 'chi', 'ch, kh'], ['Ψ ψ', 'psi', 'ps'], ['Ω ω', 'omega', 'o']];
  const CYRILLIC = [['А а', 'a', 'a'], ['Б б', 'be', 'b'], ['В в', 've', 'v'], ['Г г', 'ge', 'g'], ['Д д', 'de', 'd'], ['Е е', 'ye', 'ye, e'], ['Ё ё', 'yo', 'yo'], ['Ж ж', 'zhe', 'zh'], ['З з', 'ze', 'z'],
    ['И и', 'i', 'i'], ['Й й', 'short i', 'y'], ['К к', 'ka', 'k'], ['Л л', 'el', 'l'], ['М м', 'em', 'm'], ['Н н', 'en', 'n'], ['О о', 'o', 'o'], ['П п', 'pe', 'p'], ['Р р', 'er', 'r'], ['С с', 'es', 's'],
    ['Т т', 'te', 't'], ['У у', 'u', 'u'], ['Ф ф', 'ef', 'f'], ['Х х', 'kha', 'kh, h'], ['Ц ц', 'tse', 'ts'], ['Ч ч', 'che', 'ch'], ['Ш ш', 'sha', 'sh'], ['Щ щ', 'shcha', 'shch'], ['Ъ ъ', 'hard sign', '(no sound)'],
    ['Ы ы', 'yery', 'y'], ['Ь ь', 'soft sign', '(softens the letter before)'], ['Э э', 'e', 'e'], ['Ю ю', 'yu', 'yu'], ['Я я', 'ya', 'ya']];
  const HEBREW = [['א', 'aleph', '(silent), a'], ['ב', 'bet', 'b, v'], ['ג', 'gimel', 'g'], ['ד', 'dalet', 'd'], ['ה', 'he', 'h'], ['ו', 'vav', 'v, o, u'], ['ז', 'zayin', 'z'], ['ח', 'chet', 'ch, h'],
    ['ט', 'tet', 't'], ['י', 'yod', 'y, i'], ['כ ך', 'kaf (ך at the end of a word)', 'k, kh'], ['ל', 'lamed', 'l'], ['מ ם', 'mem (ם at the end of a word)', 'm'], ['נ ן', 'nun (ן at the end of a word)', 'n'],
    ['ס', 'samekh', 's'], ['ע', 'ayin', '(silent)'], ['פ ף', 'pe (ף at the end of a word)', 'p, f'], ['צ ץ', 'tsadi (ץ at the end of a word)', 'ts'], ['ק', 'qof', 'k, q'], ['ר', 'resh', 'r'],
    ['ש', 'shin', 'sh, s'], ['ת', 'tav', 't']];
  const ARABIC = [['ا', 'alif', 'a'], ['ب', 'ba', 'b'], ['ت', 'ta', 't'], ['ث', 'tha', 'th'], ['ج', 'jim', 'j'], ['ح', 'ha (deep h)', 'h'], ['خ', 'kha', 'kh'], ['د', 'dal', 'd'], ['ذ', 'dhal', 'dh'],
    ['ر', 'ra', 'r'], ['ز', 'zay', 'z'], ['س', 'sin', 's'], ['ش', 'shin', 'sh'], ['ص', 'sad', 's (deep)'], ['ض', 'dad', 'd (deep)'], ['ط', 'ta (deep)', 't (deep)'], ['ظ', 'za', 'z (deep)'],
    ['ع', 'ayn', '(throat sound)'], ['غ', 'ghayn', 'gh'], ['ف', 'fa', 'f'], ['ق', 'qaf', 'q'], ['ك', 'kaf', 'k'], ['ل', 'lam', 'l'], ['م', 'mim', 'm'], ['ن', 'nun', 'n'], ['ه', 'ha', 'h'],
    ['و', 'waw', 'w, u'], ['ي', 'ya', 'y, i']];
  const DEVANAGARI = [['अ', 'a', 'a'], ['आ', 'aa', 'aa'], ['इ', 'i', 'i'], ['ई', 'ii', 'ee'], ['उ', 'u', 'u'], ['ऊ', 'uu', 'oo'], ['ए', 'e', 'e'], ['ऐ', 'ai', 'ai'], ['ओ', 'o', 'o'], ['औ', 'au', 'au'],
    ['क', 'ka', 'k'], ['ख', 'kha', 'kh'], ['ग', 'ga', 'g'], ['घ', 'gha', 'gh'], ['च', 'cha', 'ch'], ['छ', 'chha', 'chh'], ['ज', 'ja', 'j'], ['झ', 'jha', 'jh'], ['ट', 'ta (hard)', 't'], ['ठ', 'tha (hard)', 'th'],
    ['ड', 'da (hard)', 'd'], ['ढ', 'dha (hard)', 'dh'], ['ण', 'na (hard)', 'n'], ['त', 'ta', 't'], ['थ', 'tha', 'th'], ['द', 'da', 'd'], ['ध', 'dha', 'dh'], ['न', 'na', 'n'], ['प', 'pa', 'p'],
    ['फ', 'pha', 'ph, f'], ['ब', 'ba', 'b'], ['भ', 'bha', 'bh'], ['म', 'ma', 'm'], ['य', 'ya', 'y'], ['र', 'ra', 'r'], ['ल', 'la', 'l'], ['व', 'va', 'v, w'], ['श', 'sha', 'sh'], ['ष', 'sha (hard)', 'sh'],
    ['स', 'sa', 's'], ['ह', 'ha', 'h']];
  // the Japanese syllables (gojūon), hiragana and katakana: the same sounds, katakana for foreign words and names
  const KANA_ROMAN = ['a', 'i', 'u', 'e', 'o', 'ka', 'ki', 'ku', 'ke', 'ko', 'sa', 'shi', 'su', 'se', 'so', 'ta', 'chi', 'tsu', 'te', 'to', 'na', 'ni', 'nu', 'ne', 'no', 'ha', 'hi', 'fu', 'he', 'ho',
    'ma', 'mi', 'mu', 'me', 'mo', 'ya', 'yu', 'yo', 'ra', 'ri', 'ru', 're', 'ro', 'wa', 'wo', 'n'];
  const HIRA = 'あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわをん', KATA = 'アイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワヲン';
  const KOREAN = [['ㄱ', 'giyeok', 'g, k'], ['ㄴ', 'nieun', 'n'], ['ㄷ', 'digeut', 'd, t'], ['ㄹ', 'rieul', 'r, l'], ['ㅁ', 'mieum', 'm'], ['ㅂ', 'bieup', 'b, p'], ['ㅅ', 'siot', 's'], ['ㅇ', 'ieung', 'ng (silent at the start)'],
    ['ㅈ', 'jieut', 'j'], ['ㅊ', 'chieut', 'ch'], ['ㅋ', 'kieuk', 'k'], ['ㅌ', 'tieut', 't'], ['ㅍ', 'pieup', 'p'], ['ㅎ', 'hieut', 'h'], ['ㅏ', 'a', 'a'], ['ㅑ', 'ya', 'ya'], ['ㅓ', 'eo', 'eo'], ['ㅕ', 'yeo', 'yeo'],
    ['ㅗ', 'o', 'o'], ['ㅛ', 'yo', 'yo'], ['ㅜ', 'u', 'u'], ['ㅠ', 'yu', 'yu'], ['ㅡ', 'eu', 'eu'], ['ㅣ', 'i', 'i']];

  /* ── WORDS: [English, [every meaning], tags, { lang: [text, how it is said, note?] }] ── */
  const W = [
    ['love', ['love', 'affection'], 'romance wedding', { es: ['amor', 'ah-MOR'], fr: ['amour', 'ah-MOOR'], it: ['amore', 'ah-MO-reh'], pt: ['amor', 'ah-MOR'], de: ['Liebe', 'LEE-beh'], la: ['amor', 'amor'], ga: ['grá', 'graw'],
      haw: ['aloha', 'ah-LO-ha', 'aloha also means hello, goodbye, kindness and compassion'], el: ['αγάπη', 'agápi', 'also selfless love (agape)'], ru: ['любовь', "lyubov'"], he: ['אהבה', 'ahava'], ar: ['حب', 'hubb'],
      hi: ['प्यार', 'pyaar', 'also प्रेम (prem)'], zh: ['爱', 'ài', 'traditional 愛; also to like, to cherish'], ja: ['愛', 'ai'], ko: ['사랑', 'sarang'] }],
    ['I love you', ['I love you'], 'romance wedding phrase', { es: ['Te amo', 'teh AH-mo', 'Te quiero (teh KYEH-ro) is softer, also used for family and friends'], fr: ["Je t'aime", 'zhuh TEM'], it: ['Ti amo', 'tee AH-mo'],
      pt: ['Eu te amo', 'EH-oo chee AH-moo'], de: ['Ich liebe dich', 'ikh LEE-beh dikh'], la: ['Te amo', 'te amo'], ga: ['Tá grá agam duit', 'taw graw AH-gum ditch'], el: ["Σ' αγαπώ", "s'agapó"], ru: ['Я тебя люблю', 'ya tebya lyublyu'],
      he: ['אני אוהב אותך', 'ani ohev otach', 'said by a man; a woman says אני אוהבת אותך (ani ohevet otcha)'], ar: ['أحبك', 'uhibbuk', 'the same letters to a man or a woman; said uhibbuka to a man, uhibbuki to a woman'],
      hi: ['मैं तुमसे प्यार करता हूँ', 'main tumse pyaar karta hoon', 'said by a man; a woman says करती हूँ (karti hoon)'], zh: ['我爱你', 'wǒ ài nǐ'], ja: ['愛してる', 'aishiteru'], ko: ['사랑해', 'saranghae'] }],
    ['mother', ['mother', 'mom', 'mum', 'mama'], 'family mom mum', { es: ['mamá', 'ma-MA', 'madre (MA-dreh) is mother'], fr: ['maman', 'ma-MAN', 'mère (mair) is mother'], it: ['mamma', 'MAM-ma', 'madre is mother'],
      pt: ['mãe', 'my'], de: ['Mama', 'MA-ma', 'Mutter is mother'], la: ['mater', 'mater'], ga: ['máthair', 'MAW-her', 'mam is mom'], el: ['μαμά', 'mamá', 'μητέρα (mitéra) is mother'], ru: ['мама', 'mama', 'мать (mat\') is mother'],
      he: ['אמא', 'ima'], ar: ['أم', 'umm', 'ماما (mama) is mom'], hi: ['माँ', 'maa'], zh: ['妈妈', 'māma', 'traditional 媽媽; 母亲 (mǔqīn) is mother'], ja: ['お母さん', 'okaasan', '母 (haha) is my mother'], ko: ['엄마', 'eomma', '어머니 (eomeoni) is mother'] }],
    ['father', ['father', 'dad', 'daddy', 'papa'], 'family dad', { es: ['papá', 'pa-PA', 'padre is father'], fr: ['papa', 'pa-PA', 'père (pair) is father'], it: ['papà', 'pa-PA', 'padre is father'], pt: ['pai', 'pie'],
      de: ['Papa', 'PA-pa', 'Vater (FA-ter) is father'], la: ['pater', 'pater'], ga: ['athair', 'AH-her', 'daid is dad'], el: ['μπαμπάς', 'bampás', 'πατέρας (patéras) is father'], ru: ['папа', 'papa', 'отец (otets) is father'],
      he: ['אבא', 'aba'], ar: ['أب', 'ab', 'بابا (baba) is dad'], hi: ['पापा', 'papa', 'पिता (pita) is father'], zh: ['爸爸', 'bàba', '父亲 (fùqīn) is father'], ja: ['お父さん', 'otousan', '父 (chichi) is my father'], ko: ['아빠', 'appa', '아버지 (abeoji) is father'] }],
    ['family', ['family'], 'family', { es: ['familia', 'fa-MEE-lya'], fr: ['famille', 'fa-MEE'], it: ['famiglia', 'fa-MEE-lya'], pt: ['família', 'fa-MEE-lya'], de: ['Familie', 'fa-MEE-lyeh'], la: ['familia', 'familia'], ga: ['clann', 'klown'],
      haw: ['ʻohana', 'oh-HA-na', 'family in the widest sense: those you choose too'], el: ['οικογένεια', 'oikogéneia'], ru: ['семья', "sem'ya"], he: ['משפחה', 'mishpacha'], ar: ['عائلة', "'a'ila"], hi: ['परिवार', 'parivaar'],
      zh: ['家人', 'jiārén', '家 (jiā) is home and family'], ja: ['家族', 'kazoku'], ko: ['가족', 'gajok'] }],
    ['friend', ['friend'], 'friendship', { es: ['amigo', 'ah-MEE-go', 'amiga for a woman'], fr: ['ami', 'ah-MEE', 'amie for a woman'], it: ['amico', 'ah-MEE-ko', 'amica for a woman'], pt: ['amigo', 'ah-MEE-go', 'amiga for a woman'],
      de: ['Freund', 'froynt', 'Freundin for a woman; also boyfriend / girlfriend'], la: ['amicus', 'amicus', 'amica for a woman'], ga: ['cara', 'KA-ra'], el: ['φίλος', 'fílos', 'φίλη (fíli) for a woman'], ru: ['друг', 'drug', 'подруга (podruga) for a woman'],
      he: ['חבר', 'chaver', 'חברה (chavera) for a woman; also boyfriend / girlfriend'], ar: ['صديق', 'sadiq', 'صديقة (sadiqa) for a woman'], hi: ['दोस्त', 'dost'], zh: ['朋友', 'péngyou'], ja: ['友達', 'tomodachi', '友 (tomo) alone is also friend'], ko: ['친구', 'chingu'] }],
    ['best friends', ['best friends'], 'friendship phrase bff', { es: ['mejores amigas', 'meh-HO-res ah-MEE-gas', 'mejores amigos for men or a mix'], fr: ['meilleures amies', 'meh-YUR za-MEE', 'meilleurs amis for men or a mix'],
      it: ['migliori amiche', 'mee-LYO-ree ah-MEE-keh', 'migliori amici for men or a mix'], pt: ['melhores amigas', 'meh-LYO-res ah-MEE-gas', 'melhores amigos for men or a mix'], de: ['beste Freundinnen', 'BES-teh FROYN-din-en', 'beste Freunde for men or a mix'] }],
    ['soul friend', ['soul friend', 'a friend of the soul'], 'friendship phrase irish', { ga: ['anam cara', 'AH-num KA-ra'] }],
    ['hope', ['hope'], 'faith', { es: ['esperanza', 'es-peh-RAN-sa'], fr: ['espoir', 'es-PWAR'], it: ['speranza', 'speh-RAN-tsa'], pt: ['esperança', 'es-peh-RAN-sa'], de: ['Hoffnung', 'HOF-noong'], la: ['spes', 'spes'],
      ga: ['dóchas', 'DOH-khus'], el: ['ελπίδα', 'elpída'], ru: ['надежда', 'nadezhda'], he: ['תקווה', 'tikva'], ar: ['أمل', 'amal'], hi: ['आशा', 'aasha', 'also उम्मीद (ummeed)'], zh: ['希望', 'xīwàng'], ja: ['希望', 'kibou'], ko: ['희망', 'huimang'] }],
    ['faith', ['faith', 'belief', 'trust'], 'faith religion', { es: ['fe', 'feh'], fr: ['foi', 'fwa'], it: ['fede', 'FEH-deh'], pt: ['fé', 'feh'], de: ['Glaube', 'GLOW-beh'], la: ['fides', 'fides'], ga: ['creideamh', 'KREH-jiv'],
      el: ['πίστη', 'písti'], ru: ['вера', 'vera'], he: ['אמונה', 'emuna'], ar: ['إيمان', 'iman'], hi: ['विश्वास', 'vishvaas', 'also आस्था (aastha)'], zh: ['信仰', 'xìnyǎng', '信 (xìn) alone: trust, faith, a letter'], ja: ['信仰', 'shinkou'], ko: ['믿음', 'mideum'] }],
    ['peace', ['peace'], 'calm', { es: ['paz', 'pas'], fr: ['paix', 'peh'], it: ['pace', 'PA-cheh'], pt: ['paz', 'pas'], de: ['Frieden', 'FREE-den'], la: ['pax', 'pax'], ga: ['síocháin', 'SHEE-uh-khawn'],
      el: ['ειρήνη', 'eiríni'], ru: ['мир', 'mir', 'мир also means world'], he: ['שלום', 'shalom', 'shalom also means hello and goodbye, and wholeness'], ar: ['سلام', 'salam', 'also a greeting'], hi: ['शांति', 'shaanti'],
      zh: ['和平', 'hépíng', '和 (hé) alone: harmony, peace, gentle, and'], ja: ['平和', 'heiwa'], ko: ['평화', 'pyeonghwa'] }],
    ['strength', ['strength', 'power', 'force'], 'strong', { es: ['fuerza', 'FWER-sa'], fr: ['force', 'fors'], it: ['forza', 'FOR-tsa'], pt: ['força', 'FOR-sa'], de: ['Stärke', 'SHTER-keh', 'also Kraft'], la: ['fortitudo', 'fortitudo', 'vis is force'],
      ga: ['neart', 'nyart'], el: ['δύναμη', 'dýnami'], ru: ['сила', 'sila'], he: ['כוח', 'koach'], ar: ['قوة', 'quwwa'], hi: ['शक्ति', 'shakti', 'also ताकत (taakat)'], zh: ['力量', 'lìliàng', '力 (lì) alone is strength, power'],
      ja: ['力', 'chikara'], ko: ['힘', 'him'] }],
    ['courage', ['courage', 'bravery'], 'brave', { es: ['valor', 'va-LOR', 'also coraje'], fr: ['courage', 'koo-RAZH'], it: ['coraggio', 'ko-RAD-jo'], pt: ['coragem', 'ko-RA-zhem'], de: ['Mut', 'moot'], la: ['virtus', 'virtus', 'also manly worth, excellence'],
      el: ['θάρρος', 'thárros'], ru: ['смелость', "smelost'", 'also мужество (muzhestvo)'], he: ['אומץ', 'ometz'], ar: ['شجاعة', "shaja'a"], hi: ['साहस', 'saahas', 'also हिम्मत (himmat)'], zh: ['勇气', 'yǒngqì', 'traditional 勇氣; 勇 (yǒng) alone: brave'],
      ja: ['勇気', 'yuuki'], ko: ['용기', 'yonggi'] }],
    ['forever', ['forever', 'eternally', 'for always'], 'romance eternity', { es: ['para siempre', 'PA-ra SYEM-preh'], fr: ['pour toujours', 'poor too-ZHOOR'], it: ['per sempre', 'pehr SEM-preh'], pt: ['para sempre', 'PA-ra SEM-preh'],
      de: ['für immer', 'fuer IM-er'], la: ['in aeternum', 'in aeternum'], ga: ['go deo', 'guh JOH'], el: ['για πάντα', 'gia pánta'], ru: ['навсегда', 'navsegda'], he: ['לנצח', 'lanetzach'], ar: ['إلى الأبد', 'ila al-abad'],
      hi: ['हमेशा के लिए', 'hamesha ke liye'], zh: ['永远', 'yǒngyuǎn', 'traditional 永遠'], ja: ['永遠', 'eien', 'eternity; 永遠に (eien ni) is forever'], ko: ['영원히', 'yeongwonhi'] }],
    ['always', ['always'], 'romance', { es: ['siempre', 'SYEM-preh'], fr: ['toujours', 'too-ZHOOR'], it: ['sempre', 'SEM-preh'], pt: ['sempre', 'SEM-preh'], de: ['immer', 'IM-er'], la: ['semper', 'semper'], ga: ['i gcónaí', 'ih GOH-nee'],
      el: ['πάντα', 'pánta'], ru: ['всегда', 'vsegda'], he: ['תמיד', 'tamid'], ar: ['دائما', "da'iman"], hi: ['हमेशा', 'hamesha'], zh: ['永远', 'yǒngyuǎn', 'also forever'], ja: ['いつも', 'itsumo'], ko: ['항상', 'hangsang', 'also 언제나 (eonjena)'] }],
    ['happiness', ['happiness', 'joy', 'bliss'], 'happy joy', { es: ['felicidad', 'feh-lee-see-DAD', 'alegría is joy'], fr: ['bonheur', 'bo-NUR', 'joie is joy'], it: ['felicità', 'feh-lee-chee-TA', 'gioia is joy'], pt: ['felicidade', 'feh-lee-see-DA-jee', 'alegria is joy'],
      de: ['Glück', 'gluek', 'also luck; Freude is joy'], la: ['felicitas', 'felicitas', 'gaudium is joy'], el: ['ευτυχία', 'eftychía', 'χαρά (chará) is joy'], ru: ['счастье', "schast'ye", 'радость (radost\') is joy'],
      he: ['אושר', 'osher', 'שמחה (simcha) is joy'], ar: ['سعادة', "sa'ada", 'فرح (farah) is joy'], hi: ['खुशी', 'khushi', 'also आनंद (aanand), bliss'], zh: ['幸福', 'xìngfú', '喜 (xǐ) alone: joy, happiness'],
      ja: ['幸せ', 'shiawase', '喜び (yorokobi) is joy'], ko: ['행복', 'haengbok', '기쁨 (gippeum) is joy'] }],
    ['luck', ['luck', 'good fortune'], 'lucky fortune', { es: ['suerte', 'SWER-teh'], fr: ['chance', 'shans'], it: ['fortuna', 'for-TOO-na'], pt: ['sorte', 'SOR-chee'], de: ['Glück', 'gluek', 'also happiness'], la: ['fortuna', 'fortuna'],
      ga: ['ádh', 'aw'], el: ['τύχη', 'týchi'], ru: ['удача', 'udacha'], he: ['מזל', 'mazal', 'מזל טוב (mazal tov) is congratulations'], ar: ['حظ', 'hazz'], hi: ['भाग्य', 'bhaagya', 'also किस्मत (kismat), fate'],
      zh: ['福', 'fú', 'good fortune, blessing, happiness: hung upside down at New Year'], ja: ['幸運', 'kouun'], ko: ['행운', 'haengun'] }],
    ['blessed', ['blessed'], 'faith religion', { es: ['bendecida', 'ben-deh-SEE-da', 'bendecido for a man'], fr: ['bénie', 'beh-NEE', 'béni for a man'], it: ['benedetta', 'beh-neh-DET-ta', 'benedetto for a man'],
      pt: ['abençoada', 'ah-ben-so-AH-da', 'abençoado for a man'], de: ['gesegnet', 'geh-ZEG-net'], la: ['beata', 'beata', 'beatus for a man'], el: ['ευλογημένη', 'evlogiméni', 'ευλογημένος (evlogiménos) for a man'],
      he: ['מבורכת', 'mevorechet', 'מבורך (mevorach) for a man'], ar: ['مبارك', 'mubarak'], hi: ['धन्य', 'dhanya'] }],
    ['grace', ['grace', 'favour', 'elegance'], 'faith', { es: ['gracia', 'GRA-sya'], fr: ['grâce', 'grahs'], it: ['grazia', 'GRA-tsya'], pt: ['graça', 'GRA-sa'], de: ['Gnade', 'GNA-deh', 'Anmut is elegance'], la: ['gratia', 'gratia'],
      el: ['χάρη', 'chári'], he: ['חסד', 'chesed', 'loving-kindness; חן (chen) is charm'], ar: ['نعمة', "ni'ma"], hi: ['कृपा', 'kripa'], zh: ['恩典', 'ēndiǎn'], ja: ['恵み', 'megumi'], ko: ['은혜', 'eunhye'] }],
    ['angel', ['angel'], 'faith memorial', { es: ['ángel', 'AN-hel'], fr: ['ange', 'anzh'], it: ['angelo', 'AN-jeh-lo'], pt: ['anjo', 'AN-zho'], de: ['Engel', 'ENG-el'], la: ['angelus', 'angelus'], ga: ['aingeal', 'ANG-gyal'],
      el: ['άγγελος', 'ángelos'], ru: ['ангел', 'angel'], he: ['מלאך', "mal'ach"], ar: ['ملاك', 'malak'], hi: ['फ़रिश्ता', 'farishta'], zh: ['天使', 'tiānshǐ'], ja: ['天使', 'tenshi'], ko: ['천사', 'cheonsa'] }],
    ['sister', ['sister'], 'family', { es: ['hermana', 'er-MA-na'], fr: ['sœur', 'sur'], it: ['sorella', 'so-REL-la'], pt: ['irmã', 'eer-MAN'], de: ['Schwester', 'SHVES-ter'], la: ['soror', 'soror'], ga: ['deirfiúr', 'DREH-fyoor'],
      el: ['αδελφή', 'adelfí'], ru: ['сестра', 'sestra'], he: ['אחות', 'achot'], ar: ['أخت', 'ukht'], hi: ['बहन', 'behen'], zh: ['姐妹', 'jiěmèi', 'sisters; 姐姐 (jiějie) older sister, 妹妹 (mèimei) younger sister'],
      ja: ['姉妹', 'shimai', 'sisters; 姉 (ane) older, 妹 (imouto) younger'], ko: ['자매', 'jamae', 'sisters; 언니 (eonni) a girl\'s older sister, 누나 (nuna) a boy\'s older sister'] }],
    ['brother', ['brother'], 'family', { es: ['hermano', 'er-MA-no'], fr: ['frère', 'frair'], it: ['fratello', 'fra-TEL-lo'], pt: ['irmão', 'eer-MOWN'], de: ['Bruder', 'BROO-der'], la: ['frater', 'frater'], ga: ['deartháir', 'DRAH-her'],
      el: ['αδελφός', 'adelfós'], ru: ['брат', 'brat'], he: ['אח', 'ach'], ar: ['أخ', 'akh'], hi: ['भाई', 'bhai'], zh: ['兄弟', 'xiōngdì', 'brothers; 哥哥 (gēge) older, 弟弟 (dìdi) younger'],
      ja: ['兄弟', 'kyoudai', 'brothers; 兄 (ani) older, 弟 (otouto) younger'], ko: ['형제', 'hyeongje', 'brothers; 형 (hyeong) a boy\'s older brother, 오빠 (oppa) a girl\'s older brother'] }],
    ['son', ['son'], 'family', { es: ['hijo', 'EE-ho'], fr: ['fils', 'fees'], it: ['figlio', 'FEE-lyo'], pt: ['filho', 'FEE-lyo'], de: ['Sohn', 'zohn'], la: ['filius', 'filius'], ga: ['mac', 'mok'], el: ['γιος', 'gios'],
      ru: ['сын', 'syn'], he: ['בן', 'ben'], ar: ['ابن', 'ibn'], hi: ['बेटा', 'beta'], zh: ['儿子', 'érzi'], ja: ['息子', 'musuko'], ko: ['아들', 'adeul'] }],
    ['daughter', ['daughter'], 'family', { es: ['hija', 'EE-ha'], fr: ['fille', 'fee', 'also girl'], it: ['figlia', 'FEE-lya'], pt: ['filha', 'FEE-lya'], de: ['Tochter', 'TOKH-ter'], la: ['filia', 'filia'], ga: ['iníon', 'in-EEN'],
      el: ['κόρη', 'kóri'], ru: ['дочь', "doch'"], he: ['בת', 'bat'], ar: ['ابنة', 'ibna', 'also بنت (bint), girl'], hi: ['बेटी', 'beti'], zh: ['女儿', "nǚ'ér"], ja: ['娘', 'musume'], ko: ['딸', 'ttal'] }],
    ['grandmother', ['grandmother', 'grandma', 'nana'], 'family grandma', { es: ['abuela', 'ah-BWEH-la'], fr: ['mamie', 'ma-MEE', 'grand-mère is grandmother'], it: ['nonna', 'NON-na'], pt: ['avó', 'ah-VAW'], de: ['Oma', 'O-ma', 'Großmutter is grandmother'],
      la: ['avia', 'avia'], el: ['γιαγιά', 'giagiá'], ru: ['бабушка', 'babushka'], he: ['סבתא', 'savta'], ar: ['جدة', 'jadda', 'تيتا (teta) is grandma'],
      hi: ['दादी', 'daadi', 'father\'s mother; नानी (naani) is mother\'s mother'], zh: ['奶奶', 'nǎinai', 'father\'s mother; 外婆 (wàipó) is mother\'s mother'], ja: ['おばあちゃん', 'obaachan'], ko: ['할머니', 'halmeoni'] }],
    ['grandfather', ['grandfather', 'grandpa'], 'family grandpa', { es: ['abuelo', 'ah-BWEH-lo'], fr: ['papi', 'pa-PEE', 'grand-père is grandfather'], it: ['nonno', 'NON-no'], pt: ['avô', 'ah-VO'], de: ['Opa', 'O-pa', 'Großvater is grandfather'],
      la: ['avus', 'avus'], el: ['παππούς', 'pappoús'], ru: ['дедушка', 'dedushka'], he: ['סבא', 'saba'], ar: ['جد', 'jadd', 'جدو (jiddo) is grandpa'],
      hi: ['दादा', 'daada', 'father\'s father; नाना (naana) is mother\'s father'], zh: ['爷爷', 'yéye', 'father\'s father; 外公 (wàigōng) is mother\'s father'], ja: ['おじいちゃん', 'ojiichan'], ko: ['할아버지', 'harabeoji'] }],
    ['baby', ['baby'], 'family', { es: ['bebé', 'beh-BEH'], fr: ['bébé', 'beh-BEH'], it: ['bambino', 'bam-BEE-no', 'bambina for a girl; also child'], pt: ['bebê', 'beh-BEH'], de: ['Baby', 'BAY-bee'], la: ['infans', 'infans'],
      el: ['μωρό', 'moró'], ru: ['малыш', 'malysh'], he: ['תינוק', 'tinok', 'תינוקת (tinoket) for a girl'], ar: ['طفل', 'tifl', 'also child'], hi: ['बच्चा', 'bachcha', 'also child'], zh: ['宝宝', 'bǎobao', 'also darling'],
      ja: ['赤ちゃん', 'akachan'], ko: ['아기', 'agi'] }],
    ['home', ['home', 'house'], 'family', { es: ['hogar', 'o-GAR', 'casa is house'], fr: ['maison', 'meh-ZON', 'also foyer, the hearth'], it: ['casa', 'KA-za'], pt: ['lar', 'lar', 'casa is house'], de: ['Zuhause', 'tsoo-HOW-zeh', 'Heim is home'],
      la: ['domus', 'domus'], ga: ['baile', 'BAL-yeh', 'also town'], el: ['σπίτι', 'spíti'], ru: ['дом', 'dom'], he: ['בית', 'bayit'], ar: ['بيت', 'bayt'], hi: ['घर', 'ghar'],
      zh: ['家', 'jiā', 'home, family, house'], ja: ['家', 'ie', 'also read uchi, my home'], ko: ['집', 'jip'] }],
    ['life', ['life'], 'live', { es: ['vida', 'VEE-da'], fr: ['vie', 'vee'], it: ['vita', 'VEE-ta'], pt: ['vida', 'VEE-da'], de: ['Leben', 'LAY-ben'], la: ['vita', 'vita'], ga: ['saol', 'seel'], el: ['ζωή', 'zoí'],
      ru: ['жизнь', "zhizn'"], he: ['חיים', 'chayim', 'חי (chai) alone: alive, and the number 18'], ar: ['حياة', 'hayah'], hi: ['जीवन', 'jeevan', 'also ज़िंदगी (zindagi)'], zh: ['生命', 'shēngmìng', '生 (shēng) alone: life, birth, to grow'],
      ja: ['人生', 'jinsei', 'one\'s life; 命 (inochi) is life itself'], ko: ['인생', 'insaeng', 'also 삶 (sam)'] }],
    ['dream', ['dream'], 'dreams', { es: ['sueño', 'SWEH-nyo', 'also sleep'], fr: ['rêve', 'rev'], it: ['sogno', 'SO-nyo'], pt: ['sonho', 'SO-nyo'], de: ['Traum', 'trowm'], la: ['somnium', 'somnium'], ga: ['aisling', 'ASH-ling', 'also a vision'],
      el: ['όνειρο', 'óneiro'], ru: ['мечта', 'mechta', 'a dream you wish for; сон (son) is a dream while asleep'], he: ['חלום', 'chalom'], ar: ['حلم', 'hulm'], hi: ['सपना', 'sapna'],
      zh: ['梦', 'mèng', 'traditional 夢; 梦想 (mèngxiǎng) is a dream you wish for'], ja: ['夢', 'yume'], ko: ['꿈', 'kkum'] }],
    ['believe', ['believe'], 'faith phrase', { es: ['cree', 'KREH-eh'], fr: ['crois', 'krwa'], it: ['credi', 'KREH-dee'], pt: ['acredite', 'ah-kreh-DEE-chee'], de: ['glaube', 'GLOW-beh'], la: ['crede', 'crede'],
      el: ['πίστεψε', 'pístepse'], ru: ['верь', "ver'"], he: ['להאמין', "leha'amin", 'to believe'], ar: ['آمن', 'amin'], hi: ['विश्वास करो', 'vishvaas karo'], zh: ['相信', 'xiāngxìn'], ja: ['信じて', 'shinjite'], ko: ['믿어', 'mideo'] }],
    ['beautiful', ['beautiful', 'pretty'], 'beauty', { es: ['hermosa', 'er-MO-sa', 'hermoso for a man or a thing'], fr: ['belle', 'bel', 'beau for a man or a thing'], it: ['bella', 'BEL-la', 'bello for a man or a thing'],
      pt: ['linda', 'LEEN-da', 'lindo for a man or a thing'], de: ['schön', 'shoen'], la: ['pulchra', 'pulchra', 'pulcher for a man'], ga: ['álainn', 'AW-lin'], el: ['όμορφη', 'ómorfi', 'όμορφος for a man'],
      ru: ['красивая', 'krasivaya', 'красивый for a man'], he: ['יפה', 'yafa', 'said yafe for a man (same letters)'], ar: ['جميلة', 'jamila', 'جميل (jamil) for a man'], hi: ['सुंदर', 'sundar'],
      zh: ['美丽', 'měilì', '美 (měi) alone: beautiful, beauty'], ja: ['美しい', 'utsukushii', '美 (bi) alone: beauty'], ko: ['아름다운', 'areumdaun'] }],
    ['heart', ['heart', 'mind', 'spirit', 'feeling'], 'love', { es: ['corazón', 'ko-ra-SON'], fr: ['cœur', 'kur'], it: ['cuore', 'KWO-reh'], pt: ['coração', 'ko-ra-SOWN'], de: ['Herz', 'herts'], la: ['cor', 'cor'],
      ga: ['croí', 'kree', 'mo chroí (muh khree): my heart, my darling'], el: ['καρδιά', 'kardiá'], ru: ['сердце', 'serdtse'], he: ['לב', 'lev'], ar: ['قلب', 'qalb'], hi: ['दिल', 'dil', 'also हृदय (hriday)'],
      zh: ['心', 'xīn', 'heart, mind, feeling, centre'], ja: ['心', 'kokoro', 'heart, mind, spirit'], ko: ['마음', 'maeum', 'heart and mind; 심장 (simjang) is the organ'] }],
    ['soul', ['soul', 'spirit'], 'spirit', { es: ['alma', 'AL-ma'], fr: ['âme', 'ahm'], it: ['anima', 'AH-nee-ma'], pt: ['alma', 'AL-ma'], de: ['Seele', 'ZAY-leh'], la: ['anima', 'anima'], ga: ['anam', 'AH-num'],
      el: ['ψυχή', 'psychí', 'also mind, life'], ru: ['душа', 'dusha'], he: ['נשמה', 'neshama', 'also breath'], ar: ['روح', 'ruh', 'also spirit'], hi: ['आत्मा', 'aatma'], zh: ['灵魂', 'línghún'], ja: ['魂', 'tamashii'], ko: ['영혼', 'yeonghon'] }],
    ['light', ['light'], 'shine', { es: ['luz', 'loos'], fr: ['lumière', 'loo-MYAIR'], it: ['luce', 'LOO-cheh'], pt: ['luz', 'loos'], de: ['Licht', 'likht'], la: ['lux', 'lux'], ga: ['solas', 'SUL-us'], el: ['φως', 'fos'],
      ru: ['свет', 'svet', 'also world'], he: ['אור', 'or'], ar: ['نور', 'nur'], hi: ['रोशनी', 'roshni', 'also प्रकाश (prakaash)'], zh: ['光', 'guāng', 'also glory, bare'], ja: ['光', 'hikari'], ko: ['빛', 'bit'] }],
    ['star', ['star'], 'sky', { es: ['estrella', 'es-TREH-ya'], fr: ['étoile', 'eh-TWAL'], it: ['stella', 'STEL-la'], pt: ['estrela', 'es-TREH-la'], de: ['Stern', 'shtern'], la: ['stella', 'stella'], ga: ['réalta', 'RAYL-ta'],
      el: ['αστέρι', 'astéri'], ru: ['звезда', 'zvezda'], he: ['כוכב', 'kochav'], ar: ['نجمة', 'najma'], hi: ['तारा', 'taara'], zh: ['星', 'xīng'], ja: ['星', 'hoshi'], ko: ['별', 'byeol'] }],
    ['moon', ['moon', 'month'], 'sky', { es: ['luna', 'LOO-na'], fr: ['lune', 'loon'], it: ['luna', 'LOO-na'], pt: ['lua', 'LOO-a'], de: ['Mond', 'mohnt'], la: ['luna', 'luna'], ga: ['gealach', 'GYA-lukh'], el: ['φεγγάρι', 'fengári'],
      ru: ['луна', 'luna'], he: ['ירח', 'yareach'], ar: ['قمر', 'qamar'], hi: ['चाँद', 'chaand'], zh: ['月亮', 'yuèliang', '月 (yuè) alone: moon, month'], ja: ['月', 'tsuki', 'moon, month'], ko: ['달', 'dal', 'moon, month'] }],
    ['sun', ['sun', 'day'], 'sky', { es: ['sol', 'sol'], fr: ['soleil', 'so-LAY'], it: ['sole', 'SO-leh'], pt: ['sol', 'sow'], de: ['Sonne', 'ZON-eh'], la: ['sol', 'sol'], ga: ['grian', 'GREE-un'], el: ['ήλιος', 'ílios'],
      ru: ['солнце', 'solntse'], he: ['שמש', 'shemesh'], ar: ['شمس', 'shams'], hi: ['सूरज', 'sooraj'], zh: ['太阳', 'tàiyáng', '日 (rì) alone: sun, day'], ja: ['太陽', 'taiyou', '日 (hi) alone: sun, day'], ko: ['해', 'hae', 'sun, year'] }],
    ['freedom', ['freedom', 'liberty'], 'free', { es: ['libertad', 'lee-ber-TAD'], fr: ['liberté', 'lee-ber-TEH'], it: ['libertà', 'lee-ber-TA'], pt: ['liberdade', 'lee-ber-DA-jee'], de: ['Freiheit', 'FRY-hite'], la: ['libertas', 'libertas'],
      ga: ['saoirse', 'SEER-sheh'], el: ['ελευθερία', 'eleftheria'], ru: ['свобода', 'svoboda'], he: ['חופש', 'chofesh', 'also holiday; חירות (cherut) is liberty'], ar: ['حرية', 'hurriyya'], hi: ['आज़ादी', 'aazaadi'],
      zh: ['自由', 'zìyóu'], ja: ['自由', 'jiyuu'], ko: ['자유', 'jayu'] }],
    ['warrior', ['warrior', 'fighter'], 'strong brave', { es: ['guerrera', 'geh-RREH-ra', 'guerrero for a man'], fr: ['guerrière', 'geh-RYAIR', 'guerrier for a man'], it: ['guerriera', 'geh-RRYEH-ra', 'guerriero for a man'],
      pt: ['guerreira', 'geh-HAY-ra', 'guerreiro for a man'], de: ['Kriegerin', 'KREE-geh-rin', 'Krieger for a man'], la: ['bellatrix', 'bellatrix', 'bellator for a man'], el: ['πολεμίστρια', 'polemístria', 'πολεμιστής for a man'],
      ru: ['воин', 'voin'], he: ['לוחמת', 'lochemet', 'לוחם (lochem) for a man'], ar: ['محاربة', 'muhariba', 'محارب (muharib) for a man'], hi: ['योद्धा', 'yoddha'], zh: ['战士', 'zhànshì'],
      ja: ['戦士', 'senshi', '侍 (samurai) is a samurai'], ko: ['전사', 'jeonsa'] }],
    ['thank you', ['thank you', 'thanks', 'gratitude'], 'phrase', { es: ['gracias', 'GRA-syas'], fr: ['merci', 'mer-SEE'], it: ['grazie', 'GRA-tsyeh'], pt: ['obrigada', 'o-bree-GA-da', 'said by a woman; a man says obrigado'],
      de: ['danke', 'DAN-keh'], la: ['gratias', 'gratias'], ga: ['go raibh maith agat', 'guh rev mah AH-gut'], haw: ['mahalo', 'ma-HA-lo'], el: ['ευχαριστώ', 'efcharistó'], ru: ['спасибо', 'spasibo'], he: ['תודה', 'toda'],
      ar: ['شكرا', 'shukran'], hi: ['धन्यवाद', 'dhanyavaad'], zh: ['谢谢', 'xièxie'], ja: ['ありがとう', 'arigatou'], ko: ['감사합니다', 'gamsahamnida'] }],
    ['hello', ['hello', 'greetings'], 'phrase', { es: ['hola', 'O-la'], fr: ['bonjour', 'bon-ZHOOR'], it: ['ciao', 'chow', 'also goodbye'], pt: ['olá', 'o-LA'], de: ['hallo', 'HA-lo'], la: ['salve', 'salve'], ga: ['dia duit', 'JEE-a ditch', 'God be with you'],
      haw: ['aloha', 'ah-LO-ha', 'also love, goodbye'], el: ['γεια σου', 'geia sou'], ru: ['привет', 'privet'], he: ['שלום', 'shalom', 'also peace, goodbye'], ar: ['مرحبا', 'marhaban'],
      hi: ['नमस्ते', 'namaste', 'I bow to you: hello and goodbye'], zh: ['你好', 'nǐ hǎo'], ja: ['こんにちは', 'konnichiwa'], ko: ['안녕하세요', 'annyeonghaseyo'] }],
    ['in loving memory', ['in loving memory', 'in memory of', 'remembered'], 'memorial phrase', { es: ['en memoria de', 'en meh-MO-rya deh'], fr: ['à la mémoire de', 'a la meh-MWAR duh'], it: ['in memoria di', 'een meh-MO-rya dee'],
      pt: ['em memória de', 'eng meh-MO-rya jee'], de: ['in liebevoller Erinnerung', 'in LEE-beh-fol-er er-IN-er-oong'], la: ['in memoriam', 'in memoriam'], he: ['לזכר', 'lezecher', 'in memory of; ז"ל after a name: may his/her memory be a blessing'],
      ar: ['في ذكرى', 'fi dhikra'], zh: ['永远怀念', 'yǒngyuǎn huáiniàn', 'forever remembered'] }],
    ['live laugh love', ['live, laugh, love'], 'phrase', { es: ['vive, ríe, ama', 'VEE-veh, REE-eh, AH-ma'], fr: ['vis, ris, aime', 'vee, ree, em'], it: ['vivi, ridi, ama', 'VEE-vee, REE-dee, AH-ma'], pt: ['viva, ria, ame', 'VEE-va, HEE-a, AH-mee'] }],
    ['I am my beloved\'s', ["I am my beloved's and my beloved is mine"], 'wedding romance phrase bible song of songs', { he: ['אני לדודי ודודי לי', "ani l'dodi v'dodi li", 'Song of Songs 6:3, a wedding ring favourite'] }],
    ['seize the day', ['seize the day', 'make the most of today'], 'motto phrase latin', { la: ['carpe diem', 'carpe diem'] }],
    ['love conquers all', ['love conquers all'], 'motto phrase latin romance', { la: ['amor vincit omnia', 'amor vincit omnia'] }],
    ['always faithful', ['always faithful', 'always loyal'], 'motto phrase latin military marines', { la: ['semper fidelis', 'semper fidelis', 'the US Marines\' motto'] }],
    ['through hardships to the stars', ['through hardships to the stars'], 'motto phrase latin', { la: ['per aspera ad astra', 'per aspera ad astra'] }],
    ['she flies with her own wings', ['she flies with her own wings'], 'motto phrase latin', { la: ['alis volat propriis', 'alis volat propriis', 'Oregon\'s state motto'] }],
    ['remember you will die', ['remember that you will die'], 'motto phrase latin', { la: ['memento mori', 'memento mori'] }],
    ['God willing', ['God willing', 'hopefully'], 'faith phrase religion', { ar: ['إن شاء الله', "in sha' Allah"] }],
    ['praise be to God', ['praise be to God', 'thank God'], 'faith phrase religion', { ar: ['الحمد لله', 'al-hamdu lillah'] }],
    ['in the name of God', ['in the name of God'], 'faith phrase religion', { ar: ['بسم الله', 'bismillah'] }],
    ['safe and sound', ['peace and safety', 'safe and sound', 'a safe journey'], 'phrase blessing', { zh: ['平安', "píng'ān"] }],
    ['for this whole life', ['for this whole life', 'forever, lifetime after lifetime'], 'romance wedding phrase', { zh: ['一生一世', 'yìshēng yíshì'] }],
    ['you can do it', ['you can do it', 'keep going', 'add fuel'], 'phrase encouragement', { zh: ['加油', 'jiāyóu'], ko: ['화이팅', 'hwaiting'] }],
    ['once in a lifetime', ['one time, one meeting', 'treasure every moment, it will never come again'], 'motto phrase', { ja: ['一期一会', 'ichigo ichie'] }],
    ['fall seven times, stand up eight', ['fall seven times, stand up eight', 'never give up'], 'motto phrase strength', { ja: ['七転び八起き', 'nana korobi ya oki'] }],
    ['I love you forever', ['I love you forever'], 'romance phrase', { ko: ['영원히 사랑해', 'yeongwonhi saranghae'] }],
    ['om', ['om', 'the sacred sound'], 'spiritual yoga meditation', { sa: ['ॐ', 'om'] }],
    ['karma', ['karma', 'action', 'deeds and their fruit'], 'spiritual yoga', { sa: ['कर्म', 'karma'] }],
    ['namaste', ['I bow to you', 'hello', 'goodbye'], 'spiritual yoga greeting', { sa: ['नमस्ते', 'namaste'] }],
    ['peace (yoga)', ['peace', 'calm'], 'spiritual yoga meditation', { sa: ['शान्ति', 'shanti'] }],
    ['life (chai)', ['life', 'alive', 'the number 18'], 'faith jewish', { he: ['חי', 'chai', 'also the number 18, a lucky number'] }],
    ['congratulations', ['congratulations', 'good luck'], 'phrase celebration', { he: ['מזל טוב', 'mazal tov'] }],
    ['dragon', ['dragon'], 'animal zodiac', { zh: ['龙', 'lóng', 'traditional 龍; strength, good fortune, the emperor'], ja: ['龍', 'ryuu', 'also written 竜'] }],
    ['tiger', ['tiger'], 'animal zodiac', { zh: ['虎', 'hǔ', 'courage, protection'], ja: ['虎', 'tora'] }],
    ['long life', ['long life', 'longevity', 'birthday'], 'blessing', { zh: ['寿', 'shòu', 'traditional 壽'] }],
    ['double happiness', ['double happiness', 'marriage'], 'wedding blessing', { zh: ['囍', 'shuāngxǐ', 'two 喜 joined: the sign of a wedding'] }],
    ['samurai', ['samurai', 'one who serves'], 'warrior', { ja: ['侍', 'samurai'] }],
    ['bushido', ['the way of the warrior'], 'warrior motto', { ja: ['武士道', 'bushidou'] }]
  ];

  /* ── the flat list a search reads ── */
  const low = s => String(s == null ? '' : s).toLowerCase();
  // a letter's or word's Latin spelling without its accents: "pýstepse" is found by "pistepse"
  const plain = s => low(s).normalize('NFD').replace(/[̀-ͯ]/g, '');
  let memo = null;
  function letters() {
    const out = [], add = (script, lang, list, label) => list.forEach(([text, name, latin], i) => out.push({ id: `l:${script}:${i}`, kind: 'letter', lang, langName: label || script, script, text, roman: latin, name, meanings: [], note: '' }));
    add('Greek', 'el', GREEK, 'Greek'); add('Cyrillic', 'ru', CYRILLIC, 'Russian (Cyrillic)'); add('Hebrew', 'he', HEBREW, 'Hebrew'); add('Arabic', 'ar', ARABIC, 'Arabic'); add('Devanagari', 'hi', DEVANAGARI, 'Hindi (Devanagari)');
    KANA_ROMAN.forEach((r, i) => { out.push({ id: `l:hira:${i}`, kind: 'letter', lang: 'ja', langName: 'Japanese hiragana', script: 'Japanese', text: HIRA[i], roman: r, name: r, meanings: [], note: '' });
      out.push({ id: `l:kata:${i}`, kind: 'letter', lang: 'ja', langName: 'Japanese katakana', script: 'Japanese', text: KATA[i], roman: r, name: r, meanings: [], note: 'katakana: for names and words from other languages' }); });
    add('Korean', 'ko', KOREAN, 'Korean (Hangul)');
    return out;
  }
  function words() {
    const out = [];
    W.forEach(([en, meanings, tags, by], i) => {
      for (const [lang, v] of Object.entries(by)) {
        const L = LANGS[lang] || [lang, 'Latin'];
        out.push({ id: `w:${i}:${lang}`, kind: 'word', lang, langName: L[0], script: L[1], text: v[0], roman: v[1] || '', meanings: meanings.slice(), en, tags, note: v[2] || '' });
      }
    });
    return out;
  }
  function entries() {
    if (memo) return memo;
    memo = letters().concat(words());
    for (const e of memo) e.hay = [e.en, e.meanings.join(' '), e.name, e.roman, e.langName, e.script, e.tags, e.note].map(plain).join(' \u0001 ') + ' \u0001 ' + low(e.text);
    return memo;
  }
  return { LANGS, SCRIPTS, letters, words, entries, plain, version: 1 };
});
