/* THE WORDS OF EACH PIECE of a line of quantity N (PIECES, Paul, 16 Oct 2026, order 4195059483: "Customer ordered two of the same Charm ... sometimes customers will request different engraving for each
 * piece, even though the two pieces are the same."). A line of quantity 2 is two pieces, each with its OWN words, its own fit, its own Approve. This file answers the one question that decides what each piece starts with:
 *
 *     the buyer wrote this text, the line has N pieces: which words does piece 1 get, which piece 2 ... ?
 *
 * A plain rule that can be tested: no paid reader, no network, no clock, no storage, nothing written, no page. Loads in the page (window.CharmNestEngravePieceWords) and in node (module.exports).
 *
 *   read(texts, n, o?)      texts: the buyer's personalisation (a string, or the array of boxes: spec.personalization) · n: the number of pieces (the line's quantity, one piece per copy).
 *     -> { n, words, parts, split, same, said, none, blank, how, sure, found, why, note }
 *        words   exactly n strings, piece 1 first. '' = nothing to engrave on that piece. Never fewer, never more.
 *        split   true: the text holds n parts and each piece got its own (words differ by piece; `parts` is the same list).   false: the text was NOT split.
 *        same    true: every piece has the very same, non-empty words. With split false it is the WHOLE text on every piece: the card says `note` ("Same text on every piece") calmly and the person edits a piece on its own.
 *        said    true: the buyer wrote that it is the same on every piece ("Anna on both", "Same on all: Anna").
 *        none    true: nothing to engrave (empty, "N/A", "none", "see my message" ...): words are all ''.
 *        blank   [piece numbers, 1-based] whose part says "N/A" / "none": that piece has no words ("Anna / N/A").
 *        how     'none' | 'single' (n < 2: passed through) | 'same' (the buyer said so) | 'numbered' (1) 2) · 1. 2. · Piece 1: · #1 · First: · Charm 2) | 'labelled' (Left: Right:) | 'lines' (n separate lines / boxes)
 *                | 'list' (n items in one line: "Anna & Ben", "Mom, Dad, Me") | 'whole' (not split: the whole text on every piece)
 *        sure    true: numbered / labelled / lines / said "the same": the words may go on to their fit.   false: a proposal for a person to confirm ('list', or a piece left blank). 'whole' and 'none' are false.
 *        found   how many parts the text holds (0 nothing, 1 one text, k).  why  the plain reason a text was not split ("3 parts for 2 pieces", "looks like an address", "one text, nothing says it is for separate pieces").
 *        note    SAME_NOTE when `same` and not split; else ''.
 *
 *   forSlots(spec, slots, o?)   the same for a line spec ({ personalization, buyerMessage, staffNote, messages }) and its piece slots IN PIECE ORDER (names are only keys: C1..Cn, anything). -> read(...) plus
 *        bySlot { [slot exactly as passed]: words }, deferred.  deferred: another voice spoke (a buyer message, a staff note, an engraving-like message: o.engravingNote(text) says whether a message is about the
 *        engraving; absent, every message counts): this file reads nothing then (how 'deferred', words all '', bySlot all ''), the caller asks its reader once and gives that one reading to every piece.
 *   read(texts, n, { noun })  (BARS, Paul 19 Oct 2026: the bar listing's "Bar names:" and "Bar 1: A, Bar 2: B") a list label may carry the piece's own noun in front of it ("Bar names: Sophie, Sam, Eddy", "Bar names:" on a line of its own,
 *        "Bar 1: A, Bar 2: B" after "Bar names:"), and a line of ONE piece has that label taken off too; without `noun` every reading below is exactly what it was.
 *   isNoWords(text)  true when the text says there is nothing to write ("N/A", "none", "no personalisation", "Not requested on this item.", "see message", only punctuation).   SAME_NOTE.
 *
 * THE RULE (in this order; the first that fits decides):
 *   1  n < 2                       passed through untouched: words [texts joined by a line break], how 'single'. (A quantity-1 line never reads here; nothing about it changes.)
 *   2  nothing to write            none.  A text with a face label ("Front: .. Back: ..") is about the faces, not the pieces: not split.
 *   3  "the same on every piece"   Anna on both / on each piece / same on all: Anna / All: Anna / Both - Anna  ->  the one text on every piece, said. ("Love for all", "Anna on all", "All-Star" are not.)
 *   4  numbered or labelled        exactly n markers, each of 1..n once, in any order: "1) Anna 2) Ben" · "1. Anna\n2. Ben" · "Piece 1: Anna, Piece 2: Ben" · "#1 Anna #2 Ben" · "First: Anna, Second: Ben" · "Charm 2) Ben"
 *                                  (n = 2 also Left: / Right: / L: / R:). A different count, a number twice or missing, mixed kinds, other words before the first marker: NOT split (a text that numbers pieces and does not
 *                                  fit is never read as a list). A lone "1." is no numbering.
 *   5  n separate lines / boxes    exactly n non-empty lines (or boxes of the personalisation field) -> one each. Not for an address or contact details.
 *   6  n items in one line         the line cut at , ; | / & + and "and" (und, et, y), n items exactly (all the separators together; failing that ; | alone, then / alone: "Smith, Anna / Jones, Ben"; a label in front,
 *                                  "Names: Anna, Ben", is not a word). NOT split: 1 item, k != n items, a date ("June 5, 2024", "05/06/2024"), an address, coordinates, an e-mail / phone / web
 *                                  address, a long text (> 600 characters), items of more than 4 words, items joined only by "and" / "&" that differ in length ("Mom & Dad forever", "Mary Jane and John"), a greeting or a
 *                                  closing ("Love, Mom", "Smith, Jr"). A tight "&" "+" "/" is a separator only between words of 2+ letters ("Anna&Ben"; "R&D", "AT&T", "24/7", "1,000" are not).
 *   7  otherwise                   the WHOLE text on every piece, same true, how 'whole', why says why.
 * Words are never lost or duplicated silently: the parts are the buyer's own words (entities decoded "Mom&#39;s" -> "Mom's", invisible characters out, spaces collapsed, nothing else touched: no case, no spelling, no
 * punctuation); only the separators and the numbering marks between the parts are left out; a text that is not split is given WHOLE, and `same` says so. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngravePieceWords = api;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const SAME_NOTE = 'Same text on every piece';
  const MAX_PIECES = 400, MAX_INLINE = 600, MAX_TOKENS = 4;
  const str = x => String(x == null ? '' : x);

  /* ── the buyer's text, as typed ─────────────────────────────────────────────────────────────────────────── */
  const ENTITY = /&(?:#(\d{1,7})|#[xX]([0-9a-fA-F]{1,6})|(amp|apos|quot|lt|gt|nbsp));/g;
  const NAMED = { amp: '&', apos: "'", quot: '"', lt: '<', gt: '>', nbsp: ' ' };
  const decode = v => {   // (Etsy hands the words HTML-escaped, "Mom&#39;s &amp; Dad", sometimes twice)
    let s = str(v);
    while (s.indexOf('&') >= 0) {
      const was = s;
      s = s.replace(ENTITY, (m, dec, hex, name) => { if (name) return NAMED[name]; const cp = dec != null ? parseInt(dec, 10) : parseInt(hex, 16); return cp >= 32 && cp <= 0x10FFFF && !(cp >= 0xD800 && cp <= 0xDFFF) ? String.fromCodePoint(cp) : m; });
      if (s === was) break;
    }
    return s;
  };
  // (U+200C and U+200D stay: they join the parts of an emoji family and shape Persian and Indic words)
  const INVISIBLE = /[\u200B\u200E\u200F\u2060\uFEFF\uFFFC\uFFFD\u0000-\u0008\u000E-\u001F\u007F]/g;
  const SPACES = /[ \t\u00a0\u1680\u2000-\u200a\u202f\u205f\u3000]+/g;
  /** The text as a list of non-empty lines: entities decoded, invisible characters out, every kind of line break a line break, spaces collapsed. */
  function linesOf(texts) {
    const out = [];
    for (const t of Array.isArray(texts) ? texts : [texts]) {
      const s = decode(t).replace(/\r\n?|[\u2028\u2029\u0085\u000B\u000C]/g, '\n').replace(INVISIBLE, '');
      for (const l of s.split('\n')) { const x = l.replace(SPACES, ' ').trim(); if (x) out.push(x); }
    }
    return out;
  }
  const tokens = s => str(s).split(' ').filter(Boolean);
  const hasWords = s => /[\p{L}\p{N}\p{Extended_Pictographic}]/u.test(s);

  /* ── nothing to write ──────────────────────────────────────────────────────────────────────────────────── */
  const NONE = [
    /^(?:n\s*[/.\\-]?\s*a|nil|null|none|nope|no|nothing|nada|not applicable|not required|not needed|not wanted|no need|no thanks?|no thank you|blank|empty|tbd|tba|tbc|to be (?:decided|confirmed|determined)|not requested(?: on this item)?|none (?:needed|required|please|thanks?)|no (?:text|name|words?|initials?|message|inscription)|no (?:personali[sz]ation|personali[sz]ing|engraving|engrave)(?: (?:needed|required|please|thanks?|thank you|wanted|necessary|on this))?|(?:personali[sz]ation|engraving|text) (?:not )?(?:needed|required|wanted|necessary|applicable)|without (?:any )?(?:personali[sz]ation|engraving|text)|leave (?:it |this )?(?:blank|empty)|nothing (?:needed|required|please|to engrave|to write)|do(?:n'?t| not) (?:need|want) (?:any |an )?(?:personali[sz]ation|engraving|text|name))$/i,
    /^(?:i(?:'ll| will)|i am going to|we will|will|going to) (?:send|message|dm|e-?mail|follow up|get back|let you know)(?: (?:you|it|them|details|the details|later|soon|separately|after|afterwards|this))*$/i,
    /^(?:please |pls )?(?:see|check|refer to|read|look at|as per|per|as in|as stated in|as discussed in|as sent in|sent in|sent via|via|in|details in|info in|will (?:send|message|follow up|dm|email)|i(?:'ll| will) (?:send|message|follow up|dm|email))(?: (?:you|it|them|details))?(?: (?:to follow|later|soon|separately))? ?(?:my |the |your |our |a |an |this |that )?(?:etsy )?(?:messages?|notes?|convo|conversation|attachments?|photos?|pictures?|pics?|images?|e-?mail|dm|order notes?)(?: (?:sent|above|below|attached|later|please|thanks?|thank you))?$/i
  ];
  // (a part of a list that says nothing is written for that piece: narrower than a whole text; a bare "No" or "Nothing" can be a word of the list)
  const BLANK_PART = /^(?:n\s*[/.\\-]?\s*a|nil|null|none|nada|blank|empty|tbd|tba|tbc|no (?:text|name|words?|engraving|personali[sz]ation)|leave (?:it |this )?(?:blank|empty)|not applicable)$/i;
  const keyOf = s => s.toLowerCase().replace(/^[\s"'\u201c\u201d\u2018\u2019()[\]{}.!,;:*_~-]+|[\s"'\u201c\u201d\u2018\u2019()[\]{}.!,;:*_~-]+$/g, '').replace(/\s+/g, ' ');
  /** True when the text says there is nothing to write: empty, only punctuation, "N/A", "none", "no personalisation", "Not requested on this item.", "see my message". */
  const noLines = lines => lines.every(l => !hasWords(l) || NONE.some(rx => rx.test(keyOf(l))));
  function isNoWords(text) { return noLines(linesOf(text)); }
  const isBlankPart = p => !hasWords(p) || BLANK_PART.test(keyOf(p));

  /* ── what is not a list of pieces ──────────────────────────────────────────────────────────────────────── */
  const MONTH = '(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)';
  const ADDRESS = [
    /\b\d{1,5}[a-z]?\s+(?:[\p{L}.'-]+\s+){0,3}(?:st|str|street|ave?|avenue|rd|road|blvd|boulevard|ln|lane|dr|drive|ct|court|way|pl|place|ter|terrace|cir|circle|hwy|highway|pkwy|parkway|sq|square|strasse|stra\u00dfe|rue|via|calle)\b/iu,
    /\b(?:apt|apartment|suite|ste|unit|floor|p\.?\s?o\.?\s*box)\b\.?\s*#?\s*\w+/i,
    /(?:^|[\s,])\d{5}(?:-\d{4})?(?=$|[\s,])/,
    /\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b/
  ];
  const DATE = [
    new RegExp('\\b' + MONTH + '\\b\\.?\\s*\\d{1,2}(?:st|nd|rd|th)?\\b', 'i'),
    new RegExp('\\b\\d{1,2}(?:st|nd|rd|th)?\\s+(?:of\\s+)?' + MONTH + '\\b', 'i'),
    /\b\d{1,4}[./-]\d{1,2}[./-]\d{1,4}\b/,
    /\b\d{1,2}:\d{2}\b/
  ];
  const COORD = [/\d\s*[\u00b0\u00ba\u02da*]\s*\d/, /\d\s*['\u2032"\u2033]\s*[NSEW]\b/, /[-+]?\d{1,3}\.\d{3,}\s*[,;]\s*[-+]?\d{1,3}\.\d{3,}/, /\b\d{1,3}\s*\u00b0/];
  const CONTACT = [/@/, /https?:|www\./i, /\.(?:com|net|org|co|uk|de|fr|io)\b/i, /\b\d{3}[\s.\-)]*\d{3}[\s.\-]*\d{4}\b/];
  const FACE = /(?:^|[\s,;|/&(])(?:front|back|reverse|obverse|inside|outside|both sides|side\s*[12ab])(?:\s*[:=)]|\s+[-\u2013\u2014]+\s)/i;
  const anyOf = (list, s) => list.some(rx => rx.test(s));
  /** Why a text is not a list of pieces: 'looks like an address' ..., or ''. `lines` true: separate lines the buyer wrote on purpose (a date or a coordinate on its own line is a piece's words). */
  function notAList(text, lines) {
    if (anyOf(ADDRESS, text)) return 'looks like an address';
    if (anyOf(CONTACT, text)) return 'looks like contact details';
    if (lines) return '';
    if (anyOf(DATE, text)) return 'looks like a date';
    if (anyOf(COORD, text)) return 'looks like coordinates';
    return '';
  }

  /* ── the buyer says it is the same on every piece ─────────────────────────────────────────────────────── */
  const NOUN = '(?:pieces?|items?|charms?|tags?|discs?|pendants?|earrings?|necklaces?|bars?|rings?|ones?|two|three|four)';
  const EACH = '(?:all|each|every|both)(?:\\s+(?:of\\s+)?(?:the\\s+)?' + NOUN + ')?';
  const EACH_SURE = '(?:(?:both|each|every)(?:\\s+(?:of\\s+)?(?:the\\s+)?' + NOUN + ')?|all\\s+(?:of\\s+)?(?:the\\s+)?' + NOUN + ')';   // ("Love for all" is a message; "Anna on all pieces" is not)
  const COLON = '(?:\\s*[:=]\\s*|\\s+[-\\u2013\\u2014]+\\s+)';   // (a colon or an equals sign, or a dash with a space each side: "All-Star" is a name)
  const SAME_LEAD = [
    new RegExp('^(?:(?:the\\s+)?(?:same|identical)(?:\\s+(?:text|words?|name|engraving|inscription))?\\s+(?:on|for)\\s+)?' + EACH + COLON + '(.+)$', 'i'),
    new RegExp('^(?:the\\s+)?(?:same|identical)(?:\\s+(?:text|words?|name|engraving|inscription))?(?:\\s+(?:on|for)\\s+' + EACH + ')?' + COLON + '(.+)$', 'i')
  ];
  const SAME_TAIL = [
    new RegExp('^(.+?)[\\s,;(\\-\\u2013\\u2014:]+(?:the\\s+)?(?:same|identical)(?:\\s+(?:text|words?|name|engraving))?\\s+(?:on|for)\\s+' + EACH + '\\)?\\s*$', 'i'),
    new RegExp('^(.+?)[\\s,;(\\-\\u2013\\u2014:]+(?:on|for)\\s+' + EACH_SURE + '\\)?\\s*$', 'i')
  ];
  function saidSame(text) {
    for (const rx of SAME_LEAD) { const m = rx.exec(text); if (m) return trimWord(m[1]); }
    for (const rx of SAME_TAIL) { const m = rx.exec(text); if (m) return trimWord(m[1]); }
    return '';
  }

  const trimWord = x => {
    // (a minus sign that belongs to a number, "-96.7", stays; a dash that stands apart is a separator)
    let s = str(x).replace(/^(?:[\s,;|/&+:=]|[\u2013\u2014-]+(?=\s))+/, '').replace(/(?:[\s,;|/&+]|\s[\u2013\u2014-]+(?=\s|$)|\s(?:and|&)(?=\s|$))+$/i, '').trim();
    const q = /^(["'\u201c\u2018])([^]*)(["'\u201d\u2019])$/.exec(s);
    if (q && !/["'\u201c\u201d\u2018\u2019]/.test(q[2])) s = q[2].trim();
    return s;
  };

  /* ── numbered or labelled ──────────────────────────────────────────────────────────────────────────────── */
  const NUMW = { one: 1, two: 2, three: 3, four: 4, five: 5, six: 6, seven: 7, eight: 8, nine: 9, ten: 10, eleven: 11, twelve: 12, first: 1, second: 2, third: 3, fourth: 4, fifth: 5, sixth: 6, seventh: 7, eighth: 8, ninth: 9, tenth: 10 };
  const NUMALT = Object.keys(NUMW).join('|'), ORDALT = 'first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|1st|2nd|3rd|[4-9]th|10th|11th|12th';
  const LABELW = 'pieces?|charms?|tags?|discs?|disks?|items?|earrings?|ears?|necklaces?|bracelets?|rings?|bars?|pendants?|names?|words?|texts?|initials?|letters?|numbers?|nr|num|no';
  const OPEN = '(^|[\\s,;|/&+(\\[])';
  const SEPS = '(?:=>|->|\u2192|[:=)\\]\\-\u2013\u2014.])';
  const numOf = w => { w = str(w).toLowerCase(); const d = /^(\d{1,3})(?:st|nd|rd|th)?$/.exec(w); return d ? +d[1] : NUMW[w] || 0; };
  const MARK = [
    // Piece 1 · Charm #2 · Tag two · No. 3 (a separator after the number is optional: "Piece 1 Anna Piece 2 Ben")
    { rx: new RegExp(OPEN + '(?:' + LABELW + ')\\.?\\s*#?\\s*(\\d{1,3}|' + NUMALT + ')(?:st|nd|rd|th)?(?![\\d\\p{L}])(?:\\s*' + SEPS + '+(?!\\d))?\\s*', 'giu'), num: 2, kind: 'n' },
    // #1 · # 2
    { rx: new RegExp(OPEN + '#\\s*(\\d{1,3})(?![\\d\\p{L}])(?:\\s*' + SEPS + '+(?!\\d))?\\s*', 'giu'), num: 2, kind: 'n' },
    // 1) · (1) · 1. · 1: · 1- · 1= (a separator right after the number, and no digit right after the separator: not 1.5, not 10:30, not 1-2-3)
    { rx: new RegExp(OPEN + '\\(?(\\d{1,2})(?:\\)|[\\]:.=\\-\u2013\u2014])(?!\\d)\\s*', 'gu'), num: 2, kind: 'n' },
    // First: · 2nd - · Third piece:
    { rx: new RegExp(OPEN + '(' + ORDALT + ')(?:\\s+(?:' + LABELW + '))?\\s*' + SEPS + '+\\s*', 'giu'), num: 2, kind: 'n' },
    // Left: · Right ear - · L: · R)
    { rx: new RegExp(OPEN + '(left|right|l|r)(?:\\s+(?:ear|earring|piece|side|one|charm|stud|hoop|hand))?\\s*(?:=>|->|\u2192|[:=)\\-\u2013\u2014])\\s*', 'giu'), side: 2, kind: 's' }
  ];
  function findMarks(text) {
    const hits = [];
    for (const M of MARK) {
      M.rx.lastIndex = 0;
      for (let m; (m = M.rx.exec(text));) {
        const at = m.index + m[1].length, end = m.index + m[0].length;
        if (m[0].length === 0) { M.rx.lastIndex++; continue; }
        if (M.kind === 's') { const w = m[M.side].toLowerCase(); hits.push({ at, end, kind: 's', k: w[0] === 'l' ? 1 : 2 }); }
        else { const k = numOf(m[M.num]); if (k) hits.push({ at, end, kind: 'n', k }); }
      }
    }
    hits.sort((a, b) => a.at - b.at || b.end - a.end);
    const out = []; for (const h of hits) if (!out.length || h.at >= out[out.length - 1].end) out.push(h);
    return out;
  }
  const LEADIN = /^(?:please\s+)?(?:engrav\w*|initials?|names?|text|words?|inscription|personali[sz]ation)\s*:\s*/i;   // ("Names: Anna, Ben": the label is not a piece's word)
  const LEAD = /^(?:please\s+)?(?:engrav\w*|initials?|letters?|names?|text|words?|personali[sz]ation|pieces?|charms?|tags?|discs?|each|both|as follows|like this|the following)?\s*[:\-]?\s*$/i;
  const LABEL_LINE = /^(?:please\s+)?(?:engrav\w*|initials?|names?|text|words?|inscription|personali[sz]ation)\s*:$/i;   // (a label on a line of its own: "Names:")
  /** The label rules of a reading: the ones above, or (BARS) the same with the piece's noun allowed in front ("Bar names:"). { LEADIN, LEAD, LABEL_LINE, noun } */
  function leadsFor(o) {
    const noun = o && o.noun ? str(o.noun).toLowerCase().replace(/[^a-z]/g, '') : '';
    if (!noun) return { LEADIN, LEAD, LABEL_LINE, noun: '' };
    const pre = '(?:' + noun + 's?\\s+)?', at = rx => new RegExp(rx.source.replace('^(?:please\\s+)?', '^(?:please\\s+)?' + pre).replace('(?:engrav\\w*|', '(?:' + noun + 's?|engrav\\w*|'), 'i');   // ("Bars:" alone is a label too)
    return { LEADIN: at(LEADIN), LEAD: at(LEAD), LABEL_LINE: at(LABEL_LINE), MARK1: new RegExp('^' + noun + 's?\\s*#?\\s*(?:1|one|first)\\s*[:=)\\-\\u2013\\u2014.]\\s*', 'i'), noun };
  }
  /** { words } when the text numbers or labels its pieces and fits n, { why } when it tries to and does not, null when it holds no (or only a lone) marker. */
  function fromMarks(text, n, lead0) {
    const marks = findMarks(text);
    if (marks.length < 2) return null;
    const kinds = new Set(marks.map(m => m.kind));
    if (kinds.size > 1) return { why: 'the labels are of different kinds' };
    const nums = marks.map(m => m.k), uniq = [...new Set(nums)].sort((a, b) => a - b);
    const kind = marks[0].kind;
    if (kind === 's' ? !(n === 2 && marks.length === 2 && uniq.length === 2) : (marks.length !== n || uniq.length !== n || nums.some(k => k < 1 || k > n))) {
      return { why: kind === 's' ? `Left / Right labels, the line has ${n} pieces` : `the numbers in the text (${uniq.slice(0, 6).join(', ')}${uniq.length > 6 ? ', ..' : ''}) do not fit the ${n} pieces` };
    }
    const lead = text.slice(0, marks[0].at).trim();
    if (lead && !(lead0 || LEAD).test(lead)) return { why: 'other words come before the numbered ones' };
    const words = new Array(n).fill(null);
    marks.forEach((m, i) => { words[m.k - 1] = trimWord(text.slice(m.end, i + 1 < marks.length ? marks[i + 1].at : text.length)); });
    if (words.some(w => w === null || w === '')) return { why: 'a numbered piece has no words' };
    return { words, kind };
  }

  /* ── n items in one line ───────────────────────────────────────────────────────────────────────────────── */
  const SEP_RX = /(\s+(?:and|und|et|y)\s+)|(\s*[;\uff1b\u061b|\uff5c]\s*)|(\s*[,\uff0c\u3001\u060c]\s*)|(\s*[/\uff0f]\s*)|(\s*[&\uff06+\uff0b]\s*)/giu;
  const LETTER = /\p{L}/u;
  const endTok = s => { const t = str(s).replace(/\s+$/, ''); const i = t.lastIndexOf(' '); return t.slice(i + 1); };
  const startTok = s => { const t = str(s).replace(/^\s+/, ''); const i = t.indexOf(' '); return i < 0 ? t : t.slice(0, i); };
  const lenOf = s => Array.from(s).length;
  const PHRASE_LEAD = /^(?:love|with love|hello|hi|hey|dear|to|from|happy|merry|best|xoxo?|thanks?|thank you|please|always|forever|mr|mrs|ms|dr|miss|sir|sincerely|yours|cheers|congrats|congratulations)$/i;
  const PHRASE_TAIL = /^(?:forever|always|4ever|xoxo?|love|jr|sr|ii|iii|iv|md|phd|esq)$/i;
  /** The items of one line: { parts, andOnly } (andOnly: every separator was "and", "&" or "+"). Separators inside a word are not separators ("1,000", "R&D", "24/7"). */
  function splitInline(line, only) {   // only: 'semi' (; |) or 'slash' (/): cut at that kind alone
    const cuts = []; let andOnly = true, any = false;
    SEP_RX.lastIndex = 0;
    for (let m; (m = SEP_RX.exec(line));) {
      const at = m.index, end = at + m[0].length, hit = m[0], left = line.slice(0, at), right = line.slice(end);
      let ok = true, kindAnd = false;
      if (m[1]) {
        kindAnd = true;
        const w = hit.trim().toLowerCase();
        if (w !== 'and') ok = lenOf(endTok(left).replace(/[^\p{L}]/gu, '')) >= 2 && lenOf(startTok(right).replace(/[^\p{L}]/gu, '')) >= 2;
      } else if (m[3]) {
        if (!/\s$/.test(hit) && end < line.length) ok = LETTER.test(line[at - 1] || '') && LETTER.test(line[end] || '');   // (tight: "Anna,Ben" yes, "1,000" no)
      } else if (m[4] || m[5]) {
        kindAnd = !!m[5];
        const spaced = /^\s/.test(hit) && /\s$/.test(hit);   // (spaced on both sides: a separator; else the words beside it must be words of 2+ letters: "Anna&Ben" yes, "w/ love" "R&D" "24/7" no)
        if (!spaced) { const l = endTok(left), r = startTok(right); ok = lenOf(l) >= 2 && lenOf(r) >= 2 && LETTER.test(l) && LETTER.test(r) && /\p{L}$/u.test(l) && /^\p{L}/u.test(r); }
      }
      if (m[0].length === 0) SEP_RX.lastIndex++;
      if (only === 'semi' ? !m[2] : only === 'slash' ? !m[4] : false) ok = false;
      if (!ok) continue;
      cuts.push([at, end]); any = true; if (!kindAnd) andOnly = false;
    }
    const parts = []; let last = 0;
    for (const [a, b] of cuts) { parts.push(line.slice(last, a)); last = b; }
    parts.push(line.slice(last));
    return { parts: parts.map(p => trimWord(p.replace(/^(?:and|&|\+)\s+/i, ''))).filter(Boolean), andOnly: any && andOnly };
  }

  /* ── the reading ───────────────────────────────────────────────────────────────────────────────────────── */
  const clampN = n => { n = Math.floor(+n); return n >= 1 ? Math.min(n, MAX_PIECES) : 1; };
  function result(n, o) {
    return Object.assign({ n, words: new Array(n).fill(''), parts: [], split: false, same: false, said: false, none: false, blank: [], how: 'whole', sure: false, found: 0, why: '', note: '' }, o || {});
  }
  const whole = (n, lines, found, why) => {
    const w = lines.join('\n');
    return result(n, { words: new Array(n).fill(w), same: true, how: 'whole', found, why, note: SAME_NOTE });
  };
  function splitDone(n, words, how, sure, found) {
    const blank = []; words.forEach((w, i) => { if (!w) blank.push(i + 1); });
    if (blank.length === n) return result(n, { none: true, how: 'none' });
    const same = words.every(w => w && w === words[0]);
    return result(n, { words, parts: words.slice(), split: true, same, blank, how, sure: !!sure && !blank.length, found: found == null ? n : found });
  }
  const partWords = parts => parts.map(p => (isBlankPart(p) ? '' : p));

  function read(texts, n, o) {
    n = clampN(n);
    const L = leadsFor(o);
    if (n < 2) {   // (a line of one piece: the text untouched, as it always was)
      if (L.noun) {   // (BARS: one bar, one name: "Bar names:" or "Bar names: Sophie" is the label and the name)
        const ls = linesOf(texts); if (ls.length && L.LABEL_LINE.test(ls[0]) && ls.length > 1) ls.shift(); else if (ls.length) { const w = ls[0].replace(L.LEADIN, '').replace(L.MARK1, '').trim(); if (w) ls[0] = w; else ls.shift(); }   // ("Bar 1: Sophie" is the one bar's label)
        const raw1 = ls.join('\n'); return result(1, { words: [raw1], how: 'single', found: raw1 ? 1 : 0 });
      }
      const raw = (Array.isArray(texts) ? texts : [texts]).map(str).join('\n');
      return result(1, { words: [raw], how: 'single', found: raw ? 1 : 0 });
    }
    const lines = linesOf(texts);
    if (L.noun && L.LABEL_LINE.test(lines[0] || '')) { if (lines.length > 1) lines.shift(); else lines.length = 0; }   // (BARS: "Bar names:" on a line of its own is the label, never a name: two names under it are two names, not three lines)
    if (!lines.length || noLines(lines)) return result(n, { none: true, how: 'none' });
    const text = lines.join('\n');
    if (FACE.test(text)) return whole(n, lines, 1, 'the text is about the front and the back, not about separate pieces');

    const s = saidSame(text);
    if (s && hasWords(s) && !noLines([s])) return result(n, { words: new Array(n).fill(s), same: true, said: true, how: 'same', sure: true, found: 1, note: SAME_NOTE });

    const mk = fromMarks(text, n, L.LEAD);
    if (mk && mk.words) return splitDone(n, partWords(mk.words), mk.kind === 's' ? 'labelled' : 'numbered', true);
    if (mk && mk.why) return whole(n, lines, 0, mk.why);

    if (lines.length === n + 1 && L.LABEL_LINE.test(lines[0])) { const rest = read(lines.slice(1), n, o); if (rest.split) return rest; }   // ("Names:" on a line of its own)
    if (lines.length === n) {
      const bad = notAList(text, true);
      if (bad) return whole(n, lines, n, bad);
      return splitDone(n, partWords(lines), 'lines', true);
    }
    if (lines.length > 1) return whole(n, lines, lines.length, `${lines.length} lines for ${n} pieces`);

    const line = lines[0].replace(L.LEADIN, '') || lines[0];
    if (line.length > MAX_INLINE) return whole(n, lines, 1, 'a long text, nothing says it is for separate pieces');
    const bad = notAList(line, false);
    // (all the separators together; failing that, the strong ones alone: "Smith, Anna / Jones, Ben" is two pieces, each with a comma inside)
    let sp = splitInline(line);
    if (sp.parts.length !== n) for (const only of ['semi', 'slash']) { const alt = splitInline(line, only); if (alt.parts.length === n) { sp = alt; break; } }
    const parts = sp.parts;
    if (parts.length !== n) return whole(n, lines, parts.length || 1, bad || (parts.length > 1 ? `${parts.length} parts for ${n} pieces` : 'one text, nothing says it is for separate pieces'));
    if (bad) return whole(n, lines, n, bad);
    if (parts.some(p => tokens(p).length > MAX_TOKENS)) return whole(n, lines, n, 'the parts are long phrases, not names');
    if (sp.andOnly && new Set(parts.map(p => tokens(p).length)).size > 1) return whole(n, lines, n, 'the parts differ in length: one phrase, not a list');
    if (n === 2 && (PHRASE_LEAD.test(keyOf(parts[0])) || PHRASE_TAIL.test(keyOf(parts[1])))) return whole(n, lines, n, 'a greeting or a closing, not two pieces');
    return splitDone(n, partWords(parts), 'list', false);
  }

  /** read() for a line spec and its piece slots in piece order. deferred: another voice spoke, nothing is read here. */
  function forSlots(spec, slots, o) {
    const sp = spec || {}, list = (Array.isArray(slots) ? slots : []).map(String), n = list.length;
    const note = o && typeof o.engravingNote === 'function' ? o.engravingNote : () => true;
    const others = !!(str(sp.staffNote).trim() || str(sp.buyerMessage).trim() || (Array.isArray(sp.messages) ? sp.messages : []).some(m => note(m && m.text)));
    let r;
    if (n >= 2 && others) r = result(n, { how: 'deferred', why: 'someone else wrote about the engraving too: the reader decides' });
    else r = read(sp.personalization == null ? [] : sp.personalization, Math.max(n, 1), o);
    const by = {}; list.forEach((slot, i) => { by[slot] = r.words[i] != null ? r.words[i] : ''; });
    return Object.assign(r, { bySlot: by, deferred: r.how === 'deferred' });
  }

  return { read, forSlots, isNoWords, SAME_NOTE, linesOf };
});
