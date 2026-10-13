/* THE READING OF WHAT THE BUYER TYPED, before any paid reader (SETBAR-C, Paul, 13 Oct 2026, order 4202291826, Etsy listing 532401548 "Bar Necklace SET of 2": "The system should intelligently read and
 * understand the meaning and context of the users requirement, and based on that make a decision as to engraving details whether engraving should be there or not, and what the user actually wants engraved.").
 *
 * The buyer wrote   16"= 33*0'45" N 96*45'53" W (the asterisk is supposed to be the little degree circle-I just don't have that on my keyboard) 18"= Ginger
 * and meant         the 16" necklace is engraved   33°0'45" N 96°45'53" W     the 18" necklace is engraved   Ginger
 * (each * is a degree sign, the bracket is an explanation to the shop and is NOT engraving). The app used to glue it all into one text for every piece. This file reads it the way a person would, with plain
 * rules that can be tested, so the words are right before a model is asked anything and a model is only needed for what the rules cannot settle:
 *
 *   readPersonalisation(text, { lengths:[16,18] })  -> {
 *       raw        the buyer's words (entities decoded, invisible characters out, nothing else touched)
 *       labelled   true when the buyer gave a piece its own words by a label: 16"= ...  18"= ...  ("16 inch:", "Left: ...", "Short chain - ...", "Necklace 2 = ...")
 *       pieces     [{ label:'16"', length:16, kind:'length', words:'33°0\'45" N 96°45\'53" W', raw }, { label:'18"', length:18, kind:'length', words:'Ginger', raw }]   (kind: length | side | rank; side 'L'|'R' or
 *                  rank 0|1 for the other kinds). The buyer's own spelling and case stay; line breaks the buyer typed stay (words has "\n" between them).
 *       whole      the words as ONE run when nothing is labelled (a name; or the text with the notes to the shop taken out); lines = whole split at the buyer's line breaks
 *       dropped    [{ text, why }]  what was left out because it talks to the shop and is not engraving ("(the asterisk is supposed to be ...)", "I just don't have that on my keyboard"). Never engraved,
 *                  always shown to the person who confirms the words (say() puts it in one plain line).
 *       fixes      [{ kind:'degree', count }]  an asterisk between digits of a coordinate (33*0'45" N) read as the degree sign U+00B0. Nothing else is rewritten: not a name, not a word, not a case.
 *       layoutRequest / layoutNote   the buyer asks for a DIFFERENT layout of the set ("swap the bars", "thick bar on the 16", both on one chain): never guessed, the line waits for a person (layoutNote is the buyer's own clause)
 *       unsure / unsureWhy   the rules cannot tell what is engraving (a sentence that mixes a request and a name, a bracket that may be for the shop, a label used twice): the reading is NOT used to decide anything,
 *                  the caller goes on as it always did (the paid reader, a person)
 *       changed    anything above differs from the raw words
 *     }
 *   say(reading)                       one plain line of what was read, split, left out and fixed (the card's reason line)
 *   fixDegrees(text) -> { text, count } the asterisk-as-degree rule alone (also run on a model's answer, which copies the buyer's asterisk)
 *   lengthsOf(text|[text])             the necklace lengths a value names: '16"&18"·Set·ANGELINA' -> [16, 18]; '18"& 20"·Set·COMIC' -> [18, 20]; '16"' -> [16]
 *   lengthsOfLine(line)                the lengths of a set from the options of an Etsy line (the "Necklace Length·FONT" option)
 *   decide(o)                          the engraving yes / no / ask of a line: { engrave:'yes'|'no'|'ask', token:'ENGRAVED'|'BLANK'|'', why }  o: { options:[{name,value}], words:boolean, note:boolean (a message asks for engraving), anyNote:boolean (anything is written in a message / note) }
 *                                      "ENGRAVED" in an option value = engrave; "BLANK" = no engraving; and the general rule: a line with personalisation words that is not BLANK is engraved.
 *                                      BLANK with words (or a note that asks for engraving), or ENGRAVED with nothing written, is 'ask': it waits for a person (never engraved, never silently dropped).
 *   assign(reading, slots)             THE JOIN (the one place a wording meets a piece). slots: [{ slot:'D1', length:16 }, ...] (the length of the chain the piece hangs on, from the set's bar-and-chain mapping
 *                                      that SETBAR-A writes) or [{ slot, rank }] (rank 0 = the thinner bar = the SHORTER chain, Paul 13 Oct: "The thinner bar always sits on the shorter chain unless the user special requests
 *                                      a different layout"). -> { ok, bySlot:{ D1:{ label, words, length } }, missing:[slots with no words], unmatched:[labels with no piece], why }
 *   slotsOf(row, jobs)                 the slots handed to assign() for a line: through slotInfo (below). null when nothing says which chain a piece hangs on.
 *   slotInfo                           THE HOOK, the one place a wording meets a bar. A function (row, slot, job) -> { length } | { rank } | number | null, set by the page. SETBAR-A's bar-and-chain mapping plugs in with ONE line,
 *                                      for example (the thinner bar hangs on the shorter chain, Paul 13 Oct 2026, unless the buyer asks otherwise):
 *                                          CharmNestEngraveRead.slotInfo = (row, slot) => ({ length: <the chain length, in inches, that piece hangs on> });   // or { rank: 0 } for the thinner bar, { rank: 1 } for the thicker
 *                                      Until it is set a labelled reading is NOT assigned: every piece's card shows every wording (16" → ...  18" → ...) and asks which goes where. Never guessed. Nothing else in the page changes
 *                                      when it is set: the bridge asks slotsOf() and assign() (classifyOnce, readByRules) and, when the lengths are known, tells the paid reader the chain of each piece (pieceLengths).
 *
 * Pure: no network, no clock, no storage, nothing written. Loads in the page (window.CharmNestEngraveRead) and in node (module.exports). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveRead = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const str = x => String(x == null ? '' : x);

  /* ── the buyer's text, decoded ─────────────────────────────────────────────────────────────────────────── */
  const ENTITY = /&(?:#(\d{1,7})|#[xX]([0-9a-fA-F]{1,6})|(amp|apos|quot|lt|gt|nbsp));/g;
  const NAMED = { amp: '&', apos: "'", quot: '"', lt: '<', gt: '>', nbsp: ' ' };
  const decode = v => {   // (Etsy hands the words HTML-escaped, "Mom&#39;s &amp; Dad", sometimes twice: each pass that changes anything shortens the text)
    let s = str(v);
    while (s.indexOf('&') >= 0) {
      const was = s;
      s = s.replace(ENTITY, (m, dec, hex, name) => { if (name) return NAMED[name]; const cp = dec != null ? parseInt(dec, 10) : parseInt(hex, 16); return cp >= 32 && cp <= 0x10FFFF && !(cp >= 0xD800 && cp <= 0xDFFF) ? String.fromCodePoint(cp) : m; });
      if (s === was) break;
    }
    return s;
  };
  const INVISIBLE = /[\u200B-\u200F\u2028\u2029\u2060\uFEFF\uFFFC\uFFFD\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g;
  /** The words as typed: decoded, invisible characters out, no-break spaces as spaces, each line trimmed, empty lines gone. The line breaks the buyer typed stay. */
  const clean = v => decode(v).replace(INVISIBLE, '').replace(/\u00a0/g, ' ').replace(/\r/g, '').split('\n').map(l => l.replace(/[ \t]+/g, ' ').trim()).filter(Boolean).join('\n');
  const oneLine = s => str(s).replace(/\s+/g, ' ').trim();
  const clip = (s, n) => { s = oneLine(s); return s.length > n ? s.slice(0, n - 1) + '…' : s; };

  /* ── the degree sign (3: an asterisk standing for the little circle) ───────────────────────────────────────────────────────────────────────
   * An asterisk is read as a degree sign ONLY inside a coordinate: straight after a number and (a) before minutes ("33*0'45\"", "96*45'53\""), (b) before a compass letter ("33.4567* N", "96*W"),
   * or (c) after a decimal number with two or more decimals ("33.4567*, -96.7654*"). The masculine ordinal and the ring (º, ˚) typed between a number and minutes or a compass letter mean the same and read as ° too
   * (Marck Script has no º). A * anywhere else ("Mom*Dad", "*Ginger*") is the buyer's own and stays. The minute and second marks stay as typed (' and "): the prime marks U+2032 / U+2033 are missing from four of
   * the app's engraving fonts. U+00B0 itself is drawn by all eleven (checked by tests/charm-nest/engrave-read.cjs against vendor/fonts). */
  const MIN_MARK = '[\'\u2032\u2019\u2018\u00b4`]';
  const DEG = [
    new RegExp('(\\d(?:[.,]\\d+)?)[ \\t]*[*\u00ba\u02da](?=[ \\t]*\\d{1,3}(?:[.,]\\d+)?[ \\t]*' + MIN_MARK + ')', 'g'),   // 33*0'45"
    /(\d(?:[.,]\d+)?)[ \t]*[*\u00ba\u02da](?=[ \t]*[NSEWnsew](?![A-Za-z\u00c0-\u024f])(?=[ \t]*(?:$|\n|[,;.\d\u2013\u2014-]|[NSEWnsew](?![A-Za-z\u00c0-\u024f]))))/g,   // 33* N   96*W   (the letter ends the coordinate: "5*S is a code" is not one)
    /(\d+[.,]\d{2,})[ \t]*\*(?![\w*])/g                                                                                  // 33.4567*,  -96.7654*
  ];
  function fixDegrees(text) {
    let s = str(text), count = 0;
    for (const rx of DEG) s = s.replace(rx, (m, d) => { count++; return d + '\u00b0'; });
    return { text: s, count };
  }

  /* ── what talks to the shop (2) ────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  // A bracket the buyer typed is a clean boundary: what is inside is dropped when it plainly talks to the shop. Words that can carry the inscription itself (a name, a date, "Mom") stay.
  const BRACKET_CUE = [
    /\b(?:please|pls|plz|kindly|thanks?|thank\s*you|thx|sorry)\b/i,
    /\bif\s+(?:it'?s\s+|it\s+is\s+)?(?:possible|poss|you\s+(?:can|could|are\s+able))\b/i,
    /\blet\s+me\s+know\b/i,
    /\bi(?:'|\u2019)?(?:m|ve|d|ll)?\s+(?:just\s+|really\s+|also\s+|only\s+|simply\s+|actually\s+)*(?:don'?t|do\s+not|can'?t|cannot|couldn'?t|didn'?t|wasn'?t|haven'?t|forgot|typed|wrote|meant|mean|want(?:ed)?|would\s+like|need(?:ed)?|hope|prefer|think|am\s+not|not\s+sure|like\s+(?:it|this|that|the|to))\b/i,
    /\b(?:is|are|was|were)\s+(?:supposed|meant|intended)\b|\bsupposed\s+to\b/i,
    /\b(?:stands?|standing)\s+for\b|\brepresents?\b|\bmeans\b|\bmeaning\b|\bi\.e\b|\be\.g\b|\bas\s+in\b|\bsame\s+as\b|\bfor\s+example\b|\bsic\b|\btypo\b/i,
    /\b(?:keyboard|asterisks?|symbols?|degree\s+(?:signs?|symbols?|circles?|marks?)|circle|emoji)\b/i,
    /\b(?:engrav\w*|inscri\w*|etch\w*|font|typeface|cursive|italics?|bold|upper\s*case|lower\s*case|capital\w*|all\s*caps|cent(?:er|re)d?|size)\b/i,
    /\b(?:chain|necklace|bar|bars|pendant|charm)s?\b[^.]{0,40}\b(?:on|for|with|to)\b|\b(?:swap|switch|opposite|instead)\b/i
  ];
  // An unbracketed sentence that EXPLAINS (no inscription can be in it) and stands apart from the words (its own sentence or line) is dropped too. A sentence that REQUESTS ("please engrave Ginger on
  // the long bar") may carry the inscription, so it is never cut out: the reading says it is unsure and the paid reader / a person decides.
  const EXPLAIN_CUE = [
    /\bi(?:'|\u2019)?(?:m|ve|d|ll)?\s+(?:just\s+|really\s+|also\s+|only\s+|simply\s+|actually\s+)*(?:don'?t|do\s+not|can'?t|cannot|couldn'?t|didn'?t|wasn'?t|haven'?t|forgot|typed|wrote|meant|am\s+not\s+sure|am\s+sorry)\b/i,
    /\b(?:is|are|was|were)\s+(?:supposed|meant|intended)\s+to\b|\bsupposed\s+to\b/i,
    /\bkeyboard\b|\basterisks?\b|\bdegree\s+(?:signs?|symbols?|circles?|marks?)\b|\btypo\b|\bsorry\b/i
  ];
  const REQUEST_CUE = /\b(?:please|pls|plz|kindly|if\s+(?:it'?s\s+|it\s+is\s+)?(?:possible|poss)|let\s+me\s+know|engrav\w*|inscri\w*|etch\w*|thanks?|thank\s*you|thx|hello|hi|hey|dear|regards)\b/i;
  const hasCue = (list, s) => list.some(rx => rx.test(s));
  const wordCount = s => (oneLine(s).match(/[\p{L}\p{N}]+/gu) || []).length;
  const FUNCTION_WORDS = new Set('the a an is are was were to for of my that this it and on in with be as at by so i you we our your me us not'.split(' '));

  /* ── a different layout of the set (the coordinator's decision, Paul 13 Oct: the thinner bar sits on the shorter chain unless the buyer asks otherwise) ─────────────────────────────── */
  const SIZE_W = '(?:thin(?:ner)?|skinny|slim|narrow(?:er)?|small(?:er)?|little|mini|short(?:er)?|thick(?:er)?|wide(?:r)?|big(?:ger)?|large(?:r)?|long(?:er)?|chunky|bigger|smaller)';
  const CHAIN_W = '(?:\\d{1,2}\\s*(?:"|\u201d|\u2033|\'\'|in\\b|inch\\w*)|short(?:er)?|long(?:er)?|chain|necklace)';
  const LAYOUT = [
    /\b(?:swap(?:ped|ping)?|switch(?:ed|ing)?|interchang\w*|vice\s*versa|opposite|reverse(?:d)?)\b/i,
    /\bother\s+way(?:\s+(?:round|around))?\b/i,
    new RegExp('\\b' + SIZE_W + '\\s+(?:bar|charm|pendant|one)s?\\b[^.\\n]{0,50}?\\b(?:on|onto|to|for|with|goes|go)\\b[^.\\n]{0,25}?' + CHAIN_W, 'i'),
    new RegExp('\\b(?:on|onto)\\s+(?:the\\s+)?' + CHAIN_W + '[^.\\n]{0,30}?\\b(?:put|use|place|add|the)\\b[^.\\n]{0,15}?\\b' + SIZE_W + '\\s+(?:bar|charm|pendant|one)\\b', 'i'),
    /\bboth\b[^.\n]{0,30}\b(?:bars?|charms?|pendants?|names?)\b[^.\n]{0,30}\b(?:on|onto|to|in)\b[^.\n]{0,15}\b(?:same|one|single|1|a)\s+(?:chain|necklace)\b/i,
    /\b(?:on|onto|to)\s+(?:the\s+)?(?:same|one|a\s+single|single)\s+(?:chain|necklace)\b/i,
    /\bwhich\s+(?:bar|one|charm|pendant)\b|\b(?:bars?|charms?|pendants?)\s+(?:goes\s+|go\s+)?(?:on|onto)\s+(?:the\s+)?(?:\d{1,2}|short|long|opposite|other)/i
  ];
  /** The clause of the buyer's own text that asks for a different layout, or "" ('' when it asks nothing of the kind). A clause ends at a sentence mark, a line break or a bracket. */
  function layoutClause(text) {
    const s = clean(text);
    for (const clause of sentences(s)) {
      const c = oneLine(clause); if (c && LAYOUT.some(rx => rx.test(c))) return c;
    }
    return LAYOUT.some(rx => rx.test(s)) ? clip(s, 200) : '';
  }
  /** A text cut at its sentence marks, line breaks and brackets; a sentence keeps its own full stop. (No look-behind: an older Safari that has none must still load this file.) */
  function sentences(s) {
    const out = []; let cur = '';
    for (const part of str(s).split(/([.!?;]+(?:\s+|$)|\n|[()\[\]{}])/)) {
      if (/^[.!?;]+(?:\s+|$)$/.test(part)) { out.push(cur + part.replace(/\s+$/, '')); cur = ''; }
      else if (/^(?:\n|[()\[\]{}])$/.test(part)) { out.push(cur); cur = ''; }
      else cur += part;
    }
    out.push(cur);
    return out.filter(x => x.trim());
  }

  /* ── the labels (1: 16"= ...  18"= ...) ─────────────────────────────────────────────────────────────────────────────────────────────────── */
  const INCH = '(?:"|\u201d|\u201c|\u2033|\'\'|\u2019\u2019|\u2018\u2018|`` ?|in\\b\\.?|inch(?:es)?\\b)';
  const NOUN = '(?:\\s*(?:necklace|chain|bar|pendant|charm|one)s?)?';
  const SEP = '\\s*(?:=>|->|\u2192|[=:)>]|[-\u2013\u2014]+)\\s*';
  const OPEN = '(^|[\\s,;|/&+(\\[\uE001\uE003\uE005])';
  const PRE = '(?:(?:for|on)\\s+)?(?:the\\s+)?';
  const RX_LEN_MARK = new RegExp(OPEN + PRE + '(\\d{1,2}(?:\\.\\d)?)\\s*' + INCH + NOUN + SEP, 'giu');
  const RX_LEN_BARE = new RegExp(OPEN + PRE + '(\\d{1,2}(?:\\.\\d)?)' + NOUN + '\\s*[=:]\\s*', 'giu');
  const RX_WORD = new RegExp(OPEN + PRE + '(left|right|l|r|short(?:er)?|long(?:er)?|small(?:er)?|big(?:ger)?|large(?:r)?|thin(?:ner)?|thick(?:er)?|skinny|first|second|1st|2nd)(?:\\s*(?:necklace|chain|bar|pendant|charm|one|side|ear(?:ring)?))?' + SEP, 'giu');
  const RX_INDEX = new RegExp(OPEN + PRE + '(?:necklace|chain|bar|pendant|charm|piece|item|no\\.?|number|#)\\s*#?\\s*([12])' + SEP, 'giu');
  const RX_LEN_FREE = new RegExp(OPEN + '(\\d{1,2}(?:\\.\\d)?)\\s*' + INCH + '(?=\\s+\\S)', 'giu');   // "16" Anna 18" Ben": a length mark alone, with the set's own lengths
  const WORD_KIND = { left: ['side', 'L'], right: ['side', 'R'], l: ['side', 'L'], r: ['side', 'R'], short: ['rank', 0], shorter: ['rank', 0], small: ['rank', 0], smaller: ['rank', 0], thin: ['rank', 0], thinner: ['rank', 0], skinny: ['rank', 0], first: ['rank', 0], '1st': ['rank', 0],
    long: ['rank', 1], longer: ['rank', 1], big: ['rank', 1], bigger: ['rank', 1], large: ['rank', 1], larger: ['rank', 1], thick: ['rank', 1], thicker: ['rank', 1], second: ['rank', 1], '2nd': ['rank', 1] };
  const cap1 = s => s.charAt(0).toUpperCase() + s.slice(1);
  const inchLabel = n => (Number.isInteger(n) ? n : n.toFixed(1)) + '"';

  /** The labels found in a text: [{ at, end, kind, label, length?, side?, rank? }] in order of position, or { unsure: why }. A number is a length label only straight after a boundary (never after a minute mark: "96*45'53" W"). */
  function findLabels(text, lengths) {
    const hits = [];
    const add = (rx, make) => { rx.lastIndex = 0; for (let m; (m = rx.exec(text));) { const at = m.index + m[1].length, end = m.index + m[0].length, h = make(m); if (h) hits.push(Object.assign({ at, end }, h)); if (m[0].length === 0) rx.lastIndex++; } };
    const lenOk = n => n >= 8 && n <= 60;
    add(RX_LEN_MARK, m => { const n = parseFloat(m[2]); return lenOk(n) ? { kind: 'length', length: n, label: inchLabel(n) } : null; });
    add(RX_LEN_BARE, m => { const n = parseFloat(m[2]); return lengths.includes(n) ? { kind: 'length', length: n, label: inchLabel(n) } : null; });
    add(RX_WORD, m => { const k = WORD_KIND[m[2].toLowerCase()]; return k ? { kind: k[0], [k[0] === 'side' ? 'side' : 'rank']: k[1], label: cap1(m[2].toLowerCase()) } : null; });
    add(RX_INDEX, m => ({ kind: 'rank', rank: +m[2] - 1, label: '#' + m[2] }));
    // the same spot read two ways (a bare "16 =" inside a "16\"=" ): the longer match wins
    hits.sort((a, b) => a.at - b.at || b.end - a.end);
    let out = []; for (const h of hits) if (!out.length || h.at >= out[out.length - 1].end) out.push(h);
    // a word label ("Left:", "Short chain -", "Necklace 2 =") is a label only beside another one: a lone "Long - Island" or "Second: chance" in a name is the buyer's own
    if (out.filter(h => h.kind !== 'length').length < 2) out = out.filter(h => h.kind === 'length');
    // no separator at all, only the set's own lengths with a length mark: "16" Anna 18" Ben" (the first is at the very start; coordinates never match: their 53" follows a minute mark)
    if (!out.length && lengths.length >= 2) {
      const free = []; RX_LEN_FREE.lastIndex = 0;
      for (let m; (m = RX_LEN_FREE.exec(text));) { const n = parseFloat(m[2]); if (lengths.includes(n)) free.push({ at: m.index + m[1].length, end: m.index + m[0].length, kind: 'length', length: n, label: inchLabel(n) }); if (m[0].length === 0) RX_LEN_FREE.lastIndex++; }
      if (free.length >= 2 && free[0].at === 0 && new Set(free.map(f => f.length)).size === free.length) return free;
    }
    return out;
  }

  /* ── the reading ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const OPENERS = { '(': '\uE001', ')': '\uE002', '[': '\uE003', ']': '\uE004', '{': '\uE005', '}': '\uE006' };
  const RESTORE = { '\uE001': '(', '\uE002': ')', '\uE003': '[', '\uE004': ']', '\uE005': '{', '\uE006': '}' };
  const restore = s => s.replace(/[\uE001-\uE006]/g, c => RESTORE[c]);

  /** Take out the brackets that plainly talk to the shop (innermost first); the others are the buyer's own words and stay (their brackets are kept). */
  function peelBrackets(s, out) {
    const rx = /[(\[{]([^()\[\]{}]*)[)\]}]/;
    for (let guard = 0, m; guard < 30 && (m = rx.exec(s)); guard++) {
      const inner = m[1], whole = m[0];
      if (hasCue(BRACKET_CUE, inner)) { out.dropped.push({ text: oneLine(whole), why: 'a note to the shop, not engraving' }); s = s.slice(0, m.index) + ' ' + s.slice(m.index + whole.length); continue; }
      const sentence = wordCount(inner) >= 5 && inner.split(/\s+/).filter(w => FUNCTION_WORDS.has(w.toLowerCase())).length >= 2;
      if (sentence) { out.unsure = true; out.unsureWhy = out.unsureWhy || `the bracket “${clip(whole, 60)}” may be a note to the shop`; }
      s = s.slice(0, m.index) + OPENERS[whole[0]] + inner + OPENERS[whole[whole.length - 1]] + s.slice(m.index + whole.length);
    }
    return s;
  }

  /** The sentences / lines of a run of words that only EXPLAIN are dropped when they stand apart from other words; a request that may carry the inscription makes the reading unsure (it is never cut out). */
  /** A line cut into clauses: [clause, separator, clause, ...]; a clause keeps its own full stop. */
  function splitClauses(line) {
    const out = [], rx = /([.!?;]+)(\s+)|(\s+[–—]\s+)|(\s+-\s+(?=[A-Za-z]))/g; let last = 0, m;
    while ((m = rx.exec(line))) { if (m[1] !== undefined) out.push(line.slice(last, m.index) + m[1], m[2]); else out.push(line.slice(last, m.index), m[0]); last = m.index + m[0].length; }
    out.push(line.slice(last));
    return out;
  }
  const isLayoutClause = c => LAYOUT.some(rx => rx.test(c));
  function peelClauses(words, out, apart) {   // apart: these words stand apart from the engraved ones already (the text before the first label)
    const lines = words.split('\n').map(splitClauses), total = apart ? 2 : lines.reduce((n, parts) => n + Math.ceil(parts.length / 2), 0);
    const kept = [];
    for (const parts of lines) {
      let line = '', cut = false;
      for (let i = 0; i < parts.length; i += 2) {
        const clause = parts[i], sep = i > 0 ? parts[i - 1] : '';
        const layout = isLayoutClause(clause), explains = hasCue(EXPLAIN_CUE, clause) && wordCount(clause) >= 3;
        if (layout || explains) {
          if (total >= 2) { out.dropped.push({ text: oneLine(clause), why: layout ? 'a request about the layout, not engraving' : 'a note to the shop, not engraving' }); cut = true; continue; }
          out.unsure = true; out.unsureWhy = out.unsureWhy || `“${clip(clause, 60)}” may be a note to the shop`;
        } else if (REQUEST_CUE.test(clause) && wordCount(clause) >= 2) { out.unsure = true; out.unsureWhy = out.unsureWhy || `“${clip(clause, 60)}” asks for something and may hold the words`; }
        line += (line ? sep : '') + clause;
      }
      if (cut) line = line.replace(/[.;]+\s*$/, '');   // (the full stop that ended the sentence before a note we cut was the note's boundary, not the buyer's punctuation)
      if (line.trim()) kept.push(line);
    }
    return kept.join('\n');
  }

  const tidy = s => restore(str(s)).split('\n').map(l => l.replace(/[ \t]+/g, ' ').replace(/^(?:[\s,;|/&+:=]|[\u2013\u2014-]+(?=\s))+/, '').replace(/(?:[\s,;|/&+]|\band\b|\s[\u2013\u2014-]+)+$/i, '').trim()).filter(Boolean).join('\n');   // (a minus sign that belongs to a number, "-96.7", stays)

  function readPersonalisation(text, o) {
    o = o || {};
    const lengths = (Array.isArray(o.lengths) ? o.lengths : []).map(Number).filter(n => n > 0);
    const raw = (Array.isArray(text) ? text : [text]).map(clean).filter(Boolean).join('\n');
    const out = { raw, labelled: false, pieces: [], whole: '', lines: [], dropped: [], fixes: [], layoutRequest: false, layoutNote: '', unsure: false, unsureWhy: '', changed: false, lengths };
    if (!raw) return out;
    const lay = layoutClause(raw); if (lay) { out.layoutRequest = true; out.layoutNote = clip(lay, 200); }
    let s = peelBrackets(raw, out);
    // the labels (on the text with the notes taken out, so a note can never hide or fake one)
    const labels = findLabels(s, lengths);
    const finish = (words, extra) => {
      let w = peelClauses(tidy(words), out);
      w = tidy(w);
      const d = fixDegrees(w); w = d.text; if (d.count) out.fixes.push({ kind: 'degree', count: d.count });
      return Object.assign({ words: w }, extra || {});
    };
    if (labels.length) {
      const kinds = new Set(labels.map(l => l.kind)), labs = labels.map(l => l.kind + ':' + (l.length != null ? l.length : l.side != null ? l.side : l.rank));
      if (kinds.size > 1) { out.unsure = true; out.unsureWhy = out.unsureWhy || 'the labels are of different kinds'; }
      else if (new Set(labs).size !== labs.length) { out.unsure = true; out.unsureWhy = out.unsureWhy || `the label ${labels[0].label} is used twice`; }
      const lead = tidy(peelClauses(tidy(s.slice(0, labels[0].at)), out, true));   // (a layout request or an explanation before the first label goes the same way as anywhere else)
      if (lead) { out.unsure = true; out.unsureWhy = out.unsureWhy || `“${clip(lead, 40)}” comes before the first label`; }
      labels.forEach((l, i) => {
        const seg = s.slice(l.end, i + 1 < labels.length ? labels[i + 1].at : s.length);
        const p = finish(seg, { label: l.label, kind: l.kind, raw: oneLine(restore(seg)) });
        if (l.length != null) p.length = l.length; if (l.side != null) p.side = l.side; if (l.rank != null) p.rank = l.rank;
        if (!p.words) { out.unsure = true; out.unsureWhy = out.unsureWhy || `the label ${l.label} has no words`; }
        out.pieces.push(p);
      });
      out.labelled = true;   // (no `whole` for a labelled reading: the words of two pieces are never one text, that glue was the mistake)
    } else {
      out.whole = finish(s).words;
      out.lines = out.whole.split('\n').filter(Boolean);
      // never let the notes take every word: an empty result is the reading saying "I cannot tell"
      if (!out.whole) { out.unsure = true; out.unsureWhy = out.unsureWhy || 'nothing is left once the notes are taken out'; }
    }
    out.changed = out.labelled || out.dropped.length > 0 || out.fixes.length > 0 || out.whole.replace(/\s+/g, ' ') !== raw.replace(/\s+/g, ' ');
    return out;
  }

  /** The wordings of a labelled reading, each beside its label: 16" → ... · 18" → ... */
  const wordings = r => (r && r.pieces ? r.pieces : []).map(p => `${p.label} → ${clip(p.words.replace(/\n/g, ' / '), 60) || 'nothing'}`).join(' · ');
  /** One plain line of what was read (the card's reason line, the timeline). */
  function say(r) {
    if (!r) return '';
    const bits = [];
    if (r.labelled) bits.push('Read ' + r.pieces.map(p => `“${p.label}” as ${p.words ? '“' + clip(p.words.replace(/\n/g, ' / '), 60) + '”' : 'nothing'}`).join(' and '));
    else if (r.changed && r.whole) bits.push(`Read “${clip(r.whole.replace(/\n/g, ' / '), 80)}”`);
    const deg = r.fixes.filter(f => f.kind === 'degree').reduce((a, f) => a + f.count, 0);
    if (deg) bits.push(`${deg === 1 ? 'an asterisk' : deg + ' asterisks'} read as the degree sign °`);
    if (r.dropped.length) bits.push('left out as a note to the shop: ' + r.dropped.map(d => `“${clip(d.text, 160)}”`).join(', '));
    if (r.layoutRequest) bits.push(`the buyer asks for a different layout (“${clip(r.layoutNote, 90)}”)`);
    return bits.join('; ');
  }

  /* ── lengths of a set ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const LEN_PAIR = new RegExp('(\\d{2}(?:\\.\\d)?)\\s*' + INCH + '?\\s*(?:&|\\band\\b|\\+|/|,)\\s*(\\d{2}(?:\\.\\d)?)\\s*' + INCH + '?', 'iu');
  const LEN_ONE = new RegExp('(?:^|[\\s\u00b7|/,(])(\\d{2}(?:\\.\\d)?)\\s*' + INCH, 'iu');
  /** The necklace lengths a text names, ascending. A pair needs a length mark on one of its numbers (a date "12 & 14" is no length). */
  function lengthsOf(v) {
    const t = decode(Array.isArray(v) ? v.join(' ') : v);
    const m = LEN_PAIR.exec(t);
    if (m && INCH_RX.test(m[0])) return [parseFloat(m[1]), parseFloat(m[2])].sort((a, b) => a - b);
    const one = LEN_ONE.exec(t); return one ? [parseFloat(one[1])] : [];
  }
  const INCH_RX = new RegExp(INCH, 'iu');
  /** The lengths of a set from an Etsy line's options ("Necklace Length·FONT": '16"&18"·Set·ANGELINA'); the personalisation box is not an option. */
  function lengthsOfLine(line) {
    const vs = (line && Array.isArray(line.variations) ? line.variations : []);
    for (const v of vs) {
      const name = str(v && (v.name != null ? v.name : v.formatted_name)), value = str(v && (v.value != null ? v.value : v.formatted_value));
      if (/personali[sz]ation/i.test(name)) continue;
      const l = lengthsOf(value); if (l.length) return l;
    }
    return [];
  }

  /* ── to engrave or not (4) ─────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const TOKEN_SPLIT = /\s*[\u00b7|\/,+•]\s*|\s+[-\u2013\u2014]\s+/;
  const isBlank = v => str(decode(v)).split(TOKEN_SPLIT).some(seg => /^\s*blank\s*$/i.test(seg)) || /(?:^|[\s\u00b7|\-\u2013+\/,])blank(?:$|[\s\u00b7|\-\u2013+\/,])/i.test(str(decode(v)));
  const isEngraved = v => str(decode(v)).split(TOKEN_SPLIT).some(seg => /^\s*(?:engraved|engraving)\s*$/i.test(seg));
  const ENGRAVING_ASK = /\b(?:engrav\w*|inscri\w*|etch\w*|personali[sz]\w*|initials?|monogram\w*)\b/i;
  /** The engraving yes / no / ask of a line. o: { options:[{name,value}], words (the buyer wrote personalisation words), note (a buyer message / staff note says something about the engraving) }.
   *    BLANK            the buyer chose no engraving: nothing is engraved. With words (or a note asking for engraving) the line WAITS for a person: 'ask' ("blank, but the buyer wrote ...").
   *    ENGRAVED         engrave. With nothing written anywhere it is 'ask': the buyer paid for engraving and gave no words, a person asks.
   *    neither          the general rule: a line with personalisation words is engraved ('yes'), a line with none is 'no'.
   *  Both tokens on one line (two options disagree) is 'ask'. */
  function decide(o) {
    o = o || {};
    const opts = (Array.isArray(o.options) ? o.options : []).filter(x => x && !/personali[sz]ation/i.test(str(x.name)));
    const blank = opts.find(x => isBlank(x.value)), engraved = opts.find(x => isEngraved(x.value));
    const words = !!o.words, note = !!o.note, anyNote = !!o.anyNote || note;   // (note: a message that asks for engraving; anyNote: the buyer or the shop wrote anything at all, where the words may be)
    const opt = x => (x ? { name: str(x.name), value: str(x.value) } : null);
    if (blank && engraved) return { engrave: 'ask', token: '', option: opt(blank), why: `two options disagree: “${clip(blank.value, 40)}” (no engraving) and “${clip(engraved.value, 40)}” (engraved): a person decides` };
    if (blank) {
      if (words) return { engrave: 'ask', token: 'BLANK', option: opt(blank), why: `the option “${clip(blank.value, 40)}” says BLANK (no engraving), but the buyer wrote words: nothing is engraved until a person decides` };
      if (note) return { engrave: 'ask', token: 'BLANK', option: opt(blank), why: `the option “${clip(blank.value, 40)}” says BLANK (no engraving), but a note asks for engraving: nothing is engraved until a person decides` };
      return { engrave: 'no', token: 'BLANK', option: opt(blank), why: `the option “${clip(blank.value, 40)}” says BLANK: no engraving` };
    }
    if (engraved) {
      if (words || anyNote) return { engrave: 'yes', token: 'ENGRAVED', option: opt(engraved), why: `the option “${clip(engraved.value, 40)}” says ENGRAVED` };
      return { engrave: 'ask', token: 'ENGRAVED', option: opt(engraved), why: `the buyer chose “${clip(engraved.value, 40)}” (ENGRAVED) but wrote no words: a person asks the buyer what to engrave` };
    }
    return words ? { engrave: 'yes', token: '', option: null, why: 'the buyer wrote words and no option says BLANK' } : { engrave: 'no', token: '', option: null, why: 'no words to engrave' };
  }
  /** Does a note (buyer message, staff note) ask for engraving? (BLANK with such a note waits for a person; any other note on a BLANK line is no reason to ask.) */
  const noteAsksEngraving = t => ENGRAVING_ASK.test(decode(t));

  /* ── the join: which wording goes on which piece (1) — THE ONE PLACE ───────────────────────────────────────────────────────────────────────────── */
  /** slots: [{ slot, length? , rank?, side? }]. A length label goes to the slot hanging on a chain of that length; failing lengths, the shortest label goes to rank 0 (the thinner bar), the next to rank 1
   *  (Paul's rule, 13 Oct). A side label goes to slot 'L' / 'R'; a rank label to its rank. Every piece needs exactly one wording and every wording exactly one piece, or ok is false and nothing is assigned. */
  function assign(reading, slots) {
    const res = { ok: false, bySlot: {}, missing: [], unmatched: [], why: '' };
    const sl = (slots || []).filter(x => x && x.slot);
    if (!reading || !reading.labelled || !reading.pieces.length) { res.why = 'the buyer gave no labelled words'; return res; }
    if (!sl.length) { res.why = 'nothing says which chain each piece hangs on'; res.unmatched = reading.pieces.map(p => p.label); return res; }
    const lens = reading.pieces.filter(p => p.kind === 'length').map(p => p.length).sort((a, b) => a - b);
    const used = new Set();
    for (const p of reading.pieces) {
      let hit = null;
      if (p.kind === 'length') {
        hit = sl.filter(x => x.length != null && Math.abs(x.length - p.length) < 0.01);
        if (!hit.length && sl.every(x => x.length == null) && sl.every(x => x.rank != null)) hit = sl.filter(x => x.rank === lens.indexOf(p.length));
      } else if (p.kind === 'side') hit = sl.filter(x => x.slot === p.side || x.side === p.side);
      else hit = sl.filter(x => x.rank === p.rank);
      if (hit.length === 1 && !used.has(hit[0].slot) && p.words) { used.add(hit[0].slot); res.bySlot[hit[0].slot] = { label: p.label, words: p.words, length: p.length != null ? p.length : null }; }
      else res.unmatched.push(p.label);
    }
    res.missing = sl.filter(x => !used.has(x.slot)).map(x => x.slot);
    res.ok = !res.unmatched.length && !res.missing.length;
    res.why = res.ok ? '' : res.unmatched.length ? `the label ${res.unmatched.join(', ')} matches no piece` : `${res.missing.join(', ')} has no words of its own`;
    return res;
  }

  /** THE HOOK. Set by the page: CharmNestEngraveRead.slotInfo = (row, slot) => ({ length: 16 }) | ({ rank: 0 }) | 16 | null. Until it is set nothing is assigned (the person sees every wording). */
  const api = { slotInfo: null };
  function slotsOf(row, jobs) {
    const fn = api.slotInfo; if (typeof fn !== 'function') return null;
    const out = [];
    for (const j of jobs || []) {
      const slot = j && (j.slot || (j.row && j.row.slot)); if (!slot) return null;
      let v = null; try { v = fn(row, slot, j); } catch (_) { v = null; }
      if (typeof v === 'number' && isFinite(v)) v = { length: v };
      if (!v || typeof v !== 'object' || (!isFinite(v.length) && !isFinite(v.rank))) return null;
      out.push({ slot, length: isFinite(v.length) ? +v.length : null, rank: isFinite(v.rank) ? +v.rank : null });
    }
    return out.length ? out : null;
  }

  return Object.assign(api, { readPersonalisation, say, wordings, fixDegrees, lengthsOf, lengthsOfLine, decide, noteAsksEngraving, layoutClause, assign, slotsOf, clean, decode });
});
