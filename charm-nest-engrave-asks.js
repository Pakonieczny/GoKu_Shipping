/* FOLLOWASK (Paul, 15 Nov 2026): "the engraving that is applied by the system needs to also be smart and interpreted so that the system doesn't conflate user instructions with the actual things they want on the charm,
 * and then also sometimes users will say put it on the left side, put it on the right side, make it this or make it that; those instructions should be followed, and then to the system's best ability that engraving
 * should be applied based on the user's instructions."
 *
 * asksOf(text) -> { words, asks:[{type:'position'|'size'|'case'|'layout'|'reference', value, quote}], removed:[text], unsure:boolean }
 * Plain rules, no model, no network. It only strips a phrase that is clearly an instruction to the shop: a whole line, a bracketed note, or a clause set off by punctuation or by "please" / "on the" / "put it" /
 * "make it". A phrase that can be the inscription ("Left my heart in Rome", "Top Gun", "Big Sur", a line that is only "Right") stays as words; a bare direction / size word is `unsure` (a person confirms).
 * URLs are always `reference`, never words. Values: position left|right|top|bottom|center; size small|large; case upper|lower; layout one-line|two-lines.
 * plan(asks) -> { position, size, case, layout } (what to apply). Nothing here approves anything. English only: the repo has no word list of other languages for these. */
(function (root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveAsks = api;
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const URL_RE = /\b(?:https?:\/\/|www\.)[^\s)\]}>"']+/gi;
  const FILL = new Set('please pls plz thanks thank you kindly the a an it this that put place have make set write engrave add move align aligned keep text font letters lettering writing print size words name inscription engraving on in at to of with and but side hand corner charm can could would like want i id if possible for me my is be so just use using as possible'.split(' '));
  const POLITE = /\b(please|pls|plz|thanks|thank you|kindly|make it|put it|have it|place it|can you|could you|would like|i want|i'd like)\b/;
  const SIZE_NOUN = /\b(font|text|letters|lettering|writing|print|size|words|name)\b/;
  // [regex over the lowercase words from here on, kind, value, strong on its own]
  const PATS = [
    [/^(?:all |the )?(?:capital letters|capitals|caps|uppercase|upper case|block capitals)\b/, 'case', 'upper', true],
    [/^all (?:capital letters|capitals|caps|uppercase|upper case)\b/, 'case', 'upper', true],
    [/^(?:all )?(?:lowercase|lower case|small letters)\b/, 'case', 'lower', true],
    [/^no (?:caps|capitals)\b/, 'case', 'lower', true],
    [/^(?:on )?(?:one|1|a single|single) lines?\b/, 'layout', 'one-line', true],
    [/^(?:on |in |over )?(?:two|2) (?:lines|rows)\b|^(?:two|2) line\b/, 'layout', 'two-lines', true],
    [/^(?:smaller|tiny|little|small|petite)\b/, 'size', 'small', false],
    [/^(?:larger|bigger|largest|biggest|large|big|huge)\b/, 'size', 'large', false],
    [/^(?:as (?:big|large) as possible)\b/, 'size', 'large', true],
    [/^(?:cent(?:ere|re)d)\b/, 'position', 'center', true],
    [/^(?:left|right|top|bottom|cent(?:er|re)|middle)\b/, 'position', null, false]
  ];
  const norm = s => String(s).toLowerCase().replace(/[^a-z0-9'’\s]/g, ' ').replace(/’/g, "'").replace(/\s+/g, ' ').trim();
  /** The clause is made ONLY of instruction words -> { asks, strong } else null. strong: a person could not mean it as the inscription. */
  function clause(text) {
    let s = norm(text); if (!s) return null;
    const polite = POLITE.test(s), asks = []; let strong = false, prep = false, side = false, hasNoun = SIZE_NOUN.test(s);
    let guard = 0;
    while (s && guard++ < 40) {
      let hit = false;
      for (const [re, type, value, st] of PATS) {
        const m = re.exec(s); if (!m) continue;
        let v = value;
        if (type === 'position' && !v) { const w = /^(left|right|top|bottom|middle|cent)/.exec(m[0])[1]; v = w === 'left' ? 'left' : w === 'right' ? 'right' : w === 'top' ? 'top' : w === 'bottom' ? 'bottom' : 'center'; }
        asks.push({ type, value: v }); if (st) strong = true;
        if (type === 'size' && (hasNoun || /^(smaller|larger|bigger|largest|biggest)/.test(m[0]))) strong = true;
        s = s.slice(m[0].length).trim(); hit = true; break;
      }
      if (hit) continue;
      const w = s.split(' ')[0];
      if (w === 'side') side = true;
      if (/^(on|in|at|to)$/.test(w) && /^(?:on|in|at|to) the\b/.test(s)) prep = true;
      if (!FILL.has(w)) return null;
      s = s.slice(w.length).trim();
    }
    if (s || !asks.length) return null;
    if (asks.some(a => a.type === 'position') && (side || prep)) strong = true;
    if (polite) strong = true;
    return { asks, strong };
  }
  const BRACKET = /[(\[{]([^()\[\]{}]*)[)\]}]/g;
  const SEP = /(\s[-–—]+\s|[,;:|•])/;
  const words = t => t.split(/\s+/).filter(Boolean);
  /** One line -> { text, asks, removed, weak } */
  function line(raw) {
    let t = raw, asks = [], removed = [], weak = false;
    t = t.replace(BRACKET, (all, inner) => { const c = clause(inner); if (c && c.strong) { c.asks.forEach(a => asks.push({ ...a, quote: all.trim() })); removed.push(all.trim()); return ' '; } return all; });
    t = t.replace(/\s+/g, ' ').trim();
    if (!t) return { text: '', asks, removed, weak };
    /* ENGPROOF, Paul 15 Nov 2026: "Name: Anna — on the left side, small": the form's own label ("Name:") in front of the words is not engraved. */
    { const lab = /^(?:name|names|text|words?|initials?|personali[sz]ation|engraving)\s*:\s*(?=\S)/i.exec(t); if (lab && t.length > lab[0].length) { removed.push(lab[0].trim()); t = t.slice(lab[0].length); } }
    const whole = clause(t);
    if (whole) { if (whole.strong) { whole.asks.forEach(a => asks.push({ ...a, quote: t })); removed.push(t); return { text: '', asks, removed, weak }; } return { text: t, asks, removed, weak: true }; }
    // clauses set off by punctuation, from both ends
    const parts = t.split(SEP);   // [seg, sep, seg, sep, seg]
    const popped = [];
    /* ENGPROOF, Paul 15 Nov 2026: "Name: Anna — on the left side, small": a bare "small" is only a weak ask on its own, but beside a strong one in the same chain of clauses ("on the left side, small") it is the same instruction: the chain is taken from the end (and the start) as far as it is all instruction, when at least one of it is strong. */
    const chain = (at, step) => { const got = []; let i = at; while (i >= 0 && i < parts.length) { const c = clause(parts[i]); if (!c) break; got.push({ c, i }); i += 2 * step; if (parts.length - got.length * 2 < 1) break; } return got; };
    for (const [at, step] of [[parts.length - 1, -1], [0, 1]]) {
      if (parts.length < 3) break;
      const got = chain(at, step), cut = got.map(g => g.c.strong).lastIndexOf(true);
      if (cut < 0) { if (got.length) weak = true; continue; }
      for (const g of got.slice(0, cut + 1)) { popped.push(parts[g.i]); g.c.asks.forEach(a => asks.push({ ...a, quote: parts[g.i].trim() })); }
      const drop = new Set(); got.slice(0, cut + 1).forEach(g => { drop.add(g.i); drop.add(g.i + step); });
      for (let k = parts.length - 1; k >= 0; k--) if (drop.has(k)) parts.splice(k, 1);
      if (got.length > cut + 1) weak = true;
    }
    removed.push(...popped.map(x => x.trim()));
    t = parts.join('').replace(/\s+/g, ' ').trim();
    if (popped.length) return { text: t, asks, removed, weak };
    // a trailing clause with no punctuation: "Anna on the left side", "Anna please put it on the right", "Anna small font"
    const ws = words(t), TRIG = /^(please|pls|plz|thanks|put|place|make|have|keep|on|in|at|with|to|all|caps|uppercase|lowercase|two|one|2|1|single|small|smaller|large|larger|big|bigger|centered|centred|font|use|and)$/i;
    for (let k = 1; k < ws.length; k++) {
      if (!TRIG.test(ws[k])) continue;
      const tail = ws.slice(k).join(' '), c = clause(tail); if (!c || !c.strong) continue;
      const polite = /^(please|pls|plz|thanks|put|place|make|have|keep)$/i.test(ws[k]);
      if (k > (polite ? 6 : 3)) continue;
      c.asks.forEach(a => asks.push({ ...a, quote: tail })); removed.push(tail);
      return { text: ws.slice(0, k).join(' '), asks, removed, weak };
    }
    return { text: t, asks, removed, weak };
  }
  /* (NUMBERED, Paul 15 Nov 2026: "a person writes 'I love you best' and then the second thing it says 'center': what they really mean here is they want it centred on a bar and this is not a personalization"): a listing
     that asks two things in one box ("1. your text  2. alignment") is answered "1. I love you best 2. center". The numbers are the form's, never engraved; an answer that is only an instruction (center, left, all caps,
     small) beside an answer that has words is the instruction it reads as, not words. Answers 1, 2, 3 in order, from the start of the text; anything else is read as before. -> [answer, ...] or null */
  function numbered(text) {
    const t = String(text || '').replace(/\r/g, ''), re = /(^|\n|\s)(\d{1,2})[.)]\s+/g, marks = []; let m, want = 1;
    while ((m = re.exec(t))) { if (+m[2] !== want) continue; if (want === 1 && t.slice(0, m.index + m[1].length).trim()) return null; marks.push({ at: m.index + m[1].length, end: re.lastIndex }); want++; }
    if (!marks.length) return null;
    const items = marks.map((k, i) => t.slice(k.end, i + 1 < marks.length ? marks[i + 1].at : t.length).trim());
    return items.some(Boolean) ? items : null;
  }
  function asksOf(text) {
    const items = numbered(text);
    if (items) {
      const kept = [], asksN = [], removedN = [];
      const read = items.map(it => ({ it, c: it ? clause(it) : null }));
      const hasWords = read.some(x => x.it && !x.c);
      for (const x of read) { if (!x.it) continue; if (x.c && hasWords) { x.c.asks.forEach(a => asksN.push({ ...a, quote: x.it })); removedN.push(x.it); } else kept.push(x.it); }
      const inner = asksOfPlain(kept.join('\n'));
      const all = asksN.concat(inner.asks); let clash = false;
      for (const ty of ['position', 'size', 'case', 'layout']) if (new Set(all.filter(a => a.type === ty).map(a => a.value)).size > 1) clash = true;
      return { words: inner.words, asks: all, removed: removedN.concat(inner.removed), unsure: inner.unsure || clash, numbered: true };
    }
    return asksOfPlain(text);
  }
  function asksOfPlain(text) {
    const src = String(text == null ? '' : text), asks = [], removed = [];
    let rest = src.replace(URL_RE, m => { asks.push({ type: 'reference', value: m, quote: m }); removed.push(m); return ' '; });
    const lines = rest.split(/\r?\n/).map(l => l.replace(/[ \t]+/g, ' ').trim()).filter(Boolean);
    const out = [], found = []; let unsure = false;
    for (const l of lines) { const r = line(l); if (r.weak) unsure = true; found.push(...r.asks); removed.push(...r.removed); if (r.text) out.push(r.text); }
    // the same kind of ask with two different values ("left" and "right"): nothing is guessed
    for (const ty of ['position', 'size', 'case', 'layout']) if (new Set(found.filter(a => a.type === ty).map(a => a.value)).size > 1) unsure = true;
    const refs = asks.slice();
    if (unsure) return { words: lines.join('\n'), asks: refs, removed: removed.filter(r => refs.some(a => a.quote === r)), unsure: true };
    let w = out;
    if (found.some(a => a.type === 'layout' && a.value === 'two-lines') && w.length === 1 && /\s[\/|]\s/.test(w[0])) w = w[0].split(/\s[\/|]\s/).map(x => x.trim()).filter(Boolean);
    return { words: w.join('\n'), asks: found.concat(refs), removed, unsure: false };
  }
  /** What to apply: the first of each kind. */
  function plan(asks) { const p = {}; for (const a of asks || []) { const k = a.type === 'case' ? 'case' : a.type; if (k !== 'reference' && p[k] == null) p[k] = a.value; } return p; }
  /** The target centre (pt) of a placement along one axis inside the safe bounds [lo, hi]: the third line from the named side. */
  function thirdOf(pos, b) {
    if (pos === 'left') return { axis: 'x', to: b.x0 + (b.x1 - b.x0) / 3 };
    if (pos === 'right') return { axis: 'x', to: b.x1 - (b.x1 - b.x0) / 3 };
    if (pos === 'top') return { axis: 'y', to: b.y1 - (b.y1 - b.y0) / 3 };
    if (pos === 'bottom') return { axis: 'y', to: b.y0 + (b.y1 - b.y0) / 3 };
    return null;
  }
  const caseLines = (lines, c) => lines.map(l => c === 'upper' ? l.toUpperCase() : c === 'lower' ? l.toLowerCase() : l);
  /** The line mode for a layout ask: one line -> 1; two lines -> 2 when there is one line of words, else the lines as written. */
  const layoutMode = (v, lines) => v === 'one-line' ? '1' : v === 'two-lines' ? (lines.length === 1 ? '2' : 'preserve') : null;
  return { asksOf, plan, thirdOf, caseLines, layoutMode };
});
