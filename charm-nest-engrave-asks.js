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
    const whole = clause(t);
    if (whole) { if (whole.strong) { whole.asks.forEach(a => asks.push({ ...a, quote: t })); removed.push(t); return { text: '', asks, removed, weak }; } return { text: t, asks, removed, weak: true }; }
    // clauses set off by punctuation, from both ends
    const parts = t.split(SEP);   // [seg, sep, seg, sep, seg]
    const popped = [];
    while (parts.length > 1) { const c = clause(parts[parts.length - 1]); if (c && c.strong) { popped.push(parts[parts.length - 1]); c.asks.forEach(a => asks.push({ ...a, quote: parts[parts.length - 1].trim() })); parts.pop(); parts.pop(); } else { if (c) weak = true; break; } }
    while (parts.length > 1) { const c = clause(parts[0]); if (c && c.strong) { popped.push(parts[0]); c.asks.forEach(a => asks.push({ ...a, quote: parts[0].trim() })); parts.shift(); parts.shift(); } else { if (c) weak = true; break; } }
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
  function asksOf(text) {
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
