/* WHAT THE CUSTOMER WROTE, for the engraving screens (ENGNOTE, Paul, 12 Oct 2026, round 2, point 6: "I see that you removed the customer note from this view. I didn't ask for you to remove that and you need
 * to put that back so the user can understand and see what the customer wrote/communicated.").
 *
 * The customer's own words about an order line, as one small labelled box: the personalisation text, the buyer's message, the staff note, and the choices the customer made among the listing's options that are
 * about the engraving (the font, front / back / both sides, engraving or not, where). Pure reading of the line the page already holds: no network, no clock, no paid call, nothing written.
 *
 *   rowsOf(row)    -> [[label, text]]   Personalisation | Buyer message | Staff note | Options (one "Name: value" per line, said as CharmNestOrders.optionViews says it: "Metal: Gold · Engraved", OPTTIDY 13 Oct 2026;
 *                                       the original is the label's hover); a part the customer left empty is not in the list
 *   choicesOf(row) -> [{ name, value }] the engraving-related option choices of the line (the order window's one extra line)
 *   html(row)      -> string            `<section class="pvSec pvSaid" data-said>` with the title "What the customer wrote" and the `.owSaid` box the order window uses ("" when the customer wrote nothing:
 *                                       no empty box); `html(row, { bare: true })` is the box without the section's title (not used by the screens, for a caller that draws its own)
 *
 * `row` is a line of an order (or the side row of one of its pieces: the line is read through `parentRow`). Loads in the page (window.CharmNestEngraveSaid) and in node (module.exports; CharmNestOrders from `root`). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestEngraveSaid = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const O = () => root.CharmNestOrders || (typeof globalThis !== 'undefined' && globalThis.CharmNestOrders) || null;   // (the page: window; node: a global the caller set)
  const str = x => String(x == null ? '' : x);
  const esc = s => str(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const lineOf = row => (row && row.parentRow) || row || {};
  // (ENGENT: Etsy hands the customer's words HTML-escaped, "Mom&#39;s &amp; Dad"; they are read decoded with the app's one decoder and escaped again by esc() when drawn, so "&lt;b&gt;" stays text. Display only: nothing stored changes.)
  const visible = x => { const o = O(), t = o && o.visible ? o.visible(x) : str(x); return (o && typeof o.decodeEntities === 'function' ? o.decodeEntities(t) : t).trim(); };
  // an option is "about the engraving" when its name or its value says so: a font, an engraving word, a face (front / back / both) or a count of sides, a place for the words
  const ENGRAVING_WORD = /engrav|font|typeface|lettering|inscri|monogram|etch|writing|alignment|placement|position|location|\b(?:front|back|both|sides?|sided|faces?)\b|\bblank\b/i;
  const norm = s => str(s).replace(/\s+/g, ' ').trim().toLowerCase();

  /** The options the customer chose that are about the engraving: [{ name, value }] in the order of the listing. The personalisation box itself is not an option (it is said as the personalisation). */
  function choicesOf(row) {
    const L = lineOf(row), sp = L.spec || (row && row.spec) || {}, o = O();
    if (!o || typeof o.purchaseOptions !== 'function') return [];
    let bought = []; try { bought = o.purchaseOptions(L.line || (row && row.line) || {}, sp) || []; } catch (_) { bought = []; }
    // an option the line reader settled as saying the engraving (a person's saved answer for the value, the Mini Tag rule) counts whatever its words are
    const settled = new Set((sp.options || []).filter(x => x && x.mapped && /^(?:front|both|back)$/.test(str(x.mapped.engrave))).map(x => norm(x.name) + '\n' + norm(x.value)));
    return bought.filter(v => {
      const name = str(v.name), value = str(v.value);
      if (!value) return false;
      if (settled.has(norm(name) + '\n' + norm(value))) return true;
      if (typeof o.isFontOption === 'function' && o.isFontOption(name)) return true;
      if (typeof o.engravingModeOfOption === 'function' && o.engravingModeOfOption(name, value)) return true;
      if (typeof o.sidesOfValue === 'function' && o.sidesOfValue(value) > 0) return true;
      return ENGRAVING_WORD.test(name) || ENGRAVING_WORD.test(value);
    }).map(v => ({ name: str(v.name), value: str(v.value) }));
  }

  /** OPTTIDY (Paul, 13 Oct 2026: "strip out the needless/filler text from these options and only show the relevant information from each dropdown selection"): the choices as the app's one display reader
   *  tidies them (CharmNestOrders.optionViews: "Metal: Gold · Engraved", "Length: 16" & 18" · Font: Angelina"); null when the reader is not on the page, and the plain "name: value" lines are drawn. DISPLAY ONLY:
   *  choicesOf still answers the options as the line holds them, and the original reads in the label's title. */
  function viewsOf(row, ch) {
    const L = lineOf(row), sp = L.spec || (row && row.spec) || {}, line = L.line || (row && row.line) || {}, o = O();
    if (!o || typeof o.optionViews !== 'function') return null;
    let all = ch; try { all = o.purchaseOptions(line, sp) || ch; } catch (_) { all = ch; }
    try { return o.optionViews(ch, line, all); } catch (_) { return null; }
  }
  const plainLine = c => (c.name ? c.name + ': ' + c.value : c.value);
  /** What the customer wrote and chose about this line, part by part: [[label, text]], empty when they wrote and chose nothing. */
  function rowsOf(row) {
    const L = lineOf(row), sp = L.spec || (row && row.spec) || {}, line = L.line || (row && row.line) || {};
    const out = [];
    const pers = (sp.personalization && sp.personalization.length ? sp.personalization : line.personalization || []).map(visible).filter(Boolean);
    if (pers.length) out.push(['Personalisation', pers.join('\n')]);
    const bm = visible(sp.buyerMessage); if (bm) out.push(['Buyer message', bm]);
    const sn = visible(sp.staffNote); if (sn) out.push(['Staff note', sn]);
    const ch = choicesOf(row); if (ch.length) { const vs = viewsOf(row, ch); out.push(['Options', (vs ? vs.map(v => v.line) : ch.map(plainLine)).join('\n')]); }
    return out;
  }

  // SMARTNOTE (Paul, 15 Nov 2026: "This needs to be an intelligent customer note where the system interprets what the customer is actually looking for ... Sometimes customers will add images or links here; those
  // links need to be clickable. They need to be itemized."): the same words, itemized, links made clickable, and the plain asks (font, side, place, size, case, layout) read by rules. No AI, display only.
  const URL_RE = /https?:\/\/(?:(?!&quot;|&#39;|&lt;|&gt;)[^\s<>"'])+/gi;
  const IMG_EXT = /\.(?:jpe?g|png|webp|gif)$/i;
  const urlParts = u => { const m = /^(https?:\/\/)([^\/?#]+)([^?#]*)/i.exec(u); return m ? { host: m[2].toLowerCase().replace(/:\d+$/, ''), path: m[3] } : null; };
  const unEsc = u => u.replace(/&amp;/g, '&');
  const isImageUrl = u => { const p = urlParts(unEsc(u)); return !!p && (p.host === 'i.etsystatic.com' || IMG_EXT.test(p.path)); };
  // the links of an ESCAPED text: [{ at, len, u (still escaped), tail }] (a trailing . , ; ) is not part of the link; an entity's own ";" stays)
  function scan(e) {
    const out = []; let m; const re = new RegExp(URL_RE.source, 'gi');
    while ((m = re.exec(e))) {
      let u = m[0], tail = '';
      for (;;) { const t = u.replace(/[.,;)]$/, ''); if (t === u || /&(?:#\d+|[a-z]+)$/i.test(t)) break; tail = u.slice(t.length) + tail; u = t; }
      if (/^https?:\/\/[^\s]{2,}/i.test(u)) out.push({ at: m.index, len: m[0].length, u, tail });
    }
    return out;
  }
  /** The http(s) links of a text, as written. */
  const urlsOf = text => scan(esc(text)).map(x => unEsc(x.u));
  /** The text, escaped FIRST, then each http(s) link made a link that opens in a new tab (never javascript: / data:); a picture link also shows a small thumbnail that opens the picture. */
  function linkify(text) {
    const e = esc(text); let out = '', at = 0;
    for (const { at: i, len, u, tail } of scan(e)) {
      out += e.slice(at, i);
      const a = `target="_blank" rel="noopener noreferrer" referrerpolicy="no-referrer"`;
      if (isImageUrl(u)) out += `<a class="owThumb" href="${u}" ${a}><img src="${u}" loading="lazy" referrerpolicy="no-referrer" alt=""></a> `;
      out += `<a class="owLink" href="${u}" ${a}>${u}</a>${tail}`; at = i + len;
    }
    return out + e.slice(at);
  }

  // the plain asks, by a short table of phrases (conservative: a phrase that could be about the product, not the engraving, needs an engraving word near it)
  const CUE = /text|font|letter|writ|name|words?|engrav|initial|size|make|print|line/i;
  const near = (t, m, w) => CUE.test(t.slice(Math.max(0, m.index - w), m.index + m[0].length + w));
  const PHRASES = [
    ['position', 'left', /\b(?:(?:on|to|at|in)\s+the\s+left|left[\s-]*(?:side|aligned?|justified?|hand))\b/i],
    ['position', 'right', /\b(?:(?:on|to|at|in)\s+the\s+right|right[\s-]*(?:side|aligned?|justified?|hand))\b/i],
    ['position', 'top', /\b(?:(?:on|at|in|to)\s+the\s+top|top[\s-]*(?:side|aligned?|of the))\b/i],
    ['position', 'bottom', /\b(?:(?:on|at|in|to)\s+the\s+bottom|bottom[\s-]*(?:side|aligned?|of the))\b/i],
    ['position', 'centre', /\b(?:(?:in|at|on|to)\s+the\s+(?:centre|center|middle)|cent(?:re|er)(?:ed|d)|(?:centre|center)[\s-]*aligned?)\b/i],
    ['size', 'small', /\b(?:smaller|small|tiny)\b/i, true],
    ['size', 'big', /\b(?:bigger|big|larger|large)\b/i, true],
    ['case', 'all caps', /\b(?:all[\s-]*caps(?:ital(?:s|\s+letters)?)?|upper[\s-]*case|block\s+capitals|capital\s+letters)\b/i],
    ['case', 'lowercase', /\b(?:lower[\s-]*case|all\s+lower|no\s+caps|small\s+letters)\b/i],
    ['layout', 'one line', /\b(?:one|single|1)[\s-]*line\b/i],
    ['layout', 'two lines', /\b(?:two|2)[\s-]*lines\b/i],
    ['layout', 'vertical', /\bvertical(?:ly)?\b/i, true]
  ];
  const phrasesIn = text => {
    const out = [];
    for (const [type, value, re, cue] of PHRASES) { const m = re.exec(text); if (m && (!cue || near(text, m, 30))) out.push({ type, value }); }
    return out;
  };
  const SAY = { position: v => v + ' side', size: v => v + ' text', case: v => v, layout: v => v, side: v => v, font: v => v };
  const SAY_POS = { top: 'at the top', bottom: 'at the bottom', centre: 'in the centre' };

  /** The note, itemized: [{ kind, label, text, urls, note, type?, value? }] - kinds: option | file | personalisation | message | staff | instruction. */
  function itemsOf(row) {
    const L = lineOf(row), sp = L.spec || (row && row.spec) || {}, line = L.line || (row && row.line) || {}, o = O();
    const items = [], mk = (kind, label, text, extra) => Object.assign({ kind, label, text, urls: urlsOf(text), note: '' }, extra || {});
    const texts = [];
    // options: the engraving ones, and any whose value is only a link (the customer's file)
    const ch = choicesOf(row), keep = new Set(ch.map(c => norm(c.name) + '\n' + norm(c.value)));
    const views = ch.length ? viewsOf(row, ch) : null;
    let all = ch; try { if (o && typeof o.purchaseOptions === 'function') all = o.purchaseOptions(line, sp) || ch; } catch (_) { all = ch; }
    for (const v of all) {
      const name = str(v.name), value = str(v.value);
      if (/^\s*https?:\/\/[^\s<>"]+\s*$/i.test(value)) items.push(mk('file', 'Customer file', value, { name }));
      else if (keep.has(norm(name) + '\n' + norm(value))) { const k = ch.findIndex(c => c.name === name && c.value === value), vw = k >= 0 && views ? views[k] : null;   // (OPTTIDY: the tidied line; the original is the hover)
        items.push(mk('option', 'Option', vw ? vw.line : plainLine({ name, value }), { name, value, was: vw && vw.changed ? vw.was : '' })); }
    }
    const pers = (sp.personalization && sp.personalization.length ? sp.personalization : line.personalization || []).map(visible).filter(Boolean);
    for (const t of pers.join('\n').split('\n').map(x => x.trim()).filter(Boolean)) { items.push(mk('personalisation', 'Personalisation', t)); texts.push(t); }
    const bm = visible(sp.buyerMessage); if (bm) { items.push(mk('message', 'Buyer message', bm)); texts.push(bm); }
    const sn = visible(sp.staffNote); if (sn) { items.push(mk('staff', 'Staff note', sn)); texts.push(sn); }
    // the asks, read from the free words only (the options already say their own)
    const asks = [], seen = new Set(), add = (type, value, text) => { const k = type + '\n' + value; if (!seen.has(k)) { seen.add(k); asks.push(mk('instruction', 'Asked', text, { type, value })); } };
    const free = texts.join('\n');
    if (free) {
      try { const fr = o && typeof o.fontReadText === 'function' ? o.fontReadText(free) : null; const r = fr && fr.requests && fr.requests.find(q => q.asked); if (r) add('font', r.asked, r.asked + ' font'); } catch (_) { /* no font read */ }
      try { const m = o && typeof o.engravingModeIn === 'function' ? o.engravingModeIn(free, true) : ''; if (m) add('side', m, m === 'both' ? 'both sides' : m + ' side'); } catch (_) { /* no side read */ }
      for (const t of texts) for (const { type, value } of phrasesIn(t)) add(type, value, type === 'position' ? (SAY_POS[value] || value + ' side') : SAY[type](value));
    }
    return items.concat(asks);
  }

  /** The box: a title that says plainly what it is, then each item under its small label chip. "" when there is nothing to show. `opts.applied` ([{ type, value, ok, why }]) adds "applied" / "could not apply: why" to the matching ask. */
  function html(row, o) {
    const items = itemsOf(row); if (!items.length) return '';
    const applied = o && Array.isArray(o.applied) ? o.applied : null;
    const status = it => {
      if (!applied || it.kind !== 'instruction') return '';
      const a = applied.find(x => x && x.type === it.type && (x.value == null || norm(x.value) === norm(it.value)));
      return a ? (a.ok ? ' <span class="owOk">applied</span>' : ` <span class="owNo">could not apply${a.why ? ': ' + esc(a.why) : ''}</span>`) : '';
    };
    const box = `<div class="owSaid">${items.map(it => `<div class="owI" data-kind="${it.kind}"${it.was ? ` title="${esc('As ordered:\n' + it.was)}"` : ''}><span class="lbl">${esc(it.label)}</span> <span class="owT">${linkify(it.text)}${status(it)}</span></div>`).join('')}</div>`;
    return `<section class="pvSec pvSaid" data-said>${o && o.bare ? '' : '<h4 class="pvH">What the customer wrote</h4>'}${box}</section>`;
  }

  return { rowsOf, choicesOf, itemsOf, linkify, html, ENGRAVING_WORD };
});
