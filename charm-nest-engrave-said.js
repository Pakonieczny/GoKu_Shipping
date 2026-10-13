/* WHAT THE CUSTOMER WROTE, for the engraving screens (ENGNOTE, Paul, 12 Oct 2026, round 2, point 6: "I see that you removed the customer note from this view. I didn't ask for you to remove that and you need
 * to put that back so the user can understand and see what the customer wrote/communicated.").
 *
 * The customer's own words about an order line, as one small labelled box: the personalisation text, the buyer's message, the staff note, and the choices the customer made among the listing's options that are
 * about the engraving (the font, front / back / both sides, engraving or not, where). Pure reading of the line the page already holds: no network, no clock, no paid call, nothing written.
 *
 *   rowsOf(row)    -> [[label, text]]   Personalisation | Buyer message | Staff note | Options (one "Name: value" per line); a part the customer left empty is not in the list
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

  /** What the customer wrote and chose about this line, part by part: [[label, text]], empty when they wrote and chose nothing. */
  function rowsOf(row) {
    const L = lineOf(row), sp = L.spec || (row && row.spec) || {}, line = L.line || (row && row.line) || {};
    const out = [];
    const pers = (sp.personalization && sp.personalization.length ? sp.personalization : line.personalization || []).map(visible).filter(Boolean);
    if (pers.length) out.push(['Personalisation', pers.join('\n')]);
    const bm = visible(sp.buyerMessage); if (bm) out.push(['Buyer message', bm]);
    const sn = visible(sp.staffNote); if (sn) out.push(['Staff note', sn]);
    const ch = choicesOf(row); if (ch.length) out.push(['Options', ch.map(c => c.name ? c.name + ': ' + c.value : c.value).join('\n')]);
    return out;
  }

  /** The box: a title that says plainly what it is, then each part under its small label (the look of the order window's "What the customer wrote"). "" when there is nothing to show. */
  function html(row, o) {
    const rows = rowsOf(row); if (!rows.length) return '';
    const box = `<div class="owSaid">${rows.map(([k, v]) => `<span class="lbl">${esc(k)}</span> ${esc(v)}`).join('')}</div>`;
    return `<section class="pvSec pvSaid" data-said>${o && o.bare ? '' : '<h4 class="pvH">What the customer wrote</h4>'}${box}</section>`;
  }

  return { rowsOf, choicesOf, html, ENGRAVING_WORD };
});
