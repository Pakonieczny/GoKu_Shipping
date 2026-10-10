/* CharmNestSheetName: the ONE place a sheet's number and name are decided (Paul, 10 Oct 2026, sandbox Library).
 *
 *   "There are lot's of issues here with how the sheets are names ... there are 2 SS sheets both names #1"
 *
 * Why two numberings met on one card. A sheet has TWO numbers on its record: `page` (its place among the pages of its metal in the
 * run that nested it: 1, 2, 3 ... counted over every sheet the run made, in a set or not) and `sheetIndex` (its place among the
 * sheets of its metal in its SET: given only when it joins a set, one more than the highest number in use there). The cards showed
 * `sheetIndex || page`, so a sheet in a set read by its number in the set and a draft outside every set read by its page in the run:
 * the 51/51 SS sheet that was page 2 of the run became Set-1's "Sheet 1", and the 71/122 draft that was page 1 of the run is still
 * "Sheet 1". Page 4 of the run was Set-1's third GF sheet, so the draft that was page 5 read "Sheet 5" with no 4 anywhere.
 *
 * The rule (what every surface now reads; nothing is stored differently, a name is derived from what the record already says):
 *   · A sheet IN A SET is "<metal> Sheet <n>", n = its number in that set for its metal (sheetIndex, which is also the _Sheet-<n> of
 *     its file name). Numbers start at 1 in each set and each metal. Two sheets of one metal in one set never share one.
 *   · A sheet NOT in a set (a draft: held for a later set, taken out of a set, a solid sheet not ticked in) is "<metal> Draft <p>",
 *     p = its page in the run. It has no number in any set, so it can never read like a sheet of a set. The word is the app's own
 *     ("Still a draft"). A draft taken out of a set remembers where it was: was() says "GF Sheet 3 of Set 1" (its file name).
 *   · A sheet already CUT keeps the number its file and label carry, in a set or not: it is a physical object with that name.
 *   · A number is given once and is never changed by anything else: when another sheet joins or leaves, nobody renumbers. A number
 *     that has been in use in a set is RETIRED (the set remembers the highest per metal: sheetNos), so a new sheet never takes the
 *     number of one that left (a printed QR label or a laser file with that number may still exist). The one exception is the sheet
 *     itself: it takes its own old number back when it returns to the set it left and the number is still free (claim).
 *   · Where one view lists sheets of several sets or several runs, uniqueNames() adds " · Set 2" (or the day, for drafts) to exactly
 *     the names that would otherwise read alike, so two sheets on one board never show the same name.
 *
 * Pure: reads records (Library rows, sheet documents, live page objects: metal, setId, draft, solidIncluded, sheetIndex, page,
 * fileBase / folder, laserDoneAt, roseCutAt, day), never the cloud. UMD like the other charm-nest-*.js files: window.CharmNestSheetName
 * in the page, module.exports in node and in the Netlify functions. */
(function (root, factory) { const api = factory(); if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestSheetName = api; })(typeof self !== 'undefined' ? self : this, function () {
  'use strict';
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const METAL_OF = Object.fromEntries(Object.entries(CODE).map(([m, c]) => [c, m]));
  const str = v => String(v == null ? '' : v);
  const num = v => { const n = Math.floor(+v); return Number.isFinite(n) && n > 0 ? n : 0; };
  const idOf = r => str(r && (r.id || r.sheetId));
  const fileOf = r => str(r && (r.fileBase || r.folder));
  /** "GF_Oct.03.26_Set-1_Sheet-3" -> { code:'GF', set:1, no:3 }; null for any other name (a working_ name, a bare id). */
  function fileTag(r) {
    const m = /^([A-Za-z0-9]+)_.*?_?Set-(\d+)_Sheet-(\d+)/.exec(fileOf(r)) || /^([A-Za-z0-9]+)_.*_Sheet-(\d+)/.exec(fileOf(r));
    if (!m) return null;
    return m.length === 4 ? { code: m[1], set: +m[2], no: +m[3] } : { code: m[1], set: 0, no: +m[2] };
  }
  const codeOf = r => (r && CODE[r.metal]) || (r && r.metalLabel && str(r.metalLabel).length <= 4 ? str(r.metalLabel) : '') || (fileTag(r) || {}).code || '';
  /** In a set: the same words as charm-nest-orders.js libraryGroup and charm-nest-set-edit.js inSetOf. */
  const inSet = r => !!(r && r.setId && !r.draft && r.solidIncluded !== false);
  const isCut = r => !!(r && (+r.laserDoneAt > 0 || +r.roseCutAt > 0));
  /** The number a sheet has in its set (0: none): its record's, else its file name's. Never the page. */
  const numberOf = r => num(r && r.sheetIndex) || (fileTag(r) || {}).no || 0;
  /** A sheet that has a name in a set: in one, or cut (its file and label already carry the number). A record that says nothing about
   *  membership (a partial read of the document: no setId, no draft) is read by the number it keeps, as it always was; a record that says
   *  it is a draft, or says it is in no set, has no number in any set, whatever a stale field still holds. */
  function numbered(r) {
    if (!r) return false;
    if (isCut(r)) return true;
    if (r.draft === true || r.solidIncluded === false) return false;
    if (r.setId) return true;
    if (r.setId === undefined && r.draft === undefined) return numberOf(r) > 0;
    return false;
  }
  /** The provisional number of a sheet outside every set: its page in the run. */
  const draftNo = r => num(r && r.page) || 1;
  /** The set number a record says (setSeq, seq of a set row, or its file name); 0 when it says none. */
  const setNoOf = r => num(r && r.setSeq) || num(r && r.setId ? r.seq : 0) || num((/-(\d+)$/.exec(str(r && r.setId)) || [])[1]) || (fileTag(r) || {}).set || 0;

  /** "Sheet 3" or "Draft 5": what a tab or a card says after the metal chip. */
  function short(r) {
    if (numbered(r)) return `Sheet ${numberOf(r) || num(r && r.page) || 1}`;
    return `Draft ${draftNo(r)}`;
  }
  /** "GF Sheet 3" or "GF Draft 5". */
  function name(r) { return r ? `${codeOf(r)} ${short(r)}`.trim() : ''; }
  /** What a sheet outside every set used to be, when its file name says so: "GF Sheet 3 of Set 1"; '' for a sheet that was never numbered. */
  function was(r) {
    if (numbered(r)) return '';
    const t = fileTag(r); if (!t || !t.no) return '';
    return `${t.code} Sheet ${t.no}${t.set ? ' of Set ' + t.set : ''}`;
  }
  const dayWord = d => { const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(str(d)); return m ? new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], 12)).toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }) : ''; };
  /** The name with where it is: "GF Sheet 3 · Set 1" (a draft: "GF Draft 5 · Oct 3"). */
  function qualified(r) {
    const s = setNoOf(r);
    if (numbered(r) && s) return `${name(r)} · Set ${s}`;
    const d = numbered(r) ? '' : dayWord(r && r.day);
    return d ? `${name(r)} · ${d}` : name(r);
  }
  /** One name for each sheet of a list that no other sheet of the list shares: the plain name where it is alone, else the
   *  qualified one (set, or day), else a last "#k" so even two records that read alike (a damaged set) are told apart.
   *  Returns Map(id -> name). */
  function uniqueNames(list) {
    const rows = (list || []).filter(r => r && idOf(r)), out = new Map(), groups = new Map();
    for (const r of rows) { const k = name(r); (groups.get(k) || groups.set(k, []).get(k)).push(r); }
    for (const [plain, g] of groups) {
      if (g.length === 1) { out.set(idOf(g[0]), plain); continue; }
      const q = new Map(); for (const r of g) { const k = qualified(r); (q.get(k) || q.set(k, []).get(k)).push(r); }
      for (const [full, h] of q) {
        if (h.length === 1) { out.set(idOf(h[0]), full); continue; }
        h.slice().sort((a, b) => (idOf(a) < idOf(b) ? -1 : 1)).forEach((r, i) => out.set(idOf(r), `${full} #${i + 1}`));
      }
    }
    return out;
  }

  /** The highest number each metal has had in a set: `sheetNos` of the set record (a number never goes down), raised by the
   *  records given (the sheets in the set now, and the one that is leaving). Returns a new map; the input is left as it was. */
  function raise(sheetNos, records) {
    const out = {}; for (const [m, n] of Object.entries(sheetNos && typeof sheetNos === 'object' ? sheetNos : {})) if (num(n)) out[m] = num(n);
    for (const r of records || []) { const n = r && r.metal ? numberOf(r) : 0; if (n > (out[r.metal] || 0)) out[r.metal] = n; }
    return out;
  }
  /** Two maps of highest numbers, as one: the larger of each (a stale writer can never lower what is retired). */
  function mergeNos(a, b) { const out = raise(a, []); for (const [m, n] of Object.entries(b && typeof b === 'object' ? b : {})) if (num(n) > (out[m] || 0)) out[m] = num(n); return out; }
  /** The number a sheet takes when it joins a set: one more than every number of its metal in use there now (`members`: the sheets
   *  of the set, the joiner not among them) and every number the set has retired (`sheetNos`). */
  function nextNumber(members, metal, sheetNos) {
    let top = num(sheetNos && sheetNos[metal]);
    for (const m of members || []) if (m && m.metal === metal) top = Math.max(top, numberOf(m));
    return top + 1;
  }
  /** The number the sheet `rec` takes in the set whose other sheets are `members`: its own old number when it comes back to the
   *  set that gave it (its file name is the one `fileBaseFor(n)` makes for this set) and nobody holds it; else the next number.
   *  fileBaseFor(n): the file name the set would give number n; without it a sheet always takes the next number. */
  function claim(rec, members, sheetNos, fileBaseFor) {
    const old = (fileTag(rec) || {}).no || 0, others = (members || []).filter(m => m && idOf(m) !== idOf(rec) && m.metal === rec.metal);
    if (old && typeof fileBaseFor === 'function' && fileOf(rec) === fileBaseFor(old) && !others.some(m => numberOf(m) === old)) return old;
    return nextNumber(others, rec.metal, sheetNos);
  }

  return { CODE, METAL_OF, codeOf, inSet, isCut, numbered, numberOf, draftNo, setNoOf, fileTag, short, name, was, qualified, uniqueNames, raise, mergeNos, nextNumber, claim };
});
