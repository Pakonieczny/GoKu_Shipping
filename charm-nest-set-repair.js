/* SetRepair: an explicit, safe, one-press repair of a set that breaks the owner's rules (SETREPAIR, Paul 17 Oct 2026).
 *
 *   "I want you to fix it once and for all."
 *
 * Why. Fixing how sets FORM stops new bad sets, but charm-nest-set-rules.js keeps a rule on purpose: "a set that already exists is never touched" (it is only
 * FLAGGED). The owner's live Set 2 (GF Sheet 1-8, SS Sheet 1-4, RG Sheet 16, RG Sheet 29, uncommitted, "Approve for laser cutting" grey) was formed wrongly
 * before that fix and stays wrong. This file is the one deliberate act that puts such a set right, and the one place that says in plain words that a set is wrong.
 *
 *   diagnose(sets, sheets, opts?) -> { list:[info], bySet:{setId: info}, result }
 *       Reads the state with CharmNestSetInvariants.check (the acceptance criterion, docs/charm-nest/set-rules.md) and, for every UNCOMMITTED set that breaks a
 *       rule, gives info = { setId, name, seq, items:[violation], line, short, rules:['R7','R12'], sheets:[id], names:[label] }:
 *       line  "Set 2 breaks your rules: it is bigger than the minimum (GF Sheet 3, GF Sheet 4, ... are not needed for it to be valid); Rose Gold sheet RG Sheet 16 is not cut yet (no green line)"
 *       short the same in a few words (the grey reason under Approve for laser cutting).
 *       A committed / completed / replaced set is never judged (CharmNestSetRules.exempt). R1 (a set short of a class) is the existing "Waiting for ..." line, not a rebuild.
 *   plan(set, sheets, allSets, opts?) -> { ok, noop, refused:{key,reason}|null, mode:'trim'|'dissolve', keep:[id], expel:[id], sets:[...], waiting:[...], partners:[...], held:[...],
 *                                          before, after, words:{ title, lines, summary, nothing }, signature }
 *       What the repair would do, WITHOUT doing it: the exact formation CharmNestSetInvariants.expected() gives for the sheets the set holds and the loose sheets of
 *       its run (the real SetRules.formAll over the real shared-order units), verified with CharmNestSetInvariants.check(afterState) BEFORE anything is applied.
 *       A plan that would still break a rule is refused. Nothing is deleted anywhere: no sheet, no file, no order piece moves between sheets.
 *       Refusals (plain words): a committed / completed / replaced set (never touched) · a set approved for laser cutting · a sheet that was cut and would have to move ·
 *       a sheet being nested or saved right now · a run not open on this screen · a later set of the run that is still forming · an offline page.
 *       Idempotent: a set that is right says so (noop) and nothing is changed.
 *   rebuild(set, opts?)  (page) -> Promise<{ok, ...}>   the press: reads the set and its run's sheets again, plans, asks in a small window, reads again, applies, checks.
 *
 * (SETFIX A, Paul 17 Oct 2026: "fix the below" — "a set holding an uncut Rose Gold, 10K or 14K sheet can no longer be approved until that sheet is cut or the set is rebuilt. Existing sets
 *  that break your rule will be blocked at approval.") A blocked set is never a dead end: every set that breaks a rule carries its ONE way out, `remedy`, and says exactly what to press.
 *   info.remedy = { kind:'cut'|'rebuild', then?:'rebuild', button, sheets:[id], names, say, short, server }
 *       'cut'      the set's only fault is a Rose Gold / 10K / 14K sheet with no green line cut (R12 (a)): the press is "Cut RG Sheet 16", the existing Cut Sheet press (LibraryFlow.cutLine,
 *                  charm-nest-flow-rose.js: the green dash line drawn and dated, the cut recorded). A person's explicit press only: the line is NEVER added by itself, and the window says
 *                  that a recorded cut is permanent. Nothing moves: the sheet stays in its set, which then follows the rules (an uncommitted set may be cut in place: R12 (a) is "its line is cut").
 *                  With other faults as well (bigger than the minimum, ...): "Cut ..." first, then "Rebuild this set correctly" (cut first, so the rebuild keeps the sheet with its unit).
 *       'rebuild'  everything else: "Rebuild this set correctly", as before.
 *   Neither press is ever run by itself: the rebuild moves sheets between sets and the cut is permanent, so both stay the person's explicit act (a window, Cancel changes nothing).
 *   cutGold(set, opts?) (page) the "Cut ..." press · fix(set, opts?) (page) the remedy's first press, for the button under Approve for laser cutting.
 *   cutPending(set, sheetId, opts?) (page) (Paul, 26 Oct 2026) the "Approve & cut green line" press of a 10K / 14K sheet that waits greyed in the set (set.goldPending): never cut or approved by itself,
 *                  (a FULL one, Paul 27 Oct 2026: "Approve" only, it joins with no green line and no recorded cut: approveFull)
 *                  it joins the set by this press, the existing Cut Sheet press with its window · wirePending(card, set) its buttons on a drawn set card.
 *
 * (SETFIX B, Paul 17 Oct 2026: "a sheet someone drags into a formed set can make that set unapprovable".)
 *   joinCheck(set, joinIds, sheets, allSets, opts?) -> { ok, items:[violation], rules, label, detail, sheets:[id] }   pure
 *       The state AFTER the sheets `joinIds` are in `set`, judged by CharmNestSetInvariants.check (through diagnose) against the state before: a move that adds a violation the set did not
 *       have is refused, with the rule and what to do instead. Only a FORMED set is judged (it holds a completed GF and a completed SS sheet before the move: a set still forming is the
 *       run's own business, R5/R8); a committed / completed / replaced set never is (its edit has its own rule, charm-nest-set-edit.js). A Rose Gold / 10K / 14K sheet that joins is read
 *       as cut (it joins only by its Cut Sheet press or its own tick, R12); R14 numbers and R6 are not judged (the join gives the number; a person's drop is allowed after formation).
 *       opts.rules: only these rules (the server, at the write: 'surplus' and 'partial', the two a saved record always answers). The Library plan (LibraryFlow) and the server's write
 *       (charmNestLibrary putSheet / flowApply setMember) read this one function.
 *
 * What the repair does to a set (page side, the existing rule-respecting operations only):
 *   1. the sheets that the correct first set does not hold are marked taken out and made drafts, ALL AT ONCE (a unit that shares an order leaves whole, so the cardinal rule
 *      cannot pull one of them back), and the run's own assembly (Gate.assemble: tendSet) lets them go exactly as it lets a sheet go that stops qualifying: pieces back
 *      to ready, the sheet saved as a draft (putSheet), the set's lists, order lines and labels made again. The set keeps its number, its folder and the sheets that stay in it.
 *      "dissolve": when the first correct set also needs sheets from outside, every sheet leaves and the emptied set is refilled by the run's formation.
 *   2. the sheets that left (except Rose Gold / 10K / 14K, which join by their own Cut Sheet press) are made eligible again and the run's formation forms the correct
 *      sets after the first one, in the right order, numbered through the existing numbering path (Sets.ensure -> op setAllocate, "after" the set before it).
 *   Back files, QR label files, seals, approvals and every cut record stay; a sheet keeps its pieces; the server's own rules (putSheet, setUpdate) decide each write.
 *
 * UMD like charm-nest-set-rules.js: window.SetRepair in the page, module.exports in node and in the Netlify functions (the pure part). */
(function (root, factory) { const api = factory(root); if (typeof module === 'object' && module.exports) module.exports = api; else root.SetRepair = api; })(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  let INVnode = null; try { INVnode = require('./charm-nest-set-invariants.js'); } catch (_) { INVnode = null; }
  let SRnode = null; try { SRnode = require('./charm-nest-set-rules.js'); } catch (_) { SRnode = null; }
  let SNnode = null; try { SNnode = require('./charm-nest-sheet-name.js'); } catch (_) { SNnode = null; }
  const INV = () => root.CharmNestSetInvariants || INVnode;
  const SR = () => root.CharmNestSetRules || SRnode;
  const SN = () => root.CharmNestSheetName || SNnode;

  const str = v => String(v == null ? '' : v);
  const list = x => Array.isArray(x) ? x : x instanceof Set ? [...x] : x instanceof Map ? [...x.values()] : x ? [x] : [];
  const idOf = r => str(r && typeof r === 'object' ? (r.id || r.sheetId) : r);
  const and = l => l.length < 2 ? l.join('') : `${l.slice(0, -1).join(', ')} and ${l[l.length - 1]}`;
  const cap = s => s.charAt(0).toUpperCase() + s.slice(1);
  const NAMES_MAX = 4;
  const some = l => l.length <= NAMES_MAX ? and(l) : `${l.slice(0, NAMES_MAX).join(', ')} and ${l.length - NAMES_MAX} more`;
  const WORD = { RG: 'Rose Gold', '10K': '10K Gold', '14K': '14K Gold' };
  const GOLD_FAMILY = ['RG', '10K', '14K'];
  /** Rules this file never turns into a rebuild: R1 is the existing "Waiting for ..." line of a set short of a class, R0 is bookkeeping, R5 belongs to no set. */
  const NOT_SHOWN = new Set(['invalid', 'membership', 'unformed', 'partial']);   // ('partial', Paul 26 Oct 2026, Decision 3: a sheet of the set filling again is a wait the set holds its place for, said by SetRules.gate, never a break to Rebuild)

  /* ── reading the input ───────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  /** A sheet as a record in a set: the sheet's own fields say it (the card draws the same), not the set's list. */
  const inSetOf = r => (r && r.setId && !r.draft && r.solidIncluded !== false && !r.archived) ? str(r.setId) : null;
  const isCut = r => !!r && (+r.laserDoneAt > 0 || !!r.roseCutAt);
  const classOf = r => { const R = SR(); return R ? R.metalClass(r) : ''; };
  /** Shallow copies of the live sheets. A GF or SS record that carries no completion mark at all (releaseFull never saved) is unknown, not partial (SetRules.recordPartial). */
  function rowsOf(input) {
    const src = input instanceof Map ? [...input.values()] : Array.isArray(input) ? input : input && typeof input === 'object' ? Object.entries(input).map(([k, v]) => (v && typeof v === 'object' && !idOf(v) ? Object.assign({ id: k }, v) : v)) : [];
    const out = [], seen = new Set();
    for (const s of src) {
      if (!s || typeof s !== 'object') continue;
      const id = idOf(s); if (!id || seen.has(id) || s.archived) continue;
      seen.add(id);
      const row = Object.assign({}, s, { id });
      if (GOLD_FAMILY.includes(classOf(row)) && row.solidIncluded === true && row.cutApproved == null) row.cutApproved = true;   // (the page reads a person's tick as R12(a)'s yes: so do the Library cards and the server, which only see the saved tick)
      if (['GF', 'SS'].includes(classOf(row)) && !Object.prototype.hasOwnProperty.call(row, 'releaseFull') && !isCut(row)) { row.releaseFull = true; row._unmarked = true; }
      out.push(row);
    }
    return out;
  }
  const setNumber = d => +d.seq || +d.setSeq || 0;
  const setWord = d => setNumber(d) ? `Set ${setNumber(d)}` : (d && d.name && !/^set-\d/i.test(str(d.name)) ? str(d.name) : 'This set');
  /** The sets as the checker reads them: the set records given, and a record for any set a sheet names that the list does not have (unknown: never judged, never rebuilt). */
  function setsOf(rows, docs) {
    const by = new Map();
    for (const d of list(docs)) { if (!d || typeof d !== 'object') continue; const id = str(d.setId || d.id); if (!id) continue; by.set(id, Object.assign({}, d, { id, setId: id, seq: setNumber(d), sheetIds: [] })); }
    for (const r of rows) {
      const sid = inSetOf(r); if (!sid) continue;
      if (!by.has(sid)) by.set(sid, { id: sid, setId: sid, seq: +r.setSeq || 0, name: r.setSeq ? `Set ${r.setSeq}` : 'A set not listed', sheetIds: [], committedAt: 1, unlisted: true });   // (unknown = not judged)
      by.get(sid).sheetIds.push(r.id);
    }
    for (const s of by.values()) s.name = setWord(s);
    return [...by.values()];
  }

  /* ── the words ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const nameInSet = (r, setId) => { const N = SN(); return N ? N.name(Object.assign({}, r, { draft: false, setId: setId || r.setId || 'x', solidIncluded: true })) : `${classOf(r)} Sheet ${r.sheetIndex || r.page || 1}`; };
  const nameDraft = r => { const N = SN(); return N ? N.name(Object.assign({}, r, { draft: true })) : `${classOf(r)} Draft ${r.page || 1}`; };
  const currentName = r => inSetOf(r) ? nameInSet(r, inSetOf(r)) : nameDraft(r);
  const byKindNo = (a, b) => (({ GF: 0, SS: 1 })[classOf(a)] ?? 2) - (({ GF: 0, SS: 1 })[classOf(b)] ?? 2) || ((SN() ? SN().numberOf(a) : +a.sheetIndex || 0) || +a.page || 0) - ((SN() ? SN().numberOf(b) : +b.sheetIndex || 0) || +b.page || 0) || (idOf(a) < idOf(b) ? -1 : 1);

  /** One violation as a clause of the red line. */
  function clause(rule, vs, rowOf) {
    const names = ids => ids.map(i => rowOf.has(i) ? currentName(rowOf.get(i)) : i);
    const sorted = ids => [...new Set(ids)].filter(i => rowOf.has(i)).sort((a, b) => byKindNo(rowOf.get(a), rowOf.get(b)));
    if (rule === 'surplus') {
      const spare = sorted(vs.flatMap(v => v.sheets)), nm = names(spare);
      return { long: `it is bigger than the minimum (${some(nm)} ${nm.length === 1 ? 'is' : 'are'} not needed for it to be valid)`, short: 'bigger than the minimum', sheets: spare };
    }
    if (rule === 'notReady') {
      const byWord = new Map(), backs = [], sheets = [];
      for (const v of vs) {
        const i = v.sheets[0], r = rowOf.get(i); if (!r) continue; sheets.push(i);
        const reasons = list(v.reasons).map(str);
        if (reasons.some(x => /green line/.test(x))) { const w = WORD[classOf(r)] || 'Gold'; (byWord.get(w) || byWord.set(w, []).get(w)).push(i); }
        if (reasons.some(x => /back engraving/.test(x))) backs.push(i);
      }
      const parts = [], shortParts = [];
      for (const [w, ids] of byWord) { const nm = names(sorted(ids)); parts.push(`${w} sheet${nm.length === 1 ? '' : 's'} ${and(nm)} ${nm.length === 1 ? 'is' : 'are'} not cut yet (no green line)`); shortParts.push(`${and(nm)} not cut yet`); }
      if (backs.length) { const nm = names(sorted(backs)); parts.push(`${and(nm)} ${nm.length === 1 ? 'has' : 'have'} no approved back engraving yet`); shortParts.push(`${and(nm)} back engraving not approved`); }
      return { long: parts.join('; '), short: shortParts.join('; '), sheets: sorted(sheets) };
    }
    if (rule === 'partial') {
      const ids = sorted(vs.flatMap(v => v.sheets)), nm = names(ids);
      return { long: `${some(nm)} ${nm.length === 1 ? 'is' : 'are'} not full yet (a partial sheet never joins a set)`, short: `${some(nm)} not full yet`, sheets: ids };
    }
    if (rule === 'cardinal') {
      const outside = vs.filter(v => v.outside), spread = vs.filter(v => !v.outside);
      const ids = sorted(vs.flatMap(v => v.sheets));
      const parts = [], shortParts = [];
      if (outside.length) { const o = names(sorted(outside.map(v => v.outside))); parts.push(`${some(o)} share${o.length === 1 ? 's' : ''} an order with a sheet of this set but ${o.length === 1 ? 'is' : 'are'} not in it`); shortParts.push(`${some(o)} should be in it`); }
      if (spread.length) { const os = [...new Set(spread.flatMap(v => list(v.orders)))]; parts.push(`${os.length === 1 ? 'order ' + os[0] : 'orders ' + os.slice(0, 3).join(', ') + (os.length > 3 ? ' and more' : '')} ${os.length === 1 ? 'has' : 'have'} pieces on sheets of different sets`); shortParts.push('orders split over two sets'); }
      return { long: parts.join('; '), short: shortParts.join('; '), sheets: ids };
    }
    if (rule === 'partialPartner') {
      const p = sorted(vs.map(v => v.partial)), nm = names(p);
      return { long: `${some(nm)} ${nm.length === 1 ? 'is' : 'are'} not full yet and share${nm.length === 1 ? 's' : ''} an order with sheets of this set (that group waits outside any set until it is full)`, short: `${some(nm)} not full yet`, sheets: sorted(vs.flatMap(v => v.sheets)) };
    }
    if (rule === 'closedJoin') { const ids = sorted(vs.flatMap(v => v.sheets)), nm = names(ids); return { long: `${some(nm)} joined after the set had formed`, short: 'sheets joined after it had formed', sheets: ids }; }
    if (rule === 'numbering') { const ids = sorted(vs.flatMap(v => v.sheets)), nm = names(ids); return { long: `${some(nm)} carry the same number`, short: 'two sheets share a number', sheets: ids }; }
    return { long: vs.map(v => v.message).join('; '), short: 'a rule is broken', sheets: sorted(vs.flatMap(v => v.sheets)) };
  }
  const RULE_ORDER = ['surplus', 'notReady', 'partial', 'cardinal', 'partialPartner', 'closedJoin', 'numbering'];

  /** The info of one set from its violations: the one plain line and its short form. */
  function infoOf(set, vs, rowOf) {
    const by = new Map(); for (const v of vs) (by.get(v.rule) || by.set(v.rule, []).get(v.rule)).push(v);
    const cl = RULE_ORDER.filter(r => by.has(r)).concat([...by.keys()].filter(r => !RULE_ORDER.includes(r))).map(r => ({ rule: r, ...clause(r, by.get(r), rowOf) })).filter(c => c.long);
    const sheets = [...new Set(cl.flatMap(c => c.sheets))];
    return {
      setId: set.id, name: setWord(set), seq: setNumber(set), items: vs.map(v => Object.assign({ rule: v.rule, ruleNo: v.ruleNo, letter: v.letter || '', sheets: v.sheets.slice(), names: v.names.slice(), message: v.message },
        ...['outside', 'partial'].filter(k => v[k]).map(k => ({ [k]: v[k] })), ...['orders', 'reasons'].filter(k => Array.isArray(v[k])).map(k => ({ [k]: v[k].slice() })))),   // (SETFIX B: what a refused move names, joinWords)
      line: `${setWord(set)} breaks your rules: ${cl.map(c => c.long).join('; ')}`, short: `breaks your rules: ${cl.map(c => c.short).join('; ')}`,
      rules: [...new Set(vs.map(v => v.ruleNo))], sheets, names: sheets.map(i => rowOf.has(i) ? currentName(rowOf.get(i)) : i), clauses: cl.map(c => ({ rule: c.rule, text: c.long })),
      remedy: remedyOf(set, vs, rowOf)
    };
  }
  /** (SETFIX A, Paul 17 Oct 2026) The press that puts a set that breaks a rule right, in the words the person reads: see the head of this file. */
  const REBUILD = 'Rebuild this set correctly';
  function remedyOf(set, vs, rowOf) {
    const name = setWord(set), nm = i => (rowOf.has(i) ? currentName(rowOf.get(i)) : i);
    const lineless = vs.filter(v => v.rule === 'notReady' && list(v.reasons).some(x => /green line/.test(str(x))));
    const cut = [...new Set(lineless.map(v => v.sheets[0]))].filter(i => rowOf.has(i)).sort((a, b) => byKindNo(rowOf.get(a), rowOf.get(b)));
    // (audit, 25 Oct 2026: "never a dead end". A back engraving that is not approved is no fault a Rebuild or a Cut puts right: a cut sheet never moves, so "Cut, then Rebuild" was refused
    //  for good. Its press is approving that back engraving (the Engraving tab), said by name, first)
    const backIds = [...new Set(vs.filter(v => v.rule === 'notReady' && list(v.reasons).some(x => /back engraving/.test(str(x)))).map(v => v.sheets[0]))].filter(i => rowOf.has(i));
    const backs = backIds.length ? `approve the back engraving of ${and(backIds.map(nm))} (Engraving tab)` : '', Backs = backs ? backs[0].toUpperCase() + backs.slice(1) : '';
    const rest = vs.some(v => v.rule !== 'notReady');
    if (!cut.length && backs && !rest) return { kind: 'backs', button: '', sheets: backIds, names: backIds.map(nm), say: `${Backs}; ${name} can then be approved.`, short: backs, server: Backs };
    if (!cut.length) return { kind: 'rebuild', button: REBUILD, sheets: [], names: [], say: `Press "${REBUILD}" on its card in the Library.`, short: `press "${REBUILD}"`, server: `Press "${REBUILD}" on its card in the Library` };
    const names = cut.map(nm), one = cut.length === 1, button = `Cut ${and(names)}`, does = `the Cut Sheet press: it draws ${one ? 'its green line and records its cut' : 'their green lines and records their cuts'}`;
    if (rest) return { kind: 'cut', then: 'rebuild', button, sheets: cut, names, say: `${backs ? Backs + ' first, then press' : 'Press'} "${button}"${backs ? '' : ' first'} (${does}), then "${REBUILD}".`, short: `${backs ? backs + ', ' : ''}press "${button}", then "${REBUILD}"`, server: `${backs ? Backs + ', then press' : 'Press'} "${button}" on its card in the Library (${does}), then "${REBUILD}"` };
    if (backs) return { kind: 'cut', then: 'backs', button, sheets: cut, names, say: `Press "${button}" (${does}) and ${backs}; ${name} can then be approved.`, short: `press "${button}" and ${backs}`, server: `Press "${button}" on its card in the Library (${does}) and ${backs}` };
    // (a sheet that shares no order with the rest of the set may simply leave it instead: the rebuild makes the set again without it; one that shares an order would only become the partner the set waits for)
    const loose = lineless.every(v => !list(v.tied).length);
    return { kind: 'cut', button, sheets: cut, names, say: `Press "${button}" (${does}), and ${name} can then be approved${loose ? `. Or press "${REBUILD}": ${name} is made again without ${one ? 'it' : 'them'} until ${one ? 'it is' : 'they are'} cut` : ''}.`, short: `press "${button}"`, server: `Press "${button}" on its card in the Library (${does})` };
  }

  /* ── diagnose ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const empty = () => ({ list: [], bySet: {}, result: null });
  /** The sets that break a rule. `sets`: the set records (committed ones too: they are read as containers and never judged); `sheets`: EVERY sheet the shop holds that could share an order
   *  (the chain of shared orders runs through sheets outside the set: a Rose Gold partner ties the sheets either side of it). opts.viaGate: the answer of Gate.violations(run), when the page has it. */
  function diagnose(setDocs, sheetsIn, opts) {
    const I = INV(), R = SR(); if (!I || !R) return empty();
    const o = opts || {}, rows = rowsOf(sheetsIn), sets = setsOf(rows, setDocs), rowOf = new Map(rows.map(r => [r.id, r]));
    // (a set whose GF and SS records carry no completion mark at all cannot be read: SetRules.gate leaves it alone too)
    const readable = s => s.sheetIds.some(i => { const r = rowOf.get(i); return r && ['GF', 'SS'].includes(classOf(r)) && !r._unmarked; });
    // (a set that is approved for laser cutting, or already partly cut, is history: it is neither flagged, nor held back from Laser cutting, nor rebuilt)
    const started = s => !!approvedWhy(s, rows.filter(r => inSetOf(r) === s.id)) || s.sheetIds.some(i => { const r = rowOf.get(i); return r && +r.laserDoneAt > 0; });   // (approvedWhy: a ready flag that is a moment old, with a sheet that says it is not ready now, is no approval)
    const judged = sets.filter(s => !R.exempt(s) && !s.unlisted && s.sheetIds.length >= 2 && readable(s) && (o.includeStarted || !started(s)));
    if (!judged.length) return empty();
    let result;
    try { result = I.check(sets.map(s => ({ id: s.id, name: s.name, seq: s.seq, sheetIds: s.sheetIds, committedAt: s.committedAt, laserDoneAt: s.laserDoneAt, status: s.status })), rows, { skipExempt: true, unformed: false, membership: false, partialPartners: 'pending' }); }
    catch (e) { return empty(); }
    const gate = o.viaGate && Array.isArray(o.viaGate.violations) ? o.viaGate.violations : null;
    const out = [], bySet = {};
    for (const s of judged) {
      // (a page that has Gate.violations(run) for the run's own sets: its words win; any other set is read here)
      const own = gate && gate.some(v => v.set === s.id) ? gate.filter(v => v.set === s.id) : result.violations.filter(v => v.set === s.id || list(v.sets).includes(s.id));
      const vs = own.filter(v => !NOT_SHOWN.has(v.rule) || (o.joining && v.rule === 'partial'));   // (a partial sheet DROPPED into a formed set is still refused, R3: Decision 3's wait is for a sheet of the set filling again, Paul 26 Oct 2026)
      if (!vs.length) continue;
      const info = infoOf(s, vs, rowOf); out.push(info); bySet[s.id] = info;
    }
    return { list: out, bySet, result };
  }
  /** The annotation a set group carries for the Library and the readiness gate: { items, line, short, rules }, or null. */
  const breaksOf = (diag, setId) => { const i = diag && diag.bySet[setId]; return i ? { items: i.items, line: i.line, short: i.short, rules: i.rules, sheets: i.sheets.slice(), names: i.names.slice(), ...(i.remedy ? { remedy: Object.assign({}, i.remedy) } : {}) } : null; };

  /* ── joinCheck (SETFIX B) ────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const GOLD_JOIN = { cutApproved: true };
  /** What a move of the sheets `joinIds` into `setIn` would do to the owner's rules: see the head of this file. Pure: nothing is written. */
  function joinCheck(setIn, joinIdsIn, sheetsIn, allSetsIn, opts) {
    const I = INV(), R = SR(), o = opts || {}, pass = extra => Object.assign({ ok: true, items: [], rules: [], label: '', detail: '', sheets: [] }, extra || {});
    if (!I || !R) return pass({ skipped: 'rules' });
    const doc = setIn && typeof setIn === 'object' ? setIn : null, setId = str(doc && (doc.setId || doc.id));
    if (!doc || !setId) return pass({ skipped: 'noSet' });
    if (R.exempt(doc)) return pass({ skipped: 'committed' });
    const rows = rowsOf(sheetsIn), rowOf = new Map(rows.map(r => [r.id, r]));
    const join = new Set(list(joinIdsIn).map(idOf).filter(i => i && rowOf.has(i) && inSetOf(rowOf.get(i)) !== setId));
    if (!join.size) return pass({ skipped: 'nothing' });
    // only a FORMED set: it holds a completed GF and a completed SS sheet before the move (a set still forming is completed by the run, R5 / R8)
    const before = rows.filter(r => inSetOf(r) === setId);
    if (!R.validSet(before).ok) return pass({ skipped: 'forming' });
    const docs = list(allSetsIn).filter(d => d && str(d.setId || d.id) !== setId).concat([doc]);
    const after = rows.map(r => {
      if (!join.has(r.id)) return r;
      const x = Object.assign({}, r, { setId, draft: false, archived: false, joinedBy: 'person', sheetIndex: null });
      if (GOLD_FAMILY.includes(classOf(r))) Object.assign(x, { solidIncluded: true }, GOLD_JOIN);   // (it joins only by its own Cut Sheet press or its own tick: R12 (a)'s yes)
      else if (x.solidIncluded === false) x.solidIncluded = null;
      return x;
    });
    const was = diagnose(docs, rows, { includeStarted: true, joining: true }).bySet[setId], now = diagnose(docs, after, { includeStarted: true, joining: true }).bySet[setId];
    if (!now) return pass();
    // a violation the set already had is not this move's doing; one that only names the joining sheets as well as those it had is the same fault (a unit that grows)
    const key = v => `${v.rule}:${v.sheets.filter(i => !join.has(i)).slice().sort().join('+')}`, had = new Set(((was && was.items) || []).map(key));
    const SKIP = new Set(['numbering', 'closedJoin']), want = o.rules ? new Set(list(o.rules)) : null;
    const fresh = now.items.filter(v => !SKIP.has(v.rule) && (!want || want.has(v.rule)) && !had.has(key(v)));
    if (!fresh.length) return pass();
    return Object.assign({ ok: false, items: fresh, rules: [...new Set(fresh.map(v => v.ruleNo))], sheets: [...join], names: [...join].map(i => currentName(rowOf.get(i))) }, joinWords(doc, [...join], fresh, rowOf, rows));
  }
  /** The refusal of a move into a set, in plain words: which rule, and what to do instead. Names as the sheets are called now (before the move). */
  function joinWords(doc, joinIds, vs, rowOf, rows) {
    const R = SR(), set = setWord(doc), nm = i => (rowOf.has(i) ? currentName(rowOf.get(i)) : i);
    const who = and(joinIds.slice().sort((a, b) => byKindNo(rowOf.get(a), rowOf.get(b))).map(nm)), many = joinIds.length > 1;
    const lines = [], first = vs[0];
    for (const rule of RULE_ORDER.concat([...new Set(vs.map(v => v.rule))].filter(r => !RULE_ORDER.includes(r)))) {
      const mine = vs.filter(v => v.rule === rule); if (!mine.length) continue;
      if (rule === 'surplus') {
        const have = rows.filter(r => inSetOf(r) === str(doc.setId || doc.id));
        // (the sheets the move adds are what makes the set bigger than it needs: the checker may name an earlier member as the spare one, the words name the move)
        const kinds = [...new Set(joinIds.map(i => classOf(rowOf.get(i))).filter(c => c === 'GF' || c === 'SS'))];
        const needs = kinds.length === 1 ? ` with a completed ${kinds[0] === 'GF' ? 'SS' : 'GF'} sheet` : '';
        lines.push({ rule, label: `${set} would be bigger than the minimum`, text: `${set} already holds a completed GF and a completed SS sheet${have.length ? ` (${and(have.slice().sort(byKindNo).slice(0, 4).map(r => nm(r.id)))}${have.length > 4 ? ' and more' : ''})` : ''}, and no order ties ${who} to its sheets, so ${set} does not need ${many ? 'them' : 'it'}. A set holds only the sheets it needs (your rule R7): with ${many ? 'them' : 'it'}, ${set} could not be approved for laser cutting. Leave ${who} out: ${many ? 'they form' : 'it forms'} the next set${needs}.` });
      } else if (rule === 'partial') {
        const p = [...new Set(mine.flatMap(v => v.sheets))], one = p.length === 1;
        lines.push({ rule, label: `${some(p.map(nm))} ${one ? 'is' : 'are'} not full yet`, text: `${some(p.map(nm))} ${one ? 'is' : 'are'} not full yet${one && R ? R.fillWords(rowOf.get(p[0]) || {}) : ''}, and a partial sheet never joins a set (your rule R3). Let ${one ? 'it' : 'them'} finish nesting, or release ${one ? 'it' : 'them'} as ${one ? 'it stands' : 'they stand'}, then add ${one ? 'it' : 'them'} to ${set}.` });
      } else if (rule === 'notReady') {
        const p = [...new Set(mine.flatMap(v => v.sheets))], one = p.length === 1;
        lines.push({ rule, label: `${some(p.map(nm))} ${one ? 'is' : 'are'} not ready to join a set`, text: `${some(p.map(nm))} ${one ? 'has' : 'have'} no cut green line or no approved back engraving yet (your rule R12). Press Cut Sheet on ${one ? 'it' : 'them'} and approve ${one ? 'its' : 'their'} back engraving, then add ${one ? 'it' : 'them'} to ${set}.` });
      } else if (rule === 'cardinal') {
        const out = [...new Set(mine.map(v => v.outside).filter(Boolean))].map(nm), os = [...new Set(mine.flatMap(v => list(v.orders)))];
        lines.push({ rule, label: 'Orders shared with another sheet', text: `${os.length ? (os.length === 1 ? `Order ${os[0]} has` : `Orders ${os.slice(0, 3).join(', ')}${os.length > 3 ? ' and more' : ''} have`) + ' pieces' : 'Pieces of an order are'} on ${out.length ? and(out) : 'a sheet'} too, and sheets that share a multi-piece order are always in the same set (your rule R9). Drop them together, or take the order off one of the sheets first.` });
      } else if (rule === 'partialPartner') {
        const p = [...new Set(mine.map(v => v.partial).filter(Boolean))].map(nm);
        lines.push({ rule, label: `${who} waits for ${and(p)}`, text: `${who} ${many ? 'share' : 'shares'} an order with ${and(p)}, which ${p.length === 1 ? 'is' : 'are'} not full yet: that group waits outside every set until ${p.length === 1 ? 'it is' : 'they are'} full (your rule R11).` });
      } else lines.push({ rule, label: `${set} would break your rules`, text: mine.map(v => v.message).join('. ') + '.' });
    }
    const head = `${who} cannot go into ${set}`;
    return { label: `${head}: ${lines.map(l => l.label).join('; ')}`, detail: lines.map(l => l.text).join(' '), lines, rule: first.rule };
  }

  /* ── plan ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const refuse = (out, key, reason) => Object.assign(out, { ok: false, refused: { key, reason } });
  /** A set that is approved for laser cutting NOW: the cloud says it is ready (processReady), and no sheet's own report says otherwise (a flag that is a moment old, from before a sheet joined, is not an approval).
   *  The seals are history, not a state: a set that was ready once and has changed since is not approved. */
  function approvedWhy(set, members) {
    if (set.processReady !== true) return '';
    const reports = list(members).map(m => m && m.laser).filter(r => r && typeof r === 'object');
    if (reports.length === list(members).length && reports.some(r => r.ready === false)) return '';
    return `${setWord(set)} is approved for laser cutting already: an approved set is never rebuilt`;
  }
  /** The numbers the sheets of a set take, as the run numbers them (charm-nest-sheet-name.js: the next number of the metal in that set, after every number the set ever gave). */
  function numberJoiners(nos0, staying, joining) {
    const N = SN(); const nos = Object.assign({}, nos0), placed = staying.slice(), out = new Map();
    for (const r of joining.slice().sort((a, b) => (+a.page || 0) - (+b.page || 0) || (idOf(a) < idOf(b) ? -1 : 1))) {
      const here = placed.filter(p => p.metal === r.metal), n = N ? N.nextNumber(here, r.metal, nos) : 1 + Math.max(0, ...here.map(p => +p.sheetIndex || 0));
      const copy = Object.assign({}, r, { sheetIndex: n }); placed.push(copy); out.set(r.id, n);
      if (N) Object.assign(nos, N.raise(nos, [copy]));
    }
    return out;
  }
  /** What a sheet that waits outside every set is told, singular and plural ("waits for its green line" / "wait for their green line"). */
  const waitWords = (r, why, held) => {
    const R = SR(), I = INV(), cls = classOf(r);
    if (GOLD_FAMILY.includes(cls)) {
      const gaps = I && I.goldGaps ? I.goldGaps(r) : [];
      if (gaps.some(g => /green line/.test(g))) return { one: 'waits for its green line', many: 'wait for their green line' };
      if (gaps.some(g => /back engraving/.test(g))) return { one: 'waits for its back engraving to be approved', many: 'wait for their back engraving to be approved' };
      return { one: 'waits until you add it (Include / Cut Sheet)', many: 'wait until you add them (Include / Cut Sheet)' };
    }
    if (R && R.isPartial(r)) return { one: `is not full yet${R.fillWords(r)}`, many: `are not full yet${R.fillWords(r)}` };
    if (held) return held;
    const m = /needs a completed (GF|SS) sheet/i.exec(why || '') || /Waits for a completed (GF|SS)/i.exec(why || '');
    if (m) return { one: `waits for a completed ${m[1].toUpperCase()} sheet`, many: `wait for a completed ${m[1].toUpperCase()} sheet` };
    const t = str(why).replace(/^The set has formed, so this sheet /, '').replace(/[.\s]+$/, '') || 'waits outside every set';
    return { one: t, many: t };
  };

  /** What the repair would do to `set`. See the head of this file. opts: { busy(row)->'' | reason, runHere:false, cloud:false, committing:true, firstSeq }. */
  function plan(setIn, sheetsIn, allSetsIn, opts) {
    const I = INV(), R = SR(), N = SN(), o = opts || {};
    const out = { ok: false, noop: false, refused: null, mode: null, keep: [], expel: [], sets: [], waiting: [], partners: [], held: [], before: null, after: null, words: null, signature: '', setId: '', name: '', seq: 0 };
    if (!I || !R) return refuse(out, 'rules', 'The set rules are not loaded on this page: reload it');
    const doc = setIn && typeof setIn === 'object' ? setIn : null;
    const setId = str(doc && (doc.setId || doc.id));
    if (!doc || !setId) return refuse(out, 'noSet', 'That set could not be found: refresh the Library');
    const rows = rowsOf(sheetsIn), rowOf = new Map(rows.map(r => [r.id, r]));
    const docs = list(allSetsIn).filter(d => d && str(d.setId || d.id) !== setId).concat([doc]);
    const sets = setsOf(rows, docs), me = sets.find(s => s.id === setId);
    out.setId = setId; out.name = setWord(doc); out.seq = setNumber(doc);
    if (R.exempt(doc) || R.exempt(me)) return refuse(out, 'committed', `${out.name} was sent to the Design Station, completed or replaced: a committed set is never rebuilt`);
    const members = rows.filter(r => inSetOf(r) === setId), memberIds = new Set(members.map(r => r.id));
    if (!members.length) return refuse(out, 'empty', `${out.name} holds no sheet: there is nothing to rebuild`);
    const approved = approvedWhy(doc, members); if (approved) return refuse(out, 'approved', approved);
    if (o.cloud === false) return refuse(out, 'offline', 'The page is offline: reconnect, then rebuild. Nothing was changed');
    if (o.committing) return refuse(out, 'committing', 'The run is committing a set right now: try again when it has finished');
    if (o.runHere === false) return refuse(out, 'run', `${out.name} is made by a run that is not open on this screen: open that run here, then rebuild its set`);
    // a later set of the run that is still forming would take the freed sheets before a new set could be numbered for them
    const later = sets.filter(s => s.id !== setId && !R.exempt(s) && !s.unlisted && str(s.runId) === str(doc.runId) && setNumber(s) > setNumber(doc) && !R.closed(rows.filter(r => inSetOf(r) === s.id)));
    if (later.length) return refuse(out, 'later', `${later[0].name} comes after ${out.name} in this run and is still forming: finish it first, then rebuild ${out.name}`);

    const diag = diagnose(docs, rows, { includeStarted: true }); out.before = diag.bySet[setId] || null;   // (the press says WHY a started set cannot be rebuilt: the refusals above and below)
    if (!out.before) { out.noop = true; return refuse(out, 'right', `${out.name} already follows your rules: nothing to rebuild`); }

    // the sheets the formation works from: this set's sheets and the loose sheets of its run (a sheet in another set is not free; one a person took out stays out)
    const inOther = new Set(sets.filter(s => s.id !== setId).flatMap(s => s.sheetIds));
    const sameRun = r => !doc.runId || !r.runId || str(r.runId) === str(doc.runId);
    const heldBack = r => !memberIds.has(r.id) && r.laserHold && +r.laserHold.at > 0;
    const pool = rows.filter(r => !inOther.has(r.id) && (memberIds.has(r.id) || (sameRun(r) && !inSetOf(r) && !heldBack(r))));
    const poolIds = new Set(pool.map(r => r.id)), firstSeq = +o.firstSeq || 1;
    const exp = I.expected(pool, { firstSeq: 1 });
    const planned = exp.sets.map(s => s.sheetIds.slice());
    // a sheet that is busy (being nested or saved) is not touched: neither a sheet of the set nor a sheet the new sets would take (a loose sheet still filling is no concern of the repair)
    if (typeof o.busy === 'function') for (const r of members.concat(planned.flat().filter(i => !memberIds.has(i)).map(i => rowOf.get(i)))) { const why = str(o.busy(r)); if (why) return refuse(out, 'busy', `${currentName(r)} is ${why}: try again when it is done. Nothing was changed`); }
    const firstIds = planned[0] || [];
    const trim = firstIds.length > 0 && firstIds.every(id => memberIds.has(id));
    const tryMode = mode => {
      const keep = mode === 'trim' ? firstIds.slice() : [], keepSet = new Set(keep), expel = members.filter(r => !keepSet.has(r.id)).map(r => r.id);
      return { mode, keep, expel };
    };
    // (dissolve empties the set and leaves its refill to the run's formation, which fills the run's NEWEST set only: with a later set of the run, formed or not, Set N would stay empty and its sheets would form later numbers)
    const laterSet = sets.some(s => s.id !== setId && !R.exempt(s) && !s.unlisted && str(s.runId) === str(doc.runId) && setNumber(s) > setNumber(doc));
    const modes = (trim ? [tryMode('trim'), tryMode('dissolve')] : [tryMode('dissolve')]).filter(m => !(laterSet && m.mode === 'dissolve'));
    if (!modes.length) return refuse(out, 'later', `${out.name} would have to be made again from scratch, and a later set of this run exists: the sheets would form later sets and leave ${out.name} empty. Nothing was changed`);

    // the numbers the new sets will carry: the day's counter hands out the next one after every set it knows (op setAllocate); the set keeps its own
    const known = sets.filter(s => !s.unlisted && (!doc.day || !s.day || str(s.day) === str(doc.day))).map(setNumber);
    const topSeq = Math.max(setNumber(doc), 0, ...known);
    let last = null;
    for (const m of modes) {
      if (m.expel.some(id => isCut(rowOf.get(id)))) { const c = m.expel.find(id => isCut(rowOf.get(id))); last = ['cut', `${currentName(rowOf.get(c))} was already cut: a cut sheet stays in its set, so ${out.name} cannot be rebuilt around it`]; continue; }
      // the state after: the first correct set keeps this set's record, the others are new sets numbered after the highest
      const afterSets = [], newOf = new Map();
      planned.forEach((ids, k) => {
        const id = k === 0 ? setId : `new-${k}`, seq = k === 0 ? setNumber(doc) || firstSeq : topSeq + k;
        afterSets.push({ id, name: `Set ${seq}`, seq, sheetIds: ids.slice(), runId: doc.runId, day: doc.day, fresh: k > 0 }); ids.forEach(i => newOf.set(i, id));
      });
      // numbers: kept sheets keep theirs; a sheet that joins takes the next one of its metal in its set
      const numbers = new Map(), keepRows = m.keep.map(i => rowOf.get(i));
      afterSets.forEach((s, k) => {
        const rowsIn = s.sheetIds.map(i => rowOf.get(i)), staying = k === 0 ? keepRows : [], joining = rowsIn.filter(r => !staying.includes(r));
        const nos0 = k === 0 ? (N ? N.raise(doc.sheetNos, k === 0 && m.mode === 'dissolve' ? members : staying) : {}) : {};
        for (const r of staying) numbers.set(r.id, (N ? N.numberOf(r) : +r.sheetIndex) || +r.sheetIndex || 0);
        for (const [i, n] of numberJoiners(nos0, staying, joining)) numbers.set(i, n);
      });
      const afterRows = rows.map(r => {
        const sid = newOf.get(r.id);
        if (sid) return Object.assign({}, r, { setId: sid, draft: false, solidIncluded: r.solidIncluded === false ? true : r.solidIncluded, sheetIndex: numbers.get(r.id) || r.sheetIndex });
        if (poolIds.has(r.id)) return Object.assign({}, r, { setId: null, draft: true });
        return r;
      });
      const verdict = I.check(afterSets.concat(sets.filter(s => s.id !== setId).map(s => ({ id: s.id, name: s.name, seq: s.seq, sheetIds: s.sheetIds, committedAt: s.committedAt, laserDoneAt: s.laserDoneAt, status: s.status }))), afterRows, { skipExempt: true, membership: false, partialPartners: 'pending' });
      const ours = new Set(afterSets.map(s => s.id));
      const bad = verdict.violations.filter(v => !NOT_SHOWN.has(v.rule) ? (ours.has(v.set) || list(v.sets).some(x => ours.has(x))) : v.rule === 'unformed' && v.sheets.some(i => poolIds.has(i)));
      if (bad.length) { last = ['unfixed', `A rebuild would still break your rules (${bad.map(v => v.message).slice(0, 2).join('; ')}): ${out.name} was not changed`]; continue; }
      return finish(out, { mode: m.mode, keep: m.keep, expel: m.expel, afterSets, afterRows, numbers, exp, pool, rowOf, doc, members, verdict, diag, topSeq });
    }
    return refuse(out, last[0], last[1]);
  }

  /** The plan, with its words: shared by every successful mode. */
  function finish(out, c) {
    const { afterSets, numbers, exp, rowOf, doc, verdict } = c;
    const label = s => s.name;
    const entry = (s, k) => {
      const rowsIn = s.sheetIds.map(i => rowOf.get(i)), nm = r => nameInSet(Object.assign({}, r, { sheetIndex: numbers.get(r.id) || r.sheetIndex }), 'x');
      return { index: k, setId: k === 0 ? doc.setId || doc.id : null, name: label(s), seq: s.seq, predicted: k > 0, sheetIds: s.sheetIds.slice(), names: rowsIn.map(nm), was: rowsIn.map(r => { const was = currentName(r); return was === nm(r) ? '' : was; }), keeps: s.sheetIds.filter(i => c.keep.includes(i)), joins: s.sheetIds.filter(i => !c.keep.includes(i)) };
    };
    out.sets = afterSets.map(entry);
    const placed = new Set(afterSets.flatMap(s => s.sheetIds));
    const heldIds = new Map(); for (const h of exp.held) for (const i of h.ids) heldIds.set(i, h);
    const expelled = new Set(c.expel);
    // the sheets that wait outside every set: the members that leave, and the loose sheets of a group that waits with them
    const touched = new Set(c.expel);
    for (const h of exp.held) if (h.ids.some(i => expelled.has(i))) h.ids.forEach(i => touched.add(i));
    out.waiting = exp.waiting.filter(w => !placed.has(w.id) && touched.has(w.id) && rowOf.has(w.id)).map(w => {
      const r = rowOf.get(w.id), h = heldIds.get(w.id);
      const who = h ? and(h.partial.map(p => rowOf.has(p) ? nameDraft(rowOf.get(p)) : p)) : '', be = h && h.partial.length === 1 ? 'is' : 'are';
      const heldBy = h ? { one: `waits with its group until ${who} ${be} full`, many: `wait with their group until ${who} ${be} full` } : null;
      const ww = waitWords(r, w.why, heldBy);
      return { id: w.id, name: nameDraft(r), was: expelled.has(w.id) ? currentName(r) : '', why: ww.one, whyMany: ww.many, reason: w.why };
    });
    out.partners = exp.partners.filter(p => rowOf.has(p.sheet)).map(p => { const k = +str(p.set).replace(/\D/g, '') - 1; return { setIndex: k, set: out.sets[k] ? out.sets[k].name : p.setName, sheet: p.sheet, name: nameDraft(rowOf.get(p.sheet)), reasons: p.reasons.slice() }; });
    out.held = exp.held.map(h => ({ ids: h.ids.slice(), line: h.line, partial: h.partial.slice() }));
    out.mode = c.mode; out.keep = c.keep.slice(); out.expel = c.expel.slice();
    out.after = { ok: verdict.ok, violations: verdict.violations.filter(v => !NOT_SHOWN.has(v.rule)).length };
    out.ok = true; out.refused = null;
    out.words = wordsOf(out, doc);
    out.signature = JSON.stringify([out.mode, doc.setId || doc.id, out.sets.map(s => s.sheetIds.slice().sort()), out.expel.slice().sort()]);
    return out;
  }

  /** The words of the small window and its one-line form. */
  function wordsOf(p, doc) {
    const lines = [];
    const nameOne = (n, was) => was && was !== n ? `${n} (was ${was})` : n;
    p.sets.forEach((s, k) => {
      const names = s.names.map((n, i) => nameOne(n, s.was[i]));
      const mine = p.partners.filter(x => x.setIndex === k);
      const tail = mine.length ? ` · ${and(mine.map(x => x.name))} ${mine.length === 1 ? 'is its partner' : 'are its partners'} to add, once ${mine.length === 1 ? 'it has' : 'they have'} a green line` : '';
      lines.push({ kind: 'set', text: `${s.name}: ${names.join(', ')}${tail}` });
    });
    // sheets that wait for the same thing are told once ("GF Draft 4, GF Draft 5 and SS Draft 3 wait with their group until GF Draft 10 is full")
    const waits = new Map();
    for (const w of p.waiting) (waits.get(w.why) || waits.set(w.why, []).get(w.why)).push(w);
    for (const [why, ws] of waits) lines.push({ kind: 'wait', text: `${and(ws.map(w => nameOne(w.name, w.was)))} ${ws.length === 1 ? why : ws[0].whyMany}` });
    const nothing = 'Nothing is deleted: every sheet keeps its pieces, back files, QR label files, seals and cut records.';
    const becomes = p.sets.length ? p.sets.map(s => lines[p.sets.indexOf(s)].text).join(' · ') : 'no set (no valid set can be made from its sheets yet)';
    const summary = `${p.name} becomes ${becomes}${lines.filter(l => l.kind === 'wait').length ? ' · ' + lines.filter(l => l.kind === 'wait').map(l => l.text).join(' · ') : ''}. Nothing is deleted.`;
    const what = p.mode === 'trim'
      ? `${p.name} keeps its first ${p.keep.length} sheet${p.keep.length === 1 ? '' : 's'}. The ${p.expel.length} other${p.expel.length === 1 ? '' : 's'} leave it as drafts and form the next sets by the usual rules, in order.`
      : `${p.name} is taken apart, and its sheets form the correct sets again by the usual rules, in order.`;
    return {
      title: `Rebuild ${p.name} correctly?`, intro: `${p.name} breaks your rules. It becomes:`, lines, what, nothing, summary,
      confirm: 'Rebuild', cancel: 'Cancel', numbering: p.sets.some(s => s.predicted) ? 'The later sets get the next set numbers when they are made.' : ''
    };
  }

  /* ── the page ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const esc = x => str(x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const doc_ = () => (typeof document !== 'undefined' ? document : null);
  const CSS = `
.setBreak{display:flex;align-items:center;gap:10px 14px;flex-wrap:wrap;margin:8px 0 0;padding:8px 10px;border:1px solid var(--clay,#b0563f);border-radius:9px;background:var(--claySoft,#f4e3dc);color:var(--clay,#b0563f)}
.setBreak .setBreakLine{flex:1 1 320px;min-width:0;font-size:12.5px;line-height:1.45;font-weight:600}
.setBreak .btn{flex:0 0 auto}
.setBreak .setBreakSay{flex:1 1 100%;font-size:12px;line-height:1.4;color:var(--ink70,#5b554c);font-weight:500}
.setBreak .setBreakTodo{flex:1 1 100%;min-width:0;font-size:12px;line-height:1.4;color:var(--ink,#1c1a17);font-weight:500}
.setBreak[aria-busy="true"] .btn{opacity:.6}
dialog.setRepairDlg{width:min(540px,94vw)}
.setRepairDlg .dlgBody p{margin:0 0 10px;line-height:1.5;color:var(--ink70,#5b554c)}
.setRepairDlg .dlgBody ul{margin:0 0 12px;padding:0;list-style:none;display:grid;gap:6px}
.setRepairDlg .dlgBody li{padding:7px 10px;border:1px solid var(--line,#e4ddd0);border-radius:8px;background:var(--card2,#faf7f1);line-height:1.45;font-size:13px}
.setRepairDlg .dlgBody li.wait{color:var(--ink70,#5b554c)}
.setRepairDlg .dlgBody li b{font-weight:600}
.setRepairDlg .dlgBody .nothing{font-weight:600;color:var(--sage,#5f7a5b)}
`;
  function styleOnce() {
    const d = doc_(); if (!d || !d.head || d.getElementById('setRepairStyle')) return;
    const s = d.createElement('style'); s.id = 'setRepairStyle'; s.textContent = CSS; d.head.appendChild(s);
  }
  /** The red line and its one button for a set group that carries `setBreaks` (the Library's libraryGroups sets it at every drawing; never saved). '' when the set is right. */
  function lineHtml(st) {
    const b = st && st.setBreaks; if (!b || !b.line) return '';
    // (SETFIX A) the set says exactly what to press, and the press is right here: "Cut RG Sheet 16" for a sheet whose green line is not cut, and "Rebuild this set correctly"
    const r = b.remedy, cut = r && r.kind === 'cut' ? `<button type="button" class="btn sm" data-cut-set="${esc(st.setId)}">${esc(r.button)}</button>` : '';
    // (Paul, 26 Oct 2026: a back engraving to approve is no rebuild: no Rebuild button for it, ever; the press is in the Engraving tab, said in the line)
    const rebuild = r && r.kind === 'backs' ? '' : `<button type="button" class="btn sm" data-rebuild-set="${esc(st.setId)}">${REBUILD}</button>`;
    return `<div class="setBreak" data-set-break="${esc(st.setId)}" role="note"><span class="setBreakLine" data-set-break-line title="${esc((b.rules || []).join(' · ') + ' · docs/charm-nest/set-rules.md')}">${esc(b.line)}</span>${cut}${rebuild}${r && r.say ? `<span class="setBreakTodo" data-set-break-todo>${esc(r.say)}</span>` : ''}<span class="setBreakSay" data-set-break-say hidden></span></div>`;
  }

  /** The page's own state the repair reads and drives. A test passes its own `env`. */
  function pageEnv(o) {
    if (o && o.env) return o.env;
    const G = root.Gate, S_ = root.Sets, C = root.CN, B_ = root.B, RC = root.RunCtl, ops = root.CharmNestOperations;
    return {
      run: () => (B_ && B_.run) || null,
      pages: () => (C && C.allSheets ? C.allSheets() : []),
      cloud: () => !(C && C.S && C.S.cloud) || C.S.cloud.ok !== false,
      committing: run => !!(ops && run && ops.running && ops.running('commit:' + run.runId)),
      holding: p => !!(G && G.holding && G.holding(p)),
      membershipBusy: run => !!(ops && run && ops.running && ops.running('membership:' + run.runId)) || !!(G && G.state && G.state().membershipPending),
      setsOfRun: runId => (S_ && S_.ofRun ? S_.ofRun(runId) : []),
      api: (body, o2) => C.api('charmNestLibrary', body, o2 || {}),
      assemble: run => G.assemble(run),
      save: run => (RC && RC.save ? RC.save(run) : Promise.resolve()),
      /* VISUAL (26 Oct 2026, Paul: "I don't see you actually visually testing the outcome of animations and transitions"): `id`, a 10K / 14K sheet just cut out of
         its grey wait: the Library was drawn again in one go and its tile jumped from grey to colour in another place; now the Library's own glide carries it from where
         it stood greyed and its colour comes back over the same time, opacity only (reduced motion: drawn at once) */
      refresh: id => { try { if (G && G.refreshMembership) G.refreshMembership(); } catch (_) { /* the next drawing follows */ }
        try { if (!(C && C.loadLibrary)) return; const body = root.document && root.document.getElementById('libBody'), LD = root.LibraryDone;
          if (!id || !body || !LD || !LD.glide || !body.querySelector('[data-gold-pending]')) { Promise.resolve(C.loadLibrary()).catch(() => {}); return; }
          Promise.resolve(LD.glide(body, () => C.loadLibrary())).then(() => { const c = [...body.querySelectorAll('.sheetsRow > .librarySheet:not(.goldPending) > .libCard[data-id]')].find(n => n.dataset.id === String(id));
            if (c && root.Motion && !root.Motion.reduced()) c.animate([{ opacity: .5 }, { opacity: 1 }], { duration: (root.Motion.T && root.Motion.T.slide) || 760, easing: 'ease' }); }).catch(() => {});
        } catch (_) { /* as above */ } },
      say: (text, kind) => { if (C && C.toast) C.toast(text, kind || 'bad', 7000); },
      by: () => (root.CNEmployee && root.CNEmployee.name ? root.CNEmployee.name() : ''),
      confirm: p => confirmDialog(p),
      confirmCut: words => confirmDialog({ words }),
      // (SETFIX A) the Cut Sheet press of the Library (LibraryFlow.cutLine: the green dash line drawn and dated, the cut recorded, LibraryFlowRose.calculate with recordCut); a sheet whose line
      // is drawn already is skipped by that press and has its cut recorded by the Cut Sheet button's own press (RoseStock.record). Called only after the person's yes in the window.
      cut: async (ids, by) => {
        const F = root.LibraryFlow, RS = root.RoseStock, pageOf = id => (C && C.allSheets ? C.allSheets() : []).find(p => p.sheetId === id && !p.recalled);
        if (!F || typeof F.cutLine !== 'function') throw new Error('The Cut Sheet press is not loaded on this page: reload it');
        const r = await F.cutLine({ kind: 'sheet', id: ids[0], sheetIds: ids, by });
        if (!r || r.ok === false) throw new Error((r && r.error) || 'The green dash line could not be added');
        for (const id of ids) { const pg = pageOf(id); if (pg && !pg.roseCutAt && RS && typeof RS.record === 'function') await RS.record(pg, { by }); }
        return ids.map(id => ({ id, cut: !!(pageOf(id) || {}).roseCutAt }));
      },
      // (Paul, 27 Oct 2026) a 10K / 14K sheet whose layout leaves no room for another charm (RoseStock.full, the test its own Cut Sheet button reads): no green line, its approval is all it needs
      full: pg => { const RS = root.RoseStock; try { return !!(pg && RS && typeof RS.full === 'function' && RS.full(pg)); } catch (_) { return false; } },
      // its approval: the sheet's own Include into its ONE set (Gate.cutInclude, the step the Cut Sheet press takes first), nothing drawn, no cut recorded. An include that did not put it in a set is
      // taken back (Gate.undoGoldJoin): it waits greyed as before. Throws the plain reason; called only after the person's yes in the window
      join: async pg => {
        const G = root.Gate, inSet = () => !!pg.setId && !pg.draft, was = { inSet: inSet(), pick: pg.solidPick };
        if (!G || typeof G.cutInclude !== 'function') throw new Error('Sets are not loaded yet: try again in a moment');
        try { await G.cutInclude(pg); } catch (e) { throw new Error(String((e && e.message) || e).replace(/^Not cut: /, '')); }
        if (inSet()) return true;
        const pol = typeof G.policy === 'function' ? G.policy(pg) : null, why = pol && !pol.include && pol.reason ? pol.reason : '';
        if (typeof G.undoGoldJoin === 'function') { try { await G.undoGoldJoin(pg, was); } catch (_) { /* it says why on its card */ } }
        throw new Error(`it is not in a set yet${why ? ` (${why})` : ''}`);
      },
      note: () => {}
    };
  }
  /** Why a sheet's page is busy right now ('' when it is not): being nested, finishing, saved, edited in its sheet window. */
  function busyOf(page, env) {
    if (!page) return '';
    if (page.dirty || ['nesting', 'finishing', 'queued'].includes(page.status) || (page.persisted && !page.persistedDone) || page._rosePlanning || page._roseLoading || page._roseAction) return 'being nested or saved right now';
    if (page._incBusy) return 'being changed right now';
    if (env.holding(page)) return 'being changed in its sheet window';
    return '';
  }
  /** The small window: what will happen, Cancel / Rebuild. Resolves true to rebuild. */
  function confirmDialog(p) {
    const d0 = doc_(); if (!d0 || typeof d0.createElement !== 'function') return Promise.resolve(true);
    styleOnce();
    return new Promise(resolve => {
      const d = d0.createElement('dialog'); d.className = 'splitDlg setRepairDlg'; d.setAttribute('aria-labelledby', 'setRepairT'); d.setAttribute('aria-describedby', 'setRepairP');
      const w = p.words;
      d.innerHTML = `<div class="dlg"><div class="dlgHead"><h3 id="setRepairT">${esc(w.title)}</h3></div>
        <div class="dlgBody"><p id="setRepairP">${esc(w.intro)}</p><ul>${w.lines.map(x => `<li class="${x.kind}">${esc(x.text)}</li>`).join('')}</ul>
        <p>${esc(w.what)}${w.numbering ? ' ' + esc(w.numbering) : ''}</p><p class="nothing">${esc(w.nothing)}</p></div>
        <div class="dlgFoot"><div class="left"><button type="button" class="btn ghost sm" data-k="cancel">${esc(w.cancel)}</button></div><button type="button" class="btn sage sm" data-k="go">${esc(w.confirm)}</button></div></div>`;
      d0.body.appendChild(d);
      const done = v => { try { if (d.open) d.close(); } catch (_) { /* closed */ } d.remove(); resolve(v); };
      d.querySelector('[data-k=go]').onclick = () => done(true);
      d.querySelector('[data-k=cancel]').onclick = () => done(false);
      d.addEventListener('cancel', e => { e.preventDefault(); done(false); });
      try { d.showModal(); d.querySelector('[data-k=cancel]').focus(); } catch (_) { d.setAttribute('open', ''); }
    });
  }

  /** The set and the sheets of its run, read from the cloud now (the Library's list may be a few seconds old), with this page's own records of the run's sets. */
  async function readWorld(st, env) {
    const run = env.run();
    const got = await env.api({ op: 'setGet', setId: st.setId }, { quiet: true, label: 'Reading the set' });
    const doc = got && got.set ? Object.assign({}, st, got.set) : null;
    const runId = str((doc && doc.runId) || st.runId || (run && run.runId));
    const rowsRun = runId ? await env.api({ op: 'listSheets', limit: 500, runId }, { all: 'sheets', quiet: true }) : { sheets: [] };
    const ids = new Set((rowsRun.sheets || []).map(r => r.id));
    // sheets of this set that carry another run id (a sheet the set took from another run) are read by the set
    const more = doc && doc.sheetIds ? doc.sheetIds.filter(i => !ids.has(i)) : [];
    const extra = more.length ? await env.api({ op: 'listSheets', setId: st.setId, limit: 300 }, { all: 'sheets', quiet: true }) : { sheets: [] };
    const sheets = [...new Map([...(rowsRun.sheets || []), ...(extra.sheets || [])].map(r => [r.id, r])).values()];
    const sets = env.setsOfRun(runId).map(s => Object.assign({}, s)).filter(s => s.setId !== st.setId);
    return { doc, sheets, sets, runId };
  }
  /** What only the page knows of a sheet, laid on its cloud record: the time the run first saw it completed (the order sets form in) and that a Rose Gold sheet is full (it takes no green line). */
  function withPageFacts(rows, pageOf) {
    return list(rows).map(r => {
      const pg = r && pageOf(r.id), e = {};
      if (!pg) return r;
      if (+pg.releasedAt > 0) e.releasedAt = +pg.releasedAt;
      if (pg._roseFull === true) e.roseFull = true;
      return Object.keys(e).length ? Object.assign({}, r, e) : r;
    });
  }
  const planFrom = (w, env) => {
    const run = env.run(), pages = env.pages();
    const pageOf = id => pages.find(p => p.sheetId === id && !p.recalled);
    // the order the run forms sets in is the time it first saw a sheet completed (page.releasedAt, which the cloud does not keep): the plan reads the same order
    w = Object.assign({}, w, { sheets: withPageFacts(w.sheets, pageOf) });
    const o = {
      cloud: env.cloud(), committing: env.committing(run), runHere: !!(run && w.runId && run.runId === w.runId && !['complete', 'abandoned'].includes(run.status)),
      busy: row => {
        const pg = pageOf(row.id);
        if (env.membershipBusy(run) && inSetOf(row) === w.doc.setId) return 'waiting for the run to finish updating its sets';
        return busyOf(pg, env);
      }
    };
    return { p: plan(w.doc, w.sheets, w.sets.concat([w.doc]), o), pageOf };
  };

  const running = new Set();
  /** The press. Resolves { ok, ...plan, formed, check } or { ok:false, refused|cancelled|error }. */
  async function rebuild(st, o) {
    const env = pageEnv(o), id = str(st && st.setId);
    if (!id) return { ok: false, refused: { key: 'noSet', reason: 'That set could not be found' } };
    if (running.has(id)) return { ok: false, refused: { key: 'busy', reason: 'This set is being rebuilt right now' } };
    running.add(id);
    let touched = false;      // (the Library is drawn again only when something may have changed: a refusal leaves the card, and its line, as it is)
    try {
      const first = await readWorld(st, env);
      if (!first.doc) { const r = { key: 'noSet', reason: 'That set could not be found: refresh the Library' }; env.say(r.reason); return { ok: false, refused: r }; }
      const a = planFrom(first, env).p;
      if (!a.ok) { env.say(a.refused.reason, a.noop ? 'ok' : 'bad'); return { ok: false, refused: a.refused, noop: a.noop, plan: a }; }
      if (!(o && o.skipConfirm) && !(await env.confirm(a))) return { ok: false, cancelled: true, plan: a };
      // the window was open for a while: the same plan must come out of what the cloud says NOW, or nothing is changed
      const second = await readWorld(st, env), b = planFrom(second, env);
      if (!b.p.ok || b.p.signature !== a.signature) {
        const reason = b.p.ok ? `${a.name} changed while you were looking: nothing was changed. Press Rebuild again to see the new plan` : b.p.refused.reason;
        env.say(reason); return { ok: false, refused: { key: 'changed', reason }, plan: b.p };
      }
      touched = true;
      if (o && typeof o.onState === 'function') { try { o.onState('rebuilding', `Rebuilding ${a.name}: the sheets leave, then the next sets form. Nothing is deleted.`); } catch (_) { /* a line of text only */ } }
      const res = await apply(b.p, second, env, b.pageOf);
      return Object.assign({ plan: b.p }, res);
    } catch (e) {
      const reason = `${(e && e.message) || e}`;
      env.say(`The set was not rebuilt completely: ${reason}. Nothing was deleted; press Rebuild again to carry on.`);
      return { ok: false, error: reason };
    } finally { running.delete(id); if (touched) env.refresh(); }
  }

  /** The writes: the existing operations only (the run's own assembly lets sheets go and forms the next sets). */
  async function apply(p, w, env, pageOf) {
    const run = env.run(), setId = p.setId, G = root.Gate || null;
    const pagesOfExpelled = p.expel.map(i => ({ id: i, pg: pageOf(i), row: w.sheets.find(r => r.id === i) }));
    const lost = pagesOfExpelled.find(x => !x.pg);
    if (lost) throw new Error(`${lost.row ? currentName(lost.row) : 'a sheet'} is not loaded on this screen: reload the run first`);
    const stale = pagesOfExpelled.find(x => x.row && inSetOf(x.row) === setId && !(x.pg.setId === setId && !x.pg.draft));
    if (stale) throw new Error(`${currentName(stale.row)} is not in ${p.name} on this page: reload the run first`);
    const solid = m => m === 'gold10k' || m === 'gold14k';
    const family = x => GOLD_FAMILY.includes(classOf(x.row || x.pg));
    const marks = pagesOfExpelled.map(x => ({ x, was: { leftSet: x.pg.leftSet, draft: x.pg.draft, solidPick: x.pg.solidPick } }));
    // the loose sheets the plan puts into the new sets: held for the first pass too, or the run would form their set before the sheets that leave are free (the numbers would not follow the plan)
    const leaving = new Set(p.expel), staying = new Set(p.keep);
    const outsiders = p.sets.flatMap(s => s.sheetIds).filter(i => !leaving.has(i) && !staying.has(i)).map(i => pageOf(i)).filter(pg => pg && !pg.setId).map(pg => ({ pg, was: pg.leftSet === setId ? undefined : pg.leftSet }));
    const letGo = () => { for (const q of outsiders) { if (q.was === undefined) delete q.pg.leftSet; else q.pg.leftSet = q.was; } };
    // 1. all the sheets that leave, at once: taken out and drafts (a unit that shares an order leaves whole, so the cardinal rule pulls none of them back)
    for (const m of marks) { m.x.pg.leftSet = setId; m.x.pg.draft = true; if (solid(m.x.pg.metal)) m.x.pg.solidPick = false; }
    for (const q of outsiders) q.pg.leftSet = setId;
    const undo = list => { for (const m of list) { m.x.pg.leftSet = m.was.leftSet; m.x.pg.draft = m.was.draft; m.x.pg.solidPick = m.was.solidPick; if (m.was.leftSet === undefined) delete m.x.pg.leftSet; } };
    try { if (marks.length) await env.assemble(run); } catch (e) { letGo(); undo(marks.filter(m => m.x.pg.setId)); for (const m of marks) if (!m.x.pg.setId && !family(m.x)) delete m.x.pg.leftSet; throw e; }   // (a sheet already taken out is free again, not marked for the next press)
    letGo();
    const stuck = marks.filter(m => m.x.pg.setId);
    if (stuck.length) {
      undo(stuck);
      const why = env.holding(stuck[0].x.pg) ? 'it is being changed in its sheet window' : stuck[0].x.pg.cardinalPull ? 'an order on it is shared with another sheet of the set' : 'the run keeps it';
      throw new Error(`${currentName(stuck[0].x.row)} could not be taken out of ${p.name}: ${why}. ${marks.length - stuck.length} sheet${marks.length - stuck.length === 1 ? ' was' : 's were'} already taken out`);
    }
    // 2. the sheets that left may join a set again by the usual rules (Rose Gold, 10K and 14K only by their own press: they stay marked as taken out until you Include / Cut them)
    for (const m of marks) if (!family(m.x)) delete m.x.pg.leftSet;
    // (a stop between here and the end leaves the run owing a membership pass: it forms the next sets by itself at its next look, as for any change of membership)
    try { run.membershipDirty = true; } catch (_) { /* a frozen run record */ }
    if (marks.length) { await env.assemble(run); }
    try { run.membershipDirty = false; } catch (_) { /* a frozen run record */ }
    await env.save(run);
    // 3. what is there now, from the cloud, judged by the same checker
    const after = await readWorld({ setId, runId: w.runId }, env);
    const sets = [after.doc].concat(after.sets).filter(Boolean), diag = diagnose(sets, after.sheets, {});
    const mine = new Set([setId].concat(after.sheets.filter(r => inSetOf(r) && !w.sheets.some(x => x.id === r.id && inSetOf(x) === inSetOf(r))).map(r => inSetOf(r))));
    const still = diag.list.filter(i => mine.has(i.setId));
    const formed = sets.filter(s => s && !s.committedAt).map(s => ({ setId: s.setId, name: setWord(s), sheetIds: after.sheets.filter(r => inSetOf(r) === s.setId).map(r => r.id) })).filter(s => s.sheetIds.length);
    const words = p.words.summary;
    if (still.length) {
      env.say(`${p.name} was rebuilt, but it still breaks your rules: ${still[0].line}. Nothing was deleted.`, 'bad');
      return { ok: false, rebuilt: true, formed, still: still.map(i => i.line), words };
    }
    env.say(`${p.name} rebuilt. ${words}`, 'ok');
    return { ok: true, rebuilt: true, formed, words };
  }

  /** (SETFIX A) The "Cut RG Sheet 16" press of a set whose only fault is a Rose Gold / 10K / 14K sheet with no green line cut: a small window (what is drawn, that a recorded cut is permanent,
   *  Cancel changes nothing), then the existing Cut Sheet press for those sheets. Nothing moves: the sheets stay in the set. Resolves { ok, cut:[{id,cut}] } or { ok:false, refused|cancelled|error }. */
  async function cutGold(st, o) {
    const env = Object.assign({}, pageEnv({}), (o && o.env) || {}), b = st && st.setBreaks, r = b && b.remedy, id = str(st && st.setId);
    const refuse = (key, reason) => { env.say(reason); return { ok: false, refused: { key, reason } }; };
    if (!r || r.kind !== 'cut' || !list(r.sheets).length) return refuse('none', `${setWord(st || {})} has no sheet whose green line is to be cut`);
    if (running.has('cut:' + id)) return { ok: false, refused: { key: 'busy', reason: 'This sheet is being cut right now' } };
    const ids = list(r.sheets).map(str), names = and(list(r.names).length ? r.names : ids), one = ids.length === 1, set = setWord(st);
    const by = str(env.by()).trim();
    if (!by) return refuse('who', 'Sign in first: the cut is recorded with your name');
    if (!env.cloud()) return refuse('offline', 'The page is offline: reconnect, then press it again. Nothing was changed');
    const pages = env.pages(), pageOf = x => list(pages).find(p => p && p.sheetId === x && !p.recalled);
    const away = ids.filter(x => !pageOf(x));
    if (away.length) return refuse('away', `${and(away.map(x => list(r.names)[ids.indexOf(x)] || x))} ${away.length === 1 ? 'is' : 'are'} not open on this page, so ${away.length === 1 ? 'its' : 'their'} green line cannot be cut from here. Open ${away.length === 1 ? 'it' : 'them'} on the Nest tab and press Cut Sheet there; ${set} can then be approved`);
    for (const x of ids) { const why = busyOf(pageOf(x), env); if (why) return refuse('busy', `${list(r.names)[ids.indexOf(x)] || x} is ${why}: try again when it is done. Nothing was changed`); }
    const words = {
      title: `Cut ${names}?`, intro: `${set} cannot be approved while ${names} ${one ? 'has' : 'have'} no cut green line: a Rose Gold, 10K or 14K sheet is in a set only once its green line is cut (your rule R12). This is the Cut Sheet press:`,
      lines: ids.map((x, k) => ({ kind: 'set', text: `${list(r.names)[k] || x}: the green dash line is drawn and dated, and the cut is recorded` })),
      what: `${one ? 'It stays' : 'They stay'} in ${set} with ${one ? 'its' : 'their'} pieces; nothing else changes. A recorded cut is permanent.`, numbering: '',
      nothing: 'Nothing is added until you press Cut.', confirm: 'Cut', cancel: 'Cancel'
    };
    if (!(o && o.skipConfirm) && !(await env.confirmCut(words))) return { ok: false, cancelled: true };
    running.add('cut:' + id);
    try {
      if (o && typeof o.onState === 'function') { try { o.onState('cutting', `Cutting ${names}: the green dash line is drawn, then the cut is recorded.`); } catch (_) { /* a line of text only */ } }
      const done = await env.cut(ids, by), left = done.filter(d => !d.cut);
      if (left.length) { const reason = `The green line of ${and(left.map(d => list(r.names)[ids.indexOf(d.id)] || d.id))} is not cut yet: open it on the Nest tab and press Cut Sheet there`; env.say(reason); return { ok: false, error: reason, cut: done }; }
      env.say(`${names} cut. ${set} follows your rules${r.then ? `: now press "${REBUILD}"` : ' and can be approved for laser cutting'}.`, 'ok');
      return { ok: true, cut: done, words: r.then ? `Cut. Now press "${REBUILD}".` : `Cut. ${set} can be approved for laser cutting.` };
    } catch (e) {
      const reason = `${(e && e.message) || e}`; env.say(`${names} ${one ? 'was' : 'were'} not cut: ${reason}`); return { ok: false, error: reason };
    } finally { running.delete('cut:' + id); env.refresh(); }
  }
  /** (Paul, 26 Oct 2026: "the 10k and 14k sheets should stay greyed-out inside the Set of Sheets until a user approves and adds a Green Cut line ... at which point that particular 10k/14k gold sheet would be
   *  locked and the Set of Sheets where it resides would be released from that particular Hold".) The "Approve & cut green line" press of a 10K / 14K sheet waiting greyed in its set (st.goldPending, the
   *  Library's goldPendingOf): a small window (it joins the set, its green dash line is drawn and dated, the cut is recorded with your name and is permanent, its layout locks; Cancel changes nothing), then
   *  the existing Cut Sheet press (env.cut: LibraryFlow.cutLine, and the Cut Sheet button's own press for a full sheet that needs no line), with every check it has. The cut sheet is in the set (R12) and the
   *  set's hold for it lifts. A press whose cut is not recorded leaves the sheet waiting greyed as before (RoseStock.record takes back the include it made). Never run by itself.
   *  Resolves { ok, cut:[{id,cut}], words } or { ok:false, refused|cancelled|error }. */
  async function cutPending(st, sheetId, o) {
    const env = Object.assign({}, pageEnv({}), (o && o.env) || {}), id = str(sheetId), set = setWord(st || {});
    const refuse = (key, reason) => { env.say(reason); return { ok: false, refused: { key, reason } }; };
    const row = list(st && st.goldPending).find(r => idOf(r) === id);
    if (!row) return refuse('none', `That sheet is not waiting in ${set} any more: the Library is drawn again`);
    if (running.has('cut:' + id)) return { ok: false, refused: { key: 'busy', reason: 'This sheet is being cut right now' } };
    const name = currentName(row), by = str(env.by()).trim();
    if (!by) return refuse('who', 'Sign in first: the cut is recorded with your name');
    if (!env.cloud()) return refuse('offline', 'The page is offline: reconnect, then press it again. Nothing was changed');
    const pages = env.pages(), page = list(pages).find(p => p && p.sheetId === id && !p.recalled);
    if (!page) return refuse('away', `${name} is not open on this page, so its green line cannot be cut from here. Open it on the Nest tab and press Cut Sheet there; it then joins ${set}`);
    { const why = busyOf(page, env); if (why) return refuse('busy', `${name} is ${why}: try again when it is done. Nothing was changed`); }
    // (Paul, 27 Oct 2026: "Only partially completed 10K / 14K Gold sheets need to have a Green Cut line. Completed 10K / 14K Gold Sheets only need an user approval") a full sheet is approved into the set:
    // no line is drawn, no cut is recorded; its set then waits for its back engravings as for every sheet's
    if (typeof env.full === 'function' && env.full(page)) return approveFull(st, env, page, { id, name, set, by }, o);
    const words = {
      title: `Approve and cut ${name}?`, intro: `${name} shares orders with the sheets of ${set} and waits there, greyed, for your approval. This is its Cut Sheet press:`,
      lines: [{ kind: 'set', text: `${name}: the green dash line is drawn and dated, and the cut is recorded with your name` }, { kind: 'set', text: `${name} joins ${set}, and ${set} no longer waits for it` }],
      what: `A recorded cut is permanent: ${name}'s layout is locked from then on, so move its charms by hand first if anything should change.`, numbering: '',
      nothing: 'Nothing is cut until you press Approve & cut.', confirm: 'Approve & cut', cancel: 'Cancel'
    };
    if (!(o && o.skipConfirm) && !(await env.confirmCut(words))) return { ok: false, cancelled: true };
    running.add('cut:' + id);
    try {
      if (o && typeof o.onState === 'function') { try { o.onState('cutting', `Cutting ${name}: the green dash line is drawn, then the cut is recorded.`); } catch (_) { /* a line of text only */ } }
      const done = await env.cut([id], by);
      if (!done.every(d => d.cut)) { const reason = `The green line of ${name} is not cut yet: open it on the Nest tab and press Cut Sheet there`; env.say(reason); return { ok: false, error: reason, cut: done }; }
      env.say(`${name} approved and cut: it is in ${set} now.`, 'ok');
      return { ok: true, cut: done, words: `Approved and cut: ${name} is in ${set}.` };
    } catch (e) {
      const reason = `${(e && e.message) || e}`; env.say(`${name} was not cut: ${reason}`); return { ok: false, error: reason };
    } finally { running.delete('cut:' + id); env.refresh(id); }
  }
  /** cutPending's press for a FULL 10K / 14K sheet: a small window (it joins the set; it is full, so no green line is drawn and no cut is recorded; Cancel changes nothing), then its approval (env.join). */
  async function approveFull(st, env, page, x, o) {
    const { id, name, set } = x;
    const words = {
      title: `Approve ${name}?`, intro: `${name} shares orders with the sheets of ${set} and waits there, greyed, for your approval. It is full, so there is nothing to cut away:`,
      lines: [{ kind: 'set', text: `${name} joins ${set}, and ${set} no longer waits for it` }, { kind: 'set', text: `No green line is drawn and no cut is recorded` }],
      what: `${set} is approved for laser cutting once the back engravings of its sheets are approved, ${name}'s too.`, numbering: '',
      nothing: 'Nothing changes until you press Approve.', confirm: 'Approve', cancel: 'Cancel'
    };
    if (!(o && o.skipConfirm) && !(await env.confirmCut(words))) return { ok: false, cancelled: true };
    running.add('cut:' + id);
    try {
      if (o && typeof o.onState === 'function') { try { o.onState('joining', `Adding ${name} to ${set}…`); } catch (_) { /* a line of text only */ } }
      await env.join(page);
      env.say(`${name} approved: it is in ${set} now.`, 'ok');
      return { ok: true, joined: true, cut: [], words: `Approved: ${name} is in ${set}.` };
    } catch (e) {
      const reason = `${(e && e.message) || e}`; env.say(`${name} was not approved: ${reason}`); return { ok: false, error: reason };
    } finally { running.delete('cut:' + id); env.refresh(id); }
  }
  /** Wires the "Approve & cut green line" press of every 10K / 14K sheet drawn greyed in a set card (the Library's goldPendingTile), and the "Approve" of a full one. */
  function wirePending(card, st, o) {
    if (!card || typeof card.querySelectorAll !== 'function') return;
    for (const b of card.querySelectorAll('[data-gold-cut]')) {
      b.onclick = async e => {
        if (e && e.stopPropagation) e.stopPropagation();
        const box = b.closest('[data-gold-pending-note]'), say = box && box.querySelector('[data-gold-say]');
        b.disabled = true; if (box) box.setAttribute('aria-busy', 'true');
        try {
          const r = await cutPending(st, b.dataset.goldCut, Object.assign({}, o || {}, { onState: (kind, text) => { if (say) { say.hidden = false; say.textContent = text; } } }));
          if (say) { say.textContent = r.ok ? r.words || '' : r.cancelled ? '' : r.refused ? r.refused.reason : r.error || ''; say.hidden = !say.textContent; }
        } finally { b.disabled = false; if (box) box.removeAttribute('aria-busy'); }
      };
    }
  }
  /** (SETFIX A) The remedy's first press (the button under Approve for laser cutting): the Cut Sheet press, or the rebuild. */
  const fix = (st, o) => { const k = st && st.setBreaks && st.setBreaks.remedy && st.setBreaks.remedy.kind; if (k === 'backs') return Promise.resolve({ ok: false, error: st.setBreaks.remedy.say }); return k === 'cut' ? cutGold(st, o) : rebuild(st, o); };   // ('backs': nothing to press here, the Engraving tab approves it)

  /** Wires the button of a drawn card. `card` holds lineHtml(st). */
  function wire(card, st, o) {
    if (!card || typeof card.querySelector !== 'function') return;
    const c = card.querySelector('[data-cut-set]');
    if (c) c.onclick = async e => {
      if (e && e.stopPropagation) e.stopPropagation();
      const box = c.closest('.setBreak'), say = box && box.querySelector('[data-set-break-say]');
      c.disabled = true; if (box) box.setAttribute('aria-busy', 'true');
      try {
        const r = await cutGold(st, Object.assign({}, o || {}, { onState: (kind, text) => { if (say) { say.hidden = false; say.textContent = text; } } }));
        if (say) { say.textContent = r.ok ? r.words || '' : r.cancelled ? '' : r.refused ? r.refused.reason : r.error || ''; say.hidden = !say.textContent; }
      } finally { c.disabled = false; if (box) box.removeAttribute('aria-busy'); }
    };
    const b = card.querySelector('[data-rebuild-set]'); if (!b) return;
    styleOnce();
    b.onclick = async e => {
      if (e && e.stopPropagation) e.stopPropagation();
      const box = b.closest('.setBreak'), say = box && box.querySelector('[data-set-break-say]');
      b.disabled = true; if (box) box.setAttribute('aria-busy', 'true');
      try {
        const r = await rebuild(st, Object.assign({}, o || {}, { onState: (kind, text) => { if (say) { say.hidden = false; say.textContent = text; } } }));
        if (say) { say.hidden = false; say.textContent = r.ok ? `Rebuilt. ${r.words || ''}` : r.cancelled ? '' : r.refused ? r.refused.reason : r.error ? `Not completely rebuilt: ${r.error}` : (r.still || [])[0] || ''; if (!say.textContent) say.hidden = true; }
      } finally { b.disabled = false; if (box) box.removeAttribute('aria-busy'); }
    };
  }
  /** The groups the Library draws (libraryGroups' map values), annotated with `setBreaks` where a set breaks a rule. Derived each time, never saved. */
  function annotate(groups, sheets, setDocs, o) {
    let pages = []; try { pages = root.CN && typeof root.CN.allSheets === 'function' ? root.CN.allSheets() : []; } catch (_) { pages = []; }
    const byPage = new Map(list(pages).filter(p => p && p.sheetId).map(p => [p.sheetId, p]));
    const rows = withPageFacts(sheets, id => byPage.get(id)); let via = null;
    // (the page's own read of the open run, when the page has one: Gate.violations(run), the checker's answer or its list of violations; any other shape is ignored and the sets are read here)
    try { const G = root.Gate, B_ = root.B; if (G && typeof G.violations === 'function' && B_ && B_.run) { via = G.violations(B_.run); if (Array.isArray(via)) via = { violations: via }; if (!via || typeof via.then === 'function' || !Array.isArray(via.violations)) via = null; } } catch (_) { via = null; }
    const diag = diagnose(list(setDocs), rows, { viaGate: via, ...(o || {}) });
    for (const g of list(groups)) {
      if (!g || !g.setId) continue;
      // (Paul, 26 Oct 2026: "we shouldn't double hold things") what the set's Approve for laser cutting waits for that is no rule it breaks: a green-line sheet's back engraving still to approve. One plain
      //  waiting line (CharmNestReadiness reads set.setWaits: Approve stays grey), never the red line or Rebuild. Derived at every drawing, never saved
      const ws = ((diag && diag.result && diag.result.waits) || []).filter(w => w.set === g.setId);
      if (ws.length) { const who = ws.map(w => w.name), l = `waiting for the back engraving approval of ${who.length < 2 ? who.join('') : who.slice(0, -1).join(', ') + ' and ' + who[who.length - 1]}`; g.setWaits = { line: l.charAt(0).toUpperCase() + l.slice(1), short: l, items: ws.map(w => Object.assign({}, w)) }; }
      else delete g.setWaits;
      const b = breaksOf(diag, g.setId);
      if (b) {
        g.setBreaks = b;
        // the older sentence under the sheets ("... share no order with the other sheets, and the set is valid without them") names a whole unit as if each sheet stood alone; the red line says it exactly
        if (typeof g.cardinalLinks === 'string') { const kept = g.cardinalLinks.split('. ').filter(s => !/ no order with the other sheets, and the set is valid without /.test(s)).join('. '); if (kept) g.cardinalLinks = kept; else delete g.cardinalLinks; }
      } else delete g.setBreaks;
    }
    return diag;
  }

  const exempt = d => { const R = SR(); return !!R && R.exempt(d); };
  return { diagnose, breaksOf, plan, rebuild, apply, lineHtml, wire, annotate, confirmDialog, busyOf, infoOf, clause, numberJoiners, inSetOf, rowsOf, setsOf, exempt, NOT_SHOWN: [...NOT_SHOWN],
    remedyOf, joinCheck, cutGold, cutPending, wirePending, fix, REBUILD };
});
