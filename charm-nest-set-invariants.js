/* CharmNestSetInvariants: THE acceptance criterion of a Set of Sheets (docs/charm-nest/set-rules.md is its rule list; R-numbers below are that file's).
 *
 * Paul stated the rules of a Set of Sheets more than once and the app kept forming a set wrongly. This file reads ANY state of sheets and sets, a live
 * assembly's result, a Library read, a test fixture, and says, rule by rule, whether it is a state the owner's rules allow. It decides nothing about HOW
 * a set is formed (charm-nest-set-rules.js form/formAll does that and is read, not copied, here); it only judges what exists. A formation is right when
 * check() returns ok; a formation that check() calls wrong is wrong, whatever else passes.
 *
 *   check(sets, sheets, opts?) -> { ok, violations, partners, warnings, sets, notes }
 *   expected(sheets, opts?)    -> { sets:[{ id, name, sheetIds, names }], waiting:[{ id, name, why }], partners, held }   the formation the rules make from these sheets
 *                                 (every sheet taken as free; the real SetRules.formAll over the real SharedOrders units; check() accepts it by construction)
 *   explain(result)            -> [string]   one plain line per violation / partner, for a log or a failing test
 *
 *   violation = { rule, ruleNo, letter?, id, set, setName, sheets:[sheetId], names:[label], message, ...extra }
 *     rule 'invalid'         (a) R1  the set does not hold a completed GF sheet and a completed SS sheet
 *     rule 'surplus'         (b) R7  a UNIT (sheets tied by a shared multi-piece order, SharedOrders.groups) the set does not need: it is still valid without it
 *     rule 'partial'         (c) R3  a GF or SS sheet that is not full / released / cut is in the set
 *     rule 'notReady'        (d) R12 a Rose Gold / 10K / 14K sheet is in the set without its green line cut (or its Cut Sheet press approved) or without its back engraving approved
 *     rule 'cardinal'        (e) R9  two sheets that share a multi-piece order are in different sets, or one that could join is outside the set that holds its partner
 *     rule 'closedJoin'      (f) R6  a sheet joined a set after it had formed (closed to automatic joining) and no shared order, no gold-family rule and no person explains it
 *     rule 'partialPartner'      R11 a sheet of the set shares an order with a partial GF/SS sheet outside every set: that whole group waits outside any set until it is full
 *     rule 'numbering'           R14 two sheets of one metal in one set carry the same number
 *     rule 'unformed'            R5  completed sheets wait outside every set although a completed GF and a completed SS sheet among them make a valid set (opts.unformed:false switches it off)
 *     rule 'membership'          bookkeeping: a sheet the set lists does not exist / is archived / is a draft / says it is in another set / is in two sets
 *   partners = [{ set, setName, sheet, name, kind:'notReady'|'partial'|'bridge', because:[orderId], with:[label], reasons:[string] }]: a sheet outside the set that shares an order with it and
 *     cannot join yet (R10). Not a violation: the set is right without it; it cannot be approved for laser cutting until that sheet is in it. kind 'bridge' (SetRules.PARTIAL_BRIDGE 'spread' only): a
 *     partial sheet with orders on several groups, which holds none of them: a person takes those orders off.
 *   GROUPS (SETFIX, Paul 17 Oct 2026): the sheets that share a multi-piece order are read by SharedOrders formation: a partial GF/SS sheet outside every set is an END of its groups (it holds them
 *     whole, it never chains two groups together or makes the set they join); every other sheet, a Rose Gold partner outside a set included, ties the sheets it shares orders with (R9).
 *   warnings = [{ rule:'orderSplit', ruleNo:'R16', ... }]: a multi-piece order spread over two sheets of one colour (a nesting fault that makes the chains); never fails ok.
 *
 * INPUT (page objects, Library rows and fixtures are all read; nothing is written, no page state, no network):
 *   sheets  [ { id | sheetId, metal:'gold'|'silver'|'rose'|'gold10k'|'gold14k' (or metalLabel "GF"), releaseFull, laserDoneAt, roseCutAt, density, placedCount | placements,
 *               page, sheetIndex, releasedAt, draft, archived, setId,
 *               the pieces: poolIds ["order_transaction_copy"] | pieces [{key, orderId}] | orders ["order"] | charms+placements (a page),
 *               gold-family only: cutApproved (the person's yes at the Cut Sheet press), roseFull (a full sheet takes no line), backsApproved (true|false) or laser (the readiness report),
 *               joinedAt (when it joined its set), joinedBy:'person' } ]     an object { id: sheet } or a Map is read too
 *   sets    [ { id | setId, name, seq | setSeq, sheetIds | sheets | members, formedAt?, committedAt?, status? } ]  or  [ [sheetId, ...] ]  or  { setId: [sheetId...] | set }
 *   opts    { skipExempt (a set committed to the Design Station / completed / replaced is not checked, as SetRules.gate; default: checked),
 *             partialPartners:'pending' (R11 listed in partners, not a violation: only for a set that already held a sheet of the group before the partial sheet appeared),
 *             legacySets:[setId] (the same, per set), waiting:[sheetId] (completed sheets outside every set that cannot join yet, still saving: they tie the sheets they share orders with, R5 and R9 do not call them due; SETFIX 19 Oct 2026), membership:false (do not read a listed sheet's own draft flag) | 'strict' (also refuse a sheet whose own setId names another set; the sets argument is otherwise the truth), allowShort:[setId] (a set still forming), firstSeq }
 *
 * Reads the real helpers, never a copy of them: SetRules (validSet, isCompleted, isPartial, metalClass, unitsOf, surplus, groupWait, formAll, exempt, fillWords),
 * SharedOrders core (sheetOf, groups, index, spans), SheetName (name, numberOf), SetEdit.engravingGap, CharmNestRose.cuts.
 * UMD like charm-nest-set-rules.js: window.CharmNestSetInvariants in pages, module.exports in node and in the Netlify functions. */
(function (root, factory) { const api = factory(root); if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestSetInvariants = api; })(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  let SRnode = null; try { SRnode = require('./charm-nest-set-rules.js'); } catch (_) { SRnode = null; }
  let SOnode = null; try { SOnode = require('./charm-nest-shared-orders.js').core; } catch (_) { SOnode = null; }
  let SNnode = null; try { SNnode = require('./charm-nest-sheet-name.js'); } catch (_) { SNnode = null; }
  let SEnode = null; try { SEnode = require('./charm-nest-set-edit.js'); } catch (_) { SEnode = null; }
  let CUTnode = null; try { CUTnode = require('./charm-nest-rose.js'); } catch (_) { CUTnode = null; }
  const SR = () => root.CharmNestSetRules || SRnode;
  const SO = () => root.CharmNestSharedOrders || SOnode;
  const SN = () => root.CharmNestSheetName || SNnode;
  const SE = () => root.SetEdit || SEnode;
  const CUTS = () => root.CharmNestRose || CUTnode;

  const RULE_NO = { invalid: 'R1', unformed: 'R5', partial: 'R3', closedJoin: 'R6', surplus: 'R7', cardinal: 'R9', partialPartner: 'R11', notReady: 'R12', numbering: 'R14', membership: 'R0', orderSplit: 'R16' };
  const LETTER = { invalid: 'a', surplus: 'b', partial: 'c', notReady: 'd', cardinal: 'e', closedJoin: 'f' };
  const GOLD_FAMILY = ['RG', '10K', '14K'];     // the metals with a green line (charm-nest-rose.js cuts(): the one list; this is its class codes)
  const WORD = { RG: 'Rose Gold', '10K': '10K Gold', '14K': '14K Gold' };

  const str = v => String(v == null ? '' : v);
  const list = x => Array.isArray(x) ? x : x instanceof Set ? [...x] : x instanceof Map ? [...x.values()] : x ? [x] : [];
  const idOf = s => str(s && typeof s === 'object' ? (s.id || s.sheetId) : s);
  const and = l => l.length < 2 ? l.join('') : `${l.slice(0, -1).join(', ')} and ${l[l.length - 1]}`;
  const orderWords = os => os.length === 1 ? `order ${os[0]}` : `orders ${os.slice(0, 3).join(', ')}${os.length > 3 ? ` and ${os.length - 3} more` : ''}`;
  const cap = s => s.charAt(0).toUpperCase() + s.slice(1);
  const needs = () => { const R = SR(), S = SO(); if (!R || !S) throw new Error('charm-nest-set-invariants.js needs charm-nest-set-rules.js and charm-nest-shared-orders.js'); return { R, S }; };
  const isCut = s => !!s && (!!s.roseCutAt || +s.laserDoneAt > 0);
  const rank = r => ({ GF: 0, SS: 1 })[SR().metalClass(r)] ?? 2;       // GF before SS before the rest, as the sets list their sheets
  const byKind = (x, y) => rank(x) - rank(y) || (+x.sheetIndex || +x.page || 0) - (+y.sheetIndex || +y.page || 0);

  /* ── reading the input ─────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  const pageId = s => s.id || s.sheetId || (s.runId && s.metal ? `${s.runId}:${s.metal}:${s.page || 1}` : '');
  function readSheets(input) {
    const rows = input instanceof Map ? [...input.values()] : Array.isArray(input) ? input : input && typeof input === 'object'
      ? Object.entries(input).map(([k, v]) => (v && typeof v === 'object' && !pageId(v) ? Object.assign({ id: k }, v) : v)) : [];
    const out = new Map();
    for (const s of rows) { if (!s || typeof s !== 'object') continue; const id = str(pageId(s)); if (id && !out.has(id)) out.set(id, Object.assign({}, s, { id })); }
    return out;
  }
  function readSets(input) {
    const rows = input instanceof Map ? [...input.entries()].map(([k, v]) => ({ k, v })) : Array.isArray(input) ? input.map(v => ({ k: null, v }))
      : input && typeof input === 'object' ? Object.entries(input).map(([k, v]) => ({ k, v })) : [];
    return rows.map((r, i) => {
      const v = r.v, plain = Array.isArray(v) || v instanceof Set, doc = plain ? {} : (v && typeof v === 'object' ? v : {});
      const ids = [...new Set((plain ? [...v] : list(doc.sheetIds || doc.sheets || doc.members || doc.ids)).map(idOf).filter(Boolean))];
      const seq = +doc.seq || +doc.setSeq || 0;
      return { id: str(doc.id || doc.setId || r.k || `set-${i + 1}`), name: str(doc.name || `Set ${seq || i + 1}`), seq: seq || i + 1, ids, doc };
    });
  }
  /** The sheet as the shared-orders core reads it: its pieces from poolIds, pieces, orders or a page's placed charms. */
  function toShared(rec, label, setId) {
    const { S } = needs();
    if (Array.isArray(rec.pieces) && rec.pieces.length && !(rec.poolIds && rec.poolIds.length)) {
      const pieces = rec.pieces.map(p => typeof p === 'string' ? { key: p, orderId: S.orderOfKey(p) } : { key: str(p && p.key), orderId: str(p && (p.orderId || S.orderOfKey(p.key))), ...(p && (p.side === 'L' || p.side === 'R') ? { side: p.side } : {}) }).filter(p => p.key && p.orderId);
      return { id: rec.id, label, metal: rec.metal || '', setId, runId: rec.runId || null, fixed: '', pieces };
    }
    let poolIds = rec.poolIds;
    if (!(poolIds && poolIds.length) && Array.isArray(rec.charms) && Array.isArray(rec.placements)) {
      const placed = new Set(rec.placements.map(p => p.id)); poolIds = rec.charms.filter(c => placed.has(c.id) && c.poolId).map(c => c.poolId);
    }
    return S.sheetOf(Object.assign({}, rec, { poolIds: poolIds || rec.poolIds }), { label, setId });
  }
  /** "GF Sheet 3" in a set, "GF Draft 5" outside (charm-nest-sheet-name.js: the one name rule), whatever stale fields the record carries. */
  function nameOf(rec, setId) {
    const N = SN(), R = SR();
    const view = setId ? Object.assign({}, rec, { draft: false, setId, solidIncluded: true }) : Object.assign({}, rec, { draft: true });
    if (N) return N.name(view);
    return `${R.metalClass(rec) || 'A'} ${setId ? 'Sheet' : 'Draft'} ${(setId ? rec.sheetIndex : 0) || rec.page || 1}`;
  }

  /** The gaps of a Rose Gold / 10K / 14K sheet that is in a set (or about to be): [] when none. R12: (a) its green line is cut, or the person said yes to the Cut Sheet press, and (b) its back engraving is approved. */
  function goldGaps(rec) {
    const cls = SR().metalClass(rec), gaps = [];
    if (!GOLD_FAMILY.includes(cls)) return gaps;
    if (!(isCut(rec) || rec.cutApproved === true || rec.roseFull === true)) gaps.push('its green line is not cut (the Cut Sheet press is not approved either)');
    // (Paul, 26 Oct 2026: a CUT sheet's back engraving is the set's approval to wait for (the Library holds Approve for laser cutting until every back is approved), not a rule the set breaks:
    //  the automatic cut joins it to its set before its backs are approved, and it is not held twice)
    if (isCut(rec)) return gaps;
    if (rec.backsApproved === false) gaps.push('its back engraving is not approved');
    else if (rec.backsApproved !== true) {
      const E = SE();
      if (E && typeof E.engravingGap === 'function') {
        const K = CUTS(), cuts = m => K && typeof K.cuts === 'function' ? K.cuts(m) : GOLD_FAMILY.includes(SR().metalClass({ metal: m }));
        let g = null; try { g = E.engravingGap(Object.assign({}, rec, { setId: null, draft: true, laserHold: null }), { cuts }); } catch (_) { g = null; }
        if (g) gaps.push(`its back engraving is not approved${g.label ? ` (${g.label.replace(/^[^:]*: /, '')})` : ''}`);
      }
    }
    return gaps;
  }

  /* ── the whole read ───────────────────────────────────────────────────────────────────────────────────────────────────────────────── */
  function context(setsIn, sheetsIn, opts) {
    const { R, S } = needs();
    const recs = readSheets(sheetsIn), setList = readSets(setsIn), setOf = new Map(), twice = [];
    for (const st of setList) for (const id of st.ids) { if (setOf.has(id) && setOf.get(id) !== st.id) twice.push({ id, sets: [setOf.get(id), st.id] }); else if (!setOf.has(id)) setOf.set(id, st.id); }
    const norm = [], nameById = new Map();
    for (const rec of recs.values()) {
      const inSet = setOf.get(rec.id) || null, label = nameOf(rec, inSet);
      nameById.set(rec.id, label);
      if (rec.archived) continue;
      const n = toShared(rec, label, inSet); if (n) norm.push(n);
    }
    const ends = [...recs.values()].filter(r => !r.archived && ['GF', 'SS'].includes(R.metalClass(r)) && R.isPartial(r) && !setOf.has(r.id)).map(r => r.id);
    /* SETFIX (Paul, 19 Oct 2026: "the first set is always correct, but all subsequent sets ... jamming all of the available sheets into a single set"). The groups tied sheets THROUGH a sheet that can
       never be in the set being judged, so the app's Set 2 made of units that each shared one order with a sheet of the committed Set 1 (or with an uncut Rose Gold sheet) read as ONE unit here, and
       this check said ok. The same anchors as the page's assembly (charm-nest-shared-orders.js formation): they are attached to the groups they share an order with but never tie two sheets together.
         anchors          a sheet in a set committed / completed / replaced (SetRules.exempt), and a sheet outside every set that the laser cut or that is a Rose Gold / 10K / 14K sheet that cannot join yet
         unitAnchors(st)  those, and every sheet in ANOTHER set: the units a set's minimality (R7) is judged by */
    const exemptSet = new Set(setList.filter(st => R.exempt(st.doc)).map(st => st.id));
    const anchors = [...recs.values()].filter(r => !r.archived && (setOf.has(r.id) ? exemptSet.has(setOf.get(r.id)) : (+r.laserDoneAt > 0 || (GOLD_FAMILY.includes(R.metalClass(r)) && goldGaps(r).length > 0)))).map(r => r.id);
    const formation = anc => typeof S.formation === 'function' ? S.formation(norm, ends, R.PARTIAL_BRIDGE, anc) : { groups: S.groups(norm), bridges: [] };
    const F = formation(anchors);
    const groups = F.groups, bridges = F.bridges, groupOfId = new Map(), normById = new Map(norm.map(n => [n.id, n])), anchorSet = new Set(anchors);
    for (const g of groups) for (const id of g.ids) if (!groupOfId.has(id)) groupOfId.set(id, g);
    const unitGroups = st => formation(anchors.concat([...setOf].filter(([, sid]) => sid !== st.id).map(([id]) => id))).groups;
    return { R, S, opts: opts || {}, recs, setList, setOf, twice, norm, normById, groups, bridges, groupOfId, anchorSet, unitGroups, name: id => nameById.get(id) || id };
  }

  function evaluate(setsIn, sheetsIn, opts) {
    const C = context(setsIn, sheetsIn, opts), { R, S, recs, setList, setOf, groups, bridges, groupOfId } = C, o = C.opts;
    const violations = [], partners = [], warnings = [], notes = [], summary = [], seen = new Set();
    const add = (rule, st, sheets, message, extra) => {
      const id = `${rule}:${st ? st.id : '-'}:${sheets.join('+')}`; if (seen.has(id)) return; seen.add(id);
      violations.push(Object.assign({ rule, ruleNo: RULE_NO[rule], ...(LETTER[rule] ? { letter: LETTER[rule] } : {}), id, set: st ? st.id : null, setName: st ? st.name : '', sheets, names: sheets.map(C.name), message }, extra || {}));
    };
    const legacy = new Set(list(o.legacySets).map(str)), short = new Set(list(o.allowShort).map(str)), waitingIds = new Set(list(o.waiting).map(str));      // (waiting: completed sheets that cannot join yet, still saving: they tie, they are not due in a set)

    for (const st of setList) {
      const exempt = R.exempt(st.doc), members = st.ids.map(i => recs.get(i)).filter(Boolean), live = members.filter(r => !r.archived);
      const row = { id: st.id, name: st.name, sheets: st.ids.slice(), names: st.ids.map(C.name), exempt, valid: R.validSet(live).ok, units: [], formedAt: 0, partners: 0 };
      summary.push(row);
      if (exempt && o.skipExempt) { notes.push(`${st.name} was committed, completed or replaced under older rules: not checked (skipExempt)`); continue; }

      // ── bookkeeping: the set lists a sheet that is missing, archived, a draft, or says it is in another set ──
      for (const id of st.ids) {
        const r = recs.get(id);
        if (!r) { add('membership', st, [id], `${st.name} lists a sheet that does not exist (${id})`); continue; }
        if (r.archived) add('membership', st, [id], `${st.name} lists ${C.name(id)}, which was taken out of the Library (archived)`);
        else if (o.membership !== false) {
          if (r.draft === true) add('membership', st, [id], `${st.name} lists ${C.name(id)}, but the sheet says it is a draft (in no set)`);
          else if (o.membership === 'strict' && r.setId && str(r.setId) !== st.id) add('membership', st, [id], `${st.name} lists ${C.name(id)}, but the sheet says it is in set ${r.setId}`);
        }
      }
      for (const t of C.twice.filter(t => t.sets.includes(st.id) && st.ids.includes(t.id))) add('membership', st, [t.id], `${C.name(t.id)} is listed in two sets (${t.sets.join(' and ')}): a sheet is in exactly one set`);

      // ── (a) R1: a completed GF sheet and a completed SS sheet ──
      const v = R.validSet(live);
      if (!v.ok && !short.has(st.id)) add('invalid', st, live.map(r => r.id), `${st.name} is not a valid set: it needs at least 1 completed GF sheet and 1 completed SS sheet, and it has no completed ${v.missing.join(' sheet and no completed ')} sheet (it holds ${v.have.GF} completed GF and ${v.have.SS} completed SS)`, { missing: v.missing, have: v.have });

      // ── (c) R3: a partial GF or SS sheet is in the set ──
      for (const r of live) if (R.isPartial(r)) add('partial', st, [r.id], `${C.name(r.id)} is in ${st.name} but is not full yet${R.fillWords(r)}: a partial sheet never joins a set`);

      // ── (d) R12: a Rose Gold / 10K / 14K sheet in the set without its green line cut (or approved) or its back engraving approved ──
      for (const r of live) {
        const gaps = goldGaps(r); if (!gaps.length) continue;
        const tied = [...new Set(live.filter(m => m !== r && (groupOfId.get(r.id) || { ids: [] }).ids.includes(m.id)).map(m => m.id))], partnerOf = tied.map(C.name);
        add('notReady', st, [r.id], `${C.name(r.id)} is in ${st.name} but ${and(gaps)}: a ${WORD[R.metalClass(r)]} sheet joins a set only once its green line is cut (or the Cut Sheet press is approved) and its back engraving is approved. It belongs outside the set, named as a partner to add${partnerOf.length ? ` (it shares orders with ${and(partnerOf)})` : ''}`, { reasons: gaps, tied });   // (tied: the sheets of the set it shares orders with; charm-nest-set-repair.js remedyOf reads it)
      }

      // ── (b) R7: a unit the set does not need (SetRules.surplus over SharedOrders units; SETFIX 19 Oct 2026: units never tie through a sheet of another set, a cut one or an uncut gold-family one) ──
      const ug = C.unitGroups(st), units = R.unitsOf(live, ug), spare = new Set(R.surplus(live, ug).map(r => r.id));
      row.units = units.map(u => u.sheets.map(r => r.id));
      const keep = units.filter(u => !u.sheets.every(r => spare.has(r.id)));
      for (const u of units) {
        if (!u.sheets.length || !u.sheets.every(r => spare.has(r.id))) continue;
        const ids = u.sheets.slice().sort(byKind).map(r => r.id), names = ids.map(C.name), tied = [...new Set(ug.filter(g => g.ids.some(i => ids.includes(i))).flatMap(g => g.orders))].filter(oid => ids.filter(i => { const n = C.normById.get(i); return n && n.pieces.some(p => p.orderId === oid); }).length > 1);
        const keeps = and(keep.flatMap(k => k.sheets).sort(byKind).map(r => C.name(r.id)));
        add('surplus', st, ids, ids.length === 1
          ? `${names[0]} shares no order with the others and ${st.name} is valid without it${keeps ? ` (${keeps} are enough)` : ''}`
          : `${and(names)} are tied to each other by ${orderWords(tied)} but share no order with the rest of ${st.name}, and ${st.name} is valid without them${keeps ? ` (${keeps} are enough)` : ''}`,
        { unit: ids, orders: tied, keeps: keep.flatMap(k => k.sheets.map(r => r.id)) });
      }

      // ── (e) R9 / R10 / R11: the sheets outside this set that share an order with it ──
      const inside = new Set(st.ids), pend = o.partialPartners === 'pending' || legacy.has(st.id);
      for (const g of groups.filter(g => g.ids.some(i => inside.has(i)))) {
        const mine = g.ids.filter(i => inside.has(i)), outside = g.ids.filter(i => !inside.has(i) && !setOf.has(i));
        for (const id of outside) {
          const r = recs.get(id); if (!r || r.archived || +r.laserDoneAt > 0 || waitingIds.has(id)) continue;      // (SETFIX 19 Oct 2026: a sheet the laser cut moves to no set: the order is split for good, like an order mate in a committed set)
          const cls = R.metalClass(r), n = C.normById.get(id), because = g.orders.filter(oid => n && n.pieces.some(p => p.orderId === oid)), withNames = mine.map(C.name);
          if (cls === 'GF' || cls === 'SS') {
            if (R.isPartial(r)) {
              if (!partners.some(x => x.set === st.id && x.sheet === id && x.kind === 'partial')) { partners.push({ set: st.id, setName: st.name, sheet: id, name: C.name(id), kind: 'partial', because, with: withNames, reasons: [`it is not full yet${R.fillWords(r)}: it joins ${st.name} once it is full`] }); row.partners++; }
              if (!pend) add('partialPartner', st, mine.concat(id), `${C.name(id)} is not full yet${R.fillWords(r)} and shares ${orderWords(because)} with ${and(withNames)}: the sheets that share an order with a partial sheet wait outside any set until it is full, so ${st.name} should not have been formed with them`, { partial: id, orders: because });
            } else add('cardinal', st, mine.concat(id), `${C.name(id)} is not in any set but shares ${orderWords(because)} with ${and(withNames)} in ${st.name}: sheets that share a multi-piece order must be in the same set (it is completed, so it joins ${st.name})`, { outside: id, orders: because });
          } else if (GOLD_FAMILY.includes(cls)) {
            const gaps = goldGaps(r);
            if (gaps.length) { if (!partners.some(x => x.set === st.id && x.sheet === id && x.kind === 'notReady')) { partners.push({ set: st.id, setName: st.name, sheet: id, name: C.name(id), kind: 'notReady', because, with: withNames, reasons: gaps }); row.partners++; } }
            else add('cardinal', st, mine.concat(id), `${C.name(id)} is not in any set but shares ${orderWords(because)} with ${and(withNames)} in ${st.name}, and nothing stops it joining (its green line is cut and its back engraving is approved): add it to ${st.name}`, { outside: id, orders: because });
          }
        }
      }

      // ── SetRules.PARTIAL_BRIDGE 'spread': a partial sheet with orders on several groups holds none of them; with this set's sheets among them it is named, not a fault ──
      for (const b of bridges) {
        const mine = b.groups.filter(ids => ids.some(i => inside.has(i))).flat().filter(i => inside.has(i)); if (!mine.length) continue;
        partners.push({ set: st.id, setName: st.name, sheet: b.id, name: C.name(b.id), kind: 'bridge', because: b.orders, with: mine.map(C.name), reasons: [`it is not full yet${R.fillWords(recs.get(b.id))} and has orders on sheets of ${b.groups.length} different groups: take those orders off a sheet so that no order is spread over two sets`] }); row.partners++;
      }

      // ── (f) R6: a sheet joined after the set had formed ──
      const times = live.map(r => ({ r, t: +r.joinedAt || 0 })).filter(x => x.t > 0);
      let formedAt = +st.doc.formedAt > 0 ? +st.doc.formedAt : 0;
      if (!formedAt && times.length) { const asc = times.slice().sort((a, b) => a.t - b.t); for (const t of [...new Set(asc.map(x => x.t))]) if (R.validSet(asc.filter(x => x.t <= t).map(x => x.r)).ok) { formedAt = t; break; } }
      row.formedAt = formedAt;
      if (formedAt && v.ok) {
        const lateGroups = new Map();
        for (const x of times.filter(x => x.t > formedAt)) {
          const r = x.r, cls = R.metalClass(r);
          if (GOLD_FAMILY.includes(cls) || r.joinedBy === 'person' || r.byHand === true) continue;      // gold-family sheets join by their own press; a person's drag is allowed
          const g = groupOfId.get(r.id), early = g && g.ids.some(i => { const m = recs.get(i); return m && inside.has(i) && m !== r && !(+m.joinedAt > formedAt); });
          if (early) continue;                                                                           // the cardinal rule pulled it in with a sheet that was there at formation
          const key = g ? g.ids.join('|') : r.id; if (!lateGroups.has(key)) lateGroups.set(key, []); lateGroups.get(key).push(r.id);
        }
        for (const ids of lateGroups.values()) add('closedJoin', st, ids, `${and(ids.map(C.name))} joined ${st.name} after it had formed (it held a completed GF and a completed SS sheet already, so it is closed to automatic joining) and no shared order, no gold-family rule and no person explains it`, { formedAt });
      }

      // ── R14: one number per metal in a set ──
      const N = SN(), byNo = new Map();
      for (const r of live) { const cls = R.metalClass(r), n = N ? N.numberOf(r) : +r.sheetIndex || 0; if (!n) continue; const k = `${cls}:${n}`; if (!byNo.has(k)) byNo.set(k, []); byNo.get(k).push(r.id); }
      for (const [k, ids] of byNo) if (ids.length > 1) add('numbering', st, ids, `${and(ids.map(C.name))} carry the same number (${k.replace(':', ' Sheet ')}) in ${st.name}: two sheets of one metal never share a number in a set`);
    }

    // ── (e) R9: a group spread over two sets ──
    // (SETFIX, Paul 19 Oct 2026: read per ORDER: an order whose pieces are on sheets of two sets. A group whose sets are tied only through a sheet outside every set has no such order: that sheet is
    //  the fault, and it is named as the completed sheet that is in no set ('cardinal' above), never as "orders on sheets of different sets", which those orders are not)
    const idxR9 = S.index(C.norm);
    for (const g of groups) {
      const crossing = g.orders.filter(oid => { const m = idxR9.get(oid); return !!m && new Set([...m.keys()].map(i => setOf.get(i)).filter(Boolean)).size > 1; });
      if (!crossing.length) continue;
      const on = [...new Set(crossing.flatMap(oid => [...idxR9.get(oid).keys()]))].filter(i => setOf.has(i)), sets = [...new Set(on.map(i => setOf.get(i)))];
      const st = setList.find(s => s.id === sets[0]), by = sets.map(sid => `${(setList.find(s => s.id === sid) || {}).name || sid}: ${and(on.filter(i => setOf.get(i) === sid).map(C.name))}`);
      if (o.skipExempt && setList.filter(s => sets.includes(s.id)).some(s => R.exempt(s.doc))) continue;      // (a mate in a set sent to the station, completed or replaced is fixed: the open set is not at fault, the work goes on)
      add('cardinal', st, on, `${cap(orderWords(crossing))} ${crossing.length === 1 ? 'has' : 'have'} pieces on sheets of different sets (${by.join('; ')}): sheets that share a multi-piece order must always be in the same set`, { sets, orders: crossing });
    }

    // ── R5: completed sheets that wait outside every set although they make a valid set ("as soon as the minimum criteria is met ... the set should be formed") ──
    if (o.unformed !== false) {
      const near = new Set(), heldIds = new Set();
      for (const g of groups) {
        if (g.ids.some(i => setOf.has(i) && !C.anchorSet.has(i))) g.ids.forEach(i => near.add(i));          // it belongs with the set its partner is in (R9), not to a new set (SETFIX 19 Oct: a committed set is no partner's place)
        else if (!R.groupWait(g.ids.map(i => recs.get(i)).filter(Boolean), () => false).ok) g.ids.forEach(i => heldIds.add(i));   // a group with a partial sheet waits whole (R11)
      }
      const free = [...recs.values()].filter(r => !r.archived && !waitingIds.has(r.id) && !setOf.has(r.id) && !near.has(r.id) && !heldIds.has(r.id) && ['GF', 'SS'].includes(R.metalClass(r)) && R.isCompleted(r));
      for (const l of R.formAll([], free, false, '', groups).sets) {
        const sids = l.map(r => r.id);
        add('unformed', null, sids, `${and(sids.map(C.name))} ${sids.length === 1 ? 'is' : 'are'} completed, in no set, and make a valid set (a completed GF and a completed SS sheet): a set forms the moment its minimum is met`, { unit: sids });
      }
    }

    // ── R16 (warning): a multi-piece order spread over two sheets of one colour (the nesting fault behind the chains) ──
    const idx = S.index(C.norm);
    for (const [oid, m] of idx) {
      if (!S.spans(m)) continue;
      const byCls = new Map(); for (const sid of m.keys()) { const cls = R.metalClass(C.recs.get(sid)); if (!byCls.has(cls)) byCls.set(cls, []); byCls.get(cls).push(sid); }
      for (const [cls, ids] of byCls) if (ids.length > 1) warnings.push({ rule: 'orderSplit', ruleNo: RULE_NO.orderSplit, id: `orderSplit:${oid}:${cls}`, set: setOf.get(ids[0]) || null, sheets: ids, names: ids.map(C.name), message: `Order ${oid} has pieces on ${and(ids.map(C.name))}: a multi-piece order stays on ONE sheet of its colour, otherwise same-colour sheets chain into one unit` });
    }

    for (const row of summary) row.canApprove = !violations.some(x => x.set === row.id) && !partners.some(p => p.set === row.id);
    return { ok: violations.length === 0, violations, partners, warnings, sets: summary, notes };
  }

  /** The formation the rules make from these sheets, every one taken as free (membership, draft and setId are not read):
   *  completed GF / SS sheets only, a group with a partial GF/SS sheet held whole (groupWait), the real formAll over the real units. */
  function expected(sheetsIn, opts) {
    const C = context([], sheetsIn, opts), { R } = C, held = new Map(), heldGroups = [];
    for (const g of C.groups) {
      const w = R.groupWait(g.ids.map(i => C.recs.get(i)).filter(Boolean), () => false);
      if (!w.ok) { heldGroups.push({ ids: g.ids.slice(), line: w.line, partial: w.partial.map(p => p.id) }); for (const i of g.ids) held.set(i, w.line); }
    }
    const free = [...C.recs.values()].filter(r => !r.archived && ['GF', 'SS'].includes(R.metalClass(r)) && R.isCompleted(r) && !held.has(r.id));
    const f = R.formAll([], free, false, '', C.groups), first = +(opts && opts.firstSeq) || 1;
    const sets = f.sets.map((l, k) => ({ id: `set-${first + k}`, name: `Set ${first + k}`, sheetIds: l.map(r => r.id).sort((a, b) => { const x = C.recs.get(a), y = C.recs.get(b); return (R.metalClass(x) === R.metalClass(y) ? 0 : R.metalClass(x) === 'GF' ? -1 : 1) || ((+x.page || +x.sheetIndex || 0) - (+y.page || +y.sheetIndex || 0)); }) }));
    // a Rose Gold / 10K / 14K sheet whose green line is cut (or approved) and whose back engraving is approved goes with the unit it shares an order with (R9 + R12); every other one is a partner to add
    const homeOf = new Map(); sets.forEach(s => s.sheetIds.forEach(id => homeOf.set(id, s)));
    for (const g of C.groups) {
      const homes = [...new Set(g.ids.map(i => homeOf.get(i)).filter(Boolean))]; if (homes.length !== 1) continue;
      for (const id of g.ids) { const r = C.recs.get(id); if (r && !r.archived && !homeOf.has(id) && GOLD_FAMILY.includes(R.metalClass(r)) && !goldGaps(r).length) { homes[0].sheetIds.push(id); homeOf.set(id, homes[0]); } }
    }
    for (const s of sets) s.names = s.sheetIds.map(id => nameOf(C.recs.get(id), s.id));
    const why = new Map(f.wait.map(w => [w.sheet.id, w.why]));
    for (const [id, line] of held) if (!why.has(id)) why.set(id, line);
    for (const r of C.recs.values()) {
      if (r.archived || homeOf.has(r.id) || why.has(r.id)) continue;
      const gaps = goldGaps(r);
      why.set(r.id, R.isPartial(r) ? R.partialWhy(r) : gaps.length ? `a ${WORD[R.metalClass(r)]} sheet joins a set by its own press: ${gaps.join(' and ')}` : 'it shares no order with a set that has formed: a person adds it (Include / Cut Sheet)');
    }
    const waiting = [...why].map(([id, w]) => ({ id, name: C.name(id), ...(C.recs.get(id).sheetIndex ? { was: nameOf(C.recs.get(id), 'was') } : {}), why: w }));
    return { sets, waiting, held: heldGroups, bridges: C.bridges.map(b => ({ id: b.id, name: C.name(b.id), groups: b.groups, orders: b.orders })), partners: evaluate(sets, sheetsIn, opts).partners };
  }

  /** One plain line per violation, partner and warning (a log, a failing test). */
  function explain(result) {
    const out = [];
    for (const v of result.violations) out.push(`[${v.ruleNo}${v.letter ? ' (' + v.letter + ')' : ''} ${v.rule}] ${v.message}`);
    for (const p of result.partners) out.push(p.kind === 'bridge' ? `[R11 bridge] ${p.name} (${p.reasons.join('; ')}) shares ${orderWords(p.because)} with ${and(p.with)} in ${p.setName}` : `[R10 partner] ${p.setName}: add ${p.name} (${p.reasons.join('; ')}), it shares ${orderWords(p.because)} with ${and(p.with)}`);
    for (const w of result.warnings) out.push(`[${w.ruleNo} ${w.rule}] ${w.message}`);
    return out;
  }

  const RULES = Object.freeze(Object.assign({}, RULE_NO));
  return { check: evaluate, expected, explain, goldGaps, RULES, LETTER, GOLD_FAMILY };
});
