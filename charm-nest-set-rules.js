/* SetRules: when a Set of Sheets may exist (Paul, 10 Oct 2026, Library).
 *
 *   "In order for a Set of Sheets to be allowed to exist there must be at minimum 1 Completed GF Sheet and
 *    1 Completed SS Sheet."
 *
 * THE DEFINITION OF A COMPLETED SHEET (SETFORM, from the code's own states; the reason is in SETFORM-findings.md)
 *
 *   A sheet is COMPLETED when nothing more will be put on it, because it was filled, not because a set was made around it:
 *     · releaseFull is true: the one release mark the sorter writes when a sheet is full. It is set by sheetFull() (the fill
 *       ceiling is reached, or an order that fits an empty sheet was turned away by a search that finished), by topupSettle()
 *       (Gold and Silver: 75% reached, 35 later orders tried, no room for the smallest charm, or the run stopped bringing
 *       orders), by the sandbox / run stream ending ("every order of the stream is in"), or by a person's Make QR label.
 *       charm-nest-orders.js sheetRelease() reads exactly this mark: a Gold or Silver sheet makes a set only when it is "Full sheet";
 *     · or the laser has cut it (laserDoneAt), or its Rose Gold cut is recorded (roseCutAt): it can never take anything again.
 *   A sheet is NOT completed when it is still filling (releaseFull false), however it is shown:
 *     · its nest status "complete" means only that every piece given to it was placed; "partial" means the opposite (some pieces
 *       moved on to the next sheet, so the sheet IS full): do not read the nest status as Paul's word;
 *     · being in a set does not complete it (a sheet the cardinal rule pulled in with a partner, cardinalPull, is closed to new
 *       pieces but is not completed: it stays at the fill it had);
 *     · a draft, an archived sheet, a sheet with nothing placed.
 *
 *   A SET is valid when its sheets hold at least 1 completed GF sheet AND at least 1 completed SS sheet. Other sheets (partial
 *   GF or SS, RG, 10K, 14K) may be in a valid set; they never count towards it and never stand in for a completed one.
 *
 * FORMATION AND CLOSING (Paul, 12 Oct 2026, replacing the older "a set takes every full sheet until it is approved"):
 *   "as soon as the minimum criteria is met, and none of the other stipulations are conflicting, the set should be formed and as a result all following
 *    sheets should go into a following new set etc."
 *   · A set FORMS the moment the sheets that can be in it hold at least 1 completed GF and 1 completed SS sheet (and the cardinal rule and the safety rules
 *     do not forbid it). The completed sheets present at that moment form it; partial sheets never start a set by themselves and wait.
 *   · A set that has formed is CLOSED to automatic joining (closed(), the ONE place that says so; CLOSING.afterFormation flips it): the completed sheets that
 *     come after it form the NEXT set (Set 2, Set 3, ...) when THEIR own minimum is met. A person can still drag any sheet in or out of any set, and the sheets
 *     that share a multi-piece order with a sheet of a closed set still join THAT set (the cardinal rule outranks the closing).
 *   · Back engraving is NOT a criterion for any of this (it only gates approving a set for laser cutting).
 *   · A sheet that shares an order with a sheet that cannot join yet (Rose Gold before its Cut Sheet press, a sheet still being saved) no longer holds the group
 *     out: the set forms with the sheets that can be in it, says plainly which partner sheet to add (cardinalWait, derived by the page, never saved), and cannot be
 *     approved for laser cutting until that sheet is in it (gate()).
 *
 * ONE SET AT A TIME, EACH AS SMALL AS THE MINIMUM ALLOWS (Paul, 15 Oct 2026, SETRULE):
 *   "a set of sheets requires at least one GF and one SS sheet at the minimum and when this threshold is met then that becomes an individual Set of sheets ... how is it that this one
 *    set has 4 GF and 2 SS sheets. This should be literally impossible"
 *   Several completed sheets can be free at the same moment (a stream's end marks every sheet released at once, a page that reloads, a slow cloud, GF sheets that waited for the
 *   first completed SS sheet). They do NOT all go into one set. formAll() forms the sets one after the other, in a fixed order (the time a sheet was seen completed, releasedAt, then its
 *   sheet number, then GF before SS, then its id):
 *     · the UNITS are the sheets that share a multi-piece order (the cardinal rule, shareGroups): a unit is indivisible, it goes into one set whole;
 *     · a new set takes the earliest unit, then only as many further units, earliest first and each one that supplies the class the set still lacks, as it needs to hold 1 completed GF
 *       and 1 completed SS sheet (a unit that turns out not to be needed, because a later one holds both by itself, waits for its own set). The moment it holds both it is formed and
 *       closed, and the next set starts from the units that are left;
 *     · units left over that cannot make a valid set wait outside every set with their plain line ("Set 1 has formed, so this sheet waits for the next set: it needs a completed SS sheet with it");
 *     · a set that exists but is short of a class (an empty or half-made one) is completed first, with the fewest units that complete it, before any new set starts;
 *     · a set that already exists is never touched (by the assembly: a set that breaks the owner's rules is put right only by the person's own press, "Rebuild this set correctly",
 *       charm-nest-set-repair.js, SETREPAIR 17 Oct). 10K, 14K and Rose Gold sheets never start or complete a set, a partial sheet never joins one.
 *   Sheets that share an order stay one unit however many sheets that is: a day whose cross-metal orders (GF with SS, or either with Rose Gold, 14K, 10K) link every sheet is ONE unit and so one
 *   set. That is the cardinal rule outranking the closing rule, never a formation fault.
 *   formAll(lastMembers, free, hasLast, lastName, shareGroups) -> { last:[sheet], sets:[[sheet]], wait:[{sheet, why}], valid, closed }  `last`: the sheets that complete the newest open set;
 *                                                  `sets`: the new sets, in order; shareGroups: [[sheet or id]] or [{ids}] (SharedOrders groups)
 *   form(...) is the first step of it: { where, join, wait, later:[[sheet]], valid, closed }, `later` the sets that follow once this one is made (the page asks again after each).
 *
 * PARTIAL SHEETS NEVER JOIN A SET (Paul, 13 Oct 2026, SETPART):
 *   "A partial GF and SS sheet should never be part of a Set of sheets. That sheet should have been held back because it was not fully nested."
 *   · "Fully nested" is the app's own notion above: a GF or SS sheet is COMPLETED (releaseFull, or laser cut) or it is PARTIAL (isPartial: still filling).
 *     A partial sheet joins no set by any route: not the minimum rule, not the cardinal rule, not automatic assembly, not a person's drop. It stays a loose draft
 *     ("GF Draft n"), never deleted. A set forms only from completed sheets (at least 1 completed GF + 1 completed SS); the next completed sheets form the next sets.
 *   · The cardinal rule (sheets that share an order stay in one set) is read with it as ONE GROUP: the sheets that share orders are a group; the group is eligible
 *     for a set only when no GF or SS sheet of it is partial (groupWait). A partial sheet that shares an order with completed sheets keeps the whole group waiting
 *     outside any set (nothing is split, nothing is dissolved) until the partial one is completed. When a sheet of the group is in a set already, the completed
 *     sheets of the group go on joining that set, the partial one waits outside it (the set says so and its Approve stays grey until it is full and in).
 *   · A set that already holds a partial sheet (made before this rule) is not touched, rebuilt or dissolved: it is only FLAGGED (gate().partial, never blocking).
 *     A person can still take such a sheet out of the set (the cardinal rule does not keep a partial sheet in a set).
 *   · The sheet window's Make QR label (Release as it stands) is the person declaring a sheet finished: it writes releaseFull, so the sheet is Completed by the
 *     definition above from then on. That is the only way a partial sheet becomes eligible; a drop never does it.
 *   · 10K, 14K and Rose Gold sheets are not GF/SS sheets: they keep their own two conditions (green line cut or approved, back engraving approved) and are never "partial".
 *   isPartial(sheet)           -> true for a GF or SS sheet that is not Completed (a sheet with no completion mark at all is still filling)
 *   recordPartial(sheet)       -> the same for a stored record or Library row: only when it carries the completion mark (releaseFull), else unknown = false
 *   partialLine(sheet)         -> "GF Draft 3 is not full yet (65% full)"            partialDetail: "A partial sheet never joins a set."
 *   groupWait(sheets, inSet)   -> { ok, partial:[sheet], line } the group rule: ok only when no GF/SS sheet outside a set is partial
 *
 * A PARTIAL SHEET THAT TOUCHES SEVERAL GROUPS (SETFIX, Paul 17 Oct 2026; docs/charm-nest/set-rules.md R11): a partial GF/SS sheet outside every set is an END node of the groups of sheets that share orders:
 *   it never connects two groups and never seeds the set they join (charm-nest-shared-orders.js formation). The groups it shares an order with wait outside any set, whole, until it is full.
 *   What happens when it is full and has orders on SEVERAL groups is the owner's open decision, so it is ONE switch, here:
 *     PARTIAL_BRIDGE = 'hold'    (the rule as written, DEFAULT) every group it touches waits whole until it is full, then (it is a sheet like any other) they are one group and one set
 *     PARTIAL_BRIDGE = 'spread'  a partial sheet that touches two or more groups holds none of them: each complete group forms its own minimal set now, the partial sheet and its mixed orders are
 *                                flagged (cardinalNote; the checker lists it as a partner, not a violation) and a person takes those orders off (SharedOrders.removeFromSheet)
 *   Read through SetRules.PARTIAL_BRIDGE by the page's assembly (Gate), the checker (charm-nest-set-invariants.js) and the tests; set it with SetRules.PARTIAL_BRIDGE = 'spread'.
 *
 * Pure: no cloud, no page state. window.CharmNestSetRules in pages; module.exports in node and netlify functions
 * (charmNestLibrary.js requires this file, like charm-nest-set-edit.js).
 *
 *   isCompleted(sheet)                          -> boolean
 *   metalClass(sheet)                           -> "GF" | "SS" | "RG" | "10K" | "14K" | "" (a plain code string is read too)
 *   validSet(sheets)                            -> { ok, missing:["GF"|"SS"], reason, have:{GF,SS} } (have: completed sheets counted)
 *   wouldStayValid(setSheets, movedOutIds, movedInSheets) -> same shape, for the set as it would be after the move
 *   whyNotCompleted(sheet)                      -> "" for a completed sheet, else one plain line saying what it still waits for
 *   describe(sheet)                             -> "GF Sheet 2" style label
 *
 * Added by SETFORM for set formation (page, server and Library all call these, so they cannot disagree):
 *   settle(members, wanting)                    -> { formed, join:[sheet], wait:[{sheet, why}], valid }  which of the sheets that want into a set
 *                                                  may join NOW: all of them when the set would then be valid, none otherwise (a set that does not
 *                                                  exist is not made; a set that exists and is short of a class takes only a batch that completes it)
 *   gate(set, sheets)                           -> { applies, ok, missing, reason, short, line, have } the principle read for an EXISTING set record
 *                                                  and its member sheets. `applies` is false (and ok true) for a set already committed to the Design
 *                                                  Station, completed or replaced (they were made under older rules and are never blocked
 *                                                  retroactively) and when the records carry no completion mark at all (releaseFull) to read.
 *   exempt(set)                                 -> true for those committed / completed / replaced sets
 *   closed(members)                             -> true when a set holding `members` has formed and takes no sheet on its own any more (THE closing rule)
 *   form(lastMembers, free, hasLast, lastName, shareGroups) -> { where:'last'|'new'|null, join:[sheet], wait:[{sheet, why}], later:[[sheet]], valid, closed }  where the free (completed, not in a set)
 *                                                  sheets go NOW: into the newest open set while it has not formed, into a NEW set when there is none or it is closed, nowhere while
 *                                                  they cannot make a valid set (they wait, each with the reason). SETRULE: join is the fewest units that make ONE valid set; `later` are the sets after it
 *   formAll(...)                                -> { last, sets, wait, valid, closed }  every set the free sheets make, one after the other (see ONE SET AT A TIME above)
 *   surplus(setSheets, shareGroups)             -> [sheet]  the sheets of a set that no order ties to its others and that it is still valid without (the Library says so on the set)
 *   partnerWords(labels)                        -> "orders shared with RG Draft 1: add that sheet to this set"
 *   enforce(false)                              -> for an offline suite about something else whose fixture sets predate the principle: gate() then reads applies:false
 */
(function (root, factory) { const api = factory(root); if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestSetRules = api; })(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
  const CODES = new Set(Object.values(CODE));
  const str = v => String(v == null ? '' : v);
  const idOf = s => str(s && (s.id || s.sheetId));

  /** "GF" | "SS" | other metal codes, as the sheet cards show them. Reads metal, then the card's own code (metalLabel "GF 14/20 · sheet 2", "SS", "14K Gold"). */
  function metalClass(sheet) {
    if (sheet == null) return '';
    if (typeof sheet === 'string') { const t = sheet.trim(); return CODE[t] || (CODES.has(t.toUpperCase()) ? t.toUpperCase() : ''); }
    if (CODE[sheet.metal]) return CODE[sheet.metal];
    const m = /^\s*(GF|SS|RG|10K|14K)\b/i.exec(str(sheet.metalLabel || sheet.code || sheet.metalCode));
    return m ? m[1].toUpperCase() : '';
  }
  const placedOf = s => s.placedCount != null ? +s.placedCount : Array.isArray(s.placements) ? s.placements.length : null;

  /** The definition above. */
  function isCompleted(sheet) {
    if (!sheet || typeof sheet !== 'object' || sheet.archived) return false;
    if (+sheet.laserDoneAt > 0 || sheet.roseCutAt) return true;                  // cut: nothing can go on it again
    if (!(sheet.releaseFull === true || sheet.releaseFull === 1 || sheet.releaseFull === 'true')) return false;   // still filling (whatever its nest status says)
    const n = placedOf(sheet);
    return n === null || n > 0;                                                  // a release with nothing placed is no sheet
  }
  // (SETNUMBER) a sheet's name is decided in one place, charm-nest-sheet-name.js: "GF Sheet 3" in a set, "GF Draft 5" outside every set. The page in the run is never a number in a set.
  const nameFile = () => { try { return root.CharmNestSheetName || (typeof module === 'object' && typeof require === 'function' ? require('./charm-nest-sheet-name.js') : null); } catch (_) { return null; } };
  const describe = sheet => { const N = nameFile(); if (N && sheet && typeof sheet === 'object' && (sheet.metal || sheet.metalLabel)) return N.name(sheet); const c = metalClass(sheet); return `${c || 'A'} Sheet ${sheet && (sheet.sheetIndex || sheet.page) || 1}`.replace(/^A Sheet/, 'Sheet'); };

  /** One plain line: what a sheet that is not completed still waits for ("" when it is completed). */
  function whyNotCompleted(sheet) {
    if (isCompleted(sheet)) return '';
    if (!sheet) return 'There is no sheet.';
    if (sheet.archived) return 'It was taken out of the Library.';
    const n = placedOf(sheet);
    if (n === 0) return 'Nothing is placed on it.';
    const pct = Number.isFinite(+sheet.density) && +sheet.density > 0 ? ` (${Math.round((+sheet.density) * 100)}% full)` : '';
    return `${describe(sheet)} is still filling${pct}: it counts once it is full or released.`;
  }

  const countBy = sheets => { const have = { GF: 0, SS: 0 }; for (const s of sheets) { const c = metalClass(s); if ((c === 'GF' || c === 'SS') && isCompleted(s)) have[c]++; } return have; };
  const list = x => Array.isArray(x) ? x : x instanceof Set ? [...x] : x ? [x] : [];

  /** ok only when a completed GF and a completed SS sheet are both present. */
  function validSet(sheets) {
    const have = countBy(list(sheets).filter(Boolean)), missing = ['GF', 'SS'].filter(c => !have[c]);
    const reason = missing.length ? `A set needs at least 1 completed GF sheet and 1 completed SS sheet: it has no completed ${missing.join(' sheet and no completed ')} sheet.` : '';
    return { ok: !missing.length, missing, reason, have };
  }

  /** The set as it would be after sheets leave (by id) and sheets join (records): validSet of that. `after` is the resulting list. */
  function wouldStayValid(setSheets, movedOutIds, movedInSheets) {
    const out = new Set(list(movedOutIds).map(str)), inn = list(movedInSheets).filter(Boolean), inIds = new Set(inn.map(idOf));
    const after = list(setSheets).filter(s => s && !out.has(idOf(s)) && !inIds.has(idOf(s))).concat(inn);
    return Object.assign(validSet(after), { after });
  }

  /* ── PARTIAL SHEETS (Paul, 13 Oct 2026): see the head of this file ─────────────────────────────────────────────────────────────────────────── */
  const hasMark = sheet => !!sheet && typeof sheet === 'object' && Object.prototype.hasOwnProperty.call(sheet, 'releaseFull');
  /** A GF or SS sheet that is not Completed. A page's sheet with no releaseFull yet is still filling. */
  const isPartial = sheet => !!sheet && typeof sheet === 'object' && !sheet.archived && (metalClass(sheet) === 'GF' || metalClass(sheet) === 'SS') && !isCompleted(sheet);
  /** The same for a stored record or Library row: unknown (false) when the record carries no completion mark at all, as gate() and the plan already read it. */
  const recordPartial = sheet => isPartial(sheet) && hasMark(sheet);
  const fillWords = sheet => Number.isFinite(+(sheet && sheet.density)) && +sheet.density > 0 ? ` (${Math.round((+sheet.density) * 100)}% full)` : '';
  /** The one plain line for a partial sheet: "GF Draft 3 is not full yet (65% full)". */
  const partialLine = sheet => `${describe(sheet)} is not full yet${fillWords(sheet)}`;
  const partialDetail = 'A partial sheet never joins a set.';
  const partialWhy = sheet => `${partialLine(sheet)}: a partial sheet never joins a set`;
  /** The group rule: sheets that share an order are one group; it is eligible for a set only when no GF or SS sheet of it that is outside a set (inSet(sheet) says which are in one) is partial. */
  function groupWait(sheets, inSet) {
    const partial = list(sheets).filter(s => isPartial(s) && !(typeof inSet === 'function' && inSet(s)));
    const names = partial.map(describe), one = names.length === 1;
    return { ok: !partial.length, partial, line: partial.length ? `${names.join(' and ')} ${one ? 'is' : 'are'} not full yet, so the sheets that share an order with ${one ? 'it' : 'them'} wait outside any set until ${one ? 'it is' : 'they are'} full` : '' };
  }

  /** What a sheet waiting outside a set is told ("" when nothing is missing). */
  const waitWords = missing => `Waits for a completed ${missing.join(' sheet and a completed ')} sheet: a set needs at least 1 completed GF sheet and 1 completed SS sheet`;

  /** Which sheets that want into a set (members: those already in it) may join now. The set after the join must be valid, or nobody joins. */
  function settle(members, wanting) {
    const have = list(members).filter(Boolean), asked = list(wanting).filter(Boolean);
    // (Paul, 13 Oct: a partial GF or SS sheet never joins a set by any route: it waits, with its one line, and the others are settled without it)
    const want = asked.filter(w => !isPartial(w)), barred = asked.filter(isPartial).map(sheet => ({ sheet, why: partialWhy(sheet) }));
    const valid = validSet(have.concat(want.filter(w => !have.some(h => idOf(h) && idOf(h) === idOf(w)))));
    if (valid.ok || !want.length) return { formed: valid.ok || have.length > 0, join: want, wait: barred, valid };
    return { formed: have.length > 0, join: [], wait: want.map(sheet => ({ sheet, why: waitWords(valid.missing) })).concat(barred), valid };
  }

  /* ── THE CLOSING RULE (Paul, 12 Oct 2026) ───────────────────────────────────────────────────────────────────────────────────────────────────
     "as soon as the minimum criteria is met ... the set should be formed and as a result all following sheets should go into a following new set".
     READING (SETWHY): a set CLOSES to AUTOMATIC joining the moment it has formed, that is, as soon as it holds at least 1 completed GF and 1 completed SS sheet.
     The completed sheets that come after it do not join it on their own; they wait outside every set and form the next set (Set 2, then Set 3, ...) when their own
     minimum is met. Closed means closed to AUTOMATIC joining only: a person's drag still puts any sheet in or out, and a sheet that shares a multi-piece order
     with a sheet of the closed set still goes into THAT set (the cardinal rule is Paul's most important one and outranks the closing).
     To read it the other way (a set keeps taking every following completed sheet until a person approves it, as before) set CLOSING.afterFormation to false:
     this constant and closed() are the only two places that say it. */
  const CLOSING = { afterFormation: true };
  // (SETFIX, Paul 17 Oct 2026) the ONE switch for a partial sheet that shares orders with several complete groups: see the head of this file
  let partialBridge = 'hold';
  /** True when a set holding `members` has formed and takes no sheet on its own any more. An empty set, or one short of a completed GF or SS, is still forming. */
  const closed = members => CLOSING.afterFormation && validSet(list(members).filter(Boolean)).ok;

  /** The one plain line a set gives for partner sheets that cannot join yet: "orders shared with RG Draft 1: add that sheet to this set". */
  const partnerWords = (labels, partials) => {
    const l = list(labels).map(str).filter(Boolean), p = list(partials).map(str).filter(x => l.includes(x)), add = l.filter(x => !p.includes(x));
    if (!p.length) return `orders shared with ${l.join(' and ')}: add ${l.length > 1 ? 'those sheets' : 'that sheet'} to this set`;
    // (Paul, 13 Oct: a partner that is not full yet cannot be added; it joins by itself once it is full)
    const once = `once ${p.length > 1 ? 'they are' : 'it is'} full`;
    return add.length ? `orders shared with ${l.join(' and ')}: add ${add.join(' and ')} to this set; ${p.join(' and ')} ${p.length > 1 ? 'join' : 'joins'} it ${once}` : `orders shared with ${p.join(' and ')}, which ${p.length > 1 ? 'are' : 'is'} not full yet: ${p.length > 1 ? 'they join' : 'it joins'} this set ${once}`;
  };

  /** What a completed sheet that waits for the NEXT set is told when the newest set has formed already. */
  const nextSetWords = (setName, missing) => `${setName || 'The set'} has formed, so this sheet waits for the next set: ${waitWords(missing).replace(/^Waits for /, 'it needs ').replace(/: a set needs.*$/, '')} with it`;

  /* ── ONE SET AT A TIME (Paul, 15 Oct 2026, SETRULE): see the head of this file ─────────────────────────────────────────────────────────────── */
  /** The order sheets form sets in: when it was seen completed (releasedAt; unknown = before any known time), its sheet number, GF before SS, its id. Total and the same every time. */
  const orderKey = s => [+s.releasedAt > 0 ? +s.releasedAt : 0, +s.page || +s.sheetIndex || 0, metalClass(s) === 'GF' ? 0 : metalClass(s) === 'SS' ? 1 : 2, idOf(s)];
  const byOrder = (a, b) => { const x = orderKey(a), y = orderKey(b); for (let i = 0; i < 3; i++) if (x[i] !== y[i]) return x[i] - y[i]; return x[3] < y[3] ? -1 : x[3] > y[3] ? 1 : 0; };
  /** The free sheets as units: the sheets that share a multi-piece order (shareGroups: lists of sheets or ids, or { ids }) are ONE unit, the rest are alone. Earliest unit first. */
  function unitsOf(sheets, shareGroups) {
    const parent = sheets.map((_, i) => i), find = i => { while (parent[i] !== i) { parent[i] = parent[parent[i]]; i = parent[i]; } return i; };
    const at = new Map(); sheets.forEach((s, i) => { at.set(s, i); const id = idOf(s); if (id && !at.has('#' + id)) at.set('#' + id, i); });
    for (const g of list(shareGroups)) {
      // (SETFIX, Paul 19 Oct 2026) a group's anchors and ends (charm-nest-shared-orders.js formation) are attached to it but tie nothing: a free sheet among them is a unit of its own
      const skip = g && !Array.isArray(g) ? new Set(list(g.anchors).concat(list(g.ends)).map(m => (m && typeof m === 'object' ? idOf(m) : str(m)))) : null;
      const ix = list(g && !Array.isArray(g) && g.ids ? g.ids : g).filter(m => !skip || !skip.has(m && typeof m === 'object' ? idOf(m) : str(m))).map(m => (m && typeof m === 'object' ? at.get(m) : at.get('#' + str(m)))).filter(i => i != null);
      for (let k = 1; k < ix.length; k++) { const a = find(ix[0]), b = find(ix[k]); if (a !== b) parent[Math.max(a, b)] = Math.min(a, b); }
    }
    const by = new Map(); sheets.forEach((s, i) => { const r = find(i); if (!by.has(r)) by.set(r, []); by.get(r).push(s); });
    return [...by.values()].map(l => ({ sheets: l.sort(byOrder) })).sort((u, v) => byOrder(u.sheets[0], v.sheets[0]));
  }
  const supplies = (u, missing) => u.sheets.some(s => isCompleted(s) && missing.includes(metalClass(s)));
  /** The fewest units (earliest first, each one that supplies a class the set still lacks) that make `base` plus them hold a completed GF and a completed SS sheet; null when the units cannot.
      With no base sheet the earliest unit that holds a completed GF or SS sheet starts the set (a sheet of any other metal never starts one). */
  function fewest(base, units) {
    const chosen = []; let cur = list(base).filter(Boolean);
    if (!cur.length) { const first = units.find(u => supplies(u, ['GF', 'SS'])); if (!first) return null; chosen.push(first); cur = cur.concat(first.sheets); }
    for (;;) {
      const v = validSet(cur); if (v.ok) break;
      const next = units.find(u => !chosen.includes(u) && supplies(u, v.missing)); if (!next) return null;
      chosen.push(next); cur = cur.concat(next.sheets);
    }
    // a unit that turned out not to be needed (a later unit holds a completed GF and a completed SS sheet by itself) does not come along: the earliest such unit waits for its own set
    for (const u of chosen.slice()) { if (chosen.length > 1 && validSet(list(base).filter(Boolean).concat(chosen.filter(c => c !== u).flatMap(c => c.sheets))).ok) chosen.splice(chosen.indexOf(u), 1); }
    return chosen;
  }
  /** Every set the free sheets make, one after the other (see ONE SET AT A TIME at the head of this file). `last`: the sheets that complete the newest open set while it has not formed. */
  function formAll(lastMembers, free, hasLast, lastName, shareGroups) {
    const have = list(lastMembers).filter(Boolean), asked = list(free).filter(Boolean);
    // (Paul, 13 Oct: a partial GF or SS sheet never joins a set by any route: it waits, with its one line, and the others are settled without it)
    const barred = asked.filter(isPartial).map(sheet => ({ sheet, why: partialWhy(sheet) })), want = asked.filter(w => !isPartial(w));
    let pool = unitsOf(want, shareGroups), open = !!hasLast && !closed(have);
    const out = { last: [], sets: [], wait: [], valid: null, closed: !!hasLast && !open };
    const take = chosen => { pool = pool.filter(u => !chosen.includes(u)); return chosen.flatMap(u => u.sheets); };
    if (open) { const c = fewest(have, have.length ? pool.filter(u => !validSet(u.sheets).ok) : pool); if (c) { out.last = take(c); open = false; } else if (have.length && pool.some(u => validSet(u.sheets).ok)) open = false; }   // (audit B, 25 Oct 2026: a short set is completed only by units that are not a set by themselves; a whole valid unit made it bigger than the minimum, R7, and forms its own set)
    while (!open && pool.length) { const c = fewest([], pool); if (!c) break; out.sets.push(take(c)); }
    const first = out.last.length ? have.concat(out.last) : out.sets[0] || null;
    const rest = pool.flatMap(u => u.sheets), left = validSet((open ? have : []).concat(rest));
    out.valid = first ? validSet(first) : validSet(have.concat(want));
    // what is left cannot make a valid set (yet): it waits. Once a set has formed (or the newest one has), a sheet that waits is waiting for the NEXT set
    const next = open ? null : out.sets.length ? nextSetWords(null, left.missing) : hasLast || out.last.length ? nextSetWords(lastName, left.missing) : null;
    out.wait = rest.map(sheet => ({ sheet, why: next || waitWords(left.missing) })).concat(barred);
    return out;
  }
  /** The sheets a set holds that no order ties to its other sheets and that it could do without (SETRULE): it stays valid, a completed GF and a completed SS sheet, without them.
      The latest units leave first, so what is left is the smallest set the earliest sheets make. 10K, 14K and Rose Gold sheets are a person's choice and never count. [] for a minimal set. */
  function surplus(setSheets, shareGroups) {
    const units = unitsOf(list(setSheets).filter(Boolean), shareGroups); let keep = units.slice(); const out = [];
    for (const u of units.slice().reverse()) {
      if (!supplies(u, ['GF', 'SS'])) continue;
      const rest = keep.filter(k => k !== u);
      if (validSet(rest.flatMap(k => k.sheets)).ok) { keep = rest; out.push(...u.sheets); }
    }
    return out.sort(byOrder);
  }
  /** The first step of formAll, in the shape the page's assembly reads: where the free sheets go NOW, the fewest units that make ONE valid set (`later`: the sets that follow it). */
  function form(lastMembers, free, hasLast, lastName, shareGroups) {
    const f = formAll(lastMembers, free, hasLast, lastName, shareGroups), where = f.last.length ? 'last' : f.sets.length ? 'new' : null;
    return { where, join: f.last.length ? f.last : f.sets[0] || [], wait: f.wait, later: f.last.length ? f.sets : f.sets.slice(1), valid: f.valid, closed: where === 'last' ? false : f.closed };
  }

  /** A set committed to the Design Station, completed or replaced was made under the older rules: the principle never blocks it afterwards. */
  const exempt = set => !!set && (+set.committedAt > 0 || +set.laserDoneAt > 0 || /^complete|superseded/.test(str(set.status)));

  // The readiness read of the principle (gate) can be switched off by an OFFLINE SUITE whose subject is something else (approval mechanics, completion messages) and whose
  // fixture sets predate the principle. Nothing in the app calls this; a page, the server and every release run with it on.
  let enforcing = true;
  const enforce = on => { enforcing = on !== false; return enforcing; };

  /** The principle read for an existing set and its sheets (see the head of this file). */
  function gate(set, sheets) {
    const members = list(sheets).filter(s => s && !s.archived);
    // (Paul, 13 Oct) a set made before partial sheets were barred may hold one: it is FLAGGED here (labels, derived each time, never saved) and never blocked or touched by this
    const flag = members.filter(recordPartial).map(describe);
    const none = { applies: false, ok: true, missing: [], reason: '', short: '', line: '', have: { GF: 0, SS: 0 }, partial: flag };
    if (!enforcing || !members.length || exempt(set)) return none;
    // (Paul, 12 Oct) a set formed with the sheets that could join while a partner that shares its orders cannot yet (Rose Gold before its Cut Sheet press, a sheet still being
    //  saved): the page says which sheets (set.cardinalWait: derived from the sheets each time, never saved). Orders are never spread over two sets when a set advances.
    const partners = list(set && set.cardinalWait).map(w => str(w && (w.label || w))).filter(Boolean), notFull = list(set && set.cardinalWait).filter(w => w && w.partial).map(w => str(w.label || w));
    const partnerGate = have => { const w = partnerWords(partners, notFull); return { applies: true, ok: false, missing: [], reason: `This set has ${w}.`, short: w, line: `Waiting: ${w}, so that orders are not spread over two sets.`, have, partners, partial: flag }; };
    if (!members.some(s => Object.prototype.hasOwnProperty.call(s, 'releaseFull'))) return partners.length ? partnerGate(none.have) : none;
    const v = validSet(members);
    if (v.ok) return partners.length ? partnerGate(v.have) : Object.assign({}, none, { applies: true, have: v.have });
    const need = `a completed ${v.missing.join(' sheet and a completed ')} sheet`;
    return { applies: true, ok: false, missing: v.missing, reason: v.reason, short: `needs ${need}`,
      line: `Waiting for ${need}: a set needs at least 1 completed GF sheet and 1 completed SS sheet before it can go to laser cutting.`, have: v.have, partial: flag };
  }

  const api = { isCompleted, metalClass, validSet, wouldStayValid, whyNotCompleted, describe, settle, gate, exempt, enforce, waitWords, form, formAll, unitsOf, surplus, closed, partnerWords, nextSetWords, isPartial, recordPartial, partialLine, partialDetail, partialWhy, fillWords, groupWait, CLOSING, CODE };
  Object.defineProperty(api, 'PARTIAL_BRIDGE', { enumerable: true, get: () => partialBridge, set: v => { if (v !== 'hold' && v !== 'spread') throw new Error("PARTIAL_BRIDGE is 'hold' or 'spread'"); partialBridge = v; } });
  return api;
});
