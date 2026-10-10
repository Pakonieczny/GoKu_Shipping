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
  const describe = sheet => { const c = metalClass(sheet); return `${c || 'A'} Sheet ${sheet && (sheet.sheetIndex || sheet.page) || 1}`.replace(/^A Sheet/, 'Sheet'); };

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

  /** What a sheet waiting outside a set is told ("" when nothing is missing). */
  const waitWords = missing => `Waits for a completed ${missing.join(' sheet and a completed ')} sheet: a set needs at least 1 completed GF sheet and 1 completed SS sheet`;

  /** Which sheets that want into a set (members: those already in it) may join now. The set after the join must be valid, or nobody joins. */
  function settle(members, wanting) {
    const have = list(members).filter(Boolean), want = list(wanting).filter(Boolean);
    const valid = validSet(have.concat(want.filter(w => !have.some(h => idOf(h) && idOf(h) === idOf(w)))));
    if (valid.ok || !want.length) return { formed: valid.ok || have.length > 0, join: want, wait: [], valid };
    return { formed: have.length > 0, join: [], wait: want.map(sheet => ({ sheet, why: waitWords(valid.missing) })), valid };
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
    const none = { applies: false, ok: true, missing: [], reason: '', short: '', line: '', have: { GF: 0, SS: 0 } };
    if (!enforcing || !members.length || exempt(set) || !members.some(s => Object.prototype.hasOwnProperty.call(s, 'releaseFull'))) return none;
    const v = validSet(members);
    if (v.ok) return Object.assign({}, none, { applies: true, have: v.have });
    const need = `a completed ${v.missing.join(' sheet and a completed ')} sheet`;
    return { applies: true, ok: false, missing: v.missing, reason: v.reason, short: `needs ${need}`,
      line: `Waiting for ${need}: a set needs at least 1 completed GF sheet and 1 completed SS sheet before it can go to laser cutting.`, have: v.have };
  }

  return { isCompleted, metalClass, validSet, wouldStayValid, whyNotCompleted, describe, settle, gate, exempt, enforce, waitWords, CODE };
});
