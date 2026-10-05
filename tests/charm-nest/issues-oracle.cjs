/* INDEPENDENT ORACLE for the Library's order issues (Paul, 5 Oct 2026, round 2, point 9):
 *   "...verify all of this carefully thoughtfully logically with an adversarial agent to ensure that every single one that
 *    you want to display as an issue is an actual issue and a verified issue."
 *
 * Written from Paul's rules and from how the app and server KEEP their data, NOT from CharmNestReadiness.issues():
 * it requires nothing from charm-nest-readiness.js. It takes RAW shop data and answers, for every live sheet, which
 * orders are a true issue for that sheet and which pieces cause it.
 *
 * RAW SHOP  { lines, archivedLines?, sheets, sets?, pool? }
 *   lines          { [lineKey]: line record }, as the run document keeps them (the page's lineRecord()):
 *                  { orderId, transactionId, state:'pulled'|'waiting'|'noDesign'|'pooled'|'nested'|'written'|'labelled'|
 *                    'committed'|'held'|'unmatched'|'gone', quantity, poolIds:[pool id per copy that was pooled],
 *                    problems:['unmatchedSku'|'missingSize'|'blockedSku'|'needsMaterial'|'needsMapping'], sku (resolved
 *                    design sku, '' when none), hold, changePending, noDesign, engraveCandidate, engrave:{needed,state,approved}, snap:{title,buyer} }
 *   archivedLines  the same, from the run's line archive (the record's own lines win over an archived one)
 *   sheets         sheet records as Charm_Nest_Sheets keeps them (poolIds, orders, placedCount, setId, sheetIndex, metal,
 *                  draft, solidIncluded, laserHold, laserDoneAt, processSeals, archived, verification, outputs, label, backPool ...)
 *   sets           [{ setId, sheetIds }]
 *   pool           [{ poolId, lineKey, sheetId|null }]  (Charm_Pool; informational: the sheets' poolIds are the server's evidence)
 *
 * RULES (Paul, point 9; agreed with Issues-truth)
 *  piece   one copy of a line (line.quantity copies; copy ids <lineKey>_<n>). A cancelled/gone line and a noDesign line
 *          (nothing to cut) are NOT pieces: they never block and do not count.
 *  by hand a piece a person COMPLETED BY HAND (Review "Complete Order", or its QR label printed from Custom Orders: EITHER button, or both, in any order and
 *          with any number of reprints, releases it (round 8, Paul: "If either or both of the buttons Print QR Label, Complete Order are pressed then that piece
 *          should be considered released"): customPut writes state 'completed' for either press; Paul, 5 Oct round 6: "this chain
 *          only piece obviously does not go on any sheet ... it's still blocking this sheet") is RESOLVED: it needs no sheet, so a copy of it that is on
 *          no sheet is not a piece (it blocks nothing, waits for nothing, is nobody's mate), whatever the run's copy of the line still says (it can read
 *          'unmatched', 'waiting', 'held' or an unknown SKU long after). The truth is the custom order's own record (shop.customs[lineKey], what
 *          Charm_Custom_Orders keeps): state 'completed' with how 'button' | 'print'. A reopened one (state 'open') is a piece again, and so is one
 *          sent to the sheets with its own designs (how 'sheet': it is cut, not completed by hand). Two limits: a HELD piece (hold / changePending)
 *          still holds the sheets of its order wherever it is, and a copy that already sits on a live sheet stays that sheet's piece (it is cut there).
 *          A cancelled order is unchanged. (A record whose state is 'noDesign' only because of a hand completion carries the hint handDone: that
 *          state alone, once the completion is gone, is not a "nothing to cut".)
 *  nested  a piece is on a sheet iff its pool id is in poolIds of a LIVE (non-archived) sheet record. line.state stays
 *          'pooled' until the set is written, so it says nothing about where a piece is. A line with fewer pool ids than
 *          quantity has unnested copies. A nested piece is never "unmatched": nested wins over line.problems.
 *  issue   an order is an issue for sheet X iff X holds at least one live piece of it AND it has at least two live pieces
 *          AND some live piece that is NOT on X is one of:
 *            pooled               not on any sheet yet
 *            noSku / unmatched / noDesign   (and the other problem kinds) not on any sheet and the line has a problem
 *            held                 the line is held / has a change to review (hold, changePending)
 *            otherSheetNotReady   it sits only on sheets that are not ready AND not every one of those sheets is in X's OWN set
 *                                 (round 7, Paul: "it's on both sheets and both sheets are in the same set": a set advances as ONE, a mate
 *                                 sheet of the same set that is not ready is the SET's wait, never an issue of the order. An order whose
 *                                 other piece sits on a not-ready sheet of ANOTHER set is split between two sets (the cardinal rule): it
 *                                 stays a real issue, flagged `split`. Round 8: when that sheet is in NO set at all while X is in one, the wait stays the one
 *                                 honest issue, flagged `noSet`: a real piece on a real sheet that has no set, never the piece completed by hand.)
 *          Single-piece orders and orders whose live pieces are all on X are NEVER an order issue for X through where pieces are,
 *          and X's own readiness (unapproved engraving, no QR label, unverified layout...) never makes an order an issue for X.
 *          ONE exception, agreed with Issues-truth (3ff5b790): a HELD piece (hold text, changePending) is a person's / Etsy's explicit stop
 *          and holds the sheet it sits on too, even a one-piece order on X itself. Never for a gone or noDesign line.
 *  older   a line record with no problems entry but state 'unmatched' / 'oversize' / 'held' is read by that state while its piece is on no sheet
 *  unverified  an order the sheet lists (orders, or the numeric prefix of its pool ids) for which no line record exists at all is not an order
 *          issue: the sheet gets ONE 'orders not checked yet' entry (rec.ghosts), the order gate stays closed.
 *  ready   (another sheet Y is "ready") laserDoneAt>0, or Y is included (not draft, not archived, not held back by a
 *          person, solidIncluded!==false) AND (it was completed before, or all its PHYSICAL stages pass: layout, front
 *          (cutting file + picture), approval (every back decided), backs (every approved back saved), qr).
 *  inSet   a sheet is in a set iff it has a setId and is neither a draft nor left out of it (solidIncluded === false): effSet(sheet).
 *  set     issues of a set = the issues of its member sheets (one entry per sheet and order).
 *  setWait (round 7, R7-1's gate) the SET's wait, said once and never an order issue: for a sheet X in a set (not cut), every OTHER live sheet Y
 *          the set lists that is not ready to be approved: hardBlock(Y) = not cut, not laser-ready, and (it is joined and still has a back
 *          engraving undecided, or it is being laid out, or it is a draft / left out of its set). Soft gaps (QR label, a hold, orders waiting for
 *          other pieces, a layout check to come) never make a mate block: pressing Approve does or lists them.
 *
 * Exports: truth(shop) -> { sheets:{[sheetId]:{label, orders:{[orderId]:{orderId, livePieces, pieces:[...], offenders:[...] , kinds:Set}}}}, ... }
 *          physical(sheet, linesByKey) / ready(sheet, linesByKey), labelOf(sheet), lineOf(poolId) */
'use strict';

const CODE = { gold: 'GF', silver: 'SS', rose: 'RG', gold10k: '10K', gold14k: '14K' };
const url = x => (typeof x === 'string' ? x : (x && x.url) || '');
const num = v => (+v > 0 ? +v : 0);
const lineOf = pid => { const s = String(pid), i = s.lastIndexOf('_'); return i > 0 ? s.slice(0, i) : s; };
const orderOfLine = (key, l) => String((l && l.orderId) || String(key).split('_')[0]);
const sheetId = s => s.id || s.sheetId;
/** completed by hand, from the custom order's own record */
const handDoc = (shop, key) => { const c = shop.customs && shop.customs[key]; return !!c && c.state !== 'open' && c.how !== 'sheet'; };

function labelOf(s) {
  const n = +s.sheetIndex || +((/_Sheet-(\d+)/.exec(s.folder || s.fileBase || '') || [])[1]) || +s.page || 1;
  return `${CODE[s.metal] || s.metalLabel || ''} Sheet ${n}`.trim();
}

/** What a copy's engraving decision is, from the LINE it belongs to (the server derives sheet.engraving the same way). */
function decisionOf(l) {
  const plain = { needed: false, state: 'none', approved: true };
  if (!l) return { needed: true, state: 'unknown', approved: false };
  const e = l.engrave;
  if (l.state === 'gone' && !(e && e.approved)) return plain;            // a cancelled order's piece waits on no decision
  if (e) return { needed: !!e.needed, state: e.state, approved: !!e.approved };
  if (l.engraveCandidate === false) return plain;                         // nothing to engrave
  return { needed: true, state: 'unknown', approved: false };
}

/** The five PHYSICAL stages of a sheet (everything but the order check), from the raw record and the lines. */
function physical(s, linesByKey) {
  const ids = [...new Set((s.poolIds || []).filter(Boolean))], sid = sheetId(s);
  const backs = new Map((s.backPool || s.backs || []).filter(b => !b.invalidated && (!b.sheetId || b.sheetId === sid)).map(b => [b.poolId, b]));
  let waiting = 0, saved = 0, plain = 0;
  for (const id of ids) {
    const d = decisionOf(linesByKey[lineOf(id)]), b = backs.get(id);
    if (d.needed === false && d.approved === true && ['none', 'skipped'].includes(d.state) && !b) { plain++; continue; }
    const accepted = !!(b && b.approvedAt && b.approvedBy) || (d.approved && ['approved', 'written'].includes(d.state));
    if (!accepted) waiting++;
    if (b && b.approvedAt && b.approvedBy && b.verified && b.verified.geometry && b.verified.geometry.ok && b.verified.file && b.verified.file.ok && b.outputs && b.outputs.ai && b.outputs.ai.path && url(b.outputs.ai)) saved++;
  }
  const unidentified = Math.max(0, (+s.placedCount || (s.placements || []).length || 0) - ids.length);
  waiting += unidentified;
  const total = ids.length + unidentified, required = total - plain;
  const files = (s.label && s.label.files) || [], covered = new Set(files.flatMap(f => f.orders || []).map(String)), orders = s.orders || (s.label && s.label.orders) || [];
  return {
    layout: total > 0 && !(s.metal === 'rose' && s.roseStockId && !s.rosePlanHash) && !!s.verification && s.verification.ok === true && !s.dirty && !s.saving && !['nesting', 'finishing', 'queued', 'error'].includes(s.status),
    front: !!url(s.outputs && s.outputs.ai) && !!(s.preview || url(s.outputs && s.outputs.preview)),
    approval: total > 0 && waiting === 0,
    backs: total > 0 && saved === required,
    qr: files.length > 0 && files.every(f => f.path && f.url && f.payload) && orders.every(id => covered.has(String(id))),
    counts: { total, required, waiting, saved, plain }
  };
}
const included = s => !s.draft && s.solidIncluded !== false && !s.archived && !(s.laserHold && num(s.laserHold.at));
/** The set a sheet is really in (a draft, or a sheet left out of it, is in none). */
const effSet = s => (s && s.setId && !s.draft && s.solidIncluded !== false ? String(s.setId) : null);
const laying = s => !!(s.dirty || s.saving || ['nesting', 'finishing', 'queued', 'error'].includes(s.status));
const joined = s => !s.draft && s.solidIncluded !== false;
const completedBefore = s => num(s.laserDoneAt) > 0 || (s.processSeals || []).some(x => x.how === 'laserDone' && num(x.at));
/** Is this sheet ready, as ANOTHER sheet's order sees it (physical readiness: two sheets never wait on each other's order check). */
function ready(s, linesByKey) {
  if (num(s.laserDoneAt) > 0) return true;
  if (!included(s)) return false;
  if (completedBefore(s)) return true;
  const p = physical(s, linesByKey);
  return p.layout && p.front && p.approval && p.backs && p.qr;
}

const PROBLEM_KEYS = (l, kind) => {
  // the keys that name this problem truthfully (a shown key must be one of these)
  if (kind === 'unmatchedSku') return l.sku ? ['unmatched'] : ['noSku'];
  if (kind === 'missingSize') return ['noDesign'];
  if (kind === 'blockedSku') return ['noDesign', 'unmatched', 'blocked'];
  return ['unmatched', 'noSku', 'noDesign', 'needsMaterial', 'needsMapping'];   // needsMaterial / needsMapping: the line cannot be read yet
};
const kindOf = p => (p && p.kind) || p;
/** An older record may carry only the state the page gave the line. */
const STATE_KEYS = { unmatched: l => (l.sku ? ['unmatched'] : ['noSku']), oversize: () => ['noDesign'], held: () => ['held'] };

/** All pieces of one line record: [{lineKey, copy, poolId}]. A pool id names its copy by its last number (<lineKey>_<n>);
 *  a copy the record has no pool id for (not pooled yet, or a record that lost its pool ids) is the pool's own <lineKey>_<n>
 *  for a number the listed ids leave free: if a saved sheet lists that id the piece IS on that sheet, whatever the record says. */
function copiesOf(key, l, indexOrder) {
  const q = Math.max(1, Math.round(+l.quantity || +(l.spec && l.spec.quantity) || 1)), ids = [...new Set((l.poolIds || []).filter(Boolean))];
  const numOf = pid => { const m = /_(\d+)$/.exec(String(pid)); return m ? +m[1] : 0; };
  const out = ids.map(pid => ({ lineKey: key, copy: numOf(pid), poolId: pid }));
  const used = new Set(out.map(c => c.copy));
  for (let n = 1, need = q - ids.length; need > 0; n++) if (!used.has(n)) { out.push({ lineKey: key, copy: n, poolId: `${key}_${n}`, derived: true }); used.add(n); need--; }
  // a piece's NUMBER ('piece 2'): the line's copies by their copy number (OrderPieces and issues() number them alike, so "piece 2" is the same piece
  // on every screen); indexOrder 'listed' (env ISSUES_LISTED_INDEX) is the older numbering, in the order the line lists its pool ids
  return indexOrder === 'copy' ? out.sort((x, y) => x.copy - y.copy || String(x.poolId).localeCompare(String(y.poolId))) : out;
}

/** truth() of a shop, memoised for the default policy (the harness asks for it from every check; the answer is read-only). */
const truthMemo = new WeakMap();
function truth(shop, policy) {
  if (policy) return truthOf(shop, policy);
  const strict = !!process.env.ISSUES_LISTED_INDEX, hit = truthMemo.get(shop);
  if (hit && hit.strict === strict) return hit.t;
  const t = truthOf(shop, {}); truthMemo.set(shop, { strict, t }); return t;
}
function truthOf(shop, policy = {}) {
  policy = { completedBeforeSilent: true, indexOrder: process.env.ISSUES_LISTED_INDEX ? 'listed' : 'copy', ...policy };
  const lines = { ...(shop.archivedLines || {}), ...(shop.lines || {}) };
  const live = (shop.sheets || []).filter(s => !s.archived);
  const byId = new Map(live.map(s => [sheetId(s), s]));
  const where = new Map();                                             // pool id -> [live sheet ids]
  for (const s of live) for (const pid of new Set((s.poolIds || []).filter(Boolean))) { const xs = where.get(pid) || []; xs.push(sheetId(s)); where.set(pid, xs); }
  const readyOf = new Map(live.map(s => [sheetId(s), ready(s, lines)]));
  // the live pieces of each order, in line-key order
  const orders = new Map();
  const tail = k => { const m = /_(\d+)$/.exec(String(k)); return m ? +m[1] : 0; };
  for (const key of Object.keys(lines).sort((a, b) => tail(a) - tail(b) || (a < b ? -1 : a > b ? 1 : 0))) {
    const l = lines[key];
    if (!l || l.state === 'gone') continue;
    const copies = copiesOf(key, l, policy.indexOrder);
    // completed by hand: the custom order's record says so, the line was never pooled and none of its copies sits on a live sheet
    const hand = handDoc(shop, key) && !(l.poolIds || []).length && !copies.some(c => (where.get(c.poolId) || []).length);
    const nothingToCut = l.noDesign || (l.state === 'noDesign' && !l.handDone) || (l.spec && l.spec.noDesign);
    if (nothingToCut && !hand) continue;
    const oid = orderOfLine(key, l), xs = orders.get(oid) || [];
    for (const c of copies) {
      if (hand && !(l.hold || l.changePending)) continue;   // completed by hand: resolved, not a piece (a held one is still a held piece)
      xs.push({ ...c, line: l, sheets: where.get(c.poolId) || [], orderId: oid, hand });
    }
    orders.set(oid, xs);
  }
  for (const pieces of orders.values()) pieces.forEach((p, i) => { p.index = i + 1; });
  const out = { sheets: {}, lines, readyOf, policy, pieces: orders };   // pieces: orderId -> every live piece of the order [{lineKey, copy, poolId, index, sheets:[sheet ids]}]
  for (const s of live) {
    const known = new Set(Object.keys(lines).map(k => orderOfLine(k, lines[k])));
    const listed = [...new Set([...(s.orders || []).map(String), ...(s.poolIds || []).map(p => String(p).split('_')[0]).filter(x => /^\d+$/.test(x))])];
    const sid = sheetId(s), rec = { ghosts: listed.filter(id => !known.has(id)).sort(), label: labelOf(s), ready: readyOf.get(sid), included: included(s), done: num(s.laserDoneAt) > 0, completedBefore: completedBefore(s), physical: physical(s, lines), orders: {} };
    out.sheets[sid] = rec;
    // a sheet that already passed Laser cutting (done, or reopened after it) is laser-ready whatever its orders say, so
    // nothing holds it back: there is no order issue to show (policy completedBeforeSilent, as explain() always did)
    const lasered = () => { rec.laserReady = rec.included && (rec.completedBefore || (rec.physical.layout && rec.physical.front && rec.physical.approval && rec.physical.backs && rec.physical.qr && !Object.keys(rec.orders).length && !rec.ghosts.length)); };
    if (policy.completedBeforeSilent && rec.completedBefore) { lasered(); continue; }
    for (const [oid, pieces] of orders) {
      if (!pieces.some(p => p.sheets.includes(sid))) continue;              // X holds none of it
      const offenders = [];
      for (const p of pieces) {
        const reasons = new Set(), l = p.line, here = p.sheets.includes(sid); let split = false, noSet = false;
        if (l.hold || l.changePending) reasons.add('held');                // a person's stop holds the sheet it sits on too
        if (here) { if (!reasons.size) continue; }                          // on this sheet: that sheet's own readiness shows the rest
        else if (!p.sheets.length) {
          reasons.add('pooled');
          for (const pr of l.problems || []) for (const k of PROBLEM_KEYS(l, kindOf(pr))) reasons.add(k);
          if (!(l.problems || []).length && STATE_KEYS[l.state]) for (const k of STATE_KEYS[l.state](l)) reasons.add(k);
        } else if (p.sheets.every(id => !readyOf.get(id))) {
          // a not-ready sheet of X's OWN set is the set's wait, not the order's; one in another set (or in no set) is a real split / wait
          const mine = effSet(s), same = !policy.sameSetIsIssue && !!mine && p.sheets.every(id => effSet(byId.get(id)) === mine);   // (policy.sameSetIsIssue: the OLD rule, for the coverage count and the mutant)
          // (`split`: an order split between two sets. A held piece is listed as held, whatever sheet it sits on, so it is not listed as a split)
          if (!same) {
            reasons.add('otherSheetNotReady'); split = !!mine && !(l.hold || l.changePending) && p.sheets.some(id => effSet(byId.get(id)) && effSet(byId.get(id)) !== mine);
            // (`noSet`, round 8: the piece's not-ready sheet is in NO set at all while X is in one: the one honest wait, said with its real sheet and that it has no set)
            noSet = !!mine && !split && !(l.hold || l.changePending) && p.sheets.every(id => !effSet(byId.get(id)));
          }
        }
        if (!reasons.size) continue;
        offenders.push({ index: p.index, lineKey: p.lineKey, copy: p.copy, poolId: p.poolId, reasons, ...(split ? { split: true } : {}), ...(noSet ? { noSet: true } : {}), sheets: p.sheets.map(id => ({ id, label: labelOf(byId.get(id)) })) });
      }
      if (offenders.length) rec.orders[oid] = { orderId: oid, livePieces: pieces.length, offenders, pieces: pieces.map(p => ({ index: p.index, lineKey: p.lineKey, copy: p.copy, poolId: p.poolId, sheets: p.sheets })) };
    }
    lasered();
  }
  return out;
}

/** The sheet's OWN trouble: the keys that are true of it right now (its own steps, never its orders). A sheet that was cut (laserDoneAt)
 *  or cut once before keeps that approval: only a missing place in a set / a person's hold is still asked. */
function ownTruth(s, linesByKey) {
  const p = physical(s, linesByKey), out = [];
  if (s.laserHold && num(s.laserHold.at)) out.push('held');
  if (s.draft || s.solidIncluded === false) out.push('notInSet');
  if (completedBefore(s)) return out;
  if (s.metal === 'rose' && s.roseStockId && !s.rosePlanHash) out.push('roseLine');
  if (!(p.layout && p.front)) out.push('layout');
  if (!p.approval) out.push('approvalsNeeded');
  if (!p.backs) out.push('backFilesMissing');
  if (!p.qr) out.push('qrMissing');
  return out;
}
const OWN_STEP = { held: 'nesting', notInSet: 'nesting', roseLine: 'nesting', layout: 'nesting', approvalsNeeded: 'engraving', backFilesMissing: 'engraving', qrMissing: 'orders' };   // (round 13: the saved back files are part of Engraving, the QR label part of Order check)

/** Is this sheet one that keeps its SET from being approved (R7-1's gate, restated from Paul's rule)? 'engraving' | 'nesting' | null. */
function hardBlock(s, t) {
  const r = t.sheets[sheetId(s)];
  if (s.archived || num(s.laserDoneAt) || !r || r.laserReady) return null;
  if (joined(s) && r.physical.counts.waiting > 0) return 'engraving';
  if (laying(s)) return 'nesting';
  if (!joined(s)) return 'nesting';
  return null;
}
/** The set wait of sheet X: the other live sheets of its set that keep the set from being approved ([] for a cut sheet or a sheet in no set). */
function setWaits(shop, t, sid) {
  const x = (shop.sheets || []).find(s => sheetId(s) === sid && !s.archived);
  if (!x || num(x.laserDoneAt) || !effSet(x)) return [];
  const set = (shop.sets || []).find(z => z.setId === x.setId); if (!set) return [];
  const live = new Map((shop.sheets || []).filter(s => !s.archived).map(s => [sheetId(s), s]));
  return set.sheetIds.filter(id => id !== sid && live.has(id) && hardBlock(live.get(id), t)).sort();
}

/** The issue order ids of a sheet, sorted: what the comparison needs most. */
const issueOrders = (t, sid) => Object.keys((t.sheets[sid] || { orders: {} }).orders).sort();

module.exports = { truth, issueOrders, ownTruth, effSet, hardBlock, setWaits, OWN_STEP, physical, ready, included, completedBefore, decisionOf, labelOf, lineOf, copiesOf, CODE };
