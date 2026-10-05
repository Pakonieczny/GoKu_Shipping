/*  netlify/functions/_charmNestPlacement.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  ONE cloud truth for "is this piece on a sheet, which one, on hold, waiting, taken off, cancelled" (Paul, 5 Oct 2026:
 *  "no mismatch in orders being on sheets, not being on sheets ... as information is added, changed, removed, altered").
 *  The answer used to live in four places that different writers changed in different requests:
 *
 *    Charm_Pool/{poolId}          state, sheetId, setId, heldAt/removedAt (the take-off marks)       the PIECE's row
 *    Charm_Nest_Sheets/{id}       poolIds, orders, placements, laserDoneAt, setId, draft, dirty      the SHEET's record
 *    Charm_Nest_Sets/{setId}      sheetIds, orders                                                  the SET's record
 *    Charm_Nest_Runs/{run}.lines  line.hold / state "held"                                          the run's copy
 *    Order_Timeline/*             placed / removed / held / released events                         HISTORY (permanent)
 *
 *  What is authoritative (the rules every reader and writer on the server follows; the page's resolver reads the answer):
 *
 *    on a sheet      a saved, not archived SHEET RECORD lists the pool id in `poolIds`. The pool row's sheetId is a hint (a
 *                    piece on a sheet still filling has no sheetId on its row until the set is written).
 *    taken off       the pool row says so: state "abandoned" (or "superseded"), no sheetId, and the take-off's mark (heldAt for
 *                    a hold, removedAt for a cancel or an order gone from Etsy). That mark is written in the SAME commit that
 *                    takes the piece off the sheet record (poolTakeOff below), so no reader sees the one without the other.
 *                    A record that still lists such a piece (a write that never reached it) is STALE: the reader prefers the
 *                    take-off, says so (repaired: "staleListing"), and never writes. A later re-placement re-pools first
 *                    (poolPut makes the row live again), which is how Release has always worked.
 *    a sheet gone    a pool row whose sheetId names a record that is deleted or archived, or that is read and does not list
 *                    the piece, is out of date: the piece is on no sheet (repaired: "sheetGone" / "hintStale").
 *    cut             the sheet record's laserDoneAt / roseCutAt. A cut sheet's record is never edited by a take-off.
 *
 *  History is never rewritten here: seals, timeline events, set records of what was committed and cut sheets stay as they are.
 *  A repair is a different ANSWER, never a different record; the only writes are the ones a person's press asked for.
 *
 *  placementRev: a short digest of the update times of the documents an answer was made from. Firestore changes a
 *  document's update time with every write to it, by whoever made it, so the digest changes when any of them does and
 *  never otherwise. A page that holds one compares it with the next answer's and reads again only when it moved.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const crypto = require("crypto");

const num = v => (Number.isFinite(+v) ? +v : 0);
const ms = v => (v && typeof v.toMillis === "function" ? v.toMillis() : v instanceof Date ? v.getTime() : typeof v === "number" ? v : 0);
const orderOfKey = k => (/^(\d{1,30})_/.exec(String(k || "")) || [])[1] || "";
const METAL_CODE = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
const TAKE_OFF_STATES = new Set(["abandoned", "superseded"]);

/** A Firestore document's update time as one short string ("0" for a document that is not there). */
const revOf = snap => (snap && snap.exists !== false && snap.updateTime ? `${snap.updateTime.seconds}.${snap.updateTime.nanoseconds}` : "0");
/** The digest of [kind:id:rev] strings, order-free. */
function digest(parts) { return crypto.createHash("sha1").update([...parts].sort().join("|")).digest("hex").slice(0, 12); }

/** A poolUpdate patch that takes pieces off their sheets (a hold, a cancel, an order gone from Etsy, a repooled line's old pieces):
    the row becomes abandoned or superseded with no sheet, and says why (heldAt/removedAt/…By), or is superseded. A patch that only
    marks a run's rows abandoned (a run given up: no sheetId in the patch) is NOT one: those pieces stay on their saved sheets. */
function isTakeOff(patch) {
  if (!patch || typeof patch !== "object" || !TAKE_OFF_STATES.has(patch.state)) return false;
  if (!Object.prototype.hasOwnProperty.call(patch, "sheetId") || patch.sheetId != null) return false;
  return patch.state === "superseded" || !!(patch.heldAt || patch.removedAt || patch.heldBy || patch.removedBy);
}
/** A pool row as it stands after a take-off: abandoned or superseded, on no sheet, with the take-off's own mark. The marks
    (heldAt, removedAt …) stay on a row for good, as history (the order's timeline derives "removed" from them), so a row that was
    released and made up again carries them still: poolPut stamps repooledAt on such a row, a later take-off clears it, and a row
    with repooledAt is live again, whatever state a run that was given up has left it in (releaseRun writes "abandoned" alone). */
function takenOff(p) {
  if (!p || !TAKE_OFF_STATES.has(p.state) || p.sheetId) return false;
  if (p.state === "superseded") return true;
  return !p.repooledAt && !!(num(p.heldAt) || num(p.removedAt) || p.heldBy || p.removedBy);
}
/** The one field a row that is live again must carry (written by poolPut over a taken-off row), and the one a take-off clears. */
const REPOOLED = "repooledAt";
/** A sheet record a take-off may edit: saved, not archived, not cut. */
const editable = s => !!s && !s.archived && !(num(s.laserDoneAt) > 0) && !(num(s.roseCutAt) > 0);

/** The writes that take `ids` (a Set of pool ids) off one sheet record, as one update: its piece list, the orders it still names,
    the engraving backs of the pieces that left, its counts, and dirty: true (the record's own flag for "the layout changes: write
    it again"; Readiness holds a dirty sheet back from the laser until the page has written it). null: nothing of it is listed.
    The files, charms and placements are the page's to write (rewritePage): this only stops the record from saying what is not true. */
function takeOffUpdate(sheet, ids, serverTs) {
  if (!editable(sheet)) return null;
  const list = Array.isArray(sheet.poolIds) ? sheet.poolIds.map(String) : [];
  const removed = list.filter(id => ids.has(id));
  if (!removed.length) return null;
  const poolIds = list.filter(id => !ids.has(id));
  const gone = new Set(removed.map(orderOfKey).filter(Boolean)), still = new Set(poolIds.map(orderOfKey).filter(Boolean));
  const update = { poolIds, dirty: true, updatedAt: serverTs };
  if (Array.isArray(sheet.orders)) update.orders = sheet.orders.filter(o => !(gone.has(String(o)) && !still.has(String(o))));
  if (Array.isArray(sheet.backPool)) update.backPool = sheet.backPool.filter(b => !(b && ids.has(String(b.poolId))));
  if (num(sheet.placedCount) > 0) update.placedCount = Math.max(0, num(sheet.placedCount) - removed.length);
  if (num(sheet.charmCount) > 0) update.charmCount = Math.max(0, num(sheet.charmCount) - removed.length);
  return { update, removed };
}

/** The label of a sheet as the Library says it ("GF Sheet 2"). */
function labelOf(s) {
  const f = String((s && (s.fileBase || s.folder)) || ""), m = /^([A-Za-z0-9]+)_.*_Sheet-(\d+)/.exec(f);
  const code = (s && METAL_CODE[s.metal]) || (m && m[1]) || "", no = (s && (num(s.sheetIndex) || num(s.page))) || (m && +m[2]) || 0;
  return code && no ? `${code} Sheet ${no}` : f.slice(0, 80);
}

/** Reconciles the documents read for one order into one answer.
    input: { orderId, pools: [row…], sheets: [record…] (non-archived records that name the order or list its pieces),
             hinted: Map sheetId → { exists, archived, listed } for the sheets pool rows name that `sheets` does not hold
                     (listed: how many pieces that record lists; null when not read) }
    output: { pools: rows as the answer shows them, sheets: records with the stale listings left out, placement: { [poolId]: … },
              repaired: [{ poolId, kind, … }], summary } — no input is changed. */
function reconcile(input) {
  const rid = String(input.orderId || "").replace(/\D/g, ""), prefix = rid + "_";
  const hinted = input.hinted instanceof Map ? input.hinted : new Map();
  const poolById = new Map();
  for (const p of input.pools || []) if (p && p.poolId) poolById.set(String(p.poolId), p);
  const repaired = [];

  // 1 · a record that still lists a piece the pool row says was taken off: stale (the take-off wins; the record is not written)
  // (a sheet that was cut is what was made: it is never called stale, whatever a row says)
  const sheets = (input.sheets || []).filter(s => s && !s.archived).map(s => {
    if (!editable(s)) return s;
    const list = Array.isArray(s.poolIds) ? s.poolIds.map(String) : [];
    const stale = list.filter(id => id.startsWith(prefix) && takenOff(poolById.get(id)));
    if (!stale.length) return s;
    for (const id of stale) repaired.push({ poolId: id, kind: "staleListing", sheetId: s.id || s.sheetId || null, why: "taken off, and this sheet's record has not been written again yet" });
    return Object.assign({}, s, { poolIds: list.filter(id => !stale.includes(id)), staleListed: stale });
  });
  const byId = new Map(sheets.map(s => [s.id || s.sheetId, s]));
  const listedBy = new Map();
  for (const s of sheets) for (const id of s.poolIds || []) { const k = String(id); if (!k.startsWith(prefix)) continue; (listedBy.get(k) || listedBy.set(k, []).get(k)).push(s); }

  // 2 · where each piece is
  const ids = new Set([...poolById.keys(), ...listedBy.keys()]);
  const placement = {}, pools = new Map();
  for (const id of ids) {
    const p = poolById.get(id) || null, mine = listedBy.get(id) || [];
    let holder = null, via = null;
    if (mine.length) {
      const want = p && p.sheetId ? mine.find(s => (s.id || s.sheetId) === p.sheetId) : null;
      holder = want || mine.slice().sort((a, b) => (!!a.draft - !!b.draft) || ms(b.updatedAt) - ms(a.updatedAt))[0]; via = "record";
      if (mine.length > 1) repaired.push({ poolId: id, kind: "duplicateListing", sheetId: holder.id || holder.sheetId || null, others: mine.filter(s => s !== holder).map(s => s.id || s.sheetId), why: "listed on more than one sheet record" });
    }
    let row = p;
    if (p && p.sheetId && !TAKE_OFF_STATES.has(p.state)) {
      // the pool row's sheet is a hint: a record that is read and lists other pieces but not this one says the hint is out of date,
      // and so does a record that is gone; a record that lists nothing at all (an old one) cannot contradict it
      const named = byId.get(p.sheetId), h = named ? null : hinted.get(p.sheetId);
      let bad = null;
      if (named) bad = (named.poolIds || []).length > 0 && !(named.poolIds || []).map(String).includes(id) ? "hintStale" : null;
      else if (h) bad = !h.exists || h.archived ? "sheetGone" : h.listed > 0 ? "hintStale" : null;
      if (bad) {
        row = Object.assign({}, p, { sheetId: null, sheetName: null, setId: null, sheetIdWas: p.sheetId, repaired: [bad] });
        repaired.push({ poolId: id, kind: bad, sheetId: p.sheetId, why: bad === "sheetGone" ? "its sheet was deleted or repacked" : "its sheet's record does not list it" });
      } else if (!holder && (named || h)) {
        holder = named || { id: p.sheetId, metal: p.material || null, fileBase: p.sheetName || null, setId: p.setId || null, poolIds: [] }; via = "hint";
      }
    }
    if (row) pools.set(id, row);
    const set = holder ? holder.setId || (row && row.setId) || null : null;
    if (holder) {
      placement[id] = { state: "sheet", sheetId: holder.id || holder.sheetId || null, sheetLabel: labelOf(holder) || (row && row.sheetName) || null, setId: set && !holder.draft ? set : null, metal: holder.metal || (row && row.material) || null,
        draft: !!holder.draft, cut: num(holder.laserDoneAt) > 0 || num(holder.roseCutAt) > 0, via, since: ms(holder.updatedAt) || null, why: "its sheet's record lists it" };
    } else if (takenOff(p)) {
      const removed = num(p.removedAt) > 0 || p.removedBy, cancel = /^cancel/i.test(String(p.removedReason || ""));
      placement[id] = p.state === "superseded" ? { state: "superseded", sheetId: null, setId: null, since: ms(p.updatedAt) || null, why: "its line was made up again" }
        : removed ? { state: "removed", sheetId: null, setId: null, cancel, by: p.removedBy || null, since: num(p.removedAt) || ms(p.updatedAt) || null, why: String(p.removedReason || (cancel ? "cancelled" : "taken off")).slice(0, 200) }
        : { state: "held", sheetId: null, setId: null, by: p.heldBy || null, since: num(p.heldAt) || ms(p.updatedAt) || null, why: String(p.heldReason || "on hold").slice(0, 200) };
    } else if (p && p.state === "abandoned") {
      placement[id] = { state: "abandoned", sheetId: null, setId: null, since: ms(p.updatedAt) || null, why: "its run was given up" };
    } else {
      placement[id] = { state: "waiting", sheetId: null, setId: null, since: p ? ms(p.updatedAt) || null : null, why: "not on a sheet yet" };
    }
    if (row && row.repaired) placement[id].repaired = row.repaired;
    if (p && p.state === "committed") placement[id].committed = true;
  }
  const vals = Object.values(placement), count = k => vals.filter(x => x.state === k).length;
  const summary = { pieces: vals.length, onSheet: count("sheet"), waiting: count("waiting"), held: count("held"), removed: count("removed"), cancelling: vals.filter(x => x.state === "removed" && x.cancel).length };
  return { pools: [...pools.values()], sheets, placement, repaired, summary };
}

/** A pool row list as a poolList answers it: a row whose sheet record is deleted or archived says it is on no sheet any more.
    `gone`: Set of sheet ids read and found deleted or archived. */
function repairPoolRows(rows, gone) {
  return rows.map(r => (r && r.sheetId && gone.has(String(r.sheetId)) && !TAKE_OFF_STATES.has(r.state) ? Object.assign({}, r, { sheetId: null, sheetName: null, setId: null, sheetIdWas: r.sheetId, repaired: ["sheetGone"] }) : r));
}

module.exports = { isTakeOff, takenOff, editable, takeOffUpdate, reconcile, repairPoolRows, revOf, digest, labelOf, orderOfKey, REPOOLED, TAKE_OFF_STATES };
