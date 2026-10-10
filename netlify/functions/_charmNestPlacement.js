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
const SheetName = require("../../charm-nest-sheet-name.js");   // the one rule for a sheet's name
const Pair = require("../../charm-nest-pair.js");   // the one definition of a group (receiptId:transactionId) and of the sides of a pair

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

/* ── pairs, mismatched pairs and multi-piece orders (Paul, 9 Oct 2026) ───────────────────────────────────────────────────────
   Every rule on this page is by POOL ID and by ORDER, never by SKU or design: a mismatched pair's two pieces are different
   designs, so a check that matched pieces by what they show would miss them. A GROUP is every piece of one order line
   (receiptId:transactionId, charm-nest-pair.js groupKey); a pool id "receipt_transaction_copy" names its group by itself, so
   nothing here needs a field an older record does not have. */
const POOL_ID = /^\d{4,20}_\d{1,20}_\d{1,3}$/;
/** The group a pool id belongs to ("receiptId:transactionId"), or "" for anything that is not a pool id. */
const groupOfPool = id => (POOL_ID.test(String(id == null ? "" : id)) ? Pair.groupKey(String(id)) : "");
/** The pair fields of a pool row (side, mirror, bodyIndex, groupKey, groupSize: PAIRPOOL) made safe, on the row given (a copy the caller owns):
    side is "L", "R" or null (every piece of an earring pair says L or R, matching or mismatched; discs, letters and singles say null);
    mirror is true or false (the piece is the mirror image of the as-drawn master design); bodyIndex 0 to 9 and groupSize 1 to 400 are
    whole numbers; groupKey is what the pool id says (kept as sent only when it has the shape and the row has no pool id to check it
    against, a charm on a sheet record). What is not one of those is left out (never an error: a row is still written, with the
    fields it can vouch for). A row without them is left alone. */
function cleanPiece(row) {
  if (!row || typeof row !== "object") return row;
  const has = k => Object.prototype.hasOwnProperty.call(row, k);
  if (has("side") && row.side !== "L" && row.side !== "R" && row.side !== null) delete row.side;
  if (has("mirror") && typeof row.mirror !== "boolean") delete row.mirror;
  if (has("bodyIndex")) { const n = Number(row.bodyIndex); if (Number.isInteger(n) && n >= 0 && n <= 9) row.bodyIndex = n; else delete row.bodyIndex; }
  if (has("groupSize")) { const n = Number(row.groupSize); if (Number.isInteger(n) && n >= 1 && n <= 400) row.groupSize = n; else delete row.groupSize; }
  if (has("groupKey")) { const k = groupOfPool(row.poolId); if (k) row.groupKey = k; else if (!(row.poolId == null && typeof row.groupKey === "string" && /^\d{4,20}:\d{1,20}$/.test(row.groupKey))) delete row.groupKey; }
  return row;
}
const PIECE_KEYS = ["side", "mirror", "bodyIndex", "groupKey", "groupSize"];
/** The charms of a sheet record (its `charms` list), each made safe by cleanPiece when it carries any pair field; the others are the same
    objects as were given. Not a list: left as it is. */
function cleanCharms(list) {
  if (!Array.isArray(list)) return list;
  return list.map(c => (c && typeof c === "object" && !Array.isArray(c) && PIECE_KEYS.some(k => Object.prototype.hasOwnProperty.call(c, k)) ? cleanPiece(Object.assign({}, c)) : c));
}
/** A sheet record's `pieceSides` ({ poolId: "L" | "R" }, one flat map, no nested lists): only the pieces whose id is a pool id and whose
    side is L or R; at most 400. Anything that is not such a map gives null (the caller leaves the field out). */
function cleanPieceSides(m) {
  if (!m || typeof m !== "object" || Array.isArray(m)) return null;
  const out = {}; let n = 0;
  for (const [id, v] of Object.entries(m)) { if (n >= 400) break; if (POOL_ID.test(id) && (v === "L" || v === "R")) { out[id] = v; n++; } }
  return out;
}
/** The pieces of `sheet` that belong to a group one of `ids` (a Set of pool ids) belongs to, and are not in `ids` themselves: the rest of a
    line that a take-off named only part of. (R4: a line comes off whole; the page's own plan keeps its copies together, and so does the server.)
    Only a GROUP comes off whole (Paul 9 Oct, after ADVCOMPAT 1: an earring pair, a necklace of counted discs or charms, a line the intake marks multi); the copies
    of a plain quantity-N line each stand alone and a take-off naming one takes one. `grouped` says which lines are groups: a Set of group keys, a function
    (groupKey) => boolean, or `true` for "every line" (the candidates, before the rows are asked). Without it nothing is a group. */
function groupMates(sheet, ids, grouped) {
  const isGroup = grouped === true ? () => true : typeof grouped === "function" ? grouped : grouped && typeof grouped.has === "function" ? k => grouped.has(k) : () => false;
  const groups = new Set(); for (const id of ids) { const g = groupOfPool(id); if (g && isGroup(g)) groups.add(g); }
  if (!groups.size || !sheet || !Array.isArray(sheet.poolIds)) return [];
  return sheet.poolIds.map(String).filter(id => !ids.has(id) && groups.has(groupOfPool(id)));
}
/** Which groups (order lines of more than one piece) are NOT all in one place, from a reconcile's `placement` and the pool rows:
    their pieces on two or more sheets, or some on a sheet and some on none (waiting, held, taken off), or fewer pieces than the group's rows say it has (missing). One entry per group:
    { groupKey, pieces: [{ poolId, side, state, sheetId, sheetLabel, setId, cut }], sheets: [sheetId], offSheet: n }. A group whose pieces
    are all on one sheet, or all on none, is not listed; neither is a single piece or a piece that was made up again (superseded). */
function splitsOf(placement, pools) {
  const row = new Map((pools || []).map(p => [String(p.poolId), p]));
  const groups = new Map();
  for (const [id, pl] of Object.entries(placement || {})) {
    if (!pl || pl.state === "superseded") continue;
    const g = groupOfPool(id); if (!g) continue;
    (groups.get(g) || groups.set(g, []).get(g)).push([id, pl]);
  }
  const out = [];
  for (const [groupKey, list] of groups) {
    // a pair always has two pieces: the rows of a group say its size (groupSize), and when they agree and fewer pieces are left, that is said
    const sizes = new Set(list.map(([id]) => num((row.get(id) || {}).groupSize)).filter(n => n > 1)), of = sizes.size === 1 ? [...sizes][0] : 0, missing = of > list.length ? of - list.length : 0;
    if (!sizes.size) continue;   // (only a group is split: the rows of a plain quantity-N line carry no group size, and its copies each stand alone)
    if (list.length < 2 && !missing) continue;
    const sheets = [...new Set(list.filter(([, pl]) => pl.state === "sheet" && pl.sheetId).map(([, pl]) => pl.sheetId))], off = list.filter(([, pl]) => pl.state !== "sheet").length;
    if (sheets.length < 2 && !(sheets.length === 1 && off) && !missing) continue;
    const tail = id => num((/_(\d+)$/.exec(id) || [])[1]);
    out.push({ groupKey, sheets, offSheet: off, ...(missing ? { of, missing } : {}), pieces: list.sort((a, b) => tail(a[0]) - tail(b[0])).map(([id, pl]) => {
      const p = row.get(id) || {};
      return { poolId: id, side: p.side === "L" || p.side === "R" ? p.side : null, ...(p.mirror === true ? { mirror: true } : {}), state: pl.state, sheetId: pl.state === "sheet" ? pl.sheetId || null : null, sheetLabel: pl.state === "sheet" ? pl.sheetLabel || null : null, setId: pl.setId || null, cut: !!pl.cut };
    }) });
  }
  return out.sort((a, b) => (a.groupKey < b.groupKey ? -1 : a.groupKey > b.groupKey ? 1 : 0));
}

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

/** The label of a sheet as the Library says it ("GF Sheet 2" in a set, "GF Draft 5" outside every set: charm-nest-sheet-name.js). */
function labelOf(s) {
  if (s && METAL_CODE[s.metal]) return SheetName.name(s);
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
        // (a record that lists the piece says where it is now: a move whose row did not catch up; none: it is on no sheet)
        const to = holder ? { sheetId: holder.id || holder.sheetId || null, sheetName: holder.fileBase || holder.folder || null, setId: holder.draft ? null : holder.setId || null } : { sheetId: null, sheetName: null, setId: null };
        row = Object.assign({}, p, to, { sheetIdWas: p.sheetId, repaired: [bad] });
        repaired.push({ poolId: id, kind: bad, sheetId: p.sheetId, now: to.sheetId, why: bad === "sheetGone" ? "its sheet was deleted or repacked" : "its sheet's record does not list it" });
      } else if (!holder && (named || h)) {
        holder = named || { id: p.sheetId, metal: p.material || null, fileBase: p.sheetName || null, setId: p.setId || null, poolIds: [] }; via = "hint";
      }
    }
    if (row) pools.set(id, row);
    const set = holder ? holder.setId || (row && row.setId) || null : null;
    if (holder) {
      placement[id] = { state: "sheet", sheetId: holder.id || holder.sheetId || null, sheetLabel: labelOf(holder) || (row && row.sheetName) || null, setId: set && !holder.draft ? set : null, metal: holder.metal || (row && row.material) || null,
        draft: !!holder.draft, cut: num(holder.laserDoneAt) > 0 || num(holder.roseCutAt) > 0, via, sheetAt: ms(holder.updatedAt) || null, why: "its sheet's record lists it" };
    } else if (takenOff(p)) {
      const removed = num(p.removedAt) > 0 || p.removedBy, cancel = /^cancel/i.test(String(p.removedReason || ""));
      placement[id] = p.state === "superseded" ? { state: "superseded", sheetId: null, setId: null, since: ms(p.updatedAt) || null, why: "its piece was made up again" }
        : removed ? { state: "removed", sheetId: null, setId: null, cancel, by: p.removedBy || null, since: num(p.removedAt) || ms(p.updatedAt) || null, why: String(p.removedReason || (cancel ? "cancelled" : "taken off")).slice(0, 200) }
        : { state: "held", sheetId: null, setId: null, by: p.heldBy || null, since: num(p.heldAt) || ms(p.updatedAt) || null, why: String(p.heldReason || "on hold").slice(0, 200) };
    } else if (p && p.state === "abandoned") {
      placement[id] = { state: "abandoned", sheetId: null, setId: null, since: ms(p.updatedAt) || null, why: "its run was given up" };
    } else {
      placement[id] = { state: "waiting", sheetId: null, setId: null, since: p ? ms(p.updatedAt) || null : null, why: "not on a sheet yet" };
    }
    if (row && row.repaired) placement[id].repaired = row.repaired;
    // which ear of a mismatched pair this piece is (stored on its row by the intake: absent on every other piece and on older rows)
    if (p && (p.side === "L" || p.side === "R")) placement[id].side = p.side;
    if (p && p.mirror === true) placement[id].mirror = true;   // (the piece is the mirror image of the as-drawn design: its own direction)
    if (p && p.state === "committed") placement[id].committed = true;
  }
  const vals = Object.values(placement), count = k => vals.filter(x => x.state === k).length;
  const summary = { pieces: vals.length, onSheet: count("sheet"), waiting: count("waiting"), held: count("held"), removed: count("removed"), cancelling: vals.filter(x => x.state === "removed" && x.cancel).length };
  const splits = splitsOf(placement, [...pools.values()]);
  return { pools: [...pools.values()], sheets, placement, repaired, summary, ...(splits.length ? { splits } : {}) };
}

/** A pool row list as a poolList answers it: a row whose sheet record is deleted or archived says it is on no sheet any more.
    `gone`: Set of sheet ids read and found deleted or archived. */
function repairPoolRows(rows, gone) {
  return rows.map(r => (r && r.sheetId && gone.has(String(r.sheetId)) && !TAKE_OFF_STATES.has(r.state) ? Object.assign({}, r, { sheetId: null, sheetName: null, setId: null, sheetIdWas: r.sheetId, repaired: ["sheetGone"] }) : r));
}

module.exports = { isTakeOff, takenOff, editable, takeOffUpdate, reconcile, repairPoolRows, revOf, digest, labelOf, orderOfKey, REPOOLED, TAKE_OFF_STATES, groupOfPool, cleanPiece, cleanCharms, cleanPieceSides, groupMates, splitsOf };
