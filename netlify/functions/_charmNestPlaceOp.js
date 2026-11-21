/*  netlify/functions/_charmNestPlaceOp.js — placeOnSheet: the cloud puts ONE line's pieces of one metal on ONE sheet record, in ONE transaction (cloud plan v2 §1.4, build step 5;
 *  P4, P5). CLOUD_ONLY (_charmNestOpClass.js): charmNestLibrary's handler refuses it on every page's request, in every mode; it runs only as the cloud's own in-process call,
 *  fenced on the cloud driver's lease and the run's carrier (fenceTx, the first read of its transaction). Nothing calls it while Charm_Nest_Open/drive is absent or "off":
 *  only the driver's placement unit sends it (_charmNestPlaceUnit.js), and the driver runs a step only when the owner has listed it in drive.cloudSteps.
 *
 *  placeOnSheet { runId, sheetId, layoutBase, contentBase, pieces: [charm], home?: orderId }
 *               { runId, create: { metal, page, greenLeftover? }, pieces, home? }        a NEW DRAFT, made in the same transaction
 *    In one transaction it reads the sheet (and, for a new draft, Charm_Nest_Drive/state.draftSeq: the draft's id is "d-" + sha16(runId|metal|draftSeq), created with create(),
 *    the counter moved in the same commit, so a replay never makes a second draft), the run's holds, the sets that list the sheet, the pieces' rows and every live sheet that
 *    lists one of them (poolIds array-contains-any, 30 at a time: the query poolTakeOff already runs inside its transaction); then it refuses, in words and with nothing written:
 *      · a layout or content revision that moved since the unit read the sheet (layoutBase, contentBase: 409 conflict; the unit reads again and decides again)
 *      · a piece another live sheet lists (one piece on exactly one sheet record), a piece taken off on purpose (held, cancelled) or cleaned off, a piece of another run or metal,
 *        a piece already made (committed, engraved, labelled), a piece named twice
 *      · SETSHUT: a sheet in a set takes no new order; a piece of an order already on it may join it (putSheet's own rule, step 1d)
 *      · a sheet that takes no new order (closedRec: cut, released full, held, by hand, a heavy task on it, in a set), unless `home` names the pieces' own order, the sheet already
 *        holds it and it reopens for it (R16: reopensHomeRec), a cut layout (the rose guard), an archived sheet, a record that would pass 900 KB
 *    and otherwise appends the charms and the pool ids (a piece the record already names as a charm is not named twice), marks the nest owed (nestOwed: the time) and the sheet
 *    dirty, raises layoutRev and contentRev, and writes each row's sheetId in the same commit. Pieces already on this very sheet are answered `already` and cost no write.
 *    It never writes placements (the nest does) and never a sheet's membership (setId, draft, setSeq, sheetIndex: NOPARTIALSET, KEEPINSET, OWNEROVERRIDE stay the owner's).
 *  → { ok, sheetId, created, layoutRev, contentRev, placed: [poolId], already: [poolId] } | { error, status: 409, conflict | refused, ... }                                    */
"use strict";
const crypto = require("crypto");
const Place = require("../../charm-nest-place.js");

const str = (v, n = 200) => String(v == null ? "" : v).slice(0, n);
const num = v => (Number.isFinite(+v) ? +v : 0);
const sha16 = s => crypto.createHash("sha256").update(String(s)).digest("hex").slice(0, 16);
const MADE = new Set(["committed", "engraved", "labelled"]);
const ROW_FIELDS = ["state", "sheetId", "runId", "material", "metal", "orderId", "heldAt", "removedAt", "heldBy", "removedBy", "repooledAt", "side", "mirror", "custom"];
const SHEET_PIECE_KEYS = ["id", "hash", "name", "sourceId", "sourceName", "index", "order", "layer", "poolId", "custom", "sku", "metal", "lineKey", "areaPt2", "widthPt", "heightPt", "aiPath", "orderDate", "arrivedAt", "frontAt",
  "excluded", "open", "side", "mirror", "bodyIndex", "groupKey", "groupSize", "oneBody", "chunk", "placedBy", "placedAt"];

/** deps: { db, col, FV, txGetAll, Placement, fenceTx, revTouch, feedOrders, isPoolId, isId, orderOfKey, SHEETS, SETS, POOL, RUNS, SHEET_DOC_BYTES, marked(), fenced(), sandbox() } */
module.exports = function placeOp(d) {
  const { db, col, FV, txGetAll, Placement, fenceTx, isPoolId, isId, orderOfKey, SHEETS, SETS, POOL, RUNS, SHEET_DOC_BYTES } = d;
  /** A charm as the cloud sends it, kept to the fields a sheet record's charm has (finish-core record()) and the cloud's own marks; the pair fields are kept only when sound. */
  function cleanPiece(c) {
    const out = {}; for (const k of SHEET_PIECE_KEYS) if (c[k] !== undefined) out[k] = c[k];
    out.poolId = String(c.poolId); out.order = str(c.order || orderOfKey(c.poolId), 40); out.id = str(c.id || "cloud:" + c.poolId, 200); out.excluded = false;
    return Placement.cleanPiece(out);
  }
  async function placeOnSheet(b) {
    if (!d.marked()) return { error: "only the cloud places a piece on a sheet", status: 403 };
    if (!d.fenced()) return { error: "the cloud's placement is always fenced on its lease: nothing was written", status: 400 };
    if (d.sandbox()) return { error: "the cloud places pieces in production only", status: 400 };
    const runId = str(b.runId, 80); if (!isId(runId)) return { error: "bad run id" };
    const create = b.create && typeof b.create === "object" ? b.create : null;
    const sheetId = create ? null : str(b.sheetId, 120); if (!create && !isId(sheetId)) return { error: "bad sheet id" };
    if (create && !Place.METALS.includes(create.metal)) return { error: "a new draft needs its metal" };
    const sent = (Array.isArray(b.pieces) ? b.pieces : []).filter(c => c && typeof c === "object" && isPoolId(c.poolId)).slice(0, 200);
    if (!sent.length) return { error: "no pieces to place" };
    const ids = sent.map(c => String(c.poolId));
    if (new Set(ids).size !== ids.length) return { error: "a piece is named twice: a piece goes on a sheet once", status: 409 };
    const home = b.home != null && b.home !== "" ? String(b.home).split("/")[0] : null, now = Date.now();
    const stateRef = db.collection("Charm_Nest_Drive").doc("state");   // (the driver's own state document: the drafts' counter, draftSeq; production only)
    let answer = null;
    const refused = await db.runTransaction(async tx => {
      answer = null;
      await fenceTx(tx);   // (P7/C7: the lease's nonce, its state and the run's carrier, read first; a pass that lost its lease writes nothing)
      // ── reads (a transaction writes after it has read) ──
      let ref = null, snap = null, seq = 0;
      if (create) {
        const st = await tx.get(stateRef); seq = num(st.exists ? (st.data() || {}).draftSeq : 0);
        for (let k = 0; k < 8; k++) { seq++; ref = col(SHEETS).doc("d-" + sha16(`${runId}|${create.metal}|${seq}`)); snap = await tx.get(ref); if (!snap.exists) break; }   // (an id already taken: the next number)
        if (snap.exists) return { error: "no free id for a new draft", status: 409, conflict: true };
      } else { ref = col(SHEETS).doc(sheetId); snap = await tx.get(ref); if (!snap.exists) return { error: "that sheet is not in the cloud", status: 404, gone: true }; }
      const old = snap.exists ? snap.data() || {} : {}, id = ref.id, metal = create ? create.metal : old.metal, label = Placement.labelOf(Object.assign({ id }, old)) || `Sheet ${id}`;
      const [runSnap] = await txGetAll(tx, [col(RUNS).doc(runId)], ["sheetHolds", "status", "runId"]);
      const run = Object.assign({ runId }, runSnap && runSnap.exists ? runSnap.data() || {} : {});
      const listing = create ? [] : (await tx.get(col(SETS).where("sheetIds", "array-contains", id))).docs.map(x => Object.assign({ setId: x.id }, x.data()));
      const rows = new Map(); for (let i = 0; i < ids.length; i += 100) for (const r of await txGetAll(tx, ids.slice(i, i + 100).map(x => col(POOL).doc(x)), ROW_FIELDS)) rows.set(r.id, r.exists ? r.data() || {} : null);
      const elsewhere = new Map(); for (let i = 0; i < ids.length; i += 30) for (const x of (await tx.get(col(SHEETS).where("poolIds", "array-contains-any", ids.slice(i, i + 30)).select("poolIds", "archived", "metal", "runId", "fileBase"))).docs) {
        if (x.id === id || (x.data() || {}).archived) continue;
        for (const p of (x.data() || {}).poolIds || []) if (ids.includes(String(p))) elsewhere.set(String(p), x.id);
      }
      // ── the checks, in words ──
      if (old.archived) return { error: `${label} is no longer in use: nothing was placed on it`, status: 409, refused: "archived" };
      if (!create) {
        if (!(typeof b.layoutBase === "number" && Number.isFinite(b.layoutBase)) || !(typeof b.contentBase === "number" && Number.isFinite(b.contentBase))) return { error: "the cloud's placement names the revisions it read", status: 400 };
        if (b.layoutBase !== num(old.layoutRev) || b.contentBase !== num(old.contentRev)) return { error: `${label} changed since the cloud read it: it is read again`, status: 409, conflict: true, layoutRev: num(old.layoutRev), contentRev: num(old.contentRev) };
        if (old.runId && old.runId !== runId) return { error: `${label} is another run's sheet`, status: 409, refused: "run" };
        if (old.roseCutAt) return { error: `${label} was already cut: its leftover is a new sheet`, status: 409, refused: "cut" };
      }
      const had = new Set((old.poolIds || []).map(String)), already = ids.filter(x => had.has(x)), adding = ids.filter(x => !had.has(x));
      if (!adding.length) { answer = { ok: true, sheetId: id, created: false, layoutRev: num(old.layoutRev), contentRev: num(old.contentRev), placed: [], already }; return null; }
      const other = adding.filter(x => elsewhere.has(x));
      if (other.length) return { error: `${other.length === 1 ? "Piece " + other[0] + " is" : other.length + " pieces are"} on another sheet already: nothing was placed`, status: 409, refused: "elsewhere", where: Object.fromEntries(other.slice(0, 20).map(x => [x, elsewhere.get(x)])) };
      const cleared = new Set(old.cleanup ? [].concat(old.cleanup.removedPoolIds || []) : []);
      const bad = [];
      for (const x of adding) {
        const r = rows.get(x), c = sent.find(p => String(p.poolId) === x);
        if (!r) { bad.push([x, "its row is not in the cloud"]); continue; }
        if (Placement.takenOff(r) || Placement.TAKE_OFF_STATES.has(r.state)) { bad.push([x, "it was taken off on purpose (on hold, cancelled or replaced)"]); continue; }
        if (MADE.has(r.state)) { bad.push([x, "it is made already"]); continue; }
        if (r.runId && r.runId !== runId) { bad.push([x, "it is another run's piece"]); continue; }
        if ((r.material || r.metal) && (r.material || r.metal) !== metal) { bad.push([x, `it is ${r.material || r.metal}, the sheet ${metal}`]); continue; }
        if (cleared.has(x)) { bad.push([x, "a cleanup took it off this sheet on purpose"]); continue; }
        if (c && (r.side === "L" || r.side === "R") && (c.side === "L" || c.side === "R") && typeof c.mirror === "boolean" && typeof r.mirror === "boolean" && (c.side !== r.side || c.mirror !== r.mirror)) { bad.push([x, "it would face the other way from its piece record"]); continue; }
      }
      if (bad.length) return { error: `Nothing was placed on ${label}: ${bad.slice(0, 3).map(([x, why]) => `${x} (${why})`).join("; ")}`, status: 409, refused: "pieces", pieces: bad.slice(0, 20).map(([x, why]) => ({ poolId: x, why })) };
      // SETSHUT (P4): a sheet in a set takes no new order; a piece of an order already on it may still join it
      const own = new Set([...had].map(orderOfKey).filter(Boolean).map(String).concat((old.orders || []).map(String)).concat((old.charms || []).filter(c => c && c.poolId && had.has(String(c.poolId))).map(c => String(c.order || "").split("/")[0])));
      if ((old.setId && !old.draft) || listing.length) {
        const shut = adding.filter(x => !own.has(String(orderOfKey(x))));
        if (shut.length) return { error: `${label} is in a set: it takes no new order (${shut.slice(0, 3).join(", ")}${shut.length > 3 ? "…" : ""})`, status: 409, refused: "setShut", pieces: shut.slice(0, 20) };
      }
      // a sheet that takes no new order, unless it is these pieces' own order's sheet and reopens for it (R16)
      if (!create && Place.closedRec(Object.assign({ id }, old), listing, run, now)) {
        const mine = !!home && own.has(home) && adding.every(x => String(orderOfKey(x)) === home);
        if (!mine || !Place.reopensHomeRec(Object.assign({ id }, old), listing, run, now)) return { error: `${label} takes no new order now: nothing was placed on it`, status: 409, refused: "closed" };
      }
      // ── the writes ──
      const keep = Array.isArray(old.charms) ? old.charms.slice() : [];
      // (a charm the record already names but does not list, written by a page before the cloud carried the run, is that piece: it is named once, and what the page traced of it is kept)
      for (const x of adding) { const c = cleanPiece(Object.assign({}, sent.find(p => String(p.poolId) === x), { placedBy: "Cloud", placedAt: now })), i = keep.findIndex(k => k && String(k.poolId) === x); if (i >= 0) keep[i] = Object.assign({}, c, keep[i], { placedBy: "Cloud", placedAt: now, excluded: false }, c.chunk ? { chunk: c.chunk } : {}); else keep.push(c); }
      const poolIds = (old.poolIds || []).map(String).concat(adding), layoutRev = num(old.layoutRev) + 1, contentRev = num(old.contentRev) + 1;
      const patch = { id, runId, metal, charms: keep, poolIds, charmCount: keep.length, nestOwed: now, dirty: true, layoutRev, contentRev, cloudPlaced: { at: now, n: adding.length }, archived: false, updatedAt: FV.serverTimestamp() };
      if (create) Object.assign(patch, { page: Math.max(1, Math.floor(num(create.page)) || 1), draft: true, setId: null, setSeq: null, sheetIndex: null, placements: [], orders: [], placedCount: 0, status: "ready", current: true,
        cardStartedAt: now, createdAt: FV.serverTimestamp(), createdBy: "Cloud", day: str(create.day, 20) || null, ...(create.greenLeftover && typeof create.greenLeftover === "object" ? { greenLeftover: { stockId: str(create.greenLeftover.stockId, 120), revision: num(create.greenLeftover.revision), partialId: str(create.greenLeftover.partialId, 160), from: str(create.greenLeftover.from, 120) || null } } : {}) });
      const bytes = Buffer.byteLength(JSON.stringify(Object.assign({}, old, patch)));
      if (bytes > SHEET_DOC_BYTES) return { error: `${label}: its record would be ${Math.round(bytes / 1024)} KB, over what one sheet record holds: nothing was placed`, status: 413, refused: "size" };
      if (create) { tx.create(ref, patch); tx.set(stateRef, { draftSeq: seq }, { merge: true }); }
      else tx.set(ref, patch, { merge: true });
      for (const x of adding) tx.set(col(POOL).doc(x), { sheetId: id, placedAt: now, placedBy: "Cloud", updatedAt: FV.serverTimestamp() }, { merge: true });
      answer = { ok: true, sheetId: id, created: !!create, layoutRev, contentRev, placed: adding, already };
      return null;
    });
    if (refused) return refused;
    if (answer && answer.placed && answer.placed.length) { d.revTouch(b, ["s:" + answer.sheetId]); d.feedOrders(b, [answer.placed]); }   // (FEED: the sheet and the orders whose pieces moved, so every page hears it)
    return answer || { error: "nothing was answered", status: 500 };
  }
  return { placeOnSheet };
};
