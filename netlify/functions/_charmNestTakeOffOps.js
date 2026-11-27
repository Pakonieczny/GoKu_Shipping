/*  netlify/functions/_charmNestTakeOffOps.js — THE CLOUD'S TAKE-OFF OPS (cloud plan v2, build step 9: P10, C2 (a), m:C14). charmNestLibrary.js registers them.
 *  DORMANT: cloudTakeOff and takeOffJournal are CLOUD_ONLY (refused on every request that is not the cloud driver's own in-process call, in every mode), and the driver sends
 *  them only while it carries a run and "takeoff" is in Charm_Nest_Open/drive.cloudSteps; takeOffAsk is refused in words while the cloud does no take-offs, so a page then does
 *  them itself, as today. A page's own take-off (poolUpdate → poolTakeOff, roseTakeOff, its own sheet save) is NOT changed by anything here (P10).
 *
 *    cloudTakeOff  { orderId, sheetId | null, ids, kind, at, who, note?, reason?, job?, autoOnly? }   (CLOUD_ONLY, fenced on the driver's lease and the run's carrier;
 *                  who: the person, or "Etsy", the take-off is for: the call itself is always sent by "Cloud")
 *       ONE transaction per sheet, with only that sheet's rows in it (so a commit stays well under Firestore's 10 MiB with a 900 KB sheet record, m:C14). It:
 *         1. marks the rows abandoned with the take-off's own mark (removedAt / heldAt …), as poolTakeOff does;
 *         2. writes takeOffUpdate's sheet fields (poolIds, orders, backPool, counts, dirty) — the same pure function a page's take-off uses (_charmNestPlacement.js);
 *         3. drops the pieces from the sheet's charms and placements (and its rejects, feedWait and pieceSides): every other piece keeps its pose, byte for byte;
 *         4. keeps every green line of a Rose Gold, 10K or 14K sheet as saved (THELINE: _charmNestRoseStock.js lineKeeps), the pieces no longer named inside it;
 *         5. sets filesOwed = now (the sheet's files are written again by the files lane), layoutRev + 1 and contentRev + 1;
 *       and in the same commit: the pieces' engraving records leave the sheet (their sheetId), the copies leave the order list of the sheet's set
 *       (the sheet window's dropFromSets), and the order's take-off journal (Charm_Nest_TakeOff/<order>) records the sheet as done.
 *       It refuses (409, nothing written) and says why in words when the sheet is busy (nesting, finishing, saving, Manual Control, a nest task) or belongs to another run that is still
 *       open. A sheet marked laser cut (its own mark, or its set's), or (autoOnly) that holds this order alone, keeps the pieces: answered as kept, rows untouched; so does, for an order
 *       shipped or completed, a sheet with a recorded cut or whose set went to the station.
 *       sheetId null: the order's pieces that no live sheet lists (pooled, waiting): their rows only.
 *    takeOffJournal { orderId, job?, create?, set?, ask? }   (CLOUD_ONLY, fenced): the take-off journal, written before anything moves and after each step.
 *    takeOffAsk    { orderId, kind: hold | sheetOff | retry, sheetId?, lineKeys?, only?, scope?, note?, by, dev }   (a person's press; production only. only: the pieces
 *                  chosen, only those come off; scope: sheet | order. → { askId, at, job }: job is the take-off the lane makes of it, for the page to follow)
 *    takeOffAck    { orderId, at?, by, dev, forget? }   (a person's "Set aside", or the cloud's "not cancelled any more": the notice goes from the shared store)
 *    takeOffGet    { orderIds }   (read: the journals, for every page's progress)                                                                                   */
"use strict";
const crypto = require("crypto");
const { str, num } = require("./_charmNestAuth");
const Placement = require("./_charmNestPlacement");
const RoseStockMod = require("./_charmNestRoseStock");
const Faces = require("../../charm-nest-engrave-faces.js");

const COLL = "Charm_Nest_TakeOff";
const KINDS = new Set(["cancel", "shipped", "completed", "hold", "sheetOff"]);
const ASK_KINDS = new Set(["hold", "sheetOff", "retry"]);
const REASON = { shipped: "shipped at our Shipping station", completed: "completed at the Design Station (never cut again)" };
const BUSY_STATUS = new Set(["nesting", "finishing", "queued", "saving"]);
const TASK_STALE_MS = 14 * 60000;
const CUSTOM_ORDER = /^CUSTOM-\d{8}-\d{1,6}$/;
const orderIdOf = v => { const s = String(v == null ? "" : v).trim(); return CUSTOM_ORDER.test(s) ? s : s.replace(/\D/g, "").slice(0, 30); };
const ridOfPiece = id => { const s = String(id || ""), i = s.indexOf("_"); return i > 0 ? s.slice(0, i) : ""; };
const jobKeyOk = k => typeof k === "string" && /^[\w\-.:]{1,80}$/.test(k);
/** A take-off's own mark on a piece record (the same fields a page's take-off writes: Placement.isTakeOff is true for each). */
function markOf(kind, o) {
  const at = Math.round(num(o.at)) || Date.now(), by = str(o.by, 80) || "System", note = str(o.note, 200);
  if (kind === "hold" || kind === "sheetOff") return { state: "abandoned", sheetId: null, setId: null, heldBy: by, heldReason: note || "on hold", heldAt: at };
  const reason = kind === "cancel" ? str(o.reason, 300) || "cancelled" : REASON[kind] || str(o.reason, 300) || "taken off";
  return { state: "abandoned", sheetId: null, setId: null, removedBy: by, removedReason: reason, removedAt: at };
}
/** The copies of `gone` taken out of a set's order list (both shapes a set keeps: lines as a list or as a map), or null when none was there. */
function setOrdersWithout(orders, gone) {
  if (!orders || typeof orders !== "object") return null;
  let touched = false; const out = {};
  for (const [k, o] of Object.entries(orders)) {
    if (!o || typeof o !== "object") { out[k] = o; continue; }
    if (Array.isArray(o.lines)) {
      const lines = o.lines.map(l => { if (!l || !Array.isArray(l.copies)) return l; const c = l.copies.filter(x => !(x && gone.has(String(x.poolId)))); if (c.length !== l.copies.length) touched = true; return Object.assign({}, l, { copies: c }); }).filter(l => !l || !Array.isArray(l.copies) || l.copies.length);
      if (lines.length) out[k] = Object.assign({}, o, { lines }); else if (!touched) out[k] = o;
      continue;
    }
    if (o.lines && typeof o.lines === "object") {
      const lines = {}; let hit = false;
      for (const [tx, l] of Object.entries(o.lines)) { if (!l || !Array.isArray(l.copies)) { lines[tx] = l; continue; } const c = l.copies.filter(x => !(x && gone.has(String(x.poolId)))); if (c.length !== l.copies.length) hit = true; if (c.length) lines[tx] = Object.assign({}, l, { copies: c }); }
      if (hit) { touched = true; if (Object.keys(lines).length) out[k] = Object.assign({}, o, { lines }); } else out[k] = o;
      continue;
    }
    out[k] = o;
  }
  return touched ? out : null;
}
/** A set already released (approved for laser cutting, its labels made, sent to the station): a change of one of its sheets has its files made again. rec: the sheet. */
const released = (s, rec) => !!(s && (s.committedAt || s.processReady || s.laserSetPending || s.status === "labelled" || (s.labels && s.labels.pdf) || (rec && (rec.processReady || rec.laserSetPending))));
/** Why a sheet record cannot be changed by a take-off now (it is at work), in words; "" when it can. */
function busyWhy(s, now) {
  const name = Placement.labelOf(s) || "The sheet";
  if (BUSY_STATUS.has(String(s.status || ""))) return `${name} is ${s.status} now: its pieces come off when it is done`;
  if (s.saving) return `${name} is being saved now: its pieces come off when it is done`;
  if (s.manualEdit && now - num(s.manualEdit.at) < (num(s.manualStaleMs) || 90000)) return `${name} is being changed by hand (Manual Control, ${str(s.manualEdit.by, 60) || "someone"}): its pieces come off when that ends`;
  const job = s.nestJob;
  if (job && typeof job === "object" && job.jobId && !num(job.endedAt) && now - num(job.at || job.startedAt) < TASK_STALE_MS) return `${name} is being nested now: its pieces come off when the nest ends`;
  return "";
}

module.exports = function (d) {
  const { db, col, FV, txGetAll } = d, N = d.names;
  const MANUAL_STALE_MS = num(d.MANUAL_STALE_MS) || 90000;
  const jref = rid => col(COLL).doc(String(rid));

  /* ── cloudTakeOff ── */
  async function op_cloudTakeOff(b) {
    if (!d.marked()) return { error: "only the cloud sends cloudTakeOff", status: 403 };
    const rid = orderIdOf(b.orderId), kind = KINDS.has(b.kind) ? b.kind : null;
    if (!rid || !kind) return { error: "orderId and kind (cancel, shipped, completed, hold or sheetOff) required" };
    const sheetId = b.sheetId == null || b.sheetId === "" ? null : str(b.sheetId, 80);
    if (sheetId && !d.isId(sheetId)) return { error: "bad sheet id" };
    const ids = [...new Set((Array.isArray(b.ids) ? b.ids : []).map(String))].filter(id => d.isPoolId(id) && ridOfPiece(id) === rid).slice(0, 400);
    if (!ids.length) return { error: "no piece of order " + rid + " was named" };
    // (who: the person or "Etsy" the take-off is for; the cloud's own call is always sent by "Cloud", so the one it is for comes in its own field)
    const patch = markOf(kind, { at: b.at, by: b.who, note: b.note, reason: b.reason }), job = jobKeyOk(b.job) ? b.job : null, autoOnly = b.autoOnly === true, now = Date.now();
    const sref = sheetId ? col(N.SHEETS).doc(sheetId) : null;
    const res = await db.runTransaction(async tx => {
      const f = d.fenceTx(tx); if (f) await f;
      // every read first: a transaction writes after it has read
      const sd = sref ? await tx.get(sref) : null, s = sd && sd.exists ? Object.assign({}, sd.data(), { id: sheetId }) : null;
      const live = !!(s && !s.archived), out = { ok: true, orderId: rid, sheetId, kind, removed: [], extended: [], kept: [], onSheet: [] };
      const want = new Set(ids), at = FV.serverTimestamp();
      let rows = [], sheetUpd = null, setRef = null, setOrders = null, setRevNow = 0, backRows = [], backOf = new Map(), writeRows = [];
      if (live) {
        out.label = Placement.labelOf(s) || ""; out.setId = s.draft ? null : s.setId || null; out.metal = s.metal || null;
        // the laser's mark (the sheet's, or its set's) alone keeps a piece on its sheet (Placement.editable). An order shipped or completed, found by the cloud itself, also leaves what a recorded cut or a set sent to the station holds
        const auto = kind === "shipped" || kind === "completed";
        let keptWhy = !Placement.editable(s) || s.recalled || (auto && num(s.roseCutAt) > 0) ? "already cut" : "", done = !Placement.editable(s) || !!s.recalled, inSet = !s.draft && !!s.setId;
        if (!keptWhy || (kind === "shipped" && !done)) { const sets = (await tx.get(col(N.SETS).where("sheetIds", "array-contains", sheetId))).docs.map(x => x.data() || {}); if (sets.length && !s.draft) inSet = true; if (sets.some(x => num(x.laserDoneAt) > 0)) { keptWhy = "already cut"; done = true; } else if (!keptWhy && auto && sets.some(x => d.setEdit().committedSet(x))) keptWhy = "sent to the station"; }
        /* SHIPPEDSTAYS (Paul, 24 Nov 2026: "nothing should be removed from that set even when the pieces are being shipped, they need to remain on the sheets for historical purposes"; "Yes, keep them" (sheets not marked
           completed yet too); "instead of just removing them, make them outlines noticeable grey them out an ad read outlines ... all the information about the harmony order should still remain"; "once a set of sheet is
           completed and then nothing changes on any of the parts or sheets"). A shipped piece of a sheet that is in a set stays where it lies: its layout, its files and its order stay as they are; the sheet's record says
           which pieces shipped (shippedOut), which every screen draws grey with a red outline, and its picture is drawn again (pictureOwed; its laser files are never touched). A sheet or set marked cut is not written at
           all. A draft's shipped piece still comes off: a draft is cut later, and it would be made twice. */
        if (!keptWhy && kind === "shipped" && inSet) keptWhy = "shipped: it stays on its sheet";
        // a sheet another run still carries is that run's to change
        const fr = d.fenceRun ? d.fenceRun() : null;
        if (!keptWhy && s.runId && fr && s.runId !== fr) { const rs = await tx.get(col(N.RUNS).doc(String(s.runId))), r = rs.exists ? rs.data() || {} : null; if (r && !["complete", "abandoned", "archived"].includes(String(r.status || "")) && !r.retiredInto) return { error: `${out.label} belongs to run ${s.runId}, which is still open: its pieces come off with that run`, status: 409, waiting: true, otherRun: s.runId }; }
        const listed = (s.poolIds || []).map(String), mine = listed.filter(id => want.has(id));
        if (keptWhy) {
          Object.assign(out, { kept: mine, keptWhy });
          const had = s.shippedOut && typeof s.shippedOut === "object" ? s.shippedOut : {}, fresh = kind === "shipped" && !done && !s.draft ? mine.filter(id => !had[id]) : [];
          if (fresh.length) { tx.update(sref, { shippedOut: Object.assign({}, had, Object.fromEntries(fresh.map(id => [id, { at: num(b.at) || now, by: str(b.who, 80) || "Shipping" }]))), pictureOwed: now, updatedAt: at }); out.marked = fresh; }
          return out;
        }
        if (autoOnly && mine.length && listed.every(id => ridOfPiece(id) === rid)) { Object.assign(out, { kept: mine, keptWhy: "the only order on it", only: true }); return out; }
        const busy = busyWhy(Object.assign({ manualStaleMs: MANUAL_STALE_MS }, s), now);
        if (busy && mine.length) return { error: busy, status: 409, waiting: true };
        // a group comes off whole (a pair, counted discs): its other pieces on this sheet come with it (the rows say which lines are groups, as poolTakeOff reads them)
        rows = mine.length ? await txGetAll(tx, mine.map(id => col(N.POOL).doc(id)), d.POOL_EVENT_FIELDS) : [];
        const grouped = new Set(rows.filter(r => r.exists && num((r.data() || {}).groupSize) >= 2).map(r => Placement.groupOfPool(r.id)).filter(Boolean));
        const take = new Set(mine);
        for (const id of Placement.groupMates(s, take, grouped)) if (!take.has(id)) { take.add(id); out.extended.push(id); }
        if (out.extended.length) rows = rows.concat(await txGetAll(tx, out.extended.map(id => col(N.POOL).doc(id)), d.POOL_EVENT_FIELDS));
        if (!take.size) { out.already = true; return out; }
        // the sheet's own fields, as a page's take-off writes them, and the pieces out of its layout (every other pose stays as it is)
        const t = Placement.takeOffUpdate(s, take, at);
        if (t) {
          const charms = (s.charms || []).filter(c => !(c && take.has(String(c.poolId)))), keepIds = new Set(charms.map(c => c && String(c.id)));
          sheetUpd = Object.assign({}, t.update, { charms, placements: (s.placements || []).filter(p => p && keepIds.has(String(p.id))), filesOwed: now, layoutRev: num(s.layoutRev) + 1, contentRev: num(s.contentRev) + 1, takenOffAt: now });
          if (Array.isArray(s.rejects)) sheetUpd.rejects = s.rejects.filter(x => keepIds.has(String(x)));
          if (Array.isArray(s.feedWait)) { const keepPool = new Set(charms.map(c => c && String(c.poolId))); sheetUpd.feedWait = s.feedWait.filter(x => keepIds.has(String(x)) || keepPool.has(String(x))); }
          if (s.pieceSides && typeof s.pieceSides === "object") { const ps = Object.assign({}, s.pieceSides); for (const id of take) delete ps[id]; sheetUpd.pieceSides = ps; }
          Object.assign(sheetUpd, RoseStockMod.lineKeeps(s, new Set((s.charms || []).filter(c => c && take.has(String(c.poolId))).map(c => String(c.id)))));   // (THELINE: every line as saved)
          // its QR label names the orders it holds: a draft's label (not one a person made) goes, as the sheet window's remakeLabels does; a set's is owed again (the labels unit)
          if (s.label) { if (s.draft && !(s.label && s.label.own)) sheetUpd.label = null; sheetUpd.labelOwed = now; }
          out.removed = t.removed; out.layoutRev = sheetUpd.layoutRev; out.contentRev = sheetUpd.contentRev; out.left = charms.length;
          // the copies leave its set's order list (dropFromSets); a set already released has its labels, manifest and files made again (the lane asks for them)
          if (s.setId && !s.draft && d.isId(s.setId)) { setRef = col(N.SETS).doc(String(s.setId)); const ss = await tx.get(setRef), sx = ss.exists ? ss.data() || {} : null; if (sx) { setOrders = setOrdersWithout(sx.orders, take); setRevNow = Number(sx.setRev) || 0; if (released(sx, s)) out.refile = s.setId; } }
          // the pieces' engraving records leave the sheet too (their approval stays: TAKEOFFSOLID G4)
          const refs = []; for (const id of t.removed) for (const face of ["back", "front"]) { const k = Faces.recKey(id, face); if (!backOf.has(k)) { backOf.set(k, sheetId); refs.push(col(N.BACK).doc(k)); } }
          backRows = refs.length ? await txGetAll(tx, refs, ["sheetId"]) : [];
          writeRows = t.removed;
        }
      } else {
        // the order's pieces no live sheet lists (pooled, waiting, or a sheet that is gone): their rows only. A piece some live sheet lists is left for that sheet's own call.
        const list = ids.slice();
        for (let j = 0; j < list.length; j += 30) for (const x of (await tx.get(col(N.SHEETS).where("poolIds", "array-contains-any", list.slice(j, j + 30)))).docs) { const sx = x.data() || {}; if (!sx.archived) for (const id of (sx.poolIds || []).map(String)) if (want.has(id) && !out.onSheet.includes(id)) out.onSheet.push(id); }
        const free = list.filter(id => !out.onSheet.includes(id));
        rows = free.length ? await txGetAll(tx, free.map(id => col(N.POOL).doc(id)), d.POOL_EVENT_FIELDS) : [];
        // (a row that is gone, committed (cut and sent), or already taken off with this very mark is left as it is: a second pass writes nothing)
        writeRows = rows.filter(r => { if (!r.exists) return false; const p = r.data() || {}; if (p.state === "committed") return false; if (Placement.takenOff(p)) return false; return true; }).map(r => r.id);
        out.removed = writeRows.slice();
        out.off = rows.filter(r => r.exists && Placement.takenOff(r.data() || {}) && !writeRows.includes(r.id)).map(r => r.id);   // (off already: its line no longer names it)
      }
      // the journal (read last, written in this same commit: a replay finds the sheet done)
      const js = job ? await tx.get(jref(rid)) : null, jd = js && js.exists ? js.data() || {} : null;
      if (!writeRows.length && !sheetUpd) { out.already = true; return out; }
      // writes
      const seen = new Map();
      for (const r of rows) { const prev = r.exists ? r.data() || {} : null; seen.set(r.id, prev && !prev.sheetId && s ? Object.assign({}, prev, { sheetId: s.id, sheetName: s.fileBase || s.folder || null, setId: prev.setId || s.setId || null, material: prev.material || s.metal || null }) : prev); }
      for (const id of writeRows) tx.set(col(N.POOL).doc(id), Object.assign({}, patch, { updatedAt: at, [Placement.REPOOLED]: null }), { merge: true });
      if (sheetUpd) tx.update(sref, sheetUpd);
      if (setRef && setOrders) tx.update(setRef, { orders: setOrders, setRev: setRevNow + 1, updatedAt: at });   // (FRESHWRITE, 27 Nov 2026: the set's revision moves with its order list, so a cloud look planned before this take-off is refused, never written over it)
      for (const r of backRows) if (r.exists && (r.data() || {}).sheetId && (r.data() || {}).sheetId === backOf.get(r.id)) tx.set(col(N.BACK).doc(r.id), { sheetId: null, updatedAt: at }, { merge: true });
      if (job) {
        const k = sheetId || "pool", was = jd && jd.jobs && jd.jobs[job] && jd.jobs[job].sheets ? jd.jobs[job].sheets[k] : null;
        tx.set(jref(rid), { rid, updatedAt: now, jobs: { [job]: { sheets: { [k]: { state: "off", at: now, n: out.removed.length + (was && was.state === "off" ? num(was.n) : 0), label: str(out.label || (sheetId ? sheetId : "the pool"), 80), ...(out.layoutRev ? { layoutRev: out.layoutRev } : {}) } } } } }, { merge: true });
      }
      out.wrote = true; out.before = seen;
      return out;
    });
    if (!res || res.error) return res;
    const before = res.before; delete res.before;
    if (res.marked) { d.revTouch(b, ["s:" + sheetId]); d.feedOrders(b, [[rid], res.marked]); }   // (SHIPPEDSTAYS)
    if (res.wrote) {
      d.revTouch(b, sheetId ? ["s:" + sheetId] : []); d.feedOrders(b, [[rid], res.removed]);
      const all = res.removed.slice(), who = { by: patch.removedBy || patch.heldBy || "Cloud", device: "cloud", signedIn: true };
      // the order's timeline: "removed" from the pieces' own records (the id the sheet window and AutoCancel use, so a page's own event lands on the same one)
      if (patch.removedBy) { await d.stamp(() => d.poolEvents(all, patch, before, who), "cloud take-off"); if (/^cancel/i.test(String(patch.removedReason || ""))) await d.noteCancelRemovals(all, patch, before, who); }
      // a hold is recorded as held (the sheet window's own event)
      if (patch.heldBy && all.length) await d.stamp(() => [{ orderId: rid, type: "held", by: patch.heldBy, station: "sorter", device: "cloud", sheetId: sheetId || "", sheet: str(res.label || "", 80), id: `cloud-${sheetId || "pool"}-${patch.heldAt}`, at: patch.heldAt, text: `Taken off ${res.label || "the pool"} by ${patch.heldBy}${patch.heldReason && patch.heldReason !== "on hold" ? ": " + patch.heldReason : ""}, on hold`.slice(0, 200), data: { pieces: all.length, poolIds: all.slice(0, 40), cloud: true } }], "cloud hold");
    }
    delete res.wrote;
    return res;
  }

  /* ── takeOffJournal: the cloud's journal of one order's take-offs ── */
  const cleanVal = v => JSON.parse(JSON.stringify(v === undefined ? null : v));
  async function op_takeOffJournal(b) {
    if (!d.marked()) return { error: "only the cloud writes the take-off journal", status: 403 };
    const rid = orderIdOf(b.orderId); if (!rid) return { error: "orderId required" };
    const job = b.job == null ? null : jobKeyOk(b.job) ? b.job : undefined; if (job === undefined) return { error: "bad job key" };
    const now = Date.now(), ref = jref(rid);
    const out = await db.runTransaction(async tx => {
      const f = d.fenceTx(tx); if (f) await f;
      const s = await tx.get(ref), x = s.exists ? s.data() || {} : {}, had = job && x.jobs && x.jobs[job];
      const put = { rid, updatedAt: now };
      if (b.runId) put.runId = str(b.runId, 80);
      // (create: a job already journalled is never made again, and what it says is kept: only a press's own mark below is still written)
      const exists = !!(job && b.create && typeof b.create === "object" && had);
      if (job && b.create && typeof b.create === "object" && !had) { put.jobs = { [job]: Object.assign(cleanVal(b.create), { createdAt: now }) }; put.due = true; }
      if (!exists && job && b.set && typeof b.set === "object") put.jobs = { [job]: Object.assign((put.jobs && put.jobs[job]) || {}, cleanVal(b.set), { updatedAt: now }) };
      else if (!exists && !job && b.set && typeof b.set === "object") Object.assign(put, cleanVal(b.set));
      if (b.ask && typeof b.ask === "object" && /^[\w\-]{1,40}$/.test(String(b.ask.id || ""))) { const a = Object.assign({}, cleanVal(b.ask)); delete a.id; put.asks = { [b.ask.id]: Object.assign(a, { takenAt: now }) }; }
      if (exists && !put.asks) return { ok: true, exists: true, job: had };
      if (Buffer.byteLength(JSON.stringify(Object.assign({}, x, put))) > 800000) return { error: `the take-off journal of order ${rid} is full`, status: 413 };
      tx.set(ref, put, { merge: true });
      return { ok: true, created: !!(job && b.create && !had) };
    });
    if (out && out.ok) d.feedOrders(b, [[rid]], "o");
    return out;
  }

  /* ── takeOffAsk: a person's Hold, sheet Take off, accepted change or "Try again", for the cloud to do ── */
  async function op_takeOffAsk(b) {
    if (d.prefix()) return { error: "the cloud takes pieces off in production only: this computer does it in the sandbox", status: 400, notCloud: true };
    const rid = orderIdOf(b.orderId), kind = ASK_KINDS.has(b.kind) ? b.kind : null;
    if (!rid || !kind) return { error: "orderId and kind (hold, sheetOff or retry) required" };
    const sheetId = b.sheetId ? str(b.sheetId, 80) : null; if (sheetId && !d.isId(sheetId)) return { error: "bad sheet id" };
    const lineKeys = [...new Set((Array.isArray(b.lineKeys) ? b.lineKeys : []).map(String).filter(k => k.startsWith(rid + "_") && k.length <= 80))].slice(0, 50);
    /* (S5, Paul 21 Nov 2026: the sheet window's "Take off and hold", "Only this charm" or "The whole order", pressed on a staff computer, was refused: no sheet was sent, and the
       pieces chosen were never kept, so "Only this charm" would have taken the whole order off that sheet.) only: the pieces chosen (piece ids of this order): only those come off,
       the rest of the order stays where it is; scope: "sheet" (the order's pieces on that sheet) or "order" (wherever they are). Both are kept on the ask, for the lane. */
    const only = [...new Set((Array.isArray(b.only) ? b.only : []).map(String).filter(id => d.isPoolId(id) && ridOfPiece(id) === rid))].slice(0, 400);
    if (Array.isArray(b.only) && b.only.length && !only.length) return { error: `None of the pieces named is a piece of order ${rid}. Nothing was changed` };
    const scope = b.scope === "sheet" || b.scope === "order" ? b.scope : null;
    if (kind === "sheetOff" && !sheetId && !only.length) return { error: "which sheet the order comes off, or which of its pieces, is required" };
    const drive = await d.drive().catch(() => null), mode = drive && (drive.mode === "standby" || drive.mode === "on") ? drive.mode : "off", steps = drive && Array.isArray(drive.cloudSteps) ? drive.cloudSteps.map(String) : [];
    if (mode === "off" || !steps.includes("takeoff")) return { error: "The cloud does not take pieces off yet: this computer does it. Nothing was changed", status: 409, notCloud: true };
    const by = str(b.by, 80), dev = str(b.dev, 60), note = str(b.note, 200), now = Date.now(), id = "a" + now.toString(36) + crypto.randomBytes(3).toString("hex");
    const out = await db.runTransaction(async tx => {
      const s = await tx.get(jref(rid)), x = s.exists ? s.data() || {} : {};
      const waiting = Object.values(x.asks || {}).filter(a => a && a.state === "waiting");
      if (waiting.length >= 20) return { error: `Order ${rid} has 20 requests waiting for the cloud already: wait for them`, status: 409 };
      tx.set(jref(rid), { rid, updatedAt: now, due: true, asks: { [id]: { kind, sheetId: sheetId || null, lineKeys, ...(only.length ? { only } : {}), ...(scope ? { scope } : {}), note, by, dev, at: now, state: "waiting" } } }, { merge: true });
      // (job: the key of the take-off the lane makes of this press, so the page follows it and says the cloud's answer; a "Try again" makes none: it starts the order's own again)
      return { ok: true, orderId: rid, askId: id, kind, at: now, job: kind === "retry" ? null : `${kind}-${now}`, ...(only.length ? { only: only.length } : {}), ...(scope ? { scope } : {}) };
    });
    if (out && out.ok) d.feedOrders(b, [[rid]], "o");
    return out;
  }

  /* ── takeOffAck: the set-aside notice of a cancelled order goes (a person set the pieces aside; or the cloud: the order is not cancelled any more) ── */
  async function op_takeOffAck(b) {
    const rid = orderIdOf(b.orderId); if (!rid) return { error: "orderId required" };
    const at = Math.round(num(b.at)), forget = b.forget === true, who = str(b.by, 80) || "someone", name = "autocancel-" + (d.prefix() ? "sandbox" : "production");
    const sh = await col(N.SHARE).doc(name).get(), f = sh.exists ? (sh.data() || {}).f || {} : {};
    const keys = Object.keys(f).filter(k => k.startsWith("n:" + rid + ":") && !(f[k] && f[k].x) && (!at || k === `n:${rid}:${at}`));
    const notices = keys.map(k => { try { return JSON.parse(f[k].j); } catch (_) { return null; } }).filter(Boolean);
    if (keys.length) { const r = await d.sharePut({ name, del: keys, by: str(b.by, 80), dev: str(b.dev, 60) }); if (r && r.error) return r; d.revTouch(b, ["g:" + name]); }   // (FEED: every page hears the notice went)
    if (!forget) {
      // each cut sheet's step now says it is done, and by whom (the sheet window's ack: step "setAside" under the cancel's own step id); a notice from before its sheets were kept: one note
      const TL = require("./_orderTimeline"), evs = [];
      for (const n of notices) {
        const sheets = Array.isArray(n.sheets) ? n.sheets.slice(0, 12) : [];
        if (sheets.length && num(n.at) > 0) for (const w of sheets) evs.push({ orderId: rid, type: "cancelStep", by: who, station: "sorter", sheet: str(w, 80), text: `On a cut sheet: set aside by ${who} (${w})`.slice(0, 200), id: TL.stepId(n.at, w), data: { outcome: "setAside", done: true, sheet: str(w, 80), kind: "sheet", cancelAt: Math.round(num(n.at)) } });
        else evs.push({ orderId: rid, type: "note", by: who, station: "sorter", text: `Pieces set aside by ${who} (${str(n.where, 120)})`.slice(0, 200), id: `autocancel-aside-ok-${num(n.at)}` });
        if (sheets.length && num(n.at) > 0) { await d.cancelFates({ orderId: rid, fates: [], by: who, removals: sheets.map(w => ({ id: `sheet~${w}`, where: w, kind: "sheet", outcome: "setAside", by: who, text: `On a cut sheet: set aside by ${who} (${w})`.slice(0, 160) })) }).catch(() => null); d.revTouch(b, ["x:" + rid]); }
      }
      if (evs.length) await d.stamp(evs, "set aside");
    }
    const job = at ? `cancel-${at}` : null;
    if (job && (keys.length || forget)) await db.runTransaction(async tx => { const s = await tx.get(jref(rid)); if (!s.exists || !((s.data() || {}).jobs || {})[job]) return; tx.set(jref(rid), { updatedAt: Date.now(), jobs: { [job]: { noticeAck: { by: forget ? "Cloud" : who, at: Date.now(), forget } } } }, { merge: true }); });
    d.feedOrders(b, [[rid]], "o");
    return { ok: true, orderId: rid, removed: keys.length };
  }

  /* ── takeOffGet: the journals of these orders (every page shows the cloud's take-off as it goes) ── */
  async function op_takeOffGet(b) {
    const ids = [...new Set((Array.isArray(b.orderIds) ? b.orderIds : [b.orderId]).map(orderIdOf).filter(Boolean))].slice(0, 60);
    if (!ids.length) return { journals: {} };
    const docs = await db.getAll(...ids.map(jref)), journals = {};
    docs.forEach((x, i) => { journals[ids[i]] = x.exists ? x.data() || null : null; });
    return { journals };
  }

  return { COLL, ops: { cloudTakeOff: op_cloudTakeOff, takeOffJournal: op_takeOffJournal, takeOffAsk: op_takeOffAsk, takeOffAck: op_takeOffAck, takeOffGet: op_takeOffGet } };
};
module.exports.COLL = COLL;
module.exports.markOf = markOf;
module.exports.setOrdersWithout = setOrdersWithout;
module.exports.busyWhy = busyWhy;
module.exports.released = released;
module.exports.REASON = REASON;
