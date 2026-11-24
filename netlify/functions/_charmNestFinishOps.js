/*  netlify/functions/_charmNestFinishOps.js — the ops of the cloud's one sheet writer (cloud plan v2 build step 7a), put into charmNestLibrary's OPS by install().
 *  DORMANT: while Charm_Nest_Open/drive is absent or "off" finishSheet refuses in words and writes nothing; finishClaim is CLOUD_ONLY (the cloud's own in-process call alone sends it,
 *  _charmNestOpClass.js); finishGet only reads. Production only: the sandbox's requests are refused (the cloud's calls never carry the sandbox: Lib.call drops it).
 *
 *    finishSheet { id, reason?, nestJobId?, again?, start? }      (PRESS: a page's "Write files again", the end of a nest; and the cloud driver's files lane)
 *        The sheet is owed its files: in ONE transaction its record's filesOwed (now, or kept when already owed and this is no new press) and its task document
 *        Charm_Nest_Finish/<id> (want: what is asked, and the last 20 requests). Refused, in words, while the drive is off, for a sheet that is gone or cut on the laser, and
 *        while the cloud does not write this sheet's files: the "files" lane is not in drive.cloudSteps, or (standby) a computer carries its run (one lane, one place, P9).
 *        The cloud driver's own call with start:true then starts the writer: the task's lease (_charmNestLease, heartbeat on) gives a nonce and charmNestSheet-background
 *        is kicked with it. A page's press starts nothing itself: it kicks the cloud driver (driveSoon, DRIVE_TRIGGERS), and the driver starts the writer (only the driver
 *        starts a worker, cloud plan v2 1.1).
 *    finishGet { id }  → { task (no nonce), sheet: { filesOwed, filesJob, layoutRev, contentRev, outputs, verification, saving } }   (READ)
 *    finishClaim { id, jobId, stage, layoutRev?, contentRev?, nestJobId? }                          (CLOUD_ONLY, fenced on the writer's task nonce, P1)
 *        stage "begin"  the job takes the sheet: filesJob = { jobId, at } (a sheet has one heavy task at a time: refused while another nest of it runs), files owed from now
 *                       if they were not; → { layoutRev, contentRev, filesOwed }
 *        stage "files"  the claim before any final path is written: goes on only when layoutRev, contentRev and filesJob.jobId are still the job's; → { owedAt }, or 409 moved
 *        stage "end"    a job that stops without its files: filesJob cleared when it is still this job's (filesOwed stays: the files are still owed)
 *    finishOverflow { id, runId, layoutBase, contentBase, pieceIds, to }                            (CLOUD_ONLY, fenced; build step 7b: the writer's overflow, below)            */
"use strict";
const FINISH = "Charm_Nest_Finish", SHEETS = "Charm_Nest_Sheets", CARRIER = "Charm_Nest_Run_Carrier", JOBS = "Charm_Nest_Jobs";
const PoolPieces = require("../../charm-nest-pool-pieces.js");   // (SRCLIST: carrySources, the design files of pieces that move go with them)

function install(L) {
  const { OPS, NO_GEN_BUMP, SANDBOXED, db, col, FV, fenceTx, revTouch, driveDoc, driveModeOf, marked, fenced, prefix, str, num, isId } = L;
  if (SANDBOXED && SANDBOXED.add) SANDBOXED.add(FINISH);
  const label = d => str(d && (d.fileBase || d.metalLabel), 120) || "this sheet";

  /** Whether the cloud writes this sheet's files now (P9): the "files" lane is the cloud's, and in standby the cloud carries the sheet's run. Read in the caller's transaction. */
  async function cloudWrites(tx, drive, mode, runId) {
    const steps = Array.isArray(drive && drive.cloudSteps) ? drive.cloudSteps.map(String) : [];
    if (!steps.includes("files")) return { ok: false, why: "the cloud does not write sheet files yet (its files lane is not on): this computer writes them" };
    if (mode === "on") return { ok: true };
    if (!isId(runId)) return { ok: false, why: "this sheet belongs to no run the cloud carries: this computer writes its files" };
    const cs = await tx.get(db.collection(CARRIER).doc(runId)), c = cs.exists ? (cs.data() || {}).carrier : null;
    return c && c.dev === "cloud" ? { ok: true } : { ok: false, why: `${(c && c.by) || "a computer"} carries this sheet's run: it writes the sheet's files` };
  }

  OPS.finishSheet = async function op_finishSheet(b) {
    const id = str(b.id, 80); if (!isId(id)) return { error: "bad sheet id" };
    if (prefix()) return { error: "The cloud's sheet writer is production only: a sandbox sheet's files are written by its page", status: 400 };
    const drive = await driveDoc(), mode = driveModeOf(drive);
    if (mode === "off") return { error: "The cloud does not write sheet files while it is off: the computer that carries the run writes them. Nothing was changed", status: 409, off: true };
    const now = Date.now(), by = str(b.by, 80) || "", reason = str(b.reason, 120) || (marked() ? "owed" : "asked"), nestJobId = /^[\w\-~]{4,120}$/.test(String(b.nestJobId || "")) ? String(b.nestJobId) : null;
    const sref = col(SHEETS).doc(id), tref = col(FINISH).doc(id);
    const out = await db.runTransaction(async tx => {
      if (fenced()) await fenceTx(tx);
      const ss = await tx.get(sref), s = ss.exists ? ss.data() || {} : null;
      if (!s || s.archived) return { error: "This sheet is no longer in the Library: no files were asked for", status: 404 };
      if (num(s.laserDoneAt) > 0) return { error: `${label(s)} has been cut on the laser: its files are not written again`, status: 409, refused: "cut" };
      const ts = await tx.get(tref), t = ts.exists ? ts.data() || {} : {};
      const can = await cloudWrites(tx, drive, mode, s.runId);
      if (!can.ok) return { error: can.why, status: 409, notCloud: true };
      const owed = b.again === true || !(num(s.filesOwed) > 0) ? now : num(s.filesOwed);
      const want = { reason, nestJobId: nestJobId || (t.want && t.want.nestJobId && !(t.lastDone && t.lastDone.nestJobId === t.want.nestJobId) ? t.want.nestJobId : null), at: now, by, owedAt: owed };
      const requests = (Array.isArray(t.requests) ? t.requests : []).concat([{ at: now, by, reason, ...(nestJobId ? { nestJobId } : {}), cloud: !!marked() }]).slice(-20);
      if (owed !== num(s.filesOwed)) tx.set(sref, { filesOwed: owed, updatedAt: FV.serverTimestamp() }, { merge: true });
      tx.set(tref, { sheetId: id, runId: s.runId || null, sheetName: str(s.fileBase, 120) || null, metal: s.metal || null, want, requests, wantAt: now }, { merge: true });
      return { ok: true, id, owedAt: owed };
    });
    if (!out || !out.ok) return out;
    revTouch(b, ["s:" + id]);
    // (only the cloud driver's own call starts the writer: a page's press kicks the driver, which starts it, cloud plan v2 1.1)
    if (marked() && b.start === true) {
      const Lease = require("./_charmNestLease"), got = await Lease.dispatch(db, tref, "finish:" + (str(b.reason, 40) || "owed"), { heartbeat: true });
      if (got && got.nonce) { out.dispatched = true; await kickWriter(id, got.nonce); } else out.queued = true;
    } else out.queued = true;
    return out;
  };

  OPS.finishGet = async function op_finishGet(b) {
    const id = str(b.id, 80); if (!isId(id)) return { error: "bad sheet id" };
    const [ts] = await db.getAll(col(FINISH).doc(id)), [ss] = await db.getAll(col(SHEETS).doc(id), { fieldMask: ["filesOwed", "filesJob", "layoutRev", "contentRev", "outputs", "verification", "saving", "fileBase", "status"] });
    const t = ts.exists ? Object.assign({}, ts.data()) : null; if (t) { delete t.nonce; delete t.tookFrom; }
    return { ok: true, id, task: t, sheet: ss.exists ? ss.data() : null, now: Date.now() };
  };

  OPS.finishClaim = async function op_finishClaim(b) {
    if (!marked()) return { error: "only the cloud sends finishClaim", status: 403 };
    const id = str(b.id, 80), jobId = str(b.jobId, 120), stage = str(b.stage, 10);
    if (!isId(id) || !/^[\w\-]{6,120}$/.test(jobId) || !["begin", "files", "end"].includes(stage)) return { error: "bad claim", status: 400 };
    const sref = col(SHEETS).doc(id), now = Date.now();
    const out = await db.runTransaction(async tx => {
      if (fenced()) await fenceTx(tx);
      const ss = await tx.get(sref), s = ss.exists ? ss.data() || {} : null;
      if (!s || s.archived) return { error: "This sheet is no longer in the Library", status: 404, gone: true };
      const lr = num(s.layoutRev) || 0, cr = num(s.contentRev) || 0, fj = s.filesJob && typeof s.filesJob === "object" ? s.filesJob : null;
      if (stage === "begin") {
        if (num(s.laserDoneAt) > 0) return { error: `${label(s)} has been cut on the laser: its files are not written again`, status: 409, refused: "cut" };
        // (one heavy task a sheet at a time: a nest of it still running that is not the one this job writes)
        const nj = s.nestJob && typeof s.nestJob === "object" ? s.nestJob : null, njId = nj ? str(nj.jobId || nj.id, 120) : "";
        if (njId && njId !== str(b.nestJobId, 120) && !nj.endedAt && !nj.done && !["done", "error", "stopped", "superseded"].includes(String(nj.state || nj.status || ""))) {
          const js = await tx.get(db.collection(JOBS).doc(njId)), j = js.exists ? js.data() || {} : null;
          if (j && !["done", "error", "stopped", "superseded"].includes(String(j.status || j.state || ""))) return { error: `${label(s)} is being nested: its files wait for the nest`, status: 409, busy: "nesting" };
        }
        const owed = num(s.filesOwed) > 0 ? num(s.filesOwed) : now;
        tx.set(sref, { filesJob: { jobId, at: now }, filesOwed: owed, updatedAt: FV.serverTimestamp() }, { merge: true });
        return { ok: true, id, layoutRev: lr, contentRev: cr, filesOwed: owed };
      }
      if (stage === "files") {
        const moved = [];
        if (lr !== num(b.layoutRev)) moved.push("its layout");
        if (cr !== num(b.contentRev)) moved.push("its pieces by hand or their artwork");
        if (!fj || fj.jobId !== jobId) moved.push("the job that writes its files");
        if (moved.length) return { error: `${label(s)} changed (${moved.join(", ")}) since these files were made: they are not written`, status: 409, moved, layoutRev: lr, contentRev: cr, filesJob: fj };
        return { ok: true, id, owedAt: num(s.filesOwed) || 0, layoutRev: lr, contentRev: cr };
      }
      if (fj && fj.jobId === jobId) tx.set(sref, { filesJob: null, updatedAt: FV.serverTimestamp() }, { merge: true });
      return { ok: true, id, ended: true };
    });
    if (out && out.ok && stage !== "files") revTouch(b, ["s:" + id]);
    return out;
  };
  ["finishSheet", "finishGet", "finishClaim"].forEach(op => NO_GEN_BUMP.add(op));   // (none of them moves a piece: a placement answer is made from nothing they write)

  /** finishOverflow { id, runId, layoutBase, contentBase, pieceIds, to: { sheetId, layoutBase, contentBase } | { create: { metal, page, greenLeftover? } }, why? }
   *  (CLOUD_ONLY, fenced on the writer's task nonce; build step 7b, C4: charm-nest-1.html overflowToNextSheet on the records.) In ONE transaction the pieces a sheet's nest turned away
   *  (its rejects, or the later orders a full sheet passes on: feedOn) leave sheet A (its charms, pool ids, rejects, feedWait and orders; A at the revisions the writer just wrote,
   *  layoutBase / contentBase) and wait on the next sheet the writer chose (charm-nest-place.js nextSheet), or on a NEW DRAFT made here with a deterministic id (a replay makes no second
   *  one); their pool rows name that sheet; it owes a nest (nestOwed, dirty; appendOnly when it has a layout). Refused, in words and with nothing written: A changed since (conflict),
   *  the target changed, takes no new order now (closedRec: cut, released full, held, a heavy task on it, in a set) or is in a set the pieces' orders are not on (SETSHUT), another run's or
   *  metal's sheet. A piece taken off on purpose or already made stays where it is. A's placements, membership and files are not touched. → { ok, moved, to: { sheetId, created } } */
  OPS.finishOverflow = async function op_finishOverflow(b) {
    if (!marked()) return { error: "only the cloud's sheet writer moves a sheet's turned-away pieces on", status: 403 };
    if (!fenced()) return { error: "the sheet writer's moves are always fenced on its task: nothing was moved", status: 400 };
    if (prefix()) return { error: "The cloud's sheet writer is production only", status: 400 };
    const id = str(b.id, 80), runId = str(b.runId, 80), T = b.to && typeof b.to === "object" ? b.to : {}, create = T.create && typeof T.create === "object" ? T.create : null;
    if (!isId(id) || !isId(runId)) return { error: "bad sheet or run id", status: 400 };
    if (!create && (!isId(T.sheetId) || T.sheetId === id)) return { error: "bad target sheet", status: 400 };
    const want = [...new Set((Array.isArray(b.pieceIds) ? b.pieceIds : []).map(x => str(x, 200)).filter(Boolean))].slice(0, 300);
    if (!want.length) return { error: "no pieces to move", status: 400 };
    const Place = require("../../charm-nest-place.js"), crypto = require("crypto"), now = Date.now(), fRef = col(SHEETS).doc(id);
    const sha16 = s => crypto.createHash("sha256").update(String(s)).digest("hex").slice(0, 16), rid = c => String((c && c.order) || "").split("/")[0];
    const out = await db.runTransaction(async tx => {
      await fenceTx(tx);
      const fs = await tx.get(fRef); if (!fs.exists) return { error: "That sheet is not in the cloud", status: 404, gone: true };
      const f = fs.data() || {}, fn = label(f);
      if (f.archived) return { error: `${fn} is no longer in use`, status: 409, gone: true };
      if (f.runId !== runId) return { error: `${fn} is not a sheet of this run`, status: 409, refused: "run" };
      if ((typeof b.layoutBase === "number" && num(f.layoutRev) !== b.layoutBase) || (typeof b.contentBase === "number" && num(f.contentRev) !== b.contentBase)) return { error: `${fn} changed since its pieces were turned away: they are looked at again`, status: 409, conflict: true };
      const placed = new Set((f.placements || []).map(p => p && String(p.id)));
      let going = (f.charms || []).filter(c => c && want.includes(String(c.id)) && !placed.has(String(c.id)) && c.poolId);
      if (!going.length) return { ok: true, id, moved: 0, already: true };
      // the target: the sheet chosen, or a new draft whose id is this move's own
      const tRef = create ? col(SHEETS).doc("d-" + sha16(`${runId}|${f.metal}|ovf|${id}|${num(f.layoutRev)}|${num(f.contentRev)}`)) : col(SHEETS).doc(T.sheetId);
      const ts = await tx.get(tRef), t = ts.exists ? ts.data() || {} : null, tn = t ? label(t) : "a new draft";
      if (create && create.metal !== f.metal) return { error: "a new draft for these pieces is of their metal", status: 400 };
      if (!create && !t) return { error: "The next sheet is not in the cloud: it is chosen again", status: 409, conflict: true };
      if (t && create && (t.poolIds || []).map(String).some(x => going.some(c => String(c.poolId) === x))) return { ok: true, id, moved: 0, already: true, to: { sheetId: tRef.id, created: false } };
      if (t && !create && ((typeof T.layoutBase === "number" && num(t.layoutRev) !== T.layoutBase) || (typeof T.contentBase === "number" && num(t.contentRev) !== T.contentBase))) return { error: `${tn} changed since it was chosen: it is chosen again`, status: 409, conflict: true };
      const listing = t ? (await tx.get(col("Charm_Nest_Sets").where("sheetIds", "array-contains", tRef.id))).docs.map(x => Object.assign({ setId: x.id }, x.data())) : [];
      const rs = await tx.get(col("Charm_Nest_Runs").doc(runId)), run = Object.assign({ runId }, rs.exists ? rs.data() || {} : {});
      const rows = new Map(); for (const c of going) { const r = await tx.get(col("Charm_Pool").doc(String(c.poolId))); rows.set(String(c.poolId), r.exists ? r.data() || {} : null); }
      if (t) {
        if (t.archived) return { error: `${tn} is no longer in use: the next sheet is chosen again`, status: 409, conflict: true };
        if (t.runId !== runId || t.metal !== f.metal) return { error: `${tn} is another run's or metal's sheet`, status: 409, refused: "run" };
        if (t.roseCutAt || num(t.laserDoneAt) > 0) return { error: `${tn} was already cut: the next sheet is chosen again`, status: 409, conflict: true };
        if (!create && Place.closedRec(Object.assign({ id: tRef.id }, t), listing, run, now)) return { error: `${tn} takes no new order now: the next sheet is chosen again`, status: 409, conflict: true, refused: "closed" };
        if ((t.setId && !t.draft) || listing.length) {
          const own = new Set((t.charms || []).map(rid).concat((t.orders || []).map(String)).filter(Boolean)), shut = [...new Set(going.map(rid))].filter(o => o && !own.has(o));
          if (shut.length) return { error: `${tn} is in a set: it takes no new order (${shut.slice(0, 3).join(", ")})`, status: 409, conflict: true, refused: "setShut" };
        }
      }
      // (a piece taken off on purpose (held, cancelled, replaced) or already made stays where it is: the take-off or the commit owns it)
      going = going.filter(c => { const r = rows.get(String(c.poolId)); return r && !["abandoned", "superseded", "committed", "engraved", "labelled"].includes(String(r.state || "")) && !(num(r.heldAt) > 0) && !(num(r.removedAt) > 0); });
      if (!going.length) return { ok: true, id, moved: 0, already: true };
      const gIds = new Set(going.map(c => String(c.id))), gPool = new Set(going.map(c => String(c.poolId)));
      const moving = going.map(c => { const x = Object.assign({}, c, { pinned: null, metal: f.metal }); delete x.arrivalPin; return x; });
      // (CARRYBACKS, Paul 23 Nov 2026, lost approvals: the approved engraving of a piece that moves on stayed on the sheet it left, its list and its own record: both go with the piece)
      const goBacks = (f.backPool || []).filter(x => x && gPool.has(String(x.poolId))), bKeys = [], Faces = require("../../charm-nest-engrave-faces.js"), tSet = t && t.setId && !t.draft ? t.setId : null;
      for (const x of goBacks) { const s = await tx.get(col("Charm_Pool_Back").doc(Faces.recKey(String(x.poolId), Faces.faceOf(x)))); if (s.exists && (s.data() || {}).invalidated !== true) bKeys.push(s.id); }
      const fCharms = (f.charms || []).filter(c => !(c && gIds.has(String(c.id)))), keptOrders = new Set(fCharms.map(rid).filter(Boolean));
      const fNext = { charms: fCharms, poolIds: (f.poolIds || []).map(String).filter(x => !gPool.has(x)), rejects: (f.rejects || []).filter(r => !gIds.has(String(r && typeof r === "object" ? r.id : r))),
        feedWait: Array.isArray(f.feedWait) ? f.feedWait.filter(x => !gIds.has(String(x))) : f.feedWait || null, orders: (Array.isArray(f.orders) ? f.orders : []).map(String).filter(o => keptOrders.has(o)),
        ...(goBacks.length ? { backPool: (f.backPool || []).filter(x => !(x && gPool.has(String(x.poolId)))) } : {}), charmCount: fCharms.length, rejectCount: (f.rejects || []).filter(r => !gIds.has(String(r && typeof r === "object" ? r.id : r))).length, movedOn: { n: moving.length, names: moving.map(c => c.name || c.id).slice(0, 12), to: tRef.id, toName: t ? label(t) : null, at: now, by: "cloud" },
        layoutRev: num(f.layoutRev) + 1, contentRev: num(f.contentRev) + 1, updatedAt: FV.serverTimestamp() };
      const tPlaced = !!t && (t.placements || []).length > 0, base = t || {}, carry = PoolPieces.carrySources(base, moving, [f]);   // (SRCLIST: the pieces' design files go with them)
      const tNext = { runId, metal: f.metal, charms: (base.charms || []).concat(moving), poolIds: [...new Set((base.poolIds || []).map(String).concat([...gPool]))], orders: [...new Set((Array.isArray(base.orders) ? base.orders : []).map(String).concat(moving.map(rid).filter(Boolean)))], ...(carry || {}),
        ...(goBacks.length ? { backPool: (base.backPool || []).filter(x => !(x && gPool.has(String(x.poolId)))).concat(goBacks.map(x => Object.assign({}, x, { sheetId: tRef.id, setId: tSet }))) } : {}), charmCount: (base.charms || []).length + moving.length, nestOwed: now, dirty: true, appendOnly: tPlaced, intakeAppend: tPlaced, status: "ready", movedIn: { n: moving.length, from: id, fromName: fn, at: now, by: "cloud", why: str(b.why, 120) || "turned away by its nest" },
        layoutRev: num(base.layoutRev) + 1, contentRev: num(base.contentRev) + 1, archived: false, updatedAt: FV.serverTimestamp() };
      if (!t) Object.assign(tNext, { id: tRef.id, page: Math.max(1, Math.floor(num(create.page)) || 1), draft: true, setId: null, setSeq: null, sheetIndex: null, placements: [], rejects: [], placedCount: 0, current: true, cardStartedAt: now,
        createdAt: FV.serverTimestamp(), createdBy: "Cloud", day: str(create.day, 20) || null, ...(create.greenLeftover && typeof create.greenLeftover === "object" && create.greenLeftover.stockId ? { greenLeftover: { stockId: str(create.greenLeftover.stockId, 120), revision: num(create.greenLeftover.revision), partialId: str(create.greenLeftover.partialId, 160), from: str(create.greenLeftover.from, 120) || null } } : {}) });
      if (Buffer.byteLength(JSON.stringify(Object.assign({}, base, tNext, { updatedAt: 0, createdAt: 0 }))) > 900000) return { error: `${tn} cannot hold that many more pieces in one record`, status: 413, refused: "size" };
      tx.set(fRef, fNext, { merge: true });
      if (t) tx.set(tRef, tNext, { merge: true }); else tx.create(tRef, tNext);
      for (const k of bKeys) tx.set(col("Charm_Pool_Back").doc(k), { sheetId: tRef.id, setId: tSet, carriedAt: now, updatedAt: FV.serverTimestamp() }, { merge: true });
      for (const x of gPool) tx.set(col("Charm_Pool").doc(x), { sheetId: tRef.id, movedFrom: id, movedTo: tRef.id, movedBy: "Cloud", movedAt: now, updatedAt: FV.serverTimestamp() }, { merge: true });
      return { ok: true, id, moved: moving.length, pieces: [...gPool], orders: [...new Set(moving.map(rid).filter(Boolean))], from: { sheetId: id, layoutRev: fNext.layoutRev, contentRev: fNext.contentRev }, to: { sheetId: tRef.id, created: !t, name: t ? label(t) : null, layoutRev: tNext.layoutRev, contentRev: tNext.contentRev } };
    });
    if (out && out.ok && out.moved) revTouch(b, ["s:" + id, "s:" + out.to.sheetId]);
    return out;
  };
  // (finishOverflow moves pieces between sheets: the placement answers change, so it is not in NO_GEN_BUMP)
}

/** POST charmNestSheet-background { sheetId, nonce }: best-effort, at most 1.5 s of the caller's time (a missed kick is taken up by the driver's files lane, the task being stale). */
async function kickWriter(sheetId, nonce) {
  try {
    const fetch = require("node-fetch"), origin = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
    await Promise.race([fetch(`${String(origin).replace(/\/+$/, "")}/.netlify/functions/charmNestSheet-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sheetId, nonce }) }).catch(() => null), new Promise(r => setTimeout(r, 1500))]);
  } catch (e) { console.warn("[finish] the sheet writer was not kicked:", e && e.message); }
}

/** The one sheet writer started now, for a sheet whose nest just ended or whose pieces a person changed: asked as finishSheet asks it, never at the driver's next pass (52 s for order 4245537582, 23 Nov 2026). */
async function startWriter(db, sheetId, w) {
  try {
    const now = Date.now(), ref = db.collection(FINISH).doc(String(sheetId)), Lease = require("./_charmNestLease");
    await ref.set({ sheetId: String(sheetId), runId: w.runId || null, want: { reason: w.reason, nestJobId: w.nestJobId || null, at: now, by: "Cloud", owedAt: now }, wantAt: now }, { merge: true });
    const got = await Lease.dispatch(db, ref, "finish:" + w.reason, { heartbeat: true }); if (got && got.nonce) await kickWriter(String(sheetId), got.nonce);
  } catch (e) { console.warn("[finish] not started at once:", sheetId, e && e.message); }
}

module.exports = { install, kickWriter, startWriter, FINISH };
