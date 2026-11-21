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
 *        stage "end"    a job that stops without its files: filesJob cleared when it is still this job's (filesOwed stays: the files are still owed)                          */
"use strict";
const FINISH = "Charm_Nest_Finish", SHEETS = "Charm_Nest_Sheets", CARRIER = "Charm_Nest_Run_Carrier", JOBS = "Charm_Nest_Jobs";

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
}

/** POST charmNestSheet-background { sheetId, nonce }: best-effort, at most 1.5 s of the caller's time (a missed kick is taken up by the driver's files lane, the task being stale). */
async function kickWriter(sheetId, nonce) {
  try {
    const fetch = require("node-fetch"), origin = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
    await Promise.race([fetch(`${String(origin).replace(/\/+$/, "")}/.netlify/functions/charmNestSheet-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sheetId, nonce }) }).catch(() => null), new Promise(r => setTimeout(r, 1500))]);
  } catch (e) { console.warn("[finish] the sheet writer was not kicked:", e && e.message); }
}

module.exports = { install, kickWriter, FINISH };
