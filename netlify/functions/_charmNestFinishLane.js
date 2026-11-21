/*  netlify/functions/_charmNestFinishLane.js — the cloud driver's "files" lane (cloud plan v2 build step 7a; section 1.6 step 6, 2.2 lane "files"): which sheets of the run the
 *  cloud's one sheet writer (charmNestSheet-background.js, _charmNestFinish.js) is started for, and the cron's look at it. Light on purpose: the driver loads this, never the writer.
 *  DORMANT: the driver calls it only while it carries the run and "files" is in Charm_Nest_Open/drive.cloudSteps; with the drive off nothing here runs.
 *
 *    driveUnit({ db, runId, sheets, call, log })   one look at the run's sheets (the driver's own read of them): a sheet whose nest ended and was not written yet (its record's
 *        nestJob, a Charm_Nest_Jobs document "done" with its result), a sheet owed its files (filesOwed: a move by hand, an artwork edit, a take-off, a person's "Write files again"),
 *        and a job its writer left mid-way (stopped, or its task's lease went stale) each get finishSheet { start } through the driver's own fenced call: the op marks the files owed,
 *        and starts the writer on the task's lease. At most MAX_START a look; a sheet whose writer is at work is left alone; a job that ended failed or skipped on exactly the sheet
 *        as it is now is not asked again until the sheet changes (no loop).
 *    tickWhy(db, runId)   the cron's reason to start a driver pass: a writer's task that went quiet (its lease stale, or a job left mid-way with no writer).                   */
"use strict";
const Lease = require("./_charmNestLease");
const FINISH = "Charm_Nest_Finish", JOBS = "Charm_Nest_Jobs", MAX_START = 6;
const ENDED = new Set(["done", "superseded", "failed", "skipped", "waiting"]);
const num = v => (Number.isFinite(+v) ? +v : 0);
const midway = t => !!(t && t.job && t.job.step && !ENDED.has(t.job.step));

/** The id of a nest of this sheet that ended with a result and that its writer has not taken yet; null when none. */
async function finishedNest(db, s, t) {
  const nj = s && s.nestJob && typeof s.nestJob === "object" ? s.nestJob : null, id = nj ? String(nj.jobId || nj.id || "") : "";
  if (!/^[\w\-~]{4,120}$/.test(id)) return null;
  if (t && ((t.lastDone && t.lastDone.nestJobId === id) || (t.job && t.job.nestJobId === id))) return null;
  const st = String(nj.state || nj.status || "");
  if (["running", "queued", "dispatched", "nesting", "pending"].includes(st)) return null;
  const j = await db.collection(JOBS).doc(id).get(), d = j.exists ? j.data() || {} : null;
  return d && String(d.status || d.state || "") === "done" && (d.result || d.best) ? id : null;
}

async function driveUnit(ctx) {
  const { db, runId } = ctx, now = Date.now(), say = ctx.log || (() => {});
  const sheets = (ctx.sheets || []).filter(s => s && s.id && !s.archived && !(num(s.laserDoneAt) > 0));
  if (!sheets.length || !runId) return { started: 0, asked: [] };
  const q = await db.collection(FINISH).where("runId", "==", runId).get(), tasks = new Map(q.docs.map(d => [d.id, d.data() || {}]));
  let started = 0; const asked = [];
  for (const s of sheets) {
    if (started >= MAX_START) break;
    const t = tasks.get(s.id) || null;
    if (t && Lease.held(t, now)) continue;   // (its writer is at work, or was just started)
    const nest = await finishedNest(db, s, t), owed = num(s.filesOwed) > 0, going = midway(t);
    if (!owed && !nest && !going) continue;
    const ld = t && t.lastDone;
    // (a job that ended failed, skipped or waiting for its nest on exactly this sheet as it is now is not asked again until the sheet, its owed mark or its nest changes: no loop)
    if (!nest && !going && ld && ["failed", "skipped", "waiting"].includes(ld.outcome) && ld.base && num(ld.base.layoutRev) === num(s.layoutRev) && num(ld.base.contentRev) === num(s.contentRev) && num(ld.owedAt) === num(s.filesOwed)) continue;
    const reason = nest ? "nest" : going ? "resume" : "owed";
    // (the unit names what the sheet and its task are now: the same look at the same sheet is written once (the ledger); a sheet that changed, or a task dispatched since, is a new one)
    const unit = ["files", runId, s.id, reason, nest || "", num(s.layoutRev), num(s.contentRev), num(s.filesOwed), going ? t.job.jobId : "", t ? t.nonce || "" : ""].join("|");
    const r = await ctx.call(Object.assign({ op: "finishSheet", id: s.id, start: true, reason }, nest ? { nestJobId: nest } : {}), unit);
    if (r && r.status < 400 && r.ok) { started++; asked.push(s.id); }
    else if (r && r.status >= 400) say(`[drive] files: ${s.fileBase || s.id}: ${r.error || r.status}`);
  }
  return { started, asked };
}

async function tickWhy(db, runId, now) {
  if (!runId) return null;
  now = now || Date.now();
  const q = await db.collection(FINISH).where("runId", "==", runId).get();
  return q.docs.some(d => { const t = d.data() || {}; return Lease.stale(t, now) || (midway(t) && !Lease.held(t, now)); }) ? "a sheet's files task went quiet" : null;
}

module.exports = { driveUnit, tickWhy, finishedNest, MAX_START };
