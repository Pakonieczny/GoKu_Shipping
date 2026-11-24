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
const FINISH = "Charm_Nest_Finish", JOBS = "Charm_Nest_Jobs", MAX_START = 6, REASK_MS = 30 * 60000;
const ENDED = new Set(["done", "superseded", "failed", "skipped", "waiting"]);
const num = v => (Number.isFinite(+v) ? +v : 0);
const midway = t => !!(t && t.job && t.job.step && !ENDED.has(t.job.step));

/** The id of a nest of this sheet that ended with a result and that its writer has not taken yet; null when none. */
async function finishedNest(db, s, t) {
  /* OWEDCUT (Paul, 23 Nov 2026: the Rose Gold Nov 22 sheet stood "files owed" with its three files written): the layout of a sheet whose green-line cut is recorded is never written from a nest (the writer's first step
     refuses it), so its old nest is never asked for: that very ask marked the sheet owed again a minute after its files were written, was skipped, and left the mark for good. */
  if (num(s && s.roseCutAt) > 0) return null;
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
    const nest = await finishedNest(db, s, t), owed = num(s.filesOwed) > 0 || num(s.artOwed) > 0, going = midway(t);   // (TWOEDITS: a front edit the sheet's files do not show yet is owed, whatever the other mark says)
    const ld = t && t.lastDone, picture = !owed && !nest && !going && num(s.pictureOwed) > 0;
    if (!owed && !nest && !going && !picture) continue;
    /* (SHIPPEDSTAYS: the sheet's picture alone is drawn again, its shipped pieces grey with a red outline; its files are not touched. One that could not be drawn is asked again after half an hour) */
    if (picture) {
      if (ld && num(ld.picture) === num(s.pictureOwed) && now - num(ld.at) < REASK_MS) continue;
      const r = await ctx.call({ op: "finishSheet", id: s.id, start: true, reason: "picture", picture: true }, ["picture", runId, s.id, num(s.pictureOwed), num(s.layoutRev), t ? t.nonce || "" : "", Math.floor(now / REASK_MS)].join("|"));
      if (r && r.status < 400 && r.ok) { started++; asked.push(s.id); } else if (r && r.status >= 400) say(`[drive] picture: ${s.fileBase || s.id}: ${r.error || r.status}`);
      continue;
    }
    // (a job that ended failed, skipped or waiting for its nest on exactly this sheet as it is now is not asked again until the sheet, its owed mark or its nest changes: no loop)
    if (!nest && !going && ld && ["failed", "skipped", "waiting"].includes(ld.outcome) && ld.base && num(ld.base.layoutRev) === num(s.layoutRev) && num(ld.base.contentRev) === num(s.contentRev) && num(ld.owedAt) === num(s.filesOwed)) {
      /* FILESCURRENT (Paul, 21 Nov 2026: "implement all of the updates"): the writer skipped because the sheet's files already show its layout exactly (NOREWRITE: no piece added,
         removed or moved since they were written). Then nothing is owed and the picture stands for the layout as it is: the owed mark comes off and the picture's version is the
         layout's (putSheet stamps previewRev when the save carries the picture). Nothing is written to the files, nothing is nested. Only the record's two labels change. */
      if (ld.outcome === "skipped" && /^No new nest and no piece added, removed or moved/.test(String(ld.why || "")) && !(num(s.artOwed) > 0) && (owed || num(s.previewRev) < num(s.layoutRev)) && s.outputs && s.outputs.preview && s.outputs.preview.url) {
        const r = await ctx.call({ op: "putSheet", sheet: { id: s.id, filesOwed: null, outputs: s.outputs }, contentBase: num(s.contentRev), layoutBase: num(s.layoutRev), filesOwedBase: num(s.filesOwed) }, ["filescurrent", runId, s.id, num(s.layoutRev), num(s.contentRev), num(s.filesOwed)].join("|"));
        if (r && r.status < 400 && r.ok) say(`[drive] files: ${s.fileBase || s.id}: its files already show its layout: the owed mark is off and its picture is current`);
        continue;
      }
      /* OWEDAGAIN (Paul, 23 Nov 2026): an owed sheet is always asked again. A job that ended for a NEST (forNest; before this, the nest it names) said nothing of the files the sheet owes: asked at once as owed, once.
         Any other end is asked again after half an hour (a file or a drawing that was missing can be there by then); never in a loop, and never a nest. */
      const forNest = ld.forNest === undefined ? !!ld.nestJobId : ld.forNest === true;
      if (!(owed && ((ld.outcome === "skipped" && forNest) || now - num(ld.at) > REASK_MS))) continue;
    }
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

module.exports = { driveUnit, tickWhy, finishedNest, MAX_START, REASK_MS };
