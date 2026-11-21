/*  netlify/functions/_charmNestFinish.js — THE ONE SHEET WRITER (cloud plan v2 build step 7a; designs.json "Finishing a sheet"). It does in the cloud what one page's finishNest
 *  and persistSheet do (charm-nest-1.html :6397, :6827): a sheet's files (.ai, labelled proof, nest report, preview picture), its checks (geometry, every layer of the written
 *  file re-rendered, the exact cut-line check), the automatic overlap fixes, the files in Storage and the record that names them. Run by charmNestSheet-background.js, one task
 *  per sheet (Charm_Nest_Finish/<sheetId>, its own lease: _charmNestLease, heartbeat on), started only by the cloud driver (finishSheet with start, the files lane:
 *  _charmNestFinishLane.js). DORMANT: the task's lease refuses to begin while Charm_Nest_Open/drive is absent or "off", while "files" is not in drive.cloudSteps, and (standby)
 *  while the cloud does not carry the sheet's run; nothing is then read or written.
 *
 *  THE RULE (P1): files may lag the record, and say so through filesOwed; they never lead it. Every file is made and checked in a work folder of its own
 *  (charmnest/_work/finish/<sheetId>/<jobId>/); the record is written first (one putSheet carrying the placements, with layoutBase and contentBase: layoutRev goes up by exactly
 *  one for the job); the claim (finishClaim, fenced) then checks nothing moved since (layoutRev, contentRev, filesJob); only then are the final paths written, keeping their
 *  download tokens; filesOwed is cleared last, by compare-and-clear (a newer owed mark stays, P2). A change landing at any point ends the job "superseded" with no final path
 *  touched; the files stay owed and the driver asks again.
 *
 *  STEPS, with a cursor in the task document (job.step; the work state in the work folder's state.json), each saved before the next, so an invocation past its budget, a crash
 *  or a takeover goes on from the last saved step (a replayed write is the ledger's: the cloud's mid):
 *    S0 claim     the sheet read; laser-cut, gone or a recorded green-line cut: nothing to write. The job: a finished nest's result (Charm_Nest_Jobs/<nestJobId>, its base must be
 *                 the record's layoutRev) or the record's own layout (filesOwed: a hand move, an artwork edit, a take-off, "Write files again"). finishClaim begin takes the sheet
 *                 (filesJob). A nest that changed nothing (its layout key is the record's) only brings the record's state up to date and clears filesOwed (finishNest's shortcut).
 *    S1 pieces    the sheet's pieces made again from the library (_charmNestFinishHost.js makePieces; once per invocation, nothing stored); a nest result is applied as finishNest
 *                 applies it (pieces that left meanwhile, keepOrdersWhole, the 2.9 % write scale, custom pieces at 100 %)
 *    S2 write     sheetSpec, CharmNestPDF.buildSheet: the .ai and the labelled proof, into the work folder
 *    S3 geometry  CharmNestSolver.verify on the node silhouettes (the page's geomCheck)
 *    S4 render    every charm layer of the written .ai re-rendered alone (_charmNestFinishRender.js renderCheck); the layers' areas kept in the work folder by layerSig
 *    S5 fixes     OVERLAPFIX (finish-core overlapRound / applyOverlapFixes, at most three rounds, each back to S2) then EXACTOVERLAP (finish-core exactFix, at every write but a
 *                 cut green-line sheet; what it changes is checked again: S3, S4)
 *    S6 verdict   OVNOHOLD's verification, CharmNestCustomPool.checkWritten, finish-core buildReport, the preview (resvg)
 *    S7 save      the record (one putSheet with layoutBase), finishClaim files, the four files to their final paths (tokens kept), the final putSheet (filesOwed compare-and-clear,
 *                 filesJob cleared), sheetPdf for a final sheet, putCalibration (as persistSheet). A nest's record also carries finishNest's after-nest marks (build step 7b, C4:
 *                 finish-core sheetFull and topupSettle: releaseFull and the gap fill; intakeOptimized / intakeFinalized; the turn still waiting, feedWait); a released sheet stays
 *                 released (RELEASEMARK); a draft is named `${TAG}_working_${id}` (finish-core sheetFileBase), and an existing file name is never changed
 *    S8 after     (build step 7b) what the nest turned away goes on: finish-core overflowPlan; the next sheet chosen by charm-nest-place.js nextSheet, or a new draft, takes it in ONE
 *                 transaction (finishOverflow) and is asked to nest once (nestStart); feedOn's waiting turn asks this sheet's next nest once (finish-core feedNext); an oldest order that
 *                 cannot fit on an empty sheet is cut in two when it can be (rechunkStalled) and otherwise stops the run with those words (problemStop, read by the nest step)
 *  MEMBERSHIP (P4, NOPARTIALSET / SETSHUT / KEEPINSET): the writer never sends draft, setId, setSeq, sheetIndex or solidIncluded; putSheet leaves membership alone without them.
 *  What membershipOf (finish-core, with SETSHUT's held rule and KEEPINSET through CharmNestSetRules.isPartial) says the sheet's membership should be is worked out at S7 and,
 *  when it differs from the record's (a partial sheet its set does not list), only noted on the task (membership) for the assembly unit: the writer changes no set.
 *  Every write goes through Lib.call (the cloud's in-process call), fenced on this task's nonce and the run's carrier. No Etsy, no world build, no network but its own kick.  */
"use strict";
const crypto = require("crypto");
const zlib = require("zlib");
const Lease = require("./_charmNestLease");

const FINISH = "Charm_Nest_Finish", SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets", JOBS = "Charm_Nest_Jobs", OPEN = "Charm_Nest_Open", CARRIER = "Charm_Nest_Run_Carrier";
const BUDGET_MS = 12 * 60000, TAIL_MS = 90000, RENEW_MS = 20000, WORK = "charmnest/_work/finish";
const STEPS = ["S0", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"];
const ENDED = new Set(["done", "superseded", "failed", "skipped", "waiting"]);
const num = v => (Number.isFinite(+v) ? +v : 0);
const sha = (s, n) => crypto.createHash("sha256").update(String(s)).digest("hex").slice(0, n || 24);
const newToken = () => (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex"));
const urlOf = (bucket, path, token) => "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucket.name) + "/o/" + encodeURIComponent(path) + "?alt=media&token=" + encodeURIComponent(token);
const torontoDay = t => { try { return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(t)); } catch (_) { return new Date(t).toISOString().slice(0, 10); } };
class Stop extends Error { constructor(kind, why) { super(why); this.kind = kind; } }   // (the job ends here: kind superseded | failed | skipped | waiting)
class Fenced extends Error { constructor(why) { super(why || "fenced"); this.fenced = true; } }
class Handed extends Error { constructor(why) { super(why || "handed back"); this.handed = true; } }

/* ── the lease's check (begin and every renew, in its transaction): the drive's mode, the files lane, and (standby) the run carried by the cloud, whose carrier it keeps alive (C6) ── */
async function carries(db, tx, d) {
  const dv = await tx.get(db.collection(OPEN).doc("drive")), drive = dv.exists ? dv.data() || {} : null, mode = drive && (drive.mode === "standby" || drive.mode === "on") ? drive.mode : "off";
  if (mode === "off") return { ok: false, why: "the drive is off: the cloud writes no sheet files" };
  if (!(Array.isArray(drive.cloudSteps) && drive.cloudSteps.map(String).includes("files"))) return { ok: false, why: "the files lane is not the cloud's" };
  const runId = d && d.runId;
  if (!runId) return mode === "on" ? { ok: true } : { ok: false, why: "this sheet belongs to no run the cloud carries" };
  const cref = db.collection(CARRIER).doc(String(runId)), cs = await tx.get(cref), c = cs.exists ? (cs.data() || {}).carrier : null;
  if (!c || c.dev !== "cloud") return { ok: false, why: `${(c && c.by) || "a computer"} carries the sheet's run` };
  return { ok: true, writes: [[cref, { carrier: Object.assign({}, c, { at: Date.now() }) }, { merge: true }]] };
}

/* ── the work folder ── */
const workDir = (sheetId, jobId) => `${WORK}/${sheetId}/${jobId}`;
async function putWork(bucket, path, data, type) { await bucket.file(path).save(Buffer.isBuffer(data) ? data : Buffer.from(data), { resumable: false, contentType: type || "application/octet-stream", metadata: { cacheControl: "no-store", metadata: { uploadedBy: "charmNestSheet-background" } } }); }
async function getWork(bucket, path) { try { const [b] = await bucket.file(path).download(); return b; } catch (e) { if (e && (e.code === 404 || e.code === "404")) return null; throw e; } }
async function dropWork(bucket, dir) { try { await bucket.deleteFiles({ prefix: dir + "/" }); } catch (_) { /* a lifecycle rule takes what is left */ } }
/** The token already on an object (never rotated: every link to the path stays valid; charmNestOutput.js existingToken). */
async function existingToken(file) { try { const [meta] = await file.getMetadata(); const t = meta && meta.metadata && meta.metadata.firebaseStorageDownloadTokens; return t ? String(t).split(",")[0] : null; } catch (_) { return null; } }

/** One run of the writer for one sheet: as many steps as its budget allows. env: { db, Lib: { call }, bucket, sheetId, nonce, budgetMs?, renewMs?, log? }
 *  → { ok, again?, ended?, outcome?, why?, steps } (again: the job goes on in the next invocation; ended: the job ended, done or not). */
async function run(env) {
  const { db, bucket, sheetId, nonce } = env, log = env.log || (() => {});
  const taskRef = db.collection(FINISH).doc(String(sheetId)), t0 = Date.now(), budgetEnd = t0 + (env.budgetMs || BUDGET_MS);
  const out = { ok: true, sheetId, steps: [] };
  const began = await Lease.begin(db, taskRef, nonce, { heartbeat: true, check: (tx, d) => carries(db, tx, d) });
  if (!began) return { ok: false, skipped: "not this task's turn" };
  if (began.refused) { await Lease.end(db, taskRef, nonce, { refused: began.why }).catch(() => {}); return { ok: false, refused: began.why }; }
  let lost = false, handed = null;
  const timer = setInterval(() => { Lease.renew(db, taskRef, nonce, { check: (tx, d) => carries(db, tx, d) }).then(r => { if (r && r.lost) lost = true; else if (!r || !r.ok) handed = (r && r.why) || "the cloud no longer writes this sheet"; }).catch(() => {}); }, env.renewMs || RENEW_MS);
  if (timer && typeof timer.unref === "function") timer.unref();
  const live = () => { if (lost) throw new Fenced("another writer holds this sheet's task"); if (handed) throw new Handed(handed); };
  const snap = await taskRef.get(), task = snap.exists ? snap.data() || {} : {};
  const saveTask = async patch => {
    live();
    const clean = JSON.parse(JSON.stringify(Object.assign({}, patch, { stepAt: Date.now() })));   // (Firestore takes no undefined: what is undefined is left out, as a page's JSON leaves it out)
    const r = await Lease.renew(db, taskRef, nonce, { patch: clean, check: (tx, d) => carries(db, tx, d) });
    if (!r || !r.ok) { if (r && r.lost) throw new Fenced("the task's lease was lost"); throw new Handed((r && r.why) || "the cloud no longer writes this sheet"); }
    Object.assign(task, patch);
  };
  // (C7: before a Storage write, the nonce is read again; a writer that lost its task writes nothing more)
  const nonceHeld = async () => { live(); const [s] = await db.getAll(taskRef, { fieldMask: ["nonce", "state"] }); const d = s && s.exists ? s.data() || {} : {}; if (d.nonce !== nonce || d.state !== "running") { lost = true; throw new Fenced("another writer holds this sheet's task"); } };
  const fence = { path: FINISH + "/" + sheetId, nonce, runId: task.runId || null };
  const X = { env, db, bucket, sheetId, task, log, live, saveTask, nonceHeld, budgetEnd, notes: [],
    call: async (body, unit) => { live(); const r = await env.Lib.call(body, { unit: `finish|${sheetId}|${(task.job && task.job.jobId) || "-"}|${unit}`, fence }); if (r && r.fenced) { lost = true; throw new Fenced(r.error); } return r; },
    read: body => env.Lib.call(body, {}) };
  try {
    for (;;) {
      live();
      const job = task.job && !ENDED.has(task.job.step) ? task.job : null, step = job ? job.step : "S0";
      if (Date.now() > budgetEnd - TAIL_MS) { out.again = true; break; }
      out.steps.push(step);
      const next = await STEP[step](X);
      if (next === "end") { out.ended = true; out.outcome = task.lastDone && task.lastDone.outcome; out.why = task.lastDone && task.lastDone.why; break; }
      if (next === "more") { out.again = true; break; }
      await saveTask({ job: Object.assign({}, task.job, { step: next, steps: (task.job.steps || []).concat([{ step: next, at: Date.now() }]).slice(-40) }) });
    }
  } catch (e) {
    if (e instanceof Stop) { await endJob(X, e.kind, e.message).catch(err => log("[finish] the job's end was not saved: " + (err && err.message))); out.ended = true; out.outcome = e.kind; out.why = e.message; }
    else if (e && e.fenced) { out.ok = false; out.fenced = true; out.why = e.message; clearInterval(timer); return out; }   // (another writer holds the task: nothing more is written, not even the lease's end)
    else if (e && e.handed) { out.ok = false; out.handed = true; out.why = e.message; }
    else {
      out.ok = false; out.error = String((e && e.message) || e).slice(0, 300); log("[finish] " + sheetId + ": " + out.error);
      // (an error is tried again from the saved step, three times in all; then the job ends failed, in words, and the files stay owed)
      if (task.job && !ENDED.has(task.job.step)) {
        const n = num(task.job.errors) + 1;
        if (n >= 3) { await endJob(X, "failed", out.error).catch(() => {}); out.ended = true; out.outcome = "failed"; out.why = out.error; }
        else { task.job.errors = n; await saveTask({ job: task.job }).catch(() => {}); out.again = true; }
      }
    }
  }
  clearInterval(timer);
  const ended = await Lease.end(db, taskRef, nonce, { steps: out.steps, outcome: out.outcome || null, again: !!out.again }, out.error || null).catch(() => ({ again: false }));
  // (asked again while it ran: another job is wanted; or the job itself goes on)
  if (ended && ended.again && out.ended) out.again = true;
  return out;
}

/** The job ends (done or not): the sheet given back (finishClaim end, when it still names this job), the work folder dropped, lastDone kept for the driver's lane. */
async function endJob(X, kind, why) {
  const job = X.task.job || {}, now = Date.now();
  if (job.jobId && kind !== "done") { try { await X.call({ op: "finishClaim", id: X.sheetId, jobId: job.jobId, stage: "end" }, "end"); } catch (e) { if (e && e.fenced) throw e; } }
  if (job.jobId) await dropWork(X.bucket, workDir(X.sheetId, job.jobId));
  // (the nest it was for is not asked for again, unless the job only waited for it; the base it saw lets the driver's lane leave a sheet alone until it changes)
  const seen = X.seen || {}, wantNest = X.task.want && X.task.want.nestJobId ? String(X.task.want.nestJobId) : null;
  const lastDone = { jobId: job.jobId || null, nestJobId: kind === "waiting" ? null : job.nestJobId || wantNest, outcome: kind, why: String(why || "").slice(0, 400), at: now,
    base: job.base || (seen.layoutRev != null ? { layoutRev: seen.layoutRev, contentRev: seen.contentRev } : null), owedAt: num(job.owedAt) || num(seen.owedAt), wantAt: num(job.wantAt) || num(X.task.want && X.task.want.at),
    written: job.written || null, key: job.key || null, membership: job.membership || null, notes: (X.notes || []).slice(-12) };
  await X.saveTask({ job: null, lastDone, step: kind, outcome: kind, why: lastDone.why });
}

/* ════ the steps ════ */
const STEP = {};

/** S0: the job — what is written (a finished nest, or the record as it stands), and the sheet taken for it (finishClaim begin). */
STEP.S0 = async X => {
  const { db, task } = X, rec = await readSheet(db, X.sheetId), want = task.want || {};
  if (rec) X.seen = { layoutRev: num(rec.layoutRev), contentRev: num(rec.contentRev), owedAt: num(rec.filesOwed) };
  if (!rec || rec.archived) throw new Stop("skipped", "The sheet is no longer in the Library: no files were written");
  if (num(rec.laserDoneAt) > 0) throw new Stop("skipped", `${rec.fileBase || "This sheet"} has been cut on the laser: its files are not written again`);
  // (a Rose Gold, 10K or 14K sheet whose green-line cut is recorded keeps its layout as cut: putSheet refuses a layout over it; the person sets the cut aside first)
  if (rec.roseCutAt) throw new Stop("skipped", `${rec.fileBase || "This sheet"}'s green-line cut is recorded: its layout is not written again until the cut is set aside`);
  let nest = null;
  if (want.nestJobId && !(task.lastDone && task.lastDone.nestJobId === String(want.nestJobId))) {
    const js = await db.collection(JOBS).doc(String(want.nestJobId)).get(), j = js.exists ? js.data() || {} : null;
    const st = j ? String(j.status || j.state || "") : "";
    if (j && st !== "done" && !["error", "stopped", "superseded"].includes(st)) throw new Stop("waiting", `the nest ${want.nestJobId} has not ended: its files wait for it`);
    const result = j && st === "done" ? j.result || j.best || null : null;
    if (result && Array.isArray(result.placements)) {
      const base = j.layoutRev != null ? num(j.layoutRev) : j.input && j.input.layoutRev != null ? num(j.input.layoutRev) : null;
      if (base != null && base !== num(rec.layoutRev)) throw new Stop("superseded", `the nest ${want.nestJobId} was made for layout ${base}; the sheet is at ${num(rec.layoutRev)} now: it is not written`);
      nest = { jobId: String(want.nestJobId), result, appendOnly: j.appendOnly != null ? !!j.appendOnly : j.input && j.input.appendOnly != null ? !!j.input.appendOnly : null,
        intakePhase: j.auto && String(j.mode || "auto") !== "fresh" ? "fill" : null };   // (build step 7b: the cloud's own ask of a nest is the page's live intake, its quick gap fill: phase "fill")
    }
  }
  if (!nest && !(num(rec.filesOwed) > 0)) throw new Stop("skipped", "Nothing is owed: the sheet's files stand as they are");
  if (!(rec.placements || []).length && !(nest && nest.result.placements.length)) throw new Stop("skipped", "The sheet places no piece: there is nothing to write");
  const jobId = "fin-" + sha([X.sheetId, num(rec.layoutRev), num(rec.contentRev), nest ? nest.jobId : "", num(want.at)].join("|"), 20);
  X.task.job = { jobId, step: "S0", steps: [], startedAt: Date.now(), nestJobId: nest ? nest.jobId : null, appendOnly: nest ? nest.appendOnly : null, intakePhase: nest ? nest.intakePhase : null, wantAt: num(want.at), reason: want.reason || null };
  const c = await X.call({ op: "finishClaim", id: X.sheetId, jobId, stage: "begin", nestJobId: nest ? nest.jobId : null }, "begin");
  if (!c || c.status >= 400 || !c.ok) {
    if (c && c.busy) throw new Stop("waiting", c.error || "the sheet is busy");
    throw new Stop(c && c.status === 404 ? "skipped" : "failed", (c && c.error) || "the sheet could not be taken for its files");
  }
  X.task.job.base = { layoutRev: num(c.layoutRev), contentRev: num(c.contentRev) };
  X.task.job.owedAt = num(c.filesOwed);
  // (finishNest's shortcut: a nest that changed nothing (its layout key is the record's, and the record has its files and checks): only its state is brought up to date)
  if (nest && ["gold", "silver"].includes(rec.metal) && nest.result.endedBy !== "stopped" && rec.outputs && rec.outputs.ai && rec.verification) {
    const FC = require("../../charm-nest-finish-core.js"), byId = new Map((rec.charms || []).map(ch => [ch.id, ch]));
    const keyOf = placements => FC.layoutKey({ sheetId: rec.id, placements }, byId);
    if (keyOf(nest.result.placements.map(p => Object.assign({}, p, { scale: ((rec.placements || []).find(q => q.id === p.id) || {}).scale || p.scale }))) === keyOf(rec.placements || [])) {
      X.task.job.shortcut = true;
      await X.saveTask({ job: X.task.job, step: "S7", outcome: null });
      const rejects = (nest.result.rejects || []).map(String).filter(id => byId.has(id)), status = rejects.length ? "partial" : "complete";
      // (build step 7b: the shortcut keeps finishNest's after-nest marks too (releaseFull, the gap fill, the intake marks), and what the nest turned away is moved on at S8)
      const FN = require("../../charm-nest-nest-plan.js"), sh0 = { sheetId: rec.id, metal: rec.metal, charms: rec.charms || [], placements: rec.placements || [], rejects, verification: rec.verification, topup: rec.topup ? JSON.parse(JSON.stringify(rec.topup)) : null,
        nestInitial: (rec.placements || []).slice(), feedWait: (nest.result.feedWait || []).map(String), roseProtected: !!rec.roseProtectedJson, intakePhase: nest.intakePhase || undefined, runId: rec.runId };
      const marks = await afterNestMarks(X, sh0, Object.assign({}, nest.result, { rejects }), (rec.charms || []).filter(ch => ch && !ch.excluded), { FC, NP: FN, rec, S: await require("./_charmNestFinishHost").settings(db) });
      const fw = (nest.result.feedWait || []).map(String).filter(id => byId.has(id) && !(rec.placements || []).some(p => String(p.id) === id));
      const fin = await X.call({ op: "putSheet", sheet: Object.assign({ id: X.sheetId, status, endedBy: nest.result.endedBy || "kept", trials: num(nest.result.trials), rejects, feedWait: fw.length ? fw : null, saving: false, filesOwed: null, filesJob: null, dirty: false }, marks), contentBase: num(c.contentRev), filesOwedBase: num(c.filesOwed), filesJobBase: jobId }, "shortcut");
      if (!fin || fin.status >= 400) throw new Stop("superseded", (fin && fin.error) || "the sheet changed: its state was not written");
      X.task.job.written = { layoutRev: num(c.layoutRev), contentRev: num(c.contentRev) };
      X.notes.push(fin.filesOwedKept ? "the files were asked for again meanwhile" : "no room for the new pieces: the layout and its files stand as they are");
      X.task.job.doneWhy = "the layout and its saved files stand as they are";
      return "S8";
    }
  }
  X.nest = nest;
  await X.saveTask({ job: X.task.job, step: "S1", outcome: null, why: null, runs: num(X.task.runs) + 1 });
  return "S1";
};

/** S1: the pieces (made again from the library) and the work state: the layout to write, from the nest's result or the record. */
STEP.S1 = async X => {
  await ready(X, { fresh: true });
  return "S2";
};
/** S2: the .ai and the labelled proof, into the work folder. */
STEP.S2 = async X => {
  await ready(X);
  const { sh, W, byId, st, P, FC } = X;
  const spec = FC.sheetSpec(sh, byId, st, W.result, X.specHost);
  const ai = await P.buildSheet(spec);
  sh.placements.forEach((p, i) => { p.layerName = spec.placements[i].layerName; p.layerSig = spec.placements[i].layerSig; p.name = (byId.get(p.id) || {}).name; });
  const labelled = await P.buildSheet(Object.assign({}, spec, { labelled: true, placements: spec.placements.map(p => Object.assign({}, p)) }));
  X.ai = Buffer.from(ai); X.labelled = Buffer.from(labelled); X.specBase = spec;
  await X.nonceHeld();
  await putWork(X.bucket, X.dir + "/sheet.ai", X.ai, "application/illustrator");
  await putWork(X.bucket, X.dir + "/labelled.pdf", X.labelled, "application/pdf");
  W.writes = num(W.writes) + 1;
  await saveW(X);
  return "S3";
};
/** S3: the geometry check (the page's geomCheck: CharmNestSolver.verify on the pieces' silhouettes). */
STEP.S3 = async X => {
  await ready(X);
  const g = geomCheck(X, false);
  X.W.geom = strip(g);
  await saveW(X);
  return "S4";
};
/** S4: the written file re-rendered, layer by layer. */
STEP.S4 = async X => {
  await ready(X, { files: true });
  const r = await renderCheck(X);
  X.W.render = strip(r); X.render = r;
  await saveAreas(X);
  await saveW(X);
  return "S5";
};
/** S5: OVERLAPFIX (at most three rounds, each written and checked again), then EXACTOVERLAP (what it changes is checked again). */
STEP.S5 = async X => {
  await ready(X, { files: true });
  const { W, sh, FC } = X;
  W.ovf = W.ovf || { round: 0, done: false, touched: [], before: null };
  if (!W.ovf.done) {
    const F = require("../../charm-nest-overlap-fix.js"), geom = W.geom, render = X.render || (X.render = await renderCheck(X));
    if (!sh.roseCutAt && F && typeof F.plan === "function" && W.ovf.round < 3 && render && FC.overlapOnly(render, geom) && render.areas && render.page) {
      const res = render.res || verifyRes(X), job = X.buildJob(sh), locked = new Set(((job.protectedRose && job.protectedRose.placements) || []).map(p => p.id));
      for (const id of FC.exchangeHeld(sh, X.byId, W.result)) locked.add(id);
      const kept = new Set((sh.nestInitial || []).map(p => p.id)), saved = new Map((sh.nestInitial || []).map(p => [p.id, p]));
      if (!W.ovf.before) W.ovf.before = sh.placements.map(p => ({ id: p.id, cxPt: p.cxPt, cyPt: p.cyPt, scale: p.scale || 1 }));
      const gm = geomCheck(X, true);
      const step = FC.overlapRound(sh, { F, render, gm, res, byId: X.byId, locked, kept, isFrozen: c => X.CustomPool.isFrozen(c) });
      if (step.error) { X.notes.push("overlap fix could not plan: " + step.error.message); W.ovf.done = true; }
      else if (!step.plan || !step.plan.fixes || !step.plan.fixes.length) { X.notes.push("overlap fix: no nudge or shrink within the limits clears it" + (step.plan && step.plan.why ? ` (${step.plan.why})` : "")); W.ovf.done = true; }
      else {
        const done = new Map(); FC.applyOverlapFixes(step.plan, step.byLayer, saved, done, res);
        W.ovf.touched = [...new Set(W.ovf.touched.concat([...done.keys()]))]; W.ovf.round++;
        syncW(X); await saveW(X);
        return "S2";   // (the file again, and both checks again)
      }
    } else W.ovf.done = true;
    if (W.ovf.touched.length) {
      const before = new Map((W.ovf.before || []).map(b => [b.id, b])), done = new Map(sh.placements.filter(p => W.ovf.touched.includes(p.id)).map(p => [p.id, p]));
      const moved = FC.overlapMoved(done, before, X.byId), r = X.render || {};
      sh.overlapFix = W.overlapFix = { at: Date.now(), rounds: W.ovf.round, ok: !!(r.ok && W.geom && W.geom.ok), charms: moved };
      X.notes.push(`overlap fixed automatically: ${moved.map(m => `${m.name} ${m.shrinkPct > 0 ? `${m.shrinkPct}% smaller and ` : ""}moved ${m.movedPt} pt`).join("; ")}`);
    }
    syncW(X); await saveW(X);
  }
  // EXACTOVERLAP (EXACTALWAYS: every write; a green-line sheet only before its line is cut)
  const greenLine = ["rose", "gold10k", "gold14k"].includes(sh.metal);
  if (!W.exactDone && (greenLine ? !sh.roseCutAt : true)) {
    W.exactDone = true;
    try {
      const xf = await FC.exactFix(sh, { byId: X.byId, st: X.st, result: W.result, aiBytes: new Uint8Array(X.ai), labelledBytes: new Uint8Array(X.labelled) }, X.exactHost);
      W.exactOverlap = sh.exactOverlap || null;
      if (xf && xf.changed) {
        X.ai = Buffer.from(xf.aiBytes); X.labelled = Buffer.from(xf.labelledBytes); X.specBase = xf.specBase;
        sh.placements.forEach((p, i) => { p.layerSig = xf.specBase.placements[i].layerSig; });
        await X.nonceHeld();
        await putWork(X.bucket, X.dir + "/sheet.ai", X.ai, "application/illustrator");
        await putWork(X.bucket, X.dir + "/labelled.pdf", X.labelled, "application/pdf");
        W.recheck = true; syncW(X); await saveW(X);
        return "S3";   // (both checks again on what it changed; S5 then finds both passes done)
      }
    } catch (e) { X.notes.push("exact overlap check could not run: " + e.message); W.exactOverlap = sh.exactOverlap = { at: Date.now(), ok: false, error: String(e.message || e) }; }
    syncW(X); await saveW(X);
  }
  return "S6";
};
/** S6: the verdict (OVNOHOLD), the custom pieces read back, the report and the preview. */
STEP.S6 = async X => {
  await ready(X, { files: true });
  const { W, sh, FC } = X, geom = W.geom || { ok: false, error: "the geometry check did not run" }, render = W.render || null;
  const onlyOverlapLeft = (r, g) => !!r && !!g && !r.error && !g.error && !r.outsidePx && !r.cornerPx && !(r.emptyLayers || []).length && !g.outsidePx && !g.cornerPx && !g.removedPx && !g.protectedMoved;
  const overlapLeft = !(geom.ok && render && render.ok) && onlyOverlapLeft(render, geom), exactLeft = !!(sh.exactOverlap && sh.exactOverlap.ok === false && !sh.exactOverlap.error);
  sh.verification = W.verification = { geom, render, ok: (geom.ok && !!(render && render.ok)) || overlapLeft, overlapLeft: overlapLeft || exactLeft, minGapPt: geom.minGapPt, minEdgePt: geom.minEdgePt, exact: sh.exactOverlap || null };
  // (CUSTOMPOOL: a custom design sent at its exact size is read back from the written file; one written at another size or with other parts stops the save, loudly)
  const spec = X.specBase || FC.sheetSpec(sh, X.byId, X.st, W.result, X.specHost);
  const cp = await X.CustomPool.checkWritten(sh, new Uint8Array(X.ai), spec);
  if (cp && !cp.ok) throw new Stop("failed", cp.why || "a custom piece is not written as designed");
  const items = sh.charms.filter(c => !c.excluded);
  const report = FC.buildReport(sh, W.result, items, { stockFor: (m, s) => X.Host.stockFor(m, s, X.S), labelOf: m => X.Host.labelOf(m), shapeSignature: its => X.Host.shapeSignature(its, X.S), now: () => Date.now() });
  const png = X.Render.previewPng(sh, X.st);
  await X.nonceHeld();
  await putWork(X.bucket, X.dir + "/report.json", JSON.stringify(report, null, 1), "application/json");
  if (png) await putWork(X.bucket, X.dir + "/preview.png", png, "image/png");
  W.preview = !!png; W.allPlaced = (sh.rejects || []).length === 0; W.totalMs = sh.totalMs = Date.now() - num(X.task.job && X.task.job.startedAt);
  await saveW(X);
  return "S7";
};
/** S7: the record, the claim, the files, the record completed (each part saved before the next: W.save). */
STEP.S7 = async X => {
  await ready(X);
  const { W, sh, FC, bucket } = X, job = X.task.job, rec = X.rec;
  W.save = W.save || { phase: "record" };
  const names = await pathsOf(X), files = [["ai", "sheet.ai", `${names.base}/${names.name}.ai`, "application/illustrator"], ["labelled", "labelled.pdf", `${names.base}/${names.name}_labelled.pdf`, "application/pdf"],
    ["report", "report.json", `${names.base}/${names.name}_nest-report.json`, "application/json"], ...(W.preview ? [["preview", "preview.png", `${names.base}/preview.png`, "image/png"]] : [])];
  if (!W.save.tokens) { W.save.tokens = {}; for (const [k, , path] of files) W.save.tokens[k] = (await existingToken(bucket.file(path))) || newToken(); W.save.paths = Object.fromEntries(files.map(([k, , path]) => [k, path])); await saveW(X); }
  const outputs = { ai: null, pdf: null, labelled: null, report: null, preview: null };
  for (const [k] of files) outputs[k] = { path: W.save.paths[k], url: urlOf(bucket, W.save.paths[k], W.save.tokens[k]) };
  // (membership, P4: worked out with membershipOf and SETSHUT's held rule / KEEPINSET, never sent: the assembly unit acts on a difference)
  if (!job.membership) job.membership = await membershipCheck(X);
  if (W.save.phase === "record") {
    const reportJson = await getWork(bucket, X.dir + "/report.json"); if (!reportJson) throw new Error("the report is not in the work folder: it is made again");
    sh.outputs = { report: JSON.parse(reportJson.toString("utf8")) };
    const member = { draft: !!rec.draft, setId: rec.setId || null, setSeq: rec.setSeq || null, sheetIndex: rec.sheetIndex || null };
    const full = FC.record(sh, { items: sh.charms.filter(c => !c.excluded), byId: X.byId, day: names.day, name: names.name, member, shapes: new Map(), sheetName: () => rec.metalLabel || X.Host.labelOf(rec.metal), sourceOf: () => null }, null);
    const sheet = { id: X.sheetId, metal: rec.metal, runId: rec.runId || null, saving: true, dirty: !!W.dirty, outputs };
    for (const k of WRITER_FIELDS) if (full[k] !== undefined) sheet[k] = full[k];
    sheet.status = W.allPlaced && sh.verification.ok ? "complete" : "partial";
    if (!rec.fileBase) { sheet.fileBase = names.name; sheet.folder = names.name; sheet.day = names.day; }   // (a sheet with no file name yet takes the one its files are written under; an existing one is never changed)
    // (build step 7b, C4: a nest's write carries finishNest's after-nest marks: releaseFull (sheetFull, then the gap fill), the gap fill's state, intakeOptimized / intakeFinalized, and the later
    //  orders still waiting their turn (feedWait); a sheet released full stays released (RELEASEMARK); a sheet the oldest order stopped is let go of that stop once it places pieces again)
    if (job.nestJobId) {
      const items = sh.charms.filter(c => !c.excluded), placedIds = new Set(sh.placements.map(p => String(p.id)));
      Object.assign(sheet, await afterNestMarks(X, Object.assign(sh, { intakePhase: job.intakePhase || undefined, topup: rec.topup ? JSON.parse(JSON.stringify(rec.topup)) : null }), Object.assign({}, W.result, { rejects: (W.result && W.result.rejects) || sh.rejects }), items));
      const fw = ((W.result && W.result.feedWait) || []).map(String).filter(id => X.byId.has(id) && !placedIds.has(id));
      sheet.feedWait = fw.length ? fw : null;
    }
    if (rec.problemStop && sh.placements.length) { sheet.problem = null; sheet.problemStop = null; }
    const ans = await X.call({ op: "putSheet", sheet, layoutBase: job.base.layoutRev, contentBase: job.base.contentRev, filesJobBase: job.jobId }, "record");
    if (!ans || ans.status >= 400 || !ans.ok) {
      if (ans && (ans.conflict || ans.status === 409)) throw new Stop("superseded", (ans && ans.error) || "the sheet changed since its files were made: they are not written");
      throw new Stop("failed", (ans && ans.error) || "the sheet's record was not written");
    }
    job.written = { layoutRev: num(ans.layoutRev), contentRev: job.base.contentRev + 1 };   // (the record carried the placements: putSheet raised layoutRev once and contentRev once)
    job.key = FC.layoutKey(sh, X.byId);
    W.save.phase = "claim"; await saveW(X); await X.saveTask({ job });
  }
  if (W.save.phase === "claim") {
    const c = await X.call({ op: "finishClaim", id: X.sheetId, jobId: job.jobId, stage: "files", layoutRev: job.written.layoutRev, contentRev: job.written.contentRev }, "claim");
    if (!c || c.status >= 400 || !c.ok) throw new Stop("superseded", (c && c.error) || "the sheet changed since its record was written: its files are not written");
    job.owedAt = num(c.owedAt); W.save.phase = "copy"; await saveW(X); await X.saveTask({ job });
  }
  if (W.save.phase === "copy") {
    // (each final file written in ONE save that carries its kept token, as op_put does: a link already stored never stops working, not even for a moment)
    for (const [k, work, path, type] of files) {
      const buf = await getWork(bucket, X.dir + "/" + work); if (!buf) throw new Error(`${work} is not in the work folder: the job writes it again`);
      await X.nonceHeld();
      await bucket.file(path).save(buf, { resumable: false, contentType: type, metadata: { cacheControl: "public, max-age=31536000", metadata: { firebaseStorageDownloadTokens: W.save.tokens[k], uploadedBy: "charmNestSheet-background" } } });
    }
    W.save.phase = "final"; await saveW(X);
  }
  if (W.save.phase === "final") {
    const fin = await X.call({ op: "putSheet", sheet: { id: X.sheetId, saving: false, filesOwed: null, filesJob: null }, contentBase: job.written.contentRev, filesOwedBase: job.owedAt, filesJobBase: job.jobId }, "final");
    if (!fin || fin.status >= 400 || !fin.ok) {
      // (the files are at their paths and the record names them; only the clear was refused: they stay owed, and the driver asks again)
      throw new Stop("superseded", (fin && fin.error) || "the sheet changed as its files were written: they stay owed");
    }
    if (fin.filesOwedKept) X.notes.push("the files were asked for again while they were written: they stay owed");
    W.save.phase = "after"; await saveW(X);
  }
  if (W.save.phase === "after") {
    // a final sheet's .pdf, copied from its .ai in the bucket (persistSheet: released full or its intake finished); it never fails the save
    if (rec.releaseFull || rec.intakeFinalized) { const r = await X.call({ op: "sheetPdf", id: X.sheetId }, "pdf").catch(e => { if (e && e.fenced) throw e; return null; }); if (!r || r.status >= 400) X.notes.push("sheet .pdf not made yet: " + ((r && r.error) || "no answer")); }
    // the calibration row: only a sheet that filled teaches the fill estimate (full for release, or one that turned a charm away)
    try {
      const rep = JSON.parse(((await getWork(bucket, X.dir + "/report.json")) || Buffer.from("{}")).toString("utf8")), cal = rep.calibration || {};
      if (cal.density > 0 && cal.count > 0 && (rec.releaseFull || cal.placedAll === false)) { const r = await X.call({ op: "putCalibration", row: Object.assign({ sheetId: X.sheetId, metal: rec.metal, clearancePt: +X.S.settings.clearancePt }, cal) }, "calibration"); if (!r || r.status >= 400) X.notes.push("calibration not recorded: " + ((r && r.error) || "no answer")); }
    } catch (e) { if (e && e.fenced) throw e; X.notes.push("calibration not recorded: " + e.message); }
  }
  job.doneWhy = `written: ${files.map(f => f[2].split("/").pop()).join(", ")}`;
  return "S8";
};

/** S8 (build step 7b, C4): once the files are written, what finishNest did after its save: (b) what the nest turned away goes on to the next sheet (overflowToNextSheet), (c) the later
 *  orders waiting their turn get it (feedOn), (d) the oldest order that cannot fit on an empty sheet is cut in two, or stops the run with those words. Each write is the writer's own
 *  fenced call under this job's unit, so a replay is written once. Only a nest's write moves pieces on: a sheet written as it stands keeps what it has. */
STEP.S8 = async X => {
  const { db } = X, job = X.task.job, end = async why => { await endJob(X, "done", [job.doneWhy || "written", why].filter(Boolean).join(" · ")); return "end"; };
  if (!job.nestJobId) {
    // (mcWriteStandalone's last step: a sheet its set lists, written again after a change by hand, an artwork edit or a take-off, says its set changed after it was released, as the page's
    //  Review.inPlaceRefile says it; RELEASEMARK: only a set that lists the sheet)
    const r0 = await readSheet(db, X.sheetId);
    if (r0 && r0.setId && !r0.draft && !r0.archived && !(num(r0.laserDoneAt) > 0)) {
      let listed = false; try { const ss = await db.collection(SETS).doc(String(r0.setId)).get(); listed = ss.exists && ((ss.data() || {}).sheetIds || []).includes(r0.id); } catch (_) { listed = false; }
      if (listed) {
        const set = `Set ${r0.setSeq || ""}`.trim(), nm = r0.fileBase || r0.metalLabel || r0.id;
        const r = await X.call({ op: "putSheet", sheet: { id: r0.id, releasedChange: { setId: r0.setId, set, at: Date.now(), by: "Cloud", orders: [], word: `${set} changed after it was released: ${nm} — its files are written again` } } }, "released-change");
        if (!r || r.status >= 400) X.notes.push("its set was not told it changed: " + ((r && r.error) || "no answer"));
      }
    }
    return end("");
  }
  const rec = await readSheet(db, X.sheetId);
  if (!rec || rec.archived || num(rec.laserDoneAt) > 0 || rec.roseCutAt || !rec.runId || rec.endedBy === "stopped") return end("");
  if (job.written && (num(rec.layoutRev) !== num(job.written.layoutRev) || num(rec.contentRev) !== num(job.written.contentRev))) return end("the sheet changed after its files were written: what it turned away waits for its next nest");
  const Nest = require("./_charmNestNest"), NP = require("../../charm-nest-nest-plan.js"), FC = require("../../charm-nest-finish-core.js"), O = require("../../charm-nest-orders.js"), Place = require("../../charm-nest-place.js");
  const S = await Nest.readSettings(db), view = Object.assign({}, rec, { sheetId: rec.id }), name = rec.fileBase || rec.metalLabel || rec.id, said = [];
  const env = { maxFill: +S.settings.maxFill || 0.8, orderKey: NP.orderKey, orderRank: c => NP.orderRank(c, O), inflatedArea: c => Nest.inflatedArea(S, c), usablePt2: Nest.usableArea(S, view), groupOf: Nest.groupOf,
    NS: S.settings.nospread !== "off" ? require("../../charm-nest-nospread.js") : null, topupRoom: v => Nest.topupRoom(v, false) };
  const feed = FC.feedNext(view, env);
  const rejects = new Set((rec.rejects || []).map(r => String(r && typeof r === "object" ? r.id : r))); for (const id of feed.pass || []) rejects.add(String(id));
  const plan = FC.overflowPlan(Object.assign({}, view, { rejects: [...rejects] }), env);
  if (plan.kind === "stop") {
    // (d) the oldest order cannot fit on an empty sheet at these settings: its pieces stay on the sheet, and the run stops with those words (the nest step reads problemStop once; after a Resume the sheet is held)
    const r = await X.call({ op: "putSheet", sheet: { id: rec.id, problem: plan.why, problemStop: { at: Date.now(), why: plan.why, by: "cloud" }, status: "partial" } }, "stop");
    if (!r || r.status >= 400) X.notes.push("the stop was not recorded on the sheet: " + ((r && r.error) || "no answer"));
    return end(plan.why);
  }
  if (plan.kind === "rechunk") {
    // (NOSPREAD: the oldest unit is cut in two and the sheet nests again, in order)
    const charms = (rec.charms || []).map(c => (c && plan.chunks[String(c.id)] ? Object.assign({}, c, { chunk: plan.chunks[String(c.id)] }) : c));
    const r = await X.call({ op: "putSheet", sheet: { id: rec.id, charms, rejects: [], problem: null, status: "ready", dirty: true, appendOnly: false, intakeAppend: false }, layoutBase: num(rec.layoutRev), contentBase: num(rec.contentRev) }, "rechunk");
    if (r && r.ok && r.status < 400) { const n = await X.call({ op: "nestStart", sheetId: rec.id, auto: true, why: plan.words }, "rechunk-nest"); if (!n || n.status >= 400) X.notes.push("its nest was not asked: " + ((n && n.error) || "no answer")); said.push(plan.words); }
    else X.notes.push("the order was not cut: " + ((r && r.error) || "no answer"));
    return end(said.join(" · "));
  }
  if (plan.kind === "move") {
    // (b) the next sheet in line (charm-nest-place.js nextSheet: NOSPREAD, PAIR / PAIR2, ONEFILL / PAIRFIX), or a new draft, takes them in one transaction and nests once
    const world = await require("./_charmNestPlaceUnit").readWorld({ db, runId: rec.runId, read: X.read });
    const rs = await db.collection("Charm_Nest_Runs").doc(String(rec.runId)).get(), run = Object.assign({ runId: rec.runId }, rs.exists ? rs.data() || {} : {});
    const H = Place.recordHost({ run, records: world.records, sets: world.sets, settings: world.settings, preset: world.preset, partial: world.partial });
    const me = H.views().find(v => v.sheetId === rec.id) || H.refresh(rec), ids = new Set(plan.moving.map(c => String(c.id))), movingV = (me.charms || []).filter(c => ids.has(String(c.id)));
    const next = H.rules.nextSheet(me, movingV.length ? movingV : plan.moving, { orderKey: NP.orderKey, admitted: (p, units) => Nest.planEnv(S).nospreadAdmitted(p, units) });
    const fresh = () => ({ create: { metal: rec.metal, page: next && next._new ? next.page : Math.max(0, ...H.views().filter(v => v.metal === rec.metal).map(v => num(v.page))) + 1, day: run.day || null, greenLeftover: next && next._new ? next.greenLeftover || null : null } });
    const body = { op: "finishOverflow", id: rec.id, runId: rec.runId, layoutBase: num(rec.layoutRev), contentBase: num(rec.contentRev), pieceIds: [...ids], why: `turned away by ${name}'s nest`,
      to: !next || next._new || !next.sheetId ? fresh() : { sheetId: next.sheetId, layoutBase: num(next.layoutRev), contentBase: num(next.contentRev) } };
    let r = await X.call(body, "overflow");
    // (the sheet chosen changed or shut meanwhile: a new draft takes them, as the page opens one when the next sheet is shut or busy)
    if (r && r.status === 409 && r.conflict && body.to.sheetId && !/changed since its pieces were turned away/.test(String(r.error || ""))) r = await X.call(Object.assign({}, body, { to: fresh() }), "overflow-new");
    if (r && r.ok && r.moved) {
      said.push(`${r.moved} piece(s) moved on to ${r.to.name || "a new draft"}`);
      const n = await X.call({ op: "nestStart", sheetId: r.to.sheetId, auto: true, why: `${r.moved} piece(s) moved on from ${name}` }, "nest-next");
      if (!n || n.status >= 400) X.notes.push("the next sheet's nest was not asked: " + ((n && n.error) || "no answer"));
    } else if (r && r.status >= 400) X.notes.push("what the nest turned away was not moved on: " + (r.error || r.status));
  }
  // (c) feedOn: the later orders waiting their turn on this sheet get it, once
  if (feed.act === "nest" || feed.act === "pass+nest") {
    const n = await X.call({ op: "nestStart", sheetId: rec.id, auto: true, why: `${name}: the next orders waiting their turn` }, "feed");
    if (n && n.status < 400) said.push("its next orders take their turn"); else X.notes.push("its next turn was not asked: " + ((n && n.error) || "no answer"));
  }
  return end(said.join(" · "));
};

/** finishNest's after-nest marks on a nest's write (build step 7b, C4): releaseFull (finish-core sheetFull, then topupSettle: the gap fill), the gap fill's state, intakeOptimized /
 *  intakeFinalized. A sheet released full stays released whatever the nest says (RELEASEMARK: only a person reopens it, and putSheet keeps the mark too). The gap fill waits for later
 *  orders only on the open Auto run (live). → the record's fields */
async function afterNestMarks(X, sh, result, items, o = {}) {
  const FC = o.FC || X.FC, NP = o.NP || X.NP, rec = o.rec || X.rec, S = o.S || X.S, Nest = require("./_charmNestNest"), O = require("../../charm-nest-orders.js");
  let run = null; if (rec.runId) { try { const rs = await X.db.collection("Charm_Nest_Runs").doc(String(rec.runId)).get(); run = rs.exists ? rs.data() || {} : null; } catch (_) { run = null; } }
  const live = !!run && run.mode === "auto" && !["complete", "abandoned", "archived"].includes(String(run.status || ""));
  // (NOSPREAD's turn-away (sh._nsCheck / _nsPassed) counts only in the one extra nest the page's nospreadSettle asks after a quiet stretch; a write here is never that nest, so it is not read)
  sh._nsCheck = false;
  const env = { maxFill: +S.settings.maxFill || 0.8, orderKey: NP.orderKey, orderRank: c => NP.orderRank(c, O), inflatedArea: c => Nest.inflatedArea(S, c), live, manual: !!run && run.mode === "manual", topupOff: S.settings.topup === "off",
    now: () => Date.now(), log: (s, w) => X.notes.push(w) };
  const full = FC.topupSettle(sh, result, FC.sheetFull(sh, result, items, env), env) || !!rec.releaseFull;
  return Object.assign({ releaseFull: !!full, topup: sh.topup || null }, sh.releaseWhy ? { releaseWhy: sh.releaseWhy } : {}, FC.intakeMarks(sh, result, items));
}
/* The fields of finish-core's record() the writer sends (persistSheet's record, less what is not the writer's: membership (P4), the sheet's name, day and folder once it has one,
   its pieces' list, orders and listings (they change only with its pieces), its label, backs, release marks (build step 7b) and the page's own marks). */
const WRITER_FIELDS = ["status", "endedBy", "trials", "searchMetrics", "elapsedMs", "totalMs", "stock", "params", "density", "freePt2", "usablePt2", "pocket", "charmCount", "placedCount", "rejectCount", "verification", "placements", "rejects", "names"];

/* ════ the context an invocation builds once ════ */
/** The modules, the record, the settings, the pieces and the work state; o.fresh: S1 (the layout made from the nest or the record), o.files: the written files read back. */
async function ready(X, o = {}) {
  if (!X.FC) {
    X.Host = require("./_charmNestFinishHost"); X.Render = require("./_charmNestFinishRender");
    const pdf = require("./_charmNestPdf"); pdf.withCanvas({ backend: process.env.CHARM_NEST_CANVAS || "auto" }); X.P = pdf.CharmNestPDF;
    X.FC = require("../../charm-nest-finish-core.js"); X.NP = require("../../charm-nest-nest-plan.js"); X.Solver = require("./_charmNestSolver"); X.O = require("../../charm-nest-orders.js");
    X.CustomPool = require("../../charm-nest-custom-pool.js"); X.Vector = require("../../charm-nest-vector.js"); X.Export = require("../../charm-nest-export.js");
    let Rose = null; try { Rose = require("../../charm-nest-rose.js"); } catch (_) { Rose = null; } X.Rose = Rose;
  }
  if (!X.rec) X.rec = await readSheet(X.db, X.sheetId);
  if (!X.rec) throw new Stop("skipped", "The sheet is no longer in the Library");
  if (!X.S) X.S = await X.Host.settings(X.db);
  const job = X.task.job; X.dir = workDir(X.sheetId, job.jobId);
  if (!X.sh) {
    const pace = (() => { let at = Date.now(); return async () => { await new Promise(r => setImmediate(r)); if (Date.now() - at > 5000) { X.live(); at = Date.now(); } }; })();
    const made = await X.Host.makePieces({ db: X.db, bucket: X.bucket, rec: X.rec, S: X.S, read: X.read, log: X.log, pace });
    if (made.missing.length) throw new Stop("failed", `${made.missing.length === 1 ? "A piece" : made.missing.length + " pieces"} of ${X.rec.fileBase || X.sheetId} could not be made again from the library: ${made.missing.slice(0, 4).join("; ")}${made.missing.length > 4 ? "…" : ""}`);
    X.sources = made.sources; X.byId = new Map(made.charms.map(c => [c.id, c]));
    X.sh = sheetOf(X, made.charms);
    // (a Rose Gold / 10K / 14K sheet's stock: its size and the shape left of it, which the geometry check and the corners read; the page's sh.roseStock)
    if (X.rec.roseStockId) { try { const rs = await X.db.collection("Charm_Nest_Rose_Stock").doc(String(X.rec.roseStockId)).get(), d = rs.exists ? rs.data() || {} : null; if (d && +d.wPt > 0 && +d.hPt > 0) X.sh.roseStock = { id: rs.id, revision: d.revision || 0, profileJson: d.profileJson || null, wPt: +d.wPt, hPt: +d.hPt }; } catch (_) { /* its record's own size stands */ } }
    X.st = X.Host.stockFor(X.rec.metal, X.sh, X.S);
    X.specHost = { sourceOf: id => X.sources[id], labelOf: m => X.Host.labelOf(m), charmScale: X.Host.CHARM_SCALE };
    X.buildJob = s => X.NP.buildJob(s, { settings: X.S.settings, stockFor: (m, z) => X.Host.stockFor(m, z, X.S), cornerPt: z => X.Host.cornerPtSetting(z, X.S), angleSet: () => angleSet(X.S), carefulAngles: X.NP.CAREFUL_ANGLES,
      nestItems: z => (z.charms || []).filter(c => !c.excluded), orderRank: c => X.NP.orderRank(c, X.O), Solver: X.Solver, Rose: X.Rose, cores: 4 });
    X.exactHost = { buildJob: s => X.buildJob(s), insetFor: s => X.Host.insetFor(s, X.S), isFrozen: c => X.CustomPool.isFrozen(c), Vector: X.Vector, Export: X.Export, PDF: X.P,
      sheetSpec: (s, byId, st, result) => X.FC.sheetSpec(s, byId, st, result, X.specHost), buildSheet: spec => X.P.buildSheet(spec), progress: () => {}, log: (s, msg) => { (s.log || (s.log = [])).push(msg); X.notes.push(msg); }, agent: (who, kind, text) => X.notes.push(text), now: () => Date.now() };
    // the work state: made at S1, read back from the work folder after it
    const saved = o.fresh ? null : await getWork(X.bucket, X.dir + "/state.json");
    if (saved) X.W = JSON.parse(saved.toString("utf8"));
    else if (o.fresh || job.step === "S1" || job.step === "S2") { X.W = await layoutOf(X); await saveW(X); }
    else throw new Error("the job's work state is not in the work folder: the job starts again");
    applyW(X);
  }
  if (o.files && !X.ai) {
    const [ai, labelled] = await Promise.all([getWork(X.bucket, X.dir + "/sheet.ai"), getWork(X.bucket, X.dir + "/labelled.pdf")]);
    if (!ai || !labelled) throw new Error("the written files are not in the work folder: they are written again");
    X.ai = ai; X.labelled = labelled;
  }
  if (o.files && !X.areas) X.areas = await loadAreas(X);
}
const angleSet = S => { const st = [2, 5, 10].includes(+S.settings.angleStep) ? +S.settings.angleStep : 10; const a = []; for (let d = 0; d < 360; d += st) a.push(d); return a; };
const verifyRes = X => +X.S.settings.verifyRes || 6;
async function readSheet(db, id) { const s = await db.collection(SHEETS).doc(String(id)).get(); return s.exists ? Object.assign({ id: s.id }, s.data()) : null; }

/** The sheet as finish-core reads a page's sheet, made from its record and its pieces. */
function sheetOf(X, charms) {
  const rec = X.rec; let roseProtected = null, roseStock = null;
  // (the page's sh.roseProtected, as it reads it from a saved sheet: charm-nest-rose-ui.js, the record's roseProtectedJson)
  try { roseProtected = rec.roseProtectedJson ? JSON.parse(rec.roseProtectedJson) : null; } catch (_) { roseProtected = null; }
  if (rec.roseStock && rec.roseStock.wPt) roseStock = rec.roseStock;
  return { sheetId: rec.id, metal: rec.metal, runId: rec.runId || null, charms, placements: [], rejects: [], nestInitial: null, roseProtected, roseStock, recordStock: rec.stock || null,
    roseCutAt: rec.roseCutAt || null, laserDoneAt: rec.laserDoneAt || null, topup: rec.topup || null, appendOnly: false, page: rec.page || 1, seq: rec.setSeq || null, setId: rec.setId || null, draft: !!rec.draft,
    sheetIndex: rec.sheetIndex || null, fileBase: rec.fileBase || null, releaseFull: !!rec.releaseFull, intakeFinalized: !!rec.intakeFinalized, intakeOptimized: !!rec.intakeOptimized, intakeOptimizedCount: rec.intakeOptimizedCount || 0,
    keepInSet: rec.keepInSet || null, ownerSet: rec.ownerSet || null, cardStartedAt: rec.cardStartedAt || null, draftOrder: rec.draftOrder, missRearranged: !!rec.missRearranged, backPool: [], backOutputs: null, label: null,
    bestRevision: num(rec.bestRevision), bestHistory: [], log: [], dirty: false, status: "finishing" };
}
/** S1's layout: the nest's result applied as finishNest applies it, or the record's layout as it stands. → the work state W. */
async function layoutOf(X) {
  const rec = X.rec, job = X.task.job, sh = X.sh, byId = X.byId, NP = X.NP, num0 = v => num(v);
  const norm = p => ({ id: String(p.id), cxPt: +p.cxPt, cyPt: +p.cyPt, angle: +p.angle || 0, scale: +p.scale > 0 ? +p.scale : 1, wPt: num0(p.wPt), hPt: num0(p.hPt), ...(+p.fixScale > 0 && +p.fixScale < 1 ? { fixScale: +p.fixScale } : {}) });
  let nest = X.nest || null;
  if (!nest && job.nestJobId) { const js = await X.db.collection(JOBS).doc(job.nestJobId).get(), j = js.exists ? js.data() || {} : null, r = j && (j.result || j.best); if (r && Array.isArray(r.placements)) nest = { jobId: job.nestJobId, result: r, appendOnly: job.appendOnly }; }
  if (!nest) {
    // (the record as it stands: written again with the pieces where they lie, writeAsPlaced / mcWriteStandalone)
    const placements = (rec.placements || []).filter(p => p && byId.has(String(p.id))).map(norm), rejects = (rec.rejects || []).map(r => String(r && r.id != null ? r.id : r)).filter(id => byId.has(id));
    return { placements, rejects, nestInitial: placements.map(p => Object.assign({}, p)), dirty: false,
      result: { params: rec.params || {}, endedBy: "kept", trials: num0(rec.trials), elapsedMs: num0(rec.elapsedMs), density: num0(rec.density), usablePt2: num0(rec.usablePt2), freePt2: num0(rec.freePt2), placedPt2: Math.max(0, num0(rec.usablePt2) - num0(rec.freePt2)), pocket: rec.pocket || null, metrics: rec.searchMetrics || null } };
  }
  const r = nest.result;
  let placements = (r.placements || []).map(norm), rejects = (r.rejects || []).map(String);
  // (a piece taken off the sheet while it was nested is not written: its place stays free and the sheet is arranged again)
  const gone = placements.filter(p => !byId.has(p.id)).length + rejects.filter(id => !byId.has(id)).length;
  placements = placements.filter(p => byId.has(p.id)); rejects = rejects.filter(id => byId.has(id));
  if (gone) X.notes.push(`${gone === 1 ? "A piece" : gone + " pieces"} left the sheet while it was nested; the sheet is arranged again next`);
  sh.placements = placements; sh.rejects = rejects; sh.nestInitial = (rec.placements || []).filter(p => p && byId.has(String(p.id))).map(norm);
  try { NP.keepOrdersWhole(sh, byId, new Set(r.pocketFilled || []), { orderKey: NP.orderKey, orderRank: c => NP.orderRank(c, X.O), groupOf: c => (c && c.groupKey && +c.groupSize > 1 ? String(c.groupKey) : null), agent: (who, kind, text) => X.notes.push(text) }); }
  catch (e) { X.notes.push("orders kept whole: " + e.message); }
  // (the 2.9 % shrink of every charm written, about its own centre; a piece already on the sheet keeps the size it was written at when the nest appended (appendOnly), a protected Rose Gold one its own; a
  //  piece resized by hand at that size; a custom design at 100 %)
  const was = new Map((rec.placements || []).map(p => [String(p.id), p])), prot = new Map(((sh.roseProtected && sh.roseProtected.placements) || []).map(p => [String(p.id), +p.scale]));
  const same = (a, b) => !!b && Math.abs(a.cxPt - +b.cxPt) < 1e-3 && Math.abs(a.cyPt - +b.cyPt) < 1e-3 && Math.abs(a.angle - (+b.angle || 0)) < 1e-6;
  for (const p of sh.placements) {
    const c = byId.get(p.id), w = was.get(p.id), m = NP.manualSize(c), keep = prot.get(p.id) || ((nest.appendOnly === true || (nest.appendOnly == null && same(p, w))) && w && +w.scale > 0 ? +w.scale : 0);
    p.scale = keep || (m !== 1 ? +(X.Host.CHARM_SCALE * m).toFixed(5) : X.Host.CHARM_SCALE); delete p.fixScale;
  }
  X.CustomPool.holdSize(sh.placements, byId, sh);
  const ids = new Set(sh.placements.map(p => p.id));
  sh.nestInitial = sh.placements.filter(p => was.has(p.id)).map(p => Object.assign({}, p));   // (the saved layout as this write holds it: the checks and the fixes keep it where it lies)
  return { placements: sh.placements, rejects: sh.rejects.filter(id => !ids.has(id)), nestInitial: sh.nestInitial, dirty: gone > 0,
    result: { params: r.params || {}, endedBy: r.endedBy || "complete", trials: num0(r.trials), elapsedMs: num0(r.elapsedMs), density: num0(r.density), usablePt2: num0(r.usablePt2), freePt2: num0(r.freePt2), placedPt2: num0(r.placedPt2), pocket: r.pocket || null, metrics: r.metrics || null, pocketFilled: r.pocketFilled || [],
      // (build step 7b: what sheetFull and topupSettle read of a nest's result, and the later orders it left waiting their turn, feedWait)
      rejects: (r.rejects || []).map(String), feedWait: (r.feedWait || []).map(String), ...(r.smallRoom === false || r.smallRoom === true ? { smallRoom: r.smallRoom } : {}), ...(r.timedOut ? { timedOut: true, noRoom: (r.noRoom || []).map(String), capped: (r.capped || []).map(String) } : {}) } };
}
/** The work state onto the sheet (placements, rejects, saved layout, the checks so far). */
function applyW(X) {
  const { sh, W } = X;
  sh.placements = (W.placements || []).map(p => Object.assign({}, p)); sh.rejects = (W.rejects || []).slice(); sh.nestInitial = W.nestInitial ? W.nestInitial.map(p => Object.assign({}, p)) : null;
  sh.dirty = !!W.dirty; sh.exactOverlap = W.exactOverlap || null; sh.overlapFix = W.overlapFix || null; sh.verification = W.verification || null;
  const r = W.result || {};
  sh.endedBy = r.endedBy; sh.trials = r.trials; sh.solveParams = r.params || {}; sh.density = r.density; sh.elapsedMs = r.elapsedMs; sh.searchMetrics = r.metrics || null;
  sh.liveInfo = { freePt2: num(r.freePt2), usablePt2: num(r.usablePt2), placedPt2: num(r.placedPt2), pocket: r.pocket || null, placed: sh.placements.length, total: sh.charms.length };
  sh.totalMs = W.totalMs != null ? W.totalMs : Date.now() - num(X.task.job && X.task.job.startedAt);   // (fixed at S6: a record written again after a crash is the same body, and the ledger answers it once)
}
/** The sheet's placements back into the work state (after a fix moved or shrank some). */
function syncW(X) { X.W.placements = X.sh.placements.map(p => Object.assign({}, p)); if (X.sh.nestInitial) X.W.nestInitial = X.sh.nestInitial.map(p => Object.assign({}, p)); }
async function saveW(X) { syncW(X); await X.nonceHeld(); await putWork(X.bucket, X.dir + "/state.json", JSON.stringify(X.W), "application/json"); }
async function saveAreas(X) {
  if (!X.areas) return;
  const out = {}; for (const [k, a] of X.areas) out[k] = X.Render.packArea(a);
  await X.nonceHeld(); await putWork(X.bucket, X.dir + "/areas.json.gz", zlib.gzipSync(Buffer.from(JSON.stringify(out))), "application/gzip");
}
async function loadAreas(X) {
  const m = new Map(), b = await getWork(X.bucket, X.dir + "/areas.json.gz"); if (!b) return m;
  try { const o = JSON.parse(zlib.gunzipSync(b).toString("utf8")); for (const [k, v] of Object.entries(o)) m.set(k, X.Render.unpackArea(v)); } catch (_) { /* rendered again */ }
  return m;
}
/** What a check answers, without its masks and areas (the record and the work state keep it small). */
function strip(r) { if (!r) return r; const o = Object.assign({}, r); delete o.masks; delete o.areas; delete o.page; return JSON.parse(JSON.stringify(o)); }

/** The geometry check (the page's geomCheck: its worker's CharmNestSolver.verify), with the masks the overlap fix plans on when asked. */
function geomCheck(X, masks) {
  try { return X.Solver.verify(X.buildJob(X.sh), X.sh.placements, verifyRes(X), undefined, masks ? { masks: true } : undefined); }
  catch (e) { return { ok: false, error: String((e && e.message) || e), overlapPx: 0, outsidePx: 0, overlappingPairs: [] }; }
}
/** The render check (the page's renderCheck, node as the rasteriser): each layer's area kept by its layerSig across rounds and invocations. */
async function renderCheck(X) {
  const sh = X.sh, st = X.st, clr = +X.S.settings.clearancePt || 0;
  let corners = []; try { corners = X.Solver.cornerTriangles ? X.Solver.cornerTriangles(X.Host.cornerSheet(sh, st, X.S)) : []; } catch (_) { corners = []; }
  X.areas = X.areas || new Map();
  try {
    return await X.Render.renderCheck(new Uint8Array(X.ai), { wPt: st.wPt, hPt: st.hPt, insetPt: X.Host.insetFor(sh, X.S), res: verifyRes(X), erodePt: clr < 0 ? Math.round(-clr / 2 * 2) / 2 : 0, corners,
      layerSigs: new Map(sh.placements.map(p => [p.layerName, p.layerSig])), cache: X.areas, pace: async () => { await new Promise(r => setImmediate(r)); X.live(); } });
  } catch (e) { if (e && (e.fenced || e.handed)) throw e; X.areas = new Map(); return { ok: false, error: String((e && e.message) || e) }; }
}

/** Where the files go (persistSheet's rule: a sheet in a set under its set's folder, by the SET's day (NAMECLASH); one outside a set keeps the folder it was first saved in;
 *  a new one under charmnest/sheets/<day>/). A name another sheet of a set already has stops the job (it would write over that sheet's files). */
async function pathsOf(X) {
  if (X.paths) return X.paths;
  const rec = X.rec, O = X.O, tag = X.Host.METAL_TAG[rec.metal] || String(rec.metal || "").toUpperCase();
  let set = null; if (rec.setId) { try { const s = await X.db.collection(SETS).doc(String(rec.setId)).get(); set = s.exists ? s.data() || {} : null; } catch (_) { set = null; } }
  // (RELEASEMARK, Paul 20 Nov 2026: GF Draft 1 and 2, drafts that named Set 5, had their files under Set 5's folder although Set 5 never listed them): a sheet is filed under its set only when it is
  //  in it (not a draft) and the set's record lists it; every other sheet is filed as a sheet of its own day
  const filed = !!rec.setId && !rec.draft && !!set && (set.sheetIds || []).includes(rec.id);
  const setDay = (filed && set.day) || rec.day || torontoDay(Date.now());
  // (build step 7b: finish-core sheetFileBase, the page's rule: a draft, or a sheet owed its number, is `${TAG}_working_${id}`; an existing file name is never changed)
  const name = rec.fileBase || X.FC.sheetFileBase({ metal: rec.metal, sheetId: rec.id, draft: !!rec.draft, numberOwed: !!rec.numberOwed, setDay, seq: rec.setSeq || 1, sheetIndex: rec.sheetIndex, page: rec.page }, { tag: () => tag, sheetName: O.sheetName, today: () => setDay });
  const aiPath = rec.outputs && rec.outputs.ai && rec.outputs.ai.path, folder = aiPath ? aiPath.replace(/\/[^/]+$/, "") : "";
  const kept = !filed && /^charmnest\/sheets\/\d{4}-\d{2}-\d{2}\//.test(folder) && folder.endsWith("/" + name) ? folder : null;
  const base = filed ? `${O.setFolder(setDay, rec.setSeq)}/${name}` : kept || `charmnest/sheets/${rec.day || setDay}/${name}`;
  if (filed) {
    const q = await X.db.collection(SHEETS).where("fileBase", "==", name).where("archived", "==", false).limit(5).get();
    const other = q.docs.find(d => d.id !== rec.id && (d.data() || {}).setId && !(d.data() || {}).draft);
    if (other) throw new Stop("failed", `${name} is also the name of another sheet of a set (${other.id}): writing it would write over that sheet's files. It was not written; the assembly renames it`);
  }
  return (X.paths = { base, name, day: filed ? setDay : rec.day || setDay });
}
/** membershipOf (finish-core: NOPARTIALSET, with SETSHUT's held rule; KEEPINSET through CharmNestSetRules.isPartial) on the record, with the run's sets. Never sent (P4): a difference
 *  is kept on the task for the assembly unit to act on. → { own, wanted, differs, why } */
async function membershipCheck(X) {
  const rec = X.rec, SetRules = require("../../charm-nest-set-rules.js");
  let sets = []; if (rec.runId) { try { const q = await X.db.collection(SETS).where("runId", "==", rec.runId).get(); sets = q.docs.map(d => Object.assign({ setId: d.id }, d.data())); } catch (_) { sets = []; } }
  const shm = Object.assign({}, rec, { sheetId: rec.id, seq: rec.setSeq, placements: X.sh.placements });
  const wanted = X.FC.membershipOf(shm, { SetRules, setsOfRun: () => sets, modern: () => true }), own = { draft: !!rec.draft, setId: rec.setId || null, setSeq: rec.setSeq || null, sheetIndex: rec.sheetIndex || null };
  const differs = wanted.draft !== own.draft || (wanted.setId || null) !== own.setId;
  return { own, wanted, differs, why: differs ? "NOPARTIALSET: a partial sheet its set does not list is a draft; the writer changes no membership (P4): the assembly unit drafts it" : null, at: Date.now() };
}

module.exports = { run, carries, STEPS, STEP, WRITER_FIELDS, workDir, FINISH, BUDGET_MS, afterNestMarks, pathsOf };
