/*  netlify/functions/_charmNestDrive.js — THE CLOUD DRIVER (cloud plan v2 §1, build step 2: the driver with stub steps). DORMANT: while Charm_Nest_Open/drive is absent or
 *  in mode "off" the cron reads that one document and does nothing else, and a pass that is started anyway refuses at its lease and writes nothing.
 *
 *  ONE driver moves a run, and no browser takes part (it replaces RunCtl, charm-nest-bridge.js: loopNow, doStep, stop, resume, autoResume, poke). A pass:
 *    begin   the lease (Charm_Nest_Drive/lease, _charmNestLease.js) and, in the same transaction, the mode and the run's carrier (§1.3): the cloud writes the carrier
 *            only when nobody carries the run, the cloud already does, or the computer that did has not been seen for 90 s (standby); in "on" the carrier is the
 *            cloud's already (the drain). A live computer carrying the run: the pass writes nothing for it and ends with { yield }.
 *    control the run's presses (runControl): stop, resume, next, commit, abandon; then the cloud acknowledges them.
 *    commands a request from a page (Charm_Nest_Run_Cmds) the cloud cannot do yet is answered at once, in words (a request never waits silently).
 *    resume  a stopped run is taken up again only by the rules of §1.8: never an Etsy watchdog or budget stop; an Etsy (429) stop only after Resume Etsy, by the page's
 *            follow rule; nothing at all while the Etsy switch is on or cannot be read; a passing failure 1, 2, 5 and 10 minutes after it (AUTO_WAIT).
 *    steps   the run's current step, unit by unit, each saved before the next starts (the cursor), each bounded by time; a step the cloud does not do yet (not in
 *            drive.cloudSteps) parks the run with words. Step 2 ships every step as a stub (no unit does any work): the real units come in build steps 3 to 13.
 *  The driver never calls Etsy, never builds the world, never dispatches the world's lease and never passes force or sweep (E1). It writes only through Lib.call
 *  (the cloud's own calls, fenced on this pass's nonce and the run's carrier) and its own lease and state documents; Lib.cloudOps() is its only op surface (E5). */
"use strict";
const O = require("../../charm-nest-orders.js");
const Lease = require("./_charmNestLease");
const Standby = require("./_charmNestStandby");   // (build steps 8 and 15: what the cloud owns, the park's words, the reconcile of a run taken from a closed computer, the hand-over)
const TakeOff = require("./_charmNestTakeOff");   // (build step 9: the take-off lane and the revalidate step)

const DRIVE = "Charm_Nest_Drive", OPEN = "Charm_Nest_Open", CARRIER = "Charm_Nest_Run_Carrier", RUNS = "Charm_Nest_Runs", CMDS = "Charm_Nest_Run_Cmds";
const LEASE_PATH = DRIVE + "/lease";
const CLOUD_DEV = "cloud", CLOUD_BY = "Cloud";
const COMPUTER_FRESH_MS = 90000, CLOUD_FRESH_MS = 180000, DISPATCH_STALE_MS = 2 * 60000;
const UNIT_BUDGET_MS = 45000, UNIT_HARD_MS = 60000, PASS_TAIL_MS = 60000, RENEW_MS = 20000, BUDGET_MS = 12 * 60000;
const AUTO_WAIT = [60000, 120000, 300000, 600000], RETRY_IN_PASS = [4000, 8000];
const RUN_STEPS = O.RUN_STEPS;
const LANES = ["files", "takeoff", "engrave"];   // (and "cmd:<type>": §1.5)
/** Whether a pass item's lane is the cloud's: the lane itself in drive.cloudSteps, or for "<prefix>:*" any lane of that prefix ("cmd:*": any "cmd:<type>"). (build step 13) */
const laneOn = (cloudSteps, l) => (/:\*$/.test(String(l)) ? cloudSteps.some(s => String(s).startsWith(String(l).slice(0, -1))) : cloudSteps.includes(String(l)));
/* What a stopped run's words say it is (the page's kindOf, bridge ~17768), with Etsy's causes first (E8): an Etsy watchdog or a spent call budget is never taken up by the
   cloud, and a 429 / etsy_stopped is an "etsy" stop that follows Resume Etsy only. PASSING is the page's (bridge 17766), plus an answer the cloud's own call says is in flight. */
const PASSING = /answer in time|timed out|network|Failed to fetch|HTTP 5\d\d|link is down|no reply|closed|Reconnect (to|before)|frame is not open|is being written already|ECONNRESET|ETIMEDOUT|socket hang up/i;
const ARRIVAL_STOP = /^New orders could not be added:/, ETSY_CAUSE = /\b429\b|etsy_stopped|rate.?limit/i, PARKED = /^The cloud parked this run/;
const REVIEW_STOP = /needs a look|shape analysis failed|a back flip failed its checks|^Sheet options changed|^Sheet settings changed/i;
function kindOf(why) {
  why = String(why || "");
  if (/^Etsy watchdog/.test(why)) return "watchdog";
  if (/^Etsy call budget reached/.test(why)) return "budget";
  if (ETSY_CAUSE.test(why)) return "etsy";
  if (why === "stopped by the operator") return "operator";
  if (why === "Workspace restored after refresh") return "restored";
  if (PARKED.test(why)) return "parked";
  if (REVIEW_STOP.test(why)) return "review";
  if (/\bsign/i.test(why)) return "auth";
  if (PASSING.test(why) || /stopped answering the heartbeat/.test(why)) return "transient";
  return "error";
}
const ETSY_KINDS = new Set(["watchdog", "budget", "etsy"]);
/** The stop's kind as kept (an Etsy kind is never read back as another, E8 e), else read from its words. */
function stopKindOf(r) {
  if (!r || r.status !== "stopped") return null;
  const said = kindOf(r.stoppedBy);
  if (ETSY_KINDS.has(said)) return said;
  if (r.stopKind && ETSY_KINDS.has(r.stopKind)) return r.stopKind;
  return r.stopKind && !(r.stopKind === "error" && PASSING.test(r.stoppedBy || "")) ? r.stopKind : said;
}
const stoppedAt = r => { const e = (r.errors || []).filter(x => x && x.why === r.stoppedBy).pop(); return e ? +e.t || 0 : 0; };
/** §1.8 c: whether the cloud takes a stopped run up again by itself. c: { runMode, stop: { ok, stopped, resumedAt }, cloudSteps } */
function autoResumable(r, c) {
  if (!r || r.status !== "stopped") return false;
  const k = stopKindOf(r), stop = c && c.stop;
  if (k === "watchdog" || k === "budget") return false;                       // (only a person's Resume clears these; the page heartbeat's resume of a budget stop is not ported)
  if (!stop || !stop.ok || stop.stopped) return false;                        // (no stop of any kind while Etsy's switch is on or cannot be read)
  if (k === "etsy") return +stop.resumedAt > stoppedAt(r);                    // (the page's follow rule, charm-nest-1.html etsyStopped / follow)
  if (k === "parked") return !!(c.cloudSteps && c.cloudSteps.includes(r.step));
  if (c.runMode !== "auto" && !(k === "transient" && ARRIVAL_STOP.test(r.stoppedBy || ""))) return false;
  return k === "transient" || k === "restored";
}
/** The Etsy switch read strictly (§1.8 a5): a read that fails is { ok: false } (a hold), never "not stopped" (_etsyStop.state() is not used: it answers its cache when the read fails). */
async function stopRead(db) {
  try { const s = await db.doc(require("./_etsyStop").DOC).get(), d = s.exists ? s.data() || {} : {}; return { ok: true, stopped: d.stopped === true, resumedAt: +d.resumedAt || 0, since: +d.since || 0 }; }
  catch (e) { return { ok: false, why: String((e && e.message) || e).slice(0, 160) }; }
}
const ms = v => (v && v.toMillis ? v.toMillis() : +v || 0);
/* §1.8 a: the Etsy freshness gate. Revalidate, labels and commit go ahead only when the mail system's copy of Etsy's receipts synced well after the step started, the
   world was built from that copy, none of the receipts concerned waits to be read, Etsy's daily limit is not set and the switch reads "not stopped". A step declares it
   (gate: true); every failing condition is said in words. Read by the driver itself, at the moment of the check. */
async function freshnessGate(db, o = {}) {
  const at = +o.stepStartedAt || 0, why = [];
  const [mirror, summary, sync] = await Promise.all([db.doc("EtsyMail_Config/receiptsMirrorState").get(), db.collection("Charm_World").doc("summary").get(), db.doc("EtsyMail_Config/syncState").get()]).catch(() => [null, null, null]);
  const m = mirror && mirror.exists ? mirror.data() || {} : {}, s = summary && summary.exists ? summary.data() || {} : {}, y = sync && sync.exists ? sync.data() || {} : {}, done = ms(m.lastSyncCompletedAt);
  if (!mirror) why.push("the mail system's copy of Etsy's receipts could not be read");
  else if (m.lastSyncOutcome !== "ok" || m.lastSyncCutByClock === true) why.push(`Waiting for the mail system's copy of Etsy's receipts: its last sync was ${m.lastSyncOutcome || "not finished"}`);
  else if (!(done > at)) why.push("Waiting for the mail system's next copy of Etsy's receipts");
  if (!(ms(s.mirrorBefore) >= done && done > 0)) why.push("Waiting for the world to be built from that copy");
  const toRead = new Set((s.toReadIds || []).map(String));
  if (Array.isArray(o.receipts) ? o.receipts.some(id => toRead.has(String(id))) : +s.toRead > 0) why.push("A receipt of these orders is still to be read");
  if (ms(y.etsyDailyLimitResetAt) > Date.now()) why.push("Etsy's daily limit for the shop is reached");
  const st = await stopRead(db);
  if (!st.ok) why.push("Etsy's switch could not be read: the step waits");
  else if (st.stopped) why.push("Etsy is stopped for the whole shop: this step waits for Resume Etsy");
  return { ok: !why.length, why, hold: !st.ok || st.stopped || ms(y.etsyDailyLimitResetAt) > Date.now() };
}

/* ── the run's record (RunCtl recordOf / sizeCheck / archiveFinished / updatePending, from the documents, never from a page's memory) ── */
const RECORD_FIELDS = ["status", "step", "errors", "stoppedBy", "stopKind", "fix", "note", "setIds", "setId", "processingSignature", "processingComplete", "finishedAt", "pendingWork", "nothingToCut", "commitRequested", "awaitCommit", "claimedAt", "abandonedAt", "sheetHolds"];
/** What the driver saves of a run: its scalar fields only (never its lines: they change only through runLinesAdd / runLinesPatch), the last fifty errors, the signature as a hash. */
function recordOf(r) {
  const rec = { runId: r.runId };
  for (const k of RECORD_FIELDS) if (r[k] !== undefined) rec[k] = r[k];
  if (Array.isArray(rec.errors)) rec.errors = rec.errors.slice(-50);
  if (typeof rec.processingSignature === "string" && rec.processingSignature.length > 64) rec.processingSignature = O.textHash(rec.processingSignature);
  return rec;
}
/** Said once a run: a record past 70% of what one document holds (RunCtl sizeCheck). → null or { share, text } */
function sizeCheck(rec) {
  const bare = Object.assign({}, rec); delete bare.lines;
  const bytes = O.utf8Bytes(JSON.stringify(bare)), entries = O.indexEntries(bare), L = O.RUN_RECORD, share = Math.max(bytes / L.bytes, entries / L.entries);
  return share < L.warn ? null : { share, text: `The online record of run ${rec.runId} is ${Math.round(share * 100)}% full (${Math.round(bytes / 1024)} KB of 1,024 KB; ${entries} of 40,000 index entries). Finish or skip some of the orders waiting in this run before it fills.` };
}
/** The finished orders' lines the archive does not hold in their current form (RunCtl archiveFinished): written as content-named parts through runArchive. */
async function archiveFinished(r, lines, put) {
  const done = Object.assign({}, r.archivedLines || {}), hashes = new Map();
  for (const keys of O.closedOrders(lines).values()) for (const k of keys) hashes.set(k, O.textHash(JSON.stringify(lines[k])));
  const due = [...hashes].filter(([k, h]) => done[k] !== h).map(([k]) => [k, lines[k]]);
  if (!due.length) return { parts: 0, done };
  const parts = O.archiveParts(due);
  for (let i = 0; i < parts.length; i += 4) {
    const batch = parts.slice(i, i + 4);
    const res = await put({ op: "runArchive", runId: r.runId, parts: batch.map(p => ({ json: p.json })) }, "archive|" + batch.map(p => O.textHash(p.json)).join(","));
    if (!res || res.status >= 400) throw new Error("the finished orders could not be archived: " + ((res && res.error) || "no answer"));
    for (const p of batch) for (const k of p.keys) done[k] = hashes.get(k);
  }
  return { parts: parts.length, done };
}
/** RunCtl updatePending, from the documents: what still waits in the run (orders, sheets, Review, engraving, sets, commit). */
function updatePending(r, d = {}) {
  const lines = Object.values(r.lines || {}).filter(Boolean), pages = (d.sheets || []).filter(s => s && !s.archived && (s.charms || []).length);
  r.pendingWork = { orders: lines.filter(l => ["held", "waiting", "pulled", "pooled"].includes(l.state)).length, sheets: pages.filter(p => p.runHold || !p.outputs || !(p.verification && p.verification.ok) || p.dirty || p.filesOwed).length,
    review: +d.review || 0, engraving: lines.filter(l => l.engrave && l.engrave.needed && !l.engrave.approved && !O.FINISHED_LINE.has(l.state)).length, sets: Object.values(r.deferredSets || {}).filter(Boolean).length, commit: !!r.awaitCommit };   // (a set that waits no more is kept as null: a merge cannot take a key away)
  return r.pendingWork;
}
const hasPending = r => Object.values(r.pendingWork || {}).some(Boolean);
/** The state of the work as a short hash (RunCtl workSignature), from the documents: a resting run whose signature moved goes through its steps again (poke). */
function workSignature(r, d = {}) {
  const rows = Object.entries(r.lines || {}).filter(([, l]) => l && !O.FINISHED_LINE.has(l.state)).map(([k, l]) => [k, l.state, l.hold || null, !!l.changePending, l.engrave ? l.engrave.state || null : null, l.engrave ? !!l.engrave.approved : null]).sort();
  const sheets = (d.sheets || []).filter(s => s && !s.archived).map(s => [s.id, s.metal, s.status || null, !!s.dirty, (s.placements || []).length, s.filesOwed || null, s.contentRev || 0]).sort();
  return O.textHash(JSON.stringify([r.membershipRevision || 0, rows, sheets]));
}

/* ── the steps (§1.7). Step 2: every step is a stub with one unit that does nothing (the real units arrive in build steps 3 to 13). A step is a list of units; a unit is
   async (ctx) → undefined (done, next unit) | { more: true } (run me again) | { goto: step } | { wait: kind, why } | { stop: { why, fix, kind } } | { done: true }. ── */
const stubStep = () => Object.assign([async () => undefined], { stub: true });   // (.stub: a step that is not built is never "owned": the run parks at it with words, Standby.owns)
/* The real units of a step, registered by the build step that makes them (a step not listed here is still a stub). Each runs only while its step is in drive.cloudSteps. */
const STEP_UNITS = {};
Object.assign(STEP_UNITS, TakeOff.STEPS);   // (build step 9: revalidate, read from the world behind the freshness gate)
/* build step 4 (cloud plan v2): new orders join the run and become pool pieces in the cloud: the run's "pull" and "pool" steps, and the intake that opens its "checkpoint", are the
   intake's units (_charmNestIntake.js); the pass's own intake runs beside the steps (pass, below). Each runs only for a step drive.cloudSteps names. */
const Intake = require("./_charmNestIntake");
/** Every step: a stub, with the steps built so far in their place (STEP_UNITS; build step 4: pull / pool / checkpoint, _charmNestIntake.js; build step 6: nest, _charmNestNestStep.js;
 *  build step 5: placement first in nest, withUnits). */
function stubSteps() { const out = {}; for (const s of RUN_STEPS) out[s] = STEP_UNITS[s] ? STEP_UNITS[s].slice() : stubStep(); return withUnits(Object.assign(out, Intake.steps(), require("./_charmNestNestStep").steps())); }
/* The real units, over the stubs, as each build step adds them (§1.7). Build step 5: "nest: placement, then nestStart": every ready piece goes on its sheet record first
   (_charmNestPlaceUnit.js; placeOnSheet, CLOUD_ONLY). Its placeAll is also the unit a checkpoint's intake calls after it pools (§1.6 item 6). */
function withUnits(out) {
  out.nest = [require("./_charmNestPlaceUnit").unit].concat(out.nest || []);
  return out;
}
/* The steps and lanes the driver really has: the cron (tick), the background pass and Settings (driveStatus, driveSet) all read these, so what Settings says the cloud
   does is what it does. A build step wires its units in here (a step: a list of units; a lane: true once built). */
function defaultSteps() { return Object.assign({}, require("./_charmNestSteps").steps); }   // (stubSteps, with the unit modules of _charmNestSteps.js over them: hard-parts §0.1)
function defaultLanes() {
  const out = { takeoff: true };   // (build step 9: the take-off lane, TakeOff.lane in the pass)
  for (const it of require("./_charmNestSteps").passItems || []) if (it && it.lane) out[String(it.lane)] = true;   // (the pass items' lanes, from the unit modules: build steps 10, 11, 13)
  const wild = Object.keys(out).filter(k => /:\*$/.test(k)).map(k => k.slice(0, -1));   // ("cmd:*" → "cmd:")
  for (const l of Standby.LANES || []) if (wild.some(p => String(l).startsWith(p))) out[l] = true;   // (a wildcard lane builds every lane of its kind: "cmd:*", every "cmd:<type>", build step 13)
  return out;
}

const docAt = (db, p) => { const seg = String(p).split("/"); return db.collection(seg[0]).doc(seg[1]); };
const num = v => (Number.isFinite(+v) ? +v : 0);
class Fenced extends Error { constructor(why) { super(why || "fenced"); this.fenced = true; } }
class Yield extends Error { constructor(why) { super(why || "yield"); this.yielded = true; } }

/** Inside begin's and renew's transaction (§1.3): the mode and the run's carrier. Reads first, then answers the writes (the carrier keep-alive) for the lease to make. */
async function carrierCheck(db, tx, o = {}) {
  const now = Date.now(), dv = await tx.get(db.collection(OPEN).doc("drive")), drive = dv.exists ? dv.data() || {} : null, mode = drive && (drive.mode === "standby" || drive.mode === "on") ? drive.mode : "off";
  if (mode === "off") return { ok: false, why: "the drive is off" };
  let runId = o.runId || null;
  if (!runId) {
    const ov = await tx.get(db.collection(OPEN).doc("run")); runId = ov.exists ? (ov.data() || {}).runId || null : null;
    // (the run named open may have ended since: then no run is open, and nothing is carried)
    if (runId) { const rs = await tx.get(db.collection(RUNS).doc(runId)), r = rs.exists ? rs.data() || {} : null; if (!r || ["complete", "abandoned", "archived"].includes(String(r.status || "")) || r.retiredInto) runId = null; }
  }
  if (!runId) return { ok: true, info: { mode, drive, runId: null } };
  const cref = db.collection(CARRIER).doc(runId), cs = await tx.get(cref), cd = cs.exists ? cs.data() || {} : {}, c = cd.carrier || null;
  const cloud = !!(c && c.dev === CLOUD_DEV), computerAlive = !!(c && c.dev && !cloud && now - num(c.at) < COMPUTER_FRESH_MS);
  if (o.mustCarry && !cloud) return { ok: false, why: `${(c && c.by) || "a computer"} carries the run now`, handedBack: true };
  if (computerAlive || (mode === "on" && c && c.dev && !cloud)) return { ok: true, info: { mode, drive, runId, yield: true, carrier: c } };
  // (build step 8: while the cloud owns no step of a run yet, listed and built, it takes no run: a run it could only park stays where it is, for a computer's Resume as today)
  if (!cloud && typeof o.ownsRun === "function" && !o.ownsRun(Array.isArray(drive.cloudSteps) ? drive.cloudSteps.map(String) : [])) return { ok: true, info: { mode, drive, runId, yield: true, notYet: true, carrier: c } };
  if (cloud && cd.yieldTo && cd.yieldTo.dev && num(cd.yieldTo.at) >= num(c.since) && now - num(cd.yieldTo.at) < 10 * 60000) return { ok: true, info: { mode, drive, runId, carries: true, yieldTo: cd.yieldTo, carrier: c } };
  const next = { dev: CLOUD_DEV, by: CLOUD_BY, at: now, since: cloud ? num(c.since) || now : now, ...(!cloud && c && c.dev ? { took: { dev: c.dev, by: c.by || "", at: num(c.at) } } : {}) };
  return { ok: true, writes: [[cref, { runId, carrier: next, expireAt: new Date(now + 30 * 86400000) }, { merge: true }]], info: { mode, drive, runId, carries: true, carrier: next, took: !cloud } };
}

/** One pass (§1.6). env: { db, Lib, nonce, budgetMs?, steps?, sleep?, renewMs?, log? }. → { ok, again?, yield?, fenced?, refused?, ... } */
async function pass(env) {
  const { db, Lib } = env, say = env.log || (() => {}), sleep = env.sleep || (t => new Promise(r => setTimeout(r, t)));
  const steps = env.steps || defaultSteps(), lanes = env.lanes || defaultLanes(), leaseRef = docAt(db, LEASE_PATH), stateRef = db.collection(DRIVE).doc("state");
  const startedAt = Date.now(), budgetEnd = startedAt + (env.budgetMs || BUDGET_MS);
  const ownsRun = list => Standby.owned(list, steps, lanes).some(s => RUN_STEPS.includes(s));   // (build step 8: the cloud takes a run only when it does at least one of its steps)
  const began = await Lease.begin(db, leaseRef, env.nonce, { heartbeat: true, check: tx => carrierCheck(db, tx, { ownsRun }) });
  if (!began) return { ok: false, skipped: "not this pass's turn" };
  if (began.refused) { await Lease.end(db, leaseRef, env.nonce, { refused: began.why }).catch(() => {}); return { ok: false, refused: began.why }; }
  const info = began.info || {}, nonce = env.nonce, drive = info.drive || {}, cloudSteps = Array.isArray(drive.cloudSteps) ? drive.cloudSteps.map(String) : [];
  const ownedSteps = Standby.owned(cloudSteps, steps, lanes), owns = s => Standby.owns(s, cloudSteps, steps, lanes);   // (listed in drive.cloudSteps AND built: build step 8)
  const out = { ok: true, mode: info.mode, runId: info.runId || null, units: 0, writes: 0 };
  let timer = null, lost = false, handed = null, yieldAsked = null;   // (lost: another pass holds the lease; handed: a person took the run back, or the drive was turned off; yieldAsked: a person's Resume waits for the step to end)
  const end = async (result, error) => { if (timer) clearInterval(timer); const e = await Lease.end(db, leaseRef, nonce, result, error).catch(() => ({ again: false })); out.again = out.again || !!e.again; return out; };
  /* (build steps 8 and 15) a change of mode the owner asked for comes first, whoever carries the run: the hand-over to "on" (the carrying computer's idle report, then
     one transaction that flips the mode and takes the carrier), or the way back (a run the cloud carries is read and parked first, below). No unit runs while it waits. */
  const handBack = !!(drive.pending && drive.pending.to && drive.pending.to !== "on");
  if (drive.pending && drive.pending.to) {
    const m = await Standby.modeChange({ db, drive, info, phase: "begin", steps, lanes, say }).catch(e => ({ ended: true, result: { error: String((e && e.message) || e).slice(0, 200) } }));
    if (m && m.ended) { out.pending = drive.pending.to; out.modeChange = m.result; out.again = out.again || !!m.again; return end(Object.assign({ modeChange: true }, m.result)); }
  }
  if (info.yield) {
    out.yield = true; out.why = info.notYet ? "the cloud does no step of a run yet: it takes no run" : `${(info.carrier && info.carrier.by) || "a computer"} carries the run`;
    // (m:C7, build step 10a: while a computer carries the run, the cloud plans what it would do and keeps only its own notes; only when drive.shadow names a plan)
    if (env.shadow && !info.notYet && info.runId && Array.isArray(drive.shadow) && drive.shadow.length) { try { out.shadow = await env.shadow({ db, runId: info.runId, drive }); await db.collection(DRIVE).doc("state").set({ shadowAt: Date.now() }, { merge: true }); } catch (e) { say("[drive] shadow: " + ((e && e.message) || e)); } }
    return end({ yield: true });
  }
  if (info.yieldTo && !handBack) { out.yield = true; out.why = `${info.yieldTo.by || "a computer"} asked for the run back: the cloud starts nothing new`; return end({ yield: true, to: info.yieldTo.dev }); }
  const renewMs = env.renewMs == null ? RENEW_MS : env.renewMs;   // (the beat every 20 s, beside the one each unit's save makes: a long unit never starves it)
  // (a person's Resume that asks for the run back while this pass works, yieldTo: the pass ends at the next unit's boundary, never in the middle of one, and starts nothing new)
  if (renewMs > 0) timer = setInterval(() => { Lease.renew(db, leaseRef, nonce, { check: tx => carrierCheck(db, tx, { runId: info.runId, mustCarry: !!info.runId }) }).then(r => { if (r && r.lost) lost = true; else if (!r || !r.ok) handed = (r && r.why) || "the run was handed back"; else if (r.info && r.info.yieldTo) yieldAsked = r.info.yieldTo; }).catch(() => {}); }, renewMs);
  if (timer && typeof timer.unref === "function") timer.unref();
  const live = () => { if (lost) throw new Fenced("another pass holds the lease"); if (handed) throw new Yield(handed); };
  try {
    const stateSnap = await stateRef.get(), state = stateSnap.exists ? stateSnap.data() || {} : {};
    // the cloud's own calls: fenced on this pass's lease and the run's carrier, each with its unit's key (the mid)
    const fence = { path: LEASE_PATH, nonce, runId: info.runId || null };
    const call = async (body, unit) => {
      live();
      const res = await Lib.call(body, { unit: String(unit || ""), fence });
      if (res && res.fenced) throw new Fenced(res.error);
      if (res && res.status === 409 && res.notCarrier) throw new Yield(res.error);
      if (res && res.inFlight) throw new Error(String(res.error || "is being written already"));
      if (res && !OP_READS.has(body.op) && res.status < 400 && !res.replayed) out.writes++;
      return res;
    };
    const read = body => Lib.call(body, {});
    const saveState = patch => Lease.renew(db, leaseRef, nonce, { check: async tx => { const c = await carrierCheck(db, tx, { runId: info.runId, mustCarry: !!info.runId && info.carries }); return c.ok ? { ok: true, writes: (c.writes || []).concat([[stateRef, Object.assign({}, patch, { at: Date.now() }), { merge: true }]]) } : c; } }).then(r => { if (!r || !r.ok) throw r && r.lost ? new Fenced("the lease was lost") : new Yield((r && r.why) || "the run was handed back"); if (r.info && r.info.yieldTo) yieldAsked = r.info.yieldTo; });

    // ── no run open: in auto, the cloud opens one when there are open orders and it does the run's first step (runOpen, made by the cloud: carried by it).
    //    (build step 8: only when the cloud OWNS the pull step, listed and built: a stub never opens a run)
    let runId = info.runId;
    if (!runId) {
      out.releases = await releasesUnit(db, call, say);   // (RELEASEHOLD: a release needs no run: its marks and its timeline step are the order's)
      /* (G5, Paul 21 Nov 2026: staff keep full use of every input) the pass items that need no run ("runless": a person's label asks, each answered on its own set's / sheet's
         run's records) run here too, each only while its lane is the cloud's, bounded like a unit; a failure is said and never stops anything; a fence or a hand-back ends the pass */
      for (const item of (env.passItems || []).filter(it => it && it.runless && typeof it.unit === "function" && laneOn(cloudSteps, String(it.lane || it.name)))) {
        if (Date.now() > budgetEnd - PASS_TAIL_MS) { out.again = true; break; }
        live();
        const t0 = Date.now(), itemCall = (b, u) => { if (Date.now() - t0 > UNIT_HARD_MS) throw Object.assign(new Error("the unit ran past its time: it goes on in the next pass"), { overBudget: true }); return call(b, u); };
        try {
          const r = await item.unit({ db, run: null, runId: null, step: "pass:" + item.name, cursor: {}, call: itemCall, read, drive, cloudSteps, state, fence, live, say, unitKey: `norun|pass|${item.name}`, budgetEnd: Math.min(budgetEnd - PASS_TAIL_MS, t0 + UNIT_BUDGET_MS), gate: o => freshnessGate(db, Object.assign({ stepStartedAt: 0 }, o)), pass: true, runless: true });
          out.units++; out.items = (out.items || 0) + 1; if (r && r.more) out.again = true;
        } catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; if (e && e.overBudget) out.again = true; else { say(`[drive] ${item.name}: ${String((e && e.message) || e).slice(0, 300)}`); out.itemErrors = (out.itemErrors || []).concat([item.name]); } }
      }
      const sum =await db.collection("Charm_World").doc("summary").get(), openLines = sum.exists ? num((sum.data() || {}).live) : 0;
      if (drive.runMode === "auto" && openLines > 0 && owns("pull") && !drive.pending) {
        const made = await call({ op: "runOpen", mode: "auto", day: torontoDay(Date.now()) }, "open|" + torontoDay(Date.now()));
        out.opened = made && made.runId; say(`[drive] opened ${out.opened}`);
        out.again = true;   // (the next pass carries it: its lease reads the carrier the run was made with)
      }
      return end({ idle: !out.opened });
    }
    fence.runId = runId;
    const got = await read({ op: "runGet", runId }), run = got && got.run;
    if (!run) { out.why = "the open run is not in the cloud"; return end({ missing: runId }); }
    const before = JSON.stringify(recordOf(run));
    const saveRun = async why => { const rec = recordOf(run), now = JSON.stringify(rec); if (now === runSaved) return; await call({ op: "runPut", run: Object.assign(rec, { driveAt: Date.now() }), merge: true }, "record|" + runId + "|" + why); runSaved = now; };
    let runSaved = before;
    const stop = async (why, fix, kind) => {
      kind = kind || kindOf(why);
      run.status = "stopped"; run.stoppedBy = why; run.fix = fix || null; run.stopKind = kind; run.errors = (run.errors || []).concat([{ t: Date.now(), why }]).slice(-50);
      await saveRun("stop|" + kind + "|" + Date.now()); say(`[drive] run stopped: ${why}`);
    };

    // (build step 15: the way back the owner asked for; the run the cloud carries is parked at this boundary, and the mode flips once no task of the cloud is alive)
    if (handBack) {
      const m = await Standby.modeChange({ db, drive, info, phase: "run", run, stopRun: stop, steps, lanes, say });
      if (m && m.ended) { out.pending = drive.pending.to; out.modeChange = m.result; out.again = out.again || !!m.again; return end(Object.assign({ modeChange: true }, m.result)); }
    }
    // ── (build step 8) a run the cloud has just taken, from a computer not seen for 90 s or as the hand-over's carrier: rebuilt from the cloud only, once per take.
    //    Its stop and the stop's kind are left as they are (an Etsy stop stays one, E8); the closed computer's own copy is never used.
    if (info.carries) {
      const rc = await Standby.reconcile({ db, run, runId, carrier: info.carrier, state, call, steps, lanes, cloudSteps, saveState, say });
      if (rc) { out.reconciled = { nest: rc.nest.length, files: rc.files.length, owed: rc.owed.length, cmds: rc.cmds, pulled: rc.pulled }; run.note = String(rc.words || "").slice(0, 300); await saveRun("took|" + rc.since); }
    }
    // ── control: the run's presses, oldest first, then acknowledged
    const ctlDone = await control(run, { call, stop, saveRun, cursorJobs: (state.cursor && state.cursor.jobIds) || [] });
    if (ctlDone.acted) out.control = ctlDone.acted;
    // ── requests from pages: a type the cloud does not do yet is answered at once, in words (a request never waits silently)
    if (run.status !== "abandoned" && run.status !== "complete") out.answered = await answerCommands(db, runId, cloudSteps, call, (env.passItems || []).some(i => i && i.name === "commands"));
    // ── take-offs (§1.6 item 2, build step 9, lane "takeoff"): cancelled, shipped at our station, completed at the Design Station, accepted changes, Hold and sheet Take off
    //    presses. Before intake and placement, so a cancelled order is never placed; while the run is stopped or resting too, as the page's AutoCancel.
    if (cloudSteps.includes("takeoff") && run.status !== "abandoned" && run.status !== "complete" && Date.now() < budgetEnd - PASS_TAIL_MS) {
      out.takeoff = await TakeOff.lane({ db, run, runId, call, read, state, saveState, say, live, mode: info.mode, budgetEnd: Math.min(budgetEnd - PASS_TAIL_MS, Date.now() + UNIT_BUDGET_MS) });
      if (out.takeoff && out.takeoff.more) out.again = true;
    }
    // ── releases (§1.6 item 3, before intake; RELEASEHOLD): a Release Hold press releases the hold in the cloud at once (op releaseAsk); one a function left half done is finished here
    out.releases = await releasesUnit(db, call, say);
    // ── (NESTNOW, Paul 21 Nov 2026: a Nest press waited while the run stood at another step) a person's Nest / "Nest the rest" (nestStart from a page) is a job queued at once; it used
    //    to start only when the run reached its own "nest" step, so with the run waiting at commit or labels (the Etsy freshness gate) a person's nest never ran. Every pass now starts
    //    the oldest queued job of each free lane and keeps running ones alive (Nest.pumpLanes: one lane per run and metal, as the nest step does). Nothing new is asked here.
    if (cloudSteps.includes("nest") && run.status !== "abandoned" && run.status !== "complete" && !ctlDone.stopped && Date.now() < budgetEnd - PASS_TAIL_MS) {
      try { const NestQ = require("./_charmNestNest"), live = (await NestQ.jobsOfRun(db, runId)).filter(j => j && (j.status === "queued" || j.status === "running")); if (live.length) { await NestQ.pumpLanes(db, live, { by: "drive" }); out.nestPumped = live.length; } }
      catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; say("[drive] queued nests: " + String((e && e.message) || e).slice(0, 200)); }
    }
    // ── the intake (§1.6 item 5, build step 4): new orders join the run, and behind the gate (C5) become pool pieces, before the run's own step (_charmNestIntake.js; only for "pull" / "pool" in cloudSteps)
    if (run.status !== "abandoned" && run.status !== "complete" && !ctlDone.stopped) {
      const ik = await Intake.passIntake({ db, run, runId, drive, cloudSteps, call, read, fence, say, budgetEnd: Math.min(budgetEnd - PASS_TAIL_MS, Date.now() + UNIT_BUDGET_MS) });
      if (ik) { out.intake = ik.summary; if (ik.more) out.again = true; if (ik.stop && run.status !== "stopped") await stop(ik.stop.why, ik.stop.fix); }
    }
    // ── the files lane (build step 7a): a sheet owed its files, or whose nest ended, gets the one sheet writer (charmNestSheet-background); only while "files" is the cloud's (a stopped run's
    //    sheets too: writing a placed layout's files nests nothing and asks Etsy nothing)
    let filesLooked = false;   // (build step 7b: the lane's pass item, _charmNestSteps.js, is skipped in a pass that looked here already: one files look a pass)
    if (cloudSteps.includes("files") && run.status !== "abandoned" && run.status !== "complete") { filesLooked = true; try { const f = await require("./_charmNestFinishLane").driveUnit({ db, runId, sheets: await sheetsOf(db, runId), call: (b, u) => call(b, u), log: say }); if (f.started) out.files = f.asked; } catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; say("[drive] files: " + ((e && e.message) || e)); } }
    if (ctlDone.stopped) { await saveState({ runId, seenWorldAt: await worldAt(db), cursor: Object.assign({}, state.cursor || {}, { runId }) }); return end({ stopped: true }); }
    // ── the pass items beside the steps (§1.6 items 2-6 and 9: take-offs, releases, commands, placement, engraving, sweeps; hard-parts §0.1 passItems, _charmNestSteps.js), in their
    //    order (`order`, 50 when a module gives none), each only while its lane is the cloud's (laneOn: "cmd:*" is any "cmd:<type>"), every pass, whatever the run's step or status (a
    //    unit says itself what it waits on): bounded like a unit, each its own gate, its own time and its own saved cursor (state.items). A unit's own failure is said and never stops the
    //    run (a request answers its own); a fence or a hand-back ends the pass as it does for a step. (build steps 10, 11, 13)
    const orderOf = it => (it && Number.isFinite(+it.order) ? +it.order : 50);
    const items = (env.passItems || []).slice().sort((a, b) => orderOf(a) - orderOf(b));
    /** One pass item, bounded like a unit, with its own saved cursor (state.items). → its answer ({} for none), or false when the pass's time is spent. */
    const runItem = async item => {
      if (Date.now() > budgetEnd - PASS_TAIL_MS) { out.again = true; return false; }
      live();
      const t0 = Date.now(), itemCall = (b, u) => { if (Date.now() - t0 > UNIT_HARD_MS) throw Object.assign(new Error("the unit ran past its time: it goes on in the next pass"), { overBudget: true }); return call(b, u); };
      let r = null;
      try { r = await item.unit({ db, run, runId, step: "pass:" + item.name, cursor: Object.assign({}, (state.items || {})[item.name] || {}), call: itemCall, read, drive, cloudSteps, state, fence, live, say, unitKey: `${runId}|pass|${item.name}`, budgetEnd: Math.min(budgetEnd - PASS_TAIL_MS, t0 + UNIT_BUDGET_MS), gate: o => freshnessGate(db, Object.assign({ stepStartedAt: 0 }, o)), pass: true }); out.units++; }
      catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; if (e && e.overBudget) r = { more: true }; else { say(`[drive] ${item.name}: ${String((e && e.message) || e).slice(0, 300)}`); out.itemErrors = (out.itemErrors || []).concat([item.name]); } }
      out.items = (out.items || 0) + 1;
      if (r && r.more) out.again = true;
      const keep = Object.assign({}, r && r.state ? r.state : {}, r && r.cursor ? { items: Object.assign({}, state.items || {}, { [item.name]: r.cursor }) } : {});
      if (Object.keys(keep).length) { await saveState(Object.assign({ runId }, keep)); Object.assign(state, keep); }
      return r || {};
    };
    /* NOWCMD (Paul 21 Nov 2026, a Change SKU that said "The cloud has not started it yet: it does it at its next pass": "it must be propagated immediately not on a next pass"): a person's
       request put while this pass runs is taken up by THIS pass, within seconds: the run's requests document is read between every unit of the steps, after every pass item, and before
       the pass ends; one still "waiting" is done now through the same commands item (its claim, its fence on this pass's lease and the run's carrier, its lanes, its order: oldest first,
       one computer's presses in the order asked, each idempotent by its own keys), and one whose lane the cloud does not hold is answered at once in words (answerCommands), as at the
       pass's start. One read when nothing waits; a fence or a hand-back ends the pass as a unit's does. (driveSoon still starts a pass at once when none runs.) */
    const cmdItem = items.find(it => it && it.name === "commands" && typeof it.unit === "function");
    const cmdTried = new Map();
    const cmdNow = async where => {
      if (["abandoned", "complete"].includes(run.status) || Date.now() > budgetEnd - PASS_TAIL_MS) return 0;
      let waiting = [];
      try { const s = await db.collection(CMDS).doc(runId).get(); waiting = Object.values((s.exists && (s.data() || {}).cmds) || {}).filter(c => c && c.id && c.type && c.state === "waiting"); } catch (_) { return 0; }
      const now = Date.now(); waiting = waiting.filter(c => now - num(cmdTried.get(c.id)) > 15000);   // (one the commands item left waiting, a passing failure or another computer's live claim, is looked at again 15 s on, not before every unit)
      if (!waiting.length) return 0;
      live();
      for (const c of waiting) cmdTried.set(c.id, now);
      const ours = c => !!cmdItem && Standby.cmdLaneOn(cloudSteps, c.type);
      let n = 0;
      if (waiting.some(c => !ours(c))) { const a = await answerCommands(db, runId, cloudSteps, call, (env.passItems || []).some(i => i && i.name === "commands")); out.answered = (out.answered || 0) + a; n += a; }
      if (waiting.some(ours)) { say(`[drive] NOWCMD: ${waiting.filter(ours).length} request(s) put while the pass runs, taken up now (${where})`); const r = await runItem(cmdItem); if (r !== false) n++; }
      if (n) out.cmdNow = (out.cmdNow || []).concat([where]).slice(-20);
      return n;
    };
    for (const item of items) {
      if (!item || typeof item.unit !== "function" || !laneOn(cloudSteps, String(item.lane || item.name)) || ["abandoned", "complete"].includes(run.status)) continue;
      if (item.name === "release" && out.releases !== undefined) continue;   // (RELEASEHOLD: this pass ran the release unit already, releasesUnit above)
      if (item.name === "files" && filesLooked) continue;   // (build step 7b: the files lane ran above in this pass already)
      if ((await runItem(item)) === false) break;
      if (item !== cmdItem && (!cmdItem || orderOf(item) > orderOf(cmdItem))) await cmdNow("after " + item.name);   // (NOWCMD: after the commands item's own turn, each item that follows it)
    }
    /* (ONSWITCH) a run step's unit may run one lane's pass items again at its moment of the run, each as above (its own budget, cursor and saved state; a failure said, never a
       stop): the engrave step runs the engraving lane, as the page's step ran Engrave.background. Only a lane that is the cloud's (laneOn): never a lane a computer keeps (P9). */
    const laneNow = async name => {
      const ran = [];
      if (!laneOn(cloudSteps, String(name))) return { ran, off: true };
      for (const item of items) { if (!item || typeof item.unit !== "function" || String(item.lane || item.name) !== String(name)) continue; const r = await runItem(item); if (r === false) break; ran.push(item.name); }
      return { ran };
    };

    // ── (ONSWITCH, Paul 20 Nov 2026: "ON now") a set look at once when the cloud takes a run (from a computer that closed, or in the hand-over to "on") and when the owner turns
    //    the cloud on: the checkpoint step's set assembly (_charmNestAssembly.js over charm-nest-assembly-core.js, the owner's set rules unchanged), never waiting for new orders. A
    //    run resting (processed) goes through its steps again from the checkpoint, as a computer's poke does (the assembly; engraving, labels and commit then follow as their
    //    own units and the owner's settings say); a run past its checkpoint gets one look now; a run stopped or paused waits for its person. Once per take, per flip and per
    //    ownership of the checkpoint step (state.setLook).
    if (info.carries && !["abandoned", "complete"].includes(run.status)) {
      const lookKey = [runId, num(info.carrier && info.carrier.since), info.mode || "", num(drive.flippedAt), owns("checkpoint") ? 1 : 0].join("|");
      if (state.setLook !== lookKey) {
        let keep = {};
        if (owns("checkpoint") && run.status === "processed") {
          run.status = "running"; run.processingComplete = false; run.step = "checkpoint";
          await saveRun("look|" + lookKey);
          state.cursor = { runId, step: "checkpoint", i: 0, stepStartedAt: Date.now(), jobIds: [], sheetIds: [] }; keep = { cursor: state.cursor, retry: null };
          out.setLook = "steps"; say(`[drive] a set look at once: the resting run goes through its steps again from the checkpoint`);
        } else if (owns("checkpoint") && run.status === "running" && O.stepIndex(run.step) > O.stepIndex("checkpoint")) {
          try { const r = await require("./_charmNestAssembly").assemblyUnit({ db, run, runId, step: "checkpoint", cursor: { runId, step: "checkpoint", i: 0, stepStartedAt: Date.now() }, call, read, drive, cloudSteps, fence, say, unitKey: `${runId}|look`, budgetEnd: Math.min(budgetEnd - PASS_TAIL_MS, Date.now() + UNIT_BUDGET_MS) }); out.setLook = r && r.wait ? "deferred" : "look"; }
          catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; out.setLook = e && e.overBudget ? "later" : "failed"; if (!(e && e.overBudget)) say("[drive] set look: " + ((e && e.message) || e)); }
          await saveRun("look|" + lookKey);
        }
        if (out.setLook !== "later") { await saveState(Object.assign({ runId, setLook: lookKey }, keep)); state.setLook = lookKey; }
      }
    }

    // ── a stopped run: taken up again only by §1.8's rules
    const retry = state.retry && state.retry.runId === runId ? state.retry : null;
    if (run.status === "stopped") {
      const st = await stopRead(db), ok = autoResumable(run, { runMode: drive.runMode, stop: st, cloudSteps: ownedSteps });
      const k = stopKindOf(run), due = k !== "transient" || !retry || Date.now() >= num(retry.next);
      if (ok && due) {
        say(`[drive] resuming by itself: ${run.stoppedBy}`);
        run.status = "running"; run.stoppedBy = null; run.fix = null; delete run.stopKind; run.stopKind = null; out.resumed = true;
        await saveRun("resume|" + Date.now());
      } else {
        // (build step 8, C5: parked at the checkpoint step, the cloud keeps placing new orders: the pool and nest steps' units, once a pass, when the cloud owns them)
        const pw = await Standby.parkedWork({ run, steps, lanes, cloudSteps, say, ctxFor: (step, i) => { const cur = { runId, step, i, stepStartedAt: Date.now(), parked: true }; return { db, run, runId, step, cursor: cur, call, read, drive, cloudSteps, fence, say, parked: true, unitKey: `${runId}|${step}|${i}|parked`, budgetEnd: Math.min(budgetEnd - PASS_TAIL_MS, Date.now() + UNIT_BUDGET_MS), gate: o => freshnessGate(db, Object.assign({ stepStartedAt: cur.stepStartedAt }, o)) }; } });
        if (pw) out.parkedWork = pw;
        await cmdNow("end (stopped)"); await saveState({ runId, seenWorldAt: await worldAt(db) }); out.why = ok ? "waiting for the next try" : "stopped"; return end({ stopped: true });
      }
    }
    // ── a resting run: its signature moved → its steps again (poke)
    const sheets = await sheetsOf(db, runId);
    if (run.status === "processed" || run.status === "paused") {
      const sig = workSignature(run, { sheets });
      if (run.status === "processed" && run.processingSignature && sig !== run.processingSignature) { run.status = "running"; run.processingComplete = false; run.step = "nest"; run.processingSignature = sig; await saveRun("poke|" + sig); out.poked = true; }
      else { await cmdNow("end (resting)"); await saveState({ runId, seenWorldAt: await worldAt(db) }); out.why = "resting"; return end({ resting: true }); }
    }
    if (run.status !== "running") { await cmdNow("end (" + run.status + ")"); await saveState({ runId, seenWorldAt: await worldAt(db) }); return end({ status: run.status }); }

    // ── the steps, unit by unit (§1.6, 1.7)
    let cursor = state.cursor && state.cursor.runId === runId && state.cursor.step === run.step ? Object.assign({}, state.cursor) : { runId, step: run.step, i: 0, stepStartedAt: Date.now(), jobIds: [], sheetIds: [] };
    let n = retry && retry.step === run.step ? num(retry.n) : 0;
    while (run.status === "running") {
      if (Date.now() > budgetEnd - PASS_TAIL_MS) { out.again = true; break; }
      live();
      // (build step 8, C6: a person's Resume asked for the run back while this pass worked: it ends here, between two units, and the run waits for that computer)
      if (yieldAsked) { out.yield = true; out.why = `${yieldAsked.by || "a computer"} asked for the run back: the cloud ended its pass between two steps of work`; break; }
      // a press since the last unit is honoured now (a stop within one unit)
      { const c = await freshControl(db, runId); if (c && num(c.seq) > num(c.ack)) { run.control = c; const r2 = await control(run, { call, stop, saveRun, cursorJobs: cursor.jobIds || [] }); if (r2.stopped || run.status !== "running") break; } }
      await cmdNow("before " + run.step + " unit " + (cursor.step === run.step ? cursor.i : 0));   // (NOWCMD: a person's request put since the last unit is done now, between two units)
      if (run.status !== "running") break;
      const step = run.step;
      // (build step 8: a step the cloud does not own, not listed in drive.cloudSteps or not built yet, parks the run with words naming what needs a computer)
      if (!owns(step)) { await stop(Standby.parkWhy(step, cloudSteps, steps, lanes), Standby.parkFix, "parked"); break; }
      const units = steps[step] || stubStep();
      if (cursor.step !== step) cursor = { runId, step, i: 0, stepStartedAt: Date.now(), jobIds: [], sheetIds: [] };
      if (cursor.i >= units.length) {
        const next = O.nextStep(step);
        if (!next) {
          updatePending(run, { sheets }); run.status = hasPending(run) ? "processed" : "complete"; run.processingComplete = true; run.finishedAt = Date.now(); run.processingSignature = workSignature(run, { sheets });
          await saveRun("rest|" + run.processingSignature); cursor = { runId, step, i: 0, stepStartedAt: Date.now() }; await saveState({ runId, cursor, retry: null }); break;
        }
        run.step = next; cursor = { runId, step: next, i: 0, stepStartedAt: Date.now(), jobIds: [], sheetIds: [] };
        await saveRun("step|" + next + "|" + cursor.stepStartedAt); await saveState({ runId, cursor }); continue;
      }
      // (C7: a unit has a 45 s budget it reads (ctx.budgetEnd); one past 60 s is stopped before its next write, and runs again, from its own saved progress, in the next pass)
      const unitStart = Date.now(), unitCall = (b, u) => { if (Date.now() - unitStart > UNIT_HARD_MS) throw Object.assign(new Error("the unit ran past its time: it goes on in the next pass"), { overBudget: true }); return call(b, u); };
      const unit = units[cursor.i], ctx = { db, run, runId, step, cursor, call: unitCall, read, drive, cloudSteps, unitKey: `${runId}|${step}|${cursor.i}`, budgetEnd: Math.min(budgetEnd - PASS_TAIL_MS, unitStart + UNIT_BUDGET_MS), gate: o => freshnessGate(db, Object.assign({ stepStartedAt: cursor.stepStartedAt }, o)), fence, lane: laneNow, sheets: () => sheetsOf(db, runId) };   // (fence: a unit reads it again before a Storage write, C7; lane: one lane's pass items now, ONSWITCH)
      ctx.say = say;   // (build step 4: a unit that writes a document of its own fences it on this pass's lease and the run's carrier, as Lib.call does: _charmNestIntake.js fencedWrite)
      let res;
      for (let attempt = 0; ; attempt++) {
        try { res = await unit(ctx); break; }
        catch (e) {
          if (e && (e.fenced || e.yielded || e.fatal)) throw e;
          if (e && e.overBudget) { res = { overBudget: true }; break; }
          const why = String((e && e.message) || e).slice(0, 300), k = kindOf(why);
          if (k === "transient" && attempt < RETRY_IN_PASS.length) { say(`[drive] ${step}: ${why} — trying again (${attempt + 1} of 2)`); await sleep(RETRY_IN_PASS[attempt]); continue; }
          if (k === "transient") { const next = Date.now() + AUTO_WAIT[Math.min(n, AUTO_WAIT.length - 1)]; await stop(why, "The cloud tries again by itself", "transient"); await saveState({ runId, cursor, retry: { runId, step, n: n + 1, next, kind: k } }); out.retryAt = next; }
          else await stop(why, "Fix the cause and press Resume.", k);
          res = { stopped: true }; break;
        }
      }
      out.units++;
      if (res && res.stopped) break;
      if (res && res.overBudget) { await saveState({ runId, cursor }); out.again = true; break; }
      if (res && res.stop) { await stop(res.stop.why, res.stop.fix, res.stop.kind); await saveState({ runId, cursor }); break; }
      if (res && res.wait && res.cursor) Object.assign(cursor, res.cursor);   // (a waiting unit's own cursor is kept too: the nest step's job ids, which a Stop press stops)
      if (res && res.wait) { run.note = String(res.why || res.wait).slice(0, 300); await saveRun("wait|" + res.wait + "|" + run.note); await saveState({ runId, cursor, wait: { kind: String(res.wait), why: run.note, since: Date.now(), ids: (res.ids || []).slice(0, 50) } }); out.waiting = res.wait; break; }
      if (res && res.goto) { run.step = String(res.goto); cursor = { runId, step: run.step, i: 0, stepStartedAt: Date.now(), jobIds: [], sheetIds: [] }; await saveRun("goto|" + run.step + "|" + cursor.stepStartedAt); await saveState({ runId, cursor }); continue; }
      if (!(res && res.more)) cursor.i++;
      if (res && res.cursor) Object.assign(cursor, res.cursor);
      if (run.note && !(res && res.wait)) { run.note = null; }
      await saveRun("unit|" + ctx.unitKey + "|" + cursor.i);
      await saveState({ runId, cursor, wait: null, ...(n ? { retry: null } : {}) }); n = 0;
    }
    await cmdNow("end of the pass");   // (NOWCMD: one put during the last unit is done before the pass ends; one put after this read finds the lease's "again": the next pass, at once)
    await saveState({ runId, seenWorldAt: await worldAt(db) });
    return end({ status: run.status, step: run.step, units: out.units });
  } catch (e) {
    if (e && e.fenced) { out.fenced = true; out.ok = false; out.why = e.message; if (timer) clearInterval(timer); return out; }   // (another pass holds the lease: nothing more is written, not even the lease's end)
    if (e && e.yielded) { out.yield = true; out.why = e.message; return end({ yield: true }); }
    if (e && e.fatal) { if (timer) clearInterval(timer); throw e; }   // (a test's crash: the pass dies where it stands, as an instance that is killed)
    say("[drive] pass failed: " + ((e && e.message) || e));
    return end(null, (e && e.message) || String(e));
  }
}
const OP_READS = new Set(["runGet", "worldGet", "getSheet", "getOrderPieces", "poolGet", "poolList", "cancelList", "cancelCheck", "shippedByUs", "masterList", "optionMapGet", "aliasGet", "noDesignGet", "customGet", "customSheetGet", "customReadGet", "shareGet", "etsyStopGet", "driveGet", "listingSkus", "listingPhotos", "runCmdList"]);
const torontoDay = t => { try { return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(t)); } catch (_) { return new Date(t).toISOString().slice(0, 10); } };
async function worldAt(db) { try { const s = await db.collection("Charm_World").doc("summary").get(); return s.exists ? ms((s.data() || {}).at) : 0; } catch (_) { return 0; } }
async function freshControl(db, runId) { const [s] = await db.getAll(db.collection(RUNS).doc(runId), { fieldMask: ["control"] }); return s && s.exists ? (s.data() || {}).control || null : null; }
async function sheetsOf(db, runId) { try { const q = await db.collection("Charm_Nest_Sheets").where("runId", "==", runId).get(); return q.docs.map(d => Object.assign({ id: d.id }, d.data())); } catch (_) { return []; } }

/** The run's presses not yet acknowledged, oldest first (RunCtl operatorStop, resume, next, commitNow, releaseRun), then the acknowledgement. */
async function control(run, k) {
  const c = run.control || {}, ack = num(c.ack), acts = ["stop", "resume", "next", "commit", "abandon", "start"].map(a => [a, c[a]]).filter(([, v]) => v && num(v.seq) > ack).sort((x, y) => num(x[1].seq) - num(y[1].seq));
  if (!acts.length) return { acted: null };
  let stopped = false; const acted = [];
  // (build step 6: the cloud's nest jobs the step is waiting on are stopped with the run, within one step of their search; Resume asks for them again, under a new attempt)
  const stopJobs = async why => { for (const id of [...new Set((k.cursorJobs || []).map(String))].slice(0, 60)) await k.call({ op: "stopJob", id, for: "run", why }, "stopjob|" + run.runId + "|" + id + "|" + num(c.seq)); };
  for (const [a] of acts) {
    acted.push(a);
    if (a === "stop" && !["complete", "abandoned"].includes(run.status)) { await k.stop("stopped by the operator", "Press Resume when ready.", "operator"); await stopJobs("stopped by the operator"); stopped = true; }
    else if (a === "resume" && ["stopped", "paused", "processed"].includes(run.status)) { if (run.status === "processed") { run.step = "engrave"; run.processingComplete = false; } run.status = "running"; run.stoppedBy = null; run.fix = null; run.stopKind = null; stopped = false; }
    else if (a === "next" && run.status === "paused") run.status = "running";
    else if (a === "commit") { run.commitRequested = true; run.awaitCommit = false; if (["paused", "processed"].includes(run.status)) { run.status = "running"; run.processingComplete = false; run.step = "labels"; } }
    else if (a === "abandon" && !["complete", "abandoned"].includes(run.status)) {
      // (releaseRun: the run's unfinished pieces are let go, so the next run can take them; a piece already cut in a committed set stays as it is)
      const open = Object.values(run.lines || {}).filter(l => l && l.state !== "committed" && Array.isArray(l.poolIds) && l.poolIds.length);
      const ids = [...new Set(open.flatMap(l => l.poolIds))].slice(0, 400);
      if (ids.length) await k.call({ op: "poolUpdate", poolIds: ids, patch: { state: "abandoned" } }, "abandon|" + run.runId);
      await stopJobs("the run was given up");
      run.status = "abandoned"; run.abandonedAt = Date.now(); stopped = true;
    }
  }
  await k.saveRun("control|" + num(c.seq));
  await k.call({ op: "runControl", action: "ack", runId: run.runId, seq: num(c.seq) }, "ack|" + run.runId + "|" + num(c.seq));
  run.control = Object.assign({}, c, { ack: num(c.seq) });
  return { acted, stopped: stopped || run.status === "stopped" || run.status === "abandoned" };
}
/** A request a page sent (Charm_Nest_Run_Cmds) that the cloud cannot do yet: answered at once, in words, so it never waits for a computer that no longer carries the run.
 *  owned: the commands pass item is in this build (build step 13): a request whose lane "cmd:<type>" is the cloud's is left to it (GAP G2: or a lane on already that covers it,
 *  Standby.cmdLaneOn: a hop while "cmd:seat" or "cmd:renest" is on). */
async function answerCommands(db, runId, cloudSteps, call, owned) {
  const s = await db.collection(CMDS).doc(runId).get(), cmds = Object.values((s.exists && (s.data() || {}).cmds) || {}).filter(c => c && c.state === "waiting" && !(owned && Standby.cmdLaneOn(cloudSteps, c.type))).sort((a, b) => num(a.at) - num(b.at));
  let n = 0;
  for (const c of cmds.slice(0, 10)) {
    const why = Standby.cmdLaneOn(cloudSteps, c.type) ? `The cloud carries this run and cannot do "${c.type}" yet: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again` : `The cloud carries this run, and "${c.type}" is done by a computer in this build: with the cloud on Standby (Settings → Cloud), press Resume on a computer, then ask again`;
    await call({ op: "runCmdDone", runId, id: c.id, ok: false, text: why }, "cmd|" + runId + "|" + c.id); n++;
  }
  return n;
}

/** RELEASEHOLD (build step 13): the release unit, _charmNestHoldRelease.js driveUnit, through the pass's own fenced call. A fence or a hand-back ends the pass as any unit's does; any other failure is
 *  said and left for the next pass (a release is finished by the next press too). -> how many releases were taken up */
async function releasesUnit(db, call, say) {
  try { return await require("./_charmNestHoldRelease").driveUnit({ db, call }); }
  catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; (say || (() => {}))("[drive] releases: " + ((e && e.message) || e)); return 0; }
}

/* ── the cron's tick (§1.1): with the drive absent or off, ONE read and nothing else. Otherwise it reads what it needs, keeps the cloud's carrier alive while the cloud
   carries a run (its only write, one transaction a minute: C6), and dispatches a pass when there is something to do. It never dispatches a world build (E1). ── */
async function tick(env) {
  const { db } = env, now = Date.now(), out = { reads: 1, dispatched: false };
  const dv = await db.collection(OPEN).doc("drive").get(), drive = dv.exists ? dv.data() || {} : null, mode = drive && (drive.mode === "standby" || drive.mode === "on") ? drive.mode : "off";
  if (mode === "off") { out.off = true; return out; }
  const cloudSteps = Array.isArray(drive.cloudSteps) ? drive.cloudSteps.map(String) : [];
  const steps = env.steps || defaultSteps(), lanes = env.lanes || defaultLanes(), owned = Standby.owned(cloudSteps, steps, lanes);   // (build step 8: what the cloud owns, listed and built)
  const [ls, st, ov, ws] = await db.getAll(db.collection(DRIVE).doc("lease"), db.collection(DRIVE).doc("state"), db.collection(OPEN).doc("run"), db.collection("Charm_World").doc("summary"));
  out.reads += 4;
  const L = ls.exists ? ls.data() || {} : {}, S = st.exists ? st.data() || {} : {}, runId = ov.exists ? (ov.data() || {}).runId || null : null, W = ws.exists ? ws.data() || {} : {};
  let run = null, carrier = null, why = [];
  if (runId) {
    const [rs, cs] = await db.getAll(db.collection(RUNS).doc(runId), { fieldMask: ["status", "step", "stoppedBy", "stopKind", "errors", "control"] }).then(async ([r]) => [r, await db.collection(CARRIER).doc(runId).get()]);
    out.reads += 2; run = rs.exists ? rs.data() || {} : null; carrier = cs.exists ? (cs.data() || {}).carrier || null : null;
  }
  const closed = !run || ["complete", "abandoned", "archived"].includes(String(run.status || ""));
  // the carrier's keep-alive while the cloud carries a run that is not finished (C6)
  if (runId && !closed && carrier && carrier.dev === CLOUD_DEV) {
    const cref = db.collection(CARRIER).doc(runId);
    await db.runTransaction(async tx => { const cs = await tx.get(cref), c = cs.exists ? (cs.data() || {}).carrier : null; if (c && c.dev === CLOUD_DEV) tx.set(cref, { carrier: Object.assign({}, c, { at: Date.now() }) }, { merge: true }); });
    out.kept = true;
  }
  const cloudCarries = !!(carrier && carrier.dev === CLOUD_DEV), computerAlive = !!(carrier && carrier.dev && !cloudCarries && now - num(carrier.at) < COMPUTER_FRESH_MS);
  if (run && !closed && !computerAlive && (cloudCarries || owned.some(s => RUN_STEPS.includes(s)))) {   // (build step 8: a run the cloud could only park is not a reason to start a pass)
    if (run.status === "running") why.push("running");
    const ctl = run.control || {}; if (num(ctl.seq) > num(ctl.ack)) why.push("a press waits");
    if (run.status === "stopped") {
      const sr = await stopRead(db); out.reads++;
      if (autoResumable(Object.assign({ runId }, run), { runMode: drive.runMode, stop: sr, cloudSteps: owned })) { const r = S.retry && S.retry.runId === runId ? S.retry : null; if (stopKindOf(run) !== "transient" || !r || now >= num(r.next)) why.push("a stop may be taken up"); }
    }
    if (["processed", "paused", "review"].includes(run.status) && ms(W.at) > num(S.seenWorldAt)) why.push("the world moved");   // (review: a run in review at engraving takes new orders too, build step 4's gate)
    if (cloudCarries && cloudSteps.includes("files")) { const w = await require("./_charmNestFinishLane").tickWhy(db, runId, now).catch(() => null); out.reads++; if (w) why.push(w); }   // (build step 7a: a sheet writer's task that went quiet)
    // (build step 8, C5: a run the cloud parked at the checkpoint step keeps placing new orders: a new world is a reason to look)
    if (run.status === "stopped" && run.stopKind === "parked" && run.step === "checkpoint" && (owned.includes("pool") || owned.includes("nest")) && ms(W.at) > num(S.seenWorldAt)) why.push("new orders for a parked run");
    if (cloudCarries || !carrier) {
      const cs = await db.collection(CMDS).doc(runId).get(); out.reads++;
      const cmdList = Object.values((cs.exists && (cs.data() || {}).cmds) || {});
      if (cmdList.some(c => c && c.state === "waiting")) why.push("a request waits");   // (read from the run's own requests document, which runCmdPut writes: never the shared feed, m:C2)
      else if (cmdList.some(c => c && c.state === "running" && c.claim && c.claim.dev === CLOUD_DEV)) why.push("a request is under way");   // (build step 13: a request the cloud works on over several passes)
      // (build step 9: a cancel recorded since the lane's last read, a take-off a person asked for or one that waits, or the lane's 10-minute look)
      if (cloudSteps.includes("takeoff") && !["complete", "abandoned"].includes(String(run.status || ""))) { const t = await TakeOff.due(db, S, now); out.reads += t.reads; if (t.why) why.push(t.why); }
    }
    // (build step 11: an approved engraving whose file the cloud writes: a press no pass took up, a pass that died while writing it, or one waiting for its sheet, looked at again every 5 minutes)
    if (cloudCarries && cloudSteps.includes("engrave")) {
      const et = await db.collection("Charm_Engrave_Files").where("state", "in", ["waiting", "working"]).limit(20).get(); out.reads++;
      if (et.docs.map(d => d.data() || {}).some(t => t.runId === runId && (t.state === "working" ? now - num(t.beatAt) > DISPATCH_STALE_MS : !t.waitingSince || now - num(t.updatedAt) > 5 * 60000))) why.push("an engraving file waits");
    }
    // (build step 11, C3: a line's paid reading started by the classify unit and not answered yet, looked at again every minute until it is)
    if (cloudCarries && cloudSteps.includes("engrave")) { const ci = (S.items || {})["engrave-classify"] || {}; if (ci.runId === runId && num(ci.readingAt) > 0 && now - num(ci.readingAt) >= 60000) why.push("an engraving reading waits"); }
    if (cloudCarries && cloudSteps.includes("sweeps") && now - num((S.sweeps || {}).at) >= 2 * 60000) why.push("a sweep is due");   // (build step 13: the repair sweeps' two-minute look)
  }
  if ((!runId || closed) && drive.runMode === "auto" && num(W.live) > 0 && owned.includes("pull") && !drive.pending) why.push("open orders and no run");
  // (G5: a person's label ask waits for the cloud whether or not a run is open: one not looked at yet, one waiting with words looked at again every 2 minutes, one left working)
  if (laneOn(cloudSteps, "labels") && !computerAlive) {
    const lq = await db.collection("Charm_Nest_Label_Asks").where("state", "in", ["asked", "working"]).limit(20).get().catch(() => null); out.reads++;
    const due = a => (a.state === "working" ? now - num(a.at) > 10 * 60000 && now - ms(a.updatedAt) > DISPATCH_STALE_MS : !a.why || now - ms(a.updatedAt) >= 2 * 60000);
    if (lq && lq.docs.some(d => due(d.data() || {}))) why.push("a label ask waits");
  }
  { const rq = await db.collection(require("./_charmNestHoldRelease").JOBS).where("state", "==", "lifting").limit(1).get().catch(() => null); out.reads++; const j = rq && !rq.empty ? rq.docs[0].data() || {} : null; if (j && now - Math.max(num(j.beatAt), num(j.at)) >= 60000) why.push("a release waits"); }   // (RELEASEHOLD: one a function left half done)
  if (L.again) why.push("asked again");
  if (run && !closed && computerAlive && Array.isArray(drive.shadow) && drive.shadow.length && now - num(S.shadowAt) > 5 * 60000) why.push("a shadow plan is due");   // (m:C7: every 5 minutes while a computer carries, and only when the owner asked for it)
  if (drive.pending) why.push("a change of mode waits");
  if (L.state === "running" && Lease.stale(L, now) && run && run.status === "running") why.push("a pass went quiet");
  out.why = why;
  if (!why.length) return out;
  const got = await Lease.dispatch(db, db.collection(DRIVE).doc("lease"), "cron", { heartbeat: true });
  if (got && got.nonce) { out.dispatched = true; out.nonce = got.nonce; if (env.kick) await env.kick(got.nonce); }
  else out.queued = true;
  return out;
}

/** POST charmNestDrive-background { nonce } (the cron, driveSoon, a pass that asks again). Best-effort. */
async function kick(nonce) {
  const fetch = require("node-fetch"), origin = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
  try { await fetch(`${String(origin).replace(/\/+$/, "")}/.netlify/functions/charmNestDrive-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ nonce }) }); }
  catch (e) { console.warn("[drive] could not start the next pass:", e && e.message); }
}

module.exports = { pass, tick, kick, kindOf, stopKindOf, autoResumable, stopRead, freshnessGate, recordOf, sizeCheck, archiveFinished, updatePending, workSignature, carrierCheck, stubSteps, defaultSteps, defaultLanes, laneOn,
  PASSING, AUTO_WAIT, RETRY_IN_PASS, RUN_STEPS, LANES, LEASE_PATH, CLOUD_FRESH_MS, COMPUTER_FRESH_MS, DISPATCH_STALE_MS };
