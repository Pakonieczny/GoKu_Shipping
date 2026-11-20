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

const DRIVE = "Charm_Nest_Drive", OPEN = "Charm_Nest_Open", CARRIER = "Charm_Nest_Run_Carrier", RUNS = "Charm_Nest_Runs", CMDS = "Charm_Nest_Run_Cmds";
const LEASE_PATH = DRIVE + "/lease";
const CLOUD_DEV = "cloud", CLOUD_BY = "Cloud";
const COMPUTER_FRESH_MS = 90000, CLOUD_FRESH_MS = 180000, DISPATCH_STALE_MS = 2 * 60000;
const UNIT_BUDGET_MS = 45000, UNIT_HARD_MS = 60000, PASS_TAIL_MS = 60000, RENEW_MS = 20000, BUDGET_MS = 12 * 60000;
const AUTO_WAIT = [60000, 120000, 300000, 600000], RETRY_IN_PASS = [4000, 8000];
const RUN_STEPS = O.RUN_STEPS;
const LANES = ["files", "takeoff", "engrave"];   // (and "cmd:<type>": §1.5)
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
const RECORD_FIELDS = ["status", "step", "errors", "stoppedBy", "stopKind", "fix", "note", "setIds", "setId", "processingSignature", "processingComplete", "finishedAt", "pendingWork", "nothingToCut", "commitRequested", "awaitCommit", "claimedAt", "abandonedAt"];
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
    review: +d.review || 0, engraving: lines.filter(l => l.engrave && l.engrave.needed && !l.engrave.approved && !O.FINISHED_LINE.has(l.state)).length, sets: Object.keys(r.deferredSets || {}).length, commit: !!r.awaitCommit };
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
const stubStep = () => [async () => undefined];
function stubSteps() { const out = {}; for (const s of RUN_STEPS) out[s] = stubStep(); return out; }

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
  if (cloud && cd.yieldTo && cd.yieldTo.dev && num(cd.yieldTo.at) >= num(c.since) && now - num(cd.yieldTo.at) < 10 * 60000) return { ok: true, info: { mode, drive, runId, carries: true, yieldTo: cd.yieldTo, carrier: c } };
  const next = { dev: CLOUD_DEV, by: CLOUD_BY, at: now, since: cloud ? num(c.since) || now : now, ...(!cloud && c && c.dev ? { took: { dev: c.dev, by: c.by || "", at: num(c.at) } } : {}) };
  return { ok: true, writes: [[cref, { runId, carrier: next, expireAt: new Date(now + 30 * 86400000) }, { merge: true }]], info: { mode, drive, runId, carries: true, carrier: next, took: !cloud } };
}

/** One pass (§1.6). env: { db, Lib, nonce, budgetMs?, steps?, sleep?, renewMs?, log? }. → { ok, again?, yield?, fenced?, refused?, ... } */
async function pass(env) {
  const { db, Lib } = env, say = env.log || (() => {}), sleep = env.sleep || (t => new Promise(r => setTimeout(r, t)));
  const steps = env.steps || stubSteps(), leaseRef = docAt(db, LEASE_PATH), stateRef = db.collection(DRIVE).doc("state");
  const startedAt = Date.now(), budgetEnd = startedAt + (env.budgetMs || BUDGET_MS);
  const began = await Lease.begin(db, leaseRef, env.nonce, { heartbeat: true, check: tx => carrierCheck(db, tx) });
  if (!began) return { ok: false, skipped: "not this pass's turn" };
  if (began.refused) { await Lease.end(db, leaseRef, env.nonce, { refused: began.why }).catch(() => {}); return { ok: false, refused: began.why }; }
  const info = began.info || {}, nonce = env.nonce, drive = info.drive || {}, cloudSteps = Array.isArray(drive.cloudSteps) ? drive.cloudSteps.map(String) : [];
  const out = { ok: true, mode: info.mode, runId: info.runId || null, units: 0, writes: 0 };
  let timer = null, lost = false, handed = null;   // (lost: another pass holds the lease; handed: a person took the run back, or the drive was turned off)
  const end = async (result, error) => { if (timer) clearInterval(timer); const e = await Lease.end(db, leaseRef, nonce, result, error).catch(() => ({ again: false })); out.again = out.again || !!e.again; return out; };
  if (info.yield) { out.yield = true; out.why = `${(info.carrier && info.carrier.by) || "a computer"} carries the run`; return end({ yield: true }); }
  if (info.yieldTo) { out.yield = true; out.why = `${info.yieldTo.by || "a computer"} asked for the run back: the cloud starts nothing new`; return end({ yield: true, to: info.yieldTo.dev }); }
  const renewMs = env.renewMs == null ? RENEW_MS : env.renewMs;   // (the beat every 20 s, beside the one each unit's save makes: a long unit never starves it)
  if (renewMs > 0) timer = setInterval(() => { Lease.renew(db, leaseRef, nonce, { check: tx => carrierCheck(db, tx, { runId: info.runId, mustCarry: !!info.runId }) }).then(r => { if (r && r.lost) lost = true; else if (!r || !r.ok) handed = (r && r.why) || "the run was handed back"; }).catch(() => {}); }, renewMs);
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
    const saveState = patch => Lease.renew(db, leaseRef, nonce, { check: async tx => { const c = await carrierCheck(db, tx, { runId: info.runId, mustCarry: !!info.runId && info.carries }); return c.ok ? { ok: true, writes: (c.writes || []).concat([[stateRef, Object.assign({}, patch, { at: Date.now() }), { merge: true }]]) } : c; } }).then(r => { if (!r || !r.ok) throw r && r.lost ? new Fenced("the lease was lost") : new Yield((r && r.why) || "the run was handed back"); });

    /* a change of mode the owner asked for (runControl cloud, P12, §4): the way back to standby is taken at once (the cloud has no task of its own before build step 6; a running
       run is parked with words); "on" is refused in words until the drain exists (build step 14: every page a viewer, every lane the cloud's). → true when the pass ends here */
    const pendingMode = async (r, stopRun) => {
      const p = drive.pending; if (!p || !p.to) return false;
      if (p.to === "standby" && r && r.status === "running") await stopRun("The cloud handed this run back: press Resume on a computer to carry it there", "Press Resume on a computer", "handback");
      const dref = db.collection(OPEN).doc("drive");
      await db.runTransaction(async tx => {
        const dv = await tx.get(dref), d = dv.exists ? dv.data() || {} : {};
        if (!d.pending || d.pending.at !== p.at) return;
        if (p.to === "standby") tx.set(dref, { mode: "standby", pending: null, flippedAt: Date.now(), flippedBy: d.pending.by || "" }, { merge: true });
        else tx.set(dref, { pending: null, pendingRefused: { at: Date.now(), to: p.to, why: "The cloud cannot carry every run yet: every page must first be a viewer (cloud plan build step 14) and every lane must be the cloud's. Nothing was changed" } }, { merge: true });
      });
      out.pending = p.to;
      return p.to === "standby";
    };
    // ── no run open: in auto, the cloud opens one when there are open orders and it does the run's first step (runOpen, made by the cloud: carried by it)
    let runId = info.runId;
    if (!runId) {
      if (await pendingMode(null)) return end({ handedBack: true });
      const sum = await db.collection("Charm_World").doc("summary").get(), openLines = sum.exists ? num((sum.data() || {}).live) : 0;
      if (drive.runMode === "auto" && openLines > 0 && cloudSteps.includes("pull") && !drive.pending) {
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

    if (await pendingMode(run, stop)) return end({ handedBack: true });
    // ── control: the run's presses, oldest first, then acknowledged
    const ctlDone = await control(run, { call, stop, saveRun, cursorJobs: (state.cursor && state.cursor.jobIds) || [] });
    if (ctlDone.acted) out.control = ctlDone.acted;
    // ── requests from pages: a type the cloud does not do yet is answered at once, in words (a request never waits silently)
    if (run.status !== "abandoned" && run.status !== "complete") out.answered = await answerCommands(db, runId, cloudSteps, call);
    if (ctlDone.stopped) { await saveState({ runId, seenWorldAt: await worldAt(db), cursor: Object.assign({}, state.cursor || {}, { runId }) }); return end({ stopped: true }); }

    // ── a stopped run: taken up again only by §1.8's rules
    const retry = state.retry && state.retry.runId === runId ? state.retry : null;
    if (run.status === "stopped") {
      const st = await stopRead(db), ok = autoResumable(run, { runMode: drive.runMode, stop: st, cloudSteps });
      const k = stopKindOf(run), due = k !== "transient" || !retry || Date.now() >= num(retry.next);
      if (ok && due) {
        say(`[drive] resuming by itself: ${run.stoppedBy}`);
        run.status = "running"; run.stoppedBy = null; run.fix = null; delete run.stopKind; run.stopKind = null; out.resumed = true;
        await saveRun("resume|" + Date.now());
      } else { await saveState({ runId, seenWorldAt: await worldAt(db) }); out.why = ok ? "waiting for the next try" : "stopped"; return end({ stopped: true }); }
    }
    // ── a resting run: its signature moved → its steps again (poke)
    const sheets = await sheetsOf(db, runId);
    if (run.status === "processed" || run.status === "paused") {
      const sig = workSignature(run, { sheets });
      if (run.status === "processed" && run.processingSignature && sig !== run.processingSignature) { run.status = "running"; run.processingComplete = false; run.step = "nest"; run.processingSignature = sig; await saveRun("poke|" + sig); out.poked = true; }
      else { await saveState({ runId, seenWorldAt: await worldAt(db) }); out.why = "resting"; return end({ resting: true }); }
    }
    if (run.status !== "running") { await saveState({ runId, seenWorldAt: await worldAt(db) }); return end({ status: run.status }); }

    // ── the steps, unit by unit (§1.6, 1.7)
    let cursor = state.cursor && state.cursor.runId === runId && state.cursor.step === run.step ? Object.assign({}, state.cursor) : { runId, step: run.step, i: 0, stepStartedAt: Date.now(), jobIds: [], sheetIds: [] };
    let n = retry && retry.step === run.step ? num(retry.n) : 0;
    while (run.status === "running") {
      if (Date.now() > budgetEnd - PASS_TAIL_MS) { out.again = true; break; }
      live();
      // a press since the last unit is honoured now (a stop within one unit)
      { const c = await freshControl(db, runId); if (c && num(c.seq) > num(c.ack)) { run.control = c; const r2 = await control(run, { call, stop, saveRun, cursorJobs: cursor.jobIds || [] }); if (r2.stopped || run.status !== "running") break; } }
      const step = run.step;
      if (!cloudSteps.includes(step)) { await stop(`The cloud parked this run at ${step}: it needs a computer for ${step}: press Resume on a computer`, "Press Resume on a computer", "parked"); break; }
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
      const unit = units[cursor.i], ctx = { db, run, runId, step, cursor, call: unitCall, read, drive, cloudSteps, unitKey: `${runId}|${step}|${cursor.i}`, budgetEnd: Math.min(budgetEnd - PASS_TAIL_MS, unitStart + UNIT_BUDGET_MS), gate: o => freshnessGate(db, Object.assign({ stepStartedAt: cursor.stepStartedAt }, o)) };
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
      if (res && res.wait) { run.note = String(res.why || res.wait).slice(0, 300); await saveRun("wait|" + res.wait + "|" + run.note); await saveState({ runId, cursor, wait: { kind: String(res.wait), why: run.note, since: Date.now(), ids: (res.ids || []).slice(0, 50) } }); out.waiting = res.wait; break; }
      if (res && res.goto) { run.step = String(res.goto); cursor = { runId, step: run.step, i: 0, stepStartedAt: Date.now(), jobIds: [], sheetIds: [] }; await saveRun("goto|" + run.step + "|" + cursor.stepStartedAt); await saveState({ runId, cursor }); continue; }
      if (!(res && res.more)) cursor.i++;
      if (res && res.cursor) Object.assign(cursor, res.cursor);
      if (run.note && !(res && res.wait)) { run.note = null; }
      await saveRun("unit|" + ctx.unitKey + "|" + cursor.i);
      await saveState({ runId, cursor, wait: null, ...(n ? { retry: null } : {}) }); n = 0;
    }
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
  for (const [a] of acts) {
    acted.push(a);
    if (a === "stop" && !["complete", "abandoned"].includes(run.status)) { await k.stop("stopped by the operator", "Press Resume when ready.", "operator"); stopped = true; }
    else if (a === "resume" && ["stopped", "paused", "processed"].includes(run.status)) { if (run.status === "processed") { run.step = "engrave"; run.processingComplete = false; } run.status = "running"; run.stoppedBy = null; run.fix = null; run.stopKind = null; stopped = false; }
    else if (a === "next" && run.status === "paused") run.status = "running";
    else if (a === "commit") { run.commitRequested = true; run.awaitCommit = false; if (["paused", "processed"].includes(run.status)) { run.status = "running"; run.processingComplete = false; run.step = "labels"; } }
    else if (a === "abandon" && !["complete", "abandoned"].includes(run.status)) {
      // (releaseRun: the run's unfinished pieces are let go, so the next run can take them; a piece already cut in a committed set stays as it is)
      const open = Object.values(run.lines || {}).filter(l => l && l.state !== "committed" && Array.isArray(l.poolIds) && l.poolIds.length);
      const ids = [...new Set(open.flatMap(l => l.poolIds))].slice(0, 400);
      if (ids.length) await k.call({ op: "poolUpdate", poolIds: ids, patch: { state: "abandoned" } }, "abandon|" + run.runId);
      run.status = "abandoned"; run.abandonedAt = Date.now(); stopped = true;
    }
  }
  await k.saveRun("control|" + num(c.seq));
  await k.call({ op: "runControl", action: "ack", runId: run.runId, seq: num(c.seq) }, "ack|" + run.runId + "|" + num(c.seq));
  run.control = Object.assign({}, c, { ack: num(c.seq) });
  return { acted, stopped: stopped || run.status === "stopped" || run.status === "abandoned" };
}
/** A request a page sent (Charm_Nest_Run_Cmds) that the cloud cannot do yet: answered at once, in words, so it never waits for a computer that no longer carries the run. */
async function answerCommands(db, runId, cloudSteps, call) {
  const s = await db.collection(CMDS).doc(runId).get(), cmds = Object.values((s.exists && (s.data() || {}).cmds) || {}).filter(c => c && c.state === "waiting").sort((a, b) => num(a.at) - num(b.at));
  let n = 0;
  for (const c of cmds.slice(0, 10)) {
    const lane = "cmd:" + c.type, why = cloudSteps.includes(lane) ? `The cloud carries this run and cannot do "${c.type}" yet: press Resume on a computer, then ask again` : `The cloud carries this run, and "${c.type}" is done on a computer: press Resume on a computer, then ask again`;
    await call({ op: "runCmdDone", runId, id: c.id, ok: false, text: why }, "cmd|" + runId + "|" + c.id); n++;
  }
  return n;
}

/* ── the cron's tick (§1.1): with the drive absent or off, ONE read and nothing else. Otherwise it reads what it needs, keeps the cloud's carrier alive while the cloud
   carries a run (its only write, one transaction a minute: C6), and dispatches a pass when there is something to do. It never dispatches a world build (E1). ── */
async function tick(env) {
  const { db } = env, now = Date.now(), out = { reads: 1, dispatched: false };
  const dv = await db.collection(OPEN).doc("drive").get(), drive = dv.exists ? dv.data() || {} : null, mode = drive && (drive.mode === "standby" || drive.mode === "on") ? drive.mode : "off";
  if (mode === "off") { out.off = true; return out; }
  const cloudSteps = Array.isArray(drive.cloudSteps) ? drive.cloudSteps.map(String) : [];
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
  if (run && !closed && !computerAlive) {
    if (run.status === "running") why.push("running");
    const ctl = run.control || {}; if (num(ctl.seq) > num(ctl.ack)) why.push("a press waits");
    if (run.status === "stopped") {
      const sr = await stopRead(db); out.reads++;
      if (autoResumable(Object.assign({ runId }, run), { runMode: drive.runMode, stop: sr, cloudSteps })) { const r = S.retry && S.retry.runId === runId ? S.retry : null; if (stopKindOf(run) !== "transient" || !r || now >= num(r.next)) why.push("a stop may be taken up"); }
    }
    if (["processed", "paused"].includes(run.status) && ms(W.at) > num(S.seenWorldAt)) why.push("the world moved");
    if (cloudCarries || !carrier) {
      const cs = await db.collection(CMDS).doc(runId).get(); out.reads++;
      if (Object.values((cs.exists && (cs.data() || {}).cmds) || {}).some(c => c && c.state === "waiting")) why.push("a request waits");   // (read from the run's own requests document, which runCmdPut writes: never the shared feed, m:C2)
    }
  }
  if ((!runId || closed) && drive.runMode === "auto" && num(W.live) > 0 && cloudSteps.includes("pull") && !drive.pending) why.push("open orders and no run");
  if (L.again) why.push("asked again");
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

module.exports = { pass, tick, kick, kindOf, stopKindOf, autoResumable, stopRead, freshnessGate, recordOf, sizeCheck, archiveFinished, updatePending, workSignature, carrierCheck, stubSteps,
  PASSING, AUTO_WAIT, RETRY_IN_PASS, RUN_STEPS, LANES, LEASE_PATH, CLOUD_FRESH_MS, COMPUTER_FRESH_MS, DISPATCH_STALE_MS };
