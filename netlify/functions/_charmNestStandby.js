/*  netlify/functions/_charmNestStandby.js — STANDBY and THE SWITCH-OVER (cloud plan v2, build steps 8 and 15; §1.3, §4 phases 1 and 3, P8, P12, C5, C6, E8).
 *  Used by the cloud driver (_charmNestDrive.js) and by two ops of charmNestLibrary.js: driveStatus (a read for Settings) and driveSet (the owner's control in
 *  Brites Manager Settings, beside the Etsy section). DORMANT while Charm_Nest_Open/drive is absent or "off": nothing here runs then, except the owner's own press.
 *
 *  THE OWNER'S CONTROL (driveSet, admin-locked like the other Settings buttons):
 *    Off → Standby     at once. The cloud's steps for standby are pull, claim, pool, plan, nest and the files lane (build step 8).
 *    Standby → Cloud carries   only when every computer OPEN NOW (seen in the last 10 minutes) has the viewer build and every step and lane is built (refused in
 *                      words, naming what is missing). A computer seen earlier in the day without it is only listed ("loads the viewer page when it is opened"):
 *                      (ONSWITCH, Paul 20 Nov 2026: ON tonight with the shop's other computers closed) whatever an old page still open on it sends in mode "on" is
 *                      refused: its production writes and its presses (no cv: the reload words, charmNestLibrary modeOnRefusal), its writes of a sheet's or a set's
 *                      files (charmNestOutput's door), and it never takes the run (runCarrier). Then only drive.pending is written: the hand-over below flips the mode.
 *    Cloud carries → Standby   the way back (rollback): drive.pending; the driver parks the run at a step boundary (stopKind "handback") and flips when no task of
 *                      the cloud holds a live lease. Computers carry again only through a person's Resume.
 *    Standby → Off     at once when the cloud carries nothing; otherwise drive.pending, parked and flipped the same way, and the cloud's carrier is released.
 *    Cloud carries → Off   refused in words: Standby first. Off → Cloud carries: refused in words: Standby first.
 *  STANDBY (step 8): the cloud carries a run only when no computer has been seen for 90 s (or, in Auto, it opens the run itself); it reconciles from the cloud only
 *    (reconcile: sheets a closed computer left nesting, saving or owing files, requests it left running, lines still only "pulled"); it never clears a stop's kind
 *    (an Etsy stop stays an Etsy stop, E8); at a step it does not own (not in drive.cloudSteps, or not built yet) it parks with words naming what needs a computer;
 *    parked at the checkpoint step it keeps placing new orders (C5: parkedWork runs the pool and nest steps' units).
 *  THE HAND-OVER (step 15, P12): with "Cloud carries" pressed, the computer that carries the run starts nothing new, finishes what it is doing, saves, empties its
 *    outbox and reports idle (runCarrier idle: true, charm-nest-drive.js); the driver then sets mode "on" AND takes the carrier in ONE transaction, only if that
 *    computer still carries the run (same dev and since) and its idle report is newer than the press. No report within 10 minutes: nothing flips, and the owner is
 *    told which computer and what it is still doing. Nothing is lost (the page saved and sent everything; the cloud reconciles what it finds owed) and nothing is done
 *    twice (one carrier at a time; every cloud write is fenced on its lease and the carrier; a page's late production write is refused in mode "on").
 *  It never calls Etsy and never builds the world. */
"use strict";
const O = require("../../charm-nest-orders.js");
const OpClass = require("./_charmNestOpClass");

const OPEN = "Charm_Nest_Open", DRIVE_ID = "drive", CARRIER = "Charm_Nest_Run_Carrier", RUNS = "Charm_Nest_Runs", CMDS = "Charm_Nest_Run_Cmds", SHEETS = "Charm_Nest_Sheets";
const JOBS = "Charm_Nest_Jobs", FINISH = "Charm_Nest_Finish", COMPUTERS = "Charm_Nest_Computers", DRIVE_COLL = "Charm_Nest_Drive";
const CLOUD_DEV = "cloud", CLOUD_BY = "Cloud";
const COMPUTER_FRESH_MS = 90000, DRAIN_MS = 10 * 60000, TASK_BEAT_MS = 5 * 60000, OWED_FILES_MS = 2 * 60000, CMD_STALE_MS = 10 * 60000, SEEN_DAY_MS = 24 * 3600000;
/* (ONSWITCH) a computer is OPEN NOW when its last report (charm-nest-drift.js, every 4 minutes or so) is younger than 10 minutes: only those must have the viewer build */
const OPEN_NOW_MS = 10 * 60000;
const RUN_STEPS = O.RUN_STEPS;
/* the relayed requests (charmNestLibrary CMD_TYPES): each is a lane "cmd:<type>" the cloud owns once it does it (build step 13) */
const CMD_TYPES = ["reorder", "consolidate", "empty", "delete", "repicture", "seat", "renest", "remake", "unkeep", "placewaiting", "customsend", "setrebuild", "pairnow", "leaveset", "sendhome", "remakeline", "keepsync", "release", "skuonce", "hop", "backhome", "restorenest"];   // (RGRESTORE, Paul 22 Nov 2026: "restorenest", a nest's verified layout put back on its sheet; lane "cmd:restorenest", or a sheet lane on already: CMD_LANE_ALIAS) (ITEM 13, Paul 21 Nov 2026: "hop", a charm dragged onto the next or previous 10K / 14K sheet from any computer: lane "cmd:hop", which the owner turns on in Settings)
/* (ONSWITCH) the pass items' own lanes are lanes too: "rosecut" (the green line: the Rose Gold cut by itself and its presses, build step 10b) and "sweeps" (the repair sweeps,
   build step 13): in mode "on" no page does either, so "Cloud carries" lists them with the rest (a lane left out of cloudSteps would never run) */
const LANES = ["files", "takeoff", "engrave", "rosecut", "sweeps"].concat(CMD_TYPES.map(t => "cmd:" + t));
/* GAP G2 (Paul 21 Nov 2026: the sheet hop must work with no owner step): a request type whose own lane is new is covered by a lane that is on already. "hop" (a charm dragged onto
   the next / previous 10K / 14K sheet) is a sheet command of the same family as "seat" and "renest" (each moves a sheet's pieces): while either is the cloud's, so is the hop. Its
   own lane "cmd:hop" is still accepted. ONE list, read by the driver (answerCommands) and the commands unit (_charmNestCmdUnit.js); the page (charm-nest-viewer.js lane) keeps a copy. */
const CMD_LANE_ALIAS = { hop: ["seat", "renest"], backhome: ["remakeline", "skuonce"], restorenest: ["renest", "seat"] };   // (BACKHOME: the last step of a Change SKU, on while Change SKU is) (RGRESTORE: a layout put back is a Re-nest's RESTORE of a sheet, of the family of "renest" and "seat")
/** Whether a request type's lane is in cloudSteps: "cmd:<type>" itself, or a lane CMD_LANE_ALIAS names for it. */
function cmdLaneOn(cloudSteps, type) {
  const on = Array.isArray(cloudSteps) ? cloudSteps.map(String) : [], t = String(type || "");
  return on.includes("cmd:" + t) || (CMD_LANE_ALIAS[t] || []).some(a => on.includes("cmd:" + a));
}
/* Standby's steps (build step 8, §4 phase 1): orders arrive and are placed, nested and saved with every computer closed */
const STANDBY_STEPS = ["pull", "claim", "pool", "plan", "nest", "files"];
/* "Cloud carries" (step 15): every step and every lane */
const ON_STEPS = RUN_STEPS.concat(LANES);
const ALLOWED = new Set(ON_STEPS);
/* What a person reads for each step a computer must still do (the words of a park) */
const STEP_WORDS = { pull: "taking in new orders", claim: "claiming orders", pool: "making pieces", plan: "planning sheets", nest: "nesting", checkpoint: "sets", engrave: "engraving files", revalidate: "checking the orders again", labels: "labels", commit: "commit", complete: "finishing the run" };
const PARK_PREFIX = "The cloud parked this run";   // (the driver's kindOf reads a stop with these words as "parked")
const HANDBACK_WHY = "The cloud handed this run back: press Resume on a computer to carry it there";
/* The capability a page reports when it is a viewer (charm-nest-viewer.js, build step 14: charm-nest-drive.js caps() says "viewer" once it is loaded): "Cloud carries" needs it on
   every computer open now (ONSWITCH) */
const VIEWER_CAP = "viewer";

const num = v => (Number.isFinite(+v) ? +v : 0);
const ms = v => (v && v.toMillis ? v.toMillis() : num(v));
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const modeOf = d => (d && (d.mode === "standby" || d.mode === "on") ? d.mode : "off");
const driveRef = db => db.collection(OPEN).doc(DRIVE_ID);
const listWords = a => (a.length <= 1 ? a.join("") : a.slice(0, -1).join(", ") + " and " + a[a.length - 1]);
const clock = t => { try { return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", hour: "numeric", minute: "2-digit" }).format(new Date(t)); } catch (_) { return new Date(t).toISOString().slice(11, 16); } };

/** A step is BUILT when the driver has real units for it (the step-2 stubs carry .stub). */
function isBuilt(steps, s) { const u = steps && steps[s]; return Array.isArray(u) && u.length > 0 && !u.stub; }
/** A lane is built when the driver's lanes say so (build steps 7b, 9, 11, 13 each add theirs). */
function laneBuilt(lanes, l) { return !!(lanes && lanes[l]); }
/** Whether the cloud does this step (or lane) now: listed in drive.cloudSteps AND built. A listed step that is not built is parked at, in words. */
function owns(step, cloudSteps, steps, lanes) {
  if (!Array.isArray(cloudSteps) || !cloudSteps.includes(step)) return false;
  return RUN_STEPS.includes(step) ? isBuilt(steps, step) : laneBuilt(lanes, step);
}
/** drive.cloudSteps as the cloud does them now (listed and built). */
function owned(cloudSteps, steps, lanes) { return (Array.isArray(cloudSteps) ? cloudSteps : []).filter(s => owns(s, cloudSteps, steps, lanes)); }
/** The words of a park at `step`: what still needs a computer, from this step to the end of the run. */
function parkWhy(step, cloudSteps, steps, lanes) {
  const at = Math.max(0, RUN_STEPS.indexOf(step)), need = [];
  for (const s of RUN_STEPS.slice(at)) if (!owns(s, cloudSteps, steps, lanes) && !need.includes(STEP_WORDS[s])) need.push(STEP_WORDS[s]);
  const shown = need.length > 1 ? need.filter(w => w !== STEP_WORDS.complete) : need;
  const built = Array.isArray(cloudSteps) && cloudSteps.includes(step) && !owns(step, cloudSteps, steps, lanes) ? ` (the cloud's ${step} step is not built yet)` : "";
  return `${PARK_PREFIX} at ${step}: it needs a computer for ${listWords(shown.length ? shown : [STEP_WORDS[step] || step])}${built}: with the cloud on Standby (Settings → Cloud), press Resume on a computer`;
}
const parkFix = "With the cloud on Standby (Settings → Cloud), press Resume on a computer: it takes the run back once the cloud has finished what it is doing";

/* ── what the cloud is still doing on a run, or what a computer still does: the words the owner reads ── */
/** Tasks of the run whose lease is alive (a nest job's slots, a sheet's finish): the cloud's own (by "cloud") or a browser's. */
async function liveTasks(db, runId, now) {
  if (!runId) return [];
  const fresh = d => d && d.state === "running" && now - (num(d.beatAt) || num(d.startedAt) || 0) < TASK_BEAT_MS;
  const out = [];
  for (const c of [JOBS, FINISH]) {
    try { const q = await db.collection(c).where("runId", "==", runId).get(); for (const d of q.docs) { const x = d.data() || {}; if (fresh(x)) out.push({ id: d.id, coll: c, by: str(x.by || x.dev, 60), name: str(x.sheetName || x.fileBase || x.sheetId || d.id, 80) }); } }
    catch (_) { /* a collection a build step has not made yet: nothing of it runs */ }
  }
  return out;
}
/** Requests of the run that a computer (or the cloud) has started and not finished. */
async function runningCmds(db, runId) {
  if (!runId) return [];
  try { const s = await db.collection(CMDS).doc(runId).get(); return Object.values((s.exists && (s.data() || {}).cmds) || {}).filter(c => c && c.state === "running"); } catch (_) { return []; }
}
/** In words: what the computer that carries the run is still doing (its own report, then what the cloud's records show). */
async function busyWords(db, runId, car) {
  const who = (car && car.by) || "The computer that carries the run", parts = [];
  if (car && car.busy && car.busy.why) parts.push(`it is still ${str(car.busy.why, 200)}`);
  else if (car && num(car.idleAt) > 0) parts.push("its last report said it was idle before the press");
  else parts.push("it has not said it is idle yet (is it open, and on the new page?)");
  const cmds = await runningCmds(db, runId); if (cmds.length) parts.push(`${cmds.length} request${cmds.length === 1 ? " is" : "s are"} running`);
  return `${who}: ${parts.join("; ")}`;
}

/* ── the readiness for "Cloud carries" (§4 phase 3, C1, C2), read from the cloud ── */
/** { ok, missing: [words], later: [words], computers: [{ dev, by, ageS, caps, viewer, open }] }. o: { steps, lanes, now }
 *  (ONSWITCH) only a computer open now (seen in the last 10 minutes) without the viewer build holds "Cloud carries"; one seen earlier in the day is listed in `later`. */
async function readiness(db, o = {}) {
  const now = o.now || Date.now(), missing = [];
  const stepsOut = RUN_STEPS.filter(s => !isBuilt(o.steps, s));
  if (stepsOut.length) missing.push(`the cloud cannot do these steps of a run yet: ${stepsOut.join(", ")}`);
  const lanesOut = LANES.filter(l => !laneBuilt(o.lanes, l));
  if (lanesOut.length) {
    const reqs = lanesOut.filter(l => l.startsWith("cmd:")).map(l => l.slice(4)), other = lanesOut.filter(l => !l.startsWith("cmd:"));
    missing.push(`a person's presses with no cloud path yet: ${[...other.map(l => ({ files: "writing a sheet's files", takeoff: "taking pieces off (cancel, Hold, Take off)", engrave: "engraving files", rosecut: "the Rose Gold cut", sweeps: "the repair sweeps" })[l] || l), ...(reqs.length ? [`the Sheets card's requests (${reqs.join(", ")})`] : [])].join("; ")}`);
  }
  let computers = [];
  try {
    const q = await db.collection(COMPUTERS).where("at", ">", now - SEEN_DAY_MS).get();
    computers = q.docs.map(d => { const x = d.data() || {}, caps = String(x.drive || "").split(/\s+/).filter(Boolean); return { dev: d.id, by: str(x.by, 80), station: str(x.station, 60), ageS: Math.max(0, Math.round((now - num(x.at)) / 1000)), caps, viewer: caps.includes(VIEWER_CAP), open: now - num(x.at) < OPEN_NOW_MS }; });
  } catch (e) { missing.push("the list of computers could not be read: " + str(e && e.message, 120)); }
  const names = a => `${a.slice(0, 12).map(c => `${c.by || "someone"} on ${c.station || c.dev}`).join(", ")}${a.length > 12 ? "…" : ""}`;
  const old = computers.filter(c => !c.viewer && c.open), closed = computers.filter(c => !c.viewer && !c.open), later = [];
  if (old.length) missing.push(`${old.length === 1 ? "this computer is open and has" : "these computers are open and have"} not loaded the viewer build (reload ${old.length === 1 ? "it" : "them"}): ${names(old)}`);
  if (closed.length) later.push(`${closed.length === 1 ? "this computer was" : "these computers were"} not open in the last 10 minutes: ${closed.length === 1 ? "it loads" : "each loads"} the viewer page when it is opened (an older page still open there is refused every production write and press): ${names(closed)}`);
  return { ok: !missing.length, missing, later, computers };
}

/* ── the drive document is changed only inside a transaction, read whole and written whole (no deep merge keeps a stale pending field) ── */
async function driveTx(db, fn) {
  const ref = driveRef(db);
  return db.runTransaction(async tx => { const s = await tx.get(ref), d = s.exists ? s.data() || {} : null; const r = await fn(tx, d); if (r && r.write) tx.set(ref, Object.assign({}, d || {}, r.write)); return r ? r.answer : null; });
}
/** The press p is still the one waiting (nobody changed it meanwhile). */
const samePending = (d, p) => !!(d && d.pending && num(d.pending.at) === num(p.at) && d.pending.to === p.to);
async function refuse(db, p, why) {
  return driveTx(db, async (tx, d) => (samePending(d, p) ? { write: { pending: null, pendingWait: null, pendingRefused: { at: Date.now(), to: p.to, by: str(p.by, 80), why: str(why, 900) } }, answer: { refused: why } } : { answer: { gone: true } }));
}
async function waitSaid(db, p, why) {
  return driveTx(db, async (tx, d) => (samePending(d, p) ? { write: { pendingWait: { at: Date.now(), why: str(why, 600) } }, answer: { waiting: why } } : { answer: { gone: true } }));
}

/* ── THE HAND-OVER to "on" (P12) ── */
async function toOn(o, p) {
  const { db } = o, now = Date.now(), mode = modeOf(o.drive), say = o.say || (() => {});
  if (mode === "on") { await driveTx(db, async (tx, d) => (samePending(d, p) ? { write: { pending: null, pendingWait: null } } : null)); return { ended: true, result: { pending: "on", already: true } }; }
  if (mode !== "standby") { await refuse(db, p, "The cloud carries every run only from Standby. Nothing was changed"); return { ended: true, result: { refused: true } }; }
  const ready = await readiness(db, { steps: o.steps, lanes: o.lanes, now });
  if (!ready.ok) { await refuse(db, p, `The cloud cannot carry every run yet: ${ready.missing.join(" · ")}. Nothing was changed`); return { ended: true, result: { refused: true } }; }
  const runId = (o.info && o.info.runId) || null;
  // read from the cloud first (outside the flip): a request still running, or a browser's task with a fresh beat, holds the flip
  const [cmds, tasks] = await Promise.all([runningCmds(db, runId), liveTasks(db, runId, now)]);
  const browserTasks = tasks.filter(t => t.by && t.by !== CLOUD_DEV);
  if (now - num(p.at) > DRAIN_MS) {
    let car = null; if (runId) { try { const cs = await db.collection(CARRIER).doc(runId).get(); car = cs.exists ? (cs.data() || {}).carrier || null : null; } catch (_) { car = null; } }
    await refuse(db, p, `Nothing was changed: the hand-over did not finish within 10 minutes. ${await busyWords(db, runId, car)}${browserTasks.length ? `; still writing ${browserTasks.map(t => t.name).join(", ")}` : ""}. Press Cloud carries again once it has finished, or stop the run on that computer first`);
    return { ended: true, result: { refused: true, timeout: true } };
  }
  const flip = await db.runTransaction(async tx => {
    const dref = driveRef(db), dv = await tx.get(dref), d = dv.exists ? dv.data() || {} : null;
    if (!samePending(d, p) || modeOf(d) !== "standby") return { gone: true };
    let cref = null, cd = {}, car = null;
    if (runId) { cref = db.collection(CARRIER).doc(runId); const cs = await tx.get(cref); cd = cs.exists ? cs.data() || {} : {}; car = cd.carrier || null; }
    const computer = car && car.dev && car.dev !== CLOUD_DEV ? car : null, alive = !!(computer && now - num(computer.at) < COMPUTER_FRESH_MS);
    if (alive) {
      const seen = d.pending.carrier;
      // the computer that carries the run when the press is first seen is the one that must say it is idle; another computer taking it meanwhile starts the wait again
      if (!seen || seen.dev !== computer.dev || num(seen.since) !== num(computer.since)) { tx.set(dref, Object.assign({}, d, { pending: Object.assign({}, d.pending, { carrier: { dev: computer.dev, by: str(computer.by, 80), since: num(computer.since), at: now } }) })); return { waiting: true, car: computer }; }
      if (!(num(computer.idleAt) > num(d.pending.at))) return { waiting: true, car: computer };
      if (cmds.length || browserTasks.length) return { waiting: true, car: computer, held: true };
    }
    tx.set(dref, Object.assign({}, d, { mode: "on", cloudSteps: ON_STEPS.slice(), pending: null, pendingWait: null, pendingRefused: null, flippedAt: now, flippedBy: str(d.pending.by, 80), flippedFrom: "standby" }));
    if (cref) {
      const keep = Object.assign({}, cd); delete keep.yieldTo;
      const next = { dev: CLOUD_DEV, by: CLOUD_BY, at: now, since: car && car.dev === CLOUD_DEV ? num(car.since) || now : now, ...(computer ? { took: { dev: computer.dev, by: str(computer.by, 80), at: num(computer.at), idleAt: num(computer.idleAt) } } : {}) };
      tx.set(cref, Object.assign(keep, { runId, carrier: next, expireAt: new Date(now + 30 * 86400000) }));
    }
    return { flipped: true, took: computer };
  });
  if (flip && flip.flipped) { say(`[drive] the cloud carries every run now${flip.took ? `: taken from ${flip.took.by || flip.took.dev}, idle since ${clock(num(flip.took.idleAt) || now)}` : ""}`); return { ended: true, again: true, result: { flipped: "on", took: flip.took ? flip.took.dev : null } }; }
  if (flip && flip.waiting) {
    const why = flip.held ? `Waiting: ${cmds.length ? `${cmds.length} request${cmds.length === 1 ? " is" : "s are"} still running` : ""}${cmds.length && browserTasks.length ? "; " : ""}${browserTasks.length ? `${browserTasks.map(t => t.name).join(", ")} still being written by ${browserTasks[0].by}` : ""}` : `Waiting for the computer that carries the run to finish and say it is idle. ${await busyWords(db, runId, flip.car)}`;
    await waitSaid(db, p, why + ` (it gives up at ${clock(num(p.at) + DRAIN_MS)})`);
    return { ended: true, result: { waiting: "on" } };
  }
  return { ended: true, result: { gone: true } };
}

/* ── THE WAY BACK: "standby" (rollback from "on") or "off" (from standby). The run the cloud carries is parked at a step boundary (this pass is between units), the cloud
   starts no new task, and the mode flips only when no task of the run holds a live lease. Turning the cloud off also lets go of its carrier, so any computer may take the run. ── */
async function backTo(o, p) {
  const { db } = o, now = Date.now(), mode = modeOf(o.drive), to = p.to, say = o.say || (() => {});
  if (to !== "standby" && to !== "off") { await refuse(db, p, "Unknown change of mode. Nothing was changed"); return { ended: true, result: { refused: true } }; }
  if (to === mode) { await driveTx(db, async (tx, d) => (samePending(d, p) ? { write: { pending: null, pendingWait: null } } : null)); return { ended: true, result: { pending: to, already: true } }; }
  if (to === "off" && mode === "on") { await refuse(db, p, "The cloud carries every run: choose Standby first (the runs are handed back), then Off. Nothing was changed"); return { ended: true, result: { refused: true } }; }
  const info = o.info || {}, runId = info.runId || null, cloudHolds = !!(info.carries || info.yieldTo);
  if (o.phase === "begin" && cloudHolds && runId) return null;   // (the run the cloud carries is read first, then parked: phase "run")
  if (o.phase === "run" && o.run && o.run.status === "running" && typeof o.stopRun === "function") await o.stopRun(HANDBACK_WHY, "Press Resume on a computer", "handback");
  const tasks = await liveTasks(db, runId, now), mine = tasks.filter(t => !t.by || t.by === CLOUD_DEV);
  if (mine.length) { await waitSaid(db, p, `The cloud hands the run back once it has finished ${listWords(mine.map(t => t.name))}`); return { ended: true, result: { waiting: to } }; }
  const flipped = await db.runTransaction(async tx => {
    const dref = driveRef(db), dv = await tx.get(dref), d = dv.exists ? dv.data() || {} : null;
    if (!samePending(d, p)) return false;
    let cref = null, cd = null;
    if (to === "off" && runId) { cref = db.collection(CARRIER).doc(runId); const cs = await tx.get(cref); cd = cs.exists ? cs.data() || {} : null; }
    tx.set(dref, Object.assign({}, d, { mode: to, pending: null, pendingWait: null, flippedAt: now, flippedBy: str(d.pending.by, 80), flippedFrom: modeOf(d) }));
    // (off: the cloud's carrier is let go at once, so the run is taken like any other, by a person's Resume or a computer's boot, with no 3-minute wait)
    if (cref && cd && cd.carrier && cd.carrier.dev === CLOUD_DEV) { const keep = Object.assign({}, cd); delete keep.yieldTo; tx.set(cref, Object.assign(keep, { carrier: Object.assign({}, cd.carrier, { at: 0, released: now }) })); }
    return true;
  });
  if (flipped) say(`[drive] the mode is ${to} now`);
  return { ended: true, result: flipped ? { flipped: to } : { gone: true } };
}

/** A change of mode the owner asked for (drive.pending), handled first in every pass. o: { db, drive, info, phase: "begin" | "run", run?, stopRun?, steps, lanes, say }.
 *  → null (nothing to do in this phase) or { ended: true, again?, result }: the pass ends there, whatever the result (no unit runs while a mode change waits). */
async function modeChange(o) {
  const p = o.drive && o.drive.pending; if (!p || !p.to) return null;
  if (p.to === "on") return o.phase === "begin" ? toOn(o, p) : null;
  return backTo(o, p);
}

/* ── STANDBY: taking a run from a computer that closed (§4 phase 1, step 8): reconcile from the cloud only, once per take (keyed on the carrier's since) ── */
/** o: { db, run, runId, carrier, state, call, steps, lanes, cloudSteps, saveState, say }. → null (done before) or what was found and done. */
async function reconcile(o) {
  const { db, run, runId } = o, since = num(o.carrier && o.carrier.since), R = o.state && o.state.reconciled, say = o.say || (() => {});
  if (!runId || !since) return null;
  if (R && R.runId === runId && num(R.since) === since && R.done) return null;
  const now = Date.now(), took = (o.carrier && o.carrier.took) || null;
  const out = { runId, since, at: now, from: took ? { dev: str(took.dev, 60), by: str(took.by, 80), at: num(took.at) } : null, nest: [], files: [], owed: [], cmds: 0, pulled: 0, done: false };
  // 1. the run's sheets that the computer left in the middle of something: nested again or written again by the cloud when it does that, else said
  let sheets = [];
  try { const q = await db.collection(SHEETS).where("runId", "==", runId).get(); sheets = q.docs.map(d => Object.assign({ id: d.id }, d.data() || {})); } catch (e) { say("[drive] reconcile: sheets not read: " + (e && e.message)); }
  for (const s of sheets) {
    if (s.archived || num(s.laserDoneAt) > 0 || s.roseCutAt) continue;
    const name = str(s.fileBase || s.metalLabel || s.id, 80);
    const nestOwed = s.dirty === true || s.status === "nesting" || s.status === "queued";
    const filesOwed = !nestOwed && (s.saving === true || s.status === "finishing" || (ms(s.filesOwed) > 0 && now - ms(s.filesOwed) > OWED_FILES_MS));
    if (!nestOwed && !filesOwed) continue;
    const op = nestOwed ? "nestStart" : "finishSheet", lane = nestOwed ? "nest" : "files";
    if (OpClass.isCloudOp(op) && owns(lane, o.cloudSteps, o.steps, o.lanes)) {
      const res = await o.call({ op, sheetId: s.id, runId }, `reconcile|${runId}|${since}|${op}|${s.id}`);
      if (res && res.status < 400) out[nestOwed ? "nest" : "files"].push(name); else out.owed.push(`${name}: ${str(res && res.error, 120) || "not started"}`);
    } else out.owed.push(`${name} (${nestOwed ? "to nest again" : "its files"})`);
  }
  // 2. requests the closed computer started and never finished (no step for 10 minutes): answered as failed, with why, never run twice (run-follow.js's rule)
  for (const c of await runningCmds(db, runId)) {
    if (now - (num(c.stepAt) || num(c.at)) <= CMD_STALE_MS) continue;
    await o.call({ op: "runCmdDone", runId, id: c.id, ok: false, text: `Stopped before it finished: the computer doing it (${str(c.stepBy || c.by || "another computer", 80)}) stopped answering at "${str(c.step || "its first step", 80)}". Nothing more was done: ask again if it is still wanted.` }, `reconcile|${runId}|cmd|${c.id}`);
    out.cmds++;
  }
  // 3. lines only "pulled" go to the intake (gated, build step 4); the dead page's IndexedDB is never used; the run's stop and its kind are left as they are (E8)
  out.pulled = Object.values((run && run.lines) || {}).filter(l => l && l.state === "pulled").length;
  out.done = true;
  if (typeof o.saveState === "function") await o.saveState({ reconciled: out });
  const words = [`The cloud carries this run since ${clock(since)}${out.from ? `: ${out.from.by || "the computer that carried it"} was not seen for 90 s` : ""}`];
  if (out.nest.length) words.push(`nesting again: ${out.nest.join(", ")}`);
  if (out.files.length) words.push(`writing the files of: ${out.files.join(", ")}`);
  if (out.owed.length) words.push(`waiting for a computer or a later build: ${out.owed.slice(0, 8).join(", ")}${out.owed.length > 8 ? "…" : ""}`);
  if (out.cmds) words.push(`${out.cmds} unfinished request${out.cmds === 1 ? "" : "s"} answered`);
  if (out.pulled) words.push(`${out.pulled} new order line${out.pulled === 1 ? "" : "s"} wait for the intake`);
  out.words = words.join("; ");
  say("[drive] " + out.words);
  return out;
}

/* ── STANDBY, C5: parked at the checkpoint step, the cloud keeps placing new orders (it counts as the page's checkpoint intake, §1.10): the units of the pool and
   nest steps run once a pass, when the cloud owns them. A unit that waits ends this pass's parked work; a failure is said, never a stop (the run is parked already). ── */
/** o: { run, steps, lanes, cloudSteps, ctxFor(step, i) → ctx, say }. → null (not parked at checkpoint) or { did: [...], wait?, errors? } */
async function parkedWork(o) {
  const r = o.run; if (!r || r.status !== "stopped" || r.stopKind !== "parked" || r.step !== "checkpoint") return null;
  const did = [], errors = [];
  for (const step of ["pool", "nest"]) {
    if (!owns(step, o.cloudSteps, o.steps, o.lanes)) continue;
    const units = o.steps[step];
    for (let i = 0; i < units.length; i++) {
      let res;
      try { res = await units[i](o.ctxFor(step, i)); }
      catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; errors.push(`${step}: ${str(e && e.message, 200)}`); break; }
      did.push(`${step}:${i}`);
      if (res && (res.wait || res.stop)) return { did, wait: res.wait || "stop", why: str(res.why || (res.stop && res.stop.why), 300), errors };
    }
  }
  if (errors.length && o.say) o.say("[drive] parked work: " + errors.join(" · "));
  return { did, errors };
}

/* ── THE OWNER'S CONTROL: driveSet (Settings, admin-locked: _charmNestRepoAuth SETTINGS_OPS) and driveStatus (Settings shows it) ── */
function cleanSteps(a) { return [...new Set((Array.isArray(a) ? a : []).map(x => str(x, 40)).filter(x => ALLOWED.has(x)))]; }
/** driveSet { mode?: off|standby|on, runMode?: auto|manual, cloudSteps?: [...], by }. ctx: { steps, lanes, sandbox, forget }. */
async function setMode(db, b, ctx = {}) {
  if (ctx.sandbox) return { error: "The cloud carries production runs only: the sandbox keeps its computers", status: 400 };
  const want = ["off", "standby", "on"].includes(b.mode) ? b.mode : null, runMode = ["auto", "manual"].includes(b.runMode) ? b.runMode : null, steps = Array.isArray(b.cloudSteps) ? cleanSteps(b.cloudSteps) : null;
  if (!want && !runMode && !steps) return { error: "Nothing to change", status: 400 };
  const by = str(b.by, 80) || "the owner", now = Date.now();
  // the hard conditions of "Cloud carries" are read first, so a refusal says what is missing at once (and again by the driver when it flips)
  if (want === "on") { const r = await readiness(db, { steps: ctx.steps, lanes: ctx.lanes, now }); if (!r.ok) return { error: `The cloud cannot carry every run yet: ${r.missing.join(" · ")}. Nothing was changed`, status: 409, missing: r.missing }; }
  const out = await db.runTransaction(async tx => {
    const dref = driveRef(db), dv = await tx.get(dref), d = dv.exists ? dv.data() || {} : {}, cur = modeOf(d), p = d.pending && d.pending.to ? d.pending : null;
    // turning the cloud off from standby: whether it carries the open run (its carrier) or is in the middle of a pass (its lease), read in the same transaction
    let holds = false;
    if (want === "off" && cur === "standby") {
      const ov = await tx.get(db.collection(OPEN).doc("run")), runId = ov.exists ? (ov.data() || {}).runId || null : null;
      if (runId) { const cs = await tx.get(db.collection(CARRIER).doc(runId)), c = cs.exists ? (cs.data() || {}).carrier : null; holds = !!(c && c.dev === CLOUD_DEV && num(c.at) > 0); }
      const ls = await tx.get(db.collection(DRIVE_COLL).doc("lease")), L = ls.exists ? ls.data() || {} : {}; if (L.state === "running" && now - (num(L.beatAt) || num(L.startedAt)) < TASK_BEAT_MS) holds = true;
    }
    const w = { by, at: now }; let said = "";
    if (runMode) w.runMode = runMode;
    if (steps) w.cloudSteps = steps;
    if (want && want !== (p ? p.to : cur)) {
      if (p && p.to !== "on") return { error: "The cloud is handing the runs back now: wait until Settings says it is done", status: 409 };
      if (p && p.to === "on" && want === cur) { w.pending = null; w.pendingWait = null; said = "The hand-over to the cloud was called off. Nothing else was changed"; }
      else if (cur === "off" && want === "standby") { Object.assign(w, { mode: "standby", pending: null, pendingWait: null, pendingRefused: null, flippedAt: now, flippedBy: by, flippedFrom: "off" }); if (!steps && !(Array.isArray(d.cloudSteps) && d.cloudSteps.length)) w.cloudSteps = STANDBY_STEPS.slice(); if (!runMode && !d.runMode) w.runMode = "manual"; said = "Standby: a computer that is open keeps its run; the cloud carries a run only when no computer has been seen for 90 seconds"; }
      else if (cur === "standby" && want === "on") { Object.assign(w, { pending: { to: "on", at: now, by }, pendingWait: null, pendingRefused: null }); said = "The computer that carries the run finishes what it is doing, then the cloud takes every run"; }
      else if (cur === "on" && want === "standby") { Object.assign(w, { pending: { to: "standby", at: now, by }, pendingWait: null, pendingRefused: null }); said = "The cloud parks the run and hands it back once its tasks end; then a person's Resume carries it on a computer"; }
      else if (cur === "standby" && want === "off") { if (holds) { Object.assign(w, { pending: { to: "off", at: now, by }, pendingWait: null, pendingRefused: null }); said = "The cloud parks the run it carries and lets it go once its tasks end"; } else { Object.assign(w, { mode: "off", pending: null, pendingWait: null, pendingRefused: null, flippedAt: now, flippedBy: by, flippedFrom: "standby" }); said = "Off: the computers carry every run, as before"; } }
      else if (cur === "on" && want === "off") return { error: "The cloud carries every run: choose Standby first (the runs are handed back), then Off. Nothing was changed", status: 409 };
      else if (cur === "off" && want === "on") return { error: "Choose Standby first: the cloud carries every run only from Standby. Nothing was changed", status: 409 };
    } else if (want && p && p.to === want) said = "That change is already under way";
    tx.set(dref, Object.assign({}, d, w));
    return { ok: true, mode: w.mode || cur, pending: Object.prototype.hasOwnProperty.call(w, "pending") ? w.pending : p, runMode: w.runMode || d.runMode || null, cloudSteps: w.cloudSteps || d.cloudSteps || [], said };
  });
  try { if (typeof ctx.forget === "function") ctx.forget(); } catch (_) {}
  if (out && out.ok) console.warn(`[charmNestLibrary] drive set by ${by}: ${JSON.stringify({ mode: out.mode, pending: out.pending && out.pending.to, runMode: out.runMode })}`);
  return out;
}

/** driveStatus: what Settings shows (the mode, the change that waits or was refused, the open run and who carries it, what the cloud does, and what "Cloud carries" still needs). */
async function status(db, ctx = {}) {
  if (ctx.sandbox) return { ok: true, sandbox: true, mode: "off" };
  const now = Date.now();
  const [dv, ov, ls] = await db.getAll(driveRef(db), db.collection(OPEN).doc("run"), db.collection(DRIVE_COLL).doc("lease"));
  const d = dv.exists ? dv.data() || {} : null, mode = modeOf(d), runId = ov.exists ? (ov.data() || {}).runId || null : null, L = ls.exists ? ls.data() || {} : null;
  let run = null, carrier = null;
  if (runId) {
    const [rs, cs] = await db.getAll(db.collection(RUNS).doc(runId), db.collection(CARRIER).doc(runId), { fieldMask: ["status", "step", "stoppedBy", "stopKind", "note", "carrier", "yieldTo"] });
    const r = rs.exists ? rs.data() || {} : null, c = cs.exists ? (cs.data() || {}).carrier || null : null;
    if (r) run = { runId, status: r.status || null, step: r.step || null, stoppedBy: r.stoppedBy || null, stopKind: r.stopKind || null, note: r.note || null };
    if (c) carrier = { dev: c.dev || "", by: c.by || "", at: num(c.at), since: num(c.since), ageS: num(c.at) ? Math.max(0, Math.round((now - num(c.at)) / 1000)) : null, cloud: c.dev === CLOUD_DEV, idleAt: num(c.idleAt) || null, busy: c.busy || null, took: c.took || null, released: num(c.released) || null };
  }
  const cloudSteps = d && Array.isArray(d.cloudSteps) ? d.cloudSteps : [];
  const ready = mode === "standby" || mode === "off" ? await readiness(db, { steps: ctx.steps, lanes: ctx.lanes, now }) : { ok: true, missing: [], later: [], computers: [] };
  return { ok: true, mode, runMode: (d && d.runMode) || "manual", cloudSteps, owned: owned(cloudSteps, ctx.steps, ctx.lanes), built: RUN_STEPS.filter(s => isBuilt(ctx.steps, s)), lanesBuilt: LANES.filter(l => laneBuilt(ctx.lanes, l)), standbySteps: STANDBY_STEPS, allSteps: ON_STEPS,
    pending: (d && d.pending) || null, pendingWait: (d && d.pendingWait) || null, pendingRefused: (d && d.pendingRefused) || null, flippedAt: (d && num(d.flippedAt)) || null, flippedBy: (d && d.flippedBy) || null,
    run, carrier, lease: L ? { state: L.state || null, at: num(L.at), beatAt: num(L.beatAt), endedAt: num(L.endedAt), error: L.error || null } : null, ready, now };
}

module.exports = { STANDBY_STEPS, ON_STEPS, LANES, CMD_TYPES, CMD_LANE_ALIAS, cmdLaneOn, STEP_WORDS, HANDBACK_WHY, PARK_PREFIX, VIEWER_CAP, DRAIN_MS, COMPUTER_FRESH_MS, OPEN_NOW_MS,
  isBuilt, laneBuilt, owns, owned, parkWhy, parkFix, readiness, liveTasks, busyWords, modeChange, reconcile, parkedWork, setMode, status, modeOf };
