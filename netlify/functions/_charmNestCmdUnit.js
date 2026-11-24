/*  netlify/functions/_charmNestCmdUnit.js — THE CLOUD DOES WHAT A PERSON ASKED OF THE RUN (cloud plan v2 §1.6 item 4, build step 13; lane "cmd:<type>").
 *  The driver's pass item "commands": the requests of Charm_Nest_Run_Cmds/<run> (runCmdPut, from any computer: the Sheets card, Release Hold, "Just this order", Place the waiting
 *  pieces, a design saved, ...) are done by the cloud while it carries the run, oldest first, one type at a time as the owner turns each lane on (drive.cloudSteps "cmd:<type>").
 *  It replaces the carrying page's runCommands (charm-nest-run-follow.js), with its rules:
 *    · claimed first (runCmdClaim): a request one carrier took is answered by that one only (P9); a page claims too whenever the drive is not off;
 *    · the age rule: a request still waiting after 10 minutes (6 hours for release and skuonce) is answered in words, never done late;
 *    · a request another computer began and stopped answering for 10 minutes is answered as failed, with why, and never run twice;
 *    · each one ends answered (runCmdDone): done, refused in words, or past its own time with what it did; while it works it says its step (runCmdStep), which every
 *      computer shows (the sheet card's progress, the relay's onStep).
 *  A request that takes more than one pass keeps its stage in Charm_Nest_Cmd_Work/<cmdId> (cmdWorkPut) and goes on in the next pass; the cron dispatches a pass while one runs.
 *  DORMANT: nothing runs while the drive is off, while the cloud does not carry the run, or while no "cmd:<type>" lane is in cloudSteps (the driver then answers every request in
 *  words, as build step 2 did).                                                                                                                                                  */
"use strict";
const Exec = require("./_charmNestCmdExec");

const CMDS = "Charm_Nest_Run_Cmds", CLOUD_DEV = "cloud";
const NOW_TYPES = new Set(["skuonce", "remakeline", "keepsync", "remake", "backhome", "customsend"]);   // (a person's requests done at once, in a process of their own: nowRun)
const LONG = new Set(["release", "skuonce"]), AGE_MS = 10 * 60000, LONG_AGE_MS = 6 * 3600000, STALE_MS = 10 * 60000, WORK_MAX_MS = 8 * 3600000;
const PASSING = /answer in time|timed out|network|HTTP 5\d\d|ECONNRESET|ETIMEDOUT|socket hang up|is being written already|try again|DEADLINE_EXCEEDED|UNAVAILABLE|ABORTED/i;
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const minutes = t => Math.max(1, Math.round(t / 60000));
const { cmdLaneOn } = require("./_charmNestStandby");
const laneOn = (ctx, type) => cmdLaneOn(ctx.cloudSteps || [], type);   // (GAP G2, Paul 21 Nov 2026: "cmd:<type>", or a lane on already that covers it: a hop while "cmd:seat" or "cmd:renest" is on)

/* CMDFAST (Paul, 23 Nov 2026: "You have a budget of 30 sec to make the update", "I cannot wait"): a request that makes an order's pieces again waited for the pass: up to a minute while a pass begins (take-offs, the intake),
   and behind the pass's own calls all the way (they go one at a time). It has a process of its own now (nowRun, started by runCmdPut itself), under the running pass's lease, every write fenced on it, exactly as the pass
   does it. One process holds a request at a time (Charm_Nest_Cmd_Hold/<id>): the pass leaves a fresh one to its process, and goes on with one whose process went quiet, from the request's own journal. */
const HOLD = "Charm_Nest_Cmd_Hold", HOLD_MS = 15000, KICK_MS = 9000, BEAT_MS = 4000;
async function take(ctx, c) {
  const me = ctx.worker || "pass", ref = ctx.db.collection(HOLD).doc(String(c.id)), at0 = Date.now();
  const mine = await ctx.db.runTransaction(async tx => {
    const s = await tx.get(ref), h = s.exists ? s.data() || {} : {};
    if (h.by && h.by !== me && at0 - num(h.at) < HOLD_MS && at0 >= num(h.at)) return false;
    if (me === "pass" && num(h.kickedAt) > 0 && at0 - num(h.kickedAt) < KICK_MS) return false;
    tx.set(ref, Object.assign({ runId: ctx.runId, by: me, at: at0 }, me === "pass" ? { state: "pass" } : {}, { expireAt: new Date(at0 + 2 * 86400000) }), { merge: true });
    return true;
  });
  if (!mine) return null;
  const t = setInterval(() => { ref.set({ at: Date.now() }, { merge: true }).catch(() => {}); }, BEAT_MS); if (t && typeof t.unref === "function") t.unref();
  return async () => { clearInterval(t); await ref.set({ by: null, at: 0, kickedAt: 0 }, { merge: true }).catch(() => {}); };
}

async function commandsUnit(ctx) {
  const held = { go: null };
  try { return await commands(ctx, held); } finally { if (held.go) await held.go(); }
}
async function commands(ctx, held) {
  const { db, runId } = ctx, now = Date.now(), bucket = Math.floor(now / 60000);
  /* FASTPICK (Paul, 22 Nov 2026): a request's progress and answer are always saved, even after a step that ran past the unit's one-minute limit (ctx.call refuses every write then, and a step
     that ran long was done again at every pass, for good, with every request after it waiting). ctx.callNow is the same fenced call without that limit (the driver's pass items); the unit
     still ends at once after it (more: the next pass) */
  const put = (b, u) => (typeof ctx.callNow === "function" ? ctx.callNow : ctx.call)(b, u);
  const s = await db.collection(CMDS).doc(runId).get(), all = Object.values((s.exists && (s.data() || {}).cmds) || {}).filter(c => c && c.id && c.type);
  const mine = c => !!(c.claim && c.claim.dev === CLOUD_DEV);
  const key = (c, what) => `${runId}|cmd|${c.id}|${what}`;
  const answer = async (c, ok, text, freed) => {
    text = str(text, 400) || (ok ? "Done" : "Not done");
    if (!NOW_TYPES.has(c.type)) await put({ op: "cmdWorkPut", runId, cmdId: c.id, patch: { state: "done", ok: !!ok, text, doneAt: Date.now() } }, key(c, "work-done"));   // (one done in one go needs no journal)
    await put(Object.assign({ op: "runCmdDone", runId, id: c.id, ok: !!ok, text }, freed ? { freed: str(freed, 120) } : {}), key(c, "done"));
  };
  let n = 0;
  // a request another computer began and never finished (it closed, or stopped answering): answered as failed, with why, for every computer to see; never run twice
  for (const c of ctx.worker ? [] : all.filter(c => c.state === "running" && !mine(c) && laneOn(ctx, c.type) && now - Math.max(num(c.stepAt), num(c.claim && c.claim.at), num(c.at)) > STALE_MS)) {
    const cl = await ctx.call({ op: "runCmdClaim", runId, id: c.id }, key(c, "claim-stale|" + bucket));
    if (cl && cl.claimed) { await answer(c, false, `Stopped before it finished: the computer doing it (${c.stepBy || (c.claim && c.claim.by) || "another computer"}) stopped answering at "${str(c.step, 80)}". Ask again.`); n++; }
  }
  const list = all.filter(c => laneOn(ctx, c.type) && (!ctx.only || ctx.only.has(c.type)) && (c.state === "waiting" || (c.state === "running" && mine(c)))).sort((a, b) => num(a.at) - num(b.at));
  const behind = new Set();   // (one computer's requests in the order asked: those after one that another process holds wait for it)
  const free = async () => { const f = held.go; held.go = null; if (f) await f(); };
  for (const c of list) {
    await free();
    if (Date.now() > ctx.budgetEnd) return { more: true, done: n };
    if (c.dev && behind.has(c.dev)) continue;
    if (NOW_TYPES.has(c.type)) { held.go = await take(ctx, c); if (!held.go) { if (c.dev) behind.add(c.dev); continue; } }
    // the age rule (charm-nest-run-follow.js runCommands): said, never a bare "too old"
    if (c.state === "waiting" && now - num(c.at) > (LONG.has(c.type) ? LONG_AGE_MS : AGE_MS)) {
      const cl = await ctx.call({ op: "runCmdClaim", runId, id: c.id }, key(c, "claim-old|" + bucket));
      if (cl && cl.claimed) { await answer(c, false, `Not done: asked ${minutes(now - num(c.at))} min ago by ${c.by || "another computer"}, while no computer carried the run. Ask again if it is still wanted.`); n++; }
      continue;
    }
    const cl = await ctx.call({ op: "runCmdClaim", runId, id: c.id }, key(c, "claim|" + bucket));
    if (!cl || !cl.claimed) continue;   // (another computer holds it and is alive: that one answers it)
    const ws = await db.collection(Exec.WORK).doc(c.id).get(); let work = ws.exists ? ws.data() || {} : {};
    if (work.state === "done") { await put({ op: "runCmdDone", runId, id: c.id, ok: !!work.ok, text: str(work.text, 400) || "Done" }, key(c, "done")); n++; continue; }   // (answered in its journal; the answer itself is sent again)
    if (num(work.firstAt) && now - num(work.firstAt) > WORK_MAX_MS) { await answer(c, false, `Stopped: it was still at "${str(work.text || c.step, 80)}" after ${Math.round((now - num(work.firstAt)) / 3600000)} hours. Ask again.`); n++; continue; }
    const ex = Exec.EXEC[c.type];
    let r;
    try { r = ex ? await ex({ ctx, cmd: c, a: c.args || {}, work: Object.assign({}, work), now }) : { done: { ok: false, text: "Unknown command" } }; }
    catch (e) {
      if (e && (e.fenced || e.yielded || e.fatal)) throw e;
      if (e && e.overBudget) return { more: true, done: n };
      const why = str((e && e.message) || e, 300);
      if (PASSING.test(why) && now - num(c.at) < 30 * 60000) { ctx.say && ctx.say(`[drive] request ${c.type} ${c.id}: ${why} — tried again in the next pass`); continue; }
      r = { done: { ok: false, text: why } };
    }
    if (r && r.done) { await answer(c, r.done.ok, r.done.text, r.done.freed); n++; continue; }
    if (r && r.wait) {
      const w = Object.assign({}, r.wait.work || {}, { state: "working", text: r.wait.text || "", type: c.type, firstAt: num(work.firstAt) || now });
      await put({ op: "cmdWorkPut", runId, cmdId: c.id, patch: w }, key(c, "work"));
      if (r.wait.text && (r.wait.text !== c.step || now - num(c.stepAt) > 60000)) await put(Object.assign({ op: "runCmdStep", runId, id: c.id, text: str(r.wait.text, 160) }, r.wait.pct == null ? {} : { pct: r.wait.pct }), key(c, "step|" + bucket));
      n++;
    }
  }
  return { done: n };
}

/** CMDFAST: one person's request done now, by a process of its own, under a lease of its own: its hold (Charm_Nest_Cmd_Hold/<id>), begun while the drive is on and the cloud carries the run, every write of its calls
 *  fenced on it and on the run's carrier. It was the pass's lease: no pass running, or a pass that ended in the middle, and the person waited (23 Nov 2026). Any older request of these kinds is done first, in order.
 *  → { done } | { skipped } | { again } (the pass goes on with it) */
async function nowRun(o) {
  const { db, Lib, runId, id, key, say } = o, t0 = Date.now(), ref = db.collection(HOLD).doc(String(id)), nap = ms => new Promise(r => setTimeout(r, ms)), worker = "now:" + String(key).slice(0, 8), back = why => ref.set({ kickedAt: 0, state: "" }, { merge: true }).catch(() => {}).then(() => ({ again: why }));
  const began = await db.runTransaction(async tx => {
    const s = await tx.get(ref), h = s.exists ? s.data() || {} : {}, dv = await tx.get(db.collection("Charm_Nest_Open").doc("drive")), d = dv.exists ? dv.data() || {} : {}, cs = await tx.get(db.collection("Charm_Nest_Run_Carrier").doc(String(runId))), c = cs.exists ? (cs.data() || {}).carrier : null;
    if (!h.key || h.key !== key || h.runId !== runId) return { skipped: "not this request's turn" };
    if ((d.mode !== "on" && d.mode !== "standby") || !c || c.dev !== CLOUD_DEV) return { no: "the cloud does not carry the run" };
    tx.set(ref, { state: "running", nonce: key, beatAt: t0 }, { merge: true });
    return { drive: d };
  });
  if (began.skipped) return began;
  if (began.no) return back(began.no);
  const fence = { path: HOLD + "/" + id, nonce: key, runId }, fenced = t => Object.assign(new Error(t), { fenced: true }), read = b => Lib.call(b, {}), cloudSteps = Array.isArray(began.drive.cloudSteps) ? began.drive.cloudSteps.map(String) : [];
  const call = async (b, unit) => { const r = await Lib.call(b, { unit: String(unit || ""), fence }); if (r && (r.fenced || (r.status === 409 && r.notCarrier))) throw fenced(r.error); if (r && r.inFlight) throw new Error(String(r.error || "is being written already")); return r; };
  try {
    for (const until = t0 + (o.maxMs || 80000); Date.now() < until; await nap(500)) {
      try {
        const got = await read({ op: "runGet", runId }), run = got && got.run;
        if (!run || ["abandoned", "complete"].includes(run.status)) return back("the run is closed");
        const runFresh = Object.assign({}, run, { runId });
        await commandsUnit({ db, run: runFresh, runId, step: "pass:commands", cursor: {}, call, callNow: call, read, drive: began.drive, cloudSteps, state: {}, fence, say: say || (() => {}), live() {}, worker, only: NOW_TYPES, unitKey: `${runId}|now|${id}`, budgetEnd: Date.now() + 90000, _runNow: { at: Date.now(), run: runFresh } });
      } catch (e) { if (e && e.fenced) return back(str(e.message, 160)); if (say) say(`[cmd now] ${id}: ${str((e && e.message) || e, 200)}`); }
      const c = (((await db.collection(CMDS).doc(runId).get()).data() || {}).cmds || {})[id];
      if (!c || !["waiting", "running"].includes(c.state)) { if (say) say(`[cmd now] ${id}: ${((Date.now() - t0) / 1000).toFixed(1)} s`); return { done: (c && c.state) || "gone" }; }
    }
    return back("not done in time");
  } finally { await ref.set({ state: "" }, { merge: true }).catch(() => {}); }
}

module.exports = { nowRun, NOW_TYPES, HOLD, units: { passItems: [{ name: "commands", lane: "cmd:*", order: 40, unit: commandsUnit }] }, commandsUnit };
