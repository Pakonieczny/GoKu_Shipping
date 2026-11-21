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
const LONG = new Set(["release", "skuonce"]), AGE_MS = 10 * 60000, LONG_AGE_MS = 6 * 3600000, STALE_MS = 10 * 60000, WORK_MAX_MS = 8 * 3600000;
const PASSING = /answer in time|timed out|network|HTTP 5\d\d|ECONNRESET|ETIMEDOUT|socket hang up|is being written already|try again|DEADLINE_EXCEEDED|UNAVAILABLE|ABORTED/i;
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const minutes = t => Math.max(1, Math.round(t / 60000));
const laneOn = (ctx, type) => (ctx.cloudSteps || []).includes("cmd:" + type);

async function commandsUnit(ctx) {
  const { db, runId } = ctx, now = Date.now(), bucket = Math.floor(now / 60000);
  const s = await db.collection(CMDS).doc(runId).get(), all = Object.values((s.exists && (s.data() || {}).cmds) || {}).filter(c => c && c.id && c.type);
  const mine = c => !!(c.claim && c.claim.dev === CLOUD_DEV);
  const key = (c, what) => `${runId}|cmd|${c.id}|${what}`;
  const answer = async (c, ok, text, freed) => {
    text = str(text, 400) || (ok ? "Done" : "Not done");
    await ctx.call({ op: "cmdWorkPut", runId, cmdId: c.id, patch: { state: "done", ok: !!ok, text, doneAt: Date.now() } }, key(c, "work-done"));
    await ctx.call(Object.assign({ op: "runCmdDone", runId, id: c.id, ok: !!ok, text }, freed ? { freed: str(freed, 120) } : {}), key(c, "done"));
  };
  let n = 0;
  // a request another computer began and never finished (it closed, or stopped answering): answered as failed, with why, for every computer to see; never run twice
  for (const c of all.filter(c => c.state === "running" && !mine(c) && laneOn(ctx, c.type) && now - Math.max(num(c.stepAt), num(c.claim && c.claim.at), num(c.at)) > STALE_MS)) {
    const cl = await ctx.call({ op: "runCmdClaim", runId, id: c.id }, key(c, "claim-stale|" + bucket));
    if (cl && cl.claimed) { await answer(c, false, `Stopped before it finished: the computer doing it (${c.stepBy || (c.claim && c.claim.by) || "another computer"}) stopped answering at "${str(c.step, 80)}". Ask again.`); n++; }
  }
  const list = all.filter(c => laneOn(ctx, c.type) && (c.state === "waiting" || (c.state === "running" && mine(c)))).sort((a, b) => num(a.at) - num(b.at));
  for (const c of list) {
    if (Date.now() > ctx.budgetEnd) return { more: true, done: n };
    // the age rule (charm-nest-run-follow.js runCommands): said, never a bare "too old"
    if (c.state === "waiting" && now - num(c.at) > (LONG.has(c.type) ? LONG_AGE_MS : AGE_MS)) {
      const cl = await ctx.call({ op: "runCmdClaim", runId, id: c.id }, key(c, "claim-old|" + bucket));
      if (cl && cl.claimed) { await answer(c, false, `Not done: asked ${minutes(now - num(c.at))} min ago by ${c.by || "another computer"}, while no computer carried the run. Ask again if it is still wanted.`); n++; }
      continue;
    }
    const cl = await ctx.call({ op: "runCmdClaim", runId, id: c.id }, key(c, "claim|" + bucket));
    if (!cl || !cl.claimed) continue;   // (another computer holds it and is alive: that one answers it)
    const ws = await db.collection(Exec.WORK).doc(c.id).get(), work = ws.exists ? ws.data() || {} : {};
    if (work.state === "done") { await ctx.call({ op: "runCmdDone", runId, id: c.id, ok: !!work.ok, text: str(work.text, 400) || "Done" }, key(c, "done")); n++; continue; }   // (answered in its journal; the answer itself is sent again)
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
      await ctx.call({ op: "cmdWorkPut", runId, cmdId: c.id, patch: w }, key(c, "work"));
      if (r.wait.text && (r.wait.text !== c.step || now - num(c.stepAt) > 60000)) await ctx.call(Object.assign({ op: "runCmdStep", runId, id: c.id, text: str(r.wait.text, 160) }, r.wait.pct == null ? {} : { pct: r.wait.pct }), key(c, "step|" + bucket));
      n++;
    }
  }
  return { done: n };
}

module.exports = { units: { passItems: [{ name: "commands", lane: "cmd:*", order: 40, unit: commandsUnit }] }, commandsUnit };
