/*  netlify/functions/_charmNestCheckpoint.js — THE RUN'S CHECKPOINT STEP IN THE CLOUD (cloud plan v2 §1.7 "checkpoint", build step 10a; designs.json "Run checkpoint").
 *  RunCtl.doStep "checkpoint" (charm-nest-bridge.js ~16686), unit by unit, each saved before the next starts and each safe to run again:
 *    1 intake      Arrivals.processPending({ checkpoint: true }): the cloud's intake unit (build step 4), called with { checkpoint: true } when it is here (C5)
 *    2 finish      LiveNest.finish (~30097): with the run's intakeRecovery, a look at the sets, then each sheet the intake emptied and named to retire is archived
 *                  (archiveEmptySheet, whose transaction now refuses a sheet that is not empty), and the recovery is cleared (its back files stay for the engraving lane)
 *    3 assembly    Gate.assemble (_charmNestAssembly.js): the sets formed, joined and listed by the page's rules
 *    4 green       Gate.autoCut, Rose Gold only (_charmNestGreenLine.js autoUnit): run right after the assembly, where the page ran it
 *    5 review      every set of the run not committed: status "awaiting review"
 *    6 archive     RunCtl.save's archiveFinished + recordOf: the lines of the orders the run is done with go to the run's line archive (runArchive, content-named parts) and
 *                  then leave its live lines (runLinesRetire, which checks each line is archived exactly as it is now)
 *    7 setIds      the run's setIds and setId (saved with the run's record after the unit, recordOf)
 *  The unit list is checkpointUnits(named): `named` is the steps file's registry, where the intake unit is found by name ("intake"). No Etsy call, no world build.
 *  DORMANT: the driver runs this step only while the drive is "standby" or "on" and "checkpoint" is in drive.cloudSteps.                                                       */
"use strict";
const O = require("../../charm-nest-orders.js");
const Assembly = require("./_charmNestAssembly");

const num = v => (Number.isFinite(+v) ? +v : 0);
const words = r => String((r && (r.error || r.why)) || "no answer").slice(0, 300);
const say = (level, text) => { try { console.log(`[checkpoint] ${level}: ${String(text).slice(0, 400)}`); } catch (_) { /* a note only */ } };

/** 1 · the intake at the checkpoint (C5): the step 4 unit, found by its name, given { checkpoint: true }. Absent: nothing to do here. */
const intakeUnit = named => async ctx => {
  const unit = named && (named.intake || named.intakeUnit);
  if (typeof unit !== "function") return undefined;
  const r = await unit(Object.assign({}, ctx, { checkpoint: true }));
  if (r && r.wait === "intake") return undefined;   // (the gate says no intake now: the checkpoint goes on, as processPending's own gate let it)
  return r;
};
/** 2 · LiveNest.finish */
async function finishUnit(ctx) {
  ctx = Assembly.passKeyed(ctx);
  const run = ctx.run, rec = run.intakeRecovery;
  if (!rec || typeof rec !== "object") return undefined;
  await Assembly.assemblyUnit(ctx);
  const ids = [...new Set((Array.isArray(rec.retire) ? rec.retire : []).map(String))], kept = [];
  for (const id of ids) {
    if (Date.now() > ctx.budgetEnd) return { more: true };
    const s = await ctx.db.collection("Charm_Nest_Sheets").doc(id).get(), d = s.exists ? s.data() || {} : null;
    if (!d || d.archived || d.runId !== run.runId) continue;
    if ((d.charms || []).length) { kept.push(id); continue; }   // (a sheet that has pieces again is no empty sheet: the page skipped it, page.charms.length)
    const r = await ctx.call({ op: "archiveEmptySheet", id, runId: run.runId }, `retire|${run.runId}|${id}`);
    if (!r || r.status >= 400) { say("warn", `${d.fileBase || id} was not archived: ${words(r)}`); kept.push(id); }
  }
  // (the back files the recovery names are the engraving lane's to write again: they stay; the retire list is done)
  const backs = Array.isArray(rec.backs) ? rec.backs : [], next = backs.length ? { retire: [], backs } : null;
  const r = await ctx.call({ op: "runPut", run: { runId: run.runId, intakeRecovery: next }, merge: true }, `recovery|${run.runId}|${O.textHash(JSON.stringify(next))}`);
  if (r && r.status < 400) run.intakeRecovery = next;
  return undefined;
}
/** 5 · every set of the run that is not committed: "awaiting review" (the page saved each with Sets.save) */
async function reviewUnit(ctx) {
  ctx = Assembly.passKeyed(ctx);
  const run = ctx.run, q = await ctx.db.collection("Charm_Nest_Sets").where("runId", "==", run.runId).get();
  for (const d of q.docs) {
    const s = d.data() || {}; if (s.committedAt || /^superseded/.test(String(s.status || "")) || s.status === "awaiting review") continue;
    const r = await ctx.call({ op: "setUpdate", setId: d.id, patch: { status: "awaiting review" } }, `review|${d.id}|${s.status || ""}`);
    if (!r || r.status >= 400) throw new Error(`${s.name || d.id} was not saved: ${words(r)}`);
  }
  return undefined;
}
/** 6 · the closed orders' lines: archived (content-named parts, four a call), then retired from the live lines (only a line archived exactly as it is now leaves) */
async function archiveUnit(ctx) {
  const run = ctx.run, lines = run.lines || {}, closed = O.closedOrders(lines);
  if (!closed.size) return undefined;
  const due = [...closed.values()].flat().sort().map(k => [k, lines[k]]).filter(([, l]) => l);
  const parts = O.archiveParts(due);
  for (let i = 0; i < parts.length; i += 4) {
    if (Date.now() > ctx.budgetEnd) return { more: true };
    const batch = parts.slice(i, i + 4);
    const r = await ctx.call({ op: "runArchive", runId: run.runId, parts: batch.map(p => ({ json: p.json })) }, "archive|" + batch.map(p => O.textHash(p.json)).join(","));
    if (!r || r.status >= 400) throw new Error("the finished orders could not be archived: " + words(r));
  }
  const retire = Object.fromEntries(due.map(([k, l]) => [k, O.textHash(JSON.stringify(l))]));
  const keys = Object.keys(retire);
  for (let i = 0; i < keys.length; i += 2000) {
    const part = Object.fromEntries(keys.slice(i, i + 2000).map(k => [k, retire[k]]));
    const r = await ctx.call({ op: "runLinesRetire", runId: run.runId, retire: part, parts: i ? 0 : parts.length }, "retire-lines|" + O.textHash(JSON.stringify(part)));
    if (!r || r.status >= 400) throw new Error("the finished orders could not leave the run's record: " + words(r));
    for (const k of r.retired || []) delete lines[k];
  }
  return undefined;
}
/** 7 · the run's sets (r.setIds = Sets.ofRun(...).map(setId); r.setId = the first) */
async function setIdsUnit(ctx) {
  const run = ctx.run, q = await ctx.db.collection("Charm_Nest_Sets").where("runId", "==", run.runId).get();
  const sets = q.docs.map(d => Object.assign({ setId: d.id }, d.data())).sort((a, b) => String(a.day || "").localeCompare(String(b.day || "")) || num(a.seq) - num(b.seq));
  run.setIds = sets.map(s => s.setId); run.setId = run.setIds[0] || run.setId || null;
  return undefined;
}

/** The checkpoint step's units, in RunCtl's order. green: the automatic cut's unit (it runs right after the assembly). */
function checkpointUnits(named, green) {
  return [intakeUnit(named), finishUnit, Assembly.assemblyUnit].concat(typeof green === "function" ? [green] : []).concat([reviewUnit, archiveUnit, setIdsUnit]);
}

module.exports = { checkpointUnits, intakeUnit, finishUnit, reviewUnit, archiveUnit, setIdsUnit };
/* the steps file's entry (_charmNestSteps.js): the checkpoint step, the automatic Rose Gold cut inside it only while the lane "rosecut" is the cloud's (build step 10b), and the
   assembly's shadow plan (m:C7), run by the driver on its yield path only while drive.shadow names "assembly" */
const greenIn = reg => ctx => ((ctx.cloudSteps || []).includes("rosecut") && typeof reg.named.greenAuto === "function" ? reg.named.greenAuto(ctx) : undefined);
module.exports.units = reg => ({ checkpoint: checkpointUnits(reg.named, greenIn(reg)), shadow: o => (o && o.drive && Array.isArray(o.drive.shadow) && o.drive.shadow.includes("assembly") ? Assembly.shadow({ db: o.db, runId: o.runId }) : null) });
module.exports.greenIn = greenIn;
