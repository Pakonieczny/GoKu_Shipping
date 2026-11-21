/*  netlify/functions/_charmNestSteps.js — the cloud driver's steps and pass items, from the unit modules of the build steps (cloud plan v2 §1.6-1.7; hard-parts §0.1: "a new file exports
 *  { steps, passItems } built with one tryUnits(require-path) line per unit module (a module that is missing is skipped)"; each build step adds exactly its own lines, so the copies merge
 *  by adding lines). charmNestDrive-background.js hands the driver (_charmNestDrive.js pass) { steps, passItems, shadow } from here, and the driver's defaultSteps / defaultLanes read it.
 *
 *  A unit module exports `units` (an object, or a function of this registry (reg) giving one), or gives the same keys on its own exports:
 *    { passItems: [{ name, lane, order?, unit }],   run by pass() before the run's current step, each only while its lane is in drive.cloudSteps, in `order` (50 when none)
 *      <runStep>: [unit, ...]  or  steps: { <runStep>: [unit, ...] },   the units of one of O.RUN_STEPS (pull ... complete); a step no module gives keeps the driver's own units or stub
 *      named: { name: unit } }                     units another module may call by name (reg.named), e.g. the checkpoint calls the intake unit with { checkpoint: true }
 *  `shadow` (optional): a plan made while a computer still carries the run (charmNestDrive-background passes it; the driver runs it on its yield path only when drive.shadow
 *  names it). DORMANT: nothing here runs unless the driver runs, which it does only while Charm_Nest_Open/drive is "standby" or "on".
 *  Only the named module itself may be missing (its line is skipped); one that is there but cannot load, or misses a module of its own, fails the pass (never a step run as a stub
 *  by mistake). Each line has its own STATIC require: the functions are bundled by esbuild, which follows only a require it can read.                                         */
"use strict";
const O = require("../../charm-nest-orders.js");
const Drive = require("./_charmNestDrive");

const steps = Drive.stubSteps(), passItems = [], named = {}, shadows = {}, given = new Set(), loaded = [], reg = { steps, passItems, named, shadows };
const orderOf = it => (it && Number.isFinite(+it.order) ? +it.order : 50);
/** tryUnits(path, load): load is `() => require("<path>")`, written out on each line so the bundler (esbuild) sees every unit module; a module that is missing at run time is skipped. */
function tryUnits(path, load) {
  let mod;
  try { mod = typeof load === "function" ? load() : require(path); }
  catch (e) { if (e && e.code === "MODULE_NOT_FOUND" && String(e.message || "").split("\n")[0].includes(String(path).replace(/^\.\//, ""))) return false; throw e; }
  const u0 = (mod && mod.units) || mod; if (!u0) return false;
  const units = (typeof u0 === "function" ? u0(reg) : u0) || {};
  for (const it of Array.isArray(units.passItems) ? units.passItems : []) if (it && it.name && typeof it.unit === "function" && !passItems.some(p => p.name === it.name)) passItems.push(it);
  passItems.sort((a, b) => orderOf(a) - orderOf(b));
  if (units.named && typeof units.named === "object") Object.assign(named, units.named);
  if (typeof units.shadow === "function") shadows[path] = units.shadow;
  const bySteps = Object.assign({}, units.steps && typeof units.steps === "object" ? units.steps : {});
  for (const s of O.RUN_STEPS) if (Array.isArray(units[s])) bySteps[s] = (bySteps[s] || []).concat(units[s]);
  for (const [k, v] of Object.entries(bySteps)) {
    if (!O.RUN_STEPS.includes(k) || !Array.isArray(v) || !v.length || !v.every(f => typeof f === "function")) continue;
    steps[k] = given.has(k) ? steps[k].concat(v) : v.slice(); given.add(k);   // (the first module that gives a step replaces the driver's own; a later one adds its units after)
  }
  loaded.push(path);
  return true;
}

/* ── one line per unit module ── */
tryUnits("./_charmNestIntake", () => ({ units: { named: { intake: require("./_charmNestIntake").checkpointUnit } } }));   // build step 4: the intake at the checkpoint, the unit the checkpoint step calls by name first (C5)
tryUnits("./_charmNestCheckpoint", () => require("./_charmNestCheckpoint"));   // build step 10a: the checkpoint step (intake at the checkpoint, retire of empty sheets, the set assembly, sets awaiting review, the closed lines archived)
tryUnits("./_charmNestLabels", () => require("./_charmNestLabels"));           // build step 10a: the labels pass item (presses, owed QR labels) and the labels step (each set's release check and files); the assembly's shadow
tryUnits("./_charmNestGreenLine", () => require("./_charmNestGreenLine"));     // build step 10b: the green-line presses (lane "rosecut"); the automatic Rose Gold cut, called in checkpoint and labels
tryUnits("./_charmEngraveClassify", () => require("./_charmEngraveClassify"));   // build step 11 (C3): lane "engrave": the reading of a line's words, before the fit (order 80)
tryUnits("./_charmEngraveFiles", () => require("./_charmEngraveFiles"));       // build step 11: lane "engrave": the fit, the back files, the sheet's back index (order 81-83)
tryUnits("./_charmEngraveStep", () => require("./_charmEngraveStep"));         // ONSWITCH: the run's "engrave" step: the engraving lane once more at its moment, then what waits; never blocks
tryUnits("./_charmNestCommit", () => require("./_charmNestCommit"));           // build step 12: commit, complete (sending finished sets to the Design Station)
tryUnits("./_charmNestCmdUnit", () => require("./_charmNestCmdUnit"));   // build step 13: a person's requests (cmd:<type>), order 40
tryUnits("./_charmNestSweeps", () => require("./_charmNestSweeps"));     // build step 13: the repair sweeps (lane "sweeps"), order 90: after every other pass item
tryUnits("./_charmNestCombine", () => require("./_charmNestCombine"));   // COMBINEDRAFTS (21 Nov 2026): underfilled Gold Filled / Sterling Silver drafts combined, one a pass (lane "nest"), order 55
// build step 13: a Release Hold the cloud began and a function left half done ("lifting") is finished by the driver (Release Hold in the cloud: _charmNestHoldRelease.js driveUnit),
// order 20, lane "cmd:release". (the require sits in its own try: a copy without that module is still bundled, and the line is skipped). The driver's pass runs the same
// driveUnit itself every pass (releasesUnit, RELEASEHOLD live), so it skips this item in a pass that already did it: one release unit a pass.
tryUnits("./_charmNestHoldRelease", () => { let m; try { m = require("./_charmNestHoldRelease"); } catch (e) { throw e; } return { passItems: typeof m.driveUnit === "function" ? [{ name: "release", lane: "cmd:release", order: 20, unit: ctx => m.driveUnit({ db: ctx.db, call: ctx.call }).then(() => undefined) }] : [] }; });

// build step 7b: lane "files" (the one sheet writer, _charmNestFinishLane.js) is built: its pass item makes it so (the driver's defaultLanes read the pass items' lanes, and readiness with
// them). The driver looks at the lane itself every pass (its own block in pass, build step 7a: a stopped run's sheets too) and skips this item in a pass that did: one files look a pass.
tryUnits("./_charmNestFinishLane", () => { const m = require("./_charmNestFinishLane"); return { passItems: [{ name: "files", lane: "files", order: 60, unit: async ctx => { const q = await ctx.db.collection("Charm_Nest_Sheets").where("runId", "==", ctx.runId).get(); await m.driveUnit({ db: ctx.db, runId: ctx.runId, sheets: q.docs.map(d => Object.assign({ id: d.id }, d.data())), call: ctx.call, log: ctx.say }); return undefined; } }] }; });

/** The driver's shadow plans, one call: each module's, in load order. → { [path]: answer } */
async function shadow(o) { const out = {}; for (const [p, f] of Object.entries(shadows)) { try { out[p] = await f(o); } catch (e) { out[p] = { error: String((e && e.message) || e).slice(0, 200) }; } } return out; }

/** The run steps for the driver: every step a module fills, the driver's own units (stubSteps) or the given stub for the rest. (build step 13) */
function stepsFor(stub) { const out = {}; for (const s of O.RUN_STEPS) out[s] = steps[s] && steps[s].length ? steps[s] : (typeof stub === "function" ? stub() : steps[s]); return out; }
module.exports = { steps, passItems, named, shadow, tryUnits, stepsFor, loaded };
