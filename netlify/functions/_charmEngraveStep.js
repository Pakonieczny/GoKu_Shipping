/*  netlify/functions/_charmEngraveStep.js — THE RUN'S "engrave" STEP IN THE CLOUD (cloud plan v2 §1.7 "engrave: the engraving units. This step never blocks"; build step 11;
 *  ONSWITCH, Paul 20 Nov 2026: the cloud carries every run tonight, so no run step may stay a stub).
 *  RunCtl.doStep "engrave" (charm-nest-bridge.js ~16053), from the records:
 *    "Nothing here waits for engraving (Paul, 24 Sep): approved backs not written yet are written, and new lines' words read and fitted, beside the run. A set still waits for
 *     them before it goes to the laser, and the run looks again when they are done."
 *  In the cloud the engraving work is the lane "engrave" (pass items, every pass, while "engrave" is in drive.cloudSteps): the reading of a line's words (_charmEngraveClassify.js,
 *  order 80), the fit, the back files of approved engravings (approved before their piece was on a sheet too: ENGPRE) and the sheet's back index (_charmEngraveFiles.js, 81-83).
 *  This step runs those units once more at this moment of the run (the driver's ctx.lane: each with its own budget, cursor and saved state, as in the pass), so the lines the
 *  checkpoint just placed are read and fitted now, as the page's Engrave.background(r) did; then the run's pendingWork from the records (the page's updatePending). It never
 *  waits: a unit that has more to do goes on in the next pass, a failure is said, and the step ends. A lane a computer keeps (not in cloudSteps) is never run here (P9). The
 *  set still waits for its engravings at its release check (labels), and a resting run whose engraving changed goes through its steps again (the driver's poke).
 *  No Etsy call, no world build, no agent of its own (the paid reading is the classify unit's, engraveIntentStart). DORMANT: only while the drive is "standby" or "on" and
 *  "engrave" is in drive.cloudSteps.                                                                                                                                           */
"use strict";

const say = (ctx, text) => { try { (ctx && ctx.say ? ctx.say : t => console.log(t))(`[engrave step] ${String(text).slice(0, 400)}`); } catch (_) { /* a note only */ } };

/** The step's one unit. → undefined, always (the run goes on to revalidate). */
async function engraveStep(ctx) {
  const run = ctx.run;
  let lane = null;
  if ((ctx.cloudSteps || []).includes("engrave") && typeof ctx.lane === "function") {
    try { lane = await ctx.lane("engrave"); }
    catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; say(ctx, `the engraving lane: ${(e && e.message) || e} (it goes on in the next pass)`); }
  }
  // (the page's updatePending: what still waits in the run, from the records; saved with the run's record after the unit)
  try {
    const sheets = typeof ctx.sheets === "function" ? await ctx.sheets() : [];
    require("./_charmNestDrive").updatePending(run, { sheets });
  } catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; say(ctx, `what waits was not counted: ${(e && e.message) || e}`); }
  const waits = run.pendingWork && run.pendingWork.engraving;
  if (waits) say(ctx, `${waits} line(s) still wait for their engraving: the run goes on, their sets wait for them before the laser`);
  if (lane && lane.ran && lane.ran.length) ctx.engraveRan = lane.ran;
  return undefined;
}

module.exports = { engraveStep, units: { engrave: [engraveStep] } };
