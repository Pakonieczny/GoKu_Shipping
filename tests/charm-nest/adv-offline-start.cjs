// ADV-OFFLINE (Paul, 15 Oct 2026: "resilient against lost internet connections"): Auto starts a run while the cloud answers 502 (or the connection drops) for the run's very first save.
// The save threw before the run's loop was started, so the run stood "running" at Pull for good: the banner said "Pulling orders", nothing pulled, nothing stopped it and Auto's own
// resume only takes up a STOPPED run. Found by a whole run with the first call of every operation lost (adv-offline-run.cjs, CHAOS=lost: still at Pull after 7 minutes).
// A passing failure now lets the run go on (its record goes online with the next save); any other failure stops the run, visibly, to be resumed. The real start/next code runs in node's vm. No network.
//   /tmp/cn-run.sh 120 tests/charm-nest/adv-offline-start.cjs
"use strict";
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path"), vm = require("node:vm");
const bridge = fs.readFileSync(process.env.BRIDGE_SRC || path.join(__dirname, "../../charm-nest-bridge.js"), "utf8");
const part = (src, a, b) => { const i = src.indexOf(a); assert(i >= 0, "not found: " + a); const j = src.indexOf(b, i); assert(j > i, "not found: " + b); return src.slice(i, j); };
const from = bridge.indexOf("  const saveSoft = ") >= 0 ? "  const saveSoft = " : "  async function start(opts = {}) {";   // (the old code has no saveSoft)
const code = part(bridge, from, "  let loopTask=null") + "\nthis.start = start;";

function world(failWith) {
  const c = { console, Promise, Object, Error, Date, failWith, log: { loops: 0, stops: [], saves: 0 } };
  vm.createContext(c);
  vm.runInContext(`
    const B = { run: null }, Recall = { on: () => false }, Gate = { state: () => ({}) }, claimRun = () => true, holdsRun = async () => true;   // (the second-tab run lock, ADV-AUTORUN: this tab holds it)
    const toast = () => {}, agent = () => {}, renderBanner = () => {}, clearRunState = () => true, newRun = mode => ({ runId: "run-1", status: "running", step: "pull", mode, errors: [] });
    const loop = () => { log.loops++; return Promise.resolve(); };
    const stop = (why, fix) => { log.stops.push(why); B.run.status = "stopped"; B.run.stoppedBy = why; };
    const save = async () => { log.saves++; if (failWith) throw failWith; };
  `, c);
  vm.runInContext(code, c);
  return c;
}

(async () => {
  // a quiet start: the loop goes
  { const c = world(null); const r = await c.start({ mode: "auto" }); assert.equal(r.status, "running"); assert.equal(c.log.loops, 1); assert.equal(c.log.stops.length, 0); }
  // the cloud answers 502 / the connection drops at the run's first save: the run goes on, it is not left standing
  for (const why of ["The cloud is not answering right now (HTTP 502)", "Failed to fetch"]) {
    const c = world(Object.assign(new Error(why), { transient: true }));
    const r = await c.start({ mode: "auto" });
    assert.equal(r.status, "running", why); assert.equal(c.log.loops, 1, "its steps are started: " + why); assert.equal(c.log.stops.length, 0);
  }
  // any other failure: the run is stopped, visibly, to be resumed (it was left "running" with no loop)
  { const c = world(Object.assign(new Error("run record too large"), { status: 400 }));
    await assert.rejects(c.start({ mode: "auto" }), /too large/); assert.equal(c.log.loops, 0); assert.deepEqual(Array.from(c.log.stops), ["run record too large"]); }
  console.log("adv-offline-start OK: a run whose first save meets a 502 or a lost connection goes on; any other failure stops it visibly");
})().catch(e => { console.error(e); process.exit(1); });
