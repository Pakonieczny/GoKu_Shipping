// Adversarial (wave 3, area 14): a sandbox reset leaves nothing of the rehearsal on its way to the sandbox timeline.
// The timeline's outbox (order-timeline.js) is kept in localStorage across the reload the reset ends with, and the sorter
// queues its events (CNTimeline) before handing them over: a rehearsal event still waiting there (recorded a moment
// before, or held back while the server was refusing) was sent after the wipe, onto the replay of the same real order
// number, or right after the reload. The reset now drops the sandbox's waiting events (every page's), keeps
// production's, and forgets which events the sandbox already sent once, so the replay records them again.
// The real order-timeline.js, CNTimeline and Sandbox run in node's vm with stand-ins around them. No network.
//   node tests/charm-nest/adv-sandbox-reset-outbox.cjs
"use strict";
const assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path"), vm = require("node:vm");
const root = path.join(__dirname, "../..");
const bridge = fs.readFileSync(path.join(root, "charm-nest-bridge.js"), "utf8"), outbox = fs.readFileSync(path.join(root, "order-timeline.js"), "utf8");
const part = (src, a, b) => { const i = src.indexOf(a); assert(i >= 0, "not found: " + a); const j = src.indexOf(b, i); assert(j > i, "not found: " + b); return src.slice(i, j); };
const turn = () => new Promise(r => setImmediate(r));
const RID = "4200000201", OUTBOX = "orderTimeline.outbox.v1";

function page(stream) {
  const seq = [], timers = [], ls = new Map();
  const c = { console, JSON, Math, Date, Promise, String, Number, Set, Map, Array, Object, Error, encodeURIComponent, seq, timers };
  c.window = c;
  c.localStorage = { getItem: k => (ls.has(k) ? ls.get(k) : null), setItem: (k, v) => ls.set(k, String(v)), removeItem: k => ls.delete(k) };
  c.sessionStorage = { getItem: () => null, setItem() {}, removeItem() {} };
  c.location = { protocol: "https:", reload() { seq.push("reload"); } };
  c.document = { hidden: false, visibilityState: "visible", addEventListener() {}, getElementById: () => null, documentElement: { classList: { toggle() {} } } };
  c.addEventListener = () => {};
  c.setTimeout = (f, ms) => { timers.push(f); return timers.length; }; c.clearTimeout = () => {};
  c.fetch = async (url, o) => { const b = JSON.parse(o.body); if (b.op === "timelineAdd") seq.push({ sent: b.events.map(e => e.id), sandbox: b.sandbox }); return { ok: true, status: 200, json: async () => ({ ok: true }) }; };
  vm.createContext(c);
  vm.runInContext(`
    const WORKSPACE_SANDBOX = true;
    const S = { settings: { sandbox: "on", sandboxStream: ${JSON.stringify(stream)}, sandboxSpeed: 50, sandboxSeed: 0 }, cloud: { ok: true }, passcode: "" };
    const B = { employee: "Tester" }, employeeName = () => B.employee;
    const api = async (fn, body) => { seq.push("api:" + body.op); return body.op === "sandboxReset" ? { ok: true, more: false, deleted: 4, files: 0 } : {}; };
    const toast = () => {}, agent = () => {}, confirm = () => true;
    const Arrivals = { pause: async () => {}, resume() {}, reset() {} };
    const RunCtl = { clearRunState: () => { window.CNTimeline.rec({ orderId: "${RID}", type: "removed", id: "clear-1", text: "Taken off with the run" }); return true; } };
    const DesignLink = { ensure: async () => {}, state: () => ({ commands: [] }), call: async () => {} };
    const SimClock = { set() {}, on: () => false, now: () => Date.now() };
  `, c);
  vm.runInContext(outbox, c);
  vm.runInContext(part(bridge, "const TL = window.CNTimeline = (() => {", "/* One drawing a frame."), c);
  vm.runInContext(part(bridge, "const Sandbox = window.Sandbox = (() => {", "/* ═══ 24a · TeamMail"), c);
  const run = async () => { for (let k = 0; k < 20 && timers.length; k++) { for (const f of timers.splice(0)) f(); for (let i = 0; i < 10; i++) await turn(); } };
  return { c, seq, ls, run, disk: () => JSON.parse(ls.get(OUTBOX) || "[]") };
}

(async () => {
  /* ── 1 · the stream's replay: the reset, then the reload ── */
  {
    const p = page("on"), { c, seq } = p;
    // a production station event another page left on the disk: not the sandbox's, it stays
    p.ls.set(OUTBOX, JSON.stringify([{ orderId: RID, type: "scan", id: "prod-scan", sandbox: false, mode: "station", at: Date.now() }]));
    // the rehearsal: one event handed over to the outbox (not sent yet), one still in the sorter's own queue
    c.OrderTimeline.config({ mode: "sorter", sandbox: true, by: "Tester" });
    c.OrderTimeline.record({ orderId: RID, type: "placed", id: "rehearsal-placed", text: "On GF Sheet 1" });
    c.CNTimeline.rec({ orderId: RID, type: "qrLabel", id: "rehearsal-qr", text: "QR label for GF Sheet 1" });
    assert(p.disk().some(e => e.id === "rehearsal-placed"), "the rehearsal event waits in the outbox");
    await c.Sandbox.reset();
    await p.run();
    const wipe = seq.lastIndexOf("api:sandboxReset");
    assert(wipe >= 0 && seq.includes("reload"), "the reset ran and the page reloads");
    const after = seq.slice(wipe + 1).filter(x => x && x.sent && x.sandbox);
    assert.deepEqual(after, [], "a rehearsal event reached the sandbox timeline after its wipe: " + JSON.stringify(after));
    const left = p.disk().filter(e => e.sandbox);
    assert.deepEqual(left.map(e => e.id), [], "a rehearsal event waits on the disk for the reloaded page: " + JSON.stringify(left.map(e => e.id)));
    assert(p.disk().some(e => e.id === "prod-scan"), "production's waiting event is kept");
  }
  /* ── 2 · the whole snapshot at once: no reload, the replay's events are recorded again ── */
  {
    const p = page("off"), { c } = p;
    c.OrderTimeline.config({ mode: "sorter", sandbox: true, by: "Tester" });
    assert.equal(c.CNTimeline.rec({ orderId: RID, type: "interpreted", id: "k.1" }, true), true, "first reading recorded");
    assert.equal(c.CNTimeline.rec({ orderId: RID, type: "interpreted", id: "k.1" }, true), false, "once: the same reading again is not");
    await p.run();
    await c.Sandbox.reset(); await p.run();
    assert.equal(c.CNTimeline.rec({ orderId: RID, type: "interpreted", id: "k.1" }, true), true, "after the reset the replay's reading is recorded on its (cleared) timeline again");
  }
  console.log("adv-sandbox-reset-outbox OK: a sandbox reset drops the rehearsal's waiting timeline events (the outbox on disk and the sorter's queue), keeps production's, and the replay's once-events are recorded again");
})().catch(e => { console.error(e); process.exit(1); });
