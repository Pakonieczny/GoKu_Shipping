// Shared fake world for the cn-poll tests (FC15): a virtual clock, tabs with their own document / visibility / online state, Web Locks,
// BroadcastChannel, a shared localStorage and a fake fetch that records every call. No network, no browser.
"use strict";
const path = require("node:path");
const CnPoll = require(path.join(__dirname, "../../cn-poll.js"));
let t0 = 1_700_000_000_000;
function makeWorld() {
  const w = { t: t0, timers: new Map(), seq: 0, calls: [], rev: 1, latency: 0, serverFn: null, ls: new Map(), channels: [], locks: new Map(), tabs: [] };
  w.now = () => w.t;
  w.flush = async () => { for (let i = 0; i < 10; i++) await new Promise(r => setImmediate(r)); };
  w.advance = async (ms) => {
    const end = w.t + ms;
    await w.flush();
    for (;;) {
      let next = null;
      for (const t of w.timers.values()) if (t.at <= end && (!next || t.at < next.at || (t.at === next.at && t.id < next.id))) next = t;
      if (!next) break;
      w.timers.delete(next.id); w.t = Math.max(w.t, next.at); next.f(); await w.flush();
    }
    w.t = end; await w.flush();
  };
  // Web Locks (FIFO queue per name, abort removes a waiting request, a crashed tab loses its locks)
  const entry = n => { if (!w.locks.has(n)) w.locks.set(n, { holder: null, queue: [] }); return w.locks.get(n); };
  const pump = n => {
    const e = entry(n);
    if (e.holder || !e.queue.length) return;
    const r = e.queue.shift(); e.holder = r; r.granted = true;
    Promise.resolve().then(() => r.cb({ name: n })).then(v => Promise.resolve(v)).then(
      v => { if (e.holder === r) { e.holder = null; } r.resolve(v); pump(n); }, er => { if (e.holder === r) e.holder = null; r.reject(er); pump(n); });
  };
  w.locksFor = tab => ({
    request(name, opts, cb) {
      return new Promise((resolve, reject) => {
        const r = { tab, cb, resolve, reject, granted: false };
        const sig = opts && opts.signal;
        if (sig) {
          if (sig.aborted) return reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
          sig.addEventListener("abort", () => { if (!r.granted) { const q = entry(name).queue, i = q.indexOf(r); if (i >= 0) q.splice(i, 1); reject(Object.assign(new Error("aborted"), { name: "AbortError" })); } });
        }
        entry(name).queue.push(r); pump(name);
      });
    }
  });
  w.heldBy = name => { const h = entry(name).holder; return h ? h.tab.name : null; };
  w.crash = tab => {
    tab.dead = true;
    for (const [id, t] of [...w.timers]) if (t.tab === tab) w.timers.delete(id);
    for (const [n, e] of w.locks) { e.queue = e.queue.filter(r => r.tab !== tab); if (e.holder && e.holder.tab === tab) { e.holder = null; pump(n); } }
    w.channels = w.channels.filter(c => c.tab !== tab);
  };
  w.server = call => {
    if (w.serverFn) return w.serverFn(call);
    const etag = '"r' + call.rev + '"';
    if (call.inm === etag) return new Response(null, { status: 304, headers: { ETag: etag } });
    return new Response(JSON.stringify({ rev: call.rev, items: [call.rev] }), { status: 200, headers: { ETag: etag } });
  };
  return w;
}

function makeTab(w, name, o = {}) {
  const t = { name, hidden: !!o.hidden, online: true, dead: false, L: { doc: {}, win: {} }, data: [], unchanged: 0, errors: [], datas: [] };
  const target = k => ({
    addEventListener(n, f) { (t.L[k][n] = t.L[k][n] || new Set()).add(f); },
    removeEventListener(n, f) { if (t.L[k][n]) t.L[k][n].delete(f); }
  });
  const doc = target("doc"), win = target("win");
  Object.defineProperty(doc, "hidden", { get: () => t.hidden });
  t.fire = (k, n, ev) => { for (const f of [...(t.L[k][n] || [])]) f(ev || {}); };
  t.setHidden = h => { t.hidden = h; t.fire("doc", "visibilitychange"); };
  t.setOnline = v => { t.online = v; t.fire("win", v ? "online" : "offline"); };
  t.activity = () => t.fire("doc", "pointerdown");
  t.setTimeout = (f, ms) => { const id = ++w.seq; w.timers.set(id, { id, at: w.t + ms, f, tab: t }); return id; };
  t.clearTimeout = id => { w.timers.delete(id); };
  const BC = class { constructor(n) { this.name = n; this.tab = t; this.closed = false; w.channels.push(this); }
    postMessage(m) { const d = JSON.parse(JSON.stringify(m)); for (const c of w.channels) if (c !== this && c.name === this.name && !c.closed && c.onmessage && !c.tab.dead) setImmediate(() => c.onmessage({ data: d })); }
    close() { this.closed = true; w.channels = w.channels.filter(c => c !== this); } };
  const fetchFn = (url, init) => new Promise((resolve, reject) => {
    const h = init.headers || {}, call = { tab: name, t: w.t, url, inm: h["If-None-Match"] || null, method: init.method || "GET", body: init.body || null, cache: init.cache, rev: w.rev };
    w.calls.push(call);
    const sig = init.signal;
    if (sig) sig.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })));
    const run = () => { if (t.dead) return; try { Promise.resolve(w.server(call)).then(resolve, reject); } catch (e) { reject(e); } };
    if (w.latency) t.setTimeout(run, w.latency); else Promise.resolve().then(run);
  });
  t.env = {
    now: w.now, setTimeout: t.setTimeout, clearTimeout: t.clearTimeout, random: o.random || (() => 0.5), fetch: fetchFn,
    document: doc, window: win,
    navigator: Object.assign({ get onLine() { return t.online; } }, o.noLocks ? {} : { locks: w.locksFor(t) }),
    BroadcastChannel: o.noBC ? null : BC, AbortController, localStorage: o.storage ? {
      getItem: k => (w.ls.has(k) ? w.ls.get(k) : null), setItem: (k, v) => { w.ls.set(k, String(v)); }, removeItem: k => { w.ls.delete(k); } } : null
  };
  t.poll = (opts = {}) => {
    const p = CnPoll.create(Object.assign({
      key: "k", url: "/api", init: { method: "POST", body: "{}" }, intervalMs: 3000, env: t.env,
      onData: (d, i) => { t.data.push(d); t.datas.push({ d, i, at: w.t }); }, onUnchanged: () => { t.unchanged++; }, onError: e => { t.errors.push(e); }
    }, opts));
    t.p = p; return p;
  };
  w.tabs.push(t);
  return t;
}
module.exports = { makeWorld, makeTab, CnPoll };
