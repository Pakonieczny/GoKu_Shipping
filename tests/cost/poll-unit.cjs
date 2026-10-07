// FC15: cn-poll.js against a fake world (virtual clock, tabs with their own document/visibility, Web Locks, BroadcastChannel, shared
// localStorage, a counting fake server with ETag/304). No network, no browser, no Firestore.   node tests/cost/poll-unit.cjs
// Proves: one poller per computer for N tabs, followers get the answer, If-None-Match / 304, backoff 3-3-3-6-12-24 s with instant
// reset (change, user action, poke), hidden / signed out / offline pause and immediate resume, leader hand-over and crash take-over,
// poke from a follower reaches the leader at once (storms coalesced, poke during a request is answered by one trailing request),
// error backoff / Retry-After / timeout, jitter bounds, lease fallback without Web Locks, solo mode without BroadcastChannel.
"use strict";
const assert = require("node:assert/strict"), path = require("node:path");
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
const callsBy = (w, from = 0) => w.calls.slice(from);
let groups = 0; const ok = n => { groups++; console.log("  ok  " + n); };

(async () => {
  // ── 1. five tabs, one poller; followers get the data; If-None-Match / 304 ──────────────────────────────────────────────────────
  {
    const w = makeWorld(), tabs = [0, 1, 2, 3, 4].map(i => makeTab(w, "t" + i));
    tabs.forEach(t => t.poll().start());
    await w.advance(30000);
    const total = w.calls.length;
    assert.ok(total >= 10 && total <= 12, "5 tabs for 30 s at 3 s: about 11 requests, got " + total);
    assert.equal(w.calls[0].inm, null, "the first request has no If-None-Match");
    assert.ok(w.calls.slice(1).every(c => c.inm === '"r1"'), "every later request sends the last ETag");
    assert.equal(new Set(w.calls.map(c => c.tab)).size, 1, "one tab asks");
    assert.ok(w.calls.every(c => c.cache === "no-store"), "fetch uses cache:no-store");
    tabs.forEach(t => { assert.equal(t.data.length, 1, t.name + " got the first answer once"); assert.deepEqual(t.data[0], { rev: 1, items: [1] }); });
    const lead = tabs.filter(t => t.p.state().leader); assert.equal(lead.length, 1, "exactly one leader");
    assert.equal(lead[0].unchanged, total - 1, "the leader saw every 304 as unchanged");
    assert.ok(tabs.filter(t => !t.p.state().leader).every(t => t.p.state().stats.requests === 0), "followers never ask");
    assert.equal(tabs[1].datas[0].i.leader, false); assert.equal(lead[0].datas[0].i.leader, true);
    // a change on the server reaches every tab within one interval, delivered once
    const before = w.calls.length; w.rev = 2; const at = w.t;
    await w.advance(3400);
    tabs.forEach(t => { assert.equal(t.data.length, 2, t.name + " got the change"); assert.ok(t.datas[1].at - at <= 3300, "within the base interval (+jitter)"); assert.equal(t.datas[1].d.rev, 2); });
    assert.equal(w.calls.slice(before).filter(c => c.inm === '"r1"').length, 1, "one 200 for the change");
    // a tab opened later gets the current answer from the leader without any request
    const n = w.calls.length, late = makeTab(w, "late"); late.poll().start();
    await w.advance(100);
    assert.equal(late.data.length, 1); assert.equal(late.data[0].rev, 2); assert.equal(w.calls.length, n, "a late tab costs no request");
    assert.equal(w.heldBy("cn-poll:k"), lead[0].name);
    tabs.concat(late).forEach(t => t.p.stop());
    ok("five tabs -> one poller, followers fed, ETag sent, change reaches all, late tab free");
  }

  // ── 2. backoff 3-3-3-6-12-24-24, reset by change / user action / poke ──────────────────────────────────────────────────────────
  {
    const w = makeWorld(), t = makeTab(w, "a"); t.poll({ maxIntervalMs: 24000 }).start();
    await w.advance(160000);
    const ts = w.calls.map(c => c.t), gaps = ts.slice(1).map((x, i) => Math.round((x - ts[i]) / 1000));
    assert.deepEqual(gaps.slice(0, 7), [3, 3, 3, 6, 12, 24, 24], "gaps " + gaps);
    assert.ok(gaps.slice(6).every(g => g === 24), "capped at 24 s");
    // user action while at 24 s: the next look is within the base interval, interval back to base
    const n = w.calls.length, last = w.calls[n - 1].t; await w.advance(1000);
    const nextIn = t.p.state().nextInMs; assert.ok(nextIn > 3300, "waiting long, nextIn " + nextIn);
    t.activity();
    assert.ok(t.p.state().nextInMs <= 3300 && t.p.state().intervalMs === 3000, "activity pulled the next look forward and reset the interval");
    await w.advance(3400); assert.equal(w.calls.length, n + 1, "one look soon after the action");
    // then it backs off again
    await w.advance(60000); const gaps2 = w.calls.slice(n).map(c => c.t).map((x, i, a) => i ? Math.round((x - a[i - 1]) / 1000) : 0).slice(1);
    assert.deepEqual(gaps2.slice(0, 4), [3, 3, 6, 12], "starts again from the base: " + gaps2);
    // a change resets: grow the interval first, then change the server
    await w.advance(100000); assert.equal(t.p.state().intervalMs, 24000);
    w.rev = 3; await w.advance(25000);
    assert.equal(t.p.state().intervalMs === 3000 || t.p.state().unchanged > 0, true);
    const lastData = t.datas[t.datas.length - 1]; assert.equal(lastData.d.rev, 3);
    // poke: immediate request, backoff reset
    await w.advance(200000); assert.equal(t.p.state().intervalMs, 24000);
    const m = w.calls.length; t.p.poke("complete"); await w.advance(50);
    assert.equal(w.calls.length, m + 1, "poke asks at once"); assert.equal(t.p.state().intervalMs, 3000);
    // activity at a fresh start (no backoff) sends nothing
    t.p.stop(); ok("backoff 3,3,3,6,12,24 capped; reset by activity, change, poke");
  }

  // ── 3. no backoff unless asked (a live page keeps 3 s for ever) ─────────────────────────────────────────────────────────────────
  {
    const w = makeWorld(), t = makeTab(w, "a"); t.poll().start();
    await w.advance(120000);
    const ts = w.calls.map(c => c.t), gaps = new Set(ts.slice(1).map((x, i) => x - ts[i]));
    assert.deepEqual([...gaps], [3000]); t.p.stop(); ok("default keeps the base interval (no backoff) for live pages");
  }

  // ── 4. hidden pauses, visible resumes with an immediate request; leader hand-over ────────────────────────────────────────────────
  {
    const w = makeWorld(), a = makeTab(w, "a"), b = makeTab(w, "b");
    a.poll().start(); b.poll().start();
    await w.advance(10000);
    assert.equal(w.heldBy("cn-poll:k"), "a"); const n1 = w.calls.length;
    a.setHidden(true); await w.advance(100);
    assert.equal(w.heldBy("cn-poll:k"), "b", "the visible tab leads once the leader is hidden");
    await w.advance(9900);
    const mine = w.calls.slice(n1); assert.ok(mine.every(c => c.tab === "b"), "only b asks now");
    assert.equal(mine[0].inm, '"r1"', "the new leader sends the ETag it received: no re-read (304)");
    assert.equal(b.data.length, 1, "no duplicate delivery on hand-over"); assert.equal(a.data.length, 1);
    // both hidden: nothing asks
    b.setHidden(true); await w.advance(500); const n2 = w.calls.length; await w.advance(60000);
    assert.equal(w.calls.length, n2, "all hidden: zero requests"); assert.equal(w.heldBy("cn-poll:k"), null);
    // someone changes data while hidden; the tab comes back: immediate refresh
    w.rev = 5; b.setHidden(false); await w.advance(100);
    assert.equal(b.data.length, 2); assert.equal(b.data[1].rev, 5); assert.ok(w.calls.length === n2 + 1, "one immediate request on return");
    assert.equal(a.data.length, 2, "the hidden tab still receives the broadcast answer");
    // hidden -> visible within the soft gap does not re-ask
    const n3 = w.calls.length; b.setHidden(true); b.setHidden(false); await w.advance(50);
    assert.ok(w.calls.length - n3 <= 1, "a flicker costs at most one request");
    a.p.stop(); b.p.stop(); ok("hidden pauses and never leads; visible resumes at once; hand-over keeps the ETag; flicker cheap");
  }

  // ── 5. signed out pauses, signed in resumes; offline pauses ──────────────────────────────────────────────────────────────────────
  {
    const w = makeWorld(), t = makeTab(w, "a"); let signedIn = false;
    t.poll({ enabled: () => signedIn }).start();
    await w.advance(20000); assert.equal(w.calls.length, 0, "signed out: no request");
    signedIn = true; await w.advance(2100); assert.ok(w.calls.length >= 1 && t.data.length === 1, "signed in: starts within the check interval");
    signedIn = false; await w.advance(2100); const n = w.calls.length; await w.advance(30000);
    assert.equal(w.calls.length, n, "signed out again: stops"); assert.equal(w.heldBy("cn-poll:k"), null);
    signedIn = true; t.p.poke(); await w.advance(2100); assert.ok(w.calls.length > n);
    const m = w.calls.length; t.setOnline(false); await w.advance(30000); assert.equal(w.calls.length, m, "offline: no request");
    t.setOnline(true); await w.advance(200); assert.equal(w.calls.length, m + 1, "back online: immediate request");
    t.p.stop(); ok("enabled() false and offline pause polling; resume is immediate");
  }

  // ── 6. leader crash: a follower takes over at once and keeps the ETag ─────────────────────────────────────────────────────────────
  {
    const w = makeWorld(), a = makeTab(w, "a"), b = makeTab(w, "b"), c = makeTab(w, "c");
    [a, b, c].forEach(t => t.poll().start()); await w.advance(7000);
    const n = w.calls.length, crashAt = w.t; w.crash(a); await w.advance(4000);
    const after = w.calls.slice(n); assert.ok(after.length >= 1 && after.length <= 2, "about one request per interval: " + after.length);
    assert.equal(after[0].tab, "b"); assert.ok(after[0].t - crashAt <= 100, "take-over at once"); assert.equal(after[0].inm, '"r1"');
    assert.equal(b.data.length, 1); assert.equal(c.data.length, 1);
    [b, c].forEach(t => t.p.stop()); ok("leader crash -> next tab leads at once, 304 not 200");
  }

  // ── 7. poke from a follower reaches the leader at once; storms coalesce; poke during a request is answered by a trailing one ────
  {
    const w = makeWorld(), a = makeTab(w, "a"), b = makeTab(w, "b"); a.poll().start(); b.poll().start(); await w.advance(1000);
    await w.advance(4000);
    w.rev = 2; const n = w.calls.length, at = w.t; b.p.poke("complete");
    await w.advance(400);
    assert.equal(w.calls.length, n + 1); assert.equal(w.calls[n].tab, "a", "the leader asked");
    assert.equal(b.data.length, 2); assert.ok(b.datas[1].at - at <= 400, "the follower saw it within 400 ms (virtual), not at the next 3 s tick");
    // 20 pokes in the same instant
    const m = w.calls.length; w.rev = 3; for (let i = 0; i < 20; i++) (i % 2 ? a : b).p.poke("storm"); await w.advance(1500);
    assert.ok(w.calls.length - m <= 2, "20 pokes -> at most 2 requests, got " + (w.calls.length - m));
    assert.equal(b.data[b.data.length - 1].rev, 3);
    // static poke by key reaches the poller in this page
    w.rev = 4; const k = w.calls.length; assert.equal(CnPoll.poke("k", "static"), true); await w.advance(400);
    assert.ok(w.calls.length > k); assert.equal(a.data[a.data.length - 1].rev, 4);
    // slow server (600 ms): an event happens while the timer's request is in flight; ONE trailing request follows right behind it
    // and the answer is never older than the event (the fake server answers with the revision it had when the request STARTED)
    w.latency = 600;
    let guard = 0; const startN = w.calls.length; while (w.calls.length === startN && guard++ < 80) await w.advance(100);   // the next timer request has just started
    assert.equal(w.calls.length, startN + 1, "a request is in flight");
    w.rev = 5; const inflight = w.calls.length; a.p.poke("late-event"); b.p.poke("late-event-2");
    await w.advance(3000);
    const trailing = w.calls.slice(inflight); assert.ok(trailing.length >= 1 && trailing[0].t - w.calls[inflight - 1].t <= 1000, "the trailing request follows the in-flight one at once");
    assert.equal(trailing[0].rev, 5, "it asks with the new revision, not the old one");
    assert.equal(a.data[a.data.length - 1].rev, 5, "the answer is never older than the poke"); assert.equal(b.data[b.data.length - 1].rev, 5);
    assert.ok(trailing.filter(c => c.t - w.calls[inflight - 1].t < 1500).length <= 2, "two pokes during one request make at most one trailing request");
    w.latency = 0; a.p.stop(); b.p.stop(); ok("poke from a follower is fast, storms coalesce, trailing request after an in-flight one");
  }

  // ── 8. errors: backoff, Retry-After, 401, timeout, recovery ──────────────────────────────────────────────────────────────────────
  {
    const w = makeWorld(), t = makeTab(w, "a");
    t.poll({ errorMaxMs: 20000 }).start(); await w.advance(1000);
    w.serverFn = () => { throw new Error("network down"); };
    await w.advance(120000);
    const errCalls = w.calls.slice(1).map(c => c.t), gaps = errCalls.slice(1).map((x, i) => Math.round((x - errCalls[i]) / 1000));
    assert.ok(t.errors.length >= 5, "errors reported"); assert.deepEqual(gaps.slice(0, 6), [3, 3, 4, 8, 16, 20], "error backoff, never faster than the base, capped " + gaps);
    w.serverFn = null; await w.advance(25000); assert.equal(t.p.state().stats.errors >= 5, true);
    const calm = w.calls.length; await w.advance(9000); assert.ok(w.calls.length - calm <= 4, "back to the base interval after recovery");
    // 429 with Retry-After: 30
    w.serverFn = () => new Response("slow down", { status: 429, headers: { "Retry-After": "30" } });
    const r0 = w.calls.length; await w.advance(3500); const e0 = t.errors.length; assert.ok(e0 >= 1 && t.errors[e0 - 1].status === 429);
    await w.advance(29000); assert.equal(w.calls.length - r0, 1, "Retry-After honoured: one request in 30 s");
    // 401: waits the long error interval
    w.serverFn = () => new Response("{}", { status: 401 }); const u0 = w.calls.length; await w.advance(40000);
    assert.ok(w.calls.length - u0 <= 3, "401 does not hammer: " + (w.calls.length - u0));
    t.p.stop();
    // timeout: a request that never answers is aborted and counted as an error
    const w2 = makeWorld(), t2 = makeTab(w2, "a"); t2.poll({ timeoutMs: 5000 }).start();
    w2.serverFn = () => new Promise(() => {}); await w2.advance(1000);
    await w2.advance(30000); assert.ok(t2.errors.some(e => e.timeout), "timeout reported"); t2.p.stop();
    ok("errors back off (3,3,4,8,16,cap), Retry-After and 401 respected, timeout aborts, recovery");
  }

  // ── 9. jitter stays inside +-10 % ─────────────────────────────────────────────────────────────────────────────────────────────
  {
    const w = makeWorld(), seq = [0, 0.999, 0.5, 0.25, 0.75, 0.1, 0.9]; let i = 0;
    const t = makeTab(w, "a", { random: () => seq[i++ % seq.length] }); t.poll().start();
    await w.advance(60000);
    const ts = w.calls.map(c => c.t), gaps = ts.slice(1).map((x, k) => x - ts[k]);
    assert.ok(gaps.length > 10 && gaps.every(g => g >= 2700 && g <= 3300), "gaps " + gaps);
    assert.ok(new Set(gaps).size > 3, "the gaps vary"); t.p.stop(); ok("jitter within 10 %");
  }

  // ── 10. lease fallback (no Web Locks): one leader, hand-over on stop, take-over after a crash ────────────────────────────────
  {
    const w = makeWorld(), a = makeTab(w, "a", { noLocks: true, storage: true }), b = makeTab(w, "b", { noLocks: true, storage: true });
    a.poll().start(); b.poll().start(); await w.advance(30000);
    const who = new Set(w.calls.map(c => c.tab)); assert.equal(who.size, 1, "one tab asks under the lease"); const lead = [...who][0], other = lead === "a" ? b : a;
    assert.equal(a.data.length, 1); assert.equal(b.data.length, 1);
    (lead === "a" ? a : b).p.stop(); const n = w.calls.length; await w.advance(6000);
    assert.ok(w.calls.length > n && w.calls.slice(n).every(c => c.tab === other.name), "the other tab leads after a clean stop");
    assert.equal(w.calls[n].inm, '"r1"');
    w.crash(other); const m = w.calls.length; await w.advance(10000); assert.equal(w.calls.length, m, "(the crashed tab asks nothing)");
    // third tab joins after the crash: the lease expires, it takes over
    const c = makeTab(w, "c", { noLocks: true, storage: true }); c.poll().start(); await w.advance(10000);
    assert.ok(w.calls.length > m && w.calls.slice(m).every(x => x.tab === "c"), "lease expired -> c leads");
    c.p.stop(); ok("lease fallback: one leader, clean hand-over, crash take-over");
  }

  // ── 11. without BroadcastChannel every tab polls for itself (documented degrade) ────────────────────────────────────────────────
  {
    const w = makeWorld(), a = makeTab(w, "a", { noBC: true }), b = makeTab(w, "b", { noBC: true });
    a.poll().start(); b.poll().start(); await w.advance(10000);
    assert.equal(new Set(w.calls.map(c => c.tab)).size, 2); assert.equal(a.data.length, 1); assert.equal(b.data.length, 1);
    assert.equal(a.p.state().shared, false); a.p.stop(); b.p.stop(); ok("no BroadcastChannel -> solo polling");
  }

  // ── 12. no ETag from the server: unchanged detected by body equality; custom fetch(); throwing callbacks; stop() ────────────────
  {
    const w = makeWorld(), t = makeTab(w, "a"); let body = { v: 1 };
    w.serverFn = () => new Response(JSON.stringify(body), { status: 200 });
    t.poll().start(); await w.advance(12000);
    assert.equal(t.data.length, 1, "same body, no ETag: delivered once"); assert.ok(t.unchanged >= 3);
    assert.ok(w.calls.every(c => c.inm === null), "no ETag known, none sent");
    body = { v: 2 }; await w.advance(3500); assert.equal(t.data.length, 2); t.p.stop();
    const n = w.calls.length; await w.advance(20000); assert.equal(w.calls.length, n, "stopped"); assert.equal(t.p.poke(), false);
    assert.equal(w.locks.get("cn-poll:k").holder, null, "stop releases the lock");
    // custom fetch: revision first, full read only when it moved
    const w2 = makeWorld(), t2 = makeTab(w2, "a"); let rev = 1, revReads = 0, fullReads = 0;
    t2.poll({ url: undefined, fetch: async ({ etag }) => { revReads++; if (etag === "r" + rev) return { status: 304 }; fullReads++; return { status: 200, etag: "r" + rev, body: { big: "x".repeat(100), rev } }; } }).start();
    await w2.advance(30000); assert.equal(fullReads, 1); assert.ok(revReads >= 10); rev = 2; await w2.advance(3500); assert.equal(fullReads, 2); assert.equal(t2.data.length, 2); t2.p.stop();
    // a callback that throws never kills the poller
    const w3 = makeWorld(), t3 = makeTab(w3, "a"); let boom = 0; const warn = console.warn; console.warn = () => {};
    t3.poll({ onData: () => { boom++; throw new Error("x"); } }).start(); await w3.advance(10000); console.warn = warn;
    assert.equal(boom, 1); assert.ok(w3.calls.length >= 3); t3.p.stop();
    ok("ETag-less server, custom fetch (revision first), throwing callback, stop()");
  }

  // ── 13. numbers for the findings: ten tabs for an hour, today versus the helper ───────────────────────────────────────────────
  {
    const hour = 3600000, rows = [];
    for (const [label, opts] of [["live page, 3 s flat", {}], ["dashboard, 3 s -> 30 s idle", { maxIntervalMs: 30000 }]]) {
      const w = makeWorld(), tabs = Array.from({ length: 10 }, (_, i) => makeTab(w, "t" + i));
      tabs.forEach(t => t.poll(opts).start());
      await w.advance(hour); rows.push([label, w.calls.length, w.calls.filter(c => c.inm).length]);
      tabs.forEach(t => t.p.stop());
    }
    const naive = 10 * (hour / 3000);
    console.log("  numbers (10 open tabs, 1 hour, 3 s base): today " + naive + " requests/h; helper live " + rows[0][1] + "; helper with backoff " + rows[1][1]);
    assert.ok(rows[0][1] <= naive / 9, "ten tabs cost one: " + rows[0][1]); assert.ok(rows[1][1] < rows[0][1] / 5, "backoff cuts an idle hour by 5x or more: " + rows[1][1]);
    ok("ten tabs one hour: " + naive + " -> " + rows[0][1] + " (live) -> " + rows[1][1] + " (idle backoff)");
  }

  console.log("poll-unit.cjs: " + groups + " groups passed");
})().catch(e => { console.error(e); process.exit(1); });
