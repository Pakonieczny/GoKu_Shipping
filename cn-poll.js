/*  cn-poll.js — ONE shared polling helper for every page that asks the server "did anything change?" on a timer.
 *  (FC15, Firebase cost emergency, 7 Oct 2026; plans/firebase-cost/fc15-findings.md has the same text with examples.)
 *
 *  WHY. Nearly all Firestore reads come from Netlify functions that a page calls on a timer. Cost = calls x documents x bytes, and
 *  calls scale with TIME and with the number of OPEN TABS, not with changes. This helper makes a poll cost what it should:
 *    (a) ONE poller per computer for a key: ten open tabs cost one tab. The leader tab (Web Locks API, localStorage lease where
 *        there are no Web Locks) asks the server; the others get the answer by BroadcastChannel. A closed leader is replaced at once.
 *    (b) it PAUSES while the tab is hidden (a hidden tab never polls and never leads) or while the person is signed out
 *        (options.enabled() false) or the browser is offline, and RESUMES with an immediate refresh on visibility or focus.
 *    (c) it BACKS OFF while the answer is unchanged (3 s, then 6, 12 ... up to options.maxIntervalMs), with jitter, and goes back
 *        to the base interval at once on a change, on a user action (click, key, scroll, touch) or on poke().
 *    (d) CONDITIONAL requests: the last ETag goes out as If-None-Match; a 304 (empty body) means unchanged. Server half:
 *        netlify/functions/_etag.js (revision ETag, body-hash ETag, 304, conditional()).
 *  Speed is kept: poke() (an event on THIS computer: a Complete, a QR Print, a save) refreshes at once from any tab (about 300 ms
 *  plus the round trip, so well inside 1 s); another computer's change is seen within one base interval (3 s) while a tab is
 *  visible, because backoff is OFF unless a page opts in with maxIntervalMs. A page that must feel live keeps maxIntervalMs unset.
 *
 *  API (browser global `CnPoll`; also module.exports for Node tests)
 *    const p = CnPoll.create({
 *      key: "library:live",            REQUIRED. Same key = same poller on this computer. The key must say everything the request
 *                                      depends on (page area, signed-in person, filters): only the leader's request is made.
 *      url: "/.netlify/functions/x" | () => url,      and
 *      init: { method:"POST", headers:{...}, body: JSON.stringify({...}) } | () => init,   (a function is read at every request)
 *        — or — fetch: async ({etag, signal, reason, attempt}) => ({ status: 200|304, etag, body })   (your own request code, e.g. a
 *        revision check first and the full read only when the revision moved; return {status:304} or {notModified:true} for unchanged)
 *      parse: (text, response) => value     default JSON.parse(text) (null for an empty body)
 *      onData: (data, info) => {}      the answer when it is NEW (first answer, or changed). Runs in every tab (leader and followers).
 *      onUnchanged: (info) => {}       leader only: the server said unchanged (304, same ETag, same body).
 *      onError: (error, info) => {}    network error, timeout, HTTP error (error.status). The next try is backed off. Runs in the leader and,
 *                                      relayed (error.relayed === true, message + status + plain properties), in every follower.
 *      enabled: () => bool             false = signed out / not wanted: no polling, no leading. Re-checked every enabledCheckMs.
 *      intervalMs: 3000                base interval while things change.
 *      maxIntervalMs: 0                0 = no backoff (a "live" page). N = unchanged answers stretch the interval up to N ms.
 *      factor: 2, idleAfter: 2         growth factor and how many unchanged answers keep the base interval before growing.
 *      steps: [100, 200, 400, 800]     (instead of the three above) an explicit wait after 0, 1, 2 ... unchanged answers, the last one
 *                                      stays; for a page that already has such a schedule and must keep its cadence exactly.
 *      jitter: 0.1                     +-10 % on every wait, so a shop of computers does not hit the server in the same instant.
 *      minGapMs: 250                   two requests of one poller are never closer than this (poke storms are coalesced; a poke
 *                                      during a request runs ONE more request right after it, so the answer is never older than the poke).
 *      softGapMs: 1000                 a focus / visibility return skips the refresh if the last request started less than this ago.
 *      timeoutMs: 20000, errorMaxMs: 60000, enabledCheckMs: 2000
 *      pauseHidden: true               false = keep polling while hidden (only for a page that must; it costs reads).
 *      activity: true                  user actions reset the backoff (and tell the leader, from a follower tab).
 *      share: true                     false = every tab polls for itself (debug only).
 *    }).start();
 *    p.poke(reason?)    something happened on this computer: refresh now (any tab; a follower asks the leader), backoff reset.
 *    p.refresh(reason?) same as poke without the backoff reset (a plain "look again").
 *    p.wake()           the page knows someone is working here (its own idea of activity, with options.activity false): back to the
 *                       base interval and the next look pulled forward, no request now.
 *    p.stop()           release leadership, stop timers, close the channel (the answer last received stays in p.data()).
 *    p.data()           the last answer received (leader or follower), null before the first.
 *    p.state()          { started, active, leader, shared, intervalMs, nextInMs, etag, seq, unchanged, hasData, stats:{requests,
 *                         notModified, changed, errors, received, pokes} }
 *    CnPoll.poke(key, reason?)   poke by key from code that holds no poller (it reaches the poller in this page or in other tabs).
 *    CnPoll.report()             { key: state } of every poller in this page (for the idle-cost table and tests).
 *
 *  info passed to the callbacks: { key, seq, etag, at, reason, leader, changed, hidden, intervalMs }.
 *
 *  RULES FOR ADOPTERS
 *   - Polling a cheap REVISION (one tiny document, server answers 304 while it is unchanged, see _etag.js conditional()) is what
 *     saves Firestore reads. A body-hash ETag only saves bytes to the browser: the function still read its documents.
 *   - Delta feeds (a cursor "since" kept per tab) do not share: poll a snapshot/revision, or make the shared answer the whole delta.
 *   - Put the person in the key when the answer depends on who is signed in ("efficiency:overview:" + name).
 *   - Followers never run url/init/fetch: build everything the request needs inside those functions, not from per-tab state.
 *   - A same-key poller created twice in one page is harmless (the second follows the first), but its url/fetch is never used.
 *   - Protocol (BroadcastChannel "cn-poll:<key>", lock "cn-poll:<key>", lease "cnpoll:lease:<key>"): version 1. Messages carry v:1;
 *     other versions are ignored, so a rolled-out change can never starve a tab silently: bump the lock name when changing it.
 *
 *  No dependencies, no network of its own, no storage other than the lease fallback (a key that holds a person's name or PIN is
 *  never written: the lease holds only a random instance id and an expiry).                                                      */
(function (root) {
  "use strict";
  if (typeof module === "undefined" && root.CnPoll) return;   // a page that loads the file twice keeps the first

  const PROTOCOL = 1, CH = "cn-poll:", LOCK = "cn-poll:", LEASE_PREFIX = "cnpoll:lease:";
  const LEASE_MS = 4000, VERIFY_MS = 40;
  const DEFAULTS = {
    intervalMs: 3000, maxIntervalMs: 0, factor: 2, idleAfter: 2, jitter: 0.1, minGapMs: 250, softGapMs: 1000,
    timeoutMs: 20000, errorMaxMs: 60000, enabledCheckMs: 2000, pauseHidden: true, activity: true, share: true
  };
  const ACTIVITY_EVENTS = ["pointerdown", "keydown", "wheel", "touchstart"];
  const registry = Object.create(null);   // key -> [poller]

  const num = (v, lo, hi, d) => { v = Number(v); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; };
  const rid = () => Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4);

  function makeEnv(ov) {
    const g = root;
    let ls = null;
    try { ls = g.localStorage || null; } catch (_) { ls = null; }
    const e = {
      now: () => Date.now(),
      setTimeout: (f, ms) => g.setTimeout(f, ms),
      clearTimeout: t => g.clearTimeout(t),
      random: () => Math.random(),
      fetch: (...a) => g.fetch(...a),
      document: g.document || null, window: g.addEventListener ? g : null, navigator: g.navigator || null,
      BroadcastChannel: g.BroadcastChannel || null, AbortController: g.AbortController || null, localStorage: ls
    };
    if (ov) for (const k of Object.keys(ov)) e[k] = ov[k];
    return e;
  }

  function create(options) {
    const o = Object.assign({}, DEFAULTS, options || {});
    if (!o.key || typeof o.key !== "string") throw new Error("CnPoll: options.key is required");
    if (!o.url && typeof o.fetch !== "function") throw new Error("CnPoll: options.url or options.fetch is required");
    const env = makeEnv(o.env);
    const id = rid();
    // steps: an explicit schedule [100, 200, 400 ...] of waits after 0, 1, 2 ... unchanged answers (for a page that already has one);
    // intervalMs / maxIntervalMs / factor / idleAfter are then ignored. Without steps: base * factor^n up to maxIntervalMs.
    const steps = Array.isArray(o.steps) && o.steps.length ? o.steps.map(v => num(v, 50, 3600000, 3000)) : null;
    const base = steps ? steps[0] : num(o.intervalMs, 250, 3600000, 3000);
    const cap = steps ? Math.max.apply(null, steps) : (o.maxIntervalMs ? Math.max(base, num(o.maxIntervalMs, base, 3600000, base)) : base);
    const factor = num(o.factor, 1, 10, 2), idleAfter = num(o.idleAfter, 0, 1000, 2), jit = num(o.jitter, 0, 0.5, 0.1);
    const minGap = num(o.minGapMs, 0, 60000, 250), softGap = num(o.softGapMs, 0, 600000, 1000);
    const timeoutMs = num(o.timeoutMs, 1000, 300000, 20000), errorMax = num(o.errorMaxMs, 1000, 3600000, 60000);
    const checkMs = num(o.enabledCheckMs, 250, 60000, 2000);

    let started = false, leader = false, cand = null, ch = null;
    let timer = 0, watchT = 0, inflight = null, again = false, nextAt = 0;
    let etag = null, body = null, text = null, hasData = false, seq = 0;
    let interval = base, unchanged = 0, errStreak = 0, lastStart = -1e12, lastAct = -1e12, lastActMsg = -1e12;
    const stats = { requests: 0, notModified: 0, changed: 0, errors: 0, received: 0, pokes: 0 };

    const jittered = ms => Math.max(50, Math.round(ms * (1 + (env.random() * 2 - 1) * jit)));
    const safe = (fn, a, b) => { try { return typeof fn === "function" ? fn(a, b) : undefined; } catch (e) { try { if (root.console) root.console.warn("CnPoll " + o.key, e); } catch (_) {} } };
    const visible = () => { if (!o.pauseHidden) return true; try { return !(env.document && env.document.hidden); } catch (_) { return true; } };
    const online = () => { try { return !(env.navigator && env.navigator.onLine === false); } catch (_) { return true; } };
    const allowed = () => { try { return o.enabled ? !!o.enabled() : true; } catch (_) { return false; } };
    const active = () => started && visible() && allowed() && online();
    const sharing = () => !!(o.share && env.BroadcastChannel && ((env.navigator && env.navigator.locks) || env.localStorage));
    const mkInfo = (reason, changed) => ({ key: o.key, seq, etag, at: env.now(), reason: reason || "", leader, changed: !!changed, hidden: !visible(), intervalMs: interval });

    // what a follower is told of a failure: the message and every plain own property (status, timeout, flags a page put on it)
    function errData(e) {
      const d = { message: String((e && e.message) || e || "error") };
      try { for (const k of Object.keys(e || {})) { const v = e[k]; if (v == null || typeof v === "string" || typeof v === "number" || typeof v === "boolean") d[k] = v; } } catch (_) {}
      return d;
    }
    function post(m) { if (!ch) return; try { m.v = PROTOCOL; m.from = id; ch.postMessage(m); } catch (_) {} }

    /* ── the answer: new data goes to this page and (from the leader) to the other tabs ── */
    function deliver(data, info) { safe(o.onData, data, info); }
    function adopt(m) {
      if (hasData && m.etag && m.etag === etag) return;
      let t = null; if (!m.etag) { try { t = JSON.stringify(m.body); } catch (_) {} if (hasData && t !== null && t === text) return; }
      stats.received++; hasData = true; etag = m.etag || null; body = m.body; text = t; seq++;
      deliver(body, Object.assign(mkInfo(m.reason, true), { leader: false, at: m.at || env.now() }));
    }
    function sendData(to, reason) { post({ t: "data", to: to || null, etag, body, at: env.now(), reason: reason || "" }); }

    /* ── leader: the request cycle ── */
    function resetBackoff() { unchanged = 0; interval = base; }
    function schedule(ms) {
      env.clearTimeout(timer); timer = 0; nextAt = 0;
      if (!leader || !started) return;
      const wait = ms != null ? ms : jittered(interval);
      nextAt = env.now() + wait;
      timer = env.setTimeout(() => { timer = 0; nextAt = 0; cycle("timer"); }, wait);
    }
    function headersOf(h) {
      const out = {};
      if (!h) return out;
      if (typeof h.forEach === "function" && !Array.isArray(h)) h.forEach((v, k) => { out[k] = v; });
      else Object.assign(out, h);
      return out;
    }
    function httpFetch(req) {
      const url = typeof o.url === "function" ? o.url() : o.url;
      const init = (typeof o.init === "function" ? o.init() : o.init) || {};
      const headers = headersOf(init.headers);
      if (req.etag) headers["If-None-Match"] = req.etag;
      return env.fetch(url, Object.assign({ cache: "no-store", credentials: "same-origin" }, init, { headers, signal: req.signal })).then(r => {
        const hd = r.headers && typeof r.headers.get === "function" ? r.headers : null;
        const out = { status: r.status, etag: hd ? hd.get("etag") : null };
        const ra = hd ? Number(hd.get("retry-after")) : NaN;
        if (Number.isFinite(ra) && ra > 0) out.retryAfterMs = ra * 1000;
        if (r.status === 304) return out;
        return r.text().then(t => {
          out.text = t;
          if (r.ok) out.body = o.parse ? o.parse(t, r) : (t ? JSON.parse(t) : null);
          return out;
        });
      });
    }
    function cycle(reason) {
      if (!leader || !started) return;
      if (inflight) { again = true; return; }
      if (!active()) { evaluate("cycle"); return; }
      const since = env.now() - lastStart;
      if (reason !== "timer" && reason !== "lead" && since < minGap) {   // coalesce a burst: one request at the earliest allowed moment
        env.clearTimeout(timer); nextAt = env.now() + (minGap - since);
        timer = env.setTimeout(() => { timer = 0; nextAt = 0; cycle(reason); }, minGap - since);
        return;
      }
      env.clearTimeout(timer); timer = 0; nextAt = 0;
      lastStart = env.now(); stats.requests++;
      const ctl = env.AbortController ? new env.AbortController() : { signal: undefined, abort() {} };
      const st = { ctl, timedOut: false, to: 0 };
      st.to = env.setTimeout(() => { st.timedOut = true; try { ctl.abort(); } catch (_) {} }, timeoutMs);
      inflight = st;
      const req = { etag, signal: ctl.signal, reason: reason || "", attempt: errStreak };
      let p;
      try { p = Promise.resolve(typeof o.fetch === "function" ? o.fetch(req) : httpFetch(req)); } catch (e) { p = Promise.reject(e); }
      p.then(res => finish(st, reason, res, null), err => finish(st, reason, null, err));
    }
    function finish(st, reason, res, err) {
      env.clearTimeout(st.to);
      if (inflight !== st) return;          // leadership was dropped meanwhile: the answer is not ours to use
      inflight = null;
      if (!leader || !started) return;
      let wait = null;
      if (!err && res && res.status >= 400) { err = new Error("HTTP " + res.status); err.status = res.status; err.retryAfterMs = res.retryAfterMs; }
      if (err) {
        errStreak++; stats.errors++;
        if (st.timedOut) err = Object.assign(new Error("timeout after " + timeoutMs + " ms"), { timeout: true });
        wait = Math.min(errorMax, Math.max(interval, 1000 * Math.pow(2, Math.min(errStreak - 1, 10))));
        if (err.status === 401 || err.status === 403) wait = Math.max(wait, errorMax);
        if (err.retryAfterMs) wait = Math.min(120000, Math.max(wait, err.retryAfterMs));
        safe(o.onError, err, mkInfo(reason, false));
        post({ t: "error", err: errData(err), reason: reason || "" });
        wait = jittered(wait);
      } else {
        errStreak = 0;
        const status = res ? res.status || 200 : 304;
        let same = false, nb = null, nt = null, ne = null;
        if (status === 304 || !res || res.notModified) same = true;
        else {
          nb = res.body === undefined ? null : res.body; ne = res.etag || null;
          nt = res.text != null ? res.text : null;
          if (nt === null) { try { nt = JSON.stringify(nb); } catch (_) { nt = null; } }
          same = hasData && (ne ? ne === etag : (nt !== null && nt === text));
        }
        if (same) {
          unchanged++; stats.notModified++;
          if (steps) interval = steps[Math.min(unchanged, steps.length - 1)];
          else if (unchanged > idleAfter) interval = Math.min(cap, Math.round(interval * factor));
          safe(o.onUnchanged, mkInfo(reason, false));
        } else {
          hasData = true; etag = ne; body = nb; text = nt; seq++; resetBackoff(); stats.changed++;
          deliver(body, mkInfo(reason, true));
          sendData(null, reason);
        }
      }
      if (again) { again = false; cycle("trailing"); return; }
      schedule(wait);
    }

    /* ── election: one leader per key per computer ── */
    function becomeLeader() {
      if (leader) return;
      leader = true; resetBackoff(); errStreak = 0;
      cycle("lead");
    }
    function loseLeadership() {
      if (!leader) return;
      leader = false; again = false;
      env.clearTimeout(timer); timer = 0; nextAt = 0;
      if (inflight) { const st = inflight; inflight = null; env.clearTimeout(st.to); try { st.ctl.abort(); } catch (_) {} }
    }
    function lockElection() {
      const c = { held: false, release: null, ac: env.AbortController ? new env.AbortController() : null };
      c.leave = () => { if (c.held) { if (c.release) c.release(); } else if (c.ac) { try { c.ac.abort(); } catch (_) {} } };
      let p;
      try {
        p = env.navigator.locks.request(LOCK + o.key, c.ac ? { signal: c.ac.signal } : {}, () => {
          if (cand !== c || !active()) return undefined;      // not wanted any more: the lock goes straight to the next tab
          c.held = true;
          return new Promise(res => { c.release = res; becomeLeader(); });
        });
      } catch (_) { p = null; }
      if (p && typeof p.then === "function") p.then(() => { if (cand === c) cand = null; }, () => { if (cand === c) cand = null; });
      else { c.dead = true; }
      return c;
    }
    function leaseElection() {
      const c = { dead: false, t: 0 }, K = LEASE_PREFIX + o.key;
      const read = () => { try { const v = JSON.parse(env.localStorage.getItem(K) || "null"); return v && typeof v === "object" ? v : null; } catch (_) { return null; } };
      const write = v => { try { env.localStorage.setItem(K, JSON.stringify(v)); } catch (_) {} };
      const renew = () => {
        c.t = env.setTimeout(() => {
          if (c.dead) return;
          const v = read();
          if (v && v.id !== id && v.until > env.now()) { loseLeadership(); attempt(); return; }
          write({ id, until: env.now() + LEASE_MS }); renew();
        }, Math.round(LEASE_MS / 3));
      };
      const attempt = () => {
        if (c.dead) return;
        const cur = read(), t = env.now();
        if (cur && cur.id !== id && cur.until > t) { c.t = env.setTimeout(attempt, jittered(LEASE_MS / 2)); return; }
        write({ id, until: t + LEASE_MS });
        c.t = env.setTimeout(() => {         // the last writer wins: look again after a beat
          if (c.dead) return;
          const v = read();
          if (v && v.id === id) { renew(); becomeLeader(); } else attempt();
        }, VERIFY_MS);
      };
      c.leave = () => {
        c.dead = true; env.clearTimeout(c.t);
        const v = read(); if (v && v.id === id) { try { env.localStorage.removeItem(K); } catch (_) {} }
      };
      c.kick = () => { if (!c.dead && !leader) { env.clearTimeout(c.t); c.t = env.setTimeout(attempt, Math.round(env.random() * 120)); } };
      attempt();
      return c;
    }
    function joinElection() {
      if (cand) return;
      if (!sharing()) { cand = { solo: true, leave() {} }; becomeLeader(); return; }
      cand = (env.navigator && env.navigator.locks) ? lockElection() : leaseElection();
      if (cand.dead) cand = null;
    }
    function leaveElection() {
      if (!cand) return;
      const c = cand; cand = null;
      try { c.leave(); } catch (_) {}
      loseLeadership();
    }
    function evaluate() {
      if (!started) return;
      if (active()) { if (!cand) joinElection(); }
      else if (cand) leaveElection();
    }

    /* ── events ── */
    function onMessage(ev) {
      const m = ev && ev.data;
      if (!m || m.v !== PROTOCOL || m.from === id || !started) return;
      if (m.t === "data") { if (m.to && m.to !== id) return; adopt(m); }
      else if (m.t === "error") {
        if (!leader && m.err) { const e = Object.assign(new Error(m.err.message), m.err, { relayed: true }); safe(o.onError, e, Object.assign(mkInfo(m.reason, false), { leader: false })); }
      }
      else if (m.t === "hello") { if (leader && hasData && (m.etag || null) !== etag) sendData(m.from, "hello"); }
      else if (m.t === "poke") { if (leader) poke(m.reason || "remote-poke", true); }
      else if (m.t === "active") { if (leader) bump(); }
    }
    function bump() {
      if (unchanged === 0 && interval === base) return;
      resetBackoff();
      if (leader && !inflight && nextAt && nextAt - env.now() > base * (1 + jit)) schedule();
    }
    function onActivity() {
      const t = env.now();
      if (t - lastAct < 500) return;
      lastAct = t;
      if (leader) bump();
      else if (t - lastActMsg >= 1000) { lastActMsg = t; post({ t: "active" }); }
    }
    function onReturn() {                       // visible again, focus, back online
      if (!started) return;
      evaluate();
      if (!active()) return;
      if (leader) { if (env.now() - lastStart >= softGap) cycle("return"); }
      else post({ t: "hello", etag });
    }
    function onHide() { evaluate(); }
    function onPageHide() { if (cand) leaveElection(); }
    function onPageShow() { onReturn(); }
    function onStorage(e) { if (cand && cand.kick && e && e.key === LEASE_PREFIX + o.key && !e.newValue) cand.kick(); }
    function watch() {
      env.clearTimeout(watchT); watchT = 0;
      if (!started) return;
      evaluate();
      if (o.enabled) watchT = env.setTimeout(watch, checkMs);
    }
    // the visibility listener must be removable, so it is one named function
    const onVis = () => (visible() ? onReturn() : onHide());
    function wire(add) {
      const w = env.window, d = env.document, m = add ? "addEventListener" : "removeEventListener";
      try {
        if (d && d[m]) {
          d[m]("visibilitychange", onVis);
          if (o.activity) for (const n of ACTIVITY_EVENTS) d[m](n, onActivity, { capture: true, passive: true });
        }
        if (w && w[m]) {
          w[m]("focus", onReturn); w[m]("online", onReturn); w[m]("offline", onHide);
          w[m]("pagehide", onPageHide); w[m]("pageshow", onPageShow); w[m]("storage", onStorage);
        }
      } catch (_) {}
    }

    /* ── public ── */
    function poke(reason, remote) {
      stats.pokes++;
      resetBackoff();
      if (!started) return false;
      if (leader) cycle(reason || "poke");
      else if (!remote) { post({ t: "poke", reason: reason || "poke" }); evaluate(); }
      return true;
    }
    /** The page decided "someone is working here": back to the base interval and the next look pulled forward, no request now. */
    function wake() {
      if (!started) return false;
      if (leader) bump(); else post({ t: "active" });
      return true;
    }
    function refresh(reason) {
      if (!started) return false;
      if (leader) cycle(reason || "refresh");
      else post({ t: "poke", reason: reason || "refresh" });
      return true;
    }
    function start() {
      if (started) return api;
      started = true;
      (registry[o.key] = registry[o.key] || []).push(api);
      if (sharing()) {
        try { ch = new env.BroadcastChannel(CH + o.key); ch.onmessage = onMessage; } catch (_) { ch = null; }
      }
      wire(true);
      post({ t: "hello", etag });
      evaluate();
      if (o.enabled) watchT = env.setTimeout(watch, checkMs);
      return api;
    }
    function stop() {
      if (!started) return api;
      leaveElection();
      started = false;
      env.clearTimeout(timer); env.clearTimeout(watchT); timer = watchT = 0; nextAt = 0;
      wire(false);
      if (ch) { try { ch.close(); } catch (_) {} ch = null; }
      const list = registry[o.key] || [], i = list.indexOf(api);
      if (i >= 0) list.splice(i, 1);
      return api;
    }
    function state() {
      return {
        key: o.key, started, active: active(), leader, shared: sharing(), intervalMs: interval, baseMs: base, capMs: cap,
        nextInMs: nextAt ? Math.max(0, nextAt - env.now()) : null, etag, seq, unchanged, hasData, stats: Object.assign({}, stats)
      };
    }
    const api = { start, stop, poke, refresh, wake, state, data: () => body, key: o.key, id };
    return api;
  }

  /** Poke by key from code that holds no poller: this page's pollers directly, other tabs through the channel. */
  function pokeKey(key, reason) {
    const list = registry[key] || [];
    if (list.length) { list.forEach(p => p.poke(reason || "poke")); return true; }
    try {
      if (!root.BroadcastChannel) return false;
      const c = new root.BroadcastChannel(CH + key);
      c.postMessage({ v: PROTOCOL, t: "poke", from: "ext:" + rid(), reason: reason || "poke" });
      root.setTimeout(() => { try { c.close(); } catch (_) {} }, 100);
      return true;
    } catch (_) { return false; }
  }
  function report() {
    const out = {};
    for (const k of Object.keys(registry)) registry[k].forEach((p, i) => { out[i ? k + "#" + (i + 1) : k] = p.state(); });
    return out;
  }

  const CnPoll = { version: PROTOCOL, create, poke: pokeKey, report, defaults: Object.assign({}, DEFAULTS) };
  if (typeof module !== "undefined" && module.exports) module.exports = CnPoll;
  if (!root.CnPoll) root.CnPoll = CnPoll;
})(typeof window !== "undefined" ? window : globalThis);
