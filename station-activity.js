/*  station-activity.js — what each person does at a station, recorded as it happens (Paul, 2 Oct: every scan, every
 *  action, every error that matters, per employee and per station, so time, efficiency and throughput can be compared).
 *  Contract: plans/employee-efficiency/contract.md. Loaded after station-session.js, which says who is signed in.
 *
 *    window.StationActivity && StationActivity.log("scan" | "reject" | "complete" | "print" | "undo" | "error" | "note",
 *        { orderId, line, sku, parts, orders, detail })          → true when queued, false when not
 *    StationActivity.flush()    → Promise (sends what is queued now)
 *    StationActivity.pending()  → how many events are not acknowledged yet
 *    StationActivity.who()      → the identity the next event would carry, or null
 *
 *  The person, station, device, computer, session, time and the gap since the person's previous action are added here.
 *  Nothing happens (and nothing throws) when nobody is signed in or there is no session. Events wait in memory and in
 *  localStorage (station_activity_q.<device>), go out every 10 s, on pagehide / when the page is hidden (sendBeacon or a
 *  keepalive request) and when the browser is back online, and are sent again until the server says 200: their ids are fixed
 *  at the moment they happen, so a repeat is stored once and counted once. At most 500 wait (the oldest go first).
 *  Never a PIN: a digits-only name refuses the event, a digits-only id or detail is blanked, a 6-digit number inside a detail
 *  becomes "[#]" (the server does the same again). The call never touches the network and never blocks the page. */
(function () {
  "use strict";
  if (window.StationActivity) return;
  const ACTIONS = new Set(["scan", "reject", "complete", "print", "undo", "error", "note"]);
  const FLUSH_MS = 10000, BACKOFF_MAX = 120000, MAX_QUEUE = 500, BATCH = 50, EVENT_BYTES = 600, GAP_CAP = 3600000;
  const K = { q: "station_activity_q.", seq: "station_activity_seq.", last: "station_activity_last." };
  let queue = [], loadedFor = "", sending = null, failures = 0, nextTry = 0, batchMax = BATCH, started = false;
  const beaconed = new Set(), memSeq = {}, memLast = {};      // (kept in memory too: localStorage can be blocked or full)

  const warn = (...a) => { try { console.warn("[StationActivity]", ...a); } catch (_) {} };
  const lsGet = k => { try { return localStorage.getItem(k) || ""; } catch (_) { return ""; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, v); } catch (_) {} };
  const lsDel = k => { try { localStorage.removeItem(k); } catch (_) {} };
  const lsJson = (k, d) => { try { const v = JSON.parse(localStorage.getItem(k) || "null"); return v == null ? d : v; } catch (_) { return d; } };
  const clean = (v, n) => String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, n);
  const pinLike = s => /^\d{4,8}$/.test(s);
  const int = (v, lo, hi) => { const x = Math.round(Number(v)); return Number.isFinite(x) ? Math.max(lo, Math.min(hi, x)) : lo; };
  const bytes = s => { try { return new TextEncoder().encode(s).length; } catch (_) { try { return unescape(encodeURIComponent(s)).length; } catch (_2) { return s.length * 3; } } };

  /** who is signed in on this page: StationSession.who(), or null */
  function whoNow() {
    try {
      const S = window.StationSession;
      if (!S || typeof S.who !== "function") return null;
      const w = S.who();
      if (!w || !w.person || !w.station || !w.device) return null;
      if (!/\p{L}/u.test(String(w.person))) return null;        // a name has a letter: "123456" or "123 456" would be a PIN
      return w;
    } catch (_) { return null; }
  }
  /** this page (known with nobody signed in): { station, device, computer, sandbox } */
  function pageNow() {
    try { const S = window.StationSession; const p = S && typeof S.page === "function" ? S.page() : null; if (p && p.device) return p; } catch (_) {}
    const w = whoNow(); return w ? { station: w.station, device: w.device, computer: w.computer, sandbox: w.sandbox } : null;
  }

  /* ── the queue ── */
  function load(device) {
    if (loadedFor === device) return;
    loadedFor = device;
    const stored = lsJson(K.q + device, []), have = new Set(queue.map(e => e.id));
    const old = (Array.isArray(stored) ? stored : []).filter(e => e && typeof e === "object" && typeof e.id === "string" && !have.has(e.id));
    queue = old.concat(queue).slice(-MAX_QUEUE);
  }
  function save(device) {
    try { if (queue.length) lsSet(K.q + device, JSON.stringify(queue)); else lsDel(K.q + device); } catch (_) {}
  }
  function nextSeq(device) {
    const n = Math.max(parseInt(lsGet(K.seq + device), 10) || 0, memSeq[device] || 0) + 1;
    memSeq[device] = n; lsSet(K.seq + device, String(n));
    return n;
  }

  /* ── log ── */
  function log(action, o) {
    try {
      if (typeof action !== "string" || !ACTIONS.has(action)) return false;
      const w = whoNow();
      if (!w) return false;
      load(w.device);
      o = o && typeof o === "object" ? o : {};
      const now = Date.now(), seq = nextSeq(w.device);
      const last = lsJson(K.last + w.device, null) || memLast[w.device] || null;
      const base = last && last.person === w.person && last.session === w.session && Number(last.at) > 0 ? Number(last.at) : Number(w.startAt) || now;
      let detail = clean(o.detail, 120);
      if (/^\d+$/.test(detail)) detail = "";
      else detail = detail.replace(/(?<!\d)\d{6}(?!\d)/g, "[#]");
      const idish = (v, n) => { const s = clean(v, n); return pinLike(s) ? "" : s; };
      const ev = {
        id: `${w.device}_${String(w.computer || "").replace(/^pc-/, "").slice(0, 4)}_${seq}_${now}`.replace(/[^\w.:-]/g, "_").slice(0, 100),
        station: w.station, device: clean(w.device, 40), computer: String(w.computer || ""), session: String(w.session || ""),
        person: clean(w.person, 80), action,
        orderId: idish(String(o.orderId == null ? "" : o.orderId).replace(/\D/g, ""), 30), line: idish(o.line, 40), sku: idish(o.sku, 60),
        parts: int(o.parts, 0, 100000), orders: Number(o.orders) >= 1 ? 1 : 0, detail,
        at: now, seq, sincePrevMs: int(now - base, 0, GAP_CAP)
      };
      if (w.sandbox) ev.sandbox = true;
      // the server takes at most 600 bytes an event: the optional text goes first
      for (const k of ["detail", "sku", "line"]) {
        while (bytes(JSON.stringify(ev)) > EVENT_BYTES && ev[k]) ev[k] = ev[k].length > 20 ? ev[k].slice(0, ev[k].length - 20) : "";
      }
      if (bytes(JSON.stringify(ev)) > EVENT_BYTES) return false;
      memLast[w.device] = { person: w.person, session: w.session, at: now }; lsSet(K.last + w.device, JSON.stringify(memLast[w.device]));
      queue.push(ev);
      if (queue.length > MAX_QUEUE) { warn("the queue is full: the oldest " + (queue.length - MAX_QUEUE) + " event(s) were dropped"); queue = queue.slice(-MAX_QUEUE); }
      save(w.device);
      start();
      return true;
    } catch (e) { warn("log:", e); return false; }
  }

  /* ── sending ── */
  const url = sb => "/.netlify/functions/firebaseOrders" + (sb ? "?sandbox=1" : "");
  /** the first events that share one store (a page is one or the other), at most batchMax */
  function nextBatch() {
    const first = queue[0], sb = !!(first && first.sandbox);
    const out = [];
    for (const e of queue) { if (!!e.sandbox !== sb || out.length >= batchMax) break; out.push(e); }
    return { sb, events: out };
  }
  function settle(events) {
    const gone = new Set(events.map(e => e.id));
    queue = queue.filter(e => !gone.has(e.id));
    for (const id of gone) beaconed.delete(id);
    const p = pageNow(); if (p) save(p.device);
  }
  function post(sb, events, keepalive) {
    const text = JSON.stringify({ activity: events });
    if (typeof fetch !== "function") return Promise.resolve(null);
    let ctl = null, timer = 0;
    try { if (!keepalive && typeof AbortController === "function") { ctl = new AbortController(); timer = setTimeout(() => { try { ctl.abort(); } catch (_) {} }, 20000); } } catch (_) {}
    return fetch(url(sb), { method: "POST", headers: { "Content-Type": "application/json" }, body: text, keepalive: !!keepalive, signal: ctl ? ctl.signal : undefined })
      .then(r => { clearTimeout(timer); return { status: r.status }; }, () => { clearTimeout(timer); return null; });
  }
  /** sends the next batch; resolves when it is settled (or kept for a later try) */
  function flush(force) {
    try {
      const p = pageNow();
      if (p) load(p.device);
      if (sending) return sending;
      if (!queue.length) return Promise.resolve();
      if (!force && (navigator.onLine === false || Date.now() < nextTry)) return Promise.resolve();
      const { sb, events } = nextBatch();
      if (!events.length) return Promise.resolve();
      sending = post(sb, events, false).then(r => {
        const st = r ? r.status : 0;
        if (st >= 200 && st < 300) { failures = 0; nextTry = 0; batchMax = BATCH; settle(events); }
        else if (st === 413 && events.length > 1) { batchMax = Math.max(1, Math.floor(events.length / 2)); nextTry = 0; }
        else if (st === 413 || st === 400 || st === 422) { failures = 0; settle(events); warn("a batch of " + events.length + " event(s) was refused for good:", st); }
        else { failures = Math.min(failures + 1, 6); nextTry = Date.now() + Math.min(BACKOFF_MAX, FLUSH_MS * Math.pow(2, failures)); }
      }).catch(() => { failures = Math.min(failures + 1, 6); nextTry = Date.now() + Math.min(BACKOFF_MAX, FLUSH_MS * Math.pow(2, failures)); })
        .then(() => { sending = null; if (queue.length && failures === 0) return flush(); });
      return sending;
    } catch (e) { warn("flush:", e); sending = null; return Promise.resolve(); }
  }
  /** the page is going away or hidden: whatever is queued goes out now without waiting for an answer (kept until a 200
      on a later load, so a lost beacon costs nothing; a repeat is stored once) */
  function leave() {
    try {
      const p = pageNow(); if (p) load(p.device);
      for (let n = 0; queue.length && n < 4; n++) {
        const batch = queue.filter(e => !beaconed.has(e.id)).slice(0, BATCH);
        if (!batch.length) return;
        const sb = !!batch[0].sandbox, same = batch.filter(e => !!e.sandbox === sb);
        const text = JSON.stringify({ activity: same });
        let ok = false;
        try { if (navigator.sendBeacon) ok = navigator.sendBeacon(url(sb), new Blob([text], { type: "application/json" })); } catch (_) {}
        if (!ok) { try { if (typeof fetch === "function") { fetch(url(sb), { method: "POST", headers: { "Content-Type": "application/json" }, body: text, keepalive: true }).catch(() => {}); ok = true; } } catch (_) {} }
        if (!ok) return;
        for (const e of same) beaconed.add(e.id);
      }
    } catch (_) {}
  }
  function start() {
    if (started) return;
    started = true;
    try {
      setInterval(() => { try { flush(); } catch (_) {} }, FLUSH_MS);
      window.addEventListener("pagehide", leave);
      document.addEventListener("visibilitychange", () => { if (document.visibilityState === "hidden") leave(); });
      window.addEventListener("online", () => { failures = 0; nextTry = 0; flush(true); });
    } catch (e) { warn("start:", e); }
  }

  /** A sandbox reset (the sorter): the sandbox's events still waiting, in memory and on the disk, go with its records. */
  function discard(sandbox) {
    let n = 0;
    try {
      const keep = e => !!(e && e.sandbox) !== !!sandbox;
      const before = queue.length; queue = queue.filter(keep); n += before - queue.length;
      for (let i = 0; i < localStorage.length; i++) {
        const k = localStorage.key(i); if (!k || k.indexOf(K.q) !== 0) continue;
        const list = lsJson(k, []); if (!Array.isArray(list)) continue;
        const rest = list.filter(keep); if (rest.length === list.length) continue;
        n += list.length - rest.length; if (rest.length) lsSet(k, JSON.stringify(rest)); else lsDel(k);
      }
    } catch (e) { warn("discard:", e); }
    return n;
  }
  window.StationActivity = {
    log, flush: () => flush(true), pending: () => queue.length, discard,
    who: () => { const w = whoNow(); return w ? { person: w.person, station: w.station, device: w.device, computer: w.computer, session: w.session } : null; }
  };
  // events left from an earlier page load (offline, closed too fast) go out soon after the station has set itself up
  try { start(); setTimeout(() => { try { flush(); } catch (_) {} }, 4000); } catch (_) {}
})();
