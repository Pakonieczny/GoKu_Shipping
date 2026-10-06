/*  station-activity.js — what each person does at a station, recorded as it happens (Paul, 2 Oct: every scan, every
 *  action, every error that matters, per employee and per station, so time, efficiency and throughput can be compared).
 *  Contract: plans/employee-efficiency/contract.md. Loaded after station-session.js, which says who is signed in.
 *
 *    window.StationActivity && StationActivity.log("scan" | "reject" | "complete" | "print" | "undo" | "error" | "note",
 *        { orderId, line, sku, parts, orders, detail })          → true when queued, false when not
 *    StationActivity.matched({ orderId, at?, scanId?, detail? }) → the Welding station's scanner app: one order QR matched (see "matched" below;
 *                               log("matched", …) is the same call). StationActivity.matcher() → who a scan would be credited to now, or null.
 *    StationActivity.flush()    → Promise (sends what is queued now)
 *    StationActivity.pending()  → how many events are not acknowledged yet
 *    StationActivity.who()      → the identity the next event would carry, or null
 *    StationActivity.working({ rid, orderNumber, customer, pieces:[{ id, label, sku, listingId, size }], note, station, holdMs })
 *                               → the order this person has in hand RIGHT NOW (the console's live stations board); idle() ends it
 *    StationActivity.idle(rid?) / touch() / current()      (the live layer, below; plans/employee-hr/api.md)
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
  const ACTIONS = new Set(["scan", "reject", "complete", "print", "undo", "error", "note", "matched"]);
  const FLUSH_MS = 10000, BACKOFF_MAX = 120000, MAX_QUEUE = 500, BATCH = 50, EVENT_BYTES = 600, GAP_CAP = 3600000;
  const K = { q: "station_activity_q.", seq: "station_activity_seq.", last: "station_activity_last.", seen: "station_matched_seen." };
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
      if (action === "matched") return matched(o);              // one rule for every caller (the credit, the real scan time, the duplicate window)
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

  /* ── matched: one order QR scanned with the Welding station's scanner app (stations round 2; plans/stations-round2/api.md) ──
     matched({ orderId, at?, scanId?, detail? })   (log("matched", …) is the same call)   → true when queued; false when not: no page, not the
     Welding station, no usable order number, the same scan told twice, or the same order again within seconds.
     The scanner app has no login: the desktop page that receives its relayed scan says who is credited, by rule R3, which StationSession.who()
     keeps: the Matching person; two of them: the one with the latest input; nobody in Matching (a welder is never credited): the event is stored
     `unattributed: true` with no person (the server files it under "Unattributed": "scanned with nobody in Matching"). The credit is decided
     when this is called (station-scan-queue.js calls it when a scan arrives while somebody is signed in, otherwise at the replay right after
     the next sign-in); `at` is the real scan time. A scan from an earlier shop day is never credited to whoever signs in later (unattributed).
     The same order again within 10 s, or the same scanId, is ONE event. The event id is made from the scan, so a retry, a reload or a second
     tab writes it once and the rollup counts it once. A repeat after those 10 s is its own event whose detail ends " · again" (for 12 h).
     Matched work is scanned work, never an order completion (parts 0, orders 0). Nothing here touches the network (the queue sends it). */
  const MATCH = { dupMs: 10000, againMs: 12 * 3600e3, idMs: 6 * 3600e3, lastMs: 3600e3, max: 200 };
  const memSeen = {};
  function seenOf(device, now) {
    const s = memSeen[device] || (memSeen[device] = lsJson(K.seen + device, null) || {});
    const age = { ids: MATCH.idMs, orders: MATCH.againMs, last: MATCH.lastMs };
    for (const k of Object.keys(age)) {
      if (!s[k] || typeof s[k] !== "object" || Array.isArray(s[k])) s[k] = {};
      for (const [key, t] of Object.entries(s[k])) if (!(Number(t) > 0) || now - Number(t) >= age[k]) delete s[k][key];
      const keys = Object.keys(s[k]);
      if (keys.length > MATCH.max) for (const key of keys.sort((a, b) => s[k][a] - s[k][b]).slice(0, keys.length - MATCH.max)) delete s[k][key];
    }
    return s;
  }
  function matched(o) {
    try {
      o = o && typeof o === "object" ? o : {};
      const page = pageNow();
      if (!page || page.station !== "welding" || !page.device) return false;
      const orderId = String(o.orderId == null ? "" : o.orderId).replace(/\D/g, "").slice(0, 30);
      if (!orderId || pinLike(orderId)) return false;
      const now = Date.now(), at0 = Number(o.at), at = at0 > 1e12 ? Math.min(Math.round(at0), now) : now;
      const device = clean(page.device, 40);
      load(device);
      const seen = seenOf(device, now);
      const scanKey = String(o.scanId == null ? "" : o.scanId).replace(/[^\w.:-]/g, "").slice(0, 24) || at.toString(36);
      const id = `mt_${device}_${orderId}_${scanKey}`.replace(/[^\w.:-]/g, "_").slice(0, 100);
      if (seen.ids[id] != null || queue.some(e => e.id === id)) return false;          // this very scan, told again
      const prev = Number(seen.orders[orderId]) || 0;
      if (prev && Math.abs(at - prev) < MATCH.dupMs) return false;                     // the same order within seconds: one scan
      // the credit (R3): StationSession.who() = the Matching person (latest input if two), or nobody. A welder is never credited.
      let w = whoNow();
      if (w && w.task && w.task !== "matching") w = null;
      try {
        const S = window.StationSession, day = t => (S && typeof S.nyDay === "function" ? S.nyDay(t) : new Date(t).toDateString());
        if (w && day(at) !== day(now)) w = null;                                       // a scan from an earlier day: not whoever signs in now
      } catch (_) {}
      const person = w ? clean(w.person, 80) : "";
      let detail = clean(o.detail || "phone scan", 100);
      if (/^\d+$/.test(detail)) detail = "phone scan"; else detail = detail.replace(/(?<!\d)\d{6}(?!\d)/g, "[#]");
      if (prev && !/\bagain\b/i.test(detail)) detail += " · again";
      // the gap since this person's previous action (a long replay never turns into idle time: the gap only counts forward from an earlier action)
      let base = at;
      if (w) {
        const own = Number(seen.last[person]) || 0, last = lsJson(K.last + device, null) || memLast[device] || null;
        const other = last && last.person === person && Number(last.at) > 0 ? Number(last.at) : 0;
        const from = [own, other, Number(w.startAt) || 0].filter(t => t > 0 && t <= at);
        if (from.length) base = Math.max(...from);
      }
      const ev = {
        id, station: page.station, device, computer: String(page.computer || ""), session: w ? String(w.session || "") : "",
        person, action: "matched", orderId, line: "", sku: "", parts: 0, orders: 0, detail, task: "matching",
        at, seq: nextSeq(device), sincePrevMs: int(at - base, 0, GAP_CAP)
      };
      if (!w) ev.unattributed = true;
      if (page.sandbox) ev.sandbox = true;
      for (const k of ["detail", "session"]) {
        while (bytes(JSON.stringify(ev)) > EVENT_BYTES && ev[k]) ev[k] = ev[k].length > 20 ? ev[k].slice(0, ev[k].length - 20) : "";
      }
      if (bytes(JSON.stringify(ev)) > EVENT_BYTES) return false;
      seen.ids[id] = at; seen.orders[orderId] = Math.max(prev, at);
      if (w) seen.last[person] = Math.max(Number(seen.last[person]) || 0, at);
      lsSet(K.seen + device, JSON.stringify(seen));
      queue.push(ev);
      if (queue.length > MAX_QUEUE) { warn("the queue is full: the oldest " + (queue.length - MAX_QUEUE) + " event(s) were dropped"); queue = queue.slice(-MAX_QUEUE); }
      save(device);
      start();
      return true;
    } catch (e) { warn("matched:", e); return false; }
  }
  /** who a scan would be credited to right now (rule R3), as { person, task, session }, or null (it would be stored unattributed) */
  function matcher() {
    try { const w = whoNow(); return w && (!w.task || w.task === "matching") ? { person: w.person, task: "matching", session: String(w.session || "") } : null; } catch (_) { return null; }
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
  /* ═══ the live layer: what this page is working on RIGHT NOW (the console's stations board; plans/employee-hr/api.md) ═══
     working({ rid, orderNumber, customer, pieces:[{ id, label, sku, listingId, size }], pieceCount, note, station, holdMs, ... })
        an order was scanned or started: it is this person's current order at this station until idle() or holdMs of quiet.
     idle(arg?)   it was completed or closed: no argument = everything this page holds; a rid (string or number) = that order only;
                  { station, rid } = that station's slot. touch() = something is still going on (restarts the quiet time).
     A separate, tiny channel: a `work` write when the order starts or changes, a `beat` every 30 s while it stays open (the server
     shows an order only while a beat is under 3 minutes old, so a closed tab never leaves a ghost), an `idle` write at the end and on
     pagehide. One request at a time, in order; a failed one is retried with backoff; nothing here throws, waits, or touches the
     network inside the call. The person is the sign-in's NAME (never a PIN); nobody signed in = nothing. Thumbnails and the QR
     are resolved by the server from data the app already stores (never Etsy): the page only says which order and which pieces. */
  const LIVE_STATIONS = new Set(["sorting", "welding", "assembly", "shipping", "design", "laser", "sorter", "qr", "inbox"]);
  const LIVE = { keepAlive: 30000, tick: 5000, refresh: 120000, coalesce: 15000, hold: 600000, holdMax: 1800000, pieces: 24, backoff: 60000, bytes: 7000, burst: 5, every: 2000 };
  const slots = new Map(), owed = [];            // station → what is current · idles the server still has to hear
  let liveTimer = 0, liveBusy = false, liveHooked = false, liveWake = 0, liveTokens = LIVE.burst, liveTokenAt = 0;
  /** Order changes ("work" writes) are limited to a burst of 5 and then one every 2 s (only the newest order is ever sent): a stuck scanner
      or 100 scans in 10 s cost about 10 writes, and cannot use up the shop's 600 a minute (one address) that every station's beat shares. */
  function liveRoom(now) {
    if (!liveTokenAt || now < liveTokenAt) liveTokenAt = now;               // (a clock set back never freezes it)
    const add = Math.floor((now - liveTokenAt) / LIVE.every);
    if (add > 0) { liveTokens = Math.min(LIVE.burst, liveTokens + add); liveTokenAt += add * LIVE.every; }
    return liveTokens >= 1;
  }
  function liveLater(now) {
    if (liveWake) return;
    liveWake = setTimeout(() => { liveWake = 0; livePump(); }, Math.max(250, liveTokenAt + LIVE.every - now));
  }
  const scrub = (v, n) => { const s = clean(v, n); return /^\d+$/.test(s) ? "" : s.replace(/(?<!\d)\d{6}(?!\d)/g, "[#]"); };
  const livePieces = list => (Array.isArray(list) ? list : []).slice(0, LIVE.pieces).map(p => {
    if (!p || typeof p !== "object") return null;
    const out = { id: clean(p.id, 40).replace(/[^\w.:-]/g, "_"), label: scrub(p.label, 60) };
    if (pinLike(out.id)) out.id = "";
    const sku = clean(p.sku, 60), lid = String(p.listingId == null ? "" : p.listingId).replace(/\D/g, "").slice(0, 20), size = clean(p.size, 6);
    if (sku && !pinLike(sku)) out.sku = sku;
    if (lid.length >= 3 && !pinLike(lid)) out.listingId = lid;
    if (/^[A-Za-z0-9]{1,6}$/.test(size)) out.size = size;
    return out.id || out.label || out.sku || out.listingId ? out : null;
  }).filter(Boolean);
  const liveFp = s => JSON.stringify([s.person, s.device, s.station, s.order]);
  const liveUrl = sb => "/.netlify/functions/firebaseOrders" + (sb ? "?sandbox=1" : "");
  function liveBody(s, event) {
    const b = { v: 1, event, station: s.station, device: s.device, person: s.person };
    if (s.sandbox) b.sandbox = true;
    if (event === "beat") return { live: b };
    b.computer = s.computer; b.session = s.session; b.startAt = s.startAt;
    b.sentAt = Date.now();                       // this computer's clock now: the server shifts the scan time by the difference to its own (a clock that is minutes off)
    if (event === "work") {
      b.order = Object.assign({}, s.order);
      while (JSON.stringify({ live: b }).length > LIVE.bytes && b.order.pieces.length) b.order.pieces = b.order.pieces.slice(0, Math.max(0, b.order.pieces.length - 4));
    } else b.ended = { kind: s.order.kind, rid: s.order.rid, orderNumber: s.order.orderNumber, title: s.order.title, scannedAt: s.order.scannedAt };
    return { live: b };
  }
  function livePost(sb, body, keepalive) {
    if (typeof fetch !== "function") return Promise.resolve(null);
    let ctl = null, timer = 0;
    try { if (!keepalive && typeof AbortController === "function") { ctl = new AbortController(); timer = setTimeout(() => { try { ctl.abort(); } catch (_) {} }, 15000); } } catch (_) {}
    return fetch(liveUrl(sb), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body), keepalive: !!keepalive, signal: ctl ? ctl.signal : undefined })
      .then(r => { clearTimeout(timer); return Promise.resolve(r && typeof r.json === "function" ? r.json().catch(() => null) : null).then(j => ({ status: r.status, json: j })); }, () => { clearTimeout(timer); return null; });
  }
  const liveFail = s => { s.failures = Math.min((s.failures || 0) + 1, 6); s.nextTry = Date.now() + Math.min(LIVE.backoff, 5000 * Math.pow(2, s.failures - 1)); };
  /** sends the one next thing that is owed (an idle, a changed order, a refresh, a beat); one request at a time, in order */
  function livePump() {
    if (liveBusy) return;
    try {
      const now = Date.now();
      let s = owed.find(x => now >= (x.nextTry || 0)), event = "idle";
      if (!s) {
        event = "";
        for (const x of slots.values()) {
          if (now < (x.nextTry || 0)) continue;
          if (x.sentFp !== liveFp(x) || now - (x.workAt || 0) >= LIVE.refresh) event = "work"; else if (now - x.sentAt >= LIVE.keepAlive) event = "beat";
          if (event === "work" && !liveRoom(now)) { event = ""; liveLater(now); continue; }      // a burst of new orders: the newest goes in a moment
          if (event) { s = x; break; }
        }
      }
      if (!s || !event) return;
      if (event === "work") liveTokens = Math.max(0, liveTokens - 1);
      const fp = liveFp(s);
      liveBusy = true; s.attempted = true;
      livePost(s.sandbox, liveBody(s, event), false).then(r => {
        liveBusy = false;
        const st = r ? r.status : 0;
        if (st >= 200 && st < 300) {
          s.failures = 0; s.nextTry = 0;
          if (event === "idle") { const i = owed.indexOf(s); if (i >= 0) owed.splice(i, 1); }
          else { s.sentAt = Date.now(); if (event === "work") { s.sentFp = fp; s.workAt = s.sentAt; } else if (r.json && r.json.resend) s.sentFp = ""; }
        } else if (st === 400 || st === 413 || st === 422) {          // refused for good: not sent again
          warn("a live write was refused:", st);
          if (event === "idle") { const i = owed.indexOf(s); if (i >= 0) owed.splice(i, 1); } else { s.sentFp = fp; s.sentAt = s.workAt = Date.now(); }
        } else liveFail(s);
        livePump();
      }).catch(() => { liveBusy = false; liveFail(s); });
    } catch (e) { liveBusy = false; warn("live:", e); }
  }
  /** a slot ends: its idle is owed to the server when it ever spoke to it */
  function liveRetire(s) {
    if (slots.get(s.station) === s) slots.delete(s.station);
    if (s.attempted) owed.push({ station: s.station, person: s.person, device: s.device, computer: s.computer, session: s.session, startAt: s.startAt, sandbox: s.sandbox, order: s.order, attempted: true, failures: 0, nextTry: 0 });
  }
  function liveTick() {
    try {
      const now = Date.now(), w = whoNow();
      for (const s of [...slots.values()]) {
        if (!w || w.person !== s.person || w.device !== s.device || !!w.sandbox !== s.sandbox || now - s.touchedAt >= s.hold) liveRetire(s);   // signed out, someone else, or quiet for too long
      }
      livePump();
      if (!slots.size && !owed.length) { clearInterval(liveTimer); liveTimer = 0; }
    } catch (e) { warn("liveTick:", e); }
  }
  /** the page is going away: every idle is sent now without waiting (a lost beacon is no worry: the beat stops and the order expires) */
  function liveLeave() {
    try {
      for (const s of [...slots.values()]) liveRetire(s);
      for (const o of owed.splice(0)) {
        const text = JSON.stringify(liveBody(o, "idle")); let ok = false;
        try { if (navigator.sendBeacon) ok = navigator.sendBeacon(liveUrl(o.sandbox), new Blob([text], { type: "application/json" })); } catch (_) {}
        if (!ok) { try { if (typeof fetch === "function") fetch(liveUrl(o.sandbox), { method: "POST", headers: { "Content-Type": "application/json" }, body: text, keepalive: true }).catch(() => {}); } catch (_) {} }
      }
    } catch (_) {}
  }
  function working(o) {
    try {
      o = o && typeof o === "object" ? o : {};
      const w = whoNow(); if (!w) return false;
      const kind = o.kind === "sheet" ? "sheet" : "order";
      let rid = String(o.rid == null ? (o.orderId == null ? "" : o.orderId) : o.rid).replace(/\D/g, "").slice(0, 30);
      if (pinLike(rid)) rid = "";
      const title = scrub(o.title, 80), station = typeof o.station === "string" && LIVE_STATIONS.has(o.station) ? o.station : w.station;
      if (!LIVE_STATIONS.has(station) || (kind === "order" ? !rid : !(title || clean(o.orderNumber, 40)))) return false;
      const now = Date.now(), prev = slots.get(station);
      const sameWho = !!prev && prev.person === w.person && prev.device === w.device && !!prev.sandbox === !!w.sandbox;
      if (prev && !sameWho) liveRetire(prev);                                 // someone else at this page: the first person's order ends (an idle is owed)
      const same = sameWho && prev.order.kind === kind && prev.order.rid === rid && prev.order.title === title;   // (another order of the same person just replaces it: one write)
      const keepOld = same && now - prev.order.scannedAt < LIVE.coalesce;     // the same scan told again (more detail): one scan, not two
      const at = Number(o.scannedAt), p = keepOld ? prev.order : null;
      const order = { kind, rid, title, orderNumber: scrub(o.orderNumber, 40) || rid,
        customer: o.customer !== undefined ? scrub(o.customer, 60) : (p ? p.customer : ""),
        scannedAt: keepOld ? prev.order.scannedAt : (at > 1e12 && at <= now ? Math.round(at) : now),
        pieces: o.pieces !== undefined ? livePieces(o.pieces) : (p ? p.pieces : []),
        pieceCount: o.pieceCount !== undefined ? int(o.pieceCount, 0, 500) : (p ? p.pieceCount : 0),
        note: o.note !== undefined ? scrub(o.note, 80) : (p ? p.note : "") };
      order.pieceCount = Math.max(order.pieceCount, order.pieces.length);
      const slot = sameWho ? prev : { station, attempted: false, sentFp: "", sentAt: 0, workAt: 0, failures: 0, nextTry: 0 };
      Object.assign(slot, { person: w.person, device: w.device, computer: String(w.computer || ""), session: String(w.session || ""), startAt: Number(w.startAt) || 0, sandbox: !!w.sandbox, order,
        hold: int(o.holdMs == null ? LIVE.hold : o.holdMs, 5000, LIVE.holdMax), touchedAt: now });
      slots.set(station, slot);
      if (!liveTimer) { liveTimer = setInterval(liveTick, LIVE.tick); }
      if (!liveHooked) {
        liveHooked = true;
        try { window.addEventListener("pagehide", liveLeave); } catch (_) {}
        // the network is back: what backed off while it was down is tried at once (not after the one-minute wait)
        try { window.addEventListener("online", () => { for (const x of [...slots.values(), ...owed]) { x.failures = 0; x.nextTry = 0; } setTimeout(livePump, 0); }); } catch (_) {}
      }
      setTimeout(livePump, 0);
      return true;
    } catch (e) { warn("working:", e); return false; }
  }
  function idle(arg) {
    try {
      const a = arg && typeof arg === "object" ? arg : { rid: arg };
      const rid = a.rid == null || a.rid === "" ? "" : String(a.rid).replace(/\D/g, "");
      let n = 0;
      for (const s of [...slots.values()]) {
        if (a.station && s.station !== a.station) continue;
        if (rid && s.order.rid !== rid) continue;
        liveRetire(s); n++;
      }
      if (n) setTimeout(livePump, 0);
      return n > 0;
    } catch (e) { warn("idle:", e); return false; }
  }
  const touch = () => { try { const now = Date.now(); for (const s of slots.values()) s.touchedAt = now; return slots.size > 0; } catch (_) { return false; } };
  window.StationActivity = {
    log, matched, matcher, flush: () => flush(true), pending: () => queue.length, discard, working, idle, touch,
    current: () => [...slots.values()].map(s => ({ station: s.station, device: s.device, rid: s.order.rid, orderNumber: s.order.orderNumber, kind: s.order.kind, scannedAt: s.order.scannedAt, pieces: s.order.pieces.length, sent: s.sentFp === liveFp(s) })),
    who: () => { const w = whoNow(); return w ? { person: w.person, station: w.station, device: w.device, computer: w.computer, session: w.session } : null; }
  };
  // events left from an earlier page load (offline, closed too fast) go out soon after the station has set itself up
  try { start(); setTimeout(() => { try { flush(); } catch (_) {} }, 4000); } catch (_) {}
})();
