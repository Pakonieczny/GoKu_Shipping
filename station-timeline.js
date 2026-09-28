/*  station-timeline.js — the one station module every production station loads (Paul, 28 Sep 19:21: A4, C5, C6).
 *  Loaded next to order-timeline.js (station mode). Records every scan (who, where, when, how), what the station did
 *  with the order, and stops a worker from going on with a cancelled order: a full-screen red alert on the scan, and a
 *  "Do it anyway?" confirm before any action that moves a cancelled order on.
 *
 *    StationTimeline.init({ station: "welding"|"assembly"|"shipping"|"sorting", device: "weld-1"|"assembly-2"|…,
 *                           getEmployee: () => name, sandbox })
 *    StationTimeline.scanned(orderId, { how: "scan"|"typed"|"paste", extra })
 *        → Promise<{ cancelled: false } | { cancelled: true, record }>
 *        records a `scan` event (stable id `${device}-${orderId}-${minute}`), asks the server whether the order is
 *        cancelled (≈ 2.5 s at most; offline or slow is not blocked, the scan then says it was not checked) and shows
 *        the alert when it is. extra is kept on the scan (small); extra.etsyStatus — the receipt status the page already
 *        read from Etsy — also counts as a cancel when it says so (no extra Etsy call).
 *    StationTimeline.did(type, orderId, text, data)   welded · assembled · packed · labelPrinted · shipped ·
 *        etsyCompleted · sorted (any station type). Stable id `${device}-${orderId}-${type}-${minute}`.
 *    (scanned's opts.quiet: the page draws its own alert, as the Sorting station does for a whole batch)
 *    StationTimeline.isCancelled(orderId)  → the cached cancel record ({ at, by, why, source }) or null
 *    StationTimeline.guard(orderId, { action, anchor })  → Promise<boolean>: true to go on. For a cancelled order it
 *        asks "This order is cancelled. Do it anyway?" inline (inside the alert while it is up); a yes is recorded.
 *
 *  Never breaks the page: every entry point is wrapped, a failure answers "not cancelled" / true and the page works as
 *  before. No Etsy calls: the cancel check reads Charm_Nest_Cancelled through firebaseOrders?cancelCheck=. */
(function () {
  "use strict";
  if (window.StationTimeline) return;
  const LABEL = { welding: "Welding", assembly: "Assembly", shipping: "Shipping", sorting: "Sorting" };
  const HOW = new Set(["scan", "typed", "paste"]);
  const CHECK_MS = 2500, CACHE = "stationTimeline.cancelled.v1";
  const cfg = { station: "", device: "", getEmployee: null, sandbox: false };
  const known = new Map();     // orderId → cancel record (only cancelled orders are kept)
  const inflight = new Map();  // orderId → the cancel check under way
  const cleared = new Map();   // orderId → when the server last said "not cancelled" (the guard asks again after FRESH_MS)
  const FRESH_MS = 30000;
  // (an answer older than FRESH_MS is never used again: a station open for weeks kept every order it ever scanned)
  const prune = () => { if (cleared.size > 500) { const now = Date.now(); for (const [k, t] of cleared) if (now - t >= FRESH_MS) cleared.delete(k); } };
  let configured = false, keysOn = false;

  const digits = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
  const minute = at => Math.floor((at || Date.now()) / 60000);
  const where = () => LABEL[cfg.station] || cfg.station || "the station";
  const warn = (...a) => { try { console.warn("[StationTimeline]", ...a); } catch (_) {} };
  function who() {
    try { const n = cfg.getEmployee ? cfg.getEmployee() : ""; if (n) return String(n).trim().slice(0, 80); } catch (_) {}
    try { return String(localStorage.getItem("employee_name") || "").trim().slice(0, 80); } catch (_) { return ""; }
  }
  function ot() {
    const o = window.OrderTimeline;
    if (o && !configured) { try { o.config({ mode: "station", station: cfg.station, device: cfg.device, by: who(), sandbox: !!cfg.sandbox }); configured = true; } catch (_) {} }
    return o || null;
  }
  function rec(ev) { try { const o = ot(); return o ? o.record(Object.assign({ station: cfg.station, device: cfg.device }, ev)) : null; } catch (_) { return null; } }
  const withTimeout = (p, ms) => new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error("timeout")), ms);
    Promise.resolve(p).then(v => { clearTimeout(t); res(v); }, e => { clearTimeout(t); rej(e); });
  });
  function small(o) {
    if (!o || typeof o !== "object") return undefined;
    try { const j = JSON.stringify(o); return j.length <= 1200 ? JSON.parse(j) : { note: "too large to keep" }; } catch (_) { return undefined; }
  }

  /* ── the cached answer: cancelled orders stay known across a reload, so the guard still works ── */
  function loadCache() {
    try { const o = JSON.parse(localStorage.getItem(CACHE) || "{}"); for (const k of Object.keys(o || {})) if (o[k] && typeof o[k] === "object") known.set(k, o[k]); } catch (_) {}
  }
  function saveCache() {
    try { const all = [...known.entries()].slice(-300); localStorage.setItem(CACHE, JSON.stringify(Object.fromEntries(all))); } catch (_) {}
  }
  function norm(r) {
    r = r && typeof r === "object" ? r : {};
    let at = Number(r.at) || 0; if (at > 1e9 && at < 1e12) at *= 1000;
    return { at, by: String(r.by || "").slice(0, 80), why: String(r.why || "").slice(0, 400), source: String(r.source || "").slice(0, 20) };
  }
  function remember(id, r) { known.delete(id); known.set(id, norm(r)); saveCache(); return known.get(id); }
  function forget(id) { if (known.delete(id)) saveCache(); }

  /* The orders asked about in the same moment (a Sorting batch scans them all at once) go in one request, up to the 60
     the server answers at a time: a batch of 40 was 40 function calls at once, each a cold start that could miss the
     2.5 s answer and let a cancelled order through unchecked. */
  let group = null;
  function askGrouped(o, id) {
    if (!group || group.ids.size >= 60) {
      const g = group = { ids: new Set() };
      g.p = new Promise(r => setTimeout(r, 0)).then(() => { if (group === g) group = null; return o.cancelCheck([...g.ids]); });
    }
    group.ids.add(id);
    return group.p;
  }
  /* the server's answer for one order: its cancel record, null when it is not cancelled; throws when it is no answer
     (a 200 without a `cancelled` map must not count as "clear", nor wipe a cancel this page already knows) */
  function answerOf(id, j) {
    if (!j || typeof j !== "object" || !j.cancelled || typeof j.cancelled !== "object") throw new Error("no answer");
    return j.cancelled[id] || null;
  }
  /** Asks the server (and the Etsy status the page already has) whether one order is cancelled. Never rejects.
      late(record): an answer that comes after the 2.5 s and says cancelled (the scan went on unchecked) */
  function check(id, extra, late) {
    const p = (async () => {
      let state = "unchecked", record = null, why = "";
      const o = ot();
      if (o && typeof o.cancelCheck === "function") {
        let ask = null;
        try {
          ask = Promise.resolve(askGrouped(o, id));
          const r = answerOf(id, await withTimeout(ask, CHECK_MS));
          if (r) { state = "cancelled"; record = remember(id, r); } else { state = "clear"; forget(id); cleared.set(id, Date.now()); prune(); }
        } catch (e) {
          why = (navigator.onLine === false ? "offline" : String((e && e.message) || e || "failed")).slice(0, 80);
          if (known.has(id)) { state = "cancelled"; record = known.get(id); }   // a cancel we already knew still counts
          else if (ask && why === "timeout")                                     // slow, not lost: a late "cancelled" still counts
            ask.then(j => { const r = answerOf(id, j); if (!r || known.has(id)) return; const rec = remember(id, r); if (late) late(rec); }).catch(() => {});
        }
      } else why = "no timeline client";
      const es = extra && typeof extra.etsyStatus === "string" ? extra.etsyStatus : "";
      if (state !== "cancelled" && /cancel/i.test(es)) { state = "cancelled"; record = remember(id, { at: 0, by: "Etsy", source: "etsy", why: `Etsy shows this order as "${es}"` }); }
      return { state, record, why };
    })();
    inflight.set(id, p);
    p.then(() => { if (inflight.get(id) === p) inflight.delete(id); });
    return p;
  }

  async function scanned(orderId, opts) {
    try {
      const id = digits(orderId); if (!id) return { cancelled: false };
      opts = opts || {};
      const how = HOW.has(opts.how) ? opts.how : "scan", at = Date.now(), by = who();
      const extra = small(opts.extra);
      const res = await check(id, extra, rec => { try { if (!opts.quiet) showAlert(id, rec); } catch (e) { warn("alert failed", e); } });
      const data = { how, check: res.state };
      if (res.why) data.checkNote = res.why;
      if (extra) data.extra = extra;
      rec({ orderId: id, type: "scan", at, by, id: `${cfg.device}-${id}-${minute(at)}`, data,
            text: `Scanned at ${where()}${cfg.device ? " (" + cfg.device + ")" : ""}${how === "scan" ? "" : " · " + how}${res.state === "cancelled" ? " · cancelled order" : ""}` });
      if (res.state === "unchecked") warn("cancel check not done for", id, "—", res.why);
      if (res.state === "cancelled") { try { if (!opts.quiet) showAlert(id, res.record); } catch (e) { warn("alert failed", e); } return { cancelled: true, record: res.record }; }
      return { cancelled: false };
    } catch (e) { warn("scan not recorded", e); return { cancelled: false }; }
  }

  function did(type, orderId, text, data) {
    try {
      const id = digits(orderId); if (!id || !type) return null;
      const at = Date.now(), d = Object.assign({}, small(data) || {});
      if (known.has(id)) d.despiteCancel = true;
      const lbl = (window.OrderTimeline && window.OrderTimeline.TYPES && window.OrderTimeline.TYPES[type] && window.OrderTimeline.TYPES[type].label) || type;
      return rec({ orderId: id, type, at, by: who(), id: `${cfg.device}-${id}-${type}-${minute(at)}`,
                   text: text ? String(text).slice(0, 200) : `${lbl} at ${where()}`, data: Object.keys(d).length ? d : undefined });
    } catch (_) { return null; }
  }

  function isCancelled(orderId) { try { return known.get(digits(orderId)) || null; } catch (_) { return null; } }

  async function guard(orderId, opts) {
    try {
      const id = digits(orderId); if (!id) return true;
      opts = opts || {};
      if (inflight.has(id)) {   // the scan's check is still on its way: wait for it (≤ 2.5 s), and say so
        const stop = waiting(`Checking whether order ${id} is cancelled…`);
        try { await withTimeout(inflight.get(id), CHECK_MS + 500); } catch (_) {} finally { stop(); }
      } else if (!known.has(id) && !(Date.now() - (cleared.get(id) || 0) < FRESH_MS)) {
        // no recent answer (never scanned here, the scan's check was slow or offline, or it was cancelled since): ask now
        // (≤ 2.5 s); a cancel found here raises the full-screen alert first, and the question is asked inside it
        const stop = waiting(`Checking whether order ${id} is cancelled…`);
        let res = null;
        try { res = await check(id); } catch (_) {} finally { stop(); }
        if (res && res.state === "cancelled" && !opts.quiet) { try { showAlert(id, res.record); } catch (e) { warn("alert failed", e); } }
      }
      const r = known.get(id); if (!r) return true;
      const yes = await confirmCancelled(id, opts);
      if (yes) rec({ orderId: id, type: "note", by: who(), id: `${cfg.device}-${id}-anyway-${minute()}`,
                     text: `Went ahead although cancelled${opts.action ? ": " + String(opts.action).slice(0, 60) : ""}`,
                     data: { despiteCancel: true, action: String(opts.action || "").slice(0, 60) } });
      return yes;
    } catch (e) { warn("guard failed, not blocking", e); return true; }
  }

  /* ── looks ──────────────────────────────────────────────────────────────────────────────────────────────────── */
  /* the station alert follows the order-view design spec (§8, shot-alert.png): clay red, flashes twice, the words slam in */
  const SANS = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,system-ui,sans-serif", MONO = "ui-monospace,'SF Mono',Menlo,Consolas,monospace";
  const CSS = `
.sttl-alert{position:fixed;inset:0;z-index:2147483000;display:grid;place-items:center;box-sizing:border-box;padding:4vh 2vw;overflow:auto;text-align:center;
  background:#7e2415;color:#fff;font-family:${SANS};-webkit-font-smoothing:antialiased;opacity:0;transition:opacity .26s ease}
.sttl-alert.in{opacity:1;animation:sttlAlarm 1.1s ease-in-out 2}.sttl-alert.out{opacity:0;transition-duration:.24s}
.sttl-card{width:100%;max-width:1400px;transform:scale(.94);transition:transform .42s cubic-bezier(.2,.8,.2,1)}
.sttl-alert.in .sttl-card{transform:scale(1)}.sttl-alert.out .sttl-card{transform:scale(.97);transition-duration:.24s}
.sttl-alert .sttl-h1{margin:0;font:800 clamp(48px,8.6vw,132px)/.95 ${SANS};letter-spacing:.01em;animation:sttlSlam .36s cubic-bezier(.3,1.7,.5,1) both}
.sttl-alert .sttl-h2{margin:18px 0 26px;font:700 clamp(24px,3.2vw,44px)/1.1 ${SANS};letter-spacing:.14em;color:#ffd9cf;animation:sttlSlam .36s cubic-bezier(.3,1.7,.5,1) .08s both}
.sttl-rows{display:grid;gap:10px;justify-items:center}
.sttl-row{font:18px/1.5 ${MONO};color:#ffe9e3}
.sttl-row b{color:#fff;font-weight:700}
.sttl-why{opacity:.9}
.sttl-rows.many .sttl-row{font-size:16px}
.sttl-who{margin-top:6px;font:16px/1.5 ${MONO};color:#ffe9e3;opacity:.8}
.sttl-guard{margin:26px auto 0;max-width:620px;display:flex;align-items:center;justify-content:center;flex-wrap:wrap;gap:10px 12px;padding:14px 16px;border-radius:14px;
  background:#fff;color:#7e2415;font:700 17px/1.35 ${SANS};animation:sttlIn .32s cubic-bezier(.2,.8,.2,1) both}
.sttl-guard>span{flex-basis:100%}
.sttl-alert .sttl-ok,.sttl-alert .sttl-ok:focus,.sttl-alert .sttl-ok:hover{margin-top:36px;padding:16px 48px;border:0;border-radius:14px;background:#fff;color:#7e2415;
  font:700 22px/1.2 ${SANS};cursor:pointer;box-shadow:0 10px 30px rgba(0,0,0,.22);transition:transform .15s ease}
.sttl-alert .sttl-ok:hover{transform:translateY(-1px)}.sttl-alert .sttl-ok:active{transform:scale(.97)}
.sttl-alert .sttl-ok:focus-visible{outline:3px solid rgba(255,255,255,.65);outline-offset:4px}
.sttl-hint{margin-top:12px;font:13px ${SANS};color:#ffd9cf;opacity:.75}
.sttl-bar{position:fixed;top:18px;left:50%;z-index:2147482990;display:flex;align-items:center;gap:12px;max-width:calc(100vw - 32px);box-sizing:border-box;
  padding:11px 12px 11px 16px;background:#fff;color:#3a1a12;border:1px solid rgba(168,50,30,.22);border-left:5px solid #a8321e;border-radius:14px;
  box-shadow:0 18px 50px rgba(60,20,10,.22);font:600 15px/1.35 ${SANS};
  opacity:0;transform:translateX(-50%) scale(.9);transition:opacity .26s ease,transform .36s cubic-bezier(.2,.8,.2,1)}
.sttl-bar.in{opacity:1;transform:translateX(-50%) scale(1)}.sttl-bar.out{opacity:0;transform:translateX(-50%) scale(.96);transition-duration:.18s}
.sttl-bar b{color:#a8321e}
.sttl-btn,.sttl-btn:focus,.sttl-btn:hover{border:0;border-radius:10px;padding:9px 14px;font:700 14px/1 ${SANS};cursor:pointer;white-space:nowrap}
.sttl-btn.stop,.sttl-btn.stop:focus{background:#f3eeec;color:#3a1a12}.sttl-btn.go,.sttl-btn.go:focus{background:#a8321e;color:#fff}
.sttl-btn:focus-visible{outline:3px solid rgba(168,50,30,.45);outline-offset:2px}
.sttl-alert .sttl-guard .sttl-btn{padding:11px 18px;font-size:15px}
.sttl-wait{position:fixed;left:50%;bottom:22px;z-index:2147482990;display:flex;align-items:center;gap:9px;padding:8px 14px;border-radius:999px;background:#fff;color:#333;
  box-shadow:0 8px 26px rgba(0,0,0,.18);font:500 13px/1.2 ${SANS};transform:translateX(-50%);animation:sttlFade .2s ease both}
.sttl-spin{width:14px;height:14px;border-radius:50%;border:2px solid rgba(0,0,0,.14);border-top-color:#a8321e;animation:sttlSpin .8s linear infinite}
@keyframes sttlSpin{to{transform:rotate(360deg)}}
@keyframes sttlFade{from{opacity:0}to{opacity:1}}
@keyframes sttlIn{from{opacity:0;transform:scale(.92)}to{opacity:1;transform:scale(1)}}
@keyframes sttlSlam{from{opacity:0;transform:scale(1.25)}}
@keyframes sttlAlarm{0%,100%{background-color:#7e2415}50%{background-color:#b1321c}}
@media (prefers-reduced-motion:reduce){.sttl-alert,.sttl-bar{transition-duration:.12s!important}.sttl-alert.in{animation:none}
  .sttl-card,.sttl-alert.in .sttl-card{transform:none!important;transition:none}.sttl-bar.in,.sttl-bar{transform:translateX(-50%)!important}
  .sttl-alert .sttl-h1,.sttl-alert .sttl-h2,.sttl-guard{animation:none}}`;
  function css() {
    if (document.getElementById("sttlStyle")) return;
    const s = document.createElement("style"); s.id = "sttlStyle"; s.textContent = CSS;
    (document.head || document.documentElement).appendChild(s);
  }
  function el(tag, cls, text) { const e = document.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; }
  function ago(at) {
    const s = Math.max(0, (Date.now() - at) / 1000); if (s < 90) return "just now";
    const m = s / 60; if (m < 60) return Math.round(m) + " min ago";
    const h = m / 60; if (h < 36) return Math.round(h) + " h ago";
    return Math.round(h / 24) + " days ago";
  }
  function whenText(at) {
    if (!(at > 1e12)) return "";
    try {
      const d = new Date(at);
      return `${d.toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" })}, ${d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" })} (${ago(at)})`;
    } catch (_) { return ""; }
  }
  function byText(r) {
    if (r.source === "etsy" || /^etsy$/i.test(r.by)) return "Cancelled on Etsy";
    return r.by ? `Cancelled by ${r.by}` : "Cancelled";
  }

  /* ── the warning sound: three short tones (WebAudio; unlocked on the first click or key, as browsers ask) ── */
  let actx = null;
  function audio() {
    try {
      if (!actx) { const C = window.AudioContext || window.webkitAudioContext; if (!C) return null; actx = new C(); }
      if (actx.state === "suspended") actx.resume().catch(() => {});
      return actx;
    } catch (_) { return null; }
  }
  function beep() {
    try {
      const c = audio(); if (!c) return;
      const t0 = c.currentTime + 0.03;
      for (let i = 0; i < 3; i++) {
        const t = t0 + i * 0.34, o = c.createOscillator(), g = c.createGain();
        o.type = "square"; o.frequency.setValueAtTime(i === 1 ? 660 : 880, t);
        g.gain.setValueAtTime(0.0001, t); g.gain.exponentialRampToValueAtTime(0.22, t + 0.02); g.gain.exponentialRampToValueAtTime(0.0001, t + 0.24);
        o.connect(g); g.connect(c.destination); o.start(t); o.stop(t + 0.26);
      }
    } catch (_) {}
  }

  /* ── the alert: one at a time; a second cancelled order scanned while it is up joins the same alert ── */
  const A = { root: null, rows: null, who: null, ok: null, guardBox: null, list: [], shownAt: 0, prevFocus: null, pending: null };
  const alertUp = () => !!(A.root && A.root.isConnected && !A.root.classList.contains("out"));
  function buildAlert() {
    css();
    const root = el("div", "sttl-alert");
    root.setAttribute("role", "alertdialog"); root.setAttribute("aria-modal", "true"); root.setAttribute("aria-label", "CANCELLED ORDER — DO NOT PROCEED");
    const card = el("div", "sttl-card");
    const rows = el("div", "sttl-rows"), who = el("div", "sttl-who"), guardBox = el("div"), ok = el("button", "sttl-ok", "Understood");
    ok.type = "button";
    ok.addEventListener("click", () => acknowledge("button"));
    card.append(el("h1", "sttl-h1", "CANCELLED ORDER"), el("h2", "sttl-h2", "DO NOT PROCEED"), rows, who, guardBox, ok, el("div", "sttl-hint", "Press Understood or Enter to close"));
    root.appendChild(card);
    Object.assign(A, { root, rows, who, ok, guardBox });
  }
  function renderRows() {
    A.rows.textContent = "";
    A.rows.classList.toggle("many", A.list.length > 1);
    for (const it of A.list) {
      const row = el("div", "sttl-row"), r = it.record || {};
      row.append(el("b", "", `Order ${it.orderId}`), ` · ${[byText(r), whenText(r.at)].filter(Boolean).join(" · ")}`);
      row.append(el("div", "sttl-why", r.why ? `Reason: ${r.why}` : "No reason given"));
      A.rows.appendChild(row);
    }
    const by = who();
    A.who.textContent = `Scanned at ${where()}${by ? " by " + by : ""} · this scan is on the order's timeline`;
  }
  function showAlert(id, record) {
    if (!document.body) return;
    const again = alertUp();
    if (!A.root) buildAlert();
    const i = A.list.findIndex(x => x.orderId === id);
    if (i >= 0) A.list.splice(i, 1);
    A.list.unshift({ orderId: id, record: record || {}, shownAt: Date.now() });
    renderRows();
    beep();
    if (again) return;
    A.shownAt = Date.now();
    A.prevFocus = document.activeElement;
    A.root.classList.remove("in", "out");
    document.body.appendChild(A.root);
    void A.root.offsetWidth;                         // start the zoom + fade from its first frame
    A.root.classList.add("in");
    try { A.ok.focus({ preventScroll: true }); } catch (_) {}
  }
  function acknowledge(how) {
    if (!alertUp()) return;
    const closedAt = Date.now(), by = who();
    for (const it of A.list)
      rec({ orderId: it.orderId, type: "cancelAlert", by, at: closedAt, id: `${cfg.device}-${it.orderId}-alert-${minute(it.shownAt)}`,
            text: `${by || "Someone"} saw the cancel alert at ${where()}`,
            data: { how, shownAt: it.shownAt, seenAfterMs: closedAt - it.shownAt, cancelledBy: (it.record && it.record.by) || "", why: ((it.record && it.record.why) || "").slice(0, 200) } });
    if (A.pending) { const p = A.pending; A.pending = null; p.resolve(false); }
    A.list = []; A.guardBox.textContent = "";
    const root = A.root; root.classList.add("out"); root.classList.remove("in");
    setTimeout(() => { try { if (root.classList.contains("out")) root.remove(); } catch (_) {} }, 240);
    const back = A.prevFocus; A.prevFocus = null;
    try { if (back && back.isConnected && back.focus) back.focus({ preventScroll: true }); } catch (_) {}
  }
  function onKey(e) {
    try {
      if (!alertUp() || !e.isTrusted) return;           // a scan's own (synthetic) Enter goes on to the page
      if (e.key === "Tab") {                            // keep the keyboard inside the alert
        const f = [...A.root.querySelectorAll("button")]; if (!f.length) return;
        const i = f.indexOf(document.activeElement); e.preventDefault(); e.stopPropagation();
        f[(i + (e.shiftKey ? -1 : 1) + f.length) % f.length].focus(); return;
      }
      if (e.key === "Enter" || e.key === "NumpadEnter" || e.key === "Escape") {
        e.preventDefault(); e.stopPropagation();
        if (e.key === "Escape" || e.repeat || Date.now() - A.shownAt < 450) return;   // a held Enter does not dismiss it unseen
        const f = document.activeElement;
        if (f && f !== A.ok && A.root.contains(f) && f.tagName === "BUTTON") f.click(); else acknowledge("enter");
      }
    } catch (_) {}
  }

  /* ── "This order is cancelled. Do it anyway?" — inside the alert while it is up, otherwise a small inline bar ── */
  let bar = null;
  function confirmCancelled(id, opts) {
    return new Promise(resolve => {
      const done = v => { try { resolve(!!v); } catch (_) {} };
      if (A.pending) { const p = A.pending; A.pending = null; p.resolve(false); }
      closeBar(false);
      const q = el("span", "", ""), stop = el("button", "sttl-btn stop", "Stop"), go = el("button", "sttl-btn go", "Do it anyway");
      stop.type = go.type = "button";
      q.append("Order ", el("b", "", "#" + id), " is cancelled. Do it anyway?");
      if (alertUp()) {
        const box = el("div", "sttl-guard");
        box.append(q, stop, go); A.guardBox.textContent = ""; A.guardBox.appendChild(box);
        A.pending = { resolve: v => { A.guardBox.textContent = ""; done(v); } };
        stop.addEventListener("click", () => { const p = A.pending; A.pending = null; if (p) p.resolve(false); try { A.ok.focus(); } catch (_) {} });
        go.addEventListener("click", () => { const p = A.pending; A.pending = null; if (p) p.resolve(true); acknowledge("anyway"); });
        try { stop.focus({ preventScroll: true }); } catch (_) {}
        return;
      }
      css();
      const b = el("div", "sttl-bar"); b.setAttribute("role", "alertdialog"); b.setAttribute("aria-label", "Cancelled order");
      b.append(q, stop, go);
      const finish = v => { if (bar && bar.el === b) { bar = null; hide(b); } document.removeEventListener("keydown", esc, true); done(v); };
      const esc = e => { if (e.key === "Escape" && bar && bar.el === b) { e.preventDefault(); e.stopPropagation(); finish(false); } };
      stop.addEventListener("click", () => finish(false));
      go.addEventListener("click", () => finish(true));
      document.addEventListener("keydown", esc, true);
      bar = { el: b, finish };
      document.body.appendChild(b);
      place(b, opts && opts.anchor);
      void b.offsetWidth; b.classList.add("in");
      try { stop.focus({ preventScroll: true }); } catch (_) {}
    });
  }
  function place(b, anchor) {
    try {
      const a = typeof anchor === "string" ? document.querySelector(anchor) : anchor;
      const r = a && a.getBoundingClientRect && a.getBoundingClientRect();
      if (!r || !(r.width > 0 && r.height > 0)) return;              // hidden or none: top centre
      const w = b.offsetWidth, x = Math.min(Math.max(r.left + r.width / 2, w / 2 + 16), innerWidth - w / 2 - 16);
      b.style.left = x + "px"; b.style.top = Math.min(r.bottom + 10, innerHeight - b.offsetHeight - 16) + "px";
    } catch (_) {}
  }
  function hide(n) { try { n.classList.add("out"); n.classList.remove("in"); setTimeout(() => { try { n.remove(); } catch (_) {} }, 200); } catch (_) {} }
  function closeBar(v) { if (bar) { const f = bar.finish; f(v); } }
  function waiting(text) {
    let n = null;
    const t = setTimeout(() => { try { css(); n = el("div", "sttl-wait"); n.append(el("span", "sttl-spin"), el("span", "", text)); document.body.appendChild(n); } catch (_) {} }, 250);
    return () => { clearTimeout(t); if (n) hide(n); };
  }

  function init(o) {
    try {
      o = o || {};
      cfg.station = String(o.station || cfg.station || "").slice(0, 20);
      cfg.device = String(o.device || cfg.device || "").slice(0, 40);
      if (typeof o.getEmployee === "function") cfg.getEmployee = o.getEmployee;
      if (o.sandbox !== undefined) cfg.sandbox = !!o.sandbox;
      configured = false; ot();
      if (!keysOn) {
        keysOn = true; loadCache();
        window.addEventListener("keydown", onKey, true);
        const unlock = () => { audio(); window.removeEventListener("pointerdown", unlock, true); window.removeEventListener("keydown", unlock, true); };
        window.addEventListener("pointerdown", unlock, true); window.addEventListener("keydown", unlock, true);
      }
    } catch (e) { warn("init failed", e); }
    return api;
  }

  const api = {
    init, scanned, did, isCancelled, guard,
    alertOpen: () => { try { return alertUp(); } catch (_) { return false; } },
    config: () => Object.assign({}, cfg, { getEmployee: undefined })
  };
  window.StationTimeline = api;
})();
