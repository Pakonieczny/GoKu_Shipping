/*  charm-nest-bridge.js — the Charm Sorter as master of the Design Station.
 *  ═══════════════════════════════════════════════════════════════════════
 *  Loaded after charm-nest-1.html's own script and built on it: the same S,
 *  api(), agent log, sheets, nesting and cloud helpers. Sections follow the
 *  design document ("Charm Sorter ⇄ Design Station bridge", draft 4):
 *
 *    17 · DesignLink   the station framed and driven over postMessage (§4.7), heartbeat, session log, console
 *    18 · Orders       pull + interpret, claims, date rule, re-validation (§5, §10.3)
 *    19 · Master       master files → per-SKU designs, labels, vision fallback, overrides (§6)
 *    20 · Pool         one charm per order line and copy, keyed deterministically (§6.4)
 *    21 · Engrave      words (Claude), the checked flip, the fit, the review queue, back files (§7)
 *    22 · Sets         one set per run, one label per sheet, manifest, completion over the bridge (§8)
 *    23 · RunCtl       the two halves, the persistent run record, resume, stop, Auto/Manual (§10)
 *    24 · Review       every decision a person must make, with the problem and the quick fixes (§11)
 *    25 · boot
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
(function () {
const O = CharmNestOrders, G = CharmNestGeom, P = CharmNestPDF;
const MM = 25.4 / 72, PT = 72 / 25.4;
const B = window.B = { link: null, orders: { rows: [], byKey: new Map(), pulledAt: 0, stale: false, snapshot: null, filtered: 0 }, master: { entries: new Map(), files: [], loadedAt: 0, loading: null, jobs: new Map() }, maps: { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [], rows: [] }, loadedAt: 0 }, pool: { rows: new Map(), sources: new Map() }, engrave: { items: new Map(), fonts: { ok: false, Regular: null, Semibold: null, error: null, loading: null } }, review: { items: [] }, run: null, sets: new Map(), employee: (localStorage.getItem("cn.employee") || "").trim() };
const SOURCE_LABEL = { personalization: "the personalisation box", personalisation: "the personalisation box", buyerMessage: "the buyer's message", staffNote: "the staff note", messages: "the staff messages", none: "", "": "" };
const el = (tag, cls, html) => { const e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
const fmtT = t => new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const sleep = ms => new Promise(r => setTimeout(r, ms));
const notifyPerson = (title, body) => { if (S.settings.notify === "on" && "Notification" in window && Notification.permission === "granted") { try { new Notification(title, { body }); } catch (_) {} } };
const employeeName = () => B.employee || (B.link && B.link.state() && B.link.state().employee) || "";
function askEmployee() {
  const cur = employeeName();
  const v = prompt("Your name — recorded with every approval and decision:", cur || "");
  if (v && v.trim()) { B.employee = v.trim(); localStorage.setItem("cn.employee", B.employee); }
  return employeeName();
}

/* ═══ 17 · DesignLink — the Design Station as a slave ═════════════════════ */
const LiveStrip = window.LiveStrip = (() => {
  const rows = [];
  function push(ev) { rows.push({ t: ev.t || Date.now(), kind: ev.kind, text: ev.text || (ev.html ? ev.html.replace(/<[^>]+>/g, "") : "") }); if (rows.length > 5) rows.shift(); render(); }
  function render() { const b = document.getElementById("liveStripBody"); if (!b) return; b.innerHTML = rows.length ? rows.map(r => `<span class="ev"><span class="t">${fmtT(r.t)}</span><b>${esc(r.kind)}</b> ${esc(r.text).slice(0, 140)}</span>`).join("") : "bridge idle"; const n = Review.count(); const rb = document.getElementById("tabReviewN"); if (rb) rb.textContent = n ? String(n) : ""; const eb = document.getElementById("tabEngraveN"); if (eb) { const k = Engrave.pendingCount(); eb.textContent = k ? String(k) : ""; } }
  return { push, render, rows };
})();

const DesignLink = window.DesignLink = (() => {
  const S_ = { frame: null, nonce: null, id: 0, pending: new Map(), state: null, log: [], logBuf: [], up: false, control: false, misses: 0, hb: null, flushT: null, dropped: 0, lastHello: 0, count: 0, replies: 0, errors: 0 };
  const origin = () => (S.settings.dsOrigin || DEFAULTS.dsOrigin).replace(/\/+$/, "");
  const frameUrl = () => `${origin()}/design-1.html?bridge=1${S.settings.sandbox === "on" ? "&sandbox=1" : "&sandbox=0"}`;
  function mount(host) {
    if (S_.frame) return S_.frame;
    // the frame lives in a fixed dock on <body>, never inside a tab: re-parenting an iframe reloads it and would end the
    // session, so the dock is laid over the Design Station tab's placeholder when that tab shows and shrinks to a
    // picture-in-picture panel on every other tab (design §5.7 live view)
    const dock = Dock.ensure();
    const f = document.createElement("iframe"); f.id = "dsFrame"; f.title = "Design Station 1"; f.allow = "clipboard-read; clipboard-write"; f.src = frameUrl();
    const veil = el("div", "veil", `<div>Design Station<br><span style="font-size:12.5px;color:var(--ink45)">press <b>Take control</b> to open the session</span></div>`);
    dock.body.append(f, veil); Dock.setHost(host);
    S_.frame = f; S_.veil = veil;
    // the first hello must wait for the frame to navigate (a message posted to the blank frame is lost); later loads (a reload
    // of the station) re-open the session by themselves
    S_.loaded = new Promise(resolve => f.addEventListener("load", () => resolve(), { once: true }));
    f.addEventListener("load", () => { S_.loadedAt = Date.now(); agent({ bridge: true }, "DS", `Design Station frame loaded (${frameUrl()})`); if (S_.control && S_.loadedAt - S_.mountedAt > 500) open().catch(e => agent({ bridge: true }, "warn", `hello after reload failed: ${e.message}`)); });
    S_.mountedAt = Date.now();
    window.addEventListener("message", onMessage);
    return f;
  }
  async function open() {
    if (!S_.frame) throw new Error("the Design Station frame is not mounted");
    if (!S_.nonce) S_.nonce = uid() + uid();
    S_.control = true;
    if (S_.loaded) await Promise.race([S_.loaded, sleep(20000)]);
    let st;
    try { st = await call("hello", { sorterClientId: S_.nonce, runId: B.run ? B.run.runId : null }, { timeoutMs: 15000 }); }
    catch (e) { if (!/answer in time/.test(e.message)) throw e; agent({ bridge: true }, "warn", "hello unanswered — trying once more"); st = await call("hello", { sorterClientId: S_.nonce, runId: B.run ? B.run.runId : null }, { timeoutMs: 30000 }); }
    S_.state = st; S_.up = true; S_.misses = 0; S_.lastHello = Date.now(); if (st.etsy && st.etsy.meter) etsyReadout(st.etsy.meter);
    if (!!st.sandbox !== (S.settings.sandbox === "on")) { S_.control = false; S_.up = false; const why = `the station is in ${st.sandbox ? "SANDBOX" : "production"} mode but this sorter is in ${S.settings.sandbox === "on" ? "SANDBOX" : "production"} mode`; agent({ bridge: true }, "warn", `Session refused: ${why}`); toast(`Session refused — ${why}. Reload the frame.`, "bad", 9000); throw new Error(why); }
    if (st.employee && !B.employee) { B.employee = st.employee; localStorage.setItem("cn.employee", st.employee); }
    S_.veil && S_.veil.classList.add("hidden"); Dock.layout();
    agent({ bridge: true }, "DS", `Session ${S_.nonce.slice(0, 4)} open on ${st.bench} · ${st.counts.open} open orders (${st.counts.hydrated} read) · ${st.selection.length} selected · Etsy ${st.etsy.signedIn ? "signed in" : "NOT signed in"}${st.releasedFromPreviousSession && st.releasedFromPreviousSession.length ? ` · released ${st.releasedFromPreviousSession.length} lock(s) from a previous session` : ""}`);
    if (!st.etsy.signedIn) toast("The Design Station is not signed in to Etsy — press Connect Etsy", "bad", 8000);
    startHeartbeat(); renderConsole();
    api("charmNestLibrary", { op: "bridgeLog", session: S_.nonce, rows: [], meta: { sorterClientId: S_.nonce, bench: st.bench, startedAt: Date.now(), version: st.version } }).catch(() => {});
    return st;
  }
  function call(type, args = {}, { timeoutMs = 120000, onProgress, quiet = false } = {}) {
    if (!S_.frame || !S_.frame.contentWindow) return Promise.reject(new Error(`${type}: the Design Station frame is not open`));
    if (!S_.nonce) S_.nonce = uid() + uid();
    const id = ++S_.id;
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => { S_.pending.delete(id); if (!quiet) { logLine("reply", type, { error: "timeout" }, timeoutMs, true); S_.errors++; } reject(new Error(`${type}: the Design Station did not answer in time`)); }, timeoutMs);
      S_.pending.set(id, { resolve, reject, t, onProgress, type, sent: performance.now(), quiet });
      if (!quiet) { logLine("cmd", type, args); S_.count++; }
      S_.frame.contentWindow.postMessage({ source: "brites-sorter", nonce: S_.nonce, id, type, args }, origin());
    });
  }
  function onMessage(ev) {
    if (ev.origin !== origin()) { S_.dropped++; return; }
    const d = ev.data; if (!d || d.source !== "brites-design") return;
    if (d.type === "etsy.connected") { onEtsyConnected(d); return; }
    if (d.nonce !== S_.nonce) { S_.dropped++; renderConsole(); return; }
    if (d.id === 0) { onEvent(d); return; }
    const p = S_.pending.get(d.id); if (!p) return;
    if (d.type === "progress") { p.onProgress && p.onProgress(d); if (d.text) agentLive(`DS · ${p.type}`, d.text, d.done, d.total); return; }
    if (d.type === "ack") { p.acked = performance.now(); return; }
    clearTimeout(p.t); S_.pending.delete(d.id); if (!p.quiet) S_.replies++;
    if (!p.quiet) logLine("reply", p.type, d.type === "done" ? d.result : { error: d.error }, performance.now() - p.sent, d.type !== "done");
    if (d.type === "done") p.resolve(d.result); else { if (!p.quiet) S_.errors++; p.reject(new Error(d.error || `${p.type} failed`)); }
  }
  /* ── Connect Etsy from the sorter. Etsy refuses to load inside a frame, so the station is opened in its own popup on
        its origin (a click is needed — browsers block popups otherwise); the popup signs in, the token lands in the
        station origin's storage, the framed copy sees it, the popup reports back and closes, and a run that stopped for
        the sign-in resumes on its own. ── */
  let connectWin = null;
  async function connectEtsy() {
    if (!S_.frame) { toast("Open the Design Station tab first", "bad"); return null; }
    if (!S_.control) { try { await open(); } catch (e) { toast("Could not open the session: " + e.message, "bad", 6000); return null; } }
    const r = await call("etsy.connect", {}, { timeoutMs: 15000 });
    if (r.signedIn) { toast("The Design Station is already signed in to Etsy", "ok"); return r; }
    connectWin = window.open(r.url, "britesEtsyConnect", "popup,width=640,height=780");
    if (!connectWin) { toast("The browser blocked the sign-in window — allow popups for this site and press Connect Etsy again", "bad", 9000); agent({ bridge: true }, "warn", "Connect Etsy: popup blocked"); return r; }
    agent({ bridge: true }, "DS", `Connect Etsy: the station opened in its own window (${r.url}) — sign in there; the sorter carries on when it reports back`);
    renderConsole();
    return r;
  }
  async function onEtsyConnected(d) {
    agent({ bridge: true }, "DS", `Etsy connected at the station${d.etsy && d.etsy.expiresAt ? ` · token to ${new Date(d.etsy.expiresAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}` : ""}`);
    toast("Design Station signed in to Etsy", "ok");
    connectWin = null;
    if (S_.control) { try { const st = await open(); if (st.etsy && st.etsy.signedIn && B.run && B.run.status === "stopped" && /sign/i.test(B.run.stoppedBy || "")) { agent({ run: B.run.runId }, "DS", "Resuming the run now that the station is signed in"); RunCtl.resume(); } } catch (e) { agent({ bridge: true }, "warn", `re-hello after the sign-in failed: ${e.message}`); } }
    renderConsole();
  }
  let liveEv = null;
  function agentLive(label, text, done, total) {
    const html = `<b>${esc(label)}</b> ${esc(text)}${total ? ` <i>${done}/${total}</i>` : ""}`;
    if (!liveEv || !liveEv.live) liveEv = agent({ bridge: true }, "DS", text, { live: true, html });
    else agentUpdate(liveEv, { html, text });
    if (total && done >= total) { agentUpdate(liveEv, { live: false }); liveEv = null; }
  }
  function onEvent(d) {
    logLine("evt", d.type, d);
    if (d.type === "activity") agent({ bridge: true }, "DS", d.text, { fromStation: true });
    else if (d.type === "selection") S_.state = Object.assign({}, S_.state, { selection: d.selected });
    else if (d.type === "filters") S_.state = Object.assign({}, S_.state, { filters: d.filters });
    else if (d.type === "orders.changed") { Orders.markStale(); agent({ bridge: true }, "DS", `Station refreshed its order list: ${d.open} open`); }
    else if (d.type === "lock.changed") agent({ bridge: true }, "DS", `Locks changed at the station: ${Object.keys(d.locks || {}).length} locked elsewhere, ${Object.keys(d.claims || {}).length} claimed`);
    else if (d.type === "complete.done") agent({ bridge: true }, "DS", `Station completed ${(d.completed || []).length} order(s)${d.setId ? ` for ${d.setId}` : ""}`);
    else if (d.type === "ui.modal") agent({ bridge: true }, "DS", d.open ? `Station opened item ${d.transactionId} of order ${d.receiptId}` : "Station closed its dialog");
    else if (d.type === "error") agent({ bridge: true }, "warn", `Station reported: ${d.command} — ${d.error}`);
    else if (d.type === "etsy.alarm") { etsyReadout(d); }
    else if (d.type === "etsy.connected") { onEtsyConnected(d); }
    else if (d.type === "etsy.signin") { agent({ bridge: true }, "warn", "The station needs an Etsy sign-in — press Connect Etsy"); toast("Design Station: press Connect Etsy on the run banner or the Design Station tab", "bad", 8000); if (B.run) RunCtl.stop("the Design Station is not signed in to Etsy", "Press Connect Etsy: the station opens in its own window to sign in, then the run resumes by itself."); }
    else if (d.type === "state" && d.ended) { S_.up = false; S_.control = false; stopHeartbeat(); S_.veil && S_.veil.classList.remove("hidden"); agent({ bridge: true }, "warn", `Station ended the session: ${d.reason}`); if (B.run && B.run.status === "running") RunCtl.stop(`the Design Station ended the session (${d.reason})`, "Press Take control, then Resume."); }
    renderConsole();
  }
  function slimForLog(payload) {
    if (payload == null) return null;
    const o = {};
    if (Array.isArray(payload.receiptIds)) o.receiptIds = payload.receiptIds.slice(0, 40);
    for (const k of ["receiptId", "transactionId", "total", "hydrated", "runId", "on", "error", "text", "count", "open", "reason", "command"]) if (payload[k] != null) o[k] = typeof payload[k] === "string" ? payload[k].slice(0, 120) : payload[k];
    if (Array.isArray(payload.orders)) o.orders = payload.orders.length;
    if (Array.isArray(payload.selected)) o.selected = payload.selected.length;
    if (Array.isArray(payload.completed)) o.completed = payload.completed.length;
    if (Array.isArray(payload.refused)) o.refused = payload.refused.length;
    if (Array.isArray(payload.jobs)) o.jobs = payload.jobs.length;
    if (payload.labels && payload.labels.files) o.labels = payload.labels.files.length;
    if (payload.counts) o.counts = payload.counts;
    return o;
  }
  function logLine(dir, type, payload, ms, err) {
    const row = { t: Date.now(), dir, type, ms: ms == null ? null : Math.round(ms), payload: slimForLog(payload), err: !!err };
    S_.log.push(row); if (S_.log.length > 400) S_.log.shift();
    S_.logBuf.push(row);
    if (!S_.flushT) S_.flushT = setTimeout(flushLog, 2500);
    renderConsole();
  }
  function flushLog() { S_.flushT = null; const rows = S_.logBuf.splice(0, 200); if (!rows.length || !S.cloud.ok || !S_.nonce) return; api("charmNestLibrary", { op: "bridgeLog", session: S_.nonce, rows, meta: { dropped: S_.dropped } }).catch(() => {}); if (S_.logBuf.length) S_.flushT = setTimeout(flushLog, 2500); }
  function startHeartbeat() {
    stopHeartbeat();
    const every = Math.max(2, +S.settings.heartbeatS || 5) * 1000;
    S_.hb = setInterval(async () => {
      if (!S_.control) return;
      try { const pong = await call("ping", {}, { timeoutMs: Math.max(1500, every - 500), quiet: true }); if (pong && pong.etsy) etsyReadout(pong.etsy); if (!S_.up) { S_.up = true; agent({ bridge: true }, "DS", "Design Station is back"); } S_.misses = 0; }
      catch (_) { S_.misses++; if (S_.misses >= (+S.settings.heartbeatMiss || 3) && S_.up) { S_.up = false; agent({ bridge: true }, "warn", `Design Station down — ${S_.misses} heartbeats missed`); if (B.run && B.run.status === "running") RunCtl.stop("the Design Station stopped answering the heartbeat", "Check the Design Station tab; when it answers again, press Resume."); } }
      renderConsole(); Dock.schedule();
    }, every);
  }
  function stopHeartbeat() { if (S_.hb) clearInterval(S_.hb); S_.hb = null; }
  /* ── the Etsy meter and budget: every reply that cost Etsy calls says so; the sorter keeps a rolling hour of them and
        refuses to start an Etsy-touching step past the cap (Settings → Etsy calls per hour). The station's own brakes
        (250 ms pacing, 429 backoff, the detail cache, the sweep cooldown) still apply underneath. ── */
  const E_ = { window: [], sessionTotal: 0, stationTotal: 0, station: null, alarmed: null };
  /** The station's ledger is the truth (only the station calls Etsy): every ping and every Etsy-touching reply carries it. */
  function etsyReadout(m) {
    if (!m) return; E_.station = m; E_.stationTotal = m.total;
    const el = document.getElementById("etsyPill"), n = document.getElementById("etsyPillN"); if (!el || !n) return;
    n.textContent = `${S_.state && S_.state.sandbox ? "emulated · " : ""}today ${m.today} · 10m ${m.last10Min} · 1m ${m.lastMinute}`;
    const hot = m.last10Min >= m.guard.per10Min * 0.7 || m.lastMinute >= m.guard.burstPerMinute * 0.7;
    el.classList.toggle("alarm", !!m.braked); el.classList.toggle("warn", !m.braked && hot);
    el.title = m.braked ? `Etsy watchdog at the station: ${m.alarm && m.alarm.why} — automatic Etsy work paused until ${new Date(m.brakeUntil).toLocaleTimeString()}` : `Etsy calls counted by the Design Station: ${m.total} this session · ${m.lastMinute} in the last minute · ${m.last10Min} in the current 10-minute window · ${m.lastHour} in the last hour · ${m.today} today · peak ${m.maxQps}/s · ${m.status429} rate-limit answers. Guard: ${m.guard.burstPerMinute}/min, ${m.guard.per10Min}/10 min, ${m.guard.sameOrderPer10Min} reads of one order/10 min.`;
    if (m.braked && (!E_.alarmed || E_.alarmed !== m.alarm.at)) { E_.alarmed = m.alarm.at; agent({ bridge: true }, "warn", `Etsy watchdog at the station: ${m.alarm.why} — automatic Etsy work is paused for ${Math.round(m.guard.brakeMs / 60000)} min`); if (B.run && ["running", "paused", "review"].includes(B.run.status)) RunCtl.stop(`Etsy watchdog: ${m.alarm.why}`, `The station paused automatic Etsy work until ${new Date(m.brakeUntil).toLocaleTimeString()}. Check the API meter on both apps, then Resume.`); }
  }
  function meter(reply, what) {
    const n = reply && Number(reply.etsyCalls) || 0;
    if (reply && reply.etsy && reply.etsy.meter) etsyReadout(reply.etsy.meter);
    if (reply && reply.etsy && reply.etsy.calls != null) E_.stationTotal = reply.etsy.calls;
    if (n > 0) { E_.window.push({ t: Date.now(), n }); E_.sessionTotal += n; }
    const cut = Date.now() - 3600000; while (E_.window.length && E_.window[0].t < cut) E_.window.shift();
    agent({ bridge: true }, n > 12 ? "warn" : "DS", `Etsy: ${n} call${n === 1 ? "" : "s"} for ${what}${(reply && reply.refreshSkipped) || (reply && reply.swept === false) ? " (open list reused — swept under 90 s ago)" : ""} · ${hourCalls()} this hour · ${E_.stationTotal} this station session`);
    renderConsole();
    return n;
  }
  const hourCalls = () => E_.window.reduce((a, x) => a + x.n, 0);
  const etsyCap = () => Math.max(50, +S.settings.etsyHourlyCap || 600);
  /** Before an Etsy-touching step: false (and a stopped run) when the hour's budget is spent; a warning past 70 %. */
  function etsyBudgetOk(step) {
    const used = hourCalls(), cap = etsyCap();
    if (used >= cap) { const why = `Etsy call budget reached (${used} of ${cap} this hour) before ${step}`; agent({ bridge: true }, "warn", why); toast(why, "bad", 8000); if (B.run && B.run.status === "running") RunCtl.stop(why, `Wait for the hour to roll over or raise the cap in Settings, then Resume.`); return false; }
    if (used >= cap * 0.7) agent({ bridge: true }, "warn", `Etsy calls at ${used} of ${cap} this hour — ${step} goes ahead, the run stops at the cap`);
    return true;
  }
  /* ── the sorter's side of the story, streamed to the station's banner (design §5.7): every agent line and sheet log
        line while in control, coalesced into one quiet post every half second; the station shows the last four. ── */
  const feedQ = []; let feedT = null;
  function feed(ev) {
    if (!S_.control || !S_.frame || ev.fromStation) return;
    const text = String(ev.text || (ev.html ? ev.html.replace(/<[^>]+>/g, "") : "")).trim(); if (!text) return;
    const last = feedQ[feedQ.length - 1]; if (last && last.text === text) return;
    feedQ.push({ t: ev.t || Date.now(), kind: String(ev.kind || ""), text: text.slice(0, 220) }); if (feedQ.length > 12) feedQ.shift();
    if (!feedT) feedT = setTimeout(flushFeed, 450);
  }
  function flushFeed() { feedT = null; const rows = feedQ.splice(0, 12).slice(-6); if (!rows.length || !S_.control) return; call("feed.post", { rows }, { timeoutMs: 4000, quiet: true }).catch(() => {}); }
  async function release() { S_.control = false; stopHeartbeat(); try { await call("release", {}, { timeoutMs: 5000 }); } catch (_) {} S_.up = false; S_.veil && S_.veil.classList.remove("hidden"); Dock.layout(); renderConsole(); }
  function ensure() { if (S_.control && S_.up) return Promise.resolve(S_.state); if (!S_.frame) mount(document.querySelector("#designView .dsFrameHost") || Views.designHost()); return open(); }
  function renderConsole() {
    const host = document.getElementById("dsConsole"); if (!host) return;
    const st = S_.state || {};
    const kv = host.querySelector(".kv"); if (kv) kv.innerHTML = `<b>${S_.control ? (S_.up ? "Connected" : "Link down") : "Not in control"}</b> · session <span class="mono">${S_.nonce ? S_.nonce.slice(0, 6) : "—"}</span><br>${S_.count} commands · ${S_.replies} replies · ${S_.errors} errors · ${S_.dropped} dropped · ${S_.pending.size} pending<br>${st.bench ? `bench ${esc(st.bench)} · ${esc(st.version || "")} · employee ${esc(st.employee || "—")}` : ""}<br>${st.etsy ? `Etsy: ${st.etsy.signedIn ? "signed in" + (st.etsy.expiresAt ? ` · token to ${new Date(st.etsy.expiresAt).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}` : "") : "<span style='color:#8a3a26'>NOT signed in</span>"}` : ""}<br>${st.counts ? `${st.counts.open} open · ${st.counts.selected} selected · ${st.counts.claimed} claimed · ${st.counts.locked} locked elsewhere` : ""}<br>Etsy calls: <b>${hourCalls()}</b> this hour of ${etsyCap()} · ${E_.sessionTotal} by this sorter · ${E_.stationTotal} on the station`;
    const cb = host.querySelector("#dsConnectEtsy"); if (cb) { const signed = !!(st.etsy && st.etsy.signedIn); cb.classList.toggle("gold", !signed); cb.classList.toggle("ghost", signed); cb.textContent = signed ? "Etsy signed in ✓" : "Connect Etsy"; }
    const sw = host.querySelector("#dsControl"); if (sw) { sw.classList.toggle("on", S_.control); sw.textContent = S_.control ? "Release" : "Take control"; }
    const log = host.querySelector(".log"); if (log) { const rows = S_.log.slice(-200); log.innerHTML = rows.map(r => `<div class="row ${r.dir}${r.err ? " err" : ""}"><span class="d">${fmtT(r.t)}</span><span class="ty">${r.dir === "cmd" ? "→" : r.dir === "reply" ? "←" : "·"} ${esc(r.type)}</span><span class="pl">${esc(r.payload ? JSON.stringify(r.payload) : "")}</span><span class="ms">${r.ms != null ? r.ms + "ms" : ""}</span></div>`).join(""); log.scrollTop = log.scrollHeight; }
    const tb = document.getElementById("tabDesignN"); if (tb) tb.textContent = S_.control && !S_.up ? "!" : "";
  }
  return { mount, open, call, release, ensure, feed, meter, etsyBudgetOk, etsyReadout, connectEtsy, etsy: () => ({ hour: hourCalls(), cap: etsyCap(), session: E_.sessionTotal, station: E_.stationTotal, meter: E_.station }), state: () => S_.state, log: S_.log, up: () => S_.up, inControl: () => S_.control, nonce: () => S_.nonce, renderConsole, flushLog, origin, frameUrl, _S: S_, _E: E_ };
})();

/* ── the dock: where the station frame is shown. "full" over the Design Station tab's placeholder, "pip" as a live panel
   in the corner of every other tab while the sorter is in control, hidden otherwise. The station is always rendered at a
   desktop width and scaled to fit, so its order rail, tiles and dialogs look as they do on its own screen. ── */
const Dock = window.Dock = (() => {
  const D = { el: null, body: null, bar: null, host: null, mode: "hidden", hiddenByUser: false, shownByUser: false, pill: null, virtualW: 1200, ro: null, raf: 0 };
  function ensure() {
    if (D.el) return D;
    const el = document.createElement("div"); el.id = "dsDock"; el.className = "hidden";
    el.innerHTML = `<div class="dockBar"><span class="dot"></span><b>Design Station</b><span class="st" id="dockState">live</span><span class="spacer"></span><button type="button" class="dockBtn" id="dockOpen" title="Open the Design Station tab">Open ⤢</button><button type="button" class="dockBtn" id="dockHide" title="Hide the live view">Hide</button></div><div class="dockBody"></div>`;
    document.body.appendChild(el);
    D.el = el; D.body = el.querySelector(".dockBody"); D.bar = el.querySelector(".dockBar");
    el.querySelector("#dockOpen").onclick = () => setMode("design");
    el.querySelector("#dockHide").onclick = () => { D.hiddenByUser = true; layout(); };
    D.bar.addEventListener("dblclick", () => setMode("design"));
    const pill = document.createElement("button"); pill.type = "button"; pill.id = "dsDockPill"; pill.className = "hidden"; pill.innerHTML = `<span class="dot"></span>Design Station live view`; pill.onclick = () => { D.hiddenByUser = false; D.shownByUser = true; layout(); };
    document.body.appendChild(pill); D.pill = pill;
    window.addEventListener("resize", schedule); document.addEventListener("scroll", schedule, true);
    return D;
  }
  function setHost(host) { D.host = host; if (D.ro) D.ro.disconnect(); if (host && window.ResizeObserver) { D.ro = new ResizeObserver(schedule); D.ro.observe(host); } schedule(); }
  function schedule() { if (D.raf) return; D.raf = requestAnimationFrame(() => { D.raf = 0; layout(); }); }
  /** Which mode applies now: the Design Station tab shows the frame full size; any other tab shows the panel while the link is in control (or a run is on). */
  function wanted() {
    if (!D.el || !document.getElementById("dsFrame")) return "hidden";
    if (S.mode === "design") return "full";
    const live = DesignLink.inControl() || (B.run && ["running", "review", "paused"].includes(B.run.status));
    return live ? (D.hiddenByUser ? "pilled" : "pip") : "hidden";
  }
  function layout() {
    if (!D.el) return;
    const mode = wanted(); D.mode = mode;
    const f = document.getElementById("dsFrame");
    D.pill.classList.toggle("hidden", mode !== "pilled");
    D.el.classList.toggle("hidden", mode === "hidden" || mode === "pilled");
    D.el.classList.toggle("full", mode === "full"); D.el.classList.toggle("pip", mode === "pip");
    // on the engraving and review screens the controls live at the bottom of the page, so the live view tucks itself
    // into a smaller corner there rather than sitting on top of them
    D.el.classList.toggle("tucked", mode === "pip" && ["engrave", "review"].includes(S.mode) && !D.shownByUser);
    if (mode === "hidden" || mode === "pilled" || !f) return;
    let w, h;
    if (mode === "full") {
      const host = D.host; if (!host) return; const r = host.getBoundingClientRect();
      D.el.style.left = r.left + "px"; D.el.style.top = r.top + "px"; D.el.style.width = r.width + "px"; D.el.style.height = r.height + "px"; D.el.style.right = ""; D.el.style.bottom = "";
      w = r.width; h = r.height;
    } else {
      D.el.style.left = ""; D.el.style.top = ""; D.el.style.width = ""; D.el.style.height = ""; D.el.style.right = "16px"; D.el.style.bottom = "16px";
      const r = D.body.getBoundingClientRect(); w = r.width; h = r.height;
    }
    // the station renders at a desktop width and is scaled to the dock; the veil and cursor scale with it
    const vw = Math.max(D.virtualW, Math.round(w)); const k = w / vw;
    f.style.width = vw + "px"; f.style.height = Math.round(h / k) + "px"; f.style.transform = `scale(${k})`;
    const st = D.el.querySelector("#dockState"); if (st) st.textContent = DesignLink.inControl() ? (DesignLink.up() ? (B.run ? `run · ${B.run.step}` : "live") : "link down") : "not in control";
    D.el.classList.toggle("down", DesignLink.inControl() && !DesignLink.up());
  }
  return { ensure, setHost, layout, schedule, mode: () => D.mode, _D: D };
})();

/* ── the tabs' hosts ── */
const Views = window.Views = (() => {
  function designHost() {
    const v = document.getElementById("designView"); if (v.dataset.built) return v.querySelector(".dsFrameHost");
    v.dataset.built = "1";
    v.innerHTML = `<div class="dsFrameHost"></div>
      <div class="dsConsole" id="dsConsole">
        <div class="card"><div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap"><button class="switchBtn" id="dsControl" type="button">Take control</button><a class="btn ghost xs" id="dsOpenTab" target="_blank" rel="noopener">Open station in a tab</a><button class="btn gold xs" id="dsConnectEtsy" type="button" title="Signs the station in to Etsy in its own window; the framed station picks the token up">Connect Etsy</button><button class="btn ghost xs" id="dsReload" type="button">Reload frame</button></div><div class="kv">Not in control</div></div>
        <div class="card" style="gap:4px"><div class="section" style="margin:0 0 4px">Employee</div><div style="display:flex;gap:6px;align-items:center"><span id="dsEmployee" class="mono">${esc(employeeName() || "— not set —")}</span><button class="btn ghost xs" id="dsSetEmployee" type="button">Change</button></div><div class="help" style="font-size:11px;color:var(--ink45)">Recorded with every approval. Any employee may approve.</div></div>
        <div class="log"></div>
      </div>`;
    // The fold lives in the panel's own column, never over the station, and the column keeps a strip of itself when
    // folded so the way back is always on screen.
    { const console_ = v.querySelector("#dsConsole");
      const btn = document.createElement("button"); btn.type = "button"; btn.className = "dsFold";
      const paint = () => { const on = v.classList.contains("dsWide"); btn.textContent = on ? "‹ Panel" : "Hide panel ›"; btn.title = on ? "Bring the controls back" : "Fold the controls away and give the station the whole tab"; btn.setAttribute("aria-expanded", on ? "false" : "true"); };
      btn.onclick = () => { const on = !v.classList.contains("dsWide"); v.classList.toggle("dsWide", on); localStorage.setItem("cn.dsWide", on ? "1" : "0"); paint(); Dock.schedule(); };
      v.classList.toggle("dsWide", localStorage.getItem("cn.dsWide") === "1");
      paint(); console_.insertBefore(btn, console_.firstChild); }
    const host = v.querySelector(".dsFrameHost");
    // the tab is a mirror of the station being driven, not a second station: same page, following the same pointer
    v.querySelector("#dsOpenTab").href = DesignLink.frameUrl() + (DesignLink.frameUrl().includes("?") ? "&" : "?") + "mirror=1";
    v.querySelector("#dsOpenTab").textContent = "Watch full screen in a tab";
    v.querySelector("#dsOpenTab").title = "Opens the same station in a tab that follows this one, pointer and all";
    v.querySelector("#dsControl").onclick = async () => { if (DesignLink.inControl()) await DesignLink.release(); else { try { await DesignLink.ensure(); } catch (e) { toast("Could not open the session: " + e.message, "bad", 6000); } } DesignLink.renderConsole(); };
    v.querySelector("#dsConnectEtsy").onclick = () => DesignLink.connectEtsy().catch(e => toast(e.message, "bad", 6000));
    v.querySelector("#dsReload").onclick = () => { const f = document.getElementById("dsFrame"); if (f) f.src = DesignLink.frameUrl(); };
    v.querySelector("#dsSetEmployee").onclick = () => { askEmployee(); document.getElementById("dsEmployee").textContent = employeeName() || "— not set —"; };
    return host;
  }
  function onShow(mode) {
    if (mode === "design") { const host = designHost(); if (!document.getElementById("dsFrame")) DesignLink.mount(host); else Dock.setHost(host); DesignLink.renderConsole(); }
    Dock.schedule();
    if (mode === "orders") Orders.render();
    if (mode === "master") Master.render();
    if (mode === "engrave") Engrave.render();
    if (mode === "review") Review.render();
  }
  return { designHost, onShow };
})();

/* ═══ 18 · Orders — pulled through the station, read deterministically ══════ */
const Orders = window.Orders = (() => {
  const rowsOf = () => B.orders.rows;
  function markStale() { B.orders.stale = true; render(); }
  async function loadMaps(force) {
    if (!S.cloud.ok) return;
    if (!force && Date.now() - B.maps.loadedAt < 60000) return;
    const [om, al, nd] = await Promise.all([api("charmNestLibrary", { op: "optionMapGet" }), api("charmNestLibrary", { op: "aliasGet" }), api("charmNestLibrary", { op: "noDesignGet" })]);
    B.maps.optionMaps = om.maps || {}; B.maps.aliases = al.aliases || {}; B.maps.noDesign = nd.list || { patterns: [], skus: [], rows: [] }; B.maps.loadedAt = Date.now();
  }
  const ctx = () => ({ optionMaps: B.maps.optionMaps, aliases: B.maps.aliases, noDesign: B.maps.noDesign, masterEntry: sku => Master.entryFor(sku) });
  /** The pull rule (Settings → Pull orders): every open order, those due by a date, or the N most urgent by ship-by date. */
  function applyPullRule(orders) {
    const mode = S.settings.pullMode || "all";
    let list = orders.slice().sort((a, b) => (a.shipBy || 9e12) - (b.shipBy || 9e12));
    if (mode === "dueBy" && S.settings.pullDueBy) { const [y, m, d] = S.settings.pullDueBy.split("-").map(Number); const end = new Date(y, m - 1, d, 23, 59, 59).getTime() / 1000; list = list.filter(o => o.shipBy && o.shipBy <= end); }
    if (mode === "count") list = list.slice(0, Math.max(1, +S.settings.pullCount || 40));
    return list;
  }
  function interpretAll() {
    for (const row of rowsOf()) { if (row.state === "gone") continue; row.spec = O.interpretLine(row.order, row.line, ctx()); row.problems = row.spec.problems.slice(); if (row.spec.noDesign) row.state = row.state === "pulled" ? "noDesign" : row.state; if (row.materialOverride) { row.spec.material = row.materialOverride; row.problems = row.problems.filter(p => p.kind !== "needsMaterial"); } if (row.sizeOverride) { row.spec.size = row.sizeOverride; row.problems = row.problems.filter(p => p.kind !== "missingSize"); } row.material = row.spec.material; }
    Review.syncOrderItems();
  }
  async function pull(run, { silent = false } = {}) {
    await DesignLink.ensure();
    if (!DesignLink.etsyBudgetOk("the pull")) throw new Error("Etsy call budget reached — the pull was not started");
    await Promise.all([loadMaps(), Master.load()]);
    const pullBar = window.CNProgress ? CNProgress.start("Pulling orders from Etsy") : null;
    try {
    const r = await DesignLink.call("orders.snapshot", { hydrate: true, refresh: true }, { timeoutMs: 20 * 60 * 1000, onProgress: p => { if (pullBar) { if (p.done != null && p.total) pullBar.set(p.done, p.total, p.text || ""); else if (p.text) pullBar.note(p.text); } if (p.text) agentLiveLine("Pulling orders", p.text, p.done, p.total); } });
    DesignLink.meter(r, "the pull");
    B.orders.snapshot = { total: r.total, hydrated: r.hydrated, etsy: r.etsy, at: Date.now() };
    if (r.hydrated < r.total) throw new Error(`only ${r.hydrated} of ${r.total} orders could be read from Etsy — ${r.etsy && !r.etsy.signedIn ? "the station is not signed in: press Connect Etsy" : "check the Design Station and pull again"}`);
    const picked = applyPullRule(r.orders);
    B.orders.filtered = r.orders.length - picked.length;
    B.orders.rows = picked.flatMap(o => o.lines.map(l => ({ key: O.lineKey(o, l), order: o, line: l, spec: null, problems: [], state: "pulled", reason: null, claimedBy: null, poolIds: [], engrave: null, metal: null })));
    B.orders.byKey = new Map(B.orders.rows.map(r => [r.key, r]));
    interpretAll();
    B.orders.pulledAt = Date.now(); B.orders.stale = false;
    if (run) { run.lines = Object.fromEntries(B.orders.rows.map(lineRecord)); run.orders = picked.map(o => o.receiptId); run.step = "pull"; await RunCtl.save(run); }
    const held = B.orders.rows.filter(x => x.problems.length).length;
    agent({ bridge: true }, "DS", `Pulled ${picked.length} order(s), ${B.orders.rows.length} line(s)${B.orders.filtered ? ` (${B.orders.filtered} more open orders left out by the pull rule)` : ""} · ${held} line(s) need a decision`);
    if (!silent) toast(`${picked.length} orders · ${B.orders.rows.length} lines pulled from the Design Station`, "ok");
    render();
    return B.orders.rows;
    } finally { if (pullBar) pullBar.end(); }
  }
  let liveEv = null;
  function agentLiveLine(label, text, done, total) { const html = `<b>${esc(label)}</b> ${esc(text)}${total ? ` <i>${done}/${total}</i>` : ""}`; if (!liveEv || !liveEv.live) liveEv = agent({ bridge: true }, "DS", text, { live: true, html }); else agentUpdate(liveEv, { html, text }); if (total && done >= total) { agentUpdate(liveEv, { live: false }); liveEv = null; } }
  function lineRecord(row) { return [row.key, { state: row.state, poolIds: row.poolIds, reason: row.reason, sku: row.spec && row.spec.designSku, material: row.material, quantity: row.spec ? row.spec.quantity : 1, engrave: row.engrave ? { needed: !!row.engrave.needed, state: row.engrave.state, approved: !!row.engrave.approved, text: row.engrave.text || null } : null, problems: (row.problems || []).map(p => p.kind), updateTs: row.order.updateTs, orderId: row.order.receiptId, transactionId: row.line.transactionId }]; }
  async function claim(ids) { if (!ids.length) return; const r = await DesignLink.call("claim", { receiptIds: ids, runId: B.run && B.run.runId }); for (const row of rowsOf()) if (r.claimed.includes(row.order.receiptId)) row.claimedBy = "sorter"; agent({ bridge: true }, "DS", `Claimed ${r.claimed.length} order(s) on the station (gold dot)`); render(); }
  async function unclaim(ids) { if (!ids.length) return; try { await DesignLink.call("unclaim", { receiptIds: ids }); } catch (e) { agent({ bridge: true }, "warn", `unclaim: ${e.message}`); } for (const row of rowsOf()) if (ids.includes(row.order.receiptId)) row.claimedBy = null; render(); }
  /** §10.3: every order's update_timestamp re-read through the station; changed → re-interpret; vanished → dropped. */
  async function revalidate(run, why) {
    const orders = [...new Set(rowsOf().map(r => r.order.receiptId))];
    const changed = [], gone = [];
    if (!DesignLink.etsyBudgetOk(`re-validation ${why}`)) return { changed, gone, skipped: true };
    // one paged list sweep tells which orders Etsy touched or closed since the pull; only those get a fresh detail read
    let chk = null;
    try { chk = await DesignLink.call("orders.check", { receiptIds: orders }, { timeoutMs: 10 * 60 * 1000, onProgress: p => { if (p.text) agentLiveLine("Re-validating orders", p.text, p.done, p.total); } }); DesignLink.meter(chk, `the open-list check ${why}`); }
    catch (e) { agent({ bridge: true }, "warn", `open-list check failed (${e.message}) — orders are taken as unchanged`); return { changed, gone, failed: true }; }
    const need = orders.filter(rid => { const c = chk.orders[rid]; const cur = rowsOf().find(r => r.order.receiptId === rid); return !c || !c.open || c.touched || (cur && c.updateTs && c.updateTs !== cur.order.updateTs); });
    agent({ bridge: true }, "DS", `Re-validation ${why}: ${orders.length} order(s) checked against the open list${chk.swept ? "" : " (list reused)"} · ${need.length} need a fresh read`);
    let done = 0;
    for (const rid of need) {
      let d = null;
      try { d = await DesignLink.call("orders.detail", { receiptId: rid, fresh: true }, { timeoutMs: 60000 }); DesignLink.meter(d, `re-reading ${rid}`); } catch (e) { agent({ bridge: true }, "warn", `re-validation of ${rid} failed: ${e.message}`); continue; }
      agentLiveLine("Re-validating orders", `order ${rid}`, ++done, need.length);
      const rows = rowsOf().filter(r => r.order.receiptId === rid);
      if (!d || !d.order || d.gone) { gone.push(rid); for (const r of rows) { r.state = "gone"; r.reason = d && d.reason ? d.reason : (d && d.isShipped ? "shipped" : d && d.status ? d.status : "no longer open"); } continue; }
      const before = rows[0].order.updateTs;
      if (d.order.updateTs !== before) {
        changed.push(rid);
        for (const r of rows) {
          const nl = d.order.lines.find(l => l.transactionId === r.line.transactionId);
          const oldText = r.engrave && r.engrave.text; const oldSpec = r.spec;
          r.order = d.order; if (nl) r.line = nl; else { r.state = "gone"; r.reason = "line vanished from the order"; continue; }
          r.spec = O.interpretLine(r.order, r.line, ctx()); r.problems = r.spec.problems.slice(); r.material = r.spec.material;
          const textInputsChanged = JSON.stringify([oldSpec && oldSpec.personalization, oldSpec && oldSpec.buyerMessage, oldSpec && oldSpec.staffNote]) !== JSON.stringify([r.spec.personalization, r.spec.buyerMessage, r.spec.staffNote]);
          const materialChanged = oldSpec && oldSpec.material !== r.spec.material, skuChanged = oldSpec && oldSpec.designSku !== r.spec.designSku;
          Review.add({ kind: "orderChanged", key: "chg:" + r.key, row: r, old: { text: oldText, spec: oldSpec }, why: `Etsy updated order ${rid} after the pull (${why})${materialChanged ? " · material changed" : ""}${skuChanged ? " · SKU changed" : ""}${textInputsChanged ? " · the customer's words changed" : ""}` });
          if (textInputsChanged || materialChanged || skuChanged) { if (r.engrave) { r.engrave.invalidatedBy = "order changed"; r.engrave.approved = false; r.engrave.state = "reclassify"; } Engrave.invalidate(r, "order changed"); }
        }
      }
    }
    if (run) { run.lines = Object.fromEntries(rowsOf().map(lineRecord)); run.revalidatedAt = Date.now(); await RunCtl.save(run); }
    agent({ bridge: true }, "DS", `Re-validated ${orders.length} order(s) ${why}: ${changed.length} changed, ${gone.length} gone`);
    Review.syncOrderItems(); render();
    return { changed, gone };
  }
  const STATE_PILL = { pulled: ["neutral", "pulled"], noDesign: ["info", "no design"], pooled: ["info", "pooled"], nested: ["ok", "nested"], written: ["ok", "written"], labelled: ["ok", "labelled"], committed: ["ok", "complete"], unmatched: ["bad", "unmatched"], held: ["bad", "held"], contended: ["warn", "other run"], skipped: ["warn", "skipped"], gone: ["bad", "gone"], oversize: ["bad", "oversize"] };
  function engravePill(r) { const e = r.engrave; if (!e) return r.spec && r.spec.engraveCandidate ? ["warn", "words?"] : ["neutral", "—"]; if (!e.needed) return ["neutral", e.state === "skipped" ? "skipped" : "no engraving"]; if (e.approved) return ["ok", "approved"]; if (e.state === "words") return ["warn", "words"]; if (e.state === "review") return ["warn", "review"]; if (e.state === "fitted") return ["info", "fitted"]; if (e.state === "blocked") return ["bad", "blocked"]; return ["info", e.state || "engrave"]; }
  function render() {
    const v = document.getElementById("ordersView"); if (!v || v.classList.contains("hidden")) { const tb = document.getElementById("tabOrdersN"); if (tb) tb.textContent = B.orders.rows.length ? String(new Set(B.orders.rows.map(r => r.order.receiptId)).size) : ""; return; }
    const s = S.settings;
    const runBtns = B.run && B.run.status !== "complete" && B.run.status !== "stopped" ? `<button class="btn ghost sm" id="ordStop">Stop run</button>` : `<button class="btn gold sm" id="ordRun">Run set ▶</button><button class="btn ghost sm" id="ordResume">Resume run…</button>`;
    v.innerHTML = `<div class="ordBar">
        <button class="btn sm" id="ordPull">Pull orders</button>${runBtns}
        <select id="ordPullMode"><option value="all"${s.pullMode === "all" ? " selected" : ""}>Every open order</option><option value="dueBy"${s.pullMode === "dueBy" ? " selected" : ""}>Due by date</option><option value="count"${s.pullMode === "count" ? " selected" : ""}>N most urgent</option></select>
        <input type="date" id="ordDueBy" value="${esc(s.pullDueBy || "")}" title="Orders due by" class="${s.pullMode === "dueBy" ? "" : "hidden"}"><input type="number" id="ordCount" value="${s.pullCount || 40}" min="1" max="500" title="How many" class="${s.pullMode === "count" ? "" : "hidden"}">
        <span class="pill ${B.orders.stale ? "warn" : "neutral"}" id="ordMeta">${B.orders.pulledAt ? `pulled ${fmtT(B.orders.pulledAt)} · ${new Set(B.orders.rows.map(r => r.order.receiptId)).size} orders · ${B.orders.rows.length} lines${B.orders.filtered ? ` · ${B.orders.filtered} left out by the rule` : ""}${B.orders.stale ? " · station list changed since" : ""}` : "nothing pulled yet"}</span>
        <span class="spacer"></span><span class="pill info">${Review.count() ? Review.count() + " to decide in Review" : "nothing to decide"}</span>
      </div><div class="ordGroups" id="ordGroups"></div>`;
    v.querySelector("#ordPullMode").onchange = e => { S.settings.pullMode = e.target.value; saveSettings(); render(); };
    v.querySelector("#ordDueBy").onchange = e => { S.settings.pullDueBy = e.target.value; saveSettings(); };
    v.querySelector("#ordCount").onchange = e => { S.settings.pullCount = Math.max(1, +e.target.value || 40); saveSettings(); };
    Sandbox.mountPanel(v);
    v.querySelector("#ordPull").onclick = async () => { try { await pull(null); } catch (e) { toast(e.message, "bad", 7000); agent({ bridge: true }, "warn", e.message); } };
    const rb = v.querySelector("#ordRun"); if (rb) rb.onclick = () => RunCtl.start();
    const rs = v.querySelector("#ordResume"); if (rs) rs.onclick = () => RunCtl.pickResume();
    const st = v.querySelector("#ordStop"); if (st) st.onclick = () => RunCtl.stop("stopped by the operator", "Press Resume to carry on from the recorded step.");
    const groups = v.querySelector("#ordGroups");
    const byM = new Map();
    for (const r of rowsOf()) { const m = r.material || "none"; if (!byM.has(m)) byM.set(m, []); byM.get(m).push(r); }
    const order = ["gold", "silver", "rose", "gold10k", "gold14k", "none"];
    for (const m of order) {
      const rows = byM.get(m); if (!rows || !rows.length) continue;
      const g = el("div", "ordGroup"); g.dataset.m = m;
      g.innerHTML = `<div class="gh"><span class="sw" style="width:10px;height:10px;border-radius:3px;background:var(--accent)"></span>${m === "none" ? "No material yet" : esc(labelOf(m))}<span class="n">${rows.length} line(s) · ${new Set(rows.map(r => r.order.receiptId)).size} order(s)</span></div>`;
      for (const r of rows.sort((a, b) => (a.order.shipBy || 0) - (b.order.shipBy || 0))) {
        const sp = r.spec || {}; const [k, t] = STATE_PILL[r.state] || ["neutral", r.state]; const [ek, et] = engravePill(r);
        const held = r.problems.length || r.state === "held";
        const row = el("div", "ordRow" + (held ? " held" : "") + (r.state === "gone" ? " gone" : ""));
        const words = sp.personalization && sp.personalization.length ? `<i>pers</i>${esc(sp.personalization.join(" / "))} ` : ""; const bm = sp.buyerMessage ? `<i>msg</i>${esc(sp.buyerMessage)} ` : ""; const sn = sp.staffNote ? `<i>note</i>${esc(sp.staffNote)}` : "";
        row.innerHTML = `<span class="num">${esc(r.order.receiptId)}${r.order.attention ? '<span class="attn" title="personalisation or buyer message">!</span>' : ""}${r.claimedBy ? ' <span title="claimed on the station" style="color:var(--gold)">●</span>' : ""}</span>
          <span class="sku" title="${esc(r.line.title)}">${esc(sp.designSku || "— no SKU —")}${sp.form ? ` · ${esc(sp.form)}` : ""}${sp.size ? ` · ${esc(sp.size)}` : ""}</span>
          <span class="txt" title="${esc((sp.personalization || []).join(" / "))} ${esc(sp.buyerMessage || "")} ${esc(sp.staffNote || "")}">${words}${bm}${sn}${!words && !bm && !sn ? `<span style="color:var(--ink25)">${esc(r.line.title)}</span>` : ""}</span>
          <span class="qty">×${sp.quantity || r.line.quantity}</span>
          <span class="ship" title="ship by">${r.order.shipBy ? new Date(r.order.shipBy * 1000).toLocaleDateString("en-US", { month: "short", day: "2-digit" }) : "—"}</span>
          <span class="st ${ek}" title="engraving">${et}</span>
          <span class="st ${k}">${t}</span>
          <span class="ship" title="update_timestamp">${r.order.updateTs ? new Date(r.order.updateTs * 1000).toLocaleString("en-US", { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "—"}</span>`;
        if (held || r.reason) { const why = el("div", "why", `${r.reason ? esc(r.reason) : ""}${r.problems.map(p => `<span>${esc(Review.problemText(p))}</span>`).join(" · ")} <button class="btn ghost xs" type="button">Fix in Review</button>`); why.querySelector("button").onclick = () => { setMode("review"); Review.focus(r.key); }; row.appendChild(why); }
        g.appendChild(row);
      }
      groups.appendChild(g);
    }
    if (!rowsOf().length) groups.innerHTML = `<div class="libEmpty">No orders pulled. Press <b>Pull orders</b> — the Design Station reads Etsy and hands every open order over the bridge.</div>`;
    const tb = document.getElementById("tabOrdersN"); if (tb) tb.textContent = B.orders.rows.length ? String(new Set(B.orders.rows.map(r => r.order.receiptId)).size) : "";
  }
  return { pull, claim, unclaim, revalidate, render, markStale, loadMaps, interpretAll, lineRecord, rows: rowsOf, applyPullRule, ctx };
})();

/* ═══ 19 · Master — SKU labels under charms, per-SKU designs, the index ══════ */
const Master = window.Master = (() => {
  const entryFor = sku => B.master.entries.get(String(sku || "").toUpperCase()) || null;
  let showAll = false;                                              // the grid draws 600 tiles until asked for the rest
  let reindexAll = false;                                           // by default a SKU the library already holds is left alone
  /** A design drawn only in sizes keeps its picture and file under each size; the entry's own are empty. */
  const thumbOf = e => e.thumbUrl || ((Object.values(e.sizes || {}).find(s => s && s.thumbUrl) || {}).thumbUrl) || "";
  async function load(force) {
    if (!S.cloud.ok) return;
    if (!force && Date.now() - B.master.loadedAt < 120000) return B.master.loading || null;
    if (B.master.loading) return B.master.loading;
    B.master.loading = (async () => {
      render();
      const [ix, fl] = await Promise.all([api("charmNestLibrary", { op: "masterList", limit: 3000 }, { label: "Loading the charm library" }), api("charmNestLibrary", { op: "masterListFiles" })]);
      B.master.entries = new Map((ix.entries || []).map(e => [e.sku, e])); B.master.files = fl.files || []; B.master.loadedAt = Date.now();
    })().finally(() => { B.master.loading = null; render(); });
    return B.master.loading;
  }
  async function fetchEntry(sku) { sku = String(sku || "").toUpperCase(); if (!sku) return null; const r = await api("charmNestLibrary", { op: "masterGet", sku }); if (r.entry) B.master.entries.set(sku, r.entry); return r.entry; }
  const skuRegex = () => { try { return new RegExp(S.settings.skuPattern || DEFAULTS.skuPattern); } catch (_) { return P.SKU_PATTERN_DEFAULT; } };
  /** Render the strip under a charm (for the vision fallback) → PNG data URL. */
  /** The strip a label would occupy: the outline's width (widened 30 %), from its bottom edge down by the label gap. */
  function stripBox(c, gapPt) { const b = c.outline.bbox, w = b[2] - b[0]; return [b[0] - w * 0.3, b[1] - gapPt - 6, b[2] + w * 0.3, b[1] + 2]; }
  /** Which of these charms have something drawn under them that belongs to no charm — outlined label text, most likely.
      Everything every charm owns is collected once, so a sheet of thousands is a pass over what is left, not over all of it. */
  async function strayInkUnder(parsed, group, charms, gapPt, onTick) {
    const owned = new Set();
    for (const c of group.charms) { owned.add(c.outline); for (const m of c.members) owned.add(m); }
    const cand = parsed.segments.concat(parsed.nested).filter(s => s.bbox && !owned.has(s) && (s.kind === "path" || s.kind === "text" || s.kind === "image"));
    const out = [];
    for (let i = 0; i < charms.length; i++) {
      const c = charms[i], box = stripBox(c, gapPt), top = c.outline.bbox[1] + 2, segs = [];
      let x0 = Infinity, x1 = -Infinity;
      for (const sg of cand) { const b = sg.bbox; if (b[2] < box[0] || b[0] > box[2] || b[3] < box[1] || b[1] > box[3] || b[3] >= top) continue; segs.push(sg); x0 = Math.min(x0, b[0]); x1 = Math.max(x1, b[2]); }
      // outlined text is many small shapes in a row, one or more per letter; one or two stray bits are a scrap of
      // artwork, not a label, and sending them to be read wastes a call and asks a person to confirm nothing
      const cw = c.outline.bbox[2] - c.outline.bbox[0];
      if (segs.length >= 6 && x1 - x0 >= cw * 0.4) out.push({ charm: c, segs });
      if (onTick && i % 250 === 0) { onTick(i, charms.length, cand.length); await sleep(0); }
    }
    return out;
  }
  function stripPng(parsed, c, gapPt, only) {
    const b = c.outline.bbox, w = b[2] - b[0]; const x0 = b[0] - w * 0.3, x1 = b[2] + w * 0.3, y1 = b[1] + 2, y0 = b[1] - gapPt - 6;
    const k = Math.min(6, 900 / (x1 - x0)); const cv = document.createElement("canvas"); cv.width = Math.ceil((x1 - x0) * k); cv.height = Math.ceil((y1 - y0) * k);
    const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height);
    const tx = (x, y) => [(x - x0) * k, (y1 - y) * k];
    const segs = only || parsed.segments.concat(parsed.nested).filter(s => s.bbox && s.kind !== "clip" && s.kind !== "noop" && !(s.kind === "xobj" && s.children && s.children.length) && !(s.bbox[2] < x0 || s.bbox[0] > x1 || s.bbox[3] < y0 || s.bbox[1] > y1));
    P.drawSegments(ctx, segs, tx, k);
    return cv.toDataURL("image/png");
  }
  /** Index a master file in the browser (design §6.3); the server route is used above `masterServerAbove` charms. */
  /** How many charms in this file carry a SKU written under them — is it a master library sheet, not a sheet to nest?
      Stray text elsewhere on a page does not count: the line has to sit under an outline, as a label does. */
  function looksLikeMaster(parsed, charms) {
    const pat = skuRegex(), gap = (+S.settings.labelGapMm || 6.4) * PT;
    const runs = parsed.segments.concat(parsed.nested).filter(x => x.kind === "text"), lines = [];
    for (const r of runs) { if (r.pieces && r.pieces.length > 1) lines.push(...r.pieces); else lines.push(r); }
    const live = charms.filter(c => c.mergedInto == null); if (!live.length) return 0;
    let n = 0;
    for (const t of lines) {
      if (!t.str || !t.bbox || !P.parseSkuLabel(t.str, pat)) continue;
      const cx = (t.bbox[0] + t.bbox[2]) / 2, top = t.bbox[3];
      for (const c of live) { const b = c.outline.bbox, w = b[2] - b[0], d = b[1] - top; if (d < -1 || d > gap) continue; if (cx < b[0] - w * 0.25 || cx > b[2] + w * 0.25) continue; n++; break; }
    }
    return n >= 3 && n >= live.length * 0.2 ? n : 0;
  }
  async function indexFile(file) {
    // whatever the caller had to hand — a File from the picker or a drop, bytes already read, or a plain buffer
    const bytes = await (async () => {
      // `data` is what this app hands over. A File also answers to `bytes`, but there it is a method the browser added
      // (Blob.bytes()), not the contents — reading it as data is what made an upload fail with "subarray is not a function".
      const own = file && (file.data != null ? file.data : (file.bytes != null && typeof file.bytes !== "function" ? file.bytes : null));
      const raw = own != null ? own : (file && typeof file.arrayBuffer === "function" ? await file.arrayBuffer() : file);
      if (raw instanceof Uint8Array) return raw;
      if (raw instanceof ArrayBuffer) return new Uint8Array(raw);
      if (ArrayBuffer.isView(raw)) return new Uint8Array(raw.buffer, raw.byteOffset, raw.byteLength);
      throw new Error(`could not read ${file && file.name ? file.name : "the file"} — its contents arrived as ${Object.prototype.toString.call(raw)}`);
    })();
    if (!bytes.length) throw new Error(`${file && file.name ? file.name : "the file"} is empty, or its contents were already handed to something else`);
    const masterHash = await sha256(bytes);
    const job = { name: file.name, masterHash, state: "parsing", t0: performance.now(), log: [] }; B.master.jobs.set(masterHash, job); render();
    job.bar = window.CNProgress ? CNProgress.start(`Indexing ${file.name}`, { note: `${(bytes.length / 1048576).toFixed(1)} MB · reading` }) : null;
    const say = (kind, text) => { job.log.push(text); agent({ master: masterHash }, kind, `${file.name}: ${text}`); render(); };
    try {
      say("MASTER", "reading…");
      const parsed = await P.parseSource(bytes, file.name);
      if (job.bar) job.bar.note("finding the charms");
      const g = P.groupCharms(parsed, { minPt: +S.settings.minPt || 6 });
      say("MASTER", `${g.charms.length} charm outline(s), ${parsed.counts.text} text block(s)`);
      // the server route (a background function with about 1 GB) is opt-in by charm count; 0 keeps every master in this tab
      if (+S.settings.masterServerAbove > 0 && g.charms.length > +S.settings.masterServerAbove && S.cloud.ok) {
        say("MASTER", `over ${S.settings.masterServerAbove} charms — indexing on the server`);
        const up = await uploadBytes(`charmnest/master/files/${masterHash.slice(0, 12)}-${file.name.replace(/[^\w.\-]+/g, "_")}`, bytes, "application/pdf", "Uploading master");
        const r = await api("charmNestLibrary", { op: "startMaster", path: up.path, name: file.name, opts: { skuPattern: S.settings.skuPattern, labelGapMm: S.settings.labelGapMm, minPt: S.settings.minPt, engraveMarginMm: S.settings.engraveMarginMm, replaces: B.master.files.filter(f => f.name === file.name && f.masterHash !== masterHash).map(f => f.masterHash) } });
        job.state = "server"; job.jobId = r.id;
        const t0 = Date.now(); let lastChange = Date.now(), lastSig = "";
        for (;;) { await sleep(3000); const j = await api("charmNestLibrary", { op: "getJob", id: r.id }); const d = j.job; if (!d) throw new Error("server job vanished");
          const sig = `${d.status}|${d.stage}|${d.done}`; if (sig !== lastSig) { lastSig = sig; lastChange = Date.now(); }
          const silent = Math.round((Date.now() - lastChange) / 60000);
          job.progress = `${d.done && d.total ? `${d.stage} ${d.done}/${d.total}` : d.stage} · ${Math.round((Date.now() - t0) / 60000)} min${silent >= 2 ? ` · no progress for ${silent} min` : ""}`; render();
          if (d.status === "done") { job.result = d.result; break; }
          if (d.status === "error") throw new Error(d.error || "server indexing failed");
          // a background function that stops reporting has run out of memory or time (about 1 GB and 15 min): say so instead of spinning
          if (Date.now() - lastChange > 6 * 60000) throw new Error(`the server stopped reporting at "${d.stage}" ${silent} min ago — the file is too large for the server route (about 1 GB of memory, 15 min). Index it with the local indexer: node scripts/index-master.cjs "<file>" --origin ${location.origin}`);
          if (Date.now() - t0 > 16 * 60000) throw new Error("the server route ran past its 15-minute limit — index the file with the local indexer: node scripts/index-master.cjs"); }
        await load(true);
        job.state = "done"; say("ok", `server indexed ${job.result.labelled} SKU(s), ${job.result.unlabelled.length} unlabelled, ${job.result.orphans.length} orphan label(s), ${job.result.blocked.length} blocked`);
        render(); return job;
      }
      const lab = P.labelCharms(parsed, g.charms, { pattern: skuRegex(), gapPt: (+S.settings.labelGapMm || 6.4) * PT, widen: 0.25 });
      say("MASTER", `${lab.labels.size} labelled (${lab.skuCount} SKU line(s)) · ${lab.unlabelled.length} unlabelled · ${lab.orphans.length} orphan label(s) · ${lab.duplicates.length} duplicate(s)${lab.undecodable.length ? ` · ${lab.undecodable.length} text run(s) unreadable (outlined or CID font without ToUnicode)` : ""}`);
      // Claude's grouping review merges fragments before anything is indexed
      const src = { id: "master:" + masterHash.slice(0, 8), name: file.name, bytes, parsed, group: g, charms: g.charms, metal: null, state: "ready", master: true };
      src.charms.forEach((c, i) => Object.assign(c, { id: src.id + ":" + i, sourceId: src.id, sourceName: file.name, index: c.index, name: c.sku || null, excluded: false, cloud: null }));
      job.state = "silhouettes";
      // only the charms that carry a SKU are indexed, so only they are traced: on the real master that is 1,038 of 3,408
      await P.buildSilhouettes(parsed, g.charms.filter(c => c.mergedInto == null && c.sku && !c.alreadyHeld), +S.settings.silhouetteRes || 6, (d, t) => { if (job.bar) job.bar.label(`Tracing charms · ${file.name}`).set(d, t); job.progress = `silhouettes ${d}/${t}`; if (d % 10 === 0) render(); });
      // The SKUs are read from the sheet as text, which is quick, so the library is consulted before any work is done:
      // a charm whose SKUs are all held already is left alone. A charm with even one new SKU is rebuilt whole, so all of
      // its SKUs keep sharing one design file, and the master they came from is superseded rather than reported as a clash.
      await load(true).catch(() => {});
      const skusOf = c => [c.sku, ...(c.extraSkus || []).map(x => x.sku)].filter(Boolean).map(x => String(x).toUpperCase());
      const known = B.master.entries;
      const labelledCharms = g.charms.filter(c => c.mergedInto == null && c.sku);
      const supersede = new Set();
      let held = 0;
      if (!reindexAll) {
        for (const c of labelledCharms) {
          const mine = skusOf(c);
          if (mine.every(sk => known.has(sk))) { c.alreadyHeld = true; held++; continue; }
          for (const sk of mine) { const e = known.get(sk); if (e && e.masterHash && e.masterHash !== masterHash) supersede.add(e.masterHash); }
        }
      }
      job.held = held; job.supersede = [...supersede];
      if (held) say("MASTER", `${held} charm(s) are already in the library and are left as they are · ${labelledCharms.length - held} to index${reindexAll ? "" : " (tick “re-index SKUs already held” to rebuild them all)"}`);
      if (held === labelledCharms.length) { await load(true); job.state = "done"; job.written = 0; say("ok", `nothing new on this sheet — all ${held} charm(s) are already in the library`); render(); return job; }
      const liveCount = g.charms.filter(c => c.mergedInto == null).length;
      if (S.settings.review !== "off" && S.cloud.ok && liveCount <= 300) { try { job.state = "review"; render(); await reviewGrouping(src); } catch (e) { say("warn", `Claude grouping review skipped: ${e.message}`); } }
      else if (S.settings.review !== "off" && S.cloud.ok) say("MASTER", `grouping review skipped: ${liveCount} charms is more than one review can hold (300) — the geometry stands, the report lists what to check`);
      // outlined labels: the strip under each unlabelled charm goes to Claude with a strict schema; a person confirms every
      // read. Only a strip with something drawn in it is sent: a charm with nothing under it is reported, not read.
      job.state = "vision"; job.progress = "looking under the unlabelled charms"; render(); await sleep(0);
      const unlAll = g.charms.filter(c => c.mergedInto == null && !c.sku);
      let found = await strayInkUnder(parsed, g, unlAll, (+S.settings.labelGapMm || 6.4) * PT, (d, t) => { if (job.bar) job.bar.label(`Looking under the unlabelled charms · ${file.name}`).set(d, t); job.progress = `looking under the unlabelled charms ${d}/${t}`; render(); });
      let unl = found.map(f => f.charm);
      job.vision = [];
      if (unlAll.length > unl.length) say("MASTER", `${unlAll.length - unl.length} unlabelled charm(s) have nothing under them — reported as unlabelled`);
      const VISION_MAX = 200;
      if (unl.length > VISION_MAX) { say("MASTER", `${unl.length} charm(s) have something under them that was not read as text — more than one pass sends to Claude (${VISION_MAX}); they are reported as unlabelled instead`); unl = []; found = []; }
      if (unl.length && S.cloud.ok) {
        say("MASTER", `asking Claude to read the strip under ${unl.length} unlabelled charm(s)`);
        const strips = found.map(f => ({ index: f.charm.index, image: stripPng(parsed, f.charm, (+S.settings.labelGapMm || 6.4) * PT, f.segs) }));
        for (let i = 0; i < strips.length; i += 40) {
          const r = await agentCall("labelRead", { sourceName: file.name, strips: strips.slice(i, i + 40) }, { label: "Claude is reading labels" });
          if (r.skipped) { say("warn", `label read skipped — ${r.skipped}`); break; }
          // a strip Claude could not read is not a label to confirm: it stays an unlabelled charm in the report
          for (const rd of r.reads || []) { if (!rd.sku) continue; const c = g.charms.find(x => x.index === rd.index); const st = strips.find(x => x.index === rd.index); if (c && st) job.vision.push({ index: rd.index, sku: rd.sku, size: rd.size, confidence: rd.confidence, image: st.image, charm: c, confirmed: false }); }
        }
      }
      if (unl.length && !job.vision.length) say("MASTER", `Claude could not read a SKU under any of the ${unl.length} charm(s) with marks beneath them — they stay unlabelled`);
      job.state = "writing"; job.parsed = parsed; job.charms = g.charms; job.lab = lab; job.src = src;
      await writeIndex(job);
      await load(true);                                                    // the index is reloaded before the job reads "done"
      job.finishedAt = Date.now(); job.state = "done"; say("ok", `indexed ${job.written} SKU(s)${job.held ? ` · ${job.held} charm(s) were already in the library` : ""}${job.vision.length ? ` · ${job.vision.length} label(s) read by Claude await confirmation` : ""}`);
      render(); return job;
    } catch (e) { job.finishedAt = Date.now(); job.state = "error"; job.error = e.message; say("warn", `indexing failed: ${e.message}`); render(); throw e; }
    finally { if (job.bar) { job.bar.end(); job.bar = null; } }
  }
  /** Per labelled charm: the per-SKU .ai + thumbnail, the geometry, the derived flags; then the index and file records. */
  async function writeIndex(job) {
    const { parsed, charms, lab, masterHash, name } = job; const entries = [], blocked = [], skus = [];
    const live = charms.filter(c => c.mergedInto == null && c.sku && !c.excluded && !c.alreadyHeld);
    let n = 0;
    const one = async (c) => {
      const key = c.skuSize ? `${c.sku}__${c.skuSize}` : c.sku;
      let ai = null, png = null;
      if (S.cloud.ok) { const bytes = await P.buildSingleCharm(c, parsed); [ai, png] = await Promise.all([uploadBytes(`charmnest/master/${key}.ai`, bytes, "application/illustrator", `Saving ${key}`), uploadBytes(`charmnest/master/${key}.png`, dataUrlToBytes(c.thumb), "image/png")]); }
      const reasons = []; if (c.open) reasons.push("open outline");
      let engravable = true, upAngle = null, upSource = "drawn", flipOk = true;
      try { const up = G.upAngleOf(c); upAngle = up.angle; upSource = up.source; const view = G.backView(c, { res: 6, upAngle }); const mask = G.engraveMask(view, { marginMm: +S.settings.engraveMarginMm || 0.8, keepOut: keepOutOf(c) }); const r = G.largestRectangles(mask, 1)[0]; engravable = !!r && ((r.wPt * MM >= 6 && r.hPt * MM >= 3) || (r.wPt * MM >= 3 && r.hPt * MM >= 6)); }
      catch (e) { flipOk = false; engravable = false; reasons.push(e.message); }
      const wMm = c.widthPt * MM, hMm = c.heightPt * MM; const outOfRange = Math.max(wMm, hMm) > (+S.settings.sizeMaxMm || 60) || Math.max(wMm, hMm) < (+S.settings.sizeMinMm || 3);
      entries.push({ sku: c.sku, size: c.skuSize, charmHash: c.hash, widthPt: c.widthPt, heightPt: c.heightPt, areaPt2: c.areaPt2, members: c.members.length, holes: P.cutLinesOf(c).length, engravable, upAngle, upSource, aiPath: ai && ai.path, aiUrl: ai && ai.url, thumbPath: png && png.path, thumbUrl: png && png.url, open: !!c.open, labelSource: c.labelSource || "text", confidence: c.labelConfidence == null ? null : c.labelConfidence, blocked: reasons.length ? reasons.join("; ") : null, outOfRange, flipOk, backKeepOut: keepOutOf(c).length ? keepOutOf(c).map(m => ({ layer: m.layer })) : null });
      if (reasons.length) blocked.push({ sku: c.sku, reason: reasons.join("; ") }); skus.push(c.sku);
      for (const x of c.extraSkus || []) { entries.push(Object.assign({}, entries[entries.length - 1], { sku: x.sku, size: x.size })); skus.push(x.sku); if (reasons.length) blocked.push({ sku: x.sku, reason: reasons.join("; ") }); }   // every further line under the charm: the same design under another SKU
      job.progress = `written ${++n}/${live.length}`; if (job.bar) job.bar.label(`Writing the charm library · ${job.name}`).set(n, live.length); if (n % 5 === 0) render();
    };
    const queue = live.slice(); await Promise.all(Array.from({ length: 4 }, async () => { while (queue.length) await one(queue.shift()); }));
    // a small ring left loose beside a charm (not merged by grouping or review) blocks that charm
    for (const o of job.src.group.orphans || []) { const b = o.bbox; if (!b || o.kind !== "path" || !o.closed) continue; if (Math.max(b[2] - b[0], b[3] - b[1]) > 13) continue; const cx = (b[0] + b[2]) / 2, cy = (b[1] + b[3]) / 2; for (const c of live) { const ob = c.outline.bbox; if (cx < ob[0] - 4 * PT || cx > ob[2] + 4 * PT || cy < ob[1] - 4 * PT || cy > ob[3] + 4 * PT) continue; if (G.distToPolys(cx, cy, G.flatten(c.outline, 8)) <= 3 * PT) { const e = entries.find(x => x.sku === c.sku); if (e && !/detached ring/.test(e.blocked || "")) { e.blocked = (e.blocked ? e.blocked + "; " : "") + "detached ring not merged"; blocked.push({ sku: c.sku, reason: "detached ring not merged" }); } } } }
    job.entries = entries; job.blocked = blocked; job.written = entries.length;
    if (!S.cloud.ok) return;
    const replaces = [...new Set(B.master.files.filter(f => f.name === name && f.masterHash !== masterHash).map(f => f.masterHash).concat(job.supersede || []))];
    const r = await api("charmNestLibrary", { op: "masterPutIndex", entries, masterHash, masterPath: job.masterPath || null, masterName: name, hashSource: "browser", replaces }, { label: "Writing the master index" });
    job.conflicts = r.blocked || []; job.sizeMoved = r.sizeMoved || [];
    if (job.conflicts.length) agent({ master: masterHash }, "warn", `${name}: ${job.conflicts.length} SKU(s) also live in another master file — blocked until fixed: ${job.conflicts.map(b => b.sku).join(", ")}`);
    if (job.sizeMoved.length) agent({ master: masterHash }, "warn", `${name}: ${job.sizeMoved.length} SKU(s) changed size by more than 5% since the last index: ${job.sizeMoved.map(b => b.sku).join(", ")}`);
    await api("charmNestLibrary", { op: "masterPutFile", file: { masterHash, path: job.masterPath || null, name, charms: charms.filter(c => c.mergedInto == null).length, labelled: lab.labels.size, unlabelled: lab.unlabelled, orphans: lab.orphans, duplicates: lab.duplicates, undecodable: lab.undecodable.length, blocked: blocked.concat(job.conflicts.map(b => ({ sku: b.sku, reason: b.reason }))), skus, indexedBy: "browser", pageW: parsed.pageW, pageH: parsed.pageH, replaces, visionReads: (job.vision || []).map(v => ({ index: v.index, sku: v.sku, size: v.size, confidence: v.confidence, confirmed: v.confirmed })) } });
  }
  /** Members drawn on a "BACK KEEP-OUT" layer of the master are subtracted from the engraving mask. */
  const keepOutOf = c => c.members.filter(m => m.layer && /back\s*keep-?out/i.test(m.layer));
  /** A person confirms Claude's read of an outlined label: the charm gets that SKU and is written like any labelled one. */
  async function confirmVision(job, reads) {
    for (const v of reads) { const c = v.charm; c.sku = v.sku; c.skuSize = v.size || null; c.labelSource = "vision"; c.labelConfidence = v.confidence; c.name = v.sku; v.confirmed = true; job.lab.labels.set(c.index, { sku: v.sku, size: v.size, seg: null }); job.lab.unlabelled = job.lab.unlabelled.filter(i => i !== c.index); }
    job.state = "writing"; render();
    await writeIndex(job);
    for (const v of reads) await api("charmNestLibrary", { op: "masterPatch", sku: v.sku, patch: { labelSource: "vision", confirmedBy: employeeName() || "operator" } }).catch(() => {});
    job.state = "done"; await load(true); render();
  }
  /** The same patch on every SKU of one charm, with a single redraw. */
  /** Everything the run found, as a file: the lists are too long to read on screen but belong somewhere. */
  function saveReport(job) {
    const l = job.lab || {};
    const rep = { file: job.name, masterHash: job.masterHash, at: new Date().toISOString(),
      charms: job.charms ? job.charms.length : null, labelled: l.labels ? l.labels.size : null, skuLines: l.skuCount || null, written: job.written || 0,
      unlabelledCharmIndices: l.unlabelled || [], linesWithNoCharmAbove: (l.orphans || []).map(o => ({ sku: o.sku, size: o.size || null })),
      skusUnderTwoCharms: l.duplicates || [], blocked: job.blocked || [], alsoInAnotherMaster: job.conflicts || [], readByClaude: (job.vision || []).map(v => ({ index: v.index, sku: v.sku, size: v.size, confidence: v.confidence, confirmed: v.confirmed })) };
    const url = URL.createObjectURL(new Blob([JSON.stringify(rep, null, 1)], { type: "application/json" }));
    const a = document.createElement("a"); a.href = url; a.download = `${String(job.name || "master").replace(/\.[^.]+$/, "")}-index-report.json`; a.click();
    setTimeout(() => URL.revokeObjectURL(url), 8000);
    toast("report saved", "ok");
  }
  async function patchMany(skus, p) { for (const s of skus) { await api("charmNestLibrary", { op: "masterPatch", sku: s, patch: p }); const e = entryFor(s); if (e) Object.assign(e, p); } render(); }
  async function patch(sku, p) { await api("charmNestLibrary", { op: "masterPatch", sku, patch: p }); const e = entryFor(sku); if (e) Object.assign(e, p); render(); }
  function render() {
    const v = document.getElementById("masterView"); if (!v || v.classList.contains("hidden")) return;
    if (!v.dataset.built) {
      v.dataset.built = "1";
      v.innerHTML = `<div class="masterHead"><input type="file" id="mFile" accept=".ai,.pdf" class="hidden"><input type="search" id="mSearch" placeholder="Search SKUs" style="border:1px solid var(--line);border-radius:9px;padding:7px 10px"><label style="display:flex;gap:5px;align-items:center;font-size:11px" title="Off: a SKU the library already holds is left as it is, and only new charms are built. On: every charm on the sheet is rebuilt and rewritten."><input type="checkbox" id="mAllSkus"> re-index SKUs already held</label><span class="pill neutral" id="mCount"></span></div>
        <div class="noteBox">Each charm in a master file has its SKU as text directly under it (within ${S.settings.labelGapMm} mm, centred under the outline). A SKU is one design whatever colour it is ordered in; the material comes from the order. Labels are never part of the charm. Unlabelled charms, orphan labels and duplicates are listed in red; a SKU present in two masters is blocked until fixed.</div>
        <div id="mJobs" style="display:grid;gap:10px"></div><div id="mFiles" style="display:grid;gap:10px"></div><div class="section">Indexed SKUs</div><div class="skuGrid" id="mGrid"></div>`;
      { const cb = v.querySelector("#mAllSkus"); cb.checked = reindexAll; cb.onchange = () => { reindexAll = cb.checked; toast(reindexAll ? "every charm on the next sheet will be rebuilt" : "charms already in the library will be skipped", "ok"); }; }
      // the file goes where it is dropped: on this tab it is a master for the library
      ["dragenter", "dragover"].forEach(ev => v.addEventListener(ev, e => { if (!(e.dataTransfer && [...(e.dataTransfer.types || [])].includes("Files"))) return; e.preventDefault(); e.stopPropagation(); v.classList.add("dragOver"); e.dataTransfer.dropEffect = "copy"; }));
      ["dragleave", "drop"].forEach(ev => v.addEventListener(ev, () => v.classList.remove("dragOver")));
      v.addEventListener("drop", e => { if (!(e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length)) return; e.preventDefault(); e.stopPropagation(); for (const f of e.dataTransfer.files) indexFile(f).catch(err => toast(err.message, "bad", 7000)); });
      v.querySelector("#mFile").onchange = e => { for (const f of e.target.files) indexFile(f).catch(err => toast(err.message, "bad", 7000)); e.target.value = ""; };
      v.querySelector("#mSearch").oninput = render;
      load().then(render).catch(() => {});
    }
    const jobs = v.querySelector("#mJobs"); jobs.innerHTML = "";
    for (const job of B.master.jobs.values()) {
      const card = el("div", "masterFile");
      card.innerHTML = `<div class="fh"><b>${esc(job.name)}</b><span class="hash">${job.masterHash.slice(0, 12)}</span><span class="pill ${job.state === "done" ? "ok" : job.state === "error" ? "bad" : "warn"}">${job.state === "done" ? "indexed" : job.state === "error" ? "failed" : `<span class="spin"></span>${job.state}${job.progress ? " · " + job.progress : ""}`}</span>${job.written != null ? `<span>${job.written} SKU(s) written</span>` : ""}${job.error ? `<span style="color:#8a3a26">${esc(job.error)}</span>` : ""}</div>`;
      if (job.lab) {
        // the counts are the report; the lists behind them are for a file, not for a wall of red
        const l = job.lab, left = [], MAX = 24;
        const some = (arr, f) => arr.slice(0, MAX).map(f).join(", ") + (arr.length > MAX ? ` <i>… and ${arr.length - MAX} more</i>` : "");
        if (l.unlabelled.length) left.push(`<b>${l.unlabelled.length} charm(s) with no SKU under them</b>: ${some(l.unlabelled, i2 => "#" + i2)}`);
        if (l.orphans.length) left.push(`<b>${l.orphans.length} line(s) with no charm above</b>: ${some(l.orphans, o => esc(o.sku))}`);
        if (l.duplicates.length) left.push(`<b>${l.duplicates.length} SKU(s) written under two charms</b>: ${some(l.duplicates, d => `${esc(d.sku)} (#${d.charmIndex})`)}`);
        if (job.blocked && job.blocked.length) left.push(`<b>${job.blocked.length} blocked</b>: ${some(job.blocked, b => `${esc(b.sku)} — ${esc(b.reason)}`)}`);
        if (job.conflicts && job.conflicts.length) left.push(`<b>${job.conflicts.length} in another master too</b>: ${some(job.conflicts, b => esc(b.sku))}`);
        if (job.entries) { const oor = job.entries.filter(e => e.outOfRange); if (oor.length) left.push(`<b>${oor.length} outside the ${S.settings.sizeMinMm}–${S.settings.sizeMaxMm} mm range</b>: ${some(oor, e => esc(e.sku))}`); }
        if (left.length) {
          const box = el("div", "leftovers", left.map(x => `<div>${x}</div>`).join(""));
          const save = el("button", "btn ghost xs", "Save the full report"); save.style.marginTop = "8px"; save.onclick = () => saveReport(job);
          box.appendChild(save); card.appendChild(box);
        }
      }
      const pending = (job.vision || []).filter(x => !x.confirmed);
      if (pending.length) {
        const tray = el("div", "visionTray"); const head = el("div", "section", `Confirm ${pending.length} label(s) read by Claude from outlined text (reads under 95% are unchecked)`);
        pending.forEach((x, i) => { const t = el("div", "vt"); t.innerHTML = `<img src="${x.image}" alt=""><div><label style="display:flex;gap:6px;align-items:center"><input type="checkbox" data-i="${i}" ${x.confidence >= 0.95 && x.sku ? "checked" : ""}><span class="mono">#${x.index}</span> <span class="pill ${x.confidence >= 0.95 ? "ok" : "warn"}">${Math.round(x.confidence * 100)}%</span></label><input type="text" data-sku="${i}" value="${esc(x.sku)}" placeholder="SKU as written"><input type="text" data-size="${i}" value="${esc(x.size || "")}" placeholder="size (optional)" style="margin-top:4px"><img src="${x.charm.thumb}" style="width:48px;margin-top:4px;border-radius:4px" alt=""></div>`; tray.appendChild(t); });
        const btn = el("button", "btn gold sm", "Confirm checked labels"); btn.type = "button";
        btn.onclick = async () => { const reads = []; tray.querySelectorAll("input[type=checkbox]").forEach(cb => { if (!cb.checked) return; const i = +cb.dataset.i; const x = pending[i]; const sku = tray.querySelector(`input[data-sku="${i}"]`).value.trim().toUpperCase(); if (!skuRegex().test(sku)) { toast(`${sku || "(empty)"} is not a valid SKU`, "bad"); return; } x.sku = sku; x.size = tray.querySelector(`input[data-size="${i}"]`).value.trim().toUpperCase() || null; reads.push(x); }); if (!reads.length) return; if (!employeeName()) askEmployee(); await confirmVision(job, reads); toast(`${reads.length} label(s) confirmed and indexed`, "ok"); };
        card.append(head, tray, btn);
      }
      // a finished run is a line you can open; only what is still running stays open in front of you
      if (job.state === "done" || job.state === "error") {
        const d = el("details", "masterFileRow"), sum = document.createElement("summary");
        const when = new Date(job.finishedAt || Date.now()).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
        sum.innerHTML = `<b>${esc(job.name)}</b><span class="hash">${when}</span><span>${job.state === "error" ? "failed" : `${job.written || 0} SKU(s) written`}</span>${job.held ? `<span class="hash">${job.held} already held</span>` : ""}`;
        d.appendChild(sum); d.appendChild(card); jobs.appendChild(d);
      } else jobs.appendChild(card);
    }
    // Indexed files pile up as the sheet is revised. One line each — what it is, when, how much — folded away by date,
    // with the full report and the remove behind a second fold. Nothing is lost, nothing is in the way.
    const files = v.querySelector("#mFiles");
    const byDay = new Map();
    for (const f of B.master.files) {
      const at = f.indexedAt ? new Date(f.indexedAt) : null;
      const day = at ? at.toLocaleDateString() : "no date";
      if (!byDay.has(day)) byDay.set(day, []);
      byDay.get(day).push(f);
    }
    const days = [...byDay.entries()].sort((a, b) => (byDay.get(b[0])[0].indexedAt || 0) - (byDay.get(a[0])[0].indexedAt || 0));
    const num = (c, l) => (c != null ? c : (l || []).length);
    files.innerHTML = days.map(([day, list], di) => {
      list.sort((a, b) => (b.indexedAt || 0) - (a.indexedAt || 0));
      const skus = list.reduce((n, f) => n + (f.skus ? f.skus.length : 0), 0);
      return `<details class="masterDay"${di === 0 ? " open" : ""}><summary>${esc(day)} · ${list.length} file${list.length === 1 ? "" : "s"} · ${skus} SKU line(s)</summary>` +
        list.map(f => {
          const at = f.indexedAt ? new Date(f.indexedAt) : null;
          const u = num(f.unlabelledCount, f.unlabelled), o = num(f.orphanCount, f.orphans), b = num(f.blockedCount, f.blocked);
          return `<details class="masterFileRow"><summary><b>${esc(f.name || f.masterHash)}</b><span class="hash">${at ? at.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : ""}</span><span>${f.labelled || 0} of ${f.charms || 0} labelled</span>${b ? `<span style="color:#8a3a26">${b} blocked</span>` : ""}</summary>` +
            `<div class="fh" style="margin:6px 0 4px"><span class="hash">${esc((f.masterHash || "").slice(0, 12))}</span>${u ? `<span style="color:#8a3a26">${u} charm(s) with no SKU under them</span>` : ""}${o ? `<span style="color:#8a3a26">${o} line(s) with no charm above</span>` : ""}<span class="hash">${esc(f.indexedBy || "")}</span></div>` +
            `<button class="btn ghost xs" data-rm="${esc(f.masterHash || "")}">Remove every SKU from this file</button></details>`;
        }).join("") + `</details>`;
    }).join("");
    files.querySelectorAll("[data-rm]").forEach(b => b.onclick = async () => { if (!confirm("Remove every SKU indexed from this master file? Pool adds for them will fail until it is re-indexed.")) return; await api("charmNestLibrary", { op: "masterRemoveFile", masterHash: b.dataset.rm }); await load(true); render(); });
    const q = (v.querySelector("#mSearch").value || "").trim().toUpperCase();
    // One charm is one tile, whatever it is sold as. The same design carries several SKUs (the jewellery it goes into),
    // and they share one file in the library; the tile lists every one of them and the search matches any of them.
    const fileOf = e => e.aiPath || (Object.values(e.sizes || {}).find(s => s && s.aiPath) || {}).aiPath || "";
    const groups = new Map();
    for (const e of B.master.entries.values()) { const k = fileOf(e) || "sku:" + e.sku; if (!groups.has(k)) groups.set(k, []); groups.get(k).push(e); }
    const designs = [...groups.values()].map(list => { list.sort((a, b) => a.sku.localeCompare(b.sku)); return { list, head: list[0], skus: list.map(e => e.sku) }; });
    // a SKU can read as part of a longer one ("FROG4" inside "HUGGIE HOOPS- FROG4"): an exact SKU first, then the ones
    // that start with what was typed, then the rest — alphabetical within each
    const rank = d => d.skus.some(s => s === q) ? 0 : d.skus.some(s => s.startsWith(q)) ? 1 : 2;
    const rows = designs.filter(d => !q || d.skus.some(s => s.includes(q)))
      .sort((a, b) => (q ? rank(a) - rank(b) : 0) || a.skus[0].localeCompare(b.skus[0]));
    const cap = showAll ? rows.length : 600;
    const shown = rows.slice(0, cap);
    v.querySelector("#mCount").innerHTML = `${designs.length} charm(s) · ${B.master.entries.size} SKU(s) indexed${q ? ` · ${rows.length} match &ldquo;${esc(q)}&rdquo;` : ""}` +
      (rows.length > shown.length ? ` · <b>showing ${shown.length}</b> <button class="btn sm" id="mAll" style="margin-left:6px">Show all ${rows.length}</button>` : "");
    const all = v.querySelector("#mAll"); if (all) all.onclick = () => { showAll = true; render(); };
    const grid = v.querySelector("#mGrid");
    if (B.master.loading && !B.master.entries.size) { grid.innerHTML = `<div class="libEmpty">Loading the charm library…</div>`; return; }
    grid.innerHTML = shown.map(d => {
      const e = d.head, keys = esc(d.skus.join("|"));
      const blocked = [...new Set(d.list.map(x => x.blocked).filter(Boolean))].join("; ");
      const sizes = e.sizes ? Object.entries(e.sizes) : [];
      return `<div class="skuTile${blocked ? " blocked" : ""}" data-sku="${esc(e.sku)}">` +
        (thumbOf(e) ? `<img crossorigin="anonymous" src="${thumbOf(e)}" loading="lazy" alt="">` : `<div style="aspect-ratio:1;background:#fff;border-radius:6px"></div>`) +
        `<div class="sku" title="${esc(d.skus.join(", "))}">${esc(e.sku)}</div>` +
        (d.skus.length > 1 ? `<div class="meta">${d.skus.slice(1).map(s => `<div>${esc(s)}</div>`).join("")}</div>` : "") +
        `<div class="meta">${(e.widthPt * MM).toFixed(1)} × ${(e.heightPt * MM).toFixed(1)} mm · ${e.holes} hole(s)${sizes.length ? ` · sizes ${sizes.map(([k]) => k).join("/")}` : ""}${d.skus.length > 1 ? ` · ${d.skus.length} SKUs` : ""}</div>` +
        `<div class="meta">up ${e.upAngle == null ? "as drawn" : Math.round(e.upAngle) + "°"} · ${esc(e.labelSource || "text")}${e.hashSource === "server" ? " · server" : ""}</div>` +
        (blocked ? `<div class="bad">${esc(blocked)}</div>` : "") +
        `<div class="row"><label style="display:flex;gap:4px;align-items:center;font-size:11px"><input type="checkbox" data-eng="${keys}" ${e.engravable !== false ? "checked" : ""}> engravable</label>` +
        `<input type="number" data-up="${keys}" value="${e.upAngle == null ? "" : Math.round(e.upAngle)}" placeholder="up°" style="width:52px;border:1px solid var(--line);border-radius:6px;padding:2px 4px;font-size:11px">` +
        (blocked ? `<button class="btn ghost xs" data-unblock="${keys}">unblock</button>` : "") +
        (e.aiUrl ? `<a class="btn ghost xs" href="${e.aiUrl}" target="_blank" rel="noopener">.ai</a>`
                 : sizes.filter(([, s]) => s && s.aiUrl).map(([k, s]) => `<a class="btn ghost xs" href="${s.aiUrl}" target="_blank" rel="noopener">${esc(k)}.ai</a>`).join("")) +
        `</div></div>`;
    }).join("") + (rows.length > shown.length ? `<div class="libEmpty">${rows.length - shown.length} more — press &ldquo;Show all&rdquo;, or narrow the search</div>` : "");
    // a charm's settings belong to the charm, so they are written to every SKU that shares it
    const each = (attr, fn) => grid.querySelectorAll(`[data-${attr}]`).forEach(el => fn(el, el.dataset[attr].split("|")));
    each("eng", (cb, skus) => cb.onchange = () => patchMany(skus, { engravable: cb.checked }).then(() => toast(`${skus.join(", ")}: ${cb.checked ? "engravable" : "not engravable"} (operator)`, "ok")));
    each("up", (inp, skus) => inp.onchange = () => { const v2 = inp.value.trim(); if (v2 === "") return; patchMany(skus, { upAngle: +v2 }).then(() => toast(`${skus.join(", ")}: up = ${+v2}° (operator)`, "ok")); });
    each("unblock", (b, skus) => b.onclick = () => patchMany(skus, { blocked: null }).then(() => toast(`${skus.join(", ")} unblocked`, "ok")));
  }
  return { entryFor, thumbOf, fetchEntry, load, indexFile, looksLikeMaster, render, patch, patchMany, keepOutOf, skuRegex, stripPng, strayInkUnder };
})();

/* ═══ 20 · Pool — one charm per order line and copy ══════════════════════ */
const Pool = window.Pool = (() => {
  const sizeEntry = (entry, size) => (entry.sizes && Object.keys(entry.sizes).length ? (size && entry.sizes[size]) || null : entry);
  /** Fetch the per-SKU .ai once per session, parse it, trace it; every copy shares the geometry. */
  async function masterCharm(entry, size) {
    const geom = sizeEntry(entry, size); if (!geom || !geom.aiPath) throw new Error(`no design file for ${entry.sku}${size ? " · " + size : ""}`);
    const key = geom.aiPath;
    if (B.pool.sources.has(key)) return B.pool.sources.get(key);
    const url = geom.aiUrl || (await api("charmNestOutput", { op: "url", path: geom.aiPath })).url;
    const bytes = new Uint8Array(await (await fetch(url)).arrayBuffer());
    const parsed = await P.parseSource(bytes, `${entry.sku}.ai`);
    const g = P.groupCharms(parsed, { minPt: +S.settings.minPt || 6 });
    if (!g.charms.length) throw new Error(`${entry.sku}: no outline in the master copy`);
    const charm = g.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
    if (g.charms.length > 1) { for (const c of g.charms) if (c !== charm) { for (const m of c.members) if (!charm.members.includes(m)) charm.members.push(m); charm.topIndices = [...new Set(charm.topIndices.concat(c.topIndices))]; } agent({ pool: true }, "warn", `${entry.sku}: the master copy split into ${g.charms.length} pieces — folded back into one charm`); }
    await P.buildSilhouettes(parsed, [charm], +S.settings.silhouetteRes || 6);
    const srcId = "pool:" + key.replace(/[^\w]+/g, "_");
    const src = { id: srcId, pool: true, name: `${entry.sku}${size ? " · " + size : ""} (master)`, sku: entry.sku, bytes, hash: entry.charmHash || charm.hash, parsed, group: g, charms: [charm], metal: null, state: "ready", t0: performance.now(), cloud: { path: geom.aiPath, url }, persisting: null };
    Object.assign(charm, { id: srcId + ":0", sourceId: srcId, sourceName: src.name, index: 0, name: entry.sku, sku: entry.sku, namedBy: "master", excluded: false, cloud: { ai: url, aiPath: geom.aiPath, png: geom.thumbUrl || null, pngPath: geom.thumbPath || null }, upAngle: entry.upAngle, engravable: entry.engravable !== false, backKeepOut: Master.keepOutOf(charm) });
    S.poolSources[srcId] = src; B.pool.sources.set(key, src);
    return src;
  }
  function cloneCharm(c, id) { const k = Object.assign({}, c, { id, pinned: null }); return k; }
  /** §6.4 · one pooled charm per copy of the line, on the material card the ORDER says. */
  async function poolAdd(row, run) {
    const sp = row.spec;
    if (!sp || sp.noDesign) { row.state = "noDesign"; return; }
    row.problems = row.problems.filter(p => !["unmatchedSku", "blockedSku", "missingSize", "oversize"].includes(p.kind));   // re-derived below on every attempt
    if (row.problems.length) { row.state = "held"; row.reason = Review.problemText(row.problems[0]); return; }
    let entry = Master.entryFor(sp.designSku) || await Master.fetchEntry(sp.designSku);
    if (!entry) { row.state = "unmatched"; row.reason = "not in any master file"; row.problems.push({ kind: "unmatchedSku", reason: "not in any master file", sku: sp.designSku, listingId: String(row.line.listingId || ""), title: row.line.title }); agent({ pool: true }, "warn", `${row.order.receiptId} · ${sp.designSku}: not in any master file`); return; }
    if (entry.blocked) { row.state = "held"; row.reason = `SKU blocked: ${entry.blocked}`; row.problems.push({ kind: "blockedSku", reason: entry.blocked, sku: sp.designSku }); return; }
    if (entry.sizes && Object.keys(entry.sizes).length && !(sp.size && entry.sizes[sp.size])) { row.state = "held"; row.reason = `no design for size ${sp.size || "(none)"}`; row.problems.push({ kind: "missingSize", sku: sp.designSku, size: sp.size, available: Object.keys(entry.sizes) }); return; }
    const src = await masterCharm(entry, sp.size);
    const base = src.charms[0];
    // oversize: the charm cannot fit the plate under the ceiling
    const st = stockFor(sp.material); const usable = (st.wPt - 2 * (+S.settings.insetPt || 0)) * (st.hPt - 2 * (+S.settings.insetPt || 0)) * (+S.settings.maxFill || 0.74);
    if (base.areaPt2 > usable || Math.min(base.widthPt, base.heightPt) > Math.max(st.wPt, st.hPt) - 2 * (+S.settings.insetPt || 0)) { row.state = "oversize"; row.reason = `charm ${(base.widthPt * MM).toFixed(1)} × ${(base.heightPt * MM).toFixed(1)} mm does not fit the ${labelOf(sp.material)} plate under the ceiling`; row.problems.push({ kind: "oversize", sku: sp.designSku, widthMm: base.widthPt * MM, heightMm: base.heightPt * MM, material: sp.material }); return; }
    const pools = [], charms = [];
    for (let copy = 1; copy <= sp.quantity; copy++) {
      const poolId = O.poolId(row.order, row.line, copy);
      const charm = copy === 1 && !base.poolId ? base : cloneCharm(base, `${src.id}:${poolId}`);
      charm.name = `${row.order.receiptId} · ${sp.designSku}${sp.quantity > 1 ? ` · ${copy}/${sp.quantity}` : ""}`;
      charm.order = row.order.receiptId; charm.orderInfo = { receiptId: row.order.receiptId, transactionId: row.line.transactionId, sku: sp.designSku, copy, quantity: sp.quantity, form: sp.form, size: sp.size };
      charm.poolId = poolId; charm.metal = sp.material; charm.lineKey = row.key; charm.pinned = null; charm.excluded = false;
      pools.push({ poolId, runId: run ? run.runId : null, setId: run ? run.setId || null : null, sheetId: null, orderId: row.order.receiptId, transactionId: row.line.transactionId, sku: sp.designSku, material: sp.material, size: sp.size || null, form: sp.form || null, chain: sp.chain || null, copy, quantity: sp.quantity, charmHash: charm.hash, masterHash: entry.masterHash || null, aiPath: sizeEntry(entry, sp.size).aiPath, engrave: !!(row.engrave && row.engrave.needed), state: "ready", lineKey: row.key, updateTs: row.order.updateTs });
      charms.push(charm);
    }
    if (S.cloud.ok) {
      const r = await api("charmNestLibrary", { op: "poolPut", pools }, { label: "Recording the pool" });
      if (r.contended && r.contended.length) { row.state = "contended"; row.reason = `claimed by run ${r.contended[0].runId}`; agent({ pool: true }, "warn", `${row.order.receiptId} · ${sp.designSku}: a live run (${r.contended[0].runId}) already holds this line — skipped`); return; }
    }
    const page = activePage(sp.material); if (run) page.runId = run.runId;
    for (const c of charms) if (!page.charms.includes(c)) page.charms.push(c);
    for (const p of pools) B.pool.rows.set(p.poolId, p);
    row.poolIds = pools.map(p => p.poolId); row.state = "pooled"; row.material = sp.material; row.reason = null;
    sheetDirty(page);
    agent({ metal: sp.material, pool: true }, "POOL", `${row.order.receiptId} · ${sp.designSku}${sp.quantity > 1 ? " ×" + sp.quantity : ""} → ${labelOf(sp.material)} (${row.engrave && row.engrave.needed ? "engrave" : "plain"})`);
  }
  async function addAll(run) {
    const rows = Orders.rows().filter(r => r.state === "pulled" || r.state === "held" || r.state === "unmatched" || r.state === "oversize");
    let n = 0;
    const bar = rows.length && window.CNProgress ? CNProgress.start(`Preparing ${rows.length} order line(s)`, { total: rows.length }) : null;
    for (const row of rows) { if (bar) bar.set(n, rows.length, row.spec && row.spec.designSku ? String(row.spec.designSku) : ""); try { await poolAdd(row, run); } catch (e) { row.state = "held"; row.reason = e.message; agent({ pool: true }, "warn", `${row.order.receiptId} · ${row.spec && row.spec.designSku}: ${e.message}`); } if (++n % 5 === 0) { Orders.render(); } }
    if (bar) bar.end();
    Review.syncOrderItems(); Orders.render(); renderRail(); updateTopSub(); refreshAllCards();
    if (run) { run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); await RunCtl.save(run); }
    const pooled = rows.filter(r => r.state === "pooled").length;
    agent({ pool: true }, "POOL", `Pool: ${pooled} line(s) queued on the cards · ${rows.filter(r => r.state !== "pooled" && r.state !== "noDesign").length} held`);
    return pooled;
  }
  async function update(poolIds, patch) { for (const id of poolIds) { const p = B.pool.rows.get(id); if (p) Object.assign(p, patch); } if (S.cloud.ok && poolIds.length) await api("charmNestLibrary", { op: "poolUpdate", poolIds, patch }).catch(() => {}); }
  const charmOf = poolId => allSheets().flatMap(sh => sh.charms).find(c => c.poolId === poolId) || null;
  const sheetOf = poolId => allSheets().find(sh => sh.placements.some(p => { const c = sh.charms.find(x => x.id === p.id); return c && c.poolId === poolId; })) || null;
  return { poolAdd, addAll, masterCharm, cloneCharm, update, charmOf, sheetOf, sizeEntry };
})();

/* ═══ 21 · Engrave — the words, the checked flip, the fit, the review, the back files ═══ */
const Engrave = window.Engrave = (() => {
  const F_ = B.engrave.fonts;
  // Source Sans 3 (Adobe, SIL Open Font License): a humanist sans drawn in the same tradition as Myriad Pro, shipped with the app
  const FONT_FILES = { Regular: "vendor/fonts/SourceSans3-Regular.otf", Semibold: "vendor/fonts/SourceSans3-Semibold.otf" };
  async function loadFonts() {
    if (F_.ok || F_.loading) return F_.loading || F_;
    F_.loading = (async () => {
      for (const [w, path] of Object.entries(FONT_FILES)) {
        try { const r = await fetch(path, { cache: "force-cache" }); if (!r.ok) throw new Error(`HTTP ${r.status}`); const buf = await r.arrayBuffer(); if (buf.byteLength < 1000) throw new Error("empty file"); F_[w] = opentype.parse(buf); }
        catch (e) { if (w === "Regular") F_.error = `${path}: ${e.message}`; else F_.semiboldMissing = `${path}: ${e.message}`; }
      }
      F_.ok = !!F_.Regular; if (!F_.ok) agent({ engrave: true }, "warn", `Source Sans 3 is not available (${F_.error}) — engraving cannot be set exactly; the .otf files belong in vendor/fonts/`);
      else agent({ engrave: true }, "ENGRAVE", `Source Sans 3 loaded: ${F_.Regular.names.fullName ? Object.values(F_.Regular.names.fullName)[0] : "Regular"}${F_.Semibold ? " + Semibold" : " (Semibold missing — Regular used at every size)"}`);
      const h = document.getElementById("stFontsHelp"); if (h) h.innerHTML = F_.ok ? `Loaded: ${esc(Object.values(F_.Regular.names.fullName || {})[0] || "Source Sans 3 Regular")}${F_.Semibold ? ", " + esc(Object.values(F_.Semibold.names.fullName || {})[0] || "Semibold") : " · Semibold missing"}` : `<span style="color:#8a3a26">Not found: ${esc(F_.error)}</span> — SourceSans3-Regular.otf and SourceSans3-Semibold.otf belong in vendor/fonts/`;
    })().finally(() => { F_.loading = null; });
    return F_.loading;
  }
  const fontFor = weight => (weight === "Semibold" && F_.Semibold) || F_.Regular;
  const fitOpts = () => ({ minCapMm: +S.settings.engraveMinCapMm || 1.6, maxHeightFrac: +S.settings.engraveMaxHeightFrac || 0.4, lineGap: 0.18, minStrokeMm: +S.settings.engraveMinStrokeMm || 0, minGapMm: +S.settings.engraveMinGapMm || 0, tryRotated: S.settings.engraveTryRotated !== "off" });
  const items = () => B.engrave.items;
  const jobOf = row => items().get(row.key) || null;
  function ensureJob(row) { let j = items().get(row.key); if (!j) { j = { key: row.key, row, state: "classify", text: null, lines: [], source: null, quote: null, confidence: null, requests: null, questions: [], decision: null, fit: null, view: null, mask: null, claude: null, approvedBy: null, approvedAt: null, backs: [], copies: [], reason: null, t: Date.now() }; items().set(row.key, j); } j.copies = row.poolIds.slice(); return j; }
  const pendingCount = () => [...items().values()].filter(j => ["words", "review", "fitting", "ready", "classify"].includes(j.state) && j.row.state !== "gone").length;
  /** How many placements have already been settled in this run — the numerator of "3 of 9" on the card. */
  const reviewedCount = () => [...items().values()].filter(j => ["approved", "skipped", "none", "written"].includes(j.state) && j.row.state !== "gone").length;

  /* ── 7.1 · which lines are engraved, and what the text is ── */
  async function classify(row) {
    const job = ensureJob(row); const sp = row.spec;
    if (!sp.engraveCandidate) { setNone(job, "no personalisation, message or note"); return job; }
    const entry = Master.entryFor(sp.designSku); const engravable = !entry || entry.engravable !== false;
    job.state = "classify"; row.engrave = { needed: false, state: "classify" };
    let r = null;
    try { r = await agentCall("engraveIntent", { order: row.order.receiptId, sku: sp.designSku, title: row.line.title, form: sp.form, quantity: sp.quantity, engravable, personalization: sp.personalization, buyerMessage: sp.buyerMessage, staffNote: sp.staffNote, messages: sp.messages }, { label: `Claude reads the words of ${row.order.receiptId}` }); }
    catch (e) { r = { skipped: e.message }; }
    if (!r || r.skipped || r.error) {
      // the classifier is unavailable: a person decides, with the verbatim personalisation as the proposal
      job.text = sp.personalization.join("\n"); job.lines = sp.personalization.slice(); job.source = "personalization"; job.confidence = 0; job.questions = [`Claude was unavailable (${(r && (r.skipped || r.error)) || "no answer"}) — confirm the words`]; job.requests = { side: "back", font: null, handwriting: false, image: false };
      return toWords(job, "classifier unavailable");
    }
    job.text = r.text || ""; job.lines = job.text.split(/\r?\n/).map(s => s.trim()).filter(Boolean); job.source = r.source; job.quote = r.sourceQuote; job.confidence = r.confidence; job.requests = r.requests; job.questions = r.questions || []; job.claudeReasoning = r.reasoning || null;
    agent({ engrave: true }, "ENGRAVE", `${row.order.receiptId} · ${sp.designSku}: Claude reads ${r.engrave ? `"${job.text.replace(/\n/g, " / ")}" from ${r.source} (${Math.round(r.confidence * 100)}%)` : "no engraving"}${job.questions.length ? ` · ${job.questions.length} question(s)` : ""}`, { reason: r.reasoning || null });
    if (!r.engrave) { if (job.confidence < (+S.settings.engraveConfidence || 0.8) || job.questions.length) return toWords(job, "Claude is not sure there is no engraving"); setNone(job, "Claude: no engraving requested"); return job; }
    const reqBad = job.requests && ((job.requests.side && !["back", "unspecified"].includes(job.requests.side)) || job.requests.font || job.requests.handwriting || job.requests.image);
    if (job.confidence < (+S.settings.engraveConfidence || 0.8)) return toWords(job, `confidence ${Math.round(job.confidence * 100)}% is under ${Math.round((+S.settings.engraveConfidence || 0.8) * 100)}%`);
    if (job.questions.length) return toWords(job, "Claude has questions");
    if (reqBad) return toWords(job, `the customer asks for ${job.requests.side !== "back" && job.requests.side !== "unspecified" ? "the " + job.requests.side : ""}${job.requests.font ? " font " + job.requests.font : ""}${job.requests.handwriting ? " handwriting" : ""}${job.requests.image ? " an image" : ""} — engraving is back only, in the house font only`);
    if (!engravable) return toWords(job, `${sp.designSku} is marked not engravable in the Master tab`);
    if (!job.lines.length) return toWords(job, "Claude returned no text");
    return setReady(job);
  }
  function setNone(job, why) { job.state = "none"; job.reason = why; job.row.engrave = { needed: false, state: "none", reason: why, approved: true }; Review.remove("eng:" + job.key); RunCtl.poke(); return job; }
  function toWords(job, why) { job.state = "words"; job.reason = why; job.row.engrave = { needed: true, state: "words", text: job.text, approved: false, reason: why }; Review.add({ kind: "engraveWords", key: "eng:" + job.key, row: job.row, job, why }); RunCtl.poke(); return job; }
  async function setReady(job) {
    await loadFonts();
    if (!F_.ok) { job.state = "blocked"; job.reason = "Source Sans 3 font files are missing"; job.row.engrave = { needed: true, state: "blocked", text: job.text, approved: false, reason: job.reason }; Review.add({ kind: "fontMissing", key: "eng:" + job.key, row: job.row, job, why: F_.error }); return job; }
    const cov = G.glyphCoverage(F_.Regular, job.lines.join("\n"));
    if (!cov.ok) { job.state = "words"; job.reason = `characters Source Sans 3 lacks: ${cov.missing.join(" ")}`; job.missing = cov.missing; job.row.engrave = { needed: true, state: "words", text: job.text, approved: false, reason: job.reason }; Review.add({ kind: "notRepresentable", key: "eng:" + job.key, row: job.row, job, why: job.reason }); return job; }
    job.state = "ready"; job.reason = null; job.row.engrave = { needed: true, state: "ready", text: job.text, approved: false }; Review.remove("eng:" + job.key);
    Pool.update(job.copies, { engrave: true }).catch(() => {});
    RunCtl.poke();                                                        // a waiting run fits it now (the classifier answers asynchronously)
    return job;
  }
  /** A person's decision on the words (confirm / edit / no engraving), recorded with the name. */
  async function decideWords(job, { text, none, by, note }) {
    by = by || employeeName() || askEmployee(); if (!by) { toast("Set your name first", "bad"); return; }
    if (none) { setNone(job, `no engraving — decided by ${by}`); job.decision = { by, at: Date.now(), none: true }; Review.remove("eng:" + job.key); agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId}: no engraving (${by})`); Orders.render(); RunCtl.poke(); return job; }
    job.text = String(text || "").trim(); job.lines = job.text.split(/\r?\n/).map(s => s.trim()).filter(Boolean); job.decision = { by, at: Date.now(), text: job.text, note: note || null }; job.questions = []; job.requests = { side: "back", font: null, handwriting: false, image: false }; job.confidence = 1;
    if (job.text && job.text !== (job.row.spec.personalization || []).join("\n")) { try { await DesignLink.call("notes.set", { receiptId: job.row.order.receiptId, text: `${job.row.spec.staffNote ? job.row.spec.staffNote + "\n" : ""}Engrave (${by}): ${job.text.replace(/\n/g, " / ")}` }); } catch (e) { agent({ engrave: true }, "warn", `staff note not saved: ${e.message}`); } }
    agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId}: words decided by ${by}: "${job.text.replace(/\n/g, " / ")}"`);
    Review.remove("eng:" + job.key);
    await setReady(job);
    if (job.state === "ready") { const ch = job.copies.length && Pool.sheetOf(job.copies[0]); if (ch && ch.fileBase) await fitJob(job); }
    Orders.render(); RunCtl.poke(); return job;
  }
  async function classifyAll(run) {
    await loadFonts();
    const rows = Orders.rows().filter(r => r.state === "pooled" && r.spec && r.spec.engraveCandidate && (!r.engrave || r.engrave.state === "reclassify" || r.engrave.state === "classify"));
    const q = rows.slice(); let done = 0;
    await Promise.all(Array.from({ length: 3 }, async () => { while (q.length) { const r = q.shift(); try { await classify(r); } catch (e) { agent({ engrave: true }, "warn", `${r.order.receiptId}: classifier failed — ${e.message}`); toWords(ensureJob(r), e.message); } done++; } }));
    for (const r of Orders.rows()) if (r.state === "pooled" && r.spec && !r.spec.engraveCandidate && !r.engrave) r.engrave = { needed: false, state: "none", approved: true };
    Orders.render(); render();
    if (run) { run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); await RunCtl.save(run); }
    return done;
  }

  /* ── 7.2 / 7.3 · the flip and the fit, once per distinct placement (identical copies share it) ── */
  async function fitJob(job) {
    await loadFonts(); if (!F_.ok) return setReady(job);
    const poolId = job.copies[0]; const charm = Pool.charmOf(poolId); if (!charm) { job.state = "ready"; return job; }
    job.state = "fitting"; job.row.engrave.state = "fitting"; render();
    const entry = Master.entryFor(job.row.spec.designSku) || {};
    let view;
    try { view = G.backView(charm, { res: 6, upAngle: entry.upAngle == null ? undefined : +entry.upAngle }); }
    catch (e) {
      job.state = "blocked"; job.reason = e.message; job.flipError = e; job.row.engrave.state = "blocked"; job.row.engrave.reason = job.reason;
      agent({ engrave: true }, "warn", `${job.row.order.receiptId} · ${job.row.spec.designSku}: ${job.reason}`);
      Review.add({ kind: "flipFailed", key: "eng:" + job.key, row: job.row, job, why: job.reason, checks: e.checks, images: e.images });
      RunCtl.stopIfRunning("a back flip failed its checks", "See the Review tab: re-run, mark the SKU not engravable, or hold the order."); return job;
    }
    job.view = view;
    job.mask = G.engraveMask(view, { marginMm: +S.settings.engraveMarginMm || 0.8, keepOut: charm.backKeepOut || [] });
    agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId} · ${job.row.spec.designSku}: back flipped and verified (pixels ${(view.detail.pixelDiff * 100).toFixed(3)}%, area ${view.detail.areaF} = ${view.detail.areaB}, ${view.detail.dropped} front detail member(s) dropped, ${view.cutMembers.length} cut) · hoop ${Math.round(view.upAngle)}° → up`);
    let fit = G.fitText(job.lines, F_.Regular, job.mask, fitOpts());
    if (fit.ok && fit.weight === "Semibold" && F_.Semibold) { const sb = G.fitText(job.lines, F_.Semibold, job.mask, fitOpts()); if (sb.ok) fit = Object.assign(sb, { weight: "Semibold" }); }
    if (!fit.ok) { job.state = "review"; job.fit = null; job.reason = fit.reason; job.row.engrave.state = "review"; agent({ engrave: true }, "warn", `${job.row.order.receiptId} · ${job.row.spec.designSku}: ${fit.reason}`); Review.add({ kind: "placement", key: "eng:" + job.key, row: job.row, job, why: fit.reason }); render(); return job; }
    fit.fittedMax = fit.size; job.fit = fit; job.fitAt = Date.now();
    // the largest that fits is the ceiling; the default is sized to how much there is to say (design §7.4)
    {
      const bw = charm.widthPt || (charm.bbox ? charm.bbox[2] - charm.bbox[0] : 0), bh = charm.heightPt || (charm.bbox ? charm.bbox[3] - charm.bbox[1] : 0);
      const sizeOpts = font => ({
        capPerEm: G.capPerEm(font), minCapMm: +S.settings.engraveMinCapMm || 1.6,
        charmMinMm: Math.min(bw, bh) * MM, charmMaxMm: Math.max(bw, bh) * MM,
        usableAreaMm2: G.area(job.mask) / (job.mask.res * job.mask.res) * MM * MM,
        advanceOf: t => font.getAdvanceWidth(t, 1, { kerning: true })
      });
      let want = G.defaultSize(job.lines, fit.fittedMax, sizeOpts(fontFor(fit.weight)));
      // the weight follows the size that will actually be cut, not the ceiling: a name that came down to 2.0 mm is
      // Semibold even though the largest that fitted was 3.7 mm. Advances differ by ~3%, so one re-fit settles it.
      const semiBelow = fitOpts().semiboldBelowMm || 2.2;
      const wantWeight = want * G.capPerEm(fontFor(fit.weight)) * MM < semiBelow ? "Semibold" : "Regular";
      if (wantWeight !== fit.weight && F_[wantWeight]) {
        const re = G.fitText(job.lines, F_[wantWeight], job.mask, fitOpts());
        if (re.ok) { re.fittedMax = re.size; fit = Object.assign(re, { weight: wantWeight }); job.fit = fit; want = G.defaultSize(job.lines, fit.fittedMax, sizeOpts(fontFor(fit.weight))); }
      }
      if (want < fit.size - 0.01) {
        const L = G.layoutLines(job.lines, fontFor(fit.weight), want, 0.18, fit.angle, fit.centre);
        if (G.verifyInk(L.cmds, job.mask).ok) {
          const capMm = want * G.capPerEm(fontFor(fit.weight)) * MM;
          job.fit = fit = Object.assign({}, fit, { size: want, capMm, layout: L, glyphs: L.glyphs, cmds: L.cmds, fittedMax: fit.fittedMax, sized: "default" });
        }
      }
      // when the honest target is well past what the mask allows, the line wants re-breaking — say so rather than clamp in silence
      if (fit.size >= fit.fittedMax - 0.01 && (job.lines || []).join(" ").trim().length > 8)
        agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId} · ${job.row.spec.designSku}: the words only fit at the largest size the area allows — a different split of the lines may read better`);
    }
    const check = G.verifyInk(fit.cmds, job.mask);                          // 7.4 · geometry: zero ink outside the eroded mask, zero in any hole
    if (!check.ok) { throw new Error(`ink outside the eroded mask after fitting (${check.outside} px) — a bug, not a review item`); }
    job.verify = { geometry: check, at: Date.now() };
    job.state = "review"; job.row.engrave.state = "review"; job.claude = null;
    agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId} · ${job.row.spec.designSku}: "${job.lines.join(" / ")}" fits at ${fit.size.toFixed(2)} pt (cap ${fit.capMm.toFixed(2)} mm, ${fit.weight}${fit.angle ? `, ${fit.angle}°` : ""}${fit.small ? ", SMALL" : ""}${fit.thin ? ", strokes under the engraver limit" : ""}) — awaiting a person`);
    Review.add({ kind: "placement", key: "eng:" + job.key, row: job.row, job });
    claudeRead(job).catch(() => {});
    render(); return job;
  }
  async function claudeRead(job) {
    if (!S.cloud.ok || !job.fit) return;
    const png = renderBack(job, 700, { grid: true }).toDataURL("image/png");
    const r = await agentCall("engraveReview", { image: png, order: job.row.order.receiptId, sku: job.row.spec.designSku, text: job.lines.join("\n"), capMm: job.fit.capMm, font: "Source Sans 3", weight: job.fit.weight, angle: job.fit.angle, small: job.fit.small }, { label: `Claude looks at the back of ${job.row.order.receiptId}` });
    if (r.skipped) { job.claude = { skipped: r.skipped }; render(); return; }
    job.claude = { legible: !!r.legible, notes: r.notes || "", concerns: r.concerns || [] };
    agent({ engrave: true }, r.legible ? "ENGRAVE" : "warn", `${job.row.order.receiptId}: Claude ${r.legible ? "reads it fine" : "finds it hard to read"} — ${r.notes}`);
    render();
  }
  async function fitAll(run) {
    const jobs = [...items().values()].filter(j => j.state === "ready" && j.row.state !== "gone");
    for (const j of jobs) { const sh = j.copies.length && Pool.sheetOf(j.copies[0]); if (!sh || !sh.fileBase) continue; try { await fitJob(j); } catch (e) { j.state = "blocked"; j.reason = e.message; j.row.engrave.state = "blocked"; agent({ engrave: true }, "warn", `${j.row.order.receiptId}: ${e.message}`); Review.add({ kind: "flipFailed", key: "eng:" + j.key, row: j.row, job: j, why: e.message }); } }
    if (run) { run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); await RunCtl.save(run); }
    render(); return jobs.length;
  }
  function invalidate(row, why) { const j = items().get(row.key); if (!j) return; if (j.state === "approved" || j.state === "written" || j.state === "review" || j.state === "ready") { j.previous = { text: j.text, fit: j.fit, approvedBy: j.approvedBy }; j.state = "classify"; j.fit = null; j.approvedBy = null; j.approvedAt = null; j.reason = why; Review.remove("eng:" + j.key); } }

  /* ── 7.5 · the review controls ── */
  function refit(job, place) { const f = G.refitAt(job.lines, fontFor(job.fit.weight), job.mask, fitOpts(), place); if (!f.ok) return false; f.fittedMax = f.size; f.weight = job.fit.weight; f.rect = job.fit.rect; job.fit = f; job.verify = { geometry: G.verifyInk(f.cmds, job.mask), at: Date.now() }; job.nudged = true; job.claude = null; return true; }
  function nudge(job, dxMm, dyMm) { if (!job.fit) return; const c = [job.fit.centre[0] + dxMm * PT, job.fit.centre[1] + dyMm * PT]; if (!refit(job, { centre: c, angle: job.fit.angle })) toast("No room there", "bad"); render(); }
  /** The middle of the area the text may use: the centre of gravity of the solid pixels, not of the bounding box, so a
      cat's head with ears puts the name where the metal actually is. */
  function maskCentroid(m) {
    let sx = 0, sy = 0, n = 0;
    for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) if (m.bits[y * m.w + x]) { sx += x; sy += y; n++; }
    if (!n) return [m.cx, m.cy];
    return [m.ox + (sx / n + 0.5) / m.res, m.oy + (sy / n + 0.5) / m.res];
  }
  function centreText(job) { if (!job.fit) return; if (!moveTo(job, maskCentroid(job.mask))) toast("The text does not fit in the middle — left where it was", "bad"); }
  function moveTo(job, centre) { if (!job.fit) return false; const ok = refit(job, { centre, angle: job.fit.angle }); if (ok) render(); return ok; }
  function resize(job, size) { if (!job.fit) return; size = Math.min(size, job.fit.fittedMax); const L = G.layoutLines(job.lines, fontFor(job.fit.weight), size, 0.18, job.fit.angle, job.fit.centre); const v = G.verifyInk(L.cmds, job.mask); if (!v.ok) { toast("That size does not verify", "bad"); return; } job.fit = Object.assign({}, job.fit, { size, layout: L, glyphs: L.glyphs, cmds: L.cmds, capMm: size * G.capPerEm(fontFor(job.fit.weight)) * MM, small: size * G.capPerEm(fontFor(job.fit.weight)) * MM < (+S.settings.engraveMinCapMm || 1.6), metrics: G.strokeMetrics(L.cmds, 24) }); job.verify = { geometry: v, at: Date.now() }; job.claude = null; render(); }
  async function resplit(job) { const vars = G.splitVariants(job.lines); const i = (job.splitIndex || 0) + 1; const pick = vars[i % vars.length]; job.splitIndex = i; job.lines = pick; job.text = pick.join("\n"); agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId}: re-split as "${pick.join(" / ")}"`); await fitJob(job); }
  async function skip(job, by) { by = by || employeeName() || askEmployee(); if (!by) return; job.state = "skipped"; job.approvedBy = null; job.row.engrave = { needed: false, state: "skipped", text: job.text, approved: true, reason: `cut plain — skipped by ${by}` }; job.row.flag = `engraving skipped by ${by}`; Review.remove("eng:" + job.key); agent({ engrave: true }, "warn", `${job.row.order.receiptId} · ${job.row.spec.designSku}: engraving skipped by ${by} — cut plain, order flagged`); await Pool.update(job.copies, { engrave: false, engraveSkippedBy: by }); Orders.render(); render(); RunCtl.poke(); }
  function sendBack(job, why) { job.state = "words"; job.reason = why || "sent back from the placement review — a decision on the words is needed"; job.row.engrave.state = "words"; job.row.engrave.approved = false; Review.remove("eng:" + job.key); Review.add({ kind: "engraveWords", key: "eng:" + job.key, row: job.row, job, why: job.reason }); render(); Orders.render(); }
  async function approve(job, by) {
    by = by || employeeName() || askEmployee(); if (!by) { toast("An employee name is required to approve", "bad"); return; }
    if (!job.fit || !job.verify || !job.verify.geometry.ok) { toast("Nothing verified to approve", "bad"); return; }
    job.state = "approved"; job.approvedBy = by; job.approvedAt = Date.now(); job.row.engrave = Object.assign(job.row.engrave || {}, { needed: true, state: "approved", approved: true, text: job.text, approvedBy: by });
    Review.remove("eng:" + job.key);
    agent({ engrave: true }, "ENGRAVE", `${job.row.order.receiptId} · ${job.row.spec.designSku}: placement approved by ${by} (${job.fit.size.toFixed(2)} pt, cap ${job.fit.capMm.toFixed(2)} mm${job.nudged ? ", nudged" : ""})`);
    render(); Orders.render();
    try { await writeBacks(job); } catch (e) { job.state = "review"; job.row.engrave.state = "review"; job.row.engrave.approved = false; job.reason = "back file failed: " + e.message; agent({ engrave: true }, "warn", `${job.row.order.receiptId}: ${job.reason}`); Review.add({ kind: "placement", key: "eng:" + job.key, row: job.row, job, why: job.reason }); render(); }
    RunCtl.poke();
  }

  /* ── 7.6 · back files, one per piece, only after approval ── */
  function renderBack(job, px, { grid = false, hatch = true } = {}) {
    const view = job.view, mask = job.mask, fit = job.fit; const cv = document.createElement("canvas");
    const bb = view.members.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
    const pad = 3 * PT; const w = bb[2] - bb[0] + 2 * pad, h = bb[3] - bb[1] + 2 * pad; const k = px / Math.max(w, h);
    cv.width = Math.round(w * k); cv.height = Math.round(h * k); const ctx = cv.getContext("2d");
    ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height);
    const tx = (x, y) => [(x - bb[0] + pad) * k, (bb[3] + pad - y) * k];
    if (grid) { ctx.strokeStyle = "rgba(0,0,0,.09)"; ctx.lineWidth = 1; const x0 = Math.floor((bb[0] - pad) * MM), x1 = Math.ceil((bb[2] + pad) * MM); for (let mm = x0; mm <= x1; mm++) { const p = tx(mm * PT, 0); ctx.beginPath(); ctx.moveTo(p[0], 0); ctx.lineTo(p[0], cv.height); ctx.stroke(); } const y0 = Math.floor((bb[1] - pad) * MM), y1 = Math.ceil((bb[3] + pad) * MM); for (let mm = y0; mm <= y1; mm++) { const p = tx(0, mm * PT); ctx.beginPath(); ctx.moveTo(0, p[1]); ctx.lineTo(cv.width, p[1]); ctx.stroke(); } ctx.fillStyle = "rgba(0,0,0,.4)"; ctx.font = `${Math.max(9, k * 2)}px sans-serif`; ctx.fillText("1 mm grid", 4, 12); }
    if (hatch && mask) { // the eroded mask as a light tint: where text may go
      const img = ctx.createImageData(cv.width, cv.height); const d = img.data;
      for (let py = 0; py < cv.height; py++) for (let pxx = 0; pxx < cv.width; pxx++) { const x = bb[0] - pad + pxx / k, y = bb[3] + pad - py / k; if (G.at(mask, x, y)) { const i = (py * cv.width + pxx) * 4; d[i] = 231; d[i + 1] = 237; d[i + 2] = 223; d[i + 3] = 255; } }
      const off = document.createElement("canvas"); off.width = cv.width; off.height = cv.height; off.getContext("2d").putImageData(img, 0, 0); ctx.globalCompositeOperation = "multiply"; ctx.drawImage(off, 0, 0); ctx.globalCompositeOperation = "source-over";
    }
    const base = document.createElement("canvas"); base.width = cv.width; base.height = cv.height;   // grid + mask tint, drawn once
    base.getContext("2d").drawImage(cv, 0, 0);
    for (const m of view.members) { ctx.beginPath(); P.pathToCanvas(ctx, m, tx); ctx.strokeStyle = m.original === view.cutMembers[0] || m.original === job.view.cutMembers.find(c => c === Pool.charmOf(job.copies[0]).outline) ? "rgba(190,40,40,.95)" : "rgba(60,60,60,.9)"; ctx.lineWidth = Math.max(1, 0.5 * k); ctx.stroke(); }
    if (fit) { ctx.fillStyle = "#111"; for (const g of fit.glyphs) { ctx.beginPath(); let cur = null; for (const c of g.cmds) { if (c.type === "M") { const p = tx(c.x, c.y); ctx.moveTo(p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "L") { const p = tx(c.x, c.y); ctx.lineTo(p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "C") { const a = tx(c.x1, c.y1), b = tx(c.x2, c.y2), p = tx(c.x, c.y); ctx.bezierCurveTo(a[0], a[1], b[0], b[1], p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "Q") { const a = tx(c.x1, c.y1), p = tx(c.x, c.y); ctx.quadraticCurveTo(a[0], a[1], p[0], p[1]); cur = [c.x, c.y]; } else ctx.closePath(); } ctx.fill("nonzero"); } }
    const outline = document.createElement("canvas"); outline.width = cv.width; outline.height = cv.height;   // …and the charm itself
    outline.getContext("2d").drawImage(cv, 0, 0);
    /** Repaint: the static layers, then the text — the fitted one, or a provisional one while a hand is moving it. */
    const glyphsOf = g => { ctx.fillStyle = "#111"; for (const gl of g) { ctx.beginPath(); for (const c of gl.cmds) { if (c.type === "M") { const p = tx(c.x, c.y); ctx.moveTo(p[0], p[1]); } else if (c.type === "L") { const p = tx(c.x, c.y); ctx.lineTo(p[0], p[1]); } else if (c.type === "C") { const a = tx(c.x1, c.y1), b2 = tx(c.x2, c.y2), d2 = tx(c.x, c.y); ctx.bezierCurveTo(a[0], a[1], b2[0], b2[1], d2[0], d2[1]); } else if (c.type === "Q") { const a = tx(c.x1, c.y1), d2 = tx(c.x, c.y); ctx.quadraticCurveTo(a[0], a[1], d2[0], d2[1]); } else if (c.type === "Z") ctx.closePath(); } ctx.fill("nonzero"); } };
    cv._paint = (prov) => {
      ctx.clearRect(0, 0, cv.width, cv.height); ctx.drawImage(outline, 0, 0);
      const gl = prov && prov.glyphs ? prov.glyphs : (job.fit ? job.fit.glyphs : []);
      if (gl.length) glyphsOf(gl);
      if (prov && prov.centre) {                                              // guides: the charm's own centre lines, lit when the text is on them
        const c = prov.centre, snapX = Math.abs(c[0] - mask.cx) < 0.35 * PT, snapY = Math.abs(c[1] - mask.cy) < 0.35 * PT;
        ctx.save(); ctx.setLineDash([4, 4]); ctx.lineWidth = 1;
        ctx.strokeStyle = snapX ? "rgba(160,110,30,.9)" : "rgba(0,0,0,.18)"; const px1 = tx(mask.cx, bb[1]), px2 = tx(mask.cx, bb[3]); ctx.beginPath(); ctx.moveTo(px1[0], px1[1]); ctx.lineTo(px2[0], px2[1]); ctx.stroke();
        ctx.strokeStyle = snapY ? "rgba(160,110,30,.9)" : "rgba(0,0,0,.18)"; const py1 = tx(bb[0], mask.cy), py2 = tx(bb[2], mask.cy); ctx.beginPath(); ctx.moveTo(py1[0], py1[1]); ctx.lineTo(py2[0], py2[1]); ctx.stroke();
        ctx.restore();
      }
    };
    cv._map = { bb, pad, k, tx, base, outline };
    return cv;
  }
  function renderFront(charm, px) { const cv = document.createElement("canvas"); const b = charm.bbox, pad = 3 * PT; const w = b[2] - b[0] + 2 * pad, h = b[3] - b[1] + 2 * pad, k = px / Math.max(w, h); cv.width = Math.round(w * k); cv.height = Math.round(h * k); const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height); const tx = (x, y) => [(x - b[0] + pad) * k, (b[3] + pad - y) * k]; P.drawSegments(ctx, charm.members, tx, k); ctx.beginPath(); P.pathToCanvas(ctx, charm.outline, tx); ctx.strokeStyle = "rgba(190,40,40,.9)"; ctx.lineWidth = Math.max(1, 0.5 * k); ctx.stroke(); return cv; }
  async function writeBacks(job) {
    const charm0 = Pool.charmOf(job.copies[0]); const src = sourceOf(charm0.sourceId); const view = job.view, fit = job.fit;
    const rel = fit.glyphs.map(g => ({ cmds: g.cmds.map(c => { const o = { type: c.type }; if (c.type !== "Z") { o.x = c.x - view.cx; o.y = c.y - view.cy; } if (c.type === "C" || c.type === "Q") { o.x1 = c.x1 - view.cx; o.y1 = c.y1 - view.cy; } if (c.type === "C") { o.x2 = c.x2 - view.cx; o.y2 = c.y2 - view.cy; } return o; }) }));
    const bySheet = new Map();
    for (const poolId of job.copies) { const sh = Pool.sheetOf(poolId); if (!sh) continue; if (!bySheet.has(sh)) bySheet.set(sh, []); bySheet.get(sh).push(poolId); }
    if (!bySheet.size) throw new Error("no sheet holds these pieces yet");
    const png = renderBack(job, 500, { grid: false, hatch: false }); const pngBlob = await new Promise(r => png.toBlob(r, "image/png"));
    job.backs = [];
    for (const [sh, poolIds] of bySheet) {
      sh.backPool = sh.backPool || [];
      for (const poolId of poolIds) {
        const p = B.pool.rows.get(poolId) || {}; const copy = p.copy || 1;
        const built = await P.buildBackFile({ charm: charm0, parsed: src.parsed, cutMembers: view.cutMembers, cx: view.cx, cy: view.cy, angleDeg: view.angleDeg, padPt: 5 * PT, glyphs: rel, view: S.settings.backFileView || "asSeenFromBack", title: `${job.row.order.receiptId} · ${job.row.spec.designSku} · back`, meta: { poolId, order: job.row.order.receiptId, sku: job.row.spec.designSku, copy, text: job.text, font: "Source Sans 3", weight: fit.weight, sizePt: fit.size, capMm: fit.capMm, angle: fit.angle, approvedBy: job.approvedBy, approvedAt: job.approvedAt, upAngle: view.upAngle, flipChecks: view.checks } });
        const verified = await verifyBackFile(built.bytes, job);                 // 7.4 · flip integrity re-run on the written, re-parsed file
        if (!verified.ok) throw new Error(`the written back file did not re-verify (${verified.why})`);
        const name = `${sh.fileBase}_back_${job.row.order.receiptId}_${job.row.spec.designSku}_${copy}`;
        let ai = null, pngUp = null;
        if (S.cloud.ok && sh.folderPath) { ai = await uploadBytes(`${sh.folderPath}/back/${name}.ai`, built.bytes, "application/illustrator", `Saving back ${copy}`); pngUp = await uploadBytes(`${sh.folderPath}/back/${name}.png`, pngBlob, "image/png"); }
        const rec = { poolId, sheetId: sh.sheetId, setId: sh.setId || null, runId: sh.runId || null, order: job.row.order.receiptId, transactionId: job.row.line.transactionId, sku: job.row.spec.designSku, copy, text: job.text, lines: job.lines, font: "Source Sans 3", weight: fit.weight, sizePt: +fit.size.toFixed(3), capMm: +fit.capMm.toFixed(3), box: fit.rect ? [fit.rect.x0, fit.rect.y0, fit.rect.x1, fit.rect.y1].map(v => +v.toFixed(2)) : null, centre: fit.centre.map(v => +v.toFixed(2)), angle: fit.angle, small: !!fit.small, thin: !!fit.thin, metrics: fit.metrics, flipChecks: view.checks, flipDetail: view.detail, verified: { geometry: job.verify.geometry, file: verified }, review: job.claude, approvedBy: job.approvedBy, approvedAt: job.approvedAt, nudged: !!job.nudged, decision: job.decision || null, source: job.source, sourceQuote: job.quote, confidence: job.confidence, view: S.settings.backFileView || "asSeenFromBack", reference: built.reference, outputs: { ai: ai && { path: ai.path, url: ai.url }, png: pngUp && { path: pngUp.path, url: pngUp.url } }, name, pageWPt: built.wPt, pageHPt: built.hPt };
        sh.backPool = sh.backPool.filter(b => b.poolId !== poolId).concat([rec]); job.backs.push(rec);
        if (S.cloud.ok) await api("charmNestLibrary", { op: "backPut", back: rec }).catch(e => agent({ engrave: true }, "warn", `back record: ${e.message}`));
        agent({ metal: sh.metal, engrave: true }, "ENGRAVE", `Back file written and re-verified: ${name}.ai (${built.reference.redrawn ? "cut reference redrawn from the exact transformed paths" : "original cut bytes under the mirror matrix"})`);
      }
      if (S.cloud.ok && sh.sheetId) await api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, backPool: sh.backPool } }).catch(() => {});
      await sheetBackOutputs(sh).catch(e => agent({ metal: sh.metal }, "warn", `back index: ${e.message}`));
    }
    job.state = "written"; job.row.engrave.state = "written";
    await Pool.update(job.copies, { engrave: true, engraveApprovedBy: job.approvedBy, state: "engraved" });
    render();
  }
  /** Parse the written back file and compare its cut geometry with the verified view (mirrored back for the front-coordinates variant). */
  async function verifyBackFile(bytes, job) {
    try {
      const parsed = await P.parseSource(bytes, "back.ai");
      const g = P.groupCharms(parsed, { minPt: 4 });
      if (!g.charms.length) return { ok: false, why: "no cut outline found in the written file" };
      const c = g.charms.reduce((a, b) => (b.bbox[2] - b.bbox[0]) * (b.bbox[3] - b.bbox[1]) > (a.bbox[2] - a.bbox[0]) * (a.bbox[3] - a.bbox[1]) ? b : a);
      const cut = c.members.filter(m => m === c.outline || G.isCutLine(m));
      const bbW = cut.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const bbV = job.view.members.reduce((a, s) => [Math.min(a[0], s.bbox[0]), Math.min(a[1], s.bbox[1]), Math.max(a[2], s.bbox[2]), Math.max(a[3], s.bbox[3])], [Infinity, Infinity, -Infinity, -Infinity]);
      const res = 6; const fw = G.makeFrame([0, 0, bbW[2] - bbW[0], bbW[3] - bbW[1]], res, (bbW[2] - bbW[0]) / 2, 1), fv = G.makeFrame([0, 0, bbV[2] - bbV[0], bbV[3] - bbV[1]], res, (bbV[2] - bbV[0]) / 2, 1);
      if (Math.abs(fw.w - fv.w) > 2 || Math.abs(fw.h - fv.h) > 2) return { ok: false, why: `written extent ${fw.w}×${fw.h} px differs from the verified back ${fv.w}×${fv.h} px` };
      const frame = fv; const W = G.raster(cut.map(m => G.transformSeg(m, G.translate(-bbW[0], -bbW[1]))), frame); const V = G.raster(job.view.members.map(m => G.transformSeg(m, G.translate(-bbV[0], -bbV[1]))), frame);
      const front = (S.settings.backFileView || "asSeenFromBack") === "frontCoordinates";
      const diff = G.diffFraction(W, front ? G.flipX(V) : V);
      const textOk = parsed.segments.concat(parsed.nested).filter(s => s.kind === "path" && s.fill && !s.stroke).length >= 1;
      return { ok: diff <= 0.01 && textOk, why: diff > 0.01 ? `cut geometry differs by ${(diff * 100).toFixed(2)}%` : (!textOk ? "no engraving paths in the file" : null), pixelDiff: diff, textPaths: textOk };
    } catch (e) { return { ok: false, why: e.message }; }
  }
  /** back/back-index.pdf and back/back-report.json for a sheet, rebuilt whenever a back is added. */
  async function sheetBackOutputs(sh) {
    if (!S.cloud.ok || !sh.folderPath || !(sh.backPool || []).length) return;
    const { PDFDocument, StandardFonts, rgb } = PDFLib;
    const doc = await PDFDocument.create(); const font = await doc.embedFont(StandardFonts.Helvetica);
    const per = 6; const backs = sh.backPool;
    for (let i = 0; i < backs.length; i += per) {
      const page = doc.addPage([612, 792]); page.drawText(`${sh.fileBase} · engraved backs ${i + 1}–${Math.min(backs.length, i + per)} of ${backs.length}`, { x: 36, y: 756, size: 12, font });
      for (let j = 0; j < per && i + j < backs.length; j++) {
        const b = backs[i + j]; const col = j % 2, row = Math.floor(j / 2); const x = 36 + col * 280, y = 720 - row * 230;
        try { if (b.outputs && b.outputs.png && b.outputs.png.url) { const pngBytes = await (await fetch(b.outputs.png.url)).arrayBuffer(); const img = await doc.embedPng(pngBytes); const s = Math.min(150 / img.width, 150 / img.height); page.drawImage(img, { x, y: y - 150, width: img.width * s, height: img.height * s }); } } catch (_) { /* thumbnail optional */ }
        const lines = [`${b.order} · ${b.sku} · copy ${b.copy}`, `"${String(b.text).replace(/\n/g, " / ")}"`, `${b.font} ${b.weight} · ${b.sizePt} pt · cap ${b.capMm} mm${b.angle ? ` · ${b.angle}°` : ""}${b.small ? " · SMALL" : ""}`, `approved by ${b.approvedBy || "—"} ${b.approvedAt ? new Date(b.approvedAt).toLocaleString() : ""}`];
        lines.forEach((t, k) => page.drawText(t.slice(0, 60), { x, y: y - 165 - k * 12, size: 8, font, color: rgb(0.1, 0.1, 0.1) }));
      }
    }
    const pdf = await doc.save({ useObjectStreams: false });
    const report = backs.map(b => ({ poolId: b.poolId, order: b.order, sku: b.sku, copy: b.copy, text: b.text, font: b.font, weight: b.weight, sizePt: b.sizePt, capMm: b.capMm, box: b.box, centre: b.centre, angle: b.angle, flipChecks: b.flipChecks, verified: b.verified, review: b.review, approvedBy: b.approvedBy, approvedAt: b.approvedAt, file: b.outputs && b.outputs.ai && b.outputs.ai.path }));
    const [idx, rep] = await Promise.all([uploadBytes(`${sh.folderPath}/back/back-index.pdf`, pdf, "application/pdf", "Saving back index"), uploadBytes(`${sh.folderPath}/back/back-report.json`, new TextEncoder().encode(JSON.stringify(report, null, 1)), "application/json")]);
    sh.backOutputs = { index: { path: idx.path, url: idx.url }, report: { path: rep.path, url: rep.url }, count: backs.length };
    if (sh.sheetId) await api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, backOutputs: sh.backOutputs } }).catch(() => {});
  }

  /* ── the Engraving tab ── */
  const EG = { tab: null, focus: null, chosen: false };                                   // which tab is open and which placement is in front
  function render() {
    LiveStrip.render();
    const v = document.getElementById("engraveView"); if (!v || v.classList.contains("hidden")) return;
    const jobs = [...items().values()].filter(j => j.row.state !== "gone");
    const words = jobs.filter(j => j.state === "words" || j.state === "blocked"), queue = jobs.filter(j => j.state === "review"), done = jobs.filter(j => ["approved", "written", "skipped"].includes(j.state));
    // One screen, three tabs, one thing in front of you at a time: the words a person has to settle, the placements to
    // approve, and what has already been decided. The counts are the tabs, so what is left is never more than a glance.
    // the screen opens on whatever has work; once a person picks a tab it stays picked, empty or not
    if (!EG.tab || (!EG.chosen && ((EG.tab === "words" && !words.length && queue.length) || (EG.tab === "place" && !queue.length && words.length)))) EG.tab = queue.length ? "place" : words.length ? "words" : "done";
    const tab = EG.tab;
    const focus = queue.find(j2 => j2.key === EG.focus) || queue[0] || null;
    const tabBtn = (id, label, n, cls) => `<button class="egTab${tab === id ? " on" : ""}" data-tab="${id}">${label}<b class="${cls}">${n}</b></button>`;
    v.innerHTML = `<div class="ordBar egBar">
        ${tabBtn("place", "Placements", queue.length, "info")}${tabBtn("words", "Words", words.length, "warn")}${tabBtn("done", "Decided", done.length, "ok")}
        <span class="spacer"></span><span class="pill ${F_.ok ? "ok" : "bad"}">${F_.ok ? "Source Sans 3" : "Source Sans 3 missing"}</span><span class="mono" style="font-size:11.5px">${esc(employeeName() || "— set your name in the Design Station tab —")}</span></div>
      <div class="egPane grow"${tab === "place" ? "" : " hidden"}><div class="rvList" id="egQueue"></div>
        <div class="egNext" id="egNext"></div></div>
      <div class="egPane grow scroll"${tab === "words" ? "" : " hidden"}><div class="rvList" id="egWords"></div></div>
      <div class="egPane grow scroll"${tab === "done" ? "" : " hidden"}><div id="egBacks"></div></div>`;
    v.querySelectorAll(".egTab").forEach(b => b.onclick = () => { EG.tab = b.dataset.tab; EG.chosen = true; render(); });
    if (tab === "words") {
      const w = v.querySelector("#egWords"); for (const j2 of words) w.appendChild(Review.card(Review.items().find(i2 => i2.key === "eng:" + j2.key) || { kind: j2.state === "blocked" ? "flipFailed" : "engraveWords", key: "eng:" + j2.key, row: j2.row, job: j2, why: j2.reason }));
      if (!words.length) w.innerHTML = `<div class="libEmpty">nothing waiting</div>`;
    }
    if (tab === "place") {
      const q = v.querySelector("#egQueue");
      if (focus) { const c = placementCard(focus, queue.length); c.classList.add("full"); q.appendChild(c); }
      else q.innerHTML = `<div class="libEmpty">no placement to review</div>`;
      // what is coming: the order and the words, so the list and the picture are the same thing
      const rail = v.querySelector("#egNext");
      const rest = queue.filter(j2 => j2 !== focus);
      rail.innerHTML = rest.length ? `<span class="lbl">up next</span>` + rest.slice(0, 12).map(j2 => `<button class="egChip" data-key="${esc(j2.key)}" title="${esc(j2.row.spec.designSku)} · ${esc(j2.lines.join(" / "))}"><b>${esc(j2.row.order.receiptId)}</b><span>${esc(j2.lines.join(" / ").slice(0, 22))}</span></button>`).join("") + (rest.length > 12 ? `<span class="lbl">+${rest.length - 12}</span>` : "") : "";
      rail.querySelectorAll(".egChip").forEach(b => b.onclick = () => { EG.focus = b.dataset.key; render(); });
    }
    if (tab === "done") {
      const bk = v.querySelector("#egBacks"); const sheets = allSheets().filter(sh => (sh.backPool || []).length);
      const lnk = (o, t) => o && o.url ? ` · <a href="${esc(o.url)}" target="_blank" rel="noopener">${t}</a>` : "";
      bk.innerHTML = sheets.length ? sheets.map(sh => `<div class="section" style="margin-top:6px">${esc(sh.fileBase || labelOf(sh.metal))} · ${sh.backPool.length} back${sh.backPool.length === 1 ? "" : "s"}${sh.backOutputs ? lnk(sh.backOutputs.index, "back-index.pdf") + lnk(sh.backOutputs.report, "back-report.json") : ""}</div>
        <div class="backPool">${sh.backPool.map(b => `<div class="bp">${b.outputs && b.outputs.png && b.outputs.png.url ? `<img src="${esc(b.outputs.png.url)}" alt="" loading="lazy">` : ""}<span class="t">${esc((b.lines || [b.text || ""]).join(" / "))}</span><span class="m">${esc(b.order || "")}${b.capMm ? ` · cap ${(+b.capMm).toFixed(2)} mm` : ""}${b.approvedBy ? ` · ${esc(b.approvedBy)}` : ""}</span>${b.outputs && b.outputs.ai && b.outputs.ai.url ? `<a class="m" href="${esc(b.outputs.ai.url)}" target="_blank" rel="noopener">${esc(b.name || "back")}.ai</a>` : ""}</div>`).join("")}</div>`).join("") : `<div class="libEmpty">nothing decided yet</div>`;
    }
  }
  /** The placement review card: front and back side by side, the mask hatch, the text as it will be cut, the controls. */
  function placementCard(job, remaining) {
    const it = Review.items().find(i => i.key === "eng:" + job.key) || { kind: "placement", key: "eng:" + job.key, row: job.row, job };
    const card = el("div", "rvItem"); card.dataset.kind = "placement"; card.tabIndex = 0;
    const r = job.row, sp = r.spec, f = job.fit;
    const done = (Engrave.reviewedCount ? Engrave.reviewedCount() : 0) + 1;
    // One card, one order, one screen: the back is the work and the right column is everything you need to judge it.
    const pct = Math.round((job.confidence != null ? job.confidence : 0) * 100);
    const conf = job.source ? `<span class="conf ${pct >= 80 ? "" : pct >= 60 ? "mid" : "low"}" title="how sure Claude is that these are the words to cut, read from ${esc(SOURCE_LABEL[job.source] || job.source)}${job.quote ? ` — “${esc(job.quote)}”` : ""}">${pct}% sure</span>` : "";
    const row2 = (t, v) => v && v !== "—" ? `<dt>${t}</dt><dd>${esc(v)}</dd>` : "";
    card.innerHTML = `<div class="rh"><span class="kind">${done} of ${done + Math.max(0, remaining - 1)}</span><span class="ttl">${esc(r.order.receiptId)}</span><span class="sub">${esc(sp.designSku)}${sp.form ? " · " + esc(sp.form) : ""}${sp.size ? " · " + esc(sp.size) : ""}${job.copies.length > 1 ? ` · ${job.copies.length} copies` : ""}</span>${conf}${f && f.small ? `<span class="small" title="the cap height is under the engraver minimum in Settings">SMALL · cap ${f.capMm.toFixed(2)} mm</span>` : ""}${f && f.thin ? `<span class="small" title="the thinnest stroke is under the engraver limit">THIN STROKES</span>` : ""}</div>
      <div class="placeView">
        <div class="pvMain"><div class="backHost"></div>
          <div class="ctl">${f ? `<button class="btn sage sm" data-a="approve">Approve <b class="k">A</b></button>
            <span class="grp" title="nudge the text by 0.25 mm — the arrow keys do the same"><button class="btn ghost sm" data-a="left" title="nudge left 0.25 mm" aria-label="nudge left">◀</button><button class="btn ghost sm" data-a="down" title="nudge down 0.25 mm" aria-label="nudge down">▼</button><button class="btn ghost sm" data-a="up" title="nudge up 0.25 mm" aria-label="nudge up">▲</button><button class="btn ghost sm" data-a="right" title="nudge right 0.25 mm" aria-label="nudge right">▶</button><button class="btn ghost sm" data-a="centre" title="put the text in the middle of the area it may use">Centre</button></span>
            <span class="sizer"><input type="range" min="${(0.5 * f.fittedMax).toFixed(2)}" max="${f.fittedMax.toFixed(2)}" step="0.05" value="${f.size.toFixed(2)}" data-a="resize" title="text size" aria-label="text size"><b class="mono" data-cap>${f.capMm.toFixed(2)} mm</b></span>` : ""}
            <span class="rest">${f ? `<button class="btn ghost sm" data-a="resplit" title="try a different split of the words across lines">Re-split</button>` : ""}<button class="btn ghost sm" data-a="skip" title="cut this charm with no engraving">Skip <b class="k">S</b></button><button class="btn ghost sm" data-a="back" title="send this back to the words step">Send back</button></span></div>
          <div class="help">drag the text to move it · shift-drag to resize from the centre · arrows nudge 0.25 mm</div></div>
        <div class="pvSide">
          <div class="pvWords"><span class="lbl">Words on the back</span>${esc(job.lines.join(" / ")) || "—"}</div>
          <div class="frontHost"></div>
          <dl class="meta">${row2("Customer", (sp.personalization || []).join(" / "))}${row2("Buyer msg", sp.buyerMessage)}${row2("Staff note", sp.staffNote)}${job.decision ? `<dt>Decided by</dt><dd>${esc(job.decision.by)}</dd>` : ""}</dl>
          ${job.claude ? `<div class="claude">${job.claude.skipped ? `Claude could not look at the render: ${esc(job.claude.skipped)}` : `<b>${job.claude.legible ? "Reads clearly" : "Hard to read"}</b> — ${esc(job.claude.notes)}`}</div>` : `<div class="claude" style="opacity:.6">Claude is looking at the rendered back…</div>`}
          ${f ? `<details class="pvNums"><summary>the numbers</summary><dl class="meta"><dt>Size</dt><dd>${f.size.toFixed(2)} pt · cap ${f.capMm.toFixed(2)} mm · ${esc(f.weight)}${f.angle ? ` · ${f.angle}°` : ""}</dd><dt>Strokes</dt><dd>min stem ${f.metrics ? f.metrics.strokeMm.toFixed(2) : "?"} mm · min gap ${f.metrics && f.metrics.gapMm ? f.metrics.gapMm.toFixed(2) : "—"} mm</dd><dt>Flip</dt><dd>${Object.entries(job.view.checks).map(([k, ok]) => `${esc(k)} ${ok ? "✓" : "✗"}`).join(" · ")}</dd><dt>Geometry</dt><dd>${job.verify && job.verify.geometry.ok ? `no ink outside the allowed area (${job.verify.geometry.total} px checked)` : "NOT verified"}</dd></dl></details>` : `<div class="why">${esc(job.reason || "no fit")}</div>`}
        </div></div>`;
    const charm = Pool.charmOf(job.copies[0]);
    card.querySelector(".frontHost").appendChild(renderFront(charm, 148));
    // the back preview is drawn to the box it is actually given, and redrawn when that box changes: no fixed number,
    // nothing cut off on a short laptop screen, nothing left blurry after the window is resized
    const backHost = card.querySelector(".backHost");
    let mounted = 0, raf = 0;
    const wire = bc => {
      let drag = null;
      // the pointer is captured by the canvas, so the handlers live and die with this canvas: every card used to add
      // another pair of listeners to the window and none of them was ever removed
      bc.addEventListener("pointerdown", e => { if (!job.fit) return; bc.setPointerCapture(e.pointerId); drag = { x: e.clientX, y: e.clientY, c: job.fit.centre.slice(), size: job.fit.size, resize: e.shiftKey }; bc.classList.add("drag"); e.preventDefault(); });
      bc.addEventListener("pointermove", e => {
        if (!drag) return;
        const rect = bc.getBoundingClientRect(), kx = bc.width / rect.width;
        const dx = (e.clientX - drag.x) * kx / bc._map.k, dy = -(e.clientY - drag.y) * kx / bc._map.k;
        if (drag.resize) {
          // shift-drag grows and shrinks about the centre the text already has, so it never wanders while being sized
          const span = Math.max(24, bc.height / 3) * kx / bc._map.k;
          drag.pendingSize = Math.max(0.5, Math.min(job.fit.fittedMax, drag.size * (1 + dy / span)));
          const L = G.layoutLines(job.lines, fontFor(job.fit.weight), drag.pendingSize, 0.18, job.fit.angle, drag.c);
          bc._paint({ glyphs: L.glyphs, centre: drag.c });
        } else {
          const c = [drag.c[0] + dx, drag.c[1] + dy];
          // within a third of a millimetre of the charm's own centre line, the text takes it
          if (Math.abs(c[0] - job.mask.cx) < 0.35 * PT) c[0] = job.mask.cx;
          if (Math.abs(c[1] - job.mask.cy) < 0.35 * PT) c[1] = job.mask.cy;
          drag.pending = c;
          const L = G.layoutLines(job.lines, fontFor(job.fit.weight), job.fit.size, 0.18, job.fit.angle, c);
          bc._paint({ glyphs: L.glyphs, centre: c });
        }
      });
      bc.addEventListener("pointerup", () => {
        if (!drag) return;
        const p = drag.pending, sz = drag.pendingSize, was = drag.resize; drag = null; bc.classList.remove("drag");
        if (was) { if (sz != null) resize(job, sz); else bc._paint(); }
        else if (p) { if (!moveTo(job, p)) { toast("No room there — kept the previous position", "bad"); bc._paint(); } }
        else bc._paint();
      });
      bc.addEventListener("pointercancel", () => { drag = null; bc.classList.remove("drag"); bc._paint(); });
    };
    const mountBack = () => {
      if (!job.view || !backHost.isConnected) return;
      const r = backHost.getBoundingClientRect();
      // side by side, the box says how big; stacked on a narrow screen the box has no height of its own, so half
      // the window is the ceiling and the charm keeps its shape either way
      const stacked = getComputedStyle(backHost).flexGrow === "0";
      const room = stacked ? Math.min(r.width || 320, (window.innerHeight || 700) * 0.46) : Math.min(r.width || 320, r.height || 320);
      const px = Math.round(Math.min(640, Math.max(200, room - 4)));
      if (!px || Math.abs(px - mounted) < 12) return;
      mounted = px; backHost.textContent = "";
      const bc = renderBack(job, px, { grid: true }); backHost.appendChild(bc); wire(bc);
    };
    requestAnimationFrame(mountBack);
    if (window.ResizeObserver) { const ro = new ResizeObserver(() => { if (raf) return; raf = requestAnimationFrame(() => { raf = 0; mountBack(); }); }); ro.observe(backHost); card._ro = ro; }
    const capOut = card.querySelector("[data-cap]");
    card.querySelectorAll("[data-a]").forEach(b => { const a = b.dataset.a; if (a === "resize") { b.oninput = () => { if (capOut && f && f.capMm && f.size) capOut.textContent = (f.capMm * (+b.value) / f.size).toFixed(2) + " mm"; resize(job, +b.value); }; return; } b.onclick = () => { if (a === "approve") approve(job); else if (a === "left") nudge(job, -0.25, 0); else if (a === "right") nudge(job, 0.25, 0); else if (a === "up") nudge(job, 0, 0.25); else if (a === "down") nudge(job, 0, -0.25); else if (a === "centre") centreText(job);
      else if (a === "resplit") resplit(job); else if (a === "skip") skip(job); else if (a === "back") sendBack(job); }; });
    card.addEventListener("keydown", e => { if (e.target.tagName === "INPUT") return; const k = e.key.toLowerCase(); if (k === "a") { e.preventDefault(); approve(job); } else if (k === "s") { e.preventDefault(); skip(job); } else if (e.key === "ArrowLeft") { e.preventDefault(); nudge(job, -0.25, 0); } else if (e.key === "ArrowRight") { e.preventDefault(); nudge(job, 0.25, 0); } else if (e.key === "ArrowUp") { e.preventDefault(); nudge(job, 0, 0.25); } else if (e.key === "ArrowDown") { e.preventDefault(); nudge(job, 0, -0.25); } });
    setTimeout(() => card.focus(), 30);
    void it;
    return card;
  }
  return { loadFonts, classify, classifyAll, fitJob, fitAll, approve, nudge, resize, resplit, skip, sendBack, decideWords, invalidate, render, placementCard, renderBack, renderFront, pendingCount, reviewedCount, items, jobOf, ensureJob, setReady, writeBacks, verifyBackFile, sheetBackOutputs, fonts: F_ };
})();

/* ═══ 22 · Sets — one run, one date, one folder, one numbering across materials ═══ */
const Sets = window.Sets = (() => {
  const byRun = () => B.sets;
  /** The set for a run key: allocated once per run in a Firestore transaction (per date, across materials). */
  async function ensure(runKey) {
    if (byRun().has(runKey)) return byRun().get(runKey);
    const day = today();
    let set;
    if (S.cloud.ok) { const r = await api("charmNestLibrary", { op: "setAllocate", day, runId: runKey }, { label: "Numbering the set" }); set = { setId: r.setId, seq: r.seq, day: r.day || day, runId: runKey, name: O.setLabel(r.seq), folder: O.setFolder(r.day || day, r.seq), orders: {}, sheetIds: [], materials: [], labelFiles: [], status: "open" }; }
    else { const k = "cn.setseq." + day; const seq = (+localStorage.getItem(k) || 0) + 1; localStorage.setItem(k, String(seq)); set = { setId: O.setId(day, seq), seq, day, runId: runKey, name: O.setLabel(seq), folder: O.setFolder(day, seq), orders: {}, sheetIds: [], materials: [], labelFiles: [], status: "open", offline: true }; }
    byRun().set(runKey, set); if (B.run && B.run.runId === runKey) { B.run.setId = set.setId; B.run.day = set.day; }
    agent({ run: runKey }, "cloud", `Set ${set.name} allocated for ${set.day} → ${set.folder}`);
    return set;
  }
  const setOfSheet = sh => sh.runId ? byRun().get(sh.runId) || null : null;
  /** The QR label, 145 × 145 pt, the print page's exact geometry (QR 85 pt at 3,3 · label 9 pt bold at 1,93 · "Notes:" at 92,0.5), ECC M. */
  async function renderLabelPng(payload, label, scale) {
    const k = scale || 8; const cv = document.createElement("canvas"); cv.width = Math.round(145 * k); cv.height = Math.round(145 * k);
    const ctx = cv.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, cv.width, cv.height);
    const holder = document.createElement("div"); holder.style.cssText = "position:absolute;left:-9999px;top:0"; document.body.appendChild(holder);
    let ecc = "M";
    try {
      const make = level => { holder.innerHTML = ""; return new QRCode(holder, { text: payload, width: Math.round(85 * k), height: Math.round(85 * k), correctLevel: level }); };
      try { make(QRCode.CorrectLevel.M); } catch (e) { make(QRCode.CorrectLevel.L); ecc = "L"; }
      await new Promise(r => setTimeout(r, 30));
      const qcv = holder.querySelector("canvas"); const qimg = holder.querySelector("img");
      if (qcv) ctx.drawImage(qcv, 3 * k, 3 * k, 85 * k, 85 * k); else if (qimg) { await new Promise(r => { if (qimg.complete) r(); else qimg.onload = r; }); ctx.drawImage(qimg, 3 * k, 3 * k, 85 * k, 85 * k); }
    } finally { holder.remove(); }
    ctx.fillStyle = "#000"; ctx.textBaseline = "top"; ctx.font = `bold ${9 * k}px Helvetica, Arial, sans-serif`; ctx.fillText(label, 1 * k, 93 * k, 143 * k); ctx.fillText("Notes:", 92 * k, 0.5 * k);
    const blob = await new Promise(r => cv.toBlob(r, "image/png")); return { blob, dataUrl: cv.toDataURL("image/png"), ecc };
  }
  /** After a sheet is saved: its label(s) beside it, the sheet record and the set record kept current, pool rows → written. */
  async function onSheetSaved(sh, items, outputs) {
    const set = setOfSheet(sh); if (!set) return;
    const byId = new Map(items.map(c => [c.id, c]));
    const placed = sh.placements.map(p => byId.get(p.id)).filter(Boolean);
    const ids = [...new Set(placed.map(c => String(c.order || c.id).split("/")[0]))];
    const metalDS = O.CARD_TO_METAL[sh.metal] || sh.metal;
    const parts = O.safeChunks(ids, metalDS, 1000, 50, 8);
    const files = [];
    for (const [i, slice] of parts.entries()) {
      const payload = O.encodeOrderList(slice, metalDS);
      const label = `${METAL_TAG[sh.metal]} · ${set.name} · Sheet ${sh.sheetIndex || sh.page}${parts.length > 1 ? ` [${i + 1}/${parts.length}]` : ""} · ${slice.length} order${slice.length === 1 ? "" : "s"}`;
      const png = await renderLabelPng(payload, label);
      let up = null; if (S.cloud.ok && sh.folderPath) up = await uploadBytes(`${sh.folderPath}/${sh.fileBase}_label${parts.length > 1 ? `_${i + 1}of${parts.length}` : ""}.png`, png.blob, "image/png", "Saving the sheet label");
      files.push({ path: up && up.path, url: up && up.url, dataUrl: up ? null : png.dataUrl, sheet: sh.fileBase, sheetId: sh.sheetId, metal: metalDS, part: i + 1, parts: parts.length, orders: slice, payload, ecc: png.ecc, label });
    }
    sh.label = { files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload, ecc: f.ecc, label: f.label })), orders: ids };
    sh.setId = set.setId; sh.setSeq = set.seq;
    if (S.cloud.ok && sh.sheetId) await api("charmNestLibrary", { op: "putSheet", sheet: { id: sh.sheetId, label: sh.label, setId: set.setId, setSeq: set.seq, sheetIndex: sh.sheetIndex, orders: ids, runId: sh.runId, poolIds: placed.map(c => c.poolId).filter(Boolean) } }).catch(() => {});
    // pool rows and order lines
    const poolIds = placed.map(c => c.poolId).filter(Boolean);
    if (poolIds.length) await Pool.update(poolIds, { sheetId: sh.sheetId, setId: set.setId, state: "written", sheetName: sh.fileBase });
    for (const row of Orders.rows()) { if (!row.poolIds.length) continue; const allPlaced = row.poolIds.every(pid => { const p = B.pool.rows.get(pid); return p && p.sheetId; }); if (allPlaced && row.state === "pooled") row.state = "written"; }
    // the set record
    if (!set.sheetIds.includes(sh.sheetId)) set.sheetIds.push(sh.sheetId);
    if (!set.materials.includes(sh.metal)) set.materials.push(sh.metal);
    for (const c of placed) { const rid = String(c.order || "").split("/")[0]; if (!rid || !c.orderInfo) continue; const o = set.orders[rid] = set.orders[rid] || { lines: {}, held: null }; const ln = o.lines[c.orderInfo.transactionId] = o.lines[c.orderInfo.transactionId] || { transactionId: c.orderInfo.transactionId, sku: c.orderInfo.sku, copies: [] }; if (!ln.copies.some(x => x.poolId === c.poolId)) ln.copies.push({ copy: c.orderInfo.copy, sheetId: sh.sheetId, sheet: sh.fileBase, poolId: c.poolId, backPoolId: null }); }
    set.labelFiles = set.labelFiles.filter(f => f.sheetId !== sh.sheetId).concat(sh.label.files.map(f => Object.assign({ sheetId: sh.sheetId }, f)));
    set.status = "nesting";
    await save(set);
    agent({ metal: sh.metal, run: sh.runId }, "cloud", `${sh.fileBase}: ${files.length} label${files.length === 1 ? "" : "s"} saved (${ids.length} order${ids.length === 1 ? "" : "s"}) · set ${set.name} now ${set.sheetIds.length} sheet(s)`);
    Orders.render();
  }
  async function save(set) { if (!S.cloud.ok || set.offline) return; const orders = {}; for (const [rid, o] of Object.entries(set.orders)) orders[rid] = { held: o.held || null, lines: Object.values(o.lines) }; await api("charmNestLibrary", { op: "setUpdate", setId: set.setId, patch: { runId: set.runId, day: set.day, seq: set.seq, name: set.name, folder: set.folder, materials: set.materials, sheetIds: set.sheetIds, orders, status: set.status, labels: set.labels || null, labelFiles: set.labelFiles.map(f => ({ path: f.path, url: f.url, sheet: f.sheet, sheetId: f.sheetId, part: f.part, parts: f.parts, orders: f.orders, label: f.label })), committed: set.committed || null, refused: set.refused || null, committedAt: set.committedAt || null, backCount: set.backCount || 0 } }).catch(e => agent({ run: set.runId }, "warn", `set record: ${e.message}`)); }
  const sheetsOf = set => allSheets().filter(sh => sh.setId === set.setId);
  /** Which orders of the set travel, which are held and why (design §5.4, §8.4). */
  function evaluate(set) {
    const rows = Orders.rows(); const byOrder = new Map();
    for (const r of rows) { if (!byOrder.has(r.order.receiptId)) byOrder.set(r.order.receiptId, []); byOrder.get(r.order.receiptId).push(r); }
    const out = { committable: [], held: {}, gone: [] };
    for (const [rid, lines] of byOrder) {
      if (lines.every(l => l.state === "gone")) { out.gone.push(rid); continue; }
      const ev = O.evaluateOrder(lines.map(l => ({ key: l.key, spec: l.spec, state: l.state, reason: l.reason, problems: l.problems, engrave: l.engrave })));
      const holdByPerson = lines.find(l => l.hold);
      if (ev.committable && !holdByPerson) out.committable.push(rid); else out.held[rid] = holdByPerson ? { line: holdByPerson.key, why: holdByPerson.hold } : ev.held;
      if (set.orders[rid]) set.orders[rid].held = out.held[rid] || null;
    }
    return out;
  }
  /** labels/Set-K_labels.pdf (one page per sheet label), Set-K_manifest.pdf, set.json. */
  async function finalize(set) {
    const { PDFDocument, StandardFonts, rgb } = PDFLib;
    const files = set.labelFiles.slice().sort((a, b) => a.sheet.localeCompare(b.sheet) || a.part - b.part);
    const labels = await PDFDocument.create();
    for (const f of files) { const bytes = f.url ? await (await fetch(f.url)).arrayBuffer() : dataUrlToBytes(f.dataUrl); const img = await labels.embedPng(bytes); const page = labels.addPage([145, 145]); page.drawImage(img, { x: 0, y: 0, width: 145, height: 145 }); }
    const ev = evaluate(set);
    const man = await PDFDocument.create(); const font = await man.embedFont(StandardFonts.Helvetica), bold = await man.embedFont(StandardFonts.HelveticaBold);
    let page = man.addPage([612, 792]), y = 756;
    const ansi = t => String(t).replace(/→/g, "->").replace(/↔/g, "<->").replace(/·/g, "-").replace(/[^\x20-\x7E\xA0-\xFF]/g, "?");   // Helvetica (WinAnsi) only
    const line = (t, opts = {}) => { if (y < 48) { page = man.addPage([612, 792]); y = 756; } page.drawText(ansi(t).slice(0, 110), Object.assign({ x: 36, y, size: 9.5, font }, opts)); y -= opts.size ? opts.size + 4 : 13; };
    line(`${set.name} · ${set.day} · run ${set.runId}`, { size: 15, font: bold }); line(`${set.sheetIds.length} sheet(s) · materials ${set.materials.map(m => labelOf(m)).join(", ")} · ${Object.keys(set.orders).length} order(s) · ${ev.committable.length} committable · ${Object.keys(ev.held).length} held · ${ev.gone.length} gone`); y -= 6;
    line("Orders and sheets", { font: bold, size: 11 });
    for (const [rid, o] of Object.entries(set.orders).sort()) { const copies = Object.values(o.lines).flatMap(l => l.copies.map(c => `${l.sku}${l.copies.length > 1 ? "#" + c.copy : ""}→${c.sheet}`)); line(`${rid}  ${ev.held[rid] ? "HELD: " + (ev.held[rid].why || "") + "  " : ""}${copies.join("  ")}`); }
    y -= 6; line("Engraving", { font: bold, size: 11 });
    const backs = sheetsOf(set).flatMap(sh => (sh.backPool || []).map(b => `${sh.fileBase}: ${b.order} ${b.sku} #${b.copy} "${String(b.text).replace(/\n/g, " / ")}" ${b.sizePt} pt · ${b.approvedBy || "?"}`));
    if (backs.length) backs.forEach(b => line(b)); else line("no engraving in this set");
    y -= 6; line("Labels", { font: bold, size: 11 }); files.forEach(f => line(`${f.label}  ${f.path || "(not uploaded)"}`));
    const json = { setId: set.setId, runId: set.runId, day: set.day, seq: set.seq, name: set.name, folder: set.folder, materials: set.materials, sheets: sheetsOf(set).map(sh => ({ sheetId: sh.sheetId, name: sh.fileBase, metal: sh.metal, sheetIndex: sh.sheetIndex, folder: sh.folderPath, orders: sh.label ? sh.label.orders : [], placements: sh.placements.length, backs: (sh.backPool || []).map(b => ({ poolId: b.poolId, order: b.order, sku: b.sku, copy: b.copy, text: b.text, approvedBy: b.approvedBy, file: b.outputs && b.outputs.ai && b.outputs.ai.path })), verification: sh.verification && { ok: sh.verification.ok }, outputs: sh.cloud || null })), orders: Object.fromEntries(Object.entries(set.orders).map(([rid, o]) => [rid, { held: ev.held[rid] || null, lines: Object.values(o.lines) }])), held: ev.held, gone: ev.gone, labels: files.map(f => ({ sheet: f.sheet, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload, path: f.path })), approvals: backs.length, generatedAt: new Date().toISOString() };
    set.backCount = sheetsOf(set).reduce((n, sh) => n + (sh.backPool || []).length, 0);
    if (S.cloud.ok && !set.offline) {
      const [lp, mp, jp] = await Promise.all([uploadBytes(`${set.folder}/labels/${set.name}_labels.pdf`, await labels.save({ useObjectStreams: false }), "application/pdf", "Saving the set's labels PDF"), uploadBytes(`${set.folder}/${set.name}_manifest.pdf`, await man.save({ useObjectStreams: false }), "application/pdf", "Saving the manifest"), uploadBytes(`${set.folder}/set.json`, new TextEncoder().encode(JSON.stringify(json, null, 1)), "application/json")]);
      set.labels = { pdf: { path: lp.path, url: lp.url }, manifest: { path: mp.path, url: mp.url }, json: { path: jp.path, url: jp.url }, files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet })) };
    } else set.labels = { files: files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet })) };
    set.status = "labelled"; await save(set);
    agent({ run: set.runId }, "cloud", `${set.name}: ${files.length} label(s) collected in labels/${set.name}_labels.pdf · manifest and set.json saved`);
    return set;
  }
  /** §8.4 · the real lock, the preview, the commit — held and refused orders stay open on the station. */
  async function commit(set, run) {
    const ev = evaluate(set);
    const committable = ev.committable.filter(rid => set.orders[rid]);
    for (const [rid, h] of Object.entries(ev.held)) Review.add({ kind: "heldOrder", key: "held:" + rid, rid, why: `${rid} held — ${h.why || "unresolved line"}`, line: h.line });
    if (!committable.length) { agent({ run: run.runId }, "warn", `${set.name}: no committable order — ${Object.keys(ev.held).length} held, ${ev.gone.length} gone`); set.status = "complete-with-holds"; set.committed = []; set.refused = Object.entries(ev.held).map(([id, h]) => ({ id, reason: h.why })); await save(set); return { completed: [], refused: set.refused }; }
    if (!set.labels || !(set.labels.files || []).length) throw new Error("no saved labels for the set");
    await DesignLink.ensure();
    const sel = await DesignLink.call("ui.select", { receiptIds: committable }, { timeoutMs: 120000 });
    const refused = (sel.refused || []).slice();
    const ids = sel.selected.filter(id => committable.includes(id));
    if (!ids.length) { set.status = "complete-with-holds"; set.committed = []; set.refused = refused; await save(set); agent({ run: run.runId }, "warn", `${set.name}: the station refused every selection — ${refused.map(r => r.id + " (" + r.reason + ")").join(", ")}`); return { completed: [], refused }; }
    const preview = await DesignLink.call("complete.preview", { receiptIds: ids }, { timeoutMs: 120000 });
    agent({ run: run.runId }, "DS", `Station preview: ${preview.jobs.length} label job(s) for ${ids.length} order(s)${preview.notes && preview.notes.skipped && preview.notes.skipped.length ? ` · ${preview.notes.skipped.length} with no recognised metal` : ""}`);
    const labels = { setId: set.setId, folder: `${set.folder}/labels`, files: set.labels.files.map(f => ({ path: f.path, url: f.url, sheet: f.sheet })), pdf: set.labels.pdf ? set.labels.pdf.url : null };
    const r = await DesignLink.call("complete.commit", { receiptIds: ids, labels, runId: run.runId, setId: set.setId, sheetIds: set.sheetIds, backCount: set.backCount || 0, completedBy: `Charm Sorter (${employeeName() || "operator"})` }, { timeoutMs: 180000 });
    set.committed = r.completed; set.refused = refused.concat(r.refused || []); set.committedAt = Date.now(); set.status = set.refused.length || Object.keys(ev.held).length ? "complete-with-holds" : "complete";
    for (const row of Orders.rows()) if (r.completed.includes(row.order.receiptId) && (row.state === "written" || row.state === "labelled" || row.state === "noDesign")) row.state = "committed";
    await Pool.update([...B.pool.rows.keys()].filter(id => r.completed.includes(B.pool.rows.get(id).orderId)), { state: "committed", committedAt: Date.now() });
    await save(set);
    agent({ run: run.runId }, "DS", `${set.name} committed: ${r.completed.length} order(s) marked design-complete on the station · ${set.refused.length} refused · ${Object.keys(ev.held).length} held`);
    return { completed: r.completed, refused: set.refused, held: ev.held };
  }
  async function undo(set) {
    await DesignLink.ensure(); await DesignLink.call("complete.undo", { receiptIds: (set.committed || []).slice() }, { timeoutMs: 180000 });
    set.status = "awaiting review"; set.committed = []; set.committedAt = null; await save(set);
    await Pool.update([...B.pool.rows.keys()].filter(id => (B.pool.rows.get(id) || {}).setId === set.setId), { state: "written" });
    for (const row of Orders.rows()) if (row.state === "committed") row.state = "written";
    if (B.run && B.run.setId === set.setId) { B.run.status = "review"; B.run.step = "engrave"; await RunCtl.save(B.run); RunCtl.renderBanner(); }
    agent({ run: set.runId }, "DS", `${set.name}: completion undone on the station — set back to awaiting review, every file kept`);
    Orders.render();
  }
  /** The Library's Sets view: one card per set, its sheets side by side, held orders, engraving count, label thumbnails. */
  async function renderLibrary(body) {
    body.innerHTML = `<div class="libEmpty">Loading sets…</div>`;
    try {
      const [ss, sh] = await Promise.all([api("charmNestLibrary", { op: "setList", from: document.getElementById("libFrom").value || null, to: document.getElementById("libTo").value || null, limit: 200 }), api("charmNestLibrary", { op: "listSheets", from: document.getElementById("libFrom").value || null, to: document.getElementById("libTo").value || null, limit: 500 })]);
      const sheets = sh.sheets || []; const sets = ss.sets || [];
      document.getElementById("libCount").textContent = `${sets.length} set${sets.length === 1 ? "" : "s"}`;
      if (!sets.length) { body.innerHTML = `<div class="libEmpty">No sets yet.</div>`; return; }
      body.innerHTML = "";
      for (const st of sets) {
        const mine = sheets.filter(x => x.setId === st.setId).sort((a, b) => (a.metal || "").localeCompare(b.metal || "") || (a.sheetIndex || 0) - (b.sheetIndex || 0));
        const held = Object.entries(st.orders || {}).filter(([, o]) => o.held);
        const card = el("div", "setCard");
        card.innerHTML = `<div class="sh"><span class="nm">${esc(st.name || st.setId)}</span><span class="pill ${/complete/.test(st.status) ? "ok" : st.status === "labelled" ? "info" : "neutral"}">${esc(st.status || "open")}</span><span class="mono" style="font-size:11px;color:var(--ink45)">${esc(st.day)} · run ${esc(st.runId || "—")}</span><span>${mine.length} sheet(s) · ${(st.materials || []).map(m => labelOf(m)).join(", ")}</span><span>${Object.keys(st.orders || {}).length} order(s)</span><span>Engraving · ${st.backCount || mine.reduce((n, x) => n + (x.backCount || 0), 0)}</span><span class="spacer"></span>${st.labels && st.labels.pdf ? `<a class="btn ghost xs" href="${st.labels.pdf.url}" target="_blank" rel="noopener">labels PDF</a>` : ""}${st.labels && st.labels.manifest ? `<a class="btn ghost xs" href="${st.labels.manifest.url}" target="_blank" rel="noopener">manifest</a>` : ""}${st.labels && st.labels.json ? `<a class="btn ghost xs" href="${st.labels.json.url}" target="_blank" rel="noopener">set.json</a>` : ""}${/complete/.test(st.status) ? `<button class="btn ghost xs" data-undo="${esc(st.setId)}">Undo set</button>` : ""}</div>
          <div class="sheetsRow">${mine.map(r => `<div class="libCard" data-m="${r.metal}" data-id="${r.id}"><div class="h"><span class="nm">${esc(r.folder || r.id)}</span></div>${r.preview ? `<img class="pv" crossorigin="anonymous" src="${r.preview}" loading="lazy" alt="">` : `<div class="pv ph">no preview</div>`}<div class="m"><span><b>${r.placedCount}</b>/${r.charmCount}</span><span><b>${Math.round((r.density || 0) * 100)}%</b></span><span>${(r.orders || []).length} orders</span>${r.backCount ? `<span>✎ ${r.backCount}</span>` : ""}<span class="pill ${r.status === "complete" ? "ok" : "bad"}" style="padding:2px 7px">${esc(r.status)}</span></div></div>`).join("") || "<div class='libEmpty'>no sheets recorded</div>"}</div>
          ${held.length ? `<div class="holds"><b>Held:</b> ${held.map(([rid, o]) => `${esc(rid)} — ${esc(o.held.why || "")}`).join(" · ")}</div>` : ""}
          ${st.refused && st.refused.length ? `<div class="holds"><b>Refused by the station:</b> ${st.refused.map(r => `${esc(r.id)} — ${esc(r.reason)}`).join(" · ")}</div>` : ""}
          <div class="labels">${(st.labelFiles || []).map(f => f.url ? `<img src="${f.url}" title="${esc(f.label || f.sheet)}" data-big="${f.url}" alt="">` : "").join("")}</div>`;
        card.querySelectorAll(".libCard").forEach(x => x.onclick = () => openLibrarySheet(x.dataset.id));
        card.querySelectorAll("[data-big]").forEach(img => img.onclick = () => { const d = document.createElement("dialog"); d.className = "wide"; d.innerHTML = `<div class="dlg"><div class="dlgHead"><h3>${esc(img.title)}</h3><div class="right"><button class="btn ghost xs">Close</button></div></div><div class="dlgBody" style="display:grid;place-items:center"><img class="labelBig" src="${img.dataset.big}" alt=""></div></div>`; d.querySelector("button").onclick = () => d.close(); d.addEventListener("close", () => d.remove()); document.body.appendChild(d); d.showModal(); });
        const ub = card.querySelector("[data-undo]"); if (ub) ub.onclick = async () => { if (!confirm(`Undo the completion of ${st.name}? The orders return to the station's list; every file is kept.`)) return; const local = [...byRun().values()].find(x => x.setId === st.setId) || Object.assign({ orders: {}, sheetIds: st.sheetIds || [], materials: st.materials || [], labelFiles: st.labelFiles || [] }, st); byRun().set(local.runId || st.setId, local); try { await undo(local); toast(`${st.name} undone`, "ok"); renderLibrary(body); } catch (e) { toast(e.message, "bad", 6000); } };
        body.appendChild(card);
      }
    } catch (e) { body.innerHTML = `<div class="libEmpty">Could not load sets: ${esc(e.message)}</div>`; }
  }
  return { ensure, onSheetSaved, finalize, commit, undo, evaluate, save, renderLibrary, renderLabelPng, sheetsOf, setOfSheet, byRun };
})();

/* ═══ 23 · RunCtl — the two halves, the record, resume, stop, Auto/Manual ═══ */
const RunCtl = window.RunCtl = (() => {
  const PAUSE_AFTER = new Set(["pull", "pool", "nest", "engrave", "labels"]);
  let waiter = null, reviewWaiter = null, autoTimer = null;
  const run = () => B.run;
  async function save(r) { r = r || B.run; if (!r) return; r.updatedAt = Date.now(); if (!S.cloud.ok) return; const rec = Object.assign({}, r, { lines: r.lines || {}, sheets: r.sheets || {}, holds: r.holds || {}, errors: (r.errors || []).slice(-50) }); delete rec._wait; await api("charmNestLibrary", { op: "runPut", run: rec }).catch(e => agent({ run: r.runId }, "warn", `run record: ${e.message}`)); renderBanner(); }
  function newRun(mode) { const day = today(); return { runId: `run-${day}-${uid()}`, day, setId: null, step: "pull", status: "running", mode: mode || S.settings.runMode || "manual", startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; }
  async function start(opts = {}) {
    if (B.run && ["running", "review", "paused"].includes(B.run.status)) { toast("A run is already open — stop it or let it finish", "bad"); return B.run; }
    const r = newRun(opts.mode); B.run = r;
    agent({ run: r.runId }, "DS", `Run ${r.runId} started (${r.mode} mode)`);
    await save(r); renderBanner();
    loop().catch(e => { stop(e.message, "Fix the cause and press Resume."); });
    return r;
  }
  async function loop() {
    const r = B.run;
    while (r && r.status === "running") {
      const step = r.step;
      renderBanner();
      const outcome = await doStep(r, step);
      if (r.status !== "running") break;
      if (outcome && outcome.goto) { r.step = outcome.goto; await save(r); continue; }
      const next = O.nextStep(step);
      if (!next) { r.status = "complete"; r.finishedAt = Date.now(); await save(r); renderBanner(); onComplete(r); break; }
      r.step = next;
      if (r.mode === "manual" && PAUSE_AFTER.has(step)) { r.status = "paused"; await save(r); renderBanner(); agent({ run: r.runId }, "DS", `Paused after ${step} — press Next step (${next})`); break; }
      await save(r);
    }
  }
  async function next() { const r = B.run; if (!r || r.status !== "paused") return; r.status = "running"; await save(r); loop().catch(e => stop(e.message, "Fix the cause and press Resume.")); }
  async function doStep(r, step) {
    switch (step) {
      case "pull": { const rows = await Orders.pull(r, { silent: true }); if (!rows.length) { stop("no orders match the pull rule", "Change the pull rule on the Orders tab (or in Settings) and start again."); return; } return; }
      case "claim": { await Orders.claim([...new Set(Orders.rows().map(x => x.order.receiptId))]); r.claimedAt = Date.now(); return; }
      case "pool": { const n = await Pool.addAll(r); if (!n) { stop("nothing could be pooled — every line needs a decision", "Resolve the items in Review, then Resume."); return; } Engrave.classifyAll(r).catch(e => agent({ run: r.runId }, "warn", `classifier: ${e.message}`)); return; }
      case "plan": { for (const m of METALS) { const pg = activePage(m.key); if (!pg.charms.some(c => c.poolId)) continue; const sat = computeSaturation(pg); agent({ metal: m.key, run: r.runId }, "info", `${labelOf(m.key)}: ${sat.count} piece(s) need ${fmt.pct(sat.totalNeeded / sat.usable)} of the plate — ${sat.recommend ? "over the " + fmt.pct(sat.rho.cap) + " ceiling, the overflow goes to a second sheet" : "under the " + fmt.pct(sat.rho.cap) + " ceiling"}`); } return; }
      case "nest": { await nestAll(r); return; }
      case "checkpoint": { const set = await Sets.ensure(r.runId); set.status = "awaiting review"; await Sets.save(set); r.setId = set.setId; r.status = "running"; await save(r); return; }
      case "engrave": { await Engrave.classifyAll(r); await Engrave.fitAll(r); await waitForReview(r); return; }
      case "revalidate": { const v = await Orders.revalidate(r, "before labels"); if (v.changed.length && [...Engrave.items().values()].some(j => j.state === "classify")) { agent({ run: r.runId }, "warn", `${v.changed.length} order(s) changed — back to engraving`); return { goto: "engrave" }; } return; }
      case "labels": { const set = await Sets.ensure(r.runId); await Sets.finalize(set); return; }
      case "commit": {
        if (S.settings.autoCommit === "off" && !r.commitRequested) { r.status = "paused"; r.awaitCommit = true; await save(r); renderBanner(); agent({ run: r.runId }, "DS", "Auto-commit is off — press Commit set when ready"); return; }
        const v = await Orders.revalidate(r, "before commit");
        if (v.changed.length && [...Engrave.items().values()].some(j => j.state === "classify")) { agent({ run: r.runId }, "warn", `${v.changed.length} order(s) changed before the commit — back to engraving`); r.commitRequested = false; return { goto: "engrave" }; }
        const set = await Sets.ensure(r.runId); const res = await Sets.commit(set, r); r.committed = res.completed; r.refused = res.refused; r.holds = res.held || {}; return;
      }
      case "complete": { try { await Orders.unclaim([...new Set(Orders.rows().map(x => x.order.receiptId))]); } catch (_) {} return; }
      default: return;
    }
  }
  /** Nest every card that holds this run's pool charms; wait until every sheet of the run (overflow included) is written, verified and saved. */
  function nestAll(r) {
    return new Promise((resolve, reject) => {
      const pages = allSheets().filter(pg => pg.runId === r.runId && pg.charms.some(c => c.poolId && !c.excluded));
      if (!pages.length) { stop("no sheet holds pooled charms", "Resolve the Review items, then Resume."); return resolve(); }
      waiter = { r, resolve, reject };
      for (const pg of pages) if (pg.status === "ready" || pg.status === "idle") startNest(pg); else if (["complete", "partial"].includes(pg.status) && pg.persisted) pg.persisted.then(() => onSheetDone(pg));
      onSheetDone(null);
    });
  }
  function onSheetDone(sh, err) {
    const r = B.run; if (!r) return;
    if (sh && sh.runId === r.runId) {
      r.sheets[sh.sheetId || sh.metal + "-" + sh.page] = { metal: sh.metal, page: sh.page, status: sh.status, verified: !!(sh.verification && sh.verification.ok), placed: sh.placements.length, rejects: sh.rejects.length, fileBase: sh.fileBase || null, error: err ? err.message : null };
      if (err) { stop(`${sheetName(sh)}: ${err.message}`, "Fix the cause (see the card's log), then Resume — the sheet nests again."); return; }
      if (["complete", "partial"].includes(sh.status) && sh.verification && !sh.verification.ok) { stop(`${sheetName(sh)} verification flagged`, "Open the sheet report; re-nest that card, then Resume."); return; }
      if (sh.endedBy === "stopped") { stop(`${sheetName(sh)} nesting was stopped by the operator`, "Press Nest on that card, then Resume."); return; }
      save(r).catch(() => {});
    }
    if (!waiter || waiter.r !== r) return;
    const pages = allSheets().filter(pg => pg.runId === r.runId && pg.charms.some(c => c.poolId && !c.excluded));
    const busy = pages.some(pg => ["nesting", "finishing", "queued", "ready", "idle"].includes(pg.status) || pg.dirty || (pg.persisted && !pg.persistedDone));
    for (const pg of pages) if (pg.persisted && !pg.persistedDone) pg.persisted.then(() => { pg.persistedDone = true; onSheetDone(null); }, () => { pg.persistedDone = true; onSheetDone(null); });
    if (busy) return;
    const notWritten = pages.filter(pg => !pg.fileBase);
    if (notWritten.length) { stop(`${notWritten.map(sheetName).join(", ")} not written`, "Check the cloud connection and the card's log, then Resume."); waiter = null; return; }
    const w = waiter; waiter = null; w.resolve();
  }
  function waitForReview(r) {
    return new Promise(resolve => {
      const check = () => {
        const open = [...Engrave.items().values()].filter(j => j.row.state !== "gone" && ["words", "review", "fitting", "ready", "classify", "blocked"].includes(j.state));
        const ready = open.filter(j => j.state === "ready");
        if (ready.length) { Engrave.fitAll(r).catch(() => {}).then(() => { if (reviewWaiter === check) setTimeout(check, 50); }); return; }
        if (!open.length) { if (r.status === "review") { r.status = "running"; save(r).catch(() => {}); } reviewWaiter = null; resolve(); return; }
        if (r.status !== "review") { r.status = "review"; save(r).catch(() => {}); renderBanner(); notifyPerson("Charm Sorter needs a person", `${open.length} engraving item(s) await a decision`); }
      };
      reviewWaiter = check; check();
    });
  }
  function poke() { if (reviewWaiter) setTimeout(reviewWaiter, 50); renderBanner(); LiveStrip.render(); }
  function stop(why, fix) { const r = B.run; if (!r) return; r.status = "stopped"; r.stoppedBy = why; r.fix = fix || null; r.errors.push({ t: Date.now(), why }); if (waiter && waiter.r === r) { const w = waiter; waiter = null; w.resolve(); } reviewWaiter = null; agent({ run: r.runId }, "warn", `Run stopped: ${why}${fix ? " — " + fix : ""}`); toast(`Run stopped: ${why}`, "bad", 8000); notifyPerson("Charm Sorter run stopped", why); save(r).catch(() => {}); renderBanner(); }
  function stopIfRunning(why, fix) { if (B.run && B.run.status === "running") stop(why, fix); }
  async function resume() {
    const r = B.run; if (!r || !["stopped", "paused"].includes(r.status)) return;
    if (r.awaitCommit) r.commitRequested = true;
    r.status = "running"; r.stoppedBy = null; r.fix = null; await save(r);
    if (["nest"].includes(r.step)) { for (const pg of allSheets()) if (pg.runId === r.runId && ["complete", "partial"].includes(pg.status) && pg.verification && !pg.verification.ok) sheetDirty(pg); }
    if (r.step === "pool" || r.step === "pull" || r.step === "claim") r.step = "pull";
    loop().catch(e => stop(e.message, "Fix the cause and press Resume."));
  }
  async function commitNow() { const r = B.run; if (!r) return; r.commitRequested = true; r.awaitCommit = false; if (r.status === "paused") { r.status = "running"; await save(r); loop().catch(e => stop(e.message, "Fix the cause and press Resume.")); } }
  /** Resume a run from its record after a reload or a crash: at or before pool → start over from pull (pool ids are deterministic); later → the set's sheets are restored from their records first. */
  async function pickResume() {
    if (!S.cloud.ok) { toast("Cloud offline — nothing to resume", "bad"); return; }
    const r = await api("charmNestLibrary", { op: "runList", limit: 30 });
    const open = (r.runs || []).filter(x => !["complete", "abandoned"].includes(x.status));
    if (!open.length) { toast("No open run to resume", ""); return; }
    const pick = prompt(`Open runs:\n${open.map((x, i) => `${i + 1}. ${x.runId} · ${x.step} · ${x.status} · ${x.lines} line(s) · ${new Date(x.updatedAt).toLocaleString()}${x.stoppedBy ? " · " + x.stoppedBy : ""}`).join("\n")}\n\nNumber to resume (or a to abandon one):`, "1");
    if (!pick) return;
    const m = /^a\s*(\d+)/i.exec(pick.trim()); if (m) { const x = open[+m[1] - 1]; if (x) { await api("charmNestLibrary", { op: "runPut", run: { runId: x.runId, status: "abandoned", abandonedAt: Date.now() }, merge: true }); toast(`${x.runId} abandoned`, "ok"); } return; }
    const x = open[+pick - 1]; if (!x) return;
    await resumeRun(x.runId);
  }
  async function resumeRun(runId) {
    const rr = await api("charmNestLibrary", { op: "runGet", runId }); const rec = rr.run; if (!rec) { toast("Run record not found", "bad"); return; }
    B.run = rec; rec.status = "running"; rec.stoppedBy = null; rec.fix = null;
    agent({ run: runId }, "DS", `Resuming ${runId} at step ${rec.step}`);
    if (O.stepIndex(rec.step) <= O.stepIndex("pool")) { rec.step = "pull"; }
    else {
      await Orders.pull(rec, { silent: true });                          // orders re-read through the station, lines re-interpreted
      const set = rec.setId ? await restoreRunSheets(rec) : null; if (set) Sets.byRun().set(runId, set);
      await Pool.addAll(rec);                                              // idempotent: existing pool rows are the same ids; only unplaced lines get charms
      if (rec.step === "nest" || rec.step === "checkpoint") rec.step = "nest";
    }
    await save(rec); renderBanner();
    loop().catch(e => stop(e.message, "Fix the cause and press Resume."));
  }
  async function restoreRunSheets(rec) {
    const sr = await api("charmNestLibrary", { op: "setGet", setId: rec.setId }); const sd = sr.set; if (!sd) return null;
    const set = { setId: sd.setId, seq: sd.seq, day: sd.day, runId: rec.runId, name: sd.name || O.setLabel(sd.seq), folder: sd.folder || O.setFolder(sd.day, sd.seq), orders: Object.fromEntries(Object.entries(sd.orders || {}).map(([rid, o]) => [rid, { held: o.held || null, lines: Object.fromEntries((o.lines || []).map(l => [l.transactionId, l])) }])), sheetIds: sd.sheetIds || [], materials: sd.materials || [], labelFiles: sd.labelFiles || [], labels: sd.labels || null, status: sd.status || "open", committed: sd.committed || null, refused: sd.refused || null, backCount: sd.backCount || 0 };
    const ls = await api("charmNestLibrary", { op: "listSheets", limit: 500, setId: set.setId });
    for (const slim of ls.sheets || []) {
      const d = (await api("charmNestLibrary", { op: "getSheet", id: slim.id })).sheet; if (!d) continue;
      const prim = S.sheets[d.metal]; let pg = prim.pages.find(p => p.sheetId === d.id) || (prim.pages[0].charms.length ? addPage(d.metal) : prim.pages[0]);
      pg.sheetId = d.id; pg.runId = rec.runId; pg.setId = set.setId; pg.seq = d.setSeq; pg.setDay = d.day; pg.sheetIndex = d.sheetIndex; pg.fileBase = d.fileBase; pg.folderPath = `${set.folder}/${d.fileBase}`; pg.label = d.label || null; pg.backPool = d.backPool || []; pg.backOutputs = d.backOutputs || null; pg.cloud = d.outputs ? { ai: d.outputs.ai && d.outputs.ai.url, pdf: d.outputs.pdf && d.outputs.pdf.url, labelled: d.outputs.labelled && d.outputs.labelled.url, report: d.outputs.report && d.outputs.report.url, preview: d.outputs.preview && d.outputs.preview.url } : null;
      pg.restored = true; pg.persistedDone = true;
      // the pieces: each placement's pool charm from the master copy, pinned at its cut position
      for (const p of d.placements || []) {
        const rc = (d.charms || []).find(c => c.id === p.id); if (!rc || !rc.poolId) continue;
        const pool = B.pool.rows.get(rc.poolId) || (await api("charmNestLibrary", { op: "poolGet", poolIds: [rc.poolId] })).pools[rc.poolId]; if (!pool) continue; B.pool.rows.set(rc.poolId, pool);
        const entry = Master.entryFor(pool.sku) || await Master.fetchEntry(pool.sku); if (!entry) continue;
        const src = await Pool.masterCharm(entry, pool.size); const base = src.charms[0];
        const charm = Pool.cloneCharm(base, `${src.id}:${rc.poolId}`); charm.name = rc.name; charm.order = pool.orderId; charm.poolId = rc.poolId; charm.metal = d.metal; charm.lineKey = pool.lineKey; charm.orderInfo = { receiptId: pool.orderId, transactionId: pool.transactionId, sku: pool.sku, copy: pool.copy, quantity: pool.quantity, form: pool.form, size: pool.size }; charm.pinned = { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle };
        if (!pg.charms.some(c => c.poolId === rc.poolId)) pg.charms.push(charm);
        pg.placements = pg.placements.filter(x => x.id !== charm.id).concat([{ id: charm.id, angle: p.angle, cxPt: p.cxPt, cyPt: p.cyPt, wPt: p.wPt, hPt: p.hPt, layerName: p.layer, scale: 0.975 }]);
      }
      pg.status = d.status === "complete" ? "complete" : "partial"; pg.dirty = false; pg.verification = d.verification ? Object.assign({ ok: !!d.verification.ok, geom: { ok: !!d.verification.ok, res: 6, overlapPx: 0, outsidePx: 0, overlappingPairs: [] }, render: d.verification.render || null }, d.verification) : null; pg.endedBy = d.endedBy; pg.density = d.density; pg.liveInfo = { freePt2: d.freePt2, usablePt2: d.usablePt2, placedPt2: (d.usablePt2 || 0) - (d.freePt2 || 0), pocket: d.pocket, placed: pg.placements.length, total: pg.charms.length };
      pg.persisted = Promise.resolve();
      computeSaturation(pg); renderCard(pg);
      for (const row of Orders.rows()) { if (!row.poolIds.length) continue; if (row.poolIds.every(pid => (d.poolIds || []).includes(pid) || (B.pool.rows.get(pid) || {}).sheetId)) row.state = "written"; }
      for (const b of pg.backPool) { const row = Orders.rows().find(x => x.poolIds.includes(b.poolId)); if (row) { const j = Engrave.ensureJob(row); j.state = "written"; j.text = b.text; j.lines = b.lines || String(b.text).split("\n"); j.approvedBy = b.approvedBy; j.approvedAt = b.approvedAt; j.backs.push(b); row.engrave = { needed: true, state: "written", approved: true, text: b.text, approvedBy: b.approvedBy }; } }
    }
    agent({ run: rec.runId }, "cloud", `Restored ${ls.sheets.length} sheet(s) of ${set.name} from their records`);
    return set;
  }
  function onComplete(r) {
    agent({ run: r.runId }, "ok", `Run ${r.runId} complete: ${(r.committed || []).length} order(s) committed · ${Object.keys(r.holds || {}).length} held · ${(r.refused || []).length} refused`);
    toast(`Set complete — ${(r.committed || []).length} order(s) marked design-complete`, "ok", 7000); ding && ding();
    if (S.settings.runMode === "auto" && +S.settings.autoEvery > 0) { const every = Math.max(5, +S.settings.autoEvery); clearTimeout(autoTimer); autoTimer = setTimeout(() => { if (S.settings.runMode === "auto") { clearRunState(); start({ mode: "auto" }); } }, every * 60000); agent({ bridge: true }, "DS", `Auto: the next run starts in ${every} min (never under 5, to spare the Etsy API)`); }
  }
  /** After a set is done: clear the cards and pooled lines so the next run starts clean (files and records are kept). */
  function clearRunState() { for (const m of METALS) { const prim = S.sheets[m.key]; if (prim.pages.some(p => p.charms.some(c => c.poolId))) { for (const pg of prim.pages.slice()) { if (pg.status === "nesting") stopNest(pg); pg.charms = pg.charms.filter(c => !c.poolId); pg.sheetId = null; pg.fileBase = null; pg.setId = null; pg.runId = null; } prim.pages = [prim]; prim.active = 0; prim.el = prim.cardEl; sheetDirty(prim); } } B.orders.rows = []; B.orders.byKey = new Map(); B.engrave.items = new Map(); B.review.items = []; B.pool.rows = new Map(); B.run = null; Orders.render(); Engrave.render(); Review.render(); renderBanner(); renderRail(); updateTopSub(); }
  function setMode(mode) {
    S.settings.runMode = mode === "auto" ? "auto" : "manual"; saveSettings(); renderModeBtn();
    if (mode === "auto") { agent({ bridge: true }, "DS", "Auto mode on: the sorter pulls the latest orders by the date rule and runs the whole process, stopping only for a person"); if (!B.run || ["complete", "stopped"].includes(B.run.status)) { if (B.run && B.run.status === "complete") clearRunState(); start({ mode: "auto" }).catch(e => toast(e.message, "bad")); } else if (B.run.status === "paused") { B.run.mode = "auto"; next(); } else B.run.mode = "auto"; }
    else { clearTimeout(autoTimer); if (B.run) B.run.mode = "manual"; agent({ bridge: true }, "DS", "Manual mode: every step waits for a click"); }
    renderBanner();
  }
  function renderModeBtn() { const b = document.getElementById("btnRunMode"); if (!b) return; const auto = S.settings.runMode === "auto"; b.classList.toggle("auto", auto); document.getElementById("runModeText").textContent = auto ? "Auto" : "Manual"; }
  function renderBanner() {
    const h = document.getElementById("runBanner"); if (!h) return; const r = B.run;
    if (!r) { h.classList.add("hidden"); Dock.schedule(); return; }
    h.classList.remove("hidden"); h.className = "runBanner" + (r.status === "stopped" ? " stopped" : r.status === "complete" ? " done" : r.status === "review" ? " review" : "");
    const idx = O.stepIndex(r.step);
    const steps = O.RUN_STEPS.map((s, i) => `<i class="${i < idx || r.status === "complete" ? "done" : i === idx ? (r.status === "stopped" ? "stop" : "now") : ""}" title="${s}"></i>`).join("");
    const reviewN = Review.count();
    const why = r.status === "stopped" ? `<b>Stopped:</b> ${esc(r.stoppedBy || "")}${r.fix ? ` — <span>${esc(r.fix)}</span>` : ""}` : r.status === "review" ? `<b>Waiting for a person:</b> ${reviewN} item(s) in Review` : r.status === "paused" ? (r.awaitCommit ? `<b>Ready to commit</b> — every sheet written, every engraving decided` : `<b>Paused</b> after ${esc(O.RUN_STEPS[idx - 1] || r.step)} — next: ${esc(r.step)} · <span style="opacity:.75">this run is set to Manual, so it waits at every step</span>`) : r.status === "complete" ? `<b>Complete</b> · ${(r.committed || []).length} committed · ${Object.keys(r.holds || {}).length} held` : `<b>${esc(r.step)}</b> · half ${O.HALF[r.step]} · ${r.mode}`;
    Dock.schedule();
    h.innerHTML = `<span class="step">run ${esc(r.runId.slice(-8))}</span><span class="steps">${steps}</span><span class="why">${why}${r.setId ? ` · <span class="mono">${esc(r.setId)}</span>` : ""}</span>
      ${r.status === "paused" && !r.awaitCommit ? `<button class="btn gold sm" id="rbNext">Next step ▶</button><button class="btn ghost sm" id="rbAuto" title="Stop waiting at every step: the run carries on by itself and only stops when it needs a person">Run the rest by itself</button>` : ""}${r.status === "paused" && r.awaitCommit ? `<button class="btn sage sm" id="rbCommit">Commit set</button>` : ""}${r.status === "stopped" && /sign/i.test(r.stoppedBy || "") ? `<button class="btn gold sm" id="rbConnect">Connect Etsy</button>` : ""}${r.status === "stopped" ? `<button class="btn gold sm" id="rbResume">Resume</button>` : ""}${reviewN ? `<button class="btn ghost sm" id="rbReview">Review (${reviewN})</button>` : ""}${["running", "review", "paused"].includes(r.status) ? `<button class="btn ghost sm" id="rbStop">Stop</button>` : ""}${["complete", "stopped"].includes(r.status) ? `<button class="btn ghost sm" id="rbClear">Clear run</button>` : ""}`;
    const q = id => h.querySelector("#" + id);
    if (q("rbConnect")) q("rbConnect").onclick = () => DesignLink.connectEtsy().catch(e => toast(e.message, "bad", 6000)); if (q("rbNext")) q("rbNext").onclick = () => next();
    if (q("rbAuto")) q("rbAuto").onclick = async () => { const r2 = B.run; if (!r2) return; r2.mode = "auto"; await save(r2); agent({ run: r2.runId }, "DS", "This run carries on by itself from here — it stops only when it needs a person"); next(); }; if (q("rbCommit")) q("rbCommit").onclick = () => commitNow(); if (q("rbResume")) q("rbResume").onclick = () => resume(); if (q("rbReview")) q("rbReview").onclick = () => setMode("review"); if (q("rbStop")) q("rbStop").onclick = () => stop("stopped by the operator", "Press Resume to carry on from the recorded step."); if (q("rbClear")) q("rbClear").onclick = () => { if (confirm("Clear the finished run from the cards? Files and records are kept.")) clearRunState(); };
    Orders.render();
  }
  return { start, next, resume, stop, stopIfRunning, poke, save, onSheetDone, pickResume, resumeRun, commitNow, setMode, renderBanner, renderModeBtn, clearRunState, run };
})();

/* ═══ 24 · Review — every decision a person must make ═════════════════════ */
const Review = window.Review = (() => {
  const items = () => B.review.items;
  const count = () => items().filter(it => !(it.row && it.row.state === "gone")).length;
  function add(it) { const i = items().findIndex(x => x.key === it.key); const fresh = i < 0; if (fresh) items().push(Object.assign({ t: Date.now() }, it)); else items()[i] = Object.assign(items()[i], it); if (fresh && B.run && ["review", "paused", "stopped"].includes(B.run.status)) notifyPerson("Charm Sorter needs a person", it.why || it.kind); render(); LiveStrip.render(); RunCtl.renderBanner(); }
  function remove(key) { const n = items().length; B.review.items = items().filter(x => x.key !== key); if (n !== items().length) { render(); LiveStrip.render(); RunCtl.renderBanner(); } }
  function problemText(p) { return p.kind === "needsMaterial" ? `needs material (${p.metalLabel || "none"})` : p.kind === "needsMapping" ? `option "${p.optionName}: ${p.optionValue}" not mapped` : p.kind === "unmatchedSku" ? `SKU ${p.sku || "?"}: ${p.reason}` : p.kind === "blockedSku" ? `SKU ${p.sku} blocked: ${p.reason}` : p.kind === "missingSize" ? `no design for size ${p.size || "(none)"} (have ${(p.available || []).join(", ")})` : p.kind === "oversize" ? `oversize for the ${labelOf(p.material)} plate` : p.kind; }
  /** Order-level items follow the rows' problems: added when a problem appears, removed when it is fixed. */
  function syncOrderItems() {
    const keep = new Set();
    for (const row of Orders.rows()) { if (row.state === "gone") continue; for (const p of row.problems || []) { const key = `ord:${row.key}:${p.kind}`; keep.add(key); if (!items().some(x => x.key === key)) add({ kind: p.kind, key, row, problem: p, why: problemText(p) }); } }
    B.review.items = items().filter(x => !x.key.startsWith("ord:") || keep.has(x.key));
    render(); LiveStrip.render(); RunCtl.renderBanner();
  }
  function focus(rowKey) { RV.filter = null; render(); const c = document.querySelector(`#reviewView [data-row="${CSS.escape(rowKey)}"]`); if (c) { c.scrollIntoView({ behavior: "smooth", block: "center" }); c.classList.add("pulse"); setTimeout(() => c.classList.remove("pulse"), 1300); } }
  async function repool(row) { row.problems = []; row.state = "pulled"; row.reason = null; Orders.interpretAll(); if (row.problems.length) { Orders.render(); return; } if (B.run && O.stepIndex(B.run.step) >= O.stepIndex("pool")) { try { await Pool.poolAdd(row, B.run); } catch (e) { row.state = "held"; row.reason = e.message; } if (row.state === "pooled" && row.spec.engraveCandidate) Engrave.classify(row).catch(() => {}); } syncOrderItems(); Orders.render(); renderRail(); updateTopSub(); refreshAllCards(); RunCtl.poke(); }
  const by = () => employeeName() || askEmployee();
  /** What Claude decided, in one line and one number: a reading is either text to engrave or a note to the shop. */
  function claudeVerdict(j) {
    const pct = Math.round((j.confidence || 0) * 100);
    const text = (j.text || "").trim();
    const from = SOURCE_LABEL[j.source] != null ? SOURCE_LABEL[j.source] : esc(String(j.source || ""));
    if (!text) return `<b>nothing to engrave</b> — what the customer wrote reads as a note to the shop, not words for the charm <i style="color:var(--ink45)">· ${pct}% sure it is not engraving${j.quote ? ` · "${esc(j.quote)}"` : ""}</i>`;
    return `<b>engrave this</b>${from ? ` — read from ${from}` : ""} <i style="color:var(--ink45)">· ${pct}% sure${j.quote ? ` · "${esc(j.quote)}"` : ""}</i>`;
  }
  function card(it) {
    const c = el("div", "rvItem"); c.dataset.kind = it.kind; if (it.row) c.dataset.row = it.row.key;
    const r = it.row, sp = r && r.spec, p = it.problem || {};
    const head = (kind, ttl, sub) => `<div class="rh"><span class="kind">${esc(kind)}</span><span class="ttl">${esc(ttl)}</span><span class="sub">${esc(sub || "")}</span></div>`;
    const evRow = (lbl, val) => `<div><span class="lbl">${esc(lbl)}</span>${val}</div>`;
    const orderSub = r ? `${r.order.receiptId} · ${sp && sp.designSku || r.line.sku || "no SKU"} · ${r.line.title}` : "";
    if (it.kind === "needsMaterial") {
      c.innerHTML = head("Needs material", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Station read", esc(p.metalLabel || "nothing"))}${evRow("Options", (r.line.variations || []).map(v => `<q>${esc(v.name)}: ${esc(v.value)}</q>`).join(" "))}${evRow("Title", esc(r.line.title))}</div><div class="why">${esc(it.why)}</div>
        <div class="fixes"><select data-f="mat"><option value="">pick a material…</option>${METALS.map(m => `<option value="${m.key}">${esc(m.label)}</option>`).join("")}</select><button class="btn gold sm" data-a="mat">Use it (writes a staff note)</button><button class="btn ghost sm" data-a="skip">Skip line</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      c.querySelector("[data-a=mat]").onclick = async () => { const m = c.querySelector("[data-f=mat]").value; if (!m) return; const who = by(); if (!who) return; row_material(r, m, who); };
    } else if (it.kind === "needsMapping") {
      c.innerHTML = head("Needs mapping", `${p.optionName}: ${p.optionValue}`, orderSub) + `<div class="ev">${evRow("Listing", esc(p.listingId))}${evRow("Title", esc(p.title))}${evRow("All options", (r.line.variations || []).map(v => `<q>${esc(v.name)}: ${esc(v.value)}</q>`).join(" "))}</div>
        <div class="fixes"><select data-f="field"><option value="form">form</option><option value="size">size</option><option value="chain">chain</option></select><input data-f="val" placeholder="value (necklace · earrings · charm · S · 18 inch)"><select data-f="scope"><option value="listing">this listing only</option><option value="*">every listing</option></select><button class="btn gold sm" data-a="map">Map it (remembered)</button><button class="btn ghost sm" data-a="ignore">Not relevant for this listing</button></div>`;
      c.querySelector("[data-a=map]").onclick = async () => { const field = c.querySelector("[data-f=field]").value, val = c.querySelector("[data-f=val]").value.trim(), scope = c.querySelector("[data-f=scope]").value; if (!val) return; const who = by(); if (!who) return; await api("charmNestLibrary", { op: "optionMapPut", listingId: scope === "*" ? "*" : p.listingId, optionName: p.optionName, optionValue: p.optionValue, map: { field, value: field === "size" ? val.toUpperCase() : val.toLowerCase() }, by: who }); await Orders.loadMaps(true); toast("Mapped and remembered", "ok"); for (const rr of Orders.rows()) if (rr.problems.some(x => x.kind === "needsMapping")) await repool(rr); };
      c.querySelector("[data-a=ignore]").onclick = async () => { const who = by(); if (!who) return; await api("charmNestLibrary", { op: "optionMapPut", listingId: p.listingId, optionName: p.optionName, optionValue: p.optionValue, map: { field: "ignore" }, by: who }); await Orders.loadMaps(true); await repool(r); };
    } else if (it.kind === "unmatchedSku" || it.kind === "blockedSku") {
      const skus = [...B.master.entries.keys()].sort();
      c.innerHTML = head(it.kind === "blockedSku" ? "SKU blocked" : "Unmatched SKU", p.sku || "no SKU", orderSub) + `<div class="ev">${evRow("Why", esc(p.reason || it.why))}${evRow("Listing", esc(String(r.line.listingId || "")))}${evRow("Title", esc(r.line.title))}</div>
        <div class="fixes"><input list="rvSkus" data-f="sku" placeholder="pick the charm from the master index…"><datalist id="rvSkus">${skus.map(s => `<option value="${esc(s)}">`).join("")}</datalist><button class="btn gold sm" data-a="alias">Use this SKU (alias remembered for the listing)</button><button class="btn ghost sm" data-a="nodesign">No design (remembered)</button><button class="btn ghost sm" data-a="hold">Hold order</button>${it.kind === "blockedSku" ? `<button class="btn ghost sm" data-a="master">Open Master</button>` : ""}</div>`;
      c.querySelector("[data-a=alias]").onclick = async () => { const sku = c.querySelector("[data-f=sku]").value.trim().toUpperCase(); if (!sku) return; const who = by(); if (!who) return; if (!B.master.entries.has(sku)) { toast(`${sku} is not in the master index`, "bad"); return; } await api("charmNestLibrary", { op: "aliasPut", listingId: r.line.listingId, sku, by: who, title: r.line.title }); await Orders.loadMaps(true); toast(`Listing ${r.line.listingId} → ${sku} remembered`, "ok"); for (const rr of Orders.rows()) if (String(rr.line.listingId) === String(r.line.listingId)) await repool(rr); };
      c.querySelector("[data-a=nodesign]").onclick = async () => { const who = by(); if (!who) return; const sku = p.sku || (sp && sp.designSku); if (sku) await api("charmNestLibrary", { op: "noDesignPut", sku, by: who, note: r.line.title }); else await api("charmNestLibrary", { op: "noDesignPut", pattern: "^" + String(r.line.title).replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 40), by: who, note: "by title" }); await Orders.loadMaps(true); await repool(r); };
      const mb = c.querySelector("[data-a=master]"); if (mb) mb.onclick = () => setMode("master");
    } else if (it.kind === "missingSize") {
      c.innerHTML = head("Missing size", `${p.sku} · size ${p.size || "(none)"}`, orderSub) + `<div class="ev">${evRow("Sizes available", (p.available || []).join(", "))}${evRow("Options", (r.line.variations || []).map(v => `<q>${esc(v.name)}: ${esc(v.value)}</q>`).join(" "))}</div><div class="fixes"><select data-f="size">${(p.available || []).map(s => `<option>${esc(s)}</option>`).join("")}</select><button class="btn gold sm" data-a="size">Use this size (staff decision)</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      c.querySelector("[data-a=size]").onclick = async () => { const s = c.querySelector("[data-f=size]").value; const who = by(); if (!who) return; r.sizeOverride = s; try { await DesignLink.call("notes.set", { receiptId: r.order.receiptId, text: `${sp.staffNote ? sp.staffNote + "\n" : ""}Size ${s} chosen by ${who} (sorter)` }); } catch (_) {} await repool(r); };
    } else if (it.kind === "oversize") {
      c.innerHTML = head("Oversize", `${p.sku} · ${p.widthMm.toFixed(1)} × ${p.heightMm.toFixed(1)} mm`, orderSub) + `<div class="ev">${evRow("Plate", `${labelOf(p.material)} ${fmt.mm(stockFor(p.material).wPt)} × ${fmt.mm(stockFor(p.material).hPt)} under the ${fmt.pct(S.settings.maxFill)} ceiling`)}</div><div class="fixes"><button class="btn ghost sm" data-a="stock">Different stock (Settings)</button><button class="btn ghost sm" data-a="retry">Try again</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      c.querySelector("[data-a=stock]").onclick = () => openSettings(); c.querySelector("[data-a=retry]").onclick = () => repool(r);
    } else if (it.kind === "engraveWords" || it.kind === "notRepresentable" || it.kind === "fontMissing") {
      const j = it.job; const miss = j.missing || [];
      const hl = t => esc(t).replace(/\n/g, "<br>"); const marked = miss.length ? [...(j.text || "")].map(ch => miss.includes(ch) ? `<span class="miss">${esc(ch)}</span>` : esc(ch) === "\n" ? "<br>" : esc(ch)).join("") : hl(j.text || "");
      c.innerHTML = head(it.kind === "notRepresentable" ? "Not representable" : it.kind === "fontMissing" ? "Font files missing" : "Engraving words", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Customer typed", `<q>${esc((sp.personalization || []).join(" / ") || "—")}</q>`)}${evRow("Buyer message", `<q>${esc(sp.buyerMessage || "—")}</q>`)}${evRow("Staff note", `<q>${esc(sp.staffNote || "—")}</q>`)}${sp.messages && sp.messages.length ? evRow("Staff messages", sp.messages.map(m => `<q>${esc(m.senderName)}: ${esc(m.text)}</q>`).join(" ")) : ""}${evRow("Claude read", claudeVerdict(j))}${j.questions && j.questions.length ? evRow("Claude asks", j.questions.map(q => `<q>${esc(q)}</q>`).join(" ")) : ""}${j.requests && (j.requests.font || j.requests.handwriting || j.requests.image || (j.requests.side && !["back", "unspecified"].includes(j.requests.side))) ? evRow("Customer asks", esc(JSON.stringify(j.requests))) : ""}</div><div class="why">${esc(it.why || j.reason || "")}</div>
        <div class="fixes"><textarea data-f="text">${esc(j.text || (sp.personalization || []).join("\n"))}</textarea></div>
        <div class="fixes"><button class="btn gold sm" data-a="confirm">Engrave this text</button>${miss.length ? `<button class="btn ghost sm" data-a="drop">Drop the character${miss.length > 1 ? "s" : ""} ${esc(miss.join(" "))}</button>` : ""}<button class="btn ghost sm" data-a="msg">Ask the customer</button><button class="btn ghost sm" data-a="none">Don't engrave</button>${it.kind === "fontMissing" ? `<button class="btn ghost sm" data-a="fonts">Retry font files</button>` : ""}</div>`;
      c.querySelector("[data-a=confirm]").onclick = () => Engrave.decideWords(j, { text: c.querySelector("[data-f=text]").value, note: c.querySelector("[data-f=text]").value.trim() !== (j.text || "").trim() ? "edited" : "confirmed" });
      const dr = c.querySelector("[data-a=drop]"); if (dr) dr.onclick = () => { let t = c.querySelector("[data-f=text]").value; for (const ch of miss) t = t.split(ch).join(""); c.querySelector("[data-f=text]").value = t.replace(/[ ]{2,}/g, " ").trim(); };
      c.querySelector("[data-a=msg]").onclick = async () => { const who = by(); if (!who) return; const draft = prompt("Message to post in the order's internal chat (the station staff will contact the customer):", `${who}: please confirm the engraving text for ${sp.designSku} — we read "${(j.text || "").replace(/\n/g, " / ")}"${miss.length ? `; the symbol ${miss.join(" ")} cannot be engraved exactly` : ""}.`); if (!draft) return; try { await DesignLink.call("chat.post", { receiptId: r.order.receiptId, text: draft, sender: "Charm Sorter" }); toast("Posted to the station chat", "ok"); } catch (e) { toast(e.message, "bad"); } };
      c.querySelector("[data-a=none]").onclick = () => Engrave.decideWords(j, { none: true });
      const fb = c.querySelector("[data-a=fonts]"); if (fb) fb.onclick = async () => { B.engrave.fonts.ok = false; B.engrave.fonts.error = null; await Engrave.loadFonts(); if (B.engrave.fonts.ok) { remove(it.key); await Engrave.setReady(j); RunCtl.poke(); } };
    } else if (it.kind === "flipFailed") {
      const j = it.job; const ch = it.checks || (j.flipError && j.flipError.checks) || {};
      c.innerHTML = head("Flip check failed", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Checks", Object.keys(ch).length ? Object.entries(ch).map(([k, ok]) => `${esc(k)} ${ok ? "✓" : "<b style='color:#8a3a26'>✗</b>"}`).join(" · ") : esc(it.why || ""))}</div><div class="imgs" style="display:flex;gap:8px"></div><div class="why">${esc(it.why || j.reason || "")}</div><div class="fixes"><button class="btn gold sm" data-a="rerun">Re-run</button><button class="btn ghost sm" data-a="noeng">Mark ${esc(sp.designSku)} not engravable</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      const imgs = it.images || (j.flipError && j.flipError.images); if (imgs) { for (const [k, m] of [["front", imgs.F], ["back", imgs.B], ["front flipped", imgs.flipF]]) { if (!m) continue; const cv = document.createElement("canvas"); cv.width = m.w; cv.height = m.h; cv.style.cssText = "width:150px;border:1px solid var(--line);background:#fff"; cv.title = k; const ctx = cv.getContext("2d"); const img = ctx.createImageData(m.w, m.h); for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) { const i = ((m.h - 1 - y) * m.w + x) * 4, v = m.bits[y * m.w + x] ? 40 : 255; img.data[i] = v; img.data[i + 1] = v; img.data[i + 2] = v; img.data[i + 3] = 255; } ctx.putImageData(img, 0, 0); c.querySelector(".imgs").appendChild(cv); } }
      c.querySelector("[data-a=rerun]").onclick = () => { remove(it.key); j.state = "ready"; Engrave.fitJob(j).catch(e => toast(e.message, "bad")); };
      c.querySelector("[data-a=noeng]").onclick = async () => { const who = by(); if (!who) return; await Master.patch(sp.designSku, { engravable: false }); remove(it.key); Engrave.decideWords(j, { none: true, by: who }); };
    } else if (it.kind === "placement") {
      return Engrave.placementCard(it.job, 1);
    } else if (it.kind === "orderChanged") {
      c.innerHTML = head("Order changed", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Was", `<q>${esc(it.old && it.old.text || (it.old && it.old.spec && it.old.spec.personalization || []).join(" / ") || "—")}</q> · ${esc(it.old && it.old.spec ? `${it.old.spec.designSku} · ${it.old.spec.material || "?"} · ${it.old.spec.form || ""} ${it.old.spec.size || ""}` : "")}`)}${evRow("Now", `<q>${esc((sp.personalization || []).join(" / ") || "—")}</q> · ${esc(`${sp.designSku} · ${sp.material || "?"} · ${sp.form || ""} ${sp.size || ""}`)}`)}${evRow("Buyer / note", `<q>${esc(sp.buyerMessage || "—")}</q> / <q>${esc(sp.staffNote || "—")}</q>`)}</div><div class="why">${esc(it.why)}</div><div class="fixes"><button class="btn gold sm" data-a="accept">Accept the new order (re-read, re-fit)</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      c.querySelector("[data-a=accept]").onclick = async () => { remove(it.key); if (r.state === "written" || r.state === "pooled") { if (sp.engraveCandidate) await Engrave.classify(r); else r.engrave = { needed: false, state: "none", approved: true }; } else await repool(r); RunCtl.poke(); };
    } else if (it.kind === "heldOrder") {
      c.innerHTML = head("Held order", it.rid, it.why) + `<div class="fixes"><button class="btn ghost sm" data-a="jump">Jump to the line's item</button></div>`;
      c.querySelector("[data-a=jump]").onclick = () => { remove(it.key); focus(it.line); };
    } else { c.innerHTML = head(it.kind, it.why || "", orderSub); }
    const skipB = c.querySelector("[data-a=skip]"); if (skipB) skipB.onclick = () => { const who = by(); if (!who) return; r.state = "skipped"; r.reason = `line skipped by ${who}`; r.problems = []; r.hold = `line skipped by ${who}`; syncOrderItems(); Orders.render(); RunCtl.poke(); };
    const holdB = c.querySelector("[data-a=hold]"); if (holdB) holdB.onclick = () => { const who = by(); if (!who) return; r.hold = `held by ${who}`; r.reason = r.hold; remove(it.key); Orders.render(); RunCtl.poke(); toast(`${r.order.receiptId} held — it stays open on the station`, ""); };
    return c;
  }
  async function row_material(r, m, who) { r.materialOverride = m; try { await DesignLink.call("notes.set", { receiptId: r.order.receiptId, text: `${r.spec.staffNote ? r.spec.staffNote + "\n" : ""}Material: ${labelOf(m)} (${who}, sorter)` }); } catch (e) { toast("Staff note not written: " + e.message, "bad"); } await repool(r); }
  const KIND_WORDS = { needsMaterial: "Material", needsMapping: "Options", unmatchedSku: "Unknown SKU", blockedSku: "Blocked SKU", missingSize: "Size", oversize: "Too big", fontMissing: "Font", engraveWords: "Words", notRepresentable: "Characters", flipFailed: "Flip", placement: "Placement", orderChanged: "Changed", heldOrder: "Held" };
  const RV = { filter: null };
  function render() {
    const v = document.getElementById("reviewView"); LiveStrip.render(); if (!v || v.classList.contains("hidden")) return;
    const all = items().filter(it => !(it.row && it.row.state === "gone"));
    const ORDER = ["needsMaterial", "needsMapping", "unmatchedSku", "blockedSku", "missingSize", "oversize", "fontMissing", "engraveWords", "notRepresentable", "flipFailed", "placement", "orderChanged", "heldOrder"];
    all.sort((a, b) => ORDER.indexOf(a.kind) - ORDER.indexOf(b.kind) || a.t - b.t);
    // the kinds present are the filter: one chip each, so a long mixed list becomes the one kind being worked through
    const byKind = new Map(); for (const it of all) byKind.set(it.kind, (byKind.get(it.kind) || 0) + 1);
    if (RV.filter && !byKind.has(RV.filter)) RV.filter = null;
    const list = RV.filter ? all.filter(it => it.kind === RV.filter) : all;
    const chip = (id, label, n) => `<button class="egTab${(RV.filter || "") === id ? " on" : ""}" data-k="${esc(id)}">${esc(label)}<b class="warn">${n}</b></button>`;
    v.innerHTML = `<div class="ordBar egBar">${chip("", "Everything", all.length)}${ORDER.filter(k => byKind.has(k)).map(k => chip(k, KIND_WORDS[k] || k, byKind.get(k))).join("")}<span class="spacer"></span><span class="mono" style="font-size:11.5px">${esc(employeeName() || "— no reviewer name —")}</span><button class="btn ghost xs" id="rvName">Change name</button></div>
      <div class="egPane grow scroll"><div class="rvList" id="rvList"></div></div>`;
    v.querySelector("#rvName").onclick = () => { askEmployee(); render(); };
    v.querySelectorAll("[data-k]").forEach(b => b.onclick = () => { RV.filter = b.dataset.k || null; render(); });
    const host = v.querySelector("#rvList");
    for (const it of list) host.appendChild(card(it));
    if (!list.length) host.innerHTML = `<div class="libEmpty">Nothing waits for a decision.</div>`;
  }
  return { items, count, add, remove, render, card, problemText, syncOrderItems, focus, repool };
})();

/* ═══ 24b · Sandbox — a stored copy of the open orders, an emulated Etsy, isolated records (nothing real is touched) ═══ */
const Sandbox = window.Sandbox = (() => {
  const on = () => S.settings.sandbox === "on";
  let status = null;
  async function refresh() { if (!S.cloud.ok) return null; try { status = await api("charmNestLibrary", { op: "sandboxStatus" }); } catch (e) { status = { error: e.message }; } render(); return status; }
  /** One real read of the open orders through the station (production mode), stored as JSON under charmnest/sandbox/. */
  async function snapshot() {
    if (on()) throw new Error("switch the sandbox OFF first: the snapshot is taken from the real Etsy through the station");
    if (!S.cloud.ok) throw new Error("cloud offline");
    await DesignLink.ensure();
    if (!DesignLink.etsyBudgetOk("the sandbox snapshot")) throw new Error("Etsy call budget reached");
    const r = await DesignLink.call("orders.raw", { refresh: true }, { timeoutMs: 20 * 60 * 1000, onProgress: p => { if (p.text) agent({ bridge: true }, "DS", `Snapshot: ${p.text}`); } });
    DesignLink.meter(r, "the sandbox snapshot");
    const at = Date.now(); const path = `charmnest/sandbox/orders-${new Date(at).toISOString().replace(/[:.]/g, "-")}.json`;
    const bytes = new TextEncoder().encode(JSON.stringify({ at, count: r.count, receipts: r.receipts }));
    const up = await uploadBytes(path, bytes, "application/json", "Saving the sandbox snapshot");
    const put = await api("charmNestLibrary", { op: "sandboxPut", path: up.path, count: r.count, at, takenBy: employeeName() || "operator" });
    agent({ bridge: true }, "ok", `Sandbox snapshot: ${r.count} open order(s) copied to ${up.path} (${(bytes.length / 1024).toFixed(0)} KB)`);
    toast(`Snapshot taken: ${r.count} orders — switch the sandbox ON in Settings to run against it`, "ok", 8000);
    await refresh(); return put.snapshot;
  }
  const waitFor = (fn, ms, why) => new Promise((res, rej) => { const t0 = Date.now(); (function tick() { if (fn()) return res(true); if (Date.now() - t0 > ms) return rej(new Error(why)); setTimeout(tick, 500); })(); });
  /** The whole chain from one press: the station signed in (its own window if needed), the snapshot taken, the sandbox
      switched on, the sorter reloaded, and the orders pulled from the copy (a run starts by itself in Auto mode). */
  async function enable() {
    if (on()) return;
    if (!(status && status.snapshot)) {
      toast("No snapshot yet — taking one from Etsy first", "", 5000);
      await DesignLink.ensure();
      if (!(DesignLink.state() && DesignLink.state().etsy.signedIn)) {
        toast("The station is not signed in — Connect Etsy opens in its own window", "", 6000);
        await DesignLink.connectEtsy();
        await waitFor(() => DesignLink.state() && DesignLink.state().etsy.signedIn, 4 * 60 * 1000, "the Etsy sign-in did not complete within 4 minutes");
      }
      await snapshot();
    }
    S.settings.sandbox = "on"; saveSettings();
    try { sessionStorage.setItem("cn.sandboxAutoPull", "1"); } catch (_) {}
    toast("Sandbox ON — reloading, then pulling the orders from the copy", "ok", 4000);
    setTimeout(() => location.reload(), 700);
  }
  /** After the reload that switched the sandbox on: straight to the Orders tab and a pull (Auto mode starts its run instead). */
  function afterReload() {
    let want = false; try { want = sessionStorage.getItem("cn.sandboxAutoPull") === "1"; sessionStorage.removeItem("cn.sandboxAutoPull"); } catch (_) {}
    if (!want || !on()) return;
    setTimeout(async () => { setMode("orders"); if (S.settings.runMode === "auto") { agent({ bridge: true }, "DS", "Sandbox on — Auto mode starts the run"); return; } try { await Orders.pull(null); } catch (e) { toast(e.message, "bad", 8000); } }, 900);
  }
  async function reset() {
    if (!confirm("Delete every sandbox record (sandbox pools, sets, runs, sheets, locks, ledger, archive)? Files and the snapshot stay. Production data is untouched.")) return;
    const r = await api("charmNestLibrary", { op: "sandboxReset" }); toast(`Sandbox reset — ${r.deleted} record(s) removed`, "ok"); await refresh();
  }
  function mountPanel(v) {
    let bar = v.querySelector("#sandboxBar"); if (!bar) { bar = document.createElement("div"); bar.id = "sandboxBar"; bar.className = "sandboxBar"; const ob = v.querySelector(".ordBar"); if (ob) ob.insertAdjacentElement("afterend", bar); else v.prepend(bar); }
    bar.innerHTML = `<b>${on() ? "SANDBOX ON" : "Sandbox off"}</b><span id="sbStatus">${status ? statusText() : "…"}</span><span class="spacer"></span><button class="btn ghost xs" id="sbSnap" type="button" ${on() ? "disabled title=\"switch the sandbox off to take a snapshot from the real Etsy\"" : ""}>Snapshot open orders → sandbox</button><button class="btn ghost xs" id="sbReset" type="button">Reset sandbox records</button><button class="btn ${on() ? "ghost" : "gold"} xs" id="sbToggle" type="button">${on() ? "Switch sandbox OFF" : "Switch sandbox ON"}</button>`;
    bar.querySelector("#sbSnap").onclick = () => snapshot().catch(e => toast(e.message, "bad", 8000));
    bar.querySelector("#sbReset").onclick = () => reset().catch(e => toast(e.message, "bad", 8000));
    bar.querySelector("#sbToggle").onclick = () => { if (on()) { S.settings.sandbox = "off"; saveSettings(); toast("Sandbox off — reloading", "ok", 3000); setTimeout(() => location.reload(), 600); return; } const b = bar.querySelector("#sbToggle"); b.disabled = true; b.textContent = "Switching on…"; enable().catch(e => { toast(`Sandbox: ${e.message}`, "bad", 9000); agent({ bridge: true }, "warn", `Sandbox switch-on stopped: ${e.message}`); b.disabled = false; b.textContent = "Switch sandbox ON"; }); };
    if (!status) refresh();
  }
  function statusText() { if (!status || status.error) return status && status.error ? `status: ${status.error}` : ""; const sn = status.snapshot; const rec = status.records || {}; return `${sn ? `snapshot of ${sn.count} order(s) taken ${new Date(sn.at).toLocaleString()}${sn.takenBy ? " by " + sn.takenBy : ""}` : "no snapshot yet"} · sandbox records: ${rec.Charm_Pool || 0} pool, ${rec.Charm_Nest_Sets || 0} sets, ${rec.Charm_Nest_Runs || 0} runs, ${rec.Charm_Nest_Sheets || 0} sheets`; }
  function render() { const el = document.getElementById("sbStatus"); if (el) el.textContent = statusText(); const pill = document.getElementById("sandboxPill"); if (pill) pill.classList.toggle("hidden", !on()); document.documentElement.classList.toggle("sandbox", on()); }
  return { on, refresh, snapshot, enable, afterReload, reset, mountPanel, render, status: () => status };
})();

/* ═══ 25 · boot ═══════════════════════════════════════════════════════════ */
function bootBridge() {
  RunCtl.renderModeBtn(); RunCtl.renderBanner(); LiveStrip.render(); Sandbox.render(); Sandbox.afterReload(); if (Sandbox.on()) agent({ bridge: true }, "warn", "SANDBOX mode: emulated Etsy from the stored snapshot, every record and file goes to sandbox copies");
  document.getElementById("btnRunMode").onclick = () => { const auto = S.settings.runMode !== "auto"; if (auto && !confirm("Auto mode: the sorter connects to the Design Station, pulls the latest orders by the date rule, nests, fits engraving, saves labels and marks the orders complete — stopping only when a person must decide. Turn Auto on?")) return; RunCtl.setMode(auto ? "auto" : "manual"); };
  Orders.loadMaps().catch(() => {}); Master.load().catch(() => {});
  Engrave.loadFonts().catch(() => {});
  // an open run from a previous session is offered for resume
  if (S.cloud.ok) api("charmNestLibrary", { op: "runList", limit: 10 }).then(r => { const open = (r.runs || []).filter(x => !["complete", "abandoned"].includes(x.status)); if (open.length) { agent({ bridge: true }, "DS", `${open.length} open run(s) on record — Orders tab › Resume run…`); toast(`${open.length} run(s) can be resumed (Orders tab)`, "", 6000); } }).catch(() => {});
  // the Design Station frame mounts on first visit to its tab; Auto mode mounts it now
  if (S.settings.runMode === "auto") { setTimeout(() => RunCtl.setMode("auto"), 1500); }
  document.addEventListener("keydown", e => { if (e.altKey && e.key === "r") { e.preventDefault(); setMode("review"); } });
  agent({ bridge: true }, "DS", `Bridge ready · station ${DesignLink.origin()} · ${S.settings.runMode} mode`);
}
(function whenReady() { if (window.CN && S.cloud.ok !== null) bootBridge(); else setTimeout(whenReady, 150); })();
})();
