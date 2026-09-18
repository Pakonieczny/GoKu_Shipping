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
const B = window.B = { link: null, orders: { rows: [], byKey: new Map(), pulledAt: 0, stale: false, snapshot: null, filtered: 0 }, master: { entries: new Map(), files: [], loadedAt: 0, loading: null, error: null, jobs: new Map() }, maps: { optionMaps: {}, aliases: {}, noDesign: { patterns: [], skus: [], rows: [] }, loadedAt: 0 }, pool: { rows: new Map(), sources: new Map() }, engrave: { items: new Map(), fonts: { ok: false, Regular: null, Semibold: null, error: null, loading: null } }, review: { items: [] }, openRuns: null, run: null, sets: new Map(), employee: (localStorage.getItem("cn.employee") || "").trim() };
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
  function push(ev) { rows.push({ t: ev.t || Date.now(), kind: ev.kind, text: ev.text || (ev.html ? ev.html.replace(/<[^>]+>/g, "") : "") }); if (rows.length > 40) rows.shift(); render(); }
  function render() {
    const b = document.getElementById("railRecentBody");
    // newest first, and only drawn while the disclosure is open — it used to be a permanent band that clipped the newest line
    if (b && b.parentElement && b.parentElement.open) b.innerHTML = rows.length ? rows.slice().reverse().map(r => `<div><i>${fmtT(r.t)}</i><b>${esc(r.kind)}</b> ${esc(r.text).slice(0, 160)}</div>`).join("") : "<div>nothing yet</div>";
    const n = Review.count(); const rb = document.getElementById("tabReviewN"); if (rb) rb.textContent = n ? String(n) : "";
    const eb = document.getElementById("tabEngraveN"); if (eb) { const k = Engrave.pendingCount(); eb.textContent = k ? String(k) : ""; }
    if (window.Ladder) Ladder.render();
  }
  return { push, render, rows };
})();

/* ═══ 17b · Ladder — where the work is, on every screen ══════════════════════
   The one thing the station never said: what has been approved, what is where, what is set. It was eleven nine-pixel
   squares whose meaning lived in a title attribute, a scrolling ticker of the five most recent lines, and four badges
   spread across four tabs — so answering "where are we" meant a tour of the app and some mental arithmetic.
   The ladder is that answer, in the rail, on all eight tabs, built from state the app already holds. Seven steps a
   person would name, each with the evidence for it; the step that is waiting on someone is the only one with a
   background, and it is a button that goes to the screen that settles it. */
const Ladder = window.Ladder = (() => {
  const STEPS = [
    { id: "pull", label: "Pull", covers: ["pull", "claim"], tab: "orders",
      count: () => { const n = Orders.rows().filter(r => r.state !== "gone").length; return n ? `${n} line${n === 1 ? "" : "s"}` : ""; } },
    { id: "pool", label: "Pool", covers: ["pool", "plan"], tab: "orders",
      count: () => { const n = Orders.rows().filter(r => r.poolIds && r.poolIds.length).length; return n ? `${n} on the cards` : ""; } },
    { id: "nest", label: "Nest", covers: ["nest"], tab: "nest",
      count: () => { const sh = allSheets().filter(p => p.runId && B.run && p.runId === B.run.runId); const done = sh.filter(p => ["complete", "partial"].includes(p.status)).length; return sh.length ? `${done} of ${sh.length} sheet${sh.length === 1 ? "" : "s"}` : ""; } },
    { id: "checkpoint", label: "Check", covers: ["checkpoint"], tab: "nest",
      count: () => { const bad = allSheets().filter(p => p.runId && B.run && p.runId === B.run.runId && p.verification && !p.verification.ok).length; return bad ? `${bad} to look at` : ""; } },
    { id: "engrave", label: "Engrave", covers: ["engrave", "revalidate"], tab: "engrave",
      count: () => { const k = Engrave.pendingCount(); return k ? `${k} to settle` : (Engrave.reviewedCount() ? `${Engrave.reviewedCount()} decided` : ""); },
      waits: () => Engrave.pendingCount() },
    { id: "labels", label: "Labels", covers: ["labels"], tab: "nest",
      count: () => { const n = B.run && Sets.ofRun ? Sets.ofRun(B.run.runId).reduce((k, s2) => k + (s2.labelFiles ? s2.labelFiles.length : 0), 0) : 0; return n ? `${n} saved` : ""; } },
    { id: "commit", label: "Commit", covers: ["commit", "complete"], tab: "orders",
      count: () => { const n = B.run && B.run.committed ? B.run.committed.length : 0; const h = B.run && B.run.holds ? Object.keys(B.run.holds).length : 0; return n || h ? `${n} committed${h ? ` · ${h} held` : ""}` : ""; } },
  ];
  const WORD = { running: ["Running", "go"], review: ["Waiting on you", "wait"], paused: ["Paused", "wait"], stopped: ["Stopped", "stop"], complete: ["Done", "go"] };
  const mount = () => document.getElementById("ladder");
  /** Which of the seven the run is standing on, and which of them is waiting on a person. */
  function shape() {
    const r = B.run;
    const idx = r ? O.stepIndex(r.step) : -1;
    const here = r ? STEPS.findIndex(s => s.covers.includes(r.step)) : -1;
    const revN = Review.count(), engN = Engrave.pendingCount();
    return { r, idx, here, revN, engN };
  }
  function render() {
    const host = mount(); if (!host) return;
    const { r, here, revN, engN } = shape();
    if (!r) {
      // no run: the ladder still answers "what is here" — the library, the pull, and what the library is missing
      const pulled = Orders.rows().filter(x => x.state !== "gone").length;
      const miss = Master.missingCount ? Master.missingCount() : 0;
      host.innerHTML = `<div class="ldBand ldIdle"><b>No run open</b><span>${B.master.entries.size} SKU${B.master.entries.size === 1 ? "" : "s"} in the library</span></div>`
        + `<div class="ldRows">`
        + `<button class="ldRow" data-tab="orders" title="the orders on the cards"><i class="g ${pulled ? "done" : "todo"}"></i><span class="n">Orders</span><span class="c">${pulled ? `${pulled} line${pulled === 1 ? "" : "s"} pulled` : "nothing pulled"}</span></button>`
        + (miss ? `<button class="ldRow wait" data-tab="master" title="SKUs the orders want that no master file holds"><i class="g stop"></i><span class="n">Library</span><span class="c">${miss} SKU${miss === 1 ? "" : "s"} missing</span></button>` : "")
        + `</div>`;
      wire(host); return;
    }
    const [word, tone] = WORD[r.status] || ["Running", "go"];
    const waitRow = r.status === "review" ? (engN ? "engrave" : "pull") : null;
    host.innerHTML = `<div class="ldBand ld-${tone}" title="run ${esc(r.runId)}"><b>${esc(word)}</b><span>${esc(r.setId || r.day || "")}</span></div>`
      + `<div class="ldRows">` + STEPS.map((s, i) => {
        const state = r.status === "complete" || i < here ? "done" : i === here ? (r.status === "stopped" ? "stop" : "now") : "todo";
        const waiting = (s.waits && s.waits()) || (s.id === waitRow && revN);
        const c = s.count() || "";
        return `<button class="ldRow${waiting ? " wait" : ""}${state === "now" ? " now" : ""}" data-tab="${s.tab}" title="${esc(s.label)}${c ? " — " + esc(c) : ""}"><i class="g ${state}"></i><span class="n">${esc(s.label)}</span><span class="c">${esc(c)}</span></button>`;
      }).join("") + `</div>`
      + (revN || engN ? `<div class="ldWaits">${revN ? `<button class="ldChip warn" data-tab="review" title="decisions a person must make">Review<b>${revN}</b></button>` : ""}${engN ? `<button class="ldChip info" data-tab="engrave" title="engraving still to be settled">Engraving<b>${engN}</b></button>` : ""}</div>` : "");
    wire(host);
  }
  function wire(host) {
    host.querySelectorAll("[data-tab]").forEach(b => b.onclick = () => { setMode(b.dataset.tab); if (b.dataset.tab === "engrave" && window.Engrave) Engrave.render(); });
  }
  return { render, STEPS };
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
    // a rate meter is something you look at when something is wrong: in the normal state it holds no space, and the full
    // readout stays on the Design Station panel where it always was
    el.classList.toggle("quiet", !m.braked && !hot && !(S_.state && S_.state.sandbox));
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
    feedQ.push({ t: ev.t || Date.now(), kind: String(ev.kind || ""), text: text.slice(0, 220) }); if (feedQ.length > 240) feedQ.shift();
    if (!feedT) feedT = setTimeout(flushFeed, 450);
  }
  /* A burst — pooling forty lines, nesting six sheets — used to lose most of itself here: the queue was capped at twelve
     and each flush sent only the last six of the twelve it took. Everything queued now reaches the station, a dozen at a
     time, and only a runaway ever drops a line. */
  function flushFeed() {
    feedT = null;
    if (!feedQ.length || !S_.control) return;
    const rows = feedQ.splice(0, 12);
    call("feed.post", { rows }, { timeoutMs: 4000, quiet: true }).catch(() => {});
    if (feedQ.length) feedT = setTimeout(flushFeed, 250);
  }
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
    const pill = document.createElement("button"); pill.type = "button"; pill.id = "dsDockPill"; pill.className = "hidden"; pill.innerHTML = `<span class="dot"></span>Design Station live view`; pill.onclick = () => { D.hiddenByUser = false; layout(); };
    pill.title = "the Design Station is live — click to show the status strip";
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
    const dockH = mode === "pip" ? Math.round(D.el.getBoundingClientRect().height) + 12 : 0;
    document.documentElement.style.setProperty("--dockH", dockH + "px");
    // the notices hang under whatever chrome the page currently has
    const stg = document.querySelector(".stage");
    if (stg) document.documentElement.style.setProperty("--chromeH", Math.round(stg.getBoundingClientRect().top + 10) + "px");
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
    // The frame was always laid out at 1200 px and scaled down to fit, so on the Design Station tab the app being
    // supervised rendered at 34–61 % — its 10 px order rows at 4–7 px. It is laid out at the width it is given, down to
    // the narrowest the station itself is built for, and only the small corner view is ever scaled.
    if (mode === "full") {
      // the station you are supervising is laid out at the width it is given, never scaled below 1
      const vw = Math.max(980, Math.round(w)); const k = w / vw;
      f.style.width = vw + "px"; f.style.height = Math.round(h / k) + "px"; f.style.transform = `scale(${k})`;
    } else { f.style.width = D.virtualW + "px"; f.style.height = "800px"; f.style.transform = "none"; }
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
    B.orders.snapshot = { total: r.total, hydrated: r.hydrated, etsy: r.etsy, at: Date.now() }; B.orders.recalled = null;
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
  /* The record used to keep a line's state and little else, so a finished run could be listed but never opened: no
     title, no listing, no metal, no ship-by meant no card, no thumbnail and no filter. It keeps a compact copy of the
     line now — everything a row is built from, capped so four hundred of them still fit in one document. */
  const cap = (v, n) => String(v == null ? "" : v).slice(0, n);
  function lineRecord(row) {
    const l = row.line, o = row.order;
    return [row.key, { state: row.state, poolIds: row.poolIds, reason: row.reason, hold: row.hold || null, wait: row.wait || null, sku: row.spec && row.spec.designSku, material: row.material || (row.spec && row.spec.material) || null, quantity: row.spec ? row.spec.quantity : 1,
      engrave: row.engrave ? { needed: !!row.engrave.needed, state: row.engrave.state, approved: !!row.engrave.approved, text: row.engrave.text || null } : null,
      problems: (row.problems || []).map(p => p.kind), updateTs: o.updateTs, orderId: o.receiptId, transactionId: l.transactionId,
      snap: { title: cap(l.title, 160), listingId: cap(l.listingId, 24), metalKey: cap(l.metalKey, 24), metalLabel: cap(l.metalLabel, 40),
        orderNumber: cap(o.orderNumber, 24), buyer: cap(o.buyer && o.buyer.name, 60), shipBy: +o.shipBy || 0, isGift: !!o.isGift,
        vars: (l.variations || []).slice(0, 8).map(v => cap(v.name, 40) + "\u241f" + cap(v.value, 60)),
        pers: (l.personalization || []).slice(0, 4).map(x => cap(x, 200)) } }];
  }
  /** The other direction: a recorded line, back to the row shape every card, list, filter and window already reads. */
  function rowFromRecord(key, l) {
    const s2 = l.snap || {};
    /* Runs recorded before a line's own copy was kept have a SKU and a material and nothing else. The master index
       holds the rest: the charm's name stands in for the listing title, and its drawing — the thing that will actually
       be cut — stands in for the shop photograph, which is arguably the better picture anyway. */
    const me = !s2.title && l.sku ? Master.entryFor(l.sku) : null;
    const order = { receiptId: String(l.orderId || ""), orderNumber: s2.orderNumber || String(l.orderId || ""), shipBy: +s2.shipBy || 0, updateTs: +l.updateTs || 0,
      buyer: { name: s2.buyer || "", country: "", city: "" }, buyerMessage: "", isGift: !!s2.isGift, giftMessage: "", staffNote: "", messages: [], metals: {}, lines: [] };
    const line = { transactionId: String(l.transactionId || ""), listingId: s2.listingId || "", sku: l.sku || "", title: s2.title || (me ? `${me.sku}${me.size ? " · " + me.size : ""}` : ""), quantity: +l.quantity || 1,
      metalKey: s2.metalKey || "", metalLabel: s2.metalLabel || (l.material ? labelOf(l.material) : ""), personalization: s2.pers || [], buyerMessage: "", expectedShipDate: 0,
      variations: (s2.vars || []).map(v => { const i = String(v).indexOf("\u241f"); return { name: String(v).slice(0, i < 0 ? 0 : i), value: i < 0 ? String(v) : String(v).slice(i + 1) }; }) };
    order.lines = [line];
    return { key, order, line, spec: null, problems: [], state: l.state || "pulled", reason: l.reason || null, hold: l.hold || null, wait: l.wait || null, claimedBy: null,
      poolIds: l.poolIds || [], engrave: l.engrave || null, metal: l.material || null, materialOverride: l.material || null, fromRecord: true };
  }
  /* The claim is a courtesy — a gold dot on the station's rows saying the sorter has these — never a lock. So it goes
     in batches of a hundred, each with its own time, and a batch the station does not answer is retried once and then
     let go with a warning: 357 orders in one message once ran past the two-minute reply limit and stopped the whole
     run at its first step, for a dot. */
  async function claim(ids) {
    if (!ids.length) return;
    const got = new Set(); let missed = 0;
    for (let i = 0; i < ids.length; i += 100) {
      const part = ids.slice(i, i + 100); let r = null;
      for (let attempt = 0; attempt < 2 && !r; attempt++) { try { r = await DesignLink.call("claim", { receiptIds: part, runId: B.run && B.run.runId }, { timeoutMs: 90000 }); } catch (e) { if (attempt) { missed += part.length; agent({ bridge: true }, "warn", `claim: ${e.message} — carrying on without the dot on ${part.length} order(s)`); } } }
      for (const id of (r && r.claimed) || []) got.add(id);
    }
    for (const row of rowsOf()) if (got.has(row.order.receiptId)) row.claimedBy = "sorter";
    agent({ bridge: true }, "DS", `Claimed ${got.size} order(s) on the station (gold dot)${missed ? ` · ${missed} unanswered` : ""}`); render();
  }
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
  const STATE_PILL = { pulled: ["neutral", "pulled"], waiting: ["info", "waiting"], noDesign: ["info", "no design"], pooled: ["info", "pooled"], nested: ["ok", "nested"], written: ["ok", "written"], labelled: ["ok", "labelled"], committed: ["ok", "complete"], unmatched: ["bad", "unmatched"], held: ["bad", "held"], contended: ["warn", "other run"], skipped: ["warn", "skipped"], gone: ["bad", "gone"], oversize: ["bad", "oversize"] };
  function engravePill(r) { const e = r.engrave; if (!e) return r.spec && r.spec.engraveCandidate ? ["warn", "words?"] : ["neutral", "—"]; if (!e.needed) return ["neutral", e.state === "skipped" ? "skipped" : "no engraving"]; if (e.approved) return ["ok", "approved"]; if (e.state === "words") return ["warn", "words"]; if (e.state === "review") return ["warn", "review"]; if (e.state === "fitted") return ["info", "fitted"]; if (e.state === "blocked") return ["bad", "blocked"]; return ["info", e.state || "engrave"]; }
  /* Which pile a line is in. The three the shop asked for — went through, needs a person, needs engraving settled —
     plus the two that are neither. A line is in exactly one pile, so the counts add up to the lines pulled. */
  const PILES = [
    // a due line that needs a decision is first of all a decision: the date is on its card either way
    { id: "attn", label: "Needs a decision", cls: "warn", of: r => r.problems.length || ["held", "unmatched", "oversize", "gone"].includes(r.state) },
    { id: "late", label: "Due or overdue", cls: "bad", of: r => dueOf(r).soon && !["committed", "labelled", "skipped"].includes(r.state) },
    { id: "eng", label: "Engraving to settle", cls: "info", of: r => r.engrave && r.engrave.needed && !r.engrave.approved && r.engrave.state !== "skipped" },
    { id: "done", label: "Done", cls: "ok", of: r => ["committed", "labelled"].includes(r.state) },
    { id: "ready", label: "Went through", cls: "ok", of: r => ["pooled", "nested", "written", "noDesign", "skipped"].includes(r.state) },
    { id: "wait", label: "Waiting", cls: "info", of: r => r.state === "waiting" },
    { id: "rest", label: "Not started", cls: "neutral", of: r => true },
  ];
  const pileOf = r => (PILES.find(g => g.of(r)) || PILES[PILES.length - 1]).id;
  const OV = { pile: null, metal: null, form: null, eng: null, q: "", view: null, sort: "due", desc: false };   // what the tab is showing right now
  const FORM_LABEL = { necklace: "Necklaces", earrings: "Earrings", "earring-single": "Single earrings", huggie: "Huggies", charm: "Charms only", bracelet: "Bracelets", anklet: "Anklets", keychain: "Keychains" };
  const viewMode = () => OV.view || S.settings.orderView || "cards";
  /** The lines the filters leave, in ship-by order. */
  function visibleRows() {
    const q = OV.q.trim().toLowerCase();
    return rowsOf().filter(r => {
      if (OV.pile && pileOf(r) !== OV.pile) return false;
      if (OV.metal && (r.material || "none") !== OV.metal) return false;
      if (OV.form && ((r.spec && r.spec.form) || "none") !== OV.form) return false;
      if (OV.eng) { const needs = !!(r.engrave && r.engrave.needed); if (OV.eng === "yes" ? !needs : needs) return false; }
      if (!q) return true;
      const sp = r.spec || {};
      return [r.order.receiptId, sp.designSku, r.line.sku, r.line.title, (sp.personalization || []).join(" "), sp.buyerMessage, sp.staffNote, r.reason]
        .some(x => String(x || "").toLowerCase().includes(q));
    }).sort((x, y) => {
      const k = OV.sort === "order" ? String(x.order.receiptId).localeCompare(String(y.order.receiptId))
        : OV.sort === "state" ? String(x.state).localeCompare(String(y.state))
        : (x.order.shipBy || 0) - (y.order.shipBy || 0);
      return (OV.desc ? -k : k) || String(x.order.receiptId).localeCompare(String(y.order.receiptId));
    });
  }
  /* The listing photographs come from the Design Station, which already has them cached and rate limited, so a wall of
     cards here costs Etsy nothing this page would not already have spent. They are asked for only as a card comes into
     view, at most a dozen at a time, and remembered for the session. A charm we hold a design for falls back to its own
     thumbnail, which is the drawing that will actually be cut. */
  const IMG = { got: new Map(), want: new Set(), timer: 0, io: null, retry: 0 };
  /** Only a card a person can actually see asks for its photograph: 411 lines must not mean 411 Etsy images. */
  function watchImages(host) {
    if (IMG.io) IMG.io.disconnect();
    if (!window.IntersectionObserver) { host.querySelectorAll("[data-lid]").forEach(n => wantImage(n.dataset.lid)); return; }
    IMG.io = new IntersectionObserver(es => {
      for (const e of es) if (e.isIntersecting) { wantImage(e.target.dataset.lid); IMG.io.unobserve(e.target); }
    }, { root: host, rootMargin: "300px 0px" });
    host.querySelectorAll("[data-lid]").forEach(n => { if (!n.dataset.painted) IMG.io.observe(n); });
  }
  function imageFor(r) {
    const lid = String(r.line.listingId || "");
    if (IMG.got.get(lid)) return IMG.got.get(lid);
    const e = r.spec && r.spec.designSku ? B.master.entries.get(r.spec.designSku) : null;
    return (e && Master.thumbOf(e)) || null;
  }
  function wantImage(lid) {
    lid = String(lid || ""); if (!lid || IMG.got.has(lid) || IMG.want.has(lid)) return;
    IMG.want.add(lid);
    if (IMG.timer) return;
    IMG.timer = setTimeout(async () => {
      IMG.timer = 0;
      const ids = [...IMG.want].slice(0, 12); ids.forEach(id => IMG.want.delete(id));
      if (!ids.length) return;
      /* A "no" while the Design Station was still connecting used to be remembered forever, and the cards stayed blank
         for the rest of the session however long the link had been up since. A link that is down is not an answer:
         the ids go back in the queue and are asked again when it comes up. */
      if (!DesignLink.up()) { ids.forEach(id => IMG.want.add(id)); IMG.retry = setTimeout(() => { IMG.retry = 0; const back = [...IMG.want]; IMG.want.clear(); back.forEach(wantImage); }, 4000); return; }
      try {
        const got = await DesignLink.call("orders.images", { listingIds: ids }, { timeoutMs: 45000, quiet: true });
        for (const id of ids) IMG.got.set(id, (got.images && got.images[id]) || null);
      } catch (e) { if (/timed out|link|closed|no reply/i.test(e.message || "")) ids.forEach(id => IMG.want.add(id)); else ids.forEach(id => IMG.got.set(id, null)); }
      paintImages();
      if (IMG.want.size) { const next = [...IMG.want][0]; IMG.want.delete(next); wantImage(next); }
    }, 120);
  }
  /** Fill in every picture that has arrived, without rebuilding the cards under the person's cursor. */
  function paintImages() {
    for (const host of document.querySelectorAll("#ordBody [data-lid]")) {
      const url = IMG.got.get(host.dataset.lid);
      if (host.dataset.painted === "1") continue;
      if (!url) { if (IMG.got.has(host.dataset.lid)) { host.dataset.painted = "1"; const p2 = host.querySelector(".ph"); if (p2) p2.textContent = "no image"; } continue; }
      host.dataset.painted = "1";
      if (host.tagName === "IMG") { host.onerror = () => { host.removeAttribute("src"); delete host.dataset.painted; }; host.src = url; continue; }
      const img = host.querySelector("img") || host.appendChild(el("img"));
      img.loading = "lazy"; img.alt = ""; img.crossOrigin = "anonymous";
      img.onerror = () => { img.remove(); delete host.dataset.painted; if (!host.querySelector(".ph")) { const p = el("span", "ph"); p.textContent = "no image"; host.appendChild(p); } };
      img.src = url;
      const ph = host.querySelector(".ph"); if (ph) ph.remove();
      if (IMG.io) IMG.io.unobserve(host);
    }
  }
  /* Thirteen state words in four colours said nothing about order. The five that are progress now carry their place in
     the run, so "3/5 nested" reads as progress; the exceptions stay unnumbered, so a problem reads differently. */
  const PROGRESS = ["pooled", "nested", "written", "labelled", "committed"];
  function stateWords(r) {
    const [k, t] = STATE_PILL[r.state] || ["neutral", r.state];
    const i = PROGRESS.indexOf(r.state);
    return [k, i < 0 ? t : `${i + 1}/${PROGRESS.length} ${t}`];
  }
  const DAY = 86400;
  /** How the ship-by date reads today: overdue, due, or simply a date. */
  function dueOf(r) {
    const by = r.order.shipBy; if (!by) return { cls: "", txt: "\u2014", late: false, soon: false };
    const today0 = Math.floor(Date.now() / 1000 / DAY) * DAY, d = Math.floor(by / DAY) * DAY;
    const txt = new Date(by * 1000).toLocaleDateString("en-US", { month: "short", day: "2-digit" });
    return { cls: d < today0 ? "bad" : d <= today0 + DAY ? "warn" : "", txt, late: d < today0, soon: d <= today0 + DAY };
  }
  const shipTxt = r => r.order.shipBy ? new Date(r.order.shipBy * 1000).toLocaleDateString("en-US", { month: "short", day: "2-digit" }) : "—";
  const wordsOf = sp => (sp.personalization || []).join(" / ") || sp.buyerMessage || "";
  /** Where this line physically is: the set and the sheet it was nested on. "What's where", answered on the line itself. */
  function placeOf(r) {
    for (const id of r.poolIds || []) { const p2 = B.pool.rows.get(id); if (p2 && (p2.sheetName || p2.sheetId)) return { set: p2.setId || "", sheet: p2.sheetName || p2.sheetId, sheetId: p2.sheetId || null }; }
    return null;
  }
  /** Everything a person needs to recognise one line, as a card or as a row: the same fields either way. */
  function renderBody() {
    const host = document.getElementById("ordBody"); if (!host) return;
    const at = host.scrollTop;                                             // a run writing to the list must not scroll it away
    const rows = visibleRows();
    const restore = () => { if (at) host.scrollTop = at; };
    if (!rowsOf().length) {
      host.innerHTML = '<div class="libEmpty">Nothing pulled yet \u2014 press <b>Pull orders</b> above.</div>';
      return;
    }
    if (!rows.length) {
      host.innerHTML = '<div class="libEmpty">Nothing matches these filters.<br><button class="btn ghost sm" id="ordClear" style="margin-top:10px">Show everything</button></div>';
      host.querySelector("#ordClear").onclick = () => { OV.pile = null; OV.metal = null; OV.form = null; OV.eng = null; OV.q = ""; render(); };
      return;
    }
    const cards = viewMode() === "cards";
    host.innerHTML = '<div class="' + (cards ? "ordCards" : "ordList") + '" id="ordItems"></div>';
    const list = host.querySelector("#ordItems");
    if (!cards) {
      const th = (k, t) => '<button data-sort="' + k + '" class="' + (OV.sort === k ? "on" : "") + '" title="order the list by this">' + t + (OV.sort === k ? (OV.desc ? " \u25be" : " \u25b4") : "") + '</button>';
      const hdr = el("div", "olist hdr");
      hdr.innerHTML = '<span></span>' + th("order", "Order") + '<span>SKU</span><span class="hideSm">Item</span><span>Qty</span><span class="hideSm">Metal</span>' + th("due", "Ship by") + th("state", "State");
      hdr.querySelectorAll("[data-sort]").forEach(b => b.onclick = () => { if (OV.sort === b.dataset.sort) OV.desc = !OV.desc; else { OV.sort = b.dataset.sort; OV.desc = false; } renderBody(); });
      list.appendChild(hdr);
    }
    for (const r of rows) {
      const sp = r.spec || {}, m = r.material || "none";
      const st = stateWords(r), due = dueOf(r), where = placeOf(r);
      const attn = r.problems.length || ["held", "unmatched", "oversize"].includes(r.state);
      const lid = String(r.line.listingId || "");
      const url = imageFor(r);
      const why = attn ? (r.problems.map(x => Review.problemText(x)).join(" · ") || r.reason || "") : r.state === "waiting" ? (r.reason || "") : "";
      const gateBtn = r.state === "waiting" && r.wait ? `<button class="relHold" type="button" data-gate="${r.wait.kind === "slow" ? "release" : "cut"}" data-gm="${esc(r.wait.material)}" title="${r.wait.kind === "slow" ? "send " + esc(labelOf(r.wait.material)) + " to the laser with this set instead of waiting" : "cut the partial " + esc(labelOf(r.wait.material)) + " sheet now"}">${r.wait.kind === "slow" ? "Send now" : "Cut it anyway"}</button>` : "";
      const node = el("button", (cards ? "ocard" : "olist") + " hoverItem" + (attn ? " attn" : ""));
      node.type = "button"; node.dataset.m = m; node.dataset.key = r.key;
      node.title = r.order.receiptId + " · " + (sp.designSku || r.line.sku || "no SKU") + " — " + r.line.title;
      const qty = sp.quantity || r.line.quantity || 1;
      const P = (c, h) => '<span class="' + c + '">' + h + '</span>';
      if (cards) {
        node.innerHTML =
          '<span class="oimg" data-lid="' + esc(lid) + '"' + (url ? ' data-painted="1"' : "") + '>' +
            (url ? '<img crossorigin="anonymous" loading="lazy" alt="" src="' + esc(url) + '" onerror="this.remove()">' : '<span class="ph">' + (lid ? 'loading…' : 'no image') + '</span>') +
            (qty > 1 ? P("qty", "×" + qty) : "") +
            (attn ? '<span class="flag" title="' + esc(why) + '">!</span>' : "") +
          '</span>' +
          '<span class="obody">' +
            '<span class="orow1"><b class="onum">' + esc(r.order.receiptId) + '</b><span class="spacer"></span><span class="ost ' + st[0] + '">' + esc(st[1]) + '</span></span>' +
            '<span class="ometal"><i></i><span>' + esc(m === "none" ? "no material yet" : labelOf(m)) + '</span>' +
              (due.txt !== "\u2014" ? '<span class="due ' + due.cls + '" title="ship by ' + esc(due.txt) + (due.late ? " \u2014 overdue" : due.soon ? " \u2014 due now" : "") + '">' + esc(due.txt) + '</span>' : "") + '</span>' +
            '<span class="osku"><i>SKU</i><b>' + esc(sp.designSku || r.line.sku || "— none —") + '</b></span>' +
            (where ? '<span class="owhere" title="the set and sheet this piece was nested on">' + esc(where.set) + (where.sheet ? " · " + esc(where.sheet) : "") + '</span>' : "") +
            (wordsOf(sp) ? '<span class="opers" title="' + esc(wordsOf(sp)) + '">' + esc(wordsOf(sp)) + '</span>' : "") +
            (why ? P("owhy", esc(why)) : "") +
            (r.hold ? '<button class="relHold" type="button" title="put this line back in play">Release hold</button>' : "") + gateBtn +
          '</span>';
      } else {
        node.innerHTML =
          '<img crossorigin="anonymous" class="th" data-lid="' + esc(lid) + '" alt="" onerror="this.removeAttribute(\'src\')"' + (url ? ' src="' + esc(url) + '" data-painted="1"' : "") + '>' +
          P("cell onum", esc(r.order.receiptId)) +
          '<span class="cell osku"><b>' + esc(sp.designSku || r.line.sku || "— none —") + '</b></span>' +
          '<span class="cell hideSm" style="font-size:12px;color:var(--ink70)">' + esc(wordsOf(sp) || r.line.title) + '</span>' +
          P("qtyc", "×" + qty) +
          '<span class="cell hideSm ometal"><i></i><span>' + esc(m === "none" ? "none" : labelOf(m)) + '</span></span>' +
          '<span class="cell hideSm due ' + due.cls + '" title="ship by">' + esc(due.txt) + '</span>' +
          '<span class="ost ' + st[0] + '">' + esc(st[1]) + '</span>' +
          (why ? '<span class="cell whyc owhy">' + esc(why) + '</span>' : "") +
          (r.hold ? '<button class="relHold" type="button" title="put this line back in play">Release hold</button>' : "") + gateBtn;
      }
      node.onclick = e => { if (e.target.closest(".relHold")) return; OrderWin.open(r.key); };
      { const rh = node.querySelector(".relHold:not([data-gate])"); if (rh) rh.onclick = e => { e.stopPropagation(); Review.repool(r); }; }
      { const gb = node.querySelector("[data-gate]"); if (gb) gb.onclick = e => { e.stopPropagation(); gb.disabled = true; (gb.dataset.gate === "release" ? Gate.release(gb.dataset.gm) : Gate.cutAnyway(gb.dataset.gm)).catch(err => toast(err.message, "bad", 6000)); }; }
      // an order can be several lines on several cards: hovering one lifts all of them, the way the station does
      node.dataset.rid = String(r.order.receiptId);
      list.appendChild(node);
    }
    paintImages();
    watchImages(host);
    restore();
  }
  /* The tab used to be one `v.innerHTML = …` on every call, and it is called from fifteen places — RunCtl.renderBanner's
     last line among them, which itself has eighteen callers, and Pool.addAll every five rows. Pooling two hundred lines
     rebuilt the whole tab forty times, and each rebuild took the scroller's position and the caret out of the search box
     with it. The head is built once, what changes is patched, and the body is the only thing ever re-emitted. */
  function buildHead(v) {
    /* One row. What a person does here, in the order they do it: bring orders in, narrow them, find one, choose how
       to look. It wraps on a narrow window and never scrolls sideways; the count of the pull is a line of small type
       at the end, not a pill in a row of its own. The sandbox's own controls live under the SANDBOX pill up top. */
    v.innerHTML = `<div class="ordHead">
        <div class="ordBar" id="ordBar">
          <span class="grp"><button class="btn sm" id="ordPull">Pull orders</button><select id="ordPullMode" title="which open orders to bring in"><option value="all">every open order</option><option value="dueBy">due by a date</option><option value="count">the most urgent</option></select><input type="date" id="ordDueBy" title="orders due on or before this date"><input type="number" id="ordCount" min="1" max="500" title="how many of the most urgent orders"></span>
          <button class="btn gold sm" id="ordRun" title="nest, engrave and label every line that is ready to go">Run set ▶</button>
          <button class="btn ghost sm" id="ordResume" title="every set on record — open its sheets, its orders and its engraving">Earlier sets…</button>
          <span class="chips" id="ordChips"></span>
          <span class="chips" id="ordMetalHost"></span>
          <input class="ordSearch" id="ordQ" placeholder="order, SKU, words…" title="search the order number, the SKU, the title and everything the customer or the shop wrote">
          <select class="ordSort" id="ordSort" title="what orders the cards"><option value="due">by ship-by</option><option value="order">by order</option><option value="state">by state</option></select>
          <span class="viewSeg" id="ordViewSeg"></span><span class="ordMeta mono" id="ordMeta"></span>
        </div>
      </div><div class="ordBody" id="ordBody"></div>`;
    const q = v.querySelector("#ordQ");
    q.oninput = () => { OV.q = q.value; renderBody(); };                 // never rebuilt now, so the caret needs no restoring
    v.querySelector("#ordPullMode").onchange = e => { S.settings.pullMode = e.target.value; saveSettings(); renderHead(v); };
    v.querySelector("#ordDueBy").onchange = e => { S.settings.pullDueBy = e.target.value; saveSettings(); };
    v.querySelector("#ordCount").onchange = e => { S.settings.pullCount = Math.max(1, +e.target.value || 40); saveSettings(); };
    v.querySelector("#ordSort").onchange = e => { OV.sort = e.target.value; OV.desc = false; renderBody(); };
    v.querySelector("#ordPull").onclick = async () => { if (v.querySelector("#ordPull").disabled) return; try { await pull(null); } catch (e) { toast(e.message, "bad", 7000); agent({ bridge: true }, "warn", e.message); } };
    v.querySelector("#ordRun").onclick = () => RunCtl.start();
    v.querySelector("#ordResume").onclick = () => RunHistory.show();
    Sandbox.mountPanel(v);
  }
  /** What changes while a run works: the counts, the chips, the summary, and which run buttons apply. */
  function renderHead(v) {
    const s = S.settings, all = rowsOf();
    const running = B.run && !["complete", "stopped"].includes(B.run.status);
    const pull = v.querySelector("#ordPull");
    pull.disabled = !!running;
    pull.title = running ? "a run is open — stop it first, or its lines would be replaced under it" : "ask the Design Station for every open order that matches the rule below";
    // while a run is open the banner above owns it: repeating Run and Resume here only made it unclear which did what
    v.querySelector("#ordRun").classList.toggle("hidden", !!running);
    v.querySelector("#ordPullMode").value = s.pullMode || "all";
    v.querySelector("#ordDueBy").value = s.pullDueBy || "";
    v.querySelector("#ordDueBy").classList.toggle("hidden", s.pullMode !== "dueBy");
    v.querySelector("#ordCount").value = s.pullCount || 40;
    v.querySelector("#ordCount").classList.toggle("hidden", s.pullMode !== "count");
    v.querySelector("#ordSort").value = OV.sort;
    const meta = v.querySelector("#ordMeta");
    meta.className = "ordMeta mono" + (B.orders.stale ? " warn" : "");
    const nOrd = new Set(all.map(r => r.order.receiptId)).size;
    const full = B.orders.recalled ? `${nOrd} orders · ${all.length} lines · ${B.orders.recalled.seq ? "Set " + B.orders.recalled.seq : "run"}${B.orders.recalled.day ? " · " + B.orders.recalled.day : ""} · from the record` : B.orders.pulledAt ? `${nOrd} orders · ${all.length} lines · pulled ${fmtT(B.orders.pulledAt)}${B.orders.filtered ? ` · ${B.orders.filtered} left out by the rule` : ""}${B.orders.stale ? " · the station's open list has changed since — pull again to catch up" : ""}` : "nothing pulled yet";
    meta.title = full;
    meta.textContent = B.orders.recalled ? full : B.orders.pulledAt ? `${nOrd} orders · ${all.length} lines · ${fmtT(B.orders.pulledAt)}${B.orders.stale ? " · stale" : ""}` : "";
    const counts = {}; for (const r of all) counts[pileOf(r)] = (counts[pileOf(r)] || 0) + 1;
    const byMetal = {}; for (const r of all) { const m = r.material || "none"; byMetal[m] = (byMetal[m] || 0) + 1; }
    const chip = (on, id, label, n, cls, title) => `<button class="egTab${on ? " on" : ""}" data-pile="${esc(id)}" title="${esc(title || "")}">${esc(label)}<b class="${cls}">${n}</b></button>`;
    /* Six state chips and six metal chips wrapped onto two rows above every list. The state is the question a person
       actually asks; the metal is a narrowing of it, so it is one control, not six, and the row stays one row. */
    const metals = ["gold", "silver", "rose", "gold10k", "gold14k", "none"].filter(m => byMetal[m] || OV.metal === m);
    /* Material, kind of jewellery and "does it get engraved" are the three ways a bench actually narrows a day's work.
       Each one appears only when the lines on screen give it more than one answer — a filter with one option is a
       control that can only ever do nothing. */
    const byForm = {}; for (const r of all) { const f = (r.spec && r.spec.form) || "none"; byForm[f] = (byForm[f] || 0) + 1; }
    const forms = Object.keys(byForm).filter(f => f !== "none" || OV.form === "none").sort((a2, b2) => byForm[b2] - byForm[a2]);
    const engN = all.filter(r => r.engrave && r.engrave.needed).length;
    v.querySelector("#ordChips").innerHTML =
      chip(!OV.pile, "", "Everything", all.length, "", "every line that was pulled")
      + PILES.filter(g => counts[g.id] || OV.pile === g.id).map(g => chip(OV.pile === g.id, g.id, g.label, counts[g.id] || 0, g.cls, g.label)).join("")
;
    const sel = (id, ttl, any, opts, cur) => opts.length > 1 || cur ? `<select class="ordMetal" id="${id}" title="${esc(ttl)}">${[`<option value="">${esc(any)}</option>`].concat(opts.map(o => `<option value="${esc(o[0])}"${cur === o[0] ? " selected" : ""}>${esc(o[1])} \u00b7 ${o[2]}</option>`)).join("")}</select>` : "";
    v.querySelector("#ordMetalHost").innerHTML =
      sel("ordMetal", "narrow it to one material", "Any material", metals.map(m => [m, m === "none" ? "No material" : labelOf(m), byMetal[m] || 0]), OV.metal)
      + sel("ordForm", "narrow it to one kind of jewellery", "Any kind", forms.map(f => [f, FORM_LABEL[f] || (f === "none" ? "Kind not set" : f), byForm[f] || 0]), OV.form)
      + (engN && engN < all.length || OV.eng ? sel("ordEng", "engraved or not", "Engraved or not", [["yes", "Engraved", engN], ["no", "Not engraved", all.length - engN]], OV.eng) : "");
    v.querySelectorAll("[data-pile]").forEach(b => b.onclick = () => { OV.pile = b.dataset.pile || null; renderHead(v); renderBody(); });
    for (const [id, k] of [["ordMetal", "metal"], ["ordForm", "form"], ["ordEng", "eng"]]) { const n = v.querySelector("#" + id); if (n) n.onchange = () => { OV[k] = n.value || null; renderHead(v); renderBody(); }; }
    v.querySelector("#ordViewSeg").innerHTML = ["cards", "list"].map(k => `<button data-view="${k}"${viewMode() === k ? ' class="on"' : ""} title="${k === "cards" ? "a card for every line, with its picture" : "the same lines as rows"}">${k === "cards" ? "Cards" : "List"}</button>`).join("");
    v.querySelectorAll("[data-view]").forEach(b => b.onclick = () => { OV.view = b.dataset.view; S.settings.orderView = OV.view; saveSettings(); renderHead(v); renderBody(); });
  }
  function render() {
    const v = document.getElementById("ordersView");
    if (!v || v.classList.contains("hidden")) { const tb = document.getElementById("tabOrdersN"); if (tb) tb.textContent = B.orders.rows.length ? String(new Set(B.orders.rows.map(r => r.order.receiptId)).size) : ""; return; }
    if (!v.dataset.built) { v.dataset.built = "1"; buildHead(v); }
    renderHead(v);
    renderBody();
    const tb = document.getElementById("tabOrdersN"); if (tb) tb.textContent = B.orders.rows.length ? String(new Set(B.orders.rows.map(r => r.order.receiptId)).size) : "";
  }
  return { pull, claim, unclaim, revalidate, render, renderBody, markStale, loadMaps, interpretAll, lineRecord, rowFromRecord, rows: rowsOf, visibleRows, placeOf, imageFor, wantImage, shipTxt, statePill: r => STATE_PILL[r.state] || ["neutral", r.state], applyPullRule, ctx };
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
      try {
        const [ix, fl] = await Promise.all([api("charmNestLibrary", { op: "masterList", limit: 3000 }, { label: "Loading the charm library" }), api("charmNestLibrary", { op: "masterListFiles" })]);
        B.master.entries = new Map((ix.entries || []).map(e => [e.sku, e])); B.master.files = fl.files || []; B.master.loadedAt = Date.now();
        B.master.error = null;
      } catch (e) { B.master.error = e.message; throw e; }   // a failed load must not look like an empty library
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
      // a ring drawn beside the body is welded into the cut line before the charm is measured or written
      { let welded = 0, left = 0; for (const c of g.charms) { if (c.mergedInto != null || !c.sku || c.alreadyHeld) continue; const r = P.integrateRings(c); welded += r.welded; left += r.left.length; } if (welded || left) say("MASTER", `${welded} jump ring(s) welded into their charm's cut line${left ? ` · ${left} left as drawn (the outline crosses the ring more than twice)` : ""}`); }
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
  /** Every SKU the pulled orders want that no master file holds, newest pull first. */
  function missingSkus() {
    const out = new Map();
    for (const r of Orders.rows()) {
      if (r.state === "gone" || r.spec.noDesign) continue;
      const p = (r.problems || []).find(x => x.kind === "unmatchedSku" && x.sku);
      if (!p) continue;
      if (!out.has(p.sku)) out.set(p.sku, { sku: p.sku, lines: 0, orders: new Set(), title: r.line.title });
      const e = out.get(p.sku); e.lines++; e.orders.add(r.order.receiptId);
    }
    return [...out.values()].sort((a, b) => b.lines - a.lines || a.sku.localeCompare(b.sku));
  }
  /* This used to open as a wall: a headline, a paragraph, sixty SKU chips and three buttons, on a tab a person opened to
     do something else, with no way to shut it. It is one line now, it closes, and it stays closed. */
  function paintMissing(v) {
    let box = v.querySelector("#mMissing");
    if (!box) { box = el("div", "missBox"); box.id = "mMissing"; const head = v.querySelector(".noteBox"); if (head) head.insertAdjacentElement("afterend", box); else v.prepend(box); }
    const miss = missingSkus();
    if (!miss.length || B.missShut) { box.className = "missBox hidden"; box.innerHTML = ""; return; }
    const lines = miss.reduce((n, m) => n + m.lines, 0);
    const orders = new Set(); miss.forEach(m => m.orders.forEach(o => orders.add(o)));
    box.className = "missBox" + (B.missOpen ? " open" : "");
    box.innerHTML = '<div class="t"><b>' + miss.length + ' SKU' + (miss.length === 1 ? "" : "s") + ' the orders want have no master file</b>' +
      '<span>' + lines + ' line' + (lines === 1 ? "" : "s") + ' \u00b7 ' + orders.size + ' order' + (orders.size === 1 ? "" : "s") + '</span>' +
      '<button class="btn ghost xs" data-a="see">' + (B.missOpen ? "Hide" : "See them") + '</button>' +
      '<button class="x" data-a="shut" title="close this \u2014 it stays closed">\u00d7</button></div>' +
      (B.missOpen ? '<div class="skus">' + miss.slice(0, 60).map(m => '<span class="s" title="' + esc(m.title) + '">' + esc(m.sku) + (m.lines > 1 ? '<i>\u00d7' + m.lines + '</i>' : "") + '</span>').join("") +
      (miss.length > 60 ? '<span class="s more">\u2026 and ' + (miss.length - 60) + ' more</span>' : "") + '</div>' +
      '<div class="acts"><button class="btn ghost xs" data-a="copy">Copy the list</button><button class="btn ghost xs" data-a="save">Save as a file</button><button class="btn gold xs" data-a="add">Add a master file</button></div>' : "");
    box.querySelector("[data-a=shut]").onclick = () => { B.missShut = true; paintMissing(v); };
    box.querySelector("[data-a=see]").onclick = () => { B.missOpen = !B.missOpen; paintMissing(v); };
    if (!B.missOpen) return;
    box.querySelector("[data-a=copy]").onclick = async () => { try { await navigator.clipboard.writeText(miss.map(m => m.sku).join("\n")); toast(miss.length + " SKU(s) copied", "ok"); } catch (_) { toast("Could not reach the clipboard", "bad"); } };
    box.querySelector("[data-a=save]").onclick = () => {
      const rows = [["sku", "lines", "orders", "title"]].concat(miss.map(m => [m.sku, m.lines, [...m.orders].join(" "), m.title]));
      const csv = rows.map(r => r.map(c => '"' + String(c).replace(/"/g, '""') + '"').join(",")).join("\n");
      const a = document.createElement("a"); a.href = URL.createObjectURL(new Blob([csv], { type: "text/csv" }));
      a.download = "charm-library-missing-" + today() + ".csv"; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 4000);
    };
    box.querySelector("[data-a=add]").onclick = () => { const f = v.querySelector("#mFile"); if (f) f.click(); };
  }
  function render() {
    const v = document.getElementById("masterView"); if (!v || v.classList.contains("hidden")) return;
    if (!v.dataset.built) {
      v.dataset.built = "1";
      v.innerHTML = `<div class="masterHead"><input type="file" id="mFile" accept=".ai,.pdf" class="hidden"><button class="btn gold sm" id="mAdd" title="index another master .ai into the charm library">＋ Add a master file</button><input type="search" id="mSearch" placeholder="Search SKUs" style="border:1px solid var(--line);border-radius:9px;padding:7px 10px"><label style="display:flex;gap:5px;align-items:center;font-size:11px" title="Off: a SKU the library already holds is left as it is, and only new charms are built. On: every charm on the sheet is rebuilt and rewritten."><input type="checkbox" id="mAllSkus"> re-index SKUs already held</label><span class="pill neutral" id="mCount"></span></div>
        <details class="noteBox"><summary>How a master file is read</summary>Drop a master file here, or press Add. Each charm in it has its SKU as text directly under it (within ${S.settings.labelGapMm} mm, centred under the outline). A SKU is one design whatever colour it is ordered in; the material comes from the order. Labels are never part of the charm. Unlabelled charms, orphan labels and duplicates are listed in red; a SKU present in two masters is blocked until fixed.</details>
        <div id="mJobs" style="display:grid;gap:10px"></div><div id="mFiles" style="display:grid;gap:10px"></div><div class="section">Indexed SKUs</div><div class="skuGrid" id="mGrid"></div>`;
      { const cb = v.querySelector("#mAllSkus"); cb.checked = reindexAll; cb.onchange = () => { reindexAll = cb.checked; toast(reindexAll ? "every charm on the next sheet will be rebuilt" : "charms already in the library will be skipped", "ok"); }; }
      // the file goes where it is dropped: on this tab it is a master for the library
      ["dragenter", "dragover"].forEach(ev => v.addEventListener(ev, e => { if (!(e.dataTransfer && [...(e.dataTransfer.types || [])].includes("Files"))) return; e.preventDefault(); e.stopPropagation(); v.classList.add("dragOver"); e.dataTransfer.dropEffect = "copy"; }));
      ["dragleave", "drop"].forEach(ev => v.addEventListener(ev, () => v.classList.remove("dragOver")));
      v.addEventListener("drop", e => { if (!(e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files.length)) return; e.preventDefault(); e.stopPropagation(); for (const f of e.dataTransfer.files) indexFile(f).catch(err => toast(err.message, "bad", 7000)); });
      v.querySelector("#mFile").onchange = e => { for (const f of e.target.files) indexFile(f).catch(err => toast(err.message, "bad", 7000)); e.target.value = ""; };
      v.querySelector("#mSearch").oninput = render;
      v.querySelector("#mAdd").onclick = () => v.querySelector("#mFile").click();
      load().then(render).catch(() => {});
    }
    // What the orders on the cards are asking for that this library cannot answer. One real run wanted 133 SKUs the
    // library had never heard of, because only one master file had ever been indexed — and the only way to learn that
    // was to read 180 review cards. It is one fact, so it is said once, here, where the master files are added.
    paintMissing(v);
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
        pending.forEach((x, i) => { const t = el("div", "vt"); t.innerHTML = `<img crossorigin="anonymous" src="${x.image}" alt=""><div><label style="display:flex;gap:6px;align-items:center"><input type="checkbox" data-i="${i}" ${x.confidence >= 0.95 && x.sku ? "checked" : ""}><span class="mono">#${x.index}</span> <span class="pill ${x.confidence >= 0.95 ? "ok" : "warn"}">${Math.round(x.confidence * 100)}%</span></label><input type="text" data-sku="${i}" value="${esc(x.sku)}" placeholder="SKU as written"><input type="text" data-size="${i}" value="${esc(x.size || "")}" placeholder="size (optional)" style="margin-top:4px"><img crossorigin="anonymous" src="${x.charm.thumb}" style="width:48px;margin-top:4px;border-radius:4px" alt=""></div>`; tray.appendChild(t); });
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
    if (!shown.length) {
      // Four answers, not one: an empty library, a search that found nothing, a cloud that is down and a load that
      // failed used to be pixel-identical, because load() returns early when the cloud is off and a failed masterList
      // is swallowed. The stronger claim — "the master file has not been indexed" — is only made when the orders on the
      // cards are actually waiting on that exact SKU.
      const raw = (v.querySelector("#mSearch").value || "").trim();
      const wanted = q ? missingSkus().find(m => m.sku.toUpperCase() === q) : null;
      grid.innerHTML = !S.cloud.ok
        ? `<div class="libEmpty">Cloud offline — the charm library lives in the cloud. Nothing can be looked up until it is back.</div>`
        : B.master.error
          ? `<div class="libEmpty">Could not load the charm library: ${esc(B.master.error)}<br><button class="btn ghost sm" id="mRetry" style="margin-top:10px">Try again</button></div>`
          : q
            ? `<div class="libEmpty">No indexed SKU contains &ldquo;${esc(raw)}&rdquo;.` +
              (wanted ? ` ${wanted.lines} order line(s) are waiting on it — the master file that carries it has not been indexed yet.` : ` Check the spelling, or index the master file that carries it.`) +
              `<br><button class="btn ghost sm" id="mClear" style="margin-top:10px">Show all ${designs.length} charm(s)</button>` +
              `<button class="btn gold sm" id="mEmptyAdd" style="margin:10px 0 0 6px">＋ Add a master file</button></div>`
            : `<div class="libEmpty">No charms indexed yet — drop a master file here, or press Add.<br><button class="btn gold sm" id="mEmptyAdd" style="margin-top:10px">＋ Add a master file</button></div>`;
      const c2 = grid.querySelector("#mClear"); if (c2) c2.onclick = () => { const f = v.querySelector("#mSearch"); f.value = ""; f.focus(); render(); };
      const a2 = grid.querySelector("#mEmptyAdd"); if (a2) a2.onclick = () => v.querySelector("#mFile").click();
      const r2 = grid.querySelector("#mRetry"); if (r2) r2.onclick = () => { B.master.error = null; load(true).then(render).catch(() => render()); };
      return;
    }
    grid.innerHTML = shown.map(d => {
      const e = d.head, keys = esc(d.skus.join("|"));
      const blocked = [...new Set(d.list.map(x => x.blocked).filter(Boolean))].join("; ");
      const sizes = e.sizes ? Object.entries(e.sizes) : [];
      return `<div class="skuTile hoverItem${blocked ? " blocked" : ""}" data-sku="${esc(e.sku)}">` +
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
  return { entryFor, thumbOf, fetchEntry, load, indexFile, looksLikeMaster, render, patch, patchMany, keepOutOf, skuRegex, stripPng, strayInkUnder, missingSkus, missingCount: () => missingSkus().length };
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
    if (g.charms.length > 1) { for (const c of g.charms) if (c !== charm) { for (const m of c.members) if (!charm.members.includes(m)) charm.members.push(m); charm.topIndices = [...new Set(charm.topIndices.concat(c.topIndices))]; charm.bbox = [Math.min(charm.bbox[0], c.bbox[0]), Math.min(charm.bbox[1], c.bbox[1]), Math.max(charm.bbox[2], c.bbox[2]), Math.max(charm.bbox[3], c.bbox[3])]; /* the silhouette canvas is cut to the bbox: a merged piece outside it would be drawn but never collide */ } agent({ pool: true }, "warn", `${entry.sku}: the master copy split into ${g.charms.length} pieces — folded back into one charm`); }
    { const r = P.integrateRings(charm); if (r.left.length) agent({ pool: true }, "warn", `${entry.sku}: a jump ring was left as drawn — ${r.left[0]}`); }
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
    const rows = Orders.rows().filter(r => ["pulled", "held", "unmatched", "oversize", "waiting"].includes(r.state));
    // what goes to the laser today and what waits: full sheets for SS and GF, every other day for the slow metals, orders whole
    Orders.interpretAll();
    const plan = await Gate.plan(rows);
    const held = [...plan.wait.values()];
    if (held.length) agent({ pool: true }, "POOL", `${held.length} line(s) wait: ${held.filter(w => w.kind === "fill").length} for a full sheet, ${held.filter(w => w.kind === "slow").length} for a slow metal's day`);
    let n = 0;
    const bar = rows.length && window.CNProgress ? CNProgress.start(`Preparing ${rows.length} order line(s)`, { total: rows.length }) : null;
    for (const row of rows) { if (row.state === "waiting") { n++; continue; } if (bar) bar.set(n, rows.length, row.spec && row.spec.designSku ? String(row.spec.designSku) : ""); try { await poolAdd(row, run); } catch (e) { row.state = "held"; row.reason = e.message; agent({ pool: true }, "warn", `${row.order.receiptId} · ${row.spec && row.spec.designSku}: ${e.message}`); } if (++n % 5 === 0) { Orders.render(); } }
    if (bar) bar.end();
    await Gate.afterPool(run);
    Review.syncOrderItems(); Orders.render(); renderRail(); updateTopSub(); refreshAllCards();
    if (run) { run.lines = Object.fromEntries(Orders.rows().map(Orders.lineRecord)); await RunCtl.save(run); }
    const pooled = rows.filter(r => r.state === "pooled").length;
    agent({ pool: true }, "POOL", `Pool: ${pooled} line(s) queued on the cards · ${rows.filter(r => r.state === "waiting").length} waiting · ${rows.filter(r => !["pooled", "noDesign", "waiting"].includes(r.state)).length} held`);
    return pooled;
  }
  async function update(poolIds, patch) { for (const id of poolIds) { const p = B.pool.rows.get(id); if (p) Object.assign(p, patch); } if (S.cloud.ok && poolIds.length) await api("charmNestLibrary", { op: "poolUpdate", poolIds, patch }).catch(() => {}); }
  const charmOf = poolId => allSheets().flatMap(sh => sh.charms).find(c => c.poolId === poolId) || null;
  const sheetOf = poolId => allSheets().find(sh => sh.placements.some(p => { const c = sh.charms.find(x => x.id === p.id); return c && c.poolId === poolId; })) || null;
  return { poolAdd, addAll, masterCharm, cloneCharm, update, charmOf, sheetOf, sizeEntry };
})();

/* ═══ 20b · Gate — what goes to the laser today ═══════════════════════════════════════════════════════════════════
   The rules live in CharmNestOrders.planRelease and are tested there. This is the part that knows the shop: the record
   of when each slow material last went out (shop-wide, in the cloud, not in one browser), the day a person opened one
   early, the footprint of a line from its master design, and the one line on each material's card that says what is
   waiting and for what, with the button that stops the waiting. */
const Gate = window.Gate = (() => {
  const R = { lastReleased: {}, released: {}, forceFill: {}, loaded: false, plan: null };
  const O_ = window.CharmNestOrders;
  async function load() {
    if (R.loaded) return R;
    if (S.cloud.ok) { try { const r = await api("charmNestLibrary", { op: "releaseGet" }, { quiet: true }); R.lastReleased = r.lastReleased || {}; R.released = r.released || {}; } catch (e) { agent({ bridge: true }, "warn", `release record: ${e.message}`); } }
    R.loaded = true; return R;
  }
  async function put(patch) {
    if (patch.lastReleased) Object.assign(R.lastReleased, patch.lastReleased);
    if (patch.released) Object.assign(R.released, patch.released);
    if (S.cloud.ok) await api("charmNestLibrary", { op: "releasePut", lastReleased: R.lastReleased, released: R.released }, { quiet: true }).catch(e => agent({ bridge: true }, "warn", `release record: ${e.message}`));
  }
  /** A line's footprint on the plate, from its master design: the silhouette grown by half the clearance, times copies. */
  function footprint(row) {
    const sp = row.spec; if (!sp || !sp.designSku) return 0;
    const e = Master.entryFor(sp.designSku); if (!e) return 0;
    const g = Pool.sizeEntry(e, sp.size) || e; if (!(g.areaPt2 > 0)) return 0;
    return CN.inflatedArea({ areaPt2: g.areaPt2, widthPt: g.widthPt || 0, heightPt: g.heightPt || 0 }) * Math.max(1, sp.quantity || 1);
  }
  const capacity = () => Object.fromEntries(METALS.map(m => [m.key, CN.usableArea(S.sheets[m.key]) * (+S.settings.maxFill || 0.74)]));
  /** Plan the lines that could pool now, and mark the ones that wait. Returns the plan. */
  async function plan(rows) {
    await load();
    const ready = rows.filter(r => r.spec && !r.spec.noDesign && !r.problems.length && r.spec.material);
    for (const r of ready) if (r.spec.designSku && !Master.entryFor(r.spec.designSku)) await Master.fetchEntry(r.spec.designSku).catch(() => {});
    const lines = ready.map(r => ({ key: r.key, orderId: String(r.order.receiptId), material: r.spec.material, areaPt2: footprint(r), shipBy: +r.order.shipBy || 0 }));
    R.plan = O_.planRelease(lines, { today: today(), capacity: capacity(), lastReleased: R.lastReleased, released: R.released, forceFill: R.forceFill, cadenceDays: +S.settings.cadenceDays || 2, lateDays: S.settings.lateDays == null ? 2 : +S.settings.lateDays });
    for (const r of ready) {
      const w = R.plan.wait.get(r.key);
      if (w) { r.state = "waiting"; r.wait = w; r.reason = waitWords(w); }
      else if (r.state === "waiting") { r.state = "pulled"; r.wait = null; r.reason = null; }
    }
    return R.plan;
  }
  const dayWord = d => d ? new Date(d + "T12:00:00").toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" }) : "";
  function daysUntil(d) { return Math.round((Date.parse(d + "T12:00:00") - Date.parse(today() + "T12:00:00")) / 86400000); }
  function waitWords(w) {
    if (w.kind === "slow") { const n = daysUntil(w.until); return `${labelOf(w.material)} goes to the laser ${n <= 0 ? "today" : n === 1 ? "tomorrow" : dayWord(w.until)} — waits for it`; }
    return `waits for a full ${labelOf(w.material)} sheet · ${w.pct}% so far`;
  }
  /** After pooling: the slow materials that went out today are recorded, and every card learns its kin group. */
  async function afterPool(run) {
    const pooled = Orders.rows().filter(r => r.state === "pooled" && r.material);
    const went = {}; for (const r of pooled) if (O_.SLOW_MATERIALS.has(r.material) && R.lastReleased[r.material] !== today()) went[r.material] = today();
    if (Object.keys(went).length) { await put({ lastReleased: went }); agent({ bridge: true }, "POOL", `${Object.keys(went).map(m => labelOf(m)).join(", ")} released to the laser today — next in ${+S.settings.cadenceDays || 2} days`); }
    const groups = O_.kinGroups(pooled.map(r => ({ orderId: String(r.order.receiptId), material: r.material })));
    for (const m of METALS) for (const pg of pagesOf(m.key)) if (!pg.fileBase && (!run || pg.runId === run.runId)) pg.group = groups[m.key] || m.key;
    if (run) run.groups = groups;
    refreshAllCards();
  }
  /** A person opens a slow material today, or cuts a fast material's partial sheet: the waiting lines pool at once. */
  async function release(material) { await put({ released: { [material]: today() } }); await repoolWaiting(material, `${labelOf(material)} released to the laser by hand`); }
  async function cutAnyway(material) { R.forceFill[material] = true; await repoolWaiting(material, `${labelOf(material)}: partial sheet cut by hand`); }
  async function repoolWaiting(material, why) {
    const who = employeeName() || askEmployee(); if (!who) return;
    agent({ bridge: true, metal: material }, "POOL", `${why} (${who})`);
    const rows = Orders.rows().filter(r => r.state === "waiting" && r.wait && r.wait.material === material);
    for (const r of rows) { r.state = "pulled"; r.wait = null; r.reason = null; }
    if (B.run && O.stepIndex(B.run.step) >= O.stepIndex("pool")) await Pool.addAll(B.run); else await plan(Orders.rows());
    Orders.render(); RunCtl.poke();
  }
  /** The one line under a card's head. Slow: the next day it goes, and how much is waiting. Fast: how full the waiting
   *  sheet has got. Nothing at all when there is nothing to say. */
  function renderCard(sh) {
    const el2 = sh.el && sh.el.querySelector('[data-r="gate"]'); if (!el2) return;
    const m = sh.metal, p = R.plan && R.plan.materials[m];
    const waiting = Orders.rows().filter(r => r.state === "waiting" && r.wait && r.wait.material === m);
    const pieces = waiting.reduce((n, r) => n + Math.max(1, (r.spec && r.spec.quantity) || 1), 0);
    let html = "", cls = "shGate";
    if (O_.SLOW_MATERIALS.has(m)) {
      const last = R.lastReleased[m], open = !last || (R.released[m] === today()) || daysUntil(last) <= -(+S.settings.cadenceDays || 2);
      const next = open ? today() : new Date(Date.parse(last + "T12:00:00") + (+S.settings.cadenceDays || 2) * 86400000).toISOString().slice(0, 10);
      const n = daysUntil(next);
      if (open) { if (!pieces && !(p && p.taken)) { el2.classList.add("hidden"); return; } cls += " open"; html = `<span class="t"><b>Goes to the laser today</b>${R.released[m] === today() ? " · opened by hand" : ""}</span><span class="n">${p && p.taken ? `${p.taken} piece${p.taken === 1 ? "" : "s"} on the sheet` : ""}</span>`; }
      else html = `<span class="t"><b>Next to the laser ${n === 1 ? "tomorrow" : dayWord(next)}</b> · every ${+S.settings.cadenceDays || 2} days</span><span class="n">${pieces ? `${pieces} piece${pieces === 1 ? "" : "s"} waiting` : "nothing waiting"}</span>${pieces ? `<button class="btn ghost xs" data-gate="release" title="send the ${pieces} waiting piece${pieces === 1 ? "" : "s"} with this set instead of waiting for ${dayWord(next)}">Send now</button>` : ""}`;
    } else {
      if (!pieces) { el2.classList.add("hidden"); return; }
      const pct = (waiting[0].wait && waiting[0].wait.pct) || 0;
      html = `<span class="t"><b>${pct}% of a sheet</b> waiting for more</span><span class="n">${pieces} piece${pieces === 1 ? "" : "s"}</span><button class="btn ghost xs" data-gate="cut" title="cut the partial sheet now instead of waiting for it to fill">Cut it anyway</button>`;
    }
    el2.className = cls; el2.classList.remove("hidden"); el2.innerHTML = html;
    const b = el2.querySelector("[data-gate]"); if (b) b.onclick = () => { b.disabled = true; (b.dataset.gate === "release" ? release(m) : cutAnyway(m)).catch(e => toast(e.message, "bad", 6000)); };
  }
  return { load, plan, afterPool, release, cutAnyway, renderCard, footprint, state: () => R };
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
  const DECIDED = ["approved", "written", "skipped"];                    // the same set the Decided tab lists
  const decidedJobs = () => [...items().values()].filter(j => DECIDED.includes(j.state) && j.row.state !== "gone");
  /** A recalled set's engraving: each back written on its sheets becomes a decided job on the line it belongs to, with
   *  its words, who approved it and the file, so the Decided list reads the same for a set from March as for today's. */
  function fromRecall() {
    items().clear();
    const rows = Orders.rows();
    for (const pg of allSheets()) {
      if (!pg.recalled) continue;
      for (const bk of pg.backPool || []) {
        const row = rows.find(r => (bk.poolId && r.poolIds.includes(bk.poolId)) || (String(r.order.receiptId) === String(bk.order) && (!bk.sku || (r.spec && r.spec.designSku) === bk.sku || r.line.sku === bk.sku)));
        if (!row) continue;
        const j = ensureJob(row);
        const lines = bk.lines && bk.lines.length ? bk.lines : String(bk.text || "").split("\n").filter(Boolean);
        Object.assign(j, { state: "written", text: lines.join("\n"), lines, approvedBy: bk.approvedBy || null, approvedAt: pg.recalled.updatedAt || null, backs: [{ poolId: bk.poolId, sheet: pg.fileBase, png: bk.outputs && bk.outputs.png && bk.outputs.png.url, ai: bk.outputs && bk.outputs.ai && bk.outputs.ai.url, capMm: bk.capMm }], recalledFrom: pg });
        row.engrave = { needed: true, state: "written", approved: true, text: j.text };
      }
    }
    render();
  }
  const reviewedCount = () => decidedJobs().length;

  /* ── 7.1 · which lines are engraved, and what the text is ── */
  async function classify(row) {
    const job = ensureJob(row); const sp = row.spec;
    if (!sp.engraveCandidate) { setNone(job, "no personalisation, message or note"); return job; }
    const entry = Master.entryFor(sp.designSku); const engravable = !entry || entry.engravable !== false;
    job.state = "classify"; row.engrave = { needed: false, state: "classify" };
    let r = null;
    try { r = await agentCall("engraveIntent", { order: row.order.receiptId, sku: sp.designSku, title: row.line.title, form: sp.form, quantity: sp.quantity, engravable, personalization: sp.personalization, buyerMessage: sp.buyerMessage, staffNote: sp.staffNote, messages: sp.messages }, { label: `Claude reads the words of ${row.order.receiptId}`, background: true }); }
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
    // one bar for the whole pass, not one per order: what a person needs to know is how far along the reading is
    const bar = rows.length && window.CNProgress ? CNProgress.start(`Reading the words of ${rows.length} order line${rows.length === 1 ? "" : "s"}`, { total: rows.length }) : null;
    try {
      await Promise.all(Array.from({ length: 3 }, async () => { while (q.length) { const r = q.shift(); try { await classify(r); } catch (e) { agent({ engrave: true }, "warn", `${r.order.receiptId}: classifier failed — ${e.message}`); toWords(ensureJob(r), e.message); } done++; if (bar) bar.set(done, rows.length, r.order.receiptId); } }));
    } finally { if (bar) bar.end(); }
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
    render(); return job;
  }
  async function claudeRead(job) {
    if (!S.cloud.ok || !job.fit) return;
    const png = renderBack(job, 700, { grid: true }).toDataURL("image/png");
    const r = await agentCall("engraveReview", { image: png, order: job.row.order.receiptId, sku: job.row.spec.designSku, text: job.lines.join("\n"), capMm: job.fit.capMm, font: "Source Sans 3", weight: job.fit.weight, angle: job.fit.angle, small: job.fit.small }, { label: `Claude looks at the back of ${job.row.order.receiptId}`, background: true });
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
  /* Moving the text is moving the text. It used to be a re-fit with no ceiling, so one tap of a nudge arrow threw away
     the size a person had just chosen and set the largest that happened to fit at the new spot — and overwrote the
     slider's own scale on the way out, so they could never get back. The ceiling is recomputed where the text now is,
     the chosen size is kept, and it only shrinks when the new spot genuinely cannot hold it. */
  function refit(job, place) {
    // the size a person chose (or the fit's own default) is what the text goes back to whenever there is room for it:
    // a nudge into a narrow spot used to bring the lettering down and leave it down
    const font = fontFor(job.fit.weight), want = job.wantSize != null ? job.wantSize : job.fit.size;
    const ceil = G.refitAt(job.lines, font, job.mask, fitOpts(), place);
    if (!ceil.ok) return false;
    // If the chosen size still fits where the text now is, keep it EXACTLY. Re-fitting with a ceiling of `want` bisects
    // to just under it, so twenty nudges used to walk the lettering down by a tenth of its size.
    let f = null;
    if (ceil.size >= want - 1e-6) {
      const L = G.layoutLines(job.lines, font, want, 0.18, place.angle || job.fit.angle, place.centre);
      const v = G.verifyInk(L.cmds, job.mask);
      if (v.ok) f = { ok: true, size: want, capMm: want * G.capPerEm(font) * MM, weight: job.fit.weight, angle: place.angle || job.fit.angle, centre: place.centre, layout: L, glyphs: L.glyphs, cmds: L.cmds, metrics: G.strokeMetrics(L.cmds, 24), small: want * G.capPerEm(font) * MM < (+S.settings.engraveMinCapMm || 1.6), thin: job.fit.thin };
    }
    if (!f) f = ceil.size <= want + 1e-6 ? ceil : G.refitAt(job.lines, font, job.mask, fitOpts(), Object.assign({}, place, { maxSize: want }));
    if (!f.ok) return false;
    f.fittedMax = ceil.size; f.weight = job.fit.weight; f.rect = job.fit.rect;
    if (f.size < want - 0.01) toast(`Only ${f.size.toFixed(2)} pt fits there — the lettering was brought down`, "", 3500);
    job.fit = f; job.verify = { geometry: G.verifyInk(f.cmds, job.mask), at: Date.now() }; job.nudged = true; job.claude = null;
    return true;
  }
  function nudge(job, dxMm, dyMm) { if (!job.fit) return; const c = [job.fit.centre[0] + dxMm * PT, job.fit.centre[1] + dyMm * PT]; if (!refit(job, { centre: c, angle: job.fit.angle })) toast("No room there", "bad"); refresh(job); }
  /** The middle of the area the text may use: the centre of gravity of the solid pixels, not of the bounding box, so a
      cat's head with ears puts the name where the metal actually is. */
  function maskCentroid(m) {
    let sx = 0, sy = 0, n = 0;
    for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) if (m.bits[y * m.w + x]) { sx += x; sy += y; n++; }
    if (!n) return [m.cx, m.cy];
    return [m.ox + (sx / n + 0.5) / m.res, m.oy + (sy / n + 0.5) / m.res];
  }
  function centreText(job) { if (!job.fit) return; if (!moveTo(job, maskCentroid(job.mask))) toast("The text does not fit in the middle — left where it was", "bad"); }
  function moveTo(job, centre) { if (!job.fit) return false; const ok = refit(job, { centre, angle: job.fit.angle }); if (ok) refresh(job); return ok; }
  /** Turn the text about its centre. Within three degrees of straight or upright it snaps there. */
  function rotateTo(job, angle) { if (!job.fit) return false; angle = ((angle % 360) + 360) % 360; for (const snap of [0, 90, 180, 270, 360]) if (Math.abs(angle - snap) < 3) angle = snap % 360; const ok = refit(job, { centre: job.fit.centre, angle }); if (!ok) toast("The text does not fit at that angle", "bad"); refresh(job); return ok; }
  /** The box around the text: its centre, its width and height in the text's own frame, and its angle. */
  function textBox(glyphs, centre, angleDeg) {
    const a = -(angleDeg || 0) * Math.PI / 180, ca = Math.cos(a), sa = Math.sin(a);
    let x0 = Infinity, y0 = Infinity, x1 = -Infinity, y1 = -Infinity;
    for (const g of glyphs) for (const c of g.cmds) { if (c.type === "Z") continue; for (const [px, py] of [[c.x, c.y], c.x1 != null ? [c.x1, c.y1] : null, c.x2 != null ? [c.x2, c.y2] : null].filter(Boolean)) { const dx = px - centre[0], dy = py - centre[1]; const lx = dx * ca - dy * sa, ly = dx * sa + dy * ca; if (lx < x0) x0 = lx; if (lx > x1) x1 = lx; if (ly < y0) y0 = ly; if (ly > y1) y1 = ly; } }
    if (!isFinite(x0)) return null;
    return { cx: centre[0], cy: centre[1], lx0: x0, ly0: y0, lx1: x1, ly1: y1, angle: angleDeg || 0 };
  }
  function resize(job, size) { if (!job.fit) return; size = Math.max(0.5, Math.min(size, job.fit.fittedMax)); job.wantSize = size; const L = G.layoutLines(job.lines, fontFor(job.fit.weight), size, 0.18, job.fit.angle, job.fit.centre); const v = G.verifyInk(L.cmds, job.mask); if (!v.ok) { toast("That size does not verify", "bad"); return; } job.fit = Object.assign({}, job.fit, { size, layout: L, glyphs: L.glyphs, cmds: L.cmds, capMm: size * G.capPerEm(fontFor(job.fit.weight)) * MM, small: size * G.capPerEm(fontFor(job.fit.weight)) * MM < (+S.settings.engraveMinCapMm || 1.6), metrics: G.strokeMetrics(L.cmds, 24) }); job.verify = { geometry: v, at: Date.now() }; job.claude = null; reRead(job); refresh(job); }
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
  function renderBack(job, px, { grid = false, hatch = true, editable = false } = {}) {
    const view = job.view, mask = job.mask, fit = job.fit; const cv = document.createElement("canvas"); cv._editable = editable;
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
    if (false && fit) { ctx.fillStyle = "#111"; for (const g of fit.glyphs) { ctx.beginPath(); let cur = null; for (const c of g.cmds) { if (c.type === "M") { const p = tx(c.x, c.y); ctx.moveTo(p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "L") { const p = tx(c.x, c.y); ctx.lineTo(p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "C") { const a = tx(c.x1, c.y1), b = tx(c.x2, c.y2), p = tx(c.x, c.y); ctx.bezierCurveTo(a[0], a[1], b[0], b[1], p[0], p[1]); cur = [c.x, c.y]; } else if (c.type === "Q") { const a = tx(c.x1, c.y1), p = tx(c.x, c.y); ctx.quadraticCurveTo(a[0], a[1], p[0], p[1]); cur = [c.x, c.y]; } else ctx.closePath(); } ctx.fill("nonzero"); } }
    const outline = document.createElement("canvas"); outline.width = cv.width; outline.height = cv.height;   // …and the charm itself
    outline.getContext("2d").drawImage(cv, 0, 0);
    /** Repaint: the static layers, then the text — the fitted one, or a provisional one while a hand is moving it. */
    const glyphsOf = g => { ctx.fillStyle = "#111"; for (const gl of g) { ctx.beginPath(); for (const c of gl.cmds) { if (c.type === "M") { const p = tx(c.x, c.y); ctx.moveTo(p[0], p[1]); } else if (c.type === "L") { const p = tx(c.x, c.y); ctx.lineTo(p[0], p[1]); } else if (c.type === "C") { const a = tx(c.x1, c.y1), b2 = tx(c.x2, c.y2), d2 = tx(c.x, c.y); ctx.bezierCurveTo(a[0], a[1], b2[0], b2[1], d2[0], d2[1]); } else if (c.type === "Q") { const a = tx(c.x1, c.y1), d2 = tx(c.x, c.y); ctx.quadraticCurveTo(a[0], a[1], d2[0], d2[1]); } else if (c.type === "Z") ctx.closePath(); } ctx.fill("nonzero"); } };
    cv._paint = (prov) => {
      ctx.clearRect(0, 0, cv.width, cv.height); ctx.drawImage(outline, 0, 0);
      const gl = prov && prov.glyphs ? prov.glyphs : (job.fit ? job.fit.glyphs : []);
      const centre = prov && prov.centre ? prov.centre : (job.fit ? job.fit.centre : null);
      const angle = prov && prov.angle != null ? prov.angle : (job.fit ? job.fit.angle || 0 : 0);
      if (gl.length) glyphsOf(gl);
      if (prov && prov.centre && prov.mode === "move") {                     // guides: the charm's own centre lines, lit when the text is on them
        const c = prov.centre, snapX = Math.abs(c[0] - mask.cx) < 0.35 * PT, snapY = Math.abs(c[1] - mask.cy) < 0.35 * PT;
        ctx.save(); ctx.setLineDash([4, 4]); ctx.lineWidth = 1;
        ctx.strokeStyle = snapX ? "rgba(160,110,30,.9)" : "rgba(0,0,0,.18)"; const px1 = tx(mask.cx, bb[1]), px2 = tx(mask.cx, bb[3]); ctx.beginPath(); ctx.moveTo(px1[0], px1[1]); ctx.lineTo(px2[0], px2[1]); ctx.stroke();
        ctx.strokeStyle = snapY ? "rgba(160,110,30,.9)" : "rgba(0,0,0,.18)"; const py1 = tx(bb[0], mask.cy), py2 = tx(bb[2], mask.cy); ctx.beginPath(); ctx.moveTo(py1[0], py1[1]); ctx.lineTo(py2[0], py2[1]); ctx.stroke();
        ctx.restore();
      }
      /* The box around the text is the whole editor: drag inside it to move, drag a corner to resize, drag the handle
         above it to turn. It is drawn in screen pixels so it reads the same at every zoom. */
      cv._box = null;
      if (gl.length && centre && cv._editable) {
        const box = textBox(gl, centre, angle); if (!box) return;
        const m = 3 * PT;                                                     // a little air around the letters
        const corners = [[box.lx0 - m, box.ly0 - m], [box.lx1 + m, box.ly0 - m], [box.lx1 + m, box.ly1 + m], [box.lx0 - m, box.ly1 + m]];
        const a = (box.angle || 0) * Math.PI / 180, ca = Math.cos(a), sa = Math.sin(a);
        const world = ([lx, ly]) => [box.cx + lx * ca - ly * sa, box.cy + lx * sa + ly * ca];
        const pts = corners.map(world).map(p => tx(p[0], p[1]));
        const topMid = world([(box.lx0 + box.lx1) / 2, box.ly1 + m]); const tm = tx(topMid[0], topMid[1]);
        const up = [-sa, ca];                                                 // the text's own "up", in pt
        const hp = tx(topMid[0] + up[0] * 6 * PT, topMid[1] + up[1] * 6 * PT);
        ctx.save(); ctx.lineWidth = 1; ctx.strokeStyle = "rgba(38,110,190,.9)"; ctx.setLineDash([]);
        ctx.beginPath(); pts.forEach((p, i) => i ? ctx.lineTo(p[0], p[1]) : ctx.moveTo(p[0], p[1])); ctx.closePath(); ctx.stroke();
        ctx.beginPath(); ctx.moveTo(tm[0], tm[1]); ctx.lineTo(hp[0], hp[1]); ctx.stroke();
        ctx.fillStyle = "#fff";
        for (const p of pts) { ctx.beginPath(); ctx.rect(p[0] - 4, p[1] - 4, 8, 8); ctx.fill(); ctx.stroke(); }
        ctx.beginPath(); ctx.arc(hp[0], hp[1], 5, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
        ctx.restore();
        cv._box = { box, corners: pts, rotate: hp, centrePx: tx(centre[0], centre[1]) };
      }
    };
    cv._map = { bb, pad, k, tx, base, outline, inv: (px, py) => [bb[0] - pad + px / k, bb[3] + pad - py / k] };
    cv._paint();
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
  const EG = { tab: null, focus: null, chosen: false, card: null, cardKey: null, reread: 0, q: "" };
  const matchesQ = j => { const q = (EG.q || "").trim().toLowerCase(); if (!q) return true; return [j.row.order.receiptId, j.row.spec && j.row.spec.designSku, j.row.line.sku, j.text, (j.lines || []).join(" "), j.row.line.title].some(x => String(x || "").toLowerCase().includes(q)); };
  /** Which run's engraving this is. A tab that showed a queue and no run left nobody able to say whose queue it was. */
  function runWord() {
    // a recalled set names itself, even while a finished run is still remembered underneath
    const rc = B.orders.recalled && window.Recall && Recall.on() ? Object.assign({ runId: B.orders.recalled.runId || "" }, B.orders.recalled) : null;
    const r = rc || B.run || (B.orders.recalled ? Object.assign({ runId: B.orders.recalled.runId || "" }, B.orders.recalled) : null); if (!r) return "no run — earlier runs…";
    return (r.seq ? `Set ${r.seq}` : r.runId.slice(-8)) + (r.day ? " · " + new Date(r.day + "T12:00:00").toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "");
  }
  /** An empty queue has four different causes, and only one of them means "you are done". Say which one it is. */
  function emptyWhy(jobs, nWords, nDone) {
    const r = B.run;
    const line = (t, btn) => `<div class="libEmpty"><b>${t}</b>${btn || ""}</div>`;
    if (!r && window.Recall && Recall.on()) return nDone
      ? line(`Everything engraved in this set is under Decided (${nDone}). Nothing else in it was engraved.`, `<button class="btn ghost sm" data-go="done">Open Decided</button>`)
      : line("Nothing in this set was engraved.", `<button class="btn ghost sm" data-go="hist">Another set…</button>`);
    if (!r && !jobs.length) return line("No run is open, so there is nothing to engrave yet.", `<button class="btn gold sm" data-go="hist">Earlier sets…</button>`);
    if (r && r.status === "stopped") return line(`This run stopped before the engraving step${r.stoppedBy ? ` — ${esc(r.stoppedBy)}` : ""}.`, `<button class="btn ghost sm" data-go="orders">Back to the run</button>`);
    if (r && O.stepIndex(r.step) < O.stepIndex("engrave")) return line(`Nothing to engrave yet — this run is still at ${esc(STEP_WORDS_EG[r.step] || r.step)}.`, `<button class="btn ghost sm" data-go="orders">Back to the run</button>`);
    if (nWords) return line(`${nWords} still need${nWords === 1 ? "s" : ""} its words settled before a placement can be drawn.`, `<button class="btn gold sm" data-go="words">Open Words</button>`);
    const rv = Review.count();
    if (rv) return line(`${rv} line${rv === 1 ? " needs" : "s need"} a decision before anything can be engraved.`, `<button class="btn gold sm" data-go="review">Open Review</button>`);
    if (nDone) return line("Every placement in this run is decided.", `<button class="btn ghost sm" data-go="hist">Another run…</button>`);
    return line("Nothing in this run needs engraving.", `<button class="btn ghost sm" data-go="hist">Another run…</button>`);
  }
  const STEP_WORDS_EG = { pull: "pulling orders", claim: "claiming", pool: "pooling", plan: "planning", nest: "nesting", checkpoint: "checking the sheets", labels: "writing labels", commit: "committing" };
  /** Bring the counts and the up-next rail up to date without touching the card a person is working on. */
  function renderChrome(v, queue) {
    const jobs = [...items().values()].filter(j => j.row.state !== "gone");
    { const rw = v.querySelector("#egRun"); if (rw) rw.textContent = runWord(); }
    const n = { place: queue.length, words: jobs.filter(j => j.state === "words" || j.state === "blocked").length, done: decidedJobs().length };
    v.querySelectorAll(".egTab[data-tab]").forEach(b => {
      const k = n[b.dataset.tab]; let t = b.querySelector("b");
      if (!k) { if (t) t.remove(); return; }
      if (!t) { t = el("b", b.dataset.tab === "words" ? "warn" : b.dataset.tab === "place" ? "info" : "ok"); b.appendChild(t); }
      t.textContent = String(k);
    });
    const rail = v.querySelector("#egNext"); if (!rail) return;
    const rest = queue.filter(j2 => j2.key !== EG.cardKey);
    rail.innerHTML = rest.length ? `<span class="lbl" title="every placement still queued — scroll the rail to reach any of them">up next · ${rest.length}</span>` + rest.map(j2 => `<button class="egChip hoverItem" data-rid="${esc(j2.row.order.receiptId)}" data-key="${esc(j2.key)}" title="${esc(j2.row.spec.designSku)} · ${esc(j2.lines.join(" / "))}"><b>${esc(j2.row.order.receiptId)}</b><span>${esc(j2.lines.join(" / ").slice(0, 22))}</span></button>`).join("") : "";
    rail.querySelectorAll(".egChip").forEach(b => b.onclick = () => { EG.focus = b.dataset.key; render(); });
    const card = EG.card; if (card) { const kind = card.querySelector(".rh .kind"); if (kind) kind.textContent = `${decidedJobs().length + 1} of ${decidedJobs().length + queue.length}`; }
  }
  /** Repaint the card in place: the picture, the numbers, the chips. The pane is only rebuilt when what it holds changes. */
  function refresh(job) {
    const c = EG.card;
    if (!c || !c.isConnected || EG.cardKey !== job.key) { render(); return; }
    const f = job.fit; if (!f) { render(); return; }
    const bc = c.querySelector(".backHost canvas"); if (bc && bc._paint) bc._paint();
    const cap = c.querySelector("[data-cap]"); if (cap) cap.textContent = f.capMm.toFixed(2) + " mm" + (f.angle ? ` · ${Math.round(f.angle)}°` : "");
    const sl = c.querySelector('input[data-a="resize"]');
    if (sl && document.activeElement !== sl) { sl.max = f.fittedMax.toFixed(2); sl.min = (0.5 * f.fittedMax).toFixed(2); sl.value = f.size.toFixed(2); }
    else if (sl) { sl.max = f.fittedMax.toFixed(2); sl.min = (0.5 * f.fittedMax).toFixed(2); }
    const nums = c.querySelector(".pvNums dd"); if (nums) nums.textContent = `${f.size.toFixed(2)} pt · cap ${f.capMm.toFixed(2)} mm · ${f.weight}${f.angle ? ` · ${f.angle}°` : ""}`;
    const rh = c.querySelector(".rh"); if (rh) {
      rh.querySelectorAll(".small").forEach(n => n.remove());
      if (f.small) rh.insertAdjacentHTML("beforeend", `<span class="small" title="the cap height is under the engraver minimum in Settings">SMALL · cap ${f.capMm.toFixed(2)} mm</span>`);
      if (f.thin) rh.insertAdjacentHTML("beforeend", `<span class="small" title="the thinnest stroke is under the engraver limit">THIN STROKES</span>`);
    }
    const cl = c.querySelector(".claude"); if (cl) cl.remove();
    LiveStrip.render();
  }
  /** What Claude last said about the rendered back — and, once it has been moved, that nothing has looked at it since. */
  function claudeBlock(job) {
    if (job.claude) return `<div class="claude">${job.claude.skipped ? `Claude could not look at the render: ${esc(job.claude.skipped)}` : `<b>${job.claude.legible ? "Reads clearly" : "Hard to read"}</b> — ${esc(job.claude.notes)}`}</div>`;
    if (job.rereading) return `<div class="claude" style="opacity:.6">Claude is looking at the rendered back…</div>`;
    return `<div class="claude" style="opacity:.75">Nothing has looked at this since you moved it.</div>`;
  }
  /** A placement that has been moved is exactly the one worth looking at again, so it is looked at again. */
  function reRead(job) {
    clearTimeout(EG.reread);
    EG.reread = setTimeout(() => {
      if (!job.fit || job.state !== "review") return;
      job.rereading = true; refresh(job);
      claudeRead(job).catch(() => {}).then(() => { job.rereading = false; refresh(job); });
    }, 700);
  }                                   // which tab is open and which placement is in front
  function render() {
    LiveStrip.render();
    const v = document.getElementById("engraveView"); if (!v || v.classList.contains("hidden")) return;
    // a background fit finishing must not tear down the card someone is judging: when nothing about what this pane holds
    // has changed, only the counts and the rail are brought up to date
    if (EG.card && EG.card.isConnected && EG.tab === "place") {
      const q2 = [...items().values()].filter(j => j.state === "review" && j.row.state !== "gone");
      const f2 = q2.find(j => j.key === EG.focus) || q2[0];
      if (f2 && f2.key === EG.cardKey) { renderChrome(v, q2); return; }
    }
    const jobs = [...items().values()].filter(j => j.row.state !== "gone");
    const words = jobs.filter(matchesQ).filter(j => j.state === "words" || j.state === "blocked"), queue = jobs.filter(matchesQ).filter(j => j.state === "review"), done = jobs.filter(matchesQ).filter(j => ["approved", "written", "skipped"].includes(j.state));
    // One screen, three tabs, one thing in front of you at a time: the words a person has to settle, the placements to
    // approve, and what has already been decided. The counts are the tabs, so what is left is never more than a glance.
    // Until a person picks a tab, the screen follows the work: it used to settle on Decided while the run was still
    // classifying and then stay there as placements arrived behind it, so someone watching this tab saw an empty pane
    // and no sign that anything was waiting. Once a tab is picked by hand it stays picked, empty or not.
    if (!EG.tab || !EG.chosen) EG.tab = queue.length ? "place" : words.length ? "words" : "done";
    const tab = EG.tab;
    const focus = queue.find(j2 => j2.key === EG.focus) || queue[0] || null;
    const tabBtn = (id, label, n, cls) => `<button class="egTab${tab === id ? " on" : ""}" data-tab="${id}" title="${esc(label)}">${label}${n ? `<b class="${cls}">${n}</b>` : ""}</button>`;
    const working = jobs.filter(j => ["classify", "fitting", "ready"].includes(j.state)).length;
    v.innerHTML = `<div class="ordBar egBar">
        ${tabBtn("place", "Placements", queue.length, "info")}${tabBtn("words", "Words", words.length, "warn")}${tabBtn("done", "Decided", done.length, "ok")}
        ${working ? `<span class="egTab working" title="being read and fitted now — they arrive in Words or Placements on their own"><span class="spin"></span>Working<b>${working}</b></span>` : ""}
        <input class="ordSearch" id="egQ" placeholder="order, SKU, words…" value="${esc(EG.q || "")}" title="search the placements, the words and what has been decided by order number, SKU or the engraved words">
        <span class="spacer"></span><button class="btn ghost xs" id="egRun" title="which run this engraving belongs to — click for every run on record">${esc(runWord())}</button><span class="pill ${F_.ok ? "ok" : "bad"}" title="${F_.ok ? "the engraving font is loaded" : esc(F_.error || "the engraving font files are missing")}">${F_.ok ? "Source Sans 3" : "Source Sans 3 missing"}</span><button class="btn ghost xs" id="egWho" title="every approval is recorded under this name — click to change it">${esc(employeeName() || "set your name")}</button></div>
      <div class="egPane grow"${tab === "place" ? "" : " hidden"}><div class="rvList" id="egQueue"></div>
        <div class="egNext" id="egNext"></div></div>
      <div class="egPane grow scroll"${tab === "words" ? "" : " hidden"}><div class="rvList" id="egWords"></div></div>
      <div class="egPane grow scroll"${tab === "done" ? "" : " hidden"}><div id="egBacks"></div></div>`;
    v.querySelectorAll(".egTab[data-tab]").forEach(b => b.onclick = () => { EG.tab = b.dataset.tab; EG.chosen = true; render(); });
    v.querySelector("#egWho").onclick = () => { askEmployee(); render(); };
    v.querySelector("#egRun").onclick = () => RunHistory.show();
    { const q = v.querySelector("#egQ"); q.oninput = () => { EG.q = q.value; render(); const q2 = v.querySelector("#egQ"); if (q2) { q2.focus(); q2.setSelectionRange(q2.value.length, q2.value.length); } }; }
    if (tab === "words") {
      const w = v.querySelector("#egWords"); for (const j2 of words) w.appendChild(Review.card(Review.items().find(i2 => i2.key === "eng:" + j2.key) || { kind: j2.state === "blocked" ? "flipFailed" : "engraveWords", key: "eng:" + j2.key, row: j2.row, job: j2, why: j2.reason }));
      if (!words.length) w.innerHTML = `<div class="libEmpty">nothing waiting</div>`;
    }
    if (tab === "place") {
      const q = v.querySelector("#egQueue");
      if (focus && EG.list) {
        EG.card = null; EG.cardKey = null;
        q.innerHTML = `<div class="rvList">` + queue.map(j2 => `<div class="doneRow hoverItem" data-rid="${esc(j2.row.order.receiptId)}" data-open="${esc(j2.key)}" title="open this placement"><span class="mini"></span><b class="mono">${esc(j2.row.order.receiptId)}</b><span class="sku mono">${esc(j2.row.spec.designSku || "")}</span><span class="w">${esc(j2.lines.join(" / "))}</span><span class="ost info">${j2.fit ? `cap ${j2.fit.capMm.toFixed(2)} mm` : "fitting"}</span><span class="by"></span><button class="btn ghost xs">Open</button></div>`).join("") + `</div>`;
        q.querySelectorAll("[data-open]").forEach(rw => rw.onclick = () => { EG.focus = rw.dataset.open; EG.list = false; render(); });
      }
      else if (focus) { const c = placementCard(focus, queue.length); c.classList.add("full"); q.appendChild(c); EG.card = c; EG.cardKey = focus.key; }
      else { EG.card = null; EG.cardKey = null; q.innerHTML = emptyWhy(jobs, words.length, done.length); q.querySelectorAll("[data-go]").forEach(b => b.onclick = () => { const g = b.dataset.go; if (g === "hist") RunHistory.show(); else if (g === "words") { EG.tab = "words"; EG.chosen = true; render(); } else { setMode(g); if (g === "review") Review.render(); } }); }
      // what is coming: the order and the words, so the list and the picture are the same thing
      const rail = v.querySelector("#egNext");
      const rest = queue.filter(j2 => j2 !== focus);
      rail.innerHTML = rest.length ? `<span class="lbl" title="every placement still queued — scroll the rail to reach any of them">up next · ${rest.length}</span>` + rest.map(j2 => `<button class="egChip hoverItem" data-rid="${esc(j2.row.order.receiptId)}" data-key="${esc(j2.key)}" title="${esc(j2.row.spec.designSku)} · ${esc(j2.lines.join(" / "))}"><b>${esc(j2.row.order.receiptId)}</b><span>${esc(j2.lines.join(" / ").slice(0, 22))}</span></button>`).join("") : "";
      rail.querySelectorAll(".egChip").forEach(b => b.onclick = () => { EG.focus = b.dataset.key; render(); });
    }
    if (tab === "done") {
      const bk = v.querySelector("#egBacks");
      const decided = decidedJobs().filter(matchesQ).sort((a, b) => (b.approvedAt || 0) - (a.approvedAt || 0));
      const stateWord = j2 => j2.state === "written" ? "written" : j2.state === "skipped" ? "no engraving" : "approved";
      const stateWhy = j2 => j2.state === "written" ? "the back file is saved with the sheet" : j2.state === "skipped" ? "cut plain, nothing on the back" : "approved — the back file is written when the sheet is";
      const fmtT = t => t ? new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "";
      /* One row per decision; the row opens into everything there is to know about it — the back as it was written,
         the front it belongs to, the words, the size, who decided and when, which sheet, the file — and Reopen. The
         separate "back files written" grid said the same things a second time, smaller, and is gone. */
      bk.innerHTML = decided.length
        ? `<div class="section" style="margin-top:2px">Decided · ${decided.length}</div><div class="rvList" id="egDone">` + decided.map(j2 => {
            const w = j2.state === "skipped" ? "cut plain" : esc(j2.lines.join(" / "));
            const who = j2.approvedBy || (j2.decision && j2.decision.by) || "";
            const b0 = (j2.backs || [])[0] || {}; const png = b0.png || (b0.outputs && b0.outputs.png && b0.outputs.png.url) || ""; const ai = b0.ai || (b0.outputs && b0.outputs.ai && b0.outputs.ai.url) || "";
            const open = EG.openDone === j2.key;
            const detail = !open ? "" : `<div class="doneDetail">
                <div class="dd back">${png ? `<img crossorigin="anonymous" src="${esc(png)}" alt="the back as written" referrerpolicy="no-referrer" data-retry="1">` : `<div class="noPic">${j2.state === "skipped" ? "cut plain — nothing on the back" : "the back picture is written with the sheet"}</div>`}<span class="cap">back${b0.sheet ? " · " + esc(b0.sheet) : ""}</span></div>
                <div class="dd front"><div class="frontHost"></div><span class="cap">front</span></div>
                <dl class="meta">
                  <dt>Words</dt><dd class="serif">${w}</dd>
                  ${j2.fit || b0.capMm ? `<dt>Size</dt><dd>cap ${(+(b0.capMm || (j2.fit && j2.fit.capMm) || 0)).toFixed(2)} mm · Source Sans 3${j2.fit && j2.fit.weight ? " " + esc(j2.fit.weight) : ""}</dd>` : ""}
                  <dt>Decided</dt><dd>${esc(who || "—")}${j2.approvedAt ? " · " + esc(new Date(j2.approvedAt).toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })) : ""}${j2.decision && j2.decision.why ? " · " + esc(j2.decision.why) : ""}</dd>
                  ${(j2.backs || []).length ? `<dt>File</dt><dd>${(j2.backs || []).map(b => { const u = b.ai || (b.outputs && b.outputs.ai && b.outputs.ai.url); return u ? `<a href="${esc(u)}" target="_blank" rel="noopener" title="the back file, as it went to the laser">${esc((b.name || "back") + ".ai")}</a>` : "not saved to the cloud"; }).join(" · ")}</dd>` : ""}
                  ${j2.copies && j2.copies.length ? `<dt>Pieces</dt><dd>${j2.copies.length} on ${[...new Set(j2.copies.map(pid => (Pool.sheetOf(pid) || {}).fileBase).filter(Boolean))].map(esc).join(", ") || "the sheet"}</dd>` : ""}
                </dl>
                <div class="ctl"><button class="btn ghost sm" data-a="reopen" title="take this decision back: the words are settled again and the placement is redrawn — a back file already written is superseded">Reopen</button>${j2.recalledFrom ? `<span class="hint">this set is recalled — reopening rebuilds its sheet from the master files first</span>` : ""}</div>
              </div>`;
            return `<div class="doneRow hoverItem${open ? " open" : ""}" data-rid="${esc(j2.row.order.receiptId)}" data-key="${esc(j2.key)}" title="click for the back as it was written, who decided, the file and Reopen">${png ? `<img crossorigin="anonymous" class="mini" src="${esc(png)}" alt="" loading="lazy" referrerpolicy="no-referrer">` : `<span class="mini"></span>`}<b class="mono">${esc(j2.row.order.receiptId)}</b><span class="sku mono">${esc(j2.row.spec.designSku || "")}</span><span class="w">${w}</span><span class="ost ${j2.state === "skipped" ? "warn" : "ok"}" title="${stateWhy(j2)}">${stateWord(j2)}</span><span class="by">${esc(who)}${j2.approvedAt ? " · " + fmtT(j2.approvedAt) : ""}</span>${open ? "" : `<button class="btn ghost xs" data-a="reopen" title="take this decision back">Reopen</button>`}${detail}</div>`;
          }).join("") + `</div>`
        : `<div class="libEmpty">${window.Recall && Recall.on() ? "Nothing in this set was engraved." : "nothing decided yet"}</div>`;
      bk.querySelectorAll(".doneRow").forEach(rw => rw.addEventListener("click", e => { if (e.target.closest("button, a, .doneDetail")) return; EG.openDone = EG.openDone === rw.dataset.key ? null : rw.dataset.key; render(); }));
      { const rw = bk.querySelector(".doneRow.open"); const j2 = rw && items().get(rw.dataset.key); const host = rw && rw.querySelector(".frontHost");
        if (j2 && host) { const charm = j2.copies && j2.copies.length ? Pool.charmOf(j2.copies[0]) : null; if (charm) host.appendChild(renderFront(charm, 148)); else { const e2 = Master.entryFor(j2.row.spec.designSku || j2.row.line.sku); const t = e2 && Master.thumbOf(e2); host.innerHTML = t ? `<img crossorigin="anonymous" src="${esc(t)}" alt="" referrerpolicy="no-referrer">` : `<div class="noPic">no picture of the front</div>`; } } }
      // a picture that will not load is retried once with a fresh request, then says so instead of a broken icon
      bk.querySelectorAll("img").forEach(im => im.addEventListener("error", () => { if (im.dataset.retry) { im.dataset.retry = ""; im.src = im.src.replace(/([?&])_r=\d+/, "$1").replace(/[?&]$/, "") + (im.src.includes("?") ? "&" : "?") + "_r=" + Date.now(); return; } const d = document.createElement("div"); d.className = im.classList.contains("mini") ? "mini" : "noPic"; d.textContent = im.classList.contains("mini") ? "" : "the picture did not load — the .ai file is still there"; im.replaceWith(d); }));
      bk.querySelectorAll("[data-a=reopen]").forEach(b => b.onclick = () => {
        const j2 = items().get(b.closest(".doneRow").dataset.key); if (!j2) return;
        const who = employeeName() || askEmployee(); if (!who) return;
        if (!confirm(`Reopen ${j2.row.order.receiptId}? It goes back to the words step, and any back file already written for it is superseded.`)) return;
        const go = () => { sendBack(j2, `reopened by ${who}`); EG.tab = "words"; EG.chosen = true; render(); };
        if (j2.recalledFrom && j2.recalledFrom.recalled) { Recall.rebuild(j2.recalledFrom).then(() => { const pool = B.pool.rows; for (const [pid, p] of pool) if (String(p.orderId) === String(j2.row.order.receiptId) && (p.sku === (j2.row.spec && j2.row.spec.designSku) || p.sku === j2.row.line.sku) && !j2.row.poolIds.includes(pid)) j2.row.poolIds.push(pid); go(); }).catch(e => toast(`Could not rebuild the sheet: ${e.message}`, "bad", 7000)); return; }
        go();
      });
    }
  }
  /** The placement review card: front and back side by side, the mask hatch, the text as it will be cut, the controls. */
  function placementCard(job, remaining) {
    const it = Review.items().find(i => i.key === "eng:" + job.key) || { kind: "placement", key: "eng:" + job.key, row: job.row, job };
    const card = el("div", "rvItem"); card.dataset.kind = "placement"; card.tabIndex = 0;
    const r = job.row, sp = r.spec, f = job.fit;
    const decided = decidedJobs().length;
    // One card, one order, one screen: the back is the work and the right column is everything you need to judge it.
    const pct = Math.round((job.confidence != null ? job.confidence : 0) * 100);
    const conf = job.source ? `<span class="conf ${pct >= 80 ? "" : pct >= 60 ? "mid" : "low"}" title="how sure Claude is that these are the words to cut, read from ${esc(SOURCE_LABEL[job.source] || job.source)}${job.quote ? ` — “${esc(job.quote)}”` : ""}">${pct}% sure</span>` : "";
    const row2 = (t, v) => v && v !== "—" ? `<dt>${t}</dt><dd>${esc(v)}</dd>` : "";
    card.innerHTML = `<div class="rh"><span class="kind" title="where you are in the placements still to decide">${decided + 1} of ${decided + remaining}</span><button class="x" data-a="close" title="back to the list of placements" aria-label="close">×</button><span class="ttl">${esc(r.order.receiptId)}</span><span class="sub">${esc(sp.designSku)}${sp.form ? " · " + esc(sp.form) : ""}${sp.size ? " · " + esc(sp.size) : ""}${job.copies.length > 1 ? ` · ${job.copies.length} copies` : ""}</span>${conf}${f && f.small ? `<span class="small" title="the cap height is under the engraver minimum in Settings">SMALL · cap ${f.capMm.toFixed(2)} mm</span>` : ""}${f && f.thin ? `<span class="small" title="the thinnest stroke is under the engraver limit">THIN STROKES</span>` : ""}</div>
      <div class="placeView">
        <div class="pvMain"><div class="backHost"></div>
          <div class="ctl">${f ? `<button class="btn sage sm" data-a="approve" title="this placement is right — write the back file">Approve <b class="k">A</b></button><button class="btn ghost sm" data-a="centre" title="put the text in the middle of the metal it may use">Centre</button><span class="mono dim" data-cap title="cap height of the lettering · its angle">${f.capMm.toFixed(2)} mm${f.angle ? ` · ${Math.round(f.angle)}°` : ""}</span>` : ""}
            <span class="rest"><button class="btn ghost sm" data-a="skip" title="cut this charm plain — nothing engraved on its back">No engraving <b class="k">S</b></button></span></div>
          <div class="help">drag the words to move them · drag a corner to resize · drag the handle above to turn · arrow keys nudge 0.25 mm, with shift they turn 1° · cut-outs and holes stay clear: only flat metal takes engraving</div></div>
        <div class="pvSide">
          <div class="pvWords"><span class="lbl">Words on the back</span>${esc(job.lines.join(" / ")) || "—"}</div>
          <div class="frontHost"></div>
          <dl class="meta">${row2("Customer", (sp.personalization || []).join(" / "))}${row2("Buyer msg", sp.buyerMessage)}${row2("Staff note", sp.staffNote)}${job.decision ? `<dt>Decided by</dt><dd>${esc(job.decision.by)}</dd>` : ""}</dl>
        </div></div>`;
    const charm = Pool.charmOf(job.copies[0]);
    card.querySelector(".frontHost").appendChild(renderFront(charm, 420));
    // the back preview is drawn to the box it is actually given, and redrawn when that box changes: no fixed number,
    // nothing cut off on a short laptop screen, nothing left blurry after the window is resized
    const backHost = card.querySelector(".backHost");
    let mounted = 0, raf = 0;
    const wire = bc => {
      let drag = null;
      // the pointer is captured by the canvas, so the handlers live and die with this canvas: every card used to add
      // another pair of listeners to the window and none of them was ever removed
      const near = (p, q2, r) => Math.hypot(p[0] - q2[0], p[1] - q2[1]) <= r;
      const local = e => { const rect = bc.getBoundingClientRect(); return [(e.clientX - rect.left) * bc.width / rect.width, (e.clientY - rect.top) * bc.height / rect.height]; };
      bc.addEventListener("pointermove", e => { if (drag || !bc._box) return; const p = local(e); const b = bc._box; bc.style.cursor = near(p, b.rotate, 9) ? "alias" : b.corners.some(c => near(p, c, 8)) ? "nwse-resize" : "grab"; });
      bc.addEventListener("pointerdown", e => {
        if (!job.fit) return; bc.setPointerCapture(e.pointerId); e.preventDefault();
        const p = local(e), b = bc._box;
        const mode = b && near(p, b.rotate, 10) ? "rotate" : (b && b.corners.some(c => near(p, c, 9))) || e.shiftKey ? "resize" : "move";
        const c0 = job.fit.centre.slice(); const cpx = b ? b.centrePx : bc._map.tx(c0[0], c0[1]);
        drag = { mode, x: e.clientX, y: e.clientY, c: c0, size: job.fit.size, angle: job.fit.angle || 0, cpx, r0: Math.max(4, Math.hypot(p[0] - cpx[0], p[1] - cpx[1])), a0: Math.atan2(p[1] - cpx[1], p[0] - cpx[0]) };
        bc.classList.add("drag");
      });
      bc.addEventListener("pointermove", e => {
        if (!drag) return;
        const rect = bc.getBoundingClientRect(), kx = bc.width / rect.width;
        const dx = (e.clientX - drag.x) * kx / bc._map.k, dy = -(e.clientY - drag.y) * kx / bc._map.k;
        const p = local(e);
        if (drag.mode === "resize") {
          // a corner pulled away from the centre grows the text, pulled in shrinks it: the size follows the distance
          const r = Math.hypot(p[0] - drag.cpx[0], p[1] - drag.cpx[1]);
          drag.pendingSize = Math.max(0.5, Math.min(job.fit.fittedMax, drag.size * r / drag.r0));
          const L = G.layoutLines(job.lines, fontFor(job.fit.weight), drag.pendingSize, 0.18, drag.angle, drag.c);
          bc._paint({ glyphs: L.glyphs, centre: drag.c, angle: drag.angle, mode: "resize" });
        } else if (drag.mode === "rotate") {
          const a = Math.atan2(p[1] - drag.cpx[1], p[0] - drag.cpx[0]);
          let ang = drag.angle - (a - drag.a0) * 180 / Math.PI;               // screen y points down, so the sign flips
          ang = ((ang % 360) + 360) % 360; for (const snap of [0, 90, 180, 270, 360]) if (Math.abs(ang - snap) < 3) ang = snap % 360;
          drag.pendingAngle = ang;
          const L = G.layoutLines(job.lines, fontFor(job.fit.weight), job.fit.size, 0.18, ang, drag.c);
          bc._paint({ glyphs: L.glyphs, centre: drag.c, angle: ang, mode: "rotate" });
          const cap = card.querySelector("[data-cap]"); if (cap) cap.textContent = `${job.fit.capMm.toFixed(2)} mm · ${Math.round(ang)}°`;
        } else {
          const c = [drag.c[0] + dx, drag.c[1] + dy];
          // within a third of a millimetre of the charm's own centre line, the text takes it
          if (Math.abs(c[0] - job.mask.cx) < 0.35 * PT) c[0] = job.mask.cx;
          if (Math.abs(c[1] - job.mask.cy) < 0.35 * PT) c[1] = job.mask.cy;
          drag.pending = c;
          const L = G.layoutLines(job.lines, fontFor(job.fit.weight), job.fit.size, 0.18, drag.angle, c);
          bc._paint({ glyphs: L.glyphs, centre: c, angle: drag.angle, mode: "move" });
        }
      });
      bc.addEventListener("pointerup", () => {
        if (!drag) return;
        const d = drag; drag = null; bc.classList.remove("drag");
        if (d.mode === "resize") { if (d.pendingSize != null) resize(job, d.pendingSize); else bc._paint(); }
        else if (d.mode === "rotate") { if (d.pendingAngle != null) rotateTo(job, d.pendingAngle); else bc._paint(); }
        else if (d.pending) { if (!moveTo(job, d.pending)) { toast("No room there — kept the previous position", "bad"); bc._paint(); } }
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
      const bc = renderBack(job, px, { grid: true, editable: true }); backHost.appendChild(bc); wire(bc);
    };
    requestAnimationFrame(mountBack);
    if (window.ResizeObserver) { const ro = new ResizeObserver(() => { if (raf) return; raf = requestAnimationFrame(() => { raf = 0; mountBack(); }); }); ro.observe(backHost); card._ro = ro; }
    const capOut = card.querySelector("[data-cap]");
    card.querySelectorAll("[data-a]").forEach(b => { const a = b.dataset.a; b.onclick = () => { if (a === "approve") approve(job); else if (a === "centre") centreText(job); else if (a === "close") { EG.list = true; EG.card = null; EG.cardKey = null; render(); }
      else if (a === "resplit") resplit(job); else if (a === "skip") skip(job); else if (a === "back") sendBack(job); }; });
    void capOut;
    card.addEventListener("keydown", e => { if (e.target.tagName === "INPUT" || e.repeat) return; const k = e.key.toLowerCase(); if (k === "a") { e.preventDefault(); approve(job); } else if (k === "s") { e.preventDefault(); skip(job); } else if (e.key === "Escape") { EG.list = true; EG.card = null; EG.cardKey = null; render(); } else if (e.shiftKey && (e.key === "ArrowLeft" || e.key === "ArrowRight")) { e.preventDefault(); rotateTo(job, (job.fit ? job.fit.angle || 0 : 0) + (e.key === "ArrowLeft" ? 1 : -1)); } else if (e.key === "ArrowLeft") { e.preventDefault(); nudge(job, -0.25, 0); } else if (e.key === "ArrowRight") { e.preventDefault(); nudge(job, 0.25, 0); } else if (e.key === "ArrowUp") { e.preventDefault(); nudge(job, 0, 0.25); } else if (e.key === "ArrowDown") { e.preventDefault(); nudge(job, 0, -0.25); } });
    // the next card takes focus only when the person was already working in this pane, so a held key cannot run the queue
    const wasHere = document.activeElement && document.activeElement.closest && document.activeElement.closest("#egQueue");
    if (wasHere || !document.activeElement || document.activeElement === document.body) setTimeout(() => { if (card.isConnected) card.focus(); }, 30);
    void it;
    return card;
  }
  return { loadFonts, classify, classifyAll, fitJob, fitAll, approve, nudge, resize, resplit, skip, sendBack, decideWords, invalidate, render, fromRecall, placementCard, renderBack, renderFront, pendingCount, reviewedCount, items, jobOf, ensureJob, setReady, writeBacks, verifyBackFile, sheetBackOutputs, fonts: F_ };
})();

/* ═══ 22 · Sets — one run, one date, one folder, one numbering across materials ═══ */
const Sets = window.Sets = (() => {
  const byRun = () => B.sets;                         // keyed `${runId}|${group}` — a run has one set per kin group
  const keyOf = (runId, group) => `${runId}|${group || "all"}`;
  /* A set used to be "the run": every sheet of every material a run made, under one number. But the only thing that
     ties two sheets together is an order with a piece on each, and the shop said so: a material no order ties to
     another is a set of its own. So a run has one set per kin group (Orders.kinGroups), each numbered on the day's
     counter in its own transaction, and a second run later the same day carries the numbering on. */
  async function ensure(runId, group) {
    const k = keyOf(runId, group);
    if (byRun().has(k)) return byRun().get(k);
    const day = today();
    let set;
    if (S.cloud.ok) { const r = await api("charmNestLibrary", { op: "setAllocate", day, runId, group: group || "" }, { label: "Numbering the set" }); set = { setId: r.setId, seq: r.seq, day: r.day || day, runId, group: group || null, name: O.setLabel(r.seq), folder: O.setFolder(r.day || day, r.seq), orders: {}, sheetIds: [], materials: [], labelFiles: [], status: "open" }; }
    else { const kk = "cn.setseq." + day; const seq = (+localStorage.getItem(kk) || 0) + 1; localStorage.setItem(kk, String(seq)); set = { setId: O.setId(day, seq), seq, day, runId, group: group || null, name: O.setLabel(seq), folder: O.setFolder(day, seq), orders: {}, sheetIds: [], materials: [], labelFiles: [], status: "open", offline: true }; }
    byRun().set(k, set);
    if (B.run && B.run.runId === runId) { B.run.setIds = [...new Set((B.run.setIds || []).concat([set.setId]))]; B.run.setId = B.run.setId || set.setId; B.run.day = set.day; B.run.seq = B.run.seq || set.seq; RunCtl.renderBanner(); }
    agent({ run: runId }, "cloud", `${set.name} allocated for ${set.day}${group ? ` · ${group.split("+").map(m => labelOf(m)).join(" + ")}` : ""} → ${set.folder}`);
    return set;
  }
  /** Every set of a run, in set order. */
  const ofRun = runId => [...byRun().values()].filter(x => x.runId === runId).sort((a, b) => (a.seq || 0) - (b.seq || 0));
  const setOfSheet = sh => sh.runId ? (byRun().get(keyOf(sh.runId, sh.group)) || (sh.setId ? [...byRun().values()].find(x => x.setId === sh.setId) : null) || null) : null;
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
    if (B.run && (B.run.setId === set.setId || (B.run.setIds || []).includes(set.setId))) { B.run.status = "review"; B.run.step = "engrave"; await RunCtl.save(B.run); RunCtl.renderBanner(); }
    agent({ run: set.runId }, "DS", `${set.name}: completion undone on the station — set back to awaiting review, every file kept`);
    Orders.render();
  }
  /** The Library's Sets view: one card per set, its sheets side by side, held orders, engraving count, label thumbnails. */
  let _cache = null;
  async function renderLibrary(body, opts) {
    const reuse = !!(opts && opts.reuse && _cache);
    if (!reuse) body.innerHTML = `<div class="libEmpty">Loading sets…</div>`;
    try {
      if (!reuse) {
        const [ss, sh] = await Promise.all([api("charmNestLibrary", { op: "setList", from: document.getElementById("libFrom").value || null, to: document.getElementById("libTo").value || null, limit: 200 }), api("charmNestLibrary", { op: "listSheets", from: document.getElementById("libFrom").value || null, to: document.getElementById("libTo").value || null, limit: 500 })]);
        _cache = { sets: ss.sets || [], sheets: sh.sheets || [] };
      }
      const sheets = _cache.sheets;
      // the metal chips and the search used to light up and change nothing here: this view read neither
      const metal = S.library.metal && S.library.metal !== "all" ? S.library.metal : null;
      const q = (document.getElementById("libSearch").value || "").trim().toLowerCase();
      let sets = _cache.sets;
      if (metal) sets = sets.filter(st => (st.materials || []).includes(metal));
      if (q) sets = sets.filter(st => `${st.name || ""} ${st.setId || ""} ${st.day || ""} ${st.runId || ""} ${Object.keys(st.orders || {}).join(" ")}`.toLowerCase().includes(q));
      document.getElementById("libCount").textContent = `${sets.length} set${sets.length === 1 ? "" : "s"}`;
      if (!sets.length) { body.innerHTML = `<div class="libEmpty">${q || metal ? "No sets match this filter." : "No sets yet."}</div>`; return; }
      body.innerHTML = "";
      for (const st of sets) {
        const all = sheets.filter(x => x.setId === st.setId).sort((a, b) => (a.metal || "").localeCompare(b.metal || "") || (a.sheetIndex || 0) - (b.sheetIndex || 0));
        const mine = metal ? all.filter(x => x.metal === metal) : all;
        const held = Object.entries(st.orders || {}).filter(([, o]) => o.held);
        const card = el("div", "setCard");
        card.innerHTML = `<div class="sh"><span class="nm">${esc(st.name || st.setId)}</span><span class="pill ${/complete/.test(st.status) ? "ok" : st.status === "labelled" ? "info" : "neutral"}">${esc(st.status || "open")}</span><span class="mono" style="font-size:11px;color:var(--ink45)">${esc(st.day)} · run ${esc(st.runId || "—")}</span><span>${mine.length} sheet(s) · ${(st.materials || []).map(m => labelOf(m)).join(", ")}</span><span>${Object.keys(st.orders || {}).length} order(s)</span><span>Engraving · ${st.backCount || mine.reduce((n, x) => n + (x.backCount || 0), 0)}</span><span class="spacer"></span>${st.labels && st.labels.pdf ? `<a class="btn ghost xs" href="${st.labels.pdf.url}" target="_blank" rel="noopener">labels PDF</a>` : ""}${st.labels && st.labels.manifest ? `<a class="btn ghost xs" href="${st.labels.manifest.url}" target="_blank" rel="noopener">manifest</a>` : ""}${st.labels && st.labels.json ? `<a class="btn ghost xs" href="${st.labels.json.url}" target="_blank" rel="noopener">set.json</a>` : ""}${/complete/.test(st.status) ? `<button class="btn ghost xs" data-undo="${esc(st.setId)}">Undo set</button>` : ""}</div>
          <div class="sheetsRow">${mine.map(r => `<div class="libCard hoverItem" data-m="${r.metal}" data-id="${r.id}" title="${esc(r.folder || r.id)}">${window.sheetHead ? sheetHead(r, { inFan: true }) : `<div class="h"><span class="nm">${esc(r.folder || r.id)}</span></div>`}${r.preview ? `<img class="pv" crossorigin="anonymous" src="${r.preview}" loading="lazy" alt="">` : `<div class="pv ph">no preview</div>`}<div class="m"><span><b>${r.placedCount}</b>/${r.charmCount}</span><span><b>${Math.round((r.density || 0) * 100)}%</b></span><span>${(r.orders || []).length} orders</span>${r.backCount ? `<span>✎ ${r.backCount}</span>` : ""}<span class="pill ${r.status === "complete" ? "ok" : "bad"}" style="padding:2px 7px">${esc(r.status)}</span></div></div>`).join("") || "<div class='libEmpty'>no sheets recorded</div>"}</div>
          ${held.length ? `<div class="holds"><b>Held:</b> ${held.map(([rid, o]) => `${esc(rid)} — ${esc(o.held.why || "")}`).join(" · ")}</div>` : ""}
          ${st.refused && st.refused.length ? `<div class="holds"><b>Refused by the station:</b> ${st.refused.map(r => `${esc(r.id)} — ${esc(r.reason)}`).join(" · ")}</div>` : ""}
          <div class="labels">${(st.labelFiles || []).map(f => f.url ? `<img crossorigin="anonymous" src="${f.url}" title="${esc(f.label || f.sheet)}" data-big="${f.url}" alt="">` : "").join("")}</div>`;
        card.querySelectorAll(".libCard").forEach(x => x.onclick = () => openLibrarySheet(x.dataset.id));
        card.querySelectorAll("[data-big]").forEach(img => img.onclick = () => { const d = document.createElement("dialog"); d.className = "wide"; d.innerHTML = `<div class="dlg"><div class="dlgHead"><h3>${esc(img.title)}</h3><div class="right"><button class="btn ghost xs">Close</button></div></div><div class="dlgBody" style="display:grid;place-items:center"><img crossorigin="anonymous" class="labelBig" src="${img.dataset.big}" alt=""></div></div>`; d.querySelector("button").onclick = () => d.close(); d.addEventListener("close", () => d.remove()); document.body.appendChild(d); d.showModal(); });
        const ub = card.querySelector("[data-undo]"); if (ub) ub.onclick = async () => { if (!confirm(`Undo the completion of ${st.name}? The orders return to the station's list; every file is kept.`)) return; const local = [...byRun().values()].find(x => x.setId === st.setId) || Object.assign({ orders: {}, sheetIds: st.sheetIds || [], materials: st.materials || [], labelFiles: st.labelFiles || [] }, st); byRun().set(local.runId || st.setId, local); try { await undo(local); toast(`${st.name} undone`, "ok"); renderLibrary(body); } catch (e) { toast(e.message, "bad", 6000); } };
        body.appendChild(card);
      }
    } catch (e) { body.innerHTML = `<div class="libEmpty">Could not load sets: ${esc(e.message)}</div>`; }
  }
  return { ensure, ofRun, keyOf, onSheetSaved, finalize, commit, undo, evaluate, save, renderLibrary, renderLabelPng, sheetsOf, setOfSheet, byRun };
})();

/* ═══ 23 · RunCtl — the two halves, the record, resume, stop, Auto/Manual ═══ */
const RunCtl = window.RunCtl = (() => {
  /* A run used to halt after five of its own steps and ask, with a button named after the source code, whether to do
     the next one. Nobody could answer that question usefully, and nothing was gained by asking it: a run that has been
     started is a run that should finish. It now runs straight through and stops only where a person is genuinely
     needed — a decision in Review or Engraving, a commit held back on purpose, or something gone wrong.
     Manual and Auto still differ, in the one place the difference means anything: Auto starts the next run when this
     one is done, Manual does not. */
  const PAUSE_AFTER = new Set();
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
      /* A step that fails for a passing reason — the station did not answer, the network dropped, a function answered
         5xx — is tried again, twice, with a breath in between, before the run stops. The stages know what to expect
         of each other; a slow reply is not a reason to leave a whole day's orders standing. */
      let outcome = null;
      for (let attempt = 0; ; attempt++) {
        try { outcome = await doStep(r, step); break; }
        catch (e) {
          const passing = /answer in time|timed out|network|Failed to fetch|HTTP 5\d\d|link is down|no reply|closed/i.test(e.message || "");
          if (!passing || attempt >= 2 || r.status !== "running") throw e;
          agent({ run: r.runId }, "warn", `${step}: ${e.message} — trying again (${attempt + 1} of 2)`);
          await new Promise(res => setTimeout(res, 4000 * (attempt + 1)));
        }
      }
      if (r.status !== "running") break;
      if (outcome && outcome.done) break;
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
      case "pool": { const n = await Pool.addAll(r); if (!n) { const w = Orders.rows().filter(x => x.state === "waiting").length; if (w) { r.status = "complete"; r.finishedAt = Date.now(); r.nothingToCut = true; await save(r); renderBanner(); agent({ run: r.runId }, "DS", `Nothing goes to the laser today: ${w} line(s) wait for a full sheet or a slow metal's day`); return { goto: null, done: true }; } stop("nothing could be pooled — every line needs a decision", "Resolve the items in Review, then Resume."); return; } Engrave.classifyAll(r).catch(e => agent({ run: r.runId }, "warn", `classifier: ${e.message}`)); return; }
      case "plan": { for (const m of METALS) { const pg = activePage(m.key); if (!pg.charms.some(c => c.poolId)) continue; const sat = computeSaturation(pg); agent({ metal: m.key, run: r.runId }, "info", `${labelOf(m.key)}: ${sat.count} piece(s) need ${fmt.pct(sat.totalNeeded / sat.usable)} of the plate — ${sat.recommend ? "over the " + fmt.pct(sat.rho.cap) + " ceiling, the overflow goes to a second sheet" : "under the " + fmt.pct(sat.rho.cap) + " ceiling"}`); } return; }
      case "nest": { await nestAll(r); return; }
      case "checkpoint": { const sets = Sets.ofRun(r.runId); for (const set of sets) { set.status = "awaiting review"; await Sets.save(set); } r.setIds = sets.map(x => x.setId); r.setId = r.setIds[0] || r.setId || null; r.status = "running"; await save(r); return; }
      case "engrave": { await Engrave.classifyAll(r); await Engrave.fitAll(r); await waitForReview(r); return; }
      case "revalidate": { const v = await Orders.revalidate(r, "before labels"); if (v.changed.length && [...Engrave.items().values()].some(j => j.state === "classify")) { agent({ run: r.runId }, "warn", `${v.changed.length} order(s) changed — back to engraving`); return { goto: "engrave" }; } return; }
      case "labels": { for (const set of Sets.ofRun(r.runId)) await Sets.finalize(set); return; }
      case "commit": {
        if (S.settings.autoCommit === "off" && !r.commitRequested) { r.status = "paused"; r.awaitCommit = true; await save(r); renderBanner(); agent({ run: r.runId }, "DS", "Auto-commit is off — press Commit set when ready"); return; }
        const v = await Orders.revalidate(r, "before commit");
        if (v.changed.length && [...Engrave.items().values()].some(j => j.state === "classify")) { agent({ run: r.runId }, "warn", `${v.changed.length} order(s) changed before the commit — back to engraving`); r.commitRequested = false; return { goto: "engrave" }; }
        const all = { completed: [], refused: [], held: {} };
        for (const set of Sets.ofRun(r.runId)) { const res = await Sets.commit(set, r); all.completed.push(...res.completed); all.refused.push(...res.refused); Object.assign(all.held, res.held || {}); }
        r.committed = all.completed; r.refused = all.refused; r.holds = all.held; return;
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
      /* A sheet's trouble belongs on that sheet's card. The banner used to carry the whole message — "GF 14/20 · sheet 3:
         bad row — Fix the cause (see the card's log), then Resume" — across every tab, above every list, for the rest of
         the session. Now the banner names the sheet and offers the way to it; the reason is written where the sheet is. */
      if (err) { sh.problem = err.message; CN.renderCard(sh); stop(`${sheetName(sh)} could not be saved`, "", { metal: sh.metal, page: sh.page }); return; }
      if (["complete", "partial"].includes(sh.status) && sh.verification && !sh.verification.ok) { sh.problem = "verification flagged — open the report"; CN.renderCard(sh); stop(`${sheetName(sh)} needs a look`, "", { metal: sh.metal, page: sh.page }); return; }
      if (sh.endedBy === "stopped") { sh.problem = "nesting was stopped here"; CN.renderCard(sh); stop(`${sheetName(sh)} was stopped`, "", { metal: sh.metal, page: sh.page }); return; }
      save(r).catch(() => {});
    }
    if (!waiter || waiter.r !== r) return;
    const pages = allSheets().filter(pg => pg.runId === r.runId && pg.charms.some(c => c.poolId && !c.excluded));
    const busy = pages.some(pg => ["nesting", "finishing", "queued", "ready", "idle"].includes(pg.status) || pg.dirty || (pg.persisted && !pg.persistedDone));
    for (const pg of pages) if (pg.persisted && !pg.persistedDone) pg.persisted.then(() => { pg.persistedDone = true; onSheetDone(null); }, () => { pg.persistedDone = true; onSheetDone(null); });
    if (busy) return;
    const notWritten = pages.filter(pg => !pg.fileBase);
    if (notWritten.length) { const one = notWritten[0]; one.problem = "not written to the cloud"; CN.renderCard(one); stop(`${notWritten.length === 1 ? sheetName(one) : notWritten.length + " sheets"} not written`, "", { metal: one.metal, page: one.page }); waiter = null; return; }
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
  /* Auto used to finish a set, blank the screen and refill it minutes later with no countdown anywhere — a person came
     back from the bench to an empty app and could not tell whether the shift was done or something had crashed. */
  const NEXT = { at: 0, t: 0 };
  function armNext(ms) { NEXT.at = Date.now() + ms; clearInterval(NEXT.t); NEXT.t = setInterval(() => { if (!NEXT.at) { clearInterval(NEXT.t); return; } renderBanner(); }, 1000); }
  function cancelNext() { NEXT.at = 0; clearInterval(NEXT.t); clearTimeout(autoTimer); renderBanner(); }
  /** A run that is given up lets go of its lines: its pool rows are marked abandoned and the station's claims are lifted,
      so the next run can take them without waiting a day for the rows to go stale. */
  function releaseRun(r) {
    const ids = [...new Set(Orders.rows().filter(x => x.poolIds && x.poolIds.length).flatMap(x => x.poolIds))];
    if (ids.length && S.cloud.ok) api("charmNestLibrary", { op: "poolUpdate", poolIds: ids, patch: { state: "abandoned" } }, { quiet: true }).catch(() => {});
    Orders.unclaim([...new Set(Orders.rows().map(x => x.order.receiptId))]).catch(() => {});
    r.status = "abandoned"; save(r).catch(() => {});
  }
  function stop(why, fix, at) { const r = B.run; if (!r) return; r.status = "stopped"; r.stoppedBy = why; r.fix = fix || null; r.at = at || null; r.errors.push({ t: Date.now(), why }); if (waiter && waiter.r === r) { const w = waiter; waiter = null; w.resolve(); } reviewWaiter = null; agent({ run: r.runId }, "warn", `Run stopped: ${why}${fix ? " — " + fix : ""}`); toast(`Run stopped: ${why}`, "bad", 8000); notifyPerson("Charm Sorter run stopped", why); save(r).catch(() => {}); renderBanner(); }
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
    const m = /^a\s*(\d+)/i.exec(pick.trim());
    if (m) {
      const x = open[+m[1] - 1]; if (!x) return;
      if (!confirm(`Give up run ${x.runId.slice(-8)}?\n\n${x.lines} line(s) at step ${x.step}. Anything decided but not written is lost.\n\nThe sheets and files already saved are kept.`)) return;
      await api("charmNestLibrary", { op: "runPut", run: { runId: x.runId, status: "abandoned", abandonedAt: Date.now() }, merge: true });
      toast(`${x.runId} abandoned`, "ok");
      return;
    }
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
      if (rec.setIds && rec.setIds.length || rec.setId) await restoreRunSheets(rec);
      await Pool.addAll(rec);                                              // idempotent: existing pool rows are the same ids; only unplaced lines get charms
      if (rec.step === "nest" || rec.step === "checkpoint") rec.step = "nest";
    }
    await save(rec); renderBanner();
    loop().catch(e => stop(e.message, "Fix the cause and press Resume."));
  }
  async function restoreRunSheets(rec) {
    const setIds = [...new Set((rec.setIds || []).concat(rec.setId ? [rec.setId] : []))];
    const sets = [];
    for (const sid of setIds) {
      const sr = await api("charmNestLibrary", { op: "setGet", setId: sid }); const sd = sr.set; if (!sd) continue;
      const set = { setId: sd.setId, seq: sd.seq, day: sd.day, runId: rec.runId, group: sd.group || null, name: sd.name || O.setLabel(sd.seq), folder: sd.folder || O.setFolder(sd.day, sd.seq), orders: Object.fromEntries(Object.entries(sd.orders || {}).map(([rid, o]) => [rid, { held: o.held || null, lines: Object.fromEntries((o.lines || []).map(l => [l.transactionId, l])) }])), sheetIds: sd.sheetIds || [], materials: sd.materials || [], labelFiles: sd.labelFiles || [], labels: sd.labels || null, status: sd.status || "open", committed: sd.committed || null, refused: sd.refused || null, backCount: sd.backCount || 0 };
      Sets.byRun().set(Sets.keyOf(rec.runId, set.group), set); sets.push(set);
    }
    if (!sets.length) return null;
    const bySet = new Map(sets.map(x => [x.setId, x]));
    const ls = await api("charmNestLibrary", { op: "listSheets", limit: 500, runId: rec.runId });

    for (const slim of ls.sheets || []) {
      const d = (await api("charmNestLibrary", { op: "getSheet", id: slim.id })).sheet; if (!d) continue;
      const prim = S.sheets[d.metal]; let pg = prim.pages.find(p => p.sheetId === d.id) || (prim.pages[0].charms.length ? addPage(d.metal) : prim.pages[0]);
      const set = bySet.get(d.setId) || sets[0];
      pg.sheetId = d.id; pg.runId = rec.runId; pg.group = set.group || null; pg.setId = set.setId; pg.seq = d.setSeq || set.seq; pg.setDay = d.day; pg.sheetIndex = d.sheetIndex; pg.fileBase = d.fileBase; pg.folderPath = `${set.folder}/${d.fileBase}`; pg.label = d.label || null; pg.backPool = d.backPool || []; pg.backOutputs = d.backOutputs || null; pg.cloud = d.outputs ? { ai: d.outputs.ai && d.outputs.ai.url, pdf: d.outputs.pdf && d.outputs.pdf.url, labelled: d.outputs.labelled && d.outputs.labelled.url, report: d.outputs.report && d.outputs.report.url, preview: d.outputs.preview && d.outputs.preview.url } : null;
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
    agent({ run: rec.runId }, "cloud", `Restored ${ls.sheets.length} sheet(s) of ${sets.map(x => x.name).join(", ")} from their records`);
    return sets[0];
  }
  function onComplete(r) {
    agent({ run: r.runId }, "ok", `Run ${r.runId} complete: ${(r.committed || []).length} order(s) committed · ${Object.keys(r.holds || {}).length} held · ${(r.refused || []).length} refused`);
    toast(`Set complete — ${(r.committed || []).length} order(s) marked design-complete`, "ok", 7000); ding && ding();
    if (S.settings.runMode === "auto" && +S.settings.autoEvery > 0) { const every = Math.max(5, +S.settings.autoEvery); clearTimeout(autoTimer); autoTimer = setTimeout(() => { NEXT.at = 0; clearInterval(NEXT.t); if (S.settings.runMode === "auto") { clearRunState(); start({ mode: "auto" }); } }, every * 60000); armNext(every * 60000); agent({ bridge: true }, "DS", `Auto: the next run starts in ${every} min (never under 5, to spare the Etsy API)`); }
  }
  /** After a set is done: clear the cards and pooled lines so the next run starts clean (files and records are kept). */
  function clearRunState() { for (const m of METALS) { const prim = S.sheets[m.key]; if (prim.pages.some(p => p.charms.some(c => c.poolId))) { for (const pg of prim.pages.slice()) { if (pg.status === "nesting") stopNest(pg); pg.charms = pg.charms.filter(c => !c.poolId); pg.sheetId = null; pg.fileBase = null; pg.setId = null; pg.runId = null; } prim.pages = [prim]; prim.active = 0; prim.el = prim.cardEl; sheetDirty(prim); } } B.orders.rows = []; B.orders.byKey = new Map(); B.engrave.items = new Map(); B.review.items = []; B.pool.rows = new Map(); B.run = null; B.orders.recalled = null; B.orders.pulledAt = null; B.orders.filtered = 0; B.orders.stale = false; Orders.render(); Engrave.render(); Review.render(); renderBanner(); renderRail(); updateTopSub(); }
  function setRunMode(mode) {
    S.settings.runMode = mode === "auto" ? "auto" : "manual"; saveSettings(); renderModeBtn();
    if (mode === "auto") { agent({ bridge: true }, "DS", "Auto mode on: the sorter pulls the latest orders by the date rule and runs the whole process, stopping only for a person"); if (!B.run || ["complete", "stopped"].includes(B.run.status)) { if (B.run && B.run.status === "complete") clearRunState(); start({ mode: "auto" }).catch(e => toast(e.message, "bad")); } else if (B.run.status === "paused") { B.run.mode = "auto"; next(); } else B.run.mode = "auto"; }
    else { clearTimeout(autoTimer); if (B.run) B.run.mode = "manual"; agent({ bridge: true }, "DS", "Manual mode: every step waits for a click"); }
    renderBanner();
  }
  function renderModeBtn() { const b = document.getElementById("btnRunMode"); if (!b) return; const auto = S.settings.runMode === "auto"; b.classList.toggle("auto", auto); document.getElementById("runModeText").textContent = auto ? "Auto" : "Manual"; }
  const STEP_WORDS = { pull: "Pulling orders", claim: "Claiming", pool: "Pooling", plan: "Planning", nest: "Nesting", checkpoint: "Checking the sheets", engrave: "Engraving", revalidate: "Re-checking the orders", labels: "Writing labels", commit: "Committing", complete: "Complete" };
  function stepDetail(r) {
    const sh = allSheets().filter(p => p.runId === r.runId);
    if (r.step === "nest" && sh.length) return ` · sheet ${Math.min(sh.filter(p => ["complete", "partial"].includes(p.status)).length + 1, sh.length)} of ${sh.length}`;
    if (r.step === "pool") return ` · ${Orders.rows().filter(x => x.poolIds && x.poolIds.length).length} of ${Orders.rows().filter(x => x.state !== "gone").length} lines`;
    if (r.step === "pull" || r.step === "claim") return ` · ${Orders.rows().filter(x => x.state !== "gone").length} lines`;
    return "";
  }
  function renderBanner() {
    if (window.guardBench) guardBench();
    const h = document.getElementById("runBanner"); if (!h) return; const r = B.run;
    LiveStrip.render();                                     // one path: the banner and the ladder can never disagree
    if (!r) {
      // a set has finished and Auto will start another: the banner stays, so nobody comes back to a blank app
      if (NEXT.at > Date.now()) {
        const left = Math.max(0, NEXT.at - Date.now()), mm = Math.floor(left / 60000), ss = Math.floor(left % 60000 / 1000);
        h.classList.remove("hidden"); h.className = "runBanner done";
        h.innerHTML = `<span class="why"><b>Set finished</b> · the next run starts in ${mm}:${String(ss).padStart(2, "0")}</span><span class="spacer"></span><span class="acts"><button class="btn gold sm" id="rbNow" title="start the next run now instead of waiting">Run now</button><button class="btn ghost sm" id="rbCancelNext" title="do not start another run on its own">Cancel</button></span>`;
        h.querySelector("#rbNow").onclick = () => { cancelNext(); clearRunState(); start({ mode: "auto" }).catch(e => toast(e.message, "bad")); };
        h.querySelector("#rbCancelNext").onclick = () => cancelNext();
        Dock.schedule(); return;
      }
      // a run left open by a reload or a closed tab is offered here, not in a toast pointing at another tab's button
      if (B.openRuns && B.openRuns.length) {
        h.classList.remove("hidden"); h.className = "runBanner review";
        h.innerHTML = `<span class="why"><b>${B.openRuns.length} open run${B.openRuns.length === 1 ? "" : "s"}</b> · left from an earlier session</span><span class="spacer"></span><span class="acts">${B.openRuns.slice(0, 3).map(x => `<button class="btn gold sm" data-rbres="${esc(x.runId)}" title="carry on from ${esc(x.step)} · ${x.lines} line(s) · ${new Date(x.updatedAt).toLocaleString()}">Resume ${esc(x.runId.slice(-8))} · ${esc(x.step)}</button>`).join("")}<button class="btn ghost sm" id="rbLater" title="leave these for later — they stay on record">Not now</button></span>`;
        h.querySelectorAll("[data-rbres]").forEach(b => b.onclick = () => { B.openRuns = null; resumeRun(b.dataset.rbres).catch(e => toast(e.message, "bad", 7000)); });
        h.querySelector("#rbLater").onclick = () => { B.openRuns = null; renderBanner(); };
        Dock.schedule(); return;
      }
      // nothing is running, so there is nothing to report: the banner is not a place to advertise from
      h.classList.add("hidden"); Dock.schedule(); return;
    }
    h.classList.remove("hidden"); h.className = "runBanner" + (r.status === "stopped" ? " stopped" : r.status === "complete" ? " done" : r.status === "review" ? " review" : "");
    const idx = O.stepIndex(r.step);
    const reviewN = Review.count(), engN = Engrave.pendingCount();
    const waitingFor = [reviewN ? `${reviewN} in Review` : "", engN ? `${engN} in Engraving` : ""].filter(Boolean).join(" · ") || "nothing";
    const why = r.status === "stopped" ? `<b>Stopped:</b> ${esc(r.stoppedBy || "")}${r.fix ? ` — <span>${esc(r.fix)}</span>` : ""}` : r.status === "review" ? `<b>Waiting for a person:</b> ${waitingFor}` : r.status === "paused" ? `<b>Ready to commit</b> — every sheet written, every engraving decided` : r.status === "complete" ? `<b>Complete</b> · ${(r.committed || []).length} committed · ${Object.keys(r.holds || {}).length} held` : `<b>${esc(STEP_WORDS[r.step] || r.step)}</b>${esc(stepDetail(r))}`;
    Dock.schedule();
    h.title = `run ${r.runId}`;
    /* Which run is this? Three cards on the Nest tab and a banner that named only a step left no way to tell this
       morning's set from yesterday's. The set, the day and the size of the run now lead it. */
    const nSheets = Object.keys(r.sheets || {}).length;
    const seqOf = x => x.seq || +((/-(\d+)$/.exec(String(x.setId || "")) || [])[1] || 0) || null;
    const who = [seqOf(r) ? `Set ${seqOf(r)}` : "", r.day ? new Date(r.day + "T12:00:00").toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "",
      `${Object.keys(r.lines || {}).length || Orders.rows().filter(x => x.state !== "gone").length} lines`, nSheets ? `${nSheets} sheet${nSheets === 1 ? "" : "s"}` : ""].filter(Boolean).join(" \u00b7 ");
    h.innerHTML = `<span class="rid" title="run ${esc(r.runId)}${r.setId ? " \u00b7 set " + esc(r.setId) : ""}">${esc(who)}</span><span class="why">${why}</span><span class="spacer"></span><span class="acts">
      ${r.at ? `<button class="btn ghost sm" id="rbAt" title="open the sheet this is about">Show the sheet</button>` : ""}
      ${r.status === "paused" && r.awaitCommit ? `<button class="btn sage sm" id="rbCommit" title="mark every order in the set design-complete on the station">Commit set</button>` : ""}${r.status === "stopped" && /sign/i.test(r.stoppedBy || "") ? `<button class="btn gold sm" id="rbConnect" title="sign the Design Station back in to Etsy, then the run can carry on">Connect Etsy</button>` : ""}${r.status === "stopped" ? `<button class="btn gold sm" id="rbResume" title="carry on from the step this run stopped at">Resume</button>` : ""}${["running", "review", "paused"].includes(r.status) ? `<button class="btn ghost sm" id="rbStop" title="stop after the step in progress — the run can be resumed from where it stopped">Stop</button>` : ""}${r.status === "complete" ? `<button class="btn ghost sm" id="rbClear" title="take the finished run off the cards — its files and records are kept">Clear run</button>` : ""}${r.status !== "complete" ? `<button class="btn ghost sm" id="rbAbandon" title="give this run up and take the banner away — the sheets and files already saved are kept">Abandon run</button>` : ""}</span>`;
    const q = id => h.querySelector("#" + id);
    const rid = h.querySelector(".rid"); if (rid) { rid.tabIndex = 0; rid.title += " \u2014 click for every run on record"; rid.onclick = () => RunHistory.show(); rid.onkeydown = e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); RunHistory.show(); } }; }
    if (q("rbAt")) q("rbAt").onclick = () => { CN.setMode("nest"); const i = CN.pagesOf(r.at.metal).findIndex(p => p.page === r.at.page); CN.showPage(r.at.metal, Math.max(0, i)); };
    if (q("rbConnect")) q("rbConnect").onclick = () => DesignLink.connectEtsy().catch(e => toast(e.message, "bad", 6000)); 
    if (q("rbCommit")) q("rbCommit").onclick = () => commitNow(); if (q("rbResume")) q("rbResume").onclick = () => resume(); if (q("rbStop")) q("rbStop").onclick = () => stop("stopped by the operator", "Press Resume to carry on from the recorded step."); if (q("rbClear")) q("rbClear").onclick = () => { if (confirm("Clear the finished run from the cards? Files and records are kept.")) clearRunState(); };
    if (q("rbAbandon")) q("rbAbandon").onclick = () => {
      const eng = [...Engrave.items().values()].filter(j => ["approved", "words", "review"].includes(j.state) && !j.backs).length;
      const rev = Review.count();
      const lost = [eng ? `${eng} engraving decision${eng === 1 ? "" : "s"}` : "", rev ? `${rev} review decision${rev === 1 ? "" : "s"}` : ""].filter(Boolean).join(" and ");
      if (confirm(`Give up run ${r.runId.slice(-8)}?${lost ? `\n\n${lost} made in this run are not yet written and will be lost.` : ""}\n\nThe sheets and files already saved are kept.`)) { releaseRun(r); clearRunState(); }
    };
    Orders.render();
  }
  return { start, next, resume, stop, stopIfRunning, poke, save, onSheetDone, pickResume, resumeRun, restoreRunSheets, commitNow, setMode: setRunMode, setRunMode, renderBanner, renderModeBtn, clearRunState, run };
})();

/* ═══ 24 · Review — every decision a person must make ═════════════════════ */
const Review = window.Review = (() => {
  const items = () => B.review.items;
  const mine = it => !String(it.key || "").startsWith("eng:") && !(it.row && it.row.state === "gone");   // engraving is the Engraving tab's
  const isNotice = it => String(it.key || "").startsWith("held:");        // an order left open, not a decision to make
  const count = () => items().filter(it => mine(it) && !isNotice(it)).length;
  function add(it) { const i = items().findIndex(x => x.key === it.key); const fresh = i < 0; if (fresh) items().push(Object.assign({ t: Date.now() }, it)); else items()[i] = Object.assign(items()[i], it); if (fresh && B.run && ["review", "paused", "stopped"].includes(B.run.status)) notifyPerson("Charm Sorter needs a person", it.why || it.kind); render(); LiveStrip.render(); RunCtl.renderBanner(); }
  const settled = [];                                                     // what this shift has answered, newest first
  function remove(key, how) {
    const n = items().length;
    const gone = items().find(x => x.key === key);
    B.review.items = items().filter(x => x.key !== key);
    if (n === items().length) return;
    if (gone && !/^(eng|held):/.test(String(key))) settled.unshift({ key, kind: gone.kind, why: gone.why || "", lines: (gone.rows || [gone.row]).filter(Boolean).length, orders: [...new Set((gone.rows || [gone.row]).filter(Boolean).map(r2 => r2.order.receiptId))], by: how || employeeName() || "", t: Date.now() });
    if (settled.length > 200) settled.length = 200;
    render(); LiveStrip.render(); RunCtl.renderBanner();
  }
  function problemText(p) { return p.kind === "needsMaterial" ? `needs material (${p.metalLabel || "none"})` : p.kind === "needsMapping" ? `option "${p.optionName}: ${p.optionValue}" not mapped` : p.kind === "unmatchedSku" ? `SKU ${p.sku || "?"}: ${p.reason}` : p.kind === "blockedSku" ? `SKU ${p.sku} blocked: ${p.reason}` : p.kind === "missingSize" ? `no design for size ${p.size || "(none)"} (have ${(p.available || []).join(", ")})` : p.kind === "oversize" ? `oversize for the ${labelOf(p.material)} plate` : p.kind; }
  /** The key of the DECISION a problem asks for, not of the line that raised it. An unknown SKU is one decision however
   *  many orders bought it; an unmapped option is one decision however many lines carry it. A run that raised 180 of the
   *  first and 79 of the second showed 259 items where 148 decisions were waiting. */
  const SEP = "\u0000";
  function decisionKey(row, p) {
    if (p.kind === "unmatchedSku") return p.sku ? `ord:sku:${p.sku}` : `ord:listing:${p.listingId || row.key}`;
    if (p.kind === "blockedSku") return `ord:blocked:${p.sku}`;
    if (p.kind === "needsMapping") return `ord:opt:${p.optionName}${SEP}${p.optionValue}`;
    if (p.kind === "needsMaterial") return `ord:mat:${p.listingId || row.key}${SEP}${p.metalLabel || ""}`;
    return `ord:${row.key}:${p.kind}`;                                   // a size or a plate is this charm's own
  }
  /** Order-level items follow the rows' problems: added when a problem appears, removed when it is fixed. */
  function syncOrderItems() {
    const keep = new Map();
    // a row a person parked is parked: it used to re-raise its decision the moment the next card was answered, because
    // interpretAll recomputes problems from scratch and sync rebuilt the queue from them
    for (const row of Orders.rows()) { if (row.state === "gone" || row.hold) continue; for (const p of row.problems || []) {
      const key = decisionKey(row, p);
      if (!keep.has(key)) keep.set(key, { kind: p.kind, key, row, problem: p, rows: [], why: problemText(p) });
      const it = keep.get(key); if (!it.rows.includes(row)) it.rows.push(row);
    } }
    for (const [key, it] of keep) { const had = items().find(x => x.key === key); if (had) Object.assign(had, { rows: it.rows, row: it.row, problem: it.problem, why: it.why }); else add(it); }
    B.review.items = items().filter(x => !x.key.startsWith("ord:") || keep.has(x.key));
    // a held notice is a notice, not a decision: it goes when the order it names is committed, gone, or no longer held
    const byRid = new Map();
    for (const r of Orders.rows()) { const k = r.order.receiptId; if (!byRid.has(k)) byRid.set(k, []); byRid.get(k).push(r); }
    B.review.items = items().filter(x => {
      if (!isNotice(x)) return true;
      const lines = byRid.get(x.rid); if (!lines || !lines.length) return false;
      if (lines.every(l => ["committed", "gone"].includes(l.state))) return false;
      const held = lines.find(l => l.hold);
      if (!held && !lines.some(l => l.problems && l.problems.length)) return false;
      x.note = held ? held.hold : (lines.find(l => l.reason) || {}).reason || "unresolved line";
      x.line = held ? held.key : (lines.find(l => l.problems && l.problems.length) || lines[0]).key;
      return true;
    });
    render(); LiveStrip.render(); RunCtl.renderBanner();
  }
  /** Every line the item speaks for — the group when it has one, the single row otherwise. */
  const rowsOf = it => (it.rows && it.rows.length ? it.rows : it.row ? [it.row] : []).filter(r => r.state !== "gone");
  /** Apply one decision to every line it covers, then re-pool them together. */
  async function repoolAll(it, before) {
    const rows = rowsOf(it);
    for (const r of rows) if (before) before(r);
    for (const r of rows) await repool(r);
  }
  function focus(rowKey) { RV.filter = null; render(); const c = document.querySelector(`#reviewView [data-row="${CSS.escape(rowKey)}"]`); if (c) { c.scrollIntoView({ behavior: "smooth", block: "center" }); c.classList.add("pulse"); setTimeout(() => c.classList.remove("pulse"), 1300); } }
  async function repool(row) { row.problems = []; row.state = "pulled"; row.reason = null; row.hold = null; Orders.interpretAll(); if (row.problems.length) { Orders.render(); return; } if (B.run && O.stepIndex(B.run.step) >= O.stepIndex("pool")) { try { await Pool.poolAdd(row, B.run); } catch (e) { row.state = "held"; row.reason = e.message; } if (row.state === "pooled" && row.spec.engraveCandidate) Engrave.classify(row).catch(() => {}); } syncOrderItems(); Orders.render(); renderRail(); updateTopSub(); refreshAllCards(); if (OrderWin.isOpen()) OrderWin.paint(); RunCtl.poke(); }
  const by = () => employeeName() || askEmployee();
  /** What Claude decided, in one line and one number: a reading is either text to engrave or a note to the shop. */
  function claudeVerdict(j) {
    const pct = Math.round((j.confidence || 0) * 100);
    const text = (j.text || "").trim();
    const from = SOURCE_LABEL[j.source] != null ? SOURCE_LABEL[j.source] : esc(String(j.source || ""));
    if (!text) return `<b>nothing to engrave</b> — what the customer wrote reads as a note to the shop, not words for the charm <i style="color:var(--ink45)">· ${pct}% sure it is not engraving${j.quote ? ` · "${esc(j.quote)}"` : ""}</i>`;
    return `<b>engrave this</b>${from ? ` — read from ${from}` : ""} <i style="color:var(--ink45)">· ${pct}% sure${j.quote ? ` · "${esc(j.quote)}"` : ""}</i>`;
  }
  /** A primary action whose field is empty cannot be pressed, and says so, instead of returning in silence. */
  function bindNeeds(c, action, field) {
    const b = c.querySelector(`[data-a=${action}]`), f = c.querySelector(`[data-f=${field}]`);
    if (!b || !f) return;
    const sync = () => { const empty = !String(f.value || "").trim(); b.disabled = empty; b.title = empty ? "fill the field beside it first" : ""; };
    f.addEventListener("input", sync); f.addEventListener("change", sync); sync();
  }
  function card(it) {
    const c = el("div", "rvItem hoverItem"); c.dataset.kind = it.kind; if (it.row) { c.dataset.row = it.row.key; c.dataset.rid = String(it.row.order.receiptId); }
    const r = it.row, sp = r && r.spec, p = it.problem || {};
    const group = rowsOf(it);
    const orders = [...new Set(group.map(x => x.order.receiptId))];
    const scope = group.length > 1
      ? `<span class="pill neutral" title="${esc(orders.slice(0, 20).join(" · ") + (orders.length > 20 ? " …" : ""))}">${group.length} lines · ${orders.length} order${orders.length === 1 ? "" : "s"} · one decision</span>` : "";
    const head = (kind, ttl, sub) => `<div class="rh"><span class="kind">${esc(kind)}</span><span class="ttl">${esc(ttl)}</span><span class="sub">${esc(sub || "")}</span>${scope}</div>`;
    const evRow = (lbl, val) => `<div><span class="lbl">${esc(lbl)}</span>${val}</div>`;
    const orderSub = r ? `${r.order.receiptId} · ${sp && sp.designSku || r.line.sku || "no SKU"} · ${r.line.title}` : "";
    if (it.kind === "needsMaterial") {
      c.innerHTML = head("Needs material", r.order.receiptId, orderSub) + `<div class="ev">${evRow("Station read", esc(p.metalLabel || "nothing"))}${evRow("Options", (r.line.variations || []).map(v => `<q>${esc(v.name)}: ${esc(v.value)}</q>`).join(" "))}${evRow("Title", esc(r.line.title))}</div><div class="why">${esc(it.why)}</div>
        <div class="fixes"><select data-f="mat"><option value="">pick a material…</option>${METALS.map(m => `<option value="${m.key}">${esc(m.label)}</option>`).join("")}</select><button class="btn gold sm" data-a="mat">Use it (writes a staff note)</button><button class="btn ghost sm" data-a="skip">Skip line</button><button class="btn ghost sm" data-a="hold">Hold order</button></div>`;
      bindNeeds(c, "mat", "mat");
      c.querySelector("[data-a=mat]").onclick = async () => { const m = c.querySelector("[data-f=mat]").value; if (!m) return; const who = by(); if (!who) return; row_material(it, m, who); };
    } else if (it.kind === "needsMapping") {
      /* This was a dropdown, a free-text box, a second dropdown and a button that did nothing at all until you had
         guessed the exact word it wanted. The question only ever has a handful of answers, so they are the buttons:
         one press decides it, and "Something else" opens the old typing for the rare value none of them covers. */
      const lids = [...new Set(group.map(x => String(x.line.listingId)))];
      const CHOICES = [["necklace", "form", "Necklace"], ["earrings", "form", "Earrings"], ["huggie", "form", "Huggie"], ["charm", "form", "Charm only"], ["bracelet", "form", "Bracelet"], ["keychain", "form", "Keychain"]];
      const guess = CharmNestOrders.FORM_VALUES[CharmNestOrders.norm(p.optionValue)] || null;
      c.innerHTML = head("Needs mapping", `${p.optionName}: ${p.optionValue}`, `${lids.length === 1 ? "listing " + p.listingId : lids.length + " listings"} · ${p.title || r.line.title}`) +
        `<div class="ask">What does this option decide?</div>
        <div class="fixes pick">${CHOICES.map(([v, f, lbl]) => `<button class="btn ${v === guess ? "gold" : "ghost"} sm" data-pick="${v}" data-field="${f}">${lbl}</button>`).join("")}<button class="btn ghost sm" data-a="ignore" title="it changes nothing about what gets made">Nothing — ignore it</button><button class="btn ghost sm" data-a="other">Something else…</button></div>
        <div class="fixes other hidden"><select data-f="field"><option value="form">form</option><option value="size">size</option><option value="chain">chain length</option></select><input data-f="val" placeholder="the value to remember"><button class="btn gold sm" data-a="map">Remember it</button></div>
        ${lids.length > 1 ? `<label class="scopeOne"><input type="checkbox" data-f="one"> only for listing ${esc(p.listingId)} — otherwise all ${lids.length} are mapped together</label>` : ""}`;
      const oneOnly = () => { const b = c.querySelector("[data-f=one]"); return !!(b && b.checked); };
      const put = async (field, value) => {
        const who = by(); if (!who) return;
        const wide = lids.length > 1 && !oneOnly();
        [...c.querySelectorAll("button")].forEach(b => { b.disabled = true; });
        try {
          await api("charmNestLibrary", { op: "optionMapPut", listingId: wide ? "*" : p.listingId, optionName: p.optionName, optionValue: p.optionValue, map: { field, value: field === "size" ? String(value).toUpperCase() : String(value).toLowerCase() }, by: who });
          await Orders.loadMaps(true);
          toast(`“${p.optionValue}” → ${value} · remembered for ${wide ? "every listing" : "this listing"}`, "ok");
          for (const rr of Orders.rows()) if (rr.problems.some(x => x.kind === "needsMapping")) await repool(rr);
        } catch (e) { toast("Could not save that: " + e.message, "bad", 7000); [...c.querySelectorAll("button")].forEach(b => { b.disabled = false; }); }
      };
      c.querySelectorAll("[data-pick]").forEach(b => { b.onclick = () => put(b.dataset.field, b.dataset.pick); });
      c.querySelector("[data-a=other]").onclick = () => { c.querySelector(".fixes.other").classList.toggle("hidden"); const f = c.querySelector("[data-f=val]"); if (f) f.focus(); };
      bindNeeds(c, "map", "val");
      c.querySelector("[data-a=map]").onclick = () => { const val = c.querySelector("[data-f=val]").value.trim(); if (!val) return; put(c.querySelector("[data-f=field]").value, val); };
      c.querySelector("[data-a=ignore]").onclick = async () => { const who = by(); if (!who) return; for (const lid of (oneOnly() ? [String(p.listingId)] : lids)) await api("charmNestLibrary", { op: "optionMapPut", listingId: lid, optionName: p.optionName, optionValue: p.optionValue, map: { field: "ignore" }, by: who }); await Orders.loadMaps(true); await repoolAll(it); };
    } else if (it.kind === "unmatchedSku" || it.kind === "blockedSku") {
      const skus = [...B.master.entries.keys()].sort();
      c.innerHTML = head(it.kind === "blockedSku" ? "SKU blocked" : "Unmatched SKU", p.sku || "no SKU", orderSub) + `<div class="why">${esc(p.reason || it.why)}</div>
        <div class="fixes"><input list="rvSkus" data-f="sku" placeholder="pick the charm from the master index…"><datalist id="rvSkus">${skus.map(s => `<option value="${esc(s)}">`).join("")}</datalist><button class="btn gold sm" data-a="alias" title="every line of this listing uses that charm from now on">Use this charm</button><button class="btn ghost sm" data-a="nodesign" title="this line never needs a design — remembered, so it stops asking">Nothing to cut</button><button class="btn ghost sm" data-a="hold" title="hold the whole order until someone sorts it out">Hold order</button>${it.kind === "blockedSku" ? `<button class="btn ghost sm" data-a="master">Open Master</button>` : ""}</div>`;
      bindNeeds(c, "alias", "sku");
      c.querySelector("[data-a=alias]").onclick = async () => { const sku = c.querySelector("[data-f=sku]").value.trim().toUpperCase(); if (!sku) return; const who = by(); if (!who) return; if (!B.master.entries.has(sku)) { toast(`${sku} is not in the master index`, "bad"); return; } const lids = [...new Set(group.map(x => String(x.line.listingId)))]; for (const lid of lids) await api("charmNestLibrary", { op: "aliasPut", listingId: lid, sku, by: who, title: r.line.title }); await Orders.loadMaps(true); toast(`${lids.length} listing${lids.length === 1 ? "" : "s"} → ${sku} remembered`, "ok"); for (const rr of Orders.rows()) if (lids.includes(String(rr.line.listingId))) await repool(rr); };
      c.querySelector("[data-a=nodesign]").onclick = async () => { const who = by(); if (!who) return; const sku = p.sku || (sp && sp.designSku); if (sku) await api("charmNestLibrary", { op: "noDesignPut", sku, by: who, note: r.line.title }); else await api("charmNestLibrary", { op: "noDesignPut", pattern: "^" + String(r.line.title).replace(/[.*+?^${}()|[\]\\]/g, "\\$&").slice(0, 40), by: who, note: "by title" }); await Orders.loadMaps(true); await repoolAll(it); };
      const mb = c.querySelector("[data-a=master]");
      if (mb) mb.onclick = () => {
        const sku = p.sku || (sp && sp.designSku) || "";
        modeFromUser = true;                                            // pushState, so Back returns to this card
        setMode("master");
        const f = document.getElementById("mSearch"); if (!f) return;
        f.value = sku; Master.render(); f.focus(); f.select();
        let tries = 0;
        const show = () => {
          const t = document.querySelector("#mGrid .skuTile");
          if (!t) { if (++tries < 40) setTimeout(show, 150); return; }   // a cold tab is still loading the library
          t.scrollIntoView({ behavior: "smooth", block: "center" });
          t.classList.add("pulse"); setTimeout(() => t.classList.remove("pulse"), 1300);
        };
        show();
      };
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
    const skipB = c.querySelector("[data-a=skip]"); if (skipB) skipB.onclick = () => { const who = by(); if (!who) return; for (const rr of rowsOf(it)) { rr.state = "skipped"; rr.reason = `line skipped by ${who}`; rr.problems = []; rr.hold = `line skipped by ${who}`; } syncOrderItems(); Orders.render(); RunCtl.poke(); };
    const holdB = c.querySelector("[data-a=hold]"); if (holdB) holdB.onclick = () => { const who = by(); if (!who) return; const g = rowsOf(it); for (const rr of g) { rr.hold = `held by ${who}`; rr.reason = rr.hold; rr.state = "held"; } remove(it.key); Orders.render(); RunCtl.poke(); const ords = [...new Set(g.map(x => x.order.receiptId))]; toast(`${ords.length === 1 ? ords[0] : ords.length + " orders"} held by ${who} — release them from the Orders tab`, ""); };
    return c;
  }
  async function row_material(it, m, who) {
    for (const r of rowsOf(it)) {
      r.materialOverride = m;
      try { await DesignLink.call("notes.set", { receiptId: r.order.receiptId, text: `${r.spec.staffNote ? r.spec.staffNote + "\n" : ""}Material: ${labelOf(m)} (${who}, sorter)` }); } catch (e) { toast("Staff note not written: " + e.message, "bad"); }
    }
    await repoolAll(it);
  }
  const KIND_WORDS = { needsMaterial: "Material", needsMapping: "Options", unmatchedSku: "Unknown SKU", blockedSku: "Blocked SKU", missingSize: "Size", oversize: "Too big", fontMissing: "Font", engraveWords: "Words", notRepresentable: "Characters", flipFailed: "Flip", placement: "Placement", orderChanged: "Changed", heldOrder: "Held" };
  const RV = { filter: null };
  function render() {
    const v = document.getElementById("reviewView"); LiveStrip.render(); if (!v || v.classList.contains("hidden")) return;
    const all = items().filter(it => mine(it) && !isNotice(it));
    const ORDER = ["needsMaterial", "needsMapping", "unmatchedSku", "blockedSku", "missingSize", "oversize", "fontMissing", "engraveWords", "notRepresentable", "flipFailed", "placement", "orderChanged", "heldOrder"];
    all.sort((a, b) => ORDER.indexOf(a.kind) - ORDER.indexOf(b.kind) || a.t - b.t);
    // the kinds present are the filter: one chip each, so a long mixed list becomes the one kind being worked through
    const byKind = new Map(); for (const it of all) byKind.set(it.kind, (byKind.get(it.kind) || 0) + 1);
    if (RV.filter && RV.filter !== "done" && !byKind.has(RV.filter)) RV.filter = null;
    const list = RV.filter && RV.filter !== "done" ? all.filter(it => it.kind === RV.filter) : RV.filter === "done" ? [] : all;
    const chip = (id, label, n, cls) => `<button class="egTab${(RV.filter || "") === id ? " on" : ""}" data-k="${esc(id)}" title="${esc(label)}">${esc(label)}${n ? `<b class="${cls || "warn"}">${n}</b>` : ""}</button>`;
    v.innerHTML = `<div class="ordBar egBar">${chip("", "Everything", all.length, "info")}${ORDER.filter(k => byKind.has(k)).map(k => chip(k, KIND_WORDS[k] || k, byKind.get(k))).join("")}${settled.length ? chip("done", "Decided", settled.length, "ok") : ""}<span class="spacer"></span><button class="btn ghost xs" id="rvName" title="every decision is recorded under this name — click to change it">${esc(employeeName() || "set your name")}</button></div>
      <div class="egPane grow scroll"><div class="rvList" id="rvList"></div></div>`;
    v.querySelector("#rvName").onclick = () => { askEmployee(); render(); };
    v.querySelectorAll("[data-k]").forEach(b => b.onclick = () => { RV.filter = b.dataset.k || null; render(); });
    const host = v.querySelector("#rvList");
    if (RV.filter === "done") {
      // what this shift settled: the other half of "what has been approved", which the screen never used to say
      host.innerHTML = settled.length ? settled.map(d => `<div class="doneRow hoverItem" data-rid="${esc((d.orders || [])[0] || "")}"><b class="mono">${esc((d.orders || []).slice(0, 2).join(" "))}${(d.orders || []).length > 2 ? ` +${d.orders.length - 2}` : ""}</b><span class="sku mono">${esc(KIND_WORDS[d.kind] || d.kind)}</span><span class="w">${esc(d.why)}</span><span class="ost ok">settled</span><span class="by">${esc(d.by)}${d.t ? " · " + fmtT(d.t) : ""}</span><span class="mono" style="font-size:11px;color:var(--ink45)">${d.lines} line${d.lines === 1 ? "" : "s"}</span></div>`).join("") : `<div class="libEmpty">nothing settled yet this session</div>`;
      return;
    }
    if (!list.length) host.innerHTML = `<div class="libEmpty">Nothing waits for a decision.</div>`;
    else for (const it of list) host.appendChild(card(it));
    const notices = items().filter(isNotice);
    if (notices.length) {
      host.insertAdjacentHTML("beforeend", `<div class="rvNotices"><div class="nHead">Left open on the station — no decision needed here</div>${notices.map(n => `<div class="nRow"><b class="mono">${esc(n.rid)}</b><span class="w">${esc(n.note || String(n.why || "").replace(n.rid + " held — ", ""))}</span><button class="btn ghost xs" data-open="${esc(n.line || "")}" title="open this order on the cards">Open order ↗</button></div>`).join("")}</div>`);
      host.querySelectorAll("[data-open]").forEach(b => b.onclick = () => { if (b.dataset.open) OrderWin.open(b.dataset.open); });
    }
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
    /* The sandbox's controls hang off the SANDBOX pill in the top bar — a click opens them — instead of taking a
       third of the Orders toolbar on every visit. Off is the normal state and says nothing: Settings holds the way in. */
    let bar = document.getElementById("sandboxBar");
    if (!bar) { const pill = document.getElementById("sandboxPill"); bar = document.createElement("div"); bar.id = "sandboxBar"; bar.className = "sandboxBar sbPop hidden"; (pill ? pill.parentElement : v).appendChild(bar); if (pill) { pill.style.cursor = "pointer"; pill.onclick = () => bar.classList.toggle("open"); } document.addEventListener("pointerdown", e => { if (!e.target.closest("#sandboxBar, #sandboxPill")) bar.classList.remove("open"); }); }
    bar.classList.toggle("hidden", !on());
    bar.innerHTML = on()
      ? `<b>SANDBOX</b><span id="sbStatus" title="${esc(status ? statusText() : "nothing here touches the real Etsy or the real records")}">rehearsal — nothing real is touched</span><button class="btn ghost xs" id="sbReset" type="button" title="empty the sandbox pool, sets, runs and sheets — the real records are untouched">Reset</button><button class="btn ghost xs" id="sbToggle" type="button" title="go back to the real Etsy and the real records">Switch off</button>`
      : "";                                            // off is the normal state and says nothing: Settings holds the way in
    const sn = bar.querySelector("#sbSnap"); if (sn) sn.onclick = () => snapshot().catch(e => toast(e.message, "bad", 8000));
    const rs2 = bar.querySelector("#sbReset"); if (rs2) rs2.onclick = () => reset().catch(e => toast(e.message, "bad", 8000));
    if (!bar.querySelector("#sbToggle")) { if (!status) refresh(); return; }
    bar.querySelector("#sbToggle").onclick = () => { if (on()) { S.settings.sandbox = "off"; saveSettings(); toast("Sandbox off — reloading", "ok", 3000); setTimeout(() => location.reload(), 600); return; } const b = bar.querySelector("#sbToggle"); b.disabled = true; b.textContent = "Switching on…"; enable().catch(e => { toast(`Sandbox: ${e.message}`, "bad", 9000); agent({ bridge: true }, "warn", `Sandbox switch-on stopped: ${e.message}`); b.disabled = false; b.textContent = "Rehearse in the sandbox"; }); };
    if (!status) refresh();
  }
  function statusText() { if (!status || status.error) return status && status.error ? `status: ${status.error}` : ""; const sn = status.snapshot; const rec = status.records || {}; return `${sn ? `snapshot of ${sn.count} order(s) taken ${new Date(sn.at).toLocaleString()}${sn.takenBy ? " by " + sn.takenBy : ""}` : "no snapshot yet"} · sandbox records: ${rec.Charm_Pool || 0} pool, ${rec.Charm_Nest_Sets || 0} sets, ${rec.Charm_Nest_Runs || 0} runs, ${rec.Charm_Nest_Sheets || 0} sheets`; }
  function render() { const el = document.getElementById("sbStatus"); if (el) el.title = statusText() || el.title; const pill = document.getElementById("sandboxPill"); if (pill) pill.classList.toggle("hidden", !on()); document.documentElement.classList.toggle("sandbox", on()); }
  return { on, refresh, snapshot, enable, afterReload, reset, mountPanel, render, status: () => status };
})();


/* ═══ 24b · OrderWin — one line, everything about it, and the way to settle it ═══
   The Design Station's own order window, here: the picture, the SKU, what the customer typed, the staff note that saves
   itself, the internal thread every station shares, and — the reason it is worth having here — the review decision the
   line is waiting on, answered without leaving the order. Messages go over the bridge, so the station keeps the one Etsy
   session and the one Firestore listener and this page never grows a second of either. */
const OrderWin = window.OrderWin = (() => {
  const W = { key: null, rid: null, at: 0, dlg: null, thread: [], tray: [], poll: 0, noteTimer: 0, wired: false };
  const byId = id => document.getElementById(id);
  const rowOf = key => Orders.rows().find(r => r.key === key) || null;
  const me = () => employeeName() || "";

  function wire() {
    if (W.wired) return; W.wired = true;
    W.dlg = byId("orderWin"); if (!W.dlg) return;
    const close = () => W.dlg.close();
    byId("owClose").onclick = close;
    W.dlg.addEventListener("close", () => { clearInterval(W.poll); W.poll = 0; W.key = null; W.tray.forEach(t => { try { URL.revokeObjectURL(t.url); } catch (_) {} }); W.tray = []; });
    byId("owPhoto").onclick = e => e.currentTarget.classList.toggle("zoom");
    byId("owCopy").onclick = async () => { const r = rowOf(W.key); const sku = r && (r.spec.designSku || r.line.sku); if (!sku) return; try { await navigator.clipboard.writeText(sku); toast("SKU copied", "ok", 1800); } catch (_) {} };
    byId("owWhoBtn").onclick = () => { askEmployee(); paintWho(); };
    const note = byId("owNote");
    note.oninput = () => { clearTimeout(W.noteTimer); W.noteTimer = setTimeout(saveNote, 700); };
    note.onblur = () => { clearTimeout(W.noteTimer); saveNote(); };
    const input = byId("owInput");
    const grow = () => { input.style.height = "auto"; input.style.height = Math.min(120, input.scrollHeight) + "px"; byId("owSend").disabled = !input.value.trim() && !W.tray.length; };
    input.oninput = grow;
    input.onkeydown = e => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } };
    input.addEventListener("paste", e => { const f = [...(e.clipboardData || {}).items || []].filter(i => i.type.startsWith("image/")).map(i => i.getAsFile()).filter(Boolean); if (f.length) { e.preventDefault(); addFiles(f); } });
    byId("owSend").onclick = send;
    byId("owAttach").onclick = () => byId("owFile").click();
    byId("owFile").onchange = e => { addFiles([...e.target.files]); e.target.value = ""; };
    const pane = W.dlg.querySelector(".owChat");
    pane.addEventListener("dragover", e => { e.preventDefault(); });
    pane.addEventListener("drop", e => { e.preventDefault(); addFiles([...(e.dataTransfer.files || [])].filter(f => f.type.startsWith("image/"))); });
    byId("owSkip").onclick = toggleSkip;
    byId("owFind").onclick = async () => {
      const r = rowOf(W.key); if (!r) return;
      try { await DesignLink.call("ui.scrollTo", { receiptId: r.order.receiptId }); toast("Shown on the Design Station", "ok", 2200); }
      catch (e) { toast(e.message, "bad", 5000); }
    };
    byId("owPrev").onclick = () => step(-1);
    byId("owNext").onclick = () => step(1);
  }
  /** The lines the Orders tab is showing, so Previous and Next walk what the person is actually looking at. */
  const siblings = () => Orders.visibleRows();
  function step(d) {
    const list = siblings(); const i = list.findIndex(r => r.key === W.key);
    // a line that has just left the list (skipped, or filtered out by the fix that was applied) resumes from where it was
    const from = i < 0 ? Math.min(W.at || 0, list.length - 1) : i;
    const at = from + d;
    if (at < 0 || at >= list.length) return;
    const next = list[at];
    if (next && next.key !== W.key) open(next.key);
  }
  function paintWho() {
    const w = byId("owWho"); if (w) w.textContent = me() || "— no name set —";
    // the thread and the staff note both travel through the Design Station: when that link is down, say so here rather
    // than letting a person type a message and meet an error
    const st = byId("owLink"); if (!st) return;
    const up = DesignLink.inControl() && DesignLink.up();
    const etsy = DesignLink.state() && DesignLink.state().etsy;
    const bad = !up ? "the Design Station link is down — messages and notes cannot be saved"
      : etsy && etsy.signedIn === false ? "the Design Station is not signed in to Etsy" : "";
    st.textContent = bad ? "● offline" : "● live";
    st.className = "owLink " + (bad ? "bad" : "ok");
    st.title = bad || "messages and notes are saving through the Design Station";
  }

  async function saveNote() {
    const r = rowOf(W.key); if (!r) return;
    const text = byId("owNote").value;
    if (text === (r.spec.staffNote || "")) return;
    r.spec.staffNote = text;
    try { await DesignLink.call("notes.set", { receiptId: r.order.receiptId, text }, { quiet: true }); toast("Staff note saved", "ok", 1400); }
    catch (e) { toast("Staff note not saved: " + e.message, "bad", 5000); }
  }
  function addFiles(files) {
    for (const f of files.slice(0, 6)) W.tray.push({ file: f, url: URL.createObjectURL(f) });
    paintTray(); byId("owSend").disabled = !byId("owInput").value.trim() && !W.tray.length;
  }
  function paintTray() {
    const t = byId("owTray"); if (!t) return;
    t.innerHTML = "";
    W.tray.forEach((x, i) => {
      const c = el("span", "chip", '<img crossorigin="anonymous" alt="" src="' + x.url + '"><span>' + esc(x.file.name.slice(0, 18)) + '</span><button type="button" title="remove">×</button>');
      c.querySelector("button").onclick = () => { try { URL.revokeObjectURL(x.url); } catch (_) {} W.tray.splice(i, 1); paintTray(); };
      t.appendChild(c);
    });
  }
  async function send() {
    const r = rowOf(W.key); if (!r) return;
    const who = me() || askEmployee(); if (!who) return;
    const text = byId("owInput").value.trim(), files = W.tray.slice();
    if (!text && !files.length) return;
    byId("owInput").value = ""; byId("owInput").style.height = "auto"; W.tray = []; paintTray(); byId("owSend").disabled = true;
    const rid = r.order.receiptId;
    try {
      for (const f of files) {
        const bytes = new Uint8Array(await f.file.arrayBuffer());
        const up = await uploadBytes("chatImages/" + rid + "/" + Date.now() + "_" + f.file.name.replace(/[^\w.\-]+/g, "_"), bytes, f.file.type || "image/png", "Sending image");
        await DesignLink.call("chat.post", { receiptId: rid, text: "Image attachment", sender: who, imageUrl: up.url });
        try { URL.revokeObjectURL(f.url); } catch (_) {}
      }
      if (text) await DesignLink.call("chat.post", { receiptId: rid, text, sender: who });
      await loadThread(rid, true);
    } catch (e) { toast("Message not sent: " + e.message, "bad", 6000); }
  }
  async function loadThread(rid, force) {
    if (!rid || (!force && W.rid === rid && W.thread.length)) return;
    W.rid = rid;
    try { const res = await DesignLink.call("chat.list", { receiptId: rid, limit: 80 }, { quiet: true }); W.thread = res.messages || []; }
    catch (_) { const r = rowOf(W.key); W.thread = (r && r.spec.messages) || []; }
    if (W.rid === rid) paintThread();
  }
  function paintThread() {
    const t = byId("owThread"); if (!t) return;
    if (!W.thread.length) { t.innerHTML = '<div class="owEmpty"><b>No internal messages yet</b>Anything sent here reaches every station working this order. The customer never sees it.</div>'; return; }
    const mine = me().toLowerCase();
    t.innerHTML = W.thread.map(m => {
      const own = String(m.senderName || "").toLowerCase() === mine && mine;
      const when = m.at ? new Date(m.at).toLocaleString("en-US", { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" }) : "";
      return '<div class="owMsg' + (own ? " me" : "") + '"><span class="who">' + esc(m.senderName || "Staff") + (when ? " · " + esc(when) : "") + '</span>' +
        (m.text && m.text !== "Image attachment" ? esc(m.text).replace(/\n/g, "<br>") : "") +
        (m.imageUrl ? '<img crossorigin="anonymous" loading="lazy" alt="" src="' + esc(m.imageUrl) + '">' : "") + '</div>';
    }).join("");
    t.scrollTop = t.scrollHeight;
  }
  function toggleSkip() {
    const r = rowOf(W.key); if (!r) return;
    const who = me() || askEmployee(); if (!who) return;
    const on = r.state !== "skipped";
    if (on) { r.state = "skipped"; r.reason = "line skipped by " + who; r.problems = []; r.hold = r.reason; }
    else { r.state = "pulled"; r.reason = null; r.hold = null; Orders.interpretAll(); }
    Review.syncOrderItems(); Orders.render(); RunCtl.poke(); paint();
  }
  /** Paint the window from the row it is showing. */
  function paint() {
    const r = rowOf(W.key); if (!r) { if (W.dlg && W.dlg.open) W.dlg.close(); return; }
    const sp = r.spec || {};
    const sibs = (Orders.rows() || []).filter(x => x.order.receiptId === r.order.receiptId && x.state !== "gone");
    const li = sibs.findIndex(x => x.key === r.key);
    const list = siblings(); W.at = Math.max(0, list.findIndex(x => x.key === r.key));
    byId("owTitle").textContent = "Order " + r.order.receiptId + (sibs.length > 1 ? "  \u00b7  line " + (li + 1) + " of " + sibs.length : "");
    const pos = byId("owPos"); if (pos) pos.textContent = list.length ? (W.at + 1) + " of " + list.length : "";
    const pv = byId("owPrev"), nx = byId("owNext");
    if (pv) pv.disabled = W.at <= 0;
    if (nx) nx.disabled = W.at >= list.length - 1;
    const mp = byId("owMetal"); mp.textContent = r.material ? labelOf(r.material) : (sp.materialLabel || "no material");
    mp.className = "pill " + (r.material ? "neutral" : "bad");
    const ph = byId("owPhoto"); const url = Orders.imageFor(r);
    ph.classList.remove("zoom");
    ph.innerHTML = url ? '<img crossorigin="anonymous" alt="" src="' + esc(url) + '">' : '<span class="ph">no image</span>';
    ph.dataset.lid = String(r.line.listingId || ""); if (url) ph.dataset.painted = "1"; else { delete ph.dataset.painted; Orders.wantImage(r.line.listingId); }
    byId("owSku").textContent = "SKU: " + (sp.designSku || r.line.sku || "—");
    // the one field that must be read exactly: labelled, whole, and never boxed into a scroller under the staff note
    const said = [];
    if ((sp.personalization || []).length) said.push(["Personalisation", sp.personalization.join("\n")]);
    if (sp.buyerMessage) said.push(["Buyer message", sp.buyerMessage]);
    if (sp.messages && sp.messages.length) said.push(["Staff messages", sp.messages.map(m => `${m.senderName}: ${m.text}`).join("\n")]);
    const notes = byId("owNotes");
    notes.className = said.length ? "owSaid" : "owSaid none";
    notes.innerHTML = said.length ? said.map(([k, v2]) => `<span class="lbl">${esc(k)}</span>${esc(v2)}`).join("") : "— the customer wrote nothing —";
    const note = byId("owNote"); if (document.activeElement !== note) note.value = sp.staffNote || "";
    const st = Orders.statePill(r), where = Orders.placeOf(r);
    const mcell = (lbl, val) => '<div class="m"><i>' + esc(lbl) + '</i><span>' + esc(val) + '</span></div>';
    byId("owMeta").innerHTML =
      mcell("Quantity", String(sp.quantity || r.line.quantity || 1)) +
      mcell("Metal", r.material ? labelOf(r.material) : (sp.materialLabel || "none")) +
      mcell("State", st[1]) +
      (where ? mcell("Sheet", (where.set ? where.set + " · " : "") + (where.sheet || "")) : "") +
      mcell("Ship by", Orders.shipTxt(r)) +
      (sp.form ? mcell("Form", sp.form) : "") + (sp.size ? mcell("Size", sp.size) : "") + (sp.chain ? mcell("Chain", sp.chain) : "") +
      (r.engrave && r.engrave.needed ? mcell("Engraving", (r.engrave.approved ? "approved" : r.engrave.state || "waiting") + (r.engrave.text ? " · " + r.engrave.text : "")) : "") +
      (sp.options || []).filter(o => o.mapped).map(o => mcell(o.name, o.value)).join("") +
      mcell("Listing", String(r.line.listingId || "—")) +
      mcell("Title", r.line.title || "—");
    // the decision this line is waiting on, answered here
    const fix = byId("owFix"); fix.innerHTML = "";
    const item = Review.items().find(x => (x.rows || [x.row]).some(y => y && y.key === r.key) && !String(x.key).startsWith("eng:"));
    if (item) {
      const box = el("div", "owFix", '<div class="t">This line is waiting on a decision</div>');
      box.appendChild(Review.card(item));
      fix.appendChild(box);
    } else if (r.engrave && r.engrave.needed && !r.engrave.approved) {
      const box = el("div", "owFix", '<div class="t">Its engraving is still to be settled</div>');
      const b = el("button", "btn ghost sm", "Open it in Engraving");
      b.onclick = () => { W.dlg.close(); setMode("engrave"); Engrave.render(); };
      box.appendChild(b); fix.appendChild(box);
    }
    const sw = byId("owSkip"); sw.setAttribute("aria-checked", r.state === "skipped" ? "true" : "false");
    paintWho();
  }
  function open(key) {
    wire(); if (!W.dlg) return;
    const r = rowOf(key); if (!r) { toast("That line is no longer in the pull", "bad"); return; }
    W.key = key; W.thread = []; W.rid = null;
    paint();
    if (!W.dlg.open) W.dlg.showModal();
    paintThread();
    loadThread(r.order.receiptId, true);
    clearInterval(W.poll);
    W.poll = setInterval(() => { if (W.dlg.open && W.key) loadThread(rowOf(W.key) ? rowOf(W.key).order.receiptId : null, true); }, 15000);
  }
  return { open, paint, close: () => W.dlg && W.dlg.close(), isOpen: () => !!(W.dlg && W.dlg.open), key: () => W.key };
})();

/* ═══ 24c · RunHistory — every run that ever ran, and the way back into one ═══════════════════════════════════════════
   Until now the only run a person could reach was the one in front of them. Yesterday's set, the order that went out on
   Tuesday, the sheet a charm was cut on — none of it had a door. This is the door: one list of runs, one search box over
   all of them, and two ways in — carry on with a run that never finished, or load a finished one back onto the cards to
   look at, download and print. It is reachable from the run banner, from Orders and from Engraving, because the question
   "which run was that?" is asked from wherever you happen to be standing. */
const RunHistory = window.RunHistory = (() => {
  const H = { q: "", when: "all", view: localStorage.getItem("cn.histView") || "cards", runs: [], sheets: [], sets: [], scanned: null, loading: false, err: null, dlg: null, open: new Set(), lines: new Map() };
  /* "Select previous run sets or days" is a filing question, so the dialog files them: four ways to narrow by time and
     state, and a heading for every day, because a flat list of eighty runs is a wall whatever order it is in. */
  const WHEN = [["all", "All"], ["today", "Today"], ["week", "Last 7 days"], ["open", "Unfinished"]];
  const DAY_MS = 86400000;
  function inWhen(r) {
    if (H.when === "open") return !["complete", "abandoned"].includes(r.status);
    if (H.when === "all" || !r.day) return H.when === "all";
    const age = (Date.now() - new Date(r.day + "T12:00:00").getTime()) / DAY_MS;
    return H.when === "today" ? age < 1 : age < 7;
  }
  function ensure() {
    if (H.dlg) return H.dlg;
    const d = el("dialog", "hist"); d.id = "histDlg";
    d.innerHTML = `<form method="dialog" class="x"><button class="btn ghost sm" value="cancel">Close</button></form>
      <h2>Sets</h2>
      <div class="hq"><input id="hQ" type="search" placeholder="order number, SKU, engraved words, a date, a set…" autocomplete="off">
        <button class="btn ghost sm" id="hRefresh" title="read the records again">Refresh</button></div>
      <div class="hWhen" id="hWhen"></div>
      <div class="hBody" id="hBody"></div>
      <div class="hFoot" id="hFoot"></div>`;
    document.body.appendChild(d); H.dlg = d;
    const q = d.querySelector("#hQ");
    let t = 0;
    q.oninput = () => { H.q = q.value; clearTimeout(t); t = setTimeout(load, 260); };
    d.querySelector("#hWhen").onclick = e => { const b = e.target.closest("[data-when]"); if (b) { H.when = b.dataset.when; render(); return; } const v = e.target.closest("[data-view]"); if (v) { H.view = v.dataset.view; try { localStorage.setItem("cn.histView", H.view); } catch (_) {} render(); } };
    d.querySelector("#hRefresh").onclick = e => { e.preventDefault(); load(); };
    return d;
  }
  function show(q) {
    const d = ensure();
    if (q != null) { H.q = q; d.querySelector("#hQ").value = q; }
    if (!d.open) d.showModal();
    d.querySelector("#hQ").focus();
    load();
  }
  async function load() {
    if (!S.cloud.ok) { H.err = "the cloud is not connected, so there is nothing to read"; H.runs = []; H.sheets = []; render(); return; }
    H.loading = true; H.err = null; render();
    try {
      const r = await api("charmNestLibrary", { op: "history", q: H.q, limit: 60 }, { quiet: true });
      H.runs = r.runs || []; H.sheets = r.sheets || []; H.scanned = r.scanned || null; H.truncated = r.truncated || null;
      /* A set is what a person recalls, so the sheets are folded into their sets here: name, day, materials, how many
         sheets and orders, and the picture of its GF sheet (the first sheet, failing that) to know it by. */
      const bySet = new Map();
      for (const x of H.sheets) { const k = x.setId || `sheet:${x.id}`; if (!bySet.has(k)) bySet.set(k, { setId: x.setId || null, seq: x.setSeq || null, day: x.day, runId: x.runId || null, sheets: [], materials: [], orders: 0, updatedAt: 0 }); const g = bySet.get(k); g.sheets.push(x); if (!g.materials.includes(x.metal)) g.materials.push(x.metal); g.orders += x.orders || 0; g.updatedAt = Math.max(g.updatedAt, x.updatedAt || 0); }
      H.sets = [...bySet.values()].map(g => { g.sheets.sort((a, b) => (a.sheetIndex || 0) - (b.sheetIndex || 0)); const gf = g.sheets.find(x => x.metal === "gold" && x.preview) || g.sheets.find(x => x.preview) || null; g.thumb = gf ? gf.preview + (gf.preview.includes("?") ? "&" : "?") + "v=" + (gf.updatedAt || 0) : null; g.thumbOf = gf ? gf.fileBase : null; g.status = g.sheets.every(x => x.status === "complete") ? "complete" : "partial"; return g; }).sort((a, b) => String(b.day).localeCompare(String(a.day)) || (b.seq || 0) - (a.seq || 0));
    } catch (e) { H.err = e.message; H.runs = []; H.sheets = []; }
    H.loading = false; render();
  }
  const dayWord = d => d ? new Date(d + "T12:00:00").toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" }) : "";
  const STATUS = { complete: ["ok", "finished"], running: ["warn", "running"], review: ["warn", "waiting for a person"], paused: ["warn", "paused"], stopped: ["bad", "stopped"], abandoned: ["neutral", "given up"] };
  function render() {
    const b = H.dlg && H.dlg.querySelector("#hBody"); if (!b) return;
    const f = H.dlg.querySelector("#hFoot");
    if (H.loading && !H.runs.length) { b.innerHTML = `<div class="hEmpty"><span class="spin"></span> reading the records…</div>`; f.textContent = ""; return; }
    if (H.err) { b.innerHTML = `<div class="hEmpty bad">${esc(H.err)}</div>`; f.textContent = ""; return; }
    if (!H.runs.length && !H.sheets.length) {
      b.innerHTML = `<div class="hEmpty">${H.q ? `nothing matches “${esc(H.q)}”` : "no runs on record yet"}</div>`;
      H.dlg.querySelector("#hWhen").innerHTML = "";
      f.textContent = H.scanned ? `looked at the ${H.scanned.runs} most recent runs and ${H.scanned.sheets} most recent sheets` : "";
      return;
    }
    const cur = B.run && B.run.runId;
    const seenRuns = H.runs.filter(inWhen);
    const seenSets = H.sets.filter(inWhen);
    const wsel = H.dlg.querySelector("#hWhen");
    const countIn = (k, arr) => arr.filter(r => { const was = H.when; H.when = k; const yes = inWhen(r); H.when = was; return yes; }).length;
    wsel.innerHTML = WHEN.map(([k, lbl]) => `<button class="egTab${H.when === k ? " on" : ""}" data-when="${k}">${lbl}<b>${countIn(k, H.sets)}</b></button>`).join("")
      + `<span class="sp"></span><span class="viewSeg">${["cards", "list"].map(v => `<button data-view="${v}"${H.view === v ? ' class="on"' : ""} title="${v === "cards" ? "a picture of each set" : "one line per set"}">${v === "cards" ? "Cards" : "List"}</button>`).join("")}</span>`;
    if (!seenSets.length && !seenRuns.length) { b.innerHTML = `<div class="hEmpty">no sets ${H.when === "today" ? "today" : H.when === "week" ? "in the last seven days" : H.when === "open" ? "left unfinished" : "on record"}</div>`; f.textContent = ""; return; }
    const openRuns = seenRuns.filter(r => ["running", "review", "paused", "stopped"].includes(r.status) && r.runId !== cur);
    let lastDay = null, html = "";
    for (const g of seenSets) {
      if (g.day !== lastDay) { html += `<div class="hDay">${esc(dayWord(g.day) || "no date")}</div>`; lastDay = g.day; }
      const name = g.seq ? `Set ${g.seq}` : (g.sheets[0].fileBase || "sheet");
      const mats = g.materials.map(m => labelOf(m)).join(" \u00b7 ");
      const isCur = g.runId && g.runId === cur;
      const openBtn = `<button class="btn gold sm" data-a="open" title="put this set's sheets back on the material cards \u2014 from what was saved, nothing runs">Open</button>`;
      const resumeBtn = g.runId && openRuns.some(r => r.runId === g.runId) ? `<button class="btn ghost sm" data-a="resume" title="pick the unfinished run this set belongs to up where it stopped \u2014 it re-reads every order from Etsy first">Resume the run\u2026</button>` : "";
      html += H.view === "cards"
        ? `<div class="hSet card hoverItem${isCur ? " cur" : ""}" data-set="${esc(g.setId || "")}" data-run="${esc(g.runId || "")}" tabindex="0">
            ${g.thumb ? `<img crossorigin="anonymous" class="hThumb" loading="lazy" alt="" src="${esc(g.thumb)}" title="${esc(g.thumbOf || "")}" onerror="this.replaceWith(Object.assign(document.createElement('div'),{className:'hThumb ph',textContent:'no preview'}))">` : `<div class="hThumb ph">no preview</div>`}
            <div class="hRow"><span class="nm">${esc(name)}</span><span class="pill ${g.status === "complete" ? "ok" : "bad"}">${g.status}</span><span class="ct">${g.sheets.length} sheet${g.sheets.length === 1 ? "" : "s"} \u00b7 ${g.orders} order${g.orders === 1 ? "" : "s"}</span></div>
            <div class="hRow sub"><span class="ct">${esc(mats)}</span><span class="sp"></span>${isCur ? `<span class="pill neutral">on the cards</span>` : openBtn}${resumeBtn}</div>
          </div>`
        : `<div class="hSet row hoverItem${isCur ? " cur" : ""}" data-set="${esc(g.setId || "")}" data-run="${esc(g.runId || "")}"><div class="hRow">
            ${g.thumb ? `<img crossorigin="anonymous" class="hMini" loading="lazy" alt="" src="${esc(g.thumb)}" onerror="this.remove()">` : `<span class="hMini ph"></span>`}
            <span class="nm">${esc(name)}</span><span class="pill ${g.status === "complete" ? "ok" : "bad"}">${g.status}</span>
            <span class="ct">${esc(mats)} \u00b7 ${g.sheets.length} sheet${g.sheets.length === 1 ? "" : "s"} \u00b7 ${g.orders} order${g.orders === 1 ? "" : "s"}</span><span class="sp"></span>${isCur ? `<span class="pill neutral">on the cards</span>` : openBtn}${resumeBtn}</div></div>`;
    }
    // unfinished runs that wrote no sheet yet have nothing to picture, but can still be picked up
    for (const r of openRuns.filter(r => !seenSets.some(g => g.runId === r.runId))) html += `<div class="hSet row" data-run="${esc(r.runId)}"><div class="hRow"><span class="nm">${r.seq ? "Set " + r.seq : "run " + r.runId.slice(-8)}</span><span class="pill warn">${esc(r.status)}${r.stoppedBy ? " \u00b7 " + esc(r.stoppedBy) : ""}</span><span class="ct">${r.lines} line${r.lines === 1 ? "" : "s"} \u00b7 no sheet written yet</span><span class="sp"></span><button class="btn ghost sm" data-a="resume" title="pick it up where it stopped \u2014 it re-reads every order from Etsy first">Resume the run\u2026</button></div></div>`;
    b.innerHTML = `<div class="hSets ${H.view}">${html}</div>`;
    f.textContent = [H.scanned ? `looked at the ${H.scanned.runs} most recent runs and ${H.scanned.sheets} most recent sheets` : "",
      H.truncated && (H.truncated.runs || H.truncated.sheets) ? "there is more history than that — narrow the search to reach further back" : ""].filter(Boolean).join(" · ");
    b.querySelectorAll(".hSet").forEach(node => {
      const setId = node.dataset.set || null, runId = node.dataset.run || null;
      const o = node.querySelector("[data-a=open]"); if (o) o.onclick = e => { e.stopPropagation(); if (H.dlg.open) H.dlg.close(); Recall.open(setId ? { setId, runId } : { runId }).catch(err => toast(err.message, "bad", 7000)); };
      const rs = node.querySelector("[data-a=resume]"); if (rs) rs.onclick = e => {
        e.stopPropagation(); const r2 = H.runs.find(x => x.runId === runId) || {};
        if (!confirm(`Pick run ${r2.seq ? "Set " + r2.seq : String(runId).slice(-8)} up again?\n\nIt re-reads all ${r2.orders || ""} orders from Etsy through the Design Station before it can carry on, which takes a few minutes.\n\nTo look at what it already made, press Open instead \u2014 that reads nothing from Etsy.`)) return;
        H.dlg.close(); RunCtl.resumeRun(runId).catch(err => toast(err.message, "bad", 7000));
      };
      if (o) node.onclick = e => { if (e.target.closest("button")) return; o.click(); };
    });
  }
  return { show, load, runs: () => H.runs };
})();

/* ═══ 24d · Recall — a saved set back on the cards, from what was saved ═══════════════════════════════════════════════
   The material cards are the view. A recalled set is not a different screen: its sheets go back onto the GF card, the
   SS card, the RG card — as pages under the same tabs a live run uses — drawn from the picture each sheet saved when it
   was nested and the numbers in its record. One read of the sheet list; the pictures load as the cards scroll into view.
   Nothing is rebuilt from the master files unless a person presses "Rebuild to edit" on one card, and then only that
   sheet. The set's orders come back onto the Orders tab the same way, from the run record. */
const Recall = window.Recall = (() => {
  const RC = { runId: null, setId: null, live: null };
  const on = () => !!(RC.runId || RC.setId);
  /** Put a run's or a set's sheets onto their material cards. Instant: the slim sheet list is all it reads. */
  /* Recall stays on the tab it was asked from. Asked from Orders, the orders come up on Orders; from Nest, the sheets
     on Nest; from anywhere else, Nest — every tab is filled either way, because what is on the cards is what every
     tab is about, live or recalled. */
  let opening = null;
  async function open(sel) { if (opening) await opening.catch(() => {}); opening = openNow(sel); try { return await opening; } finally { opening = null; } }
  async function openNow(sel) {
    const from = S.mode;
    const q = sel.setId ? { setId: sel.setId } : { runId: sel.runId };
    const ls = await api("charmNestLibrary", Object.assign({ op: "listSheets", limit: 200 }, q), { label: "Reading the set" });
    /* One record per sheet name, the newest: before a re-nested sheet kept its identity, the library could hold two
       GF_Sep.17.26_Set-1_Sheet-1 records, one stale. */
    const byName = new Map();
    for (const x of ls.sheets || []) { const k = x.fileBase || x.id; const had = byName.get(k); if (!had || (x.updatedAt || 0) > (had.updatedAt || 0)) byName.set(k, x); }
    const sheets = [...byName.values()].sort((a, b) => (a.setSeq || 0) - (b.setSeq || 0) || (a.sheetIndex || 0) - (b.sheetIndex || 0));
    if (!sheets.length) { if (!sel.quiet) toast("Nothing is saved under that set", "bad", 5000); return; }
    if (B.run && ["running", "review"].includes(B.run.status)) { toast("A run is working — stop it first, or its sheets would be replaced under it", "bad", 6000); return; }
    RunCtl.clearRunState();
    RC.runId = sel.runId || sheets[0].runId || null; RC.setId = sel.setId || null;
    for (const m of METALS) {
      const prim = S.sheets[m.key];
      // a recall replaces what a previous recall left: never pages stacked on pages
      for (const pg of prim.pages.slice(1)) { (pg.workers || []).forEach(w => w.terminate()); }
      prim.pages = [prim]; prim.active = 0; prim.el = prim.cardEl; prim.recalled = null; prim.charms = prim.charms.filter(c => !c.poolId); prim.status = prim.charms.length ? "ready" : "idle"; prim.placements = []; prim.fileBase = null; prim.setId = null; prim.seq = null; prim.sheetIndex = null;
      const mine = sheets.filter(x => x.metal === m.key); if (!mine.length) { CN.renderCard(prim); continue; }
      mine.forEach((rec, i) => {
        const pg = i === 0 ? prim.pages[0] : CN.addPage(m.key);
        pg.charms = []; pg.placements = []; pg.rejects = []; pg.outputs = null; pg.verification = rec.verification || null; pg.liveInfo = null; pg.dirty = false; pg.problem = null;
        pg.recalled = rec; pg.status = "complete"; pg.sheetId = rec.id; pg.runId = rec.runId || RC.runId; pg.setId = rec.setId; pg.seq = rec.setSeq; pg.setDay = rec.day; pg.sheetIndex = rec.sheetIndex; pg.fileBase = rec.fileBase; pg.group = null;
        pg.backPool = (rec.backs || []).map(bk => ({ poolId: bk.poolId, order: bk.order, sku: bk.sku, text: bk.text, lines: bk.lines || (bk.text ? String(bk.text).split("\n") : []), approvedBy: bk.approvedBy, capMm: bk.capMm, outputs: { png: bk.png ? { url: bk.png } : null, ai: bk.ai ? { url: bk.ai } : null } }));
        pg.cloud = Object.assign({ preview: rec.preview }, rec.outputs || {});
        pg.persistedDone = true; pg.persisted = Promise.resolve(); pg._img = null;
      });
      prim.active = 0; prim.el = prim.cardEl; CN.showPage(m.key, 0);
    }
    CN.refreshAllCards(); CN.renderRail(); CN.updateTopSub();
    // and the run's orders, as the Orders tab's own rows
    if (RC.runId) await ordersOf(RC.runId, sheets).catch(e => agent({ run: RC.runId }, "warn", `orders of the run: ${e.message}`));
    Engrave.fromRecall(); Review.syncOrderItems(); Review.render();
    agent({ run: RC.runId }, "cloud", `Recalled ${sheets.length} sheet(s)${sel.setId ? " of " + (sheets[0].setSeq ? "Set-" + sheets[0].setSeq : sel.setId) : ""} from ${sheets[0].day || "the record"} onto the cards — nothing is running`);
    setMode(["orders", "nest", "engrave", "review"].includes(from) ? from : "nest");
    Engrave.render(); Review.render();
  }
  async function ordersOf(runId, sheets) {
    const r = await api("charmNestLibrary", { op: "runGet", runId }, { quiet: true }); if (!r.run) return;
    await Orders.loadMaps().catch(() => {}); await Master.load().catch(() => {});
    let rows = Object.entries(r.run.lines || {}).map(([k, l]) => Orders.rowFromRecord(k, l));
    // opened as a set, the Orders tab is that set's parcels: the orders the recalled sheets name
    const named = new Set((sheets || []).flatMap(x => (x.orders || []).map(String)));
    if (RC.setId && named.size) { const mine = rows.filter(x => named.has(String(x.order.receiptId))); if (mine.length) rows = mine; }
    B.orders.rows = rows;
    B.orders.byKey = new Map(B.orders.rows.map(x => [x.key, x]));
    B.orders.pulledAt = r.run.updatedAt || r.run.startedAt || null; B.orders.filtered = 0; B.orders.stale = false;
    const first = (sheets || []).find(x => x.setSeq) || {};
    B.orders.recalled = { runId, seq: first.setSeq || r.run.seq || +((/-(\d+)$/.exec(String(r.run.setId || "")) || [])[1] || 0) || null, day: first.day || r.run.day || null };
    Orders.interpretAll(); Orders.render();
  }
  /** Bring one recalled sheet's charms back from the master files so it can be edited and nested again. On demand only. */
  async function rebuild(pg) {
    const rec = pg.recalled; if (!rec) return;
    const bar = window.CNProgress ? CNProgress.start(`Rebuilding ${rec.fileBase || rec.id}`) : null;
    try {
      const d = (await api("charmNestLibrary", { op: "getSheet", id: rec.id })).sheet; if (!d) throw new Error("the sheet record is gone");
      for (const p of d.placements || []) {
        const rc = (d.charms || []).find(c => c.id === p.id); if (!rc || !rc.poolId) continue;
        const pool = B.pool.rows.get(rc.poolId) || ((await api("charmNestLibrary", { op: "poolGet", poolIds: [rc.poolId] })).pools || {})[rc.poolId]; if (!pool) continue; B.pool.rows.set(rc.poolId, pool);
        const entry = Master.entryFor(pool.sku) || await Master.fetchEntry(pool.sku); if (!entry) continue;
        const src = await Pool.masterCharm(entry, pool.size); const base = src.charms[0];
        const charm = Pool.cloneCharm(base, `${src.id}:${rc.poolId}`); charm.name = rc.name; charm.order = pool.orderId; charm.poolId = rc.poolId; charm.metal = d.metal; charm.lineKey = pool.lineKey; charm.orderInfo = { receiptId: pool.orderId, transactionId: pool.transactionId, sku: pool.sku, copy: pool.copy, quantity: pool.quantity, form: pool.form, size: pool.size }; charm.pinned = { cxPt: p.cxPt, cyPt: p.cyPt, angle: p.angle };
        pg.charms.push(charm);
        pg.placements.push({ id: charm.id, angle: p.angle, cxPt: p.cxPt, cyPt: p.cyPt, wPt: p.wPt, hPt: p.hPt, layerName: p.layer, scale: 0.975 });
      }
      pg.recalled = null; pg.status = "complete"; pg.dirty = false;
      CN.computeSaturation(pg); CN.renderCard(pg);
      agent({ metal: pg.metal }, "cloud", `${rec.fileBase || rec.id}: ${pg.charms.length} charm(s) rebuilt from the master files — the sheet can be edited and nested again`);
    } finally { if (bar) bar.end(); }
  }
  return { open, rebuild, on, state: () => RC };
})();


/* ═══ 24e · Kin — the rest of the order, wherever it is ══════════════════════════════════════════════════
   A parcel with four charms in it is four lines, and they can be four cards on Orders, a row in Review, a chip in the
   engraving rail and a tile among the backs — on four different screens. Hovering any one of them lights the others,
   wherever they are, because the question a person is asking is always "what else is in this parcel?".

   One listener on the document does it for the whole application: anything that carries data-rid is kin to anything
   else with the same data-rid, and the ring is drawn only when there is more than one, because a single-piece order
   has no rest to show. Nothing has to register; new screens get it by carrying the attribute. */
const Kin = window.Kin = (() => {
  let cur = null;
  const all = rid => document.querySelectorAll(`[data-rid="${String(rid).replace(/["\\]/g, "")}"]`);
  function mark(rid) {
    if (rid === cur) return;
    if (cur) all(cur).forEach(n => n.classList.remove("kin"));
    cur = null;
    if (!rid) return;
    const kin = all(rid);
    if (kin.length < 2) return;                       // one piece is not a set: there is nothing to point at
    kin.forEach(n => n.classList.add("kin"));
    cur = rid;
  }
  function from(e) { const n = e.target && e.target.closest ? e.target.closest("[data-rid]") : null; mark(n ? n.dataset.rid : null); }
  function mount() {
    document.addEventListener("pointerover", from, true);
    document.addEventListener("focusin", from, true);
    document.addEventListener("pointerleave", () => mark(null), true);
    // a repaint under the cursor drops the classes with the old nodes: the next move puts them back
    document.addEventListener("scroll", () => mark(null), true);
  }
  return { mount, mark, of: () => cur };
})();

/* ═══ 25 · boot ═══════════════════════════════════════════════════════════ */
function bootBridge() {
  // The Design Station guards unload in three places; the sorter guarded it nowhere. A reload mid-run loses every
  // engraving approval not yet written to a back file and every review decision made that shift.
  window.addEventListener("beforeunload", e => {
    const r = B.run; if (!r) return;
    const live = ["running", "review", "paused"].includes(r.status);
    // a stopped run is the likeliest moment for a reload and holds the most unwritten work — but only nag when there is some
    const unwritten = r.status === "stopped" && ([...Engrave.items().values()].filter(j => ["approved", "words", "review"].includes(j.state) && !j.backs).length || Review.count());
    if (!live && !unwritten) return;
    e.preventDefault(); e.returnValue = "";
  });
  RunCtl.renderModeBtn(); RunCtl.renderBanner(); LiveStrip.render(); Sandbox.render(); Sandbox.afterReload(); if (Sandbox.on()) agent({ bridge: true }, "warn", "SANDBOX mode: emulated Etsy from the stored snapshot, every record and file goes to sandbox copies");
  document.getElementById("btnRunMode").onclick = () => { const auto = S.settings.runMode !== "auto"; if (auto && !confirm("Auto mode: the sorter connects to the Design Station, pulls the latest orders by the date rule, nests, fits engraving, saves labels and marks the orders complete — stopping only when a person must decide. Turn Auto on?")) return; RunCtl.setMode(auto ? "auto" : "manual"); };
  Orders.loadMaps().catch(() => {}); Master.load().catch(() => {});
  Engrave.loadFonts().catch(() => {});
  Kin.mount();
  /* The app used to open on an empty Orders tab whatever had happened yesterday, and the only way to anything was to
     pull again. It opens on the last run instead — its orders, its sheets, its engraving, read from the record, with
     one line at the top saying so and a Done that puts it down. An open run is offered for resume as before. */
  if (S.cloud.ok) api("charmNestLibrary", { op: "runList", limit: 10 }).then(r => {
    const runs = r.runs || [];
    const open = runs.filter(x => !["complete", "abandoned"].includes(x.status));
    if (open.length) { B.openRuns = open; agent({ bridge: true }, "DS", `${open.length} open run(s) on record — offered on the run banner`); RunCtl.renderBanner(); }
    const last = runs.find(x => x.lines > 0);
    if (last && !B.run && !B.orders.rows.length && !Recall.on()) Recall.open({ runId: last.runId, quiet: true }).catch(() => {});
  }).catch(() => {});
  // the Design Station frame mounts on first visit to its tab; Auto mode mounts it now
  if (S.settings.runMode === "auto") { setTimeout(() => RunCtl.setMode("auto"), 1500); }
  document.addEventListener("keydown", e => { if (e.altKey && e.key === "r") { e.preventDefault(); setMode("review"); } });
  agent({ bridge: true }, "DS", `Bridge ready · station ${DesignLink.origin()} · ${S.settings.runMode} mode`);
}
(function whenReady() { if (window.CN && S.cloud.ok !== null) bootBridge(); else setTimeout(whenReady, 150); })();
})();
