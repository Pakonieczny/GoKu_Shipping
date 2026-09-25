/*  charm-nest-mail.js — the sorter's line to its customers, through the inbox (etsy-mail-1).
 *  ═══════════════════════════════════════════════════════════════════════
 *  A question to a customer about an order, or about one line of it for engraving, goes out through the inbox's own
 *  Etsy helper, and the customer's answers come back through the inbox's own scrape
 *  (netlify/functions/_etsyMailOrderLink.js). This file is the sorter's half:
 *
 *    link       one endpoint, with this sorter's station key; nothing else here talks to the server
 *    sync       one cheap poll (a single document read when nothing changed), shared by every sorter tab in the browser
 *    store      a summary per question: new replies, what is on its way, what did not go — the badges and alerts read it
 *    pane       the conversation itself: the order window's Customer tab, or a window of its own
 *    line box   the engraving card's short version: the last answer, a question box, the way into the conversation
 *    translate  English and Ukrainian, of any message, and of what is being written
 *
 *  Nothing here can hold up the rest of the page: every request is its own and time-boxed, a failure shows only where
 *  the conversation is shown, and what someone typed or sent survives a reload, a closed window and a lost connection.
 */
"use strict";
(function () {
  const ENDPOINT = location.origin + "/.netlify/functions/etsyMailOrderLink";
  const INBOX = "https://etsy-mail-1.goldenspike.app/";
  const SANDBOX = typeof S !== "undefined" && S.settings && S.settings.sandbox === "on";
  // "test": the questions of "Send a test", which go only to our own Etsy account and are real in the sandbox too
  const TEST = "test";
  const sbOf = rid => SANDBOX && String(rid) !== TEST;
  const LS = { key: "cn.mail.station", who: "cn.mail.operator", drafts: "cn.mail.drafts", out: "cn.mail.outbox", told: "cn.mail.told", tab: "cn.mail.tab", tr: "cn.mail.tr", health: "cn.mail.health", test: "cn.mail.test" };
  const PAIR = "cn.mail.pair";   // sessionStorage: a connect request survives this tab's reload, not the tab

  // ─── small helpers ───────────────────────────────────────────────────────
  const get = (k, d) => { try { const v = localStorage.getItem(k); return v == null ? d : JSON.parse(v); } catch (_) { return d; } };
  const put = (k, v) => { try { if (v == null) localStorage.removeItem(k); else localStorage.setItem(k, JSON.stringify(v)); } catch (_) {} };
  const h = (tag, cls, html) => { const e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
  const E = s => typeof esc === "function" ? esc(s) : String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const say = (msg, kind = "", ms) => { try { toast(msg, kind, ms); } catch (_) {} };
  const decoder = document.createElement("textarea");
  /** Etsy's text arrives with its HTML entities (&#39; for an apostrophe); people should read the characters. */
  const plain = s => { const t = String(s == null ? "" : s); if (!/&(#\d+|#x[0-9a-f]+|[a-z]+);/i.test(t)) return t; decoder.innerHTML = t; return decoder.value; };
  const html = s => E(plain(s)).replace(/\n/g, "<br>");
  const firstName = n => String(n || "").trim().split(/\s+/)[0] || "";
  const uidOf = () => "c" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  function when(ms) {
    if (!ms) return "";
    const d = Date.now() - ms;
    if (d < 45000) return "just now";
    if (d < 3600000) return Math.round(d / 60000) + " min ago";
    const t = new Date(ms), now = new Date();
    const hm = t.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    if (t.toDateString() === now.toDateString()) return hm;
    return t.toLocaleDateString([], { month: "short", day: "numeric" }) + ", " + hm;
  }
  const ICON = {
    mail: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="5" width="18" height="14" rx="2.5"/><path d="M4 7l8 6 8-6"/></svg>',
    send: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4 20-7z"/></svg>',
    out: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M14 4h6v6"/><path d="M20 4l-9 9"/><path d="M19 14v4a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h4"/></svg>'
  };

  // ─── state ───────────────────────────────────────────────────────────────
  const M = {
    key: get(LS.key, null), who: get(LS.who, null), pair: null,
    n: -1, since: 0, needFull: true, fullAt: 0, first: true,
    store: new Map(), link: "off", lastOkAt: 0, fails: 0,
    panes: new Set(), lines: new Map()
  };
  try { M.pair = JSON.parse(sessionStorage.getItem(PAIR) || "null"); } catch (_) { M.pair = null; }

  // ─── the one door to the server ──────────────────────────────────────────
  async function call(op, body = {}, { timeout = 25000, key = M.key } = {}) {
    const ctl = new AbortController(), t = setTimeout(() => ctl.abort(), timeout);
    try {
      const headers = { "Content-Type": "application/json" };
      if (key) headers["X-Mail-Station"] = key;
      const res = await fetch(ENDPOINT, { method: "POST", headers, body: JSON.stringify(Object.assign({ op }, body)), signal: ctl.signal, cache: "no-store" });
      let data = {}; try { data = await res.json(); } catch (_) {}
      if (!res.ok) { const e = new Error(data.error || "The inbox link answered " + res.status); e.status = res.status; e.code = data.code || null; throw e; }
      return data;
    } catch (e) {
      if (e.status != null) throw e;
      const x = new Error(e.name === "AbortError" ? "The inbox link did not answer in time" : "No connection to the inbox link");
      x.status = 0; x.code = e.name === "AbortError" ? "TIMEOUT" : "NETWORK"; throw x;
    } finally { clearTimeout(t); }
  }
  /** A station key the server no longer knows (disconnected, or its inbox account removed) is forgotten here. */
  function authLost(e) {
    if (!(e && e.status === 401 && (e.code === "NOT_CONNECTED" || e.code === "OPERATOR_REVOKED"))) return false;
    forget(e.code === "OPERATOR_REVOKED" ? "The inbox account that connected this sorter is no longer active. Connect it again." : "");
    return true;
  }
  function forget(message) {
    put(LS.key, null); put(LS.who, null);
    M.key = null; M.who = null; M.store.clear(); M.link = "off"; M.n = -1; M.since = 0; M.needFull = true; M.first = true;
    clearTimeout(syncTimer); clearTimeout(healthTimer); clearTimeout(testTimer); closeHealth();
    if (message) say(message, "bad");
    paintAll();
  }

  // ─── connecting this sorter to the inbox, once ──────────────────────────
  /* The sorter asks; a person signed in to the inbox approves in the inbox; only this sorter (it alone holds the pair
     id) collects the key. Nothing needs typing, and nothing secret travels in a link. */
  function browserName() {
    const u = navigator.userAgent;
    const b = /Edg\//.test(u) ? "Edge" : /Chrome\//.test(u) ? "Chrome" : /Firefox\//.test(u) ? "Firefox" : /Safari\//.test(u) ? "Safari" : "a browser";
    const os = /Mac OS X/.test(u) ? "Mac" : /Windows/.test(u) ? "Windows" : /Linux/.test(u) ? "Linux" : "";
    return b + (os ? " on " + os : "");
  }
  let pairTimer = 0;
  async function connect() {
    if (M.pair && M.pair.expiresAtMs > Date.now() + 30000) { openInbox(M.pair.url); return; }
    try {
      const who = window.B && B.employee ? " · " + B.employee : "";
      const r = await call("pair_start", { label: "Charm Sorter, " + browserName() + who }, { key: null });
      M.pair = { pairId: r.pairId, expiresAtMs: r.expiresAtMs, url: r.inboxUrl };
      try { sessionStorage.setItem(PAIR, JSON.stringify(M.pair)); } catch (_) {}
      openInbox(r.inboxUrl);
      pollPair(); paintAll();
    } catch (e) { say("Could not start connecting to the inbox: " + e.message, "bad"); }
  }
  function openInbox(url) { const w = window.open(url, "cn-mail-inbox"); if (!w && M.pair) M.pair.blocked = true; paintAll(); }
  function cancelPair() { M.pair = null; clearTimeout(pairTimer); try { sessionStorage.removeItem(PAIR); } catch (_) {} paintAll(); }
  async function pollPair() {
    clearTimeout(pairTimer);
    const p = M.pair; if (!p) return;
    if (Date.now() > p.expiresAtMs) { cancelPair(); say("The connect request ran out of time. Press Connect again.", "", 6000); return; }
    try {
      const r = await call("pair_claim", { pairId: p.pairId }, { key: null, timeout: 15000 });
      if (M.pair !== p) return;
      if (r.stationKey) { adopt(r.stationKey, r.operator); return; }
      if (r.denied || r.expired) { cancelPair(); say(r.denied ? "The inbox declined the connection." : "The connect request ran out of time. Press Connect again.", r.denied ? "bad" : "", 6000); return; }
    } catch (_) { /* a blip: keep asking until the request expires */ }
    pairTimer = setTimeout(pollPair, document.hidden ? 5000 : 2000);
  }
  function adopt(key, who) {
    put(LS.key, key); put(LS.who, who || null);
    M.key = key; M.who = who || null; M.n = -1; M.since = 0; M.needFull = true; M.first = true;
    cancelPair();
    say("Connected to the inbox" + (who && who.name ? " as " + who.name : ""), "ok", 4000);
    kick(0); flushOut(); paintAll(); healthSoon();
    for (const P of M.panes) load(P);
  }
  async function disconnect() {
    if (!M.key || !confirm("Disconnect this sorter from the inbox? Messages already sent stay in the inbox. You can connect again at any time.")) return;
    try { await call("disconnect"); } catch (_) {}
    forget("");
  }
  // another tab of this sorter connected or disconnected: follow it
  window.addEventListener("storage", e => {
    if (e.key !== LS.key) return;
    const key = get(LS.key, null);
    if (key === M.key) return;
    if (key) { M.key = key; M.who = get(LS.who, null); M.n = -1; M.since = 0; M.needFull = true; M.first = true; cancelPair(); kick(0); for (const P of M.panes) load(P); paintAll(); }
    else forget("");
  });

  // ─── sync: what changed, shared by every tab ────────────────────────────
  const bc = "BroadcastChannel" in window ? new BroadcastChannel("cn-mail") : null;
  const TAB = uidOf();
  let syncTimer = 0, syncing = false, lastTouch = Date.now();
  ["pointerdown", "keydown"].forEach(ev => document.addEventListener(ev, () => { lastTouch = Date.now(); }, { capture: true, passive: true }));
  /* The pace follows what the person is looking at: a conversation on screen or a message on its way is watched every
     few seconds; open questions without a window on them every quarter minute; nothing open, every minute or two. A tab
     in the background slows down, and a poll another tab already made is not made again. */
  function pace() {
    if (!M.key) return 0;
    const shown = [...M.panes].some(P => P.visible()) || [...M.lines.values()].some(L => L.node.isConnected);
    const moving = pendingOut().length || [...M.store.values()].some(s => s.pending > 0);
    const open = [...M.store.values()].some(s => s.status === "open");
    const idle = Date.now() - lastTouch > 10 * 60000;
    let ms = shown || moving ? 4000 : open ? 15000 : 90000;
    if (document.hidden) ms = Math.max(ms * 3, moving ? 15000 : 45000);
    else if (idle) ms = Math.max(ms, 30000);
    return ms;
  }
  function kick(ms) { clearTimeout(syncTimer); if (!M.key) return; syncTimer = setTimeout(syncNow, ms == null ? pace() : ms); }
  async function syncNow() {
    if (syncing || !M.key) return;
    syncing = true;
    try {
      if (Date.now() - M.fullAt > 15 * 60000) M.needFull = true;
      const res = await call("sync", { n: M.n, since: M.since, sandbox: SANDBOX, full: M.needFull }, { timeout: 20000 });
      M.fails = 0; M.lastOkAt = Date.now(); setLink("ok");
      apply(res, true);
      if (bc) try { bc.postMessage({ t: "sync", from: TAB, sandbox: SANDBOX, res }); } catch (_) {}
    } catch (e) {
      if (!authLost(e)) { M.fails++; if (M.fails > 1) setLink("offline"); }
    } finally {
      syncing = false;
      if (M.key) kick(M.fails ? Math.max(pace(), Math.min(60000, 2500 * 2 ** Math.min(M.fails, 5))) : pace());
    }
  }
  if (bc) bc.onmessage = e => {
    const d = e.data || {};
    if (d.t === "sync" && d.sandbox === SANDBOX && d.res && M.key) { M.lastOkAt = Date.now(); M.fails = 0; setLink("ok"); apply(d.res, false); kick(); }
    else if (d.t === "out") flushOut();
    else if (d.t === "health" && d.h && d.h.res && M.key) { M.health = d.h; paintLights(); healthLater(); }
    else if (d.t === "test" && d.x && d.x.v && M.key) { const was = testV(); M.test = d.x; testNews(was, d.x.v); paintTest(); testLater(); }
  };
  document.addEventListener("visibilitychange", () => { if (!document.hidden) { clearTitle(); if (M.key) kick(300); for (const P of M.panes) if (P.visible()) markRead(P); healthSoon(); if (testWaiting()) testCall("test_info"); } });
  window.addEventListener("online", () => { if (M.key) kick(200); flushOut(); });

  function setLink(state) { if (M.link === state) return; M.link = state; for (const P of M.panes) paintState(P); paintLights(); }

  // ─── is the line working: the light where messages are written ─────────
  /* Green "Active" when every part a message passes through is working: this sorter reaches the inbox, sending is on,
     the inbox's Etsy helper checks in, and the inbox notices and reads new Etsy messages (the server reads the evidence;
     see health in _etsyMailOrderLink.js). Anything wrong turns it amber or red, with one plain sentence under the box
     being written in. It is checked about once a minute while a place to write is on screen, once for all tabs. */
  const HEALTH_EVERY = 60000;
  let healthTimer = 0, healthBusy = false, healthErr = "";
  M.health = get(LS.health, null);
  const writingShown = () => !!M.key && ([...M.panes].some(P => P.visible()) || [...M.lines.values()].some(L => L.node.isConnected && !!L.node.offsetParent));
  function healthLater() {
    clearTimeout(healthTimer); healthTimer = 0;
    if (!writingShown()) return;
    const age = M.health ? Date.now() - M.health.at : Infinity;
    healthTimer = setTimeout(() => checkHealth(false), Math.max(1000, (document.hidden ? 3 * HEALTH_EVERY : HEALTH_EVERY) - age));
  }
  /** A place to write came on screen: a check that is due happens now. */
  function healthSoon() {
    if (!M.key) return;
    const shared = get(LS.health, null);
    if (shared && (!M.health || shared.at > M.health.at)) { M.health = shared; paintLights(); }
    if (writingShown() && (!M.health || Date.now() - M.health.at > HEALTH_EVERY - 2000)) checkHealth(false);
    else healthLater();
  }
  async function checkHealth(fresh) {
    if (!M.key || healthBusy) return;
    const shared = get(LS.health, null);
    if (!fresh && shared && Date.now() - shared.at < HEALTH_EVERY - 5000) { M.health = shared; paintLights(); healthLater(); return; }
    healthBusy = true; paintLights();
    try {
      const res = await call("health", { fresh: !!fresh }, { timeout: 15000 });
      M.health = { at: Date.now(), res }; healthErr = "";
      put(LS.health, M.health);
      if (bc) try { bc.postMessage({ t: "health", h: M.health }); } catch (_) {}
    } catch (e) { if (!authLost(e)) healthErr = e.message; }
    finally { healthBusy = false; paintLights(); healthLater(); }
  }
  /** What the light says: tone (ok, warn, down, wait; the same in the sandbox), its word, and the sentence under the box when it is not ok. */
  function lightState() {
    const h = M.health && Date.now() - M.health.at < 5 * 60000 ? M.health.res : null;
    if (M.link === "offline") return { tone: "down", word: "Offline", text: "This sorter cannot reach the inbox's server right now. What you write is kept, and goes as soon as it answers again." };
    if (!h) return { tone: "wait", word: "Checking…", text: "" };
    if (h.level === "ok") return { tone: "ok", word: "Active", text: "" };
    return { tone: h.level === "down" ? "down" : "warn", word: h.short || "Problem", text: h.problem || "" };
  }
  function lightHtml(st) {
    const tip = st.tone === "ok" ? "Connected: sending and receiving are working. Click for details."
      : st.tone === "sb" ? "Sandbox: nothing here reaches a real customer. Click for details."
      : st.tone === "wait" ? "Checking the email link…" : st.text + " Click for details.";
    return `<button type="button" class="cmLight ${st.tone}" data-health title="${E(tip)}" aria-label="Email link: ${E(st.word)}. Details"><i></i>${E(st.word)}</button>`;
  }
  function warnHtml(st) {
    if (st.tone !== "warn" && st.tone !== "down") return "";
    return `<span>${E(st.text)}</span> <button type="button" class="lnk" data-health>Details</button>`;
  }
  let healthBox = null;   // { box, owner } — the one details panel open, if any
  function healthRows() {
    const h = M.health && M.health.res;
    const row = (level, label, text) => `<li class="${E(level)}"><i></i><b>${E(label)}</b><span>${E(text)}</span></li>`;
    const rows = [row(M.link === "offline" ? "down" : "ok", "Sorter to inbox", M.link === "offline" ? "Not answering right now; what you write is kept" : "Connected" + (M.who && M.who.name ? " as " + M.who.name : ""))];
    if (h) for (const c of h.checks) rows.push(row(c.level, c.label, c.text));
    else rows.push(`<li class="wait"><i></i><b>Checking the rest</b><span>${healthErr ? E(healthErr) : "…"}</span></li>`);
    const when0 = healthBusy ? `<span class="cmSpin" aria-hidden="true"></span>checking` : M.health ? "checked " + when(M.health.at) : "";
    return `<div class="cmHBh"><b>Email link</b><span class="cmHBat">${when0}</span><button type="button" class="lnk" data-hcheck${healthBusy ? " disabled" : ""}>Check now</button></div><ul class="cmHBl">${rows.join("")}</ul>`
      + testHtml()
      + (SANDBOX ? `<div class="cmHBf">Sandbox is on: questions about sandbox orders stay in this sorter and never reach anyone. The light still shows the real link.</div>` : "");
  }
  function toggleHealth(box, owner) {
    if (healthBox && healthBox.box === box) { closeHealth(); return; }
    closeHealth();
    healthBox = { box, owner };
    box.hidden = false; box.innerHTML = healthRows();
    owner.querySelectorAll("[data-health]").forEach(b => b.setAttribute("aria-expanded", "true"));
    if (!M.health || Date.now() - M.health.at > 20000) checkHealth(true);
    if (!testV() || testWaiting() || Date.now() - M.test.at > 60000) testCall("test_info"); else testLater();
  }
  function closeHealth() {
    if (!healthBox) return;
    healthBox.box.hidden = true; healthBox.box.innerHTML = "";
    healthBox.owner.querySelectorAll("[data-health]").forEach(b => b.setAttribute("aria-expanded", "false"));
    healthBox = null;
  }
  document.addEventListener("pointerdown", e => { if (healthBox && !healthBox.box.contains(e.target) && !e.target.closest("[data-health]")) closeHealth(); }, true);
  document.addEventListener("keydown", e => { if (e.key === "Escape" && healthBox) { e.preventDefault(); e.stopPropagation(); closeHealth(); } }, true);
  document.addEventListener("click", e => { const b = e.target.closest("[data-hcheck]"); if (b && healthBox && healthBox.box.contains(b)) checkHealth(true); });
  /** One place to write: its light and its sentence, redrawn only when they changed (a redrawn button loses focus). */
  function paintLight(el, owner, cls, st) {
    st = st || lightState();
    const light = M.key ? lightHtml(st) : "", warn = M.key ? warnHtml(st) : "";
    if (el.live.dataset.v !== light) {
      el.live.dataset.v = light; el.live.innerHTML = light;
      if (healthBox && healthBox.owner === owner) el.live.querySelectorAll("[data-health]").forEach(b => b.setAttribute("aria-expanded", "true"));
    }
    if (el.warn.dataset.v !== warn) { el.warn.dataset.v = warn; el.warn.innerHTML = warn; }
    el.live.hidden = !light; el.warn.hidden = !warn; el.warn.className = cls + " " + st.tone;
    if (el.foot) el.foot.hidden = !light;
  }
  function paintLights() {
    const st = lightState();
    for (const P of M.panes) if (P.host.isConnected) paintLight(P.el, P.host, "cmWarn", st);
    for (const L of M.lines.values()) if (L.node.isConnected) paintLight(L.el, L.node, "cmLWarn", st);
    if (healthBox) { if (healthBox.box.isConnected && M.key) healthBox.box.innerHTML = healthRows(); else closeHealth(); }
  }

  // ─── a test without a customer: an Etsy account of our own ──────────────
  /* "Send a test" goes the whole real way (the inbox, its Etsy helper, Etsy, and the answer back through the inbox's
     scrape) but only to the shop's conversation with an Etsy account of our own. The server picks that conversation by
     a code sent from it, never by a guess (testing in _etsyMailOrderLink.js), and a question on "test" can only go
     there. Set up once, in the light's details; shared by every tab; asked about only while a code waits. */
  M.test = get(LS.test, null);   // { at, v: { ready, threadId, customer, pending: { code, expiresAtMs, expired } } }
  let testBusy = false, testErr = "", testTimer = 0;
  const testV = () => (M.test && M.test.v) || null;
  const testWaiting = () => { const v = testV(); return !!(M.key && v && v.pending && !v.pending.expired); };
  const testWho = v => { const c = (v && v.customer) || {}; return c.name && c.username ? `${c.name} (@${c.username})` : c.name || (c.username ? "@" + c.username : "your test account"); };
  const clock = ms => new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  async function testCall(op) {
    if (!M.key || testBusy) return;
    testBusy = true; testErr = ""; paintTest();
    try {
      const v = await call(op, {}, { timeout: 20000 });
      const was = testV();
      M.test = { at: Date.now(), v }; put(LS.test, M.test);
      if (bc) try { bc.postMessage({ t: "test", x: M.test }); } catch (_) {}
      testNews(was, v);
    } catch (e) { if (!authLost(e)) testErr = e.message; }
    finally { testBusy = false; paintTest(); testLater(); }
  }
  /** The code reached the inbox: said once, where someone is looking. */
  function testNews(was, v) {
    if (!document.hidden && v && v.ready && was && was.pending && !was.pending.expired && (!was.ready || was.threadId !== v.threadId))
      say(`Test account set: tests go only to ${testWho(v)}.`, "ok", 9000);
  }
  function testLater() {
    clearTimeout(testTimer); testTimer = 0;
    if (!testWaiting() || (document.hidden && !healthBox)) return;   // a hidden page asks again when it is looked at
    testTimer = setTimeout(() => testCall("test_info"), healthBox ? 5000 : 20000);
  }
  function paintTest() { if (healthBox && healthBox.box.isConnected && M.key) healthBox.box.innerHTML = healthRows(); }
  function testHtml() {
    if (!M.key) return "";
    const v = testV(), spin = `<span class="cmSpin" aria-hidden="true"></span>`;
    let body;
    if (!v) body = testErr ? `<p class="bad">${E(testErr)}</p><div class="cmHTb"><button type="button" class="lnk" data-t="info">Try again</button></div>` : `<p class="soft">${spin} Looking…</p>`;
    else if (v.pending && !v.pending.expired) body =
      `<p>Send this code to the shop as an Etsy message, from your own Etsy account (not the shop's):</p>`
      + `<div class="cmHTc"><code>${E(v.pending.code)}</code><button type="button" class="btn ghost sm" data-t="copy">Copy</button></div>`
      + `<p class="soft">${spin} Waiting for the inbox to read it, usually a minute or two.${v.ready ? ` Until then, tests still go to ${E(testWho(v))}.` : ""}</p>`
      + `<div class="cmHTb"><span class="soft">The code works until ${E(clock(v.pending.expiresAtMs))}.</span><button type="button" class="lnk soft" data-t="cancel">Cancel</button></div>`;
    else if (v.ready) body =
      `<p>Tests go only to <b>${E(testWho(v))}</b> on Etsy, never to a customer.</p>`
      + `<div class="cmHTb"><button type="button" class="btn sm cmBtn" data-t="send">Send a test</button><button type="button" class="btn ghost sm" data-t="open">Open the test conversation</button><button type="button" class="lnk soft" data-t="start">Change</button></div>`
      + (v.pending ? `<p class="soft">The code for another account ran out before it reached the inbox.</p>` : "");
    else body =
      (v.pending ? `<p class="bad">The code ran out before it reached the inbox.</p>` : `<p>Real messages through the inbox, but only to an Etsy account of your own. Set it up once:</p>`)
      + `<div class="cmHTb"><button type="button" class="btn sm cmBtn" data-t="start">${v.pending ? "Get a new code" : "Set up a test account"}</button></div>`;
    return `<div class="cmHT"><div class="cmHTh"><b>Test without a customer</b>${testBusy && v ? spin : ""}</div>${body}${testErr && v ? `<p class="bad">${E(testErr)}</p>` : ""}</div>`;
  }
  /** The test conversation: the newest question to the current test account, if there is one. */
  const testEng = () => { const v = testV(); return v && v.ready ? [...M.store.values()].filter(s => String(s.receiptId) === TEST && s.threadId === v.threadId).sort((a, b) => (b.createdAtMs || 0) - (a.createdAtMs || 0))[0] || null : null; };
  function openTest() { closeHealth(); const s = testEng(); standalone({ receiptId: TEST, scope: "order", id: s ? s.id : null }); }
  function sendTest() {
    const v = testV(); if (!v || !v.ready) return;
    const s = testEng(), clientId = uidOf();
    const at = new Date().toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
    queueOut({
      receiptId: TEST, clientId, sandbox: false, scope: "order", lineId: null, engagementId: s ? s.id : null, newQuestion: false,
      text: `Email link test from the Charm Sorter (${at}). Reply to this message to check that answers come back.`,
      lineLabel: "", orderNumber: "", buyerName: (v.customer && v.customer.name) || ""
    });
    openTest();
    if (SA && SA.P) SA.P.waitingFor = clientId;
  }
  document.addEventListener("click", async e => {
    const b = e.target.closest("[data-t]");
    if (!b || !healthBox || !healthBox.box.contains(b)) return;
    const a = b.dataset.t;
    if (a === "info" || a === "start" || a === "cancel") testCall("test_" + a);
    else if (a === "send") sendTest();
    else if (a === "open") openTest();
    else if (a === "copy") {
      const v = testV(), code = v && v.pending ? v.pending.code : "";
      try { await navigator.clipboard.writeText(code); say("Copied: " + code, "ok", 3000); } catch (_) { say("Select the code and copy it yourself", "bad"); }
    }
  });

  function apply(res, mine) {
    if (!res || typeof res !== "object") return;
    const changed = new Set();
    if (res.full) {
      const keep = new Set((res.changes || []).map(s => s.id));
      for (const id of [...M.store.keys()]) if (!keep.has(id)) { M.store.delete(id); changed.add(id); }
      M.needFull = false; M.fullAt = Date.now();
    }
    for (const s of res.changes || []) if (merge(s)) changed.add(s.id);
    if (typeof res.n === "number") M.n = res.n;
    if (typeof res.v === "number" && res.v > M.since) M.since = res.v;
    const first = M.first; M.first = false;
    if (changed.size) { announce([...changed], mine, first); paintSoon([...changed]); }
    else if (first) paintSoon([]);
  }
  /** One summary into the store; true when it is news. */
  function merge(s) {
    if (!s || !s.id || !!s.sandbox !== sbOf(s.receiptId)) return false;
    const cur = M.store.get(s.id);
    if (cur && (cur.v || 0) >= (s.v || 0) && !cur.local) return false;
    M.store.set(s.id, s);
    return true;
  }
  const forReceipt = rid => [...M.store.values()].filter(s => String(s.receiptId) === String(rid)).sort((a, b) => (b.createdAtMs || 0) - (a.createdAtMs || 0));

  // ─── alerts: a new answer from a customer is noticed, once ──────────────
  let titleBase = null, titleN = 0;
  function announce(ids, mine, first) {
    const told = get(LS.told, {});
    const fresh = [];
    for (const id of ids) {
      const s = M.store.get(id);
      if (!s || s.status !== "open" || !(s.unread > 0) || !s.lastInboundAtMs) continue;
      if (s.lastInboundAtMs <= (told[id] || 0)) continue;
      told[id] = s.lastInboundAtMs;
      // replies that were already waiting when the page opened are counted on the envelope, not announced
      if (!first) fresh.push(s);
    }
    // the list only ever holds questions still in the store
    for (const id of Object.keys(told)) if (!M.store.has(id) && M.store.size) delete told[id];
    put(LS.told, told);
    if (!fresh.length) return;
    // one tab speaks: the one being looked at, or when none is, the one that fetched the news
    if (document.hidden && !mine) return;
    const s = fresh[0];
    const who = (s.customer && (firstName(s.customer.name) || s.customer.username)) || "The customer";
    const more = fresh.length > 1 ? ` (and ${fresh.length - 1} more)` : "";
    say(`✉ ${String(s.receiptId) === TEST ? "Your test account replied" : `${who} replied about order ${s.receiptId}${s.scope === "engraving" ? " (engraving)" : ""}`}: “${plain(s.lastInboundPreview || "a photo").slice(0, 90)}”${more}`, "ok", 9000);
    try { if (typeof ding === "function") ding(); } catch (_) {}
    if (document.hidden) { titleN += fresh.length; if (titleBase == null) titleBase = document.title; document.title = `(${titleN}) ✉ ${titleBase}`; }
  }
  function clearTitle() { if (titleBase != null) { document.title = titleBase; titleBase = null; titleN = 0; } }

  // ─── painting, coalesced ─────────────────────────────────────────────────
  let paintTimer = 0, paintIds = new Set(), paintAllNext = false;
  function paintSoon(ids) { ids.forEach(id => paintIds.add(id)); if (!ids.length) paintAllNext = true; if (!paintTimer) paintTimer = setTimeout(flushPaint, 60); }
  function paintAll() { paintAllNext = true; if (!paintTimer) paintTimer = setTimeout(flushPaint, 0); }
  function flushPaint() {
    paintTimer = 0;
    const ids = paintIds; paintIds = new Set(); const all = paintAllNext; paintAllNext = false;
    paintPill();
    const rids = new Set([...ids].map(id => { const s = M.store.get(id); return s && String(s.receiptId); }).filter(Boolean));
    for (const P of M.panes) {
      if (!P.rid) continue;
      if (all || rids.has(String(P.rid)) || !M.key) {
        const s = P.engId && M.store.get(P.engId);
        if (M.key && s && P.eng && (s.v || 0) > (P.eng.v || 0)) refreshEng(P);
        else if (M.key && !P.eng && !P.fresh && forReceipt(P.rid).length && !P.loading) load(P);
        else paintPane(P);
      }
    }
    for (const L of M.lines.values()) if (L.node.isConnected && (all || rids.has(String(L.rid)))) paintLine(L);
    for (const L of M.lines.values()) if (!L.node.isConnected && Date.now() - L.at > 10 * 60000) { M.lines.delete(L.key); if (lineSeen) lineSeen.unobserve(L.node); }
    paintTabDots();
    badgesChanged(all ? null : rids);
  }

  // ─── the envelope in the top bar: new replies and messages that did not go ──
  let pillMenu = null;
  function paintPill() {
    let b = document.getElementById("mailPill");
    if (!b) {
      const host = document.querySelector(".topbar .topTools"); if (!host) return;
      b = h("button", "mailPill hidden"); b.id = "mailPill"; b.type = "button";
      b.onclick = e => { e.stopPropagation(); togglePillMenu(); };
      host.insertBefore(b, host.firstChild);
    }
    const open = [...M.store.values()].filter(s => s.status === "open");
    const unread = open.filter(s => s.unread > 0), stuck = open.filter(s => s.failed > 0 || s.manual > 0);
    const n = unread.length || stuck.length;
    b.classList.toggle("hidden", !M.key || !n);
    b.classList.toggle("bad", !unread.length && !!stuck.length);
    b.innerHTML = ICON.mail + `<b>${n}</b>`;
    b.title = unread.length ? `${unread.length} customer ${unread.length === 1 ? "conversation has" : "conversations have"} a new reply` : stuck.length ? `${stuck.length} message${stuck.length === 1 ? "" : "s"} to customers did not go` : "";
    if (pillMenu && pillMenu.isConnected) fillPillMenu();
  }
  function togglePillMenu() {
    if (pillMenu && pillMenu.isConnected) { pillMenu.remove(); pillMenu = null; return; }
    const b = document.getElementById("mailPill"); if (!b) return;
    pillMenu = h("div", "mailMenu"); pillMenu.setAttribute("role", "menu");
    document.body.appendChild(pillMenu);
    const r = b.getBoundingClientRect();
    pillMenu.style.top = Math.round(r.bottom + 6) + "px";
    pillMenu.style.right = Math.max(8, Math.round(window.innerWidth - r.right)) + "px";
    fillPillMenu();
    setTimeout(() => document.addEventListener("pointerdown", function off(e) { if (pillMenu && !pillMenu.contains(e.target) && e.target !== b && !b.contains(e.target)) { pillMenu.remove(); pillMenu = null; document.removeEventListener("pointerdown", off, true); } }, true), 0);
  }
  function fillPillMenu() {
    const open = [...M.store.values()].filter(s => s.status === "open" && (s.unread > 0 || s.failed > 0 || s.manual > 0))
      .sort((a, b) => (b.unread > 0) - (a.unread > 0) || (b.lastInboundAtMs || b.updatedAtMs || 0) - (a.lastInboundAtMs || a.updatedAtMs || 0));
    pillMenu.innerHTML = `<div class="mmHead">Customers</div>` + (open.length ? open.slice(0, 12).map(s => {
      const who = (s.customer && (s.customer.name || s.customer.username)) || "Customer";
      const line = s.unread > 0 ? `“${E(plain(s.lastInboundPreview || "a photo"))}”` : s.failed > 0 ? "A message did not go — open it to retry" : "Waiting to be sent by hand on Etsy";
      return `<button type="button" class="mmRow${s.unread > 0 ? " new" : " bad"}" data-id="${E(s.id)}"><span class="mmTop"><b>${E(who)}</b><span class="mono">${String(s.receiptId) === TEST ? "email link test" : E(s.receiptId)}${s.scope === "engraving" ? " · engraving" : ""}</span><span class="mmWhen">${E(when(s.lastInboundAtMs || s.updatedAtMs))}</span></span><span class="mmLine">${line}</span></button>`;
    }).join("") : `<div class="mmEmpty">Nothing waiting.</div>`);
    pillMenu.querySelectorAll(".mmRow").forEach(x => x.onclick = () => { const s = M.store.get(x.dataset.id); pillMenu.remove(); pillMenu = null; if (s) openConversation(s); });
  }

  /** Show one question: in the order window when its order is in the pull, otherwise in a window of its own. */
  function openConversation(s, rowKey, extra = {}) {
    const rows = (window.Orders && Orders.rows && Orders.rows()) || [];
    const row = rows.find(r => r.key === rowKey)
      || (s.lineId && rows.find(r => String(r.order.receiptId) === String(s.receiptId) && String(r.line.transactionId) === String(s.lineId)))
      || rows.find(r => String(r.order.receiptId) === String(s.receiptId));
    if (row && window.OrderWin) { OrderWin.open(row.key, Object.assign({ tab: "customer", engagementId: s.id || null }, extra)); return; }
    standalone(s, extra);
  }
  /** "Pull all messages" pressed somewhere else: it runs in the pane once the conversation there has loaded. */
  function pullWhenReady(P) { if (P.loading) P.pullOnLoad = true; else pullAll(P); }

  // ─── Orders tab badges ───────────────────────────────────────────────────
  let badgeTimer = 0, badgeRids = new Set(), badgeAll = false;
  function badgesChanged(rids) {
    if (rids === null) badgeAll = true; else rids.forEach(r => badgeRids.add(r));
    if (!badgeAll && !badgeRids.size) return;
    if (badgeTimer) return;
    badgeTimer = setTimeout(() => {
      badgeTimer = 0;
      const all = badgeAll, set = badgeRids; badgeAll = false; badgeRids = new Set();
      const rows = (window.Orders && Orders.rows && Orders.rows()) || [];
      if (all || rows.some(r => set.has(String(r.order.receiptId)))) { try { (Orders.renderBody || Orders.render)(); } catch (_) {} }
    }, 400);
  }
  function badgeState(rid) {
    const list = forReceipt(rid).filter(s => s.status === "open");
    if (!list.length || !M.key) return null;
    const unread = list.reduce((n, s) => n + (s.unread || 0), 0);
    const bad = list.some(s => s.failed > 0 || s.manual > 0);
    const pending = list.some(s => s.pending > 0);
    return { unread, bad, pending, n: list.length };
  }
  function badge(rid) {
    const b = badgeState(rid); if (!b) return "";
    if (b.unread) return `<span class="mailTag new" title="${b.unread} new ${b.unread === 1 ? "reply" : "replies"} from the customer">${ICON.mail}${b.unread}</span>`;
    if (b.bad) return `<span class="mailTag bad" title="a message to this customer did not go">${ICON.mail}!</span>`;
    return `<span class="mailTag" title="${b.pending ? "a message to the customer is on its way" : "a question to the customer is open"}">${ICON.mail}</span>`;
  }
  const badgeStamp = rid => JSON.stringify(badgeState(rid));

  // ─── what someone typed is kept: drafts, and sends that have not reached the server yet ──
  const drafts = get(LS.drafts, {});
  let draftTimer = 0;
  function setDraft(key, text) {
    if (text && text.trim()) drafts[key] = { t: text, at: Date.now() }; else delete drafts[key];
    clearTimeout(draftTimer);
    draftTimer = setTimeout(() => {
      // a month of drafts at most, and the newest sixty
      const cut = Date.now() - 30 * 86400000;
      const keys = Object.keys(drafts).filter(k => drafts[k].at > cut).sort((a, b) => drafts[b].at - drafts[a].at).slice(0, 60);
      const keep = {}; keys.forEach(k => keep[k] = drafts[k]);
      Object.keys(drafts).forEach(k => { if (!keep[k]) delete drafts[k]; });
      put(LS.drafts, keep);
    }, 300);
  }
  const draftOf = key => (drafts[key] && drafts[key].t) || "";

  /* A message is written to this browser's outbox before it is sent, and leaves it when the server has it. A reload, a
     closed window or a dropped connection in between only delays it: the server takes each message once, by its id. */
  let outBusy = false;
  const pendingOut = () => get(LS.out, []);
  function queueOut(body) {
    const list = pendingOut();
    list.push({ id: body.clientId, body, at: Date.now(), tries: 0 });
    put(LS.out, list);
    if (bc) try { bc.postMessage({ t: "out" }); } catch (_) {}
    flushOut();
  }
  function dropOut(id) { put(LS.out, pendingOut().filter(x => x.id !== id)); }
  async function flushOut() {
    if (outBusy || !M.key) return;
    outBusy = true;
    try {
      for (;;) {
        const list = pendingOut().filter(x => !x.error);
        if (!list.length) break;
        const x = list[0];
        // another tab may be sending it right now: whoever claims it first sends it
        const claim = get("cn.mail.outclaim." + x.id, null);
        if (claim && claim.tab !== TAB && Date.now() - claim.at < 30000) break;
        put("cn.mail.outclaim." + x.id, { tab: TAB, at: Date.now() });
        try {
          const res = await call("ask", x.body);
          dropOut(x.id); put("cn.mail.outclaim." + x.id, null);
          received(res, x.body);
        } catch (e) {
          put("cn.mail.outclaim." + x.id, null);
          if (authLost(e)) break;
          const all = pendingOut(); const it = all.find(y => y.id === x.id);
          if (it) {
            it.tries = (it.tries || 0) + 1;
            // a refusal is final and shown on the message; a lost connection is tried again
            if (e.status >= 400 && e.status < 500 && e.status !== 408 && e.status !== 429) it.error = e.message;
            put(LS.out, all);
          }
          paintAll();
          if (!it || !it.error) { setTimeout(flushOut, Math.min(60000, 3000 * 2 ** Math.min(it ? it.tries : 1, 4))); break; }
        }
      }
    } finally { outBusy = false; paintAll(); }
  }
  /** A full engagement came back from the server: every pane showing it takes it. */
  function received(res, sentBody) {
    if (!res || !res.id) return;
    merge(Object.assign({}, res, { messages: undefined, earlier: undefined }));
    for (const P of M.panes) {
      if (String(P.rid) !== String(res.receiptId)) continue;
      const mine = P.engId === res.id || (sentBody && sentBody.clientId && P.waitingFor === sentBody.clientId) || (!P.eng && (!P.engId || P.fresh));
      if (!mine) continue;
      P.eng = res; P.engId = res.id; P.fresh = false; P.waitingFor = null;
      paintPane(P);
    }
    paintSoon([res.id]);
  }

  // ─── translation ─────────────────────────────────────────────────────────
  const trMem = new Map(get(LS.tr, []));
  let trSaveTimer = 0;
  const hashOf = s => { let x = 0x811c9dc5; for (let i = 0; i < s.length; i++) { x ^= s.charCodeAt(i); x = Math.imul(x, 16777619); } return (x >>> 0).toString(36) + "." + s.length; };
  const trKey = (lang, text) => lang + "|" + hashOf(text);
  function trRemember(key, v) {
    trMem.set(key, v);
    clearTimeout(trSaveTimer);
    trSaveTimer = setTimeout(() => { const all = [...trMem.entries()].slice(-250); put(LS.tr, all); }, 800);
  }
  const trInflight = new Map();
  /** Translations of several texts into one language: kept ones at once, the rest in small batches, two at a time. */
  async function translateTexts(texts, lang) {
    const want = [...new Set(texts.map(t => plain(t).trim()).filter(Boolean))];
    const out = new Map(), missing = [];
    for (const t of want) { const k = trKey(lang, t); if (trMem.has(k)) out.set(t, trMem.get(k)); else missing.push(t); }
    const batches = [];
    let cur = [], chars = 0;
    for (const t of missing) {
      if (cur.length && (cur.length >= 4 || chars + t.length > 2500)) { batches.push(cur); cur = []; chars = 0; }
      cur.push(t); chars += t.length;
    }
    if (cur.length) batches.push(cur);
    const run = async batch => {
      const key = lang + "|" + batch.map(hashOf).join(",");
      if (!trInflight.has(key)) trInflight.set(key, call("translate", { target: lang, items: batch.map((t, i) => ({ id: String(i), text: t })) }, { timeout: 26000 }).finally(() => trInflight.delete(key)));
      const res = await trInflight.get(key);
      batch.forEach((t, i) => { const v = res.translations && res.translations[String(i)]; if (v) { trRemember(trKey(lang, t), v); out.set(t, v); } });
    };
    for (let i = 0; i < batches.length; i += 2) await Promise.all(batches.slice(i, i + 2).map(run));
    return out;
  }
  const trCached = (text, lang) => trMem.get(trKey(lang, plain(text).trim())) || null;
  const LANG = { en: "English", uk: "Ukrainian" };
  const LANG_SHORT = { en: "EN", uk: "УКР" };
  const langName = code => ({ en: "English", uk: "Ukrainian", ru: "Russian", de: "German", fr: "French", es: "Spanish", it: "Italian", nl: "Dutch", pl: "Polish", pt: "Portuguese", ja: "Japanese" }[code] || (code ? code.toUpperCase() : ""));

  // ─── the conversation pane ──────────────────────────────────────────────
  /* One question at a time, and only its stretch of the conversation: the customer's messages from the moment it was
     asked (a question can be resolved, and a new one starts clean). What goes to the customer is slate and says so; the
     team's own chat, next door, is warm paper. */
  function Pane(host, opts = {}) {
    const P = {
      host, opts, rid: null, ctx: {}, eng: null, engId: null, data: null, fresh: false, loading: false, err: null, seq: 0,
      earlier: null, lang: null, trOpen: new Map(), undo: null, waitingFor: null, stick: true, hist: null, view: "question", hseq: 0, pullOnLoad: false,
      visible: () => host.isConnected && !host.closest("[hidden]") && !!(host.offsetWidth || host.offsetHeight) && !document.hidden && (!opts.visible || opts.visible())
    };
    host.classList.add("cm");
    host.innerHTML = `
      <div class="cmHead">
        <span class="cmMark">${ICON.mail}</span>
        <div class="cmWho"><b data-cm="name">Customer</b><span data-cm="sub"></span></div>
        <span class="cmLang" role="group" aria-label="Translate this conversation" title="Show every message in this conversation translated"><button type="button" data-cm="all" data-l="en">EN</button><button type="button" data-cm="all" data-l="uk">УКР</button></span>
        <span class="cmState" data-cm="state"></span>
        <details class="cmMore"><summary title="More" aria-label="More">⋯</summary><div class="cmMenu" data-cm="menu"></div></details>
      </div>
      <div class="cmQs" data-cm="qs" hidden></div>
      <div class="cmNotice" data-cm="notice" hidden></div>
      <div class="cmHist" data-cm="hist" hidden></div>
      <div class="cmThread" data-cm="thread" aria-live="polite"></div>
      <div class="cmComp" data-cm="comp">
        <div class="cmHBox" data-cm="hbox" role="dialog" aria-label="Email link" hidden></div>
        <div class="cmWarn" data-cm="warn" hidden></div>
        <div class="cmUndo" data-cm="undo" hidden></div>
        <div class="cmLine"><textarea data-cm="input" rows="1" placeholder="Write to the customer…" aria-label="Message to the customer"></textarea><button type="button" class="cmSend" data-cm="send" title="Send to the customer (Enter)" aria-label="Send to the customer" disabled>${ICON.send}</button></div>
        <div class="cmHint"><span class="cmLive" data-cm="live"></span><span class="cmHintT" data-cm="hint">Goes to the customer on Etsy, through the inbox</span><span class="cmTrIn">Translate mine <button type="button" data-cm="trIn" data-l="en" title="Translate what you wrote into English">EN</button><button type="button" data-cm="trIn" data-l="uk" title="Translate what you wrote into Ukrainian">УКР</button></span></div>
      </div>`;
    const $ = n => host.querySelector(`[data-cm="${n}"]`);
    P.el = { name: $("name"), sub: $("sub"), state: $("state"), menu: $("menu"), more: host.querySelector(".cmMore"), lang: host.querySelector(".cmLang"), qs: $("qs"), notice: $("notice"), thread: $("thread"), comp: $("comp"), input: $("input"), send: $("send"), hint: $("hint"), undo: $("undo"), live: $("live"), warn: $("warn"), hbox: $("hbox"), hist: $("hist") };
    const input = P.el.input;
    const grow = () => { input.style.height = "auto"; const hh = input.scrollHeight; input.style.height = hh ? Math.min(160, hh + 2) + "px" : ""; P.el.send.disabled = !input.value.trim(); };
    input.addEventListener("input", () => { grow(); setDraft(draftKey(P), input.value); if (P.undo && input.value !== P.undo.to) { P.undo = null; paintUndo(P); } });
    input.addEventListener("keydown", e => { e.stopPropagation(); if (e.key === "Enter" && !e.shiftKey && !e.isComposing) { e.preventDefault(); send(P); } });
    P.el.send.onclick = () => send(P);
    P.grow = grow;
    P.el.thread.addEventListener("scroll", () => { const t = P.el.thread; P.stick = t.scrollHeight - t.scrollTop - t.clientHeight < 40; }, { passive: true });
    host.addEventListener("click", e => onPaneClick(P, e));
    M.panes.add(P);
    return P;
  }
  function draftKey(P) { return `${P.rid}:${P.fresh ? "new" : P.engId || (P.ctx.scope === "engraving" ? "l" + (P.ctx.lineId || "") : "o")}`; }

  /** Point a pane at an order (and optionally at one of its questions). */
  function show(P, ctx, { engagementId = null } = {}) {
    const rid = String(ctx.receiptId || "");
    const same = P.rid === rid;
    P.ctx = ctx;
    if (!same) { P.rid = rid; P.eng = null; P.engId = engagementId; P.data = null; P.fresh = false; P.err = null; P.earlier = null; P.trOpen = new Map(); P.stick = true; P.undo = null; P.hist = null; P.view = "question"; P.hseq++; }
    else if (engagementId && engagementId !== P.engId) { P.engId = engagementId; P.eng = null; P.fresh = false; P.earlier = null; P.stick = true; }
    if (!same || engagementId || !P.eng) { input0(P); load(P); } else paintPane(P);
    healthSoon();
  }
  function input0(P) {
    const d = draftOf(draftKey(P));
    if (document.activeElement !== P.el.input) { P.el.input.value = d; P.grow(); }
  }

  async function load(P) {
    if (!P.rid) return;
    if (!M.key) { paintPane(P); return; }
    const seq = ++P.seq;
    P.loading = true; paintState(P);
    try {
      const d = await call("order", { receiptId: P.rid, sandbox: sbOf(P.rid), scope: P.ctx.scope || "order", lineId: P.ctx.lineId || null, engagementId: P.engId || null });
      if (seq !== P.seq) return;
      P.data = d; P.err = null;
      for (const s of d.engagements || []) merge(s);
      if (!P.fresh) { P.eng = d.active || null; P.engId = d.active ? d.active.id : null; }
      if (P.eng && P.lang == null) P.lang = P.eng.lang || null;
      input0(P);
    } catch (e) { if (seq === P.seq && !authLost(e)) P.err = e.message; }
    finally {
      if (seq === P.seq) {
        P.loading = false; paintPane(P); markRead(P); if (P.lang) translateAll(P);
        if (P.pullOnLoad) { P.pullOnLoad = false; pullAll(P); }
      }
    }
  }
  async function refreshEng(P, earlier) {
    if (!P.engId || !M.key) return;
    const seq = ++P.seq;
    try {
      const d = await call("thread", { engagementId: P.engId, sandbox: sbOf(P.rid), earlier: !!earlier });
      if (seq !== P.seq) return;
      P.eng = d; P.err = null; merge(Object.assign({}, d, { messages: undefined, earlier: undefined }));
      if (earlier) P.earlier = d.earlier || [];
    } catch (e) { if (seq === P.seq && !authLost(e)) P.err = e.message; }
    if (seq === P.seq) { paintPane(P); markRead(P); if (P.lang) translateAll(P); }
  }
  let readBusy = new Set();
  async function markRead(P) {
    const s = P.engId && M.store.get(P.engId);
    if (!s || !(s.unread > 0) || !P.visible() || readBusy.has(s.id)) return;
    readBusy.add(s.id);
    // every open question of the same conversation showed the reply: it is read once
    const cleared = [...M.store.values()].filter(x => x.id === s.id || (s.threadId && x.threadId === s.threadId && x.status === "open" && x.unread > 0));
    cleared.forEach(x => M.store.set(x.id, Object.assign({}, x, { unread: 0 })));
    paintSoon(cleared.map(x => x.id));
    try { await call("read", { engagementId: s.id }); } catch (_) {}
    readBusy.delete(s.id);
  }

  function paintState(P) {
    // whether the line works is the light under the box being written in; up here only a first load shows
    P.el.state.textContent = M.key && P.loading && !P.eng ? "loading…" : "";
  }

  function paintPane(P) {
    if (!P.host.isConnected && !P.opts.keep) return;
    paintState(P);
    const e = P.el;
    const list = P.rid ? forReceipt(P.rid) : [];
    const s = P.eng || (P.engId && M.store.get(P.engId)) || null;
    const conv = P.data && P.data.conversation;
    const cust = (s && s.customer) || (conv && conv.customer) || {};
    const name = cust.name || cust.username || P.ctx.buyerName || "Customer";
    e.name.textContent = name;
    const sb = sbOf(P.rid), test = P.rid === TEST;
    e.sub.textContent = [test ? "your test account" : P.rid ? "order " + P.rid : "", cust.username && cust.name ? "@" + cust.username : "", sb ? "sandbox" : "via Etsy"].filter(Boolean).join(" · ");
    host0(P);
    e.lang.hidden = e.more.hidden = !M.key;
    // not connected: one clear way in, and nothing else
    if (!M.key) {
      e.qs.hidden = true; e.notice.hidden = true; e.comp.hidden = true; e.hist.hidden = true;
      e.thread.innerHTML = connectCard();
      e.thread.querySelectorAll("[data-cm-do]").forEach(b => b.onclick = () => { const a = b.dataset.cmDo; if (a === "connect") connect(); else if (a === "inbox" && M.pair) openInbox(M.pair.url); else if (a === "cancel") cancelPair(); });
      return;
    }
    e.comp.hidden = false;
    // the questions of this order, when there is more than one
    const others = list.filter(x => x.status === "open" || x.id === P.engId);
    if (others.length > 1 || (P.fresh && others.length)) {
      e.qs.hidden = false;
      e.qs.innerHTML = others.map(x => `<button type="button" class="cmQ${x.id === P.engId && !P.fresh ? " on" : ""}" data-q="${E(x.id)}" title="${E(x.title || "")}">${E(qLabel(x))}${x.unread > 0 ? '<i class="cmDot"></i>' : ""}</button>`).join("") + (P.fresh ? `<button type="button" class="cmQ on">New question</button>` : "");
    } else { e.qs.hidden = true; e.qs.innerHTML = ""; }
    // the state of the question itself, in one line where it matters
    const notice = noticeFor(P, s, conv);
    e.notice.hidden = !notice; e.notice.innerHTML = notice || "";
    e.hint.textContent = sb ? "Sandbox: kept here, never sent to a real customer" : test ? ((s && s.threadId) || conv ? `Test: goes only to ${name}, your own Etsy account` : "No test account yet") : s && s.status === "resolved" ? "Sending reopens this question" : (s && s.link === "waiting") || (!s && P.data && !conv) ? "No Etsy conversation with this buyer yet: you send it on Etsy by hand" : `Goes to ${firstName(name) || "the customer"} on Etsy, through the inbox`;
    e.input.placeholder = P.fresh || !s ? `Ask ${firstName(name) || "the customer"} something…` : `Write to ${firstName(name) || "the customer"}…`;
    // the menu
    e.menu.innerHTML = [
      s && s.threadId && !sb ? `<a href="${E(INBOX + "#thread=" + encodeURIComponent(s.threadId))}" target="cn-mail-inbox" rel="noopener">Open in the inbox ${ICON.out}</a>` : "",
      s && s.status === "open" ? `<button type="button" data-cm-do="resolve">Mark as answered</button>` : "",
      s && s.status === "resolved" ? `<button type="button" data-cm-do="reopen">Reopen</button>` : "",
      s && !P.fresh ? `<button type="button" data-cm-do="new">Ask a new question</button>` : "",
      s && s.link === "waiting" && !sb ? `<button type="button" data-cm-do="link">Link an Etsy conversation…</button>` : "",
      sb && s ? `<button type="button" data-cm-do="simulate">Play a customer reply</button>` : "",
      `<button type="button" data-cm-do="disconnect" class="soft">Disconnect this sorter</button>`
    ].filter(Boolean).join("");
    paintHist(P);
    paintThread(P, s);
    paintUndo(P);
    paintLight(P.el, P.host, "cmWarn");
  }
  function host0(P) { P.host.dataset.state = !M.key ? "off" : P.fresh ? "new" : P.eng ? (P.eng.status || "open") : "empty"; }
  const qLabel = x => x.scope === "engraving" ? "Engraving" + (x.lineLabel ? " · " + x.lineLabel : "") : x.title ? x.title.slice(0, 36) : "Order question";

  function connectCard() {
    if (M.pair) return `<div class="cmEmpty"><b>Approve it in the inbox</b>The inbox opened in another tab: press <i>Connect</i> there. This waits for it${M.pair.blocked ? " — the browser stopped the tab from opening, so open it yourself" : ""}.
      <div class="cmBtns"><button type="button" class="btn sm cmBtn" data-cm-do="inbox">Open the inbox</button><button type="button" class="btn ghost sm" data-cm-do="cancel">Cancel</button></div></div>`;
    return `<div class="cmEmpty"><b>Talk to this customer from here</b>Questions go out through the inbox's Etsy connection, and their answers show up here the moment the inbox reads them. Connect this sorter to the inbox once.
      <div class="cmBtns"><button type="button" class="btn sm cmBtn" data-cm-do="connect">Connect to the inbox</button></div></div>`;
  }

  function noticeFor(P, s, conv) {
    if (P.err && !s) return `<span class="bad">${E(P.err)}</span> <button type="button" class="lnk" data-cm-do="reload">Try again</button>`;
    if (!s) {
      if (!P.data) return "";
      if (sbOf(P.rid)) return "Sandbox: questions here are kept apart from real customers.";
      if (!conv && P.rid === TEST) return "No test account yet: open the light under the message box and press Set up a test account.";
      if (!conv) return "This buyer has not written to the shop yet, so the inbox has no conversation to send through. What you write here is kept, and you send it on Etsy by hand.";
      return "";
    }
    if (s.status === "resolved") return `Answered${s.resolvedBy ? " · marked by " + E(s.resolvedBy) : ""}${s.resolvedAtMs ? " · " + E(when(s.resolvedAtMs)) : ""}. <button type="button" class="lnk" data-cm-do="reopen">Reopen</button> · <button type="button" class="lnk" data-cm-do="new">Ask a new question</button>`;
    if (s.link === "waiting" && !sbOf(P.rid)) return "This buyer has no Etsy conversation with the shop yet. Messages wait here: send them on Etsy by hand, or they go by themselves as soon as the customer writes to the shop.";
    if (s.linkedBy === "buyer") return "Sent through this buyer's latest conversation with the shop (it does not name this order).";
    return "";
  }

  function paintUndo(P) {
    const u = P.el.undo;
    if (!P.undo) { u.hidden = true; u.innerHTML = ""; return; }
    u.hidden = false;
    u.innerHTML = `Translated into ${E(LANG[P.undo.lang])} <button type="button" class="lnk" data-cm-do="undo">Undo</button>`;
  }

  /** The thread: this question's stretch of the conversation, plus what this browser has not handed over yet. */
  function paintThread(P, s) {
    const t = P.el.thread;
    const wasAt = t.scrollTop, stick = P.stick;
    const rows = [];
    if (P.fresh) {
      t.innerHTML = `<div class="cmEmpty"><b>A new question</b>Only the answers from now on show here, so the earlier conversation stays out of the way.</div>` + localRows(P).map(r => msgHtml(P, r)).join("");
      if (stick) t.scrollTop = t.scrollHeight;
      return;
    }
    if (P.loading && !s) { t.innerHTML = `<div class="cmEmpty soft">Loading the conversation…</div>`; return; }
    const msgs = (P.eng && P.eng.messages) || [];
    if (showingAll(P)) {
      t.innerHTML = allHtml(P);
      t.querySelectorAll("img[data-cm-img]").forEach(im => im.addEventListener("error", () => { const a = h("a", "cmPhoto", "photo"); a.href = im.dataset.href || im.src; a.target = "_blank"; a.rel = "noopener"; im.replaceWith(a); }, { once: true }));
      if (stick) t.scrollTop = t.scrollHeight; else t.scrollTop = wasAt;
      return;
    }
    if (P.earlier && P.earlier.length) {
      rows.push(`<div class="cmEarlier">${P.earlier.map(r => msgHtml(P, Object.assign({}, r, { old: true }))).join("")}</div><div class="cmSep"><span>Question asked${P.eng && P.eng.startedAtMs ? " · " + E(when(P.eng.startedAtMs)) : ""}</span></div>`);
    }
    for (const m of msgs) rows.push(msgHtml(P, m));
    for (const r of localRows(P)) rows.push(msgHtml(P, r));
    if (!msgs.length && !localRows(P).length) rows.push(s ? `<div class="cmEmpty soft">Nothing in this conversation yet.</div>` : `<div class="cmEmpty"><b>No questions to this customer yet</b>Write below: it reaches ${E(firstName(P.el.name.textContent) || "the customer")} on Etsy through the inbox, and the answer shows up here.</div>`);
    t.innerHTML = rows.join("");
    t.querySelectorAll("img[data-cm-img]").forEach(im => im.addEventListener("error", () => { const a = h("a", "cmPhoto", "photo"); a.href = im.dataset.href || im.src; a.target = "_blank"; a.rel = "noopener"; im.replaceWith(a); }, { once: true }));
    if (stick) t.scrollTop = t.scrollHeight; else t.scrollTop = wasAt;
  }
  /** Messages still in this browser's outbox for this pane's question. */
  function localRows(P) {
    return pendingOut().filter(x => String(x.body.receiptId) === String(P.rid) && (
      x.body.engagementId ? x.body.engagementId === P.engId && !P.fresh
        : P.fresh ? x.body.newQuestion : !P.engId && (x.body.scope || "order") === (P.ctx.scope || "order")))
      .map(x => ({ id: "local_" + x.id, local: x, side: "us", who: (M.who && M.who.name) || "You", atMs: x.at, text: x.body.text, status: x.error ? "local_failed" : "local", error: x.error || null }));
  }
  const STATUS = {
    local: "Sending…", new: "Sending…", queued: "With the Etsy helper…", sending: "Sending on Etsy…",
    waiting: "Waits for the inbox to finish a send", manual: "Not sent yet", failed: "Not sent", local_failed: "Not sent", sent: "Sent"
  };
  function msgHtml(P, m) {
    const side = m.side === "customer" ? "cust" : m.side === "shop" ? "shop" : "us";
    const text = plain(m.text || "");
    const o = P.trOpen.get(m.id);
    const tro = o === "__off" ? null : (o || P.lang || null);
    const tr = tro && text.trim() ? trCached(text, tro) : null;
    let status = "", acts = "", tone = "";
    if (side === "us") {
      const st = m.status || "sent";
      // solid slate is what reached the customer; on its way is paler, what did not go is outlined
      tone = st === "failed" || st === "local_failed" ? " fail" : st === "manual" ? " hold" : st === "sent" ? "" : " pend";
      let word = STATUS[st] || "";
      if (st === "waiting" && m.waitReason === "paused") word = "Waits: sending is paused in the inbox";
      if (st === "waiting" && m.waitReason === "retry") word = "Trying again…";
      if (st === "sent") word = m.manualSent ? "Sent by hand" : m.unverified ? "Sent — Etsy did not confirm" : m.delivered ? "Sent · on Etsy" : "Sent";
      status = `<span class="cmSt ${st === "failed" || st === "local_failed" ? "bad" : st === "sent" ? "ok" : st === "manual" ? "warn" : "go"}">${E(word)}</span>`;
      if (st === "failed" || st === "local_failed") acts = `<div class="cmErr">${E(m.error || "It did not go.")}</div><div class="cmActs"><button type="button" class="lnk" data-cm-do="${st === "local_failed" ? "edit" : "retry"}" data-item="${E(m.itemId || m.local && m.local.id || "")}">${st === "local_failed" ? "Edit and send again" : "Try again"}</button><button type="button" class="lnk soft" data-cm-do="${st === "local_failed" ? "discard" : "cancel"}" data-item="${E(m.itemId || m.local && m.local.id || "")}">Discard</button></div>`;
      else if (st === "manual") acts = `<div class="cmActs"><button type="button" class="lnk" data-cm-do="copy" data-item="${E(m.itemId)}">Copy and open the order on Etsy</button><button type="button" class="lnk" data-cm-do="sentByHand" data-item="${E(m.itemId)}">I sent it</button><button type="button" class="lnk soft" data-cm-do="cancel" data-item="${E(m.itemId)}">Discard</button></div>${m.copied ? "" : `<div class="cmFine">It goes by itself if the customer writes to the shop first.</div>`}`;
      else if (st === "new" || st === "queued" || (st === "waiting")) acts = `<div class="cmActs"><button type="button" class="lnk soft" data-cm-do="cancel" data-item="${E(m.itemId || "")}">Cancel</button></div>`;
      else if (st === "failed") acts = "";
    }
    const who = side === "cust" ? (m.who || "Customer") : side === "shop" ? (m.who ? m.who + " · inbox" : "Inbox") : (m.who || "You");
    const images = (m.images || []).map(im => im.src ? `<a href="${E(im.href || im.src)}" target="_blank" rel="noopener"><img data-cm-img data-href="${E(im.href || "")}" loading="lazy" alt="photo from the customer" src="${E(im.src)}"></a>` : `<a class="cmPhoto" href="${E(im.href)}" target="_blank" rel="noopener">photo ${ICON.out}</a>`).join("");
    const cards = (m.cards || []).map(c => `<a class="cmCard" href="${E(c.url)}" target="_blank" rel="noopener">${E(c.title)} ${ICON.out}</a>`).join("");
    const trBox = tr && !tr.same ? `<div class="cmTr"><span class="cmTrLbl">${E(LANG[tro])}${tr.from && tr.from !== tro ? " · from " + E(langName(tr.from)) : ""}</span>${html(tr.text)}</div>`
      : tro && text.trim() && !tr ? `<div class="cmTr soft">Translating…</div>` : "";
    const trBtns = text.trim() && !m.local ? `<span class="cmTrB" role="group" aria-label="Translate this message"><button type="button" data-cm-tr="en" data-id="${E(m.id)}" class="${tro === "en" ? "on" : ""}" title="Translate into English">EN</button><button type="button" data-cm-tr="uk" data-id="${E(m.id)}" class="${tro === "uk" ? "on" : ""}" title="Translate into Ukrainian">УКР</button></span>` : "";
    return `<div class="cmMsg ${side}${tone}${m.old ? " old" : ""}" data-id="${E(m.id)}"><div class="cmMeta"><b>${E(who)}</b><span>${E(when(m.atMs))}</span>${status}${trBtns}</div>${text.trim() ? `<div class="cmBody">${html(text)}</div>` : ""}${images ? `<div class="cmImgs">${images}</div>` : ""}${cards}${trBox}${acts}</div>`;
  }

  /** Every text a pane shows, for translating the lot. */
  function paneTexts(P) {
    const msgs = showingAll(P) ? P.hist.rows.slice(-60).concat((P.eng && P.eng.messages) || []) : [].concat(P.earlier || [], (P.eng && P.eng.messages) || []);
    return msgs.map(m => ({ id: m.id, text: plain(m.text || "") })).filter(x => x.text.trim());
  }
  async function translateAll(P) {
    const lang = P.lang; if (!lang) return;
    const items = paneTexts(P).filter(x => !trCached(x.text, lang));
    if (!items.length) { paintThread(P, P.eng); return; }
    paintThread(P, P.eng);
    try { await translateTexts(items.map(x => x.text), lang); }
    catch (e) { say("Translation: " + e.message, "bad"); P.lang = null; }
    paintThread(P, P.eng); paintLangBtns(P);
  }
  function paintLangBtns(P) { P.host.querySelectorAll('[data-cm="all"]').forEach(b => b.classList.toggle("on", b.dataset.l === P.lang)); }

  async function onPaneClick(P, e) {
    if (e.target.closest("[data-health]")) { toggleHealth(P.el.hbox, P.host); return; }
    const tr = e.target.closest("[data-cm-tr]");
    if (tr) {
      const id = tr.dataset.id, lang = tr.dataset.cmTr;
      const o = P.trOpen.get(id);
      const cur = o === "__off" ? null : (o || P.lang);
      // a second press hides it; with the whole conversation translated, this one message stays hidden
      if (cur === lang) { if (P.lang) P.trOpen.set(id, "__off"); else P.trOpen.delete(id); }
      else P.trOpen.set(id, lang);
      const want = P.trOpen.get(id);
      const m = [].concat(P.earlier || [], (P.eng && P.eng.messages) || [], (P.hist && P.hist.rows) || []).find(x => x.id === id);
      paintThread(P, P.eng);
      if (m && want && want !== "__off" && !trCached(m.text, want)) {
        try { await translateTexts([m.text], want); } catch (err) { say("Translation: " + err.message, "bad"); P.trOpen.delete(id); }
        paintThread(P, P.eng);
      }
      return;
    }
    const all = e.target.closest('[data-cm="all"]');
    if (all) {
      P.lang = P.lang === all.dataset.l ? null : all.dataset.l;
      P.trOpen = new Map();
      paintLangBtns(P);
      if (P.engId && !String(P.engId).startsWith("local")) call("lang", { engagementId: P.engId, lang: P.lang || "" }).catch(() => {});
      if (P.lang) translateAll(P); else paintThread(P, P.eng);
      return;
    }
    const tin = e.target.closest('[data-cm="trIn"]');
    if (tin) { translateComposer(P, tin.dataset.l); return; }
    const q = e.target.closest("[data-q]");
    if (q) { P.fresh = false; P.engId = q.dataset.q; P.eng = null; P.earlier = null; P.stick = true; input0(P); refreshEng(P); paintPane(P); return; }
    const b = e.target.closest("[data-cm-do]");
    if (!b) return;
    const a = b.dataset.cmDo, item = b.dataset.item;
    if (P.el.more.open && P.el.menu.contains(b)) P.el.more.open = false;
    const s = P.eng;
    const act = async (op, body) => { b.disabled = true; try { const d = await call(op, body); if (d && d.id) received(d); } catch (err) { if (!authLost(err)) say(err.message, "bad"); } finally { b.disabled = false; } };
    switch (a) {
      case "reload": P.err = null; load(P); break;
      case "pull": pullAll(P); break;
      case "only": P.view = "question"; P.stick = true; paintHist(P); paintThread(P, P.eng); break;
      case "showall": P.view = "all"; P.stick = true; paintHist(P); paintThread(P, P.eng); if (P.lang) translateAll(P); break;
      case "recount": histCount(P.rid, P.engId, true); break;
      case "earlier": refreshEng(P, true); break;
      case "undo": if (P.undo) { P.el.input.value = P.undo.from; P.undo = null; P.grow(); setDraft(draftKey(P), P.el.input.value); paintUndo(P); } break;
      case "new": P.fresh = true; P.eng = null; P.earlier = null; P.stick = true; input0(P); paintPane(P); P.el.input.focus(); break;
      case "resolve": if (s) await act("resolve", { engagementId: s.id }); break;
      case "reopen": if (s) await act("reopen", { engagementId: s.id }); break;
      case "retry": if (s) await act("retry", { engagementId: s.id, itemId: item }); break;
      case "cancel": if (s) await act("cancel", { engagementId: s.id, itemId: item }); break;
      case "sentByHand": if (s) await act("sent", { engagementId: s.id, itemId: item }); break;
      case "copy": {
        const m = s && (s.messages || []).find(x => x.itemId === item);
        if (m) { try { await navigator.clipboard.writeText(plain(m.text)); say("Copied — paste it into Etsy's message to the buyer", "ok", 4000); } catch (_) { say("Could not copy: select the text and copy it yourself", "bad"); } }
        window.open("https://www.etsy.com/your/orders/sold?order_id=" + encodeURIComponent(P.rid), "_blank", "noopener");
        if (s) call("copied", { engagementId: s.id, itemId: item }).then(d => d && received(d)).catch(() => {});
        break;
      }
      case "edit": case "discard": {
        const x = pendingOut().find(y => y.id === item);
        dropOut(item);
        if (a === "edit" && x) { P.el.input.value = x.body.text; P.grow(); setDraft(draftKey(P), x.body.text); P.el.input.focus(); }
        paintPane(P); break;
      }
      case "link": {
        const url = prompt("Paste the address of the customer's Etsy conversation (etsy.com/your/conversations/…):");
        if (url && s) await act("link_url", { engagementId: s.id, url });
        break;
      }
      case "simulate": {
        const text = prompt("What does the customer answer? (sandbox only)", "Yes, that's right — thank you!");
        if (text && s) await act("simulate", { engagementId: s.id, text });
        break;
      }
      case "disconnect": disconnect(); break;
    }
  }

  async function translateComposer(P, lang) {
    const from = P.el.input.value;
    if (!from.trim()) { P.el.input.focus(); return; }
    P.el.comp.classList.add("busy");
    try {
      const got = await translateTexts([from], lang);
      const v = got.get(plain(from).trim());
      if (!v) throw new Error("no translation came back");
      if (v.same) { say(`It is already in ${LANG[lang]}`, "", 2500); return; }
      if (P.el.input.value !== from) return;             // the person kept typing: leave their words alone
      P.el.input.value = v.text; P.grow(); setDraft(draftKey(P), v.text);
      P.undo = { from, to: v.text, lang }; paintUndo(P);
    } catch (e) { say("Translation: " + e.message, "bad"); }
    finally { P.el.comp.classList.remove("busy"); }
  }

  function send(P) {
    const text = P.el.input.value.trim();
    if (!text) return;
    if (!M.key) { connect(); return; }
    const s = P.eng;
    const clientId = uidOf();
    const body = {
      receiptId: P.rid, text, clientId, sandbox: sbOf(P.rid),
      scope: P.fresh ? "order" : (s ? s.scope : P.ctx.scope) || "order",
      lineId: P.fresh ? null : (s ? s.lineId : P.ctx.lineId) || null,
      engagementId: P.fresh ? null : (s ? s.id : null), newQuestion: !!P.fresh,
      lineLabel: P.ctx.lineLabel || "", orderNumber: String(P.rid), buyerName: P.ctx.buyerName || ""
    };
    P.waitingFor = clientId;
    setDraft(draftKey(P), "");
    P.el.input.value = ""; P.grow(); P.undo = null; paintUndo(P); P.stick = true;
    queueOut(body);
    paintPane(P);
  }

  // ─── the buyer's whole history: counted first, pulled on request ─────────
  /* "Pull all messages" reads every message this buyer and the shop ever wrote, from the inbox's own copy of each of
     their conversations (never from Etsy). The count shows before anything is pulled; pulling shows how far along it is;
     afterwards the pane can switch between the whole history and just this question. Counts are shared by every pane
     and engraving card of the same order for five minutes. */
  const HCOUNT_TTL = 5 * 60000;
  const hcount = new Map();   // receipt → { at, info, busy, err }
  const showingAll = P => P.view === "all" && !!P.hist && P.hist.done && P.hist.rid === P.rid && !P.fresh;
  const histWanted = P => !!M.key && !sbOf(P.rid) && !!P.rid && !P.fresh && !!((P.eng && P.eng.threadId) || (P.data && P.data.conversation));
  async function histCount(rid, engId, force) {
    rid = String(rid || "");
    const c = hcount.get(rid);
    if (!rid || !M.key || sbOf(rid) || (c && (c.busy || (!force && Date.now() - c.at < HCOUNT_TTL)))) return c || null;
    const cur = { at: Date.now(), info: (c && c.info) || null, busy: true, err: null };
    hcount.set(rid, cur);
    if (hcount.size > 100) hcount.delete(hcount.keys().next().value);
    paintHistFor(rid);
    try { cur.info = await call("history_info", { receiptId: rid, engagementId: engId || null, sandbox: sbOf(rid) }, { timeout: 20000 }); cur.at = Date.now(); }
    catch (e) { if (!authLost(e)) cur.err = e.message; cur.at = Date.now() - HCOUNT_TTL + 30000; }
    finally { cur.busy = false; paintHistFor(rid); }
    return cur;
  }
  function paintHistFor(rid) {
    for (const P of M.panes) if (String(P.rid) === rid && P.host.isConnected) paintHist(P);
    for (const L of M.lines.values()) if (L.rid === rid && L.node.isConnected) paintLinePull(L);
  }
  const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;
  function paintHist(P) {
    const el = P.el.hist;
    if (!histWanted(P)) { el.hidden = true; return; }
    const c = hcount.get(String(P.rid));
    if (!c || (!c.busy && Date.now() - c.at > HCOUNT_TTL)) setTimeout(() => histCount(P.rid, P.engId), 0);
    const H = P.hist && P.hist.rid === P.rid ? P.hist : null;
    const whose = P.rid === TEST ? "the test account's" : "this buyer's";
    let v;
    if (H && H.busy) {
      const got = Math.min(H.got, H.total || H.got), pct = H.total ? Math.round(got / H.total * 100) : 0;
      v = `<span class="cmHistT"><span class="cmSpin" aria-hidden="true"></span>Pulling all messages… ${got} of ${H.total}</span><span class="cmBar" role="progressbar" aria-label="Pulling all messages" aria-valuemin="0" aria-valuemax="${H.total}" aria-valuenow="${got}"><i style="width:${pct}%"></i></span>`;
    } else if (H && H.err) v = `<span class="cmHistT bad">Could not pull all the messages: ${E(H.err)}</span><button type="button" class="lnk" data-cm-do="pull">Try again</button>`;
    else if (H && H.done) v = showingAll(P)
      ? `<span class="cmHistT">All <b>${plural(H.rows.length, "message", "messages")}</b> with ${P.rid === TEST ? "the test account" : "this buyer"}</span><button type="button" class="lnk" data-cm-do="only">Only this question</button>`
      : `<span class="cmHistT">Only this question</span><button type="button" class="lnk" data-cm-do="showall">Show all ${H.rows.length}</button>`;
    else if (!c || (c.busy && !c.info)) v = `<span class="cmHistT soft"><span class="cmSpin" aria-hidden="true"></span>Counting ${whose} messages…</span>`;
    else if (!c.info) v = `<span class="cmHistT soft">Could not count ${whose} messages.</span><button type="button" class="lnk" data-cm-do="recount">Try again</button>`;
    else if (!c.info.total) { el.hidden = true; return; }
    else v = `<span class="cmHistT">${P.rid === TEST ? "The test account's" : "This buyer's"} full history: <b>${plural(c.info.total, "message", "messages")}</b>${c.info.threads.length > 1 ? ` in ${c.info.threads.length} conversations` : ""}</span><button type="button" class="cmPull" data-cm-do="pull">Pull all messages</button>`;
    if (el.dataset.v !== v) { el.dataset.v = v; el.innerHTML = v; }
    el.hidden = false;
  }
  /** Every message of every conversation of this buyer, a page at a time, then the whole history on screen. */
  async function pullAll(P) {
    if (!histWanted(P) || (P.hist && P.hist.busy && P.hist.rid === P.rid)) return;
    const rid = String(P.rid), seq = ++P.hseq;
    let c = hcount.get(rid);
    P.hist = { rid, rows: [], got: 0, total: (c && c.info && c.info.total) || 0, busy: true, err: null, done: false };
    paintHist(P);
    try {
      if (!c || !c.info || Date.now() - c.at > 60000) c = await histCount(rid, P.engId, true);
      if (seq !== P.hseq) return;
      if (!c || !c.info) throw new Error((c && c.err) || "the count did not come back");
      P.hist.total = c.info.total;
      const threads = c.info.threads.slice().sort((a, b) => a.lastAtMs - b.lastAtMs);
      for (const t of threads) {
        let after = null;
        do {
          const r = await call("history", { receiptId: rid, threadId: t.threadId, engagementId: P.engId || null, after, limit: 200 }, { timeout: 25000 });
          if (seq !== P.hseq) return;
          P.hist.rows.push(...(r.messages || []));
          P.hist.got += r.read || (r.messages || []).length;
          after = r.next || null;
          paintHist(P);
        } while (after);
      }
      P.hist.busy = false; P.hist.done = true; P.view = "all"; P.stick = true;
    } catch (e) {
      if (seq !== P.hseq) return;
      if (authLost(e)) return;
      P.hist.busy = false; P.hist.err = e.message;
    }
    paintHist(P); paintThread(P, P.eng);
    if (showingAll(P) && P.lang) translateAll(P);
  }
  const dayOf = ms => ms ? new Date(ms).toLocaleDateString([], { month: "short", day: "numeric", year: "numeric" }) : "";
  /** The whole history: one block per conversation, oldest first, this question's messages in theirs. */
  function allHtml(P) {
    const eng = P.eng, q = (eng && eng.threadId) || (P.data && P.data.conversation && P.data.conversation.threadId) || "_";
    const groups = new Map();
    const add = (k, r) => { if (!groups.has(k)) groups.set(k, new Map()); groups.get(k).set(r.id, r); };
    for (const r of P.hist.rows) add(r.threadId || q, r);
    const ours = [];
    for (const m of (eng && eng.messages) || []) { if (m.side === "us") ours.push(m); else add(q, m); }
    // a sorter message that reached Etsy shows once: as ours, with its state, not again as the inbox's copy
    for (const m of ours) if (m.msgId) for (const g of groups.values()) g.delete(m.msgId);
    for (const m of ours.concat(localRows(P))) add(q, m);
    const info = (hcount.get(String(P.rid)) || {}).info;
    const list = [...groups.entries()].map(([k, g]) => ({ k, rows: [...g.values()].sort((a, b) => a.atMs - b.atMs) }))
      .filter(g => g.rows.length).sort((a, b) => a.rows[0].atMs - b.rows[0].atMs);
    const start = eng ? (eng.startedAtMs || eng.createdAtMs || 0) : 0;
    const out = [];
    for (const g of list) {
      if (list.length > 1) {
        const t = info && info.threads.find(x => x.threadId === g.k);
        out.push(`<div class="cmSep conv"><span>${g.k === q ? "This conversation" : "Another conversation"}${t && t.orderId ? " · order " + E(t.orderId) : ""} · since ${E(dayOf(g.rows[0].atMs))}</span></div>`);
      }
      let marked = !start || g.k !== q;
      for (const r of g.rows) {
        if (!marked && r.atMs >= start - 120000) { out.push(`<div class="cmSep"><span>Question asked · ${E(when(start))}</span></div>`); marked = true; }
        out.push(msgHtml(P, Object.assign({}, r, { old: g.k !== q || (start && r.atMs < start - 120000) })));
      }
    }
    return out.join("") || `<div class="cmEmpty soft">No messages with this buyer in the inbox yet.</div>`;
  }

  // ─── the order window: a Customer tab beside the team's chat ────────────
  const OW = { dlg: null, P: null, tab: get(LS.tab, "team"), rowKey: null };
  function orderWindow(dlg) {
    if (OW.dlg || !dlg) return;
    OW.dlg = dlg;
    const host = dlg.querySelector("#owPaneCust"); if (!host) return;
    OW.P = Pane(host, { visible: () => dlg.open && OW.tab === "customer" });
    dlg.querySelectorAll("[data-ow-tab]").forEach(b => b.addEventListener("click", () => setTab(b.dataset.owTab, true)));
    dlg.querySelector(".owTabs")?.addEventListener("keydown", e => {
      if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
      e.preventDefault(); e.stopPropagation(); setTab(OW.tab === "customer" ? "team" : "customer", true);
      dlg.querySelector(`[data-ow-tab="${OW.tab}"]`)?.focus();
    });
    setTab(OW.tab, false);
  }
  function setTab(tab, byHand) {
    OW.tab = tab === "customer" ? "customer" : "team";
    if (byHand) put(LS.tab, OW.tab);
    const d = OW.dlg; if (!d) return;
    d.querySelectorAll("[data-ow-tab]").forEach(b => { const on = b.dataset.owTab === OW.tab; b.setAttribute("aria-selected", on ? "true" : "false"); b.tabIndex = on ? 0 : -1; });
    const cust = d.querySelector("#owPaneCust"), team = d.querySelector("#owPaneTeam");
    if (cust) cust.hidden = OW.tab !== "customer";
    if (team) team.hidden = OW.tab !== "team";
    d.classList.toggle("owCustOn", OW.tab === "customer");
    if (OW.tab === "customer" && OW.P) { paintPane(OW.P); markRead(OW.P); if (M.key) kick(300); healthSoon(); }
    paintTabDots();
  }
  function paintTabDots() {
    const d = OW.dlg; if (!d || !OW.P || !OW.P.rid) return;
    const b = badgeState(OW.P.rid);
    const dot = d.querySelector('[data-ow-tab="customer"] .owDot');
    if (dot) { dot.hidden = !(b && (b.unread || b.bad)); dot.classList.toggle("bad", !!(b && !b.unread && b.bad)); }
  }
  /** The order window shows a line: the Customer tab follows its order. */
  function orderShown(row, opts = {}) {
    if (!OW.P || !row) return;
    const rid = String(row.order.receiptId);
    const lineIds = ((window.Orders && Orders.rows && Orders.rows()) || []).filter(r => String(r.order.receiptId) === rid);
    const at = lineIds.findIndex(r => r.key === row.key);
    const ctx = {
      receiptId: rid, scope: "order", lineId: row.line && row.line.transactionId != null ? String(row.line.transactionId) : null,
      lineLabel: (row.spec && row.spec.designSku || row.line.sku || "") + (lineIds.length > 1 ? ` · line ${at + 1} of ${lineIds.length}` : ""),
      buyerName: row.order.buyerName || row.order.name || ""
    };
    OW.rowKey = row.key;
    show(OW.P, ctx, { engagementId: opts.engagementId || null });
    if (opts.pull) pullWhenReady(OW.P);
    const b = badgeState(rid);
    if (opts.tab) setTab(opts.tab, false);
    else if (b && (b.unread || b.bad)) setTab("customer", false);
    else setTab(get(LS.tab, "team"), false);
    paintTabDots();
  }
  function orderClosed() { if (OW.P) { OW.P.seq++; } }

  // ─── a window of its own, for an order that is no longer in the pull ────
  let SA = null;
  function standalone(s, extra = {}) {
    if (!SA) {
      const d = h("dialog", "owin cmDlg"); d.setAttribute("aria-label", "Customer conversation");
      d.innerHTML = `<div class="owBox"><div class="owHead"><h3>Customer</h3><span class="spacer"></span><button type="button" class="btn ghost sm" data-x>✕ Close</button></div><div class="cmSolo"></div></div>`;
      document.body.appendChild(d);
      d.querySelector("[data-x]").onclick = () => d.close();
      SA = { d, P: Pane(d.querySelector(".cmSolo"), { visible: () => d.open }) };
    }
    SA.d.querySelector("h3").textContent = String(s.receiptId) === TEST ? "Email link test" : "Order " + s.receiptId;
    if (!SA.d.open) { try { SA.d.showModal(); } catch (_) { SA.d.setAttribute("open", ""); } }
    show(SA.P, { receiptId: s.receiptId, scope: s.scope, lineId: s.lineId, lineLabel: s.lineLabel || "" }, { engagementId: s.id || null });
    if (extra.pull) pullWhenReady(SA.P);
  }

  // ─── the engraving card's customer box ──────────────────────────────────
  /* The same node follows its placement through every rebuild of the card, so what was typed, and the caret, stay put. */
  let lastBlur = null, healthSoonQueued = false;
  function lineBox(job) {
    if (!job || !job.row) return null;
    const row = job.row, key = String(job.key);
    let L = M.lines.get(key);
    if (!L) {
      const node = h("div", "cmLineBox");
      node.innerHTML = `<div class="cmLH"><span class="cmMark">${ICON.mail}</span><b>Customer</b><span class="cmLS" data-l="sub"></span><span class="grow"></span><button type="button" class="lnk" data-l="open">Conversation ${ICON.out}</button></div>
        <div class="cmLast" data-l="last" hidden></div>
        <div class="cmLine" data-l="comp"><textarea rows="1" data-l="input" aria-label="Question to the customer"></textarea><button type="button" class="cmSend" data-l="send" title="Send to the customer (Enter)" aria-label="Send to the customer" disabled>${ICON.send}</button></div>
        <div class="cmLFoot" data-l="foot" hidden><span class="cmLive" data-l="live" hidden></span><span class="cmLWarn" data-l="warn" hidden></span><button type="button" class="lnk cmLPull" data-l="pull" data-l-do="pull" title="Every message with this buyer, from the inbox" hidden></button></div>
        <div class="cmHBox" data-l="hbox" role="dialog" aria-label="Email link" hidden></div>
        <div class="cmLNote" data-l="note" hidden></div>`;
      L = { key, node, row, job, at: Date.now() };
      const q = n => node.querySelector(`[data-l="${n}"]`);
      L.el = { sub: q("sub"), open: q("open"), last: q("last"), comp: q("comp"), input: q("input"), send: q("send"), note: q("note"), live: q("live"), warn: q("warn"), hbox: q("hbox"), foot: q("foot"), pull: q("pull") };
      const input = L.el.input;
      const grow = () => { input.style.height = "auto"; const hh = input.scrollHeight; input.style.height = hh ? Math.min(120, hh + 2) + "px" : ""; L.el.send.disabled = !input.value.trim(); };
      L.grow = grow;
      input.addEventListener("input", () => { grow(); setDraft(lineDraftKey(L), input.value); });
      input.addEventListener("keydown", e => { e.stopPropagation(); if (e.key === "Enter" && !e.shiftKey && !e.isComposing) { e.preventDefault(); lineSend(L); } });
      input.addEventListener("focus", () => { L.focus = true; });
      // a blur with nowhere to go is a rebuild taking the box away, not the person leaving it
      input.addEventListener("blur", e => { L.focus = false; lastBlur = e.relatedTarget ? null : { key, at: Date.now(), start: input.selectionStart, end: input.selectionEnd }; });
      L.el.send.onclick = () => lineSend(L);
      L.el.open.onclick = () => { const s = lineEng(L); openConversation(s || { receiptId: L.row.order.receiptId, scope: "engraving", lineId: lineIdOf(L.row) }, L.row.key); };
      node.addEventListener("click", e => {
        if (e.target.closest("[data-health]")) { toggleHealth(L.el.hbox, node); return; }
        const c = e.target.closest("[data-l-do]"); if (!c) return;
        if (c.dataset.lDo === "connect") connect();
        else if (c.dataset.lDo === "inbox" && M.pair) openInbox(M.pair.url);
        else if (c.dataset.lDo === "tr") lineTranslate(L, c.dataset.lang);
        else if (c.dataset.lDo === "pull") { const s = lineEng(L); openConversation(s || { receiptId: L.rid, scope: "engraving", lineId: lineIdOf(L.row) }, L.row.key, { pull: true }); }
      });
      M.lines.set(key, L);
      input.value = draftOf(lineDraftKey(L)); grow();
      node.dataset.key = key;
      if (lineSeen) lineSeen.observe(node);
    }
    // a card coming on screen checks the line if a check is due (once, however many cards are drawn)
    if (!healthSoonQueued) { healthSoonQueued = true; setTimeout(() => { healthSoonQueued = false; healthSoon(); }, 0); }
    L.row = row; L.job = job; L.rid = String(row.order.receiptId); L.at = Date.now();
    paintLine(L);
    if (L.el.input.value) setTimeout(L.grow, 0);
    // a rebuild of the card took the focus from the box someone was typing in: it goes back, caret and all, once the new
    // card is on the page (browsers that say nothing when a focused box is taken away leave it marked as focused)
    const b = lastBlur && lastBlur.key === key && Date.now() - lastBlur.at < 400 ? lastBlur
      : L.focus && document.activeElement !== L.el.input ? { start: L.el.input.selectionStart, end: L.el.input.selectionEnd } : null;
    if (b) setTimeout(() => {
      lastBlur = null;
      const a = document.activeElement;
      if (L.node.isConnected && a !== L.el.input && (!a || a === document.body)) { L.el.input.focus({ preventScroll: true }); try { L.el.input.setSelectionRange(b.start, b.end); } catch (_) {} }
    }, 0);
    return L.node;
  }
  const lineIdOf = row => row.line && row.line.transactionId != null ? String(row.line.transactionId) : null;
  const lineDraftKey = L => `${L.row.order.receiptId}:l${lineIdOf(L.row) || ""}`;
  function lineEng(L) {
    const lid = lineIdOf(L.row);
    const list = forReceipt(L.row.order.receiptId).filter(s => s.scope === "engraving" && String(s.lineId || "") === String(lid || ""));
    return list.find(s => s.status === "open") || list[0] || null;
  }
  function suggestion(L) {
    const qs = ((L.job && L.job.questions) || []).filter(Boolean);
    return qs.length ? "e.g. " + qs[0] : "Ask about this engraving…";
  }
  function paintLine(L) {
    const e = L.el;
    if (!M.key) {
      e.sub.textContent = "";
      e.last.hidden = false;
      e.last.innerHTML = M.pair
        ? `<span class="soft">Approve the connection in the inbox tab.</span> <button type="button" class="lnk" data-l-do="inbox">Open the inbox</button>`
        : `<span class="soft">Ask this customer about the engraving without leaving the card.</span> <button type="button" class="lnk" data-l-do="connect">Connect to the inbox</button>`;
      e.comp.hidden = true; e.note.hidden = true; e.open.hidden = true;
      paintLight(e, L.node, "cmLWarn");
      return;
    }
    e.comp.hidden = false; e.open.hidden = false;
    const s = lineEng(L);
    const cust = s && s.customer && (firstName(s.customer.name) || s.customer.username);
    e.sub.textContent = s ? `${cust ? cust + " · " : ""}${s.status === "resolved" ? "answered" : s.unread > 0 ? `${s.unread} new ${s.unread === 1 ? "reply" : "replies"}` : s.lastInboundAtMs ? "replied " + when(s.lastInboundAtMs) : "asked " + when(s.startedAtMs || s.createdAtMs)}` : "";
    L.node.classList.toggle("new", !!(s && s.unread > 0));
    if (s && s.lastInboundAtMs) {
      e.last.hidden = false;
      const text = plain(s.lastInboundPreview || "Sent a photo");
      const tl = L.tr && trCached(text, L.tr);
      e.last.innerHTML = `<div class="cmMsg cust mini"><div class="cmMeta"><b>${E(s.lastInboundBy || cust || "Customer")}</b><span>${E(when(s.lastInboundAtMs))}</span><span class="cmTrB"><button type="button" data-l-do="tr" data-lang="en" class="${L.tr === "en" ? "on" : ""}">EN</button><button type="button" data-l-do="tr" data-lang="uk" class="${L.tr === "uk" ? "on" : ""}">УКР</button></span></div><div class="cmBody">${html(text)}</div>${tl && !tl.same ? `<div class="cmTr"><span class="cmTrLbl">${E(LANG[L.tr])}</span>${html(tl.text)}</div>` : ""}</div>`;
    } else e.last.hidden = true;
    const out = s && s.lastOut;
    const local = pendingOut().find(x => String(x.body.receiptId) === L.rid && x.body.scope === "engraving" && String(x.body.lineId || "") === String(lineIdOf(L.row) || ""));
    const note = local ? (local.error ? `<span class="bad">Not sent: ${E(local.error)}</span>` : "Sending…")
      : out && out.status === "failed" ? `<span class="bad">The last message did not go.</span> Open the conversation to try again.`
      : out && out.status === "manual" ? `<span class="warn">No Etsy conversation with this buyer yet:</span> open the conversation to send it by hand.`
      : out && (out.status === "new" || out.status === "queued" || out.status === "sending" || out.status === "waiting") ? "On its way to the customer…"
      : out && out.status === "sent" && !s.lastInboundAtMs ? "Sent " + when(out.sentAtMs || out.atMs) + " · waiting for the answer" : "";
    e.note.hidden = !note; e.note.innerHTML = note;
    e.input.placeholder = s && s.status === "open" ? "Reply…" : suggestion(L);
    paintLight(e, L.node, "cmLWarn");
    if (!lineSeen) histCount(L.rid, s && s.id);
    paintLinePull(L);
  }
  /** The engraving card's way into the whole history: the count once it is known, then one press to pull it all. */
  function paintLinePull(L) {
    const b = L.el.pull;
    const c = hcount.get(L.rid), n = c && c.info ? c.info.total : null;
    b.hidden = !M.key || SANDBOX || !(n > 0 || (c && c.busy && n == null));
    const v = n == null ? `<span class="cmSpin" aria-hidden="true"></span>Counting messages…` : `Pull all messages · ${n}`;
    if (b.dataset.v !== v) { b.dataset.v = v; b.innerHTML = v; }
  }
  // an engraving card that comes into view counts its buyer's messages (cards scrolled past cost nothing)
  const lineSeen = "IntersectionObserver" in window ? new IntersectionObserver(entries => {
    for (const en of entries) {
      if (!en.isIntersecting) continue;
      const L = M.lines.get(en.target.dataset.key);
      if (L && M.key && !SANDBOX) histCount(L.rid, (lineEng(L) || {}).id);
    }
  }) : null;
  async function lineTranslate(L, lang) {
    L.tr = L.tr === lang ? null : lang;
    paintLine(L);
    const s = lineEng(L);
    if (L.tr && s && s.lastInboundPreview && !trCached(plain(s.lastInboundPreview), L.tr)) {
      try { await translateTexts([s.lastInboundPreview], L.tr); } catch (e) { say("Translation: " + e.message, "bad"); L.tr = null; }
      paintLine(L);
    }
  }
  function lineSend(L) {
    const text = L.el.input.value.trim(); if (!text) return;
    if (!M.key) { connect(); return; }
    const row = L.row, s = lineEng(L);
    const sibs = ((window.Orders && Orders.rows && Orders.rows()) || []).filter(r => String(r.order.receiptId) === L.rid);
    const at = sibs.findIndex(r => r.key === row.key);
    queueOut({
      receiptId: L.rid, text, clientId: uidOf(), sandbox: SANDBOX, scope: "engraving", lineId: lineIdOf(row),
      engagementId: s && s.status === "open" ? s.id : null,
      lineLabel: (row.spec && row.spec.designSku || row.line.sku || "") + (sibs.length > 1 ? ` · line ${at + 1} of ${sibs.length}` : ""),
      orderNumber: L.rid, buyerName: row.order.buyerName || row.order.name || ""
    });
    L.el.input.value = ""; L.grow(); setDraft(lineDraftKey(L), "");
    paintLine(L);
  }

  // ─── the team's chat: any message, translated with a click ──────────────
  /* Internal messages get the same two buttons. They are drawn by the order window; the translations live here. */
  const teamTr = new Map();   // message text → language shown
  function teamButtons(text) {
    if (!String(text || "").trim()) return "";
    const on = teamTr.get(plain(text).trim());
    return `<span class="cmTrB team" role="group" aria-label="Translate this message"><button type="button" data-team-tr="en" class="${on === "en" ? "on" : ""}" title="Translate into English">EN</button><button type="button" data-team-tr="uk" class="${on === "uk" ? "on" : ""}" title="Translate into Ukrainian">УКР</button></span>`;
  }
  function teamTranslation(text) {
    const t = plain(text).trim(), lang = teamTr.get(t);
    if (!lang) return "";
    const v = trCached(t, lang);
    return v ? (v.same ? "" : `<div class="cmTr team"><span class="cmTrLbl">${E(LANG[lang])}</span>${html(v.text)}</div>`) : `<div class="cmTr team soft">Translating…</div>`;
  }
  document.addEventListener("click", async e => {
    const b = e.target.closest("[data-team-tr]"); if (!b) return;
    const msg = b.closest("[data-team-text]"); if (!msg) return;
    e.stopPropagation();
    if (!M.key) { say("Connect the sorter to the inbox first (Customer tab) to translate", "", 5000); return; }
    const t = plain(msg.dataset.teamText || "").trim(), lang = b.dataset.teamTr;
    teamTr.set(t, teamTr.get(t) === lang ? null : lang);
    if (window.OrderWin && OrderWin.repaintThread) OrderWin.repaintThread();
    if (teamTr.get(t) && !trCached(t, lang)) {
      try { await translateTexts([t], lang); } catch (err) { say("Translation: " + err.message, "bad"); teamTr.delete(t); }
      if (window.OrderWin && OrderWin.repaintThread) OrderWin.repaintThread();
    }
  });

  // ─── start ───────────────────────────────────────────────────────────────
  function boot() {
    paintPill();
    if (M.pair) { if (M.key) cancelPair(); else pollPair(); }
    if (M.key) { kick(200); flushOut(); }
    // a message left in the outbox by a closed tab goes now
    setInterval(() => { if (M.key && pendingOut().some(x => !x.error)) flushOut(); }, 30000);
    // the times on screen ("4 min ago") stay true
    setInterval(() => { for (const P of M.panes) if (P.visible() && !document.hidden) paintThread(P, P.eng); for (const L of M.lines.values()) if (L.node.isConnected) paintLine(L); }, 60000);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else setTimeout(boot, 0);

  window.CustomerMail = {
    orderWindow, orderShown, orderClosed, setTab, lineBox, badge, badgeStamp, teamButtons, teamTranslation,
    openConversation, connect, connected: () => !!M.key,
    _state: M
  };
})();
