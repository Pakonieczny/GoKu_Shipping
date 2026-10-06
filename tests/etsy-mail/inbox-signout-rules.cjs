// The inbox (etsy-mail-1.html, station "inbox") obeys the same sign-out rules as every station (stations round 2, R9):
// 10 minutes without input signs a non-Admin operator out, 17:00 America/Toronto signs them out unless there was input in the
// last 10 minutes, Admin is exempt. Whatever the reason, the inbox's own sign-in screen comes up with one calm line and NOTHING
// is lost: the open conversation, the reply being typed, an AI draft being written, a reply on its way, the background work.
//
//   1 · idle sign-out keeps the draft: it is still in the box under the sign-in screen, and back when the same person signs in
//   2 · 5 pm with and without input (and a page that slept through 5 pm): who is signed out, who stays, which line is shown
//   3 · Admin is never signed out (10 minutes, an hour, across 5 pm)
//   4 · input counts (a key, a click on a conversation, the wheel over the list, the Polish and AI Draft buttons) and polling,
//       the AI's words arriving and code that clicks for itself do not
//   5 · a reply on its way when its person is signed out finishes under that person (and an AI draft still lands in its own
//       conversation); nothing is recorded under the person who signs in next
//   6 · a second person signing in sees none of the first person's unsent words and cannot send them under their own name;
//       the first person finds them again when they sign in
//   7 · the session is written under the display name, never a PIN-like username
//
// StationSession: the timers (idle, closing, Admin) are worker AD1's, in station-session.js. Until the real module has them this
// file runs the page against a small stand-in that follows the plan's contract (R5 R6 C2); with SS=real it uses the real file.
// The stand-in is only a clock: the page code under test is the page's.
//
//   PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   [SHOTS_DIR=/path/to/screens] [SS=real|standin] node tests/etsy-mail/inbox-signout-rules.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no AI, nothing sent); every other request is aborted.
// The clock is Playwright's fake clock; no PIN or passcode appears anywhere (the fake passwords are "pw-<username>").
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));
const SHOTS = process.env.SHOTS_DIR || "";

const REAL_FILE = path.join(root, "station-session.js");
const realHasRules = /["']closing["']/.test(fs.readFileSync(REAL_FILE, "utf8")) && /["']idle["']/.test(fs.readFileSync(REAL_FILE, "utf8"));
const USE_REAL = process.env.SS === "real" || (process.env.SS !== "standin" && realHasRules);

/* ── the stand-in for station-session.js (this file only): the plan's contract, nothing more ── */
const STAND_IN = `(function () {
  var IDLE = 10 * 60000, TICK = 30000, TZ = "America/Toronto";
  var calls = [], o = null, cur = null, lastIn = 0, quiet = "", lastTick = 0;
  window.__ss = { calls: calls };
  var fmt = new Intl.DateTimeFormat("en-CA", { timeZone: TZ, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" });
  function parts(t) { var r = {}; fmt.formatToParts(new Date(t)).forEach(function (p) { r[p.type] = p.value; }); return r; }
  function closingAt(t) {   // 17:00 Toronto on the Toronto day of t
    var p = parts(t), off = Date.UTC(+p.year, +p.month - 1, +p.day, +p.hour, +p.minute, +p.second) - Math.floor(t / 1000) * 1000;
    return Date.UTC(+p.year, +p.month - 1, +p.day, 17, 0, 0) - off;
  }
  var admin = function (n) { return (window.__admins || []).some(function (a) { return String(a).toLowerCase() === String(n).toLowerCase(); }); };
  var log = function (k, v) { calls.push([k, v === undefined ? null : JSON.parse(JSON.stringify(v)), Date.now()]); };
  function begin(p) { cur = { id: "inbox-FIX1-" + Date.now().toString(36), name: p.name, eid: p.id || "", startAt: Date.now() }; lastIn = Math.max(lastIn, Date.now()); log("start", { name: p.name, id: p.id || "" }); }
  function end(reason) { if (!cur) return; log("end", { name: cur.name, reason: reason, at: Date.now(), lastInputAt: lastIn }); cur = null; }
  function tick() {
    try {
      var now = Date.now(), t0 = lastTick || now; lastTick = now;
      var p = o && o.person ? o.person() : null;
      if (!cur) { if (p && p.name !== quiet) begin(p); else if (!p) quiet = ""; return; }
      if (!p) { end("signOut"); quiet = ""; return; }
      if (p.name !== cur.name) { end("switched"); begin(p); return; }
      if (admin(cur.name)) return;                                         // Admin: exempt from idle and closing
      var idle = now - lastIn >= IDLE, crossed = t0 < closingAt(now) && closingAt(now) <= now;
      if (idle) { var reason = crossed ? "closing" : "idle"; quiet = cur.name; end(reason); o.signOut(reason); }
    } catch (e) { log("error", String(e)); }
  }
  window.StationSession = {
    init: function (opts) { o = opts; log("init", { station: opts.station, device: opts.device, person: opts.person() }); lastTick = Date.now(); tick(); setInterval(tick, TICK); },
    signedIn: function (p) { log("signedIn", p); quiet = ""; if (cur) end("switched"); begin(p); },
    signedOut: function (r) { log("signedOut", r); var p = o.person(); end(r || "signOut"); quiet = p ? p.name : ""; },
    touch: function (ts) { var t = Number(ts) || Date.now(); if (t > lastIn) lastIn = t; log("touch", t); },
    lastInput: function () { return lastIn; },
    who: function () { return cur ? { person: cur.name, station: "inbox", device: "etsy-mail-1", computer: "pc-FIXTURE1", session: cur.id, startAt: cur.startAt, sandbox: false } : null; },
    page: function () { return { station: "inbox", device: "etsy-mail-1", computer: "pc-FIXTURE1", sandbox: false }; },
    current: function () { return cur ? { id: cur.id, person: cur.name, startAt: cur.startAt } : null; }
  };
})();`;

/* ── the fixture: two customers, four operators (Admin is "Paul"), a fake server ── */
const NOW = Date.now();
const A = "t_soA", B = "t_soB";
const thread = (id, name, ago) => ({
  id, customerName: name, status: "etsy_scraped", unread: false,
  lastInboundAt: { _ts: true, ms: NOW - ago }, awaitingReplySince: { _ts: true, ms: NOW - ago },
  updatedAt: { _ts: true, ms: NOW - ago }, etsyConversationUrl: "https://example.invalid/c/" + id, etsyOrderId: id === A ? "3600000001" : "3600000002"
});
const THREADS = [thread(A, "Mia Fixture", 3600e3), thread(B, "Leo Fixture", 7200e3)];
const msgs = id => [0, 1].map(i => ({
  id: id + "m" + i, direction: i ? "outbound" : "inbound", senderName: i ? "CustomBrites" : "Customer",
  text: i ? "Thanks, we will check." : "When does my order ship?",
  timestamp: { _ts: true, ms: NOW - (9 - i) * 3600e3 }, createdAt: { _ts: true, ms: NOW - (9 - i) * 3600e3 }
}));
const USERS = {
  ann: { display: "Ann Operator", role: "operator" },
  ben: { display: "Ben Operator", role: "operator" },
  paul: { display: "Paul", role: "owner" },
  "4821": { display: "Dana Mail", role: "operator" },       // a username of digits only (could be a PIN), with a display name
  "5550123": { display: "", role: "operator" }              // digits only and no display name: nothing usable as a name
};
const whoIs = u => ({ ok: true, username: u, displayName: USERS[u].display || u, role: USERS[u].role });
let calls = [];
const holds = {};                       // kind -> { p, release }: a request the fake answers only when released
const hold = kind => { let release; const p = new Promise(r => { release = r; }); holds[kind] = { p, release }; return release; };
let draftA = null;
async function fake(method, url, body, headers) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  const token = headers["x-etsymail-session"] || null;
  calls.push({ name, method, q, b, token, at: Date.now() });
  if (name === "firestoreProxy") {
    if (q.op === "list" && q.coll === "EtsyMail_Threads") return { docs: THREADS };
    if (q.op === "listSub") return { docs: msgs(q.id) };
    if (q.op === "get" && q.coll === "EtsyMail_Drafts" && q.id === "draft_" + A && draftA) return { exists: true, doc: draftA };
    if (q.op === "get" && q.coll === "EtsyMail_Config" && q.id === "autoPipeline") return { exists: true, doc: { enabled: false, manualAiDraftAutoSend: false, threshold: 0.8 } };   // auto-send OFF
    if (q.op === "get") return { exists: false };
    return { ok: true };
  }
  if (name === "etsyMailAuth") {
    if (b.op === "login") {
      const us = USERS[b.username];
      if (!us || b.password !== "pw-" + b.username) { return { __status: 401, error: "bad", reason: "BAD_PASSWORD" }; }
      return Object.assign(whoIs(b.username), { sessionToken: "tok-" + b.username });
    }
    if (b.op === "currentUser") {
      const un = token && /^tok-(.+)$/.exec(token);
      return un && USERS[un[1]] ? whoIs(un[1]) : { __status: 401, error: "no", reason: "SESSION_NOT_FOUND" };
    }
    return { ok: true };
  }
  if (name === "etsyMailPolish") return { ok: true, text: "Hi Mia,\n\nYour order ships tomorrow.\n\nMany Thanks,\nCustomBrites" };
  if (name === "etsyMailDraftReply" && b.threadId === A) {
    if (holds.draft) { const h = holds.draft; holds.draft = null; await h.p; }
    const text = "AI reply for Mia: your ring ships Friday.";
    draftA = { threadId: A, status: "draft", generatedByAI: true, text, createdAt: { _ts: true, ms: Date.now() } };
    return { ok: true, text, aiConfidence: 0.5, autoSendBlockers: [] };
  }
  if (name === "etsyMailDraftSend" && method === "POST" && b.op === "enqueue") {
    if (holds.enqueue) { const h = holds.enqueue; holds.enqueue = null; await h.p; }
    return { ok: true, draftId: "draft_" + b.threadId, attachments: b.attachments || [], text: b.text };
  }
  if (name === "etsyMailDraftSend") return { ok: true, draft: { status: "queued" } };
  if (name === "firebaseOrders") return { ok: true };
  return { ok: true };
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    let body = "";
    req.on("data", d => { body += d; });
    req.on("end", async () => {
      const out = await fake(req.method, req.url, body, req.headers);
      const st = out && out.__status; if (st) delete out.__status;
      res.writeHead(st || 200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(out));
    });
    return;
  }
  if (u === "/station-session.js" && !USE_REAL) { res.writeHead(200, { "Content-Type": "text/javascript" }); return res.end(STAND_IN); }
  const f = u === "/etsy-mail-1.html" && process.env.INBOX_PAGE ? path.resolve(process.env.INBOX_PAGE) : path.join(root, u);
  if ((!f.startsWith(root) && f !== path.resolve(process.env.INBOX_PAGE || "-")) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  const type = /\.js$/.test(f) ? "text/javascript" : /\.css$/.test(f) ? "text/css" : /\.html$/.test(f) ? "text/html" : "application/octet-stream";
  res.writeHead(200, { "Content-Type": type });
  fs.createReadStream(f).pipe(res);
}).listen(0);

const wait = ms => new Promise(r => setTimeout(r, ms));
async function until(fn, what, ms = 12000) { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error("timed out waiting for " + what); await wait(80); } }
const tor = (day, hh, mm) => new Date(Date.parse(`2026-10-${day}T00:00:00Z`) + 4 * 3600e3 + (hh * 60 + mm) * 60e3);   // Toronto wall time (EDT, UTC-4)
const MORNING = tor("07", 10, 0);
const IDLE_MS = 10 * 60e3, TICK_PAD = 31e3;

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const fails = [];
  const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
  console.log("StationSession: " + (USE_REAL ? "the real station-session.js" : "the stand-in (plan contract) in this file"));
  const errorsAll = [];

  /** a page of the inbox on the fake clock, signed in as `as` (cached session) when given */
  async function open(time, as, admins) {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    await ctx.route(u => !/^http:\/\/127\.0\.0\.1[:/]/.test(u.href), r => r.abort());
    await ctx.addInitScript(({ as, admins }) => {
      window.__admins = admins || [];
      try {
        if (sessionStorage.getItem("__seeded")) return;
        sessionStorage.setItem("__seeded", "1");
        if (as) {
          localStorage.setItem("etsymail_session", "tok-" + as.username);
          localStorage.setItem("etsymail_session_profile", JSON.stringify({ username: as.username, displayName: as.display, role: as.role, cachedAtMs: Date.now() }));
        }
      } catch (_) {}
    }, { as: as ? { username: as, display: USERS[as].display, role: USERS[as].role } : null, admins: admins || [] });
    const page = await ctx.newPage();
    page.on("pageerror", e => errorsAll.push(String(e)));
    if (process.env.DEBUG) page.on("console", m => { if (/error|warn/.test(m.type())) console.log("    [page " + m.type() + "] " + m.text().slice(0, 200)); });
    page.on("dialog", d => d.dismiss().catch(() => {}));
    await page.clock.install({ time });
    await page.goto(`http://127.0.0.1:${server.address().port}/etsy-mail-1.html`);
    if (as) {
      await until(() => page.evaluate(() => document.body.classList.contains("authed")), "the inbox to open as " + as);
      await page.waitForSelector(`[data-id="${A}"]`, { timeout: 15000 });
    } else {
      await page.waitForSelector("#siUser", { state: "visible", timeout: 15000 });
    }
    return { ctx, page };
  }
  const authed = page => page.evaluate(() => document.body.classList.contains("authed"));
  const banner = page => page.evaluate(() => { const b = document.getElementById("siBanner"); return b && b.classList.contains("show") ? document.getElementById("siBannerText").textContent : ""; });
  const openThread = async (page, id) => {
    await page.click(`[data-id="${id}"]`);
    try {
      await page.waitForFunction(id => { const b = document.querySelector(`[data-id="${id}"]`); return b && (b.classList.contains("active") || b.classList.contains("selected")) && document.getElementById("emDraftText"); }, id, { timeout: 10000 });
    } catch (e) {
      console.log("    (diagnostic) " + JSON.stringify(await page.evaluate(id => { const b = document.querySelector(`[data-id="${id}"]`); return { cls: b && b.className, authed: document.body.classList.contains("authed"), hasBox: !!document.getElementById("emDraftText"), detail: (document.getElementById("emDetail") || {}).innerText && document.getElementById("emDetail").innerText.slice(0, 120) }; }, id)));
      throw e;
    }
    await page.waitForTimeout(500);
  };
  const box = page => page.evaluate(() => { const t = document.getElementById("emDraftText"); return t ? t.value : null; });
  const typeInBox = async (page, text) => { await page.click("#emDraftText"); await page.keyboard.type(text); };
  const selected = (page, id) => page.evaluate(id => { const b = document.querySelector(`[data-id="${id}"]`); return !!(b && (b.classList.contains("active") || b.classList.contains("selected"))); }, id);
  const ss = page => page.evaluate(() => (window.__ss && window.__ss.calls) || []);
  const ended = async page => (await ss(page)).filter(c => c[0] === "end").map(c => c[1]);
  const lastInput = page => page.evaluate(() => (window.StationSession && StationSession.lastInput && StationSession.lastInput()) || 0);
  const run = (page, ms) => page.clock.runFor(ms);
  const idleOut = async (page, what) => {
    await run(page, IDLE_MS + TICK_PAD);
    await until(async () => !(await authed(page)), what || "the idle sign-out");
  };
  const signIn = async (page, user) => {
    await page.fill("#siUser", user); await page.fill("#siPass", "pw-" + user);
    await page.click("#siBtn");
    await until(() => authed(page), "the sign-in as " + user);
    await page.waitForTimeout(400);
  };
  // (a screenshot of the fixture: the icon font is a web font the offline run cannot load, so its names are hidden; the gate fades in over 0.4 s)
  const shot = async (page, name) => {
    if (!SHOTS) return;
    try {
      fs.mkdirSync(SHOTS, { recursive: true });
      await page.waitForTimeout(900);
      const h = await page.addStyleTag({ content: ".material-icons{display:inline-block!important;width:1em!important;overflow:hidden!important;color:transparent!important;white-space:nowrap!important}" });
      await page.screenshot({ path: path.join(SHOTS, name + ".png") });
      await h.evaluate(el => el.remove());
    } catch (e) { console.log("  (screenshot failed: " + e.message + ")"); }
  };
  const enqueues = () => calls.filter(c => c.name === "etsyMailDraftSend" && c.method === "POST" && c.b && c.b.op === "enqueue");
  const activity = () => {   // every activity event the page's station-activity sent to the station door
    const out = [];
    for (const c of calls) if (c.name === "firebaseOrders" && c.b && Array.isArray(c.b.activity)) out.push(...c.b.activity);
    return out;
  };
  const NOTE_IDLE = "Signed out after 10 minutes without input. Sign in to pick up where you left off.";
  const NOTE_CLOSING = "Signed out at 5:00 pm. Sign in to pick up where you left off.";

  try {
    /* ═══ 1 · idle sign-out keeps the draft ═══ */
    console.log("\n1 · 10 minutes without input: signed out, nothing lost");
    {
      calls = []; draftA = null;
      const { ctx, page } = await open(MORNING, "ann");
      await openThread(page, A);
      const DRAFT = "Hi Mia, your ring is being engraved now and ships Friday";
      await typeInBox(page, DRAFT);
      check(await box(page) === DRAFT, "Ann's reply is in the box");
      await shot(page, "1-inbox-reply-typed");
      const pollsBefore = calls.filter(c => c.name === "firestoreProxy" && c.q.op === "list").length;
      await run(page, 9 * 60e3);
      check(await authed(page), "after 9 minutes without input she is still signed in");
      await idleOut(page);
      const b = await banner(page);
      check(b === NOTE_IDLE, "the sign-in screen says why in one calm line: " + JSON.stringify(b));
      check((await ended(page)).some(e => e.name === "Ann Operator" && e.reason === "idle"), "her session ended with reason idle");
      check(calls.some(c => c.name === "etsyMailAuth" && c.b.op === "logout" && c.token === "tok-ann"), "her server session was ended");
      check(await page.evaluate(() => !localStorage.getItem("etsymail_session") && !sessionStorage.getItem("etsymail_session") && !localStorage.getItem("etsymail_session_profile")), "her token and cached identity are gone from this browser");
      check(await box(page) === DRAFT, "her reply is still in the page, in its box, under the sign-in screen");
      check(await selected(page, A), "the conversation she had open is still selected");
      await shot(page, "2-sign-in-after-10-minutes");
      // background work carries on while the sign-in screen is up (polling is not input, and is not stopped)
      await run(page, 2 * 60e3);
      await wait(600);
      const pollsAfter = calls.filter(c => c.name === "firestoreProxy" && c.q.op === "list").length;
      check(pollsAfter > pollsBefore, "the conversation list keeps polling under the sign-in screen (" + (pollsAfter - pollsBefore) + " reads)");
      check(!enqueues().length, "nothing was sent");
      // the send button cannot send for nobody
      await page.evaluate(() => { const bt = document.getElementById("emSendEtsyBtn"); if (bt) bt.click(); });
      await wait(500);
      check(!enqueues().length, "with nobody signed in, pressing Send sends nothing");
      check(await box(page) === DRAFT, "and her words stay in the box");
      // the same person signs in again
      await signIn(page, "ann");
      check(await box(page) === DRAFT, "after Ann signs in again her reply is back in the box: " + JSON.stringify((await box(page) || "").slice(0, 30)));
      check(await selected(page, A), "on the same conversation");
      const starts = (await ss(page)).filter(c => c[0] === "signedIn" || c[0] === "start").map(c => c[1]);
      check(starts.some(s => s && s.name === "Ann Operator"), "a new session starts for Ann under her display name");
      await shot(page, "3-same-person-signed-in-again-reply-back");
      await ctx.close();
    }

    /* ═══ 2 · 5 pm ═══ */
    console.log("\n2 · 17:00 Toronto");
    {
      // 2a · input in the last 10 minutes at 5 pm: stays; signed out 10 minutes after that input
      calls = [];
      const { ctx, page } = await open(tor("07", 16, 45), "ann");
      await openThread(page, A);
      await run(page, 8 * 60e3);                                           // 16:53
      await typeInBox(page, "still here at five to five");                 // input at ~16:53
      check(await authed(page), "16:53, after typing: signed in");
      await run(page, 8 * 60e3);                                           // 17:01: input 8 minutes ago
      check(await authed(page), "17:01 with input 8 minutes ago: still signed in (5 pm does not sign her out)");
      await run(page, 3 * 60e3);                                           // 17:04: 11 minutes since input
      await until(async () => !(await authed(page)), "the sign-out 10 minutes after her last input");
      const b = await banner(page);
      check(b === NOTE_IDLE || b === NOTE_CLOSING, "then she is signed out with a calm line: " + JSON.stringify(b));
      check((await box(page)) === "still here at five to five", "her words are still in the box");
      await ctx.close();
    }
    {
      // 2b · no input at 5 pm (the page slept through it): signed out, reason closing, the 5 pm line
      calls = [];
      const { ctx, page } = await open(tor("07", 16, 30), "ann");
      await openThread(page, A);
      await typeInBox(page, "reply left over at closing time");
      await page.clock.fastForward(2 * 3600e3);                            // 18:30: the page slept through 5 pm
      await until(async () => !(await authed(page)), "the sign-out after sleeping through 5 pm");
      const b = await banner(page);
      const reason = ((await ended(page)).find(e => e.name === "Ann Operator") || {}).reason;
      check(reason === "closing" || reason === "idle", "ended with a closing or idle reason (" + reason + ")");
      check(b === (reason === "closing" ? NOTE_CLOSING : NOTE_IDLE), "and the line matches the reason: " + JSON.stringify(b));
      if (USE_REAL || reason === "closing") check(reason === "closing", "a page that slept through 5 pm is signed out as closing");
      check((await box(page)) === "reply left over at closing time", "the reply left on screen is kept");
      await shot(page, "4-sign-in-at-5pm");
      await signIn(page, "ann");
      check((await box(page)) === "reply left over at closing time", "and is back after she signs in");
      await ctx.close();
    }

    /* ═══ 3 · Admin ═══ */
    console.log("\n3 · Admin is exempt");
    {
      calls = [];
      const { ctx, page } = await open(tor("07", 16, 40), "paul", ["Paul", "Paul K"]);
      await openThread(page, A);
      await typeInBox(page, "owner's note");
      await run(page, 30 * 60e3);
      check(await authed(page), "Admin: 30 minutes with no input, across 5 pm: still signed in");
      await page.clock.fastForward(3 * 3600e3);
      await wait(800);
      await run(page, 60e3);
      check(await authed(page), "Admin: three more hours: still signed in");
      check(!(await ended(page)).some(e => e.reason === "idle" || e.reason === "closing"), "no idle or closing end for Admin");
      await ctx.close();
    }

    /* ═══ 4 · what counts as input ═══ */
    console.log("\n4 · input counts; polling and the page's own events do not");
    {
      calls = []; draftA = null;
      const { ctx, page } = await open(MORNING, "ann");
      const step = async (what, act, expectCounts) => {
        await run(page, 1500);                                             // the page marks input once a second at most
        const before = await lastInput(page);
        await act();
        await page.waitForTimeout(150);
        const after = await lastInput(page);
        check(expectCounts ? after > before : after === before, what + (expectCounts ? " counts as input" : " is not input"));
      };
      await run(page, 3000);
      const t0 = await lastInput(page);
      await run(page, 4 * 60e3);
      await wait(500);
      check((await lastInput(page)) === t0, "four minutes of polling and timers move the last input by nothing");
      await step("clicking a conversation", () => page.click(`[data-id="${A}"]`), true);
      await page.waitForSelector("#emDraftText");
      await step("typing in the reply box", async () => { await page.click("#emDraftText"); await page.keyboard.type("hi"); }, true);
      await step("scrolling the conversation list with the wheel", async () => { await page.mouse.move(150, 400); await page.mouse.wheel(0, 300); }, true);
      await step("a keystroke", () => page.keyboard.press("End"), true);
      await step("code that clicks for itself (not a person)", () => page.evaluate(id => document.querySelector(`[data-id="${id}"]`).click(), B), false);
      await step("an input event the page makes up", () => page.evaluate(() => { const t = document.getElementById("emDraftText"); t.dispatchEvent(new Event("input", { bubbles: true })); }), false);
      await step("the Polish button", () => page.click("#emPolishBtn"), true);
      await page.waitForSelector("#emPolishNote", { timeout: 6000 }).catch(() => {});
      await step("the AI Draft button", () => page.click("#emAiDraftBtn"), true);
      await page.waitForTimeout(600);
      await step("the AI's words arriving in the box", async () => { await wait(300); }, false);
      await ctx.close();
    }

    /* ═══ 5 · a reply on its way, an AI draft in progress ═══ */
    console.log("\n5 · work in flight when the person is signed out");
    {
      calls = []; draftA = null;
      const { ctx, page } = await open(MORNING, "ann");
      await openThread(page, A);
      const WORDS = "Hi Mia, it ships tomorrow with tracking";
      await typeInBox(page, WORDS);
      const release = hold("enqueue");
      await page.click("#emSendEtsyBtn");
      await until(() => calls.some(c => c.name === "etsyMailDraftSend" && c.b && c.b.op === "enqueue"), "the enqueue request to reach the fake (it is held there)");
      await page.waitForTimeout(500);
      check(await box(page) === "", "Send emptied the box (the reply is on its way)");
      await idleOut(page, "the sign-out while the reply is still in flight");
      check(await banner(page) === NOTE_IDLE, "Ann is signed out while her reply is still on its way");
      release();
      await until(() => enqueues().length > 0, "the enqueue to reach the fake");
      await wait(700);
      const eq = enqueues()[0];
      check(eq && eq.b.text === WORDS && eq.b.employeeName === "ann", "the reply that was in flight went out as Ann's (employeeName ann, her words)");
      check(await box(page) === "", "it finished: nothing came back into the box");
      check(!(await page.evaluate(() => /Not sent/.test(document.body.innerText))), "no 'Not sent' notice");
      // somebody else signs in before Ann is back: nothing about Ann's reply is recorded under them
      await signIn(page, "ben");
      await run(page, 12e3); await wait(500);
      const benNotes = () => activity().filter(e => /reply sent|reply drafted/.test(e.detail || "") && e.person === "Ben Operator");
      check(benNotes().length === 0, "Ann's reply is not recorded under Ben");
      await idleOut(page, "Ben's idle sign-out");
      await signIn(page, "ann");
      await run(page, 12e3); await wait(600);
      const annSent = activity().filter(e => /^reply sent/.test(e.detail || "") && e.person === "Ann Operator");
      check(annSent.length === 1, "when Ann signs in again her reply is recorded once, under her (" + annSent.length + ")");
      check(benNotes().length === 0, "and still nothing under Ben");
      // an AI draft being written when the person is signed out lands in its own conversation
      await openThread(page, A);
      const releaseAi = hold("draft");
      await page.click("#emAiDraftBtn");
      await page.waitForTimeout(400);
      await idleOut(page, "the sign-out while the AI draft is being written");
      releaseAi();
      await until(async () => (await box(page)) === "AI reply for Mia: your ring ships Friday.", "the AI draft in its box", 8000).catch(() => {});
      check(await box(page) === "AI reply for Mia: your ring ships Friday.", "the AI draft that finished after the sign-out landed in its own conversation's box");
      check(!(await authed(page)), "(Ann is still signed out)");
      await signIn(page, "ann");
      check(await box(page) === "AI reply for Mia: your ring ships Friday.", "and it is there when Ann signs in");
      await run(page, 12e3); await wait(600);
      check(activity().filter(e => e.detail === "reply drafted" && e.person === "Ann Operator").length === 1, "the draft is recorded under Ann (who asked for it)");
      check(!enqueues().some(e => e.b.text.startsWith("AI reply")), "the AI draft was not sent (auto-send is off)");
      await ctx.close();
    }

    /* ═══ 6 · a second person ═══ */
    console.log("\n6 · another person signs in on the same page");
    {
      calls = []; draftA = null;
      const { ctx, page } = await open(MORNING, "ann");
      await openThread(page, A);
      const ANN_WORDS = "Ann's private words for Mia: the price is $48";
      await typeInBox(page, ANN_WORDS);
      await idleOut(page, "Ann's idle sign-out");
      check(await box(page) === ANN_WORDS, "Ann's words are kept in the page under the sign-in screen");
      await signIn(page, "ben");
      const seen = await page.evaluate(w => ({
        box: document.getElementById("emDraftText").value,
        text: document.body.innerText.includes(w),
        live: Object.keys(sessionStorage).filter(k => /^em\.typed\./.test(k) && sessionStorage.getItem(k).includes(w)),
        local: Object.keys(localStorage).filter(k => localStorage.getItem(k).includes(w))
      }), ANN_WORDS);
      check(seen.box === "" && !seen.text, "Ben does not see Ann's words: his box is empty and they are nowhere on his screen");
      check(seen.live.length === 0 && seen.local.length === 0, "they are not in the live kept-reply store either (set aside)");
      check(await page.evaluate(w => Object.keys(sessionStorage).some(k => sessionStorage.getItem(k).includes(w)), ANN_WORDS), "but they are kept in this tab, not deleted");
      await shot(page, "5-second-person-box-empty");
      await page.click("#emSendEtsyBtn");
      await wait(600);
      check(!enqueues().length, "Ben pressing Send on an empty box sends nothing");
      const BEN_WORDS = "Ben here: your order is packed";
      await typeInBox(page, BEN_WORDS);
      await page.click("#emSendEtsyBtn");
      await until(() => enqueues().length > 0, "Ben's send", 6000);
      check(enqueues().length === 1 && enqueues()[0].b.text === BEN_WORDS && enqueues()[0].b.employeeName === "ben", "Ben sends his own words, as ben");
      check(!enqueues().some(e => e.b.text.includes("private words")), "no request ever carried Ann's words under Ben's name");
      const BEN_LEFT = "Ben's unsent note about the chain";
      await typeInBox(page, BEN_LEFT);
      await idleOut(page, "Ben's idle sign-out");
      await signIn(page, "ann");
      check(await box(page) === ANN_WORDS, "Ann signs in again: her words are back in the box");
      check(!(await page.evaluate(w => document.body.innerText.includes(w), BEN_LEFT)), "and Ben's unsent words are not shown to her");
      await page.click("#emSendEtsyBtn");
      await until(() => enqueues().length > 1, "Ann's send", 6000);
      const second = enqueues()[1];
      check(second.b.text === ANN_WORDS && second.b.employeeName === "ann", "Ann can send her own words, as ann");
      await idleOut(page, "Ann's idle sign-out");
      await signIn(page, "ben");
      check(await box(page) === BEN_LEFT, "Ben signs in again later: his own unsent words are back");
      await ctx.close();
    }

    /* ═══ 7 · the session's name ═══ */
    console.log("\n7 · the session is written under a name, never a PIN");
    {
      calls = [];
      const { ctx, page } = await open(MORNING, null);
      await signIn(page, "4821");
      let signed = (await ss(page)).filter(c => c[0] === "signedIn").map(c => c[1]);
      check(signed.length === 1 && signed[0].name === "Dana Mail" && !/^\d+$/.test(signed[0].id), "digits-only username with a display name: the name is the display name, the id is not digits (" + JSON.stringify(signed[0]) + ")");
      await idleOut(page, "the idle sign-out of Dana");
      await signIn(page, "5550123");
      signed = (await ss(page)).filter(c => c[0] === "signedIn").map(c => c[1]);
      check(signed.length === 1, "a digits-only username with no display name starts no session under that number (still only Dana's)");
      check(!JSON.stringify(await ss(page)).includes("5550123"), "that number is nowhere in what the session was told");
      check(!JSON.stringify(calls.filter(c => c.name === "firebaseOrders")).includes("5550123"), "nor in anything sent to the station door");
      await ctx.close();
    }
    check(errorsAll.length === 0, "no page errors" + (errorsAll.length ? ": " + errorsAll.slice(0, 3).join(" | ") : ""));
  } finally {
    await browser.close();
    server.close();
  }
  if (fails.length) { console.log("\n" + fails.length + " failed:\n - " + fails.join("\n - ")); process.exit(1); }
  console.log("\nall passed");
})().catch(e => { console.error(e); process.exit(1); });
