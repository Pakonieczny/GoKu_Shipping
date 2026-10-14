// The inbox (etsy-mail-1.html) when the Etsy helper's computer is off (14 Oct 2026, Paul): "show a small delay message inside each
// affected email thread only ... not super visible ... a live timer ... just informational that everything is fine".
//
//   · a message waiting for an away helper gets ONE quiet line in its own conversation (grey, small, no alarm colours), with a clock that
//     runs from when the message was written, and says it sends by itself when the connection is back;
//   · nothing else on the page says anything about it: no top-bar alarm, no line at the top of the list, no toast, no word "helper";
//   · while every open message only waits for the helper, the page looks every 20 s, not every 4 s;
//   · the bubble's own tag reads "delayed" (no spinner); the list row says "delayed" instead of "sending…" after 10 minutes;
//   · the phone shows the same line; it goes when the message goes.
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   node tests/etsy-mail/adv-inbox-send-delay.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no AI, nothing sent to anyone); every other request is aborted.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const T0 = Date.now();
const ts = ms => ({ _ts: true, ms });
const IDS = { A: "etsy_conv_3001", B: "etsy_conv_3002", C: "etsy_conv_3003" };
const HELD_AT = T0 - (2 * 3600e3 + 14 * 60e3 + 5e3);        // written 2 h 14 min 5 s ago
const mkThread = (id, name, i) => ({
  id, customerName: name, status: "etsy_scraped", unread: false,
  lastInboundAt: ts(T0 - 3 * 3600e3 - i * 60e3), awaitingReplySince: ts(T0 - 3 * 3600e3 - i * 60e3), updatedAt: ts(T0 - 3 * 3600e3 - i * 60e3),
  etsyConversationUrl: "https://example.invalid/c/" + id
});
const THREADS = Object.entries(IDS).map(([k, id], i) => mkThread(id, "Cust " + k, i));
const heldMsg = (id, atMs, text) => ({
  id: "optim_draft_" + id, direction: "outbound", source: "owner", senderName: "Paul", senderRole: "shop_owner", text,
  timestamp: ts(atMs), createdAt: ts(atMs), localOptimistic: true, optimisticDraftId: "draft_" + id, optimisticTextKey: text.trim().toLowerCase().slice(0, 200)
});
const baseMessages = id => [
  { id: id + "_m1", direction: "inbound", senderName: "Cust", senderRole: "customer", text: "Hi, can you add the birthstone?", timestamp: ts(T0 - 7 * 3600e3), createdAt: ts(T0 - 7 * 3600e3) },
  { id: id + "_m2", direction: "outbound", senderName: "CustomBrites", senderRole: "staff", source: "etsy", text: "Of course, one moment please.", timestamp: ts(T0 - 6.9 * 3600e3), createdAt: ts(T0 - 6.9 * 3600e3) }
];
const world = {};
for (const id of Object.values(IDS)) world[id] = { messages: baseMessages(id), qn: 1, items: [], recent: [] };
const calls = [];
const totals = { held: 5 };          // every email waiting for the helper, in all conversations (the server counts them)
const qitem = (id, sendId, o) => Object.assign({ id: sendId, t: id, st: "queued", at: HELD_AT, src: "inbox", by: "Paul", tp: "", att: 0, n: 0, open: true, wait: "offline", pos: 1 }, o || {});
// the link monitor's judgement, as the server now writes it while the helper is away: green, with calm "wait" lines in the details
let linkDoc = () => ({
  id: "linkHealth", atMs: Date.now(), level: "ok", short: "", problem: "", kind: "", downSinceMs: 0, incidentId: "", worstSinceMs: 0,
  checks: [
    { id: "helper", label: "Etsy helper", level: "wait", text: "The inbox's Etsy helper is away (last checked 2 h ago). Nothing is lost: messages wait and go out by themselves, in order, when it is back." },
    { id: "send", label: "Sending", level: "wait", text: "2 messages are waiting for the helper and go out by themselves, in order, when it is back." },
    { id: "read", label: "Reading answers", level: "wait", text: "12 new Etsy messages are waiting to be read into the inbox; the helper reads them when it is back." }
  ]
});

function fake(method, url, body) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  calls.push({ name, method, q, b, at: Date.now() });
  if (name === "firestoreProxy") {
    if (q.op === "get" && q.coll === "EtsyMail_Config" && q.id === "linkHealth") return { success: true, exists: true, doc: linkDoc() };
    if (q.op === "list" && q.coll === "EtsyMail_Threads") return { docs: THREADS };
    if (q.op === "listSub") return { docs: (world[q.id] || { messages: [] }).messages };
    if (q.op === "get") return { exists: false };
    return { ok: true };
  }
  if (name === "etsyMailAuth") return { ok: true, username: "paul", displayName: "Paul", role: "owner" };
  if (name === "etsyMailDraftSend" && method === "GET" && q.op === "queue_state") {
    const w = world[q.threadId];
    if (!w) return { ok: true, queue: { n: 0, now: Date.now(), items: [], recent: [] } };
    if (q.n !== undefined && q.n !== "" && Number(q.n) === w.qn) return { ok: true, queue: { n: w.qn, unchanged: true, now: Date.now() } };
    return { ok: true, queue: { n: w.qn, now: Date.now(), gapMs: 12000, helperSeenAtMs: Date.now() - 2 * 3600e3, offline: true, held: totals.held, recent: w.recent, items: w.items } };
  }
  if (name === "etsyMailDraftSend" && method === "GET" && q.op === "status") return { __status: 404, error: "Draft not found" };
  return { ok: true };
}
function setQueue(id, items) { const w = world[id]; w.items = items; w.recent = []; w.qn++; }

const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    let body = "";
    req.on("data", d => { body += d; });
    req.on("end", () => {
      const out = fake(req.method, req.url, body);
      const status = out.__status || 200; delete out.__status;
      res.writeHead(status, { "Content-Type": "application/json" }); res.end(JSON.stringify(out));
    });
    return;
  }
  const f = path.join(root, u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "Content-Type": "text/html" });
  fs.createReadStream(f).pipe(res);
}).listen(0);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const fails = [];
  const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
  const open = async (mobile) => {
    const ctx = await browser.newContext(mobile
      ? { viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, userAgent: "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148" }
      : { viewport: { width: 1400, height: 900 } });
    await ctx.route(x => !/^http:\/\/127\.0\.0\.1[:/]/.test(x.href), r => r.abort());
    await ctx.addInitScript(() => {
      try {
        localStorage.setItem("etsymail_session", "tok");
        localStorage.setItem("etsymail_session_profile", JSON.stringify({ username: "paul", displayName: "Paul", role: "owner" }));
      } catch (_) {}
    });
    const page = await ctx.newPage();
    const errors = [];
    page.on("pageerror", e => errors.push(String(e)));
    await page.goto(`http://127.0.0.1:${server.address().port}/etsy-mail-1.html` + (mobile ? "?force=mobile" : ""));
    return { ctx, page, errors };
  };
  const pick = async (page, id, mobile) => {
    if (mobile) { await page.waitForSelector(`.m-thread-row[data-id="${id}"]`, { timeout: 15000 }); await page.click(`.m-thread-row[data-id="${id}"]`); await page.waitForSelector("#mComposerTextarea", { timeout: 10000 }); return; }
    await page.waitForSelector(`[data-id="${id}"]`, { timeout: 15000 });
    await page.click(`[data-id="${id}"]`);
    await page.waitForSelector("#emDraftText", { timeout: 10000 });
  };
  const note = (page, mobile) => page.evaluate((mobile) => {
    const el = document.getElementById(mobile ? "mQueueDelay" : "emQueueDelay");
    if (!el || el.style.display === "none") return null;
    const clock = el.querySelector("[data-sq-since]");
    const cs = getComputedStyle(el);
    return { text: Array.from(el.querySelectorAll(":scope > span")).map(x => x.textContent).join(" ").replace(/\s+/g, " ").trim(), clock: clock ? clock.textContent : null, color: cs.color, size: parseFloat(cs.fontSize), title: el.title, bg: cs.backgroundColor };
  }, mobile);
  const noteUntil = async (page, mobile, pred, ms) => {
    const end = Date.now() + ms; let s = null;
    while (Date.now() < end) { s = await note(page, mobile); if (pred(s)) return s; await page.waitForTimeout(200); }
    return s;
  };
  const count = (name, pred) => calls.filter(c => c.name === name && (!pred || pred(c))).length;
  const secs = c => { const p = String(c).split(":").map(Number); return p.length === 3 ? p[0] * 3600 + p[1] * 60 + p[2] : p[0] * 60 + p[1]; };

  try {
    // ── 1. desktop: one quiet line, a running clock, and nothing else ──
    {
      const id = IDS.A;
      THREADS[0].sendQueueState = "queued"; THREADS[0].sendQueueSinceMs = HELD_AT;
      world[id].messages.push(heldMsg(id, HELD_AT, "Yes, we can add the birthstone for $4."));
      setQueue(id, [qitem(id, "q_3001", { tp: "Yes, we can add the birthstone for $4." })]);
      const { ctx, page, errors } = await open(false);
      await pick(page, id, false);
      let n = await noteUntil(page, false, x => !!x, 12000);
      check(n && /^Delayed · \d+:\d\d:\d\d · 5 emails queued · sends automatically when the connection is back$/.test(n.text), "the thread says, in one short line, that the message is delayed, how many emails are queued in all, and that it sends by itself: \"" + (n && n.text) + "\"");
      check(n && /^2:1[45]:\d\d$/.test(n.clock), "the clock runs from when the message was written (2 h 14 min ago): " + (n && n.clock));
      const c1 = n && secs(n.clock);
      if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + "-desktop.png" });   // (a picture of the thread for a person to look at)
      await page.waitForTimeout(2600);
      n = await note(page, false);
      check(n && secs(n.clock) >= c1 + 2 && secs(n.clock) <= c1 + 4, "and it is live: it moved on by about 2.6 s (" + c1 + " -> " + (n && secs(n.clock)) + ")");
      check(n && n.size <= 12 && /rgb\(1[01]\d, 1[12]\d, 1[23]\d\)/.test(n.color) && n.bg === "rgba(0, 0, 0, 0)", "small and grey, no alarm colours or box: " + (n && n.size) + "px " + (n && n.color));
      check(n && /nothing is lost|Nothing is lost/.test(n.title) && /saved/.test(n.title), "its tooltip explains: saved, nothing lost, goes out by itself");
      const tag = await page.evaluate(() => {
        const all = Array.from(document.querySelectorAll("#emThreadBox [data-mid]")).filter(x => /optim_|local_/.test(x.getAttribute("data-mid") || ""));
        const el = all.length ? all[all.length - 1].querySelector(".em-msg-sync") : null;
        return el ? { text: el.textContent.replace(/^·\s*/, "").trim(), quiet: el.classList.contains("quiet"), warn: el.classList.contains("warn"), spin: !!el.querySelector(".em-sq-spin") } : null;
      });
      check(tag && tag.text === "delayed" && tag.quiet && !tag.warn && !tag.spin, "the message's own tag reads \"delayed\": quiet, not a warning, no spinner: " + JSON.stringify(tag));
      // everything else on the page is silent about it
      const rest = await page.evaluate(() => ({
        alarmShown: (() => { const e = document.getElementById("emLinkAlarm"); return !!e && !e.hidden; })(),
        errorLines: document.querySelectorAll(".em-smart-line.is-error, .m-smart-line.is-error").length,
        alerts: Array.from(document.querySelectorAll("[role=alert]")).map(x => x.textContent.trim()).filter(Boolean),
        body: document.body.innerText,
        title: document.title,
        badge: Array.from(document.querySelectorAll('[data-id="etsy_conv_3001"] .em-sc')).map(x => x.textContent.trim() + "/" + x.className.replace(/.*em-sc-(\w+).*/, "$1")).filter(x => /Sending|Delayed/.test(x)).join(",")
      }));
      check(!rest.alarmShown && rest.errorLines === 0 && rest.alerts.length === 0, "no alarm in the top bar, no red line at the top of the list, nothing with role=alert");
      check(!/helper|Email link|did not go out|need a person|waiting to be read/i.test(rest.body), "no word about the helper or the email link anywhere on the page");
      check(!/^\u26a0/.test(rest.title), "the browser tab title carries no warning mark");
      check(rest.badge === "Delayed/quiet", "the list row's label says \"Delayed\" in a quiet grey (not \"Sending…\" in green) for a message that has waited over 10 minutes: \"" + rest.badge + "\"");
      // not polled every 4 s while it only waits for the helper
      const q0 = count("etsyMailDraftSend", c => c.q.op === "queue_state");
      await page.waitForTimeout(9000);
      const looks = count("etsyMailDraftSend", c => c.q.op === "queue_state") - q0;
      check(looks <= 1, "the page looks at the queue every 20 s while the message only waits for the helper, not every 4 s (" + looks + " looks in 9 s)");
      check(count("etsyMailDraftSend", c => c.q.op === "status") === 0, "and does not read the draft slot as well");
      // it goes: the line goes with it
      setQueue(id, []);
      n = await noteUntil(page, false, x => x === null, 26000);
      check(n === null, "when the message is gone from the queue the line goes");
      check(errors.length === 0, "no page errors" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
      await ctx.close();
    }

    // ── 2. two messages: one line, counted, the clock runs from the oldest ──
    {
      const id = IDS.B;
      THREADS[1].sendQueueState = "queued"; THREADS[1].sendQueueSinceMs = HELD_AT;
      setQueue(id, [qitem(id, "q_3002", { tp: "First", at: HELD_AT, pos: 1 }), qitem(id, "q_3003", { tp: "Second", at: HELD_AT + 600e3, pos: 2 })]);
      const { ctx, page, errors } = await open(false);
      await pick(page, id, false);
      const n = await noteUntil(page, false, x => !!x, 12000);
      check(n && /^2 messages delayed · oldest \d+:\d\d:\d\d · 5 emails queued · send automatically when the connection is back$/.test(n.text) && /^2:1[45]:\d\d$/.test(n.clock), "two waiting: one line, \"2 messages delayed · oldest 2:14:xx\": \"" + (n && n.text) + "\"");
      check(errors.length === 0, "no page errors (two messages)");
      await ctx.close();
    }

    // ── 2b. one email in all: "1 email queued" ──
    {
      const id = IDS.B;
      totals.held = 1;
      setQueue(id, [qitem(id, "q_3002", { tp: "First", at: HELD_AT, pos: 1 })]);
      const { ctx, page } = await open(false);
      await pick(page, id, false);
      const n = await noteUntil(page, false, x => !!x, 12000);
      check(n && /^Delayed · \d+:\d\d:\d\d · 1 email queued · sends automatically/.test(n.text), "one email in all: \"1 email queued\" (singular): \"" + (n && n.text) + "\"");
      totals.held = 5;
      await ctx.close();
    }

    // ── 3. a message with a retry wait, or moving, is NOT dressed as a delay ──
    {
      const id = IDS.C;
      setQueue(id, [qitem(id, "q_3004", { tp: "Moving", wait: "pace", pos: 1, at: Date.now() })]);
      const { ctx, page } = await open(false);
      await pick(page, id, false);
      await page.waitForTimeout(2500);
      check((await note(page, false)) === null, "a message that is just pausing between sends shows no delay line");
      await ctx.close();
    }

    // ── 4. the phone ──
    {
      const id = IDS.A;
      THREADS[0].sendQueueState = "queued"; THREADS[0].sendQueueSinceMs = HELD_AT;
      world[id].messages = baseMessages(id).concat(heldMsg(id, HELD_AT, "Yes, we can add the birthstone for $4."));
      setQueue(id, [qitem(id, "q_3001", { tp: "Yes, we can add the birthstone for $4." })]);
      const { ctx, page, errors } = await open(true);
      await pick(page, id, true);
      let n = await noteUntil(page, true, x => !!x, 12000);
      check(n && /^Delayed · \d+:\d\d:\d\d · 5 emails queued · sends automatically when the connection is back$/.test(n.text), "phone: the same one line: \"" + (n && n.text) + "\"");
      const c1 = n && secs(n.clock);
      if (process.env.SHOT) await page.screenshot({ path: process.env.SHOT + "-phone.png" });
      await page.waitForTimeout(2600);
      n = await note(page, true);
      check(n && secs(n.clock) >= c1 + 2, "phone: the clock runs");
      const body = await page.evaluate(() => document.body.innerText);
      check(!/helper|Email link|did not go out|need a person/i.test(body), "phone: nothing else says anything about it");
      check(errors.length === 0, "no page errors (phone)" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
      await ctx.close();
    }
  } catch (e) {
    fails.push("test crashed: " + (e && e.stack || e));
    console.log("  FAIL test crashed:", e && e.stack || e);
  }
  await browser.close();
  server.close();
  console.log(fails.length ? "\n" + fails.length + " FAILED" : "\nall passed");
  process.exit(fails.length ? 1 : 0);
})();
