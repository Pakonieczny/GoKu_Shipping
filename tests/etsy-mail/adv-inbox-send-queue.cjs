// The inbox (etsy-mail-1.html) and the send queue (MAILQUEUE, 10 Oct 2026). Every message to a buyer waits its turn in one
// server queue; the open conversation shows where its messages stand, with no per-second polling, and a message that needs a
// person gets a bar with the plain reason and one click for each thing a person can do.
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   node tests/etsy-mail/adv-inbox-send-queue.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no AI, nothing sent to anyone); every other request is
// aborted. The fake plays the server's queue: queue_state (with the revision shortcut), enqueue, queue_retry / queue_mark_sent /
// queue_dismiss.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const T0 = Date.now();
const ts = ms => ({ _ts: true, ms });
const IDS = { A: "etsy_conv_2001", B: "etsy_conv_2002", C: "etsy_conv_2003", D: "etsy_conv_2004", E: "etsy_conv_2005" };
const mkThread = (id, name, i) => ({
  id, customerName: name, status: "etsy_scraped", unread: false,
  lastInboundAt: ts(T0 - 3600e3 - i * 60e3), awaitingReplySince: ts(T0 - 3600e3 - i * 60e3), updatedAt: ts(T0 - 3600e3 - i * 60e3),
  etsyConversationUrl: "https://example.invalid/c/" + id
});
const THREADS = Object.entries(IDS).map(([k, id], i) => mkThread(id, "Cust " + k, i));
const baseMessages = id => [
  { id: id + "_m1", direction: "inbound", senderName: "Cust", senderRole: "customer", text: "Hi, can you add the birthstone?", timestamp: ts(T0 - 7200e3), createdAt: ts(T0 - 7200e3) },
  { id: id + "_m2", direction: "outbound", senderName: "CustomBrites", senderRole: "staff", source: "etsy", text: "Of course, one moment please.", timestamp: ts(T0 - 7000e3), createdAt: ts(T0 - 7000e3) }
];
// Per-thread world: the messages, the draft slot, and the queue as the server would answer it.
const world = {};
for (const id of Object.values(IDS)) world[id] = { messages: baseMessages(id), draft: null, qn: 0, items: [], recent: [] };
const calls = [];
const keys = [];
let nowOff = 0;   // the fake server's clock is this far ahead of the browser's

function queueAnswer(w, q) {
  if (q.n !== undefined && q.n !== "" && Number(q.n) === w.qn) return { ok: true, queue: { n: w.qn, unchanged: true, now: Date.now() + nowOff } };
  return { ok: true, queue: { n: w.qn, now: Date.now() + nowOff, gapMs: 12000, helperSeenAtMs: Date.now(), recent: w.recent, items: w.items } };
}
function setQueue(id, items, recent) { const w = world[id]; w.items = items; w.recent = recent || []; w.qn++; }
const qitem = (id, sendId, o) => Object.assign({ id: sendId, t: id, st: "queued", at: Date.now() + nowOff, src: "inbox", by: "Paul", tp: "", att: 0, n: 0, open: true }, o || {});

function fake(method, url, body) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  calls.push({ name, method, q, b, at: Date.now() });
  if (name === "firestoreProxy") {
    if (q.op === "list" && q.coll === "EtsyMail_Threads") return { docs: THREADS };
    if (q.op === "listSub") return { docs: (world[q.id] || { messages: [] }).messages };
    if (q.op === "get" && q.coll === "EtsyMail_Drafts") {
      const w = world[String(q.id).replace(/^draft_/, "")];
      return w && w.draft ? { exists: true, doc: w.draft } : { exists: false };
    }
    if (q.op === "get") return { exists: false };
    return { ok: true };
  }
  if (name === "etsyMailAuth") return { ok: true, username: "paul", displayName: "Paul", role: "owner" };
  if (name === "etsyMailDraftSend" && method === "POST" && b.op === "enqueue") {
    const w = world[b.threadId];
    keys.push(b.idempotencyKey);
    const now = Date.now();
    w.draft = { id: "draft_" + b.threadId, threadId: b.threadId, text: b.text, status: "queued", queuedAt: ts(now) };
    w.messages = w.messages.filter(m => m.id !== "optim_draft_" + b.threadId).concat({
      id: "optim_draft_" + b.threadId, direction: "outbound", source: "owner", senderName: "Paul", senderRole: "shop_owner",
      text: b.text, timestamp: ts(now), createdAt: ts(now), localOptimistic: true, optimisticDraftId: "draft_" + b.threadId,
      optimisticTextKey: String(b.text).trim().toLowerCase().slice(0, 200)
    });
    const sendId = "q_" + b.threadId.slice(-4);
    setQueue(b.threadId, [qitem(b.threadId, sendId, { tp: b.text.slice(0, 90), pos: 3 })]);
    return { __delay: 100, ok: true, draftId: "draft_" + b.threadId, sendId, queueState: "queued", attachments: b.attachments || [], text: b.text };
  }
  if (name === "etsyMailDraftSend" && method === "GET" && q.op === "queue_state") {
    const w = world[q.threadId];
    return w ? queueAnswer(w, q) : { ok: true, queue: { n: 0, now: Date.now(), items: [], recent: [] } };
  }
  if (name === "etsyMailDraftSend" && method === "GET" && q.op === "status") {
    const w = world[String(q.draftId).replace(/^draft_/, "")];
    return w && w.draft ? { ok: true, draft: w.draft } : { __status: 404, error: "Draft not found" };
  }
  if (name === "etsyMailDraftSend" && method === "POST" && /^queue_/.test(b.op)) {
    const w = Object.values(world).find(x => x.items.some(v => v.id === b.sendId));
    if (!w) return { __status: 404, error: "Message not found", errorCode: "QUEUE_NOT_FOUND" };
    const v = w.items.find(x => x.id === b.sendId);
    if (b.op === "queue_retry" && v.st === "failed" && world.__maybe && !b.confirmMaybeSent) return { __status: 409, error: "This message may already have gone out. Check the conversation on Etsy, then confirm to send it again.", errorCode: "QUEUE_MAYBE_SENT", text: v.text };
    if (b.op === "queue_retry") setQueue(v.t, [Object.assign({}, v, { st: "queued", pos: 1, why: undefined, text: undefined })]);
    else setQueue(v.t, [], [{ id: v.id, t: v.t, st: b.op === "queue_mark_sent" ? "confirmed" : "cancelled", at: Date.now() + nowOff }]);
    return { ok: true, sendId: b.sendId, state: "queued" };
  }
  return { ok: true };
}

const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    let body = "";
    req.on("data", d => { body += d; });
    req.on("end", () => {
      const out = fake(req.method, req.url, body);
      const delay = out.__delay || 0, status = out.__status || 200; delete out.__delay; delete out.__status;
      setTimeout(() => { res.writeHead(status, { "Content-Type": "application/json" }); res.end(JSON.stringify(out)); }, delay);
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
    await ctx.grantPermissions(["clipboard-read", "clipboard-write"], { origin: "http://127.0.0.1:" + server.address().port }).catch(() => {});
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
  const pick = async (page, id) => {
    await page.waitForSelector(`[data-id="${id}"]`, { timeout: 15000 });
    await page.click(`[data-id="${id}"]`);
    await page.waitForSelector("#emDraftText", { timeout: 10000 });
  };
  // The newest stand-in bubble's tag text, and whether it carries the spinner.
  const tagOf = (page, mobile) => page.evaluate((mobile) => {
    const sel = mobile ? "#mMessagesList .m-msg-wrap[data-mid]" : "#emThreadBox [data-mid]";
    const all = Array.from(document.querySelectorAll(sel)).filter(n => n.getAttribute("data-mid") && !/_m\d$/.test(n.getAttribute("data-mid")));
    const last = all[all.length - 1];
    if (!last) return null;
    const meta = last.querySelector(mobile ? ".m-msg-meta" : ".em-meta");
    const tag = mobile ? (meta.textContent.split("·").slice(2).join("·").trim()) : ((meta.querySelector(".em-msg-sync") || {}).textContent || "").replace(/^·\s*/, "").trim();
    const el = meta.querySelector(".em-msg-sync");
    return { mid: last.getAttribute("data-mid"), tag, spin: !!(el && el.querySelector(".em-sq-spin")), warn: !!(el && el.classList.contains("warn")), title: el ? el.title : "", count: all.length };
  }, mobile);
  const until = async (page, mobile, pred, ms) => {
    const end = Date.now() + ms; let s = null;
    while (Date.now() < end) { s = await tagOf(page, mobile); if (pred(s)) return s; await page.waitForTimeout(200); }
    return s;
  };
  const bar = (page, mobile) => page.evaluate((mobile) => {
    const el = document.getElementById(mobile ? "mQueueStatus" : "emQueueStatus");
    if (!el || el.style.display === "none") return null;
    return { text: el.textContent.replace(/\s+/g, " ").trim(), cls: el.className, buttons: [...el.querySelectorAll("[data-sq]")].map(b => b.textContent.trim()) };
  }, mobile);
  const barUntil = async (page, mobile, pred, ms) => {
    const end = Date.now() + ms; let s = null;
    while (Date.now() < end) { s = await bar(page, mobile); if (pred(s)) return s; await page.waitForTimeout(200); }
    return s;
  };
  const count = (name, pred) => calls.filter(c => c.name === name && (!pred || pred(c))).length;

  try {
    // ── 1. A reply waits its turn, goes, and the page follows with one small read per change ──
    {
      const { ctx, page, errors } = await open(false);
      await pick(page, IDS.A);
      await page.waitForFunction(() => document.querySelectorAll("#emThreadBox [data-mid]").length >= 2, null, { timeout: 10000 });
      await page.fill("#emDraftText", "Yes, we can add the birthstone for $4.");
      await page.click("#emSendEtsyBtn");
      let s = await until(page, false, x => x && /^local_/.test(x.mid) && x.tag === "queued (3rd)", 8000);
      check(s && s.tag === "queued (3rd)" && !s.spin, "third in line: the bubble reads \"queued (3rd)\" (got " + JSON.stringify(s && s.tag) + ")");
      check(s && /one at a time/.test(s.title), "its tooltip says messages go one at a time: " + (s && s.title));
      check(keys.length === 1 && /^i:etsy_conv_2001:.{8,}/.test(keys[0]), "the press carries its own key to the queue: " + keys[0]);
      const q0 = count("etsyMailDraftSend", c => c.q.op === "queue_state");
      const slot0 = count("etsyMailDraftSend", c => c.q.op === "status");
      // nothing changed: the page asks again with its revision and keeps what it knows
      await page.waitForTimeout(9000);
      const later = calls.filter(c => c.name === "etsyMailDraftSend" && c.q.op === "queue_state").slice(q0 - 1);
      check(later.length >= 2 && later.every((c, i) => i === 0 || String(c.q.n) === String(world[IDS.A].qn)), "later looks carry the revision it has (n=" + world[IDS.A].qn + "), so an idle queue is one small answer");
      check(later.length <= 4, "about one look every 4 s, not per second (" + later.length + " looks in 9 s)");
      check(count("etsyMailDraftSend", c => c.q.op === "status") - slot0 === 0, "the draft slot is not read as well while the queue is followed");
      s = await tagOf(page, false);
      check(s && s.tag === "queued (3rd)", "an unchanged answer leaves the words as they were");

      setQueue(IDS.A, [qitem(IDS.A, "q_2001", { tp: "Yes, we can add the birthstone for $4.", pos: 1, wait: "pace" })]);
      s = await until(page, false, x => x && x.tag === "queued (1st)", 9000);
      check(s && s.tag === "queued (1st)" && s.spin && /short pause/.test(s.title), "first in line but pausing between sends: \"queued (1st)\" with the small spinner and the reason");
      setQueue(IDS.A, [qitem(IDS.A, "q_2001", { tp: "Yes, we can add the birthstone for $4.", st: "sending" })]);
      s = await until(page, false, x => x && x.tag === "sending…", 9000);
      check(s && s.tag === "sending…" && s.spin, "being sent: \"sending…\" with the spinner");
      setQueue(IDS.A, [qitem(IDS.A, "q_2001", { tp: "Yes, we can add the birthstone for $4.", wait: "retry", nb: Date.now() + nowOff + 125000, n: 1, pos: 1 })]);
      s = await until(page, false, x => x && /^trying again/.test(x.tag), 9000);
      check(s && s.tag === "trying again in 2 min" && s.spin, "a delayed retry says when: \"" + (s && s.tag) + "\"");
      setQueue(IDS.A, [qitem(IDS.A, "q_2001", { tp: "Yes, we can add the birthstone for $4.", wait: "helper", pos: 1 })]);
      s = await until(page, false, x => x && /helper/.test(x.tag), 9000);
      check(s && s.tag === "waiting for the Etsy helper" && s.spin && /nothing is lost/.test(s.title), "no helper: \"waiting for the Etsy helper\" and \"nothing is lost\"");

      // it goes: the queue closes it; the page reads the conversation and the list at once (not at the next poll)
      const msgs0 = count("firestoreProxy", c => c.q.op === "listSub" && c.q.id === IDS.A);
      const list0 = count("firestoreProxy", c => c.q.op === "list" && c.q.coll === "EtsyMail_Threads");
      world[IDS.A].draft = Object.assign({}, world[IDS.A].draft, { status: "sent", sentAt: ts(Date.now()) });
      setQueue(IDS.A, [], [{ id: "q_2001", t: IDS.A, st: "confirmed", at: Date.now() + nowOff }]);
      s = await until(page, false, x => x && x.tag === "delivered", 9000);
      check(s && s.tag === "delivered" && !s.spin, "sent and accepted by Etsy: \"delivered\"");
      await page.waitForTimeout(1500);
      check(count("firestoreProxy", c => c.q.op === "listSub" && c.q.id === IDS.A) > msgs0 && count("firestoreProxy", c => c.q.op === "list" && c.q.coll === "EtsyMail_Threads") > list0, "the moment it is over, the conversation and the thread list are read again");
      const q1 = count("etsyMailDraftSend", c => c.q.op === "queue_state");
      await page.waitForTimeout(9000);
      check(count("etsyMailDraftSend", c => c.q.op === "queue_state") - q1 <= 1, "nothing left on its way: the page stops asking the queue");
      check(errors.length === 0, "no page errors" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
      await ctx.close();
    }

    // ── 2. Did not go: the bar says why; one click to try again; copy; put away ──
    {
      const id = IDS.B;
      THREADS[1].sendQueueProblem = true;
      setQueue(id, [qitem(id, "q_2002", { st: "failed", tp: "Your order ships Monday.", text: "Your order ships Monday.", why: "The Etsy helper did not answer after 4 tries. Nothing was sent.", code: "HELPER_OFFLINE", n: 4 })]);
      const { ctx, page } = await open(false);
      await pick(page, id);
      let b = await barUntil(page, false, x => !!x, 9000);
      check(b && /Not sent\./.test(b.text) && /did not answer after 4 tries/.test(b.text), "the bar says not sent and why, in plain words: " + (b && b.text));
      check(b && ["Try again", "Copy message", "Discard"].every(x => b.buttons.includes(x)), "with Try again, Copy message and Discard: " + (b && b.buttons.join(" | ")));
      check(b && /failed/.test(b.cls), "drawn as a problem");
      const q0 = count("etsyMailDraftSend", c => c.q.op === "queue_state");
      await page.waitForTimeout(9000);
      check(count("etsyMailDraftSend", c => c.q.op === "queue_state") - q0 === 0, "a message waiting for a person is looked at once, not polled");
      await page.click("#emQueueStatus [data-sq=copy]");
      await page.waitForTimeout(300);
      const clip = await page.evaluate(() => navigator.clipboard.readText().catch(() => null));
      check(clip === null || clip === "Your order ships Monday.", "Copy message puts the words on the clipboard" + (clip === null ? " (clipboard read not available here)" : ""));
      world.__maybe = true;
      let asked = 0;
      page.on("dialog", d => { asked++; d.accept(); });
      await page.click("#emQueueStatus [data-sq=retry]");
      await page.waitForTimeout(1200);
      const rt = calls.filter(c => c.b.op === "queue_retry" && c.b.sendId === "q_2002");
      check(rt.length === 2 && rt[0].b.confirmMaybeSent !== true && rt[1].b.confirmMaybeSent === true && asked === 1, "Try again: when the server says it may already have gone, one question, then the confirmed retry (" + rt.length + " calls, " + asked + " question)");
      world.__maybe = false;
      const gone = await barUntil(page, false, x => !x, 9000);
      check(gone === null, "the bar goes once it is back in the queue");
      const t2 = await until(page, false, x => x && x.tag && /queued/.test(x.tag), 9000);
      check(true, "(back in the line: " + JSON.stringify(t2 && t2.tag) + ")");
      THREADS[1].sendQueueProblem = false;
      await ctx.close();
    }

    // ── 3. May have gone: never "sent", never "failed"; sending again asks first ──
    {
      const id = IDS.C;
      THREADS[2].sendQueueProblem = true;
      setQueue(id, [qitem(id, "q_2003", { st: "needs_attention", tp: "We will start on it tomorrow.", text: "We will start on it tomorrow.", why: "The Etsy helper clicked Send and then stopped answering. It may have gone: check the conversation on Etsy.", code: "STRANDED_POST_CLICK" })]);
      const { ctx, page } = await open(false);
      await pick(page, id);
      const b = await barUntil(page, false, x => !!x, 9000);
      check(b && /Check Etsy: it may have gone\./.test(b.text) && /stopped answering/.test(b.text) && /Only send it again if the customer did not get it/.test(b.text), "the bar says check Etsy, why, and the warning about sending twice: " + (b && b.text));
      check(b && ["It was sent", "Send again", "Copy message", "Discard"].every(x => b.buttons.includes(x)), "four choices: " + (b && b.buttons.join(" | ")));
      check(b && /sent_text_only/.test(b.cls), "drawn as a caution (amber), not as an error");
      let asked = 0, answer = false;
      page.on("dialog", d => { asked++; answer ? d.accept() : d.dismiss(); });
      await page.click("#emQueueStatus [data-sq=retry]");
      await page.waitForTimeout(600);
      check(asked === 1 && count("etsyMailDraftSend", c => c.b.op === "queue_retry" && c.b.sendId === "q_2003") === 0, "Send again asks first, and No sends nothing");
      await page.click("#emQueueStatus [data-sq=sent]");
      await page.waitForTimeout(600);
      check(asked === 2 && count("etsyMailDraftSend", c => c.b.op === "queue_mark_sent") === 0, "It was sent asks first, and No changes nothing");
      answer = true;
      await page.click("#emQueueStatus [data-sq=sent]");
      await page.waitForTimeout(800);
      check(count("etsyMailDraftSend", c => c.b.op === "queue_mark_sent" && c.b.sendId === "q_2003") === 1, "a Yes tells the queue it went (one call)");
      THREADS[2].sendQueueProblem = false;
      await ctx.close();
    }

    // ── 4. Discard puts the words back in the reply box ──
    {
      const id = IDS.D;
      THREADS[3].sendQueueProblem = true;
      setQueue(id, [qitem(id, "q_2004", { st: "failed", tp: "Thank you for waiting!", text: "Thank you for waiting!", why: "Etsy's page would not take the message.", code: "DOM_SEND_BUTTON_MISS" })]);
      const { ctx, page } = await open(false);
      await pick(page, id);
      await barUntil(page, false, x => !!x, 9000);
      await page.click("#emQueueStatus [data-sq=dismiss]");
      await page.waitForFunction(() => document.getElementById("emDraftText").value.includes("Thank you for waiting!"), null, { timeout: 8000 }).catch(() => {});
      const v = await page.evaluate(() => document.getElementById("emDraftText").value);
      check(count("etsyMailDraftSend", c => c.b.op === "queue_dismiss" && c.b.sendId === "q_2004") === 1 && v.includes("Thank you for waiting!"), "Discard puts the message away and gives the words back in the reply box");
      THREADS[3].sendQueueProblem = false;
      await ctx.close();
    }

    // ── 5. The thread list shows the queue's flags ──
    {
      THREADS[0].sendQueueState = "queued"; THREADS[1].sendQueueProblem = true;
      const { ctx, page } = await open(false);
      await page.waitForSelector(`[data-id="${IDS.A}"]`, { timeout: 15000 });
      await page.waitForTimeout(800);
      const rows = await page.evaluate((ids) => Object.fromEntries(ids.map(id => { const r = document.querySelector(`.em-list-item[data-id="${id}"]`); return [id, r ? [...r.querySelectorAll(".em-sc, .em-badge")].map(x => x.textContent.trim()).join("|") : null]; })), [IDS.A, IDS.B, IDS.E]);
      check(/Sending…/.test(rows[IDS.A] || ""), "a conversation with a message in the queue shows \"Sending…\" in the list: " + rows[IDS.A]);
      check(/Send problem/.test(rows[IDS.B] || ""), "one with a message that needs a person shows \"Send problem\": " + rows[IDS.B]);
      check(!/Sending|Send problem/.test(rows[IDS.E] || ""), "a quiet conversation shows neither: " + rows[IDS.E]);
      THREADS[0].sendQueueState = undefined; THREADS[1].sendQueueProblem = false;
      await ctx.close();
    }

    // ── 5b. A sandbox conversation has no place in the real queue: no chip, no look at the queue, no bar ──
    {
      const id = IDS.D;
      THREADS[3].sandbox = true; THREADS[3].sendQueueState = "queued"; THREADS[3].sendQueueProblem = true;
      setQueue(id, [qitem(id, "q_real_2004", { st: "failed", tp: "A real message", text: "A real message", why: "Must not be shown on a sandbox conversation." })]);
      const { ctx, page } = await open(false);
      await page.waitForSelector(`[data-id="${id}"]`, { timeout: 15000 });
      await page.waitForTimeout(800);
      const row = await page.evaluate((id) => { const r = document.querySelector(`.em-list-item[data-id="${id}"]`); return r ? [...r.querySelectorAll(".em-sc, .em-badge")].map(x => x.textContent.trim()).join("|") : null; }, id);
      check(!/Sending|Send problem/.test(row || ""), "a sandbox conversation shows no queue chip in the list: " + JSON.stringify(row));
      const q0 = calls.length;
      await pick(page, id);
      await page.waitForTimeout(6000);
      check(calls.slice(q0).filter(c => c.name === "etsyMailDraftSend" && c.q.op === "queue_state").length === 0, "opening it never looks at the real queue");
      check((await bar(page, false)) === null, "and no bar appears for it");
      THREADS[3].sandbox = false; THREADS[3].sendQueueState = undefined; THREADS[3].sendQueueProblem = false;
      setQueue(id, [], []);
      await ctx.close();
    }

    // ── 6. The phone ──
    {
      const id = IDS.E;
      THREADS[4].sendQueueProblem = true;
      setQueue(id, [qitem(id, "q_2005", { st: "failed", tp: "Hello again", text: "Hello again", why: "The Etsy helper did not answer after 4 tries. Nothing was sent." })]);
      const { ctx, page, errors } = await open(true);
      await page.waitForSelector(`.m-thread-row[data-id="${id}"]`, { timeout: 15000 });
      await page.click(`.m-thread-row[data-id="${id}"]`);
      await page.waitForSelector("#mComposerTextarea", { timeout: 10000 });
      const b = await barUntil(page, true, x => !!x, 9000);
      check(b && /Not sent\./.test(b.text) && b.buttons.includes("Try again"), "phone: the same bar, with the reason and Try again: " + (b && b.text));
      check(errors.length === 0, "no page errors (phone)" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
      THREADS[4].sendQueueProblem = false;
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
