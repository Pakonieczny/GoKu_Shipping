// A reply's bubble in the inbox (etsy-mail-1.html) must follow the send, not
// the scrape. Etsy's own copy of a reply is stored only when the conversation
// is next scraped, and nothing asks for a scrape after a send, so the bubble
// used to read "syncing…" for as long as it took the customer to write again,
// long after the extension had delivered the reply (Alice Viotti, 2026-09-29).
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   node tests/etsy-mail/adv-inbox-send-tag.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no
// AI, nothing sent to anyone); every other request is aborted. INBOX_PAGE=<file>
// serves another copy of the page (to show the bug on the page before the fix).
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const T0 = Date.now();
const ts = ms => ({ _ts: true, ms });
const mkThread = (id, name) => ({
  id, customerName: name, status: "etsy_scraped", unread: false,
  lastInboundAt: ts(T0 - 3600e3), awaitingReplySince: ts(T0 - 3600e3), updatedAt: ts(T0 - 3600e3),
  etsyConversationUrl: "https://example.invalid/c/" + id
});
const IDS = { A: "etsy_conv_1001", B: "etsy_conv_1002", C: "etsy_conv_1003", D: "etsy_conv_1004", E: "etsy_conv_1005", F: "etsy_conv_1006" };
const THREADS = Object.entries(IDS).map(([k, id], i) => Object.assign(mkThread(id, "Cust " + k), { lastInboundAt: ts(T0 - (3600e3 + i * 60e3)) }));
const baseMessages = id => [
  { id: id + "_m1", direction: "inbound", senderName: "Cust", senderRole: "customer", text: "Hi, thank you so much for the update!",
    timestamp: ts(T0 - 7200e3), createdAt: ts(T0 - 7200e3) },
  { id: id + "_m2", direction: "outbound", senderName: "CustomBrites", senderRole: "staff", source: "etsy", text: "You are most welcome, have a lovely day.",
    timestamp: ts(T0 - 7000e3), createdAt: ts(T0 - 7000e3) }
];
// Per-thread world: the messages the fake returns, and the thread's draft slot.
const world = {};
for (const id of Object.values(IDS)) world[id] = { messages: baseMessages(id), draft: null };
const calls = [];

function fake(method, url, body) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  calls.push({ name, method, q, b });
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
    const now = Date.now();
    w.draft = { id: "draft_" + b.threadId, threadId: b.threadId, text: b.text, status: "queued", queuedAt: ts(now) };
    // The server's stand-in for the reply (optim_draft_<thread>), as buildOptimisticDoc writes it.
    w.messages = w.messages.filter(m => m.id !== "optim_draft_" + b.threadId).concat({
      id: "optim_draft_" + b.threadId, direction: "outbound", source: "owner", senderName: "Paul", senderRole: "shop_owner",
      text: b.text, timestamp: ts(now), createdAt: ts(now), localOptimistic: true, optimisticDraftId: "draft_" + b.threadId,
      optimisticTextKey: String(b.text).trim().toLowerCase().slice(0, 200)
    });
    return { __delay: 150, ok: true, draftId: "draft_" + b.threadId, attachments: b.attachments || [], text: b.text };
  }
  if (name === "etsyMailDraftSend" && q.op === "status") {
    const w = world[String(q.draftId).replace(/^draft_/, "")];
    return w && w.draft ? { ok: true, draft: w.draft } : { __status: 404, error: "Draft not found" };
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
  const f = u === "/etsy-mail-1.html" && process.env.INBOX_PAGE ? path.resolve(process.env.INBOX_PAGE) : path.join(root, u);
  if ((!f.startsWith(root) && f !== path.resolve(process.env.INBOX_PAGE || "-")) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
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
  // The tag text on the newest stand-in bubble ("" when it reads like any other message).
  const standIn = (page, mobile) => page.evaluate((mobile) => {
    const sel = mobile ? "#mMessagesList .m-msg-wrap[data-mid]" : "#emThreadBox [data-mid]";
    const all = Array.from(document.querySelectorAll(sel)).filter(n => n.getAttribute("data-mid") && !/_m\d$/.test(n.getAttribute("data-mid")));
    const last = all[all.length - 1];
    if (!last) return null;
    const meta = last.querySelector(mobile ? ".m-msg-meta" : ".em-meta");
    const tag = mobile ? (meta.textContent.split("·").slice(2).join("·").trim()) : ((meta.querySelector(".em-msg-sync") || {}).textContent || "").replace(/^·\s*/, "").trim();
    return { mid: last.getAttribute("data-mid"), tag, meta: meta.textContent.trim(), count: all.length, text: (last.querySelector(mobile ? ".m-msg-text" : ".em-msg-text") || {}).textContent };
  }, mobile);
  const until = async (page, mobile, pred, ms) => {
    const end = Date.now() + ms; let s = null;
    while (Date.now() < end) { s = await standIn(page, mobile); if (pred(s)) return s; await page.waitForTimeout(250); }
    return s;
  };
  const noSyncingWord = page => page.evaluate(() => !/syncing/i.test(document.body.innerText));
  // The bubble reads "sending…" the moment Send is pressed, a few ms before the request reaches the server: wait for it before
  // the fake extension "delivers" it (otherwise the delivery can be overwritten by the queueing that arrives a moment later).
  const landed = async id => { for (let i = 0; i < 100 && !(world[id].draft && world[id].draft.text); i++) await new Promise(r => setTimeout(r, 20)); };

  try {
    // ── 1. Desktop: send a reply, the extension delivers it, no scrape ever comes ──
    {
      const { ctx, page, errors } = await open(false);
      await page.waitForSelector(`[data-id="${IDS.A}"]`, { timeout: 15000 });
      await page.click(`[data-id="${IDS.A}"]`);
      await page.waitForSelector("#emDraftText", { timeout: 10000 });
      await page.waitForFunction(() => document.querySelectorAll("#emThreadBox [data-mid]").length >= 2, null, { timeout: 10000 });
      await page.fill("#emDraftText", "You're welcome, Alice!\n\nMany Thanks,\nCustomBrites");
      await page.click("#emSendEtsyBtn");
      let s = await until(page, false, x => x && /^local_/.test(x.mid) && x.tag === "sending…", 6000);
      check(s && s.tag === "sending…", "queued and not yet delivered: the bubble reads \"sending…\" (got " + JSON.stringify(s && s.tag) + ")");
      check(await noSyncingWord(page), "the page never says \"syncing\" while the reply is queued");
      await landed(IDS.A);
      world[IDS.A].draft = Object.assign({}, world[IDS.A].draft, { status: "sending" });
      await page.waitForTimeout(4500);
      s = await standIn(page, false);
      check(s && s.tag === "sending…", "the extension is typing it into Etsy: still \"sending…\"");
      world[IDS.A].draft = Object.assign({}, world[IDS.A].draft, { status: "sent", sentAt: ts(Date.now()) });
      s = await until(page, false, x => x && x.tag === "", 9000);
      check(s && s.tag === "", "delivered by the extension: the tag goes away by itself (got " + JSON.stringify(s && s.tag) + ")");
      check(s && s.count === 1 && /welcome, Alice/.test(s.text || ""), "the reply's bubble stays in the thread, once");
      // Minutes later, still no scrape: it must not go back to a waiting state.
      await page.waitForTimeout(9000);
      s = await standIn(page, false);
      check(s && s.tag === "" && await noSyncingWord(page), "long after delivery, with no copy from Etsy, it still reads like any message");
      // Etsy's copy arrives with the next scrape: typographic apostrophe, a time
      // the scraper had to place days back, but written after the send.
      world[IDS.A].messages = world[IDS.A].messages.concat({
        id: "etsy_abc", direction: "outbound", senderName: "CustomBrites", senderRole: "staff", source: "etsy",
        text: "You’re welcome, Alice!\n\nMany Thanks,\nCustomBrites", timestamp: ts(T0 - 7000e3 + 1), createdAt: ts(Date.now()), timestampSource: "placed"
      });
      s = await until(page, false, x => x && x.count === 1 && /^etsy_/.test(x.mid), 9000);
      check(s && s.count === 1 && /^etsy_/.test(s.mid), "Etsy's copy replaces the stand-in, one bubble, even with curly quotes and a placed time (got " + JSON.stringify(s && { mid: s.mid, count: s.count }) + ")");
      check(errors.length === 0, "no page errors (desktop send)" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
      await ctx.close();
    }

    // ── 2. A reply left stuck by the old page: sent long ago, no scrape since ──
    {
      const w = world[IDS.B], at = Date.now() - 6 * 60e3;
      w.draft = { id: "draft_" + IDS.B, threadId: IDS.B, text: "You're welcome, Alice!", status: "sent", queuedAt: ts(at), sentAt: ts(at + 60e3) };
      w.messages = w.messages.concat({ id: "optim_draft_" + IDS.B, direction: "outbound", source: "owner", senderName: "Paul", senderRole: "shop_owner",
        text: "You're welcome, Alice!", timestamp: ts(at), createdAt: ts(at), localOptimistic: true, optimisticDraftId: "draft_" + IDS.B, optimisticTextKey: "you're welcome, alice!" });
      const { ctx, page, errors } = await open(false);
      await page.waitForSelector(`[data-id="${IDS.B}"]`, { timeout: 15000 });
      await page.click(`[data-id="${IDS.B}"]`);
      await page.waitForFunction(() => document.querySelectorAll("#emThreadBox [data-mid]").length >= 3, null, { timeout: 10000 });
      const s = await until(page, false, x => x && x.tag === "", 4000);
      check(s && /^optim_/.test(s.mid) && s.tag === "", "a stand-in the extension delivered minutes ago reads like any message when the thread opens");
      check(await noSyncingWord(page), "and the page does not say \"syncing\" anywhere");
      check(errors.length === 0, "no page errors (opened thread)");
      await ctx.close();
    }

    // ── 3. A stand-in for a send from another device: in flight, then delivered ──
    {
      const w = world[IDS.C], at = Date.now() - 5e3;
      w.draft = { id: "draft_" + IDS.C, threadId: IDS.C, text: "Your order ships tomorrow.", status: "queued", queuedAt: ts(at) };
      w.messages = w.messages.concat({ id: "optim_draft_" + IDS.C, direction: "outbound", source: "owner", senderName: "Paul", senderRole: "shop_owner",
        text: "Your order ships tomorrow.", timestamp: ts(at), createdAt: ts(at), localOptimistic: true, optimisticDraftId: "draft_" + IDS.C, optimisticTextKey: "your order ships tomorrow." });
      const { ctx, page } = await open(false);
      await page.waitForSelector(`[data-id="${IDS.C}"]`, { timeout: 15000 });
      await page.click(`[data-id="${IDS.C}"]`);
      let s = await until(page, false, x => x && x.tag === "sending…", 8000);
      check(s && s.tag === "sending…", "a queued reply from another device reads \"sending…\"");
      w.draft = Object.assign({}, w.draft, { status: "sent", sentAt: ts(Date.now()) });
      s = await until(page, false, x => x && x.tag === "", 9000);
      check(s && s.tag === "", "and drops the tag once the extension has delivered it");
      await ctx.close();
    }

    // ── 4. Not delivered: say so instead of "syncing…" ──
    {
      const w = world[IDS.D], at = Date.now() - 8 * 60e3;
      w.draft = { id: "draft_" + IDS.D, threadId: IDS.D, text: "Sorry for the wait!", status: "failed", queuedAt: ts(at), sendErrorCode: "QUEUED_EXPIRED" };
      w.messages = w.messages.concat({ id: "optim_draft_" + IDS.D, direction: "outbound", source: "owner", senderName: "Paul", senderRole: "shop_owner",
        text: "Sorry for the wait!", timestamp: ts(at), createdAt: ts(at), localOptimistic: true, optimisticDraftId: "draft_" + IDS.D, optimisticTextKey: "sorry for the wait!" });
      const { ctx, page } = await open(false);
      await page.waitForSelector(`[data-id="${IDS.D}"]`, { timeout: 15000 });
      await page.click(`[data-id="${IDS.D}"]`);
      const s = await until(page, false, x => x && x.tag === "not sent", 6000);
      check(s && s.tag === "not sent", "a reply the extension could not send is tagged \"not sent\" (got " + JSON.stringify(s && s.tag) + ")");
      check(await page.evaluate(() => !!document.querySelector("#emThreadBox .em-msg-sync.warn")), "the note is styled as a warning");
      await ctx.close();
    }

    // ── 5. Clicked Send, Etsy never confirmed ──
    {
      const w = world[IDS.E], at = Date.now() - 3 * 60e3;
      w.draft = { id: "draft_" + IDS.E, threadId: IDS.E, text: "Thanks so much!", status: "sent_unverified", queuedAt: ts(at), sentAt: ts(at + 30e3) };
      w.messages = w.messages.concat({ id: "optim_draft_" + IDS.E, direction: "outbound", source: "owner", senderName: "Paul", senderRole: "shop_owner",
        text: "Thanks so much!", timestamp: ts(at), createdAt: ts(at), localOptimistic: true, optimisticDraftId: "draft_" + IDS.E, optimisticTextKey: "thanks so much!" });
      const { ctx, page } = await open(false);
      await page.waitForSelector(`[data-id="${IDS.E}"]`, { timeout: 15000 });
      await page.click(`[data-id="${IDS.E}"]`);
      const s = await until(page, false, x => x && x.tag === "not confirmed by Etsy", 6000);
      check(s && s.tag === "not confirmed by Etsy", "an unverified send says Etsy did not confirm it (got " + JSON.stringify(s && s.tag) + ")");
      await ctx.close();
    }

    // ── 6. The phone ──
    {
      const { ctx, page, errors } = await open(true);
      await page.waitForSelector(`.m-thread-row[data-id="${IDS.F}"]`, { timeout: 15000 });
      await page.click(`.m-thread-row[data-id="${IDS.F}"]`);
      await page.waitForSelector("#mComposerTextarea", { timeout: 10000 });
      await page.waitForFunction(() => document.querySelectorAll("#mMessagesList .m-msg-wrap[data-mid]").length >= 2, null, { timeout: 10000 });
      await page.fill("#mComposerTextarea", "You're welcome, Alice!");
      await page.evaluate(() => document.getElementById("mComposerSend").click());
      let s = await until(page, true, x => x && /^local_/.test(x.mid) && x.tag === "sending…", 6000);
      check(s && s.tag === "sending…", "phone: queued reply reads \"sending…\" (got " + JSON.stringify(s && s.meta) + ")");
      await landed(IDS.F);
      world[IDS.F].draft = Object.assign({}, world[IDS.F].draft, { status: "sent", sentAt: ts(Date.now()) });
      s = await until(page, true, x => x && x.tag === "", 10000);
      check(s && s.tag === "", "phone: delivered, the tag goes away by itself (got " + JSON.stringify(s && s.meta) + ")");
      check(await noSyncingWord(page), "phone: the page never says \"syncing\"");
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
