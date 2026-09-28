// A reply typed but not sent stays with its conversation (etsy-mail-1.html):
// switching to another conversation and back, a redraw of the open one, or a
// reload puts it back in the box. It never shows in another conversation, is
// never sent anywhere, and goes once that reply is sent or the box is emptied. An AI
// draft left untouched is not pinned (a newer draft still shows), and a
// Polished reply keeps its Polished mark.
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   node tests/etsy-mail/adv-inbox-typed.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no
// AI, nothing sent); every other request is aborted.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const NOW = Date.now();
const A = "t_typedA", B = "t_typedB";
const thread = (id, name, ago) => ({
  id, customerName: name, status: "etsy_scraped", unread: false,
  lastInboundAt: { _ts: true, ms: NOW - ago }, awaitingReplySince: { _ts: true, ms: NOW - ago },
  updatedAt: { _ts: true, ms: NOW - ago }, etsyConversationUrl: "https://example.invalid/c/" + id
});
const THREADS = [thread(A, "Ann Typed", 3600e3), thread(B, "Ben Other", 7200e3)];
const msgs = id => [0, 1].map(i => ({
  id: id + "m" + i, direction: i ? "outbound" : "inbound", senderName: i ? "CustomBrites" : "Customer",
  text: i ? "Thanks, we will check." : "When does my order ship?",
  timestamp: { _ts: true, ms: NOW - (9 - i) * 3600e3 }, createdAt: { _ts: true, ms: NOW - (9 - i) * 3600e3 }
}));
let aiDraftB = "AI draft one for Ben";   // the saved AI draft for B (untouched by staff)
const calls = [];
function fake(method, url, body) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  calls.push({ name, method, q, b, raw: url + " " + (body || "") });
  if (name === "firestoreProxy") {
    if (q.op === "list" && q.coll === "EtsyMail_Threads") return { docs: THREADS };
    if (q.op === "listSub") return { docs: msgs(q.id) };
    if (q.op === "get" && q.coll === "EtsyMail_Drafts" && q.id === "draft_" + B) {
      return { exists: true, doc: { threadId: B, status: "draft", text: aiDraftB, createdAt: { _ts: true, ms: Date.now() } } };
    }
    if (q.op === "get") return { exists: false };
    return { ok: true };
  }
  if (name === "etsyMailAuth") return { ok: true, username: "paul", displayName: "Paul", role: "owner" };
  if (name === "etsyMailPolish") return { ok: true, text: "Hi Ann,\n\nYour order ships tomorrow.\n\nMany Thanks,\nCustomBrites" };
  if (name === "etsyMailDraftSend" && method === "POST" && b.op === "enqueue") {
    return { ok: true, draftId: "draft_" + b.threadId, attachments: b.attachments || [], text: b.text };
  }
  if (name === "etsyMailDraftSend") return { ok: true, draft: { status: "queued" } };
  return { ok: true };
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    let body = "";
    req.on("data", d => { body += d; });
    req.on("end", () => { res.writeHead(200, { "Content-Type": "application/json" }); res.end(JSON.stringify(fake(req.method, req.url, body))); });
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
  try {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    await ctx.route(u => !/^http:\/\/127\.0\.0\.1[:/]/.test(u.href), r => r.abort());
    await ctx.addInitScript(() => {
      try {
        localStorage.setItem("etsymail_session", "tok");
        localStorage.setItem("etsymail_session_profile", JSON.stringify({ username: "paul", displayName: "Paul", role: "owner" }));
      } catch (_) {}
    });
    const page = await ctx.newPage();
    const errors = [];
    page.on("pageerror", e => errors.push(String(e)));
    const url = `http://127.0.0.1:${server.address().port}/etsy-mail-1.html`;
    const open = async id => {
      await page.click(`[data-id="${id}"]`);
      await page.waitForFunction(id => { const b = document.querySelector(`[data-id="${id}"]`); return b && b.classList.contains("active") && document.getElementById("emDraftText"); }, id, { timeout: 10000 }).catch(() => {});
      await page.waitForTimeout(700);
    };
    const box = () => page.inputValue("#emDraftText");
    await page.goto(url);
    await page.waitForSelector(`[data-id="${A}"]`, { timeout: 15000 });

    // ── An untouched AI draft is not pinned: a newer one still shows ──
    await open(B);
    check(await box() === "AI draft one for Ben", "B's saved AI draft fills its empty box");
    await open(A);
    check(await box() === "", "A's box starts empty (B's AI draft does not follow)");
    aiDraftB = "AI draft two for Ben";
    await open(B);
    check(await box() === "AI draft two for Ben", "back on B, the newer AI draft shows (untouched AI text is not kept over it)");

    // ── Typed text stays with its own conversation ──
    const typedA = "Hi Ann, your ring is being engraved now";
    const typedB = "Ben: the chain is 18 inches";
    await open(A);
    await page.fill("#emDraftText", typedA);
    await open(B);
    check(await box() !== typedA && !(await box()).includes("Ann"), "A's typed reply does not show in B");
    await page.fill("#emDraftText", typedB);
    await open(A);
    check(await box() === typedA, "back on A, its typed reply is in the box: " + JSON.stringify((await box()).slice(0, 40)));
    await open(B);
    check(await box() === typedB, "back on B, its own typed reply is in the box (not the AI draft, not A's)");

    // ── A redraw of the open conversation keeps it ──
    await page.fill("#emDraftText", typedB + " and ships Friday");
    await page.evaluate(() => window.emRerenderOpenThread && window.emRerenderOpenThread());
    await page.waitForTimeout(500);
    check(await box() === typedB + " and ships Friday", "a redraw of the open conversation keeps the typed reply");

    // ── A reload keeps it too (this tab only) ──
    await open(A);
    await page.reload();
    await page.waitForSelector(`[data-id="${A}"]`, { timeout: 15000 });
    await open(B);
    check(await box() === typedB + " and ships Friday", "after a reload, B's typed reply is back");
    await open(A);
    check(await box() === typedA, "after a reload, A's typed reply is back");

    // ── Never sent anywhere before Send ──
    const leaked = calls.filter(c => c.raw.includes("ring is being engraved") || c.raw.includes("chain is 18 inches"));
    check(leaked.length === 0, "no request carried the typed text before Send (" + leaked.map(c => c.name + ":" + (c.b.op || c.q.op || "")).join(",") + ")");

    // ── Sent: it goes ──
    calls.length = 0;
    await page.click("#emSendEtsyBtn");
    await page.waitForTimeout(1200);
    const enq = calls.find(c => c.name === "etsyMailDraftSend" && c.b && c.b.op === "enqueue");
    check(enq && enq.b.threadId === A && enq.b.text === typedA, "Send queues A's typed reply for A");
    // (A has left the waiting list now, so check its kept copy directly.)
    check(await page.evaluate(id => sessionStorage.getItem("em.typed." + id), A) === null, "after A's reply was sent, its kept copy is gone");
    check(await box() === "", "and A's box is empty");
    await page.reload();
    await page.waitForSelector(`[data-id="${A}"]`, { timeout: 15000 });
    await open(A);
    check(await box() === "", "and it does not come back after a reload");

    // ── Emptied by hand: it goes ──
    await open(B);
    check(await box() === typedB + " and ships Friday", "B's typed reply still waits in B");
    await page.fill("#emDraftText", "");
    await open(A);
    await open(B);
    check(await box() === "AI draft two for Ben", "after the box was emptied, B's typed reply is gone (the saved AI draft shows again)");

    // ── Polished words keep their Polished mark ──
    await open(A);
    await page.fill("#emDraftText", "hi ann ur order ships tmrw");
    await page.click("#emPolishBtn");
    await page.waitForSelector("#emPolishNote", { timeout: 5000 });
    const polished = await box();
    await open(B);
    await open(A);
    check(await box() === polished, "back on A, the polished reply is in the box");
    check(!!(await page.$("#emPolishNote")), "and its Polished · Undo note is back with it");
    calls.length = 0;
    await page.click("#emSendEtsyBtn");
    await page.waitForTimeout(1200);
    const enq2 = calls.find(c => c.name === "etsyMailDraftSend" && c.b && c.b.op === "enqueue");
    check(enq2 && enq2.b.polished === true, "a sent polished reply is still marked polished, so it is not learned as staff wording");

    check(errors.length === 0, "no page errors" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
  } finally {
    await browser.close();
    server.close();
  }
  if (fails.length) { console.log("\n" + fails.length + " failed"); process.exit(1); }
  console.log("\nall passed");
})().catch(e => { console.error(e); process.exit(1); });
