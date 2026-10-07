// FC13 cost meter: what an open Inbox (etsy-mail-1.html) asks the server, and what that reads, per simulated hour.
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   [INBOX_PAGE=/path/to/another/copy.html] [TABS=3] [MINUTES=60] node tests/etsy-mail/cost-idle.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Firestore, no Etsy, no AI, nothing sent); every other
// request is aborted. Time is Playwright's fake clock, so an hour takes seconds. Reads are counted the way Firestore
// counts them (a document get is 1 read, a query is one read per document returned and at least 1); bytes are the
// length of the answer. Scenarios: A = signed in, nothing open; B = one conversation open; C = tab hidden all hour.
// It prints a table and always exits 0 (it measures, it does not judge); CHECK=1 turns the "after" expectations into
// pass/fail.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const MINUTES = Number(process.env.MINUTES || 60), TABS = Number(process.env.TABS || 1);
const NOW0 = Date.now();
const pad = (s, n) => String(s).padEnd(n);
const ts = ms => ({ _ts: true, ms });
const filler = n => ("customer asked about the engraving, the chain length and when the necklace ships; ").repeat(Math.ceil(n / 80)).slice(0, n);
const thread = (i, over) => Object.assign({
  id: "etsy_conv_" + (1000 + i), threadId: "etsy_conv_" + (1000 + i), customerName: "Customer " + i, etsyUsername: "cust" + i,
  status: i % 7 === 0 ? "pending_human_review" : "auto_replied", unread: false, subject: "Conversation with Customer " + i,
  etsyConversationUrl: "https://example.invalid/c/" + i, lastInboundAt: ts(NOW0 - i * 600e3), lastOutboundAt: ts(NOW0 - i * 600e3 - 60e3),
  awaitingReplySince: i % 3 === 0 ? ts(NOW0 - i * 600e3) : undefined, updatedAt: ts(NOW0 - i * 600e3), messageCount: 12,
  lastInboundPreview: "Hi, could you tell me when my necklace ships?", lastOutboundPreview: "Thanks, it ships tomorrow.",
  searchableText: filler(6000), searchableMessageText: filler(5800), riskFlags: [], tags: [], linkedListingIds: []
}, over || {});
const THREADS = Array.from({ length: 500 }, (_, i) => thread(i));
const OPEN = THREADS[1];
const MESSAGES = Array.from({ length: 40 }, (_, i) => ({
  id: "m" + i, direction: i % 2 ? "outbound" : "inbound", senderName: i % 2 ? "CustomBrites" : "Sue", text: filler(260),
  timestamp: ts(NOW0 - (41 - i) * 3600e3), createdAt: ts(NOW0 - (41 - i) * 3600e3), source: "etsy", imageUrls: [], listingCards: []
}));
const sites = {};
for (let i = 0; i < 25; i++) sites["site" + i + ".call"] = { attempt: 100 + i, ok: 99 + i, failHttp: 0, fail429: 0, failNet: 0, last60s: 0, lastAttemptAt: ts(NOW0 - i * 1000) };
const COUNTERS = { id: "etsyApiCounters", day: "2026-10-07", grandTotal: 2500, sites, updatedAt: ts(NOW0 - 5000) };

let calls = [];
function fake(method, url, body) {
  const u = new URL(url, "http://x"), name = u.pathname.split("/").pop(), q = Object.fromEntries(u.searchParams);
  let b = {}; try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  let out = { success: true, ok: true }, reads = 0;
  if (name === "firestoreProxy") {
    if (q.op === "get") {
      reads = 1;
      if (q.coll === "EtsyMail_Config" && q.id === "etsyApiCounters") out = { success: true, exists: true, doc: COUNTERS };
      else if (q.coll === "EtsyMail_Config" && q.id === "scrapeHealth") out = { success: true, exists: true, doc: { id: "scrapeHealth", consecutiveBad: 0, lastOkAtMs: NOW0 } };
      else if (q.coll === "EtsyMail_Config" && q.id === "autoPipeline") out = { success: true, exists: true, doc: { id: "autoPipeline", enabled: false, threshold: 0.9 } };
      else if (q.coll === "EtsyMail_Threads") out = { success: true, exists: true, doc: OPEN };
      else out = { success: true, exists: false, doc: null };
    } else if (q.op === "list" && q.coll === "EtsyMail_Threads") {
      let docs;
      if (q.since) docs = [];
      else if (/awaitingReplySince/.test(q.orderBy || "")) docs = THREADS.filter(t => t.awaitingReplySince);
      else if (q.where === "status,==,pending_human_review") docs = THREADS.filter(t => t.status === "pending_human_review");
      else if (q.where) docs = [];
      else docs = THREADS;
      reads = Math.max(1, docs.length); out = { success: true, docs };
    } else if (q.op === "listSub") {
      const docs = q.sub === "messages" ? (q.since ? [] : MESSAGES) : [];
      reads = Math.max(1, docs.length); out = { success: true, docs };
    }
  } else if (name === "etsyMailDraftSend") out = { success: true, killSwitch: { disabled: false } };
  else if (name === "etsyMailGmailConfig") { reads = 3; out = { ok: true, enabled: true, pullFromMs: null, syncState: { oauthSeeded: true, lastSyncCompletedAt: new Date(NOW0).toISOString(), lastSyncMessagesScanned: 4, lastSyncJobsEnqueued: 1 } }; }
  else if (name === "etsyMailAuth") out = { ok: true, username: "paul", displayName: "Paul", role: "owner" };
  else if (name === "etsyMailDraftReply") out = { ok: true, intent: "x", intentLabel: "Question", urgency: "low", flags: [] };
  const text = JSON.stringify(out);
  calls.push({ name, op: q.op || b.op || q.counts && "counts" || "", coll: q.coll || "", id: q.id || "", sub: q.sub || "", reads, bytes: text.length });
  return text;
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    let body = ""; req.on("data", d => { body += d; });
    req.on("end", () => { res.writeHead(200, { "Content-Type": "application/json" }); res.end(fake(req.method, req.url, body)); });
    return;
  }
  const f = u === "/etsy-mail-1.html" && process.env.INBOX_PAGE ? path.resolve(process.env.INBOX_PAGE) : path.join(root, u);
  if ((!f.startsWith(root) && f !== path.resolve(process.env.INBOX_PAGE || "-")) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "Content-Type": u.endsWith(".js") ? "text/javascript" : "text/html" });
  fs.createReadStream(f).pipe(res);
}).listen(0);

function summarize(label, list, hours) {
  const by = new Map();
  for (const c of list) {
    const k = c.name + (c.op ? " " + c.op : "") + (c.coll ? " " + c.coll.replace("EtsyMail_", "") : "") + (c.id && c.coll === "EtsyMail_Config" ? "/" + c.id : "") + (c.sub ? "/" + c.sub : "");
    const e = by.get(k) || { calls: 0, reads: 0, bytes: 0 }; e.calls++; e.reads += c.reads; e.bytes += c.bytes; by.set(k, e);
  }
  let tc = 0, tr = 0, tb = 0;
  console.log("\n== " + label + " (per hour; simulated " + (hours * 60).toFixed(0) + " min)");
  console.log(pad("call", 52) + pad("calls/h", 10) + pad("reads/h", 10) + "KB/h");
  for (const [k, e] of [...by.entries()].sort((a, b) => b[1].bytes - a[1].bytes)) {
    tc += e.calls; tr += e.reads; tb += e.bytes;
    console.log(pad(k, 52) + pad(Math.round(e.calls / hours), 10) + pad(Math.round(e.reads / hours), 10) + Math.round(e.bytes / hours / 1024));
  }
  console.log(pad("TOTAL", 52) + pad(Math.round(tc / hours), 10) + pad(Math.round(tr / hours), 10) + Math.round(tb / hours / 1024));
  return { calls: tc / hours, reads: tr / hours, kb: tb / hours / 1024 };
}

async function openTab(ctx, openThread) {
  const page = await ctx.newPage();
  page.on("pageerror", e => console.log("  pageerror:", String(e).slice(0, 160)));
  await page.goto(`http://127.0.0.1:${server.address().port}/etsy-mail-1.html`);
  await page.waitForSelector(`[data-id="${THREADS[0].id}"], #emListItems .em-list-item, #emListItems [data-id]`, { timeout: 20000 }).catch(() => {});
  if (openThread) {
    await page.waitForSelector(`[data-id="${OPEN.id}"]`, { timeout: 20000 });
    await page.click(`[data-id="${OPEN.id}"]`);
    await page.waitForSelector("#emDraftText", { timeout: 10000 });
  }
  return page;
}
async function run(browser, label, { openThread, hidden, locked }) {
  const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
  await ctx.route(u => !/^http:\/\/127\.0\.0\.1[:/]/.test(u.href), r => r.abort());
  await ctx.addInitScript(() => {
    try {
      localStorage.setItem("etsymail_session", "tok");
      localStorage.setItem("etsymail_session_profile", JSON.stringify({ username: "paul", displayName: "Paul", role: "owner" }));
    } catch (_) {}
  });
  await ctx.clock.install({ time: new Date(NOW0) });
  const pages = [];
  const settle = async ms => { for (let t = 0; t < ms; t += 1000) { await ctx.clock.runFor(Math.min(1000, ms - t)); await new Promise(r => setTimeout(r, 4)); } };
  for (let i = 0; i < TABS; i++) {
    pages.push(await openTab(ctx, openThread && i === 0));
    await settle(i === 0 ? 4000 : 3300 + i * 1700);   // later tabs start offset, as real tabs do
  }
  if (hidden) for (const p of pages) await p.evaluate(() => {
    Object.defineProperty(document, "visibilityState", { configurable: true, get: () => "hidden" });
    Object.defineProperty(document, "hidden", { configurable: true, get: () => true });
    document.dispatchEvent(new Event("visibilitychange"));
  });
  // the sign-in screen, as after the idle sign-out: EtsyMailAuth.lock() (private to the page) first removes body.authed, which is what the polls read
  if (locked) for (const p of pages) await p.evaluate(() => { document.body.classList.remove("authed"); });
  await settle(5000);
  calls = [];                       // the page loads are counted separately; this is the steady state
  // the person is at the desk (scenarios A and B): one key press every 20 s keeps the idle sign-out away
  for (let t = 0; t < MINUTES * 60 * 1000; t += 20000) {
    // a real key press (the page only counts input a person made: isTrusted)
    if (!hidden && !locked) for (const p of pages) await p.keyboard.press("Shift");
    await settle(20000);
  }
  const steady = calls.slice(); calls = [];
  const stillIn = await pages[0].evaluate(() => document.body.classList.contains("authed"));
  const r = summarize(label + ", " + TABS + " tab(s)", steady, MINUTES / 60);
  r.signedInAtEnd = stillIn;
  console.log("  signed in at the end: " + stillIn + (locked ? " (expected: no)" : " (expected: " + (hidden ? "either" : "yes") + ")"));
  await ctx.close();
  return r;
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const out = {};
  try {
    out.A = await run(browser, "A. signed in, nothing open", { openThread: false, hidden: false });
    out.B = await run(browser, "B. one conversation open", { openThread: true, hidden: false });
    out.C = await run(browser, "C. tab hidden", { openThread: false, hidden: true });
    out.D = await run(browser, "D. sign-in screen showing (after a 5 pm sign-out), tab shown", { openThread: false, hidden: false, locked: true });
  } finally { await browser.close(); server.close(); }
  console.log("\nSUMMARY " + JSON.stringify(out));
  if (process.env.CHECK === "1") {
    const fails = [];
    // a low count only means something if the person was still signed in the whole hour
    if (!out.A.signedInAtEnd) fails.push("A: the page signed itself out during the hour, so its low count proves nothing");
    if (!out.B.signedInAtEnd) fails.push("B: the page signed itself out during the hour, so its low count proves nothing");
    // the person presses a key every 20 s here (worst case: at the desk all hour), so the Etsy meter reads every 10 s; the old page read 1,700 / 4,400 an hour
    if (out.A.reads > 750 * TABS) fails.push("A reads/h too high: " + out.A.reads);
    if (out.B.reads > 1300) fails.push("B reads/h too high: " + out.B.reads);
    if (out.C.reads > 5) fails.push("C reads/h while hidden: " + out.C.reads);
    if (out.D.reads > 5) fails.push("D reads/h on the sign-in screen: " + out.D.reads);
    if (fails.length) { console.log("FAIL " + fails.join("; ")); process.exit(1); }
    console.log("PASS");
  }
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
