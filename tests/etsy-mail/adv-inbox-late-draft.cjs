// A result that comes back late for one conversation never lands in another
// conversation's reply box (etsy-mail-1.html). Staff open A, move to B, and
// then A's saved AI draft, A's AI Draft run, or A's custom-listing reply
// arrives: B's box, chips and notes stay B's; A's result waits for A.
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   node tests/etsy-mail/adv-inbox-late-draft.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no
// AI, nothing sent); every other request is aborted. The fake answers A's
// requests slowly so the switch to B happens while they are in flight.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const NOW = Date.now();
const A = "t_lateA", B = "t_lateB";
const SLOW = 1500;
const thread = (id, name, ago) => ({
  id, customerName: name, status: "etsy_scraped", unread: false,
  lastInboundAt: { _ts: true, ms: NOW - ago }, awaitingReplySince: { _ts: true, ms: NOW - ago },
  updatedAt: { _ts: true, ms: NOW - ago }, etsyConversationUrl: "https://example.invalid/c/" + id
});
const THREADS = [thread(A, "Ann Early", 3600e3), thread(B, "Ben Later", 7200e3)];
const msgs = id => [0, 1].map(i => ({
  id: id + "m" + i, direction: i ? "outbound" : "inbound", senderName: i ? "CustomBrites" : "Customer",
  text: i ? "Thanks, we will check." : "When does my order ship?",
  timestamp: { _ts: true, ms: NOW - (9 - i) * 3600e3 }, createdAt: { _ts: true, ms: NOW - (9 - i) * 3600e3 }
}));
// A's saved AI draft (what a parallel run left), with a tracking image chip.
let draftA = {
  threadId: A, status: "draft", generatedByAI: true, text: "Hi Ann, your order ships Tuesday (tracking 9400ANN).",
  createdAt: { _ts: true, ms: Date.now() },
  aiDiscountCode: { code: "ANNONLY10", percent: 10 },
  trackingImages: [{ trackingCode: "9400ANN", imageUrl: "https://example.invalid/t.png", status: "ready" }]
};
let listingA = null;   // set once the fake listing creator "finishes" for A
const calls = [];
async function fake(method, url, body) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  calls.push({ name, method, q, b, raw: url + " " + (body || "") });
  const slow = () => new Promise(r => setTimeout(r, SLOW));
  if (name === "firestoreProxy") {
    if (q.op === "list" && q.coll === "EtsyMail_Threads") return { docs: THREADS };
    if (q.op === "listSub") return { docs: msgs(q.id) };
    if (q.op === "get" && q.coll === "EtsyMail_Drafts" && q.id === "draft_" + A) {
      await slow();
      return draftA ? { exists: true, doc: draftA } : { exists: false };
    }
    if (q.op === "get" && q.coll === "EtsyMail_Threads" && q.id === A && listingA) {
      await slow();
      return { exists: true, doc: { ...THREADS[0], ...listingA } };
    }
    // Auto-send is off by design (Settings): the config says so.
    if (q.op === "get" && q.coll === "EtsyMail_Config" && q.id === "autoPipeline") {
      return { exists: true, doc: { enabled: false, manualAiDraftAutoSend: false, threshold: 0.8 } };
    }
    if (q.op === "get") return { exists: false };
    return { ok: true };
  }
  if (name === "etsyMailAuth") return { ok: true, username: "paul", displayName: "Paul", role: "owner" };
  if (name === "etsyMailDraftReply" && b.threadId === A) {
    await slow();
    const text = "AI reply for Ann: your ring ships Friday.";
    // The server saves the run's result as A's draft.
    draftA = { threadId: A, status: "draft", generatedByAI: true, text, createdAt: { _ts: true, ms: Date.now() } };
    return { ok: true, text, aiConfidence: 0.99, autoSendBlockers: [] };
  }
  if (name === "etsyMailListingCreator-background" && b.threadId === A) {
    listingA = {
      customListingStatus: "created", customListingUrl: "https://www.etsy.com/listing/555",
      customListingManualCreatedAt: { _ts: true, ms: Date.now() },
      customListingReplyText: "Ann, here is your custom listing: https://www.etsy.com/listing/555"
    };
    return { ok: true };
  }
  if (name === "etsyMailDraftSend") return { ok: true, draft: { status: "draft" } };
  return { ok: true };
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    let body = "";
    req.on("data", d => { body += d; });
    req.on("end", async () => {
      const out = await fake(req.method, req.url, body);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(out));
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
  try {
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    await ctx.route(u => !/^http:\/\/127\.0\.0\.1[:/]/.test(u.href), r => r.abort());
    await ctx.addInitScript(() => {
      try {
        localStorage.setItem("etsymail_session", "tok");
        localStorage.setItem("etsymail_session_profile", JSON.stringify({ username: "paul", displayName: "Paul", role: "owner" }));
      } catch (_) {}
      // Watch B's pane the whole time: anything of Ann's shown there, even
      // briefly (a redraw may wipe it a moment later), is recorded.
      window.__leaks = [];
      setInterval(() => {
        const act = document.querySelector('[data-id="t_lateB"].active, [data-id="t_lateB"].selected');
        if (!act) return;
        const ta = document.getElementById("emDraftText");
        const chips = document.getElementById("emComposerChips");
        const note = document.getElementById("emDiscountCodeNote");
        const seen = [ta && ta.value, chips && chips.innerHTML,
                      note && note.style.display !== "none" ? note.textContent : ""].join(" | ");
        if (/Ann|9400ANN|ANNONLY10|listing\/555/.test(seen)) window.__leaks.push(seen.slice(0, 120));
      }, 30);
    });
    const page = await ctx.newPage();
    const errors = [];
    page.on("pageerror", e => errors.push(String(e)));
    page.on("dialog", d => d.dismiss().catch(() => {}));
    const url = `http://127.0.0.1:${server.address().port}/etsy-mail-1.html`;
    const click = async id => {
      await page.click(`[data-id="${id}"]`);
      await page.waitForFunction(id => { const b = document.querySelector(`[data-id="${id}"]`); return b && (b.classList.contains("active") || b.classList.contains("selected")) && document.getElementById("emDraftText"); }, id, { timeout: 10000 });
    };
    const box = () => page.inputValue("#emDraftText");
    const chipsText = () => page.$eval("#emComposerChips", el => el.innerHTML).catch(() => "");
    const bPaneClean = async (what) => {
      const leaks = await page.evaluate(() => window.__leaks.splice(0));
      check(leaks.length === 0, what + ": nothing of A's ever showed in B's pane" + (leaks.length ? " (saw " + JSON.stringify(leaks[0]) + ")" : ""));
      const v = await box();
      check(v === "", what + ": B's reply box stays empty (was " + JSON.stringify(v.slice(0, 50)) + ")");
      const note = await page.$eval("#emDiscountCodeNote", el => el.style.display !== "none" && el.textContent.includes("ANNONLY10")).catch(() => false);
      check(!note, what + ": A's discount-code note is not shown under B");
    };
    await page.goto(url);
    await page.waitForSelector(`[data-id="${A}"]`, { timeout: 15000 });

    // ── 1. A's saved draft loads slowly; staff move to B before it arrives ──
    await click(A);
    await click(B);
    await page.waitForTimeout(SLOW + 1200);
    await bPaneClean("late saved draft for A");
    check(!(await chipsText()).includes("9400ANN"), "late saved draft for A: A's tracking image is not shown in B's attachments");
    check(!calls.some(c => c.raw.includes("draft_" + B) && c.raw.includes("9400ANN")), "late saved draft for A: A's tracking image is not saved onto B's draft");
    await click(A);
    await page.waitForTimeout(SLOW + 800);
    check((await box()).startsWith("Hi Ann, your order ships Tuesday"), "back on A, A's saved draft is in A's box");

    // ── 2. AI Draft for A comes back after staff moved to B ──
    calls.length = 0;
    await page.click("#emAiDraftBtn");
    await page.waitForTimeout(150);
    await click(B);
    await page.waitForTimeout(SLOW + 1200);
    await bPaneClean("late AI Draft for A");
    check(!calls.some(c => c.name === "etsyMailDraftReply" && c.b.threadId === B), "no AI draft was asked for B");
    check(!calls.some(c => c.name === "etsyMailDraftSend" && c.b && c.b.op === "enqueue"), "nothing was queued to send (auto-send path not taken)");
    await click(A);
    await page.waitForTimeout(SLOW + 800);
    check(await box() === "AI reply for Ann: your ring ships Friday.", "back on A, A's new AI reply is in A's box");

    // ── 3. A's custom-listing reply comes back after staff moved to B ──
    draftA = null;
    await click(B);
    await click(A);
    await page.waitForTimeout(SLOW + 500);
    await page.fill("#emDraftText", "");
    await page.click("#emCustomListingBtn");
    await page.waitForSelector("#emListingPriceAsk input", { timeout: 5000 });
    await page.fill("#emListingPriceAsk input", "42");
    await page.click('#emListingPriceAsk [data-act="go"]');
    await page.waitForTimeout(200);
    await click(B);
    await page.waitForTimeout(SLOW + 2500 + 1500);
    await bPaneClean("late custom-listing reply for A");
    await click(A);
    await page.waitForTimeout(SLOW + 800);
    check((await box()).includes("https://www.etsy.com/listing/555"), "back on A, A's listing reply is in A's box: " + JSON.stringify((await box()).slice(0, 60)));

    check(!calls.some(c => c.name === "etsyMailDraftSend" && c.b && c.b.op === "enqueue"), "nothing was sent to any customer");
    check(errors.length === 0, "no page errors" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
  } finally {
    await browser.close();
    server.close();
  }
  if (fails.length) { console.log("\n" + fails.length + " failed"); process.exit(1); }
  console.log("\nall passed");
})().catch(e => { console.error(e); process.exit(1); });
