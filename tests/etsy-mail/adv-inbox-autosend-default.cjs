// Auto-send stays OFF unless the owner saved it ON (etsy-mail-1.html and
// netlify/functions/etsyMailAutoPipeline-background.js). A missing settings
// record, a record without the auto-send fields, or a failed read must never
// turn auto-send on. An explicit saved "on" still auto-sends.
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   node tests/etsy-mail/adv-inbox-autosend-default.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no
// AI, nothing sent); every other request is aborted. The server check runs
// the pipeline's config reader against a fake Firestore.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path"), vm = require("vm");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const NOW = Date.now();
const T = "t_autoDef";
const THREADS = [{
  id: T, customerName: "Cara Default", status: "etsy_scraped", unread: false,
  lastInboundAt: { _ts: true, ms: NOW - 3600e3 }, awaitingReplySince: { _ts: true, ms: NOW - 3600e3 },
  updatedAt: { _ts: true, ms: NOW - 3600e3 }, etsyConversationUrl: "https://example.invalid/c/" + T
}];
const MSGS = [{
  id: T + "m0", direction: "inbound", senderName: "Customer", text: "When does my order ship?",
  timestamp: { _ts: true, ms: NOW - 3600e3 }, createdAt: { _ts: true, ms: NOW - 3600e3 }
}];

// cfgMode: "missing" | "fail" | "empty" | "on"
let cfgMode = "missing";
const calls = [];
async function fake(method, url, body) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  calls.push({ name, method, q, b });
  if (name === "firestoreProxy") {
    if (q.op === "list" && q.coll === "EtsyMail_Threads") return { docs: THREADS };
    if (q.op === "listSub") return { docs: MSGS };
    if (q.op === "get" && q.coll === "EtsyMail_Config" && q.id === "autoPipeline") {
      if (cfgMode === "fail") return { __status: 500, error: "firestore unavailable" };
      if (cfgMode === "empty") return { exists: true, doc: { threshold: 0.8 } };
      if (cfgMode === "on") return { exists: true, doc: { enabled: true, manualAiDraftAutoSend: true, threshold: 0.8 } };
      return { exists: false };
    }
    if (q.op === "get") return { exists: false };
    return { ok: true };
  }
  if (name === "etsyMailAuth") return { ok: true, username: "paul", displayName: "Paul", role: "owner" };
  if (name === "etsyMailDraftReply") {
    return { ok: true, text: "Hi Cara, your order ships Friday.", aiConfidence: 0.99, autoSendBlockers: [] };
  }
  if (name === "etsyMailDraftSend") return { ok: true, draft: { status: "queued" } };
  return { ok: true };
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    let body = "";
    req.on("data", d => { body += d; });
    req.on("end", async () => {
      const out = await fake(req.method, req.url, body);
      const status = out && out.__status ? out.__status : 200;
      res.writeHead(status, { "Content-Type": "application/json" });
      res.end(JSON.stringify(out));
    });
    return;
  }
  const f = u === "/etsy-mail-1.html" && process.env.INBOX_PAGE ? path.resolve(process.env.INBOX_PAGE) : path.join(root, u);
  if ((!f.startsWith(root) && f !== path.resolve(process.env.INBOX_PAGE || "-")) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "Content-Type": "text/html" });
  fs.createReadStream(f).pipe(res);
}).listen(0);

// The pipeline's config reader, run against a fake Firestore.
async function serverEnabled(mode) {
  const file = process.env.PIPELINE_FN || path.join(root, "netlify/functions/etsyMailAutoPipeline-background.js");
  const src = fs.readFileSync(file, "utf8");
  const consts = src.match(/const FALLBACK_THRESHOLD[^\n]*\nconst FALLBACK_ENABLED[^\n]*\n/)[0];
  const start = src.indexOf("let _autoCfgCache");
  const end = src.indexOf("\n}\n", src.indexOf("async function getAutoPipelineConfig")) + 3;
  const doc = {
    missing: { exists: false, data: () => undefined },
    empty: { exists: true, data: () => ({ threshold: 0.8 }) },
    on: { exists: true, data: () => ({ enabled: true, threshold: 0.8 }) }
  }[mode];
  const db = { collection: () => ({ doc: () => ({ get: async () => {
    if (mode === "fail") throw new Error("firestore unavailable");
    return doc;
  } }) }) };
  const ctx = vm.createContext({ db, CONFIG_COLL: "EtsyMail_Config", console: { warn() {} }, Date, Math, Array });
  vm.runInContext(consts + src.slice(start, end) + "\nthis.get = getAutoPipelineConfig;", ctx);
  return (await ctx.get()).enabled;
}

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const fails = [];
  const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
  try {
    // ── Server: the pipeline's reading of the setting ──
    for (const [mode, label] of [["missing", "record missing"], ["fail", "read fails"], ["empty", "record without the field"]]) {
      check(await serverEnabled(mode) === false, "server, " + label + ": auto-pipeline auto-send is OFF");
    }
    check(await serverEnabled("on") === true, "server, owner saved ON: auto-pipeline auto-send is ON");

    // ── Page: AI Draft with a 99%-confidence, veto-free reply ──
    const ctx = await browser.newContext({ viewport: { width: 1400, height: 900 } });
    await ctx.route(u => !/^http:\/\/127\.0\.0\.1[:/]/.test(u.href), r => r.abort());
    await ctx.addInitScript(() => {
      try {
        localStorage.setItem("etsymail_session", "tok");
        localStorage.setItem("etsymail_session_profile", JSON.stringify({ username: "paul", displayName: "Paul", role: "owner" }));
      } catch (_) {}
    });
    const url = `http://127.0.0.1:${server.address().port}/etsy-mail-1.html`;
    const errors = [];
    const run = async (mode) => {
      cfgMode = mode;
      const page = await ctx.newPage();
      page.on("pageerror", e => errors.push(mode + ": " + String(e)));
      page.on("dialog", d => d.dismiss().catch(() => {}));
      await page.goto(url);
      await page.waitForSelector(`[data-id="${T}"]`, { timeout: 15000 });
      await page.click(`[data-id="${T}"]`);
      await page.waitForSelector("#emDraftText", { timeout: 10000 });
      await page.waitForTimeout(600);
      calls.length = 0;
      await page.click("#emAiDraftBtn");
      await page.waitForFunction(() => /Cara, your order ships Friday/.test((document.getElementById("emDraftText") || {}).value || ""), null, { timeout: 10000 }).catch(() => {});
      await page.waitForTimeout(1200);
      const box = await page.inputValue("#emDraftText").catch(() => "");
      const drafted = calls.some(c => c.name === "etsyMailDraftReply");
      const enqueued = calls.some(c => c.name === "etsyMailDraftSend" && c.b && c.b.op === "enqueue");
      await page.close();
      return { drafted, enqueued, box };
    };
    for (const [mode, label] of [["missing", "record missing"], ["fail", "read fails"], ["empty", "record without the fields"]]) {
      const r = await run(mode);
      check(r.drafted, "page, " + label + ": the AI draft was made (fake)");
      check(!r.enqueued, "page, " + label + ": the draft was NOT queued to send");
      check(r.box.includes("Cara, your order ships Friday"), "page, " + label + ": the draft waits in the reply box");
    }
    const on = await run("on");
    check(on.enqueued, "page, owner saved ON: the draft is queued to send (to the fake)");
    check(errors.length === 0, "no page errors" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
  } finally {
    await browser.close();
    server.close();
  }
  if (fails.length) { console.log("\n" + fails.length + " failed"); process.exit(1); }
  console.log("\nall passed");
})().catch(e => { console.error(e); process.exit(1); });
