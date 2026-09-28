// Adversarial checks of the inbox reply box (etsy-mail-1.html) in a real
// Chromium: the send flight is visible and smooth and moves only
// transform / opacity / clip-path; Polish and its Undo note; Clear.
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   node tests/etsy-mail/adv-inbox-ui.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no
// AI, nothing sent); every other request is aborted.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const NOW = Date.now();
const TID = "t_adv17";
const THREAD = {
  id: TID, customerName: "Sue Tester", status: "etsy_scraped", unread: false,
  lastInboundAt: { _ts: true, ms: NOW - 3600e3 }, awaitingReplySince: { _ts: true, ms: NOW - 3600e3 },
  updatedAt: { _ts: true, ms: NOW - 3600e3 }, etsyConversationUrl: "https://example.invalid/c/1"
};
const TID2 = "t_adv17b";
const THREAD2 = { ...THREAD, id: TID2, customerName: "Bob Other",
  lastInboundAt: { _ts: true, ms: NOW - 7200e3 }, awaitingReplySince: { _ts: true, ms: NOW - 7200e3 }, updatedAt: { _ts: true, ms: NOW - 7200e3 } };
const MESSAGES = [];
for (let i = 0; i < 40; i++) {
  MESSAGES.push({
    id: "m" + i, direction: i % 2 ? "outbound" : "inbound", senderName: i % 2 ? "CustomBrites" : "Sue",
    text: (i % 2 ? "Thanks for your message, we will check the engraving and the chain length for you today. " :
      "Hi, could you tell me when my necklace ships and whether the chain is 18 inches? ").repeat(1 + (i % 3)),
    timestamp: { _ts: true, ms: NOW - (41 - i) * 3600e3 }, createdAt: { _ts: true, ms: NOW - (41 - i) * 3600e3 }
  });
}
const calls = [];
let enqueueDelay = 400;
function fake(method, url, body) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  calls.push({ name, method, q, b });
  if (name === "firestoreProxy") {
    if (q.op === "list" && q.coll === "EtsyMail_Threads") return { docs: [THREAD, THREAD2] };
    if (q.op === "listSub") return { docs: q.id === TID2 ? MESSAGES.slice(0, 2).map(m => ({ ...m, id: "b" + m.id })) : MESSAGES };
    if (q.op === "get") return { exists: false };
    return { ok: true };
  }
  if (name === "etsyMailAuth") return { ok: true, username: "paul", displayName: "Paul", role: "owner" };
  if (name === "etsyMailPolish") return { ok: true, text: "Hi Sue,\n\nYour necklace ships tomorrow.\n\nMany Thanks,\nCustomBrites" };
  if (name === "etsyMailDraftSend" && method === "POST" && b.op === "enqueue") {
    return { __delay: enqueueDelay, ok: true, draftId: "draft_" + TID, attachments: b.attachments || [], text: b.text };
  }
  if (name === "etsyMailDraftSend") return { ok: true, draft: { status: "queued" } };
  return { ok: true };
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    let body = "";
    req.on("data", d => { body += d; });
    req.on("end", () => {
      const out = fake(req.method, req.url, body);
      const delay = out.__delay || 0; delete out.__delay;
      setTimeout(() => { res.writeHead(200, { "Content-Type": "application/json" }); res.end(JSON.stringify(out)); }, delay);
    });
    return;
  }
  // INBOX_PAGE=<file> serves another copy of the page (to show a fix's before and after).
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
    await page.goto(`http://127.0.0.1:${server.address().port}/etsy-mail-1.html`);
    await page.waitForSelector(`[data-id="${TID}"]`, { timeout: 15000 });
    await page.click(`[data-id="${TID2}"]`);
    await page.waitForSelector("#emDraftText", { timeout: 10000 });

    // ── 1. Polish: the Undo note and the "polished" mark belong to the
    //    words Polish wrote, not to whatever the box holds later ─────────
    await page.fill("#emDraftText", "hi bob ur necklace ships tmrw");
    await page.click("#emPolishBtn");
    await page.waitForSelector("#emPolishNote", { timeout: 5000 });
    const polished = await page.inputValue("#emDraftText");
    check(/ships tomorrow/.test(polished), "Polish puts the fake's polished words in the box");
    await page.click(`[data-id="${TID}"]`);
    await page.waitForTimeout(700);
    await page.click(`[data-id="${TID2}"]`);
    await page.waitForTimeout(900);
    const back = await page.inputValue("#emDraftText");
    const noteBack = !!(await page.$("#emPolishNote"));
    console.log("  after switching away and back: box =", JSON.stringify(back.slice(0, 40)), "note shown =", noteBack);
    check(!noteBack || back === polished, "the Polished · Undo note only shows over the polished words");
    await page.fill("#emDraftText", "Hi Bob, the chain is 18 inches and it ships Friday, sorry for the wait.");
    calls.length = 0;
    await page.click("#emSendEtsyBtn");
    await page.waitForTimeout(900);
    const enq = calls.find(c => c.name === "etsyMailDraftSend" && c.b && c.b.op === "enqueue");
    check(!!enq, "the reply was queued with the fake");
    check(enq && enq.b.polished === false, "a reply typed by staff (not the polished words) is not marked polished, so it is learned from");

    await page.waitForTimeout(1200);
    await page.click(`[data-id="${TID}"]`);
    await page.waitForSelector("#emDraftText", { timeout: 10000 });
    await page.waitForFunction(() => document.querySelectorAll("#emThreadBox [data-mid]").length >= 40, null, { timeout: 10000 });
    await page.waitForTimeout(600);
    // ── 2. The send flight: visible, smooth, compositor-only ─────────────
    await page.fill("#emDraftText", "Hi Sue, your necklace ships tomorrow with an 18 inch chain. Many Thanks, CustomBrites");
    await page.waitForTimeout(300);
    const flight = await page.evaluate(async () => {
      const box = document.getElementById("emThreadBox");
      const out = { deltas: [], longTasks: [], layoutWrites: [], samples: [], layerSeen: false };
      let po = null;
      try {
        po = new PerformanceObserver(l => l.getEntries().forEach(e => out.longTasks.push(Math.round(e.duration))));
        po.observe({ entryTypes: ["longtask"] });
      } catch (_) {}
      // Layout properties written on any message node while it flies.
      const mo = new MutationObserver(ms => ms.forEach(m => {
        const el = m.target;
        if (!el.closest || !el.closest("#emThreadBox")) return;
        const s = el.style;
        ["height", "paddingTop", "paddingBottom", "marginBottom", "top", "left", "width"].forEach(k => {
          if (s[k] && !out.layoutWrites.includes(k)) out.layoutWrites.push(k);
        });
      }));
      mo.observe(box, { attributes: true, attributeFilter: ["style"], subtree: true });
      const comp = document.getElementById("emComposerCard");
      const lastOld = box.querySelector('[data-mid="m39"]');
      out.compBefore = Math.round(comp.getBoundingClientRect().top);
      out.oldBefore = Math.round(lastOld.getBoundingClientRect().top);
      out.track = [];
      const t0 = performance.now();
      let last = t0, running = true;
      const tick = now => {
        out.deltas.push(now - last); last = now;
        const nb = box.querySelector("[data-mid]:last-child");
        out.track.push({ t: Math.round(now - t0), comp: Math.round(comp.getBoundingClientRect().top), old: Math.round(lastOld.getBoundingClientRect().top),
          clip: nb ? getComputedStyle(nb).clipPath : "", op: nb ? getComputedStyle(nb).opacity : "" });
        const layer = document.querySelector(".em-flight-layer");
        if (layer) {
          out.layerSeen = true;
          const w = layer.querySelector(".em-flight-y");
          if (w) { const r = w.getBoundingClientRect(); out.samples.push({ t: Math.round(now - t0), x: Math.round(r.left), y: Math.round(r.top) }); }
        }
        if (running) requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
      document.getElementById("emSendEtsyBtn").click();
      await new Promise(r => setTimeout(r, 1400));
      running = false;
      mo.disconnect();
      if (po) po.disconnect();
      out.deltas.shift();
      out.bubble = !!box.querySelector("[data-mid^='local']") || box.querySelectorAll("[data-mid]").length > 40;
      return out;
    });
    const worst = Math.max(...flight.deltas);
    if (process.env.DEBUG) console.log(JSON.stringify(flight.track.slice(0, 24)), flight.compBefore, flight.oldBefore, flight.deltas.map(Math.round).join(","));
    const tr = flight.track.filter(x => x.t > 0);
    check(tr.every(x => Math.abs(x.comp - flight.compBefore) <= 1), "the reply box stays still while the bubble opens");
    const first = tr[0], end = tr[tr.length - 1];
    check(Math.abs(first.old - flight.oldBefore) <= 3, "the history starts where it was before Send (no jump): " + first.old + " vs " + flight.oldBefore);
    check(end.old < flight.oldBefore - 20, "the history ends pushed up by the new bubble");
    const mids = tr.filter(x => x.t > 60 && x.t < 240).map(x => x.old);
    check(mids.some(v => v < first.old - 2 && v > end.old + 2), "the history slides up through in-between positions");
    console.log("  flight frames:", flight.deltas.length, "worst", Math.round(worst), "ms; long tasks", JSON.stringify(flight.longTasks),
      "; layout writes", JSON.stringify(flight.layoutWrites));
    check(flight.layerSeen, "the flying words are drawn on screen");
    const xs = new Set(flight.samples.map(s => s.x + "," + s.y));
    check(xs.size >= 5, "the words move over several frames (" + xs.size + " positions)");
    check(flight.bubble, "the sent reply's bubble is in the list");
    check(flight.layoutWrites.length === 0, "no layout property (height, padding, margin) is animated on the list during the flight");
    check(worst <= 34, "no frame over 34 ms during the flight (worst " + Math.round(worst) + " ms)");
    check(!flight.longTasks.some(d => d > 50), "no long task over 50 ms during the flight");

    check(errors.length === 0, "no page errors" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
  } finally {
    await browser.close();
    server.close();
  }
  if (fails.length) { console.log("\n" + fails.length + " failed"); process.exit(1); }
  console.log("\nall passed");
})().catch(e => { console.error(e); process.exit(1); });
