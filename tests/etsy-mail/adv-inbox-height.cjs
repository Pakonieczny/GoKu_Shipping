// Adversarial checks of two inbox animations (etsy-mail-1.html) in a real
// Chromium: the emptied reply box easing back after a send, and a failed
// reply's bubble folding away. Both must move by transform, opacity or
// clip-path only (the reply box's height is set once, never per frame),
// stay visible and smooth, and leave nothing cut off or out of place.
//
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules \
//   CHROMIUM=$(ls -d /opt/pw-browsers/chromium-*/chrome-linux/chrome|head -1) \
//   node tests/etsy-mail/adv-inbox-height.cjs
//
// Every /.netlify/functions call goes to a fake in this file (no Etsy, no
// AI, nothing sent); every other request is aborted.
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));

const NOW = Date.now();
const TID = "t_adv5h";
const THREAD = {
  id: TID, customerName: "Sue Tester", status: "etsy_scraped", unread: false,
  lastInboundAt: { _ts: true, ms: NOW - 3600e3 }, awaitingReplySince: { _ts: true, ms: NOW - 3600e3 },
  updatedAt: { _ts: true, ms: NOW - 3600e3 }, etsyConversationUrl: "https://example.invalid/c/1"
};
const MESSAGES = [];
for (let i = 0; i < 40; i++) {
  MESSAGES.push({
    id: "m" + i, direction: i % 2 ? "outbound" : "inbound", senderName: i % 2 ? "CustomBrites" : "Sue",
    text: (i % 2 ? "Thanks for your message, we will check the engraving and the chain length for you today. " :
      "Hi, could you tell me when my necklace ships and whether the chain is 18 inches? ").repeat(1 + (i % 3)),
    timestamp: { _ts: true, ms: NOW - (41 - i) * 3600e3 }, createdAt: { _ts: true, ms: NOW - (41 - i) * 3600e3 }
  });
}
let enqueueDelay = 400, enqueueFail = false;
function fake(method, url, body) {
  const u = new URL(url, "http://x");
  const name = u.pathname.split("/").pop();
  const q = Object.fromEntries(u.searchParams);
  let b = {};
  try { b = body ? JSON.parse(body) : {}; } catch (_) {}
  if (name === "firestoreProxy") {
    if (q.op === "list" && q.coll === "EtsyMail_Threads") return { docs: [THREAD] };
    if (q.op === "listSub") return { docs: MESSAGES };
    if (q.op === "get") return { exists: false };
    return { ok: true };
  }
  if (name === "etsyMailAuth") return { ok: true, username: "paul", displayName: "Paul", role: "owner" };
  if (name === "etsyMailDraftSend" && method === "POST" && b.op === "enqueue") {
    if (enqueueFail) return { __delay: enqueueDelay, __status: 500, error: "fake failure" };
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
      const delay = out.__delay || 0, status = out.__status || 200;
      delete out.__delay; delete out.__status;
      setTimeout(() => { res.writeHead(status, { "Content-Type": "application/json" }); res.end(JSON.stringify(out)); }, delay);
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
    await page.click(`[data-id="${TID}"]`);
    await page.waitForSelector("#emDraftText", { timeout: 10000 });
    await page.waitForFunction(() => document.querySelectorAll("#emThreadBox [data-mid]").length >= 40, null, { timeout: 10000 });
    await page.waitForTimeout(600);

    // Click Send and watch for ms: every frame's positions, and every
    // height / padding / margin written on the reply box or a bubble.
    const watch = (ms) => page.evaluate(async (ms) => {
      const box = document.getElementById("emThreadBox");
      const ta = document.getElementById("emDraftText");
      const comp = document.getElementById("emComposerCard");
      const old = box.querySelector('[data-mid="m39"]');
      const h4 = comp.querySelector("h4");
      const out = { frames: [], deltas: [], writes: [], longTasks: [] };
      let po = null;
      try { po = new PerformanceObserver(l => l.getEntries().forEach(e => out.longTasks.push(Math.round(e.duration)))); po.observe({ entryTypes: ["longtask"] }); } catch (_) {}
      const t0 = performance.now();
      const mo = new MutationObserver(ms => ms.forEach(m => {
        const el = m.target, s = el.style;
        if (el !== ta && !(el.closest && el.closest("#emThreadBox"))) return;
        ["height", "paddingTop", "paddingBottom", "marginBottom", "marginTop", "top", "left", "width"].forEach(k => {
          if (s[k]) out.writes.push({ t: Math.round(performance.now() - t0), who: el === ta ? "box" : "bubble", k, v: s[k] });
        });
      }));
      mo.observe(document.body, { attributes: true, attributeFilter: ["style"], subtree: true });
      const locals0 = box.querySelectorAll("[data-mid^='local']").length;
      out.before = { h: ta.getBoundingClientRect().height, old: old.getBoundingClientRect().top, h4: h4.getBoundingClientRect().top,
        compBottom: comp.getBoundingClientRect().bottom, send: document.getElementById("emSendEtsyBtn").getBoundingClientRect().top };
      let last = t0, running = true;
      const tick = now => {
        out.deltas.push(now - last); last = now;
        const nb = box.querySelectorAll("[data-mid^='local']")[locals0] || null;
        out.frames.push({ t: Math.round(now - t0), h: Math.round(ta.getBoundingClientRect().height * 10) / 10,
          old: Math.round(old.getBoundingClientRect().top * 10) / 10, h4: Math.round(h4.getBoundingClientRect().top * 10) / 10,
          compBottom: Math.round(comp.getBoundingClientRect().bottom),
          bubble: nb ? { op: +getComputedStyle(nb).opacity, clip: getComputedStyle(nb).clipPath, shown: getComputedStyle(nb).display !== "none" } : null,
          val: ta.value.length });
        if (running) requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
      document.getElementById("emSendEtsyBtn").click();
      await new Promise(r => setTimeout(r, ms));
      running = false; mo.disconnect(); if (po) po.disconnect();
      out.deltas.shift();
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
      const sb = document.getElementById("emSendEtsyBtn").getBoundingClientRect();
      // Animations other than the red "came back" glow on the box.
      const moving = el => el.getAnimations().some(a => {
        try { return a.effect.getKeyframes().some(k => Object.keys(k).some(p => /transform|clip|opacity|height/i.test(p))); } catch (_) { return true; }
      });
      out.after = { h: ta.getBoundingClientRect().height, fits: ta.scrollHeight <= ta.clientHeight + 1,
        old: old.isConnected ? old.getBoundingClientRect().top : null, h4: h4.getBoundingClientRect().top,
        send: sb.top, value: ta.value,
        hint: getComputedStyle(ta, "::placeholder").color,
        leftovers: Array.from(document.querySelectorAll("#emComposerCard, #emComposerCard *, .em-detail-main *, #emThreadBox > *"))
          .filter(el => moving(el) || (el.style.transform && el.style.transform !== "none") ||
            (el.style.clipPath && el.style.clipPath !== "none")).length,
        bubbles: box.querySelectorAll("[data-mid^='local']").length - locals0 };
      return out;
    }, ms);

    // ── 1. The emptied reply box eases back after a send ───────────────
    const long = "Hi Sue,\n\nThank you so much for your order and for your patience.\nYour necklace ships tomorrow with an 18 inch chain.\n" +
      "We checked the engraving twice and it looks lovely.\nTracking will follow by email as soon as the label is printed.\n\nMany Thanks,\nCustomBrites";
    await page.fill("#emDraftText", long);
    await page.waitForTimeout(400);
    const s = await watch(1900);
    if (process.env.DEBUG) console.log(JSON.stringify(s.frames.filter(f => f.val === 0).map(f => [f.t, f.h, f.old, f.h4])), JSON.stringify(s.writes.slice(0, 12)));
    const h0 = s.before.h, h1 = s.after.h;
    check(h0 > h1 + 30, "the long reply grew the box (" + Math.round(h0) + " px) and it is back to its resting size (" + Math.round(h1) + " px)");
    const mid = s.frames.filter(f => f.h > h1 + 1 && f.h < h0 - 1);
    check(mid.length === 0, "the reply box's height is never an in-between value (" + mid.length + " frames were)");
    const distinct = new Set(s.writes.filter(w => w.who === "box" && w.k === "height" && w.v !== "auto").map(w => w.v));
    check(distinct.size <= 2, "the reply box's height is set once after the send, not per frame (" + distinct.size + " values written)");
    // The history and the reply box's heading slide down smoothly as it shrinks.
    const startH4 = s.before.h4, endH4 = s.after.h4;
    const emptied = s.frames.filter(f => f.val === 0);
    const inBetween = emptied.filter(f => f.h4 > Math.min(startH4, endH4) + 2 && f.h4 < Math.max(startH4, endH4) - 2);
    check(Math.abs(endH4 - startH4) > 20 && inBetween.length >= 3,
      "the reply box's heading slides through in-between positions (" + inBetween.length + " frames)");
    let jump = 0;
    for (let i = 1; i < emptied.length; i++) jump = Math.max(jump, Math.abs(emptied[i].h4 - emptied[i - 1].h4));
    check(jump <= Math.max(12, (h0 - h1) * 0.45), "no jump in the heading's slide (largest step " + Math.round(jump) + " px of " + Math.round(h0 - h1) + ")");
    check(s.frames.every(f => Math.abs(f.compBottom - s.before.compBottom) <= 1), "the bottom of the reply box (Send) stays still");
    check(s.after.fits, "the emptied box shows all of itself at the end (nothing cut off)");
    check(Math.abs(s.after.send - s.before.send) <= 1, "the Send button ends where it was");
    check(s.after.leftovers === 0, "no transform, clip or animation is left behind (" + s.after.leftovers + ")");
    check(!/rgba\(0, 0, 0, 0\)|transparent/.test(s.after.hint), "the reply box's hint shows again (" + s.after.hint + ")");
    // Timed from the landing on (the send's own work is timed by adv-inbox-ui).
    const worstA = Math.max(...s.deltas.filter((d, i) => s.frames[i + 1] && s.frames[i + 1].t > 700));
    console.log("  settle: frames", s.deltas.length, "worst", Math.round(worstA), "ms; long tasks", JSON.stringify(s.longTasks));

    // ── 2. A failed reply folds away and comes back to the box ─────────
    await page.waitForTimeout(800);
    enqueueFail = true; enqueueDelay = 1500;
    const typed = "Hi Sue, the chain is 18 inches and it ships Friday.";
    await page.fill("#emDraftText", typed);
    await page.waitForTimeout(400);
    const r = await watch(2600);
    enqueueFail = false; enqueueDelay = 400;
    if (process.env.DEBUG) console.log(JSON.stringify(r.frames.filter(f => f.t > 1300).map(f => [f.t, f.old, f.bubble && f.bubble.op, f.bubble && f.bubble.clip])), JSON.stringify(r.writes.slice(0, 12)));
    const bubbleWrites = r.writes.filter(w => w.who === "bubble");
    check(bubbleWrites.length === 0, "no height, padding or margin is animated on the failed bubble (" +
      JSON.stringify(Array.from(new Set(bubbleWrites.map(w => w.k)))) + ")");
    const fold = r.frames.filter(f => f.t > 1400 && f.bubble && f.bubble.shown && f.bubble.op > 0.02 && f.bubble.op < 0.98);
    check(fold.length >= 3, "the failed bubble fades away over several frames (" + fold.length + ")");
    check(r.after.bubbles === 0, "the failed bubble is gone");
    check(r.after.value === typed, "the reply is back in the box");
    const tail = r.frames.filter(f => f.t > 1400);
    const moving = tail.filter(f => f.old > Math.min(r.before.old, tail[0].old) + 2 && f.old < Math.max(r.before.old, tail[0].old) - 2);
    check(moving.length >= 3, "the history slides back through in-between positions (" + moving.length + " frames)");
    let step = 0;
    for (let i = 1; i < tail.length; i++) step = Math.max(step, Math.abs(tail[i].old - tail[i - 1].old));
    check(step <= 20, "the history slides back without a jump (largest step " + Math.round(step) + " px)");
    check(Math.abs(r.after.old - r.before.old) <= 2, "the history ends where it was before the send (" + Math.round(r.after.old) + " vs " + Math.round(r.before.old) + ")");
    check(r.frames.every(f => Math.abs(f.compBottom - r.before.compBottom) <= 1), "the reply box stays still while the bubble folds");
    check(r.after.leftovers === 0, "no transform, clip or animation is left behind after the fold (" + r.after.leftovers + ")");
    const worstB = Math.max(...r.deltas.filter((d, i) => r.frames[i + 1] && r.frames[i + 1].t > 1400));
    console.log("  fold: frames", r.deltas.length, "worst", Math.round(worstB), "ms; long tasks", JSON.stringify(r.longTasks));
    check(Math.max(worstA, worstB) <= 34, "no frame over 34 ms in either animation (worst " + Math.round(Math.max(worstA, worstB)) + " ms)");
    check(![...s.longTasks, ...r.longTasks].some(d => d > 50), "no long task over 50 ms");

    check(errors.length === 0, "no page errors" + (errors.length ? ": " + errors.slice(0, 3).join(" | ") : ""));
  } finally {
    await browser.close();
    server.close();
  }
  if (fails.length) { console.log("\n" + fails.length + " failed"); process.exit(1); }
  console.log("\nall passed");
})().catch(e => { console.error(e); process.exit(1); });
