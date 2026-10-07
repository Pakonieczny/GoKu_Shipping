// FC15 adoption 1: etsy-pricing.html's Etsy API usage meter now rides cn-poll.js (one poller per computer, hidden pause, backoff
// 15 -> 30 -> 45 s while the reading is unchanged, repaint in every tab). Real Chromium, the real page, a fake clock, a local server
// that stands in for the Netlify functions (nothing leaves 127.0.0.1; etsyApiProbe, the only call that can spend an Etsy request, is
// counted and must stay at zero while the reading is fresh).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/cost/etsy-usage-poll.cjs
"use strict";
const fs = require("fs"), path = require("path"), http = require("http"), assert = require("assert");
const root = path.join(__dirname, "../..");
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));
const sleep = ms => new Promise(r => setTimeout(r, ms));

const st = { count: 100, reportedAt: 0, usage: 0, probe: 0, other: [], failing: 0 };
const types = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".png": "image/png" };
const srv = http.createServer((req, res) => {
  const u = new URL(req.url, "http://x"), json = (code, o) => { res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" }); res.end(JSON.stringify(o)); };
  if (u.pathname === "/.netlify/functions/etsyApiUsage") {
    st.usage++;
    if (st.failing) return json(st.failing, { error: "boom" });
    return json(200, { ok: true, verified: true, count: st.count, count_since: st.reportedAt - 3 * 86400000, budget: 2500, max_qps: 1.5, qps_cap: 2.5,
      etsy_limit_per_day: 5000, etsy_remaining_today: 5000 - st.count, etsy_reported_at: st.reportedAt, server_time: Date.now() });
  }
  if (u.pathname === "/.netlify/functions/etsyApiProbe") { st.probe++; return json(200, { ok: true, verified: true }); }
  if (u.pathname.startsWith("/.netlify/functions/")) { st.other.push(u.pathname); return json(u.pathname.endsWith("authGate") ? 200 : 404, { ok: false }); }
  const f = path.join(root, decodeURIComponent(u.pathname.replace(/^\/+/, "") || "etsy-pricing.html"));
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "Content-Type": types[path.extname(f)] || "application/octet-stream", "Cache-Control": "no-store" }); res.end(fs.readFileSync(f));
});

(async () => {
  await new Promise(r => srv.listen(0, "127.0.0.1", r));
  const base = "http://127.0.0.1:" + srv.address().port;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  try {
    const ctx = await browser.newContext();
    const pages = [];
    st.reportedAt = Date.now();
    for (let i = 0; i < 3; i++) {
      const p = await ctx.newPage(); await p.clock.install({ time: Date.now() });
      p.on("pageerror", e => { if (/etsy|ApiUsage|CnPoll/i.test(String(e))) console.log("pageerror", e.message); });
      await p.goto(base + "/etsy-pricing.html"); pages.push(p);
    }
    const pageNow = p => p.evaluate(() => Date.now());
    // advances every tab's fake clock; the page clock also flows, so the elapsed page time is measured, not assumed
    const run = async ms => { const t0 = await pageNow(pages[0]); for (let t = 0; t < ms; t += 1000) { for (const p of pages) await p.clock.runFor(1000); await sleep(40); } await sleep(150); return ((await pageNow(pages[0])) - t0) / 1000; };
    const num = p => p.evaluate(() => document.getElementById("awAppNum") && document.getElementById("awAppNum").textContent);
    await sleep(800);
    assert.equal(await pages[0].evaluate(() => typeof CnPoll + "/" + typeof ApiUsage), "object/object", "the page loaded cn-poll.js and the meter");
    for (const p of pages) assert.match(await num(p), /^100 \/ 2\.5k$/, "the first reading is painted in every tab");
    // three visible tabs, nothing changes, about 4 minutes of page time (kept under the page's own 5-minute prime tick)
    st.usage = 0; const secs = await run(75000);
    const old = Math.round(3 * (secs / 15 + 1));
    console.log("  unchanged phase: " + secs.toFixed(0) + " s of page time, " + st.usage + " requests (the 15 s timers of 3 tabs made " + old + ")");
    assert.ok(secs > 100 && secs < 285, "page time within the 5-minute window: " + secs);
    assert.ok(st.usage <= old / 3, "leader only and backing off: " + st.usage + " vs " + old);
    assert.ok(st.usage >= 4, "the leader keeps looking: " + st.usage);
    assert.equal(st.probe, 0, "no probe while the reading is fresh, so no Etsy call");
    // the count moves (and Etsy's headers with it): every tab shows it within one backed-off wait (45 s page time at the most)
    st.count = 137; st.reportedAt = await pageNow(pages[0]);
    await run(20000);
    for (const p of pages) assert.match(await num(p), /^137 \/ 2\.5k$/, "every tab shows the new count");
    // all tabs hidden: nothing is asked
    const hide = (p, h) => p.evaluate(h => { Object.defineProperty(document, "hidden", { configurable: true, get: () => h }); document.dispatchEvent(new Event("visibilitychange")); }, h);
    for (const p of pages) await hide(p, true);
    await sleep(300); st.usage = 0; const hs = await run(60000);
    assert.equal(st.usage, 0, "hidden: zero requests in " + hs.toFixed(0) + " s of page time (the old timers made " + Math.round(3 * hs / 15) + ")");
    // shown again: an immediate reading, painted in every tab
    st.count = 150; st.reportedAt = await pageNow(pages[1]);
    await hide(pages[1], false); await sleep(700);
    assert.ok(st.usage >= 1, "shown again: asks at once"); assert.match(await num(pages[1]), /^150 \/ 2\.5k$/);
    for (const p of [pages[0], pages[2]]) assert.match(await num(p), /^150 \/ 2\.5k$/, "the hidden tabs were painted from the shared answer");
    assert.equal(st.probe, 0, "still no probe");
    // a brief outage (HTTP 503) keeps the numbers and says so in every tab; a real failure (HTTP 400) blanks them in every tab;
    // the next good reading clears both, in every tab
    const title = p => p.evaluate(() => document.getElementById("apiWidget").title);
    for (const p of pages) await hide(p, false);
    st.failing = 503; await run(50000);
    for (const p of pages) { assert.match(await title(p), /last good reading still shown/, "the outage is told in every tab"); assert.match(await num(p), /^150 \/ 2\.5k$/, "numbers kept"); }
    st.failing = 400; await run(70000);
    for (const p of pages) assert.equal(await num(p), "Unavailable", "the failure blanks the meter in every tab");
    st.failing = 0; await run(100000);
    for (const p of pages) { assert.match(await num(p), /^150 \/ 2\.5k$/, "the next good reading restores the meter in every tab"); assert.equal(await title(p), "", "and clears the warning"); }
    console.log("etsy-usage-poll.cjs: passed (probes " + st.probe + ")");
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
