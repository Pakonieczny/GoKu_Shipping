// FC15: cn-poll.js in REAL Chromium tabs (real Web Locks, real BroadcastChannel, real localStorage lease) against a local counting server.
// Six tabs of one browser context poll one key: the server must see one poller, ETag/304 must be used, a change must reach every tab,
// closing or hiding the leader must hand over, a poke from a follower must be answered at once, the lease fallback must work without
// Web Locks. No live site, no Firestore, nothing leaves 127.0.0.1.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/cost/poll-browser.cjs
"use strict";
const fs = require("fs"), path = require("path"), http = require("http"), assert = require("assert");
const root = path.join(__dirname, "../..");
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));
const sleep = ms => new Promise(r => setTimeout(r, ms));

const server = { rev: 1, hits: [] };
const PAGE = `<!doctype html><meta charset=utf-8><title>cn-poll test</title><script src="/cn-poll.js"></script><script>
const q = new URLSearchParams(location.search);
window.__got = []; window.__tab = q.get("tab") || Math.random().toString(36).slice(2);
window.__p = CnPoll.create({ key: q.get("key") || "live", url: "/api", init: () => ({ method: "POST", headers: { "X-Tab": window.__tab }, body: "{}" }),
  intervalMs: Number(q.get("ms")) || 500, maxIntervalMs: Number(q.get("max")) || 0, jitter: 0,
  onData: (d, i) => window.__got.push({ rev: d.rev, at: Date.now(), leader: i.leader }) }).start();
</script>`;
const srv = http.createServer((req, res) => {
  const u = new URL(req.url, "http://x");
  if (u.pathname === "/cn-poll.js") { res.writeHead(200, { "Content-Type": "text/javascript", "Cache-Control": "no-store" }); return res.end(fs.readFileSync(path.join(root, "cn-poll.js"))); }
  if (u.pathname === "/page.html") { res.writeHead(200, { "Content-Type": "text/html" }); return res.end(PAGE); }
  if (u.pathname === "/api") {
    const etag = '"r' + server.rev + '"', inm = req.headers["if-none-match"] || null;
    server.hits.push({ t: Date.now(), tab: req.headers["x-tab"], inm, rev: server.rev });
    req.resume();
    if (inm === etag) { res.writeHead(304, { ETag: etag }); return res.end(); }
    res.writeHead(200, { "Content-Type": "application/json", ETag: etag, "Cache-Control": "no-store" }); return res.end(JSON.stringify({ rev: server.rev }));
  }
  res.writeHead(404); res.end();
});

(async () => {
  await new Promise(r => srv.listen(0, "127.0.0.1", r));
  const base = "http://127.0.0.1:" + srv.address().port;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  let groups = 0; const ok = n => { groups++; console.log("  ok  " + n); };
  const gotOf = p => p.evaluate(() => window.__got), stateOf = p => p.evaluate(() => window.__p.state());
  const open = async (ctx, q, init) => { const p = await ctx.newPage(); if (init) await p.addInitScript(init); await p.goto(base + "/page.html?" + (q || "")); return p; };
  const leaderOf = async pages => { const out = []; for (const p of pages) if (!p.isClosed() && (await stateOf(p)).leader) out.push(p); return out; };
  try {
    // ── 1. six tabs, one poller ──
    {
      const ctx = await browser.newContext(); server.hits.length = 0; server.rev = 1;
      const pages = []; for (let i = 0; i < 6; i++) pages.push(await open(ctx, "tab=t" + i));
      await sleep(6000);
      const n = server.hits.length, tabs = new Set(server.hits.map(h => h.tab));
      assert.ok(n >= 8 && n <= 16, "6 tabs, 6 s, 500 ms: about 13 requests (not 72), got " + n);
      assert.equal(tabs.size, 1, "one tab asks, saw " + [...tabs]);
      assert.equal(server.hits[0].inm, null); assert.ok(server.hits.slice(1).every(h => h.inm === '"r1"'), "ETag sent back");
      for (const p of pages) { const g = await gotOf(p); assert.equal(g.length, 1, "each tab got the answer once"); assert.equal(g[0].rev, 1); }
      assert.equal((await leaderOf(pages)).length, 1, "one leader");
      assert.equal(await pages[0].evaluate(() => document.visibilityState), "visible");
      // a change reaches every tab quickly
      const before = server.hits.length, at = Date.now(); server.rev = 2;
      await sleep(1200);
      for (const p of pages) { const g = await gotOf(p); assert.equal(g.length, 2, "tab got the change"); assert.equal(g[1].rev, 2); assert.ok(g[1].at - at < 1100, "within ~one interval: " + (g[1].at - at)); }
      assert.equal(server.hits.slice(before).filter(h => h.inm === '"r1"').length, 1);
      // close the leader: another tab leads within ~1 s, 304 not 200, nobody sees a duplicate
      const lead = (await leaderOf(pages))[0], rest = pages.filter(p => p !== lead), nLead = server.hits.length;
      await lead.close(); await sleep(1500);
      const after = server.hits.slice(nLead); assert.ok(after.length >= 1, "a follower took over");
      assert.equal(new Set(after.map(h => h.tab)).size, 1); assert.ok(after.every(h => h.inm === '"r2"'), "take-over sends the ETag it received");
      for (const p of rest) assert.equal((await gotOf(p)).length, 2, "no duplicate delivery after take-over");
      assert.equal((await leaderOf(rest)).length, 1);
      // poke from a follower (base 500 ms is too fast to prove it, so use a 30 s tab set below); here: a poke is answered inside 700 ms
      const fol = rest.find(p => true), nBefore = server.hits.length; server.rev = 3; const t0 = Date.now();
      await fol.evaluate(() => window.__p.poke("complete"));
      await sleep(700);
      assert.ok(server.hits.length > nBefore, "poke reached the server");
      const g = await gotOf(fol); assert.equal(g[g.length - 1].rev, 3); assert.ok(g[g.length - 1].at - t0 < 700, "poke answered in " + (g[g.length - 1].at - t0) + " ms");
      await ctx.close(); ok("6 real tabs -> one poller; ETag/304; change reaches all; leader closed -> take-over; poke fast");
    }

    // ── 2. slow base interval: a follower's poke is answered at once, not at the next tick ──
    {
      const ctx = await browser.newContext(); server.hits.length = 0; server.rev = 1;
      const a = await open(ctx, "tab=a&ms=30000"), b = await open(ctx, "tab=b&ms=30000"), c = await open(ctx, "tab=c&ms=30000");
      await sleep(800); assert.equal(server.hits.length, 1, "30 s interval: one request so far");
      server.rev = 2; const t0 = Date.now(); await b.evaluate(() => window.__p.poke("qr-print")); await sleep(600);
      for (const p of [a, b, c]) { const g = await gotOf(p); assert.equal(g[g.length - 1].rev, 2); assert.ok(g[g.length - 1].at - t0 < 600, "seen in " + (g[g.length - 1].at - t0) + " ms"); }
      // a static poke by key from code that holds no poller (any tab)
      server.rev = 3; const t1 = Date.now(); await c.evaluate(() => CnPoll.poke("live", "x")); await sleep(600);
      for (const p of [a, b, c]) { const g = await gotOf(p); assert.equal(g[g.length - 1].rev, 3); assert.ok(g[g.length - 1].at - t1 < 600); }
      await ctx.close(); ok("poke on a 30 s poller reaches all tabs within 600 ms");
    }

    // ── 3. hidden: hand-over, all hidden = zero requests, visible = immediate request ──
    {
      const ctx = await browser.newContext(); server.hits.length = 0; server.rev = 1;
      const hide = (p, h) => p.evaluate(h => { Object.defineProperty(document, "hidden", { configurable: true, get: () => h }); document.dispatchEvent(new Event("visibilitychange")); }, h);
      const pages = [await open(ctx, "tab=a"), await open(ctx, "tab=b"), await open(ctx, "tab=c")]; await sleep(1500);
      const lead = (await leaderOf(pages))[0]; await hide(lead, true); await sleep(1200);
      const lead2 = (await leaderOf(pages))[0]; assert.ok(lead2 && lead2 !== lead, "a visible tab leads now");
      for (const p of pages) if (p !== lead) await hide(p, true);
      await sleep(600); const n = server.hits.length; await sleep(3000);
      assert.equal(server.hits.length, n, "all hidden: the server sees nothing for 3 s (6 intervals)");
      server.rev = 2; const t0 = Date.now(); await hide(pages[1], false); await sleep(500);
      assert.ok(server.hits.length === n + 1 || server.hits.length === n + 2, "one immediate request on return, got " + (server.hits.length - n));
      const g = await gotOf(pages[1]); assert.equal(g[g.length - 1].rev, 2); assert.ok(g[g.length - 1].at - t0 < 500);
      await ctx.close(); ok("hidden tabs never poll; hand-over; return is immediate");
    }

    // ── 4. backoff against the real server: unchanged answers stretch the interval, a change resets it ──
    {
      const ctx = await browser.newContext(); server.hits.length = 0; server.rev = 1;
      const p = await open(ctx, "tab=a&ms=250&max=2000"); await sleep(9000);
      const ts = server.hits.map(h => h.t), gaps = ts.slice(1).map((x, i) => x - ts[i]);
      assert.ok(gaps[0] < 450 && gaps[gaps.length - 1] > 1700, "gaps grow from 250 ms to 2 s: " + gaps.join(","));
      assert.ok(server.hits.length < 22, "9 s at 250 ms flat would be 36 requests, got " + server.hits.length);
      const n = server.hits.length; await p.mouse.move(10, 10); await p.mouse.click(10, 10); await sleep(700);
      assert.ok(server.hits.length > n, "a click pulls the next look forward (reset to the base interval)");
      await ctx.close(); ok("backoff and instant reset on a click in a real tab");
    }

    // ── 5. no Web Locks: the localStorage lease gives one leader ──
    {
      const ctx = await browser.newContext(); server.hits.length = 0; server.rev = 1;
      const init = () => Object.defineProperty(navigator, "locks", { value: undefined, configurable: true });
      const pages = []; for (let i = 0; i < 3; i++) pages.push(await open(ctx, "tab=l" + i, init));
      assert.equal(await pages[0].evaluate(() => typeof navigator.locks), "undefined");
      await sleep(6000);
      const tabs = new Set(server.hits.map(h => h.tab)); assert.equal(tabs.size, 1, "lease: one poller, saw " + [...tabs]);
      assert.ok(server.hits.length <= 16, "requests " + server.hits.length);
      for (const p of pages) assert.equal((await gotOf(p)).length, 1);
      const lead = (await leaderOf(pages))[0]; const nn = server.hits.length; await lead.close(); await sleep(5500);
      const after = server.hits.slice(nn); assert.ok(after.length >= 3, "another tab took over the lease"); assert.equal(new Set(after.map(h => h.tab)).size, 1);
      await ctx.close(); ok("lease fallback in real tabs: one poller, take-over after close");
    }
    console.log("poll-browser.cjs: " + groups + " groups passed");
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
