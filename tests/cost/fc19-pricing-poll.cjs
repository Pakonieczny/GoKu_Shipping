// What the Etsy Pricing console asks the store (Firebase cost emergency, FC19): the real etsy-pricing.html in real Chromium, a fake clock, and a local
// server that stands in for etsyPricingStore (nothing leaves 127.0.0.1; every function other than the stand-in answers 404, no Etsy call is possible).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/cost/fc19-pricing-poll.cjs
//   - the first read is the whole list, the next ones ask { since } and merge only what changed (the list the page shows is the same)
//   - the end of a run asks for the whole list again; a server that does not know `since` still works (whole list each time)
//   - the run poll is 3.5 s while the run moves, rests while the tab is hidden and polls at once when shown, and is 15 s when the run is paused or silent
"use strict";
const fs = require("fs"), path = require("path"), http = require("http"), assert = require("assert");
const root = path.join(__dirname, "../..");
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, "node_modules");
const { chromium } = require(path.join(pwDir, "playwright-core"));
const sleep = ms => new Promise(r => setTimeout(r, ms));

const st = { calls: [], docs: {}, run: { status: "running", updated: 0, done: 0 }, knowsSince: true, other: [] };
for (let i = 0; i < 40; i++) st.docs[String(2000000 + i)] = { chain_set: true, engrave_set: true, batched: i < 10, chain_type: "regular", title: "T" + i, updated_at: 1000 + i };
const types = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".png": "image/png" };
const srv = http.createServer((req, res) => {
  const u = new URL(req.url, "http://x"), json = (code, o) => { res.writeHead(code, { "Content-Type": "application/json", "Cache-Control": "no-store" }); res.end(JSON.stringify(o)); };
  if (u.pathname === "/.netlify/functions/etsyPricingStore" && req.method === "POST") {
    let b = ""; req.on("data", c => b += c); req.on("end", () => {
      const body = JSON.parse(b || "{}"); st.calls.push(body);
      const now = Date.now();
      if (body.action === "getAll") {
        if (body.since != null && st.knowsSince) {
          const docs = {}; for (const [id, d] of Object.entries(st.docs)) if (d.updated_at > body.since - 15000) docs[id] = d;
          return json(200, { docs, count: Object.keys(docs).length, delta: true, asOf: now });
        }
        return json(200, { docs: st.docs, count: Object.keys(st.docs).length, asOf: now });
      }
      if (body.action === "getRun") return json(200, { run_id: "r1", run: { status: st.run.status, total: 40, done: st.run.done, ok: st.run.done, fail: 0, current: "", updated_at: st.run.updated || now, errors: [] } });
      if (body.action === "activeRun") return json(200, { run: null });
      if (body.action === "getSchedule") return json(200, { schedule: null });
      return json(200, { ok: true });
    });
    return;
  }
  if (u.pathname === "/.netlify/functions/etsyApiUsage") return json(200, { ok: true, verified: true, count: 10, count_since: 0, budget: 2500, max_qps: 1, qps_cap: 2.5, etsy_limit_per_day: 5000, etsy_remaining_today: 4990, etsy_reported_at: Date.now(), server_time: Date.now() });
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
    const p = await ctx.newPage(); await p.clock.install({ time: Date.now() });
    const errs = []; p.on("pageerror", e => errs.push(e.message));
    await p.goto(base + "/etsy-pricing.html"); await sleep(600);
    const run = async ms => { for (let t = 0; t < ms; t += 500) { await p.clock.runFor(500); await sleep(25); } await sleep(120); };
    const hide = h => p.evaluate(h => { Object.defineProperty(document, "hidden", { configurable: true, get: () => h }); document.dispatchEvent(new Event("visibilitychange")); }, h);
    const only = a => st.calls.filter(c => c.action === a);
    const size = () => p.evaluate(() => S.prep.size);
    const titleOf = id => p.evaluate(id => (S.prep.get(id) || {}).title, id);

    /* ── the list: whole, then changes only ── */
    st.calls = [];
    await p.evaluate(() => hydrateFromStore(true));
    assert.equal(only("getAll").length, 1); assert.equal(only("getAll")[0].since, undefined, "the first read is the whole list");
    assert.equal(await size(), 40);
    st.docs["2000005"] = Object.assign({}, st.docs["2000005"], { title: "Changed", updated_at: Date.now() + 5 });
    st.docs["2000100"] = { chain_set: true, engrave_set: true, batched: false, title: "Brand new", updated_at: Date.now() + 6 };
    await p.evaluate(() => hydrateFromStore(true));
    assert.equal(only("getAll").length, 2); assert(only("getAll")[1].since > 0, "the second read asks since: " + JSON.stringify(only("getAll")[1]));
    assert.equal(await size(), 41, "the new listing is added"); assert.equal(await titleOf("2000005"), "Changed", "the changed listing is replaced"); assert.equal(await titleOf("2000006"), "T6", "an unchanged listing is kept");
    await p.evaluate(() => hydrateFromStore(true, true));
    assert(only("getAll")[2].since === undefined, "forceFull asks for the whole list"); assert.equal(await size(), 41);
    // overlapping reads run one after the other
    st.calls = [];
    await p.evaluate(async () => { await Promise.all([hydrateFromStore(true), hydrateFromStore(true), hydrateFromStore(true)]); });
    assert.equal(only("getAll").length, 3); assert(only("getAll").every(c => c.since > 0));
    // a server that does not know `since` (an older deployment) answers the whole list; the page replaces its list with it
    st.knowsSince = false; st.docs["2000007"] = Object.assign({}, st.docs["2000007"], { title: "Via old server", updated_at: Date.now() + 9 });
    await p.evaluate(() => hydrateFromStore(true));
    assert.equal(await titleOf("2000007"), "Via old server"); assert.equal(await size(), 41);
    st.knowsSince = true;
    await p.evaluate(() => hydrateFromStore(true, true));

    /* ── the run poll ── */
    st.calls = [];
    const pageNow = () => p.evaluate(() => Date.now());      // the run document's updated_at is stamped with the PAGE's fake clock, as a live server's would match a live page
    st.run = { status: "running", updated: await pageNow(), done: 5 };
    await p.evaluate(() => { S.currentRunId = "r1"; pollRun("r1"); });
    await run(35000);       // 35 s of page time
    const rp = only("getRun").length;
    assert(rp >= 9 && rp <= 12, "a moving run is polled about every 3.5 s: " + rp + " polls in 35 s");
    const hy = only("getAll").length;
    assert(hy >= 2 && hy <= 4, "every 4th poll asks the list (since): " + hy);
    assert(only("getAll").every(c => c.since > 0), "and only for changes");
    // hidden: nothing asked
    await hide(true); await run(2000); st.calls = []; await run(60000);
    assert.equal(st.calls.length, 0, "hidden tab asks nothing (" + st.calls.length + ")");
    // shown: a poll at once
    await hide(false); await run(1500);
    assert(only("getRun").length >= 1, "shown again: polls at once");
    // a paused run: 15 s
    st.run = { status: "paused", updated: Date.now() + 100, done: 7 };
    await run(5000); st.calls = []; await run(60000);
    const pp = only("getRun").length;
    assert(pp >= 3 && pp <= 5, "a paused run is polled about every 15 s: " + pp + " polls in 60 s");
    // back to running and moving: 3.5 s again, after the page is told (click Resume = pollRun at once)
    st.run = { status: "running", updated: await pageNow(), done: 8 };
    await p.evaluate(() => pollRun("r1")); st.calls = []; await run(20000);
    assert(only("getRun").length >= 5, "running again: fast polling returns: " + only("getRun").length);
    // a run that says 'running' but has shown no progress for two minutes (its worker died): 15 s
    st.run = { status: "running", updated: (await pageNow()) - 120000, done: 8 };
    await p.evaluate(() => pollRun("r1")); await run(3000); st.calls = []; await run(60000);
    const sp = only("getRun").length;
    assert(sp >= 3 && sp <= 5, "a silent run is polled about every 15 s: " + sp + " polls in 60 s");
    // finished: the whole list is read once more, polling stops
    st.run = { status: "done", updated: await pageNow(), done: 40 };
    st.calls = []; await run(20000);
    assert(only("getAll").some(c => c.since === undefined), "the end of the run reads the whole list");
    const after = only("getRun").length; st.calls = []; await run(30000);
    assert.equal(only("getRun").length, 0, "a finished run is not polled any more (" + after + " polls before it ended)");
    assert.deepEqual(errs.filter(e => /hydrate|pollRun|prepAsOf|S is not/.test(e)), [], "no page errors: " + errs.join(" | "));
    console.log("fc19-pricing-poll.cjs: passed (run polls in 35 s " + rp + ", list reads " + hy + ", paused polls in 60 s " + pp + ")");
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
