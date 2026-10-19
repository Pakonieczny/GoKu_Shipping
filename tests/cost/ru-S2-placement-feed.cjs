// Cost of the placement feed's cadence (RU-S2, Firebase cost reduction): charm-nest-placement-feed.js in headless Chromium on a page of 80 order rows, a fake clock, a recording
// stand-in for OrderPieces.load / Cancelled.load (nothing is read from any database here; the reads a list of orders costs are measured apart over the real handler, see the report).
//   · a window a person works in (input, focused): the feed reads the rows on screen and the band of NEAR px around them, every ACTIVE (4 s), as before;
//   · a window nobody has touched for 2 minutes: only the rows laid out on screen (a third of the orders), so each read a change of the cloud causes is a third as large;
//   · the first input after that brings the band back and reads at once;
//   · a window that is not focused but is scrolled by the wheel (Windows scrolls the window under the pointer): the wheel used to pull the next read forward to "now" with every event, so a scroll
//     of the list was a full read of the changed set about once a second; it is now at most one read per ACTIVE.
//   node tests/cost/ru-S2-placement-feed.cjs [PW_DIR]      (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; FEED_JS=<another copy of the file> to count the version before)
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, "node_modules");
let chromium; try { ({ chromium } = require(path.join(pwDir, "playwright-core"))); } catch (_) { console.log("  - no playwright-core: the browser checks were not run"); process.exit(0); }
const CHROME = process.env.CHROMIUM || (() => { try { const d = "/opt/pw-browsers"; const c = fs.readdirSync(d).filter(x => /^chromium-\d/.test(x)).sort().pop(); return c ? path.join(d, c, "chrome-linux/chrome") : undefined; } catch (_) { return undefined; } })();

const ROWS = 80, ROW_H = 60;
const harness = () => `<!doctype html><html><head><meta charset="utf-8"><title>feed</title><style>body{margin:0}.row{height:${ROW_H}px;box-sizing:border-box;border-bottom:1px solid #ccc}</style></head>
<body>${Array.from({ length: ROWS }, (_, i) => `<div class="row" data-rid="${4190000000 + i}">order ${i}</div>`).join("")}
<script>
window.__loads = []; window.__cancel = 0; window.__focus = true;
document.hasFocus = () => window.__focus;
window.CN = { S: { cloud: { ok: true }, mode: "orders" } };
window.OrderPieces = { load: (ids, o) => { window.__loads.push({ n: ids.length, force: !!(o && o.force), first: ids[0], last: ids[ids.length - 1] }); return Promise.resolve(true); }, subscribe: () => () => {}, notify: () => {} };
window.Cancelled = { load: () => { window.__cancel++; return Promise.resolve(); }, has: () => false };
window.Orders = { rows: () => [] };
</script>
<script src="/charm-nest-placement-feed.js"></script></body></html>`;

function server() {
  return new Promise(resolve => {
    const srv = http.createServer((req, res) => {
      const u = new URL(req.url, "http://x");
      if (u.pathname === "/h.html") { res.writeHead(200, { "Content-Type": "text/html" }); return res.end(harness()); }
      if (u.pathname === "/charm-nest-placement-feed.js") { res.writeHead(200, { "Content-Type": "text/javascript" }); return res.end(fs.readFileSync(process.env.FEED_JS || path.join(root, "charm-nest-placement-feed.js"))); }
      res.writeHead(404); res.end();
    }).listen(0, "127.0.0.1", () => resolve(srv));
  });
}

(async () => {
  const srv = await server(), origin = "http://127.0.0.1:" + srv.address().port;
  const browser = await chromium.launch({ executablePath: CHROME, args: ["--no-sandbox"] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? "  ok   " : "  FAIL ") + msg); };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  try {
    async function open() {
      const context = await browser.newContext({ viewport: { width: 900, height: 900 } });
      const page = await context.newPage(), errors = [];
      page.on("pageerror", e => { errors.push(e.message); console.error("page error:", e.message); });
      await page.clock.install({ time: Date.parse("2026-10-19T09:00:00Z") });
      await page.goto(`${origin}/h.html`, { waitUntil: "load" });
      await page.evaluate(() => document.dispatchEvent(new Event("pointerdown")));   // (a person is here)
      return { page, context, errors };
    }
    const run = async (page, ms, step = 500) => { for (let t = 0; t < ms; t += step) { await page.clock.runFor(step); await sleep(4); } };
    const loads = page => page.evaluate(() => window.__loads.slice());
    const reset = page => page.evaluate(() => { window.__loads.length = 0; window.__cancel = 0; });

    // ── 1 · a person works here: the band around the screen is read, every ~4 s ──
    {
      const { page, context, errors } = await open();
      await run(page, 2000); await reset(page);
      await run(page, 60000);
      const L = await loads(page);
      check(L.length >= 13 && L.length <= 17, `worked in for a minute: ${L.length} reads (every 4 s)`);
      const n = L.length ? L[L.length - 1].n : 0;
      check(n >= 24 && n <= 30, `... of ${n} rows (the 15 on screen and the band below them)`);
      check(L.every(x => x.force), "every read is the forced, conditional one");
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");

      // ── 2 · nobody has touched it for 2 minutes: the screen's rows alone ──
      await run(page, 150000); await reset(page);
      await run(page, 90000);
      const Q = await loads(page);
      check(Q.length >= 4 && Q.length <= 8, `quiet: ${Q.length} reads in 90 s (every 15 s)`);
      const nq = Q.length ? Q[Q.length - 1].n : 0;
      check(nq >= 14 && nq <= 16, `... of ${nq} rows (only the ones on screen; was ${n})`);

      // ── 3 · the first input brings the band back and reads at once ──
      await reset(page);
      await page.evaluate(() => document.dispatchEvent(new Event("pointerdown")));
      await run(page, 2000);
      const B = await loads(page);
      check(B.length >= 1 && B[0].n >= 24, `first input: a read within 2 s of ${B.length ? B[0].n : 0} rows (the band is back)`);
      await context.close();
    }

    // ── 4 · not focused, scrolled by the wheel: at most one read per ACTIVE, not one per wheel event ──
    {
      const { page, context, errors } = await open();
      await run(page, 2000);
      await page.evaluate(() => { window.__focus = false; });
      await run(page, 130000);                // (the quiet spell: the feed is at its slow pace)
      await reset(page);
      const t0 = Date.now();
      for (let i = 0; i < 40; i++) {           // 40 wheel events, one every 250 ms of the fake clock = 10 s of scrolling, the list moving 60 px each
        await page.evaluate(() => { window.scrollBy(0, 60); document.dispatchEvent(new Event("wheel")); });
        await run(page, 250, 250);
      }
      await run(page, 1000);
      const S = await loads(page);
      check(S.length <= 5, `10 s of wheel scrolling in a window without the focus: ${S.length} reads (was one about every second)`);
      check(S.length >= 1, "... and the first wheel after the quiet spell still reads at once");
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }
  } finally { await browser.close(); srv.close(); }
  console.log(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nru-S2-placement-feed OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
