// Cost of the sorter's customer-mail polling when nobody is working in the window (RU-S2, Firebase cost reduction), in headless Chromium over a fake
// order-link endpoint and a fake clock (nothing reaches Etsy, the inbox or a customer; no Firestore is touched). It counts the calls charm-nest-mail.js makes:
//   · the "full" sync (the server reads EVERY open conversation and 3 days of resolved ones: ~225 reads at 150 + 100) and the "health" check (~55 reads);
//   · a window a person works in (an input every 5 minutes): the full sync every 15 minutes and the health check every minute, as before;
//   · a window nobody has touched for 10 minutes: the full sync hourly and the health check every 3 minutes;
//   · a hidden tab: no periodic full sync at all, the health check every 3 minutes (as before);
//   · back in sight / the first input after the quiet spell: the due full sync and a fresh light follow within seconds.
// Server-side costs are measured apart (the sync / health rows below use the figures of the real handler over the meter, see the report); this file proves the call counts.
//   node tests/cost/ru-S2-mail-idle.cjs [PW_DIR]      (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, "node_modules");
let chromium; try { ({ chromium } = require(path.join(pwDir, "playwright-core"))); } catch (_) { console.log("  - no playwright-core: the browser checks were not run"); process.exit(0); }
const CHROME = process.env.CHROMIUM || (() => { try { const d = "/opt/pw-browsers"; const c = fs.readdirSync(d).filter(x => /^chromium-\d/.test(x)).sort().pop(); return c ? path.join(d, c, "chrome-linux/chrome") : undefined; } catch (_) { return undefined; } })();

const RID = "4174973193";
const log = [];   // { op, full }
const okHealth = () => ({ at: Date.now(), level: "ok", short: "", problem: "", checks: [{ id: "helper", label: "Etsy helper", level: "ok", text: "Checked in 1 min ago" }] });
const okLink = () => ({ level: "ok", short: "", problem: "", atMs: Date.now(), downSinceMs: 0, incidentId: "" });
const okOrder = () => ({ receiptId: RID, sandbox: false, conversation: { threadId: "t1", customer: { name: "Ada Byrne" } }, engagements: [], active: null, lookupFailed: null });

const harness = () => `<!doctype html><html><head><meta charset="utf-8"><title>Charm Sorter</title>
<style>[hidden]{display:none!important}.hidden{display:none!important}dialog{width:420px;height:520px}.cm{display:flex;flex-direction:column}</style></head>
<body><div class="topbar"><div class="topTools"></div></div><div id="toasts"></div>
<script>
window.S = { settings: { sandbox: "off", sound: "on", notify: "on" } };
window.toast = () => {}; window.ding = () => {}; window.notifyPerson = () => {};
window.esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
</script>
<script src="/charm-nest-mail.js"></script></body></html>`;

function server() {
  return new Promise(resolve => {
    const srv = http.createServer((req, res) => {
      const u = new URL(req.url, "http://x");
      if (u.pathname === "/h.html") { res.writeHead(200, { "Content-Type": "text/html" }); return res.end(harness()); }
      if (u.pathname === "/charm-nest-mail.js") { res.writeHead(200, { "Content-Type": "text/javascript" }); return res.end(fs.readFileSync(process.env.MAIL_JS || path.join(root, "charm-nest-mail.js"))); }   // (MAIL_JS: another copy of the file, to count the calls of the version before)
      if (u.pathname.endsWith("/etsyMailOrderLink")) {
        let raw = ""; req.on("data", c => raw += c); req.on("end", () => {
          let b = {}; try { b = JSON.parse(raw || "{}"); } catch (_) {}
          log.push({ op: b.op, full: !!b.full });
          const send = obj => { res.writeHead(200, { "Content-Type": "application/json" }); res.end(JSON.stringify(obj)); };
          switch (b.op) {
            case "sync": return send({ n: 1, v: 1, full: !!b.full, changes: [], link: okLink() });
            case "health": return send(okHealth());
            case "order": return send(okOrder());
            case "history_info": return send({ receiptId: RID, threads: [], total: 0, exact: true, why: "ok", reason: "" });
            default: return send({});
          }
        });
        return;
      }
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
  const count = (from, op, full) => log.slice(from).filter(x => x.op === op && (full == null || x.full === full)).length;
  try {
    async function open() {
      log.length = 0;
      const context = await browser.newContext({ viewport: { width: 900, height: 800 } });
      await context.addInitScript(() => { try { if (!sessionStorage.getItem("__seeded")) { localStorage.clear(); localStorage.setItem("cn.mail.station", JSON.stringify("k-test-station-key-0123456789abcdef")); sessionStorage.setItem("__seeded", "1"); } } catch (_) {} });
      const page = await context.newPage(), errors = [];
      page.on("pageerror", e => { errors.push(e.message); console.error("page error:", e.message); });
      await page.clock.install({ time: Date.parse("2026-10-19T09:00:00Z") });
      await page.goto(`${origin}/h.html`, { waitUntil: "load" });
      await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
      await page.evaluate(rid => CustomerMail.openConversation({ receiptId: rid, scope: "order" }), RID);   // (a place to write is on screen: the light is checked)
      for (let i = 0; i < 80 && !(count(0, "health") && count(0, "sync", true)); i++) { await page.clock.runFor(500); await sleep(25); }   // (the first sync is a full one, made at load: not part of what is counted)
      return { page, context, errors };
    }
    /** Move the fake clock by `ms` in steps, giving the page's own requests real time to land between steps; `touch` presses a key now and then. */
    async function run(page, ms, touchEvery) {
      const STEP = 10000; let since = 0;
      for (let t = 0; t < ms; t += STEP) {
        await page.clock.runFor(STEP); await sleep(12);
        since += STEP; if (touchEvery && since >= touchEvery) { since = 0; await page.keyboard.press("Shift"); }
      }
      await sleep(60);
    }
    const hidden = (page, on) => page.evaluate(h => { Object.defineProperty(document, "hidden", { configurable: true, get: () => h }); Object.defineProperty(document, "visibilityState", { configurable: true, get: () => (h ? "hidden" : "visible") }); document.dispatchEvent(new Event("visibilitychange")); }, on);

    // ── 1 · a window a person works in (an input every 4 minutes): 15-minute full syncs and the minute light, unchanged ──
    {
      const { page, context, errors } = await open();
      const s0 = log.length;
      await run(page, 60 * 60000, 4 * 60000);
      const full = count(s0, "sync", true), health = count(s0, "health");
      check(full >= 3 && full <= 5, `worked in for an hour: ${full} full syncs (every 15 minutes: 3 to 5)`);
      check(health >= 40, `worked in for an hour: ${health} light checks (about every minute)`);
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }

    // ── 2 · a window nobody touches: hourly full syncs, a light check every 3 minutes ──
    {
      const { page, context, errors } = await open();
      const s0 = log.length;
      await run(page, 60 * 60000, 0);
      const full = count(s0, "sync", true), health = count(s0, "health"), all = count(s0, "sync");
      check(full <= 1, `untouched for an hour: ${full} full sync(s) (was 4)`);
      check(health >= 12 && health <= 28, `untouched for an hour: ${health} light checks (was about 60)`);
      check(all >= 10, `the deltas go on: ${all} syncs in the hour`);
      // the first input after the quiet spell: a fresh light, and the 15-minute rule again
      const h0 = log.length;
      await page.keyboard.press("Shift");
      for (let i = 0; i < 30 && !count(h0, "health"); i++) { await page.clock.runFor(500); await sleep(25); }
      check(count(h0, "health") >= 1, "the first input after the quiet spell makes the light fresh within seconds");
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }

    // ── 3 · a hidden tab: no periodic full sync, the deltas and the (already slower) light go on; back in sight it is complete again at once ──
    {
      const { page, context, errors } = await open();
      await page.keyboard.press("Shift");
      await hidden(page, true);
      const s0 = log.length;
      await run(page, 60 * 60000, 0);
      const full = count(s0, "sync", true), health = count(s0, "health"), all = count(s0, "sync");
      check(full === 0, `hidden for an hour: ${full} full syncs (was 4)`);
      check(health <= 28, `hidden for an hour: ${health} light checks`);
      check(all >= 5, `the deltas go on while hidden: ${all} syncs`);
      const b0 = log.length;
      await page.keyboard.press("Shift");   // (the person is back: an input, and the tab shown)
      await hidden(page, false);
      for (let i = 0; i < 40 && !count(b0, "sync", true); i++) { await page.clock.runFor(500); await sleep(25); }
      check(count(b0, "sync", true) >= 1, "shown again: the due full sync is made within seconds");
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }
  } finally { await browser.close(); srv.close(); }
  console.log(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nru-S2-mail-idle OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
