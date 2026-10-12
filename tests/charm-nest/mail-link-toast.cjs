// MAILDOWN (12 Oct 2026): the sorter's red "email link" notice, in headless Chromium with the sorter's REAL toast function
// (taken from charm-nest-1.html) against a fake order-link endpoint. Nothing reaches Etsy, the inbox or a customer.
// Paul's screenshot showed three stacked notices for one trouble ("needs attention ... 18 min", "needs attention ... 13 min",
// "is down ... 24 min"). Now:
//   · ONE red notice per incident; every new judgement of the monitor changes its words in place (13 -> 18 -> 24 min);
//   · it dings once when it appears and once when it gets worse ("needs attention", then "is down"), never on a refresh;
//   · closing it keeps it closed until the state changes (worse, or a new incident);
//   · when the line works again the red notice goes and one plain line says so;
//   · the words are the server's (_etsyMailLinkHealth.js): "last checked 18 min ago", then what to do.
//   node tests/charm-nest/mail-link-toast.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
let chromium; try { ({ chromium } = require(path.join(pwDir, "playwright-core"))); } catch (_) { console.log("  - no playwright-core: the browser checks were not run"); process.exit(0); }
const LH = require(path.join(root, "netlify/functions/_etsyMailLinkHealth.js"));

// the sorter's own toast function and its CSS, so the stack, the limit of three and the dismiss behave as they do for Paul
const sorter = fs.readFileSync(path.join(root, "charm-nest-1.html"), "utf8");
const toastSrc = sorter.slice(sorter.indexOf('function toast(msg, kind = "", ms = 3600, key = "") {'), sorter.indexOf("function openDlg(el)"));
const toastCss = sorter.slice(sorter.indexOf(".toasts{position:fixed"), sorter.indexOf("\n", sorter.indexOf(".toasts{position:fixed")));
if (!/function toast\(/.test(toastSrc) || !toastCss) { console.error("could not find the sorter's toast function"); process.exit(1); }

const MIN = 60e3, NOW = Date.now();
const mode = { link: null };
const okLink = (over = {}) => Object.assign({ level: "ok", short: "", problem: "", atMs: Date.now(), downSinceMs: 0, incidentId: "" }, over);
/** The server's own words for a helper that last checked `min` minutes ago (nothing waiting). */
const helperLink = (min, id, over = {}) => {
  const ev = { global: {}, watcher: { enabled: true }, gmail: { lastSyncCompletedAt: NOW - MIN }, helper: { seenAtMs: NOW - min * MIN }, scrapeHealth: { consecutiveBad: 0 }, mirror: { enabled: true, lastSyncCompletedAt: NOW - 3 * MIN },
    bell: { reconcileAtMs: NOW - MIN }, monitor: { atMs: NOW - MIN }, queuedDrafts: [], scrapeJobs: [], readOk: {}, queue: null };
  const r = LH.evaluate(Object.assign(ev, over), NOW);
  return okLink({ level: r.level, short: r.short, problem: r.problem, downSinceMs: Date.now() - 6 * MIN, incidentId: id });
};

function harness() {
  return `<!doctype html><html><head><meta charset="utf-8"><title>Charm Sorter</title>
<style>[hidden]{display:none!important}.hidden{display:none!important}dialog{width:420px;height:520px}${toastCss}</style></head>
<body><div class="topbar"><div class="topTools"></div></div><div class="toasts" id="toasts"></div>
<script>
const $ = s => document.querySelector(s);
${toastSrc}
window.S = { settings: { sandbox: "off", sound: "on", notify: "on" } };
window.__dings = 0; window.__notes = [];
window.ding = () => { window.__dings++; };
window.notifyPerson = (t, b) => { window.__notes.push({ t, b }); };
window.esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
</script>
<script src="/charm-nest-mail.js"></script></body></html>`;
}

function server() {
  return new Promise(resolve => {
    const srv = http.createServer((req, res) => {
      const u = new URL(req.url, "http://x");
      if (u.pathname === "/h.html") { res.writeHead(200, { "Content-Type": "text/html" }); return res.end(harness()); }
      if (u.pathname === "/charm-nest-mail.js") { res.writeHead(200, { "Content-Type": "text/javascript" }); return res.end(fs.readFileSync(path.join(root, "charm-nest-mail.js"))); }
      if (u.pathname.endsWith("/etsyMailOrderLink")) {
        let raw = ""; req.on("data", c => raw += c); req.on("end", () => {
          let b = {}; try { b = JSON.parse(raw || "{}"); } catch (_) {}
          const send = (code, obj) => { res.writeHead(code, { "Content-Type": "application/json" }); res.end(JSON.stringify(obj)); };
          if (b.op === "sync") return send(200, { n: 1, v: 1, full: !!b.full, changes: [], now: Date.now(), link: mode.link });
          if (b.op === "health") return send(200, { at: Date.now(), level: "ok", short: "", problem: "", checks: [] });
          return send(200, {});
        });
        return;
      }
      res.writeHead(404); res.end();
    }).listen(0, "127.0.0.1", () => resolve(srv));
  });
}

(async () => {
  const srv = await server(), origin = "http://127.0.0.1:" + srv.address().port;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? "  ✓ " : "  ✗ ") + msg); };
  try {
    mode.link = okLink();
    const context = await browser.newContext({ viewport: { width: 900, height: 800 } });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem("__seeded")) { localStorage.clear(); localStorage.setItem("cn.mail.station", JSON.stringify("k-test-station-key-0123456789abcdef")); sessionStorage.setItem("__seeded", "1"); } } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(15000);
    page.on("pageerror", e => { errors.push(e.message); console.error("page error:", e.message); });
    await page.goto(`${origin}/h.html`, { waitUntil: "load" });
    await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());

    const shown = () => page.evaluate(() => [...document.querySelectorAll("#toasts .toast")].filter(t => t.style.opacity !== "0").map(t => ({ k: t.className.replace("toast", "").trim(), m: t.dataset.msg, key: t.dataset.key || "" })));
    const red = async () => (await shown()).filter(t => t.k === "bad");
    const dings = () => page.evaluate(() => window.__dings);
    const sync = async () => { await page.evaluate(() => CustomerMail._state && document.dispatchEvent(new Event("visibilitychange"))); await new Promise(r => setTimeout(r, 700)); };
    const settle = async () => { await new Promise(r => setTimeout(r, 400)); };
    const take = async link => { mode.link = link; await sync(); await settle(); };
    const closeIt = () => page.evaluate(() => document.querySelector("#toasts .toast.bad .c").click());
    const mark = () => page.evaluate(() => { const t = document.querySelector("#toasts .toast.bad"); if (t) t.dataset.mark = "same"; });
    const marked = () => page.evaluate(() => { const t = document.querySelector("#toasts .toast.bad"); return !!t && t.dataset.mark === "same"; });

    // ── the screenshot's trouble: the Etsy helper quiet, judged every five minutes ──
    // (helperLink puts the start of the trouble 6 minutes back, so the first judgement already counts as 5 minutes of trouble)
    await take(helperLink(13, "inc-a"));
    const first = await red();
    check(first.length === 1 && first[0].key === "email-link" && /^The email link needs attention: The inbox's Etsy helper last checked 13 min ago\. Open Chrome on the computer that has the inbox's Etsy extension; messages wait and nothing is lost\.$/.test(first[0].m), "one red notice, in the server's words: " + (first[0] && first[0].m));
    check(!/checked in/i.test(first[0].m), "it never says 'last checked in'");
    check((await dings()) === 1 && (await page.evaluate(() => window.__notes.length)) === 1, "one ding and one desktop notification when it appears");
    await mark();

    await take(helperLink(18, "inc-a"));
    let r = await red();
    check(r.length === 1 && /last checked 18 min ago/.test(r[0].m) && (await marked()), "five minutes later: still ONE notice, the same one, now saying 18 min ago");
    await take(helperLink(24, "inc-a"));
    r = await red();
    check(r.length === 1 && /last checked 24 min ago/.test(r[0].m) && (await marked()) && /needs attention/.test(r[0].m), "and again, 24 min ago: still one, still 'needs attention' (the red words wait for the red threshold)");
    check((await dings()) === 1, "no ding for a refresh");
    await sync(); await sync();
    check((await red()).length === 1 && (await dings()) === 1, "polling again and again changes nothing");

    await take(helperLink(31, "inc-a"));
    r = await red();
    check(r.length === 1 && /^The email link is down: The inbox's Etsy helper last checked 31 min ago, so new answers are not being read\. Open Chrome/.test(r[0].m) && (await marked()), "past 30 minutes: the same notice now says 'is down'");
    check((await dings()) === 2, "one more ding for getting worse");
    await take(helperLink(36, "inc-a"));
    r = await red();
    check(r.length === 1 && /last checked 36 min ago/.test(r[0].m) && (await dings()) === 2, "later numbers keep it in place, no more dings");

    // ── closing it ──
    await closeIt(); await settle();
    check((await red()).length === 0, "closed with the cross: gone");
    await take(helperLink(41, "inc-a")); await take(helperLink(46, "inc-a"));
    check((await red()).length === 0 && (await dings()) === 2, "the same incident, same level, new minutes: it stays closed");
    await page.reload({ waitUntil: "load" }); await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
    await take(helperLink(51, "inc-a"));
    check((await red()).length === 0, "and after a reload (what was told is remembered)");

    // ── a new incident is news again ──
    await take(okLink()); await settle();
    check((await red()).length === 0, "the helper is back: no red notice");
    const ok1 = await shown();
    check(ok1.length === 1 && ok1[0].k === "ok" && /^The email link is working again\.$/.test(ok1[0].m), "one plain line says it works again, even though the red one was closed");
    await take(helperLink(14, "inc-b"));
    r = await red();
    check(r.length === 1 && /last checked 14 min ago/.test(r[0].m) && (await dings()) === 1, "a new incident: a new red notice (and a ding)");

    // ── the red notice goes when the line recovers, whatever tab shows it ──
    await take(okLink()); await settle();
    const after = await shown();
    check((await red()).length === 0 && after.length >= 1 && after.every(t => t.k !== "bad") && after.some(t => /working again/.test(t.m)), "recovery: the red notice is removed and the 'working again' line takes its place");

    // ── Paul's screenshot: separate incidents never stack ──
    await take(helperLink(18, "inc-c"));
    await take(helperLink(13, "inc-d"));   // (the monitor started a new incident id while the first notice was still showing)
    r = await red();
    check(r.length === 1 && /last checked 13 min ago/.test(r[0].m), "a second incident while the first notice is still showing takes its place: one notice, not two (" + r.length + ")");
    await take(helperLink(24, "inc-d", { scrapeJobs: [NOW - 17 * MIN] }));
    r = await red();
    check(r.length === 1 && /^The email link is down: .*last checked 24 min ago, and a message has been waiting 17 min for it\. Open Chrome/.test(r[0].m), "red at 24 minutes says why (a message has waited 17 min): " + (r[0] && r[0].m.slice(0, 120)));
    check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
    await context.close();
  } finally { await browser.close(); srv.close(); }
  console.log(fails.length ? "\nmail-link-toast FAILED: " + fails.join(" | ") : "\nmail-link-toast OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
