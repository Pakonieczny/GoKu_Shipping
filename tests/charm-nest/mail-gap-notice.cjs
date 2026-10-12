// The sorter's "Messages missing" notice (MAILMISS), in headless Chromium against a fake order-link endpoint (nothing reaches Etsy, the
// inbox or a customer). Paul could not clear it: Check now only re-read the same stored sentence and there was no way to close it.
//   · the notice names the order (never a buyer), what is short, what the inbox is doing, when it last tried and when it tries next;
//   · Check now shows a small labelled spinner ("Re-reading..."), then a result line: "Re-read: all 8 messages match." (green, the notice
//     goes) or "Still 3 short (order ...), trying again at HH:MM." (amber); the answer's judgement replaces the old one at once;
//   · Dismiss sends the signature of the gap on screen, says it was dismissed and that it comes back if the gap changes; a "changed"
//     answer shows the notice again; both work in the sandbox (the envelope shows the real link there too);
//   · the notice clearing by itself (the next sync says all is well) is told in the open panel with no press; the tab says
//     "The missing-messages notice has cleared" once, not "The email link is working again";
//   · red for a gap that stayed: "Messages are still missing", not "The email link is down"; other problems keep the old box, and their
//     Check now still works.
//   node tests/charm-nest/mail-gap-notice.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
let chromium; try { ({ chromium } = require(path.join(pwDir, "playwright-core"))); } catch (_) { console.log("  - no playwright-core: the browser checks were not run"); process.exit(0); }

const RID = "4174973193", SIG = "a1b2c3d4e5f6";
const NOW0 = Date.now();
const gapItem = (over) => Object.assign({ o: RID, k: "fewer_than_counted", c: 8, s: 5, st: "wait", lt: NOW0 - 12 * 60000, nt: NOW0 + 48 * 60000, tr: 1, fa: NOW0 - 3 * 3600000 }, over || {});
const gapOf = (over, itemOver) => Object.assign({ n: 1, noAddr: 0, esc: false, sig: SIG, items: [gapItem(itemOver)], more: 0 }, over || {});
const okLink = (over = {}) => Object.assign({ level: "ok", short: "", problem: "", atMs: Date.now(), downSinceMs: 0, incidentId: "" }, over);
const gapLink = (over = {}, gap) => okLink(Object.assign({ level: "warn", short: "Messages missing", problem: "Order 4174973193 shows 3 fewer messages in the inbox than expected (8 expected, 5 stored). The inbox asked the Etsy helper to re-read it 12 min ago and is waiting for the helper's turn.", kind: "consistency", gap: gap || gapOf(), downSinceMs: Date.now() - 20 * 60000, incidentId: "inc-gap" }, over));
const okHealth = () => ({ at: Date.now(), level: "ok", short: "", problem: "", checks: [{ id: "helper", label: "Etsy helper", level: "ok", text: "Checked in 1 min ago" }] });
const gapHealth = (link) => ({ at: Date.now(), level: link.level, short: link.short, problem: link.problem, kind: link.kind, gap: link.gap, checks: [{ id: "helper", label: "Etsy helper", level: "ok", text: "Checked in 1 min ago" }, { id: "consistency", label: "Missing messages", level: link.level, text: link.problem, data: link.gap }] });
const mode = { link: null, health: null, recheck: null, recheckDelay: 0, dismiss: null, recheckFail: 0 };
const log = [], bodies = [];
const reset = () => Object.assign(mode, { link: okLink(), health: okHealth(), recheck: null, recheckDelay: 0, dismiss: null, recheckFail: 0 });

function harness(sandbox) {
  return `<!doctype html><html><head><meta charset="utf-8"><title>Charm Sorter</title>
<style>[hidden]{display:none!important}.hidden{display:none!important}dialog{width:420px;height:520px}.cm{display:flex;flex-direction:column}</style></head>
<body><div class="topbar"><div class="topTools"></div></div><div id="toasts"></div>
<script>
window.S = { settings: { sandbox: ${JSON.stringify(sandbox ? "on" : "off")}, sound: "on", notify: "on" } };
window.__toasts = []; window.__dings = 0; window.__notes = [];
window.toast = (m, k) => { window.__toasts.push({ m: String(m), k: k || "" }); };
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
      if (u.pathname === "/h.html") { res.writeHead(200, { "Content-Type": "text/html" }); return res.end(harness(u.searchParams.get("sandbox") === "on")); }
      if (u.pathname === "/charm-nest-mail.js") { res.writeHead(200, { "Content-Type": "text/javascript" }); return res.end(fs.readFileSync(path.join(root, "charm-nest-mail.js"))); }
      if (u.pathname.endsWith("/etsyMailOrderLink")) {
        let raw = ""; req.on("data", c => raw += c); req.on("end", () => {
          let b = {}; try { b = JSON.parse(raw || "{}"); } catch (_) {}
          log.push(b.op); bodies.push(b);
          const send = (code, obj, delay) => setTimeout(() => { res.writeHead(code, { "Content-Type": "application/json" }); res.end(JSON.stringify(obj)); }, delay || 0);
          switch (b.op) {
            case "sync": return send(200, { n: 1, v: 1, full: !!b.full, changes: [], link: mode.link });
            case "health": return send(200, mode.health);
            case "link_recheck": return mode.recheckFail ? send(500, { error: "Firestore could not be read" }) : send(200, mode.recheck || { at: Date.now(), ran: false, queued: 0, gap: null, results: [], health: okHealth(), link: null }, mode.recheckDelay);
            case "link_dismiss": return send(200, mode.dismiss || { ok: true, health: okHealth(), link: okLink() });
            case "history_info": return send(200, { receiptId: RID, threads: [], total: 0, exact: true, why: "none", reason: "" });
            case "order": return send(200, { receiptId: RID, sandbox: false, conversation: null, engagements: [], active: null, lookupFailed: null });
            default: return send(200, {});
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
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? "  ✓ " : "  ✗ ") + msg); };
  try {
    async function open(sandbox) {
      reset(); log.length = 0; bodies.length = 0;
      const context = await browser.newContext({ viewport: { width: 900, height: 800 } });
      await context.addInitScript(() => { try { if (!sessionStorage.getItem("__seeded")) { localStorage.clear(); localStorage.setItem("cn.mail.station", JSON.stringify("k-test-station-key-0123456789abcdef")); sessionStorage.setItem("__seeded", "1"); } } catch (_) {} });
      const page = await context.newPage(), errors = [];
      page.setDefaultTimeout(15000);
      page.on("pageerror", e => { errors.push(e.message); console.error("page error:", e.message); });
      await page.goto(`${origin}/h.html${sandbox ? "?sandbox=on" : ""}`, { waitUntil: "load" });
      await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
      return { page, context, errors };
    }
    const until = (p, fn, arg) => p.waitForFunction(fn, arg, { timeout: 12000 });
    const sync = async p => { const n = log.filter(x => x === "sync").length; await p.evaluate(() => document.dispatchEvent(new Event("visibilitychange"))); for (let i = 0; i < 40 && log.filter(x => x === "sync").length <= n; i++) await new Promise(r => setTimeout(r, 50)); await new Promise(r => setTimeout(r, 150)); };
    const menu = p => p.evaluate(() => { const m = document.querySelector(".mailMenu"); return m ? m.innerText : null; });
    const openMenu = async p => { await p.evaluate(() => { const m = document.querySelector(".mailMenu"); if (m) m.remove(); document.getElementById("mailPill").click(); }); };
    const pill = p => p.evaluate(() => { const b = document.getElementById("mailPill"); return b ? { hidden: b.classList.contains("hidden"), cls: [...b.classList].filter(c => c === "bad" || c === "warn").join("") } : null; });
    const toasts = p => p.evaluate(() => window.__toasts.map(t => t.m));
    const clickIn = (p, sel) => p.evaluate(s => { const b = document.querySelector(".mailMenu " + s); if (b) b.click(); return !!b; }, sel);

    for (const sandbox of [false, true]) {
      const tag = sandbox ? "sandbox: " : "";
      const { page, context, errors } = await open(sandbox);
      // ── 1 · the notice says which conversation, what is short, what the inbox does, last and next try ──
      mode.link = gapLink(); mode.health = gapHealth(mode.link);
      await sync(page);
      await until(page, () => { const b = document.getElementById("mailPill"); return b && !b.classList.contains("hidden"); });
      check((await pill(page)).cls === "warn", tag + "the envelope is amber for the gap");
      await openMenu(page);
      let m = await menu(page);
      console.log("   panel:", m.replace(/\s+/g, " "));
      check(/Messages missing\./.test(m) && /Order 4174973193 shows 3 fewer messages in the inbox than expected \(8 expected, 5 stored\)/.test(m), tag + "it names the order and the counts");
      check(/asked the Etsy helper to re-read it 12 min ago/.test(m) && /waiting for the helper's turn/.test(m), tag + "it says what the inbox is doing and when it last tried");
      check(!/Ada|Byrne/.test(m), tag + "no buyer name");
      check(/Check now/.test(m) && /Dismiss/.test(m) && /Nothing is lost/.test(m), tag + "Check now and Dismiss are there, with a line saying what they do");
      // a gap that was read and is still short: the next try at a clock time
      mode.link = gapLink({}, gapOf({}, { st: "retry", lt: Date.now() - 10 * 60000, nt: Date.now() + 13 * 60000 })); mode.link.gap = gapOf({}, { st: "retry", lt: Date.now() - 10 * 60000, nt: Date.now() + 13 * 60000 }); mode.health = gapHealth(mode.link);
      await sync(page);
      m = await menu(page);
      check(/Last tried 10 min ago, still short\. Next try about \d{1,2}:\d{2}.*\(in 13 min\)/.test(m), tag + "after a read that did not fix it: 'Last tried 10 min ago, still short. Next try about HH:MM (in 13 min)': " + (m.match(/Last tried[^.]*\.[^.]*\./) || [""])[0].replace(/\s+/g, " "));

      // ── 2 · Check now: a labelled spinner, then the answer in a line ──
      mode.recheckDelay = 900;
      const keepGap = gapLink({}, gapOf({}, { st: "retry", lt: Date.now() - 10 * 60000, nt: Date.now() + 13 * 60000 }));
      mode.recheck = { at: Date.now(), ran: true, queued: 0, gap: keepGap.gap, results: [{ o: RID, outcome: "short", c: 8, s: 5, tr: 1, lt: Date.now() - 10 * 60000, nt: Date.now() + 13 * 60000, q: false }], health: gapHealth(keepGap), link: keepGap };
      await clickIn(page, "[data-pill-check]");
      await until(page, () => /Re-reading/.test(document.querySelector(".mailMenu").innerText));
      m = await menu(page);
      check(/Re-reading…/.test(m) && await page.evaluate(() => !!document.querySelector(".mailMenu .cmSpin") && document.querySelector(".mailMenu [data-pill-check]").disabled), tag + "pressing Check now shows a small spinner with the words 'Re-reading…' and disables the buttons");
      await until(page, () => /Still 3 short/.test(document.querySelector(".mailMenu").innerText));
      m = await menu(page);
      check(/Still 3 short \(order 4174973193\), trying again at \d{1,2}:\d{2}/.test(m), tag + "the result line: " + (m.match(/Still 3 short[^\n]*/) || [""])[0]);
      check(log.includes("link_recheck"), tag + "Check now asked the server to re-run the comparison (link_recheck), not only to re-read the stored sentence");

      // ── 3 · Check now finds the counts equal: green line, the notice and the envelope go, with no further press ──
      mode.recheckDelay = 0;
      mode.link = okLink();
      mode.recheck = { at: Date.now(), ran: true, queued: 0, gap: null, results: [{ o: RID, outcome: "proven", c: 8, s: 8, tr: 2, lt: Date.now() - 60000, nt: 0, q: false }], health: okHealth(), link: okLink() };
      await clickIn(page, "[data-pill-check]");
      await until(page, () => /Re-read from Etsy: nothing is missing/.test(document.querySelector(".mailMenu").innerText));
      m = await menu(page);
      check(/✓/.test(m) && /all 8 messages are in the inbox/.test(m) && !/Messages missing/.test(m), tag + "the notice becomes a green line 'Re-read from Etsy: nothing is missing (all 8 messages are in the inbox)'");
      check(await page.evaluate(() => !!document.querySelector(".mailMenu .cmWarn.ok")), tag + "in the green style");
      await until(page, () => { const b = document.getElementById("mailPill"); return b && b.classList.contains("hidden"); });
      check(true, tag + "the envelope goes quiet");
      await page.evaluate(() => { const m = document.querySelector(".mailMenu"); if (m) m.remove(); });
      // the matched wording
      mode.link = gapLink(); mode.health = gapHealth(mode.link);
      await sync(page); await openMenu(page);
      mode.recheck = { at: Date.now(), ran: true, queued: 0, gap: null, results: [{ o: RID, outcome: "matched", c: 12, s: 12, tr: 1, lt: 0, nt: 0, q: false }], health: okHealth(), link: okLink() };
      await clickIn(page, "[data-pill-check]");
      await until(page, () => /Re-read: all 12 messages match\./.test(document.querySelector(".mailMenu").innerText));
      check(true, tag + "counts equal again: 'Re-read: all 12 messages match.'");
      await page.evaluate(() => { const m = document.querySelector(".mailMenu"); if (m) m.remove(); });

      // ── 4 · a failed re-check is said, and the notice stays ──
      mode.link = gapLink(); mode.health = gapHealth(mode.link); mode.recheckFail = 1;
      await sync(page); await openMenu(page);
      await clickIn(page, "[data-pill-check]");
      await until(page, () => /Could not re-check just now/.test(document.querySelector(".mailMenu").innerText));
      m = await menu(page);
      check(/Could not re-check just now \(Firestore could not be read\)\. Try again in a minute\./.test(m) && /Messages missing/.test(m), tag + "a failed re-check says why and the notice stays: never 'all clear'");
      mode.recheckFail = 0;

      // ── 5 · Dismiss: sends the signature on screen, says what it did, the notice goes ──
      bodies.length = 0;
      mode.dismiss = { ok: true, health: okHealth(), link: okLink() };
      await clickIn(page, "[data-gap-dismiss]");
      await until(page, () => /Dismissed\./.test(document.querySelector(".mailMenu").innerText));
      m = await menu(page);
      const dBody = bodies.find(b => b.op === "link_dismiss");
      check(dBody && dBody.sig === SIG, tag + "Dismiss sends the signature of the gap that was on screen");
      check(/Dismissed\. It comes back only if the gap changes or another conversation falls short; the inbox keeps re-reading it\./.test(m) && !/Messages missing\./.test(m), tag + "and says what it did and when it comes back");
      await until(page, () => { const b = document.getElementById("mailPill"); return b && b.classList.contains("hidden"); });
      check(true, tag + "the envelope goes quiet");
      await page.evaluate(() => { const m = document.querySelector(".mailMenu"); if (m) m.remove(); });
      // the gap changed meanwhile
      mode.link = gapLink(); mode.health = gapHealth(mode.link);
      await sync(page); await openMenu(page);
      const worse = gapLink({}, gapOf({ sig: "ffffffffffff" }, { c: 12, s: 5 }));
      mode.dismiss = { ok: false, changed: true, nothing: false, health: gapHealth(worse), link: worse };
      await clickIn(page, "[data-gap-dismiss]");
      await until(page, () => /The gap changed just now/.test(document.querySelector(".mailMenu").innerText));
      m = await menu(page);
      check(/shows 7 fewer messages/.test(m) && /Dismiss/.test(m), tag + "'changed': the notice is shown again with the new numbers");
      await page.evaluate(() => { const m = document.querySelector(".mailMenu"); if (m) m.remove(); });

      check(errors.length === 0, tag + "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }

    // ── 6 · it clears by itself: the next sync says all is well while the panel is open, and the tab says so once ──
    {
      const { page, context, errors } = await open(false);
      mode.link = gapLink(); mode.health = gapHealth(mode.link);
      await sync(page);
      await until(page, () => window.__toasts.length >= 1);
      let t = await toasts(page);
      check(t.length === 1 && /^Messages missing: Order 4174973193 shows 3 fewer messages/.test(t[0]), "the alert (after 5 minutes) says 'Messages missing: …', not 'The email link needs attention': " + t[0].slice(0, 70));
      await openMenu(page);
      mode.link = okLink(); mode.health = okHealth();
      await sync(page);
      await until(page, () => /All messages match\./.test(document.querySelector(".mailMenu").innerText));
      check(await page.evaluate(() => !!document.querySelector(".mailMenu .cmWarn.ok")), "the open panel turns to a green 'All messages match.' with nobody pressing anything");
      await until(page, () => window.__toasts.length >= 2);
      t = await toasts(page);
      check(/All messages match: the missing-messages notice has cleared\./.test(t[t.length - 1]) && !/working again/.test(t.join("|")), "and the tab says so once, in its own words: " + t[t.length - 1]);
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }

    // ── 7 · red for a gap that stayed; other problems keep the old box and their Check now works ──
    {
      const { page, context, errors } = await open(false);
      mode.link = gapLink({ level: "down", short: "Messages still missing", problem: "Order 4174973193 shows 3 fewer messages in the inbox than expected (8 expected, 5 stored). It has stayed short for 3 days after 4 tries: open it in the inbox and compare with Etsy." }, gapOf({ esc: true }, { st: "retry", tr: 4 }));
      mode.link.gap = gapOf({ esc: true }, { st: "retry", tr: 4, fa: Date.now() - 80 * 3600000 });
      mode.health = gapHealth(mode.link);
      await sync(page);
      await until(page, () => window.__toasts.length >= 1);
      const t = await toasts(page);
      check(/^Messages are still missing: /.test(t[0]) && !/is down/.test(t[0]), "red for a gap that stayed: 'Messages are still missing: …', not 'The email link is down': " + t[0].slice(0, 60));
      check((await pill(page)).cls === "bad", "the envelope is red");
      await openMenu(page);
      const m = await menu(page);
      check(/Short for 3 days after 4 tries: open it in the inbox and compare with Etsy\./.test(m), "the panel says how long, how many tries, and what to do: " + (m.match(/Short for[^\n]*/) || [""])[0]);
      mode.link = okLink({ level: "warn", short: "Etsy helper idle", problem: "The Etsy helper has not checked in for 40 minutes.", downSinceMs: Date.now() - 2 * 60000, incidentId: "inc2" });
      mode.health = okHealth();
      await sync(page); await openMenu(page);
      const m2 = await menu(page);
      check(/Etsy helper idle\./.test(m2) && /Check now/.test(m2) && !/Dismiss/.test(m2), "another problem: the old box, with Check now only (nothing to dismiss)");
      bodies.length = 0;
      mode.recheck = null;   // (no gap open: the server answers with the plain health)
      await clickIn(page, "[data-pill-check]");
      for (let i = 0; i < 40 && !log.includes("link_recheck"); i++) await new Promise(r => setTimeout(r, 50));
      check(log.includes("link_recheck"), "its Check now asks the server too (which answers with the plain health when no gap is open)");
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }
  } finally { await browser.close(); srv.close(); }
  console.log(fails.length ? `\nmail-gap-notice FAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nmail-gap-notice OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
