// "Reconnect & retry all" (LINKRECOVER, 20 Oct 2026) in the sorter's envelope menu, in headless Chromium against a fake order-link
// endpoint (nothing reaches Etsy, the inbox or a customer). Paul could not clear "A message has waited 58 min for the inbox's Etsy helper
// to send it": a red line is never dismissed, it is repaired.
//   · a failing line offers "Reconnect & retry all" next to Check now; pressing it starts ONE run (link_recover) and the button turns into
//     a labelled spinner while the run goes; the page follows the run (link_recover_status) until it ends;
//   · the run's summary is shown in a line, green when the line works again, amber with what is left otherwise; the answer's judgement
//     replaces the old one at once, so the light follows without waiting for the next check;
//   · a second press while the run goes does not start another.
//   node tests/charm-nest/mail-recover-button.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
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
const mode = { link: null, health: null, recheck: null, recheckDelay: 0, dismiss: null, recheckFail: 0, polls: 0, healthAfter: null, recoverStatus: () => ({ state: "never" }) };
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
            case "link_recover": return send(200, { started: true, joined: false, status: mode.recoverStatus() });
            case "link_recover_status": { mode.polls++; const st = mode.recoverStatus(); return send(200, st.state === "running" ? { status: st } : { status: st, health: mode.healthAfter || okHealth() }); }
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


const downLink = () => okLink({ level: "down", short: "Not sending", problem: "A message has waited 58 min for the inbox's Etsy helper to send it.", kind: "send", downSinceMs: Date.now() - 60 * 60000, incidentId: "inc-send" });
const downHealth = () => ({ at: Date.now(), level: "down", short: "Not sending", problem: "A message has waited 58 min for the inbox's Etsy helper to send it.", kind: "send", checks: [{ id: "helper", label: "Etsy helper", level: "ok", text: "Last checked 6 s ago" }, { id: "send", label: "Sending", level: "down", text: "A message has waited 58 min for the inbox's Etsy helper to send it." }] });

(async () => {
  const srv = await server(), origin = "http://127.0.0.1:" + srv.address().port;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? "  ✓ " : "  ✗ ") + msg); };
  try {
    reset(); Object.assign(mode, { polls: 0, healthAfter: null });
    const context = await browser.newContext({ viewport: { width: 900, height: 800 } });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem("__seeded")) { localStorage.clear(); localStorage.setItem("cn.mail.station", JSON.stringify("k-test-station-key-0123456789abcdef")); sessionStorage.setItem("__seeded", "1"); } } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(15000);
    page.on("pageerror", e => { errors.push(e.message); console.error("page error:", e.message); });
    await page.goto(origin + "/h.html", { waitUntil: "load" });
    await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
    const sync = async () => { const n = log.filter(x => x === "sync").length; await page.evaluate(() => document.dispatchEvent(new Event("visibilitychange"))); for (let i = 0; i < 40 && log.filter(x => x === "sync").length <= n; i++) await new Promise(r => setTimeout(r, 50)); await new Promise(r => setTimeout(r, 150)); };
    const menu = () => page.evaluate(() => { const m = document.querySelector(".mailMenu"); return m ? m.innerText : null; });
    const openMenu = () => page.evaluate(() => { const m = document.querySelector(".mailMenu"); if (m) m.remove(); document.getElementById("mailPill").click(); });

    mode.link = downLink(); mode.health = downHealth();
    await sync();
    await page.waitForFunction(() => { const b = document.getElementById("mailPill"); return b && !b.classList.contains("hidden"); });
    await openMenu();
    let m = await menu();
    check(/Not sending/.test(m) && /waited 58 min/.test(m), "the menu shows the red line: " + m.replace(/\s+/g, " ").slice(0, 90));
    check(/Reconnect & retry all/.test(m), "...and offers Reconnect & retry all");

    // the run: two reads while it goes, then done and green
    const t0 = Date.now();
    const steps = [{ id: "send", label: "Sending", level: "ok", text: "Put 1 message that had lost its turn back in the queue. The next message has been handed to the Etsy helper.", atMs: t0 }];
    mode.recoverStatus = () => mode.polls < 2 ? { runId: "r1", state: "running", startedAtMs: t0, steps, by: "Bo" }
      : { runId: "r1", state: "done", startedAtMs: t0, finishedAtMs: Date.now(), steps: steps.concat([{ id: "helper", label: "Etsy helper", level: "ok", text: "The Etsy helper is answering: it is sending again.", atMs: Date.now() }]), after: { level: "ok" }, summary: "Everything works again." };
    await page.evaluate(() => document.querySelector(".mailMenu [data-pill-recover]").click());
    await page.waitForFunction(() => { const b = document.querySelector(".mailMenu [data-pill-recover]"); return b && b.disabled && /Reconnecting/.test(b.textContent); });
    check(true, "the button turns into a labelled spinner ('Reconnecting…') while the run goes");
    await page.evaluate(() => { const b = document.querySelector(".mailMenu [data-pill-recover]"); if (b) b.click(); });
    check(log.filter(x => x === "link_recover").length === 1, "a second press while it runs does not start another run");
    mode.link = okLink(); mode.healthAfter = okHealth(); mode.health = okHealth();   // (the server's line, once the run has repaired it)
    await page.waitForFunction(() => { const m = document.querySelector(".mailMenu"); return m && /Everything works again/.test(m.innerText); }, null, { timeout: 20000 });
    check(mode.polls >= 2, "the page followed the run (" + mode.polls + " status reads)");
    check(true, "the run's summary is shown: 'Everything works again.'");
    await sync();
    const pill = await page.evaluate(() => { const b = document.getElementById("mailPill"); return b.classList.contains("bad") || b.classList.contains("warn"); });
    check(!pill, "the envelope is no longer red or amber");

    // a run that leaves the helper stuck: amber, with what a person must do
    mode.link = downLink(); mode.health = downHealth(); mode.polls = 0;
    mode.recoverStatus = () => ({ runId: "r2", state: "done", startedAtMs: Date.now(), finishedAtMs: Date.now(), steps: [{ id: "helper", label: "Etsy helper", level: "down", text: "The Etsy helper is heard but it was given a message and did not send it." }], after: { level: "down" }, summary: "The Etsy helper is heard but it was given a message and did not send it. On the computer that has the inbox's Etsy extension: open chrome://extensions and press the extension's reload arrow." });
    mode.healthAfter = downHealth();
    await sync(); await openMenu();
    await page.evaluate(() => document.querySelector(".mailMenu [data-pill-recover]").click());
    await page.waitForFunction(() => { const m = document.querySelector(".mailMenu"); return m && /chrome:\/\/extensions/.test(m.innerText); }, null, { timeout: 20000 });
    m = await menu();
    check(/did not send it/.test(m) && /chrome:\/\/extensions/.test(m), "when only the helper's computer can fix it, the line says exactly what to do there");
    check(!errors.length, "no page errors");
    await context.close();
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.log("\n" + fails.length + " FAILED"); process.exit(1); }
  console.log("\nmail-recover-button OK");
})().catch(e => { console.error(e); process.exit(1); });
