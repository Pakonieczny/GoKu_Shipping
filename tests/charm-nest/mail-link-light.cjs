// The sorter's half of the customer-mail health (charm-nest-mail.js), in headless Chromium against a fake order-link
// endpoint (nothing reaches Etsy, the inbox or a customer):
//   · the light under the message box and the top-bar envelope: green only when everything is, amber or red with one plain
//     sentence otherwise; "unknown" (a monitor that never reported or stopped, a check that cannot be made) is amber, never
//     "Checking..." for ever and never green;
//   · the alert: once per incident after 5 minutes, again only if it gets worse, once when it clears;
//   · the Customer pane: "no messages" only when the inbox really answered "none"; a failed look-up says why and asks again.
//   node tests/charm-nest/mail-link-light.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
let chromium; try { ({ chromium } = require(path.join(pwDir, "playwright-core"))); } catch (_) { console.log("  - no playwright-core: the browser checks were not run"); process.exit(0); }

const RID = "4174973193";
const mode = {
  link: null, syncFail: false, health: null, healthFail: false,
  info: null, infoFail: false, order: null, orderFail: false
};
const okHealth = () => ({ at: Date.now(), level: "ok", short: "", problem: "", checks: [{ id: "helper", label: "Etsy helper", level: "ok", text: "Checked in 1 min ago" }] });
const okLink = (over = {}) => Object.assign({ level: "ok", short: "", problem: "", atMs: Date.now(), downSinceMs: 0, incidentId: "" }, over);
const okInfo = () => ({ receiptId: RID, threads: [{ threadId: "t1", count: 3, lastAtMs: Date.now(), orderId: RID, customer: {} }], total: 3, exact: true, why: "ok", reason: "" });
const okOrder = () => ({ receiptId: RID, sandbox: false, conversation: { threadId: "t1", customer: { name: "Ada Byrne" } }, engagements: [], active: null, lookupFailed: null });
const reset = () => Object.assign(mode, { link: okLink(), syncFail: false, health: okHealth(), healthFail: false, info: okInfo(), infoFail: false, order: okOrder(), orderFail: false });
const log = [];

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
          log.push(b.op);
          const send = (code, obj) => { res.writeHead(code, { "Content-Type": "application/json" }); res.end(JSON.stringify(obj)); };
          switch (b.op) {
            case "sync": return mode.syncFail ? send(503, { error: "The inbox link answered 503" }) : send(200, { n: 1, v: 1, full: !!b.full, changes: [], link: mode.link });
            case "health": return mode.healthFail ? send(500, { error: "Firestore could not be read" }) : send(200, mode.health);
            case "order": return mode.orderFail ? send(500, { error: "The inbox could not be read" }) : send(200, mode.order);
            case "history_info": return mode.infoFail ? send(500, { error: "The inbox could not count" }) : send(200, mode.info);
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
      reset(); log.length = 0;
      const context = await browser.newContext({ viewport: { width: 900, height: 800 } });
      await context.addInitScript(() => { try { if (!sessionStorage.getItem("__seeded")) { localStorage.clear(); localStorage.setItem("cn.mail.station", JSON.stringify("k-test-station-key-0123456789abcdef")); sessionStorage.setItem("__seeded", "1"); } } catch (_) {} });
      const page = await context.newPage(), errors = [];
      page.setDefaultTimeout(15000);
      page.on("pageerror", e => { errors.push(e.message); console.error("page error:", e.message); });
      await page.goto(`${origin}/h.html${sandbox ? "?sandbox=on" : ""}`, { waitUntil: "load" });
      await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
      await page.evaluate(rid => CustomerMail.openConversation({ receiptId: rid, scope: "order" }), RID);
      return { page, context, errors };
    }
    const light = p => p.evaluate(() => { const b = document.querySelector("dialog .cmLight"); return b ? { cls: b.className.replace("cmLight", "").trim(), text: b.textContent.trim() } : null; });
    const warn = p => p.evaluate(() => { const w = document.querySelector("dialog [data-cm=warn]"); return w && !w.hidden ? w.textContent.trim() : ""; });
    const pill = p => p.evaluate(() => { const b = document.getElementById("mailPill"); return b ? { hidden: b.classList.contains("hidden"), cls: [...b.classList].filter(c => c === "bad" || c === "warn").join(""), text: b.textContent.trim(), title: b.title } : null; });
    const hist = p => p.evaluate(() => { const e = document.querySelector("dialog [data-cm=hist]"); return e && !e.hidden ? e.textContent.trim() : ""; });
    const notice = p => p.evaluate(() => { const e = document.querySelector("dialog [data-cm=notice]"); return e && !e.hidden ? e.textContent.trim() : ""; });
    const toasts = p => p.evaluate(() => window.__toasts.map(t => t.m));
    const sync = async p => { const n = log.filter(x => x === "sync").length; await p.evaluate(() => document.dispatchEvent(new Event("visibilitychange"))); for (let i = 0; i < 40 && log.filter(x => x === "sync").length <= n; i++) await new Promise(r => setTimeout(r, 100)); await new Promise(r => setTimeout(r, 250)); };
    const until = (p, fn, arg) => p.waitForFunction(fn, arg, { timeout: 12000 });

    // ── 1 · everything works: green, and the envelope has nothing to say ──
    {
      const { page, context, errors } = await open(false);
      await until(page, () => { const b = document.querySelector("dialog .cmLight"); return b && /Active/.test(b.textContent); });
      check((await light(page)).cls === "ok", "everything working: the light says Active (green)");
      await until(page, () => /full history/.test(document.querySelector("dialog [data-cm=hist]").textContent));
      check(/3 messages/.test(await hist(page)), "the buyer's real count shows: " + (await hist(page)));
      const p0 = await pill(page); check(p0 && p0.hidden, "the envelope stays out of the way while nothing is wrong or waiting");
      check((await warn(page)) === "", "no sentence under the box");

      // ── 2 · the monitor reports trouble: red, an alert once after 5 minutes, again only if worse, once when clear ──
      mode.link = okLink({ level: "warn", short: "Etsy helper idle", problem: "The Etsy helper has not checked in for 40 minutes.", downSinceMs: Date.now() - 2 * 60000, incidentId: "inc1" });
      await sync(page);
      await until(page, () => { const b = document.querySelector("dialog .cmLight"); return b && /Etsy helper idle/.test(b.textContent); });
      check((await light(page)).cls === "warn", "monitor says amber: the light is amber with its word (" + (await light(page)).text + ")");
      check(/40 minutes/.test(await warn(page)), "the plain sentence is under the box: " + (await warn(page)));
      let pl = await pill(page);
      check(pl && !pl.hidden && pl.cls === "warn" && pl.text === "!", "the envelope shows amber with a ! though nothing is waiting");
      check(/Etsy helper idle/.test(pl.title), "its tooltip names the problem");
      check((await toasts(page)).length === 0 && (await page.evaluate(() => window.__dings)) === 0, "a trouble of 2 minutes raises no alert yet (a blip says nothing)");

      mode.link = okLink({ level: "warn", short: "Etsy helper idle", problem: "The Etsy helper has not checked in for 40 minutes.", downSinceMs: Date.now() - 6 * 60000, incidentId: "inc1" });
      await sync(page);
      await until(page, () => window.__toasts.length >= 1);
      let t = await toasts(page);
      check(t.length === 1 && /needs attention/.test(t[0]) && /40 minutes/.test(t[0]), "after 5 minutes: one alert with the sentence: " + t[0]);
      check((await page.evaluate(() => window.__dings)) === 1 && (await page.evaluate(() => window.__notes.length)) === 1, "with a ding and a desktop notification call");
      await sync(page); await sync(page);
      check((await toasts(page)).length === 1, "the same incident does not alert again");
      await page.reload({ waitUntil: "load" });
      await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
      await page.evaluate(rid => CustomerMail.openConversation({ receiptId: rid, scope: "order" }), RID);
      await sync(page);
      check((await page.evaluate(() => window.__toasts.length)) === 0, "nor after a reload (what was told is remembered)");

      mode.link = okLink({ level: "down", short: "Sending is off", problem: "Sending to customers is switched off.", downSinceMs: Date.now() - 8 * 60000, incidentId: "inc1" });
      await sync(page);
      await until(page, () => window.__toasts.length >= 1);
      t = await toasts(page);
      check(t.length === 1 && /is down/.test(t[0]), "worse in the same incident: alerts once more, as down: " + t[0]);
      pl = await pill(page);
      check(pl.cls === "bad" && pl.text === "!", "the envelope turns red");
      await page.evaluate(() => document.getElementById("mailPill").click());   // (the open window is modal)
      check(await page.evaluate(() => /Sending is off/.test(document.querySelector(".mailMenu").textContent) && /Nothing waiting/.test(document.querySelector(".mailMenu").textContent)), "its menu opens with the plain sentence");
      await page.evaluate(() => { const m = document.querySelector(".mailMenu"); if (m) m.remove(); });

      mode.link = okLink();
      await sync(page);
      await until(page, () => window.__toasts.length >= 2);
      t = await toasts(page);
      check(/working again/.test(t[t.length - 1]), "when it clears: one 'working again': " + t[t.length - 1]);
      await until(page, () => { const b = document.getElementById("mailPill"); return b && b.classList.contains("hidden"); });
      check(true, "the envelope goes quiet again");

      // ── 3 · unknown is not green ──
      mode.link = okLink({ atMs: Date.now() - 20 * 60000 });
      await sync(page);
      await until(page, () => { const b = document.getElementById("mailPill"); return b && !b.classList.contains("hidden"); });
      pl = await pill(page);
      check(pl.cls === "warn" && /link monitor has stopped/.test(pl.title), "a judgement 20 minutes old: amber, the monitor has stopped: " + pl.title);
      check(/Monitor stopped/.test((await light(page)).text) && (await light(page)).cls === "warn", "the light says Monitor stopped, not Active");
      mode.link = null;
      await page.evaluate(() => { CustomerMail._state.linkBell = { first: Date.now() - 2 * 60000, link: null, at: Date.now() }; });
      await sync(page);
      check((await light(page)).cls === "ok", "no judgement yet, page just opened: still waiting for the first, not alarmed");
      await page.evaluate(() => { CustomerMail._state.linkBell.first = Date.now() - 13 * 60000; });
      await sync(page);
      await until(page, () => { const b = document.querySelector("dialog .cmLight"); return b && /Monitor not running/.test(b.textContent); });
      check((await light(page)).cls === "warn", "no judgement for 13 minutes: amber, Monitor not running");

      // the check itself cannot be made: amber with the reason, not 'Checking...' for ever
      mode.link = okLink(); mode.healthFail = true;
      await page.evaluate(() => { localStorage.removeItem("cn.mail.health"); CustomerMail._state.health = null; });
      await sync(page);
      await page.evaluate(() => document.dispatchEvent(new Event("visibilitychange")));
      await until(page, () => { const b = document.querySelector("dialog .cmLight"); return b && /Can't check/.test(b.textContent); });
      check((await light(page)).cls === "warn" && /Can't check the email link: Firestore could not be read - retrying/.test(await warn(page)), "the check fails: amber 'Can't check the email link: <why> - retrying'");
      mode.healthFail = false;
      // (the next attempt is on a backoff of a few seconds)
      await until(page, () => { const b = document.querySelector("dialog .cmLight"); return b && /Active/.test(b.textContent); });
      check(true, "the next attempt works: back to Active");

      // sync itself fails: red Offline
      mode.syncFail = true;
      for (let i = 0; i < 3; i++) await sync(page);
      await until(page, () => { const b = document.querySelector("dialog .cmLight"); return b && /Offline/.test(b.textContent); });
      check((await light(page)).cls === "down", "the sorter cannot reach the server: red Offline");
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }

    // ── 4 · the Customer pane tells nothing there from could not look ──
    {
      const { page, context, errors } = await open(false);
      await until(page, () => document.querySelector("dialog [data-cm=hist]") && !document.querySelector("dialog [data-cm=hist]").hidden);
      // a lookup that failed: the reason, a Retry, never "No messages"
      mode.info = Object.assign(okInfo(), { threads: [], total: 0, why: "lookup_failed", reason: "Etsy's daily limit is used up" });
      await page.evaluate(rid => { CustomerMail.openConversation({ receiptId: rid + "1", scope: "order" }); }, RID);
      await until(page, () => /Can't reach/.test(document.querySelector("dialog [data-cm=hist]").textContent));
      let h = await hist(page);
      check(/Can't reach this buyer's messages: Etsy's daily limit is used up - retrying/.test(h) && !/No messages/.test(h) && /Retry/.test(h), "a failed look-up says why, with Retry, and not 'No messages': " + h);
      // it asks again by itself (5 s) and recovers
      mode.info = okInfo();
      await until(page, () => /full history/.test(document.querySelector("dialog [data-cm=hist]").textContent));
      check(true, "it asked again by itself and now shows the real count");

      mode.info = Object.assign(okInfo(), { exact: false, why: "count_failed", reason: "the inbox could not finish counting" });
      await page.evaluate(rid => { CustomerMail.openConversation({ receiptId: rid + "2", scope: "order" }); }, RID);
      await until(page, () => /Can't count all/.test(document.querySelector("dialog [data-cm=hist]").textContent));
      check(/Can't count all of this buyer's messages: the inbox could not finish counting - retrying/.test(await hist(page)), "a half count is not shown as a number: " + (await hist(page)));

      mode.info = null; mode.infoFail = true;
      await page.evaluate(rid => { CustomerMail.openConversation({ receiptId: rid + "3", scope: "order" }); }, RID);
      await until(page, () => /Can't reach/.test(document.querySelector("dialog [data-cm=hist]").textContent));
      check(/Can't reach this buyer's messages: The inbox could not count - retrying/.test(await hist(page)), "the count call itself fails: " + (await hist(page)));
      mode.infoFail = false; mode.info = Object.assign(okInfo(), { threads: [], total: 0, why: "none" });
      await until(page, () => /No messages with this buyer in the inbox yet/.test(document.querySelector("dialog [data-cm=hist]").textContent));
      check(true, "'none' (the inbox looked and there is nothing) is the only thing that says 'No messages'");

      mode.info = Object.assign(okInfo(), { threads: [], total: 0, why: "no_buyer" });
      await page.evaluate(rid => { CustomerMail.openConversation({ receiptId: rid + "4", scope: "order" }); }, RID);
      await until(page, () => /does not know who bought/.test(document.querySelector("dialog [data-cm=hist]").textContent));
      check(true, "an order whose buyer the inbox does not know: " + (await hist(page)));

      // the order call: a failed look-up of the conversation and a failed call
      mode.order = Object.assign(okOrder(), { conversation: null, lookupFailed: "Etsy could not be asked (Etsy's daily limit is used up)" });
      await page.evaluate(rid => { CustomerMail.openConversation({ receiptId: rid + "5", scope: "order" }); }, RID);
      await until(page, () => /look up this buyer/.test(document.querySelector("dialog [data-cm=notice]").textContent));
      let n = await notice(page);
      check(/Can't look up this buyer's conversation: Etsy could not be asked/.test(n) && !/has not written to the shop/.test(n), "the conversation look-up failed: " + n);
      mode.order = okOrder();
      await until(page, () => { const e = document.querySelector("dialog [data-cm=notice]"); return e.hidden || !/look up/.test(e.textContent); });
      check(true, "and it asked again by itself");
      mode.order = Object.assign(okOrder(), { conversation: null, lookupFailed: null });
      await page.evaluate(rid => { CustomerMail.openConversation({ receiptId: rid + "6", scope: "order" }); }, RID);
      await until(page, () => /has not written to the shop/.test(document.querySelector("dialog [data-cm=notice]").textContent));
      check(true, "a buyer who really has not written still says so (a true state)");
      mode.orderFail = true;
      await page.evaluate(rid => { CustomerMail.openConversation({ receiptId: rid + "7", scope: "order" }); }, RID);
      await until(page, () => /Can't reach the inbox/.test(document.querySelector("dialog [data-cm=notice]").textContent));
      check(/Can't reach the inbox: The inbox could not be read - retrying/.test(await notice(page)), "the order call fails: " + (await notice(page)));
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }

    // ── 5 · the engraving card's short version says the same, and its pull button becomes a Retry ──
    {
      const { page, context, errors } = await open(false);
      await page.evaluate(() => { const d = document.querySelector("dialog"); if (d) d.close(); });
      mode.info = Object.assign(okInfo(), { threads: [], total: 0, why: "lookup_failed", reason: "Etsy could not be asked" });
      await page.evaluate(rid => { const n = CustomerMail.lineBox({ key: "k1", row: { key: "k1", order: { receiptId: rid + "8" }, line: { transactionId: "11" } } }); document.body.appendChild(n); }, RID);
      const pull = () => page.evaluate(() => { const b = document.querySelector(".cmLineBox .cmLPull"); return b && !b.hidden ? { text: b.textContent.trim(), disabled: b.disabled, act: b.dataset.lDo } : null; });
      await until(page, () => { const b = document.querySelector(".cmLineBox .cmLPull"); return b && !b.hidden && /Can't reach/.test(b.textContent); });
      let b0 = await pull();
      check(b0 && /Can't reach this buyer's messages/.test(b0.text) && !/No messages/.test(b0.text) && !b0.disabled && b0.act === "recount", "engraving card, look-up failed: " + JSON.stringify(b0));
      mode.info = okInfo();
      await page.evaluate(() => document.querySelector(".cmLineBox .cmLPull").click());
      await until(page, () => /Pull all messages . 3/.test(document.querySelector(".cmLineBox .cmLPull").textContent));
      b0 = await pull();
      check(b0 && !b0.disabled && b0.act === "pull", "Retry reads the real count: " + b0.text);
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await context.close();
    }

    // ── 6 · the sandbox pane ──
    {
      const { page, context, errors } = await open(true);
      mode.info = Object.assign(okInfo(), { threads: [], total: 0, why: "no_buyer" });
      await page.evaluate(rid => { CustomerMail.openConversation({ receiptId: rid + "9", scope: "order" }); }, RID);
      await until(page, () => /Sandbox: no order loaded/.test(document.querySelector("dialog [data-cm=hist]").textContent));
      check(!/No messages/.test(await hist(page)), "sandbox, no order behind it: 'Sandbox: no order loaded', not 'No messages'");
      check(errors.length === 0, "no page errors");
      await context.close();
    }
  } finally { await browser.close(); srv.close(); }
  console.log(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\nmail-link-light OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
