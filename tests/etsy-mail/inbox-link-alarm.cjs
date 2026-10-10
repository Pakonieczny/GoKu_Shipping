// The inbox's half of the customer-mail health (etsy-mail-1.html), in headless Chromium against a fake backend (nothing
// reaches Etsy, Firestore or a customer):
//   · the link alarm in the top bar, the line at the top of the list and the mark in the tab title follow
//     EtsyMail_Config/linkHealth: amber/red when a stage is down, amber when the monitor stopped, the document is missing or
//     the read fails twice; nothing when all is well; one toast per incident after 5 minutes, one when it clears;
//   · a list or a conversation that could not be loaded says why and asks again; it never says "Nothing waiting" or
//     "No messages yet" over an error.
//   NODE_PATH=$(npm root -g) PW_DIR=$(npm root -g)/playwright/node_modules CHROMIUM=... node tests/etsy-mail/inbox-link-alarm.cjs
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
let chromium; try { ({ chromium } = require(path.join(pwDir, "playwright-core"))); } catch (_) { console.log("  - no playwright-core: the browser checks were not run"); process.exit(0); }

const NOW = Date.now(), TID = "t_link1";
const THREAD = {
  id: TID, customerName: "Sue Tester", status: "etsy_scraped", unread: false,
  lastInboundAt: { _ts: true, ms: NOW - 3600e3 }, awaitingReplySince: { _ts: true, ms: NOW - 3600e3 },
  updatedAt: { _ts: true, ms: NOW - 3600e3 }, etsyConversationUrl: "https://example.invalid/c/1"
};
const MESSAGES = [0, 1, 2].map(i => ({
  id: "m" + i, direction: i % 2 ? "outbound" : "inbound", senderName: i % 2 ? "CustomBrites" : "Sue", text: "Hello number " + i,
  timestamp: { _ts: true, ms: NOW - (4 - i) * 3600e3 }, createdAt: { _ts: true, ms: NOW - (4 - i) * 3600e3 }
}));
const mode = { link: null, linkFail: false, doneFail: false, msgFail: false, partial: false };
const okDoc = (over = {}) => Object.assign({ id: "linkHealth", atMs: Date.now(), level: "ok", short: "", problem: "", checks: [], downSinceMs: 0, incidentId: "", worstSinceMs: 0 }, over);
const reset = () => Object.assign(mode, { link: okDoc(), linkFail: false, doneFail: false, msgFail: false, partial: false });

function fake(method, url) {
  const u = new URL(url, "http://x"), name = u.pathname.split("/").pop(), q = Object.fromEntries(u.searchParams);
  if (name === "firestoreProxy") {
    if (q.op === "get" && q.coll === "EtsyMail_Config" && q.id === "linkHealth") {
      if (mode.linkFail) return { status: 500, body: { error: "Firestore could not be read" } };
      return mode.link ? { body: { success: true, exists: true, doc: mode.link } } : { body: { success: true, exists: false } };
    }
    if (q.op === "list" && q.coll === "EtsyMail_Threads") {
      if (/^archivedAt/.test(q.orderBy || "")) return mode.doneFail ? { status: 500, body: { error: "Firestore index is building" } } : { body: { docs: [] } };
      if (mode.partial && q.where === "starred,==,true") return { status: 500, body: { error: "Firestore could not run the starred query" } };
      return { body: { docs: [THREAD] } };
    }
    if (q.op === "listSub") return mode.msgFail ? { status: 500, body: { error: "Firestore could not be read" } } : { body: { docs: MESSAGES } };
    if (q.op === "get") return { body: { exists: false } };
    return { body: { ok: true } };
  }
  if (name === "etsyMailAuth") return { body: { ok: true, username: "paul", displayName: "Paul", role: "owner" } };
  return { body: { ok: true } };
}
const server = http.createServer((req, res) => {
  const u = decodeURIComponent(req.url.split("?")[0]);
  if (u.startsWith("/.netlify/functions/")) {
    req.on("data", () => {}); req.on("end", () => {
      const out = fake(req.method, req.url);
      res.writeHead(out.status || 200, { "Content-Type": "application/json" }); res.end(JSON.stringify(out.body));
    });
    return;
  }
  const f = path.join(root, u);
  if (!f.startsWith(root) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { "Content-Type": /\.js$/.test(f) ? "text/javascript" : "text/html" });
  fs.createReadStream(f).pipe(res);
}).listen(0);

(async () => {
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const fails = [];
  const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };
  const origin = "http://127.0.0.1:" + server.address().port;
  try {
    async function open(query) {
      reset();
      const ctx = await browser.newContext({ viewport: query ? { width: 400, height: 800 } : { width: 1400, height: 900 } });
      await ctx.route(u => !/^http:\/\/127\.0\.0\.1[:/]/.test(u.href), r => r.abort());
      await ctx.addInitScript(() => {
        try {
          if (!sessionStorage.getItem("__seeded")) {
            localStorage.setItem("etsymail_session", "tok");
            localStorage.setItem("etsymail_session_profile", JSON.stringify({ username: "paul", displayName: "Paul", role: "owner" }));
            sessionStorage.setItem("__seeded", "1");
          }
        } catch (_) {}
      });
      const page = await ctx.newPage(), errors = [];
      page.setDefaultTimeout(15000);
      page.on("pageerror", e => errors.push(String(e)));
      await page.goto(origin + "/etsy-mail-1.html" + (query || ""));
      return { page, ctx, errors };
    }
    const refresh = async page => {
      await page.evaluate(() => { try { localStorage.removeItem("etsymail_shared_linkHealth"); } catch (_) {} document.dispatchEvent(new Event("visibilitychange")); });
      await page.waitForTimeout(700);
    };
    const chip = page => page.evaluate(() => { const e = document.getElementById("emLinkAlarm"); return e ? { hidden: e.hidden, warn: e.classList.contains("warn"), text: e.textContent.trim(), title: e.title } : null; });
    const toastText = page => page.evaluate(() => { const t = document.getElementById("emToast"); return t && t.classList.contains("show") ? t.textContent : ""; });
    const until = (page, fn, arg) => page.waitForFunction(fn, arg, { timeout: 25000 });

    // ── desktop ──
    {
      const { page, ctx, errors } = await open("");
      await page.waitForSelector(`[data-id="${TID}"]`);
      const title0 = await page.title();
      await page.waitForTimeout(5600);   // the first read of the link status
      let c = await chip(page);
      check(c && c.hidden, "all well: no alarm in the top bar");
      check((await page.title()) === title0 && !/⚠/.test(await page.title()), "and the tab title is unmarked");

      mode.link = okDoc({ level: "warn", short: "Etsy helper idle", problem: "The Etsy helper has not checked in for 40 minutes.", downSinceMs: Date.now() - 2 * 60000, incidentId: "i1", checks: [{ label: "Etsy helper", level: "warn", text: "No check-in for 40 min" }] });
      await refresh(page);
      c = await chip(page);
      check(c && !c.hidden && c.warn && /Email link: Etsy helper idle/.test(c.text), "monitor says amber: the top bar shows it: " + (c && c.text));
      check(c && /40 minutes/.test(c.title), "its tooltip has the plain sentence: " + (c && c.title));
      check(/^⚠ /.test(await page.title()), "the tab title carries a mark");
      check(await page.evaluate(() => document.getElementById("emTopbarToggleBtn").classList.contains("em-alarm-link")), "the top-bar toggle has the dot (for a hidden bar)");
      check(await page.evaluate(() => /Email link: Etsy helper idle/.test(document.getElementById("emListItems").textContent)), "the list has the sentence at the top (the phone sees the list)");
      check((await toastText(page)) === "", "2 minutes of trouble: no toast yet");

      mode.link = okDoc({ level: "warn", short: "Etsy helper idle", problem: "The Etsy helper has not checked in for 40 minutes.", downSinceMs: Date.now() - 6 * 60000, incidentId: "i1" });
      await refresh(page);
      check(/needs attention: The Etsy helper has not checked in for 40 minutes/.test(await toastText(page)), "after 5 minutes: one toast with the sentence: " + (await toastText(page)));
      await page.evaluate(() => document.getElementById("emToast").classList.remove("show"));
      await refresh(page);
      check((await toastText(page)) === "", "the same incident does not toast again");

      mode.link = okDoc({ level: "down", short: "Sending is off", problem: "Sending to customers is switched off.", downSinceMs: Date.now() - 8 * 60000, incidentId: "i1" });
      await refresh(page);
      c = await chip(page);
      check(c && !c.warn && /Sending is off/.test(c.text) && /is down: Sending to customers is switched off/.test(await toastText(page)), "worse in the same incident: red, and one more toast");

      mode.link = okDoc();
      await refresh(page);
      c = await chip(page);
      check(c.hidden && !/⚠/.test(await page.title()) && /working again/.test(await toastText(page)), "when it clears: the alarm and the mark go, one 'working again'");
      check(await page.evaluate(() => !/Email link:/.test(document.getElementById("emListItems").textContent)), "and the line leaves the list");

      mode.link = okDoc({ atMs: Date.now() - 20 * 60000 });
      await refresh(page);
      c = await chip(page);
      check(!c.hidden && c.warn && /monitor stopped/.test(c.text), "a judgement 20 minutes old: amber, the monitor stopped: " + c.text);

      mode.link = null;
      await refresh(page);
      c = await chip(page);
      check(!c.hidden && c.warn && /monitor not running/.test(c.text), "no document at all: amber, not running: " + c.text);

      mode.link = okDoc(); await refresh(page);
      mode.linkFail = true;
      await refresh(page);
      check((await chip(page)).hidden, "one failed read keeps what was shown");
      await refresh(page);
      c = await chip(page);
      check(!c.hidden && c.warn && /Can't check the email link: Firestore could not be read - retrying/.test(c.title), "a second failed read is itself the news: " + c.title);
      mode.linkFail = false; await refresh(page);
      check((await chip(page)).hidden, "and it clears when the read works again");

      // ── a folder that could not be loaded is not an empty folder ──
      mode.doneFail = true;
      await page.click('.em-rail-item[data-filter="x_done"]');
      await until(page, () => /Can't reach the inbox/.test(document.getElementById("emListItems").textContent));
      let t = await page.evaluate(() => document.getElementById("emListItems").textContent);
      check(/Can't reach the inbox: Firestore index is building - retrying/.test(t) && !/Nothing in Done yet/.test(t), "failed folder: the reason, never 'Nothing in Done yet': " + t.trim().slice(0, 120));
      check(await page.evaluate(() => !!document.querySelector("[data-smart-retry]")), "with a Retry");
      mode.doneFail = false;
      await until(page, () => /Nothing in/.test(document.getElementById("emListItems").textContent) && !/Can't reach the inbox/.test(document.getElementById("emListItems").textContent));
      check(true, "it asked again by itself (15 s) and now the folder is truly empty");

      // ── a conversation that could not be read ──
      mode.msgFail = true;
      await page.click('.em-rail-item[data-filter="x_all"]');
      await page.waitForSelector(`[data-id="${TID}"]`);
      await page.click(`[data-id="${TID}"]`);
      await until(page, () => /Can't reach the inbox/.test(document.getElementById("emThreadBox").textContent));
      t = await page.evaluate(() => document.getElementById("emThreadBox").textContent);
      check(/Can't reach the inbox: Firestore could not be read - retrying/.test(t) && !/No messages yet/.test(t), "unreadable conversation: the reason, not 'No messages yet': " + t.trim().slice(0, 100));
      mode.msgFail = false;
      await until(page, () => document.querySelectorAll("#emThreadBox [data-mid]").length >= 3);
      check(true, "the poll asked again and the messages are there");
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await ctx.close();
    }

    // ── one of the four queries behind "Needs attention" fails: the list is partial, and says so ──
    {
      const { page, ctx, errors } = await open("");
      mode.partial = true;
      await page.reload();
      await page.waitForSelector(`[data-id="${TID}"]`);
      await until(page, () => /part of this list could not be read/.test(document.getElementById("emListItems").textContent));
      check(true, "a partial load of Needs attention says so: " + (await page.evaluate(() => document.getElementById("emListItems").textContent)).trim().slice(0, 140));
      mode.partial = false;
      await page.click("[data-smart-retry]");
      await until(page, () => !/part of this list/.test(document.getElementById("emListItems").textContent));
      check(true, "Retry reads it all and the line goes");
      check(errors.length === 0, "no page errors (" + errors.join("; ") + ")");
      await ctx.close();
    }

    // ── the phone ──
    {
      const { page, ctx, errors } = await open("?force=mobile");
      await page.waitForFunction(() => window.IS_MOBILE === true);
      await page.waitForSelector("#mList .m-thread, #mList [data-id]", { timeout: 15000 }).catch(() => {});
      mode.link = okDoc({ level: "down", short: "Etsy helper offline", problem: "The Etsy helper has not checked in for 2 hours.", downSinceMs: Date.now() - 20 * 60000, incidentId: "i9" });
      await refresh(page);
      await until(page, () => /Email link: Etsy helper offline/.test(document.getElementById("mList").textContent));
      check(true, "phone: the list shows the sentence at the top");
      check(errors.length === 0, "phone: no page errors (" + errors.join("; ") + ")");
      await ctx.close();
    }
  } finally { await browser.close(); server.close(); }
  console.log(fails.length ? `\nFAILED: ${fails.length}\n- ` + fails.join("\n- ") : "\ninbox-link-alarm OK");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
