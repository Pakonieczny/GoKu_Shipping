// SANDMAIL (10 Oct 2026): the customer's email works in the Charm Sorter's Sandbox the way it does on the real side, end to end, in headless
// Chromium: the REAL charm-nest-mail.js against the REAL etsyMailOrderLink endpoint (and etsySandbox's order lookup) on the faithful
// in-memory Firestore, the sandbox playing a pulled set of orders whose buyers have real conversations in the inbox. Nothing is sent, no
// Etsy call, no paid AI call (a canned stand-in translates), and the one thing that stays: nothing typed in the sandbox reaches a customer.
//
//   1 · reading: a buyer with threads (real count, Pull all messages, a photo, EN/УКР), a buyer with none ("looked and found none"),
//       a lookup that fails (honest retry text, then it recovers), "Sandbox: no order loaded"
//   2 · writing: the per-order draft (cn.mail.drafts:sandbox, not the real slot), "translate mine", Enter -> Queued (1st) -> Sending… ->
//       Sent, the same words and spinner as the real side, a small sandbox tooltip, never a dialog
//   3 · receiving: "Play a customer reply" is an inline strip (no pop-up); a reply played for later arrives by itself with the toast,
//       the ding, the new mark, the unread count and the envelope; resolve / reopen / language
//   4 · the engraving card's message column does the same through the same pane
//   5 · the link light and its box say plainly what the sandbox does, and the sandbox's own queue never mixes with the real one
//   6 · a reset and a new Start come back clean; the real inbox is byte-for-byte as it was; a real page sees none of it
//   node tests/charm-nest/mail-sandbox-full.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
"use strict";
const path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
let chromium; try { ({ chromium } = require(path.join(pwDir, "playwright-core"))); } catch (_) { console.log("  - no playwright-core: the browser checks were not run"); process.exit(0); }
const { boot, reporter, ORDERS, KEY } = require("../etsy-mail/_sandboxMailRig.cjs");

const T = reporter();
const R = boot();
const real = Date.now;
let skew = 0;
Date.now = () => real() + skew;   // the server's clock (in this process) moves the sandbox's send states; the browser keeps real time
const sleep = ms => new Promise(r => setTimeout(r, ms));
const SB_TIP = "Sandbox: not sent to the customer. The states are played in the sorter only.";

const harness = sandbox => `<!doctype html><html><head><meta charset="utf-8"><title>Charm Sorter</title>
<style>[hidden]{display:none!important}.hidden{display:none!important}dialog{width:520px;height:620px}.cm{display:flex;flex-direction:column}.cmEng{width:520px;height:520px}</style></head>
<body><div class="topbar"><div class="topTools"></div></div><div id="toasts"></div>
<script>
window.S = { settings: { sandbox: ${JSON.stringify(sandbox ? "on" : "off")}, sound: "off", notify: "off" } };
window.__toasts = []; window.__dings = 0; window.__words = [];
window.toast = (m, k) => { window.__toasts.push({ m: String(m), k: k || "" }); };
window.ding = () => { window.__dings++; }; window.notifyPerson = () => {};
window.esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
// what the status word of our newest message says, every 40 ms: the order it changes in is what the person sees
setInterval(() => { const all = document.querySelectorAll(".cmMsg.us .cmSt"); const s = all[all.length - 1]; const w = s ? s.textContent.trim() + (s.querySelector(".cmSpin") ? " (spinner)" : "") : null; if (w && window.__words[window.__words.length - 1] !== w) window.__words.push(w); }, 40);
</script><script src="/charm-nest-mail.js"></script></body></html>`;

(async () => {
  const srv = await R.serve(p => harness(p.get("sandbox") === "on"));
  const origin = "http://127.0.0.1:" + srv.address().port;
  const seen = []; R.serve.seen = seen;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || "/opt/pw-browsers/chromium-1194/chrome-linux/chrome", args: ["--no-sandbox"] });
  const dialogs = [], errors = [];
  try {
    const before = R.realSnap();
    async function open(sandbox, seed) {
      const context = await browser.newContext({ viewport: { width: 900, height: 1000 } });
      await context.addInitScript(([k, extra]) => { try { if (!sessionStorage.getItem("__s")) { localStorage.clear(); localStorage.setItem("cn.mail.station", JSON.stringify(k)); for (const [a, b] of Object.entries(extra || {})) localStorage.setItem(a, b); sessionStorage.setItem("__s", "1"); } } catch (_) {} }, [KEY, seed || {}]);
      const page = await context.newPage(); page.setDefaultTimeout(15000);
      page.on("pageerror", e => { errors.push(e.message); console.error("page error:", e.message); });
      page.on("dialog", d => { dialogs.push(d.type() + ": " + d.message()); d.dismiss(); });
      await page.goto(`${origin}/h.html${sandbox ? "?sandbox=on" : ""}`, { waitUntil: "load" });
      await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
      return { page, context };
    }
    const syncs = () => seen.filter(x => x.op === "sync").length;
    const kick = async page => { const n = syncs(); await page.evaluate(() => document.dispatchEvent(new Event("visibilitychange"))); for (let i = 0; i < 60 && syncs() <= n; i++) await sleep(100); await sleep(450); };
    const text = (page, sel) => page.evaluate(s => { const e = document.querySelector(s); return e ? e.textContent.replace(/\s+/g, " ").trim() : null; }, sel);
    const until = (page, fn, arg) => page.waitForFunction(fn, arg, { timeout: 15000 });
    const openOrder = async (page, rid) => { await page.evaluate(r => CustomerMail.openConversation({ receiptId: r, scope: "order" }), rid); };
    const histDone = (page, re) => until(page, src => { const h = document.querySelector("dialog .cmHist"); return h && !h.hidden && !/Counting|Looking/.test(h.textContent) && (!src || new RegExp(src).test(h.textContent)); }, re ? re.source : "");
    const closeOrder = page => page.evaluate(() => { const d = document.querySelector("dialog"); if (d) d.close(); });
    const mine = page => page.evaluate(() => { const all = document.querySelectorAll("dialog .cmMsg.us"); const m = all[all.length - 1]; if (!m) return null; const s = m.querySelector(".cmSt"); return { word: s ? s.textContent.trim() : "", spin: !!(s && s.querySelector(".cmSpin")), tip: s ? s.title : "", cls: s ? s.className : "" }; });
    const pill = page => page.evaluate(() => { const b = document.getElementById("mailPill"); return b ? { hidden: b.classList.contains("hidden"), text: b.textContent.trim(), title: b.title } : null; });
    const plainTxt = html => String(html || "").replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim();
    const lsKeys = page => page.evaluate(() => Object.fromEntries(Object.keys(localStorage).filter(k => /^cn\.mail\.(drafts|outbox)/.test(k)).map(k => [k, localStorage.getItem(k)])));

    // ═══ 1 · reading the buyer's real conversation, read only ═══
    console.log("1 · The buyer's real conversation (read only)");
    let { page, context } = await open(true);
    await openOrder(page, ORDERS.withThreads);
    await histDone(page, /full history/);
    let h = await text(page, "dialog .cmHist");
    T.check(/full history: 4 messages in 2 conversations/.test(h) && /Pull all messages/.test(h), "a buyer with threads: the real count and the Pull button: " + h);
    T.check(!/Counting|Sandbox: no order/.test(h), "it is not the 'no order loaded' text");
    await page.click("dialog [data-cm-do=pull]");
    await until(page, () => /All 4 messages/.test((document.querySelector("dialog .cmHist") || {}).textContent || ""));
    const all = await page.evaluate(() => ({ txt: document.querySelector("dialog .cmThread").textContent, imgs: [...document.querySelectorAll("dialog .cmThread img")].map(i => i.src) }));
    T.check(/Anna & Tom/.test(all.txt) && /Do you ship to Canada/.test(all.txt) && /handwriting/.test(all.txt), "Pull all messages: both conversations, oldest first, on screen");
    T.check(all.imgs.some(s => /etsystatic/.test(s)), "the buyer's photo is shown: " + all.imgs.length + " image");
    T.check(!/stranger/i.test(all.txt), "another buyer's conversation is never shown");
    await page.click("dialog [data-cm=all][data-l=en]");
    await until(page, () => /\[translated\]/.test(document.querySelector("dialog .cmThread").textContent));
    T.check(R.counts.ai > 0, "EN translates the messages (through the stand-in, the same op as the real side)");
    await page.click("dialog [data-cm=all][data-l=en]");   // off again
    await closeOrder(page);

    await openOrder(page, ORDERS.noThreads);
    await histDone(page, /No messages with this buyer/);
    h = await text(page, "dialog .cmHist");
    T.check(/No messages with this buyer in the inbox yet/.test(h) && !(await page.$("dialog [data-cm-do=pull]")) && !(await page.$("dialog [data-cm-do=recount]")), "a buyer with none: the plain statement, no Pull button, no retry: " + h);
    await closeOrder(page);

    await openOrder(page, ORDERS.unknown);
    await histDone(page, /Sandbox: no order loaded/);
    h = await text(page, "dialog .cmHist");
    T.check(/Sandbox: no order loaded/.test(h), "an order nothing knows: " + h);
    await closeOrder(page);

    R.fake.failOn((op, p) => p === "Charm_Sandbox/stream", new Error("14 UNAVAILABLE: the sandbox store is down"), false);
    await openOrder(page, "4170000003");
    await until(page, () => /Can't reach this buyer's messages/.test((document.querySelector("dialog .cmHist") || {}).textContent || ""));
    h = await text(page, "dialog .cmHist");
    T.check(/Can't reach this buyer's messages: .*retrying/.test(h) && !!(await page.$("dialog [data-cm-do=recount]")), "a failed lookup: honest text with the reason and a Retry button: " + h);
    R.fake.clearFail(); skew += 21000;
    await page.click("dialog [data-cm-do=recount]");
    await until(page, () => /No messages with this buyer in the inbox yet/.test((document.querySelector("dialog .cmHist") || {}).textContent || ""));
    T.check(true, "pressing Retry once the store is back finds the buyer (nothing in the inbox, said plainly)");
    await closeOrder(page);

    // ═══ 2 · writing ═══
    console.log("2 · Writing (the draft, translate mine, the send states)");
    await openOrder(page, ORDERS.withThreads);
    await histDone(page);
    const input = "dialog textarea[data-cm=input]";
    await page.fill(input, "Which font would you like for the back?");
    await sleep(700);   // (drafts are saved after a short pause)
    let ls = await lsKeys(page);
    T.check(/Which font/.test(ls["cn.mail.drafts:sandbox"] || "") && !ls["cn.mail.drafts"], "the draft is kept in the sandbox's own slot (cn.mail.drafts:sandbox), not the real one: " + Object.keys(ls).join(", "));
    const hint = await text(page, "dialog [data-cm=hint]");
    T.check(/Sandbox/i.test(hint) && /stays in the sorter|not sent/i.test(hint), "the small note under the box says it plainly: " + hint);
    await page.click("dialog [data-cm=trIn][data-l=uk]");
    await until(page, () => /\[translated\]/.test(document.querySelector("dialog textarea[data-cm=input]").value));
    T.check(true, "translate mine (УКР) works on the draft: " + (await page.inputValue(input)));
    await page.click("dialog [data-cm-do=undo]");
    await sleep(200);
    T.check((await page.inputValue(input)) === "Which font would you like for the back?", "and Undo puts what I wrote back");
    await page.evaluate(() => { window.__words.length = 0; });
    await page.press(input, "Enter");
    await until(page, () => window.__words.some(w => /^Queued \(1st\)/.test(w)));
    let m = await mine(page);
    T.check(m.word === "Queued (1st)" && m.tip === SB_TIP, "the first state is 'Queued (1st)' with the sandbox reminder as its tooltip: " + m.word + " / " + m.tip);
    await sleep(600);
    ls = await lsKeys(page);
    T.check(!/Which font/.test(ls["cn.mail.drafts:sandbox"] || ""), "the sent draft leaves the sandbox slot");
    let badgeNow = plainTxt(await page.evaluate(rid => CustomerMail.badge(rid), ORDERS.withThreads));
    T.check(badgeNow === "Sending", "the order's mark on the list says 'Sending' while it is on its way (the queue badge): " + badgeNow);
    let p = await pill(page);
    T.check(p && p.hidden, "and the envelope stays quiet: the sandbox's own send is not a problem, the link light is green: " + JSON.stringify(p));
    skew += 2600;
    await kick(page);
    await until(page, () => /^Sending…/.test(((document.querySelectorAll("dialog .cmMsg.us .cmSt"))[0] || {}).textContent || ""));
    m = await mine(page);
    T.check(m.word === "Sending…" && m.spin && m.tip === SB_TIP, "then 'Sending…' with the small spinner: " + m.word + (m.spin ? " (spinner)" : ""));
    skew += 5200;
    await kick(page);
    await until(page, () => /^Sent$/.test(((document.querySelectorAll("dialog .cmMsg.us .cmSt"))[0] || {}).textContent || ""));
    m = await mine(page);
    T.check(m.word === "Sent" && !m.spin && /ok/.test(m.cls) && m.tip === SB_TIP, "and 'Sent' (solid, no spinner, still marked as the sandbox's): " + m.word);
    const order = ws => ws.map(w => w.replace(" (spinner)", "")).filter((w, i, a) => a[i - 1] !== w).join(" -> ");
    const seq = order(await page.evaluate(() => window.__words));
    T.check(/Queued \(1st\) -> Sending… -> Sent$/.test(seq), "the order the person saw (the first Sending… is this browser handing it over): " + seq);
    badgeNow = plainTxt(await page.evaluate(rid => CustomerMail.badge(rid), ORDERS.withThreads));
    T.check(badgeNow === "Asked", "once sent the mark reads 'Asked' (an open question): " + badgeNow);
    const sbTitle = await page.evaluate(() => document.querySelector("dialog .cmMsg.us").title);
    T.check(!!seen.find(x => x.op === "ask" && x.body.sandbox === true && x.status === 200), "the question went to the order-link endpoint as a sandbox one");
    T.check(!seen.some(x => x.op === "sync" && x.body.sandbox === true && x.body.qo === true), "a sandbox page never asks for the real queue (no qo)");

    // a second question waits behind a first and can be taken back while it waits
    await page.fill(input, "Second question: which colour?");
    await page.press(input, "Enter");
    await until(page, () => /Queued/.test(((document.querySelectorAll("dialog .cmMsg.us .cmSt"))[1] || {}).textContent || ""));
    let words = await page.evaluate(() => [...document.querySelectorAll("dialog .cmMsg.us .cmSt")].map(s => s.textContent.trim()));
    T.check(words[0] === "Sent" && /^Queued/.test(words[1]), "a second message follows the first, one at a time: " + words.join(" | "));
    await closeOrder(page);

    // ═══ 3 · receiving ═══
    console.log("3 · Receiving (the customer's side)");
    await openOrder(page, ORDERS.withThreads);
    await histDone(page);
    dialogs.length = 0;
    await page.click("dialog .cmMore summary");
    const menu = await page.evaluate(() => [...document.querySelectorAll("dialog .cmMenu button")].map(b => b.textContent.trim()));
    T.check(menu.some(x => /Play a customer reply/.test(x)), "the menu offers 'Play a customer reply' (sandbox only): " + menu.join(" | "));
    await page.click("dialog [data-cm-do=simulate]");
    await until(page, () => { const s = document.querySelector("dialog [data-cm=sim]"); return s && !s.hidden; });
    T.check(dialogs.length === 0, "it opens a small strip in the pane, not a pop-up dialog");
    await page.fill("dialog [data-cm=simText]", "Gold, please");
    await page.press("dialog [data-cm=simText]", "Escape");
    T.check(await page.evaluate(() => !!document.querySelector("dialog") && document.querySelector("dialog").open && document.querySelector("dialog [data-cm=sim]").hidden), "Esc puts the strip away without closing the window");
    await page.click("dialog .cmMore summary");
    await page.click("dialog [data-cm-do=simulate]");
    await page.fill("dialog [data-cm=simText]", "Yes please, Anna & Tom");
    await page.press("dialog [data-cm=simText]", "Enter");
    await until(page, () => /Yes please, Anna/.test(document.querySelector("dialog .cmThread").textContent));
    T.check(await page.evaluate(() => document.querySelector("dialog [data-cm=sim]").hidden), "Enter plays it now: the customer's words are in the thread and the strip is closed");
    await closeOrder(page);
    await page.evaluate(() => { window.__toasts.length = 0; window.__dings = 0; });

    // played for later: leave the window and watch the marks arrive
    await openOrder(page, ORDERS.withThreads);
    await histDone(page);
    await page.click("dialog .cmMore summary");
    await page.click("dialog [data-cm-do=simulate]");
    await page.fill("dialog [data-cm=simText]", "Gold, please");
    await page.click("dialog [data-cm-do=simlater]");
    await until(page, () => window.__toasts.some(t => /answers in 10 seconds/.test(t.m)));
    T.check(true, "'In 10 s' says so in the small notice: " + (await page.evaluate(() => window.__toasts[window.__toasts.length - 1].m)));
    T.check(await page.evaluate(() => !/Gold, please/.test(document.querySelector("dialog .cmThread").textContent)), "the reply is not there yet");
    await closeOrder(page);
    await page.evaluate(() => { window.__toasts.length = 0; window.__dings = 0; });
    const markBefore = await page.evaluate(rid => CustomerMail.tabNews(rid), ORDERS.withThreads);
    skew += 11000;
    await kick(page);
    await until(page, () => window.__toasts.length > 0);
    const toasts = await page.evaluate(() => window.__toasts.map(t => t.m));
    T.check(toasts.some(t => /Gold, please|replied|reply|Ada|new/i.test(t)) && (await page.evaluate(() => window.__dings)) >= 1, "it arrives by itself, like a real reply: the toast and the ding: " + toasts.join(" | "));
    const tn = await page.evaluate(rid => CustomerMail.tabNews(rid), ORDERS.withThreads);
    T.check(tn && tn.unread === true && !(markBefore && markBefore.unread && false), "the order carries the new mark (Customer tab dot)");
    const badge = await page.evaluate(rid => CustomerMail.badge(rid), ORDERS.withThreads);
    T.check(/Gold, please|new|Reply|reply/i.test(plainTxt(badge)) || /unread|new/i.test(String(badge)), "the list row's badge shows the waiting reply: " + plainTxt(badge));
    p = await pill(page);
    T.check(p && !p.hidden && /Gold|1|reply|waiting/i.test(p.title + p.text), "the envelope shows it: " + JSON.stringify(p));
    await openOrder(page, ORDERS.withThreads);
    await histDone(page);
    await until(page, () => /Gold, please/.test(document.querySelector("dialog .cmThread").textContent));
    await sleep(600);
    const stillUnread = await page.evaluate(rid => (CustomerMail.tabNews(rid) || {}).unread, ORDERS.withThreads);
    T.check(!stillUnread, "reading it clears the mark" + (stillUnread ? " " + JSON.stringify(seen.filter(x => x.op === "read").map(x => [x.status, x.body.engagementId])) : ""));
    // resolve / reopen / language
    await page.click("dialog .cmMore summary");
    await page.click("dialog [data-cm-do=resolve]");
    await until(page, () => /Answered|resolved/i.test(document.querySelector("dialog").textContent));
    T.check(true, "resolve works in the sandbox (the Answered notice and Reopen are the same code)");
    await page.click("dialog [data-cm=notice] [data-cm-do=reopen]");
    await until(page, () => !!document.querySelector("dialog [data-cm-do=resolve]") || !!document.querySelector("dialog .cmMore"));
    await page.click("dialog [data-cm=all][data-l=uk]");
    await sleep(700);
    const doc = Object.values(R.fake.list("EtsyMail_OrderLinks")).map(x => x.data).find(x => x.sandbox === true && x.receiptId === ORDERS.withThreads);
    T.check(doc && doc.status === "open" && doc.sandbox === true && !doc.threadId, "the sandbox engagement is open again, still sandbox:true and linked to no real conversation");

    // ═══ 5 · the link light and the queue badge ═══
    console.log("4 · The link light and its box");
    const light = await page.evaluate(() => { const l = document.querySelector("dialog [data-cm=live]"); return l ? { cls: l.className, title: l.title, txt: l.textContent.trim() } : null; });
    T.check(light && /^cmLive\b/.test(light.cls) && !/\bbad\b|\bwarn\b/.test(light.cls) && /Active|Working|Connected|Ready/i.test(light.txt), "the link light is green and reads 'Active' in the sandbox: " + JSON.stringify(light));
    await page.click("dialog [data-cm=live]");
    await until(page, () => { const b = document.querySelector("dialog [data-cm=hbox]"); return b && !b.hidden && /Sandbox/.test(b.textContent); });
    const box = await text(page, "dialog [data-cm=hbox]");
    T.check(/Sandbox/.test(box) && /stays? in the sorter|stay in the sorter/.test(box) && /real email link/.test(box), "the details box says first what the sandbox does and that the checks are the real link's: " + box.slice(0, 220));
    await page.keyboard.press("Escape");
    await closeOrder(page);
    await context.close();

    // ═══ 4 · the engraving card's message column ═══
    console.log("5 · The engraving card's message column");
    const jobOf = (rid, line, key) => ({ key, row: { key: "row-" + key, order: { receiptId: rid, buyerName: "Buyer" }, line: { transactionId: line, sku: "CHARM" }, spec: { designSku: "BACK-1" } } });
    const putCard = (pg, j) => pg.evaluate(job => { const host = CustomerMail.cardPane(job); document.body.appendChild(host); window.__card = host; }, j);
    // (a) a buyer with threads: the count, Pull all messages with its photo, and the order's open question (one at a time, as on the real side)
    ({ page, context } = await open(true));
    await putCard(page, jobOf(ORDERS.withThreads, 777, "job-1"));
    await until(page, () => { const h = window.__card.querySelector(".cmHist"); return h && !h.hidden && /full history|No messages|Sandbox/.test(h.textContent); });
    h = await text(page, ".cmEng .cmHist");
    T.check(/full history: 4 messages in 2 conversations/.test(h) && /Pull all messages/.test(h), "the card's column shows the buyer's real count and Pull all messages: " + h);
    await page.click(".cmEng [data-cm-do=pull]");
    await until(page, () => /All 4 messages/.test(window.__card.querySelector(".cmHist").textContent));
    T.check(await page.evaluate(() => /Anna & Tom/.test(window.__card.querySelector(".cmThread").textContent) && !!window.__card.querySelector(".cmThread img")), "pulling shows the whole conversation with its photo in the card");
    await page.fill(".cmEng textarea[data-cm=input]", "Is the engraving as the sketch? (card)");
    await sleep(700);
    ls = await lsKeys(page);
    T.check(Object.keys(ls).includes("cn.mail.drafts:sandbox") && !ls["cn.mail.drafts"], "the card's draft is the sandbox's too");
    await page.evaluate(() => { window.__words.length = 0; });
    await page.press(".cmEng textarea[data-cm=input]", "Enter");
    await until(page, () => window.__words.some(w => /^Queued \(1st\)/.test(w)));
    skew += 2600; await kick(page);
    await until(page, () => /^Sending…/.test(window.__words[window.__words.length - 1] || ""));
    skew += 5200; await kick(page);
    await until(page, () => window.__words[window.__words.length - 1] === "Sent");
    const cardSeq = order(await page.evaluate(() => window.__words));
    T.check(/Queued \(1st\) -> Sending… -> Sent$/.test(cardSeq), "a message written in the card walks the same states: " + cardSeq);
    await context.close();
    // (b) a buyer with none, and an engraving question of its own: a sandbox engagement on that line
    ({ page, context } = await open(true));
    await putCard(page, jobOf(ORDERS.noThreads, 888, "job-2"));
    await until(page, () => { const h = window.__card.querySelector(".cmHist"); return h && !h.hidden && /No messages|full history|Sandbox/.test(h.textContent); });
    h = await text(page, ".cmEng .cmHist");
    T.check(/No messages with this buyer in the inbox yet/.test(h) && !(await page.$(".cmEng [data-cm-do=pull]")), "a buyer with none: the same plain statement in the card, no Pull button: " + h);
    await page.fill(".cmEng textarea[data-cm=input]", "Is the back engraving as you wanted?");
    await page.press(".cmEng textarea[data-cm=input]", "Enter");
    await until(page, () => /Queued/.test(((window.__card.querySelectorAll(".cmMsg.us .cmSt"))[0] || {}).textContent || ""));
    const eng = R.fake.list("EtsyMail_OrderLinks").map(x => x.data).find(x => x.sandbox === true && x.scope === "engraving");
    T.check(eng && /^olsb_/.test(eng.id) && !eng.threadId && String(eng.lineId) === "888" && eng.receiptId === ORDERS.noThreads, "an engraving question is a sandbox engagement on that line, linked to no real conversation");
    skew += 2600; await kick(page); skew += 5200; await kick(page);
    await until(page, () => /^Sent$/.test(((window.__card.querySelectorAll(".cmMsg.us .cmSt"))[0] || {}).textContent || ""));
    await page.click(".cmEng .cmMore summary");
    await page.click(".cmEng [data-cm-do=simulate]");
    await page.fill(".cmEng [data-cm=simText]", "Yes, like the sketch");
    await page.press(".cmEng [data-cm=simText]", "Enter");
    await until(page, () => /Yes, like the sketch/.test(window.__card.querySelector(".cmThread").textContent));
    T.check(true, "and the customer's answer plays in the card's column, in the pane (no pop-up)");
    T.check(dialogs.length === 0, "no native dialog appeared anywhere in the whole run: " + dialogs.join(" | "));
    await context.close();

    // ═══ 6 · a real page sees none of it; a reset and a new Start come back clean ═══
    console.log("6 · The worlds never mix; a reset and a new Start");
    const changed = R.diff(before, R.realSnap());
    T.check(changed.length === 0, "the real inbox is byte-for-byte as it was after all of the above (the conversations, the shared reply box, the real queue, the jobs, the real questions)" + (changed.length ? ": " + changed.join(", ") : ""));
    ({ page, context } = await open(false));
    await sleep(900);
    const rb = await page.evaluate(rid => CustomerMail.tabNews(rid), ORDERS.withThreads);
    T.check(rb && rb.unread === true, "a real page: the real order's own new-reply mark is its own");
    p = await pill(page);
    T.check(p && /1 customer conversation has a new reply/.test(p.title), "and its envelope counts only the real one (not the sandbox's replies): " + (p && p.title));
    await openOrder(page, ORDERS.withThreads);
    await page.waitForFunction(() => document.querySelector("dialog .cmThread"));
    await sleep(900);
    const realThread = await page.evaluate(() => document.querySelector("dialog .cmThread").textContent);
    T.check(!/Gold, please|Yes please, Anna|Is the engraving|Which font/.test(realThread) && /Your order ships Friday/.test(realThread), "a real page shows its own question and none of the sandbox's");
    T.check(!(await lsKeys(page))["cn.mail.drafts:sandbox"], "and has no sandbox draft slot");
    await context.close();
    const afterReal = R.realSnap();

    // the Reset: the bridge clears the sandbox's engagements (server) and the page clears its sandbox drafts and outbox, then reloads
    ({ page, context } = await open(true, { "cn.mail.drafts:sandbox": JSON.stringify({ [ORDERS.withThreads + ":o"]: { t: "left over", at: real() } }), "cn.mail.outbox:sandbox": JSON.stringify([]) }));
    await openOrder(page, ORDERS.withThreads);
    await histDone(page);
    const leftovers = R.fake.list("EtsyMail_OrderLinks").filter(x => /^olsb_/.test(x.id) && x.data.sandbox === true);
    T.check(leftovers.length >= 2, "(the old run has " + leftovers.length + " sandbox questions when the reset comes)");
    await page.fill(input, "queued when the reset comes");
    await page.press(input, "Enter");
    await until(page, () => /Queued/.test(((document.querySelectorAll("dialog .cmMsg.us .cmSt"))[0] || {}).textContent || "") || document.querySelectorAll("dialog .cmMsg.us").length > 0);
    await page.evaluate(() => CustomerMail.wipeSandbox());
    for (const x of R.fake.list("EtsyMail_OrderLinks")) if (/^olsb_/.test(x.id) && x.data.sandbox === true) await R.fake.db.doc("EtsyMail_OrderLinks/" + x.id).delete();
    R.fake.poke("Charm_Sandbox/current", { path: "charmnest/sandbox/orders-pull/test.json", source: "etsy-pull", startId: "start-2", count: 3, open: 3, at: real(), pulledAt: real() });   // a new Start
    ls = await lsKeys(page);
    T.check(!/left over|queued when the reset/.test(JSON.stringify(ls)), "the page's sandbox drafts and outbox are cleared by the reset");
    await page.reload({ waitUntil: "load" });
    await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
    await openOrder(page, ORDERS.withThreads);
    await histDone(page);
    h = await text(page, "dialog .cmHist");
    T.check(/full history: 4 messages/.test(h), "after the new Start the buyer's real history is there again: " + h);
    const thread = await text(page, "dialog .cmThread");
    T.check(!(await page.$("dialog .cmMsg.us")) && !/Gold, please|Yes please/.test(thread || ""), "and the order's thread is clean: no old question, no old reply");
    T.check((await page.inputValue(input)) === "", "the message box is empty");
    p = await pill(page);
    T.check(!p || p.hidden, "the envelope is empty (nothing of the old run): " + JSON.stringify(p));
    await page.fill(input, "A new run, a new question");
    await page.press(input, "Enter");
    await until(page, () => /Queued \(1st\)/.test(((document.querySelectorAll("dialog .cmMsg.us .cmSt"))[0] || {}).textContent || ""));
    T.check(true, "a first question of the new run starts at 'Queued (1st)', not behind the old run's");
    await context.close();

    T.check(R.diff(afterReal, R.realSnap()).length === 0, "the reset and the new run changed nothing of the real inbox");
    T.check(R.counts.etsy === 0 && R.counts.network === 0, "no Etsy call and no network call were made");
    T.check(errors.length === 0, "no page errors: " + errors.join("; "));
    T.check(!R.warn.some(w => /ERROR/.test(w) && !/the sandbox store is down/.test(w)), "no server error was logged");
  } finally {
    Date.now = real; await browser.close().catch(() => {}); srv.close(); R.done();
  }
  T.finish("mail-sandbox-full");
})().catch(e => { Date.now = real; console.error(e); process.exit(1); });
