// The sorter's half of the send queue (charm-nest-mail.js), in headless Chromium against a fake order-link endpoint
// (nothing reaches Etsy, the inbox or a customer):
//   · a message that is waiting its turn says where it stands ("Queued (3rd)"), says "Sending…" while it goes, and shows a small
//     labelled spinner for a wait (the short pause between messages, the helper, a retry with its time) - from the sync answer,
//     with no polling of its own and a revision number that makes the idle answer one small object;
//   · a message that may have gone out ("attention") is never presented as sent or as failed: it says "Check Etsy", offers
//     "It was sent", "Send again" (after a plain question) and "Copy the message", and counts on the badge and the envelope;
//   · a message that did not go says why in plain words and offers one-click retry and copy-and-send-by-hand;
//   · a sandbox page never asks for the real queue.
//   node tests/charm-nest/mail-send-queue-ui.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
"use strict";
const http = require("http"), fs = require("fs"), path = require("path");
const root = path.join(__dirname, "../..");
const pwDir = process.env.PW_DIR || path.join(root, "node_modules");
let chromium; try { ({ chromium } = require(path.join(pwDir, "playwright-core"))); } catch (_) { console.log("  - no playwright-core: the browser checks were not run"); process.exit(0); }

const RID = "4174973193", EID = "ol_4174973193_order_x1", ITEM = "m1";
const T0 = Date.now();
const st = { v: 5, n: 1, queue: null, msg: null, retryReply: null, ops: [], syncs: [] };
function setMsg(over) { st.msg = Object.assign({ itemId: ITEM, id: "out_" + ITEM, side: "us", who: "Charm Sorter · Bo", atMs: T0 - 60000, text: "Which font would you like for the back?", images: [], cards: [], status: "queued", error: null, note: null, unverified: false, delivered: false, sentAtMs: 0, waitReason: null, copied: false, manualSent: false, msgId: null, qid: "q_abc", errorCode: null }, over || {}); st.v++; }
const summary = () => {
  const m = st.msg, s = m.status;
  return {
    id: EID, receiptId: RID, lineId: null, scope: "order", lineLabel: "", orderNumber: RID, status: "open", sandbox: false, link: "thread", linkedBy: "order", threadId: "etsy_conv_1",
    customer: { name: "Ada Byrne" }, title: "", lang: null, createdBy: "Bo", createdAtMs: T0 - 60000, startedAtMs: T0 - 60000, updatedAtMs: T0, v: st.v,
    resolvedAtMs: 0, resolvedBy: "", unread: 0, inboundCount: 0, lastInboundAtMs: 0, lastInboundPreview: "", lastInboundBy: "", lastOutboundAtMs: 0, lastShopReplyAtMs: 0,
    pending: s === "queued" || s === "new" || s === "waiting" ? 1 : 0, failed: s === "failed" ? 1 : 0, attention: s === "attention" ? 1 : 0, manual: 0,
    lastOut: { id: m.itemId, status: s, atMs: m.atMs, sentAtMs: m.sentAtMs }
  };
};
const full = () => Object.assign(summary(), { messages: [st.msg], earlier: [] });
const okLink = () => ({ level: "ok", short: "", problem: "", atMs: Date.now(), downSinceMs: 0, incidentId: "" });
const qitem = o => Object.assign({ id: "q_abc", t: "etsy_conv_1", st: "queued", at: T0 - 60000, src: "sorter", by: "", tp: "", att: 0, n: 0, open: true, ol: ITEM, oe: EID }, o || {});

function harness(sandbox) {
  return `<!doctype html><html><head><meta charset="utf-8"><title>Charm Sorter</title>
<style>[hidden]{display:none!important}.hidden{display:none!important}dialog{width:520px;height:620px}.cm{display:flex;flex-direction:column}</style></head>
<body><div class="topbar"><div class="topTools"></div></div><div id="toasts"></div>
<script>
window.S = { settings: { sandbox: ${JSON.stringify(sandbox ? "on" : "off")}, sound: "off", notify: "off" } };
window.__toasts = [];
window.toast = (m, k) => { window.__toasts.push({ m: String(m), k: k || "" }); };
window.ding = () => {}; window.notifyPerson = () => {};
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
          const send = (code, obj) => { res.writeHead(code, { "Content-Type": "application/json" }); res.end(JSON.stringify(obj)); };
          if (b.op !== "sync" && b.op !== "health") st.ops.push(b);
          switch (b.op) {
            case "sync": {
              st.syncs.push({ n: b.n, since: b.since, qn: b.qn, qo: b.qo, sandbox: b.sandbox });
              let queue = null;
              if (st.queue && !b.sandbox) queue = b.qn === st.queue.n ? { n: st.queue.n, unchanged: true, now: Date.now() } : Object.assign({ now: Date.now(), gapMs: 12000, recent: [] }, st.queue);
              const changes = b.since >= st.v && !b.full ? [] : [summary()];
              return send(200, { n: st.v, v: st.v, full: !!b.full, changes, link: okLink(), now: Date.now(), queue });
            }
            case "health": return send(200, { at: Date.now(), level: "ok", short: "", problem: "", checks: [] });
            case "order": return send(200, { receiptId: RID, sandbox: false, conversation: { threadId: "etsy_conv_1", customer: { name: "Ada Byrne" } }, engagements: [summary()], active: full(), lookupFailed: null });
            case "thread": return send(200, full());
            case "history_info": return send(200, { receiptId: RID, threads: [], total: 0, exact: true, why: "ok", reason: "" });
            case "retry": if (st.retryReply) return send(st.retryReply.status, st.retryReply.body); setMsg({ status: "queued" }); return send(200, full());
            case "sent": setMsg({ status: "sent", manualSent: true, sentAtMs: Date.now() }); return send(200, full());
            case "cancel": setMsg({ status: "cancelled" }); return send(200, full());
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
      st.ops.length = 0; st.syncs.length = 0; st.retryReply = null;
      const context = await browser.newContext({ viewport: { width: 900, height: 900 } });
      await context.grantPermissions(["clipboard-read", "clipboard-write"], { origin }).catch(() => {});
      await context.addInitScript(() => { try { if (!sessionStorage.getItem("__seeded")) { localStorage.clear(); localStorage.setItem("cn.mail.station", JSON.stringify("k-test-station-key-0123456789abcdef")); sessionStorage.setItem("__seeded", "1"); } } catch (_) {} });
      const page = await context.newPage(), errors = [];
      page.setDefaultTimeout(15000);
      page.on("pageerror", e => { errors.push(e.message); console.error("page error:", e.message); });
      await page.goto(`${origin}/h.html${sandbox ? "?sandbox=on" : ""}`, { waitUntil: "load" });
      await page.waitForFunction(() => window.CustomerMail && CustomerMail.connected());
      await page.evaluate(rid => CustomerMail.openConversation({ receiptId: rid, scope: "order" }), RID);
      return { page, context, errors };
    }
    const mine = p => p.evaluate(() => { const m = document.querySelector("dialog .cmMsg.us"); if (!m) return null; const s = m.querySelector(".cmSt"); return { cls: m.className, word: s ? s.textContent.trim() : "", spin: !!(s && s.querySelector(".cmSpin")), tip: s ? s.title : "", acts: [...m.querySelectorAll("[data-cm-do]")].map(b => b.textContent.trim()), err: (m.querySelector(".cmErr") || {}).textContent || "", fine: (m.querySelector(".cmFine") || {}).textContent || "" }; });
    const pill = p => p.evaluate(() => { const b = document.getElementById("mailPill"); return b ? { hidden: b.classList.contains("hidden"), text: b.textContent.trim(), title: b.title } : null; });
    const kick = async p => { const n = st.syncs.length; await p.evaluate(() => document.dispatchEvent(new Event("visibilitychange"))); for (let i = 0; i < 50 && st.syncs.length <= n; i++) await new Promise(r => setTimeout(r, 100)); await new Promise(r => setTimeout(r, 400)); };
    const until = (p, fn, arg) => p.waitForFunction(fn, arg, { timeout: 12000 });

    // ── 1 · queued: where it stands, from the sync answer ──
    {
      setMsg({ status: "queued" });
      st.queue = { n: 1, items: [qitem({ st: "queued", pos: 3 })] };
      const { page, context, errors } = await open(false);
      await until(page, () => /Queued \(3rd\)/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      let m = await mine(page);
      check(m && m.word === "Queued (3rd)" && !m.spin, "a message third in line says \"Queued (3rd)\" (no spinner for a plain place in line): " + (m && m.word));
      check(/one at a time/.test(m.tip), "its tooltip explains the one-at-a-time rule: " + m.tip);
      check(st.syncs.some(s => s.qo === true), "the sync asks for the queue view while a message is on its way (qo)");

      // the place moves up and the short pause shows as a wait with a spinner
      st.queue = { n: 2, items: [qitem({ st: "queued", pos: 1, wait: "pace" })] }; st.v++;
      await kick(page);
      await until(page, () => /Queued \(1st\)/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      m = await mine(page);
      check(m.word === "Queued (1st)" && m.spin && /short pause/.test(m.tip), "first in line but pausing between messages: \"Queued (1st)\" with the spinner and the reason: " + m.tip);
      // an unchanged revision is answered with the one small object, and the page keeps what it knows
      const before = st.syncs.length;
      await kick(page);
      const last = st.syncs[st.syncs.length - 1];
      check(st.syncs.length > before && last.qn === 2, "the next sync carries the queue revision it has (qn 2), so an idle queue costs one small answer");
      m = await mine(page);
      check(m.word === "Queued (1st)", "an unchanged answer leaves the words as they were");

      st.queue = { n: 3, items: [qitem({ st: "sending" })] }; st.v++;
      await kick(page);
      await until(page, () => /Sending/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      m = await mine(page);
      check(m.word === "Sending…" && m.spin, "while the helper sends it: \"Sending…\" with the spinner");

      st.queue = { n: 4, items: [qitem({ st: "queued", wait: "retry", nb: Date.now() + 125000, n: 1, pos: 1 })] }; st.v++;
      await kick(page);
      await until(page, () => /Trying again/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      m = await mine(page);
      check(/^Trying again in 2 min$/.test(m.word) && m.spin, "a delayed retry says when: \"" + m.word + "\"");

      st.queue = { n: 5, items: [qitem({ st: "queued", wait: "helper", nb: Date.now() + 60000, code: "NO_HELPER", pos: 1 })] }; st.v++;
      await kick(page);
      await until(page, () => /Etsy helper/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      m = await mine(page);
      check(m.word === "Waiting for the Etsy helper" && m.spin && /nothing is lost/.test(m.tip), "no helper: \"Waiting for the Etsy helper\", spinner, and \"nothing is lost\"");
      check(errors.length === 0, "no page errors in the waiting states: " + errors.join("; "));
      await context.close();
    }

    // ── 2 · needs attention: it may have gone; never "sent" and never plain "failed" ──
    {
      setMsg({ status: "queued" });
      st.queue = { n: 1, items: [qitem({ st: "sending" })] };
      const { page, context, errors } = await open(false);
      await until(page, () => /Sending/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      setMsg({ status: "attention", error: "The helper clicked Send but then stopped answering. It may have gone: check the conversation on Etsy.", errorCode: "STRANDED_POST_CLICK" });
      st.queue = { n: 2, items: [], recent: [qitem({ st: "needs_attention", open: true })] };
      await kick(page);
      await until(page, () => /Check Etsy/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      let m = await mine(page);
      check(m.word === "Check Etsy: it may have gone" && !m.spin, "the word is \"Check Etsy: it may have gone\": " + m.word);
      check(/hold/.test(m.cls) && !/pend/.test(m.cls), "it is drawn as a hold (dashed), not as sent and not as failed");
      check(["It was sent", "Send again", "Copy the message", "Discard"].every(a => m.acts.includes(a)), "the four choices: " + m.acts.join(" | "));
      check(/stopped answering/.test(m.err) && /only send again if the customer did not get it/i.test(m.fine), "the reason is in plain words, with the warning about sending twice");
      const pl = await pill(page);
      check(pl && !pl.hidden && /may or may not have gone/.test(pl.title), "the envelope counts it: " + (pl && pl.title));
      const badge = await page.evaluate(() => CustomerMail.badge("4174973193"));
      check(/Check Etsy/.test(badge), "the order's badge on its list row says Check Etsy: " + String(badge).replace(/<[^>]+>/g, "").trim());

      // Send again: asks first; "Cancel" on the question sends nothing
      let asked = 0;
      page.once("dialog", d => { asked++; d.dismiss(); });
      await page.click("dialog [data-cm-do=retry]");
      await new Promise(r => setTimeout(r, 500));
      check(asked === 1 && !st.ops.some(o => o.op === "retry"), "\"Send again\" asks \"only if the customer did NOT get it\" first, and a No sends nothing");
      page.once("dialog", d => { asked++; d.accept(); });
      st.retryReply = null;
      await page.click("dialog [data-cm-do=retry]");
      await new Promise(r => setTimeout(r, 600));
      const rt = st.ops.filter(o => o.op === "retry");
      check(rt.length === 1 && rt[0].confirmMaybeSent === true && rt[0].itemId === ITEM, "a Yes sends one retry that says the person confirmed (confirmMaybeSent)");

      // "It was sent": one click, no question
      setMsg({ status: "attention", error: "It may have gone.", errorCode: "STRANDED_POST_CLICK" });
      st.queue = { n: 3, items: [], recent: [qitem({ st: "needs_attention", open: true })] };
      await kick(page);
      await until(page, () => /Check Etsy/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      await page.click("dialog [data-cm-do=sentByHand]");
      await until(page, () => /Sent by hand/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      check(st.ops.some(o => o.op === "sent" && o.itemId === ITEM), "\"It was sent\" tells the inbox, and the message reads \"Sent by hand\"");
      check(errors.length === 0, "no page errors in the attention flow: " + errors.join("; "));
      await context.close();
    }

    // ── 3 · did not go: why, one-click retry, copy-and-send-by-hand; a server that wants a second confirmation gets it ──
    {
      setMsg({ status: "failed", error: "The Etsy helper did not answer after 4 tries. Nothing was sent.", errorCode: "HELPER_OFFLINE" });
      st.queue = null;
      const { page, context } = await open(false);
      await until(page, () => /Not sent/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      let m = await mine(page);
      check(m.word === "Not sent" && /did not answer after 4 tries/.test(m.err), "a dead letter says so with the plain reason: " + m.err);
      check(["Try again", "Copy the message", "Discard"].every(a => m.acts.includes(a)), "it offers Try again, Copy the message, Discard: " + m.acts.join(" | "));
      await page.click("dialog [data-cm-do=copytext]");
      await new Promise(r => setTimeout(r, 300));
      const clip = await page.evaluate(() => navigator.clipboard.readText().catch(() => null));
      check(clip === null || clip === "Which font would you like for the back?", "Copy the message puts the text on the clipboard" + (clip === null ? " (clipboard read not available here)" : ""));
      st.retryReply = { status: 409, body: { error: "This message may already have gone out. Check the conversation on Etsy, then press Send again to confirm.", code: "MAYBE_SENT" } };
      let asked = 0;
      page.on("dialog", d => { asked++; d.accept(); });
      await page.click("dialog [data-cm-do=retry]");
      await new Promise(r => setTimeout(r, 800));
      const rt = st.ops.filter(o => o.op === "retry");
      check(asked === 1 && rt.length === 2 && rt[1].confirmMaybeSent === true, "a server that says \"may already have gone\" is asked about once, then the confirmed retry goes (" + rt.length + " retries, " + asked + " question)");
      await context.close();
    }

    // ── 4 · sent and delivered ──
    {
      setMsg({ status: "sent", sentAtMs: Date.now() });
      st.queue = null;
      const { page, context } = await open(false);
      await until(page, () => /Sent/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      let m = await mine(page);
      check(m.word === "Sent", "sent: \"Sent\"");
      setMsg({ status: "sent", sentAtMs: Date.now(), delivered: true, msgId: "etsy_msg_9" });
      await kick(page);
      await until(page, () => /Delivered/.test((document.querySelector("dialog .cmMsg.us .cmSt") || {}).textContent || ""));
      m = await mine(page);
      check(m.word === "Delivered", "once the inbox has read it back from Etsy: \"Delivered\"");
      check(!st.syncs.some(s => s.qo === true), "nothing on its way: the sync never asks for the queue view");
      await context.close();
    }

    // ── 5 · a sandbox page never asks for, nor receives, the real queue ──
    {
      setMsg({ status: "queued" });
      st.queue = { n: 9, items: [qitem({ st: "queued", pos: 2 })] };
      const { page, context } = await open(true);
      await new Promise(r => setTimeout(r, 1500));
      check(st.syncs.length > 0 && st.syncs.every(s => s.qo !== true), "a sandbox page never asks for the queue view (qo is never set)");
      await context.close();
    }
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.log("\n" + fails.length + " FAILED:\n - " + fails.join("\n - ")); process.exit(1); }
  console.log("\nmail-send-queue-ui: all checks passed");
})().catch(e => { console.error(e); process.exit(1); });
