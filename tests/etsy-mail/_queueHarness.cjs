"use strict";
/* The offline rig for the send dispatcher (MAILQUEUE, 10 Oct 2026).
 *
 *   - the real server code (etsyMailDraftSend, _etsyMailSendQueue, _etsyMailOrderLink, the cleanup cron) on an in-memory
 *     Firestore that refuses nested arrays and `undefined`, enforces read-before-write in transactions and retries
 *     optimistic conflicts (tests/etsy-mail/_fakeFirestore.cjs);
 *   - a virtual clock (Date.now), so pauses, leases and back-offs take no real time;
 *   - a simulated Chrome extension that speaks the exact peek / claim / heartbeat / mark_clicked / complete / fail
 *     protocol of 0.9.52 against the real handler, and can die, fail, hang or double up on request.
 * Nothing here reaches Etsy, a customer, an AI or the network (fetch and node-fetch refuse).
 */
const path = require("path");
const { createFake, install } = require("./_fakeFirestore.cjs");
const fnDir = path.join(__dirname, "../../netlify/functions");

function boot(opts = {}) {
  process.env.ETSYMAIL_EXTENSION_SECRET = "test-secret-not-real";
  const fake = createFake({ seed: opts.seed || 7, jitterMs: opts.jitterMs || 0 });
  const inst = install(fake);
  const realNow = Date.now;
  let t = opts.start || Date.UTC(2026, 9, 10, 12, 0, 0);
  Date.now = () => t;
  const clock = { now: () => t, advance: ms => { t += ms; return t; }, set: v => { t = v; } };
  const warn = [], realWarn = console.warn, realError = console.error, realLog = console.log;
  console.warn = (...a) => warn.push(a.map(String).join(" "));
  console.error = (...a) => warn.push("ERROR " + a.map(String).join(" "));
  if (!opts.verbose) console.log = () => {};
  // every boot gets a fresh copy of the server modules (they keep their Firestore handle and caches at module level)
  for (const k of Object.keys(require.cache)) if (k.startsWith(fnDir)) delete require.cache[k];
  const stubs = { learning: 0, ai: 0 };
  const stub = (rel, exports) => { const p = require.resolve(path.join(fnDir, rel)); require.cache[p] = { id: p, filename: p, loaded: true, exports }; };
  stub("_etsyMailLearning.js", { recordOutcome: async () => { stubs.learning++; } });
  stub("_etsyMailAnthropic.js", { fetchClassificationContext: async () => { stubs.ai++; throw new Error("no AI in this test"); } });
  const Q = require(path.join(fnDir, "_etsyMailSendQueue.js"));
  const DS = require(path.join(fnDir, "etsyMailDraftSend.js"));
  const OL = require(path.join(fnDir, "_etsyMailOrderLink.js"));
  const Cron = require(path.join(fnDir, "etsyMailDraftSendCleanupCron.js"));
  Q._setRandom(() => 0.5);          // no jitter unless a test asks for it: gap = 12 s, back-off exactly 30 s, 2 min, 8 min
  const secretHeader = { "x-etsymail-secret": process.env.ETSYMAIL_EXTENSION_SECRET };

  const parse = r => { let b = {}; try { b = JSON.parse(r.body || "{}"); } catch (e) { b = { raw: r.body }; } return { status: r.statusCode, body: b }; };
  const h = {
    fake, Q, DS, OL, Cron, clock, warn, stubs,
    call: async (op, body) => parse(await DS.handler({ httpMethod: "POST", headers: secretHeader, body: JSON.stringify(Object.assign({ op }, body || {})) })),
    get: async (op, qs) => parse(await DS.handler({ httpMethod: "GET", headers: secretHeader, queryStringParameters: Object.assign({ op }, qs || {}) })),
    thread: (n, extra) => { fake.poke("EtsyMail_Threads/etsy_conv_" + n, Object.assign({ threadId: "etsy_conv_" + n, status: "pending_human_review", etsyConversationUrl: "https://www.etsy.com/your/conversations/" + n }, extra || {})); return "etsy_conv_" + n; },
    slot: threadId => fake.peek("EtsyMail_Drafts/draft_" + threadId),
    item: sendId => fake.peek("EtsyMail_SendQueue/" + sendId),
    items: () => fake.list("EtsyMail_SendQueue").map(x => x.data).sort((a, b) => a.createdAtMs - b.createdAtMs),
    lease: () => fake.peek("EtsyMail_SendQueueMeta/lease") || {},
    rev: () => (fake.peek("EtsyMail_SendQueueMeta/rev") || {}).n || 0,
    /** a message from the inbox (a person pressed Send) */
    send: async (threadId, text, o) => {
      o = o || {};
      const n = threadId.replace("etsy_conv_", "");
      return h.call("enqueue", Object.assign({
        threadId, etsyConversationUrl: "https://www.etsy.com/your/conversations/" + n, text, employeeName: o.by || "Anna",
        sendOrigin: o.origin || "manual", attachments: o.attachments || [], allowSendWithoutPendingTracking: true
      }, o.key ? { idempotencyKey: o.key } : {}, o.aiMeta ? { aiMeta: o.aiMeta } : {}, o.extra || {}));
    },
    sorter: (threadId, text, engId, itemId) => {
      const n = threadId.replace("etsy_conv_", "");
      return h.call("enqueue", { threadId, etsyConversationUrl: "https://www.etsy.com/your/conversations/" + n, text, employeeName: "Charm Sorter · Bo", sendOrigin: "manual", attachments: [], allowSendWithoutPendingTracking: true, orderLink: { e: engId, i: itemId }, idempotencyKey: "sorter:" + engId + ":" + itemId });
    },
    done() { Date.now = realNow; console.warn = realWarn; console.error = realError; console.log = realLog; inst.restore(); }
  };
  h.Ext = makeExt(h);
  return h;
}

/** One Chrome extension (one browser). Speaks the 0.9.52 protocol. */
function makeExt(h) {
  let nSession = 0;
  return class Ext {
    constructor(name, o) {
      this.name = name; this.o = Object.assign({ sendMs: 9000, hbMs: 5000 }, o || {});
      this.dead = false; this.inFlight = new Set(); this.log = []; this.calls = { claim: 0, markClicked: 0, complete: 0, fail: 0, clicks: 0 };
      this.session = () => "ext_" + name + "_" + (++nSession);
    }
    note(s) { this.log.push(s); }
    /** background.js pollQueuedSends: list status==queued, open up to two tabs */
    async poll(maxTabs = 2, scriptFor) {
      if (this.dead) return [];
      const queued = h.fake.list("EtsyMail_Drafts").map(x => x.data).filter(d => d.status === "queued" && !this.inFlight.has(d.draftId));
      const out = [];
      for (const d of queued.slice(0, maxTabs)) out.push(await this.tab(d.threadId, scriptFor ? scriptFor(d) : undefined));
      return out;
    }
    /** content-sender peekOnce for the tab on this conversation */
    async tab(threadId, script) {
      if (this.dead) return { dead: true };
      const draftId = "draft_" + threadId;
      this.inFlight.add(draftId);
      try {
        const p = await h.call("peek", { threadId });
        if (!p.body.queued) return { peek: "none" };
        return await this.handle(p.body.draft, script);
      } finally { this.inFlight.delete(draftId); }
    }
    /** handleQueuedDraft: claim, heartbeat while working, mark_clicked, click, complete / fail */
    async handle(draft, script) {
      const o = Object.assign({}, this.o, script || {});
      const draftId = draft.id || draft.draftId;
      const session = this.session();
      this.calls.claim++;
      const c = await h.call("claim", { draftId, sessionId: session, workerId: this.name });
      if (c.status !== 200) return { claim: c.status, code: c.body.errorCode, retryAfterMs: c.body.retryAfterMs };
      const claimed = c.body.draft;
      const res = { claim: 200, session, text: claimed.text, attempts: c.body.attempts };
      const hb = async () => { const r = await h.call("heartbeat", { draftId, sessionId: session }); res.lastHb = r.status; return r; };
      // typing the message takes sendMs of (virtual) time, with a heartbeat every hbMs
      const steps = Math.max(1, Math.round((o.sendMs || 0) / o.hbMs));
      for (let i = 0; i < steps; i++) {
        h.clock.advance(Math.round((o.sendMs || 0) / steps));
        if (o.dieAt === "typing" && i === 0) { this.dead = true; res.died = "typing"; return res; }
        if (i < steps - 1 || o.hbLast !== false) await hb();
        if (o.onStep) await o.onStep(i, res);
      }
      if (o.failBeforeClick) {
        this.calls.fail++;
        const f = await h.call("fail", { draftId, sessionId: session, error: o.failBeforeClick.error || "boom", errorCode: o.failBeforeClick.code || "DOM_TEXTAREA_MISS", retry: o.failBeforeClick.retry !== false });
        res.fail = f.status; return res;
      }
      if (o.dieAt === "before_mark") { this.dead = true; res.died = "before_mark"; return res; }
      this.calls.markClicked++;
      const m = await h.call("mark_clicked", { draftId, sessionId: session });
      res.markClicked = m.status; res.markCode = m.body.errorCode;
      if (m.status === 200 && o.dieAt === "after_mark") { this.dead = true; res.died = "after_mark"; return res; }
      if (m.status !== 200) {                                   // the extension never clicks if mark_clicked fails
        this.calls.fail++;
        const f = await h.call("fail", { draftId, sessionId: session, error: "mark_clicked failed: " + (m.body.error || m.status), errorCode: "MARK_CLICKED_FAILED", retry: true });
        res.fail = f.status; return res;
      }
      this.calls.clicks++; res.clicked = true;                  // <- Etsy's Send button, the one irreversible step
      h.sentToEtsy = h.sentToEtsy || [];
      h.sentToEtsy.push({ threadId: draft.threadId, text: claimed.text, session, at: h.clock.now() });
      if (o.dieAt === "after_click") { this.dead = true; res.died = "after_click"; return res; }
      h.clock.advance(o.confirmMs == null ? 1500 : o.confirmMs);
      if (o.failAfterClick) {
        this.calls.fail++;
        const f = await h.call("fail", { draftId, sessionId: session, error: o.failAfterClick, errorCode: "CONFIRM_FAILED", retry: true });
        res.fail = f.status; return res;
      }
      this.calls.complete++;
      const body = { draftId, sessionId: session };
      if (o.unverified) body.unverified = true;
      if (o.partial) { body.partial = true; body.imagesSent = 0; body.imagesTotal = 1; }
      const done = await h.call("complete", body);
      res.complete = done.status; res.status = done.body.status;
      return res;
    }
  };
}

/** Run a test file's checks and print them the way the repo's other offline tests do. */
function reporter() {
  const fails = [];
  const out = { check(ok, what) { if (!ok) fails.push(what); process.stdout.write((ok ? "  ok   " : "  FAIL ") + what + "\n"); }, fails };
  out.finish = name => {
    if (fails.length) { process.stdout.write("\n" + fails.length + " FAILED in " + name + "\n"); process.exit(1); }
    process.stdout.write("\n" + name + ": all checks passed\n");
  };
  return out;
}

module.exports = { boot, reporter };
