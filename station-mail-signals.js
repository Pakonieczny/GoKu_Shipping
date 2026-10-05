/*  station-mail-signals.js — what an inbox operator does with customer replies, as activity events (Paul, 5 Oct 2026: "their
 *  contact success and failure rates"). Loaded by etsy-mail-1.html after station-activity.js. Every event goes through
 *  StationActivity.log, so the person is the signed-in operator's NAME, the station is "inbox", and nothing here makes a
 *  request of its own except one small status read of a reply that is still on its way (the page hands in the reader).
 *  No Etsy call, no AI call, never a customer name, address or message text: fixed words and numbers only.
 *
 *  The kinds (the `detail` of the event; plans/employee-hr/api.md lists them for the aggregator):
 *    note   "reply drafted"                    the operator asked the AI for a draft (also " · follow-up", " · revised")
 *    note   "reply sent" [· tokens]            the operator's reply is in Etsy's queue. Tokens, in this order, each optional:
 *                                                " · design note"        the reply also went to the design chat
 *                                                " · ai draft"           it started as an AI draft and was sent as it was
 *                                                " · ai draft edited"    it started as an AI draft and the operator changed it
 *                                                " · first reply <N>m"   first reply of the conversation, N whole minutes after
 *                                                                        the customer's first message (0..99999)
 *                                                " · waited <N>m"        a later reply: minutes the customer's last message waited
 *    note   "reply delivered"                  the Etsy helper confirmed it (" · images not sent" when only the words went out)
 *    note   "reply unconfirmed"                probably sent, Etsy did not confirm it
 *    error  "reply failed" [· CODE]            the Etsy helper could not send it (CODE is the helper's own short code)
 *    error  "reply not sent"                   the queue refused it (written by the page, as before)
 *
 *  A reply that is still on its way is kept in this browser (localStorage, at most 30, 40 minutes), so a reload or another
 *  open conversation never loses its outcome; it is settled by whatever status read the page makes anyway (seen()), and by
 *  one slow read every 30 s of its own for the ones nobody else is watching. Each outcome is recorded once, under the person
 *  who sent it (never under somebody who signed in later). Nothing here throws or waits. */
(function () {
  "use strict";
  if (window.MailSignals) return;
  const KEY = "etsymail_signals_pending_v1", EXPIRE_MS = 40 * 60000, POLL_MS = 30000, MAX_PENDING = 30, MAX_MIN = 99999;

  const lsGet = k => { try { return localStorage.getItem(k) || ""; } catch (_) { return ""; } };
  const lsSet = (k, v) => { try { localStorage.setItem(k, v); } catch (_) {} };
  const digits = v => String(v == null ? "" : v).replace(/\D/g, "");
  const norm = s => String(s == null ? "" : s).replace(/\s+/g, " ").trim().toLowerCase();
  const hash = s => { let h = 0; const t = norm(s); for (let i = 0; i < t.length; i++) h = (h * 31 + t.charCodeAt(i)) | 0; return t.length + ":" + h; };
  function act(action, o) { try { return !!(window.StationActivity && window.StationActivity.log(action, o || {})); } catch (_) { return false; } }
  function who() { try { const w = window.StationActivity && window.StationActivity.who(); return w && w.person ? String(w.person) : ""; } catch (_) { return ""; } }
  const minutes = ms => { const m = Math.round(Number(ms) / 60000); return Number.isFinite(m) ? Math.max(0, Math.min(MAX_MIN, m)) : -1; };

  const baselines = new Map();        // thread id → hash of the AI text put into the reply box
  let pending = [];                   // replies on their way
  let timer = 0, reader = null, polling = false, loaded = false;

  function load() {
    if (loaded) return; loaded = true;
    try { const a = JSON.parse(lsGet(KEY) || "[]"); pending = (Array.isArray(a) ? a : []).filter(e => e && typeof e === "object" && e.threadId && e.key).slice(-MAX_PENDING); } catch (_) { pending = []; }
  }
  function save() { try { lsSet(KEY, JSON.stringify(pending.slice(-MAX_PENDING))); } catch (_) {} }

  /** The AI's words were put into a conversation's reply box: they are what "edited before sending" is measured against. */
  function baseline(threadId, text) {
    try { if (threadId && String(text || "").trim()) baselines.set(String(threadId), hash(text)); } catch (_) {}
  }
  /** The operator asked the AI for a draft and it came back (mode: initial | follow_up | revise). */
  function drafted(threadId, mode, orderId) {
    try { return act("note", { orderId: digits(orderId), detail: "reply drafted" + (mode === "follow_up" ? " · follow-up" : mode === "revise" ? " · revised" : "") }); } catch (_) { return false; }
  }

  /** The operator's reply is in Etsy's queue. o: { threadId, orderId, text (as sent), englishText (what was typed), designNote,
      sinceMs (when the customer's latest message started waiting, 0 unknown), firstReply (true | false | null unknown),
      draftId, queuedAt }. Records "reply sent" once, and keeps the reply on the list until its outcome is known. */
  function sent(o) {
    try {
      o = o || {};
      const tid = String(o.threadId || ""), bits = ["reply sent"];
      if (o.designNote) bits.push("design note");
      const base = baselines.get(tid);
      if (base) bits.push(hash(o.englishText != null ? o.englishText : o.text) === base ? "ai draft" : "ai draft edited");
      baselines.delete(tid);               // the next reply starts fresh
      const since = Number(o.sinceMs) || 0, m = since > 0 ? minutes(Date.now() - since) : -1;
      if (m >= 0) bits.push((o.firstReply === true ? "first reply " : "waited ") + m + "m");
      const ok = act("note", { orderId: digits(o.orderId), detail: bits.join(" · ") });
      if (ok && o.draftId) track({ threadId: tid, draftId: String(o.draftId), key: hash(o.text), order: digits(o.orderId), who: who(), at: Date.now() });
      return ok;
    } catch (_) { return false; }
  }

  function track(e) {
    load();
    pending = pending.filter(x => x.draftId !== e.draftId || x.key !== e.key).concat(e).slice(-MAX_PENDING);
    save(); arm(POLL_MS);
  }

  /** What a draft slot says about one pending reply: delivered | delivered-text | unconfirmed | failed | wait | other. */
  function judge(e, d) {
    if (!d || typeof d !== "object") return "other";
    if (hash(d.text) !== e.key) return "other";                 // the slot holds somebody else's words now
    const st = String(d.status || "");
    if (st === "sent") return "delivered";
    if (st === "sent_text_only") return "delivered-text";
    if (st === "sent_unverified") return "unconfirmed";
    if (st === "failed") return String(d.sendErrorCode || "") === "STRANDED_POST_CLICK" ? "unconfirmed" : "failed";
    if (st === "queued" || st === "sending" || st === "claimed" || st === "sending_in_progress") return "wait";
    return "other";
  }
  function record(e, verdict, d) {
    if (verdict === "delivered") return act("note", { orderId: e.order, detail: "reply delivered" });
    if (verdict === "delivered-text") return act("note", { orderId: e.order, detail: "reply delivered · images not sent" });
    if (verdict === "unconfirmed") return act("note", { orderId: e.order, detail: "reply unconfirmed" });
    const code = String((d && d.sendErrorCode) || "");
    return act("error", { orderId: e.order, detail: "reply failed" + (/^[A-Z][A-Z0-9_]{2,40}$/.test(code) ? " · " + code : "") });
  }
  function settle(e, verdict, d) {
    // another person is signed in here now: the outcome is not theirs, so it is dropped rather than put under their name
    if (e.who && who() && who() !== e.who) { pending = pending.filter(x => x !== e); save(); return false; }
    if (!who()) return false;                                   // nobody signed in at this moment: kept for the next look
    pending = pending.filter(x => x !== e); save();
    return record(e, verdict, d);
  }

  /** A status read the page made anyway (draft is the slot's document, or null when it is gone). Settles what it can. */
  function seen(threadId, d) {
    try {
      load();
      if (!pending.length || !threadId) return;
      const now = Date.now();
      for (const e of pending.slice()) {
        if (e.threadId !== String(threadId)) continue;
        if (now - e.at > EXPIRE_MS) { pending = pending.filter(x => x !== e); save(); continue; }
        const v = judge(e, d);
        if (v === "wait") continue;
        if (v === "other") { if (d && now - e.at > 2 * 60000) { pending = pending.filter(x => x !== e); save(); } continue; }
        settle(e, v, d);
      }
    } catch (_) {}
  }

  /* the slow look at the replies nobody else is watching (after a reload, on the phone, past the page's own 3 minutes) */
  function arm(ms) { try { if (!timer && pending.length) timer = setTimeout(tick, ms == null ? POLL_MS : ms); } catch (_) {} }
  async function tick() {
    timer = 0;
    try {
      load();
      const now = Date.now();
      pending = pending.filter(e => now - e.at <= EXPIRE_MS); save();
      if (!pending.length || polling || typeof reader !== "function" || !who()) { arm(); return; }
      polling = true;
      try {
        for (const e of pending.slice(0, 5)) {
          let d = null, got = false;
          try { d = await reader(e.threadId, e.draftId); got = true; } catch (_) {}
          if (!got) continue;
          const v = judge(e, d);
          if (v === "wait") continue;
          if (v === "other") { pending = pending.filter(x => x !== e); save(); continue; }
          settle(e, v, d);
        }
      } finally { polling = false; }
    } catch (_) { polling = false; }
    arm();
  }
  /** The page's reader: (threadId, draftId) → Promise of the draft slot's document, null when it is gone. Replies still on
      their way from before a reload are looked at soon after. */
  function resume(read) {
    try { if (typeof read === "function") reader = read; load(); arm(5000); } catch (_) {}
  }

  window.MailSignals = { baseline, drafted, sent, seen, resume, pending: () => { load(); return pending.length; }, _judge: judge };
})();
