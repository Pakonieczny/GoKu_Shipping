/*  netlify/functions/etsyMailLinkWatchdog.js
 *
 *  Scheduled every 5 minutes (netlify.toml). Watches the customer-mail line from outside the reaper, so it can notice the
 *  reaper (or any other part) has stopped:
 *    1. If the reaper's catch-up pass (order_links) has not reported for 12 minutes, runs it here (reconcile is
 *       idempotent: it re-reads anything a scrape missed and settles sends whose hooks were lost).
 *    2. Judges the whole line with _etsyMailLinkHealth.js (the same judgement the Charm Sorter's light uses) and writes it to
 *         EtsyMail_Config/linkHealth       the inbox's banner reads this (one small document, once a minute per computer)
 *         EtsyMail_OrderLinkMeta/bell.link  a four-field summary every sorter receives in the sync answer it already makes
 *       with the time the trouble began (downSinceMs / incidentId) so the apps can alert once per incident.
 *    3. Once a night, the consistency check (_etsyMailLinkConsistency.js) when it exists in this build.
 *  A reader treats a judgement older than 12 minutes as "the monitor stopped" (amber), so this function failing is itself shown.
 *
 *  Cost: about 12 reads and 2 writes a run (about 0.12 M operations a month). No Etsy call, no AI, nothing sent to a customer.
 *  Scheduled functions answer 403 to a direct call from outside; a person can run it with the extension secret.
 */
"use strict";

const admin = require("./firebaseAdmin");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");
const LinkHealth = require("./_etsyMailLinkHealth");

const db = admin.firestore();
const MIN = 60 * 1000;
const CATCH_UP_AFTER_MS = 12 * MIN;
/** Netlify's own scheduler only (its header). The body markers other cron functions accept are not honoured here: a person runs this with the secret. */
function scheduledByNetlify(event) {
  const h = {};
  for (const [k, v] of Object.entries((event && event.headers) || {})) h[String(k).toLowerCase()] = v;
  return h["x-nf-event-source"] === "scheduled" || h["x-netlify-event"] === "schedule";
}
const json = (statusCode, body) => ({ statusCode, headers: CORS, body: JSON.stringify(body) });

async function run({ now = Date.now(), reconcile } = {}) {
  const out = { catchUp: null, level: null, consistency: null };
  let ev = await LinkHealth.gather(db, { self: true });

  // 1. the catch-up pass: the reaper normally runs it every five minutes
  const stamp = ev.bell && ev.bell.reconcileAtMs || 0;
  if (ev.readOk.bell !== false && now - stamp > CATCH_UP_AFTER_MS) {
    try {
      const r = await (reconcile || require("./_etsyMailOrderLink").reconcile)({ budgetMs: 12000 });
      out.catchUp = { ran: true, error: r && r.error ? String(r.error).slice(0, 120) : null };
      if (!(r && r.error)) ev.bell = Object.assign({}, ev.bell, { reconcileAtMs: Date.now(), reconcileErrorAtMs: 0, reconcileError: "" });
    } catch (e) { out.catchUp = { ran: true, error: String(e && e.message).slice(0, 120) }; console.warn("linkWatchdog catch-up:", e && e.message); }
  }

  // 3. the nightly consistency check, when this build has it
  try {
    let Consistency = null;
    try { Consistency = require("./_etsyMailLinkConsistency"); } catch (e) { if (!/Cannot find module/.test(String(e && e.message))) throw e; }
    if (Consistency) {
      out.consistency = await Consistency.maybeRun(db, { now });
      if (out.consistency && out.consistency.ran) ev.consistency = out.consistency.doc;
    }
  } catch (e) { out.consistency = { error: String(e && e.message).slice(0, 120) }; console.warn("linkWatchdog consistency:", e && e.message); }

  // 2. judge and publish
  const res = LinkHealth.evaluate(ev, now);
  let prev = null;
  try { const s = await db.collection("EtsyMail_Config").doc("linkHealth").get(); prev = s.exists ? s.data() : null; }
  catch (e) { console.warn("linkWatchdog previous judgement:", e && e.message); }
  const doc = LinkHealth.nextDoc(prev, res, now);
  // whether the inbox's shared secret is configured on the server: booleans only, never the value
  doc.auth = { secretSet: !!process.env.ETSYMAIL_EXTENSION_SECRET, context: String(process.env.CONTEXT || "").slice(0, 20) };
  await db.collection("EtsyMail_Config").doc("linkHealth").set(doc);
  await db.collection("EtsyMail_OrderLinkMeta").doc("bell").set({ link: LinkHealth.summaryOf(doc) }, { merge: true });
  out.level = doc.level; out.short = doc.short;
  return out;
}

exports.handler = async (event) => {
  if (event && event.httpMethod === "OPTIONS") return { statusCode: 200, headers: CORS, body: "ok" };
  if (event && event.httpMethod && !scheduledByNetlify(event)) {
    const auth = requireExtensionAuth(event);
    if (!auth.ok) return auth.response;
  }
  try { return json(200, await run()); }
  catch (e) { console.error("etsyMailLinkWatchdog:", e); return json(500, { error: e && e.message || String(e) }); }
};
exports.run = run;
