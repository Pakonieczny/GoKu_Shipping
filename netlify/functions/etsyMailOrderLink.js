/*  netlify/functions/etsyMailOrderLink.js
 *
 *  The Charm Sorter's line to customers through the inbox. The work is in _etsyMailOrderLink.js.
 *
 *  Sorter requests carry X-Mail-Station: a key the sorter collected once a signed-in inbox user
 *  approved it (op pair_start, then the inbox's #pair= link, then op pair_claim). The inbox's
 *  two pairing ops carry the inbox's own secret and session instead.
 *
 *  POST { op, ... }
 *    pair_start { label }                        → { pairId, code, expiresAtMs, inboxUrl }
 *    pair_claim { pairId }                       → { pending } | { expired } | { denied } | { stationKey, operator }
 *    pair_info  { code }            (inbox)      → { found, status, label, createdAtMs }
 *    pair_answer { code, approve }  (inbox)      → { ok, status }
 *    whoami, disconnect, sync, order, thread, ask, retry, cancel, copied, sent, read,
 *    resolve, reopen, lang, link_url, simulate, translate, health,
 *    history_info { receiptId, engagementId }    → { threads: [{ threadId, count, lastAtMs }], total }
 *    history { receiptId, threadId, after }      → { messages, next }  (the buyer's whole history, a page at a time)
 *    simulate { engagementId, text, delayMs }    (sandbox only) the customer's side: a reply now, or after delayMs (up to two minutes,
 *                                                  settled by whoever asks next, so it survives a reload and arrives through sync)
 *    In the sandbox, ask / retry / cancel / sync / order / thread play a simulated send (Queued, Sending, Sent) on the sandbox's own
 *    queue (the bell's sbFlight list): never EtsyMail_SendQueue*, the shared reply box, the Chrome extension, Etsy or a customer.
 *    test_info, test_start, test_cancel          → { ready, customer, pending: { code, expiresAtMs } }  (the test account;
 *                                                  a question with receiptId "test" goes only to it)
 */
"use strict";

const link = require("./_etsyMailOrderLink");
const { requireExtensionAuth, CORS } = require("./_etsyMailAuth");
const { requireSession } = require("./_etsyMailRoles");

const HEADERS = Object.assign({}, CORS, {
  "Access-Control-Allow-Headers": "Content-Type,X-Mail-Station,X-EtsyMail-Secret,X-EtsyMail-Session",
  "Content-Type": "application/json",
  "Cache-Control": "no-store"
});
const json = (statusCode, body) => ({ statusCode, headers: HEADERS, body: JSON.stringify(body) });

/* An engagement belongs to one world, named by the id it was given: olsb_… is the sandbox's (and says sandbox:true), ol_… is
   production's. These ops act on an engagement by id alone, so a request that says which world it is in and names an id from the
   other one is refused before anything is read or written: a sandbox page can never retry, cancel, mark, resolve or link a real
   customer's message, nor a real page a rehearsal's. A request that does not say (an older page) is not judged. */
const BY_ID = new Set(["retry", "cancel", "copied", "sent", "read", "resolve", "reopen", "lang", "link_url", "simulate"]);
function otherWorld(body) {
  if (typeof body.sandbox !== "boolean") return false;
  const id = String(body.engagementId || "");
  const theirs = /^olsb_/.test(id) ? true : /^ol_/.test(id) ? false : null;
  return theirs !== null && theirs !== body.sandbox;
}
exports.otherWorld = otherWorld;

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { error: "POST only" });
  let body;
  try { body = JSON.parse(event.body || "{}") || {}; } catch (_) { return json(400, { error: "Invalid JSON" }); }
  const op = String(body.op || "");
  try {
    // ── pairing: the sorter asks, a signed-in inbox answers ──
    if (op === "pair_start") return json(200, await link.pairStart(body));
    if (op === "pair_claim") return json(200, await link.pairClaim(body));
    if (op === "pair_info" || op === "pair_answer") {
      const auth = requireExtensionAuth(event);
      if (!auth.ok) return auth.response;
      const session = await requireSession(event);
      if (!session.ok) return json(401, { error: "Sign in to the inbox first", code: session.reason || "NO_SESSION" });
      if (op === "pair_info") return json(200, await link.pairInfo(body.code));
      return json(200, await link.pairAnswer(body.code, session, body.approve === true));
    }

    // ── everything else comes from a connected sorter ──
    const st = await link.requireStation(event);
    if (!st.ok) return json(st.status || 401, { error: st.error, code: st.code });
    const station = st.station;
    if (BY_ID.has(op) && otherWorld(body)) return json(404, { error: "That conversation is gone" });
    switch (op) {
      case "whoami":     return json(200, { operator: { username: station.username, name: station.name } });
      case "disconnect": return json(200, await link.disconnect(station));
      case "sync":       return json(200, await link.sync(body));
      case "order":      return json(200, await link.order(body));
      case "thread":     return json(200, await link.thread(body));
      case "ask":        return json(200, await link.ask(station, body));
      case "retry":      return json(200, await link.retry(body, station));
      case "cancel":     return json(200, await link.cancel(body));
      case "copied":     return json(200, await link.markCopied(body));
      case "sent":       return json(200, await link.markSent(body, station));
      case "read":       return json(200, await link.read(body));
      case "resolve":    return json(200, await link.setStatus(station, body, "resolved"));
      case "reopen":     return json(200, await link.setStatus(station, body, "open"));
      case "lang":       return json(200, await link.setLang(body));
      case "link_url":   return json(200, await link.linkUrl(body));
      case "simulate":   return json(200, await link.simulateReply(body));
      case "translate":  return json(200, await link.translate(body));
      case "health":     return json(200, await link.health(body));
      case "history_info": return json(200, await link.historyInfo(body));
      case "history":    return json(200, await link.history(body));
      case "test_info":  return json(200, await link.testInfo());
      case "test_start": return json(200, await link.testStart(station));
      case "test_cancel": return json(200, await link.testCancel());
      default:           return json(400, { error: "Unknown op" });
    }
  } catch (e) {
    const status = e.status || 500;
    if (status >= 500) console.error("etsyMailOrderLink", op, e);
    return json(status, { error: e.message || "Something went wrong", code: e.code || null });
  }
};
