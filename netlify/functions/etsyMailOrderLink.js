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
    switch (op) {
      case "whoami":     return json(200, { operator: { username: station.username, name: station.name } });
      case "disconnect": return json(200, await link.disconnect(station));
      case "sync":       return json(200, await link.sync(body));
      case "order":      return json(200, await link.order(body));
      case "thread":     return json(200, await link.thread(body));
      case "ask":        return json(200, await link.ask(station, body));
      case "retry":      return json(200, await link.retry(body));
      case "cancel":     return json(200, await link.cancel(body));
      case "copied":     return json(200, await link.markCopied(body));
      case "sent":       return json(200, await link.markSent(body));
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
      default:           return json(400, { error: "Unknown op" });
    }
  } catch (e) {
    const status = e.status || 500;
    if (status >= 500) console.error("etsyMailOrderLink", op, e);
    return json(status, { error: e.message || "Something went wrong", code: e.code || null });
  }
};
