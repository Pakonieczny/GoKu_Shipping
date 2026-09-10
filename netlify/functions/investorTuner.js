/*  netlify/functions/investorTuner.js
 *  The door the local dip-rule tuner (scripts/dipTuner.js, on the operator's
 *  own computer) uses. It never sees the console's session: it carries a
 *  runner key the console issued, of which only the hash is stored.
 *
 *    POST { action: "hello" }                → control, live dip settings, the library index
 *    POST { action: "bars", ids: [...] }     → packed month documents (≤ 30 per call)
 *    POST { action: "report", status, leaderboard, outcomes } → progress into Firestore
 *    POST { action: "apply", index, specHash, params, score } → tuned settings into the dip lane
 *
 *  Paper only, no model gateway, no market data provider.
 */
"use strict";
const A = require("./_investorAdmin");
const TUNER = require("./_investorDipTuner");
const DIP = require("./_investorDipReversal");
const BARS = require("./_investorBarStore");

const MAX_BODY = 6 * 1024 * 1024;
function reply(statusCode, body) { return { statusCode, headers: { "content-type": "application/json", "cache-control": "no-store" }, body: JSON.stringify(body) }; }

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") return reply(405, { error: "POST only" });
  if ((event.body || "").length > MAX_BODY) return reply(413, { error: "body too large" });
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return reply(400, { error: "invalid JSON" }); }
  const auth = String(event.headers.authorization || event.headers.Authorization || "");
  const key = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  const control = await TUNER.readControl(A);
  if (!TUNER.keyMatches(key, control.keyHash)) return reply(401, { error: "runner key not accepted; issue a new key in the console's Dip tuner section" });
  const nowMs = Date.now();
  try {
    switch (body.action) {
      case "hello": {
        const ctrlSnap = await A.col(A.COL.control).doc("control").get();
        const ctrl = ctrlSnap.exists ? ctrlSnap.data() : {};
        const dip = DIP.normalizeSettings(ctrl.dip);
        const snap = await A.col(BARS.COL).select("symbol", "month", "bars", "days", "complete", "fetchedAtMs").get();
        const library = snap.docs.map((d) => { const x = d.data(); return { id: d.id, symbol: x.symbol, month: x.month, bars: x.bars || 0, days: x.days || 0, complete: x.complete !== false, fetchedAtMs: x.fetchedAtMs || null }; });
        const { keyHash: _h, ...safe } = control;
        await A.col(TUNER.COL).doc("status").set({ lastSeenMs: nowMs, lastHelloMs: nowMs }, { merge: true });
        return reply(200, { ok: true, control: safe, dip, library, libraryStatus: await BARS.status(A), serverNowMs: nowMs });
      }
      case "bars": {
        const ids = Array.isArray(body.ids) ? body.ids.slice(0, 30).filter((x) => /^[A-Z][A-Z0-9.-]{0,9}_\d{4}-\d{2}$/.test(String(x))) : [];
        const docs = [];
        for (const id of ids) {
          const s = await A.col(BARS.COL).doc(id).get();
          if (!s.exists) { docs.push({ id, missing: true }); continue; }
          const x = s.data();
          const buf = x.data ? (Buffer.isBuffer(x.data) ? x.data : Buffer.from(x.data.buffer || x.data)) : null;
          docs.push({ id, symbol: x.symbol, month: x.month, bars: x.bars || 0, days: x.days || 0, complete: x.complete !== false, fetchedAtMs: x.fetchedAtMs || null, data: buf ? buf.toString("base64") : null });
        }
        return reply(200, { ok: true, docs });
      }
      case "report": {
        const status = body.status && typeof body.status === "object" ? body.status : {};
        await A.col(TUNER.COL).doc("status").set({ ...clip(status), lastSeenMs: nowMs, version: TUNER.VERSION }, { merge: false });
        if (body.leaderboard && typeof body.leaderboard === "object") await TUNER.writeDoc(A, "leaderboard", { ...clip(body.leaderboard), entries: (body.leaderboard.entries || []).slice(0, 100) });
        if (body.outcomes && typeof body.outcomes === "object") await TUNER.writeDoc(A, "variables", clip(body.outcomes));
        const fresh = await TUNER.readControl(A);
        const { keyHash: _h2, ...safe } = fresh;
        return reply(200, { ok: true, control: safe, serverNowMs: nowMs });
      }
      case "apply": {
        if (!body.params || typeof body.params !== "object") return reply(400, { error: "params required" });
        if (!control.autoApply) return reply(409, { error: "automatic apply is off in the console" });
        const out = await TUNER.applyParams(A, { params: body.params, source: { specHash: String(body.specHash || ""), index: Number(body.index) || 0, score: Number(body.score) || null, rank: Number(body.rank) || null, how: "auto" }, actorId: "dip-tuner", reason: "dip tuner: most successful set applied automatically", nowMs });
        return reply(200, { ok: true, applied: out.applied, settingsVersion: out.dip.version });
      }
      default: return reply(400, { error: "unknown action" });
    }
  } catch (e) {
    console.error("investorTuner failed", { action: body.action, error: e.message });
    return reply(500, { error: String(e.message || e).slice(0, 200) });
  }
};
/** Keep reported documents within Firestore's 1 MB document limit. */
function clip(obj) {
  const s = JSON.stringify(obj);
  if (s.length < 900000) return JSON.parse(s);
  const out = { ...obj }; if (Array.isArray(out.entries)) out.entries = out.entries.slice(0, 40); if (out.byVariable) for (const k of Object.keys(out.byVariable)) out.byVariable[k] = { ...out.byVariable[k], values: (out.byVariable[k].values || []).slice(0, 12) };
  return JSON.parse(JSON.stringify(out));
}
