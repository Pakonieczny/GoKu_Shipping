// netlify/functions/charmSetsConsole.js
// Backend for the standalone Set Matcher Console (Brites_Set_Matcher_Console.html).
// Runs entirely outside the storefront. Passcode-gated (X-Edit-Passcode).
// Actions (POST {action}):
//   status    -> verification progress summary + recent family verdicts
//   kick      -> triggers verifyCharmSets-background (15-min run), returns 202 status
//   exportCsv -> full product↔partner linkage CSV, enriched with live visual
//                verdicts from Firestore (confirmed / pruned / pending)

const fetch = require("node-fetch");
const { CHARM_SETS } = require("./charmSetsData");
let _fb = null;
function fb() {
  if (_fb !== null) return _fb;
  try { const admin = require("./firebaseAdmin"); _fb = { db: admin.firestore() }; }
  catch (e) { _fb = false; }
  return _fb;
}
const HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type, X-Edit-Passcode",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json"
};

async function getState() {
  const f = fb();
  if (!f) return {};
  try { const s = await f.db.collection("Brites_Editor_Meta").doc("charmVerifyState").get();
    return s.exists ? s.data() : {}; } catch (e) { return {}; }
}
async function getAudits() {
  const f = fb();
  if (!f) return {};
  try { const s = await f.db.collection("Brites_Editor_Meta").doc("charmVerifyAudits").get();
    return s.exists ? s.data() : {}; } catch (e) { return {}; }
}

function buildVerdictIndex(state, audits) {
  // per-handle verdict: confirmed / pruned / pending (+charm fields from audits)
  const byHandle = {};
  for (const k of Object.keys(audits || {})) {
    const a = audits[k];
    (a.detail || []).forEach(d => { byHandle[d.handle] = d; });
    if (a.members && a.members[0] && !byHandle[a.members[0]]) byHandle[a.members[0]] = { same_charm: true, reference: true };
  }
  return byHandle;
}

function csvEscape(v) { v = String(v == null ? "" : v); return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; }

async function exportCsv() {
  const [state, audits] = [await getState(), await getAudits()];
  const idx = buildVerdictIndex(state, audits);
  const rows = [["product_handle","matched_partner_handle","partner_form","partner_title","match_basis","visual_verification","charm_seen","charm_detail","confidence","reason"]];
  for (const h of Object.keys(CHARM_SETS)) {
    for (const p of CHARM_SETS[h]) {
      const v = idx[p.h];
      let verdict = "pending";
      if (v) verdict = v.reference ? "reference (confirmed family)" : (v.same_charm ? "confirmed" : "PRUNED — different charm");
      rows.push([h, p.h, p.f, p.t, "charm signature (title engine)", verdict,
        v && v.charm || "", v && v.charm_detail || "", v && v.confidence || "", v && v.reason || ""]);
    }
  }
  return rows.map(r => r.map(csvEscape).join(",")).join("\n");
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: HEADERS };
  const pass = (event.headers && (event.headers["x-edit-passcode"] || event.headers["X-Edit-Passcode"])) || "";
  if (pass !== process.env.EDIT_PASSCODE) return { statusCode: 401, headers: HEADERS, body: JSON.stringify({ error: "bad passcode" }) };

  try {
    const b = event.httpMethod === "POST" ? JSON.parse(event.body || "{}") : {};
    const action = b.action || "status";

    if (action === "kick") {
      const base = process.env.URL || "https://goldenspike.app";
      const r = await fetch(base + "/.netlify/functions/verifyCharmSets-background", {
        method: "POST", headers: { "X-Edit-Passcode": process.env.EDIT_PASSCODE } });
      return { statusCode: 200, headers: HEADERS, body: JSON.stringify({ ok: true, kicked: true, upstream: r.status,
        note: "Background run started (up to 15 min). Refresh status to watch progress." }) };
    }

    if (action === "exportCsv") {
      const csvText = await exportCsv();
      return { statusCode: 200, headers: { ...HEADERS, "Content-Type": "text/csv",
        "Content-Disposition": "attachment; filename=brites_charm_set_links.csv" }, body: csvText };
    }

    // status
    const state = await getState();
    const audits = await getAudits();
    const recent = Object.keys(audits).slice(-12).map(k => ({ family: k, at: audits[k].at,
      members: (audits[k].members || []).length,
      verdicts: (audits[k].detail || []).map(d => (d.same_charm ? "✓ " : "✗ ") + d.handle + (d.charm ? " (" + d.charm + ")" : "")) }));
    return { statusCode: 200, headers: HEADERS, body: JSON.stringify({
      ok: true, summary: state.summary || { familiesVerified: Object.keys(state.verified || {}).length, complete: false, note: "no run yet" },
      prunedTotal: state.pruned || 0, recent }, null, 1) };
  } catch (e) {
    return { statusCode: 500, headers: HEADERS, body: JSON.stringify({ error: String(e.message || e) }) };
  }
};
