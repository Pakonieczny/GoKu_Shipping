// netlify/functions/charmSetsConsole.js
// Backend for the standalone Set Matcher Console (Brites_Set_Matcher_Console.html).
// Runs entirely outside the storefront. NO passcode (owner request — one-time use).
// Actions (POST {action}):
//   status    -> verification progress summary + recent family verdicts
//   kick      -> triggers verifyCharmSets-background (15-min run), returns 202 status
//   exportCsv -> full product↔partner linkage CSV, enriched with live visual
//                verdicts from Firestore (confirmed / pruned / pending)

const fetch = require("node-fetch");
const { CHARM_SETS, PHASE2_FAMILIES: P2_FAMILIES, PHASE2_VERSION: DATA_VERSION } = require("./charmSetsData");
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
  if (event.httpMethod === "GET") {
    const st = await getState();
    return { statusCode: 200, headers: HEADERS, body: JSON.stringify({
      dataVersion: DATA_VERSION, summary: st.summary || {},
      pairCount: Object.keys(st.pairs || {}).length, pairs: st.pairs || {} }) };
  }
  // Passcode removed at owner request (single-user, one-time tool). Delete these functions after the run.

  try {
    const b = event.httpMethod === "POST" ? JSON.parse(event.body || "{}") : {};
    const action = b.action || "status";

    // family total for THIS deployed universe (same grouping as the verifier)
    function familiesTotal(){ return P2_FAMILIES.length; }
    function _unused_familiesTotal(){
      const visited = new Set(); let n = 0;
      for (const h of Object.keys(CHARM_SETS)) {
        if (visited.has(h)) continue;
        const members = [h];
        for (const p of CHARM_SETS[h]) if (!visited.has(p.h) && CHARM_SETS[p.h]) members.push(p.h);
        members.forEach(m => visited.add(m));
        if (members.length >= 2) n++;
      }
      return n;
    }

    if (action === "reset") {
      const f = fb();
      if (!f) return { statusCode: 200, headers: HEADERS, body: JSON.stringify({ ok: false, error: "no firestore" }) };
      await f.db.collection("Brites_Editor_Meta").doc("charmVerifyState")
        .set({ verified: {}, pruned: 0, dataVersion: DATA_VERSION, summary: { reset: new Date().toISOString() } });
      return { statusCode: 200, headers: HEADERS, body: JSON.stringify({ ok: true, reset: true, dataVersion: DATA_VERSION }) };
    }

    if (action === "kick") {
      const base = process.env.URL || "https://goldenspike.app";
      const r = await fetch(base + "/.netlify/functions/verifyCharmSets-background", {
        method: "POST" });
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
    const all = Object.values(state.verified || {});
    const errs = all.filter(v => typeof v.verdict === "string" && v.verdict.indexOf("error") === 0);
    const liveVerified = all.length - errs.length; // live count, updates every checkpoint mid-run
    const total = familiesTotal(); /* always THIS universe's count, never a stale summary */
    const staleState = state.dataVersion !== DATA_VERSION;
    const liveSummary = Object.assign({}, state.summary || {}, {
      familiesVerified: liveVerified, familiesTotal: total,
      complete: liveVerified >= total });
    const liveCount = Object.keys(state.verified || {}).length;
    const summary = Object.assign({ complete: false }, state.summary || {});
    summary.familiesVerified = liveCount; // live, regardless of summary staleness
    if (summary.inProgress) summary.note = "run in progress — refresh to watch";
    return { statusCode: 200, headers: HEADERS, body: JSON.stringify({
      ok: true, summary, dataVersion: DATA_VERSION, staleState: staleState || false,
      prunedTotal: state.pruned || 0,
      errorCount: errs.length,
      errorSample: errs.slice(-3).map(v => v.verdict),
      note: errs.length ? "Errored families retry automatically on the next run." : undefined,
      recent }, null, 1) };
  } catch (e) {
    return { statusCode: 500, headers: HEADERS, body: JSON.stringify({ error: String(e.message || e) }) };
  }
};
