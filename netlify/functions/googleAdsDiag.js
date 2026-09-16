// netlify/functions/googleAdsDiag.js
// ─────────────────────────────────────────────────────────────────────────────
// Read-only ground-truth diagnostic. Open in a browser:
//   https://goldenspike.app/.netlify/functions/googleAdsDiag
// Shows, independent of the console/snapshot:
//   1) EVERY campaign that actually exists in your Google Ads account (live read,
//      no date segment — paused/zero-impression campaigns included)
//   2) Recent approvals and their real status (PENDING / APPROVED / APPLIED / REJECTED / error)
//   3) The mutate ledger (every apply attempt, dry-run flag, and error message)
//   4) Current control (enabled / dryRun)
// Nothing is mutated. Safe to delete once you've diagnosed the pipeline.
// Self-contained: only needs node-fetch + ./firebaseAdmin (already in the repo).
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require("node-fetch");
const ENV = process.env;
const V = ENV.GADS_API_VERSION || "v24";
const CID = (ENV.GADS_CUSTOMER_ID || "").replace(/\D/g, "");
const LOGIN = (ENV.GADS_LOGIN_CUSTOMER_ID || "").replace(/\D/g, "");

function db() {
  try { const admin = require("./firebaseAdmin"); return admin.firestore(); }
  catch (e) { return null; }
}
function fromMicros(m) { return m == null ? null : Math.round((Number(m) / 1e6) * 100) / 100; }

async function mintToken() {
  const res = await fetch("https://oauth2.googleapis.com/token", {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      client_id: ENV.GADS_CLIENT_ID, client_secret: ENV.GADS_CLIENT_SECRET,
      refresh_token: ENV.GADS_REFRESH_TOKEN, grant_type: "refresh_token"
    })
  });
  const d = await res.json().catch(() => ({}));
  if (!res.ok || !d.access_token) throw new Error("OAuth: " + (d.error_description || d.error || res.status));
  return d.access_token;
}

async function liveCampaigns() {
  const token = await mintToken();
  const res = await fetch(`https://googleads.googleapis.com/${V}/customers/${CID}/googleAds:search`, {
    method: "POST",
    headers: {
      "Authorization": "Bearer " + token, "developer-token": ENV.GADS_DEVELOPER_TOKEN,
      "login-customer-id": LOGIN, "Content-Type": "application/json"
    },
    body: JSON.stringify({ query:
      `SELECT campaign.id, campaign.name, campaign.status, campaign_budget.amount_micros
       FROM campaign WHERE campaign.status != 'REMOVED' ORDER BY campaign.id` })
  });
  const d = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error("GAQL " + res.status + " — " + JSON.stringify(d).slice(0, 300));
  return ((d.results) || []).map(r => ({
    id: r.campaign.id, name: r.campaign.name, status: r.campaign.status,
    budget: fromMicros(r.campaignBudget && r.campaignBudget.amountMicros)
  }));
}

async function run() {
  const out = { campaigns: null, campaignsError: null, approvals: [], ledger: [], control: null, dbOk: false };
  try { out.campaigns = await liveCampaigns(); } catch (e) { out.campaignsError = e.message; }
  const d = db();
  if (d) {
    out.dbOk = true;
    try { const s = await d.collection("Brites_GAds_Control").doc("control").get(); if (s.exists) out.control = s.data(); } catch (e) {}
    try {
      const ap = await d.collection("Brites_GAds_Approvals").limit(50).get();
      const rows = [];
      ap.forEach(x => { const v = x.data(); rows.push({ id: x.id, type: v.type, status: v.status, summary: (v.summary || "").slice(0, 90), ts: v.createdAt && v.createdAt.toMillis ? v.createdAt.toMillis() : 0, appliedAt: v.appliedAt && v.appliedAt.toMillis ? v.appliedAt.toMillis() : null }); });
      rows.sort((a, b) => b.ts - a.ts); out.approvals = rows.slice(0, 15);
    } catch (e) { out.approvals = [{ error: e.message }]; }
    try {
      const lg = await d.collection("Brites_GAds_Ledger").orderBy("at", "desc").limit(20).get();
      lg.forEach(x => { const v = x.data(); out.ledger.push({ at: v.at && v.at.toMillis ? v.at.toMillis() : null, kind: v.kind, label: v.label, service: v.service, ok: v.ok !== false, validateOnly: !!v.validateOnly, error: v.error || null, results: v.results != null ? v.results : undefined }); });
    } catch (e) { out.ledger = [{ error: e.message }]; }
  }
  return out;
}

function tdiff(ms) { if (!ms) return ""; const s = Math.floor((Date.now() - ms) / 1000); if (s < 60) return s + "s ago"; if (s < 3600) return Math.floor(s / 60) + "m ago"; if (s < 86400) return Math.floor(s / 3600) + "h ago"; return Math.floor(s / 86400) + "d ago"; }
const esc = s => String(s == null ? "" : s).replace(/[&<>]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

exports.handler = async (event) => {
  const r = await run();
  const wantsHtml = (event.headers && /text\/html/.test(event.headers.accept || ""));
  if (!wantsHtml) return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(r, null, 2) };

  const baCount = (r.campaigns || []).filter(c => /^BA · /.test(c.name)).length;
  const campRows = r.campaignsError
    ? `<tr><td colspan="4" style="color:#b3402f">Live read failed: ${esc(r.campaignsError)}</td></tr>`
    : (r.campaigns || []).map(c => `<tr${/^BA · /.test(c.name) ? ' style="background:#fbf6e9"' : ""}>
        <td style="font-family:monospace;font-size:11px">${esc(c.id)}</td>
        <td style="font-weight:600">${esc(c.name)}</td>
        <td>${esc(c.status)}</td>
        <td style="text-align:right;font-family:monospace">${c.budget == null ? "—" : "$" + c.budget}</td></tr>`).join("") || `<tr><td colspan="4">No campaigns in account.</td></tr>`;

  const apRows = (r.approvals || []).map(a => a.error ? `<tr><td colspan="4" style="color:#b3402f">${esc(a.error)}</td></tr>` : `<tr>
      <td>${esc(a.type)}</td>
      <td><b style="color:${a.status === "APPLIED" ? "#3c7a39" : a.status === "REJECTED" ? "#8a8a8a" : a.status === "APPROVED" ? "#b3402f" : "#7a5a1d"}">${esc(a.status)}</b>${a.status === "APPROVED" ? " ⚠ approved but not applied — apply likely errored" : ""}</td>
      <td style="font-size:12px">${esc(a.summary)}</td>
      <td style="font-size:11px;color:#888">${tdiff(a.ts)}</td></tr>`).join("") || `<tr><td colspan="4">No approvals yet.</td></tr>`;

  const lgRows = (r.ledger || []).map(l => l.error && !l.kind ? `<tr><td colspan="4" style="color:#b3402f">${esc(l.error)}</td></tr>` : `<tr>
      <td>${l.ok ? "🟢" : "🔴"}</td>
      <td>${esc(l.label || l.service || l.kind || "")}${l.validateOnly ? " · dry-run" : ""}</td>
      <td style="font-size:11px;color:#b3402f">${esc(l.error || "")}</td>
      <td style="font-size:11px;color:#888">${tdiff(l.at)}</td></tr>`).join("") || `<tr><td colspan="4">No ledger entries.</td></tr>`;

  const tbl = (title, head, body) => `<h3 style="margin:22px 0 6px">${title}</h3><table style="border-collapse:collapse;width:100%;border:1px solid #eee;font-size:13px"><thead><tr style="background:#f3f0e9;text-align:left">${head}</tr></thead><tbody>${body}</tbody></table>`;
  const html = `<!doctype html><meta charset="utf-8"><title>Ad Autopilot · diagnostic</title>
  <body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:860px;margin:36px auto;color:#1a1a1a;padding:0 16px">
  <h2 style="font-weight:600">Ad Autopilot — ground-truth diagnostic</h2>
  <div style="font-size:13.5px;color:#555">Control: <b>${r.control ? (r.control.enabled ? "LIVE" : "stopped") : "?"}</b> · dry-run <b>${r.control ? (r.control.dryRun ? "ON" : "off") : "?"}</b> · autopilot-built campaigns found in account: <b>${baCount}</b></div>
  ${tbl("Campaigns actually in your Google Ads account <span style='font-weight:400;font-size:12px;color:#888'>(BA · rows highlighted)</span>", "<th>ID</th><th>Name</th><th>Status</th><th style='text-align:right'>Budget</th>", campRows)}
  ${tbl("Approvals", "<th>Type</th><th>Status</th><th>Summary</th><th>When</th>", apRows)}
  ${tbl("Mutate ledger <span style='font-weight:400;font-size:12px;color:#888'>(every apply attempt + errors)</span>", "<th></th><th>Action</th><th>Error</th><th>When</th>", lgRows)}
  <p style="color:#888;font-size:12px;margin-top:20px">Read-only. How to read this: if a <b>BA ·</b> campaign appears above, the pipeline created it (enable it in Google Ads to make it spend). If an approval shows <b>APPROVED</b> (not APPLIED) and the ledger has a 🔴 error, the apply failed — the error tells you why.</p></body>`;
  return { statusCode: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body: html };
};
