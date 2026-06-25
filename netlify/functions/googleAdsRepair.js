// netlify/functions/googleAdsRepair.js
// ─────────────────────────────────────────────────────────────────────────────
// ONE-CLICK REPAIR for approvals stuck in APPROVED (approved, but the apply errored
// because the saved payload was frozen with the rejected Campaign.text_guidelines
// field). This endpoint is fully self-contained — it does NOT call googleAdsAutopilot.js,
// so it works even if that file hasn't been redeployed.
//
// What it does, for every approval whose status is APPROVED (not yet APPLIED):
//   1) loads its frozen payload from Firestore
//   2) STRIPS text_guidelines / textGuidelines from the campaign create op
//   3) sends it to Google Ads (real create — campaigns are built PAUSED)
//   4) flips the approval to APPLIED and records the new campaign resource name
//
// Usage (open in a browser):
//   https://goldenspike.app/.netlify/functions/googleAdsRepair?key=YOUR_EDIT_PASSCODE
// Add &dry=1 to validate WITHOUT creating (a safe test run).
//
// Safe to delete once your campaigns exist. Needs node-fetch + ./firebaseAdmin only.
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require("node-fetch");
const ENV = process.env;
const V = ENV.GADS_API_VERSION || "v24";
const CID = (ENV.GADS_CUSTOMER_ID || "").replace(/\D/g, "");
const LOGIN = (ENV.GADS_LOGIN_CUSTOMER_ID || "").replace(/\D/g, "");
const APPROVALS = "Brites_GAds_Approvals";

function db() {
  try { const admin = require("./firebaseAdmin"); return { d: admin.firestore(), admin }; }
  catch (e) { return { d: null, admin: null }; }
}
const esc = s => String(s == null ? "" : s).replace(/[&<>]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

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

// Remove the field that Google rejects from any campaign create/update op (both spellings).
function sanitize(ops) {
  let removed = 0;
  (Array.isArray(ops) ? ops : []).forEach(op => {
    if (!op) return;
    const c = (op.campaignOperation && (op.campaignOperation.create || op.campaignOperation.update)) || op.create || op.update;
    if (c && typeof c === "object") {
      if (c.text_guidelines !== undefined) { delete c.text_guidelines; removed++; }
      if (c.textGuidelines !== undefined) { delete c.textGuidelines; removed++; }
    }
  });
  return removed;
}

async function applyOne(token, ops, dry) {
  const res = await fetch(`https://googleads.googleapis.com/${V}/customers/${CID}/googleAds:mutate`, {
    method: "POST",
    headers: {
      "Authorization": "Bearer " + token, "developer-token": ENV.GADS_DEVELOPER_TOKEN,
      "login-customer-id": LOGIN, "Content-Type": "application/json"
    },
    body: JSON.stringify({ mutateOperations: ops, partialFailure: false, validateOnly: !!dry })
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error("HTTP " + res.status + " — " + JSON.stringify(data).slice(0, 500));
  if (data.partialFailureError) throw new Error("partial failure — " + JSON.stringify(data.partialFailureError).slice(0, 400));
  // pull the new campaign resource name if present
  let campaign = null;
  (data.mutateOperationResponses || []).forEach(r => {
    if (r && r.campaignResult && r.campaignResult.resourceName) campaign = r.campaignResult.resourceName;
  });
  return campaign;
}

async function run(dry) {
  const out = { ran: true, dry: !!dry, found: 0, created: 0, validated: 0, items: [], fatal: null };
  const { d, admin } = db();
  if (!d) { out.fatal = "Firestore unavailable (./firebaseAdmin not found)."; return out; }
  if (!CID || !ENV.GADS_DEVELOPER_TOKEN || !ENV.GADS_CLIENT_ID) { out.fatal = "Missing Google Ads env vars."; return out; }

  let token;
  try { token = await mintToken(); } catch (e) { out.fatal = e.message; return out; }

  let snap;
  try { snap = await d.collection(APPROVALS).where("status", "==", "APPROVED").limit(25).get(); }
  catch (e) { out.fatal = "Approvals read failed: " + e.message; return out; }

  const docs = [];
  snap.forEach(x => docs.push({ id: x.id, data: x.data() }));
  out.found = docs.length;

  for (const doc of docs) {
    const it = doc.data || {};
    const item = { id: doc.id, summary: (it.summary || "").slice(0, 90), stripped: 0, status: "", campaign: null, error: null };
    const p = it.payload || {};
    const ops = p.mutateOperations || (p.operations ? null : null);
    if (!ops) { item.status = "skipped"; item.error = "no mutateOperations in payload (can't repair this shape)."; out.items.push(item); continue; }
    item.stripped = sanitize(ops);
    try {
      const campaign = await applyOne(token, ops, dry);
      if (dry) { item.status = "validated"; out.validated++; }
      else {
        item.status = "created"; item.campaign = campaign; out.created++;
        try { await d.collection(APPROVALS).doc(doc.id).set({ status: "APPLIED", appliedAt: admin.firestore.FieldValue.serverTimestamp(), repairedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true }); }
        catch (e) { item.error = "created OK but status update failed: " + e.message; }
      }
    } catch (e) { item.status = "error"; item.error = e.message; }
    out.items.push(item);
  }
  return out;
}

exports.handler = async (event) => {
  const q = (event && event.queryStringParameters) || {};
  const wantsHtml = (event.headers && /text\/html/.test(event.headers.accept || ""));
  const gate = ENV.EDIT_PASSCODE || "";

  // passcode gate — this endpoint mutates, so it must not run by accident
  if (!gate || q.key !== gate) {
    const msg = "Add ?key=YOUR_EDIT_PASSCODE to the URL to run the repair. Add &dry=1 to validate without creating.";
    if (!wantsHtml) return { statusCode: 401, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ error: "unauthorized", hint: msg }) };
    return { statusCode: 401, headers: { "Content-Type": "text/html; charset=utf-8" },
      body: `<!doctype html><meta charset="utf-8"><body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:680px;margin:40px auto;color:#1a1a1a;padding:0 16px"><h2 style="font-weight:600">Ad Autopilot — repair</h2><p style="color:#555">${esc(msg)}</p></body>` };
  }

  const dry = q.dry === "1" || q.dry === "true";
  const r = await run(dry);

  if (!wantsHtml) return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(r, null, 2) };

  const rowColor = s => s === "created" ? "#3c7a39" : s === "validated" ? "#7a5a1d" : s === "error" ? "#b3402f" : "#8a8a8a";
  const rows = (r.items || []).map(i => `<tr>
      <td style="font-size:12px">${esc(i.summary)}</td>
      <td><b style="color:${rowColor(i.status)}">${esc(i.status || "?")}</b></td>
      <td style="font-family:monospace;font-size:11px">${esc(i.campaign || (i.error ? "" : "—"))}</td>
      <td style="font-size:11px;color:#b3402f">${esc(i.error || "")}</td></tr>`).join("") || `<tr><td colspan="4">No approvals were stuck in APPROVED — nothing to repair.</td></tr>`;

  const banner = r.fatal
    ? `<div style="background:#fbeae7;border:1px solid #e2b6ad;color:#b3402f;padding:12px 14px;border-radius:8px">Couldn't run: ${esc(r.fatal)}</div>`
    : `<div style="background:${r.created ? "#eaf3e8" : "#f6f3ec"};border:1px solid #d9d2c2;padding:12px 14px;border-radius:8px;font-size:14px">
        Found <b>${r.found}</b> stuck approval(s). ${r.dry ? `Validated <b>${r.validated}</b> (dry run — nothing created).` : `Created <b>${r.created}</b> campaign(s) in Google Ads (PAUSED).`}
        ${(!r.dry && r.created) ? " Open Google Ads or the console's Performance tab to enable them." : ""}</div>`;

  const html = `<!doctype html><meta charset="utf-8"><title>Ad Autopilot · repair</title>
  <body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:820px;margin:36px auto;color:#1a1a1a;padding:0 16px">
  <h2 style="font-weight:600">Ad Autopilot — repair stuck approvals</h2>
  ${banner}
  <h3 style="margin:22px 0 6px">Results ${r.dry ? "<span style='font-weight:400;font-size:12px;color:#888'>(dry run)</span>" : ""}</h3>
  <table style="border-collapse:collapse;width:100%;border:1px solid #eee;font-size:13px"><thead><tr style="background:#f3f0e9;text-align:left"><th>Draft</th><th>Result</th><th>New campaign</th><th>Error</th></tr></thead><tbody>${rows}</tbody></table>
  <p style="color:#888;font-size:12px;margin-top:18px">This stripped the rejected <code>text_guidelines</code> field from each frozen payload and re-sent it. Campaigns are created <b>PAUSED</b> — nothing spends until you enable it. ${r.dry ? "Remove <code>&dry=1</code> to actually create them." : "Re-run with <code>&dry=1</code> anytime to preview."} Safe to delete this function once done.</p></body>`;
  return { statusCode: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body: html };
};
