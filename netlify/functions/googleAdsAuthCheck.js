// netlify/functions/googleAdsAuthCheck.js
// ─────────────────────────────────────────────────────────────────────────────
// One-off credentials tester for the Ad Autopilot. Open it in a browser:
//   https://<site>/.netlify/functions/googleAdsAuthCheck
// It runs READ-ONLY checks and returns a per-credential green/red so a
// fat-fingered paste is obvious. It does NOT mutate anything in Google Ads.
//
// Self-contained (doesn't require the engine) so you can deploy + test it
// before wiring the rest. Uses the same env var names the engine reads.
// Safe to leave deployed, or delete once you've confirmed green across the board.
// ─────────────────────────────────────────────────────────────────────────────

const fetch = require("node-fetch");
const ENV = process.env;
const V = ENV.GADS_API_VERSION || "v24";
const CID = (ENV.GADS_CUSTOMER_ID || "").replace(/\D/g, "");
const LOGIN = (ENV.GADS_LOGIN_CUSTOMER_ID || "").replace(/\D/g, "");

function row(name, ok, detail) { return { name, status: ok ? "ok" : "FAIL", detail }; }

async function run() {
  const checks = [];
  const present = k => typeof ENV[k] === "string" && ENV[k].trim().length > 0;

  // 1) Presence + shape of the seven values
  checks.push(row("GADS_API_VERSION", /^v\d+$/.test(V), V));
  checks.push(row("GADS_DEVELOPER_TOKEN", present("GADS_DEVELOPER_TOKEN"),
    present("GADS_DEVELOPER_TOKEN") ? "present (" + ENV.GADS_DEVELOPER_TOKEN.length + " chars)" : "missing"));
  checks.push(row("GADS_CLIENT_ID",
    present("GADS_CLIENT_ID") && /\.apps\.googleusercontent\.com$/.test(ENV.GADS_CLIENT_ID),
    present("GADS_CLIENT_ID") ? "present" : "missing / wrong format"));
  checks.push(row("GADS_CLIENT_SECRET",
    present("GADS_CLIENT_SECRET") && /^GOCSPX-/.test(ENV.GADS_CLIENT_SECRET),
    present("GADS_CLIENT_SECRET") ? "present (GOCSPX-…)" : "missing / not a GOCSPX- secret"));
  checks.push(row("GADS_REFRESH_TOKEN",
    present("GADS_REFRESH_TOKEN") && /^1\/\//.test(ENV.GADS_REFRESH_TOKEN),
    present("GADS_REFRESH_TOKEN") ? "present (1//…)" : "missing / not a 1// token"));
  checks.push(row("GADS_LOGIN_CUSTOMER_ID", /^\d{10}$/.test(LOGIN),
    LOGIN ? LOGIN + (/^\d{10}$/.test(LOGIN) ? "" : " ← should be 10 digits, no dashes") : "missing"));
  checks.push(row("GADS_CUSTOMER_ID", /^\d{10}$/.test(CID),
    CID ? CID + (/^\d{10}$/.test(CID) ? "" : " ← should be 10 digits, no dashes") : "missing"));

  const anyMissing = checks.some(c => c.status === "FAIL");

  // 2) OAuth: mint an access token from the refresh token (proves client id/secret/refresh together)
  let token = null;
  if (!anyMissing) {
    try {
      const res = await fetch("https://oauth2.googleapis.com/token", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          client_id: ENV.GADS_CLIENT_ID, client_secret: ENV.GADS_CLIENT_SECRET,
          refresh_token: ENV.GADS_REFRESH_TOKEN, grant_type: "refresh_token"
        })
      });
      const data = await res.json().catch(() => ({}));
      if (res.ok && data.access_token) {
        token = data.access_token;
        checks.push(row("OAuth token exchange", true, "minted access token ✓"));
      } else {
        checks.push(row("OAuth token exchange", false,
          (data.error || res.status) + " — " + (data.error_description || "check client id/secret/refresh token; did you rotate the secret?")));
      }
    } catch (e) { checks.push(row("OAuth token exchange", false, e.message)); }
  } else {
    checks.push(row("OAuth token exchange", false, "skipped — fix the values above first"));
  }

  // 3) Developer token + manager context: list accessible customers (read-only, no headers beyond auth+dev-token)
  if (token) {
    try {
      const res = await fetch(`https://googleads.googleapis.com/${V}/customers:listAccessibleCustomers`, {
        headers: { "Authorization": "Bearer " + token, "developer-token": ENV.GADS_DEVELOPER_TOKEN }
      });
      const data = await res.json().catch(() => ({}));
      if (res.ok) {
        const ids = (data.resourceNames || []).map(r => r.split("/")[1]);
        checks.push(row("Developer token + access", true,
          "developer token valid · sees " + ids.length + " account(s): " + ids.join(", ")));
        const linkedOk = ids.includes(CID) || ids.includes(LOGIN);
        checks.push(row("Brites account reachable", linkedOk,
          linkedOk ? "CustomBrites (" + CID + ") is reachable ✓"
                   : "CustomBrites (" + CID + ") not in the accessible list — confirm the manager link was accepted"));
      } else {
        const err = JSON.stringify(data).slice(0, 300);
        checks.push(row("Developer token + access", false,
          res.status + " — " + (/DEVELOPER_TOKEN/.test(err) ? "developer token not approved/incorrect" : err)));
      }
    } catch (e) { checks.push(row("Developer token + access", false, e.message)); }
  }

  // 4) Real read against the Brites account (proves login-customer-id + customer-id pairing)
  if (token) {
    try {
      const res = await fetch(`https://googleads.googleapis.com/${V}/customers/${CID}/googleAds:search`, {
        method: "POST",
        headers: {
          "Authorization": "Bearer " + token, "developer-token": ENV.GADS_DEVELOPER_TOKEN,
          "login-customer-id": LOGIN, "Content-Type": "application/json"
        },
        body: JSON.stringify({ query: "SELECT customer.id, customer.descriptive_name, customer.currency_code FROM customer LIMIT 1" })
      });
      const data = await res.json().catch(() => ({}));
      if (res.ok) {
        const c = ((data.results || [])[0] || {}).customer || {};
        checks.push(row("Live read (login + customer pairing)", true,
          "read account: " + (c.descriptiveName || c.id || CID) + " · " + (c.currencyCode || "")));
      } else {
        const err = JSON.stringify(data).slice(0, 320);
        checks.push(row("Live read (login + customer pairing)", false,
          res.status + " — " + (/USER_PERMISSION_DENIED|login-customer-id/i.test(err)
            ? "login-customer-id ↔ customer-id mismatch, or manager link not accepted" : err)));
      }
    } catch (e) { checks.push(row("Live read (login + customer pairing)", false, e.message)); }
  }

  return { allGreen: checks.every(c => c.status === "ok"), apiVersion: V, checks };
}

exports.handler = async (event) => {
  const result = await run();
  const wantsHtml = (event.headers && /text\/html/.test(event.headers.accept || ""));
  if (!wantsHtml) {
    return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(result, null, 2) };
  }
  const rows = result.checks.map(c =>
    `<tr><td style="padding:8px 12px">${c.status === "ok" ? "🟢" : "🔴"}</td>
        <td style="padding:8px 12px;font-weight:600">${c.name}</td>
        <td style="padding:8px 12px;color:#555;font-family:monospace;font-size:12px">${(c.detail || "").replace(/</g, "&lt;")}</td></tr>`).join("");
  const html = `<!doctype html><meta charset="utf-8"><title>Ad Autopilot · credentials check</title>
  <body style="font-family:-apple-system,Segoe UI,sans-serif;max-width:760px;margin:40px auto;color:#1a1a1a">
  <h2 style="font-weight:600">Ad Autopilot — credentials check</h2>
  <p style="font-size:18px">${result.allGreen ? "🟢 All checks passed — you're cleared to deploy the engine and run a dry-run measure." : "🔴 Something needs fixing — see the red rows below."}</p>
  <table style="border-collapse:collapse;width:100%;border:1px solid #eee">${rows}</table>
  <p style="color:#888;font-size:12px;margin-top:16px">Read-only. Nothing in your Google Ads account was changed. Safe to delete this function once green.</p></body>`;
  return { statusCode: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body: html };
};
