// netlify/functions/exchangeToken.js
// A single function that auto-detects your domain from request headers.
// If "sorting.goldenspike.app" => uses that redirect
// else if "goldenspike.app" => uses that
// else => defaults to goldenspike (you can invert this default if you wish).

const fetch = require("node-fetch");
const crypto = require("crypto");

// We only allow these two final domains:
const SORTING_DOMAIN      = "https://sorting.goldenspike.app";
const GOLDENSPIKE_DOMAIN  = "https://goldenspike.app";
const DESIGN_DOMAIN  = "https://design.goldenspike.app";

// Helpers for PKCE:
function generateRandomString(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++) {
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}

function generateCodeChallenge(codeVerifier) {
  const hash = crypto.createHash("sha256").update(codeVerifier).digest();
  return hash.toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

// Helper to auto-detect domain from the request's host header
function pickDomainFromHost(event) {
  // 'x-forwarded-host' is typical under Netlify; fallback to Host if needed
  const host = event.headers["x-forwarded-host"] 
            || event.headers["host"] 
            || "";

  console.log("exchangeToken => Detected host:", host);

  const param = (event.queryStringParameters || {}).redirect_domain || "";
  if (param === "sorting") {
    console.log("Overriding domain: user specified ?redirect_domain=sorting");
    return SORTING_DOMAIN;
  } else if (param === "goldenspike") {
    console.log("Overriding domain: user specified ?redirect_domain=goldenspike");
    return GOLDENSPIKE_DOMAIN;
  }
    else if (param === "design") {
    console.log("Overriding domain: user specified ?redirect_domain=design");
    return DESIGN_DOMAIN;
  }  

  // If no param => auto detect from host
  if (host.includes("sorting.goldenspike.app")) {
    console.log("Auto-detected sorting domain from host");
    return SORTING_DOMAIN;
  } 

    else if (host.includes("design.goldenspike.app")) {
    console.log("Auto-detected design domain from host");
    return DESIGN_DOMAIN;
  }  

    else if (host.includes("goldenspike.app")) {
    console.log("Auto-detected goldenspike domain from host");
    return GOLDENSPIKE_DOMAIN;
  }

  // If we can't detect, pick a default. Let's default to goldenspike:
  console.log("Host doesn't match either domain => defaulting to goldenspike");
  return GOLDENSPIKE_DOMAIN;
}

exports.handler = async function(event) {
  try {
    const query = event.queryStringParameters || {};
    const code = query.code;
    const codeVerifier = query.code_verifier;

    // 1) Decide which domain to use => either from ?redirect_domain=... or from host auto-detection
    const finalRedirectUri = pickDomainFromHost(event);

    console.log("exchangeToken => finalRedirectUri:", finalRedirectUri);

    // 2) If no code => begin OAuth w/ PKCE
    if (!code) {
      console.log("No ?code => starting PKCE handshake...");

      const newCodeVerifier = generateRandomString(64);
      const codeChallenge   = generateCodeChallenge(newCodeVerifier);
      
      console.log("PKCE code_verifier:", newCodeVerifier);
      console.log("PKCE code_challenge:", codeChallenge);

      const CLIENT_ID = process.env.CLIENT_ID;
      if (!CLIENT_ID) {
        console.error("Missing CLIENT_ID env var!");
        return {
          statusCode: 500,
          body: JSON.stringify({ error: "Server config error: no CLIENT_ID" })
        };
      }

      const scope = "listings_w listings_r transactions_r transactions_w";
      const state = "randomState123"; // or random

      // Build Etsy OAuth URL
      const oauthUrl =
        "https://www.etsy.com/oauth/connect" +
        `?response_type=code` +
        `&client_id=${CLIENT_ID}` +
        `&redirect_uri=${encodeURIComponent(finalRedirectUri)}` +
        `&scope=${encodeURIComponent(scope)}` +
        `&state=${state}` +
        `&code_challenge=${encodeURIComponent(codeChallenge)}` +
        `&code_challenge_method=S256`;

      console.log("Redirecting user to Etsy =>", oauthUrl);
      return {
        statusCode: 302,
        headers: { Location: oauthUrl },
        body: ""
      };
    }

    // 3) If we have code => we need codeVerifier => if missing => 400
    if (!codeVerifier) {
      console.error("Missing code_verifier => can't finalize token exchange!");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing code_verifier param" })
      };
    }

    // 4) Perform token exchange
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    if (!CLIENT_ID || !CLIENT_SECRET) {
      console.error("Missing CLIENT_ID or CLIENT_SECRET env vars!");
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Server config error: missing Etsy OAuth credentials"
        })
      };
    }

    const params = new URLSearchParams({
      grant_type:    "authorization_code",
      client_id:     CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code,
      redirect_uri:  finalRedirectUri,
      code_verifier: codeVerifier
    });
    console.log("Token exchange =>", params.toString());

    const resp = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params
    });
    const data = await resp.json();
    console.log("Token exchange response =>", data);

    if (!resp.ok) {
      console.error("Etsy token exchange error =>", data.error, data.error_description);
      return {
        statusCode: resp.status,
        body: JSON.stringify({
          error: data.error,
          error_description: data.error_description
        })
      };
    }

    // 5) success => redirect to domain with ?access_token=...
    const finalUrl = `${finalRedirectUri}?access_token=${encodeURIComponent(data.access_token)}`;
    console.log("Token exchange success => final redirect =>", finalUrl);

    return {
      statusCode: 302,
      headers: { Location: finalUrl },
      body: ""
    };

  } catch (err) {
    console.error("Error in exchangeToken =>", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};