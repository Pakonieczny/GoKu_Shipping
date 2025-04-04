// netlify/functions/exchangeToken.js

const fetch = require("node-fetch");
const crypto = require("crypto");

/*
  We support exactly two domains:
    1) https://sorting.goldenspike.app
    2) https://goldenspike.app

  - If ?redirect_domain=sorting => domain = sorting.goldenspike.app
  - If ?redirect_domain=goldenspike => domain = goldenspike.app
  - If missing or empty => default to "sorting".
  - If something else => return 400.
*/

const ALLOWED_REDIRECT_URIS = {
  sorting:     "https://sorting.goldenspike.app",
  goldenspike: "https://goldenspike.app"
};

// Helper: Generate PKCE code_verifier
function generateRandomString(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++){
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}

// Helper: Convert code_verifier => code_challenge
function generateCodeChallenge(codeVerifier) {
  const hash = crypto.createHash("sha256").update(codeVerifier).digest();
  return hash.toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

exports.handler = async function(event, context) {
  try {
    // 1) Parse query params
    const query = event.queryStringParameters || {};
    const code = query.code;                 // from Etsy
    const codeVerifier = query.code_verifier; // from localStorage, if used

    // domainParam might be "sorting", "goldenspike", or missing
    const domainParam = (query.redirect_domain || "sorting").toLowerCase();
    console.log("exchangeToken => raw domainParam:", query.redirect_domain, " => final:", domainParam);

    // If it's not in our allowed set => 400
    if (!ALLOWED_REDIRECT_URIS[domainParam]) {
      console.error("Invalid domainParam =>", domainParam);
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: `Invalid redirect_domain="${domainParam}". Must be "sorting" or "goldenspike".`
        })
      };
    }

    // Choose the final domain
    const finalRedirectUri = ALLOWED_REDIRECT_URIS[domainParam];
    console.log("exchangeToken => using redirectUri:", finalRedirectUri);

    // 2) If there's no code => start OAuth with PKCE
    if (!code) {
      console.log("No ?code => starting new OAuth flow w/ PKCE...");

      const newCodeVerifier = generateRandomString(64);
      const codeChallenge   = generateCodeChallenge(newCodeVerifier);

      // Typically, store newCodeVerifier in a cookie/session:
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
      const state = "randomState123"; // in production => randomize

      // Build the Etsy OAuth URL
      const oauthUrl =
        `https://www.etsy.com/oauth/connect?response_type=code` +
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

    // 3) If code but no codeVerifier => can't finish
    if (!codeVerifier) {
      console.error("No code_verifier => can't complete token exchange");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing code_verifier parameter" })
      };
    }

    // 4) Exchange code for an Etsy access_token
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    if (!CLIENT_ID || !CLIENT_SECRET) {
      console.error("Missing CLIENT_ID or CLIENT_SECRET!");
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

    // 5) On success => redirect back w/ ?access_token=...
    const finalUrl = `${finalRedirectUri}?access_token=${encodeURIComponent(data.access_token)}`;
    console.log("Token exchange success => redirect =>", finalUrl);

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