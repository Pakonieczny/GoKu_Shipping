// netlify/functions/exchangeToken.js

const fetch = require("node-fetch");
const crypto = require("crypto");

// We only allow EXACTLY these two domains
const ALLOWED_REDIRECT_URIS = {
  sorting:     "https://sorting.goldenspike.app",
  goldenspike: "https://goldenspike.app"
};

function generateRandomString(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++){
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

exports.handler = async function(event) {
  try {
    const query = event.queryStringParameters || {};
    const code = query.code;
    const codeVerifier = query.code_verifier;

    // 1) Must specify ?redirect_domain=sorting or =goldenspike
    const domainParam = query.redirect_domain;
    if (!domainParam) {
      console.error("No redirect_domain => can't pick the domain");
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing ?redirect_domain= sorting or goldenspike"
        })
      };
    }
    if (!ALLOWED_REDIRECT_URIS[domainParam]) {
      console.error(`Invalid redirect_domain => ${domainParam}`);
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: `Invalid redirect_domain='${domainParam}'. Must be 'sorting' or 'goldenspike'`
        })
      };
    }
    const finalRedirectUri = ALLOWED_REDIRECT_URIS[domainParam];
    console.log("exchangeToken => domainParam:", domainParam);
    console.log("exchangeToken => using redirectUri:", finalRedirectUri);

    // 2) If no ?code => start PKCE
    if (!code) {
      console.log("No code => starting new OAuth flow...");

      const newCodeVerifier = generateRandomString(64);
      const codeChallenge   = generateCodeChallenge(newCodeVerifier);

      // for demonstration
      console.log("PKCE code_verifier:", newCodeVerifier);
      console.log("PKCE code_challenge:", codeChallenge);

      const CLIENT_ID = process.env.CLIENT_ID;
      if (!CLIENT_ID) {
        console.error("Missing CLIENT_ID env var");
        return {
          statusCode: 500,
          body: JSON.stringify({ error: "Server config error: no CLIENT_ID" })
        };
      }

      const scope = "listings_w listings_r transactions_r transactions_w";
      const state = "randomState123";

      const oauthUrl =
        `https://www.etsy.com/oauth/connect` +
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

    // 3) If code but no codeVerifier => fail
    if (!codeVerifier) {
      console.error("No code_verifier => can't finalize token");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing code_verifier param" })
      };
    }

    // 4) Attempt token exchange with Etsy
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    if (!CLIENT_ID || !CLIENT_SECRET) {
      console.error("Missing CLIENT_ID or CLIENT_SECRET env vars");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Server config error: missing credentials" })
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

    // success => redirect to domain with ?access_token=...
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