// netlify/functions/exchangeToken.js
const fetch = require("node-fetch");
const crypto = require("crypto");

// We only allow two possible redirects now
const ALLOWED_REDIRECTS = {
  sorting:     "https://sorting.goldenspike.app",
  goldenspike: "https://goldenspike.app"
};

function generateRandomString(length) {
  const possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
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

exports.handler = async function(event, context) {
  try {
    const queryParams = event.queryStringParameters || {};
    const code = queryParams.code;
    const codeVerifier = queryParams.code_verifier;

    // Choose which domain to redirect to at the end
    // If none specified, we default to "sorting".
    const requestedDomain = (queryParams.redirect_domain || "sorting").toLowerCase();
    const redirectUri = ALLOWED_REDIRECTS[requestedDomain] || ALLOWED_REDIRECTS.sorting;

    // If no code => initiate the OAuth flow
    if (!code) {
      console.log("No code parameter found – initiating OAuth redirect.");
      const codeVerif = generateRandomString(64);
      const codeChall = generateCodeChallenge(codeVerif);

      // In production, store codeVerif securely (e.g., a cookie)
      const CLIENT_ID = process.env.CLIENT_ID;
      if (!CLIENT_ID || !redirectUri) {
        console.error("Missing CLIENT_ID or redirectUri for OAuth start.");
        return { statusCode: 500, body: JSON.stringify({ error: "Server config error." }) };
      }
      const state = "randomState123";
      const scope = "listings_w listings_r";

      const oauthUrl = `https://www.etsy.com/oauth/connect`
        + `?response_type=code`
        + `&client_id=${CLIENT_ID}`
        + `&redirect_uri=${encodeURIComponent(redirectUri)}`
        + `&scope=${encodeURIComponent(scope)}`
        + `&state=${state}`
        + `&code_challenge=${encodeURIComponent(codeChall)}`
        + `&code_challenge_method=S256`;

      console.log("Redirecting to Etsy OAuth URL:", oauthUrl);
      return {
        statusCode: 302,
        headers: { Location: oauthUrl },
        body: ""
      };
    }

    // If code is provided but no code_verifier => error
    if (!codeVerifier) {
      console.error("Missing 'code_verifier' parameter in query string.");
      return { statusCode: 400, body: JSON.stringify({ error: "Missing code_verifier parameter" }) };
    }

    // Pull environment variables
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    if (!CLIENT_ID || !CLIENT_SECRET) {
      console.error("Missing CLIENT_ID or CLIENT_SECRET env variables.");
      return { statusCode: 500, body: JSON.stringify({ error: "Server config error" }) };
    }

    // Token exchange parameters
    const params = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code: code,
      redirect_uri: redirectUri,
      code_verifier: codeVerifier
    });
    console.log("Token exchange parameters:", params.toString());

    // POST to Etsy's token endpoint
    const response = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params
    });
    const data = await response.json();
    console.log("Token exchange response:", data);

    if (!response.ok) {
      console.error("Token exchange failed:", data.error, data.error_description);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: data.error, error_description: data.error_description })
      };
    }

    // Finally, redirect to the chosen domain with ?access_token=...
    return {
      statusCode: 302,
      headers: {
        Location: `${redirectUri}?access_token=${encodeURIComponent(data.access_token)}`
      },
      body: ""
    };

  } catch (error) {
    console.error("Error in exchangeToken function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};