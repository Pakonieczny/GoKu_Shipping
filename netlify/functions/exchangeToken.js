// netlify/functions/exchangeToken.js
const fetch = require("node-fetch");
const crypto = require("crypto");

/*
  We allow three possible redirect URIs:
  1) https://sorting.goldenspike.app
  2) https://scanner.goldenspike.app
  3) https://goldenspike.app

  The user picks via a ?redirect_domain=sorting|scanner|goldenspike
  If absent or invalid, we default to the "sorting" domain.
*/
const ALLOWED_REDIRECTS = {
  sorting:     "https://sorting.goldenspike.app",
  scanner:     "https://scanner.goldenspike.app",
  goldenspike: "https://goldenspike.app"
};

// Helpers
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

// Main handler
exports.handler = async function(event, context) {
  try {
    // 1) Parse query params
    const queryParams = event.queryStringParameters || {};
    const code = queryParams.code;
    const codeVerifier = queryParams.code_verifier;
    // The user can specify "sorting", "scanner", or "goldenspike"
    // If none is provided, we default to "sorting"
    const requestedDomain = (queryParams.redirect_domain || "sorting").toLowerCase();
    const redirectUri = ALLOWED_REDIRECTS[requestedDomain] || ALLOWED_REDIRECTS.sorting;

    // If no code => initiate the OAuth flow 
    // (rarely used from a function, but we keep it for completeness)
    if (!code) {
      console.log("No code parameter found – initiating OAuth redirect.");

      const newCodeVerifier = generateRandomString(64);
      const codeChallenge = generateCodeChallenge(newCodeVerifier);

      // In production, store newCodeVerifier securely (e.g. via a cookie)
      const CLIENT_ID = process.env.CLIENT_ID;
      if (!CLIENT_ID || !redirectUri) {
        console.error("Missing CLIENT_ID or redirectUri for OAuth start.");
        return { statusCode: 500, body: JSON.stringify({ error: "Server config error." }) };
      }
      const state = "randomState123";
      const scope = "listings_w listings_r";

      const oauthUrl = `https://www.etsy.com/oauth/connect?response_type=code&client_id=${CLIENT_ID}` +
                       `&redirect_uri=${encodeURIComponent(redirectUri)}` +
                       `&scope=${encodeURIComponent(scope)}` +
                       `&state=${state}` +
                       `&code_challenge=${encodeURIComponent(codeChallenge)}` +
                       `&code_challenge_method=S256`;

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

    // 2) Use environment variables for the final exchange
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    if (!CLIENT_ID || !CLIENT_SECRET) {
      console.error("Missing CLIENT_ID or CLIENT_SECRET env variables.");
      return { statusCode: 500, body: JSON.stringify({ error: "Server config error" }) };
    }

    // Build Etsy’s token exchange POST
    const params = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code: code,
      redirect_uri: redirectUri,
      code_verifier: codeVerifier
    });
    console.log("Token exchange parameters:", params.toString());

    // 3) POST to Etsy's token endpoint
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

    // 4) Redirect to the specified domain with the access token
    // e.g. https://sorting.goldenspike.app?access_token=xxx
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