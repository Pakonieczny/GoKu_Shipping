// netlify/functions/exchangeToken.js

const fetch = require("node-fetch");
const crypto = require("crypto");

// We only allow EXACTLY these two domains:
const DOMAINS = {
  sorting:     "https://sorting.goldenspike.app",
  goldenspike: "https://goldenspike.app"
};

// Helper: code_verifier
function generateRandomString(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++){
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}

// Helper: code_challenge
function generateCodeChallenge(codeVerifier) {
  const hash = crypto.createHash("sha256").update(codeVerifier).digest();
  return hash.toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

exports.handler = async (event, context) => {
  try {
    // 1) Parse query params
    const query = event.queryStringParameters || {};
    const code = query.code;
    const codeVerifier = query.code_verifier;
    const domainParam = query.redirect_domain; // must be "sorting" or "goldenspike"

    // 2) If domainParam missing or invalid => 400
    if (!domainParam) {
      console.error("No redirect_domain param provided => can't pick a redirect URI!");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing ?redirect_domain= sorting or goldenspike" })
      };
    }
    if (!DOMAINS[domainParam]) {
      console.error("Invalid redirect_domain param =>", domainParam);
      return {
        statusCode: 400,
        body: JSON.stringify({ error: `Invalid redirect_domain param: ${domainParam}` })
      };
    }
    // So we definitely pick the correct domain with NO fallback
    const redirectUri = DOMAINS[domainParam];
    console.log("exchangeToken.js => domainParam:", domainParam);
    console.log("exchangeToken.js => using redirectUri:", redirectUri);

    // 3) If no code => start the OAuth flow with PKCE
    if (!code) {
      console.log("No ?code => initiating new OAuth flow with PKCE...");
      const newCodeVerifier = generateRandomString(64);
      const codeChallenge   = generateCodeChallenge(newCodeVerifier);

      // For real usage, store newCodeVerifier in a secure cookie or session
      console.log("Generated code_verifier:", newCodeVerifier);
      console.log("Generated code_challenge:", codeChallenge);

      const CLIENT_ID = process.env.CLIENT_ID;
      if (!CLIENT_ID) {
        console.error("Missing CLIENT_ID env variable!");
        return {
          statusCode: 500,
          body: JSON.stringify({ error: "Server config error: missing CLIENT_ID" })
        };
      }

      const scope = "listings_w listings_r transactions_r transactions_w";
      const state = "randomState123"; // in production, use a truly random state

      // Build Etsy OAuth URL
      const oauthUrl = 
        "https://www.etsy.com/oauth/connect" +
        "?response_type=code" +
        `&client_id=${CLIENT_ID}` +
        `&redirect_uri=${encodeURIComponent(redirectUri)}` +
        `&scope=${encodeURIComponent(scope)}` +
        `&state=${state}` +
        `&code_challenge=${encodeURIComponent(codeChallenge)}` +
        "&code_challenge_method=S256";

      console.log("Redirecting user to Etsy:", oauthUrl);
      return {
        statusCode: 302,
        headers: { Location: oauthUrl },
        body: ""
      };
    }

    // 4) If we have ?code => must also have code_verifier => if missing => 400
    if (!codeVerifier) {
      console.error("No code_verifier => can't finish token exchange");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing code_verifier parameter" })
      };
    }

    // 5) Token exchange with Etsy
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    if (!CLIENT_ID || !CLIENT_SECRET) {
      console.error("Missing CLIENT_ID or CLIENT_SECRET env variables!");
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Server config error: missing Etsy OAuth credentials"
        })
      };
    }

    // Prepare POST body
    const params = new URLSearchParams({
      grant_type:    "authorization_code",
      client_id:     CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code:          code,
      redirect_uri:  redirectUri,
      code_verifier: codeVerifier
    });
    console.log("Token exchange params =>", params.toString());

    const response = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params
    });
    const data = await response.json();
    console.log("Etsy token exchange response =>", data);

    if (!response.ok) {
      console.error("Etsy token exchange failed =>", data.error, data.error_description);
      return {
        statusCode: response.status,
        body: JSON.stringify({
          error: data.error,
          error_description: data.error_description
        })
      };
    }

    // 6) On success => redirect to the domain with ?access_token=...
    const finalUrl = `${redirectUri}?access_token=${encodeURIComponent(data.access_token)}`;
    console.log("Token exchange success => redirecting to:", finalUrl);

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