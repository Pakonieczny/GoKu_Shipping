// netlify/functions/exchangeToken.js
// Final version supporting exactly these 2 domains:
//   1) https://sorting.goldenspike.app
//   2) https://goldenspike.app
//
// If user calls it without ?redirect_domain=... => we throw an error (400).
// If user calls ?redirect_domain=somethingElse => we throw an error (400).

const fetch = require("node-fetch");
const crypto = require("crypto");

// 1) We define only the two allowed domains:
const ALLOWED_REDIRECT_URIS = {
  sorting:     "https://sorting.goldenspike.app",
  goldenspike: "https://goldenspike.app"
};

// 2) Helper: generate a random code_verifier for PKCE
function generateRandomString(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++){
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}

// 3) Helper: generate code_challenge from code_verifier
function generateCodeChallenge(codeVerifier) {
  const hash = crypto.createHash("sha256").update(codeVerifier).digest();
  return hash.toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

exports.handler = async function(event, context) {
  try {
    // 4) Read query params
    const query = event.queryStringParameters || {};
    const code = query.code;               // present only after Etsy returns
    const codeVerifier = query.code_verifier; // from localStorage, if you do that

    // This param must be either "sorting" or "goldenspike"
    const domainParam = query.redirect_domain;
    if (!domainParam) {
      console.error("No redirect_domain param => cannot pick which domain to use");
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing ?redirect_domain= (must be 'sorting' or 'goldenspike')"
        })
      };
    }
    if (!ALLOWED_REDIRECT_URIS[domainParam]) {
      console.error("Invalid domainParam =>", domainParam);
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: `Invalid redirect_domain: ${domainParam} (must be 'sorting' or 'goldenspike')`
        })
      };
    }

    // So we definitely pick the correct domain with no fallback
    const finalRedirectUri = ALLOWED_REDIRECT_URIS[domainParam];
    console.log(`exchangeToken => domainParam: ${domainParam}`);
    console.log(`exchangeToken => finalRedirectUri: ${finalRedirectUri}`);

    // 5) If no code => start PKCE-based OAuth with Etsy
    if (!code) {
      console.log("No ?code => beginning new OAuth request with PKCE...");

      const newCodeVerifier = generateRandomString(64);
      const codeChallenge   = generateCodeChallenge(newCodeVerifier);

      // Typically, you'd store newCodeVerifier in a cookie or localStorage.
      // For demonstration, we log it here:
      console.log("PKCE code_verifier:", newCodeVerifier);
      console.log("PKCE code_challenge:", codeChallenge);

      const CLIENT_ID = process.env.CLIENT_ID;
      if (!CLIENT_ID) {
        console.error("Missing CLIENT_ID in environment variables!");
        return {
          statusCode: 500,
          body: JSON.stringify({ error: "Server config error: no CLIENT_ID" })
        };
      }

      // The scopes you need for your Etsy app
      const scope = "listings_w listings_r transactions_r transactions_w";
      const state = "randomState123"; // in production, use a real random value

      // Build the Etsy OAuth connect URL
      const oauthUrl = 
        `https://www.etsy.com/oauth/connect` +
        `?response_type=code` +
        `&client_id=${CLIENT_ID}` +
        `&redirect_uri=${encodeURIComponent(finalRedirectUri)}` +
        `&scope=${encodeURIComponent(scope)}` +
        `&state=${state}` +
        `&code_challenge=${encodeURIComponent(codeChallenge)}` +
        `&code_challenge_method=S256`;

      console.log("Redirecting to Etsy =>", oauthUrl);
      return {
        statusCode: 302,
        headers: { Location: oauthUrl },
        body: ""
      };
    }

    // 6) If we do have ?code=..., we need code_verifier => if missing => error
    if (!codeVerifier) {
      console.error("Missing code_verifier => cannot finalize token exchange!");
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing code_verifier param => can't complete token exchange"
        })
      };
    }

    // 7) Exchange the code for an Etsy access token
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

    // Build the POST body
    const params = new URLSearchParams({
      grant_type:    "authorization_code",
      client_id:     CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code:          code,
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
    console.log("Etsy token exchange response =>", data);

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

    // 8) On success => redirect user to domain with ?access_token=...
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