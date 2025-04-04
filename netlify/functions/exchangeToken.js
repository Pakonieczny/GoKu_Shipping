// netlify/functions/exchangeToken.js
const fetch = require("node-fetch");
const crypto = require("crypto");

// Exactly these two, no fallback to anything else:
const ALLOWED_DOMAINS = {
  sorting: "https://sorting.goldenspike.app",
  goldenspike: "https://goldenspike.app"
};

// Generate code_verifier
function generateRandomString(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++) {
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}

// Generate code_challenge from code_verifier
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

    // Must specify ?redirect_domain=sorting or =goldenspike
    const domainParam = query.redirect_domain;
    if (!domainParam) {
      // If user didn’t specify ANY domain => fail clearly
      console.error("No redirect_domain param => can't decide which domain to use");
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing ?redirect_domain= sorting or goldenspike"
        })
      };
    }
    if (!ALLOWED_DOMAINS[domainParam]) {
      // If user specified something else => fail
      console.error("Invalid redirect_domain =>", domainParam);
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Invalid redirect_domain param. Must be 'sorting' or 'goldenspike'."
        })
      };
    }

    // So we’re using exactly the domain they requested
    const redirectUri = ALLOWED_DOMAINS[domainParam];
    console.log("exchangeToken => domainParam:", domainParam);
    console.log("exchangeToken => chosen redirectUri:", redirectUri);

    // If no code => begin OAuth with PKCE
    if (!code) {
      console.log("No ?code => starting new OAuth flow with PKCE...");

      const newCodeVerifier = generateRandomString(64);
      const codeChallenge   = generateCodeChallenge(newCodeVerifier);

      console.log("Generated code_verifier:", newCodeVerifier);
      console.log("Generated code_challenge:", codeChallenge);

      const CLIENT_ID = process.env.CLIENT_ID;
      if (!CLIENT_ID) {
        console.error("Missing CLIENT_ID env var");
        return {
          statusCode: 500,
          body: JSON.stringify({
            error: "Server config error: missing CLIENT_ID"
          })
        };
      }

      const scope = "listings_w listings_r transactions_r transactions_w";
      const state = "randomState123"; // in production, randomize
      const oauthUrl =
        `https://www.etsy.com/oauth/connect?response_type=code` +
        `&client_id=${CLIENT_ID}` +
        `&redirect_uri=${encodeURIComponent(redirectUri)}` +
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

    // If code is present but no code_verifier => can’t finish
    if (!codeVerifier) {
      console.error("Missing code_verifier => can’t do token exchange");
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing code_verifier parameter"
        })
      };
    }

    // We have code & code_verifier => final token exchange
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

    const params = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code,
      redirect_uri: redirectUri,
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

    // success => redirect to chosen domain with ?access_token=...
    const finalUrl = `${redirectUri}?access_token=${encodeURIComponent(data.access_token)}`;
    console.log("Token exchange success => redirecting =>", finalUrl);

    return {
      statusCode: 302,
      headers: { Location: finalUrl },
      body: ""
    };

  } catch (error) {
    console.error("Error in exchangeToken =>", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};