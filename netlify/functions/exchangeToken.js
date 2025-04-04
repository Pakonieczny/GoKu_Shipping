// netlify/functions/exchangeToken.js
// A complete Netlify serverless function for Etsy PKCE OAuth.

const fetch = require("node-fetch");
const crypto = require("crypto");

/*
  We only allow 2 possible redirect domains:
    1. sorting.goldenspike.app
    2. goldenspike.app

  If the query string has ?redirect_domain=goldenspike
  => final redirect: https://goldenspike.app
  If ?redirect_domain=sorting => final redirect: https://sorting.goldenspike.app
  If no param is passed => default to sorting.goldenspike.app
*/

const ALLOWED_REDIRECTS = {
  sorting:     "https://sorting.goldenspike.app",
  goldenspike: "https://goldenspike.app"
};

// Helper: generate a random string for the code_verifier
function generateRandomString(length) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++) {
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}

// Helper: generate code challenge from code verifier using SHA-256
function generateCodeChallenge(codeVerifier) {
  const hash = crypto.createHash("sha256").update(codeVerifier).digest();
  return hash.toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

exports.handler = async function(event, context) {
  try {
    // 1) Gather query params
    const queryParams = event.queryStringParameters || {};
    const code = queryParams.code;                // from Etsy
    const codeVerifier = queryParams.code_verifier; // from localStorage
    const chosenDomain = (queryParams.redirect_domain || "sorting").toLowerCase();
    
    // The final redirectUri we’ll use in the token exchange
    // and in the final redirect back to the browser
    const redirectUri = ALLOWED_REDIRECTS[chosenDomain] || ALLOWED_REDIRECTS.sorting;
    
    console.log("exchangeToken.js => Chosen domain:", chosenDomain);
    console.log("exchangeToken.js => Using redirectUri:", redirectUri);

    // If there's NO ?code => start the OAuth flow from scratch
    // (Not typical from a function, but we keep it for completeness.)
    if (!code) {
      console.log("No 'code' param => initiating new OAuth flow with PKCE...");
      
      // Generate code_verifier + code_challenge
      const newCodeVerifier = generateRandomString(64);
      const codeChallenge = generateCodeChallenge(newCodeVerifier);

      // You might set a cookie or session to store newCodeVerifier. 
      // For demonstration, just logging it (not recommended in production).
      console.log("Generated code_verifier:", newCodeVerifier);
      console.log("Generated code_challenge:", codeChallenge);

      const CLIENT_ID = process.env.CLIENT_ID;
      if (!CLIENT_ID) {
        console.error("Missing CLIENT_ID environment variable!");
        return {
          statusCode: 500,
          body: JSON.stringify({ error: "Server configuration error: missing CLIENT_ID" })
        };
      }

      const state = "randomState123"; // in production, generate a truly random state
      const scope = "listings_w listings_r transactions_r transactions_w"; // Adjust as needed

      // Build the Etsy OAuth URL
      const oauthUrl = 
        "https://www.etsy.com/oauth/connect?" +
        "response_type=code" +
        `&client_id=${CLIENT_ID}` +
        `&redirect_uri=${encodeURIComponent(redirectUri)}` +
        `&scope=${encodeURIComponent(scope)}` +
        `&state=${state}` +
        `&code_challenge=${encodeURIComponent(codeChallenge)}` +
        `&code_challenge_method=S256`;

      console.log("Redirecting to Etsy OAuth URL:", oauthUrl);
      // Return a 302 => browser will go to Etsy's OAuth page
      return {
        statusCode: 302,
        headers: { Location: oauthUrl },
        body: ""
      };
    }

    // If code is provided but no code_verifier => error
    if (!codeVerifier) {
      console.error("No code_verifier in the query string => can't complete token exchange.");
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Missing code_verifier parameter" })
      };
    }

    // 2) Token Exchange with Etsy
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    if (!CLIENT_ID || !CLIENT_SECRET) {
      console.error("Missing CLIENT_ID or CLIENT_SECRET environment variables!");
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Server config error: missing Etsy OAuth credentials" })
      };
    }

    // Prepare the POST body
    const params = new URLSearchParams({
      grant_type:     "authorization_code",
      client_id:      CLIENT_ID,
      client_secret:  CLIENT_SECRET,
      code:           code,
      redirect_uri:   redirectUri,
      code_verifier:  codeVerifier
    });
    console.log("Token exchange params:", params.toString());

    const tokenResponse = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: params
    });
    const tokenData = await tokenResponse.json();
    console.log("Token exchange response:", tokenData);

    if (!tokenResponse.ok) {
      // If Etsy returns an error (e.g. 400), we log & pass it back
      console.error("Etsy token exchange failed:", tokenData.error, tokenData.error_description);
      return {
        statusCode: tokenResponse.status,
        body: JSON.stringify({
          error: tokenData.error,
          error_description: tokenData.error_description
        })
      };
    }

    // 3) If successful, we get an access_token => redirect user to the correct domain
    // with ?access_token=someToken
    const finalLocation = `${redirectUri}?access_token=${encodeURIComponent(tokenData.access_token)}`;
    console.log("Token exchange success => redirecting to:", finalLocation);

    return {
      statusCode: 302,
      headers: { Location: finalLocation },
      body: ""
    };

  } catch (err) {
    // Catch any unexpected error
    console.error("Error in exchangeToken:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};