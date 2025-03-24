const fetch = require("node-fetch");
const crypto = require("crypto");

// Helper: generate a random string of specified length.
function generateRandomString(length) {
  const possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < length; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}

// Helper: generate code challenge from the code verifier using SHA-256.
function generateCodeChallenge(codeVerifier) {
  const hash = crypto.createHash("sha256").update(codeVerifier).digest();
  return hash.toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

exports.handler = async function(event, context) {
  try {
    // Retrieve query parameters.
    const code = event.queryStringParameters && event.queryStringParameters.code;
    const codeVerifier = event.queryStringParameters && event.queryStringParameters.code_verifier;

    // If no code is provided, initiate the OAuth flow.
    if (!code) {
      console.log("No code parameter found – initiating OAuth redirect.");

      // Generate a new code verifier and corresponding code challenge.
      const newCodeVerifier = generateRandomString(64);
      const codeChallenge = generateCodeChallenge(newCodeVerifier);
      
      // In a production environment, store the newCodeVerifier securely (e.g., in a secure cookie)
      // so it can be used later in the token exchange.
      
      // Retrieve environment variables for OAuth.
      const CLIENT_ID = process.env.CLIENT_ID;
      const CLIENT_SECRET = process.env.CLIENT_SECRET;  // Not used in the redirect but needed for exchange.
      const REDIRECT_URI = process.env.REDIRECT_URI;
      
      if (!CLIENT_ID || !REDIRECT_URI) {
        console.error("Missing required environment variables (CLIENT_ID or REDIRECT_URI).");
        return { statusCode: 500, body: JSON.stringify({ error: "Server configuration error" }) };
      }
      
      // Define additional parameters.
      const state = "randomState123"; // In production, generate a secure random state.
      const scope = "listings_w listings_r"; // Adjust scope as needed.
      
      // Construct the Etsy OAuth URL.
      const oauthUrl = `https://www.etsy.com/oauth/connect?response_type=code&client_id=${CLIENT_ID}` +
                       `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
                       `&scope=${encodeURIComponent(scope)}` +
                       `&state=${state}` +
                       `&code_challenge=${encodeURIComponent(codeChallenge)}` +
                       `&code_challenge_method=S256`;
      
      console.log("Redirecting to Etsy OAuth URL:", oauthUrl);
      return {
        statusCode: 302,
        headers: {
          Location: oauthUrl
        },
        body: ""
      };
    }

    // If code is provided but code_verifier is missing, return an error.
    if (!codeVerifier) {
      console.error("Missing 'code_verifier' parameter in query string.");
      return { statusCode: 400, body: JSON.stringify({ error: "Missing code_verifier parameter" }) };
    }
    
    // Retrieve environment variables.
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    const REDIRECT_URI = process.env.REDIRECT_URI;
    
    if (!CLIENT_ID || !CLIENT_SECRET || !REDIRECT_URI) {
      console.error("Missing required environment variables (CLIENT_ID, CLIENT_SECRET, REDIRECT_URI).");
      return { statusCode: 500, body: JSON.stringify({ error: "Server configuration error" }) };
    }
    
    // Build request parameters for token exchange.
    const params = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code: code,
      redirect_uri: REDIRECT_URI,
      code_verifier: codeVerifier
    });
    
    console.log("Token exchange parameters:", params.toString());
    
    // Perform the token exchange with Etsy.
    const response = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: params
    });
    
    const data = await response.json();
    console.log("Token exchange response:", data);
    
    if (!response.ok) {
      console.error("Token exchange failed with status", response.status);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: data.error, error_description: data.error_description })
      };
    }
    
    // On successful token exchange, redirect back to your index page with the access token in the query.
    return {
      statusCode: 302,
      headers: {
        Location: "/?access_token=" + data.access_token
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