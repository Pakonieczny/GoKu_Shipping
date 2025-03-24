const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Log the incoming query parameters (avoid logging sensitive values in production)
    console.log("Received query parameters:", event.queryStringParameters);

    // Retrieve query parameters passed via event.queryStringParameters
    const code = event.queryStringParameters.code;
    const codeVerifier = event.queryStringParameters.code_verifier;

    if (!code) {
      console.error("Missing 'code' parameter in query string.");
      return { statusCode: 400, body: JSON.stringify({ error: "Missing code parameter" }) };
    }
    if (!codeVerifier) {
      console.error("Missing 'code_verifier' parameter in query string.");
      return { statusCode: 400, body: JSON.stringify({ error: "Missing code_verifier parameter" }) };
    }

    // Retrieve environment variables for OAuth
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    const REDIRECT_URI = process.env.REDIRECT_URI;

    if (!CLIENT_ID || !CLIENT_SECRET || !REDIRECT_URI) {
      console.error("Missing required environment variables (CLIENT_ID, CLIENT_SECRET, REDIRECT_URI).");
      return { statusCode: 500, body: JSON.stringify({ error: "Server configuration error" }) };
    }

    // Build request parameters for token exchange
    const params = new URLSearchParams({
      grant_type: "authorization_code",
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code: code,
      redirect_uri: REDIRECT_URI,
      code_verifier: codeVerifier
    });

    console.log("Request parameters for token exchange prepared.");

    // Perform the token exchange with Etsy
    const response = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: params
    });

    const data = await response.json();
    console.log("Response from Etsy OAuth token exchange:", data);

    if (!response.ok) {
      console.error("Etsy token exchange failed with status", response.status);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: data.error, error_description: data.error_description })
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };

  } catch (error) {
    console.error("Error in exchangeToken function:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};