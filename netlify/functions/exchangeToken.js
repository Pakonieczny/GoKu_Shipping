const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  try {
    // Retrieve query parameters
    const code = event.queryStringParameters.code;
    const codeVerifier = event.queryStringParameters.code_verifier;
    if (!code || !codeVerifier) {
      console.error("Missing code or code_verifier in query parameters.");
      return { statusCode: 400, body: JSON.stringify({ error: "Missing code or code_verifier" }) };
    }

    // Retrieve environment variables
    const CLIENT_ID = process.env.CLIENT_ID;
    const CLIENT_SECRET = process.env.CLIENT_SECRET;
    const REDIRECT_URI = process.env.REDIRECT_URI;
    if (!CLIENT_ID || !CLIENT_SECRET || !REDIRECT_URI) {
      console.error("Missing required environment variables.");
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
    console.log("Token exchange parameters:", params.toString());

    // Perform token exchange with Etsy
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
      console.error("Token exchange failed:", data.error, data.error_description);
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: data.error, error_description: data.error_description })
      };
    }

    // Redirect to index.html with the access_token as a query parameter.
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