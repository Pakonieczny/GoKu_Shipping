// netlify/functions/exchangeToken.js
// Handles Etsy OAuth → exchanges “code” for access_token, auto-routes by sub-domain.
//
// Domains we recognize --------------------------------------------------
const SORTING_DOMAIN           = "https://sorting.goldenspike.app";
const GOLDENSPIKE_DOMAIN       = "https://goldenspike.app";
const DESIGN_DOMAIN            = "https://design.goldenspike.app";
const DESIGNMESSAGE_DOMAIN     = "https://design-message.goldenspike.app";
const DESIGNMESSAGE1_DOMAIN    = "https://design-message-1.goldenspike.app";
const ASSEMBLY1_DOMAIN         = "https://assembly-1.goldenspike.app";
const ASSEMBLY2_DOMAIN         = "https://assembly-2.goldenspike.app";
const ASSEMBLY3_DOMAIN         = "https://assembly-3.goldenspike.app";
const ASSEMBLY4_DOMAIN         = "https://assembly-4.goldenspike.app";
const SHIPPING1_DOMAIN         = "https://shipping-1.goldenspike.app";
const SHIPPING2_DOMAIN         = "https://shipping-2.goldenspike.app";
const SHIPPING3_DOMAIN         = "https://shipping-3.goldenspike.app";
const WELD1_DOMAIN             = "https://weld-1.goldenspike.app";
const DESIGN1_DOMAIN           = "https://design-1.goldenspike.app";

// CORS (public JSON API) -----------------------------------------------
const CORS = {
  "Access-Control-Allow-Origin" : "*",
  "Access-Control-Allow-Headers": "Content-Type,Authorization",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
};

const fetch  = require("node-fetch");
const crypto = require("crypto");

// ───────────────────────────────────────────────────────────────────────
// Helpers
// ───────────────────────────────────────────────────────────────────────
function generateRandomString(len) {
  return crypto.randomBytes(Math.ceil(len / 2))
               .toString("hex")
               .slice(0, len);
}

function toBase64URL(buf) {
  return buf.toString("base64")
            .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function sha256(message) {
  return crypto.createHash("sha256").update(message).digest();
}

// Decide which domain to redirect back to ------------------------------
function pickDomainFromHost(event) {
  const host = (event.headers["x-forwarded-host"] || event.headers["host"] || "")
                 .toLowerCase();
  console.log("exchangeToken => Detected host:", host);

  const param = ((event.queryStringParameters || {}).redirect_domain || "").trim().toLowerCase();

  /* explicit ?redirect_domain= overrides */
  switch (param) {
    case "sorting":          return SORTING_DOMAIN;
    case "weld-1":           return WELD1_DOMAIN;
    case "design":           return DESIGN_DOMAIN;
    case "design-1":         return DESIGN1_DOMAIN;
    case "design-message":   return DESIGNMESSAGE_DOMAIN;
    case "design-message-1": return DESIGNMESSAGE1_DOMAIN;
    case "assembly-1":       return ASSEMBLY1_DOMAIN;
    case "assembly-2":       return ASSEMBLY2_DOMAIN;
    case "assembly-3":       return ASSEMBLY3_DOMAIN;
    case "assembly-4":       return ASSEMBLY4_DOMAIN;
    case "shipping-1":       return SHIPPING1_DOMAIN;
    case "shipping-2":       return SHIPPING2_DOMAIN;
    case "shipping-3":       return SHIPPING3_DOMAIN;
    default: /* fall through */
  }

  /* host-based routing */
  if (host.includes("sorting."))          return SORTING_DOMAIN;
  if (host.includes("weld-1."))           return WELD1_DOMAIN;
  if (host.includes("design-1."))         return DESIGN1_DOMAIN;
  if (host.includes("design-message-1.")) return DESIGNMESSAGE1_DOMAIN;
  if (host.includes("design-message."))   return DESIGNMESSAGE_DOMAIN;
  if (host.includes("design."))           return DESIGN_DOMAIN;
  if (host.includes("assembly-1."))       return ASSEMBLY1_DOMAIN;
  if (host.includes("assembly-2."))       return ASSEMBLY2_DOMAIN;
  if (host.includes("assembly-3."))       return ASSEMBLY3_DOMAIN;
  if (host.includes("assembly-4."))       return ASSEMBLY4_DOMAIN;
  if (host.includes("shipping-1."))       return SHIPPING1_DOMAIN;
  if (host.includes("shipping-2."))       return SHIPPING2_DOMAIN;
  if (host.includes("shipping-3."))       return SHIPPING3_DOMAIN;

  /* default */                           return GOLDENSPIKE_DOMAIN;
}

// ───────────────────────────────────────────────────────────────────────
// Netlify handler
// ───────────────────────────────────────────────────────────────────────
exports.handler = async (event) => {
  const method = event.httpMethod.toUpperCase();

  if (method === "OPTIONS") {
    return { statusCode: 204, headers: CORS, body: "" };
  }

  /* Step 1 — start OAuth ------------------------------------------------*/
  if (method === "GET" && !event.queryStringParameters.code) {
    const state        = generateRandomString(16);
    const codeVerifier = generateRandomString(64);
    const codeData     = await sha256(codeVerifier);
    const codeChallenge = toBase64URL(codeData);

    // cache verifier in cookie (30 min) so second-leg call can read it
    const cookie = `cv=${codeVerifier}; Path=/; Max-Age=1800; SameSite=Lax; Secure`;

    const EtsyAuthURL = new URL("https://www.etsy.com/oauth/connect");
    EtsyAuthURL.searchParams.set("response_type", "code");
    EtsyAuthURL.searchParams.set("redirect_uri", `${pickDomainFromHost(event)}/.netlify/functions/exchangeToken`);
    EtsyAuthURL.searchParams.set("client_id", process.env.CLIENT_ID);
    EtsyAuthURL.searchParams.set("state", state);
    EtsyAuthURL.searchParams.set("code_challenge", codeChallenge);
    EtsyAuthURL.searchParams.set("code_challenge_method", "S256");
    EtsyAuthURL.searchParams.set("scope", "transactions_r listings_r listings_w");

    return {
      statusCode: 302,
      headers: {
        ...CORS,
        "Set-Cookie": cookie,
        Location: EtsyAuthURL.toString()
      },
      body: ""
    };
  }

  /* Step 2 — exchange code for access_token ----------------------------*/
  if (method === "GET" && event.queryStringParameters.code) {
    const code         = event.queryStringParameters.code;
    const codeVerifier =
      (event.queryStringParameters.code_verifier || "") ||
      (event.headers.cookie || "").split(";").map(c => c.trim())
        .find(c => c.startsWith("cv="))?.slice(3) || "";

    if (!codeVerifier) {
      return {
        statusCode: 400,
        headers: CORS,
        body: JSON.stringify({ error: "Missing code_verifier param" })
      };
    }

    const resp = await fetch("https://api.etsy.com/v3/public/oauth/token", {
      method : "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body   : new URLSearchParams({
        grant_type   : "authorization_code",
        client_id    : process.env.CLIENT_ID,
        redirect_uri : `${pickDomainFromHost(event)}/.netlify/functions/exchangeToken`,
        code         : code,
        code_verifier: codeVerifier
      }).toString()
    });

    const data = await resp.json();
    if (!resp.ok) {
      console.error("Token exchange failed:", data);
      return { statusCode: 500, headers: CORS, body: JSON.stringify(data) };
    }

    // success → redirect back to app with token in hash
    return {
      statusCode: 302,
      headers: {
        ...CORS,
        Location: `${pickDomainFromHost(event)}/#access_token=${data.access_token}`
      },
      body: ""
    };
  }

  /* Unknown route ------------------------------------------------------*/
  return {
    statusCode: 404,
    headers: CORS,
    body: JSON.stringify({ error: "Route not found" })
  };
};