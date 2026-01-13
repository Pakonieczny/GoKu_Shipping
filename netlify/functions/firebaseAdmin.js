// firebaseAdmin.js (UPDATED for Listing-Generator-1 CORS)
// - Adds https://listing-generator-1.goldenspike.app to Storage bucket CORS
// - Keeps your existing init + “set once per cold start” behavior

const admin = require("firebase-admin");

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

const serviceAccount = {
  type: "service_account",
  project_id: requireEnv("FIREBASE_PROJECT_ID"),
  private_key_id: requireEnv("FIREBASE_PRIVATE_KEY_ID"),
  private_key: requireEnv("FIREBASE_PRIVATE_KEY").replace(/\\n/g, "\n"),
  client_email: requireEnv("FIREBASE_CLIENT_EMAIL"),
  client_id: requireEnv("FIREBASE_CLIENT_ID"),
  auth_uri: requireEnv("FIREBASE_AUTH_URI"),
  token_uri: requireEnv("FIREBASE_TOKEN_URI"),
  auth_provider_x509_cert_url: requireEnv("FIREBASE_AUTH_PROVIDER_X509_CERT_URL"),
  client_x509_cert_url: requireEnv("FIREBASE_CLIENT_X509_CERT_URL"),
  universe_domain: process.env.FIREBASE_UNIVERSE_DOMAIN,
};

const STORAGE_BUCKET = process.env.FIREBASE_STORAGE_BUCKET || "gokudatabase.firebasestorage.app";

const ALLOWED_ORIGINS = [
  "https://shipping-1.goldenspike.app",
  "https://design-message.goldenspike.app",
  "https://design-message-1.goldenspike.app",
  "https://listing-generator-1.goldenspike.app",
];

// Initialize Admin SDK once
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    storageBucket: STORAGE_BUCKET,
  });
}

/* ─── ensure CORS rule (runs once per cold-start) ───────── */
if (!process.env.CORS_SET) {
  const { Storage } = require("@google-cloud/storage");

  new Storage({ credentials: serviceAccount })
    .bucket(STORAGE_BUCKET)
    .setCorsConfiguration([
      {
        origin: ALLOWED_ORIGINS,
        method: ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"],
        responseHeader: ["Content-Type", "Authorization"],
        maxAgeSeconds: 3600,
      },
    ])
    .then(() => console.log("CORS confirmed"))
    .catch((err) => console.error("CORS error:", err));

  process.env.CORS_SET = "1"; // prevent repeats on warm invokes
}

module.exports = admin;