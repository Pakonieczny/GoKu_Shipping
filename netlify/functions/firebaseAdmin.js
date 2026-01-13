// firebaseAdmin.js (UPDATED - minimal required env vars)
// Fixes: missing FIREBASE_CLIENT_X509_CERT_URL crashes
// Requires ONLY:
//   - FIREBASE_PROJECT_ID
//   - FIREBASE_CLIENT_EMAIL
//   - FIREBASE_PRIVATE_KEY  (with \n newlines escaped in Netlify)
// Recommended:
//   - FIREBASE_STORAGE_BUCKET (bucket NAME, e.g. gokudatabase.appspot.com)

const admin = require("firebase-admin");

function requireEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var: ${name}`);
  return v;
}

function normalizePrivateKey(k) {
  return String(k).replace(/\\n/g, "\n");
}

function getBucketName() {
  const v = process.env.FIREBASE_STORAGE_BUCKET;

  // If user didn't set it, default to the common Firebase bucket format
  const fallback = "gokudatabase.appspot.com";

  const bucket = (v || fallback).trim();

  // Guard: firebasestorage.app is NOT a bucket name for admin/storage().bucket()
  if (bucket.endsWith(".firebasestorage.app")) {
    throw new Error(
      `Invalid FIREBASE_STORAGE_BUCKET "${bucket}". ` +
      `Use the bucket NAME shown in Firebase Storage as gs://<bucket> (typically "<project>.appspot.com").`
    );
  }

  return bucket;
}

// Build a minimal service account object.
// (Admin SDK + google-cloud/storage only need project_id, client_email, private_key)
const serviceAccount = {
  project_id: requireEnv("FIREBASE_PROJECT_ID"),
  client_email: requireEnv("FIREBASE_CLIENT_EMAIL"),
  private_key: normalizePrivateKey(requireEnv("FIREBASE_PRIVATE_KEY")),
};

const STORAGE_BUCKET = getBucketName();

// Initialize Admin SDK once
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    storageBucket: STORAGE_BUCKET, // ✅ critical for admin.storage().bucket()
  });
}

/* ─── ensure CORS rule (runs once per cold-start) ───────── */
if (!process.env.CORS_SET) {
  const { Storage } = require("@google-cloud/storage");

  const ALLOWED_ORIGINS = [
    "https://shipping-1.goldenspike.app",
    "https://design-message.goldenspike.app",
    "https://design-message-1.goldenspike.app",
    "https://listing-generator-1.goldenspike.app",
  ];

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
    .then(() => console.log("CORS confirmed for bucket:", STORAGE_BUCKET))
    .catch((err) => console.error("CORS error:", err));

  process.env.CORS_SET = "1";
}

module.exports = admin;