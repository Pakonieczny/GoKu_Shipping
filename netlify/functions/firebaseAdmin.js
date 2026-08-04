const admin = require("firebase-admin");

// Only the three fields below are read by admin.credential.cert() AND by
// new Storage({ credentials }) (the CORS block). `type` is a hardcoded literal
// (no env cost) so the auth library routes this as a service-account JWT.
// The previous private_key_id / client_id / auth_uri / token_uri /
// auth_provider_x509_cert_url / client_x509_cert_url / universe_domain fields
// were unused by both consumers and were removed to shrink the env footprint.
// A missing FIREBASE_PRIVATE_KEY used to throw a bare
// "Cannot read properties of undefined (reading 'replace')" at module load,
// which surfaced as an opaque 500 in whichever of the ~60 consumers happened
// to cold-start first. Same failure, but now it names itself.
if (!process.env.FIREBASE_PRIVATE_KEY) {
  throw new Error("firebaseAdmin: FIREBASE_PRIVATE_KEY is not set in this environment");
}

const serviceAccount = {
  type: "service_account",
  project_id: process.env.FIREBASE_PROJECT_ID,
  private_key: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n"),
  client_email: process.env.FIREBASE_CLIENT_EMAIL
};

function normalizeBucketName(value) {
  const s = String(value || "").trim();
  if (!s) return "";
  // Accept common formats: "bucket-name", "gs://bucket-name", "https://storage.googleapis.com/bucket-name/..."
  return s
    .replace(/^gs:\/\//i, "")
    .replace(/^https?:\/\/storage\.googleapis\.com\//i, "")
    .replace(/\/.+$/, ""); // strip any path after the bucket
}

const DEFAULT_BUCKET =
  normalizeBucketName(process.env.FIREBASE_STORAGE_BUCKET) ||
  "gokudatabase.firebasestorage.app"; 

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    storageBucket: DEFAULT_BUCKET
  });
}

/* ─── bucket CORS — SINGLE SOURCE OF TRUTH ──────────────────────────────
   setCorsConfiguration REPLACES the whole config, it does not merge, so this
   list is the complete set of browser origins allowed to talk to Storage.
   Anything not named here cannot upload or read a download URL from the
   browser. Add stations here and nowhere else — setCorsRule.js imports this
   same array rather than keeping its own copy.                             */
const CORS_ORIGINS = [
  "https://shipping-1.goldenspike.app",
  "https://listing-generator-1.goldenspike.app",
  "https://design.goldenspike.app",          // design station — chat attachments
  "https://design-1.goldenspike.app",        // second design bench (RT.page "design-1")
  "https://design-message.goldenspike.app",
  "https://design-message-1.goldenspike.app",
  "https://game-generator-1.goldenspike.app",
  "http://localhost:8888"
];

/* responseHeader maps to Access-Control-Expose-Headers, i.e. which response
   headers the browser is allowed to READ. The Firebase JS SDK's resumable
   uploader drives its protocol through the X-Goog-Upload-* headers — it reads
   the session URL and the per-chunk status out of them — so if they are not
   exposed the upload stalls with a CORS error rather than a useful one.
   Downloads need the content/range headers to stream and resume.           */
const CORS_RESPONSE_HEADERS = [
  "Content-Type",
  "Content-Length",
  "Content-Range",
  "Content-Disposition",
  "Content-Encoding",
  "Authorization",
  "Range",
  "ETag",
  "X-Goog-Upload-URL",
  "X-Goog-Upload-Status",
  "X-Goog-Upload-Size-Received",
  "X-Goog-Upload-Chunk-Granularity",
  "X-Firebase-Storage-Version",
  "X-Goog-Hash"
];

const CORS_CONFIG = [{
  origin        : CORS_ORIGINS,
  method        : ["GET","POST","PUT","DELETE","HEAD","OPTIONS","PATCH"],
  responseHeader: CORS_RESPONSE_HEADERS,
  maxAgeSeconds : 3600
}];

function applyBucketCors() {
  const { Storage } = require("@google-cloud/storage");
  return new Storage({ credentials: serviceAccount })
    .bucket(DEFAULT_BUCKET)
    .setCorsConfiguration(CORS_CONFIG);
}

/* ─── ensure CORS rule (runs once per cold-start) ───────── */
if (!process.env.CORS_SET) {
  applyBucketCors()
    .then(() => console.log("CORS confirmed for", CORS_ORIGINS.length, "origins"))
    .catch(err => console.error("CORS error:", err));
  process.env.CORS_SET = "1";   // prevent repeats on warm invokes
}

module.exports = admin;

// ~60 functions do `const admin = require("./firebaseAdmin")` and use the
// result as the firebase-admin namespace, so the export shape cannot change.
// These few extras hang off it instead, purely so setCorsRule.js cannot drift
// out of sync with the list above. None of these names exist on firebase-admin.
module.exports.CORS_ORIGINS = CORS_ORIGINS;
module.exports.CORS_CONFIG  = CORS_CONFIG;
module.exports.applyBucketCors = applyBucketCors;
module.exports.DEFAULT_BUCKET  = DEFAULT_BUCKET;
