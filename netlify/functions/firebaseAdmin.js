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
   configuration is the complete set of browser origins allowed to talk to
   Storage. Existing stations retain upload access; the Ads app only needs
   image reads. Add origins here — setCorsRule.js reapplies this same complete
   configuration rather than keeping its own copy.                          */
const CORS_ORIGINS = [
  "https://shipping-1.goldenspike.app",
  "https://listing-generator-1.goldenspike.app",
  "https://design.goldenspike.app",          // design station — chat attachments
  "https://design-1.goldenspike.app",        // second design bench (RT.page "design-1")
  "https://design-message.goldenspike.app",
  "https://design-message-1.goldenspike.app",
  "https://game-generator-1.goldenspike.app",
  "https://brites-charm-sorter.goldenspike.app", // charm nesting station — direct signed-URL uploads of .ai/.pdf/.png/.json
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
}, {
  origin        : ["https://goldenspike.app", "https://brites-adwords.goldenspike.app"],
  method        : ["GET", "HEAD"],
  responseHeader: ["Content-Type", "Content-Length", "Content-Range", "ETag"],
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
    .then(() => console.log("CORS confirmed for", new Set(CORS_CONFIG.flatMap(rule => rule.origin)).size, "origins"))
    .catch(err => console.error("CORS error:", err));
  process.env.CORS_SET = "1";   // prevent repeats on warm invokes
}

/* ─── READMETER (25 Nov 2026: Firestore reads cost ~$60/day; find where they come from) ──────
   Counts the documents every function reads (DocumentReference.get, Query.get, Transaction.get / getAll,
   Firestore.getAll), by function, request tag (charmNestLibrary sets its op) and collection, and adds the
   totals, at most once a minute per warm instance, to ONE document per hour: Ops_Read_Meter/<YYYY-MM-DDTHH>
   { c: { "<function>|<tag>|<collection>": n }, n: total }. It never changes what a read returns; a failure
   to count or to save is ignored. Off with READ_METER=off in the environment.                            */
(function readMeter() {
  if (global.__readMeter || String(process.env.READ_METER || "").toLowerCase() === "off") return;
  let F; try { F = admin.firestore; } catch (_) { return; }
  if (!F || !F.DocumentReference || !F.Query) return;
  const M = global.__readMeter = { counts: new Map(), total: 0, last: Date.now(), tag: "", busy: false };
  const fnName = String(process.env.AWS_LAMBDA_FUNCTION_NAME || process.env.FUNCTION_NAME || "local").replace(/^[0-9a-f]{8,}[-_]/i, "").replace(/[^A-Za-z0-9_\-]/g, "_").slice(0, 60);
  const collOfPath = path => String(path || "").split("/").filter((_, i) => i % 2 === 0).join("/").slice(0, 80) || "?";
  const collOfQuery = q => { try { const o = q && q._queryOptions; if (!o) return "query"; return (o.allDescendants ? "group:" : "") + (o.collectionId || "?"); } catch (_) { return "query"; } };
  const collOf = x => (x && typeof x.path === "string" && x.firestore ? collOfPath(x.path) : collOfQuery(x));
  let sampleN = 0;
  /** The size of what was read, in KB, from one document in 8 (its data as JSON), scaled: the transfer a read costs is its bytes. */
  const kbOf = docs => { let kb = 0; try { for (const d of docs) { if (!d || (++sampleN % 8)) continue; const x = typeof d.data === "function" ? d.data() : null; if (x) kb += 8 * JSON.stringify(x).length / 1024; } } catch (_) { kb = 0; } return kb; };
  const add = (coll, n, docs) => {
    try {
      const k = `${fnName}|${M.tag || "-"}|${coll}`.replace(/[.\/]/g, "_");
      M.counts.set(k, (M.counts.get(k) || 0) + n); M.total += n;
      const kb = docs ? kbOf(docs) : 0; if (kb) M.counts.set(k + "|kb", (M.counts.get(k + "|kb") || 0) + Math.round(kb));
      if (!M.busy && Date.now() - M.last > 60000) flush();
    } catch (_) { /* never in the way of a read */ }
  };
  function flush() {
    if (!M.counts.size) { M.last = Date.now(); return; }
    const c = {}, FV = F.FieldValue; let n = 0;
    for (const [k, v] of M.counts) { c[k] = FV.increment(v); n += v; }
    M.counts = new Map(); M.last = Date.now(); M.busy = true;
    const hour = new Date().toISOString().slice(0, 13);
    try {
      admin.firestore().collection("Ops_Read_Meter").doc(hour).set({ c, n: FV.increment(n), at: FV.serverTimestamp() }, { merge: true })
        .catch(() => null).then(() => { M.busy = false; });
    } catch (_) { M.busy = false; }
  }
  const wrap = (proto, name, count) => {
    const orig = proto && proto[name]; if (typeof orig !== "function" || orig.__metered) return;
    const w = function () { const args = arguments, self = this; const p = orig.apply(self, args); try { if (p && typeof p.then === "function") p.then(r => { try { count(self, args, r); } catch (_) {} }, () => null); } catch (_) {} return p; };
    w.__metered = true; proto[name] = w;
  };
  const sizeOf = r => (r && typeof r.size === "number" ? Math.max(1, r.size) : 1);   // (an empty query is still charged one read)
  wrap(F.DocumentReference.prototype, "get", (self, a, r) => add(collOf(self), 1, [r]));
  wrap(F.Query.prototype, "get", (self, a, r) => add(collOf(self), sizeOf(r), r && r.docs));
  if (F.Transaction) {
    wrap(F.Transaction.prototype, "get", (self, a, r) => add(collOf(a[0]), sizeOf(r), r && r.docs ? r.docs : [r]));
    wrap(F.Transaction.prototype, "getAll", (self, a, r) => { const list = Array.isArray(r) ? r : []; for (const d of list) add(collOf(d && d.ref), 1, [d]); });
  }
  if (F.Firestore) wrap(F.Firestore.prototype, "getAll", (self, a, r) => { const list = Array.isArray(r) ? r : []; for (const d of list) add(collOf(d && d.ref), 1, [d]); });
  /** The tag the next reads are counted under (charmNestLibrary: its op). → the tag before. */
  M.setTag = t => { const was = M.tag; if (t !== undefined) M.tag = String(t || "").replace(/[^A-Za-z0-9_:\-]/g, "_").slice(0, 60); return was; };
})();

module.exports = admin;

// ~60 functions do `const admin = require("./firebaseAdmin")` and use the
// result as the firebase-admin namespace, so the export shape cannot change.
// These few extras hang off it instead, purely so setCorsRule.js cannot drift
// out of sync with the list above. None of these names exist on firebase-admin.
module.exports.CORS_ORIGINS = CORS_ORIGINS;
module.exports.CORS_CONFIG  = CORS_CONFIG;
module.exports.applyBucketCors = applyBucketCors;
module.exports.DEFAULT_BUCKET  = DEFAULT_BUCKET;
/** READMETER: the tag the next reads are counted under. meterTag() reads it; meterTag(t) sets it and returns the one before. */
module.exports.meterTag = t => (global.__readMeter && global.__readMeter.setTag ? global.__readMeter.setTag(t) : "");
