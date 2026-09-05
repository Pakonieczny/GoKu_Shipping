/*  netlify/functions/_investorContentStore.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — content-addressed storage for large payloads
 *  (blueprint §10.3): filings, research packets, raw model outputs, manifests.
 *
 *  WHY THIS FILE EXISTS
 *  ------------------------------------------------------------------------
 *  Firestore caps a document at 1 MiB and bills by the byte read; a 10-K is
 *  bigger than that and a research packet is read by one job, once. The
 *  legacy desk truncated bodies into fields ("first 40 KB of the filing") and
 *  then reasoned over the truncation. That is not evidence, it is a sample
 *  nobody chose. Under the blueprint the body lives in a PRIVATE Google Cloud
 *  Storage bucket under a key derived from its sha256, and Firestore keeps an
 *  immutable manifest pointer (InvestorAI_ContentManifests/{contentHash})
 *  that any record can reference through `_investorStorageCodec.contentRef`.
 *
 *  Rules this module enforces:
 *    · content-addressed: the object key is the hash of the bytes actually
 *      stored, so the same content is stored once and a manifest is written
 *      once; a second put returns the first manifest;
 *    · never overwritten: the first write uses ifGenerationMatch: 0. A 412
 *      means the object already exists, and since the key is its hash, it is
 *      the same content — that is success, not an error;
 *    · never public: no ACL is ever set here; the bucket is private and the
 *      service account is the only reader;
 *    · never truncated: if object storage is unavailable the ingest FAILS
 *      VISIBLY (CONTENT_STORE_UNAVAILABLE). Nothing falls back to a field.
 *    · verified on read: every download is re-hashed before it is returned.
 *
 *  The GCS client is built lazily from the same three FIREBASE_* variables
 *  _investorAdmin uses (no new credential). The legacy firebaseAdmin helper
 *  is deliberately NOT imported: it rewrites the bucket CORS policy on every
 *  cold start. Tests and fixtures use the in-memory adapter and an in-memory
 *  manifest store, so `selfCheck()` never touches GCS or Firestore.
 *
 *  ENVIRONMENT:
 *    INVESTOR_CONTENT_BUCKET                       — private bucket name
 *    FIREBASE_PROJECT_ID / FIREBASE_CLIENT_EMAIL / FIREBASE_PRIVATE_KEY
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");
const zlib = require("zlib");
const A = require("./_investorAdmin");
const Codec = require("./_investorStorageCodec");

const KEY_PREFIX = "investor-ai/";
const KEY_VERSION = "v1";
const MANIFEST_SCHEMA = "content-manifest.v1";
const MANIFEST_COLLECTION = "InvestorAI_ContentManifests";
const KINDS = Object.freeze(["document", "packet", "model-output", "manifest", "archive", "export"]);
const ENCODINGS = Object.freeze(["identity", "gzip"]);
const MAX_OBJECT_BYTES = 256 * 1024 * 1024;
const SHA256_HEX_RE = /^[0-9a-f]{64}$/;

function fail(code, message, extra = {}) {
  return Object.assign(new Error(`_investorContentStore: ${message}`), { code, ...extra });
}
function sha256(bytes) { return crypto.createHash("sha256").update(bytes).digest("hex"); }
function bucketName() { return String(process.env.INVESTOR_CONTENT_BUCKET || "").trim(); }

/** Object key for a stored body: investor-ai/<kind>/<hash[0:2]>/<hash>. */
function objectKey(kind, contentHash) {
  if (!KINDS.includes(kind)) throw fail("INVALID_KIND", `kind "${kind}" is not one of ${KINDS.join(", ")}`);
  const h = String(contentHash || "").toLowerCase();
  if (!SHA256_HEX_RE.test(h)) throw fail("BAD_HASH", "contentHash must be a sha256 hex digest");
  return `${KEY_PREFIX}${kind}/${h.slice(0, 2)}/${h}`;
}

/* ── credential (identical shape to _investorAdmin.serviceAccount) ─────── */
function serviceAccount() {
  if (!process.env.FIREBASE_PRIVATE_KEY) {
    throw fail("CONTENT_STORE_UNAVAILABLE", "FIREBASE_PRIVATE_KEY is not set in this environment");
  }
  if (!process.env.FIREBASE_PROJECT_ID || !process.env.FIREBASE_CLIENT_EMAIL) {
    throw fail("CONTENT_STORE_UNAVAILABLE", "FIREBASE_PROJECT_ID / FIREBASE_CLIENT_EMAIL are required");
  }
  return {
    type: "service_account",
    project_id: process.env.FIREBASE_PROJECT_ID,
    private_key: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n"),
    client_email: process.env.FIREBASE_CLIENT_EMAIL,
  };
}

/* ── adapters ───────────────────────────────────────────────────────────
 * interface: { kind, uri(key), put({key, bytes, contentType, metadata})
 *              → {uri, generation, existed}, get(key) → {bytes, generation,
 *              metadata} | null, head(key) → {generation, bytes} | null }   */

function gcsAdapter() {
  let _storage = null, _bucket = null;
  const bucket = () => {
    if (_bucket) return _bucket;
    const name = bucketName();
    if (!name) throw fail("CONTENT_STORE_UNAVAILABLE", "INVESTOR_CONTENT_BUCKET is not configured");
    const sa = serviceAccount();
    const { Storage } = require("@google-cloud/storage");
    _storage = new Storage({ projectId: sa.project_id,
      credentials: { client_email: sa.client_email, private_key: sa.private_key } });
    _bucket = _storage.bucket(name);
    return _bucket;
  };
  const uri = (key) => `gs://${bucketName()}/${key}`;
  return {
    kind: "gcs", uri,
    async put({ key, bytes, contentType, metadata }) {
      const file = bucket().file(key);
      try {
        /* ifGenerationMatch: 0 — create only. No `public`, no predefinedAcl:
           the object inherits the bucket's private policy. */
        await file.save(bytes, { resumable: false, contentType, validation: "crc32c",
          metadata: { contentType, metadata }, preconditionOpts: { ifGenerationMatch: 0 } });
        const [md] = await file.getMetadata();
        return { uri: uri(key), generation: String(md.generation), existed: false };
      } catch (e) {
        const status = Number(e && (e.code || (e.response && e.response.status)));
        if (status !== 412) throw fail("CONTENT_STORE_WRITE_FAILED", `${key}: ${String(e && e.message || e)}`, { cause: e });
        /* Precondition failed: the key already holds these bytes. */
        const [md] = await file.getMetadata();
        return { uri: uri(key), generation: String(md.generation), existed: true };
      }
    },
    async get(key) {
      const file = bucket().file(key);
      const [exists] = await file.exists();
      if (!exists) return null;
      const [[bytes], [md]] = await Promise.all([file.download(), file.getMetadata()]);
      return { bytes: Buffer.from(bytes), generation: String(md.generation), metadata: md.metadata || {} };
    },
    async head(key) {
      const file = bucket().file(key);
      const [exists] = await file.exists();
      if (!exists) return null;
      const [md] = await file.getMetadata();
      return { generation: String(md.generation), bytes: Number(md.size) };
    },
  };
}

/** In-memory adapter: fake:// URIs, monotonically increasing generations,
 *  content-addressed create-only semantics identical to the GCS path. The
 *  `objects` Map is exposed so fixtures can tamper with stored bytes. */
function memoryAdapter({ bucket = "memory" } = {}) {
  const objects = new Map();
  let generation = 0;
  const uri = (key) => `fake://${bucket}/${key}`;
  return {
    kind: "memory", objects, uri,
    async put({ key, bytes, contentType, metadata }) {
      const cur = objects.get(key);
      if (cur) return { uri: uri(key), generation: String(cur.generation), existed: true };
      generation += 1;
      objects.set(key, { bytes: Buffer.from(bytes), contentType, metadata: { ...metadata }, generation });
      return { uri: uri(key), generation: String(generation), existed: false };
    },
    async get(key) {
      const cur = objects.get(key);
      return cur ? { bytes: Buffer.from(cur.bytes), generation: String(cur.generation), metadata: { ...cur.metadata } } : null;
    },
    async head(key) {
      const cur = objects.get(key);
      return cur ? { generation: String(cur.generation), bytes: cur.bytes.length } : null;
    },
  };
}

function createAdapter({ kind = "gcs", ...opts } = {}) {
  if (kind === "gcs") return gcsAdapter(opts);
  if (kind === "memory") return memoryAdapter(opts);
  throw fail("INVALID_ADAPTER", `adapter kind "${kind}" unknown (gcs | memory)`);
}

let _defaultAdapter = null;
function defaultAdapter() {
  if (!_defaultAdapter) _defaultAdapter = createAdapter({ kind: "gcs" });
  return _defaultAdapter;
}
function setDefaultAdapter(adapter) {
  if (adapter != null && (typeof adapter.put !== "function" || typeof adapter.get !== "function")) {
    throw fail("INVALID_ADAPTER", "adapter must implement put() and get()");
  }
  const prev = _defaultAdapter;
  _defaultAdapter = adapter || null;
  return prev;
}

/* ── manifest stores ────────────────────────────────────────────────────
 * interface: { get(id) → manifest | null, create(id, manifest) → {manifest,
 *              existed} }. Manifests are stored ENCODED through the
 * content-manifest.v1 codec and decoded on read — the store is a place the
 * codec's "never guess the shape" rule applies to. */
const manifestCodec = () => Codec.codecFor(MANIFEST_SCHEMA);

function firestoreManifestStore() {
  const col = () => (A.COL.contentManifests ? A.col(A.COL.contentManifests) : A.col(MANIFEST_COLLECTION));
  return {
    kind: "firestore",
    async get(id) {
      const snap = await col().doc(id).get();
      return snap.exists ? manifestCodec().decode(snap.data()) : null;
    },
    async create(id, manifest) {
      const ref = col().doc(id);
      const encoded = manifestCodec().encode(manifest);
      const existed = await A.runTransaction(async (tx) => {
        const cur = await tx.get(ref);
        if (cur.exists) return true;
        tx.set(ref, encoded);
        return false;
      });
      /* Read back so the caller sees resolved server timestamps, not sentinels. */
      const snap = await ref.get();
      return { manifest: manifestCodec().decode(snap.data()), existed };
    },
  };
}

/** In-memory manifest store over a Map — what selfCheck and fixtures use. */
function memoryManifestStore(manifests = new Map()) {
  return {
    kind: "memory", manifests,
    async get(id) {
      const doc = manifests.get(id);
      return doc ? manifestCodec().decode(doc) : null;
    },
    async create(id, manifest) {
      if (manifests.has(id)) return { manifest: manifestCodec().decode(manifests.get(id)), existed: true };
      /* Same JSON round trip Firestore imposes, so fixtures see what production sees. */
      const encoded = manifestCodec().encode({ ...manifest, created_at: null });
      manifests.set(id, JSON.parse(JSON.stringify(encoded)));
      return { manifest: manifestCodec().decode(manifests.get(id)), existed: false };
    },
  };
}

let _manifestStore = null;
function manifestStore() {
  if (!_manifestStore) _manifestStore = firestoreManifestStore();
  return _manifestStore;
}
/** Accepts a store object, `{ manifests: Map }`, or null to return to Firestore. */
function setManifestStore(store) {
  const prev = _manifestStore;
  if (store == null) _manifestStore = null;
  else if (typeof store.get === "function" && typeof store.create === "function") _manifestStore = store;
  else if (store.manifests instanceof Map) _manifestStore = memoryManifestStore(store.manifests);
  else throw fail("INVALID_MANIFEST_STORE", "store must implement get()/create() or be { manifests: Map }");
  return prev;
}

/* ── availability ───────────────────────────────────────────────────── */
/** Blueprint rule: no object storage means the ingest fails, visibly. */
function assertAvailable({ adapter = defaultAdapter() } = {}) {
  if (adapter.kind !== "gcs") return true;
  if (!bucketName()) throw fail("CONTENT_STORE_UNAVAILABLE", "INVESTOR_CONTENT_BUCKET is not configured — refusing to ingest; nothing is truncated into Firestore");
  serviceAccount();
  return true;
}

/* ── put / get / verify ─────────────────────────────────────────────── */
function toBuffer({ bytes, text, json }, mimeType) {
  const given = [bytes !== undefined, text !== undefined, json !== undefined].filter(Boolean).length;
  if (given !== 1) throw fail("INVALID_INPUT", "provide exactly one of bytes | text | json");
  if (bytes !== undefined) {
    if (!(Buffer.isBuffer(bytes) || bytes instanceof Uint8Array)) throw fail("INVALID_INPUT", "bytes must be a Buffer or Uint8Array");
    return { plain: Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength), mimeType: mimeType || "application/octet-stream" };
  }
  if (text !== undefined) {
    if (typeof text !== "string") throw fail("INVALID_INPUT", "text must be a string");
    return { plain: Buffer.from(text, "utf8"), mimeType: mimeType || "text/plain; charset=utf-8" };
  }
  return { plain: Buffer.from(Codec.canonicalJson(json), "utf8"), mimeType: mimeType || "application/json" };
}
function isJsonMime(m) { return /^application\/(json|[a-z0-9.+-]+\+json)\b/i.test(String(m || "")); }

function refOf(manifest) {
  return Codec.contentRef({ contentHash: manifest.contentHash, bytes: manifest.bytes, storageUri: manifest.storageUri,
    mimeType: manifest.mimeType, generation: manifest.generation, encoding: manifest.encoding });
}

/** Store one body. Hashes what is STORED (gzip output when encoding is
 *  gzip) and records `plainSha256` of the original bytes as well. */
async function putContent({ kind, bytes, text, json, mimeType = null, encoding = "identity",
  asOf = null, createdBy = "investor-ai", meta = {}, adapter = null, store = null } = {}) {
  const ad = adapter || defaultAdapter();
  const ms = store || manifestStore();
  if (!KINDS.includes(kind)) throw fail("INVALID_KIND", `kind "${kind}" is not one of ${KINDS.join(", ")}`);
  if (!ENCODINGS.includes(encoding)) throw fail("INVALID_ENCODING", `encoding "${encoding}" is not identity | gzip`);
  const { plain, mimeType: mt } = toBuffer({ bytes, text, json }, mimeType);
  if (plain.length > MAX_OBJECT_BYTES) {
    throw fail("OBJECT_TOO_LARGE", `${plain.length} bytes exceeds MAX_OBJECT_BYTES ${MAX_OBJECT_BYTES}`, { bytes: plain.length });
  }
  assertAvailable({ adapter: ad });
  /* Node's gzip header carries mtime 0 and no filename, so the same plain
     bytes always compress to the same stored bytes — the hash is stable. */
  const stored = encoding === "gzip" ? zlib.gzipSync(plain, { level: 9 }) : plain;
  if (stored.length > MAX_OBJECT_BYTES) {
    throw fail("OBJECT_TOO_LARGE", `${stored.length} stored bytes exceeds MAX_OBJECT_BYTES ${MAX_OBJECT_BYTES}`, { bytes: stored.length });
  }
  const contentHash = sha256(stored), plainSha256 = sha256(plain);

  const existing = await ms.get(contentHash);
  if (existing) return { id: contentHash, manifest: existing, ref: refOf(existing), existed: true };

  const key = objectKey(kind, contentHash);
  const written = await ad.put({ key, bytes: stored, contentType: mt,
    metadata: { contentHash, kind, mimeType: mt, encoding, keyVersion: KEY_VERSION } });
  const manifest = {
    schemaVersion: MANIFEST_SCHEMA, contentHash, plainSha256,
    bytes: stored.length, plainBytes: plain.length, mimeType: mt, encoding,
    storageUri: written.uri, generation: String(written.generation), kind, keyVersion: KEY_VERSION,
    chunkCount: 1, createdAtMs: Date.now(), asOf: asOf == null ? null : String(asOf), createdBy: String(createdBy || "investor-ai"),
    meta: A.firestoreSafe(meta && typeof meta === "object" ? meta : {}),
    ...A.envelope({ created_by: String(createdBy || "investor-ai") }),
  };
  const { manifest: saved, existed } = await ms.create(contentHash, manifest);
  return { id: contentHash, manifest: saved, ref: refOf(saved), existed };
}

async function readVerified(contentHash, { adapter, store, verify }) {
  const h = String(contentHash || "").toLowerCase();
  if (!SHA256_HEX_RE.test(h)) throw fail("BAD_HASH", "contentHash must be a sha256 hex digest");
  const manifest = await store.get(h);
  if (!manifest) throw fail("CONTENT_NOT_FOUND", `no manifest for ${h}`, { contentHash: h });
  const obj = await adapter.get(objectKey(manifest.kind, h));
  if (!obj) throw fail("CONTENT_NOT_FOUND", `manifest exists but object is missing for ${h}`, { contentHash: h });
  const computedHash = sha256(obj.bytes);
  if (verify && (computedHash !== h || obj.bytes.length !== manifest.bytes)) {
    throw fail("CONTENT_HASH_MISMATCH", `${h.slice(0, 12)}… stored bytes hash to ${computedHash.slice(0, 12)}…`,
      { contentHash: h, storedHash: h, computedHash, bytes: obj.bytes.length });
  }
  return { manifest, obj, computedHash };
}

/** Download, verify, decompress. `json` is parsed only for JSON mime types. */
async function getContent(contentHash, { verify = true, adapter = null, store = null } = {}) {
  const ad = adapter || defaultAdapter(), ms = store || manifestStore();
  const { manifest, obj } = await readVerified(contentHash, { adapter: ad, store: ms, verify });
  let plain = obj.bytes;
  if (manifest.encoding === "gzip") {
    try { plain = zlib.gunzipSync(obj.bytes); }
    catch (e) { throw fail("CONTENT_DECODE_FAILED", `gunzip failed for ${manifest.contentHash}: ${e.message}`); }
  }
  if (verify && manifest.plainSha256 && sha256(plain) !== manifest.plainSha256) {
    throw fail("CONTENT_HASH_MISMATCH", "plain bytes do not match manifest.plainSha256",
      { contentHash: manifest.contentHash, storedHash: manifest.plainSha256, computedHash: sha256(plain) });
  }
  const text = plain.toString("utf8");
  let json;
  if (isJsonMime(manifest.mimeType)) {
    try { json = JSON.parse(text); }
    catch (e) { throw fail("CONTENT_DECODE_FAILED", `JSON parse failed for ${manifest.contentHash}: ${e.message}`); }
  }
  return { bytes: plain, text, json, manifest };
}

/** Integrity report without returning the body. */
async function verifyContent(contentHash, { adapter = null, store = null } = {}) {
  const ad = adapter || defaultAdapter(), ms = store || manifestStore();
  try {
    const { manifest, obj, computedHash } = await readVerified(contentHash, { adapter: ad, store: ms, verify: false });
    const ok = computedHash === manifest.contentHash && obj.bytes.length === manifest.bytes;
    return { ok, bytes: obj.bytes.length, storedHash: manifest.contentHash, computedHash, generation: obj.generation };
  } catch (e) {
    return { ok: false, bytes: null, storedHash: String(contentHash || ""), computedHash: null, error: e.code || String(e.message) };
  }
}

/** Canonical JSON in, so identical values hash identically. */
function putJson(kind, value, opts = {}) {
  return putContent({ ...opts, kind, json: value, mimeType: opts.mimeType || "application/json" });
}
async function getJson(contentHash, opts = {}) {
  const r = await getContent(contentHash, opts);
  if (r.json === undefined) throw fail("CONTENT_DECODE_FAILED", `${contentHash} is ${r.manifest.mimeType}, not JSON`);
  return r.json;
}

/* ── self check ─────────────────────────────────────────────────────── */
async function selfCheck() {
  const failures = [];
  const check = async (name, fn) => {
    try { const r = await fn(); if (r === false) failures.push(`${name}: returned false`); }
    catch (e) { failures.push(`${name}: ${String(e && e.message || e)}`); }
  };
  const expectCode = (name, code, fn) => check(name, async () => {
    try { await fn(); } catch (e) { if (e.code === code) return true; throw new Error(`expected ${code}, got ${e.code}: ${e.message}`); }
    throw new Error(`expected ${code}, nothing thrown`);
  });

  const adapter = memoryAdapter({ bucket: "selfcheck" });
  const manifests = new Map();
  const prevAdapter = setDefaultAdapter(adapter);
  const prevStore = setManifestStore({ manifests });
  const prevBucket = process.env.INVESTOR_CONTENT_BUCKET;
  try {
    await check("text round trip", async () => {
      const put = await putContent({ kind: "document", text: "hello, filing — é", createdBy: "selfcheck", meta: { source: "unit" } });
      if (put.existed || put.manifest.encoding !== "identity" || put.manifest.kind !== "document") return false;
      if (!Codec.isContentRef(put.ref) || !put.ref.storageUri.startsWith("fake://selfcheck/investor-ai/document/")) return false;
      if (put.manifest.contentHash !== put.manifest.plainSha256) return false;
      const got = await getContent(put.id);
      const v = await verifyContent(put.id);
      return got.text === "hello, filing — é" && got.json === undefined && got.manifest.meta.source === "unit"
        && v.ok && v.bytes === put.manifest.bytes && v.computedHash === put.id;
    });
    await check("json round trip is key-order independent", async () => {
      const a = await putJson("packet", { b: [1, { z: 1, y: 2 }], a: "x" });
      const b = await putJson("packet", { a: "x", b: [1, { y: 2, z: 1 }] });
      if (a.id !== b.id || !b.existed) return false;
      const j = await getJson(a.id);
      return j.a === "x" && j.b[1].y === 2 && a.manifest.mimeType === "application/json";
    });
    await check("gzip round trip hashes the stored bytes", async () => {
      const text = "0123456789".repeat(5000);
      const put = await putContent({ kind: "archive", text, encoding: "gzip", mimeType: "text/plain" });
      const m = put.manifest;
      if (m.encoding !== "gzip" || m.bytes >= m.plainBytes || m.contentHash === m.plainSha256) return false;
      if (m.contentHash !== sha256(adapter.objects.get(objectKey("archive", m.contentHash)).bytes)) return false;
      const got = await getContent(put.id);
      const again = await putContent({ kind: "archive", text, encoding: "gzip", mimeType: "text/plain" });
      return got.text === text && got.bytes.length === m.plainBytes && again.id === put.id && again.existed;
    });
    await check("idempotent double put", async () => {
      const before = adapter.objects.size, beforeM = manifests.size;
      const p1 = await putContent({ kind: "model-output", text: "same" });
      const p2 = await putContent({ kind: "model-output", text: "same" });
      return p1.id === p2.id && !p1.existed && p2.existed && p1.manifest.generation === p2.manifest.generation
        && adapter.objects.size === before + 1 && manifests.size === beforeM + 1;
    });
    await check("generations are monotonic", async () => {
      const p1 = await putContent({ kind: "export", text: "g1" });
      const p2 = await putContent({ kind: "export", text: "g2" });
      return Number(p2.manifest.generation) > Number(p1.manifest.generation);
    });
    await check("tamper detection", async () => {
      const put = await putContent({ kind: "document", text: "untampered body" });
      const key = objectKey("document", put.id);
      const cur = adapter.objects.get(key);
      const evil = Buffer.from(cur.bytes); evil[0] = evil[0] ^ 0xff;
      adapter.objects.set(key, { ...cur, bytes: evil });
      const v = await verifyContent(put.id);
      if (v.ok || v.computedHash === put.id) return false;
      try { await getContent(put.id); return false; } catch (e) { if (e.code !== "CONTENT_HASH_MISMATCH") return false; }
      const unverified = await getContent(put.id, { verify: false });
      return unverified.bytes.length === cur.bytes.length;
    });
    await check("manifest tamper detection", async () => {
      const put = await putContent({ kind: "document", text: "manifest body" });
      const doc = manifests.get(put.id);
      manifests.set(put.id, { ...doc, bytes: doc.bytes + 1 });
      try { await getContent(put.id); return false; } catch (e) { return e.code === "CONTENT_HASH_MISMATCH"; }
    });
    await expectCode("size refusal", "OBJECT_TOO_LARGE", () =>
      putContent({ kind: "archive", bytes: Buffer.allocUnsafe(MAX_OBJECT_BYTES + 1) }));
    await expectCode("invalid kind", "INVALID_KIND", () => putContent({ kind: "secrets", text: "x" }));
    await expectCode("exactly one body", "INVALID_INPUT", () => putContent({ kind: "document", text: "x", json: {} }));
    await expectCode("not found", "CONTENT_NOT_FOUND", () => getContent("0".repeat(64)));
    await expectCode("bad hash", "BAD_HASH", () => getContent("nope"));
    await check("unavailable without a bucket fails visibly", async () => {
      delete process.env.INVESTOR_CONTENT_BUCKET;
      const gcs = createAdapter({ kind: "gcs" });
      try { assertAvailable({ adapter: gcs }); return false; } catch (e) { if (e.code !== "CONTENT_STORE_UNAVAILABLE") return false; }
      try { await putContent({ kind: "document", text: "x", adapter: gcs }); return false; }
      catch (e) { if (e.code !== "CONTENT_STORE_UNAVAILABLE") return false; }
      return assertAvailable({ adapter }) === true;
    });
    await check("object key layout", () => objectKey("packet", "ab" + "c".repeat(62)) === `investor-ai/packet/ab/ab${"c".repeat(62)}`);
  } finally {
    setDefaultAdapter(prevAdapter);
    setManifestStore(prevStore);
    if (prevBucket === undefined) delete process.env.INVESTOR_CONTENT_BUCKET;
    else process.env.INVESTOR_CONTENT_BUCKET = prevBucket;
  }
  return { pass: failures.length === 0, failures };
}

module.exports = {
  KINDS, ENCODINGS, KEY_PREFIX, KEY_VERSION, MANIFEST_SCHEMA, MANIFEST_COLLECTION, MAX_OBJECT_BYTES,
  objectKey, sha256,
  createAdapter, memoryAdapter, defaultAdapter, setDefaultAdapter,
  memoryManifestStore, setManifestStore,
  assertAvailable,
  putContent, getContent, verifyContent, putJson, getJson,
  selfCheck,
};
