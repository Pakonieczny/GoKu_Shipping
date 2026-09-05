/*  netlify/functions/_investorStorageCodec.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — schema-aware encode/decode for every v1 storage record
 *  (blueprint §10.3).
 *
 *  WHY THIS FILE EXISTS
 *  ------------------------------------------------------------------------
 *  A Firestore document is not JSON. Four things bit the legacy desk, each
 *  discovered in production rather than in a test:
 *
 *    · an array inside an array is refused by Firestore and the WHOLE write
 *      fails (one such value reported every scan as incomplete for a day);
 *    · a document is capped at 1,048,576 bytes — a manager run that stored
 *      its 304 per-symbol rows inline hit the ceiling the first day the
 *      universe grew;
 *    · `undefined` is silently dropped by the client (ignoreUndefinedProperties)
 *      so a field can vanish between writer and reader with no error;
 *    · a money amount stored as a JS double reads back as a different number
 *      than the ledger has, and nobody notices until the reconciliation.
 *
 *  Every new v1 record therefore passes through a registered codec. The
 *  codec refuses what Firestore cannot hold (or holds lossily), converts what
 *  can be converted REVERSIBLY, records exactly which paths it converted, and
 *  stamps a content hash so a reader can prove the bytes it decoded are the
 *  bytes the writer meant. `decode` never guesses: a document without a
 *  `_codec` stamp, or with a schema this module has not registered, is an
 *  UNRECOGNISED_SHAPE — the caller must say what it expected.
 *
 *  What the stamp covers: `_codec.contentHash` is sha256 over the canonical
 *  JSON of the encoded document minus `_codec` and minus any OPAQUE paths.
 *  Opaque values are class instances the server resolves or re-types on the
 *  way back (FieldValue.serverTimestamp, Timestamp, Date, Buffer); their
 *  value at write time is not the value at read time, so they cannot be part
 *  of a client-computed hash. Their paths are recorded so a reader knows
 *  precisely what the hash does not attest.
 *
 *  Nothing in this file talks to Firestore. It is pure, synchronous and
 *  requireable without credentials; the write sites pair it with
 *  `_investorAdmin` and large bodies go through `_investorContentStore`.
 * ---------------------------------------------------------------------------
 */

"use strict";

const crypto = require("crypto");

const CODEC_VERSION = "investor-storage-codec.v1";
const COL_PREFIX = "InvestorAI_";

/* Firestore's hard document ceiling and the size this codec is willing to
   write. The gap covers Firestore's own field-name and type-tag encoding,
   which our estimate approximates rather than reproduces. */
const MAX_DOCUMENT_BYTES = 1048576;
const SAFE_DOCUMENT_BYTES = 900000;
const PER_FIELD_OVERHEAD_BYTES = 32;

/* Field names that carry money or fixed-point quantities. A JS Number at one
   of these boundaries is refused: the value must already be a canonical
   integer string (or a BigInt, which the codec converts to one). */
const MONEY_KEY_RE = /(Micros|Minor|Units|Ppm|Bps|Cents)$/;
/* Structural counts that happen to end in "Units" — they count things, they
   do not denominate money, and a plain integer is the honest representation. */
const STRUCTURAL_COUNT_KEYS = Object.freeze([
  "workUnits", "computeUnits", "tokenUnits", "requestUnits",
  "chunkUnits", "legUnits", "rowUnits", "sampleUnits", "storageUnits",
]);
const CANONICAL_INT_RE = /^-?(0|[1-9][0-9]*)$/;
const SHA256_HEX_RE = /^[0-9a-f]{64}$/;

function fail(code, message, extra = {}) {
  return Object.assign(new Error(`_investorStorageCodec: ${message}`), { code, ...extra });
}

/* ── JSON pointers (RFC 6901) ───────────────────────────────────────────
 * Paths are stored as pointer STRINGS, not arrays of segments: an array of
 * path arrays would itself be the nested array Firestore refuses. */
function escapeSeg(s) { return String(s).replace(/~/g, "~0").replace(/\//g, "~1"); }
function unescapeSeg(s) { return s.replace(/~1/g, "/").replace(/~0/g, "~"); }
function pointer(segs) { return segs.length ? "/" + segs.map(escapeSeg).join("/") : ""; }
function segments(ptr) {
  if (ptr === "") return [];
  if (ptr[0] !== "/") throw fail("BAD_POINTER", `malformed pointer "${ptr}"`);
  return ptr.slice(1).split("/").map(unescapeSeg);
}
function getAt(root, segs) {
  let cur = root;
  for (const s of segs) {
    if (cur === null || typeof cur !== "object") return undefined;
    cur = cur[s];
  }
  return cur;
}
function setAt(root, segs, value) {
  if (!segs.length) return value;
  let cur = root;
  for (let i = 0; i < segs.length - 1; i += 1) cur = cur[segs[i]];
  cur[segs[segs.length - 1]] = value;
  return root;
}

/* ── value classification ───────────────────────────────────────────── */
function isPlainObject(v) {
  if (v === null || typeof v !== "object") return false;
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
}
/** A class instance the server re-types on the way back (Timestamp, Date,
 *  FieldValue sentinel, Buffer, GeoPoint, DocumentReference …). */
function isOpaque(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v) && !isPlainObject(v);
}

/* ── canonical JSON ─────────────────────────────────────────────────────
 * Sorted keys, no whitespace, BigInt as decimal digits, opaque values by
 * their most stable textual form. Used for hashing and for the size
 * estimate; it is NOT what gets written to Firestore. */
function canonicalize(v) {
  if (typeof v === "bigint") return v.toString();
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (v === undefined) return null;
  if (Array.isArray(v)) return v.map(canonicalize);
  if (isPlainObject(v)) {
    return Object.fromEntries(Object.keys(v).sort().map((k) => [k, canonicalize(v[k])]));
  }
  if (isOpaque(v)) {
    if (v instanceof Date) return v.toISOString();
    if (typeof v.toDate === "function") { try { return v.toDate().toISOString(); } catch { /* fall through */ } }
    if (Buffer.isBuffer(v) || v instanceof Uint8Array) return Buffer.from(v).toString("base64");
    if (typeof v.toJSON === "function") { try { return canonicalize(v.toJSON()); } catch { /* fall through */ } }
    return `[opaque ${v.constructor && v.constructor.name || "object"}]`;
  }
  return v;
}
function canonicalJson(v) { return JSON.stringify(canonicalize(v)); }
function sha256Hex(text) { return crypto.createHash("sha256").update(text, "utf8").digest("hex"); }

function countFields(v) {
  if (Array.isArray(v)) return v.reduce((n, x) => n + countFields(x), 0);
  if (isPlainObject(v)) return Object.keys(v).reduce((n, k) => n + 1 + countFields(v[k]), 0);
  return 0;
}
/** Conservative serialized size of a plain object: UTF-8 bytes of its
 *  canonical JSON plus a fixed per-field allowance for Firestore's own
 *  field-name and type encoding. Over-estimates on purpose. */
function documentBytes(value) {
  return Buffer.byteLength(canonicalJson(value), "utf8") + PER_FIELD_OVERHEAD_BYTES * countFields(value);
}

/* ── the reversible transform ───────────────────────────────────────── */
function moneyBoundary(fieldName) {
  return fieldName != null && MONEY_KEY_RE.test(fieldName) && !STRUCTURAL_COUNT_KEYS.includes(fieldName);
}

/** Walk `value`, producing the Firestore-safe shape and recording every
 *  conversion in `ctx`. `fieldName` is the nearest enclosing object key —
 *  array elements inherit it, so `feesCents: [1.5]` is a money boundary. */
function encodeTree(value, segs, ctx, insideArray, fieldName) {
  const here = () => pointer(segs);
  if (value === undefined) throw fail("UNDEFINED_FIELD", `undefined at ${here() || "/"}`, { path: here() });
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw fail("NON_FINITE_NUMBER", `non-finite number ${String(value)} at ${here()}`, { path: here() });
    }
    if (moneyBoundary(fieldName)) {
      throw fail("FLOAT_AT_MONEY_BOUNDARY",
        `JS Number ${value} at money boundary ${here()} — use a canonical integer string or BigInt`,
        { path: here(), field: fieldName });
    }
    return value;
  }
  if (typeof value === "bigint") {
    ctx.bigintPaths.push(here());
    return value.toString();
  }
  if (typeof value === "string") {
    if (moneyBoundary(fieldName) && !CANONICAL_INT_RE.test(value)) {
      throw fail("NON_CANONICAL_MONEY_STRING",
        `"${value.slice(0, 40)}" at money boundary ${here()} is not a canonical integer string`,
        { path: here(), field: fieldName });
    }
    return value;
  }
  if (typeof value === "function" || typeof value === "symbol") {
    throw fail("UNSUPPORTED_VALUE", `${typeof value} at ${here()}`, { path: here() });
  }
  if (value === null || typeof value !== "object") return value;
  if (ctx.seen.has(value)) throw fail("CYCLIC_VALUE", `cycle at ${here()}`, { path: here() });
  ctx.seen.add(value);
  if (segs.length > 64) throw fail("TOO_DEEP", `nesting deeper than 64 at ${here()}`, { path: here() });
  let out;
  if (Array.isArray(value)) {
    const items = value.map((x, i) => encodeTree(x, segs.concat(String(i)), ctx, true, fieldName));
    if (insideArray) {
      ctx.nestedArrayPaths.push(here());
      out = {};
      items.forEach((x, i) => { out[String(i)] = x; });
    } else {
      out = items;
    }
  } else if (isPlainObject(value)) {
    out = {};
    for (const k of Object.keys(value)) {
      out[k] = encodeTree(value[k], segs.concat(k), ctx, false, k);
    }
  } else {
    ctx.opaquePaths.push(here());
    out = value;
  }
  ctx.seen.delete(value);
  return out;
}

/** Deep clone of a plain tree with the listed pointer paths removed —
 *  the exact input the content hash is computed over. */
function withoutPaths(doc, paths) {
  const skip = new Set(paths);
  const walk = (v, segs) => {
    if (Array.isArray(v)) return v.map((x, i) => walk(x, segs.concat(String(i))));
    if (isPlainObject(v)) {
      const out = {};
      for (const k of Object.keys(v)) {
        const p = pointer(segs.concat(k));
        if (skip.has(p)) continue;
        out[k] = walk(v[k], segs.concat(k));
      }
      return out;
    }
    return v;
  };
  return walk(doc, []);
}
function contentHashOf(encodedDoc, opaquePaths = []) {
  const { _codec, ...body } = encodedDoc;
  return sha256Hex(canonicalJson(withoutPaths(body, opaquePaths)));
}

/** Reverse encodeTree using the recorded paths. Deepest paths first so a
 *  restored parent never invalidates a child pointer. */
function restore(doc, { nestedArrayPaths = [], bigintPaths = [] }) {
  const byDepth = (a, b) => segments(b).length - segments(a).length;
  for (const p of [...nestedArrayPaths].sort(byDepth)) {
    const segs = segments(p);
    const obj = getAt(doc, segs);
    if (!isPlainObject(obj)) throw fail("RESTORE_FAILED", `expected index-keyed object at ${p}`, { path: p });
    const keys = Object.keys(obj);
    const arr = new Array(keys.length);
    keys.forEach((k) => {
      const i = Number(k);
      if (!Number.isInteger(i) || i < 0 || i >= keys.length) {
        throw fail("RESTORE_FAILED", `non-index key "${k}" at ${p}`, { path: p });
      }
      arr[i] = obj[k];
    });
    doc = setAt(doc, segs, arr);
  }
  for (const p of bigintPaths) {
    const segs = segments(p);
    const s = getAt(doc, segs);
    if (typeof s !== "string" || !CANONICAL_INT_RE.test(s)) {
      throw fail("RESTORE_FAILED", `expected canonical integer string at ${p}`, { path: p });
    }
    doc = setAt(doc, segs, BigInt(s));
  }
  return doc;
}

/* ── registry ───────────────────────────────────────────────────────── */
const registry = Object.create(null);

/** Read-only live view of the registry: `CODECS["manager-run.v1"]`. */
const CODECS = new Proxy(registry, {
  set() { throw fail("READ_ONLY", "CODECS is a read-only view — use registerCodec()"); },
  deleteProperty() { throw fail("READ_ONLY", "CODECS is a read-only view"); },
  defineProperty() { throw fail("READ_ONLY", "CODECS is a read-only view"); },
});

function codecFor(schemaVersion) {
  const c = registry[String(schemaVersion)];
  if (!c) throw fail("UNRECOGNISED_SHAPE", `no codec registered for schemaVersion "${schemaVersion}"`,
    { schemaVersion: String(schemaVersion) });
  return c;
}
function collectionFor(schemaVersion) { return codecFor(schemaVersion).collection; }

/** Register one schema. `encode`/`decode` are optional shape transforms
 *  (record → storable, storable → record); `validate(record)` returns
 *  `{ ok, errors }` and runs on both sides. */
function registerCodec({ schemaVersion, collection, encode = null, decode = null, validate = null,
  required = [], maxBytes = SAFE_DOCUMENT_BYTES }) {
  const sv = String(schemaVersion || "");
  if (!/^[a-z0-9-]+\.v[0-9]+$/.test(sv)) throw fail("BAD_SCHEMA_VERSION", `schemaVersion "${sv}" must look like "name.vN"`);
  if (registry[sv]) throw fail("CODEC_ALREADY_REGISTERED", `"${sv}" is already registered`);
  const col = String(collection || "");
  if (!col.startsWith(COL_PREFIX) || col.includes("/")) {
    throw fail("BAD_COLLECTION", `"${col}" must be a bare ${COL_PREFIX} collection name`);
  }
  if (!(Number.isInteger(maxBytes) && maxBytes > 0 && maxBytes <= SAFE_DOCUMENT_BYTES)) {
    throw fail("BAD_LIMIT", `maxBytes must be an integer in (0, ${SAFE_DOCUMENT_BYTES}]`);
  }
  const requiredKeys = Object.freeze([...required].map(String));

  function runValidate(record, stage) {
    if (typeof validate !== "function") return;
    const r = validate(record);
    if (!r || r.ok !== true) {
      const errors = (r && Array.isArray(r.errors)) ? r.errors.map(String) : ["validate() did not return { ok: true }"];
      throw fail("SCHEMA_INVALID", `${sv} failed validation on ${stage}: ${errors.join("; ")}`, { errors, schemaVersion: sv });
    }
  }

  function encodeRecord(record) {
    if (!isPlainObject(record)) throw fail("SCHEMA_INVALID", `${sv}: record must be a plain object`, { schemaVersion: sv });
    if (record.schemaVersion !== sv) {
      throw fail("SCHEMA_MISMATCH", `record.schemaVersion is ${JSON.stringify(record.schemaVersion)}, codec is "${sv}"`,
        { expected: sv, actual: record.schemaVersion });
    }
    const missing = requiredKeys.filter((k) => record[k] === undefined || record[k] === null);
    if (missing.length) {
      throw fail("MISSING_REQUIRED", `${sv} record is missing required field(s): ${missing.join(", ")}`, { missing });
    }
    runValidate(record, "encode");
    const shaped = typeof encode === "function" ? encode(record) : record;
    if (!isPlainObject(shaped)) throw fail("SCHEMA_INVALID", `${sv}: encode() must return a plain object`);
    if ("_codec" in shaped) throw fail("RESERVED_FIELD", "record must not carry its own _codec field");
    const ctx = { nestedArrayPaths: [], bigintPaths: [], opaquePaths: [], seen: new WeakSet() };
    const body = encodeTree(shaped, [], ctx, false, null);
    const stamp = {
      schemaVersion: sv, codecVersion: CODEC_VERSION, encodedAtMs: Date.now(), bytes: 0,
      nestedArrayPaths: ctx.nestedArrayPaths, bigintPaths: ctx.bigintPaths, opaquePaths: ctx.opaquePaths,
      contentHash: contentHashOf(body, ctx.opaquePaths),
    };
    const doc = { ...body, _codec: stamp };
    stamp.bytes = documentBytes(doc);
    if (stamp.bytes > maxBytes) {
      throw fail("DOCUMENT_TOO_LARGE",
        `${sv} document is ${stamp.bytes} bytes; limit for this schema is ${maxBytes} (Firestore hard cap ${MAX_DOCUMENT_BYTES})`,
        { bytes: stamp.bytes, maxBytes, schemaVersion: sv,
          suggestion: "store the large part through _investorContentStore and keep a manifest pointer" });
    }
    return doc;
  }

  function decodeDocument(doc, { schemaVersion: expected = null } = {}) {
    if (!isPlainObject(doc) || !isPlainObject(doc._codec)) {
      throw fail("UNRECOGNISED_SHAPE", "document carries no _codec stamp — refusing to guess its shape");
    }
    const stamp = doc._codec;
    if (stamp.schemaVersion !== sv) {
      throw fail("SCHEMA_MISMATCH", `document is "${stamp.schemaVersion}", codec is "${sv}"`,
        { expected: sv, actual: stamp.schemaVersion });
    }
    if (expected != null && String(expected) !== sv) {
      throw fail("SCHEMA_MISMATCH", `caller expected "${expected}", document is "${sv}"`, { expected, actual: sv });
    }
    if (stamp.codecVersion !== CODEC_VERSION) {
      throw fail("UNRECOGNISED_SHAPE", `unknown codecVersion "${stamp.codecVersion}"`, { codecVersion: stamp.codecVersion });
    }
    if (!SHA256_HEX_RE.test(String(stamp.contentHash || ""))) {
      throw fail("UNRECOGNISED_SHAPE", "_codec.contentHash is not a sha256 hex digest");
    }
    const opaquePaths = Array.isArray(stamp.opaquePaths) ? stamp.opaquePaths : [];
    const computed = contentHashOf(doc, opaquePaths);
    if (computed !== stamp.contentHash) {
      throw fail("CONTENT_HASH_MISMATCH", `stored ${stamp.contentHash.slice(0, 12)}…, computed ${computed.slice(0, 12)}…`,
        { storedHash: stamp.contentHash, computedHash: computed });
    }
    const { _codec, ...body } = doc;
    /* withoutPaths with no paths is a deep clone: never mutate the caller's read. */
    const restored = restore(withoutPaths(body, []), {
      nestedArrayPaths: Array.isArray(stamp.nestedArrayPaths) ? stamp.nestedArrayPaths : [],
      bigintPaths: Array.isArray(stamp.bigintPaths) ? stamp.bigintPaths : [],
    });
    const record = typeof decode === "function" ? decode(restored) : restored;
    runValidate(record, "decode");
    return record;
  }

  const codec = Object.freeze({
    schemaVersion: sv, collection: col, required: requiredKeys, maxBytes,
    encode: encodeRecord, decode: decodeDocument,
    validate: typeof validate === "function" ? validate : null,
  });
  registry[sv] = codec;
  return codec;
}

/* ── top-level encode / decode ──────────────────────────────────────── */
function encode(record) {
  if (!isPlainObject(record)) throw fail("SCHEMA_INVALID", "record must be a plain object");
  return codecFor(record.schemaVersion).encode(record);
}
/** Decode a document read from Firestore. Refuses anything without a stamp
 *  or with an unregistered schema — new code must never guess. */
function decode(doc, opts = {}) {
  if (!isPlainObject(doc) || !isPlainObject(doc._codec)) {
    throw fail("UNRECOGNISED_SHAPE", "document carries no _codec stamp — refusing to guess its shape");
  }
  const sv = doc._codec.schemaVersion;
  if (!registry[String(sv)]) {
    throw fail("UNRECOGNISED_SHAPE", `no codec registered for schemaVersion "${sv}"`, { schemaVersion: sv });
  }
  return registry[String(sv)].decode(doc, opts);
}

/* ── deep equality for fixtures ─────────────────────────────────────── */
function deepEqual(a, b, segs = [], diff = []) {
  if (a === b) return true;
  if (typeof a === "bigint" || typeof b === "bigint") {
    if (typeof a === typeof b && a === b) return true;
    diff.push(pointer(segs) || "/"); return false;
  }
  if (typeof a === "number" && typeof b === "number" && Number.isNaN(a) && Number.isNaN(b)) return true;
  if (a instanceof Date && b instanceof Date) {
    if (a.getTime() === b.getTime()) return true;
    diff.push(pointer(segs) || "/"); return false;
  }
  if (isOpaque(a) || isOpaque(b)) {
    if (isOpaque(a) && isOpaque(b) && typeof a.isEqual === "function" && a.isEqual(b)) return true;
    if (canonicalJson(a) === canonicalJson(b)) return true;
    diff.push(pointer(segs) || "/"); return false;
  }
  if (Array.isArray(a) !== Array.isArray(b) || isPlainObject(a) !== isPlainObject(b)) {
    diff.push(pointer(segs) || "/"); return false;
  }
  if (Array.isArray(a)) {
    if (a.length !== b.length) { diff.push(pointer(segs) || "/"); return false; }
    let ok = true;
    a.forEach((x, i) => { if (!deepEqual(x, b[i], segs.concat(String(i)), diff)) ok = false; });
    return ok;
  }
  if (isPlainObject(a)) {
    const ka = Object.keys(a).sort(), kb = Object.keys(b).sort();
    let ok = true;
    for (const k of new Set([...ka, ...kb])) {
      if (!(k in a) || !(k in b)) { diff.push(pointer(segs.concat(k))); ok = false; continue; }
      if (!deepEqual(a[k], b[k], segs.concat(k), diff)) ok = false;
    }
    return ok;
  }
  diff.push(pointer(segs) || "/");
  return false;
}

/** decode(encode(record)) with a deep-equality report, for fixtures. */
function roundTrip(record) {
  try {
    const encoded = encode(record);
    const decoded = decode(encoded);
    const diff = [];
    const equal = deepEqual(record, decoded, [], diff);
    return { ok: true, equal, diff, bytes: encoded._codec.bytes, contentHash: encoded._codec.contentHash };
  } catch (e) {
    return { ok: false, equal: false, diff: [], error: String(e && e.message || e), code: e && e.code || null };
  }
}

/* ── content references ─────────────────────────────────────────────── */
const CONTENT_URI_RE = /^(gs|fake):\/\/[^\s/]+\/\S+$/;

/** Small pointer to a body held by _investorContentStore. This is what a
 *  document keeps INSTEAD of the body. */
function contentRef({ contentHash, bytes, storageUri, mimeType = "application/octet-stream",
  generation = null, encoding = "identity" } = {}) {
  const hash = String(contentHash || "").toLowerCase();
  if (!SHA256_HEX_RE.test(hash)) throw fail("BAD_CONTENT_REF", "contentHash must be a sha256 hex digest");
  const n = Number(bytes);
  if (!(Number.isInteger(n) && n > 0)) throw fail("BAD_CONTENT_REF", "bytes must be a positive integer");
  const uri = String(storageUri || "");
  if (!CONTENT_URI_RE.test(uri)) throw fail("BAD_CONTENT_REF", `storageUri "${uri}" must be gs:// or fake://`);
  if (encoding !== "identity" && encoding !== "gzip") throw fail("BAD_CONTENT_REF", `encoding "${encoding}" unsupported`);
  return { kind: "content_ref", contentHash: hash, bytes: n, storageUri: uri,
    mimeType: String(mimeType || "application/octet-stream"),
    generation: generation == null ? null : String(generation), encoding };
}
function isContentRef(v) {
  return isPlainObject(v) && v.kind === "content_ref" && SHA256_HEX_RE.test(String(v.contentHash || ""))
    && Number.isInteger(v.bytes) && v.bytes > 0 && CONTENT_URI_RE.test(String(v.storageUri || ""));
}

/* ── built-in v1 schemas ────────────────────────────────────────────────
 * Identity transforms today; the limits and required keys are the contract.
 * A manager run stores counters and hashes, never rows — 200 KB is generous
 * for that and far too small for 304 inline decisions, which is the point. */
const BUILTIN = [
  { schemaVersion: "manager-run.v1", collection: "InvestorAI_ManagerRuns",
    required: ["managerRunId", "status", "universeVersion", "universeHash", "eligibleCount"], maxBytes: 200000 },
  { schemaVersion: "manager-decision.v1", collection: "InvestorAI_ManagerDecisions",
    required: ["managerRunId", "symbol", "decision"], maxBytes: 65536 },
  { schemaVersion: "mandate-proposal.v1", collection: "InvestorAI_MandateProposals",
    required: ["proposalId", "proposalHash", "symbol", "decision"] },
  { schemaVersion: "mandate-binding.v1", collection: "InvestorAI_Mandates",
    required: ["mandateVersionId", "mandateSeriesId", "version", "proposalId", "proposalHash"] },
  { schemaVersion: "activation-envelope.v1", collection: "InvestorAI_ActivationEnvelopes",
    required: ["mandateVersionId", "status", "activationSnapshotId"] },
  { schemaVersion: "dossier-version.v1", collection: "InvestorAI_DossierVersions",
    required: ["symbol", "version", "contentHash"] },
  { schemaVersion: "financial-fact.v1", collection: "InvestorAI_FinancialFacts",
    required: ["cik", "concept", "period", "asOfVersion"] },
  { schemaVersion: "evidence-delta.v1", collection: "InvestorAI_EvidenceDeltas" },
  { schemaVersion: "order-set.v1", collection: "InvestorAI_OrderSets" },
  { schemaVersion: "order-leg.v1", collection: "InvestorAI_OrderLegs" },
  { schemaVersion: "broker-event.v1", collection: "InvestorAI_BrokerEvents" },
  { schemaVersion: "capital-reservation.v1", collection: "InvestorAI_CapitalReservations" },
  { schemaVersion: "content-manifest.v1", collection: "InvestorAI_ContentManifests",
    required: ["contentHash", "bytes", "storageUri"] },
];
BUILTIN.forEach(registerCodec);

/* ── self check ─────────────────────────────────────────────────────── */
function selfCheck() {
  const failures = [];
  const check = (name, fn) => {
    try { const r = fn(); if (r === false) failures.push(`${name}: returned false`); }
    catch (e) { failures.push(`${name}: ${String(e && e.message || e)}`); }
  };
  const expectCode = (name, code, fn) => check(name, () => {
    try { fn(); } catch (e) { if (e.code === code) return true; throw new Error(`expected ${code}, got ${e.code}: ${e.message}`); }
    throw new Error(`expected ${code}, nothing thrown`);
  });

  check("nested array round trip", () => {
    const rec = { schemaVersion: "evidence-delta.v1", grid: [[1, 2], [3, [4, "x"]], []], flat: [1, 2], o: { inner: [[true]] } };
    const enc = encode(rec);
    if (!isPlainObject(enc.grid[0]) || enc.grid[0]["1"] !== 2) throw new Error("inner array not index-keyed");
    if (!isPlainObject(enc.grid[1]["1"])) throw new Error("doubly nested array not index-keyed");
    if (!Array.isArray(enc.flat)) throw new Error("top-level array must stay an array");
    const paths = enc._codec.nestedArrayPaths;
    if (!paths.includes("/grid/0") || !paths.includes("/grid/1/1") || !paths.includes("/o/inner/0")) throw new Error(`paths ${paths}`);
    const rt = roundTrip(rec);
    if (!rt.ok || !rt.equal) throw new Error(`round trip diff ${rt.diff} ${rt.error || ""}`);
    return true;
  });
  check("BigInt round trip", () => {
    const rec = { schemaVersion: "order-leg.v1", notionalMicros: 123456789012345678901234n, qtyUnits: -7n, n: { deep: [1n] } };
    const enc = encode(rec);
    if (enc.notionalMicros !== "123456789012345678901234" || enc.qtyUnits !== "-7") throw new Error("bigint not stringified");
    if (!enc._codec.bigintPaths.includes("/n/deep/0")) throw new Error("bigint path missing");
    const dec = decode(enc);
    if (dec.notionalMicros !== 123456789012345678901234n || dec.n.deep[0] !== 1n) throw new Error("bigint not restored");
    const rt = roundTrip(rec);
    if (!rt.equal) throw new Error(`diff ${rt.diff}`);
    return true;
  });
  expectCode("undefined refusal", "UNDEFINED_FIELD", () => encode({ schemaVersion: "order-set.v1", a: { b: undefined } }));
  check("undefined refusal names the path", () => {
    try { encode({ schemaVersion: "order-set.v1", a: { b: undefined } }); }
    catch (e) { return e.path === "/a/b" && e.message.includes("/a/b"); }
    return false;
  });
  expectCode("non-finite refusal", "NON_FINITE_NUMBER", () => encode({ schemaVersion: "order-set.v1", x: [NaN] }));
  expectCode("float at money boundary (fraction)", "FLOAT_AT_MONEY_BOUNDARY", () => encode({ schemaVersion: "order-set.v1", priceCents: 1.5 }));
  expectCode("float at money boundary (integer Number)", "FLOAT_AT_MONEY_BOUNDARY", () => encode({ schemaVersion: "order-set.v1", fees: { totalMicros: 150 } }));
  expectCode("float at money boundary (array element)", "FLOAT_AT_MONEY_BOUNDARY", () => encode({ schemaVersion: "order-set.v1", legsCents: [100] }));
  expectCode("non-canonical money string", "NON_CANONICAL_MONEY_STRING", () => encode({ schemaVersion: "order-set.v1", priceCents: "01.5" }));
  check("money boundary accepts canonical strings and structural counts", () => {
    const enc = encode({ schemaVersion: "order-set.v1", priceCents: "-150", sizeBps: "25", workUnits: 3, rowUnits: 304 });
    return enc.priceCents === "-150" && enc.workUnits === 3;
  });
  check("too-large refusal carries bytes and suggestion", () => {
    try { encode({ schemaVersion: "order-set.v1", blob: "x".repeat(1100000) }); }
    catch (e) {
      return e.code === "DOCUMENT_TOO_LARGE" && e.bytes > 1100000
        && e.suggestion === "store the large part through _investorContentStore and keep a manifest pointer";
    }
    return false;
  });
  check("documentBytes over-estimates JSON length", () => {
    const v = { a: 1, b: { c: "xyz" } };
    return documentBytes(v) === Buffer.byteLength(JSON.stringify(v)) + 3 * PER_FIELD_OVERHEAD_BYTES;
  });
  expectCode("hash mismatch detection", "CONTENT_HASH_MISMATCH", () => {
    const enc = encode({ schemaVersion: "broker-event.v1", seq: 1, payload: { a: [1, 2] } });
    const tampered = JSON.parse(JSON.stringify(enc));
    tampered.payload.a[1] = 3;
    decode(tampered);
  });
  check("hash survives a JSON round trip (as Firestore would return it)", () => {
    const enc = encode({ schemaVersion: "broker-event.v1", seq: 2, z: [[1]], big: 5n, t: "é" });
    const dec = decode(JSON.parse(JSON.stringify(enc)));
    return dec.z[0][0] === 1 && dec.big === 5n && dec.t === "é";
  });
  check("opaque values are excluded from the hash, not from the document", () => {
    const d = new Date(0);
    const enc = encode({ schemaVersion: "broker-event.v1", at: d, n: 1 });
    if (enc.at !== d || !enc._codec.opaquePaths.includes("/at")) return false;
    const asRead = { ...enc, at: { toDate() { return new Date(0); } } };   /* a Timestamp-like */
    return decode(asRead).n === 1;
  });
  expectCode("unrecognised shape: no stamp", "UNRECOGNISED_SHAPE", () => decode({ foo: 1, schemaVersion: "order-set.v1" }));
  expectCode("unrecognised shape: unknown schema", "UNRECOGNISED_SHAPE", () => decode({ _codec: { schemaVersion: "nope.v9", codecVersion: CODEC_VERSION } }));
  expectCode("unrecognised shape: codecFor", "UNRECOGNISED_SHAPE", () => codecFor("nope.v9"));
  expectCode("schema mismatch on decode option", "SCHEMA_MISMATCH", () =>
    decode(encode({ schemaVersion: "order-set.v1", a: 1 }), { schemaVersion: "order-leg.v1" }));
  expectCode("schema mismatch on encode", "SCHEMA_MISMATCH", () => codecFor("order-set.v1").encode({ schemaVersion: "order-leg.v1" }));
  expectCode("missing required", "MISSING_REQUIRED", () => encode({ schemaVersion: "manager-decision.v1", managerRunId: "r1", symbol: "AAPL" }));
  expectCode("cycle refusal", "CYCLIC_VALUE", () => { const a = { schemaVersion: "order-set.v1" }; a.self = a; encode(a); });
  expectCode("CODECS view is read-only", "READ_ONLY", () => { CODECS["x.v1"] = {}; });
  check("collectionFor", () => collectionFor("manager-decision.v1") === "InvestorAI_ManagerDecisions"
    && Object.keys(CODECS).length === BUILTIN.length && !!CODECS["content-manifest.v1"]);
  check("contentRef validation", () => {
    const ref = contentRef({ contentHash: "a".repeat(64), bytes: 10, storageUri: "fake://b/investor-ai/packet/aa/x", generation: 3 });
    if (!isContentRef(ref) || ref.generation !== "3") return false;
    try { contentRef({ contentHash: "zz", bytes: 10, storageUri: "gs://b/k" }); return false; } catch (e) { if (e.code !== "BAD_CONTENT_REF") return false; }
    try { contentRef({ contentHash: "a".repeat(64), bytes: 10, storageUri: "http://b/k" }); return false; } catch (e) { if (e.code !== "BAD_CONTENT_REF") return false; }
    return !isContentRef({ kind: "content_ref" });
  });

  /* The blueprint's sizing rule, asserted rather than assumed. */
  const decisionRow = (i) => ({
    symbol: `SYM${String(i).padStart(4, "0")}`, decision: i % 3 === 0 ? "buy" : "hold",
    confidence: 0.5 + (i % 50) / 100, horizonDays: 20 + (i % 40),
    thesis: `Residual reversal after an idiosyncratic drawdown with stable revenue guidance; row ${i}. `.repeat(6),
    factors: { momentum: (i % 17) / 17, quality: (i % 13) / 13, value: (i % 11) / 11, liquidity: (i % 7) / 7 },
    evidence: [`doc-${i}-a`, `doc-${i}-b`, `doc-${i}-c`], riskFlags: i % 5 === 0 ? ["earnings_within_5d"] : [],
    sizeBps: String(25 + (i % 100)), limitPriceCents: String(1000 + i * 37),
  });
  const rows = Array.from({ length: 304 }, (_, i) => decisionRow(i));
  expectCode("304 inline rows do NOT fit a manager-run.v1", "DOCUMENT_TOO_LARGE", () => encode({
    schemaVersion: "manager-run.v1", managerRunId: "run-1", status: "complete",
    universeVersion: "u-7", universeHash: "b".repeat(64), eligibleCount: 304, rows,
  }));
  check("the same run fits as counters and hashes", () => {
    const enc = encode({ schemaVersion: "manager-run.v1", managerRunId: "run-1", status: "complete",
      universeVersion: "u-7", universeHash: "b".repeat(64), eligibleCount: 304, decisionCount: 304,
      decisionsHash: "c".repeat(64), counts: { buy: 102, hold: 202 } });
    return enc._codec.bytes < 2000;
  });
  check("304 separate manager-decision.v1 documents each fit", () => {
    let maxBytes = 0;
    rows.forEach((r) => {
      const enc = encode({ schemaVersion: "manager-decision.v1", managerRunId: "run-1", ...r });
      if (enc._codec.bytes > maxBytes) maxBytes = enc._codec.bytes;
      const dec = decode(JSON.parse(JSON.stringify(enc)));
      if (dec.symbol !== r.symbol) throw new Error("decision round trip lost the symbol");
    });
    if (maxBytes >= 65536) throw new Error(`largest decision is ${maxBytes} bytes`);
    return true;
  });

  return { pass: failures.length === 0, failures };
}

module.exports = {
  CODEC_VERSION, MAX_DOCUMENT_BYTES, SAFE_DOCUMENT_BYTES, PER_FIELD_OVERHEAD_BYTES,
  MONEY_KEY_RE, STRUCTURAL_COUNT_KEYS,
  documentBytes, canonicalJson, sha256Hex,
  registerCodec, CODECS, codecFor, collectionFor,
  encode, decode, roundTrip, deepEqual,
  contentRef, isContentRef,
  pointer, segments,
  selfCheck,
};
