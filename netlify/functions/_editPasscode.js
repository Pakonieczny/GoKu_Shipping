// netlify/functions/_editPasscode.js
// ─────────────────────────────────────────────────────────────────────────────
// The passcode for the Brites Adwords console (googleAdsAutopilotApi → Kick), its
// background worker and its check pages (_adsCheckGate.js).
// Kept in Firestore beside the site's other runtime settings, because Netlify
// environment variables are a limited resource on this site:
//
//   Firestore → config/editPasscode
//   { passcode: "k7qm-3xva-9wpd", createdAt, note }
//
// resolve() answers { value, source: "env" | "firebase" | "none", error? }:
//   1. EDIT_PASSCODE in the environment (trimmed, surrounding quotes removed)
//      still wins when set, so nothing that already works changes.
//   2. Otherwise the document's `passcode` field. Read at most once a minute per
//      warm lambda, one read shared by concurrent callers, so a passcode changed
//      in the Firebase console takes effect within a minute, without a deploy.
//   3. No document at all (first use) → it is created atomically with a
//      generated passcode, which is then used. create() fails when the document
//      already exists, so two cold lambdas cannot both write one: the loser reads
//      the winner's passcode back.
//   A document whose passcode is empty or missing, or any Firestore error, gives
//   no passcode (source "none") and every caller fails closed: reads only. A
//   passcode is never generated over an existing document or after an error.
//
// The passcode is never logged, returned in a response or put in an error message.
// ─────────────────────────────────────────────────────────────────────────────
"use strict";

const crypto = require("crypto");

const COLLECTION = "config", DOC_ID = "editPasscode", FIELD = "passcode";
const DOC_PATH = COLLECTION + "/" + DOC_ID;
const TTL_MS = 60 * 1000;
const ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789"; // no i, l, o, 0 or 1 to misread
const NOTE = "Passcode for the Brites Adwords console and its check pages. Change it here any time; the site picks up a change within a minute.";

let cache = null, cachedAt = 0, inFlight = null;

// A value pasted as "abc" or with a trailing space still means abc.
function clean(v) {
  if (typeof v === "number" && Number.isFinite(v)) v = String(v);
  if (typeof v !== "string") return "";
  return v.trim().replace(/^["']|["']$/g, "").trim();
}
function envPasscode(env) { return clean((env || process.env).EDIT_PASSCODE); }

// Constant-time comparison of what a caller offered (trimmed) with the passcode.
function sameSecret(a, b) {
  try { a = String(a == null ? "" : a).trim(); b = String(b == null ? "" : b); }
  catch (_) { return false; }                          // (a JSON object with its own toString, {"toString":1}, cannot be turned into text: that is a wrong key, not a crash)
  if (!a || !b) return false;
  const h = x => crypto.createHash("sha256").update(x).digest();
  return crypto.timingSafeEqual(h(a), h(b));
}

// Three groups of four, e.g. "k7qm-3xva-9wpd" (about 59 bits).
function generate() {
  const groups = [];
  for (let g = 0; g < 3; g++) {
    let s = "";
    for (let i = 0; i < 4; i++) s += ALPHABET[crypto.randomInt(ALPHABET.length)];
    groups.push(s);
  }
  return groups.join("-");
}

function alreadyExists(e) {
  return !!e && (e.code === 6 || e.code === "already-exists" || e.code === "ALREADY_EXISTS" ||
    /ALREADY_EXISTS|already exists/i.test(String(e.message || "")));
}
function fromDocument(snapshot) {
  const data = (typeof snapshot.data === "function" && snapshot.data()) || {};
  const value = clean(data[FIELD]);
  return value ? { value, source: "firebase" } : { value: "", source: "none" };
}
// Fail closed. Firestore errors do not carry document data, but a candidate passcode is
// scrubbed from the message anyway so it can never reach a log or a caller.
function failure(stage, error, secret) {
  let message = String((error && (error.message || error.code)) || error || "unknown error");
  if (secret) message = message.split(secret).join("[passcode]");
  const out = { value: "", source: "none", error: "Firestore " + DOC_PATH + " " + stage + " failed: " + message.slice(0, 300) };
  console.warn("[editPasscode] " + out.error + " (changes stay locked: reads only)");
  return out;
}

async function load(deps) {
  let ref;
  try {
    const db = deps.db || require("./firebaseAdmin").firestore();
    ref = db.collection(COLLECTION).doc(DOC_ID);
    const snapshot = await ref.get();
    if (snapshot.exists) return fromDocument(snapshot);
  } catch (error) { return failure("read", error); }
  const value = generate();
  try {
    await ref.create({ [FIELD]: value, createdAt: new Date(), note: NOTE });
    console.log("[editPasscode] created " + DOC_PATH + " with a generated passcode; read it in the Firebase console");
    return { value, source: "firebase" };
  } catch (error) {
    if (!alreadyExists(error)) return failure("create", error, value);
  }
  // Another request created it first: use that passcode.
  try {
    const snapshot = await ref.get();
    return snapshot.exists ? fromDocument(snapshot) : failure("read", new Error("the document was removed while it was being created"), value);
  } catch (error) { return failure("read", error, value); }
}

async function resolve(deps) {
  deps = deps || {};
  const fromEnv = envPasscode(deps.env);
  if (fromEnv) return { value: fromEnv, source: "env" };
  if (cache && Date.now() - cachedAt < TTL_MS) return { ...cache };
  if (!inFlight) inFlight = (async () => {
    try {
      const out = await load(deps);
      // A failed read is not remembered: the next request tries Firestore again.
      if (out.error) { cache = null; cachedAt = 0; } else { cache = out; cachedAt = Date.now(); }
      return out;
    } finally { inFlight = null; }
  })();
  return { ...(await inFlight) };
}

function resetCache() { cache = null; cachedAt = 0; inFlight = null; }

module.exports = { resolve, sameSecret, envPasscode, generate, resetCache, DOC_PATH, FIELD, TTL_MS, ALPHABET, NOTE };
