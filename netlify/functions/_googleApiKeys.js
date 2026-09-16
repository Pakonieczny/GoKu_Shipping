'use strict';

// Google API keys kept in Firestore rather than in Netlify environment
// variables, which are a limited resource on this site.
//
//   Firestore → config/googleApiKeys
//   { "youtubeApiKey": "AIza..." }
//
// A key stored here is only as safe as the restrictions set on it in Google
// Cloud Console. Restrict every key to the single API it serves; that, not
// secrecy of storage, is what limits the damage if one leaks. Values that are
// genuinely secret — OAuth refresh tokens, service-account keys — do not belong
// in this document; they stay in their own sealed records.
//
// An environment variable of the same upper-snake name still wins when set, so
// nothing that already works changes.

const DOC_PATH = 'config/googleApiKeys';
const TTL_MS = 5 * 60 * 1000;

let cache = null, cachedAt = 0, inFlight = null;

function envNameFor(field) {
  return field.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toUpperCase();
}

async function loadDocument(deps) {
  const db = deps && deps.db ? deps.db : require('./firebaseAdmin').firestore();
  const snapshot = await db.doc(DOC_PATH).get();
  return snapshot.exists ? snapshot.data() || {} : {};
}

// One read per process per five minutes, shared by concurrent callers. A key
// added in Firestore appears without a redeploy.
async function keys(deps) {
  if (cache && Date.now() - cachedAt < TTL_MS) return cache;
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try { cache = await loadDocument(deps); cachedAt = Date.now(); return cache; }
    // Firestore being unreachable must not mask a key that is in the
    // environment; callers fall back to it below.
    catch (error) { cache = null; cachedAt = 0; return { _error: String(error && error.message || error) }; }
    finally { inFlight = null; }
  })();
  return inFlight;
}

async function googleApiKey(field, deps) {
  const env = (deps && deps.env) || process.env;
  const fromEnv = env[envNameFor(field)];
  if (typeof fromEnv === 'string' && fromEnv.trim()) return fromEnv.trim();
  const stored = await keys(deps);
  const value = stored && stored[field];
  return typeof value === 'string' && value.trim() ? value.trim() : null;
}

// Why a key is missing is the difference between "add it" and "Firestore is
// down"; a caller that reports the gap needs to tell those apart.
async function googleApiKeyStatus(field, deps) {
  const env = (deps && deps.env) || process.env;
  const name = envNameFor(field);
  if (typeof env[name] === 'string' && env[name].trim()) return { key: env[name].trim(), source: 'environment ' + name };
  const stored = await keys(deps);
  if (stored && stored._error) return { key: null, source: null, error: stored._error };
  const value = stored && stored[field];
  return typeof value === 'string' && value.trim()
    ? { key: value.trim(), source: 'Firestore ' + DOC_PATH + '.' + field }
    : { key: null, source: null, error: null };
}

function resetCache() { cache = null; cachedAt = 0; inFlight = null; }

module.exports = { googleApiKey, googleApiKeyStatus, resetCache, envNameFor, DOC_PATH };
