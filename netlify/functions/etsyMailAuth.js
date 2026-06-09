/*  netlify/functions/etsyMailAuth.js
 *
 *  v4.1 — Multi-operator auth endpoint.
 *
 *  Username + password login, 30-day Firestore-backed sessions, and
 *  owner-only operator management. Builds on the existing
 *  EtsyMail_Operators collection (which previously held only
 *  role/displayName) by adding password fields and introducing
 *  EtsyMail_Sessions for issued tokens.
 *
 *  ─── Operator schema (after this change) ───────────────────────────
 *
 *  EtsyMail_Operators/{username} = {
 *    username        : "paul",         // doc id; canonical login name
 *    displayName     : "Paul K",       // shown in UI, used in audit rows
 *    role            : "owner" | "operator",
 *    passwordHash    : <hex>,          // pbkdf2-sha512(password, salt, 200000, 64)
 *    salt            : <hex>,          // 32 random bytes per operator
 *    iterations      : 200000,
 *    digest          : "sha512",
 *    createdAt       : Timestamp,
 *    createdBy       : "<owner username>",
 *    lastLoginAt     : Timestamp | null,
 *    revokedAt       : Timestamp | null  // soft delete
 *  }
 *
 *  EtsyMail_Sessions/{token} = {
 *    username        : "paul",
 *    createdAt       : Timestamp,
 *    expiresAt       : Timestamp,      // createdAt + 30 days
 *    lastSeenAt      : Timestamp,      // bumped on each successful auth
 *    userAgent       : <string|null>   // best-effort browser fingerprint
 *  }
 *
 *  ─── Ops ────────────────────────────────────────────────────────────
 *
 *    POST { op:"login", username, password }
 *      Public (gated only by the X-EtsyMail-Secret bootstrap header).
 *      Returns { ok:true, sessionToken, username, displayName, role,
 *                expiresAtMs } or 401 with { error }.
 *
 *    POST { op:"logout" }
 *      Authenticated (X-EtsyMail-Session). Deletes the session doc and
 *      clears the role/session caches.
 *
 *    POST { op:"currentUser" }
 *      Authenticated. Returns who the session belongs to. Used by the
 *      front-end on page load to validate a stored token and populate
 *      operator state.
 *
 *    POST { op:"setMyPassword", currentPassword, newPassword }
 *      Authenticated. Any user can change their own password by
 *      proving knowledge of the current one. Doesn't require owner.
 *      All other sessions for that user are revoked.
 *
 *    POST { op:"listOperators" }
 *      Owner-only. Returns the operator roster minus password material.
 *
 *    POST { op:"addOperator", username, displayName, role, password }
 *      Owner-only. Creates an EtsyMail_Operators doc. role must be
 *      "owner" or "operator". Idempotent: if username exists and is
 *      revoked, the row is rehydrated; if username exists and is
 *      active, returns 409.
 *
 *    POST { op:"removeOperator", username }
 *      Owner-only. Soft-deletes (sets revokedAt). Active sessions for
 *      that user are deleted so they're booted within ~60s. Owner
 *      cannot remove the last remaining owner (returns 400).
 *
 *    POST { op:"resetOperatorPassword", username, newPassword }
 *      Owner-only. Sets a new password on behalf of an operator (e.g.
 *      they forgot it). Their other sessions are revoked.
 *
 *  ─── Security notes ────────────────────────────────────────────────
 *
 *    - All POSTs require the X-EtsyMail-Secret bootstrap header. Auth
 *      endpoints are NOT publicly reachable — without the dev-time
 *      secret, no operator can even attempt to log in. This is by
 *      design: it prevents probing usernames over the public internet.
 *    - Failed-login attempts are not rate-limited at this layer (could
 *      add later). Per-operator backoff would prevent brute force on
 *      small wordlists; relying on the secret as the outer gate is
 *      acceptable for a closed-team tool.
 *    - All password material flows through hashSecret/verifySecret
 *      below — same PBKDF2-SHA512 + per-secret salt + constant-time
 *      compare pattern used for the master-purge password.
 *    - Session tokens are 32 random bytes (256 bits) base64url-encoded.
 *      Treated as opaque by callers; not signed (no JWT). Trust comes
 *      from possession + Firestore lookup.
 */

"use strict";

const admin = require("./firebaseAdmin");
const crypto = require("crypto");
const { CORS, requireExtensionAuth } = require("./_etsyMailAuth");
const {
  requireSession,
  requireOwnerSession,
  invalidateRoleCache,
  invalidateSessionCache,
  OPERATORS_COLL,
  SESSIONS_COLL
} = require("./_etsyMailRoles");

const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const AUDIT_COLL = "EtsyMail_Audit";

const PBKDF2_ITERATIONS = 200_000;
const PBKDF2_KEY_BYTES  = 64;
const PBKDF2_DIGEST     = "sha512";
const SALT_BYTES        = 32;

// 30-day session lifetime, expressed in ms.
const SESSION_LIFETIME_MS = 30 * 24 * 60 * 60 * 1000;

// ─── Invite system (v6) ──────────────────────────────────────────────────
const INVITES_COLL = "EtsyMail_Invites";
// Invites expire after 14 days if unused. Enforced on validate + signup.
const INVITE_LIFETIME_MS = 14 * 24 * 60 * 60 * 1000;
// Unambiguous code alphabet (no 0/O/1/I/L). Code shape: XXXXX-XXXXX.
// 31^10 ≈ 8.2e14 keyspace; combined with the X-EtsyMail-Secret outer gate
// this is not feasibly guessable. (Still: add login/validate rate-limiting.)
const CODE_ALPHABET = "ABCDEFGHJKMNPQRSTUVWXYZ23456789";

// ─── Helpers ────────────────────────────────────────────────────────────

function json(statusCode, body) {
  return {
    statusCode,
    headers: { ...CORS, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}
function bad(msg, code = 400) { return json(code, { error: msg }); }

/** Hash a secret with a fresh random salt. Same shape as the master-purge
 *  password helper in etsyMailThreads.js; duplicated here rather than
 *  imported because both files are reachable independently and we want
 *  to avoid an unnecessary cross-file dependency. */
function hashSecret(plaintext) {
  const salt = crypto.randomBytes(SALT_BYTES);
  const hash = crypto.pbkdf2Sync(
    String(plaintext),
    salt,
    PBKDF2_ITERATIONS,
    PBKDF2_KEY_BYTES,
    PBKDF2_DIGEST
  );
  return {
    hash       : hash.toString("hex"),
    salt       : salt.toString("hex"),
    iterations : PBKDF2_ITERATIONS,
    digest     : PBKDF2_DIGEST
  };
}

/** Constant-time compare a plaintext attempt against a stored
 *  { hash, salt, iterations, digest } record. Returns boolean. */
function verifySecret(plaintext, stored) {
  if (!stored || !stored.hash || !stored.salt) return false;
  const iterations = stored.iterations || PBKDF2_ITERATIONS;
  const digest     = stored.digest     || PBKDF2_DIGEST;
  let attempt;
  try {
    attempt = crypto.pbkdf2Sync(
      String(plaintext),
      Buffer.from(stored.salt, "hex"),
      iterations,
      PBKDF2_KEY_BYTES,
      digest
    );
  } catch {
    return false;
  }
  let storedBuf;
  try { storedBuf = Buffer.from(stored.hash, "hex"); }
  catch { return false; }
  if (attempt.length !== storedBuf.length) return false;
  return crypto.timingSafeEqual(attempt, storedBuf);
}

/** Generate a fresh session token: 32 random bytes, base64url. */
function generateSessionToken() {
  return crypto.randomBytes(32).toString("base64url");
}

/** Validate username syntax — lowercase alphanumeric + underscore + dash,
 *  3-32 chars. Keeps doc IDs Firestore-safe and prevents weird login
 *  collisions like trailing-whitespace usernames. */
function isValidUsername(s) {
  return typeof s === "string" && /^[a-z0-9_-]{3,32}$/.test(s);
}

/** The v6 auth UI speaks master_admin/user; storage + login + the legacy
 *  operator panel speak owner/operator. Translate at the boundary so both
 *  layers stay coherent and the canonical stored role never changes. */
function toCanonicalRole(r) {
  const s = String(r || "").toLowerCase();
  if (s === "owner" || s === "master_admin") return "owner";
  return "operator"; // "user", "operator", or anything unexpected
}
function toV6Role(r) {
  return String(r || "").toLowerCase() === "owner" ? "master_admin" : "user";
}

/** Cryptographically-random invite code, shape XXXXX-XXXXX. */
function generateInviteCode() {
  const pick = () => CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)];
  let a = "", b = "";
  for (let i = 0; i < 5; i++) a += pick();
  for (let i = 0; i < 5; i++) b += pick();
  return `${a}-${b}`;
}

/** An invite is redeemable only if it isn't used, revoked, or expired. */
function inviteIsPending(inv, now = Date.now()) {
  if (!inv) return false;
  if (inv.used || inv.usedAt) return false;
  if (inv.revokedAt) return false;
  const expMs = inv.expiresAt && inv.expiresAt.toMillis ? inv.expiresAt.toMillis() : 0;
  if (expMs && expMs < now) return false;
  return true;
}

async function writeAudit({ eventType, actor, payload = {} }) {
  try {
    await db.collection(AUDIT_COLL).add({
      threadId : null,
      draftId  : null,
      eventType,
      actor    : actor || "system:auth",
      payload,
      createdAt: FV.serverTimestamp()
    });
  } catch (e) {
    console.warn("auth audit write failed:", e.message);
  }
}

async function deleteSessionsForUser(username) {
  const snap = await db.collection(SESSIONS_COLL)
    .where("username", "==", username)
    .limit(500)
    .get();
  if (snap.empty) return 0;
  const batch = db.batch();
  for (const d of snap.docs) {
    batch.delete(d.ref);
    invalidateSessionCache(d.id);
  }
  await batch.commit();
  return snap.size;
}

// ─── Op handlers ────────────────────────────────────────────────────────

async function handleLogin(body) {
  const username = String(body.username || "").trim().toLowerCase();
  const password = String(body.password || "");
  const userAgent = String(body.userAgent || "").slice(0, 200) || null;

  if (!username || !password) return bad("Username and password required");

  const opSnap = await db.collection(OPERATORS_COLL).doc(username).get();
  if (!opSnap.exists) {
    // Generic message — don't leak whether the username exists.
    await writeAudit({ eventType: "login_failed", actor: username, payload: { reason: "no_such_user" } });
    return json(401, { error: "Invalid username or password" });
  }
  const op = opSnap.data() || {};
  if (op.revokedAt) {
    await writeAudit({ eventType: "login_failed", actor: username, payload: { reason: "revoked" } });
    return json(401, { error: "Invalid username or password" });
  }
  if (!op.passwordHash || !op.salt) {
    // This operator was created in the v4.0 era before passwords. Reject
    // with a clear message so the owner knows to set a password for them.
    await writeAudit({ eventType: "login_failed", actor: username, payload: { reason: "no_password_set" } });
    return json(401, { error: "No password configured. Ask the owner to set one." });
  }

  const ok = verifySecret(password, {
    hash: op.passwordHash, salt: op.salt,
    iterations: op.iterations, digest: op.digest
  });
  if (!ok) {
    await writeAudit({ eventType: "login_failed", actor: username, payload: { reason: "bad_password" } });
    return json(401, { error: "Invalid username or password" });
  }

  // Issue session.
  const token = generateSessionToken();
  const expiresAtMs = Date.now() + SESSION_LIFETIME_MS;
  const expiresAt = admin.firestore.Timestamp.fromMillis(expiresAtMs);

  await db.collection(SESSIONS_COLL).doc(token).set({
    username,
    createdAt : FV.serverTimestamp(),
    expiresAt,
    lastSeenAt: FV.serverTimestamp(),
    userAgent
  });
  await db.collection(OPERATORS_COLL).doc(username).set({
    lastLoginAt: FV.serverTimestamp()
  }, { merge: true });

  await writeAudit({
    eventType: "login_success",
    actor: username,
    payload: { tokenPrefix: token.slice(0, 8), userAgent }
  });

  return json(200, {
    ok          : true,
    sessionToken: token,
    username,
    displayName : op.displayName || username,
    role        : op.role || "operator",
    expiresAtMs
  });
}

async function handleLogout(event) {
  const token = (event.headers && (event.headers["x-etsymail-session"] || event.headers["X-EtsyMail-Session"])) || null;
  if (!token) return bad("No session token to log out");
  // Don't fail if the token is already gone — logout should be idempotent.
  const ref = db.collection(SESSIONS_COLL).doc(token);
  let username = null;
  try {
    const snap = await ref.get();
    if (snap.exists) username = (snap.data() || {}).username || null;
    if (snap.exists) await ref.delete();
  } catch (e) {
    console.warn("logout delete failed:", e.message);
  }
  invalidateSessionCache(token);
  await writeAudit({ eventType: "logout", actor: username || "unknown" });
  return json(200, { ok: true });
}

async function handleCurrentUser(event) {
  const sess = await requireSession(event);
  if (!sess.ok) return json(401, { error: "Not authenticated", reason: sess.reason });
  return json(200, {
    ok: true,
    username   : sess.username,
    displayName: sess.displayName,
    role       : sess.role
  });
}

async function handleSetMyPassword(event, body) {
  const sess = await requireSession(event);
  if (!sess.ok) return json(401, { error: "Not authenticated", reason: sess.reason });

  const cur = String(body.currentPassword || "");
  const next = String(body.newPassword || "");
  if (!cur || !next) return bad("currentPassword and newPassword required");
  if (next.length < 8) return bad("New password must be at least 8 characters");

  const ref = db.collection(OPERATORS_COLL).doc(sess.username);
  const snap = await ref.get();
  if (!snap.exists) return bad("Operator record missing", 404);
  const op = snap.data() || {};
  const verified = verifySecret(cur, {
    hash: op.passwordHash, salt: op.salt,
    iterations: op.iterations, digest: op.digest
  });
  if (!verified) {
    await writeAudit({ eventType: "password_change_failed", actor: sess.username });
    return json(401, { error: "Current password is incorrect" });
  }

  const fresh = hashSecret(next);
  await ref.set({
    passwordHash: fresh.hash,
    salt        : fresh.salt,
    iterations  : fresh.iterations,
    digest      : fresh.digest,
    passwordChangedAt: FV.serverTimestamp()
  }, { merge: true });

  // Revoke other sessions but keep THIS session live so the operator
  // doesn't get kicked out of the tab they're using.
  const currentToken = event.headers["x-etsymail-session"] || event.headers["X-EtsyMail-Session"];
  const others = await db.collection(SESSIONS_COLL)
    .where("username", "==", sess.username)
    .get();
  const batch = db.batch();
  let killed = 0;
  for (const d of others.docs) {
    if (d.id === currentToken) continue;
    batch.delete(d.ref);
    invalidateSessionCache(d.id);
    killed++;
  }
  if (killed) await batch.commit();

  await writeAudit({
    eventType: "password_changed",
    actor: sess.username,
    payload: { otherSessionsRevoked: killed }
  });
  return json(200, { ok: true, otherSessionsRevoked: killed });
}

async function handleListOperators(event) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const snap = await db.collection(OPERATORS_COLL).get();
  const ops = [];
  for (const d of snap.docs) {
    const data = d.data() || {};
    ops.push({
      username    : d.id,
      displayName : data.displayName || d.id,
      role        : data.role || "operator",
      createdAt   : data.createdAt && data.createdAt.toMillis ? data.createdAt.toMillis() : null,
      lastLoginAt : data.lastLoginAt && data.lastLoginAt.toMillis ? data.lastLoginAt.toMillis() : null,
      revoked     : !!data.revokedAt,
      hasPassword : !!(data.passwordHash && data.salt)
    });
  }
  ops.sort((a, b) => (a.username || "").localeCompare(b.username || ""));
  return json(200, { ok: true, operators: ops });
}

async function handleAddOperator(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const username = String(body.username || "").trim().toLowerCase();
  const displayName = String(body.displayName || "").trim() || username;
  const role = String(body.role || "operator").toLowerCase();
  const password = String(body.password || "");

  if (!isValidUsername(username)) {
    return bad("Username must be 3-32 chars, lowercase letters/digits/underscore/dash");
  }
  if (!["owner", "operator"].includes(role)) {
    return bad("role must be 'owner' or 'operator'");
  }
  if (password.length < 8) {
    return bad("Password must be at least 8 characters");
  }

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const existing = await ref.get();
  if (existing.exists && !(existing.data() || {}).revokedAt) {
    return json(409, { error: `Operator '${username}' already exists` });
  }

  const fresh = hashSecret(password);
  // v4.1.1 — Use a full-overwrite set() (merge:false) to drop ANY
  // prior fields on this doc. The previous version mixed merge:false
  // with FV.delete(), which Firestore rejects ("FieldValue.delete()
  // can only be used in update() or set() with {merge:true}"). The
  // full-overwrite approach is cleaner anyway: a re-added operator
  // gets a fresh, well-formed record with no leftover fields from
  // any prior soft-revoked state.
  await ref.set({
    username,
    displayName,
    role,
    passwordHash: fresh.hash,
    salt        : fresh.salt,
    iterations  : fresh.iterations,
    digest      : fresh.digest,
    createdAt   : FV.serverTimestamp(),
    createdBy   : sess.username
    // (no revokedAt — by omission, the field is absent on the new doc)
  }, { merge: false });

  invalidateRoleCache(username);

  await writeAudit({
    eventType: "operator_added",
    actor: sess.username,
    payload: { username, displayName, role }
  });
  return json(200, { ok: true, username });
}

async function handleRemoveOperator(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  // v4.1.1 — Remove must be permissive about what username it accepts.
  // The doc ID in Firestore is whatever string was used to create the
  // record, which may include legacy entries with capitals or spaces
  // (e.g. an operator created out-of-band before username validation
  // existed, or via direct Firestore-console editing). The strict
  // validator is right for ADD (we want clean data going in) but
  // wrong for REMOVE (we need to clean out whatever's already there).
  // We only require:
  //   - non-empty after trim
  //   - <= 200 chars (Firestore doc ID limit is 1500 bytes; 200 chars
  //     is plenty and protects against absurd input)
  // We do NOT lowercase — Firestore doc IDs are case-sensitive, and
  // lowercasing the lookup would miss "Paul K" while finding "paul".
  const username = String(body.username || "").trim();
  if (!username) return bad("username required");
  if (username.length > 200) return bad("username too long");

  if (username === sess.username) {
    return bad("You cannot remove your own account");
  }

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const snap = await ref.get();
  if (!snap.exists) return bad("No such operator", 404);
  const data = snap.data() || {};

  if (data.role === "owner") {
    // Don't let the last remaining owner be removed.
    const owners = await db.collection(OPERATORS_COLL).where("role", "==", "owner").get();
    const activeOwners = owners.docs.filter(d => !(d.data() || {}).revokedAt);
    // Count this doc as one of the active owners only if it's currently
    // active; if it's already revoked, removing it again doesn't reduce
    // the active-owner count.
    const isCurrentlyActive = !data.revokedAt;
    const activeCountAfter = activeOwners.length - (isCurrentlyActive ? 1 : 0);
    if (activeCountAfter < 1) {
      return bad("Cannot remove the last remaining active owner");
    }
  }

  // For malformed legacy docs (e.g. ones lacking passwordHash / created
  // outside the addOperator path), do a HARD delete instead of soft-
  // revoke. Soft-revoke leaves the row in the listOperators output
  // forever, which is what's frustrating you right now. Real operator
  // accounts that have been used (passwordHash present + a lastLoginAt)
  // get soft-revoked so the audit trail is preserved.
  const isLegacyMalformed = !data.passwordHash || !data.salt;
  if (isLegacyMalformed) {
    await ref.delete();
  } else {
    await ref.set({
      revokedAt: FV.serverTimestamp(),
      revokedBy: sess.username
    }, { merge: true });
  }

  invalidateRoleCache(username);
  const killedSessions = await deleteSessionsForUser(username);

  await writeAudit({
    eventType: "operator_removed",
    actor: sess.username,
    payload: { username, killedSessions, hardDeleted: isLegacyMalformed }
  });
  return json(200, { ok: true, killedSessions, hardDeleted: isLegacyMalformed });
}

async function handleResetOperatorPassword(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  // v4.1.1 — Same permissive-lookup rule as handleRemoveOperator.
  // We're operating on an existing doc by ID; that ID can be anything
  // the original creator put there. Strict validation belongs on the
  // add path, not on lookups against existing rows.
  const username = String(body.username || "").trim();
  if (!username) return bad("username required");
  if (username.length > 200) return bad("username too long");
  const newPassword = String(body.newPassword || "");
  if (newPassword.length < 8) return bad("New password must be at least 8 characters");

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const snap = await ref.get();
  if (!snap.exists) return bad("No such operator", 404);

  const fresh = hashSecret(newPassword);
  await ref.set({
    passwordHash: fresh.hash,
    salt        : fresh.salt,
    iterations  : fresh.iterations,
    digest      : fresh.digest,
    passwordChangedAt: FV.serverTimestamp(),
    passwordChangedBy: sess.username
  }, { merge: true });

  const killed = await deleteSessionsForUser(username);

  await writeAudit({
    eventType: "operator_password_reset",
    actor: sess.username,
    payload: { username, killedSessions: killed }
  });
  return json(200, { ok: true, killedSessions: killed });
}

// ─── v6 invite + team handlers ────────────────────────────────────────────

/** Public (secret-gated). Tells the gate whether this is a fresh install
 *  (no operators yet) and surfaces any pending owner invites as setup links. */
async function handleSetupState() {
  const opsSnap = await db.collection(OPERATORS_COLL).get();
  const activeOps = opsSnap.docs.filter(d => !(d.data() || {}).revokedAt);
  const needsSetup = activeOps.length === 0;

  const now = Date.now();
  const invSnap = await db.collection(INVITES_COLL).get();
  const masterLinks = invSnap.docs
    .map(d => d.data() || {})
    .filter(v => inviteIsPending(v, now) && toCanonicalRole(v.role) === "owner")
    .map(v => ({ code: v.code, forName: v.forName || "" }));

  return json(200, { ok: true, needsSetup, masterLinks });
}

/** Public (secret-gated). Username availability check during invite signup,
 *  before any session exists. Any existing doc id (even soft-revoked) blocks
 *  reuse so we never resurrect a stale audit identity. */
async function handleCheckUsername(body) {
  const username = String(body.username || "").trim().toLowerCase();
  if (!isValidUsername(username)) {
    return json(200, { ok: true, available: false, reason: "invalid", suggestions: [] });
  }
  const snap = await db.collection(OPERATORS_COLL).doc(username).get();
  if (!snap.exists) return json(200, { ok: true, available: true, suggestions: [] });

  const suggestions = [];
  for (let i = 0; suggestions.length < 3 && i < 60; i++) {
    const cand = `${username}${crypto.randomInt(10, 100)}`.slice(0, 32);
    if (!isValidUsername(cand) || suggestions.includes(cand)) continue;
    const cs = await db.collection(OPERATORS_COLL).doc(cand).get();
    if (!cs.exists) suggestions.push(cand);
  }
  return json(200, { ok: true, available: false, suggestions });
}

/** Public (secret-gated). The invitee has no session yet, so this can't
 *  require one. Returns {valid:false} (not an error) for bad/used codes so
 *  the UI shows the friendly "not valid or used" hint. */
async function handleValidateInvite(body) {
  const code = String(body.code || "").trim().toUpperCase();
  if (!code) return json(200, { ok: true, valid: false });
  const snap = await db.collection(INVITES_COLL).doc(code).get();
  if (!snap.exists) return json(200, { ok: true, valid: false });
  const inv = snap.data() || {};
  if (!inviteIsPending(inv)) return json(200, { ok: true, valid: false });
  return json(200, { ok: true, valid: true, role: toV6Role(inv.role), forName: inv.forName || "" });
}

async function handleCreateInvite(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const role = toCanonicalRole(body.role);
  const forName = String(body.forName || "").trim().slice(0, 80);

  // Mint a unique code (retry on the astronomically unlikely collision).
  let code = generateInviteCode();
  for (let i = 0; i < 5; i++) {
    const existing = await db.collection(INVITES_COLL).doc(code).get();
    if (!existing.exists) break;
    code = generateInviteCode();
  }

  const expiresAt = admin.firestore.Timestamp.fromMillis(Date.now() + INVITE_LIFETIME_MS);
  await db.collection(INVITES_COLL).doc(code).set({
    code,
    role,
    forName: forName || null,
    createdAt: FV.serverTimestamp(),
    createdBy: sess.username,
    expiresAt,
    used: false,
    usedAt: null,
    usedBy: null,
    revokedAt: null
  }, { merge: false });

  await writeAudit({
    eventType: "invite_created",
    actor: sess.username,
    payload: { code, role, forName: forName || null }
  });

  return json(200, { ok: true, code, role: toV6Role(role), forName: forName || "" });
}

async function handleListInvites(event) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const snap = await db.collection(INVITES_COLL).get();
  const now = Date.now();
  const invites = [];
  for (const d of snap.docs) {
    const v = d.data() || {};
    if (!inviteIsPending(v, now)) continue;   // pending only — matches the UI header
    invites.push({
      code: v.code || d.id,
      role: toV6Role(v.role),
      forName: v.forName || "",
      by: v.createdBy || "—"
    });
  }
  invites.sort((a, b) => (a.code || "").localeCompare(b.code || ""));
  return json(200, { ok: true, invites });
}

async function handleRevokeInvite(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const code = String(body.code || "").trim().toUpperCase();
  if (!code) return bad("code required");
  const ref = db.collection(INVITES_COLL).doc(code);
  const snap = await ref.get();
  if (!snap.exists) return bad("No such invite", 404);

  await ref.set({ revokedAt: FV.serverTimestamp(), revokedBy: sess.username }, { merge: true });
  await writeAudit({ eventType: "invite_revoked", actor: sess.username, payload: { code } });
  return json(200, { ok: true });
}

/** Public (secret-gated). Redeems an invite and creates the operator. The
 *  invite-consume + account-create run in a transaction so a code can't be
 *  redeemed twice under a race, and no half-account is ever left behind.
 *  Logs the new operator straight in (the UI expects a session back). */
async function handleSignup(body) {
  const code = String(body.invite || "").trim().toUpperCase();
  const username = String(body.username || "").trim().toLowerCase();
  const displayName = String(body.displayName || "").trim() || username;
  const password = String(body.password || "");
  const userAgent = String(body.userAgent || "").slice(0, 200) || null;

  if (!code) return json(400, { error: "An invite is required.", reason: "INVITE_REQUIRED" });
  if (!isValidUsername(username)) {
    return bad("Username must be 3-32 chars, lowercase letters/digits/underscore/dash");
  }
  if (password.length < 8) return bad("Password must be at least 8 characters");

  const inviteRef = db.collection(INVITES_COLL).doc(code);
  const opRef = db.collection(OPERATORS_COLL).doc(username);
  const fresh = hashSecret(password);

  let canonicalRole, createdBy;
  try {
    const result = await db.runTransaction(async (tx) => {
      const invSnap = await tx.get(inviteRef);
      if (!invSnap.exists) { const e = new Error("That invite isn't valid or has already been used."); e._reason = "INVITE_REQUIRED"; throw e; }
      const inv = invSnap.data() || {};
      if (!inviteIsPending(inv)) { const e = new Error("That invite isn't valid or has already been used."); e._reason = "INVITE_REQUIRED"; throw e; }

      const opSnap = await tx.get(opRef);
      if (opSnap.exists) { const e = new Error("That username is taken."); e._reason = "USERNAME_TAKEN"; throw e; }

      const r = toCanonicalRole(inv.role);
      tx.set(opRef, {
        username,
        displayName,
        role: r,
        passwordHash: fresh.hash,
        salt        : fresh.salt,
        iterations  : fresh.iterations,
        digest      : fresh.digest,
        createdAt   : FV.serverTimestamp(),
        createdBy   : inv.createdBy || `invite:${code}`,
        signedUpViaInvite: code
      });
      tx.set(inviteRef, { used: true, usedAt: FV.serverTimestamp(), usedBy: username }, { merge: true });
      return { role: r, by: inv.createdBy || null };
    });
    canonicalRole = result.role; createdBy = result.by;
  } catch (e) {
    if (e._reason === "INVITE_REQUIRED") return json(400, { error: e.message, reason: "INVITE_REQUIRED" });
    if (e._reason === "USERNAME_TAKEN") return json(409, { error: e.message, reason: "USERNAME_TAKEN" });
    throw e;
  }

  invalidateRoleCache(username);

  // Issue a session immediately — the UI logs the new account straight in.
  const token = generateSessionToken();
  const expiresAtMs = Date.now() + SESSION_LIFETIME_MS;
  await db.collection(SESSIONS_COLL).doc(token).set({
    username,
    createdAt : FV.serverTimestamp(),
    expiresAt : admin.firestore.Timestamp.fromMillis(expiresAtMs),
    lastSeenAt: FV.serverTimestamp(),
    userAgent
  });
  await db.collection(OPERATORS_COLL).doc(username).set({ lastLoginAt: FV.serverTimestamp() }, { merge: true });

  await writeAudit({
    eventType: "operator_signup",
    actor: username,
    payload: { viaInvite: code, role: canonicalRole, invitedBy: createdBy, displayName }
  });

  return json(200, {
    ok          : true,
    sessionToken: token,
    username,
    displayName,
    role        : canonicalRole,   // owner/operator; the gate maps to master_admin/user
    expiresAtMs
  });
}

async function handleListUsers(event) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const snap = await db.collection(OPERATORS_COLL).get();
  const users = [];
  for (const d of snap.docs) {
    const data = d.data() || {};
    users.push({
      username   : d.id,
      displayName: data.displayName || d.id,
      role       : toV6Role(data.role),
      suspended  : !!data.revokedAt
    });
  }
  // Masters first, then alphabetical — matches how the team sheet reads.
  users.sort((a, b) =>
    a.role === b.role
      ? (a.username || "").localeCompare(b.username || "")
      : (a.role === "master_admin" ? -1 : 1)
  );
  return json(200, { ok: true, users });
}

async function handleSetRole(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const username = String(body.username || "").trim();
  if (!username) return bad("username required");
  if (username.length > 200) return bad("username too long");
  const role = toCanonicalRole(body.role);

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const snap = await ref.get();
  if (!snap.exists) return bad("No such operator", 404);
  const data = snap.data() || {};

  // Never demote the last active owner — that strands team management.
  if (data.role === "owner" && role !== "owner") {
    const owners = await db.collection(OPERATORS_COLL).where("role", "==", "owner").get();
    const activeOwners = owners.docs.filter(d => !(d.data() || {}).revokedAt);
    if (activeOwners.length <= 1) return bad("Cannot demote the last remaining owner");
  }

  await ref.set({ role, roleChangedAt: FV.serverTimestamp(), roleChangedBy: sess.username }, { merge: true });
  invalidateRoleCache(username);
  await writeAudit({ eventType: "operator_role_changed", actor: sess.username, payload: { username, role } });
  return json(200, { ok: true, username, role: toV6Role(role) });
}

async function handleSetSuspended(event, body) {
  const sess = await requireOwnerSession(event);
  if (!sess.ok) return json(403, { error: "Owner role required", reason: sess.reason });

  const username = String(body.username || "").trim();
  if (!username) return bad("username required");
  if (username.length > 200) return bad("username too long");
  const suspended = !!body.suspended;

  if (username === sess.username) return bad("You cannot suspend your own account");

  const ref = db.collection(OPERATORS_COLL).doc(username);
  const snap = await ref.get();
  if (!snap.exists) return bad("No such operator", 404);
  const data = snap.data() || {};

  if (suspended && data.role === "owner") {
    const owners = await db.collection(OPERATORS_COLL).where("role", "==", "owner").get();
    const activeOwners = owners.docs.filter(d => !(d.data() || {}).revokedAt);
    if (activeOwners.length <= 1) return bad("Cannot suspend the last remaining active owner");
  }

  let killed = 0;
  if (suspended) {
    // revokedAt is the same flag login already checks, so a suspended user
    // is blocked at the door and their live sessions are torn down now.
    await ref.set({ revokedAt: FV.serverTimestamp(), revokedBy: sess.username }, { merge: true });
    killed = await deleteSessionsForUser(username);
  } else {
    await ref.set({
      revokedAt: FV.delete(),
      revokedBy: FV.delete(),
      reactivatedAt: FV.serverTimestamp(),
      reactivatedBy: sess.username
    }, { merge: true });
  }
  invalidateRoleCache(username);
  await writeAudit({
    eventType: suspended ? "operator_suspended" : "operator_reactivated",
    actor: sess.username,
    payload: { username, killedSessions: killed }
  });
  return json(200, { ok: true, suspended, killedSessions: killed });
}

// ─── Handler ────────────────────────────────────────────────────────────

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST")    return bad("POST required", 405);

  // X-EtsyMail-Secret as outer gate. Without this, no auth attempt
  // even reaches the username/password check — protects against
  // public probing.
  const auth = requireExtensionAuth(event);
  if (!auth.ok) return auth.response;

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return bad("Invalid JSON body"); }

  const op = String(body.op || "").toLowerCase();

  try {
    switch (op) {
      case "login":                  return await handleLogin(body);
      case "logout":                 return await handleLogout(event);
      case "currentuser":            return await handleCurrentUser(event);
      case "setmypassword":          return await handleSetMyPassword(event, body);
      case "listoperators":          return await handleListOperators(event);
      case "addoperator":            return await handleAddOperator(event, body);
      case "removeoperator":         return await handleRemoveOperator(event, body);
      case "resetoperatorpassword":  return await handleResetOperatorPassword(event, body);

      // ── v6 invite-gated auth + team management ──
      case "setupstate":             return await handleSetupState();
      case "checkusername":          return await handleCheckUsername(body);
      case "validateinvite":         return await handleValidateInvite(body);
      case "signup":                 return await handleSignup(body);
      case "createinvite":           return await handleCreateInvite(event, body);
      case "listinvites":            return await handleListInvites(event);
      case "revokeinvite":           return await handleRevokeInvite(event, body);
      case "listusers":              return await handleListUsers(event);
      case "setrole":                return await handleSetRole(event, body);
      case "setsuspended":           return await handleSetSuspended(event, body);
      // v6 team sheet uses op:"resetPassword"; route to the existing handler.
      case "resetpassword":          return await handleResetOperatorPassword(event, body);
      default:
        return bad(`Unknown op '${body.op}'`);
    }
  } catch (err) {
    console.error("etsyMailAuth unhandled error:", err);
    return json(500, { error: err.message || "Unknown error" });
  }
};
