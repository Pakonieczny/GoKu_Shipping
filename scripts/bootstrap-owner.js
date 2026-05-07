/*  scripts/bootstrap-owner.js
 *
 *  v4.1 — One-time owner-account bootstrap.
 *
 *  Run ONCE on your local machine after deploying the v4.1 auth backend
 *  (etsyMailAuth.js + updated _etsyMailRoles.js + etsy-mail-1.html).
 *  Creates an owner record in EtsyMail_Operators with a hashed password
 *  so you can log in via the new login modal.
 *
 *  ─── Usage ──────────────────────────────────────────────────────────
 *
 *  Option A — quick one-liner from your existing project:
 *
 *      cd <your-netlify-project-root>
 *      node scripts/bootstrap-owner.js paul "Paul K" your-strong-password
 *
 *  Option B — interactive (no plaintext password in shell history):
 *
 *      node scripts/bootstrap-owner.js paul "Paul K"
 *      # script prompts for the password
 *
 *  Args:
 *    1. username     (required, 3-32 chars, lowercase letters/digits/_/-)
 *    2. displayName  (required, free-form; appears in audit rows + UI)
 *    3. password     (optional; if omitted, the script prompts stdin)
 *
 *  ─── What it does ───────────────────────────────────────────────────
 *
 *    - Hashes the password with the same PBKDF2-SHA512 / 200000-iter /
 *      32-byte-salt scheme etsyMailAuth.js uses for live logins.
 *    - Writes EtsyMail_Operators/{username} with role:"owner" and the
 *      hashed credentials.
 *    - Idempotent: if the doc already exists, the script REJECTS rather
 *      than silently overwriting. To rotate, delete the doc first or
 *      use the in-app password-reset flow once you have a working owner.
 *
 *  ─── Why this isn't done from the UI ────────────────────────────────
 *
 *    The Add Operator modal is owner-only — by design. Without this
 *    seed step, there'd be no owner to use it. Once one owner exists,
 *    further owners and satellites are created via the UI, and this
 *    script never runs again.
 *
 *  ─── Requirements ───────────────────────────────────────────────────
 *
 *    - Node 18+ (uses crypto.pbkdf2Sync — standard Node, no installs).
 *    - firebase-admin already installed in your Netlify project (it is —
 *      every backend function imports ./firebaseAdmin.js).
 *    - Same FIREBASE_SERVICE_ACCOUNT env var or service-account JSON
 *      file path that your backend uses; see ./firebaseAdmin.js for
 *      the lookup order.
 */

"use strict";

const crypto   = require("crypto");
const readline = require("readline");

// Reuse the exact same firebaseAdmin module the backend functions use.
// Adjust the path if you move the script outside `scripts/`.
const admin = require("../netlify/functions/firebaseAdmin");

const PBKDF2_ITERATIONS = 200_000;
const PBKDF2_KEY_BYTES  = 64;
const PBKDF2_DIGEST     = "sha512";
const SALT_BYTES        = 32;

const OPERATORS_COLL = "EtsyMail_Operators";

function hashSecret(plaintext) {
  const salt = crypto.randomBytes(SALT_BYTES);
  const hash = crypto.pbkdf2Sync(
    String(plaintext), salt, PBKDF2_ITERATIONS, PBKDF2_KEY_BYTES, PBKDF2_DIGEST
  );
  return {
    hash      : hash.toString("hex"),
    salt      : salt.toString("hex"),
    iterations: PBKDF2_ITERATIONS,
    digest    : PBKDF2_DIGEST
  };
}

function isValidUsername(s) {
  return typeof s === "string" && /^[a-z0-9_-]{3,32}$/.test(s);
}

async function promptHidden(question) {
  // Minimal hidden-input prompt — Node's readline doesn't natively hide
  // input, so we briefly turn off stdout echo. Good enough for a one-
  // shot bootstrap; not as bulletproof as `read -s` in bash.
  return new Promise((resolve) => {
    const rl = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: true });
    process.stdout.write(question);
    rl.stdoutMuted = true;
    rl._writeToOutput = function _writeToOutput(s) {
      if (rl.stdoutMuted) {
        // Print nothing; \n still passes through
        if (s.endsWith("\n")) process.stdout.write("\n");
      } else {
        process.stdout.write(s);
      }
    };
    rl.question("", (answer) => {
      rl.close();
      resolve(answer);
    });
  });
}

async function main() {
  const [, , usernameArg, displayNameArg, passwordArg] = process.argv;

  if (!usernameArg || !displayNameArg) {
    console.error("Usage: node scripts/bootstrap-owner.js <username> <displayName> [<password>]");
    process.exit(1);
  }

  const username = String(usernameArg).trim().toLowerCase();
  const displayName = String(displayNameArg).trim();

  if (!isValidUsername(username)) {
    console.error(`Invalid username: '${username}'. Must be 3-32 chars, lowercase letters/digits/underscore/dash.`);
    process.exit(1);
  }

  let password;
  if (passwordArg) {
    password = String(passwordArg);
  } else {
    password = await promptHidden(`Password for owner '${username}' (input hidden, min 8 chars): `);
    if (!password || password.length < 8) {
      console.error("Password must be at least 8 characters.");
      process.exit(1);
    }
    const confirm = await promptHidden("Confirm password: ");
    if (confirm !== password) {
      console.error("Passwords don't match.");
      process.exit(1);
    }
  }

  if (password.length < 8) {
    console.error("Password must be at least 8 characters.");
    process.exit(1);
  }

  const db = admin.firestore();
  const ref = db.collection(OPERATORS_COLL).doc(username);
  const existing = await ref.get();
  if (existing.exists) {
    const data = existing.data() || {};
    if (data.passwordHash && !data.revokedAt) {
      console.error(`\nOperator '${username}' already exists with a password set.`);
      console.error(`To rotate the password, sign in as that user and use the Change Password flow,`);
      console.error(`or delete the doc at ${OPERATORS_COLL}/${username} and re-run this script.`);
      process.exit(1);
    }
  }

  const fresh = hashSecret(password);
  await ref.set({
    username,
    displayName,
    role        : "owner",
    passwordHash: fresh.hash,
    salt        : fresh.salt,
    iterations  : fresh.iterations,
    digest      : fresh.digest,
    createdAt   : admin.firestore.FieldValue.serverTimestamp(),
    createdBy   : "bootstrap-script",
    revokedAt   : admin.firestore.FieldValue.delete()
  }, { merge: true });

  console.log(`\n✓ Owner '${username}' (${displayName}) created.`);
  console.log(`  Sign in at your inbox URL with username='${username}' and the password you just set.`);
  console.log(`  Use the Operators section in Settings to add satellite operators from there.\n`);
  process.exit(0);
}

main().catch((err) => {
  console.error("Bootstrap failed:", err);
  process.exit(1);
});
