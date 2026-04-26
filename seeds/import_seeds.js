#!/usr/bin/env node
/* seeds/import_seeds.js
 *
 * One-shot Node script to import option_sheets_seed.json and
 * collateral_seed.json into Firestore. Run AFTER deploying the
 * Step 2.1 functions to Netlify.
 *
 * Usage:
 *   ETSYMAIL_BASE=https://goldenspike.app \
 *   ETSYMAIL_SECRET=<your-secret> \
 *   ETSYMAIL_OWNER_NAME=<your-employee-name> \
 *   node seeds/import_seeds.js
 *
 * Optional flags:
 *   --skip-option-sheets   Don't import option sheets
 *   --skip-collateral      Don't import collateral
 *   --dry-run              Print what would be sent, don't actually post
 *
 * Prerequisites:
 *   1. The deployed Netlify site has etsyMailOptionResolver and
 *      etsyMailCollateral live.
 *   2. Your operator role doc exists at EtsyMail_Operators/<owner-name>
 *      with role: "owner". Without this, putSheet/create returns 403.
 *   3. ETSYMAIL_EXTENSION_SECRET is set on the Netlify side and matches
 *      what you pass here as ETSYMAIL_SECRET.
 *
 * Idempotency: this script overwrites existing docs (op:"putSheet" uses
 * merge:false on the resolver; collateral op:"create" returns an error
 * if the id already exists, but the script handles that by switching
 * to op:"update").
 */

const fs   = require("fs");
const path = require("path");
const https = require("https");

const BASE   = process.env.ETSYMAIL_BASE   || "";
const SECRET = process.env.ETSYMAIL_SECRET || "";
const OWNER  = process.env.ETSYMAIL_OWNER_NAME || "";

if (!BASE || !SECRET || !OWNER) {
  console.error("Missing required env vars: ETSYMAIL_BASE, ETSYMAIL_SECRET, ETSYMAIL_OWNER_NAME");
  process.exit(1);
}

const SKIP_SHEETS     = process.argv.includes("--skip-option-sheets");
const SKIP_COLLATERAL = process.argv.includes("--skip-collateral");
const DRY_RUN         = process.argv.includes("--dry-run");

function postJson(fnName, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${BASE}/.netlify/functions/${fnName}`);
    const data = JSON.stringify(body);
    const opts = {
      hostname: url.hostname,
      port    : url.port || 443,
      path    : url.pathname,
      method  : "POST",
      headers : {
        "Content-Type"     : "application/json",
        "Content-Length"   : Buffer.byteLength(data),
        "X-EtsyMail-Secret": SECRET
      }
    };
    if (DRY_RUN) {
      console.log("\n--- DRY RUN ---");
      console.log("POST", url.toString());
      console.log("Body keys:", Object.keys(body).join(", "));
      console.log("--- /DRY RUN ---\n");
      return resolve({ dryRun: true });
    }
    const req = https.request(opts, (res) => {
      let raw = "";
      res.on("data", (c) => raw += c);
      res.on("end", () => {
        let parsed = {};
        try { parsed = JSON.parse(raw); } catch { parsed = { raw }; }
        if (res.statusCode >= 200 && res.statusCode < 300) resolve(parsed);
        else reject(new Error(`${fnName} ${res.statusCode}: ${raw}`));
      });
    });
    req.on("error", reject);
    req.write(data);
    req.end();
  });
}

async function importOptionSheets() {
  const seedPath = path.join(__dirname, "option_sheets_seed.json");
  const seed = JSON.parse(fs.readFileSync(seedPath, "utf8"));
  const families = ["huggie", "necklace", "stud"];
  console.log(`\n[1/2] Importing option sheets — ${families.length} families`);
  for (const family of families) {
    const sheet = seed[family];
    if (!sheet) {
      console.warn(`  ⚠ ${family} missing from seed; skipping`);
      continue;
    }
    process.stdout.write(`  ${family.padEnd(10)} ... `);
    try {
      const res = await postJson("etsyMailOptionResolver", {
        op    : "putSheet",
        actor : OWNER,
        family,
        sheet
      });
      if (res.success || res.dryRun) console.log("✓");
      else console.log("✗", JSON.stringify(res));
    } catch (e) {
      console.log("✗", e.message);
    }
  }
}

async function importCollateral() {
  const seedPath = path.join(__dirname, "collateral_seed.json");
  const seed = JSON.parse(fs.readFileSync(seedPath, "utf8"));
  const items = seed.items || {};
  const ids = Object.keys(items);
  console.log(`\n[2/2] Importing collateral — ${ids.length} items`);
  console.log("  (Items with REPLACE_WITH_PUBLIC_URL placeholders will still be imported,");
  console.log("   but the AI will reference broken URLs until you update them.)");
  for (const id of ids) {
    const item = items[id];
    process.stdout.write(`  ${id.padEnd(36)} ... `);
    try {
      // Try create first; if id collision, fall through to update.
      let res;
      try {
        res = await postJson("etsyMailCollateral", {
          op    : "create",
          actor : OWNER,
          item  : { ...item }
        });
      } catch (createErr) {
        // Fallback to update (matches existing UI behavior)
        res = await postJson("etsyMailCollateral", {
          op    : "update",
          actor : OWNER,
          id,
          patch : { ...item }
        });
      }
      if (res.success || res.dryRun) console.log("✓");
      else console.log("✗", JSON.stringify(res));
    } catch (e) {
      console.log("✗", e.message);
    }
  }
}

(async () => {
  console.log(`Importing seeds → ${BASE}`);
  console.log(`Owner: ${OWNER}`);
  if (DRY_RUN) console.log("(dry run mode — no actual POSTs will be sent)");

  if (!SKIP_SHEETS)     await importOptionSheets();
  if (!SKIP_COLLATERAL) await importCollateral();

  console.log("\nDone.");
  console.log("\nNEXT STEPS:");
  console.log("  1. Verify the import: open the inbox UI → Settings → Browse option sheets");
  console.log("  2. Replace REPLACE_WITH_PUBLIC_URL placeholders in collateral entries (Settings → Manage collateral)");
  console.log("  3. Enable Sales Mode in Settings (was disabled by default)");
  console.log("  4. Test on a pilot thread (add to Pilot Allow-List)");
})().catch((e) => {
  console.error("\nIMPORT FAILED:", e.message);
  process.exit(1);
});
