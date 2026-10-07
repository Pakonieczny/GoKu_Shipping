// FC13: etsyMailGmailConfig ?op=get is asked by every open inbox tab once a minute. Its answer is remembered 45 s in the
// function (one set of reads for all tabs), ?fresh=1 skips that, every write drops it, and the OAuth document is read
// once and remembered ten minutes (a "not seeded" answer never). Counts the reads against a fake Firestore.
//
//   node tests/etsy-mail/gmail-config-memo.cjs
"use strict";
const path = require("path"), Module = require("module");
const fnDir = path.join(__dirname, "../../netlify/functions");

const docs = {
  "EtsyMail_Config/gmailWatcher": { enabled: true, updatedBy: "paul" },
  "EtsyMail_Config/gmailSyncState": { lastSyncMessagesScanned: 3, lastSyncJobsEnqueued: 1 },
  "config/gmailOauth": { emailAddress: "shop@example.invalid", refreshToken: "never-in-memory" },
  "EtsyMail_Operators/paul": { role: "owner", displayName: "Paul" }
};
let reads = {};
const readsTotal = () => Object.values(reads).reduce((a, b) => a + b, 0);
const snap = p => ({ exists: !!docs[p], data: () => docs[p] ? JSON.parse(JSON.stringify(docs[p])) : undefined, id: p.split("/").pop() });
const db = {
  doc: p => ({ get: async () => { reads[p] = (reads[p] || 0) + 1; return snap(p); }, set: async (d) => { docs[p] = Object.assign({}, docs[p], d); } }),
  collection: c => ({
    add: async () => ({ id: "x" }),
    doc: id => ({ get: async () => { const p = c + "/" + id; reads[p] = (reads[p] || 0) + 1; return snap(p); }, set: async () => {} })
  })
};
const admin = { firestore: Object.assign(() => db, { FieldValue: { serverTimestamp: () => "TS", delete: () => "DEL" }, Timestamp: { fromMillis: ms => ({ toMillis: () => ms }) } }) };
const adminPath = require.resolve(path.join(fnDir, "firebaseAdmin"));
require.cache[adminPath] = { id: adminPath, filename: adminPath, loaded: true, exports: admin };
process.env.ETSYMAIL_EXTENSION_SECRET = "s3cret-for-test";

let now = Date.now();
Date.now = () => now;
const gc = require(path.join(fnDir, "etsyMailGmailConfig.js"));
const H = { "x-etsymail-secret": "s3cret-for-test" };
const get = async (q = "op=get") => {
  const r = await gc.handler({ httpMethod: "GET", headers: H, queryStringParameters: Object.fromEntries(new URLSearchParams(q)) });
  return JSON.parse(r.body);
};
const post = async body => (await gc.handler({ httpMethod: "POST", headers: H, queryStringParameters: {}, body: JSON.stringify(body) })).statusCode;

const fails = [];
const check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? "  ok   " : "  FAIL ") + what); };

(async () => {
  reads = {}; const a = await get();
  check(readsTotal() === 3 && a.ok && a.enabled === true && a.syncState.oauthSeeded === true && a.syncState.oauthEmailAddress === "shop@example.invalid", "first ask reads the three documents and answers as before");
  reads = {}; now += 30e3; const b = await get();
  check(readsTotal() === 0 && JSON.stringify(b) === JSON.stringify(a), "an ask 30 s later reads nothing and gives the same answer");
  reads = {}; now += 20e3; await get();
  check(readsTotal() === 2 && !reads["config/gmailOauth"], "after 45 s it reads the switch and the sync state, not the OAuth document (remembered)");
  reads = {}; await get("op=get&fresh=1");
  check(readsTotal() === 3 && reads["config/gmailOauth"] === 1, "?fresh=1 reads all three");
  reads = {}; docs["EtsyMail_Config/gmailSyncState"].lastSyncMessagesScanned = 9; await get("op=get&fresh=1");
  now += 1000; reads = {}; const c = await get();
  check(readsTotal() === 0 && c.syncState.lastSyncMessagesScanned === 9, "a fresh answer replaces what the next plain ask sees");
  // a write drops the answer
  reads = {}; const st = await post({ op: "set", enabled: false, actor: "paul" });
  check(st === 200, "the owner can still switch the watcher (" + st + ")");
  reads = {}; const d = await get();
  check(d.enabled === false && readsTotal() >= 2, "the ask after a write reads again and shows the new switch");
  // not seeded is never remembered
  delete docs["config/gmailOauth"]; now += 11 * 60e3;
  reads = {}; const e = await get();
  check(e.syncState.oauthSeeded === false, "OAuth removed: not seeded");
  docs["config/gmailOauth"] = { emailAddress: "back@example.invalid" };
  now += 46e3; reads = {}; const f = await get();
  check(f.syncState.oauthSeeded === true && f.syncState.oauthEmailAddress === "back@example.invalid", "OAuth seeded again: the next ask shows it (a negative is not remembered)");
  // an hour of one tab per minute
  reads = {}; for (let i = 0; i < 60; i++) { now += 60e3; await get(); }
  check(readsTotal() <= 2 * 60 + 8, "an hour at one ask a minute reads 2 documents a minute plus the OAuth document every 10 min (was 3 a minute): " + readsTotal());
  // several tabs / computers asking within the same 45 s share one set of reads
  reads = {}; now += 60e3; for (let i = 0; i < 6; i++) { await get(); now += 5e3; }
  check(readsTotal() === 2, "six tabs asking within 30 s read the two moving documents once: " + readsTotal());
  console.log(fails.length ? "FAIL " + fails.length : "PASS");
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
