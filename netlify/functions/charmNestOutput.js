/*  netlify/functions/charmNestOutput.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  Firebase Storage for the Charm Nesting Station. Every uploaded sheet,
 *  .ai file, per-charm file, thumbnail, finished output, labelled proof,
 *  preview and report gets a permanent copy under charmnest/… in the same
 *  bucket the other stations use (firebaseAdmin.DEFAULT_BUCKET).
 *
 *  Two routes, because Netlify caps a synchronous function body at ~6 MB
 *  and source artwork is often bigger than that:
 *
 *    sign  → a V4 signed PUT URL the browser uploads to directly (any size),
 *            plus the tokenised download URL the record will carry. Needs
 *            the bucket CORS to allow this console's origin — see
 *            firebaseAdmin.CORS_ORIGINS, which now lists
 *            brites-charm-sorter.goldenspike.app.
 *    put   → base64 body through the function (≤ 4.5 MB), the fallback the
 *            page uses if the direct PUT fails.
 *    url   → tokenised download URL for an existing path.
 *
 *  The tokenised URL shape is the one etsyMailCollateralUpload.js uses:
 *  firebasestorage.googleapis.com/v0/b/<bucket>/o/<path>?alt=media&token=…
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const crypto = require("crypto");
const admin = require("./firebaseAdmin");
const { json, gate, parseBody, str, safePath, CORS } = require("./_charmNestAuth");
const bucket = admin.storage().bucket();

const ALLOWED_TYPES = new Set(["application/pdf", "application/postscript", "application/illustrator", "image/png", "image/jpeg", "application/json", "application/zip", "application/octet-stream"]);
function firebaseDownloadUrl(bucketName, storagePath, token) {
  return "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucketName) + "/o/" + encodeURIComponent(storagePath) + "?alt=media&token=" + encodeURIComponent(token);
}
/** The token already on an object, if any — never rotated, so every stored link to the path stays valid. */
async function existingToken(file) {
  try { const [meta] = await file.getMetadata(); const t = meta && meta.metadata && meta.metadata.firebaseStorageDownloadTokens; return t ? String(t).split(",")[0] : null; } catch (_) { return null; }
}
const newToken = () => (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex"));
const contentTypeOf = t => { t = str(t, 80).toLowerCase(); return ALLOWED_TYPES.has(t) ? t : "application/octet-stream"; };

/* ── THE STORAGE DOOR (CLOUDDRIVE P1/C1, cloud plan v2 §1.10, build step 1d; DORMANT while Charm_Nest_Open/drive is absent or "off": then one cached read of the switch every
   15 s per instance and today's code). A page's sign, put or finalize of a production sheet's or set's file (charmnest/sheets/... or charmnest/sets/..., never the sandbox's):
     · standby: refused (409, in words) when the cloud carries the run the sheet (or set) belongs to: the cloud writes that run's files;
     · on: every such page write is refused: the cloud's own writers save in-process and never pass this door;
     · not off: a signed upload link for those paths lasts 60 s instead of 15 min.                                                                         ── */
const DOOR_PATH = /^charmnest\/(sheets|sets)\//;
let DRIVE_SEEN = null;   // { at, mode }
async function driveMode() {
  if (DRIVE_SEEN && Date.now() - DRIVE_SEEN.at < 15000 && Date.now() >= DRIVE_SEEN.at) return DRIVE_SEEN.mode;
  try { const s = await admin.firestore().collection("Charm_Nest_Open").doc("drive").get(), m = s.exists ? (s.data() || {}).mode : null; DRIVE_SEEN = { at: Date.now(), mode: m === "standby" || m === "on" ? m : "off" }; }
  catch (e) { console.warn("[charmNestOutput] the drive switch was not read:", (e && e.message) || e); return DRIVE_SEEN ? DRIVE_SEEN.mode : "off"; }
  return DRIVE_SEEN.mode;
}
/** The run a sheet's or set's file belongs to, from its folder: a sheet by its file base (charmnest/sheets/<day>/<fileBase>/...), a set by its folder (charmnest/sets/<day>/Set-N). */
async function runOfPath(path) {
  const db = admin.firestore(), seg = path.split("/");
  if (seg[1] === "sheets" && seg.length >= 5) { const q = await db.collection("Charm_Nest_Sheets").where("fileBase", "==", seg[3]).where("archived", "==", false).limit(1).get(); const d = q.docs[0] && q.docs[0].data(); return d ? { runId: d.runId || null, what: d.fileBase || seg[3] } : null; }
  if (seg[1] === "sets" && seg.length >= 5) { const q = await db.collection("Charm_Nest_Sets").where("folder", "==", seg.slice(0, 4).join("/")).limit(1).get(); const d = q.docs[0] && q.docs[0].data(); return d ? { runId: d.runId || null, what: d.name || seg[3] } : null; }
  return null;
}
/** null (go on as today) or the refusal of a page's write under a sheet's or set's folder. */
async function door(op, path, sandbox) {
  if (sandbox || !DOOR_PATH.test(path) || !["sign", "put", "finalize"].includes(op)) return null;
  const mode = await driveMode(); if (mode === "off") return null;
  if (mode === "on") return { ok: false, error: "The cloud carries every run now: a sheet's and a set's files are the cloud's to write. Nothing was saved", status: 409, cloudCarries: true };
  const owner = await runOfPath(path).catch(() => null);
  if (!owner || !owner.runId) return null;
  const cs = await admin.firestore().collection("Charm_Nest_Run_Carrier").doc(String(owner.runId)).get(), c = cs.exists ? (cs.data() || {}).carrier : null;
  if (c && c.dev === "cloud") return { ok: false, error: `The cloud carries the run of ${owner.what}: it writes that run's files. Nothing was saved from this computer`, status: 409, cloudCarries: true };
  return null;
}

async function op_sign(b) {
  const path = safePath(b.path); const contentType = contentTypeOf(b.contentType);
  const token = newToken();
  const file = bucket.file(path);
  // Only Content-Type is signed, so the browser's PUT needs no custom headers
  // (and the bucket CORS needs no extra allowed header). The download token is
  // attached by op_finalize once the PUT has landed.
  // (CLOUDDRIVE §1.10: while the drive is not off, a link for a sheet's or set's file lasts a minute; b._shortLink is set only by the handler, after the door, for those paths)
  const [uploadUrl] = await file.getSignedUrl({ version: "v4", action: "write", expires: Date.now() + (b._shortLink ? 60 * 1000 : 15 * 60 * 1000), contentType });
  return { ok: true, path, uploadUrl, contentType, headers: { "Content-Type": contentType }, downloadUrl: firebaseDownloadUrl(bucket.name, path, token), token };
}
async function op_put(b) {
  const path = safePath(b.path); const contentType = contentTypeOf(b.contentType);
  const base64 = str(b.base64, 7 * 1024 * 1024);
  if (!base64) return { error: "no data" };
  const buf = Buffer.from(base64, "base64");
  if (!buf.length) return { error: "empty body" };
  const token = (await existingToken(bucket.file(path))) || newToken();
  await bucket.file(path).save(buf, { resumable: false, contentType, metadata: { cacheControl: "public, max-age=31536000", metadata: { firebaseStorageDownloadTokens: token, uploadedBy: "charm-nest-1" } } });
  return { ok: true, path, url: firebaseDownloadUrl(bucket.name, path, token), bytes: buf.length };
}
async function op_url(b) {
  const path = safePath(b.path); const file = bucket.file(path);
  const [exists] = await file.exists(); if (!exists) return { error: "not found" };
  const [meta] = await file.getMetadata();
  let token = meta.metadata && meta.metadata.firebaseStorageDownloadTokens;
  if (!token) { token = newToken(); await file.setMetadata({ metadata: { firebaseStorageDownloadTokens: token } }); }
  return { ok: true, path, url: firebaseDownloadUrl(bucket.name, path, String(token).split(",")[0]), bytes: Number(meta.size) || 0, contentType: meta.contentType };
}
/** After a direct signed PUT, make sure the download token is on the object (the header route sets it; this is belt and braces). */
async function op_finalize(b) {
  const path = safePath(b.path); const file = bucket.file(path);
  const [exists] = await file.exists(); if (!exists) return { error: "upload not found" };
  const token = (await existingToken(file)) || str(b.token, 80) || newToken();
  await file.setMetadata({ contentType: contentTypeOf(b.contentType), metadata: { firebaseStorageDownloadTokens: token, uploadedBy: "charm-nest-1" } });
  return { ok: true, path, url: firebaseDownloadUrl(bucket.name, path, token) };
}

const OPS = { sign: op_sign, put: op_put, url: op_url, finalize: op_finalize };
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: CORS, body: "" };
  if (event.httpMethod !== "POST") return json(405, { error: "method not allowed" });
  const body = parseBody(event);
  const denied = gate(event, body); if (denied) return denied;
  // (an own name of OPS only: "constructor" or "toString" are the object's, not ops)
  const fn = typeof body.op === "string" && Object.prototype.hasOwnProperty.call(OPS, body.op) ? OPS[body.op] : null; if (!fn) return json(400, { error: "unknown op", ops: Object.keys(OPS) });
  // the Charm Repo's files (charmnest/master/...): uploading into them and minting a link to a whole master file need the repo token (_charmNestRepoAuth.js)
  { const RepoAuth = require("./_charmNestRepoAuth"), need = RepoAuth.requirement("charmNestOutput", body.op, body); if (need) { const no = await RepoAuth.authorize(admin.firestore(), need, body, event); if (no) return json(no.status, no.body); } }
  // the sandbox keeps its files apart: every path is moved under charmnest/sandbox/ (a path already there is left alone)
  if ((body.sandbox === true || body.sandbox === 1 || body.sandbox === "1") && typeof body.path === "string") { const p = safePath(body.path); body.path = /^charmnest\/sandbox\//.test(p) ? p : p.replace(/^charmnest\//, "charmnest/sandbox/"); }
  // KEEP K4: the files of a design saved to the repository (charmnest/master/custom/{SKU}.ai|png) are never replaced: the page uploads BEFORE it commits, so an upload that names a SKU
  // whose entry exists is another save's (a SKU two saves chose at the same moment) unless it says it is the same drawing (charmHash). The refusal is the commit's own word: sku-taken.
  if ((body.op === "sign" || body.op === "put") && typeof body.path === "string") {
    const m = /^charmnest\/master\/custom\/([^\/]+)\.(?:ai|png)$/.exec(safePath(body.path));
    if (m) {
      try {
        const held = await admin.firestore().collection("Charm_Master_Index").doc(m[1]).get(), e = held.exists ? held.data() || {} : null;
        if (e && e.sku && !(body.charmHash && String(e.charmHash || "") === String(body.charmHash))) return json(409, { ok: false, error: "sku-taken", sku: e.sku, by: "master", status: 409 });
      } catch (e) { console.warn("[charmNestOutput] custom file guard not read:", (e && e.message) || e); }
    }
  }
  // (CLOUDDRIVE §1.10, the Storage door; dormant while the drive is off)
  delete body._shortLink;
  if (typeof body.path === "string") {
    const sandbox = body.sandbox === true || body.sandbox === 1 || body.sandbox === "1", p = safePath(body.path);
    const no = await door(body.op, p, sandbox); if (no) return json(409, no);
    if (body.op === "sign" && !sandbox && DOOR_PATH.test(p) && (await driveMode()) !== "off") body._shortLink = true;
  }
  try { const out = await fn(body); return json(out && out.error ? 400 : 200, out); }
  catch (e) { console.error("[charmNestOutput]", body.op, e); return json(500, { error: e.message || String(e) }); }
};
