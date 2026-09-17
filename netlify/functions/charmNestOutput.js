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

async function op_sign(b) {
  const path = safePath(b.path); const contentType = contentTypeOf(b.contentType);
  const token = newToken();
  const file = bucket.file(path);
  // Only Content-Type is signed, so the browser's PUT needs no custom headers
  // (and the bucket CORS needs no extra allowed header). The download token is
  // attached by op_finalize once the PUT has landed.
  const [uploadUrl] = await file.getSignedUrl({ version: "v4", action: "write", expires: Date.now() + 15 * 60 * 1000, contentType });
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
  const fn = OPS[body.op]; if (!fn) return json(400, { error: "unknown op", ops: Object.keys(OPS) });
  // the sandbox keeps its files apart: every path is moved under charmnest/sandbox/ (a path already there is left alone)
  if ((body.sandbox === true || body.sandbox === 1 || body.sandbox === "1") && typeof body.path === "string") { const p = safePath(body.path); body.path = /^charmnest\/sandbox\//.test(p) ? p : p.replace(/^charmnest\//, "charmnest/sandbox/"); }
  try { const out = await fn(body); return json(out && out.error ? 400 : 200, out); }
  catch (e) { console.error("[charmNestOutput]", body.op, e); return json(500, { error: e.message || String(e) }); }
};
