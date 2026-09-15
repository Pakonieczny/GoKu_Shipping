/*  netlify/functions/charmNestLibrary.js
 *  ═══════════════════════════════════════════════════════════════════════
 *  Firestore for the Charm Nesting Station: the charm library (geometry
 *  hash → confirmed name), the permanent sheet records (recovery mode), the
 *  calibration rows that feed the saturation estimate, and the job records
 *  the server-side solver fallback writes to.
 *
 *  NOT the Etsy charm-set data. charmSetsData.js / syncCharmSets.js are
 *  about listing relationships; this is vector artwork. Different data,
 *  different collections, different prefix.
 *
 *  COLLECTIONS
 *    Charm_Nest_Library/{hash}       charm geometry, name, metal hint, usage
 *    Charm_Nest_Sheets/{sheetId}     one finished (or partial) sheet: stock,
 *                                    params, placements, verification,
 *                                    output + source URLs
 *    Charm_Nest_Calibration/{auto}   shape-mix signature + achieved density
 *    Charm_Nest_Jobs/{jobId}         server solver progress / result
 *
 *  NO COMPOSITE INDEXES. Every query is a single-field equality or range —
 *  matching designArchive.js — so this deploys without console setup. The
 *  metal + date filter is done as a single range on `day` with the metal
 *  applied in memory (a few hundred rows at most).
 *
 *  OPS (POST JSON {op, …}; X-Edit-Passcode when EDIT_PASSCODE is set)
 *    ping · lookupCharms · putCharms · renameCharm · listCharms
 *    putSheet · listSheets · getSheet · deleteSheet
 *    putCalibration · getCalibration
 *    startJob · getJob · stopJob
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { json, gate, parseBody, str, num } = require("./_charmNestAuth");
const db = admin.firestore();
const FV = admin.firestore.FieldValue;

const LIB = "Charm_Nest_Library", SHEETS = "Charm_Nest_Sheets", CAL = "Charm_Nest_Calibration", JOBS = "Charm_Nest_Jobs";
const MAX_PUT = 200;
const isHash = h => /^[0-9a-f]{8,64}$/i.test(String(h || ""));
const isId = s => /^[\w\-]{4,80}$/.test(String(s || ""));
const ms = v => (v && v.toMillis ? v.toMillis() : (typeof v === "number" ? v : null));

function slim(d) {
  return {
    id: d.id, metal: d.metal, metalLabel: d.metalLabel, day: d.day, status: d.status, endedBy: d.endedBy,
    charmCount: num(d.charmCount), placedCount: num(d.placedCount), rejectCount: num(d.rejectCount), density: num(d.density), freePt2: num(d.freePt2),
    verification: d.verification ? { ok: !!d.verification.ok } : null,
    preview: d.outputs && d.outputs.preview ? d.outputs.preview.url : null,
    names: str(d.names, 2000), sources: (d.sources || []).map(s => ({ name: s.name })),
    updatedAt: ms(d.updatedAt), createdAt: ms(d.createdAt)
  };
}

async function op_ping() {
  const [s, c] = await Promise.all([db.collection(SHEETS).where("archived", "==", false).limit(1000).select("id").get(), db.collection(LIB).limit(1000).select("hash").get()]);
  const cal = await db.collection(CAL).orderBy("createdAt", "desc").limit(200).get();
  return { ok: true, sheets: s.size, charms: c.size, calibration: cal.docs.map(d => { const r = d.data(); return { sheetId: r.sheetId, metal: r.metal, count: num(r.count), cv: num(r.cv), largestFrac: num(r.largestFrac), density: num(r.density), placedAll: !!r.placedAll }; }) };
}
async function op_lookupCharms(b) {
  const hashes = [...new Set((b.hashes || []).filter(isHash))].slice(0, 300);
  const out = {};
  for (let i = 0; i < hashes.length; i += 30) {
    const refs = hashes.slice(i, i + 30).map(h => db.collection(LIB).doc(h));
    const snaps = await db.getAll(...refs);
    for (const s of snaps) if (s.exists) { const d = s.data(); out[s.id] = { name: d.name || null, slug: d.slug || null, label: d.label || null, confidence: d.confidence || null, namedBy: d.namedBy || null, metalHint: d.metalHint || null, thumbUrl: d.thumbUrl || null, aiUrl: d.aiUrl || null, timesUsed: num(d.timesUsed) }; }
  }
  return { charms: out };
}
async function op_putCharms(b) {
  const rows = (b.charms || []).filter(c => c && isHash(c.hash)).slice(0, MAX_PUT);
  let batch = db.batch(), n = 0, count = 0;
  for (const c of rows) {
    const ref = db.collection(LIB).doc(c.hash);
    const doc = { hash: c.hash, updatedAt: FV.serverTimestamp(), lastUsed: FV.serverTimestamp(), timesUsed: FV.increment(1) };
    // never overwrite an operator's name with a model's or a fallback's
    const existing = await ref.get();
    const ex = existing.exists ? existing.data() : null;
    const incomingRank = { operator: 3, claude: 2, library: 1, fallback: 0 }[c.namedBy] ?? 0;
    const existingRank = ex ? ({ operator: 3, claude: 2, library: 1, fallback: 0 }[ex.namedBy] ?? 0) : -1;
    if (c.name && incomingRank >= existingRank) { doc.name = str(c.name, 80); doc.slug = str(c.slug || c.name, 80); doc.label = str(c.label, 200); doc.confidence = num(c.confidence) || null; doc.namedBy = str(c.namedBy, 20); }
    for (const k of ["areaPt2", "widthPt", "heightPt"]) if (c[k] != null) doc[k] = num(c[k]);
    for (const k of ["thumbUrl", "aiUrl", "aiPath", "sourceName", "sourcePath", "metalHint"]) if (c[k]) doc[k] = str(c[k], 600);
    if (c.open != null) doc.open = !!c.open;
    if (!ex) doc.firstSeen = FV.serverTimestamp();
    batch.set(ref, doc, { merge: true }); n++; count++;
    if (n >= 400) { await batch.commit(); batch = db.batch(); n = 0; }
  }
  if (n) await batch.commit();
  return { ok: true, count };
}
async function op_renameCharm(b) {
  if (!isHash(b.hash)) return { error: "bad hash" };
  const name = str(b.name, 80).trim(); if (!name) return { error: "empty name" };
  await db.collection(LIB).doc(b.hash).set({ hash: b.hash, name, slug: name, namedBy: "operator", updatedAt: FV.serverTimestamp() }, { merge: true });
  return { ok: true };
}
async function op_listCharms(b) {
  const q = str(b.q, 80).toLowerCase(); const limit = Math.min(1000, Math.max(1, num(b.limit) || 400));
  const snap = await db.collection(LIB).orderBy("lastUsed", "desc").limit(limit).get();
  let rows = snap.docs.map(d => { const r = d.data(); return { hash: d.id, name: r.name || null, label: r.label || null, namedBy: r.namedBy || null, metalHint: r.metalHint || null, thumbUrl: r.thumbUrl || null, aiUrl: r.aiUrl || null, widthPt: num(r.widthPt), heightPt: num(r.heightPt), areaPt2: num(r.areaPt2), timesUsed: num(r.timesUsed), sourceName: r.sourceName || null, lastUsed: ms(r.lastUsed) }; });
  if (q) rows = rows.filter(r => `${r.name || ""} ${r.label || ""} ${r.sourceName || ""}`.toLowerCase().includes(q));
  return { charms: rows };
}
async function op_putSheet(b) {
  const s = b.sheet || {}; if (!isId(s.id)) return { error: "bad sheet id" };
  const doc = Object.assign({}, s, { id: s.id, archived: false, updatedAt: FV.serverTimestamp() });
  delete doc.log;
  const ref = db.collection(SHEETS).doc(s.id); const ex = await ref.get();
  if (!ex.exists) doc.createdAt = FV.serverTimestamp();
  await ref.set(doc, { merge: true });
  return { ok: true, id: s.id };
}
async function op_listSheets(b) {
  const limit = Math.min(500, Math.max(1, num(b.limit) || 300));
  let q = db.collection(SHEETS).orderBy("day", "desc");
  if (b.from && /^\d{4}-\d{2}-\d{2}$/.test(b.from)) q = q.where("day", ">=", b.from);
  if (b.to && /^\d{4}-\d{2}-\d{2}$/.test(b.to)) q = q.where("day", "<=", b.to);
  const snap = await q.limit(limit).get();
  let rows = snap.docs.map(d => d.data()).filter(d => !d.archived);
  if (b.metal && /^(gold|silver|rose)$/.test(b.metal)) rows = rows.filter(d => d.metal === b.metal);
  rows.sort((x, y) => (ms(y.updatedAt) || 0) - (ms(x.updatedAt) || 0));
  return { sheets: rows.map(slim) };
}
async function op_getSheet(b) {
  if (!isId(b.id)) return { error: "bad id" };
  const s = await db.collection(SHEETS).doc(b.id).get();
  if (!s.exists) return { sheet: null };
  const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt);
  return { sheet: d };
}
async function op_deleteSheet(b) {
  if (!isId(b.id)) return { error: "bad id" };
  await db.collection(SHEETS).doc(b.id).set({ archived: true, archivedAt: FV.serverTimestamp() }, { merge: true });
  return { ok: true };
}
async function op_putCalibration(b) {
  const r = b.row || {};
  if (!(num(r.density) > 0) || !(num(r.count) > 0)) return { error: "bad row" };
  await db.collection(CAL).add({ sheetId: str(r.sheetId, 80), metal: str(r.metal, 12), count: num(r.count), cv: num(r.cv), largestFrac: num(r.largestFrac), density: num(r.density), placedAll: !!r.placedAll, clearancePt: num(r.clearancePt), createdAt: FV.serverTimestamp() });
  return { ok: true };
}
async function op_getCalibration(b) {
  const snap = await db.collection(CAL).orderBy("createdAt", "desc").limit(Math.min(1000, num(b.limit) || 300)).get();
  return { rows: snap.docs.map(d => d.data()) };
}
/* server solver fallback: create the job, fire the background function, poll by id */
async function op_startJob(b) {
  const job = b.job; if (!job || !Array.isArray(job.pieces) || !job.pieces.length) return { error: "no job" };
  if (job.pieces.length > 400) return { error: "too many pieces" };
  const id = "job-" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  await db.collection(JOBS).doc(id).set({ id, sheetId: str(b.sheetId, 80), status: "pending", trials: 0, createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp(), pieceCount: job.pieces.length });
  const fetch = require("node-fetch");
  const base = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
  fetch(`${base}/.netlify/functions/charmNestSolve-background`, { method: "POST", headers: { "Content-Type": "application/json", "X-Charm-Nest-Job": id }, body: JSON.stringify({ id, job }) }).catch(err => console.warn("[charmNestLibrary] background kick failed", err.message));
  return { ok: true, id };
}
async function op_getJob(b) {
  if (!isId(b.id)) return { error: "bad id" };
  const s = await db.collection(JOBS).doc(b.id).get();
  if (!s.exists) return { job: null };
  const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt);
  return { job: d };
}
async function op_stopJob(b) {
  if (!isId(b.id)) return { error: "bad id" };
  await db.collection(JOBS).doc(b.id).set({ stopRequested: true, updatedAt: FV.serverTimestamp() }, { merge: true });
  return { ok: true };
}

/* Claude jobs (review / naming) run in charmNestAgent-background; the payload
   (images) is passed straight through to the kick so Firestore never stores it. */
const AGENT = "Charm_Nest_Agent";
async function op_startAgent(b) {
  const mode = ["grouping", "layout", "name", "place"].includes(b.mode) ? b.mode : null;
  if (!mode || !b.payload) return { error: "mode and payload required" };
  const id = "agent-" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  // Background functions accept only a small request body (the images made the
  // kick 413), so the payload is parked in Storage and the job reads it back.
  const payloadPath = `charmnest/agent/${id}.json`;
  await admin.storage().bucket().file(payloadPath).save(Buffer.from(JSON.stringify(b.payload)), { resumable: false, contentType: "application/json", metadata: { cacheControl: "no-store" } });
  await db.collection(AGENT).doc(id).set({ id, mode, status: "pending", payloadPath, sheetId: str(b.sheetId, 80), sourceName: str(b.sourceName, 120), createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() });
  const fetch = require("node-fetch");
  const base = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
  const kick = await fetch(`${base}/.netlify/functions/charmNestAgent-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id, mode }) }).catch(err => ({ ok: false, status: 0, statusText: err.message }));
  if (!kick.ok && kick.status !== 202) { await db.collection(AGENT).doc(id).set({ status: "error", error: `kick failed: ${kick.status} ${kick.statusText || ""}`, updatedAt: FV.serverTimestamp() }, { merge: true }); return { error: `could not start the background job (${kick.status})` }; }
  return { ok: true, id };
}
async function op_getAgent(b) {
  if (!isId(b.id)) return { error: "bad id" };
  const s = await db.collection(AGENT).doc(b.id).get();
  if (!s.exists) return { job: null };
  const d = s.data(); return { job: { id: d.id, mode: d.mode, status: d.status, error: d.error || null, result: d.result || null, startedAt: ms(d.startedAt), createdAt: ms(d.createdAt) } };
}

const OPS = { startAgent: op_startAgent, getAgent: op_getAgent, ping: op_ping, lookupCharms: op_lookupCharms, putCharms: op_putCharms, renameCharm: op_renameCharm, listCharms: op_listCharms, putSheet: op_putSheet, listSheets: op_listSheets, getSheet: op_getSheet, deleteSheet: op_deleteSheet, putCalibration: op_putCalibration, getCalibration: op_getCalibration, startJob: op_startJob, getJob: op_getJob, stopJob: op_stopJob };

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: require("./_charmNestAuth").CORS, body: "" };
  const body = event.httpMethod === "GET" ? Object.assign({}, event.queryStringParameters || {}) : parseBody(event);
  const denied = gate(event, body); if (denied) return denied;
  const fn = OPS[body.op];
  if (!fn) return json(400, { error: "unknown op", ops: Object.keys(OPS) });
  try {
    const out = await fn(body);
    return json(out && out.error ? 400 : 200, out);
  } catch (e) {
    console.error("[charmNestLibrary]", body.op, e);
    return json(500, { error: e.message || String(e) });
  }
};
