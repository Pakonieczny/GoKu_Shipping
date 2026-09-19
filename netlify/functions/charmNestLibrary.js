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
 *    + bridge (design doc §13): masterPutIndex · masterGet · masterGetMany · masterList · masterPatch · masterPutFile ·
 *      masterListFiles · masterRemoveFile · startMaster · poolPut · poolUpdate · poolList · poolGet · backPut · backList ·
 *      setAllocate · setUpdate · setGet · setList · runPut · runGet · runList · bridgeLog · aliasGet · aliasPut ·
 *      noDesignGet · noDesignPut · noDesignDelete · optionMapGet · optionMapPut
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { json, gate, parseBody, str, num } = require("./_charmNestAuth");
const db = admin.firestore();
/* ── sandbox: when a request says sandbox:true, the sorter's OWN records (sheets, pools, backs, sets, counters, runs,
   bridge log) go to Sandbox_-prefixed collections; the master index, the charm library, maps and calibration stay
   shared and are only read. Set per request; a function instance handles one request at a time. ── */
let PREFIX = "";
const SANDBOXED = new Set(["Charm_Nest_Sheets", "Charm_Pool", "Charm_Pool_Back", "Charm_Nest_Sets", "Charm_Nest_Counters", "Charm_Nest_Runs", "Charm_Nest_Release", "Charm_Nest_Arrivals", "Design_Bridge"]);
const col = name => db.collection(SANDBOXED.has(name) ? PREFIX + name : name);
const FV = admin.firestore.FieldValue;

const LIB = "Charm_Nest_Library", SHEETS = "Charm_Nest_Sheets", CAL = "Charm_Nest_Calibration", JOBS = "Charm_Nest_Jobs";
const MAX_PUT = 200;
const isHash = h => /^[0-9a-f]{8,64}$/i.test(String(h || ""));
const isId = s => /^[\w\-]{4,80}$/.test(String(s || ""));
const ms = v => (v && v.toMillis ? v.toMillis() : (typeof v === "number" ? v : null));

function slim(d) {
  return {
    id: d.id, folder: d.folder || null, fileBase: d.fileBase || d.folder || null, saving: !!d.saving, metal: d.metal, metalLabel: d.metalLabel, day: d.day, status: d.status, endedBy: d.endedBy,
    charmCount: num(d.charmCount), placedCount: num(d.placedCount), rejectCount: num(d.rejectCount), density: num(d.density), freePt2: num(d.freePt2),
    verification: d.verification ? { ok: !!d.verification.ok } : null,
    preview: d.outputs && d.outputs.preview ? d.outputs.preview.url : null,
    // the four files a recalled card offers, so recalling a set is one read of this list and nothing more
    outputs: d.outputs ? Object.fromEntries(["ai", "pdf", "labelled", "report"].filter(k => d.outputs[k] && d.outputs[k].url).map(k => [k, d.outputs[k].url])) : {},
    backs: (d.backPool || []).map(bk => ({ poolId: bk.poolId || null, order: bk.order || null, sku: bk.sku || null, text: bk.text || null, lines: bk.lines || null, approvedBy: bk.approvedBy || null, capMm: bk.capMm || null, png: bk.outputs && bk.outputs.png ? bk.outputs.png.url : null, ai: bk.outputs && bk.outputs.ai ? bk.outputs.ai.url : null })),
    names: str(d.names, 2000), sources: (d.sources || []).map(s => ({ name: s.name, hash: s.hash || null })), runId: d.runId || null, page: num(d.page) || 1,
    setId: d.setId || null, setSeq: num(d.setSeq) || null, sheetIndex: num(d.sheetIndex) || null, orders: (d.orders || []).slice(0, 500), backCount: (d.backPool || []).length, label: d.label ? { files: (d.label.files || []).map(f => ({ path: f.path, url: f.url })) } : null,
    updatedAt: ms(d.updatedAt), createdAt: ms(d.createdAt)
  };
}

async function op_ping() {
  const [s, c] = await Promise.all([col(SHEETS).where("archived", "==", false).limit(1000).select("id").get(), db.collection(LIB).limit(1000).select("hash").get()]);
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
  // Firestore cannot match a substring, so the limit has to come first and the filter second — which means a search
  // only ever sees the most recently used `limit` charms. A charm that genuinely exists but was last used 401 charms
  // ago used to come back as "no charms yet": a false negative on correct input. The caller is told how far it looked.
  const scanned = rows.length;
  if (q) rows = rows.filter(r => `${r.name || ""} ${r.label || ""} ${r.sourceName || ""}`.toLowerCase().includes(q));
  return { charms: rows, scanned, truncated: scanned >= limit };
}
async function op_putSheet(b) {
  const s = b.sheet || {}; if (!isId(s.id)) return { error: "bad sheet id" };
  const doc = Object.assign({}, s, { id: s.id, archived: false, updatedAt: FV.serverTimestamp() });
  delete doc.log;
  const ref = col(SHEETS).doc(s.id); const ex = await ref.get();
  if (!ex.exists) doc.createdAt = FV.serverTimestamp();
  await ref.set(doc, { merge: true });
  return { ok: true, id: s.id };
}
async function op_listSheets(b) {
  const limit = Math.min(500, Math.max(1, num(b.limit) || 300));
  let q = b.runId ? col(SHEETS).where("runId", "==", b.runId) : b.setId ? col(SHEETS).where("setId", "==", b.setId) : col(SHEETS).orderBy("day", "desc");
  if (b.from && /^\d{4}-\d{2}-\d{2}$/.test(b.from)) q = q.where("day", ">=", b.from);
  if (b.to && /^\d{4}-\d{2}-\d{2}$/.test(b.to)) q = q.where("day", "<=", b.to);
  const snap = await q.limit(limit).get();
  let rows = snap.docs.map(d => d.data()).filter(d => !d.archived);
  if (b.metal && /^(gold|silver|rose|gold10k|gold14k)$/.test(b.metal)) rows = rows.filter(d => d.metal === b.metal);
  if (b.setId) rows = rows.filter(d => d.setId === b.setId);
  if (b.runId) rows = rows.filter(d => d.runId === b.runId);
  rows.sort((x, y) => (ms(y.updatedAt) || 0) - (ms(x.updatedAt) || 0));
  return { sheets: rows.map(slim) };
}
async function op_getSheet(b) {
  if (!isId(b.id)) return { error: "bad id" };
  const s = await col(SHEETS).doc(b.id).get();
  if (!s.exists) return { sheet: null };
  const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt);
  await refreshLinks(d);
  return { sheet: d };
}
/** Links in a sheet record are rebuilt from the objects' current download tokens (older records may carry a token
 *  that a later re-upload replaced). Charm links missing at save time are filled from the charm library. */
async function refreshLinks(d) {
  const bucket = admin.storage().bucket();
  const urlFor = async (path) => {
    if (!path) return null;
    try { const file = bucket.file(path); const [meta] = await file.getMetadata(); let t = meta.metadata && meta.metadata.firebaseStorageDownloadTokens; if (!t) return null; t = String(t).split(",")[0];
      return "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucket.name) + "/o/" + encodeURIComponent(path) + "?alt=media&token=" + encodeURIComponent(t); } catch (_) { return null; }
  };
  const jobs = [];
  for (const k of ["ai", "pdf", "labelled", "report", "preview"]) { const o = d.outputs && d.outputs[k]; if (o && o.path) jobs.push(urlFor(o.path).then(u => { if (u) o.url = u; })); }
  for (const f of (d.label && d.label.files) || []) if (f.path) jobs.push(urlFor(f.path).then(u => { if (u) f.url = u; }));
  for (const k of ["index", "report"]) { const o = d.backOutputs && d.backOutputs[k]; if (o && o.path) jobs.push(urlFor(o.path).then(u => { if (u) o.url = u; })); }
  for (const bk of d.backPool || []) for (const k of ["ai", "png"]) { const o = bk.outputs && bk.outputs[k]; if (o && o.path) jobs.push(urlFor(o.path).then(u => { if (u) o.url = u; })); }
  for (const c of d.charms || []) {
    const pngPath = c.pngPath || (c.hash ? "charmnest/charms/" + c.hash + ".png" : null), aiPath = c.aiPath || (c.hash ? "charmnest/charms/" + c.hash + ".ai" : null);
    jobs.push(urlFor(pngPath).then(u => { if (u) c.thumbUrl = u; }));
    jobs.push(urlFor(aiPath).then(u => { if (u) c.aiUrl = u; }));
  }
  await Promise.all(jobs);
}
/** Permanent delete of a sheet record and its output files, behind a passcode. Charm library copies are shared and stay. */
const DELETE_CODE = process.env.CHARM_NEST_DELETE_CODE || "975311";
async function op_deleteSheet(b) {
  if (!isId(b.id)) return { error: "bad id" };
  if (String(b.code || "") !== DELETE_CODE) return { error: "wrong passcode", status: 403 };
  const ref = col(SHEETS).doc(b.id); const snap = await ref.get();
  if (snap.exists) {
    const d = snap.data(); const bucket = admin.storage().bucket();
    const paths = ["ai", "pdf", "labelled", "report", "preview"].map(k => d.outputs && d.outputs[k] && d.outputs[k].path).filter(Boolean);
    await Promise.all(paths.map(p => bucket.file(p).delete().catch(() => {})));
    await ref.delete();
  }
  return { ok: true, deleted: snap.exists };
}
/* A calibration row is a statistic the fill estimate learns from — never part of the sheet record. A sheet that placed
   nothing has nothing to teach, so it is skipped, not refused: an error here used to travel all the way up and stop a run
   whose sheets were already written, verified and saved. */
async function op_putCalibration(b) {
  const r = b.row || {};
  if (!(num(r.density) > 0) || !(num(r.count) > 0)) return { ok: true, skipped: "nothing to learn from this sheet" };
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
/** Recent jobs (solver, master indexing, agent kicks) with their stage and age, so a stalled one can be seen for what it is. */
async function op_jobList(b) {
  const lim = Math.min(50, Math.max(1, num(b.limit) || 20));
  const s = await db.collection(JOBS).orderBy("createdAt", "desc").limit(lim).get();
  const now = Date.now();
  return { jobs: s.docs.map(d => { const j = d.data(); const up = ms(j.updatedAt), cr = ms(j.createdAt); return { id: d.id, kind: j.kind || "solver", status: j.status, stage: j.stage || null, done: j.done || 0, total: j.total || 0, name: j.name || null, path: j.path || null, error: j.error || null, createdAt: cr, updatedAt: up, silentForMs: up ? now - up : null, result: j.result ? { charms: j.result.charms, labelled: j.result.labelled, blocked: (j.result.blocked || []).length } : null }; }) };
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
  const mode = ["grouping", "layout", "name", "place", "labelRead", "engraveIntent", "engraveReview"].includes(b.mode) ? b.mode : null;
  if (!mode || !b.payload) return { error: "mode and payload required" };
  const fnName = /^engrave/.test(mode) ? "charmEngrave-background" : "charmNestAgent-background";
  const id = "agent-" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  // Background functions accept only a small request body (the images made the
  // kick 413), so the payload is parked in Storage and the job reads it back.
  const payloadPath = `charmnest/agent/${id}.json`;
  await admin.storage().bucket().file(payloadPath).save(Buffer.from(JSON.stringify(b.payload)), { resumable: false, contentType: "application/json", metadata: { cacheControl: "no-store" } });
  await db.collection(AGENT).doc(id).set({ id, mode, status: "pending", payloadPath, sheetId: str(b.sheetId, 80), sourceName: str(b.sourceName, 120), createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() });
  const fetch = require("node-fetch");
  const base = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
  const kick = await fetch(`${base}/.netlify/functions/${fnName}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id, mode }) }).catch(err => ({ ok: false, status: 0, statusText: err.message }));
  if (!kick.ok && kick.status !== 202) { await db.collection(AGENT).doc(id).set({ status: "error", error: `kick failed: ${kick.status} ${kick.statusText || ""}`, updatedAt: FV.serverTimestamp() }, { merge: true }); return { error: `could not start the background job (${kick.status})` }; }
  return { ok: true, id };
}
async function op_getAgent(b) {
  if (!isId(b.id)) return { error: "bad id" };
  const s = await db.collection(AGENT).doc(b.id).get();
  if (!s.exists) return { job: null };
  const d = s.data(); return { job: { id: d.id, mode: d.mode, status: d.status, error: d.error || null, result: d.result || null, startedAt: ms(d.startedAt), createdAt: ms(d.createdAt) } };
}


/* ═══ Charm Sorter ⇄ Design Station bridge — master index, pool, backs, sets, runs, maps (design §12, §13) ═══
   Every query below is a single-field equality or range, like the rest of this file: no composite indexes. */
const Master = require("./_charmNestMaster");
const POOL = "Charm_Pool", BACK = "Charm_Pool_Back", SETS = "Charm_Nest_Sets", COUNTERS = "Charm_Nest_Counters", RUNS = "Charm_Nest_Runs", RELEASE = "Charm_Nest_Release", BRIDGE = "Design_Bridge", ALIASES = "Charm_Sku_Aliases", NODESIGN = "Charm_Sku_NoDesign", OPTMAP = "Charm_Option_Map";
const isDay = d => /^\d{4}-\d{2}-\d{2}$/.test(String(d || ""));
const isPoolId = s => /^\d{5,20}_\d{5,20}_\d{1,3}$/.test(String(s || ""));
const tokenUrl = async (path) => { if (!path) return null; try { const bucket = admin.storage().bucket(); const [meta] = await bucket.file(path).getMetadata(); let t = meta.metadata && meta.metadata.firebaseStorageDownloadTokens; if (!t) return null; t = String(t).split(",")[0]; return "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucket.name) + "/o/" + encodeURIComponent(path) + "?alt=media&token=" + encodeURIComponent(t); } catch (_) { return null; } };
async function withLinks(e) { if (!e) return e; const jobs = []; if (e.aiPath) jobs.push(tokenUrl(e.aiPath).then(u => { if (u) e.aiUrl = u; })); if (e.thumbPath) jobs.push(tokenUrl(e.thumbPath).then(u => { if (u) e.thumbUrl = u; })); for (const s of Object.values(e.sizes || {})) { if (s.aiPath) jobs.push(tokenUrl(s.aiPath).then(u => { if (u) s.aiUrl = u; })); if (s.thumbPath) jobs.push(tokenUrl(s.thumbPath).then(u => { if (u) s.thumbUrl = u; })); } await Promise.all(jobs); return e; }

// ── master index ──
async function op_masterPutIndex(b) { const r = await Master.putIndex(db, FV, b); return Object.assign({ ok: true }, r); }
async function op_masterGet(b) {
  const sku = String(b.sku || "").trim().toUpperCase(); if (!Master.isSku(sku)) return { entry: null, error: "bad sku" };
  const s = await db.collection(Master.INDEX).doc(sku).get();
  return { entry: s.exists ? await withLinks(Master.slimEntry(s.data())) : null };
}
async function op_masterGetMany(b) {
  const skus = [...new Set((b.skus || []).map(s => String(s).trim().toUpperCase()).filter(Master.isSku))].slice(0, 300); const out = {};
  for (let i = 0; i < skus.length; i += 30) { const snaps = await db.getAll(...skus.slice(i, i + 30).map(s => db.collection(Master.INDEX).doc(s))); for (const s of snaps) if (s.exists) out[s.id] = await withLinks(Master.slimEntry(s.data())); }
  return { entries: out };
}
async function op_masterList(b) {
  const q = str(b.q, 80).toUpperCase(); const limit = Math.min(3000, Math.max(1, num(b.limit) || 1500));
  const snap = await db.collection(Master.INDEX).limit(limit).get();
  let rows = snap.docs.filter(d => d.data().sku).map(d => Master.slimEntry(d.data()));   // a shell without a SKU (a patch on an unindexed SKU) is not an entry
  if (b.masterHash) rows = rows.filter(r => r.masterHash === b.masterHash);
  if (q) rows = rows.filter(r => String(r.sku || "").includes(q));
  rows.sort((x, y) => x.sku.localeCompare(y.sku));
  if (b.links) for (const r of rows) await withLinks(r);
  return { entries: rows };
}
async function op_masterPatch(b) {
  const sku = String(b.sku || "").trim().toUpperCase(); if (!Master.isSku(sku)) return { error: "bad sku" };
  const p = b.patch || {}; const doc = { updatedAt: FV.serverTimestamp() };
  if (typeof p.engravable === "boolean") { doc.engravable = p.engravable; doc.engravableBy = "operator"; }
  if (p.upAngle != null) { doc.upAngle = num(p.upAngle); doc.upSource = "operator"; }
  if (Array.isArray(p.backKeepOut)) doc.backKeepOut = p.backKeepOut.slice(0, 50);
  if (p.blocked === null) { doc.blocked = FV.delete(); doc.conflict = FV.delete(); }
  if (p.labelSource) doc.labelSource = str(p.labelSource, 20);
  if (p.confirmedBy) { doc.confirmedBy = str(p.confirmedBy, 80); doc.confirmedAt = FV.serverTimestamp(); }
  const ref = db.collection(Master.INDEX).doc(sku); if (!(await ref.get()).exists) return { error: "not indexed: " + sku };   // a patch never creates a shell entry
  await ref.set(doc, { merge: true });
  return { ok: true };
}
async function op_masterPutFile(b) { return Master.putFile(db, FV, b); }
async function op_masterListFiles() { const snap = await db.collection(Master.FILES).limit(200).get(); const rows = snap.docs.map(d => { const r = d.data(); r.indexedAt = ms(r.indexedAt); return r; }); rows.sort((a, b2) => (b2.indexedAt || 0) - (a.indexedAt || 0)); return { files: rows }; }
/** Remove one SKU from the index (a stray record, a SKU that should never have been read). */
async function op_masterRemoveSku(b) {
  const sku = String(b.sku || "").trim().toUpperCase(); if (!Master.isSku(sku)) return { error: "bad sku" };
  const ref = db.collection(Master.INDEX).doc(sku); if (!(await ref.get()).exists) return { error: "not indexed: " + sku };
  await ref.delete(); return { ok: true, sku };
}
async function op_masterRemoveFile(b) {
  const hash = str(b.masterHash, 80); if (!/^[0-9a-f]{8,64}$/i.test(hash)) return { error: "bad master hash" };
  // the file record first, then the SKUs in batches of 400: one delete at a time ran past the function's time limit on
  // a real master (1,800 SKUs) and left the index half removed
  await db.collection(Master.FILES).doc(hash).delete().catch(() => {});
  let removed = 0;
  for (;;) {
    const snap = await db.collection(Master.INDEX).where("masterHash", "==", hash).limit(400).get();
    if (snap.empty) break;
    const batch = db.batch(); snap.docs.forEach(d => batch.delete(d.ref)); await batch.commit(); removed += snap.size;
    if (snap.size < 400) break;
  }
  return { ok: true, removed };
}
/** Server-side indexing of a large master already uploaded to Storage: parks a job, kicks charmMaster-background. */
async function op_startMaster(b) {
  const path = str(b.path, 600); if (!path.startsWith("charmnest/")) return { error: "bad path" };
  const id = "master-" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  await db.collection(JOBS).doc(id).set({ id, kind: "master", status: "pending", path, name: str(b.name, 200), opts: b.opts || {}, createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() });
  const fetch = require("node-fetch");
  const base = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
  const kick = await fetch(`${base}/.netlify/functions/charmMaster-background`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id }) }).catch(err => ({ ok: false, status: 0, statusText: err.message }));
  if (!kick.ok && kick.status !== 202) { await db.collection(JOBS).doc(id).set({ status: "error", error: `kick failed: ${kick.status} ${kick.statusText || ""}`, updatedAt: FV.serverTimestamp() }, { merge: true }); return { error: `could not start the background job (${kick.status})` }; }
  return { ok: true, id };
}

// ── pool ──
async function op_poolPut(b) {
  const rows = (Array.isArray(b.pools) ? b.pools : [b.pool]).filter(p => p && isPoolId(p.poolId)).slice(0, 400);
  if (!rows.length) return { error: "no pool rows" };
  const out = { written: 0, contended: [] };
  /* Two runs contending for one line: a row claimed by a LIVE run (fresh within 24 h, not finished) belongs to that run.
     A run that was stopped or given up is not live, whatever its rows say: yesterday's stopped run kept 152 lines
     out of today's on the strength of rows it never finished, so the run itself is asked, once per run. */
  const runLive = new Map();
  const liveRun = async runId => { if (runLive.has(runId)) return runLive.get(runId); let live = true; try { const s = await col(RUNS).doc(runId).get(); const r = s.exists ? s.data() : null; live = !r ? true : (!["stopped", "complete", "abandoned"].includes(r.status) && r.step !== "complete"); /* no record at all: assume live */ } catch (_) {} runLive.set(runId, live); return live; };
  for (const p of rows) {
    const ref = col(POOL).doc(p.poolId); const ex = await ref.get(); const cur = ex.exists ? ex.data() : null;
    if (cur && cur.runId && p.runId && cur.runId !== p.runId && !["complete", "abandoned", "committed"].includes(cur.state) && (Date.now() - (ms(cur.updatedAt) || 0)) < 24 * 3600 * 1000 && await liveRun(cur.runId)) { out.contended.push({ poolId: p.poolId, runId: cur.runId }); continue; }
    const doc = Object.assign({}, p, { poolId: p.poolId, updatedAt: FV.serverTimestamp() }); if (!cur) doc.createdAt = FV.serverTimestamp();
    await ref.set(doc, { merge: true }); out.written++;
  }
  return Object.assign({ ok: true }, out);
}
async function op_poolUpdate(b) {
  const ids = (Array.isArray(b.poolIds) ? b.poolIds : [b.poolId]).filter(isPoolId).slice(0, 400); if (!ids.length) return { error: "bad pool id" };
  let batch = db.batch(), n = 0;
  for (const id of ids) { batch.set(col(POOL).doc(id), Object.assign({}, b.patch || {}, { updatedAt: FV.serverTimestamp() }), { merge: true }); if (++n >= 400) { await batch.commit(); batch = db.batch(); n = 0; } }
  if (n) await batch.commit();
  return { ok: true, count: ids.length };
}
async function op_poolList(b) {
  let q = col(POOL);
  if (b.runId) q = q.where("runId", "==", str(b.runId, 80)); else if (b.setId) q = q.where("setId", "==", str(b.setId, 80)); else if (b.sheetId) q = q.where("sheetId", "==", str(b.sheetId, 80)); else if (b.orderId) q = q.where("orderId", "==", str(b.orderId, 40)); else return { error: "runId, setId, sheetId or orderId required" };
  const snap = await q.limit(Math.min(2000, num(b.limit) || 1000)).get();
  return { pools: snap.docs.map(d => { const r = d.data(); r.updatedAt = ms(r.updatedAt); r.createdAt = ms(r.createdAt); return r; }) };
}
async function op_poolGet(b) {
  const ids = (b.poolIds || []).filter(isPoolId).slice(0, 300); const out = {};
  for (let i = 0; i < ids.length; i += 30) { const snaps = await db.getAll(...ids.slice(i, i + 30).map(id => col(POOL).doc(id))); for (const s of snaps) if (s.exists) { const r = s.data(); r.updatedAt = ms(r.updatedAt); out[s.id] = r; } }
  return { pools: out };
}
// ── backs ──
/* ── the sandbox snapshot: what the emulated Etsy serves (etsySandbox.js). The sorter uploads the JSON through
   charmNestOutput and records it here; reset clears the sandbox's own records so a run can start clean. ── */
const SANDBOX = "Charm_Sandbox";
async function op_sandboxPut(b) {
  const path = str(b.path, 300); if (!/^charmnest\/sandbox\//.test(path)) return { error: "the snapshot must live under charmnest/sandbox/" };
  const [exists] = await admin.storage().bucket().file(path).exists(); if (!exists) return { error: "no such snapshot file: " + path };
  const doc = { path, count: num(b.count) || 0, at: num(b.at) || Date.now(), takenBy: str(b.takenBy, 80) || null, note: str(b.note, 200) || null, updatedAt: FV.serverTimestamp() };
  await db.collection(SANDBOX).doc("current").set(doc);
  return { ok: true, snapshot: doc };
}
async function op_sandboxStatus() {
  const doc = await db.collection(SANDBOX).doc("current").get();
  const counts = {};
  for (const name of SANDBOXED) { const s = await db.collection("Sandbox_" + name).limit(500).select().get(); counts[name] = s.size; }
  return { ok: true, snapshot: doc.exists ? doc.data() : null, records: counts };
}
async function op_sandboxReset(b) {
  let deleted = 0;
  const names = [...SANDBOXED, "Design_Completed Orders", "Design_RealTime_Selected_Orders", "Design_Order_Archive", "Brites_Orders"];
  const SUBS = { Design_Bridge: ["log"], Brites_Orders: ["messages"] };   // deleting a document never deletes its subcollections
  const wipe = async q => { let n = 0; for (;;) { const s = await q.limit(300).get(); if (s.empty) break; const batch = db.batch(); s.docs.forEach(d => batch.delete(d.ref)); await batch.commit(); n += s.size; if (s.size < 300) break; } return n; };
  for (const name of names) {
    const coll = db.collection("Sandbox_" + name);
    for (const sub of SUBS[name] || []) { const parents = await coll.select().get(); for (const d of parents.docs) deleted += await wipe(d.ref.collection(sub)); }
    deleted += await wipe(coll);
  }
  void b; return { ok: true, deleted };
}
/* ── purge: every RECORD of past runs, in production and in the sandbox. Master files, the SKU index, aliases, option
   maps, calibration and the sandbox snapshot are not history and stay. Files in Storage are not touched here: a sheet
   folder nothing refers to is an orphan the app never shows, and clearing folders is a decision made in the console.
   Gated by the delete passcode. ── */
async function op_purgeHistory(b) {
  if (String(b.code || "") !== DELETE_CODE) return { error: "wrong passcode", status: 403 };
  /* Never under a run that is still working: a purge that ran while a set was nesting took that set's finished
     sheets with it, and the run re-saved its own record afterwards as if nothing had happened. */
  if (!b.force) {
    for (const prefix of ["", "Sandbox_"]) {
      const snap = await db.collection(prefix + RUNS).where("status", "in", ["running", "review", "paused"]).limit(5).get();
      const live = snap.docs.map(d => d.data()).filter(r => ["running", "review", "paused"].includes(r.status) && Date.now() - (ms(r.updatedAt) || 0) < 6 * 3600 * 1000);
      if (live.length) return { error: `a run is still open (${live.map(r => r.runId).join(", ")}) — stop or abandon it first`, status: 409 };
    }
  }
  const names = [RUNS, SHEETS, SETS, POOL, BACK, COUNTERS, RELEASE, BRIDGE];
  const SUBS = { [BRIDGE]: ["log"] };
  const wipe = async q => { let n = 0; for (;;) { const s = await q.limit(300).get(); if (s.empty) break; const batch = db.batch(); s.docs.forEach(d => batch.delete(d.ref)); await batch.commit(); n += s.size; if (s.size < 300) break; } return n; };
  const docs = {};
  for (const prefix of ["", "Sandbox_"]) for (const name of names) {
    const coll = db.collection(prefix + name);
    for (const sub of SUBS[name] || []) { const parents = await coll.select().get(); for (const d of parents.docs) await wipe(d.ref.collection(sub)); }
    docs[prefix + name] = await wipe(coll);
  }
  return { ok: true, docs };
}
/* ── restore: a sheet record rebuilt from the files its run wrote (the nest report, the outputs, the preview), for a
   record that was lost while its files were not. The folder is the sheet's own: …/sets/<day>/Set-<n>/<fileBase>/ ── */
async function op_restoreSheet(b) {
  const folder = str(b.folder, 400).replace(/\/+$/, ""); if (!/^charmnest\/(sandbox\/)?(sets|sheets)\//.test(folder)) return { error: "bad folder" };
  const name = folder.split("/").pop(); const bucket = admin.storage().bucket();
  const rp = `${folder}/${name}_nest-report.json`; const [exists] = await bucket.file(rp).exists(); if (!exists) return { error: "no nest report at " + rp };
  const [buf] = await bucket.file(rp).download(); const rep = JSON.parse(buf.toString("utf8"));
  const m = /\/sets\/(\d{4}-\d{2}-\d{2})\/Set-(\d+)\//.exec(folder + "/"); const day = m ? m[1] : (rep.createdAt ? String(rep.createdAt).slice(0, 10) : null); const seq = m ? +m[2] : null;
  const setId = seq && day ? `set-${day}-${seq}` : null;
  let runId = str(b.runId, 80) || null; if (!runId && setId) { const st = await col(SETS).doc(setId).get(); if (st.exists) runId = st.data().runId || null; }
  const id = str(b.id, 80) || rep.sheetId; if (!isId(id)) return { error: "no sheet id" };
  const out = {}; for (const [k, suffix] of [["ai", `${name}.ai`], ["pdf", `${name}.pdf`], ["labelled", `${name}_labelled.pdf`], ["report", `${name}_nest-report.json`], ["preview", "preview.png"]]) { const p = `${folder}/${suffix}`; const [ok] = await bucket.file(p).exists(); out[k] = ok ? { path: p, url: await tokenUrl(p) } : null; }
  const placements = rep.placements || [], charms = rep.charms || [];
  const orders = [...new Set(placements.map(p => String(p.name || p.layer || "").split("·")[0].trim()).filter(x => /^\d{6,}$/.test(x)))];
  const doc = { id, metal: rep.metal, metalLabel: rep.metalLabel || null, day, folder: name, fileBase: name, seq, runId, setId, setSeq: seq, sheetIndex: +((/_Sheet-(\d+)$/.exec(name) || [])[1]) || null,
    status: "complete", endedBy: rep.endedBy || null, trials: rep.trials || 0, elapsedMs: rep.elapsedMs || 0, stock: rep.stock || null, params: rep.params || null, density: rep.density || 0, freePt2: Math.round(rep.freePt2 || 0), usablePt2: Math.round(rep.usablePt2 || 0), pocket: rep.pocket || null,
    charmCount: charms.length || placements.length, placedCount: placements.length, rejectCount: (rep.rejects || []).length, page: 1, verification: rep.verification ? { ok: !!rep.verification.ok, minGapPt: rep.verification.minGapPt, minEdgePt: rep.verification.minEdgePt } : null,
    outputs: out, sources: [], orders, poolIds: charms.map(c => c.poolId).filter(Boolean), backPool: [], backOutputs: null, label: null, charms, placements, rejects: rep.rejects || [], names: charms.map(c => c.name).filter(Boolean).join(" "), restored: true, archived: false, updatedAt: FV.serverTimestamp() };
  const ref = col(SHEETS).doc(id); const ex = await ref.get(); if (!ex.exists) doc.createdAt = FV.serverTimestamp();
  await ref.set(doc, { merge: true });
  if (setId) { const st = col(SETS).doc(setId); const sd = await st.get(); if (sd.exists) { const ids = new Set(sd.data().sheetIds || []); ids.add(id); await st.set({ sheetIds: [...ids] }, { merge: true }); } }
  return { ok: true, id, fileBase: name, placed: placements.length, orders: orders.length, outputs: Object.fromEntries(Object.entries(out).map(([k, v]) => [k, !!v])) };
}
async function op_backPut(b) {
  const rows = (Array.isArray(b.backs) ? b.backs : [b.back]).filter(x => x && isPoolId(x.poolId)).slice(0, 400); if (!rows.length) return { error: "no back rows" };
  let batch = db.batch(), n = 0;
  for (const x of rows) { batch.set(col(BACK).doc(x.poolId), Object.assign({}, x, { updatedAt: FV.serverTimestamp() }), { merge: true }); if (++n >= 400) { await batch.commit(); batch = db.batch(); n = 0; } }
  if (n) await batch.commit();
  return { ok: true, count: rows.length };
}
async function op_backList(b) {
  let q = col(BACK);
  if (b.sheetId) q = q.where("sheetId", "==", str(b.sheetId, 80)); else if (b.setId) q = q.where("setId", "==", str(b.setId, 80)); else if (b.runId) q = q.where("runId", "==", str(b.runId, 80)); else return { error: "sheetId, setId or runId required" };
  const snap = await q.limit(2000).get();
  return { backs: snap.docs.map(d => { const r = d.data(); r.updatedAt = ms(r.updatedAt); return r; }) };
}
// ── sets: the number is allocated in a transaction, per date, across every material ──
async function op_setAllocate(b) {
  const day = str(b.day, 10); if (!isDay(day)) return { error: "bad day" };
  const runId = str(b.runId, 80), group = str(b.group, 120) || "";
  /* Idempotent per (run, kin group): a run's SS set and its GF+14K set are two sets with two numbers, and asking for
     either again returns the one already allocated. Every set of the day counts up the same day counter, so the second
     run of the day is Set-2 whatever the first run made. Nothing is ever renumbered. */
  const key = group ? `${runId}|${group}` : runId;
  if (runId) { const ex = await col(SETS).where("key", "==", key).limit(1).get(); if (!ex.empty) { const d = ex.docs[0].data(); return { ok: true, setId: d.setId, seq: d.seq, day: d.day, existing: true }; }
    if (!group) { const ex2 = await col(SETS).where("runId", "==", runId).limit(1).get(); if (!ex2.empty) { const d = ex2.docs[0].data(); return { ok: true, setId: d.setId, seq: d.seq, day: d.day, existing: true }; } } }
  const res = await db.runTransaction(async t => {
    const allocation = runId ? col(COUNTERS).doc("allocation-" + require("crypto").createHash("sha256").update(key).digest("hex").slice(0, 40)) : null;
    if (allocation) { const prior = await t.get(allocation); if (prior.exists) return prior.data(); }
    const cref = col(COUNTERS).doc(day); const cs = await t.get(cref);
    const seq = (cs.exists ? num(cs.data().seq) : 0) + 1;
    const setId = `set-${day}-${seq}`;
    t.set(cref, { day, seq, updatedAt: FV.serverTimestamp() }, { merge: true });
    t.set(col(SETS).doc(setId), { setId, runId: runId || null, key, group: group || null, day, seq, name: `Set-${seq}`, folder: `charmnest/sets/${day}/Set-${seq}`, materials: [], sheetIds: [], orders: {}, labels: null, status: "open", createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() });
    if (allocation) t.set(allocation, { setId, seq });
    return { setId, seq };
  });
  return Object.assign({ ok: true, day }, res);
}
async function op_setUpdate(b) {
  const id = str(b.setId, 80); if (!isId(id)) return { error: "bad set id" };
  await col(SETS).doc(id).set(Object.assign({}, b.patch || {}, { setId: id, updatedAt: FV.serverTimestamp() }), { merge: true });
  return { ok: true };
}
async function op_setGet(b) { const id = str(b.setId, 80); if (!isId(id)) return { error: "bad set id" }; const s = await col(SETS).doc(id).get(); if (!s.exists) return { set: null }; const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt); d.committedAt = ms(d.committedAt) || d.committedAt || null; return { set: d }; }
async function op_setList(b) {
  let q = col(SETS).orderBy("day", "desc");
  if (isDay(b.from)) q = q.where("day", ">=", b.from); if (isDay(b.to)) q = q.where("day", "<=", b.to);
  const snap = await q.limit(Math.min(500, num(b.limit) || 200)).get();
  const rows = snap.docs.map(d => { const r = d.data(); r.updatedAt = ms(r.updatedAt); r.createdAt = ms(r.createdAt); return r; });
  if (b.status) return { sets: rows.filter(r => r.status === b.status) };
  return { sets: rows };
}
// ── release: when each slow material last went out, and the days a person opened one early (shop-wide, not per browser) ──
async function op_releaseGet() { const s = await col(RELEASE).doc("current").get(); const d = s.exists ? s.data() : {}; return { lastReleased: d.lastReleased || {}, released: d.released || {}, updatedAt: ms(d.updatedAt) }; }
async function op_releasePut(b) {
  const ok = v => v && typeof v === "object" && Object.entries(v).every(([k, d]) => /^[a-z0-9]{1,16}$/.test(k) && isDay(d));
  const patch = { updatedAt: FV.serverTimestamp() };
  if (b.lastReleased !== undefined) { if (!ok(b.lastReleased)) return { error: "bad lastReleased" }; patch.lastReleased = b.lastReleased; }
  if (b.released !== undefined) { if (!ok(b.released)) return { error: "bad released" }; patch.released = b.released; }
  await col(RELEASE).doc("current").set(patch, { merge: true });
  return op_releaseGet();
}
async function op_archiveEmptySheet(b) {
  if (!isId(b.id) || !isId(b.runId)) return { error: "bad sheet/run id" };
  const ref = col(SHEETS).doc(b.id), sheet = await ref.get();
  if (!sheet.exists || sheet.data().runId !== b.runId) return { error: "sheet does not belong to this run" };
  const run = await col(RUNS).doc(b.runId).get();
  if (!run.exists || ["complete", "abandoned"].includes(run.data().status)) return { error: "finished sheets cannot be changed by intake" };
  await ref.set({ archived: true, archivedReason: "open sheets repacked", updatedAt: FV.serverTimestamp() }, { merge: true });
  return { ok: true };
}
// ── arrival ledger, separate in production and sandbox ──
async function op_arrivalRecord(b) {
  const receipts = [...new Map((b.orders || []).filter(o => /^\d{1,30}$/.test(String(o.id))).map(o => [String(o.id), o])).values()];
  if (receipts.length > 5000) return { error: "too many receipts" };
  const now = Date.now(), collection = col("Charm_Nest_Arrivals"), firstSeen = {};
  // Existing receipts need one batched read; transact only first arrivals.
  const known = receipts.length ? await db.getAll(...receipts.map(o => collection.doc(String(o.id)))) : [];
  const missing = receipts.filter((o, i) => { if (!known[i].exists) return true; firstSeen[String(o.id)] = known[i].data().firstSeenAt; return false; });
  // Bound concurrency; transactions keep simultaneous stations from counting an order twice.
  let cursor = 0;
  await Promise.all(Array.from({ length: Math.min(8, missing.length) }, async () => {
    while (cursor < missing.length) {
      const o = missing[cursor++], id = String(o.id), ref = collection.doc(id);
      firstSeen[id] = await db.runTransaction(async t => {
        const old = await t.get(ref); if (old.exists) return old.data().firstSeenAt;
        t.set(ref, { id, firstSeenAt: now, createTs: num(o.createTs) }); return now;
      });
    }
  }));
  const recent = await collection.where("firstSeenAt", ">=", now - 86400000).get();
  const times = recent.docs.map(d => d.data());
  for (const row of times) firstSeen[row.id] = row.firstSeenAt;
  return { ok: true, firstSeen, count24: times.length, count1: times.filter(r => r.firstSeenAt > now - 3600000).length, at: now };
}
// ── runs ──
async function op_runPut(b) {
  const r = b.run || {}; const id = str(r.runId, 80); if (!isId(id)) return { error: "bad run id" };
  const doc = Object.assign({}, r, { runId: id, updatedAt: FV.serverTimestamp() });
  const ref = col(RUNS).doc(id); const ex = await ref.get(); doc.createdAt = ex.exists ? (ex.data().createdAt || FV.serverTimestamp()) : FV.serverTimestamp();
  await ref.set(doc, { merge: !!b.merge });
  return { ok: true, runId: id };
}
async function op_runGet(b) { const id = str(b.runId, 80); if (!isId(id)) return { error: "bad run id" }; const s = await col(RUNS).doc(id).get(); if (!s.exists) return { run: null }; const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt); return { run: d }; }
async function op_runList(b) {
  const snap = await col(RUNS).orderBy("updatedAt", "desc").limit(Math.min(200, num(b.limit) || 50)).get();
  let rows = snap.docs.map(d => { const r = d.data(); return { runId: r.runId, setId: r.setId || null, day: r.day, step: r.step, status: r.status, mode: r.mode || null, lines: r.lines ? Object.keys(r.lines).length : 0, holds: r.holds ? Object.keys(r.holds).length : 0, errors: (r.errors || []).length, updatedAt: ms(r.updatedAt), createdAt: ms(r.createdAt), stoppedBy: r.stoppedBy || null }; });
  if (b.status) rows = rows.filter(r => r.status === b.status);
  return { runs: rows };
}
/* "Find that order from last Tuesday." There is no index that can answer it: Firestore has no full-text search, and a
   shop's history of runs is small enough to read. So run summaries and sheet metadata are searched here, before result pagination, and
   the answer says how many of each it actually looked at — a search that quietly stopped early is worse than none. */
async function op_history(b) {
  const q = String(b.q || "").trim().toLowerCase(), offset = Math.max(0, num(b.offset)), limit = Math.min(100, Math.max(1, num(b.limit) || 60));
  // Search complete set membership before pagination: a matching order must reveal all of its set's sheets.
  const [rs, ss, ts] = await Promise.all([col(RUNS).select("runId", "setId", "seq", "day", "status", "step", "lines", "sheets", "errors", "stoppedBy", "createdAt", "updatedAt").get(), col(SHEETS).select("id", "setId", "setSeq", "runId", "day", "metal", "metalLabel", "status", "orders", "sheetIndex", "page", "updatedAt", "archived", "folder", "fileBase", "saving", "endedBy", "charmCount", "placedCount", "rejectCount", "density", "freePt2", "verification", "outputs", "names", "charms").get(), col(SETS).select("seq", "day", "runId", "status", "updatedAt", "materials", "orders").get()]);
  const runMap = new Map(rs.docs.map(d => [d.id, d.data()])), sheets = ss.docs.map(d => d.data()).filter(x => !x.archived);
  const groups = new Map(ts.docs.map(d => { const x = d.data(); return [d.id, { setId: d.id, seq: x.seq, day: x.day, runId: x.runId, status: x.status, updatedAt: ms(x.updatedAt), sheets: [], materials: x.materials || [], orderIds: Object.keys(x.orders || {}), search: [] }]; }));
  for (const x of sheets) {
    const key = x.setId || "sheet:" + x.id;
    if (!groups.has(key)) groups.set(key, { setId: x.setId || null, seq: x.setSeq || null, day: x.day, runId: x.runId, status: x.status, sheets: [], materials: [], orderIds: [], search: [] });
    const g = groups.get(key); g.sheets.push(Object.assign(slim(x), { orders: (x.orders || []).length, orderIds: x.orders || [], sheetIndex: x.sheetIndex || x.page || 1 }));
    if (!g.materials.includes(x.metal)) g.materials.push(x.metal);
    g.search.push(x.names || "", x.metalLabel || "", ...(x.charms || []).map(c => c.sku || ""));
    g.orderIds.push(...(x.orders || [])); g.updatedAt = Math.max(g.updatedAt || 0, ms(x.updatedAt) || 0);
  }
  const runRows = [...runMap.values()].map(r => ({ runId: r.runId, setId: r.setId || null, seq: r.seq || null, day: r.day, status: r.status, step: r.step, lines: Object.keys(r.lines || {}).length, orders: new Set(Object.values(r.lines || {}).map(l => l.orderId)).size, sheets: Object.keys(r.sheets || {}).length, updatedAt: ms(r.updatedAt), createdAt: ms(r.createdAt), stoppedBy: r.stoppedBy || null, hitOrders: [...new Set(Object.values(r.lines || {}).map(l => String(l.orderId || "")))].filter(x => q && x.toLowerCase().includes(q)), hitSkus: [...new Set(Object.values(r.lines || {}).map(l => String(l.sku || "")))].filter(x => q && x.toLowerCase().includes(q)) }));
  for (const r of runRows) if (![...groups.values()].some(g => g.runId === r.runId)) groups.set("run:" + r.runId, { setId: r.setId, seq: r.seq, runId: r.runId, day: r.day, status: r.status, sheets: [], materials: [], orderIds: [], orders: r.orders, updatedAt: r.updatedAt });
  let rows = [...groups.values()].map(g => {
    const r = runMap.get(g.runId), ids = new Set(g.orderIds.map(String));
    const lines = Object.values(r?.lines || {}).filter(l => !ids.size || ids.has(String(l.orderId)));
    const hay = [g.setId, "Set " + g.seq, g.day, g.runId, g.status, r?.stoppedBy, ...(r?.errors || []).map(e => e.why), ...g.materials, ...(g.search || []), ...ids, ...lines.flatMap(l => [l.orderId, l.sku, l.engrave?.text, l.snap?.title]), ...g.sheets.map(x => x.fileBase)].join(" ").toLowerCase();
    return Object.assign(g, { orders: ids.size || g.orders || new Set(lines.map(l => l.orderId)).size, status: g.status === "superseded" ? g.status : r?.status === "complete" ? (String(g.status).startsWith("complete") ? g.status : "complete") : r?.status || g.status, match: !q || hay.includes(q) });
  }).filter(g => g.match).sort((a, b) => String(b.day || "").localeCompare(String(a.day || "")) || (b.seq || 0) - (a.seq || 0) || (b.updatedAt || 0) - (a.updatedAt || 0));
  const total = rows.length; rows = rows.slice(offset, offset + limit);
  for (const row of rows) { delete row.search; delete row.match; }
  return { sets: rows, runs: runRows.filter(r => rows.some(g => g.runId === r.runId)), sheets: rows.flatMap(g => g.sheets), total, nextOffset: offset + rows.length < total ? offset + rows.length : null,
    scanned: { runs: rs.size, sheets: ss.size }, truncated: { runs: false, sheets: false } };
}
// ── bridge session log: Design_Bridge/{session} + /log rows (ids and counts only, never order text) ──
async function op_bridgeLog(b) {
  const session = str(b.session, 80); if (!/^[\w\-]{6,80}$/.test(session)) return { error: "bad session" };
  const ref = col(BRIDGE).doc(session);
  const meta = Object.assign({}, b.meta || {}, { sessionId: session, updatedAt: FV.serverTimestamp() });
  const rows = (b.rows || []).slice(0, 200);
  if (rows.length) meta.commands = FV.increment(rows.filter(r => r.dir === "cmd").length);
  await ref.set(meta, { merge: true });
  let batch = db.batch(), n = 0;
  for (const r of rows) { batch.set(ref.collection("log").doc(), { t: num(r.t) || Date.now(), dir: str(r.dir, 10), type: str(r.type, 40), ms: r.ms == null ? null : num(r.ms), payload: r.payload && typeof r.payload === "object" ? r.payload : (r.payload == null ? null : str(r.payload, 400)) }); if (++n >= 400) { await batch.commit(); batch = db.batch(); n = 0; } }
  if (n) await batch.commit();
  return { ok: true, rows: rows.length };
}
// ── learned maps: aliases, no-design list, option maps ──
async function op_aliasGet() { const snap = await db.collection(ALIASES).limit(3000).get(); const out = {}; snap.docs.forEach(d => { out[d.id] = d.data(); }); return { aliases: out }; }
async function op_aliasPut(b) { const lid = str(b.listingId, 30).replace(/\D/g, ""); const sku = String(b.sku || "").trim().toUpperCase(); if (!lid || !Master.isSku(sku)) return { error: "listingId and sku required" }; await db.collection(ALIASES).doc(lid).set({ listingId: lid, sku, by: str(b.by || "operator", 80), title: str(b.title, 200), updatedAt: FV.serverTimestamp() }, { merge: true }); return { ok: true }; }
async function op_noDesignGet() { const snap = await db.collection(NODESIGN).limit(1000).get(); const rows = snap.docs.map(d => Object.assign({ id: d.id }, d.data())); return { list: { patterns: rows.filter(r => r.pattern).map(r => r.pattern), skus: rows.filter(r => r.sku).map(r => r.sku), rows } }; }
async function op_noDesignPut(b) { const doc = { by: str(b.by || "operator", 80), note: str(b.note, 200), createdAt: FV.serverTimestamp() }; if (b.pattern) { try { new RegExp(String(b.pattern)); } catch (_) { return { error: "bad pattern" }; } doc.pattern = str(b.pattern, 120); } else if (b.sku) doc.sku = String(b.sku).trim().toUpperCase().slice(0, 40); else return { error: "pattern or sku required" }; const ref = await db.collection(NODESIGN).add(doc); return { ok: true, id: ref.id }; }
async function op_noDesignDelete(b) { if (!isId(b.id)) return { error: "bad id" }; await db.collection(NODESIGN).doc(b.id).delete(); return { ok: true }; }
async function op_optionMapGet() { const snap = await db.collection(OPTMAP).limit(2000).get(); const out = {}; snap.docs.forEach(d => { out[d.id] = d.data().map || {}; }); return { maps: out }; }
async function op_optionMapPut(b) {
  const lid = b.listingId === "*" ? "*" : str(b.listingId, 30).replace(/\D/g, ""); const name = str(b.optionName, 80).toLowerCase().trim(), value = str(b.optionValue, 200).toLowerCase().replace(/\s+/g, " ").trim();
  if (!lid || !name || !value) return { error: "listingId, optionName and optionValue required" };
  const m = b.map || {}; const field = ["form", "size", "chain", "ignore"].includes(m.field) ? m.field : null; if (!field) return { error: "map.field must be form, size, chain or ignore" };
  const ref = db.collection(OPTMAP).doc(lid); const snap = await ref.get(); const cur = snap.exists ? (snap.data().map || {}) : {};
  cur[name] = cur[name] || {}; cur[name][value] = { field, value: field === "ignore" ? null : str(m.value, 80), by: str(b.by || "operator", 80), at: Date.now() };
  await ref.set({ listingId: lid, map: cur, updatedAt: FV.serverTimestamp() }, { merge: true });
  return { ok: true };
}

const OPS = { archiveEmptySheet: op_archiveEmptySheet, arrivalRecord: op_arrivalRecord, startAgent: op_startAgent, getAgent: op_getAgent, ping: op_ping, lookupCharms: op_lookupCharms, putCharms: op_putCharms, renameCharm: op_renameCharm, listCharms: op_listCharms, putSheet: op_putSheet, listSheets: op_listSheets, getSheet: op_getSheet, deleteSheet: op_deleteSheet, purgeHistory: op_purgeHistory, restoreSheet: op_restoreSheet, putCalibration: op_putCalibration, getCalibration: op_getCalibration, startJob: op_startJob, getJob: op_getJob, stopJob: op_stopJob,
  masterPutIndex: op_masterPutIndex, masterGet: op_masterGet, masterGetMany: op_masterGetMany, masterList: op_masterList, masterPatch: op_masterPatch, masterPutFile: op_masterPutFile, masterListFiles: op_masterListFiles, masterRemoveFile: op_masterRemoveFile, masterRemoveSku: op_masterRemoveSku, startMaster: op_startMaster,
  jobList: op_jobList, poolPut: op_poolPut, poolUpdate: op_poolUpdate, poolList: op_poolList, poolGet: op_poolGet, backPut: op_backPut, backList: op_backList, sandboxPut: op_sandboxPut, sandboxStatus: op_sandboxStatus, sandboxReset: op_sandboxReset,
  setAllocate: op_setAllocate, setUpdate: op_setUpdate, setGet: op_setGet, setList: op_setList, runPut: op_runPut, runGet: op_runGet, runList: op_runList, history: op_history, releaseGet: op_releaseGet, releasePut: op_releasePut, bridgeLog: op_bridgeLog,
  aliasGet: op_aliasGet, aliasPut: op_aliasPut, noDesignGet: op_noDesignGet, noDesignPut: op_noDesignPut, noDesignDelete: op_noDesignDelete, optionMapGet: op_optionMapGet, optionMapPut: op_optionMapPut };

exports.ops = OPS;   // the connections check runs the same queries the app runs
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") return { statusCode: 204, headers: require("./_charmNestAuth").CORS, body: "" };
  const body = event.httpMethod === "GET" ? Object.assign({}, event.queryStringParameters || {}) : parseBody(event);
  const denied = gate(event, body); if (denied) return denied;
  PREFIX = body.sandbox === true || body.sandbox === 1 || body.sandbox === "1" ? "Sandbox_" : "";
  const fn = OPS[body.op];
  if (!fn) return json(400, { error: "unknown op", ops: Object.keys(OPS) });
  try {
    const out = await fn(body);
    return json(out && out.error ? (out.status || 400) : 200, out);
  } catch (e) {
    console.error("[charmNestLibrary]", body.op, e);
    return json(500, { error: e.message || String(e) });
  }
};
