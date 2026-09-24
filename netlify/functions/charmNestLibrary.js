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
 *    Charm_Nest_Run_Lines/{run~hash} the lines of orders a run is done with, kept
 *                                    out of its record (op_runArchive)
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
 *      setAllocate · setUpdate · setGet · setList · runPut · runArchive · runGet · runList · bridgeLog · aliasGet · aliasPut ·
 *      noDesignGet · noDesignPut · noDesignDelete · optionMapGet · optionMapPut
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { json, gate, parseBody, str, num } = require("./_charmNestAuth");
const db = admin.firestore();
const OrderRules = require("../../charm-nest-orders.js");
const Readiness = require("../../charm-nest-readiness.js");
// A run's archived lines keep the readiness policy's decisions as they were when written (op_runArchive); a part written
// under another version of Readiness.decisions is decided again from its lines when read (decisionsOfRun).
const DECISIONS_VERSION = require("crypto").createHash("sha256").update(String(Readiness.decisions)).digest("hex").slice(0, 16);
/* ── sandbox: when a request says sandbox:true, the sorter's OWN records (sheets, pools, backs, sets, counters, runs,
   bridge log) go to Sandbox_-prefixed collections; the master index, the charm library, maps and calibration stay
   shared and are only read. Set per request; a function instance handles one request at a time. ── */
let PREFIX = "";
const SANDBOXED = new Set(["Charm_Nest_Rose_Stock", "Charm_Nest_Sheets", "Charm_Pool", "Charm_Pool_Back", "Charm_Nest_Sets", "Charm_Nest_Counters", "Charm_Nest_Runs", "Charm_Nest_Run_Lines", "Charm_Nest_Release", "Charm_Nest_Arrivals", "Design_Bridge"]);
const col = name => db.collection(SANDBOXED.has(name) ? PREFIX + name : name);
const FV = admin.firestore.FieldValue;

const LIB = "Charm_Nest_Library", SHEETS = "Charm_Nest_Sheets", CAL = "Charm_Nest_Calibration", JOBS = "Charm_Nest_Jobs";
const MAX_PUT = 200;
// Reusable geometry-only guidance is shared across materials and workspaces.
// It contains no order/customer data. Sandbox tests use a separate cache.
const ShapeCache = (() => {
const crypto = require("crypto");
const { normalizePackingPlan } = require("../../charm-nest-solver.js");
const VERSION = 1;
function validKey(key) {
  if (typeof key !== "string" || key.length > 512) return false;
  try {
    const a = JSON.parse(key);
    return Array.isArray(a) && a.length === 5 && typeof a[0] === "string" && a[0].length > 0 && a.slice(1).every(n => Number.isFinite(n) && n > 0);
  } catch (_) { return false; }
}
const idFor = key => crypto.createHash("sha256").update(VERSION + ":" + key).digest("hex");
function cleanProfile(p) {
  if (!p || (p.cacheVersion != null && p.cacheVersion !== VERSION)) return null;
  const normalized = normalizePackingPlan({ profiles: [{ ...p, index: 0 }] }, 1).profiles[0];
  if (!normalized) return null;
  const { index, ...profile } = normalized;
  const partners = Object.fromEntries(Object.entries(p.partners || {}).filter(([k, v]) => validKey(k) && Number.isFinite(v)).slice(0,240).map(([k,v]) => [k, Math.max(0,Math.min(100,v))]));
  return { ...profile, partners, cacheVersion: VERSION, model: typeof p.model === "string" ? p.model.slice(0,100) : null };
}
return { VERSION, validKey, idFor, cleanProfile };
})();
const SHAPE_CACHE = "Charm_Nest_Shape_Guidance";
async function op_getShapeGuidance(b) {
  const keys = [...new Set((Array.isArray(b.keys) ? b.keys : []).filter(ShapeCache.validKey))].slice(0,100);
  const docs = keys.length ? await db.getAll(...keys.map(k => db.collection(PREFIX + SHAPE_CACHE).doc(ShapeCache.idFor(k)))) : [];
  const profiles = {};
  docs.forEach((doc,i) => {
    const d = doc.exists && doc.data(), p = d && d.key === keys[i] && d.version === ShapeCache.VERSION && ShapeCache.cleanProfile(d.profile);
    if (p) profiles[keys[i]] = p;
  });
  return { profiles, version: ShapeCache.VERSION };
}
async function op_putShapeGuidance(b) {
  const records = (Array.isArray(b.profiles) ? b.profiles : []).slice(0,50);
  let count = 0;
  for (let offset = 0; offset < records.length; offset += 10) await Promise.all(records.slice(offset,offset+10).map(async row => {
    const profile = ShapeCache.cleanProfile(row?.profile);
    if (!ShapeCache.validKey(row?.key) || !profile) return;
    const ref = db.collection(PREFIX + SHAPE_CACHE).doc(ShapeCache.idFor(row.key));
    await db.runTransaction(async tx => {
      const doc = await tx.get(ref), old = doc.exists && doc.data();
      const saved = old && old.key === row.key && old.version === ShapeCache.VERSION && ShapeCache.cleanProfile(old.profile);
      // Concurrent material sheets cannot erase one another's known neighbors.
      const merged = ShapeCache.cleanProfile({ ...(saved || profile), partners: { ...saved?.partners, ...profile.partners } });
      tx.set(ref, { key: row.key, version: ShapeCache.VERSION, profile: merged, updatedAt: FV.serverTimestamp() });
    });
    count++;
  }));
  return { ok:true, count };
}
const isHash = h => /^[0-9a-f]{8,64}$/i.test(String(h || ""));
const isId = s => /^[\w\-]{4,80}$/.test(String(s || ""));
const ms = v => (v && v.toMillis ? v.toMillis() : (typeof v === "number" ? v : null));

function slim(d) {
  return {
    id: d.id, roseStockId:d.roseStockId || null, roseCutAt:d.roseCutAt || null, rosePlanHash:d.rosePlanHash || null, solidIncluded:d.solidIncluded == null ? null : !!d.solidIncluded, draft: !!d.draft, releaseFull: !!d.releaseFull, folder: d.folder || null, fileBase: d.fileBase || d.folder || null, saving: !!d.saving, metal: d.metal, metalLabel: d.metalLabel, day: d.day, status: d.status, endedBy: d.endedBy,
    charmCount: num(d.charmCount), placedCount: num(d.placedCount), rejectCount: num(d.rejectCount), density: num(d.density), freePt2: num(d.freePt2),
    verification: d.verification ? { ok: !!d.verification.ok } : null,
    preview: d.outputs && d.outputs.preview ? d.outputs.preview.url : null,
    // the four files a recalled card offers, so recalling a set is one read of this list and nothing more
    outputs: d.outputs ? Object.fromEntries(["ai", "pdf", "labelled", "report"].filter(k => d.outputs[k] && d.outputs[k].url).map(k => [k, d.outputs[k].url])) : {},
    stock: d.stock || null, poolIds: d.poolIds || [], engraving:d.engraving || {}, laser:d.laser || null,
    backs: (d.backPool || []).map(bk => ({ poolId: bk.poolId || null, previewWPt:bk.previewWPt || null, previewHPt:bk.previewHPt || null, pageWPt:bk.pageWPt || null, pageHPt:bk.pageHPt || null, sheetId:bk.sheetId || d.id, invalidated:!!bk.invalidated, copy:bk.copy || null, approvedAt:bk.approvedAt || null, order: bk.order || null, sku: bk.sku || null, text: bk.text || null, lines: bk.lines || null, approvedBy: bk.approvedBy || null, verified:bk.verified || null, outputs:bk.outputs || null, capMm: bk.capMm || null, png: bk.outputs && bk.outputs.png ? bk.outputs.png.url : null, ai: bk.outputs && bk.outputs.ai ? bk.outputs.ai.url : null })),
    names: str(d.names, 2000), sources: (d.sources || []).map(s => ({ name: s.name, hash: s.hash || null })), runId: d.runId || null, page: num(d.page) || 1,
    setId: d.setId || null, setSeq: num(d.setSeq) || null, sheetIndex: num(d.sheetIndex) || null, orders: (d.orders || []).slice(0, 500), backCount: (d.backPool || []).length, label: d.label ? { files: (d.label.files || []).map(f => ({ path: f.path, url: f.url, payload:f.payload || null, orders:f.orders || [], part:f.part || 1 })) } : null,
    cardStartedAt: ms(d.cardStartedAt) || ms(d.createdAt), updatedAt: ms(d.updatedAt), createdAt: ms(d.createdAt)
  };
}

// One run read per batch supplies the exact per-copy engraving decisions.
// Old/incomplete records remain pending until evidence is available.
async function readinessRecords(records) {
  const runIds=[...new Set(records.map(s=>s.runId).filter(isId))], runs=new Map();
  for(let i=0;i<runIds.length;i+=100) {
    const docs=await db.getAll(...runIds.slice(i,i+100).map(id=>col(RUNS).doc(id)));
    docs.forEach(d=>{if(d.exists)runs.set(d.id,d.data());});
  }
  // A run's line archive is read only for a run whose sheets name a copy its record no longer holds.
  const decided=new Map();
  await Promise.all([...runs].map(async([id,run])=>decided.set(id,await decisionsOfRun(id,run,records.filter(s=>s.runId===id).flatMap(s=>s.poolIds || [])))));
  return records.map(s=>{
    const engraving=decided.get(s.runId) || {};
    const record={...s,engraving:Object.fromEntries((s.poolIds || []).map(id=>[id,engraving[id] || {needed:true,state:'unknown',approved:false}]))};
    record.laser=Readiness.sheet(record);return record;
  });
}
/* A run's record is one document and used to fill up with every line the run ever took. The page now moves the lines of
   an order the run is done with to the run's line archive (op_runArchive) and leaves them out of the record, which then
   says so (lineArchive). Whoever still wants such a line reads the archive under the record: the record's lines are
   newer and win, and a later part wins over an earlier one for the same line. A record without lineArchive is read as
   it always was.
   An archive grows for as long as the run stays open, so no reader reads all of it to answer about a few orders: each
   part lists its lines (keys) and orders, and carries its copies' engraving decisions (decisions), and a reader reads
   the few parts that hold what it asks for, and only the fields it needs. */
const partOrder = (a, b) => num(a.at) - num(b.at) || num(a.seq) - num(b.seq) || String(a.id).localeCompare(String(b.id));
const lineOfCopy = poolId => { const s = String(poolId), i = s.lastIndexOf("_"); return i > 0 ? s.slice(0, i) : s; };
const decisionsByLine = lines => Object.fromEntries(Object.keys(lines || {}).map(k => [k, Readiness.decisions([lines[k] || {}])]));
function mergeParts(parts, orders) {
  const lines = {};
  for (const p of parts.slice().sort(partOrder)) {
    let part = null; try { part = JSON.parse(p.json || "{}"); } catch (_) { continue; }
    for (const [k, l] of Object.entries(part || {})) if (!orders || orders.has(String(l && l.orderId))) lines[k] = l;
  }
  return lines;
}
/** A run's archive parts with the fields named (and at, seq), oldest first: every part, or — given [field, values] —
    those whose list (keys or orders) names a value, found 30 values to a query (50 queries at a time) and each part
    then read once. */
async function partsOfRun(runId, fields, want = null) {
  const mask = [...new Set(["at", "seq", ...fields])];
  if (!want) return (await col(RUN_LINES).where("runId", "==", runId).select(...mask).get()).docs.map(d => ({ id: d.id, ...d.data() })).sort(partOrder);
  const [field, values] = want, groups = [], refs = new Map(), parts = [];
  for (let i = 0; i < values.length; i += 30) groups.push(values.slice(i, i + 30));
  for (let i = 0; i < groups.length; i += 50) {
    const snaps = await Promise.all(groups.slice(i, i + 50).map(g => col(RUN_LINES).where(field, "array-contains-any", g).select("runId").get()));
    for (const s of snaps) for (const d of s.docs) if (d.data().runId === runId) refs.set(d.id, d.ref);
  }
  const list = [...refs.values()];
  for (let i = 0; i < list.length; i += 100) for (const d of await db.getAll(...list.slice(i, i + 100), { fieldMask: mask })) if (d.exists) parts.push({ id: d.id, ...d.data() });
  return parts.sort(partOrder);
}
/** Archived lines for a reader that shows them: those of the orders named, or the newest maxBytes of them (always the
    newest part; a reply is at most 6 MB), then truncated says some were left out. */
async function archivedLines(runId, { orders = null, maxBytes = 1000000 } = {}) {
  if (orders) return { lines: mergeParts(await partsOfRun(runId, ["json"], ["orders", [...orders]]), orders), truncated: false };
  const all = await partsOfRun(runId, ["bytes"]);
  let total = 0, from = all.length;
  while (from > 0 && (from === all.length || total + num(all[from - 1].bytes) <= maxBytes)) total += num(all[--from].bytes);
  const newest = all.slice(from).map(p => col(RUN_LINES).doc(p.id)), parts = [];
  for (let i = 0; i < newest.length; i += 100) for (const d of await db.getAll(...newest.slice(i, i + 100), { fieldMask: ["json", "at", "seq"] })) if (d.exists) parts.push({ id: d.id, ...d.data() });
  return { lines: mergeParts(parts), truncated: from > 0 };
}
/** The engraving decisions of a run's copies: its record's lines', and — for the copies asked for (poolIds) that those
    do not decide — the archive's, read from the parts holding those copies' lines, or from every part when the run has
    fewer parts than that takes queries. The record's lines are newer than any part and win; a later part wins over an
    earlier one. A part decided under another readiness policy (DECISIONS_VERSION) is decided again from its lines. */
async function decisionsOfRun(runId, run, poolIds = null) {
  const lines = (run && run.lines) || {}, live = Readiness.decisions(Object.values(lines));
  if (!run || !run.lineArchive || !isId(runId)) return live;
  let keys = null;
  if (poolIds) { keys = [...new Set(poolIds.filter(id => !live[id]).map(lineOfCopy))].filter(k => !lines[k]); if (!keys.length) return live; }
  const ask = keys && keys.length <= 30 * num(run.lineArchive.parts) ? ["keys", keys] : null, byLine = {}, out = {};
  const parts = await partsOfRun(runId, ["decisions", "decisionsVersion"], ask), stale = parts.filter(p => p.decisionsVersion !== DECISIONS_VERSION), again = new Map();
  for (let i = 0; i < stale.length; i += 100) for (const d of await db.getAll(...stale.slice(i, i + 100).map(p => col(RUN_LINES).doc(p.id)), { fieldMask: ["json"] })) { try { if (d.exists) again.set(d.id, decisionsByLine(JSON.parse(d.data().json || "{}"))); } catch (_) { /* unreadable: its copies stay undecided */ } }
  for (const p of parts) {
    let d = again.get(p.id) || null;
    if (!d && p.decisionsVersion === DECISIONS_VERSION) { try { d = JSON.parse(p.decisions || "{}"); } catch (_) { d = null; } }
    if (d) Object.assign(byLine, d);
  }
  for (const [k, d] of Object.entries(byLine)) if (!lines[k]) Object.assign(out, d);
  return Object.assign(out, live);
}
async function op_laserStatus(b) {
  const ids=[...new Set((b.sheetIds || []).filter(isId))].slice(0,500), records=[];
  for(let i=0;i<ids.length;i+=100){const docs=await db.getAll(...ids.slice(i,i+100).map(id=>col(SHEETS).doc(id)));for(const d of docs)if(d.exists&&!d.data().archived)records.push({...d.data(),id:d.id});}
  const setIds=[...new Set(records.map(s=>s.setId).concat(b.setIds || []).filter(isId))].slice(0,500),sets=[];
  for(let i=0;i<setIds.length;i+=100){const docs=await db.getAll(...setIds.slice(i,i+100).map(id=>col(SETS).doc(id)));for(const d of docs)sets.push({setId:d.id,sheetIds:d.exists?(d.data().sheetIds || []):[]});}
  return {sheets:(await readinessRecords(records)).map(slim),sets,checkedAt:Date.now()};
}

// Photo preparation is explicit and budgeted. Ordinary thumbnail reads are cache-only,
// including in production. Public image metadata is shared across workspaces.
async function op_listingPhotos(b) {
  const ids=[...new Set((b.listingIds || []).map(String).filter(id=>/^\d{3,20}$/.test(id)))].slice(0,100);
  const cache=require('./_etsyImageCache'),images={},states={},retryAts={};let etsyCalls=0,retryAt=0;
  const results=await cache.readMany(ids,{cacheOnly:b.prepare!==true});
  for(const id of ids){
    const r=results[id];
    const first=r.images?.[0];images[id]=first?.url_570xN||first?.url_fullxfull||first?.url||null;
    states[id]=r.source;retryAts[id]=r.retryAt||0;etsyCalls+=r.etsyCalls||0;if(['paused','budget-paused'].includes(r.source))retryAt=Math.max(retryAt,r.retryAt||0);
  }
  return {images,states,retryAts,etsyCalls,retryAt};
}

async function op_ping(b={}) {
  // Count index entries instead of downloading whole collections on every save.
  const [s,c]=await Promise.all([col(SHEETS).where("archived","==",false).count().get(),db.collection(LIB).count().get()]);
  let calibration;
  if(b.calibration!==false){
    const cal=await db.collection(CAL).orderBy("createdAt","desc").limit(200).get();
    calibration=cal.docs.map(d=>{const r=d.data();return {sheetId:r.sheetId,metal:r.metal,count:num(r.count),cv:num(r.cv),largestFrac:num(r.largestFrac),density:num(r.density),placedAll:!!r.placedAll};});
  }
  return {ok:true,sheets:s.data().count,charms:c.data().count,...(calibration?{calibration}:{})};
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
  // the charm library is shared with production and only read by the sandbox: a rehearsal must not rename or count
  if (PREFIX) return { ok: true, count: 0, skipped: "the sandbox reads the shared charm library and does not write to it" };
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
  // Physical stock and immutable cuts are only changed through transactional stock operations.
  for(const key of ['roseProtectedJson','roseStockId','roseRevision','rosePlanJson','rosePlanHash','roseFingerprint','roseCutAt','roseCutRevision'])delete doc[key];
  if (["gold10k","gold14k"].includes(s.metal) && s.solidIncluded === false) Object.assign(doc, {draft:true,setId:null,setSeq:null,sheetIndex:null,label:null});
  const ref = col(SHEETS).doc(s.id);
  await db.runTransaction(async tx => {
    const ex = await tx.get(ref), old = ex.exists ? ex.data() : {};
    if(old.roseCutAt && (s.placements || s.stock || s.sources))throw new Error('This layout was already cut. Start a new sheet to use its remnant');
    const protection=require('./_charmNestRoseStock'),guard=protection.protectedLayout(old);
    if(Object.prototype.hasOwnProperty.call(s,'placements'))protection.assertProtected(guard,s.placements);
    if(guard&&Object.prototype.hasOwnProperty.call(s,'charms')&&(!Array.isArray(s.charms)||guard.placements.some(p=>!s.charms.some(c=>c?.id===p.id))))throw new Error('The protected Rose Gold charms cannot be removed');
    if(old.rosePlanJson && s.placements && require('./_charmNestRoseStock').fingerprint(s)!==old.roseFingerprint)Object.assign(doc,{rosePlanJson:null,rosePlanHash:null,roseFingerprint:null});
    if (!ex.exists) doc.createdAt = FV.serverTimestamp();
    if(s.metal==='rose' && !old.roseStockId){const reservation=await tx.get(col('Charm_Nest_Rose_Stock').where('owner','==',s.id).limit(1));if(reservation.docs.length){doc.roseStockId=reservation.docs[0].id;doc.roseRevision=reservation.docs[0].data().revision;}}
    const ids = new Set(s.poolIds || old.poolIds || []), backs = new Map();
    for (const bk of [...(s.backPool || []), ...(old.backPool || [])]) if (ids.has(bk.poolId)) {
      const prev = backs.get(bk.poolId);
      if (!prev || (+bk.approvedAt || 0) >= (+prev.approvedAt || 0)) backs.set(bk.poolId, bk);
    }
    for (const [id,bk] of backs) {
      const saved = await tx.get(col(BACK).doc(id));
      if (saved.exists && ((saved.data().invalidated && (+saved.data().approvedAt || 0) >= (+bk.approvedAt || 0)) || (+saved.data().invalidatedAt || 0) >= (+bk.approvedAt || 0))) backs.delete(id);
    }
    doc.backPool = [...backs.values()];
    tx.set(ref, doc, {merge:true});
  });
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
  return { sheets: (await readinessRecords(rows)).map(slim) };
}
async function op_getSheet(b) {
  if (!isId(b.id)) return { error: "bad id" };
  const s = await col(SHEETS).doc(b.id).get();
  if (!s.exists) return { sheet: null };
  const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt);
  await refreshLinks(d);
  return { sheet: (await readinessRecords([d]))[0] };
}
/** Same-origin recovery of a saved PNG; never accepts an arbitrary caller URL. */
async function op_backPreview(b) {
  if (!isId(b.sheetId) || !isId(b.poolId)) return { error: "bad back identity" };
  const snap = await col(SHEETS).doc(b.sheetId).get();
  const sheet = snap.exists ? snap.data() : null;
  const back = (sheet?.backPool || []).find(x => x.poolId === b.poolId);
  if (!back || !(sheet.poolIds || []).includes(b.poolId)) return { error: "This back no longer belongs to the sheet" };
  if (b.approvedAt && +b.approvedAt !== +back.approvedAt) return { error: "This engraving changed; refresh the sheet" };
  const bucket = admin.storage().bucket();
  let path = back.outputs?.png?.path;
  if (!path && back.outputs?.png?.url) {
    try {
      const url = new URL(back.outputs.png.url), match = url.pathname.match(/^\/v0\/b\/([^/]+)\/o\/(.+)$/);
      if (url.hostname === "firebasestorage.googleapis.com" && match && decodeURIComponent(match[1]) === bucket.name) path = decodeURIComponent(match[2]);
    } catch (_) {}
  }
  if (!path) return { error: "The saved back preview is unavailable" };
  const file = bucket.file(path), [meta] = await file.getMetadata();
  if (+meta.size > 2 * 1024 * 1024) return { error: "Back preview is too large" };
  const [bytes] = await file.download();
  if (bytes.length > 2 * 1024 * 1024 || bytes.subarray(0, 8).toString("hex") !== "89504e470d0a1a0a") return { error: "The saved preview is not a PNG" };
  return { dataUrl: "data:image/png;base64," + bytes.toString("base64"), approvedAt: back.approvedAt || null };
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
  const ref = col(SHEETS).doc(b.id);
  const d = await db.runTransaction(async tx => {
    const snap = await tx.get(ref); if (!snap.exists) return null;
    const sheet = snap.data();
    if (!sheet.roseCutAt && (sheet.rosePlanJson || sheet.roseProtectedJson)) throw new Error('A planned or protected Rose Gold contour cannot be deleted');
    tx.delete(ref); return sheet;
  });
  if (d) {
    const bucket = admin.storage().bucket();
    const paths = ["ai", "pdf", "labelled", "report", "preview"].map(k => d.outputs && d.outputs[k] && d.outputs[k].path).filter(Boolean);
    await Promise.all(paths.map(p => bucket.file(p).delete().catch(() => {})));
  }
  return { ok: true, deleted: !!d };
}
/* A calibration row is a statistic the fill estimate learns from — never part of the sheet record. A sheet that placed
   nothing has nothing to teach, so it is skipped, not refused: an error here used to travel all the way up and stop a run
   whose sheets were already written, verified and saved. */
async function op_putCalibration(b) {
  // calibration steers production nesting; a rehearsal's sheets are not evidence for it
  if (PREFIX) return { ok: true, skipped: "sandbox sheets are not calibration evidence" };
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
   (images) is passed straight through to the kick so Firestore never stores it.
   In the sandbox the jobs and their parked payloads are the sandbox's own (Sandbox_Charm_Nest_Agent, marked sandbox:true,
   and charmnest/sandbox/agent/), which its reset clears. And since the sandbox replays copies of the snapshot's orders,
   the same words come to Claude again and again: an engraving reading is looked up by what Claude would be asked, less
   the order number, and one paid for once answers every copy (AGENT_CACHE: written once by charmEngrave-background,
   never overwritten, and kept by the reset). A hit starts no job at all; its id names the cached reading. Production
   asks Claude every time, as before. */
const AGENT = "Charm_Nest_Agent", AGENT_CACHE = "Sandbox_Charm_Nest_Agent_Cache";
/** The cache key of an engraving reading: a hash of the request Claude would get (instructions, schema, model, effort
    and the words), built with the order number left out. */
function agentCacheKey(mode, payload) {
  const Agent = require("./_charmNestAgent"), req = Agent.buildRequest(mode, Object.assign({}, payload, { order: "" }));
  if (req.error) return null;
  return require("crypto").createHash("sha256").update(JSON.stringify([1, mode, Agent.MODEL, req.effort, req.system, req.schema, req.content])).digest("hex").slice(0, 40);
}
/** A cached reading told for this order: the order it was first read for is named as this one (its reasoning may say it). */
const forOrder = (result, from, to) => (result && from && to && from !== to && /^\d{5,20}$/.test(from) ? JSON.parse(JSON.stringify(result).split(from).join(to)) : result);
async function op_startAgent(b) {
  const mode = ["grouping", "layout", "name", "place", "packing", "labelRead", "engraveIntent", "engraveReview"].includes(b.mode) ? b.mode : null;
  if (!mode || !b.payload) return { error: "mode and payload required" };
  const fnName = /^engrave/.test(mode) ? "charmEngrave-background" : "charmNestAgent-background";
  const order = String(b.payload.order || "").replace(/\D/g, "").slice(0, 20);
  const cacheKey = PREFIX && mode === "engraveIntent" ? agentCacheKey(mode, b.payload) : null;
  if (cacheKey) { const hit = await db.collection(AGENT_CACHE).doc(cacheKey).get(); if (hit.exists && hit.data().result) return { ok: true, id: `agentc-${cacheKey}${order ? "-" + order : ""}`, cached: true }; }
  const id = "agent-" + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  // Background functions accept only a small request body (the images made the
  // kick 413), so the payload is parked in Storage and the job reads it back.
  const payloadPath = `charmnest/${PREFIX ? "sandbox/" : ""}agent/${id}.json`;
  await admin.storage().bucket().file(payloadPath).save(Buffer.from(JSON.stringify(b.payload)), { resumable: false, contentType: "application/json", metadata: { cacheControl: "no-store" } });
  await db.collection(PREFIX + AGENT).doc(id).set(Object.assign({ id, mode, status: "pending", payloadPath, sheetId: str(b.sheetId, 80), sourceName: str(b.sourceName, 120), createdAt: FV.serverTimestamp(), updatedAt: FV.serverTimestamp() }, PREFIX ? { sandbox: true, cacheKey, order: order || null } : {}));
  const fetch = require("node-fetch");
  const base = process.env.URL || process.env.DEPLOY_PRIME_URL || "https://goldenspike.app";
  const kick = await fetch(`${base}/.netlify/functions/${fnName}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(PREFIX ? { id, mode, sandbox: true } : { id, mode }) }).catch(err => ({ ok: false, status: 0, statusText: err.message }));
  if (!kick.ok && kick.status !== 202) { await db.collection(PREFIX + AGENT).doc(id).set({ status: "error", error: `kick failed: ${kick.status} ${kick.statusText || ""}`, updatedAt: FV.serverTimestamp() }, { merge: true }); return { error: `could not start the background job (${kick.status})` }; }
  return { ok: true, id };
}
async function op_getAgent(b) {
  if (!isId(b.id)) return { error: "bad id" };
  const cached = PREFIX && /^agentc-([0-9a-f]{40})(?:-(\d{1,20}))?$/.exec(b.id);
  if (cached) {
    const s = await db.collection(AGENT_CACHE).doc(cached[1]).get(); if (!s.exists) return { job: null };
    const d = s.data(); return { job: { id: b.id, mode: d.mode, status: "done", error: null, result: forOrder(d.result, d.order, cached[2]) || null, startedAt: null, createdAt: ms(d.createdAt), cached: true } };
  }
  let s = await db.collection(PREFIX + AGENT).doc(b.id).get();
  if (!s.exists && PREFIX) s = await db.collection(AGENT).doc(b.id).get();   // a sandbox job started before they had their own collection
  if (!s.exists) return { job: null };
  const d = s.data(); return { job: { id: d.id, mode: d.mode, status: d.status, error: d.error || null, result: d.result || null, startedAt: ms(d.startedAt), createdAt: ms(d.createdAt) } };
}


/* ═══ Charm Sorter ⇄ Design Station bridge — master index, pool, backs, sets, runs, maps (design §12, §13) ═══
   Every query below is a single-field equality or range, like the rest of this file: no composite indexes. */
const Master = require("./_charmNestMaster");
const POOL = "Charm_Pool", BACK = "Charm_Pool_Back", SETS = "Charm_Nest_Sets", COUNTERS = "Charm_Nest_Counters", RUNS = "Charm_Nest_Runs", RUN_LINES = "Charm_Nest_Run_Lines", RELEASE = "Charm_Nest_Release", BRIDGE = "Design_Bridge", ALIASES = "Charm_Sku_Aliases", NODESIGN = "Charm_Sku_NoDesign", OPTMAP = "Charm_Option_Map";
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
  for (const name of SANDBOXED) { const s = await db.collection("Sandbox_" + name).count().get(); counts[name] = s.data().count; }
  return { ok: true, snapshot: doc.exists ? doc.data() : null, records: counts };
}
/* The reset works against a clock: a sandbox that streamed for days holds more than one call can delete. A call deletes
   in pages, a parent only once its subcollections are empty, and answers more:true when its time is up; the page calls
   again until it is done, and nothing is lost between calls. The records go first, then the sandbox's files (every one
   under charmnest/sandbox/ but the snapshot the stream plays and the master files the shared index points to), then the
   stream. The cache of engraving readings Claude was paid for (AGENT_CACHE) is not a record of a replay and stays. */
async function op_sandboxReset(b) {
  const until = Date.now() + 7000, late = () => Date.now() > until;
  let deleted = 0, files = 0;
  const names = ["Brites_Orders", "Design_Completed Orders", "Design_RealTime_Selected_Orders", "Design_Order_Archive", ...SANDBOXED, "Charm_Nest_Rose_Rehearsals", SHAPE_CACHE, AGENT];
  const SUBS = { Design_Bridge: ["log"], Brites_Orders: ["messages"], Charm_Nest_Rose_Stock: ["cuts"] };   // deleting a document never deletes its subcollections
  // an order's messages can sit under a Brites_Orders document that was never written (a message posted on its own),
  // which no query of that collection returns: they go with the order's other records, which name it
  const KIN = new Set(["Design_Completed Orders", "Design_RealTime_Selected_Orders", "Design_Order_Archive", "Charm_Nest_Arrivals"]), kinDone = new Set();
  const wipe = async q => { for (;;) { if (late()) return false; const s = await q.select().limit(300).get(); if (s.empty) return true; const batch = db.batch(); s.docs.forEach(d => batch.delete(d.ref)); await batch.commit(); deleted += s.size; if (s.size < 300) return true; } };
  const more = () => ({ ok: true, more: true, deleted, files });
  // which collections still hold anything, asked all at once: a call that follows another goes straight to the work left
  const left = await Promise.all(names.map(name => db.collection("Sandbox_" + name).select().limit(1).get().then(s => !s.empty)));
  for (const [i, name] of names.entries()) {
    if (!left[i]) continue;
    const coll = db.collection("Sandbox_" + name);
    if (!SUBS[name] && !KIN.has(name)) { if (!(await wipe(coll))) return more(); continue; }
    for (;;) {
      if (late()) return more();
      const page = await coll.select().limit(100).get(); if (page.empty) break;
      const gone = []; let cursor = 0;
      await Promise.all(Array.from({ length: Math.min(16, page.size) }, async () => {
        while (cursor < page.docs.length) {
          const d = page.docs[cursor++]; let clear = true;
          for (const sub of SUBS[name] || []) clear = (await wipe(d.ref.collection(sub))) && clear;
          if (KIN.has(name) && !kinDone.has(d.id)) { const ok = await wipe(db.collection("Sandbox_Brites_Orders").doc(d.id).collection("messages")); if (ok) kinDone.add(d.id); clear = ok && clear; }
          if (clear) gone.push(d.ref);
        }
      }));
      if (gone.length) { const batch = db.batch(); gone.forEach(r => batch.delete(r)); await batch.commit(); deleted += gone.length; }
      if (gone.length < page.size) return more();   // the clock ran out inside a page
    }
  }
  let filesError = null;
  try {
    const cur = await db.collection(SANDBOX).doc("current").get(), keep = cur.exists ? cur.data().path : null;
    const bucket = admin.storage().bucket(); let pageToken;
    do {
      if (late()) return more();
      const [list, next] = await bucket.getFiles({ prefix: "charmnest/sandbox/", autoPaginate: false, maxResults: 500, pageToken });
      const doomed = list.filter(f => f.name !== keep && !f.name.startsWith("charmnest/sandbox/master/")); let cursor = 0;
      await Promise.all(Array.from({ length: Math.min(16, doomed.length) }, async () => { while (cursor < doomed.length) { await doomed[cursor++].delete({ ignoreNotFound: true }); files++; } }));
      pageToken = next && next.pageToken;
    } while (pageToken);
  } catch (e) { console.warn("[charmNestLibrary] sandbox files not deleted:", e.message); filesError = e.message; }
  await db.collection(SANDBOX).doc("stream").delete();   // the order stream starts over with the records it fed
  void b; return { ok: true, more: false, deleted, files, filesError };
}
/* ── the sandbox order stream: in place of the whole snapshot at once, the emulated Etsy lists a few new orders per
   simulated ten minutes (etsySandbox.js builds them from this seed and step). The sorter moves the clock one step per
   check, and only once it has taken in the last step's orders, so a replay at 50x plays a day out in half an hour.
   get · ensure (start one for the current snapshot, or resume it) · tick (one step of a playing stream; `expect` is the
   clock the caller last saw, so two tabs never step twice) · off (the whole snapshot again) · reset. Charm_Sandbox only. ── */
const STREAM_STEP_MS = 600000;
async function op_sandboxStream(b) {
  if (!PREFIX) return { error: "the order stream exists only in the sandbox", status: 403 };
  const ref = db.collection(SANDBOX).doc("stream"), action = str(b.action, 12) || "get";
  if (action === "reset") { await ref.delete(); return { ok: true, stream: null }; }
  if (!["get", "ensure", "tick", "off"].includes(action)) return { error: "unknown stream action: " + action };
  return db.runTransaction(async t => {
    const [cur, snap] = await Promise.all([t.get(ref), t.get(db.collection(SANDBOX).doc("current"))]);
    const was = cur.exists ? cur.data() : null, path = snap.exists ? snap.data().path : null;
    if (action === "get") return { ok: true, stream: was };
    if (action === "off") { if (was && was.on) t.set(ref, Object.assign({}, was, { on: false })); return { ok: true, stream: null }; }
    if (!path) return { error: "no sandbox snapshot yet: take one first", status: 409 };
    const now = Date.now(), speed = Math.max(1, Math.min(1000, Math.round(num(b.speed)) || 50));
    let s = was && was.snapshotPath === path ? Object.assign({}, was, { on: true, speed }) : null;
    // only ensure starts or resumes one: a step asked of a stream a reset deleted (a check still out) starts none
    if (action === "tick" && !(s && was.on)) return { ok: true, stream: null, advanced: false };
    // a new stream starts at this ten minutes of the real clock; a seed given in Settings replays a recorded one
    if (!s) { const simStart = Math.floor(now / STREAM_STEP_MS) * STREAM_STEP_MS; s = { on: true, seed: Math.floor(num(b.seed)) > 0 ? Math.floor(num(b.seed)) % 2147483647 || 1 : 1 + Math.floor(Math.random() * 2147483646), speed, stepMs: STREAM_STEP_MS, min: 2, max: 5, simStart, simNow: simStart, tick: 0, snapshotPath: path, startedAt: now, tickAt: now }; }
    let advanced = false;
    if (action === "tick" && (b.expect == null || num(b.expect) === s.simNow)) { s.tick += 1; s.simNow = s.simStart + s.tick * s.stepMs; s.tickAt = now; advanced = true; }
    if (JSON.stringify(s) !== JSON.stringify(was)) t.set(ref, s);
    return { ok: true, stream: s, advanced };
  });
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
      const snap = await db.collection(prefix + RUNS).where("status", "in", ["running", "review", "paused", "processed"]).limit(5).get();
      const live = snap.docs.map(d => d.data()).filter(r => ["running", "review", "paused", "processed"].includes(r.status) && Date.now() - (ms(r.updatedAt) || 0) < 6 * 3600 * 1000);
      if (live.length) return { error: `a run is still open (${live.map(r => r.runId).join(", ")}) — stop or abandon it first`, status: 409 };
    }
  }
  const names = [RUNS, RUN_LINES, SHEETS, SETS, POOL, BACK, COUNTERS, RELEASE, BRIDGE];
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
  for (const x of rows) {
    if (!isId(x.sheetId) || !x.approvedAt || !x.approvedBy) return {error:"approved back and sheet identity required"};
    await db.runTransaction(async tx => {
      const ref = col(BACK).doc(x.poolId), target = col(SHEETS).doc(x.sheetId);
      const prior = await tx.get(ref), sheet = await tx.get(target);
      const old = prior.exists ? prior.data() : {};
      if (!sheet.exists || !(sheet.data().poolIds || []).includes(x.poolId)) throw new Error("The target sheet does not contain this exact charm copy");
      if ((old.invalidated && (+old.approvedAt || 0) >= +x.approvedAt) || (+old.approvedAt || 0) > +x.approvedAt || (+old.invalidatedAt || 0) >= +x.approvedAt) throw new Error("This approval has been superseded; reopen the engraving");
      const currentBack=(sheet.data().backPool || []).find(v=>v.poolId===x.poolId);
      if(b.expectedApprovedAt != null && +b.expectedApprovedAt !== +(currentBack?.approvedAt || old.approvedAt || 0)) throw new Error("This back was edited elsewhere. Reopen it before saving your changes.");
      const former = old.sheetId && old.sheetId !== x.sheetId ? col(SHEETS).doc(old.sheetId) : null;
      const previous = former ? await tx.get(former) : null;
      const record = Object.assign({}, x, {invalidated:false, updatedAt:FV.serverTimestamp()});
      tx.set(ref, record, {merge:true});
      tx.set(target, {backPool:(sheet.data().backPool || []).filter(b=>b.poolId !== x.poolId).concat([x]), updatedAt:FV.serverTimestamp()}, {merge:true});
      if (previous?.exists) tx.set(former, {backPool:(previous.data().backPool || []).filter(b=>b.poolId !== x.poolId), updatedAt:FV.serverTimestamp()}, {merge:true});
    });
  }
  return { ok: true, count: rows.length };
}
async function op_backInvalidate(b) {
  for (const id of (b.poolIds || []).filter(isPoolId).slice(0,400)) await db.runTransaction(async tx => {
    const ref = col(BACK).doc(id), snap = await tx.get(ref), old = snap.exists ? snap.data() : {};
    const sr = old.sheetId ? col(SHEETS).doc(old.sheetId) : null, ss = sr ? await tx.get(sr) : null;
    tx.set(ref, {poolId:id, invalidated:true, invalidatedAt:Date.now(), updatedAt:FV.serverTimestamp()}, {merge:true});
    if (ss?.exists) tx.set(sr, {backPool:(ss.data().backPool || []).filter(x=>x.poolId !== id), updatedAt:FV.serverTimestamp()}, {merge:true});
  });
  return {ok:true};
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
  /* After a commit the run's next dispatch set names the committed set it follows, so it is numbered afresh (once, however
     often it is asked for) instead of being handed the set that was already cut. */
  const after = b.after == null || b.after === "" ? "" : str(b.after, 80);
  if (after) {
    if (!isId(after) || !runId) return { error: "bad set id" };
    const prior = await col(SETS).doc(after).get();
    if (!prior.exists || prior.data().runId !== runId || !prior.data().committedAt) return { error: "the set this one follows is not a committed set of the run" };
  }
  const key = (group ? `${runId}|${group}` : runId) + (after ? `|after:${after}` : "");
  if (runId) { const ex = await col(SETS).where("key", "==", key).limit(1).get(); if (!ex.empty) { const d = ex.docs[0].data(); return { ok: true, setId: d.setId, seq: d.seq, day: d.day, existing: true }; }
    if (!group && !after) { const ex2 = await col(SETS).where("runId", "==", runId).limit(1).get(); if (!ex2.empty) { const d = ex2.docs[0].data(); return { ok: true, setId: d.setId, seq: d.seq, day: d.day, existing: true }; } } }
  const res = await db.runTransaction(async t => {
    const allocation = runId ? col(COUNTERS).doc("allocation-" + require("crypto").createHash("sha256").update(key).digest("hex").slice(0, 40)) : null;
    if (allocation) { const prior = await t.get(allocation); if (prior.exists) return prior.data(); }
    const cref = col(COUNTERS).doc(day); const cs = await t.get(cref);
    const seq = (cs.exists ? num(cs.data().seq) : 0) + 1;
    // A rose-only attempt must not consume an odd number just to make itself eligible.
    if (b.roseOnly && seq % 2 !== 0) return { deferred: true, nextSeq: seq };
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
  const ref=col(SETS).doc(id);
  await db.runTransaction(async tx=>{
    const old=await tx.get(ref),next={...(old.exists?old.data():{}),...(b.patch || {})};
    if(/^complete/.test(next.status || '')) {
      const ids=[...new Set(next.sheetIds || [])];
      if(!ids.length)throw new Error('A set without sheets is not ready for laser');
      const docs=[];for(const sheetId of ids)docs.push(await tx.get(col(SHEETS).doc(sheetId)));
      const records=docs.filter(d=>d.exists).map(d=>({...d.data(),id:d.id}));
      const runIds=[...new Set(records.map(d=>d.runId).filter(Boolean))], decided=new Map();
      for(const runId of runIds){
        const run=await tx.get(col(RUNS).doc(runId)),data=run.exists?run.data():{};
        // the lines of orders the run is done with are in its line archive, read outside the transaction: a part never changes
        decided.set(runId,await decisionsOfRun(runId,data,records.filter(s=>s.runId===runId).flatMap(s=>s.poolIds || [])));
      }
      for(const record of records)record.engraving=decided.get(record.runId) || {};
      if(records.some(s=>s.setId!==id) || !Readiness.set(next,records).ready)throw new Error('Set cannot be completed: every sheet needs approved engraving, verified back files, front files and QR labels');
    }
    tx.set(ref,Object.assign({}, b.patch || {},{setId:id,updatedAt:FV.serverTimestamp()}),{merge:true});
  });
  return { ok: true };
}
async function op_setGet(b) { const id = str(b.setId, 80); if (!isId(id)) return { error: "bad set id" }; const s = await col(SETS).doc(id).get(); if (!s.exists) return { set: null }; const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt); d.committedAt = ms(d.committedAt) || d.committedAt || null; return { set: d }; }
async function op_setList(b) {
  // Completion can occur days after allocation. Filter and sort before limiting;
  // legacy sets keep their original saved day until an actual completion exists.
  const snap = await col(SETS).get();
  let rows = snap.docs.map(d => { const r=d.data(); r.setId ||= d.id; for(const k of ["updatedAt","createdAt","completedAt","committedAt"]) r[k]=ms(r[k]); return r; });
  rows=rows.filter(r=>(!isDay(b.from) || OrderRules.completionDay(r)>=b.from)&&(!isDay(b.to) || OrderRules.completionDay(r)<=b.to)&&(!b.status || r.status===b.status)).sort(OrderRules.compareCompleted).slice(0,Math.min(500,num(b.limit)||200));
  const sheets=[];
  if(b.includeSheets) {
    const ids=[...new Set(rows.flatMap(r=>r.sheetIds || []))].filter(isId);
    for(let i=0;i<ids.length;i+=200) {
      const docs=await db.getAll(...ids.slice(i,i+200).map(id=>col(SHEETS).doc(id)));
      for(const d of docs) if(d.exists && !d.data().archived) sheets.push(d.data());
    }
  }
  return {sets:rows, ...(b.includeSheets ? {sheets:(await readinessRecords(sheets)).map(slim)} : {})};
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
  return db.runTransaction(async tx => {
    const ref = col(SHEETS).doc(b.id), sheet = await tx.get(ref);
    if (!sheet.exists || sheet.data().runId !== b.runId) return { error: "sheet does not belong to this run" };
    const run = await tx.get(col(RUNS).doc(b.runId));
    if (!run.exists || ["complete", "abandoned"].includes(run.data().status)) return { error: "finished sheets cannot be changed by intake" };
    if (!sheet.data().roseCutAt && (sheet.data().rosePlanJson || sheet.data().roseProtectedJson)) throw new Error('A planned or protected Rose Gold contour cannot be archived');
    tx.set(ref, { archived: true, archivedReason: "open sheets repacked", updatedAt: FV.serverTimestamp() }, { merge: true });
    return { ok: true };
  });
}
// ── arrival ledger, separate in production and sandbox ──
async function op_arrivalRecord(b) {
  const receipts = [...new Map((b.orders || []).filter(o => /^\d{1,30}$/.test(String(o.id))).map(o => [String(o.id), o])).values()];
  if (receipts.length > 5000) return { error: "too many receipts" };
  // the sandbox order stream stamps first arrivals with its simulated clock, so its 24 h and 1 h counts read in it
  const now = [true, 1, "1"].includes(b.sandbox) && num(b.now) > 0 ? num(b.now) : Date.now(), collection = col("Charm_Nest_Arrivals"), firstSeen = {};
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
  const [day,hour]=await Promise.all([collection.where("firstSeenAt",">=",now-86400000).count().get(),collection.where("firstSeenAt",">=",now-3600000).count().get()]);
  // the sandbox stream plays days in hours and adds a record per order: those first seen over 45 simulated days ago (far
  // past any order it still lists, and as long as the sorter keeps its own) go, a page per check. Production keeps its ledger.
  if ([true, 1, "1"].includes(b.sandbox)) await collection.where("firstSeenAt", "<", now - 45 * 86400000).limit(200).get().then(s => { if (s.empty) return; const batch = db.batch(); s.docs.forEach(d => batch.delete(d.ref)); return batch.commit(); }).catch(e => console.warn("[charmNestLibrary] old sandbox arrivals not deleted:", e.message));
  return {ok:true,firstSeen,count24:day.data().count,count1:hour.data().count,at:now};
}
// ── runs ──
// The page keeps a run's record small (RunCtl.save); one that still nears Firestore's 1 MiB document limit is said once
// per function instance in the log, in words, before the day it can no longer be saved.
const RUN_DOC_BYTES = 1048576, runSizeWarned = new Set();
async function op_runPut(b) {
  const r = b.run || {}; const id = str(r.runId, 80); if (!isId(id)) return { error: "bad run id" };
  const bytes = Buffer.byteLength(JSON.stringify(r));
  if (bytes > 0.7 * RUN_DOC_BYTES && !runSizeWarned.has(PREFIX + id)) { runSizeWarned.add(PREFIX + id); console.warn(`[charmNestLibrary] run ${id}${PREFIX ? " (sandbox)" : ""}: its record is ${Math.round(bytes / 1024)} KB, ${Math.round(100 * bytes / RUN_DOC_BYTES)}% of the 1,024 KB one Firestore document can hold, with ${Object.keys(r.lines || {}).length} lines in it. A full record cannot be saved, and the run stops.`); }
  const doc = Object.assign({}, r, { runId: id, updatedAt: FV.serverTimestamp() });
  const ref = col(RUNS).doc(id); const ex = await ref.get(); doc.createdAt = ex.exists ? (ex.data().createdAt || FV.serverTimestamp()) : FV.serverTimestamp();
  await ref.set(doc, { merge: !!b.merge });
  return { ok: true, runId: id };
}
/* ── a run's line archive: the lines of the orders a run is done with, sent by the page before it leaves them out of the
   run's record (RunCtl.save). One document per part, at most 900 KB of JSON kept as one text (two index entries, where
   the same lines as fields were some sixty each), named by its content, so a part sent twice is written once. Beside
   it, the part lists its lines (keys) and orders, so a reader finds the parts that hold the few it wants, and keeps its
   copies' engraving decisions by line (decisions), which readiness reads instead of the lines. Nothing here is ever
   changed: a line archived again is a new part, and the newer part wins (mergeParts, decisionsOfRun). ── */
const LINE_PART_BYTES = 900000;
async function op_runArchive(b) {
  const id = str(b.runId, 80); if (!isId(id)) return { error: "bad run id" };
  const parts = Array.isArray(b.parts) ? b.parts : [];
  if (!parts.length || parts.length > 8) return { error: "send one to eight parts" };
  const at = Date.now(), docs = [];
  for (const [i, p] of parts.entries()) {
    const json = p && typeof p.json === "string" ? p.json : "", bytes = Buffer.byteLength(json);
    if (!bytes || bytes > LINE_PART_BYTES) return { error: "an archive part is JSON text of at most 900 KB" };
    let lines = null; try { lines = JSON.parse(json); } catch (_) { return { error: "an archive part is not JSON" }; }
    if (!lines || typeof lines !== "object" || Array.isArray(lines) || !Object.keys(lines).length) return { error: "an archive part holds no lines" };
    const keys = Object.keys(lines), orders = [...new Set(Object.values(lines).map(l => String((l && l.orderId) ?? "")).filter(Boolean))];
    const decisions = JSON.stringify(decisionsByLine(lines));
    if (bytes + Buffer.byteLength(decisions) + Buffer.byteLength(JSON.stringify(keys.concat(orders))) > 1000000) return { error: "an archive part is too large to keep with its lists; send fewer lines in it" };
    const digest = require("crypto").createHash("sha256").update(json).digest("hex").slice(0, 40);
    docs.push([col(RUN_LINES).doc(`${id}~${digest}`), { runId: id, lines: keys.length, bytes, json, keys, orders, decisions, decisionsVersion: DECISIONS_VERSION, at, seq: i, createdAt: FV.serverTimestamp() }]);
  }
  const batch = db.batch(); for (const [ref, doc] of docs) batch.set(ref, doc); await batch.commit();
  return { ok: true, parts: docs.length, lines: docs.reduce((n, [, d]) => n + d.lines, 0) };
}
async function op_runGet(b) {
  const id = str(b.runId, 80); if (!isId(id)) return { error: "bad run id" };
  const s = await col(RUNS).doc(id).get(); if (!s.exists) return { run: null };
  const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt);
  // Asked for, the lines of the orders the run is done with come back from its line archive under the record's own: those
  // of the orders named, or the newest megabyte of them, about as many as a record held before it filled. A resume never
  // asks: it takes the orders still in progress, which are the record's.
  if (b.archived && d.lineArchive) {
    const orders = Array.isArray(b.orders) ? new Set(b.orders.slice(0, 2000).map(String)) : null;
    const old = await archivedLines(id, { orders });
    d.lines = Object.assign(old.lines, d.lines || {}); if (old.truncated) d.archiveTruncated = true;
  }
  return { run: d };
}
async function op_runList(b) {
  const snap = await col(RUNS).orderBy("updatedAt", "desc").limit(Math.min(200, num(b.limit) || 50)).get();
  // counts include what the record left out for its line archive
  let rows = snap.docs.map(d => { const r = d.data(), out = r.lineArchive || {}; return { runId: r.runId, setId: r.setId || null, day: r.day, step: r.step, status: r.status, mode: r.mode || null, lines: (r.lines ? Object.keys(r.lines).length : 0) + num(out.lines), holds: (r.holds ? Object.keys(r.holds).length : 0) + num(out.held), errors: (r.errors || []).length, updatedAt: ms(r.updatedAt), createdAt: ms(r.createdAt), stoppedBy: r.stoppedBy || null }; });
  if (b.status) rows = rows.filter(r => r.status === b.status);
  return { runs: rows };
}
/* "Find that order from last Tuesday." There is no index that can answer it: Firestore has no full-text search, and a
   shop's history of runs is small enough to read. So run summaries and sheet metadata are searched here, before result pagination, and
   the answer says how many of each it actually looked at — a search that quietly stopped early is worse than none. */
async function op_history(b) {
  const q = String(b.q || "").trim().toLowerCase(), offset = Math.max(0, num(b.offset)), limit = Math.min(100, Math.max(1, num(b.limit) || 60));
  // Search complete set membership before pagination: a matching order must reveal all of its set's sheets.
  const [rs, ss, ts, ls] = await Promise.all([col(RUNS).select("runId", "setId", "seq", "day", "status", "step", "lines", "sheets", "lineArchive", "errors", "stoppedBy", "createdAt", "updatedAt").get(), col(SHEETS).select("id", "setId", "setSeq", "runId", "day", "metal", "metalLabel", "status", "orders", "sheetIndex", "page", "updatedAt", "archived", "folder", "fileBase", "saving", "draft", "releaseFull", "endedBy", "charmCount", "placedCount", "rejectCount", "density", "freePt2", "verification", "outputs", "names", "charms", "poolIds", "backPool", "solidIncluded", "sources", "stock", "cardStartedAt", "createdAt").get(), col(SETS).select("seq", "day", "runId", "status", "updatedAt", "materials", "orders").get(), col(RUN_LINES).select("runId", "json", "at", "seq").get()]);
  const runMap = new Map(rs.docs.map(d => [d.id, d.data()])), sheets = ss.docs.map(d => d.data()).filter(x => !x.archived);
  // the lines of the orders a run is done with are in its line archive: searched and counted under the record's own
  const archive = new Map(); for (const d of ls.docs) { const p = { id: d.id, ...d.data() }; if (!archive.has(p.runId)) archive.set(p.runId, []); archive.get(p.runId).push(p); }
  for (const [id, parts] of archive) { const r = runMap.get(id); if (r) runMap.set(id, { ...r, lines: Object.assign(mergeParts(parts), r.lines || {}) }); }
  const groups = new Map(ts.docs.map(d => { const x = d.data(); return ["set:"+d.id, { key:"set:"+d.id, name:"Set "+x.seq, setId: d.id, seq: x.seq, day: x.day, runId: x.runId, status: x.status, updatedAt: ms(x.updatedAt), sheets: [], materials: x.materials || [], orderIds: Object.keys(x.orders || {}), search: [] }]; }));
  for (const x of sheets) {
    const meta=OrderRules.libraryGroup(x), key=meta.key;
    if (!groups.has(key)) groups.set(key, { ...meta, day: x.day, runId: x.runId, status: meta.standalone ? "standalone" : meta.working ? "held" : x.status, draft: meta.working, sheets: [], materials: [], orderIds: [], search: [] });
    const g = groups.get(key); g.sheets.push(Object.assign(slim(x), { orders: (x.orders || []).length, orderIds: x.orders || [], sheetIndex: x.sheetIndex || x.page || 1 }));
    if (!g.materials.includes(x.metal)) g.materials.push(x.metal);
    g.search.push(x.names || "", x.metalLabel || "", ...(x.charms || []).map(c => c.sku || ""));
    g.orderIds.push(...(x.orders || [])); g.updatedAt = Math.max(g.updatedAt || 0, ms(x.updatedAt) || 0);
  }
  const runRows = [...runMap.values()].map(r => ({ runId: r.runId, setId: r.setId || null, seq: r.seq || null, day: r.day, status: r.status, step: r.step, lines: Object.keys(r.lines || {}).length, orders: new Set(Object.values(r.lines || {}).map(l => l.orderId)).size, sheets: Object.keys(r.sheets || {}).length + num((r.lineArchive || {}).sheets), updatedAt: ms(r.updatedAt), createdAt: ms(r.createdAt), stoppedBy: r.stoppedBy || null, hitOrders: [...new Set(Object.values(r.lines || {}).map(l => String(l.orderId || "")))].filter(x => q && x.toLowerCase().includes(q)), hitSkus: [...new Set(Object.values(r.lines || {}).map(l => String(l.sku || "")))].filter(x => q && x.toLowerCase().includes(q)) }));
  for (const r of runRows) if (![...groups.values()].some(g => g.runId === r.runId)) groups.set("run:" + r.runId, { setId: r.setId, seq: r.seq, runId: r.runId, day: r.day, status: r.status, sheets: [], materials: [], orderIds: [], orders: r.orders, updatedAt: r.updatedAt });
  let rows = [...groups.values()].map(g => {
    const r = runMap.get(g.runId), ids = new Set(g.orderIds.map(String));
    const lines = Object.values(r?.lines || {}).filter(l => !ids.size || ids.has(String(l.orderId)));
    const hay = [g.setId, "Set " + g.seq, g.day, g.runId, g.status, r?.stoppedBy, ...(r?.errors || []).map(e => e.why), ...g.materials, ...(g.search || []), ...ids, ...lines.flatMap(l => [l.orderId, l.sku, l.engrave?.text, l.snap?.title]), ...g.sheets.map(x => x.fileBase)].join(" ").toLowerCase();
    return Object.assign(g, { orders: ids.size || g.orders || new Set(lines.map(l => l.orderId)).size, status: g.standalone ? "standalone" : g.draft ? "held" : g.status === "superseded" ? g.status : r?.status === "complete" ? (String(g.status).startsWith("complete") ? g.status : "complete") : r?.status || g.status, match: !q || hay.includes(q) });
  }).filter(g => g.match).sort((a, b) => String(b.day || "").localeCompare(String(a.day || "")) || (b.seq || 0) - (a.seq || 0) || (b.updatedAt || 0) - (a.updatedAt || 0));
  const total = rows.length, setCount = rows.filter(g => g.setId && g.sheets.length && g.status !== "superseded").length, workingCount = rows.filter(g => g.draft).length; rows = rows.slice(offset, offset + limit);
  for (const row of rows) { delete row.search; delete row.match; }
  return { sets: rows, runs: runRows.filter(r => rows.some(g => g.runId === r.runId)), sheets: rows.flatMap(g => g.sheets), total, setCount, workingCount, nextOffset: offset + rows.length < total ? offset + rows.length : null,
    scanned: { runs: rs.size, sheets: ss.size, lineParts: ls.size }, truncated: { runs: false, sheets: false } };
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

const RoseStock = require("./_charmNestRoseStock")({db,col,FV,Readiness,decisionsOfRun});
const OPS = { ...RoseStock, listingPhotos:op_listingPhotos, getShapeGuidance:op_getShapeGuidance, putShapeGuidance:op_putShapeGuidance, laserStatus:op_laserStatus, archiveEmptySheet: op_archiveEmptySheet, arrivalRecord: op_arrivalRecord, startAgent: op_startAgent, getAgent: op_getAgent, ping: op_ping, lookupCharms: op_lookupCharms, putCharms: op_putCharms, renameCharm: op_renameCharm, listCharms: op_listCharms, putSheet: op_putSheet, listSheets: op_listSheets, getSheet: op_getSheet, backPreview: op_backPreview, deleteSheet: op_deleteSheet, purgeHistory: op_purgeHistory, restoreSheet: op_restoreSheet, putCalibration: op_putCalibration, getCalibration: op_getCalibration, startJob: op_startJob, getJob: op_getJob, stopJob: op_stopJob,
  masterPutIndex: op_masterPutIndex, masterGet: op_masterGet, masterGetMany: op_masterGetMany, masterList: op_masterList, masterPatch: op_masterPatch, masterPutFile: op_masterPutFile, masterListFiles: op_masterListFiles, masterRemoveFile: op_masterRemoveFile, masterRemoveSku: op_masterRemoveSku, startMaster: op_startMaster,
  jobList: op_jobList, poolPut: op_poolPut, poolUpdate: op_poolUpdate, poolList: op_poolList, poolGet: op_poolGet, backPut: op_backPut, backInvalidate: op_backInvalidate, backList: op_backList, sandboxPut: op_sandboxPut, sandboxStatus: op_sandboxStatus, sandboxReset: op_sandboxReset, sandboxStream: op_sandboxStream,
  setAllocate: op_setAllocate, setUpdate: op_setUpdate, setGet: op_setGet, setList: op_setList, runPut: op_runPut, runArchive: op_runArchive, runGet: op_runGet, runList: op_runList, history: op_history, releaseGet: op_releaseGet, releasePut: op_releasePut, bridgeLog: op_bridgeLog,
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
