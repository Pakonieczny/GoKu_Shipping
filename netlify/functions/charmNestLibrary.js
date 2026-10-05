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
 *    putSheet · listSheets · getSheet · deleteSheet · sheetPdf
 *    laserDone · laserDoneList · findSheets   (the Library's Current | Completed tabs and its order / listing search)
 *    putCalibration · getCalibration
 *    startJob · getJob · stopJob
 *    + bridge (design doc §13): masterPutIndex · masterGet · masterGetMany · masterList · masterPatch · masterPutFile ·
 *      masterListFiles · masterRemoveFile · startMaster · poolPut · poolUpdate · poolList · poolGet · backPut · backList ·
 *      setAllocate · setUpdate · setGet · setList · runPut · runArchive · runGet · runList · bridgeLog · aliasGet · aliasPut ·
 *      noDesignGet · noDesignPut · noDesignDelete · optionMapGet · optionMapPut · customSheetGet · customSheetPut · customGet · customPut · customReopen
 *      (customDelete: an older page's name for customReopen; nothing deletes a record)
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const admin = require("./firebaseAdmin");
const { json, gate, parseBody, str, num } = require("./_charmNestAuth");
const db = admin.firestore();
const OrderRules = require("../../charm-nest-orders.js");
const Readiness = require("../../charm-nest-readiness.js");
const Activity = require("../../charm-nest-activity.js");
const EngravingSeals = require("../../charm-nest-engraving-seals.js");
// A run's archived lines keep the readiness policy's decisions as they were when written (op_runArchive); a part written
// under another version of Readiness.decisions is decided again from its lines when read (decisionsOfRun).
const DECISIONS_VERSION = require("crypto").createHash("sha256").update(String(Readiness.decisions)).digest("hex").slice(0, 16);
/* ── sandbox: when a request says sandbox:true, the sorter's OWN records (sheets, pools, backs, sets, counters, runs,
   bridge log) go to Sandbox_-prefixed collections; the master index, the charm library and calibration stay shared and
   are only read. The learned maps (Charm_Sku_Aliases, Charm_Option_Map, Charm_Sku_NoDesign) are READ shared but WRITTEN
   to the sandbox's own copy (Sandbox_Charm_Sku_Aliases …): a person's answer in the sandbox ("Use this charm", a mapped
   option, a no-design SKU) is a relationship between a listing/SKU and a design, and it must reach neither production nor
   the next replay of the same real order (Paul, 3 Oct: the sandbox kept the relationships he had given). Reads merge the
   two, the sandbox's own answer first; Reset deletes the copy. Set per request; a function instance handles one request
   at a time. ── */
let PREFIX = "";
const SANDBOXED = new Set(["Charm_Nest_Rose_Stock", "Charm_Nest_Sheets", "Charm_Pool", "Charm_Pool_Back", "Charm_Nest_Sets", "Charm_Nest_Counters", "Charm_Nest_Runs", "Charm_Nest_Run_Lines", "Charm_Nest_Run_Live", "Charm_Nest_Release", "Charm_Nest_Arrivals", "Charm_Nest_Cancelled", "Charm_Nest_Cancelled_History", "Design_Bridge"]);
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
/* A function's answer is sent whole and may be at most 6 MB, and a sync function has seconds to give it. A list that grows
   with the shop (a year of sheets, a set list with its sheets, the SKU index, the learned maps) used to be read and sent
   whole, or cut at a count with the rest dropped without a word. Such a list now goes in parts: a part holds what fits in
   ANSWER_BYTES and was read within ANSWER_MS of the call (always at least one item, so every part moves the list on);
   `next` says where the rest starts, the page sends it back as `cursor` until there is none, and `truncated` says the
   answer was cut short. */
const ANSWER_BYTES = 4000000, ANSWER_MS = 5000;
const bytesOf = v => Buffer.byteLength(JSON.stringify(v));
/** What one part of an answer may still take: fits(item) counts the item in, or says it is for the next part; late()
    says the part has had its time (never before it holds an item). */
function answerBudget(began = Date.now()) {
  let used = 0, items = 0;
  return {
    fits(item) { const n = bytesOf(item); if (items && used + n > ANSWER_BYTES) return false; used += n; items++; return true; },
    late: () => items > 0 && Date.now() - began > ANSWER_MS
  };
}
const docOrder = () => admin.firestore.FieldPath.documentId();

function slim(d) {
  return {
    id: d.id, roseStockId:d.roseStockId || null, roseCutAt:d.roseCutAt || null, rosePlanHash:d.rosePlanHash || null, solidIncluded:d.solidIncluded == null ? null : !!d.solidIncluded, draft: !!d.draft, releaseFull: !!d.releaseFull, folder: d.folder || null, fileBase: d.fileBase || d.folder || null, saving: !!d.saving, metal: d.metal, metalLabel: d.metalLabel, day: d.day, status: d.status, endedBy: d.endedBy,
    charmCount: num(d.charmCount), placedCount: num(d.placedCount), rejectCount: num(d.rejectCount), density: num(d.density), freePt2: num(d.freePt2),
    verification: d.verification ? { ok: !!d.verification.ok } : null,
    preview: d.outputs && d.outputs.preview ? d.outputs.preview.url : null,
    // the four files a recalled card offers, so recalling a set is one read of this list and nothing more
    outputs: d.outputs ? Object.fromEntries(["ai", "pdf", "labelled", "report"].filter(k => d.outputs[k] && d.outputs[k].url).map(k => [k, d.outputs[k].url])) : {},
    stock: d.stock || null, poolIds: d.poolIds || [], engraving:d.engraving || {}, orderReadiness:d.orderReadiness || {}, laser:d.laser || null,
    backs: (d.backPool || []).map(bk => ({ poolId: bk.poolId || null, previewWPt:bk.previewWPt || null, previewHPt:bk.previewHPt || null, pageWPt:bk.pageWPt || null, pageHPt:bk.pageHPt || null, sheetId:bk.sheetId || d.id, invalidated:!!bk.invalidated, copy:bk.copy || null, approvedAt:bk.approvedAt || null, engravingSeals:EngravingSeals.merge(bk), order: bk.order || null, sku: bk.sku || null, text: bk.text || null, lines: bk.lines || null, approvedBy: bk.approvedBy || null, verified:bk.verified || null, outputs:bk.outputs || null, capMm: bk.capMm || null, png: bk.outputs && bk.outputs.png ? bk.outputs.png.url : null, ai: bk.outputs && bk.outputs.ai ? bk.outputs.ai.url : null })),
    names: str(d.names, 2000), sources: (d.sources || []).map(s => ({ name: s.name, hash: s.hash || null })), runId: d.runId || null, page: num(d.page) || 1,
    setId: d.setId || null, setSeq: num(d.setSeq) || null, sheetIndex: num(d.sheetIndex) || null, orders: (d.orders || []).slice(0, 500), backCount: (d.backPool || []).length, label: d.label ? { files: (d.label.files || []).map(f => ({ path: f.path, url: f.url, payload:f.payload || null, orders:f.orders || [], part:f.part || 1 })) } : null,
    // cut on the laser and marked so (op_laserDone), and the listings its pieces were bought from (the Library's search)
    laserDoneAt: num(d.laserDoneAt) || null, laserDoneBy: d.laserDoneBy || null, processSeals: Readiness.processStamps(d), processReady: !!d.processReady, laserSetPending: !!d.laserSetPending, laserHold: d.laserHold && num(d.laserHold.at) > 0 ? { at: num(d.laserHold.at), by: str(d.laserHold.by, 80), note: str(d.laserHold.note, 200) } : null, listings: (d.listings || []).slice(0, 500),
    cardStartedAt: ms(d.cardStartedAt) || ms(d.createdAt), updatedAt: ms(d.updatedAt), createdAt: ms(d.createdAt),
    // a cleanup made on the record (the pieces it took off, the green line it removed): a page whose own copy of the sheet
    // is older puts the same change on it (Cleanups, charm-nest-bridge.js). Only on a record that has one
    ...(d.cleanup && d.cleanup.id ? { cleanup: { id: str(d.cleanup.id, 80), mode: d.cleanup.mode || null, at: num(d.cleanup.at) || null, removedPoolIds: (d.cleanup.removedPoolIds || []).slice(0, 50), removedCharmIds: (d.cleanup.removedCharmIds || []).slice(0, 50), lineRemoved: num(d.cleanup.lineRemoved) || null, lineKept: num(d.cleanup.lineKept) || null } } : {})
  };
}
/* The fields of a sheet record that its list entry (slim) and its laser readiness (Readiness.sheet) read, and the lists
   filter on. A list reads only these: the rest of a record (its charms with their outlines, its placements) is most of
   its up to 900 KB, and a list of 500 sheets used to read all of it to send none of it. */
const SLIM_SHEET = ["id", "sheetId", "roseStockId", "roseCutAt", "rosePlanHash", "solidIncluded", "draft", "releaseFull", "folder", "fileBase", "saving", "dirty", "metal", "metalLabel", "day", "status", "endedBy", "charmCount", "placedCount", "rejectCount", "density", "freePt2", "verification", "preview", "outputs", "stock", "poolIds", "backPool", "backs", "names", "sources", "runId", "page", "setId", "setSeq", "sheetIndex", "orders", "label", "archived", "laserDoneAt", "laserDoneBy", "listings", "cardStartedAt", "cleanup", "createdAt", "updatedAt", "processSeals", "processReady", "archivedLaserDoneAt", "archivedLaserDoneBy", "activityAt", "laserHold"];
/** Readiness counts a record's placements where it has no placedCount (Readiness.sheet): read for those alone. */
async function withPlacements(rows) {
  const want = rows.filter(([, d]) => !(+d.placedCount)), byId = new Map(want);
  for (let i = 0; i < want.length; i += 100) for (const s of await db.getAll(...want.slice(i, i + 100).map(([id]) => col(SHEETS).doc(id)), { fieldMask: ["placements"] })) { const p = s.exists && s.data().placements; if (p && byId.has(s.id)) byId.get(s.id).placements = p; }
}
/** The list entries (slim, laser readiness and all) of sheets, in the order given, a hundred at a time while the answer
    has room and time: a row is [id, its record read with SLIM_SHEET], or [id] for one read here. A sheet deleted or
    archived since it was listed is left out. Returns { sheets, rest }, rest being the ids left for the next part. */
async function sheetEntries(rows, budget) {
  const sheets = [];
  for (let i = 0; i < rows.length; i += 100) {
    if (budget.late()) return { sheets, rest: rows.slice(i).map(r => r[0]) };
    let chunk = rows.slice(i, i + 100);
    const unread = chunk.filter(r => r.length === 1);
    if (unread.length) {
      const read = new Map((await db.getAll(...unread.map(r => col(SHEETS).doc(r[0])), { fieldMask: SLIM_SHEET })).filter(s => s.exists).map(s => [s.id, s.data()]));
      chunk = chunk.map(r => (r.length === 1 ? [r[0], read.get(r[0])] : r)).filter(r => r[1] && !r[1].archived);
    }
    await withPlacements(chunk);
    const all = (await readinessRecords(chunk.map(r => r[1]))).map(slim);
    let n = 0; while (n < all.length && budget.fits(all[n])) n++;
    sheets.push(...all.slice(0, n));
    if (n < all.length) return { sheets, rest: chunk.slice(n).map(r => r[0]).concat(rows.slice(i + 100).map(r => r[0])) };
  }
  return { sheets, rest: [] };
}

// One run read per batch supplies the exact per-copy engraving decisions.
// Old/incomplete records remain pending until evidence is available.
// Set membership controls filing, independently of an individual sheet's completion badge.
async function filingRecords(records) {
  const ids=[...new Set(records.filter(s=>s.setId && !s.draft && s.solidIncluded!==false).map(s=>s.setId))].filter(isId), sets=new Map();
  for(let i=0;i<ids.length;i+=100) for(const d of await db.getAll(...ids.slice(i,i+100).map(id=>col(SETS).doc(id)),{fieldMask:["laserDoneAt"]})) sets.set(d.id,d.exists?d.data():null);
  return records.map(s=>({...s,laserSetPending:!!(s.setId && !s.draft && s.solidIncluded!==false && !(num(sets.get(s.setId)?.laserDoneAt)>0))}));
}
async function readinessRecords(records,{revs=null}={}) {
  records=await filingRecords(records);
  await productionReadiness(records,{revs});
  return records.map(s=>({...s,laser:Readiness.laserSheet(s)}));
}
/* An order's pieces are not always all in the run of the sheet that carries one of them: a later pull can take a line the
   first run did not, and a piece pooled in an older run is placed by a newer one. The run record lists its orders, and an
   archive part lists its own, so the runs that hold an order are found by asking those lists (array-contains-any, 15
   orders to a query because each is asked in its two spellings, text and number; only the lists come back). Every order
   asked about is answered, the runs of the orders it was asked of last (a minute) kept in this instance, because the
   Library reads the same sheets every few seconds and a run starts holding an order rarely. A reader that needs the
   answer exact (a transaction, which writes a seal on it) never uses what was kept. */
const HOLDERS_MAX = 5000, holders = new Map(), holdersMs = () => (process.env.CHARM_NEST_HOLDERS_MS === undefined ? 60000 : Math.max(0, +process.env.CHARM_NEST_HOLDERS_MS || 0));   // (a test sets it to 0)
async function runsOfOrders(get, orders, fresh) {
  const out = new Map(), now = Date.now(), ttl = holdersMs(), ask = [];
  for (const o of orders) {
    const kept = fresh ? null : holders.get(PREFIX + o);
    if (kept && now - kept.at < ttl) out.set(o, new Set(kept.runs)); else { out.set(o, new Set()); ask.push(o); }
  }
  const forms = v => [String(v)].concat(Number.isSafeInteger(+v) ? [+v] : []);
  for (let i = 0; i < ask.length; i += 15) {
    const batch = ask.slice(i, i + 15), values = batch.flatMap(forms), asked = new Set(batch);
    const [a, b] = await Promise.all([get(col(RUNS).where("orders", "array-contains-any", values).select("orders")), get(col(RUN_LINES).where("orders", "array-contains-any", values).select("runId", "orders"))]);
    for (const [docs, runOf] of [[a.docs, d => d.id], [b.docs, d => d.data().runId]]) for (const d of docs) {
      const id = runOf(d);
      if (isId(id)) for (const o of new Set((d.data().orders || []).map(String))) if (asked.has(o)) out.get(o).add(id);
    }
    if (!fresh && ttl) for (const o of batch) holders.set(PREFIX + o, { at: now, runs: [...out.get(o)] });
  }
  if (holders.size > HOLDERS_MAX) { for (const [k, v] of holders) if (now - v.at >= ttl) holders.delete(k); if (holders.size > HOLDERS_MAX) holders.clear(); }
  return out;
}
// Verify whole orders from their current saved lines, including archived lines and copies on another sheet.
// The caller's transaction reads the live run and every dependent sheet before it writes a seal or completion.
async function productionReadiness(records,{tx=null,revs=null}={}) {
  const get=ref=>tx?tx.get(ref):ref.get(),runs=new Map(),lines=new Map(),wanted=new Set(records.flatMap(Readiness.orderIds)),unverified=new Set();
  for(const id of [...new Set(records.map(s=>s.runId).filter(isId))]){
    const snap=await get(col(RUNS).doc(id)),run=snap.exists?await withLiveLines(id,snap.data()):null;
    if(revs)revs['r:'+id]=revOf(snap);
    runs.set(id,run || {});
    const archived=run?.lineArchive?await archivedLines(id,{orders:wanted}):{lines:{}};
    for(const [key,l] of Object.entries({...archived.lines,...run?.lines}))if(wanted.has(String(l.orderId || key.split('_')[0])))lines.set(key,{...l,key,orderId:String(l.orderId || key.split('_')[0])});
  }
  // The rest of an order can be in another run. Only a sheet that is still to be cut asks (a sheet cut once before is ready whatever its orders say), and only
  // the lines no run read already holds are added: the sheet's own run keeps its word for a line both hold. When the runs of the asked orders cannot be read,
  // those orders stay unverified rather than reading as whole.
  const seen=new Map([...runs.keys()].map(id=>[id,new Set(wanted)])),looked=new Set();
  const crossRuns=async asking=>{
    asking=asking.filter(o=>!looked.has(o));for(const o of asking)looked.add(o);
    if(!asking.length)return;
    const found=await runsOfOrders(get,asking,!!tx),extra=new Map();
    for(const [order,ids] of found)for(const id of ids){if(!extra.has(id))extra.set(id,new Set());extra.get(id).add(order);}
    for(const [id,asked] of extra){
      const had=seen.get(id) || new Set(),orders=new Set([...asked].filter(o=>!had.has(o)));
      if(!orders.size)continue;
      let run=runs.get(id);
      if(!run){const snap=await get(col(RUNS).doc(id));run=snap.exists?await withLiveLines(id,snap.data()):null;if(revs)revs['r:'+id]=revOf(snap);runs.set(id,run || {});}
      const archived=run?.lineArchive?await archivedLines(id,{orders}):{lines:{}};
      for(const [key,l] of Object.entries({...archived.lines,...run?.lines})){const order=String(l.orderId || key.split('_')[0]);if(orders.has(order) && !lines.has(key))lines.set(key,{...l,key,orderId:order});}
      seen.set(id,new Set([...had,...orders]));
    }
  };
  const asking=[...new Set(records.filter(s=>!Readiness.completedBefore(s)).flatMap(Readiness.orderIds))];
  try{await crossRuns(asking);}catch(e){console.warn('[charmNestLibrary] the runs of these orders could not be read:',e && e.message);for(const o of asking)unverified.add(o);}
  const evidence=new Map(records.map(s=>[s.id || s.sheetId,s]));
  // an order whose pieces are not all on these sheets: the sheets that hold the rest are read too. A line that lost its pool ids is read by the ids the pool gives
  // its copies ("<line key>_<n>"), a committed or written one too: it may be a no-design candidate (below), but until that is known its copies may be on a sheet that is not asked
  const present=new Set(records.flatMap(Readiness.idsOf)),lostIds=l=>!(l.poolIds || []).length && l.state!=='gone' && !l.noDesign && !l.problems?.length,
    missingOrders=[...new Set([...lines.values()].filter(l=>(l.poolIds || []).some(id=>!present.has(id)) || (lostIds(l) && Readiness.copyIds(l,l.key).some(id=>!present.has(id)))).map(l=>l.orderId))];
  for(let i=0;i<missingOrders.length;i+=30){
    const snap=await get(col(SHEETS).where('orders','array-contains-any',missingOrders.slice(i,i+30)).select(...SLIM_SHEET));
    for(const d of snap.docs){if(revs)revs['s:'+d.id]=revOf(d);if(!d.data().archived && !evidence.has(d.id))evidence.set(d.id,{...d.data(),id:d.id});}
  }
  // A dependent sheet can also carry other orders. Read their engraving evidence too;
  // otherwise its plain pieces would incorrectly become "unknown" just because this view shows another set.
  const dependencies=[...evidence.values()].filter(s=>!records.includes(s));
  for(const id of [...new Set(dependencies.map(s=>s.runId).filter(isId))]){
    if(!runs.has(id)){const snap=await get(col(RUNS).doc(id));if(revs)revs['r:'+id]=revOf(snap);runs.set(id,snap.exists?await withLiveLines(id,snap.data()):{});}
    const run=runs.get(id),orders=new Set(dependencies.filter(s=>s.runId===id).flatMap(Readiness.orderIds));
    const archived=run?.lineArchive?await archivedLines(id,{orders}):{lines:{}};
    for(const [key,l] of Object.entries({...archived.lines,...run?.lines}))if(orders.has(String(l.orderId || key.split('_')[0])))lines.set(key,{...l,key,orderId:String(l.orderId || key.split('_')[0])});
    seen.set(id,new Set([...(seen.get(id) || []),...orders]));
  }
  // a sheet that carries one of these orders can carry pieces of other orders whose lines are in another run: its engraving evidence is read there too, or its plain
  // pieces would read as undecided and the sheet as not ready (a failed read leaves them undecided, which holds the order back)
  try{await crossRuns([...new Set(dependencies.filter(s=>!Readiness.completedBefore(s)).flatMap(Readiness.orderIds))]);}catch(e){console.warn('[charmNestLibrary] the runs of the other sheets\' orders could not be read:',e && e.message);}
  // No-design exemptions are explicit in new records; an older committed chain may only name its approved SKU.
  const candidates=[...lines.values()].filter(l=>!l.poolIds?.length && !l.noDesign && ['committed','written','labelled'].includes(l.state));
  if(candidates.length){
    const rules={skus:[],patterns:[]};
    for(const x of await noDesignRows(get)){if(x.sku)rules.skus.push(x.sku);if(x.pattern)rules.patterns.push(x.pattern);}
    for(const l of candidates){
      const line={sku:l.sku,title:l.snap?.title || '',variations:(l.snap?.vars || []).map(v=>{const [name,value]=v.split('␟');return {name,value};})};
      if(OrderRules.isNoDesign(l.sku || line.title,rules) || OrderRules.specialOf(line)?.notCut){l.noDesign=true;continue;}
      // A special item completed by hand has its own permanent seal, and is not a missing charm.
      const custom=lineKeyOk(l.key)?await get(col(CUSTOM).doc(l.key)):null;
      if(custom?.exists && custom.data().state!=='open'){l.noDesign=true;continue;}
      if(lineKeyOk(l.key)){
        const saved=await get(db.collection(require('./_charmNestCustomRead').COLL).doc(l.key)),x=saved.exists?saved.data():{};
        if(OrderRules.specialOf(line,{read:x.reads?.[x.latest],decided:x[PREFIX?'decidedSandbox':'decided']})?.notCut)l.noDesign=true;
      }
    }
  }
  const decisions=Readiness.decisions([...lines.values()]);
  for(const s of evidence.values())s.engraving=Object.fromEntries(Readiness.idsOf(s).map(id=>[id,decisions[id] || {needed:true,state:'unknown',approved:false}]));
  const orders=Readiness.orderReports([...lines.values()],[...evidence.values()]);
  // each sheet's own reading of its orders: an order waits only for its OTHER pieces (Readiness.forSheet), never for a piece on this very sheet, and (round 7)
  // never for a not-ready sheet of the sheet's OWN set: a set advances as one, that wait is the set's (Readiness.setGate), not the order's. An order split across sets still waits.
  for(const s of records)s.orderReadiness=Object.fromEntries(Readiness.orderIds(s).map(id=>[id,(!unverified.has(id) && Readiness.forSheet(orders[id],s.id || s.sheetId,Readiness.setOf(s))) || {ready:false,why:'Order readiness has not been verified'}]));
  return records;
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
/* A run's orders still in progress were kept in its record as fields, some sixty index entries a line, and one document
   holds 40,000: a run that took every open order (541 lines, 24 Sep) was 76% full before it had placed a charm, and a
   run fills and stops at about 700. They are now kept beside the record as JSON text (two index entries a part), in
   parts of at most 900 KB named by their content (Charm_Nest_Run_Live), which the record lists (liveLines). op_runPut
   writes a save's new parts, the record and the removal of the parts it replaced in one transaction. A record written
   before holds its lines itself and is read as it always was. */
const LIVE_PART_BYTES = 900000;
function liveParts(runId, lines) {
  const parts = []; let cur = {}, n = 0, size = 2;
  // in key order, so a save that changes a few lines rewrites only the parts that hold them
  for (const k of Object.keys(lines).sort()) {
    if (lines[k] === undefined) continue;
    const add = Buffer.byteLength(JSON.stringify(k)) + Buffer.byteLength(JSON.stringify(lines[k])) + 2;
    if (n && size + add > LIVE_PART_BYTES) { parts.push(cur); cur = {}; n = 0; size = 2; }
    cur[k] = lines[k]; n++; size += add;
  }
  if (n) parts.push(cur);
  return parts.map((p, seq) => { const json = JSON.stringify(p); return { id: `${runId}~${require("crypto").createHash("sha256").update(json).digest("hex").slice(0, 40)}`, json, bytes: Buffer.byteLength(json), lines: Object.keys(p).length, seq }; });
}
/** A run's record with the lines of its orders still in progress in lines: its own (a record written before they moved
    out), or its live parts'. A part is missing only when a newer save replaced it after this record was read: the newer
    record is read then, with its parts. */
async function withLiveLines(runId, run) {
  for (let tries = 0; run && run.liveLines && Array.isArray(run.liveLines.ids) && isId(runId); tries++) {
    const ids = run.liveLines.ids, docs = ids.length ? await db.getAll(...ids.map(x => col(RUN_LIVE).doc(x))) : [];
    if (tries < 3 && docs.some(d => !d.exists)) { const s = await col(RUNS).doc(runId).get(); run = s.exists ? s.data() : null; continue; }
    const lines = {};
    for (const d of docs) if (d.exists) { try { Object.assign(lines, JSON.parse(d.data().json || "{}")); } catch (_) { /* unreadable: its lines are left out */ } }
    return Object.assign({}, run, { lines });
  }
  return run;
}
/** withLiveLines for many runs, a few at a time: only a run with lines in progress reads anything. */
async function eachWithLiveLines(runs) {
  const want = runs.filter(r => r && r.liveLines && Array.isArray(r.liveLines.ids) && r.liveLines.ids.length && !r.lines);
  for (let i = 0; i < want.length; i += 20) await Promise.all(want.slice(i, i + 20).map(async r => { r.lines = ((await withLiveLines(r.runId, r)) || {}).lines || {}; }));
}
/** The engraving decisions of a run's copies: its record's lines', and — for the copies asked for (poolIds) that those
    do not decide — the archive's, read from the parts holding those copies' lines, or from every part when the run has
    fewer parts than that takes queries. The record's lines are newer than any part and win; a later part wins over an
    earlier one. A part decided under another readiness policy (DECISIONS_VERSION) is decided again from its lines. */
async function decisionsOfRun(runId, run, poolIds = null) {
  if (run && run.liveLines && !run.lines) run = await withLiveLines(runId, run);
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
/* ── The Library's live read (Paul, 3 Oct 04:47: "real-time", and what is still remaining for a sheet or a set). laserStatus
   with recordSeals !== true is a pure read: it reads sheet, set, run, archive and rule documents and writes none (the only
   writes in this operation are recordProcessReadiness, behind recordSeals === true, which the Library's slow check asks
   for and its fast reads never do). A full answer is heavy, though (whole sheet records, the runs' lines, the archive), so
   a reader that wants to follow the cloud every few seconds asks for the answer's revisions (wantRevs) and sends them back
   (ifRevs) with its next read: the documents the answer was made from are then read for their update times alone, and
   when none is newer the answer is just { unchanged: true } and nothing is read or worked out again. A document's update
   time changes with every write to it, whoever made it, so this cannot miss a change in a document it watches. It watches
   the sheets and sets asked for, a set's other sheets, the runs of those sheets and the other sheets that carry their
   orders; the learned no-design rules and a hand-completed special item are not watched (the Library's slow check
   reads them). ── */
const revOf = snap => (snap && snap.exists && snap.updateTime ? `${snap.updateTime.seconds}.${snap.updateTime.nanoseconds}` : "0");
const REV_KEY = /^[str]:[\w\-]{4,80}$/, MAX_REVS = 700;
/** The number of documents read when nothing a laserStatus answer was made from (revs) is newer, 0 when something is. */
async function laserUnchanged(sheetIds, setIds, revs) {
  const keys = Object.keys(revs || {});
  if (!keys.length || keys.length > MAX_REVS || !keys.every(k => REV_KEY.test(k))) return 0;
  const asked = [...new Set((sheetIds || []).filter(isId))].map(id => "s:" + id).concat([...new Set([].concat(setIds || []).filter(isId))].map(id => "t:" + id));
  if (!asked.length || !asked.every(k => Object.prototype.hasOwnProperty.call(revs, k))) return 0;   // (a card not read before: read in full)
  const coll = { s: SHEETS, t: SETS, r: RUNS }, parts = [];
  for (let i = 0; i < keys.length; i += 100) parts.push(keys.slice(i, i + 100));
  const answers = await Promise.all(parts.map(part => db.getAll(...part.map(k => col(coll[k[0]]).doc(k.slice(2))), { fieldMask: ["updatedAt"] })));
  for (let i = 0; i < parts.length; i++) for (let j = 0; j < parts[i].length; j++) if (revOf(answers[i][j]) !== String(revs[parts[i][j]])) return 0;
  return keys.length;
}
async function op_laserStatus(b) {
  const ids=[...new Set((b.sheetIds || []).filter(isId))].slice(0,500), records=[];
  if(b.recordSeals!==true && b.ifRevs && typeof b.ifRevs==='object'){const probed=await laserUnchanged(ids,b.setIds,b.ifRevs);if(probed)return {unchanged:true,probed,checkedAt:Date.now()};}
  const revs=b.wantRevs===true && b.recordSeals!==true?{}:null;
  for(let i=0;i<ids.length;i+=100){const docs=await db.getAll(...ids.slice(i,i+100).map(id=>col(SHEETS).doc(id)));for(const d of docs){if(revs)revs['s:'+d.id]=revOf(d);if(d.exists&&!d.data().archived)records.push({...d.data(),id:d.id});}}
  const setIds=[...new Set(records.map(s=>s.setId).concat(b.setIds || []).filter(isId))].slice(0,500),sets=[];
  for(let i=0;i<setIds.length;i+=100){const docs=await db.getAll(...setIds.slice(i,i+100).map(id=>col(SETS).doc(id)));for(const d of docs){if(revs)revs['t:'+d.id]=revOf(d);const s=d.exists?d.data():{};sets.push({setId:d.id,sheetIds:s.sheetIds || [],laserDoneAt:num(s.laserDoneAt) || null,laserDoneBy:s.laserDoneBy || null,processSeals:Readiness.processStamps(s),processReady:!!s.processReady});}}
  // The Sheets view may show only one metal or one member in the viewport. Read its
  // siblings too, so it follows the same complete-set gate as the Sets view.
  const have=new Set(records.map(s=>s.id)),missing=[...new Set(sets.flatMap(s=>s.sheetIds))].filter(id=>isId(id)&&!have.has(id)).slice(0,Math.max(0,500-records.length));
  for(let i=0;i<missing.length;i+=100){const docs=await db.getAll(...missing.slice(i,i+100).map(id=>col(SHEETS).doc(id)));for(const d of docs){if(revs)revs['s:'+d.id]=revOf(d);if(d.exists&&!d.data().archived)records.push({...d.data(),id:d.id});}}
  const added=[];
  // Only the active production view asks to record transitions. Ordinary status reads stay read-only.
  if(b.recordSeals===true){
    const groups=new Map(records.map(s=>s.setId && !s.draft && s.solidIncluded!==false?['set:'+s.setId,{kind:'set',id:s.setId}]:['sheet:'+s.id,{kind:'sheet',id:s.id}]));
    for(const g of groups.values()){
      const result=await recordProcessReadiness(g.kind,g.id,str(b.by,80).trim() || 'System');
      for(const p of result.records){const target=p.kind==='set'?sets.find(s=>s.setId===p.id):records.find(s=>s.id===p.id);if(target)Object.assign(target,p.patch);}
      added.push(...result.added);
    }
  }
  return {sheets:(await readinessRecords(records,{revs})).map(slim),sets,added,checkedAt:Date.now(),...(revs?{revs}:{})};
}

// All process seals are append-only. No trimming, replacement on re-completion, or client-written history.
function processEvent(how,at,by,n){return {id:how+'-'+at+'-'+n,how,at,by};}
function processRecord(kind,id,d){return {kind,id,patch:{processSeals:Readiness.processStamps(d),processReady:!!d.processReady,laserDoneAt:num(d.laserDoneAt) || null,laserDoneBy:d.laserDoneBy || null}};}
async function processHistory(kind,id,d){
  if(Array.isArray(d.processSeals))return d.processSeals;
  // Recover earlier completions even when the old Undo removed laserDoneAt. Each order on a sheet
  // carried the same recorded event; one press becomes one seal, with its original signer and time.
  const seals=Readiness.processStamps(d);let after=null;
  for(;;){
    let q=db.collection(PREFIX+'Order_Timeline').where(kind==='set'?'setId':'sheetId','==',id).orderBy('__name__').limit(500);
    if(after)q=q.startAfter(after);
    const snap=await q.get();
    for(const doc of snap.docs){const e=doc.data();if(e.type!=='laserDone' || !num(e.at) || (kind==='set' && e.data?.marked!=='set'))continue;
      if(!seals.some(s=>s.how==='laserDone' && s.at===num(e.at)))seals.push({id:'legacy-done-'+num(e.at),how:'laserDone',at:num(e.at),by:e.by || '',legacy:true});
    }
    if(snap.size<500)break;after=snap.docs.at(-1).id;
  }
  // Preserve the blue badges that predate timestamped seals, even when the stricter whole-order
  // gate now returns their sheets to In progress. Their missing time and signer stay explicit.
  const oldReady=kind==='sheet' && ms(d.createdAt)>0 && ms(d.createdAt)<Date.UTC(2026,8,30,18,21) && Readiness.sheet(d,{physicalOnly:true}).ready;
  if((oldReady || seals.some(s=>s.how==='laserDone')) && !seals.some(s=>s.how==='laserReady'))seals.push({id:'legacy-ready',how:'laserReady',at:0,by:'',legacy:true});
  return seals.sort((a,b)=>a.at-b.at);
}
async function processDecisions(tx,records){await productionReadiness(records,{tx});}

async function recordProcessReadiness(kind,id,by){
  const at=Date.now();
  return db.runTransaction(async tx=>{
    const ref=col(kind==='set'?SETS:SHEETS).doc(id),own=await tx.get(ref);
    if(!own.exists)return {records:[],added:[]};
    const d=own.data(),ids=kind==='set'?[...new Set(d.sheetIds || [])].filter(isId):[id];
    if(ids.length>300)return {records:[],added:[]};
    const docs=kind==='set' && ids.length?await tx.getAll(...ids.map(sid=>col(SHEETS).doc(sid)),{fieldMask:SLIM_SHEET.concat(['placements'])}):kind==='sheet'?[own]:[];
    const sheets=docs.filter(x=>x.exists && !x.data().archived).map(x=>({...x.data(),id:x.id}));
    await processDecisions(tx,sheets);
    const recovered=new Map();for(const s of sheets)recovered.set('sheet:'+s.id,await processHistory('sheet',s.id,s));
    if(kind==='set')recovered.set('set:'+id,await processHistory('set',id,d));
    const records=[],added=[];
    const save=(k,key,old,ready)=>{
      const processSeals=recovered.get(k+':'+key).slice();
      const oldBadge=!Array.isArray(old.processSeals) && processSeals.some(s=>s.id==='legacy-ready') && !processSeals.some(s=>s.how==='laserDone');
      if(ready && !old.processReady && !oldBadge){const event=processEvent('laserReady',at,by,processSeals.length);processSeals.push(event);added.push({kind:k,id:key,eventId:event.id});}
      const patch={processSeals,processReady:ready};
      if(!Array.isArray(old.processSeals) || !!old.processReady!==ready || JSON.stringify(old.processSeals)!==JSON.stringify(processSeals))tx.set(col(k==='set'?SETS:SHEETS).doc(key),{...patch,updatedAt:FV.serverTimestamp()},{merge:true});
      records.push(processRecord(k,key,{...old,...patch}));
    };
    const approved=sheets.map(s=>({...s,processSeals:recovered.get('sheet:'+s.id)}));
    for(const s of sheets)save('sheet',s.id,s,!num(s.laserDoneAt) && Readiness.laserSheet(approved.find(x=>x.id===s.id)).ready);
    if(kind==='set')save('set',id,d,!num(d.laserDoneAt) && sheets.every(s=>s.setId===id) && Readiness.laserGroup(d,approved).ready);
    return {records,added};
  });
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
  // (as putCharms: a name given in the sandbox must not rename the shared library production reads)
  if (PREFIX) return { ok: true, skipped: "the sandbox reads the shared charm library and does not write to it" };
  if (!isHash(b.hash)) return { error: "bad hash" };
  const name = str(b.name, 80).trim(); if (!name) return { error: "empty name" };
  await db.collection(LIB).doc(b.hash).set({ hash: b.hash, name, slug: name, namedBy: "operator", updatedAt: FV.serverTimestamp() }, { merge: true });
  return { ok: true };
}
async function op_listCharms(b) {
  const q = str(b.q, 80).toLowerCase(); const limit = Math.min(1000, Math.max(1, num(b.limit) || 400));
  const snap = await db.collection(LIB).orderBy(b.sort==="activity"?"updatedAt":"lastUsed", b.direction==="asc"?"asc":"desc").limit(limit).get();
  let rows = snap.docs.map(d => { const r = d.data(); return { hash: d.id, name: r.name || null, label: r.label || null, namedBy: r.namedBy || null, metalHint: r.metalHint || null, thumbUrl: r.thumbUrl || null, aiUrl: r.aiUrl || null, widthPt: num(r.widthPt), heightPt: num(r.heightPt), areaPt2: num(r.areaPt2), timesUsed: num(r.timesUsed), sourceName: r.sourceName || null, updatedAt:ms(r.updatedAt), createdAt:ms(r.createdAt), lastUsed: ms(r.lastUsed) }; });
  // Firestore cannot match a substring, so the limit has to come first and the filter second — which means a search
  // only ever sees the most recently used `limit` charms. A charm that genuinely exists but was last used 401 charms
  // ago used to come back as "no charms yet": a false negative on correct input. The caller is told how far it looked.
  const scanned = rows.length;
  if (q) rows = rows.filter(r => `${r.name || ""} ${r.label || ""} ${r.sourceName || ""}`.toLowerCase().includes(q));
  return { charms: rows, scanned, truncated: scanned >= limit };
}
/* A sheet document keeps a short copy of each approved back: what the Library, laser readiness, the back previews, the set
   manifest and a recalled Decided list read. The whole record (fit metrics, flip checks, the review, the reference
   picture) stays in Charm_Pool_Back, and getSheet puts it back for the readers that edit or report a back. A whole copy
   per back used to count toward the sheet document's 1 MiB. */
const SHEET_BACK_FIELDS = ["poolId", "sheetId", "setId", "runId", "order", "transactionId", "sku", "copy", "text", "lines", "lineGap", "lineMode", "font", "weight", "sizePt", "capMm", "box", "centre", "angle", "upAngle", "solidBack", "small", "thin", "name", "approvedAt", "approvedBy", "engravingSeals", "invalidated", "previewWPt", "previewHPt", "pageWPt", "pageHPt", "materialVersion"];
function sheetBack(bk) {
  if (!bk || typeof bk !== "object") return bk;
  const out = {};
  for (const k of SHEET_BACK_FIELDS) if (bk[k] !== undefined) out[k] = bk[k];
  if (bk.verified) out.verified = { geometry: { ok: !!(bk.verified.geometry && bk.verified.geometry.ok) }, file: { ok: !!(bk.verified.file && bk.verified.file.ok) } };
  if (bk.outputs) out.outputs = Object.fromEntries(["ai", "png"].filter(k => bk.outputs[k]).map(k => [k, { path: bk.outputs[k].path || null, url: bk.outputs[k].url || null }]));
  return out;
}
/** The whole record of each back a sheet lists, from Charm_Pool_Back, when it is still the approval the sheet holds. */
async function withFullBacks(d) {
  const pool = d.backPool || [], ids = [...new Set(pool.map(bk => bk && bk.poolId).filter(isPoolId))], full = new Map();
  for (let i = 0; i < ids.length; i += 100) for (const s of await db.getAll(...ids.slice(i, i + 100).map(id => col(BACK).doc(id)))) if (s.exists) full.set(s.id, s.data());
  d.backPool = pool.map(bk => {
    const r = bk && full.get(bk.poolId); if (!r || r.invalidated || +r.approvedAt !== +bk.approvedAt) return bk;
    const { updatedAt, invalidatedAt, superseded, ...rest } = r; void updatedAt; void invalidatedAt; void superseded;
    return Object.assign(rest, bk, { verified: rest.verified || bk.verified, outputs: bk.outputs || rest.outputs });
  });
  return d;
}
// Firestore refuses a document over 1 MiB; a sheet record is refused here first, in words, before it gets there.
const SHEET_DOC_BYTES = 900000;
async function op_putSheet(b) {
  const s = b.sheet || {}; if (!isId(s.id)) return { error: "bad sheet id" };
  const doc = Object.assign({}, s, { id: s.id, archived: false, updatedAt: FV.serverTimestamp() });
  delete doc.log;
  // a sheet is marked cut only by op_laserDone: a save of the open run's copy of it keeps the mark it has
  delete doc.laserDoneAt; delete doc.laserDoneBy; delete doc.processSeals; delete doc.processReady;
  // a hold back from Laser cutting and the Library's move history are server-owned too (op_flowApply): a stale page cannot clear them
  delete doc.laserHold; delete doc.flowHistory;
  // the listings its pieces were bought from, as the Library's listing search reads them (array-contains)
  if (Object.prototype.hasOwnProperty.call(s, "listings")) doc.listings = [...new Set((Array.isArray(s.listings) ? s.listings : []).map(v => String(v)).filter(v => /^\d{1,24}$/.test(v)))].slice(0, 500);
  // Physical stock and immutable cuts are only changed through transactional stock operations.
  for(const key of ['roseProtectedJson','roseStockId','roseRevision','rosePlanJson','rosePlanHash','roseFingerprint','roseCutAt','roseCutRevision'])delete doc[key];
  if (["gold10k","gold14k"].includes(s.metal) && s.solidIncluded === false) Object.assign(doc, {draft:true,setId:null,setSeq:null,sheetIndex:null,label:null});
  const ref = col(SHEETS).doc(s.id);
  const refused = await db.runTransaction(async tx => {
    const ex = await tx.get(ref), old = ex.exists ? ex.data() : {};
    // an order line goes on a sheet once (Paul, 29 Sep: a design went on its sheet twice): a record that would place one
    // piece twice is refused, in words; one saved so before this check saves as it was, to be put right by hand
    const twice = ids => { const seen = new Set(), dup = new Set(); for (const id of Array.isArray(ids) ? ids : []) if (id) { if (seen.has(id)) dup.add(id); seen.add(id); } return dup; };
    if (Array.isArray(s.poolIds)) { const was = twice(old.poolIds), extra = [...twice(s.poolIds)].filter(id => !was.has(id)); if (extra.length) return { error: `Sheet ${s.id} was not saved: it would put ${extra.length === 1 ? "piece " + extra[0] : `${extra.length} pieces (${extra.slice(0, 3).join(", ")})`} on it twice — a piece goes on a sheet once`, status: 409 }; }
    // a piece a cleanup took off this sheet on purpose (its record's `cleanup`, rg-cleanup-2026-09-29) is not put back by
    // a page that still shows the sheet as it was before: that page reloads first
    const cleared = new Set(old.cleanup ? [].concat(old.cleanup.removedPoolIds || [], old.cleanup.removedCharmIds || []) : []);
    if (cleared.size) {
      const back = [...new Set([...(Array.isArray(s.poolIds) ? s.poolIds : []), ...(Array.isArray(s.charms) ? s.charms.flatMap(c => [c && c.id, c && c.poolId]) : []), ...(Array.isArray(s.placements) ? s.placements.map(p => p && p.id) : [])].filter(id => cleared.has(id)))];
      if (back.length) return { error: `Sheet ${s.id} was not saved: it would put back ${back[0]}, which was taken off this sheet on purpose (${old.cleanup.id || "cleanup"}). Reload the page to see the sheet as it is now`, status: 409 };
    }
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
    doc.backPool = [...backs.values()].map(sheetBack);
    const bytes = Buffer.byteLength(JSON.stringify(Object.assign({}, old, doc)));
    if (bytes > SHEET_DOC_BYTES) return { error: `Sheet ${s.id} was not saved: its record would be ${Math.round(bytes / 1024).toLocaleString("en-US")} KB, over the ${SHEET_DOC_BYTES / 1000} KB one sheet record may hold (Firestore keeps at most 1 MiB in one document). Move some of its charms to another sheet and save again.`, status: 413 };
    tx.set(ref, doc, {merge:true});
    return null;
  });
  return refused || { ok: true, id: s.id };
}
/* The newest sheets, or a run's or a set's, in parts (ANSWER_BYTES): the first part reads the records the list is made of
   (their SLIM_SHEET fields) and sends as many as fit; `next` names the rest in order, and a later part reads those by id. */
async function op_listSheets(b) {
  let rows;
  if (b.cursor && Array.isArray(b.cursor.sheets)) rows = b.cursor.sheets.filter(isId).slice(0, 500).map(id => [id]);
  else {
    const limit = Math.min(500, Math.max(1, num(b.limit) || 300));
    let q = b.runId ? col(SHEETS).where("runId", "==", b.runId) : b.setId ? col(SHEETS).where("setId", "==", b.setId) : col(SHEETS).orderBy("day", "desc");
    if (b.from && /^\d{4}-\d{2}-\d{2}$/.test(b.from)) q = q.where("day", ">=", b.from);
    if (b.to && /^\d{4}-\d{2}-\d{2}$/.test(b.to)) q = q.where("day", "<=", b.to);
    const snap = await q.limit(limit).select(...SLIM_SHEET).get();
    // (excludeDone: the Library's Current tab, which leaves out what the laser has cut, and its readiness is not worked out)
    const records=await filingRecords(snap.docs.map(d=>({...d.data(),id:d.id})));
    rows = records.filter(d=>!d.archived && !(b.excludeDone && Readiness.filed(d))).map(d=>[d.id,d]);
    if (b.metal && /^(gold|silver|rose|gold10k|gold14k)$/.test(b.metal)) rows = rows.filter(([, d]) => d.metal === b.metal);
    if (b.setId) rows = rows.filter(([, d]) => d.setId === b.setId);
    if (b.runId) rows = rows.filter(([, d]) => d.runId === b.runId);
    rows.sort(([, x], [, y]) => (ms(y.updatedAt) || 0) - (ms(x.updatedAt) || 0));
  }
  const { sheets, rest } = await sheetEntries(rows, answerBudget());
  return { sheets, next: rest.length ? { sheets: rest } : null, truncated: rest.length > 0 };
}
async function op_getSheet(b) {
  if (!isId(b.id)) return { error: "bad id" };
  const s = await col(SHEETS).doc(b.id).get();
  if (!s.exists) return { sheet: null };
  const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt);
  await withFullBacks(d);
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
 *  that a later re-upload replaced). Charm links missing at save time are filled from the charm library.
 *  This is optional enrichment, not a prerequisite for reading a saved layout: a slow Storage metadata lookup must
 *  never keep getSheet waiting indefinitely. Existing URLs survive failures and the bounded refresh below. */
const LINK_REFRESH_MS = 1500, LINK_REFRESH_CONCURRENCY = 12;
async function refreshLinks(d) {
  const bucket = admin.storage().bucket();
  const urlFor = async (path) => {
    if (!path) return null;
    try { const file = bucket.file(path); const [meta] = await file.getMetadata(); let t = meta.metadata && meta.metadata.firebaseStorageDownloadTokens; if (!t) return null; t = String(t).split(",")[0];
      return "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucket.name) + "/o/" + encodeURIComponent(path) + "?alt=media&token=" + encodeURIComponent(t); } catch (_) { return null; }
  };
  const paths = new Map();
  const link = (path, apply) => { if (!path) return; if (!paths.has(path)) paths.set(path, []); paths.get(path).push(apply); };
  for (const k of ["ai", "pdf", "labelled", "report", "preview"]) { const o = d.outputs && d.outputs[k]; if (o && o.path) link(o.path, u => { o.url = u; }); }
  for (const f of (d.label && d.label.files) || []) if (f.path) link(f.path, u => { f.url = u; });
  for (const k of ["index", "report"]) { const o = d.backOutputs && d.backOutputs[k]; if (o && o.path) link(o.path, u => { o.url = u; }); }
  for (const bk of d.backPool || []) for (const k of ["ai", "png"]) { const o = bk.outputs && bk.outputs[k]; if (o && o.path) link(o.path, u => { o.url = u; }); }
  for (const c of d.charms || []) {
    const pngPath = c.pngPath || (c.hash ? "charmnest/charms/" + c.hash + ".png" : null), aiPath = c.aiPath || (c.hash ? "charmnest/charms/" + c.hash + ".ai" : null);
    link(pngPath, u => { c.thumbUrl = u; });
    link(aiPath, u => { c.aiUrl = u; });
  }
  const entries = [...paths]; if (!entries.length) return;
  let cursor = 0, closed = false, timer;
  const worker = async () => {
    while (!closed && cursor < entries.length) {
      const [path, targets] = entries[cursor++], url = await urlFor(path);
      // A late lookup neither mutates the answer after its deadline nor starts another optional request.
      if (!closed && url) for (const apply of targets) apply(url);
    }
  };
  try {
    await Promise.race([
      Promise.all(Array.from({ length: Math.min(LINK_REFRESH_CONCURRENCY, entries.length) }, worker)),
      new Promise(resolve => { timer = setTimeout(() => { closed = true; resolve(); }, LINK_REFRESH_MS); })
    ]);
  } finally { closed = true; clearTimeout(timer); }
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
    await Promise.all(outputPaths(d).map(p => bucket.file(p).delete().catch(() => {})));
    // Approved back files are never deleted: those a Charm_Pool_Back record still names stay where they are, and the
    // rest go to the archive path, as a superseded approval's do.
    const backs = (d.backPool || []).filter(bk => bk && isPoolId(bk.poolId)), named = new Set();
    for (let i = 0; i < backs.length; i += 100) for (const s of await db.getAll(...backs.slice(i, i + 100).map(bk => col(BACK).doc(bk.poolId)))) if (s.exists) for (const k of ["ai", "png"]) { const p = s.data().outputs && s.data().outputs[k] && s.data().outputs[k].path; if (p) named.add(p); }
    await archiveFiles(backs.flatMap(bk => ["ai", "png"].map(k => bk.outputs && bk.outputs[k] && bk.outputs[k].path)).filter(p => p && !named.has(p)).map(from => ({ from, to: archivePath(from) })));
  }
  return { ok: true, deleted: !!d };
}
/** A sheet's own five files, and the .pdf beside its .ai, which is written only when the sheet is final (op_sheetPdf). */
const outputPaths = d => { const o = d.outputs || {}, ai = o.ai && o.ai.path; return [...new Set(["ai", "pdf", "labelled", "report", "preview"].map(k => o[k] && o[k].path).concat(ai && /\.ai$/.test(ai) ? [ai.replace(/\.ai$/, ".pdf")] : []).filter(Boolean))]; };
/* The archive path of a superseded approved file: charmnest/superseded/<the rest of its path> (the sandbox's stay under
   charmnest/sandbox/superseded/). Nothing there is read by the app; a bucket lifecycle rule can move it to cold storage. */
const archivePath = p => /^charmnest\/sandbox\//.test(p) ? p.replace(/^charmnest\/sandbox\//, "charmnest/sandbox/superseded/") : p.replace(/^charmnest\//, "charmnest/superseded/");
async function archiveFiles(list) {
  const bucket = list.length ? admin.storage().bucket() : null;
  await Promise.all(list.filter(f => /^charmnest\//.test(f.from) && !/^charmnest\/(sandbox\/)?superseded\//.test(f.from)).map(async ({ from, to }) => {
    try { await bucket.file(from).move(to); } catch (e) { if (!(e && (e.code === 404 || e.code === "404"))) console.warn(`[charmNestLibrary] ${from} could not be moved to ${to}: ${e && e.message}`); }
  }));
}
/* A sheet's .pdf is its .ai under a second name (an Illustrator file is a PDF), for whoever opens files by type. It was
   uploaded again, from the browser, with every save of every sheet; now it is made when the sheet is final (released
   full, its intake finished) and when its set is released for labels: a copy inside the bucket, made again only when
   the .ai changed. The record's outputs.pdf says where it is. */
async function op_sheetPdf(b) {
  const ids = [...new Set([].concat(b.ids || [], b.id || []).map(x => str(x, 80)).filter(isId))].slice(0, 60);
  if (!ids.length) return { error: "bad sheet id" };
  const bucket = admin.storage().bucket(), urls = {};
  for (const id of ids) {
    const ref = col(SHEETS).doc(id), snap = await ref.get(); if (!snap.exists) continue;
    const d = snap.data(), ai = d.outputs && d.outputs.ai && d.outputs.ai.path;
    if (d.archived || !ai || !/^charmnest\/.+\.ai$/.test(ai)) continue;
    const path = ai.replace(/\.ai$/, ".pdf"), src = bucket.file(ai), dst = bucket.file(path);
    try {
      const [[am], [pm]] = await Promise.all([src.getMetadata(), dst.getMetadata().catch(() => [null])]);
      const kept = pm && pm.metadata && String(pm.metadata.firebaseStorageDownloadTokens || "").split(",")[0], same = !!(kept && am.md5Hash && pm.md5Hash === am.md5Hash);
      if (!same) await src.copy(dst);
      const token = kept || crypto.randomUUID();
      if (!same) await dst.setMetadata({ contentType: "application/pdf", metadata: { firebaseStorageDownloadTokens: token } });
      urls[id] = "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucket.name) + "/o/" + encodeURIComponent(path) + "?alt=media&token=" + encodeURIComponent(token);
      await ref.update({ "outputs.pdf": { path, url: urls[id] } });
    } catch (e) { console.warn(`[charmNestLibrary] sheet ${id}: .pdf not made: ${e && e.message}`); }
  }
  return { ok: true, urls };
}
/* A calibration row is a statistic the fill estimate learns from — never part of the sheet record. A sheet that placed
   nothing has nothing to teach, so it is skipped, not refused: an error here used to travel all the way up and stop a run
   whose sheets were already written, verified and saved. */
async function op_putCalibration(b) {
  // calibration steers production nesting; a rehearsal's sheets are not evidence for it
  if (PREFIX) return { ok: true, skipped: "sandbox sheets are not calibration evidence" };
  const r = b.row || {};
  if (!(num(r.density) > 0) || !(num(r.count) > 0)) return { ok: true, skipped: "nothing to learn from this sheet" };
  // one row per sheet, the newest saving it: a sheet saved again as it fills replaces its row instead of adding one, so
  // the collection grows with the sheets made, not with every save (the station runs for days)
  const row = { sheetId: str(r.sheetId, 80), metal: str(r.metal, 12), count: num(r.count), cv: num(r.cv), largestFrac: num(r.largestFrac), density: num(r.density), placedAll: !!r.placedAll, clearancePt: num(r.clearancePt), createdAt: FV.serverTimestamp() };
  if (/^[\w.\-]{1,80}$/.test(row.sheetId)) await db.collection(CAL).doc(row.sheetId).set(row);
  else await db.collection(CAL).add(row);
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
  const mode = ["grouping", "layout", "name", "place", "packing", "labelRead", "engraveIntent", "engraveReview", "customRead"].includes(b.mode) ? b.mode : null;
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
  if (!kick.ok && kick.status !== 202) {
    // nothing will read the parked payload now (a retry parks its own); the job record keeps the error
    await admin.storage().bucket().file(payloadPath).delete().catch(() => {});
    await db.collection(PREFIX + AGENT).doc(id).set({ status: "error", error: `kick failed: ${kick.status} ${kick.statusText || ""}`, updatedAt: FV.serverTimestamp() }, { merge: true }); return { error: `could not start the background job (${kick.status})` };
  }
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
const POOL = "Charm_Pool", BACK = "Charm_Pool_Back", SETS = "Charm_Nest_Sets", COUNTERS = "Charm_Nest_Counters", RUNS = "Charm_Nest_Runs", RUN_LINES = "Charm_Nest_Run_Lines", RUN_LIVE = "Charm_Nest_Run_Live", RELEASE = "Charm_Nest_Release", BRIDGE = "Design_Bridge", ALIASES = "Charm_Sku_Aliases", NODESIGN = "Charm_Sku_NoDesign", OPTMAP = "Charm_Option_Map";
// the learned maps: shared and read by both sides, but a sandbox answer is written to Sandbox_<name> only (see the top of this file)
const SANDBOX_MAPS = [ALIASES, NODESIGN, OPTMAP];
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
/** What changes whenever the index does: how many entries it holds, and when one was last indexed and last edited
    (masterPatch). A background reload of the library that finds these as they were reads nothing more (Master.load). */
async function masterIndexSig() {
  const ix = db.collection(Master.INDEX);
  const [n, indexed, edited] = await Promise.all([ix.count().get(), ix.orderBy("indexedAt", "desc").limit(1).select("indexedAt").get(), ix.orderBy("updatedAt", "desc").limit(1).select("updatedAt").get()]);
  return { count: n.data().count, indexedAt: indexed.size ? ms(indexed.docs[0].data().indexedAt) : null, updatedAt: edited.size ? ms(edited.docs[0].data().updatedAt) : null };
}
/* The index in parts, in SKU (document id) order: a part holds up to `limit` entries, what fits in ANSWER_BYTES and what
   was read within ANSWER_MS, and `next` is the SKU after which the next part starts (null at the end of the index). It
   used to be the first 3,000 documents Firestore handed out, in no set order, and a library past that lost the rest
   without a word. The first part says the index's signature (index), read before any entry is: a change made while the
   parts are read shows as a change the next time. */
async function op_masterList(b) {
  const began = Date.now(), q = str(b.q, 80).toUpperCase(), limit = Math.min(3000, Math.max(1, num(b.limit) || 1500));
  const index = b.cursor ? null : await masterIndexSig(), rows = [], size = q || b.masterHash ? 500 : Math.min(500, limit);   // a few entries (the connections check) read a few
  let after = b.cursor ? str(b.cursor, 80) : null, used = 0, next = null, truncated = false;
  read: for (;;) {
    let page = db.collection(Master.INDEX).orderBy(docOrder()).limit(size); if (after) page = page.startAfter(after);
    const snap = await page.get();
    for (const d of snap.docs) {
      const e = d.data(), r = e.sku ? Master.slimEntry(e) : null;   // a shell without a SKU (a patch on an unindexed SKU) is not an entry
      if (r && (!b.masterHash || r.masterHash === b.masterHash) && (!q || String(r.sku || "").includes(q))) {
        const n = bytesOf(r);
        if (rows.length && used + n > ANSWER_BYTES) { next = after; truncated = true; break read; }
        rows.push(r); used += n;
      }
      after = d.id;
      if (rows.length >= limit) { next = after; break read; }
    }
    if (snap.size < size) break;
    if (Date.now() - began > ANSWER_MS) { next = after; truncated = true; break; }
  }
  rows.sort((x, y) => x.sku.localeCompare(y.sku));
  if (b.links) for (const r of rows) await withLinks(r);
  return Object.assign({ entries: rows, next, truncated }, index ? { index } : {});
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
/** The 200 master files indexed last, newest first (it took the first 200 Firestore handed out and sorted those), as many
    as fit in one answer (a file's record keeps lists of up to 2,000 SKUs); and the index's signature (masterIndexSig). */
async function op_masterListFiles() {
  const [snap, index] = await Promise.all([db.collection(Master.FILES).orderBy("indexedAt", "desc").limit(200).get(), masterIndexSig()]);
  const rows = snap.docs.map(d => { const r = d.data(); r.indexedAt = ms(r.indexedAt); return r; }), budget = answerBudget();
  let n = 0; while (n < rows.length && budget.fits(rows[n])) n++;
  return { files: rows.slice(0, n), index, truncated: n < rows.length };
}
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

/* ── order timeline stamps (Paul, 28 Sep, D1-D3: every change is recorded, every step passed is a milestone). The ops that
   already know who did what record it on each order's timeline (_orderTimeline.js): customPut, laserDone, backPut,
   poolUpdate, customDecide, roseRecordCut, arrivalRecord, cancelRestore. A stamp never fails its op: a failed write is
   logged and the op answers as before. One batched write per op call (another only past 100 events). Each event's id is
   made from what the op itself recorded (a time it stored, a sheet, a revision), so an op sent twice writes it once. ── */
async function stamp(events, what) {
  try {
    // (events may be a function that builds them: whatever it throws is caught here too)
    const made = typeof events === "function" ? events() : events, list = (Array.isArray(made) ? made : [made]).filter(Boolean);
    if (!list.length) return 0;
    const TL = require("./_orderTimeline");
    for (let i = 0; i < list.length; i += 100) await TL.add(db, FV, list.slice(i, i + 100), { prefix: PREFIX, source: "sorter" });
    return list.length;
  } catch (e) { console.warn(`[charmNestLibrary] timeline not recorded (${what}):`, (e && e.message) || e); return 0; }
}
/** The order (receipt) of a line key or pool id ("rid_tid" / "rid_tid_copy"); lineOfCopy gives a pool id's line. */
const orderOfKey = k => (/^(\d{1,30})_/.exec(String(k || "")) || [])[1] || "";
/** "GF Sheet 2": a sheet record's metal and number, or those in its file name (GF_Sep.16.26_Set-3_Sheet-1). */
function sheetLabel(d, name) {
  const f = String(name || (d && (d.fileBase || d.folder)) || ""), m = /^([A-Za-z0-9]+)_.*_Sheet-(\d+)/.exec(f);
  const code = (d && METAL_CODE[d.metal]) || (m && m[1]) || "", no = (d && (num(d.sheetIndex) || num(d.page))) || (m && +m[2]) || 0;
  return code && no ? `${code} Sheet ${no}` : f.slice(0, 80);
}
const setLabel = id => { const m = /-(\d+)$/.exec(String(id || "")); return m ? `Set ${+m[1]}` : ""; };
/** One event per order for a poolUpdate that says a charm left a sheet (removedBy/At), moved (movedBy/At), was committed
    with its set (committedAt) or was written on a sheet (state "written" with a sheetId). `before` is each row as it was
    (null when it could not be read): a row that already says so (a retry) records nothing again. */
function poolEvents(ids, p, before, b) {
  const kind = p.removedBy || p.removedAt ? "removed" : p.movedBy || p.movedAt ? "moved" : p.committedAt ? "setCommitted" : p.state === "written" && p.sheetId ? "placed" : null;
  if (!kind) return [];
  const at = (kind === "removed" ? num(p.removedAt) : kind === "moved" ? num(p.movedAt) : kind === "setCommitted" ? num(p.committedAt) : 0) || (kind === "placed" ? 0 : Date.now());
  const groups = new Map();
  for (const id of ids) {
    const prev = before ? before.get(id) || null : null;
    if (prev && (kind === "placed" ? prev.sheetId === p.sheetId : kind === "removed" ? num(prev.removedAt) === at : kind === "moved" ? num(prev.movedAt) === at : num(prev.committedAt) === at)) continue;
    const orderId = String((prev && prev.orderId) || orderOfKey(id)); if (!orderId) continue;
    if (!groups.has(orderId)) groups.set(orderId, []);
    groups.get(orderId).push({ id, prev: prev || {} });
  }
  // (station tracking, 28 Sep: the sorter says who is on duty, b.by, or that nobody is, b.signedIn false: then the stamp
  //  names no one, by "" with data.signedIn false, and its seal says "not signed in", never "System". A page that says
  //  neither, an older one, keeps the old default)
  const nobody = b.signedIn === false;
  const by = nobody ? "" : str(b.by || (kind === "removed" ? p.removedBy : kind === "moved" ? p.movedBy : kind === "setCommitted" ? p.committedBy : "") || (kind === "placed" ? "System" : ""), 80);
  const out = [];
  for (const [orderId, rows] of groups) {
    const lines = [...new Set(rows.map(r => r.prev.lineKey || lineOfCopy(r.id)))], tids = [...new Set(rows.map(r => String(r.prev.transactionId || r.id.split("_")[1])))];
    const was = [...new Set(rows.map(r => r.prev.sheetId).filter(Boolean))], wasNames = [...new Set(rows.map(r => sheetLabel(null, r.prev.sheetName) || r.prev.sheetId).filter(Boolean))];
    const e = { orderId, type: kind, by, station: "sorter", device: str(b.device, 40), lineKey: lines.length === 1 ? lines[0] : "", transactionId: tids.length === 1 ? tids[0] : "", data: Object.assign({ copies: rows.length, poolIds: rows.slice(0, 40).map(r => r.id), lines: lines.slice(0, 20) }, nobody ? { signedIn: false } : {}, b.employeeId ? { employeeId: str(b.employeeId, 60) } : {}) };
    // a cancel's removal (Paul, 29 Sep 00:26): each sheet (or the pool, for pieces not placed yet) its own step, "Removed
    // from GF Sheet 1 (Set 2)", with the outcome; id: the removal time and the place, which AutoCancel's own event uses too
    if (kind === "removed" && /^cancel/i.test(String(p.removedReason || ""))) {
      const bySheet = new Map(); for (const r of rows) { const k = r.prev.sheetId || ""; if (!bySheet.has(k)) bySheet.set(k, []); bySheet.get(k).push(r); }
      for (const [sid, rs] of bySheet) {
        const r0 = rs[0].prev, name = sid ? sheetLabel(null, r0.sheetName) || str(sid, 80) : "", set = setLabel(r0.setId), metal = METAL_CODE[r0.material] || METAL_CODE[r0.metal] || "";
        out.push(Object.assign({}, e, { at, sheetId: str(sid, 100), sheet: name || (metal ? metal + " pool" : "the pool"), setId: r0.setId || "", id: `${at}-${sid || "pool"}`,
          text: sid ? `Removed from ${name}${set ? " (" + set + ")" : ""}` : `Removed from the ${metal ? metal + " " : ""}pool`,
          data: Object.assign({}, e.data, { copies: rs.length, poolIds: rs.slice(0, 40).map(r => r.id), reason: str(p.removedReason, 300), sheets: sid ? [sid] : [], cancel: true, outcome: "removed" }) }));
      }
      continue;
    }
    if (kind === "removed") Object.assign(e, { at, sheetId: was[0] || "", sheet: wasNames.join(", "), setId: rows[0].prev.setId || "", text: [wasNames.join(", "), p.removedReason].filter(Boolean).join(" · "), id: String(at) }, { data: Object.assign(e.data, { reason: str(p.removedReason, 300), sheets: was }) });
    else if (kind === "moved") {
      const from = wasNames.length ? wasNames : String(p.movedFrom || "").split(",").filter(Boolean), to = sheetLabel(null, p.sheetName) || str(p.movedTo, 100);
      Object.assign(e, { at, sheetId: str(p.movedTo || p.sheetId, 100), sheet: to, setId: rows[0].prev.setId || "", text: `${from.join(", ") || "another sheet"} → ${to}`, id: String(at) }, { data: Object.assign(e.data, { from: String(p.movedFrom || was.join(",")), fromSheets: from, to: str(p.movedTo, 100) }) });
    } else if (kind === "setCommitted") { const setId = rows[0].prev.setId || ""; Object.assign(e, { at, setId, sheetId: was[0] || "", sheet: wasNames.join(", "), text: setLabel(setId), id: String(at) }); }
    else { const sheet = sheetLabel(null, p.sheetName) || str(p.sheetId, 80); Object.assign(e, { sheetId: str(p.sheetId, 100), sheet, setId: str(p.setId, 100), text: sheet, id: str(p.sheetId, 100) }); }
    out.push(e);
  }
  return out;
}

// ── pool ──
async function op_poolPut(b) {
  const rows = (Array.isArray(b.pools) ? b.pools : [b.pool]).filter(p => p && isPoolId(p.poolId)).slice(0, 400);
  if (!rows.length) return { error: "no pool rows" };
  const out = { written: 0, contended: [], placed: [] };
  /* Two runs contending for one line: a row claimed by a LIVE run (fresh within 24 h, not finished) belongs to that run.
     A run that was stopped or given up is not live, whatever its rows say: yesterday's stopped run kept 152 lines
     out of today's on the strength of rows it never finished, so the run itself is asked, once per run. */
  const runLive = new Map();
  const liveRun = async runId => { if (runLive.has(runId)) return runLive.get(runId); let live = true; try { const s = await col(RUNS).doc(runId).get(); const r = s.exists ? s.data() : null; live = !r ? true : (!["stopped", "complete", "abandoned"].includes(r.status) && r.step !== "complete"); /* no record at all: assume live */ } catch (_) {} runLive.set(runId, live); return live; };
  /* The rows are read together and written in batches. Read and written one after another, two round trips each, the
     four hundred rows of a run that took every open order ran past the edge's patience, and its HTML "Inactivity
     Timeout" page came back as the reason the orders were held (25 Sep). A row sent twice is written once, merged. */
  const byId = new Map(); for (const p of rows) byId.set(p.poolId, Object.assign(byId.get(p.poolId) || {}, p));
  const list = [...byId.values()], found = [];
  for (let i = 0; i < list.length; i += 100) found.push(...await db.getAll(...list.slice(i, i + 100).map(p => col(POOL).doc(p.poolId))));
  /* A line already on a saved sheet is never placed again (Paul, 29 Sep: an order's design went on its sheet twice): a
     row whose record puts it on a sheet (not taken off since: abandoned or superseded), and whose sheet's saved record
     still lists it, is not written over by a fresh placement (no sheet id). It is answered as placed, with where it is. */
  const onSheet = new Map(), want = new Map();
  list.forEach((p, i) => { const cur = found[i] && found[i].exists ? found[i].data() : null; if (cur && cur.sheetId && !p.sheetId && !["abandoned", "superseded"].includes(cur.state)) want.set(p.poolId, cur); });
  const sheetIds = [...new Set([...want.values()].map(c => String(c.sheetId)).filter(isId))], sheetsRead = new Map();
  for (let i = 0; i < sheetIds.length; i += 100) for (const s of await db.getAll(...sheetIds.slice(i, i + 100).map(id => col(SHEETS).doc(id)), { fieldMask: ["poolIds", "archived", "fileBase"] })) if (s.exists) sheetsRead.set(s.id, s.data());
  for (const [id, cur] of want) { const sh = sheetsRead.get(String(cur.sheetId)); if (sh && !sh.archived && (sh.poolIds || []).includes(id)) onSheet.set(id, { poolId: id, sheetId: cur.sheetId, sheetName: cur.sheetName || sh.fileBase || null, state: cur.state || null, setId: cur.setId || null }); }
  let batch = db.batch(), n = 0;
  for (const [i, p] of list.entries()) {
    const ex = found[i], cur = ex && ex.exists ? ex.data() : null;
    if (cur && cur.runId && p.runId && cur.runId !== p.runId && !["complete", "abandoned", "committed"].includes(cur.state) && (Date.now() - (ms(cur.updatedAt) || 0)) < 24 * 3600 * 1000 && await liveRun(cur.runId)) { out.contended.push({ poolId: p.poolId, runId: cur.runId }); continue; }
    if (onSheet.has(p.poolId)) { out.placed.push(onSheet.get(p.poolId)); continue; }
    const doc = Object.assign({}, p, { poolId: p.poolId, updatedAt: FV.serverTimestamp() }); if (!cur) doc.createdAt = FV.serverTimestamp();
    batch.set(col(POOL).doc(p.poolId), doc, { merge: true }); out.written++;
    if (++n >= 400) { await batch.commit(); batch = db.batch(); n = 0; }
  }
  if (n) await batch.commit();
  return Object.assign({ ok: true }, out);
}
async function op_poolUpdate(b) {
  const ids = (Array.isArray(b.poolIds) ? b.poolIds : [b.poolId]).filter(isPoolId).slice(0, 400); if (!ids.length) return { error: "bad pool id" };
  // a change the order's timeline records (poolEvents) reads the rows first: the sheet a charm leaves, its set, and
  // whether the row already says so (a retry)
  const p = b.patch && typeof b.patch === "object" ? b.patch : {}, told = !!(p.removedBy || p.removedAt || p.movedBy || p.movedAt || p.committedAt || (p.state === "written" && p.sheetId));
  let before = null;
  if (told) try { before = new Map(); for (let i = 0; i < ids.length; i += 100) (await db.getAll(...ids.slice(i, i + 100).map(id => col(POOL).doc(id)))).forEach((s, j) => before.set(ids[i + j], s.exists ? s.data() : null)); }
  catch (e) { before = null; console.warn("[charmNestLibrary] pool rows not read for the timeline:", e.message || e); }
  let batch = db.batch(), n = 0;
  for (const id of ids) { batch.set(col(POOL).doc(id), Object.assign({}, b.patch || {}, { updatedAt: FV.serverTimestamp() }), { merge: true }); if (++n >= 400) { await batch.commit(); batch = db.batch(); n = 0; } }
  if (n) await batch.commit();
  if (told) await stamp(() => poolEvents(ids, p, before, b), "pool");
  if (told && /^cancel/i.test(String(p.removedReason || ""))) await noteCancelRemovals(ids, p, before, b);
  return { ok: true, count: ids.length };
}
/* A cancelled order's pieces taken off (removedReason "cancelled...", the sheet window's Cancel and AutoCancel): each sheet
   they left is a removal on its cancel record too, with when and who (_orderCancel.noteRemovals; kept for good, 29 Sep).
   Only a record already there: a person's cancel writes its record after the take-off, and adds its sheets then
   (cancelFates). Never fails the op. */
async function noteCancelRemovals(ids, p, before, b) {
  try {
    for (const e of poolEvents(ids, p, before, b).filter(e => e.type === "removed").slice(0, 20)) {
      const where = str(e.sheet, 100);
      await OrderCancel.noteRemovals(db, e.orderId, [where ? { id: `sheet~${where}`, at: e.at, where, kind: "sheet", outcome: "removed", by: e.by, lineKey: e.lineKey, text: `taken off ${where}` }
        : { id: `pool~${e.at}`, at: e.at, where: "the pool", kind: "pool", outcome: "removed", by: e.by, lineKey: e.lineKey, text: "taken out of the pool" }], { prefix: PREFIX });
    }
  } catch (e) { console.warn("[charmNestLibrary] cancel record: removal not noted:", (e && e.message) || e); }
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
/* Where each piece of an order is, from the records that can say so (Paul, 5 Oct: a Silver piece on SS Sheet 1 read "not on a
   sheet yet" from its order's Gold Filled sheet). The pool row's sheetId is only a hint (a re-nest, a restart or an older run
   clears it while the sheet still holds the piece); the SAVED SHEETS that list the piece in poolIds are the truth. One light
   read per order (no readiness work): the order's pool rows (every state, the page decides what an abandoned one means) and the
   saved sheets that name the order or any of its pool ids, each with only the fields placing a piece needs and only the poolIds
   of this order. The page (charm-nest-order-pieces.js, OrderPieces) decides from these. Read only. */
const PIECE_SHEET_FIELDS = ["id", "sheetId", "metal", "metalLabel", "fileBase", "folder", "sheetIndex", "page", "setId", "setSeq", "poolIds", "orders", "archived", "laserDoneAt", "roseCutAt", "draft", "solidIncluded", "status", "runId", "updatedAt", "createdAt", "laserHold"];
async function op_getOrderPieces(b) {
  // sheetIds: the same fields of those sheets with ALL their poolIds (who shares a sheet with whom), read by id
  const sheetIds = (Array.isArray(b.sheetIds) ? b.sheetIds : []).filter(isId).slice(0, 40);
  const ids = [...new Set((Array.isArray(b.orderIds) ? b.orderIds : [b.orderId]).map(x => String(x == null ? "" : x).replace(/\D/g, "")).filter(x => /^\d{4,20}$/.test(x)))].slice(0, 40);
  if (!ids.length && !sheetIds.length) return { error: "orderId required" };
  const bySheet = {};
  for (let i = 0; i < sheetIds.length; i += 30) for (const d of await db.getAll(...sheetIds.slice(i, i + 30).map(id => col(SHEETS).doc(id)), { fieldMask: PIECE_SHEET_FIELDS })) {
    if (!d.exists) continue; const s = d.data(); if (s.archived) continue;
    bySheet[d.id] = { id: d.id, metal: s.metal || null, fileBase: s.fileBase || s.folder || null, folder: s.folder || null, sheetIndex: num(s.sheetIndex) || null, page: num(s.page) || null, setId: s.setId || null, setSeq: num(s.setSeq) || null, poolIds: (s.poolIds || []).map(String), orders: (s.orders || []).map(String).slice(0, 500),
      laserDoneAt: num(s.laserDoneAt) || null, roseCutAt: num(s.roseCutAt) || null, draft: !!s.draft, solidIncluded: s.solidIncluded == null ? null : !!s.solidIncluded, status: s.status || null, runId: s.runId || null, updatedAt: ms(s.updatedAt), createdAt: ms(s.createdAt) };
  }
  if (!ids.length) return { ok: true, orders: {}, sheets: bySheet };
  const both = id => [id].concat(Number.isSafeInteger(+id) ? [+id] : []);
  const pools = new Map(ids.map(id => [id, []])), sheets = new Map(), sheetsOf = new Map(ids.map(id => [id, new Map()]));
  for (let i = 0; i < ids.length; i += 10) {
    const part = ids.slice(i, i + 10), values = part.flatMap(both);
    const [ps, xs] = await Promise.all([
      col(POOL).where("orderId", "in", values).limit(2000).get(),
      col(SHEETS).where("orders", "array-contains-any", values).limit(HISTORY_CAP.sheets).select(...PIECE_SHEET_FIELDS).get()
    ]);
    for (const d of ps.docs) { const r = d.data(), rid = String(r.orderId == null ? String(d.id).split("_")[0] : r.orderId); if (pools.has(rid)) { r.updatedAt = ms(r.updatedAt); r.createdAt = ms(r.createdAt); pools.get(rid).push(r); } }
    for (const d of xs.docs) sheets.set(d.id, { ...d.data(), id: d.id });
  }
  // sheets that list one of the order's pieces although their `orders` list does not name the order (an older or cut-down record)
  const listed = new Set([...sheets.values()].flatMap(s => s.poolIds || []).map(String));
  const missing = [...pools.values()].flat().map(p => String(p.poolId)).filter(id => !listed.has(id));
  for (let i = 0; i < missing.length; i += 30) for (const d of (await col(SHEETS).where("poolIds", "array-contains-any", missing.slice(i, i + 30)).select(...PIECE_SHEET_FIELDS).get()).docs) if (!sheets.has(d.id)) sheets.set(d.id, { ...d.data(), id: d.id });
  const orders = {};
  for (const rid of ids) {
    const mine = [];
    for (const s of sheets.values()) {
      if (s.archived) continue;
      const has = (s.orders || []).map(String).includes(rid) || (s.poolIds || []).some(p => String(p).startsWith(rid + "_"));
      if (!has) continue;
      mine.push({ id: s.id, metal: s.metal || null, metalLabel: s.metalLabel || null, fileBase: s.fileBase || s.folder || null, folder: s.folder || null, sheetIndex: num(s.sheetIndex) || null, page: num(s.page) || null, setId: s.setId || null, setSeq: num(s.setSeq) || null,
        poolIds: (s.poolIds || []).map(String).filter(p => p.startsWith(rid + "_")), poolCount: (s.poolIds || []).length, laserDoneAt: num(s.laserDoneAt) || null, roseCutAt: num(s.roseCutAt) || null, draft: !!s.draft, solidIncluded: s.solidIncluded == null ? null : !!s.solidIncluded,
        status: s.status || null, runId: s.runId || null, updatedAt: ms(s.updatedAt), createdAt: ms(s.createdAt) });
    }
    orders[rid] = { pools: pools.get(rid), sheets: mine };
  }
  return { ok: true, orders, sheets: bySheet };
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
  for (const name of [...SANDBOXED, ...SANDBOX_MAPS]) { const s = await db.collection("Sandbox_" + name).count().get(); counts[name] = s.data().count; }
  return { ok: true, snapshot: doc.exists ? doc.data() : null, records: counts };
}
/* The reset works against a clock: a sandbox that streamed for days holds more than one call can delete. A call deletes
   in pages, a parent only once its subcollections are empty, and answers more:true when its time is up; the page calls
   again until it is done, and nothing is lost between calls. The records go first, then the sandbox's files (every one
   under charmnest/sandbox/ but the snapshot the stream plays and the master files the shared index points to, and the
   archive's design-archive/sandbox/), then the stream. The cache of engraving readings Claude was paid for (AGENT_CACHE)
   is not a record of a replay and stays.
   EVERY family the sandbox can write goes (Paul, 3 Oct: "the same records just keep on coming back"): the sorter's own
   (every SANDBOXED name, so Review's custom orders and custom sheets with their seals, the cancel records and their
   history: production keeps those for good, the sandbox's explicit Reset and Purge clear them), the order timeline, the
   stations' Sandbox_ copies (orders and their messages, locks, finished orders, sign-in sessions, activity and its daily
   rollups, the archive), the engraving jobs and shape guidance, the Rose Gold rehearsals, a person's sandbox decision on
   the shared line readings (decidedSandbox), and the customer messages the sorter's sandbox keeps in the inbox's own
   collection (EtsyMail_OrderLinks: flagged sandbox:true and named olsb_…, which production never makes), and the
   sandbox's own copy of the learned maps (the listing → SKU aliases, option maps and no-design SKUs a person answered in
   the sandbox: SANDBOX_MAPS). Only ever the sandbox's: every collection is named Sandbox_…, and the two shared places
   are cut by the sandbox's own marks. The sandbox's custom design files (charmnest/custom/{order}/…, which the file
   door moves to charmnest/sandbox/custom/…) go with the files below.
   Used by sandboxReset and, after production's own part, by purgeHistory. */
const ORDERLINKS = "EtsyMail_OrderLinks";
async function sandboxWipe(budgetMs) {
  const until = Date.now() + budgetMs, late = () => Date.now() > until;
  let deleted = 0, files = 0;
  const names = ["Brites_Orders", "Design_Completed Orders", "Design_RealTime_Selected_Orders", "Design_Order_Archive", ...SANDBOXED, ...SANDBOX_MAPS, "Order_Timeline", "Charm_Nest_Rose_Rehearsals", SHAPE_CACHE, AGENT, "Station_Sessions", "Station_Activity", "Efficiency_Daily"];   // (the play's timeline events go with the records they tell of)
  const SUBS = { Design_Bridge: ["log"], Brites_Orders: ["messages"], Charm_Nest_Rose_Stock: ["cuts"] };   // deleting a document never deletes its subcollections
  // an order's messages can sit under a Brites_Orders document that was never written (a message posted on its own),
  // which no query of that collection returns: they go with the order's other records, which name it, and last of all
  // with the collection's list of such parents (listDocuments names a document that only holds a subcollection too)
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
  // messages under an order nothing else names (a parent never written): the collection's parents, written or not
  {
    const brites = db.collection("Sandbox_Brites_Orders"), parents = typeof brites.listDocuments === "function" ? await brites.listDocuments() : []; let cursor = 0, incomplete = false;
    await Promise.all(Array.from({ length: Math.min(16, parents.length) }, async () => { while (cursor < parents.length) { if (!(await wipe(parents[cursor++].collection("messages")))) incomplete = true; } }));
    if (incomplete || late()) return more();
  }
  // the sorter's customer messages the sandbox keeps in the inbox's collection: only a document that says sandbox:true AND
  // is named olsb_… (production's engagements are ol_… and say sandbox:false), a page at a time by name
  {
    let after = null;
    for (;;) {
      if (late()) return more();
      const id = admin.firestore.FieldPath.documentId();
      let q = db.collection(ORDERLINKS).where(id, ">=", "olsb_").where(id, "<", "olsb`").orderBy(id).select("sandbox").limit(300);
      if (after) q = q.startAfter(after);
      const s = await q.get(); if (s.empty) break;
      const doomed = s.docs.filter(d => /^olsb_/.test(d.id) && (d.data() || {}).sandbox === true);
      if (doomed.length) { const batch = db.batch(); doomed.forEach(d => batch.delete(d.ref)); await batch.commit(); deleted += doomed.length; }
      after = s.docs[s.docs.length - 1].id; if (s.size < 300) break;
    }
  }
  // the readings of lines are shared with production and stay, and so does production's own decision (decided); the
  // sandbox's decision beside them (decidedSandbox) is the rehearsal's and goes with it, or the replay of the same real
  // order meets its line already decided and its timeline says so
  for (const KINDS = require("./_charmNestCustomRead").KINDS;;) {
    if (late()) return more();
    const s = await db.collection("Charm_Nest_CustomRead").where("decidedSandbox.kind", "in", KINDS).select().limit(300).get(); if (s.empty) break;
    const batch = db.batch(); s.docs.forEach(d => batch.update(d.ref, { decidedSandbox: FV.delete() })); await batch.commit(); deleted += s.size;
    if (s.size < 300) break;
  }
  let filesError = null;
  try {
    const cur = await db.collection(SANDBOX).doc("current").get(), keep = cur.exists ? cur.data().path : null;
    const bucket = admin.storage().bucket();
    for (const prefix of ["charmnest/sandbox/", "design-archive/sandbox/"]) {
      let pageToken;
      do {
        if (late()) return more();
        const [list, next] = await bucket.getFiles({ prefix, autoPaginate: false, maxResults: 500, pageToken });
        const doomed = list.filter(f => f.name !== keep && !f.name.startsWith("charmnest/sandbox/master/")); let cursor = 0;
        await Promise.all(Array.from({ length: Math.min(16, doomed.length) }, async () => { while (cursor < doomed.length) { await doomed[cursor++].delete({ ignoreNotFound: true }); files++; } }));
        pageToken = next && next.pageToken;
      } while (pageToken);
    }
  } catch (e) { console.warn("[charmNestLibrary] sandbox files not deleted:", e.message); filesError = e.message; }
  await db.collection(SANDBOX).doc("stream").delete();   // the order stream starts over with the records it fed
  return { ok: true, more: false, deleted, files, filesError };
}
async function op_sandboxReset() { return sandboxWipe(7000); }
/* ── the sandbox order stream: in place of the whole snapshot at once, the emulated Etsy lists a few of the snapshot's own
   orders per simulated ten minutes, oldest first, under their real Etsy numbers (etsySandbox.js plans them from this seed
   and step). The sorter moves the clock one step per check, and only once it has taken in the last step's orders, so a
   replay at 50x plays a day out in half an hour. Each order comes once: when all of them have come (`total`, the
   snapshot's open orders) the clock stops at that step, so nothing ships by hand while the last ones are worked, and `done`
   tells the sorter to stop hurrying its checks.
   get · ensure (start one for the current snapshot, or resume it) · tick (one step of a playing stream; `expect` is the
   clock the caller last saw, so two tabs never step twice) · off (the whole snapshot again) · reset. Charm_Sandbox only. ── */
const STREAM_STEP_MS = 600000;
// v2: the snapshot's own orders under their real numbers. A stream of the old kind (copies under made-up numbers) is not
// resumed: the next ensure, or the next check's step, starts a new one in its place.
const STREAM_V = 2;
const streamHash = t => { let h = 2166136261; for (let i = 0; i < t.length; i++) h = Math.imul(h ^ t.charCodeAt(i), 16777619); return h >>> 0; };
/** How many orders step k brings: the first draw of the step's own generator, exactly as etsySandbox.js draws it. */
function streamCount(s, k) {
  let a = streamHash(`${s.seed}:${k}`); a = (a + 0x6D2B79F5) | 0;
  let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
  return s.min + Math.floor((((t ^ (t >>> 14)) >>> 0) / 4294967296) * (s.max - s.min + 1));
}
/** How many of the snapshot's orders have come by step k: at most its count. */
function streamBrought(s, k) { const total = +s.total || Infinity; let n = 0; for (let j = 1; j <= k && n < total; j++) n += streamCount(s, j); return Math.min(n, total); }
/** How many of a snapshot's orders the stream brings: its open ones, as etsySandbox.js picks them. Read from the file once
    per snapshot and instance, when a stream starts; the count recorded with the snapshot if the file cannot be read. */
const streamOpen = r => !!r && r.is_paid !== false && r.was_paid !== false && !r.is_shipped && !r.was_shipped && !r.is_canceled && !r.was_canceled && !/cancel/i.test(r.status || "");
const snapshotOpen = new Map();
async function streamTotal(path, recorded) {
  if (!snapshotOpen.has(path)) {
    try { const [buf] = await admin.storage().bucket().file(path).download(), parsed = JSON.parse(buf.toString("utf8")); snapshotOpen.set(path, (Array.isArray(parsed) ? parsed : parsed.receipts || []).filter(streamOpen).length); }
    catch (e) { console.warn("[sandboxStream] snapshot not read, its recorded count stands:", e.message); return recorded; }
  }
  return snapshotOpen.get(path);
}
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
    const mine = !!was && was.snapshotPath === path && was.v === STREAM_V;
    let s = mine ? Object.assign({}, was, { on: true, speed }) : null;
    // only ensure starts or resumes one: a step asked of a stream a reset deleted (a check still out) starts none, but a
    // step asked of a playing stream of the old kind starts the new kind in its place, at step 0
    if (action === "tick" && !(s && was.on) && !(was && was.on && !mine)) return { ok: true, stream: null, advanced: false };
    // a new stream starts at this ten minutes of the real clock; a seed given in Settings replays a recorded one
    if (!s) { const simStart = Math.floor(now / STREAM_STEP_MS) * STREAM_STEP_MS; s = { on: true, v: STREAM_V, seed: Math.floor(num(b.seed)) > 0 ? Math.floor(num(b.seed)) % 2147483647 || 1 : 1 + Math.floor(Math.random() * 2147483646), speed, stepMs: STREAM_STEP_MS, min: 2, max: 5, simStart, simNow: simStart, tick: 0, snapshotPath: path, total: await streamTotal(path, Math.max(0, Math.floor(num(snap.data().count)))), startedAt: now, tickAt: now }; }
    let advanced = false;
    // every order of the snapshot has come by this step: the clock stays here
    const done = !!s.total && streamBrought(s, s.tick) >= s.total;
    if (action === "tick" && mine && !done && (b.expect == null || num(b.expect) === s.simNow)) { s.tick += 1; s.simNow = s.simStart + s.tick * s.stepMs; s.tickAt = now; advanced = true; }
    s.brought = streamBrought(s, s.tick); s.done = !!s.total && s.brought >= s.total;
    if (JSON.stringify(s) !== JSON.stringify(was)) t.set(ref, s);
    return { ok: true, stream: s, advanced };
  });
}
/* ── purge: every RECORD of past runs in production (its history families only: its seals, custom orders and cancel
   records are permanent), and the sandbox COMPLETELY (sandboxWipe: every family, the stream included). Master files, the
   SKU index, aliases, option maps, calibration and the sandbox snapshot are not history and stay. Files in Storage are not touched here: a sheet
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
  const names = [RUNS, RUN_LINES, RUN_LIVE, SHEETS, SETS, POOL, BACK, COUNTERS, RELEASE, BRIDGE];
  const SUBS = { [BRIDGE]: ["log"] };
  const wipe = async q => { let n = 0; for (;;) { const s = await q.limit(300).get(); if (s.empty) break; const batch = db.batch(); s.docs.forEach(d => batch.delete(d.ref)); await batch.commit(); n += s.size; if (s.size < 300) break; } return n; };
  const docs = {};
  // production's own part, exactly as it was: its history families only (never its seals, custom orders or cancel records)
  for (const name of names) {
    const coll = db.collection(name);
    for (const sub of SUBS[name] || []) { const parents = await coll.select().get(); for (const d of parents.docs) await wipe(d.ref.collection(sub)); }
    docs[name] = await wipe(coll);
  }
  /* The sandbox goes completely, on the reset's clock (Paul, 3 Oct: the purge left the sandbox's custom orders, custom
     sheets, cancel records, timeline, arrivals, stations' copies, files… and they came back): the same wipe as "Reset the
     sandbox", the stream included. When its time is up it says so with a 503 the page shows as a failure ("press Purge
     again", more:true in the answer): a purge that has not finished is never reported as done. */
  const sb = await sandboxWipe(7000);
  docs.Sandbox_all = sb.deleted;   // (the page adds the counts up)
  if (sb.more) return { error: `The purge removed ${sb.deleted} sandbox record(s) and is not finished: press Purge again`, more: true, docs, status: 503 };
  return { ok: true, docs, sandbox: { deleted: sb.deleted, files: sb.files, filesError: sb.filesError } };
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
/* A sheet's backs are recorded in one transaction (at most BACKS_PER_TX in each): the sheet, the backs' records and the
   sheets they leave are read once, each back is checked as it always was, in the order sent, and each record and each
   sheet is written once. A transaction per back ran past the function's time limit at about 85 backs, and one refused
   back stopped every back after it. Now a refused back is reported in `errors` (its copy, its sheet and why) and the
   others are still recorded; and one recorded already as it is sent (the same approval on the same sheet, nothing in it
   changed) is not written again, so sending a sheet's backs again costs a read. */
const BACKS_PER_TX = 200;
// a map as Firestore keeps one (not a list, a timestamp or a field value), from whichever context made it
const plainMap = v => !!v && typeof v === "object" && !Array.isArray(v) && (p => p === null || Object.getPrototypeOf(p) === null)(Object.getPrototypeOf(v));
/** Whether two values are the same, lists and maps entry by entry. */
function sameValue(a, b) {
  if (Array.isArray(a) || Array.isArray(b)) return Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((v, i) => sameValue(v, b[i]));
  if (plainMap(a) || plainMap(b)) { if (!plainMap(a) || !plainMap(b)) return false; const k = Object.keys(a); return k.length === Object.keys(b).length && k.every(key => sameValue(a[key], b[key])); }
  return a === b;
}
/** Whether a set with merge of `patch` would leave `stored` as it is: a map merges key by key, anything else (an empty map
    too) replaces. */
const merges = v => plainMap(v) && Object.keys(v).length > 0;
const holds = (stored, patch) => (merges(patch) ? plainMap(stored) && Object.keys(patch).every(k => holds(stored[k], patch[k])) : sameValue(stored, patch));
/** `patch` merged into `into` as a set with merge does it; a map of `into` it changes is copied first. */
function mergeInto(into, patch) { for (const [k, v] of Object.entries(patch)) into[k] = merges(v) && plainMap(into[k]) ? mergeInto(Object.assign({}, into[k]), v) : v; return into; }
/** One transaction's backs, every one for the sheet `sheetId`. Returns how many were written and skipped, the refused
    ones (errors) and the files to archive once it commits. */
async function putBacks(tx, sheetId, list, expected) {
  const target = col(SHEETS).doc(sheetId), ids = [...new Set(list.map(x => x.poolId))];
  const [sheet, ...priors] = await tx.getAll(target, ...ids.map(id => col(BACK).doc(id)));
  const stored = new Map(priors.map((s, i) => [ids[i], s.exists ? s.data() : {}]));
  const formerIds = [...new Set([...stored.values()].map(r => r.sheetId).filter(id => id && id !== sheetId))];
  const formers = new Map(formerIds.length ? (await tx.getAll(...formerIds.map(id => col(SHEETS).doc(id)))).filter(s => s.exists).map(s => [s.id, s.data().backPool || []]) : []);
  const poolIds = sheet.exists ? sheet.data().poolIds || [] : [], patches = new Map(), leaving = new Set(), out = { written: 0, skipped: 0, errors: [], moved: [], sheet: sheet.exists ? sheetLabel(sheet.data()) : "" };
  let pool = sheet.exists ? sheet.data().backPool || [] : null;
  for (const x of list) {
    const old = stored.get(x.poolId), currentBack = pool && pool.find(v => v.poolId === x.poolId);
    const refused = !sheet.exists || !poolIds.includes(x.poolId) ? "The target sheet does not contain this exact charm copy"
      : (old.invalidated && (+old.approvedAt || 0) >= +x.approvedAt) || (+old.approvedAt || 0) > +x.approvedAt || (+old.invalidatedAt || 0) >= +x.approvedAt ? "This approval has been superseded; reopen the engraving"
      : null;
    if (refused) { out.errors.push({ row: x, error: refused }); continue; }
    x.engravingSeals=EngravingSeals.merge(old,x);
    const copy = sheetBack(x);
    // already recorded as sent, and the sheet already lists it so: nothing would change but the time stamps
    // A committed edit retried after its response was lost still carries the prior expectedApprovedAt. Recognize only
    // this exact, already-recorded approval before comparing that old expectation; a different payload still conflicts.
    if (old.invalidated === false && old.sheetId === sheetId && holds(old, x) && sameValue(currentBack, copy)) { out.skipped++; continue; }
    if (expected != null && +expected !== +(currentBack?.approvedAt || old.approvedAt || 0)) { out.errors.push({ row: x, error: "This back was edited elsewhere. Reopen it before saving your changes." }); continue; }
    const record = Object.assign({}, x, {invalidated:false, updatedAt:FV.serverTimestamp()});
    /* A new approval of the same copy replaces the prior one's files. They were approved once, so they are archived, not
       deleted: moved to the archive path once this commits, and the record says where each went (a file whose move
       failed is still at `from`). */
    const next = new Set(["ai", "png"].map(k => x.outputs && x.outputs[k] && x.outputs[k].path).filter(Boolean));
    const moved = old.approvedAt && +old.approvedAt !== +x.approvedAt ? ["ai", "png"].map(k => old.outputs && old.outputs[k] && old.outputs[k].path).filter(p => p && !next.has(p) && /^charmnest\//.test(p)).map(from => ({ from, to: archivePath(from) })) : [];
    if (moved.length) record.superseded = (Array.isArray(old.superseded) ? old.superseded : []).concat([{ approvedAt: +old.approvedAt, approvedBy: old.approvedBy || null, files: moved, at: Date.now() }]).slice(-20);
    // a later row for the same copy is checked against this one, as when each was its own transaction; a copy sent twice is
    // written once, whole, as the two writes would have left it (null)
    patches.set(x.poolId, patches.has(x.poolId) ? null : record); stored.set(x.poolId, mergeInto(Object.assign({}, old), record));
    pool = pool.filter(b => b.poolId !== x.poolId).concat([copy]);
    if (old.sheetId && old.sheetId !== sheetId && formers.has(old.sheetId)) { formers.set(old.sheetId, formers.get(old.sheetId).filter(b => b.poolId !== x.poolId)); leaving.add(old.sheetId); }
    out.moved.push(...moved); out.written++;
  }
  for (const [id, patch] of patches) if (patch) tx.set(col(BACK).doc(id), patch, {merge:true}); else tx.set(col(BACK).doc(id), stored.get(id));
  if (patches.size) tx.set(target, {backPool:pool, updatedAt:FV.serverTimestamp()}, {merge:true});
  for (const id of leaving) tx.set(col(SHEETS).doc(id), {backPool:formers.get(id), updatedAt:FV.serverTimestamp()}, {merge:true});
  return out;
}
async function op_backPut(b) {
  const rows = (Array.isArray(b.backs) ? b.backs : [b.back]).filter(x => x && isPoolId(x.poolId)).slice(0, 400); if (!rows.length) return { error: "no back rows" };
  const bySheet = new Map(), errors = [], moved = [], labels = new Map(); let written = 0, skipped = 0;
  for (const x of rows) {
    if (!isId(x.sheetId) || !x.approvedAt || !x.approvedBy) { errors.push({ row: x, error: "approved back and sheet identity required", identity: true }); continue; }
    if (!bySheet.has(x.sheetId)) bySheet.set(x.sheetId, []); bySheet.get(x.sheetId).push(x);
  }
  for (const [sheetId, list] of bySheet) for (let i = 0; i < list.length; i += BACKS_PER_TX) {
    const part = list.slice(i, i + BACKS_PER_TX); let out;
    try { out = await db.runTransaction(tx => putBacks(tx, sheetId, part, b.expectedApprovedAt)); }
    catch (e) { errors.push(...part.map(x => ({ row: x, error: e.message || String(e) }))); continue; }
    await archiveFiles(out.moved); moved.push(...out.moved); written += out.written; skipped += out.skipped; errors.push(...out.errors); labels.set(sheetId, out.sheet);
  }
  /* Each approved back on its order's timeline, one event a piece (its copy), once per approval: a copy recorded again
     keeps its one event. The Engraved seal (station tracking, Paul 28 Sep 23:51): who approved it, as the sorter's own
     sign-in knows them (approvedBy: an approval without a name is refused above, so it is a signed-in person),
     where (the sorter, its page) and when (the approval, not the save). No employee id: the sorter's login has none. */
  const refused = new Set(errors.map(x => x.row)), device = str(b.device, 40) || "charm-nest-1";
  await stamp(() => rows.filter(x => !refused.has(x)).map(x => {
    // (the caption is one line: the back's lines joined as the seal and the derived history show them)
    const words = str((typeof x.text === "string" ? x.text : Array.isArray(x.lines) ? x.lines.join(" / ") : "").trim().replace(/\s*\n\s*/g, " / "), 180), by = str(x.approvedBy, 80).trim();
    return { orderId: String(x.order || orderOfKey(x.poolId)), type: "engraveApproved", at: num(x.approvedAt), by, station: "sorter", device, lineKey: lineOfCopy(x.poolId), transactionId: str(x.transactionId || String(x.poolId).split("_")[1], 30),
      sheetId: x.sheetId, sheet: labels.get(x.sheetId) || "", setId: str(x.setId, 100), text: words ? `“${words}”` : "", data: { text: str(x.text, 400), poolId: x.poolId, copy: num(x.copy) || null, sku: str(x.sku, 60), signedIn: !!by }, id: `${x.poolId}-${num(x.approvedAt)}` };
  }), "engraving approved");
  const done = { count: rows.length, written, skipped, superseded: moved.length };
  if (!errors.length) return Object.assign({ ok: true }, done);
  // what went wrong, back by back in the order sent; the rest were recorded. Refused as before: 400 when a back said
  // nothing of whose it is, 500 otherwise
  const order = new Map(rows.map((x, i) => [x, i])); errors.sort((p, q) => order.get(p.row) - order.get(q.row));
  return Object.assign({ error: errors[0].error + (errors.length > 1 ? ` (and ${errors.length - 1} more of the ${rows.length} backs)` : ""), status: errors.every(x => x.identity) ? 400 : 500,
    errors: errors.map(x => ({ poolId: x.row.poolId, sheetId: str(x.row.sheetId, 80) || null, error: x.error })) }, done);
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
    const old=await tx.get(ref),patch=withoutCleared(old.exists?old.data():null,b.patch || {}),next={...(old.exists?old.data():{}),...patch};
    if(/^complete/.test(next.status || '')) {
      const ids=[...new Set(next.sheetIds || [])];
      if(!ids.length)throw new Error('A set without sheets is not ready for laser');
      const docs=[];for(const sheetId of ids)docs.push(await tx.get(col(SHEETS).doc(sheetId)));
      // (a person's hold back from Laser cutting is about the laser, not about whether this set may be recorded complete)
      const records=docs.filter(d=>d.exists).map(d=>({...d.data(),id:d.id,laserHold:null}));
      await productionReadiness(records,{tx});
      if(records.some(s=>s.setId!==id) || !Readiness.set(next,records).ready)throw new Error('Set cannot be completed: every sheet needs approved engraving, verified back files, front files and QR labels');
      // the cardinal rule (charm-nest-shared-orders.js): a set is completed only with every sheet that shares a multi-piece
      // order with one of its own, unless that sheet can no longer join (cut, or in a set already sent to the station)
      if(!(old.exists && /^complete/.test(String(old.data().status || '')))){
        const split=await splitOfSet(id,ids);
        if(split.length){
          const there=[...new Set(split.flatMap(it=>it.there))];
          throw new Error(`Set cannot be completed: ${split.length===1?'order '+split[0].orderId+' also has pieces':split.length+' orders ('+split.slice(0,4).map(it=>it.orderId).join(', ')+(split.length>4?', ...':'')+') also have pieces'} on ${there.join(', ')}, which ${there.length===1?'is':'are'} not in this set. Sheets that share a multi-piece order stay in the same set: put them in one set, or take the order off one of the sheets.`);
        }
      }
    }
    // each field the patch names replaces the stored one whole, and a field it leaves out stays as it was: a set with merge
    // merged a map into the stored one key by key, so an order taken off a set stayed on its record for good
    const doc=Object.assign({}, patch,{setId:id,updatedAt:FV.serverTimestamp()});
    delete doc.laserDoneAt;delete doc.laserDoneBy;delete doc.processSeals;delete doc.processReady;   // process records are server-owned
    delete doc.flowHistory;
    if(old.exists)tx.update(ref,doc);else tx.set(ref,doc);
  });
  return { ok: true };
}
/* Copies a cleanup took off a set's sheet on purpose (the set's `cleanup`, rg-cleanup-2026-09-29) stay off the set when a
   page that still lists them saves its orders. Nothing else in the patch changes. */
function withoutCleared(set, patch) {
  const gone = new Set(set && set.cleanup && Array.isArray(set.cleanup.removedPoolIds) ? set.cleanup.removedPoolIds : []);
  if (!gone.size || !patch || !patch.orders || typeof patch.orders !== "object") return patch;
  const orders = {};
  for (const [k, o] of Object.entries(patch.orders)) orders[k] = o && Array.isArray(o.lines) ? Object.assign({}, o, { lines: o.lines.map(l => (l && Array.isArray(l.copies) ? Object.assign({}, l, { copies: l.copies.filter(c => !(c && gone.has(c.poolId))) }) : l)) }) : o;
  return Object.assign({}, patch, { orders });
}
async function op_setGet(b) { const id = str(b.setId, 80); if (!isId(id)) return { error: "bad set id" }; const s = await col(SETS).doc(id).get(); if (!s.exists) return { set: null }; const d = s.data(); d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt); d.committedAt = ms(d.committedAt) || d.committedAt || null; return { set: d }; }
async function op_setList(b) {
  // Completion can occur days after allocation. Filter and sort before limiting;
  // legacy sets keep their original saved day until an actual completion exists.
  // Only the most recently saved sets are read (completion saves the set again, so a newly completed one is among them),
  // never every set on record; given `from`, none saved a week before it. The sandbox's days run on a simulated clock, so
  // there the bound is only the count.
  // The answer comes in parts (ANSWER_BYTES): the sets first, then their sheets, read for what their list entries show
  // (SLIM_SHEET) where every sheet used to be read whole; `next` names the sets and sheets left, in order, and a later part
  // reads those by id.
  const setRow = d => { const r=d.data(); r.setId ||= d.id; for(const k of ["updatedAt","createdAt","completedAt","committedAt"]) r[k]=ms(r[k]); return [d.id, r]; };
  const budget = answerBudget(), cur = b.cursor && typeof b.cursor === "object" ? b.cursor : null;
  let rows = [], sheetIds = [];
  if (cur) {
    const ids = (Array.isArray(cur.sets) ? cur.sets : []).filter(isId).slice(0, 500);
    for (let i = 0; i < ids.length; i += 100) for (const d of await db.getAll(...ids.slice(i, i + 100).map(id => col(SETS).doc(id)))) if (d.exists) rows.push(setRow(d));
    sheetIds = (Array.isArray(cur.sheets) ? cur.sheets : []).filter(isId).slice(0, 20000);
  } else {
    const limit = Math.min(500, num(b.limit) || 200);
    let q = col(SETS).orderBy("updatedAt", "desc");
    if (isDay(b.from) && !PREFIX) q = q.where("updatedAt", ">=", new Date(Date.parse(b.from + "T00:00:00Z") - 7 * 86400000));
    const snap = await q.limit(Math.min(1000, 3 * limit)).get();
    rows = snap.docs.map(setRow);
    rows=rows.filter(([,r])=>(!isDay(b.from) || OrderRules.completionDay(r)>=b.from)&&(!isDay(b.to) || OrderRules.completionDay(r)<=b.to)&&(!b.status || r.status===b.status)&&!(b.excludeDone && num(r.laserDoneAt)>0)).sort(([,x],[,y])=>OrderRules.compareCompleted(x,y)).slice(0,limit);
    if (b.includeSheets) sheetIds = [...new Set(rows.flatMap(([,r])=>r.sheetIds || []))].filter(isId);
  }
  let n = 0; while (n < rows.length && budget.fits(rows[n][1])) n++;
  const part = b.includeSheets && n === rows.length ? await sheetEntries(sheetIds.map(id => [id]), budget) : { sheets: [], rest: sheetIds };
  const next = n < rows.length || part.rest.length ? { sets: rows.slice(n).map(([id]) => id), sheets: part.rest } : null;
  return {sets:rows.slice(0, n).map(([,r])=>r), ...(b.includeSheets ? {sheets:part.sheets} : {}), next, truncated: !!next};
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
  const done = await db.runTransaction(async tx => {
    const ref = col(SHEETS).doc(b.id), sheet = await tx.get(ref);
    if (!sheet.exists || sheet.data().runId !== b.runId) return { error: "sheet does not belong to this run" };
    const run = await tx.get(col(RUNS).doc(b.runId));
    if (!run.exists || ["complete", "abandoned"].includes(run.data().status)) return { error: "finished sheets cannot be changed by intake" };
    if (!sheet.data().roseCutAt && (sheet.data().rosePlanJson || sheet.data().roseProtectedJson)) throw new Error('A planned or protected Rose Gold contour cannot be archived');
    // its files go only if it was never cut or released: a cut Rose Gold contour or a sheet of a committed set keeps them
    const d = sheet.data(), set = isId(d.setId) ? await tx.get(col(SETS).doc(d.setId)) : null, keep = !!d.roseCutAt || !!(set && set.exists && set.data().committedAt);
    // a laser mark is kept beside it (archivedLaserDoneAt/By): an archived sheet is in no list, nor in the Completed count
    tx.set(ref, Object.assign({ archived: true, archivedReason: "open sheets repacked", updatedAt: FV.serverTimestamp() }, keep ? {} : { outputs: null }, num(d.laserDoneAt) > 0 ? archivedMark(d) : {}), { merge: true });
    return { ok: true, files: keep ? [] : outputPaths(d) };
  });
  if (!done.ok) return done;
  /* The repacked sheet was never cut (a finished run is refused above): its own five files go with it. Its back files and
     labels stay, as their records still name them. A file another live sheet record also names is left alone. */
  const ai = done.files.find(p => /\.ai$/.test(p));
  let shared = true;
  try { shared = !!ai && (await col(SHEETS).where("outputs.ai.path", "==", ai).limit(5).get()).docs.some(d => d.id !== b.id && !d.data().archived); } catch (_) { shared = true; }
  if (!shared) { const bucket = admin.storage().bucket(); await Promise.all(done.files.map(p => bucket.file(p).delete().catch(() => {}))); }
  return { ok: true, deletedFiles: shared ? 0 : done.files.length };
}
// ── arrival ledger, separate in production and sandbox ──
async function op_arrivalRecord(b) {
  const receipts = [...new Map((b.orders || []).filter(o => /^\d{1,30}$/.test(String(o.id))).map(o => [String(o.id), o])).values()];
  if (receipts.length > 5000) return { error: "too many receipts" };
  // the sandbox order stream stamps first arrivals with its simulated clock, so its 24 h and 1 h counts read in it
  const now = [true, 1, "1"].includes(b.sandbox) && num(b.now) > 0 ? num(b.now) : Date.now(), collection = col("Charm_Nest_Arrivals"), firstSeen = {};
  // the counts read the last 24 hours; a TTL policy on expireAt removes an arrival after 180 days (the sandbox's after 3),
  // on the real clock, which the sandbox's simulated one runs ahead of
  const expireAt = new Date(Date.now() + ([true, 1, "1"].includes(b.sandbox) ? 3 : 180) * 86400000);
  // Existing receipts need one batched read; transact only first arrivals.
  const known = receipts.length ? await db.getAll(...receipts.map(o => collection.doc(String(o.id)))) : [];
  const missing = receipts.filter((o, i) => { if (!known[i].exists) return true; firstSeen[String(o.id)] = known[i].data().firstSeenAt; return false; });
  // Bound concurrency; transactions keep simultaneous stations from counting an order twice.
  let cursor = 0; const arrived = [];
  await Promise.all(Array.from({ length: Math.min(8, missing.length) }, async () => {
    while (cursor < missing.length) {
      const o = missing[cursor++], id = String(o.id), ref = collection.doc(id); let made = false;
      firstSeen[id] = await db.runTransaction(async t => {
        made = false; const old = await t.get(ref); if (old.exists) return old.data().firstSeenAt;
        t.set(ref, { id, firstSeenAt: now, seenAt: Date.now(), createTs: num(o.createTs), expireAt }); made = true; return now;
      });
      if (made) arrived.push(o);
    }
  }));
  // an order's first arrival is the first milestone of its timeline (once: the ledger is written for it once). Stamped on
  // the real clock, as every other event of the timeline is (seenAt above too): the sandbox stream's simulated moment
  // (firstSeenAt, for the counts) is kept beside it, since the two clocks cannot be compared (_orderTimeline chronology)
  const sim = [true, 1, "1"].includes(b.sandbox) && num(b.now) > 0, real = sim ? Date.now() : now;
  await stamp(() => arrived.map(o => ({ orderId: String(o.id), type: "arrived", at: real, by: "System", station: "sorter", text: "First seen by the sorter",
    data: Object.assign({ firstSeenAt: now, createTs: num(o.createTs) || null, clock: "real" }, sim ? { simAt: now } : {}), milestone: true, id: "first" })), "arrivals");
  const [day,hour]=await Promise.all([collection.where("firstSeenAt",">=",now-86400000).count().get(),collection.where("firstSeenAt",">=",now-3600000).count().get()]);
  // what the counts no longer read goes, a page per check: the sandbox stream's records first seen over 45 simulated days
  // ago (it plays days in hours; far past any order it still lists, and as long as the sorter keeps its own), production's
  // over 180 days ago (a TTL policy on expireAt, if one is switched on, does the same)
  await collection.where("firstSeenAt", "<", now - ([true, 1, "1"].includes(b.sandbox) ? 45 : 180) * 86400000).limit(200).get().then(s => { if (s.empty) return; const batch = db.batch(); s.docs.forEach(d => batch.delete(d.ref)); return batch.commit(); }).catch(e => console.warn("[charmNestLibrary] old arrivals not deleted:", e.message));
  return {ok:true,firstSeen,count24:day.data().count,count1:hour.data().count,at:now};
}
// ── runs ──
// The page keeps a run's record small (RunCtl.save): the lines of the orders it is done with go to its line archive, and
// those still in progress are kept beside it (liveLines). A record that still nears a document's limits (1 MiB, 40,000
// index entries) is said once per function instance in the log, in words, before the day it can no longer be saved.
const RUN_DOC_BYTES = 1048576, RUN_DOC_ENTRIES = 40000, runSizeWarned = new Set();
async function op_runPut(b) {
  const r = b.run || {}; const id = str(r.runId, 80); if (!isId(id)) return { error: "bad run id" };
  const doc = Object.assign({}, r, { runId: id, updatedAt: FV.serverTimestamp() }); delete doc.liveLines;
  const parts = r.lines && typeof r.lines === "object" && !Array.isArray(r.lines) ? liveParts(id, r.lines) : null;
  if (parts && parts.some(p => p.bytes > 1000000)) return { error: "a piece of this run is too large to save" };
  if (parts) { delete doc.lines; doc.liveLines = { ids: parts.map(p => p.id), lines: parts.reduce((n, p) => n + p.lines, 0), bytes: parts.reduce((n, p) => n + p.bytes, 0) }; }
  const plain = Object.assign({}, doc, { updatedAt: 0 }), bytes = Buffer.byteLength(JSON.stringify(plain)), entries = OrderRules.indexEntries(plain);   // measured without the server's time mark
  if (Math.max(bytes / RUN_DOC_BYTES, entries / RUN_DOC_ENTRIES) > 0.7 && !runSizeWarned.has(PREFIX + id)) { runSizeWarned.add(PREFIX + id); console.warn(`[charmNestLibrary] run ${id}${PREFIX ? " (sandbox)" : ""}: its record is ${Math.round(bytes / 1024)} KB of the 1,024 KB and ${entries} of the 40,000 index entries one Firestore document can hold. A full record cannot be saved, and the run stops.`); }
  const ref = col(RUNS).doc(id);
  await db.runTransaction(async tx => {
    const ex = await tx.get(ref), old = ex.exists ? ex.data() : null, put = Object.assign({}, doc);
    put.createdAt = old ? (old.createdAt || FV.serverTimestamp()) : FV.serverTimestamp();
    const had = old && old.liveLines && Array.isArray(old.liveLines.ids) ? old.liveLines.ids : [];
    if (parts) {
      // a part is named by its content: one the record already lists is there as it is
      const keep = new Set(put.liveLines.ids), have = new Set(had);
      for (const p of parts) if (!have.has(p.id)) tx.set(col(RUN_LIVE).doc(p.id), { runId: id, json: p.json, bytes: p.bytes, lines: p.lines, seq: p.seq, createdAt: FV.serverTimestamp() });
      for (const x of had) if (!keep.has(x)) tx.delete(col(RUN_LIVE).doc(x));
      if (b.merge) put.lines = FV.delete();
    } else if (!b.merge && had.length) put.liveLines = old.liveLines;   // a whole record sent without its lines keeps those it had
    tx.set(ref, put, { merge: !!b.merge });
  });
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
    if (!lines || typeof lines !== "object" || Array.isArray(lines) || !Object.keys(lines).length) return { error: "an archive part holds no pieces" };
    const keys = Object.keys(lines), orders = [...new Set(Object.values(lines).map(l => String((l && l.orderId) ?? "")).filter(Boolean))];
    const decisions = JSON.stringify(decisionsByLine(lines));
    if (bytes + Buffer.byteLength(decisions) + Buffer.byteLength(JSON.stringify(keys.concat(orders))) > 1000000) return { error: "an archive part is too large to keep with its lists; send fewer pieces in it" };
    const digest = require("crypto").createHash("sha256").update(json).digest("hex").slice(0, 40);
    docs.push([col(RUN_LINES).doc(`${id}~${digest}`), { runId: id, lines: keys.length, bytes, json, keys, orders, decisions, decisionsVersion: DECISIONS_VERSION, at, seq: i, createdAt: FV.serverTimestamp() }]);
  }
  const batch = db.batch(); for (const [ref, doc] of docs) batch.set(ref, doc); await batch.commit();
  return { ok: true, parts: docs.length, lines: docs.reduce((n, [, d]) => n + d.lines, 0) };
}
async function op_runGet(b) {
  const id = str(b.runId, 80); if (!isId(id)) return { error: "bad run id" };
  const s = await col(RUNS).doc(id).get(); if (!s.exists) return { run: null };
  // the lines of the orders still in progress, kept beside the record, come back in it as they always did
  const d = await withLiveLines(id, s.data()); if (!d) return { run: null };
  delete d.liveLines; d.updatedAt = ms(d.updatedAt); d.createdAt = ms(d.createdAt);
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
  let rows = snap.docs.map(d => { const r = d.data(), out = r.lineArchive || {}; return { runId: r.runId, setId: r.setId || null, day: r.day, step: r.step, status: r.status, mode: r.mode || null, lines: (r.lines ? Object.keys(r.lines).length : num((r.liveLines || {}).lines)) + num(out.lines), holds: (r.holds ? Object.keys(r.holds).length : 0) + num(out.held), errors: (r.errors || []).length, updatedAt: ms(r.updatedAt), createdAt: ms(r.createdAt), stoppedBy: r.stoppedBy || null }; });
  if (b.status) rows = rows.filter(r => r.status === b.status);
  return { runs: rows };
}
/* "Find that order from last Tuesday." Firestore has no full-text search, and a shop's history grows every day, so no
   answer reads all of it. Groups (sets, working and standalone sheets, runs that wrote none) are read a day at a time,
   newest first:
   · a listing reads the newest days that hold a page of groups, each set with its whole membership, and never the line
     archive;
   · a search reads the days of one window (30 by default, `days` up to 90, the page's `today` its newest) and the line
     archive of the runs in it, newest part first, 16 MB of it at most; an order number is also looked up exactly, however
     old, in the order lists sheets, runs and archive parts keep;
   · `next` is where the following page starts: the groups on or before its day, after the first `skip` of that day;
   · a page is `limit` groups, fewer when their sheets would pass ANSWER_BYTES (truncated.size), each sheet in its group.
   The answer says what it read (scanned), the days it covered (window) and whether a cap cut it short (truncated).
   Every query is a single-field equality, range or array-contains: no composite index is needed. */
const HISTORY_CAP = { sets: 300, sheets: 600, runs: 300, parts: 3000 }, HISTORY_PART_BYTES = 16000000;
const HISTORY_SET = ["seq", "day", "runId", "status", "updatedAt", "materials", "orders", "laserDoneAt", "laserDoneBy", "processSeals", "processReady", "archivedLaserDoneAt", "archivedLaserDoneBy", "updatedAt", "activityAt"];
// (label: the Sets window says of each sheet whether its QR label is made — Paul, 27 Sep: "add all the appropriate
// functionality to this pop-up"; the label record is a few small entries, not the sheet's charms)
const HISTORY_SHEET = ["id", "setId", "setSeq", "runId", "day", "metal", "metalLabel", "status", "orders", "sheetIndex", "page", "updatedAt", "archived", "folder", "fileBase", "saving", "draft", "releaseFull", "endedBy", "charmCount", "placedCount", "rejectCount", "density", "freePt2", "verification", "outputs", "names", "poolIds", "backPool", "solidIncluded", "sources", "stock", "laserDoneAt", "laserDoneBy", "listings", "label", "cardStartedAt", "createdAt", "processSeals", "processReady", "archivedLaserDoneAt", "archivedLaserDoneBy", "updatedAt", "activityAt"];
const HISTORY_RUN = ["runId", "setId", "seq", "day", "status", "step", "sheets", "lineArchive", "liveLines", "errors", "stoppedBy", "orders", "createdAt", "updatedAt"];
const dayOf = x => (isDay(x && x.day) ? x.day : "");
const dayShift = (day, n) => { const d = new Date(day + "T12:00:00Z"); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
/** Newest first by day: a range (from, to inclusive) or one day (eq); records without a day are in none. */
function byDay(name, fields, { from = null, to = null, eq = null, n }) {
  let q = eq ? col(name).where("day", "==", eq) : col(name).where("day", from ? ">=" : ">", from || "");
  if (!eq && to) q = q.where("day", "<=", to);
  if (!eq) q = q.orderBy("day", "desc");
  return q.limit(n).select(...fields).get();
}
/** The newest day on record before a day (or at all), over sets, runs and sheets. */
async function newestDay(before = null) {
  const days = await Promise.all([SETS, RUNS, SHEETS].map(async name => { const s = await col(name).where("day", before ? "<" : ">", before || "").orderBy("day", "desc").limit(1).select("day").get(); return s.size ? dayOf(s.docs[0].data()) : ""; }));
  return days.filter(Boolean).sort().pop() || null;
}
async function whereIn(name, field, values, fields, cap) {
  const chunks = []; for (let i = 0; i < values.length; i += 30) chunks.push(values.slice(i, i + 30));
  return (await Promise.all(chunks.map(c => col(name).where(field, "in", c).limit(cap).select(...fields).get()))).flatMap(s => s.docs);
}
function historyRows(q, sets, sheets, runs) {
  const groups = new Map([...sets].map(([id, x]) => ["set:" + id, { key: "set:" + id, name: "Set " + x.seq, setId: id, seq: x.seq, day: x.day, runId: x.runId, status: x.status, updatedAt: ms(x.updatedAt), sheets: [], materials: x.materials || [], orderIds: Object.keys(x.orders || {}), search: [], exact: !!x.exact, laserDoneAt: num(x.laserDoneAt) || null, laserDoneBy: x.laserDoneBy || null }]));
  for (const x of sheets.values()) {
    if (x.archived) continue;
    const meta = OrderRules.libraryGroup(x), key = meta.key;
    if (!groups.has(key)) groups.set(key, { ...meta, day: x.day, runId: x.runId, status: meta.standalone ? "standalone" : meta.working ? "held" : x.status, draft: meta.working, sheets: [], materials: [], orderIds: [], search: [] });
    const g = groups.get(key); g.sheets.push(Object.assign(slim(x), { orders: (x.orders || []).length, orderIds: x.orders || [], sheetIndex: x.sheetIndex || x.page || 1 }));
    if (!g.materials.includes(x.metal)) g.materials.push(x.metal);
    g.search.push(x.names || "", x.metalLabel || "", ...(x.charms || []).map(c => c.sku || ""));
    g.orderIds.push(...(x.orders || [])); g.updatedAt = Math.max(g.updatedAt || 0, ms(x.updatedAt) || 0); if (x.exact) g.exact = true;
  }
  const hits = (r, k) => [...new Set(Object.values(r.lines || {}).map(l => String((l && l[k]) || "")))].filter(x => q && x.toLowerCase().includes(q));
  const runRows = [...runs.values()].map(r => ({ runId: r.runId, setId: r.setId || null, seq: r.seq || null, day: r.day, status: r.status, step: r.step, lines: (r.recordLines || r.lines ? Object.keys(r.recordLines || r.lines).length : num((r.liveLines || {}).lines)) + num((r.lineArchive || {}).lines), orders: new Set(Object.values(r.lines || {}).map(l => l && l.orderId)).size, sheets: Object.keys(r.sheets || {}).length + num((r.lineArchive || {}).sheets), updatedAt: ms(r.updatedAt), createdAt: ms(r.createdAt), stoppedBy: r.stoppedBy || null, hitOrders: hits(r, "orderId"), hitSkus: hits(r, "sku"), exact: !!r.exact }));
  const named = new Set([...groups.values()].map(g => g.runId).filter(Boolean));
  for (const r of runRows) if (!named.has(r.runId)) groups.set("run:" + r.runId, { key: "run:" + r.runId, setId: r.setId, seq: r.seq, runId: r.runId, day: r.day, status: r.status, sheets: [], materials: [], orderIds: [], orders: r.orders, updatedAt: r.updatedAt, exact: r.exact });
  const rows = [...groups.values()].map(g => {
    const r = runs.get(g.runId), ids = new Set(g.orderIds.map(String));
    const lines = Object.values(r?.lines || {}).filter(l => l && (!ids.size || ids.has(String(l.orderId))));
    const hay = [g.setId, "Set " + g.seq, g.day, g.runId, g.status, r?.stoppedBy, ...(r?.errors || []).map(e => e && e.why), ...g.materials, ...(g.search || []), ...ids, ...lines.flatMap(l => [l.orderId, l.sku, l.engrave?.text, l.snap?.title]), ...g.sheets.map(x => x.fileBase)].join(" ").toLowerCase();
    return Object.assign(g, { orders: ids.size || g.orders || new Set(lines.map(l => l.orderId)).size, status: g.standalone ? "standalone" : g.draft ? "held" : g.status === "superseded" ? g.status : r?.status === "complete" ? (String(g.status).startsWith("complete") ? g.status : "complete") : r?.status || g.status, match: !q || hay.includes(q) });
  }).filter(g => g.match).sort((a, b) => String(b.day || "").localeCompare(String(a.day || "")) || (b.seq || 0) - (a.seq || 0) || (b.updatedAt || 0) - (a.updatedAt || 0) || String(a.key).localeCompare(String(b.key)));
  return { rows, runRows };
}
async function activityHistory(b) {
  const omit=new Set(['outputs','backPool','sources','poolIds','verification','stock','sheets','processSeals']);
  const fields=[HISTORY_SET,HISTORY_SHEET,HISTORY_RUN].map((xs,i)=>xs.filter(x=>!omit.has(x) && !(i===0 && x==='orders')));
  const snaps=await Promise.all([SETS,SHEETS,RUNS].map((name,i)=>col(name).select(...fields[i]).get()));
  const maps=snaps.map((snap,i)=>new Map(snap.docs.map(d=>[d.id,{...d.data(),...(i===1?{id:d.id}:i===2?{runId:d.id}:{})}])));
  const q=String(b.q||'').trim().toLowerCase(), direction=b.direction==='asc'?'asc':'desc';
  const {rows:all,runRows}=historyRows(q,...maps);
  const rows=all.filter(r=>Activity.matches(r,b.range)).sort((a,b)=>Activity.compare(a,b,direction));
  const cur=b.cursor?.activity ? b.cursor:null;
  const after=cur?rows.filter(r=>Activity.compare(r,{key:cur.key,activityAt:cur.at},direction)>0):rows;
  const limit=Math.min(100,Math.max(1,num(b.limit)||60)), chosen=after.slice(0,limit), budget=answerBudget(), page=[];
  const ids=[...new Set(chosen.flatMap(g=>g.sheets.map(x=>x.id)))].filter(isId), full=new Map();
  for(let i=0;i<ids.length;i+=100)for(const d of await db.getAll(...ids.slice(i,i+100).map(id=>col(SHEETS).doc(id)),{fieldMask:HISTORY_SHEET}))if(d.exists)full.set(d.id,{...d.data(),id:d.id});
  for(const g of chosen){g.sheets=g.sheets.map(x=>full.has(x.id)?{...slim(full.get(x.id)),orders:x.orders,orderIds:x.orderIds,sheetIndex:x.sheetIndex}:x);delete g.search;delete g.match;delete g.exact;if(!budget.fits(g))break;page.push(g);}
  const last=page[page.length-1], onPage=new Set(page.map(g=>g.runId));
  return {sets:page,runs:runRows.filter(r=>onPage.has(r.runId)),next:after.length>page.length && last?{activity:true,at:Activity.at(last),key:last.key}:null,total:rows.length,scanned:{sets:snaps[0].size,sheets:snaps[1].size,runs:snaps[2].size},truncated:{size:page.length<chosen.length}};
}
async function op_history(b) {
  if(b.sort==="activity" && /^\d*$/.test(String(b.q || "").trim()))return activityHistory(b);
  const q = String(b.q || "").trim().toLowerCase(), limit = Math.min(100, Math.max(1, num(b.limit) || 60));
  const cur = b.cursor && isDay(b.cursor.day) ? { day: b.cursor.day, skip: Math.max(0, Math.floor(num(b.cursor.skip))) } : null;
  const scanned = { runs: 0, sheets: 0, sets: 0, lineParts: 0 }, truncated = { runs: false, sheets: false, sets: false, lineParts: false, size: false };
  const sets = new Map(), sheets = new Map(), runs = new Map(), parts = new Map();
  const add = (kind, docs, extra) => { scanned[kind] += docs.length; const map = { sets, sheets, runs }[kind]; for (const d of docs) if (!map.has(d.id)) map.set(d.id, Object.assign(d.data(), extra, kind === "runs" ? { runId: d.data().runId || d.id } : {})); };
  const sheetFields = q ? HISTORY_SHEET.concat(["charms"]) : HISTORY_SHEET, runFields = q ? HISTORY_RUN.concat(["lines"]) : HISTORY_RUN;
  const hi = cur ? cur.day : null; let lo = null;
  if (!q) {
    // the newest (skip + limit) of each kind on or before the cursor's day; every day after the newest day a full read
    // stopped at is then read whole, and that day too
    const n = (cur ? cur.skip : 0) + limit, want = [["sets", SETS, HISTORY_SET, n + 1], ["runs", RUNS, runFields, n + 1], ["sheets", SHEETS, sheetFields, Math.min(HISTORY_CAP.sheets, 4 * n + 20)]];
    const got = await Promise.all(want.map(([, name, fields, cap]) => byDay(name, fields, { to: hi, n: cap })));
    const stops = want.map((w, i) => (got[i].size >= w[3] ? dayOf(got[i].docs[got[i].size - 1].data()) || null : null));
    lo = stops.filter(Boolean).sort().pop() || null;
    got.forEach((s, i) => add(want[i][0], s.docs));
    if (lo) await Promise.all(want.map(async ([kind, name, fields], i) => { if (stops[i] !== lo) return; const s = await byDay(name, fields, { eq: lo, n: HISTORY_CAP[kind] }); if (s.size >= HISTORY_CAP[kind]) truncated[kind] = true; add(kind, s.docs); }));
  } else {
    const span = Math.min(90, Math.max(1, Math.floor(num(b.days)) || 30)), top = hi || (isDay(b.today) ? b.today : await newestDay());
    if (top) {
      lo = dayShift(top, -(span - 1));
      const want = [["sets", SETS, HISTORY_SET], ["runs", RUNS, runFields], ["sheets", SHEETS, sheetFields]];
      const got = await Promise.all(want.map(([kind, name, fields]) => byDay(name, fields, { from: lo, to: hi, n: HISTORY_CAP[kind] })));
      got.forEach((s, i) => { if (s.size >= HISTORY_CAP[want[i][0]]) truncated[want[i][0]] = true; add(want[i][0], s.docs); });
    }
    // an order number, however old: the sheets and runs that list it, and the archive parts that hold its lines
    if (!cur && /^\d{4,20}$/.test(q)) {
      const ids = [q].concat(Number.isSafeInteger(+q) ? [+q] : []), has = x => (x.orders || []).map(String).includes(q);
      const [ps, xs, rs] = await Promise.all([col(RUN_LINES).where("orders", "array-contains", q).limit(20).select("runId", "json", "at", "seq", "orders").get(), col(SHEETS).where("orders", "array-contains-any", ids).limit(HISTORY_CAP.sheets).select(...sheetFields).get(), col(RUNS).where("orders", "array-contains-any", ids).limit(50).select(...runFields).get()]);
      const found = ps.docs.filter(d => has(d.data()));
      scanned.lineParts += found.length; for (const d of found) parts.set(d.id, { id: d.id, ...d.data() });
      add("sheets", xs.docs.filter(d => has(d.data())), { exact: true }); add("runs", rs.docs.filter(d => has(d.data())), { exact: true });
      const more = [...new Set(found.map(d => d.data().runId))].filter(id => isId(id) && !runs.has(id));
      if (more.length) add("runs", await whereIn(RUNS, "runId", more, runFields, 50), { exact: true });
    }
  }
  // each set a sheet here belongs to, with its whole membership, and each run a set or sheet here belongs to: for the
  // days this answer covers (and an exact order-number match wherever it is), not for the older ones a read ran into
  const shown = x => x.exact || !lo || String(x.day || "") >= lo;
  const setIds = [...new Set([...sheets.values()].filter(x => !x.archived && shown(x) && OrderRules.libraryGroup(x).setId).map(x => x.setId))].filter(id => isId(id) && !sets.has(id));
  for (let i = 0; i < setIds.length; i += 100) add("sets", (await db.getAll(...setIds.slice(i, i + 100).map(id => col(SETS).doc(id)))).filter(d => d.exists));
  for (const x of sheets.values()) if (x.exact && x.setId && sets.has(x.setId)) sets.get(x.setId).exact = true;
  const members = [...sets.entries()].filter(([id, x]) => isId(id) && shown(x)).map(([id]) => id);
  if (members.length) add("sheets", await whereIn(SHEETS, "setId", members, sheetFields, HISTORY_CAP.sheets));
  const runIds = [...new Set([...sets.values(), ...sheets.values()].filter(shown).map(x => x.runId))].filter(id => isId(id) && !runs.has(id));
  if (runIds.length) add("runs", await whereIn(RUNS, "runId", runIds, runFields, HISTORY_CAP.runs));
  const named = new Set([...sets.values(), ...sheets.values()].map(x => x.runId).filter(Boolean));
  if (!q) {
    // a listing counts the lines of the runs a person can resume or that wrote no sheet: their own records' lines
    const open = [...runs.values()].filter(r => !["complete", "abandoned", "superseded"].includes(r.status) || !named.has(r.runId)).map(r => r.runId).filter(isId);
    for (const d of await whereIn(RUNS, "runId", open, ["runId", "lines"], HISTORY_CAP.runs)) { const r = runs.get(d.data().runId || d.id); if (r && d.data().lines) r.lines = d.data().lines; }
    await eachWithLiveLines(open.map(id => runs.get(id)));
  } else {
    // a search reads the lines in progress of the runs in its answer, and their line archive, newest part first, up to
    // HISTORY_PART_BYTES
    await eachWithLiveLines([...runs.values()]);
    const archived = [...runs.values()].filter(r => r.lineArchive && num(r.lineArchive.parts) > 0).map(r => r.runId).filter(isId), listed = [];
    for (const d of await whereIn(RUN_LINES, "runId", archived, ["runId", "bytes", "at", "seq"], HISTORY_CAP.parts)) listed.push({ id: d.id, ...d.data() });
    if (listed.length >= HISTORY_CAP.parts) truncated.lineParts = true;
    listed.sort((x, y) => num(y.at) - num(x.at) || num(y.seq) - num(x.seq));
    let bytes = 0; const chosen = [];
    for (const p of listed) { if (parts.has(p.id)) continue; if (chosen.length && bytes + num(p.bytes) > HISTORY_PART_BYTES) { truncated.lineParts = true; break; } bytes += num(p.bytes); chosen.push(p.id); }
    for (let i = 0; i < chosen.length; i += 100) for (const d of await db.getAll(...chosen.slice(i, i + 100).map(id => col(RUN_LINES).doc(id)))) if (d.exists) { parts.set(d.id, { id: d.id, ...d.data() }); scanned.lineParts++; }
    const byRun = new Map(); for (const p of parts.values()) { if (!byRun.has(p.runId)) byRun.set(p.runId, []); byRun.get(p.runId).push(p); }
    for (const [id, list] of byRun) { const r = runs.get(id); if (r) { r.recordLines = r.lines || {}; r.lines = Object.assign(mergeParts(list), r.lines || {}); } }
  }
  const { rows: all, runRows } = historyRows(q, sets, sheets, runs);
  // this page's days: after the cursor's first `skip` groups of its day, down to the oldest day read whole (and an exact
  // order-number match wherever it is)
  let rows = all.filter(g => g.exact || ((!hi || String(g.day || "") <= hi) && (!lo || String(g.day || "") >= lo)));
  if (cur) { let skip = cur.skip; rows = rows.filter(g => !(g.day === cur.day && skip-- > 0)); }
  for (const row of rows) { delete row.search; delete row.match; delete row.exact; }
  // a page is `limit` groups, or fewer where they would pass ANSWER_BYTES (a group carries its sheets, and they their
  // backs): `next` then starts at the first group left out, as after `limit` groups
  const budget = answerBudget(); let n = 0;
  while (n < Math.min(limit, rows.length) && budget.fits(rows[n])) n++;
  if (n < Math.min(limit, rows.length)) truncated.size = true;
  const page = rows.slice(0, n), last = page[page.length - 1];
  let next = null;
  if (rows.length > page.length && last && isDay(last.day)) next = { day: last.day, skip: page.filter(g => g.day === last.day).length + (cur && cur.day === last.day ? cur.skip : 0) };
  else if (lo) { const older = await newestDay(lo); if (older) next = { day: older, skip: 0 }; }
  const onPage = new Set(page.map(g => g.runId).filter(Boolean));
  // each sheet is sent once, in its group (the page reads them there): a second list of them used to double the answer
  return { sets: page, runs: runRows.filter(r => onPage.has(r.runId)).map(r => { delete r.exact; return r; }), total: rows.length, setCount: page.filter(g => g.setId && g.sheets.length && g.status !== "superseded").length, workingCount: page.filter(g => g.draft).length,
    next, window: { from: lo, to: hi }, scanned, truncated };
}
/* ── The Library's two tabs. The laser operator marks a sheet, or a set, cut: "completed". The Library then shows it
   under Completed instead of Current. A sheet record says when and by whom (laserDoneAt in ms, laserDoneBy); so does a
   set's record, which is completed with its last sheet and taken back with any of them. Only op_laserDone writes the two
   fields (op_putSheet and op_setUpdate leave them out of what they write), so an open run saving its copy of a sheet
   again keeps its mark. Every query below is on one field (laserDoneAt, orders, listings, day, runId): no composite index. ── */
const DONE_SHEET = ["id", "metal", "metalLabel", "day", "setId", "setSeq", "sheetIndex", "page", "folder", "fileBase", "orders", "placedCount", "charmCount", "density", "outputs", "laserDoneAt", "laserDoneBy", "archived", "draft", "solidIncluded", "processSeals", "processReady", "archivedLaserDoneAt", "archivedLaserDoneBy", "updatedAt", "activityAt"];
const DONE_FIND = DONE_SHEET.concat(["names", "sources", "listings", "runId"]);
const DONE_SET = ["setId", "seq", "day", "runId", "name", "sheetIds", "materials", "status", "laserDoneAt", "laserDoneBy", "processSeals", "processReady", "archivedLaserDoneAt", "archivedLaserDoneBy", "updatedAt", "activityAt"];
const DONE_MEMBER = ["metal", "sheetIndex", "page", "density", "placedCount", "orders", "outputs", "archived", "laserDoneAt", "processSeals", "processReady", "archivedLaserDoneAt", "archivedLaserDoneBy", "updatedAt", "activityAt"];
const DONE_SCAN = { sheets: 800, sets: 240 };          // the records one call of a search or a metal may read
// an archived sheet's laser mark, kept aside where the one-field Completed count and list do not read it (doneCounts)
const archivedMark = d => ({ processSeals: Readiness.processStamps(d), processReady: false, laserDoneAt: FV.delete(), laserDoneBy: FV.delete(), archivedLaserDoneAt: num(d.laserDoneAt), archivedLaserDoneBy: d.laserDoneBy || null });
const METAL_CODE = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
const isMetal = m => Object.prototype.hasOwnProperty.call(METAL_CODE, String(m || ""));
const previewOf = d => (d.outputs && d.outputs.preview && d.outputs.preview.url) || null;
/** Text as a search compares it: lower case, and "Sep.24.26", "2026-09-24" or "GF_Sep" split into words. */
const foldText = s => String(s == null ? "" : s).toLowerCase().replace(/[._\-/·,:]+/g, " ").replace(/\s+/g, " ").trim();
function doneSheetRow(id, d) {
  return { kind: "sheet", id, metal: d.metal || null, metalLabel: d.metalLabel || null, day: d.day || null, setId: d.setId || null, setSeq: num(d.setSeq) || null, sheetIndex: num(d.sheetIndex) || num(d.page) || 1,
    fileBase: d.fileBase || d.folder || null, draft: !!d.draft || d.solidIncluded === false, orders: (d.orders || []).length, pieces: num(d.placedCount), charms: num(d.charmCount), fill: num(d.density),
    preview: previewOf(d), activityAt:Activity.at(d), processSeals: Readiness.processStamps(d), at: num(d.laserDoneAt) || null, by: d.laserDoneBy || null };
}
function doneSetRow(id, d, members) {
  const live = members.filter(m => m && !m.archived), orders = new Set(live.flatMap(m => (m.orders || []).map(String)));
  return { kind: "set", setId: id, seq: num(d.seq) || null, day: d.day || null, name: d.name || null, materials: (d.materials && d.materials.length ? d.materials : [...new Set(live.map(m => m.metal))]).filter(Boolean), sheetIds: d.sheetIds || [], status: d.status || null,
    sheets: live.map(m => ({ id: m.id, metal: m.metal || null, sheetIndex: num(m.sheetIndex) || num(m.page) || 1, fill: num(m.density), pieces: num(m.placedCount), orders: (m.orders || []).length, preview: previewOf(m) })).sort((a, b) => String(a.metal).localeCompare(String(b.metal)) || a.sheetIndex - b.sheetIndex),
    orders: orders.size, pieces: live.reduce((n, m) => n + num(m.placedCount), 0), fill: live.length ? live.reduce((n, m) => n + num(m.density), 0) / live.length : 0,
    activityAt:Activity.at(d), processSeals: Readiness.processStamps(d), at: num(d.laserDoneAt) || null, by: d.laserDoneBy || null };
}
const sheetHay = (id, d) => [id, d.fileBase, d.folder, d.names, d.day, d.metal, d.metalLabel, METAL_CODE[d.metal], d.setId, d.setSeq ? "set " + d.setSeq : "", "sheet " + (num(d.sheetIndex) || num(d.page) || 1), (d.orders || []).join(" "), (d.listings || []).join(" "), d.laserDoneBy, (d.sources || []).map(s => s && s.name).join(" "), d.laserDoneAt ? new Date(num(d.laserDoneAt)).toISOString().slice(0, 10) : ""].join(" ");
/** Each set's sheets, read for what a Completed row shows (and a search reads): [[setId, [sheet…]]]. */
async function doneMembers(sets, fields) {
  const ids = [...new Set(sets.flatMap(([, d]) => d.sheetIds || []))].filter(isId), read = new Map();
  for (let i = 0; i < ids.length; i += 100) for (const s of await db.getAll(...ids.slice(i, i + 100).map(id => col(SHEETS).doc(id)), { fieldMask: fields })) if (s.exists) read.set(s.id, Object.assign({ id: s.id }, s.data()));
  return new Map(sets.map(([id, d]) => [id, (d.sheetIds || []).map(x => read.get(x)).filter(Boolean)]));
}
/* What the tab counts is what Completed lists: sheets not archived. A sheet is never marked while archived (op_laserDone)
   and one archived with a mark keeps it as archivedLaserDoneAt/By (op_archiveEmptySheet; laserDoneList moves one marked
   before that as it reads it), so the one-field count below holds no archived sheet, with no composite index. */
async function doneCounts() {
  const [s,t]=await Promise.all([col(SHEETS).where("laserDoneAt",">",0).select("setId","draft","solidIncluded","laserDoneAt","archived").get(),col(SETS).where("laserDoneAt",">",0).count().get()]);
  const records=await filingRecords(s.docs.map(d=>d.data()));
  return {sheets:records.filter(d=>!d.archived && Readiness.filed(d)).length,sets:t.data().count};
}
/** Marks a sheet or a set cut on the laser (done), or takes the mark back (done:false), in one transaction. A set marks
    each of its sheets; a sheet marked earlier keeps its own time and name. The set of a sheet is completed with its last
    sheet and taken back with any of them. */
async function op_laserDone(b) {
  const kind = b.kind === "set" ? "set" : b.kind === "sheet" ? "sheet" : null, id = str(b.id, 80);
  if (!kind || !isId(id)) return { error: "bad sheet or set id" };
  const done = b.done !== false && b.done !== "false", by = str(b.by, 80).trim(), at = Date.now();
  if (done && !by) return { error: "Say who marked it completed" };
  // where it was marked, for the orders' timelines (station tracking B): the page (device) and its view (via)
  const device = str(b.device, 40).replace(/[^\w.-]/g, ""), via = str(b.via, 24).replace(/[^\w .-]/g, "").trim();
  const mark = done ? { laserDoneAt: at, laserDoneBy: by } : { laserDoneAt: FV.delete(), laserDoneBy: FV.delete() };
  const res = await db.runTransaction(async tx => {
    let setRef = null, set = null;
    const own = kind === "sheet" ? await tx.get(col(SHEETS).doc(id)) : null;
    if (own && !own.exists) return { error: "There is no such sheet", status: 404 };
    // a sheet repacked into others (archived) is in no list: it is not marked, so the count stays what the list shows
    if (own && done && own.data().archived) return { error: "That sheet was repacked into other sheets: it is no longer in the Library", status: 409 };
    if (kind === "set") setRef = col(SETS).doc(id);
    else if (isId(own.data().setId) && !own.data().draft && own.data().solidIncluded !== false) setRef = col(SETS).doc(own.data().setId);
    if (setRef) { const s = await tx.get(setRef); if (s.exists) set = s.data(); else if (kind === "set" || done) return { error: "There is no such set", status: 404 }; else setRef = null; }
    const ids = set ? [...new Set(set.sheetIds || [])].filter(isId).slice(0, 300) : [];
    // (with what the orders' timelines say of each sheet: its orders, its label and the mark it had)
    const members = ids.length ? await tx.getAll(...ids.map(x => col(SHEETS).doc(x)), { fieldMask: SLIM_SHEET.concat(["placements"]) }) : [];
    const facts = new Map(members.filter(m => m.exists).map(m => [m.id, m.data()])); if (own) facts.set(id, own.data());
    const recovered=new Map();
    for(const [sid,s] of facts)recovered.set('sheet:'+sid,await processHistory('sheet',sid,s));
    if(setRef)recovered.set('set:'+setRef.id,await processHistory('set',setRef.id,set));
    if (done) {
      if (b.stage !== "laser") return {error:"Complete sheets from the Laser cutting section only",status:409};
      const records=members.filter(m=>m.exists && !m.data().archived).map(m=>({...m.data(),id:m.id,processSeals:recovered.get('sheet:'+m.id)}));
      if (set && (!ids.length || members.some(m=>!m.exists || m.data().archived) || (own && !ids.includes(id)))) return {error:"The complete set must be present in Laser cutting first",status:409};
      if (!set && own) records.push({...own.data(),id,processSeals:recovered.get('sheet:'+id)});
      await processDecisions(tx,records);
      const blocked=records.filter(s=>!Readiness.completedBefore(s)).flatMap(Readiness.orderBlockers);
      if(blocked.length)return {error:`Not ready for Laser cutting: order ${blocked[0].id} — ${blocked[0].why}`,status:409};
      const ready=set ? records.every(s=>s.setId===setRef.id) && Readiness.laserGroup(set,records).ready : records.length===1 && Readiness.laserSheet(records[0]).ready;
      if (!ready) return {error:"Not ready for Laser cutting: every remaining sheet needs approved engraving, verified files and QR labels",status:409};
    }
    const state = new Map(members.filter(m => m.exists && !m.data().archived).map(m => [m.id, num(m.data().laserDoneAt) > 0]));
    // every read is made: the writes follow. Reopening changes the current flag, never the seal history.
    const touched = [], process = [], added = [];
    const write = (k,key,old,completed) => {
      const processSeals=recovered.get(k+':'+key).slice();
      const add=how=>{const event=processEvent(how,at,by,processSeals.length);processSeals.push(event);added.push({kind:k,id:key,eventId:event.id});};
      if(completed!==false && !old.processReady && !num(old.laserDoneAt))add('laserReady');
      if(completed===true)add('laserDone');
      const current=completed==null?{}:completed?{laserDoneAt:at,laserDoneBy:by}:mark;
      tx.set(col(k==='set'?SETS:SHEETS).doc(key),{...current,processSeals,processReady:completed==null,updatedAt:FV.serverTimestamp()},{merge:true});
      process.push(processRecord(k,key,{...old,...(completed==null?{}:{laserDoneAt:completed?at:null,laserDoneBy:completed?by:null}),processSeals,processReady:completed==null}));
    };
    if (kind === "sheet") { if (!done || !(num(own.data().laserDoneAt)>0)) { write('sheet',id,own.data(),done); touched.push(id); } if (state.has(id) || !set) state.set(id, done); }
    else for (const [sid, was] of state) if (!done || !was) { write('sheet',sid,facts.get(sid),done); touched.push(sid); state.set(sid, done); }
    let setDone = null, setChanged = false;
    if (setRef) {
      const all = state.size > 0 && state.size === ids.length && [...state.values()].every(Boolean), was = num(set.laserDoneAt) > 0;
      setDone = all; setChanged = setDone !== was;
      if (setDone !== was) write('set',setRef.id,set,setDone);
      else if(!was && done && !set.processReady)write('set',setRef.id,set,undefined);
      else if(!done && set.processReady)write('set',setRef.id,set,false);
    }
    // (a sheet saved without its `orders` list still names them in its pieces' pool ids, "rid_tid_copy")
    const ordersOf = d => Array.isArray(d.orders) && d.orders.length ? d.orders : (Array.isArray(d.poolIds) ? d.poolIds : []).map(orderOfKey).filter(Boolean);
    const marks = touched.map(sid => { const d = facts.get(sid) || {}; return { sheetId: sid, was: num(d.laserDoneAt), wasBy: d.laserDoneBy || "", orders: ordersOf(d), d }; });
    return { ok: true, kind, id, done, at: done ? at : null, by: done ? by : null, sheetIds: touched, setId: setRef ? setRef.id : null, setDone, setChanged, marks, process, added };
  });
  if (res.error) return res;
  const marks = res.marks || []; delete res.marks;
  /* every order on each sheet the call marked (laserDone, a milestone) or took the mark from (a note, "laser cut
     undone"). Only a sheet whose mark changed: one marked again keeps its first event, a retry adds none. */
  await stamp(() => marks.filter(m => (done ? !(m.was > 0) : m.was > 0)).flatMap(m => {
    const sheet = sheetLabel(m.d), orders = [...new Set(m.orders.map(String))].slice(0, 300);
    return orders.map(orderId => done
      ? { orderId, type: "laserDone", at, by, station: "laser", device, sheetId: m.sheetId, sheet, setId: res.setId || "", text: sheet, data: { signedIn: true, marked: kind, via: via || undefined }, id: `${m.sheetId}-${at}` }
      : { orderId, type: "note", at, by, station: "laser", device, sheetId: m.sheetId, sheet, setId: res.setId || "", text: `laser cut undone · ${sheet}`, data: { undone: "laserDone", laserDoneAt: m.was, laserDoneBy: m.wasBy, signedIn: !!by, via: via || undefined }, id: `laserUndone-${m.sheetId}-${m.was}` });
  }), "laser done");
  return Object.assign(res, { counts: await doneCounts() });
}
/** A page of what the laser has done, newest first: sheets, or sets with a summary of their sheets (kind: "sets"). A
    cursor ({ at, skip }) is where the last page stopped: the records completed at or before `at`, past the first `skip`
    of those completed at `at` itself (a set's sheets are marked in the same moment). A search (q) and a metal are applied
    as the records are read, and one call reads at most DONE_SCAN of them: a rare match is found page after page, never
    by reading every record at once. The first page counts what is completed (two aggregates), and countOnly asks for
    nothing else. */
async function op_laserDoneList(b) {
  if(b.sort === "activity" && !b.countOnly)return activityDoneList(b);
  const kind = b.kind === "sets" ? "sets" : "sheets", name = kind === "sets" ? SETS : SHEETS;
  const cur = b.cursor && num(b.cursor.at) > 0 ? { at: num(b.cursor.at), skip: Math.max(0, Math.floor(num(b.cursor.skip))) } : null;
  const counts = cur ? null : await doneCounts();
  if (b.countOnly) return { counts };
  const limit = Math.min(200, Math.max(1, Math.floor(num(b.limit)) || 60)), q = foldText(str(b.q, 120)), metal = isMetal(b.metal) ? b.metal : null;
  const cap = kind === "sheets" || q || metal ? DONE_SCAN[kind] : limit, fields = kind === "sets" ? DONE_SET : q ? DONE_FIND : DONE_SHEET;
  const rows = [], repair = []; let at = cur ? cur.at : null, skip = cur ? cur.skip : 0, scanned = 0, end = false;
  while (rows.length < limit && scanned < cap && !end) {
    const want = Math.max(1, Math.min(cap - scanned, q || metal ? Math.max(100, limit) : limit - rows.length)), skipped = skip;
    const snap = await col(name).where("laserDoneAt", at == null ? ">" : "<=", at == null ? 0 : at).orderBy("laserDoneAt", "desc").limit(skipped + want).select(...fields).get();
    let pass = skipped; const docs = snap.docs.filter(d => !(at != null && num(d.data().laserDoneAt) === at && pass-- > 0));
    const filing = kind === "sheets" ? await filingRecords(docs.map(d=>d.data())) : null;
    const members = kind === "sets" ? await doneMembers(docs.map(d => [d.id, d.data()]).filter(([, d]) => !metal || !(d.materials || []).length || d.materials.includes(metal)), q ? DONE_MEMBER.concat(["names", "fileBase", "folder", "listings", "day", "setSeq"]) : DONE_MEMBER) : null;
    let i = 0;
    for (; i < docs.length && rows.length < limit; i++) {
      const d = docs[i].data(), t = num(d.laserDoneAt); scanned++;
      if (t === at) skip++; else { at = t; skip = 1; }
      if (kind === "sheets") {
        if (d.archived) { if (repair.length < 20) repair.push([docs[i].id, d, t]); continue; }
        if (!Readiness.filed(filing[i])) continue;
        if ((metal && d.metal !== metal) || (q && !foldText(sheetHay(docs[i].id, d)).includes(q))) continue;
        rows.push(doneSheetRow(docs[i].id, d));
      } else {
        const list = members.get(docs[i].id); if (!list) continue;
        if (metal && !list.some(m => m.metal === metal) && !(d.materials || []).includes(metal)) continue;
        if (q && !foldText([docs[i].id, d.name, d.day, "set " + d.seq, d.laserDoneBy, ...(d.materials || []).map(m => METAL_CODE[m]), ...list.map(m => sheetHay(m.id, m))].join(" ")).includes(q)) continue;
        rows.push(doneSetRow(docs[i].id, d, list));
      }
    }
    if (i === docs.length && snap.size < skipped + want) end = true;
  }
  // a sheet archived with its mark before archiving kept the mark aside: kept aside now, so the count comes to what the
  // list shows (one at the page's last moment stays, as the next page's cursor counts it among those it passes)
  const fix = repair.filter(([, , t]) => t !== at);
  if (fix.length) { const batch = db.batch(); for (const [id, d] of fix) batch.set(col(SHEETS).doc(id), archivedMark(d), { merge: true }); await batch.commit().catch(() => {}); }
  return { kind, rows, next: end ? null : { at, skip }, scanned, ...(counts ? { counts } : {}) };
}
/* Activity pages scan a single updatedAt index. Cursor ties retain every record; completion
   dates remain historical facts. The date range is shop-local, including DST boundaries. */
async function activityDoneList(b) {
  const kind=b.kind==='sets'?'sets':'sheets', name=kind==='sets'?SETS:SHEETS, direction=b.direction==='asc'?'asc':'desc';
  const limit=Math.min(200,Math.max(1,num(b.limit)||60)), q=foldText(str(b.q,120)), metal=isMetal(b.metal)?b.metal:null;
  const cur=b.cursor?.activity ? b.cursor : null, range=Activity.bounds(b.range), counts=cur?null:await doneCounts();
  let at=cur?.at || null, skip=cur?.skip || 0, stamp=cur?.stamp!==false, scanned=0, end=false;
  const rows=[], fields=kind==='sets'?DONE_SET:q?DONE_FIND:DONE_SHEET;
  while(rows.length<limit && scanned<DONE_SCAN[kind] && !end){
    const want=Math.min(100,DONE_SCAN[kind]-scanned), skipped=skip;
    let query=col(name).orderBy('updatedAt',direction);
    if(at!=null)query=query.where('updatedAt',direction==='asc'?'>=':'<=',stamp?admin.firestore.Timestamp.fromMillis(at):at);
    if(range){query=query.where('updatedAt','>=',admin.firestore.Timestamp.fromMillis(Activity.dayStart(range[0]))).where('updatedAt','<',admin.firestore.Timestamp.fromMillis(Activity.dayStart(Activity.shift(range[1],1))));}
    const snap=await query.limit(skipped+want).select(...fields).get();
    let pass=skipped;const docs=snap.docs.filter(d=>!(at!=null && Activity.ms(d.data().updatedAt)===at && pass-->0));
    const filing=kind==='sheets'?await filingRecords(docs.map(d=>d.data())):null;
    const members=kind==='sets'?await doneMembers(docs.filter(d=>num(d.data().laserDoneAt)>0).map(d=>[d.id,d.data()]),q?DONE_MEMBER.concat(['names','fileBase','folder','listings','day','setSeq']):DONE_MEMBER):null;
    let i=0;
    for(;i<docs.length && rows.length<limit;i++){
      const doc=docs[i], d=doc.data(), t=Activity.ms(d.updatedAt);scanned++;
      if(t===at)skip++;else{at=t;skip=1;}stamp=typeof d.updatedAt==='object';
      if(!num(d.laserDoneAt) || d.archived || !Activity.matches(d,b.range))continue;
      if(kind==='sheets'){
        if(!Readiness.filed(filing[i]) || (metal && d.metal!==metal) || (q && !foldText(sheetHay(doc.id,d)).includes(q)))continue;
        rows.push(doneSheetRow(doc.id,d));
      }else{
        const list=members.get(doc.id)||[];
        if(metal && !list.some(m=>m.metal===metal) && !(d.materials||[]).includes(metal))continue;
        if(q && !foldText([doc.id,d.name,d.day,'set '+d.seq,d.laserDoneBy,...list.map(m=>sheetHay(m.id,m))].join(' ')).includes(q))continue;
        rows.push(doneSetRow(doc.id,d,list));
      }
    }
    if(i===docs.length && snap.size<skipped+want)end=true;
  }
  return {kind,rows,next:end?null:{activity:true,at,skip,stamp},scanned,...(counts?{counts}:{})};
}
/* "Where is order 3712345?" and "Which sheets hold listing 1718000?" A number is looked up as both, however old:
   · an order: the sheets whose `orders` list it (as the history search finds one);
   · a listing: the sheets whose `listings` list it (written with each sheet saved since the Library could look for one),
     and — for a sheet saved before, which has no `listings` — the orders whose line was bought from that listing, read
     from the runs of the window's sheets without `listings` (their lines, those kept beside them and their line archive,
     newest part first, capped as the history search caps it), then the sheets that list those orders. `fallback` says
     what that read and whether a cap cut it short. A number found as an order is not looked for as a listing that way.
     A sheet found that way whose every piece's line was read is given its `listings` (a few a search, FIND_CAP.fill), so
     the next search finds it directly, whatever its day.
   The answer gives each sheet with how it matched (match: order, listing): the ones not completed as the Library's cards
   show them (sheets, with laser readiness) and the completed ones as the Completed list shows them (rows), and the sets
   they belong to (sets, those not completed, with only what the Library's Sets view reads; setRows for the completed
   ones), all in one answer's room (answerBudget): what does not fit is left out and `truncated` says so. */
const FIND_CAP = { sheets: 400, window: 600, runs: 120, fill: 20 };
async function legacyListingOrders(q, b) {
  // listingOf: each line read, by its key ("{order}_{transaction}"), to the listing it was bought from
  const out = { orders: new Set(), listingOf: new Map(), window: null, runs: 0, parts: 0, capped: false };
  const span = Math.min(90, Math.max(1, Math.floor(num(b.days)) || 30)), top = isDay(b.today) ? b.today : await newestDay();
  if (!top) return out;
  const lo = dayShift(top, -(span - 1)); out.window = { from: lo, to: top };
  const snap = await byDay(SHEETS, ["runId", "listings", "archived", "day"], { from: lo, n: FIND_CAP.window });
  if (snap.size >= FIND_CAP.window) out.capped = true;
  const runIds = [...new Set(snap.docs.map(d => d.data()).filter(d => !d.archived && !Array.isArray(d.listings) && isId(d.runId)).map(d => d.runId))];
  if (runIds.length > FIND_CAP.runs) out.capped = true;
  if (!runIds.length) return out;
  const runs = (await whereIn(RUNS, "runId", runIds.slice(0, FIND_CAP.runs), ["runId", "lines", "liveLines", "lineArchive"], FIND_CAP.runs)).map(d => Object.assign({}, d.data(), { runId: d.data().runId || d.id }));
  out.runs = runs.length;
  await eachWithLiveLines(runs);
  const take = lines => {
    for (const [key, l] of Object.entries(lines || {})) {
      const lid = l && l.snap ? String(l.snap.listingId || "") : "";
      if (/^\d{1,24}$/.test(lid) && out.listingOf.size < 200000) out.listingOf.set(l.orderId != null && l.transactionId != null ? `${l.orderId}_${l.transactionId}` : key, lid);
      if (lid === q && l.orderId != null && String(l.orderId)) out.orders.add(String(l.orderId));
    }
  };
  for (const r of runs) take(r.lines);
  const archived = runs.filter(r => r.lineArchive && num(r.lineArchive.parts) > 0).map(r => r.runId).filter(isId);
  const listed = (await whereIn(RUN_LINES, "runId", archived, ["runId", "bytes", "at", "seq"], HISTORY_CAP.parts)).map(d => ({ id: d.id, ...d.data() }));
  if (listed.length >= HISTORY_CAP.parts) out.capped = true;
  listed.sort((x, y) => num(y.at) - num(x.at) || num(y.seq) - num(x.seq));
  let bytes = 0; const chosen = [];
  for (const p of listed) { if (chosen.length && bytes + num(p.bytes) > HISTORY_PART_BYTES) { out.capped = true; break; } bytes += num(p.bytes); chosen.push(p.id); }
  for (let i = 0; i < chosen.length; i += 100) for (const d of await db.getAll(...chosen.slice(i, i + 100).map(id => col(RUN_LINES).doc(id)), { fieldMask: ["json"] })) {
    if (!d.exists) continue; out.parts++;
    try { take(JSON.parse(d.data().json || "{}")); } catch (_) { /* an unreadable part is left out */ }
  }
  return out;
}
async function op_findSheets(b) {
  const q = String(b.q == null ? "" : b.q).trim();
  if (!/^\d{4,20}$/.test(q)) return { error: "Search for an order or a listing number" };
  const forms = v => [String(v)].concat(Number.isSafeInteger(+v) ? [+v] : []), listed = (list, set) => (list || []).some(v => set.has(String(v)));
  const found = new Map(), mine = new Set([q]);
  const add = (docs, why, hit) => { for (const s of docs) { const d = s.data(); if (d.archived || !hit(d)) continue; const f = found.get(s.id) || { d, match: new Set() }; f.match.add(why); found.set(s.id, f); } };
  const [byOrder, byListing] = await Promise.all([col(SHEETS).where("orders", "array-contains-any", forms(q)).limit(FIND_CAP.sheets).select(...SLIM_SHEET).get(), col(SHEETS).where("listings", "array-contains-any", forms(q)).limit(FIND_CAP.sheets).select(...SLIM_SHEET).get()]);
  add(byOrder.docs, "order", d => listed(d.orders, mine)); add(byListing.docs, "listing", d => listed(d.listings, mine));
  let truncated = byOrder.size >= FIND_CAP.sheets || byListing.size >= FIND_CAP.sheets, fallback = null;
  if (!byOrder.docs.some(s => listed(s.data().orders, mine)) && b.fallback !== false) {
    const legacy = await legacyListingOrders(q, b), orders = [...legacy.orders], before = new Set(found.keys());
    fallback = { window: legacy.window, runs: legacy.runs, parts: legacy.parts, orders: orders.length, capped: legacy.capped, filled: 0 };
    for (let i = 0; i < orders.length && i < 600; i += 15) {
      const chunk = orders.slice(i, i + 15), snap = await col(SHEETS).where("orders", "array-contains-any", chunk.flatMap(forms)).limit(FIND_CAP.sheets).select(...SLIM_SHEET).get();
      if (snap.size >= FIND_CAP.sheets) truncated = true;
      const bought = new Set(chunk); add(snap.docs, "listing", d => listed(d.orders, bought));
    }
    if (orders.length > 600) fallback.capped = true;
    // the sheets found through their run's lines, each piece's line read: their listings are written, a few a search
    const fill = [];
    for (const [id, f] of found) {
      if (fill.length >= FIND_CAP.fill) break;
      if (before.has(id) || Array.isArray(f.d.listings) || !(f.d.poolIds || []).length) continue;
      const ids = f.d.poolIds.map(p => legacy.listingOf.get(String(p).replace(/_[^_]*$/, "")));
      if (ids.every(Boolean)) fill.push([id, [...new Set(ids)].slice(0, 500)]);
    }
    const wrote = await Promise.all(fill.map(([id, listings]) => col(SHEETS).doc(id).update({ listings }).then(() => { found.get(id).d.listings = listings; return 1; }, () => 0)));
    fallback.filled = wrote.reduce((n, x) => n + x, 0);
  }
  const filing=await filingRecords([...found.values()].map(f=>f.d));let fi=0;
  for(const f of found.values())f.d=filing[fi++];
  const all = [...found.entries()], isDone = f => Readiness.filed(f.d);
  // one answer's room for all of it, in the order the page needs it: the cards, the completed rows, then the sets
  const budget = answerBudget(); let cut = false;
  const fitting = list => { let n = 0; while (n < list.length && budget.fits(list[n])) n++; if (n < list.length) cut = true; return list.slice(0, n); };
  const open = all.filter(([, f]) => !isDone(f)), { sheets, rest } = await sheetEntries(open.map(([id, f]) => [id, f.d]), budget);
  for (const s of sheets) s.match = [...found.get(s.id).match];
  const rows = fitting(all.filter(([, f]) => isDone(f)).map(([id, f]) => Object.assign(doneSheetRow(id, f.d), { match: [...f.match] })).sort((x, y) => y.at - x.at));
  const setIds = [...new Set(all.map(([, f]) => f.d.setId))].filter(isId).slice(0, 200), read = [];
  for (let i = 0; i < setIds.length; i += 100) for (const d of await db.getAll(...setIds.slice(i, i + 100).map(id => col(SETS).doc(id)), { fieldMask: FIND_SET })) if (d.exists) read.push([d.id, d.data()]);
  const doneSets = read.filter(([, s]) => num(s.laserDoneAt) > 0), members = await doneMembers(doneSets, DONE_MEMBER);
  const sets = fitting(read.filter(([, s]) => !(num(s.laserDoneAt) > 0)).map(([id, s]) => findSetRow(id, s)));
  const setRows = fitting(doneSets.map(([id, s]) => doneSetRow(id, s, members.get(id) || [])).sort((x, y) => y.at - x.at));
  const count = why => all.filter(([, f]) => f.match.has(why)).length;
  return { q, sheets, rows, sets, setRows, matches: { order: count("order"), listing: count("listing") }, fallback, truncated: truncated || cut || rest.length > 0 };
}
/* A set the search found that is not completed, for the Library's Sets view (Sets.libraryGroups and libraryCard): what they
   read, and no more — its orders' held notes but not their lines, its label files but not their payloads. `partial` says
   so: undoing its completion reads the whole record first (setGet). */
const FIND_SET = DONE_SET.concat(["orders", "labels", "labelFiles", "refused", "completionDay", "completedAt", "committedAt", "updatedAt"]);
function findSetRow(id, d) {
  const orders = {}; for (const [rid, o] of Object.entries(d.orders || {})) orders[rid] = o && o.held ? { held: o.held } : {};
  return { setId: d.setId || id, seq: num(d.seq) || null, day: d.day || null, name: d.name || null, runId: d.runId || null, status: d.status || null, sheetIds: d.sheetIds || [], materials: d.materials || [], orders,
    labels: d.labels || null, labelFiles: (d.labelFiles || []).map(f => ({ path: f.path || null, url: f.url || null, sheetId: f.sheetId || null, part: f.part || 1, parts: f.parts || 1, orders: f.orders || [] })),
    refused: d.refused || null, completionDay: d.completionDay || null, completedAt: ms(d.completedAt), committedAt: ms(d.committedAt), updatedAt: ms(d.updatedAt), partial: true };
}
// ── bridge session log: Design_Bridge/{session} + /log rows (ids and counts only, never order text) ──
async function op_bridgeLog(b) {
  const session = str(b.session, 80); if (!/^[\w\-]{6,80}$/.test(session)) return { error: "bad session" };
  const ref = col(BRIDGE).doc(session);
  // nothing reads the log in bulk: a TTL policy on expireAt (collection group log, and the session documents) removes a
  // row 30 days after it was written, a sandbox row after 3; the session's own date moves with its latest row
  const expireAt = new Date(Date.now() + (PREFIX ? 3 : 30) * 86400000);
  const meta = Object.assign({}, b.meta || {}, { sessionId: session, updatedAt: FV.serverTimestamp(), expireAt });
  const rows = (b.rows || []).slice(0, 200);
  if (rows.length) meta.commands = FV.increment(rows.filter(r => r.dir === "cmd").length);
  await ref.set(meta, { merge: true });
  let batch = db.batch(), n = 0;
  for (const r of rows) { batch.set(ref.collection("log").doc(), { t: num(r.t) || Date.now(), dir: str(r.dir, 10), type: str(r.type, 40), ms: r.ms == null ? null : num(r.ms), payload: r.payload && typeof r.payload === "object" ? r.payload : (r.payload == null ? null : str(r.payload, 400)), expireAt }); if (++n >= 400) { await batch.commit(); batch = db.batch(); n = 0; } }
  if (n) await batch.commit();
  await sweepBridge(session);
  return { ok: true, rows: rows.length };
}
/* Nothing reads an old session's log. A session not written for 30 days (3 in the sandbox) goes with its log, a page per
   call and at most once every 10 minutes per instance and workspace, and a session left open that long lets go of its own
   rows past that age (a TTL policy on expireAt, if one is switched on, does the same). */
const bridgeSweptAt = new Map();
async function sweepBridge(session) {
  if (Date.now() - (bridgeSweptAt.get(PREFIX) || 0) < 600000) return 0;
  bridgeSweptAt.set(PREFIX, Date.now());
  let gone = 0;
  try {
    const keepMs = (PREFIX ? 3 : 30) * 86400000, cutoff = new Date(Date.now() - keepMs);
    const old = await col(BRIDGE).where("updatedAt", "<", cutoff).limit(3).get();
    for (const d of old.docs) {
      if (d.id === session) continue;
      const logs = await d.ref.collection("log").limit(400).get();
      const batch = db.batch(); logs.docs.forEach(x => batch.delete(x.ref)); if (logs.size < 400) batch.delete(d.ref);
      await batch.commit(); gone += logs.size;
    }
    const mine = await col(BRIDGE).doc(session).collection("log").where("expireAt", "<", new Date()).limit(400).get();
    if (!mine.empty) { const batch = db.batch(); mine.docs.forEach(x => batch.delete(x.ref)); await batch.commit(); gone += mine.size; }
  } catch (e) { console.warn("[charmNestLibrary] old bridge log not deleted:", e && e.message); }
  return gone;
}
// ── learned maps: aliases, no-design list, option maps ──
/** Every document of a map, read 500 at a time in document order, as much as fits in one answer and is read within
    ANSWER_MS (truncated says what that left out). Each map used to stop at a count (3,000 aliases, 2,000 option maps,
    1,000 no-design rows) and a map past it lost the rest without a word. */
async function pagedDocs(name, budget) {
  const docs = []; let after = null;
  for (;;) {
    let page = db.collection(name).orderBy(docOrder()).limit(500); if (after) page = page.startAfter(after);
    const snap = await page.get();
    for (const d of snap.docs) { if (!budget.fits([d.id, d.data()])) return { docs, truncated: true }; docs.push(d); after = d.id; }
    if (snap.size < 500) return { docs, truncated: false };
    if (budget.late()) return { docs, truncated: true };
  }
}
/** A map as this request sees it: the shared documents, and in the sandbox `own` too, the sandbox's copy (Sandbox_<name>),
    which only a sandbox answer ever wrote. One answer's room is shared by both. */
async function mapDocs(name) {
  const budget = answerBudget(), shared = await pagedDocs(name, budget);
  if (!PREFIX) return shared;
  const own = await pagedDocs(PREFIX + name, budget);
  return { docs: shared.docs, own: own.docs, truncated: shared.truncated || own.truncated };
}
/** The no-design rows (the shared ones the sandbox did not take off its own list, then the sandbox's own) as plain objects
    with their id; `get` reads a collection (a query, or a transaction's read). */
async function noDesignRows(get) {
  const shared = (await get(db.collection(NODESIGN))).docs.map(d => Object.assign({ id: d.id }, d.data()));
  if (!PREFIX) return shared;
  const own = (await get(db.collection(PREFIX + NODESIGN))).docs.map(d => Object.assign({ id: d.id }, d.data()));
  const hidden = new Set(own.filter(r => r.tombstone).map(r => r.tombstone));
  return shared.filter(r => !hidden.has(r.id)).concat(own.filter(r => !r.tombstone));
}
async function op_aliasGet() {
  const { docs, own = [], truncated } = await mapDocs(ALIASES), out = {};
  docs.forEach(d => { out[d.id] = d.data(); });
  // the sandbox's own answer for a listing stands over the shared one's, field by field (its per-SKU answers join the shared ones)
  own.forEach(d => { const mine = d.data(), was = out[d.id]; out[d.id] = was ? Object.assign({}, was, mine, was.bySku || mine.bySku ? { bySku: Object.assign({}, was.bySku, mine.bySku) } : {}) : mine; });
  return { aliases: out, truncated };
}
/* "Use this charm": for the listing and the SKU the line came with (fromSku, kept under bySku), so on a listing whose
   variations each have a SKU one variation's answer is never another's; a line with no SKU answers for the listing (sku).
   v: 2 marks the listing's sku as answered this way (charm-nest-orders.js resolveSku). An answer for one SKU leaves v as it
   was: an alias saved before 27 Sep (sku, no v) goes on standing in for its SKU (v: 2 there silently dropped it).
   A charm-only listing's Huggie CHARM SET with no SKU answers for the listing's huggie (huggie), never its necklace (sku). */
async function op_aliasPut(b) {
  const lid = str(b.listingId, 30).replace(/\D/g, ""), sku = String(b.sku || "").trim().toUpperCase(), from = String(b.fromSku || "").trim().toUpperCase().slice(0, 120);
  if (!lid || !Master.isSku(sku)) return { error: "listingId and sku required" };
  const doc = { listingId: lid, by: str(b.by || "operator", 80), title: str(b.title, 200), updatedAt: FV.serverTimestamp() };
  if (from) doc.bySku = { [from]: sku }; else if (b.huggie === true) doc.huggie = sku; else { doc.sku = sku; doc.v = 2; }
  await db.collection(PREFIX + ALIASES).doc(lid).set(doc, { merge: true }); return { ok: true };   // (a sandbox answer is the sandbox's own copy only)
}
async function op_noDesignGet() {
  const { docs, own = [], truncated } = await mapDocs(NODESIGN);
  const hidden = new Set(own.map(d => d.data()).filter(r => r.tombstone).map(r => r.tombstone));
  const rows = docs.filter(d => !hidden.has(d.id)).concat(own.filter(d => !d.data().tombstone)).map(d => Object.assign({ id: d.id }, d.data()));
  return { list: { patterns: rows.filter(r => r.pattern).map(r => r.pattern), skus: rows.filter(r => r.sku).map(r => r.sku), rows }, truncated };
}
async function op_noDesignPut(b) { const doc = { by: str(b.by || "operator", 80), note: str(b.note, 200), createdAt: FV.serverTimestamp() }; if (b.pattern) { try { new RegExp(String(b.pattern)); } catch (_) { return { error: "bad pattern" }; } doc.pattern = str(b.pattern, 120); } else if (b.sku) doc.sku = String(b.sku).trim().toUpperCase().slice(0, 40); else return { error: "pattern or sku required" }; const ref = await db.collection(PREFIX + NODESIGN).add(doc); return { ok: true, id: ref.id }; }
/* In the sandbox only the sandbox's own row is deleted. A shared row (production's) is never touched from there: the
   sandbox takes it off its own list with a mark in its copy (tombstone: the row's id), which Reset deletes too. */
async function op_noDesignDelete(b) {
  if (!isId(b.id)) return { error: "bad id" };
  if (!PREFIX) { await db.collection(NODESIGN).doc(b.id).delete(); return { ok: true }; }
  const own = db.collection(PREFIX + NODESIGN).doc(b.id);
  if ((await own.get()).exists) { await own.delete(); return { ok: true }; }
  await db.collection(PREFIX + NODESIGN).doc("del_" + b.id).set({ tombstone: b.id, by: str(b.by || "operator", 80), createdAt: FV.serverTimestamp() });
  return { ok: true };
}
async function op_optionMapGet() {
  const { docs, own = [], truncated } = await mapDocs(OPTMAP), out = {};
  docs.forEach(d => { out[d.id] = d.data().map || {}; });
  // the sandbox's own answer for an option value stands over the shared one's (the listing's other answers are kept)
  own.forEach(d => { const mine = d.data().map || {}, was = out[d.id] || {}, next = {}; for (const n of new Set([...Object.keys(was), ...Object.keys(mine)])) next[n] = Object.assign({}, was[n], mine[n]); out[d.id] = next; });
  return { maps: out, truncated };
}
async function op_optionMapPut(b) {
  const lid = b.listingId === "*" ? "*" : str(b.listingId, 30).replace(/\D/g, ""); const name = str(b.optionName, 80).toLowerCase().trim(), value = str(b.optionValue, 200).toLowerCase().replace(/\s+/g, " ").trim();
  if (!lid || !name || !value) return { error: "listingId, optionName and optionValue required" };
  const m = b.map || {}; const field = ["form", "size", "chain", "ignore", "design"].includes(m.field) ? m.field : null; if (!field) return { error: "map.field must be form, size, chain, ignore or design" };
  // design: the option picks the charm (Zodiac Sign: Pisces), for one listing, a SKU of the master index
  const design = field === "design" ? String(m.value || "").trim().toUpperCase() : "";
  if (field === "design" && (lid === "*" || !Master.isSku(design))) return { error: "the charm an option picks is saved for one listing, as a master SKU" };
  const ref = db.collection(PREFIX + OPTMAP).doc(lid); const snap = await ref.get(); const cur = snap.exists ? (snap.data().map || {}) : {};   // (the sandbox's copy starts from its own answers, never from production's)
  cur[name] = cur[name] || {}; cur[name][value] = { field, value: field === "ignore" ? null : design || str(m.value, 80), by: str(b.by || "operator", 80), at: Date.now() };
  await ref.set({ listingId: lid, map: cur, updatedAt: FV.serverTimestamp() }, { merge: true });
  return { ok: true };
}

// ── Custom Orders: a special line (custom, rework, chain only, add-on…) finished by hand ──
/* Review → Custom Orders prints the sorting station's QR sticker for a special line and marks it completed: the line is
   then never pooled and no longer holds its order. One document per order line (its sorter key, "{receipt}_{transaction}"),
   kept with the sticker it printed so it can be printed again after the order has left the pull. Every sorter browser
   reads the same records (customGet). A record is kept for good (Paul, 29 Sep 00:35: "the seal must always remain and
   follow that order forever"): reopening or undoing (customReopen) only sets its state to "open", with who and when in
   its history; its seals (stamps) are never cleared. Sandboxed with the sorter's own records. */
const CUSTOM = "Charm_Custom_Orders";
SANDBOXED.add(CUSTOM);
const CUSTOM_DAYS = 180;
const lineKeyOk = s => /^[\w\-]{3,120}$/.test(String(s || ""));
// the list every sorter reads with its maps carries no stickers (hasLabel says one is kept); one line's record, with its
// sticker, is read by its key when that sticker is printed again. The lists read every field but the sticker (up to 20 KB
// each); a record written before hasLabel was kept is taken to have one (every print hands one), and printing it again
// reads it by its key and says so when there is none.
const CUSTOM_FIELDS = ["key", "receiptId", "transactionId", "sku", "title", "category", "kind", "state", "lastPrintedAt", "lastPrintedBy", "prints", "printedAt", "printedBy", "completedAt", "completedBy", "how", "stamps", "history", "reopenedAt", "reopenedBy", "updatedAtMs", "hasLabel"];
const customRow = (d, withLabel) => { const x = Object.assign({}, d); delete x.updatedAt; if (!withLabel) { x.hasLabel = typeof x.hasLabel === "boolean" ? x.hasLabel : x.label !== undefined ? !!x.label : true; delete x.label; } return x; };
async function op_customGet(b) {
  if (b.key != null) { const key = String(b.key); if (!lineKeyOk(key)) return { error: "bad key" }; const s = await col(CUSTOM).doc(key).get(); return { record: s.exists ? customRow(s.data(), true) : null }; }
  // the lines a sorter has pulled, read by their keys (it polls these every minute): a key with no record was never
  // completed; one reopened says state "open" and keeps its seals; `keys` says how many were read, the first that many asked for
  if (Array.isArray(b.keys)) {
    const keys = [...new Set(b.keys.map(String))].filter(lineKeyOk).slice(0, 1000), records = {};
    for (let i = 0; i < keys.length; i += 100) for (const s of await db.getAll(...keys.slice(i, i + 100).map(k => col(CUSTOM).doc(k)), { fieldMask: CUSTOM_FIELDS })) if (s.exists) records[s.id] = customRow(s.data(), false);
    return { records, keys: keys.length, truncated: new Set(b.keys.map(String)).size > keys.length };
  }
  const since = Date.now() - Math.max(1, Math.min(CUSTOM_DAYS, b.days == null ? CUSTOM_DAYS : num(b.days))) * 86400000;
  const snap = await col(CUSTOM).where("updatedAtMs", ">=", since).orderBy("updatedAtMs", "desc").limit(1000).select(...CUSTOM_FIELDS).get();
  const records = {}; snap.docs.forEach(d => { records[d.id] = customRow(d.data(), false); });
  return { records, truncated: snap.size >= 1000 };
}
/* Custom designs accepted by Send to Sheet are a different decision from a Custom Order completed by hand.
   The pending record survives a close before pooling; the sent phase and each line's timeline commit together.
   A retry uses the original files, copy mapping, signer and time. It never writes a completion, QR or engraving seal. */
const CUSTOM_SHEET = "Charm_Custom_Sheet";
SANDBOXED.add(CUSTOM_SHEET);
const customSheetKeyOk = key => typeof key === "string" && key.length >= 3 && key.length <= 240 && !/[\x00-\x1f]/.test(key);
const customSheetHash = key => require("crypto").createHash("sha256").update(key).digest("hex");
const customSheetRef = key => col(CUSTOM_SHEET).doc("card-" + customSheetHash(key));
const customSheetLineRef = key => col(CUSTOM_SHEET).doc("line-" + customSheetHash(key));
const customSheetRow = d => { const r = Object.assign({}, d); delete r.updatedAt; delete r.fingerprint; return r; };
function customSheetCanonical(v) {
  return JSON.stringify(v, function (_, x) { return x && typeof x === "object" && !Array.isArray(x) ? Object.fromEntries(Object.keys(x).sort().map(k => [k, x[k]])) : x; });
}
function cleanCustomSheet(b) {
  const r = b.record, phase = b.phase || r?.phase;
  if (!r || !customSheetKeyOk(r.ck) || !/^\d{1,30}$/.test(String(r.rid || "")) || !["pending", "sent"].includes(phase)) return null;
  const sent = r.sent, by = str(sent?.by, 80).trim(), at = num(sent?.at), id = str(sent?.id, 600);
  if (!by || !(at > 1e12 && at <= Date.now() + 3600000) || !id || id !== String(sent.id) || !sent.lines || Array.isArray(sent.lines) || typeof sent.lines !== "object") return null;
  if (!Array.isArray(r.files) || !r.files.length || r.files.length > 100) return null;
  const files = [], ids = new Map();
  for (const f of r.files) {
    if (!f || typeof f.id !== "string" || !/^[\w.:-]{1,120}$/.test(f.id) || ids.has(f.id) || !["ai", "dxf"].includes(f.kind) || f.state !== "ready" || !isMetal(f.metal)) return null;
    const qty = num(f.qty), pieces = num(f.pieces);
    if (!Number.isInteger(qty) || qty < 1 || qty > 99 || !Number.isInteger(pieces) || pieces < 1 || pieces > 10000) return null;
    const cloud = f.cloud && typeof f.cloud === "object" ? { path: str(f.cloud.path, 400), url: str(f.cloud.url, 1600) } : null;
    if (!cloud || !((/^charmnest\//.test(cloud.path) && !cloud.path.includes("..")) || /^https:\/\//.test(cloud.url))) return null;
    const file = { id: f.id, name: str(f.name, 200), kind: f.kind, size: Math.max(0, num(f.size)), hash: str(f.hash, 100), cloud,
      metal: str(f.metal, 60), qty, pieces, state: "ready" };
    for (const key of ["wMm", "hMm", "maxPt", "minPt", "maxAreaPt2"]) file[key] = Math.max(0, num(f[key]));
    if (typeof f.thumb === "string" && f.thumb.length <= 60000 && /^data:image\/png;base64,/.test(f.thumb)) file.thumb = f.thumb;
    if (typeof f.units === "string") file.units = str(f.units, 60);
    files.push(file); ids.set(file.id, file);
  }
  const lines = {}, keys = Object.keys(sent.lines);
  if (!keys.length || keys.length > 100) return null;
  let count = 0;
  for (const key of keys) {
    if (!lineKeyOk(key) || orderOfKey(key) !== String(r.rid) || key.length > 80 || !Array.isArray(sent.lines[key])) return null;
    lines[key] = [];
    for (const pc of sent.lines[key]) {
      const f = ids.get(pc?.f), i = pc?.i;
      if (!f || !Number.isInteger(i) || i < 0 || i >= f.pieces || ++count > 10000) return null;
      const copy = { f: f.id, i };
      if (pc.removed) copy.removed = typeof pc.removed === "string" ? str(pc.removed, 120) : true;
      lines[key].push(copy);
    }
  }
  if (!count) return null;
  const doc = { ck: r.ck, rid: String(r.rid), at: num(r.at) > 0 ? num(r.at) : at, phase, files, sent: { id, at, by, lines } };
  if (bytesOf(doc) > 800000) return null;
  return doc;
}
function customSheetEvents(r) {
  return Object.keys(r.sent.lines).map(key => Timeline.clean({ orderId: r.rid, type: "designSent", id: `${key}.${r.sent.at}`, at: r.sent.at, by: r.sent.by, station: "sorter", lineKey: key, transactionId: key.split("_")[1] || "",
    text: `Custom design${r.files.length === 1 ? "" : "s"} sent to the sheets by ${r.sent.by}: ${r.files.map(f => `${f.name} × ${f.qty} → ${METAL_CODE[f.metal] || f.metal}`).join(", ")}`.slice(0, 200),
    data: { decisionId: r.sent.id, card: r.ck, files: r.files.map(f => ({ name: f.name, qty: f.qty, metal: f.metal, pieces: f.pieces })).slice(0, 12), pieces: r.sent.lines[key].length }
  }, { source: "sorter" })).filter(Boolean);
}
async function op_customSheetPut(b) {
  const doc = cleanCustomSheet(b); if (!doc) return { error: "valid custom sheet files, signed decision and piece copy mapping required" };
  // Cleanup markers describe what happened to a copy later; its original assignment and signed decision stay fixed.
  const decision = r => ({ ck: r.ck, rid: r.rid, files: r.files, sent: { id: r.sent.id, at: r.sent.at, by: r.sent.by,
    lines: Object.fromEntries(Object.entries(r.sent.lines).map(([key, pcs]) => [key, pcs.map(pc => ({ f: pc.f, i: pc.i }))])) } });
  const fingerprint = customSheetCanonical(decision(doc));
  const ref = customSheetRef(doc.ck), lineRefs = Object.keys(doc.sent.lines).map(customSheetLineRef);
  return db.runTransaction(async tx => {
    const snap = await tx.get(ref), old = snap.exists ? snap.data() : null;
    if (old && customSheetCanonical(decision(old)) !== fingerprint) return { error: "These custom designs were already sent with a different decision; reload the original send before retrying", status: 409 };
    const claimed = lineRefs.length ? await tx.getAll(...lineRefs) : [];
    if (claimed.some(s => s.exists && (s.data().ck !== doc.ck || s.data().sendId !== doc.sent.id))) return { error: "A piece of this order already belongs to another custom sheet decision", status: 409 };
    const phase = old?.phase === "sent" ? "sent" : doc.phase, next = Object.assign({}, old || doc, { phase });
    let removalChanged = false;
    if (old) next.sent = Object.assign({}, old.sent, { lines: Object.fromEntries(Object.entries(old.sent.lines).map(([key, pcs]) => [key, pcs.map((pc, i) => {
      const removed = doc.sent.lines[key][i].removed;
      if (!removed || pc.removed) return pc;
      removalChanged = true; return Object.assign({}, pc, { removed });
    })])) });
    const events = phase === "sent" ? customSheetEvents(next) : [], eventRefs = events.map(e => db.collection(PREFIX + Timeline.COL).doc(e.key));
    const recorded = eventRefs.length ? await tx.getAll(...eventRefs) : [];
    if (!old || old.phase !== phase || removalChanged) {
      next.history = (old?.history || []).concat(old?.phase === "sent" || phase !== "sent" ? [] : [{ phase: "sent", id: next.sent.id, at: next.sent.at, by: next.sent.by }]);
      next.updatedAtMs = Date.now(); next.updatedAt = FV.serverTimestamp(); next.fingerprint = fingerprint;
      tx.set(ref, next);
    }
    for (const [i, s] of claimed.entries()) if (!s.exists) tx.set(lineRefs[i], { ck: next.ck, rid: next.rid, sendId: next.sent.id });
    for (const [i, e] of events.entries()) if (!recorded[i].exists) tx.set(eventRefs[i], Object.assign({}, e.doc, { createdAt: FV.serverTimestamp() }));
    return { ok: true, record: customSheetRow(next) };
  });
}
/* Older Send to Sheet saved its receipt only in that browser. Recover it from a recorded designSent decision plus
   the exact custom pool copies and uploaded sources, never from a placement, a sheet's date or a screenshot.
   Explicit candidate lines only: no scan of regular orders, no writes, and no inferred copies to put back on a sheet. */
async function legacyCustomSheets(lineKeys, known) {
  const lines = [...new Set(lineKeys.map(String).filter(k => lineKeyOk(k) && orderOfKey(k)))], candidates = lines.filter(k => !Object.values(known).some(r => r.sent?.lines && Object.prototype.hasOwnProperty.call(r.sent.lines, k))).slice(0, 40);
  const records = Object.create(null), events = new Map(), sheets = new Map(), end = Date.now() + 3500;
  let timedOut = false, next = 0;
  async function read(task) {
    const left = end - Date.now(); if (left <= 0) { timedOut = true; return null; }
    let timer;
    try { return await Promise.race([task, new Promise(resolve => { timer = setTimeout(() => { timedOut = true; resolve(null); }, left); })]); }
    catch (_) { return null; } finally { clearTimeout(timer); }
  }
  const timelineFor = rid => {
    if (!events.has(rid)) events.set(rid, read(db.collection(PREFIX + Timeline.COL).where("orderId", "==", rid).where("type", "==", "designSent").limit(101).select("orderId", "type", "lineKey", "at", "by", "source", "station", "data").get()));
    return events.get(rid);
  };
  const sheetFor = id => {
    if (!sheets.has(id)) sheets.set(id, read(db.getAll(col(SHEETS).doc(id), { fieldMask: ["sources", "charms"] })).then(rows => rows?.[0] || null));
    return sheets.get(id);
  };
  async function recover(lineKey) {
    const rid = orderOfKey(lineKey), history = await timelineFor(rid);
    if (!history || history.size > 100 || Date.now() >= end) return;
    const decisions = history.docs.map(d => ({ id: d.id, ...d.data() })).filter(e => e.orderId === rid && e.type === "designSent" && e.lineKey === lineKey && num(e.at) > 1e12 && str(e.by, 80).trim() && (!e.source || e.source === "sorter"));
    if (!decisions.length) return;
    const snap = await read(col(POOL).where("lineKey", "==", lineKey).limit(301).get());
    if (!snap || snap.size > 300 || Date.now() >= end) return;
    const pool = snap.docs.map(d => ({ ...d.data(), poolId: d.id })).filter(p => p.custom === true && String(p.orderId) === rid && p.lineKey === lineKey).sort((a, b) => num(a.copy) - num(b.copy));
    if (!pool.length || pool.some((p, i) => p.copy !== i + 1 || p.poolId !== `${lineKey}_${i + 1}` || !/^charmnest\/(sandbox\/)?custom\//.test(p.aiPath || ""))) return;
    const sheetIds = [...new Set(pool.map(p => p.sheetId).filter(isId))];
    if (sheetIds.length > 40) return;
    const loaded = [];
    for (let i = 0; i < sheetIds.length && Date.now() < end; i += 8) loaded.push(...await Promise.all(sheetIds.slice(i, i + 8).map(sheetFor)));
    if (Date.now() >= end) return;
    const sheetRows = loaded.filter(d => d?.exists).map(d => d.data());
    for (const event of decisions.sort((a, b) => num(b.at) - num(a.at)).slice(0, 10)) {
      const metadata = event.data?.files, count = num(event.data?.pieces);
      if (!Array.isArray(metadata) || !metadata.length || !Number.isInteger(count) || count !== pool.length || pool.some(p => num(p.quantity) !== count)) continue;
      const paths = new Map(), files = [], mapping = [], seenCounts = new Map(); let valid = true, lastFile = -1;
      for (const p of pool) {
        const choices = metadata.map((f, i) => ({ f, i })).filter(({ f }) => f?.name === p.customFile && f.metal === p.material);
        if (choices.length !== 1) { valid = false; break; }
        const { f: original, i: fileNo } = choices[0], pieces = num(original.pieces), qty = num(original.qty);
        if (!Number.isInteger(pieces) || pieces < 1 || pieces > 10000 || !Number.isInteger(qty) || qty < 1 || qty > 99 || fileNo < lastFile || (paths.has(fileNo) && paths.get(fileNo) !== p.aiPath) || files.some(f => f.cloud.path === p.aiPath && f.legacyFileNo !== fileNo)) { valid = false; break; }
        paths.set(fileNo, p.aiPath); lastFile = fileNo;
        const index = (seenCounts.get(fileNo) || 0) % pieces;
        const descriptors = sheetRows.flatMap(sh => Array.isArray(sh.charms) ? sh.charms : []).filter(c => c?.poolId === p.poolId && c.custom === true), descriptor = descriptors[0];
        if (descriptors.some(c => c.aiPath && c.aiPath !== p.aiPath || Number.isInteger(c.index) && c.index !== index)) { valid = false; break; }
        let file = files.find(f => f.legacyFileNo === fileNo);
        if (!file) {
          const source = sheetRows.flatMap(sh => Array.isArray(sh.sources) ? sh.sources : []).find(src => src?.custom === true && src.path === p.aiPath && (!descriptor || src.id === descriptor.sourceId));
          // Resolve its current download URL through the existing output helper when opened; old signed URLs can expire.
          file = { id: "legacy-" + customSheetHash(p.aiPath).slice(0, 24), name: str(original.name, 200), kind: /\.dxf$/i.test(original.name) ? "dxf" : "ai", size: Math.max(0, num(source?.bytes)), hash: str(source?.hash, 100), cloud: { path: p.aiPath, url: "" },
            metal: p.material, qty, pieces, wMm: 0, hMm: 0, maxPt: 0, minPt: 0, maxAreaPt2: 0, state: "ready", legacyFileNo: fileNo };
          files.push(file);
        }
        if (descriptor) { file.maxPt = Math.max(file.maxPt, num(descriptor.widthPt), num(descriptor.heightPt)); file.minPt = Math.max(file.minPt, Math.min(num(descriptor.widthPt), num(descriptor.heightPt))); file.maxAreaPt2 = Math.max(file.maxAreaPt2, num(descriptor.areaPt2)); }
        const pc = { f: file.id, i: index }; if (num(p.removedAt) > 0) pc.removed = str(p.removedReason, 120) || true;
        mapping.push(pc); seenCounts.set(fileNo, (seenCounts.get(fileNo) || 0) + 1);
      }
      if (!valid || files.some(f => (seenCounts.get(f.legacyFileNo) || 0) % f.pieces || (seenCounts.get(f.legacyFileNo) || 0) / f.pieces > f.qty || !isMetal(f.metal))) continue;
      files.forEach(f => { delete f.legacyFileNo; });
      const ck = `legacy-custom:${lineKey}:${num(event.at)}`, sent = { id: event.data?.decisionId || event.id, at: num(event.at), by: str(event.by, 80).trim(), lines: { [lineKey]: mapping } };
      records[ck] = { ck, rid, at: sent.at, phase: "sent", legacy: true, files, sent, history: [{ phase: "sent", id: sent.id, at: sent.at, by: sent.by }] };
      return;
    }
  }
  await Promise.all(Array.from({ length: Math.min(4, candidates.length) }, async () => { while (next < candidates.length && Date.now() < end) await recover(candidates[next++]); }));
  return { records, truncated: timedOut || next < candidates.length || lines.length > 40 };
}
async function op_customSheetGet(b) {
  if (!Array.isArray(b.keys) && !Array.isArray(b.lineKeys) && !Array.isArray(b.legacyLineKeys)) return { error: "known custom card keys or piece keys required" };
  const asked = [...new Set((Array.isArray(b.keys) ? b.keys : []).filter(customSheetKeyOk))], lines = [...new Set((Array.isArray(b.lineKeys) ? b.lineKeys : []).map(String).filter(lineKeyOk))];
  const keys = new Set(asked.slice(0, 1000));
  for (let i = 0; i < Math.min(lines.length, 1000); i += 100) {
    const snaps = await db.getAll(...lines.slice(i, Math.min(i + 100, 1000)).map(customSheetLineRef));
    for (const s of snaps) if (s.exists && customSheetKeyOk(s.data().ck)) keys.add(s.data().ck);
  }
  const records = Object.create(null), budget = answerBudget(), list = [...keys].slice(0, 1000); let read = 0;
  for (let i = 0; i < list.length; i += 50) {
    const snaps = await db.getAll(...list.slice(i, i + 50).map(customSheetRef));
    for (const [j, s] of snaps.entries()) {
      const r = s.exists ? customSheetRow(s.data()) : null;
      if (r && !budget.fits(r)) return { records, keys: read, truncated: true };
      read++; if (r && r.ck === list[i + j]) records[r.ck] = r;
      if (budget.late()) return { records, keys: read, truncated: read < list.length || keys.size > 1000 || lines.length > 1000 };
    }
  }
  const legacy = Array.isArray(b.legacyLineKeys) ? await legacyCustomSheets(b.legacyLineKeys, records) : null;
  if (legacy) Object.assign(records, legacy.records);
  return { records, keys: read, truncated: keys.size > 1000 || asked.length > 1000 || lines.length > 1000, ...(legacy ? { legacyTruncated: legacy.truncated } : {}) };
}

/* how: "print" (the QR label was printed, the default) or "button" (Complete Order, 27 Sep: completed with no label
   printed, so no print is counted). Either records who completed it and when (completedAt/completedBy).
   stamps (Paul, 27 Sep 20:09-20:18): one seal per press, in order: { how: "print" | "button", at, by }. Every seal is
   kept (29 Sep: none is ever dropped; the cap only guards the document's size, far past any order's presses).
   The card shows each as a seal of its own (a print seal or a Complete Order seal). A record from before them has its
   seals read from what it kept (the completion, the first and the last print). */
const legacyStamps = c => {
  if (!c) return [];
  const out = [];
  if (c.how === "button" && c.completedAt) out.push({ how: "button", at: +c.completedAt, by: c.completedBy || "" });
  if (c.printedAt) out.push({ how: "print", at: +c.printedAt, by: c.printedBy || "" });
  if (+c.prints > 1 && c.lastPrintedAt && +c.lastPrintedAt !== +c.printedAt) out.push({ how: "print", at: +c.lastPrintedAt, by: c.lastPrintedBy || "" });
  return out.sort((a, b) => a.at - b.at);
};
async function op_customPut(b) {
  const key = String(b.key || ""); if (!lineKeyOk(key)) return { error: "key (the piece's receipt_transaction) required" };
  const label = b.label && typeof b.label === "object" ? b.label : null, labelJson = label ? JSON.stringify(label) : "";
  const now = Date.now(), ref = col(CUSTOM).doc(key), snap = await ref.get(), cur = snap.exists ? snap.data() : null;
  const button = b.how === "button", who = str(b.by || "operator", 80);
  const doc = { key, receiptId: str(b.receiptId, 40), transactionId: str(b.transactionId, 40), sku: str(b.sku, 60), title: str(b.title, 200),
    category: str(b.category, 60), kind: str(b.kind, 40), state: "completed", updatedAtMs: now, updatedAt: FV.serverTimestamp() };
  if (!cur || cur.state !== "completed" || !cur.completedAt) Object.assign(doc, { completedAt: now, completedBy: who, how: button ? "button" : "print" });
  if (!button) {
    Object.assign(doc, { lastPrintedAt: now, lastPrintedBy: who, prints: ((cur && +cur.prints) || 0) + 1 });
    if (!cur || !cur.printedAt) Object.assign(doc, { printedAt: now, printedBy: who });
  }
  if (labelJson && labelJson.length <= 20000) doc.label = labelJson;
  doc.hasLabel = !!(doc.label || (cur && cur.label));
  const prev = cur && Array.isArray(cur.stamps) ? cur.stamps : legacyStamps(cur);
  doc.stamps = prev.concat({ how: button ? "button" : "print", at: now, by: who }).slice(-STAMPS_MAX);
  await ref.set(doc, { merge: true });
  // the new seal on the order's timeline, as the record keeps it (its time is its id: the same seal is one event)
  await stamp(() => ({ orderId: doc.receiptId || orderOfKey(key), type: button ? "sealCompleted" : "sealPrinted", at: now, by: who, station: "sorter", lineKey: key, transactionId: doc.transactionId || key.split("_")[1] || "",
    text: [doc.sku, button ? "Complete Order" : `print ${doc.prints}`].filter(Boolean).join(" · "), data: { how: button ? "button" : "print", prints: doc.prints || (cur && +cur.prints) || 0, completed: !!doc.completedAt, sku: doc.sku, title: str(doc.title, 120), ...pressedIn(b) }, id: `${key}-${now}` }), "custom seal");
  return { ok: true, record: customRow(Object.assign({}, cur || {}, doc), false) };
}
const STAMPS_MAX = 2000;
// where a Complete Order or a Reopen was pressed (the order window, a Review card: the page says so), for its point on
// the order's timeline (Paul, 29 Sep 02:08)
const pressedIn = b => (b && b.from ? { pressedIn: str(b.from, 40) } : {});
/* Reopen or Undo (Paul, 29 Sep 00:35: "the seals can never ever disappear… even though you can reopen an order, the
   seal must always remain and follow that order forever"). The record is never deleted and its seals never cleared: its
   state becomes "open" (the sorter reads the line as not completed), who and when join its history, and the order's
   timeline gets a note (drawn there as a Reopen point of its own, beside the Complete point it never takes away). A record from before the stamps has its seals written out from what it kept, so a
   later completion cannot take them. One already open is left as it is (a retry records nothing again). */
async function op_customReopen(b) {
  const key = String(b.key || ""); if (!lineKeyOk(key)) return { error: "bad key" };
  const ref = col(CUSTOM).doc(key), snap = await ref.get(); if (!snap.exists) return { ok: true, record: null };
  const cur = snap.data(); if (cur.state === "open") return { ok: true, record: customRow(cur, false) };
  const how = b.how === "undo" ? "undo" : "reopen", who = str(b.by || "operator", 80), now = Date.now();
  const doc = { state: "open", reopenedAt: now, reopenedBy: who, updatedAtMs: now, updatedAt: FV.serverTimestamp(),
    history: (Array.isArray(cur.history) ? cur.history : []).concat(Object.assign({ how, at: now, by: who }, b.from ? { from: str(b.from, 40) } : {})).slice(-STAMPS_MAX) };
  if (!Array.isArray(cur.stamps)) doc.stamps = legacyStamps(cur);
  await ref.set(doc, { merge: true });
  const seals = (doc.stamps || cur.stamps || []).length, what = cur.sku || cur.title || "custom piece";
  await stamp(() => ({ orderId: cur.receiptId || orderOfKey(key), type: "note", at: now, by: who, station: "sorter", lineKey: key, transactionId: cur.transactionId || key.split("_")[1] || "",
    text: `Custom order ${how === "undo" ? "completion undone" : "reopened"} by ${who}: ${str(what, 80)} · back to Open (its ${seals === 1 ? "seal stays" : seals + " seals stay"})`,
    data: { reopened: how, seals, sku: cur.sku || "", ...pressedIn(b) }, id: `customReopen-${key}-${now}` }), "custom reopen");
  return { ok: true, record: customRow(Object.assign({}, cur, doc), false) };
}
/* Is a line a custom order? Claude's kept readings (only those read from exactly what the page has now) and a person's
   decisions (_charmNestCustomRead). A reading is shared by production and the sandbox (the sandbox plays the real orders
   under their real numbers); a person's decision is kept per workspace. A new reading is a customRead job (startAgent). */
async function op_customReadGet(b) { return require("./_charmNestCustomRead").lookup(db, b.items, !!PREFIX); }
async function op_customDecide(b) { return require("./_charmNestCustomRead").decide(db, FV, b, !!PREFIX); }

const RoseStock = require("./_charmNestRoseStock")({db,col,FV,Readiness,decisionsOfRun,productionReadiness,stamp,sheetLabel});
/* ── cancelled orders (Paul, 25 Sep 19:05): an order the operator cancels leaves every screen of the sorter, and one
   record of it is kept here as history. The sorter reads the ids to keep such an order out of every later pull.
   Since 28 Sep (A1 · A6) Etsy's own cancels land in the same record (by "Etsy", source "etsy", etsyStatus), written by
   the inbox's receipts mirror; every writer goes through _orderCancel.js, which also stamps the order's timeline. ── */
const CANCELLED = "Charm_Nest_Cancelled";
const OrderCancel = require("./_orderCancel");
const orderIdOf = v => String(v == null ? "" : v).replace(/\D/g, "").slice(0, 30);
/* source: "sorter" (a person cancelled it, the default) or "etsy" (the sorter saw Etsy had cancelled it; the record then
   says by "Etsy" and the person is kept on the timeline event). etsyStatus: Etsy's word, when known. A record already
   there is never lost: see _orderCancel.js (a person's stays over a later Etsy detection, which only adds etsyStatus). */
async function op_cancelPut(b) {
  const id = orderIdOf(b.orderId); if (!id) return { error: "orderId required" };
  const r = b.record && typeof b.record === "object" ? b.record : {}, who = str(b.by || "operator", 80);
  // at: the moment the person pressed Cancel (the page's clock, within the last day), else now (Paul, 29 Sep 00:26)
  const pressed = num(b.at), now = Date.now(), at = pressed > now - 864e5 && pressed <= now + 6e4 ? Math.min(Math.round(pressed), now) : now;
  const rec = OrderCancel.record({ orderId: id, by: who, why: str(b.why, 400), at, buyer: r.buyer, placedAt: r.placedAt, shipBy: r.shipBy, sheets: r.sheets, lines: r.lines,
    source: b.source === "etsy" ? "etsy" : "sorter", etsyStatus: str(b.etsyStatus || r.etsyStatus, 40) });
  const out = await OrderCancel.put(db, FV, rec, { prefix: PREFIX, person: who, detectedBy: "sorter", eventId: str(b.eventId, 80) });
  return out.error ? out : { ok: true, record: out.record, created: out.created, kept: out.kept };
}
/* What was written since a read (wave 4: the orders check read every record each time, AutoCancel its newest 50 every 2
   minutes): `after` is where the last read stopped, the answer's `cursor` where this one did, `more` that a page is left.
   In the order written, by createdAt (every writer sets it; a cancel made again after a restore sets it anew; its own
   single-field index) and then by id, so a batch of the mirror's, all one createdAt, pages through with nothing skipped.
   Not `at`: an Etsy cancel's `at` is when Etsy changed the receipt, often long before its record is written (the sweep).
   A restore deletes its record, which no read of what was written sees: `total` (a count) with the ids lets the reader
   see the collection hold fewer than it knows, and read the whole set again. `track` on a whole read asks for the cursor
   (the newest written, taken first: a record written meanwhile is read next time) and, with idsOnly, the total. */
const cursorOf = d => { const t = d && (d.data() || {}).createdAt; return t && typeof t.seconds === "number" ? { s: t.seconds, n: t.nanoseconds || 0, id: d.id } : null; };
const countOf = () => col(CANCELLED).count().get().then(s => s.data().count);
async function op_cancelList(b) {
  const n = Math.max(1, Math.min(500, Math.round(num(b.limit)) || 200));
  const tidyRec = d => { const x = d.data(); delete x.createdAt; if (!x.source) x.source = x.by === "Etsy" ? "etsy" : "sorter"; return x; };
  if (b.after !== undefined) {
    const a = b.after && typeof b.after === "object" ? b.after : {}, s0 = Math.round(num(a.s)), n0 = Math.round(num(a.n)), id0 = orderIdOf(a.id);
    if (!(s0 > 0) || !(n0 >= 0 && n0 < 1e9) || !id0) return { error: "bad cursor" };
    let q = col(CANCELLED).orderBy("createdAt").orderBy(docOrder()).startAfter(new admin.firestore.Timestamp(s0, n0), id0).limit(n);
    if (b.idsOnly) q = q.select("orderId", "createdAt");
    const [s, total] = await Promise.all([q.get(), b.idsOnly ? countOf() : null]);
    const out = { cursor: (s.size && cursorOf(s.docs[s.size - 1])) || { s: s0, n: n0, id: id0 }, more: s.size >= n };
    return b.idsOnly ? Object.assign(out, { ids: s.docs.map(d => d.id), total }) : Object.assign(out, { list: s.docs.map(tidyRec) });
  }
  const top = b.track ? await col(CANCELLED).orderBy("createdAt", "desc").orderBy(docOrder(), "desc").select("createdAt").limit(1).get() : null;
  const track = top ? { cursor: (top.size && cursorOf(top.docs[0])) || { s: 1, n: 0, id: "0" } } : {};
  // (newest first: past 5000 records, now that Etsy's cancels are kept here too, the oldest are the ones left out, never
  //  the order a person cancelled today, which the orders check must keep out of the pull)
  if (b.idsOnly) {
    const [s, total] = await Promise.all([col(CANCELLED).orderBy("at", "desc").select("orderId").limit(5000).get(), top ? countOf() : null]);
    return Object.assign({ ids: s.docs.map(d => d.id), truncated: s.size >= 5000 }, top ? Object.assign(track, { total }) : {});
  }
  let cq=col(CANCELLED).orderBy("at",b.direction==='asc'?'asc':'desc');
  const dates=Activity.bounds(b.range);
  if(dates)cq=cq.where('at','>=',Activity.dayStart(dates[0])).where('at','<',Activity.dayStart(Activity.shift(dates[1],1)));
  const s = await cq.limit(n).get();
  // a record from before `source` was kept reads as a person's (or Etsy's, when it says so)
  return Object.assign({ list: s.docs.map(tidyRec), truncated: s.size >= n }, track);
}
/* The one-off sweep (by hand: POST {op:"cancelSweep", dryRun?}): every receipt the inbox's mirror keeps as cancelled
   (EtsyMail_Receipts, one equality query per status) gets its record, for the cancels older than the mirror's watermark.
   Idempotent; answers more:true and `next` when its time ran out: it is called again with cursor: next until more is
   false, each call going on from where the last stopped. An order that would not write still answers 200: it is listed in
   `failures` and kept in the cancel backlog for the mirror's retry. A page that will not read (tried 3 times) answers
   200 too: ok:false, `readError`, the counts so far, and `next` at that page. Production only. */
async function op_cancelSweep(b) {
  if (PREFIX) return { error: "the sweep fills the real orders' cancel records; in the sandbox use sandboxCancel" };
  return OrderCancel.sweep(db, FV, { dryRun: b.dryRun === true || b.dryRun === 1 || b.dryRun === "1", budgetMs: 7000, cursor: b.cursor, docId: docOrder() });
}
/* The sandbox's pretend Etsy cancel ({orderId, etsyStatus?}): the order is cancelled in the Sandbox_ records exactly as
   the mirror would record Etsy's cancel (by "Etsy", source "etsy"), so the sorter's and the stations' handling of it can
   be tried with no real order touched. What was ordered is read from the inbox's mirror of the order when it has one
   (the sandbox plays real orders under their real numbers), else from `record`. Never writes production. */
async function op_sandboxCancel(b) {
  const id = orderIdOf(b.orderId); if (!id) return { error: "orderId required" };
  const snap = await db.collection(OrderCancel.RECEIPTS).doc(id).get().catch(() => null);
  const status = str(b.etsyStatus, 40) || "Canceled", now = Date.now();
  const rec = snap && snap.exists ? Object.assign(OrderCancel.fromReceipt(Object.assign({}, snap.data(), { receipt_id: id, status })), { at: now, etsyAt: now })
    : (r => OrderCancel.record({ orderId: id, source: "etsy", at: now, etsyStatus: status, buyer: r.buyer, placedAt: r.placedAt, shipBy: r.shipBy, sheets: r.sheets, lines: r.lines }))(b.record && typeof b.record === "object" ? b.record : {});
  const out = await OrderCancel.put(db, FV, rec, { prefix: "Sandbox_", person: str(b.by, 80), detectedBy: "sandbox" });
  return out.error ? out : { ok: true, sandbox: true, record: out.record, created: out.created, kept: out.kept };
}
/* What became of a cancelled order's pieces, sheet by sheet ({orderId, fates:[{sheet, fate: "removed"|"cut", text}]}): the
   sorter's AutoCancel and its sheet window write it on the record (the Cancelled tab reads it); see _orderCancel.noteFates. */
// (removals: [{ id, at, where, kind, outcome, by, text, lineKey, station }], see _orderCancel.noteRemovals; by: who)
async function op_cancelFates(b) {
  const out = await OrderCancel.noteFates(db, b.orderId, b.fates, { prefix: PREFIX, by: str(b.by, 80), removals: Array.isArray(b.removals) ? b.removals : [] });
  if (out && out.ok && !out.missing) await cancelSteps(b);
  return out;
}
/* Each fate on the order's timeline as its own step (cancelStep; Paul, 29 Sep 00:26): "On a cut sheet: set aside (GF
   Sheet 2)", "Still on SS Sheet 3: not taken off yet". A removal is the pieces' own `removed` event (poolEvents), so a
   "removed" fate is written only over a step of that sheet already there (it said "still on", it is off now). Ids are
   the cancel's time and the place (Timeline.stepId): the next check or a retry says how the same step stands now. */
async function cancelSteps(b) {
  const id = orderIdOf(b.orderId), fates = (Array.isArray(b.fates) ? b.fates : []).filter(f => f && f.sheet).slice(0, 30);
  if (!id || !fates.length) return 0;
  let cancelAt = num(b.cancelAt);
  if (!(cancelAt > 1e12)) { try { const s = await col(CANCELLED).doc(id).get(); cancelAt = s.exists ? num(s.data().at) : 0; } catch (_) { cancelAt = 0; } }
  if (!(cancelAt > 1e12)) return 0;
  const TL = require("./_orderTimeline"), ev = f => { const st = TL.cancelStepOf(f); return { orderId: id, type: "cancelStep", at: Date.now(), by: str(f.by || b.by, 80), station: "sorter", device: str(b.device, 40), sheet: str(f.sheet, 80), id: TL.stepId(cancelAt, f.sheet), text: st.text, data: { outcome: st.outcome, done: st.done, sheet: str(f.sheet, 80), cancelAt, note: str(f.text, 160) } }; };
  const off = fates.filter(f => f.fate === "removed"), rest = fates.filter(f => f.fate !== "removed");
  if (off.length) {
    try {
      const refs = off.map(f => { const c = TL.clean(ev(f), { prefix: PREFIX }); return c ? db.collection(PREFIX + TL.COL).doc(c.key) : null; });
      const got = await db.getAll(...refs.filter(Boolean));
      const had = new Set(got.filter(d => d.exists).map(d => d.id));
      off.forEach((f, i) => { if (refs[i] && had.has(refs[i].id)) rest.push(f); });
    } catch (e) { console.warn("[charmNestLibrary] cancel steps not read:", e.message || e); }
  }
  return stamp(() => rest.map(ev), "cancel steps");
}
/* ── the order timeline (_orderTimeline.js): the sorter's own events, and the whole timeline of one order ── */
const Timeline = require("./_orderTimeline");
async function op_timelineAdd(b) { return Timeline.add(db, FV, b.events, { prefix: PREFIX, source: "sorter" }); }
async function op_timelineGet(b) { return Timeline.get(db, b.orderId, { prefix: PREFIX, sandboxed: SANDBOXED, derive: b.derive !== false }); }   // recorded + derived from the records already kept
// (full: each whole record, its lines, fates and removals, for an order read long after it left the pull)
async function op_cancelCheck(b) { return Timeline.cancelCheck(db, b.orderIds || b.orderId, { prefix: PREFIX, full: b.full === true }); }
/* Restoring a cancelled order deletes its cancel record; the timeline keeps it first: the cancelRestored event carries the
   record (who cancelled it, when and why), so the cancel still shows. Its id is the cancel's own time: once per cancel.
   The whole record (its lines, fates and removals, which the event's 2 KB may not hold) is kept for good in
   Charm_Nest_Cancelled_History/{id}~{at}, in the same batch as the delete: no restore loses it. */
const CANCEL_HISTORY = "Charm_Nest_Cancelled_History";
async function op_cancelRestore(b) {
  const id = orderIdOf(b.orderId); if (!id) return { error: "orderId required" };
  const ref = col(CANCELLED).doc(id);
  let rec = null; try { const s = await ref.get(); rec = s.exists ? s.data() : null; } catch (e) { console.warn("[charmNestLibrary] cancel record not read for the timeline:", e.message || e); }
  if (rec) await stamp(() => { const c = cancelCopy(rec); return { orderId: id, type: "cancelRestored", by: str(b.by, 80) || "operator", station: "sorter", sheet: str((c.sheets || []).join(", "), 80),
    text: `Was cancelled${c.by ? " by " + c.by : ""}${c.why ? ": " + c.why : ""}`, data: { cancelled: c }, id: String(num(c.at) || "record") }; }, "cancel restored");
  const batch = db.batch();
  if (rec) batch.set(col(CANCEL_HISTORY).doc(`${id}~${Math.round(num(rec.at)) || Date.now()}`), Object.assign({}, rec, { restoredAt: Date.now(), restoredBy: str(b.by, 80) || "operator" }));
  batch.delete(ref); await batch.commit(); return { ok: true };
}
/** A cancel record small enough for an event's data (≤ 2 KB): long titles are shortened, then dropped, then lines left out. */
function cancelCopy(r) {
  const x = {}; for (const [k, v] of Object.entries(r || {})) if (k !== "createdAt") x[k] = v && typeof v.toMillis === "function" ? v.toMillis() : v;
  const fits = () => JSON.stringify(x).length <= 1900, lines = () => Array.isArray(x.lines);
  if (!fits()) x.why = str(x.why, 300);
  if (!fits() && lines()) x.lines = x.lines.map(l => Object.assign({}, l, { title: str(l && l.title, 40) }));
  if (!fits() && lines()) x.lines = x.lines.map(l => { const o = Object.assign({}, l); delete o.title; return o; });
  if (!fits() && Array.isArray(x.sheets)) x.sheets = x.sheets.slice(0, 6);
  // (the removals, where and how each went, stay before the lines: the whole record is in Charm_Nest_Cancelled_History)
  if (!fits() && Array.isArray(x.removals)) x.removals = x.removals.map(r => ({ where: str(r && r.where, 60), outcome: str(r && r.outcome, 12), at: num(r && r.at) }));
  if (!fits() && Array.isArray(x.fates)) delete x.fates;
  while (!fits() && lines() && x.lines.length) { x.lines.pop(); x.linesLeftOut = (x.linesLeftOut || 0) + 1; }
  while (!fits() && Array.isArray(x.removals) && x.removals.length) { x.removals.pop(); x.removalsLeftOut = (x.removalsLeftOut || 0) + 1; }
  if (!fits()) for (const k of Object.keys(x)) if (!["orderId", "by", "why", "at", "source", "etsyStatus", "etsyAt"].includes(k)) delete x[k];
  return x;
}
const OPS = { ...RoseStock, laserDone: op_laserDone, laserDoneList: op_laserDoneList, findSheets: op_findSheets, listingPhotos:op_listingPhotos, getShapeGuidance:op_getShapeGuidance, putShapeGuidance:op_putShapeGuidance, laserStatus:op_laserStatus, archiveEmptySheet: op_archiveEmptySheet, sheetPdf: op_sheetPdf, arrivalRecord: op_arrivalRecord, startAgent: op_startAgent, getAgent: op_getAgent, customReadGet: op_customReadGet, customDecide: op_customDecide, ping: op_ping, lookupCharms: op_lookupCharms, putCharms: op_putCharms, renameCharm: op_renameCharm, listCharms: op_listCharms, putSheet: op_putSheet, listSheets: op_listSheets, getSheet: op_getSheet, backPreview: op_backPreview, deleteSheet: op_deleteSheet, purgeHistory: op_purgeHistory, restoreSheet: op_restoreSheet, putCalibration: op_putCalibration, getCalibration: op_getCalibration, startJob: op_startJob, getJob: op_getJob, stopJob: op_stopJob,
  masterPutIndex: op_masterPutIndex, masterGet: op_masterGet, masterGetMany: op_masterGetMany, masterList: op_masterList, masterPatch: op_masterPatch, masterPutFile: op_masterPutFile, masterListFiles: op_masterListFiles, masterRemoveFile: op_masterRemoveFile, masterRemoveSku: op_masterRemoveSku, startMaster: op_startMaster,
  jobList: op_jobList, poolPut: op_poolPut, poolUpdate: op_poolUpdate, poolList: op_poolList, poolGet: op_poolGet, backPut: op_backPut, backInvalidate: op_backInvalidate, backList: op_backList, sandboxPut: op_sandboxPut, sandboxStatus: op_sandboxStatus, sandboxReset: op_sandboxReset, sandboxStream: op_sandboxStream,
  setAllocate: op_setAllocate, setUpdate: op_setUpdate, setGet: op_setGet, setList: op_setList, runPut: op_runPut, runArchive: op_runArchive, runGet: op_runGet, runList: op_runList, history: op_history, releaseGet: op_releaseGet, releasePut: op_releasePut, bridgeLog: op_bridgeLog,
  cancelPut: op_cancelPut, cancelList: op_cancelList, cancelRestore: op_cancelRestore, cancelSweep: op_cancelSweep, sandboxCancel: op_sandboxCancel, cancelFates: op_cancelFates, timelineAdd: op_timelineAdd, timelineGet: op_timelineGet, cancelCheck: op_cancelCheck,
  aliasGet: op_aliasGet, aliasPut: op_aliasPut, noDesignGet: op_noDesignGet, noDesignPut: op_noDesignPut, noDesignDelete: op_noDesignDelete, optionMapGet: op_optionMapGet, optionMapPut: op_optionMapPut,
  customSheetGet: op_customSheetGet, customSheetPut: op_customSheetPut, customGet: op_customGet, customPut: op_customPut, customReopen: op_customReopen, customDelete: op_customReopen };

/* ── sign-in time (Paul, 28 Sep 23:53; plans/sign-in-sessions.md part L): sessionsList {since, until, limit} is the
   sorter's read of Station_Sessions, one document per sign-in, which the stations write through firebaseOrders
   ({session}, station-session.js). Read-only, behind this function's gate. One range on startAt, newest first: its
   single-field index, no composite. Bounded: a span of 62 days at most and 1,000 sessions (`truncated` says more were
   there). A session still open whose heartbeat stopped 15 minutes ago is closed here, when read, at its last heartbeat
   (endReason "closed"); one still beating is `live`, its minutes counted to the server's `now`. The id a PIN login keeps
   can be the PIN itself, so no employee id leaves this op. ── */
const SESSIONS = "Station_Sessions", SESSION_GONE_MS = 15 * 60000, SESSION_SPAN_MS = 62 * 86400000;
function sessionRow(id, d, now) {
  const startAt = ms(d.startAt); if (!(startAt > 0)) return null;
  const lastSeenAt = Math.max(startAt, ms(d.lastSeenAt) || startAt);
  let endAt = ms(d.endAt) || null, endReason = endAt ? str(d.endReason, 20) || null : null, live = false, minutes;
  if (endAt) minutes = Number.isFinite(+d.minutes) && d.minutes !== null && d.minutes !== "" ? +d.minutes : (endAt - startAt) / 60000;
  else if (now - lastSeenAt > SESSION_GONE_MS) { endAt = lastSeenAt; endReason = "closed"; minutes = (lastSeenAt - startAt) / 60000; }
  else { live = true; minutes = (now - startAt) / 60000; }
  return { id: str(id, 120), person: str(d.person, 80), station: str(d.station, 20), device: str(d.device, 40), computerId: str(d.computerId, 64), computerLabel: str(d.computerLabel, 80),
    startAt, lastSeenAt, endAt, endReason, minutes: Math.max(0, Math.round(minutes * 10) / 10), live };
}
async function op_sessionsList(b) {
  const now = Date.now();
  const until = Math.min(num(b.until) > 0 ? num(b.until) : now + 60000, now + 86400000);
  const since = Math.max(num(b.since) > 0 ? num(b.since) : until - 7 * 86400000, until - SESSION_SPAN_MS);
  if (!(since < until)) return { error: "since must be before until" };
  const limit = Math.max(1, Math.min(1000, Math.floor(num(b.limit)) || 500));
  // the server's times are kept as milliseconds or as Firestore times, and one range never matches the other kind: both are read
  const TS = admin.firestore.Timestamp, ranges = [[since, until]];
  if (TS && typeof TS.fromMillis === "function") ranges.push([TS.fromMillis(since), TS.fromMillis(until)]);
  const snaps = await Promise.all(ranges.map(([a, z]) => db.collection(SESSIONS).where("startAt", ">=", a).where("startAt", "<", z).orderBy("startAt", "desc").limit(limit + 1).get()));
  const seen = new Set(), rows = [];
  for (const s of snaps) for (const d of s.docs) if (!seen.has(d.id)) { seen.add(d.id); const r = sessionRow(d.id, d.data() || {}, now); if (r) rows.push(r); }
  rows.sort((x, y) => y.startAt - x.startAt || (x.id < y.id ? -1 : x.id > y.id ? 1 : 0));
  return { sessions: rows.slice(0, limit), truncated: rows.length > limit, since, until, now, goneAfterMs: SESSION_GONE_MS };
}
OPS.sessionsList = op_sessionsList;

/* ── The Library's moves (charm-nest-flow.js, window.LibraryFlow; Paul, 3 Oct: drag a sheet or a set between In progress,
   Laser cutting and Completed, forwards and backwards). Where a sheet or set stands is read from its records, never stored:
   completed = laserDoneAt, Laser cutting = every check passed, In progress = the rest. What a move can do itself is
   written by the ops that already own it (laserDone: marking, process seals, op_laserStatus' recordProcessReadiness);
   the only new fact is a person's HOLD (`laserHold` on a sheet: {at, by, note}), which takes a ready sheet back to In
   progress without touching one approval, seal or cut record (Readiness.sheet: a held sheet is not included).
   · flowState  {sheetIds, setIds, move?}   read only: laserStatus' records and sets, each set's own record, and whether each run is open.
     With `move` ({kind, id, to:{set|newSet}, together?}) for a move into or out of a set it also answers the cardinal rule of a Set
     of Sheets (charm-nest-shared-orders.js: sheets that share a multi-piece order are always in ONE set): `shared`, the orders such a
     move would split (each with its pieces, the sheets on each side and any sheet that cannot change set, with the reason), and `group`,
     the sheets that must travel together. · sharedOrders {kind, id, to, together?}   the same answer alone (read only).
     flowApply has no membership step (hold / release / seal never change a sheet's set), so it has nothing to refuse for the rule;
     the writes that do change membership are the page's putSheet / setUpdate, and setUpdate refuses to record a set complete while a
     sheet that shares one of its multi-piece orders is outside it (the list of orders and sheets in the answer). A move whose plan
     the cloud says splits an order is refused by the page at commit, which plans again from these records first.
   · flowApply  {steps:[{type:"hold"|"release", sheetIds, note}], expect?, by}   all steps in ONE transaction: every sheet is read first and
     checked against `expect` ({sheetId:{held:bool}}, what the plan saw), so a sheet changed since the plan was made refuses the
     whole call and nothing is written. A repeat finds the sheets as asked and writes nothing (idempotent). Each change adds an
     entry to the sheet's flowHistory (append only, newest 100) and a note on each of its orders' timelines.
   · flowApply  {steps:[{type:"seal", kind:"sheet"|"set", id}], by}   records the readiness seal by the person, exactly as laserStatus
     does for the active view (recordProcessReadiness: only when the group is ready and has none yet; history is never replaced).
   · A set advances as ONE (round 7): flowState also answers `gates` ({setId: {ready, reason, blockers, toApprove}}, Readiness.setGate over
     each set of several sheets), and flowApply REFUSES (409, nothing written, `blockers` and `setId` in the answer) a release or a seal
     for a sheet of such a set while any sheet of it is not ready to be approved; a hold, a step that only restores what a failed move
     changed (`restore: true`) and a sheet in no set or in a set of one are as before. ── */
async function op_flowState(b) {
  const sheetIds = [...new Set((b.sheetIds || []).filter(isId))].slice(0, 300), askedSets = [...new Set((b.setIds || []).filter(isId))].slice(0, 100);
  const status = await op_laserStatus({ sheetIds, setIds: askedSets, recordSeals: false });
  const ids = [...new Set(askedSets.concat((status.sets || []).map(x => x.setId)))].filter(isId).slice(0, 200), docs = [];
  for (let i = 0; i < ids.length; i += 100) for (const d of await db.getAll(...ids.slice(i, i + 100).map(id => col(SETS).doc(id)))) {
    if (!d.exists) continue;
    const x = d.data();
    docs.push({ setId: d.id, seq: num(x.seq) || null, day: x.day || null, name: x.name || null, runId: x.runId || null, status: x.status || null, committedAt: ms(x.committedAt) || num(x.committedAt) || null, sheetIds: x.sheetIds || [], materials: x.materials || [], laserDoneAt: num(x.laserDoneAt) || null, laserDoneBy: x.laserDoneBy || null, processReady: !!x.processReady, processSeals: Readiness.processStamps(x) });
  }
  const runIds = [...new Set((status.sheets || []).map(x => x.runId).concat(docs.map(x => x.runId)).filter(isId))].slice(0, 100), runs = {};
  for (const id of runIds) { const r = await col(RUNS).doc(id).get(); runs[id] = { exists: r.exists, open: r.exists && !["complete", "abandoned"].includes(String((r.data() || {}).status || "")) }; }
  // the cardinal rule of a Set of Sheets (below): for a move into a set, the orders it would split, read from the records
  const mv = b.move && typeof b.move === "object" ? b.move : null;
  const shared = mv && mv.to && (mv.to.set || mv.to.newSet) ? await sharedAnswer(mv) : null;
  // a set of several sheets advances as one: its gate (Readiness.setGate) from these same records, so the page can say why none of it is approved
  const gates = {};
  for (const x of docs) if ((x.sheetIds || []).length > 1) { const g = Readiness.setGate({ ...x, name: x.name || (x.seq ? "Set " + x.seq : "") }, (status.sheets || []).filter(r => r.setId === x.setId)); gates[x.setId] = { ready: g.ready, reason: g.reason, blockers: g.blockers, toApprove: g.toApprove }; }
  return { sheets: status.sheets || [], sets: status.sets || [], setDocs: docs, runs, gates, checkedAt: Date.now(), ...(shared ? { shared: shared.shared, group: shared.group } : {}) };
}
/* ── The cardinal rule of a Set of Sheets (Paul, 5 Oct 2026; charm-nest-shared-orders.js, window.SharedOrders, which this
   file shares): every sheet that shares a multi-piece order with another is in the SAME set. Read only here: the sheets that
   hold pieces of the moving sheets' orders (one query by order for each step outwards, a few steps at most), their sets for
   the reasons a sheet cannot change set (cut, completed, set committed), and the core's answer. `to` is {set} or {newSet}. ── */
const SharedRule = require("../../charm-nest-shared-orders.js").core;
const SHARED_FIELDS = ["id", "sheetId", "metal", "metalLabel", "page", "sheetIndex", "setId", "draft", "solidIncluded", "poolIds", "orders", "archived", "runId", "laserDoneAt", "roseCutAt", "fileBase"];
const sharedOrdersOf = d => [...new Set((Array.isArray(d.orders) ? d.orders.map(String) : []).concat((Array.isArray(d.poolIds) ? d.poolIds : []).map(orderOfKey)).filter(x => /^\d{1,30}$/.test(x)))];
/** The sheets that share orders with `seedIds`, outwards (a sheet that shares with a sheet that shares): { sheets: [core sheets], more: true when a cap cut it short }. */
async function sharedSheets(seedIds, { rounds = 4, cap = 500 } = {}) {
  const docs = new Map(), seenOrders = new Set();
  const take = (id, data) => { if (data && !data.archived && !docs.has(id)) { docs.set(id, { ...data, id }); return true; } return false; };
  let frontier = [];
  const ids = [...new Set(seedIds.filter(isId))].slice(0, 300);
  for (let i = 0; i < ids.length; i += 100) for (const d of await db.getAll(...ids.slice(i, i + 100).map(id => col(SHEETS).doc(id)), { fieldMask: SHARED_FIELDS })) if (d.exists && take(d.id, d.data())) frontier.push(docs.get(d.id));
  let more = false;
  for (let round = 0; round < rounds && frontier.length; round++) {
    const orders = [];
    for (const d of frontier) for (const o of sharedOrdersOf(d)) if (!seenOrders.has(o)) { seenOrders.add(o); orders.push(o); }
    frontier = [];
    for (let i = 0; i < orders.length; i += 30) {
      const snap = await col(SHEETS).where("orders", "array-contains-any", orders.slice(i, i + 30)).select(...SHARED_FIELDS).get();
      for (const d of snap.docs) if (take(d.id, d.data())) frontier.push(docs.get(d.id));
    }
    if (docs.size > cap) { more = true; break; }
  }
  if (frontier.length && !more) more = true;       // (the last step found sheets whose own orders were not followed)
  const setIds = [...new Set([...docs.values()].filter(d => d.setId && !d.draft && d.solidIncluded !== false).map(d => String(d.setId)).filter(isId))], sets = new Map();
  for (let i = 0; i < setIds.length; i += 100) for (const d of await db.getAll(...setIds.slice(i, i + 100).map(id => col(SETS).doc(id)), { fieldMask: ["setId", "seq", "name", "status", "committedAt", "laserDoneAt"] })) if (d.exists) sets.set(d.id, d.data());
  const sheets = [...docs.values()].map(d => SharedRule.sheetOf(d, { label: sheetLabel(d), fixed: SharedRule.fixedWhy(d, SharedRule.effectiveSet(d) ? sets.get(String(d.setId)) : null) })).filter(Boolean);
  return { sheets, more };
}
async function sharedAnswer(move) {
  const kind = move.kind === "set" ? "set" : "sheet", id = str(move.id, 80), to = move.to || {};
  const dest = to.newSet ? "new" : isId(to.set) ? String(to.set) : null;
  let seeds = [id];
  if (kind === "set") { if (!isId(id)) return { shared: [], group: { ids: [], labels: [], orders: [], fixed: [] } }; const s = await col(SETS).doc(id).get(); seeds = s.exists ? (s.data().sheetIds || []).map(String) : []; }
  else if (!isId(id)) return { shared: [], group: { ids: [id], labels: [], orders: [], fixed: [] } };
  const { sheets, more } = await sharedSheets(seeds), by = new Map(sheets.map(s => [s.id, s]));
  let moving = kind === "set" ? seeds : [id];
  if (move.together && kind === "sheet") moving = SharedRule.groupOf(sheets, id).ids;
  const items = SharedRule.between(sheets, moving, kind === "set" && !dest ? id : dest);
  const g = kind === "sheet" ? SharedRule.groupOf(sheets, id) : { ids: moving, orders: [] };
  return { shared: items, group: { ids: g.ids, labels: g.ids.map(i => (by.get(i) || {}).label || i), members: g.ids.map(i => ({ id: i, label: (by.get(i) || {}).label || i, setId: (by.get(i) || {}).setId || null })), orders: g.orders, fixed: g.ids.filter(i => by.get(i) && by.get(i).fixed).map(i => ({ sheetId: i, sheetLabel: by.get(i).label, why: by.get(i).fixed })), ...(more ? { more: true } : {}) } };
}
/** The orders of a set's sheets that also have pieces on a sheet outside it that COULD still join it (not cut, not in a committed set). */
async function splitOfSet(setId, memberIds) {
  const { sheets } = await sharedSheets(memberIds), items = SharedRule.between(sheets, memberIds, setId);
  const by = new Map(sheets.map(s => [s.id, s]));
  return items.filter(it => it.thereIds.some(i => by.get(i) && !by.get(i).fixed));
}
async function op_sharedOrders(b) {
  const to = b.to && typeof b.to === "object" ? b.to : {};
  return { ok: true, ...(await sharedAnswer({ kind: b.kind, id: b.id, to, together: !!b.together })) };
}
/* ── A set advances as ONE (Paul, 5 Oct 2026, round 7: "you cannot have a green approved button on a single sheet that is part of a set
   where the other sheets are not ready yet"). Readiness.setGate (charm-nest-readiness.js) is the one truth the page's buttons read:
   every sheet of a set of several ready to be approved (the lone button's hard test), or none is. This is its server twin: an
   approving step (a hold lifted, a ready seal) for a sheet of such a set is REFUSED, and nothing is written, while any sheet of
   the set is not, so a stale page cannot approve half a set. A set of one sheet and a sheet in no set are as before; so are the
   steps that go the other way (a hold) and a step that restores what a failed move had changed (`restore: true`). ── */
async function setGatesFor(setIds, known) {
  const ids = [...new Set((setIds || []).filter(isId))].slice(0, 50), out = new Map();
  for (let i = 0; i < ids.length; i += 100) for (const d of await db.getAll(...ids.slice(i, i + 100).map(id => col(SETS).doc(id)))) {
    if (!d.exists) continue;
    const doc = { setId: d.id, ...d.data() }, members = [...new Set((doc.sheetIds || []).filter(isId))];
    if (members.length < 2) continue;                                   // one sheet: its own gate, as before
    const sheets = new Map((known || []).filter(x => x.setId === d.id).map(x => [x.id, x])), missing = members.filter(m => !sheets.has(m));
    for (let j = 0; j < missing.length; j += 100) for (const r of await db.getAll(...missing.slice(j, j + 100).map(m => col(SHEETS).doc(m)), { fieldMask: SLIM_SHEET.concat(["placements"]) })) if (r.exists && !r.data().archived) sheets.set(r.id, { ...r.data(), id: r.id });
    const records = await readinessRecords([...sheets.values()]);
    out.set(d.id, { set: doc, gate: Readiness.setGate({ ...doc, name: doc.name || (doc.seq ? "Set " + doc.seq : "") }, records) });
  }
  return out;
}
/** null when every approving step in `steps` may go ahead, else the refusal ({error, status:409, blockers, set}). */
async function approveGate(steps) {
  const sheetIds = new Set(), setIds = new Set();
  for (const x of steps) {
    if (!x || x.restore === true) continue;
    if (x.type === "release") for (const i of (x.sheetIds || [])) sheetIds.add(str(i, 80));
    else if (x.type === "seal") (x.kind === "set" ? setIds : sheetIds).add(str(x.id, 80));
  }
  const ask = [...sheetIds].filter(isId);
  for (let i = 0; i < ask.length; i += 100) for (const d of await db.getAll(...ask.slice(i, i + 100).map(id => col(SHEETS).doc(id)), { fieldMask: ["setId", "draft", "solidIncluded", "archived"] })) {
    const x = d.exists ? d.data() : null;
    if (x && !x.archived && x.setId && !x.draft && x.solidIncluded !== false) setIds.add(String(x.setId));
  }
  for (const [id, { set, gate }] of await setGatesFor([...setIds], null)) if (!gate.ready) {
    return { error: `${gate.setLabel === "This set" ? "This set" : gate.setLabel} is approved together, so every sheet of it must be ready first: ${gate.reason}`, status: 409, blockers: gate.blockers, setId: id };
  }
  return null;
}
async function op_flowApply(b) {
  const by = str(b.by, 80).trim();
  if (!by) return { error: "Say who made this change", status: 400 };
  const steps = (Array.isArray(b.steps) ? b.steps : []).slice(0, 12);
  if (!steps.length) return { error: "no step to apply", status: 400 };
  const process = [], added = [], applied = [];
  const holds = steps.filter(x => x && (x.type === "hold" || x.type === "release")), seals = steps.filter(x => x && x.type === "seal");
  if (holds.length + seals.length !== steps.length) return { error: "unknown step", status: 400 };
  const device = str(b.device, 40).replace(/[^\w.-]/g, ""), via = str(b.via, 24).replace(/[^\w .-]/g, "").trim();
  const refused = await approveGate(steps); if (refused) return refused;     // (a set advances as one: nothing is written for half of it)
  let events = [];
  if (holds.length) {
    const at = Date.now(), want = new Map(holds.map(x => [x.type + ":" + str(x.note, 200), [...new Set((x.sheetIds || []).filter(isId))].slice(0, 300)]));
    const ids = [...new Set([...want.values()].flat())];
    if (!ids.length) return { error: "no sheet to change", status: 400 };
    const expect = b.expect && typeof b.expect === "object" ? b.expect : {};
    const res = await db.runTransaction(async tx => {
      const facts = new Map();
      for (let i = 0; i < ids.length; i += 100) for (const d of await tx.getAll(...ids.slice(i, i + 100).map(id => col(SHEETS).doc(id)))) facts.set(d.id, d.exists ? d.data() : null);
      const isHeld = d => num(d && d.laserHold && d.laserHold.at) > 0;
      for (const id of ids) {
        const f = facts.get(id);
        if (!f) return { error: "There is no such sheet", status: 404 };
        if (f.archived) return { error: "That sheet was repacked into other sheets: it is no longer in the Library", status: 409 };
        const e = expect[id];
        if (e && typeof e.held === "boolean" && e.held !== isHeld(f)) return { error: "The sheet changed since this move was planned: nothing was changed. Try the move again", status: 409 };
      }
      const changed = [];
      for (const [k, list] of want) {
        const type = k.slice(0, k.indexOf(":")), note = k.slice(k.indexOf(":") + 1);
        for (const id of list) {
          const f = facts.get(id);
          if ((type === "hold") === isHeld(f)) continue;                      // already as asked: a repeat writes nothing
          const entry = { id: `${type}-${at}-${id}`, at, by, type, note: note || null, setId: f.setId || null, laserDoneAt: num(f.laserDoneAt) || null };
          const history = (Array.isArray(f.flowHistory) ? f.flowHistory : []).concat([entry]).slice(-100);
          tx.set(col(SHEETS).doc(id), { laserHold: type === "hold" ? { at, by, note: note || null } : FV.delete(), flowHistory: history, updatedAt: FV.serverTimestamp() }, { merge: true });
          facts.set(id, { ...f, laserHold: type === "hold" ? { at, by, note } : null, flowHistory: history });
          changed.push({ id, type, d: f });
          process.push({ kind: "sheet", id, patch: { laserHold: type === "hold" ? { at, by, note: note || null } : null } });
        }
      }
      return { ok: true, changed, at };
    });
    if (res.error) return res;
    for (const c of res.changed) applied.push({ id: c.id, type: c.type });
    // one note on each order of each sheet that changed (the id carries the time, so a retry of the same call writes it once)
    events = res.changed.flatMap(c => {
      const sheet = sheetLabel(c.d), orders = [...new Set((Array.isArray(c.d.orders) && c.d.orders.length ? c.d.orders : (Array.isArray(c.d.poolIds) ? c.d.poolIds : []).map(orderOfKey)).filter(Boolean).map(String))].slice(0, 300);
      return orders.map(orderId => ({ orderId, type: "note", at: res.at, by, station: "laser", device, sheetId: c.id, sheet, setId: c.d.setId || "", text: c.type === "hold" ? `held back from Laser cutting · ${sheet}` : `released to Laser cutting · ${sheet}`, data: { flow: c.type, signedIn: true, via: via || undefined }, id: `flow-${c.type}-${c.id}-${res.at}` }));
    });
  }
  for (const x of seals) {
    const kind = x.kind === "set" ? "set" : "sheet", id = str(x.id, 80);
    if (!isId(id)) return { error: "bad sheet or set id", status: 400 };
    const r = await recordProcessReadiness(kind, id, by);
    process.push(...r.records); added.push(...r.added); applied.push({ id, type: "seal", kind });
  }
  if (events.length) await stamp(() => events, "library move");
  return { ok: true, applied, process, added, at: Date.now() };
}
OPS.flowState = op_flowState;
OPS.flowApply = op_flowApply;
OPS.sharedOrders = op_sharedOrders;
OPS.getOrderPieces = op_getOrderPieces;   // where each piece of an order is (OrderPieces); read only

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
