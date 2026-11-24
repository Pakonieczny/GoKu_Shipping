/*  netlify/functions/_charmEngraveFiles.js — ENGRAVING IN THE CLOUD: the fit, the back files and the sheet's back index (cloud plan v2, build step 11; lane "engrave").
 *  ═══════════════════════════════════════════════════════════════════════
 *  DORMANT. These are pass items of the cloud driver (_charmNestDrive.js, wired through _charmNestSteps.js). The driver runs them only while Charm_Nest_Open/drive is
 *  "standby" or "on", only for a run the cloud carries, and only while "engrave" is in drive.cloudSteps (P9: one lane, one place). With the drive absent or off nothing
 *  here is ever called, and every page approves, writes and indexes its engraving exactly as today.
 *
 *  What a page did, and where it is now:
 *    1. FIT (Engrave.fitAll / fitJobOnce + its worker, charm-nest-engrave-worker.js): a job whose words are read ("ready") and whose piece is on a sheet is fitted with
 *       CharmNestEngraveFit.calculate — the very function the worker runs — on the piece's charm made in node (_charmEngravePiece.js), in the fonts read from vendor/fonts
 *       and the shaping engine vendor/hb.wasm (_charmEngraveFonts.js). The buyer's asks are applied after an automatic fit (FOLLOWASK, applyAsks). The result is written to the
 *       line's engraving record (Charm_Nest_Engrave, engraveSyncPut with its revision): state "review", the words, the place. Approve stays a person's.
 *    2. BACK FILES (Engrave.saveBacks / writeBacks / writeBacksOnce): a person's Approve, sent as the press engraveApprove (_charmEngraveOps.js), is a TASK in
 *       Charm_Engrave_Files/<jobKey>@<approvedAt> with its own cursor. Piece by piece: the approved placement made again exactly (charm-nest-back-file.js rebuild),
 *       charm-nest-pdf.js buildBackFile, the file read again and checked (verifyBackFile), its picture (resvg), the two uploads (the tokens are kept in the task before the
 *       bytes go up, so a pass that dies after an upload never uploads again), backPut (the Charm_Pool_Back record, the sheet's backPool and the order's timeline event, written
 *       once per approval), then poolUpdate (the pieces engraved), then the approval itself on the line's record (SEALAFTERSAVE: its seal and state only once the file is saved).
 *       A job approved before its piece reached a sheet (ENGPRE) gets its task when the piece is on one (resumeBacks).
 *    3. BACK INDEX (Engrave.scheduleBackOutputs / sheetBackOutputs): back/back-index.pdf and back/back-report.json (and the front ones) of every sheet a task touched,
 *       then putSheet { backOutputs, frontOutputs }.
 *  A newer approval of the same job supersedes the older task; an artwork edit made after the fit the person approved fails the task as engravingInvalid and the job goes back
 *  to Review with the reason, the back approved before staying in the files. Every write is the cloud's (Lib.call, fenced on the pass's lease and the run's carrier); the task
 *  documents are written in transactions that read that same fence first; Storage is written in-process (never through charmNestOutput's door) after the fence is read again.
 *  Nothing here calls Etsy, its image CDN, the world build or an agent.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const crypto = require("crypto");
const O = require("../../charm-nest-orders.js");
const BF = require("../../charm-nest-back-file.js");
const Sides = require("../../charm-nest-engrave-sides.js");
const Faces = require("../../charm-nest-engrave-faces.js");
const Seals = require("../../charm-nest-engraving-seals.js");

const TASKS = "Charm_Engrave_Files", WORLD_LINES = "Charm_World_Lines", CURSOR = ["Charm_Nest_Drive", "engrave"];
const ACTIVE = ["waiting", "working"];
const PT = 72 / 25.4, MM = 25.4 / 72;
/* The page's DEFAULTS for what these units read (charm-nest-1.html), under the shop's shared settings (Charm_Nest_Shared/sorter-settings). */
const DEFAULTS = { engraveMarginMm: 0.09, engraveMinCapMm: 1.6, engraveMaxHeightFrac: 0.4, engraveTryRotated: "on", engraveMinGapMm: 0.12, engraveMinStrokeMm: 0.15, backFileView: "asSeenFromBack",
  minPt: 6, silhouetteRes: 6, skuPattern: "^[A-Z0-9][A-Z0-9 _.,'&()+\\-]{1,60}$" };
const STALE_FITTING_MS = 5 * 60000, RESUME_AFTER_MS = 2 * 60000, MAX_ERRORS = 5, WAIT_DAYS_MS = 3 * 86400000;

const num = v => (Number.isFinite(+v) ? +v : 0);
const turn = () => new Promise(r => (typeof setImmediate === "function" ? setImmediate(r) : setTimeout(r, 0)));
const fencedErr = why => Object.assign(new Error(why), { fenced: true });
const invalidErr = why => Object.assign(new Error(why), { engravingInvalid: true });
const pendingErr = why => Object.assign(new Error(why), { engravingPending: true });
const uuid = () => (crypto.randomUUID ? crypto.randomUUID() : crypto.randomBytes(16).toString("hex"));
const isFrontJob = j => Faces.faceOf(j) === "front";
const pieceIndexOf = slot => { const m = /^D(\d+)(?:\.\d+)?$/.exec(String(slot || "")); return m ? +m[1] - 1 : null; };
const wordsOf = l => (Array.isArray(l) ? l : []).join(" ").replace(/\s+/g, " ").trim();
const torontoTime = at => { try { return new Date(at).toLocaleString("en-CA", { timeZone: "America/Toronto" }); } catch (_) { return new Date(at).toISOString(); } };

/* ── the fence (§1.3): the pass's lease is still this pass's and running, and the run is still the cloud's. Read first, inside every transaction of these units. ── */
async function fenceTx(ctx, tx) {
  const F = ctx.fence; if (!F || !F.path || !F.nonce) throw fencedErr("fenced: this unit has no fence, so it writes nothing");
  const seg = String(F.path).split("/"), lease = await tx.get(ctx.db.collection(seg[0]).doc(seg[1])), L = lease.exists ? lease.data() || {} : {};
  if (L.nonce !== F.nonce || L.state !== "running") throw fencedErr("fenced: another pass of the cloud holds the lease now; this write was not made");
  if (F.runId) { const cs = await tx.get(ctx.db.collection("Charm_Nest_Run_Carrier").doc(String(F.runId))), c = cs.exists ? (cs.data() || {}).carrier : null; if (!c || c.dev !== "cloud") throw fencedErr(`fenced: ${(c && c.by) || "a computer"} carries this run now; the cloud's write was not made`); }
}
const fenceCheck = ctx => ctx.db.runTransaction(tx => fenceTx(ctx, tx));
/** A task document written in a fenced transaction: fn(current) → the fields to merge (null: nothing). → the task as it now is. */
async function taskPut(ctx, id, fn) {
  const ref = ctx.db.collection(TASKS).doc(id);
  return ctx.db.runTransaction(async tx => {
    await fenceTx(ctx, tx);
    const s = await tx.get(ref), cur = s.exists ? Object.assign({ id }, s.data()) : null;
    const patch = fn(cur); if (!patch) return cur;
    const now = Date.now();
    tx.set(ref, Object.assign({}, patch, { updatedAt: now, beatAt: now, nonce: ctx.fence.nonce, worker: ctx.worker || "pass" }), { merge: true });
    return mergeDeep(Object.assign({}, cur || { id }), patch);
  });
}
function mergeDeep(a, b) { for (const [k, v] of Object.entries(b)) { if (v && typeof v === "object" && !Array.isArray(v) && a[k] && typeof a[k] === "object" && !Array.isArray(a[k])) a[k] = mergeDeep(Object.assign({}, a[k]), v); else a[k] = v; } return a; }

/* ── what one pass shares: the settings, the piece maker, the sheets and world lines read once ── */
const ENVS = new WeakMap();
async function envOf(ctx) {
  let E = ENVS.get(ctx.run); if (E) return E;
  let shared = {};
  try { const r = await ctx.read({ op: "shareGet", names: ["sorter-settings"] }), it = r && r.items && r.items["sorter-settings"]; shared = require("./_charmNestPieceHost").sharedValues(it && it.f); } catch (_) { shared = {}; }   // (each field as a value, never the store's field record)
  const settings = Object.assign({}, DEFAULTS, shared);
  const Pdf = require("./_charmNestPdf");
  const piece = require("./_charmEngravePiece").create({ db: ctx.db, read: ctx.read, settings, log: ctx.say, bucket: ctx.bucket });
  E = { settings, piece, P: piece.P, G: Pdf.Geom, Fonts: require("./_charmEngraveFonts"), sheets: new Map(), world: new Map() };
  E.bucket = () => ctx.bucket || require("./firebaseAdmin").storage().bucket();
  E.sheet = async id => { if (!id) return null; if (E.sheets.has(id)) return E.sheets.get(id); const r = await ctx.read({ op: "getSheet", id }); const s = (r && r.sheet) || null; if (s && !s.id) s.id = id; E.sheets.set(id, s); return s; };
  E.worldLine = async key => { if (E.world.has(key)) return E.world.get(key); let d = null; try { const s = await ctx.db.collection(WORLD_LINES).doc(key).get(); d = s.exists ? s.data() || null : null; } catch (_) { d = null; }
    /* WORLDDATA (Paul, 23 Nov 2026: order 4239999491, two discs approved 21 Nov, "its pieces are not made yet" ever since): the record keeps its reading as text (data), and it was read as if it were the record: no line had a spec here,
       so a line of discs had no Disc 1 and Disc 2, and no approval of one could find its piece. */
    if (d && typeof d.data === "string") { try { d = JSON.parse(d.data); } catch (_) { d = null; } }
    E.world.set(key, d); return d; };
  ENVS.set(ctx.run, E);
  return E;
}
/** The page's fitOpts (byHand: a person's own size, the ceiling four times the back's longer side, a little slide back onto the metal). */
function fitOpts(job, s, mask, byHand) {
  const o = { minCapMm: +s.engraveMinCapMm || 1.6, maxHeightFrac: +s.engraveMaxHeightFrac || 0.4, lineGap: job && job.lineGap != null ? +job.lineGap : 0.216, minStrokeMm: +s.engraveMinStrokeMm || 0, minGapMm: +s.engraveMinGapMm || 0, tryRotated: s.engraveTryRotated !== "off" };
  if (byHand && mask) { const h = mask.hPt || mask.h / mask.res, w = mask.wPt || mask.w / mask.res; o.maxHeightFrac = 4 * Math.max(w, h) / h; }
  if (byHand) o.slideMm = 1;
  return o;
}

/* ── the line and its pieces, from the records ── */
/** The run's line, the world's reading of it (its spec), its pool rows (loaded into the piece maker), and the row the slot rules read. null when the run has no such line. */
async function lineContext(ctx, E, lineKey, other) {
  const line = (ctx.run.lines || {})[lineKey] || other || null; if (!line) return null;
  const world = await E.worldLine(lineKey), ws = (world && world.spec) || {};
  const spec = Object.assign({}, ws, { designSku: line.sku || ws.designSku || null, quantity: line.quantity || ws.quantity || 1, pieceCount: line.pieceCount || ws.pieceCount || line.quantity || 1 });   // (WORLDDATA: what the run holds of the line wins, as it always did here)
  const row = { key: lineKey, state: line.state, poolIds: (line.poolIds || []).slice(), spec, line: Object.assign({}, (world && world.line) || {}, { quantity: spec.quantity || line.quantity || 1, transactionId: line.transactionId || String(lineKey).split("_")[1] }), order: { receiptId: line.orderId || String(lineKey).split("_")[0] } };
  if (row.poolIds.length) await E.piece.poolRows(row.poolIds);
  return { line, row, spec, world };
}
/** (ITEM 9, Paul 21 Nov 2026) An approved piece EDITED from the Library sheet preview whose line the run no longer holds (its order is cut and done, its sheet an earlier run's): the line as the
 *  edit names it, its one piece; the world's reading of the line still gives its words' setup. Only an edit's task reads it (a press of a line not in the run still ends with words). */
const editLine = (t, lineKey) => (t && t.origin === "edit" && t.edit && t.edit.poolId ? { state: "committed", sku: t.edit.sku || null, quantity: 1, poolIds: [t.edit.poolId], orderId: String(lineKey).split("_")[0], transactionId: String(lineKey).split("_")[1] } : null);
/** The pieces a job engraves: its slot's pieces of the line (the page's sideRow / ensureOne), less those a person took out of it (copyOverrides). */
function copiesOf(E, lc, job) {
  const slot = Faces.parseKey(job.key).slot;
  const ids = slot ? Sides.idsOfSlot(E.piece.sidesCtx(), lc.row, slot) : lc.row.poolIds.slice();
  return ids.filter(id => !(job.copyOverrides || []).includes(id));
}
/** The sheet a piece is on: its pool row's sheet, when that sheet's record still lists it (Placement's rule: the record is authoritative). */
async function sheetOf(E, poolId) {
  const p = E.piece.rows.get(poolId); if (!p || !p.sheetId || ["abandoned", "superseded"].includes(p.state)) return null;
  const sh = await E.sheet(p.sheetId);
  return sh && !sh.archived && Array.isArray(sh.poolIds) && sh.poolIds.includes(poolId) ? sh : null;
}
/** The folder a sheet's files are in (backFolderOf: its record's own .ai path). "" while the sheet has no saved files. */
const folderOf = sh => { const p = (sh && sh.outputs && sh.outputs.ai && sh.outputs.ai.path) || ""; return sh && sh.folderPath ? String(sh.folderPath) : p ? String(p).replace(/\/[^/]*$/, "") : ""; };
const artAtOf = (pool, face) => { const a = pool && pool.artEdit && typeof pool.artEdit === "object" ? pool.artEdit : {}; return num(a[face === "front" ? "front" : "back"]); };
const sealOf = (face, at, by) => (face === "front" ? { id: `engraveApproved:front:${at}:${String(by).trim()}`, how: "engraveApproved", at, by, face: "front" } : { id: `engraveApproved:${at}:${String(by).trim()}`, how: "engraveApproved", at, by });

/* ── the line's engraving record ── */
async function readLines(ctx, keys) {
  const out = new Map(), list = [...new Set(keys)].filter(Boolean);
  for (let i = 0; i < list.length; i += 400) { const r = await ctx.read({ op: "engraveSyncGet", keys: list.slice(i, i + 400) }); for (const l of (r && r.lines) || []) out.set(l.key, l); }
  return out;
}
/** One line written back whole, with the revision it was read at (ENGREV): { ok, rev } or { stale } (another computer wrote it since: read it again). */
async function writeLine(ctx, l, rec, unit) {
  const res = await ctx.call({ op: "engraveSyncPut", lines: [{ key: l.key, rec, sig: null, baseRev: l.rev }] }, unit);
  if (!res || res.status >= 400) throw new Error((res && res.error) || "the engraving record was not written");
  if ((res.stale || []).some(x => x.key === l.key)) return { stale: true };
  const done = (res.lines || []).find(x => x.key === l.key);
  if (done && done.error) throw new Error(done.error);
  return { ok: true, rev: done ? done.rev : null };
}
/** The line's own summary (row.engrave) after its jobs changed: a line of one job keeps that job's record; a line cut into pieces or faces is the slots' summary (CharmNestEngraveSides.summary). */
function lineSummary(E, lc, rec) {
  const jobs = (rec.jobs || []).filter(Boolean);
  const recOfJob = j => { const r = { needed: !["none", "skipped"].includes(j.state), state: j.state || "classify", approved: ["approved", "written", "none", "skipped"].includes(j.state), text: j.text == null ? undefined : j.text, approvedBy: j.approvedBy || undefined, approvedAt: num(j.approvedAt) || undefined, decidedAt: num(j.decidedAt) || undefined, reason: j.reason || null, seals: j.engravingSeals || [] }; if (isFrontJob(j)) r.face = "front"; for (const k of Object.keys(r)) if (r[k] === undefined) delete r[k]; return r; };
  const one = jobs.length === 1 && !Faces.parseKey(jobs[0].key).slot && !isFrontJob(jobs[0]) && !jobs[0].linked;
  if (one) return Object.assign({}, rec.engrave || {}, recOfJob(jobs[0]));
  const list = jobs.map(j => ({ key: j.key, slot: Faces.parseKey(j.key).slot, face: Faces.faceOf(j), archived: j.archived, copies: copiesOf(E, lc, j), engraveRec: recOfJob(j) }));
  return Sides.summary(E.piece.sidesCtx(), rec.engrave || {}, list) || rec.engrave || null;
}

/** The run's own copy of the line's engraving (the run line's `engrave`, Orders.lineRecord's shape), which readiness, sets and the commit read: written with runLinesPatch when it moved. */
const runEngraveOf = s => (s && typeof s === "object" ? Object.assign({ needed: !!s.needed, state: s.state || null, approved: !!s.approved, approvedAt: num(s.approvedAt), approvedBy: s.approvedBy || "", decidedAt: num(s.decidedAt), seals: s.seals || [], text: s.text || null }, s.pieces ? { pieces: s.pieces } : {}) : null);
async function patchRunLine(ctx, lineKey, summary, unit) {
  const eng = runEngraveOf(summary), cur = (ctx.run.lines || {})[lineKey]; if (!eng || !cur) return;
  if (JSON.stringify(cur.engrave || null) === JSON.stringify(eng)) return;
  const res = await ctx.call({ op: "runLinesPatch", runId: ctx.runId, lines: { [lineKey]: { engrave: eng } } }, unit);
  if (res && res.status < 400) cur.engrave = eng;
}

/* ONECOUNT (Paul, 23 Nov 2026: Set-5's sheets said 5 / 5 and 8 / 8 and "Mark set completed" was refused). A line's engraving is written twice: its own record (the jobs, which every computer shows) and the run's
   copy of the line (engrave), which is what the cloud counts. A person's None, and a reading that said there is nothing to engrave, wrote the record alone: 47 lines on 23 Nov said "nothing to engrave" or
   "approved" on every screen and "waits for review" in the cloud's count (HIPPO "(NO engraving)", LIGHTHOUSE "None", MOUSE "No engraving please", SQUIRREL "keine"). The run's copy follows the record now: each
   record written is looked at once, and what is decided on it is put on the run's line. It only ever settles (a decision is never taken back here: the fit, the reading and the back files do that). */
const jobDone = j => ["none", "skipped"].includes(j.state) || (["approved", "written"].includes(j.state) && num(j.approvedAt) > 0 && !!j.approvedBy);
async function followRecord(ctx, E, key, l) {
  const rec = l && l.rec, cur = (ctx.run.lines || {})[key], e = cur && cur.engrave, ids = (cur && cur.poolIds) || [], jobs = ((rec && rec.jobs) || []).filter(j => j && !j.archived);
  if (!e || typeof e !== "object" || !ids.length || !jobs.some(jobDone) || e.invalidatedBy || ["reclassify", "classify"].includes(e.state)) return false;   // (an order that changed is read again first)
  const one = jobs.length === 1 && !Faces.parseKey(jobs[0].key).slot && !isFrontJob(jobs[0]) && !jobs[0].linked, lc = one ? null : await lineContext(ctx, E, key);
  if (!one && !lc) return false;
  const s = lineSummary(E, lc, rec), ok = (x, id) => !!(((x.pieces || {})[id]) || x).approved;
  if (!s || ids.some(id => ok(e, id) && !ok(s, id)) || !ids.some(id => ok(s, id) && !ok(e, id))) return false;
  await patchRunLine(ctx, key, s, `engrave-one|${key}|${l.rev}`);
  return true;
}

/* ═══ 1 · FIT ═══════════════════════════════════════════════════════════ */
/** A job the fit takes: words read and waiting for their place ("ready"), or one a page left in the middle of its fit long ago. Never a decided, approved or archived one. */
const needsFit = (j, l) => !!j && !j.archived && !j.approving && (j.state === "ready" || (j.state === "fitting" && Date.now() - num(l.at) > STALE_FITTING_MS));
/** A job approved before its piece reached a sheet (ENGPRE), its back file not written yet: the cloud writes it once the piece is on a sheet (the page's resumeBacks). */
/* ARTAPPROVED (Paul, 23 Nov 2026: GF_Nov.22.26_Set-2_Sheet-1, 18 approved in its window, 17 on its card: the front of 4238830221, Charm Editor artwork and no words, approved by Michelle_R on 19 Nov, never had its file written): a face that carries
   only the editor's artwork keeps no place in its record, so its approval was never taken up. Its place is the artwork itself (artOnly). */
const wordless = j => !!j && !(Array.isArray(j.lines) ? j.lines : []).join("").trim() && !String(j.text == null ? "" : j.text).trim();
const needsResume = (j, l) => !!j && !j.archived && !j.approving && j.state === "approved" && num(j.approvedAt) > 0 && !!j.approvedBy && (!!j.place || wordless(j)) && Date.now() - num(l.at) > RESUME_AFTER_MS;

async function fitUnit(ctx) {
  if (["stopped", "abandoned", "complete"].includes(ctx.run.status)) return;   // (as the page: the automatic fit runs beside a run that is not stopped; a person's approvals are written whatever the run does)
  const lines = ctx.run.lines || {}, cref = ctx.db.collection(CURSOR[0]).doc(CURSOR[1]);
  const cs = await cref.get(), cur = cs.exists ? cs.data() || {} : {}, fresh = cur.runId !== ctx.runId;
  const keys = new Set(fresh ? [] : Object.keys(cur.pending || {}).filter(k => lines[k]));
  let since = fresh ? 0 : num(cur.since), newest = since, more = false;
  if (fresh) {
    // (a run the cloud takes up: every line of it that engraves is looked at once; from then on, only the records written since)
    for (const [k, l] of Object.entries(lines)) if (l && !O.FINISHED_LINE.has(l.state) && (l.engraveCandidate || (l.engrave && l.engrave.needed))) keys.add(k);
    newest = Date.now() - 5000;
  } else {
    const r = await ctx.read({ op: "engraveSyncGet", since }), got = (r && r.lines) || [];
    for (const l of got) { newest = Math.max(newest, num(l.at)); if (lines[l.key]) keys.add(l.key); }
    if (got.length >= 500) more = true;
    if (num(cur.oneCount) < 1) for (const [k, l] of Object.entries(lines)) if (l && !O.FINISHED_LINE.has(l.state) && l.engrave && !l.engrave.approved) keys.add(k);   // (ONECOUNT: the lines whose copy said "waits" before this rule, looked at once)
    if (num(cur.fontNow) < 1) for (const [k, l] of Object.entries(lines)) if (l && !O.FINISHED_LINE.has(l.state) && l.engrave && !l.engrave.approved) keys.add(k);   // (FONTNOW: the engravings that wait for approval, looked at once)
    if (num(cur.artLook) < 1) for (const [k, l] of Object.entries(lines)) if (l && !O.FINISHED_LINE.has(l.state) && l.engrave && /"state":"approved"/.test(JSON.stringify(l.engrave))) keys.add(k);   // (ARTAPPROVED: the lines approved before this rule, whose file was never written, looked at once)
  }
  const all = [...keys], pending = {};
  for (const k of all.slice(400)) pending[k] = true;   // (more than one read's worth: the rest at the next pass)
  if (all.length > 400) more = true;
  const E = await envOf(ctx), recs = await readLines(ctx, all.slice(0, 400));
  for (const [key, l] of recs) {
    const rec = l.rec; if (!rec || !Array.isArray(rec.jobs)) continue;
    const lineState = (lines[key] || {}).state; if (O.FINISHED_LINE.has(lineState)) continue;
    const need = rec.jobs.filter(j => needsFit(j, l)), resume = rec.jobs.filter(j => needsResume(j, l)), again = rec.jobs.filter(refontable);
    if (Date.now() > ctx.budgetEnd) { if (need.length || resume.length || again.length || ((lines[key] || {}).engrave || {}).approved === false) { more = true; pending[key] = true; } continue; }
    if (!need.length) { try { await followRecord(ctx, E, key, l); } catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; ctx.say && ctx.say(`[engrave] ${key}: the run's line was not brought up to its record (${String((e && e.message) || e).slice(0, 160)})`); } }   // (ONECOUNT)
    if (!need.length && !resume.length && !again.length) continue;
    const lc = await lineContext(ctx, E, key); if (!lc) continue;
    if (lc.spec && lc.spec.engraveNever) continue;   // (NO EARRING IS EVER ENGRAVED: the reading puts its jobs away, never the fit)
    let changed = false;
    for (const j of need) {
      if (Date.now() > ctx.budgetEnd - 10000) { more = true; pending[key] = true; break; }   // (a fit is one synchronous call: none starts so late that the line's write would come after the unit's hard limit)
      await turn(); if (ctx.live) ctx.live();
      let r;
      try { r = await fitOne(ctx, E, lc, j); }
      catch (e) {
        if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e;
        // (a placement the charm cannot take is a person's to look at: blocked, with the reason; anything else (a file that could not be read, the cloud slow) leaves the job as it is, looked at again next pass)
        r = e && e.engravingInvalid ? { patch: { state: "blocked", reason: String(e.message || e).slice(0, 300) } } : { wait: String((e && e.message) || e) };
        if (r.wait && ctx.say) ctx.say(`[engrave] ${key}: the fit waits (${r.wait.slice(0, 200)})`);
      }
      if (r && r.wait) { pending[key] = true; continue; }
      if (r && r.patch) { for (const [k, v] of Object.entries(r.patch)) { if (v === undefined) delete j[k]; else j[k] = v; } changed = true; }
    }
    for (const j of again) {   // (FONTNOW)
      if (Date.now() > ctx.budgetEnd - 10000) { more = true; pending[key] = true; break; }
      await turn(); if (ctx.live) ctx.live();
      try { const r = await refont(ctx, E, lc, j); if (r && r.wait) { pending[key] = true; continue; } if (r && r.patch) { for (const [k, v] of Object.entries(r.patch)) { if (v === undefined) delete j[k]; else j[k] = v; } changed = true; ctx.say && ctx.say(`[engrave] ${key}: set again in ${j.fontName}`); } }
      catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; ctx.say && ctx.say(`[engrave] ${key}: it keeps the font it was prepared in (${String((e && e.message) || e).slice(0, 160)})`); }
    }
    for (const j of resume) { try { const made = await resumeTask(ctx, E, lc, l, j); if (made === "wait") pending[key] = true; } catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; ctx.say && ctx.say(`[engrave] ${key}: ${e.message}`); } }
    if (changed) {
      rec.engrave = lineSummary(E, lc, rec) || rec.engrave;
      const w = await writeLine(ctx, l, rec, `engrave-fit|${key}|${l.rev}`);
      if (w.stale) pending[key] = true;   // (a page wrote the line meanwhile: it is read again next pass, and fitted only if it still waits)
      else await patchRunLine(ctx, key, rec.engrave, `engrave-line|${key}|${w.rev}`);
    }
  }
  const pend = Object.keys(pending).slice(0, 1500);
  await ctx.db.runTransaction(async tx => { await fenceTx(ctx, tx); tx.set(cref, { runId: ctx.runId, artLook: 1, oneCount: 1, fontNow: 1, since: newest, pending: Object.fromEntries(pend.map(k => [k, true])), at: Date.now() }); });
  return more ? { more: true } : undefined;
}

/* FONTNOW (Paul, 24 Nov 2026: the new fonts "throughout the application"): an engraving that waits for its approval, prepared in the font that stood in for the shop's word before, is set again in the font the word is now:
   placed anew when nobody placed it, where a person put it when somebody did. One that cannot be placed in it keeps the placement it has. Approved work is never touched. */
const refontable = j => !!j && !j.archived && !j.approving && !j.approvedAt && !j.fontLocked && j.state === "review" && !!O.FONT_WAS[j.fontKey] && !!(Array.isArray(j.lineInput) && j.lineInput.length ? j.lineInput : j.lines || []).join("").trim();
async function refont(ctx, E, lc, j) {
  const to = O.fontNow(j, lc.spec, pieceIndexOf(Faces.parseKey(j.key).slot)); if (!to) return null;
  const hand = !!(j.nudged || j.decision), t = Object.assign({}, j, { fontKey: to, state: "ready", place: hand && j.place ? Object.assign({}, j.place, { fontKey: to }) : undefined }, hand ? {} : { wantSize: undefined, applied: undefined });
  const r = await fitOne(ctx, E, lc, t); if (!r || r.wait) return r || null;
  if (!r.patch || r.patch.state !== "review" || !r.patch.place) return null;
  return { patch: Object.assign({}, r.patch, { fontKey: to, fontName: (O.fontById(to) || {}).name || to }, hand || r.patch.applied ? {} : { applied: undefined }) };
}

/** One job fitted: { patch } (the job's new fields), { wait } (its piece is not on a sheet yet, its design or font cannot be read yet: looked at again next pass), or { skip }
 *  (nothing the fit can do: a person's). */
async function fitOne(ctx, E, lc, job) {
  const G = E.G, s = E.settings, slot = Faces.parseKey(job.key).slot, front = isFrontJob(job);
  const copies = copiesOf(E, lc, job); if (!copies.length) return { wait: "its pieces are not made yet" };
  const sh = await sheetOf(E, copies[0]); if (!sh || !sh.fileBase) return { wait: "its piece is not on a sheet yet" };
  const f = await E.Fonts.forJob(job, { spec: lc.spec, pieceIndex: pieceIndexOf(slot) });
  const lines = f.lines, lineInput = f.lineInput && f.lineInput.length ? f.lineInput : lines.slice();
  if (!lineInput.length) return { skip: "no words to place" };   // (EDITOR WORDS: a face with no words is the Charm Editor's, a person approves it as drawn)
  const cov = G.glyphCoverage(f.fonts.Regular, lines.join("\n"));
  if (!cov.ok) { const why = `Unsupported engraving characters: ${cov.missing.map(c => c + " (" + [...c].map(x => "U+" + x.codePointAt(0).toString(16).toUpperCase()).join(" ") + ")").join(", ")}${E.Fonts.scriptWhy() ? " — " + E.Fonts.scriptWhy() : ""}`; return { patch: { state: "words", reason: why, missing: cov.missing } }; }
  const got = await E.piece.charmOf(copies[0], { sheet: sh, slot, side: Sides.sideOfSlot(slot) });
  const charm = got.charm; if (!charm || !charm.outline) return { wait: "its charm has no outline yet" };
  const entry = (await E.piece.fetchEntry(lc.spec.designSku || got.pool.sku)) || {};
  const opts = fitOpts(job, s);
  const place = job.place && Array.isArray(job.place.centre) && job.place.size > 0 ? job.place : null;
  const keep = place && (place.fontKey || null) === f.fontKey && wordsOf(place.lines) === wordsOf(lineInput) ? place : null;   // (KEEP: the words stay where a person placed them, keepPlacement)
  const input = { charm: { outline: charm.outline, members: charm.members, bbox: charm.bbox, widthPt: charm.widthPt, heightPt: charm.heightPt, upAngle: charm.upAngle },
    lines: lineInput.slice(), lineMode: job.lineMode || "auto", opts, fontKey: f.fontKey, ...(f.scriptIds.length ? { scriptIds: f.scriptIds } : {}),
    viewOptions: G.viewOptionsFor({ editingBack: false, savedUp: null, charmUp: charm.upAngle, entry, nudged: !!job.nudged, viewUp: undefined, oriented: job.orientVersion === G.ORIENT, turnUp: job.turnUp, ...(front ? { face: "front" } : {}) }),
    maskOptions: { marginMm: s.engraveMarginMm, keepOut: (front ? charm.frontKeepOut : charm.backKeepOut) || [], ...(!front && Array.isArray(charm.artBack) && charm.artBack.length ? { avoid: charm.artBack } : {}) },
    ...(keep ? { keep } : {}) };
  let result;
  try { result = require("../../charm-nest-engrave-fit.js").calculate(input, f.fonts, G); }
  catch (e) { return { patch: { state: "blocked", reason: String((e && e.message) || e).slice(0, 300), ...(e && e.stage === "flip" ? { flipFailed: true } : {}) } }; }
  const { view, mask } = result, kept = result.kept || null;
  const fit0 = result.fit;
  const patch = { fontKey: f.fontKey, fontName: f.fontName, fitAt: Date.now(), materialVersion: 2, reason: null, missing: undefined, flipFailed: undefined };
  if (f.capsApplied) { patch.lineInput = lineInput.slice(); patch.text = f.text; }
  if (job.turnUp == null && view && Number.isFinite(view.upAngle)) patch.upSystem = view.upAngle;
  if (!input.viewOptions.holeOnly) patch.orientVersion = G.ORIENT;
  if (!fit0) { patch.state = "review"; patch.reason = result.reason || "no room for these words: a person places them"; patch.lines = (result.lines || lines).slice(); patch.text = patch.lines.join("\n"); return { patch }; }
  // FOLLOWASK: the buyer's asks, after an automatic fit (never on a placement a person made, a kept one, a decided or approved job)
  const J = { asks: job.asks, applied: job.applied, fit: fit0, mask, view, lineInput: lineInput.slice(), lineMode: job.lineMode || "auto", lines: result.lines.slice(), text: result.lines.join("\n"), wantSize: kept ? job.wantSize : fit0.size, nudged: !!job.nudged, decision: job.decision, approvedAt: job.approvedAt, lineGap: job.lineGap };
  if (!kept && !input.keep) { try { applyAsks(J, f.fonts, s, G); } catch (_) { /* the fit stands as it is */ } }
  const fit = J.fit;
  Object.assign(patch, { state: "review", lines: J.lines.slice(), text: J.lines.join("\n"), lineInput: J.lineInput, lineMode: J.lineMode, wantSize: J.wantSize, nudged: J.nudged || undefined, ...(J.applied ? { applied: J.applied } : {}),
    place: { centre: fit.centre.slice(), angle: fit.angle || 0, size: fit.size, lines: (Array.isArray(fit.lines) && fit.lines.length ? fit.lines : J.lines).slice(), weight: fit.weight || "Regular",
      lineGap: fit.lineGap != null ? fit.lineGap : job.lineGap, fittedMax: fit.fittedMax || fit.size, upAngle: view.upAngle, cx: view.cx, cy: view.cy, fontKey: f.fontKey } });
  if (patch.place.lineGap == null) delete patch.place.lineGap;
  if (patch.nudged === undefined) delete patch.nudged;
  return { patch };
}

/* FOLLOWASK (the page's applyAsks, with its refit and its nudge): layout and case (laid out again), size (small = 70 % of the fitted size, large = the most the metal allows), then
   position (the left / right / top / bottom third of the safe area, as far as the mask allows). J.applied = [{ type, value, ok, why }]. */
function applyAsks(J, F, s, G) {
  const A = require("../../charm-nest-engrave-asks.js");
  if (!Array.isArray(J.asks) || J.applied) return false;
  const pl = A.plan(J.asks); if (!Object.keys(pl).length) return false;
  if (!J.fit || J.fit.artOnly || !J.mask || J.nudged || J.decision || J.approvedAt) return false;
  const fontOf = w => (w === "Semibold" && F.Semibold) || F.Regular;
  const refit = place => {
    const want = place.size != null ? place.size : J.wantSize != null ? J.wantSize : J.fit.size;
    const f = G.reflowAt(J.lineInput || J.lines, fontOf(J.fit.weight), J.mask, Object.assign({}, fitOpts(J, s, J.mask, true), { measure: place.measure }), Object.assign({}, place, { size: want }), J.lineMode || "auto");
    if (!f.ok) return false;
    f.atLimit = f.size < want - .005; f.fittedMax = Math.max(f.size, J.fit.fittedMax || 0); f.weight = J.fit.weight; f.rect = J.fit.rect;
    J.wantSize = want; J.lines = f.lines.slice(); J.text = J.lines.join("\n"); J.fit = f; J.nudged = true;
    return true;
  };
  const shift = (cmds, dx, dy) => cmds.map(k => { if (k.type === "Z") return { type: "Z" }; const o = Object.assign({}, k, { x: k.x + dx, y: k.y + dy }); if (k.x1 != null) { o.x1 = k.x1 + dx; o.y1 = k.y1 + dy; } if (k.x2 != null) { o.x2 = k.x2 + dx; o.y2 = k.y2 + dy; } return o; });
  const nudge = (dxMm, dyMm) => {   // (CharmNestEngravePosition.nudge over the bridge's nudge: a pure shift, as far as the words stay on the metal, never smaller)
    const f0 = J.fit, far = Math.hypot(dxMm, dyMm); if (!f0 || f0.artOnly || !(dxMm || dyMm)) return { ok: true, clamped: false };
    const fits = t => G.verifyInk(shift(f0.cmds, dxMm * PT * t, dyMm * PT * t), J.mask).ok;
    let t = 1;
    if (!fits(1)) { let lo = 0, hi = 1; const n = Math.min(24, Math.max(4, Math.ceil(Math.log2(far / 0.005)))); for (let i = 0; i < n; i++) { const mid = (lo + hi) / 2; if (fits(mid)) lo = mid; else hi = mid; } t = lo; if (t * far < 0.02) return { ok: false, clamped: true }; }
    const dx = dxMm * PT * t, dy = dyMm * PT * t, glyphs = f0.glyphs.map(g => Object.assign({}, g, { cmds: shift(g.cmds, dx, dy) })), cmds = shift(f0.cmds, dx, dy), centre = [f0.centre[0] + dx, f0.centre[1] + dy];
    const layout = f0.layout ? Object.assign({}, f0.layout, { centre, glyphs, cmds }, f0.layout.bbox ? { bbox: [f0.layout.bbox[0] + dx, f0.layout.bbox[1] + dy, f0.layout.bbox[2] + dx, f0.layout.bbox[3] + dy] } : {}) : f0.layout;
    J.fit = Object.assign({}, f0, { centre, glyphs, cmds, layout }); J.nudged = true;
    return { ok: true, clamped: t < 1 };
  };
  const done = [], rec = (type, value, ok, why) => done.push({ type, value, ok, why: why || "" });
  const keep = () => ({ lineInput: J.lineInput, lineMode: J.lineMode, lines: J.lines, text: J.text, fit: J.fit, wantSize: J.wantSize, nudged: J.nudged });
  const undo = k => Object.assign(J, k);
  const cur = () => (J.wantSize != null ? J.wantSize : J.fit.size);
  if (pl.case || pl.layout) {
    const k = keep(), base = (J.lineInput || J.lines).slice(), lines = pl.case ? A.caseLines(base, pl.case) : base, mode = pl.layout ? A.layoutMode(pl.layout, lines) : null;
    J.lineInput = lines; if (mode) J.lineMode = mode;
    const cov = G.glyphCoverage(F.Regular, lines.join("\n")).ok;
    const ok = cov && refit({ centre: J.fit.centre, angle: J.fit.angle, size: cur() });
    if (!ok) undo(k);
    for (const t of ["case", "layout"]) if (pl[t]) rec(t, pl[t], !!ok, ok ? "" : cov ? "no room: kept as it was" : "the letters are not in the font: kept as it was");
  }
  if (pl.size) {
    const k = keep(), was = J.fit.small, want = pl.size === "small" ? cur() * 0.7 : fitOpts(J, s, J.mask, true).maxHeightFrac * (J.mask.hPt || J.mask.h / J.mask.res);
    const ok = refit({ centre: J.fit.centre, angle: J.fit.angle, size: want });
    if (ok && pl.size === "small" && J.fit.small && !was) { undo(k); rec("size", "small", false, "smaller would be too fine to engrave: kept"); }
    else if (!ok) { undo(k); rec("size", pl.size, false, "no room at that size: kept"); }
    else rec("size", pl.size, true, pl.size === "large" && J.fit.size < want - 0.005 ? "as large as the metal allows" : "");
  }
  if (pl.position) {
    let ok = false, why = "";
    if (pl.position === "center") { const c = BF.maskCentroid(J.mask), r = nudge((c[0] - J.fit.centre[0]) / PT, (c[1] - J.fit.centre[1]) / PT); ok = true; why = r.ok ? "" : "already centred"; rec("position", "center", ok, why); }
    else {
      const m = J.mask; let x0 = 1e9, x1 = -1e9, y0 = 1e9, y1 = -1e9;
      for (let y = 0; y < m.h; y++) for (let x = 0; x < m.w; x++) if (m.bits[y * m.w + x]) { if (x < x0) x0 = x; if (x > x1) x1 = x; if (y < y0) y0 = y; if (y > y1) y1 = y; }
      if (x1 < x0) rec("position", pl.position, false, "no safe area: kept");
      else {
        const b = { x0: m.ox + x0 / m.res, x1: m.ox + (x1 + 1) / m.res, y0: m.oy + y0 / m.res, y1: m.oy + (y1 + 1) / m.res }, t = A.thirdOf(pl.position, b), c = J.fit.centre;
        const d = t.axis === "x" ? t.to - c[0] : t.to - c[1], toward = pl.position === "left" || pl.position === "bottom" ? d < 0 : d > 0;
        if (!toward) { ok = true; why = "already there"; }
        else { const r = nudge(t.axis === "x" ? d / PT : 0, t.axis === "y" ? d / PT : 0); ok = !!r.ok; why = r.ok ? (r.clamped ? "moved as far as the metal allows" : "") : `no room on the ${pl.position}: kept centred`; }
        rec("position", pl.position, ok, why);
      }
    }
  }
  J.applied = done; J.nudged = done.some(d => d.ok && d.why !== "already there" && d.why !== "already centred");
  return true;
}

/* ═══ 2 · BACK FILES ═════════════════════════════════════════════════════ */
const taskIdOf = (jobKey, at) => `${String(jobKey).replace(/\//g, "_")}@${Math.round(num(at))}`;
/** ENGPRE: the task of a job approved before its piece reached a sheet, made when its piece is on one. "wait" while it is not. */
async function resumeTask(ctx, E, lc, l, job) {
  const copies = copiesOf(E, lc, job); if (!copies.length) return "wait";
  for (const id of copies) if (!(await sheetOf(E, id))) return "wait";
  if (!job.place) { await E.piece.poolRows(copies); if (!wordless(job) || !copies.every(id => { const a = artAtOf(E.piece.rows.get(id), Faces.faceOf(job) === "front" ? "front" : "back"); return a > 0 && a <= num(job.approvedAt) + 2000; })) return "none"; }   // (ARTAPPROVED: only artwork that has not been changed since it was approved)
  const id = taskIdOf(job.key, job.approvedAt), ref = ctx.db.collection(TASKS).doc(id), now = Date.now();
  return ctx.db.runTransaction(async tx => {
    await fenceTx(ctx, tx);
    const s = await tx.get(ref); if (s.exists) return "had";
    tx.create(ref, { id, runId: ctx.runId, lineKey: l.key, jobKey: job.key, face: Faces.faceOf(job), slot: Faces.parseKey(job.key).slot || null, at: num(job.approvedAt), by: String(job.approvedBy), text: job.text == null ? "" : String(job.text), place: job.place || { artOnly: true, upAngle: null },
      artAtBy: null, origin: "resume", state: "waiting", stage: "check", copies: null, done: {}, staged: {}, files: {}, sheets: {}, errors: 0, createdAt: now, updatedAt: now, expireAt: new Date(now + 30 * 86400000) });
    return "made";
  });
}

/* APPROVENOW: an approval's file is written by one process at a time: the pass ("pass") or the approval's own worker (nowTask). One that beat in the last 15 s keeps it; past that (it died, its lease went) the other goes on from the task's saved progress. */
const HELD_MS = 15000;
const heldElse = (ctx, t) => !!t && t.state === "working" && !!t.worker && t.worker !== (ctx.worker || "pass") && Date.now() - num(t.beatAt) < HELD_MS;
/** One approval written now, by a process of its own, under the running pass's lease (fenced on it at every write, like the pass's own). → { done } | { skipped } | { again } (the next pass goes on with it). */
async function nowTask(o) {
  const { db, Lib, taskId, key, say } = o, ref = db.collection(TASKS).doc(taskId), s0 = await ref.get(), t0 = s0.exists ? Object.assign({ id: taskId }, s0.data() || {}) : null;
  if (!t0 || !t0.key || t0.key !== key || !ACTIVE.includes(t0.state)) return { skipped: "not this approval's turn" };
  const until = Date.now() + 9000; let L = null;
  for (;;) { const l = await db.collection("Charm_Nest_Drive").doc("lease").get(); L = l.exists ? l.data() || {} : {}; if (L.state === "running" && L.nonce) break; if (Date.now() > until) { await ref.set({ kickedAt: 0 }, { merge: true }).catch(() => {}); return { again: "no pass runs" }; } await new Promise(r => setTimeout(r, 300)); }
  const fence = { path: "Charm_Nest_Drive/lease", nonce: L.nonce, runId: t0.runId }, ctx = { db, fence, worker: "now:" + key.slice(0, 8), runId: t0.runId, say: say || (() => {}), live() {}, budgetEnd: Date.now() + 60000 };
  ctx.read = b => Lib.call(b, {});
  ctx.call = async (b, unit) => { const r = await Lib.call(b, { unit: String(unit || ""), fence }); if (r && r.fenced) throw fencedErr(r.error); if (r && r.status === 409 && r.notCarrier) throw fencedErr(r.error); if (r && r.inFlight) throw new Error(String(r.error || "is being written already")); return r; };
  const at0 = Date.now();
  try {
    const got = await ctx.read({ op: "runGet", runId: t0.runId }); ctx.run = got && got.run; if (!ctx.run) return { again: "the run could not be read" };
    const E = await envOf(ctx), r = await runTask(ctx, E, t0).catch(async e => { if (e && (e.fenced || e.yielded || e.fatal)) throw e; return (await taskError(ctx, E, t0, e)) || { again: String((e && e.message) || e).slice(0, 160) }; });
    return Object.assign({ ms: Date.now() - at0 }, r || {});
  } catch (e) { if (!(e && e.fenced)) throw e; await ref.set({ kickedAt: 0, beatAt: 0 }, { merge: true }).catch(() => {}); return { again: String(e.message || e).slice(0, 160) }; }   // (the pass ended meanwhile: the next one goes on at once)
}

async function backUnit(ctx) {
  const q = await ctx.db.collection(TASKS).where("state", "in", ACTIVE).limit(50).get();
  const tasks = q.docs.map(d => Object.assign({ id: d.id }, d.data())).filter(t => t.runId === ctx.runId).sort((a, b) => num(a.createdAt) - num(b.createdAt));
  if (!tasks.length) return;
  const E = await envOf(ctx);
  for (const t of tasks) {
    if (Date.now() > ctx.budgetEnd) return { more: true };
    if (num(t.kickedAt) > Date.now() - 9000 || heldElse(ctx, t)) continue;   // (APPROVENOW: its own worker is on it)
    let r;
    try { r = await runTask(ctx, E, t); }
    catch (e) {
      if (e && (e.fenced || e.yielded || e.fatal)) throw e;
      if (e && e.overBudget) return { more: true };
      r = await taskError(ctx, E, t, e);
    }
    if (r && r.more) return { more: true };
  }
}

/** A task's failure that may pass (the cloud slow, a read that failed): counted, and taken up again at the next pass; the fifth ends it with the words a person reads. */
async function taskError(ctx, E, t, e) {
  const why = String((e && e.message) || e || "unknown error").slice(0, 300);
  const now = await taskPut(ctx, t.id, cur => (cur && ACTIVE.includes(cur.state) ? { errors: num(cur.errors) + 1, lastError: why, state: "waiting", note: `trying again: ${why}` } : null));
  if (now && num(now.errors) >= MAX_ERRORS) return endInvalid(ctx, E, now, `Not saved: the ${now.face === "front" ? "front" : "back"} file could not be saved in the cloud (${why}). Press Approve again`, false);
  if (ctx.say) ctx.say(`[engrave] ${t.lineKey}: ${why}`);
  return null;
}
async function end(ctx, t, state, why) { await taskPut(ctx, t.id, cur => (cur && ACTIVE.includes(cur.state) ? { state, why: why || null, endedAt: Date.now() } : null)); return { ended: state }; }
async function wait(ctx, t, why) { await taskPut(ctx, t.id, cur => (cur && ACTIVE.includes(cur.state) ? { state: "waiting", note: why, waitingSince: cur.waitingSince || Date.now() } : null)); return { wait: why }; }

/** The approval cannot stand (the placement no longer fits, the artwork changed, the file failed its check, or it could not be saved at all): the task ends "failed" and the job goes back to
 *  Review with the reason, its approval not made (no seal, no event); the back approved before stays in the files. (writeBacks' SEALAFTERSAVE branch) */
async function endInvalid(ctx, E, t, why, invalid = true) {
  await end(ctx, t, "failed", why);
  for (let i = 0; i < 3; i++) {
    const l = (await readLines(ctx, [t.lineKey])).get(t.lineKey); if (!l || !l.rec) return { ended: "failed" };
    const job = (l.rec.jobs || []).find(j => j && j.key === t.jobKey); if (!job) return { ended: "failed" };
    if (t.origin === "resume") {
      // (an approval made before its piece reached a sheet, whose file cannot be made as approved: blocked with the reason, for a person to fit again, as the page's writeBacks does)
      if (!invalid || job.state !== "approved" || num(job.approvedAt) !== num(t.at)) return { ended: "failed" };
      job.state = "blocked"; job.reason = why;
    } else {
      if (!(job.approving && num(job.approving.at) === num(t.at))) return { ended: "failed" };   // (a newer approval, or a decided job: it is theirs)
      delete job.approving; job.reason = why; if (invalid && !["approved", "written"].includes(job.state)) job.state = "review";
      // (ITEM 9: an edit that could not be saved leaves the job as it was before it: its approval and file stand, the reason said)
      const prev = t.origin === "edit" && t.edit && t.edit.prev;
      if (prev && ["approved", "written"].includes(prev.state)) { job.state = prev.state; if (prev.text != null) job.text = prev.text; if (prev.lines) job.lines = prev.lines; if (prev.place) job.place = prev.place; }
    }
    const lc = await lineContext(ctx, E, t.lineKey, editLine(t, t.lineKey)); if (lc) l.rec.engrave = lineSummary(E, lc, l.rec) || l.rec.engrave;
    const w = await writeLine(ctx, l, l.rec, `engrave-fail|${t.id}|${l.rev}`);
    if (w.ok) { await patchRunLine(ctx, t.lineKey, l.rec.engrave, `engrave-line|${t.lineKey}|${w.rev}`); return { ended: "failed" }; }
  }
  return { ended: "failed" };
}

async function runTask(ctx, E, t0) {
  let mine = true;
  let t = await taskPut(ctx, t0.id, cur => { mine = !heldElse(ctx, cur); return mine && cur && ACTIVE.includes(cur.state) ? { state: "working", startedAt: cur.startedAt || Date.now(), stage: cur.stage || "check" } : null; });
  if (!t || !mine || !ACTIVE.includes(t.state)) return null;   // (APPROVENOW: another process of the cloud is writing it)
  const face = t.face === "front" ? "front" : "back", slot = t.slot || Faces.parseKey(t.jobKey).slot || null;
  const l = (await readLines(ctx, [t.lineKey])).get(t.lineKey);
  const job = l && l.rec && (l.rec.jobs || []).find(j => j && j.key === t.jobKey);
  if (!job) return end(ctx, t, "failed", "its engraving is no longer in the cloud");
  const final = ["approved", "written"].includes(job.state) && num(job.approvedAt) === num(t.at);
  // superseded: a newer approval of this job was pressed, or its words changed since (the job no longer waits on THIS approval)
  if (t.origin === "resume" ? !final : !(job.approving && num(job.approving.at) === num(t.at)) && !final) return end(ctx, t, "superseded", "a newer approval, or a change of the words, replaced this one");
  const lc = await lineContext(ctx, E, t.lineKey, editLine(t, t.lineKey)); if (!lc) return end(ctx, t, "failed", "its order line is no longer in the run");
  if (O.FINISHED_LINE.has(lc.line.state) && lc.line.state !== "committed") return end(ctx, t, "failed", `its order line is ${lc.line.state}`);
  let copies = Array.isArray(t.copies) && t.copies.length ? t.copies : copiesOf(E, lc, job);
  if (!copies.length) return wait(ctx, t, "its pieces are not made yet");
  if (!(Array.isArray(t.copies) && t.copies.length)) t = await taskPut(ctx, t.id, () => ({ copies }));
  await E.piece.poolRows(copies);
  // a piece taken off its sheet (cancelled, held, released): no sheet can hold a back for it
  if (copies.every(id => { const p = E.piece.rows.get(id); return p && ["abandoned", "superseded"].includes(p.state); })) return end(ctx, t, "failed", "its pieces were taken off their sheets: nothing to save");
  // MADEAGAIN (4239999491): a piece made again after it was approved (its charm taken back and replaced: supersededAt) is not the piece the person looked at: it is asked again, in words, never saved by itself and never waited for
  if (t.origin !== "resume" && copies.some(id => { const p = E.piece.rows.get(id); return p && num(p.supersededAt) > num(t.at) && !(t.staged && t.staged[id]); })) return endInvalid(ctx, E, t, "This piece was made again after it was approved: look at it and approve it again");
  // ARTSTAMP: the artwork of a piece's face was edited after the fit the person approved
  for (const id of copies) { const was = t.artAtBy && t.artAtBy[id] != null ? num(t.artAtBy[id]) : null; if (was != null && artAtOf(E.piece.rows.get(id), face) > was) return endInvalid(ctx, E, t, "This charm's artwork changed while it was being approved — approve it again"); }
  // the font the approval was made in (never a cut in another font)
  const fjob = Object.assign({}, job, { fontKey: (t.place && t.place.fontKey) || job.fontKey, lines: (t.place && t.place.lines) || job.lines, lineInput: job.lineInput });
  const f = await E.Fonts.forJob(fjob, { spec: lc.spec, pieceIndex: pieceIndexOf(slot) });
  if (f.fallback) throw new Error(`the engraving font of this approval: ${f.fallback}`);
  const shared = { first: copies[0] };   // (one charm, one placement and one picture for every copy of the job, as writeBacksOnce: charmFor(job) is its first piece's)
  for (const id of copies) {
    if (t.done && t.done[id]) continue;
    if (Date.now() > ctx.budgetEnd) return { more: true };
    await turn(); if (ctx.live) ctx.live();
    let r;
    try { r = await oneCopy(ctx, E, t, lc, job, f, id, slot, shared); }
    catch (e) {
      if (e && e.engravingInvalid) return endInvalid(ctx, E, t, `Not saved: ${e.message}`);
      if (e && e.superseded) return end(ctx, t, "superseded", e.message);
      if (e && e.engravingPending) return waitLong(ctx, t, e.message);
      throw e;
    }
    if (r && r.wait) return waitLong(ctx, t, r.wait);
    t = r.t;
  }
  // every piece recorded: the pieces engraved, then the approval itself (state, seal), then the sheet's index is owed
  const rec = (await readLines(ctx, [t.lineKey])).get(t.lineKey);
  await poolFlags(ctx, E, t, lc, rec && rec.rec, job, copies);
  const ok = await finalize(ctx, E, t, lc);
  await taskPut(ctx, t.id, () => ({ state: ok === false ? "superseded" : "done", stage: "index", indexOwed: true, endedAt: Date.now(), why: ok === false ? "a newer approval was pressed while this one was saved: its file stays in the archive" : null }));
  return { done: true };
}
/** Waiting on a sheet: said on the task; after three days with no sheet the approval ends with words (its pieces never reached one). */
async function waitLong(ctx, t, why) {
  if (t.waitingSince && Date.now() - num(t.waitingSince) > WAIT_DAYS_MS) return end(ctx, t, "failed", `waited three days: ${why}`);
  return wait(ctx, t, why);
}

/** One piece: built, checked, its picture, uploaded, staged, recorded. → { t } or { wait }. shared: the job's charm (its first piece's), its placement made again and its picture, made once. */
async function oneCopy(ctx, E, t, lc, job, f, poolId, slot, shared) {
  const G = E.G, P = E.P, s = E.settings, face = t.face === "front" ? "front" : "back";
  const sh = await sheetOf(E, poolId); if (!sh) return { wait: "its pieces are still being placed on their sheets: the file is saved once they are" };
  const dir = folderOf(sh); if (!dir || !sh.fileBase) return { wait: "its sheet's files are not saved yet: the back file follows them" };
  const pool = E.piece.rows.get(poolId) || {}, staged = t.staged && t.staged[poolId];
  let rec = staged && staged.sheetId === sh.id && staged.outputs && staged.outputs.ai && staged.outputs.ai.url && staged.outputs.png && staged.outputs.png.url ? staged : null;
  if (!rec) {
    // (a file that cannot be made within a pass's time is not tried for ever: each build is counted, and past three a piece the task ends with words)
    t = await taskPut(ctx, t.id, cur => ({ builds: num(cur && cur.builds) + 1 }));
    if (num(t.builds) > 3 * Math.max(1, (t.copies || []).length) + 3) throw invalidErr(`the ${face} file could not be made in the time the cloud gives one step (${num(t.builds) - 1} tries)`);
    const lineGap = t.place && t.place.lineGap != null ? +t.place.lineGap : job.lineGap != null ? +job.lineGap : 0.216;
    if (!shared.R) {
      const firstSheet = shared.first === poolId ? sh : await sheetOf(E, shared.first);
      shared.got = await E.piece.charmOf(shared.first, { sheet: firstSheet, slot, side: Sides.sideOfSlot(slot) });
      shared.R = BF.rebuild(t.place, { G, charm: shared.got.charm, face, font: w => (w === "Semibold" && f.fonts.Semibold) || f.fonts.Regular, marginMm: s.engraveMarginMm, lineGap, minCapMm: s.engraveMinCapMm, minStrokeMm: s.engraveMinStrokeMm, minGapMm: s.engraveMinGapMm,
        tolPt: 0.06 * PT });   // (the owner's tolerance between the cloud's reading of a drawing and Chrome's: 0.06 mm)
      shared.added = BF.backArtOf(shared.R.view, shared.got.charm, G);
      shared.pic = BF.previewOps({ view: shared.R.view, fit: shared.R.fit, face, added: shared.added, P, px: 500, cutLinePt: G.CUT_LINE_PT });
      shared.png = pngOf(shared.pic);
    }
    const got = shared.got, R = shared.R, added = shared.added, pic = shared.pic, png = shared.png;
    const fileView = s.backFileView || "asSeenFromBack", receipt = lc.row.order.receiptId, sku = lc.spec.designSku || pool.sku, copy = pool.copy || 1;
    const meta = { poolId, order: receipt, sku, copy, text: t.text, font: f.fontName, fontKey: f.fontKey, lineGap, approvedBy: t.by, approvedAt: num(t.at) };
    const built = await P.buildBackFile(BF.backSpec({ charm: got.charm, parsed: got.src.parsed, view: R.view, fit: R.fit, face, backFileView: fileView, title: `${receipt} · ${sku} · ${face}`, meta }));
    const verified = await BF.verifyBackFile(built.bytes, { P, G, view: R.view, mask: R.mask, fit: R.fit, face, backFileView: fileView, added });   // 7.4 · flip integrity re-run on the written, re-parsed file
    if (!verified.ok) throw invalidErr(`the written ${face} file did not re-verify (${verified.why})`);
    const name = BF.backName(sh.fileBase, face, poolId, num(t.at)), paths = { ai: `${dir}/${face}/${name}.ai`, png: `${dir}/${face}/${name}.png` };
    // (the tokens are kept BEFORE the bytes go up: a pass that dies after an upload finds its file there, under its token, and never uploads it again)
    let plan = t.files && t.files[poolId];
    if (!plan || !plan.ai || plan.ai.path !== paths.ai || !plan.png || plan.png.path !== paths.png) { plan = { ai: { path: paths.ai, token: uuid() }, png: { path: paths.png, token: uuid() } }; t = await taskPut(ctx, t.id, () => ({ files: { [poolId]: plan }, stage: "files" })); }
    const [ai, pu] = await Promise.all([upload(ctx, E, plan.ai.path, built.bytes, "application/illustrator", plan.ai.token), upload(ctx, E, plan.png.path, png, "image/png", plan.png.token)]);   // (APPROVENOW: side by side)
    const jobFields = Object.assign({}, job, { text: t.text, lines: R.fit.lines.slice(), face: face === "front" ? "front" : undefined });
    const fr = face === "front" ? { face: "front" } : {}, seal = sealOf(face, num(t.at), t.by);
    const seals = t.origin === "resume" ? Seals.merge(Object.assign({ engravingSeals: job.engravingSeals || [] }, fr), Object.assign({ seals: [seal] }, fr)) : [seal];   // (SEALAFTERSAVE: the approval's own seal; an approval made before (ENGPRE) keeps the job's seals, CNEngravingSeals.keep)
    rec = BF.backRecord({ job: jobFields, face, fit: R.fit, view: R.view, verify: R.verify, verified, built, sheet: { sheetId: sh.id, setId: sh.setId, runId: sh.runId }, poolId, order: receipt,
      transactionId: lc.row.line.transactionId, sku, copy, side: Sides.sideOfSlot(slot), mirror: !!got.charm.mirrored, lineGap, fontKey: f.fontKey, approvedBy: t.by, approvedAt: num(t.at),
      seals, backFileView: fileView, outputs: { ai: { path: ai.path, url: ai.url }, png: { path: pu.path, url: pu.url } }, name, previewPt: pic.sizePt });
    rec = JSON.parse(JSON.stringify(rec));   // (plain data: what is kept in the task is exactly what is sent)
    t = await taskPut(ctx, t.id, () => ({ staged: { [poolId]: rec }, stage: "record" }));
  }
  // the record (Charm_Pool_Back, the sheet's backPool, the order's timeline event): backPut, written once per approval of this piece (its unit key names both)
  const artAt = t.artAtBy && t.artAtBy[poolId] != null ? num(t.artAtBy[poolId]) : null;
  const was = t.origin === "edit" && t.edit && num(t.edit.was) > 0 ? { expectedApprovedAt: num(t.edit.was) } : {};   // (ITEM 9: an edit replaces the very back it was opened from, as the page's edit did; edited elsewhere since, it is not written over)
  const put = await ctx.call(Object.assign({ op: "backPut", back: artAt != null ? Object.assign({}, rec, { artAt }) : rec }, was), `engrave-back|${t.id}|${poolId}`);
  if (put && Array.isArray(put.staleArt) && put.staleArt.includes(poolId)) throw invalidErr("This charm's artwork changed while it was being approved — approve it again");
  if (!put || put.status >= 400) {
    const why = String((put && put.error) || "the back record was not written");
    if (/superseded/i.test(why)) throw Object.assign(new Error(why), { superseded: true });
    if (/does not contain this exact charm copy/i.test(why)) throw pendingErr("its piece moved to another sheet: the file is made again for that sheet");
    if (/edited elsewhere/i.test(why)) throw invalidErr(why);   // (ITEM 9)
    throw new Error(why);
  }
  E.sheets.delete(sh.id);   // (its backPool moved: read again by whoever needs it)
  t = await taskPut(ctx, t.id, () => ({ done: { [poolId]: true }, sheets: { [sh.id]: true } }));
  return { t };
}

/** The approval's picture as PNG bytes: the SVG through @resvg/resvg-js; where it cannot load, the same paint on the node canvas (_charmNestCanvas.js). */
function pngOf(pic) {
  let Resvg = null; try { Resvg = require("@resvg/resvg-js").Resvg; } catch (_) { Resvg = null; }
  if (Resvg) { const out = new Resvg(BF.svgOfOps(pic), { background: "white", fitTo: { mode: "original" } }).render().asPng(); return new Uint8Array(out.buffer, out.byteOffset, out.byteLength); }
  const C = require("./_charmNestCanvas.js"), cv = new C.NodeCanvas(pic.width, pic.height);
  BF.drawPreview(cv.getContext("2d"), pic);
  const b = cv.toBuffer(); return b instanceof Uint8Array ? b : new Uint8Array(b);
}

/* ── Storage, in-process (the cloud's own writers never pass charmNestOutput's door) ── */
const dlUrl = (bucketName, p, token) => "https://firebasestorage.googleapis.com/v0/b/" + encodeURIComponent(bucketName) + "/o/" + encodeURIComponent(p) + "?alt=media&token=" + encodeURIComponent(token);
async function tokensOf(file) { try { const [meta] = await file.getMetadata(); const t = meta && meta.metadata && meta.metadata.firebaseStorageDownloadTokens; return t ? String(t).split(",").filter(Boolean) : []; } catch (_) { return null; } }
/** One file up, under the token planned for it; a file already there under that token is not uploaded again. The fence is read again first (C7). */
async function upload(ctx, E, p, bytes, contentType, token) {
  if (!/^charmnest\/(?!sandbox\/)/.test(p)) throw new Error("the cloud writes the shop's own files only");
  await fenceCheck(ctx);
  const bucket = E.bucket(), file = bucket.file(p), had = await tokensOf(file);
  if (had && had.includes(token)) return { path: p, url: dlUrl(bucket.name, p, token), had: true };
  await file.save(Buffer.from(bytes), { resumable: false, contentType, metadata: { contentType, metadata: { firebaseStorageDownloadTokens: token } } });
  return { path: p, url: dlUrl(bucket.name, p, token) };
}
/** An index file up, keeping the token already on the path (every stored link to it stays valid, as op_put keeps it). */
async function uploadKeep(ctx, E, p, bytes, contentType) {
  const had = await tokensOf(E.bucket().file(p));
  return upload(ctx, E, p, bytes, contentType, (had && had[0]) || uuid());
}

/** The pieces engraved (the page's Pool.update after writeBacksOnce): the face's flag and who approved it, and "engraved" once every face the piece expects is written. */
async function poolFlags(ctx, E, t, lc, rec, job, copies) {
  const front = t.face === "front";
  const others = ((rec && rec.jobs) || []).filter(o => o && o.key !== job.key && !o.archived && copiesOf(E, lc, o).some(id => copies.includes(id)));
  const othersDone = others.every(o => ["written", "skipped", "none"].includes(o.state));
  const patch = front ? { engraveFront: true, engraveFrontApprovedBy: t.by } : { engrave: true, engraveApprovedBy: t.by };
  if (othersDone && t.origin !== "edit") patch.state = "engraved";   // (ITEM 9: an edit of a piece already engraved, cut or packed changes its back only, never its state, as the page's edit)
  const live = copies.filter(id => { const p = E.piece.rows.get(id); return !p || !["abandoned", "superseded"].includes(p.state); });
  if (!live.length) return;
  const res = await ctx.call({ op: "poolUpdate", poolIds: live, patch }, `engrave-pool|${t.id}`);
  if (!res || res.status >= 400) throw new Error((res && res.error) || "the pieces were not marked engraved");
}
/** SEALAFTERSAVE: the approval made what an approval always was, now that its file is saved: the job approved and written, its seal, the line's summary. false when a newer approval
 *  took the job meanwhile (its own task writes it). */
async function finalize(ctx, E, t, lc) {
  const face = t.face === "front" ? "front" : "back", seal = sealOf(face, num(t.at), t.by);
  for (let i = 0; i < 3; i++) {
    const l = (await readLines(ctx, [t.lineKey])).get(t.lineKey); if (!l || !l.rec) return false;
    const rec = l.rec, job = (rec.jobs || []).find(j => j && j.key === t.jobKey); if (!job) return false;
    if (job.state === "written" && num(job.approvedAt) === num(t.at)) return true;
    if (t.origin !== "resume" && !(job.approving && num(job.approving.at) === num(t.at)) && !(job.state === "approved" && num(job.approvedAt) === num(t.at))) return false;
    const fr = face === "front" ? { face: "front" } : {};
    job.engravingSeals = Seals.merge(Object.assign({ engravingSeals: job.engravingSeals || [] }, fr), Object.assign({ seals: [seal] }, fr));
    job.state = "written"; job.approvedBy = t.by; job.approvedAt = num(t.at); job.text = t.text; job.reason = null; delete job.approving;
    if (t.place) job.place = t.place;
    rec.engrave = lineSummary(E, lc, rec) || rec.engrave;
    const w = await writeLine(ctx, l, rec, `engrave-seal|${t.id}|${l.rev}`);
    if (w.ok) { await patchRunLine(ctx, t.lineKey, rec.engrave, `engrave-line|${t.lineKey}|${w.rev}`); return true; }
  }
  throw new Error("the line's engraving kept changing while its approval was written: it is written at the next pass");
}

/* ═══ 3 · THE SHEET'S BACK INDEX ════════════════════════════════════════ */
async function indexUnit(ctx) {
  const q = await ctx.db.collection(TASKS).where("indexOwed", "==", true).limit(30).get();
  const tasks = q.docs.map(d => Object.assign({ id: d.id }, d.data())).filter(t => t.runId === ctx.runId);
  if (!tasks.length) return;
  const E = await envOf(ctx), sheetIds = [...new Set(tasks.flatMap(t => Object.keys(t.sheets || {})))];
  for (const id of sheetIds) {
    if (Date.now() > ctx.budgetEnd) return { more: true };
    try { await sheetIndex(ctx, E, id); }
    catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; if (ctx.say) ctx.say(`[engrave] back index of ${id}: ${e.message}`); return; }
  }
  for (const t of tasks) await taskPut(ctx, t.id, cur => (cur && cur.indexOwed ? { indexOwed: false, indexAt: Date.now() } : null));
}
/** back/back-index.pdf and back/back-report.json (and front/front-index.pdf, front/front-report.json) of one sheet, from its record's backPool, then putSheet { backOutputs, frontOutputs }. */
async function sheetIndex(ctx, E, sheetId) {
  E.sheets.delete(sheetId);
  const sh = await E.sheet(sheetId); if (!sh || sh.archived) return;
  const dir = folderOf(sh); if (!dir) return;
  require("./_charmNestPdf");
  const Export = require("../../charm-nest-export.js"), PairLabels = (() => { try { return require("../../charm-nest-pair-labels.js"); } catch (_) { return null; } })();
  const pairWord = b => { try { return PairLabels && PairLabels.backWord ? PairLabels.backWord(b) : ""; } catch (_) { return ""; } };
  const earWord = slot => { try { return Sides.earOf(slot); } catch (_) { return ""; } };
  const pngBytes = async url => { const p = require("./_charmEngravePiece").pathOfUrl(url); if (!p) return null; const [buf] = await E.bucket().file(p).download(); return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength); };
  const lines = (b, face) => [`${b.order} · ${b.sku} · copy ${b.copy}${pairWord(b)}${b.slot && !b.side ? " · " + earWord(b.slot) : ""}${face === "front" ? " · FRONT" : ""}`, `"${String(b.text).replace(/\n/g, " / ")}"`, `${b.font} ${b.weight} · ${b.sizePt} pt · cap ${b.capMm} mm${b.angle ? ` · ${b.angle}°` : ""}${b.small ? " · SMALL" : ""}`, `approved by ${b.approvedBy || "—"} ${b.approvedAt ? torontoTime(b.approvedAt) : ""}`];
  const all = (sh.backPool || []).slice(), backs = all.filter(b => !BF.isFront(b)), fronts = all.filter(BF.isFront), out = {};
  if (backs.length) {
    const items = [];
    for (const b of backs) { items.push({ png: b.outputs && b.outputs.png && b.outputs.png.url ? await pngBytes(b.outputs.png.url).catch(() => null) : null, lines: lines(b, "back") }); await turn(); }
    const pdf = await Export.backIndexPdf({ title: sh.fileBase, backs: items });
    const report = backs.map(b => Export.reportRow(b));
    const idx = await uploadKeep(ctx, E, `${dir}/back/back-index.pdf`, pdf, "application/pdf"), rep = await uploadKeep(ctx, E, `${dir}/back/back-report.json`, new TextEncoder().encode(JSON.stringify(report, null, 1)), "application/json");
    out.backOutputs = { index: { path: idx.path, url: idx.url }, report: { path: rep.path, url: rep.url }, count: backs.length };
  } else if (sh.backOutputs) out.backOutputs = null;   // (NONEPLAIN: a face the sheet no longer has any record of has no index)
  if (fronts.length) {
    const items = [];
    for (const b of fronts) { items.push({ png: b.outputs && b.outputs.png && b.outputs.png.url ? await pngBytes(b.outputs.png.url).catch(() => null) : null, lines: lines(b, "front") }); await turn(); }
    const names = Export.faceFiles({ folderPath: dir, fileBase: sh.fileBase, poolId: "", approvedAt: "", face: "front" });
    const pdf = await Export.backIndexPdf({ title: sh.fileBase, backs: items, face: "front" });
    const report = fronts.map(b => Export.reportRow(b, "front"));
    const idx = await uploadKeep(ctx, E, names.index, pdf, "application/pdf"), rep = await uploadKeep(ctx, E, names.report, new TextEncoder().encode(JSON.stringify(report, null, 1)), "application/json");
    out.frontOutputs = { index: { path: idx.path, url: idx.url }, report: { path: rep.path, url: rep.url }, count: fronts.length };
  } else if (sh.frontOutputs) out.frontOutputs = null;
  if (!Object.keys(out).length) return;
  const sig = crypto.createHash("sha256").update(JSON.stringify(all.map(b => [b.poolId, b.face || "", b.approvedAt, b.outputs && b.outputs.ai && b.outputs.ai.path]))).digest("hex").slice(0, 16);
  const res = await ctx.call({ op: "putSheet", sheet: Object.assign({ id: sheetId }, out) }, `engrave-index|${sheetId}|${sig}`);
  if (!res || res.status >= 400) throw new Error((res && res.error) || "the sheet's back index was not recorded");
  E.sheets.delete(sheetId);
}

/* The pass items (§1.6 item 8: the reading, then the fit, then the back files, then the sheet's back index), each on the lane "engrave". The reading is its own unit (build step 11's
   classify, charm-nest-engrave-classify.js) with order 80. */
const passItems = [
  { name: "engrave-fit", lane: "engrave", order: 81, unit: fitUnit },
  { name: "engrave-files", lane: "engrave", order: 82, unit: backUnit },
  { name: "engrave-index", lane: "engrave", order: 83, unit: indexUnit }
];

module.exports = { passItems, steps: {}, nowTask, fitUnit, backUnit, indexUnit, fitOne, applyAsks, runTask, oneCopy, finalize, poolFlags, sheetIndex, resumeTask, lineSummary, copiesOf, taskIdOf, fenceTx, pngOf,
  TASKS, ACTIVE, DEFAULTS, needsFit, needsResume,
  readLines, writeLine, patchRunLine, runEngraveOf, followRecord };   // (the line's record read and written, and the run line's copy of its summary: also the classify unit's, _charmEngraveClassify.js)
