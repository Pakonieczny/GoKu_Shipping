/*  netlify/functions/_charmEngraveClassify.js — ENGRAVING IN THE CLOUD: the reading of a line's engraving words (cloud plan v2, build step 11, C3; lane "engrave", pass order 80).
 *  ═══════════════════════════════════════════════════════════════════════
 *  DORMANT. A pass item of the cloud driver (_charmNestDrive.js, wired through _charmNestSteps.js). The driver runs it only while Charm_Nest_Open/drive is "standby" or "on",
 *  only for a run the cloud carries, and only while "engrave" is in drive.cloudSteps, as the fit and back-file units of _charmEngraveFiles.js (orders 81-83) that follow it.
 *  With the drive absent or off nothing here is ever called, and every page reads its lines' words exactly as today (Engrave.classifyAll, Engrave.earlyAll, CNBarFront).
 *
 *  What the page did, and where it is now. The reading itself is charm-nest-engrave-classify.js, the very code the page runs; the BARFRONT rule is charm-nest-engrave-bar-front.js,
 *  the very module the page runs; both are built here over the line's records instead of the page's memory:
 *    · the work: the run's lines whose world reading (Charm_World_Lines) or engraving record (Charm_Nest_Engrave) changed since the last pass (a run the cloud takes up: all of them
 *      once), and the lines that waited for their pieces;
 *    · BARFRONT (Orders.interpretAll → CNBarFront.follow): a bar line whose jobs are all undecided backs moves its words to its front, once (one timeline line, no paid reading);
 *      a bar line whose back a person decided keeps it (stays: no front is made beside it) and reads as what was decided (keep);
 *    · the jobs (Engrave.ensureJobs, ensureFaced, migrateToPieces, settlePieces, wantsFront; ported here over the record): one per piece and face the setup asks for, made again
 *      when the line's pieces changed: a whole-line job handed to each piece, a piece the line gained started with its sibling's words, a piece it lost put away with its decision;
 *    · the reading (classifyAll → classify → classifyOnce): the rules first (free, in-process); only what the rules cannot settle asks the paid reader, through the cloud-only op
 *      engraveIntentStart (Claude, never Etsy; the question is built from the records, never a picture). The reading runs in the background: the jobs stay "classify" with the
 *      reading's id on the first job (claudeJob, as the page keeps it), and a later pass takes the answer up by that id (getAgent): a reading is never paid for twice;
 *    · ENGPRE (Engrave.earlyAll → early): a line whose pieces are not made yet gets its jobs on the pool ids its pieces WILL get (job.pre), read with NO paid reading (freeWords);
 *      nothing is written to the order's timeline for it (its approval writes that), and nothing to the pool; once pooled, it is the same job;
 *    · ENGNOTE (Paul, 20 Nov 2026, "Yes, use ENG: notes"; the shared reading's engReading / engFollow): a staff message "ENG: <words>" is the words of the order's one line that takes them,
 *      read with the order's other lines (their world reading and records); a new or changed note reads again the lines that wait for a person;
 *    · EDITOR WORDS (toWords): a face the Charm Editor added parts to (its artwork edit record adds parts on that face) never waits for words: it goes to Review as the editor drew it;
 *    · the result: the line's record written whole with engraveSyncPut (with the revision it was read at), the run line's copy of the summary (runLinesPatch), the pieces of a ready
 *      job flagged engrave / engraveFront (poolUpdate) and the order's timeline lines (timelineAdd, fixed ids), as the page's setReady, needEvent and BARFRONT did. A pooled line with
 *      nothing to engrave is marked plain on the run line, as classifyAll did.
 *  Left to others: a font that will not load and a word the font cannot draw (the fit unit, _charmEngraveFiles.js fitOne, answers "Unsupported engraving characters"); a line sent to
 *  a custom sheet (the world build clears its engraveCandidate). The fit of a line not pooled yet waits for its sheet (the fit unit).
 *  Every write is the cloud's (ctx.call: Lib.call fenced on the pass's lease and the run's carrier). Nothing here calls Etsy, its image CDN, the world build, or any agent but the
 *  engraving reader.
 *  ═══════════════════════════════════════════════════════════════════════ */
"use strict";
const O = require("../../charm-nest-orders.js");
const Classify = require("../../charm-nest-engrave-classify.js");
const BarFront = require("../../charm-nest-engrave-bar-front.js");
const Read = require("../../charm-nest-engrave-read.js");
const Asks = require("../../charm-nest-engrave-asks.js");
const Faces = require("../../charm-nest-engrave-faces.js");
const Sides = require("../../charm-nest-engrave-sides.js");
const Seals = require("../../charm-nest-engraving-seals.js");
const Pair = require("../../charm-nest-pair.js");
const Rose = require("../../charm-nest-rose.js");
const MasterIdx = require("./_charmNestMaster");
const Files = require("./_charmEngraveFiles");

const WORLD_LINES = "Charm_World_Lines", NAME = "engrave-classify";
const READ_STATES = new Set(["pooled", "written"]);
const OPEN = Classify.OPEN;
const DECIDED = ["approved", "written", "skipped"];
const MAX_PIECES = 400;
/* The job fields a line's engraving record carries (charm-nest-engrave-sync.js JOB_FIELDS: the page's sync sends exactly these, and takes them back). */
const JOB_FIELDS = ["face", "slot", "rowKey", "groupKey", "linked", "pre", "state", "reason", "text", "lines", "lineInput", "lineMode", "splitIndex", "lineGap", "decision", "source", "quote",
  "confidence", "requests", "questions", "claudeReasoning", "ignored", "wordsSource", "claudeJob", "claude", "fontKey", "fontAsked", "fontName", "fontStyle", "faceFontWord", "fontLocked",
  "fontMapped", "fontListing", "fontSubstituted", "wantSize", "nudged", "turnUp", "upSystem", "orientVersion", "approvedBy", "approvedAt", "engravingSeals", "decidedAt", "decidedBy",
  "stagedFrom", "copyOverrides", "archived", "asks", "applied"];
/* PIECES (the page's PIECE_OWN and WORDS_OF): what a piece keeps of its own when it takes a whole-line job's decision, and what a piece the line gained takes of its sibling's. */
const PIECE_OWN = new Set(["key", "row", "slot", "rowKey", "groupKey", "copies", "copyOverrides", "linked", "face", "engraveRec", "backs", "migratedFrom", "copiedFrom", "archived", "seededFrom", "pre", "t"]);
const WORDS_OF = ["text", "lines", "lineInput", "lineMode", "source", "quote", "confidence", "requests", "questions", "claudeJob", "claudeReasoning", "wordsSource", "faceFontWord", "fontKey", "fontAsked", "font", "fontName", "lineGap", "asks", "applied"];
const NEVER_WHY = "Earrings are never engraved (the shop's rule)";
const num = v => (Number.isFinite(+v) ? +v : 0);
const plain = v => { try { return v == null ? null : JSON.parse(JSON.stringify(v)); } catch (_) { return null; } };
const tlHash = s => { let a = 2166136261; for (const ch of String(s)) a = Math.imul(a ^ ch.charCodeAt(0), 16777619); return (a >>> 0).toString(36); };   // (CNTimeline.hash: the page's timeline ids)
const passThrough = e => !!(e && (e.fenced || e.yielded || e.fatal || e.overBudget));
/* KEEPLIST (Paul, 22 Nov 2026: order 4242358457 never reached Design & Engrave; no engraving was read in the cloud from 21 Nov 16:55): the driver saves a pass item's cursor with
   set(..., { merge: true }), and Firestore MERGES maps key by key, so a line taken off the `pending` map stayed in the saved one: the list only grew, every pass read the same first 300
   lines again (all done) and never the ones behind them. The lists are saved as arrays now (an array is one value: a merge replaces it whole); a cursor saved as a map is still read. */
const keysOf = v => (Array.isArray(v) ? v.map(String) : Object.keys(v || {}));
const faceOf = x => Faces.faceOf(x);
const isFront = x => faceOf(x) === "front";
const lineOf = r => (r && r.parentRow) || r;
const neverOf = row => { const r = lineOf(row); return Faces.neverOf ? Faces.neverOf(r) : (r && r.spec && r.spec.engraveNever) || null; };
const neverWhy = row => (neverOf(row) || {}).why || NEVER_WHY;
/** A job nobody has touched (the page's untouched): a face the setup no longer asks for goes only when it is this. */
const untouched = j => ["classify", "reclassify", "words", "ready", "blocked", "fitting", "review"].includes(j.state) && !j.decision && !j.nudged && !j.fontLocked && !j.approvedAt && !j.approvalIntent;

/* STANDING (Paul, 23 Nov 2026: orders 4234003099, 4236658718 and 4223534659 were approved, their engraving files saved on their sheets, and the line's record said "review", approvedAt 0, on every screen; RECBACK could not
   put them back: it reads the job, and the job itself had been written back to review). One rule: a face whose saved engraving (Charm_Pool_Back) still stands (approved by a named person, not withdrawn, on the sheet its piece
   is on now, of the piece's own SKU, made after the last change of its artwork) for the very same words, font and place IS approved. Every real change withdraws that file or no longer matches it, so nothing a person
   changed comes back approved. A job of one face is put back only when EVERY piece of it has such a file. */
const STAND_OPEN = new Set(["review", "ready", "fitting"]);
const standNear = (a, b, e) => Number.isFinite(+a) && Number.isFinite(+b) && Math.abs(+a - +b) <= e;
const standOpen = j => !!j && !j.archived && !j.approving && STAND_OPEN.has(j.state) && !(num(j.approvedAt) > 0) && !!j.place && Array.isArray(j.place.centre) && !(j.decision && (j.decision.none || j.decision.removed));
/** The approval a job's saved files still hold: { at, by } or null. backs: the order's back records; rows: poolId -> its pool row (sheetId, sku, artEdit). */
function standingOf(job, lineKey, poolIds, backs, rows) {
  if (!standOpen(job)) return null;
  const tr = String(lineKey).split("_")[1] || "", slot = job.slot || null, front = isFront(job), ids = new Set((poolIds || []).map(String)), p = job.place, words = v => String(v == null ? "" : v);
  const mine = (backs || []).filter(b => b && ids.has(String(b.poolId)) && String(b.transactionId || String(b.poolId).split("_")[1] || "") === tr && (b.slot || null) === slot && isFront(b) === front);
  if (!mine.length || (!slot && [...ids].some(id => !mine.some(b => String(b.poolId) === id)))) return null;
  for (const b of mine) {
    const r = rows.get(String(b.poolId)), art = r && r.artEdit && typeof r.artEdit === "object" ? num(r.artEdit[front ? "front" : "back"]) : 0, c = Array.isArray(b.centre) ? b.centre : [];
    if (b.invalidated !== false || b.pending || !(num(b.approvedAt) > 0) || !words(b.approvedBy).trim() || !b.sheetId || !r || words(r.sheetId) !== words(b.sheetId) || art > num(b.approvedAt) || num(b.invalidatedAt) > num(b.approvedAt)) return null;
    if (b.sku && r.sku && words(b.sku).trim().toUpperCase() !== words(r.sku).trim().toUpperCase()) return null;
    if (words(b.text) !== words(job.text) || (b.fontKey && job.fontKey && b.fontKey !== job.fontKey)) return null;
    if (!standNear(p.centre[0], c[0], 0.02) || !standNear(p.centre[1], c[1], 0.02) || !standNear(p.size, b.sizePt, 0.01) || !standNear(num(p.angle), num(b.angle), 0.5)) return null;
  }
  const last = mine.reduce((a, b) => (num(b.approvedAt) > num(a.approvedAt) ? b : a));
  return { at: num(last.approvedAt), by: words(last.approvedBy) };
}
/** The record with every such job put back as saved (written, its own approval), or null when there is none. */
function healStanding(rec, lineKey, poolIds, backs, rows) {
  let n = 0; const jobs = ((rec && rec.jobs) || []).map(j => { const s = standingOf(j, lineKey, poolIds, backs, rows); if (!s) return j; n++; return Object.assign({}, j, { state: "written", approvedAt: s.at, approvedBy: s.by, reason: null }); });
  return n ? Object.assign({}, rec, { jobs }) : null;
}
/** One line's record looked at (reads its order's back records and its pieces' rows, only when a job of it is open). */
async function standing(ctx, key, rec, poolIds) {
  if (!rec || !(poolIds || []).length || !(rec.jobs || []).some(standOpen)) return null;
  const ids = [...new Set(poolIds.map(String))].slice(0, 60);
  const [bs, ps] = await Promise.all([ctx.db.collection("Charm_Pool_Back").where("order", "==", String(key).split("_")[0]).limit(200).get(), ctx.db.getAll(...ids.map(id => ctx.db.collection("Charm_Pool").doc(id)))]);
  const backs = []; bs.forEach(d => backs.push(d.data() || {}));
  return backs.length ? healStanding(rec, key, ids, backs, new Map(ps.filter(s => s.exists).map(s => [s.id, s.data() || {}]))) : null;
}

/* SETBAR: the reader's one hook (the page installs the same one, charm-nest-bridge.js): a set's piece hangs on the chain CharmNestOrders.setPiecesOf names. */
if (!Read.slotInfo) Read.slotInfo = (row, slot) => {
  try { const pcs = O.setPiecesOf(lineOf(row)), p = pcs && pcs.find(x => x.slotKey === String(slot).replace(/\.\d+$/, "")), len = p ? parseFloat(p.length) : 0; return len > 0 ? { length: len } : null; } catch (_) { return null; }
};

/** What one pass reads once: the shop's engraving confidence, the pool rows, library entries, sheets and sets the pieces are read from, the preview ids of the lines not pooled yet,
 *  and the pieces' artwork edits. */
function passEnv(ctx) {
  const rows = new Map(), entries = new Map(), sheets = new Map(), previews = new Map(), arts = new Map();
  let settings = null, sets = null;
  const sctx = { poolRow: id => rows.get(id) || null, charmOf: () => null, entryFor: sku => entries.get(String(sku || "").trim().toUpperCase()) || null, pair: Pair, mergeSeals: (...r) => Seals.merge(...r), previewIds: row => previews.get(lineOf(row).key) || [] };
  const P = {
    rows, entries, sheets, sctx,
    async settings() {
      if (settings) return settings;
      try { const r = await ctx.read({ op: "shareGet", names: ["sorter-settings"] }), it = r && r.items && r.items["sorter-settings"]; settings = require("./_charmNestPieceHost").sharedValues(it && it.f) || {}; } catch (_) { settings = {}; }
      return settings;
    },
    async entry(sku) {
      const k = String(sku || "").trim().toUpperCase(); if (!k) return null;
      if (!entries.has(k)) { let e = null; try { const s = await ctx.db.collection(MasterIdx.INDEX).doc(k).get(); e = s.exists ? MasterIdx.slimEntry(s.data()) : null; } catch (_) { e = null; } entries.set(k, e); }
      return entries.get(k);
    },
    /** The line's pool rows, its design's entry, the sheets its pieces are on and the run's sets (the laser-cut rule reads them). */
    async load(row) {
      const want = (row.poolIds || []).filter(id => id && !rows.has(id));
      for (let i = 0; i < want.length; i += 300) { const got = await ctx.read({ op: "poolGet", poolIds: want.slice(i, i + 300) }); for (const [id, r] of Object.entries((got && got.pools) || {})) rows.set(id, r); }
      await P.entry(row.spec && row.spec.designSku);
      const sids = [...new Set((row.poolIds || []).map(id => rows.get(id)).filter(p => p && p.sheetId).map(p => p.sheetId))].filter(id => !sheets.has(id));
      for (const id of sids) { let s = null; try { const r = await ctx.read({ op: "getSheet", id }); s = (r && r.sheet) || null; if (s && !s.id) s.id = id; } catch (_) { s = null; } sheets.set(id, s); }
      if (sids.length && !sets) { try { const q = await ctx.db.collection("Charm_Nest_Sets").where("runId", "==", ctx.runId).get(); sets = q.docs.map(d => d.data() || {}); } catch (_) { sets = []; } }
    },
    /** ENGPRE (the page's Pool.preview / previewIds): the pool ids a line's pieces WILL get, for a line that has none yet and whose design would make its charm. */
    async preview(row) {
      const k = row.key; if ((row.poolIds || []).length) { previews.set(k, []); return []; }
      const sp = row.spec || {}, entry = await P.entry(sp.designSku);
      if (!entry || O.blockFor(entry, sp.designSku, row.blockOverride) || sp.noDesign || sp.ownDesign || sp.formHold) { previews.set(k, []); return []; }
      const fixed = Number.isInteger(+sp.pieceCount) && +sp.pieceCount >= 1 && +sp.pieceCount <= MAX_PIECES ? +sp.pieceCount : 0, count = Math.min(MAX_PIECES, fixed || Math.max(1, Math.round(+sp.quantity || 1)));
      const ids = Array.from({ length: count }, (_, i) => O.poolId(row.order, row.line, i + 1));   // (charm-nest-pool-make.js: copy 1 .. count)
      previews.set(k, ids); return ids;
    },
    previewIds: row => previews.get(lineOf(row).key) || [],
    /** The pieces' artwork edits (Charm_Pool_Art through artList), for EDITOR WORDS. */
    async loadArts(ids) {
      const want = [...new Set(ids)].filter(id => id && !arts.has(id + "|r"));
      for (let i = 0; i < want.length; i += 100) {
        const part = want.slice(i, i + 100); let got = {};
        try { const r = await ctx.read({ op: "artList", poolIds: part }); got = (r && r.arts) || {}; } catch (_) { got = {}; }
        for (const id of part) { arts.set(id + "|r", true); for (const f of ["front", "back"]) { const k = Faces.recKey(id, f); if (got[k]) arts.set(k, got[k]); } }
      }
    },
    art: (id, face) => arts.get(Faces.recKey(id, face)) || null,
    /** The sheet a piece is on (Placement's rule: the record is authoritative), or null. */
    sheetOf(id) {
      const p = rows.get(id); if (!p || !p.sheetId || ["abandoned", "superseded"].includes(p.state)) return null;
      const sh = sheets.get(p.sheetId); return sh && !sh.archived && Array.isArray(sh.poolIds) && sh.poolIds.includes(id) ? sh : null;
    },
    /** The page's replaceCutWhy over a sheet record: laser cut, cut, a set marked laser done, or a piece inside a protected green-line contour. */
    cutWhy(sh, ids) {
      if (!sh) return "";
      if (+sh.laserDoneAt > 0) return "laser cut";
      if (sh.roseCutAt) return "cut";
      if ((sets || []).some(s => +s.laserDoneAt > 0 && (s.sheetIds || []).includes(sh.id))) return "laser cut";
      let prot = sh.roseProtected || null; if (!prot && sh.roseProtectedJson) { try { prot = JSON.parse(sh.roseProtectedJson); } catch (_) { prot = null; } }
      const inside = !ids ? true : (() => { const ps = new Set(((prot && prot.placements) || []).map(p => p.id)); return (sh.charms || []).some(c => ids.has(c.poolId) && ps.has(c.id)); })();
      if ((sh.metal === "rose" || Rose.cuts(sh.metal)) && (sh.rosePlan || sh.rosePlanJson || prot) && inside) return `inside the protected ${Rose.cutWord ? Rose.cutWord(sh.metal) : "Rose Gold"} contour (cut)`;
      return "";
    }
  };
  return P;
}

/** The world's reading of each line (Charm_World_Lines: its order, its Etsy line, its spec), read in one go. */
async function worldLines(ctx, keys) {
  const out = new Map();
  for (let i = 0; i < keys.length; i += 300) {
    const snaps = await ctx.db.getAll(...keys.slice(i, i + 300).map(k => ctx.db.collection(WORLD_LINES).doc(k)));
    for (const s of snaps) { if (!s.exists) continue; const d = s.data() || {}; if (d.gone) continue; let w = null; try { w = typeof d.data === "string" ? JSON.parse(d.data) : null; } catch (_) { w = null; } if (w && w.spec) out.set(s.id, w); }
  }
  return out;
}

/** The line as the reading reads it: the world's order, line and spec (a copy: raiseCandidate and BARFRONT's keep may change it), the run's state and pieces, a person's setup, the summary. */
function rowOf(key, w, runLine, rec) {
  const ov = (rec && rec.override) || runLine.engravingOverride || null;
  const row = { key, order: w.order, line: w.line, spec: plain(w.spec), state: runLine.state || w.state, poolIds: (runLine.poolIds || []).slice(), engrave: plain((rec && rec.engrave) || runLine.engrave || null) };
  if (ov) row.engravingOverride = plain(ov);
  if (runLine.blockOverride) row.blockOverride = plain(runLine.blockOverride);
  return row;
}
/** ENGPRE (the page's earlyWanted): a line that carries an engraving that can be prepared before its pieces are made. */
function earlyWanted(row, candidate) {
  const sp = row && row.spec;
  if (!sp || !sp.designSku || row.state === "gone" || O.FINISHED_LINE.has(row.state) || neverOf(row)) return false;
  if (sp.noDesign || sp.ownDesign || sp.formHold || (sp.special && (sp.special.notCut || sp.special.own))) return false;
  return !!candidate;
}
/** ENGPRE (the page's madeUsual): the line carries a job made the usual way (its piece pooled, or one a person touched): that path owns it. */
const madeUsual = (row, js) => js.some(j => !j.pre && ((row.poolIds || []).length > 0 || !untouched(j)));
const raiser = () => Classify.make({ items: () => new Map(), FACES: () => Faces, READER: () => Read, SIDES: () => Sides, O: () => O, seals: () => Seals, neverEngraves: neverOf, faceOf, finished: r => O.FINISHED_LINE.has(r.state) });

/** One line: BARFRONT, its jobs made (again), the shared reading over them. → { rec (null: nothing changed), ready, events, copiesOf } */
async function readLine(ctx, P, key, row, l, hooks) {
  const old = (l && l.rec) || null, items = new Map(), ready = [], events = [], free = !!hooks.free, pooled = !!hooks.pooled, candidate = !!hooks.candidate;
  const grp = String(row.order && row.order.receiptId || "") + ":" + String(row.line && row.line.transactionId || "");
  const tid = row.line && row.line.transactionId;
  /* ── the line's jobs (the page's jobsOf / ensureOne / ensureJobs / ensureFaced / migrateToPieces / settlePieces) ── */
  const rk = j => (j.slot ? Sides.slotRank(j.slot) : -1);
  function jobsOf(r, o) {
    const out = [...items.values()].filter(j => (o && o.all) || !j.archived);
    out.sort((a, b) => rk(a) - rk(b) || (isFront(a) ? 0 : 1) - (isFront(b) ? 0 : 1));
    return o && o.face ? out.filter(j => faceOf(j) === o.face) : out;
  }
  const planRowOf = r => { const ids = (r.poolIds || []).length ? [] : P.previewIds(r); return ids.length ? Object.create(r, { poolIds: { value: ids } }) : r; };
  const newJob = key => ({ key, lineGap: .216, state: "classify", text: null, lines: [], source: null, quote: null, confidence: null, requests: null, questions: [], decision: null, claude: null, approvedBy: null, approvedAt: null, reason: null });
  function placeCopies(j, r, slot) {
    const was = j.copies || [];
    j.copies = (j.row.poolIds || []).filter(id => !(j.copyOverrides || []).includes(id));
    // ENGPRE: a line not pooled yet: its job carries the ids the piece WILL get
    if (!j.copies.length) { const pre = P.previewIds(r); if (pre.length) { j.copies = pre.filter(id => !(j.copyOverrides || []).includes(id) && (!slot || Sides.slotOfId(P.sctx, r, id) === slot)); if (j.copies.length) j.pre = true; } else if (j.pre && was.length && !(r.poolIds || []).length) j.copies = was.slice(); }
  }
  function ensureOne(r, slot, face, linked) {
    const front = face === "front", k = Faces.jobKey(r.key, slot, face); let j = items.get(k);
    if (!j) { j = newJob(k); j.row = r; if (front) j.face = "front"; if (slot) Object.assign(j, { slot, rowKey: r.key, groupKey: grp }); else if (front || linked) Object.assign(j, { rowKey: r.key, groupKey: grp }); items.set(k, j); }
    if (slot || front || linked) {
      if (!j.row || j.row.parentRow !== r) {
        if (!slot && !front) { j.rowKey = r.key; j.groupKey = j.groupKey || grp; j.linked = true; }
        j.row = Sides.sideRow(P.sctx, r, slot, j);
      }
    }
    placeCopies(j, r, slot);
    return j;
  }
  function seedPiece(r, slot, face, src, grow) {
    const j = ensureOne(r, slot, face, true), c = {}; for (const k of Object.keys(src)) if (k !== "row" && k !== "copies") c[k] = plain(src[k]);
    if (grow) { for (const k of WORDS_OF) if (c[k] !== undefined) j[k] = c[k]; j.copiedFrom = src.key; return j; }
    for (const k of Object.keys(c)) if (!PIECE_OWN.has(k)) j[k] = c[k];
    j.migratedFrom = src.key;
    return j;
  }
  function migrateToPieces(r, have, pl) {
    const live = new Set([...pl.of.values()].filter(Boolean)); let moved = false;
    for (const o of have.filter(j => !j.slot && items.get(j.key) === j)) { const face = faceOf(o); for (const slot of pl.slots) if (slot && live.has(slot)) seedPiece(r, slot, face, o, false); items.delete(o.key); moved = true; }
    for (const slot of pl.slots) {
      const info = slot && Sides.slotInfo(slot); if (!info || info.unit < 2 || info.kind === "copy" || !live.has(slot)) continue;
      const mineIds = Sides.idsOfSlot(P.sctx, r, slot);
      for (const face of ["back", "front"]) {
        if (items.get(Faces.jobKey(r.key, slot, face))) continue;
        const src = items.get(Faces.jobKey(r.key, info.base, face)); if (!src) continue;
        if ((src.copies || []).some(id => mineIds.includes(id))) { seedPiece(r, slot, face, src, false); moved = true; }
        else if (src.text) { seedPiece(r, slot, face, src, true); moved = true; }
      }
    }
    return moved;
  }
  function settlePieces(r, pl) {
    if (!pl || !pl.byUnit || !(r.poolIds || []).length) return;
    const live = new Set([...pl.of.values()].filter(Boolean));
    for (const j of jobsOf(r, { all: true })) {
      if (!j.slot) continue;
      if (live.has(j.slot)) { if (j.archived) delete j.archived; continue; }
      if (DECIDED.includes(j.state) || j.state === "none") { if (!j.archived) j.archived = Date.now(); }
      else if (items.get(j.key) === j) items.delete(j.key);
    }
  }
  let BF = null;
  function faceWant(r, pl, have) {
    const setup = Faces.setupOf(r), anyFront = have.some(isFront);
    if (!anyFront && !setup.faces.includes("front")) return null;
    if (!anyFront && BF && BF.stays(r)) return null;   // (BARFRONT: a bar line whose back a person decided keeps that back alone)
    if (O.FINISHED_LINE.has(r.state)) return have.map(j => ({ slot: j.slot || null, face: faceOf(j) }));
    return Faces.planFaces(setup, pl.slots);
  }
  function ensureFaced(r, have, backs, pl, want) {
    const o = backs.find(j => !j.slot), wanted = new Set(want.map(w => Faces.jobKey(r.key, w.slot, w.face)));
    for (const w of want) ensureOne(r, w.slot, w.face, true);
    if (o && pl.split && !wanted.has(o.key) && !DECIDED.includes(o.state)) for (const j of jobsOf(r).filter(x => !isFront(x) && x !== o)) if (!j.text && o.text) Object.assign(j, { text: o.text, lines: (o.lines || []).slice(), source: o.source, quote: o.quote, confidence: o.confidence, requests: o.requests, questions: (o.questions || []).slice(), seededFrom: o.key });
    for (const j of have) if (!wanted.has(j.key) && items.get(j.key) === j && untouched(j)) items.delete(j.key);
    settlePieces(r, pl);
    return jobsOf(r);
  }
  function ensureJobs(r) {
    r = lineOf(r);
    let have = jobsOf(r), backs = have.filter(j => !isFront(j));
    const pl = Sides.plan(P.sctx, planRowOf(r), backs);
    if (pl.byUnit && pl.split && migrateToPieces(r, have, pl)) { have = jobsOf(r); backs = have.filter(j => !isFront(j)); }
    const want = faceWant(r, pl, have);
    if (want) return ensureFaced(r, have, backs, pl, want);
    if (!pl.split) return [ensureOne(r, null)];
    const o = have.find(j => !j.slot);
    const jobs0 = pl.slots.map(s => ensureOne(r, s));
    settlePieces(r, pl);
    const jobs = pl.byUnit ? jobs0.filter(j => items.get(j.key) === j && !j.archived) : jobs0;
    if (o && !DECIDED.includes(o.state)) {
      for (const j of jobs) if (!j.text && o.text) Object.assign(j, { text: o.text, lines: (o.lines || []).slice(), source: o.source, quote: o.quote, confidence: o.confidence, requests: o.requests, questions: (o.questions || []).slice(), seededFrom: o.key });
      items.delete(o.key);
    }
    return jobs;
  }
  // the record's jobs, each on its row (a piece's or a face's own side row), with its pieces
  const had = new Set();
  for (const o of (old && Array.isArray(old.jobs) ? old.jobs : [])) {
    if (!o || !o.key) continue;
    const j = plain(o), sided = !!(j.slot || isFront(j) || j.linked);
    j.row = sided ? Sides.sideRow(P.sctx, row, j.slot || null, j) : row; placeCopies(j, row, j.slot || null); items.set(j.key, j); had.add(j.key);
  }

  /* ── the three ends of a reading (the page's setNone, toWords with EDITOR WORDS, setReady), and the order's timeline ── */
  /** CUSTOMBACK (26 Nov 2026): the job's piece is cut from a charm whose custom file drew its back (its pool row's fileBack, written when it was made): that drawing is its back's art */
  const fileBackOf = j => { const id = j && (j.copies || [])[0], r = id && P.sctx && P.sctx.poolRow ? P.sctx.poolRow(String(id)) : null; return !!(r && num(r.fileBack) > 0); };
  const setNone = (j, why) => { j.state = "none"; j.reason = why; j.row.engrave = Object.assign({}, j.row.engrave, { needed: false, state: "none", reason: why, approved: true }); return j; };
  /** EDITOR WORDS: the face's artwork edit adds parts (a back's added cut lines are refused by the editor, so they do not count) */
  const editorArt = j => { const id = (j.copies || [])[0]; if (!id) return false; if (!isFront(j) && fileBackOf(j)) return true; const a = P.art(id, faceOf(j)); return !!a && Array.isArray(a.added) && a.added.some(e => e && (isFront(j) || (e.role !== "cut" && e.artRole !== "cut"))); };
  const artOnly = j => {   // (the page's artOnlyPlacement: no words, the editor's parts are what is engraved; it waits for Approve)
    Object.assign(j, { lines: [], lineInput: [], text: "", wantSize: null, nudged: false, claude: null, reason: null, missing: null, state: "review" });
    j.row.engrave = Object.assign({}, j.row.engrave, { needed: true, state: "review", text: "", approved: false });
    return j;
  };
  const toWords = (j, why) => {
    if (neverOf(j.row) && !["approved", "written"].includes(j.state)) return setNone(j, neverWhy(j.row));
    if (editorArt(j) && !(j.decision && j.decision.text)) return artOnly(j);
    j.state = "words"; j.reason = why; j.row.engrave = Object.assign({}, j.row.engrave, { needed: true, state: "words", text: j.text, approved: false, reason: why }); return j;
  };
  const setReady = async j => {
    const r = j.row;
    if (items.get(j.key) !== j || r.state === "gone" || j.stamping || j.backSaving || ["approved", "written", "skipped", "none"].includes(j.state)) return j;
    if (neverOf(r)) return setNone(j, neverWhy(r));
    if (!j.decision && (j.lines || []).some(x => /\b(?:https?:\/\/|www\.)\S/i.test(x))) { j.askRef = true; return toWords(j, "A web link was sent with the order: it is not engraved. Say what goes on the charm"); }
    // (the fonts and the glyphs: the fit unit reads them in the shop's fonts and answers "Unsupported engraving characters" for a word the font cannot draw, as the page's setReady did)
    j.state = "ready"; j.reason = null; j.missing = null; j.row.engrave = Object.assign({}, j.row.engrave, { needed: true, state: "ready", text: j.text, approved: false });
    if (!j.editingBack && !ready.includes(j)) ready.push(j);   // (a piece with no pool row yet, ENGPRE, is flagged by no one: its pool row is written with the flag when it is pooled)
    return j;
  };
  const tlLine = (r, type, o) => {   // (the page's CNTimeline.line)
    const { once, ...rest } = o || {};
    events.push(Object.assign({ orderId: String(row.order.receiptId), type, lineKey: lineOf(r).key }, tid != null ? { transactionId: String(tid) } : {}, rest, { at: (o && o.at) || Date.now(), by: "Cloud" }));
  };
  const needEvent = j => {
    try {
      if (!j || !j.row || !j.row.engrave || !j.row.engrave.needed) return;
      const t = String(j.text || "").trim(), fw = isFront(j) ? "Front" : "Back", d = discSay(j, jobsOf(row));
      tlLine(j.row, "engraveNeeded", { id: j.row.key + "." + tlHash(t), once: true,
        text: (t ? `${fw} engraving${d ? " · " + d : ""}: “${t.replace(/\n/g, " / ")}”` : `${fw} engraving${d ? " · " + d : ""}: the words are to be confirmed`).slice(0, 200),
        data: Object.assign({ text: t, source: j.source || null, confidence: j.confidence == null ? null : j.confidence, state: j.state }, isFront(j) ? { face: "front" } : {}) });
    } catch (_) { /* the timeline line is a courtesy: the reading stands without it */ }
  };
  const say = (meta, kind, text) => { if (ctx.say) ctx.say(`[engrave classify] ${kind}: ${String(text).slice(0, 300)}`); };
  const settings = await P.settings();
  const C = Classify.make({
    items: () => items, ensureJobs, setNone, toWords, setReady, needEvent, agent: say,
    // (ENGNOTE: a line's jobs (this line's as they are now, another line's from its record) and the lines of its order, for the staff note's "which line")
    jobsOf: r => { const k = lineOf(r) && lineOf(r).key; return k === row.key ? jobsOf(row) : ((lineOf(r) && lineOf(r)._jobs) || []); },
    orderRows: () => (hooks.orderRows ? hooks.orderRows(row) : [row]),
    agentCall: (mode, ask, opts) => hooks.agentCall(row, mode, ask, opts),
    FACES: () => Faces, READER: () => Read, SIDES: () => Sides, ASKS: () => Asks, O: () => O, seals: () => Seals,
    neverEngraves: neverOf, neverWhy, NEVER_OPEN: j => !["approved", "written"].includes(j.state), faceOf,
    trust: () => settings.engraveConfidence,
    customSent: () => false,   // (a line sent to a custom sheet: the world's reading has engraveCandidate false for it already)
    fileBack: j => fileBackOf(j),   // (CUSTOMBACK)
    finished: r => O.FINISHED_LINE.has(r.state)
  });
  /** the page's classify(): the reading, then the timeline line (never for a line read before it is pooled: ENGPRE) */
  const classify = async (r, o) => { const j = await C.classifyOnce(r, o || {}); if (!(o && o.free)) needEvent(j); return j; };

  /* ── BARFRONT (charm-nest-engrave-bar-front.js, the page's module, over these jobs) ── */
  let tailDone = null;
  BF = BarFront.make({
    Engrave: { items: () => items, jobsOf, isFront, ensureJobs, neverEngraves: neverOf, classify, setReady, canFit: () => false, isWorking: () => false, fitJob: async () => {}, render: () => { if (tailDone) tailDone(); } },
    CharmNestEngraveFaces: Faces, CharmNestOrders: O, Pool: { sheetOf: id => P.sheetOf(id) }, replaceCutWhy: (sh, ids) => P.cutWhy(sh, ids),
    CNTimeline: { line: tlLine }, agent: say
  });
  if (jobsOf(row, { all: true }).length) {
    const tail = new Promise(res => { tailDone = res; });
    if (BF.move(row)) await tail;   // (the move's own reading and readiness of the fronts run after it: free, offline; their end is the render it asks for)
    else BF.keep(row);              // (a bar line a person decided reads as what was decided, in this pass's memory, as on the page)
    tailDone = null;
  }

  /* ── ENGNOTE: a new or changed staff note "ENG:" reads again the jobs that wait for a person, and a job that took an earlier note's words (the page asks it on every read) ── */
  C.engFollow(row);

  /* ── the jobs, made again for the line as it is now; then the reading of what is still to read ── */
  if (pooled && candidate && !jobsOf(row).some(isFront)) {   // (the page's wantsFront: a line whose setup now asks for a front: its untouched backs are read again with it)
    let want = false; try { want = Faces.setupOf(row).faces.includes("front") && !BF.stays(row); } catch (_) { want = false; }
    if (want && row.engrave) for (const j of jobsOf(row)) if (!isFront(j) && untouched(j) && j.wordsSource !== "spec.engraving") Object.assign(j, { state: "classify", lineInput: null, decision: null, wantSize: null, reason: null });
  }
  if (candidate || jobsOf(row).length) ensureJobs(row);
  if (!pooled) for (const j of jobsOf(row)) if (!had.has(j.key) && !(j.copies || []).some(id => P.sheetOf(id))) j.pre = true;   // (ENGPRE: the jobs made for a line before it is pooled)
  const open = jobsOf(row).filter(j => OPEN.includes(j.state) && !j.approving && (pooled || j.pre));   // (ENGPRE: only a job made for its piece-to-be is read early, and only for free)
  if (candidate && open.length) {
    await P.loadArts(jobsOf(row).flatMap(j => j.copies || []));
    await classify(row, free ? { free: true } : {});
  } else if (!candidate && pooled) {   // (classifyAll: a line that waited to be read and no longer has anything to read is plain now, and so is its waiting job)
    const e = row.engrave;
    if (e && (OPEN.includes(e.state) || (e.state === "words" && !e.text))) for (const j of jobsOf(row)) if (["classify", "words"].includes(j.state) && !j.decision) setNone(j, "no personalisation, message or note");
  }

  /* ── the record, written whole: each job's own fields as the page's sync sends them, the rest of what the record held kept ── */
  const byKey = new Map((old && Array.isArray(old.jobs) ? old.jobs : []).filter(o => o && o.key).map(o => [o.key, o]));
  const jobs = jobsOf(row, { all: true }).map(j => { const n = Object.assign({}, byKey.get(j.key) || { key: j.key }); for (const f of JOB_FIELDS) { if (j[f] === undefined) delete n[f]; else n[f] = plain(j[f]); } return n; });
  if (!jobs.length && !old) return { rec: null, ready, events, copiesOf: j => j.copies || [] };
  const rec = Object.assign({}, old || { v: 1 }, { override: plain(row.engravingOverride || (old && old.override) || null), raised: !!((row.spec && row.spec.engraveCandidate) || (old && old.raised)), jobs });
  // (a record whose jobs, setup and raised mark are as they were is not written again: its summary stays as its writer made it)
  const same = !!old && JSON.stringify(jobs) === JSON.stringify(old.jobs || []) && JSON.stringify(rec.override) === JSON.stringify(old.override == null ? null : old.override) && rec.raised === !!old.raised;
  rec.engrave = Files.lineSummary({ piece: { sidesCtx: () => P.sctx } }, { row }, rec) || rec.engrave || null;
  const live = new Map(jobsOf(row, { all: true }).map(j => [j.key, j]));
  return { rec: same || JSON.stringify(rec) === JSON.stringify(old) ? null : rec, ready: ready.filter(j => live.get(j.key) === j && j.state === "ready"), events, copiesOf: j => j.copies || [] };
}
/** "Disc 2 of 3" / "Piece 2 of 3" for the timeline line (the page's discSay); "" for a single, an ear or a plain line. */
function discSay(job, jobs) {
  try {
    const S = Sides, info = job && job.slot ? S.slotInfo(job.slot) : null; if (!info) return "";
    if (info.kind === "copy" || info.unit > 1 || (S.shape(jobs.map(x => x.slot)).multi)) {
      const shp = S.shape(jobs.map(x => x.slot)), r = S.pieceRecord ? S.pieceRecord(job, { of: shp.total, shape: shp }) : null;
      return info.kind === "copy" ? `Piece ${info.n} of ${shp.total}` : r ? r.label : S.labelOf(job.slot, shp);
    }
    if (info.kind !== "disc") return "";
    const r = S.pieceRecord ? S.pieceRecord(job) : null;
    return S.nounWord(r && r.of > 1 ? `Disc ${r.index} of ${r.of}` : `Disc ${String(job.slot).slice(1)}`, job);
  } catch (_) { return ""; }
}

/* ═══ the unit ═══════════════════════════════════════════════════════════ */
async function classifyUnit(ctx) {
  if (["stopped", "abandoned", "complete"].includes(ctx.run.status)) return;   // (as the page: the reading runs beside a run that is not stopped)
  const lines = ctx.run.lines || {}, cur = ctx.cursor || {}, fresh = cur.runId !== ctx.runId;
  const keys = new Set(), standOnly = new Set(), waiting = new Set(fresh ? [] : (cur.waiting || []).filter(k => lines[k]));
  let since = fresh ? 0 : num(cur.since), newest = since, more = false;
  if (fresh) { for (const [k, l] of Object.entries(lines)) if (l && !O.FINISHED_LINE.has(l.state) && l.state !== "gone") keys.add(k); newest = Date.now() - 5000; }
  else {
    for (const k of keysOf(cur.pending)) if (lines[k]) keys.add(k);
    for (const k of waiting) if (READ_STATES.has((lines[k] || {}).state)) { keys.add(k); waiting.delete(k); }   // (a line that waited for its pieces and has them now)
    // (the lines whose world reading or engraving record changed since the last pass)
    const ws = await ctx.db.collection(WORLD_LINES).where("at", ">", since).orderBy("at").limit(500).get();
    ws.forEach(d => { const x = d.data() || {}; newest = Math.max(newest, num(x.at)); if (lines[d.id]) keys.add(d.id); });
    const r = await ctx.read({ op: "engraveSyncGet", since }), got = (r && r.lines) || [];
    for (const l of got) { newest = Math.max(newest, num(l.at)); if (lines[l.key]) keys.add(l.key); }
    if (ws.size >= 500 || got.length >= 500) more = true;
    // (STANDING: the lines already written back to review before this rule are looked at once)
    if (num(cur.stood) < 1) for (const [k, l] of Object.entries(lines)) if (l && READ_STATES.has(l.state) && l.engrave && !l.engrave.approved && STAND_OPEN.has(l.engrave.state)) keys.add(k);
    /* STANDING 2 (Paul, 23 Nov 2026, item 10: 12 more lines, 4237468667, 4239976781, 4236980485, 4236215767, 4242448021, 4235376193's front, 4243340489, 4243389696, 4243222905, 4243254939, 4244016754, 4243151567): approved, the file
       saved, and the line's record written back to review by the approving computer four minutes later (before KEEPREV), while the run line still said approved: the look above, which asks the run line, never saw them. Every line the
       run says is approved is looked at once by its own record: one with an open job is healed by the same rule, and only healed (never read again here). */
    if (num(cur.stood2) < 1) {
      const look = Object.entries(lines).filter(([k, l]) => l && READ_STATES.has(l.state) && l.engrave && l.engrave.approved && l.engrave.state !== "none" && !keys.has(k)).map(([k]) => k);
      const got = look.length ? await Files.readLines(ctx, look) : new Map();
      for (const [k, l] of got) if (l && l.rec && (l.rec.jobs || []).some(standOpen)) standOnly.add(k);
    }
  }
  const all = [...standOnly, ...keys], pending = {};
  for (const k of all.slice(300)) pending[k] = true;
  if (all.length > 300) more = true;
  const todo = all.slice(0, 300);
  if (!todo.length) return Object.assign({ cursor: { runId: ctx.runId, stood: 1, stood2: 1, since: newest, pending: Object.keys(pending).slice(0, 1500), waiting: [...waiting].slice(0, 1500), reading: [], readingAt: 0 } }, more ? { more: true } : {});
  const [world, recs] = await Promise.all([worldLines(ctx, todo), Files.readLines(ctx, todo)]);
  /* ENGNOTE: the other lines of an order whose staff messages carry an "ENG:" note (which line the words are for is the order's question), read with the lines themselves */
  const engOrders = new Set(todo.filter(k => { const w = world.get(k); return w && Classify.engNoteOf(w.spec && w.spec.messages); }).map(k => String(k).split("_")[0]));
  if (engOrders.size) {
    const sib = Object.keys(lines).filter(k => engOrders.has(String(k).split("_")[0]) && !world.has(k) && !todo.includes(k));
    if (sib.length) { const [w2, r2] = await Promise.all([worldLines(ctx, sib), Files.readLines(ctx, sib)]); for (const [k, v] of w2) world.set(k, v); for (const [k, v] of r2) recs.set(k, v); }
  }
  const sibRow = k => { const w = world.get(k), rl = lines[k], l = recs.get(k); if (!w || !rl) return null; const r = rowOf(k, w, rl, l && l.rec); r._jobs = (l && l.rec && Array.isArray(l.rec.jobs) ? l.rec.jobs : []); return r; };
  const orderRows = row => { const rid = String(row.key).split("_")[0]; if (!engOrders.has(rid)) return [row]; return Object.keys(lines).filter(k => String(k).split("_")[0] === rid).map(k => (k === row.key ? row : sibRow(k))).filter(Boolean); };
  const P = passEnv(ctx), plainLines = {}, reading = {};
  let stash = null;
  /* the paid reader (the page's agentCall, over the cloud's own op): a reading asked before is taken up by its id; a new one is started once and answered at a later pass */
  const agentCall = async (row, mode, ask, opts) => {
    if (mode !== "engraveIntent") return { skipped: "the cloud reads only the words" };
    try {
      let id = opts && opts.existingId || null;
      if (id) {
        const g = await ctx.read({ op: "getAgent", id }), job = g && g.job;
        const failed = !job || job.status === "error" || (job.status === "done" && job.result && (job.result.skipped || job.result.error));
        if (failed && opts.retryFailed) id = null;   // (a reading that failed is replaced once, as the page's retryFailed)
        else if (!job) return { skipped: "the reading vanished" };
        else if (job.status === "done") return job.result || {};
        else if (job.status === "error") return { skipped: job.error || "the reading failed" };
        else { reading[row.key] = id; return { pending: true }; }
      }
      const unit = `engrave-intent|${row.key}|${(opts && opts.askKey) || ""}${opts && opts.existingId ? "|again:" + opts.existingId : ""}`;
      const res = await ctx.call({ op: "engraveIntentStart", payload: ask }, unit);
      if (res && res.fenced) throw Object.assign(new Error(res.error || "fenced"), { fenced: true });
      if (!res || res.status >= 400 || !res.id) return { skipped: (res && res.error) || "the reading could not be started" };
      if (opts && opts.onStarted) opts.onStarted(res.id);
      reading[row.key] = res.id;
      return { pending: true };
    } catch (e) { if (passThrough(e)) { stash = stash || e; return { pending: true }; } throw e; }
  };
  for (const key of todo) {
    if (Date.now() > ctx.budgetEnd) { more = true; pending[key] = true; continue; }
    const runLine = lines[key], w = world.get(key), l = recs.get(key) || null;
    if (!runLine || !w || O.FINISHED_LINE.has(runLine.state) || runLine.state === "gone") continue;
    const pooled = READ_STATES.has(runLine.state), row = rowOf(key, w, runLine, l && l.rec);
    const live = (l && l.rec && Array.isArray(l.rec.jobs) ? l.rec.jobs : []).filter(j => j && j.key && !j.archived);
    const candidate = !!row.spec.engraveCandidate || raiser().raiseCandidate(row);
    if (standOnly.has(key) && !(pooled && live.some(standOpen))) continue;   // (STANDING 2)
    if (!pooled) {
      waiting.add(key);   // (its pieces are not made yet: looked at again once they are)
      // ENGPRE (the page's early): the words of a line not pooled yet, on the pool ids its pieces will get, with no paid reading
      if (!earlyWanted(row, candidate) || madeUsual(row, live)) continue;
      if (!(await P.preview(row)).length) continue;
    } else if (!live.length) {
      // (the page's classifyAll: a line with no job is read when it is one to engrave (the intake's word, or raised) and its run line has no decided engraving yet; a pooled line with nothing to
      //  engrave is plain, and its run line says so once)
      if (!candidate) { if (runLine.state === "pooled" && !runLine.engrave) plainLines[key] = Files.runEngraveOf({ needed: false, state: "none", approved: true }); continue; }
      let front = false; try { front = Faces.setupOf(row).faces.includes("front"); } catch (_) { front = false; }
      if (runLine.engrave && !OPEN.includes(runLine.engrave.state) && !front) continue;   // (decided already; a setup that now asks for a front is looked at again: wantsFront)
    }
    await P.load(row);
    // (STANDING)
    if (pooled && live.some(standOpen)) {
      let up = null; try { up = await standing(ctx, key, l.rec, runLine.poolIds); } catch (e) { if (passThrough(e)) throw e; if (ctx.say) ctx.say(`[engrave standing] ${key}: ${String((e && e.message) || e).slice(0, 200)}`); }
      if (up) {
        try { up.engrave = Files.lineSummary({ piece: { sidesCtx: () => P.sctx } }, { row }, up) || up.engrave || null; } catch (e) { if (passThrough(e)) throw e; }
        const wr = await Files.writeLine(ctx, l, up, `engrave-standing|${key}|${l.rev || 0}`);
        if (wr.stale) { pending[key] = true; continue; }
        recs.set(key, { key, rec: up, rev: wr.rev }); await Files.patchRunLine(ctx, key, up.engrave, `engrave-line|${key}|${wr.rev}`);
        if (ctx.say) ctx.say(`${key}: its saved engraving still stands: shown approved again`);
        continue;   // (its other faces are read at the next pass: the record was just written)
      }
    }
    if (standOnly.has(key)) continue;   // (STANDING 2: looked at for its saved file alone)
    let got;
    try { got = await readLine(ctx, P, key, row, l, { agentCall, free: !pooled, pooled, candidate, orderRows }); }
    catch (e) { if (passThrough(e)) throw e; if (ctx.say) ctx.say(`[engrave classify] ${key}: ${String((e && e.message) || e).slice(0, 300)}`); pending[key] = true; continue; }
    if (stash) throw stash;
    if (got.rec) {
      { const up = pooled ? await standing(ctx, key, got.rec, runLine.poolIds).catch(e => { if (passThrough(e)) throw e; return null; }) : null; if (up) { got.rec = up; try { up.engrave = Files.lineSummary({ piece: { sidesCtx: () => P.sctx } }, { row }, up) || up.engrave || null; } catch (e) { if (passThrough(e)) throw e; } } }   // (STANDING: a reading never takes a saved approval off the run line)
      const wr = await Files.writeLine(ctx, l || { key }, got.rec, `engrave-classify|${key}|${(l && l.rev) || 0}`);
      if (wr.stale) { pending[key] = true; continue; }   // (a page wrote the line meanwhile: read again next pass)
      recs.set(key, { key, rec: got.rec, rev: wr.rev });   // (the order's other lines read this line as it is now: ENGNOTE)
      await Files.patchRunLine(ctx, key, got.rec.engrave, `engrave-line|${key}|${wr.rev}`);
    }
    // (the pieces of a job whose words are ready are flagged, as the page's setReady did, the pieces that have a pool row only (ENGPRE); best-effort, as there)
    for (const j of got.ready) { const ids = got.copiesOf(j).filter(id => P.rows.get(id) && !["abandoned", "superseded"].includes(P.rows.get(id).state)); if (!ids.length) continue;
      const flag = isFront(j) ? "engraveFront" : "engrave"; if (ids.every(id => P.rows.get(id)[flag] === true)) continue;
      try { await ctx.call({ op: "poolUpdate", poolIds: ids, patch: { [flag]: true } }, `engrave-ready|${j.key}|${tlHash(j.text || "")}`); } catch (e) { if (passThrough(e)) throw e; } }
    if (got.events.length) { try { await ctx.call({ op: "timelineAdd", events: got.events }, `engrave-tl|${key}|${got.events.map(e => e.id).join(",")}`); } catch (e) { if (passThrough(e)) throw e; } }
  }
  if (Object.keys(plainLines).length) {
    const res = await ctx.call({ op: "runLinesPatch", runId: ctx.runId, lines: Object.fromEntries(Object.entries(plainLines).map(([k, eng]) => [k, { engrave: eng }])) }, `engrave-plain|${Object.keys(plainLines).sort().join(",")}`);
    if (res && res.status < 400) for (const [k, eng] of Object.entries(plainLines)) if (lines[k]) lines[k].engrave = eng;
  }
  // (a reading under way: its line is looked at again at every pass until it is answered; the driver's tick reads readingAt so a waiting reading starts a pass)
  for (const k of Object.keys(reading)) pending[k] = true;
  const readingAt = Object.keys(reading).length ? (num(cur.readingAt) || Date.now()) : 0;
  return Object.assign({ cursor: { runId: ctx.runId, stood: 1, stood2: 1, since: newest, pending: Object.keys(pending).slice(0, 1500), waiting: [...waiting].slice(0, 1500), reading: Object.keys(reading), readingAt } }, more ? { more: true } : {});
}

const passItems = [{ name: NAME, lane: "engrave", order: 80, unit: classifyUnit }];
module.exports = { passItems, steps: {}, classifyUnit, readLine, NAME, JOB_FIELDS, standingOf, healStanding, standing };
