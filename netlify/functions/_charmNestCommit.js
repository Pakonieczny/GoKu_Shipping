/*  netlify/functions/_charmNestCommit.js — SENDING FINISHED SETS TO THE DESIGN STATION, FROM THE CLOUD (cloud plan v2 build step 12; hard-parts §2;
 *  E5, E6, E7, P11, P12, C2, m:P8). The run steps `commit` and `complete` as cloud driver units (_charmNestDrive.js), with no browser, no station page
 *  and no Etsy call.
 *
 *  What it replaces, and from where:
 *    RunCtl's commit step                      charm-nest-bridge.js ~16710-16728       commitUnit
 *    Sets.evaluate / commit / taken / record   bridge ~15927-15942, 16052-16118        evaluate, begin, the stages, recordStage
 *    Sets.validateRelease (on records)         bridge ~15944-16000                     releaseIssueOf (labelsReady ~15870 ported as labelsReady)
 *    deferSet                                  bridge ~16806                           defer
 *    the station's ui.select / complete.commit refusals  design-1.html ~8304-8318, 8365-8387  recheck (from the cloud's own records)
 *    commitCompletion                          design-1.html ~5415-5456                stages message, ledger, unlock, archive
 *    markBritesDesigned (a set's)              design-1.html ~5245-5279                stage message (firebaseOrders in-process: "DESIGNED :)", designSetId)
 *    persistCompleted                          design-1.html ~3070-3081                stage ledger (firebaseOrders in-process: completedIds)
 *    archiveOrders / buildArchiveRecord        design-1.html ~6552-6731                stage archive (_charmNestArchiveRecord.js; designArchive in-process, noMirror)
 *    rememberCommit / priorCommit (localStorage) design-1.html ~8011-8033              Charm_Nest_Commits/<runId>~<setId> (_charmNestCommitOps.js), written FIRST
 *    Sets.undo + the station's complete.undo   bridge ~16119-16128, design-1.html ~8388  undoSet (a person's commitUndoAsk)
 *    RunCtl's complete step                    bridge ~16730-16741                     completeUnit (the gold dot lifted: firebaseOrders rtUnclaimIds)
 *    RunCtl's claim step (Orders.claim)        bridge ~16665, ~2207                    claimUnit (the gold dot put on: firebaseOrders rtClaimIds; standby's step, build step 8)
 *
 *  One set at a time, each stage its own unit call, each saved in the set's journal before the next starts:
 *    begin   the Etsy freshness gate (§1.8a: the mail system's copy synced well since the step began, the world built from it, none of the set's receipts
 *            still to be read, Etsy's daily limit not set, the Etsy switch read strictly and off); the set's own release checks on its records; which of its
 *            orders go (O.evaluateOrder over the run's lines); the last re-check (P11: cancelled, cancelled on Etsy, a take-off not finished → the set
 *            WAITS, in words; not open any more, locked at another station, already complete, not on the set's QR labels → that order is refused, in the
 *            station's own words); the server's own completion rules (setCommitCheck); then the journal's intent (the order ids, fixed from here on).
 *    message "DESIGNED :)" in each order's chat (one per order and set: designed-set-<setId>, written once by firebaseOrders' transaction), confirmed;
 *            then the last look before the ledger (a cancel that landed meanwhile makes the set wait; a take-off that finished drops the order).
 *    ledger  Design_Completed Orders (firebaseOrders completedIds, with its index in the same batch).
 *    unlock  the station locks let go (rtUnlockIds; a failure is only noted, as the station's).
 *    archive Design_Order_Archive, each record built from the mail system's copy of the receipt (never Etsy), its picture from the stored image cache only,
 *            put with noMirror (no image is fetched: the record keeps the listing picture's address, as the sandbox's records do).
 *    record  the set's record (committed, refused, times, status: the server checks its completion again), its pool rows and run lines "committed", the run's
 *            committed / refused / holds; then the journal says done.
 *  A replay after a crash at any point goes on from the journal's stage: every side effect is keyed (the message id, the ledger's set-merge, the archive's
 *  merge by receipt id, the mid of every cloud write), so nothing is posted or marked twice, and the set's own ids are never refused as "already complete".
 *
 *  Q1 (hard-parts §2.2): the cloud marks orders design-complete with no station preview; what the preview protected is checked directly: the set's QR labels
 *  are saved, every sheet's label payload equals its placed orders (labelsReady), and every committed order is on one of them.
 *  DORMANT: the driver runs these units only while Charm_Nest_Open/drive is in standby or on AND "commit" / "complete" are in its cloudSteps; until then it
 *  parks the run at commit with words, and a computer commits as today. Nothing here calls Etsy, its image CDN or its switch.                          */
"use strict";
const O = require("../../charm-nest-orders.js");
const Archive = require("./_charmNestArchiveRecord");
const { COMMITS } = require("./_charmNestCommitOps");

const SETS = "Charm_Nest_Sets", SHEETS = "Charm_Nest_Sheets", MIRROR = "EtsyMail_Receipts", WORLD_ETSY = "Charm_World_Etsy", CANCELLED = "Charm_Nest_Cancelled",
  TAKEOFF = "Charm_Nest_TakeOff", LOCKS = "Design_RealTime_Selected_Orders", LEDGER = "Design_Completed Orders";
const COMPLETED_BY = "Brites Manager (Cloud)", LOCK_CLIENT = "cloud-commit", DESIGNED = "DESIGNED :)";
const MIDWAY = new Set(["intent", "message", "ledger", "unlock", "archive", "record"]);
const CANCEL_STATUS = /cancel|fully refunded/i;                 // (_charmNestWorld.js isOpenReceipt's)
const ORDERLESS = /^CUSTOM-\d{8}-\d{1,6}$/;                      // (charm-nest-custom-pool.js orderless: a custom piece of no order is on no shipping list)
const TAKEOFF_OPEN = new Set(["due", "working", "waiting"]);
const LINE_DONE = new Set(["committed", "gone", "skipped", "noDesign"]);   // (bridge complete step)
const TO_COMMITTED = new Set(["written", "labelled", "noDesign"]);       // (Sets.record)
const COMPLETE_REFUSAL = /^(Set cannot be completed|A set without sheets)/;   // (op_setUpdate's completion rules, in its own words)
const ARCHIVE_TRIES = 3;

/* the two station functions called in-process (no Etsy call in either: firebaseOrders.js has none; designArchive fetches only to mirror a picture, which
   noMirror skips). Replaceable for tests. */
const deps = { firebaseOrders: () => require("./firebaseOrders"), designArchive: () => require("./designArchive") };

const num = v => (Number.isFinite(+v) ? +v : 0);
const uniq = a => [...new Set((a || []).map(String))];
const hash = v => require("crypto").createHash("sha256").update(typeof v === "string" ? v : JSON.stringify(v)).digest("hex").slice(0, 16);
const valuesOf = v => (Array.isArray(v) ? v : v && typeof v === "object" ? Object.values(v) : []);
const ridOf = (key, l) => String((l && l.orderId) || String(key).split("_")[0]);
const setName = s => s.name || s.setId;
const bySetOrder = (a, b) => String(a.day || "").localeCompare(String(b.day || "")) || num(a.seq) - num(b.seq) || String(a.setId).localeCompare(String(b.setId));

async function parse(res) { let out = {}; try { out = JSON.parse((res && res.body) || "{}"); } catch (_) { out = {}; } return Object.assign({ status: res ? res.statusCode : 0 }, out && typeof out === "object" && !Array.isArray(out) ? out : {}); }
/** firebaseOrders, in-process (the station's own POSTs: completedIds, newMessage, rtUnlockIds, rtUnclaimIds, uncompleteIds). Production only. */
async function station(body) { return parse(await deps.firebaseOrders().handler({ httpMethod: "POST", headers: {}, queryStringParameters: {}, body: JSON.stringify(body) })); }
/** designArchive "put", in-process, with noMirror (E6): no picture is fetched. */
async function archivePut(orders) { return parse(await deps.designArchive().handler({ httpMethod: "POST", headers: { "x-edit-passcode": process.env.EDIT_PASSCODE || "" }, queryStringParameters: {}, body: JSON.stringify({ op: "put", orders, noMirror: true }) })); }
/** An answer that did not come is a passing failure (the driver tries again by itself: its PASSING words); one that refused is said as it is. */
const failed = (what, r) => new Error(r && r.status >= 400 && r.status < 500 ? `${what} was refused: ${(r && r.error) || "HTTP " + r.status}` : `${what} did not answer in time${r && r.error ? ": " + String(r.error).slice(0, 160) : ""}`);
const must = (r, what) => { if (!r || r.status >= 400) throw new Error(`${what}: ${(r && r.error) || "no answer"}`); return r; };

/* ── reads (the cloud's records; nothing here is a page's memory) ── */
async function getAll(db, coll, ids, mask) {
  const out = new Map(); ids = uniq(ids).filter(Boolean);
  for (let i = 0; i < ids.length; i += 300) {
    const refs = ids.slice(i, i + 300).map(id => db.collection(coll).doc(id));
    const docs = await db.getAll(...refs, ...(mask ? [{ fieldMask: mask }] : []));
    docs.forEach(d => out.set(d.id, d.exists ? Object.assign({ id: d.id }, d.data() || {}) : null));
  }
  return out;
}
async function setsOf(db, runId) { const q = await db.collection(SETS).where("runId", "==", runId).get(); return q.docs.map(d => Object.assign({}, d.data() || {}, { setId: (d.data() || {}).setId || d.id })); }
async function journalsOf(db, runId) { const q = await db.collection(COMMITS).where("runId", "==", runId).get(), out = new Map(); for (const d of q.docs) { const j = d.data() || {}; if (j.setId) out.set(String(j.setId), j); } return out; }
async function sheetsOfRun(db, runId) { const q = await db.collection(SHEETS).where("runId", "==", runId).get(); return q.docs.map(d => Object.assign({ id: d.id }, d.data() || {})); }
/** The shop's shared settings as values (shareGet answers each field as the store keeps it, { j: JSON text, ... }: _charmNestPieceHost.js sharedValues reads them). */
async function settingsOf(ctx) { try { const r = await ctx.read({ op: "shareGet", names: ["sorter-settings"] }), it = r && r.items && r.items["sorter-settings"]; return require("./_charmNestPieceHost").sharedValues(it && it.f); } catch (_) { return {}; } }
function linesByOrder(lines) { const by = new Map(); for (const [k, l] of Object.entries(lines || {})) { if (!l || typeof l !== "object") continue; const rid = ridOf(k, l); if (!by.has(rid)) by.set(rid, []); by.get(rid).push(Object.assign({ key: k }, l)); } return by; }

/* ── Sets.evaluate, on the run's line records (bridge ~15927): which orders travel, which are held and why, which are gone. As the page did, over the whole run:
   `committable` is narrowed to the set's orders by the caller; `held` is the run's (the page's set status reads it). A line record keeps its problems as kinds,
   its "no design" and "completed by hand" as flags (charm-nest-pool-make.js lineRecord): they are read back into the shape O.evaluateOrder reads. ── */
function rowOf(key, l) {
  return { key, state: l.state, reason: l.reason, hold: l.hold || null, changePending: !!l.changePending, engrave: l.engrave || null,
    problems: (Array.isArray(l.problems) ? l.problems : []).map(p => (p && typeof p === "object" ? p : { kind: String(p) })),
    spec: { noDesign: !!l.noDesign, customDone: l.handDone ? { state: "done", how: l.handDone.how === "print" ? "print" : "button" } : null } };
}
function evaluate(lines) {
  const out = { committable: [], held: {}, gone: [] };
  for (const [rid, ls] of linesByOrder(lines)) {
    if (ls.every(l => l.state === "gone")) { out.gone.push(rid); continue; }
    const ev = O.evaluateOrder(ls.map(l => rowOf(l.key, l)));
    const byPerson = ls.find(l => l.hold);
    if (ev.committable && !byPerson) out.committable.push(rid); else out.held[rid] = byPerson ? { line: byPerson.key, why: byPerson.hold } : ev.held;
  }
  return out;
}

/* ── the set's own release checks, on its records (Sets.validateRelease, bridge ~15944): what the page read from its memory, read from the sheets' and the
   run's records. The rules the server keeps itself (the GF/SS principle, the cardinal rule, every sheet ready: engraving approved, back files verified,
   front files, labels) are asked of the server by setCommitCheck, which runs op_setUpdate's own completion check on the set as it would be saved. ── */
const placedCharms = sh => { const byId = new Map((sh.charms || []).filter(Boolean).map(c => [c.id, c])); return (sh.placements || []).map(p => p && byId.get(p.id)).filter(Boolean); };
const metalOf = sh => O.CARD_TO_METAL[sh.metal] || sh.metal;
const placedOrders = sh => uniq(placedCharms(sh).map(c => String(c.order || c.id).split("/")[0])).filter(id => !ORDERLESS.test(id));
/** Sets.labelsReady (bridge ~15870), on records: the sheet's QR labels are saved, indexed in the set, and each says exactly the orders placed on it. */
function labelsReady(sh, set) {
  const metal = metalOf(sh), placed = placedCharms(sh), parts = O.safeChunks(placedOrders(sh), metal, 1000, 500, 8);
  if (!parts.length && placed.length) parts.push([]);
  const files = (sh.label && Array.isArray(sh.label.files) ? sh.label.files : []).filter(Boolean), indexed = (set.labelFiles || []).filter(f => f && String(f.sheetId) === String(sh.id));
  const part = f => num(f.part) || 1;
  return String(sh.setId || "") === String(set.setId) && !sh.draft && parts.length > 0 && files.length === parts.length && indexed.length === files.length &&
    files.every((f, i) => f.url && f.path && (!f.sheet || !sh.fileBase || f.sheet === sh.fileBase) && f.payload === O.encodeOrderList(parts[i], metal) && indexed.some(g => g.path === f.path && g.url === f.url && part(g) === part(f)));
}
/** the set's sheets' pieces whose cut line changed after nesting and that do not fit where they stand (cutHeld, bridge ~30748) */
function cutHeldOf(sh) {
  if (!sh || !sh.cutCheck || typeof sh.cutCheck !== "object") return null;
  const placed = new Set(placedCharms(sh).map(c => c.poolId));
  for (const [pid, w] of Object.entries(sh.cutCheck)) if (placed.has(pid)) return { pid, why: w && w.why };
  return null;
}
/** → null, or the words the set waits for (the page's pendingRelease messages). sheets: Map id → record. */
function releaseIssueOf(set, sheets, lines) {
  const recs = (set.sheetIds || []).map(id => sheets.get(String(id)));
  if (!recs.length || recs.some(r => !r || r.archived)) return "The set's sheets are not all in the cloud";
  if (recs.some(r => r.runHold)) return "A sheet in this set still needs attention";
  for (const sh of recs) { const h = cutHeldOf(sh); if (h) return `${sh.fileBase || "A sheet in this set"}: the cut line of ${h.pid} changed${h.why ? ` and ${h.why}` : ""}: move it clear in Manual Control, or nest the sheet again`; }
  { const rg = recs.find(sh => sh.metal === "rose" && !sh.roseCutAt && !(num(sh.laserDoneAt) > 0) && sh.autoMatched && String(sh.autoMatched) === String(set.setId)); if (rg) return `${setName(set)} is waiting for the green line cut of ${rg.fileBase || "its Rose Gold sheet"}${rg.autoCutWhy ? `: ${rg.autoCutWhy}` : ""}`; }
  const pools = new Set(recs.flatMap(sh => placedCharms(sh).map(c => c.poolId).filter(Boolean)));
  if (Object.values(lines || {}).some(l => l && l.state !== "gone" && (l.changePending || l.hold) && (l.poolIds || []).some(id => pools.has(id)))) return "An order on this sheet changed or needs review";
  for (const sh of recs) if (!sh.outputs || sh.saving || sh.filesOwed) return `${sh.fileBase || sh.id}: sheet files are not saved`;
  for (const sh of recs) if (!labelsReady(sh, set)) return `${sh.fileBase || sh.id}: QR labels are missing or out of date`;
  return null;
}
/** The orders the set's QR labels carry: placed on one of its sheets (whose labels labelsReady found right) and, where the set's label index lists its
    orders, listed there too (hard-parts §2.2: what the station's preview protected). */
function labelOrders(set, sheets) {
  const out = new Set(), own = new Set((set.sheetIds || []).map(String));
  for (const id of own) { const sh = sheets.get(id); if (sh) for (const o of placedOrders(sh)) out.add(o); }
  const listed = (set.labelFiles || []).filter(f => f && own.has(String(f.sheetId)) && Array.isArray(f.orders));
  if (!listed.length) return out;
  const named = new Set(listed.flatMap(f => f.orders.map(String)));
  return new Set([...out].filter(id => named.has(id)));
}

/* ── the last re-check (P11) and the station's refusals, from the cloud's own copies, just before anything is written ── */
async function recheck(db, rids, set, sheets, own) {
  const out = { ok: [], refused: [], wait: [] };
  if (!rids.length) return out;
  const [cancel, rcpt, take, lock, ledger] = await Promise.all([getAll(db, CANCELLED, rids, ["at"]), getAll(db, MIRROR, rids, ["status", "is_paid", "is_shipped", "receipt_id"]),
    getAll(db, TAKEOFF, rids, ["status", "state"]), getAll(db, LOCKS, rids, ["selected", "selectedBy"]), getAll(db, LEDGER, rids, ["completed"])]);
  const onLabels = labelOrders(set, sheets);
  for (const rid of rids) {
    const c = cancel.get(rid), r = rcpt.get(rid), t = take.get(rid), l = lock.get(rid), d = ledger.get(rid);
    if (c) { out.wait.push({ id: rid, why: `order ${rid} was cancelled: its pieces come off the sheet first (a take-off), then the set is sent to the Design Station` }); continue; }
    if (r && CANCEL_STATUS.test(String(r.status || ""))) { out.wait.push({ id: rid, why: `order ${rid} is cancelled on Etsy: it comes off its sheet first, then the set is sent to the Design Station` }); continue; }
    if (t && TAKEOFF_OPEN.has(String(t.status || t.state || ""))) { out.wait.push({ id: rid, why: `a take-off of order ${rid} is not finished` }); continue; }
    if (!r || r.is_paid === false || r.is_shipped) { out.refused.push({ id: rid, reason: "no longer in the open list" }); continue; }
    if (l && l.selected === true && l.selectedBy && l.selectedBy !== LOCK_CLIENT) { out.refused.push({ id: rid, reason: "locked at another station" }); continue; }
    if (d && d.completed !== false && !(own && own.has(rid))) { out.refused.push({ id: rid, reason: "already complete" }); continue; }   // (m:P8: an order this set's own journal marked is its own, never "already complete")
    if (!onLabels.has(rid)) { out.refused.push({ id: rid, reason: "its QR label is not in this set's labels" }); continue; }
    out.ok.push(rid);
  }
  return out;
}
/** The last look before the ledger (P11), for the journal's ids: a cancel that landed since the intent makes the set wait, unless its take-off already
    finished (every line of the order gone: it is dropped); an order no longer open is refused as the station would. */
async function lastLook(db, ids, lines) {
  const out = { gone: [], wait: [], refused: [] };
  if (!ids.length) return out;
  const [cancel, rcpt, take] = await Promise.all([getAll(db, CANCELLED, ids, ["at"]), getAll(db, MIRROR, ids, ["status", "is_paid", "is_shipped", "receipt_id"]), getAll(db, TAKEOFF, ids, ["status", "state"])]);
  const by = linesByOrder(lines);
  for (const rid of ids) {
    const r = rcpt.get(rid), t = take.get(rid), hit = !!cancel.get(rid) || (r && CANCEL_STATUS.test(String(r.status || ""))) || (t && TAKEOFF_OPEN.has(String(t.status || t.state || "")));
    if (hit) { const ls = by.get(rid) || []; if (ls.length && ls.every(l => l.state === "gone")) out.gone.push(rid); else out.wait.push({ id: rid, why: `order ${rid} was cancelled while its set was being sent: it comes off its sheet first` }); continue; }
    if (!r || r.is_paid === false || r.is_shipped) out.refused.push({ id: rid, reason: "no longer in the open list" });
  }
  return out;
}
const words = (set, waits) => `${setName(set)} waits: ${waits[0].why}${waits.length > 1 ? ` (and ${waits.length - 1} more order${waits.length === 2 ? "" : "s"})` : ""}`;

/* ── writes, all through the cloud's own calls (fenced on the pass's lease and the run's carrier) ── */
/** A stage of the set's journal. The unit key carries the commit's own time (its intent's createdAt), so the same move of a later commit of the set (after an undo)
    is never answered from the ledger as the earlier one's. j: the journal as read (null before the intent). */
async function journal(ctx, set, j, from, to, patch) {
  const cycle = to === "intent" ? num(patch && patch.createdAt) : num(j && j.createdAt);
  const res = await ctx.call({ op: "commitJournal", runId: ctx.runId, setId: set.setId, from: from || null, to, patch: patch || {} }, `commit|${ctx.runId}|${set.setId}|journal|${cycle}`);
  if (res && res.ok) return res.journal || {};
  if (res && res.status === 409 && Object.prototype.hasOwnProperty.call(res, "stage")) return { conflict: true, stage: res.stage };
  throw new Error(`${setName(set)}: its commit journal could not be written: ${(res && res.error) || "no answer"}`);
}
/** The run's own commit fields (the driver saves only its scalar record fields): one merge. A cleared deferral is written as null (a merge keeps a map's other keys). */
async function saveRunFields(ctx, fields) {
  const body = { op: "runPut", run: Object.assign({ runId: ctx.runId }, fields), merge: true };
  // (the time is in the key: the same fields saved again later, after they changed in between, are written again, never answered from the ledger)
  must(await ctx.call(body, `commit|run|${ctx.runId}|${hash(fields)}|${Date.now()}`), "the run's commit record could not be saved");
}
const clearDeferral = (run, setId) => { if (run.deferredSets && run.deferredSets[setId]) { run.deferredSets = Object.assign({}, run.deferredSets, { [setId]: null }); return true; } return false; };
/** deferSet (bridge ~16806): the set waits with its words, in the run's record and its own status. → { settled } */
async function defer(ctx, set, why, j) {
  why = String(why || "waiting").slice(0, 400);
  const run = ctx.run, had = run.deferredSets && run.deferredSets[set.setId];
  run.deferredSets = Object.assign({}, run.deferredSets || {}, { [set.setId]: why });
  if (set.status !== "awaiting review" && !set.committedAt) must(await ctx.call({ op: "setUpdate", setId: set.setId, patch: { status: "awaiting review" } }, `commit|defer|${set.setId}|${Date.now()}`), `${setName(set)} could not be marked as waiting`);
  if (j && MIDWAY.has(j.stage) && j.deferWhy !== why) await journal(ctx, set, j, j.stage, j.stage, { deferWhy: why });
  if (had !== why) await saveRunFields(ctx, { deferredSets: run.deferredSets });
  return { settled: "deferred", why };
}

/* ── the stages ── */
/** before anything is written: the gate, the set's checks, the orders that go, the re-check, the server's completion rules; then the intent. */
async function begin(ctx, set, j) {
  const run = ctx.run, rids = Object.keys(set.orders || {}).map(String);
  const g = await ctx.gate({ receipts: rids });
  if (!g.ok) return { wait: "world", why: `${setName(set)} waits to be sent to the Design Station: ${(g.why || []).join("; ")}`, ids: rids.slice(0, 50) };
  const sheets = await getAll(ctx.db, SHEETS, set.sheetIds || []), lines = run.lines || {};
  const issue = releaseIssueOf(set, sheets, lines);
  if (issue) return defer(ctx, set, issue, j);
  const ev = evaluate(lines), own = new Set(rids);
  const committable = ev.committable.filter(r => own.has(r)), held = {}, gone = ev.gone.filter(r => own.has(r)), heldCount = Object.keys(ev.held).length;
  for (const [r, h] of Object.entries(ev.held)) if (own.has(r)) held[r] = h;
  // (Sets.taken, bridge ~16094: an order this run already marked design-complete, whose answer or set record was lost, is its own, never "already complete": the cloud
  //  knows it from its own journal of the set, or from the run's lines, which only this run's commit sets to "committed". Its message and ledger mark are keyed: sent
  //  again, nothing is doubled)
  const byOrder = linesByOrder(lines), ownDone = new Set(j && j.stage === "done" ? uniq(j.completed) : []);
  for (const r of committable) { const ls = (byOrder.get(r) || []).filter(l => l.state !== "gone"); if (ls.length && ls.every(l => l.state === "committed")) ownDone.add(r); }
  const rc = await recheck(ctx.db, committable, set, sheets, ownDone);
  if (rc.wait.length) return defer(ctx, set, words(set, rc.wait), j);
  const ids = rc.ok, refused = rc.refused;
  if (!ids.length) {
    // nothing to send (Sets.commit: "no committable order", bridge ~16066; or every order refused, ~16076): the set's record says so, and it is tried again next time
    const at = Date.now(), patch = !committable.length
      ? { status: "complete-with-holds", completedAt: at, completionDay: Archive.dayKey(at), committed: [], refused: Object.entries(held).map(([id, h]) => ({ id, reason: (h && h.why) || "held" })) }
      : { status: "complete-with-holds", committed: [], refused };
    const res = await ctx.call({ op: "setUpdate", setId: set.setId, patch }, `commit|none|${set.setId}|${at}`);
    if (res && res.status >= 400) { if (COMPLETE_REFUSAL.test(String(res.error || ""))) return defer(ctx, set, res.error, j); must(res, `${setName(set)} could not be saved`); }
    if (clearDeferral(run, set.setId)) await saveRunFields(ctx, { deferredSets: run.deferredSets });
    return { settled: "nothing to send" };
  }
  if (!set.labels || !(set.labels.files || []).length) return defer(ctx, set, "no saved labels for the set", j);
  // the server's own completion rules on the set as it would be saved, before anything is written (the page wrote the station first and the set after)
  const chk = await ctx.read({ op: "setCommitCheck", setId: set.setId });
  if (!chk || chk.status >= 400) throw failed(`${setName(set)}: the completion check`, chk);
  if (!chk.ready) return defer(ctx, set, chk.why || "the set is not ready to be completed", j);
  const labels = { setId: set.setId, folder: `${set.folder || ""}/labels`, files: (set.labels.files || []).map(f => ({ path: f.path, url: f.url, sheet: f.sheet })), pdf: set.labels.pdf ? set.labels.pdf.url || null : null };
  const w = await journal(ctx, set, j, j ? j.stage : null, "intent", { setName: set.name || null, ids, refused, held, heldCount, gone, completed: [], messages: {}, archived: [], archiveSkipped: [],
    archiveError: null, archiveTries: 0, unlocked: false, labels, sheetIds: (set.sheetIds || []).map(String), backCount: num(set.backCount), completedBy: COMPLETED_BY, deferWhy: null, result: null,
    undo: null, undoneAt: j ? num(j.undoneAt) : 0, createdAt: Date.now() });
  return w.conflict ? { next: true, conflict: true } : { next: true };
}
async function claimMessage(ctx, set, j) { const w = await journal(ctx, set, j, "intent", "message", {}); return { next: true, conflict: !!w.conflict }; }
/** markBritesDesigned for a set (design-1.html ~5245): ten at a time, each confirmed by its message id; then the last look before the ledger. */
async function messageStage(ctx, set, j) {
  const ids = uniq(j.ids), want = "designed-set-" + encodeURIComponent(set.setId), have = Object.assign({}, j.messages || {}), refusedNow = [], missed = [];
  const due = ids.filter(id => have[id] !== want);
  for (let i = 0; i < due.length; i += 10) {
    if (i && Date.now() > ctx.budgetEnd) break;
    await Promise.all(due.slice(i, i + 10).map(async rid => {
      let r = null; try { r = await station({ orderNumber: rid, newMessage: DESIGNED, employeeName: j.completedBy || COMPLETED_BY, designSetId: set.setId }); } catch (e) { r = { status: 0, error: (e && e.message) || String(e) }; }
      if (r.status === 200 && r.success && r.messageId === want) have[rid] = want; else if (r.status >= 400 && r.status < 500) refusedNow.push(rid + " (" + String(r.error || r.status).slice(0, 60) + ")"); else missed.push(rid);
    }));
  }
  const left = ids.filter(id => have[id] !== want);
  if (Object.keys(have).length !== Object.keys(j.messages || {}).length) await journal(ctx, set, j, "message", "message", { messages: have });
  if (refusedNow.length) return defer(ctx, set, `Internal completion message not confirmed for ${refusedNow.length} order(s): ${refusedNow.slice(0, 5).join(", ")}. Confirmed messages are never posted twice`, j);
  if (missed.length) throw new Error(`The Design Station's messages did not answer in time for ${missed.length} order(s) of ${setName(set)}: the cloud tries again (a confirmed message is never posted twice)`);
  if (left.length) return { next: true };   // (out of time: the rest in the next call)
  const last = await lastLook(ctx.db, ids, ctx.run.lines);
  if (last.wait.length) return defer(ctx, set, words(set, last.wait), j);
  const drop = new Set([...last.gone, ...last.refused.map(r => r.id)]);
  const w = await journal(ctx, set, j, "message", "ledger", { messages: have, ids: ids.filter(id => !drop.has(id)), gone: uniq([...(j.gone || []), ...last.gone]), refused: (j.refused || []).concat(last.refused) });
  return { next: true, conflict: !!w.conflict };
}
/** persistCompleted (design-1.html ~3070): the ledger, and its index in the same batch (firebaseOrders completedIds). */
async function ledgerStage(ctx, set, j) {
  const ids = uniq(j.ids);
  if (ids.length) { let r = null; try { r = await station({ completedIds: ids }); } catch (e) { r = { status: 0, error: e && e.message }; } if (!(r.status === 200 && r.success)) throw failed(`The Design Station's ledger for ${setName(set)}`, r); }
  const w = await journal(ctx, set, j, "ledger", "unlock", { completed: ids });
  return { next: true, conflict: !!w.conflict };
}
/** the station locks let go (design-1.html ~5438): a failure is only noted, as the station's own. */
async function unlockStage(ctx, set, j) {
  const ids = uniq(j.completed); let ok = true;
  if (ids.length) { try { const r = await station({ rtUnlockIds: ids, clientId: LOCK_CLIENT }); ok = r.status === 200; } catch (_) { ok = false; } }
  if (!ok) console.warn(`[commit] ${setName(set)}: the station locks were not let go (they lapse by themselves)`);
  const w = await journal(ctx, set, j, "unlock", "archive", { unlocked: ok });
  return { next: true, conflict: !!w.conflict };
}
/** archiveOrders (design-1.html ~6672) with no Etsy call: each record from the mail system's copy of the receipt (else the world's, else the run's lines),
    its pictures from the stored image cache only, put with noMirror. A record with nothing readable is skipped and named; a failed put is tried again
    twice, then noted (as the station: archiving never holds a completion). */
async function archiveStage(ctx, set, j) {
  const done = new Set(uniq([...(j.archived || []), ...(j.archiveSkipped || [])])), due = uniq(j.completed).filter(id => !done.has(id)).slice(0, 30);
  if (!due.length) { const w = await journal(ctx, set, j, "archive", "record", {}); return { next: true, conflict: !!w.conflict }; }
  const [mirror, world] = await Promise.all([getAll(ctx.db, MIRROR, due), getAll(ctx.db, WORLD_ETSY, due)]), by = linesByOrder(ctx.run.lines);
  const sources = due.map(rid => [rid, Archive.dataFrom(rid, { mirror: mirror.get(rid), world: world.get(rid), lines: by.get(rid) })]);
  const listings = uniq(sources.flatMap(([, s]) => (s ? s.data.transactions : []).map(t => String(t.listing_id || "")))).filter(id => /^\d{3,20}$/.test(id)).slice(0, 100);
  let images = {};
  if (listings.length) { try { const p = await ctx.read({ op: "listingPhotos", listingIds: listings, fullFirst: true }); images = (p && p.images) || {}; } catch (_) { images = {}; } }
  const extra = { labels: j.labels || null, setId: set.setId, sheetIds: j.sheetIds || null, backCount: j.backCount == null ? null : num(j.backCount), runId: ctx.runId };
  const records = [], skipped = [];
  for (const [rid, src] of sources) {
    const rec = src ? Archive.buildArchiveRecord(rid, src.data, { completedBy: j.completedBy || COMPLETED_BY, now: num(j.createdAt) || Date.now(), imageOf: id => images[id] || "" }) : null;
    if (!Archive.usable(rec)) { skipped.push(rid); console.warn(`[commit] archive skipped, nothing readable for ${rid}`); continue; }
    for (const [k, v] of Object.entries(extra)) if (v != null) rec[k] = v;
    records.push(rec);
  }
  const archived = uniq(j.archived || []); let error = null;
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10); let r = null;
    try { r = await archivePut(chunk); } catch (e) { r = { status: 0, error: e && e.message }; }
    if (!(r.status === 200 && r.success)) { error = String((r && r.error) || "HTTP " + (r && r.status)).slice(0, 200); break; }
    for (const rec of chunk) archived.push(String(rec.receiptId));
  }
  const tries = num(j.archiveTries) + (error ? 1 : 0), giveUp = error && tries >= ARCHIVE_TRIES;
  if (giveUp) console.warn(`[commit] ${setName(set)}: archiving failed ${tries} times (${error}); the orders are design-complete all the same`);
  const patch = { archived: uniq(archived), archiveSkipped: uniq([...(j.archiveSkipped || []), ...skipped]), archiveTries: tries, archiveError: error };
  const finished = giveUp || (!error && uniq(j.completed).every(id => patch.archived.includes(id) || patch.archiveSkipped.includes(id)));
  const w = await journal(ctx, set, j, "archive", finished ? "record" : "archive", patch);
  return { next: true, conflict: !!w.conflict };
}
/** Sets.record (bridge ~16101): the set's record (the server checks its completion again inside), its pool rows and run lines, the run's commit fields; then done. */
async function recordStage(ctx, set, j) {
  const run = ctx.run, completed = uniq(j.completed), refused = Array.isArray(j.refused) ? j.refused : [], at = num(j.createdAt) || Date.now();
  const status = !completed.length || refused.length || num(j.heldCount) ? "complete-with-holds" : "complete";
  const res = await ctx.call({ op: "setUpdate", setId: set.setId, patch: { committed: completed, refused, committedAt: at, completedAt: at, completionDay: Archive.dayKey(at), status } }, `commit|record|${set.setId}|${at}`);
  if (res && res.status >= 400) {
    if (COMPLETE_REFUSAL.test(String(res.error || ""))) return defer(ctx, set, `${res.error} (its orders are design-complete at the station already: the set's record is saved once this is put right)`, j);
    must(res, `${setName(set)}: its record could not be saved`);
  }
  const by = linesByOrder(run.lines), done = new Set(completed);
  const copies = completed.flatMap(rid => valuesOf(((set.orders || {})[rid] || {}).lines).flatMap(l => valuesOf(l && l.copies).map(c => c && c.poolId)));
  const poolIds = uniq([...completed.flatMap(rid => (by.get(rid) || []).flatMap(l => l.poolIds || [])), ...copies]).filter(Boolean);
  for (let i = 0; i < poolIds.length; i += 400) must(await ctx.call({ op: "poolUpdate", poolIds: poolIds.slice(i, i + 400), patch: { state: "committed", committedAt: at } }, `commit|pool|${set.setId}|${at}|${i}`), `${setName(set)}: its pieces could not be marked committed`);
  const keys = Object.entries(run.lines || {}).filter(([k, l]) => l && done.has(ridOf(k, l)) && TO_COMMITTED.has(l.state)).map(([k]) => k);
  for (let i = 0; i < keys.length; i += 300) {
    const part = keys.slice(i, i + 300);
    must(await ctx.call({ op: "runLinesPatch", runId: ctx.runId, lines: Object.fromEntries(part.map(k => [k, { state: "committed" }])) }, `commit|lines|${set.setId}|${at}|${i}`), `${setName(set)}: its order lines could not be marked committed`);
    for (const k of part) run.lines[k] = Object.assign({}, run.lines[k], { state: "committed" });
  }
  run.committed = uniq([...(run.committed || []), ...completed]);
  const seen = new Set(refused.map(r => String(r.id)));
  run.refused = (Array.isArray(run.refused) ? run.refused : []).filter(r => r && !seen.has(String(r.id))).concat(refused);
  run.holds = Object.assign({}, run.holds || {}, evaluate(run.lines).held);
  clearDeferral(run, set.setId);
  await saveRunFields(ctx, { committed: run.committed, refused: run.refused, holds: run.holds, ...(run.deferredSets ? { deferredSets: run.deferredSets } : {}) });
  console.log(`[commit] ${setName(set)} committed: ${completed.length} order(s) marked design-complete and sent DESIGNED :) internally · ${refused.length} refused · ${Object.keys(j.held || {}).length} held`);
  const w = await journal(ctx, set, j, "record", "done", { result: { completed, refused, held: j.held || {} } });
  return w.conflict ? { next: true, conflict: true } : { settled: "committed" };
}
const STAGE = { intent: claimMessage, message: messageStage, ledger: ledgerStage, unlock: unlockStage, archive: archiveStage, record: recordStage };

/** Sets.undo (bridge ~16119) for a set the cloud committed, on a person's commitUndoAsk: the orders reopened at the station (uncompleteIds), the set back to
    awaiting review, its pieces and lines written again; the run stops at engrave, in words, for the person to put right what they wanted and press Resume. */
async function undoSet(ctx, set, j) {
  const run = ctx.run, reopened = uniq(j.completed), at = num(j.undo && j.undo.at);
  if (reopened.length) { let r = null; try { r = await station({ uncompleteIds: reopened }); } catch (e) { r = { status: 0, error: e && e.message }; } if (!(r.status === 200 && r.success)) throw failed(`Reopening ${setName(set)} at the Design Station`, r); }
  must(await ctx.call({ op: "setUpdate", setId: set.setId, patch: { status: "awaiting review", committed: [], committedAt: null, completedAt: null, completionDay: null } }, `commit|undo|${set.setId}|${at}`), `${setName(set)}: its record could not be reopened`);
  const by = linesByOrder(run.lines), open = new Set(reopened);
  const copies = reopened.flatMap(rid => valuesOf(((set.orders || {})[rid] || {}).lines).flatMap(l => valuesOf(l && l.copies).map(c => c && c.poolId)));
  const poolIds = uniq([...reopened.flatMap(rid => (by.get(rid) || []).flatMap(l => l.poolIds || [])), ...copies]).filter(Boolean);
  for (let i = 0; i < poolIds.length; i += 400) must(await ctx.call({ op: "poolUpdate", poolIds: poolIds.slice(i, i + 400), patch: { state: "written" } }, `commit|undo-pool|${set.setId}|${at}|${i}`), `${setName(set)}: its pieces could not be reopened`);
  const keys = Object.entries(run.lines || {}).filter(([k, l]) => l && l.state === "committed" && open.has(ridOf(k, l))).map(([k]) => k);
  for (let i = 0; i < keys.length; i += 300) {
    const part = keys.slice(i, i + 300);
    must(await ctx.call({ op: "runLinesPatch", runId: ctx.runId, lines: Object.fromEntries(part.map(k => [k, { state: "written" }])) }, `commit|undo-lines|${set.setId}|${at}|${i}`), `${setName(set)}: its order lines could not be reopened`);
    for (const k of part) run.lines[k] = Object.assign({}, run.lines[k], { state: "written" });
  }
  run.committed = uniq(run.committed || []).filter(id => !open.has(id));
  await saveRunFields(ctx, { committed: run.committed });
  await journal(ctx, set, j, "done", "undone", { undoneAt: at, result: Object.assign({}, j.result || {}, { reopened }) });
  run.step = "engrave";   // (Sets.undo: the run goes back to engrave; it waits for the person's Resume)
  return { stop: { why: `${setName(set)}: its completion was undone at the Design Station (${reopened.length} order${reopened.length === 1 ? "" : "s"} reopened): change what is needed, then press Resume`, fix: "Press Resume when ready.", kind: "operator" } };
}

/** THE COMMIT STEP (RunCtl ~16710): one stage of one set per call; { more } until every eligible set of the run is committed or waits with its words. */
async function commitUnit(ctx) {
  // (what this pass of the step has done, kept on the driver's cursor under the step's own start: the cursor is saved as a merge, so a key from an earlier pass
  //  of the commit step can still be there; it is read only when it is this one's)
  const at = num(ctx.cursor && ctx.cursor.stepStartedAt), was = (ctx.cursor && ctx.cursor.commit) || {}, cur = num(was.at) === at ? Object.assign({ tried: [], conflicts: 0 }, was) : { tried: [], conflicts: 0 };
  const { db, run, runId } = ctx;
  const [sets, journals] = await Promise.all([setsOf(db, runId), journalsOf(db, runId)]);
  // a person's Undo of a set the cloud committed comes first (hard-parts §2.9)
  for (const j of journals.values()) if (j.stage === "done" && j.undo && num(j.undo.at) > num(j.undoneAt)) { const set = sets.find(s => String(s.setId) === String(j.setId)); if (set) return undoSet(ctx, set, j); }
  const withSheets = sets.filter(s => (s.sheetIds || []).length);
  if (run.releasePolicy === 2 && !withSheets.length) { run.nothingToCut = true; return; }
  const midway = s => { const j = journals.get(String(s.setId)); return !!(j && MIDWAY.has(j.stage)); };
  let eligible = withSheets.filter(s => !s.committedAt || midway(s)).sort(bySetOrder);
  if (!eligible.length) { run.awaitCommit = false; return; }
  // (a set already on its way to the station is finished whatever the setting says: its side effects have begun)
  const settings = await settingsOf(ctx);
  if (settings.autoCommit === "off" && !run.commitRequested) { eligible = eligible.filter(midway); if (!eligible.length) { run.awaitCommit = true; return; } }
  else run.awaitCommit = false;
  const tried = new Set(uniq(cur.tried));
  const set = eligible.filter(s => !tried.has(String(s.setId))).sort((a, b) => (midway(b) ? 1 : 0) - (midway(a) ? 1 : 0) || bySetOrder(a, b))[0];
  if (!set) return;   // (every eligible set was committed, or waits with its words, in this pass of the step)
  const j = journals.get(String(set.setId)) || null;
  const res = j && MIDWAY.has(j.stage) ? await STAGE[j.stage](ctx, set, j) : await begin(ctx, set, j);
  if (res && (res.wait || res.stop)) return res;
  const conflicts = res && res.conflict ? num(cur.conflicts) + 1 : 0;
  if (conflicts > 3) throw new Error(`${setName(set)}: its commit journal moved under the cloud ${conflicts} times in a row: another pass may be at work`);
  if (res && res.settled) tried.add(String(set.setId));
  return { more: true, cursor: { commit: { at, tried: [...tried].slice(-200), conflicts } } };
}

/** THE COMPLETE STEP (RunCtl ~16730): the gold dot ("in a Charm Sorter run") comes off an order once the run is done with it; while work is pending, only off
    the orders that are finished (committed, gone, skipped, nothing to make). Only what the station still shows as claimed by this run is sent (the page's
    `unclaimed` memory, read from the claim documents). A failure is only noted, as today. */
async function completeUnit(ctx) {
  const { db, run, runId } = ctx;
  const Drive = require("./_charmNestDrive");
  const sheets = await sheetsOfRun(db, runId).catch(() => []);
  Drive.updatePending(run, { sheets });
  const pending = Object.values(run.pendingWork || {}).some(Boolean);
  const entries = Object.entries(run.lines || {}).filter(([, l]) => l && typeof l === "object");
  const open = pending ? new Set(entries.filter(([, l]) => !LINE_DONE.has(l.state)).map(([k, l]) => ridOf(k, l))) : new Set();
  const ids = uniq(entries.map(([k, l]) => ridOf(k, l))).filter(id => !open.has(id));
  if (!ids.length) return;
  const claims = await getAll(db, LOCKS, ids, ["claimed", "claimRun"]).catch(() => new Map());
  const due = ids.filter(id => { const c = claims.get(id); return c && c.claimed === true && (!c.claimRun || String(c.claimRun) === String(runId)); });
  for (let i = 0; i < due.length; i += 500) {
    try { const r = await station({ rtUnclaimIds: due.slice(i, i + 500) }); if (r.status !== 200) console.warn("[commit] unclaim: HTTP " + r.status); }
    catch (e) { console.warn("[commit] unclaim:", e && e.message); break; }
  }
}

/** THE CLAIM STEP (RunCtl's doStep "claim", bridge ~16665 / Orders.claim ~2207; cloud plan v2 §1.7): the gold dot ("in a Charm Sorter run") goes on every order of the run, through
    the station's own POST (firebaseOrders rtClaimIds, claimedBy "sorter", claimRun), 500 ids a call. A courtesy, never a lock: a failure is only noted, as today, and the run goes
    on. Only the orders the station does not already show as claimed by this run are sent, so a replay writes nothing. The run's claimedAt says when (the page's r.claimedAt). */
async function claimUnit(ctx) {
  const { db, run, runId } = ctx;
  run.claimedAt = Date.now();
  await claimOrders(db, runId, Object.entries(run.lines || {}).filter(([, l]) => l && typeof l === "object" && l.state !== "gone").map(([k, l]) => ridOf(k, l)));
}
/** The gold dot on these orders for this run (the claim step's, and the intake's for the orders that join a run under way, as the page's processPending claims them): the station's
    own POST, 500 ids a call, only for the orders it does not already show as claimed by this run. Never throws: a failure is only noted. → how many were sent */
async function claimOrders(db, runId, rids) {
  const ids = uniq(rids).filter(id => id && !ORDERLESS.test(id)); if (!ids.length || !runId) return 0;
  const claims = await getAll(db, LOCKS, ids, ["claimed", "claimRun"]).catch(() => new Map());
  const due = ids.filter(id => { const c = claims.get(id); return !(c && c.claimed === true && String(c.claimRun || "") === String(runId)); });
  let sent = 0;
  for (let i = 0; i < due.length; i += 500) {
    try { const r = await station({ rtClaimIds: due.slice(i, i + 500), claimedBy: "sorter", claimRun: runId }); if (r.status !== 200) console.warn("[commit] claim: HTTP " + r.status); else sent += due.slice(i, i + 500).length; }
    catch (e) { console.warn("[commit] claim:", e && e.message); break; }
  }
  return sent;
}

/* the driver's steps (one line in _charmNestSteps.js joins them) */
const steps = { claim: [claimUnit], commit: [commitUnit], complete: [completeUnit] };
module.exports = { steps, passItems: [], claimUnit, claimOrders, commitUnit, completeUnit, evaluate, rowOf, releaseIssueOf, labelsReady, labelOrders, recheck, lastLook, begin, undoSet, STAGE, deps,
  COMPLETED_BY, LOCK_CLIENT, MIDWAY };
