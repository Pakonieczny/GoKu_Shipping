/*  netlify/functions/_charmNestCmdOps.js — THE OPS OF A PERSON'S REQUESTS IN THE CLOUD (cloud plan v2, build step 13: "People's requests and repairs: commands, release, sheet
 *  operations, sweeps"). DORMANT: no page sends these while Charm_Nest_Open/drive is absent or "off", the cloud's driver never runs then, and the CLOUD_ONLY ops below are
 *  refused on every request that is not the cloud's own in-process call (charmNestLibrary handler, _charmNestOpClass.js CLOUD_ONLY).
 *
 *  What a request was: a computer that only followed the run put it in the cloud (runCmdPut: Charm_Nest_Run_Cmds/<run>), and the computer that CARRIED the run did it in its page
 *  (charm-nest-run-follow.js execCmd: Gate, LiveNest, Review, OrderHold ...), then answered (runCmdDone). Now the cloud carries the run and does them (the driver's commands unit,
 *  _charmNestCmdUnit.js); these are the writes that work needs, each in ONE transaction fenced on the driver's lease and the run's carrier (fenceTx: first read):
 *
 *    runCmdClaim  { runId, id, dev, by, nonce? }          (PRODUCTION) the request is taken by one carrier: state "running", claim { dev, by, nonce, at, since }. Once claimed,
 *                                                         runCmdStep and runCmdDone from any other computer are refused (claimHeld, read by those ops), so a request is never
 *                                                         done twice by two hands (P9). A claim nobody renewed for 10 minutes (no step, no claim) is stale and may be taken.
 *                                                         A page claims too whenever the drive is not off (charm-nest-run-follow.js runCommands); with the drive off no claim
 *                                                         is ever made and the two ops answer exactly as before.
 *    cmdWorkPut   { runId, cmdId, patch }                 (CLOUD_ONLY) the cloud's own journal of a request that takes more than one pass (Charm_Nest_Cmd_Work/<cmdId>):
 *                                                         its stage, the sheets and jobs it waits for, what it did so far. Read back by the unit straight from Firestore.
 *    cmdMovePieces { runId, from, to, poolIds?, by, why } (CLOUD_ONLY) Merge sheets' "move" and Empty (Gate.consolidate / emptySheet, charm-nest-bridge.js) and ONEFILL's gather
 *                                                         (gatherFilling) on the records: the pieces (charm records, pool ids, approved backs) leave `from` and wait on `to`, with
 *                                                         no placement: `to` keeps its own pieces where they are and owes a nest (nestOwed) that places the newcomers round them
 *                                                         (nestStart, append). The rest goes on by the writer's overflow. `from` is KEPT, empty or not (ONEGRID).
 *                                                         SETSHUT: `to` in any set takes no new order (only orders already on it). A sheet that is cut, behind a green line,
 *                                                         in a set (as a source), in Manual Control, busy, archived or of another run or metal is refused, in words.
 *    cmdLeaveSet  { runId, setId, sheetIds, by, whole?, mark? } (CLOUD_ONLY) a person's take-out of sheets from a set that is not committed (Gate.leaveSet; SetRepair's
 *                                                         expel): each sheet is marked taken out (leftSet), made a draft, its number and label go; the set's lists, order lines
 *                                                         and label files are made again (SetEdit.setAfter, as flowApply's setMember does). Refused, in words: a committed set,
 *                                                         a cut sheet, a sheet the owner put in its set (KEEPINSET keepInSet, OWNEROVERRIDE ownerSet), a sheet held right now,
 *                                                         a run that is committing, and (unless whole) a sheet sharing an order with a sheet that stays (the cardinal rule).
 *                                                         Nothing is deleted, nothing is nested again; the run's assembly forms the next sets by the usual rules.
 *    cmdSwapLine  { runId, lineKey, rid, sheetId, pools, charms, poses?, layoutBase?, keepBacks?, by } (CLOUD_ONLY) a person's change, in one write: the line's new pieces (their
 *                                                         records and their entries) take the place of the line's old ones on their sheet, laid where `poses` says; with no poses
 *                                                         the sheet owes a nest that seats them. pools: []: the old ones leave the sheet (nothing was made in their place).
 *    cmdRestoreNest { runId, sheetId, jobId, from, by, dry? } (CLOUD_ONLY) RGRESTORE: the pieces a finished, verified nest of a sheet placed, that a later write moved off onto
 *                                                         a draft, go back onto the sheet exactly where that nest left them (nothing nested; files and green line owed again);
 *                                                         a draft left empty is emptied whole (its problem and run stop words go). Refusals in words (below).
 *    cmdRoseMerge { runId, sheetId, targetId, by, dry? } (CLOUD_ONLY) RGMERGE: two Rose Gold sheets of one open set, cut (green line) one after the other from ONE physical sheet, become
 *                                                         one sheet in one transaction: every piece stays where it lies, the line between them goes. Refusals in words (below).
 *
 *  Never calls Etsy, never builds the world, never starts an agent. Every write of the cloud's own call is fenced (fenceTx), and a page's call of the PRODUCTION op is refused in
 *  mode "on" by the handler (the cloud does it).                                                                                                                              */
"use strict";
const Faces = require("../../charm-nest-engrave-faces.js");
const crypto = require("crypto");
const SetEdit = require("../../charm-nest-set-edit.js");
const SheetName = require("../../charm-nest-sheet-name.js");
const Placement = require("./_charmNestPlacement");
const CutLine = require("../../charm-nest-rose.js"), RoseStock = require("./_charmNestRoseStock");   // (RGMERGE: the green line's outline, and a layout's fingerprint)
const Core = require("../../charm-nest-assembly-core.js");   // (GAPFILL: asSet, addCopies, setPatch: a set's order list)
const { setOrdersWithout } = require("./_charmNestTakeOffOps"), { fitted } = require("./_charmNestSeatNow");
const PoolPieces = require("../../charm-nest-pool-pieces.js");   // (SRCLIST: carrySources, the design files of pieces that move go with them)

const CMDS = "Charm_Nest_Run_Cmds", WORK = "Charm_Nest_Cmd_Work", SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets", POOL = "Charm_Pool", RUNS = "Charm_Nest_Runs";
const CLAIM_STALE_MS = 10 * 60000, MANUAL_STALE_MS = 90000, FILES_BUSY_MS = 15 * 60000, KEEP_RELEASE_MS = 600000, SHEET_DOC_BYTES = 900000;
const BUSY = new Set(["nesting", "finishing", "queued"]);
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const and = l => (l.length < 2 ? l.join("") : `${l.slice(0, -1).join(", ")} and ${l[l.length - 1]}`);
const isId = s => /^[\w\-]{4,80}$/.test(String(s || ""));
const isKey = s => /^[\w\-]{3,80}$/.test(String(s || ""));
const oidOf = c => String(c && c.order != null ? c.order : "").split("/")[0];
const ridOfPool = id => (/^(\d{1,30})_/.exec(String(id || "")) || [])[1] || "";
const nameOf = r => { try { return (SheetName && SheetName.name && SheetName.name(r)) || r.fileBase || r.id; } catch (_) { return (r && (r.fileBase || r.id)) || "the sheet"; } };
const cutRec = r => !!r && (!!r.roseCutAt || num(r.laserDoneAt) > 0);
const lineDrawn = r => !!r && !!(r.rosePlanJson || r.roseProtectedJson);
const manualLive = (r, now) => !!(r && r.manualEdit && now - num(r.manualEdit.at) < MANUAL_STALE_MS);
const holdingLive = (r, now) => !!(r && r.keepRelease && now - num(r.keepRelease.at) < KEEP_RELEASE_MS);
/** A sheet the cloud must not touch now: being nested, finished, saved, or with a heavy task of its own (P1/P2: one heavy task at a time). */
function busyRec(r, now) {
  if (!r) return "";
  if (BUSY.has(String(r.status || ""))) return "is nesting or saving";
  if (r.saving) return "is being saved";
  if (r.nestJob && ["queued", "running", "starting"].includes(String(r.nestJob.status || ""))) return "is nesting";
  if (r.filesJob && r.filesJob.jobId && now - num(r.filesJob.at) < FILES_BUSY_MS) return "is having its files written";
  return "";
}
/** The claim of a request another computer holds and keeps alive (or null). Read by runCmdStep and runCmdDone: a request one computer took is answered by that one. */
function claimHeld(c, dev, now) {
  const k = c && c.claim; if (!k || !k.dev || k.dev === dev) return null;
  const seen = Math.max(num(k.at), num(c.stepAt));
  return now - seen < CLAIM_STALE_MS ? k : null;
}

module.exports = function make(d) {
  const { db, FV } = d, col = d.col, fenceTx = d.fenceTx, txGetAll = d.txGetAll;
  const revTouch = (b, keys) => { try { d.revTouch(b, keys); } catch (_) { /* the feed is best-effort */ } };
  const feedOrders = (b, lists) => { try { d.feedOrders(b, lists); } catch (_) { /* the feed is best-effort */ } };
  const words = (status, text, more) => Object.assign({ error: text, status }, more || {});
  const listingSets = async (tx, id) => (await tx.get(col(SETS).where("sheetIds", "array-contains", id))).docs;
  const orderList = charms => [...new Set((charms || []).map(oidOf).filter(Boolean))];

  /* ── runCmdClaim ── */
  async function runCmdClaim(b) {
    const runId = str(b.runId, 80), id = str(b.id, 40), dev = str(b.dev, 60), by = str(b.by, 80);
    if (!isId(runId) || !id || !dev) return words(400, "bad run, request or computer");
    const ref = col(CMDS).doc(runId), now = Date.now(), nonce = str(b.nonce, 60) || crypto.randomBytes(8).toString("hex");
    const out = await db.runTransaction(async tx => {
      await fenceTx(tx);
      const s = await tx.get(ref), cmds = Object.assign({}, (s.exists && (s.data() || {}).cmds) || {}), c = cmds[id];
      if (!c) return { ok: true, found: false, claimed: false };
      if (c.state !== "waiting" && c.state !== "running") return { ok: true, found: true, claimed: false, state: c.state };
      const held = claimHeld(c, dev, now);
      if (held) return { ok: true, found: true, claimed: false, holder: { dev: held.dev, by: held.by || "", at: num(held.at) } };
      // (a request a computer was already doing before claims existed: its own steps say so, and it is left to that computer while they are fresh)
      if (c.state === "running" && !c.claim && c.stepBy && c.stepBy !== dev && now - num(c.stepAt) < CLAIM_STALE_MS) return { ok: true, found: true, claimed: false, holder: { dev: c.stepBy, by: "", at: num(c.stepAt) } };
      const mine = c.claim && c.claim.dev === dev, claim = { dev, by, nonce: mine ? c.claim.nonce || nonce : nonce, at: now, since: mine ? num(c.claim.since) || now : now };
      cmds[id] = Object.assign({}, c, { state: "running", claim, stepAt: now, stepBy: dev, step: c.step || "Started" });
      tx.set(ref, { runId, cmds, expireAt: new Date(now + 30 * 86400000) });
      return { ok: true, found: true, claimed: true, claim, again: !!mine };
    });
    if (out && out.claimed) revTouch(b, ["r:" + runId]);
    return out;
  }

  /* ── cmdWorkPut: the cloud's journal of a request ── */
  async function cmdWorkPut(b) {
    const runId = str(b.runId, 80), cmdId = str(b.cmdId, 40);
    if (!isId(runId) || !/^[\w\-]{2,40}$/.test(cmdId)) return words(400, "bad run or request");
    const patch = b.patch && typeof b.patch === "object" && !Array.isArray(b.patch) ? b.patch : {};
    if (Buffer.byteLength(JSON.stringify(patch)) > 200000) return words(413, "the request's journal is too large");
    const ref = col(WORK).doc(cmdId), now = Date.now();
    await db.runTransaction(async tx => { await fenceTx(tx); await tx.get(ref); tx.set(ref, Object.assign({}, patch, { runId, cmdId, at: now, expireAt: new Date(now + 14 * 86400000) }), { merge: true }); });
    return { ok: true, cmdId, at: now };
  }

  /** The sheet record's lists without these pieces (charm ids from the pool ids), and what left with them. */
  function without(rec, ids) {
    const charms = Array.isArray(rec.charms) ? rec.charms : [], going = charms.filter(c => c && c.poolId && ids.has(String(c.poolId))), cids = new Set(going.map(c => c.id));
    const placements = Array.isArray(rec.placements) ? rec.placements : [];
    return {
      going, cids, poses: placements.filter(p => p && cids.has(p.id)),
      charms: charms.filter(c => !(c && c.poolId && ids.has(String(c.poolId)))),
      placements: placements.filter(p => !(p && cids.has(p.id))),
      poolIds: (rec.poolIds || []).filter(x => !ids.has(String(x))),
      // (NOUNDEF, 21 Nov 2026: a sheet with no rejects / feedWait gave undefined, which Firestore refuses: "Move all onto" failed on GF Draft 18 → 19)
      rejects: Array.isArray(rec.rejects) ? rec.rejects.filter(x => !cids.has(x)) : (rec.rejects == null ? null : rec.rejects),
      feedWait: Array.isArray(rec.feedWait) ? rec.feedWait.filter(x => !cids.has(x)) : (rec.feedWait == null ? null : rec.feedWait),
      backsGoing: (rec.backPool || []).filter(x => x && ids.has(String(x.poolId))),
      backPool: (rec.backPool || []).filter(x => !(x && ids.has(String(x.poolId))))
    };
  }
  const bytesOf = o => Buffer.byteLength(JSON.stringify(o));
  /* CARRYBACKS (Paul, 23 Nov 2026: lost approvals): a piece moved to another sheet takes its approved engraving with it. The moved entry kept the sheet it LEFT (readiness then ignored it: charm-nest-readiness.js), and the
     file's own record (Charm_Pool_Back) went on naming that sheet. Both name the new sheet now; a withdrawn record is left as it is. */
  const backKey = x => Faces.recKey(String(x.poolId), Faces.faceOf(x)), setOf = t => (t.setId && !t.draft ? t.setId : null);
  const backsTo = (backs, t) => backs.map(x => Object.assign({}, x, { sheetId: t.id, setId: setOf(t) }));
  const standingKeys = async (tx, backs) => (backs.length ? (await txGetAll(tx, backs.map(x => col("Charm_Pool_Back").doc(backKey(x))), ["invalidated"])).filter(s => s.exists && (s.data() || {}).invalidated !== true).map(s => s.id) : []);
  const carryBacks = (tx, keys, t, now) => { for (const k of keys) tx.set(col("Charm_Pool_Back").doc(k), { sheetId: t.id, setId: setOf(t), carriedAt: now, updatedAt: FV.serverTimestamp() }, { merge: true }); };

  /* ── cmdMovePieces ── */
  async function cmdMovePieces(b) {
    const runId = str(b.runId, 80), F = b.from || {}, T = b.to || {}, by = str(b.by, 80) || "Cloud";
    if (!isId(runId) || !isId(F.sheetId) || !isId(T.sheetId) || F.sheetId === T.sheetId) return words(400, "Pick two sheets");
    const fRef = col(SHEETS).doc(F.sheetId), tRef = col(SHEETS).doc(T.sheetId), now = Date.now();
    const out = await db.runTransaction(async tx => {
      await fenceTx(tx);
      const [fs, ts] = await txGetAll(tx, [fRef, tRef]);
      if (!fs.exists || !ts.exists) return words(404, "That sheet is not in the cloud");
      const f = Object.assign({ id: fs.id }, fs.data()), t = Object.assign({ id: ts.id }, ts.data()), fn = nameOf(f), tn = nameOf(t);
      if (f.archived || t.archived) return words(409, `${f.archived ? fn : tn} was repacked into other sheets`);
      if (f.runId !== runId || t.runId !== runId) return words(409, "Both sheets must be sheets of the open run");
      if (f.metal !== t.metal) return words(409, "Pick two sheets of one metal");
      if (typeof F.layoutBase === "number" && num(f.layoutRev) !== F.layoutBase) return words(409, `${fn} changed since this move was planned: nothing was moved`, { conflict: true });
      if (typeof F.contentBase === "number" && num(f.contentRev) !== F.contentBase) return words(409, `${fn} changed since this move was planned: nothing was moved`, { conflict: true });
      if (typeof T.layoutBase === "number" && num(t.layoutRev) !== T.layoutBase) return words(409, `${tn} changed since this move was planned: nothing was moved`, { conflict: true });
      if (typeof T.contentBase === "number" && num(t.contentRev) !== T.contentBase) return words(409, `${tn} changed since this move was planned: nothing was moved`, { conflict: true });
      for (const [r, n] of [[f, fn], [t, tn]]) {
        if (cutRec(r)) return words(409, `${n} was already cut`);
        if (lineDrawn(r)) return words(409, `${n} has a cut line drawn`);
        if (manualLive(r, now)) return words(409, `${n} is open in Manual Control right now`);
        if (holdingLive(r, now)) return words(409, `${n} is being held right now`);
        const busy = busyRec(r, now); if (busy) return words(409, `${n} ${busy}: try again once it is done`);
      }
      const fSets = await listingSets(tx, f.id), tSets = await listingSets(tx, t.id);
      if ((f.setId && !f.draft) || fSets.length) return words(409, `${fn} is in a set: take it out of its set first`);
      const ids = new Set((Array.isArray(b.poolIds) && b.poolIds.length ? b.poolIds : f.poolIds || []).map(String).filter(x => (f.poolIds || []).map(String).includes(x)));
      if (!ids.size) return words(409, `${fn} has no pieces to move`);
      const w = without(f, ids);
      if (!w.going.length) return words(409, `${fn} has no pieces to move`);
      // the pieces' records: live, not committed, and listed by `from`
      const rows = await txGetAll(tx, [...ids].map(id => col(POOL).doc(id)));
      // (SETSHUT, Paul 20 Nov 2026: a sheet in any set takes no new orders; a piece of an order already on it may still join it. BACKHOME, Paul 22 Nov 2026: and pieces made again that
      //  were taken from this very sheet (their records' vacatedFrom) come back to it: it is their own order's place)
      if ((t.setId && !t.draft) || tSets.length) {
        const home = rows.length > 0 && rows.every(x => x.exists && String(x.data().vacatedFrom || "") === t.id);
        const own = new Set(orderList(t.charms).concat((t.poolIds || []).map(ridOfPool)).filter(Boolean)), shut = home ? [] : orderList(w.going).filter(o => !own.has(o));
        if (shut.length) return words(409, `${tn} is in a set: it takes no new order (${shut.slice(0, 3).join(", ")}${shut.length > 3 ? "…" : ""})`, { refused: "setShut" });
        if (tSets.some(x => SetEdit.committedSet(x.data()))) return words(409, `${tn}'s set went to the station: it takes nothing more`, { refused: "committed" });
      }
      const off = rows.filter(r => r.exists && Placement.takenOff(r.data())).map(r => r.id), done = rows.filter(r => r.exists && r.data().state === "committed").map(r => r.id);
      if (off.length) return words(409, `${off.length} of these pieces were taken off on purpose (on hold or cancelled): nothing was moved`, { taken: off.slice(0, 20) });
      if (done.length) return words(409, `${done.length} of these pieces were already committed: nothing was moved`);
      const moving = w.going.map(c => { const x = Placement.fullSize(Object.assign({}, c, { pinned: null, metal: t.metal })); delete x.arrivalPin; return x; });
      const tPlaced = (t.placements || []).length > 0, tName = t.fileBase || null, carry = PoolPieces.carrySources(t, moving, [f]);   // (SRCLIST: the pieces' design files go with them)
      const tNext = { charms: (t.charms || []).concat(moving), poolIds: [...new Set((t.poolIds || []).map(String).concat(moving.map(c => String(c.poolId))))], ...(carry || {}),
        orders: [...new Set((Array.isArray(t.orders) ? t.orders : []).map(String).concat(orderList(moving)))], backPool: (t.backPool || []).filter(x => !(x && ids.has(String(x.poolId)))).concat(backsTo(w.backsGoing, t)),
        nestOwed: { at: now, n: moving.length, by: "cloud", why: str(b.why, 80) || "moved" }, appendOnly: tPlaced, intakeAppend: tPlaced, status: "ready", dirty: true,
        layoutRev: num(t.layoutRev) + 1, contentRev: num(t.contentRev) + 1, feedWait: null, rejects: [], updatedAt: FV.serverTimestamp() };
      if (bytesOf(Object.assign({}, t, tNext, { updatedAt: 0 })) > SHEET_DOC_BYTES) return words(413, `${tn} cannot hold that many more pieces in one record: move fewer at a time`);
      const keptOrders = new Set(orderList(w.charms).concat(w.poolIds.map(ridOfPool)));
      const fNext = { charms: w.charms, placements: w.placements, poolIds: w.poolIds, rejects: w.rejects, feedWait: w.feedWait, backPool: w.backPool,
        orders: (Array.isArray(f.orders) ? f.orders : []).map(String).filter(o => keptOrders.has(o)), layoutRev: num(f.layoutRev) + 1, contentRev: num(f.contentRev) + 1, dirty: true, updatedAt: FV.serverTimestamp() };
      if (w.poses.length) fNext.filesOwed = now;
      if (!w.charms.length) Object.assign(fNext, { status: "idle", nestOwed: null });
      const bKeys = await standingKeys(tx, w.backsGoing);
      tx.set(fRef, fNext, { merge: true });
      tx.set(tRef, tNext, { merge: true });
      carryBacks(tx, bKeys, t, now);   // (CARRYBACKS)
      for (const id of ids) tx.set(col(POOL).doc(id), Object.assign({ sheetId: t.id, movedFrom: f.id, movedTo: t.id, movedBy: by, movedAt: now, updatedAt: FV.serverTimestamp() }, tName ? { sheetName: tName } : {}), { merge: true });
      return { ok: true, moved: moving.length, orders: orderList(moving), from: { sheetId: f.id, name: fn, layoutRev: fNext.layoutRev, contentRev: fNext.contentRev, left: w.charms.length }, to: { sheetId: t.id, name: tn, layoutRev: tNext.layoutRev, contentRev: tNext.contentRev } };
    });
    if (out && out.ok) { revTouch(b, ["s:" + F.sheetId, "s:" + T.sheetId]); feedOrders(b, [out.orders]); }
    return out;
  }

  /* ── cmdHopPieces (ITEM 13, Paul 21 Nov 2026: "Order X was not moved to …: The cloud carries every run now", the charm went back; the sheet hop from a viewer) ──
     A person's drag of a charm onto the next or previous 10K / 14K sheet, as the page's own hop saves it (hopSave): the pieces leave their sheet and lie on the target exactly
     where they were put (pinned, the manual mark), every other piece of both sheets keeps its pose byte for byte, both records owe their files (the files lane writes them as
     they stand) and NO nest is owed (no dirty, no nestOwed: nothing is arranged again). { runId, from: [sheetId] (1-4), to: { sheetId, layoutBase? }, poses: [{ poolId, cxPt,
     cyPt, angle, wPt?, hPt?, size? }], free?, by, redraw?: [{ sheetId, cut }], dry? } → { ok, moved, orders, from: [{ sheetId, name, left }], to: { sheetId, name, layoutRev } }
     Refused in words, nothing written: the laser cut a sheet (its ONE lock, GOLDEDIT), a green line still stands (the request sets it aside first: roseReopen), a sheet is
     busy, open in Manual Control by someone else, of another run or metal, its set went to the laser station; a piece is no longer where it was; a pair would be split; a sheet
     would be emptied; the target is in a set and the order is not on it yet (SETSHUT); a piece was taken off or committed; the target changed since the drop (layoutBase). */
  async function cmdHopPieces(b) {
    const runId = str(b.runId, 80), T = b.to || {}, by = str(b.person || b.by, 80) || "a person", dry   /* (person: the cloud's own call is sent by "Cloud"; the move is the person's) */ = b.dry === true, free = b.free === true, fill = b.fill === true;
    const froms = [...new Set((Array.isArray(b.from) ? b.from : [b.from]).map(x => str(x && typeof x === "object" ? x.sheetId : x, 80)).filter(isId))].slice(0, 4);
    const poses = (Array.isArray(b.poses) ? b.poses : []).filter(p => p && p.poolId && ["cxPt", "cyPt", "angle"].every(k => Number.isFinite(+p[k])));
    if (!isId(runId) || !isId(T.sheetId) || !froms.length || froms.includes(T.sheetId)) return words(400, "Pick two sheets");
    if (!poses.length || poses.length !== (Array.isArray(b.poses) ? b.poses.length : 0)) return words(400, "Where each piece goes is not named");
    const redraw = new Map((Array.isArray(b.redraw) ? b.redraw : []).filter(x => x && isId(x.sheetId)).map(x => [x.sheetId, { cut: x.cut === true }]));
    const tRef = col(SHEETS).doc(T.sheetId), fRefs = froms.map(id => col(SHEETS).doc(id)), now = Date.now(), SOLID_HOP = ["gold10k", "gold14k"];
    /* GAP G1 (Paul 21 Nov 2026): "Fill the freed room" / Place in the sheet window from a viewer is this same request (fill: true): the pieces go into the room freed on the
       target where the page planned them, nothing is nested again. The freed room is filled on any sheet but Rose Gold (its green line decides), so fill takes those metals too */
    /* GAPFILL (Paul, 23 Nov 2026: "Whenever this happens, you must automatically fill the empty gaps from orders in unfinished draft sheets"): the cloud's own fill of a finished Gold Filled / Sterling Silver sheet of a set nobody
       approved yet that lost pieces (_charmNestAutoFill.js): the ONE exception to SETSHUT, and only for it: gap { setId, takenOffAt, note }. Checked here, in this transaction: that set and no other lists the sheet, is open (not approved,
       sent or cut), the sheet lost its pieces at that moment; every piece comes from an unfinished draft in no set; every order whole (each live piece of it moves with it, or lies in that set). The pieces are not a person's pins. */
    const gap = fill && b.gap && typeof b.gap === "object" && str(b.gap.setId, 80) && num(b.gap.takenOffAt) > 0 ? b.gap : null;
    const METALS_OK = gap ? ["gold", "silver"] : fill ? ["gold", "silver", "gold10k", "gold14k"] : SOLID_HOP;
    const out = await db.runTransaction(async tx => {
      await fenceTx(tx);
      const snaps = await txGetAll(tx, [tRef, ...fRefs]);
      if (snaps.some(s => !s.exists)) return words(404, "That sheet is not in the cloud");
      const t = Object.assign({ id: snaps[0].id }, snaps[0].data()), srcs = snaps.slice(1).map(s => Object.assign({ id: s.id }, s.data())), tn = nameOf(t);
      for (const r of [t, ...srcs]) {
        const n = nameOf(r);
        if (r.archived) return words(409, `${n} was repacked into other sheets`);
        if (r.runId !== runId) return words(409, `${n} is not a sheet of the run the cloud carries`);
        if (!METALS_OK.includes(r.metal) || r.metal !== t.metal) return words(409, fill ? "An order fills the freed room of a sheet of its own metal only, never Rose Gold" : "Charms move between sheets of one metal, 10K or 14K Gold, only");
        if (num(r.laserDoneAt) > 0) return words(409, `${n} has been cut on the laser: its charms can no longer change`, { refused: "cut" });
        const busy = busyRec(r, now); if (busy) return words(409, `${n} ${busy}: try again once it is done`, { busy: true });
        if (manualLive(r, now) && str(r.manualEdit.by, 80) !== by) return words(409, `${n} is open in Manual Control by ${str(r.manualEdit.by, 80) || "someone"} on another screen`, { refused: "held" });
        if (!dry && (r.roseCutAt || lineDrawn(r))) return words(409, `${n}'s green line still stands: it is set aside before its charms change. Try again`, { refused: "line" });
      }
      if (typeof T.layoutBase === "number" && num(t.layoutRev) !== T.layoutBase) return words(409, `${tn} changed since the charm was dropped on it: drop it again`, { conflict: true });
      // the sets: one sent to the laser station keeps its charms (as a take-off finds it); SETSHUT, a target in a set takes no new order
      const setsOf = new Map();
      for (const r of [t, ...srcs]) setsOf.set(r.id, (await listingSets(tx, r.id)).map(d => Object.assign({ setId: d.id }, d.data())));
      for (const r of [t, ...srcs]) { const s = setsOf.get(r.id).find(x => SetEdit.committedSet(x)); if (s) return words(409, `${nameOf(r)}'s set (${SetEdit.setWord ? SetEdit.setWord(s) : s.name || s.setId}) was sent to the laser station: its charms stay as they are`, { refused: "committed" }); }
      // each piece where the page saw it: listed by one of the named sheets, its charm there
      const want = new Map(poses.map(p => [String(p.poolId), p])), bySrc = new Map();
      for (const id of want.keys()) {
        const r = srcs.find(x => (x.poolIds || []).map(String).includes(id) && (x.charms || []).some(c => c && String(c.poolId) === id));
        if (!r) return words(409, `Order ${ridOfPool(id)}'s piece is no longer on ${and(srcs.map(nameOf))}: it was moved or taken off meanwhile. Nothing was moved`, { gone: true });
        if (!bySrc.has(r.id)) bySrc.set(r.id, new Set()); bySrc.get(r.id).add(id);
      }
      if ([...want.keys()].some(id => (t.poolIds || []).map(String).includes(id))) return words(409, `${tn} already lists this piece: nothing was moved`, { conflict: true });
      const lineOf = id => String(id).replace(/_\d+$/, "");
      for (const r of srcs) {
        const ids = bySrc.get(r.id); if (!ids) continue;
        const live = (r.charms || []).filter(c => c && !c.excluded && c.poolId);
        // a pair or a group of pieces is never moved in halves (R9, R16): every piece of a moving line on this sheet moves with it
        const lines = new Set([...ids].map(lineOf)), half = live.filter(c => lines.has(lineOf(c.poolId)) && !ids.has(String(c.poolId)));
        if (half.length) return words(409, `Order ${ridOfPool(half[0].poolId)} stays where it is: its pair is on ${nameOf(r)} too, and a pair is never moved in halves`, { refused: "pair" });
        if (live.filter(c => !ids.has(String(c.poolId))).length < 1) return words(409, `${nameOf(r)} would be left empty: a sheet is never emptied by a move (Merge sheets, in its Options, moves everything)`, { refused: "empty" });
      }
      const going = [...want.keys()];
      let gapSet = null, gapRows = new Map();
      if (gap) {
        const of = setsOf.get(t.id), g = of.length === 1 ? of[0] : null;
        if (!g || t.draft || String(t.setId || "") !== str(gap.setId, 80) || String(g.setId) !== str(gap.setId, 80)) return words(409, `${tn} is not in that one set any more: nothing was moved`, { refused: "gap" });
        if ([g, t].some(z => (Array.isArray(z.processSeals) ? z.processSeals : []).some(x => x && x.how === "laserReady")) || g.processReady || g.laserSetPending || num(g.laserDoneAt) > 0 || t.processReady ||   /* (NOTCUT) */ t.laserSetPending || t.status !== "complete" || t.inplaceHold) return words(409, `${tn}'s set is approved, or the sheet is not finished: nothing was moved`, { refused: "gap" });
        if (num(t.takenOffAt) !== num(gap.takenOffAt)) return words(409, `${tn} lost other pieces meanwhile: its room is looked at again`, { conflict: true });
        for (const r of srcs) if (!r.draft || setsOf.get(r.id).length || r.releaseFull || r.processReady || r.laserSetPending || r.intakeFinalized || r.ownerSet || r.keepInSet) return words(409, `${nameOf(r)} is not an unfinished draft any more: nothing was moved`, { refused: "gap" });
        const rids = [...new Set(going.map(ridOfPool))].filter(Boolean), vals = rids.concat(rids.filter(o => Number.isSafeInteger(+o)).map(Number)), inSet = new Set((g.sheetIds || []).map(String));
        for (let i = 0; i < vals.length; i += 30) for (const d of (await tx.get(col(POOL).where("orderId", "in", vals.slice(i, i + 30)))).docs) gapRows.set(d.id, d.data() || {});
        const gone = new Set(["abandoned", "superseded"]), split = [...gapRows].find(([id, r]) => !gone.has(String(r.state || "")) && !want.has(id) && !inSet.has(String(r.sheetId || "")));
        if (split) return words(409, `Order ${ridOfPool(split[0])} has a piece outside the set: it is never split, and nothing was moved`, { refused: "gapSplit" });
        if (going.some(id => !gapRows.has(id) || gone.has(String(gapRows.get(id).state || "")))) return words(409, "A piece's record is not live any more: nothing was moved", { gone: true });
        gapSet = g;
      }
      if (!gap && ((t.setId && !t.draft) || setsOf.get(t.id).length)) {
        const own = new Set(orderList(t.charms).concat((t.poolIds || []).map(ridOfPool)).filter(Boolean)), shut = [...new Set(going.map(ridOfPool))].filter(o => o && !own.has(o));
        if (shut.length) return words(409, `${tn} is in a set: it takes no new order (${shut.slice(0, 3).join(", ")}${shut.length > 3 ? "…" : ""})`, { refused: "setShut" });
      }
      const rows = await txGetAll(tx, going.map(id => col(POOL).doc(id)));
      const off = rows.filter(r => r.exists && Placement.takenOff(r.data())).map(r => r.id), done = rows.filter(r => r.exists && r.data().state === "committed").map(r => r.id);
      if (off.length) return words(409, `Order ${ridOfPool(off[0])} was put on hold or taken off meanwhile: nothing was moved`, { taken: off.slice(0, 20) });
      if (done.length) return words(409, `${done.length === 1 ? "That piece was" : done.length + " of these pieces were"} already committed: nothing was moved`);
      if (dry) return { ok: true, dry: true };
      // the writes: each source without its pieces, the target with them where they were put; both owe their files; no nest is owed
      const label = r => (r.draft && r.label && !(r.label && r.label.own) ? { label: null } : {});   // (a sheet outside any set: its label made by the run goes, as the page's remakeLabels did)
      const line = r => (redraw.has(r.id) ? { roseRedraw: { at: now, by, line: true, cut: redraw.get(r.id).cut } } : {});
      const moving = [], left = [];
      let tBacks = [];
      const bKeys = await standingKeys(tx, srcs.flatMap(r => (bySrc.get(r.id) ? without(r, bySrc.get(r.id)).backsGoing : [])));   // (CARRYBACKS: read before anything is written)
      for (const r of srcs) {
        const ids = bySrc.get(r.id); if (!ids) continue;
        const w = without(r, ids), kept = new Set(orderList(w.charms).concat(w.poolIds.map(ridOfPool)));
        for (const c of w.going) moving.push({ c, from: r, was: w.poses.find(p => p && p.id === c.id) || null });
        tBacks = tBacks.concat(w.backsGoing);
        const next = { charms: w.charms, placements: w.placements, poolIds: w.poolIds, rejects: w.rejects, feedWait: w.feedWait, backPool: w.backPool,
          orders: (Array.isArray(r.orders) ? r.orders : []).map(String).filter(o => kept.has(o)), charmCount: w.charms.length, placedCount: w.placements.length,
          layoutRev: num(r.layoutRev) + 1, contentRev: num(r.contentRev) + 1, filesOwed: now, updatedAt: FV.serverTimestamp(), ...label(r), ...line(r) };
        if (next.feedWait === undefined) delete next.feedWait; if (next.rejects === undefined) delete next.rejects;
        tx.set(col(SHEETS).doc(r.id), next, { merge: true });
        left.push({ sheetId: r.id, name: nameOf(r), left: w.charms.length, layoutRev: next.layoutRev });
      }
      const angleOf = a => ((Math.round(+a * 1000) / 1000) % 360 + 360) % 360;
      const cids = new Set(moving.map(m => m.c.id));
      const tCharms = (t.charms || []).filter(c => !(c && cids.has(c.id))).concat(moving.map(m => {
        const p = want.get(String(m.c.poolId)), size = num(p.size) > 0 && num(p.size) !== 1 ? { size: +p.size } : {};
        const x = Object.assign({}, m.c, { metal: t.metal, pinned: Object.assign({ cxPt: +p.cxPt, cyPt: +p.cyPt, angle: angleOf(p.angle) }, size), manual: Object.assign({ by, at: now }, size) });
        delete x.arrivalPin; if (gap) { x.pinned = null; delete x.manual; } return x;
      }));
      const tPl = (t.placements || []).filter(q => !(q && cids.has(q.id))).concat(moving.map(m => {
        const p = want.get(String(m.c.poolId)), was = m.was || { id: m.c.id, name: m.c.name || "", scale: 1 }, turned = Math.abs(angleOf(p.angle) - angleOf(was.angle || 0)) % 180 === 90;
        const wh = num(p.wPt) > 0 && num(p.hPt) > 0 ? { wPt: +p.wPt, hPt: +p.hPt } : turned && num(was.wPt) > 0 ? { wPt: +was.hPt, hPt: +was.wPt } : {};
        return Object.assign({}, was, { id: m.c.id, cxPt: +p.cxPt, cyPt: +p.cyPt, angle: angleOf(p.angle) }, wh);
      }));
      const carry = PoolPieces.carrySources(t, moving.map(m => m.c), srcs);   // (SRCLIST: the pieces' design files go with them)
      const tNext = { charms: tCharms, placements: tPl, poolIds: [...new Set((t.poolIds || []).map(String).concat(going))], orders: [...new Set((Array.isArray(t.orders) ? t.orders : []).map(String).concat(orderList(moving.map(m => m.c))))], ...(carry || {}),
        backPool: (t.backPool || []).filter(x => !(x && want.has(String(x.poolId)))).concat(backsTo(tBacks, t)), charmCount: tCharms.length, placedCount: tPl.length,
        layoutRev: num(t.layoutRev) + 1, contentRev: num(t.contentRev) + 1, filesOwed: now, updatedAt: FV.serverTimestamp(), ...label(t), ...line(t) };
      if (gap) { if (t.label) tNext.labelOwed = now; tNext.fillNote = { at: now, by, n: moving.length, orders: orderList(moving.map(m => m.c)), text: str(gap.note, 300) }; }   // (its QR label names its orders: made again, as after a take-off)
      if (Array.isArray(t.rejects)) tNext.rejects = t.rejects.filter(x => !cids.has(x && typeof x === "object" ? x.id : x));
      if (Array.isArray(t.feedWait)) tNext.feedWait = t.feedWait.filter(x => !cids.has(x));
      if (bytesOf(Object.assign({}, t, tNext, { updatedAt: 0 })) > SHEET_DOC_BYTES) return words(413, `${tn} cannot hold that many more pieces in one record: move fewer at a time`);
      tx.set(tRef, tNext, { merge: true });
      carryBacks(tx, bKeys, t, now);
      if (gapSet) {   // (the set's order list takes the new copies, as it does when a sheet joins it: what dropFromSets takes away at a take-off)
        const st = Core.asSet(gapSet); Core.addCopies(st, { sheetId: t.id, fileBase: t.fileBase || t.id, charms: moving.map(m => m.c), placements: moving.map(m => ({ id: m.c.id })) }, id => gapRows.get(String(id)) || {});
        tx.update(col(SETS).doc(String(gapSet.setId)), { orders: Core.setPatch(st).orders, setRev: num(gapSet.setRev) + 1, updatedAt: FV.serverTimestamp() });   // (FRESHWRITE: the set's revision moves with its order list)
      }
      const tName = t.fileBase || null;
      for (const m of moving) tx.set(col(POOL).doc(String(m.c.poolId)), Object.assign({ sheetId: t.id, movedFrom: m.from.id, movedTo: t.id, movedBy: by, movedAt: now, movedHow: gap ? "gapfill" : fill ? "fill" : free ? "hand-free" : "hand", updatedAt: FV.serverTimestamp() }, tName ? { sheetName: tName } : {}), { merge: true });
      return { ok: true, moved: moving.length, free, orders: orderList(moving.map(m => m.c)), from: left, to: { sheetId: t.id, name: tn, layoutRev: tNext.layoutRev, contentRev: tNext.contentRev } };
    });
    if (out && out.ok && !out.dry) { revTouch(b, ["s:" + T.sheetId].concat(froms.map(id => "s:" + id), gap ? ["t:" + str(gap.setId, 80)] : [])); feedOrders(b, [out.orders]); }
    return out;
  }

  /* ── cmdLeaveSet ── */
  async function cmdLeaveSet(b) {
    const runId = str(b.runId, 80), setId = str(b.setId, 80), by = str(b.by, 80) || "Cloud", whole = b.whole === true;
    const ids = [...new Set((Array.isArray(b.sheetIds) ? b.sheetIds : []).map(x => str(x, 80)).filter(isId))].slice(0, 30);
    const mark = new Set(Array.isArray(b.mark) ? b.mark.map(String) : ids);   // (the sheets that keep the "taken out" mark; SetRepair lets GF / SS go at once)
    if (!isId(runId) || !isId(setId) || !ids.length) return words(400, "bad run, set or sheet");
    const now = Date.now();
    const out = await db.runTransaction(async tx => {
      await fenceTx(tx);
      const rs = await tx.get(col(RUNS).doc(runId)), run = rs.exists ? rs.data() || {} : null;
      if (!run || ["complete", "abandoned", "archived"].includes(String(run.status || ""))) return words(409, "This run is finished");
      if (run.commitRequested || (run.status === "running" && ["labels", "commit"].includes(String(run.step || "")))) return words(409, "The set is being committed. Try again soon.");
      const ss = await tx.get(col(SETS).doc(setId)); if (!ss.exists) return words(404, "That set is not in the cloud");
      const set = Object.assign({ setId }, ss.data());
      if (SetEdit.committedSet(set)) return words(409, "Its set was sent to the station: take it out of there, not here");
      const memberIds = [...new Set((set.sheetIds || []).map(String).concat(ids))].filter(isId).slice(0, 120);
      const docs = await txGetAll(tx, memberIds.map(i => col(SHEETS).doc(i))), recs = new Map(docs.filter(x => x.exists).map(x => [x.id, Object.assign({ id: x.id }, x.data())]));
      const leave = [];
      for (const id of ids) {
        const r = recs.get(id); if (!r) return words(404, "That sheet is not in the cloud");
        const n = nameOf(r);
        if (SetEdit.inSetOf(r) !== setId) continue;   // (already out: nothing to do for it)
        if (cutRec(r)) return words(409, `${n} was already cut: a cut sheet stays in its set`);
        if ((r.keepInSet && r.keepInSet.setId === setId) || (r.ownerSet && (r.ownerSet.setId || null) === setId)) return words(409, `${n} was put in this set by the owner (Back in the set): take it out from the Library, where the owner's choice is changed`);
        if (manualLive(r, now) || holdingLive(r, now)) return words(409, `The sheet could not be taken out of the set: it is being changed in its sheet window`);
        const busy = busyRec(r, now); if (busy) return words(409, `${n} ${busy}: try again once it is done`);
        leave.push(r);
      }
      if (!leave.length) return { ok: true, noop: true, left: [] };
      const gone = new Set(leave.map(r => r.id)), members = [...recs.values()].filter(r => SetEdit.inSetOf(r) === setId && !r.archived), stay = members.filter(r => !gone.has(r.id));
      if (!whole) {   // (the cardinal rule: an order shared with a sheet that stays keeps the sheet in the set)
        const stayOrders = new Set(stay.flatMap(SetEdit.orderIdsOf)), shared = leave.flatMap(SetEdit.orderIdsOf).filter(o => stayOrders.has(o));
        if (shared.length) return words(409, `The sheet could not be taken out of the set: an order on it (${[...new Set(shared)].slice(0, 3).join(", ")}) is shared with another sheet of the set`, { shared: [...new Set(shared)].slice(0, 20) });
      }
      const after = SetEdit.setAfter(set, { leave, join: [], members, skus: {}, sides: {}, by, at: now });
      const entry = { id: `edit-${now}-${setId}`, at: now, by, type: "setEdit", out: leave.map(r => r.id), in: [], note: `${leave.map(nameOf).join(", ")} taken out` };
      const patch = { sheetIds: after.sheetIds, materials: after.materials, orders: after.orders, labelFiles: after.labelFiles, labels: null, setEdit: { at: now, by }, setRev: num(set.setRev) + 1, flowHistory: (Array.isArray(set.flowHistory) ? set.flowHistory : []).concat([entry]).slice(-100), updatedAt: FV.serverTimestamp() };
      if (after.committed) patch.committed = after.committed;
      if (after.committedOut) patch.committedOut = after.committedOut;
      tx.update(col(SETS).doc(setId), patch);   // (update: each field named replaces the stored one whole, as flowApply's set edit writes it; a merge would keep the order lines of the sheets that left)
      for (const r of leave) {
        const solid = r.metal === "gold10k" || r.metal === "gold14k", history = (Array.isArray(r.flowHistory) ? r.flowHistory : []).concat([{ id: `setLeave-${now}-${r.id}`, at: now, by, type: "setLeave", note: `Taken out of ${SetEdit.setWord(set)}`, setId, laserDoneAt: null }]).slice(-100);
        const p = { draft: true, setId: null, setSeq: null, sheetIndex: null, label: null, flowHistory: history, current: true, updatedAt: FV.serverTimestamp() };
        if (mark.has(r.id)) p.leftSet = setId; else p.leftSet = null;
        if (solid) { p.solidIncluded = false; p.solidPick = false; }
        if (r.metal === "rose") p.autoCutOff = true;   // (a person's take-out wins: the automatic cut never puts it back, Gate.autoCut)
        tx.set(col(SHEETS).doc(r.id), p, { merge: true });
      }
      return { ok: true, left: leave.map(r => r.id), names: leave.map(nameOf), orders: [...new Set(leave.flatMap(SetEdit.orderIdsOf))], setName: SetEdit.setWord(set) };
    });
    if (out && out.ok && !out.noop) { revTouch(b, ["t:" + setId].concat(out.left.map(id => "s:" + id))); feedOrders(b, [out.orders]); }
    return out;
  }

  /* ── cmdSwapLine (Paul, 23 Nov 2026: "Everything the user does is primary and must be immediate including the cloud"): a piece made again takes the old one's place in ONE write: its record, its entry on its sheet
     and where it lies. Nothing leaves the sheet first, no place is kept, nothing is waited for: a nest or a file write under way is of the sheet as it was, and its result is never written over this one (layoutRev,
     contentRev move: the writer's own rule). Paul, 23 Nov 2026: "Unless a sheet is marked as laser cut including RG 10k, 14k the user should be able to update/remove/cancel a given charm ... nothing should prevent
     that": only the laser's mark (the sheet's, or its set's) refuses, in words (keep); planned on another layout it is planned again (conflict). A green line, the metal it takes, a recorded cut, an approval and a set
     sent to the station stay as they are round the change: the new pieces stand inside the line the old ones stood in (lineKeeps), the set's order list names them, its QR label is owed again. ── */
  async function cmdSwapLine(b) {
    const runId = str(b.runId, 80), lineKey = str(b.lineKey, 80), rid = str(b.rid, 30), sheetId = str(b.sheetId, 80), by = str(b.by, 80) || "Cloud", mine = id => String(id).startsWith(lineKey + "_");
    const pools = (Array.isArray(b.pools) ? b.pools : []).filter(p => p && mine(p.poolId)), charms = Placement.cleanCharms(Array.isArray(b.charms) ? b.charms : []), ids = pools.map(p => String(p.poolId)), poses = Array.isArray(b.poses) ? b.poses : [];
    if (!isId(runId) || !isKey(lineKey) || !isId(sheetId) || charms.length !== ids.length || charms.some(c => !ids.includes(String(c.poolId)))) return words(400, "bad run, line, sheet or pieces");
    const ref = col(SHEETS).doc(sheetId), now = Date.now();
    const out = await db.runTransaction(async tx => {
      await fenceTx(tx);
      const s = await tx.get(ref); if (!s.exists) return words(404, "That sheet is not in the cloud");
      const r = Object.assign({ id: s.id }, s.data()), n = nameOf(r), old = new Set((r.poolIds || []).map(String).filter(mine)), keep = t => words(409, `${n} ${t}: its piece stays as it is`, { keep: true }), sets = await listingSets(tx, r.id);
      if (r.archived || r.runId !== runId) return keep("is not a sheet of the open run");
      if ([r].concat(sets.map(x => x.data())).some(x => num(x.laserDoneAt) > 0)) return keep("is marked laser cut");
      if (!old.size) return words(409, `${n} no longer holds this line`, { gone: true });
      if (poses.length && num(r.layoutRev) !== num(b.layoutBase)) return words(409, `${n} changed while its new piece was seated`, { conflict: true });
      // (FRESHWRITE, Paul 27 Nov 2026: a move by hand (Manual Control) changes the placements and the content revision, not layoutRev: poses seated on the sheet before it could land on a piece a person just moved;
      //  the request says the content it seated on too (contentBase), and is seated again on the sheet as it is now)
      if (poses.length && b.contentBase != null && Number.isFinite(+b.contentBase) && num(r.contentRev) !== num(b.contentBase)) return words(409, `${n} was changed by hand while its new piece was seated`, { conflict: true });
      const all = [...new Set([...old, ...ids])], rows = await txGetAll(tx, all.map(id => col(POOL).doc(id)));
      if (ids.length) { const q = await tx.get(col(SHEETS).where("poolIds", "array-contains-any", ids.slice(0, 30))); if (q.docs.some(x => x.id !== sheetId && !x.data().archived)) return keep("shares this line with another sheet"); }
      const seat = poses.length && poses.length === ids.length ? new Map(poses.map(p => [String(p.poolId), p])) : null, w = without(r, old), was = new Map(w.going.map(c => [String(c.poolId), c]));
      // an engraving is fitted to its drawing: a piece whose drawing changed does not keep the old one's back
      const same = z => { const o = was.get(String(z.poolId)), c = charms.find(y => String(y.poolId) === String(z.poolId)); return !!(o && c && o.hash && String(o.hash) === String(c.hash)); };
      const backs = b.keepBacks === true ? w.backsGoing.filter(same) : [], dropped = w.backsGoing.filter(z => !backs.includes(z));
      const placements = w.placements.concat(seat ? charms.map(c => { const p = seat.get(String(c.poolId)), o = w.poses.find(z => z && z.id === (was.get(String(c.poolId)) || {}).id) || {}; return Object.assign({}, o.n == null ? {} : { n: o.n }, { id: c.id, layer: String(o.layer || "").replace(String((was.get(String(c.poolId)) || {}).sku || "\u0000"), c.sku || "") || `${c.order}  ${c.sku || ""}`, name: c.name || "", hash: c.hash || null, source: c.sourceName || null, index: num(c.index), cxPt: +p.cxPt, cyPt: +p.cyPt, angle: +p.angle || 0, scale: num(p.scale) || 1, wPt: num(p.wPt), hPt: num(p.hPt), areaPt2: num(c.areaPt2) }); }) : []);
      const nextCharms = w.charms.concat(charms.map(c => { const x = Object.assign({}, c, { pinned: null, metal: r.metal }); delete x.source; return fitted(x, seat && seat.get(String(c.poolId)), now); })), poolIds = [...w.poolIds.map(String), ...ids], srcs = (r.sources || []).concat(charms.map(c => c.custom && c.source && c.source.path && !(r.sources || []).some(z => z && z.id === c.source.id) ? { id: str(c.source.id, 300), name: str(c.source.name, 160), path: str(c.source.path, 300), custom: true } : null).filter(Boolean)), own = new Set(orderList(nextCharms).concat(poolIds.map(ridOfPool)));
      const next = { charms: nextCharms, placements, poolIds, rejects: w.rejects, feedWait: w.feedWait, backPool: w.backPool.concat(backs), orders: [...new Set((Array.isArray(r.orders) ? r.orders : []).map(String).concat(ids.length ? [rid] : []))].filter(o => own.has(o)),
        charmCount: nextCharms.filter(c => c && !c.excluded).length, placedCount: placements.length, filesOwed: now, layoutRev: num(r.layoutRev) + 1, contentRev: num(r.contentRev) + 1, updatedAt: FV.serverTimestamp(), ...(ids.length ? {} : { takenOffAt: now }),
        ...RoseStock.lineKeeps(r, w.cids, seat ? placements.slice(w.placements.length).map(p => ({ placement: p, shape: (seat.get(String((charms.find(c => c.id === p.id) || {}).poolId)) || {}).shape })).filter(x => x.shape) : []), ...(r.label ? { labelOwed: now, ...(r.draft && !r.label.own && !ids.length ? { label: null } : {}) } : {}), ...(srcs.length > (r.sources || []).length ? { sources: srcs } : {}),
        ...(ids.length && !seat ? { nestOwed: { at: now, n: ids.length, by: "cloud", why: "made again in place" }, appendOnly: placements.length > 0, intakeAppend: placements.length > 0, status: "ready", dirty: true } : {}) };
      if (Buffer.byteLength(JSON.stringify(Object.assign({}, r, next, { updatedAt: 0 }))) > SHEET_DOC_BYTES) return words(413, `${n} cannot hold these pieces in one record`, { keep: true });
      tx.set(ref, next, { merge: true });
      for (const k of new Set(dropped.map(z => Faces.recKey(String(z.poolId), Faces.faceOf(z))))) tx.set(col("Charm_Pool_Back").doc(k), { invalidated: true, invalidatedAt: now, invalidatedWhy: "the charm changed", sheetId: null, updatedAt: FV.serverTimestamp() }, { merge: true });
      for (const p0 of pools) { const x = rows[all.indexOf(String(p0.poolId))], p = Object.assign({}, p0), sided = PoolPieces.cleanFields(p), what = Object.assign({}, sided.side ? sided : PoolPieces.cleanGroupFields(p), PoolPieces.cleanBody(p)); for (const k of PoolPieces.FIELDS.concat("oneBody")) delete p[k];
        tx.set(col(POOL).doc(p.poolId), Object.assign(Placement.cleanPiece(Object.assign(p, what, { sheetId, sheetName: r.fileBase || null, placedAt: now, placedBy: "cloud", swapAt: now, updatedAt: FV.serverTimestamp() })), x.exists ? (x.data().state === "committed" ? { state: "committed" } : {}) : { createdAt: FV.serverTimestamp() }, x.exists && Placement.takenOff(x.data()) ? { [Placement.REPOOLED]: FV.serverTimestamp() } : {}), { merge: true }); }
      for (const id of old) if (!ids.includes(id)) tx.set(col(POOL).doc(id), { state: "superseded", sheetId: null, supersededAt: now, supersededBy: by, vacatedFrom: r.id, updatedAt: FV.serverTimestamp() }, { merge: true });
      const setsDone = [];   // (SHEETLIVE stage 4 (Paul, 27 Nov 2026: "Make sure that all the automatic connections and changes that the system can make on behalf of the user are correctly wired in to this new one live copy ... Everything has to be wired perfectly and update immediately"): every set this swap rewrites is named on the feed, not only the one the sheet's record names: a draft listed by a set, a set listing it under another id)
      for (const x of sets) { const set = Object.assign({ setId: x.id }, x.data()), left = setOrdersWithout(set.orders, old); if (!left) continue; setsDone.push(String(x.id));   // (its order list names the pieces as they are now)
        const st = Core.asSet(Object.assign(set, { orders: left })); if (ids.length) Core.addCopies(st, { sheetId: r.id, fileBase: r.fileBase || r.id, charms, placements: charms.map(c => ({ id: c.id })) }, id => pools.find(p => String(p.poolId) === String(id)) || {});
        tx.update(x.ref, { orders: Core.setPatch(st).orders, setRev: num(set.setRev) + 1, updatedAt: FV.serverTimestamp() }); }   // (FRESHWRITE, 27 Nov 2026: a look of the cloud planned before this change is refused and planned again: setBase)
      return { ok: true, setsDone, sheetId, name: n, runId, seated: seat ? ids.length : 0, here: !!seat && poses.every(p => p.here === true), ...(seat && poses.some(p => p.size) ? { size: Math.min(...poses.map(p => num(p.size) || 1)) } : {}), off: old.size, setId: r.draft ? "" : str(r.setId, 100), layoutRev: next.layoutRev, ...(dropped.length ? { backsDropped: [...new Set(dropped.map(z => String(z.poolId)))] } : {}) };
    });
    if (out && out.ok) { revTouch(b, ["s:" + sheetId].concat(out.setId ? ["t:" + out.setId] : [], (out.setsDone || []).map(id => "t:" + id))); delete out.setsDone; feedOrders(b, [[rid]]); const id = `swap-${lineKey}-${now}`, ev = (type, text, k, n) => ({ orderId: rid, type, at: now + (type === "placed" ? 1 : 0), by, station: "sorter", device: "cloud", sheetId, sheet: str(out.name, 80), setId: out.setId, lineKey, text, id: `${id}-${k}`, data: { copies: n, poolIds: ids.slice(0, 40), swap: true } });
      if (d.stamp) await d.stamp([ev("removed", `Came off ${out.name}`, "off", out.off)].concat(ids.length ? [ev("placed", `Went onto ${out.name}${out.here ? ", in the old piece's place" : ""}${out.size ? `, resized to ${Math.round(out.size * 100)}% to fit its place` : ""}`, "on", ids.length)] : []), "swap"); if (out.seated || !ids.length) await require("./_charmNestFinishOps").startWriter(db, sheetId, { runId, reason: "owed" }); }
    return out;
  }

  /* ── cmdRestoreNest (RGRESTORE, Paul 22 Nov 2026: RG Sheet 1 held 29 pieces after nest-8462e548578a82c3789dfc62; the writer's ONCEWHOLE bug lifted 11 of them, they went with the
     remade BULLSEYE onto RG Draft 2, which took the leftover of Sheet 1's automatic cut and stopped the run. "Put them back where they were") ──
     { runId, sheetId, jobId, from: [sheetId] (1-4), by, dry? } → { ok, restored, orders, to: { sheetId, name, layoutRev, contentRev }, from: [{ sheetId, name, left, emptied }] } | { ok, noop }
     The pieces a finished nest of `sheetId` placed (Charm_Nest_Jobs/<jobId>: its own result, verified, no overlap) that a later write took off and that now wait on one of `from` go back
     onto the sheet EXACTLY where that nest left them. Every piece the sheet still holds must stand where that nest left it, so the sheet is that nest's verified layout again, never a new
     one. Nothing is nested: the sheet owes its files and, once they are written, its green line (roseRedraw; its cut is recorded again by its own cut, automatic or a person's press).
     A `from` sheet left with no piece is emptied whole: its turned-away and waiting lists, the problem and the run stop it asked (problem, problemStop) go and it owes nothing.
     The pieces' records name the sheet again (in a set: that set, "written", as the set's other pieces). Refused in words, nothing written: the laser cut the sheet or its set, its set
     was sent to the station, a green line or a recorded cut still stands on it (set aside first: roseReopen), its physical sheet is not its own or another sheet holds or cut it since,
     a piece of the sheet is not where the nest left it or the sheet holds a piece the nest did not place (the pieces could overlap), a piece is no longer on `from`, was taken off or
     committed, a pair would be split, a sheet is busy, open in Manual Control by someone else, in a set (a `from`), of another run or metal.
     dry: the same checks, but the sheet's own recorded cut (not lasered) and a `from` sheet holding the sheet's physical sheet are allowed: the request sets them aside first (roseRelease,
     roseReopen). Nothing written. Asked again once done: { ok, noop } (the pieces are back already), nothing written. */
  async function cmdRestoreNest(b) {
    const runId = str(b.runId, 80), sheetId = str(b.sheetId, 80), jobId = str(b.jobId, 80), by = str(b.person || b.by, 80) || "a person", dry = b.dry === true;
    const froms = [...new Set((Array.isArray(b.from) ? b.from : [b.from]).map(x => str(x && typeof x === "object" ? x.sheetId : x, 80)).filter(isId))].slice(0, 4);
    if (!isId(runId) || !isId(sheetId) || !isId(jobId) || !froms.length || froms.includes(sheetId)) return words(400, "Name the sheet, the nest whose layout goes back and the sheet its pieces are on now");
    const sRef = col(SHEETS).doc(sheetId), jRef = col("Charm_Nest_Jobs").doc(jobId), now = Date.now();
    const near = (a, b2, t) => Math.abs(num(a) - num(b2)) <= t, turn = a => ((Math.round(+a * 1000) / 1000) % 360 + 360) % 360;
    const samePose = (p, q) => near(p.cxPt, q.cxPt, 0.001) && near(p.cyPt, q.cyPt, 0.001) && Math.abs(turn(p.angle || 0) - turn(q.angle || 0)) <= 0.001 && near(+p.scale > 0 ? +p.scale : 1, +q.scale > 0 ? +q.scale : 1, 0.00001);
    const out = await db.runTransaction(async tx => {
      await fenceTx(tx);
      const [ss, js, ...fs] = await txGetAll(tx, [sRef, jRef, ...froms.map(id => col(SHEETS).doc(id))]);
      if (!ss.exists || fs.some(x => !x.exists)) return words(404, "That sheet is not in the cloud");
      const s = Object.assign({ id: ss.id }, ss.data()), srcs = fs.map(x => Object.assign({ id: x.id }, x.data())), sn = nameOf(s);
      if (s.archived) return words(409, `${sn} was repacked into other sheets`);
      if (s.runId !== runId) return words(409, `${sn} is not a sheet of the run the cloud carries`);
      const j = js.exists ? js.data() || {} : null, res = j && j.result, ver = res && res.verification;
      if (!j || j.sheetId !== s.id || j.status !== "done" || !res || !Array.isArray(res.placements) || !res.placements.length) return words(409, `That nest has no finished layout of ${sn}: nothing was put back`, { refused: "nest" });
      if (!ver || ver.ok !== true || num(ver.overlapPairs) > 0 || num(ver.overlapPx) > 0) return words(409, `That nest's layout of ${sn} was not verified free of overlaps: nothing was put back`, { refused: "overlap" });
      const pose = new Map(res.placements.filter(p => p && p.id != null).map(p => [String(p.id), p])), live = (s.charms || []).filter(c => c && !c.excluded), liveIds = new Set(live.map(c => String(c.id)));
      const back = [...pose.keys()].filter(id => !liveIds.has(id));
      if (!back.length) return { ok: true, noop: true, dry, restored: 0, to: { sheetId: s.id, name: sn, layoutRev: num(s.layoutRev), contentRev: num(s.contentRev) } };   // (asked again once done: every piece of that nest is on the sheet already)
      // the laser and the station: never changed
      if (num(s.laserDoneAt) > 0) return words(409, `${sn} has been cut on the laser: its charms can no longer change`, { refused: "cut" });
      const sets = (await listingSets(tx, s.id)).map(d => Object.assign({ setId: d.id }, d.data()));
      if (s.setId && !s.draft && !sets.some(x => x.setId === s.setId)) { const one = await tx.get(col(SETS).doc(String(s.setId))); if (one.exists) sets.push(Object.assign({ setId: one.id }, one.data())); }
      { const st = sets.find(x => num(x.laserDoneAt) > 0); if (st) return words(409, `${sn}'s set was cut on the laser: its charms can no longer change`, { refused: "cut" }); }
      { const st = sets.find(x => SetEdit.committedSet(x)); if (st) return words(409, `${sn}'s set (${SetEdit.setWord ? SetEdit.setWord(st) : st.name || st.setId}) was sent to the station: its charms stay as they are`, { refused: "committed" }); }
      if (!dry && (s.roseCutAt || lineDrawn(s) || s.rosePlanHash)) return words(409, `${sn}'s green line or its recorded cut still stands: it is set aside before its pieces come back. Try again`, { refused: "line" });
      // the sheets: still, of this run and metal; the pieces come off drafts only
      for (const r of [s, ...srcs]) {
        const n = nameOf(r);
        if (r.archived) return words(409, `${n} was repacked into other sheets`, { gone: true });
        if (r.runId !== runId || r.metal !== s.metal) return words(409, `${n} is not a ${s.metal} sheet of the run the cloud carries`);
        const busy = busyRec(r, now); if (busy) return words(409, `${n} ${busy}: try again once it is done`, { busy: true });
        if (manualLive(r, now) && str(r.manualEdit.by, 80) !== by) return words(409, `${n} is open in Manual Control by ${str(r.manualEdit.by, 80) || "someone"} on another screen`, { refused: "held" });
      }
      for (const r of srcs) {
        if (cutRec(r)) return words(409, `${nameOf(r)} was already cut: its pieces stay as they are`, { refused: "cut" });
        if (lineDrawn(r)) return words(409, `${nameOf(r)} has a green line drawn: its pieces stay as they are`, { refused: "line" });
        if ((r.setId && !r.draft) || (await listingSets(tx, r.id)).length) return words(409, `${nameOf(r)} is in a set: take it out of its set first`, { refused: "set" });
      }
      // the physical sheet: the sheet's own again (or, dry, about to be: held by a `from`, or free), its leftover never taken by a third sheet
      const stockId = str(s.roseStockId, 120);
      if (!stockId) return words(409, `${sn} holds no physical sheet: nothing was put back`, { refused: "stock" });
      const stRef = col("Charm_Nest_Rose_Stock").doc(stockId), sd = await tx.get(stRef), stock = sd.exists ? sd.data() || {} : null;
      if (!stock || stock.deleted) return words(409, `${sn}'s physical sheet is not in the cloud`, { refused: "stock" });
      const mine = new Set([s.id, ...froms]), third = (await tx.get(col(SHEETS).where("roseStockId", "==", stockId))).docs.map(d => Object.assign({ id: d.id }, d.data()))
        .filter(r => !r.archived && !mine.has(r.id) && !(r.roseCutAt && r.roseCutRevision != null && num(r.roseCutRevision) < num(s.roseCutRevision || stock.revision)));   // (a sheet that cut this stock BEFORE is its history, not a holder)
      if (third.length) return words(409, `${sn}'s physical sheet is used by a third sheet (${nameOf(third[0])}): nothing was put back`, { refused: "stock" });
      if (stock.owner && !(dry ? mine.has(stock.owner) : stock.owner === s.id)) return words(409, `${sn}'s physical sheet is held by another sheet: nothing was put back`, { refused: "stock" });
      if (s.roseCutAt) {   // (dry: what roseReopen will ask of the cut it sets aside, said here before anything is written)
        const rev = s.roseCutRevision != null ? num(s.roseCutRevision) : num(stock.revision);
        if (num(stock.revision) !== rev) return words(409, `The leftover ${sn}'s cut made has been cut from since by another sheet: its metal is that sheet's now, so nothing was put back`, { refused: "stock" });
        if (rev >= 1) {
          const rs = await tx.get(col("Charm_Nest_Remnants").doc(`${stockId}-${rev}`)), rem = rs.exists ? rs.data() || {} : null;
          if (rem && rem.status === "inUse" && rem.inUseBySheetId && !mine.has(rem.inUseBySheetId)) return words(409, `The leftover ${sn}'s cut made is in use by ${rem.inUseBySheetName || "a third sheet"}: nothing was put back`, { refused: "stock" });
          if (rem && rem.status === "used" && rem.usedBySheetId && rem.usedBySheetId !== s.id) return words(409, `The leftover ${sn}'s cut made was used by ${rem.usedBySheetName || "a third sheet"}: nothing was put back`, { refused: "stock" });
          const aside = new Set((Array.isArray(s.roseReseated) ? s.roseReseated : []).filter(x => x && x.reopen && x.cut && x.cut.revision != null).map(x => num(x.cut.revision)));
          for (let r = rev - 1; r >= 1; r--) {
            const q = await tx.get(stRef.collection("cuts").where("revision", "==", r).limit(1)), c = q.docs[0] ? q.docs[0].data() : null;
            if (aside.has(r) && (!c || c.sheetId === s.id)) continue;
            let p = null; try { p = c ? JSON.parse(c.planJson || "null") : null; } catch (_) { p = null; }
            if (!p || !p.profile) return words(409, `The outline ${sn}'s physical sheet had before its cut cannot be read: nothing was put back`, { refused: "stock" });
            break;
          }
        }
      } else if (!dry && num(s.roseRevision) !== num(stock.revision)) return words(409, `${sn}'s physical sheet changed since it was set aside: nothing was put back`, { refused: "stock" });
      // the positions: the sheet as it stands is that nest's layout (each piece where the nest left it), so the pieces coming back overlap none of them
      const moved = (s.placements || []).filter(p => p && liveIds.has(String(p.id)) && !(pose.has(String(p.id)) && samePose(p, pose.get(String(p.id)))));
      if (moved.length) return words(409, `${sn}: ${moved.length === 1 ? "a piece is" : moved.length + " pieces are"} no longer where that nest left ${moved.length === 1 ? "it" : "them"} (${moved.slice(0, 3).map(p => String(p.id).split(":").pop()).join(", ")}): the pieces put back could overlap, so nothing was put back`, { refused: "overlap" });
      const extra = live.filter(c => !pose.has(String(c.id)));
      if (extra.length) return words(409, `${sn} holds ${extra.length === 1 ? "a piece" : extra.length + " pieces"} that nest did not place (${extra.slice(0, 3).map(c => c.name || c.id).join(", ")}): the pieces put back could overlap, so nothing was put back`, { refused: "overlap" });
      // the pieces to bring back: placed by the nest, not on the sheet now, each waiting on one of `from` (listed there, its charm there)
      const bySrc = new Map();
      for (const id of back) {
        const r = srcs.find(x => (x.charms || []).some(c => c && !c.excluded && String(c.id) === id && c.poolId && (x.poolIds || []).map(String).includes(String(c.poolId))));
        if (!r) return words(409, `A piece that nest placed (${id.split(":").pop()}) is no longer on ${and(srcs.map(nameOf))}: it was moved or taken off meanwhile. Nothing was put back`, { gone: true });
        if (!bySrc.has(r.id)) bySrc.set(r.id, new Set()); bySrc.get(r.id).add(id);
      }
      const lineOf = id => String(id).replace(/_\d+$/, "");
      const going = [];
      for (const r of srcs) {
        const ids = bySrc.get(r.id); if (!ids) continue;
        const g = (r.charms || []).filter(c => c && !c.excluded && ids.has(String(c.id))), lines = new Set(g.map(c => lineOf(c.poolId)));
        const half = (r.charms || []).filter(c => c && !c.excluded && c.poolId && lines.has(lineOf(c.poolId)) && !ids.has(String(c.id)));
        if (half.length) return words(409, `Order ${ridOfPool(half[0].poolId)} stays where it is: a piece of its line on ${nameOf(r)} is not part of that nest, and a pair is never moved in halves`, { refused: "pair" });
        for (const c of g) going.push({ c, from: r });
      }
      const rows = await txGetAll(tx, going.map(m => col(POOL).doc(String(m.c.poolId))));
      const off = rows.filter(x => x.exists && Placement.takenOff(x.data())).map(x => x.id), done = rows.filter(x => x.exists && x.data().state === "committed").map(x => x.id);
      if (off.length) return words(409, `Order ${ridOfPool(off[0])} was put on hold or taken off meanwhile: nothing was put back`, { taken: off.slice(0, 20) });
      if (done.length) return words(409, `${done.length === 1 ? "A piece" : done.length + " pieces"} of that nest were already committed: nothing was put back`);
      if (dry) return { ok: true, dry: true, restored: going.length, orders: orderList(going.map(m => m.c)), to: { sheetId: s.id, name: sn } };
      // the writes: each `from` without the pieces (emptied whole when none is left); the sheet with them where the nest left them; no nest owed anywhere
      const left = [], goBacks = srcs.flatMap(r => (bySrc.get(r.id) ? without(r, new Set(going.filter(m => m.from.id === r.id).map(m => String(m.c.poolId)))).backsGoing : [])), bKeys = await standingKeys(tx, goBacks);   // (CARRYBACKS: the engravings go back with them)
      for (const r of srcs) {
        const ids = bySrc.get(r.id); if (!ids) continue;
        const w = without(r, new Set(going.filter(m => m.from.id === r.id).map(m => String(m.c.poolId)))), kept = new Set(orderList(w.charms).concat(w.poolIds.map(ridOfPool)));
        const emptied = !w.charms.filter(c => c && !c.excluded).length;
        const next = { charms: w.charms, placements: w.placements, poolIds: w.poolIds, rejects: w.rejects, feedWait: w.feedWait, backPool: w.backPool, orders: (Array.isArray(r.orders) ? r.orders : []).map(String).filter(o => kept.has(o)),
          charmCount: w.charms.length, placedCount: w.placements.length, rejectCount: Array.isArray(w.rejects) ? w.rejects.length : 0, layoutRev: num(r.layoutRev) + 1, contentRev: num(r.contentRev) + 1, updatedAt: FV.serverTimestamp() };
        if (next.feedWait === undefined) delete next.feedWait; if (next.rejects === undefined) delete next.rejects;
        if (emptied) Object.assign(next, { charms: [], placements: [], poolIds: [], rejects: [], feedWait: null, backPool: [], orders: [], charmCount: 0, placedCount: 0, rejectCount: 0, status: "idle", dirty: false, nestOwed: null,
          problem: null, problemStop: null, noRoomRun: null, releaseFull: false, releaseWhy: null, filesOwed: null, restoredOff: { to: s.id, toName: sn, jobId, n: ids.size, at: now, by } });
        else Object.assign(next, { dirty: true, filesOwed: now });
        tx.set(col(SHEETS).doc(r.id), next, { merge: true });
        left.push({ sheetId: r.id, name: nameOf(r), left: emptied ? 0 : w.charms.length, emptied });
      }
      const sCharms = (s.charms || []).slice(), sPl = (s.placements || []).filter(p => p && liveIds.has(String(p.id)));
      for (const m of going) {
        const x = Object.assign({}, m.c, { metal: s.metal, pinned: null }); delete x.arrivalPin; sCharms.push(x);
        const p = pose.get(String(m.c.id));
        sPl.push(Object.assign({ n: sPl.length + 1, id: m.c.id, layer: p.layerName || m.c.layer || "", name: m.c.name || "", hash: m.c.hash || null, source: m.c.sourceName || "", index: num(m.c.index) },
          { angle: turn(p.angle || 0), scale: +p.scale > 0 ? +p.scale : 1, cxPt: +p.cxPt, cyPt: +p.cyPt }, num(p.wPt) > 0 && num(p.hPt) > 0 ? { wPt: +p.wPt, hPt: +p.hPt } : {}, num(m.c.areaPt2) > 0 ? { areaPt2: +m.c.areaPt2 } : {}));
      }
      const gIds = new Set(going.map(m => String(m.c.id))), gPool = going.map(m => String(m.c.poolId)), gOrders = orderList(going.map(m => m.c)), inSet = !!(s.setId && !s.draft);
      const sRejects = (Array.isArray(s.rejects) ? s.rejects : []).filter(x => !gIds.has(String(x && typeof x === "object" ? x.id : x)));
      const carry = PoolPieces.carrySources(s, going.map(m => m.c), srcs);   // (SRCLIST: the pieces' design files come back with them)
      const sNext = { charms: sCharms, placements: sPl, poolIds: [...new Set((s.poolIds || []).map(String).concat(gPool))], orders: [...new Set((Array.isArray(s.orders) ? s.orders : []).map(String).concat(gOrders))], ...(carry || {}),
        ...(goBacks.length ? { backPool: (s.backPool || []).filter(x => !(x && gPool.includes(String(x.poolId)))).concat(backsTo(goBacks, s)) } : {}), charmCount: sCharms.filter(c => c && !c.excluded).length, placedCount: sPl.length, rejects: sRejects, rejectCount: sRejects.length, feedWait: null,
        noRoomRun: Array.isArray(s.noRoomRun) ? s.noRoomRun.map(String).filter(o => !gOrders.includes(o)) : (s.noRoomRun == null ? null : s.noRoomRun), movedOn: null, nestOwed: null, dirty: false,
        layoutRev: num(s.layoutRev) + 1, contentRev: num(s.contentRev) + 1, filesOwed: now, roseRedraw: { at: now, by, line: true, cut: !!(s.roseReopened && s.roseReopened.cut) },
        restored: { jobId, n: going.length, from: left.map(l => l.sheetId), at: now, by }, updatedAt: FV.serverTimestamp() };
      if (bytesOf(Object.assign({}, s, sNext, { updatedAt: 0 })) > SHEET_DOC_BYTES) return words(413, `${sn} cannot hold these pieces in one record`);
      tx.set(sRef, sNext, { merge: true });
      carryBacks(tx, bKeys, s, now);
      for (const m of going) tx.set(col(POOL).doc(String(m.c.poolId)), Object.assign({ sheetId: s.id, movedFrom: m.from.id, movedTo: s.id, movedBy: by, movedAt: now, movedHow: "restore", updatedAt: FV.serverTimestamp() },
        s.fileBase ? { sheetName: s.fileBase } : {}, inSet ? { setId: s.setId, state: "written" } : {}), { merge: true });
      return { ok: true, restored: going.length, pieces: gPool, orders: gOrders, inSet, from: left, to: { sheetId: s.id, name: sn, layoutRev: sNext.layoutRev, contentRev: sNext.contentRev } };
    });
    if (out && out.ok && !out.dry && !out.noop) { revTouch(b, ["s:" + sheetId].concat(froms.map(id => "s:" + id))); feedOrders(b, [out.orders]); }
    return out;
  }

  /* ── cmdRoseMerge (RGMERGE, Paul 23 Nov 2026: "Those pieces should've been part of sheet one without the green cut line"; "Move the 7 pieces from RG Sheet 1 onto the 19-piece sheet, with no
     green line: YES") ── { runId, sheetId: the sheet that empties, targetId, by, dry? } → { ok, moved, orders, set, lift, ... } | { ok, noop }
     Two Rose Gold sheets of ONE set that were cut (green line recorded, nothing on the laser) from the SAME physical sheet, one after the other, become ONE sheet, in ONE transaction:
       · both recorded cuts are set aside (their records stay, marked): the physical sheet is the target's again with the outline it had before its first cut (whole when none stands before);
         the leftovers those cuts made are never offered again
       · the pieces go onto the target EXACTLY where they lie: both layouts are on one physical sheet and the second was nested past the first's line (checked again here, outline by outline),
         so nothing is nested and nothing the target holds moves. The target owes its files, then ONE green line and its cut (its set's automatic cut, or Cut Sheet)
       · the emptied sheet leaves its set (the set's list, its order copies now on the target, its label files) and is archived; the pieces' records and approved engravings name the target
       · a draft in no set that holds the last leftover gives it back (its pieces stay as they lie): the request seats it again once the new line is cut
     Refused in words, nothing written: cut on the laser, the set approved, waiting at or sent to the station, not one set or one physical sheet, a layout changed since its cut, a third sheet cut
     from or (cut, in a set, with a line) seated on the plate, a piece unplaced, taken off, committed or being made again, a sheet busy or open by hand. Asked again once done: { ok, noop }. */
  async function cmdRoseMerge(b) {
    const runId = str(b.runId, 80), srcId = str(b.sheetId, 80), toId = str(b.targetId, 80), by = str(b.person || b.by, 80) || "a person", dry = b.dry === true, now = Date.now();
    if (!isId(runId) || !isId(srcId) || !isId(toId) || srcId === toId) return words(400, "Pick two sheets");
    const fRef = col(SHEETS).doc(srcId), tRef = col(SHEETS).doc(toId), json = s => { try { return s ? JSON.parse(s) : null; } catch (_) { return null; } };
    // the layout as the cut recorded it (a piece's own artwork edit changes its hash, never its place)
    const places = s => JSON.stringify((Array.isArray(s) ? s : []).map(p => p.slice(0, 5)).sort((x, y) => (String(x[0]) < String(y[0]) ? -1 : 1))), asCut = r => places(json(r.roseFingerprint)) === places(json(RoseStock.fingerprint(r)));
    const out = await db.runTransaction(async tx => {
      await fenceTx(tx);
      const [fs, ts] = await txGetAll(tx, [fRef, tRef]);
      if (!fs.exists || !ts.exists) return words(404, "That sheet is not in the cloud");
      const f = Object.assign({ id: fs.id }, fs.data()), t = Object.assign({ id: ts.id }, ts.data()), fn = nameOf(f), tn = nameOf(t);
      if (f.archived && f.mergedInto && f.mergedInto.to === t.id) return Object.assign({ ok: true, noop: true }, f.mergedInto);
      for (const [r, n] of [[f, fn], [t, tn]]) {
        if (r.archived) return words(409, `${n} was repacked into other sheets`);
        if (r.runId !== runId) return words(409, "Both sheets must be sheets of the open run");
        if (r.metal !== "rose") return words(409, "Only two Rose Gold sheets of one set are made one sheet");
        if (num(r.laserDoneAt) > 0) return words(409, `${n} has been cut on the laser: its charms can no longer change`, { refused: "cut" });
        if (manualLive(r, now) && str(r.manualEdit.by, 80) !== by) return words(409, `${n} is open in Manual Control by ${str(r.manualEdit.by, 80) || "someone"} on another screen`, { refused: "held" });
        if (holdingLive(r, now)) return words(409, `${n} is being held right now`, { refused: "held" });
        if (r.inplaceHold) return words(409, `${n} keeps a place for an order being made again: try again once it is done`, { busy: true });
        const busy = busyRec(r, now); if (busy) return words(409, `${n} ${busy}: try again once it is done`, { busy: true });
      }
      if (t.processReady) return words(409, `${tn} is approved for laser cutting: its charms stay as they are`, { refused: "approved" });
      // one open set
      const inSet = r => (r.setId && !r.draft && r.solidIncluded !== false ? String(r.setId) : null), setId = inSet(t);
      if (!setId || inSet(f) !== setId) return words(409, `${fn} and ${tn} are not in one set`, { refused: "set" });
      const setRef = col(SETS).doc(setId), ss = await tx.get(setRef); if (!ss.exists) return words(404, "Their set is not in the cloud");
      const set = Object.assign({ setId }, ss.data()), sw = SetEdit.setWord(set);
      for (const id of [f.id, t.id]) { const ls = await listingSets(tx, id); if (ls.length !== 1 || ls[0].id !== setId) return words(409, `${id === f.id ? fn : tn} is not listed by ${sw} alone`, { refused: "set" }); }
      if (num(set.laserDoneAt) > 0) return words(409, `${sw} was cut on the laser: its charms can no longer change`, { refused: "cut" });
      if (SetEdit.committedSet(set)) return words(409, `${sw} was sent to the station: its charms stay as they are`, { refused: "committed" });
      if (set.processReady || set.laserSetPending) return words(409, `${sw} is approved for laser cutting: its charms stay as they are`, { refused: "approved" });
      // the physical sheet, its cuts and leftovers
      const stockId = str(t.roseStockId, 120);
      if (!stockId || str(f.roseStockId, 120) !== stockId) return words(409, `${fn} and ${tn} are not on one physical sheet`, { refused: "stock" });
      if (!f.roseCutAt || !t.roseCutAt || f.roseCutRevision == null || t.roseCutRevision == null) return words(409, `${f.roseCutAt ? tn : fn} has no recorded green line: move its pieces as any sheet's`, { refused: "line" });
      const fr = num(f.roseCutRevision), tr = num(t.roseCutRevision), stRef = col("Charm_Nest_Rose_Stock").doc(stockId), sd = await tx.get(stRef), stock = sd.exists ? sd.data() || {} : null;
      if (!stock || stock.deleted) return words(409, "Their physical sheet is not in the cloud", { refused: "stock" });
      if (!(tr >= 1 && tr < fr)) return words(409, `${tn} was cut after ${fn}: the sheet cut first takes the other's pieces`, { refused: "stock" });
      if (num(stock.revision) !== fr) return words(409, `The leftover ${fn}'s green line made has been cut from since, by another sheet: its metal is that sheet's now`, { refused: "stock" });
      const cuts = (await tx.get(stRef.collection("cuts"))).docs.map(d => Object.assign({ _ref: d.ref, _id: d.id }, d.data())).filter(c => !c.setAsideAt);
      const fCut = cuts.find(c => c.sheetId === f.id && num(c.revision) === fr), tCut = cuts.find(c => c.sheetId === t.id && num(c.revision) === tr), between = cuts.find(c => num(c.revision) > tr && c !== fCut);
      if (!fCut || !tCut) return words(409, `The record of ${fCut ? tn : fn}'s green line cannot be read`, { refused: "stock" });
      if (between) return words(409, `A third sheet was cut from this physical sheet between ${tn} and ${fn}`, { refused: "stock" });
      const before = cuts.filter(c => num(c.revision) < tr).sort((x, y) => num(y.revision) - num(x.revision))[0] || null, bp = before ? json(before.planJson) : null;
      if (before && !(bp && bp.profile)) return words(409, "The outline their physical sheet had before could not be read", { refused: "stock" });
      const prior = bp ? JSON.stringify(bp.profile) : null, third = (await tx.get(col(SHEETS).where("roseStockId", "==", stockId))).docs.map(d => Object.assign({ id: d.id }, d.data())).filter(r => !r.archived && r.id !== f.id && r.id !== t.id && !(r.roseCutAt && r.roseCutRevision != null && num(r.roseCutRevision) < tr));
      for (const r of third) {
        const n = nameOf(r);
        if (cutRec(r) || r.runId !== runId || r.metal !== "rose" || (r.setId && !r.draft) || (await listingSets(tx, r.id)).length) return words(409, `Their physical sheet is used by a third sheet (${n}): nothing was changed`, { refused: "stock" });
        if (lineDrawn(r)) return words(409, `${n}, on the leftover of their physical sheet, has a green line drawn: nothing was changed`, { refused: "stock" });
        if (manualLive(r, now) && str(r.manualEdit.by, 80) !== by) return words(409, `${n}, on the leftover of their physical sheet, is open in Manual Control on another screen`, { refused: "held" });
        const busy = busyRec(r, now); if (busy) return words(409, `${n}, on the leftover of their physical sheet, ${busy}: try again once it is done`, { busy: true });
      }
      const mine = new Set([f.id, t.id].concat(third.map(r => r.id)));
      if (stock.owner && !mine.has(stock.owner)) return words(409, "Their physical sheet is held by another sheet: nothing was changed", { refused: "stock" });
      const rems = []; for (let r = tr; r <= fr; r++) { const ref = col("Charm_Nest_Remnants").doc(`${stockId}-${r}`), d = await tx.get(ref); rems.push({ ref, id: `${stockId}-${r}`, exists: d.exists, data: d.exists ? d.data() || {} : null }); }
      for (const r of rems) if (r.exists && ((r.data.status === "inUse" && r.data.inUseBySheetId && !mine.has(r.data.inUseBySheetId)) || (r.data.status === "used" && r.data.usedBySheetId && !mine.has(r.data.usedBySheetId)))) return words(409, `The leftover of their physical sheet is taken by ${r.data.inUseBySheetName || r.data.usedBySheetName || "another sheet"}: nothing was changed`, { refused: "stock" });
      // the pieces: all placed, where their green lines were drawn round them, the source's past the target's line
      const going = (f.charms || []).filter(c => c && !c.excluded), gid = new Set(going.map(c => String(c.id))), fPl = (f.placements || []).filter(p => p && gid.has(String(p.id)));
      if (!going.length) return words(409, `${fn} has no pieces to move`);
      if (going.some(c => !c.poolId) || (f.charms || []).some(c => c && c.excluded && c.poolId)) return words(409, `${fn} holds a piece that is not an order's: move it by hand first`);
      if (fPl.length !== going.length) return words(409, `${going.length - fPl.length} of ${fn}'s pieces ${going.length - fPl.length === 1 ? "is" : "are"} not placed: nest it first`, { refused: "unplaced" });
      for (const [r, n] of [[f, fn], [t, tn]]) if (!asCut(r)) return words(409, `${n}'s layout changed since its green line was recorded: nothing was changed`, { refused: "layout" });
      const fPlan = json(fCut.planJson), tPlan = json(tCut.planJson), shaped = new Set(((fPlan && fPlan.shapes) || []).map(s => String(s.id)));
      if (!fPlan || !tPlan || !tPlan.profile || fPl.some(p => !shaped.has(String(p.id)))) return words(409, `${fn}'s green line does not hold every piece on it: nothing was changed`, { refused: "layout" });
      for (const s of fPlan.shapes) for (const path of s.paths || []) for (let i = 0; i < path.length; i++) {
        const p = path[i], q = path[(i + 1) % path.length], n = Math.max(1, Math.ceil(Math.hypot(q[0] - p[0], q[1] - p[1]) * 4));
        for (let j = 0; j <= n; j++) if (CutLine.intersects(tPlan.profile, p[0] + (q[0] - p[0]) * j / n, p[1] + (q[1] - p[1]) * j / n, 0, 0)) return words(409, `A piece of ${fn} lies inside ${tn}'s green line: nothing was changed`, { refused: "overlap" });
      }
      const own = new Set((t.poolIds || []).map(String)); if (going.some(c => own.has(String(c.poolId)) || (t.charms || []).some(x => x && String(x.id) === String(c.id)))) return words(409, `A piece of ${fn} is on ${tn} already: nothing was changed`, { refused: "twice" });
      const ids = going.map(c => String(c.poolId)), rows = await txGetAll(tx, ids.map(id => col(POOL).doc(id)));
      const off = rows.filter(r => r.exists && Placement.takenOff(r.data())).map(r => r.id), done = rows.filter(r => r.exists && r.data().state === "committed").map(r => r.id), gone = rows.filter(r => !r.exists).map(r => r.id), away = rows.filter(r => r.exists && r.data().sheetId && String(r.data().sheetId) !== f.id).map(r => r.id);
      if (off.length) return words(409, `Order ${ridOfPool(off[0])} was put on hold or cancelled: take it off ${fn} first`, { taken: off.slice(0, 20) });
      if (done.length) return words(409, `${done.length} of these pieces were already committed: nothing was moved`);
      if (gone.length || away.length) return words(409, `${(gone[0] || away[0])} is not recorded on ${fn}: nothing was moved`, { refused: "pool" });
      const idSet = new Set(ids), w = without(f, idSet), bKeys = await standingKeys(tx, w.backsGoing), orders = orderList(going), tOrders = orderList(t.charms);
      const lift = third.map(r => ({ sheetId: r.id, name: nameOf(r) })), say = { to: t.id, toName: tn, fromName: fn, set: sw, n: going.length, stockId, fromRevision: fr, lift, at: now, by };
      if (dry) return Object.assign({ ok: true, dry: true, moved: going.length, orders }, say);
      // ── the writes ──
      for (const c of [fCut, tCut]) tx.update(c._ref, { setAsideAt: now, setAsideBy: by });
      tx.update(stRef, { profileJson: prior, owner: t.id, available: false, reopenedAt: now, reopenedBy: by, updatedAt: FV.serverTimestamp() });
      for (const r of rems) if (d.remnantSync) d.remnantSync.setAside(tx, r, { sheetId: t.id, sheetName: tn, by, at: now, reason: `${fn} and ${tn} became one sheet` });
      for (const r of third) tx.set(col(SHEETS).doc(r.id), { roseStockId: null, rosePlanJson: null, rosePlanHash: null, roseLifted: { stockId, from: t.id, at: now, by }, updatedAt: FV.serverTimestamp() }, { merge: true });
      const aside = (r, rev) => [...(Array.isArray(r.roseReseated) ? r.roseReseated : []).slice(-9), { at: now, by, fromStockId: stockId, toStockId: stockId, plan: !!r.rosePlanJson, kept: !!r.roseProtectedJson, reopen: true, merge: true, cut: { at: num(r.roseCutAt) || null, revision: rev, stockId, leftover: `${stockId}-${rev}`, how: "discarded" } }];
      const noCut = { roseCutAt: null, roseCutRevision: null, rosePlanJson: null, rosePlanHash: null, roseFingerprint: null, roseProtectedJson: null };
      const moving = going.map(c => { const x = Object.assign({}, c, { metal: "rose", pinned: null }); delete x.arrivalPin; return x; });
      const tPl = (t.placements || []).concat(fPl.map((p, i) => Object.assign({}, p, { n: (t.placements || []).length + i + 1 }))), carry = PoolPieces.carrySources(t, moving, [f]), took = moving.reduce((n, c) => n + (num(c.areaPt2) || 0), 0);
      const tNext = Object.assign({ charms: (t.charms || []).concat(moving), placements: tPl, poolIds: [...new Set((t.poolIds || []).map(String).concat(ids))], orders: [...new Set((Array.isArray(t.orders) ? t.orders : []).map(String).concat(orders))] }, carry || {},
        w.backsGoing.length ? { backPool: (t.backPool || []).filter(x => !(x && idSet.has(String(x.poolId)))).concat(backsTo(w.backsGoing, t)) } : {}, noCut,
        num(t.freePt2) > 0 && num(t.usablePt2) > 0 ? { freePt2: Math.max(0, Math.round(num(t.freePt2) - took)), density: Math.min(1, Math.max(0, 1 - Math.max(0, num(t.freePt2) - took) / num(t.usablePt2))) } : {},
        { charmCount: (t.charms || []).concat(moving).filter(c => c && !c.excluded).length, placedCount: tPl.length, nestOwed: null, dirty: false, movedOn: null, autoCutOff: false, roseStockId: stockId, roseRevision: num(stock.revision), roseReseated: aside(t, tr),
          roseReopened: { at: now, by, lines: 0, cut: true, cutAt: num(t.roseCutAt) || null }, roseRedraw: { at: now, by, line: true, cut: true }, layoutRev: num(t.layoutRev) + 1, contentRev: num(t.contentRev) + 1, filesOwed: now, merged: { from: f.id, fromName: fn, n: going.length, at: now, by }, updatedAt: FV.serverTimestamp() });
      if (bytesOf(Object.assign({}, t, tNext, { updatedAt: 0 })) > SHEET_DOC_BYTES) return words(413, `${tn} cannot hold these pieces in one record`);
      const history = r => (Array.isArray(r.flowHistory) ? r.flowHistory : []);
      tx.set(tRef, tNext, { merge: true });
      tx.set(fRef, Object.assign({ charms: [], placements: [], poolIds: [], orders: [], rejects: [], feedWait: null, backPool: [], charmCount: 0, placedCount: 0, rejectCount: 0, status: "idle", dirty: false, nestOwed: null, problem: null, problemStop: null, filesOwed: null, label: null,
        draft: true, setId: null, setSeq: null, sheetIndex: null, leftSet: null, autoMatched: null, autoCutWhy: null, roseStockId: null, roseRedraw: null, roseReopened: null, roseReseated: aside(f, fr), archived: true, archivedReason: `its pieces are on ${tn}`, current: false, mergedInto: say,
        flowHistory: history(f).concat([{ id: `merged-${now}-${f.id}`, at: now, by, type: "setLeave", note: `Its pieces moved onto ${tn}: one Rose Gold sheet in ${sw}`, setId, laserDoneAt: null }]).slice(-100), layoutRev: num(f.layoutRev) + 1, contentRev: num(f.contentRev) + 1, updatedAt: FV.serverTimestamp() }, noCut), { merge: true });
      // the set: without the emptied sheet, its copies now on the target (each copy keeps what it says: its ear, its back)
      const onT = c => (String(c.sheetId) === f.id ? Object.assign({}, c, { sheetId: t.id, sheet: t.fileBase || t.folder || tn }) : c), so = {};
      for (const [rid, o] of Object.entries(set.orders || {})) { const ls = o && o.lines, map = l => Object.assign({}, l, { copies: (l.copies || []).map(onT) }); so[rid] = Object.assign({}, o, { lines: Array.isArray(ls) ? ls.map(map) : Object.fromEntries(Object.entries(ls || {}).map(([k, l]) => [k, map(l)])) }); }
      tx.update(setRef, { setRev: num(set.setRev) + 1, sheetIds: (set.sheetIds || []).filter(i => String(i) !== f.id), orders: so, labelFiles: (set.labelFiles || []).filter(x => !(x && String(x.sheetId) === f.id)), labels: null, setEdit: { at: now, by },
        flowHistory: history(set).concat([{ id: `merge-${now}-${setId}`, at: now, by, type: "setEdit", out: [f.id], in: [], note: `${fn}'s pieces moved onto ${tn}: one Rose Gold sheet` }]).slice(-100), updatedAt: FV.serverTimestamp() });
      carryBacks(tx, bKeys, t, now);
      for (const id of ids) tx.set(col(POOL).doc(id), Object.assign({ sheetId: t.id, movedFrom: f.id, movedTo: t.id, movedBy: by, movedAt: now, movedHow: "merge", updatedAt: FV.serverTimestamp() }, t.fileBase ? { sheetName: t.fileBase } : {}), { merge: true });
      return Object.assign({ ok: true, moved: going.length, orders, kept: tOrders.filter(o => !orders.includes(o)), setId, layoutRev: tNext.layoutRev }, say);
    });
    if (out && out.ok && !out.dry && !out.noop) {
      revTouch(b, ["s:" + srcId, "s:" + toId, "t:" + out.setId].concat(out.lift.map(l => "s:" + l.sheetId))); feedOrders(b, [out.orders.concat(out.kept)]);
      if (d.stamp) { const ev = (o, text, type) => ({ orderId: String(o), type, at: out.at, by, station: "sorter", sheetId: toId, sheet: out.toName, setId: out.setId, text, data: { merged: true, from: srcId, fromName: out.fromName }, id: `rgmerge-${toId}-${out.at}` });
        try { await d.stamp(out.orders.map(o => ev(o, `Moved from ${out.fromName} to ${out.toName}: one Rose Gold sheet in ${out.set}`, "moved")).concat(out.kept.map(o => ev(o, `${out.toName}: ${out.moved} pieces of ${out.fromName} joined it, so its green line is drawn again`, "note"))), "rose merge"); } catch (_) { /* the timeline is best-effort, as everywhere */ } }
    }
    return out;
  }

  return { runCmdClaim, cmdWorkPut, cmdMovePieces, cmdHopPieces, cmdLeaveSet, cmdSwapLine, cmdRestoreNest, cmdRoseMerge };
};
module.exports.claimHeld = claimHeld;
module.exports.busyRec = busyRec;
module.exports.WORK = WORK;
module.exports.CLAIM_STALE_MS = CLAIM_STALE_MS;
module.exports.OPS = ["runCmdClaim", "cmdWorkPut", "cmdMovePieces", "cmdHopPieces", "cmdLeaveSet", "cmdSwapLine", "cmdRestoreNest", "cmdRoseMerge"];
