/*  netlify/functions/_charmNestCustomDone.js — THE CUSTOM PORTION "DONE" (AND ITS UNDO) FROM A VIEWER, DONE IN THE CLOUD AT ONCE (ITEM 10, Paul 21 Nov 2026).
 *
 *  The Charm Editor's Done card (charm-nest-custom-done.js complete / undo, CPX1) takes a custom piece of the order off its sheet and marks its rows coveredBy the saved piece. On a page
 *  that carries the run it does that itself: Orders.takeOffGone edits the sheet it holds (charms, placements, saved by putSheet), then poolUpdate { state: abandoned, coveredBy, removedBy… }.
 *  Once the cloud carries every run (Charm_Nest_Open/drive.mode "on") every page is a viewer: both writes are the run's work and the server refuses them ("The cloud carries every run now:
 *  poolUpdate…"), so the person's Done and Undo did nothing. A poolUpdate allowed alone would be half done: its take-off (poolTakeOff) leaves the piece in the sheet record's charms and
 *  placements, which the cloud nests and writes into the files, so the piece would still be cut. So the press is its own op, a person's (PRESS), done here in ONE transaction:
 *
 *    customDone { orderId, poolIds, cover: { poolId, lineKey, name, designId, version, at? }, by, dev }            (Done)
 *      · every piece named is a custom piece (its row custom: true) of that order; one already covered is left as it is (a retry writes nothing);
 *      · every live sheet that lists one of them (4 at most) must give it up now: one marked laser cut (its own mark, or its set's), or that is at work
 *        (nesting, finishing, saving, Manual Control, a nest job) refuses the WHOLE press with the reason, and nothing is written (the page's blockOf, said by the server);
 *      · each such sheet: the same edit as the cloud's own take-off (_charmNestTakeOffOps.js cloudTakeOff): takeOffUpdate's fields, the pieces out of its charms, placements, rejects,
 *        feedWait and pieceSides (every other pose byte for byte), every green line as saved, filesOwed now, layoutRev and contentRev + 1, a draft's label
 *        dropped / a set's label owed; the copies leave its set's order list; the pieces' engraving records leave the sheet;
 *      · the rows: { state: abandoned, sheetId: null, setId: null, removedBy, removedReason: "completed in the Charm Editor", removedAt, coveredBy { …cover, by, at } };
 *      · then, as a page's take-off: the order's timeline ("removed", poolEvents), the changes feed (the sheets, the order), the driver kicked (the sheet owes its files / its nest).
 *    customDone { undo: true, orderId, poolIds, by, dev }                                                               (Undo)
 *      · every piece named is a custom piece of that order, abandoned and covered (not undone): its rows live again { state: ready, sheetId: null, setId: null, repooledAt,
 *        coveredBy: { …as it was, undoneAt, undoneBy } } (the page's own Undo patch), in one transaction. A piece in any other state refuses the whole Undo, nothing written.
 *      · (G7, 21 Nov 2026) and the piece goes back where it was (customDoneFrom, kept by the Done): on its sheet at its place, in the same transaction, when the sheet can take it and the
 *        place is free; else the cloud places it as a run piece (cloudPlace on its rows, runCmdPut placewaiting). The answer says where, for each piece (back / placing / loose).
 *  Only in mode "on" and never in the sandbox: in every other mode a page does it itself, exactly as before (this op is then refused in words and the page never sends it).          */
"use strict";
const Placement = require("./_charmNestPlacement");
const RoseStockMod = require("./_charmNestRoseStock");
const Rose = require("../../charm-nest-rose.js");
const Faces = require("../../charm-nest-engrave-faces.js");
const { busyWhy, setOrdersWithout } = require("./_charmNestTakeOffOps");

const REASON = "completed in the Charm Editor";
const MAX_SHEETS = 4, MAX_IDS = 100;
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => (v == null ? "" : String(v)).slice(0, n || 200);
const CUSTOM_ORDER = /^CUSTOM-\d{8}-\d{1,6}$/;
const orderIdOf = v => { const s = String(v == null ? "" : v).trim(); return CUSTOM_ORDER.test(s) ? s : s.replace(/\D/g, "").slice(0, 30); };
const covered = p => !!(p && p.coveredBy && typeof p.coveredBy === "object" && num(p.coveredBy.at) > 0 && !num(p.coveredBy.undoneAt));
/** Why a live sheet cannot give up a piece now, in words ("" when it can): the page's blockOf, and the cloud's take-off busy rule. */
function blockWhy(s, sets, now, manualStaleMs) {
  if (!Placement.editable(s) || s.recalled || sets.some(x => num(x.laserDoneAt) > 0)) return "it is already cut on the laser";
  return busyWhy(Object.assign({ manualStaleMs }, s), now) ? "it is being nested or saved right now: try again in a moment" : "";
}

module.exports = function (d) {
  const { db, col, FV, txGetAll } = d, N = d.names;
  const refuse = (error, status) => ({ error, status: status || 409, refused: true });

  async function op_customDone(b) {
    if (d.prefix()) return refuse("The Custom piece is completed on this computer in the sandbox: nothing was changed", 400);
    if ((await d.driveMode()) !== "on") return refuse("The cloud does not carry the runs now: this computer completes the Custom piece itself. Nothing was changed");
    const rid = orderIdOf(b.orderId), by = str(b.by, 80).trim(), undo = b.undo === true;
    if (!rid) return refuse("which order is needed", 400);
    if (!by) return refuse("nobody is signed in: sign in, then press it again", 400);
    const ids = [...new Set((Array.isArray(b.poolIds) ? b.poolIds : []).map(String))].filter(id => d.isPoolId(id) && id.startsWith(rid + "_")).slice(0, MAX_IDS);
    if (!ids.length) return refuse(`no piece of order ${rid} was named`, 400);
    return undo ? undoOf(b, rid, ids, by) : doneOf(b, rid, ids, by);
  }

  async function doneOf(b, rid, ids, by) {
    const now = Date.now(), c0 = b.cover && typeof b.cover === "object" ? b.cover : {};
    const at = Math.abs(num(c0.at) - now) < 10 * 60000 ? Math.round(num(c0.at)) : now;
    const cover = { poolId: str(c0.poolId, 80), lineKey: str(c0.lineKey, 80), name: str(c0.name || "the saved piece", 80), designId: c0.designId ? str(c0.designId, 120) : null, version: num(c0.version) || null, by, at };
    const patch = { state: "abandoned", sheetId: null, setId: null, removedBy: by, removedReason: REASON, removedAt: at, coveredBy: cover };
    const res = await db.runTransaction(async tx => {
      // every read first: a transaction writes after it has read
      const rows = await txGetAll(tx, ids.map(id => col(N.POOL).doc(id)));
      for (const r of rows) {
        const p = r.exists ? r.data() || {} : null;
        if (!p) return refuse(`the piece ${r.id} is not in the cloud: nothing was changed`, 404);
        if (p.custom !== true) return refuse(`${r.id} is not a custom piece: only a custom piece is completed here. Nothing was changed`, 400);
      }
      const want = new Set(rows.filter(r => !(covered(r.data()) && Placement.TAKE_OFF_STATES.has(r.data().state))).map(r => r.id));
      if (!want.size) return { ok: true, already: true, sheets: [], removed: [] };
      const list = [...want], sheets = new Map();
      for (let j = 0; j < list.length; j += 30) for (const x of (await tx.get(col(N.SHEETS).where("poolIds", "array-contains-any", list.slice(j, j + 30)))).docs) { const s = x.data() || {}; if (!s.archived) sheets.set(x.id, Object.assign({}, s, { id: x.id })); }
      if (sheets.size > MAX_SHEETS) return refuse(`the Custom piece is on ${sheets.size} sheets: take it off them in the sheet windows. Nothing was changed`);
      const edits = [];
      for (const s of sheets.values()) {
        const label = Placement.labelOf(s) || "its sheet", take = new Set((s.poolIds || []).map(String).filter(id => want.has(id)));
        if (!take.size) continue;
        const sets = (await tx.get(col(N.SETS).where("sheetIds", "array-contains", s.id))).docs.map(x => x.data() || {});
        const why = blockWhy(s, sets, now, d.MANUAL_STALE_MS);
        if (why) return refuse(`${label}: ${why}. Nothing was changed`);
        const t = Placement.takeOffUpdate(s, take, FV.serverTimestamp()); if (!t) continue;
        const charms = (s.charms || []).filter(c => !(c && take.has(String(c.poolId)))), keepIds = new Set(charms.map(c => c && String(c.id)));
        const upd = Object.assign({}, t.update, { charms, placements: (s.placements || []).filter(p => p && keepIds.has(String(p.id))), filesOwed: now, layoutRev: num(s.layoutRev) + 1, contentRev: num(s.contentRev) + 1, takenOffAt: now });
        if (Array.isArray(s.rejects)) upd.rejects = s.rejects.filter(x => keepIds.has(String(x)));
        if (Array.isArray(s.feedWait)) { const keepPool = new Set(charms.map(c => c && String(c.poolId))); upd.feedWait = s.feedWait.filter(x => keepIds.has(String(x)) || keepPool.has(String(x))); }
        if (s.pieceSides && typeof s.pieceSides === "object") { const ps = Object.assign({}, s.pieceSides); for (const id of take) delete ps[id]; upd.pieceSides = ps; }
        Object.assign(upd, RoseStockMod.lineKeeps(s, new Set((s.charms || []).filter(c => c && take.has(String(c.poolId))).map(c => String(c.id)))));   // (THELINE)
        if (s.label) { if (s.draft && !s.label.own) upd.label = null; upd.labelOwed = now; }
        let setRef = null, setOrders = null, setWas = null, setRevNow = 0;
        if (s.setId && !s.draft && d.isId(s.setId)) { setRef = col(N.SETS).doc(String(s.setId)); const ss = await tx.get(setRef), sx = ss.exists ? ss.data() || {} : null; if (sx) { setOrders = setOrdersWithout(sx.orders, take); setRevNow = num(sx.setRev); if (setOrders) setWas = sx.orders; } }
        const backRefs = []; for (const id of t.removed) for (const face of ["back", "front"]) backRefs.push(col(N.BACK).doc(Faces.recKey(id, face)));
        const backRows = backRefs.length ? await txGetAll(tx, backRefs, ["sheetId"]) : [];
        edits.push({ s, label, upd, setRef, setOrders, backRows, removed: t.removed, setWas, setRevNow });
      }
      /* G7 (Paul 21 Nov 2026): where each piece was, kept on its row (customDoneFrom), so an Undo can put it back exactly there: the sheet and its revision after the Done, the piece's
         charm(s) and placement(s) as they stood, its pair side, engraving back, the rejects / feed-turn entries and the set's order entries the Done took out. */
      const fromOf = new Map();
      for (const e of edits) for (const id of e.removed) {
        const s = e.s, mine = (s.charms || []).filter(c => c && String(c.poolId) === String(id)), cids = new Set(mine.map(c => String(c.id)));
        const setKeys = e.setWas && e.setOrders ? Object.keys(e.setWas).filter(k => JSON.stringify(e.setWas[k]) !== JSON.stringify(e.setOrders[k])) : [];
        fromOf.set(String(id), { sheetId: s.id, label: e.label, metal: s.metal || null, runId: s.runId || null, layoutRev: e.upd.layoutRev, contentRev: e.upd.contentRev, setId: s.setId || null, draft: !!s.draft,
          state: ((rows.find(r => r.id === String(id)) || { data: () => ({}) }).data() || {}).state || "nested", sheetName: s.fileBase || s.folder || null,
          charms: mine.map(c => Object.assign({}, c)), placements: (s.placements || []).filter(p => p && cids.has(String(p.id))).map(p => Object.assign({}, p)),
          side: s.pieceSides && s.pieceSides[id] !== undefined ? s.pieceSides[id] : null, backPool: (s.backPool || []).filter(x => x && String(x.poolId) === String(id)),
          rejects: (s.rejects || []).filter(x => cids.has(String(x))), feedWait: (s.feedWait || []).filter(x => cids.has(String(x)) || String(x) === String(id)),
          backRecs: e.backRows.filter(r => r.exists && (r.data() || {}).sheetId === s.id && String((Faces.parseRecKey(r.id) || {}).poolId) === String(id)).map(r => r.id),
          setOrders: setKeys.length ? Object.fromEntries(setKeys.map(k => [k, e.setWas[k]])) : null, at: now });
      }
      // writes
      const ts = FV.serverTimestamp(), before = new Map(), onSheet = new Map();
      for (const e of edits) for (const id of e.removed) onSheet.set(String(id), e.s);
      for (const r of rows) { const prev = r.data() || {}, s = onSheet.get(r.id); before.set(r.id, s && !prev.sheetId ? Object.assign({}, prev, { sheetId: s.id, sheetName: s.fileBase || s.folder || null, setId: prev.setId || s.setId || null, material: prev.material || s.metal || null }) : prev); }
      for (const id of want) tx.set(col(N.POOL).doc(id), Object.assign({}, patch, { updatedAt: ts, [Placement.REPOOLED]: null, customDoneFrom: fromOf.get(id) || null }), { merge: true });
      for (const e of edits) {
        tx.update(col(N.SHEETS).doc(e.s.id), e.upd);
        if (e.setRef && e.setOrders) tx.update(e.setRef, { orders: e.setOrders, setRev: num(e.setRevNow) + 1, updatedAt: ts });   // (FRESHWRITE: the set's revision moves with its order list)
        for (const r of e.backRows) if (r.exists && (r.data() || {}).sheetId === e.s.id) tx.set(col(N.BACK).doc(r.id), { sheetId: null, updatedAt: ts }, { merge: true });
      }
      return { ok: true, removed: list, sheets: edits.map(e => ({ id: e.s.id, label: e.label, pieces: e.removed.length, layoutRev: e.upd.layoutRev })), before };
    });
    if (!res || res.error) return res;
    const before = res.before; delete res.before;
    if (res.removed.length) {
      d.revTouch(b, res.sheets.map(x => "s:" + x.id)); d.feedOrders(b, [[rid], res.removed]);
      await d.stamp(() => d.poolEvents(res.removed, patch, before, b), "custom done in the Charm Editor");
    }
    return Object.assign(res, { orderId: rid, cover });
  }

  /* G7 (Paul 21 Nov 2026: the Undo made the rows ready again but put the piece on no sheet: "live again: send it to a sheet from its order"). The Undo now puts the piece back where it
     was (customDoneFrom, kept by the Done): on the same sheet at the same place, when that sheet can take it (still in the Library, not cut, not sent, not approved, not at work, its
     green lines not drawn again without it, its set's order list as the Done left it) and the place is still free; in the same transaction as its rows. Otherwise the cloud places it as
     a run piece: its rows say so (cloudPlace, the open run, its size and design file from the charm it was) and the run is asked at once (runCmdPut placewaiting: the placement unit's
     own rules, lane "cmd:placewaiting" and "nest"). Each piece's answer says where, in words; a piece the cloud cannot place is live again on no sheet, and the answer says why. */
  /** Whether a piece's old place on a sheet is free: nothing moved on the sheet since the Done, or no placed piece's box (turned, with a point of room) reaches into it. */
  function placeFree(s, from) {
    const have = new Set((s.charms || []).filter(Boolean).map(c => String(c.id)));
    if ((from.charms || []).some(c => c && have.has(String(c.id)))) return false;
    if (num(s.layoutRev) === num(from.layoutRev)) return true;
    const size = new Map((s.charms || []).concat(from.charms || []).filter(Boolean).map(c => [String(c.id), c]));
    const box = p => { const c = size.get(String(p.id)) || {}, k = +p.scale > 0 ? +p.scale : 1, w = num(p.wPt) > 0 ? num(p.wPt) : num(c.widthPt) * k, h = num(p.hPt) > 0 ? num(p.hPt) : num(c.heightPt) * k, a = (+p.angle || 0) * Math.PI / 180;
      return { x: +p.cxPt, y: +p.cyPt, w: Math.abs(w * Math.cos(a)) + Math.abs(h * Math.sin(a)), h: Math.abs(w * Math.sin(a)) + Math.abs(h * Math.cos(a)) }; };
    const mine = (from.placements || []).filter(Boolean).map(box);
    if (mine.some(m => !(m.w > 0 && m.h > 0) || !Number.isFinite(m.x) || !Number.isFinite(m.y))) return false;
    return !(s.placements || []).filter(Boolean).map(box).some(o => mine.some(m => Math.abs(m.x - o.x) < (m.w + o.w) / 2 + 1 && Math.abs(m.y - o.y) < (m.h + o.h) / 2 + 1));
  }
  /** Why a piece cannot go back on its sheet now, in words ("" when it can). */
  function backWhy(s, sets, from, setNow, now) {
    if (!s || s.archived) return "its sheet is no longer in the Library";
    const why = blockWhy(s, sets, now, d.MANUAL_STALE_MS); if (why) return `${from.label || "its sheet"}: ${why}`;
    if (Rose.cuts(s.metal) && (s.roseProtectedJson || s.rosePlanJson)) return `${from.label || "its sheet"}: its green line is drawn`;
    if (from.setOrders) {
      if (!setNow || d.setEdit().committedSet(setNow)) return `${from.label || "its sheet"}: its set was sent or changed`;
      const ids = new Set((from.charms || []).map(c => String(c.poolId))), cur = setNow.orders || {};
      for (const k of Object.keys(from.setOrders)) { const was = setOrdersWithout({ [k]: from.setOrders[k] }, ids), want = was ? was[k] : from.setOrders[k]; if (JSON.stringify(want) !== JSON.stringify(cur[k])) return `${from.label || "its sheet"}: its set's order list changed since`; }
    }
    if (!placeFree(s, from)) return `${from.label || "its sheet"}: its place there is taken now`;
    return "";
  }
  const sizeOf = (row, from) => { const c = ((from && from.charms) || [])[0] || {}; return +row.areaPt2 > 0 && +row.widthPt > 0 && +row.heightPt > 0 ? {} : +c.areaPt2 > 0 && +c.widthPt > 0 && +c.heightPt > 0 ? { widthPt: +c.widthPt, heightPt: +c.heightPt, areaPt2: +c.areaPt2 } : null; };
  const fileOf = (row, from) => { if (row.aiPath) return {}; const c = ((from && from.charms) || [])[0] || {}, p = (c.cloud && c.cloud.aiPath) || (c.source && c.source.path) || null; return p ? { aiPath: str(p, 400) } : null; };

  async function undoOf(b, rid, ids, by) {
    const back = Date.now(), drive = typeof d.drive === "function" ? await d.drive().catch(() => null) : null, steps = (drive && Array.isArray(drive.cloudSteps) ? drive.cloudSteps : []).map(String);
    const laneOn = steps.includes("nest") && steps.includes("cmd:placewaiting");
    const res = await db.runTransaction(async tx => {
      const rows = await txGetAll(tx, ids.map(id => col(N.POOL).doc(id))), write = [];
      for (const r of rows) {
        const p = r.exists ? r.data() || {} : null;
        if (!p) return refuse(`the piece ${r.id} is not in the cloud: nothing was changed`, 404);
        if (p.custom !== true) return refuse(`${r.id} is not a custom piece: nothing was changed`, 400);
        if (p.coveredBy && num(p.coveredBy.undoneAt) > 0 && !Placement.TAKE_OFF_STATES.has(p.state)) continue;   // (undone already: a retry writes nothing)
        if (!covered(p) || p.state !== "abandoned") return refuse(`the Custom piece is not completed in the Charm Editor any more (${p.state || "no state"}): nothing was changed`);
        const from = p.customDoneFrom && typeof p.customDoneFrom === "object" && d.isId(p.customDoneFrom.sheetId) ? p.customDoneFrom : null;
        write.push({ id: r.id, row: p, from, coveredBy: Object.assign({}, p.coveredBy, { undoneAt: back, undoneBy: by }) });
      }
      // (every read first: the sheets the pieces were on, their sets, the engraving records they left, the runs a piece the cloud places could join)
      const bySheet = new Map(); for (const w of write) if (w.from) { if (!bySheet.has(w.from.sheetId)) bySheet.set(w.from.sheetId, []); bySheet.get(w.from.sheetId).push(w); }
      const plans = [], why = new Map();
      for (const [sid, list] of [...bySheet].slice(0, MAX_SHEETS)) {
        const ss = await tx.get(col(N.SHEETS).doc(sid)), s = ss.exists ? Object.assign({}, ss.data() || {}, { id: sid }) : null;
        const sets = s ? (await tx.get(col(N.SETS).where("sheetIds", "array-contains", sid))).docs.map(x => Object.assign({ id: x.id }, x.data() || {})) : [];
        const setNow = s && s.setId ? sets.find(x => x.id === String(s.setId)) || null : null;
        const merged = { label: list[0].from.label, layoutRev: list[0].from.layoutRev, setOrders: list.reduce((o, w) => (w.from.setOrders ? Object.assign(o || {}, w.from.setOrders) : o), null),
          charms: list.flatMap(w => w.from.charms || []), placements: list.flatMap(w => w.from.placements || []) };
        const no = backWhy(s, sets, merged, setNow, back);
        if (no || list.some(w => (s.poolIds || []).map(String).includes(w.id))) { for (const w of list) why.set(w.id, no || `${merged.label || "its sheet"} lists it already`); continue; }
        const backRefs = list.flatMap(w => (w.from.backRecs || []).map(k => col(N.BACK).doc(String(k))));
        plans.push({ s, list, merged, setNow, backRows: backRefs.length ? await txGetAll(tx, backRefs, ["sheetId"]) : [] });
      }
      for (const [, list] of [...bySheet].slice(MAX_SHEETS)) for (const w of list) why.set(w.id, "it was on too many sheets to put back at once");
      const onSheet = new Set(plans.flatMap(x => x.list.map(w => w.id))), loose = write.filter(w => !onSheet.has(w.id));
      const runIds = [...new Set(loose.flatMap(w => [w.row.runId, w.from && w.from.runId, b.runId]).filter(x => d.isId(x)).map(String))].slice(0, 10);
      const runs = new Map(); if (laneOn && runIds.length) for (const r of await txGetAll(tx, runIds.map(id => col(N.RUNS).doc(id)), ["status"])) runs.set(r.id, r.exists && !["complete", "abandoned", "archived"].includes(String((r.data() || {}).status || "")));
      // writes
      const ts = FV.serverTimestamp(), out = { back: [], placing: [], loose: [] };
      for (const x of plans) {
        const s = x.s, m = x.merged, n = x.list.length, ids2 = x.list.map(w => w.id), upd = { updatedAt: ts };
        upd.charms = (s.charms || []).concat(m.charms.map(c => Object.assign({}, c, { metal: s.metal })));
        upd.placements = (s.placements || []).concat(m.placements);
        upd.poolIds = (s.poolIds || []).map(String).concat(ids2);
        if (Array.isArray(s.orders) || ids2.length) upd.orders = [...new Set((Array.isArray(s.orders) ? s.orders : []).map(String).concat([rid]))];
        const bp = x.list.flatMap(w => w.from.backPool || []); if (bp.length || Array.isArray(s.backPool)) upd.backPool = (s.backPool || []).concat(bp);
        const rj = x.list.flatMap(w => w.from.rejects || []); if (rj.length) upd.rejects = (s.rejects || []).concat(rj);
        const fw = x.list.flatMap(w => w.from.feedWait || []); if (fw.length) upd.feedWait = (s.feedWait || []).concat(fw);
        const sides = x.list.filter(w => w.from.side != null); if (sides.length) { const ps = Object.assign({}, s.pieceSides || {}); for (const w of sides) ps[w.id] = w.from.side; upd.pieceSides = ps; }
        if (num(s.placedCount) > 0 || m.placements.length) upd.placedCount = num(s.placedCount) + m.placements.length;
        if (num(s.charmCount) > 0) upd.charmCount = num(s.charmCount) + m.charms.length;
        if (!m.placements.length) upd.nestOwed = { at: back, n, by: "cloud", why: "an Undo of Done put it back" };
        Object.assign(upd, { dirty: true, filesOwed: back, layoutRev: num(s.layoutRev) + 1, contentRev: num(s.contentRev) + 1 });
        if (s.label) upd.labelOwed = back;
        tx.update(col(N.SHEETS).doc(s.id), upd);
        if (m.setOrders && x.setNow) tx.update(col(N.SETS).doc(String(x.setNow.id)), { orders: Object.assign({}, x.setNow.orders || {}, m.setOrders), setRev: num(x.setNow.setRev) + 1, updatedAt: ts });   // (FRESHWRITE: the set's revision moves with its order list)
        for (const r of x.backRows) if (r.exists && !(r.data() || {}).sheetId) tx.set(col(N.BACK).doc(r.id), { sheetId: s.id, updatedAt: ts }, { merge: true });
        for (const w of x.list) tx.set(col(N.POOL).doc(w.id), { state: w.from.state || "nested", sheetId: s.id, sheetName: w.from.sheetName || s.fileBase || null, setId: s.setId || null, [Placement.REPOOLED]: back, coveredBy: w.coveredBy, customDoneFrom: null, updatedAt: ts }, { merge: true });
        out.back.push({ ids: ids2, sheetId: s.id, label: m.label || Placement.labelOf(s) || "its sheet", placed: m.placements.length > 0, layoutRev: upd.layoutRev });
      }
      for (const w of loose) {
        const run = [w.row.runId, w.from && w.from.runId, b.runId].map(x => (x ? String(x) : "")).find(x => runs.get(x)), size = sizeOf(w.row, w.from), file = fileOf(w.row, w.from), metal = w.row.material || w.row.metal || (w.from && w.from.metal) || null;
        const was = why.get(w.id) || "", cant = !laneOn ? "the cloud's placement is not on (the owner turns on “cmd:placewaiting” and “nest” in Settings, Cloud carries)" : !run ? "no run is open in the cloud" : !size ? "the cloud does not know its size" : !file ? "its design file is not in the cloud" : !metal ? "its metal is not known" : "";
        const base = { state: "ready", sheetId: null, setId: null, [Placement.REPOOLED]: back, coveredBy: w.coveredBy, customDoneFrom: null, updatedAt: ts };
        if (cant) { tx.set(col(N.POOL).doc(w.id), base, { merge: true }); out.loose.push({ id: w.id, why: [was, cant].filter(Boolean).join("; ") }); continue; }
        tx.set(col(N.POOL).doc(w.id), Object.assign(base, { cloudPlace: true, runId: run, material: metal }, size, file), { merge: true });
        out.placing.push({ id: w.id, runId: run, was });
      }
      return Object.assign({ ok: true, undone: write.map(w => w.id), ...(write.length ? {} : { already: true }) }, out);
    });
    if (!res || !res.ok) return res;
    if (res.undone.length) { d.feedOrders(b, [[rid], res.undone]); if (res.back.length) d.revTouch(b, res.back.map(x => "s:" + x.sheetId)); }
    // the cloud asked to place the pieces that could not go back where they were (one request per run): its answer says the sheet they went on
    const byRun = new Map(); for (const p of res.placing || []) { if (!byRun.has(p.runId)) byRun.set(p.runId, []); byRun.get(p.runId).push(p); }
    const placing = [];
    for (const [runId, list] of byRun) {
      let put = null; try { put = await d.cmdPut({ runId, dev: str(b.dev, 60) || "customDone", by, cmd: { type: "placewaiting", ids: list.map(p => p.id), label: str(`Place the custom piece ${list[0].id} again (Undo of Done in the Charm Editor)`, 150) } }); } catch (e) { put = { error: (e && e.message) || String(e) }; }
      if (put && put.ok && put.id) d.revTouch(b, ["r:" + runId]);
      if (put && put.ok && put.id) placing.push({ ids: list.map(p => p.id), runId, cmdId: put.id, was: list[0].was || "" });
      else for (const p of list) res.loose.push({ id: p.id, why: `the cloud was not asked to place it (${str((put && put.error) || "no answer", 160)})` });
    }
    res.placing = placing;
    res.said = [].concat(res.back.map(x => `back on ${x.label}${x.placed ? ", where it was" : " (the cloud nests it there)"}`),
      placing.map(x => `${x.was ? x.was + ": " : ""}the cloud places it on a sheet now`), res.loose.map(x => `live again on no sheet (${x.why}): send it to a sheet from its order`));
    return Object.assign(res, { orderId: rid, at: back });
  }

  return { ops: { customDone: op_customDone }, REASON };
};
module.exports.REASON = REASON;
module.exports.blockWhy = blockWhy;
