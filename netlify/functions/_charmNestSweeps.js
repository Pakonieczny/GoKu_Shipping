/*  netlify/functions/_charmNestSweeps.js — THE REPAIR SWEEPS, IN THE CLOUD (cloud plan v2 §1.6 item 9, build step 13; lane "sweeps"). The looks the carrying page made by itself,
 *  each on its own clock (kept in Charm_Nest_Drive/state.sweeps), each a write through the cloud's own fenced call, never Etsy:
 *    stranded   every 2 min (STRANDED, CONTENDEDBACK; the C4 rule: a piece counts as placed only when its sheet's record seats it, in its placements or feedWait, or that
 *               sheet is nesting or owes a nest): a piece record naming a sheet that does not list it lets that sheet go, a sheet that lists a piece it never seated tries it
 *               again, a line refused because another run claimed it is asked for again; the cloud's placement then places them. The same line is looked at once in 30 min.
 *    pictures   every 5 min, 3 sheets at most (PICTUREKEEP): a sheet whose saved picture is behind its layout owes its files (the cloud's sheet writer draws them).
 *    names      every 5 min (NAMECLASH): two sheets of a set with one name or folder: the one named from a day that is not its set's is named again, and owes its files.
 *    sentrel    every 5 min (SENTISRELEASED): an order a person resolved by Send to Sheet carries the ORDER RELEASED seal of that send on its lines (releaseHold).
 *    gather     every minute (ONEFILL, COMBINEDRAFTS): underfilled Gold Filled / Sterling Silver drafts combined by the combine's own rules and move (_charmNestCombine.js:
 *               all the pieces of a newer draft onto the oldest draft that takes them whole, orders kept whole, pairs kept, no person's pin; the emptied draft archived and
 *               the later drafts numbered again; a fresh nest of the target). Never a second combine in a pass: the combine pass item (order 55) and this sweep share one guard.
 *    upkeep     every 30 min (Upkeep): the orders the run finished more than a day ago, archived as they are now, every sheet they are on a day past its set's commit, leave the
 *               run's lines (runLinesRetire, build step 10a), and its committed and holds lists. Nothing is deleted: the archive and the Library keep them.
 *  DORMANT: runs only while the cloud carries the run and "sweeps" is in drive.cloudSteps; each sweep also needs the lane its writes belong to ("nest", "files").          */
"use strict";
const O = require("../../charm-nest-orders.js");
const Exec = require("./_charmNestCmdExec");
const { busyRec } = require("./_charmNestCmdOps");

const EVERY = { stranded: 2 * 60000, pictures: 5 * 60000, names: 5 * 60000, sentrel: 5 * 60000, gather: 60000, upkeep: 30 * 60000 };
const GRACE = 24 * 3600000, TRIED_MS = 30 * 60000, CLOSED_SEEN_MAX = 3000;
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n) => String(v == null ? "" : v).slice(0, n || 200);
const ms = v => (v && v.toMillis ? v.toMillis() : +v || 0);
const ridOf = k => (/^(\d{1,30})_/.exec(String(k || "")) || [])[1] || "";
const err = r => (r && r.status >= 400 ? str(r.error || "refused", 200) : "");
/** A map kept in Charm_Nest_Drive/state as a list of { k, at } (Firestore holds no list of lists; a map saved with merge never loses a key), newest kept. */
const pairsIn = list => { const o = {}; for (const e of Array.isArray(list) ? list : []) if (e && e.k) o[String(e.k)] = num(e.at); return o; };
const pairsOut = (o, max) => Object.entries(o).sort((a, b) => b[1] - a[1]).slice(0, max).map(([k, at]) => ({ k, at }));

async function sweepsUnit(ctx) {
  const now = Date.now(), st = (ctx.state && ctx.state.sweeps) || {}, next = Object.assign({}, st), said = [];
  const x = { ctx, cmd: { id: "sweep" }, work: {} }, put = (body, k) => ctx.call(body, `${ctx.runId}|sweep|${k}`);
  const due = k => now - num(st[k + "At"]) >= EVERY[k];
  const run = ctx.run || {};
  if (["complete", "abandoned", "archived"].includes(String(run.status || ""))) return undefined;
  const guard = async (k, f) => { if (Date.now() > ctx.budgetEnd) return; next[k + "At"] = now; try { const t = await f(); if (t) said.push(`${k}: ${t}`); } catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; said.push(`${k}: ${str((e && e.message) || e, 160)}`); } };

  // ── stranded (C4)
  if (due("stranded") && Exec.lane(ctx, "nest")) await guard("stranded", async () => {
    const tried = pairsIn(st.tried);   // (kept as a list of { k, at }: the state is saved with merge, which never drops a key of a map)
    for (const [k, t] of Object.entries(tried)) if (now - num(t) > 2 * 3600000) delete tried[k];
    const fix = await Exec.strandedFix(x, { tried });
    for (const k of fix.keys) tried[k] = now;
    next.tried = pairsOut(tried, 300);
    return fix.keys.length || fix.contended.length ? `${fix.keys.length} order line(s) (${fix.pieces} piece(s)) stood on no sheet and go to the placement; ${fix.contended.length} asked for again` : "";
  });

  // ── pictures (PICTUREKEEP)
  if (due("pictures") && Exec.lane(ctx, "files")) await guard("pictures", async () => {
    const sheets = await Exec.runSheets(ctx), stale = sheets.filter(r => r.outputs && r.outputs.preview && !Exec.cutRec(r) && !busyRec(r, now) && !r.filesOwed && !(r.manualEdit && now - num(r.manualEdit.at) < 90000)
      && (r.previewRev != null ? num(r.layoutRev) > num(r.previewRev) : ms(r.updatedAt) - num(r.previewAt) > 180000));
    let n = 0; for (const r of stale.slice(0, 3)) { const a = await put({ op: "putSheet", sheet: { id: r.id, filesOwed: now } }, `picture|${r.id}|${num(r.layoutRev)}`); if (!err(a)) n++; }
    return n ? `${n} sheet picture(s) behind their layout are drawn again` : "";
  });

  // ── names (NAMECLASH)
  if (due("names") && Exec.lane(ctx, "files")) await guard("names", async () => {
    const sheets = await Exec.runSheets(ctx), sets = await Exec.runSets(ctx), pages = sheets.filter(p => p.setId && !p.draft && p.fileBase), same = new Map();
    for (const p of pages) for (const k of [p.fileBase, p.folder].filter(Boolean)) (same.get(k) || same.set(k, new Set()).get(k)).add(p);
    const clash = [...new Set([...same.values()].filter(l => l.size > 1).flatMap(l => [...l]))];
    let n = 0;
    for (const sh of clash) {
      const set = sets.find(s => s.setId === sh.setId); if (!set || !set.day || !set.seq) continue;
      const want = O.sheetName(sh.metal, set.day, set.seq, num(sh.sheetIndex) || 1);
      if (!want || want === sh.fileBase || sheets.some(p => p !== sh && p.fileBase === want)) continue;
      if (Exec.cutRec(sh) || busyRec(sh, now) || (sh.manualEdit && now - num(sh.manualEdit.at) < 90000)) continue;
      const a = await put({ op: "putSheet", sheet: { id: sh.id, setDay: set.day, fileBase: want, folder: want, filesOwed: now } }, `name|${sh.id}|${want}`); if (err(a)) continue;
      n++;
      // the set's record lists each piece with its sheet's name: told the new one
      let moved = 0; const orders = JSON.parse(JSON.stringify(set.orders || {}));
      for (const o of Object.values(orders)) for (const l of (Array.isArray(o && o.lines) ? o.lines : Object.values((o && o.lines) || {}))) for (const c of (l && l.copies) || []) if (c && c.sheetId === sh.id && c.sheet !== want) { c.sheet = want; moved++; }
      if (moved) await put({ op: "setUpdate", setId: set.setId, patch: { orders } }, `name|set|${set.setId}|${sh.id}|${want}`);
    }
    return n ? `${n} sheet(s) that shared another sheet's name are written again under their own` : "";
  });

  // ── sentrel (SENTISRELEASED)
  if (due("sentrel")) await guard("sentrel", async () => {
    const L = (ctx.run && ctx.run.lines) || {}, keys = Object.keys(L).filter(k => L[k] && !["gone", "committed"].includes(L[k].state)).slice(0, 600); if (!keys.length) return "";
    const got = await ctx.read({ op: "customSheetGet", lineKeys: keys }).catch(() => null), recs = Object.values((got && got.records) || {});
    const patch = {};
    for (const e of recs) {
      if (!e || e.legacy || !e.sent || !(num(e.sent.at) > 0) || !e.sent.by || e.sendCloudPending) continue;
      const rid = String(e.rid || ""); if (!rid) continue;
      for (const k of keys.filter(k => ridOf(k) === rid)) { const rh = L[k].releaseHold; if (rh && num(rh.at) >= num(e.sent.at)) continue; patch[k] = { releaseHold: { how: "releaseHold", at: num(e.sent.at), by: str(e.sent.by, 80), id: `releaseHold:${rid}:${num(e.sent.at)}` } }; }
    }
    if (!Object.keys(patch).length) return "";
    const a = await put({ op: "runLinesPatch", runId: ctx.runId, lines: patch }, `sentrel|${Object.keys(patch).sort().join(",").slice(0, 400)}`);
    return err(a) ? "" : `${Object.keys(patch).length} line(s) sent to their sheet carry their ORDER RELEASED seal`;
  });

  // ── gather (ONEFILL, PAIRFIX, COMBINEDRAFTS): the combine's own unit (one rule, one move); it does nothing when the combine pass item already combined in this pass.
  //    (It checked Place.pairFits, which charm-nest-place.js never exported at its top: it never ran.)
  // NOAUTOCOMBINE (Paul, 21 Nov 2026: "Nothing can engage this automatically … The system is not allowed to have access to this option"): the gather sweep no longer combines drafts

  // ── upkeep (Upkeep: a day past done)
  if (due("upkeep") && OpClassHas("runLinesRetire")) await guard("upkeep", async () => {
    const L = (ctx.run && ctx.run.lines) || {}, closed = O.closedOrders(L), seen = pairsIn(st.closedSeen), archived = run.archivedLines || {};
    for (const rid of Object.keys(seen)) if (!closed.has(rid)) delete seen[rid];
    for (const rid of closed.keys()) if (!seen[rid]) seen[rid] = now;
    const sets = await Exec.runSets(ctx), sheets = await Exec.runSheets(ctx), on = Exec.listedBy(sheets);
    const spentSheet = s => { const set = sets.find(t => (t.sheetIds || []).includes(s.id)); return !!(set && num(set.committedAt) > 0 && now - num(set.committedAt) > GRACE) && !busyRec(s, now) && !s.filesOwed; };
    const keys = [], gone = [];
    for (const [rid, o] of closed) {
      const list = Array.isArray(o) ? o : (o && o.keys) || []; if (!list.length || now - num(seen[rid]) <= GRACE) continue;
      if (!list.every(k => archived[k] && archived[k] === O.textHash(JSON.stringify(L[k])))) continue;   // (archived as it is now: archiveFinished, the checkpoint)
      const where = list.flatMap(k => (L[k].poolIds || []).map(id => on.get(String(id))).filter(Boolean));
      if (where.some(s => !spentSheet(s))) continue;
      keys.push(...list); gone.push(rid);
      if (keys.length >= 280) break;
    }
    next.closedSeen = pairsOut(seen, CLOSED_SEEN_MAX);
    if (!keys.length) return "";
    const a = await put({ op: "runLinesRetire", runId: ctx.runId, keys }, `upkeep|${keys.slice().sort().join(",").slice(0, 400)}`);
    if (err(a)) return `the finished orders were not put away: ${err(a)}`;
    const out = new Set(gone), patch = { runId: ctx.runId };
    if (Array.isArray(run.committed)) patch.committed = run.committed.filter(id => !out.has(String(id)));
    if (run.holds && typeof run.holds === "object") { patch.holds = Object.fromEntries(Object.entries(run.holds).filter(([id]) => !out.has(String(id)))); }
    if (patch.committed || patch.holds) await put({ op: "runPut", run: patch, merge: true }, `upkeep|lists|${gone.slice().sort().join(",").slice(0, 400)}`);
    return `put away ${gone.length} order(s) committed over a day ago · the Library keeps them`;
  });

  next.at = now;
  if (said.length && ctx.say) ctx.say("[drive] sweeps: " + said.join(" · "));
  return { state: { sweeps: next } };
}
function OpClassHas(op) { try { return require("./_charmNestOpClass").isCloudOp(op); } catch (_) { return false; } }

module.exports = { units: { passItems: [{ name: "sweeps", lane: "sweeps", order: 90, unit: sweepsUnit }] }, sweepsUnit, EVERY };
