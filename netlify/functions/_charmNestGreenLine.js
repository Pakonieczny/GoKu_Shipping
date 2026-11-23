/*  netlify/functions/_charmNestGreenLine.js — ROSE GOLD, 10K AND 14K: THE GREEN LINE AND ITS CUT, IN THE CLOUD (cloud plan v2 build step 10b; hard-parts §4).
 *  The page's RoseStock.plan / RoseStock.record (charm-nest-rose-ui.js 228-300) and Gate.autoCut with RGONE and RGCLEAR (charm-nest-bridge.js 6464-6680), carried out from the records:
 *    Green.cut(p, set, o)   the Cut Sheet press: the sheet's own yes and a look at the sets when it is in none (the page's Include then assemble; Gate.rejoin for a sheet of a
 *                           committed set that lost its place), its physical sheet (roseClaim: 10K / 14K claim late, fresh), its line (CharmNestRose.shapes from the designs,
 *                           _charmNestRoseShapes.js, then rosePlan with cut:true), the cut recorded (roseRecordCut). NOLINE: a line this press drew whose cut is not recorded is set
 *                           aside again (roseReopen); a 10K / 14K include this press made is undone (Gate.undoGoldJoin: the set first, then the sheet).
 *    autoUnit               Gate.autoCut, ROSE GOLD ONLY (autoCuts = m => m === "rose"): the order of charm-nest-assembly-core.js autoCutPlan: A1 the orders RGCLEAR held are
 *                           released once their sheet is cut (releaseAsk, never by time: NO6H); A2 / A10 a sheet whose line would hold an order of another set is cleared
 *                           (roseReopen of a recorded cut, takeOffAsk of each foreign order, nestStart fresh); A7 RGONE: one Rose Gold sheet per set (cloudMoveIn of the
 *                           smaller sheets' orders onto the fullest, one order a minute); A12 the cut, its refusal leaving the include in its ONE set with the server's words.
 *    askUnit                a person's press (roseCutAsk: "cut" on a Rose Gold, 10K or 14K sheet; "joinFull": a full 10K / 14K sheet joins its set with its approval only), and
 *                           a line set aside by a change by hand redrawn (roseRedraw, §4.8: the line follows the charms; a recorded cut is approved again only by its own press).
 *    joinCut (G4)           "join": a sheet whose green line is cut already joins its set by the set rules from a viewer's press, never cut again.
 *  10K and 14K are NEVER cut by themselves (the server refuses via "auto" too). RG/10K/14K sheets mix only within one set: the cut is always into the sheet's ONE set (greenHomes).
 *  OWNEROVERRIDE: a sheet whose set the owner decided is never put in or cut into another set by the cloud. Its memory (the page's autoCutTried, rgOneTried, rgOneSay,
 *  run.rgClearHeld) is Charm_Nest_Green/<runId> (greenStatePut). No Etsy call. DORMANT: autoUnit runs in the checkpoint and labels steps, askUnit as the pass item
 *  "greenAsks" of lane "rosecut": only while the drive is "standby" or "on" and the step or lane is in drive.cloudSteps.
 *  GREENCORE (Paul, 20 Nov 2026): every choice here is charm-nest-assembly-core.js's (autoCutPlan, rgOne*), the copy the page's Gate.autoCut / rgOneMerge carry out too.                */
"use strict";
const Core = require("../../charm-nest-assembly-core.js");
const Rose = require("../../charm-nest-rose.js");
const RoseStock = require("./_charmNestRoseStock");
const Shapes = require("./_charmNestRoseShapes");
const Assembly = require("./_charmNestAssembly");
const { COLLECTIONS } = require("./_charmNestCloudSetOps");

const AUTO_BY = "Brites Manager (automatic)";
const num = v => (Number.isFinite(+v) ? +v : 0);
const say = (level, text) => { try { console.log(`[green] ${level}: ${String(text).slice(0, 600)}`); } catch (_) { /* a note only */ } };
const words = r => String((r && (r.error || r.why)) || "no answer").replace(/^Error: /, "").slice(0, 400);
const ok = r => !!r && r.status < 400 && !r.error;
const notCloudOp = r => !!r && r.status === 403 && /not a cloud op/.test(String(r.error || ""));
const hashOf = v => require("../../charm-nest-orders.js").textHash(JSON.stringify(v === undefined ? null : v));
const nameOf = rec => { try { return require("../../charm-nest-sheet-name.js").name(rec); } catch (_) { return rec.fileBase || rec.id; } };

async function readSheet(db, id) { const s = await db.collection("Charm_Nest_Sheets").doc(id).get(); return s.exists && !s.data().archived ? Object.assign({ id: s.id }, s.data()) : null; }
async function readStock(db, id) { if (!id) return null; const s = await db.collection("Charm_Nest_Rose_Stock").doc(id).get(); return s.exists ? Object.assign({ id: s.id }, s.data()) : null; }
async function greenState(db, runId) { const s = await db.collection(COLLECTIONS.GREEN).doc(runId).get(), d = s.exists ? s.data() || {} : {}; return { tried: d.tried || {}, oneTried: d.oneTried || {}, clearHeld: d.clearHeld || {}, said: d.said || {} }; }
async function saveGreen(ctx, next, was) {
  const keys = ["tried", "oneTried", "clearHeld", "said"], patch = {};
  for (const k of keys) if (hashOf(next[k]) !== hashOf(was[k])) patch[k] = next[k];
  if (!Object.keys(patch).length) return false;
  const r = await ctx.call({ op: "greenStatePut", runId: ctx.run.runId, patch }, `green-state|${ctx.run.runId}`);
  if (!ok(r)) say("warn", "the green line's memory was not saved: " + words(r));
  return true;
}

/* ── include / undo (Gate.changeMembership, cutInclude, rejoin, undoGoldJoin) ── */
async function include(ctx, p, set, o = {}) {
  const rec = await readSheet(ctx.db, p.sheetId); if (!rec) return { refused: true, why: "That sheet is no longer in the Library" };
  // OWNEROVERRIDE: a sheet whose set the owner decided goes into no other set by the cloud
  if (rec.ownerSet && typeof rec.ownerSet === "object" && String(rec.ownerSet.setId || "") !== String(set.setId)) return { refused: true, why: `the owner put ${nameOf(rec)} in ${rec.ownerSet.setId ? "another set" : "no set"}` };
  // (Gate.rejoin: a sheet of a COMMITTED set that lost its place on a page: the set and the record kept it, it takes its place back from them)
  if (set.committedAt && (set.sheetIds || []).includes(rec.id) && rec.setId === set.setId) {
    const r = await ctx.call({ op: "putSheet", sheet: { id: rec.id, draft: false, setId: set.setId, setSeq: set.seq } }, `green-rejoin|${rec.id}|${set.setId}`);
    return ok(r) ? { ok: true, rejoined: true } : { refused: true, why: words(r) };
  }
  const r = await ctx.call({ op: "putSheet", sheet: Object.assign({ id: rec.id, solidIncluded: true }, o.via === "auto" ? { autoMatched: set.setId } : {}, o.cutApproved ? { cutApproved: true } : {}) }, `green-yes|${rec.id}|${set.setId}|${o.via || ""}`);
  if (!ok(r)) return { refused: true, why: words(r) };
  await Assembly.assemblyUnit(ctx);   // (the include is a look at the sets with the sheet's own yes: tendSet numbers it, names its files and lists it, as the page's assemble does)
  const V = await Assembly.loadView({ db: ctx.db, run: ctx.run, now: Date.now() }), sh = V.pages.find(q => q.sheetId === rec.id);
  if (sh && sh.setId === set.setId && !sh.draft) return { ok: true, made: true };
  const G = Core.gate(V), why = sh ? (sh.setWait || sh.cardinalHold || G.policy(sh, set.seq).reason || "") : "";
  return { refused: true, why: `this sheet is not in a set yet${why ? ` (${why})` : ""}` };
}
async function undoInclude(ctx, rec, set, was) {
  // (the set first: it lists the sheet, and the server never changes the membership of a sheet an open set lists; then the sheet, a draft, its own tick as it was)
  const s = await ctx.db.collection("Charm_Nest_Sets").doc(set.setId).get(), d = s.exists ? Core.asSet(Object.assign({ setId: s.id }, s.data())) : null;
  if (d && (d.sheetIds || []).includes(rec.id)) {
    d.sheetIds = d.sheetIds.filter(x => x !== rec.id); d.labelFiles = (d.labelFiles || []).filter(f => f && f.sheetId !== rec.id); d.labels = null;
    for (const [rid, od] of Object.entries(d.orders || {})) { for (const [tid, l] of Object.entries(od.lines || {})) { l.copies = (l.copies || []).filter(c => c.sheetId !== rec.id); if (!l.copies.length) delete od.lines[tid]; } if (!Object.keys(od.lines || {}).length) delete d.orders[rid]; }
    await ctx.call({ op: "setUpdate", setId: set.setId, patch: Core.setPatch(d) }, `green-undo-set|${set.setId}|${rec.id}`);
  }
  const pick = rec.metal === "rose" ? (was.pick === true ? true : null) : was.pick === true;
  await ctx.call({ op: "putSheet", sheet: { id: rec.id, draft: true, setId: null, setSeq: null, sheetIndex: null, label: null, solidIncluded: pick } }, `green-undo-sheet|${rec.id}|${set.setId}`);
}

/* ── (VIEWINCLUDE, item 7, Paul 21 Nov 2026) the In current set switch of a Rose Gold, 10K or 14K sheet pressed on a computer that follows the run (a viewer): the page's changeMembership
   for that one sheet, from the records. include: the sheet's own yes (solidIncluded true; a take-out mark and Rose Gold's "automatic cut off" cleared), then a look at the sets: the set
   rules (charm-nest-assembly-core.js) decide whether it joins its ONE set now or waits, and the answer says which, in their words. exclude: a sheet in an open set is taken out of it
   (cmdLeaveSet, as the leaveset request does: a cut sheet, an order it shares with a sheet that stays, the owner's set refuse it in words); a sheet in no set loses its yes. → { ok, why } ── */
async function member(ctx, p, ask) {
  const rec = await readSheet(ctx.db, p.sheetId); if (!rec) return { ok: false, why: "That sheet is no longer in the Library" };
  const name = nameOf(rec), rose = rec.metal === "rose", gold = rose || rec.metal === "gold10k" || rec.metal === "gold14k", setNow = rec.setId && !rec.draft && rec.solidIncluded !== false ? String(rec.setId) : null;
  const setName = async id => { const s = await ctx.db.collection("Charm_Nest_Sets").doc(id).get(), d = s.exists ? s.data() || {} : {}; return d.seq ? `Set ${d.seq}` : d.name || "its set"; };
  if (num(rec.laserDoneAt) > 0) return { ok: false, why: `${name} has been cut on the laser: its set is fixed` };
  if (ask.kind === "include" && rec.roseCutAt && !setNow) return joinCut(ctx, p, ask);   // (G4: a cut sheet's Include is its join: never cut again, a committed set's place taken back)
  if (ask.kind === "include") {
    // (G3, Paul 21 Nov 2026: a Library move on a viewer names the set it was dropped on, ask.setId, or New set, ask.newSet; the switch of item 7 names none)
    const to = ask.setId ? String(ask.setId) : null, toName = to ? await setName(to) : ask.newSet ? "a new set" : "";
    if (setNow && (to ? to === setNow : !ask.newSet)) return { ok: true, why: `${name} is in ${await setName(setNow)} already` };
    let left = "";
    if (setNow) {   // (G3: between sets: out of the set it is in first, as the leaveset request does (the order rule, a cut, the owner's set refuse it in words), then in by the set rules)
      const r = await ctx.call({ op: "cmdLeaveSet", runId: rec.runId || ctx.run.runId, setId: setNow, sheetIds: [rec.id], by: String(ask.by || "") }, `member-move|${rec.id}|${setNow}|${num(ask.seq)}`);
      if (!ok(r)) return { ok: false, why: words(r) };
      left = await setName(setNow);
    }
    const cur = left ? (await readSheet(ctx.db, rec.id)) || rec : rec;
    const patch = Object.assign({ id: rec.id }, gold ? { solidIncluded: true } : {}, cur.leftSet ? { leftSet: null } : {}, rose && cur.autoCutOff ? { autoCutOff: false } : {});
    if (Object.keys(patch).length > 1) {
      const r = await ctx.call({ op: "putSheet", sheet: patch }, `member-yes|${rec.id}|${num(ask.seq)}`);
      if (!ok(r)) return { ok: false, why: left ? `${name} was taken out of ${left}, but it could not be put in again: ${words(r)}` : words(r) };
    }
    await Assembly.assemblyUnit(ctx, to ? { hand: { sheetId: rec.id, setId: to } } : undefined);   // (the include is a look at the sets with the sheet's own yes, as the page's changeMembership then assemble)
    const V = await Assembly.loadView({ db: ctx.db, run: ctx.run, now: Date.now() }), G = Core.gate(V), sh = V.pages.find(q => q.sheetId === rec.id), first = left ? `taken out of ${left}, ` : "";
    if (sh && sh.setId && !sh.draft) {
      const set = V.sets.find(s => s.setId === sh.setId), where = set && set.seq ? "Set " + set.seq : (set && set.name) || "its set";
      return { ok: true, why: to && String(sh.setId) !== to ? `${name} was ${first}put in ${where} by the set rules (not ${toName})` : `${name} is in ${where} now${left ? ` (taken out of ${left} first)` : ""}` };
    }
    const open = G.newestOpenOf(ctx.run.runId), why = sh ? (sh.setWait || sh.cardinalHold || G.policy(sh, open ? open.seq : 0).reason || "") : "";
    return { ok: true, why: `${name} is ${first}included: it joins ${toName && !gold ? toName : "its set"} once the set rules let it${why ? ` (${why})` : ""}` };
  }
  if (setNow) {
    const r = await ctx.call({ op: "cmdLeaveSet", runId: rec.runId || ctx.run.runId, setId: setNow, sheetIds: [rec.id], by: String(ask.by || "") }, `member-out|${rec.id}|${setNow}|${num(ask.seq)}`);
    if (!ok(r)) return { ok: false, why: words(r) };
    return { ok: true, why: `${name} was taken out of ${await setName(setNow)}: it is a draft now and waits for the next set` };
  }
  if (!gold) return { ok: true, why: `${name} is in no set: there is nothing to take it out of` };   // (G3: a GF / SS sheet has no yes of its own to take off)
  if (rec.solidIncluded !== true && !(rose && !rec.autoCutOff)) return { ok: true, why: `${name} is in no set and not included` };
  const r = await ctx.call({ op: "putSheet", sheet: Object.assign({ id: rec.id, solidIncluded: false }, rose ? { autoCutOff: true } : {}) }, `member-no|${rec.id}|${num(ask.seq)}`);
  if (!ok(r)) return { ok: false, why: words(r) };
  return { ok: true, why: `${name} is not included: it stays out of every set until In current set is switched on again` };
}
/* ── (G4, Paul 21 Nov 2026) a Rose Gold, 10K or 14K sheet whose green line is CUT ALREADY, put into its set on a computer that follows the run (a viewer): the Library's drag into a set, the set
   card's Approve, the In current set switch, Cut Sheet's Gate.rejoin. roseCutAsk "join" (and "include" on a cut sheet in no set), carried out from the records; NEVER cut again (no line, no claim,
   no roseRecordCut). A sheet whose record is in its set already is said so (Gate.rejoin's case: only a page's copy lost its place; the page follows the record); a draft record a COMMITTED set
   still lists is said in words with where it is put back (the Library's drop on that set: only the set's own edit changes a committed set); any other gets the person's own yes (solidIncluded; a take-out mark and Rose Gold's "automatic cut off" cleared, as changeMembership does for a press) and a look at the sets: the set rules
   (charm-nest-assembly-core.js) decide whether it joins now and which set, else it waits with their words (its yes is kept: it joins by itself once they let it). OWNEROVERRIDE: a sheet whose set
   the owner decided goes into no other set. ask.setId: the set it was dropped on, named in the answer when the rules put it elsewhere. → { ok, why } ── */
async function joinCut(ctx, p, ask) {
  const db = ctx.db, rec = await readSheet(db, p.sheetId); if (!rec) return { ok: false, why: "That sheet is no longer in the Library" };
  const name = nameOf(rec), rose = rec.metal === "rose", asked = ask && ask.setId ? String(ask.setId) : null;
  const setName = async id => { const s = await db.collection("Charm_Nest_Sets").doc(String(id)).get(), d = s.exists ? s.data() || {} : {}; return d.seq ? `Set ${d.seq}` : d.name || "its set"; };
  const where = async id => { const n = await setName(id); return asked && asked !== String(id) ? `${n} (its set by the set rules, not ${await setName(asked)})` : n; };
  if (!rec.roseCutAt) return { ok: false, why: `${name}: its green line is not cut yet. Press Cut Sheet (or Approve & cut green line) and it joins its set with its cut` };
  if (num(rec.laserDoneAt) > 0) return { ok: false, why: `${name} has been cut on the laser: its set is fixed` };
  if (rec.setId && !rec.draft && rec.solidIncluded !== false) return { ok: true, why: `${name} is in ${await where(rec.setId)} already. Its green line is cut, so nothing was cut again` };
  let V = await Assembly.loadView({ db, run: ctx.run, now: Date.now() }), G = Core.gate(V);
  // (Gate.rejoin) a page's copy that lost its place in a COMMITTED set while the record kept it is answered above ("in Set N already": the page follows the record). A committed set's sheets are
  // changed only by the set's own edit (the Library's drop on that set, flowApply setMember: the cloud never writes it, and putSheet never changes a committed set's membership), so a record
  // that is a draft while a committed set still lists it is said in words, with where it is done; never "done" when nothing changed
  const kept = V.sets.find(s => s.committedAt && (s.sheetIds || []).includes(rec.id));
  if (kept) return { ok: false, why: `${name} is listed by ${await setName(kept.setId)}, which was sent to the station: drop it on ${await setName(kept.setId)} in the Library to put it back there (the set's own edit). Nothing was cut` };
  const pv = V.pages.find(q => q.sheetId === rec.id) || p, home = G.homeSetOf(pv);
  if (rec.ownerSet && typeof rec.ownerSet === "object" && (!rec.ownerSet.setId || (home && String(rec.ownerSet.setId) !== String(home.setId)))) return { ok: false, why: `${name} stays where the owner put it (${rec.ownerSet.setId ? await setName(rec.ownerSet.setId) : "in no set"})` };
  const patch = Object.assign({ id: rec.id, solidIncluded: true }, rec.leftSet ? { leftSet: null } : {}, rose && rec.autoCutOff ? { autoCutOff: false } : {});
  const r = await ctx.call({ op: "putSheet", sheet: patch }, `g4-yes|${rec.id}|${num(ask && ask.seq)}`);
  if (!ok(r)) return { ok: false, why: `${name} is not in a set: ${words(r)}` };
  await Assembly.assemblyUnit(ctx);   // (its yes, then a look at the sets: tendSet numbers it, names its files and lists it, as the page's changeMembership then assemble)
  V = await Assembly.loadView({ db, run: ctx.run, now: Date.now() }); G = Core.gate(V);
  const sh = V.pages.find(q => q.sheetId === rec.id);
  if (sh && sh.setId && !sh.draft) return { ok: true, why: `${name} is in ${await where(sh.setId)} now. Its green line was cut already, so nothing was cut again` };
  const open = G.newestOpenOf(ctx.run.runId), why = sh ? (sh.setWait || sh.cardinalHold || G.policy(sh, open ? open.seq : 0).reason || "") : "";
  return { ok: false, why: `${name} is not in a set yet${why ? ` (${why})` : home ? "" : ": no open set of the run takes it yet"}. Your yes is kept: it joins its set by itself once the set rules let it` };
}
/* ── the physical sheet and the line (RoseStock.prepare / claimLate, then RoseStock.plan with cut: true), for a cut and for the line alone. no: the words' prefix ("Not cut: ").
   → { rec, drew } (rec as read after it), or { why } (refused, in words: nothing drawn; a fresh 10K / 14K sheet claimed stays claimed, as on the page) ── */
async function drawLine(ctx, p, rec, no) {
  const db = ctx.db;
  // the physical sheet (prepare / claimLate): a 10K / 14K sheet claims a fresh one now; a Rose Gold sheet holds its own since its nest
  if (!rec.roseStockId) {
    if (rec.metal === "rose") return { why: no + "its physical Rose Gold sheet is not claimed: nest the sheet again" };
    const st = rec.stock || {}, wPt = num(st.wPt) || num(st.wIn) * 72, hPt = num(st.hPt) || num(st.hIn) * 72;
    const r = await ctx.call({ op: "roseClaim", sheetId: rec.id, metal: rec.metal, wPt, hPt, fresh: true }, `green-claim|${rec.id}`);
    if (!ok(r)) return { why: no + words(r) };
    rec = await readSheet(db, p.sheetId);
    if (!rec || !rec.roseStockId) return { why: no + "its physical sheet could not be claimed" };
  }
  // the line (plan with cut: true), when the layout has none or changed since
  let drew = false;
  if (!rec.rosePlanHash || rec.roseFingerprint !== RoseStock.fingerprint(rec)) {
    if (!(rec.verification && rec.verification.ok) || rec.saving || rec.dirty || !(rec.outputs && rec.outputs.ai) || num(rec.filesOwed) > 0) return { why: no + "nest and save this layout before preparing its contour" };
    let shapes; try { shapes = await Shapes.forSheet(ctx, rec); } catch (e) { return { why: no + ((e && e.message) || e) }; }
    const stock = await readStock(db, rec.roseStockId);
    if (!stock) return { why: no + "its physical sheet is not in the cloud" };
    const r = await ctx.call({ op: "rosePlan", sheetId: rec.id, metal: rec.metal, stockId: rec.roseStockId, revision: stock.revision, fingerprint: RoseStock.fingerprint(rec), shapesJson: JSON.stringify(shapes), allowanceMm: num(rec.roseAllowanceMm) || 0.2, cut: true }, `green-plan|${rec.id}|${RoseStock.fingerprint(rec).length}|${hashOf(RoseStock.fingerprint(rec))}`);
    if (!ok(r)) return { why: no + words(r) };
    drew = true;
    rec = await readSheet(db, p.sheetId);
  }
  return { rec, drew };
}
/* ── the line alone (ITEM 4, Paul 21 Nov 2026: the Library's "Add the green dash line" pressed on a staff computer in mode "on"; RoseStock.plan with cut: true): roseCutAsk "line". The line is
   drawn and dated for the layout as it stands; no cut is recorded (only a Cut Sheet press records one) and the sheet joins no set. A line already drawn for this layout is the answer as it is. ── */
async function line(ctx, p) {
  const rec = await readSheet(ctx.db, p.sheetId);
  if (!rec) return { refused: true, why: "That sheet is no longer in the Library" };
  if (rec.roseCutAt) return { ok: true, already: true };
  if (num(rec.laserDoneAt) > 0) return { refused: true, why: "No green line drawn: this sheet has been cut on the laser" };
  const d = await drawLine(ctx, p, rec, "No green line drawn: ");
  return d.why ? { refused: true, why: d.why } : { ok: true, drew: d.drew };
}

/* ── Green.cut (RoseStock.record, rose-ui.js 250-300) ── */
async function cut(ctx, p, set, o) {
  const db = ctx.db; let rec = await readSheet(db, p.sheetId);
  if (!rec) return { refused: true, why: "That sheet is no longer in the Library" };
  if (rec.roseCutAt) return { ok: true, already: true, cutAt: num(rec.roseCutAt), stockId: rec.roseStockId || null, revision: rec.roseCutRevision || null };
  if (o.via === "auto" && rec.metal !== "rose") return { refused: true, why: `Not cut: a ${Rose.cutWord(rec.metal)} sheet is never cut automatically. It waits in its set for a person to approve it and cut its green line` };
  if (num(rec.laserDoneAt) > 0) return { refused: true, why: "Not cut: this sheet has been cut on the laser" };
  const was = { inSet: !!rec.setId && !rec.draft, pick: rec.solidIncluded === true };
  let joined = false;
  if (!was.inSet) {
    const inc = await include(ctx, p, set, o);
    if (!inc.ok) return { refused: true, why: "Not cut: " + inc.why };
    joined = !!inc.made && rec.metal !== "rose";
    rec = await readSheet(db, p.sheetId);
  } else if (o.via === "auto" && String(rec.autoMatched || "") !== String(set.setId)) {
    await ctx.call({ op: "putSheet", sheet: { id: rec.id, autoMatched: set.setId } }, `green-matched|${rec.id}|${set.setId}`);
  }
  const undo = async why => { if (joined) { const now = await readSheet(db, p.sheetId); if (now && !now.roseCutAt) await undoInclude(ctx, now, set, was); } return { refused: true, why }; };
  const d = await drawLine(ctx, p, rec, "Not cut: ");
  if (d.why) return undo(d.why);
  rec = d.rec; const drew = d.drew;
  const r = await ctx.call({ op: "roseRecordCut", sheetId: rec.id, metal: rec.metal, stockId: rec.roseStockId, revision: rec.roseRevision, planHash: rec.rosePlanHash, by: o.by || AUTO_BY, device: "cloud", via: o.via || "cloud-press" }, `green-cut|${rec.id}|${rec.rosePlanHash}`);
  if (!ok(r) || !r.cut) {
    // NOLINE (Paul, 12 Nov 2026): a green line exists only once its cut is recorded: the line this press drew is set aside again
    if (drew) { const now = await readSheet(db, p.sheetId); if (now && !now.roseCutAt) await ctx.call({ op: "roseReopen", sheetId: rec.id, replace: true, force: true, by: o.by || AUTO_BY }, `green-noline|${rec.id}|${rec.rosePlanHash}`); }
    return undo(words(r));
  }
  return { ok: true, cutAt: num(r.cut.at), stockId: r.cut.stockId || rec.roseStockId, revision: r.cut.revision || null };
}

/* ── the automatic cut (Gate.autoCut) ── */
async function autoUnit(ctx) {
  ctx = Assembly.passKeyed(ctx);
  const run = ctx.run; if (!run || run.releasePolicy !== 2) return undefined;
  const db = ctx.db, now = Date.now(), was = await greenState(db, run.runId), next = JSON.parse(JSON.stringify(was));
  const V = await Assembly.loadView({ db, run, now }), G = Core.gate(V), plan = G.autoCutPlan(next);
  // A1 · release after the cut (rgClearRelease): a release asked earlier is let go of once its job is done
  for (const [o, x] of Object.entries(next.clearHeld)) if (x && x.releasing) { try { const j = await db.collection("Charm_Nest_Release_Jobs").doc(String(o)).get(); if (j.exists && /^done/.test(String(j.data().state || ""))) delete next.clearHeld[o]; } catch (_) { /* asked again next time */ } }
  for (const o of plan.release) {
    const r = await ctx.call({ op: "releaseAsk", orderId: o, by: AUTO_BY }, `rg-release|${run.runId}|${o}`);
    if (ok(r)) { next.clearHeld[o] = Object.assign({}, next.clearHeld[o] || {}, { releasing: true, askedAt: now }); say("ok", `Order ${o}: released after the green line cut; it nests past the line`); }
    else say("warn", `Order ${o} waits for its release after the green line cut (${notCloudOp(r) ? "the cloud's release is not here yet" : words(r)})`);
  }
  // A2 / A10 · RGCLEAR: a Rose Gold sheet's line holds only its own set's orders
  for (const c of plan.clear) {
    if (Date.now() > ctx.budgetEnd) break;
    const p = c.sheet, key = "clear|" + p.sheetId, t = next.tried[key];
    if (t && now - num(t.at) < 30000) continue;
    next.tried[key] = { at: now };
    const ow = c.foreign.length === 1 ? `order ${c.foreign[0]}` : `orders ${c.foreign.join(", ")}`, name = G.sheetNameOf(p);
    next.said[p.sheetId] = `${c.setName}: ${ow} ${c.foreign.length === 1 ? "has" : "have"} pieces in another set, so ${c.foreign.length === 1 ? "it goes" : "they go"} past the green line of ${name}; the sheet is nested again, then cut`;
    if (p.roseCutAt) { const r = await ctx.call({ op: "roseReopen", sheetId: p.sheetId, replace: true, force: true, by: "Brites Manager (automatic: orders of another set go past the green line)" }, `rg-clear-reopen|${p.sheetId}|${p.roseCutAt}`); if (!ok(r)) { say("warn", `${name}: its orders of another set could not be taken off yet (${words(r)}); tried again shortly`); continue; } }
    let all = true;
    for (const o of c.foreign) {
      const r = await ctx.call({ op: "takeOffAsk", orderId: o, kind: "sheetOff", scope: "sheet", sheetId: p.sheetId, note: `Past the green line of ${name}: its other pieces are in another set`, by: AUTO_BY }, `rg-clear|${p.sheetId}|${o}`);
      if (ok(r)) { next.clearHeld[o] = { sheetId: p.sheetId, at: now }; say("ok", `Order ${o}: taken off ${name} automatically; it goes past its green line, with its own set`); }
      else { all = false; say("warn", `Order ${o} could not come off ${name} yet (${notCloudOp(r) ? "the cloud's take-off is not here yet" : words(r)}); tried again shortly`); }
    }
    if (all) { const r = await ctx.call({ op: "nestStart", sheetId: p.sheetId, fresh: true }, `rg-clear-nest|${p.sheetId}|${hashOf(c.foreign)}`); if (!ok(r)) say("warn", `${name}: its nest again waits (${notCloudOp(r) ? "the cloud's nest is not here yet" : words(r)})`); }
  }
  // A7 · RGONE: the uncut Rose Gold sheets of one set become ONE sheet: the smaller sheets' orders move onto the fullest, one order a minute
  // (GREENCORE: the same choices as the page's rgOneMerge, charm-nest-assembly-core.js rgOne*: a target that is busy, being cut by hand or holding a drawn line takes nothing; a busy source gives nothing)
  for (const m of plan.merge) {
    if (Date.now() > ctx.budgetEnd) break;
    const target = m.list[0];
    if (G.rgOneBusy(target)) continue;
    move: for (const src of m.list.slice(1)) {
      if (G.rgOneBusy(src)) continue;
      for (const o of G.rgOneOrders(src)) {
        const key = G.rgOneKey(m.home, src, o);
        if (!G.rgOneDue(next.oneTried, key)) continue;
        const pieces = G.rgOnePieces(src, o); if (!pieces.length) continue;
        next.oneTried[key] = { at: now };
        next.said[m.home] = `${m.setName}: its Rose Gold pieces go onto ONE sheet: order ${o} is moving from ${G.sheetNameOf(src)} to ${G.sheetNameOf(target)}`;
        const r = await ctx.call({ op: "cloudMoveIn", rid: o, poolIds: pieces.map(c => c.poolId).filter(Boolean), from: [{ sheetId: src.sheetId, layoutBase: src.layoutRev, contentBase: src.contentRev }], to: { sheetId: target.sheetId, layoutBase: target.layoutRev, contentBase: target.contentRev }, by: AUTO_BY, green: true }, `rg-one|${key}|${src.layoutRev}|${target.layoutRev}`);
        if (ok(r)) { say("ok", `Order ${o}: its Rose Gold piece${pieces.length === 1 ? "" : "s"} moved from ${G.sheetNameOf(src)} to ${G.sheetNameOf(target)} automatically, so ${m.setName} has ONE Rose Gold sheet`); delete next.said[m.home]; break move; }
        const why = words(r), room = /room|space|fit/i.test(why);
        next.oneTried[key] = { at: now, room };
        if (notCloudOp(r)) next.said[m.home] = `${m.setName}: its Rose Gold pieces go onto ONE sheet once the cloud moves pieces between sheets; until then neither sheet is cut automatically`;
        say("warn", `Order ${o}: its Rose Gold piece${pieces.length === 1 ? "" : "s"} could not move from ${G.sheetNameOf(src)} to ${G.sheetNameOf(target)} yet (${why})`);
        if (notCloudOp(r)) break move;
      }
    }
  }
  // A12 · the cut, each into its ONE set
  for (const x of plan.cut) {
    if (Date.now() > ctx.budgetEnd) break;
    const p = x.sheet, s = x.set, tried = { key: x.key, at: now };
    next.tried[p.sheetId] = tried;
    const r = await cut(ctx, p, s, { via: "auto", by: AUTO_BY });
    if (r.ok) { delete next.said[p.sheetId]; await ctx.call({ op: "putSheet", sheet: { id: p.sheetId, autoCutWhy: null } }, `green-why-clear|${p.sheetId}|${r.cutAt}`); say("ok", `${G.sheetNameOf(p)}: green line cut automatically; it is in ${s.name || "its set"} with the Gold Filled and Sterling Silver sheets it shares orders with`); continue; }
    const why = String(r.why || "").replace(/^Not cut(?: yet)?: /, ""); tried.refused = true;
    const rec = await readSheet(ctx.db, p.sheetId);
    if (rec && rec.setId === s.setId && !rec.draft && !rec.roseCutAt && !p.leftSet) {
      // (Paul, 12 Nov 2026: the include STAYS: the sheet waits IN its one set for its cut, the set's Approve waits for it, and the next try cuts it there)
      // (its own yes is on the record already, from the include: only the set it was matched to and the server's words are written; a membership field would be refused, its set listing it)
      await ctx.call({ op: "putSheet", sheet: { id: p.sheetId, autoMatched: s.setId, autoCutWhy: why } }, `green-wait|${p.sheetId}|${hashOf(why)}`);
      tried.waitsIn = true;
      next.said[p.sheetId] = `In ${s.name || "its set"}, waiting for its green line cut: ${why}. It is cut automatically as soon as that clears`;
      say("warn", `${G.sheetNameOf(p)}: its green line was not cut automatically yet (${why}); it waits in ${s.name || "its set"} and is tried again`);
    } else {
      next.said[p.sheetId] = `Its green line is cut automatically, and it joins ${s.name || "its set"}, once it is ready: ${why}`;
      say("warn", `${G.sheetNameOf(p)}: its green line was not cut automatically yet (${why}); it waits outside ${s.name || "its set"} and is tried again`);
    }
  }
  for (const [id, w] of Object.entries(plan.said)) next.said[id] = w;
  await saveGreen(ctx, next, was);
  return undefined;
}

/* ── a person's press, and a line set aside by a change by hand ── */
async function askList(db, runId) {
  const q = await db.collection(COLLECTIONS.CUT_ASKS).where("state", "in", ["asked", "working"]).limit(100).get(), now = Date.now();
  // (a press on a sheet of another run, one that waits in an older run's set, is answered too, with that run's records)
  return q.docs.map(d => d.data()).filter(a => a && (a.state === "asked" || (a.state === "working" && now - num(a.at) > 10 * 60000))).sort((a, b) => (a.runId === runId ? 0 : 1) - (b.runId === runId ? 0 : 1) || num(a.at) - num(b.at));
}
async function answer(ctx, ask, patch) { await ctx.call({ op: "roseCutAskPut", sheetId: ask.sheetId, seq: ask.seq, patch }, `cut-ask|${ask.sheetId}|${ask.seq}|${patch.state}`); }
async function askUnit(ctx) {
  ctx = Assembly.passKeyed(ctx);
  const run = ctx.run; if (!run || run.releasePolicy !== 2) return undefined;
  const db = ctx.db;
  for (const ask of await askList(db, run.runId)) {
    if (Date.now() > ctx.budgetEnd) return { more: true };
    await answer(ctx, ask, { state: "working" });
    try {
      // (the sheet's own run: the open run, or an older one whose set the sheet waits in)
      let actx = ctx;
      if (ask.runId && ask.runId !== run.runId) { const got = await ctx.read({ op: "runGet", runId: ask.runId }), arun = (got && got.run) || { runId: ask.runId, releasePolicy: 2, lines: {} }; actx = Object.assign({}, ctx, { run: arun, runId: arun.runId }); }
      const V = await Assembly.loadView({ db, run: actx.run, now: Date.now() }), G = Core.gate(V), p = V.pages.find(q => q.sheetId === ask.sheetId);
      if (!p) { await answer(ctx, ask, { state: "refused", why: "That sheet is not one of the run's", doneAt: Date.now() }); continue; }
      if (ask.kind === "include" || ask.kind === "exclude") {   // (VIEWINCLUDE, item 7: a viewer's In current set switch)
        const r = await member(actx, p, ask);
        await answer(ctx, ask, { state: r.ok ? "done" : "refused", why: r.why, doneAt: Date.now() });
        continue;
      }
      if (ask.kind === "join") {   // (G4: a cut sheet into its set by the set rules, never cut again)
        const r = await joinCut(actx, p, ask);
        await answer(ctx, ask, { state: r.ok ? "done" : "refused", why: r.why, doneAt: Date.now() });
        continue;
      }
      if (ask.kind === "line") {   // (ITEM 4: the line alone needs no set: it is drawn where the sheet is)
        const r = await line(actx, p);
        await answer(ctx, ask, r.ok ? { state: "done", doneAt: Date.now() } : { state: "refused", why: r.why, doneAt: Date.now() });
        continue;
      }
      const set = G.homeSetOf(p);
      if (!set) { await answer(ctx, ask, { state: "refused", why: "No open set of the run takes it yet: it is cut once its set forms", doneAt: Date.now() }); continue; }
      if (ask.kind === "joinFull") {
        const rec = await readSheet(db, p.sheetId), full = rec && (rec.releaseFull || await Shapes.full(ctx, rec, await readStock(db, rec.roseStockId)));
        if (!full) { await answer(ctx, ask, { state: "refused", why: "this sheet is not full: press Approve & cut green line", doneAt: Date.now() }); continue; }
        const inc = G.inSetNow(p) ? { ok: true } : await include(actx, p, set, { cutApproved: true, via: "cloud-press" });
        await answer(ctx, ask, inc.ok ? { state: "done", doneAt: Date.now() } : { state: "refused", why: inc.why, doneAt: Date.now() });
        continue;
      }
      const r = await cut(actx, p, set, { via: "cloud-press", by: ask.by });
      await answer(ctx, ask, r.ok ? { state: "done", cutAt: r.cutAt, stockId: r.stockId, revision: r.revision, doneAt: Date.now() } : { state: "refused", why: r.why, doneAt: Date.now() });
    } catch (e) {
      if (e && (e.fenced || e.yielded || e.overBudget)) throw e;
      await answer(ctx, ask, { state: "refused", why: String((e && e.message) || e).slice(0, 400), doneAt: Date.now() });
    }
  }
  // §4.8 · a 10K / 14K line set aside by a change by hand (roseRedraw, written by sheetLayoutPatch in mode "on"): drawn again once the sheet is written and verified
  // (one single-field query: only the sheets that owe a redraw; a sheet of another run is left to its own run)
  const q = await db.collection("Charm_Nest_Sheets").where("roseRedraw.line", "==", true).limit(50).get();
  for (const d of q.docs.filter(x => (x.data() || {}).runId === run.runId)) {
    if (Date.now() > ctx.budgetEnd) return { more: true };
    const rec = Object.assign({ id: d.id }, d.data()), rd = rec.roseRedraw;
    if (!rd || rec.archived || !rd.line || rec.roseCutAt || rec.dirty || rec.saving || num(rec.filesOwed) > 0 || !(rec.verification && rec.verification.ok) || !(rec.outputs && rec.outputs.ai) || !rec.roseStockId) continue;
    const stock = await readStock(db, rec.roseStockId); if (!stock) continue;
    let shapes; try { shapes = await Shapes.forSheet(ctx, rec); } catch (e) { say("warn", `${nameOf(rec)}: its green line waits (${(e && e.message) || e})`); continue; }
    const r = await ctx.call({ op: "rosePlan", sheetId: rec.id, metal: rec.metal, stockId: rec.roseStockId, revision: stock.revision, fingerprint: RoseStock.fingerprint(rec), shapesJson: JSON.stringify(shapes), allowanceMm: num(rec.roseAllowanceMm) || 0.2, cut: true }, `green-redraw|${rec.id}|${hashOf(RoseStock.fingerprint(rec))}`);
    if (!ok(r)) { say("warn", `${nameOf(rec)}: its green line was not drawn again (${words(r)})`); continue; }
    await ctx.call({ op: "putSheet", sheet: { id: rec.id, roseRedraw: null, ...(rd.cut ? { roseReopened: { at: num(rd.at), by: String(rd.by || "") } } : {}) } }, `green-redrawn|${rec.id}|${num(rd.at)}`);
  }
  return undefined;
}

/* ── RGLINK: the link cut (Paul, 23 Nov 2026: "another multi piece mixed order arrives to the GF draft sheet and forces the Green Cut line to occure so as to not connect 2 GF or SS sheets
   to a single RG sheet"; "It's much more challenging to cut the green line after parts have been nested") ──
   A Rose Gold draft the placement marked (roseLinkCut: an order would tie it to a second GF / SS sheet, charm-nest-place.js chooseSheet, _charmNestPlaceUnit.js markLinkCut) has its green
   line drawn around what it holds and its cut recorded AT ONCE, in no set yet: the order that waits on it then goes on the leftover of the same plate (the next draft), and the cut draft
   joins the set its own GF / SS sheet forms (the cardinal home; a recorded cut is its yes, goldGap). Rose Gold only: roseRecordCut via "auto" refuses 10K / 14K. A draft being nested or
   written waits for the next pass (drawLine asks a saved, verified, written layout); a cut refused sets the line this pass drew aside again (NOLINE) and is tried again next pass. ── */
const LIVE = ["queued", "running", "starting", "claimed"];
const jobLive = j => !!(j && LIVE.includes(String(j.status || "")));
async function linkUnit(ctx) {
  ctx = Assembly.passKeyed(ctx);
  const run = ctx.run; if (!run || !run.runId) return undefined;
  const db = ctx.db;
  /* (the lines that waited for a link cut go on once it is recorded: placed on the leftover by the placement. A resting run comes back to its steps only when a line or a sheet it
     counts changes, and a cut alone is not one: the order waited on a cut sheet until something else moved (RG Draft 1, 23 Nov 2026, 02:17). The line set back to "pooled" is that
     change, as the placement's own resume of a wait that is due) */
  { const L = run.lines || {}, waiting = Object.entries(L).filter(([, l]) => l && l.state === "waiting" && l.wait && l.wait.kind === "green" && l.wait.cut === "link" && l.wait.sheetId);
    const ids = [...new Set(waiting.map(([, l]) => String(l.wait.sheetId)))], gone = new Set();
    for (const id of ids) { const r = await readSheet(db, id); if (!r || r.roseCutAt || num(r.laserDoneAt) > 0 || r.releaseFull || (r.setId && !r.draft)) gone.add(id); }
    const due = waiting.filter(([, l]) => gone.has(String(l.wait.sheetId)));
    if (due.length) {
      const lines = Object.fromEntries(due.map(([k, l]) => [k, { state: (l.poolIds || []).length ? "pooled" : "pulled", wait: null, reason: null }]));
      const r = await ctx.call({ op: "runLinesPatch", runId: run.runId, lines }, `green-link-resume|${run.runId}|${due.map(([k]) => k).sort().join(",")}`);
      if (ok(r)) { for (const [k, l] of due) Object.assign(l, lines[k]); say("ok", `${due.length} order line(s) that waited for a green line cut go on, onto its leftover`); }
      else say("warn", `the lines that waited for a green line cut were not set going (${words(r)}); tried again on the next pass`);
    } }
  const q = await db.collection("Charm_Nest_Sheets").where("roseLinkCut.by", "==", "Cloud").limit(20).get();
  for (const d of q.docs) {
    if (Date.now() > ctx.budgetEnd) return { more: true };
    const rec = Object.assign({ id: d.id }, d.data());
    if (rec.runId !== run.runId || rec.archived) continue;   // (a sheet of another run: its own run's pass)
    if (rec.metal !== "rose" || rec.roseCutAt || num(rec.laserDoneAt) > 0 || (rec.setId && !rec.draft)) {   // (cut already, in a set (its set's automatic cut decides), or not Rose Gold: nothing owed)
      await ctx.call({ op: "putSheet", sheet: { id: rec.id, roseLinkCut: null } }, `green-link-done|${rec.id}|${num(rec.roseLinkCut && rec.roseLinkCut.at)}`); continue;
    }
    if (rec.saving || rec.dirty || num(rec.filesOwed) > 0 || jobLive(rec.nestJob) || jobLive(rec.filesJob) || (rec.manualEdit && Date.now() - num(rec.manualEdit.at) < 90000)) continue;   // (being nested, written or moved by hand: next pass)
    const p = { sheetId: rec.id }, dl = await drawLine(ctx, p, rec, "Not cut: ");
    if (dl.why) { say("warn", `${nameOf(rec)}: its link cut waits (${dl.why})`); continue; }
    const r2 = dl.rec, at = num(rec.roseLinkCut && rec.roseLinkCut.at);
    const r = await ctx.call({ op: "roseRecordCut", sheetId: r2.id, metal: r2.metal, stockId: r2.roseStockId, revision: r2.roseRevision, planHash: r2.rosePlanHash, by: AUTO_BY + " (a second GF / SS sheet would tie to it)", device: "cloud", via: "auto" }, `green-link|${r2.id}|${r2.rosePlanHash}`);
    if (!ok(r) || !r.cut) {
      if (dl.drew) { const now = await readSheet(db, r2.id); if (now && !now.roseCutAt) await ctx.call({ op: "roseReopen", sheetId: r2.id, replace: true, force: true, by: AUTO_BY }, `green-link-noline|${r2.id}|${r2.rosePlanHash}`); }
      say("warn", `${nameOf(rec)}: its link cut was not recorded (${words(r)}); tried again on the next pass`); continue;
    }
    await ctx.call({ op: "putSheet", sheet: { id: r2.id, roseLinkCut: null, roseLinkCutAt: num(r.cut.at) || Date.now() } }, `green-link-done|${r2.id}|${at}`);
    say("ok", `${nameOf(rec)}: green line cut (order ${(rec.roseLinkCut && rec.roseLinkCut.orderId) || "?"} would have tied it to a second Gold Filled / Sterling Silver sheet); that order goes on its leftover`);
  }
  return undefined;
}

module.exports = { autoUnit, askUnit, cut, include, undoInclude, member, joinCut, linkUnit, greenState, AUTO_BY };
/* the steps file's entry (_charmNestSteps.js): the presses, as the pass item "greenAsks" of lane "rosecut"; the automatic cut by name, for the checkpoint and labels steps */
module.exports.units = { passItems: [{ name: "greenAsks", lane: "rosecut", unit: askUnit }, { name: "greenLinks", lane: "rosecut", order: 65, unit: linkUnit }], named: { greenAuto: autoUnit } };   // (RGLINK: the link cut after the files lane, 60)
