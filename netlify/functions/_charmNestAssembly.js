/*  netlify/functions/_charmNestAssembly.js — THE CLOUD'S SET ASSEMBLY (cloud plan v2 build step 10a; designs.json "Set assembly"). The page's Gate.assemble, carried out by the
 *  cloud from the records: charm-nest-assembly-core.js decides (the page's own rules, ported), this file reads the records and makes the writes the decisions ask for, each
 *  through the driver's ctx.call (Lib.call: the cloud's own call, fenced on the pass's lease and the run's carrier, a mid per unit and body).
 *
 *  What a look writes, in the page's order (tendSet, then Sets.onSheetSaved without the drawing: the labels unit, _charmNestLabels.js, draws the QR labels it leaves owed):
 *    setAllocate         the next set's number (Sets.ensure: idempotent per run, group and the set it follows)
 *    putSheet            a sheet's releasedAt (the time it was first seen completed: the order sets form in), its membership (sheet first, then its set: the server refuses a
 *                        marked call that changes the membership of a sheet an open set already lists, so the set is written second)
 *    poolUpdate          the joined sheet's pieces: { sheetId, setId, state "written", sheetName }
 *    runLinesPatch       a line whose every piece is now on a sheet: pooled → written
 *    setUpdate           the set as the page's Sets.save sends it (sheetIds, materials, orders, labelFiles, labels: null, status "nesting", formedAt, sheetNos)
 *    assemblyStatePut    Charm_Nest_Assembly/<runId>: the lines each card says (setWait, cardinalHold/Note/Pull/Home, greenAway), the joins refused for ten minutes, the clocks,
 *                        the owner-rule self-check (charm-nest-set-invariants.js)
 *  A sheet a PERSON marked to leave a set (its record a draft that still names the set, leftSet = the set) is let go: the set first, then the sheet (NOSETPULL: nothing else ever
 *  leaves a set). Nothing here calls Etsy, builds the world or touches a placement; the core never adds a piece to a sheet (SETSHUT).
 *
 *  SHADOW (m:C7): shadow(db, runId) plans the same look from the records while a computer still assembles, writes nothing but Charm_Nest_Assembly/<runId>.shadow, and keeps
 *  the time since the cloud's plan last differed from what the computer did (cleanSince). The owner moves "checkpoint" and "labels" into drive.cloudSteps after a week clean.
 *  DORMANT: the unit runs only in the driver's checkpoint step, which runs only while the drive is "standby" or "on" and "checkpoint" is in drive.cloudSteps; the shadow only when
 *  drive.shadow lists "assembly".                                                                                                                                                */
"use strict";
const Core = require("../../charm-nest-assembly-core.js");
const O = require("../../charm-nest-orders.js");
const { COLLECTIONS } = require("./_charmNestCloudSetOps");

const SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets", POOL = "Charm_Pool";
const num = v => (Number.isFinite(+v) ? +v : 0);
const turn = () => new Promise(r => (typeof setImmediate === "function" ? setImmediate(r) : setTimeout(r, 0)));
const hashOf = v => O.textHash(JSON.stringify(v === undefined ? null : v));
const say = (level, text) => { try { console.log(`[assembly] ${level}: ${String(text).slice(0, 600)}`); } catch (_) { /* a note only */ } };
/** The unit keys of a pass (Lib.call's mid is made of the unit key and the body; the mid ledger answers an identical one as replayed for 7 days): each key here carries the pass's
 *  lease nonce, so a write sent twice in one pass is written once, and the same write made again in a LATER pass (a sheet that leaves and rejoins a set, a set that goes back to
 *  "awaiting review") is written again instead of answered from the ledger. Every write of these units is idempotent: it says what a record is, by key. */
const passKeyed = ctx => (ctx && !ctx.passKeyed && ctx.fence && ctx.fence.nonce ? Object.assign({}, ctx, { passKeyed: true, call: (b, u) => ctx.call(b, String(u) + "|" + ctx.fence.nonce) }) : ctx);

/** The run's records as the core's view: its sheets (not archived) as pages, its sets, the assembly's memory. o: { db, run, now, sets?: the run's set records already read,
 *  ids?: only these sheets (the labels unit reads the sheets its open sets list, not the whole run) } */
async function loadView(o) {
  const { db, run } = o, runId = run.runId, now = num(o.now) || Date.now();
  const sheetDocs = async () => { if (!Array.isArray(o.ids)) return (await db.collection(SHEETS).where("runId", "==", runId).get()).docs; const out = [], ids = [...new Set(o.ids.map(String))]; for (let i = 0; i < ids.length; i += 100) out.push(...(await db.getAll(...ids.slice(i, i + 100).map(id => db.collection(SHEETS).doc(id)))).filter(d => d.exists)); return out; };
  const [sdocs, tdocs, js] = await Promise.all([sheetDocs(), o.sets ? Promise.resolve(null) : db.collection(SETS).where("runId", "==", runId).get().then(q => q.docs), db.collection(COLLECTIONS.ASSEMBLY).doc(runId).get()]);
  const sets = (o.sets ? o.sets.map(s => Core.asSet(s)) : tdocs.map(d => Core.asSet(Object.assign({ setId: d.id }, d.data())))).filter(s => !/^superseded/.test(String(s.status || "")));
  const setsById = new Map(sets.map(s => [String(s.setId), s]));
  const recs = sdocs.map(d => Object.assign({ id: d.id }, d.data())).filter(r => r.archived !== true && r.runId === runId);
  const pages = recs.map(r => Core.asPage(r, { run, setsById, now }));
  const journal = js.exists ? js.data() || {} : {};
  const today = Core.torontoDay(now);
  const V = { run, pages, sets, now, today, journal, records: new Map(recs.map(r => [r.id, r])),
    joinRefused: new Map(Object.entries(journal.joinRefused || {}).filter(([, v]) => v && now - num(v.at) < 600000)), clock: Object.assign({ completion: 0, join: 0 }, journal.clock || {}),
    fileBase: p => Core.sheetFileBase(p, today) };
  return V;
}
/** The pool rows of these pieces (the fields a set's order lines and a sheet's label read): Map(poolId -> row). */
async function poolRows(db, ids) {
  const out = new Map(), list = [...new Set(ids.filter(Boolean).map(String))];
  for (let i = 0; i < list.length; i += 100) for (const s of await db.getAll(...list.slice(i, i + 100).map(id => db.collection(POOL).doc(id)), { fieldMask: ["orderId", "transactionId", "sku", "copy", "side", "groupSize", "state", "sheetId", "setId", "sheetName", "material"] })) if (s.exists) out.set(s.id, s.data());
  return out;
}
const placedOf = p => { const byId = new Map((p.charms || []).map(c => [c.id, c])); return (p.placements || []).map(x => byId.get(x.id)).filter(Boolean); };
const answerWords = r => String((r && (r.error || r.why)) || "no answer").slice(0, 300);
/* FRESHWRITE (Paul, 27 Nov 2026: "The Library sheets are NOT updating to the latest versions from the cloud. I have also seen sheets update and reverse back to their previous incorrect state"): a look
   read the run's sheets and sets once, then wrote a set's order lines, label files and labels (and a sheet's label and its orders) back WHOLE up to a minute later. A person's Change SKU done
   meanwhile in its own process (cmdSwapLine) was written over, and the line it moved came back on its old sheet and set. Every write a look makes of a record it planned from now says the
   revision it read (a set: setBase = its setRev; a sheet: contentBase = its contentRev); the server refuses it in its own transaction when the record moved since (409, conflict), nothing is
   written, and the look is planned again on the records as they are now (replan: the unit answers { more: true }, so the driver runs it again at once, never a stop of the run). Chosen over
   deriving the lists inside the server's transaction: a label's drawing, a set's labels PDF and its order lines are made from the look's whole view (pair notes, the other sheets, the pool
   rows), which a transaction cannot make again; a refusal keeps every field of the newer record exactly as the person's change left it. */
const moved = r => !!(r && +r.status === 409 && r.conflict);
const replan = (what, r) => Object.assign(new Error(`${what} changed since this look read it (${answerWords(r)}): it is planned again on the record as it is now`), { replan: true });
/** A look's write of a set: carries the set revision the look read (setBase); the answer's revision is kept on the set, so the look's next write of it chains on its own. Refused: replan. */
async function setWrite(ctx, set, patch, unit, more) {
  const r = await ctx.call(Object.assign({ op: "setUpdate", setId: set.setId, patch, setBase: num(set.setRev) }, more || {}), unit);
  if (moved(r)) throw replan(set.name || set.setId, r);
  if (r && r.status < 400 && Number.isFinite(+r.setRev)) set.setRev = +r.setRev;
  return r;
}

/** The writes the core asks for, made through the driver's ctx.call. */
function cloudIo(ctx, V, rowsCache) {
  const { db, run } = ctx, runId = run.runId;
  const rowOf = id => rowsCache.get(String(id)) || null;
  const io = {
    say,
    pace: async () => { await turn(); if (Date.now() > ctx.budgetEnd) throw Object.assign(new Error("the set assembly's time is spent: it goes on in the next pass"), { overBudget: true }); },
    async ensureSet(o) {
      const follow = o && o.follow && o.follow.setId ? o.follow : null;
      const after = follow && !follow.committedAt && !(follow.sheetIds || []).length ? null : (follow && follow.setId) || null;
      const r = await ctx.call({ op: "setAllocate", day: V.today, runId, group: "dispatch", roseOnly: false, after }, `set|${runId}|dispatch|${after || ""}`);
      if (!r || r.status >= 400) throw new Error("The next set could not be numbered: " + answerWords(r));
      if (r.deferred) return null;
      const known = V.sets.find(s => s.setId === r.setId); if (known) return known;
      const set = Core.asSet({ setId: r.setId, seq: r.seq, day: r.day || V.today, runId, group: "dispatch", name: O.setLabel(r.seq), folder: O.setFolder(r.day || V.today, r.seq), orders: {}, sheetIds: [], materials: [], labelFiles: [], sheetNos: {}, status: "open", setRev: num(r.setRev) });   // (FRESHWRITE: a set handed back says its revision)
      V.sets.push(set);
      run.setIds = [...new Set((run.setIds || []).concat([set.setId]))]; run.setId = run.setId || set.setId;
      say("cloud", `${set.name} allocated for ${set.day} → ${set.folder}`);
      return set;
    },
    async stampReleased(p, at) {
      const r = await ctx.call({ op: "putSheet", sheet: { id: p.sheetId, releasedAt: at || null } }, `released|${p.sheetId}|${at || 0}`);
      if (!r || r.status >= 400) say("warn", `${p.fileBase || p.sheetId}: its completion time was not saved (${answerWords(r)})`);
    },
    async join(p, set, m) {
      const r = await ctx.call({ op: "putSheet", sheet: Object.assign({ id: p.sheetId }, m) }, `join|${p.sheetId}|${set.setId}|${m.sheetIndex}`);
      if (r && r.status < 400 && r.ok) return { ok: true };
      if (r && (r.refused === "setRules" || r.refused === "setShut")) return { refused: "setRules", why: answerWords(r) };
      return { ok: false, why: answerWords(r) };
    },
    async joined(p, set, o) {
      const placed = placedOf(p), ids = placed.map(c => c.poolId).filter(Boolean);
      const missing = ids.filter(id => !rowsCache.has(String(id)));
      if (missing.length) for (const [k, v] of await poolRows(db, missing)) rowsCache.set(k, v);
      const added = Core.addCopies(set, p, rowOf);
      if (added) set.labels = null;
      let wrote = false;
      if (!(o && o.again) && !set.committedAt && ids.length) {
        const todo = ids.filter(id => { const r = rowOf(id); return !r || r.state !== "written" || r.sheetId !== p.sheetId || r.setId !== set.setId || r.sheetName !== p.fileBase; });
        for (let i = 0; i < todo.length; i += 400) {
          const part = todo.slice(i, i + 400);
          const r = await ctx.call({ op: "poolUpdate", poolIds: part, patch: { sheetId: p.sheetId, setId: set.setId, state: "written", sheetName: p.fileBase } }, `written|${p.sheetId}|${set.setId}|${hashOf(part)}`);
          if (!r || r.status >= 400) throw new Error(`${p.fileBase}: its pieces were not marked written: ${answerWords(r)}`);
          for (const id of part) rowsCache.set(String(id), Object.assign({}, rowOf(id) || {}, { sheetId: p.sheetId, setId: set.setId, state: "written", sheetName: p.fileBase }));
          wrote = true;
        }
        // (the run's lines whose every piece is on a sheet now: pooled → written, as the page's onSheetSaved did)
        const onSheet = new Set(V.pages.filter(q => !q.recalled).flatMap(q => (q.charms || []).map(c => c.poolId)).filter(Boolean));
        const lines = {}; for (const [k, l] of Object.entries(run.lines || {})) if (l && l.state === "pooled" && (l.poolIds || []).length && l.poolIds.some(id => ids.includes(id)) && l.poolIds.every(id => onSheet.has(id))) lines[k] = { state: "written" };
        const keys = Object.keys(lines);
        for (let i = 0; i < keys.length; i += 300) {
          const part = Object.fromEntries(keys.slice(i, i + 300).map(k => [k, lines[k]]));
          const r = await ctx.call({ op: "runLinesPatch", runId, lines: part }, `lines-written|${runId}|${hashOf(Object.keys(part))}`);
          if (r && r.status < 400) for (const k of Object.keys(part)) if (run.lines && run.lines[k]) run.lines[k] = Object.assign({}, run.lines[k], { state: "written" });
        }
      }
      return added > 0 || wrote;
    },
    async leave(p, set) {
      // (the set first: it no longer lists the sheet; then the sheet's record, a draft; then its pieces and lines. The server's keepListed lets it go, its record no longer being in the set)
      const after = Object.assign({}, set, { sheetIds: (set.sheetIds || []).filter(id => id !== p.sheetId), labelFiles: (set.labelFiles || []).filter(f => f && f.sheetId !== p.sheetId) });
      const r1 = await setWrite(ctx, set, Core.setPatch(after), `leave-set|${set.setId}|${p.sheetId}`);   // (FRESHWRITE)
      if (!r1 || r1.status >= 400) throw new Error(`${p.fileBase || p.sheetId} could not leave ${set.name}: ${answerWords(r1)}`);
      set.sheetIds = after.sheetIds; set.labelFiles = after.labelFiles;
      // (tendSet's own save: solidIncluded is the sheet's tick after it left, false for a 10K / 14K sheet a person took out, nothing for every other metal)
      const r2 = await ctx.call({ op: "putSheet", sheet: { id: p.sheetId, draft: true, setId: null, setSeq: null, sheetIndex: null, label: null, solidIncluded: ["gold10k", "gold14k"].includes(p.metal) ? false : null } }, `leave-sheet|${p.sheetId}|${set.setId}`);
      if (!r2 || r2.status >= 400) throw new Error(`${p.fileBase || p.sheetId}: set membership was not saved: ${answerWords(r2)}`);
      const ids = (p.charms || []).map(c => c.poolId).filter(Boolean);
      // (its pieces stay on the sheet that lists them: their sheetId is kept, so no intake ever places them a second time; only their set and state go back)
      for (let i = 0; i < ids.length; i += 400) await ctx.call({ op: "poolUpdate", poolIds: ids.slice(i, i + 400), patch: { setId: null, state: "ready" } }, `leave-pool|${p.sheetId}|${set.setId}|${i}`);
      const lines = {}; for (const [k, l] of Object.entries(run.lines || {})) if (l && l.state === "written" && (l.poolIds || []).some(id => ids.includes(id))) lines[k] = { state: "pooled" };
      if (Object.keys(lines).length) await ctx.call({ op: "runLinesPatch", runId, lines }, `leave-lines|${p.sheetId}|${set.setId}`);
      say("ok", `${p.fileBase || p.sheetId} left ${set.name}: a person took it out`);
    },
    async saveSet(set) {
      const patch = Core.setPatch(set);
      const r = await setWrite(ctx, set, patch, `set-save|${set.setId}`);   // (FRESHWRITE: refused when a person changed the set since this look read it: the look is made again)
      if (!r || r.status >= 400) throw new Error(`${set.name || set.setId} was not saved: ${answerWords(r)}`);
    }
  };
  return io;
}

/** The assembly's memory as it should be after a look: written only when it changed (one write, not one a pass). */
async function saveJournal(ctx, V, G, extra) {
  const res = G.selfCheck(V.run);
  const checks = res ? { ok: !!res.ok, at: V.now, violations: (res.violations || []).slice(0, 40).map(v => ({ id: v.id, rule: v.rule, set: v.set, message: String(v.message || "").slice(0, 400) })), lines: (res.lines || []).slice(0, 20) } : null;
  const patch = { waits: G.waitsOf(), joinRefused: Object.fromEntries([...V.joinRefused].map(([k, v]) => [k, v])), clock: V.clock, checks, ...(extra || {}) };
  const was = V.journal || {}, same = hashOf([was.waits || {}, was.joinRefused || {}, was.clock || {}, was.checks ? { v: was.checks.violations } : null]) === hashOf([patch.waits, patch.joinRefused, patch.clock, checks ? { v: checks.violations } : null]);
  if (same && !extra) return false;
  const r = await ctx.call({ op: "assemblyStatePut", runId: V.run.runId, patch }, `assembly-state|${V.run.runId}`);
  if (r && r.status < 400) { V.journal = Object.assign({}, was, patch); if (checks && checks.violations.length && hashOf(was.checks && was.checks.violations) !== hashOf(checks.violations)) say("warn", `Set check: ${checks.violations.length} rule(s) of a Set of Sheets broken · ${checks.lines.join(" · ")}`); }
  return true;
}

/** The unit: one look at the run's sets (Gate.assemble). Used by the checkpoint step (after intake and the retire of empty sheets) and by a labels ask. */
async function assemblyUnit(ctx, o) {
  ctx = passKeyed(ctx);
  const run = ctx.run; if (!run || run.releasePolicy !== 2) return undefined;   // (a run that is not modern is the page's legacy path: the driver refuses such a run for the cloud)
  // (FRESHWRITE, 27 Nov 2026: a look whose write the server refused because a person changed the record since (replan) is made again at once on the records as they are now, at most three
  //  times; every write of a look is idempotent by key, so the look made again finishes what the refused one began. Still refused: { more: true }, the driver runs it again)
  for (let look = 0; look < 3; look++) {
    const V = await loadView({ db: ctx.db, run, now: Date.now() }), G = Core.gate(V), rows = new Map();
    // (G3, Paul 21 Nov 2026: a person's Library drop of a sheet on an open set, asked from a viewer: that set is the sheet's hand set for this look, as the page's joinSet marks it
    //  (sh.handSet): the core lets the set a person chose take it, by every other rule as before; the core forgets it at the end of the look)
    if (o && o.hand && o.hand.sheetId && o.hand.setId) { const p = V.pages.find(q => q.sheetId === String(o.hand.sheetId)); if (p) p.handSet = String(o.hand.setId); }
    // (RGTWO: a person's own Include / Cut Sheet press on this sheet: it may be a set's second partial Rose Gold sheet, charm-nest-assembly-core.js rgSecond; forgotten at the end of the look)
    if (o && o.press && o.press.sheetId) { const p = V.pages.find(q => q.sheetId === String(o.press.sheetId)); if (p) p.rgPress = true; }
    const io = cloudIo(ctx, V, rows);
    let out;
    try { out = await G.assemble(io); }
    catch (e) { if (e && e.replan) { say("ok", `the set look is made again: ${String(e.message || "").slice(0, 300)}`); continue; } throw e; }
    await saveJournal(ctx, V, G, { lastLook: { at: V.now, formed: out.formed || 0, sets: out.sets || [], heldSheets: num(out.heldSheets) } });
    return out.deferred ? { wait: "task", why: "The next set's number was deferred: the sets wait for the next look" } : undefined;
  }
  return { more: true, replan: true };
}

/* ── SHADOW (m:C7): the cloud plans the look a computer is carrying out, writes nothing but its own notes, and says how long its plan has agreed with what the computer did ── */
function dryIo(V, notes) {
  return {
    say: () => {},
    pace: async () => { await turn(); },
    async ensureSet(o) { notes.push({ kind: "newSet", follow: o && o.follow ? o.follow.setId : null }); const seq = Math.max(0, ...V.sets.filter(s => s.day === V.today).map(s => num(s.seq))) + 1, set = Core.asSet({ setId: `shadow-${V.today}-${seq}`, seq, day: V.today, runId: V.run.runId, group: "dispatch", orders: {}, sheetIds: [], status: "open" }); V.sets.push(set); return set; },
    async stampReleased() {},
    async join(p, set) { notes.push({ kind: "join", sheetId: p.sheetId, sheet: p.fileBase || p.sheetId, setId: set.setId, set: set.name }); return { ok: true }; },
    async joined() { return false; },
    async leave(p, set) { notes.push({ kind: "leave", sheetId: p.sheetId, sheet: p.fileBase || p.sheetId, setId: set.setId, set: set.name }); },
    async saveSet() {}
  };
}
/** shadow({ db, runId, now }) → { differs, at }: written to Charm_Nest_Assembly/<runId>.shadow directly (the cloud's own notes, like its lease: no production write, nothing fenced). */
async function shadow(o) {
  const { db, runId } = o, now = num(o.now) || Date.now();
  const rs = await db.collection("Charm_Nest_Runs").doc(runId).get(); if (!rs.exists) return { skipped: "no run" };
  const run = Object.assign({ runId }, rs.data());
  if (run.releasePolicy !== 2 || ["complete", "abandoned", "archived"].includes(String(run.status || ""))) return { skipped: "not a modern open run" };
  // (the run's lines, for SPLITWAIT: read as runGet reads them, from the live parts)
  if (run.liveLines && Array.isArray(run.liveLines.ids) && run.liveLines.ids.length) { const lines = {}; for (const d of await db.getAll(...run.liveLines.ids.map(x => db.collection("Charm_Nest_Run_Live").doc(x)))) if (d.exists) { try { Object.assign(lines, JSON.parse(d.data().json || "{}")); } catch (_) { /* unreadable: left out */ } } run.lines = lines; }
  const V = await loadView({ db, run, now }), G = Core.gate(V), notes = [];
  await G.assemble(dryIo(V, notes));
  const ref = db.collection(COLLECTIONS.ASSEMBLY).doc(runId), was = (V.journal && V.journal.shadow) || {};
  const differs = notes.slice(0, 60), key = hashOf(differs);
  const shadowDoc = { at: now, differs, key, lastDiffAt: differs.length ? now : num(was.lastDiffAt), cleanSince: differs.length ? 0 : num(was.cleanSince) || now, looks: num(was.looks) + 1, diffLooks: num(was.diffLooks) + (differs.length ? 1 : 0) };
  await ref.set({ runId, shadow: shadowDoc, shadowWaits: G.waitsOf(), updatedAt: Date.now() }, { merge: true });
  return { differs: differs.length, at: now };
}

module.exports = { loadView, cloudIo, assemblyUnit, shadow, saveJournal, poolRows, placedOf, passKeyed, setWrite, moved, replan };
