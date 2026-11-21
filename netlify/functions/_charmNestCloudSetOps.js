/*  netlify/functions/_charmNestCloudSetOps.js — the ops build step 10 adds (cloud plan v2 step 10a "sets, QR labels and the checkpoint archive", step 10b "the green line and
 *  the cut"; hard-parts §4.3-4.4). One module, joined to charmNestLibrary's OPS by ONE line (hard-parts §0.2), so ten copies of the library merge by adding lines.
 *
 *    assemblyStatePut  CLOUD_ONLY  the assembly's memory, Charm_Nest_Assembly/<runId>: the lines each sheet's card says (waits), the joins the cloud refused for ten minutes
 *                                  (joinRefused, the page's STUCKSPIN map), the clocks a sheet's releasedAt / joinedAt are ticked from, the owner-rule self-check, the shadow plan
 *    assemblyGet       READ        that document and the green line's (Charm_Nest_Green/<runId>), for every page's cards
 *    greenStatePut     CLOUD_ONLY  the automatic cut's memory, Charm_Nest_Green/<runId> { tried, oneTried, clearHeld, said } (the page's autoCutTried, rgOneTried, rgOneSay, run.rgClearHeld)
 *    labelsAsk         PRESS       a person's press that a set's labels or files, or a sheet's own QR label, be made again: Charm_Nest_Label_Asks/<set-… | sheet-…>
 *                                  { kind: "relabel" | "setFiles" | "complete" } ("complete": the sheet window's Make QR label, the one way a person completes a partial sheet)
 *    labelsAskGet      READ        the asks of a set or a sheet (their state and words)
 *    labelsAskPut      CLOUD_ONLY  the cloud's answer to an ask (working, done, refused with words)
 *    roseCutAsk        PRESS       a person's Cut Sheet / Approve & cut green line on a Rose Gold, 10K or 14K sheet: Charm_Nest_Cut_Asks/<sheetId> { kind: "cut" | "joinFull" | "line" (item 4) }; and (item 7)
 *                                  its In current set switch pressed on a viewer { kind: "include" | "exclude" } (_charmNestGreenLine.js member); and (G3) a Library move of ANY sheet
 *                                  of an open run on a viewer, the same kinds, with the set it was dropped on { setId?, newSet?, via: "library" }
 *                                  is cut already put into its set on a viewer { kind: "join", setId?: the set it was dropped on } (_charmNestGreenLine.js joinCut: never cut again)
 *    roseCutAskGet     READ        those asks, for the cards
 *    roseCutAskPut     CLOUD_ONLY  the cloud's answer (working, done with the cut, refused with the server's words)
 *    runLinesRetire    PRODUCTION  the lines of the orders the run is done with leave the run's live lines once its line archive holds each of them exactly as it is (RunCtl recordOf)
 *
 *  DORMANT: no page sends any of them yet; the cloud sends its own only from the driver's units, which run only when Charm_Nest_Open/drive is in "standby" or "on" AND the
 *  lane is in drive.cloudSteps. A press (labelsAsk, roseCutAsk) only writes its ask and kicks the driver (driveSoon, which never dispatches while the drive is off).
 *  No Etsy call, no world build, no Storage write here.                                                                                                                           */
"use strict";

const ASSEMBLY = "Charm_Nest_Assembly", GREEN = "Charm_Nest_Green", LABEL_ASKS = "Charm_Nest_Label_Asks", CUT_ASKS = "Charm_Nest_Cut_Asks";
const SANDBOXED_ADD = [ASSEMBLY, GREEN, LABEL_ASKS, CUT_ASKS];
const DOC_BYTES = 900000;
const ASK_KINDS = new Set(["relabel", "setFiles", "complete"]), CUT_KINDS = new Set(["cut", "joinFull", "line", "include", "exclude", "join"]);   // (VIEWINCLUDE, item 7, Paul 21 Nov 2026: include / exclude, a viewer's In current set switch; G4: join, a cut sheet into its set)
const GREEN_METALS = new Set(["rose", "gold10k", "gold14k"]);

module.exports = function (deps) {
  const { db, col, FV, fenceTx, isId, str, num, revTouch, liveParts, RUNS, RUN_LIVE, SHEETS, SETS, O } = deps;
  const isPlain = v => !!v && typeof v === "object" && !Array.isArray(v);
  const bytes = v => Buffer.byteLength(JSON.stringify(v || {}));
  const fence = async tx => { if (typeof fenceTx === "function") await fenceTx(tx); };

  /* ── the assembly's memory ── */
  const ASSEMBLY_FIELDS = ["waits", "joinRefused", "checks", "shadow", "notes", "said", "labels", "lastLook"];
  async function op_assemblyStatePut(b) {
    const runId = str(b.runId, 80); if (!isId(runId)) return { error: "bad run id" };
    const patch = isPlain(b.patch) ? b.patch : {}, ref = col(ASSEMBLY).doc(runId);
    await db.runTransaction(async tx => {
      await fence(tx);
      const ex = await tx.get(ref), old = ex.exists ? ex.data() || {} : {}, doc = { runId, updatedAt: FV.serverTimestamp() };
      for (const k of ASSEMBLY_FIELDS) if (Object.prototype.hasOwnProperty.call(patch, k)) doc[k] = isPlain(patch[k]) || Array.isArray(patch[k]) ? patch[k] : null;
      // (the clocks only go up: a pass that read an older copy can never tick a sheet's time back)
      if (isPlain(patch.clock)) { const c = Object.assign({}, old.clock || {}); for (const [k, v] of Object.entries(patch.clock)) if (/^\w{1,40}$/.test(k)) c[k] = Math.max(num(c[k]), num(v)); doc.clock = c; }
      if (bytes(Object.assign({}, old, doc)) > DOC_BYTES) { delete doc.checks; delete doc.shadow; if (doc.waits && bytes(doc.waits) > DOC_BYTES / 2) doc.waits = { truncated: true }; }
      tx.set(ref, doc, { merge: true });
    });
    revTouch(b, []);
    return { ok: true, runId };
  }
  async function op_assemblyGet(b) {
    const runId = str(b.runId, 80); if (!isId(runId)) return { error: "bad run id" };
    const [a, g] = await db.getAll(col(ASSEMBLY).doc(runId), col(GREEN).doc(runId));
    return { ok: true, assembly: a.exists ? a.data() : null, green: g.exists ? g.data() : null };
  }

  /* ── the green line's memory (hard-parts §4.4) ── */
  const GREEN_FIELDS = ["tried", "oneTried", "clearHeld", "said"];
  async function op_greenStatePut(b) {
    const runId = str(b.runId, 80); if (!isId(runId)) return { error: "bad run id" };
    const patch = isPlain(b.patch) ? b.patch : {}, ref = col(GREEN).doc(runId), doc = { runId, updatedAt: FV.serverTimestamp() };
    for (const k of GREEN_FIELDS) if (Object.prototype.hasOwnProperty.call(patch, k)) doc[k] = isPlain(patch[k]) ? patch[k] : {};
    if (bytes(doc) > DOC_BYTES) return { error: "the green line's memory is too large to keep", status: 413 };
    await db.runTransaction(async tx => { await fence(tx); tx.set(ref, doc, { merge: true }); });
    revTouch(b, []);
    return { ok: true, runId };
  }

  /* ── labelsAsk: a person's press for a set's labels and files, or a sheet's own QR label ── */
  const askKey = (setId, sheetId) => (setId ? "set-" + setId : "sheet-" + sheetId);
  async function op_labelsAsk(b) {
    const kind = str(b.kind, 20), by = str(b.by, 80).trim(), dev = str(b.dev, 60), setId = str(b.setId, 80), sheetId = str(b.sheetId, 80);
    if (!ASK_KINDS.has(kind)) return { error: "ask for relabel, setFiles or complete", status: 400 };
    if (!by) return { error: "Say who is asking", status: 400 };
    if (kind === "complete" ? !isId(sheetId) : !(isId(setId) || isId(sheetId))) return { error: kind === "complete" ? "name the sheet" : "name the set or the sheet", status: 400 };
    const key = kind === "complete" ? askKey("", sheetId) : askKey(isId(setId) ? setId : "", sheetId), ref = col(LABEL_ASKS).doc(key);
    const out = await db.runTransaction(async tx => {
      const of = isId(setId) && kind !== "complete" ? await tx.get(col(SETS).doc(setId)) : await tx.get(col(SHEETS).doc(sheetId));
      if (!of.exists) return { error: kind === "complete" || !isId(setId) ? "That sheet is no longer in the Library" : "That set is not in the cloud", status: 404 };
      const d = of.data() || {}, runId = d.runId || null;
      const ex = await tx.get(ref), old = ex.exists ? ex.data() || {} : null;
      if (old && (old.state === "asked" || old.state === "working") && old.kind === kind) return { ok: true, ask: old, again: true };
      const ask = { key, kind, setId: isId(setId) && kind !== "complete" ? setId : null, sheetId: isId(sheetId) ? sheetId : null, runId, by, dev, at: Date.now(), seq: num(old && old.seq) + 1, state: "asked", why: null, doneAt: null };
      tx.set(ref, Object.assign({}, ask, { updatedAt: FV.serverTimestamp() }));
      return { ok: true, ask };
    });
    revTouch(b, []);
    return out;
  }
  async function op_labelsAskGet(b) {
    const keys = [].concat(isId(b.setId) ? [askKey(b.setId, "")] : [], isId(b.sheetId) ? [askKey("", b.sheetId)] : []);
    if (!keys.length) return { error: "name the set or the sheet" };
    const docs = await db.getAll(...keys.map(k => col(LABEL_ASKS).doc(k)));
    return { ok: true, asks: docs.filter(d => d.exists).map(d => d.data()) };
  }
  async function op_labelsAskPut(b) {
    const key = str(b.key, 100); if (!/^(set|sheet)-[\w\-]{4,80}$/.test(key)) return { error: "bad ask" };
    const p = isPlain(b.patch) ? b.patch : {}, ref = col(LABEL_ASKS).doc(key);
    const out = await db.runTransaction(async tx => {
      await fence(tx);
      const ex = await tx.get(ref); if (!ex.exists) return { error: "that ask is gone", status: 404 };
      const old = ex.data() || {};
      if (num(b.seq) && num(b.seq) !== num(old.seq)) return { ok: true, stale: true };   // (a newer press since: it is answered by its own turn)
      const doc = { updatedAt: FV.serverTimestamp() };
      for (const k of ["state", "why", "doneAt", "step", "files"]) if (Object.prototype.hasOwnProperty.call(p, k)) doc[k] = p[k] == null ? null : k === "why" ? str(p[k], 600) : p[k];
      tx.set(ref, doc, { merge: true });
      return { ok: true };
    });
    revTouch(b, []);
    return out;
  }

  /* ── roseCutAsk (hard-parts §4.3): a person's Cut Sheet / Approve & cut green line ── */
  async function op_roseCutAsk(b) {
    const sheetId = str(b.sheetId, 80), kind = str(b.kind || "cut", 20), by = str(b.by, 80).trim(), dev = str(b.dev, 60);
    if (!isId(sheetId)) return { error: "bad sheet id", status: 400 };
    if (!CUT_KINDS.has(kind)) return { error: "ask for cut, joinFull, line, include, exclude or join", status: 400 };
    const member = kind === "include" || kind === "exclude";   // (VIEWINCLUDE: the sheet's own yes for its set, or its take-out: a cut sheet's place is answered by the cloud's set rules, in words)
    const join = kind === "join", want = join && isId(str(b.setId, 80)) ? str(b.setId, 80) : null;   // (G4, Paul 21 Nov 2026: a cut sheet joins its set by the set rules, never cut again)
    if (!by) return { error: "Say who is pressing Cut Sheet", status: 400 };
    const ref = col(CUT_ASKS).doc(sheetId);
    const out = await db.runTransaction(async tx => {
      const s = await tx.get(col(SHEETS).doc(sheetId)); if (!s.exists) return { error: "That sheet is no longer in the Library", status: 404 };
      const d = s.data() || {};
      if (!GREEN_METALS.has(d.metal) && !member) return { error: "Only a Rose Gold, 10K or 14K Gold sheet has a green line", status: 409 };   // (G3: a Library move of any sheet of an open run is an include / exclude ask too)
      if (d.roseCutAt && !member && !join) return { error: "This sheet's green line cut is recorded already", status: 409, cut: true };
      if (join && !d.roseCutAt) return { error: "This sheet's green line is not cut yet: press Cut Sheet (or Approve & cut green line) and it joins its set with its cut", status: 409 };   // (G4)
      if (num(d.laserDoneAt) > 0) return { error: "This sheet has been cut on the laser", status: 409 };
      const ex = await tx.get(ref), old = ex.exists ? ex.data() || {} : null;
      if (old && (old.state === "asked" || old.state === "working")) return { ok: true, ask: old, again: true };
      const ask = { sheetId, runId: d.runId || null, metal: d.metal, kind, by, dev, at: Date.now(), seq: num(old && old.seq) + 1, state: "asked", why: null, cutAt: null, stockId: null, revision: null, setId: want };   // (setId: G4, the set a join was dropped on, for the answer's words)
      // (G3, Paul 21 Nov 2026: a Library move on a viewer names the set it was dropped on (setId), or New set; the cloud's set rules decide, and its answer says where the sheet went)
      if (member && kind === "include") { const to = str(b.setId, 80); if (to && isId(to)) ask.setId = to; if (b.newSet === true) ask.newSet = true; }
      if (member && str(b.via, 20) === "library") ask.via = "library";
      tx.set(ref, Object.assign({}, ask, { updatedAt: FV.serverTimestamp() }));
      return { ok: true, ask };
    });
    revTouch(b, []);
    return out;
  }
  async function op_roseCutAskGet(b) {
    const ids = [...new Set((Array.isArray(b.sheetIds) ? b.sheetIds : [b.sheetId]).map(x => str(x, 80)).filter(isId))].slice(0, 100);
    if (!ids.length) return { error: "name the sheets" };
    const docs = await db.getAll(...ids.map(id => col(CUT_ASKS).doc(id)));
    return { ok: true, asks: docs.filter(d => d.exists).map(d => d.data()) };
  }
  async function op_roseCutAskPut(b) {
    const sheetId = str(b.sheetId, 80); if (!isId(sheetId)) return { error: "bad sheet id" };
    const p = isPlain(b.patch) ? b.patch : {}, ref = col(CUT_ASKS).doc(sheetId);
    const out = await db.runTransaction(async tx => {
      await fence(tx);
      const ex = await tx.get(ref); if (!ex.exists) return { error: "that ask is gone", status: 404 };
      if (num(b.seq) && num(b.seq) !== num((ex.data() || {}).seq)) return { ok: true, stale: true };
      const doc = { updatedAt: FV.serverTimestamp() };
      for (const k of ["state", "why", "cutAt", "stockId", "revision", "doneAt"]) if (Object.prototype.hasOwnProperty.call(p, k)) doc[k] = p[k] == null ? null : k === "why" ? str(p[k], 600) : p[k];
      tx.set(ref, doc, { merge: true });
      return { ok: true };
    });
    revTouch(b, []);
    return out;
  }

  /* ── runLinesRetire (architecture-v2 step 10a, the checkpoint's archive): the page's recordOf, on the server, for the cloud's own call ──
     { runId, retire: { lineKey: textHash(JSON of the line as the archive holds it) }, parts: the archive parts written for them }. In ONE transaction: the run's live lines are read; an
     order leaves them only when EVERY one of its lines is named with the hash of the line exactly as it is now (a line changed since it was archived stays, and is archived again
     next time); its lines leave the live parts, its id leaves orders, committed and holds; the sheet notes are trimmed to the newest O.RUN_RECORD.keepSheets; lineArchive counts it all. */
  async function op_runLinesRetire(b) {
    const id = str(b.runId, 80); if (!isId(id)) return { error: "bad run id" };
    const want = isPlain(b.retire) ? b.retire : null;
    const keys = want ? Object.keys(want).filter(k => /^[\w\-]{3,80}$/.test(k) && typeof want[k] === "string").slice(0, 5000) : [];
    if (!keys.length) return { error: "no lines to retire" };
    const parts = Math.max(0, Math.min(1000, Math.round(num(b.parts)))), ref = col(RUNS).doc(id);
    let gone = [];
    const refused = await db.runTransaction(async tx => {
      await fence(tx);
      const ex = await tx.get(ref); if (!ex.exists) return { error: "that run is not in the cloud", status: 404 };
      const old = ex.data() || {}, had = old.liveLines && Array.isArray(old.liveLines.ids) ? old.liveLines.ids : [];
      let lines = {};
      if (had.length) { const docs = await Promise.all(had.map(x => tx.get(col(RUN_LIVE).doc(x)))); if (docs.some(d => !d.exists)) throw new Error("the run was saved while its lines were read: try again"); for (const d of docs) Object.assign(lines, JSON.parse(d.data().json || "{}")); }
      else lines = Object.assign({}, old.lines || {});
      const asked = new Set(keys.filter(k => lines[k] && O.textHash(JSON.stringify(lines[k])) === want[k]));
      const byOrder = new Map();
      for (const [k, l] of Object.entries(lines)) { const o = l && l.orderId != null ? String(l.orderId) : ""; if (!o) continue; if (!byOrder.has(o)) byOrder.set(o, []); byOrder.get(o).push(k); }
      const out = new Set([...byOrder].filter(([, ks]) => ks.every(k => asked.has(k))).map(([o]) => o));
      gone = [...asked].filter(k => out.has(String(lines[k].orderId)));
      if (!gone.length) return null;
      for (const k of gone) delete lines[k];
      const isOut = x => out.has(String(x));
      const rec = { lineEditAt: Date.now(), updatedAt: FV.serverTimestamp(), lines: FV.delete() };
      let committed = 0, held = 0, sheets = 0;
      if (Array.isArray(old.orders)) rec.orders = old.orders.filter(x => !isOut(x));
      if (Array.isArray(old.committed)) { rec.committed = old.committed.filter(x => !isOut(x)); committed = old.committed.length - rec.committed.length; }
      if (isPlain(old.holds)) { const h = Object.assign({}, old.holds); for (const k of Object.keys(h)) if (isOut(k)) { delete h[k]; held++; } if (held) rec.holds = h; }
      if (isPlain(old.sheets)) { const notes = Object.keys(old.sheets), keep = (O.RUN_RECORD && O.RUN_RECORD.keepSheets) || 100; if (notes.length > keep) { const s = Object.assign({}, old.sheets); for (const k of notes.slice(0, notes.length - keep)) delete s[k]; rec.sheets = s; sheets = notes.length - keep; } }
      const la = isPlain(old.lineArchive) ? old.lineArchive : {};
      rec.lineArchive = { lines: num(la.lines) + gone.length, committed: num(la.committed) + committed, held: num(la.held) + held, sheets: num(la.sheets) + sheets, parts: num(la.parts) + parts, at: Date.now() };
      const live = liveParts(id, lines);
      if (live.some(p => p.bytes > 1000000)) return { error: "a piece of this run is too large to save" };
      const keepIds = new Set(live.map(p => p.id)), have = new Set(had);
      for (const p of live) if (!have.has(p.id)) tx.set(col(RUN_LIVE).doc(p.id), { runId: id, json: p.json, bytes: p.bytes, lines: p.lines, seq: p.seq, createdAt: FV.serverTimestamp() });
      for (const x of had) if (!keepIds.has(x)) tx.delete(col(RUN_LIVE).doc(x));
      rec.liveLines = { ids: live.map(p => p.id), lines: live.reduce((n, p) => n + p.lines, 0), bytes: live.reduce((n, p) => n + p.bytes, 0) };
      tx.set(ref, rec, { merge: true });
      return null;
    });
    if (refused) return refused;
    if (gone.length) revTouch(b, ["r:" + id]);
    else revTouch(b, []);
    return { ok: true, runId: id, retired: gone };
  }

  /* ── setReadiness (READ): the server's own readiness of a set's sheets, the part of the page's validateRelease that read CharmNestReadiness over the page's memory
     (bridge Sets.validateRelease: orderReports, orderBlockers, Readiness.set) and the cardinal rule's server form (splitOfSet, which setUpdate checks at completion).
     { setId } → { ok, ready, why, split: [{ orderId, there }] }. Reads only. */
  async function op_setReadiness(b) {
    const setId = str(b.setId, 80); if (!isId(setId)) return { error: "bad set id" };
    const ss = await col(SETS).doc(setId).get(); if (!ss.exists) return { ok: true, ready: false, why: "That set is not in the cloud" };
    const set = ss.data() || {}, ids = [...new Set((set.sheetIds || []).map(String))].filter(isId);
    if (!ids.length) return { ok: true, ready: false, why: "The set has no sheets" };
    const docs = []; for (let i = 0; i < ids.length; i += 100) docs.push(...await db.getAll(...ids.slice(i, i + 100).map(id => col(SHEETS).doc(id))));
    const records = [];
    for (const d of docs) if (d.exists && !d.data().archived) records.push(deps.withFullBacks ? await deps.withFullBacks(Object.assign({}, d.data(), { id: d.id, laserHold: null })) : Object.assign({}, d.data(), { id: d.id, laserHold: null }));
    let why = "";
    try { if (deps.productionReadiness) await deps.productionReadiness(records); } catch (e) { why = String((e && e.message) || e).slice(0, 300); }
    const R = deps.Readiness, r = R && R.set ? R.set(Object.assign({}, set, { setId }), records) : { ready: false };
    if (!why && R && R.orderBlockers && R.blockText) { const blocked = records.flatMap(x => R.orderBlockers(x)); if (blocked.length) why = `${blocked[0].id}: ${R.blockText(blocked[0])}`; }
    if (!why && !r.ready) { const st = r.stages || {}, miss = Object.keys(st).filter(k => !st[k]); why = miss.length ? `Set is not ready for laser: ${miss.join(", ")} not done` : "Set is not ready for laser: finish every sheet's engraving approvals, saved back files, layout checks and QR labels"; }
    let split = [];
    try { if (deps.splitOfSet) split = (await deps.splitOfSet(setId, ids)).map(it => ({ orderId: it.orderId, there: it.there || [] })); } catch (_) { split = []; }
    return { ok: true, ready: !!r.ready, why: r.ready ? "" : why, stages: r.stages || null, split };
  }

  const ops = { assemblyStatePut: op_assemblyStatePut, assemblyGet: op_assemblyGet, greenStatePut: op_greenStatePut, labelsAsk: op_labelsAsk, labelsAskGet: op_labelsAskGet, labelsAskPut: op_labelsAskPut,
    roseCutAsk: op_roseCutAsk, roseCutAskGet: op_roseCutAskGet, roseCutAskPut: op_roseCutAskPut, runLinesRetire: op_runLinesRetire, setReadiness: op_setReadiness };
  // (none of them moves a placement: the placement counter stays; runLinesRetire names its run, r:<run>, which pages follow)
  const noBump = ["assemblyStatePut", "assemblyGet", "greenStatePut", "labelsAsk", "labelsAskGet", "labelsAskPut", "roseCutAsk", "roseCutAskGet", "roseCutAskPut", "runLinesRetire", "setReadiness"];
  return { ops, noBump, sandboxed: SANDBOXED_ADD };
};
module.exports.COLLECTIONS = { ASSEMBLY, GREEN, LABEL_ASKS, CUT_ASKS };
