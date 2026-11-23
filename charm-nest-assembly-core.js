/*  charm-nest-assembly-core.js — Gate's set-forming decisions, on RECORDS (cloud plan v2, build step 10a: "the cloud carries every run").
 *  ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
 *  The rules of a Set of Sheets lived inside charm-nest-bridge.js's Gate (an IIFE over the page's memory: allSheets(), Sets, B.run), so only the computer
 *  that carried the run could form a set. They are here, line for line, working on the sheet and set RECORDS of a run, so the cloud forms the sets with
 *  the page's own rules and none of a copy. Nothing here reads a clock, the network or Firestore: a view V says everything (V.now is the clock), and the
 *  writes a decision needs are asked of an `io` the caller gives (the cloud's unit: netlify/functions/_charmNestAssembly.js).
 *
 *  Loads two ways, one source:  window.CharmNestAssemblyCore (a page)   module.exports (node: require("../../charm-nest-assembly-core.js"))
 *  It reads, the same way: charm-nest-set-rules.js (form, settle, closed, validSet, isCompleted, isPartial ...), charm-nest-shared-orders.js (its core:
 *  sheetOf, formation, greenHomes, splitWords ...), charm-nest-sheet-name.js (claim, raise, name), charm-nest-orders.js (sheetRelease, setLabel, setFolder,
 *  sheetName) and charm-nest-set-invariants.js (the owner-rule self-check).
 *
 *  THE OWNER'S RULES, each kept where the page kept it (the bridge line it was ported from is named beside each function):
 *    SETSHUT       a sheet in any set takes no new orders: placement's, not this file's; this file never adds a piece to a sheet.
 *    KEEPINSET     keepInSet: SetRules.isPartial reads it (a partial sheet the owner kept in its set is not partial there); the server's putSheet keeps it.
 *    OWNEROVERRIDE ownerSet: a sheet the owner put in (or took out of) a set is never moved by the assembly (ownerMoved below; the server's putSheet keeps it too).
 *    MINFLOOR      the set minimum is a floor: SetRules.surplus() returns []; nothing here takes a sheet out of a set for being more than the minimum (NOAUTOTRIM).
 *    NOSETPULL     the system never takes a sheet out of its set by itself: only a sheet a PERSON marked to leave (leftSet + draft) leaves it (tendSet).
 *    SETSTAYSCLOSED a set that has formed (formedAt, or it holds a completed GF and a completed SS sheet) takes no sheet by itself (closedSet).
 *    SPLITWAIT     no set forms while an order has GF / SS pieces waiting to be placed (waitingOf, from the run's lines).
 *    PAIR2         placement's (pairFits): this file reads the sheets as placement left them.
 *    R12 / greenHomes  a Rose Gold / 10K / 14K sheet is in ONE set, joins only by its own yes (solidPick) or its recorded cut, never by the cardinal pull alone
 *                  (goldGap); RG/10K/14K sheets mix only within one set.
 *    NOPARTIALSET  a partial GF / SS sheet never joins a set (SetRules.isPartial), except a person's own drop (refused to them by the server).
 *  ═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════ */
/*  GATECORE (Paul, 20 Nov 2026: the page and the cloud decide sets with ONE copy): the page's Gate (charm-nest-bridge.js) now calls this file for every decision of
 *  the set assembly (policy, the cardinal marks, the green homes, SPLITWAIT, which sheets form a set and when, tendSet's joins and the person's take-outs); the page
 *  keeps only its writes, its cards and its messages, given as `io` and a LIVE view (V.live: its own sheets are the truth, so no record fields are read or written).
 *  The page's view also hands in what only a page has (V.modules: its window modules; V.ofRun: Sets.ofRun; V.rows: Orders.rows(); V.selected, V.modern, V.leaving,
 *  V.sheetFull: RoseStock.full; V.locale; V.hint). The cloud's view gives none of these and reads the records, as before.                                        */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestAssemblyCore = api;
})(typeof self !== "undefined" ? self : this, function (root) {
  "use strict";
  // (each module by its literal path, so the cloud's bundler (esbuild) packs it with the function; a page never reaches require)
  const need = (name, load) => { if (root && root[name]) return root[name]; if (typeof require === "function") { try { return load(); } catch (_) { return null; } } return null; };
  const SetRules = () => need("CharmNestSetRules", () => require("./charm-nest-set-rules.js"));
  const SharedOrders = () => need("SharedOrders", () => require("./charm-nest-shared-orders.js"));
  const SheetName = () => need("CharmNestSheetName", () => require("./charm-nest-sheet-name.js"));
  const Orders = () => need("CharmNestOrders", () => require("./charm-nest-orders.js"));
  const Invariants = () => need("CharmNestSetInvariants", () => require("./charm-nest-set-invariants.js"));
  const CustomPool = () => need("CharmNestCustomPool", () => require("./charm-nest-custom-pool.js"));
  const Place = () => need("CharmNestPlace", () => require("./charm-nest-place.js"));

  const METAL_TAG = { gold: "GF", silver: "SS", rose: "RG", gold10k: "10K", gold14k: "14K" };
  const METAL_WORD = { gold: "Gold Filled", silver: "Sterling Silver", rose: "Rose Gold", gold10k: "10K Gold", gold14k: "14K Gold" };
  const labelOf = m => METAL_WORD[m] || String(m || "");
  const MANUAL_STALE_MS = 90000, HOLD_MS = 600000, JOIN_REFUSED_MS = 600000;
  const num = v => (Number.isFinite(+v) ? +v : 0);
  const parse = s => { if (!s) return null; if (typeof s === "object") return s; try { return JSON.parse(s); } catch (_) { return null; } };
  const ms = v => (v && typeof v.toMillis === "function" ? v.toMillis() : v && typeof v === "object" && v._seconds ? v._seconds * 1000 : num(v));
  const setOrder = (a, b) => String(a.day || "").localeCompare(String(b.day || "")) || (num(a.seq) - num(b.seq));
  const torontoDay = t => { try { return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(t)); } catch (_) { return new Date(t).toISOString().slice(0, 10); } };

  /* ══ 1 · records as the page's pages (the restore of charm-nest-bridge.js ~17160-17171, and hard-parts §0.3's asPage) ════════════════════════════ */
  /** The set a person took this saved sheet out of (bridge Gate.leftSetOf): a hold on it whose history's last word on its membership is "taken out of" that set. */
  function leftSetOf(d) {
    const h = d && d.laserHold;
    if (!h || !(+h.at > 0) || (d.setId && !d.draft)) return null;
    const e = [...(Array.isArray(d.flowHistory) ? d.flowHistory : [])].reverse().find(x => x && (x.type === "setLeave" || x.type === "setJoin"));
    if (!e || e.type !== "setLeave" || !e.setId) return null;
    return String(e.setId);
  }
  const goldFamilyMetal = m => m === "rose" || m === "gold10k" || m === "gold14k";
  /** One sheet record as the page holds the same sheet after a restore (field names the Gate rules read). o: { run, setsById: Map(setId -> set), now }. */
  function asPage(rec, o) {
    const now = num(o && o.now) || 0, run = (o && o.run) || {}, setsById = (o && o.setsById) || new Map();
    const set = rec.draft ? null : (rec.setId && setsById.get(String(rec.setId))) || null;
    const outputs = rec.outputs && typeof rec.outputs === "object" && rec.outputs.ai ? rec.outputs : null;
    const aiDir = outputs && outputs.ai && outputs.ai.path ? String(outputs.ai.path).replace(/\/[^/]+$/, "") : "";
    const p = {
      id: rec.id, sheetId: rec.id, runId: rec.runId || null, metal: rec.metal, page: num(rec.page) || 1,
      ...(rec.draftOrder != null && Number.isFinite(+rec.draftOrder) ? { draftOrder: +rec.draftOrder } : {}),
      draft: !!rec.draft || !set, recSetId: rec.setId || null, recDraft: !!rec.draft,
      setId: set ? set.setId : null, seq: set ? num(rec.setSeq) || set.seq : null, setDay: (set && set.day) || rec.day || null, day: rec.day || null, group: set ? set.group || null : "dispatch",
      sheetIndex: set ? rec.sheetIndex : null, fileBase: rec.fileBase || null,
      folderPath: aiDir || (set ? `${set.folder}/${rec.fileBase}` : `charmnest/sheets/${rec.day}/${rec.fileBase}`),
      label: set ? rec.label || null : null, backPool: Array.isArray(rec.backPool) ? rec.backPool : [],
      charms: Array.isArray(rec.charms) ? rec.charms : [], placements: Array.isArray(rec.placements) ? rec.placements : [], poolIds: Array.isArray(rec.poolIds) ? rec.poolIds : [],
      orders: Array.isArray(rec.orders) ? rec.orders : [],
      releaseFull: !!rec.releaseFull, laserDoneAt: num(rec.laserDoneAt) || null, intakeFinalized: !!rec.intakeFinalized, intakeOptimized: !!rec.intakeOptimized, topup: rec.topup || null,
      verification: rec.verification || null, status: rec.status || null, dirty: !!rec.dirty, endedBy: rec.endedBy || null,
      outputs, cloud: outputs, persisted: true, persistedDone: !!outputs && !rec.saving && !(num(rec.filesOwed) > 0),
      density: rec.density, placedCount: rec.placedCount, stock: rec.stock || null,
      roseCutAt: num(rec.roseCutAt) || null, rosePlan: parse(rec.rosePlanJson), roseProtected: parse(rec.roseProtectedJson), rosePlanHash: rec.rosePlanHash || null,
      roseFingerprint: rec.roseFingerprint || null, roseStockId: rec.roseStockId || null, roseRevision: rec.roseRevision, roseAllowanceMm: rec.roseAllowanceMm,
      autoMatched: rec.autoMatched || null, autoCutOff: !!rec.autoCutOff, autoCutWhy: rec.autoCutWhy || null,
      releasedAt: num(rec.releasedAt) || 0, joinedAt: num(rec.joinedAt) || 0, ...(rec.joinedBy ? { joinedBy: rec.joinedBy } : {}),
      keepRelease: rec.keepRelease && typeof rec.keepRelease === "object" ? rec.keepRelease : null, keepInSet: rec.keepInSet || null, ownerSet: rec.ownerSet || null,
      _manualHold: !!(rec.manualEdit && typeof rec.manualEdit === "object" && now - num(rec.manualEdit.at) < MANUAL_STALE_MS),
      _inplaceHold: !!(rec.inplaceHold && now - num(rec.inplaceHold.at) < HOLD_MS),
      runHold: rec.runHold || (run.sheetHolds && run.sheetHolds[rec.id]) || null,
      recalled: rec.archived === true, cutCheck: rec.cutCheck || null, problem: rec.problem || null,
      nestJob: rec.nestJob || null, filesJob: rec.filesJob || null, filesOwed: num(rec.filesOwed) || 0, saving: !!rec.saving, layoutRev: num(rec.layoutRev), contentRev: num(rec.contentRev),
      solidIncluded: rec.solidIncluded, flowHistory: rec.flowHistory, laserHold: rec.laserHold || null, processReady: !!rec.processReady,
      cardStartedAt: rec.cardStartedAt || null, createdAt: rec.createdAt || null   // (the line order a reload restores: charm-nest-place.js lineOrder, RGREST's "a later Rose Gold sheet")
    };
    if (goldFamilyMetal(rec.metal) && rec.solidIncluded === true) p.solidPick = true;   // (a 10K / 14K / Rose Gold sheet's own Include tick)
    p.leftSet = leftSetOf(rec); if (p.leftSet && goldFamilyMetal(rec.metal)) p.solidPick = false;
    if (!p.leftSet && rec.leftSet) { p.leftSet = String(rec.leftSet); if (goldFamilyMetal(rec.metal)) p.solidPick = false; }
    return p;
  }

  /* ══ 2 · the Gate, over a view ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════
     V: { run (its record: runId, releasePolicy, solidIncluded, lines, sheetHolds, rgClearHeld), pages: [asPage], sets: [set record, mutable], now,
          joinRefused: Map(sheetId -> { setId, at, why }), clock: { completion, join }, fileBase(page) -> its set file name }
     Every function below is the bridge's of the same name, with allSheets() = V.pages, Sets.ofRun = V.sets and B.run = V.run.
     (GATECORE) allSheets() is every sheet the view holds, as the page's allSheets() is (a recalled page too; the cloud's view holds no archived record), and the rules that
     leave a recalled sheet out say so where the page's did. */
  function gate(V) {
    const M = typeof V.modules === "function" ? V.modules() || {} : null, mod = (k, f) => (M ? M[k] || null : f());   // (a page's own modules, exactly what it holds; the cloud's by require)
    const SR = mod("SetRules", SetRules), SOapi = mod("SharedOrders", SharedOrders), SN = mod("SheetName", SheetName), O = mod("Orders", Orders), PL = mod("Place", Place), INV = mod("Invariants", Invariants);
    const SOcore = SOapi && SOapi.core ? SOapi.core : SOapi;
    const live = V.live === true;   // (the page: its sheets ARE the records; the cloud: a sheet's record membership is recSetId / recDraft)
    const allSheets = () => V.pages;
    const ofRun = typeof V.ofRun === "function" ? runId => V.ofRun(runId) || [] : runId => V.sets.filter(s => s && s.runId === runId).sort(setOrder);
    const modern = typeof V.modern === "function" ? runId => V.modern(runId) : runId => !!V.run && V.run.releasePolicy === 2 && (!runId || runId === V.run.runId);
    const selected = () => (typeof V.selected === "function" ? V.selected() : (V.run && V.run.solidIncluded)) || {};
    const solid = m => ["gold10k", "gold14k"].includes(m);
    const goldFamily = m => m === "rose" || solid(m);
    const inSetNow = sh => !!sh.setId && !sh.draft;
    const picked = (sh, choices = selected()) => typeof sh.solidPick === "boolean" ? sh.solidPick : sh.metal === "rose" ? inSetNow(sh) : choices[sh.metal] === true;
    const staysCut = sh => goldFamily(sh.metal) && !!sh.roseCutAt && inSetNow(sh);
    const leaving = V.leaving || new Set();   // (mergeLeaving: a person's Merge's sheets, the page's WeakSet; never in the cloud's view)
    const warn = (...a) => { try { console.warn(...a); } catch (_) { /* a note only */ } };
    const staysJoined = sh => solid(sh.metal) && sh.solidPick === true && inSetNow(sh) && !sh.leftSet && !leaving.has(sh);
    const staysWaiting = sh => sh.metal === "rose" && !sh.roseCutAt && !(+sh.laserDoneAt > 0) && sh.solidPick === true && inSetNow(sh) && !sh.leftSet && !!sh.autoMatched && String(sh.autoMatched) === String(sh.setId) && !leaving.has(sh);
    const staysInSet = sh => staysCut(sh) || staysJoined(sh) || staysWaiting(sh);
    /* OWNEROVERRIDE (Paul, 20 Nov 2026): a sheet whose set the owner decided (ownerSet) is never put into another set, or taken out, by the assembly: the server keeps the owner's set
       (op_putSheet), and the cloud never asks. */
    const ownerMoved = sh => !!(sh.ownerSet && typeof sh.ownerSet === "object");
    const ownerWants = (sh, set) => !ownerMoved(sh) || String(sh.ownerSet.setId || "") === String(set && set.setId || "");
    const ordersOn = sh => { const ids = new Set((sh.placements || []).map(p => p.id)); return new Set((sh.charms || []).filter(c => ids.has(c.id) && c.order).map(c => String(c.order).split("/")[0])); };
    const topupOrders = () => num(V.topupOrders) || 35;   // (charm-nest-1.html TOPUP.orders: the page hands its own in)
    function basePolicy(sh, seq, choices = selected()) {
      if (sh.leftSet && !inSetNow(sh)) return { include: false, reason: "Taken out of its set" };
      if (sh.roseCutAt && staysCut(sh)) return { include: true, reason: "Cut · stays in its set" };
      if (staysJoined(sh)) return { include: true, reason: "Approved · stays in its set" };
      if (staysWaiting(sh)) return { include: true, reason: "Waiting for its green line cut · stays in its set" };
      return O.sheetRelease({ material: sh.metal, verified: !!(sh.verification && sh.verification.ok), placed: (sh.placements || []).length,
        stopped: sh.endedBy === "stopped", dirty: sh.dirty || ["nesting", "finishing", "queued"].includes(sh.status), full: !!sh.releaseFull, topup: sh.topup && !sh.topup.closedAt ? { tried: (sh.topup.tried || []).length, of: topupOrders() } : false },
        { seq, selected: solid(sh.metal) ? Object.assign({}, choices, { [sh.metal]: picked(sh, choices) }) : sh.metal === "rose" ? Object.assign({}, choices, { rose: picked(sh, choices) ? true : undefined }) : choices });
    }
    function policy(sh, seq, choices = selected()) {
      const r = basePolicy(sh, seq, choices);
      if (sh.setWait && r.include) return Object.assign({}, r, { include: false, reason: sh.setWait });
      if (sh.cardinalHold && r.include) return Object.assign({}, r, { include: false, reason: sh.cardinalHold });
      if (sh.cardinalPull && !r.include) return Object.assign({}, r, { include: true, reason: sh.cardinalPull });
      if (sh.greenAway && r.include && inSetNow(sh)) return Object.assign({}, r, { include: false, reason: sh.greenAway });
      return r;
    }
    const holding = p => !!p.keepRelease && V.now - (+p.keepRelease.at || 0) < HOLD_MS;
    const joinRefusedOf = (sh, set) => { const r = sh && V.joinRefused && V.joinRefused.get(sh.sheetId); if (!r) return ""; if (r.setId !== set.setId || V.now - num(r.at) > JOIN_REFUSED_MS) { V.joinRefused.delete(sh.sheetId); return ""; } return r.why; };
    // (STUCKSPIN: a member being written again counts as the full sheet its record holds: finishNest's _fullWhileWritten on a page, the sheet window's keepRelease on both)
    const writtenFull = p => !p.releaseFull && ((!!p._fullWhileWritten && (["nesting", "finishing", "queued"].includes(p.status) || (!!p.persisted && !p.persistedDone))) || (holding(p) && !!p.keepRelease.full));
    const formMembers = set => membersOfSet(set).map(p => (writtenFull(p) ? Object.assign({}, p, { releaseFull: true }) : p));
    const sheetNameOf = p => (SN ? SN.name(p) : `${labelOf(p.metal)} Sheet ${p.page}`);
    const ordersWords = (os, tell) => os.length === 1 ? `order ${os[0]}${tell && tell(os[0]) ? ` (${tell(os[0])})` : ""}` : `orders ${os.slice(0, 3).join(", ")}${os.length > 3 ? " and more" : ""}`;
    const tellOf = (W, ids) => oid => { const sd = new Set(); for (const id of ids) { const n = W.norm.get(id); for (const p of (n && n.pieces) || []) if (p.orderId === oid && p.side) sd.add(p.side); } return sd.has("L") && sd.has("R") ? "its left and right earrings" : ""; };
    const setDocOf = (id, runId) => ofRun(runId).find(s => s.setId === id) || null;
    const pageId = sh => (sh && (sh.sheetId || `${sh.runId || "run"}:${sh.metal}:${sh.page || 1}`)) || "";   // (SharedOrders.fromPage: a page not saved yet is read too)
    const sideOk = s => (s === "L" || s === "R") ? s : null;
    /** SharedOrders fromPage, for a page of this view (the page's own reads: Gate.solidSelected, Sets.ofRun). */
    function fromPage(sh) {
      if (!sh || sh.recalled || !SOcore) return null;
      const byId = new Map((sh.charms || []).map(c => [c.id, c])), placed = new Set((sh.placements || []).map(p => p.id));
      const keys = [...placed].map(i => byId.get(i)).filter(Boolean).map(c => c.poolId).filter(Boolean);
      const pieceSides = {}; for (const i of placed) { const c = byId.get(i), sd = c && sideOk(c.side || (c.orderInfo && c.orderInfo.side)); if (c && c.poolId && sd) pieceSides[c.poolId] = sd; }
      const isSolid = solid(sh.metal), on = isSolid ? (picked(sh) || !!sh.cardinalPull || staysCut(sh)) : true;
      const rec = { id: pageId(sh), metal: sh.metal, page: sh.page, sheetIndex: sh.sheetIndex, setId: sh.setId, draft: !!sh.draft, solidIncluded: on, poolIds: keys, runId: sh.runId, laserDoneAt: sh.laserDoneAt, roseCutAt: sh.roseCutAt, ...(Object.keys(pieceSides).length ? { pieceSides } : {}) };
      const s = SOcore.sheetOf(rec, {}); if (!s) return null;
      const set = s.setId ? setDocOf(s.setId, sh.runId) : null;
      s.fixed = SOcore.fixedWhy(rec, set) || "";
      if (sh.autoMatched) s.matched = String(sh.autoMatched);
      return s;
    }
    function cardinalView(run) {
      if (!SOcore || !run) return null;
      const sheets = [], page = new Map(), idOf = new Map();
      for (const p of allSheets().filter(p => p.runId === run.runId)) { const n = fromPage(p); if (n) { sheets.push(n); page.set(n.id, p); idOf.set(p, n.id); } }
      return { SO: { core: SOcore }, sheets, page, idOf, norm: new Map(sheets.map(n => [n.id, n])) };
    }
    function goldGap(p) {
      if (!goldFamily(p.metal)) return "";
      if ((p.roseCutAt || +p.laserDoneAt > 0) && p.autoMatched) return "";
      if (!(p.roseCutAt || +p.laserDoneAt > 0)) return p.metal === "rose" ? "a Rose Gold sheet joins a set only by its own Cut Sheet press" : `a ${labelOf(p.metal)} sheet joins a set only by its own Include or Cut Sheet press`;
      return "";
    }
    function cannotJoin(p, ready) {
      const gold = goldGap(p); if (gold) return gold;
      if (p.endedBy === "stopped") return "its nesting was stopped";
      if (p.dirty || ["nesting", "finishing", "queued"].includes(p.status) || (p.persisted && !p.persistedDone)) return "it is still being nested or saved";
      if (!(p.placements || []).length) return "nothing is placed on it";
      if (!(p.verification && p.verification.ok)) return "its layout has not been verified yet";
      if (!ready.has(p)) return "its sheet files are not saved yet";
      return "";
    }
    const openSetsOf = runId => ofRun(runId).filter(s => s.group === "dispatch" && !s.committedAt);
    const membersOfSet = set => allSheets().filter(p => p.runId === set.runId && p.setId === set.setId && !p.draft);
    const formedOnce = set => { if (!set) return false; if (+set.formedAt > 0) return true; if (!SR || typeof SR.metalClass !== "function") return false; const cls = membersOfSet(set).map(p => SR.metalClass(p)); return cls.includes("GF") && cls.includes("SS"); };
    const closedSet = set => !!set && (formedOnce(set) || (!!SR && SR.closed(membersOfSet(set))));
    function collectingOf(runId) { const open = openSetsOf(runId), last = open[open.length - 1]; return last && !closedSet(last) ? last : null; }
    const newestOpenOf = runId => { const open = openSetsOf(runId); return open[open.length - 1] || null; };
    function anchorsOf(W, choices = selected(), withSets = true) {
      return W.sheets.filter(n => { const p = W.page.get(n.id); return !!p && ((withSets && inSetNow(p)) || !!n.fixed || goldFamily(p.metal)); }).map(n => n.id);
    }
    function greenHomesOf(W, run) {
      const core = W && W.SO && W.SO.core; if (!core || typeof core.greenHomes !== "function") return new Map();
      const all = ofRun(run.runId).filter(s => !/^superseded/.test(String(s.status || "")));
      return core.greenHomes(W.sheets, { green: n => { const p = W.page.get(n.id); return !!p && goldFamily(p.metal); }, rank: sid => { const st = all.find(s => s.setId === sid); return st ? [String(st.day || ""), +st.seq || 0] : ["~", Infinity]; },
        open: sid => { const st = all.find(s => s.setId === sid); return !!st && !(SR && SR.exempt(st)); }, matched: n => { const p = W.page.get(n.id); return p && p.autoMatched ? String(p.autoMatched) : null; },
        fixed: n => { const p = W.page.get(n.id); if (!p || !inSetNow(p)) return null; const st = all.find(s => s.setId === p.setId); return (+p.laserDoneAt > 0 || (st && ((SR && SR.exempt(st)) || st.processReady || st.laserSetPending || st.laserDoneAt))) ? String(p.setId) : null; } });
    }
    function formationOf(W, anchored, choices) {
      const core = W.SO.core;
      const ends = SR && SR.isPartial ? W.sheets.filter(n => { const p = W.page.get(n.id); return p && SR.isPartial(p) && !inSetNow(p); }).map(n => n.id) : [];
      const anchors = anchored ? anchorsOf(W, choices, anchored === true) : [];
      return Object.assign(core.formation ? core.formation(W.sheets, ends, SR && SR.PARTIAL_BRIDGE, anchors) : { groups: core.groups(W.sheets), bridges: [] }, { endIds: new Set(ends), anchorIds: new Set(anchors) });
    }
    const MARKS = ["cardinalPull", "cardinalHold", "cardinalNote", "cardinalHome"];
    function cardinalApply(run, pages, sets, choices = selected()) {
      for (const p of allSheets().filter(p => p.runId === run.runId)) { delete p.cardinalPull; delete p.cardinalHold; delete p.cardinalNote; delete p.cardinalHome; }
      const W = cardinalView(run); if (!W) return [];
      const open = (Array.isArray(sets) ? sets : sets ? [sets] : []).filter(Boolean), last = open[open.length - 1] || null, collecting = last && !closedSet(last) ? last : null;
      const ready = new Set(pages), homeOf = p => (p.setId && !p.draft && open.find(s => s.setId === p.setId)) || null;
      const pass = (F, anchored) => {
        const out = [];
        for (const b of F.bridges) {
          const p = W.page.get(b.id); if (!p) continue;
          p.cardinalNote = `${ordersWords(b.orders)} ${b.orders.length === 1 ? "is" : "are"} on ${sheetNameOf(p)} and on sheets of ${b.groups.length} different groups (${b.groups.map(ids => ids.map(id => sheetNameOf(W.page.get(id))).join(", ")).join("; ")}): each group is settled on its own, take ${b.orders.length === 1 ? "that order" : "those orders"} off a sheet so that no order is spread over two sets`;
          out.push({ ids: [b.id, ...b.groups.flat()], orders: b.orders, state: "bridge", why: p.cardinalNote });
        }
        for (const g of F.groups) {
          const openCut = p => !!p && !!p.roseCutAt && !(+p.laserDoneAt > 0) && !!homeOf(p);
          let members = g.ids.map(id => W.page.get(id)).filter(p => p && !goldFamily(p.metal)); const orders = g.orders, fixed = g.ids.filter(id => W.page.get(id) && !goldFamily(W.page.get(id).metal) && W.norm.get(id).fixed && !openCut(W.page.get(id))), tell = tellOf(W, g.ids);
          if (!members.length) continue;
          if (fixed.length) {
            const why = fixed.map(f => `${W.norm.get(f).label}: ${W.norm.get(f).fixed}`).join("; ");
            for (const p of members) if (!fixed.includes(W.idOf.get(p))) p.cardinalNote = `${ordersWords(orders, tell)} also on ${why}, so ${members.length > 2 ? "they cannot" : "it cannot"} be in the same set`;
            if (!anchored || !members.some(p => !fixed.includes(W.idOf.get(p)) && homeOf(p))) { out.push({ ids: g.ids, orders, state: "fixed", why }); continue; }
            members = members.filter(p => !fixed.includes(W.idOf.get(p)));
          }
          const homes = [...new Set(members.map(homeOf).filter(Boolean))], home = homes.length === 1 ? homes[0] : null;
          const seq = home ? home.seq : collecting ? collecting.seq : 0, wants = p => basePolicy(p, seq, choices).include, inHome = p => !!home && homeOf(p) === home;
          const partialOut = SR && SR.isPartial ? new Set(members.filter(p => !homeOf(p) && SR.isPartial(p))) : new Set();
          const wanting = members.filter(wants), needs = members.filter(p => !wants(p)), stuck = needs.map(p => ({ p, w: partialOut.has(p) ? `it is not full yet${SR.fillWords(p)}` : cannotJoin(p, ready) })).filter(x => x.w), stuckSet = new Set(stuck.map(x => x.p));
          const together = o => `Shares ${ordersWords(orders, tell)} with ${members.filter(m => m !== o).map(sheetNameOf).join(", ")}`;
          if (homes.length > 1) {
            for (const p of wanting) if (!homeOf(p)) p.cardinalHold = `Waits: sheets that share ${ordersWords(orders, tell)} are already in ${homes.map(s => s.name || `Set ${s.seq}`).join(" and ")}: put them in one set first`;
            for (const p of needs) if (!homeOf(p) && !p.cardinalHold) p.cardinalNote = `Shares ${ordersWords(orders, tell)} with sheets in ${homes.map(s => s.name || `Set ${s.seq}`).join(" and ")}: it can join only one of them, so take those orders off a sheet first`;
            for (const p of needs) if (homeOf(p) && !stuckSet.has(p)) p.cardinalPull = together(p);
            out.push({ ids: g.ids, orders, state: "spread", why: homes.map(s => s.name).join(", ") });
            continue;
          }
          if (!wanting.length) continue;
          if (partialOut.size && !home) {
            const names = [...partialOut].map(sheetNameOf), line = `Waits: ${ordersWords(orders, tell)} shared with ${names.join(" and ")}, which ${names.length > 1 ? "are" : "is"} not full yet, so they stay out of a set together`;
            for (const p of wanting) p.cardinalHold = line;
            out.push({ ids: g.ids, orders, state: "waiting", why: line, partial: names });
            continue;
          }
          for (const p of needs) if (!stuckSet.has(p)) { p.cardinalPull = together(p); if (home && !inHome(p)) p.cardinalHome = home.setId; } else if (home && !inHome(p)) p.cardinalHome = home.setId;
          if (home) for (const p of wanting) if (!inHome(p)) p.cardinalHome = home.setId;
          if (!stuck.length) { out.push({ ids: g.ids, orders, state: "together", why: "" }); continue; }
          const waits = stuck.map(x => `${sheetNameOf(x.p)} (${x.w})`).join("; "), names = stuck.map(x => sheetNameOf(x.p));
          const homeName = home ? home.name || `Set ${home.seq}` : "this set", addNames = stuck.filter(x => !partialOut.has(x.p)).map(x => sheetNameOf(x.p)), fullNames = stuck.filter(x => partialOut.has(x.p)).map(x => sheetNameOf(x.p));
          const what = [addNames.length ? `add ${addNames.length > 1 ? "those sheets" : "that sheet"} to ${homeName}` : "", fullNames.length ? `${fullNames.join(" and ")} ${fullNames.length > 1 ? "join" : "joins"} ${homeName} once ${fullNames.length > 1 ? "they are" : "it is"} full` : ""].filter(Boolean).join("; ");
          for (const p of members) if (!stuckSet.has(p)) p.cardinalNote = `${ordersWords(orders, tell).replace(/^o/, "O")} shared with ${names.join(" and ")}: ${what} (${stuck.map(x => x.w).join("; ")})`;
          out.push({ ids: g.ids, orders, state: "partial", why: waits, stuck: names });
        }
        return out;
      };
      const runPages = allSheets().filter(p => p.runId === run.runId), FA = formationOf(W, true, choices);
      const notFree = p => !!homeOf(p) || (!inSetNow(p) && (FA.endIds.has(W.idOf.get(p)) || FA.anchorIds.has(W.idOf.get(p))));
      pass(formationOf(W, "set", choices), false);
      const kept = new Map(runPages.filter(notFree).map(p => [p, MARKS.map(k => p[k])]));
      for (const p of runPages) for (const k of MARKS) delete p[k];
      const out = pass(FA, true);
      for (const [p, was] of kept) MARKS.forEach((k, i) => { if (was[i] === undefined) delete p[k]; else p[k] = was[i]; });
      greenMarks(run, W, runPages, open, homeOf, ready, choices);
      return out;
    }
    /** RoseStock.full for a record: its saved plan says so, else the release mark (the full test needs the shapes: the cloud's green-line unit answers it; here a released sheet is full). */
    const sheetFull = typeof V.sheetFull === "function" ? p => !!V.sheetFull(p) : p => (p.rosePlan ? !!p.rosePlan.full : !!p.releaseFull);   // (a page: RoseStock.full)
    function greenMarks(run, W, runPages, open, homeOf, ready, choices) {
      const GH = greenHomesOf(W, run), all = ofRun(run.runId), setName = sid => { const st = all.find(s => s.setId === sid); return st ? st.name || `Set ${st.seq}` : "its set"; };
      const named = ids => ids.map(i => W.page.get(i)).filter(Boolean).map(sheetNameOf);
      const FS = formationOf(W, "set", choices), unitOf = i => { const g = FS.groups.find(x => x.ids.includes(i) && !x.anchors.includes(i) && !x.ends.includes(i)); return g ? g.ids.filter(x => !g.anchors.includes(x) && !g.ends.includes(x)) : [i]; };
      for (const p of runPages.filter(p => goldFamily(p.metal))) {
        delete p.greenAway;
        const id = W.idOf.get(p), h = id && GH.get(id); if (!h) continue;
        for (const k of MARKS) delete p[k];
        const home = h.home ? open.find(s => s.setId === h.home) || null : null, mates = h.partners.filter(i => { const q = W.page.get(i); return q && h.home && q.setId === h.home && !q.draft; });
        const mineOrders = [...new Set(mates.flatMap(i => h.orders[i] || []))].sort(), here = homeOf(p), tell = tellOf(W, [id].concat(mates));
        const splitSet = i => { const q = W.page.get(i); return q && q.setId && !q.draft ? setName(q.setId) : ""; }, splitDay = i => { const q = W.page.get(i), st = q && all.find(s => s.setId === q.setId); try { return st && st.day ? new Date(st.day + "T12:00:00").toLocaleDateString("locale" in V ? V.locale : "en-US", { month: "short", day: "numeric" }) : ""; } catch (_) { return (st && st.day) || ""; } };
        const split = h.split.length ? `${sheetNameOf(p)} belongs to ${setName(h.home)} only: ${W.SO.core.splitWords ? W.SO.core.splitWords(h.split, { name: i => named([i])[0] || i, set: splitSet, day: splitDay }) : `${ordersWords([...new Set(h.split.flatMap(x => x.orders))].sort())} it shares with ${named(h.split.map(x => x.sheet)).join(", ")} ${h.split.flatMap(x => x.orders).length === 1 ? "ships" : "ship"} in two parts`} (a ${labelOf(p.metal)} sheet is in one set)` : "";
        if (inSetNow(p)) {
          if (here && h.home && h.home !== here.setId && !(+p.laserDoneAt > 0) && !(here.processReady || here.laserSetPending || here.laserDoneAt)) { p.greenAway = `Its orders are with the sheets of ${setName(h.home)}: a ${labelOf(p.metal)} sheet is in one set, so it leaves ${here.name || "this set"}`; continue; }
          if (here && mates.length && !basePolicy(p, here.seq, choices).include && !cannotJoin(p, ready)) p.cardinalPull = `Shares ${ordersWords(mineOrders, tell)} with ${named(mates).join(", ")}`;
        } else if (home) {
          const wants = basePolicy(p, home.seq, choices).include, gap = wants ? "" : cannotJoin(p, ready);
          p.cardinalHome = home.setId;
          if (!wants && !gap) p.cardinalPull = `Shares ${ordersWords(mineOrders, tell)} with ${named(mates).join(", ")} in ${setName(home.setId)}`;
          if (gap) for (const i of mates) for (const u of unitOf(i)) { const q = W.page.get(u); if (q && !goldFamily(q.metal) && q.setId === home.setId && !q.draft && !q.cardinalNote) q.cardinalNote = `${ordersWords(mineOrders, tell).replace(/^o/, "O")} shared with ${sheetNameOf(p)}: add that sheet to ${setName(home.setId)} (${gap})`; }
          if (gap && solid(p.metal) && !p.roseCutAt && !(+p.laserDoneAt > 0)) p.cardinalNote = sheetFull(p) ? `Waits in ${setName(home.setId)} for your approval: it is full, so it needs no green line. Press Approve on ${setName(home.setId)} in the Library and it joins ${setName(home.setId)}. Until then its charms can be moved by hand` : `Waits in ${setName(home.setId)} for your approval and its green cut line: press Approve & cut green line on ${setName(home.setId)} in the Library and it joins ${setName(home.setId)}. Until then its charms can be moved by hand`;
        } else if (h.partners.length) {
          const part = h.partners.map(i => W.page.get(i)).filter(q => q && SR && SR.isPartial(q)), allOs = [...new Set(Object.values(h.orders).flat())].sort();
          p.cardinalHold = part.length ? `Waits: ${ordersWords(allOs, tell)} shared with ${part.map(sheetNameOf).join(" and ")}, which ${part.length > 1 ? "are" : "is"} not full yet, so they stay out of a set together`
            : `Waits: ${ordersWords(allOs, tell)} shared with ${named(h.partners).join(", ")}, which ${h.partners.length === 1 ? "is" : "are"} in no set yet: it joins their set once it forms`;
        }
        if (split) { if (!p.cardinalNote) p.cardinalNote = split; for (const x of h.split) { const q = W.page.get(x.sheet); if (q && !q.cardinalNote) q.cardinalNote = `${ordersWords(x.orders).replace(/^o/, "O")} also on ${sheetNameOf(p)}, which belongs to ${setName(h.home)}: a ${labelOf(p.metal)} sheet is in one set, so ${x.orders.length === 1 ? "that order ships" : "those orders ship"} in two parts`; } }
      }
    }
    /** The multi-piece orders of a set's sheets that also have pieces on a sheet of its run outside it that could still join it (Gate.cardinalSplit). */
    function cardinalSplit(set) {
      const W = V.run && V.run.runId === set.runId ? cardinalView(V.run) : null; if (!W) return [];
      const ids = W.sheets.filter(n => n.setId === set.setId).map(n => n.id), items = W.SO.core.between(W.sheets, ids, set.setId);
      const real = typeof W.SO.core.realPartners === "function" ? W.SO.core.realPartners(W.sheets, items, set.setId, greenHomesOf(W, V.run), { green: n => { const p = W.page.get(n.id); return !!p && goldFamily(p.metal); } }) : items;
      return real.filter(it => it.thereIds.some(i => !W.norm.get(i).fixed));
    }
    /* ── SPLITWAIT (Paul, 20 Nov 2026: "no set can be formed when there are mixed orders between coloured sheets"): the orders whose GF / SS line is still waiting to be placed (on no sheet yet).
       GATECORE: charm-nest-place.js splitWaiting / splitHolds, the one copy (the page ran them; this file had its own). The rows: the page's Orders.rows() (V.rows), else the run record's lines
       read as rows (a line with no material is read as the page reads a row with no spec: its Gold Filled waits). orderId -> "gold" | "silver"; any fault reads as nothing waiting, as on the page. */
    const linesAsRows = lines => Object.values(lines || {}).filter(l => l && typeof l === "object").map(l => ({ order: l.orderId == null ? null : { receiptId: l.orderId }, spec: l.material ? { material: l.material } : null, state: l.state, poolIds: l.poolIds || [] }));
    function waitingOf() {
      try { return PL.splitWaiting(typeof V.rows === "function" ? V.rows() || [] : linesAsRows(V.run && V.run.lines), allSheets()); }
      catch (e) { warn("[SPLITWAIT]", e); return new Map(); }
    }
    const tick = k => (V.clock[k] = Math.max(V.now, num(V.clock[k]) + 1));
    // (an io answer is awaited only when it is a promise: a page's own steps that never waited (its label already made, a sheet's done mark) do not yield the look to anything else)
    const solidMark = (sh, choices) => sh.metal === "rose" ? (sh.solidPick === true ? true : null) : solid(sh.metal) ? (picked(sh, choices) ? true : sh.cardinalPull ? null : false) : null;

    /* ══ ASSEMBLE (Gate.assembleNow, bridge 6331-6436, with tendSet 6807-6887): one look at the run's sets, the writes asked of io (all awaited, in the page's order):
         io.ensureSet({ follow })  → the next set record (setAllocate), added to V.sets; null when the numbering is deferred
         io.stampReleased(page, at | null)   the time it was first seen completed (releasedAt) on its record, or taken off it (filling again)
         io.released(seen, lost)             (a page) the same, once per look for all of them (CharmNestDrive.releasedAt); asked instead of stampReleased when given
         io.join(page, set, membership)      → { ok } | { refused: "setRules", why }: the sheet record's membership (sheet first, then its set)
         io.joined(page, set)                the pieces of a joined sheet (pool rows "written", run lines), its set's order lines and label owed
         io.leave(page, set, { solidIncluded }) a sheet a PERSON marked to leave this set: its record made a draft (solidIncluded: the page's own tick for a 10K / 14K sheet)
         io.saveSet(set)                     the set record as Sets.save sends it
         io.say(level, words, { metal }?)    the agent log (the function's log)
         io.pace()                           a turn of the event loop and the unit's time budget (throws when spent)
       → { sets: [setId changed], formed, heldSheets } (+ deferred: the next set's number was deferred · noOpen: the run has no open set: the page's look ends there, its follow-ups not run) ══ */
    async function assemble(io) {
      const run = V.run, choices = Object.assign({}, selected()), say = (l, t, c) => { try { io.say && io.say(l, t, c); } catch (_) { /* a note only */ } };
      const release = (sh, seq) => policy(sh, seq, choices);
      if (!modern(run && run.runId) || !run) return { skipped: "not a modern run" };
      if (!SR) throw new Error("The set rules did not load (charm-nest-set-rules.js), so no set is formed and no sheet is moved.");
      if (!PL || typeof PL.splitHolds !== "function") throw new Error("The sheet choice did not load (charm-nest-place.js), so no set is formed and no sheet is moved.");
      const committedSheets = new Set(ofRun(run.runId).filter(s => s.committedAt).flatMap(s => s.sheetIds || []));
      const pages = allSheets().filter(p => p.runId === run.runId && p.outputs && p.persistedDone && !committedSheets.has(p.sheetId));
      let open = openSetsOf(run.runId);
      // (the moment a sheet was first seen completed orders the sets: releasedAt, one tick per look; a sheet that is filling again loses it)
      // (GATECORE: the page's own words: a sheet filling again has no releasedAt at all, and a page tells all of them at once, io.released)
      { const seen = pages.filter(p => !p.releasedAt && SR.isCompleted(p)), lost = []; if (seen.length) { const at = tick("completion"); for (const p of seen) p.releasedAt = at; }
        for (const p of pages) if (p.releasedAt && !SR.isCompleted(p)) { delete p.releasedAt; lost.push(p); }
        if (typeof io.released === "function") { if (seen.length || lost.length) { const t = io.released(seen, lost); if (t && typeof t.then === "function") await t; } }
        else { for (const p of seen) await io.stampReleased(p, p.releasedAt); for (const p of lost) await io.stampReleased(p, null); } }
      const inOpen = p => !!p.setId && !p.draft && open.some(s => s.setId === p.setId), adder = p => p.metal === "rose" || solid(p.metal);
      const handOf = p => (p.handSet && !p.cardinalHome && !adder(p) && open.some(s => s.setId === p.handSet && !s.committedAt)) ? p.handSet : null;
      const changed = new Set(); let formedTotal = 0;
      for (let round = 0; ; round++) {
        cardinalApply(run, pages, open, choices);
        for (const p of allSheets()) if (p.runId === run.runId) delete p.setWait;
        const last = open.find(s => membersOfSet(s).length && !formedOnce(s) && !SR.closed(formMembers(s))) || open[open.length - 1] || null, lastClosed = !!last && (formedOnce(last) || SR.closed(formMembers(last)));
        let target = null, addTarget = null, formed = 0, apart = new Set(), joinNow = null;
        {
          // (OWNEROVERRIDE: a sheet whose set the owner decided is no free sheet)
          const free = pages.filter(p => !inOpen(p) && !p.cardinalHome && !handOf(p) && !adder(p) && !ownerMoved(p) && release(p, last && !lastClosed ? last.seq : 0).include);
          const W = cardinalView(run), pagesOfIds = ids => (ids || []).map(id => W.page.get(id)).filter(Boolean), links = W ? formationOf(W, true, choices).groups.map(g => ({ ids: pagesOfIds(g.ids), anchors: pagesOfIds(g.anchors), ends: pagesOfIds(g.ends) })) : [];
          const lastOpen = !!last && !formedOnce(last);
          // (SPLITWAIT: the orders that wait and the free sheets that wait with them, charm-nest-place.js splitWaiting / splitHolds)
          const waiting = waitingOf();
          const holds = PL.splitHolds(free, waiting, links);
          for (const [p, ks] of holds) p.setWait = `Waits for its set: order${ks.length === 1 ? "" : "s"} ${ks.slice(0, 3).join(", ")}${ks.length > 3 ? " and more" : ""} still ${ks.length === 1 ? "has" : "have"} ${waiting.get(ks[0]) === "silver" ? "Sterling Silver" : "Gold Filled"} pieces waiting to be placed, and no set forms while an order is split`;
          const freeNow = free.filter(p => !holds.has(p));
          const f = SR.form(lastOpen ? formMembers(last) : [], freeNow, lastOpen, last && last.name, links);
          for (const w of f.wait) w.sheet.setWait = w.why;
          if (f.where === "last") target = last;
          else if (f.where === "new") {
            const newest = ofRun(run.runId).filter(s => s.group === "dispatch" && !/^superseded/.test(String(s.status || ""))).pop() || last;
            target = await io.ensureSet(newest ? { follow: newest } : {});
          }
          if (f.where === "new" && !target) return { sets: [...changed], formed: formedTotal, deferred: true };
          if (f.where === "new" && target && (target === last || target.committedAt || membersOfSet(target).length)) {
            const why = `${target.name || "A set"} was handed back as the next set but it already holds sheets: no sheet joins it, the next set waits${V.hint || ""}`;
            say("warn", why);
            for (const p of f.join || []) p.setWait = why;
            target = null; f.join = [];
          }
          formed = (f.join || []).length; joinNow = new Set(f.join || []);
          if (target) open = openSetsOf(run.runId);
          addTarget = target || (last && lastClosed ? last : null);
          const adders = pages.filter(p => !inOpen(p) && !p.cardinalHome && adder(p) && !ownerMoved(p) && release(p, (addTarget || last) ? (addTarget || last).seq : 0).include);
          if (!addTarget && adders.length) for (const w of SR.settle(last ? formMembers(last) : [], adders).wait) w.sheet.setWait = w.why;
          const joining = new Set(f.join || []);
          for (const a of adders) if (links.filter(g => g.ids.includes(a)).some(g => g.ids.some(q => q !== a && !adder(q) && free.includes(q) && !joining.has(q)))) apart.add(a);
        }
        if (!open.length) { run.heldSheets = pages.length; return { sets: [...changed], formed: formedTotal, heldSheets: run.heldSheets, noOpen: true }; }
        cardinalApply(run, pages, open, choices);
        const admits = (sh, set) => sh.cardinalHome ? sh.cardinalHome === set.setId : handOf(sh) ? handOf(sh) === set.setId : adder(sh) ? !apart.has(sh) && !!addTarget && addTarget.setId === set.setId : !!target && target.setId === set.setId && (!joinNow || joinNow.has(sh));
        for (const set of open) {
          if (set.committedAt) continue;
          const wasThere = membersOfSet(set).length, kept = (set.sheetIds || []).length, forming = target && target.setId === set.setId;
          await tendSet(io, set, pages, choices, release, admits);
          if (forming || membersOfSet(set).length !== wasThere || (set.sheetIds || []).length !== kept || set.touched) { delete set.touched; await io.saveSet(set); changed.add(set.setId); }
        }
        formedTotal += formed;
        if (!formed || ![...joinNow].some(p => p.setId && !p.draft) || round > pages.length) break;
      }
      for (const p of pages) { delete p.handSet; delete p.rgPress; }
      run.heldSheets = pages.filter(p => p.draft).length;
      return { sets: [...changed], formed: formedTotal, heldSheets: run.heldSheets };
    }
    /** RGTWO (RGONE, Paul 9 Nov 2026: "there should never be two partial rose gold sheets in one set"; 23 Nov, Set-1: "block a second partial rose gold sheet from joining a set": YES): why this Rose Gold sheet does not join the
     *  set by itself: it is not full and the set has another Rose Gold sheet that is not full, or a fuller one that joins it now. "" when it may. A person's own press (Include, Cut Sheet, a drop on the set, the owner's word) wins.
     *  Its pieces are not moved here: a person puts them on the set's one sheet, or presses. Every way in comes through here: the cardinal pull of a cut sheet, the automatic cut's include, a match made earlier. */
    function rgSecond(sh, set, pages, choices, release, admits) {
      if (sh.metal !== "rose" || sheetFull(sh)) return "";
      if (sh.rgPress || sh.handSet === set.setId || (ownerMoved(sh) && String(sh.ownerSet.setId || "") === String(set.setId)) || (sh.solidPick === true && !sh.autoMatched)) return "";
      const part = q => q !== sh && q.metal === "rose" && !q.recalled && (q.placements || []).length > 0 && !sheetFull(q);
      const here = membersOfSet(set).find(part) || pages.find(q => part(q) && !inSetNow(q) && (q.placements || []).length > (sh.placements || []).length && release(q, set.seq).include && admits(q, set) && ownerWants(q, set) && !joinRefusedOf(q, set));
      return here ? `Waits: ${set.name || "the set"} has ${sheetNameOf(here)} (${(here.placements || []).length} pieces, not full), and a set never takes two partial Rose Gold sheets by itself. Put its pieces on ${sheetNameOf(here)}, or press Include to add it anyway` : "";
    }
    /** One open set kept as the rules say (Gate.tendSet): a sheet a PERSON marked to leave it goes back out (NOSETPULL: the only way out), the set's lists follow its members,
     *  a sheet the set takes now joins with its number and file name. */
    async function tendSet(io, set, pages, choices, release, admits) {
      const SRt = SR, before = tick("join"), at = tick("join"), say = (l, t, c) => { try { io.say && io.say(l, t, c); } catch (_) { /* a note only */ } };
      // (GATECORE: a sheet's membership as its record says it: on a page its own setId / draft, in the cloud the record's, read beside the page's view of it)
      const recSet = p => (live ? p.setId : p.recSetId), recDraft = p => (live ? p.draft : p.recDraft);
      set.sheetIds = Array.isArray(set.sheetIds) ? set.sheetIds : []; set.labelFiles = Array.isArray(set.labelFiles) ? set.labelFiles : []; set.orders = set.orders && typeof set.orders === "object" ? set.orders : {};
      if (SRt && !set.formedAt && (SRt.validSet(membersOfSet(set)).ok || formedOnce(set))) { set.formedAt = before; set.touched = true; }
      // (PERSONOUT, Paul 16 Nov 2026: a sheet a person marked to leave THIS set (its record: draft, still naming the set, leftSet = this set) leaves it; the system takes no sheet out by itself)
      // (GATECORE: the page's own reading: the run's saved sheets of this look (pages), and the file name it leaves with kept (leftFileBase) so it takes its old number back when it returns)
      for (const sh of pages.filter(p => recSet(p) === set.setId && recDraft(p) === true && p.leftSet === set.setId && !ownerMoved(p) && !release(p, set.seq).include)) {
        if (SN) set.sheetNos = SN.raise(set.sheetNos, [Object.assign({}, sh, { setId: set.setId, draft: false, sheetIndex: sh.sheetIndex })]);   // (the number it had is retired for this set)
        if (SN && SN.fileTag(sh)) sh.leftFileBase = sh.fileBase;
        await io.leave(sh, set, { solidIncluded: solid(sh.metal) ? picked(sh, choices) : null });
        sh.draft = true; sh.setId = null; sh.seq = null; sh.sheetIndex = null; sh.label = null; if (!live) sh.recSetId = null;
        set.labels = null; set.touched = true;
      }
      const included = new Set(allSheets().filter(p => p.setId === set.setId && !p.draft).map(p => p.sheetId));
      set.sheetIds = set.sheetIds.filter(id => included.has(id)); set.labelFiles = set.labelFiles.filter(f => f && included.has(f.sheetId));
      set.materials = [...new Set(allSheets().filter(p => included.has(p.sheetId)).map(p => p.metal))];
      for (const [rid, order] of Object.entries(set.orders)) {
        const lines = order && order.lines && typeof order.lines === "object" ? order.lines : {};
        for (const [tid, line] of Object.entries(lines)) { line.copies = (line.copies || []).filter(c => included.has(c.sheetId)); if (!line.copies.length) delete lines[tid]; }
        if (!Object.keys(lines).length) delete set.orders[rid];
      }
      for (const sh of pages) {
        if (!release(sh, set.seq).include) continue;
        if (sh.setId === set.setId) {
          // (membership saved before its label was made: the label is owed, the set lists the sheet; the labels unit draws it. A page lists it when it makes the label: io.joined, Sets.onSheetSaved)
          if (!live && !set.sheetIds.includes(sh.sheetId)) { set.sheetIds.push(sh.sheetId); set.touched = true; }
          if (!live && !set.materials.includes(sh.metal)) { set.materials.push(sh.metal); set.touched = true; }
          { const j = io.joined(sh, set, { again: true }); if (j && typeof j.then === "function" ? await j : j) set.touched = true; }
          continue;
        }
        if (sh.setId && !sh.draft) continue;
        if (!admits(sh, set)) continue;
        if (!ownerWants(sh, set)) continue;   // (OWNEROVERRIDE)
        { const two = rgSecond(sh, set, pages, choices, release, admits); if (two) { sh.setWait = two; continue; } }   // (RGTWO)
        { const rf = joinRefusedOf(sh, set); if (rf) { sh.setWait = rf; continue; } }
        if (SRt && typeof SRt.isPartial === "function" && SRt.isPartial(sh) && !(sh.handSet === set.setId && !sh.cardinalHome)) continue;   // (NOPARTIALSET)
        const previous = { draft: sh.draft, setId: sh.setId, setDay: sh.setDay, seq: sh.seq, group: sh.group, sheetIndex: sh.sheetIndex, fileBase: sh.fileBase, joinedAt: sh.joinedAt, joinedBy: sh.joinedBy }, nosWas = set.sheetNos;
        sh.draft = false; sh.setId = set.setId; sh.setDay = set.day; sh.seq = set.seq; sh.group = "dispatch";
        sh.joinedAt = at; if (sh.handSet === set.setId && !sh.cardinalHome) sh.joinedBy = "person"; else delete sh.joinedBy;
        if (SN) {
          const here = allSheets().filter(p => p !== sh && p.setId === set.setId && p.metal === sh.metal);
          set.sheetNos = SN.raise(set.sheetNos, allSheets().filter(p => p !== sh && p.setId === set.setId));
          sh.sheetIndex = SN.claim(sh.leftFileBase && !SN.fileTag(sh) ? Object.assign({}, sh, { fileBase: sh.leftFileBase }) : sh, here, set.sheetNos, n => V.fileBase(Object.assign({}, sh, { sheetIndex: n })));
          delete sh.leftFileBase;
          set.sheetNos = SN.raise(set.sheetNos, [sh]);
        } else sh.sheetIndex = 1 + Math.max(0, ...allSheets().filter(p => p !== sh && p.setId === set.setId && p.metal === sh.metal).map(p => +p.sheetIndex || 0));
        sh.fileBase = V.fileBase(sh);
        set.labels = null;
        const undo = () => { Object.assign(sh, previous); if (previous.joinedAt === undefined) delete sh.joinedAt; if (previous.joinedBy === undefined) delete sh.joinedBy; set.sheetNos = nosWas; };
        let r;
        // (a write that FAILED keeps the number it claimed retired, as the page always did: only the sheet's own fields go back)
        try { r = await io.join(sh, set, { draft: false, setId: set.setId, setSeq: set.seq, sheetIndex: sh.sheetIndex, fileBase: sh.fileBase, folder: sh.fileBase, solidIncluded: solidMark(sh, choices), joinedAt: sh.joinedAt, ...(sh.joinedBy ? { joinedBy: sh.joinedBy } : {}) }); }
        catch (e) { Object.assign(sh, previous); throw e; }
        if (r && r.refused === "setRules") {
          // (STUCKSPIN, Paul 28 Oct 2026: a join the cloud refused for the owner's rules is LEFT OUT, never the end of the run: the sheet waits outside with the server's words for ten minutes)
          undo();
          const why = String(r.why || "the set rules refused it").replace(/\.$/, "");
          if (V.joinRefused) V.joinRefused.set(sh.sheetId, { setId: set.setId, at: V.now, why }); sh.setWait = why;
          say("warn", `${sheetNameOf(sh)} was not put into ${set.name || "the set"} (the cloud refused it for your set rules, so it waits outside and the run goes on): ${why}`, { metal: sh.metal });
          continue;
        }
        if (!r || !r.ok) { undo(); throw new Error(`Set membership was not saved: ${(r && r.why) || "no answer"}`); }
        // (a page's join made its label and listed it in the set already, io.join: Sets.onSheetSaved; the cloud's records are listed here and their pieces marked by io.joined)
        if (!live) { sh.recSetId = set.setId; sh.recDraft = false; if (!set.sheetIds.includes(sh.sheetId)) set.sheetIds.push(sh.sheetId); if (!set.materials.includes(sh.metal)) set.materials.push(sh.metal); }
        { const t = io.joined(sh, set, { again: false }); if (t && typeof t.then === "function") await t; }
        if (!live) set.status = set.committedAt ? set.status : "nesting";
        set.touched = true;
        if (io.pace) await io.pace();
      }
      if (SRt && !set.formedAt && SRt.validSet(membersOfSet(set)).ok) { set.formedAt = at; set.touched = true; }
    }

    /* ══ the owner-rule self-check (Gate.selfCheck: charm-nest-set-invariants.js), never blocking ══ */
    function invariantInput(run) {
      if (!SOcore || !SR || !run) return null;
      const keyOf = p => p.sheetId || `${p.runId || "run"}:${p.metal}:${p.page || 1}`;
      const recs = [], waitingIds = [];
      for (const p of allSheets().filter(p => p.runId === run.runId && !p.recalled)) {
        const n = fromPage(p); if (!n) continue;
        if (!inSetNow(p) && SR.isCompleted(p) && !goldFamily(p.metal) && (p.leftSet || !p.outputs || !p.persistedDone || basePolicy(p, 0).reason === "Nest and verify first")) waitingIds.push(n.id);
        const gold = goldFamily(p.metal);
        recs.push({ id: n.id, metal: p.metal, runId: p.runId, page: p.page, sheetIndex: p.sheetIndex, draft: !!p.draft, setId: p.setId || null, releaseFull: !!p.releaseFull, laserDoneAt: p.laserDoneAt, roseCutAt: p.roseCutAt,
          density: p.density, placedCount: (p.placements || []).length, releasedAt: p.releasedAt, joinedAt: p.joinedAt, ...(p.joinedBy ? { joinedBy: p.joinedBy } : {}), pieces: n.pieces, ...(p.keepInSet ? { keepInSet: p.keepInSet } : {}),
          ...(gold ? { cutApproved: p.solidPick === true, ...(p.autoMatched ? { autoMatched: String(p.autoMatched) } : {}), ...(p.autoCutWhy ? { autoCutWhy: String(p.autoCutWhy) } : {}) } : {}) });
      }
      const ids = new Set(recs.map(r => r.id)), sets = [];
      for (const set of ofRun(run.runId)) {
        if (/^superseded/.test(String(set.status || ""))) continue;
        const mine = allSheets().filter(p => p.runId === run.runId && p.setId === set.setId && !p.draft && !p.recalled).map(keyOf);
        sets.push({ id: set.setId, name: set.name || `Set ${set.seq}`, seq: set.seq, day: set.day, formedAt: set.formedAt, committedAt: set.committedAt, status: set.status, laserDoneAt: set.laserDoneAt,
          sheetIds: [...new Set(mine.concat(set.committedAt ? (set.sheetIds || []).filter(i => ids.has(i)) : []))] });
      }
      return { sets, sheets: recs, opts: { skipExempt: true, partialPartners: "pending", waiting: waitingIds } };
    }
    function selfCheck(run) {
      const I = INV; if (!I || typeof I.check !== "function") return null;
      try { const input = invariantInput(run || V.run); if (!input) return null; const res = I.check(input.sets, input.sheets, input.opts); return Object.assign({}, res, { lines: I.explain(res) }); }
      catch (e) { return { ok: false, error: String((e && e.message) || e), violations: [], lines: [] }; }
    }

    /* ══ the automatic green-line cut's choices (Gate.autoCut, RGONE, RGCLEAR), as decisions: the cloud's green-line unit (_charmNestGreenLine.js autoUnit) and the page's Gate.autoCut
       / rgOneMerge (charm-nest-bridge.js) both carry them out (GREENCORE, Paul 20 Nov 2026: the Rose Gold cut is one copy too). What only a page knows is handed in through the view:
         V.roseBusy(p, kind)  its in-memory flags: "retro" (_roseAction, _autoCutting), "cut" (+ _rosePlanning, _roseLoading), "merge" (_roseAction, _autoCutting), "trim" (_incBusy, ...)
         V.canClear()         a take-off can run here (the page: SheetWin.takeOffOrder)      V.clearBusy(p)   a clear of this sheet is running (the page's rgClearBusy)
         V.canCut()           a cut can run here (the page: RoseStock.record and the cloud reachable)
         V.lineOf(metal)      the sheets of a metal in line order (the page: S.sheets[m].pages; the cloud: charm-nest-place.js lineOrder over its records, the order a reload restores)
       The retry memory (tried, oneTried, clearHeld) is the caller's: the page's Maps and run.rgClearHeld, the cloud's Charm_Nest_Green/<runId>; read here, written by the caller. ══ */
    const rgPlaced = p => (p.placements || []).length, orderOfCharm = c => String(c && c.order != null ? c.order : "").split("/")[0];
    const got = (m, k) => (m && typeof m.get === "function" ? m.get(k) : m ? m[k] : undefined);   // (a page's Map or a record's object)
    const busyOf = (p, kind) => typeof V.roseBusy === "function" && !!V.roseBusy(p, kind);
    const nesting = p => ["nesting", "finishing", "queued"].includes(p.status);
    const lineOf = m => (typeof V.lineOf === "function" ? V.lineOf(m) || [] : PL && typeof PL.lineOrder === "function" ? PL.lineOrder(allSheets().filter(q => q.metal === m)) : allSheets().filter(q => q.metal === m).slice().sort((a, b) => num(a.page) - num(b.page) || String(a.sheetId).localeCompare(String(b.sheetId))));
    function rgOneGroups(run, W, GH) {
      const uncut = new Map(), cutIn = new Map(), add = (m, k, p) => { if (!m.has(k)) m.set(k, []); m.get(k).push(p); };
      for (const p of allSheets().filter(p => p.runId === run.runId && p.metal === "rose" && !(+p.laserDoneAt > 0) && !p.recalled)) {
        const h = W && GH.get(W.idOf.get(p));
        // (RGTWO: a cut sheet in no set yet counts in the set it will join, its home: at 10:13 on 23 Nov the second sheet of Set-1 was cut while the first, cut at 2:17, waited to join that very set)
        if (p.roseCutAt) { const sid = inSetNow(p) ? String(p.setId) : h && h.home && !(p.leftSet && String(p.leftSet) === String(h.home)) && !(ownerMoved(p) && String(p.ownerSet.setId || "") !== String(h.home)) ? String(h.home) : null; if (sid) add(cutIn, sid, p); continue; }
        if (!rgPlaced(p)) continue;
        if (h && h.home) add(uncut, String(h.home), p);
      }
      for (const l of uncut.values()) l.sort((a, b) => rgPlaced(b) - rgPlaced(a) || (+a.page || 0) - (+b.page || 0));
      return { uncut, cutIn };
    }
    const rgOneNoRoom = (tried, home, list) => list.slice(1).every(src => (src.charms || []).filter(c => !c.excluded).map(orderOfCharm).filter(Boolean).every(o => { const t = got(tried, home + "|" + src.sheetId + "|" + o); return t && t.room; }));
    /* RGONE's move, one order at a time (the page's rgOneMerge, the cloud's merge): a sheet being nested, saved, cut by hand or holding a drawn line takes and gives nothing; each order
       of a smaller sheet is tried at most once a minute; a sheet cut, cut on the laser or recalled is refused at the move itself (rgOneGuard) */
    const rgOneBusy = p => !!(busyOf(p, "merge") || p.dirty || nesting(p) || !p.persistedDone || p._manualHold || p.rosePlan || p.roseProtected);
    const rgOneOrders = src => [...new Set((src.charms || []).filter(c => !c.excluded).map(orderOfCharm).filter(Boolean))];
    const rgOneKey = (home, src, o) => home + "|" + src.sheetId + "|" + o;
    const rgOneDue = (tried, key) => { const last = got(tried, key); return !(last && V.now - num(last.at) < 60000); };
    const rgOnePieces = (src, o) => (src.charms || []).filter(c => orderOfCharm(c) === o);
    const rgOneGuard = sh => (sh.roseCutAt || +sh.laserDoneAt > 0 || sh.recalled ? "it is cut" : sh._manualHold ? "it is being changed by hand" : "");
    function rgForeign(p, home) {
      const prot = p.roseCutAt && p.roseProtected && Array.isArray(p.roseProtected.placements) ? new Set(p.roseProtected.placements.map(q => q.id)) : null;
      const ids = prot || new Set((p.placements || []).map(q => q.id));
      const orders = [...new Set((p.charms || []).filter(c => !c.excluded && ids.has(c.id)).map(orderOfCharm).filter(Boolean))];
      return orders.filter(o => allSheets().some(q => q !== p && !q.recalled && (q.charms || []).some(c => !c.excluded && orderOfCharm(c) === o) && !!q.setId && !q.draft && String(q.setId) !== home));
    }
    /** RGSETS (Paul, 23 Nov 2026: one Rose Gold draft may be tied to GF / SS sheets that are in no set yet): the orders on this UNCUT sheet, about to be cut into `home`, whose other pieces are on a GF / SS sheet that is in no
        set and does not go with `home` (a formed set takes no more sheets by itself; one that shares an order with a GF / SS sheet of it still joins it): they go past the line too (RGCLEAR), with the set their own sheet forms. */
    function rgLoose(p, home) {
      const st = setDocOf(home, p.runId), W = st && closedSet(st) ? cardinalView(V.run) : null; if (!W) return [];
      const groups = formationOf(W, "set").groups, joins = new Map(), goesWith = q => { if (joins.has(q)) return joins.get(q);
        const id = W.idOf.get(q), g = groups.find(x => x.ids.includes(id) && !(x.anchors || []).includes(id) && !(x.ends || []).includes(id)) || groups.find(x => x.ids.includes(id)), r = !!g && g.ids.some(i => { const m = W.page.get(i); return m && m !== q && !goldFamily(m.metal) && inSetNow(m) && String(m.setId) === String(home); });
        joins.set(q, r); return r; };
      const ids = new Set((p.placements || []).map(q => q.id)), orders = [...new Set((p.charms || []).filter(c => !c.excluded && ids.has(c.id)).map(orderOfCharm).filter(Boolean))];
      return orders.filter(o => allSheets().some(q => q !== p && !q.recalled && !goldFamily(q.metal) && q.runId === p.runId && !inSetNow(q) && (q.charms || []).some(c => !c.excluded && orderOfCharm(c) === o) && !goesWith(q)));
    }
    /** RGLOOSE's held orders wait for the cut as RGCLEAR's do: their sheet is still uncut and goes with a formed set, and their own GF / SS sheet is still in no set. */
    function rgStillLoose(o, p) {
      const W = cardinalView(V.run), h = W && greenHomesOf(W, V.run).get(W.idOf.get(p)), home = (h && h.home) || (inSetNow(p) ? p.setId : null), st = home && setDocOf(home, p.runId);
      return !!st && closedSet(st) && allSheets().some(q => q !== p && !q.recalled && !goldFamily(q.metal) && q.runId === p.runId && !inSetNow(q) && (q.charms || []).some(c => !c.excluded && orderOfCharm(c) === o));
    }
    function rgStillForeign(o, p) {
      if (!p.roseCutAt && rgStillLoose(o, p)) return true;
      const setsOf = rid => new Set(allSheets().filter(q => q !== p && !q.recalled && !!q.setId && !q.draft && (q.charms || []).some(c => !c.excluded && orderOfCharm(c) === rid)).map(q => String(q.setId)));
      const all = new Set(setsOf(o));
      for (const r of new Set((p.charms || []).filter(c => !c.excluded).map(orderOfCharm).filter(Boolean))) for (const sid of setsOf(r)) all.add(sid);
      if (p.setId && !p.draft) all.add(String(p.setId));
      return all.size > 1;
    }
    const approvedSet = st => !!(st.committedAt || st.processReady || st.laserSetPending || st.laserDoneAt);
    // (busy: being nested, written or saved, or its files owed: the page's _roseAction/_rosePlanning/dirty/status/persistedDone, read from the record)
    const busySheet = p => !!(p.dirty || nesting(p) || !p.persistedDone || p.saving || p.filesOwed);
    /** The retry key of an automatic cut: the sheet, its set, its nest (the page's nest job id, the record's nest job), its backs, its files and its label: a refused cut is tried again
     *  at once when one of them changed (GREENCORE: the cloud read layoutRev; it reads its nest job now, as the page reads its own) */
    const cutKey = (p, s) => p.sheetId + "|" + s.setId + "|" + (live ? p.jobId || "" : (p.nestJob && (p.nestJob.id || p.nestJob.jobId)) || "") + "|" + Object.keys(p.backPool || {}).length + "|" + Object.keys(p.outputs || {}).sort().join(",") + "|" + (p.label ? 1 : 0);
    /** What the automatic cut does now (Gate.autoCut's order): { release: [orderId] (A1), clear: [{ sheet, setId, setName, foreign, cut }] (A2, A10), merge: [{ home, setName, list }] (A7),
     *  cut: [{ sheet, set, free, key }] (A12), said: { sheetId | home: words }, steps: every one of them in the order the page's loop meets them (and "cutOther", "mergeWait", "passOne"),
     *  error?: a fault of the page's loop after the steps before it } — `state`: { tried, oneTried, clearHeld }. Pure. */
    function autoCutPlan(state) {
      const run = V.run, st = state || {}, out = { release: [], clear: [], merge: [], cut: [], said: {}, keep: {}, steps: [] };
      if (!run || !modern(run.runId)) return out;
      const held = st.clearHeld || {}, canClear = typeof V.canClear === "function" ? !!V.canClear() : true;
      // (a clear of the sheet still running, or just done, holds it: the page's rgClearBusy (until 30 s after its take-offs); the cloud's own memory of the clear it asked (tried["clear|<id>"], 30 s))
      const clearBusy = typeof V.clearBusy === "function" ? p => !!V.clearBusy(p) : p => { const t = got(st.tried, "clear|" + p.sheetId); return !!t && V.now - num(t.at) < 30000; };
      const phase = (label, f) => { if (!live) return f(); try { return f(); } catch (e) { warn(label, e && e.message); return undefined; } };   // (the page's own try of each part)
      // A1 · release after cut (rgClearRelease: never by time, NO6H)
      phase("[Gate.rgClear] release", () => { for (const [o, x] of Object.entries(held)) {
        if (!x || x.releasing) continue;
        const p = allSheets().find(q => q.sheetId === x.sheetId);
        if (p && !p.roseCutAt && !(+p.laserDoneAt > 0) && !p.recalled && rgStillForeign(o, p)) continue;
        out.release.push(o); out.steps.push({ kind: "release", order: o });
      } });
      // A2 · a Rose Gold sheet already cut into an open set that holds an order of another set inside its line (retroactive RGCLEAR)
      phase("[Gate.rgClear] retro", () => { const setsNow = ofRun(run.runId).filter(s => !s.committedAt);
        for (const p of allSheets().filter(p => p.runId === run.runId && p.metal === "rose" && p.roseCutAt && !(+p.laserDoneAt > 0) && !p.recalled && inSetNow(p))) {
          const s = setsNow.find(x => x.setId === p.setId); if (!s || approvedSet(s)) continue;
          if (busyOf(p, "retro") || p._manualHold || nesting(p)) continue;
          if (!canClear || clearBusy(p)) continue;
          const foreign = rgForeign(p, String(s.setId)); if (foreign.length) { const c = { sheet: p, setId: String(s.setId), setName: s.name || "its set", foreign, cut: true }; out.clear.push(c); out.steps.push(Object.assign({ kind: "clear" }, c)); }
        } });
      if (typeof V.canCut === "function" && !V.canCut()) return out;
      const main = () => {
        const setsNow = ofRun(run.runId).filter(s => !s.committedAt), W = cardinalView(run), GH = W ? greenHomesOf(W, run) : new Map(), RG1 = rgOneGroups(run, W, GH), merging = new Set();
        for (const p of allSheets().filter(p => p.runId === run.runId && p.metal === "rose" && !p.roseCutAt && !(+p.laserDoneAt > 0) && !p.recalled)) {
          if (busyOf(p, "cut") || p.dirty || nesting(p) || !p.persistedDone || !(p.verification && p.verification.ok) || !rgPlaced(p) || p._manualHold) continue;   // A3
          let h = W && GH.get(W.idOf.get(p)), free = false;
          // A5 RGREST (Paul, 15 Nov 2026): a FINISHED Rose Gold sheet (full, or a later Rose Gold sheet of the run in the line has taken pieces) that no order ties to another sheet joins the newest open set
          if ((!h || !h.home) && !(h && h.partners && h.partners.length)) {
            let done = !!p.releaseFull || sheetFull(p);
            if (!done) { const line = lineOf("rose"), at = line.indexOf(p); done = at >= 0 && line.slice(at + 1).some(q => q && q.runId === run.runId && !q.recalled && rgPlaced(q) > 0); }
            const nst = done ? newestOpenOf(run.runId) : null;
            if (nst && !approvedSet(nst)) { h = { home: nst.setId, partners: [], split: [] }; free = true; }
          }
          if (!h || !h.home) continue;
          if (p.autoCutOff || (p.leftSet && !inSetNow(p) && String(p.leftSet) === String(h.home))) continue;   // A6 (a person's take-out wins)
          if (ownerMoved(p) && String(p.ownerSet.setId || "") !== String(h.home)) continue;                       // A6 OWNEROVERRIDE
          { const home = String(h.home), stName = (setsNow.find(x => x.setId === h.home) || {}).name || "its set", cutOther = (RG1.cutIn.get(home) || []).filter(q => q !== p), sibs = RG1.uncut.get(home) || [];
            if (cutOther.length) { const words = `${stName} already has ${sheetNameOf(cutOther[0])} cut: a set never takes two partial Rose Gold sheets by itself, so ${sheetNameOf(p)} is not cut automatically. Press its Cut Sheet to add it anyway, or move its pieces`;
              out.said[p.sheetId] = words; out.steps.push({ kind: "cutOther", sheet: p, other: cutOther[0], setName: stName, words }); continue; }
            if (sibs.length > 1 && !rgOneNoRoom(st.oneTried || {}, home, sibs)) {
              const words = `${stName} has ${sibs.length} Rose Gold sheets (${sibs.map(q => `${sheetNameOf(q)}: ${rgPlaced(q)} piece${rgPlaced(q) === 1 ? "" : "s"}`).join(", ")}): their pieces go onto ${sheetNameOf(sibs[0])} first, then that one sheet is cut`;
              out.said[p.sheetId] = words; out.steps.push({ kind: "mergeWait", sheet: p, words });
              if (!merging.has(home)) { merging.add(home); const m = { home, setName: stName, list: sibs }; out.merge.push(m); out.steps.push(Object.assign({ kind: "merge" }, m)); }
              continue;
            }
            out.steps.push({ kind: "passOne", sheet: p }); }
          if (inSetNow(p) && p.setId !== h.home) continue;                                                                   // A8
          if (!free && !h.partners.some(i => { const q = W.page.get(i); return q && q.setId === h.home && !q.draft && SR && SR.isCompleted(q); })) continue;   // A9
          const s = setsNow.find(x => x.setId === h.home); if (!s || approvedSet(s) || (!free && (!SR || !SR.closed(membersOfSet(s))))) continue;
          if (canClear) {   // A10 RGCLEAR (a clear of this sheet still running holds its cut too)
            if (clearBusy(p)) continue;
            const foreign = [...new Set(rgForeign(p, String(s.setId)).concat(rgLoose(p, String(s.setId))))];   // (RGSETS: an order whose own GF / SS sheet forms a later set goes past the line too)
            if (foreign.length) { const c = { sheet: p, setId: String(s.setId), setName: s.name || "its set", foreign, cut: false }; out.clear.push(c); out.steps.push(Object.assign({ kind: "clear" }, c)); continue; }
          }
          const key = cutKey(p, s), last = got(st.tried, p.sheetId);
          if (last && last.key === key && V.now - num(last.at) < (last.refused && !(last.waitsIn && inSetNow(p) && p.setId === h.home) ? 600000 : 60000)) continue;   // A11
          const c = { sheet: p, set: s, free, key }; out.cut.push(c); out.steps.push(Object.assign({ kind: "cut" }, c));   // A12
        }
      };
      if (!live) main(); else { try { main(); } catch (e) { out.error = e; } }
      return out;
    }
    /* ══ a spare unit out of its set (Gate.trimSurplus; NOAUTOTRIM, Paul 14 Nov 2026: never run by itself, MINFLOOR: no unit is ever spare now) and UNPAIR (Gate.unpairPlan; NOSETPULL: never
       carried out) — the choices only, the same for every caller ══ */
    /** The spare units the checker named (`spare`: its "surplus" violations) that may leave their set now: [{ st, unit: [page] }]. */
    function trimPlan(run, spare) {
      const W = cardinalView(run); if (!W || !run) return [];
      const sets = new Map(ofRun(run.runId).map(st => [st.setId, st]));
      const busy = p => !!(busyOf(p, "trim") || p.dirty || nesting(p) || (p.persisted && !p.persistedDone) || p._manualHold);
      const out = [];
      for (const v of spare || []) {
        const st = sets.get(v.set); if (!st || approvedSet(st)) continue;
        const unit = (v.unit || []).map(id => W.page.get(id));
        if (unit.some(p => !p || +p.laserDoneAt > 0 || p.processReady || p.setId !== st.setId || busy(p))) continue;
        out.push({ st, unit });
      }
      return out;
    }
    const fixedSheetBusy = p => !!(p.recalled || p.roseCutAt || +p.laserDoneAt > 0 || p._manualHold || nesting(p) || (p.persisted && !p.persistedDone));
    /** The moves that would put the run's sets right (UNPAIR): [{ set, from, to|null (a new sheet), metal, orders, second, pieces:[charm] }]. env: { pagesOf(metal), pairFits(p, orders, run, o), closed(p) } (the page's LiveNest). Pure. */
    function unpairPlan(run, env) {
      const E = env || {}, out = [], pagesOf = m => (typeof E.pagesOf === "function" ? E.pagesOf(m) || [] : lineOf(m));
      const placedOrders = p => { const on = new Set((p.placements || []).map(x => x.id)); return new Set((p.charms || []).filter(c => on.has(c.id)).map(orderOfCharm).filter(Boolean)); };
      const early = (a, b) => (+a.joinedAt || 0) - (+b.joinedAt || 0) || (+a.releasedAt || 0) - (+b.releasedAt || 0) || (+a.page || 0) - (+b.page || 0);
      const fixedSheet = p => !!(p.roseCutAt || +p.laserDoneAt > 0 || p.recalled || p.processReady || p._manualHold || p.dirty || nesting(p) || (p.persisted && !p.persistedDone) || !p.sheetId);
      for (const st of openSetsOf(run.runId)) {
        if (approvedSet(st)) continue;
        const members = membersOfSet(st).filter(p => (p.metal === "gold" || p.metal === "silver") && SR && SR.isCompleted(p)), gfs = members.filter(p => p.metal === "gold").sort(early), sss = members.filter(p => p.metal === "silver").sort(early);
        if (gfs.length < 2 && sss.length < 2) continue;
        const os = new Map(members.map(p => [p, placedOrders(p)])), common = (a, b) => [...os.get(a)].filter(o => os.get(b).has(o));
        let pair = null; for (const g of gfs) { for (const s of sss) if (common(g, s).length) { pair = [g, s]; break; } if (pair) break; }
        if (!pair) continue;
        for (const [second, mate, keep] of [...gfs.filter(g => g !== pair[0]).map(g => [g, pair[1], pair[0]]), ...sss.filter(s => s !== pair[1]).map(s => [s, pair[0], pair[1]])]) {
          const orders = common(second, mate); if (!orders.length || common(second, keep).length || members.some(m => m !== second && m !== mate && m !== keep && common(second, m).length)) continue;
          if (fixedSheet(mate)) continue;
          for (const o of orders) {
            const pieces = (mate.charms || []).filter(c => orderOfCharm(c) === o); if (!pieces.length) continue;
            const skip = new Set(pieces), fits = p => typeof E.pairFits !== "function" || E.pairFits(p, [o], run, { skip });
            const free = p => p !== mate && p.runId === run.runId && p.metal === mate.metal && !(p.setId && !p.draft) && !(typeof E.closed === "function" && E.closed(p)) && !fixedSheetBusy(p) && fits(p);
            const own = pagesOf(mate.metal).find(p => free(p) && [...placedOrders(p)].some(q => os.get(second).has(q)));
            const elsewhere = pagesOf(mate.metal).filter(p => p !== mate && p.runId === run.runId && !p.recalled && (p.charms || []).some(c => !skip.has(c) && !c.excluded && orderOfCharm(c) === o));
            if (elsewhere.length) { const h = elsewhere.length === 1 && free(elsewhere[0]) ? elsewhere[0] : null; if (h) out.push({ set: st, from: mate, to: h, metal: mate.metal, orders: [o], second, pieces }); continue; }
            out.push({ set: st, from: mate, to: own || pagesOf(mate.metal).find(p => free(p) && (p.charms || []).length) || null, metal: mate.metal, orders: [o], second, pieces });
          }
        }
      }
      return out;
    }
    /** The ONE set a green-line sheet belongs to (greenHomesOf), for a person's Cut Sheet press: its home, else the run's newest open set (the set the page's Include puts it in). */
    function homeSetOf(p) {
      const W = cardinalView(V.run), GH = W ? greenHomesOf(W, V.run) : new Map(), h = W && GH.get(W.idOf.get(p));
      const sid = (h && h.home) || (inSetNow(p) ? p.setId : null);
      return (sid && ofRun(V.run.runId).find(s => s.setId === sid)) || newestOpenOf(V.run.runId);
    }
    /** The marks the page drew on each sheet's card (setWait, cardinalHold, cardinalNote, cardinalPull, cardinalHome, greenAway): sheetId → words. */
    function waitsOf() {
      const out = {};
      for (const p of allSheets().filter(p => p.runId === (V.run && V.run.runId) && !p.recalled)) {
        const w = {}; for (const k of ["setWait", "cardinalHold", "cardinalNote", "cardinalPull", "cardinalHome", "greenAway"]) if (p[k]) w[k] = String(p[k]).slice(0, 600);
        if (Object.keys(w).length) out[p.sheetId] = w;
      }
      return out;
    }

    return { assemble, tendSet, cardinalApply, cardinalView, greenHomesOf, formationOf, cardinalSplit, policy, basePolicy, cannotJoin, goldGap, inSetNow, picked, staysInSet, staysCut, staysJoined, staysWaiting,
      openSetsOf, membersOfSet, formedOnce, closedSet, collectingOf, newestOpenOf, waitingOf, selfCheck, invariantInput, autoCutPlan, homeSetOf, rgForeign, rgStillForeign, rgOneGroups, waitsOf,
      rgOneBusy, rgOneOrders, rgOneKey, rgOneDue, rgOnePieces, rgOneGuard, rgOneNoRoom, cutKey, lineOf, trimPlan, unpairPlan, fixedSheetBusy,
      sheetNameOf, solidMark, modern, ofRun, allSheets, approvedSet, busySheet, sheetFull, ownerMoved, leftSetOf, labelOf, rgLoose, rgSecond };
  }

  /* ══ 3 · helpers the cloud's units share ══ */
  /** The file name of a sheet in its set (charm-nest-1.html sheetFileBase): a draft (or one owed its number) is "<TAG>_working_<id>". */
  function sheetFileBase(sh, today) {
    const O = Orders();
    if (sh.draft || sh.numberOwed) return `${METAL_TAG[sh.metal]}_working_${sh.sheetId}`;
    return O.sheetName(sh.metal, sh.setDay || today, sh.seq || 1, sh.sheetIndex || sh.page || 1);
  }
  /** A set record as the page's Sets.save sends it to setUpdate (orders as { rid: { held, lines: [line] } }). */
  function setPatch(set) {
    const orders = {};
    for (const [rid, o] of Object.entries(set.orders || {})) orders[rid] = { held: (o && o.held) || null, lines: Array.isArray(o && o.lines) ? o.lines : Object.values((o && o.lines) || {}) };
    return { runId: set.runId, day: set.day, seq: set.seq, name: set.name, folder: set.folder, materials: set.materials || [], sheetIds: set.sheetIds || [], orders, status: set.status, labels: set.labels || null,
      labelFiles: (set.labelFiles || []).map(f => ({ path: f.path, url: f.url, sheet: f.sheet, sheetId: f.sheetId, part: f.part, parts: f.parts, orders: f.orders, payload: f.payload || null, metal: f.metal || null, ecc: f.ecc || null, label: f.label })),
      committed: set.committed || null, refused: set.refused || null, completedAt: set.completedAt || null, completionDay: set.completionDay || null, committedAt: set.committedAt || null, backCount: set.backCount || 0,
      ...(+set.formedAt > 0 ? { formedAt: +set.formedAt } : {}), ...(set.sheetNos && Object.keys(set.sheetNos).length ? { sheetNos: set.sheetNos } : {}) };
  }
  /** A set record as the page holds it (orders' lines keyed by transaction id). */
  function asSet(d) {
    const s = Object.assign({}, d);
    s.sheetIds = Array.isArray(d.sheetIds) ? d.sheetIds.slice() : []; s.labelFiles = Array.isArray(d.labelFiles) ? d.labelFiles.map(f => Object.assign({}, f)) : []; s.materials = Array.isArray(d.materials) ? d.materials.slice() : [];
    s.orders = {};
    for (const [rid, o] of Object.entries(d.orders || {})) { const lines = {}; for (const l of (Array.isArray(o && o.lines) ? o.lines : Object.values((o && o.lines) || {}))) if (l) lines[String(l.transactionId)] = Object.assign({}, l, { copies: (l.copies || []).map(c => Object.assign({}, c)) }); s.orders[rid] = { held: (o && o.held) || null, lines }; }
    s.formedAt = num(d.formedAt) || 0; s.committedAt = ms(d.committedAt) || d.committedAt || null; s.name = d.name || (Orders() ? Orders().setLabel(d.seq) : `Set-${d.seq}`);
    s.folder = d.folder || (Orders() ? Orders().setFolder(d.day, d.seq) : `charmnest/sets/${d.day}/Set-${d.seq}`);
    return s;
  }
  /** The copies a joined sheet adds to its set's order lines (Sets.onSheetSaved), from its placed charms and their pool rows (rowOf(poolId) → { transactionId, sku, copy, side, groupSize }). */
  function addCopies(set, sh, rowOf) {
    const CPL = CustomPool(), byId = new Map((sh.charms || []).map(c => [c.id, c])), placed = (sh.placements || []).map(p => byId.get(p.id)).filter(Boolean);
    let added = 0;
    for (const c of placed) {
      const rid = String(c.order || "").split("/")[0], row = rowOf(c.poolId) || {}, oi = c.orderInfo || {};
      const tid = oi.transactionId || row.transactionId; if (!rid || !tid || (CPL && CPL.orderless(rid))) continue;
      const o = set.orders[rid] = set.orders[rid] || { lines: {}, held: null };
      const ln = o.lines[String(tid)] = o.lines[String(tid)] || { transactionId: String(tid), sku: oi.sku || row.sku || null, copies: [] };
      if (ln.copies.some(x => x.poolId === c.poolId)) continue;
      const sd = c.side || oi.side || row.side, gs = +(c.groupSize || row.groupSize);
      ln.copies.push({ copy: oi.copy || row.copy || 1, sheetId: sh.sheetId, sheet: sh.fileBase, poolId: c.poolId, backPoolId: null, ...(sd === "L" || sd === "R" ? { side: sd } : {}), ...(gs >= 2 || (gs === 1 && (sd === "L" || sd === "R")) ? { groupSize: Math.floor(gs) } : {}) });
      added++;
    }
    return added;
  }

  return { asPage, gate, leftSetOf, sheetFileBase, setPatch, asSet, addCopies, torontoDay, METAL_TAG, METAL_WORD, labelOf, MANUAL_STALE_MS, VERSION: 1 };
});
