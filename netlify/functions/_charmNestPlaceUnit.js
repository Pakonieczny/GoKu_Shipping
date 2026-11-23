/*  netlify/functions/_charmNestPlaceUnit.js — THE PLACEMENT UNIT of the cloud driver (cloud plan v2 §1.6 item 6 and §1.7 "nest: placement, then nestStart"; build step 5: P4, P5, C4).
 *  Every ready piece of the run goes on its sheet RECORD, chosen by the rules the sorter page runs (charm-nest-place.js: R2, R16, PAIR, PAIR2, Decision 2, the green-line wait,
 *  NOSPREAD, ONEFILL / PAIRFIX, SETSHUT), and is written there by placeOnSheet (one line's pieces of one metal, ONE transaction, fenced on the driver's lease). No browser takes part.
 *  DORMANT: the driver runs this unit only for a run the cloud carries, at a step the owner listed in Charm_Nest_Open/drive.cloudSteps; with the drive absent or "off" nothing runs.
 *  It makes no Etsy call and starts no world build: it reads Firestore (the run's sheet and set records, the pool rows, the library's index) and the shared settings, and writes only
 *  through the driver's call (placeOnSheet, poolUpdate, runLinesPatch).
 *
 *  One run of the unit (bounded by the unit's time, C7; it can be run again safely and a second run writes nothing):
 *    1. reads the run's sheet records, its sets, the shared settings and plate (Charm_Nest_Shared sorter-settings, sorter-stock) and Options → Partial Sheet;
 *    2. the REPAIR: a pool row with no sheetId that a live record lists gets that sheetId (poolUpdate), so it is never placed again;
 *    3. the WAITS: a line that waits for a green line (kind "green") goes on once that layout is cut, released or gone; a line that waits for the Gold Filled / Sterling Silver sheet still
 *       filling (kind "pair") goes on once another sheet is the one filling (the page's greenResume / pairResume, from the records);
 *    4. every "pooled" line with pieces no record lists, in the page's order (attachOrder: Rose Gold / 10K / 14K lines after the order's GF / SS lines, an order with a line still to come
 *       after the others, then oldest first), its pieces of each metal: an order bigger than a sheet cut into units (NOSPREAD), the sheet chosen (chooseSheet), and either
 *       placeOnSheet or, when every metal of the line waits, the line marked "waiting" with its reason (runLinesPatch).
 *  Its writes: placeOnSheet (a new draft is made in its transaction), poolUpdate { sheetId } (the repair), runLinesPatch (waits). Placements, files, sets: never (the nest, the sheet
 *  writer and the assembly do those). A sheet record that took pieces says nestOwed (the time) and dirty: the nest's unit (build step 6) starts its nest.
 *  exports: unit (the nest step's first unit), placeAll(ctx) (the same, for a unit that calls it after intake: the checkpoint's), readWorld, geometryOf.                          */
"use strict";
const Place = require("../../charm-nest-place.js");
const Placement = require("./_charmNestPlacement");
const SheetName = require("../../charm-nest-sheet-name.js");

const SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets", POOL = "Charm_Pool", MASTER = "Charm_Master_Index";
const SHEET_FIELDS = ["metal", "runId", "page", "draft", "setId", "setSeq", "sheetIndex", "charms", "placements", "poolIds", "orders", "releaseFull", "roseCutAt", "roseCutRevision", "laserDoneAt", "archived",
  "processReady", "problem", "intakeFinalized", "rosePlanHash", "roseProtectedJson", "topup", "usablePt2", "freePt2", "density", "status", "manualEdit", "nestJob", "filesJob", "keepInSet", "ownerSet",
  "label", "roseStockId", "roseRevision", "cardStartedAt", "draftOrder", "createdAt", "layoutRev", "contentRev", "stock", "keptStock", "newStock", "nestOwed", "dirty", "cardinalPull", "greenLeftover", "fileBase", "metalLabel", "solidIncluded"];
const SET_FIELDS = ["setId", "sheetIds", "committedAt", "processReady", "laserSetPending", "laserDoneAt", "status", "runId", "seq"];
const POOL_FIELDS = ["poolId", "state", "sheetId", "runId", "orderId", "orderDate", "arrivedAt", "frontAt", "transactionId", "sku", "material", "metal", "size", "copy", "quantity", "charmHash", "aiPath", "lineKey",
  "side", "mirror", "bodyIndex", "groupKey", "groupSize", "oneBody", "custom", "cloudPlace", "hand", "customPiece", "customId", "customName", "orderless", "by", "heldAt", "removedAt", "heldBy", "removedBy", "repooledAt", "areaPt2", "widthPt", "heightPt"];
const MASTER_FIELDS = ["sku", "sizes", "aiPath", "widthPt", "heightPt", "areaPt2"];
const MADE = new Set(["committed", "engraved", "labelled"]);
const LABEL = { gold: "Gold Filled", silver: "Sterling Silver", rose: "Rose Gold", gold10k: "10K Gold", gold14k: "14K Gold" };
const SIZE_PAD_PT = 2.6;   // (charm-nest-orders.js SIZE_PAD_PT: the index's size is the outer box; a drawing scaled to a size keeps its margin)
const FIT_PATH = /#fit-([A-Z0-9.]+)@(\d+(?:\.\d+)?)$/;   // (charm-nest-pool-make.js FIT_PATH: a design drawn at a size the library does not hold)
const TAIL_MS = 3000;
const num = v => (Number.isFinite(+v) ? +v : 0);
const torontoDay = t => { try { return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Toronto", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date(t)); } catch (_) { return new Date(t).toISOString().slice(0, 10); } };
const nameOf = v => { try { return SheetName.name(v) || "a sheet"; } catch (_) { return "a sheet"; } };

/** The size the library gives a piece (pool-make's sizeEntry / scaledGeom, read from the index entry): the size the row's design file is, a drawing scaled to a size, else the size
    the row names, else the entry's own. { widthPt, heightPt, areaPt2 } or null. A row that says its own size (a cloud intake that keeps it) wins. */
function geometryOf(entry, row) {
  if (row && +row.areaPt2 > 0 && +row.widthPt > 0 && +row.heightPt > 0) return { widthPt: +row.widthPt, heightPt: +row.heightPt, areaPt2: +row.areaPt2 };
  if (!entry) return null;
  const pick = g => (g && +g.widthPt > 0 && +g.heightPt > 0 ? { widthPt: +g.widthPt, heightPt: +g.heightPt, areaPt2: +g.areaPt2 || 0 } : null);
  const sizes = entry.sizes && typeof entry.sizes === "object" ? Object.values(entry.sizes).filter(Boolean) : [], want = String((row && row.aiPath) || "");
  const same = sizes.find(z => z.aiPath && z.aiPath === want) || (entry.aiPath && entry.aiPath === want ? entry : null); if (same) return pick(same);
  const m = FIT_PATH.exec(want);
  if (m) { const basePath = want.slice(0, m.index), k = +m[2], base = sizes.find(z => z.aiPath === basePath) || (entry.aiPath === basePath ? entry : null);
    if (base && k > 0 && +base.widthPt > 0) { const dim = v => (v - SIZE_PAD_PT) * k + SIZE_PAD_PT; return { widthPt: dim(+base.widthPt), heightPt: dim(+base.heightPt), areaPt2: +base.areaPt2 > 0 ? +base.areaPt2 * k * k : 0 }; } }
  if (row && row.size && entry.sizes && entry.sizes[row.size]) return pick(entry.sizes[row.size]);
  return pick(entry);
}

/** The shop's shared settings as values (_charmNestPieceHost.js sharedValues), less what stays on each computer (charm-nest-cloud-share.js SETTINGS_HERE, as the piece maker and the nest
    leave it out). */
function sharedValues(f) { const Host = require("./_charmNestPieceHost"), out = Host.sharedValues(f); for (const k of Object.keys(out)) if (Host.SETTINGS_HERE.has(k)) delete out[k]; return out; }

/** What the unit reads: the run's live sheet records, its sets, the shared settings and plate, Options → Partial Sheet. A shared read that fails stops the unit (it is tried again): the
    choice is never made on settings that were not read. */
async function readWorld(ctx, o = {}) {
  const { db, runId, read } = ctx;
  const given = Array.isArray(o.records) ? o.records : null;   // (COMBINEDRAFTS: the combine reads the full records, hold marks included, and hands them in)
  const [sq, tq, share, partial] = await Promise.all([
    given ? { docs: [] } : db.collection(SHEETS).where("runId", "==", runId).select(...SHEET_FIELDS).get(),
    db.collection(SETS).where("runId", "==", runId).select(...SET_FIELDS).get(),
    read({ op: "shareGet", names: ["sorter-settings", "sorter-stock"] }),
    read({ op: "partialPolicyGet" })
  ]);
  for (const [what, r] of [["the shared settings", share], ["Options → Partial Sheet", partial]]) if (!r || r.status >= 400) throw new Error(`${what} could not be read (HTTP ${(r && r.status) || 503}): the pieces are placed at the next try`);
  // (shareGet answers each shared field as the store keeps it, { j: its JSON text, at, dev, by } or { x: 1 } once removed: read as values, the way every page and the nest read them)
  const items = share.items || {}, sf = sharedValues(items["sorter-settings"] && items["sorter-settings"].f), pf = sharedValues(items["sorter-stock"] && items["sorter-stock"].f);
  const preset = typeof pf.preset === "string" && pf.preset ? pf.preset : "A";
  const pol = partial.policies || {}, modes = {}; for (const m of Object.keys(pol)) modes[m] = pol[m] && pol[m].mode === "new" ? "new" : "auto";
  return {
    records: (given || sq.docs.map(d => Object.assign({}, d.data(), { id: d.id }))).filter(r => !r.archived),
    sets: tq.docs.map(d => Object.assign({ setId: d.id }, d.data())).filter(s => !/^superseded/.test(String(s.status || ""))),
    settings: sf, preset, partial: modes
  };
}

/** The unit. ctx: the driver's (db, run, runId, call, read, budgetEnd). → undefined (done) | { more: true } (its time is spent: it runs again) */
const RELINKED = new Map();
async function placeAll(ctx) {
  const { db, runId, call } = ctx, run = Object.assign({}, ctx.run || {}, { runId }), lines = run.lines || {}, now = Date.now(), end = num(ctx.budgetEnd) || now + 45000;
  const say = t => { try { console.log("[place] " + t); } catch (_) { /* a note only */ } };
  const world = await readWorld(ctx);
  /* CESEND (21 Nov 2026): ctx.customIds, the pool ids of a custom piece a viewer sent to a sheet (Charm Editor -> Send to sheet; customPiecePut rows with cloudPlace), placed by a
     person's request (runCmdPut placewaiting, _charmNestCmdExec.js): ONLY those rows are placed, and no run line is written for them (they are no line of the run). */
  const only = Array.isArray(ctx.customIds) && ctx.customIds.length ? new Set(ctx.customIds.map(String)) : null;
  const W = Place.recordHost({ run, records: world.records, sets: world.sets, settings: world.settings, preset: world.preset, partial: world.partial, now });
  const R = W.rules, out = { placed: 0, waited: 0, repaired: 0, resumed: 0 };
  // where each piece is: the live records that list it (the one truth of "on a sheet")
  const listed = new Map(); for (const r of world.records) for (const id of r.poolIds || []) listed.set(String(id), r.id);
  const patches = {}, patchLine = (key, p) => { patches[key] = Object.assign(patches[key] || {}, p); if (lines[key]) Object.assign(lines[key], p); };
  const flush = async () => { const keys = Object.keys(patches); for (let i = 0; i < keys.length; i += 300) { const part = keys.slice(i, i + 300), body = { op: "runLinesPatch", runId, lines: Object.fromEntries(part.map(k => [k, patches[k]])) }; const res = await call(body, "place-lines|" + runId + "|" + part.join(",")); if (res && res.status >= 400) throw new Error(`the run's lines could not be marked (HTTP ${res.status}): ${res.error || "no answer"}`); } for (const k of keys) delete patches[k]; };

  // ── 3. the waits that are due (the page's greenResume / pairResume, from the records) ──
  for (const [key, l] of only ? [] : Object.entries(lines)) {
    if (!l || l.state !== "waiting" || !l.wait || !["green", "pair"].includes(l.wait.kind)) continue;
    const w = l.wait, metal = w.material || l.material;
    let due;
    if (w.kind === "green") { const was = W.views().find(v => v.sheetId && v.sheetId === w.sheetId); due = !was || !R.greenOpen(was, run); }
    else { const f = W.fillingPage(metal); due = !w.sheetId || !f || f.sheetId !== w.sheetId; }
    if (!due) continue;
    patchLine(key, { state: (l.poolIds || []).length ? "pooled" : "pulled", wait: null, reason: null }); out.resumed++;   // (a line that kept its pieces goes on now; one that kept none is made up again by the intake, as on the page)
  }

  // ── the lines whose pieces wait to be placed, and their rows ──
  const cand = only ? [] : Object.entries(lines).filter(([k, l]) => l && l.state === "pooled" && Array.isArray(l.poolIds) && l.poolIds.length && (!ctx.only || ctx.only.has(k)));   // (CESEND: customIds, a viewer's pieces alone; CUSTOMSEND: ctx.only, a person's Send to Sheet places its own lines only)
  const ids = [...new Set(cand.flatMap(([, l]) => l.poolIds.map(String)).concat(only ? [...only] : []))], rows = new Map();
  for (let i = 0; i < ids.length; i += 100) { const snaps = await db.getAll(...ids.slice(i, i + 100).map(id => db.collection(POOL).doc(id)), { fieldMask: POOL_FIELDS }); for (const s of snaps) if (s.exists) rows.set(s.id, Object.assign({ poolId: s.id }, s.data())); }

  // ── 2. the repair: a row with no sheetId that a live record lists gets that sheetId, and is never placed again ──
  { const bySheet = new Map(); for (const [id, r] of rows) if (!r.sheetId && listed.has(id) && !Placement.takenOff(r)) { const sid = listed.get(id); if (!bySheet.has(sid)) bySheet.set(sid, []); bySheet.get(sid).push(id); }
    for (const [sid, list] of bySheet) for (let i = 0; i < list.length; i += 400) { const part = list.slice(i, i + 400), res = await call({ op: "poolUpdate", poolIds: part, patch: { sheetId: sid } }, "place-repair|" + runId + "|" + sid + "|" + part.join(",")); if (res && res.status < 400) { out.repaired += part.length; for (const id of part) rows.get(id).sheetId = sid; } } }

  /* RELINK: a released order's piece placed before its saved engraving could follow it (4230787700, 4238855149): its sheet takes it back, once an hour a piece (_charmNestPlaceOp.js relinkBacks; nothing when there is none) */
  { const byId = new Map(world.records.map(r => [r.id, r])), want = new Map();
    for (const [key, l] of cand) { if (!(num(l.frontAt) > now - 3 * 86400000)) continue; for (const id0 of l.poolIds) { const pid = String(id0), sid = listed.get(pid), rec = sid && byId.get(sid); if (!rec || (rec.backPool || []).some(x => x && String(x.poolId) === pid) || now - num(RELINKED.get(pid)) < 3600000) continue; RELINKED.set(pid, now); if (!want.has(sid)) want.set(sid, []); want.get(sid).push(pid); } }
    for (const [sid, list] of want) { try { const r = await call({ op: "relinkBacks", runId, sheetId: sid, poolIds: list.slice(0, 60) }, "relink|" + runId + "|" + sid + "|" + list.join(",").slice(0, 300) + "|" + Math.floor(now / 3600000)); if (r && r.relinked) say(`${runId}: ${r.relinked} saved engraving(s) back on ${sid}`); } catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; } } }
  // (CESEND: the custom piece's rows, one candidate per line key of the send, never a line of the run)
  if (only) { const by = new Map(); for (const id of only) { const r = rows.get(id); if (!r || !r.custom || !r.cloudPlace) continue; const k = String(r.lineKey || id); if (!by.has(k)) by.set(k, { orderId: r.orderId, poolIds: [], state: "pooled", custom: true }); by.get(k).poolIds.push(id); } for (const [k, l] of by) cand.push([k, l]); }
  // ── 4. the pieces no record lists, by line and metal ──
  /* (a custom piece is placed by the page that sent it (charm-nest-custom-pool.js addCustomPiece) and never by the cloud, which would put it on a second sheet; CESEND: except one a viewer
     sent for the cloud to place (cloudPlace, on this run), named by its request) */
  const customOk = r => !r.custom || (!!only && only.has(String(r.poolId)) && r.cloudPlace === true && r.runId === runId);
  /* CUSTOMSEND (task A, 21 Nov 2026): a custom piece is placed when it is a piece of one of the run's order lines (a card's Send to Sheet, made by the cloud's intake from the card's
     designs, its size on its row). A free custom piece of the Charm Editor (no line of the run, no size on the row) is still left alone here, as before. */
  const customOfLine = r => !!(r.lineKey && lines[r.lineKey] && (lines[r.lineKey].poolIds || []).map(String).includes(String(r.poolId)) && +r.areaPt2 > 0 && +r.widthPt > 0 && +r.heightPt > 0);
  const placeable = r => !!r && !listed.has(String(r.poolId)) && !Placement.takenOff(r) && !Placement.TAKE_OFF_STATES.has(r.state) && !MADE.has(r.state) && (customOk(r) || customOfLine(r)) && (!r.runId || r.runId === runId) && Place.METALS.includes(r.material || r.metal);
  const todo = cand.map(([key, l]) => ({ key, l, rows: l.poolIds.map(id => rows.get(String(id))).filter(placeable) })).filter(x => x.rows.length);
  if (!todo.length && only) return { only: true, placed: 0, why: "" };
  if (!todo.length) { await flush(); if (out.resumed || out.repaired) say(`${runId}: ${out.resumed} waiting line(s) go on, ${out.repaired} piece row(s) repaired`); return { cursor: { placeWait: null } }; }
  // the size of each piece, from its row or the library's index (read once, for the SKUs that need it)
  const skus = [...new Set(todo.flatMap(x => x.rows).filter(r => !(+r.areaPt2 > 0)).map(r => String(r.sku || "").trim().toUpperCase()).filter(s => s && !s.includes("/")))], entries = new Map();
  for (let i = 0; i < skus.length; i += 100) { const snaps = await db.getAll(...skus.slice(i, i + 100).map(s => db.collection(MASTER).doc(s)), { fieldMask: MASTER_FIELDS }); for (const s of snaps) if (s.exists) entries.set(s.id, s.data()); }
  const pieces = new Map(), pieceOf = r => { if (!pieces.has(r.poolId)) pieces.set(r.poolId, customCharm(Place.charmOfRow(Object.assign({}, r, { material: r.material || r.metal }), geometryOf(entries.get(String(r.sku || "").trim().toUpperCase()), r), now), r)); return pieces.get(r.poolId); };
  // the batch (what the intake knows of each order's pieces in hand, NOSPREAD) and the totals
  const batch = new Map(); for (const x of todo) for (const r of x.rows) { const k = String(r.orderId || x.l.orderId) + "|" + (r.material || r.metal); if (!batch.has(k)) batch.set(k, []); batch.get(k).push(pieceOf(r)); }
  const totals = Place.pieceTotalsRec(lines, W.views());
  // the order lines are put on their sheets in (attachOrder): GF / SS lines before Rose Gold / 10K / 14K lines (Decision 2), an order with a line still to come after the others (NOSPREAD), oldest first
  const metalOf = x => x.rows[0].material || x.rows[0].metal, behind = x => { if (world.settings.nospread === "off") return 0; const m = metalOf(x), info = Place.orderInfoRec(String(x.l.orderId), m, x.rows.map(pieceOf), batch, totals); return info.expected > info.pieces.length + info.placed ? 1 : 0; };
  const firstAt = x => num(x.l.arrivedAt) || num(x.l.createTs) || 0;
  const ordered = todo.map(x => [x, Place.CUT_METALS.includes(metalOf(x)) ? 1 : 0, behind(x)]).sort((a, b) => a[1] - b[1] || a[2] - b[2] || firstAt(a[0]) - firstAt(b[0]) || String(a[0].key).localeCompare(String(b[0].key))).map(a => a[0]);
  const day = String(run.day || torontoDay(now));

  /* one line's pieces of one metal onto the chosen sheet: "ok" | "retry" (the sheet changed or refused: read again, choose again) | "skip" */
  async function place(view, list, home, key, metal) {
    const slim = list.map(c => Object.assign({}, c));
    const body = view._new ? { op: "placeOnSheet", runId, create: { metal, page: view.page, day, greenLeftover: view.greenLeftover || null }, pieces: slim, home }
      : { op: "placeOnSheet", runId, sheetId: view.sheetId, layoutBase: num(view.layoutRev), contentBase: num(view.contentRev), pieces: slim, home };
    const res = await call(body, "place|" + runId + "|" + key + "|" + metal);
    if (res && res.ok) {
      const rec0 = view._new ? { id: res.sheetId, runId, metal, page: view.page, draft: true, setId: null, charms: [], poolIds: [], placements: [], cardStartedAt: Date.now(), greenLeftover: view.greenLeftover || null } : Object.assign({}, view._rec);
      const had = new Map((rec0.charms || []).map((c, i) => [String(c && c.poolId), i])), charms = (rec0.charms || []).slice();
      for (const c of slim) { const i = had.get(String(c.poolId)); if (i != null) charms[i] = Object.assign({}, c, charms[i], { placedBy: "Cloud", excluded: false }, c.chunk ? { chunk: c.chunk } : {}); else charms.push(c); }   // (as placeOnSheet keeps it)
      const rec = Object.assign(rec0, { charms, poolIds: (rec0.poolIds || []).map(String).concat(res.placed || []), layoutRev: res.layoutRev, contentRev: res.contentRev, nestOwed: Date.now(), dirty: true });
      if (view._new) W.made(view, rec); else W.refresh(rec);
      for (const id of (res.placed || []).concat(res.already || [])) listed.set(String(id), res.sheetId);
      out.placed += (res.placed || []).length;
      return "ok";
    }
    const status = res ? +res.status || 0 : 0;
    if (status === 409 && res.conflict) { if (view.sheetId) { const s = await db.collection(SHEETS).doc(view.sheetId).get(); if (s.exists && !(s.data() || {}).archived) W.refresh(Object.assign({}, s.data(), { id: s.id })); else view._shut = true; } return "retry"; }
    if ((status === 409 || status === 413 || status === 404) && ["closed", "setShut", "cut", "archived", "run", "size"].includes(res.refused || (res.gone ? "archived" : ""))) { view._shut = true; return "retry"; }
    if (status === 404) { view._shut = true; return "retry"; }
    if (status >= 500 || !res) throw new Error(`a line's pieces could not be placed (HTTP ${status || 503}): ${(res && res.error) || "no answer"}`);
    say(`${runId} · ${key}: not placed (${(res && res.error) || "refused"})`);   // (a piece another sheet lists, one taken off meanwhile: the next run of the unit sees it as it is)
    return "skip";
  }

  /* RGLINK: a Rose Gold draft an order would mix a second SET on (RGSETS) is marked to be cut at once, once (the mark is the green line's link cut's to-do; the draft takes nothing meanwhile) */
  const linkWords = (nm, why) => (why === "third" ? `it would tie ${nm} to a third Gold Filled / Sterling Silver sheet` : `its other pieces go with another set than ${nm}'s`);
  async function markLinkCut(view, rid, why) {
    if (!view || !view.sheetId || view.roseLinkCut || view.metal !== "rose") return;
    const mark = { at: Date.now(), orderId: String(rid), by: "Cloud" };
    const res = await call({ op: "putSheet", sheet: { id: view.sheetId, roseLinkCut: mark } }, "rg-link|" + runId + "|" + view.sheetId);
    if (res && res.status < 400) { view.roseLinkCut = mark; if (view._rec) view._rec.roseLinkCut = mark; say(`${nameOf(view)}: its green line is cut now (order ${rid}: ${linkWords(nameOf(view), why)}); the order goes on its leftover`); }
    else say(`${nameOf(view)}: its green line cut could not be asked (${(res && res.error) || "no answer"}); asked again on the next pass`);
  }

  /* (the page's LiveNest.add: "A sheet is still being written": no new piece of a metal goes on while a sheet of that metal is nested or written; those lines wait for it, the others go on) */
  const busy = new Map(); for (const v of W.views()) if (v._inplaceHold && !busy.has(v.metal)) busy.set(v.metal, v);
  const deferred = [];
  for (const x of ordered) {
    if (Date.now() > end - TAIL_MS) { await flush(); say(`${runId}: ${out.placed} piece(s) placed so far; the rest in the next turn`); return { more: true, placed: out.placed }; }
    const rid = String(x.l.orderId || x.rows[0].orderId), byMetal = new Map();
    for (const r of x.rows) { const m = r.material || r.metal; if (!byMetal.has(m)) byMetal.set(m, []); byMetal.get(m).push(pieceOf(r)); }
    { const held = [...byMetal.keys()].map(m => busy.get(m)).find(Boolean); if (held) { deferred.push(held); continue; } }
    const on = (x.l.poolIds || []).map(String).filter(id => listed.has(id));
    let waited = 0; const waits = [];
    for (const [metal, list] of byMetal) {
      const info = Place.orderInfoRec(rid, metal, list, batch, totals);
      // (NOSPREAD, Paul 17 Oct 2026: an order bigger than one sheet is cut into units that each fit one, before its sheet is looked for)
      if (info && !info.pieces.some(c => c.chunk)) Place.nospreadChunk(metal, info.pieces, info, W.env);
      let done = false;
      for (let attempt = 0; attempt < 3 && !done; attempt++) {
        const mates = [...new Set(on.map(id => listed.get(id)))].map(sid => W.views().find(v => v.sheetId === sid)).filter(Boolean);
        const d = R.chooseSheet({ metal, rid, run, list, info, matePages: mates, byMetalSize: byMetal.size });
        for (const ev of d.events) if (ev.kind === "split") say(`Split order ${rid} (a split of necessity: its other ${LABEL[metal]} pieces are on ${ev.apart.map(nameOf).join(", ")}, which can take no more) — both sheets must go in one set`);
        /* RGLINK (Paul, 23 Nov 2026: "forces the Green Cut line to occure so as to not connect 2 GF or SS sheets to a single RG sheet"; RGSETS, the same day: only when the pieces belong to a different set): the Rose Gold
           draft this order would mix a second set on is marked to be cut at once (roseLinkCut); the green line's link cut (_charmNestGreenLine.js linkUnit) cuts it around what it holds, then this line goes on its leftover */
        if (d.wait && d.wait.cut === "link") await markLinkCut(d.wait.sheet, rid, d.wait.why);
        if (d.wait) { waits.push({ metal, w: d.wait, n: list.length }); waited++; done = true; break; }
        const home = d.back && d.page === d.back ? rid : null, got = await place(d.page, list, home, x.key, metal);
        if (got === "ok") { say(`${rid} · ${list.length} ${LABEL[metal]} piece(s) → ${nameOf(W.views().find(v => v.sheetId && list.every(c => listed.get(String(c.poolId)) === v.sheetId)) || d.page)}`); done = true; }
        else if (got === "skip") done = true;
      }
    }
    // (RGC1 / PAIRFIX: a line all of whose pieces wait is not on a sheet yet: it says so on its line, and goes on once the sheet it waits for is cut or full)
    if (waited && waited === byMetal.size && x.l.custom) { const { metal, w } = waits[0]; out.waitWhy = `it waits for ${nameOf(w.sheet)} (one ${LABEL[metal]} sheet is filled at a time): ask again once that sheet is cut or full`; continue; }   // (CESEND: no run line to mark)
    if (waited && waited === byMetal.size) {
      const { metal, w, n } = waits[0], s = w.sheet, nm = nameOf(s), other = metal === "gold" ? "Sterling Silver" : "Gold Filled";
      const reason = w.kind === "green" && w.cut === "link" ? `waits while the green line of ${nm} is cut${w.why ? ` (${linkWords(nm, w.why)})` : ""}, then goes on its leftover`
        : w.kind === "green" ? `waits for the green line on ${nm}, then continues on its leftover (one ${LABEL[metal]} sheet is filled at a time)`
        : `waits for ${nm} to fill, then goes on the next ${LABEL[metal]} sheet (${nm} already shares its orders with another ${other} sheet; one ${LABEL[metal]} sheet is filled at a time)`;
      patchLine(x.key, { state: "waiting", wait: { kind: w.kind, material: metal, sheetId: (s && s.sheetId) || null, page: (s && s.page) || null, at: now, ...(w.cut ? { cut: w.cut } : {}) }, reason });
      out.waited++; say(`${rid}: its ${n} ${LABEL[metal]} piece(s) ${reason}`);
    }
  }
  await flush();
  if (only) return { only: true, placed: out.placed, why: out.waitWhy || (deferred.length ? `${nameOf(deferred[0])} is being nested: it goes on once that ends` : "") };
  if (out.placed || out.waited || out.repaired || out.resumed) say(`${runId}: ${out.placed} piece(s) placed, ${out.waited} line(s) wait, ${out.resumed} wait(s) over, ${out.repaired} row(s) repaired`);
  /* (NESTBUSY: lines that wait for a sheet being nested do not stop the step here: the nest unit after this one still runs this pass (it moves that nest along), and it is the one that
     waits, sending the step back to this unit so those lines go on once the nest ends: the deferral rides on the cursor, placeWait) */
  if (deferred.length) { const names = [...new Set(deferred.map(nameOf))]; return { cursor: { placeWait: { ids: [...new Set(deferred.map(v => v.sheetId).filter(Boolean))], why: `${deferred.length} order line(s) wait for ${names.slice(0, 3).join(", ")}${names.length > 3 ? " and more" : ""} to finish nesting, then go on their sheets` } } }; }
  return { cursor: { placeWait: null } };
}

/** CESEND: a custom piece's charm as the cloud places it: its name, its exact size kept (frozen, as the page's addCustomPiece sets it: the nest writes it at 100 % and the sheet's files
    are checked against its hash), and its design file named as the sheet record's source (the cloud's nest makes the piece from it, _charmNestNestPieces.js charmOf). Others: as they are. */
function customCharm(c, r) {
  if (!r || !r.custom || !r.cloudPlace) return c;
  if (r.hand === true || (r.customPiece && r.customPiece.hand === true)) return handCharm(c, r);   // (S3: artwork a person added by hand)
  const cp = r.customPiece || {}, n = Math.max(1, +r.quantity || 1), nm = r.orderless ? `${r.customName || cp.name || "Custom"} · Custom · no order` : `${r.orderId} · Custom · ${r.customName || cp.name || "design"}`;
  return Object.assign(c, { name: nm + (n > 1 ? ` · ${+r.copy || 1}/${n}` : ""), sourceName: `${r.customName || cp.name || cp.designId || "custom"} (custom design)`, custom: true,
    frozen: { designId: cp.designId || null, version: +cp.version || 0, designHash: cp.designHash || null, geomHash: cp.geomHash || null, ...(cp.penHash ? { penHash: cp.penHash } : {}), widthMm: cp.widthMm, heightMm: cp.heightMm, customId: r.customId || null, name: r.customName || null, orderless: !!r.orderless, by: r.by || cp.by || null, at: +r.orderDate || 0 },
    source: { id: c.sourceId, name: `${r.customName || cp.name || cp.designId || "custom"} (custom design)`, path: r.aiPath || null } });
}

/** S3: artwork a person added by hand on a staff computer in mode "on" (one design of a dropped file, sent as a piece of no order: customPiecePut hand): named as the person's file
    names it, NEVER frozen (it is no design of the design library: the cloud nests it as a hand-dropped charm is nested on a page, and no design-library hash is checked in its
    files), marked hand (its card offers Exclude / Include and Move to <metal>), and its own one-charm file named as the record's source under a name of its own (its hash in it, so
    two pieces given the same name are never made from each other's file: _charmNestNestPieces.js charmOf finds the source by id or by name). */
function handCharm(c, r) {
  const cp = r.customPiece || {}, n = Math.max(1, +r.quantity || 1), nm = String(r.customName || cp.name || "Artwork").slice(0, 80), h = String(r.charmHash || cp.geomHash || r.poolId || "").slice(0, 24);
  delete c.frozen;
  return Object.assign(c, { name: `${nm} · by hand · no order` + (n > 1 ? ` · ${+r.copy || 1}/${n}` : ""), sourceName: `${nm} (added by hand · ${h})`, custom: true, hand: true,
    source: { id: c.sourceId, name: `${nm} (added by hand · ${h})`, path: r.aiPath || null } });
}

module.exports = { unit: placeAll, placeAll, readWorld, geometryOf, customCharm };
