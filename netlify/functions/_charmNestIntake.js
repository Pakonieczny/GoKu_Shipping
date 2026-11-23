/*  netlify/functions/_charmNestIntake.js — NEW ORDERS JOIN THE RUN AND BECOME POOL PIECES, IN THE CLOUD (cloud plan v2, build step 4: the driver's intake unit; Paul, 20 Nov 2026:
 *  "no production work runs on any computer"). DORMANT: it runs only inside a pass of the cloud driver (_charmNestDrive.js), which runs only while Charm_Nest_Open/drive is "standby" or
 *  "on", and only for the steps drive.cloudSteps names ("pull": new orders join the run; "pool": its lines become pool pieces). cloudSteps is empty until build step 8.
 *
 *  What a page does today, and what does it here (charm-nest-bridge.js):
 *    Arrivals.check + merge      → join(): the open orders are the cloud world's lines (Charm_World_Lines, built by the world's own two-minute cron: the ONLY code that may ask Etsy, never
 *                                  started from here), picked by the pull rule (Settings → Pull orders, pullPick: Toronto time), the orders the Design Station completed left out
 *                                  (its ledger, NOREPULL), the cancelled left out (Charm_Nest_Cancelled, read again just before the lines are added), first-seen stamps (arrivalRecord),
 *                                  "Pulled" on each order's timeline, then runLinesAdd (300 keys a call at most). A line joins whatever the run is doing, as merge joins it (Q17).
 *    Orders.pull (step "pull")   → the pull unit: the same, for a run that starts; nothing picked: nothing to cut.
 *    processPending + addAll     → pool(): behind the gate (C5: runMode auto; the run processed, in review at engraving, at its checkpoint, or parked there by the cloud; never an
 *      (step "pool", checkpoint)  operator's stop; no sheet of the run being nested or written), each line that is due is made up into its pieces by charm-nest-pool-make.js, the page's
 *                                  own code (_charmNestPieceHost.js: the same settings, plates, library and design files, from the cloud), its pool rows written with poolPut (200
 *                                  rows a call at most, a line never split), the run's line said "pooled" with its pool ids, or held and why (runLinesPatch, with poolProblems and
 *                                  designHold). Choosing each piece's sheet is build step 5's: a line here ends "pooled", its rows "ready" with no sheet.
 *  Specs come from the world (the reading every computer shows), never from interpretAll here, and never from Etsy. A line whose reading in the world is older than the person's last choice
 *  on it (an override, a release) waits for the next world build. Every write goes through the driver's fenced calls (Lib.call) or a transaction that reads the driver's lease and the run's
 *  carrier first; the per-line ledger (Charm_Nest_Intake_Lines/<lineKey>: the signature a held line was tried with, and a passing failure's next try) keeps a second pass from writing
 *  anything when nothing changed. A line that a computer already made up keeps its pieces (the cloud never makes them again: it cannot tell their body from a hash it draws an edge pixel
 *  apart, BODYID). Lines with a send to sheet (CustomSheet), and the take-off of cancelled or completed pieces, are left to their own jobs.                                          */
"use strict";
const O = require("../../charm-nest-orders.js");
const PoolMake = require("../../charm-nest-pool-make.js");
const Host = require("./_charmNestPieceHost");
const Place = require("../../charm-nest-place.js");

const LINES = "Charm_World_Lines", WORLD = "Charm_World", POOL = "Charm_Pool", SHEETS = "Charm_Nest_Sheets", SETS = "Charm_Nest_Sets", CANCELLED = "Charm_Nest_Cancelled";
const COMPLETED = "Design_Completed Orders", CUSTOMREAD = "Charm_Nest_CustomRead", LEDGER = "Charm_Nest_Intake_Lines", CARRIER = "Charm_Nest_Run_Carrier", MARK = ["Charm_Nest_Drive", "intake"];
const TZ = "America/Toronto", CLOUD_DEV = "cloud";
const WORLD_FRESH_MS = 10 * 60000, FULL_SWEEP_MS = 30 * 60000, SOFT_FAILS = 6;
const POOL_STATES = new Set(["pulled", "held", "unmatched", "oversize", "waiting"]), TAKEN_OFF = new Set(["abandoned", "superseded"]);
const POOL_KINDS = new Set(["unmatchedSku", "blockedSku", "missingSize", "oversize"]);
const RETRY_STEPS = [30000, 60000, 120000];   // (bridge Pool.retryLater: 30 s, 1 min, 2 min, then every RETRY_MS, 5 min)
const PUT_ROWS = 200, LINE_KEYS = 300, TL_EVENTS = 100;
/* what a person decides on a line that its reading depends on (charm-nest-bridge.js readLine / afterRead): the world's reading is used only when it was made with the same */
const READ_FACTS = ["blockOverride", "skuOverride", "materialOverride", "sizeOverride", "releaseAnyway", "engravingOverride"];
/* what the intake writes on a run's line (charmNestLibrary LINE_PATCH_CLOUD) */
const PATCH = ["state", "poolIds", "reason", "wait", "groupSize", "sides", "pieceCount", "material", "sku", "quantity", "noDesign", "poolProblems", "designHold"];

const num = v => (Number.isFinite(+v) ? +v : 0);
const J = v => { try { return JSON.stringify(v === undefined ? null : v); } catch (_) { return ""; } };
const clone = v => (v == null ? v : JSON.parse(JSON.stringify(v)));
const same = (a, b) => J(a == null ? null : a) === J(b == null ? null : b);
const ridOf = key => String(key).split("_")[0];
const chunks = (a, n) => { const out = []; for (let i = 0; i < a.length; i += n) out.push(a.slice(i, i + n)); return out; };
const fencedErr = why => Object.assign(new Error("fenced: " + why), { fenced: true });
/** A cloud call that was refused or failed, in words with its HTTP status: a 5xx reads as passing to the driver (tried again 4 and 8 s later, then by the cron), a refusal as an error. */
const answerErr = (what, r) => Object.assign(new Error(`${what} (${r && r.status ? "HTTP " + r.status : "no answer"}): ${String((r && r.error) || "no answer").slice(0, 200)}`), { status: r ? r.status : 0 });

/* ═══ 1 · reads ═══ */
/** A world line as the pages read it (_charmNestWorld.js lineOf): the JSON text opened, the query fields beside it. */
function lineOf(d) { if (!d) return null; if (d.gone) return { key: d.key, gone: true, at: d.at }; let x = {}; try { x = JSON.parse(d.data || "{}"); } catch (_) { x = {}; } return Object.assign(x, { key: d.key || x.key, sig: d.sig, at: d.at }); }
async function worldLines(db, keys) {
  const out = new Map();
  for (const part of chunks([...new Set(keys.map(String))], 300)) for (const d of await db.getAll(...part.map(k => db.collection(LINES).doc(k)))) if (d.exists) out.set(d.id, lineOf(Object.assign({ key: d.id }, d.data())));
  return out;
}
async function docsOf(db, coll, ids) {
  const out = new Map();
  for (const part of chunks([...new Set(ids.map(String).filter(Boolean))], 300)) for (const d of await db.getAll(...part.map(id => db.collection(coll).doc(id)))) if (d.exists) out.set(d.id, d.data() || {});
  return out;
}
/** The receipts among these that a cancel record names (Charm_Nest_Cancelled/<receipt>: the record exists while the order is cancelled; a restore deletes it). */
async function cancelledOf(db, rids) { return new Set((await docsOf(db, CANCELLED, rids)).keys()); }
/** NOREPULL / NOPOOLDONE: the receipts among these the Design Station's ledger says it completed ("Design_Completed Orders"/<receipt>), or null when the ledger cannot be read now. */
async function completedOf(db, rids) {
  try { const out = new Set(); for (const part of chunks([...new Set(rids.map(String))], 100)) for (const d of await db.getAll(...part.map(id => db.collection(COMPLETED).doc(id)))) if (d.exists) out.add(d.id); return out; }
  catch (_) { return null; }
}
/** The run's sheet records (and its sets), for what a page knows of its own sheets: the pieces they list, the pieces a cleanup took off on purpose, a sheet being nested or written. */
/** slim (CMDFAST): only what the piece maker asks of a sheet, for a person's few lines: which pieces it lists, what a cleanup took off, whether it is cut. */
const SLIM = ["runId", "archived", "poolIds", "cleanup", "roseCutAt", "laserDoneAt"];
async function sheetsOfRun(db, runId, slim) {
  const sheets = new Map(), listed = new Set(), cleared = new Set();
  const [ss, st] = await Promise.all([(slim ? db.collection(SHEETS).where("runId", "==", runId).select(...SLIM) : db.collection(SHEETS).where("runId", "==", runId)).get(), db.collection(SETS).where("runId", "==", runId).get().catch(() => ({ docs: [] }))]);
  for (const d of ss.docs) { const s = Object.assign({ id: d.id }, d.data() || {}); sheets.set(d.id, s); if (!s.archived) for (const id of s.poolIds || []) listed.add(String(id)); if (s.cleanup) for (const id of [].concat(s.cleanup.removedPoolIds || [])) cleared.add(String(id)); }
  for (const d of st.docs) { const x = d.data() || {}; if (x.cleanup) for (const id of [].concat(x.cleanup.removedPoolIds || [])) cleared.add(String(id)); }
  return { sheets, listed, cleared };
}
/* A cloud task's mark on a sheet record, in the shape each task writes it: the nest's nestJob { id, status, at, attempt } (_charmNestNest.js: "queued" from nestStart until its slots end
   it "nested", "error", "stopped" or "stale"), the sheet writer's filesJob { jobId, at } (cleared when it ends). Alive while it is named, has not ended, and is younger than the sheet
   choice's JOB_STALE_MS (charm-nest-place.js: a task older than that died). */
const TASK_ENDED = new Set(["nested", "applied", "done", "error", "stopped", "stale", "superseded", "failed", "skipped"]);
const taskAlive = (j, now) => !!(j && typeof j === "object" && (j.jobId || j.id) && !(num(j.endedAt) > 0) && !TASK_ENDED.has(String(j.status || j.state || "")) && !(num(j.at) > 0 && now - num(j.at) > Place.JOB_STALE_MS));
/** A sheet of the run being nested or written now (the page's statuses, or the cloud's own nest and files tasks, build steps 6 and 7): new orders wait for it (C5). A nest that ended
    (its mark says "nested", "error", "stopped" or "stale") is no reason to wait. */
const busySheet = (s, now) => !!s && !s.archived && (["nesting", "finishing", "queued"].includes(s.status) || taskAlive(s.nestJob, now || Date.now()) || taskAlive(s.filesJob, now || Date.now()));

/** The intake's own fenced write (the ledger, its marker): one transaction that reads the driver's lease (this pass's nonce, running) and the run's carrier ("cloud") first, as the
 *  library's fenceTx does for every cloud call. writes: [[ref, data | null (delete), options]]. */
async function fencedWrite(db, fence, writes) {
  if (!writes.length) return 0;
  if (!fence || !fence.path || !fence.nonce) throw fencedErr("the intake writes only inside a pass of the cloud driver");
  const seg = String(fence.path).split("/");
  for (const part of chunks(writes, 200)) {
    await db.runTransaction(async tx => {
      const ls = await tx.get(db.collection(seg[0]).doc(seg[1])), L = ls.exists ? ls.data() || {} : {};
      if (L.nonce !== fence.nonce || L.state !== "running") throw fencedErr("another pass of the cloud holds the lease now; this write was not made");
      if (fence.runId) { const cs = await tx.get(db.collection(CARRIER).doc(fence.runId)), c = cs.exists ? (cs.data() || {}).carrier : null; if (!c || c.dev !== CLOUD_DEV) throw fencedErr(`${(c && c.by) || "a computer"} carries this run now; the cloud's write was not made`); }
      for (const [ref, d, o] of part) { if (d === null) tx.delete(ref); else tx.set(ref, d, o || {}); }
    });
  }
  return writes.length;
}

/* ═══ 2 · the pull rule (bridge Orders.applyPullRule; cloud plan P13) ═══ */
/** The instant (ms) a day "YYYY-MM-DD" ends (23:59:59) in a time zone: the page reads the browser's own zone; the cloud reads the shop's, Toronto. NaN when the day does not read. */
function endOfDay(day, tz) {
  const [y, m, d] = String(day || "").split("-").map(Number), guess = Date.UTC(y, m - 1, d, 23, 59, 59);
  if (!Number.isFinite(guess)) return NaN;
  const f = new Intl.DateTimeFormat("en-US", { timeZone: tz || TZ, hourCycle: "h23", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" });
  const off = t => { const p = {}; for (const x of f.formatToParts(new Date(t))) if (x.type !== "literal") p[x.type] = +x.value; return Date.UTC(p.year, p.month - 1, p.day, p.hour % 24, p.minute, p.second) - t; };
  const t1 = guess - off(guess); return guess - off(t1);
}
/** Settings → Pull orders over the open orders: every order (all), those due by a day (dueBy: ship-by up to the end of that day), or the N most urgent (count); an order a cancel record
 *  names is never in it. Sorted by ship-by date, as the page sorts. orders: [{ receiptId, shipBy (s) }]; settings: { pullMode, pullDueBy, pullCount }. */
function pullPick(orders, settings, cancelledIds, tz) {
  const s = settings || {}, mode = s.pullMode || "all", cx = cancelledIds instanceof Set ? cancelledIds : new Set((cancelledIds || []).map(String));
  let list = (orders || []).filter(o => o && !cx.has(String(o.receiptId))).sort((a, b) => (a.shipBy || 9e12) - (b.shipBy || 9e12));
  if (mode === "dueBy" && s.pullDueBy) { const end = endOfDay(s.pullDueBy, tz || TZ) / 1000; list = list.filter(o => o.shipBy && o.shipBy <= end); }
  if (mode === "count") list = list.slice(0, Math.max(1, +s.pullCount || 40));
  return list;
}

/* ═══ 3 · new orders join the run (bridge Arrivals.merge, Orders.pull) ═══ */
async function marker(db) { const s = await db.collection(MARK[0]).doc(MARK[1]).get(); return s.exists ? s.data() || {} : {}; }
/**
 * join(c, o) → { added, picked, orders, wait?, why?, skipped? }
 *   c: the intake's context { db, run, runId, call, read, fence, say, settings, budgetEnd }; o: { how: "pull" (a run that starts: every open order the rule picks) | "arrival" }
 */
async function join(c, o = {}) {
  const { db, run, runId } = c, now = Date.now(), how = o.how === "pull" ? "pull" : "arrival", out = { added: 0, picked: 0, orders: 0 };
  const sum = await db.collection(WORLD).doc("summary").get(), S = sum.exists ? sum.data() || {} : null, worldAt = S ? num(S.at) : 0;
  if (!S || !worldAt) return Object.assign(out, { wait: "world", why: "Waiting for the cloud's list of open orders: it is not built yet" });
  if (now - worldAt > WORLD_FRESH_MS) return Object.assign(out, { wait: "world", why: `Waiting for the cloud's list of open orders: it was last built ${Math.round((now - worldAt) / 60000)} min ago` });
  // (a list built while another run was open read each line with that run's choices: this run waits for the next build, at most two minutes)
  if (S.runId && S.runId !== runId) return Object.assign(out, { wait: "world", why: "Waiting for the cloud's list of open orders to be read for this run (it is built every 2 minutes)" });
  const mk = await marker(db), mine = mk.runId === runId;
  // (nothing was built since the last look at this run, and the half-hourly sweep is not due: nothing new can be there; the settings are not even read)
  if (how !== "pull" && mine && worldAt <= num(mk.worldAt) && now - num(mk.fullAt) <= FULL_SWEEP_MS) { if (num(mk.fails)) await saveMarker(c, { fails: 0 }); return Object.assign(out, { skipped: "the world was not built again since the last look" }); }
  const settings = await settingsOf(c);
  const full = how === "pull" || (settings.pullMode || "all") !== "all" || !mine || now - num(mk.fullAt) > FULL_SWEEP_MS;
  // the open lines: every one (a run that starts, a pull rule that is not "all", or every half hour), else those the world wrote since the last look
  const snap = full ? await db.collection(LINES).where("gone", "==", false).get() : await db.collection(LINES).where("at", ">", num(mk.worldAt)).get();
  const read = new Map(); for (const d of snap.docs) { const l = lineOf(Object.assign({ key: d.id }, d.data())); if (l && !l.gone) read.set(d.id, l); }
  // (the Design Station's snapshot shape: one order per receipt, from its open lines; CloudWorld.snapshot)
  const byRid = new Map(); for (const l of read.values()) if (l.order && l.etsyOpen && !byRid.has(String(l.order.receiptId))) byRid.set(String(l.order.receiptId), l.order);
  const lines = run.lines || {}, picked = pullPick([...byRid.values()], settings, new Set(), TZ);
  out.picked = picked.length;
  // the picked orders' lines the run does not hold yet (an order's every line, read from the world: one the look above did not bring is read now)
  const want = []; for (const ord of picked) for (const ln of ord.lines || []) { const k = O.lineKey(ord, ln); if (!lines[k]) want.push([ord, ln, k]); }
  if (!want.length) { await saveMarker(c, { runId, worldAt, fullAt: full ? now : num(mk.fullAt) || now, fails: 0 }); return out; }
  const missingKeys = want.map(w => w[2]).filter(k => !read.has(k)); if (missingKeys.length) for (const [k, l] of await worldLines(db, missingKeys)) if (!l.gone) read.set(k, l);
  const ready = want.filter(([, , k]) => { const l = read.get(k); return l && l.spec && l.order && l.line; });
  // NOREPULL: an order the Design Station's ledger completed is never taken in again; when the ledger cannot be read, no new order is taken in this time
  const rids = [...new Set(ready.map(([ord]) => String(ord.receiptId)))];
  const done = await completedOf(db, rids);
  if (!done) return Object.assign(out, { wait: "intake", why: "The Design Station's completion record could not be read: new orders wait for the next look" });
  if (done.size) c.say(`[intake] ${done.size} open order(s) not taken in: already completed at the Design Station: ${[...done].slice(0, 12).join(", ")}`);
  let take = ready.filter(([ord]) => !done.has(String(ord.receiptId)));
  if (!take.length) { await saveMarker(c, { runId, worldAt, fullAt: full ? now : num(mk.fullAt) || now, fails: 0 }); return out; }
  // first seen (the arrivals ledger and the order's "arrived" milestone: one call for every order taken in)
  const orders = [...new Map(take.map(([ord]) => [String(ord.receiptId), ord])).values()];
  const arr = await c.call({ op: "arrivalRecord", orders: orders.map(x => ({ id: String(x.receiptId), createTs: num(x.createTs) })) }, `${runId}|intake|arrive|${c.fence.nonce}`);
  if (!arr || arr.status >= 400) throw answerErr("the new orders' first-seen stamps were not written", arr);
  const firstSeen = (arr && arr.firstSeen) || {};
  // Claude's reading each new line takes (RVSAME: the run names the one every computer uses: the latest stored one, as the world read it)
  const crs = await docsOf(db, CUSTOMREAD, take.map(w => w[2])), customRead = {}; for (const [k, x] of crs) if (x && x.latest) customRead[k] = { hash: x.latest };
  const B = { run, maps: { customRead } }, add = {};
  for (const [ord, ln, k] of take) {
    // (a new line is "pulled", as merge makes it, and takes what its reading says of its state: no design, a custom order done by hand, an add-on's wait)
    const wl = read.get(k), sp = clone(wl.spec), row = { key: k, order: wl.order, line: wl.line, spec: sp, problems: clone(wl.problems || []), state: "pulled", reason: null, poolIds: [], hold: null, wait: null, engrave: null, arrivedAt: num(firstSeen[String(ord.receiptId)]) || now, material: sp.material || null };
    readState(row);
    add[k] = PoolMake.lineRecord(row, B)[1];
  }
  // "Pulled" on each order's timeline (TL.pulled: once per order; the event's own id makes a second write the same event)
  const tl = orders.map(x => { const n = (x.lines || []).length; return { orderId: String(x.receiptId), type: "pulled", id: "pull", at: now, by: "Cloud", station: "sorter", text: `${how === "arrival" ? "Came into the sorter with the order check" : "Pulled into the sorter"} · ${n} piece${n === 1 ? "" : "s"}`, data: { how, lines: n, runId } }; });
  let seq = 0;
  for (const part of chunks(tl, TL_EVENTS)) { const r = await c.call({ op: "timelineAdd", events: part }, `${runId}|intake|pulled|${c.fence.nonce}|${seq++}`); if (!r || r.status >= 400) c.say(`[intake] the timeline did not take ${part.length} "Pulled" event(s): ${(r && r.error) || "no answer"}`); }
  // (P13: a cancel that landed after the world was built never joins: read again right before the lines are added)
  const cx = await cancelledOf(db, orders.map(x => String(x.receiptId)));
  if (cx.size) { for (const k of Object.keys(add)) if (cx.has(ridOf(k))) delete add[k]; c.say(`[intake] ${cx.size} order(s) cancelled since the world was built: not taken in`); }
  // runLinesAdd: 300 keys a call at most, an order's lines in one call (an order of more than 300 lines, which no shop order is, in calls of 300)
  const byOrder = new Map(); for (const k of Object.keys(add)) { const r = ridOf(k); if (!byOrder.has(r)) byOrder.set(r, []); byOrder.get(r).push(k); }
  const parts = []; let cur = [];
  for (const ks of byOrder.values()) { if (cur.length && cur.length + ks.length > LINE_KEYS) { parts.push(cur); cur = []; } for (const part of chunks(ks, LINE_KEYS)) { if (cur.length && cur.length + part.length > LINE_KEYS) { parts.push(cur); cur = []; } cur = cur.concat(part); } }
  if (cur.length) parts.push(cur);
  for (const part of parts) {
    const lp = Object.fromEntries(part.map(k => [k, add[k]]));
    const r = await c.call({ op: "runLinesAdd", runId, lines: lp }, `${runId}|intake|add|${c.fence.nonce}|${seq++}`);
    if (!r || r.status >= 400) throw answerErr("new orders could not be added to the run", r);
    const at = num(r.at) || Date.now();
    for (const k of r.added || []) { lines[k] = Object.assign({}, lp[k], { _lp: { at, dev: CLOUD_DEV, by: "Cloud", added: true } }); out.added++; }
    run.lineEditAt = at;
    const os = Array.isArray(run.orders) ? run.orders : (run.orders = []), seen = new Set(os.map(String)); for (const k of r.added || []) { const id = (lp[k] || {}).orderId; if (id != null && !seen.has(String(id))) { os.push(id); seen.add(String(id)); } }
  }
  run.lines = lines; out.orders = orders.length - cx.size;
  if (out.added) c.say(`[intake] ${out.added} new line(s) of ${out.orders} order(s) joined run ${runId} (${how})`);
  await saveMarker(c, { runId, worldAt, fullAt: full ? now : num(mk.fullAt) || now, fails: 0 });
  return out;
}
async function saveMarker(c, patch) { await fencedWrite(c.db, c.fence, [[c.db.collection(MARK[0]).doc(MARK[1]), Object.assign({}, patch, { at: Date.now() }), { merge: true }]]); }

/* ═══ 4 · the gate (C5; bridge processPending) ═══ */
const operatorStop = r => r.status === "stopped" && (r.stopKind === "operator" || r.stoppedBy === "stopped by the operator");
const waitsAtEnd = r => r.status === "running" && (r.step === "revalidate" || r.step === "labels" || r.step === "commit");   // (NOWCMD: the three steps behind the Etsy freshness gate)
const parkedAt = (r, step) => r.status === "stopped" && (r.stopKind === "parked" || /^The cloud parked this run/.test(String(r.stoppedBy || ""))) && r.step === step;
/** Why the run's lines are not made up into pieces now ("" when they are): the page's processPending rule, with the cloud's park at the checkpoint (§1.10). */
function poolGate(run, drive, sheets, o = {}) {
  if ((drive && drive.runMode) !== "auto") return "the run mode is manual: new orders join the run and wait for a person to start them";
  if (operatorStop(run)) return "the run was stopped by the operator: new orders wait for Resume";
  /* NOWCMD (Paul 21 Nov 2026): a run that merely WAITS at revalidate, labels or commit (the Etsy freshness gate can hold it there for a long time) makes new orders up too: their pieces go on drafts only
     (the placement keeps SETSHUT: a set being labelled or committed takes no new order); closed while a sheet of the run is nested or written (below) and after an operator's stop (above) */
  /* FLOWNOW (Paul, 21 Nov 2026: "fix the fact that the new orders are not being nested"; 26 lines waited at "pulled" from 18:04 while the run stood at its nest step behind one nest that kept
     failing): new orders are made up while the run is at its nest step too; that step's own placement puts the new pieces on the drafts with room, and its nest asks their nests */
  const ok = run.status === "processed" || (run.status === "review" && run.step === "engrave") || (o.checkpoint && run.status === "running" && run.step === "checkpoint") || parkedAt(run, "checkpoint") || waitsAtEnd(run) || (run.status === "running" && run.step === "nest");
  if (!ok) return `the run is ${run.status || "?"} at ${run.step || "?"}: new orders are made up at its next checkpoint`;
  // (NOBUSY / FLOWNOW: a sheet being nested or written no longer holds every new order; making pieces touches no sheet, and the placement waits for a busy sheet by itself)
  return "";
}

/* ═══ 5 · lines become pool pieces (bridge Pool.addAll / addRows / attachPool's first part) ═══ */
const retryWait = tries => { const i = Math.max(0, num(tries) - 1); return i < RETRY_STEPS.length ? RETRY_STEPS[i] : PoolMake.RETRY_MS; };
/** The pieces a line holds that are live (not taken off). */
const liveRows = (row, rows) => (row.poolIds || []).map(id => rows.get(String(id))).filter(r => r && !TAKEN_OFF.has(r.state));
/** bridge Pool.settle: a line with pieces goes back to where they are (committed, written into a set, or pooled); a line with none goes back in line. */
function settle(row, rows) {
  const recs = (row.poolIds || []).map(id => rows.get(String(id)) || {});
  row.state = !recs.length ? "pulled" : recs.every(p => p.state === "committed") ? "committed" : recs.every(p => p.sheetId) ? "written" : "pooled";
  row.reason = null; return true;
}
/** The pool-time problems a line was held for (unmatched, blocked, a size its design lacks, too big for the plate), JSON-safe and short, for Review: poolProblems. */
function poolProblemsOf(row) {
  const ps = (row.problems || []).filter(p => p && POOL_KINDS.has(p.kind)).slice(0, 10).map(p => JSON.parse(J(p)) || {});
  return ps.length ? ps : null;
}
/** What changed on a run's line: the patch runLinesPatch writes, or null when nothing did. */
function patchOf(row, rl, B) {
  const rec = PoolMake.lineRecord(row, B)[1];
  const want = { state: rec.state, poolIds: rec.poolIds || [], reason: rec.reason || null, wait: rec.wait || null, groupSize: rec.groupSize || null, sides: rec.sides || null, pieceCount: rec.pieceCount, material: rec.material || null,
    sku: rec.sku || null, quantity: rec.quantity, noDesign: !!rec.noDesign, poolProblems: poolProblemsOf(row), designHold: row.designHold || null };
  // (what a line was held for, and the drawing it was held against, are the piece maker's words: written only for a line it made up now; a line only settled keeps what it had)
  if (!row._tried) { delete want.poolProblems; delete want.designHold; }
  const out = {}; for (const k of PATCH) if (k in want && !same(want[k], rl[k])) out[k] = want[k];
  return Object.keys(out).length ? out : null;
}
/** A run line made into the row the piece maker reads: the world's reading of it (order, line, spec, problems) and the run's own facts. null when the world's reading is missing, gone, or
 *  older than a person's last choice on the line (it is made up after the next world build). */
function rowOf(key, rl, wl, runId) {
  if (!wl || wl.gone || !wl.order || !wl.line || !wl.spec) return null;
  if (runId && wl.runId && wl.runId !== runId) return null;   // (read while another run was open: with that run's choices)
  const facts = wl.run || {};
  for (const f of READ_FACTS) if (!same(facts[f], rl[f])) return null;
  if (facts.crh != null && rl.crh != null && facts.crh !== rl.crh) return null;
  const sp = clone(wl.spec), row = { key, order: wl.order, line: wl.line, spec: sp, problems: clone(wl.problems || []), state: rl.state || "pulled", reason: rl.reason || null, poolIds: (rl.poolIds || []).map(String),
    hold: rl.hold || null, wait: rl.wait || null, engrave: rl.engrave || null, arrivedAt: num(rl.arrivedAt), material: rl.materialOverride || rl.material || sp.material || null };
  for (const f of ["frontAt", "releasing", "releaseAnyway", "releaseHold", "blockOverride", "skuOverride", "materialOverride", "sizeOverride", "engravingOverride", "changePending", "repoolChanged", "activityAt", "groupSize", "etsyClosed", "crh", "_lp", "designHold"]) if (rl[f] !== undefined && rl[f] !== null) row[f] = clone(rl[f]);
  return readState(row);
}
/** The part of the world's afterRead (_charmNestWorld.js, the page's interpretAll) a line's state takes from its reading: no design, a custom order completed by hand, an add-on waiting for its own designs. */
function readState(row) {
  const sp = row.spec || {};
  if (sp.noDesign && row.state === "pulled") row.state = "noDesign";
  if ((sp.customDone || (sp.special && sp.special.notCut)) && !row.hold && !(row.poolIds || []).length && ["held", "unmatched", "waiting", "oversize"].includes(row.state)) { row.state = "noDesign"; row.reason = null; }
  else if (sp.ownDesign && !sp.customDone && !row.hold && !(row.poolIds || []).length && ["held", "unmatched", "waiting", "oversize"].includes(row.state)) { row.state = "held"; row.reason = O.specialOf && O.specialOf.addOn ? O.specialOf.addOn.wait : row.reason; }
  return row;
}
/**
 * pool(c, o) → { made, pooled, held, settled, patched, more?, wait?, why? }
 *   c: the intake's context; o: { step: true (the run's own pool step: no gate), claim: true (the step's unit run for a parked run: the orders made up get the gold dot, as arrivals do) }. Bounded by c.budgetEnd: what is left is taken up by the next unit ({ more }).
 */
async function pool(c, o = {}) {
  const { db, run, runId } = c, now = Date.now(), out = { made: 0, pooled: 0, held: 0, settled: 0, patched: 0 };
  const lines = run.lines || {};
  // (Gate.modern: only a run made by the cloud's runOpen, release policy 2, takes every line; an older run's release days are a computer's)
  if (+run.releasePolicy !== 2) return Object.assign(out, { wait: "intake", why: "This run keeps the release days of before the cloud (its release policy is not 2): a computer carries it" });
  const cand = Object.entries(lines).filter(([k, l]) => l && POOL_STATES.has(l.state) && !(l.state === "held" && l.hold) && /^\w[\w\-]*_[\w\-]+$/.test(k) && (!o.only || o.only.has(k)));   // (o.only, CUSTOMSEND: a person's Send to Sheet makes up its own lines only)
  if (!cand.length) return out;
  // the reads, together: the world's reading of each line, the run's sheets, the lines' pieces, the ledger, the cancels, the sends to sheet, the settings
  const keys = cand.map(([k]) => k), rids = [...new Set(keys.map(ridOf))];
  const [wls, sh, ledger, cancelled] = await Promise.all([worldLines(db, keys), c.sheetsRead || sheetsOfRun(db, runId, !!o.only), docsOf(db, LEDGER, keys), cancelledOf(db, rids), settingsOf(c)]);
  const settings = { settings: c.settings, stockPreset: c.stockPreset };
  const poolIds = [...new Set(cand.flatMap(([, l]) => (l.poolIds || []).map(String)))], rows = await docsOf(db, POOL, poolIds);
  const sends = new Map();
  for (const part of chunks(keys, 1000)) {
    const r = await c.read({ op: "customSheetGet", lineKeys: part });
    if (!r || r.status >= 400) throw answerErr("the sends to sheet could not be read", r);
    for (const e of Object.values(r.records || {})) { const s = e && (e.sent || e.sendIntent); if (!s || !s.lines) continue; for (const [k, list] of Object.entries(s.lines)) if ((list || []).some(pc => !(pc && pc.removed))) sends.set(k, e); }
  }
  // the rows the maker reads
  const specs = new Map(), work = [];
  let stale = 0;
  for (const [k, rl] of cand) {
    const row = rowOf(k, rl, wls.get(k), runId); if (!row) { stale++; continue; }
    const lg = ledger.get(k); if (lg && lg.runId === runId) { if (lg.sig != null) row.poolTry = String(lg.sig); if (num(lg.poolError) > 0) row.poolError = num(lg.poolError); row._tries = num(lg.tries); }
    row._rl = rl; specs.set(k, row.spec); work.push(row);
  }
  if (stale) out.waitWorld = stale;
  if (!work.length) return out;
  const H = Host.host({ db, settings: settings.settings, stockPreset: settings.stockPreset, rows: new Map(rows), listed: sh.listed, sheets: sh.sheets, cancelled, specs, run, sends, budgetMs: Math.max(1000, num(c.budgetEnd) - Date.now()), log: c.say });
  const M = H.make, B = H.B;
  // the library's records the lines name (the design, a pair's or a set's other designs), read together; the library's size only when a line names a SKU it does not hold
  const skus = []; for (const r of work) { const sp = r.spec || {}; if (sp.designSku) skus.push(sp.designSku); const pr = sp.pair || {}; for (const m of Array.isArray(pr.members) ? pr.members : []) if (m && m.sku) skus.push(m.sku); if (pr.set && Array.isArray(pr.set.designs)) skus.push(...pr.set.designs); }
  await H.readSkus(skus);
  if (work.some(r => r.spec && r.spec.designSku && !H.Master.entryFor(r.spec.designSku))) H.entries.count = await Host.libraryCount(db);
  const B2 = { run, maps: {} }, patches = new Map(), ledgerOut = new Map();
  const note = row => { const p = patchOf(row, row._rl, B2); if (p) patches.set(row.key, p); };
  /* ── which lines are made up now (bridge addAll's filter) ── */
  const due = [];
  for (const row of work) {
    const rid = ridOf(row.key), sent = sends.get(row.key);
    if (cancelled.has(rid)) continue;   // (the take-off unit takes a cancelled order's pieces off; no new piece is made for it)
    // a send to sheet (Custom Orders): its pieces are its card's designs, made by its own job. KEPTWINS: unless the card's design was saved to the repository and the line reads as it
    // (CUSTOMSEND, 21 Nov 2026: a send the cloud holds as SENT is made here from the card's own designs, H.CustomSheet = _charmNestCustomMake.js; a press still pending waits, as on the page)
    if (sent && !(row.spec && row.spec.customDone) && !keptFromCard(row, sent, H.Master) && sent.phase !== "sent") continue;
    // bridge recover: a line held for a passing failure before (no person's hold, no problem) is in progress again
    if (row.state === "held" && !row.hold && !(row.problems && row.problems.length) && M.passing(row.reason)) { row.state = "pulled"; row.reason = null; delete row.poolError; delete row.poolTry; }
    const lv = liveRows(row, B.pool.rows);
    // a line whose pieces are all on the run's sheets goes back to where they are (bridge onSheets / settle)
    if ((row.poolIds || []).length && row.poolIds.every(id => sh.listed.has(String(id)))) { settle(row, B.pool.rows); note(row); out.settled++; continue; }
    // a line waiting with its pieces made waits for its sheet (build step 5's placement decides); one waiting for an open layout's green line is not made up again (RGC1)
    if (row.state === "waiting" && ((lv.length && lv.length === row.poolIds.length) || H.greenHeld(row))) continue;
    // a line still "pulled" whose every piece is live in this run already: kept as made (a page made them, or a write after them was cut short); never made again
    if (row.state === "pulled" && row.poolIds.length && lv.length === row.poolIds.length && lv.every(r => !r.runId || r.runId === runId)) { settle(row, B.pool.rows); note(row); out.settled++; continue; }
    let isDue = false;
    try { isDue = row.state === "pulled" && row.poolError > 0 ? now - row.poolError >= retryWait(row._tries) : M.due(row); } catch (e) { isDue = true; }
    if (isDue) due.push(row); else if (row.state !== row._rl.state) note(row);   // (a state the reading moved: no design, an add-on's wait)
  }
  // NOPOOLDONE: before any line is made up, the Design Station's ledger is asked about its order. Completed: never cut again (a line with no piece leaves the run; one with pieces is the
  // take-off unit's). The ledger unread: nothing is made up this time
  const own = r => !!(r.spec && (r.spec.ownDesign || r.spec.special));
  const ask = [...new Set(due.filter(r => !own(r)).map(r => ridOf(r.key)))];
  if (ask.length) {
    const done = await completedOf(db, ask);
    if (!done) { await flushPatches(c, patches, out); return Object.assign(out, { wait: "intake", why: "The Design Station's completion record could not be read: nothing is put on a sheet this time; the next pass asks again" }); }
    if (done.size) {
      for (let i = due.length - 1; i >= 0; i--) { const r = due[i]; if (own(r) || !done.has(ridOf(r.key))) continue; due.splice(i, 1); if (liveRows(r, B.pool.rows).length) continue; r.state = "gone"; r.reason = "completed at the Design Station (never cut again)"; r.wait = null; note(r); }
      c.say(`[intake] ${done.size} order(s) already completed at the Design Station were not put on a sheet: ${[...done].slice(0, 12).join(", ")}`);
    }
  }
  // Gate.plan for a modern run (release policy 2): every line is taken; a line waiting for a release day is in line again
  for (const r of due) if (r.state === "waiting") { r.state = "pulled"; r.wait = null; r.reason = null; }
  // (the gold dot on the orders that join a run under way, as the page's processPending claims its "pulled" rows before it makes them up; the run's own pool step follows its claim step.
  //  A courtesy, never a lock: _charmNestCommit.js claimOrders sends only what the station does not show as this run's, and a failure is only noted)
  if ((!o.step || o.claim) && due.length) await require("./_charmNestCommit").claimOrders(db, runId, due.filter(r => r.state === "pulled").map(r => ridOf(r.key)));
  due.sort(O.byQueue(r => +r.order.createTs || 0));
  /* ── made up, piece by piece, under the unit's time ── */
  const preparePool = async row => {
    const prep = await M.makePool(row, run);
    if (!prep || !prep.pools.some(p => sh.cleared.has(String(p.poolId)))) return prep;
    const pools = prep.pools.filter(p => !sh.cleared.has(String(p.poolId))), keep = new Set(pools.map(p => p.poolId));
    if (!pools.length) { row.state = "noDesign"; row.reason = "its pieces were taken off their sheet on purpose (cleanup)"; return null; }
    return Object.assign({}, prep, { pools, charms: prep.charms.filter(ch => keep.has(ch.poolId)) });
  };
  const PAGE = PoolMake.PAGE;
  const tryLater = (row, e) => {
    if (!M.passing(e)) return false;
    const why = String((e && e.message) != null ? e.message : e);
    row.state = "pulled"; row.reason = `${PAGE.test(why) ? "The cloud did not answer" : why.slice(0, 200)} — trying again`; row.poolError = Date.now(); row._tries = num(row._tries) + 1; delete row.poolTry;
    return true;
  };
  const sigOf = row => { try { return M.trySig(row); } catch (_) { return null; } };
  const hold = (row, e, later) => { if (tryLater(row, e)) return; row.state = "held"; row.reason = String((e && e.message) || e).slice(0, 300); try { M.stampDesignHold(row, null); } catch (_) { delete row.designHold; } if (later) { row.poolError = Date.now(); row.poolTry = sigOf(row); } };
  let made = [], madeRows = 0, more = false, seq = 0;
  const flush = async () => {
    if (!made.length) return;
    const batch = made; made = []; madeRows = 0;
    await putAndAttach(c, batch, { M, B, preparePool, hold, rows: B.pool.rows, out, nextSeq: () => seq++ });
    for (const [row] of batch) { if (row._skip) continue; note(row); ledgerNote(row); }   // (_skip: cancelled while it was made up: nothing of it is written)
    await flushPatches(c, patches, out);
    await flushLedger(c, ledgerOut, ledger);
  };
  const ledgerNote = row => {
    const pooledNow = ["pooled", "written", "committed"].includes(row.state);
    const want = pooledNow ? null : { runId, sig: row.poolTry != null ? String(row.poolTry) : null, poolError: num(row.poolError) || 0, tries: num(row._tries) || 0, state: row.state, reason: row.reason ? String(row.reason).slice(0, 300) : null };
    ledgerOut.set(row.key, want);
  };
  for (const row of due) {
    if (Date.now() > num(c.budgetEnd)) { more = true; break; }
    const keepRow = { state: row.state, reason: row.reason, problems: clone(row.problems), spec: clone(row.spec), poolIds: row.poolIds.slice(), designHold: clone(row.designHold), groupSize: row.groupSize, poolError: row.poolError, poolTry: row.poolTry };
    delete row.poolError; row._tried = true;
    try {
      const prep = await preparePool(row);
      if (prep) { made.push([row, prep]); madeRows += prep.pools.length; out.made++; }
      else { row.poolTry = sigOf(row); note(row); ledgerNote(row); out.held++; }
    } catch (e) {
      if (e && (e.fenced || e.yielded || e.fatal)) throw e;
      if (e && e.budget) { Object.assign(row, keepRow); delete row._tried; more = true; break; }   // (the unit's time is spent: the line is made again, whole, by the next unit; nothing of it was written)
      hold(row, e, true); note(row); ledgerNote(row); out.held++;
    }
    if (madeRows >= PUT_ROWS) await flush();
  }
  await flush();
  await flushPatches(c, patches, out);
  await flushLedger(c, ledgerOut, ledger);
  if (more) out.more = true;
  if (out.made || out.held || out.patched) c.say(`[intake] run ${runId}: ${out.made} line(s) made up, ${out.pooled} pooled, ${out.held} held, ${out.settled} kept as they are${more ? "; the rest in the next unit" : ""}`);
  return out;
}
/** KEPTWINS (bridge makePoolRead keptFromCard): a line with a send to sheet whose card's own design was saved to the charm repository, and that now reads as that repository charm, is made
 *  from the repository like every other line of its listing. */
function keptFromCard(row, e, M) {
  try {
    const sp = row.spec; if (!sp || !sp.designSku || sp.noDesign || sp.ownDesign || (sp.problems || []).length) return false;
    const en = M.entryFor(sp.designSku); if (!en || !e || !/^charmnest\/master\/custom\//.test(String(en.masterPath || en.aiPath || ""))) return false;
    const nm = String(en.masterName || "").trim().toLowerCase(); return !!nm && (e.files || []).some(F => String((F && F.name) || "").trim().toLowerCase() === nm);
  } catch (_) { return false; }
}
/** poolPut for the lines made (200 rows a call at most, a line never split; a line an older run made as one glued piece is made again glued, as poolAdd does), then each line's answer
 *  read as attachPool reads it (charm-nest-pool-make.js poolAnswer): kept short, gone, held by another run, or pooled (its rows "ready", no sheet: build step 5 places them). */
async function putAndAttach(c, made, k) {
  const { M, B, preparePool, hold, rows, out } = k, runId = c.runId;
  // (P13: a cancel that landed while the lines were made up: none of its pieces is written)
  const cx = await cancelledOf(c.db, [...new Set(made.map(([r]) => ridOf(r.key)))]);
  const keep = made.filter(([r]) => !cx.has(ridOf(r.key)));
  const parts = []; let cur = []; for (const [, prep] of keep) { if (cur.length && cur.length + prep.pools.length > PUT_ROWS) { parts.push(cur); cur = []; } cur = cur.concat(prep.pools); } if (cur.length) parts.push(cur);
  let contended = [], placed = [], short = [], failure = null; const legacy = new Set();
  const put = async part => {
    const r = await c.call({ op: "poolPut", pools: part }, `${runId}|intake|put|${c.fence.nonce}|${k.nextSeq()}`);
    if (!r || r.status >= 400) throw Object.assign(new Error((r && r.error) || "the pool rows were not written"), { status: r ? r.status : 0, transient: !r || r.status >= 500 });
    contended = contended.concat(r.contended || []); placed = placed.concat(r.placed || []); short = short.concat(r.short || []); for (const id of r.legacy || []) legacy.add(String(id));
  };
  try {
    for (const part of parts) await put(part);
    if (legacy.size) {
      const redo = [];
      for (const item of keep) if (item[1].pools.some(p => legacy.has(String(p.poolId)))) { item[0].glued = true; const again = await preparePool(item[0]); if (again) { item[1] = again; redo.push(...again.pools); } }
      for (const part of chunks(redo, PUT_ROWS)) await put(part);
    }
  } catch (e) { if (e && (e.fenced || e.yielded || e.fatal)) throw e; failure = e; }
  for (const [row, prep] of keep) {
    if (failure) { hold(row, failure); out.held++; continue; }
    const ans = M.poolAnswer(row, prep, contended, placed, short);
    if (ans.kind === "short") { settle(row, rows); continue; }
    if (ans.kind === "gone") { if (ans.ids.length) await c.call({ op: "poolUpdate", poolIds: ans.ids, patch: { state: "abandoned", sheetId: null, setId: null } }, `${runId}|intake|gone|${c.fence.nonce}|${k.nextSeq()}`); continue; }
    if (ans.kind === "contended") continue;
    const { saved, on } = ans;
    for (const p of prep.pools) if (!on.has(p.poolId) || !rows.has(p.poolId)) rows.set(p.poolId, saved.has(p.poolId) ? Object.assign({}, p, saved.get(p.poolId)) : p);
    row.poolIds = prep.pools.map(p => p.poolId); row.state = prep.pools.length ? "pooled" : "pulled"; row.material = prep.sp.material; row.reason = null; delete row.poolTry; delete row.poolError; row._tries = 0;
    if (on.size && on.size === prep.pools.length) settle(row, rows);
    if (row.state === "pooled" || row.state === "written") out.pooled++;
  }
  for (const [row] of made) if (cx.has(ridOf(row.key))) row._skip = true;
}
async function flushPatches(c, patches, out) {
  const keys = [...patches.keys()]; if (!keys.length) return;
  let seq = 0;
  for (const part of chunks(keys, LINE_KEYS)) {
    const body = Object.fromEntries(part.map(k => [k, patches.get(k)]));
    const r = await c.call({ op: "runLinesPatch", runId: c.runId, lines: body }, `${c.runId}|intake|patch|${c.fence.nonce}|${part[0]}|${seq++}`);
    if (!r || r.status >= 400) throw answerErr("the run's lines could not be written", r);
    const at = num(r.at) || Date.now(), lines = c.run.lines || (c.run.lines = {});
    for (const k of r.applied || []) { lines[k] = Object.assign({}, lines[k] || {}, body[k], { _lp: { at, dev: CLOUD_DEV, by: "Cloud" } }); out.patched++; }
    c.run.lineEditAt = at;
    for (const k of part) patches.delete(k);
  }
}
async function flushLedger(c, want, had) {
  const writes = [];
  for (const [k, d] of want) {
    const was = had.get(k), wasOurs = was && was.runId === c.runId;
    if (d === null) { if (was) writes.push([c.db.collection(LEDGER).doc(k), null]); continue; }
    if (wasOurs && same([was.sig, num(was.poolError), num(was.tries), was.state, was.reason || null], [d.sig, d.poolError, d.tries, d.state, d.reason])) continue;
    writes.push([c.db.collection(LEDGER).doc(k), Object.assign({}, d, { lineKey: k, at: Date.now(), expireAt: new Date(Date.now() + 30 * 86400000) })]);
  }
  want.clear();
  if (!writes.length) return 0;
  await fencedWrite(c.db, c.fence, writes);
  for (const [ref, d] of writes) { if (d === null) had.delete(ref.id); else had.set(ref.id, d); }
  return writes.length;
}

/* ═══ 6 · the units the driver runs ═══ */
/** The intake's context from a unit's (the driver's ctx: db, run, runId, call, read, drive, cloudSteps, budgetEnd, fence, say). */
function ictx(ctx) {
  if (!ctx || !ctx.fence || !ctx.fence.nonce) throw fencedErr("the intake runs only inside a pass of the cloud driver (no fence)");
  return { db: ctx.db, run: ctx.run, runId: ctx.runId || ctx.run.runId, call: ctx.call, read: ctx.read, drive: ctx.drive || {}, cloudSteps: ctx.cloudSteps || [], fence: ctx.fence,
    say: typeof ctx.say === "function" ? ctx.say : t => console.log(t), budgetEnd: num(ctx.budgetEnd) || Date.now() + 45000, settings: null, stockPreset: null };
}
/** The shop's shared settings and plate preset, read once a unit, when first needed (_charmNestPieceHost.js readSettings). */
async function settingsOf(c) { if (!c.settings) { const s = await Host.readSettings(c.db); c.settings = s.settings; c.stockPreset = s.stockPreset; } return c.settings; }
const pooledCount = run => Object.values(run.lines || {}).filter(l => l && ["pooled", "written", "committed"].includes(l.state) && (l.poolIds || []).length).length;
/** Step "pull" (bridge doStep pull → Orders.pull): the open orders the pull rule picks join the run. None: nothing to cut. */
async function pullUnit(ctx) {
  const c = ictx(ctx);
  const r = await join(c, { how: "pull" });
  if (r.wait) return { wait: r.wait, why: r.why };
  if (!Object.keys(c.run.lines || {}).length) { c.run.nothingToCut = true; c.say(`[intake] run ${c.runId}: no open order to pull (the pull rule picked ${r.picked})`); return { goto: "complete" }; }
  return undefined;
}
/** Step "pool" (bridge doStep pool → Pool.addAll): every line that is due is made up into its pieces. None pooled in the run: nothing to cut. */
async function poolUnit(ctx) {
  const c = ictx(ctx);
  const r = await pool(c, { step: true, claim: !!ctx.parked });   // (parked at the checkpoint, the pool step's unit takes in orders that joined the run under way: they get the gold dot too)
  if (r.more) return { more: true };
  if (r.wait) return { wait: r.wait, why: r.why };
  if (!pooledCount(c.run) && r.waitWorld) return { wait: "world", why: `${r.waitWorld} line(s) wait for the cloud's next reading of them (the world is built every 2 minutes)` };
  if (!pooledCount(c.run)) { c.run.nothingToCut = true; c.say(`[intake] run ${c.runId}: no eligible pieces to nest; unresolved items remain available for follow-up`); return { goto: "complete" }; }
  return undefined;
}
/** Step "checkpoint", first (bridge doStep checkpoint → Arrivals.processPending({ checkpoint: true })): new orders join and, behind the gate, are made up before the checkpoint's own work. */
async function checkpointUnit(ctx) {
  const c = ictx(ctx);
  if (c.cloudSteps.includes("pull")) await join(c, { how: "arrival" });
  if (!c.cloudSteps.includes("pool") || poolGate(c.run, c.drive, null, { checkpoint: true })) return undefined;
  c.sheetsRead = await sheetsOfRun(c.db, c.runId);
  if (poolGate(c.run, c.drive, c.sheetsRead.sheets, { checkpoint: true })) return undefined;
  const r = await pool(c, {});
  return r.more ? { more: true } : undefined;
}
/** The units of the run's steps this build step does: { pull, pool, checkpoint } (the checkpoint's other work is build step 10a's, after this unit). */
function steps() { return { pull: [pullUnit], pool: [poolUnit], checkpoint: [checkpointUnit] }; }

/**
 * The pass's intake (§1.6 item 5), before the run's own step: new orders join the run whatever it is doing (as Arrivals.merge joins them today); behind the gate they are made up into pieces.
 * env: { db, run, runId, drive, cloudSteps, call, read, fence, say, budgetEnd }. → null (nothing for the intake to do) | { summary, more?, stop? }
 * A failure that is not the fence's is said and tried again at the next pass; six in a row stop the run in the page's words ("New orders could not be added: ...").
 */
async function passIntake(env) {
  const cs = env.cloudSteps || [], run = env.run;
  if (!cs.includes("pull") && !cs.includes("pool")) return null;
  if (!run || ["complete", "abandoned", "archived"].includes(String(run.status || ""))) return null;
  if (run.status === "running" && (run.step === "pull" || run.step === "claim" || run.step === "pool" || run.step === "checkpoint")) return null;   // (the run's own steps take them: pull, pool, the checkpoint)
  const c = ictx(env), summary = {};
  try {
    if (cs.includes("pull")) { const j = await join(c, { how: "arrival" }); summary.joined = j.added; if (j.wait) summary.joinWaits = j.why; }
    if (cs.includes("pool")) {
      // (the gate's own rules first, which read nothing; the run's sheets only when they decide)
      let why = poolGate(run, c.drive, null); if (!why) { c.sheetsRead = await sheetsOfRun(c.db, c.runId); why = poolGate(run, c.drive, c.sheetsRead.sheets); }
      if (why) summary.poolWaits = why;
      else {
        const p = await pool(c, {}); Object.assign(summary, { made: p.made, pooled: p.pooled, held: p.held, settled: p.settled, patched: p.patched }); if (p.wait) summary.poolWaits = p.why; if (p.more) summary.more = true;
        /* NOWCMD: at revalidate / labels / commit no nest step follows, so the new pieces are placed now (the placement unit's own rules: drafts only, SETSHUT) and each sheet that took one is asked to
           nest (the pass's NESTNOW pump starts it) */
        /* FLOWNOW: pieces made earlier and still on no sheet (a line "pooled") are placed too, at every pass the run waits at revalidate / labels / commit, not only in the pass that made them */
        const waitingPieces = Object.values(run.lines || {}).some(l => l && l.state === "pooled" && Array.isArray(l.poolIds) && l.poolIds.length);
        /* PLACENOW (Paul, 23 Nov 2026, orders 4235923653, 4230787700 and 4238855149: made in the right metal and on no sheet; 11 pieces of the day stood so): the nest step places new pieces once, before its nests, and
           then waits for them all: while SS Draft 11 was nested one order at a time, half an hour, no new piece of any metal went on a sheet. A run at its nest step places them at every pass too, by the same unit and
           rules (a sheet being nested still waits by itself, NESTBUSY); only when a made piece really stands on no sheet. */
        const atNest = run.status === "running" && run.step === "nest" && cs.includes("nest") && waitingPieces;
        if (atNest && !c.sheetsRead) c.sheetsRead = await sheetsOfRun(c.db, c.runId);
        const stray = atNest && (p.pooled || p.made || Object.values(run.lines || {}).some(l => l && l.state === "pooled" && (l.poolIds || []).some(id => !c.sheetsRead.listed.has(String(id)))));
        if ((waitsAtEnd(run) || stray) && cs.includes("nest") && (p.pooled || p.made || waitingPieces)) {
          const got = await c.read({ op: "runGet", runId: c.runId }).catch(() => null), fr = got && got.run;
          if (fr && fr.lines) {
            const pl = await require("./_charmNestPlaceUnit").placeAll(Object.assign({}, env, { run: Object.assign({}, fr, { runId: c.runId }) }));
            summary.placed = (pl && pl.placed) || 0;
            const sh = await sheetsOfRun(c.db, c.runId), asked = [];
            for (const s of (sh.sheets ? sh.sheets.values() : [])) if (s && s.nestOwed && !s.archived && !busySheet(s) && !s.setId) { await c.call({ op: "nestStart", runId: c.runId, sheetId: s.id, fresh: false, why: "New orders placed while the run waits at " + run.step }, `intake|nest|${s.id}|${num(s.contentRev)}`); asked.push(s.id); }
            if (asked.length) summary.nestAsked = asked;
          }
        }
      }
    }
    return { summary, more: !!summary.more };
  } catch (e) {
    if (e && (e.fenced || e.yielded || e.fatal)) throw e;
    const why = String((e && e.message) || e).slice(0, 240);
    c.say(`[intake] new orders wait: ${why}`);
    let fails = 1; try { const mk = await marker(c.db); fails = num(mk.fails) + 1; await saveMarker(c, { fails }); } catch (e2) { if (e2 && e2.fenced) throw e2; }
    if (fails >= SOFT_FAILS) { try { await saveMarker(c, { fails: 0 }); } catch (e3) { if (e3 && e3.fenced) throw e3; } return { summary: { error: why }, stop: { why: `New orders could not be added: ${why}`, fix: "Resume after fixing the cause. Your earlier work is kept." } }; }
    return { summary: { error: why, tries: fails } };
  }
}

module.exports = { steps, passIntake, pullUnit, poolUnit, checkpointUnit, join, pool, poolGate, pullPick, endOfDay, rowOf, readState, patchOf, settle, fencedWrite, lineOf, busySheet, READ_FACTS, PATCH, LEDGER, TZ };
