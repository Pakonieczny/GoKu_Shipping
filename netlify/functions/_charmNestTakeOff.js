/*  netlify/functions/_charmNestTakeOff.js — THE TAKE-OFF UNIT and the REVALIDATE STEP of the cloud driver (cloud plan v2 §1.6 item 2, §1.7 revalidate, build step 9:
 *  E7, P9 lane "takeoff", P10, P11, C2 (a), m:C14). DORMANT: the driver runs the lane only while it carries a run and "takeoff" is in Charm_Nest_Open/drive.cloudSteps, and
 *  the revalidate step only while "revalidate" is in it (otherwise it parks the run at that step with words, as every step it does not do yet). With the drive absent or "off"
 *  the driver does nothing at all, so nothing here runs.
 *
 *  THE LANE (the node port of the page's AutoCancel.poll / job, charm-nest-sheetwin.js, and of Orders.takeOffGone, charm-nest-bridge.js), once a pass, before intake:
 *    detect   · cancel records written since the lane's cursor (cancelList after state.takeoff.cancelCursor: no Etsy call; the first read takes the newest 5000 ids);
 *             · orders Etsy closed that a SHIPPING STATION marked shipped (OURSHIPONLY: the run's etsyClosed lines, shippedByUs), at most every 10 minutes;
 *             · orders the Design Station's ledger completed (never cut again; a custom piece made here is never judged by it), at most every 10 minutes;
 *             · a person's presses: takeOffAsk { hold | sheetOff | retry } (Charm_Nest_TakeOff/<order>.asks).
 *    journal  Charm_Nest_TakeOff/<order>: one job per take-off (its key: kind and time), written BEFORE anything moves, then after each step; a job left half done by a
 *             pass that ended is finished by the next one (every step can run again and writes nothing the second time).
 *    a job    1 · a cancel is read again (cancelCheck): an order not cancelled any more is "restored" and nothing moves (its set-aside notice goes);
 *             2 · where every piece of the order is (getOrderPieces: its pool rows and every live sheet that lists them, one snapshot);
 *             3 · cloudTakeOff, ONE transaction per sheet (rows, sheet fields, charms and placements, green lines, filesOwed, layoutRev and contentRev; P10): a sheet
 *                 marked laser cut keeps its pieces (set aside), one that is busy waits for the next pass;
 *                 the pieces on no sheet: their rows only;
 *             4 · the run's lines (runLinesPatch): gone (cancelled, shipped, completed), held (Hold, sheet Take off);
 *             5 · the records: cancelFates (sheet by sheet), the order's timeline (its pieces' own events, written in the take-off), the set-aside notices in the shape every
 *                 page reads (Charm_Nest_Shared autocancel-production: n:<order>:<at>, d:<order>), and the freed sheets for the placement pass (state.takeoff.freed).
 *    Filling the freed room is the placement pass's (build step 5: placeOnSheet then nestStart), never the page's moveIn; the sheet's files are the files lane's (filesOwed).
 *
 *  REVALIDATE (bridge Orders.revalidate, read from the world, never from Etsy): behind the freshness gate (§1.8 a: the mail system's copy of Etsy's receipts synced well after the
 *  step started, the world was built from that copy, none of the run's receipts waits to be read, Etsy's daily limit is not set and Etsy's switch reads "not stopped"; a failing
 *  condition waits with words and clears by itself). Then each run line is compared with the world:
 *    · its order is not open any more: cancelled (its cancel record) → a take-off job; closed but not cancelled → etsyClosed only (OURSHIPONLY), unless a person kept it (keepShipped);
 *    · its order changed after it was read (the world build's change marks, Charm_World/changed) → changePending (and repoolChanged when what it is made of changed): its
 *      pieces change only after a person accepts the change (remakeline, _charmNestCmdExec.js again);
 *    · an Etsy-closed order a shipping station marked shipped → a take-off job.
 *  No Etsy call, no world build, no browser.                                                                                                                                  */
"use strict";
const O = require("../../charm-nest-orders.js");

const COLL = "Charm_Nest_TakeOff", NOTICES = "autocancel-production", LEDGER = "Design_Completed Orders";
const LOOK_MS = 10 * 60000, MAX_TRIES = 8, WAIT_WORDS_MS = 30 * 60000;
const AUTO = new Set(["cancel", "shipped", "completed"]);
const OPEN_JOB = new Set(["due", "working", "waiting"]);
const num = v => (Number.isFinite(+v) ? +v : 0);
const str = (v, n = 400) => (v == null ? "" : String(v)).slice(0, n);
const ridOf = k => { const s = String(k || ""), i = s.indexOf("_"); return i > 0 ? s.slice(0, i) : s; };
const joinAnd = l => (l.length < 2 ? l.join("") : l.slice(0, -1).join(", ") + " and " + l[l.length - 1]);
const cap = t => (t ? t.charAt(0).toUpperCase() + t.slice(1) : t);
const FINAL_LINE = new Set(["gone", "committed", "skipped"]);
const liveLine = l => !!l && !FINAL_LINE.has(String(l.state || ""));

/* ── words (the page's own: AutoCancel noticeText, the sheet window's fates) ── */
/** The set-aside notice of a cancelled order whose pieces stay on sheets (left: [{ where, cut?, open?, only?, why }]). */
function noticeText(rid, left) {
  const cut = left.filter(l => l.cut), open = left.filter(l => l.open), rest = left.filter(l => !l.cut && !l.open), say = [];
  const openTxt = open.length ? `take its pieces off ${joinAnd(open.map(l => l.where))} before cutting.` : "";
  if (open.length && !cut.length && !rest.length) return `Order ${rid} is cancelled: ${openTxt}`;
  if (cut.length) say.push(`Its pieces are already cut on ${joinAnd(cut.map(l => l.where))}: set them aside.`);
  for (const l of rest) say.push(l.only ? `It is the only order on ${l.where}: delete that sheet from its menu, or set its pieces aside once cut.` : `Its pieces on ${l.where} stay (${l.why}): set them aside once cut.`);
  if (openTxt) say.push(cap(openTxt));
  return `Order ${rid} was cancelled. ${say.join(" ")}`;
}
/** What became of the order on each sheet, for its cancel record (Orders › Cancelled reads it). */
function fatesOf(taken, left) {
  const out = new Map();
  for (const t of taken) out.set(t.where, { sheet: t.where, fate: "removed", text: `taken off ${t.where}` });
  for (const l of left) if (!out.has(l.where)) out.set(l.where, { sheet: l.where, fate: l.open ? "open" : "cut", text: l.cut ? `already cut on ${l.where}: set aside` : l.open ? `on ${l.where}, not cut yet: take its pieces off before cutting` : `stays on ${l.where} (${l.why}): set aside once cut` });
  return [...out.values()].slice(0, 30);
}
const reasonOf = job => (job.kind === "cancel" ? (job.etsy ? "cancelled on Etsy" : `cancelled by ${job.by || "someone"}`) : job.kind === "shipped" ? "shipped" : job.kind === "completed" ? "completed at the Design Station (never cut again)" : job.reason || "");

/* ── the lane ── */
/** One look at the take-offs (§1.6 item 2). env: { db, run, runId, call, read, state, saveState, say, budgetEnd, live? }. → { jobs, done, waiting, held, more, detected } */
async function lane(env) {
  const { db, read } = env, say = env.say || (() => {}), st = Object.assign({}, (env.state && env.state.takeoff) || {}), out = { jobs: 0, done: 0, waiting: 0, held: 0, detected: 0 };
  const left = () => Date.now() < num(env.budgetEnd);
  const lines = (env.run && env.run.lines) || {};
  const held = new Map(); for (const [k, l] of Object.entries(lines)) if (liveLine(l) || (l && Array.isArray(l.poolIds) && l.poolIds.length && l.state !== "committed")) { const r = ridOf(k); if (!held.has(r)) held.set(r, []); held.get(r).push(k); }
  const save = {};
  try {
    /* 1 · detect: cancels since the cursor */
    const found = new Map();   // rid → [{ key, create }]
    const want = (rid, key, create) => { if (!found.has(rid)) found.set(rid, []); found.get(rid).push({ key, create }); };
    const recs = await newCancels(read, st, save);
    if (recs.ids.length) {
      const mine = recs.ids.filter(id => held.has(id)), far = recs.first ? [] : recs.ids.filter(id => !held.has(id)).slice(0, 80);
      // (an order the run does not hold, cancelled since the last read: looked at only when some piece of it is still live somewhere: an uncut sheet of an earlier run, or the pool)
      const farLive = far.length ? await liveSomewhere(read, far) : new Set();
      const ask = mine.concat([...farLive]);
      if (ask.length) { const c = await cancelsOf(read, ask); for (const [rid, rec] of c) want(rid, `cancel-${Math.round(num(rec.at))}`, { kind: "cancel", at: Math.round(num(rec.at)), by: rec.source === "etsy" ? "Etsy" : str(rec.by, 80) || "someone", etsy: rec.source === "etsy" || rec.by === "Etsy", why: str(rec.why, 200) }); }
    }
    /* 2 · detect (at most every 10 minutes): shipped at our station (OURSHIPONLY), completed at the Design Station; and set-aside notices of orders restored since */
    if (Date.now() - num(st.lookAt) > LOOK_MS) {
      save.lookAt = Date.now();
      const closed = [...new Set(Object.entries(lines).filter(([, l]) => l && l.etsyClosed && liveLine(l)).map(([k]) => ridOf(k)))].slice(0, 300);
      for (let i = 0; i < closed.length; i += 100) { const g = await read({ op: "shippedByUs", orders: closed.slice(i, i + 100) }); for (const [rid, ev] of Object.entries((g && g.shipped) || {})) want(rid, `shipped-${Math.round(num(ev.at))}`, { kind: "shipped", at: Date.now(), shippedAt: num(ev.at) || Date.now(), by: str(ev.by, 80) || "Shipping" }); }
      for (const [rid, keys] of await completedOf(db, held, lines)) want(rid, "completed", { kind: "completed", at: Date.now(), by: "System", lineKeys: keys });
      await forgetRestored(env, st, save);
    }
    /* 3 · the journals: the jobs found (made before anything moves) and every journal with work open (a person's press, a job that waits) */
    const due = await openJournals(db);
    const rids = [...new Set([...found.keys(), ...due.keys()])];
    const docs = new Map(due); const missing = rids.filter(r => !docs.has(r));
    for (let i = 0; i < missing.length; i += 100) { const got = await db.getAll(...missing.slice(i, i + 100).map(r => db.collection(COLL).doc(r))); got.forEach(x => docs.set(x.id, x.exists ? x.data() || {} : {})); }
    for (const [rid, list] of found) {
      const jd = docs.get(rid) || {};
      for (const { key, create } of list) {
        if (jd.jobs && jd.jobs[key]) continue;   // (a take-off already journalled is never made twice)
        if (!left()) { out.more = true; break; }
        const job = Object.assign({ status: "due", tries: 0, runId: env.runId }, create);
        const r = await env.call({ op: "takeOffJournal", orderId: rid, job: key, create: job, set: { status: "due" }, runId: env.runId }, `takeoff|journal|${rid}|${key}`);
        if (r && r.ok) { jd.jobs = Object.assign({}, jd.jobs, { [key]: job }); docs.set(rid, jd); out.detected++; say(`[takeoff] ${rid}: ${create.kind} found`); }
      }
    }
    /* 4 · the work, oldest first, each order whole before the next */
    const order = [...docs.entries()].filter(([, jd]) => hasWork(jd)).sort((a, b) => num(a[1].updatedAt) - num(b[1].updatedAt));
    for (const [rid, jd] of order) {
      if (!left()) { out.more = true; break; }
      if (env.live) env.live();
      const r = await workOrder(env, rid, jd, save);
      out.jobs += r.jobs; out.done += r.done; out.waiting += r.waiting; out.held += r.held; if (r.more) out.more = true;
    }
  } catch (e) {
    if (e && (e.fenced || e.yielded || e.fatal)) throw e;
    say("[takeoff] " + ((e && e.message) || e)); out.error = String((e && e.message) || e).slice(0, 300);
  }
  if (Object.keys(save).length && env.saveState) await env.saveState({ takeoff: save });
  return out;
}
const hasWork = jd => !!jd && (Object.values(jd.jobs || {}).some(j => j && OPEN_JOB.has(j.status)) || Object.values(jd.asks || {}).some(a => a && a.state === "waiting"));

/** The cancel records written since the lane's cursor (the first read: the newest 5000 ids and the cursor at the newest). save gets the cursor and the counter. */
async function newCancels(read, st, save) {
  if (!st.cancelCursor) {
    const r = await read({ op: "cancelList", idsOnly: true, track: true, wantGen: true });
    if (!r || r.error) throw new Error("the cancelled orders could not be read: " + ((r && r.error) || "no answer"));
    save.cancelCursor = r.cursor || null; if (r.gen != null) save.cancelGen = r.gen;
    return { ids: (r.ids || []).map(String), first: true };
  }
  let cur = st.cancelCursor; const ids = [];
  for (let i = 0; i < 5; i++) {
    const r = await read({ op: "cancelList", after: cur, idsOnly: true, limit: 200, ...(i === 0 && st.cancelGen != null ? { ifGen: st.cancelGen } : { wantGen: true }) });
    if (!r || r.error) throw new Error("the cancelled orders could not be read: " + ((r && r.error) || "no answer"));
    if (r.gen != null) save.cancelGen = r.gen;
    if (r.unchanged) break;
    ids.push(...(r.ids || []).map(String)); if (r.cursor) cur = r.cursor;
    if (!r.more) break;
  }
  if (JSON.stringify(cur) !== JSON.stringify(st.cancelCursor)) save.cancelCursor = cur;
  return { ids: [...new Set(ids)], first: false };
}
/** The cancel records of these orders as they are now: Map rid → record (cancelCheck, 60 a read). */
async function cancelsOf(read, ids) {
  const out = new Map();
  for (let i = 0; i < ids.length; i += 60) { const r = await read({ op: "cancelCheck", orderIds: ids.slice(i, i + 60) }); for (const [rid, rec] of Object.entries((r && r.cancelled) || {})) out.set(String(rid), rec); }
  return out;
}
/** The orders of `ids` with a piece still live somewhere (on a live sheet not cut, or waiting in the pool). */
async function liveSomewhere(read, ids) {
  const out = new Set();
  for (let i = 0; i < ids.length; i += 40) {
    const a = await read({ op: "getOrderPieces", orderIds: ids.slice(i, i + 40) });
    for (const [rid, o] of Object.entries((a && a.orders) || {})) if (Object.values((o && o.placement) || {}).some(p => p && ((p.state === "sheet" && !p.cut) || p.state === "waiting"))) out.add(String(rid));
  }
  return out;
}
/** The orders the Design Station's ledger completed, of those the run holds with a line not committed: Map rid → the line keys it covers (custom pieces made here
    are never judged by it: their world line says ownDesign or a special). Read only (the ledger's own documents; its index is never built from here). */
async function completedOf(db, held, lines) {
  const ids = [...held.keys()].filter(r => /^\d{4,20}$/.test(r)), done = [];
  for (let i = 0; i < ids.length; i += 300) { const got = await db.getAll(...ids.slice(i, i + 300).map(r => db.collection(LEDGER).doc(r))); got.forEach(x => { if (x.exists) done.push(x.id); }); }
  const out = new Map();
  if (!done.length) return out;
  const keys = done.flatMap(r => (held.get(r) || []).filter(k => lines[k] && lines[k].state !== "committed"));
  const own = new Set();
  for (let i = 0; i < keys.length; i += 300) { const got = await db.getAll(...keys.slice(i, i + 300).map(k => db.collection("Charm_World_Lines").doc(k))); got.forEach(x => { let sp = null; try { sp = x.exists ? (JSON.parse((x.data() || {}).data || "{}").spec || null) : null; } catch (_) { sp = null; } if (sp && (sp.ownDesign || sp.special)) own.add(x.id); }); }
  for (const r of done) { const ks = (held.get(r) || []).filter(k => lines[k] && lines[k].state !== "committed" && !own.has(k)); if (ks.length) out.set(r, ks); }
  return out;
}
/** The journals with work open (a press waiting, a job not finished): Map rid → journal. */
async function openJournals(db) {
  const out = new Map();
  try { const q = await db.collection(COLL).where("due", "==", true).limit(50).get(); for (const x of q.docs) out.set(x.id, x.data() || {}); } catch (_) { /* none read: the next pass looks again */ }
  return out;
}
/** A set-aside notice whose order is not cancelled any more goes (the page's poll asked the same: no piece of a live order is set aside). */
async function forgetRestored(env, st, save) {
  const list = Array.isArray(st.notices) ? st.notices.filter(n => n && n.rid) : [];
  if (!list.length) return;
  const c = await cancelsOf(env.read, [...new Set(list.map(n => n.rid))].slice(0, 60)), keep = [];
  for (const n of list) {
    if (c.has(n.rid)) { keep.push(n); continue; }
    await env.call({ op: "takeOffAck", orderId: n.rid, at: n.at, forget: true }, `takeoff|forget|${n.rid}|${n.at}`);
  }
  if (keep.length !== list.length) save.notices = keep.slice(-100);
}

/* ── one order ── */
async function workOrder(env, rid, jd, save) {
  const out = { jobs: 0, done: 0, waiting: 0, held: 0 }, now = Date.now();
  // a person's presses become jobs (made before anything moves)
  for (const [aid, a] of Object.entries(jd.asks || {})) {
    if (!a || a.state !== "waiting") continue;
    if (a.kind === "retry") {
      /* RETRYLEFT (Paul, 26 Nov 2026, order 4242434945: cancelled on Etsy 23 Nov, its pair left on RG Sheet 1 as "already cut" because that sheet's green line was recorded, though the laser had
         never cut it; "your previous solution now be effective on all remaining orders so I don't see this problem again"). Try again also takes up a cancel that ENDED with pieces left on a sheet:
         it is worked again under today's rule (LASERONLY: only the laser's mark keeps a piece), so what a sheet no longer has to keep comes off, and what a laser-cut sheet keeps stays, said as before.
         Its fates are written again (the cancel record and the timeline say where each piece is now). */
      for (const [k, j] of Object.entries(jd.jobs || {})) {
        const leftOn = !!j && j.kind === "cancel" && j.status === "done" && Array.isArray(j.left) && j.left.length > 0;
        if (j && (j.status === "held" || j.status === "stuck" || OPEN_JOB.has(j.status) || leftOn)) { await env.call({ op: "takeOffJournal", orderId: rid, job: k, set: Object.assign({ status: "due", tries: 0, why: null }, leftOn ? { fatesSaved: false, again: now } : {}) }, `takeoff|retry|${rid}|${k}|${aid}`); j.status = "due"; j.tries = 0; if (leftOn) { j.fatesSaved = false; j.again = now; } }
      }
      await env.call({ op: "takeOffJournal", orderId: rid, ask: { id: aid, state: "taken" }, set: { due: true } }, `takeoff|ask|${rid}|${aid}`);
      continue;
    }
    // (S5: only, the pieces the person chose, and scope, sheet or order, go with the press into its job: scopeOf takes off those pieces only)
    const key = `${a.kind}-${num(a.at)}`, job = { kind: a.kind, at: num(a.at) || now, by: str(a.by, 80) || "someone", note: str(a.note, 200), sheetId: a.sheetId || null, lineKeys: Array.isArray(a.lineKeys) ? a.lineKeys.slice(0, 50) : [], ...(Array.isArray(a.only) && a.only.length ? { only: a.only.map(String).slice(0, 400) } : {}), ...(a.scope === "sheet" || a.scope === "order" ? { scope: a.scope } : {}), ask: aid, status: "due", tries: 0, runId: env.runId };
    await env.call({ op: "takeOffJournal", orderId: rid, job: key, create: job, ask: { id: aid, state: "taken", job: key }, set: { status: "due" } }, `takeoff|ask|${rid}|${aid}`);
    jd.jobs = Object.assign({}, jd.jobs, { [key]: (jd.jobs || {})[key] || job });
  }
  const jobs = Object.entries(jd.jobs || {}).filter(([, j]) => j && OPEN_JOB.has(j.status)).sort((a, b) => num(a[1].at) - num(b[1].at));
  for (const [key, job] of jobs) {
    if (Date.now() >= num(env.budgetEnd)) { out.more = true; break; }
    out.jobs++;
    let r;
    try { r = await workJob(env, rid, key, job, save); }
    catch (e) {
      if (e && (e.fenced || e.yielded || e.fatal)) throw e;
      const tries = num(job.tries) + 1, why = String((e && e.message) || e).slice(0, 300);
      await env.call({ op: "takeOffJournal", orderId: rid, job: key, set: { status: tries >= MAX_TRIES ? "stuck" : "waiting", tries, why: tries >= MAX_TRIES ? stuckWords(env, why) : why } }, `takeoff|fail|${rid}|${key}|${tries}`);
      r = { status: "waiting" };
    }
    if (r.status === "done" || r.status === "restored") out.done++; else if (r.status === "held") out.held++; else out.waiting++;
  }
  // nothing open any more: the journal leaves the lane's list
  const after = await env.read({ op: "takeOffGet", orderIds: [rid] }), j2 = after && after.journals && after.journals[rid];
  if (j2 && j2.due && !hasWork(j2)) await env.call({ op: "takeOffJournal", orderId: rid, set: { due: false } }, `takeoff|closed|${rid}|${num(j2.updatedAt)}`);
  if (j2 && !j2.due && hasWork(j2)) await env.call({ op: "takeOffJournal", orderId: rid, set: { due: true } }, `takeoff|open|${rid}|${num(j2.updatedAt)}`);
  return out;
}

/** The words of a take-off the cloud gave up on after MAX_TRIES. (S5, Paul 21 Nov 2026: "take the pieces off on a computer" is impossible in mode "on", where every page is a
    viewer and the server refuses its production writes: there, Try again (a takeOffAsk retry, the button beside these words on every page) is the one way, and it is said so.) */
function stuckWords(env, why) {
  return env && env.mode === "on"
    ? `The cloud could not finish this take-off (${why}): press Try again and the cloud starts it again`
    : `The cloud could not finish this take-off (${why}): press Try again, or take the pieces off on a computer`;
}

/** The pieces of the order the job takes off, from its answer of getOrderPieces: { onSheets: Map sheetId → { sheet, ids }, loose: [ids], lineKeys } */
function scopeOf(job, o, lines, rid) {
  const placement = (o && o.placement) || {}, sheets = new Map(((o && o.sheets) || []).map(s => [String(s.id), s]));
  // (a completed order: the lines the ledger covers; S1, Paul 21 Nov 2026: a hold that names its lines, a person's Skip piece in Review on a staff computer, takes off the pieces of those lines only)
  const lineKeys = job.kind === "completed" || (job.kind === "hold" && (job.lineKeys || []).length) ? ((job.lineKeys || []).length ? job.lineKeys.slice() : null) : null;
  // (S5, Paul 21 Nov 2026: the sheet window's "Only this charm" / "The whole order" on a staff computer: only the pieces the person chose come off, the rest of the order stays;
  //  scope "order" takes them off wherever they are, scope "sheet" (or none, as before) off the job's sheet only)
  const only = Array.isArray(job.only) && job.only.length ? new Set(job.only.map(String)) : null;
  const bySheet = !!job.sheetId && job.scope !== "order";
  const inScope = id => (!only || only.has(String(id))) && (!lineKeys || lineKeys.some(k => String(id).startsWith(k + "_"))) && String(id).startsWith(rid + "_");
  const onSheets = new Map(), loose = [];
  for (const [id, p] of Object.entries(placement)) {
    if (!p || !inScope(id)) continue;
    if (p.state === "sheet" && p.sheetId) {
      if (bySheet && String(p.sheetId) !== String(job.sheetId)) continue;
      const k = String(p.sheetId); if (!onSheets.has(k)) onSheets.set(k, { sheet: sheets.get(k) || { id: k }, ids: [], label: p.sheetLabel || k });
      onSheets.get(k).ids.push(id);
    } else if (p.state === "waiting" && !bySheet) loose.push(id);
  }
  // (the lines the job changes: those it names, else every line of the order the run holds)
  const keys = lineKeys || Object.keys(lines).filter(k => ridOf(k) === rid);
  return { onSheets, loose, lineKeys: keys };
}

/** One take-off job, from its first step to its last; every step can run again. → { status } */
async function workJob(env, rid, key, job, save) {
  const { read } = env, call = (body, tag) => env.call(body, `takeoff|${rid}|${key}|${tag}`), lines = (env.run && env.run.lines) || {};
  const by = job.by || "System";
  await call({ op: "takeOffJournal", orderId: rid, job: key, set: { status: "working", tries: num(job.tries) } }, "working|" + num(job.tries));
  // 1 · a cancel is read again just before anything moves: an order not cancelled any more is left as it is
  if (job.kind === "cancel") {
    const c = await cancelsOf(read, [rid]), rec = c.get(rid);
    if (!rec || Math.round(num(rec.at)) !== num(job.at)) {
      await call({ op: "takeOffJournal", orderId: rid, job: key, set: { status: "restored", why: rec ? "cancelled again later: that cancel is its own take-off" : "not cancelled any more: nothing was moved", doneAt: Date.now() } }, "restored");
      if (!rec) await call({ op: "takeOffAck", orderId: rid, at: job.at, forget: true }, "forget");
      return { status: "restored" };
    }
  }
  // 2 · where every piece of it is (one snapshot)
  const a = await read({ op: "getOrderPieces", orderIds: [rid] });
  if (!a || a.error || !a.orders) throw new Error("where the order's pieces are could not be read: " + ((a && a.error) || "no answer"));
  const o = a.orders[rid] || { pools: [], sheets: [] }, sc = scopeOf(job, o, lines, rid);
  // 3 · off each sheet, one transaction each; then the pieces on no sheet
  const taken = [], left = [], waiting = [], freed = [], looseMore = [];
  // (a piece already off since an earlier pass of this job, or an earlier take-off: its line no longer names it either)
  const removed = new Set(Object.entries(o.placement || {}).filter(([, p]) => p && ["held", "removed", "superseded"].includes(p.state)).map(([id]) => String(id))), kept = new Set();
  for (const [sheetId, x] of sc.onSheets) {
    if (Date.now() >= num(env.budgetEnd)) { waiting.push({ where: x.label, why: "the pass ran out of time: the next one goes on" }); continue; }
    const r = await call({ op: "cloudTakeOff", orderId: rid, sheetId, ids: x.ids, kind: job.kind, at: job.at, who: by, note: job.note || "", reason: job.kind === "cancel" ? "cancelled" : "", job: key, autoOnly: job.kind === "cancel" && !!job.etsy }, "sheet|" + sheetId + "|" + x.ids.slice().sort().join(","));
    const where = (r && r.label) || x.label || sheetId;
    if (r && r.status < 400 && r.ok) {
      for (const id of r.removed || []) removed.add(String(id));
      for (const id of r.extended || []) removed.add(String(id));
      if ((r.removed || []).length) { taken.push({ where, sheetId, ids: r.removed }); freed.push(sheetId); }
      if ((r.kept || []).length) { for (const id of r.kept) kept.add(String(id)); left.push({ where, sheetId, ids: r.kept, cut: r.keptWhy === "already cut" || r.keptWhy === "sent to the station", only: !!r.only, why: r.keptWhy || "" }); }
      // (the sheet does not list them after all: a row's hint was out of date, or an earlier pass took them off: their rows are looked at on their own below)
      if (r.already && !(r.removed || []).length && !(r.kept || []).length) looseMore.push(...x.ids);
      if (r.refile) await call({ op: "labelsAsk", kind: "setFiles", setId: r.refile, by }, "setfiles|" + r.refile);   // (its set was released: its labels, manifest and files are made again)
    } else if (r && r.waiting) waiting.push({ where, why: r.error || "busy" });
    else throw new Error(`${where}: ${(r && r.error) || "the take-off was not answered"}`);
  }
  const loose = [...new Set(sc.loose.concat(looseMore))];
  if (loose.length) {
    const r = await call({ op: "cloudTakeOff", orderId: rid, sheetId: null, ids: loose, kind: job.kind, at: job.at, who: by, note: job.note || "", reason: job.kind === "cancel" ? "cancelled" : "", job: key }, "pool|" + loose.slice().sort().join(","));
    if (r && r.status < 400 && r.ok) { for (const id of (r.removed || []).concat(r.off || [])) removed.add(String(id)); if ((r.onSheet || []).length) waiting.push({ where: "a sheet", why: "a piece went onto a sheet meanwhile: it comes off at the next pass" }); }
    else if (r && r.waiting) waiting.push({ where: "the pool", why: r.error || "busy" });
    else throw new Error(`the pool: ${(r && r.error) || "the take-off was not answered"}`);
  }
  if (freed.length) save.freed = [...new Set([...(save.freed || ((env.state && env.state.takeoff && env.state.takeoff.freed) || [])), ...freed])].slice(-100);
  // 4 · the run's lines (only once every sheet has given its pieces up: a line never says gone while a piece of it still waits on a sheet)
  if (!waiting.length) await patchLines(env, rid, key, job, sc, removed, taken);
  // 5 · the records
  if (job.kind === "cancel" && !waiting.length) {
    const fates = fatesOf(taken, left);
    if (fates.length && !job.fatesSaved) { const r = await call({ op: "cancelFates", orderId: rid, fates, cancelAt: job.at, by }, "fates|" + fates.map(f => f.sheet + ":" + f.fate).join(",")); if (r && r.status < 400) job.fatesSaved = true; }
    const queued = sc.lineKeys.filter(k => liveLine(lines[k]) || (lines[k] && lines[k].state === "gone" && lines[k].reason === reasonOf(job))).length;
    if (queued && !job.queueSaid) {
      const where = "the queue", text = `Taken out of the queue (${queued} piece${queued === 1 ? "" : "s"})`;
      await call({ op: "timelineAdd", events: [{ orderId: rid, type: "cancelStep", by, station: "sorter", sheet: where, text, id: `cx-${Math.round(num(job.at))}-${where}`, at: Date.now(), data: { outcome: "removed", done: true, sheet: where, kind: "queue", cancelAt: Math.round(num(job.at)) } }] }, "queue");
      await call({ op: "cancelFates", orderId: rid, fates: [], by, removals: [{ id: `queue~${where}`, where, kind: "queue", outcome: "removed", by, text }] }, "queue|record");
      job.queueSaid = true;
    }
    // what stays on sheets that were cut (or could not give the pieces up) is said in the run pill of every page, until someone sets it aside
    if (left.length && !job.noticed) {
      const text = noticeText(rid, left), where = joinAnd(left.map(l => l.where));
      const n = { rid, at: num(job.at), text, where, t: Date.now(), sheets: left.map(l => l.where).slice(0, 12) };
      await call({ op: "sharePut", name: NOTICES, set: { [`n:${rid}:${num(job.at)}`]: n } }, "notice");
      await call({ op: "timelineAdd", events: [{ orderId: rid, type: "note", by: "System", station: "sorter", text: text.slice(0, 200), data: { cancelled: true, sheets: left.map(l => l.where), pieces: left.reduce((s, l) => s + ((l.ids || []).length || 1), 0) }, id: `autocancel-aside-${num(job.at)}`, at: Date.now() }] }, "notice|timeline");
      job.noticed = true;
      const prev = save.notices || ((env.state && env.state.takeoff && env.state.takeoff.notices) || []);
      save.notices = prev.filter(x => !(x.rid === rid && num(x.at) === num(job.at))).concat([{ rid, at: num(job.at) }]).slice(-100);
    }
  }
  // the job's end: done, or waiting (a sheet at work, the next pass goes on)
  const status = waiting.length ? "waiting" : "done";
  const why = waiting.length ? waiting.map(w => `${w.where}: ${w.why}`).join("; ").slice(0, 400) : null;
  const longWait = status === "waiting" && Date.now() - num(job.at) > WAIT_WORDS_MS;
  await call({ op: "takeOffJournal", orderId: rid, job: key, set: { status, why: longWait ? `Waiting since ${new Date(num(job.at)).toISOString().slice(11, 16)} UTC: ${why}` : why, taken: taken.map(t => ({ where: t.where, sheetId: t.sheetId, n: (t.ids || []).length })), left: left.map(l => ({ where: l.where, sheetId: l.sheetId, why: l.why, n: (l.ids || []).length })), removed: [...removed].slice(0, 300), fatesSaved: !!job.fatesSaved, queueSaid: !!job.queueSaid, noticed: !!job.noticed, ...(status === "done" ? { doneAt: Date.now() } : {}) } }, "end|" + status + "|" + removed.size + "|" + kept.size + "|" + waiting.length);
  if (job.kind === "cancel" && status === "done") await call({ op: "sharePut", name: NOTICES, set: { [`d:${rid}`]: { at: num(job.at), t: Date.now() } } }, "done");
  // (RETRYLEFT: a cancel worked again that leaves nothing on any sheet now: its "set them aside" notice goes from every page's run pill)
  if (job.kind === "cancel" && status === "done" && !left.length && job.noticed && num(job.again) > 0) await call({ op: "takeOffAck", orderId: rid, at: job.at, forget: true }, "aside-gone|" + num(job.again));
  if (env.say) env.say(`[takeoff] ${rid} (${job.kind}): ${status}${taken.length ? " · taken off " + joinAnd(taken.map(t => t.where)) : ""}${left.length ? " · stays on " + joinAnd(left.map(l => l.where)) : ""}${why ? " · " + why : ""}`);
  return { status };
}

/** The run's lines after the take-off (only the fields that change; a line that already says it is not written again). */
async function patchLines(env, rid, key, job, sc, removed, taken) {
  const lines = (env.run && env.run.lines) || {}, patch = {}, now = Date.now(), names = joinAnd([...new Set(taken.map(t => t.where))]);
  for (const k of sc.lineKeys) {
    const l = lines[k]; if (!l || (l.state === "committed" && !(l.poolIds || []).some(id => removed.has(String(id))))) continue;
    const ids = (l.poolIds || []).map(String), rest = ids.filter(id => !removed.has(id)), p = {};
    if (rest.length !== ids.length) p.poolIds = rest;
    if (AUTO.has(job.kind)) {
      const reason = reasonOf(job);
      if (l.state !== "gone") p.state = "gone";
      if (l.reason !== reason) p.reason = reason;
      if (job.kind === "shipped" && !l.shippedAt) p.shippedAt = num(job.shippedAt) || now;
      if (job.kind === "completed" && l.wait) p.wait = null;
    } else if (job.kind === "hold" || job.kind === "sheetOff") {
      const touched = ids.some(id => removed.has(id)) || (job.kind === "hold" && !job.sheetId);
      if (!touched) continue;
      if (!rest.length && l.state !== "gone" && l.state !== "held" && l.state !== "skipped") {   // (S1: a skipped line stays skipped: only its pieces leave)
        const text = `Taken off ${names || "its sheet"} by ${job.by || "someone"}${job.note ? ": " + job.note : ""}`;
        Object.assign(p, { state: "held", hold: text, reason: text, heldAt: now, holdSeen: false });
      }
    }
    const changed = Object.entries(p).filter(([f, v]) => JSON.stringify(l[f] === undefined ? null : l[f]) !== JSON.stringify(v === undefined ? null : v));
    if (changed.length) patch[k] = Object.fromEntries(changed);
  }
  const keys = Object.keys(patch);
  if (!keys.length) return 0;
  for (let i = 0; i < keys.length; i += 300) {
    const part = Object.fromEntries(keys.slice(i, i + 300).map(k => [k, patch[k]]));
    const r = await env.call({ op: "runLinesPatch", runId: env.runId, lines: part }, `takeoff|${rid}|${key}|lines|${JSON.stringify(part).length}|${keys.slice(i, i + 300).join(",")}`);
    if (!r || r.status >= 400) throw new Error("the run's lines were not written: " + ((r && r.error) || "no answer"));
  }
  for (const k of keys) lines[k] = Object.assign({}, lines[k], patch[k]);   // (the pass goes on with the lines as written)
  return keys.length;
}

/* ── REVALIDATE (§1.7): the run's lines against the world, behind the freshness gate ── */
const STEPS = {
  revalidate: [
    // 1 · the freshness gate: a failing condition waits with words, and the step goes on by itself as soon as it passes (no Etsy call is made for it)
    async ctx => {
      const receipts = [...new Set(Object.entries(ctx.run.lines || {}).filter(([, l]) => liveLine(l) && !l.etsyClosed).map(([k]) => ridOf(k)))];
      const g = await ctx.gate({ receipts });
      if (!g.ok) return { wait: "world", why: `Re-checking the orders waits: ${g.why.join("; ")}`.slice(0, 300) };
      return undefined;
    },
    // 2 · each line against the world: cancelled, closed by Etsy, changed. A take-off it found is done by the lane first (the next pass begins with it): the step goes on only
    //     once none of the run's orders has a take-off open, so labels and commit never see an order that is leaving
    async ctx => {
      const r = await revalidate(ctx);
      const open = await openOf(ctx.db, new Set(Object.keys(ctx.run.lines || {}).map(ridOf)));
      if (open.length) return { wait: "takeoff", why: `Re-checking the orders: ${open.length === 1 ? "order " + open[0] + " is" : open.length + " orders are"} being taken off sheets first`.slice(0, 300), ids: open.slice(0, 50) };
      return r && r.goto ? { goto: r.goto } : undefined;
    }
  ]
};
/** The run's lines against the world (no Etsy call): → { cancelled, closed, changed, shipped, goto? } */
async function revalidate(ctx) {
  const { db, read } = ctx, lines = ctx.run.lines || {}, out = { cancelled: [], closed: [], changed: [], shipped: [] }, now = Date.now();
  const keep = new Set(((ctx.run.keepShipped) || []).map(String));
  const live = Object.entries(lines).filter(([, l]) => liveLine(l));
  const rids = [...new Set(live.map(([k]) => ridOf(k)))];
  // which receipts are open: the world's own map of the mail system's copy (Charm_World/state.open); a receipt not in it is read from that copy (status, shipped) and its cancel record
  const ws = await db.collection("Charm_World").doc("state").get(), open = ws.exists ? (ws.data() || {}).open || {} : null;
  if (!open) throw new Error("the world's open orders could not be read: the step is tried again");
  // (a custom piece made here, CUSTOM-<day>-<n>, is no Etsy order: never judged by Etsy's list)
  const notOpen = rids.filter(r => /^\d{4,20}$/.test(r) && open[r] == null && !live.filter(([k]) => ridOf(k) === r).every(([, l]) => l.etsyClosed));
  const cancels = notOpen.length ? await cancelsOf(read, notOpen) : new Map();
  const patch = {}, jobs = [];
  for (const rid of notOpen) {
    const mine = live.filter(([k]) => ridOf(k) === rid);
    if (cancels.has(rid)) { const rec = cancels.get(rid); jobs.push([rid, `cancel-${Math.round(num(rec.at))}`, { kind: "cancel", at: Math.round(num(rec.at)), by: rec.source === "etsy" ? "Etsy" : str(rec.by, 80) || "someone", etsy: rec.source === "etsy" || rec.by === "Etsy" }]); out.cancelled.push(rid); continue; }
    // (KEEPSHIPPED: an order a person put back on its sheet after Etsy closed it stays as it is)
    if (keep.has(rid)) continue;
    // OURSHIPONLY: closed on Etsy (shipped, completed, refunded) and not cancelled: marked, and it stays exactly where it is in production
    for (const [k, l] of mine) if (!l.etsyClosed) patch[k] = Object.assign(patch[k] || {}, { etsyClosed: now });
    out.closed.push(rid);
  }
  // the orders Etsy updated after they were read (the world build's change marks): changePending, and the engraving asked again; pieces move only after a person accepts
  const cs = await db.collection("Charm_World").doc("changed").get(), marks = cs.exists ? (cs.data() || {}).marks || {} : {};
  let engraveAgain = false; const takenBack = [];
  for (const [k, l] of live) {
    const m = marks[k]; if (!m || !(num(m.at) > num(l.changedAt))) continue;
    const p = Object.assign(patch[k] || {}, { changePending: true, repoolChanged: !!m.shape, changedAt: num(m.at), changedWhy: str(m.why, 300) });
    if (l.engrave && typeof l.engrave === "object" && (m.shape || m.words)) { if (l.engrave.approved && l.engrave.needed) takenBack.push([k, m, l.engrave]); p.engrave = Object.assign({}, l.engrave, { invalidatedBy: "order changed", approved: false, state: "reclassify" }); if (l.engrave.needed) engraveAgain = true; }
    patch[k] = p; out.changed.push(k);
  }
  const keys = Object.keys(patch);
  for (let i = 0; i < keys.length; i += 300) {
    const part = Object.fromEntries(keys.slice(i, i + 300).map(k => [k, patch[k]]));
    const r = await ctx.call({ op: "runLinesPatch", runId: ctx.runId, lines: part }, `revalidate|lines|${keys.slice(i, i + 300).join(",")}|${JSON.stringify(part).length}`);
    if (!r || r.status >= 400) throw new Error("the run's lines were not written: " + ((r && r.error) || "no answer"));
    for (const k of keys.slice(i, i + 300)) lines[k] = Object.assign({}, lines[k], patch[k]);
  }
  // (WITHDRAWNSAID, Paul 23 Nov 2026: an approval taken back by an order's change was said nowhere: one line on the order's timeline, best-effort)
  if (takenBack.length) { try { await ctx.call({ op: "timelineAdd", events: takenBack.slice(0, 100).map(([k, m, e]) => ({ orderId: ridOf(k), type: "note", at: now, by: "System", station: "sorter", lineKey: k, id: `${k}-withdrawn-${Math.round(num(m.at))}`,
    text: `Engraving approval${e.approvedBy ? ` (${str(e.approvedBy, 60)})` : ""} taken back: ${str(m.why, 120) || "the order changed"}`.slice(0, 200), data: { approvedAt: num(e.approvedAt), approvedBy: str(e.approvedBy, 80) } })) }, `revalidate|withdrawn|${takenBack.map(x => x[0]).join(",").slice(0, 300)}|${Math.max(...takenBack.map(x => Math.round(num(x[1].at))))}`); } catch (e) { if (e && (e.fenced || e.yielded || e.fatal || e.overBudget)) throw e; } }
  // OURSHIPONLY: the orders Etsy closed that a shipping station marked shipped leave now, as a shipped order always has
  const closed = [...new Set(Object.entries(lines).filter(([, l]) => l && l.etsyClosed && liveLine(l)).map(([k]) => ridOf(k)))].slice(0, 300);
  for (let i = 0; i < closed.length; i += 100) { const g = await read({ op: "shippedByUs", orders: closed.slice(i, i + 100) }); for (const [rid, ev] of Object.entries((g && g.shipped) || {})) { jobs.push([rid, `shipped-${Math.round(num(ev.at))}`, { kind: "shipped", at: now, shippedAt: num(ev.at) || now, by: str(ev.by, 80) || "Shipping" }]); out.shipped.push(rid); } }
  // the take-off jobs (journalled before anything moves); the lane does them, this pass if time allows, else the next
  for (const [rid, key, create] of jobs) await ctx.call({ op: "takeOffJournal", orderId: rid, job: key, create: Object.assign({ status: "due", tries: 0, runId: ctx.runId }, create), set: { status: "due" }, runId: ctx.runId }, `takeoff|journal|${rid}|${key}`);
  // a changed order whose engraving is asked again goes back to the engraving step (the page: "N order(s) changed — back to engraving")
  if (engraveAgain) out.goto = "engrave";
  return out;
}

/** The orders of `rids` with a take-off journal still open (due): the step waits for them. */
async function openOf(db, rids) {
  const all = await openJournals(db), out = [];
  for (const [rid, jd] of all) if (rids.has(rid) && hasWork(jd)) out.push(rid);
  return out;
}

/* ── the cron's question (§1.1): is a take-off due? One read of the cancel counter and one small query; the lane's look every 10 minutes. ── */
async function due(db, S, now) {
  const t = (S && S.takeoff) || {}; let reads = 0;
  try { const g = await db.collection("Charm_Nest_Rev").doc("cancel").get(); reads++; const n = g.exists ? num((g.data() || {}).n) : 0; if (t.cancelGen == null || n !== num(t.cancelGen)) return { why: "a cancel was recorded", reads }; } catch (_) { /* not read: the look below */ }
  try { const q = await db.collection(COLL).where("due", "==", true).limit(1).get(); reads++; if (q.size) return { why: "a take-off waits", reads }; } catch (_) {}
  if (now - num(t.lookAt) > LOOK_MS) return { why: "the take-off look is due", reads };
  return { why: null, reads };
}

module.exports = { lane, due, STEPS, revalidate, noticeText, fatesOf, scopeOf, stuckWords, COLL, LOOK_MS };
