/*  netlify/functions/_orderCancel.js
 *  One cancel record per order, whoever cancelled it (Paul A1 · A6, 28 Sep): a person in the sorter (op_cancelPut), Etsy
 *  as the inbox's receipts mirror sees it (etsyMailReceiptsMirrorCron, every 7-10 min, no Etsy call of its own), the
 *  one-off sweep of the mirror's older receipts (op_cancelSweep) and the sandbox's pretend cancel (op_sandboxCancel) all
 *  write through here, so the record and its timeline stamp always read the same way. Nothing here calls Etsy.
 *
 *  Charm_Nest_Cancelled/{orderId}   (Sandbox_Charm_Nest_Cancelled in the sandbox)
 *    orderId · by (a person's name, or "Etsy") · why · at (ms) · buyer · placedAt · shipBy (ms) · sheets[] · lines[]
 *    source      "sorter" (a person) | "etsy" (Etsy cancelled it)
 *    etsyStatus  Etsy's own word for the receipt ("Canceled", "Fully Refunded"), once Etsy is known to have cancelled it
 *    etsyAt      when Etsy's receipt last changed (ms), with etsyStatus
 *    fates       what became of its pieces, sheet by sheet (noteFates)
 *    removals    every removal the cancel caused, each with when, where, who and how it went (noteRemovals, below):
 *                the record alone tells the story. Only added to or updated, never pruned
 *    createdAt
 *
 *  Kept for good (Paul, 29 Sep): nothing prunes, expires or collects a record. A restore deletes it (the sorter reads the
 *  ids to keep an order out of the pull), after copying the whole record to Charm_Nest_Cancelled_History/{id}~{at}
 *  (op cancelRestore, in the same batch) and onto the timeline's cancelRestored event.
 *
 *  The rule for "Etsy cancelled it" (isCancelled): the receipt's status matches /cancel/i ("Canceled"), or it is
 *  "Fully Refunded" and not shipped (a full refund before shipping is how a paid order is called off; after shipping it
 *  is a return, and the order has already left). "Partially Refunded" is not a cancel.
 *
 *  Who wins: the first record stands. A later Etsy detection never overwrites a person's record: it adds etsyStatus and
 *  fills only what the record lacks. A person cancelling an order Etsy already cancelled keeps Etsy as the canceller and
 *  adds the sheets and lines the sorter knows. A person cancelling their own record again writes it anew (as before),
 *  keeping what Etsy said. An order a person restored after Etsy's last change, or restored when Etsy already said what it
 *  says now, is not cancelled again by a detection (a new Etsy status is news). Only a restore of a record that carried
 *  Etsy's word counts: a person restoring their own cancel did not know Etsy's, which is then recorded.
 *
 *  Timeline (_orderTimeline.js): Etsy's cancel is one `etsyCancelled` event keyed by the order id (so the mirror seeing
 *  the receipt again, the sweep and the sorter all land on the same event); a person's is a `cancelled` event with
 *  their name. Each is written in the same batch or transaction as the record. */
"use strict";
const Timeline = require("./_orderTimeline");
const COL = "Charm_Nest_Cancelled", RECEIPTS = "EtsyMail_Receipts";
const ETSY_WHY = "Cancelled on Etsy";
// what the sweep asks the mirror for, an equality query each (Etsy writes "Canceled" and "Fully Refunded"; the rest cost nothing)
const SWEEP_STATUSES = ["Canceled", "Cancelled", "canceled", "cancelled", "Fully Refunded", "fully refunded"];
const SWEEP_FIELDS = ["receipt_id", "status", "is_shipped", "updated_timestamp", "created_timestamp", "buyer_name", "raw.receipt_id", "raw.status", "raw.is_shipped", "raw.name", "raw.transactions"];

const idOf = Timeline.orderIdOf;
const s = (v, n) => String(v == null ? "" : v).slice(0, n);
const n = v => (Number.isFinite(+v) ? +v : 0);
const toMs = v => (n(v) > 0 ? (n(v) < 1e11 ? Math.round(n(v) * 1000) : Math.round(n(v))) : 0);   // Etsy's seconds, or ms already
const rawOf = r => (r && r.raw && typeof r.raw === "object" ? r.raw : r || {});

/** Has Etsy cancelled this receipt? Takes Etsy's receipt or the mirror's EtsyMail_Receipts document. */
function isCancelled(r) {
  if (!r || typeof r !== "object") return false;
  const raw = rawOf(r), st = String(r.status || raw.status || "");
  if (/cancel/i.test(st)) return true;
  return /fully\s*refund/i.test(st) && !(r.is_shipped || raw.is_shipped || raw.was_shipped);
}
const lineOf = l => ({ transactionId: s(l && l.transactionId, 30), sku: s(l && l.sku, 80), title: s(l && l.title, 200), quantity: Math.max(1, Math.round(n(l && l.quantity)) || 1), material: s(l && l.material, 40) });
/** A cancel record as it is stored (createdAt aside). */
function record(x) {
  const etsy = x.source === "etsy";
  const out = {
    orderId: idOf(x.orderId), by: etsy ? "Etsy" : s(x.by || "operator", 80), why: s(x.why != null && x.why !== "" ? x.why : etsy ? ETSY_WHY : "", 400),
    at: n(x.at) || Date.now(), buyer: s(x.buyer, 120), placedAt: n(x.placedAt), shipBy: n(x.shipBy),
    sheets: (Array.isArray(x.sheets) ? x.sheets : []).slice(0, 30).map(v => s(v, 100)).filter(Boolean),
    lines: (Array.isArray(x.lines) ? x.lines : []).slice(0, 60).map(lineOf), source: etsy ? "etsy" : "sorter"
  };
  if (x.etsyStatus) { out.etsyStatus = s(x.etsyStatus, 40); out.etsyAt = n(x.etsyAt) || out.at; }
  return out;
}
/** Etsy's cancel of a receipt as a record: at = when the receipt last changed. */
function fromReceipt(r) {
  const raw = rawOf(r), tx = Array.isArray(raw.transactions) ? raw.transactions : [];
  const ships = tx.map(t => toMs(t && t.expected_ship_date)).filter(Boolean);
  const at = toMs(r.updated_timestamp || raw.updated_timestamp) || Date.now();
  return record({
    orderId: r.receipt_id || raw.receipt_id, source: "etsy", at, etsyStatus: s(r.status || raw.status, 40) || "Canceled", etsyAt: at,
    buyer: r.buyer_name || raw.name || "", placedAt: toMs(r.created_timestamp || raw.created_timestamp), shipBy: ships.length ? Math.min(...ships) : 0,
    lines: tx.map(t => ({ transactionId: t && t.transaction_id != null ? String(t.transaction_id) : "", sku: t && t.sku, title: t && t.title, quantity: t && t.quantity }))
  });
}

// a person's record (one written before `source` was kept is a person's unless it says Etsy), and one Etsy is known to have cancelled
const personal = x => (x.source ? x.source !== "etsy" : x.by !== "Etsy");
const etsyKnown = x => !!(x && (x.etsyStatus || !personal(x)));
const etsyEvent = (rec, o) => ({ orderId: rec.orderId, type: "etsyCancelled", id: rec.orderId, at: rec.etsyAt || rec.at, by: "Etsy", source: "etsy", station: "",
  // (at: Etsy's own moment, the receipt's change; seenAt: when the mirror or the sorter saw it, Paul 29 Sep 00:26)
  text: `Cancelled on Etsy${rec.etsyStatus ? ` (${rec.etsyStatus})` : ""}`, data: { etsyStatus: rec.etsyStatus || "", detectedBy: o.detectedBy || "", notedBy: o.person && o.person !== "Etsy" ? o.person : "", etsyAt: rec.etsyAt || rec.at, seenAt: Date.now() } });
const personEvent = (rec, o) => ({ orderId: rec.orderId, type: "cancelled", id: s(o.eventId, 80) || String(rec.at), at: rec.at, by: rec.by, source: "sorter", station: "sorter",
  text: rec.why ? `Cancelled · ${rec.why}` : "Cancelled", data: { why: rec.why, sheets: rec.sheets } });
/** What a record lacks that the new cancel knows: buyer, dates, lines when it has none, sheets it did not name. */
function fill(cur, inc, patch, linesToo) {
  for (const k of ["buyer", "placedAt", "shipBy"]) if (!cur[k] && inc[k]) patch[k] = inc[k];
  if (inc.lines.length && (linesToo || !(Array.isArray(cur.lines) && cur.lines.length))) patch.lines = inc.lines;
  const had = Array.isArray(cur.sheets) ? cur.sheets : [], sheets = [...new Set(had.concat(inc.sheets))].slice(0, 30);
  if (sheets.length > had.length) patch.sheets = sheets;
  return patch;
}
/** What a new cancel does to the record there is: { kind: "create" | "set" | "update" | null, doc, record, events, kept }. */
function plan(cur, inc, o = {}) {
  if (!cur) return { kind: "create", doc: inc, record: inc, events: [inc.source === "etsy" ? etsyEvent(inc, o) : personEvent(inc, o)], kept: false };
  if (inc.source === "etsy") {
    // a record is there (a person's, or Etsy's own): it stands; Etsy's word is added, and what it lacked
    const patch = {};
    if (inc.etsyStatus && cur.etsyStatus !== inc.etsyStatus) { patch.etsyStatus = inc.etsyStatus; if (!cur.etsyAt) patch.etsyAt = inc.etsyAt || inc.at; }
    fill(cur, inc, patch, false);
    const events = etsyKnown(cur) ? [] : [etsyEvent(Object.assign({}, inc, patch), o)];
    return { kind: Object.keys(patch).length ? "update" : null, doc: patch, record: Object.assign({}, cur, patch), events, kept: true };
  }
  if (personal(cur)) {
    // a person's record again: written anew, as it always was, keeping what Etsy said of it (and what became of its
    // pieces, and every removal it recorded: never lost to a second press)
    const doc = Object.assign({}, inc); if (cur.etsyStatus) { doc.etsyStatus = cur.etsyStatus; doc.etsyAt = n(cur.etsyAt); }
    if (Array.isArray(cur.fates) && cur.fates.length) doc.fates = cur.fates;
    if (Array.isArray(cur.removals) && cur.removals.length) doc.removals = cur.removals;
    if (n(cur.removalsLeftOut)) doc.removalsLeftOut = n(cur.removalsLeftOut);
    return { kind: "set", doc, record: doc, events: [personEvent(inc, o)], kept: false };
  }
  // Etsy cancelled it first: Etsy stays the canceller; the sorter adds the sheets and the lines as it knows them
  const patch = fill(cur, inc, {}, true);
  return { kind: Object.keys(patch).length ? "update" : null, doc: patch, record: Object.assign({}, cur, patch), events: [personEvent(inc, o)], kept: true };
}
const colOf = (db, prefix) => db.collection((prefix || "") + COL);
const eventRows = (events, prefix) => events.map(e => Timeline.clean(e, { prefix })).filter(Boolean);
function stage(w, db, FV, ref, p, prefix, create) {
  if (p.kind === "create") w[create ? "create" : "set"](ref, Object.assign({}, p.doc, { createdAt: FV.serverTimestamp() }));
  else if (p.kind === "set") w.set(ref, Object.assign({}, p.doc, { createdAt: FV.serverTimestamp() }));
  else if (p.kind === "update") w.update(ref, p.doc);
  for (const { key, doc } of eventRows(p.events, prefix)) w.set(db.collection((prefix || "") + Timeline.COL).doc(key), Object.assign({}, doc, { createdAt: FV.serverTimestamp() }), { merge: true });
}
const tidy = r => { const x = Object.assign({}, r); delete x.createdAt; return x; };

/** One cancel, in a transaction (a person's, the sorter's word that Etsy cancelled it, the sandbox's pretend one).
    opts: prefix · person (who pressed it) · detectedBy · eventId · mustExist (putMany's retry of a record it read: gone
    since means a person restored the order meanwhile, and a detection does not bring it back). */
async function put(db, FV, inc, opts = {}) {
  if (!inc || !inc.orderId) return { error: "orderId required" };
  const ref = colOf(db, opts.prefix).doc(inc.orderId);
  const p = await db.runTransaction(async t => {
    const snap = await t.get(ref), cur = snap.exists ? snap.data() : null;
    if (!cur && opts.mustExist) return null;
    const pl = plan(cur, inc, opts);
    stage(t, db, FV, ref, pl, opts.prefix, false);
    return pl;
  });
  if (!p) return { ok: true, record: null, created: false, kept: false, changed: false, gone: true };
  return { ok: true, record: tidy(p.record), created: p.kind === "create", kept: p.kept, changed: !!p.kind };
}

/** Orders a person restored after Etsy's last change (a cancelRestored event at or after it), or restored when Etsy
    already said what it says now (the event keeps the record, with its etsyStatus: the person knew, and a later change to
    the receipt, such as a note, is no news): a detection leaves them be. A new Etsy status after a restore cancels, and so
    does Etsy's cancel of an order whose restored record was a person's own, with no word from Etsy on it. */
async function restoredSince(db, recs, prefix) {
  const last = {}, knew = {}, ids = [...new Set(recs.map(r => r.orderId))], low = v => String(v || "").trim().toLowerCase();
  for (let i = 0; i < ids.length; i += 30) {
    const snap = await db.collection((prefix || "") + Timeline.COL).where("orderId", "in", ids.slice(i, i + 30)).select("orderId", "type", "at", "data.cancelled.etsyStatus", "data.cancelled.source", "data.cancelled.by").get();
    for (const d of snap.docs) {
      const x = d.data(); if (x.type !== "cancelRestored") continue;
      // the restore of a person's own cancel, with no word from Etsy on it, says nothing of Etsy's cancel (the person did
      // not know): Etsy's still counts. (A restore event that kept no record reads as before.)
      const c = x.data && x.data.cancelled; if (c && typeof c === "object" && !etsyKnown(c)) continue;
      last[x.orderId] = Math.max(last[x.orderId] || 0, n(x.at));
      const st = low(x.data && x.data.cancelled && x.data.cancelled.etsyStatus); if (st) (knew[x.orderId] = knew[x.orderId] || new Set()).add(st);
    }
  }
  return new Set(recs.filter(r => (last[r.orderId] && last[r.orderId] >= (r.etsyAt || r.at)) || (knew[r.orderId] && knew[r.orderId].has(low(r.etsyStatus)))).map(r => r.orderId));
}
/** Etsy's cancels, many at once (the mirror's page, the sweep): one getAll of their records, then one batch.
    opts: prefix · detectedBy · dryRun. Returns counts, the ids it created, and the orders not written ({ orderId, why }). */
async function putMany(db, FV, recs, opts = {}) {
  const byId = new Map(); for (const r of recs || []) if (r && r.orderId) byId.set(r.orderId, r);
  const list = [...byId.values()], out = { candidates: list.length, created: 0, noted: 0, unchanged: 0, restored: 0, failed: 0, ids: [], failures: [] };
  if (!list.length) return out;
  const refs = list.map(r => colOf(db, opts.prefix).doc(r.orderId)), cur = new Map();
  for (let i = 0; i < refs.length; i += 100) for (const d of await db.getAll(...refs.slice(i, i + 100))) if (d.exists) cur.set(d.id, d.data());
  const missing = list.filter(r => !cur.has(r.orderId)), skip = missing.length ? await restoredSince(db, missing, opts.prefix) : new Set();
  const todo = [];
  for (const [i, r] of list.entries()) {
    if (skip.has(r.orderId)) { out.restored++; continue; }
    const p = plan(cur.get(r.orderId) || null, r, opts);
    if (!p.kind && !p.events.length) { out.unchanged++; continue; }
    todo.push({ r, p, ref: refs[i] });
  }
  const count = x => { if (x.p.kind === "create") { out.created++; out.ids.push(x.r.orderId); } else out.noted++; };
  if (opts.dryRun) { todo.forEach(count); return out; }
  // at most 100 orders (≤ 200 writes) a batch; a batch refused (someone wrote one of them in between) is done one by one
  for (let i = 0; i < todo.length; i += 100) {
    const part = todo.slice(i, i + 100), batch = db.batch();
    for (const x of part) stage(batch, db, FV, x.ref, x.p, opts.prefix, true);
    try { await batch.commit(); part.forEach(count); }
    catch (e) {
      for (const x of part) {
        try {
          let res = await put(db, FV, x.r, Object.assign({}, opts, { mustExist: x.p.kind !== "create" }));
          // gone: restored since it was read. Left be as any restore is (restoredSince): a person's own cancel restored
          // unaware of Etsy's is not, and Etsy's cancel is recorded
          if (res.gone && !(await restoredSince(db, [x.r], opts.prefix)).has(x.r.orderId)) res = await put(db, FV, x.r, opts);
          if (res.created) { out.created++; out.ids.push(x.r.orderId); } else if (res.gone) out.restored++; else if (res.changed) out.noted++; else out.unchanged++; }
        catch (e2) { out.failed++; out.error = s(e2.message || e2, 200); out.failures.push({ orderId: x.r.orderId, why: out.error }); }
      }
    }
  }
  return out;
}
/** The mirror's hook: the cancelled receipts of one page of Etsy receipts. No receipt cancelled → no read at all. */
async function fromReceipts(db, FV, receipts, opts = {}) {
  const recs = (Array.isArray(receipts) ? receipts : []).filter(isCancelled).map(fromReceipt).filter(r => r.orderId);
  if (!recs.length) return { candidates: 0, created: 0, noted: 0, unchanged: 0, restored: 0, failed: 0, ids: [] };
  return putMany(db, FV, recs, Object.assign({ detectedBy: "mirror" }, opts, { prefix: "" }));
}
/** The one-off sweep of the mirror (production): every receipt the mirror keeps as cancelled gets its record. Idempotent.
    Reads one status at a time in document order, 200 a page, and stops at its time budget (after one page at least) with
    more:true and `next` ({ s: status index, after: last document id }): called again with that as `cursor`, it goes on
    from there, so every call makes progress. An order that would not write does not fail the call (it is no `error`, so
    op cancelSweep answers 200 with its progress and resume point): it is listed in `failures` ({ orderId, why }, the first
    50; `failed` counts them all) and kept in the backlog, which the mirror's next run retries (`backlogged`; `backlogError`
    when even that could not be written). A page that still will not read after two more tries stops the call the same
    calm way: ok:false, `readError`, the counts so far, and more:true with that page as `next`.
    opts: dryRun · budgetMs · cursor · docId (FieldPath.documentId()). */
async function sweep(db, FV, opts = {}) {
  const until = Date.now() + (n(opts.budgetMs) || 7000), PAGE = 200, docId = opts.docId || "__name__";
  let c = opts.cursor; if (typeof c === "string") { try { c = JSON.parse(c); } catch (_) { c = null; } }
  let si = Math.max(0, Math.min(SWEEP_STATUSES.length, Math.round(n(c && c.s)))), after = c && c.after ? String(c.after) : "", pages = 0;
  const out = { ok: true, dryRun: !!opts.dryRun, scanned: 0, cancelled: 0, created: 0, noted: 0, unchanged: 0, restored: 0, failed: 0, ids: [], failures: [], more: false, truncated: false };
  // one page: its receipts, then the records they need (putMany reads them all before it writes, and is idempotent)
  const page = async () => {
    let q = db.collection(RECEIPTS).where("status", "==", SWEEP_STATUSES[si]).orderBy(docId);
    if (after) q = q.startAfter(after);
    const snap = await q.select(...SWEEP_FIELDS).limit(PAGE).get(), recs = [];
    for (const d of snap.docs) { const x = d.data(); if (!x.receipt_id) x.receipt_id = d.id; if (isCancelled(x)) { const r = fromReceipt(x); if (r.orderId) recs.push(r); } }
    return { snap, recs, r: recs.length ? await putMany(db, FV, recs, { prefix: "", detectedBy: "sweep", dryRun: !!opts.dryRun }) : null };
  };
  while (si < SWEEP_STATUSES.length) {
    if (pages && Date.now() > until) { out.more = true; out.next = { s: si, after }; break; }
    // a page that will not read is tried twice more (briefly, within the budget); still failing, the call stops calmly
    // with what it did and that page as `next`, so the next call reads it again (ok:false, readError says why)
    let got = null, err = null;
    for (let t = 0; t < 3 && !got; t++) {
      if (t) { const ms = 300 * t; if (Date.now() + ms > until) break; await new Promise(res => setTimeout(res, ms)); }
      try { got = await page(); } catch (e) { err = e; }
    }
    if (!got) { out.ok = false; out.more = true; out.next = { s: si, after }; out.readError = s((err && err.message) || err, 200); break; }
    const { snap, recs, r } = got; pages++;
    out.scanned += snap.size; out.cancelled += recs.length;
    if (r) {
      for (const k of ["created", "noted", "unchanged", "restored", "failed"]) out[k] += r[k];
      out.ids.push(...r.ids); out.failures.push(...(r.failures || []));
    }
    if (snap.size < PAGE) { si++; after = ""; } else after = snap.docs[snap.docs.length - 1].id;
  }
  out.ids = out.ids.slice(0, 200);
  if (out.failures.length) {
    const ids = [...new Set(out.failures.map(f => f.orderId))];
    try { await keepBacklog(db, ids); out.backlogged = ids.length; }
    catch (e) { out.backlogged = 0; out.backlogError = s(e.message || e, 200); }
    out.failures = out.failures.slice(0, 50);
  }
  return out;
}

/* ── the mirror's backlog: a page whose cancels were not recorded (the hook failed, timed out or had no time left) keeps
   their receipt ids here, and the next mirror run retries them first from the receipts the mirror already stored (no Etsy
   call). One small document; the newest BACKLOG_MAX ids are kept (op cancelSweep catches up on any dropped). 2000, not
   the 200 it was: an ID dropped here is an Etsy cancel with no record until someone runs the sweep by hand (29 Sep,
   kept for good), and 2000 ids are some 30 KB of the document's 1 MiB. ── */
const BACKLOG = "Charm_Nest_Cancelled_Backlog", BACKLOG_MAX = 2000, RETRY_MAX = 200;
const backlogRef = db => db.collection(BACKLOG).doc("pending");
const idsOfDoc = snap => (snap && snap.exists && Array.isArray(snap.data().ids) ? snap.data().ids.map(idOf).filter(Boolean) : []);
/** The ids of a page's cancelled receipts (no read). */
const cancelledIds = receipts => [...new Set((Array.isArray(receipts) ? receipts : []).filter(isCancelled).map(r => idOf(r.receipt_id || rawOf(r).receipt_id)).filter(Boolean))];
async function keepBacklog(db, ids) {
  const add = [...new Set((Array.isArray(ids) ? ids : []).map(idOf).filter(Boolean))];
  if (!add.length) return { kept: 0, dropped: 0 };
  const ref = backlogRef(db);
  return db.runTransaction(async t => {
    const snap = await t.get(ref), all = [...new Set(idsOfDoc(snap).concat(add))], ids2 = all.slice(-BACKLOG_MAX);
    t.set(ref, { ids: ids2, at: Date.now(), dropped: n(snap.exists && snap.data().dropped) + (all.length - ids2.length) });
    return { kept: ids2.length, dropped: all.length - ids2.length };
  });
}
/** Retries the backlog (null when there is none: one document read). Ids done (recorded, no longer cancelled, or not in
    the mirror) leave it; on any failure they all stay for the next run. */
async function retryBacklog(db, FV) {
  // (the oldest RETRY_MAX a run: the mirror's run stays as short as it was; the rest wait their turn, kept)
  const ref = backlogRef(db), ids = idsOfDoc(await ref.get()).slice(0, RETRY_MAX);
  if (!ids.length) return null;
  const recs = [];
  for (let i = 0; i < ids.length; i += 100) {
    for (const d of await db.getAll(...ids.slice(i, i + 100).map(id => db.collection(RECEIPTS).doc(id)), { fieldMask: SWEEP_FIELDS })) {
      if (!d.exists) continue;
      const x = Object.assign({}, d.data()); if (!x.receipt_id) x.receipt_id = d.id;
      if (isCancelled(x)) { const r = fromReceipt(x); if (r.orderId) recs.push(r); }
    }
  }
  const out = recs.length ? await putMany(db, FV, recs, { prefix: "", detectedBy: "mirror" }) : { candidates: 0, created: 0, noted: 0, unchanged: 0, restored: 0, failed: 0, ids: [] };
  out.retried = ids.length;
  if (!out.failed) {
    const done = new Set(ids);
    await db.runTransaction(async t => {
      const left = idsOfDoc(await t.get(ref)).filter(id => !done.has(id));
      if (left.length) t.set(ref, { ids: left, at: Date.now() }, { merge: true }); else t.delete(ref);
    });
  }
  return out;
}
/** What became of a cancelled order's pieces, sheet by sheet (the sorter's AutoCancel as it takes them off or finds them
    cut, and a cancel made in its sheet window): fates [{ sheet: "GF Sheet 2", fate: "removed" | "cut" | "open", text }], merged by
    sheet into the record there is. Never makes a record: an order restored meanwhile stays restored. opts: prefix, and
    removals / by (noteRemovals): each fate is also a removal of its own (sheet~<sheet>), with its time. */
// "open": still on a saved sheet not cut yet that the sorter has not loaded (its pieces are to come off before cutting)
const fateOf = f => ({ sheet: s(f && f.sheet, 80), fate: f && (f.fate === "cut" || f.fate === "open") ? f.fate : "removed", text: s(f && f.text, 160) });
const FATE_OUTCOME = { removed: "removed", cut: "setAside", open: "waiting" };
async function noteFates(db, orderId, fates, opts = {}) {
  const id = idOf(orderId); if (!id) return { error: "orderId required" };
  const inc = (Array.isArray(fates) ? fates : []).map(fateOf).filter(f => f.sheet).slice(0, 30), now = Date.now();
  const rem = inc.map(f => removalOf({ id: `sheet~${f.sheet}`, where: f.sheet, kind: "sheet", outcome: FATE_OUTCOME[f.fate], text: f.text, by: opts.by }, now))
    .concat((Array.isArray(opts.removals) ? opts.removals : []).slice(0, 60).map(r => removalOf(r, now))).filter(Boolean);
  if (!inc.length && !rem.length) return { ok: true, changed: false };
  const ref = colOf(db, opts.prefix).doc(id);
  return db.runTransaction(async t => {
    const snap = await t.get(ref); if (!snap.exists) return { ok: true, changed: false, missing: true };
    const d = snap.data(), cur = Array.isArray(d.fates) ? d.fates : [], bySheet = new Map(cur.map(f => [f.sheet, f]));
    for (const f of inc) bySheet.set(f.sheet, f);
    const next = [...bySheet.values()].slice(0, 30), m = mergeRemovals(d.removals, rem), patch = {};
    if (JSON.stringify(next) !== JSON.stringify(cur)) patch.fates = next;
    if (m.changed) patch.removals = m.list;
    if (m.left) patch.removalsLeftOut = n(d.removalsLeftOut) + m.left;
    if (!Object.keys(patch).length) return { ok: true, changed: false, fates: cur, removals: m.list };
    t.update(ref, patch);
    return { ok: true, changed: true, fates: patch.fates || cur, removals: m.list };
  });
}
/* ── what the cancel took off, and from where (Paul, 29 Sep: "show that it was removed successfully from a given sheet or
   process ... all this must be saved"): the record's own list, one entry per removal under a stable id, so a retry or a
   re-detection lands on the same one:
     removals [{ id, at, where ("GF Sheet 1", "the SS pool", "the queue"), kind: sheet | pool | queue | station | other,
                 outcome: removed | setAside | waiting | failed | seen, by, text, lineKey, station, was? }]
   Entries are only added or updated: a waiting or failed one turns removed or set aside when that happens (its earlier
   state kept as `was`), never back. Nothing is pruned; past REMOVALS_MAX (far more than any order has pieces) a new one is
   not added and removalsLeftOut counts it. ── */
const REMOVALS_MAX = 300, OUTCOMES = new Set(["removed", "setAside", "waiting", "failed", "seen"]), FINAL = new Set(["removed", "setAside"]);
const KINDS = new Set(["sheet", "pool", "queue", "station", "other"]);
function removalOf(x, now) {
  if (!x || typeof x !== "object") return null;
  const where = s(x.where || x.sheet, 100), kind = KINDS.has(x.kind) ? x.kind : "sheet";
  const id = s(x.id, 120).replace(/[^\w.:~ ()-]/g, "_") || (where ? `${kind}~${where}` : ""); if (!id) return null;
  return { id, at: n(x.at) || now || Date.now(), where, kind, outcome: OUTCOMES.has(x.outcome) ? x.outcome : "removed", by: s(x.by, 80), text: s(x.text, 160), lineKey: s(x.lineKey, 120), station: s(x.station, 40) };
}
function mergeRemovals(cur, inc) {
  const list = (Array.isArray(cur) ? cur : []).filter(r => r && typeof r === "object").slice(), idx = new Map(list.map((r, i) => [r.id, i]));
  let changed = false, left = 0;
  for (const r of inc || []) {
    const i = idx.get(r.id);
    if (i == null) { if (list.length >= REMOVALS_MAX) { left++; continue; } idx.set(r.id, list.length); list.push(r); changed = true; continue; }
    const o = list[i], nx = Object.assign({}, o);
    for (const k of ["where", "kind", "by", "text", "lineKey", "station"]) if (r[k] && r[k] !== o[k]) nx[k] = r[k];
    if (r.outcome !== o.outcome && !(FINAL.has(o.outcome) && !FINAL.has(r.outcome))) { nx.outcome = r.outcome; nx.at = r.at; nx.was = { outcome: s(o.outcome, 20), at: n(o.at) }; }
    if (JSON.stringify(nx) !== JSON.stringify(o)) { list[i] = nx; changed = true; }
  }
  return { list, changed, left };
}
/** Removals of a cancelled order onto its record (never makes one: a restored order stays restored). opts: prefix · by. */
async function noteRemovals(db, orderId, removals, opts = {}) { return noteFates(db, orderId, [], Object.assign({}, opts, { removals })); }
module.exports = { COL, RECEIPTS, ETSY_WHY, SWEEP_STATUSES, BACKLOG, REMOVALS_MAX, isCancelled, record, fromReceipt, plan, put, putMany, fromReceipts, sweep, noteFates, noteRemovals, removalOf, mergeRemovals, cancelledIds, keepBacklog, retryBacklog };
