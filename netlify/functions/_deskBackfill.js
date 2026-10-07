/*  netlify/functions/_deskBackfill.js
 *  The desk numbers of the days BEFORE desks were told apart (Paul, 7 Oct 2026: "the totals for assembly and shipping do not match").
 *
 *  Round 3 (deploy of 16:47 UTC on 7 Oct) made the rollup keep a counter per desk (Efficiency_Daily `devices`: "assembly-2": { events, scans, completes, parts, orders, ... }) and
 *  name the desk on each touched order. Everything recorded earlier has only the kind counters, so the console showed a plain "Assembly" / "Shipping" row next to the desk rows.
 *  The events themselves (Station_Activity) always carried the desk in `device` ("assembly-2"), so this file recovers it, ONCE per person-day, from them:
 *
 *    op deskBackfill { day }        (employeeEfficiency, behind the same manager passcode as every other op; the console asks for it, never a timer)
 *
 *  What it does, for the one New York day it is given (a day newer than 31 days ago, never today+1, never the sandbox):
 *    1  reads that day's rollups (a field mask: the small counters, not `touched`) and keeps the person-days that still lack the marker `deskBackfilled` AND count more events at a
 *       numbered station (assembly, shipping) than their desks account for (pending() below: the same test the overview uses to tell the console a day needs it);
 *    2  per person-day (batches: at most BUDGET events and MAX_DOCS person-days a call, resumable: the next call finds what is left), reads that person's events of the day with the
 *       indexed query the Employee page already uses (person == and day ==) and a field mask (only the fields below), keeps the numbered stations' events;
 *    3  recomputes the desk counters in memory with the writer's own function (_stationActivity.rollupPatch, so "exactly as the live transaction would", no second copy of the rules);
 *    4  writes ONE update per person-day in a transaction that first re-reads the rollup: the write happens only when the rollup's event count is still the one the events were read
 *       against (a live write in between makes it re-read, twice at most, then the person-day is left for the next call: never a double count, never a lost live event). Each desk
 *       field is merged by MAX with what the live path already counted, the touched orders learn their desk, and the marker `deskBackfilled: true` goes in the same write.
 *  The live rollup transaction is unchanged except that a rollup it CREATES is born with the marker (all of its events are desk-counted: nothing to recover). Running the op twice
 *  changes nothing (the second finds the marker). Events that carry no desk ("assembly", an empty device) stay in the plain kind row, which the console labels "desk not recorded".
 *  Cost: one read per event of a pending person-day, once, then zero; a few small rollup reads per call; one write per person-day, once. Nothing is deleted or rewritten except those rollups. */
"use strict";
const SA = require("./_stationActivity");
const KIND = require("./_activityKinds");
const Rev = require("./_employeeRev");

const MAX_DAYS = 31;                 // a day older than this (New York) is refused
const BUDGET = 300;                  // numbered-station events a call may read (counted from the rollups BEFORE reading anything); one person-day may exceed it alone
const MAX_DOCS = 8;                  // person-days per call
const MAX_DOC_EVENTS = 1500;         // a person-day with more numbered-station events than this is left alone (marked, plain row stays)
const MAX_SCAN = 3000;               // events of one person-day a query may return
const TRIES = 2;                     // times a person-day is re-read after a live write moved it, before it is left for the next call
const NUMBERED = Object.keys(KIND.NUMBERED || {});
const ROLL_FIELDS = ["day", "person", "events", "deskBackfilled", "stations", "devices", "sandbox"];
const EV_FIELDS = ["person", "day", "station", "device", "action", "parts", "orders", "orderId", "at", "hour", "sandbox"];
const NOINC = { increment: n => n, serverTimestamp: () => null };   // (rollupPatch with plain numbers instead of increments: the counters it would add, as they are)
const ACTIONS = ["scans", "completes", "prints", "rejects", "errors", "undos", "notes"];   // every event bumps exactly one of these on its station (_stationActivity.tally)

const num = v => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
const isObj = v => !!v && typeof v === "object" && !Array.isArray(v);
/** Events a rollup counted at one station: every event adds one to exactly one of the action counters. */
const eventsAt = st => (isObj(st) ? ACTIONS.reduce((n, k) => n + Math.max(0, num(st[k])), 0) : 0);
/** Events a rollup counted at the numbered stations (assembly + shipping). */
const numberedEvents = doc => NUMBERED.reduce((n, k) => n + eventsAt(doc && isObj(doc.stations) ? doc.stations[k] : null), 0);
/** Events its desks account for (the live path's `devices`). */
function deskEvents(doc, kind) {
  let n = 0;
  if (doc && isObj(doc.devices)) for (const [k, v] of Object.entries(doc.devices)) if (KIND.deviceNo(kind, k) === k && isObj(v)) n += Math.max(0, num(v.events));
  return n;
}
/** Events counted for a kind but for none of its desks: what the console shows as the plain row. */
const gapOf = doc => NUMBERED.reduce((n, k) => n + Math.max(0, eventsAt(doc && isObj(doc.stations) ? doc.stations[k] : null) - deskEvents(doc, k)), 0);
/** A person-day that still needs the backfill: no marker and more events at a numbered station than its desks hold. (Pure: the overview calls it on rollups it already read.) */
const pending = doc => !!doc && isObj(doc) && doc.deskBackfilled !== true && gapOf(doc) > 0;

/** The memory this instance kept of the day's numbers is dropped, so the very next read shows the desk rows (other instances follow within their own short lifetimes). */
function forget(ctx, day) {
  const memo = ctx.cache && ctx.cache.memo; if (!memo) return;
  for (const k of [...memo.keys()]) if (k.startsWith("ov|") || k.startsWith("live|") || k.startsWith("ltoday|") || (k.startsWith("roll|") && k.endsWith("|" + day))) memo.delete(k);
}

/** The desk counters and the desk of each touched order, from the numbered stations' events of ONE person-day (same function as the live rollup). */
function compute(rows, day, person) {
  const evs = rows.slice().sort((a, b) => a.at - b.at || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const out = SA.rollupPatch(NOINC, null, day, person, evs, "");
  const touched = {};
  for (const [oid, m] of Object.entries(out.touched || {})) for (const [st, dk] of Object.entries(m)) if (typeof dk === "string") (touched[oid] || (touched[oid] = {}))[st] = dk;
  return { devices: out.devices || {}, touched };
}

/** The write for one person-day, given the rollup as the transaction read it: each desk field by MAX, touched orders that exist there get their desk, and the marker. */
function patchFor(cur, calc) {
  const patch = { deskBackfilled: true };
  const devices = {};
  for (const [dk, v] of Object.entries(calc.devices)) {
    const had = cur.devices && isObj(cur.devices[dk]) ? cur.devices[dk] : {}, o = {};
    for (const [f, n] of Object.entries(v)) o[f] = Math.max(num(had[f]), num(n));
    devices[dk] = o;
  }
  if (Object.keys(devices).length) patch.devices = devices;
  const touched = {};
  for (const [oid, m] of Object.entries(calc.touched)) {
    const have = cur.touched && isObj(cur.touched[oid]) ? cur.touched[oid] : null; if (!have) continue;           // (an order the rollup does not hold, past its cap, stays out)
    for (const [st, dk] of Object.entries(m)) if (have[st] != null && have[st] !== dk) (touched[oid] || (touched[oid] = {}))[st] = dk;
  }
  if (Object.keys(touched).length) patch.touched = touched;
  return patch;
}

async function readEvents(ctx, person, day) {
  let q = ctx.db.collection(SA.ACT).where("person", "==", person).where("day", "==", day);
  if (typeof q.select === "function") q = q.select(...EV_FIELDS);
  const snap = await q.limit(MAX_SCAN + 1).get();
  const rows = [];
  for (const d of snap.docs.slice(0, MAX_SCAN)) {
    const v = d.data() || {};
    if (v.sandbox === true || v.day !== day || !NUMBERED.includes(v.station) || SA.rollupId(day, v.person) !== SA.rollupId(day, person)) continue;
    rows.push({ id: d.id, person: v.person, station: v.station, device: v.device, action: v.action, parts: Math.max(0, num(v.parts)), orders: num(v.orders) >= 1 ? 1 : 0, orderId: typeof v.orderId === "string" ? v.orderId : "", at: num(v.at), hour: typeof v.hour === "string" ? v.hour : "" });
  }
  return { rows, read: snap.docs.length, capped: snap.docs.length > MAX_SCAN };
}

/** Marks a person-day done without touching its counters (nothing to recover from its events, or too many to read): the console never asks for it again. */
async function markOnly(ref, reason) {
  try { await ref.update({ deskBackfilled: true, deskSkipped: String(reason).slice(0, 60) }); return true; } catch (_) { return false; }
}

/** One person-day. Returns { status: written | already | skipped | busy | gone, read }. */
async function one(ctx, x, day) {
  const ref = ctx.db.collection(SA.DAILY).doc(x.id);
  let cur = x.v, read = 0;
  for (let t = 0; t < TRIES; t++) {
    if (t > 0) { const s = await ref.get(); cur = s && s.exists ? s.data() || {} : null; }
    if (!cur) return { status: "gone", read };
    if (!pending(cur)) return { status: "already", read };
    const expected = numberedEvents(cur);
    if (expected > MAX_DOC_EVENTS) return { status: (await markOnly(ref, "too many events")) ? "skipped" : "busy", read, reason: "too many events" };
    const ev = await readEvents(ctx, String(cur.person || ""), day); read += ev.read;
    if (ev.capped) return { status: (await markOnly(ref, "too many events")) ? "skipped" : "busy", read, reason: "too many events" };
    const calc = compute(ev.rows, day, String(cur.person || "")), want = num(cur.events);
    const res = await ctx.db.runTransaction(async tx => {
      const s = await tx.get(ref);
      if (!s || !s.exists) return { status: "gone" };
      const now = s.data() || {};
      if (now.deskBackfilled === true) return { status: "already" };
      if (num(now.events) !== want) return { status: "moved" };                    // a live event was counted since this person-day was last read: read again (also what makes the check below safe)
      if (ev.rows.length > numberedEvents(now)) {                                  // more events than the rollup counted: they cannot be lined up with its counters, so nothing is invented
        tx.set(ref, { deskBackfilled: true, deskSkipped: "more events than counted" }, { merge: true });
        return { status: "skipped", reason: "more events than counted" };
      }
      tx.set(ref, patchFor(now, calc), { merge: true });
      const placed = ev.rows.filter(r => KIND.deviceNo(r.station, r.device)).length;
      return { status: "written", recovered: placed, left: ev.rows.length - placed };
    });
    if (res.status !== "moved") return Object.assign({ read }, res);
  }
  return { status: "busy", read };
}

/** op deskBackfill { day }. See the top of this file. */
async function op(ctx, body, H) {
  const day = typeof body.day === "string" ? body.day : "";
  if (!H.validDay(day)) return H.json(400, { ok: false, error: "day must be YYYY-MM-DD" });
  if (ctx.prefix) return H.json(400, { ok: false, error: "the sandbox has no earlier records to recover" });
  if (day > ctx.today) return H.json(400, { ok: false, error: "that day has not happened yet" });
  if (day < H.addDays(ctx.today, -MAX_DAYS)) return H.json(400, { ok: false, error: `days older than ${MAX_DAYS} days are not recovered` });
  let q = ctx.db.collection(SA.DAILY).where("day", "==", day);
  if (typeof q.select === "function") q = q.select(...ROLL_FIELDS);
  const snap = await q.limit(501).get();
  const all = [];
  for (const d of snap.docs.slice(0, 500)) { const v = d.data() || {}; if (v.sandbox !== true && typeof v.person === "string" && d.id === SA.rollupId(day, v.person)) all.push({ id: d.id, v }); }
  const list = all.filter(x => pending(x.v)).sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const out = { ok: true, day, pending: list.length, processed: 0, written: 0, already: 0, skipped: 0, busy: 0, eventsRead: 0, recovered: 0, left: 0, more: false, done: false };
  let spent = 0;
  for (const x of list) {
    const want = numberedEvents(x.v);
    if (out.processed > 0 && (out.processed >= MAX_DOCS || spent + want > BUDGET)) { out.more = true; break; }   // (the rest is for the next call)
    spent += want; out.processed++;
    let r; try { r = await one(ctx, x, day); } catch (e) { r = { status: "busy", read: 0 }; console.warn("[deskBackfill] a person-day failed: " + String((e && (e.message || e.code)) || e).slice(0, 120)); }
    out.eventsRead += r.read || 0; out.recovered += r.recovered || 0; out.left += r.left || 0;
    if (r.status === "written") out.written++; else if (r.status === "skipped") out.skipped++; else if (r.status === "busy") out.busy++; else out.already++;
  }
  out.done = !out.more && out.busy === 0;
  if (out.written || out.skipped) {
    forget(ctx, day);
    await Rev.afterWrite(ctx.db, ctx.prefix, ["act"], ctx.admin && ctx.admin.firestore && ctx.admin.firestore.FieldValue);                                     // (a counted change: the console's other readers take the new numbers at their next look; production only, never throws)
  }
  return H.json(200, out);
}

module.exports = { MAX_DAYS, BUDGET, MAX_DOCS, MAX_DOC_EVENTS, eventsAt, numberedEvents, deskEvents, gapOf, pending, compute, patchFor, op };
