/*  netlify/functions/_stationWork.js
 *  Complete Order at the production stations (Paul, 9 Nov 2026: "This also needs to be linked to the employee efficiency tab so that I can clearly track
 *  what the employees did when they completed orders"). The stations (station-work.js) write:
 *    scan                                    an order opened at the station (a look: never a completion)
 *    complete  detail "Complete Order …"     the Complete Order press; workMs = the time from the order being opened at the station to the press
 *  ONE pure reading of them, used twice so the two cannot disagree:
 *    patch(prev, events)  the write side (_stationActivity.js): what a batch of one person's events of one day adds to the rollup's `work` map
 *                         { [station]: { done, parts, orders, timed, ms (increments), v: { orderId: 1 } opened, w: { orderId: 1 } completed, dur: { key: seconds } } },
 *                         the maps capped per station and day
 *    merge(acc, work) / rows(acc)   the read side (employeeEfficiency.js, _employeeProfile.js): the rollups of a range added up, then per station
 *                         { station, completed, pieces, orders, timed, avgMin, medianMin, viewed, viewedOnly }   (viewedOnly: opened, never completed)
 *  Pure: no Firestore, no clock, no network; never throws (a malformed value is skipped). Only Welding, Assembly and Shipping are read. */
"use strict";
const STATIONS = Object.freeze(["welding", "assembly", "shipping"]);
const ON = new Set(STATIONS);
const CAP = Object.freeze({ v: 400, w: 400, dur: 400 });
const COUNTERS = Object.freeze(["done", "parts", "orders", "timed", "ms"]);

const str = v => String(v == null ? "" : v);
const num = v => { const x = Number(v); return Number.isFinite(x) ? x : 0; };
const oidOf = v => str(v).replace(/\D/g, "").slice(0, 30);
/** the Complete Order press (never an older completion: a Team stamp, a scan that welded, an Etsy completion before this existed) */
const isButton = ev => !!ev && ev.action === "complete" && /^Complete Order\b/.test(str(ev.detail).trim());
const sizeOf = o => (o && typeof o === "object" ? Object.keys(o).length : 0);
const blank = () => ({ done: 0, parts: 0, orders: 0, timed: 0, ms: 0, v: {}, w: {}, dur: {} });

/** What these events (one person, one day; any order) add to the rollup's `work` map, plain numbers (the caller turns the counters into increments). {} when nothing. */
function patch(prev, events) {
  const out = {};
  try {
    const old = prev && typeof prev === "object" ? prev : {};
    for (const ev of Array.isArray(events) ? events : []) {
      if (!ev || !ON.has(ev.station)) continue;
      const st = ev.station, oid = oidOf(ev.orderId), was = old[st] && typeof old[st] === "object" ? old[st] : {};
      const o = () => out[st] || (out[st] = blank());
      const add = (k, cap) => { const x = o(); if (oid && !x[k][oid] && !(was[k] && was[k][oid]) && sizeOf(was[k]) + sizeOf(x[k]) < cap) x[k][oid] = 1; };
      if (ev.action === "scan") add("v", CAP.v);
      else if (isButton(ev)) {
        const x = o(); x.done++; x.parts += Math.max(0, Math.floor(num(ev.parts))); x.orders += num(ev.orders) >= 1 ? 1 : 0;
        add("w", CAP.w);
        if (ev.workMs != null && Number.isFinite(Number(ev.workMs)) && Number(ev.workMs) >= 0) {
          const msv = Math.min(86400000, Math.round(Number(ev.workMs)));
          x.timed++; x.ms += msv;
          if (sizeOf(was.dur) + sizeOf(x.dur) < CAP.dur) x.dur[`${oid || "x"}_${Math.max(0, Math.round(num(ev.at))).toString(36)}`] = Math.round(msv / 1000);
        }
      }
    }
    for (const k of Object.keys(out)) { const x = out[k]; for (const m of ["v", "w", "dur"]) if (!sizeOf(x[m])) delete x[m]; for (const c of COUNTERS) if (!x[c]) delete x[c]; if (!sizeOf(x)) delete out[k]; }
  } catch (_) { return {}; }
  return out;
}

const accSt = () => ({ done: 0, parts: 0, orders: 0, timed: 0, ms: 0, v: new Set(), w: new Set(), dur: [] });
/** adds one rollup's stored `work` map to the accumulator */
function merge(a, work) {
  a = a || {};
  try {
    if (!work || typeof work !== "object") return a;
    for (const st of STATIONS) {
      const w = work[st]; if (!w || typeof w !== "object") continue;
      const x = a[st] || (a[st] = accSt());
      for (const c of COUNTERS) x[c] += Math.max(0, num(w[c]));
      for (const k of ["v", "w"]) for (const id of Object.keys(w[k] && typeof w[k] === "object" ? w[k] : {})) { const o = oidOf(id); if (o) x[k].add(o); }
      for (const v of Object.values(w.dur && typeof w.dur === "object" ? w.dur : {})) { const s = num(v); if (s >= 0 && x.dur.length < 20000) x.dur.push(s); }
    }
  } catch (_) {}
  return a;
}
/** several accumulators added up (a person's days, or everybody: the business line) */
function mergeAcc(into, a) {
  into = into || {};
  try {
    for (const st of Object.keys(a || {})) {
      const x = a[st], y = into[st] || (into[st] = accSt());
      for (const c of COUNTERS) y[c] += x[c];
      x.v.forEach(o => y.v.add(o)); x.w.forEach(o => y.w.add(o));
      for (const d of x.dur) if (y.dur.length < 20000) y.dur.push(d);
    }
  } catch (_) {}
  return into;
}
const median = list => { if (!list.length) return null; const s = list.slice().sort((p, q) => p - q), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const r1 = x => Math.round(x * 10) / 10;
/** the accumulator, read: one row per station with anything, in station order; [] when nothing */
function rows(a) {
  const out = [];
  try {
    for (const st of STATIONS) {
      const x = a && a[st]; if (!x) continue;
      let viewedOnly = 0; for (const o of x.v) if (!x.w.has(o)) viewedOnly++;
      const med = median(x.dur);
      const row = { station: st, completed: x.done, pieces: x.parts, orders: x.orders, timed: x.timed,
        avgMin: x.timed > 0 ? r1(x.ms / x.timed / 60000) : null, medianMin: med == null ? null : r1(med / 60), viewed: x.v.size, viewedOnly };
      if (row.completed || row.viewed) out.push(row);
    }
  } catch (_) {}
  return out;
}

module.exports = { STATIONS, CAP, COUNTERS, isButton, patch, acc: () => ({}), merge, mergeAcc, rows, median };
