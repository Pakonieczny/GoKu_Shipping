/*  netlify/functions/_activityKinds.js
 *  What one activity event says about its person's work (Paul, 5 Oct 2026: "number of issues attributed to that employee ...
 *  their contact success and failure rates"). ONE pure function reads the event the station page wrote (station, action and
 *  the short fixed-phrase `detail`), and says which issue kind it is, and which small counters it adds to the day's rollup.
 *
 *  It is used twice, so the two can never disagree:
 *    - _stationActivity.js (the write side) bumps the counters x_* inside the rollup's own transaction, so the Employee page can
 *      count a kind over a whole year without reading a single event;
 *    - _employeeIssues.js (the read side) classifies the newest events the same way, for the list of items, and for the days
 *      whose rollup has no counters yet (days before this existed).
 *  Pure: no Firestore, no clock, no network. It cannot throw (an unexpected event is "no kind"). The text of each rule is the
 *  phrase the station pages really write: plans/employee-efficiency/station-*.md, and E3's inbox phrases in plans/employee-hr/api.md.
 *
 *  Nothing here says who is at fault. A kind is a signal about the work or the order, and the reader's definition of each says
 *  what it is NOT. */
"use strict";

/* ── ONE Sorting station (Paul, 6 Oct 2026: "both QR Printer and the Sort are part of that same station") ──
 *  The Sorter app (charm-nest-1, station key "sorter") and the QR Printer page (station key "qr") are devices of the one Sorting
 *  station. What is already stored under those keys stays exactly as it is (events, sessions, rollups, seals are permanent history:
 *  never rewritten); the fold happens only when the portal READS a station key, through displayStation(key). New writes may keep
 *  the old keys. Only the KEYS "sorter" and "qr" fold: a Sorter-app session of a Laser or Design person is written under the station
 *  key "laser" or "design" (device charm-nest-1) and so never folds into Sorting. The client mirror is EfficiencyStations.displayStation
 *  (charm-nest-efficiency-stations.js); the two lists must agree (tests/stations/sorting-fold.cjs checks it). */
const STATION_FOLD = Object.freeze({ sorter: "sorting", qr: "sorting" });
/** The key the portal shows for a stored station key: "sorter" and "qr" are "sorting"; every other key is returned as it is ("" for no key). */
function displayStation(key) {
  if (typeof key !== "string") return key == null ? "" : displayStation(String(key));
  return Object.prototype.hasOwnProperty.call(STATION_FOLD, key) ? STATION_FOLD[key] : key;
}
/** Every stored station key that is shown as this display key, in catalog order: storedStations("sorting") is ["sorting", "sorter", "qr"]. */
function storedStations(display) {
  const d = displayStation(display), out = [d];
  for (const k of Object.keys(STATION_FOLD)) if (STATION_FOLD[k] === d) out.push(k);
  return out;
}

/* ── The Welding station (Paul, 6 Oct 2026: "We do not track order throughput/completion using this station ... it does use a dedicated QR code scanner
 *  app when the user is matching the welded stud earrings to the orders") ──
 *  Two tasks: "welding" (welding the studs to the charm: time on task, no scans) and "matching" (matching the welded earrings to their orders with the
 *  scanner app and adding the backings: every scan of an order QR is a `matched` event). A session and an activity event may carry `task`. The station is
 *  NOT counted in order throughput or completion anywhere the portal reads: its pieces, orders and completions are left out of every total, rate, ranking and
 *  chart (readStationCounters below is the one place that does it); it shows time on task per task and the matched count instead. What is stored is never
 *  rewritten (the old weld scans and completions stay as written), and the `welded` seal on the order timeline is not touched. */
const TASKS = Object.freeze(["welding", "matching"]);
const UNATTRIBUTED = "Unattributed";                          // the stored person of a scan made while nobody was signed in under Matching (never credited to a welder)
const NO_THROUGHPUT = new Set(["welding"]);
/** true when this station's pieces / orders / completions count in throughput (everything but the Welding station) */
const throughput = station => !NO_THROUGHPUT.has(station);
/** one valid task, or "" (a task belongs to the welding station only) */
const taskOf = (station, task) => (station === "welding" && TASKS.includes(task) ? task : "");
/** A matched scan: the `matched` action, or a plain scan that says task "matching" at the welding station. Counts as scanned work, never as a completion. */
const isMatched = ev => !!ev && (ev.action === "matched" || (ev.action === "scan" && ev.task === "matching" && ev.station === "welding"));
const COUNTERS = ["completes", "parts", "orders", "undoParts", "undoOrders"];
/** The counters of one station's rollup entry as a reader should SUM them: a station that is not counted in throughput keeps its scans, prints, time and matched
 *  count but loses its completions, pieces and orders (a copy: the stored rollup is never changed). */
function readStationCounters(station, v) {
  if (throughput(station) || !v || typeof v !== "object") return v;
  const o = Object.assign({}, v); for (const k of COUNTERS) o[k] = 0;
  return o;
}
/** The orders of a rollup's `touched` map without the ones only a not-counted station touched; a station set without it. */
const touchedStations = list => (Array.isArray(list) ? list : [...list]).filter(throughput);

/** issue kind -> the rollup counter that counts it (kinds that are plain subtractions of the old counters have none:
    `undone` = undos, `refused` = rejects not in a kind below, `failed` = errors that are not a lookup failure or a failed reply) */
const KIND_X = { cancelAlert: "x_cancel", heldOrSkipped: "x_held", unknownSku: "x_sku", qaFlag: "x_flag", lookupFailed: "x_lookup", reprint: "x_reprint", rescan: "x_rescan" };
/** the inbox counters (E3's phrases): replies drafted / sent / delivered / unconfirmed / failed / refused, edited AI drafts, first-reply minutes */
const INBOX_X = ["x_draft", "x_sent", "x_deliv", "x_unconf", "x_fail", "x_refuse", "x_edit", "x_aiok", "x_first", "x_firstMin", "x_wait", "x_waitMin"];
const X_KEYS = Object.values(KIND_X).concat(INBOX_X);

const detailOf = ev => String((ev && ev.detail) == null ? "" : ev.detail);
const minutes = tok => { const m = /^(?:first reply|waited) (\d{1,5})m$/.exec(tok); return m ? Math.min(99999, +m[1]) : null; };

/** The inbox event, read: { reply: drafted|sent|delivered|unconfirmed|failed|refused|"", edited, aiUnchanged, firstMin, waitMin }, or null (not an inbox reply event). */
function inboxOf(ev) {
  if (!ev || ev.station !== "inbox") return null;
  const d = detailOf(ev).trim(), a = ev.action;
  const o = { reply: "", edited: false, aiUnchanged: false, firstMin: null, waitMin: null };
  if (a === "note") {
    if (/^reply drafted/i.test(d)) o.reply = "drafted";
    else if (/^reply delivered/i.test(d)) o.reply = "delivered";
    else if (/^reply unconfirmed/i.test(d)) o.reply = "unconfirmed";
    else if (/^reply sent/i.test(d)) {
      o.reply = "sent";
      for (const t of d.split(" · ").slice(1).map(x => x.trim().toLowerCase())) {          // " · " between tokens
        if (t === "ai draft edited") o.edited = true;
        else if (t === "ai draft") o.aiUnchanged = true;
        else if (/^first reply /.test(t)) o.firstMin = minutes(t);
        else if (/^waited /.test(t)) o.waitMin = minutes(t);
      }
    }
  } else if (a === "error") {
    if (/^reply failed/i.test(d)) o.reply = "failed";
    else if (/^reply not sent/i.test(d)) o.reply = "refused";
  }
  return o.reply ? o : null;
}

/** The issue kind of one event, or "". Stateless on purpose: the rollup counters must come out the same whatever order a batch is
    written in (the writer's own rule), so a kind never depends on an earlier event (a plain second scan of an order is NOT a kind). */
function kindOf(ev) {
  if (!ev) return "";
  const st = ev.station, ac = ev.action, d = detailOf(ev);
  if (st === "inbox") {
    if (ac !== "error") return "";
    return /^reply (failed|not sent)/i.test(d) ? "replyFailed" : "failed";           // (the inbox's own Done / Reopen are counted as contact, not as issues)
  }
  switch (ac) {
    case "undo": return "undone";
    case "reject":
      if (/cancel/i.test(d)) return "cancelAlert";                                  // "cancelled order alert", "sticker blocked: cancelled order", "cancelled order: do not sort" ...
      if (/^(held|skipped):\s*unmatchedSku\b/i.test(d)) return "unknownSku";        // sorter Review: an Unknown SKU card held or skipped
      if (/^(held|skipped):/i.test(d) || /sent back/i.test(d)) return "heldOrSkipped";
      if (/^flag to team/i.test(d)) return "qaFlag";                                // assembly: "flag to Team: rework" ...
      return "refused";
    case "error": return /lookup failed|not found or not loaded/i.test(d) ? "lookupFailed" : "failed";
    case "print": return /\b(again|reprint)\b/i.test(d) ? "reprint" : "";           // shipping "reprint", sorting / sorter ", again"
    case "scan": case "matched": return /\bagain\b/i.test(d) ? "rescan" : "";    // weld " · again", a matched scan " · again"
    case "note": return /^stamp again/i.test(d) ? "rescan" : "";                    // assembly "stamp again: Done"
    default: return "";
  }
}

/** { kind, x }: the issue kind ("" for none) and the rollup counters this event adds ({ x_cancel: 1 } ...). Never throws. */
function classify(ev) {
  const out = { kind: "", x: {} };
  try {
    out.kind = kindOf(ev);
    if (KIND_X[out.kind]) out.x[KIND_X[out.kind]] = 1;
    const ib = inboxOf(ev);
    if (ib) {
      const r = ib.reply;
      if (r === "drafted") out.x.x_draft = 1;
      else if (r === "sent") {
        out.x.x_sent = 1;
        if (ib.edited) out.x.x_edit = 1;
        if (ib.aiUnchanged) out.x.x_aiok = 1;
        if (ib.firstMin != null) { out.x.x_first = 1; if (ib.firstMin) out.x.x_firstMin = ib.firstMin; }
        if (ib.waitMin != null) { out.x.x_wait = 1; if (ib.waitMin) out.x.x_waitMin = ib.waitMin; }
      } else if (r === "delivered") out.x.x_deliv = 1;
      else if (r === "unconfirmed") out.x.x_unconf = 1;
      else if (r === "failed") out.x.x_fail = 1;
      else if (r === "refused") out.x.x_refuse = 1;
    }
  } catch (_) { return { kind: "", x: {} }; }
  return out;
}

module.exports = { KIND_X, INBOX_X, X_KEYS, STATION_FOLD, displayStation, storedStations, TASKS, UNATTRIBUTED, NO_THROUGHPUT, throughput, taskOf, isMatched, readStationCounters, touchedStations, kindOf, inboxOf, classify };
