"use strict";
/* How long should a recommended campaign run?  Pure planning, no network, no clock.

   buildSchedule() answers, for one recommendation: when to start, when to stop, how long Google needs to learn
   before the results mean anything, and, for a campaign tied to a date (Christmas, Black Friday), whether there is
   still enough time left to test it. It returns the `schedule` object described in the ADDENDUM of
   plans/pmax-research-contract-2026-09-29.md (schema 1). Every date is a YYYY-MM-DD string in the ACCOUNT's time zone;
   `today` is passed in (never read here), and all date arithmetic is whole days on the calendar, so the server's
   time zone and daylight-saving changes cannot move a date.

   Learning periods. The day counts in LEARNING are typical planning guidance, rounded to whole weeks. They are not
   limits set by Google and not a promise: Google's own pages say the real length depends on how many conversions
   the campaign gets and how long a buyer takes to convert, and that a campaign on Manual CPC has no learning period
   of its own. Pages read on 2026-09-29:
     https://support.google.com/google-ads/answer/11385582  Performance Max: "Run new campaigns for at least 6 weeks";
                                                            1 to 2 weeks to settle after a significant change
     https://support.google.com/google-ads/answer/13020501  what sets the length of a learning period; Manual CPC has none
     https://support.google.com/google-ads/answer/10970825  wait at least one conversion cycle before judging
     https://support.google.com/google-ads/answer/16797388  Demand Gen: judge on 30 to 50 conversions, not a calendar date
   The 42 days for Performance Max is the same figure as `evaluationDays` in brites-campaign-styles.js, and the 28-day
   Search test, the 21 days a reliable read needs and the order cutoff (end = event date less the cutoff) are the
   ones planCampaign() in googleAdsAutopilot.js already uses.

   Where this differs from planCampaign(): a dated Search plan there starts 17 days before the last order date, which
   leaves only a few selling days once a Smart Bidding campaign has had its 2 weeks to learn. buildSchedule() starts
   early enough for the learning phase to finish and still leave a stretch of selling; pass its start and end on to
   planCampaign() (startDate / endDate) when the two must agree. */

const SCHEDULE_VERSION = 1;
const MS_DAY = 86400000;
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/* Per campaign kind:
     days        the early learning phase, drawn as "learning" on the bar (Google is still tuning delivery)
     judgeAfter  days of data before a fair verdict (planning guidance; equal to `days` unless noted)
     sellDays    the comfortable stretch of selling to plan for after learning when there is room
     defaultDays the length of an undated (evergreen) test
     wait / judgeWait / noun    words for the plain-language lines
     note        one plain sentence, at most 160 characters, shown next to the learning phase */
const LEARNING = Object.freeze({
  search: Object.freeze({
    days: 14, judgeAfter: 14, sellDays: 21, defaultDays: 28, wait: "2 weeks", judgeWait: "2 weeks", noun: "a Smart Bidding Search campaign",
    note: "Smart Bidding usually needs about 2 weeks of clicks and sales before its bids settle, so early results can look unstable."
  }),
  search_manual: Object.freeze({
    days: 7, judgeAfter: 7, sellDays: 21, defaultDays: 28, wait: "1 week", judgeWait: "1 week", noun: "a Search campaign with manual bids",
    note: "With bids you set yourself there is no bidding model to train, so about a week of clicks is enough for a first read."
  }),
  pmax: Object.freeze({
    days: 21, judgeAfter: 42, sellDays: 21, defaultDays: 42, wait: "3 weeks", judgeWait: "6 weeks", noun: "a Performance Max campaign",
    note: "Performance Max usually spends its first 2 to 3 weeks learning who buys, and Google suggests about 6 weeks before judging it."
  }),
  display: Object.freeze({
    days: 14, judgeAfter: 21, sellDays: 14, defaultDays: 21, wait: "2 weeks", judgeWait: "3 weeks", noun: "a Display campaign",
    note: "Display campaigns typically settle in about 2 weeks and give a fair read after about 3 weeks."
  }),
  demand_gen: Object.freeze({
    days: 14, judgeAfter: 21, sellDays: 14, defaultDays: 28, wait: "2 weeks", judgeWait: "3 weeks", noun: "a Demand Gen campaign",
    note: "Demand Gen typically needs 2 to 3 weeks to settle, and Google suggests waiting for 30 to 50 sales before a firm read."
  }),
  video: Object.freeze({
    days: 14, judgeAfter: 14, sellDays: 14, defaultDays: 21, wait: "2 weeks", judgeWait: "2 weeks", noun: "a Video campaign",
    note: "Video campaigns typically need about 2 weeks for delivery to settle before the results say much."
  })
});

const KINDS = Object.freeze(["search", "pmax", "display", "demand_gen", "video"]);
const KIND_ALIASES = { performance_max: "pmax", responsive_display: "display", fixed_display: "display", demandgen: "demand_gen", "demand gen": "demand_gen", "demand-gen": "demand_gen" };

// ---- calendar arithmetic: a date is a whole number of days since 1970-01-01, never a time ----
function dayNumber(ymd) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(ymd == null ? "" : ymd).trim());
  if (!m) return null;
  const y = +m[1], mo = +m[2], d = +m[3];
  if (y < 1970 || y > 2200) return null;
  const t = Date.UTC(y, mo - 1, d), dt = new Date(t);
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === mo - 1 && dt.getUTCDate() === d ? t / MS_DAY : null;
}
function ymdOf(n) {
  const d = new Date(n * MS_DAY);
  return d.getUTCFullYear() + "-" + String(d.getUTCMonth() + 1).padStart(2, "0") + "-" + String(d.getUTCDate()).padStart(2, "0");
}
function shortDate(n) { const d = new Date(n * MS_DAY); return MONTHS[d.getUTCMonth()] + " " + d.getUTCDate(); }
function shortDateWithYear(n) { return shortDate(n) + ", " + new Date(n * MS_DAY).getUTCFullYear(); }

/* "Nov 6 to Dec 4". The year appears only when the two dates are so far apart that the month alone would be ambiguous. */
function formatRange(startYmd, endYmd) {
  const a = dayNumber(startYmd), b = dayNumber(endYmd);
  if (a == null && b == null) return "";
  if (a == null) return shortDate(b);
  if (b == null) return shortDate(a);
  if (a === b) return shortDate(a);
  const far = Math.abs(b - a) > 300;
  return (far ? shortDateWithYear(a) : shortDate(a)) + " to " + (far ? shortDateWithYear(b) : shortDate(b));
}
function addDays(ymd, n) { const a = dayNumber(ymd); return a == null || !Number.isFinite(Number(n)) ? null : ymdOf(a + Math.round(Number(n))); }
function daysBetween(fromYmd, toYmd) { const a = dayNumber(fromYmd), b = dayNumber(toYmd); return a == null || b == null ? null : b - a; }

// ---- small text helpers ----
const plural = (n, word) => n + " " + word + (n === 1 ? "" : "s");
function cap(text, max) {
  const s = String(text).replace(/\s+/g, " ").trim();
  if (s.length <= max) return s;
  const cut = s.slice(0, max - 1), sp = cut.lastIndexOf(" ");
  return (sp > max * 0.6 ? cut.slice(0, sp) : cut).replace(/[\s,;:.]+$/, "") + "…";
}
function cleanLabel(v) { return String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 40); }
function wholeNumber(v, lo, hi, fallback) {
  const n = v == null || v === "" || typeof v === "boolean" ? NaN : Number(v);
  return Number.isFinite(n) ? Math.max(lo, Math.min(hi, Math.round(n))) : fallback;
}
function normaliseKind(kind) {
  const k = String(kind == null ? "" : kind).trim().toLowerCase();
  const key = KIND_ALIASES[k] || k;
  return KINDS.includes(key) ? key : null;
}
function learningFor(kind, smartBidding) {
  const k = normaliseKind(kind);
  if (!k) return null;
  return k === "search" && smartBidding === false ? LEARNING.search_manual : LEARNING[k];
}

function eventLine(label, evDay, daysAway) {
  if (daysAway > 1) return label + " is " + shortDate(evDay) + ", " + daysAway + " days away.";
  if (daysAway === 1) return label + " is " + shortDate(evDay) + ", tomorrow.";
  if (daysAway === 0) return label + " is today.";
  return label + " was " + shortDate(evDay) + ", " + plural(-daysAway, "day") + " ago.";
}
// "Google needs about 2 weeks to learn a Smart Bidding Search campaign; judge it from Nov 20."
function learningLine(spec, judgeDay, endDay) {
  if (judgeDay > endDay + 1) {
    return "Google needs about " + spec.wait + " to learn " + spec.noun + ". It ends before the " + spec.judgeWait + " mark, so judge it on sales, not early numbers.";
  }
  const both = spec.judgeAfter === spec.days
    ? "Google needs about " + spec.wait + " to learn " + spec.noun
    : "Google needs about " + spec.wait + " to learn " + spec.noun + ", and about " + spec.judgeWait + " before it is fair to judge";
  return both + "; judge it from " + shortDate(judgeDay) + ".";
}

/* buildSchedule({ kind, today, event, orderCutoffDays, requestedDays, smartBidding, startDate, endDate, minSellingDays })
     kind             'search' | 'pmax' | 'display' | 'demand_gen' | 'video' (the console's style keys responsive_display,
                      fixed_display and performance_max are accepted and mapped)
     today            YYYY-MM-DD in the account's time zone
     event            { label, date:'YYYY-MM-DD', market } for a campaign tied to a date, else null
     orderCutoffDays  days before the event when a new order can no longer arrive (0 to 30, default 0); the run ends then
     requestedDays    a run length the person asked for (1 to 365); otherwise the recommended one
     smartBidding     Search only: false means manual bids, which need a shorter learning period (default true)
     startDate/endDate a window chosen for a draft; it replaces the planned one (a past start moves to today)
     minSellingDays   selling days after learning for a comfortable plan (default 10); fewer than 5 cannot work
   Returns the schedule, or null when kind, today or the event date is not usable (callers skip the card, never guess). */
function buildSchedule(input) {
  const o = input || {};
  const kind = normaliseKind(o.kind);
  const t = dayNumber(o.today);
  if (!kind || t == null) return null;
  const smart = o.smartBidding == null ? true : !!o.smartBidding;
  const spec = learningFor(kind, kind === "search" ? smart : true);
  const L = spec.days, J = spec.judgeAfter;

  let evDay = null, label = null, market = null;
  if (o.event) {
    evDay = dayNumber(o.event.date);
    if (evDay == null) return null;
    label = cleanLabel(o.event.label) || "The event";
    market = o.event.market ? cleanLabel(o.event.market).slice(0, 12) : null;
  }
  const cut = wholeNumber(o.orderCutoffDays, 0, 30, 0);
  const minSell = wholeNumber(o.minSellingDays, 1, 90, 10);
  const tightFloor = Math.min(5, minSell);
  const req = wholeNumber(o.requestedDays, 1, 365, null);
  const ws = dayNumber(o.startDate), we = dayNumber(o.endDate);
  const custom = ws != null || we != null;

  let start, end, verdict, phases = [], why = [], basis, headline, closed = false;
  const phase = (key, from, to, text) => { if (to >= from) phases.push({ key, from: ymdOf(from), to: ymdOf(to), days: to - from + 1, label: text }); };

  if (evDay != null) {
    const last = evDay - cut, daysAway = evDay - t;
    const cutPart = cut ? " (" + label + " " + shortDate(evDay) + " less a " + cut + "-day order cutoff)" : " (" + label + " itself)";
    if (last < t) {
      // Even an order placed today cannot arrive in time, or the event is over.
      closed = true; start = t; end = t; verdict = "too_short";
      if (evDay >= t) phase("cutoff", t, evDay, "Order cutoff");
      headline = evDay < t ? label + " has passed" : "Last orders were due " + shortDate(last);
      why = [
        "Too late to test this for " + label + ": the last gift orders that can arrive were due " + shortDate(last) + ".",
        eventLine(label, evDay, daysAway),
        "Better as an ongoing test, or saved for next year."
      ];
      basis = "The last order that could arrive" + (cut ? " (" + label + " " + shortDate(evDay) + " less a " + cut + "-day order cutoff)" : " by " + shortDate(evDay)) + " was " + shortDate(last) + ".";
    } else {
      if (ws != null) start = Math.max(t, ws);
      else if (req != null) start = Math.max(t, last - req + 1);
      else start = Math.max(t, last - (L + spec.sellDays) + 1);
      if (we != null) end = Math.max(we, start);
      else if (req != null && ws != null) end = start + req - 1;
      else end = Math.max(last, start);

      const effEnd = Math.min(end, last), sellFrom = start + L;
      const sellDays = Math.max(0, effEnd - sellFrom + 1), room = effEnd - start + 1;
      verdict = sellDays >= minSell ? "good" : (sellDays >= tightFloor ? "tight" : "too_short");

      if (effEnd >= start) phase("learning", start, Math.min(start + L - 1, effEnd), "Learning");
      phase("selling", sellFrom, effEnd, "Selling");
      if (cut > 0) phase("cutoff", last + 1, evDay, "Order cutoff");

      const runDays = end - start + 1;
      let stop;
      if (cut) {
        stop = end > last ? "Runs past " + shortDate(last) + ": gifts ordered after that may not arrive by " + shortDate(evDay) + "."
          : end === last ? "Stops " + shortDate(last) + " so gifts can still arrive by " + shortDate(evDay) + "."
          : "Stops " + shortDate(end) + "; orders until " + shortDate(last) + " could still arrive by " + shortDate(evDay) + ".";
      } else {
        stop = end > evDay ? "Runs past " + shortDate(evDay) + ", when gift searches stop turning into sales."
          : end === evDay ? "Runs through " + shortDate(evDay) + ", the day itself."
          : "Stops " + shortDate(end) + ", before " + label + " on " + shortDate(evDay) + ".";
      }
      if (verdict === "too_short") {
        headline = "Would run " + formatRange(ymdOf(start), ymdOf(end)) + " · " + plural(runDays, "day");
        why = [
          room <= L
            ? "Too late to test this for " + label + ": it would still be learning when the last gift orders can arrive."
            : "Too late to test this for " + label + ": learning would end only " + plural(sellDays, "day") + " before the last gift orders can arrive.",
          eventLine(label, evDay, daysAway),
          "Better as an ongoing test, or saved for next year."
        ];
        basis = "Would end " + shortDate(effEnd) + cutPart + ". With about " + spec.wait + " of learning, " + (sellDays ? plural(sellDays, "day") : "no time") + " would be left to sell.";
      } else if (verdict === "tight") {
        headline = "Runs " + formatRange(ymdOf(start), ymdOf(end)) + " · " + plural(runDays, "day");
        why = [
          eventLine(label, evDay, daysAway),
          "Tight fit: Google finishes learning " + shortDate(sellFrom) + ", leaving only " + plural(sellDays, "day") + " to sell.",
          stop
        ];
        basis = (custom ? "Dates chosen for this draft. " : "") + "Ends " + shortDate(effEnd) + cutPart + ". About " + spec.wait + " of learning leaves " + plural(sellDays, "selling day") + ".";
      } else {
        headline = "Runs " + formatRange(ymdOf(start), ymdOf(end)) + " · " + plural(runDays, "day");
        why = [eventLine(label, evDay, daysAway), learningLine(spec, start + J, end), stop];
        basis = custom
          ? "Dates chosen for this draft. The last order that can arrive is placed " + shortDate(last) + "."
          : "Ends " + shortDate(last) + cutPart + ". Counted back from there: about " + spec.wait + " of learning, then " + plural(sellDays, "day") + " of selling.";
      }
    }
  } else {
    verdict = "evergreen";
    start = ws != null ? Math.max(t, ws) : t;
    const days0 = req != null ? req : spec.defaultDays;
    end = we != null ? Math.max(we, start) : start + days0 - 1;
    phase("learning", start, Math.min(start + L - 1, end), "Learning");
    phase("selling", start + L, end, "Selling");
    const runDays = end - start + 1;
    headline = "Runs " + formatRange(ymdOf(start), ymdOf(end)) + " · " + plural(runDays, "day");
    why = ["Not tied to a date, so it can start " + (start === t ? "today" : "on " + shortDate(start)) + ".", learningLine(spec, start + J, end)];
    if (runDays < J) why.push("A " + plural(runDays, "day") + " run ends before Google has had " + spec.judgeWait + ", so read the results as early signs.");
    basis = (custom ? "Dates chosen for this draft. " : "Not tied to a date. ") + (req != null ? "Runs the " + plural(runDays, "day") + " asked for." : "Runs the usual " + plural(runDays, "day") + " for this kind of campaign.");
  }

  const days = closed ? 0 : end - start + 1;
  return {
    version: SCHEDULE_VERSION,
    kind,
    today: ymdOf(t),
    timeSensitive: evDay != null,
    event: evDay == null ? null : { label, date: ymdOf(evDay), daysAway: evDay - t, market },
    start: ymdOf(start),
    end: ymdOf(end),
    days,
    learning: { days: L, endsOn: ymdOf(start + L - 1), note: spec.note },
    judgeAfter: ymdOf(start + J),
    phases,
    verdict,
    headline: cap(headline, 120),
    why: why.slice(0, 3).map(s => cap(s, 140)),
    basis: cap(basis, 200)
  };
}

module.exports = { buildSchedule, formatRange, LEARNING, KINDS, SCHEDULE_VERSION, learningFor, addDays, daysBetween };
