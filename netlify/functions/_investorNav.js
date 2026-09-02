/*  netlify/functions/_investorNav.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — account value, live and over time.
 *
 *  The desk already keeps one NAV row per day (control/navHistory) for the
 *  promotion ladder. That is a research record, not something a person can
 *  watch. This module answers the operator's question — "am I up or down
 *  right now, and how did today go?" — with:
 *
 *    · snapshot(accountId): the account's value THIS INSTANT from the ledger
 *      balances and the latest position marks: cash, reserved, invested,
 *      unrealised (split winners/losers), realised, and the starting amount.
 *    · record(accountId, snapshot): appends the snapshot to ONE document per
 *      account-day (InvestorAI_NavMarks/{accountId}_{date}), so a week of
 *      minute-level marks is seven reads, not seven thousand.
 *    · series(accountId, range): the points for a console range, thinned to
 *      a sensible count, with daily navHistory filling any day older than
 *      the marks.
 *
 *  Display only. Nothing here feeds a decision.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const L = require("./_investorLedger");
const M = require("./_investorMarket");

const MAX_POINTS_PER_DAY = 600;          // ~1/min across a session plus closed-market guards
const RANGES = Object.freeze({
  today: { days: 1 }, yesterday: { days: 2, prevOnly: true },
  "7": { days: 7 }, "14": { days: 14 }, "30": { days: 30 }, "60": { days: 60 }, "90": { days: 90 },
});

function docId(accountId, date) { return `${accountId}_${date}`; }

/** Value the account now. Marks come from positions (guard updates them each
 *  minute in session); a position without a mark is valued at entry and
 *  reported as such. */
async function snapshot(accountId, { positions = null, balances = null, cost = null } = {}) {
  const [b, c, posRows, acct] = await Promise.all([
    balances || L.balances(accountId),
    cost || L.costMeter(accountId).catch(() => ({})),
    positions || A.col(A.COL.positions).where("accountId", "==", accountId)
      .where("open", "==", true).get().then((s) => s.docs.map((d) => d.data())),
    A.col(A.COL.accounts).doc(accountId).get().then((s) => (s.exists ? s.data() : {})),
  ]);
  const usd = (b && b.usd) || {};
  const cashUsd = Number(usd.cash) || 0, reservedUsd = Number(usd.reserved) || 0;
  let investedUsd = 0, costBasisUsd = 0, winnersUsd = 0, losersUsd = 0, unmarked = 0, open = 0;
  let lastMarkMs = 0;
  for (const p of posRows) {
    if (!p || p.open !== true) continue;
    open += 1;
    const qty = Number(p.qty) || 0;
    const entry = Number(p.entryPriceUsd != null ? p.entryPriceUsd : p.avgPrice) || 0;
    const mark = Number(p.lastMarkUsd) > 0 ? Number(p.lastMarkUsd) : entry;
    if (!(Number(p.lastMarkUsd) > 0)) unmarked += 1;
    const t = p.lastMarkAt ? Date.parse(p.lastMarkAt) : 0;
    if (t > lastMarkMs) lastMarkMs = t;
    investedUsd += qty * mark; costBasisUsd += qty * entry;
    const pl = qty * (mark - entry);
    if (pl >= 0) winnersUsd += pl; else losersUsd += pl;
  }
  const unrealisedUsd = investedUsd - costBasisUsd;
  const realisedUsd = Number(c && c.netRealizedUsd) || 0;
  const frictionUsd = Number(c && c.frictionUsd) || 0;
  const startingNavUsd = acct && acct.startingNavCents != null
    ? L.fromCents(acct.startingNavCents) : null;
  const navUsd = cashUsd + reservedUsd + investedUsd;
  const round = (x) => Number(x.toFixed(2));
  return {
    accountId, tMs: Date.now(),
    navUsd: round(navUsd), cashUsd: round(cashUsd), reservedUsd: round(reservedUsd),
    investedUsd: round(investedUsd), costBasisUsd: round(costBasisUsd),
    unrealisedUsd: round(unrealisedUsd), winnersUsd: round(winnersUsd), losersUsd: round(losersUsd),
    realisedUsd: round(realisedUsd), frictionUsd: round(frictionUsd),
    startingNavUsd: startingNavUsd != null ? round(startingNavUsd) : null,
    totalPlUsd: startingNavUsd != null ? round(navUsd - startingNavUsd) : null,
    openPositions: open, unmarkedPositions: unmarked,
    lastMarkAt: lastMarkMs ? new Date(lastMarkMs).toISOString() : null,
  };
}

/** Append one point to the account-day document. Thinned so a day never
 *  exceeds MAX_POINTS_PER_DAY (older points are kept at coarser spacing). */
async function record(accountId, snap, { source = "guard" } = {}) {
  const date = M.nyParts(new Date(snap.tMs)).date;
  const ref = A.col(A.COL.navMarks).doc(docId(accountId, date));
  const point = { t: snap.tMs, nav: snap.navUsd, cash: snap.cashUsd, res: snap.reservedUsd,
    inv: snap.investedUsd, unr: snap.unrealisedUsd, rl: snap.realisedUsd, open: snap.openPositions,
    win: snap.winnersUsd, los: snap.losersUsd, s: source };
  await A.runTransaction(async (tx) => {
    const cur = await tx.get(ref);
    let points = cur.exists ? (cur.data().points || []) : [];
    const last = points[points.length - 1];
    /* Two marks inside 20 seconds carry no information; keep the newer. */
    if (last && snap.tMs - Number(last.t) < 20000) points = points.slice(0, -1);
    points.push(point);
    if (points.length > MAX_POINTS_PER_DAY) {
      const keepTail = 200;
      const head = points.slice(0, -keepTail), tail = points.slice(-keepTail);
      const stride = Math.ceil(head.length / (MAX_POINTS_PER_DAY - keepTail));
      points = head.filter((_, i) => i % stride === 0).concat(tail);
    }
    tx.set(ref, { accountId, date, points, count: points.length,
      first: points[0] ? points[0].t : null, last: point.t,
      startingNavUsd: snap.startingNavUsd, updatedAt: A.FV.serverTimestamp(),
      ...(cur.exists ? {} : A.envelope({ created_by: "investorNav" })) }, { merge: true });
  });
  return { date, ref: ref.id };
}

/** Trading dates covering `days` sessions back from now, oldest first. */
function sessionDates(days, nowMs = Date.now()) {
  const out = [];
  const d = new Date(nowMs);
  let guard = 0;
  while (out.length < days && guard < days * 3 + 10) {
    const st = M.sessionState(d);
    if (st.tradingDay) out.push(st.date);
    d.setUTCDate(d.getUTCDate() - 1);
    guard += 1;
  }
  return out.reverse();
}

function thin(points, maxPoints) {
  if (points.length <= maxPoints) return points;
  const stride = Math.ceil(points.length / maxPoints);
  const out = points.filter((_, i) => i % stride === 0);
  if (out[out.length - 1] !== points[points.length - 1]) out.push(points[points.length - 1]);
  return out;
}

async function series(accountId, rangeKey, { nowMs = Date.now(), maxPoints = 700 } = {}) {
  const spec = RANGES[String(rangeKey)] || RANGES.today;
  let dates = sessionDates(spec.days, nowMs);
  const todayDate = M.sessionState(new Date(nowMs)).date;
  if (spec.prevOnly) dates = dates.filter((x) => x !== todayDate).slice(-1);
  const snaps = await Promise.all(dates.map((date) =>
    A.col(A.COL.navMarks).doc(docId(accountId, date)).get()));
  let points = [];
  const covered = new Set();
  snaps.forEach((s, i) => {
    if (!s.exists) return;
    covered.add(dates[i]);
    for (const p of (s.data().points || [])) points.push({ ...p, date: dates[i] });
  });
  /* Days before minute marks existed: one point per day from navHistory. */
  const missing = dates.filter((d) => !covered.has(d));
  if (missing.length) {
    const hist = await Promise.all(missing.map((d) =>
      A.col(A.COL.control).doc("control").collection("navHistory").doc(d).get()));
    hist.forEach((h, i) => {
      if (!h.exists) return;
      const r = h.data();
      const closeMs = M.nyWallClockToUtcMs ? M.nyWallClockToUtcMs(missing[i], 16 * 60) : Date.parse(`${missing[i]}T20:00:00Z`);
      points.push({ t: closeMs || Date.parse(`${missing[i]}T20:00:00Z`), nav: Number(r.navUsd) || 0,
        cash: Number(r.cashUsd) || 0, res: 0, inv: Number(r.positionsUsd) || 0,
        unr: Number(r.unrealisedUsd) || 0, rl: null, open: Number(r.openPositions) || 0,
        win: null, los: null, s: "daily", date: missing[i] });
    });
  }
  points.sort((a, b) => a.t - b.t);
  const acct = await A.col(A.COL.accounts).doc(accountId).get();
  const startingNavUsd = acct.exists && acct.data().startingNavCents != null
    ? L.fromCents(acct.data().startingNavCents) : null;
  /* Day-start reference: the last point BEFORE today (yesterday's close),
     else today's first point, so "up or down today" has an honest anchor. */
  const before = points.filter((p) => p.date !== todayDate);
  const todayPts = points.filter((p) => p.date === todayDate);
  const dayStartNav = before.length ? before[before.length - 1].nav
    : (todayPts.length ? todayPts[0].nav : null);
  return { range: String(rangeKey), dates, points: thin(points, maxPoints),
    rawCount: points.length, startingNavUsd, dayStartNav, todayDate,
    rangeStartNav: points.length ? points[0].nav : null };
}

module.exports = { RANGES, MAX_POINTS_PER_DAY, snapshot, record, series, sessionDates, thin };
