/*  netlify/functions/_investorShadow.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the shadow harness. The piece that makes learning possible.
 *
 *  THE PROBLEM IT SOLVES
 *  ------------------------------------------------------------------------
 *  To tell a real edge from luck at this signal size you need roughly 625
 *  independent observations. Trading a handful of names a week, that is years
 *  away — and no learning method, however clever, can run without data.
 *
 *  THE FIX: score every variant against every ranked name on every cycle,
 *  whether or not anything was actually traded. Each variant records what it
 *  WOULD have done and what that WOULD have earned. Real trades cost money
 *  and are rare; shadow trades cost nothing and are plentiful, and they carry
 *  the same information about which variant works.
 *
 *  This is off-policy evaluation: learning about actions you did not take.
 *  It is how you learn in a domain where exploring is expensive.
 *
 *  THE HONEST CAVEAT, ENFORCED IN CODE
 *  ------------------------------------------------------------------------
 *  Shadow observations are NOT independent. Two names bought the same morning
 *  in the same selloff win or lose together. Counting them as two independent
 *  data points is how you convince yourself you have 5,000 samples when you
 *  have 400.
 *
 *  So effectiveSampleSize() counts DISTINCT TRADING DAYS, not trades. It is
 *  deliberately conservative: a day on which a variant opened nine positions
 *  counts once. Every power calculation in the allocator uses that number,
 *  never the raw count.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const S = require("./_investorSignal");
const M = require("./_investorMarket");
const V = require("./_investorVariants");

const OPEN = A.COL_PREFIX + "ShadowOpen";     // one doc per (variant, symbol)
const CLOSED = A.COL_PREFIX + "ShadowClosed"; // the learning dataset
const STATS  = A.COL_PREFIX + "ShadowStats";   // running per-variant roll-up

/* ── entry: what would each variant have done this cycle? ──────────────── */
/**
 * @param ctx  { cycleId, ranks, zBySymbol, quality, meta, cause, session,
 *               regime, baseCfg, earnings, crowd, bars }
 * @returns    { opened, evaluated, byVariant }
 */
async function evaluateEntries(ctx) {
  const { cycleId, ranks, zBySymbol, quality, meta, causeBySymbol, session,
          regime, baseCfg, earnings, crowd, lastPrice } = ctx;
  /* Variants G and H are defined by long-horizon conditions, so the shadow
     harness must see the same history the live path sees. Without it they
     would decline every trade and their zero record would be meaningless. */
  const historyCtx = ctx.historyCtx || {};
  const reversion = ctx.reversion || {};
  const turnoverPctile = ctx.turnoverPctile || {};

  const opened = [];
  const byVariant = {};
  let evaluated = 0;

  // Which (variant, symbol) pairs are already open — one read, not N.
  const openSnap = await A.col(OPEN).get();
  const isOpen = new Set();
  openSnap.forEach((d) => isOpen.add(d.id));

  for (const variant of V.VARIANTS) {
    const cfg = V.configFor(variant.id, baseCfg);
    byVariant[variant.id] = { considered: 0, opened: 0, blocked: {} };

    for (const symbol of Object.keys(ranks)) {
      const z = zBySymbol[symbol];
      if (!z) continue;
      evaluated += 1;
      byVariant[variant.id].considered += 1;

      const key = `${variant.id}_${symbol}`;
      if (isOpen.has(key)) continue;             // already holding it

      const m = meta[symbol] || {};
      const ew = earnings[symbol];
      const res = S.evaluateCandidate({
        symbol, rank: ranks[symbol], zStat: z,
        quality: quality[symbol], advUsd: m.advUsd || 0,
        earningsDates: (ew && ew.dates) || [],
        earningsEstimated: !!(ew && ew.estimated),
        nowMs: Date.now(),
        cause: causeBySymbol[symbol] || S.CAUSE.PENDING,
        vixNorm: regime.vixNorm, cor3m: regime.cor3m,
        sectorTailFraction: crowd.fractionInTail[S.sectorOf(symbol)] ?? 0,
        session, cfg, position: null,
        historyContext: historyCtx[symbol] || null,
        reversion: reversion[symbol] || null,
        turnoverPctile: turnoverPctile[symbol] ?? null,
      });

      if (!res.pass) {
        const b = res.blockedBy[0] || "unknown";
        byVariant[variant.id].blocked[b] = (byVariant[variant.id].blocked[b] || 0) + 1;
        continue;
      }

      /* ANTI-LOOK-AHEAD, DONE THE WAY THE LIVE PATH DOES IT.
         The previous "fix" asked the caller for an eligible entry price
         computed with decisionAtMs = Date.now() — and with a 15-minute-delayed
         feed no bar in the current panel can ever satisfy that, so the
         fallback (the decision bar's close) was ALWAYS taken and the fix was a
         no-op. The learning dataset stayed optimistic by one bar per side.

         A shadow entry is now opened PENDING with its decision time recorded,
         exactly like a real order. The next cycle whose panel contains a bar
         printed after the decision fills it at that bar's open. Same clock,
         same rules, same information set as real money. */
      const slip = M.slippageBps({
        advUsd: m.advUsd || 0, grade: quality[symbol].grade,
        wideSpreadWindow: session.wideSpreadWindow, vixNorm: regime.vixNorm,
      });

      await A.col(OPEN).doc(key).set({
        variantId: variant.id, symbol, cycleId,
        pendingEntry: true,
        decisionAtMs: Date.now(),
        signalPrice: lastPrice[symbol] || null,   // recorded for slippage audit only
        openedAt: new Date().toISOString(),
        openedDate: session.date,
        entryPrice: null,
        entrySlippageBps: slip,
        rank: ranks[symbol], z: res.z, cumResidualBps: res.cumResidualBps,
        cause: causeBySymbol[symbol] || S.CAUSE.PENDING,
        sector: S.sectorOf(symbol),
        sizeMult: res.sizing.combined,
        ...A.envelope({ created_by: "shadow.evaluateEntries" }),
      });

      opened.push({ variantId: variant.id, symbol, pending: true });
      byVariant[variant.id].opened += 1;
      isOpen.add(key);
    }
  }
  return { opened: opened.length, evaluated, byVariant, detail: opened.slice(0, 40) };
}

/* ── exit: close any shadow position whose rule has fired ──────────────── */
async function evaluateExits(ctx) {
  const { ranks, session, baseCfg, lastPrice, quality, meta, regime } = ctx;
  const earnings = ctx.earnings || {};
  const snap = await A.col(OPEN).get();
  const closed = [];
  let dataLoss = 0;

  for (const doc of snap.docs) {
    const p = doc.data();
    const cfg = V.configFor(p.variantId, baseCfg);
    const rank = ranks[p.symbol];
    const px = lastPrice[p.symbol];
    const heldDays = (Date.now() - Date.parse(p.openedAt)) / 864e5;

    /* PENDING ENTRIES FILL FIRST. The position was opened with only a decision
       time; it fills at the open of the first bar the feed delivered that was
       printed after that decision — the same clock real orders use. Until
       then it has no entry price and cannot be scored. A pending entry that
       finds no fill within two days never happened. */
    if (p.pendingEntry) {
      const pbars = (ctx.panel && ctx.panel[p.symbol]) || [];
      const pe = pbars.length ? M.firstEligibleBar(pbars, {
        decisionAtMs: p.decisionAtMs || Date.parse(p.openedAt),
        provider: ctx.providerId || "alpaca",
        executionLatencyMs: baseCfg.executionLatencyMs || 60000,
      }) : { bar: null };
      if (pe.bar && pe.bar.o > 0) {
        await doc.ref.set({ pendingEntry: false, entryPrice: pe.bar.o, filledAt: pe.barOpenAt }, { merge: true });
        p.entryPrice = pe.bar.o; p.pendingEntry = false;
      } else if (heldDays > 2) {
        await doc.ref.delete();      // never fillable — not a trade, not a data point
        continue;
      } else {
        continue;                     // waiting for the feed to catch up
      }
    }
    if (!(p.entryPrice > 0)) continue;

    /* A symbol that has dropped out of the panel for good is usually halted,
       delisted or acquired — the fattest tails on the downside. The old code
       let it fall through with a fabricated rank of 0.5, which is >= the 0.50
       exit for six of eight variants, so it booked an immediate "recovery"
       exit at the ENTRY price: a tidy -15bp loss standing in for what may have
       been -60%. That is survivorship bias written straight into the training
       set. Such a position is now retired as UNRESOLVED and excluded from the
       statistics rather than recorded as a small controlled loss. */
    if (rank == null || !(px > 0)) {
      if (heldDays < (cfg.maxHoldDays ?? 10) + 5) continue;
      await A.col(CLOSED).add({
        variantId: p.variantId, symbol: p.symbol,
        openedAt: p.openedAt, openedDate: p.openedDate,
        closedAt: new Date().toISOString(), closedDate: session.date,
        heldDays: Number(heldDays.toFixed(2)),
        unresolved: true, excludeFromStats: true,
        exitReason: "symbol left the panel — outcome unknown, excluded from learning rather than guessed",
        ...A.envelope({ created_by: "shadow.evaluateExits" }),
      });
      await doc.ref.delete();
      dataLoss += 1;
      continue;
    }

    /* THE SHADOW MUST RACE THE STRATEGY THAT ACTUALLY RUNS.
       This call previously omitted the fourth argument entirely, so inside
       exitSignal `havePrice` was false and the hard stop, trailing stop, take
       profit and earnings exit were ALL skipped. The harness was therefore
       measuring a rank-and-time-only policy that the live system does not run,
       and the winner it picked would have been the winner of the wrong race. */
    const ew = earnings[p.symbol];
    let earningsInDays = null;
    if (ew && ew.dates && ew.dates.length) {
      const soonest = ew.dates.map((d) => (Date.parse(d) - Date.now()) / 864e5)
        .filter((d) => d >= 0).sort((a, b) => a - b)[0];
      if (soonest != null) earningsInDays = soonest;
    }
    const peak = Math.max(Number(p.peakPrice) || 0, px);
    if (peak > (Number(p.peakPrice) || 0) * 1.002) {   // write only on a real new high
      try { await doc.ref.set({ peakPrice: peak }, { merge: true }); } catch {}
    }

    /* Record the exit DECISION once; fill it on a later cycle's post-decision
       bar. Booking the close of the bar whose rank triggered the sale banks
       the very recovery being detected — the directional look-ahead that made
       every variant look better than it was. */
    let intent = p.exitIntent;
    if (!intent) {
      const ex = S.exitSignal(rank, heldDays, cfg, {
        mark: px, entry: p.entryPrice, peak, earningsInDays,
      });
      if (!ex.exit) continue;
      intent = { decisionAtMs: Date.now(), reason: ex.reason, kind: ex.kind || "signal" };
      await doc.ref.set({ exitIntent: intent }, { merge: true });
      continue;                       // fills on a later cycle, like real money
    }

    const ibars = (ctx.panel && ctx.panel[p.symbol]) || [];
    const ie = ibars.length ? M.firstEligibleBar(ibars, {
      decisionAtMs: intent.decisionAtMs,
      provider: ctx.providerId || "alpaca",
      executionLatencyMs: baseCfg.executionLatencyMs || 60000,
    }) : { bar: null };
    if (!ie.bar || !(ie.bar.o > 0)) continue;   // feed not caught up yet

    const ex = { reason: intent.reason, kind: intent.kind };
    const m = meta[p.symbol] || {};
    const exitSlip = M.slippageBps({
      advUsd: m.advUsd || 0, grade: (quality[p.symbol] || {}).grade || "C",
      wideSpreadWindow: session.wideSpreadWindow, vixNorm: regime.vixNorm,
    });
    const exitPx = ie.bar.o;
    const grossBps = ((exitPx - p.entryPrice) / p.entryPrice) * 1e4;
    const costBps = (p.entrySlippageBps || 0) + exitSlip;
    const netBps = grossBps - costBps;

    await A.col(CLOSED).add({
      variantId: p.variantId, symbol: p.symbol,
      openedAt: p.openedAt, openedDate: p.openedDate,
      closedAt: new Date().toISOString(), closedDate: session.date,
      heldDays: Number(heldDays.toFixed(2)),
      entryPrice: p.entryPrice, exitPrice: exitPx,
      grossBps: Number(grossBps.toFixed(2)),
      costBps: Number(costBps.toFixed(2)),
      netBps: Number(netBps.toFixed(2)),
      exitReason: ex.reason, exitKind: ex.kind || "signal",
      exitPriceEligible: true,   // structurally guaranteed now — fills only on post-decision bars
      rank: p.rank, z: p.z, cause: p.cause, sector: p.sector,
      vixNorm: regime.vixNorm, cor3m: regime.cor3m,
      ...A.envelope({ created_by: "shadow.evaluateExits" }),
    });
    await doc.ref.delete();
    try {
      await accumulate(p.variantId, {
        netBps, grossBps, costBps, heldDays, openedDate: p.openedDate,
      });
    } catch (e) { /* the roll-up is an optimisation; the row is the record */ }
    closed.push({ variantId: p.variantId, symbol: p.symbol, netBps: Number(netBps.toFixed(1)) });
  }
  return { closed: closed.length, dataLoss, detail: closed.slice(0, 40) };
}

/* ── the honest sample size ────────────────────────────────────────────── */
/**
 * Trades on the same day are not independent — they win or lose together in
 * the same market move. Counting distinct DAYS instead of trades is the
 * conservative correction, and it is what every power calculation uses.
 */
function effectiveSampleSize(rows) {
  const days = new Set();
  for (const r of rows) if (r.openedDate) days.add(r.openedDate);
  return { nominal: rows.length, effective: days.size, distinctDays: days.size };
}

/* ── per-variant statistics ────────────────────────────────────────────── */
/* ── THE EVIDENCE LEDGER: ONE DOCUMENT PER VARIANT PER DAY ─────────────────
 *
 * Two prior designs failed here, instructively:
 *   · reading "the most recent N closed trades" silently capped the distinct-
 *     day count below the power gate, so the gate could never open;
 *   · a per-variant accumulator with a `days.<date>` increment relied on
 *     dotted-key semantics that Firestore's set() does not have (set() treats
 *     dots as literal field names; only update() interprets paths), so in
 *     production the distinct-day map never formed and "independent days"
 *     silently became raw trade counts — the gate would have opened on an
 *     eighth of the required evidence. The in-memory test double deep-set
 *     dotted keys and hid it.
 *
 * A day-doc needs neither trick: id `${variantId}_${date}`, plain increments.
 * Distinct days = number of documents. It also gives, for free, the per-day
 * per-variant means that the changepoint detector and the paired promotion
 * test need. ~8 variants × 250 trading days = 2,000 docs/year.
 */
async function accumulate(variantId, row) {
  if (!variantId || !row || !Number.isFinite(row.netBps)) return;
  const date = String(row.openedDate || new Date().toISOString().slice(0, 10)).slice(0, 10);
  const ref = A.col(STATS).doc(`${variantId}_${date}`);
  const inc = A.FV.increment;
  await ref.set({
    variantId, date,
    n: inc(1),
    sumNet: inc(row.netBps),
    sumNetSq: inc(row.netBps * row.netBps),
    sumGross: inc(Number(row.grossBps) || 0),
    sumCost: inc(Number(row.costBps) || 0),
    sumHold: inc(Number(row.heldDays) || 0),
    wins: inc(row.netBps > 0 ? 1 : 0),
    updatedAt: A.FV.serverTimestamp(),
  }, { merge: true });
}

/* ── CHANGEPOINT DETECTION: THE SYSTEM MUST BE ABLE TO UN-LEARN ────────────
 *
 * A posterior that accumulates forever cannot notice that the world changed.
 * Published trading edges decay — roughly 26% out of sample and 58% after
 * publication (McLean & Pontiff, Journal of Finance 2016) — so "variant D was
 * good for two years" must not pin the book to D for the third year while D
 * quietly dies. The non-stationary bandit literature (Garivier & Moulines
 * 2011; discounted Thompson sampling) and Bayesian online changepoint
 * detection (Adams & MacKay 2007) both address this; the practical synthesis,
 * per the changepoint-module results of Wood, Roberts & Zohren (2022), is:
 * restart beats slow forgetting for abrupt breaks, and a modest discount
 * handles gradual drift.
 *
 * Implementation: a Page-Hinkley test per variant on the sequence of DAILY
 * mean net returns. Page-Hinkley tracks the cumulative gap between each new
 * observation and the running mean; when the gap exceeds a threshold scaled
 * to the series' own noise, a break is declared. On a break the SELECTION
 * evidence restarts from that date (the posterior re-inflates toward the
 * skeptical prior and Thompson sampling automatically re-explores), while the
 * POWER-GATE evidence keeps the full record — because "how much have we ever
 * observed" and "what do we currently believe" are different questions.
 */
/* Calibration: Page-Hinkley's false-alarm probability over a long stationary
   stretch behaves like exp(-2·delta·lambda / sigma²). The first cut used
   delta=0.1σ, lambda=6σ → exp(-1.2) ≈ 30% — it cried wolf on plain noise, and
   the fixture caught it. At delta=0.25σ, lambda=12σ the same bound is
   exp(-6) ≈ 0.25% per stretch, while a real break of ~3σ/day still trips the
   alarm within about four trading days. Slow false alarms are cheap here (a
   reset just re-explores); missed breaks are expensive. */
const PH_DELTA_FRAC = 0.25;   // tolerated drift, as a fraction of daily sd
const PH_LAMBDA_SD = 12;      // alarm threshold, in units of daily sd
const DISCOUNT_GAMMA = 0.994; // gradual-drift discount: n_eff ≈ 167 trading days

function pageHinkley(dayMeans, sd) {
  if (dayMeans.length < 20 || !(sd > 0)) return { break: false, at: null };
  const delta = PH_DELTA_FRAC * sd;
  const lambda = PH_LAMBDA_SD * sd;
  let mean = 0, mT = 0, minM = 0, breakAt = null;
  for (let i = 0; i < dayMeans.length; i++) {
    mean += (dayMeans[i].mean - mean) / (i + 1);
    // watch for DETERIORATION: cumulative shortfall below the running mean
    mT += mean - dayMeans[i].mean - delta;
    if (mT < minM) minM = mT;
    if (mT - minM > lambda) { breakAt = dayMeans[i].date; break; }
  }
  return { break: breakAt != null, at: breakAt };
}

/**
 * Read the day-docs back and produce BOTH evidence tracks per variant:
 *
 *   power     — undiscounted, full history. Answers "have we observed enough
 *               days for any comparison to mean anything". Feeds requiredN.
 *   selection — starts at the last detected changepoint, and discounts what
 *               remains by γ^age (γ=0.994/day, effective memory ≈ 8 months).
 *               Answers "what is each variant's edge NOW". Feeds Thompson
 *               sampling and the leader choice.
 *
 * Also returns each variant's daily series so the paired promotion test can
 * difference two variants on their shared days.
 */
async function rollUpStats() {
  const snap = await A.col(STATS).limit(8000).get();
  const byVariant = {};
  snap.forEach((d) => {
    const r = d.data();
    if (!r || !r.variantId || !r.date || !(Number(r.n) > 0)) return;
    (byVariant[r.variantId] = byVariant[r.variantId] || []).push(r);
  });

  const powerT = {}, selectionT = {}, dailyByVariant = {}, resets = {};
  const today = Date.now();

  for (const [vid, rows] of Object.entries(byVariant)) {
    rows.sort((a, b) => (a.date < b.date ? -1 : 1));
    const days = rows.map((r) => ({
      date: r.date,
      n: Number(r.n) || 0,
      mean: r.sumNet / r.n,
      sumNet: r.sumNet, sumNetSq: r.sumNetSq,
      sumGross: r.sumGross || 0, sumCost: r.sumCost || 0,
      sumHold: r.sumHold || 0, wins: r.wins || 0,
    }));
    dailyByVariant[vid] = days;

    const agg = (subset, discounted) => {
      let n = 0, sum = 0, sumsq = 0, gross = 0, cost = 0, hold = 0, wins = 0, wsum = 0, wTot = 0;
      for (const d of subset) {
        n += d.n; sum += d.sumNet; sumsq += d.sumNetSq;
        gross += d.sumGross; cost += d.sumCost; hold += d.sumHold; wins += d.wins;
        if (discounted) {
          const age = Math.max(0, (today - Date.parse(d.date)) / 864e5);
          const w = Math.pow(DISCOUNT_GAMMA, age);
          wsum += w * d.mean; wTot += w;
        }
      }
      if (n < 2 || subset.length < 2) return null;
      const mean = sum / n;
      const varr = Math.max(0, sumsq / n - mean * mean) * (n / (n - 1));
      const sd = Math.sqrt(varr);
      const distinct = subset.length;
      const effDays = discounted && wTot > 0
        ? Math.min(distinct, Math.round(wTot / Math.pow(DISCOUNT_GAMMA, 0)))  // sum of weights ≈ discounted day count
        : distinct;
      const useMean = discounted && wTot > 0 ? wsum / wTot : mean;
      return {
        trades: n,
        effectiveN: effDays,
        meanNetBps: Number(useMean.toFixed(3)),
        meanGrossBps: Number((gross / n).toFixed(3)),
        meanCostBps: Number((cost / n).toFixed(3)),
        sdBps: Number(sd.toFixed(3)),
        winRate: Number((wins / n).toFixed(3)),
        avgHoldDays: Number((hold / n).toFixed(2)),
        tStat: sd > 0 && distinct > 1
          ? Number((useMean / (sd / Math.sqrt(distinct))).toFixed(2)) : null,
      };
    };

    const power = agg(days, false);
    if (power) powerT[vid] = { ...power, source: "power" };

    // changepoint on the daily means, using the full-history sd as the yardstick
    const ph = pageHinkley(days, power ? power.sdBps : 0);
    let selDays = days;
    if (ph.break) {
      resets[vid] = ph.at;
      selDays = days.filter((d) => d.date >= ph.at);
    }
    const sel = agg(selDays, true);
    if (sel) selectionT[vid] = { ...sel, source: "selection", resetAt: ph.break ? ph.at : null };
  }

  return { power: powerT, selection: selectionT, dailyByVariant, resets,
           discountGamma: DISCOUNT_GAMMA };
}

async function variantStats({ limit = 4000 } = {}) {
  /* Ordered, so that hitting the cap reads the MOST RECENT trades rather than
     an arbitrary Firestore-ordered subset. Unordered truncation meant that
     past 4,000 closed shadow trades every statistic silently described a
     random slice of history. */
  const snap = await A.col(CLOSED).orderBy("closedAt", "desc").limit(limit).get();
  /* Unresolved rows carry no outcome. Including them at any assumed value —
     even zero — teaches the allocator something that was never observed. */
  const rows = [];
  let excluded = 0;
  snap.forEach((d) => {
    const r = d.data();
    if (r.excludeFromStats || r.unresolved || !Number.isFinite(r.netBps)) { excluded += 1; return; }
    rows.push(r);
  });

  const out = {};
  for (const v of V.VARIANTS) {
    const mine = rows.filter((r) => r.variantId === v.id);
    const net = mine.map((r) => r.netBps);
    const gross = mine.map((r) => r.grossBps);
    const cost = mine.map((r) => r.costBps);
    const ess = effectiveSampleSize(mine);
    const mean = net.length ? S.mean(net) : 0;
    const sd = net.length > 1 ? S.stdev(net) : 0;
    const se = ess.effective > 1 ? sd / Math.sqrt(ess.effective) : Infinity;

    out[v.id] = {
      id: v.id, name: v.name, plain: v.plain,
      trades: mine.length,
      effectiveN: ess.effective,
      meanNetBps: Number(mean.toFixed(2)),
      meanGrossBps: Number((gross.length ? S.mean(gross) : 0).toFixed(2)),
      meanCostBps: Number((cost.length ? S.mean(cost) : 0).toFixed(2)),
      sdBps: Number(sd.toFixed(2)),
      tStat: isFinite(se) && se > 0 ? Number((mean / se).toFixed(2)) : null,
      winRate: net.length ? Number((net.filter((x) => x > 0).length / net.length).toFixed(3)) : null,
      avgHoldDays: mine.length ? Number(S.mean(mine.map((r) => r.heldDays)).toFixed(2)) : null,
    };
  }
  /* `capped` matters: the row cap silently bounds effectiveN, and effectiveN
     is what the power gate measures. With an 8-variant harness and a 4,000-row
     window, each variant can never show more than ~500 distinct days against a
     625-day requirement — the gate would never open, and the progress bar
     would sit near 80% forever with no explanation. The cycle uses this flag
     to fall back to a persisted running roll-up instead. */
  return { variants: out, totalClosed: rows.length, excluded,
           capped: (rows.length + excluded) >= limit, limit };
}

async function openCount() {
  const snap = await A.col(OPEN).get();
  const by = {};
  snap.forEach((d) => { const v = d.data().variantId; by[v] = (by[v] || 0) + 1; });
  return { total: snap.size, byVariant: by };
}

module.exports = {
  accumulate, rollUpStats, STATS,
  OPEN, CLOSED,
  evaluateEntries, evaluateExits,
  effectiveSampleSize, variantStats, openCount,
};
