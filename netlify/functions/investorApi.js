/*  netlify/functions/investorApi.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the console API. Synchronous HTTP, NOT scheduled.
 *
 *  Split from investorKick.js because a scheduled Netlify function cannot be
 *  reached over HTTP: Netlify answers a direct request with a bare 403 and no
 *  body. Putting a console on a cron'd function makes it unreachable, which is
 *  precisely the failure the legacy Google Ads autopilot hit.
 *
 *  Every action requires a verified operator. CORS is an exact-origin
 *  allowlist and is treated as a browser mechanic, never as authorization.
 *  Nothing here fetches a URL supplied by the browser, and no action can
 *  reach a real broker because none exists.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const H = require("./_investorHistory");
const R = require("./_investorRisk");
const S = require("./_investorSignal");
const L = require("./_investorLedger");
const E = require("./_investorEvidence");
/* SH/AL/V were used by the "learning" action and never imported — the third
   occurrence of this exact bug class. Every click on the Learning tab was a
   500. The end-to-end harness now loads this module and calls the action. */
const SH = require("./_investorShadow");
const AL = require("./_investorAllocator");
const V = require("./_investorVariants");
const LD = require("./_investorLadder");
const O = require("./_investorOpenai");

const MAX_BODY = 64 * 1024;

async function ctrlDoc() {
  const s = await A.col(A.COL.control).doc("control").get();
  return s.exists ? s.data() : {};
}

function latestCycleId(rows) {
  const ids = [...new Set(rows.map((r) => r.cycleId).filter(Boolean))].sort();
  return ids[ids.length - 1] || null;
}

/* ── actions ───────────────────────────────────────────────────────────── */
const ACTIONS = {

  /* The single call the dashboard polls. One round trip, everything on it. */
  async dashboard() {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const session = M.sessionState(new Date());

    const [candSnap, ordSnap, posSnap, regSnap, costSnap, runSnap] = await Promise.all([
      A.col(A.COL.candidates).orderBy("updated_at", "desc").limit(80).get(),
      A.col(A.COL.orders).where("status", "in", ["proposed", "approved"]).limit(40).get(),
      A.col(A.COL.positions).where("open", "==", true).limit(40).get(),
      A.col(A.COL.control).doc("regime").get(),
      A.col(A.COL.costs).doc(`openai_${new Date().toISOString().slice(0, 10)}`).get(),
      A.col(A.COL.runs).orderBy("startedAt", "desc").limit(8).get(),
    ]);

    const candidates = [];
    candSnap.forEach((d) => candidates.push(d.data()));
    const cycleId = latestCycleId(candidates);
    const current = candidates.filter((c) => c.cycleId === cycleId);

    const orders = []; ordSnap.forEach((d) => orders.push(d.data()));
    const positions = []; posSnap.forEach((d) => positions.push(d.data()));
    const runs = []; runSnap.forEach((d) => {
      const r = d.data();
      runs.push({ jobId: r.jobId, kind: r.kind, status: r.status, breaches: r.breaches,
                  proposals: r.proposals, modelCalls: r.modelCalls, elapsedMs: r.elapsedMs,
                  startedAt: r.startedAt && r.startedAt.toDate ? r.startedAt.toDate().toISOString() : null });
    });

    let balances = null, cost = null;
    try { balances = await L.balances(accountId); } catch {}
    try { cost = await L.costMeter(accountId); } catch {}

    const reg = regSnap.exists ? regSnap.data() : {};
    const openai = costSnap.exists ? costSnap.data() : { usd: 0, calls: 0 };

    return {
      ok: true,
      now: new Date().toISOString(),
      session,
      control: {
        enabled: ctrl.enabled !== false,
        mode: ctrl.mode || "research",
        dryRun: ctrl.dryRun !== false,
        killSwitch: !!ctrl.killSwitch,
        accountId,
        strategyVersion: ctrl.strategyVersion || "v1",
        cycleSeconds: ctrl.cycleSeconds || 300,
        lastCycleFinishedAt: ctrl.lastCycleFinishedAt && ctrl.lastCycleFinishedAt.toDate
          ? ctrl.lastCycleFinishedAt.toDate().toISOString() : null,
        lastCycleSummary: ctrl.lastCycleSummary || null,
      },
      market: {
        provider: M.activeProvider().id,
        feedDelayMinutes: M.activeProvider().delayMinutes,
        maxGrade: M.activeProvider().maxGrade,
      },
      regime: {
        vix: reg.vix || null, vixMedian: reg.vixMedian || null,
        vixNorm: reg.vix && reg.vixMedian ? Number((reg.vix / reg.vixMedian).toFixed(2)) : null,
        cor3m: reg.cor3m || null,
        gate: S.dispersionGate(Number(reg.cor3m), {}),
        asOf: reg.asOf || null,
      },
      bootstrap: {
        version: ctrl.bootstrapVersion || null,
        at: ctrl.bootstrappedAt && ctrl.bootstrappedAt.toDate ? ctrl.bootstrappedAt.toDate().toISOString() : null,
        report: ctrl.bootstrapReport || null,
      },
      cycleId,
      candidates: current.sort((a, b) => (a.rank ?? 1) - (b.rank ?? 1)),
      breaches: current.filter((c) => c.breach),
      orders, positions,
      balances, cost,
      openaiToday: { usd: Number((openai.usd || 0).toFixed(4)), calls: openai.calls || 0,
                     ceiling: O.DAILY_USD_CEILING },
      runs,
    };
  },

  /* The learning view: what each variant has done and how much of the book
     it currently earns. */
  async learning() {
    /* The dashboard reports THE STORED VERDICT — what the cycle actually
       computed and acted on — never a fresh recompute with different
       parameters. The old version recomputed with allocator defaults, so a
       strategy-file change would have made the dashboard silently disagree
       with the cycle about who was winning and whether the gate was open. */
    const [stats, open, allocSnap] = await Promise.all([
      SH.variantStats({}), SH.openCount(),
      A.col(A.COL.control).doc("allocation").get(),
    ]);
    const stored = allocSnap.exists ? allocSnap.data() : null;
    let roll = null;
    try { roll = await SH.rollUpStats(); } catch {}

    const fallback = (() => {
      const alloc = AL.allocate(stats.variants, {});
      return AL.explain(alloc, stats.variants);
    })();
    const view = stored || fallback;

    return {
      ok: true,
      variants: V.VARIANTS.map((v) => ({ id: v.id, name: v.name, plain: v.plain, params: v.params })),
      variantsHash: V.variantsHash(),
      storedHash: stored ? stored.variantsHash : null,
      rows: view.rows || fallback.rows,
      powered: !!view.powered,
      leaderId: view.leaderId || null,
      requiredEffectiveN: view.requiredEffectiveN ?? fallback.requiredEffectiveN,
      bestEffectiveN: view.bestEffectiveN ?? fallback.bestEffectiveN,
      progressPct: view.progressPct ?? fallback.progressPct,
      note: view.note || fallback.note,
      paired: view.paired || null,
      resets: (view.resets && Object.keys(view.resets).length ? view.resets : null)
           || (roll && Object.keys(roll.resets).length ? roll.resets : null),
      discountGamma: view.discountGamma ?? (roll ? roll.discountGamma : null),
      totalClosed: stats.totalClosed,
      openShadow: open,
    };
  },

  /* Every knob, in plain English, with what raising or lowering it does. */
  async knobs() {
    const st = require("./_investorStrategy.js");
    const p = st.parameters;
    const K = (key, label, what, higher, lower) => ({
      key, label, value: p[key], what, higher, lower,
    });
    return { ok: true, strategyVersion: st.version, memory: st.memory || null, groups: [
      { group: "When to buy", items: [
        K("entryRank", "How oversold before we look",
          "A stock must be in the bottom 10% of today's ranking, after stripping out what its sector did.",
          "0.20 = act on milder drops, many more trades, weaker case each",
          "0.05 = only the worst drops, far fewer trades"),
        K("minAbsZ", "How unusual the drop must be",
          "How far below its own normal range the stock has moved. 2.0 means roughly a 1-in-40 move.",
          "3.0 = only extreme moves", "1.5 = ordinary dips also qualify"),
      ]},
      { group: "When to sell", items: [
        K("exitRank", "When we let go",
          "Hold until the stock climbs back to the middle of the pack. This one rule was worth more than any entry change in the research.",
          "0.65 = wait for a fuller recovery, longer holds",
          "0.35 = take profit earlier, shorter holds"),
        K("maxHoldDays", "Longest we will hold",
          "A hard stop on time. If the bounce has not come by then, we are wrong and we leave.",
          "more patience, more exposure", "faster turnover, more cost"),
      ]},
      { group: "Safety", items: [
        K("blackoutDays", "Days avoided around a known earnings date",
          "Earnings can move a stock 20% overnight. We simply do not hold through them.",
          "safer, fewer opportunities", "riskier, more opportunities"),
        K("blackoutDaysEstimated", "Days avoided around an ESTIMATED earnings date",
          "Earnings dates are projected from each company's filing rhythm, so the window is wider to absorb the guess.",
          "safer", "riskier - the estimate can be several days off"),
        K("minAdvUsd", "Minimum daily trading activity",
          "Thinly traded stocks cost far more to get in and out of. This blocks them.",
          "only the most liquid names, lower costs", "more names, higher costs"),
        K("sectorCrowdingMultiple", "Sector crowding limit",
          "If most of a sector is falling together, that is a sector event, not an opportunity in one company.",
          "more tolerant of sector-wide moves", "stricter, fewer correlated bets"),
      ]},
      { group: "Sizing", items: [
        K("positionPctOfNav", "Size of a normal position",
          "Share of the account put into one idea before any scaling down.",
          "bigger swings both ways", "smaller, slower"),
        K("volScalerFloor", "Smallest size in calm markets",
          "This strategy is paid for providing liquidity, and there is less of that to earn when markets are quiet.",
          "keeps more on in calm periods", "backs off harder"),
        K("volScalerCeiling", "Largest size in volatile markets",
          "The opposite end - how much more to commit when volatility is high.",
          "more aggressive in stress", "more restrained"),
        K("corStandDown", "Stop trading when stocks move as one",
          "When everything moves together there is no company-specific opportunity left to find.",
          "keeps trading in correlated markets", "stands down sooner"),
      ]},
      { group: "Cost discipline", items: [
        K("costMarginMultiple", "Required reward vs cost",
          "An idea must expect at least this many times its own trading cost before we act.",
          "much stricter, far fewer trades", "looser, more trades that may not cover costs"),
        K("decayHaircut", "Discount applied to expectations",
          "Published edges shrink by roughly half once known. We assume ours does too.",
          "more optimistic", "more conservative"),
        K("reversionCapture", "How much of the bounce we expect to catch",
          "We do not assume a full recovery - only a fraction of it.",
          "more optimistic", "more conservative"),
      ]},
      { group: "History and memory", items: [
        K("turnoverPctileCap", "Skip the most heavily traded names",
          "Reversal only works in names that are NOT being traded furiously. In the top tenth by turnover, falling stocks tend to keep falling rather than bounce (Review of Financial Studies, 2022) — so those are refused.",
          "0.95 = only the very busiest names are skipped",
          "0.80 = skip the busiest fifth, fewer but safer trades"),
        K("postEarningsDays", "Days avoided AFTER an earnings report",
          "A drop just after earnings usually reflects real information, and such moves keep drifting in the same direction for days. Fading them is the classic way a bounce strategy loses money.",
          "safer, misses some genuine overreactions",
          "riskier — trades against post-earnings drift"),
        K("blockDowntrends", "Refuse to buy dips in downtrends",
          "A stock below its 200-day average, more than 20% off its six-month high, with its 50-day line still falling, is not oversold — it is sliding. All three must be true before a name is called a downtrend.",
          "on = skip these names entirely (safer)",
          "off = treat a big drop the same whatever the six-month picture"),
        K("signalWindow", "How long a 'move' is",
          "How many 5-minute bars are added up to measure the drop. 12 bars is about an hour.",
          "smoother, slower to notice", "twitchier, more false alarms"),
      ]},
      { group: "Learning", items: [
        K("tHurdle", "Proof required before favouring a variant",
          "How certain we must be that a variant is genuinely better before giving it more money. 3.0 is the academic standard for this field.",
          "demands more proof, learns slower but more truthfully",
          "acts on weaker evidence - this is the knob that decides whether the system learns or fools itself"),
        K("expectedEdgeBps", "Edge we are looking for",
          "How big an effect we think exists, in hundredths of a percent. Used only to work out how much evidence is needed.",
          "assumes a bigger effect, needs less evidence", "assumes smaller, needs more"),
        K("perTradeSdBps", "How noisy a single trade is",
          "The spread of outcomes on any one trade. Bigger noise means more evidence needed.",
          "needs more evidence", "needs less"),
      ]},
    ]};
  },

  /* Full detail for one name — the per-candidate panel. */
  async candidate({ symbol, cycleId }) {
    if (!symbol) return { error: "symbol required" };
    let cid = cycleId;
    if (!cid) {
      /* where+orderBy on different fields needs a composite index that no
         config in this repo creates — in real Firestore this threw
         FAILED_PRECONDITION and the candidate panel 500'd. Equality-only
         query, newest picked in memory. */
      const s = await A.col(A.COL.candidates).where("symbol", "==", symbol).limit(40).get();
      if (s.empty) return { error: `no candidate record for ${symbol}` };
      let newest = null;
      s.forEach((d) => { const x = d.data(); if (!newest || String(x.cycleId) > String(newest.cycleId)) newest = x; });
      cid = newest.cycleId;
    }
    const [cSnap, pSnap] = await Promise.all([
      A.col(A.COL.candidates).doc(`${cid}_${symbol}`).get(),
      A.col(A.COL.positions).doc(`${(await ctrlDoc()).accountId || "paper-1"}_${symbol}`).get(),
    ]);
    if (!cSnap.exists) return { error: `no candidate record for ${symbol} in ${cid}` };

    const session = M.sessionState(new Date());
    let bars = [];
    try { bars = await M.readBars(symbol, session.date); } catch {}
    if (!bars.length) { try { bars = await M.readRecentBars(symbol, 2); } catch {} }

    const docs = [];
    try {
      const dsnap = await A.col(A.COL.documents).where("symbol", "==", symbol).limit(60).get();
      const dall = [];
      dsnap.forEach((d) => dall.push(d.data()));
      dall.sort((a, b) => String(b.first_seen_at || "") < String(a.first_seen_at || "") ? -1 : 1);
      for (const x of dall.slice(0, 12)) {
        docs.push({
          documentId: x.documentId, form: x.form, title: x.title, link: x.link,
          accession: x.accession,
          source_published_at: x.source_published_at,
          first_seen_at: x.first_seen_at && x.first_seen_at.toDate ? x.first_seen_at.toDate().toISOString() : null,
        });
      }
    } catch {}

    /* The six-month daily series, so the candidate view can show where today's
       move sits in the name's own longer story rather than only the last hour. */
    let daily = [], history = null, reversion = null;
    try {
      const series = await H.readDaily(symbol);
      daily = series.slice(-180).map((b) => ({ d: b.date, c: b.c, h: b.h, l: b.l, v: b.v }));
      const ctx = H.contextFor(series, session.date);
      if (ctx.ok) {
        history = ctx;
        const rev = H.reversionEvents(series, session.date);
        reversion = rev.n ? rev : null;
        history.notes = H.describe(ctx, (cSnap.data() || {}).reversion || null);
      } else {
        history = { ok: false, days: ctx.days, reason: ctx.reason };
      }
    } catch {}

    return { ok: true, candidate: cSnap.data(), bars, documents: docs,
             daily, history, reversion,
             position: pSnap.exists ? pSnap.data() : null };
  },

  /* Performance: the equity curve, how deep the worst hole got, and the one
     comparison that actually matters — did this beat holding the same names. */
  async performance() {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const navSnap = await A.col(A.COL.control).doc("control")
      .collection("navHistory").orderBy("date", "desc").limit(400).get();
    const nav = []; navSnap.forEach((d) => nav.push(d.data()));
    nav.reverse();   // fetched newest-first so the window slides; stats want oldest-first
    const equity = R.equityStats(nav);

    let bench = { ok: false, reason: "not enough daily history yet" };
    if (equity.ok) {
      const uMod = require("./_investorUniverse.js");
      const uSnap = await A.col(A.COL.universe).doc(ctrl.universeVersion || uMod.version || "v1").get();
      const u = uSnap.exists ? uSnap.data() : uMod;
      const syms = (u.tradeTier || []).map((t) => t.symbol).slice(0, 120);
      const daily = {};
      await Promise.all(syms.map(async (sym) => {
        try { const ser = await H.readDaily(sym); if (ser.length) daily[sym] = ser; } catch {}
      }));
      bench = R.benchmarkReturn(daily, nav[0].date, nav[nav.length - 1].date);
    }

    const book = await (async () => {
      const ps = await A.col(A.COL.positions).where("accountId", "==", accountId).get();
      const rows = []; ps.forEach((d) => rows.push(d.data()));
      const b = await L.balances(accountId);
      const marks = {};
      return R.summarise(rows, marks, null, (b.usd[L.ACCT.CASH] || 0) + (b.usd[L.ACCT.POSITIONS] || 0) || 1);
    })();

    return {
      ok: true, equity, benchmark: bench,
      attribution: R.attribution(equity, bench),
      navHistory: nav.map((n) => ({ date: n.date, navUsd: n.navUsd })),
      book: { count: book.count, grossPct: book.grossPct, byClusterPct: book.byClusterPct },
      costMeter: await L.costMeter(accountId).catch(() => null),
    };
  },

  /* Standalone history read, so the dashboard can show the six-month picture
     for any roster name even before it has ever become a candidate. */
  async history({ symbol }) {
    if (!symbol) return { error: "symbol required" };
    const session = M.sessionState(new Date());
    const series = await H.readDaily(symbol);
    if (!series.length) return { ok: true, symbol, daily: [], history: null,
      note: "no daily history stored for this name yet — the backfill runs on the next cycle" };
    const ctx = H.contextFor(series, session.date);
    const rev = H.reversionEvents(series, session.date);
    return {
      ok: true, symbol,
      daily: series.slice(-180).map((b) => ({ d: b.date, c: b.c, h: b.h, l: b.l, v: b.v })),
      history: ctx.ok ? ctx : { ok: false, days: ctx.days, reason: ctx.reason },
      reversion: rev.n ? rev : null,
      notes: H.describe(ctx, null),
    };
  },

  async approve({ orderId, operator }) {
    if (!orderId) return { error: "orderId required" };
    const r = await L.approveOrder(orderId, operator || "operator");
    await A.col(A.COL.audit).add({ action: "approve", orderId, operator: operator || "operator",
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    return r;
  },

  /* Approved orders whose symbol stops returning bars sit forever with cash
     debited into RESERVED and no way back — settlement skips them silently and
     nothing expires them. This releases them. */
  async expireStaleOrders({ olderThanHours = 24 } = {}) {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const cutoff = Date.now() - olderThanHours * 3600e3;
    const snap = await A.col(A.COL.orders)
      .where("accountId", "==", accountId).where("status", "==", "approved").limit(100).get();
    const released = [];
    for (const d of snap.docs) {
      const o = d.data();
      const at = o.decisionAtMs || Date.parse(o.created_at) || 0;
      if (at && at > cutoff) continue;
      try {
        const r = await L.releaseOrder(o.orderId, "expired — never became fillable");
        if (!r.noop) released.push({ orderId: o.orderId, symbol: o.symbol });
      } catch (e) { /* skip */ }
    }
    return { ok: true, released: released.length, detail: released.slice(0, 20) };
  },

  async reject({ orderId, reason, operator }) {
    if (!orderId) return { error: "orderId required" };
    const r = await L.rejectOrder(orderId, reason, operator || "operator");
    await A.col(A.COL.audit).add({ action: "reject", orderId, reason: reason || null,
      operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return r;
  },

  /* Kill switch cancels proposed and unfilled orders and blocks new decisions.
     It does NOT delete evidence, history, or the ledger. */
  async kill({ operator }) {
    await A.col(A.COL.control).doc("control").set({
      killSwitch: true, enabled: false, killedBy: operator || "operator",
      killedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    const snap = await A.col(A.COL.orders).where("status", "==", "proposed").limit(200).get();
    const b = A.batch();
    snap.forEach((d) => b.set(d.ref, { status: "cancelled", cancelReason: "kill switch" }, { merge: true }));
    await b.commit();

    /* APPROVED orders must be released too, not only proposed ones. An
       approved order holds cash in RESERVED and would still FILL after a
       resume — a kill switch that leaves live buy orders armed is not a kill
       switch. releaseOrder returns the reserved cash through the ledger. */
    let released = 0;
    const appSnap = await A.col(A.COL.orders).where("status", "==", "approved").limit(200).get();
    for (const d of appSnap.docs) {
      try { const r = await L.releaseOrder(d.data().orderId, "kill switch"); if (!r.noop) released += 1; }
      catch (e) { /* an unreleasable order is left for expiry */ }
    }

    await A.col(A.COL.audit).add({ action: "kill", cancelled: snap.size, released,
      operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { killSwitch: true, cancelledProposals: snap.size, releasedApproved: released };
  },

  async resume({ operator }) {
    await A.col(A.COL.control).doc("control").set({
      killSwitch: false, enabled: true, resumedBy: operator || "operator",
      resumedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    return { killSwitch: false, enabled: true };
  },

  async setControl({ patch, operator }) {
    /* highWaterMarkUsd was NOT settable, and accountBreakers halts all new
       entries at -6% from it. Once the account dipped, the only ways out were
       recovering the NAV or hand-editing Firestore — a genuine wedge with no
       UI escape. operatorCeiling/operatorHold steer the ladder; recallBenchmark
       records the one measurement that blocks full automation. */
    const ALLOW = ["mode", "dryRun", "cycleSeconds", "evidenceEverySeconds",
                   "highWaterMarkUsd", "operatorCeiling", "operatorHold",
                   "recallBenchmark", "afterHoursCycles", "forceBootstrap",
                   "accountId", "strategyVersion", "universeVersion"];
    const MODES = ["research", "approval", "shadow", "limited_auto"];
    const clean = {};
    for (const k of ALLOW) {
      if (patch && patch[k] !== undefined) {
        if (k === "mode" && !MODES.includes(patch[k])) continue;
        if (k === "operatorCeiling" && !MODES.includes(patch[k])) continue;
        /* A manual stage set is recorded as such so the ladder does not
           immediately undo an operator's deliberate choice without saying so. */
        if (k === "mode") { clean.stageChangedBy = "operator"; clean.stageChangedAt = A.FV.serverTimestamp(); }
        /* Raising the ceiling is the operator's consent for the ladder to
           climb again — it clears a manual stage pin. */
        if (k === "operatorCeiling") { clean.stageChangedBy = "ladder_enabled"; clean.ladderStreak = 0; }
        if (k === "cycleSeconds") { const n = Number(patch[k]); if (n >= 60 && n <= 3600) clean[k] = n; continue; }
        if (k === "evidenceEverySeconds") { const n = Number(patch[k]); if (n >= 300 && n <= 86400) clean[k] = n; continue; }
        if (k === "dryRun" || k === "afterHoursCycles") { clean[k] = !!patch[k]; continue; }
        clean[k] = patch[k];
      }
    }
    if (Object.keys(clean).length) {
      await A.col(A.COL.control).doc("control").set(clean, { merge: true });
      await A.col(A.COL.audit).add({ action: "setControl", patch: clean,
        operator: operator || "operator", at: A.FV.serverTimestamp(),
        ...A.envelope({ created_by: "investorApi" }) });
    }
    return { patched: clean };
  },

  /* Regime inputs. Entered by the operator or written by a future adapter —
     COR3M has no free programmatic feed, so a manual value beats a fabricated
     one, and a stale value degrades sizing rather than silently passing. */
  async setRegime({ vix, vixMedian, cor3m, operator }) {
    const patch = { asOf: new Date().toISOString(), setBy: operator || "operator" };
    if (isFinite(Number(vix))) patch.vix = Number(vix);
    if (isFinite(Number(vixMedian))) patch.vixMedian = Number(vixMedian);
    if (isFinite(Number(cor3m))) patch.cor3m = Number(cor3m);
    await A.col(A.COL.control).doc("regime").set(patch, { merge: true });
    return { regime: patch, gate: S.dispersionGate(patch.cor3m, {}) };
  },

  async openAccount({ accountId, startingNavUsd }) {
    const ctrl = await ctrlDoc();
    const id = accountId || ctrl.accountId || "paper-1";
    const r = await L.openAccount({
      accountId: id,
      startingNavUsd: Number(startingNavUsd) || 100000,
      strategyVersion: ctrl.strategyVersion || "v1",
    });
    await A.col(A.COL.control).doc("control").set({ accountId: id }, { merge: true });
    return r;
  },

  async ledger({ accountId, limit }) {
    const ctrl = await ctrlDoc();
    const id = accountId || ctrl.accountId || "paper-1";
    /* Equality-only, sorted in memory — where+orderBy needs a composite index
       that does not exist and threw FAILED_PRECONDITION in real Firestore. */
    const snap = await A.col(A.COL.ledger).where("accountId", "==", id).limit(600).get();
    const rows = [];
    const lim = Math.min(Number(limit) || 50, 200);
    const all = [];
    snap.forEach((d) => all.push(d.data()));
    all.sort((a, b) => String(b.postedAt || "") < String(a.postedAt || "") ? -1 : 1);
    for (const x of all.slice(0, lim)) {
      rows.push({ txnId: x.txnId, kind: x.kind, legs: x.legs, meta: x.meta,
                  postedAt: x.postedAt && x.postedAt.toDate ? x.postedAt.toDate().toISOString() : null });
    }
    return { ok: true, accountId: id, rows,
             balances: await L.balances(id), cost: await L.costMeter(id) };
  },

  /* No-trade reasons are the most valuable dataset this system produces. */
  async decisions({ limit, cycleId }) {
    let q = A.col(A.COL.decisions);
    q = cycleId ? q.where("cycleId", "==", cycleId) : q.orderBy("decisionAtMs", "desc");
    const snap = await q.limit(Math.min(Number(limit) || 60, 250)).get();
    const rows = []; snap.forEach((d) => rows.push(d.data()));
    const tally = {};
    for (const r of rows) for (const b of (r.noTradeReasons || [])) tally[b] = (tally[b] || 0) + 1;
    return { ok: true, rows, blockTally: tally };
  },

  async sources() {
    const snap = await A.col(A.COL.sourceState).limit(60).get();
    const state = {}; snap.forEach((d) => {
      const x = d.data();
      state[d.id] = {
        consecutiveFailures: x.consecutiveFailures || 0,
        lastError: x.lastError || null,
        lastSuccessAt: x.lastSuccessAt && x.lastSuccessAt.toDate ? x.lastSuccessAt.toDate().toISOString() : null,
      };
    });
    return { ok: true, registry: E.SOURCES, state };
  },

  async universe() {
    const ctrl = await ctrlDoc();
    const uMod = require("./_investorUniverse.js");
    const snap = await A.col(A.COL.universe).doc(ctrl.universeVersion || uMod.version || "v1").get();
    if (snap.exists) return { ok: true, universe: snap.data(), source: "firestore" };
    return { ok: true, universe: require("./_investorUniverse.js"), source: "repo" };
  },

  /* Freeze a universe version so additions can never be backdated. */
  async freezeUniverse({ operator }) {
    const u = require("./_investorUniverse.js");
    const ref = A.col(A.COL.universe).doc(u.version);
    const existing = await ref.get();
    if (existing.exists) return { ok: true, frozen: false, note: `${u.version} already frozen` };
    await ref.set({ ...u, frozenAt: A.FV.serverTimestamp(), frozenBy: operator || "operator",
      ...A.envelope({ created_by: "investorApi.freezeUniverse" }) });
    return { ok: true, frozen: true, version: u.version, tradeTier: (u.tradeTier || []).length };
  },

  /* Resolve tickers to CIKs from SEC's own authoritative map. A guessed CIK
     silently polls the WRONG filer, which is worse than no CIK at all — so the
     universe ships with them null and this fills them in from the source. */
  async resolveCiks({ operator }) {
    const { fetchPublic } = require("./_investorFetch");
    const r = await fetchPublic("https://www.sec.gov/files/company_tickers.json", {
      sourceId: "sec.tickers", accept: ["json"], timeoutMs: 20000,
    });
    if (!r.json) return { error: "could not parse SEC company_tickers.json" };
    const map = {};
    for (const v of Object.values(r.json)) {
      if (v && v.ticker) map[String(v.ticker).toUpperCase()] = String(v.cik_str).padStart(10, "0").replace(/^0+/, "");
    }
    const u = require("./_investorUniverse.js");
    const resolved = [], mismatched = [], missing = [];
    const apply = (row) => {
      const got = map[row.symbol];
      if (!got) { missing.push(row.symbol); return row; }
      if (row.cik && row.cik !== got) {
        mismatched.push({ symbol: row.symbol, had: row.cik, sec: got });
      }
      resolved.push({ symbol: row.symbol, cik: got });
      return { ...row, cik: got, cikSource: "sec_company_tickers" };
    };
    const next = {
      ...u,
      tradeTier: (u.tradeTier || []).map(apply),
      researchTier: (u.researchTier || []).map((r2) => {
        const got = map[r2.symbol];
        return got ? { ...r2, cik: got, cikSource: "sec_company_tickers" } : r2;
      }),
      cikResolvedAt: new Date().toISOString(),
    };
    await A.col(A.COL.universe).doc(u.version).set({
      ...next, frozenAt: A.FV.serverTimestamp(), frozenBy: operator || "operator",
      ...A.envelope({ created_by: "investorApi.resolveCiks" }),
    }, { merge: true });
    return { ok: true, resolved: resolved.length, missing, mismatched,
             note: mismatched.length ? "MISMATCHES FOUND — the manual CIK was wrong and has been corrected from SEC" : "all manual CIKs agreed with SEC" };
  },

  async health() {
    const p = M.activeProvider();
    return {
      ok: true,
      provider: { id: p.id, delayMinutes: p.delayMinutes, maxGrade: p.maxGrade,
                  degradedFrom: p.degradedFrom || null, reason: p.reason || null },
      env: {
        firebase: !!(process.env.FIREBASE_PRIVATE_KEY && process.env.FIREBASE_PROJECT_ID),
        openai: !!process.env.OPENAI_API_KEY,
        passcode: !!process.env.INVESTOR_PASSCODE,
        marketProvider: process.env.INVESTOR_MARKET_PROVIDER || "(unset — manual)",
      },
      session: M.sessionState(new Date()),
      openaiCeilingUsd: O.DAILY_USD_CEILING,
    };
  },
};

/* ── handler ───────────────────────────────────────────────────────────── */
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: AUTH.corsHeaders(event), body: "" };
  }
  if (event.httpMethod !== "POST") {
    return AUTH.json(event, 405, { error: "POST only" });
  }
  if ((event.body || "").length > MAX_BODY) {
    return AUTH.json(event, 413, { error: "request too large" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return AUTH.json(event, 400, { error: "invalid JSON body" }); }

  const guard = AUTH.requireOperator(event, body);
  if (!guard.ok) return guard.response;

  const action = String(body.action || "dashboard");
  const fn = ACTIONS[action];
  if (!fn) return AUTH.json(event, 400, { error: `unknown action "${action}"` });

  try {
    const out = await fn(body);
    /* The auth token is returned as authToken, NEVER as "session".
       The dashboard action already returns `session` meaning the MARKET
       session state, and the collision meant that on the second request the
       client overwrote its bearer token with a market-state object, sent
       "[object Object]" as the header, and locked itself out. */
    const extra = guard.session ? { authToken: guard.session } : {};
    return AUTH.json(event, 200, { ...out, ...extra });
  } catch (e) {
    console.error("investorApi", action, AUTH.redact({ error: e.message, stack: (e.stack || "").slice(0, 300) }));
    return AUTH.json(event, 500, { error: String(e.message).slice(0, 200), action });
  }
};

exports.ACTIONS = ACTIONS;
