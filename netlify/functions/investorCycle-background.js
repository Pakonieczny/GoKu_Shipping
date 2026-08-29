/*  netlify/functions/investorCycle-background.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the cycle worker. Background function, 15-minute ceiling.
 *
 *  THE INVERTED PIPELINE. The order of these steps is the cost architecture:
 *
 *    1. pull bars for the whole trade tier            (one provider call)
 *    2. compute residual returns across the panel     (pure maths, free)
 *    3. rank cross-sectionally, find threshold breaches
 *    4. ONLY for breaches: look for a cause in the evidence lanes
 *    5. ONLY if still ambiguous: ask the model
 *    6. run the gate stack, propose or record a no-trade decision
 *    7. evaluate open positions against the buy/hold exit rule
 *
 *  Steps 1-3 cost nothing but a data call. Step 5 is the expensive one and is
 *  reached by a handful of names per cycle instead of the whole roster, which
 *  is the difference between ~$285/month and ~$40/month.
 *
 *  IDEMPOTENCE. Netlify background functions return 202 immediately and retry
 *  after failure. Every write below is either deterministic-id (ledger, fills)
 *  or a merge on a stable key (candidates keyed by cycle+symbol), so a retry
 *  re-derives the same state rather than duplicating it.
 *
 *  This worker NEVER places a real order. It writes proposals; a human
 *  approves them in the console, or in a later phase a validated book may
 *  auto-approve within its own caps. There is no broker integration.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const { verifyWorkerNonce, redact } = require("./_investorAuth");
const M = require("./_investorMarket");
const S = require("./_investorSignal");
const E = require("./_investorEvidence");
const O = require("./_investorOpenai");
const L = require("./_investorLedger");

const FN_NAME = "investorCycle-background";

/* ── universe + strategy loading ───────────────────────────────────────── */
async function loadUniverse(version) {
  const snap = await A.col(A.COL.universe).doc(version || "v1").get();
  if (snap.exists) return snap.data();
  const fallback = require("./_investorUniverse.js");
  return fallback;
}

async function loadStrategy(version) {
  const snap = await A.col(A.COL.strategies).doc(version || "v1").get();
  if (snap.exists) return snap.data();
  return require("./_investorStrategy.js");
}

async function controlDoc() {
  const s = await A.col(A.COL.control).doc("control").get();
  return s.exists ? s.data() : {};
}

/* ── regime inputs ─────────────────────────────────────────────────────── */
/* VIX and COR3M drive sizing and the stand-down gate. Both are cached in
   Firestore by the evidence sweep; the cycle reads whatever is current and
   degrades to reduced size rather than failing when they are stale. */
async function regime() {
  const s = await A.col(A.COL.control).doc("regime").get();
  const d = s.exists ? s.data() : {};
  return {
    vix: Number(d.vix) || null,
    vixMedian: Number(d.vixMedian) || 18,
    vixNorm: d.vix && d.vixMedian ? d.vix / d.vixMedian : 1,
    cor3m: Number(d.cor3m) || NaN,
    asOf: d.asOf || null,
    stale: !d.asOf || (Date.now() - Date.parse(d.asOf)) > 36 * 3600e3,
  };
}

/* ── attention proxy ───────────────────────────────────────────────────── */
/* A true retail-order-flow measure needs TAQ, which is paid and out of scope.
   The tradable proxy the research supports is abnormal VOLUME against the
   symbol's own baseline: attention spikes show up as volume the price move
   alone does not justify. Named honestly so nobody mistakes it for
   Robinhood-style holdings data. */
function attentionZ(bars, window = 12) {
  if (!bars || bars.length < window + 24) return 0;
  const vols = bars.map((b) => b.v || 0);
  const recent = vols.slice(-window);
  const base = vols.slice(0, -window);
  const mu = S.mean(base), sd = S.stdev(base);
  if (!(sd > 0)) return 0;
  return (S.mean(recent) - mu) / sd;
}

/* ── main cycle ────────────────────────────────────────────────────────── */
async function runCycle(jobId) {
  const startedAt = Date.now();
  const ctrl = await controlDoc();
  const strategy = await loadStrategy(ctrl.strategyVersion);
  const universe = await loadUniverse(ctrl.universeVersion);
  const cfg = strategy.parameters || {};
  const accountId = ctrl.accountId || "paper-1";
  const session = M.sessionState(new Date());
  const reg = await regime();

  const tradeTier = universe.tradeTier || [];
  const symbols = tradeTier.map((t) => t.symbol);

  const runRef = A.col(A.COL.runs).doc(jobId);
  await runRef.set({
    jobId, kind: "cycle", startedAt: A.FV.serverTimestamp(),
    strategyVersion: strategy.version, universeVersion: universe.version,
    session, regime: reg, symbolCount: symbols.length,
    ...A.envelope({ created_by: FN_NAME }),
  }, { merge: true });

  /* 1. bars ------------------------------------------------------------- */
  const provider = M.activeProvider();
  let panel = {}, fetchMeta = {};
  try {
    const got = await M.fetchBars(symbols, { timeframe: cfg.barTimeframe || "5Min", limit: 120 });
    panel = got.bars || {};
    fetchMeta = { provider: got.provider, fetchedAt: got.fetchedAt, note: got.note || null };
  } catch (e) {
    fetchMeta = { provider: provider.id, error: String(e.code || e.message).slice(0, 160) };
  }

  // Fall back to stored bars for any symbol the provider did not return.
  for (const sym of symbols) {
    if (!panel[sym] || panel[sym].length < 20) {
      try { const stored = await M.readRecentBars(sym, 2); if (stored.length) panel[sym] = stored; } catch {}
    }
  }

  // Persist bars (one doc per symbol per day, bar array inside).
  const today = session.date;
  const quality = {};
  await Promise.all(symbols.map(async (sym) => {
    const bars = panel[sym] || [];
    quality[sym] = M.gradeSeries(bars, { provider: fetchMeta.provider || provider.id });
    if (bars.length) {
      try {
        await M.writeBars(sym, today, bars, {
          provider: fetchMeta.provider || provider.id,
          grade: quality[sym].grade,
          gradeReasons: quality[sym].reasons,
          feedDelayMinutes: quality[sym].feedDelayMinutes,
        });
      } catch (e) { /* storage failure must not kill the cycle */ }
    }
  }));

  /* 2 + 3. residuals and ranks ------------------------------------------ */
  const tradable = {};
  for (const sym of symbols) if ((panel[sym] || []).length >= 24) tradable[sym] = panel[sym];
  const rp = S.residualPanel(tradable);

  const zBySymbol = {};
  for (const sym of rp.symbols) {
    const z = S.residualZ(rp.residuals[sym], cfg.signalWindow || 12);
    if (z) zBySymbol[sym] = z;
  }
  const { ranks, n: ranked } = S.crossSectionalRanks(zBySymbol);
  const crowd = S.sectorCrowding(ranks, S.sectorOf, cfg.entryRank ?? S.ENTRY_RANK);

  /* 4-6. evaluate each symbol ------------------------------------------- */
  const cycleId = `${today}_${new Date().toISOString().slice(11, 16).replace(":", "")}`;
  const candidates = [], decisions = [], exits = [];
  let modelCalls = 0;

  const tierBySymbol = Object.fromEntries(tradeTier.map((t) => [t.symbol, t]));

  for (const sym of rp.symbols) {
    const meta = tierBySymbol[sym] || {};
    const bars = panel[sym] || [];
    const last = bars[bars.length - 1];
    if (!last) continue;

    const posSnap = await A.col(A.COL.positions).doc(`${accountId}_${sym}`).get();
    const position = posSnap.exists ? posSnap.data() : null;

    /* --- exits first: an open position is evaluated every cycle, which is
       the single highest-value use of the 5-minute loop. The research is
       explicit that the exit rule was worth more than any entry refinement. */
    if (position && position.open) {
      const heldDays = (Date.now() - Date.parse(position.openedAt)) / 864e5;
      const ex = S.exitSignal(ranks[sym] ?? 0.5, heldDays, cfg);
      if (ex.exit) {
        const eligible = M.firstEligibleBar(bars, {
          decisionAtMs: Date.now(), provider: fetchMeta.provider || provider.id,
          executionLatencyMs: cfg.executionLatencyMs || 60000,
        });
        exits.push({
          symbol: sym, reason: ex.reason, rank: ranks[sym], heldDays: Number(heldDays.toFixed(2)),
          eligible: !!eligible.bar, barOpenAt: eligible.barOpenAt,
        });
        await A.col(A.COL.decisions).doc(`${cycleId}_${sym}_exit`).set({
          cycleId, symbol: sym, kind: "exit_signal", reason: ex.reason,
          rank: ranks[sym], heldDays, eligibleBarAt: eligible.barOpenAt,
          strategyVersion: strategy.version,
          ...A.envelope({ created_by: FN_NAME }),
        }, { merge: true });
      }
      continue;   // never propose an entry in a name we already hold
    }

    /* --- entries: only threshold breaches proceed past this line --------- */
    const z = zBySymbol[sym];
    const rank = ranks[sym];
    const breach = z && rank <= (cfg.entryRank ?? S.ENTRY_RANK) && z.z <= -(cfg.minAbsZ ?? 2.0);

    let cause = S.CAUSE.PENDING, causeDetail = null, coverage = null;

    if (breach) {
      /* 4. a breach earns an evidence lookup. Nothing else does. */
      const att = attentionZ(bars, cfg.signalWindow || 12);
      let freshDocs = [];
      try {
        if (meta.cik) {
          const polled = await E.pollEdgar(meta.cik, { forms: "" });
          freshDocs = polled.entries || [];
          for (const d of freshDocs.slice(0, 5)) {
            try { await E.recordVersion({ symbol: sym, sourceId: "sec.latest", entry: d, rawSha256: polled.sha256 }); } catch {}
          }
        }
      } catch (e) { /* evidence lane failure degrades to "pending", never crashes */ }

      const pre = E.preClassify({
        symbol: sym, freshDocs, residualZ: z.z, attentionScore: att,
        hoursSinceMove: (cfg.signalWindow || 12) * 5 / 60,
      });
      cause = pre.cause; causeDetail = pre;

      /* 5. escalate to the model ONLY when deterministic rules are ambiguous */
      if (pre.needsModel && modelCalls < O.CYCLE_CALL_CEILING) {
        modelCalls += 1;
        const llm = await O.classifyMove({
          symbol: sym, role: "classify", documents: freshDocs,
          moveSummary: `residual ${(z.cumResidual * 100).toFixed(2)}% over ${z.window} bars, z ${z.z.toFixed(2)}, cross-sectional rank ${rank.toFixed(3)}`,
          attentionScore: att,
        });
        if (llm.ok) { cause = llm.cause; causeDetail = { ...pre, model: llm }; }
      }

      try { coverage = meta.cik ? await E.coverageRoster(sym, meta.cik) : null; } catch {}
    }

    /* 6. the gate stack runs for every ranked symbol, so the dashboard can
       show why a name did NOT trade as readily as why one did. */
    const evalRes = S.evaluateCandidate({
      symbol: sym, rank, zStat: z,
      quality: quality[sym], advUsd: meta.advUsd || 0,
      earningsDates: meta.earningsDates || [],
      nowMs: Date.now(), cause,
      vixNorm: reg.vixNorm, cor3m: reg.cor3m,
      sectorTailFraction: crowd.fractionInTail[S.sectorOf(sym)] ?? 0,
      session, cfg, position: null,
    });

    const card = {
      cycleId, symbol: sym, company: meta.company || sym,
      sector: S.sectorOf(sym), tier: meta.tier || "trade",
      lastPrice: last.c, lastBarAt: last.t,
      rank, z: evalRes.z, cumResidualBps: evalRes.cumResidualBps,
      betas: rp.betas[sym] || null,
      quality: quality[sym],
      attentionZ: Number(attentionZ(bars, cfg.signalWindow || 12).toFixed(2)),
      cause, causeDetail: causeDetail ? redact(causeDetail) : null,
      coverage,
      gates: evalRes.gates,
      pass: evalRes.pass, blockedBy: evalRes.blockedBy, firstBlock: evalRes.firstBlock,
      cost: evalRes.cost, sizing: evalRes.sizing,
      breach: !!breach,
      sectorTailFraction: crowd.fractionInTail[S.sectorOf(sym)] ?? 0,
      strategyVersion: strategy.version,
      updated_at: A.FV.serverTimestamp(),
    };

    await A.col(A.COL.candidates).doc(`${cycleId}_${sym}`).set(card, { merge: true });
    if (breach) candidates.push(card);

    /* Every outcome is a decision record — the rejections are the dataset. */
    await A.col(A.COL.decisions).doc(`${cycleId}_${sym}`).set({
      cycleId, symbol: sym, kind: evalRes.pass ? "propose" : "no_trade",
      noTradeReasons: evalRes.blockedBy, firstBlock: evalRes.firstBlock,
      rank, z: evalRes.z, cause, cost: evalRes.cost, sizing: evalRes.sizing,
      strategyVersion: strategy.version, decisionAtMs: Date.now(),
      ...A.envelope({ created_by: FN_NAME }),
    }, { merge: true });

    /* Propose an order when every gate passed. In dry-run or research mode
       this writes the proposal and stops; approval is a separate human act. */
    if (evalRes.pass && !ctrl.dryRun && ctrl.mode !== "research") {
      try {
        const navSnap = await L.balances(accountId);
        const cashUsd = (navSnap.usd[L.ACCT.CASH] || 0);
        const baseRisk = (cfg.positionPctOfNav ?? 0.03);
        const sized = cashUsd * baseRisk * evalRes.sizing.combined;
        const qty = Math.max(0, Math.floor(sized / last.c));
        if (qty > 0) {
          const slip = M.slippageBps({
            advUsd: meta.advUsd || 0, grade: quality[sym].grade,
            wideSpreadWindow: session.wideSpreadWindow, vixNorm: reg.vixNorm,
          });
          const o = await L.proposeOrder({
            accountId, symbol: sym, side: "buy",
            decisionId: `${cycleId}_${sym}`, strategyVersion: strategy.version,
            qty, refPriceUsd: last.c, slippageBps: slip,
            sizing: evalRes.sizing, gates: evalRes.gates, cause,
            evidenceRefs: (causeDetail && causeDetail.fundamentalDocs) || [],
            decisionAtMs: Date.now(), quality: quality[sym],
          });
          decisions.push({ symbol: sym, orderId: o.orderId, qty, refPriceUsd: o.refPriceUsd });
        }
      } catch (e) {
        console.error("propose failed", redact({ symbol: sym, error: e.message }));
      }
    }
  }

  const summary = {
    jobId, cycleId,
    symbols: symbols.length, ranked, breaches: candidates.length,
    proposals: decisions.length, exitSignals: exits.length,
    modelCalls,
    provider: fetchMeta.provider || provider.id,
    providerNote: fetchMeta.note || fetchMeta.error || null,
    regime: { vixNorm: Number(reg.vixNorm.toFixed(2)), cor3m: reg.cor3m, stale: reg.stale },
    session: { phase: session.phase, date: session.date },
    mode: ctrl.mode || "research", dryRun: ctrl.dryRun !== false,
    elapsedMs: Date.now() - startedAt,
  };

  await runRef.set({
    ...summary, finishedAt: A.FV.serverTimestamp(), status: "complete",
  }, { merge: true });

  await A.col(A.COL.control).doc("control").set({
    lastCycleSummary: summary, lastCycleFinishedAt: A.FV.serverTimestamp(),
  }, { merge: true });

  return summary;
}

/* ── evidence sweep (slower clock) ─────────────────────────────────────── */
async function runEvidence(jobId) {
  const startedAt = Date.now();
  const ctrl = await controlDoc();
  const universe = await loadUniverse(ctrl.universeVersion);
  const names = [...(universe.researchTier || [])];
  const results = [];
  for (const t of names) {
    if (!t.cik) continue;
    try {
      const polled = await E.pollEdgar(t.cik, { forms: "" });
      for (const d of (polled.entries || []).slice(0, 8)) {
        await E.recordVersion({ symbol: t.symbol, sourceId: "sec.latest", entry: d, rawSha256: polled.sha256 });
      }
      results.push({ symbol: t.symbol, fresh: (polled.entries || []).length, notModified: !!polled.notModified });
    } catch (e) {
      results.push({ symbol: t.symbol, error: String(e.code || e.message).slice(0, 120) });
    }
  }
  const summary = { jobId, kind: "evidence", swept: results.length, results, elapsedMs: Date.now() - startedAt };
  await A.col(A.COL.runs).doc(jobId).set({
    ...summary, finishedAt: A.FV.serverTimestamp(), status: "complete",
    ...A.envelope({ created_by: FN_NAME }),
  }, { merge: true });
  return summary;
}

/* ── handler ───────────────────────────────────────────────────────────── */
exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch {}
  const { jobId, task, nonce } = body;

  // A background function is a public URL. Only a nonce minted by the
  // scheduler for THIS job and THIS function is accepted.
  const claim = verifyWorkerNonce(nonce, FN_NAME);
  if (!claim || claim.jobId !== jobId) {
    console.warn("investorCycle: rejected unauthenticated invocation");
    return { statusCode: 403, body: JSON.stringify({ error: "invalid or missing worker nonce" }) };
  }

  const jobRef = A.col(A.COL.jobs).doc(jobId);
  try {
    const snap = await jobRef.get();
    if (snap.exists && snap.data().status === "complete") {
      return { statusCode: 200, body: JSON.stringify({ ok: true, duplicate: true, jobId }) };
    }
    await jobRef.set({ status: "running", startedAt: A.FV.serverTimestamp(),
                       attempts: A.FV.increment(1) }, { merge: true });

    const out = task === "evidence" ? await runEvidence(jobId) : await runCycle(jobId);

    await jobRef.set({ status: "complete", finishedAt: A.FV.serverTimestamp(), summary: out }, { merge: true });
    console.log("investorCycle done", JSON.stringify(redact(out)));
    return { statusCode: 200, body: JSON.stringify({ ok: true, ...out }) };
  } catch (e) {
    console.error("investorCycle failed", redact({ jobId, task, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    await jobRef.set({
      status: "dead", error: String(e.message).slice(0, 300),
      finishedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};

exports.runCycle = runCycle;
exports.attentionZ = attentionZ;
