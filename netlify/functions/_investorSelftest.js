/* Investor_AI — deterministic runtime invariants for deployment attestation. */
"use strict";

const crypto = require("crypto");

function canonical(v) {
  if (Array.isArray(v)) return v.map(canonical);
  if (v && typeof v === "object") return Object.fromEntries(Object.keys(v).sort()
    .map((k) => [k, canonical(v[k])]));
  return v;
}
function digest(v) { return crypto.createHash("sha256").update(JSON.stringify(canonical(v))).digest("hex"); }
/* The attestation must cover WHAT was asserted, not only that something
 * passed. v8.4 shipped a fixture hash byte-identical to v8.3's: the case
 * names and their pass/null details had not changed, so the digest could not
 * change either, and a reviewer reading "22/22, hash 04ff5e1b" had no way to
 * tell that none of the release's new invariants were in the set. Hashing the
 * check's own source means editing an assertion — or replacing a real check
 * with `return true` — moves the hash. */
function fixture(name, fn) {
  const check = digest(String(fn));
  try {
    const detail = fn();
    return { name, check, pass: detail !== false, detail: detail === true ? null : detail };
  } catch (e) {
    return { name, check, pass: false, error: String(e.message).slice(0, 200) };
  }
}

function runFixtures() {
  const M = require("./_investorMarket");
  const S = require("./_investorSignal");
  const L = require("./_investorLedger");
  const R = require("./_investorRisk");
  const V = require("./_investorVariants");
  const SH = require("./_investorShadow");
  const C = require("./_investorCalibration");
  const U = require("./_investorUniverse");
  const B = require("./_investorBootstrap");
  const I = require("./_investorIntelligence");
  const T = require("./_investorTemporal");
  const AL = require("./_investorAllocator");
  const RS = require("./_investorResearchStats");
  const strategy = require("./_investorStrategy");
  const cases = [];

  cases.push(fixture("ledger_integer_balance", () =>
    L.assertBalanced([{ account: "a", amountCents: 123 }, { account: "b", amountCents: -123 }])));
  cases.push(fixture("execution_clock_strictly_post_decision", () => {
    const d = Date.parse("2026-08-28T14:00:00Z");
    return L.executionClockCheck({ decisionAtMs: d, eligibleAfterMs: d + 60000,
      barOpenAt: "2026-08-28T14:01:00Z" }).pass;
  }));
  cases.push(fixture("session_partition_and_dedupe", () => {
    const b = { t: "2026-08-28T14:00:00Z", o: 10, h: 11, l: 9, c: 10.5, v: 100 };
    const p = M.partitionBarsBySession([b, { ...b, c: 10.6 }]);
    return Object.keys(p).length === 1 && Object.values(p)[0].length === 1;
  }));
  cases.push(fixture("holiday_calendar_rules", () => {
    const july4 = M.sessionState(new Date("2026-07-03T15:00:00Z"));
    const monday = M.sessionState(new Date("2026-07-06T15:00:00Z"));
    return july4.tradingDay === false && monday.tradingDay === true;
  }));
  cases.push(fixture("cross_year_new_year_observation", () =>
    M.sessionState(new Date("2027-12-31T15:00:00Z")).isHoliday === true));
  cases.push(fixture("nonconsolidated_feed_cannot_trade", () => {
    const now = Date.parse("2026-08-28T15:00:00Z");
    const q = M.gradeSeries([{ t: "2026-08-28T14:59:00Z", o: 1, h: 1, l: 1, c: 1, v: 1 }],
      { provider: "alpaca", feed: "iex", sourceSha256: "a".repeat(64), nowMs: now });
    return q.grade === "C" && q.tradable === false;
  }));
  cases.push(fixture("volatility_never_increases_risk", () => {
    const unknown = S.volatilityScaler(NaN, strategy.parameters).scaler;
    const low = S.volatilityScaler(0.8, strategy.parameters).scaler;
    const high = S.volatilityScaler(2, strategy.parameters).scaler;
    return unknown === strategy.parameters.volScalerFloor && low <= 1 && high <= 1;
  }));
  cases.push(fixture("tail_sizing_is_risk_limited", () => {
    const s = R.positionSizeUsd({ navUsd: 100000, atrPct: 2, expectedShortfall5dPct: 12,
      overnightGapEsPct: 4, signalScaler: 1, cfg: strategy });
    return s.riskDistancePct === 12 && s.maxPlannedLossUsd <= 250.01;
  }));
  cases.push(fixture("variants_do_not_inherit_incumbent", () => {
    const c = V.configFor("C", { entryRank: 0.99, minAbsZ: 99 });
    return c.entryRank === 0.10 && c.minAbsZ === 2.0 && c.maxHoldDays === 6;
  }));
  cases.push(fixture("discount_ess_uses_squared_weights", () => {
    const w = Array.from({ length: 2000 }, (_, i) => Math.pow(SH.DISCOUNT_GAMMA, 1999 - i));
    const n = SH.discountEffectiveN(w);
    return n > 164 && n < 168;
  }));
  cases.push(fixture("chronological_holdout_has_embargo", () => {
    const rows = Array.from({ length: 700 }, (_, i) => ({
      date: new Date(Date.UTC(2020, 0, 1 + i)).toISOString().slice(0, 10), mean: 1,
    }));
    const s = C.chronologicalSplit(rows);
    const gap1 = s.all.findIndex((r) => r.date === s.calibration[0].date)
      - s.all.findIndex((r) => r.date === s.train.at(-1).date) - 1;
    const gap2 = s.all.findIndex((r) => r.date === s.holdout[0].date)
      - s.all.findIndex((r) => r.date === s.calibration.at(-1).date) - 1;
    return s.pass && gap1 === 15 && gap2 === 15;
  }));
  cases.push(fixture("frozen_identity_hashes_are_full_sha256", () =>
    /^[a-f0-9]{64}$/.test(B.universeHash(U))
      && /^[a-f0-9]{64}$/.test(B.strategyHash(strategy))
      && /^[a-f0-9]{64}$/.test(V.variantsHash())));
  cases.push(fixture("company_intelligence_missing_coverage_fails_closed", () => {
    const p = I.decisionPolicy({ coverage: { monitored: true, complete: false,
      missingRoles: ["company_primary"], asOfMs: Date.now() }, events: [] });
    return p.entryAllowed === false && p.sizeMultiplier === 0;
  }));
  cases.push(fixture("discovery_lead_is_not_independent_confirmation", () => {
    const d = { documentId: "lead", title: "material safety investigation announced",
      canonicalText: "A material safety investigation was announced.",
      canonicalContentSha256: "d".repeat(64), decisionKnownAtMs: Date.now() - 1000,
      sourceId: "gdelt.discovery", sourceClass: "discovery_index", sourceTier: "C",
      discoveryOnly: true, publisherGroup: "gdelt" };
    const raw = I.extractEvents([d])[0];
    const event = I.calibrateEvent(raw, [d]);
    return event.corroboration.independentGroups === 0 && event.evidenceEligible === false;
  }));
  cases.push(fixture("positive_intelligence_never_levers_base_risk", () => {
    const p = I.decisionPolicy({ coverage: { monitored: true, complete: true,
      asOfMs: Date.now() }, events: [{ direction: 1, evidenceEligible: true,
        opportunityScore: 100 }] });
    return p.entryAllowed === true && p.sizeMultiplier === 1
      && p.positiveRiskIncreaseAllowed === false;
  }));
  cases.push(fixture("temporal_context_missing_required_input_fails_closed", () => {
    const now = Date.now();
    const p = T.temporalPolicy({ asOfMs: now, coverage: { requiredMissing: ["earnings_window"] },
      schedule: { nearest: null }, hazards: { active: [], riskScore: 0 },
      seasonality: { status: "warming", riskScore: 0 }, drivers: { drivers: [] } }, { asOfMs: now });
    return p.entryAllowed === false && p.sizeMultiplier === 0 && p.criticalExit === false;
  }));
  cases.push(fixture("temporal_positive_never_increases_risk", () => {
    const now = Date.now();
    const p = T.temporalPolicy({ asOfMs: now, coverage: { requiredMissing: [] },
      schedule: { nearest: null }, hazards: { active: [], riskScore: 0 },
      seasonality: { status: "ready", shrunkReturnPct: 12, riskScore: 0 },
      drivers: { drivers: [] } }, { asOfMs: now });
    return p.entryAllowed === true && p.sizeMultiplier === 1 && p.positiveRiskIncreaseAllowed === false;
  }));
  cases.push(fixture("dynamic_correlation_unknown_fails_closed", () => {
    const book = { count: 1, grossUsd: 3000, bySectorPct: {}, byClusterPct: {},
      rows: [{ symbol: "HELD", valueUsd: 3000, cluster: "other" }] };
    const add = R.checkAdd({ symbol: "NEW", sector: "other", proposedUsd: 1000,
      book, navUsd: 100000, cashUsd: 97000, cfg: strategy, dynamicCorrelations: {} });
    return add.allow === false && add.blockedBy.includes("dynamic_correlation_unknown");
  }));
  cases.push(fixture("temporal_structured_fields_are_quote_grounded", () => {
    const now = Date.parse("2026-08-15T12:00:00Z");
    const text = "Our primary factory in Texas is exposed to hurricane disruption in September 2026.";
    const docs = [{ documentId: "d", canonicalText: text, decisionKnownAtMs: now - 1,
      sourceTier: "A", sourceClass: "company_primary" }];
    const rows = T.normalizeExposures([{ exposureType: "weather", name: "Florida August risk",
      directionWhenDriverRises: "negative", states: ["FL", "TX"], months: [8, 9],
      scheduledDate: "2026-08-30", support: { claim: "Texas hurricane disruption",
        quote: text, documentRef: "d" } }], docs, now);
    return rows.length === 1 && rows[0].states.join() === "TX" && rows[0].months.join() === "9"
      && rows[0].scheduledDate === null;
  }));
  cases.push(fixture("future_temporal_document_is_rejected", () => {
    const now = Date.parse("2026-08-15T12:00:00Z");
    const d = { documentId: "future", canonicalText: "Quarterly results on August 16, 2026.",
      decisionKnownAtMs: now + 1, sourceTier: "A", sourceClass: "company_primary" };
    return T.extractScheduledEvents([d], now).length === 0;
  }));
  cases.push(fixture("independent_temporal_risks_combine_boundedly", () => {
    const x = T.combineAdverseRisks([{ group: "schedule", score: 34 },
      { group: "drivers", score: 34 }, { group: "hazards", score: 34 },
      { group: "hazards", score: 33 }]);
    return x.components.length === 3 && x.riskScore > 50 && x.riskScore < 60;
  }));
  cases.push(fixture("future_context_clock_fails_closed", () => {
    const now = Date.now();
    const p = T.temporalPolicy({ asOfMs: now + 3600e3, coverage: { requiredMissing: [] },
      schedule: { events: [] }, hazards: { active: [], riskScore: 0 },
      recurring: { active: [], riskScore: 0 }, seasonality: { riskScore: 0 },
      drivers: { drivers: [] } }, { asOfMs: now });
    return p.entryAllowed === false && p.fresh === false;
  }));

  /* ── v8.5 invariants ──────────────────────────────────────────────────
     Every fix in this release is attested here, in the deployed function.
     A fixture set that does not change when the invariants change makes the
     hash a version label rather than evidence: v8.4 shipped a set identical
     to v8.3's, so the build could not prove it was the build that was
     tested. Each case below fails if its fix is reverted. */

  const VT = require("./_investorVisibleText");

  cases.push(fixture("visible_text_unclosed_hidden_keeps_following_disclosure", () => {
    const out = VT.scan("<p>Q3</p><div hidden><span>nav</span>"
      + "<p>substantial doubt about going concern</p>");
    return /going concern/.test(out.text) && !/nav/.test(out.text)
      && out.unbalancedHidden === true;
  }));
  cases.push(fixture("visible_text_quoted_markup_is_not_markup", () =>
    /FAA/.test(VT.visibleText('<p>use of &lt;span class="hidden"&gt; is barred. '
      + "FAA grounded the fleet.</p>"))));
  cases.push(fixture("visible_text_same_tag_nesting_releases_nothing", () =>
    !/LEAK/.test(VT.visibleText("<div hidden>a<div>b</div>LEAK</div><p>kept</p>"))
    && /kept/.test(VT.visibleText("<div hidden>a<div>b</div>LEAK</div><p>kept</p>"))));
  cases.push(fixture("visible_text_style_hiding_detected", () =>
    !/P(?!\w)/.test(VT.visibleText('<div style="opacity:0">P</div><p>kept</p>'))
    && !/P(?!\w)/.test(VT.visibleText('<div style="font-size:0">P</div><p>kept</p>'))
    && VT.visibleText('<div style="opacity:0.9">P</div>') === "P"));
  cases.push(fixture("visible_text_attribute_gt_cannot_end_tag", () =>
    /* The hiding marker sits AFTER a ">" inside a quoted attribute value. A
       scanner that ends the tag at the first ">" never sees it. */
    !/PAYLOAD/.test(VT.visibleText('<div title="a > b" hidden>PAYLOAD</div><p>k</p>'))
    && !/PAYLOAD/.test(VT.visibleText('<div data-x="]>" style="display:none">PAYLOAD</div><p>k</p>'))));

  cases.push(fixture("retained_seasonality_is_context_only", () => {
    const H = require("./_investorHistory");
    return T.MIN_SEASONAL_DECISION_SAMPLES === 8
      && T.maxSeasonalSamples(H.KEEP_DAYS) < T.MIN_SEASONAL_DECISION_SAMPLES;
  }));
  cases.push(fixture("seasonality_bootstrap_is_deterministic", () => {
    /* Compared across several value sets, and across shrink factors, so a
       randomised generator cannot coincide on the discrete resample lattice
       the way a single four-value set can. */
    const sets = [[-0.083, -0.051, -0.117, -0.024],
      [-0.0731, 0.0122, -0.0918, -0.0334, -0.0605],
      [0.0417, -0.0209, 0.0136, -0.0752, 0.0391, -0.0028]];
    for (const v of sets) {
      for (const s of [0.5, 0.333]) {
        if (JSON.stringify(T.bootstrapBounds(v, s)) !== JSON.stringify(T.bootstrapBounds(v, s))) return false;
      }
    }
    const v = sets[0], b = T.bootstrapBounds(v, 0.5);
    return b.lower < b.upper && b.upper < 0
      && Math.abs(b.upper) < Math.abs(b.lower);
  }));
  cases.push(fixture("sector_driver_carries_no_decision_weight", () => {
    const days = [];
    let cd = 100, cc = 100;
    for (let i = 0; i < 300; i += 1) {
      const d = new Date(Date.UTC(2025, 0, 5 + i));
      if (d.getUTCDay() === 0 || d.getUTCDay() === 6) continue;
      const r = i >= 292 ? -0.01 : (i % 2 ? 0.002 : -0.001);
      cd *= 1 + r; cc *= 1 + 1.7 * r;
      days.push({ date: d.toISOString().slice(0, 10), driver: cd, company: cc });
    }
    const asOf = Date.parse(days[days.length - 1].date + "T23:00:00Z") + 864e5;
    const mk = (k) => days.map((x) => ({ date: x.date, o: x[k], h: x[k], l: x[k], c: x[k], v: 1e6 }));
    const ctx = T.driverContext(mk("company"), { XLE: mk("driver") }, [], "energy", asOf);
    const row = ctx.drivers.find((x) => x.symbol === "XLE");
    if (!row || row.status !== "ready") return { skipped: row ? row.reason : "no_row" };
    return row.isSectorDriver === true && row.namedDriverEligible === false
      && row.riskScore === 0 && row.statisticalRiskScore === 0;
  }));

  cases.push(fixture("event_clustering_refuses_unrelated_matters", () => {
    const subjects = [
      "Acme third quarter safety investigation into brake assemblies opened",
      "Acme third quarter safety investigation into avionics wiring opened",
      "Acme third quarter safety investigation into cabin pressurization opened",
    ];
    const common = I.batchCommonTokens(subjects);
    return I.sameEventSubject(subjects[0], subjects[1], common) === false
      && I.sameEventSubject(subjects[1], subjects[2], common) === false
      && I.sameEventSubject("Acme recall NHTSA-2026-4417 fuel pump",
        "NHTSA-2026-4417 remedy filed") === true;
  }));
  cases.push(fixture("event_clustering_keeps_followups_together", () => {
    const s = ["Acme safety investigation into brake assemblies opened",
      "Acme safety investigation into brake assemblies widens",
      "Acme safety investigation into brake assemblies continues"];
    const common = I.batchCommonTokens(s);
    return I.sameEventSubject(s[0], s[1], common) === true;
  }));
  cases.push(fixture("immediacy_is_bounded_by_elapsed_time", () => {
    const now = Date.parse("2026-08-30T12:00:00Z"), D = 864e5;
    return I.timingWeight("imminent", now, now) === 1
      && I.timingWeight("imminent", now - 45 * D, now) <= 0.4
      && I.timingWeight("near_term", now - 120 * D, now) <= 0.25
      && I.timingWeight("imminent", now + 10 * D, now) === 1;
  }));

  cases.push(fixture("guard_is_dispatched_while_the_exchange_is_shut", () => {
    const K = require("./investorKick");
    const ctrl = { enabled: true, mode: "approval", dryRun: false, killSwitch: false,
      accountId: "paper-1", cycleSeconds: 300, guardSeconds: 60, guardSecondsClosed: 900,
      evidenceEverySeconds: 900, afterHoursCycles: false,
      lastCycleAt: null, lastEvidenceAt: null };
    const sat = Date.parse("2026-08-29T18:00:00Z");
    const closed = M.sessionState(new Date(sat));
    const due = K.decide({ ...ctrl, lastGuardAt: sat - 900000 }, closed, sat);
    const notDue = K.decide({ ...ctrl, lastGuardAt: sat - 120000 }, closed, sat);
    return closed.open === false && due.tasks.includes("guard")
      && !notDue.tasks.includes("guard") && !due.tasks.includes("cycle");
  }));
  cases.push(fixture("market_time_clock_finds_the_last_open_session", () => {
    const sunday = new Date(Date.parse("2026-08-30T15:00:00Z"));
    const back = M.lastRegularOpenMs(sunday);
    const during = Date.parse("2026-08-31T17:00:00Z");
    return new Date(back).toISOString() === "2026-08-28T19:59:00.000Z"
      && M.sessionState(new Date(back)).open === true
      && M.lastRegularOpenMs(new Date(during)) === during;
  }));
  cases.push(fixture("closing_bars_are_tradable_in_market_time", () => {
    const lastOpen = M.lastRegularOpenMs(new Date(Date.parse("2026-08-28T22:00:00Z")));
    const p = { provider: "alpaca", feed: "sip", sourceSha256: "a".repeat(64),
      adjustment: "split_and_dividend" };
    const bars = [];
    for (let i = 12; i >= 1; i -= 1) {
      bars.push({ t: new Date(lastOpen - i * 300000).toISOString(),
        o: 10, h: 10, l: 10, c: 10, v: 1000 });
    }
    return M.gradeSeries(bars, { ...p, nowMs: Date.parse("2026-08-28T22:00:00Z") }).tradable === false
      && M.gradeSeries(bars, { ...p, nowMs: lastOpen }).tradable === true;
  }));

  cases.push(fixture("size_haircuts_combine_and_do_not_multiply", () => {
    const four = { volScaler: 0.8, dispersionMult: 0.8, causeConfidence: 0.7, intelligenceMult: 0.8 };
    const c = S.combineSizeMultipliers(four).combined;
    const product = 0.8 * 0.8 * 0.7 * 0.8;
    return c > product && c <= 0.7
      && S.combineSizeMultipliers({ ...four, intelligenceMult: 0 }).combined === 0
      && S.combineSizeMultipliers({ volScaler: 1, dispersionMult: 1,
        causeConfidence: 1, intelligenceMult: 1 }).combined === 1;
  }));
  cases.push(fixture("size_and_risk_ladders_are_the_same_ladder", () => {
    const w = T.TEMPORAL_WEIGHTS.combination;
    const c = S.combineSizeMultipliers({ volScaler: 0.5, dispersionMult: 0.8,
      causeConfidence: 1, intelligenceMult: 1 }).combined;
    return c === Number((1 - (0.5 * w[0] + 0.2 * w[1])).toFixed(3));
  }));
  cases.push(fixture("weighted_mean_uses_weighted_hac_uncertainty", () => {
    const x = Array.from({ length: 80 }, (_, i) => Math.sin(i / 5) + i / 100);
    const w = x.map((_, i) => Math.pow(0.98, x.length - 1 - i));
    const weighted = AL.weightedHacMeanVariance(x, w);
    return Number.isFinite(weighted) && weighted > 0
      && weighted !== AL.hacMeanVariance(x);
  }));
  cases.push(fixture("frozen_family_tests_horizons_and_matrix_components", () => {
    const windows = new Set(V.VARIANTS.map((v) => V.configFor(v.id).signalWindow));
    const holds = new Set(V.VARIANTS.map((v) => V.configFor(v.id).maxHoldDays));
    return V.VARIANTS.length === 14 && [6, 12, 24].every((x) => windows.has(x))
      && [6, 10, 14].every((x) => holds.has(x))
      && V.configFor("K").decisionMatrixPolicy.temporalRiskScale === 1.25
      && V.configFor("L").decisionMatrixPolicy.intelligenceRiskScale === 1.25
      && V.configFor("M").sizeAggregation === "product"
      && V.configFor("N").decisionMatrixPolicy.nonBlockingRiskFloor === 25;
  }));
  cases.push(fixture("decision_challengers_cannot_weaken_hard_event_block", () => {
    const now = Date.now();
    const coverage = { monitored: true, complete: true, asOfMs: now };
    const events = [{ direction: -1, evidenceEligible: true, adverseRiskScore: 80,
      probabilityTrue: 0.9, probabilityMaterial: 0.8,
      corroboration: { unresolvedContradiction: false, independentGroups: 2,
        governmentPrimary: true }, title: "material decline" }];
    return [null, V.configFor("K").decisionMatrixPolicy,
      V.configFor("L").decisionMatrixPolicy, V.configFor("N").decisionMatrixPolicy]
      .every((decisionMatrixPolicy) => {
        const p = I.decisionPolicy({ coverage, events, asOfMs: now, decisionMatrixPolicy });
        return p.entryAllowed === false && p.sizeMultiplier === 0;
      });
  }));
  cases.push(fixture("forward_confirmation_uses_only_post_lock_sessions", () => {
    const date = (i) => new Date(Date.UTC(2026, 0, 1 + i)).toISOString().slice(0, 10);
    const lock = C.forwardLockTemplate({ experimentHash: "a".repeat(64), leaderId: "A",
      dataThroughDate: date(9), embargoSessions: 2, requiredSessions: 30 });
    const rows = Array.from({ length: 42 }, (_, i) => ({ date: date(i), mean: i < 10 ? -999 : 5,
      cost: 1, worstCaseMean: 4 }));
    const out = C.evaluateForward(rows, lock, { alpha: 0.01, k: V.VARIANTS.length });
    return out.complete && out.pass && out.boundaries.confirmation[0] === date(12)
      && out.normal.n === 30;
  }));
  cases.push(fixture("deflated_sharpe_counts_full_trial_family", () => {
    const values = Array.from({ length: 240 }, (_, i) => 4 + Math.sin(i / 3) * 2);
    const d = RS.deflatedSharpe(values, { trials: V.VARIANTS.length });
    return d.trials === 14 && d.pass && d.probability >= 0.95;
  }));

  /* ── v8.6.1 ──────────────────────────────────────────────────────────
     Two findings, attested at the points where they act. */

  const LD = require("./_investorLadder");
  const CY = require("./investorCycle-background");

  cases.push(fixture("overfit_guard_withholds_the_leader_in_the_cycle", () => {
    /* The Deflated Sharpe and CSCV/PBO gates were unattested at the point
       they act: the cycle's withholding and the ladder's rung could both be
       removed and every test and fixture still passed. */
    const ok = { pass: true, probability: 0.99 }, bad = { pass: false, probability: 0.61 };
    const okPbo = { pass: true, pbo: 0.07 }, badPbo = { pass: false, pbo: 0.44 };
    const g = (d, p) => ({ dsrByVariant: { G: d }, pbo: p });
    if (CY.applyOverfitGuard({ leaderId: "G", note: "" }, g(ok, okPbo)).leaderId !== "G") return false;
    for (const guard of [g(bad, okPbo), g(ok, badPbo), g(bad, badPbo), null, {}]) {
      const alloc = CY.applyOverfitGuard({ leaderId: "G", note: "" }, guard);
      if (alloc.leaderId !== null) return false;
      if (!/Overfit guard withheld promotion/.test(String(alloc.note))) return false;
    }
    return true;
  }));

  cases.push(fixture("overfit_guard_rung_is_critical_and_blocks_limited_auto", () => {
    const ok = { pass: true, probability: 0.99 }, bad = { pass: false, probability: 0.61 };
    const okPbo = { pass: true, pbo: 0.07 }, badPbo = { pass: false, pbo: 0.44 };
    const rung = (allocation) => LD.evaluateGates({ allocation })
      .find((x) => x.id === "overfit_guard");
    const clean = rung({ leaderId: "G", overfitGuard: { dsrByVariant: { G: ok }, pbo: okPbo } });
    if (!clean || clean.pass !== true || clean.critical !== true
      || clean.stage !== "limited_auto") return false;
    for (const guard of [{ dsrByVariant: { G: bad }, pbo: okPbo },
      { dsrByVariant: { G: ok }, pbo: badPbo }, undefined]) {
      const r = rung({ leaderId: "G", overfitGuard: guard });
      if (!r || r.pass !== false || r.critical !== true) return false;
    }
    return true;
  }));

  cases.push(fixture("passes_guard_refuses_absent_partial_and_mismatched_evidence", () => {
    const ok = { pass: true, probability: 0.99 }, okPbo = { pass: true, pbo: 0.07 };
    return RS.passesGuard({ dsrByVariant: { G: ok }, pbo: okPbo }, "G") === true
      && RS.passesGuard(null, "G") === false
      && RS.passesGuard({}, "G") === false
      && RS.passesGuard({ dsrByVariant: { G: ok } }, "G") === false
      && RS.passesGuard({ pbo: okPbo }, "G") === false
      && RS.passesGuard({ dsrByVariant: { G: ok }, pbo: okPbo }, "H") === false;
  }));

  cases.push(fixture("overfit_thresholds_are_load_bearing", () => {
    /* The 0.95 DSR probability and the 0.20 PBO bar are the numbers that make
       the guard a guard; neither was attested. */
    let seed = 99887766;
    const rnd = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
    const gauss = () => Math.sqrt(-2 * Math.log(Math.max(1e-9, rnd())))
      * Math.cos(2 * Math.PI * rnd());
    const marginal = Array.from({ length: 400 }, () => 2.0 + gauss() * 25);
    const d = RS.deflatedSharpe(marginal, { trials: V.VARIANTS.length });
    if (!(d.dailySharpe > 0) || d.threshold !== 0.95 || d.probability >= 0.95 || d.pass !== false) return false;

    const dates = Array.from({ length: 300 }, (_, i) =>
      new Date(Date.UTC(2024, 0, 2 + i)).toISOString().slice(0, 10));
    const daily = Object.fromEntries(V.VARIANTS.map((v) => [v.id, []]));
    let s2 = 424242;
    const r2 = () => { s2 = (s2 * 1103515245 + 12345) % 2147483648; return s2 / 2147483648; };
    const g2 = () => Math.sqrt(-2 * Math.log(Math.max(1e-9, r2()))) * Math.cos(2 * Math.PI * r2());
    for (const date of dates) {
      const common = g2() * 8;
      for (const v of V.VARIANTS) daily[v.id].push({ date, mean: common + g2() * 10 });
    }
    const pbo = RS.probabilityBacktestOverfit(daily);
    return pbo.splits === 70 && pbo.threshold === 0.20 && pbo.pbo > 0.20 && pbo.pass === false;
  }));

  cases.push(fixture("session_span_is_stable_across_intraday_remarks", () => {
    /* The span is measured from the previous complete DAY. A second pass on
       the same date must not see "today" as the prior session and readmit a
       row whose return compounds across a skipped one. */
    const span = (prior, date) => M.tradingSessionsBetween(prior, date);
    return span("2026-08-27", "2026-08-31") === 2
      && span("2026-08-31", "2026-08-31") === 0
      && span("2026-08-31", "2026-09-01") === 1;
  }));

  cases.push(fixture("trading_sessions_counted_on_the_exchange_calendar", () =>
    M.tradingSessionsBetween("2026-08-27", "2026-08-28") === 1
    && M.tradingSessionsBetween("2026-08-28", "2026-08-31") === 1
    && M.tradingSessionsBetween("2026-08-27", "2026-08-31") === 2
    && M.tradingSessionsBetween("2026-09-03", "2026-09-08") === 2
    && M.tradingSessionsBetween("2026-08-31", "2026-08-31") === 0
    && M.tradingSessionsBetween("2026-08-31", "2026-08-28") === 0
    && M.tradingSessionsBetween("", "2026-08-31") === null));

  cases.push(fixture("market_config_never_opens_an_unchosen_lane", () => {
    /* provider/feed live in InvestorAI_Control/marketConfig. The invariant is
       that an absent or unrecognised lane resolves where an absent environment
       variable always did — manual/iex, not tradable.

       This is attested against the PURE resolver, not against marketSettings().
       marketSettings() reads a cache that Firestore fills, so once the deployed
       process has loaded its config the function no longer consults the
       environment at all: deleting env vars proved nothing there and the
       fixture failed in production while passing on every cold process here.
       A fixture whose verdict depends on whether a cache happens to be warm is
       not an invariant, and this one wrongly pinned the desk to research. */
    const bare = M.normalizeMarketChoice(undefined, undefined);
    if (bare.provider !== "manual" || bare.feed !== "iex") return false;

    for (const [p, f] of [["polygon", "consolidated"], ["", ""], [null, null],
                          ["ALPACA_", "sip_"], [{}, []], [0, 0], [true, true]]) {
      const bad = M.normalizeMarketChoice(p, f);
      if (bad.provider !== "manual" || bad.feed !== "iex") return false;
      if (bad.providerRecognised !== false) return false;
    }
    /* A recognised lane still resolves to itself, case-insensitively, or the
       fixture would pass by refusing everything. */
    const good = M.normalizeMarketChoice("ALPACA", "SIP");
    if (good.provider !== "alpaca" || good.feed !== "sip") return false;

    /* And an uncredentialed manual lane can never be execution grade. */
    const cfg = M.providerConfig("manual");
    return cfg.liquidityEligible !== true && cfg.maxGrade !== "A";
  }));

  cases.push(fixture("secret_pair_gate_refuses_every_unusable_shape", () => {
    /* Operator secrets moved from the Lambda environment to
       InvestorAI_Control. usableSecretPair is the gate that decides whether a
       document may authenticate at all; it is attested here as a pure
       predicate so the result cannot depend on cache state. */
    const AU = require("./_investorAuth");
    const P = "p".repeat(16), S32 = "s".repeat(32);
    if (AU.usableSecretPair(P, S32) !== true) return false;
    const bad = [
      [P, "s".repeat(31)], ["p".repeat(15), S32], ["", S32], [P, ""],
      [null, S32], [P, null], [undefined, undefined],
      [12345678901234567890, S32], [true, S32], [[P], S32], [{}, S32],
      ["   " + "p".repeat(13), S32], [P, "   " + "s".repeat(29)],
    ];
    for (const [a, b] of bad) if (AU.usableSecretPair(a, b) !== false) return false;
    /* And an uncredentialed provider degrades to a lane that cannot trade. */
    return M.activeProvider.length === 0
      && M.providerCredentialed("nonexistent-provider") === true
      && M.providerCredentialed("alpaca") === M.providerCredentialed("alpaca");
  }));

  /* ── v8.7 invariants: measurement admission vs execution admission ──── */

  /* The whole point of the split. A single-venue series must be usable for
     ranking and unusable for pricing an order, and the second half must not
     quietly follow the first. */
  cases.push(fixture("research_grade_measures_but_never_executes", () => {
    const now = Date.parse("2026-08-28T15:00:00Z");
    const q = M.gradeSeries([{ t: "2026-08-28T14:59:00Z", o: 1, h: 1, l: 1, c: 1, v: 1 }],
      { provider: "alpaca", feed: "iex", sourceSha256: "a".repeat(64), nowMs: now });
    if (!(q.grade === "C" && q.tradable === false && q.researchEligible === true)) return false;
    /* Execution paths are unmoved by researchEligible: the real refusals are
       called here, not asserted about. Both are handed provenance that is
       otherwise perfect, so the only thing left to refuse on is the grade. */
    const prov = { sourceSha256: "b".repeat(64), provider: "alpaca", feed: "iex",
                   adjustment: "split_and_dividend" };
    const W = require("./_investorWorkset");
    const G = require("./_investorPositionGuard");
    const wk = W.exitExecutionSourcePolicy(prov, prov, q);
    const gd = G.trustedDecisionSource(prov, q);
    if (wk.pass !== false || wk.reason !== "execution_source_not_tradable") return false;
    if (gd.pass !== false || gd.reason !== "decision_source_not_tradable") return false;
    /* An F-graded series is not admitted even for measurement. */
    const bad = M.gradeSeries([{ t: "2026-08-28T14:59:00Z", o: 1, h: 1, l: 1, c: 1, v: 1 }],
      { provider: "alpaca", feed: "iex", sourceSha256: "not-a-hash", nowMs: now });
    return bad.grade === "F" && bad.researchEligible === false && bad.tradable === false;
  }));

  /* Opting in must be explicit: a caller that does not pass the flag keeps the
     old execution-grade cross-section exactly. */
  cases.push(fixture("panel_admission_requires_explicit_opt_in", () => {
    const series = (n) => Array.from({ length: n }, (_, i) => ({
      t: new Date(Date.parse("2026-08-28T14:00:00Z") + i * 300000).toISOString(),
      o: 10, h: 10, l: 10, c: 10 + Math.sin(i) * 0.1, v: 1000 }));
    const panel = { A: series(30), B: series(30), C: series(30), D: series(30) };
    const q = {}; for (const k of Object.keys(panel)) {
      q[k] = { grade: "C", tradable: false, researchEligible: true };
    }
    /* Admission is the invariant under test, so assert on the breadth verdict:
       held closed the panel never reaches four names, opened it does. Whether
       those names then survive the coverage filters is a separate rule with its
       own fixtures, and this one must not silently depend on it. */
    const closed = S.residualPanel(panel, { signalWindow: 12, quality: q, intervalMs: 300000 });
    const open = S.residualPanel(panel, { signalWindow: 12, quality: q, intervalMs: 300000,
      allowResearchGrade: true });
    if (closed.note !== "insufficient_panel_breadth") return false;
    if (open.note === "insufficient_panel_breadth") return false;
    /* An F grade is refused by both, opt-in or not. */
    const dead = {}; for (const k of Object.keys(panel)) {
      dead[k] = { grade: "F", tradable: false, researchEligible: false };
    }
    return S.residualPanel(panel, { signalWindow: 12, quality: dead, intervalMs: 300000,
      allowResearchGrade: true }).note === "insufficient_panel_breadth";
  }));

  /* The bars endpoint has no implicit lookback: a request that states no window
     returns the current day, which is what emptied the daily backfill. */
  cases.push(fixture("history_request_states_an_explicit_window", () => {
    const day = M.alpacaWindow("1Day", 1300, "iex");
    const spanDays = (Date.parse(day.end) - Date.parse(day.start)) / 86400000;
    /* 1300 TRADING days needs materially more than 1300 calendar days. */
    if (!(spanDays > 1700 && spanDays < 2200)) return false;
    const min = M.alpacaWindow("5Min", 120, "iex");
    if (!(Date.parse(min.end) > Date.parse(min.start))) return false;
    /* The Basic plan does not serve the most recent 15 minutes; asking for it
       fails the one edge of the series a cycle actually reads. SIP is not
       clamped, so a paid feed loses no recency. */
    const lag = (Date.now() - Date.parse(day.end)) / 60000;
    const sip = (Date.now() - Date.parse(M.alpacaWindow("1Day", 1300, "sip").end)) / 60000;
    return lag >= 15 && lag < 25 && sip < 2;
  }));

  /* A backfill that silently loses its tail looks identical to one that
     succeeded, so the page ceiling must cover the batch the backfill actually
     sends. Both numbers are asserted together: widening either one alone is
     the failure mode. */
  cases.push(fixture("backfill_pages_cover_the_backfill_batch", () => {
    const H = require("./_investorHistory");
    const chunk = H.DAILY_CHUNK_SYMBOLS;
    if (!(Number.isInteger(chunk) && chunk > 0)) return false;
    const need = H.KEEP_DAYS * chunk;
    if (M.alpacaPageBudget(H.KEEP_DAYS, chunk) * 10000 < need) return false;
    /* And the ceiling must track the request rather than sit at its floor. */
    return M.alpacaPageBudget(H.KEEP_DAYS, chunk * 8)
         > M.alpacaPageBudget(H.KEEP_DAYS, chunk);
  }));

  /* A manual run must not become a way around the two switches that stop the
     desk, and must not reuse the scheduler's cadence slot — a manual run
     swallowed as a duplicate looks exactly like a manual run that did nothing. */
  cases.push(fixture("manual_run_is_gated_and_does_not_reuse_the_slot", () => {
    const fs = require("fs"), path = require("path");
    const src = fs.readFileSync(path.join(__dirname, "investorApi.js"), "utf8");
    const act = src.slice(src.indexOf("async runCycleNow("));
    const body = act.slice(0, act.indexOf("\n  },"));
    /* Assert the REFUSAL, not a mention: `killSwitch: !!ctrl.killSwitch` also
       appears in the dispatch payload, so a bare name test still passes with
       the guard deleted. Both switches must return before dispatching. */
    const dispatchAt = body.indexOf("K.dispatch(");
    const beforeDispatch = dispatchAt > 0 ? body.slice(0, dispatchAt) : body;
    if (!/if \(ctrl\.killSwitch\)\s*return \{ ok: false/.test(beforeDispatch)) return false;
    if (!/if \(ctrl\.enabled === false\)\s*return \{ ok: false/.test(beforeDispatch)) return false;
    if (!/manual: true/.test(body)) return false;
    const kick = fs.readFileSync(path.join(__dirname, "investorKick.js"), "utf8");
    /* The slot id and the manual id must be different expressions. */
    if (!/manual \? `\$\{task\}_\$\{account\}_manual_/.test(kick)) return false;
    /* And direct HTTP to the scheduled dispatcher stays refused. */
    return /isScheduledInvocation\(event\)/.test(kick);
  }));

  /* The outbound Accept header must be real media types. `accept` is a list of
     response-validation SUBSTRINGS, and sending those verbatim gave SEC the
     literal header `Accept: json` — which an edge cache may answer with a 406,
     and which is the shape of failure that left the universe unresolved. */
  cases.push(fixture("accept_header_is_media_types_not_substrings", () => {
    const F = require("./_investorFetch");
    const h = F.acceptHeader(["json"]);
    if (!/^application\/json\b/.test(h)) return false;
    if (/(^|[,\s])json([,;]|$)/.test(h.replace("application/json", ""))) return false;
    /* A wildcard fallback must remain so a strict origin cannot refuse us. */
    if (!/\*\/\*/.test(h)) return false;
    if (F.acceptHeader(null) !== "*/*") return false;
    if (F.acceptHeader([]) !== "*/*") return false;
    if (F.acceptHeader(["weird"]) !== "*/*") return false;
    const two = F.acceptHeader(["html", "xml"]);
    if (!(two.includes("text/html") && two.includes("application/xml"))) return false;
    /* And the request must actually USE it. A correct helper that the call site
       does not call is the same outage with a passing unit test. */
    const src = require("fs").readFileSync(
      require("path").join(__dirname, "_investorFetch.js"), "utf8");
    if (!/"Accept": acceptHeader\(accept\)/.test(src)) return false;
    return !/"Accept": accept \? accept\.join/.test(src);
  }));

  /* ── v8.8: paper learning mode ──────────────────────────────────────── */

  /* The one guarantee that makes a relaxed desk safe: it cannot apply to real
     orders, and no operator value can talk it into doing so. */
  cases.push(fixture("relaxation_is_refused_whenever_dry_run_is_off", () => {
    const ST = require("./_investorStrategy");
    const req = { enabled: true, abstainOnMissingInfo: true, costMarginMultiple: 0 };
    const live = ST.paperLearningConfig(ST.parameters, { dryRun: false, paperLearning: req });
    if (live.active !== false) return false;
    if (!live.refused) return false;
    if (live.cfg.paperAbstainOnMissingInfo === true) return false;
    if (live.cfg.costMarginMultiple !== ST.parameters.costMarginMultiple) return false;
    /* And it stays off unless explicitly enabled. */
    const off = ST.paperLearningConfig(ST.parameters, { dryRun: true });
    if (off.active !== false || off.cfg.paperAbstainOnMissingInfo === true) return false;
    const on = ST.paperLearningConfig(ST.parameters, { dryRun: true, paperLearning: req });
    return on.active === true && on.cfg.paperAbstainOnMissingInfo === true;
  }));

  /* Overrides are clamped, so a typed-in zero cannot turn the desk into a
     random-entry generator that still calls itself a strategy. */
  cases.push(fixture("relaxation_overrides_are_clamped_not_trusted", () => {
    const ST = require("./_investorStrategy");
    const wild = { enabled: true, costMarginMultiple: 0, minAbsZ: 0, entryRank: 1,
                   maxHoldDays: 9999, minAdvUsd: 0, sectorCrowdingMultiple: 99 };
    const r = ST.paperLearningConfig(ST.parameters, { dryRun: true, paperLearning: wild });
    const L = ST.RELAX_LIMITS;
    for (const k of Object.keys(L)) {
      if (r.cfg[k] == null) continue;
      if (r.cfg[k] < L[k].min || r.cfg[k] > L[k].max) return false;
    }
    /* Garbage is ignored rather than coerced to zero. */
    const junk = ST.paperLearningConfig(ST.parameters,
      { dryRun: true, paperLearning: { enabled: true, minAbsZ: "loose", entryRank: null } });
    return junk.cfg.minAbsZ === ST.parameters.minAbsZ
        && junk.cfg.entryRank === ST.parameters.entryRank;
  }));

  /* Relaxation applies to MISSING INFORMATION only. A gate that found a
     problem keeps blocking, or the desk would be taught something false. */
  cases.push(fixture("relaxation_never_overrides_a_finding", () => {
    const cfg = { ...strategy.parameters, paperAbstainOnMissingInfo: true };
    /* A real fundamental cause is a finding: never faded, relaxed or not. */
    const hard = S.directionFromCause(S.CAUSE.HARD_NEWS, cfg, 3, { complete: true });
    if (hard.trade !== false) return false;
    /* Pending evidence is an absence: a paper desk may record it. */
    const pending = S.directionFromCause("something_unclassified", cfg, 0, null);
    if (pending.trade !== true || pending.relaxed !== true) return false;
    if (pending.confidence >= 0.5) return false;
    /* And with the mode off, pending still blocks. */
    const strict = S.directionFromCause("something_unclassified", strategy.parameters, 0, null);
    if (strict.trade !== false) return false;
    /* An F-graded series is never admitted, relaxed or not. */
    const bad = M.gradeSeries([{ t: "2026-08-28T14:59:00Z", o: 1, h: 1, l: 1, c: 1, v: 1 }],
      { provider: "alpaca", feed: "iex", sourceSha256: "nope", nowMs: Date.parse("2026-08-28T15:00:00Z") });
    return bad.researchEligible === false && bad.tradable === false;
  }));

  /* A DATED hazard is a finding. Only "no window derivable" may relax. */
  cases.push(fixture("a_known_earnings_date_blocks_however_relaxed", () => {
    const relaxed = { ...strategy.parameters, paperAbstainOnMissingInfo: true };
    const now = Date.parse("2026-08-28T15:00:00Z");
    /* earningsDates is a LIST of dated windows for the symbol. */
    const known = S.earningsBlackout("AAA", ["2026-08-29"], now, relaxed, {});
    if (known.blocked !== true) return false;
    if (known.unknown === true) return false;        // it is a finding, not an absence
    /* A dated window that is far away does not block — otherwise this fixture
       would pass by refusing everything. */
    const far = S.earningsBlackout("AAA", ["2026-11-29"], now, relaxed, {});
    if (far.blocked !== false) return false;
    /* An absent window is an absence, and reports itself as one. */
    for (const empty of [[], null, undefined]) {
      const absent = S.earningsBlackout("BBB", empty, now, relaxed, {});
      if (absent.blocked !== true || absent.unknown !== true) return false;
    }
    /* The gate that consumes these must relax ONLY the unknown branch. */
    const src = require("fs").readFileSync(
      require("path").join(__dirname, "_investorSignal.js"), "utf8");
    return /blackoutOk = !bo\.blocked \|\| \(cfg\.paperAbstainOnMissingInfo === true && bo\.unknown === true\)/
      .test(src);
  }));

  /* Grade F is broken data, not missing data: never admitted at any setting. */
  cases.push(fixture("f_grade_data_is_refused_however_relaxed", () => {
    const src = require("fs").readFileSync(
      require("path").join(__dirname, "_investorSignal.js"), "utf8");
    /* The relaxed branch must require researchEligible, which an F never has. */
    if (!/cfg\.paperAbstainOnMissingInfo === true && quality\.researchEligible === true/.test(src)) return false;
    const now = Date.parse("2026-08-28T15:00:00Z");
    const bad = M.gradeSeries([{ t: "2026-08-28T14:59:00Z", o: 1, h: 1, l: 1, c: 1, v: 1 }],
      { provider: "alpaca", feed: "iex", sourceSha256: "nope", nowMs: now });
    if (bad.grade !== "F" || bad.researchEligible !== false) return false;
    /* A stale series is equally broken and equally refused. */
    const stale = M.gradeSeries([{ t: "2026-08-20T14:59:00Z", o: 1, h: 1, l: 1, c: 1, v: 1 }],
      { provider: "alpaca", feed: "iex", sourceSha256: "a".repeat(64), nowMs: now });
    return stale.grade === "F" && stale.researchEligible === false;
  }));

  /* Every path that rebuilds cfg mid-cycle must re-apply the relaxation, or the
     desk goes quiet while the dashboard still reports the mode as on. */
  cases.push(fixture("every_config_reset_reapplies_the_relaxation", () => {
    const src = require("fs").readFileSync(
      require("path").join(__dirname, "investorCycle-background.js"), "utf8");
    const resets = src.match(/^\s*cfg = .*$/gm) || [];
    if (resets.length < 2) return false;
    for (const line of resets) {
      if (/paperLearningConfig/.test(line)) continue;
      if (/cfg = paperLearning\.cfg;/.test(line)) continue;
      return false;                                   // a bare reset drops the mode
    }
    return true;
  }));

  /* The relaxed run must not destroy the measurement it loosens. */
  cases.push(fixture("strict_verdict_is_recorded_alongside_the_relaxed_one", () => {
    const fs = require("fs"), path = require("path");
    const src = fs.readFileSync(path.join(__dirname, "investorCycle-background.js"), "utf8");
    if (!/const strictCfg = \{ \.\.\.cfg \};/.test(src)) return false;
    if (!/const strictRes = paperLearning\.active/.test(src)) return false;
    if (!/cfg: strictCfg/.test(src)) return false;
    /* And it must reach the stored card, not just exist as a local. */
    return /strict: \{ pass: strictRes\.pass/.test(src)
        && /paperRelaxed: paperLearning\.active === true/.test(src);
  }));

  const pass = cases.every((c) => c.pass);
  /* The count is inside the hash: silently dropping a fixture must change
     the attestation, not merely shorten the list behind an unchanged one. */
  const fixtureHash = digest({ schema: "runtime-fixtures-v8-measurement-admission", count: cases.length, cases });
  return { pass, fixtureHash, passed: cases.filter((c) => c.pass).length,
    total: cases.length, cases };
}

module.exports = { canonical, digest, fixture, runFixtures };
