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
/* Netlify's esbuild output contains executable module functions, but it does
 * not guarantee that the original sibling source files remain on disk.
 * Function#toString keeps source-attestation checks inside the deployed
 * bundle instead of turning a successful build into twelve ENOENT failures. */
function sourceOf(value) {
  return typeof value === "function" ? Function.prototype.toString.call(value) : "";
}
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
    return V.VARIANTS.length === 15 && [6, 12, 24].every((x) => windows.has(x))
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
  cases.push(fixture("nonblocking_risk_floor_cannot_erase_moderate_risk", () => {
    const now = Date.now();
    const p = I.decisionPolicy({
      coverage: { monitored: true, complete: true, asOfMs: now },
      events: [{ direction: -1, evidenceEligible: true, adverseRiskScore: 40,
        corroboration: {}, title: "moderate adverse event" }],
      asOfMs: now, decisionMatrixPolicy: { nonBlockingRiskFloor: 99 },
    });
    return p.decisionMatrixPolicy.nonBlockingRiskFloor === 25
      && p.decisionRiskScore === 40 && p.sizeMultiplier < 1;
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
    return d.trials === 15 && d.pass && d.probability >= 0.95;
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

  cases.push(fixture("alpaca_delayed_sip_is_exactly_15m_and_execution_grade", () => {
    const now = Date.parse("2026-08-28T15:30:00Z");
    const choice = M.normalizeMarketChoice("ALPACA", "DELAYED_SIP");
    const cfg = M.providerConfig("alpaca", "delayed_sip", { sipRealtime: false });
    const win = M.alpacaWindow("5Min", 120, "delayed_sip",
      { nowMs: now, sipRealtime: false });
    const q = M.gradeSeries([{
      t: "2026-08-28T15:15:00Z", o: 10, h: 10.2, l: 9.9, c: 10.1, v: 1000,
    }], { provider: "alpaca", feed: "delayed_sip",
      sourceSha256: "a".repeat(64), nowMs: now });
    return choice.provider === "alpaca" && choice.feed === "delayed_sip"
      && cfg.delayMinutes === 15 && cfg.consolidated === true
      && cfg.liquidityEligible === true && Date.parse(win.end) === now
      && q.grade === "B" && q.tradable === true && q.researchEligible === true;
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
    /* Basic IEX is real-time; Basic SIP is embargoed. A paid SIP entitlement
       must be explicit and then retains recency. */
    const iexLag = (Date.now() - Date.parse(day.end)) / 60000;
    const basicSip = (Date.now() - Date.parse(
      M.alpacaWindow("1Day", 1300, "sip", { sipRealtime: false }).end)) / 60000;
    const paidSip = (Date.now() - Date.parse(
      M.alpacaWindow("1Day", 1300, "sip", { sipRealtime: true }).end)) / 60000;
    return iexLag < 2 && basicSip >= 15 && basicSip < 25 && paidSip < 2;
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
    const API = require("./investorApi"), K = require("./investorKick");
    const body = sourceOf(API.ACTIONS.runCycleNow);
    /* Assert the REFUSAL, not a mention: `killSwitch: !!ctrl.killSwitch` also
       appears in the dispatch payload, so a bare name test still passes with
       the guard deleted. Both switches must return before dispatching. */
    const dispatchAt = body.indexOf("K.dispatch(");
    const beforeDispatch = dispatchAt > 0 ? body.slice(0, dispatchAt) : body;
    if (!/\.describe\(ctrl\)/.test(beforeDispatch)) return false;
    if (!/if \(operating\w*\.paused\)\s*return \{ ok: false/.test(beforeDispatch)) return false;
    if (!/manual: true/.test(body)) return false;
    const kick = sourceOf(K.dispatch);
    /* The slot id and the manual id must be different expressions. */
    if (!/manual \? `\$\{task\}_\$\{account\}_manual_/.test(kick)) return false;
    /* And direct HTTP to the scheduled dispatcher stays refused. */
    return /isScheduledInvocation\w*\(event\)/.test(sourceOf(K.handler));
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
    const src = sourceOf(F.fetchPublic);
    if (!/"Accept": acceptHeader\(accept\)/.test(src)) return false;
    return !/"Accept": accept \? accept\.join/.test(src);
  }));

  /* ── v8.8: paper learning mode ──────────────────────────────────────── */

  /* This entire application is paper-only. Relaxation must behave identically
     in observation-only and simulated-ledger states, while the strict config
     remains untouched for the counterfactual verdict. */
  cases.push(fixture("paper_learning_applies_to_both_paper_states", () => {
    const ST = require("./_investorStrategy");
    const req = { enabled: true, abstainOnMissingInfo: true, costMarginMultiple: 0 };
    const ledger = ST.paperLearningConfig(ST.parameters, { dryRun: false, paperLearning: req });
    if (ledger.active !== true || ledger.refused !== null) return false;
    if (ledger.cfg.paperAbstainOnMissingInfo !== true) return false;
    if (ledger.cfg.requireCalibratedEdge !== false) return false;
    if (ledger.cfg.paperObservationSizeFloor !== 0.10) return false;
    if (ST.parameters.requireCalibratedEdge !== true) return false;
    /* And it stays off unless explicitly enabled. */
    const off = ST.paperLearningConfig(ST.parameters, { dryRun: true });
    if (off.active !== false || off.cfg.paperAbstainOnMissingInfo === true) return false;
    const on = ST.paperLearningConfig(ST.parameters, { dryRun: true, paperLearning: req });
    if (on.active !== true || on.cfg.paperAbstainOnMissingInfo !== true) return false;
    const args = { cumResidual: -0.20, advUsd: 1e9, grade: "A",
      wideSpreadWindow: false, vixNorm: 1, reversionMult: 1 };
    const exploratory = S.costHurdle({ ...args, cfg: on.cfg });
    const strict = S.costHurdle({ ...args, cfg: ST.parameters });
    return exploratory.pass === true && strict.pass === false
      && strict.calibratedNetLowerBoundBps === null;
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
    if (r.cfg.maxHoldDays !== 14) return false;
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
    /* Unknown correlation is also an absence and is admitted small; a known
       high-correlation regime is a finding and still blocks. */
    const unknownCor = S.effectiveDispersionGate(NaN, cfg);
    if (unknownCor.pass !== true || !(unknownCor.sizeMult > 0 && unknownCor.sizeMult < 1)) return false;
    const crowded = S.effectiveDispersionGate(99, cfg);
    if (crowded.pass !== false || crowded.state !== "stand_down") return false;
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
    const src = sourceOf(S.evaluateCandidate);
    return /blackoutOk\s*=\s*!bo\.blocked\s*\|\|\s*\(?cfg\.paperAbstainOnMissingInfo === true && bo\.unknown === true\)?/
      .test(src);
  }));

  /* Grade F is broken data, not missing data: never admitted at any setting. */
  cases.push(fixture("f_grade_data_is_refused_however_relaxed", () => {
    const src = sourceOf(S.evaluateCandidate);
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
    const src = sourceOf(require("./investorCycle-background").runCycle);
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
    const src = sourceOf(require("./investorCycle-background").runCycle);
    if (!/(?:const|let) strictCfg = \{ \.\.\.cfg \};/.test(src)) return false;
    if (!/const strictRes = paperLearning\.active/.test(src)) return false;
    if (!/cfg: strictCfg/.test(src)) return false;
    /* And it must reach the stored card, not just exist as a local. */
    return /strict:\s*\{\s*pass:\s*strictRes\.pass/.test(src)
        && /paperRelaxed: paperLearning\.active === true/.test(src);
  }));

  /* ── v8.8.1: audit hardening and chart-history repair ─────────────── */
  cases.push(fixture("holding_horizon_uses_exchange_sessions", () => {
    const held = M.tradingDaysHeld("2026-08-28T15:30:00Z", "2026-08-31T15:30:00Z");
    return Math.abs(held - 1) < 0.002;
  }));

  cases.push(fixture("daily_learning_waits_for_buffered_close", () => {
    const before = M.sessionState(new Date("2026-08-31T20:19:00Z"));
    const after = M.sessionState(new Date("2026-08-31T20:21:00Z"));
    return M.dailyFinalizationState(before).ready === false
      && M.dailyFinalizationState(after).ready === true;
  }));

  cases.push(fixture("paper_learning_never_overrides_the_authoritative_ledger_state", () => {
    const ST = require("./_investorStrategy");
    const id = { accountId: "paper-1", strategyVersion: "v8", universeVersion: "v1",
      strategyHash: "a".repeat(64), universeHash: "b".repeat(64), variantsHash: "c".repeat(64) };
    const commit = process.env.COMMIT_REF || process.env.DEPLOY_ID || "local";
    const ctrl = { enabled: true, dryRun: true, mode: "research",
      paperLearning: { enabled: true, ledgerEnabled: true }, fixturesPass: true,
      fixturesCommit: commit, safetyEpoch: { ...id, commit } };
    const config = ST.paperLearningConfig(ST.parameters, ctrl);
    const observation = L.controlAllowsEntry(ctrl, id);
    const permission = L.controlAllowsEntry({ ...ctrl, dryRun: false, mode: "approval",
      operatingState: "manual_approval" }, id);
    return config.active && config.cfg.requireCalibratedEdge === false
      && !observation.pass && permission.pass && permission.scope === "paper_ledger";
  }));

  cases.push(fixture("legacy_ledger_flag_cannot_override_authoritative_state", () => {
    const ST = require("./_investorStrategy"), API = require("./investorApi");
    const config = ST.paperLearningConfig(ST.parameters, { dryRun: true,
      paperLearning: { enabled: true, ledgerEnabled: false } });
    const api = sourceOf(API.ACTIONS.setControl)
      + sourceOf(API._closeEntryQueueForAttestation);
    return config.active === true && /\w+\.transition\(/.test(api)
      && /closeEntryQueue/.test(api);
  }));

  cases.push(fixture("shadow_experiment_binds_build_and_intelligence", () => {
    const base = { universeHash: "a".repeat(64), strategyHash: "b".repeat(64),
      variantsHash: "c".repeat(64), buildCommit: "one",
      intelligenceConfig: { watchlist: ["AAPL"] },
      marketIdentity: { provider: "alpaca", feed: "iex", adjustment: "split_and_dividend" } };
    const one = SH.experimentIdentity(base);
    const build = SH.experimentIdentity({ ...base, buildCommit: "two" });
    const watch = SH.experimentIdentity({ ...base,
      intelligenceConfig: { watchlist: ["MSFT"] } });
    return one.experimentHash !== build.experimentHash
      && one.experimentHash !== watch.experimentHash;
  }));

  cases.push(fixture("holdout_reuse_is_bound_to_full_lock_identity", () => {
    const base = { experimentHash: "a".repeat(64), leaderId: "A",
      confirmation: { startDate: "2024-01-01", endDate: "2024-06-30",
        sessions: 126, embargoSessions: 15, dataThroughDate: "2024-06-30" },
      variantsHash: "b".repeat(64), simulatorVersion: "sim-v1",
      calibrationOpts: { alpha: 0.01, k: V.VARIANTS.length, embargoSessions: 15,
        minTrain: 252, minCalibration: 126, minHoldout: 126 } };
    const first = C.holdoutLockTemplate(base);
    const changed = C.holdoutLockTemplate({ ...base,
      calibrationOpts: { ...base.calibrationOpts, alpha: 0.02 } });
    return /^[a-f0-9]{64}$/.test(first.lockHash) && first.lockHash !== changed.lockHash;
  }));

  cases.push(fixture("split_basis_change_is_quarantined", () => {
    const CA = require("./_investorCorporateActions");
    const split = CA.assessPositionMark({ position: { symbol: "KLAC", qty: 2,
      lastMarkUsd: 100, lastMarkProvenance: { adjustment: "split_and_dividend" } },
      currentPrice: 10, currentProvenance: { adjustment: "split_and_dividend" } });
    const ordinary = CA.assessPositionMark({ position: { symbol: "XYZ", qty: 2,
      lastMarkUsd: 100, lastMarkProvenance: { adjustment: "split_and_dividend" } },
      currentPrice: 61, currentProvenance: { adjustment: "split_and_dividend" } });
    return split.quarantine && split.shareRatio === 10 && ordinary.quarantine === false;
  }));

  cases.push(fixture("cash_dividend_ledger_legs_balance", () => {
    const d = L.cashDividendLegs(12.5, 0.48);
    return d.amountCents === 600 && L.assertBalanced(d.legs)
      && d.legs.some((x) => x.account === L.ACCT.CASH && x.amountCents === 600)
      && d.legs.some((x) => x.account === L.ACCT.DIVIDEND_INCOME && x.amountCents === -600);
  }));

  cases.push(fixture("chart_all_range_is_not_silently_capped", () => {
    const H = require("./_investorHistory");
    const rows = Array.from({ length: 420 }, (_, i) => ({
      date: new Date(Date.UTC(2024, 0, 1 + i)).toISOString().slice(0, 10),
      o: 10, h: 11, l: 9, c: 10.5, v: 1000,
    }));
    const API = require("./investorApi");
    const candidate = sourceOf(API.ACTIONS.candidate);
    const history = sourceOf(API.ACTIONS.history);
    const api = candidate + history;
    return H.chartSeries(rows).length === 420
      && !/series\.slice\(-180\)/.test(api)
      && /ensureDailyHistory\(/.test(candidate)
      && /ensureDailyHistory\(/.test(history);
  }));

  cases.push(fixture("daily_evidence_requires_finalization_token", () => {
    const SHADOW = require("./_investorShadow");
    const shadow = sourceOf(SHADOW.accumulate) + sourceOf(SHADOW.markAccounts);
    const feedback = sourceOf(require("./_investorDecisionFeedback").updateObservations);
    return /session\.dailyFinalized === true/.test(shadow)
      && /session\.dailyFinalized === true/.test(feedback)
      && /completePortfolioDay: false,[\s\S]{0,100}provisional: true/.test(shadow)
      && /old\.dailyFinalized === true/.test(shadow)
      && /markSetSha256/.test(shadow);
  }));

  cases.push(fixture("paper_nav_evidence_is_finalized_and_provenance_bound", () => {
    const cycle = sourceOf(require("./investorCycle-background").runCycle);
    const api = sourceOf(require("./investorApi").ACTIONS.performance);
    return /const markSetSha256 = sha256Json\d*\(navMarkSources\)/.test(cycle)
      && /finalized === true && row\.marksComplete === true/.test(api)
      && /returnAdmissible/.test(cycle);
  }));

  cases.push(fixture("final_nav_snapshot_follows_settlement", () => {
    const cycle = sourceOf(require("./investorCycle-background").runCycle);
    const settlementAt = cycle.indexOf("let settled =");
    const finalBookAt = cycle.indexOf("const marked2 = R.markedBook", settlementAt);
    const finalNavWriteAt = cycle.lastIndexOf("navMarkResult = await writeNavSnapshot({");
    const checkpointAt = cycle.indexOf("lastDailyFinalizeDate: session.date", finalNavWriteAt);
    return settlementAt > 0 && finalBookAt > settlementAt
      && finalNavWriteAt > finalBookAt && checkpointAt > finalNavWriteAt
      && /session\.dailyFinalized === true && !dailyAlreadyFinalized\s*&& finalBookReadComplete/.test(cycle);
  }));

  cases.push(fixture("multi_session_nav_jump_is_not_one_daily_return", () => {
    const stats = R.equityStats([
      { date: "2026-08-27", navUsd: 100, returnAdmissible: false },
      { date: "2026-08-28", navUsd: 101, returnAdmissible: true },
      { date: "2026-08-31", navUsd: 120, returnAdmissible: false },
      { date: "2026-09-01", navUsd: 121.2, returnAdmissible: true },
    ]);
    return stats.returnDays === 2 && stats.winDays === 2
      && stats.totalReturnPct === 21.2;
  }));

  /* ── v8.13: immediate, full-capital exploratory paper automation ────── */
  cases.push(fixture("exploratory_auto_is_explicit_automatic_paper_state", () => {
    const ST = require("./_investorState");
    const described = ST.describe({ operatingState: ST.STATES.EXPLORATORY_AUTO });
    const refused = ST.transition({}, ST.STATES.EXPLORATORY_AUTO,
      { source: "operator" });
    const allowed = ST.transition({}, ST.STATES.EXPLORATORY_AUTO,
      { source: "operator", validEpoch: true });
    return described.paperLedger && described.exploratoryAuto
      && described.automaticPaperEntries && !described.manualApproval
      && refused.ok === false && allowed.ok === true;
  }));

  cases.push(fixture("exploratory_policy_exposes_full_reconciled_virtual_account", () => {
    const policy = strategy.exploratoryAuto || {};
    const pc = policy.portfolioControls || {};
    const cfg = { ...strategy, portfolioControls: {
      ...(strategy.portfolioControls || {}), ...pc } };
    const admission = R.checkAdd({ symbol: "AAPL", sector: "hardware",
      proposedUsd: 100000, book: { count: 0, grossUsd: 0, grossPct: 0,
        rows: [], bySectorPct: {}, byClusterPct: {} },
      navUsd: 100000, cashUsd: 100000, cfg, dynamicCorrelations: {} });
    const learn = policy.paperLearningDefaults || {};
    return policy.startingNavUsd === 100000
      && policy.version === "exploratory-auto-v4"
      && learn.minAbsZ === 1.0 && learn.entryRank === 0.5
      && learn.costMarginMultiple === 0.25
      && learn.exitRank === 0.6 && learn.maxHoldDays === 3
      && pc.maxGrossExposurePct === 100 && pc.minCashPct === 0
      && pc.maxOpenPositions === 304
      && policy.autoApproval.unlimitedOrdersPerDay === true
      && admission.allow === true && admission.headroomUsd === 100000;
  }));

  cases.push(fixture("exploratory_approval_has_no_daily_or_position_quota", () => {
    const LD = require("./_investorLadder");
    const order = { paperLearningOnly: true, qty: 10, refPriceUsd: 100,
      decisionAtMs: Date.now() - 60e3,
      quality: { grade: "B", tradable: true, researchEligible: true },
      universeHash: "a".repeat(64), strategyHash: "b".repeat(64),
      variantsHash: "c".repeat(64),
      decisionMarketProvenance: { provider: "massive", feed: null,
        adjustment: "split_adjusted", sourceSha256: "d".repeat(64) },
      cost: { ratio: 0.5, calibratedNetLowerBoundBps: null },
      gates: [{ id: "active_cost", blocking: true, pass: true }],
      decisionContext: { strictVerdict: { pass: false,
        blockedBy: ["calibration"] } } };
    const accepted = LD.exploratoryAutoApproval(order, {
      operatingState: "exploratory_auto", book: { count: 303, grossPct: 99 },
      navUsd: 100000, cfg: strategy, dayCount: 999999 });
    const malformed = LD.exploratoryAutoApproval({ ...order,
      decisionMarketProvenance: { provider: "massive", sourceSha256: "bad" } }, {
      operatingState: "exploratory_auto", navUsd: 100000, cfg: strategy });
    const researchOnly = LD.exploratoryAutoApproval({ ...order,
      quality: { grade: "C", tradable: false, researchEligible: true } }, {
      operatingState: "exploratory_auto", navUsd: 100000, cfg: strategy });
    return accepted.approve === true && accepted.unlimitedOrdersPerDay === true
      && malformed.approve === false && researchOnly.approve === false;
  }));

  cases.push(fixture("bootstrap_runs_after_hours_and_authorizes_before_enrichment", () => {
    const K = require("./investorKick");
    const decision = K.decide({ bootstrapPending: true, enabled: true,
      paused: false, killSwitch: false, afterHoursCycles: false,
      cycleSeconds: 300, guardSeconds: 60, guardSecondsClosed: 900,
      evidenceEverySeconds: 900, lastCycleAt: Date.now(),
      lastGuardAt: Date.now(), lastEvidenceAt: Date.now() }, {
      tradingDay: false, open: false, phase: "closed", date: "2026-09-01",
    }, Date.now());
    const source = String(B.ensureBootstrapped);
    const commitAt = source.indexOf("bootstrapVersion: BOOTSTRAP_VERSION");
    const deferredAt = source.indexOf("if (!enrich)");
    const earningsAt = source.indexOf("await populateEarnings");
    return decision.tasks.includes("cycle") && commitAt > 0
      && deferredAt > commitAt && earningsAt > deferredAt;
  }));

  cases.push(fixture("active_paper_scans_are_not_silently_row_capped", () => {
    const guard = sourceOf(require("./_investorPositionGuard").runGuard);
    const cycle = sourceOf(require("./investorCycle-background").runCycle);
    const API = require("./investorApi");
    const api = sourceOf(API.ACTIONS.kill) + sourceOf(API.ACTIONS.setControl)
      + sourceOf(API._closeEntryQueueForAttestation);
    const cappedState = /\.where\(\s*["'](?:status|open)["'][\s\S]{0,120}?\.limit\(\s*(?:50|100|200)\s*\)/;
    const cappedOrders = /\.where\(\s*["']status["']\s*,\s*["']==["']\s*,\s*["'](?:proposed|approved)["']\s*\)[\s\S]{0,40}?\.limit\(\s*(?:50|100|200)\s*\)/;
    return !cappedState.test(guard) && !cappedOrders.test(cycle)
      && !cappedOrders.test(api);
  }));

  cases.push(fixture("signal_frontier_gets_company_research_priority", () => {
    const sources = require("./_investorIntelligenceSources");
    const rows = sources.focusSymbols({ candidates: [
      { symbol: "MSFT", rank: 0.02 }, { symbol: "AAPL", rank: 0.01 }],
      researchTier: [{ symbol: "AMD" }], max: 24 });
    return rows.length === 3 && rows[0].symbol === "AAPL"
      && rows[1].symbol === "MSFT" && rows[2].symbol === "AMD"
      && strategy.parameters.intelligenceCompaniesPerSweep === 6
      && strategy.parameters.intelligenceMaxFocus === 304
      && sources.MAX_WATCHLIST === 24;
  }));

  cases.push(fixture("exploratory_policy_is_bound_into_strategy_identity", () => {
    const original = B.strategyHash(strategy);
    const changed = B.strategyHash({ ...strategy,
      exploratoryAuto: { ...strategy.exploratoryAuto,
        startingNavUsd: 99999 } });
    return /^[a-f0-9]{64}$/.test(original) && original !== changed;
  }));

  /* ── exploratory activity layer (v9 / exploratory-auto-v3) ──────────── */
  const XP = require("./_investorExplore");

  /* A shared, gate-clean entry input. Every fixture below flips ONE thing. */
  const exploreNow = Date.parse("2026-09-01T15:00:00Z");   // 11:00 ET, regular session
  const exploreBase = () => ({
    symbol: "AAA", rank: 0.2, nowMs: exploreNow,
    zStat: { z: -1.6, cumResidual: -0.012, window: 12 },
    quality: { grade: "B", tradable: true, researchEligible: true, reasons: [] },
    advUsd: 2e9, earningsDates: ["2026-11-20"], earningsEstimated: false,
    cause: S.CAUSE.NONE, vixNorm: 1, cor3m: 20,
    sectorTailFraction: 0.1, turnoverPctile: 0.4,
    session: { open: true, wideSpreadWindow: false, phase: "regular" },
    position: null, historyContext: null, reversion: null,
  });
  const exploreRelaxed = { ...strategy.parameters, ...strategy.exploratoryAuto.paperLearningDefaults,
    paperAbstainOnMissingInfo: true, paperObservationSizeFloor: 0.1, requireCalibratedEdge: false };
  const gateOf = (res, id) => (res.gates || []).find((g) => g.id === id) || {};
  const intelWith = ({ asOfMs, events = [], temporalMissing = [] }) => ({
    coverage: { monitored: true, complete: true, asOfMs },
    events,
    temporalContext: { asOfMs, coverage: { requiredMissing: temporalMissing },
      schedule: { nearest: null }, hazards: { active: [], riskScore: 0 },
      seasonality: { status: temporalMissing.length ? "warming" : "ready", riskScore: 0 },
      drivers: { drivers: [] } },
  });

  cases.push(fixture("temporal_absence_relaxes_in_paper_mode_but_never_a_material_finding", () => {
    /* Fresh, complete dossier; temporal layer still warming = an ABSENCE. */
    const warming = intelWith({ asOfMs: exploreNow - 3600e3, temporalMissing: ["seasonality_warming"] });
    const relaxed = S.evaluateCandidate({ ...exploreBase(), intelligence: warming, cfg: exploreRelaxed });
    if (gateOf(relaxed, "intelligence").pass !== true) return false;
    if (relaxed.intelligencePolicy.relaxedMissingInformation !== true) return false;
    if (!(relaxed.intelligencePolicy.sizeMultiplier >= 0.2)) return false;
    /* The strict policy still refuses the same absence. */
    const strict = S.evaluateCandidate({ ...exploreBase(), intelligence: warming, cfg: strategy.parameters });
    if (gateOf(strict, "intelligence").pass !== false) return false;
    /* A stale dossier with a trivial adverse mention is an absence too. */
    const trivial = intelWith({ asOfMs: exploreNow - 10 * 3600e3,
      events: [{ direction: -1, evidenceEligible: true, adverseRiskScore: 5, impactScore: 5,
        probabilityTrue: 0.5, probabilityMaterial: 0.2, corroboration: { independentGroups: 1 } }] });
    const trivialRes = S.evaluateCandidate({ ...exploreBase(), intelligence: trivial, cfg: exploreRelaxed });
    if (gateOf(trivialRes, "intelligence").pass !== true) return false;
    /* A stale dossier with a MATERIAL adverse score is a finding: never relaxed. */
    const material = intelWith({ asOfMs: exploreNow - 10 * 3600e3,
      events: [{ direction: -1, evidenceEligible: true, adverseRiskScore: 40, impactScore: 40,
        probabilityTrue: 0.8, probabilityMaterial: 0.7, corroboration: { independentGroups: 2 } }] });
    const materialRes = S.evaluateCandidate({ ...exploreBase(), intelligence: material, cfg: exploreRelaxed });
    if (gateOf(materialRes, "intelligence").pass !== false) return false;
    /* A fresh, complete temporal read that refuses entry (risk >= 70) is a finding. */
    const hot = intelWith({ asOfMs: exploreNow - 3600e3 });
    hot.temporalContext.recurring = { riskScore: 90, active: [{ name: "x" }],
      calendarMonth: new Date(exploreNow).getUTCMonth() + 1 };
    const hotPolicy = T.temporalPolicy(hot.temporalContext, { asOfMs: exploreNow });
    if (hotPolicy.entryAllowed !== false || hotPolicy.complete !== true || hotPolicy.fresh !== true) return false;
    const hotRes = S.evaluateCandidate({ ...exploreBase(), intelligence: hot, cfg: exploreRelaxed });
    return gateOf(hotRes, "intelligence").pass === false
      && S.INTEL_FINDING_RISK_FLOOR === 25;
  }));

  cases.push(fixture("control_cohort_may_fail_only_signal_gates", () => {
    /* No signal: rank in the middle, z mild. Every hazard gate clean. */
    const quiet = S.evaluateCandidate({ ...exploreBase(), rank: 0.8,
      zStat: { z: 0.2, cumResidual: 0.001, window: 12 }, cfg: exploreRelaxed });
    if (quiet.pass !== false || !XP.controlEligible(quiet)) return false;
    if (!quiet.blockedBy.every((id) => XP.CONTROL_MAY_FAIL.includes(id))) return false;
    /* A dated earnings window is a hazard: not control-eligible. */
    const hazard = S.evaluateCandidate({ ...exploreBase(), rank: 0.8,
      zStat: { z: 0.2, cumResidual: 0.001, window: 12 },
      earningsDates: ["2026-09-02"], cfg: exploreRelaxed });
    if (XP.controlEligible(hazard)) return false;
    /* A passing signal candidate is not a control candidate. */
    const signal = S.evaluateCandidate({ ...exploreBase(), cfg: exploreRelaxed });
    if (signal.pass !== true || XP.controlEligible(signal)) return false;
    /* The ladder approves a control order that failed only signal gates and
       refuses one that failed a hazard gate; a non-control order with a
       failed gate is refused as before. */
    const base = { paperLearningOnly: true, qty: 1, refPriceUsd: 100, decisionAtMs: Date.now() - 60e3,
      quality: { grade: "B", tradable: true, researchEligible: true },
      universeHash: "a".repeat(64), strategyHash: "b".repeat(64), variantsHash: "c".repeat(64),
      decisionMarketProvenance: { provider: "alpaca", feed: "delayed_sip",
        adjustment: "split_and_dividend", sourceSha256: "d".repeat(64) },
      cost: { ratio: 0.1, calibratedNetLowerBoundBps: null },
      decisionContext: { strictVerdict: { pass: false, blockedBy: ["signal"] } } };
    const ctx = { operatingState: "exploratory_auto", book: { count: 3, grossPct: 2 },
      navUsd: 100000, cfg: strategy };
    const controlOk = LD.exploratoryAutoApproval({ ...base,
      learningCohort: XP.COHORT_CONTROL,
      decisionContext: { ...base.decisionContext, cohortRole: "control" },
      gates: quiet.gates }, ctx);
    const controlBad = LD.exploratoryAutoApproval({ ...base,
      learningCohort: XP.COHORT_CONTROL,
      decisionContext: { ...base.decisionContext, cohortRole: "control" },
      gates: hazard.gates }, ctx);
    const signalBad = LD.exploratoryAutoApproval({ ...base, gates: quiet.gates }, ctx);
    return controlOk.approve === true && controlOk.cohortRole === "control"
      && controlOk.cohort === XP.COHORT_CONTROL
      && controlBad.approve === false && signalBad.approve === false;
  }));

  cases.push(fixture("exploratory_scoreboard_shrinks_toward_prior_and_selection_is_deterministic", () => {
    const sel = XP.activityPolicy(strategy).policySelection;
    const empty = XP.buildScoreboard([], { policySelection: sel, policyIds: ["A", "B"] });
    if (empty.closedExploratoryTrades !== 0) return false;
    if (empty.policies.A.n !== 0 || empty.policies.A.posteriorMeanBps !== sel.priorMeanBps) return false;
    const trade = (net, ids, role = "signal") => ({ paperLearningOnly: true,
      learningCohort: role === "control" ? XP.COHORT_CONTROL : XP.COHORT_SIGNAL,
      netBps: net, decisionContext: { explorationPolicyIds: ids, cohortRole: role } });
    const trades = [];
    for (let i = 0; i < 20; i += 1) trades.push(trade(50, ["A"]));
    for (let i = 0; i < 20; i += 1) trades.push(trade(-40, ["B"]));
    for (let i = 0; i < 10; i += 1) trades.push(trade(0, [], "control"));
    /* A strict-only trade is never counted. */
    trades.push({ paperLearningOnly: false, learningCohort: "strict_policy", netBps: 9999, variantId: "A" });
    const sb = XP.buildScoreboard(trades, { policySelection: sel, policyIds: ["A", "B"] });
    if (sb.closedExploratoryTrades !== 50) return false;
    const a = sb.policies.A, b = sb.policies.B;
    if (!(a.n === 20 && a.meanNetBps === 50 && a.posteriorMeanBps > 0 && a.posteriorMeanBps < 50)) return false;
    if (!(b.n === 20 && b.posteriorMeanBps < 0 && b.posteriorMeanBps > -40)) return false;
    if (!(a.posteriorSdBps < sel.priorSdBps)) return false;
    if (sb.cohorts.control.n !== 10 || sb.cohorts.signal.n !== 40) return false;
    if (!sb.signalVsControl || typeof sb.signalVsControl.differenceBps !== "number") return false;
    if (sb.leadingPolicyId !== "A") return false;
    /* Same seed, same order; the ordering is a function of the evidence. */
    const cands = [{ sym: "X", policyIds: ["B"], utilityBps: 5 }, { sym: "Y", policyIds: ["A"], utilityBps: 5 },
      { sym: "Z", policyIds: ["A", "B"], utilityBps: 1 }];
    const r1 = XP.rankCandidates(cands, sb, { seed: "cycle-1", policySelection: sel });
    const r2 = XP.rankCandidates(cands, sb, { seed: "cycle-1", policySelection: sel });
    if (r1.map((r) => r.sym).join() !== r2.map((r) => r.sym).join()) return false;
    /* With this much evidence A's draw beats B's in the overwhelming majority
       of seeds; assert it over a small seed family rather than one draw. */
    let aFirst = 0;
    for (let k = 0; k < 40; k += 1) {
      const r = XP.rankCandidates(cands, sb, { seed: `s${k}`, policySelection: sel });
      if (r[0].sym !== "X") aFirst += 1;
    }
    if (aFirst < 30) return false;
    /* Control selection is deterministic per seed and bounded by limit. */
    const pool = ["P", "Q", "R", "S"].map((sym) => ({ sym }));
    const c1 = XP.selectControl(pool, { seed: "cycle-1", limit: 2 });
    const c2 = XP.selectControl(pool, { seed: "cycle-1", limit: 2 });
    return c1.length === 2 && c1.map((x) => x.sym).join() === c2.map((x) => x.sym).join()
      && XP.selectControl(pool, { seed: "cycle-1", limit: 0 }).length === 0;
  }));

  cases.push(fixture("deploy_rollover_requires_unchanged_frozen_identity", () => {
    const codeStrategy = B.strategyDocument
      ? B.strategyDocument(strategy)
      : Object.fromEntries(Object.entries(strategy).filter(([, v]) => typeof v !== "function"));
    const sHash = B.strategyHash(codeStrategy), vHash = V.variantsHash();
    const control = { accountId: "paper-1", strategyVersion: strategy.version, strategyHash: sHash,
      universeVersion: "u1", universeHash: "u".repeat(64), variantsHash: vHash,
      autoExploratoryAuthorized: true, operatingState: "exploratory_auto",
      safetyEpoch: { accountId: "paper-1", strategyVersion: strategy.version, strategyHash: sHash,
        universeVersion: "u1", universeHash: "u".repeat(64), variantsHash: vHash, commit: "old" } };
    const ok = B.epochRolloverEligible(control, { commit: "new", codeStrategy, variantsHash: vHash });
    if (ok.eligible !== true) return false;
    const same = B.epochRolloverEligible(control, { commit: "old", codeStrategy, variantsHash: vHash });
    if (same.eligible !== false) return false;
    const variantsChanged = B.epochRolloverEligible(control, { commit: "new", codeStrategy, variantsHash: "x".repeat(64) });
    const strategyChanged = B.epochRolloverEligible(control, { commit: "new",
      codeStrategy: { ...codeStrategy, parameters: { ...codeStrategy.parameters, entryRank: 0.11 } }, variantsHash: vHash });
    const notAuthorised = B.epochRolloverEligible({ ...control, autoExploratoryAuthorized: false },
      { commit: "new", codeStrategy, variantsHash: vHash });
    const paused = B.epochRolloverEligible({ ...control, killSwitch: true },
      { commit: "new", codeStrategy, variantsHash: vHash });
    return variantsChanged.eligible === false && strategyChanged.eligible === false
      && notAuthorised.eligible === false && paused.eligible === false;
  }));

  /* ── cross-cycle exposure, shadow parity, strict evidence isolation ──── */
  cases.push(fixture("pending_orders_count_against_every_portfolio_cap", () => {
    const sectorOf = () => "semi";
    const book = { count: 0, grossUsd: 0, grossPct: 0, bySectorPct: {}, byClusterPct: {}, rows: [] };
    const pending = [
      { symbol: "AAA", status: "approved", qty: 10, refPriceUsd: 100, orderId: "o1" },
      { symbol: "BBB", status: "proposed", qty: 20, refPriceUsd: 50, orderId: "o2" },
      { symbol: "CCC", status: "filled", qty: 5, refPriceUsd: 10 },          // not pending
      { symbol: "AAA", status: "proposed", qty: 1, refPriceUsd: 100 },       // duplicate symbol
    ];
    const fold = R.foldPendingOrders(book, pending, 100000, sectorOf);
    if (fold.orders !== 2 || fold.usd !== 2000 || fold.proposedUnreservedUsd !== 1000) return false;
    if (book.count !== 2 || book.grossUsd !== 2000 || Math.abs(book.bySectorPct.semi - 2) > 1e-9) return false;
    /* The caps now bind on the committed book: a third name is refused by the
       position count, and a duplicate of a pending name is refused. */
    const cfg = { ...strategy, portfolioControls: { ...strategy.portfolioControls,
      maxOpenPositions: 2, requireDynamicCorrelation: false } };
    const third = R.checkAdd({ symbol: "DDD", sector: "semi", proposedUsd: 1000, book,
      navUsd: 100000, cashUsd: 97000, cfg, dynamicCorrelations: {} });
    const dup = R.checkAdd({ symbol: "BBB", sector: "semi", proposedUsd: 1000, book,
      navUsd: 100000, cashUsd: 97000, cfg: strategy, dynamicCorrelations: {} });
    return third.allow === false && third.blockedBy.includes("max_positions")
      && dup.allow === false && dup.blockedBy.includes("duplicate");
  }));

  cases.push(fixture("shadow_book_includes_pending_entries_at_whole_shares", () => {
    const rows = SH.positionRows([
      { symbol: "AAA", pendingEntry: true, qty: 3, signalPrice: 100, entryPrice: null, sector: "semi" },
      { symbol: "BBB", pendingEntry: false, qty: 2, signalPrice: 50, entryPrice: 51, sector: "semi" },
      { symbol: "CCC", pendingEntry: true, qty: 0, signalPrice: 10, sector: "semi" },
    ]);
    if (rows.length !== 2) return false;
    const a = rows.find((r) => r.symbol === "AAA"), b = rows.find((r) => r.symbol === "BBB");
    if (!(a && a.pending === true && a.entryPriceUsd === 100 && a.qty === 3)) return false;
    if (!(b && b.pending === false && b.entryPriceUsd === 51)) return false;
    /* Whole shares: the entry path floors, and the simulator version names it. */
    const src = sourceOf(SH.evaluateEntries);
    return /Math\.floor\(permittedNotionalUsd \/ c\.price\)/.test(src)
      && /whole-shares|structural-pullback-exits/.test(SH.SIMULATOR_VERSION)
      && /tradingDaysHeld\(p\.filledAt \|\| p\.openedAt/.test(sourceOf(SH.evaluateExits));
  }));

  cases.push(fixture("promotion_evidence_admits_only_strict_current_identity_trades", () => {
    const id = { strategyHash: "s".repeat(64), universeHash: "u".repeat(64), variantsHash: "v".repeat(64) };
    const base = { ...id, paperLearningOnly: false, learningCohort: "strict_policy", cohortRole: "signal", netBps: 10 };
    if (LD.strictTradeAdmissible(base, id) !== true) return false;
    if (LD.strictTradeAdmissible({ ...base, learningCohort: null, cohortRole: null }, id) !== true) return false; // legacy rows
    const refused = [
      { ...base, paperLearningOnly: true },
      { ...base, learningCohort: "exploratory_auto_unvalidated" },
      { ...base, learningCohort: "relaxed_operator_paper" },
      { ...base, cohortRole: "control" },
      { ...base, strategyHash: "x".repeat(64) },
      { ...base, variantsHash: "x".repeat(64) },
    ];
    if (!refused.every((t) => LD.strictTradeAdmissible(t, id) === false)) return false;
    /* Bound to the policy being promoted: a baseline trade never counts for a
       promoted challenger, and only trades opened after the forward lock do. */
    const aTrade = { ...base, variantId: "A", openedAt: "2026-09-01T15:00:00Z" };
    const bTrade = { ...base, variantId: "B", openedAt: "2026-09-01T15:00:00Z" };
    if (LD.strictTradeAdmissible(aTrade, id, { leaderId: "B" }) !== false) return false;
    if (LD.strictTradeAdmissible(bTrade, id, { leaderId: "B" }) !== true) return false;
    if (LD.strictTradeAdmissible(bTrade, id, { leaderId: "B", sinceMs: Date.parse("2026-09-02T00:00:00Z") }) !== false) return false;
    if (LD.strictTradeAdmissible(bTrade, id, { leaderId: null }) !== false) return false;   // no leader → baseline only
    /* And the cycle feeds the ladder through that filter, not the raw count. */
    const cycle = sourceOf(require("./investorCycle-background").runCycle);
    /* Source assertions are whitespace-tolerant: Netlify's esbuild reprints
       multi-line calls and objects, so the deployed bundle's text differs
       from the file on disk (v16 shipped three exact-text checks that passed
       on disk and failed inside the bundle, freezing every entry). */
    return /\w+\.strictTradeAdmissible\(\s*t,\s*\{\s*strategyHash,\s*universeHash,\s*variantsHash\s*\},\s*\{\s*leaderId:\s*evidenceLeaderId,\s*sinceMs:\s*evidenceSinceMs\s*\}\s*\)/.test(cycle)
      && /cohortCostMeter\(accountId, \{ admit: strictTradeAdmit \}\)/.test(cycle)
      && /closedRealTrades: closedReal\.size/.test(cycle)
      && /if \(strictTradeAdmit\(d\.data\(\)\)\) closedStrict \+= 1/.test(cycle);
  }));

  cases.push(fixture("worker_lease_exceeds_platform_ceiling_and_heartbeats", () => {
    const CYC = require("./investorCycle-background");
    const LEASE = require("./_investorLease");
    if (!(Number(CYC.WORKER_LEASE_TTL_MS) > 15 * 60 * 1000)) return false;
    const handler = sourceOf(CYC.handler), guard = sourceOf(require("./_investorPositionGuard").runGuard);
    return /\w+\.setActive\(\s*\{\s*jobRef,\s*accountLeaseRef:\s*accountCycleLeaseRef,\s*leaseOwner/.test(handler)
      && /\w+\.clear\(\)/.test(handler)
      && /await \w+\.heartbeat\(nowMs\);/.test(sourceOf(CYC.reportRunProgress))
      && /heartbeat\(Date\.now\(\)\)/.test(sourceOf(require("./_investorPositionGuard").guardProgress))
      /* The lease is a live singleton: when the cycle re-attests fixtures
         mid-run it IS active, so its state is never asserted here (v17 did,
         and froze the desk on the first scan after every Start). */
      && typeof LEASE.heartbeat === "function" && typeof LEASE.active === "function" && guard.length > 0;
  }));

  /* ── round 3: order lifetime, fail-closed exposure, chunked fetch ────── */
  cases.push(fixture("stale_proposals_are_refused_by_every_automatic_approval_path", () => {
    const now = Date.parse("2026-09-01T15:00:00Z");
    const fresh = LD.proposalFreshness({ decisionAtMs: now - 5 * 60e3, expiresAtMs: now + 3600e3,
      decisionSessionDate: "2026-09-01" }, { nowMs: now, sessionDate: "2026-09-01" });
    if (fresh.fresh !== true) return false;
    const ancient = LD.proposalFreshness({ decisionAtMs: now - 2435 * 864e5, decisionSessionDate: "2019-12-31" },
      { nowMs: now, sessionDate: "2026-09-01" });
    if (ancient.fresh !== false || ancient.reasons.length < 2) return false;
    const expired = LD.proposalFreshness({ decisionAtMs: now - 10 * 60e3, expiresAtMs: now - 60e3,
      decisionSessionDate: "2026-09-01" }, { nowMs: now, sessionDate: "2026-09-01" });
    if (expired.fresh !== false) return false;
    const yesterday = LD.proposalFreshness({ decisionAtMs: now - 30 * 60e3, decisionSessionDate: "2026-08-31" },
      { nowMs: now, sessionDate: "2026-09-01" });
    if (yesterday.fresh !== false) return false;
    /* Both automatic approval paths refuse the ancient proposal. */
    const order = { paperLearningOnly: true, qty: 10, refPriceUsd: 100, decisionAtMs: now - 2435 * 864e5,
      decisionSessionDate: "2019-12-31",
      quality: { grade: "B", tradable: true, researchEligible: true },
      universeHash: "a".repeat(64), strategyHash: "b".repeat(64), variantsHash: "c".repeat(64),
      decisionMarketProvenance: { provider: "alpaca", feed: "delayed_sip", adjustment: "split_and_dividend", sourceSha256: "d".repeat(64) },
      cost: { ratio: 2, calibratedNetLowerBoundBps: 5 },
      gates: [{ id: "signal", blocking: true, pass: true }],
      cause: S.CAUSE.ABNORMAL_ACTIVITY,
      decisionContext: { strictVerdict: { pass: true, blockedBy: [] } } };
    const limited = LD.autoApproval(order, { stage: "limited_auto", book: { count: 0, grossPct: 0 },
      navUsd: 100000, cfg: strategy, dayCount: 0, nowMs: now, sessionDate: "2026-09-01" });
    const exploratory = LD.exploratoryAutoApproval(order, { operatingState: "exploratory_auto",
      book: { count: 0, grossPct: 0 }, navUsd: 100000, cfg: strategy, nowMs: now, sessionDate: "2026-09-01" });
    if (limited.approve !== false || !/old|expired|session/.test(limited.detail)) return false;
    if (exploratory.approve !== false || !/old|expired|session/.test(exploratory.detail)) return false;
    /* And the ledger refuses an expired proposal at approval, in source. */
    const ledgerSrc = sourceOf(L.approveOrder);
    return /proposal expired before approval/.test(ledgerSrc)
      && /expiresAtMs: expiryMs/.test(sourceOf(L.proposeOrder));
  }));

  cases.push(fixture("pending_exposure_read_failure_closes_new_entries", () => {
    const cycle = sourceOf(require("./investorCycle-background").runCycle);
    if (!/freshAllocation && freshAllocation\.leaderId/.test(cycle)) return false;   // ladder bound to the fresh allocation
    return /pendingExposureUnavailable = String\(e\.message \|\| e\)/.test(cycle)
      && /entryControl = \{ pass: false, reason: `pending_exposure_unavailable/.test(cycle)
      && !/on failure, the recordFill double-fill guard still holds the line/.test(cycle);
  }));

  cases.push(fixture("roster_fetch_is_chunked_retried_and_reports_missing_symbols", () => {
    const src = sourceOf(M.fetchBarsChunked);
    if (!/for \(let attempt = 0; attempt <= retries; attempt \+= 1\)/.test(src)) return false;
    const list = ["AAA", "BBB", "CCC", "DDD"], chunks = [["AAA", "BBB"], ["CCC", "DDD"]];
    const ok = { provider: "alpaca", feed: "delayed_sip", adjustment: "split_and_dividend", pages: 1,
      manifestSha256: "1".repeat(64), bars: { AAA: [{ t: "x" }] }, fetchedAt: "2026-09-01T15:00:00Z" };
    const merged = M.mergeChunkResults(list, chunks, [ok, { error: "ECONNRESET" }], "alpaca");
    if (merged.chunks.failed.length !== 1 || merged.failedSymbols.join() !== "CCC,DDD") return false;
    if (merged.missingSymbols.join() !== "BBB") return false;          // answered chunk, omitted symbol
    if (merged.symbolSha256.AAA !== "1".repeat(64)) return false;     // provenance is its own chunk's hash
    if (!/^[a-f0-9]{64}$/.test(merged.manifestSha256)) return false;
    const twoOk = M.mergeChunkResults(list, chunks, [ok, { ...ok, manifestSha256: "2".repeat(64), bars: { CCC: [{ t: "y" }] } }], "alpaca");
    return twoOk.chunks.failed.length === 0 && twoOk.missingSymbols.join() === "BBB,DDD"
      && twoOk.symbolSha256.CCC === "2".repeat(64) && twoOk.manifestSha256 !== merged.manifestSha256;
  }));

  cases.push(fixture("dispatch_failure_restores_cadence_stamp_by_compare_and_set", () => {
    const src = sourceOf(require("./investorKick").dispatch);
    return /res = \{ ok: false, status: 0, thrown:/.test(src)
      && /if \(stored === now\) tx\.set\(cref, \{ \[stamp\]: previousStamp \}, \{ merge: true \}\)/.test(src)
      /* The module alias may be renamed by the bundler (A → A2); the
         transaction shape is what matters. */
      && /\w+\.runTransaction\(async \(tx\) => \{\s*const cur = await tx\.get\(cref\)/.test(src);
  }));

  cases.push(fixture("data_sufficiency_scores_sizes_and_orders_thin_data_without_refusing_it", () => {
    const DS = require("./_investorSufficiency");
    const full = DS.score({ sessionBarCount: 78, historyDays: 300, historyOk: true, earningsKnown: true,
      intelligenceState: "fresh_complete", cor3mKnown: true, advMeasured: true, grade: "B" });
    const thin = DS.score({ sessionBarCount: 26, historyDays: 0, historyOk: false, earningsKnown: false,
      intelligenceState: "none", cor3mKnown: false, advMeasured: false, grade: "B" });
    if (full.score !== 100 || full.bucket !== "high" || full.sizeMultiplier !== 1) return false;
    if (!(thin.score < 45 && thin.bucket === "low")) return false;
    if (!(thin.sizeMultiplier >= DS.SIZE_FLOOR && thin.sizeMultiplier < 0.75)) return false;   // smaller, never zero
    if (!(thin.orderingPenaltyBps > full.orderingPenaltyBps && thin.orderingPenaltyBps <= 20)) return false;
    if (!thin.missing.includes("earningsWindow") || !thin.missing.includes("intelligence")) return false;
    /* Ordering: with identical draws, the fully-known name is taken first. */
    const rows = XP.rankCandidates([
      { sym: "THIN", policyIds: ["A"], utilityBps: 5, penaltyBps: thin.orderingPenaltyBps },
      { sym: "FULL", policyIds: ["A"], utilityBps: 5, penaltyBps: full.orderingPenaltyBps }],
      null, { seed: "s", policySelection: { ...XP.DEFAULTS.policySelection, method: "utility" } });
    if (rows[0].sym !== "FULL") return false;
    /* Scoreboard splits signal trades by the sufficiency they were taken with. */
    const trade = (net, bucket) => ({ paperLearningOnly: true, learningCohort: XP.COHORT_SIGNAL, netBps: net,
      decisionContext: { explorationPolicyIds: ["A"], cohortRole: "signal",
        dataSufficiency: { score: bucket === "high" ? 90 : 30, bucket } } });
    const sb = XP.buildScoreboard([trade(10, "high"), trade(20, "high"), trade(-30, "low")], { policyIds: ["A"] });
    return sb.sufficiency.high.n === 2 && sb.sufficiency.low.n === 1 && sb.sufficiency.low.meanNetBps === -30
      && DS.ofRecord({ decisionContext: { dataSufficiency: { score: 80 } } }).bucket === "high";
  }));

  /* ── round 4: frozen sufficiency policy, scoreboard identity, legacy expiry, rounding ── */
  cases.push(fixture("sufficiency_policy_is_frozen_versioned_and_counts_same_session_bars", () => {
    const DS = require("./_investorSufficiency");
    const pol = DS.policyFrom(strategy);
    const declared = strategy.exploratoryAuto.activity.sufficiency;
    if (!declared || pol.version !== declared.version || pol.sizeFloor !== declared.sizeFloor) return false;
    /* Hash-bound: changing a weight changes the frozen strategy identity. */
    const h0 = B.strategyHash(strategy);
    const changed = JSON.parse(JSON.stringify(Object.fromEntries(Object.entries(strategy).filter(([, v]) => typeof v !== "function"))));
    changed.exploratoryAuto.activity.sufficiency.weights.intradayBars += 1;
    if (B.strategyHash(changed) === h0) return false;
    /* Same-session bars only: 4 current bars with a full prior day stored
       must score as 4 bars, not 82. */
    const thin = DS.score({ sessionBarCount: 4, barCount: 82, historyDays: 300, historyOk: true, earningsKnown: true,
      intelligenceState: "fresh_complete", cor3mKnown: true, advMeasured: true, grade: "B" }, pol);
    const full = DS.score({ sessionBarCount: 82, barCount: 82, historyDays: 300, historyOk: true, earningsKnown: true,
      intelligenceState: "fresh_complete", cor3mKnown: true, advMeasured: true, grade: "B" }, pol);
    if (!(thin.score < full.score && full.score === 100)) return false;
    if (thin.version !== declared.version || thin.kind !== "heuristic_coverage_score") return false;
    /* Kick-start floor: the thinnest name is still taken at >= 60% size. */
    const nothing = DS.score({ sessionBarCount: 0, historyDays: 0, historyOk: false, earningsKnown: false,
      intelligenceState: "none", cor3mKnown: false, advMeasured: false, grade: "C" }, pol);
    return nothing.sizeMultiplier >= 0.6 && nothing.sizeMultiplier > 0 && nothing.orderingPenaltyBps <= 20;
  }));

  cases.push(fixture("exploratory_scoreboard_admits_only_this_experiment_newest_first_and_hashes_the_sample", () => {
    const id = { strategyHash: "a".repeat(64), universeHash: "b".repeat(64), variantsHash: "c".repeat(64),
      exploratoryPolicyVersion: "exploratory-auto-v4", sufficiencyVersion: "data-sufficiency-v2" };
    const mk = (over) => ({ paperLearningOnly: true, learningCohort: XP.COHORT_SIGNAL, netBps: 1, tradeId: "t" + Math.random(),
      strategyHash: id.strategyHash, universeHash: id.universeHash, variantsHash: id.variantsHash,
      exploratoryPolicyVersion: "exploratory-auto-v4", closedAt: "2026-09-01T15:00:00Z",
      decisionContext: { dataSufficiency: { score: 50, version: "data-sufficiency-v2" } }, ...over });
    const trades = [
      mk({ tradeId: "a", closedAt: "2026-09-01T15:00:00Z" }),
      mk({ tradeId: "b", closedAt: "2026-09-02T15:00:00Z" }),
      mk({ tradeId: "c", strategyHash: "x".repeat(64) }),                       // other identity
      mk({ tradeId: "d", exploratoryPolicyVersion: "exploratory-auto-v3" }),  // older policy
      mk({ tradeId: "e", decisionContext: { dataSufficiency: { score: 50, version: "data-sufficiency-v1" } } }),
      mk({ tradeId: "f", paperLearningOnly: false, learningCohort: "strict_policy" }),
    ];
    trades.push(mk({ tradeId: "g", exploratoryPolicyVersion: undefined }));                       // missing policy version
    trades.push(mk({ tradeId: "h", decisionContext: { dataSufficiency: { score: 50 } } }));         // missing sufficiency version
    const r = XP.admitScoreboardTrades(trades, { ...id, lookback: 1 });
    if (r.count !== 1 || r.trades[0].tradeId !== "b") return false;             // newest first, lookback applied
    if (r.excluded.identity !== 1 || r.excluded.policyVersion !== 2 || r.excluded.sufficiencyVersion !== 2
        || r.excluded.notExploratory !== 1 || r.excluded.lookback !== 1) return false;
    /* An incomplete current identity admits nothing. */
    if (XP.admitScoreboardTrades(trades, { ...id, sufficiencyVersion: null }).count !== 0) return false;
    const r2 = XP.admitScoreboardTrades(trades, { ...id, lookback: 10 });
    const r3 = XP.admitScoreboardTrades(trades, { ...id, lookback: 10 });
    return r2.count === 2 && r2.sampleHash === r3.sampleHash && r2.sampleHash !== r.sampleHash
      && /^[a-f0-9]{64}$/.test(r2.sampleHash);
  }));

  cases.push(fixture("legacy_proposals_without_expiry_are_bounded_by_the_default_lifetime", () => {
    const src = sourceOf(L.approveOrder);
    return /legacy proposal without expiry/.test(src)
      && /decisionAt \+ DEFAULT_LIFETIME_MS/.test(src)
      && /proposal has no decision time and cannot be approved/.test(src)
      && /legacy proposal without an expiry rejected at bootstrap/.test(sourceOf(B.ensureBootstrapped));
  }));

  cases.push(fixture("portfolio_headroom_is_rounded_down_to_the_cent", () => {
    const book = { count: 0, grossUsd: 0, grossPct: 0, bySectorPct: {}, byClusterPct: {}, rows: [] };
    const cfg = { ...strategy, portfolioControls: { ...strategy.portfolioControls, requireDynamicCorrelation: false,
      maxGrossExposurePct: 60, minCashPct: 40 } };
    const add = R.checkAdd({ symbol: "AAA", sector: "semi", proposedUsd: 1e9, book, navUsd: 33333.33, cashUsd: 33333.33, cfg, dynamicCorrelations: {} });
    const cents = Math.round(add.headroomUsd * 100);
    return Math.abs(add.headroomUsd * 100 - cents) < 1e-6 && add.headroomUsd <= 33333.33 * 0.6 && add.permittedUsd === add.headroomUsd;
  }));

  /* ── Policy Q: pullback in trend ─────────────────────────────────────── */
  const qSeries = (() => {
    const bars = []; let px = 60; const d0 = Date.UTC(2025, 8, 1);
    for (let i = 0; i < 260; i++) {
      const date = new Date(d0 + i * 864e5).toISOString().slice(0, 10);
      if (i < 200) px += 0.2; else if (i < 230) px = 100 + (i - 200); else px = 130 - (i - 230) * 0.5;
      bars.push({ date, o: px, h: px * 1.005, l: px * 0.995, c: px, v: 1e6 });
    }
    return bars;
  })();

  cases.push(fixture("pullback_leg_is_measured_from_history_without_look_ahead", () => {
    const H = require("./_investorHistory");
    const ctx = H.contextFor(qSeries, null);
    if (!ctx.ok || !ctx.pullback) return false;
    const leg = ctx.pullback;
    if (!(leg.legLow === 100 && leg.legHigh === 130 && leg.legPct === 30 && leg.level50 === 115)) return false;
    if (!(Math.abs(leg.retracementPct - 48.3) < 0.2)) return false;
    if (ctx.sma50Rising !== true || ctx.aboveSma200 !== true || ctx.downtrend) return false;
    /* Cut the series before the leg's high existed: the leg must not know it. */
    const early = H.contextFor(qSeries, qSeries[215].date);
    if (!early.ok || !early.pullback || early.pullback.legHigh >= 130) return false;
    if (Math.abs(H.retracementAt(leg, 115) - 0.5) > 1e-9) return false;
    /* Too little of a window, or a low that is the latest close, yields no leg. */
    return H.pullbackLeg([1, 2, 3]) === null
      && H.pullbackLeg(Array.from({ length: 40 }, (_, i) => 100 - i)) === null;
  }));

  cases.push(fixture("policy_q_fires_only_inside_the_retracement_window_of_a_rising_trend", () => {
    const H = require("./_investorHistory");
    const ctx = H.contextFor(qSeries, null);
    const Q = V.configFor("Q");
    if (!Q.requirePullback || !Q.pullbackExit || Q.exitRank !== 0.9 || Q.maxHoldDays !== 14) return false;
    const inWindow = S.entrySignal(0.05, -2.5, Q, ctx, { price: 115 });
    if (inWindow.fire !== true || !inWindow.pullback || inWindow.pullback.legHigh !== 130) return false;
    if (S.entrySignal(0.05, -2.5, Q, ctx, { price: 128 }).fire !== false) return false;   // too shallow
    if (S.entrySignal(0.05, -2.5, Q, ctx, { price: 105 }).fire !== false) return false;   // level failed
    if (S.entrySignal(0.05, -2.5, Q, { ok: false }, { price: 115 }).fire !== false) return false;   // no history: declines
    if (S.entrySignal(0.05, -2.5, Q, { ...ctx, sma50Rising: false }, { price: 115 }).fire !== false) return false;
    if (S.entrySignal(0.05, -2.5, Q, { ...ctx, pullback: { ...ctx.pullback, legPct: 5 } }, { price: 115 }).fire !== false) return false;
    /* Without a live price the last close stands in (48% retrace → fires). */
    if (S.entrySignal(0.05, -2.5, Q, ctx).fire !== true) return false;
    /* Every other arm is untouched by the new conditions. */
    return V.VARIANTS.filter((v) => v.id !== "Q").every((v) =>
      S.entrySignal(0.05, -2.5, { ...V.configFor(v.id), requireAboveSma200: false, requireDrawdownPct: null }, ctx, { price: 105 }).fire === true)
      && V.VARIANTS.length === 15 && V.byId("Q").name === "Pullback in trend";
  }));

  cases.push(fixture("policy_q_exits_at_the_leg_high_or_when_the_level_fails_and_holds_through_rank_recovery", () => {
    const Q = V.configFor("Q"), A = V.configFor("A");
    const leg = { legHigh: 130, legLow: 100 };
    const target = S.exitSignal(0.3, 2, Q, { mark: 130.5, entry: 115, peak: 130.5, pullbackLeg: leg });
    const failed = S.exitSignal(0.3, 2, Q, { mark: 106, entry: 115, peak: 116, pullbackLeg: leg });
    const held = S.exitSignal(0.7, 2, Q, { mark: 120, entry: 115, peak: 121, pullbackLeg: leg });
    const hardStop = S.exitSignal(0.3, 2, Q, { mark: 105, entry: 115, peak: 116, pullbackLeg: leg });
    if (!(target.exit && target.kind === "pullback_target")) return false;
    if (!(failed.exit && failed.kind === "pullback_failed" && failed.urgent === true)) return false;
    if (held.exit !== false) return false;                                   // rank 0.7 < 0.9: continuation kept
    if (!(hardStop.exit && hardStop.kind === "stop_loss")) return false;      // the floor still comes first
    /* A leg without the declaring variant, or the variant without a leg, changes nothing. */
    if (S.exitSignal(0.7, 2, A, { mark: 120, entry: 115, peak: 121, pullbackLeg: leg }).kind !== "signal") return false;
    if (S.exitSignal(0.7, 2, Q, { mark: 120, entry: 115, peak: 121 }).exit !== false) return false;
    /* The shadow simulator freezes the leg at entry and judges the exit against it. */
    const entries = sourceOf(SH.evaluateEntries), exits = sourceOf(SH.evaluateExits);
    return /pullbackLeg:\s*params\.pullbackExit\s*&&/.test(entries)
      && /pullbackLeg:\s*p\.pullbackLeg\s*\|\|\s*null/.test(exits)
      && /structural-pullback-exits/.test(SH.SIMULATOR_VERSION);
  }));

  cases.push(fixture("attestation_freeze_names_itself_and_self_heals_when_fixtures_pass_again", () => {
    const src = sourceOf(B.ensureBootstrapped);
    const LGR = sourceOf(L.controlAllowsEntry);
    return /operatingStateSource:\s*"bootstrap:fixtures"/.test(src)
      && /fixture_freeze_lifted/.test(src)
      && /c\.operatingStateSource === "bootstrap:fixtures"/.test(src)
      && /frozen automatically/.test(LGR);
  }));

  cases.push(fixture("scan_snapshot_and_nightly_archive_record_what_the_desk_saw", () => {
    const K = require("./investorKick");
    const CYCLE = require("./investorCycle-background");
    const cycleSrc = sourceOf(CYCLE.runCycle);
    if (!/\w+\.col\(\w+\.COL\.scanSnapshots\)\.doc\(cycleId\)/.test(cycleSrc)) return false;
    if (!/panel:\s*panelOut/.test(cycleSrc) || !/scoreboard:\s*scoreboard \|\| null/.test(cycleSrc)) return false;
    if (typeof CYCLE.runArchive !== "function") return false;
    /* The archive is due once per trading day after the close buffer, and is
       not re-dispatched within 30 minutes; it is never due before the buffer
       or twice for the same date. */
    const base = { paused: false, killSwitch: false, enabled: true, cycleSeconds: 300, guardSeconds: 60,
      guardSecondsClosed: 900, evidenceEverySeconds: 900, lastCycleAt: Date.now(), lastGuardAt: Date.now(),
      lastEvidenceAt: Date.now(), bootstrapPending: false, lastDailyFinalizeDate: "2026-09-01" };
    const closed = { date: "2026-09-01", tradingDay: true, open: false, phase: "postmarket",
      regularCloseMinutesEt: 16 * 60, minutesEt: 17 * 60, isHalfDay: false };
    const due = K.decide({ ...base, lastArchiveDate: null, lastArchiveAt: null }, closed, Date.now());
    const done = K.decide({ ...base, lastArchiveDate: "2026-09-01", lastArchiveAt: null }, closed, Date.now());
    const recent = K.decide({ ...base, lastArchiveDate: null, lastArchiveAt: Date.now() - 5 * 60000 }, closed, Date.now());
    const early = K.decide({ ...base, lastArchiveDate: null, lastArchiveAt: null },
      { ...closed, open: true, phase: "regular", minutesEt: 12 * 60 }, Date.now());
    return due.tasks.includes("archive") && !done.tasks.includes("archive")
      && !recent.tasks.includes("archive") && !early.tasks.includes("archive");
  }));

  /* ── manual sell ──────────────────────────────────────────────────────
     The operator's Sell button is the one place a human decision enters the
     execution path. Three things must hold or it becomes a way to trade on a
     price already visible on the screen, which would invalidate every number
     this desk reports about itself. */
  cases.push(fixture("manual_sell_records_an_intent_and_never_fills", () => {
    const API = require("./investorApi");
    if (typeof API.ACTIONS.requestSell !== "function") return false;
    if (typeof API.ACTIONS.cancelSell !== "function") return false;
    const body = sourceOf(API.ACTIONS.requestSell);
    /* It writes a REQUEST on the position and touches nothing that executes. */
    if (!/manualExitRequest/.test(body)) return false;
    if (!/status:\s*"requested"/.test(body)) return false;
    if (/closePosition|applyLegs|firstEligibleBar|armExitIntent/.test(body)) return false;
    /* It refuses a symbol that is not open, and a quarantined price basis. */
    if (!/open !== true/.test(body)) return false;
    if (!/corporateActionPending/.test(body)) return false;
    /* An armed exit's clock is already running and cannot be withdrawn — that
       would let an operator cancel after seeing the price it would have got. */
    const cancel = sourceOf(API.ACTIONS.cancelSell);
    return /exitIntent[\s\S]{0,80}decisionAtMs/.test(cancel)
      && /return \{ ok: false, refused/.test(cancel);
  }));

  cases.push(fixture("manual_sell_enters_the_same_exit_clock_as_every_rule", () => {
    const G = require("./_investorPositionGuard");
    const src = sourceOf(G.runGuard);
    /* The request is turned into an exit at the SAME point a rule exit is, so
       it inherits armExitIntent, the feed delay and firstEligibleBar. A branch
       that armed or closed anywhere else would bypass all three. */
    if (!/manualExitRequest/.test(src)) return false;
    if (!/kind:\s*"manual"/.test(src)) return false;
    if (!/reason:\s*"manual_operator_sell"/.test(src)) return false;
    const armAt = src.indexOf("armExitIntent(");
    const manualAt = src.indexOf("manualPending");
    if (!(manualAt > 0 && armAt > manualAt)) return false;
    /* And the decision clock it inherits is still delay + latency. esbuild
       reprints numeric literals (60000 -> 6e4), so match the shape, not the
       spelling — a source assertion that only holds before bundling is the
       one kind of self-check that fails exclusively in production. */
    return /eligibleAfterMs\s*=\s*decisionAtMs\s*\+\s*feedCfg\.delayMinutes\s*\*\s*(?:60000|6e4)/.test(src)
      && /executionLatencyMs/.test(src);
  }));

  cases.push(fixture("hand_sold_trades_are_excluded_from_the_promotion_sample", () => {
    const LD = require("./_investorLadder");
    const identity = { strategyHash: "s", universeHash: "u", variantsHash: "v" };
    const base = { ...identity, variantId: "A", cohortRole: "signal",
      learningCohort: "strict_policy", openedAt: "2026-08-01T14:00:00.000Z" };
    if (!LD.strictTradeAdmissible({ ...base }, identity)) return false;
    if (LD.strictTradeAdmissible({ ...base, manualClose: true }, identity)) return false;
    if (LD.strictTradeAdmissible({ ...base, exitKind: "manual" }, identity)) return false;
    if (LD.strictTradeAdmissible({ ...base, closeReason: "manual_operator_sell" }, identity)) return false;
    /* The ledger has to carry the marker, or the exclusion above is unreachable. */
    const L = require("./_investorLedger");
    const close = sourceOf(L.closePosition);
    return /exitKind:\s*intent\.kind/.test(close) && /manualClose:/.test(close);
  }));

  /* The console's closed-trade rows drive the operator's whole history view.
     Reading only the legacy aliases left every row with a blank P&L and no
     sell price against a ledger that writes netRealizedCents / exitFillUsd. */
  cases.push(fixture("closed_trade_view_reads_the_fields_the_ledger_actually_writes", () => {
    const API = require("./investorApi");
    if (typeof API.closedTradeForUi !== "function") return false;
    /* Exactly the shape _investorLedger.closePosition writes. */
    const row = API.closedTradeForUi({ symbol: "AAA", qty: 4,
      entryFillUsd: 10, exitFillUsd: 9.5, netRealizedCents: -215,
      openedAt: "2026-08-20T14:00:00.000Z", closedAt: "2026-08-24T14:00:00.000Z",
      closeReason: "rank 0.612 crossed exit 0.6", exitKind: "signal" });
    if (row.entryPriceUsd !== 10 || row.exitPriceUsd !== 9.5) return false;
    if (Math.abs(row.realisedPnlUsd + 2.15) > 1e-9) return false;
    if (row.exitReason !== "rank 0.612 crossed exit 0.6") return false;
    if (row.exitKind !== "signal" || row.manualClose !== false) return false;
    const hand = API.closedTradeForUi({ symbol: "BBB", qty: 1,
      closeReason: "manual_operator_sell", exitKind: "manual" });
    return hand.manualClose === true;
  }));

  const pass = cases.every((c) => c.pass);
  /* The count is inside the hash: silently dropping a fixture must change
     the attestation, not merely shorten the list behind an unchanged one. */
  const SCHEMA = "runtime-fixtures-v21-strategy-v10-manual-exit";
  const fixtureHash = digest({ schema: SCHEMA, count: cases.length, cases });
  return { schema: SCHEMA, pass, fixtureHash, passed: cases.filter((c) => c.pass).length,
    total: cases.length, cases };
}

module.exports = { canonical, digest, fixture, runFixtures };
