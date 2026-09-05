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
    /* An asynchronous fixture is PENDING until runFixturesAsync awaits it. A
       synchronous caller sees it as a failure, never as a pass: forgetting to
       await must fail closed, not attest a promise object as true. */
    if (detail && typeof detail.then === "function") {
      return { name, check, pass: false, pending: true, promise: detail,
        error: "asynchronous fixture not awaited — use runFixturesAsync()" };
    }
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
    /* A dispatch error must reach the console as a refusal and release the
       one-minute click throttle; otherwise the button reports success even
       though no worker exists, then blocks the operator from retrying. */
    if (!/upstream >= 200 && upstream < 300/.test(body)) return false;
    if (!/run_cycle_now_dispatch_failed/.test(body)) return false;
    if (!/lastManualRunAtMs: 0/.test(body)) return false;
    if (!/return \{\s*ok: false,\s*task: wanted/.test(body)) return false;
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
    if (ledger.cfg.paperObservationSizeFloor !== ST.RELAX_LIMITS.observationSizeFloor.dflt) return false;
    if (!(ledger.cfg.paperObservationSizeFloor >= 0.35)) return false;
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
    /* Evidence not yet gathered is an absence: a paper desk may record it.
       An UNKNOWN cause is not an absence — it fails closed (D-1). */
    const pending = S.directionFromCause(S.CAUSE.NOT_YET_GATHERED, cfg, 0, null);
    if (pending.trade !== true || pending.relaxed !== true) return false;
    if (pending.confidence >= 0.5) return false;
    if (S.directionFromCause("something_unclassified", cfg, 0, null).trade !== false) return false;
    /* And with the mode off, pending still blocks. */
    const strict = S.directionFromCause(S.CAUSE.NOT_YET_GATHERED, strategy.parameters, 0, null);
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
      proposedUsd: 16000, book: { count: 0, grossUsd: 0, grossPct: 0,
        rows: [], bySectorPct: {}, byClusterPct: {} },
      navUsd: 100000, cashUsd: 100000, cfg, dynamicCorrelations: {} });
    const learn = policy.paperLearningDefaults || {};
    return policy.startingNavUsd === 100000
      && policy.version === "exploratory-auto-v12"
      && learn.minAbsZ === 1.25 && learn.entryRank === 0.5
      && learn.costMarginMultiple === 0.25
      && learn.exitRank === 0.75 && learn.maxHoldDays === 3
      && pc.maxGrossExposurePct === 95 && pc.minCashPct === 0
      && pc.maxOpenPositions === null && R.openPositionLimit(pc) === null
      && pc.ordinaryPositionPctOfNav === 16
      && pc.riskBudgetPerTradePctOfNav === 1.5
      && policy.autoApproval.unlimitedOrdersPerDay === true
      /* One ordinary position is admitted in full; the gross ceiling is the
         headroom, not the whole account, now that it is below 100%. */
      && admission.allow === true && admission.headroomUsd === 95000;
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
    /* Must clear the LIVE exploratory bar, which v12 raised to |z| >= 1.75
       in the bottom 30% of the cross-section. A base that only cleared the
       old 1.0 would silently stop representing a qualifying candidate. */
    zStat: { z: -2.1, cumResidual: -0.016, window: 12 },
    quality: { grade: "B", tradable: true, researchEligible: true, reasons: [] },
    advUsd: 2e9, earningsDates: ["2026-11-20"], earningsEstimated: false,
    cause: S.CAUSE.NONE, vixNorm: 1, cor3m: 20,
    sectorTailFraction: 0.1, turnoverPctile: 0.4,
    session: { open: true, wideSpreadWindow: false, phase: "regular" },
    position: null, historyContext: null, reversion: null,
    /* Selection is now five COMPLETED sessions; the hour only confirms. A base
       candidate must therefore carry a passing five-session picture, or it no
       longer represents a qualifying company. Absence legitimately blocks. */
    sessionMove: { ok: true, r5Pct: -7.4, sessionZ: -1.9, sessionRank: 0.05,
      normalSwingPct: 3.1, marketPartPct: -0.9, sectorPartPct: -1.2, companyPartPct: -5.3 },
    sessionVerdict: { pass: true, reason: "five-session fall in a rising trend" },
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
      && /if \(strictTradeAdmit\(d\.data\(\)\)\)\s*closedStrict \+= 1/.test(cycle);
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
      && /if \(stored === now\)\s*tx\.set\(cref, \{ \[stamp\]: previousStamp \}, \{ merge: true \}\)/.test(src)
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
      exploratoryPolicyVersion: "exploratory-auto-v12", sufficiencyVersion: "data-sufficiency-v2" };
    const mk = (over) => ({ paperLearningOnly: true, learningCohort: XP.COHORT_SIGNAL, netBps: 1, tradeId: "t" + Math.random(),
      strategyHash: id.strategyHash, universeHash: id.universeHash, variantsHash: id.variantsHash,
      exploratoryPolicyVersion: "exploratory-auto-v12", closedAt: "2026-09-01T15:00:00Z",
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

  /* ── two-tier cadence ─────────────────────────────────────────────────── */
  cases.push(fixture("scheduled_deep_scan_fires_once_per_new_york_slot_and_catches_up", () => {
    const K = require("./investorKick");
    const base = { paused: false, killSwitch: false, enabled: true, bootstrapPending: false,
      planMode: "scheduled", planTimesEt: ["09:50", "13:00"], lastPlanKey: null,
      cycleSeconds: 300, guardSeconds: 60, guardSecondsClosed: 900, evidenceEverySeconds: 900,
      lastCycleAt: Date.now(), lastGuardAt: Date.now(), lastEvidenceAt: Date.now(),
      lastDailyFinalizeDate: "2026-09-02", lastArchiveDate: "2026-09-02" };
    const at = (min) => ({ date: "2026-09-02", tradingDay: true, open: true, phase: "regular",
      minutesEt: min, regularOpenMinutesEt: 570, regularCloseMinutesEt: 960, isHalfDay: false });
    const early = K.decide(base, at(9 * 60 + 47), Date.now());                    // before the first slot
    const due = K.decide(base, at(9 * 60 + 51), Date.now());                      // 09:50 passed, never run
    const ran = K.decide({ ...base, lastPlanKey: "2026-09-02 09:50" }, at(10 * 60), Date.now());
    const second = K.decide({ ...base, lastPlanKey: "2026-09-02 09:50" }, at(13 * 60 + 2), Date.now());
    const catchUp = K.duePlanSlot(base, at(14 * 60 + 10));                        // desk came up late: one scan, for 13:00
    const done = K.decide({ ...base, lastPlanKey: "2026-09-02 13:00" }, at(15 * 60), Date.now());
    const yesterday = K.duePlanSlot({ ...base, lastPlanKey: "2026-09-01 13:00" }, at(9 * 60 + 55));
    const closed = K.decide(base, { date: "2026-09-02", tradingDay: true, open: false, phase: "postmarket",
      minutesEt: 17 * 60, regularCloseMinutesEt: 960 }, Date.now());
    const lateSlot = K.normalizePlanTimes(["15:55", "09:30", "12:00", "nonsense", "12:00"]);
    return !early.tasks.includes("cycle") && due.tasks.includes("cycle")
      && !ran.tasks.includes("cycle") && second.tasks.includes("cycle")
      && catchUp && catchUp.time === "13:00" && !done.tasks.includes("cycle")
      && yesterday && yesterday.time === "09:50" && !closed.tasks.includes("cycle")
      && JSON.stringify(lateSlot) === JSON.stringify(["12:00"]);
  }));

  cases.push(fixture("unspecified_plan_mode_keeps_five_minute_full_universe_scans", () => {
    const K = require("./investorKick");
    if (K.resolvePlanMode(undefined) !== "interval") return false;
    if (K.resolvePlanMode(null) !== "interval") return false;
    if (K.resolvePlanMode("scheduled") !== "scheduled") return false;
    if (K.scheduledMode({ planMode: undefined, planTimesEt: ["09:50"] })) return false;
    const now = 1_000_000;
    const ctrl = { paused: false, killSwitch: false, enabled: true, bootstrapPending: false,
      planMode: K.resolvePlanMode(undefined), planTimesEt: ["09:50", "13:00"],
      cycleSeconds: 300, guardSeconds: 60, guardSecondsClosed: 900,
      evidenceEverySeconds: 900, lastCycleAt: now - 600000, lastGuardAt: now,
      lastEvidenceAt: now, lastDailyFinalizeDate: "2026-09-02", lastArchiveDate: "2026-09-02" };
    const session = { date: "2026-09-02", tradingDay: true, open: true, phase: "regular",
      minutesEt: 11 * 60, regularOpenMinutesEt: 570, regularCloseMinutesEt: 960,
      isHalfDay: false };
    return K.decide(ctrl, session, now).tasks.includes("cycle");
  }));

  cases.push(fixture("strike_pass_runs_every_minute_and_is_independent_of_the_deep_scan", () => {
    const K = require("./investorKick");
    const base = { paused: false, killSwitch: false, enabled: true, bootstrapPending: false,
      planMode: "scheduled", planTimesEt: ["09:50", "13:00"], lastPlanKey: "2026-09-02 09:50",
      cycleSeconds: 300, guardSeconds: 60, guardSecondsClosed: 900, evidenceEverySeconds: 900,
      lastCycleAt: Date.now(), lastEvidenceAt: Date.now(), lastDailyFinalizeDate: "2026-09-02",
      lastArchiveDate: "2026-09-02" };
    const open = { date: "2026-09-02", tradingDay: true, open: true, phase: "regular",
      minutesEt: 11 * 60, regularCloseMinutesEt: 960 };
    const now = Date.now();
    const due = K.decide({ ...base, lastGuardAt: now - 61000 }, open, now);
    const notDue = K.decide({ ...base, lastGuardAt: now - 20000 }, open, now);
    /* netlify.toml must fire the dispatcher every minute or the 60s clock is fiction. */
    const toml = require("fs").existsSync(require("path").join(__dirname, "..", "..", "netlify.toml"))
      ? require("fs").readFileSync(require("path").join(__dirname, "..", "..", "netlify.toml"), "utf8") : null;
    const cronOk = toml === null || /\[functions\."investorKick"\]\s*\n\s*schedule = "\* \* \* \* \*"/.test(toml);
    return due.tasks.includes("guard") && !due.tasks.includes("cycle")
      && !notDue.tasks.includes("guard") && cronOk;
  }));

  cases.push(fixture("armed_level_is_the_price_where_the_residual_breaches_and_strikes_stay_in_band", () => {
    const STK = require("./_investorStrike");
    /* cum -0.6%, sigma 1%, threshold z 1 -> needs a further -0.4%: 100 -> 99.60 */
    const level = STK.armLevel({ lastPrice: 100, cumResidual: -0.006, residVol: 0.01, minAbsZ: 1 });
    const breached = STK.armLevel({ lastPrice: 100, cumResidual: -0.012, residVol: 0.01, minAbsZ: 1 });
    const far = STK.armLevel({ lastPrice: 100, cumResidual: 0.05, residVol: 0.01, minAbsZ: 1, maxArmDropPct: 5 });
    const plan = { armBelowUsd: 99.6, floorUsd: 97.11 };
    const above = STK.strikeVerdict(99.7, plan), at = STK.strikeVerdict(99.6, plan);
    const inBand = STK.strikeVerdict(98, plan), gap = STK.strikeVerdict(96.5, plan);
    const pick = STK.selectPlans([{ ok: true, symbol: "B", dropPct: 2 }, { ok: true, symbol: "A", dropPct: 1 },
      { ok: false, symbol: "C" }, { ok: true, symbol: "D", dropPct: 3 }], { maxArmedPlans: 2 }, { exclude: new Set(["A"]) });
    return level.ok && level.armBelowUsd === 99.6 && level.dropPct === 0.4
      && !breached.ok && breached.reason === "already_breached"
      && !far.ok && far.reason === "too_far"
      && !above.strike && at.strike && inBand.strike && !gap.strike && gap.gap === true
      && pick.selected.map((x) => x.symbol).join(",") === "B,D";
  }));

  cases.push(fixture("plan_candidate_requires_every_hazard_gate_and_a_clearing_cost_at_the_level", () => {
    const STK = require("./_investorStrike");
    const XPL = require("./_investorExplore");
    /* The armed tier is RETIRED in the frozen strategy (v15): entries are made
       at the market price or not at all. Its code is kept so the record of
       what it did stays readable, so this fixture enables it explicitly and
       separately asserts that the shipped policy leaves it off. */
    const shipped = XPL.activityPolicy(strategy).strike;
    const policy = { ...shipped, enabled: true };
    /* The relaxed exploratory configuration the deep scan actually plans
       with: paper learning lifts the calibration requirement. */
    const cfg = { ...strategy.parameters, minAbsZ: 1.0, entryRank: 0.5, costMarginMultiple: 0.25,
      paperAbstainOnMissingInfo: true, requireCalibratedEdge: false };
    const gates = (fail) => ["quality", "session", "dispersion", "blackout", "liquidity", "signal", "trend",
      "turnover", "intelligence", "evidence", "cost"].map((id) => ({ id, label: id, pass: !fail.includes(id), blocking: true }));
    const common = { symbol: "NVDA", strictRes: { pass: false, blockedBy: ["signal"], firstBlock: "Residual signal" },
      strictUncalibratedRes: null, z: { z: -0.6, cumResidual: -0.006, residVol: 0.01, window: 12 }, rank: 0.3,
      last: { c: 100, t: "2026-09-02T15:00:00Z" }, cfg, policy,
      quality: { tradable: true, grade: "B" }, advUsd: 5e9, sector: "semi", historyContext: { ok: true, atrPct: 2 },
      reversion: { multiplier: 1 }, dataSufficiency: { sizeMultiplier: 1, score: 80, bucket: "high" }, intelligence: null,
      cause: "pending", coverage: null, decisionManifest: { manifestHash: "a".repeat(64), version: "decision-input-manifest-v1" },
      marketProvenance: { provider: "alpaca", feed: "delayed_sip", adjustment: "all", sourceSha256: "b".repeat(64) },
      sessionCloseMs: Date.parse("2026-09-02T20:00:00Z"), vixNorm: 1, session: { date: "2026-09-02", open: true },
      policyIdentity: { accountId: "paper-1", strategyVersion: "v11", universeVersion: "v1",
        strategyHash: "c".repeat(64), universeHash: "d".repeat(64), variantsHash: "e".repeat(64) },
      variantId: "A", exploratoryPolicyVersion: "exploratory-auto-v12", cohortLabel: "exploratory_auto_unvalidated",
      paperLearningOnly: true, activePortfolioControls: strategy.exploratoryAuto.portfolioControls,
      activity: XPL.activityPolicy(strategy), cycleId: "2026-09-02_1500", strategyVersion: "v11",
      operatingState: "exploratory_auto", moveStartedAtMs: Date.parse("2026-09-02T14:00:00Z"), positionScale: 1 };
    const sizing = { volScaler: 1, dispersionMult: 1, causeConfidence: 0.2, intelligenceMult: 0.2, combined: 0.2 };
    const nearMiss = STK.planCandidate({ ...common, evalRes: { pass: false, blockedBy: ["signal", "cost"], gates: gates(["signal", "cost"]), sizing } });
    const hazard = STK.planCandidate({ ...common, evalRes: { pass: false, blockedBy: ["signal", "trend"], gates: gates(["signal", "trend"]), sizing } });
    const passed = STK.planCandidate({ ...common, evalRes: { pass: true, blockedBy: [], gates: gates([]), sizing } });
    const highRank = STK.planCandidate({ ...common, rank: 0.8, evalRes: { pass: false, blockedBy: ["signal"], gates: gates(["signal"]), sizing } });
    const researchOnly = STK.planCandidate({ ...common, quality: { tradable: false, researchEligible: true, grade: "C" },
      evalRes: { pass: false, blockedBy: ["signal"], gates: gates(["signal"]), sizing } });
    return nearMiss.ok && nearMiss.plan.armBelowUsd === 99.6 && nearMiss.plan.floorUsd < nearMiss.plan.armBelowUsd
      && nearMiss.plan.expiresAtMs === Date.parse("2026-09-02T19:45:00Z")
      && nearMiss.plan.decisionManifestHash === "a".repeat(64) && nearMiss.plan.strictVerdict.pass === false
      && !hazard.ok && hazard.reason === "blocked_by_trend"
      && !passed.ok && !highRank.ok && highRank.reason === "rank_above_plan_ceiling"
      && !researchOnly.ok && researchOnly.reason === "execution_source_not_tradable"
      && policy.maxArmedPlans === 6 && policy.strikeBandPct === 2.5
      && shipped.enabled === false;
  }));

  cases.push(fixture("exit_levels_are_the_rule_engine_in_dollars", () => {
    const EX = require("./_investorExitPolicy");
    const cfg = { stopLossPct: -8, trailingStopPct: -4, trailingArmsAtPct: 3, takeProfitPct: null, maxHoldDays: 3 };
    const flat = EX.exitLevels(cfg, { entry: 100, peak: 101, heldDays: 1 });
    const armed = EX.exitLevels(cfg, { entry: 100, peak: 105, heldDays: 2.5 });
    const stopHit = EX.exitSignal(undefined, 1, cfg, { mark: flat.stopUsd - 0.01, entry: 100, peak: 100 });
    const trailHit = EX.exitSignal(undefined, 1, cfg, { mark: armed.trailStopUsd - 0.01, entry: 100, peak: 105 });
    return flat.stopUsd === 92 && flat.trailArmUsd === 103 && flat.trailArmed === false && flat.trailStopUsd === null
      && flat.targetUsd === null && flat.sessionsLeft === 2
      && armed.trailArmed === true && armed.trailStopUsd === 100.8 && armed.sessionsLeft === 0.5
      && stopHit.exit && stopHit.kind === "stop_loss" && trailHit.exit && trailHit.kind === "trailing_stop"
      && EX.exitLevels(cfg, { entry: 0 }) === null;
  }));

  cases.push(fixture("plain_reasons_are_a_closed_five_word_vocabulary_covering_every_rule", () => {
    const PR = require("./_investorPlainReason");
    const XP = require("./_investorExitPolicy");
    const vocab = PR.vocabulary();
    /* The five-word ceiling is the whole point: it has to be scannable. */
    const allShort = [...vocab.buy, ...vocab.sell]
      .every((r) => r.text && r.text.trim().split(/\s+/).length <= PR.MAX_WORDS && r.detail);
    /* Every exitKind the policy can emit must have a label, or a real sale
       would render as a fallback that explains nothing. */
    const emitted = [...new Set((String(XP.exitSignal).match(/kind: "([a-z_]+)"/g) || [])
      .map((m) => m.replace(/kind: "|"/g, "")))];
    const covered = emitted.every((k) => !!PR.SELL_REASONS[k]) && !!PR.SELL_REASONS.manual;
    /* Every cause the signal lane can assign must map to a buy label. */
    const S2 = require("./_investorSignal");
    /* Every cause that CAN produce a buy maps to a buy label. evidence_unavailable
       never can (invariant I-1), so its absence from the buy vocabulary is the
       invariant, not a gap. */
    const causesCovered = Object.values(S2.CAUSE).filter((c) => c !== S2.CAUSE.UNAVAILABLE)
      .every((c) => !!PR.BUY_REASONS[PR.CAUSE_TO_BUY[c]]) && !PR.CAUSE_TO_BUY[S2.CAUSE.UNAVAILABLE];
    /* Routing: control beats cause; a legacy row with no exitKind recovers
       its rule from the stored sentence; an open position is not "sold". */
    const control = PR.buyReason({ cohortRole: "control", cause: S2.CAUSE.NONE }).code === "control";
    const quiet = PR.buyReason({ cause: S2.CAUSE.NONE }).code === "quiet_drop";
    const unknown = PR.buyReason({}).code === "peer_gap";
    const legacy = PR.sellReason({ closeReason: "down -8.3% — hard stop at -8%" }).code === "stop_loss";
    const open = PR.sellReason({ open: true }).code === "open";
    const exiting = PR.sellReason({ open: true, exitIntent: { decisionAtMs: 1, kind: "stop_loss" } }).pending === true;
    const manual = PR.sellReason({ manualClose: true, exitKind: "signal" }).code === "manual";
    /* Held time is in fractional SESSIONS, so a fifth of one is ~80 minutes
       of market time, not five hours of wall clock. */
    const held = PR.heldText(0.2) === "1.3 hrs" && PR.heldText(1) === "1 session"
      && PR.heldText(2.5) === "2.5 sessions" && PR.heldText(null) === "—";
    return allShort && covered && causesCovered && control && quiet && unknown
      && legacy && open && exiting && manual && held && emitted.length >= 9;
  }));

  cases.push(fixture("concentrated_book_deploys_the_account_and_the_cap_is_what_binds", () => {
    const pc = strategy.exploratoryAuto.portfolioControls;
    const learn = strategy.exploratoryAuto.paperLearningDefaults;
    const pl = strategy.paperLearningConfig({ ...strategy.parameters }, { paperLearning: { ...learn } });
    const cfg = pl.cfg;
    /* The floor must survive every clamp site, or the information haircut
       silently binds instead of the position cap and every position is a
       few hundred dollars. */
    if (cfg.paperObservationSizeFloor !== 0.35) return false;
    if (strategy.PAPER_OBSERVATION_FLOOR_MAX < 0.35) return false;
    const raw = S.combineSizeMultipliers({ volScaler: 1, dispersionMult: 1,
      causeConfidence: 0.2, intelligenceMult: 0.2 });
    const combined = Math.max(raw.combined, cfg.paperObservationSizeFloor);
    const sized = (sufficiency) => R.positionSizeUsd({ navUsd: 100000, atrPct: 1.7,
      expectedShortfall5dPct: 3, overnightGapEsPct: 2,
      signalScaler: Math.min(1, combined * sufficiency * cfg.positionScale),
      cfg: { portfolioControls: pc, parameters: cfg } }).usd;
    const good = sized(1), thin = sized(0.6);
    /* Five full positions is about 80% invested; the sixth is trimmed by the
       gross ceiling rather than refused. */
    const fiveInvestedPct = 5 * pc.ordinaryPositionPctOfNav;
    /* The risk budget must not bind below the cap, or the cap is decorative. */
    const riskLimited = (100000 * pc.riskBudgetPerTradePctOfNav / 100) / (8 / 100);
    const capUsd = 100000 * pc.ordinaryPositionPctOfNav / 100;
    return good === capUsd && good >= 10000 && thin > 0 && thin < good
      && riskLimited > capUsd
      && fiveInvestedPct === 80 && R.openPositionLimit(pc) === null
      && pc.maxGrossExposurePct === 95;
  }));

  cases.push(fixture("patience_sleeve_is_earned_bounded_and_never_weakens_a_protection", () => {
    const PA = require("./_investorPatience");
    const XP2 = require("./_investorExitPolicy");
    const policy = PA.policyFrom(strategy);
    if (!(policy.enabled && policy.sleevePctOfNav >= 10 && policy.sleevePctOfNav <= 15)) return false;
    /* The grant is tied to the horizon the reversion evidence is measured
       over; granting more sessions than the statistic covers would be
       extrapolating past the measurement. */
    const HIST = require("./_investorHistory");
    if (policy.grantSessions !== HIST.REVERSION_HORIZON) return false;

    /* EARNED: every bar is about the company's OWN record. */
    const strong = { n: 24, winRate: 0.71, shrunkPct: 1.8, weightOwn: 0.55 };
    if (!PA.assess(strong, policy).eligible) return false;
    const rejects = [
      { n: 3, winRate: 0.9, shrunkPct: 3, weightOwn: 0.13 },       // too few events
      { n: 30, winRate: 0.42, shrunkPct: 1.5, weightOwn: 0.6 },    // rarely recovers
      { n: 30, winRate: 0.7, shrunkPct: -0.4, weightOwn: 0.6 },    // recovers downward
      { n: 30, winRate: 0.7, shrunkPct: 1.5, weightOwn: 0.05 },    // carried by the pool
    ];
    if (rejects.some((r) => PA.assess(r, policy).eligible)) return false;
    /* A months-long downtrend is not a short-term slump. */
    if (PA.assess(strong, policy, { historyContext: { ok: true, downtrend: true } }).eligible) return false;

    /* BOUNDED, at cost. A held patient position and an unfilled patient order
       both consume the sleeve; a falling position must not free up room. */
    const held = [{ open: true, symbol: "AAA", qty: 100, entryPriceUsd: 80,
      lastMarkUsd: 40, patience: { granted: true } }];
    const pend = [{ status: "approved", symbol: "CCC", qty: 10, refPriceUsd: 100,
      patience: { granted: true } }];
    const usage = PA.sleeveUsage(held, pend, 100000, policy);
    if (usage.capUsd !== 12000 || usage.usedUsd !== 9000 || usage.roomUsd !== 3000) return false;
    const fits = PA.grant({ reversion: strong, policy, proposedUsd: 3000,
      positions: held, pendingOrders: pend, navUsd: 100000 });
    const overflows = PA.grant({ reversion: strong, policy, proposedUsd: 3001,
      positions: held, pendingOrders: pend, navUsd: 100000 });
    if (!fits || fits.granted !== true || overflows !== null) return false;
    /* A qualifying name that does not fit is taken on ORDINARY terms, not
       refused — grant() returning null is not a veto on the trade. */

    /* THE CONCESSIONS, and only these two. */
    const cfg = { stopLossPct: -8, trailingStopPct: -4, trailingArmsAtPct: 3,
      maxHoldDays: 3, exitRank: 0.75 };
    const patient = { patience: { granted: true, grantSessions: policy.grantSessions } };
    const terms = (h, pnl) => PA.exitTerms(patient, { heldDays: h, pnlPct: pnl });
    const rankDown = XP2.exitSignal(0.9, 0.4, cfg, { mark: 98.5, entry: 100, peak: 100, patience: terms(0.4, -1.5) });
    const rankUp = XP2.exitSignal(0.9, 0.4, cfg, { mark: 102, entry: 100, peak: 102, patience: terms(0.4, 2) });
    const rankPlain = XP2.exitSignal(0.9, 0.4, cfg, { mark: 98.5, entry: 100, peak: 100 });
    if (rankDown.exit !== false || rankDown.patienceHeld !== true) return false;   // held while under
    if (rankUp.kind !== "signal") return false;                                     // up: sells anyway
    /* v18: a losing position is held off the rank exit whether or not it is
       patient — a loss is closed by a rule about money, never by the ranking.
       What patience still buys, and what this fixture exists to protect, is
       the EXTENDED TIME WINDOW asserted immediately below. */
    if (rankPlain.exit !== false || rankPlain.heldThroughRank !== true) return false;
    /* And with the loss protection explicitly off, the rank exit still fires
       for a non-patient position, so the two mechanisms stay distinguishable. */
    const strictRank = XP2.exitSignal(0.9, 0.4, { ...cfg, holdLosersThroughRankExit: false },
      { mark: 98.5, entry: 100, peak: 100 });
    if (strictRank.kind !== "signal") return false;
    /* Past the window it is an ordinary position again. */
    if (XP2.exitSignal(0.9, 9, cfg, { mark: 98.5, entry: 100, peak: 100, patience: terms(9, -1.5) }).exit !== true) return false;

    /* NEVER WEAKENS A PROTECTION. Each of these must still fire for a
       patient, underwater, in-window position. */
    const p04 = terms(0.4, -9);
    if (XP2.exitSignal(0.9, 0.4, cfg, { mark: 91, entry: 100, peak: 100, patience: p04 }).kind !== "stop_loss") return false;
    if (XP2.exitSignal(0.9, 0.4, cfg, { mark: 100.5, entry: 100, peak: 105, patience: terms(0.4, 0.5) }).kind !== "trailing_stop") return false;
    if (XP2.exitSignal(0.9, 0.4, cfg, { mark: 99, entry: 100, peak: 100, earningsInDays: 1, patience: terms(0.4, -1) }).kind !== "earnings_exit") return false;
    if (XP2.exitSignal(0.9, 0.4, { ...cfg, takeProfitPct: 5 }, { mark: 106, entry: 100, peak: 106, patience: terms(0.4, 6) }).kind !== "take_profit") return false;
    const critical = { criticalExit: true, monitored: true, fresh: true, complete: true };
    if (XP2.exitSignal(0.9, 0.4, cfg, { mark: 99, entry: 100, peak: 100, intelligencePolicy: critical, patience: terms(0.4, -1) }).kind !== "intelligence_critical") return false;
    /* The extended time stop replaces the ordinary one, and only upward. */
    if (XP2.exitSignal(0.1, 4, cfg, { mark: 99, entry: 100, peak: 100, patience: terms(4, -1) }).exit !== false) return false;
    if (XP2.exitSignal(0.1, 5, cfg, { mark: 99, entry: 100, peak: 100, patience: terms(5, -1) }).kind !== "time") return false;
    if (XP2.exitSignal(0.1, 3, cfg, { mark: 99, entry: 100, peak: 100 }).kind !== "time") return false;

    /* IMMUTABLE AND DECIDED AT ENTRY. The stamp travels on the order, and no
       exit path may create or alter one — exitTerms only READS it. */
    const ledgerSrc = sourceOf(L.proposeOrder);
    if (!/patience/.test(ledgerSrc)) return false;
    const guardSrc = sourceOf(require("./_investorPositionGuard").runGuard);
    if (/PA\.grant/.test(guardSrc)) return false;
    if (PA.exitTerms({}, { heldDays: 1, pnlPct: -1 }) !== null) return false;
    /* A control-cohort pick is never patient: it has no signal to recover. */
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    if (!/control \? null : PA\.grant\(\{/.test(cycleSrc)) return false;
    return true;
  }));

  cases.push(fixture("strategy_version_change_forces_a_rebootstrap", () => {
    /* control.strategyVersion and the safety epoch are only written by the
       FULL bootstrap block, which ensureBootstrapped skips while the stored
       bootstrapVersion still matches BOOTSTRAP_VERSION. A new frozen strategy
       shipped without bumping it therefore never gets adopted: the cycle
       calls loadStrategy(ctrl.strategyVersion) and keeps reading the previous
       version out of Firestore, so the new policy is inert and nothing
       reports an error because every hash still agrees with itself. */
    if (B.BOOTSTRAP_STRATEGY_VERSION !== strategy.version) return false;
    const src = sourceOf(B.ensureBootstrapped);
    /* The skip this invariant protects against must still be the shape we
       think it is. */
    if (!/const done = c\.bootstrapVersion === BOOTSTRAP_VERSION/.test(src)) return false;
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    if (!/loadStrategy\(ctrl\.strategyVersion\)/.test(cycleSrc)) return false;
    return true;
  }));

  cases.push(fixture("entry_creation_is_serialized_between_the_two_tiers", () => {
    /* R.checkAdd is pure over a book snapshot its caller built, and the
       approval transaction re-checks only cash — never maxOpenPositions or
       the gross/sector ceilings. Two writers that both see "5 of 6 used"
       would both admit one. Entry creation is therefore serialized on one
       account-wide lock; exits, marks and fills must stay outside it. */
    const LEASE = require("./_investorLease");
    if (typeof LEASE.acquireEntryLock !== "function"
      || typeof LEASE.releaseEntryLock !== "function") return false;
    const acq = sourceOf(LEASE.acquireEntryLock);
    /* Held by someone else and unexpired => refused; an unreadable lock must
       also refuse, never silently permit two writers. */
    if (!/expiresAtMs\) > nowMs && cur\.owner && cur\.owner !== owner/.test(acq)) return false;
    if (!/return \{ acquired: false, error:/.test(acq)) return false;
    if (!/\w+\.runTransaction/.test(acq)) return false;
    const rel = sourceOf(LEASE.releaseEntryLock);
    if (!/owner !== owner\)\s*return \{ released: false, reason: "not_owner" \}/.test(rel)) return false;

    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    /* The deep scan gates BOTH proposal loops on the lock and releases it
       before fills, in a finally so a throw cannot freeze entries. */
    if (!/acquireEntryLock\(accountId, entryLockOwner\)/.test(cycleSrc)) return false;
    if (!/entryLock\.acquired && queueIndex < proposalQueue\.length/.test(cycleSrc)) return false;
    if (!/entryLock\.acquired && exploratorySelection/.test(cycleSrc)) return false;
    if (!/\} finally \{[\s\S]{0,1500}releaseEntryLock\(accountId, entryLockOwner\)/.test(cycleSrc)) return false;

    const guardSrc = sourceOf(require("./_investorPositionGuard").runGuard);
    if (!/acquireEntryLock\(accountId, lockOwner\)/.test(guardSrc)) return false;
    if (!/if \(entryLock\.acquired\) \{[\s\S]{0,1500}evaluateStrikes/.test(guardSrc)) return false;
    /* Exits are evaluated and armed BEFORE the lock is taken, so a locked
       account can still protect its holdings. */
    if (guardSrc.indexOf("evaluate_exits") > guardSrc.indexOf("acquireEntryLock")) return false;
    return true;
  }));

  cases.push(fixture("plan_status_transitions_are_compare_and_set", () => {
    /* writePlans read-then-wrote with merge:false while the one-minute pass
       was writing the same documents, so a struck plan could be reverted to
       "armed" and lose its orderId and approval record. Every status move is
       now a transaction that only advances a plan still "armed". */
    const STK = require("./_investorStrike");
    const mark = sourceOf(STK.markPlan);
    /* Test the exported predicate by behaviour. esbuild legitimately rewrites
       `undefined` to `void 0` inside bundled functions; asserting its source
       spelling made the production bundle fail while the same code passed in
       raw Node, freezing every new entry. */
    if (typeof STK.planPatchChangesStatus !== "function") return false;
    if (STK.planPatchChangesStatus({ lastSeenUsd: 10 }) !== false) return false;
    if (STK.planPatchChangesStatus({ status: undefined }) !== false) return false;
    if (STK.planPatchChangesStatus({ status: "struck" }) !== true) return false;
    if (!/\w+\.runTransaction/.test(mark)) return false;
    if (!/if \(expect && current !== expect\)\s*return \{ applied: false, reason: current \}/.test(mark)) return false;
    const write = sourceOf(STK.writePlans);
    if (/\w+\.batch\(\)/.test(write)) return false;                       // the blind batch is gone
    if (!/\["struck", "cancelled"\]\.includes\(prev\.status\)/.test(write)) return false;
    if (!/\w+\.runTransaction/.test(write)) return false;
    /* A lost race must not lose the trade: the order stands, only the audit
       row moved, and the strike says so rather than failing silently. */
    const strike = sourceOf(STK.evaluateStrikes);
    if (!/if \(!claimed\.applied\)/.test(strike)) return false;
    if (!/plan_status_race/.test(strike)) return false;
    const api = sourceOf(require("./investorApi").ACTIONS.cancelPlan);
    /* Local identifiers are renamed while bundling (for example ST2 -> ST22),
       so assert the operation and transition rather than the identifier. */
    if (!/\.markPlan\(/.test(api) || !/status: "cancelled"/.test(api)) return false;
    return true;
  }));

  cases.push(fixture("a_dead_deep_scan_leaves_its_scheduled_slot_retryable", () => {
    /* lastPlanKey used to be stamped inside the dispatch transaction, so a
       worker that returned 202 and then died left the slot marked satisfied.
       With two slots a day that costs a whole scan, or the day. The kick now
       stamps only its cadence clock; the worker stamps the slot when it has
       actually finished, and the per-slot job document is what prevents a
       double dispatch. */
    const K = require("./investorKick");
    const dispatch = sourceOf(K.dispatch);
    if (/lastPlanKey: planSlot\.key/.test(dispatch)) return false;
    if (!/tx\.set\(cref, \{ \[stamp\]: now \}, \{ merge: true \}\)/.test(dispatch)) return false;
    /* A 202 whose background worker never starts remains `queued`; a hard
       timeout remains `running`. Both must become reclaimable when their own
       lease expires, while fresh work and completed work remain protected. */
    if (typeof K.jobBlocksDispatch !== "function") return false;
    const now = 1_000_000;
    if (!K.jobBlocksDispatch({ status: "queued", leaseExpiresAt: now + 1 }, now)) return false;
    if (K.jobBlocksDispatch({ status: "queued", leaseExpiresAt: now - 1 }, now)) return false;
    if (!K.jobBlocksDispatch({ status: "running", workerLeaseExpiresAt: now + 1 }, now)) return false;
    if (K.jobBlocksDispatch({ status: "running", workerLeaseExpiresAt: now - 1 }, now)) return false;
    if (!K.jobBlocksDispatch({ status: "complete" }, now)) return false;
    if (K.jobBlocksDispatch({ status: "dead" }, now)) return false;
    if (!/jobBlocksDispatch\(cur\.data\(\), now\)/.test(dispatch)) return false;
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    if (!/planSlotKey \? \{ lastPlanKey: planSlotKey \}/.test(cycleSrc)) return false;
    if (!/jobSnap\.data\(\)\.planSlot/.test(cycleSrc)) return false;
    /* And a slot never yet satisfied is still due, so the retry is real. */
    const at = (min) => ({ date: "2026-09-02", tradingDay: true, open: true, phase: "regular",
      minutesEt: min, regularCloseMinutesEt: 960 });
    const ctrl = { planMode: "scheduled", planTimesEt: ["09:50", "13:00"], lastPlanKey: null };
    return !!K.duePlanSlot(ctrl, at(10 * 60));
  }));

  cases.push(fixture("one_minute_bars_never_merge_into_the_five_minute_series", () => {
    /* The guard backfills from the day store when a fetch is short, which is
       routine in the first twenty minutes of a session. The store holds the
       strategy's 5-minute bars; the strike pass fetches 1-minute ones.
       Concatenating them produced one array of mixed spacing, and
       S.attentionZ compares a recent window against a per-slot historical
       baseline — inconsistent spacing biases the volume statistic that gates
       a strike. Identical provider/feed/adjustment is not enough. */
    const guardSrc = sourceOf(require("./_investorPositionGuard").runGuard);
    if (!/\} else if \(persistBars && fresh/.test(guardSrc)) return false;
    /* persistBars is exactly "the timeframes agree", and it also still gates
       the write path so minute bars cannot reach the day store. */
    if (!/persistBars = strikeTimeframe === \(cfg\.barTimeframe \|\| "5Min"\)/.test(guardSrc)) return false;
    if (!/if \(persistBars && \(panel\[symbol\] \|\| \[\]\)\.length\)/.test(guardSrc)) return false;
    /* Wholesale replacement stays allowed: it yields a homogeneous series. */
    if (!/if \(!fetchedBars\.length\) \{/.test(guardSrc)) return false;
    return true;
  }));

  cases.push(fixture("legacy_twice_daily_default_migrates_once_to_five_minute_full_scans", () => {
    const K = require("./investorKick");
    /* A stored scheduled value from the retired implicit default has no
       policy marker and must not survive this release. A later explicit
       operator choice carries the marker and remains scheduled. */
    if (K.effectivePlanMode({ planMode: "scheduled" }) !== "interval") return false;
    if (K.effectivePlanMode({ planMode: "scheduled",
      fullScanCadenceVersion: K.FULL_SCAN_CADENCE_VERSION }) !== "scheduled") return false;
    const control = sourceOf(K.control);
    if (!/planMode: DEFAULT_PLAN_MODE/.test(control)
        || !/cycleSeconds: 300/.test(control)
        || !/fullScanCadenceVersion: FULL_SCAN_CADENCE_VERSION/.test(control)) return false;
    const setControl = sourceOf(require("./investorApi").ACTIONS.setControl);
    return /planModeSource = "operator"/.test(setControl)
      && /fullScanCadenceVersion/.test(setControl);
  }));

  cases.push(fixture("account_breakers_block_purchases_without_hiding_the_company_scan", () => {
    const CYCLE = require("./investorCycle-background");
    const stopped = CYCLE.applyAccountBreakers({ pass: true }, { halted: true,
      breakers: [{ id: "drawdown_freeze", reason: "drawdown limit reached" }] });
    if (stopped.pass !== false || !/account risk stop/.test(stopped.reason)) return false;
    const open = { pass: true, reason: null };
    if (CYCLE.applyAccountBreakers(open, { halted: false }) !== open) return false;
    const source = sourceOf(CYCLE.runCycle);
    /* The old shortcut was the defect: after the holdings it silently
       continued over every entry-eligible company. Qualification is now a
       company verdict; entryControl is enforced later at purchase creation. */
    if (/if \(breakers\.halted\) \{[^}]*continue/.test(source)) return false;
    if (!/if \(evalRes\.pass && manifestValidation\.pass\)/.test(source)) return false;
    if (!/company_error/.test(source) || !/evaluation_error/.test(source)) return false;

    const API = require("./investorApi");
    const partial = API.fullScanIntegrity({ kind: "cycle", status: "complete", ranked: 276,
      live: { counters: { evaluated: 1, held: 1, blocked: 275 } } });
    const covered = API.fullScanIntegrity({ kind: "cycle", status: "complete", ranked: 276,
      live: { counters: { evaluated: 276, held: 1, no_signal: 273, passed: 1, company_error: 1 } } });
    return partial.pass === false && partial.evaluated === 1
      && covered.pass === true && covered.evaluated === 276;
  }));

  cases.push(fixture("attention_z_computes_from_multi_session_history_in_module_scope", () => {
    /* attentionZ moved into _investorSignal with the two-tier cadence and kept
       calling S.mean / S.stdev — the alias of the file it had LEFT. Nothing
       caught it: the branch only runs once a company has three prior
       sessions per intraday slot, and under `node -e` a top-level `const S`
       lands in the global lexical scope, so a hand check "passed". In the
       deployed bundle every company that breached the entry signal threw
       "S is not defined" from inside the evidence step, so no candidate ever
       reached the gates, and before the per-company catch existed the first
       breach aborted the whole scan. Evaluate under a fresh Function scope
       where no outer S exists, on a series deep enough to reach the branch. */
    const SIG = require("./_investorSignal");
    const bars = [];
    const days = ["2026-08-26", "2026-08-27", "2026-08-28", "2026-08-31", "2026-09-01", "2026-09-02"];
    days.forEach((d, dayIndex) => {
      for (let i = 0; i < 78; i += 1) {
        const t = new Date(`${d}T13:30:00Z`).getTime() + i * 300000;
        /* Prior sessions vary slot by slot so the baseline has a real scale;
           the latest session runs hot so the statistic is positive. */
        bars.push({ t: new Date(t).toISOString(), o: 100, h: 101, l: 99, c: 100.5,
          v: 1000 + ((i * 37) % 400) + ((dayIndex * 131 + i * 17) % 300)
            + (d === "2026-09-02" ? 1500 : 0) });
      }
    });
    /* The bundle keeps an unbound identifier verbatim, so the source check
       holds there too; the behavioural call is what fails under a leaked
       global. Together they cover both the deployed bundle and raw Node. */
    if (/\bS\.(mean|stdev)\(/.test(sourceOf(SIG.attentionZ))) return false;
    const z = SIG.attentionZ(bars, 12);
    return Number.isFinite(z) && z > 0;
  }));

  cases.push(fixture("run_document_risk_block_carries_no_nested_arrays", () => {
    /* Firestore refuses an array inside an array with "Property risk contains
       an invalid nested entity". risk.topClusters was Object.entries output —
       [name, pct] pairs — so the moment the book held a position the run
       document write failed and every scan was reported incomplete. */
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    if (!/topClusters: Object\.entries\(finalBook\.byClusterPct\)[\s\S]{0,120}\.map\(\(\[cluster, pct\]\) => \(\{ cluster, pct \}\)\)/.test(cycleSrc)) return false;
    const R = require("./_investorRisk");
    const marked = R.markedBook([{ symbol: "NVDA", open: true, qty: 10, entryPriceUsd: 100 },
      { symbol: "AMD", open: true, qty: 5, entryPriceUsd: 50 }], { NVDA: 110, AMD: 40 },
      () => "semis", { cash: 10000 });
    const top = Object.entries(marked.book.byClusterPct).sort((a, b) => b[1] - a[1]).slice(0, 5)
      .map(([cluster, pct]) => ({ cluster, pct }));
    if (!top.length) return false;
    const nested = (v) => Array.isArray(v) && v.some((x) => Array.isArray(x));
    return !nested(top) && top.every((x) => typeof x.cluster === "string" && Number.isFinite(x.pct));
  }));

  cases.push(fixture("no_cap_on_how_many_companies_are_held_at_once", () => {
    /* The operator's instruction: no limit on the NUMBER of companies held.
       maxOpenPositions null must read as unlimited everywhere a count was
       enforced — the portfolio admission check and the auto-approval ladder —
       while an explicit number and the strict block's absent key keep their
       meaning. Capital limits (position size, gross ceiling, cash) still
       bound the book; a count never does. */
    const R2 = require("./_investorRisk");
    if (R2.openPositionLimit({ maxOpenPositions: null }) !== null) return false;
    if (R2.openPositionLimit({ maxOpenPositions: 0 }) !== null) return false;
    if (R2.openPositionLimit({ maxOpenPositions: 6 }) !== 6) return false;
    if (R2.openPositionLimit({}) !== 12) return false;
    const xp = strategy.exploratoryAuto;
    const pc = xp.portfolioControls;
    if (R2.openPositionLimit(pc) !== null) return false;
    const rows = Array.from({ length: 40 }, (_, i) => ({ symbol: `S${i}`, sector: "s" + i, cluster: "c" + i }));
    const wide = R2.checkAdd({ symbol: "NEW", sector: "new", proposedUsd: 1000,
      book: { count: 40, grossUsd: 40000, grossPct: 40, rows, bySectorPct: {}, byClusterPct: {} },
      navUsd: 100000, cashUsd: 60000, cfg: { portfolioControls: pc }, dynamicCorrelations: {} });
    const countCheck = (wide.checks || []).find((c) => c.id === "max_positions");
    if (!countCheck || countCheck.pass !== true) return false;
    const ladderSrc = sourceOf(require("./_investorLadder").autoApproval);
    if (!/openPositionLimit\(pc\)/.test(ladderSrc) || !/maxOpenForAuto !== null &&/.test(ladderSrc)) return false;
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    return /maxOpenPositions: R\.openPositionLimit\(activePortfolioControls\)/.test(cycleSrc);
  }));

  cases.push(fixture("every_document_write_survives_a_nested_array", () => {
    /* Belt to the topClusters braces: the admin layer rewrites any array
       nested inside an array into an index-keyed object before Firestore sees
       it, so a new write site cannot bring back "invalid nested entity". */
    const A2 = require("./_investorAdmin");
    const out = A2.firestoreSafe({ risk: { topClusters: [["semis", 12.5], ["cloud", 3]],
      breakers: [{ id: "x", halt: true }], notes: ["a", "b"] }, n: 1, when: new Date(0) });
    const nested = (v) => Array.isArray(v) ? v.some((x) => Array.isArray(x) || nested(x))
      : (v && typeof v === "object" && !(v instanceof Date)) ? Object.values(v).some(nested) : false;
    if (nested(out)) return false;
    if (out.risk.topClusters[0]["0"] !== "semis" || out.risk.topClusters[1]["1"] !== 3) return false;
    if (out.risk.breakers[0].halt !== true || out.risk.notes[1] !== "b") return false;
    if (!(out.when instanceof Date)) return false;
    const adminSrc = sourceOf(A2.installNestedArrayGuard);
    if (!/DocumentReference\.prototype, "set"/.test(adminSrc) || !/Transaction\.prototype, "set"/.test(adminSrc)
        || !/WriteBatch\.prototype, "set"/.test(adminSrc)) return false;
    /* And the guard is installed before the client is ever handed out. */
    return /installNestedArrayGuard\(\);[\s\S]{0,300}_db = new Firestore/.test(sourceOf(A2.rawDb));
  }));

  cases.push(fixture("bootstrap_rebinds_a_missing_or_stale_safety_epoch_for_an_authorised_desk", () => {
    /* A BOOTSTRAP_VERSION bump while the desk was frozen adopts the new
       strategy identity without activating (no epoch is issued). When a later
       build lifts the freeze the desk runs exploratory auto with no epoch —
       or an epoch naming the previous strategy — so the ladder's
       safety_epoch gate fails and rolls it back to watching-only after every
       scan. The bootstrap must re-bind the epoch on the current identity for
       an already-authorised desk, and only for one. */
    const code = { version: strategy.version, contentHash: strategy.contentHash };
    const codeStrategy = { ...strategy };
    const sh = B.strategyHash(codeStrategy);
    const base = { autoExploratoryAuthorized: true, operatingState: "exploratory_auto",
      enabled: true, killSwitch: false, entriesFrozen: false, dryRun: false, mode: "approval",
      accountId: "paper-1", strategyVersion: strategy.version, strategyHash: sh,
      universeVersion: "v6", universeHash: "u".repeat(64), variantsHash: V.variantsHash() };
    const args = { commit: "build-2", codeStrategy, variantsHash: V.variantsHash() };
    if (B.epochRebindNeeded({ ...base, safetyEpoch: null }, args).rebind !== true) return false;
    const stale = { accountId: "paper-1", strategyVersion: "v13", strategyHash: "x", universeVersion: "v6",
      universeHash: base.universeHash, variantsHash: base.variantsHash, commit: "build-1" };
    if (B.epochRebindNeeded({ ...base, safetyEpoch: stale }, args).rebind !== true) return false;
    const current = { ...stale, strategyVersion: strategy.version, strategyHash: sh, commit: "build-2" };
    if (B.epochRebindNeeded({ ...base, safetyEpoch: current }, args).rebind !== false) return false;
    /* Never for a desk the operator did not authorise, never in observation,
       never when the stored identity is not this build's. */
    if (B.epochRebindNeeded({ ...base, safetyEpoch: null, autoExploratoryAuthorized: false }, args).rebind) return false;
    if (B.epochRebindNeeded({ ...base, safetyEpoch: null, operatingState: "observation", dryRun: true, mode: "research" }, args).rebind) return false;
    if (B.epochRebindNeeded({ ...base, safetyEpoch: null, strategyVersion: "v13" }, args).rebind) return false;
    const src = sourceOf(B.ensureBootstrapped);
    if (!/epochRebindNeeded\(c, \{\s*commit,/.test(src)) return false;
    if (!/activatedBy: "bootstrap:identity_rebind"/.test(src)) return false;
    /* And a freeze imposed by a failed attestation no longer blocks auto-start. */
    if (!/const attestationFreeze = priorOperating\.entriesFrozen/.test(src)) return false;
    /* esbuild drops the redundant parentheses; either spelling is the same logic. */
    if (!/priorFreeze = \(?priorOperating\.entriesFrozen\s*&&\s*!attestationFreeze\)?\s*\|\|/.test(src)) return false;
    return !!code;
  }));

  cases.push(fixture("dashboard_response_stays_under_the_platform_payload_limit", () => {
    /* A response over 6 MB is never delivered: the runtime answers
       RequestEntityTooLarge and the browser sees an opaque 502, so the
       operator cannot even sign in. Restoring genuine full-universe scans
       took the dashboard from a handful of candidate cards to one per
       evaluated company and crossed that limit. Two defences are asserted
       here: the fields no view reads are not shipped at all, and a payload
       that is still too large degrades in a defined order instead of
       failing. */
    const API = require("./investorApi");
    if (typeof API.boundDashboardPayload !== "function") return false;

    /* The projection drops exactly the unread fields and keeps the rest. */
    const card = { symbol: "NVDA", rank: 0.02, pass: true, gates: [{ id: "g", pass: true }],
      cost: { ratio: 1.2 }, breach: true, unionEvidenceTrigger: true,
      frozenDecision: { big: "x".repeat(50) }, historyNotes: ["a", "b"],
      manifestCoverage: { a: 1 }, decisionManifestCoverage: { a: 1 },
      decisionManifestHash: "h", decisionManifestValid: true, sigmaBlend: 1,
      zShortOnly: -2, sectorTailFraction: 0.1, marketProvider: "p",
      marketFeed: "f", buildCommit: "c" };
    const row = API.dashboardCandidateRow(card);
    for (const k of API.DASHBOARD_CANDIDATE_OMIT) if (k in row) return false;
    if (row.symbol !== "NVDA" || !row.gates || row.cost.ratio !== 1.2 || row.breach !== true) return false;
    /* The minimal row still supports the ranked list and the block tally. */
    const min = API.minimalCandidateRow({ ...card, firstBlock: "cost", blockedBy: ["cost"] });
    if (min.symbol !== "NVDA" || min.firstBlock !== "cost" || min.pass !== true
        || min.detailTrimmed !== true || min.gates !== undefined) return false;

    /* A realistic oversized payload: one fat card per company in the roster. */
    const fat = () => ({ ...card, gates: Array.from({ length: 16 }, (_, i) => ({
      id: `gate_${i}`, pass: i % 2 === 0, detail: "y".repeat(420) })),
      history: { note: "z".repeat(4200) }, coverage: { roster: "r".repeat(2100) },
      intelligence: { topEvents: Array.from({ length: 3 }, () => "e".repeat(1500)) } });
    const many = Array.from({ length: 300 }, (_, i) => ({ ...fat(), symbol: `S${i}`, rank: i / 300 }));
    const big = { ok: true, candidates: many, closedTrades: Array.from({ length: 200 }, () => ({ x: "t".repeat(400) })),
      intelligence: Array.from({ length: 50 }, () => ({ d: "i".repeat(900) })), quoteCoverage: [] };
    if (API.payloadBytes(big) <= 4_500_000) return false;     // the fixture must actually exercise it
    const bounded = API.boundDashboardPayload(big);
    if (API.payloadBytes(bounded) > 4_500_000) return false;
    if (!bounded.payloadTrimmed || !bounded.payloadTrimmed.dropped.length) return false;
    /* Every company is still present and still countable. */
    if (bounded.candidates.length !== many.length) return false;
    if (bounded.candidates.some((c) => !c.symbol)) return false;
    /* A payload that already fits is returned untouched. */
    const small = { ok: true, candidates: [card] };
    const same = API.boundDashboardPayload(small);
    if (same !== small || same.payloadTrimmed) return false;

    /* And the handler refuses to hand the platform a body it would reject. */
    const handlerSrc = sourceOf(API.handler);
    if (!/payloadBytes\(full\)/.test(handlerSrc)) return false;
    if (!/bytes > RESPONSE_HARD_LIMIT_BYTES/.test(handlerSrc)) return false;
    /* 413 with a `refused` string: the console renders that at the sign-in
       gate, where an opaque 502 told the operator nothing. */
    if (!/AUTH\.json\(event, 413,/.test(handlerSrc)) return false;
    if (!/refused:/.test(handlerSrc)) return false;
    /* The dashboard action must actually apply the bound. */
    return /return boundDashboardPayload\(payload\)/.test(sourceOf(API.ACTIONS.dashboard));
  }));

  cases.push(fixture("one_entry_path_buys_at_market_or_not_at_all", () => {
    /* The armed tier parked qualified companies in "armed", then "replaced by
       a later scan", then "retired", and bought almost nothing. v15 retires it:
       a company short of the entry threshold on |z| ALONE, still inside the
       rank band and above the z floor, is bought at the price on the screen.
       Nothing else is forgiven — every hazard gate, and the cost gate measured
       at the price actually paid, still refuse. */
    const STK = require("./_investorStrike");
    const XPL = require("./_investorExplore");
    const policy = XPL.activityPolicy(strategy).immediateEntry;
    if (policy.enabled !== true) return false;
    if (!(policy.minAbsZFloor < Number(strategy.exploratoryAuto.paperLearningDefaults.minAbsZ))) return false;

    const cfg = { minAbsZ: Number(strategy.exploratoryAuto.paperLearningDefaults.minAbsZ) };
    const base = { cfg, policy, rank: 0.28,
      quality: { tradable: true, grade: "A" },
      decisionManifest: { manifestHash: "a".repeat(64) },
      marketProvenance: { sourceSha256: "b".repeat(64) } };
    const ev = (blockedBy) => ({ pass: blockedBy.length === 0, blockedBy });

    /* Admitted: every gate passes, only |z| is short, and it clears the floor.
       Derived from the frozen values so this cannot drift out of date again. */
    const midBand = Number((-(cfg.minAbsZ + policy.minAbsZFloor) / 2).toFixed(2));
    const ok = STK.nearMissEntryEligible({ ...base, z: { z: midBand }, evalRes: ev(["signal"]) });
    if (!ok.ok) return false;
    if (Math.abs(ok.zShortfall - (midBand + cfg.minAbsZ)) > 1e-9) return false;

    /* Refused, one reason each. */
    const no = (r, args) => { const v = STK.nearMissEntryEligible({ ...base, ...args }); return !v.ok && v.reason === r; };
    /* The cost gate is never forgiven: no edge left at the price being paid. */
    if (!no("blocked_by_cost", { z: { z: midBand }, evalRes: ev(["signal", "cost"]) })) return false;
    /* A hazard finding is not a near miss. */
    if (!no("blocked_by_trend", { z: { z: midBand }, evalRes: ev(["signal", "trend"]) })) return false;
    /* Too weak a move: the floor is a real bound, not decoration. */
    const tooWeak = Number((-(policy.minAbsZFloor / 2)).toFixed(2));
    if (!no(`z_${tooWeak.toFixed(2)}_short_of_floor_${policy.minAbsZFloor}`,
      { z: { z: tooWeak }, evalRes: ev(["signal"]) })) return false;
    /* Outside the rank band. */
    if (!no("rank_above_entry_ceiling", { z: { z: midBand }, rank: 0.8, evalRes: ev(["signal"]) })) return false;
    /* Already past the threshold: that is the ordinary path's trade, and the
       signal must have failed on rank instead. */
    if (!no("signal_failed_on_rank_not_z", { z: { z: -(cfg.minAbsZ + 0.5) }, evalRes: ev(["signal"]) })) return false;
    /* A full pass belongs to the ordinary path, which already queued it. */
    if (!no("already_passes", { z: { z: -(cfg.minAbsZ + 0.5) }, evalRes: ev([]) })) return false;
    /* Untradable source, and missing identity, still refuse. */
    if (!no("execution_source_not_tradable", { z: { z: midBand }, evalRes: ev(["signal"]),
      quality: { tradable: false, researchEligible: true, grade: "C" } })) return false;
    if (!no("no_market_provenance", { z: { z: midBand }, evalRes: ev(["signal"]), marketProvenance: null })) return false;
    /* Off by policy is off. */
    if (!no("immediate_entry_disabled", { z: { z: midBand }, evalRes: ev(["signal"]),
      policy: { ...policy, enabled: false } })) return false;

    /* The cycle must route an admitted near miss through the ordinary proposal
       path — same gates, same caps, same approval — and size it with the paper
       floor, or it lands as a few hundred dollars against a five-figure cap. */
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    if (!/nearMissEntryEligible\(\{/.test(cycleSrc)) return false;
    /* esbuild drops the redundant parentheses around the && operand. */
    if (!/if \(\(evalRes\.pass \|\| \(?nearMiss && nearMiss\.ok\)?\) && manifestValidation\.pass\)/.test(cycleSrc)) return false;
    if (!/q\.nearMiss \? Math\.max\(baseCombined, paperFloor\) : baseCombined/.test(cycleSrc)) return false;
    /* And nothing arms a level any more. */
    return XPL.activityPolicy(strategy).strike.enabled === false;
  }));

  cases.push(fixture("one_unpriced_holding_cannot_halt_every_purchase", () => {
    /* A single open position without a validated current mark used to halt the
       account outright: every purchase in every other company stopped until
       that one name's feed returned. The rule exists because you cannot see a
       drawdown you cannot price, so what matters is the SHARE of the book that
       is unpriceable, not the row count. An unmarked position is carried at
       cost, so it understates a loss on itself and nothing else. */
    const R2 = require("./_investorRisk");
    const pc = strategy.exploratoryAuto.portfolioControls;
    const limit = Number(pc.unmarkedExposureHaltPctOfNav);
    if (!(limit > 0)) return false;
    const cfg = { portfolioControls: pc };

    const positions = [{ symbol: "CEG", open: true, qty: 10, entryPriceUsd: 284 },
      { symbol: "BKR", open: true, qty: 100, entryPriceUsd: 64 }];
    const marked = R2.markedBook(positions, { BKR: 64 }, () => "x", { cash: 100000 });
    /* The unpriced position is counted, named and valued at cost. */
    if (marked.untrustedMarks !== 1) return false;
    if (JSON.stringify(marked.unmarkedSymbols) !== JSON.stringify(["CEG"])) return false;
    if (!(marked.unmarkedExposurePctOfNav > 0 && marked.unmarkedExposurePctOfNav < limit)) return false;

    const state = { navUsd: marked.navUsd, hwmUsd: marked.navUsd,
      untrustedOpenMarks: marked.untrustedMarks,
      unmarkedExposurePctOfNav: marked.unmarkedExposurePctOfNav,
      unmarkedSymbols: marked.unmarkedSymbols, drawdownPct: 0, dayPnlPct: 0 };
    const small = R2.accountBreakers(state, cfg);
    if (small.halted !== false) return false;
    const row = (small.breakers || []).find((b) => b.id === "untrusted_open_marks");
    /* Still reported, and it names the company — "1 position lacks a mark" is
       not something an operator can act on. */
    if (!row || row.halt !== false || !/CEG/.test(row.reason)) return false;

    /* Above the limit it still halts: a book that is mostly unpriceable
       blinds the drawdown breaker, which is what the rule protects. */
    const big = R2.accountBreakers({ ...state, unmarkedExposurePctOfNav: limit + 15 }, cfg);
    if (big.halted !== true) return false;
    /* Drawdown and day-loss breakers are untouched. The exploratory cohort
       sets both limits wide, so assert against an explicit limit. */
    const tight = { portfolioControls: { ...pc, drawdownFreezePctFromHigh: 6, oneDayLossPausePctOfNav: 1 } };
    if (R2.accountBreakers({ ...state, drawdownPct: -7 }, tight).halted !== true) return false;
    if (R2.accountBreakers({ ...state, dayPnlPct: -2 }, tight).halted !== true) return false;

    /* And a holding that the roster fetch missed is re-requested on its own
       before anything reads a mark, so this state is rare to begin with. */
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    if (!/const heldSymbols = allPositions/.test(cycleSrc)) return false;
    return /chunkSize: 10, retries: 3/.test(cycleSrc);
  }));

  cases.push(fixture("a_scan_that_priced_a_third_of_the_roster_is_not_a_full_scan", () => {
    /* fullScanIntegrity measured coverage against the number of companies the
       scan MANAGED to rank, so a fetch that returned 97 of 277 still reported
       a clean full scan and nothing on the desk said otherwise. The
       denominator is the roster the scan set out to cover. */
    const API = require("./investorApi");
    const run = { kind: "cycle", status: "complete", ranked: 97,
      live: { counters: { evaluated: 97 } },
      marketCoverage: { roster: 277, priced: 97, evaluated: 97, failed: 3,
        failedSample: ["AAPL"], missingSample: [], error: "provider chunk failed" } };
    const thin = API.fullScanIntegrity(run);
    if (thin.roster !== 277 || thin.priced !== 97) return false;
    if (thin.coveragePct !== 35 || thin.rosterCovered !== false) return false;
    if (thin.coverageReason !== "provider chunk failed") return false;

    const full = API.fullScanIntegrity({ kind: "cycle", status: "complete", ranked: 277,
      live: { counters: { evaluated: 277 } },
      marketCoverage: { roster: 277, priced: 277, evaluated: 277 } });
    if (full.rosterCovered !== true || full.pass !== true) return false;

    /* An older run with no coverage record must not be called short. */
    const legacy = API.fullScanIntegrity({ kind: "cycle", status: "complete", ranked: 200,
      live: { counters: { evaluated: 200 } } });
    return legacy.rosterCovered === null && legacy.pass === true;
  }));

  cases.push(fixture("a_missing_price_is_not_a_ledger_discrepancy", () => {
    /* The same unpriced holding that tripped the account breaker also failed
       RECONCILIATION, by a second route: the mark loop skipped an unpriced
       position entirely, so it contributed nothing to the computed NAV while
       the displayed NAV carried it at cost — a guaranteed mismatch — and the
       violation failed the check outright. The operator was told "the paper
       ledger did not reconcile" and given nothing to reconcile.
       Accounting integrity is untouched; only the mark handling changes. */
    const src = sourceOf(require("./_investorLedger").reconcileAccount);
    /* Unpriced positions are valued at cost on BOTH sides, so the two agree. */
    if (!/unpricedCostCents/.test(src)) return false;
    if (!/marketValueCents \+= costCentsFor/.test(src)) return false;
    /* Mark completeness is reported, not fatal. */
    if (/pass: markViolations\.length === 0/.test(src)) return false;
    if (!/marksComplete: markViolations\.length === 0/.test(src)) return false;
    /* The NAV agreement itself is still enforced, with its tolerance. */
    if (!/Math\.abs\(displayedNavCents - computedNavCents\) <= navToleranceCents/.test(src)) return false;
    /* And every accounting invariant still gates the result. */
    if (!/pass: journal\.pass && lifecycle\.pass && equationPass && nav\.pass/.test(src)) return false;
    /* The failure is named, so the console can say what to fix. */
    if (!/const firstFailure = \(\(\) =>/.test(src)) return false;
    if (!/reason: result\.pass \? null : firstFailure/.test(src)) return false;
    /* markedBook uses the same cost fallback, which is what makes the two
       sides agree in the first place. */
    const R2 = require("./_investorRisk");
    const marked = R2.markedBook([{ symbol: "CEG", open: true, qty: 10, entryPriceUsd: 100 }],
      {}, () => "x", { cash: 5000 });
    return marked.navUsd === 6000 && marked.untrustedMarks === 1;
  }));

  cases.push(fixture("five_completed_sessions_choose_the_company_not_one_hour", () => {
    /* The desk used to select on 12 five-minute bars — one hour — which cannot
       tell a multi-day decline from a one-off dislocation, and cannot say why
       a company fell. Selection is now five COMPLETED sessions measured against
       the company's own sector, and the hour is only a timer. */
    const MS = require("./_investorMultiSession");
    const mk = (lastFive) => {
      const out = []; let c = 100;
      for (let i = 0; i < 120; i += 1) {
        const d = new Date(Date.UTC(2026, 0, 1) + i * 86400000).toISOString().slice(0, 10);
        c *= 1 + 0.0006 + (((i * 37) % 7) - 3) / 1000;
        out.push({ date: d, c: Number(c.toFixed(4)) });
      }
      for (let k = 0; k < 5; k += 1) {
        out[out.length - 5 + k].c = Number((out[out.length - 6].c * (1 + lastFive * (k + 1) / 5)).toFixed(4));
      }
      return out;
    };
    const asOf = new Date(Date.UTC(2026, 0, 1) + 120 * 86400000).toISOString().slice(0, 10);
    const sectors = { A: "tech", B: "tech", C: "tech", D: "tech", E: "fin", F: "fin", G: "fin" };
    const prov = {}; for (const k of Object.keys(sectors)) prov[k] = { homogeneous: true, provider: "p", sourceSha256: "a".repeat(64) };
    const build = (daily) => MS.buildSessionPanel(daily, prov, { asOfDate: asOf, sectorOf: (x) => sectors[x] });
    const policy = { minSessionMoveZ: 1.25, sessionRankCap: 0.2, sessionTrendMode: "either" };
    const uptrend = { ok: true, aboveSma200: true, sma50Rising: true, downtrendFlags: [] };

    /* THE WHOLE POINT: a company that fell WITH its sector has not dislocated. */
    const together = {}; for (const k of Object.keys(sectors)) together[k] = mk(sectors[k] === "tech" ? -0.08 : 0);
    const p1 = build(together);
    if (MS.sessionAdmits(p1.bySymbol.A, uptrend, policy).pass !== false) return false;
    if (Math.abs(p1.bySymbol.A.companyPartPct) > 0.01) return false;

    /* The same fall, but its sector did not fall: that is the company's own. */
    const alone = { ...together }; alone.A = mk(-0.16);
    const p2 = build(alone);
    const a = p2.bySymbol.A;
    if (!MS.sessionAdmits(a, uptrend, policy).pass) return false;
    /* The three parts must sum to the total fall — this is the operator's
       "why", and it is arithmetic, not a score. */
    if (Math.abs((a.marketPartPct + a.sectorPartPct + a.companyPartPct) - a.r5Pct) > 0.02) return false;

    /* A FALL MUST BE A FALL. A company UP while its sector is further up has a
       large negative excess and would otherwise be announced as having fallen. */
    const up = { ...together }; up.A = mk(0.04);
    for (const k of ["B", "C", "D"]) up[k] = mk(0.10);
    const risen = build(up).bySymbol.A;
    if (!(risen.r5Pct > 0)) return false;
    const upVerdict = MS.sessionAdmits(risen, uptrend, policy);
    if (upVerdict.pass !== false || !/^up /.test(upVerdict.reason)) return false;

    /* A five-session fall with NEITHER trend confirmation is a falling knife.
       (v18 requires either confirmation rather than both; "both" is still
       selectable and is asserted separately by the profit-taking fixture.) */
    if (MS.sessionAdmits(a, { ok: true, aboveSma200: false, sma50Rising: false, downtrendFlags: [] }, policy).pass) return false;
    if (MS.sessionAdmits(a, { ok: true, aboveSma200: true, sma50Rising: true, downtrendFlags: ["a", "b"] }, policy).pass) return false;
    if (MS.sessionAdmits(a, { ok: false }, policy).pass) return false;

    /* Coverage failures are NAMED, never a silent skip: a stale spine and an
       unprovenanced one are different problems from "nothing qualified". */
    const stale = { ...alone }; stale.B = alone.B.slice(0, -1);
    /* An UNIDENTIFIED series is refused; a series whose FEED changed is not.
       Volume-provenance homogeneity latches false the first time the provider
       swaps feed — which happens routinely on this account — and this
       statistic reads closes only, so requiring it rejected the roster for a
       property the measurement never touches. */
    const badProv = { ...prov, C: { homogeneous: true, provider: null, sourceSha256: "a".repeat(64) } };
    if (!MS.trustedDailyProvenance({ provider: "alpaca", sourceSha256: "a".repeat(64), homogeneous: false })) return false;
    if (MS.trustedDailyProvenance({ provider: "alpaca", homogeneous: true })) return false;
    const p3 = MS.buildSessionPanel(stale, badProv, { asOfDate: asOf, sectorOf: (x) => sectors[x] });
    if (p3.rejected.stale !== 1 || p3.rejected.provenance !== 1) return false;

    /* NO LIVE PRICE. Today's bar must not change any five-session number, or
       the shortlist would drift through the session and a split effective
       today could manufacture a phantom fall. */
    const withToday = { ...alone };
    withToday.A = [...alone.A, { date: asOf, c: alone.A[alone.A.length - 1].c * 0.5 }];
    const p4 = build(withToday);
    if (p4.bySymbol.A.r5Pct !== a.r5Pct) return false;

    /* The frozen policy must actually reach the running config, and the hour
       must have been LOOSENED — adding the five-session test on top of the old
       hourly bar would make selection strictly tighter, the opposite of intent. */
    const learn = strategy.exploratoryAuto.paperLearningDefaults;
    if (learn.requireSessionMove !== true) return false;
    if (learn.minAbsZ !== 1.25 || learn.entryRank !== 0.5) return false;
    /* The near-miss band must not be empty, or the only entry path is dead. */
    if (!(strategy.exploratoryAuto.activity.immediateEntry.minAbsZFloor < learn.minAbsZ)) return false;
    const applied = strategy.paperLearningConfig(strategy.parameters,
      { paperLearning: { enabled: true, ...learn } }).cfg;
    if (applied.requireSessionMove !== true) return false;
    if (applied.minSessionMoveZ !== Number(learn.minSessionMoveZ)
      || applied.sessionRankCap !== Number(learn.sessionRankCap)) return false;
    if (!(Number(learn.minSessionMoveZ) > 0) || !(Number(learn.sessionRankCap) > 0)) return false;

    /* And the gate is wired: it blocks by its own id, and the cycle feeds it. */
    const SIG = require("./_investorSignal");
    const gateSrc = sourceOf(SIG.evaluateCandidate);
    /* esbuild re-wraps long argument lists across lines. */
    if (!/add\(\s*"session_move",\s*"Five-session move"/.test(gateSrc)) return false;
    if (!/cfg\.requireSessionMove === true/.test(gateSrc)) return false;
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    if (!/MS\.buildSessionPanel\(\s*dailySeriesBySymbol/.test(cycleSrc)) return false;
    return /MS\.sessionAdmits\(sessionMove/.test(cycleSrc);
  }));

  cases.push(fixture("a_failed_request_costs_a_handful_of_companies_not_sixty", () => {
    /* The roster is fetched in chunks of sixty, and a chunk that failed was
       sixty companies deleted from the scan in one stroke. With 277 symbols in
       five chunks, three failures is exactly the "97 of 277" and "104 of 277"
       the desk kept reporting — and the reason was never recorded anywhere the
       operator could see it. A failed chunk is now split and retried, and the
       provider's own error is carried out. */
    const M2 = require("./_investorMarket");

    /* Recovery: a symbol delivered by a smaller request is NOT a failure. */
    const list = ["A", "B", "C", "D"];
    const chunks = [["A", "B", "C", "D"], ["A", "B"], ["C", "D"]];
    const results = [
      { error: "socket hang up" },
      { bars: { A: [{ t: 1 }], B: [{ t: 1 }] }, provider: "alpaca", manifestSha256: "h1", symbolSha256: {} },
      { error: "429 rate limited" },
    ];
    const out = M2.mergeChunkResults(list, chunks, results, "alpaca");
    if (JSON.stringify(out.failedSymbols.sort()) !== JSON.stringify(["C", "D"])) return false;
    if (out.failureCount !== 2) return false;
    if (!out.bars.A || !out.bars.B) return false;
    /* The provider's reasons survive to the caller. */
    const reasons = (out.chunks.failed || []).map((f) => f.error);
    if (!reasons.includes("socket hang up") || !reasons.includes("429 rate limited")) return false;

    /* Nothing lost when every chunk succeeds. */
    const clean = M2.mergeChunkResults(["A"], [["A"]],
      [{ bars: { A: [{ t: 1 }] }, provider: "alpaca", manifestSha256: "h", symbolSha256: {} }], "alpaca");
    if (clean.failureCount !== 0 || clean.missingSymbols.length !== 0) return false;

    /* The split-and-retry pass exists, appends its pieces so the merge counts
       them, and records what it recovered. */
    const src = sourceOf(M2.fetchBarsChunked);
    if (!/rescueSize = 12/.test(src)) return false;
    if (!/chunks\.push\(piece\)/.test(src) || !/results\.push\(got\)/.test(src)) return false;
    if (!/recoveredSymbols/.test(src)) return false;
    /* And the cycle carries the reason into the run record, where the console
       reads it — a coverage failure must never again be silent. */
    const cycleSrc = sourceOf(require("./investorCycle-background").runCycle);
    if (!/chunkErrors/.test(cycleSrc)) return false;
    return /provider request failed/.test(cycleSrc);
  }));

  cases.push(fixture("the_desk_takes_profits_and_never_closes_a_loss_on_a_statistic", () => {
    /* THE RECORD THAT FORCED THIS: 38 round trips, 20 up and 18 down, holds of
       one to six hours, net roughly zero before friction — and nearly every
       exit was the RANK exit firing a fraction of a percent either side of
       flat. The desk had no profit target at all (takeProfitPct was null), so
       it never once sold because a position had made money, and the rank exit
       closed losers because the gap had narrowed relative to the cross-section
       — which happens just as readily when other companies fall further. */
    const XP2 = require("./_investorExitPolicy");
    const cfg = strategy.paperLearningConfig(strategy.parameters,
      { paperLearning: { enabled: true, ...strategy.exploratoryAuto.paperLearningDefaults } }).cfg;

    /* A profit target now exists and is a real number. */
    if (!(Number(cfg.takeProfitPct) > 0)) return false;
    /* The trailing stop cannot turn a gain into a loss: it must arm at a gain
       larger than what it then gives back. Armed at +3% giving back 4% could
       sell a +3% position at -1%, which is how a correct call became a loss. */
    if (!(Number(cfg.trailingArmsAtPct) > Math.abs(Number(cfg.trailingStopPct)))) return false;

    const exitAt = (entry, mark, peak, rank, held) => XP2.exitSignal(rank, held, cfg,
      { mark, entry, peak: peak == null ? Math.max(entry, mark) : peak });

    /* Every one of the operator's small losses was sold by the rank exit.
       None of them may be now — the rank alone is not a reason to realise a
       loss. Real numbers from the record. */
    for (const [entry, mark] of [[284.32, 283.38], [48.86, 47.50], [100.93, 100.83], [41.90, 41.82]]) {
      const v = exitAt(entry, mark, entry, 0.90, 0.2);
      if (v.exit !== false || v.heldThroughRank !== true) return false;
    }
    /* A WINNER still takes the rank exit: banking a gain when the reason for
       the trade has gone is exactly right. */
    if (exitAt(83.94, 84.48, 84.48, 0.90, 0.2).kind !== "signal") return false;
    /* The profit target fires on its own, without needing the rank. */
    const tp = exitAt(100, 100 + Number(cfg.takeProfitPct) + 0.1, null, 0.10, 0.1);
    if (tp.exit !== true || tp.kind !== "take_profit") return false;
    /* A position that peaked and slipped is sold while still UP. */
    const trail = exitAt(100, 101.1, 102.5, 0.10, 0.1);
    if (trail.exit !== true || trail.kind !== "trailing_stop" || !(trail.pnlPct > 0)) return false;

    /* RULES ABOUT MONEY STILL CLOSE A LOSS — holding losers off the rank exit
       must never become holding them forever. */
    if (exitAt(100, 91.5, 100, 0.10, 0.1).kind !== "stop_loss") return false;
    if (exitAt(100, 98, 100, 0.10, Number(cfg.maxHoldDays) + 1).kind !== "time") return false;
    const earn = XP2.exitSignal(0.10, 0.1, cfg, { mark: 98, entry: 100, peak: 100, earningsInDays: 1 });
    if (earn.kind !== "earnings_exit") return false;
    /* And the operator can restore the old behaviour explicitly. */
    const strictCfg = { ...cfg, holdLosersThroughRankExit: false };
    if (XP2.exitSignal(0.90, 0.2, strictCfg, { mark: 99, entry: 100, peak: 100 }).kind !== "signal") return false;

    /* SELECTION LOOSENS TOO: requiring a company be above its 200-day average
       AND have a rising 50-day line, on top of a five-session fall, admitted
       ONE company out of 270 measured. Either confirmation now suffices. */
    const MS = require("./_investorMultiSession");
    if (cfg.sessionTrendMode !== "either") return false;
    const move = { ok: true, r5Pct: -6, sessionZ: -1.4, sessionRank: 0.1, normalSwingPct: 3 };
    const pol = { minSessionMoveZ: cfg.minSessionMoveZ, sessionRankCap: cfg.sessionRankCap,
      sessionTrendMode: "either" };
    if (!MS.sessionAdmits(move, { ok: true, aboveSma200: true, sma50Rising: false, downtrendFlags: [] }, pol).pass) return false;
    if (!MS.sessionAdmits(move, { ok: true, aboveSma200: false, sma50Rising: true, downtrendFlags: [] }, pol).pass) return false;
    /* Neither confirmation is still a falling knife, and "both" still works. */
    if (MS.sessionAdmits(move, { ok: true, aboveSma200: false, sma50Rising: false, downtrendFlags: [] }, pol).pass) return false;
    if (MS.sessionAdmits(move, { ok: true, aboveSma200: true, sma50Rising: false, downtrendFlags: [] },
      { ...pol, sessionTrendMode: "both" }).pass) return false;
    return true;
  }));



  /* ═══════════════════════════════════════════════════════════════════════
     BLUEPRINT v2 §17.2 — GROUP 1 BLOCKING SUITES (D-1 … D-9). Each halts
     trading on the deployed build through controlAllowsEntry when it fails.
     ═══════════════════════════════════════════════════════════════════════ */

  /* D-1, D-8b; invariant I-1. The absence of a verdict is never a verdict. */
  cases.push(fixture("fail_closed_evidence_every_gateway_failure_is_unavailable_and_never_trades", () => {
    const O = require("./_investorOpenai");
    const relaxed = { ...strategy.parameters, paperAbstainOnMissingInfo: true };
    /* The gateway: no failure path may still emit the old token, the schema
       enum must not offer it to the model, and every failure return must go
       through the unavailable() shape. */
    const src = sourceOf(O.classifyMove);
    if (/evidence_pending/.test(src)) return false;
    if (O.CLASSIFY_SCHEMA.properties.cause.enum.includes("evidence_pending")) return false;
    if (!O.CLASSIFY_SCHEMA.properties.cause.enum.includes("evidence_insufficient")) return false;
    const unavailableSites = (src.match(/unavailable\(/g) || []).length;
    if (unavailableSites < 8) throw new Error(`FAIL ${JSON.stringify({ unavailableSites })}`);
    if ((src.match(/CAUSES\.UNAVAILABLE/g) || []).length < 3) return false;
    if (O.CAUSES.UNAVAILABLE !== S.CAUSE.UNAVAILABLE || O.CAUSES.INSUFFICIENT !== S.CAUSE.INSUFFICIENT) return false;
    /* The direction rule, in the most permissive cohort there is. */
    const un = S.directionFromCause(S.CAUSE.UNAVAILABLE, relaxed, 3, { complete: true });
    if (un.trade !== false || un.failClosed !== true) return false;
    const unknown = S.directionFromCause("cause_added_later", relaxed, 3, { complete: true });
    if (unknown.trade !== false || unknown.failClosed !== true) return false;
    const legacy = S.directionFromCause("evidence_pending", relaxed, 3, { complete: true });
    if (legacy.trade !== false) return false;
    /* Absence and insufficiency remain paper observations at reduced size,
       and only where the cohort declares it. */
    const gathered = S.directionFromCause(S.CAUSE.NOT_YET_GATHERED, relaxed, 0, null);
    const insufficient = S.directionFromCause(S.CAUSE.INSUFFICIENT, relaxed, 0, null);
    if (gathered.trade !== true || gathered.relaxed !== true || gathered.confidence >= 0.5) return false;
    if (insufficient.trade !== true || insufficient.relaxed !== true || insufficient.confidence >= 0.5) return false;
    if (S.directionFromCause(S.CAUSE.NOT_YET_GATHERED, strategy.parameters, 0, null).trade !== false) return false;
    if (S.directionFromCause(S.CAUSE.INSUFFICIENT, strategy.parameters, 0, null).trade !== false) return false;
    /* The default branch fails closed in source, not only in this run. */
    const dsrc = sourceOf(S.directionFromCause);
    const defaultBranch = dsrc.slice(dsrc.lastIndexOf("default:"));
    if (/trade:\s*true/.test(defaultBranch)) return false;
    return true;
  }));

  /* D-2; invariant I-2. Touch before close, adverse on collision. */
  cases.push(fixture("touch_before_close_targets_at_bar_high_stops_at_bar_low_collisions_adverse", () => {
    const XP = require("./_investorExitPolicy");
    const cfg = { ...strategy.parameters, takeProfitPct: 2, trailingArmsAtPct: 1.5, trailingStopPct: -1, stopLossPct: -8 };
    /* REGRESSION: the pre-fix close-only convention. The bar touched the
       target at its high and closed below it; the close alone sees nothing. */
    const closeOnly = XP.exitSignal(0.1, 1, cfg, { mark: 101, entry: 100, peak: 100 });
    if (closeOnly.exit !== false || closeOnly.touchBasis !== "close_only") return false;
    const touched = XP.exitSignal(0.1, 1, cfg, { mark: 101, entry: 100, peak: 100, barHigh: 102.5, barLow: 100.4 });
    if (touched.exit !== true || touched.kind !== "take_profit" || touched.touchBasis !== "bar_high_low") return false;
    /* A stop touched at the low and recovered by the close is still a stop. */
    const stopTouch = XP.exitSignal(0.1, 1, cfg, { mark: 95, entry: 100, peak: 100, barHigh: 96, barLow: 91.5 });
    if (stopTouch.exit !== true || stopTouch.kind !== "stop_loss") return false;
    if (XP.exitSignal(0.1, 1, cfg, { mark: 95, entry: 100, peak: 100 }).exit !== false) return false;
    /* Trailing stop: the giveback is measured at the bar low. */
    const trail = XP.exitSignal(0.1, 1, cfg, { mark: 104.5, entry: 100, peak: 105, barHigh: 105, barLow: 103.8 });
    if (trail.exit !== true || trail.kind !== "trailing_stop") return false;
    /* SAME-BAR COLLISION: both the target and the stop inside one bar and no
       finer sequence. Resolved adversely, flagged UNSCORABLE; never the
       favourable path. */
    const both = XP.exitSignal(0.1, 1, cfg, { mark: 100, entry: 100, peak: 100, barHigh: 102.5, barLow: 91 });
    if (both.exit !== true || both.kind !== "stop_loss" || both.sameBarCollision !== true) return false;
    if (!both.collision || both.collision.resolution !== "adverse" || both.collision.scoring !== "UNSCORABLE" || both.scorable !== false) return false;
    /* A strike is a touch: the bar low reached the level, the close did not. */
    const STK = require("./_investorStrike");
    const plan = { armBelowUsd: 99.6, floorUsd: 97.11 };
    const closeMiss = STK.strikeVerdict({ low: 99.5, high: 100.4, close: 100.1 }, plan);
    if (closeMiss.strike !== true || closeMiss.touchBasis !== "bar_low") return false;
    if (STK.strikeVerdict(100.1, plan).strike !== false) return false;
    const gap = STK.strikeVerdict({ low: 96.9, high: 100, close: 98.5 }, plan);
    if (gap.strike !== false || gap.gap !== true) return false;
    /* The guard and the deep scan both pass the bar's high and low. */
    const PG = require("./_investorPositionGuard");
    if (!/barHigh:\s*Number\(last\.h\)/.test(sourceOf(PG.runGuard))) return false;
    const cycle = require("./investorCycle-background");
    if (!/barHigh:\s*Number\(last\.h\)/.test(sourceOf(cycle.runCycle))) return false;
    return true;
  }));

  /* D-3; invariant I-4. The calibration gate stays, and is declared. */
  cases.push(fixture("calibration_state_is_declared_with_finite_sessions_and_an_earliest_eligible_date", () => {
    const state = C.calibrationState({ sessionsAvailable: 40, nowMs: Date.parse("2026-09-04T16:00:00Z") });
    if (state.state !== "calibration_unavailable") return false;
    if (state.sessionsRequired !== 534 || C.REQUIRED_SESSIONS !== 534) return false;
    if (state.sessionsAvailable !== 40 || state.sessionsRemaining !== 494) return false;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(String(state.earliestEligibleSession))) return false;
    if (!(Number(state.calendarDaysAhead) > 494 && Number(state.calendarDaysAhead) < 800)) return false;
    /* The strict cost hurdle reports the state instead of failing silently. */
    const hurdle = S.costHurdle({ cumResidual: -0.03, advUsd: 5e8, grade: "A", wideSpreadWindow: false,
      vixNorm: 1, cfg: strategy.parameters, reversionMult: 1 });
    if (hurdle.pass !== false || !hurdle.calibration || hurdle.calibration.state !== "calibration_unavailable") return false;
    if (hurdle.calibration.required !== true || hurdle.calibration.known !== false) return false;
    /* The strict version is recorded as never order-eligible; a claim is refused. */
    const ST = require("./_investorStrategy");
    const eligibility = ST.orderEligibility(strategy.parameters);
    if (eligibility.eligible !== false || eligibility.code !== "calibration_unavailable") return false;
    let refused = false;
    try { ST.assertPerformanceClaimAllowed({ version: strategy.version, parameters: strategy.parameters }); }
    catch { refused = true; }
    if (!refused) return false;
    if (ST.orderEligibility({ ...strategy.parameters, requireCalibratedEdge: false }).eligible !== true) return false;
    /* The dashboard and the frozen strategy document carry it. */
    const api = require("./investorApi");
    if (!/calibrationState\(/.test(sourceOf(api.ACTIONS.dashboard))) return false;
    if (!/orderEligibility/.test(sourceOf(B.freezeStrategy))) return false;
    return true;
  }));

  /* D-4; invariant I-4. Configuration provenance on every result row. */
  cases.push(fixture("configuration_provenance_names_lowered_hurdles_and_disabled_breakers", () => {
    const ST = require("./_investorStrategy");
    const explore = ST.paperLearningConfig(strategy.parameters,
      { paperLearning: { enabled: true, ...strategy.exploratoryAuto.paperLearningDefaults } }).cfg;
    const controls = { ...strategy.portfolioControls, ...strategy.exploratoryAuto.portfolioControls };
    const id = ST.riskConfigIdentity(explore, controls);
    if (!/^[a-f0-9]{64}$/.test(id.riskConfigHash)) return false;
    const keys = id.deviationsFromDeclared.map((d) => d.key);
    for (const k of ["costMarginMultiple", "minAdvUsd"]) {
      const d = id.deviationsFromDeclared.find((x) => x.key === k);
      if (!d || d.kind !== "hurdle_lowered" || !(Number(d.effective) < Number(d.declared))) throw new Error(`FAIL ${JSON.stringify({ missing: k })}`);
    }
    for (const k of ST.BREAKER_KEYS) {
      const d = id.deviationsFromDeclared.find((x) => x.key === k);
      if (!d || d.kind !== "breaker_disabled" || d.declared === null) throw new Error(`FAIL ${JSON.stringify({ missing: k })}`);
    }
    if (id.breakersDisabled.length !== 5 || id.bannerRequired !== true || id.matchesDeclared !== false) return false;
    /* The declared configuration deviates from itself nowhere and needs no banner. */
    const strict = ST.riskConfigIdentity(strategy.parameters, strategy.portfolioControls);
    if (strict.deviationsFromDeclared.length !== 0 || strict.bannerRequired !== false || strict.matchesDeclared !== true) return false;
    if (strict.riskConfigHash === id.riskConfigHash) return false;
    /* Hash moves with any effective parameter, not only the named ones. */
    if (ST.riskConfigIdentity({ ...explore, exitBeforeEarningsDays: 3 }, controls).riskConfigHash === id.riskConfigHash) return false;
    /* Every result row carries it: order → position → closed trade, NAV row,
       daily finalization row. */
    if (!/riskConfigHash/.test(sourceOf(L.proposeOrder)) || !/riskConfigHash:o\.riskConfigHash/.test(sourceOf(L.recordFill))
        || !/riskConfigHash:p\.riskConfigHash/.test(sourceOf(L.closePosition))) return false;
    const NAV = require("./_investorNav");
    if (!/riskConfigHash/.test(sourceOf(NAV.record))) return false;
    const cycle = require("./investorCycle-background");
    if (!/riskConfigHash: riskConfig \? riskConfig\.riskConfigHash/.test(sourceOf(cycle.runCycle))) return false;
    if (!/proposeOrder\(\{[^}]*riskConfig/.test(sourceOf(cycle.runCycle))) return false;
    const api = require("./investorApi");
    if (!/riskConfigSeries/.test(sourceOf(api.ACTIONS.performance)) || !/riskConfigIdentity/.test(sourceOf(api.ACTIONS.dashboard))) return false;
    return true;
  }));

  /* D-5; invariant I-4. Exit regimes never mix. */
  cases.push(fixture("variant_parity_every_variant_declares_its_exit_regime_and_regimes_cannot_be_promoted_across", () => {
    if (V.validateVariants().count !== 15) return false;
    for (const variant of V.VARIANTS) {
      const p = V.exitParity(variant);
      if (!p.regime) throw new Error(`FAIL ${JSON.stringify({ variant: variant.id })}`);
      if (!p.complete && variant.exitRegime !== "pre-v18") throw new Error(`FAIL ${JSON.stringify({ variant: variant.id, missing: p.missing })}`);
    }
    /* A pre-v18 variant may not become the live configuration of a desk that
       takes profits (the post-v18 exploratory configuration). */
    const post = { ...strategy.parameters, ...strategy.exploratoryAuto.paperLearningDefaults };
    if (V.exitRegimeOf(post) !== "post-v18" || V.exitRegimeOf(strategy.parameters) !== "pre-v18") return false;
    const blocked = V.promotionAllowed(V.byId("A"), post);
    if (blocked.ok !== false || !/regimes cannot mix/.test(blocked.reason)) return false;
    if (V.promotionAllowed(V.byId("A"), strategy.parameters).ok !== true) return false;
    /* A variant that declares every exit parameter carries its own regime. */
    const complete = { id: "Z", params: { takeProfitPct: 2, trailingArmsAtPct: 1.5, trailingStopPct: -1,
      holdLosersThroughRankExit: true, requireSessionMove: true } };
    if (V.exitParity(complete).regime !== "post-v18" || V.promotionAllowed(complete, post).ok !== true) return false;
    /* Undeclared AND untagged is refused at validation. */
    let refused = false;
    try { V.validateVariants([{ id: "Y", params: { entryRank: 0.1 } }]); } catch { refused = true; }
    if (!refused) return false;
    /* The hash over the observation-bearing parameters is unchanged by the tag. */
    if (!/^[a-f0-9]{64}$/.test(V.variantsHash())) return false;
    if (Object.keys(V.materialized()[0]).sort().join(",") !== "id,name,params") return false;
    /* The cycle refuses the promotion, not merely the validator. */
    const cycle = require("./investorCycle-background");
    if (!/promotionAllowed\(liveVariant/.test(sourceOf(cycle.runCycle))) return false;
    return true;
  }));

  /* D-6; invariant I-3. A two-company sweep issues one request per global source. */
  cases.push(fixture("source_scope_global_feeds_are_fetched_once_per_sweep_and_fanned_out", async () => {
    const IS = require("./_investorIntelligenceSources");
    for (const src of Object.values(IS.SOURCE_REGISTRY)) {
      if (!["global", "company", "regional"].includes(src.scope)) throw new Error(`FAIL ${JSON.stringify({ source: src.source_id })}`);
    }
    if (IS.scopeOf("dol.releases") !== "global" || IS.scopeOf("company.direct") !== "company"
        || IS.scopeOf("federal.register") !== "company" || IS.scopeOf("nws.alerts") !== "regional") return false;
    if (Object.values(IS.SOURCE_REGISTRY).filter((x) => x.kind === "feed").some((x) => x.scope !== "global")) return false;
    if (IS.stateDocId("dol.releases", "ACME") !== "dol.releases" || IS.stateDocId("federal.register", "ACME") !== "federal.register_ACME") return false;
    /* An in-memory sweep: one global feed, one company source, two companies. */
    const urls = [];
    const feedXml = `<rss><channel><item><title>Acme Corp settles wage case</title><link>https://www.dol.gov/a1</link>` +
      `<description>Acme Corp and Beta Industries named in the settlement</description></item></channel></rss>`;
    const io = {
      fetch: async (url) => { urls.push(String(url)); return /federalregister/.test(String(url))
        ? { json: { results: [] }, text: "", sha256: "b".repeat(64), fetchedAt: new Date().toISOString() }
        : { text: feedXml, sha256: "a".repeat(64), etag: null, lastModified: null, notModified: false }; },
      readState: async (sourceId, symbol) => ({ id: IS.stateDocId(sourceId, symbol), ref: null, data: {} }),
      markSuccess: async () => {}, markFailure: async () => {},
      recordItems: async (profile, source, items) => items.filter((it) => IS.matchesProfile(it, profile, source)).length,
    };
    const profiles = ["ACME|Acme Corp", "BETA|Beta Industries"].map((s) => {
      const [symbol, companyName] = s.split("|");
      return { symbol, companyName, aliases: [companyName], sourceIds: ["dol.releases", "federal.register"] };
    });
    const sweep = IS.createSweep({ companies: profiles.map((p) => p.symbol), io });
    const out = [];
    for (const p of profiles) out.push(await IS.pollCompany(p, { sweep }));
    const feedFetches = urls.filter((u) => /dol\.gov/.test(u)).length;
    const registerFetches = urls.filter((u) => /federalregister/.test(u)).length;
    if (feedFetches !== 1 || registerFetches !== 2) throw new Error(`FAIL ${JSON.stringify({ feedFetches, registerFetches })}`);
    /* Both companies received the feed's items through entity resolution. */
    const matched = out.map((o) => o.results.find((r) => r.sourceId === "dol.releases").matched);
    if (matched[0] !== 1 || matched[1] !== 1) throw new Error(`FAIL ${JSON.stringify({ matched })}`);
    if (out[1].results.find((r) => r.sourceId === "dol.releases").reused !== true) return false;
    const budget = IS.sweepBudget(sweep);
    /* one global source × 1, one company source × two companies */
    if (budget.allowed !== 1 + 1 * 2 || budget.used !== 1 + 2 || budget.ok !== true) throw new Error(`FAIL ${JSON.stringify({ budget })}`);
    if (IS.assertSweepBudget(sweep).ok !== true) return false;
    /* The assertion fires when a global source is fetched once per company. */
    const bad = IS.createSweep({ companies: ["ACME", "BETA"], io });
    bad.polled.global.add("dol.releases"); bad.counts.globalFetches = 2;
    const breach = IS.assertSweepBudget(bad);
    if (breach.ok !== false || breach.alert.type !== "SOURCE_SCOPE_BREACH" || bad.breaches.length !== 1) return false;
    /* The evidence sweep uses one sweep context and asserts the budget. */
    const cycle = require("./investorCycle-background");
    const cycleSrc = sourceOf(cycle.handler);
    if (!/createSweep/.test(sourceOf(cycle.runCycle)) && !/createSweep/.test(cycleSrc)) {
      const fs = require("fs"), path = require("path");
      let text = "";
      try { text = fs.readFileSync(path.join(__dirname, "investorCycle-background.js"), "utf8"); } catch {}
      if (text && !(/IS\.createSweep\(/.test(text) && /IS\.assertSweepBudget\(sweep\)/.test(text))) return false;
    }
    return true;
  }));

  /* D-7. Static assertions over the console source. */
  cases.push(fixture("console_has_no_duplicate_top_level_declarations_and_plan_block_dictionary_is_reachable", () => {
    const fs = require("fs"), path = require("path");
    const candidates = [];
    for (const root of [__dirname, process.cwd()]) {
      for (let up = 0; up <= 4; up += 1) candidates.push(path.join(root, ...Array(up).fill(".."), "investor.html"));
    }
    const file = candidates.find((p) => { try { return fs.statSync(p).isFile(); } catch { return false; } });
    if (!file) throw new Error(`investor.html not reachable from the bundle (netlify.toml included_files must carry it); looked in ${candidates.slice(0, 3).join(", ")}`);
    const html = fs.readFileSync(file, "utf8");
    const script = html.slice(html.indexOf("<script>"), html.lastIndexOf("</script>"));
    const declared = new Map();
    for (const m of script.matchAll(/^function ([A-Za-z_$][\w$]*)\s*\(/gm)) declared.set(m[1], (declared.get(m[1]) || 0) + 1);
    const dupes = [...declared.entries()].filter(([, n]) => n > 1).map(([k]) => k);
    if (dupes.length) throw new Error(`FAIL ${JSON.stringify({ duplicates: dupes })}`);
    if (declared.has("plainBlock")) return false;
    if (!declared.has("plainPlanBlock") || !declared.has("plainSellBlock")) return false;
    const planFn = /function plainPlanBlock\([^)]*\)\s*\{[^}]*PLAN_BLOCK_PLAIN/.test(script);
    const callSites = (script.match(/plainPlanBlock\(/g) || []).length - 1;
    if (!planFn || callSites < 1) throw new Error(`FAIL ${JSON.stringify({ planFn, callSites })}`);
    if ((script.match(/plainSellBlock\(/g) || []).length < 2) return false;
    return true;
  }));

  /* D-8. Dispatch windows are per task class. */
  cases.push(fixture("scheduler_windows_premarket_manager_dispatchable_at_0830_and_scan_window_unchanged", () => {
    const K = require("./investorKick");
    const at0830 = 8 * 60 + 30;
    if (K.taskDispatchable("premarket_manager", at0830) !== true) return false;
    if (K.taskDispatchable("focused_research", at0830) !== true) return false;
    if (K.taskDispatchable("scan", at0830) !== false) return false;
    if (K.taskDispatchable("ingest", at0830) !== true || K.taskDispatchable("ingest", 12 * 60) !== false) return false;
    if (K.taskDispatchable("no_such_task", 12 * 60) !== false || K.dispatchWindow("no_such_task") !== null) return false;
    /* Bit-identical scan window. */
    if (K.PLAN_EARLIEST_MIN !== 9 * 60 + 45 || K.PLAN_LATEST_MIN !== 15 * 60 + 30) return false;
    if (JSON.stringify(K.TASK_WINDOWS_ET.scan.segments) !== JSON.stringify([[585, 930]])) return false;
    if (K.taskDispatchable("scan", 585) !== true || K.taskDispatchable("scan", 584) !== false
        || K.taskDispatchable("scan", 930) !== true || K.taskDispatchable("scan", 931) !== false) return false;
    if (JSON.stringify(K.normalizePlanTimes(["15:55", "09:30", "12:00", "nonsense", "12:00"])) !== JSON.stringify(["12:00"])) return false;
    if (JSON.stringify(K.normalizePlanTimes(["08:30", "09:30"], "premarket_manager")) !== JSON.stringify(["08:30"])) return false;
    if (K.normalizePlanTimes(["08:30"], "no_such_task") !== null) return false;
    return true;
  }));


  /* ── The legacy baseline is frozen and exported (blueprint §21 commit 3;
     §16.5 arms 2a/2b; §10.4 legacy collections; §13 configuration hygiene). */
  cases.push(fixture("legacy_baseline_is_frozen_with_full_hashes_and_the_legacy_surface_is_labelled", () => {
    const fs = require("fs"), path = require("path");
    const A2 = require("./_investorAdmin");
    const base = B.legacyBaseline({ strategy, universe: U, commit: "fixture" });
    if (base.schema !== "legacy-baseline.v1" || base.engineVersion !== "legacy-v18") return false;
    for (const h of [base.strategyHash, base.universeHash, base.variantsHash,
      base.arms.exploratoryControl.riskConfigHash, base.arms.declaredStrict.riskConfigHash]) {
      if (!/^[a-f0-9]{64}$/.test(String(h))) return false;
    }
    if (base.strategyVersion !== "v18" || base.universeVersion !== "v6" || base.eligibleCount !== 304) return false;
    /* Determinism: the same inputs give the same identity. */
    const again = B.legacyBaseline({ strategy, universe: U, commit: "fixture" });
    if (JSON.stringify(canonical(again)) !== JSON.stringify(canonical(base))) return false;
    /* The two arms are distinct configurations; the strict arm is not order-eligible. */
    if (base.arms.exploratoryControl.riskConfigHash === base.arms.declaredStrict.riskConfigHash) return false;
    if (base.arms.exploratoryControl.breakersDisabled.length !== 5 || base.arms.exploratoryControl.trackRecord !== true) return false;
    if (base.arms.declaredStrict.orderEligibility.eligible !== false || base.arms.declaredStrict.trackRecord !== false) return false;
    /* The legacy collections of §10.4 are declared and projected records are labelled. */
    const expected = ["Candidates", "Decisions", "StrategyVersions", "ShadowDays", "ShadowOpen", "ShadowClosed",
      "ShadowAccounts", "ShadowObservations", "Calibration", "SoakCycles", "ScanSnapshots", "EntryPlans"];
    for (const n of expected) if (!A2.LEGACY_COLLECTIONS.includes(A2.COL_PREFIX + n)) throw new Error(`legacy collection missing: ${n}`);
    if (A2.LEGACY_COLLECTIONS.length !== expected.length) return false;
    const projected = A2.legacyProjection({ symbol: "X" }, { collection: A2.COL.plans });
    if (projected.engineVersion !== "legacy-v18" || projected.decisionAuthority !== "deterministic_v18") return false;
    if (A2.legacyProjection({ engineVersion: "fund-manager-v1" }).engineVersion !== "fund-manager-v1") return false;
    if (A2.legacyProjection({ x: 1 }, { collection: A2.COL.control }).engineVersion !== "fund-manager-v1") return false;
    /* Configuration hygiene, where the checkout is visible: no nested
       netlify.toml, and the stale JSON files carry a retired marker. */
    const root = [path.join(__dirname, "..", ".."), process.cwd()].find((r) => fs.existsSync(path.join(r, "netlify.toml")));
    if (root) {
      if (fs.existsSync(path.join(root, "netlify", "functions", "netlify.toml"))) throw new Error("nested netlify/functions/netlify.toml must not exist");
      for (const f of ["investor/strategies/v1.json", "investor/universe/v1.json"]) {
        const p = path.join(root, f);
        if (!fs.existsSync(p)) continue;
        const doc = JSON.parse(fs.readFileSync(p, "utf8"));
        if (!doc.retired || doc.retired.authority !== "none") throw new Error(`${f} lacks a retired marker`);
      }
    }
    return true;
  }));


  /* ═══ GROUP 2 — PRIMITIVES (blueprint §19.1 group 2; §21 commit 4) ═══ */

  /* §3 fixed routing, §7.1 contract, §9.2/9.3 prices, §15.1 mandate. */
  cases.push(fixture("fund_manager_policy_is_hashed_routing_is_fixed_and_the_mandate_contract_validates", () => {
    const P = require("./_investorPolicy");
    /* Routing: Luna extracts, Sol decides at high effort, Terra is gone, and
       no model may filter the roster before Sol sees it. */
    if (P.ROLE_MODELS.facts.model !== "gpt-5.6-luna" || P.ROLE_MODELS.manager.model !== "gpt-5.6-sol") return false;
    if (P.ROLE_MODELS.manager.reasoning.effort !== "high") return false;
    if (!P.FORBIDDEN_INVESTMENT_MODELS.includes("gpt-5.6-terra") || P.ROSTER_FILTER_MODELS_ALLOWED.length !== 0) return false;
    /* Identity is stable and every schema has a hash. */
    const a = P.policyIdentity(), b = P.policyIdentity();
    for (const h of [a.policyHash, a.riskPolicyHash, a.modelPolicyHash, a.costPolicyHash]) if (!/^[a-f0-9]{64}$/.test(h)) return false;
    if (a.policyHash !== b.policyHash || Object.keys(a.schemaHashes).length !== 11) return false;
    if (a.calendarId !== "XNYS" || P.POLICY_VERSION !== "fund-manager-v1") return false;
    /* The §7.1 worked example validates; a JSON number at a money boundary,
       an unknown field, a fractional share and INCREASE do not. */
    const ex = P.EXAMPLE_MANDATE_PROPOSAL;
    if (!P.validate("mandate-proposal.v1", ex).ok) throw new Error(JSON.stringify(P.validate("mandate-proposal.v1", ex).errors));
    const withNumber = { ...ex, allocation: { ...ex.allocation, proposedQuantityUnits: 86 } };
    if (P.validate("mandate-proposal.v1", withNumber).ok) return false;
    if (P.validate("mandate-proposal.v1", { ...ex, extra: 1 }).ok) return false;
    if (P.validate("mandate-proposal.v1", { ...ex, allocation: { ...ex.allocation, quantityScale: 2 } }).ok) return false;
    if (P.validate("mandate-proposal.v1", { ...ex, action: { ...ex.action, kind: "INCREASE" } }).ok) return false;
    if (P.validate("mandate-proposal.v1", { ...ex, decision: "INCREASE" }).ok) return false;
    /* The strict subset is generated, never hand-maintained: no unsupported
       keyword survives, every object is closed and fully required. */
    const strict = P.strictOutputSchema(P.SCHEMAS["mandate-proposal.v1"], { name: "mandate" });
    const text = JSON.stringify(strict.schema);
    if (/"pattern"|"maxLength"|"minimum"/.test(text)) return false;
    if (strict.schema.additionalProperties !== false || strict.schema.required.length !== Object.keys(strict.schema.properties).length) return false;
    if (strict.schemaHash !== a.schemaHashes["mandate-proposal.v1"]) return false;
    /* §9.3 cost formula: exact, and the §9.5 reasoning-token arithmetic. */
    const c = (rt) => P.costMinor({ model: "gpt-5.6-sol", outputTokens: 304 * rt }).amountMinor;
    if (c(500) !== "304" || c(1000) !== "608" || c(1500) !== "912") return false;
    const luna = P.costMinor({ model: "gpt-5.6-luna", ordinaryInputTokens: 1000000, cacheWriteTokens: 0, cachedReadTokens: 0, outputTokens: 100000 });
    if (luna.amountMinor !== "32") return false;            // $0.20 + $0.12
    const long = P.costNanoUsd({ model: "gpt-5.6-sol", ordinaryInputTokens: 300000, outputTokens: 1000 });
    if (long.longContext !== true || long.nanoUsd !== 2430000000n) return false;
    if (P.costNanoUsd({ model: "gpt-5.6-sol", ordinaryInputTokens: 272000, outputTokens: 0 }).longContext !== false) return false;
    /* §15.1: long-only whole-share common equity; owner overrides stay inside bounds. */
    const rm = P.RISK_MANDATE;
    if (!rm.instruments.longOnly || rm.instruments.quantityScale !== 0 || rm.instruments.leverage || rm.instruments.increaseAction) return false;
    const ov = P.applyRiskMandateOverrides({ "weights.maxSingleNameWeightBps": "800", "weights.maxGrossExposureBps": "99999", "losses.dailyLossFreezeBps": 12 });
    if (ov.applied.length !== 1 || ov.refused.length !== 2 || ov.riskMandate.weights.maxSingleNameWeightBps !== "800") return false;
    if (P.RISK_MANDATE.weights.maxSingleNameWeightBps !== "1000") return false;   // never mutated
    /* §8.7: the emergency policy is inactive until an owner approves it, and
       cannot be widened to open a long. */
    if (P.activeEmergencyPolicy(null).active !== false) return false;
    const approved = { ...JSON.parse(JSON.stringify(P.EMERGENCY_RISK_POLICY_TEMPLATE)), status: "APPROVED", approvedBy: "owner", approvedAtMs: 1 };
    delete approved.policyHash; approved.policyHash = P.sha256(approved);
    if (P.activeEmergencyPolicy(approved).active !== true) return false;
    const widened = { ...approved, permittedOperations: [...approved.permittedOperations, "OPEN_LONG"] };
    delete widened.policyHash; widened.policyHash = P.sha256(widened);
    if (P.activeEmergencyPolicy(widened).active !== false) return false;
    if (P.activeEmergencyPolicy({ ...approved, approvedBy: null }).active !== false) return false;
    /* Hash sets are validated exactly. */
    const active = P.loadActiveSync({});
    if (!P.validateHashSet({ policyHash: active.policyHash, schemaHash: active.schemaHashes["universe-review.v1"] }, active).ok) return false;
    if (P.validateHashSet({ riskPolicyHash: "0".repeat(64) }, active).ok) return false;
    /* §6.1 cutoffs and D-11 ingest configuration. */
    if (P.CUTOFFS_ET.evidenceFreezeMin !== 510 || P.CUTOFFS_ET.holdingHardDeadlineMin !== 555 || P.CUTOFFS_ET.expansionSoftDeadlineMin !== 560) return false;
    if (P.CUTOFFS_ET.ingest.perSweep !== 8 || P.CUTOFFS_ET.ingest.daytimeRescan !== false || P.CUTOFFS_ET.ingest.freshnessWindowHours !== 6) return false;
    if (!(P.CUTOFFS_ET.platform.runLeaseTtlSeconds < P.CUTOFFS_ET.platform.functionCapSeconds)) return false;
    return true;
  }));

  /* §7.1 numeric contract: strings on the wire, BigInt in memory, Number never. */
  cases.push(fixture("money_primitive_is_exact_and_refuses_binary_floating_point_at_every_boundary", () => {
    const MN = require("./_investorMoney");
    const r = MN.selfCheck();
    if (r.pass !== true) throw new Error(`money selfCheck: ${JSON.stringify(r.failures.slice(0, 3))}`);
    if (!(r.vectors >= 25)) return false;
    /* The §7.1 worked example, derived exactly once by the calculator. */
    const P = require("./_investorPolicy");
    const f = P.EXAMPLE_MANDATE_PROPOSAL.forecast;
    if (String(MN.expectedTerminalPriceMicros(f.outcomeBuckets)) !== "46750000") return false;
    if (MN.assertPpmDistribution(f.outcomeBuckets).ok !== true) return false;
    if (MN.assertPpmDistribution([{ id: "a", probabilityPpm: "500000" }, { id: "b", probabilityPpm: "499999" }]).ok !== false) return false;
    const loss = MN.plannedLossMinor({ limitPriceMicros: "43250000", lossBoundaryPriceMicros: "37250000",
      quantityUnits: "86", costPerShareMicros: "40000", mode: MN.ROUNDING.HALF_UP });
    /* 86 × ($43.25 − $37.25 + $0.04 cost) = $519.44 at the worst authorized fill. */
    if (String(loss) !== "51944") throw new Error(`planned loss ${String(loss)}`);
    /* A JSON number is refused wherever an audited value is expected. */
    let refused = false;
    try { MN.parseInteger(86, { name: "quantityUnits" }); } catch (e) { refused = e.code === "NOT_CANONICAL_INTEGER"; }
    if (!refused) return false;
    if (MN.isCanonicalIntegerString("007") || MN.isCanonicalIntegerString("+7") || MN.isCanonicalIntegerString("-0")) return false;
    if (MN.validateQuantity({ quantityUnits: "5", quantityScale: 2 }).ok !== false) return false;
    /* Broker round trips are lossless and off-tick prices are rejected. */
    if (MN.toBrokerDecimalPrice("43250000", { tickMicros: "10000", decimals: 2 }) !== "43.25") return false;
    let offTick = false;
    try { MN.toBrokerDecimalPrice("43251000", { tickMicros: "10000", decimals: 2 }); } catch (e) { offTick = e.code === "OFF_TICK"; }
    if (!offTick) return false;
    if (MN.divRound(-7n, 2n, MN.ROUNDING.HALF_EVEN) !== -4n || MN.divRound(7n, 2n, MN.ROUNDING.HALF_EVEN) !== 4n
        || MN.divRound(5n, 2n, MN.ROUNDING.HALF_EVEN) !== 2n || MN.divRound(-1n, 3n, MN.ROUNDING.FLOOR) !== -1n) return false;
    return true;
  }));

  /* §10.2 collections and §10.6 indexes are declared once, uniquely. */
  cases.push(fixture("manager_collections_and_required_indexes_are_declared", () => {
    const A2 = require("./_investorAdmin");
    const names = Object.values(A2.COL);
    if (new Set(names).size !== names.length) return false;
    if (!names.every((n) => n.startsWith(A2.COL_PREFIX))) return false;
    for (const k of ["dossiers", "dossierVersions", "financialFacts", "evidenceDeltas", "managerRuns", "managerDecisions",
      "claimVerifications", "researchMemos", "portfolioPlans", "activationSnapshots", "mandateProposals", "mandates",
      "activationEnvelopes", "activeMandates", "mandateEvents", "orderSets", "orderLegs", "brokerEvents",
      "capitalReservations", "reservationAccounts", "executionOutbox", "workerNonces", "emergencyPlans",
      "emergencyRiskPolicies", "alerts", "mutations", "calendarSnapshots", "corporateActions", "forecasts",
      "counterfactuals", "postmortems", "evals", "kpiDaily", "contentManifests"]) {
      if (!A2.COL[k]) throw new Error(`collection missing: ${k}`);
    }
    if (A2.REQUIRED_INDEXES.length < 20) return false;
    for (const ix of A2.REQUIRED_INDEXES) {
      if (!names.includes(ix.collection) || !Array.isArray(ix.fields) || ix.fields.length < 2) return false;
    }
    return true;
  }));

  /* §10.3 storage shape: schema-aware codecs, byte limits, content pointers. */
  cases.push(fixture("storage_codec_round_trips_every_v1_shape_and_refuses_oversized_or_guessed_documents", () => {
    const SC = require("./_investorStorageCodec");
    const r = SC.selfCheck();
    if (r.pass !== true) throw new Error(`codec selfCheck: ${JSON.stringify(r.failures.slice(0, 3))}`);
    if (SC.MAX_DOCUMENT_BYTES !== 1048576 || !(SC.SAFE_DOCUMENT_BYTES < SC.MAX_DOCUMENT_BYTES)) return false;
    for (const v of ["manager-run.v1", "manager-decision.v1", "mandate-proposal.v1", "mandate-binding.v1",
      "activation-envelope.v1", "dossier-version.v1", "financial-fact.v1", "order-set.v1", "order-leg.v1",
      "broker-event.v1", "capital-reservation.v1", "content-manifest.v1"]) {
      if (!SC.codecFor(v) || !String(SC.collectionFor(v)).startsWith("InvestorAI_")) throw new Error(`codec missing: ${v}`);
    }
    /* A manager run never carries 304 rows; one decision per document does. */
    const rows = Array.from({ length: 304 }, (_, i) => ({ symbol: `S${i}`, decision: "WATCH", reason: "x".repeat(900) }));
    let tooLarge = false;
    try { SC.encode({ schemaVersion: "manager-run.v1", managerRunId: "r", status: "COMPLETE", universeVersion: "v6",
      universeHash: "a".repeat(64), eligibleCount: 304, rows }); } catch (e) { tooLarge = e.code === "DOCUMENT_TOO_LARGE"; }
    if (!tooLarge) return false;
    const one = SC.encode({ schemaVersion: "manager-decision.v1", managerRunId: "r", symbol: "S1", decision: "WATCH", reason: "y" });
    if (!one._codec || !/^[a-f0-9]{64}$/.test(one._codec.contentHash)) return false;
    let guessed = false;
    try { SC.decode({ managerRunId: "r", symbol: "S1" }); } catch (e) { guessed = e.code === "UNRECOGNISED_SHAPE"; }
    if (!guessed) return false;
    let floatRefused = false;
    try { SC.encode({ schemaVersion: "manager-decision.v1", managerRunId: "r", symbol: "S1", decision: "BUY", limitPriceMicros: 43.25 }); }
    catch (e) { floatRefused = e.code === "FLOAT_AT_MONEY_BOUNDARY"; }
    if (!floatRefused) return false;
    return true;
  }));

  /* §10.3 content store: content-addressed, verified, never truncated into a field. */
  cases.push(fixture("content_store_is_content_addressed_verified_and_fails_visibly_when_unavailable", async () => {
    const CS = require("./_investorContentStore");
    const r = await CS.selfCheck();
    if (r.pass !== true) throw new Error(`content store selfCheck: ${JSON.stringify(r.failures.slice(0, 3))}`);
    if (!String(CS.objectKey("document", "a".repeat(64))).startsWith("investor-ai/document/aa/")) return false;
    if (CS.MAX_OBJECT_BYTES !== 256 * 1024 * 1024) return false;
    return true;
  }));


  /* ═══ GROUP 3 — JOBS AND DISPATCH (blueprint §11.1, D-10; §21 commit 6) ═══ */

  /* An in-memory Firestore for the lease/nonce/claim transactions. */
  function fakeAdmin() {
    const A2 = require("./_investorAdmin");
    const docs = new Map();
    const key = (c, id) => `${c}/${id}`;
    const merge = (cur, data) => {
      const out = { ...(cur || {}) };
      for (const [k, v] of Object.entries(data)) {
        if (v && typeof v === "object" && v.__inc !== undefined) out[k] = (Number(out[k]) || 0) + v.__inc;
        else if (v && typeof v === "object" && v.__ts) out[k] = Date.now();
        else out[k] = v;
      }
      return out;
    };
    const ref = (c, id) => ({ id, collection: c,
      async get() { const d = docs.get(key(c, id)); return { exists: d !== undefined, id, data: () => d }; },
      async set(data, opts) { const cur = docs.get(key(c, id)); docs.set(key(c, id), opts && opts.merge ? merge(cur, data) : merge({}, data)); } });
    const query = (c, filters = []) => ({
      where: (f, op, v) => query(c, [...filters, [f, op, v]]),
      limit: () => query(c, filters),
      async get() {
        const rows = [];
        for (const [k, d] of docs) {
          if (!k.startsWith(`${c}/`)) continue;
          const cmp = (a, b) => (typeof a === "string" || typeof b === "string" ? String(a).localeCompare(String(b)) : Number(a) - Number(b));
          if (filters.every(([f, op, v]) => op === "==" ? d[f] === v : op === "in" ? v.includes(d[f]) : op === "!=" ? d[f] !== v
            : op === ">=" ? d[f] !== undefined && cmp(d[f], v) >= 0 : op === ">" ? d[f] !== undefined && cmp(d[f], v) > 0
            : op === "<=" ? d[f] !== undefined && cmp(d[f], v) <= 0 : op === "<" ? d[f] !== undefined && cmp(d[f], v) < 0 : true)) rows.push(d);
        }
        return { docs: rows.map((d) => ({ data: () => d })), forEach: (fn) => rows.forEach((d) => fn({ data: () => d })) };
      },
    });
    return { docs, COL: A2.COL, FV: { increment: (n) => ({ __inc: n }), serverTimestamp: () => ({ __ts: true }) },
      envelope: () => ({}),
      col: (c) => ({ doc: (id) => ref(c, id), where: (...a) => query(c).where(...a), limit: (n) => query(c).limit(n) }),
      runTransaction: async (fn) => fn({ get: (r) => r.get(), set: (r, d, o) => r.set(d, o) }) };
  }

  cases.push(fixture("run_scoped_lease_spans_segments_yields_immediately_and_reclaims_a_dead_segment_inside_the_cap", async () => {
    const JOBS0 = require("./_investorJobs");
    const P = require("./_investorPolicy");
    const fake = fakeAdmin();
    const JOBS = JOBS0.withAdmin(fake);
    Object.assign(JOBS, { RUN_LEASE_TTL_MS: JOBS0.RUN_LEASE_TTL_MS, runIdFor: JOBS0.runIdFor, runLeaseDocId: JOBS0.runLeaseDocId });
    try {
      /* The run lease TTL is shorter than the 15-minute function cap (D-10). */
      if (!(JOBS.RUN_LEASE_TTL_MS < P.CUTOFFS_ET.platform.functionCapSeconds * 1000)) return false;
      if (!(JOBS.RUN_LEASE_TTL_MS <= 10 * 60 * 1000)) return false;
      const runId = JOBS.runIdFor({ task: "premarket_manager", accountId: "paper-1", tradingDate: "2026-09-04" });
      const seg1 = { jobId: "j1", attempt: 1, runId };
      const seg2 = { jobId: "j2", attempt: 1, runId };
      const t0 = 1_800_000_000_000;
      /* Segment 1 claims; a second segment cannot while the lease is live. */
      if ((await JOBS.claimRunLease(seg1, { nowMs: t0 })).claimed !== true) return false;
      const refused = await JOBS.claimRunLease(seg2, { nowMs: t0 + 60000 });
      if (refused.claimed !== false || refused.reason !== "run_in_flight" || refused.heldBy !== "j1#1") return false;
      /* The same segment re-claims idempotently. */
      if ((await JOBS.claimRunLease(seg1, { nowMs: t0 + 60000 })).claimed !== true) return false;
      /* Segment 1 yields (releases): segment 2 claims IMMEDIATELY, not after 16 minutes. */
      if ((await JOBS.releaseRunLease(seg1, { reason: "yield" })).released !== true) return false;
      const handoff = await JOBS.claimRunLease(seg2, { nowMs: t0 + 61000 });
      if (handoff.claimed !== true || handoff.reclaimed !== true) return false;
      /* A dead segment (never released) is reclaimed once its TTL lapses — inside the cap. */
      const seg3 = { jobId: "j3", attempt: 1, runId };
      if ((await JOBS.claimRunLease(seg3, { nowMs: t0 + 61000 + JOBS.RUN_LEASE_TTL_MS - 1 })).claimed !== false) return false;
      const reclaimed = await JOBS.claimRunLease(seg3, { nowMs: t0 + 61000 + JOBS.RUN_LEASE_TTL_MS + 1 });
      if (reclaimed.claimed !== true) return false;
      const lease = fake.docs.get(`${fake.COL.jobs}/${JOBS.runLeaseDocId(runId)}`);
      if (!lease || lease.segmentOwner !== "j3#1" || lease.lineage.length !== 4 || lease.segmentsSeen !== 3) return false;
      /* Lineage records every handoff, oldest first. */
      if (lease.lineage.map((l) => l.jobId).join(",") !== "j1,j1,j2,j3") return false;
      return true;
    } finally { /* scoped instance; nothing to unbind */ }
  }));

  cases.push(fixture("worker_nonce_is_bound_to_the_exact_attempt_and_consumed_atomically_so_a_replay_is_refused", async () => {
    const JOBS0 = require("./_investorJobs");
    const AUTH2 = require("./_investorAuth");
    const fake = fakeAdmin();
    const JOBS = JOBS0.withAdmin(fake);
    Object.assign(JOBS, { taskFor: JOBS0.taskFor, payloadHash: JOBS0.payloadHash });
    const key = require("crypto").randomBytes(32);
    try {
      const payload = { accountId: "paper-1", tradingDate: "2026-09-04" };
      const enq = await JOBS.enqueueOnce({ task: "premarket_manager", dedupeId: "paper-1_2026-09-04", accountId: "paper-1", payload });
      if (enq.enqueued !== true) return false;
      /* Enqueue is idempotent. */
      if ((await JOBS.enqueueOnce({ task: "premarket_manager", dedupeId: "paper-1_2026-09-04", accountId: "paper-1", payload })).duplicate !== true) return false;
      const jobId = enq.jobId;
      const spec = JOBS.taskFor("premarket_manager");
      if (spec.targetFunction !== "investorManager-background") return false;
      const nonce = await JOBS.issueWorkerNonce({ jobId, task: "premarket_manager", targetFunction: spec.targetFunction,
        attempt: 1, payloadHash: JOBS.payloadHash(payload), key });
      /* Verification is bound to the target function and the attempt. */
      const good = AUTH2.verifyBoundWorkerNonce(nonce.token, "investorManager-background", { key });
      if (!good || good.jobId !== jobId || good.attempt !== 1 || good.nonceId !== nonce.nonceId) return false;
      if (AUTH2.verifyBoundWorkerNonce(nonce.token, "investorExecution-background", { key }) !== null) return false;
      if (AUTH2.verifyBoundWorkerNonce(nonce.token, "investorManager-background", { key, nowMs: nonce.expiresAtMs + 1 }) !== null) return false;
      if (AUTH2.verifyBoundWorkerNonce(nonce.token, "investorManager-background", { key: require("crypto").randomBytes(32) }) !== null) return false;
      /* A tampered payload is refused before any claim. */
      const tampered = await JOBS.claimOnce({ jobId, task: "premarket_manager", targetFunction: spec.targetFunction,
        token: nonce.token, payload: { ...payload, tradingDate: "2026-09-05" }, key });
      if (tampered.claimed !== false || tampered.reason !== "payload_hash_mismatch") return false;
      /* The claim consumes the nonce and starts the segment. */
      const claim = await JOBS.claimOnce({ jobId, task: "premarket_manager", targetFunction: spec.targetFunction, token: nonce.token, payload, key });
      if (claim.claimed !== true || !claim.claim.leaseOwner || claim.claim.attempt !== 1) return false;
      const stored = fake.docs.get(`${fake.COL.workerNonces}/${nonce.nonceId}`);
      if (!stored || stored.status !== "CONSUMED" || stored.consumedBy !== claim.claim.leaseOwner) return false;
      /* A replay of the same token is refused with 409. */
      const replay = await JOBS.claimOnce({ jobId, task: "premarket_manager", targetFunction: spec.targetFunction, token: nonce.token, payload, key });
      if (replay.claimed !== false || replay.httpStatus !== 409 || replay.reason !== "nonce_consumed") return false;
      /* Checkpoint, yield, and the re-dispatch lineage. */
      if ((await JOBS.checkpoint(claim.claim, { stage: "coverage", cursor: 120, data: { ok: true } })).ok !== true) return false;
      if ((await JOBS.yieldSegment(claim.claim, { reason: "segment_budget", resumeAtMs: 1 })).status !== "yielded_resumable") return false;
      const job = fake.docs.get(`${fake.COL.jobs}/${jobId}`);
      if (job.status !== "yielded_resumable" || job.checkpoint.cursor !== 120 || job.attempts !== 1 || job.segments !== 1) return false;
      /* The next segment needs a NEW nonce for attempt 2; the old one is spent. */
      const nonce2 = await JOBS.issueWorkerNonce({ jobId, task: "premarket_manager", targetFunction: spec.targetFunction,
        attempt: 2, payloadHash: JOBS.payloadHash(payload), key });
      const claim2 = await JOBS.claimOnce({ jobId, task: "premarket_manager", targetFunction: spec.targetFunction, token: nonce2.token, payload, key });
      if (claim2.claimed !== true || claim2.claim.attempt !== 2 || claim2.claim.checkpoint.cursor !== 120) return false;
      if ((await JOBS.complete(claim2.claim, { rows: 304 })).status !== "complete") return false;
      if (fake.docs.get(`${fake.COL.jobs}/${jobId}`).status !== "complete") return false;
      /* A completed job is never claimed again, and a failure fails CLOSED. */
      const nonce3 = await JOBS.issueWorkerNonce({ jobId, task: "premarket_manager", targetFunction: spec.targetFunction, attempt: 3, payloadHash: JOBS.payloadHash(payload), key });
      const done = await JOBS.claimOnce({ jobId, task: "premarket_manager", targetFunction: spec.targetFunction, token: nonce3.token, payload, key });
      if (done.claimed !== false || done.reason !== "job_complete") return false;
      return true;
    } finally { /* scoped instance; nothing to unbind */ }
  }));

  cases.push(fixture("dispatch_launches_execute_first_then_events_continuations_manager_ingest_and_caps_heavy_work", () => {
    const JOBS = require("./_investorJobs");
    const mk = (task, extra = {}) => ({ jobId: `${task}_${extra.n || 1}`, task, status: "queued", dueAtMs: 0, priority: 100, attempts: 0, ...extra });
    const jobs = [mk("archive"), mk("ingest"), mk("premarket_manager", { status: "yielded_resumable" }), mk("focused_research"),
      mk("event_revision"), mk("execute"), mk("postclose"), mk("ingest", { n: 2 })];
    const plan = JOBS.dispatchPlan(jobs, { nowMs: 0, startedAtMs: 0, budgetMs: 20000, maxJobs: 4 });
    const chosen = plan.chosen.map((j) => j.task);
    /* execute → event → the yielded continuation → the first fresh heavy manager job */
    if (chosen.join(",") !== "execute,event_revision,premarket_manager,focused_research") throw new Error(chosen.join(","));
    if (!plan.deferred.some((d) => d.reason === "tick_full")) return false;
    /* One heavy job per category: the second ingest and the postclose pair wait. */
    const two = JOBS.dispatchPlan([mk("ingest"), mk("ingest", { n: 2 }), mk("archive", { priority: 500 }), mk("postclose", { priority: 400 })], { nowMs: 0, startedAtMs: 0, maxJobs: 4 });
    if (two.chosen.map((j) => j.task).join(",") !== "ingest,postclose") throw new Error(two.chosen.map((j) => j.task).join(","));
    if (!two.deferred.some((d) => d.reason === "one_heavy_per_category")) return false;
    /* The dispatch budget is respected. */
    const late = JOBS.dispatchPlan([mk("execute"), mk("event_revision")], { nowMs: 19000, startedAtMs: 0, budgetMs: 20000 });
    if (late.chosen.length !== 0 || !late.deferred.every((d) => d.reason === "dispatch_budget")) return false;
    /* Every task names its handler and engine; legacy tasks never dispatch under the manager engine. */
    for (const [task, spec] of Object.entries(JOBS.TASKS)) {
      if (!/-background$/.test(spec.targetFunction) || !["manager", "legacy"].includes(spec.engine)) throw new Error(task);
    }
    if (JOBS.MAX_JOBS_PER_TICK !== 4 || JOBS.DISPATCH_BUDGET_MS !== 20000) return false;
    return true;
  }));

  cases.push(fixture("kick_manager_engine_enqueues_the_overnight_pass_to_finish_at_the_freeze_and_the_meeting_after_it", () => {
    const K = require("./investorKick");
    const at = (min, extra = {}) => ({ date: "2026-09-04", tradingDay: true, open: false, phase: "premarket_early",
      minutesEt: min, regularOpenMinutesEt: 570, regularCloseMinutesEt: 960, isHalfDay: false, ...extra });
    const nowMs = Date.parse("2026-09-04T09:00:00Z");
    /* Legacy engine by default; the manager engine is an explicit control state. */
    if (K.engineMode({}) !== "legacy" || K.engineMode({ engineMode: "manager" }) !== "manager") return false;
    /* An unmeasured pass starts 3.2 h + margin before the 08:30 freeze; a measured pass moves it. */
    const unmeasured = K.ingestStartMinuteEt({});
    if (unmeasured.minuteEt !== 510 - 192 - 10 || unmeasured.previousEvening !== false) throw new Error(JSON.stringify(unmeasured));
    const measured = K.ingestStartMinuteEt({ lastIngestPass: { elapsedMs: 2 * 3600e3 } });
    if (measured.minuteEt !== 510 - 120 - 10) return false;
    /* A measured pass is capped at 8 h: longer than that cannot finish inside the window anyway. */
    const long = K.ingestStartMinuteEt({ lastIngestPass: { elapsedMs: 9 * 3600e3 } });
    if (long.minuteEt !== 510 - 480 - 10 || long.passMs !== 8 * 3600e3) return false;
    /* Before the start minute nothing is due; at it the pass is enqueued once; a started pass is not re-enqueued. */
    const ctrl = { accountId: "paper-1", engineMode: "manager" };
    if (K.decideManager(ctrl, at(300), nowMs).enqueue.some((e) => e.task === "ingest")) return false;
    const due = K.decideManager(ctrl, at(310), nowMs);
    const ingest = due.enqueue.find((e) => e.task === "ingest");
    if (!ingest || ingest.dedupeId !== "paper-1_2026-09-04" || ingest.payload.perSweep !== 8) throw new Error(JSON.stringify(due));
    if (K.decideManager({ ...ctrl, ingestPassState: { tradingDate: "2026-09-04" } }, at(310), nowMs).enqueue.some((e) => e.task === "ingest")) return false;
    /* No daytime rescan: the ingest window is closed at noon (D-11). */
    if (K.decideManager({ ...ctrl, lastIngestPass: { elapsedMs: 60000 } }, at(12 * 60, { open: true, phase: "regular" }), nowMs).enqueue.some((e) => e.task === "ingest")) return false;
    /* The Manager Meeting is enqueued at the freeze, once per date, never while paused. */
    const meeting = K.decideManager(ctrl, at(510), nowMs).enqueue.find((e) => e.task === "premarket_manager");
    if (!meeting || meeting.runId !== "premarket_manager_paper-1_2026-09-04" || meeting.payload.reason !== "SCHEDULED_PREMARKET") return false;
    if (K.decideManager(ctrl, at(509), nowMs).enqueue.some((e) => e.task === "premarket_manager")) return false;
    if (K.decideManager({ ...ctrl, lastManagerRunDate: "2026-09-04" }, at(520), nowMs).enqueue.some((e) => e.task === "premarket_manager")) return false;
    if (K.decideManager({ ...ctrl, managerState: "PAUSED" }, at(520), nowMs).enqueue.some((e) => e.task === "premarket_manager")) return false;
    /* Execution every minute in its window, never while paused for safety; nothing on a non-trading day. */
    const exec = K.decideManager(ctrl, at(600, { open: true, phase: "regular" }), nowMs).enqueue.find((e) => e.task === "execute");
    if (!exec || exec.priority !== 10) return false;
    if (K.decideManager({ ...ctrl, executorState: "PAUSED_SAFETY" }, at(600, { open: true, phase: "regular" }), nowMs).enqueue.some((e) => e.task === "execute")) return false;
    if (K.decideManager(ctrl, at(600, { tradingDay: false }), nowMs).enqueue.some((e) => e.task === "execute" || e.task === "premarket_manager")) return false;
    /* The ingest handler processes at most perSweep companies per segment and measures elapsed time. */
    const ING = require("./investorIngest-background");
    if (ING.PER_SEGMENT !== 8) return false;
    const roster = ING.passRoster({ universe: { tradeTier: [{ symbol: "A" }, { symbol: "B" }, { symbol: "C" }] },
      positions: [{ open: true, symbol: "Z" }], pendingOrders: [{ symbol: "B" }], rotation: 1 });
    if (roster.join(",") !== "B,C,Z,A") throw new Error(roster.join(","));
    const stats = ING.elapsedStats([{ ms: 100, ok: true }, { ms: 300, ok: false }, { ms: 200, ok: true }]);
    if (stats.count !== 3 || stats.maxMs !== 300 || stats.failed !== 1 || stats.p50Ms !== 200) return false;
    if (!/segmentBudgetRemainingMs/.test(sourceOf(ING.runIngestSegment)) || !/yieldSegment/.test(sourceOf(ING.handler))) return false;
    return true;
  }));

  /* ── Group 4 (commit 7): transparent universe snapshot and the 304-name
     coverage contract (§6.3). The snapshot is frozen per trading date, its
     hash is deterministic, and a coverage response must match it exactly. */
  cases.push(fixture("universe_snapshot_freezes_the_eligible_roster_and_coverage_must_match_it_exactly", () => {
    const U = require("./_investorUniverse");
    const t0 = 1_800_000_000_000;
    const snap = U.freezeEligibleSnapshot({ tradingDate: "2026-09-04", nowMs: t0 });
    if (snap.schemaVersion !== "universe-snapshot.v1") throw new Error("schemaVersion");
    if (snap.eligibleCount !== U.tradeTier.length || snap.eligibleCount !== 304) throw new Error(`eligibleCount ${snap.eligibleCount}`);
    if (snap.symbols.length !== 304 || new Set(snap.symbols).size !== 304) throw new Error("symbols not unique");
    const sorted = [...snap.symbols].sort();
    if (snap.symbols.some((s, i) => s !== sorted[i])) throw new Error("symbols not sorted");
    /* Determinism: the same roster and removals give the same hash regardless of clock. */
    const again = U.freezeEligibleSnapshot({ tradingDate: "2026-09-05", nowMs: t0 + 86_400_000 });
    if (again.universeHash !== snap.universeHash || !/^[0-9a-f]{64}$/.test(snap.universeHash)) throw new Error("hash not deterministic");
    /* Every excluded name carries an owner, a reason and an effective date; attractiveness is never a reason. */
    if (!snap.excluded.length) throw new Error("no exclusions declared");
    for (const x of snap.excluded) {
      if (!x.symbol || !x.reason || !x.owner || !x.effectiveDate) throw new Error(`exclusion incomplete ${JSON.stringify(x)}`);
      if (/attractiv|momentum|performance/i.test(String(x.reason))) throw new Error(`attractiveness used as eligibility ${x.symbol}`);
    }
    if (!snap.eligibility.rules.some((r) => /never an eligibility event/.test(r))) throw new Error("policy text");
    /* An objective removal shrinks the roster, changes the hash, and a held name stays managed off-roster. */
    const victim = snap.symbols[7];
    const removed = U.freezeEligibleSnapshot({ tradingDate: "2026-09-04", nowMs: t0,
      removed: [{ symbol: victim, reason: "acquired", owner: "operator", effectiveDate: "2026-09-01" }],
      positions: [{ symbol: victim, open: true }], pendingOrders: [{ symbol: "ZZZZ" }] });
    if (removed.eligibleCount !== 303 || removed.symbols.includes(victim)) throw new Error("removal not applied");
    if (removed.universeHash === snap.universeHash) throw new Error("hash ignores removals");
    if (!removed.managedOffRoster.some((m) => m.symbol === victim && m.reason === "held_position")) throw new Error("held name dropped");
    if (!removed.managedOffRoster.some((m) => m.symbol === "ZZZZ" && m.reason === "working_or_pending_order")) throw new Error("pending order dropped");
    if (!removed.excluded.some((x) => x.symbol === victim && x.reason === "acquired")) throw new Error("removal not recorded");
    /* Coverage: exact match passes; one missing, one duplicate, one unknown each fail with the offender named. */
    const full = snap.symbols.map((symbol) => ({ symbol, decision: "NO_BUY" }));
    const ok = U.coverageMatches(snap, full);
    if (ok.ok !== true || ok.completedCount !== 304 || ok.universeHash !== snap.universeHash) throw new Error("full coverage rejected");
    const missing = U.coverageMatches(snap, full.slice(1));
    if (missing.ok !== false || missing.missing.length !== 1 || missing.missing[0] !== snap.symbols[0]) throw new Error("missing not named");
    const dup = U.coverageMatches(snap, [...full, full[3]]);
    if (dup.ok !== false || dup.duplicates.length !== 1 || dup.duplicates[0] !== full[3].symbol) throw new Error("duplicate not named");
    const unknown = U.coverageMatches(snap, [...full, { symbol: "NOTAROSTERNAME" }]);
    if (unknown.ok !== false || unknown.unknown[0] !== "NOTAROSTERNAME") throw new Error("unknown not named");
    return true;
  }));

  /* ── Group 4 (commit 7): the numerical filing plane is point-in-time and
     exact (§5.1, §5.2, §6.5). A restated number is invisible before the
     restatement was filed; values are canonical integer strings; the
     provider registry states what is NOT available rather than guessing. */
  cases.push(fixture("sec_facts_are_point_in_time_exact_and_the_provider_registry_declares_what_is_unavailable", async () => {
    const F = require("./_investorFundamentals");
    const D = require("./_investorDataProviders");
    const f = await F.selfCheck();
    if (f.pass !== true) throw new Error(`fundamentals selfCheck: ${JSON.stringify(f.failures).slice(0, 300)}`);
    const d = await D.selfCheck();
    if (d.pass !== true) throw new Error(`providers selfCheck: ${JSON.stringify(d.failures).slice(0, 300)}`);
    /* Restatement: the 10-K/A value must not be visible before its own filed date. */
    const payload = { cik: 123456, facts: { "us-gaap": { Revenues: { units: { USD: [
      { start: "2025-01-01", end: "2025-12-31", val: 1000000000, accn: "0000123456-26-000001", fy: 2025, fp: "FY", form: "10-K", filed: "2026-02-20" },
      { start: "2025-01-01", end: "2025-12-31", val: 950000000, accn: "0000123456-26-000009", fy: 2025, fp: "FY", form: "10-K/A", filed: "2026-05-15" },
    ] } } } } };
    const { facts, rejected } = F.normalizeCompanyFacts(payload, { cik: 123456, retrievedAtMs: Date.UTC(2026, 5, 1) });
    if (rejected.length || facts.length !== 2) throw new Error(`normalise ${facts.length} facts, ${rejected.length} rejected`);
    for (const x of facts) if (!/^-?(0|[1-9][0-9]*)$/.test(String(x.valueScaled))) throw new Error("value is not a canonical integer string");
    const before = F.pointInTime(facts, { asOfMs: Date.UTC(2026, 3, 1) });
    if (before.length !== 1 || before[0].form !== "10-K" || before[0].valueScaled !== "100000000000") throw new Error(`pre-restatement view ${JSON.stringify(before[0])}`);
    const after = F.pointInTime(facts, { asOfMs: Date.UTC(2026, 6, 1) });
    if (after.length !== 1 || after[0].form !== "10-K/A" || after[0].valueScaled !== "95000000000") throw new Error("restatement not visible after filing");
    if (F.pointInTime(facts, { asOfMs: Date.UTC(2026, 0, 1) }).length !== 0) throw new Error("fact visible before it was filed");
    const sel = F.selectAsOf(facts, { asOfMs: Date.UTC(2026, 6, 1) });
    const orig = sel.lineage.find((l) => l.accession === "0000123456-26-000001");
    const amend = sel.lineage.find((l) => l.accession === "0000123456-26-000009");
    if (!orig || !amend || orig.supersededByFactId !== amend.factId || amend.supersedesFactId !== orig.factId) throw new Error("restatement lineage");
    let code = null;
    try { F.pointInTime(facts, {}); } catch (e) { code = e.code; }
    if (code !== "AS_OF_REQUIRED") throw new Error("as-of not required");
    /* Providers: consensus and transcripts are declared unavailable; SEC is entitled; unknown kinds fail closed. */
    const cov = D.coverageStatement();
    if (cov.consensus !== "no_consensus_vendor" || cov.transcripts !== "unavailable") throw new Error("coverage statement");
    if (D.entitlement("consensus").entitled !== false || D.entitlement("transcripts").entitled !== false) throw new Error("unavailable data treated as entitled");
    if (D.entitlement("filings").entitled !== true) throw new Error("SEC filings not entitled");
    if (D.entitlement("made_up_kind").entitled !== false || D.entitlement("made_up_kind").reason !== "unknown_kind") throw new Error("unknown kind not refused");
    const bad = await D.searchDecisionData({ symbol: "ABC", kinds: ["consensus"] });
    if (!bad.missing.some((m) => /asOfMs_required/.test(m.reason))) throw new Error("as-of not required by the search tool");
    return true;
  }));

  /* ── Group 4 (commit 8): guidance and earnings-date claims are versioned,
     point-in-time, and immutable by identity (§5.3, D-11 companion). */
  cases.push(fixture("guidance_claims_supersede_by_metric_and_period_and_earnings_dates_are_point_in_time", async () => {
    const E = require("./_investorEvidence");
    const fake = fakeAdmin();
    const t1 = Date.UTC(2026, 6, 1), t2 = Date.UTC(2026, 7, 1), t3 = Date.UTC(2026, 8, 1);
    const q1 = "we now expect full year 2026 revenue of $4.0 billion to $4.2 billion";
    const q2 = "we are raising our full year 2026 revenue outlook to $4.3 billion to $4.5 billion";
    const first = await E.recordClaims({ symbol: "abc", documentId: "doc1", documentVersionId: "v1", sourceId: "sec:8k", publishedAtMs: t1,
      extractedBy: "luna", admin: fake, claims: [
        { claimType: "GUIDANCE", metric: "revenue", periodLabel: "FY2026", lowValue: "4000000000", highValue: "4200000000", unit: "USD", text: "FY26 revenue guide", quote: q1 },
        { claimType: "EARNINGS_DATE", date: "2026-10-28", confirmed: true, text: "Q3 call", quote: "will report third quarter results on October 28, 2026" },
        { claimType: "GUIDANCE", metric: "revenue", periodLabel: "FY2026", quote: "too short" },
      ] });
    if (first.written.length !== 2 || first.skipped.length !== 1 || first.skipped[0].reason !== "invalid_claim") throw new Error(`first ${JSON.stringify(first)}`);
    if (first.superseded.length !== 0) throw new Error("nothing to supersede yet");
    /* Idempotent by claim identity: re-recording the same version writes nothing. */
    const again = await E.recordClaims({ symbol: "ABC", documentId: "doc1", documentVersionId: "v1", publishedAtMs: t1, admin: fake, claims: [
      { claimType: "GUIDANCE", metric: "revenue", periodLabel: "FY2026", lowValue: "4000000000", highValue: "4200000000", unit: "USD", quote: q1 } ] });
    if (again.written.length !== 0 || !again.skipped.some((s) => s.reason === "exists")) throw new Error("not idempotent");
    /* A later statement for the same metric and period supersedes; both remain readable. */
    const second = await E.recordClaims({ symbol: "ABC", documentId: "doc2", documentVersionId: "v2", publishedAtMs: t2, admin: fake, claims: [
      { claimType: "GUIDANCE", metric: "revenue", periodLabel: "FY2026", lowValue: "4300000000", highValue: "4500000000", unit: "USD", quote: q2 } ] });
    if (second.written.length !== 1 || second.superseded.length !== 1 || second.superseded[0].claimId !== first.written[0]) throw new Error("supersession");
    const priorDoc = fake.docs.get(`${fake.COL.claims}/${first.written[0]}`);
    const newDoc = fake.docs.get(`${fake.COL.claims}/${second.written[0]}`);
    if (priorDoc.supersededBy !== second.written[0] || newDoc.supersedes !== first.written[0]) throw new Error("links not bidirectional");
    if (priorDoc.lowValue !== "4000000000" || newDoc.highValue !== "4500000000") throw new Error("numeric contract");
    /* Point-in-time reads: as of t1+1 only the first guide is known; as of t3 the raised guide wins. */
    const early = await E.claimsForCompany("ABC", { asOfMs: t1 + 1, admin: fake });
    if (early.length !== 2 || early.some((c) => c.documentVersionId === "v2")) throw new Error("future claim leaked");
    const g1 = E.latestGuidanceFrom(early, { asOfMs: t1 + 1 });
    if (g1.length !== 1 || g1[0].highValue !== "4200000000") throw new Error("early guide");
    const late = await E.claimsForCompany("ABC", { asOfMs: t3, admin: fake });
    const g2 = E.latestGuidanceFrom(late, { asOfMs: t3 });
    if (g2.length !== 1 || g2[0].lowValue !== "4300000000" || g2[0].supersedes !== first.written[0]) throw new Error("late guide");
    const before = await E.claimsForCompany("ABC", { asOfMs: t1 - 1, admin: fake });
    if (before.length !== 0) throw new Error("claim known before publication");
    /* Earnings date: known only from publication, and only while it is still ahead. */
    const ed = E.nextEarningsFrom(late, { asOfMs: t3 });
    if (!ed || ed.date !== "2026-10-28" || ed.confirmed !== true || !ed.claimId) throw new Error("earnings date");
    if (E.nextEarningsFrom(late, { asOfMs: Date.UTC(2026, 10, 1) }) !== null) throw new Error("past earnings date returned");
    if (E.nextEarningsFrom(late, { asOfMs: t1 - 1 }) !== null) throw new Error("earnings date known before publication");
    return true;
  }));

  /* Claim verification: span identity is exact, the verifier is independent,
     an unavailable verifier is INSUFFICIENT (I-1), a CONTRADICTED material
     premise forces abstention, and a stored verdict cannot be altered. */
  cases.push(fixture("claim_verifier_fails_closed_on_span_identity_and_a_contradicted_premise_forces_abstain", async () => {
    const V = require("./_investorClaimVerifier");
    const fake = fakeAdmin();
    const text = "Net revenue for the quarter was $1.21 billion, up 14% year over year. We expect full year revenue of $4.0 billion to $4.2 billion.";
    const version = { versionId: "v1", contentHash: "h1", canonicalText: text };
    const good = V.spanIdentity({ premise: { quote: "up 14% year over year" }, version });
    if (good.ok !== true || !good.spanId || good.versionId !== "v1") throw new Error("verbatim quote rejected");
    if (V.spanIdentity({ premise: { quote: "up 41% year over year" }, version }).reason !== "quote_not_found_verbatim") throw new Error("paraphrase accepted");
    if (V.spanIdentity({ premise: { quote: "up 14%" }, version }).reason !== "quote_too_short") throw new Error("short quote accepted");
    if (V.spanIdentity({ premise: { quote: "up 14% year over year" }, version: null }).reason !== "document_version_missing") throw new Error("missing version accepted");
    const claims = {
      c1: { claimId: "c1", documentVersionId: "v1", text: "Revenue +14% y/y", quote: "up 14% year over year" },
      c2: { claimId: "c2", documentVersionId: "v1", text: "FY revenue guide", quote: "full year revenue of $4.0 billion to $4.2 billion" },
      c3: { claimId: "c3", documentVersionId: "v1", text: "Margins expanded", quote: "gross margin expanded 300 basis points" },
    };
    const proposal = { symbol: "ABC", decision: "BUY", thesis: { evidenceFor: ["c1", "c2"], evidenceAgainst: ["c3"] }, sourceManifest: [] };
    const spanReader = async () => ({ v1: version });
    /* Verifier unavailable: every premise is INSUFFICIENT and the gate blocks (never SUPPORTED by default). */
    const down = await V.verifyAndPersistBatch({ proposals: [proposal], claimsById: claims, spanReader, admin: fake,
      verifier: async () => ({ available: false, reason: "gateway_unavailable" }) });
    const d = down.byProposal.ABC;
    if (down.allSupported !== false || d.blocking !== true || d.forceAbstain !== false || d.reason !== "UNSUPPORTED_MATERIAL_PREMISE") throw new Error(`down ${JSON.stringify(d)}`);
    if (fake.docs.size !== 3) throw new Error("verdicts not persisted");
    /* The stored verdict for c1 is immutable: a later SUPPORTED verdict does not overwrite it. */
    const up = await V.verifyAndPersistBatch({ proposals: [proposal], claimsById: claims, spanReader, admin: fake,
      verifier: async ({ premises }) => ({ available: true, model: "luna", output: { verdicts: premises.map((p) => ({ claimId: p.claimId, verdict: "SUPPORTED" })) } }) });
    const u = up.byProposal.ABC;
    /* c3's quote is not in the document, so it stays INSUFFICIENT whatever the verifier says. */
    if (u.insufficientClaimIds.length !== 1 || u.insufficientClaimIds[0] !== "c3" || u.supported !== 2) throw new Error(`span gate ${JSON.stringify(u)}`);
    const c1Stored = [...fake.docs.values()].find((v) => v.claimId === "c1");
    if (c1Stored.verdict !== "INSUFFICIENT") throw new Error("stored verdict overwritten");
    const tx = { get: (ref) => ref.get() };
    let threw = null;
    try { await V.assertImmutableClaimVerdicts(tx, up, { admin: fake }); } catch (e) { threw = e.code; }
    if (threw !== "CLAIM_VERDICT_ALTERED") throw new Error(`altered verdict passed activation: ${threw}`);
    /* A fresh store with a contradicted material premise forces abstention. */
    const fake2 = fakeAdmin();
    const contra = await V.verifyAndPersistBatch({ proposals: [{ ...proposal, thesis: { evidenceFor: ["c1", "c2"], evidenceAgainst: [] } }], claimsById: claims, spanReader, admin: fake2,
      verifier: async ({ premises }) => ({ available: true, output: { verdicts: premises.map((p) => ({ claimId: p.claimId, verdict: p.claimId === "c2" ? "CONTRADICTED" : "SUPPORTED" })) } }) });
    const c = contra.byProposal.ABC;
    if (c.forceAbstain !== true || c.reason !== "CONTRADICTED_MATERIAL_PREMISE" || contra.forceAbstainSymbols[0] !== "ABC") throw new Error("contradiction ignored");
    let code = null;
    try { await V.assertImmutableClaimVerdicts(tx, contra, { admin: fake2 }); } catch (e) { code = e.code; }
    if (code !== "CLAIM_NOT_SUPPORTED") throw new Error(`contradicted verdict passed activation: ${code}`);
    /* All supported on a fresh store passes the activation assertion. */
    const fake3 = fakeAdmin();
    const clean = await V.verifyAndPersistBatch({ proposals: [{ ...proposal, thesis: { evidenceFor: ["c1", "c2"], evidenceAgainst: [] } }], claimsById: claims, spanReader, admin: fake3,
      verifier: async ({ premises }) => ({ available: true, output: { verdicts: premises.map((p) => ({ claimId: p.claimId, verdict: "SUPPORTED" })) } }) });
    if (clean.allSupported !== true) throw new Error("clean set blocked");
    const checked = await V.assertImmutableClaimVerdicts(tx, clean, { admin: fake3 });
    if (checked.checked !== 2) throw new Error("activation check count");
    return true;
  }));

  /* Event routing (Appendix B): classes are objective and high-recall, an
     event is recorded once, only a high-impact class pauses an unfilled
     entry, and routine deltas wait for the meeting unless a mandate is
     active. Code never decides materiality. */
  cases.push(fixture("event_router_classifies_objectively_records_once_and_pauses_only_high_impact_unfilled_entries", async () => {
    const R = require("./_investorEventRouter");
    const earnings = R.classifyDocument({ form: "8-K", title: "Form 8-K", canonicalText: "Item 2.02 Results of Operations and Financial Condition. Item 9.01 Exhibits." });
    if (earnings.eventClass !== "EARNINGS" || earnings.safetyClass !== "high_impact" || !earnings.items.includes("2.02")) throw new Error(`earnings ${JSON.stringify(earnings)}`);
    const reg = R.classifyDocument({ form: "8-K", canonicalText: "Item 7.01 Regulation FD Disclosure. Item 9.01 Financial Statements and Exhibits." });
    if (reg.eventClass !== "ROUTINE_NEWS" || reg.safetyClass !== "routine" || reg.verified !== true) throw new Error(`fd ${JSON.stringify(reg)}`);
    const tenq = R.classifyDocument({ form: "10-Q", canonicalText: "Quarterly report. Revenue increased." });
    if (tenq.eventClass !== "PERIODIC_FILING" || tenq.safetyClass !== "routine") throw new Error("10-Q");
    const recall = R.classifyDocument({ sourceClass: "regulator_primary", title: "Company recalls 40,000 units" });
    if (recall.eventClass !== "RECALL" || recall.safetyClass !== "high_impact") throw new Error("recall");
    const lead = R.classifyDocument({ sourceClass: "discovery_index", title: "Analyst chatter about a possible deal" });
    if (lead.eventClass !== "DISCOVERY_LEAD" || lead.safetyClass !== "attention_only" || lead.verified !== false) throw new Error("discovery lead treated as verified");
    if (!R.EVENT_CLASSES.includes(earnings.eventClass) || R.HIGH_IMPACT_CLASSES.has("ROUTINE_NEWS")) throw new Error("class vocabulary");
    /* Dedupe is by canonical hash: a syndicated copy of the same accession is the same event. */
    const a = R.canonicalEventHash({ symbol: "abc", doc: { sourceId: "sec", accession: "0001-26-1" }, eventClass: "EARNINGS" });
    const b = R.canonicalEventHash({ symbol: "ABC", doc: { sourceId: "sec", accession: "0001-26-1", title: "different title" }, eventClass: "EARNINGS" });
    if (a !== b) throw new Error("hash unstable");
    /* Routing in memory with the fixture's own effect recorders. */
    const fake = fakeAdmin();
    const calls = [];
    const deps = { admin: fake,
      dossier: { recordRoutineDelta: async (m) => calls.push(["delta", m.symbol, m.eventClass]) },
      mandate: { hasActiveMandate: async (s) => s === "MND", pauseUnfilledEntry: async (s, e) => calls.push(["pause", s, e]) },
      jobs: { enqueueOnce: async (j) => { calls.push(["enqueue", j.task, j.dedupeId]); return { enqueued: true }; },
              prioritizeNextManagerPacket: async (p) => calls.push(["prioritize", p.symbol]) } };
    const hi = await R.routeEvidenceEvent({ symbol: "ABC", document: { form: "8-K", sourceId: "sec", accession: "A1", firstSeenAtMs: 1_800_000_000_000,
      canonicalText: "Item 4.01 Changes in Registrant's Certifying Accountant." } }, { deps });
    if (hi.route.action !== "high_impact" || hi.eventClass !== "AUDITOR_CHANGE") throw new Error(`hi ${JSON.stringify(hi.route)}`);
    if (!hi.effects.includes("record_delta") || !hi.effects.includes("pause_unfilled_entry") || !hi.effects.includes("enqueued_event_revision")) throw new Error(`hi effects ${hi.effects}`);
    if (!calls.some((c) => c[0] === "pause" && c[1] === "ABC") || !calls.some((c) => c[0] === "enqueue" && c[1] === "event_revision")) throw new Error("high-impact effects missing");
    const dupe = await R.routeEvidenceEvent({ symbol: "ABC", document: { form: "8-K", sourceId: "sec", accession: "A1", canonicalText: "Item 4.01 syndicated copy." } }, { deps });
    if (dupe.duplicate !== true || dupe.route.action !== "ignore" || dupe.effects) throw new Error("duplicate re-routed");
    const stored = [...fake.docs.values()].filter((d) => d.schemaVersion === "evidence-delta.v1");
    if (stored.length !== 1 || stored[0].managerMateriality !== "pending") throw new Error("delta not recorded once with materiality pending");
    /* Routine delta on a name without a mandate: recorded, nothing paused, nothing enqueued. */
    calls.length = 0;
    const routine = await R.routeEvidenceEvent({ symbol: "XYZ", document: { form: "8-K", sourceId: "sec", accession: "B1", canonicalText: "Item 7.01 Regulation FD." } }, { deps });
    if (routine.route.action !== "routine" || calls.some((c) => c[0] !== "delta")) throw new Error("routine delta triggered a protective action");
    /* Routine delta on a name with an active mandate: prioritised in the next packet, still no pause. */
    calls.length = 0;
    const withMandate = await R.routeEvidenceEvent({ symbol: "MND", document: { form: "10-Q", sourceId: "sec", accession: "C1", canonicalText: "Quarterly report." } }, { deps });
    if (withMandate.route.action !== "routine_with_mandate" || !calls.some((c) => c[0] === "prioritize" && c[1] === "MND") || calls.some((c) => c[0] === "pause")) throw new Error("mandate routing");
    /* A discovery lead is never a verified delta and never routes. */
    const disc = await R.routeEvidenceEvent({ symbol: "ABC", document: { sourceClass: "discovery_index", sourceId: "idx", link: "https://x/y", title: "rumour" } }, { deps });
    if (disc.isNewVerifiedDelta !== false || disc.route.action !== "ignore") throw new Error("discovery lead routed");
    /* The false-negative review sample is deterministic. */
    const sample = R.sampleForReview([{ canonicalEventHash: "000000abc" }, { canonicalEventHash: "ffffffabc" }]);
    if (sample.length !== 1 || sample[0].canonicalEventHash !== "000000abc") throw new Error("sample");
    return true;
  }));

  /* ── Group 4 (commit 9): dossiers hash the FACTS layer only, deltas are
     typed differences, the §5.4 card has the blueprint shape with a sector
     block derived from filings, and "fresh" is a matrix, not a boolean. */
  cases.push(fixture("dossier_versions_hash_facts_only_and_deltas_are_typed_not_opinions", async () => {
    const D = require("./_investorDossier");
    const F = require("./_investorFundamentals");
    const asOf = Date.UTC(2026, 8, 1);
    const U = (start, end, val, accn, fy, fp, form, filed) => ({ start, end, val, accn, fy, fp, form, filed });
    const I = (end, val, accn, fy, fp, form, filed) => ({ end, val, accn, fy, fp, form, filed });
    const K25 = ["0000123456-26-000001", 2025, "FY", "10-K", "2026-02-20"];
    const payload = { cik: 123456, facts: { "us-gaap": {
      Revenues: { units: { USD: [U("2024-01-01", "2024-12-31", 3600000000, "0000123456-25-000001", 2024, "FY", "10-K", "2025-02-21"), U("2025-01-01", "2025-12-31", 4000000000, ...K25)] } },
      GrossProfit: { units: { USD: [U("2025-01-01", "2025-12-31", 2800000000, ...K25)] } },
      ResearchAndDevelopmentExpense: { units: { USD: [U("2025-01-01", "2025-12-31", 900000000, ...K25)] } },
      ContractWithCustomerLiabilityCurrent: { units: { USD: [I("2025-12-31", 1200000000, ...K25)] } },
      InventoryNet: { units: { USD: [I("2025-12-31", 500000000, ...K25)] } },
      CostOfRevenue: { units: { USD: [U("2025-01-01", "2025-12-31", 1200000000, ...K25)] } },
    } } };
    const { facts, rejected } = F.normalizeCompanyFacts(payload, { cik: 123456, retrievedAtMs: asOf });
    if (rejected.length) throw new Error(`normalise rejected ${JSON.stringify(rejected).slice(0, 200)}`);
    const fundamentals = F.deriveMetrics(facts, { asOfMs: asOf });
    const identity = { name: "ABC Corp", sector: "sw", cik: "0000123456" };
    const claims = [{ claimId: "claim_g1", claimType: "GUIDANCE", metric: "revenue", periodLabel: "FY2026", lowValue: "4300000000", highValue: "4500000000",
      unit: "USD", publishedAtMs: Date.UTC(2026, 6, 29), documentVersionId: "v9", quote: "we expect FY2026 revenue of $4.3 billion to $4.5 billion" },
      { claimId: "claim_e1", claimType: "EARNINGS_DATE", date: "2026-10-28", confirmed: true, publishedAtMs: Date.UTC(2026, 7, 20), documentVersionId: "v10", quote: "will report on October 28, 2026" }];
    const documents = [{ documentId: "ABC_sec.latest_1", versionId: "ABC_sec.latest_1_aaaa", canonicalContentSha256: "h1", sourceId: "sec.latest", form: "10-K", firstSeenAtMs: Date.UTC(2026, 1, 20) }];
    const v1 = D.composeVersion({ symbol: "abc", identity, asOfMs: asOf, facts, fundamentals, claims, documents });
    if (v1.schemaVersion !== "dossier-version.v1" || v1.symbol !== "ABC" || v1.version !== 1) throw new Error("version envelope");
    /* the sector block comes from the filings, with fact ids, and is the software block */
    if (v1.sectorBlock.block !== "software") throw new Error(`block ${v1.sectorBlock.block}`);
    if (v1.sectorBlock.metrics.grossProfitTtmMinor !== "280000000000" || v1.sectorBlock.metrics.deferredRevenueMinor !== "120000000000") throw new Error(`sector metrics ${JSON.stringify(v1.sectorBlock.metrics)}`);
    if (!Array.isArray(v1.sectorBlock.factIds.grossProfitTtmMinor_facts) || !v1.sectorBlock.factIds.grossProfitTtmMinor_facts.length) throw new Error("sector metric without fact ids");
    if (v1.sectorBlock.metrics.salesMarketingTtmMinor !== null || !v1.sectorBlock.notes.some((n) => /salesMarketingTtmMinor/.test(n))) throw new Error("missing metric not explained");
    if (v1.fundamentals.revenueGrowthBps !== "1111") throw new Error(`revenue growth ${v1.fundamentals.revenueGrowthBps}`);
    if (v1.guidance.length !== 1 || v1.guidance[0].claimId !== "claim_g1" || v1.nextEarnings.date !== "2026-10-28") throw new Error("claims not carried by id");
    /* no interpretation lives in the version: only pointers */
    for (const k of ["thesis", "decision", "memo", "score", "forecast"]) if (k in v1) throw new Error(`opinion field ${k} in facts layer`);
    if (!("pointers" in v1) || v1.pointers.researchMemoId !== null) throw new Error("pointer layer");
    /* the hash ignores time and price: same facts at a later as-of → same hash */
    const v1b = D.composeVersion({ symbol: "ABC", identity, asOfMs: asOf + 86_400_000, facts, fundamentals, claims, documents });
    if (v1b.contentHash !== v1.contentHash) throw new Error("hash depends on clock");
    /* a new document version and a superseding guide → typed delta */
    const claims2 = [...claims, { claimId: "claim_g2", claimType: "GUIDANCE", metric: "revenue", periodLabel: "FY2026", lowValue: "4400000000", highValue: "4600000000",
      unit: "USD", publishedAtMs: Date.UTC(2026, 7, 30), documentVersionId: "v11", supersedes: "claim_g1", quote: "raising FY2026 revenue outlook to $4.4 billion to $4.6 billion" }];
    const documents2 = [{ ...documents[0], versionId: "ABC_sec.latest_1_bbbb", canonicalContentSha256: "h2" }, { documentId: "ABC_sec.latest_2", versionId: "ABC_sec.latest_2_cccc", canonicalContentSha256: "h3", sourceId: "sec.latest", form: "8-K", firstSeenAtMs: Date.UTC(2026, 7, 30) }];
    const v2 = D.composeVersion({ symbol: "ABC", identity, asOfMs: asOf, facts, fundamentals, claims: claims2, documents: documents2, priorVersion: 1 });
    if (v2.contentHash === v1.contentHash) throw new Error("hash ignores new evidence");
    const delta = D.buildDelta(v1, v2);
    if (delta.schemaVersion !== "evidence-delta.v1" || delta.changed !== true || delta.safetyClass !== "routine") throw new Error(`delta ${JSON.stringify(delta.counts)} ${delta.safetyClass}`);
    if (!delta.added.some((x) => x.kind === "claim" && x.id === "claim_g2") || !delta.added.some((x) => x.kind === "document" && x.id === "ABC_sec.latest_2")) throw new Error("added not typed");
    if (!delta.revised.some((x) => x.kind === "document" && x.id === "ABC_sec.latest_1") || !delta.revised.some((x) => x.kind === "guidance" && x.from === "claim_g1")) throw new Error("revised not typed");
    if (delta.contradicted.length !== 0) throw new Error("supersession mis-typed as contradiction");
    if (D.buildDelta(v1, v1b).changed !== false) throw new Error("no-op delta reports change");
    /* two live guides for the same metric and period that disagree → integrity event */
    const conflict = D.composeVersion({ symbol: "ABC", identity, asOfMs: asOf, facts, fundamentals, documents, claims: [...claims,
      { claimId: "claim_g3", claimType: "GUIDANCE", metric: "revenue", periodLabel: "FY2026", lowValue: "3000000000", highValue: "3100000000", unit: "USD", publishedAtMs: Date.UTC(2026, 6, 29), documentVersionId: "v12", quote: "expects FY2026 revenue of $3.0 billion to $3.1 billion" }] });
    /* latestGuidanceFrom keeps one live guide per metric/period, so the version itself cannot carry two; the delta reports the contradiction only when both are live */
    const forced = { ...conflict, guidance: [...conflict.guidance, { claimId: "claim_g3", metric: "revenue", periodLabel: "FY2026", lowValue: "3000000000", highValue: "3100000000" }] };
    if (D.buildDelta(v1, forced).safetyClass !== "integrity") throw new Error("conflicting live guides not an integrity event");
    /* persistence appends only on hash change; pointer stays small and learns of deltas */
    const fake = fakeAdmin();
    const w1 = await D.persistVersion(v1, { admin: fake, sourceAtMs: asOf });
    const w1b = await D.persistVersion(v1b, { admin: fake });
    const w2 = await D.persistVersion(v2, { admin: fake });
    if (w1.appended !== true || w1b.appended !== false || w2.appended !== true || w2.version !== 2 || w2.previousVersionId !== w1.versionId) throw new Error(`persist ${JSON.stringify([w1, w1b, w2])}`);
    const versions = [...fake.docs.keys()].filter((k) => k.startsWith(`${fake.COL.dossierVersions}/`));
    if (versions.length !== 2) throw new Error(`versions stored ${versions.length}`);
    const pointer = await D.current("ABC", { admin: fake });
    if (pointer.currentVersionId !== w2.versionId || pointer.version !== 2 || pointer.unchangedRuns !== 0 || pointer.contentHash !== v2.contentHash) throw new Error("pointer");
    if (JSON.stringify(pointer).length > 4000) throw new Error("pointer is not small");
    const back = await D.readVersion(w2.versionId, { admin: fake });
    if (!back || back.contentHash !== v2.contentHash || back.sectorBlock.metrics.grossProfitTtmMinor !== "280000000000") throw new Error("version round trip");
    await D.recordRoutineDelta({ symbol: "ABC", deltaId: "delta_ABC_1", eventClass: "ROUTINE_NEWS", safetyClass: "routine", firstSeenAt: asOf }, { admin: fake });
    const p2 = await D.current("ABC", { admin: fake });
    if (p2.pendingDeltaCount !== 1 || p2.recentDeltaIds[0] !== "delta_ABC_1" || p2.lastDeltaEventClass !== "ROUTINE_NEWS") throw new Error("routine delta not recorded on pointer");
    /* freshness is a matrix: each dimension carries its own state */
    const fm = D.freshnessMatrix({ asOfMs: asOf - 3600e3, lastSourceAtMs: asOf - 40 * 3600e3, lastManagerReviewAtMs: null, lastMandateAtMs: asOf - 86400e3, lastMarketMarkAtMs: asOf - 600e3 }, { nowMs: asOf });
    if (fm.dossier.fresh !== true || fm.source.fresh !== false || fm.source.state !== "stale" || fm.managerReview.state !== "never" || fm.mandate.fresh !== true || fm.marketMark.fresh !== true) throw new Error(`freshness ${JSON.stringify(fm)}`);
    if (typeof fm.fresh !== "undefined") throw new Error("global fresh boolean present");
    return true;
  }));

  cases.push(fixture("compact_card_has_the_blueprint_shape_declares_missing_data_and_carries_no_score", async () => {
    const D = require("./_investorDossier");
    const asOf = Date.UTC(2026, 8, 1);
    const bars = [];
    let c = 40;
    for (let i = 300; i >= 1; i -= 1) { const d = new Date(asOf - i * 864e5).toISOString().slice(0, 10); c *= 1 + ((i % 7) - 3) / 400; bars.push({ date: d, c: Number(c.toFixed(4)) }); }
    const price = D.returnsBps(bars, { asOfMs: asOf });
    if (price.ok !== true || !/^[1-9][0-9]*$/.test(price.closeMicros) || price.asOfDate >= "2026-09-01") throw new Error(`price ${JSON.stringify(price)}`);
    for (const k of ["1d", "5d", "3m", "1y"]) if (!/^-?(0|[1-9][0-9]*)$/.test(String(price.bps[k]))) throw new Error(`return ${k} not canonical`);
    /* the cutoff day itself is never included: bars dated on or after the cutoff are ignored */
    const leak = D.returnsBps([...bars, { date: "2026-09-01", c: 999 }, { date: "2026-09-02", c: 999 }], { asOfMs: asOf });
    if (leak.closeMicros !== price.closeMicros) throw new Error("future bar leaked into the card");
    const identity = { name: "ABC Corp", sector: "energy", cik: "0000123456" };
    const version = D.composeVersion({ symbol: "ABC", identity, asOfMs: asOf, facts: [], fundamentals: { revenueGrowthBps: "920", fcfMarginBps: "1210", netDebtEbitdaMilli: "1400", sharesOutstanding: "100000000", epsTrailingMicros: "2500000", revenueTtmMinor: "400000000000", netIncomeTtmMinor: "25000000000", basis: { factIds: {}, periods: {}, notes: [] } },
      claims: [{ claimId: "claim_g1", claimType: "GUIDANCE", metric: "revenue", periodLabel: "FY2026", lowValue: "4100000000", highValue: "4300000000", unit: "USD", publishedAtMs: Date.UTC(2026, 6, 29), documentVersionId: "docv_1", quote: "we expect FY2026 revenue of $4.1 billion to $4.3 billion" }], documents: [] });
    const card = D.cardFromVersion(version, { cutoffMs: asOf, price, sectorPrice: price, marketPrice: price,
      changes: [{ deltaId: "delta_1", eventClass: "NEW_8K", form: "8-K", safetyClass: "high_impact", managerMateriality: "pending" }],
      standingView: { status: "WATCH", researchVersion: 3, ageTradingDays: 7 }, portfolio: { held: false, activeMandate: false } });
    const required = ["symbol", "identity", "asOf", "price", "relative", "fundamentals", "valuation", "guidance", "expectations", "nextEarnings", "changes", "standingView", "portfolio", "dataQuality"];
    for (const k of required) if (!(k in card)) throw new Error(`card missing ${k}`);
    for (const k of Object.keys(card)) if (/score|rank|signal|conviction/i.test(k)) throw new Error(`composite field ${k} on card`);
    if (card.price.currency !== "USD" || card.price.closeMicros !== price.closeMicros || card.price.returnBps["5d"] !== price.bps["5d"]) throw new Error("price block");
    if (card.relative.sector5dBps !== price.bps["5d"] || card.relative.drawdownBps !== price.drawdownBps) throw new Error("relative block");
    if (card.fundamentals.revenueGrowthBps !== "920" || card.fundamentals.fcfMarginBps !== "1210" || card.fundamentals.netDebtEbitdaMilli !== "1400") throw new Error("fundamentals block");
    if (card.expectations.coverage !== "no_consensus_vendor" || card.expectations.revision30dBps !== null) throw new Error("consensus mislabelled");
    if (card.guidance.lowMicros !== "4100000000000000" || card.guidance.claimId !== "claim_g1" || card.guidance.documentVersionId !== "docv_1") throw new Error(`guidance block ${JSON.stringify(card.guidance)}`);
    if (!card.valuation.methodHints.includes("ev_ebitda") || card.valuation.trailingMultipleMilli === null || card.valuation.forwardMultipleMilli === null || card.valuation.forwardBasis !== "price_to_guided_revenue_midpoint") throw new Error(`valuation ${JSON.stringify(card.valuation)}`);
    if (card.changes[0].safetyClass !== "high_impact" || card.changes[0].managerMateriality !== "pending") throw new Error("changes block");
    if (card.standingView.status !== "WATCH" || card.standingView.researchVersion !== 3 || card.portfolio.held !== false) throw new Error("standing view / portfolio");
    if (card.nextEarnings !== null || card.dataQuality.complete !== false || !card.dataQuality.missing.includes("nextEarnings.confirmed")) throw new Error("missing data not declared");
    if (card.sectorBlock.block !== "resources") throw new Error("sector block on card");
    if (card.tokenEstimate > D.MAX_TOKENS_PER_CARD + 150) throw new Error(`card too large: ~${card.tokenEstimate} tokens`);
    /* with no guidance the forward multiple stays null and is never inferred */
    const bare = D.cardFromVersion(D.composeVersion({ symbol: "ABC", identity, asOfMs: asOf, facts: [], fundamentals: null, claims: [], documents: [] }), { cutoffMs: asOf, price: null });
    if (bare.guidance !== null || bare.valuation.forwardMultipleMilli !== null || bare.valuation.trailingMultipleMilli !== null || bare.price !== null) throw new Error("absent data was invented");
    if (!bare.dataQuality.missing.includes("guidance") || !bare.dataQuality.missing.includes("price") || !bare.dataQuality.missing.includes("fundamentals")) throw new Error("absence not declared");
    return true;
  }));

  cases.push(fixture("managed_workset_is_eligible_union_held_union_pending_and_forbids_scaling_in", () => {
    const W = require("./_investorWorkset");
    const roster = { symbols: ["AAA", "BBB", "CCC"], universeVersion: "v6", universeHash: "h".repeat(64) };
    const ws = W.buildManaged({ roster,
      positions: [{ symbol: "BBB", qty: 10, open: true }, { symbol: "OLD", qty: 5, open: true }, { symbol: "FLAT", qty: 0, open: false }],
      pending: [{ symbol: "CCC", orderId: "o1", status: "working", side: "buy" }, { symbol: "GONE", orderId: "o2", status: "pending_cancel", side: "sell" }] });
    if (ws.symbols.join(",") !== "AAA,BBB,CCC,OLD,GONE") throw new Error(`workset order ${ws.symbols}`);
    if (ws.eligibleCount !== 3 || ws.counts.held !== 2 || ws.counts.offRoster !== 2 || ws.counts.pending !== 2) throw new Error(`counts ${JSON.stringify(ws.counts)}`);
    if (ws.managedPositionSymbols.join(",") !== "BBB,OLD") throw new Error("managed positions");
    if (!ws.managedOffRoster.some((m) => m.symbol === "OLD" && m.reason === "held_off_roster") || !ws.managedOffRoster.some((m) => m.symbol === "GONE" && m.reason === "pending_off_roster")) throw new Error("off-roster reporting");
    if (ws.entryCandidateSymbols.join(",") !== "AAA") throw new Error(`entry candidates ${ws.entryCandidateSymbols}`);
    const row = (s) => ws.rows.find((r) => r.symbol === s);
    if (row("BBB").scalingInForbidden !== true || row("BBB").entryEligible !== false) throw new Error("held name still entry-eligible");
    if (W.actionAllowedForRow(row("BBB"), "BUY").reason !== "SCALING_IN_FORBIDDEN") throw new Error("scaling in allowed");
    if (W.actionAllowedForRow(row("OLD"), "BUY").reason !== "SCALING_IN_FORBIDDEN") throw new Error("off-roster held BUY");
    if (W.actionAllowedForRow(row("OLD"), "SELL").ok !== true || W.actionAllowedForRow(row("OLD"), "REDUCE").ok !== true) throw new Error("off-roster holding cannot be managed");
    if (W.actionAllowedForRow(row("GONE"), "BUY").reason !== "OFF_ROSTER_ENTRY_FORBIDDEN") throw new Error("off-roster entry allowed");
    if (W.actionAllowedForRow(row("AAA"), "HOLD").reason !== "NOT_HELD" || W.actionAllowedForRow(row("AAA"), "BUY").ok !== true) throw new Error("flat eligible name");
    if (W.actionAllowedForRow(null, "BUY").ok !== false) throw new Error("unknown symbol");
    /* fair queue: never-built first, then held, then stalest; rotation never reorders staleness bands */
    const now = Date.UTC(2026, 8, 1);
    const pointers = { AAA: { asOfMs: now - 10 * 3600e3 }, BBB: { asOfMs: now - 2 * 3600e3 }, CCC: { asOfMs: now - 50 * 3600e3 } };
    const q = W.fairDossierQueue({ workset: ws, pointers, nowMs: now });
    if (q[0] !== "OLD" && q[1] !== "OLD") throw new Error(`never-built held name not first: ${q}`);
    if (q.indexOf("CCC") > q.indexOf("AAA")) throw new Error("stalest not ahead of fresher");
    const q2 = W.fairDossierQueue({ workset: ws, pointers, nowMs: now, rotation: 3 });
    if (q2.length !== q.length || new Set(q2).size !== q.length) throw new Error("rotation lost symbols");
    return true;
  }));

  /* ── Group 5 (commit 10): the model gateway. One request path, fixed role
     routing, strict schemas, integer-cent cost from published rates, a
     ModelRequest record per call, bounded allowlisted tools, verbatim
     extraction checks, and a failure shape that can never be read as a
     decision (§3, §9, §12.2; invariant I-1). */
  function scriptedTransport(script) {
    const calls = [];
    let i = 0;
    const transport = async (url, opts) => {
      const body = opts && opts.body ? JSON.parse(opts.body) : null;
      calls.push({ url, method: opts && opts.method, body });
      const step = script[i] || script[script.length - 1];
      i += 1;
      const reply = typeof step === "function" ? step({ url, body, n: i }) : step;
      return { ok: reply.status ? reply.status < 400 : true, status: reply.status || 200, json: async () => reply.data };
    };
    return { transport, calls };
  }
  const HEX64 = "a".repeat(64);
  function completed(model, outputObject, usage = {}, extra = {}) {
    return { data: { id: `resp_${Math.random().toString(36).slice(2, 10)}`, status: "completed", model,
      output: [{ type: "message", content: [{ type: "output_text", text: JSON.stringify(outputObject) }] }],
      usage: { input_tokens: 50000, output_tokens: 8000, input_tokens_details: { cached_tokens: 0 }, output_tokens_details: { reasoning_tokens: 6000 }, ...usage }, ...extra } };
  }
  const coverageRow = (symbol, extra = {}) => ({ symbol, reviewDirective: "NONE", provisionalDisposition: "WATCH", reason: "no change", changedSincePrior: false, reasonCode: null, ...extra });
  const reviewOutput = (symbols, requests = []) => ({ schemaVersion: "universe-review.v1", universeVersion: "v6", universeHash: HEX64, eligibleCount: symbols.length,
    coverage: symbols.map((s) => coverageRow(s)), holdingAnalysis: [], researchRequests: requests, managerNote: "" });

  cases.push(fixture("gateway_routes_by_fixed_role_settles_exact_cents_and_records_every_request", async () => {
    const O = require("./_investorOpenai");
    const P = require("./_investorPolicy");
    const fake = fakeAdmin();
    const symbols = ["AAA", "BBB", "CCC"];
    const manifest = { universeVersion: "v6", universeHash: HEX64, eligibleCount: 3, symbols };
    const cards = symbols.map((symbol) => ({ symbol, identity: { sector: "sw" }, price: null, dataQuality: { complete: false, missing: ["price"] } }));
    /* background lifecycle: queued on POST, completed on the first GET */
    const { transport, calls } = scriptedTransport([
      ({ body }) => ({ data: { id: "resp_bg1", status: "queued", model: body.model } }),
      () => completed("gpt-5.6-sol", reviewOutput(symbols, [{ symbol: "BBB", researchPriority: 1, completionClass: "BUY_REQUIRED", reason: "inflection", reviewDirective: "RESEARCH_NOW" }])),
    ]);
    const G = O.withDeps({ admin: fake, fetchImpl: transport, env: { OPENAI_API_KEY: "test-key" } });
    const r = await G.reviewUniverse({ cards, universeManifest: manifest, holdings: [], portfolio: { navMinor: "10000000" }, policy: P.loadActiveSync({}), contextManifestHash: "c".repeat(64) });
    if (r.ok !== true) throw new Error(`review failed ${JSON.stringify(r).slice(0, 300)}`);
    if (r.plan.mode !== "single" || r.coverage.length !== 3 || r.researchRequests[0].symbol !== "BBB") throw new Error("review output");
    /* the POST carried the fixed manager routing: Sol, high reasoning, store:false, strict schema, background */
    const post = calls[0];
    if (post.method !== "POST" || post.body.model !== "gpt-5.6-sol" || post.body.store !== false || post.body.background !== true) throw new Error(`post body ${JSON.stringify(post.body).slice(0, 200)}`);
    if (!post.body.reasoning || post.body.reasoning.effort !== "high" || post.body.text.format.strict !== true || post.body.text.format.type !== "json_schema") throw new Error("reasoning/schema");
    if (!post.body.text.format.schema.required.includes("coverage") || post.body.text.format.schema.additionalProperties !== false) throw new Error("strict schema not generated from the canonical one");
    if (calls[1].method !== "GET" || !/resp_bg1/.test(calls[1].url)) throw new Error("background poll");
    /* exact cost from the published rates: 50k ordinary input × $4/M + 8k output × $20/M = $0.36 */
    if (r.costMinor !== "36") throw new Error(`cost ${r.costMinor}`);
    const spend = await G.spendToday();
    if (spend.spentMinor !== 36 || spend.reservedMinor !== 0 || spend.calls !== 1 || spend.tokens.reasoning !== 6000 || spend.byRole.manager !== 1) throw new Error(`ledger ${JSON.stringify(spend)}`);
    const rec = await G.readRequest(r.requestIds[0]);
    if (!rec || rec.status !== "complete" || rec.responseId !== "resp_bg1" && !rec.responseId) throw new Error("request record");
    for (const k of ["promptHash", "schemaHash", "policyHash", "contextManifestHash", "tokens", "costMinor", "latencyMs", "returnedModel", "reasoningEffort", "outputHash"]) if (rec[k] == null) throw new Error(`record missing ${k}`);
    if (rec.role !== "manager" || rec.model !== "gpt-5.6-sol" || rec.costMinor !== "36" || rec.policyHash !== P.policyIdentity().policyHash) throw new Error("record identity");
    /* the same request key resumes from the record instead of paying twice */
    const again = await G.reviewUniverse({ cards, universeManifest: manifest, holdings: [], portfolio: { navMinor: "10000000" }, policy: P.loadActiveSync({}), contextManifestHash: "c".repeat(64) });
    if (again.ok !== true || calls.length !== 2) throw new Error("completed request was re-bought");
    /* Terra is forbidden and no function routes to it; facts and verification route to Luna */
    if (O.ROLE_OF.reviewUniverse !== "manager" || O.ROLE_OF.extractFacts !== "facts" || O.ROLE_OF.verifyClaimsIndependently !== "verification") throw new Error("role map");
    for (const fn of Object.keys(O.ROLE_OF)) { const m = P.ROLE_MODELS[O.ROLE_OF[fn]].model; if (P.FORBIDDEN_INVESTMENT_MODELS.includes(m)) throw new Error(`${fn} routes to a forbidden model`); }
    for (const fn of ["reviewUniverse", "researchCompany", "finalizePortfolio", "reviseEntry", "reviseHolding", "finalizeEventRevision", "repairCoverageStructure"]) if (P.ROLE_MODELS[O.ROLE_OF[fn]].model !== "gpt-5.6-sol") throw new Error(`${fn} is not Sol`);
    if (O.MODELS.classify.model !== "gpt-5.6-luna" || /terra/.test(JSON.stringify(O.MODELS))) throw new Error("legacy table still names Terra");
    return true;
  }));

  cases.push(fixture("gateway_failures_are_never_decisions_refusal_truncation_schema_budget_and_substitution", async () => {
    const O = require("./_investorOpenai");
    const P = require("./_investorPolicy");
    const symbols = ["AAA"];
    const manifest = { universeVersion: "v6", universeHash: HEX64, eligibleCount: 1, symbols };
    const cards = [{ symbol: "AAA" }];
    const run = async (script, seed = null, key = "k") => {
      const fake = fakeAdmin();
      if (seed) fake.docs.set(`${fake.COL.costs}/openai_${new Date().toISOString().slice(0, 10)}`, seed);
      const { transport, calls } = scriptedTransport(script);
      const G = O.withDeps({ admin: fake, fetchImpl: transport, env: { OPENAI_API_KEY: "test-key" } });
      const r = await G.reviewUniverse({ cards, universeManifest: manifest, background: false, contextManifestHash: key.repeat(64).slice(0, 64) });
      return { r, calls, fake, G };
    };
    const assertFailure = (r, error) => {
      if (r.ok !== false || r.error !== error) throw new Error(`expected ${error}, got ${JSON.stringify(r).slice(0, 200)}`);
      if (r.cause !== "evidence_unavailable" || r.decision !== "ABSTAIN" || r.reasonCode !== "MODEL_FAILURE" || r.evidenceUnavailable !== true) throw new Error(`failure shape for ${error}`);
      if (r.coverage.length !== 0) throw new Error("partial coverage returned on failure");
    };
    /* refusal */
    const refusal = await run([{ data: { id: "r1", status: "completed", model: "gpt-5.6-sol", output: [{ type: "message", content: [{ type: "refusal", refusal: "cannot" }] }], usage: { input_tokens: 1000, output_tokens: 10 } } }]);
    assertFailure(refusal.r, "model_refusal");
    if ((await refusal.G.spendToday()).spentMinor !== 0 || (await refusal.G.spendToday()).calls !== 1) throw new Error("refusal not settled at its billed cost");
    /* truncation */
    const trunc = await run([completed("gpt-5.6-sol", reviewOutput(symbols), {}, { status: "incomplete", incomplete_details: { reason: "max_output_tokens" } })]);
    assertFailure(trunc.r, "output_truncated");
    /* schema-invalid: a workflow value in the decision column and a missing field */
    const bad = await run([completed("gpt-5.6-sol", { ...reviewOutput(symbols), coverage: [{ ...coverageRow("AAA"), provisionalDisposition: "RESEARCH_NOW" }] })]);
    assertFailure(bad.r, "schema_invalid");
    if (!bad.r.schemaErrors || !bad.r.schemaErrors.length) throw new Error("schema errors not surfaced");
    /* unparseable */
    const junk = await run([{ data: { id: "j", status: "completed", model: "gpt-5.6-sol", output: [{ type: "message", content: [{ type: "output_text", text: "{not json" }] }], usage: {} } }]);
    assertFailure(junk.r, "unparseable_model_output");
    /* an unexpected tool call when none were offered */
    const tool = await run([{ data: { id: "t", status: "completed", model: "gpt-5.6-sol", output: [{ type: "function_call", call_id: "c", name: "submitOrder", arguments: "{}" }], usage: {} } }]);
    assertFailure(tool.r, "tool_call_without_tools");
    /* model substitution by the provider */
    const sub = await run([completed("gpt-5.6-terra", reviewOutput(symbols))]);
    assertFailure(sub.r, "model_substituted");
    /* HTTP error and unreachable: reservation released, nothing spent */
    const http = await run([{ status: 429, data: { error: { message: "rate" } } }]);
    assertFailure(http.r, "openai_http_429");
    if ((await http.G.spendToday()).reservedMinor !== 0) throw new Error("reservation leaked after HTTP error");
    const dead = await run([() => { throw new Error("ECONNRESET"); }]);
    assertFailure(dead.r, "model_unreachable");
    /* exhausted reservation: no HTTP call is made, and the shortfall is not a smaller proposal */
    const ceiling = Number(P.budgetPolicy().dailyReservationMinor);
    const broke = await run([completed("gpt-5.6-sol", reviewOutput(symbols))], { day: new Date().toISOString().slice(0, 10), spentMinor: ceiling, reservedMinor: 0 });
    assertFailure(broke.r, "daily_reservation_exhausted");
    if (broke.calls.length !== 0 || broke.r.budgetBlocked !== true) throw new Error("budget-blocked call still reached the model");
    const rec = [...broke.fake.docs.values()].find((d) => d && d.status === "budget_blocked");
    if (!rec) throw new Error("budget block not recorded");
    /* no key: fail closed before any transport */
    const nokey = O.withDeps({ admin: fakeAdmin(), fetchImpl: async () => { throw new Error("must not be called"); }, env: {} });
    const nk = await nokey.reviewUniverse({ cards, universeManifest: manifest });
    if (nk.ok !== false || nk.decision !== "ABSTAIN") throw new Error("missing key not fail-closed");
    /* the failure helper itself */
    const f = O.failure("x");
    if (f.ok !== false || f.cause !== "evidence_unavailable" || f.decision !== "ABSTAIN" || f.reasonCode !== "MODEL_FAILURE") throw new Error("failure shape");
    return true;
  }));

  cases.push(fixture("gateway_tools_are_allowlisted_bounded_and_scoped_and_citations_must_exist", async () => {
    const O = require("./_investorOpenai");
    const P = require("./_investorPolicy");
    const memo = (symbol, claimId = "claim_1") => ({ schemaVersion: "research-memo.v1", symbol, asOf: "2026-09-04T12:00:00Z",
      checklist: { business: "b", whatChanged: "c", agreementDisagreement: "a", risks: "r", valuationFramework: "v", disconfirmingEvidence: "d", returnAndHorizon: "h", versusAlternatives: "x", mandateOrAbstain: "m" },
      factualPremises: [{ premiseId: "p1", text: "revenue grew", claimId, documentVersionId: "docv_1" }], inferences: [], valuation: null, bearCase: "bear",
      thesisHealth: "INTACT", proposedDecision: "WATCH", reasonCode: null, mandate: null });
    const dossier = { symbol: "ABC", dossierHash: "d".repeat(64), claimIds: ["claim_1"], card: {} };
    const executed = [];
    const tools = {
      getFilingFactsAsOf: { description: "facts", parameters: { type: "object", additionalProperties: false, required: ["symbol", "asOfMs", "concepts"], properties: { symbol: { type: "string" }, asOfMs: { type: "number" }, concepts: { type: "array", items: { type: "string" } } } },
        execute: async (args) => { executed.push(args); return { facts: [{ concept: "Revenues", valueScaled: "1" }] }; } },
    };
    const cutoffMs = Date.UTC(2026, 8, 4, 12);
    /* one allowlisted call, then the memo: stateless continuation carries the call and its output */
    let t = scriptedTransport([
      { data: { id: "r1", status: "completed", model: "gpt-5.6-sol", output: [{ type: "function_call", call_id: "c1", name: "getFilingFactsAsOf", arguments: JSON.stringify({ symbol: "ABC", asOfMs: cutoffMs, concepts: ["Revenues"] }) }], usage: { input_tokens: 1000, output_tokens: 100 } } },
      completed("gpt-5.6-sol", memo("ABC"), { input_tokens: 2000, output_tokens: 3000 }),
    ]);
    let G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const r = await G.researchCompany({ dossier, tools, cutoffMs });
    if (r.ok !== true || r.memo.symbol !== "ABC" || r.toolCalls.length !== 1 || r.toolCalls[0].ok !== true || !r.toolCalls[0].resultHash) throw new Error(`tool loop ${JSON.stringify(r).slice(0, 300)}`);
    if (executed.length !== 1 || executed[0].symbol !== "ABC") throw new Error("tool not executed once");
    const second = t.calls[1].body;
    if (!second.input.some((x) => x.type === "function_call_output" && x.call_id === "c1") || !second.input.some((x) => x.type === "function_call")) throw new Error("continuation lacks the call and its output");
    if (!t.calls[0].body.tools || t.calls[0].body.tools[0].name !== "getFilingFactsAsOf" || t.calls[0].body.tools[0].strict !== true) throw new Error("tool definition");
    /* both turns settle into one cost: (1000+2000) input × $4/M + (100+3000) output × $20/M = $0.074 → 7 cents */
    if (r.costMinor !== "7") throw new Error(`tool-loop cost ${r.costMinor}`);
    /* a tool that is not allowlisted is a rejected output, not an executed call */
    t = scriptedTransport([{ data: { id: "r2", status: "completed", model: "gpt-5.6-sol", output: [{ type: "function_call", call_id: "c2", name: "browse", arguments: "{}" }], usage: {} } }]);
    G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const bad = await G.researchCompany({ dossier, tools, cutoffMs });
    if (bad.ok !== false || bad.error !== "tool_not_allowlisted" || bad.decision !== "ABSTAIN" || t.calls.length !== 1) throw new Error(`browse ${JSON.stringify(bad).slice(0, 200)}`);
    /* a call for another symbol is out of scope */
    t = scriptedTransport([{ data: { id: "r3", status: "completed", model: "gpt-5.6-sol", output: [{ type: "function_call", call_id: "c3", name: "getFilingFactsAsOf", arguments: JSON.stringify({ symbol: "ZZZ", asOfMs: cutoffMs, concepts: [] }) }], usage: {} } }]);
    G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const scope = await G.researchCompany({ dossier, tools, cutoffMs });
    if (scope.ok !== false || scope.error !== "tool_symbol_out_of_scope") throw new Error("symbol scope");
    /* the call cap ends the loop as a failure, never as an unbounded spend */
    const cap = P.TOOL_POLICY.maxCallsPerJob;
    let n = 0;
    t = scriptedTransport([() => { n += 1; return { data: { id: `r${n}`, status: "completed", model: "gpt-5.6-sol", output: [{ type: "function_call", call_id: `c${n}`, name: "getFilingFactsAsOf", arguments: JSON.stringify({ symbol: "ABC", asOfMs: cutoffMs, concepts: [] }) }], usage: {} } }; }]);
    G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const capped = await G.researchCompany({ dossier, tools, cutoffMs });
    if (capped.ok !== false || capped.error !== "tool_call_cap" || t.calls.length !== cap + 1) throw new Error(`cap ${capped.error} after ${t.calls.length} calls`);
    /* a defined tool outside the allowlist cannot even be offered */
    let threw = null;
    try { G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } }); await G.researchCompany({ dossier, tools: { submitOrder: { execute: async () => ({}) } }, cutoffMs }); } catch (e) { threw = e.code; }
    if (threw !== "TOOL_NOT_ALLOWLISTED") throw new Error("forbidden tool offered");
    /* an invented claim id is a rejected output */
    t = scriptedTransport([completed("gpt-5.6-sol", memo("ABC", "claim_made_up"))]);
    G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const invented = await G.researchCompany({ dossier, tools: null, cutoffMs });
    if (invented.ok !== false || invented.error !== "unknown_claim_reference" || invented.unknownClaimIds[0] !== "claim_made_up") throw new Error("invented citation accepted");
    /* BUY without a mandate is not a research result */
    t = scriptedTransport([completed("gpt-5.6-sol", { ...memo("ABC"), proposedDecision: "BUY" })]);
    G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    if ((await G.researchCompany({ dossier, cutoffMs })).error !== "buy_without_mandate") throw new Error("BUY without mandate accepted");
    /* extraction: only verbatim quotes survive; a paraphrase is dropped and reported */
    const text = "Net revenue for the quarter was $1.21 billion, up 14% year over year. We now expect full year revenue of $4.0 billion to $4.2 billion.";
    t = scriptedTransport([completed("gpt-5.6-luna", { schemaVersion: "fact-extraction.v1", abstained: false, abstainReason: "", contradictions: [], claims: [
      { claimType: "GUIDANCE", text: "FY revenue guide", quote: "expect full year revenue of $4.0 billion to $4.2 billion", documentRef: "v1", effectivePeriod: "FY2026", metric: "revenue", lowValue: "4000000000", highValue: "4200000000", unit: "USD", date: null, confirmed: null, supersedesHint: null },
      { claimType: "FACT", text: "Revenue rose 14%", quote: "revenue increased fourteen percent", documentRef: "v1", effectivePeriod: null, metric: null, lowValue: null, highValue: null, unit: null, date: null, confirmed: null, supersedesHint: null },
      { claimType: "FACT", text: "x", quote: "up 14% year over year", documentRef: "v_unknown", effectivePeriod: null, metric: null, lowValue: null, highValue: null, unit: null, date: null, confirmed: null, supersedesHint: null },
    ] }, { input_tokens: 3000, output_tokens: 500 })]);
    G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const ex = await G.extractFacts({ documentVersions: [{ versionId: "v1", canonicalText: text, sourceId: "sec.latest", form: "8-K" }], symbol: "ABC" });
    if (ex.ok !== true || ex.claims.length !== 1 || ex.claims[0].claimType !== "GUIDANCE" || ex.claims[0].documentVersionId !== "v1" || ex.dropped.length !== 2) throw new Error(`extraction ${JSON.stringify(ex).slice(0, 300)}`);
    if (!ex.dropped.some((d) => d.dropReason === "quote_not_found_verbatim") || !ex.dropped.some((d) => d.dropReason === "unknown_document_reference")) throw new Error("drop reasons");
    if (t.calls[0].body.model !== "gpt-5.6-luna" || t.calls[0].body.reasoning) throw new Error("extraction did not route to Luna without reasoning");
    /* Luna cost at $0.20/M in + $1.20/M out: 3000 in, 500 out → $0.0012 → 0 cents, recorded exactly in nano-dollars */
    if (ex.costMinor !== "0") throw new Error(`luna cost ${ex.costMinor}`);
    /* nothing survived verification → extraction incomplete, not an empty finding */
    t = scriptedTransport([completed("gpt-5.6-luna", { schemaVersion: "fact-extraction.v1", abstained: false, abstainReason: "", contradictions: [], claims: [
      { claimType: "FACT", text: "x", quote: "this sentence is not in the document", documentRef: "v1", effectivePeriod: null, metric: null, lowValue: null, highValue: null, unit: null, date: null, confirmed: null, supersedesHint: null }] })]);
    G = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const none = await G.extractFacts({ documentVersions: [{ versionId: "v1", canonicalText: text }], symbol: "ABC" });
    if (none.ok !== true || none.claims.length !== 0 || none.extractionIncomplete !== true) throw new Error("unverified extraction reported as complete");
    return true;
  }));

  cases.push(fixture("gateway_partitions_a_large_roster_into_sol_blocks_without_filtering_and_synthesis_checks_ranks", async () => {
    const O = require("./_investorOpenai");
    const P = require("./_investorPolicy");
    const symbols = Array.from({ length: 40 }, (_, i) => `S${String(i).padStart(3, "0")}`);
    const cards = symbols.map((symbol) => ({ symbol, identity: { sector: "sw" }, filler: "x".repeat(400) }));
    const holdings = [{ symbol: "S005", position: { qty: 10 } }, { symbol: "OFFR", position: { qty: 3 } }];
    const plan = O.planBlocks({ cards, holdings, guardrailTokens: 3000 });
    if (plan.mode !== "blocks" || plan.blocks.length < 2) throw new Error(`plan ${plan.mode} ${plan.blocks.length}`);
    const covered = plan.blocks.flatMap((b) => b.symbols);
    if (covered.length !== 40 || new Set(covered).size !== 40 || !symbols.every((s) => covered.includes(s))) throw new Error("a block filtered or duplicated a symbol");
    const heldPlaced = plan.blocks.flatMap((b) => b.holdings.map((h) => h.symbol));
    if (heldPlaced.length !== 2 || !heldPlaced.includes("OFFR") || !heldPlaced.includes("S005")) throw new Error("holdings not analysed exactly once");
    if (plan.blocks.some((b) => b.symbols.length > Math.ceil(40 / plan.blocks.length))) throw new Error("blocks unbalanced");
    /* every block is a Sol call; merged coverage is complete and research priorities are unique across blocks */
    const manifest = { universeVersion: "v6", universeHash: HEX64, eligibleCount: 40, symbols };
    const { transport, calls } = scriptedTransport([({ body }) => {
      const m = JSON.parse(body.input[1].content.match(/UNIVERSE_MANIFEST=(\{.*?\})\n/s)[1]);
      return completed("gpt-5.6-sol", reviewOutput(m.symbols, [{ symbol: m.symbols[0], researchPriority: 1, completionClass: "OPTIONAL_DISCOVERY", reason: "r", reviewDirective: "RESEARCH_NOW" }]), { input_tokens: 10000, output_tokens: 2000 });
    }]);
    const G = O.withDeps({ admin: fakeAdmin(), fetchImpl: transport, env: { OPENAI_API_KEY: "k" } });
    const r = await G.reviewUniverse({ cards, universeManifest: manifest, holdings, background: false, guardrailTokens: 3000, contextManifestHash: "e".repeat(64) });
    if (r.ok !== true || r.plan.mode !== "blocks" || r.coverage.length !== 40 || new Set(r.coverage.map((x) => x.symbol)).size !== 40) throw new Error(`merged coverage ${r.coverage.length}`);
    if (calls.length !== plan.blocks.length || calls.some((c) => c.body.model !== "gpt-5.6-sol")) throw new Error("a block did not go to Sol");
    const prios = r.researchRequests.map((q) => q.researchPriority);
    if (new Set(prios).size !== prios.length || prios[0] !== 1 || r.researchRequests.every((q) => q.blockPriority === 1) !== true) throw new Error("priorities not unique after merge");
    if (r.costMinor !== String(calls.length * 8)) throw new Error(`block cost ${r.costMinor}`);
    /* final synthesis: duplicate capital ranks and BUY on a held name are rejected; a clean basket passes */
    const synth = (decisions, mandates) => ({ schemaVersion: "portfolio-synthesis.v1", planClass: "EXPANSION", decisions, expansionMandates: mandates, comparisonNote: "n" });
    const row = (symbol, decision, capitalRank, fundingState = "FUNDED") => ({ symbol, decision, capitalRank, reasonCode: null, fundingState, reason: "r" });
    const mandate = (symbol) => ({ ...P.EXAMPLE_MANDATE_PROPOSAL, symbol });
    let t = scriptedTransport([completed("gpt-5.6-sol", synth([row("AAA", "BUY", 1), row("BBB", "BUY", 1)], [mandate("AAA"), mandate("BBB")]))]);
    let F = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const dup = await F.finalizePortfolio({ coverage: [], researchResults: [{ memo: { symbol: "AAA", factualPremises: [{ claimId: "claim_1" }, { claimId: "claim_2" }, { claimId: "claim_3" }] } }], holdings: [], background: false });
    if (dup.ok !== false || dup.error !== "capital_ranks_not_unique") throw new Error("duplicate ranks accepted");
    t = scriptedTransport([completed("gpt-5.6-sol", synth([row("HLD", "BUY", 1)], [mandate("HLD")]))]);
    F = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const held = await F.finalizePortfolio({ coverage: [], researchResults: [{ memo: { symbol: "HLD", factualPremises: [{ claimId: "claim_1" }, { claimId: "claim_2" }, { claimId: "claim_3" }] } }], holdings: [{ symbol: "HLD" }], background: false });
    if (held.ok !== false || held.error !== "buy_on_held_symbol") throw new Error("scaling in accepted by the gateway");
    t = scriptedTransport([completed("gpt-5.6-sol", synth([row("AAA", "BUY", 1), row("BBB", "WATCH", null, "UNFUNDED")], [mandate("AAA")]))]);
    F = O.withDeps({ admin: fakeAdmin(), fetchImpl: t.transport, env: { OPENAI_API_KEY: "k" } });
    const clean = await F.finalizePortfolio({ coverage: [], researchResults: [{ memo: { symbol: "AAA", factualPremises: [{ claimId: "claim_1" }, { claimId: "claim_2" }, { claimId: "claim_3" }] } }], holdings: [], background: false });
    if (clean.ok !== true || clean.synthesis.expansionMandates.length !== 1 || clean.synthesis.decisions[1].reasonCode !== null) throw new Error(`clean synthesis ${JSON.stringify(clean).slice(0, 200)}`);
    return true;
  }));

  /* ── Group 5 (commit 11): the one logical Manager Meeting, in memory.
     Freeze → review → exact coverage with one MISSING-rows repair →
     holding maintenance → bounded research → final synthesis → activation
     → one decision row per managed symbol; typed no-BUY reasons (D-12);
     yield and resume from a checkpoint without paying twice. */
  function meetingWorld({ reviewMissing = ["BBB"], reviewFails = false, synthesisBuy = true, heldMandateWidens = false } = {}) {
    const D = require("./_investorDossier");
    const P = require("./_investorPolicy");
    const U = require("./_investorUniverse");
    const fake = fakeAdmin();
    const t0 = Date.UTC(2026, 8, 4, 12, 45);   // 08:45 ET on a Friday
    const symbols = ["AAA", "BBB", "CCC"];
    const bars = [];
    let c = 40;
    for (let i = 300; i >= 1; i -= 1) { const d = new Date(t0 - i * 864e5).toISOString().slice(0, 10); c *= 1 + ((i % 7) - 3) / 400; bars.push({ date: d, c: Number(c.toFixed(4)) }); }
    const snapshotRoster = { schemaVersion: "universe-snapshot.v1", universeVersion: "v6", universeHash: "f".repeat(64), eligibleCount: 3, symbols, members: [], excluded: [], managedOffRoster: [], tradingDate: "2026-09-04" };
    const universe = { freezeEligibleSnapshot: () => snapshotRoster, coverageMatches: U.coverageMatches, tradeTier: symbols.map((symbol) => ({ symbol, sector: "sw", cik: "0000123456", company: `${symbol} Corp` })), researchTier: [] };
    const calls = { review: 0, repair: 0, research: 0, synthesis: 0, staged: [] };
    const mandateFor = (symbol) => ({ ...P.EXAMPLE_MANDATE_PROPOSAL, symbol });
    const gateway = {
      reviewUniverse: async ({ cards, universeManifest, holdings }) => {
        calls.review += 1;
        if (reviewFails) return { ok: false, error: "daily_reservation_exhausted", budgetBlocked: true, cause: "evidence_unavailable", decision: "ABSTAIN", reasonCode: "MODEL_FAILURE", coverage: [], holdingAnalysis: [], researchRequests: [] };
        if (cards.length !== 3 || universeManifest.universeHash !== snapshotRoster.universeHash) throw new Error("review did not receive the frozen roster");
        const rows = symbols.filter((s) => !reviewMissing.includes(s)).map((symbol) => ({ symbol, reviewDirective: symbol === "AAA" ? "RESEARCH_NOW" : "NONE", provisionalDisposition: symbol === "CCC" ? "IGNORE" : "WATCH", reason: "r", changedSincePrior: false, reasonCode: null }));
        const holdingAnalysis = holdings.map((h) => ({ symbol: h.symbol, decision: "HOLD", reasonCode: null, revisionResult: heldMandateWidens ? "REVISED" : "UNCHANGED", researchDirective: "NONE", thesisHealth: "INTACT",
          emergency: { emergencyReductionRank: 1, emergencyRankAsOf: new Date(t0).toISOString(), emergencyRankExpiresAfterSession: "2026-09-08", rationale: "x" }, rationale: "hold",
          mandate: heldMandateWidens ? { ...mandateFor(h.symbol), decision: "HOLD", action: { ...P.EXAMPLE_MANDATE_PROPOSAL.action, kind: "HOLD_PROTECT", entry: null, protection: { ...P.EXAMPLE_MANDATE_PROPOSAL.action.protection, lossBoundaryPriceMicros: "1000000" } } } : null }));
        return { ok: true, coverage: rows, holdingAnalysis, researchRequests: [{ symbol: "AAA", researchPriority: 1, completionClass: "BUY_REQUIRED", reason: "inflection", reviewDirective: "RESEARCH_NOW" }], managerNote: "", plan: { mode: "single" }, responseIds: ["resp_1"], requestIds: ["mr_1"], costMinor: "36" };
      },
      repairCoverageStructure: async ({ missing }) => { calls.repair += 1; return { ok: true, coverage: [...missing, "ZZZ"].map((symbol) => ({ symbol, reviewDirective: "NONE", provisionalDisposition: "WATCH", reason: "repaired", changedSincePrior: false, reasonCode: null })), repaired: missing.length, costMinor: "4" }; },
      researchCompany: async ({ dossier }) => { calls.research += 1; return { ok: true, symbol: dossier.symbol, costMinor: "120", requestId: "mr_r", outputHash: "h".repeat(64), memo: { schemaVersion: "research-memo.v1", symbol: dossier.symbol, asOf: new Date(t0).toISOString(),
        checklist: { business: "b", whatChanged: "c", agreementDisagreement: "a", risks: "r", valuationFramework: "v", disconfirmingEvidence: "d", returnAndHorizon: "h", versusAlternatives: "x", mandateOrAbstain: "m" },
        factualPremises: [{ premiseId: "p1", text: "t", claimId: "claim_1", documentVersionId: "v1" }, { premiseId: "p2", text: "t", claimId: "claim_2", documentVersionId: "v1" }, { premiseId: "p3", text: "t", claimId: "claim_3", documentVersionId: "v1" }],
        inferences: [], valuation: null, bearCase: "bear", thesisHealth: "INTACT", proposedDecision: "BUY", reasonCode: null, mandate: mandateFor(dossier.symbol) } }; },
      finalizePortfolio: async ({ researchResults, holdings }) => { calls.synthesis += 1; const s = researchResults[0].symbol; return { ok: true, costMinor: "80", requestId: "mr_f", synthesis: { schemaVersion: "portfolio-synthesis.v1", planClass: "EXPANSION",
        decisions: [{ symbol: s, decision: synthesisBuy ? "BUY" : "WATCH", capitalRank: synthesisBuy ? 1 : null, reasonCode: synthesisBuy ? null : "UNCERTAINTY", fundingState: synthesisBuy ? "FUNDED" : "NOT_APPLICABLE", reason: "best" }],
        expansionMandates: synthesisBuy ? [mandateFor(s)] : [], comparisonNote: "n" } }; },
    };
    const claimVerifier = { verifyAndPersistBatch: async ({ proposals }) => ({ allSupported: true, blockingSymbols: [], forceAbstainSymbols: [], byProposal: Object.fromEntries(proposals.map((p) => [p.symbol, { symbol: p.symbol, allSupported: true, blocking: false, forceAbstain: false, verdictIds: [] }])) }), verifyAndPersist: async () => ({ summary: { allSupported: true } }) };
    const mandate = { stagePortfolioPlan: async ({ planClass, proposals, activationSnapshot }) => { calls.staged.push({ planClass, symbols: proposals.map((p) => p.symbol), snapshotId: activationSnapshot.activationSnapshotId }); return { status: "COMMITTED", planId: `plan_${planClass}_${calls.staged.length}` }; } };
    const history = { readDailyWithMeta: async () => ({ series: bars, provenance: null }) };
    const seed = async () => {
      for (const symbol of symbols) {
        const v = D.composeVersion({ symbol, identity: { name: `${symbol} Corp`, sector: "sw", cik: "0000123456" }, asOfMs: t0 - 3600e3, facts: [], fundamentals: null, claims: [], documents: [] });
        await D.persistVersion(v, { admin: fake, sourceAtMs: t0 - 3600e3 });
      }
      fake.docs.set(`${fake.COL.accounts}/paper-1`, { accountId: "paper-1", balanceCents: { cash: 1000000, reserved: 0, positions: 42000 }, balanceRevision: 7 });
      fake.docs.set(`${fake.COL.positions}/paper-1_BBB`, { accountId: "paper-1", symbol: "BBB", open: true, qty: 10, entryPriceUsd: 40, lastMarkUsd: 42, lastMarkAt: new Date(t0 - 60e3).toISOString(), costBasisCents: 40000 });
    };
    const deps = { admin: fake, gateway, universe, claimVerifier, mandate, history, tools: null, now: () => t0, reconcile: null };
    return { fake, deps, calls, t0, symbols, seed, snapshotRoster };
  }

  cases.push(fixture("manager_meeting_covers_the_frozen_roster_exactly_repairs_missing_rows_once_and_persists_one_decision_per_symbol", async () => {
    const MGR = require("./_investorManager");
    const W = meetingWorld();
    await W.seed();
    const claim = { jobId: "j_mgr", runId: "run_premarket_manager_paper-1_2026-09-04", payload: { accountId: "paper-1", tradingDate: "2026-09-04" }, checkpoint: null };
    const out = await MGR.runManagerMeeting({ claim, deps: W.deps, budget: () => 10 * 60 * 1000, control: { engineMode: "manager" } });
    if (out.done !== true || out.failed) throw new Error(`meeting ${JSON.stringify(out).slice(0, 400)}`);
    const s = out.summary;
    if (s.status !== "complete" || s.eligibleCount !== 3 || s.universeHash !== W.snapshotRoster.universeHash || s.decisionCount !== 3) throw new Error(`summary ${JSON.stringify(s).slice(0, 300)}`);
    /* coverage: one structural repair supplied the missing BBB row; the extraneous ZZZ row was ignored */
    if (W.calls.review !== 1 || W.calls.repair !== 1 || s.coverage.ok !== true || s.coverage.completedCount !== 3 || s.coverage.repaired !== 1) throw new Error(`coverage ${JSON.stringify(s.coverage)} calls ${JSON.stringify(W.calls)}`);
    const decisions = [...W.fake.docs.entries()].filter(([k]) => k.startsWith(`${W.fake.COL.managerDecisions}/`)).map(([, v]) => v);
    if (decisions.length !== 3) throw new Error(`decision rows ${decisions.length}`);
    const SC = require("./_investorStorageCodec");
    const rows = decisions.map((d) => (d._codec ? SC.decode(d) : d));
    const by = Object.fromEntries(rows.map((r) => [r.symbol, r]));
    if (by.AAA.decision !== "BUY" || by.AAA.capitalRank !== 1 || by.AAA.fundingState !== "FUNDED" || by.AAA.source !== "final_synthesis") throw new Error(`AAA ${JSON.stringify(by.AAA)}`);
    if (by.BBB.decision !== "HOLD" || by.BBB.held !== true || by.BBB.source !== "holding_analysis") throw new Error(`BBB ${JSON.stringify(by.BBB)}`);
    if (by.CCC.decision !== "IGNORE" || by.CCC.source !== "coverage") throw new Error(`CCC ${JSON.stringify(by.CCC)}`);
    for (const r of rows) { if (!/^(BUY|WATCH|IGNORE|HOLD|REDUCE|SELL|ABSTAIN)$/.test(r.decision)) throw new Error("workflow value in decision column"); if (r.managerRunId !== claim.runId || !r.contextManifestHash) throw new Error("decision lineage"); }
    /* research ran once, smallest priority first; synthesis once; the expansion basket was staged against a fresh activation snapshot after maintenance */
    if (W.calls.research !== 1 || W.calls.synthesis !== 1) throw new Error(`calls ${JSON.stringify(W.calls)}`);
    if (W.calls.staged.length !== 1 || W.calls.staged[0].planClass !== "EXPANSION" || W.calls.staged[0].symbols[0] !== "AAA" || !W.calls.staged[0].snapshotId) throw new Error(`staging ${JSON.stringify(W.calls.staged)}`);
    if (s.activation.status !== "COMMITTED" || !s.activation.planId || s.noBuyReasons.length !== 0 || s.buys[0].symbol !== "AAA") throw new Error(`activation ${JSON.stringify(s.activation)} ${JSON.stringify(s.noBuyReasons)}`);
    if (s.costMinor !== "240") throw new Error(`cost ${s.costMinor}`);
    /* the run record is counters and hashes, never 304 rows */
    const run = await MGR.readRun(claim.runId, { admin: W.fake });
    if (!run || run.status !== "complete" || run.universeHash !== W.snapshotRoster.universeHash || run.decisionCount !== 3 || JSON.stringify(run).length > 20000) throw new Error("run record");
    const memo = await require("./_investorResearch").latest("AAA", { admin: W.fake });
    if (!memo || memo.researchVersion !== 1 || memo.proposedDecision !== "BUY" || memo.factualPremises.length !== 3) throw new Error("memo not persisted immutably");
    const ctrl = W.fake.docs.get(`${W.fake.COL.control}/control`);
    if (!ctrl || ctrl.lastManagerRunDate !== "2026-09-04" || ctrl.lastManagerRun.status !== "complete") throw new Error("control not updated");
    return true;
  }));

  cases.push(fixture("manager_meeting_yields_on_budget_resumes_from_its_checkpoint_and_never_pays_for_a_stage_twice", async () => {
    const MGR = require("./_investorManager");
    const W = meetingWorld({ reviewMissing: [] });
    await W.seed();
    const claim = { jobId: "j_mgr2", runId: "run_premarket_manager_paper-1_2026-09-04", payload: { accountId: "paper-1", tradingDate: "2026-09-04" }, checkpoint: null };
    let ticks = 0;
    const first = await MGR.runManagerMeeting({ claim, deps: W.deps, budget: () => (ticks++ < 1 ? 10 * 60 * 1000 : 1000), control: { engineMode: "manager" } });
    if (first.yielded !== true || first.checkpoint.stage !== "review" || !first.checkpoint.data.contextManifestHash) throw new Error(`first ${JSON.stringify(first).slice(0, 200)}`);
    if (W.calls.review !== 0) throw new Error("review paid before the yield");
    const second = await MGR.runManagerMeeting({ claim: { ...claim, checkpoint: first.checkpoint }, deps: W.deps, budget: () => 10 * 60 * 1000, control: { engineMode: "manager" } });
    if (second.done !== true || second.failed || second.summary.decisionCount !== 3) throw new Error(`second ${JSON.stringify(second).slice(0, 300)}`);
    if (W.calls.review !== 1 || W.calls.repair !== 0 || W.calls.research !== 1 || W.calls.synthesis !== 1) throw new Error(`calls after resume ${JSON.stringify(W.calls)}`);
    if (second.summary.contextManifestHash !== first.checkpoint.data.contextManifestHash) throw new Error("context hash changed across resume");
    return true;
  }));

  cases.push(fixture("manager_meeting_fails_closed_with_typed_no_buy_reasons_and_holding_protection_is_never_widened_from_a_delta_review", async () => {
    const MGR = require("./_investorManager");
    /* an exhausted reservation at review: no decisions, typed BUDGET_EXHAUSTED, run failed closed */
    const W = meetingWorld({ reviewFails: true });
    await W.seed();
    const claim = { jobId: "j_mgr3", runId: "run_premarket_manager_paper-1_2026-09-04", payload: { accountId: "paper-1", tradingDate: "2026-09-04" }, checkpoint: null };
    const out = await MGR.runManagerMeeting({ claim, deps: W.deps, budget: () => 10 * 60 * 1000, control: { engineMode: "manager" } });
    if (out.done !== true || out.failed !== true || out.reason !== "REVIEW_FAILED") throw new Error(`failure ${JSON.stringify(out).slice(0, 200)}`);
    if (!out.summary.noBuyReasons.some((r) => r.code === "BUDGET_EXHAUSTED")) throw new Error("no-BUY reason not typed");
    if ([...W.fake.docs.keys()].some((k) => k.startsWith(`${W.fake.COL.managerDecisions}/`))) throw new Error("decisions written on a failed run");
    if (W.calls.staged.length !== 0) throw new Error("something was staged on a failed run");
    const run = await MGR.readRun(claim.runId, { admin: W.fake });
    if (!run || run.status !== "failed_closed") throw new Error("run not failed closed");
    /* a missing card fails before Sol is asked anything */
    const W2 = meetingWorld();
    await W2.seed();
    W2.fake.docs.delete(`${W2.fake.COL.dossiers}/CCC`);
    const out2 = await MGR.runManagerMeeting({ claim: { ...claim, jobId: "j_mgr4" }, deps: W2.deps, budget: () => 10 * 60 * 1000, control: { engineMode: "manager" } });
    if (out2.failed !== true || out2.reason !== "COVERAGE_INPUT_INCOMPLETE" || W2.calls.review !== 0 || !out2.summary.noBuyReasons[0].missing.includes("CCC")) throw new Error(`cards gate ${JSON.stringify(out2).slice(0, 200)}`);
    /* anti-goalpost: a HOLD mandate that widens the loss boundary from a delta review is refused; prior protection stays; expansion freezes */
    const W3 = meetingWorld({ reviewMissing: [], heldMandateWidens: true });
    await W3.seed();
    W3.fake.docs.set(`${W3.fake.COL.activeMandates}/paper-1_BBB`, { accountId: "paper-1", symbol: "BBB", status: "ACTIVE", appliedVersionId: "mv1", lossBoundaryPriceMicros: "39000000", action: { protection: { lossBoundaryPriceMicros: "39000000" } } });
    const out3 = await MGR.runManagerMeeting({ claim: { ...claim, jobId: "j_mgr5" }, deps: W3.deps, budget: () => 10 * 60 * 1000, control: { engineMode: "manager" } });
    if (out3.done !== true || out3.failed) throw new Error(`goalpost run ${JSON.stringify(out3).slice(0, 200)}`);
    const m = out3.summary.maintenance;
    if (!m || !m.actionRequired.some((x) => x.symbol === "BBB" && /ANTI_GOALPOST/.test(x.reason))) throw new Error(`anti-goalpost not enforced ${JSON.stringify(m)}`);
    if (W3.calls.staged.some((x) => x.planClass === "RISK_MAINTENANCE")) throw new Error("widened protection was staged");
    if (out3.summary.buys.length !== 0 || !out3.summary.noBuyReasons.some((r) => r.code === "FREEZE_STATE")) throw new Error(`expansion not frozen on an unsafe holding ${JSON.stringify(out3.summary.noBuyReasons)}`);
    const SC = require("./_investorStorageCodec");
    const bbb = SC.decode(W3.fake.docs.get(`${W3.fake.COL.managerDecisions}/${claim.runId}_BBB`));
    if (bbb.decision !== "ABSTAIN" || bbb.reasonCode !== "DATA_INCOMPLETE") throw new Error(`held row after refused maintenance ${JSON.stringify(bbb)}`);
    /* pure pieces: a held name never becomes BUY; repair adds only expected missing rows; ranks must be unique */
    const rows = MGR.composeFinalDecisionRows({ roster: { symbols: ["AAA", "BBB"] }, workset: { symbols: ["AAA", "BBB"], rows: [{ symbol: "AAA", eligible: true, held: false, heldQty: 0, entryEligible: true }, { symbol: "BBB", eligible: true, held: true, heldQty: 5, entryEligible: false }] },
      effective: { coverage: [{ symbol: "AAA", reviewDirective: "NONE", provisionalDisposition: "WATCH", reason: "r", changedSincePrior: false, reasonCode: null }, { symbol: "BBB", reviewDirective: "NONE", provisionalDisposition: "HOLD_CANDIDATE", reason: "r", changedSincePrior: false, reasonCode: null }], holdingAnalysis: [{ symbol: "BBB", decision: "HOLD", rationale: "h" }] },
      synthesis: { decisions: [{ symbol: "BBB", decision: "BUY", capitalRank: 1, fundingState: "FUNDED", reason: "x" }], expansionMandates: [{ symbol: "BBB" }] } });
    if (rows.find((r) => r.symbol === "BBB").decision !== "HOLD") throw new Error("held name overridden by a BUY");
    const merged = MGR.mergeStructuralCoverageRepair({ original: { coverage: [{ symbol: "AAA", provisionalDisposition: "WATCH" }] }, repairedRows: [{ symbol: "AAA", provisionalDisposition: "IGNORE" }, { symbol: "BBB", provisionalDisposition: "WATCH" }, { symbol: "ZZZ" }], expectedMissing: ["BBB"] });
    if (merged.coverage.length !== 2 || merged.coverage[0].provisionalDisposition !== "WATCH" || merged.repair.ignored.length !== 2) throw new Error("repair changed a returned row or added an unexpected one");
    let code = null;
    try { MGR.validateUniqueCapitalRanksAndFeasibility([{ decision: "BUY", capitalRank: 1, mandate: {} }, { decision: "BUY", capitalRank: 1, mandate: {} }]); } catch (e) { code = e.code; }
    if (code !== "CAPITAL_RANK_DUPLICATE") throw new Error("duplicate ranks accepted");
    if (MGR.antiGoalpostViolations({ action: { protection: { lossBoundaryPriceMicros: "38000000" } } }, { action: { protection: { lossBoundaryPriceMicros: "39000000" } } })[0] !== "LOSS_BOUNDARY_WIDENED") throw new Error("widening not detected");
    if (MGR.antiGoalpostViolations({ action: { protection: { lossBoundaryPriceMicros: "40000000" } } }, { action: { protection: { lossBoundaryPriceMicros: "39000000" } } }).length !== 0) throw new Error("tightening refused");
    return true;
  }));

  cases.push(fixture("research_pool_launches_smallest_unique_priority_first_defers_only_a_suffix_and_waits_at_the_barrier", async () => {
    const R = require("./_investorResearch");
    const P = require("./_investorPortfolio");
    const pool = R.createPool({ now: () => 0 });
    const req = (symbol, p, cls = "OPTIONAL_DISCOVERY") => ({ symbol, researchPriority: p, completionClass: cls, reason: "r", reviewDirective: "RESEARCH_NOW" });
    const launched = [];
    const worker = async (r) => { launched.push(r.researchPriority); return { ok: true, symbol: r.symbol }; };
    const out = await pool.run({ requests: [req("C", 3, "HOLDING_REQUIRED"), req("A", 1), req("B", 2)], concurrency: 1, worker });
    if (out.launchedOrder.join(",") !== "1,2,3" || out.completed.length !== 3 || out.barrier !== true) throw new Error(`order ${out.launchedOrder}`);
    /* deferral removes only a suffix and never a HOLDING_REQUIRED request */
    const d = R.deferSuffix(R.orderRequests([req("A", 1), req("B", 2, "HOLDING_REQUIRED"), req("C", 3), req("D", 4)]), { maxJobs: 1 });
    if (d.keep.map((r) => r.symbol).join(",") !== "A,B" || d.deferred.map((r) => r.symbol).join(",") !== "C,D") throw new Error(`suffix ${JSON.stringify(d)}`);
    let code = null;
    try { R.orderRequests([req("A", 1), req("B", 1)]); } catch (e) { code = e.code; }
    if (code !== "RESEARCH_PRIORITY_DUPLICATE") throw new Error("duplicate priority accepted");
    /* deadline pressure after the must-complete work defers the rest, and the pool's cap never exceeds the policy */
    let t = 0;
    const late = R.createPool({ now: () => t });
    const out2 = await late.run({ requests: [req("A", 1, "HOLDING_REQUIRED"), req("B", 2), req("C", 3)], concurrency: 9, deadlineMs: 5, worker: async (r) => { t = 10; return { ok: true, symbol: r.symbol }; } });
    if (out2.completed.length !== 1 || out2.deferred.length !== 2 || out2.concurrency > 3) throw new Error(`deadline ${JSON.stringify(out2.ranges)} cap ${out2.concurrency}`);
    /* symbol state kinds from the portfolio reader */
    const fake = fakeAdmin();
    fake.docs.set(`${fake.COL.accounts}/paper-1`, { accountId: "paper-1", balanceCents: { cash: 500000, reserved: 12000 } });
    fake.docs.set(`${fake.COL.positions}/p1`, { accountId: "paper-1", symbol: "HLD", open: true, qty: 4, entryPriceUsd: 10, lastMarkUsd: 11 });
    fake.docs.set(`${fake.COL.positions}/p2`, { accountId: "paper-1", symbol: "PRT", open: true, qty: 2, entryPriceUsd: 10, lastMarkUsd: 10 });
    fake.docs.set(`${fake.COL.orders}/o1`, { accountId: "paper-1", symbol: "UNF", side: "buy", status: "working", qty: 5, qtyFilled: 0, refPriceUsd: 20, grossCents: 10000, frictionCents: 10 });
    fake.docs.set(`${fake.COL.orders}/o2`, { accountId: "paper-1", symbol: "PRT", side: "buy", status: "partially_filled", qty: 5, qtyFilled: 2, refPriceUsd: 10 });
    const kinds = {};
    for (const s of ["HLD", "PRT", "UNF", "NON"]) kinds[s] = (await P.symbolState({ accountId: "paper-1", symbol: s, admin: fake })).kind;
    if (kinds.HLD !== "HELD" || kinds.PRT !== "PARTIALLY_FILLED_ENTRY" || kinds.UNF !== "UNFILLED_ENTRY" || kinds.NON !== "NONHOLDING_NO_ENTRY") throw new Error(`kinds ${JSON.stringify(kinds)}`);
    const snap = await P.snapshot({ accountId: "paper-1", admin: fake });
    if (snap.navMinor !== String(500000 + 12000 + 4400 + 2000) || snap.settledCashMinor !== "500000" || snap.aggregates.heldSymbols.join(",") !== "HLD,PRT" || snap.aggregates.unprotected.length !== 2) throw new Error(`snapshot ${JSON.stringify(snap.aggregates)} ${snap.navMinor}`);
    if (!/^[a-f0-9]{64}$/.test(snap.contentHash)) throw new Error("snapshot hash");
    const act = await P.captureActivationSnapshot({ accountId: "paper-1", admin: fake, reason: "test" });
    if (!act.activationSnapshotId || act.contentHash !== snap.contentHash || !fake.docs.has(`${fake.COL.activationSnapshots}/${act.activationSnapshotId}`)) throw new Error("activation snapshot not persisted");
    return true;
  }));

  /* ── Group 5 (commit 11): the deterministic calculators. Sizing precedence
     (§15.1) only ever makes a quantity smaller and names the binding term;
     clamp materiality (§6.8) returns a proposal to Sol rather than silently
     changing it; valuation arithmetic is exact and rejects model-authored
     derived numbers (§6.6). */
  cases.push(fixture("sizing_precedence_only_shrinks_names_the_binding_term_and_valuation_arithmetic_is_exact_and_rejects_derived_input", () => {
    const PR = require("./_investorPortfolioRisk");
    const V = require("./_investorValuation");
    const P = require("./_investorPolicy");
    const pr = PR.selfCheck();
    if (pr.pass !== true) throw new Error(`portfolio risk selfCheck: ${JSON.stringify(pr.failures).slice(0, 300)}`);
    const va = V.selfCheck();
    if (va.pass !== true) throw new Error(`valuation selfCheck: ${JSON.stringify(va.failures).slice(0, 300)}`);
    /* §15.1 on the policy's own paper mandate: NAV $1,000,000; the name cap (10%) binds before cash */
    const portfolio = { navMinor: "100000000", settledCashMinor: "60000000", reservedMinor: "0", positions: [], workingOrders: [] };
    const candidate = { symbol: "AAA", sector: "sw", proposedQuantityUnits: "5000", limitPriceMicros: "50000000", lossBoundaryPriceMicros: "47000000", costPerShareMicros: "20000", advMinor: "10000000000", spreadBps: "10" };
    const q = PR.quantityAuthority({ candidate, portfolio, policy: P.RISK_MANDATE, marks: {}, advBySymbol: {}, sectorOf: () => "sw", clusterOf: null });
    if (BigInt(q.authorizedQuantityUnits) > BigInt(candidate.proposedQuantityUnits)) throw new Error("authority enlarged the proposal");
    if (!/^(0|[1-9][0-9]*)$/.test(q.authorizedQuantityUnits) || !q.bindingConstraint) throw new Error(`authority shape ${JSON.stringify(q).slice(0, 200)}`);
    /* name cap: 10% × $1,000,000 = $100,000 / $50 = 2000 shares; planned loss: 1% × NAV = $10,000 / ($3 + $0.02) = 3311 → name binds at 2000 */
    if (q.authorizedQuantityUnits !== "2000" || !/name/i.test(q.bindingConstraint)) throw new Error(`binding ${q.bindingConstraint} ${q.authorizedQuantityUnits}`);
    /* a wide spread refuses outright with a rule reason, never a smaller position */
    const wide = PR.quantityAuthority({ candidate: { ...candidate, spreadBps: "500" }, portfolio, policy: P.RISK_MANDATE, marks: {}, advBySymbol: {}, sectorOf: () => "sw", clusterOf: null });
    if (wide.authorizedQuantityUnits !== "0" || wide.refused !== true || !wide.reasons.some((r) => /SPREAD/.test(r.code || r))) throw new Error(`spread refusal ${JSON.stringify(wide).slice(0, 200)}`);
    /* clamp materiality on tiny positions (§6.8): 3→2 is material, 100→96 is silent rounding, 1→0 is a rejection */
    const cm = (p, a, extra = {}) => PR.clampMateriality({ proposedQuantityUnits: p, authorizedQuantityUnits: a, limitPriceMicros: "50000000", navMinor: "100000000", policy: P.RISK_MANDATE, ...extra });
    if (cm("3", "2").material !== true || cm("1", "0").material !== true || cm("100", "96").silent !== true || cm("100", "95").material !== true) throw new Error("clamp thresholds");
    if (cm("100", "100", { boundaryTouched: true }).material !== true) throw new Error("boundary equality not material");
    /* scenarios carry measurements only: no score, no rank */
    const sc = PR.runScenarios({ portfolio, candidates: [candidate], policy: P.RISK_MANDATE, marks: {}, advBySymbol: {}, sectorOf: () => "sw", clusterOf: null });
    if (/"(score|ranking|preferred)"/i.test(JSON.stringify(sc))) throw new Error("scenario output ranks companies");
    if (typeof (sc.set && sc.set.basketFeasible) !== "boolean") throw new Error("basket feasibility missing");
    /* valuation: exact event tree and the derived-arithmetic refusal */
    const leaf = (t) => [{ name: "branch.x.probabilityPpm", value: "1000000", unit: "ppm" }, { name: "branch.x.terminalPriceMicros", value: t, unit: "micros" }];
    const tree = V.run({ method: "event_tree", assumptions: [], scenarios: [{ id: "bear", probabilityPpm: "250000", assumptions: leaf("20000000"), terminalPriceMicros: "20000000" }, { id: "base", probabilityPpm: "500000", assumptions: leaf("50000000"), terminalPriceMicros: "50000000" }, { id: "bull", probabilityPpm: "250000", assumptions: leaf("80000000"), terminalPriceMicros: "80000000" }], priceMicros: "40000000" });
    if (tree.ok !== true || tree.expectedTerminalPriceMicros !== "50000000" || tree.impliedReturnBps !== "2500") throw new Error(`event tree ${JSON.stringify(tree).slice(0, 200)}`);
    let code = null;
    try { V.run({ method: "event_tree", assumptions: [], scenarios: [{ id: "base", probabilityPpm: "1000000", assumptions: leaf("1"), terminalPriceMicros: "1" }], priceMicros: "1", expectedTerminalPriceMicros: "1" }); } catch (e) { code = e.code; }
    if (code !== "MODEL_DERIVED_ARITHMETIC_REJECTED") throw new Error(`derived arithmetic accepted: ${code}`);
    code = null;
    try { V.run({ method: "event_tree", assumptions: [], scenarios: [{ id: "base", probabilityPpm: "900000", assumptions: leaf("1"), terminalPriceMicros: "1" }], priceMicros: "1" }); } catch (e) { code = e.code; }
    if (code !== "PROBABILITY_SUM_INVALID") throw new Error(`probability sum accepted: ${code}`);
    return true;
  }));

  /* ── Group 6 (commit 12): the activation invariant. Three records, three
     hashes; the envelope only ever shrinks; staging is one all-or-none CAS
     transaction against a fresh activation snapshot; the outbox carries
     desired state and nothing claims the broker applied it. */
  function stagingWorld() {
    const P = require("./_investorPolicy");
    const fake = fakeAdmin();
    const nowMs = Date.UTC(2026, 8, 4, 13);
    fake.docs.set(`${fake.COL.accounts}/paper-1`, { accountId: "paper-1", balanceCents: { cash: 6000000, reserved: 0, contributed_capital: -6000000 }, balanceRevision: 7, startingNavCents: 6000000 });
    /* the opening balance is a journal entry, as openAccount writes it, so conservation can be rebuilt from the ledger alone */
    fake.docs.set(`${fake.COL.ledger}/genesis_paper-1`, { txnId: "genesis_paper-1", accountId: "paper-1", kind: "capital_contribution", legs: [{ account: "cash", amountCents: 6000000 }, { account: "contributed_capital", amountCents: -6000000 }] });
    const snapshot = (extra = {}) => ({ activationSnapshotId: "act_1", accountId: "paper-1", navMinor: "10000000", settledCashMinor: "6000000", reservedMinor: "0", positions: [], workingOrders: [], portfolioVersion: 7, reservationAccountVersion: 0, writerEpoch: 0, ...extra });
    const proposal = (symbol, extra = {}) => ({ ...P.EXAMPLE_MANDATE_PROPOSAL, symbol, ...extra });
    const texts = { docv_1: "Net revenue for the quarter was $1.21 billion, up 14% year over year.", docv_2: "We now expect full year revenue of $4.0 billion to $4.2 billion.", docv_3: "Gross margin contracted 300 basis points on input costs." };
    const claimsById = { claim_1: { claimId: "claim_1", documentVersionId: "docv_1", text: "revenue +14%", quote: "up 14% year over year" },
      claim_2: { claimId: "claim_2", documentVersionId: "docv_2", text: "FY guide", quote: "full year revenue of $4.0 billion to $4.2 billion" },
      claim_3: { claimId: "claim_3", documentVersionId: "docv_3", text: "margin contracted", quote: "gross margin contracted 300 basis points" } };
    const spanReader = async (ids) => Object.fromEntries(ids.map((id) => [id, { versionId: id, contentHash: `h_${id}`, canonicalText: texts[id] || "" }]));
    const verifier = async ({ premises }) => ({ available: true, model: "luna", output: { verdicts: premises.map((p) => ({ claimId: p.claimId, verdict: "SUPPORTED" })) } });
    const marks = { AAA: { advMinor: "10000000000", spreadBps: "10" }, BBB: { advMinor: "10000000000", spreadBps: "10" }, CCC: { advMinor: "10000000000", spreadBps: "10" }, DDD: { advMinor: "10000000000", spreadBps: "10" } };
    const verify = async (proposals) => require("./_investorClaimVerifier").verifyAndPersistBatch({ proposals, claimsById, spanReader, verifier, admin: fake });
    const stage = async (args) => require("./_investorMandate").stagePortfolioPlan({ accountId: "paper-1", policy: P.loadActiveSync({}), managerRunId: "run_1", admin: fake, marks, nowMs, eligibleSymbols: ["AAA", "BBB", "CCC", "DDD"], ...args });
    const docsIn = (col) => [...fake.docs.entries()].filter(([k]) => k.startsWith(`${col}/`)).map(([, v]) => v);   /* raw documents: an added id would break the codec hash */
    return { P, fake, nowMs, snapshot, proposal, verify, stage, docsIn, marks };
  }

  cases.push(fixture("mandate_staging_binds_three_records_reserves_capital_writes_the_outbox_and_commits_all_or_none_under_cas", async () => {
    const MD = require("./_investorMandate");
    const SC = require("./_investorStorageCodec");
    const W = stagingWorld();
    const AAA = W.proposal("AAA"), BBB = W.proposal("BBB", { allocation: { ...W.P.EXAMPLE_MANDATE_PROPOSAL.allocation, capitalRank: 1 } });
    const verified = await W.verify([AAA, BBB]);
    if (verified.allSupported !== true) throw new Error("claims not verified");
    const staged = await W.stage({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION" }, proposals: [AAA, BBB], verifiedProposalClaims: verified, activationSnapshot: W.snapshot() });
    if (staged.status !== "COMMITTED" || staged.mandateVersionIds.length !== 2 || !staged.planId) throw new Error(`staging ${JSON.stringify(staged).slice(0, 300)}`);
    const dec = (d) => (d._codec ? SC.decode(d) : d);
    const proposals = W.docsIn(W.fake.COL.mandateProposals).map(dec), bindings = W.docsIn(W.fake.COL.mandates).map(dec), envelopes = W.docsIn(W.fake.COL.activationEnvelopes).map(dec);
    const reservations = W.docsIn(W.fake.COL.capitalReservations).map(dec), orderSets = W.docsIn(W.fake.COL.orderSets).map(dec), legs = W.docsIn(W.fake.COL.orderLegs).map(dec);
    const outbox = W.docsIn(W.fake.COL.executionOutbox), pointers = W.docsIn(W.fake.COL.activeMandates), events = W.docsIn(W.fake.COL.mandateEvents), plans = W.docsIn(W.fake.COL.portfolioPlans);
    if (proposals.length !== 2 || bindings.length !== 2 || envelopes.length !== 2 || reservations.length !== 2 || orderSets.length !== 2 || legs.length !== 8 || outbox.length !== 2 || pointers.length !== 2 || events.length !== 2) throw new Error(`counts ${[proposals.length, bindings.length, envelopes.length, reservations.length, orderSets.length, legs.length, outbox.length, pointers.length, events.length]}`);
    /* the proposal is stored unmodified; the binding and the envelope are separate records with their own fields */
    const pa = proposals.find((p) => p.symbol === "AAA");
    if (pa.proposalHash !== MD.hashPortfolioPlanProposal && JSON.stringify(pa.proposal) !== JSON.stringify(AAA)) throw new Error("proposal edited");
    const ba = bindings.find((b) => b.symbol === "AAA");
    for (const k of ["mandateVersionId", "mandateSeriesId", "version", "expectedActiveVersion", "proposalId", "proposalHash", "portfolioPlanId", "managerRunId", "schemaHash", "policyHash", "sourceManifestHash"]) if (ba[k] == null) throw new Error(`binding missing ${k}`);
    if (ba.version !== 1 || ba.expectedActiveVersion !== 0 || ba.mandateSeriesId !== "paper-1_AAA") throw new Error("lineage");
    const ea = envelopes.find((e) => e.symbol === "AAA");
    for (const k of ["status", "activationSnapshotId", "riskPolicyHash", "authorizedQuantityUnits", "reservedNotionalMinor", "plannedLossAtBoundaryMinor", "bindingGapStressLossMinor"]) if (ea[k] == null) throw new Error(`envelope missing ${k}`);
    if (ea.status !== "ALLOW" || BigInt(ea.authorizedQuantityUnits) > BigInt(AAA.allocation.proposedQuantityUnits)) throw new Error("envelope enlarged the proposal");
    /* §7.1 worked example: 86 × ($43.25 − $37.25) + 86 × $0.04 = $519.44 planned loss at the boundary; reservation covers limit plus cost */
    if (ea.plannedLossAtBoundaryMinor !== "51944" || ea.reservedNotionalMinor !== "372294" || ea.withinDeclaredCapital !== true || ea.withinDeclaredLoss !== true) throw new Error(`arithmetic ${JSON.stringify(ea).slice(0, 300)}`);
    /* desired order set: ENTRY, TARGET, STOP and TIME_EXIT for exactly the authorized quantity; nothing is applied */
    const os = orderSets.find((o) => o.symbol === "AAA");
    if (os.status !== "DESIRED" || os.appliedMandateVersionId !== null || os.purpose !== "ENTRY_WITH_PROTECTION") throw new Error("order set state");
    const roles = os.legs.map((l) => l.role).sort().join(",");
    if (roles !== "ENTRY,SELL,STOP,TARGET" || os.legs.some((l) => l.quantityUnits !== ea.authorizedQuantityUnits)) throw new Error(`legs ${roles}`);
    if (os.legs.find((l) => l.role === "ENTRY").timeInForce !== "DAY" || os.legs.find((l) => l.role === "STOP").timeInForce !== "GTC") throw new Error("leg time in force");
    if (outbox.some((o) => o.status !== "PENDING" || o.kind !== "APPLY_DESIRED_ORDER_SET" || o.authority !== "SOL_MANDATE")) throw new Error("outbox");
    const pt = pointers.find((p) => p.symbol === "AAA");
    if (pt.desiredVersion !== 1 || pt.status !== "DESIRED" || pt.appliedVersionId !== null || pt.lossBoundaryPriceMicros !== "37250000" || !pt.expiresAtMs) throw new Error(`pointer ${JSON.stringify(pt).slice(0, 200)}`);
    const res = W.fake.docs.get(`${W.fake.COL.reservationAccounts}/paper-1`);
    if (res.version !== 1 || res.reservedNotionalMinor !== String(372294 * 2) || res.committedPortfolioPlanId !== staged.planId) throw new Error(`reservation account ${JSON.stringify(res)}`);
    if (!plans.some((p) => p.planId === staged.planId && p.status === "COMMITTED" && !p.auditOnly)) throw new Error("plan record");
    /* the same plan against the same snapshot is a duplicate; a stale snapshot is refused; neither writes */
    const before = W.fake.docs.size;
    const dup = await W.stage({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION" }, proposals: [AAA, BBB], verifiedProposalClaims: verified, activationSnapshot: W.snapshot({ reservationAccountVersion: 1 }) });
    if (dup.status !== "DUPLICATE") throw new Error(`duplicate ${dup.status} ${dup.reason}`);
    const stale = await W.stage({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION" }, proposals: [W.proposal("CCC")], verifiedProposalClaims: await W.verify([W.proposal("CCC")]), activationSnapshot: W.snapshot({ activationSnapshotId: "act_2", reservationAccountVersion: 0 }) });
    if (stale.status !== "STALE") throw new Error(`stale ${stale.status} ${stale.reason}`);
    if (W.docsIn(W.fake.COL.activeMandates).length !== 2 || W.docsIn(W.fake.COL.mandates).length !== 2) throw new Error("a refused plan wrote mandate state");
    if (W.fake.docs.size - before !== 2) throw new Error("refusals should leave exactly their two audit records");
    /* all-or-none: one invalid member (boundary above limit) rejects the whole basket; the valid member is not activated */
    const bad = W.proposal("DDD", { action: { ...W.P.EXAMPLE_MANDATE_PROPOSAL.action, protection: { ...W.P.EXAMPLE_MANDATE_PROPOSAL.action.protection, lossBoundaryPriceMicros: "44000000" } } });
    const basket = await W.stage({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION" }, proposals: [W.proposal("CCC"), bad], verifiedProposalClaims: await W.verify([W.proposal("CCC"), bad]), activationSnapshot: W.snapshot({ activationSnapshotId: "act_3", reservationAccountVersion: 1 }) });
    if (basket.status !== "REJECTED" || !basket.rejected.some((r) => r.symbol === "DDD" && r.reasons.includes("LOSS_BOUNDARY_NOT_BELOW_LIMIT"))) throw new Error(`basket ${JSON.stringify(basket).slice(0, 300)}`);
    if (W.docsIn(W.fake.COL.activeMandates).some((p) => p.symbol === "CCC")) throw new Error("a valid member of a rejected basket was activated");
    /* a held name cannot receive BUY; a WATCH cannot carry order fields */
    const held = await W.stage({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION" }, proposals: [W.proposal("CCC")], verifiedProposalClaims: await W.verify([W.proposal("CCC")]), activationSnapshot: W.snapshot({ activationSnapshotId: "act_4", reservationAccountVersion: 1, positions: [{ symbol: "CCC", quantityUnits: "10", markMicros: "43000000", sector: "sw" }] }) });
    if (held.status !== "REJECTED" || !held.rejected.some((r) => r.reasons.includes("SCALING_IN_FORBIDDEN"))) throw new Error("scaling in accepted");
    const watch = MD.validateProposal({ ...W.P.EXAMPLE_MANDATE_PROPOSAL, decision: "WATCH", forecast: null, allocation: null }, { nowMs: W.nowMs });
    if (watch.ok !== false || !watch.errors.includes("NON_EXECUTABLE_DECISION_CARRIES_ORDER_FIELDS")) throw new Error("WATCH with order fields accepted");
    /* a material clamp returns the basket to Sol instead of silently shrinking it */
    const tiny = await W.stage({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION" }, proposals: [W.proposal("CCC")], verifiedProposalClaims: await W.verify([W.proposal("CCC")]), activationSnapshot: W.snapshot({ activationSnapshotId: "act_5", reservationAccountVersion: 1, navMinor: "1000000", settledCashMinor: "900000" }) });
    if (tiny.status !== "NEEDS_SOL_RESYNTHESIS" || !tiny.revisedFeasibleAlternatives || tiny.envelopes[0].materialClamp !== true || BigInt(tiny.envelopes[0].authorizedQuantityUnits) >= 86n) throw new Error(`clamp ${JSON.stringify(tiny).slice(0, 200)}`);
    /* RISK_MAINTENANCE: a HOLD for a held name commits protection legs only and reserves nothing */
    const hold = W.proposal("BBB", { decision: "HOLD", forecast: null, allocation: null, action: { kind: "HOLD_PROTECT", entry: null, protection: W.P.EXAMPLE_MANDATE_PROPOSAL.action.protection, exit: null } });
    const maint = await W.stage({ planClass: "RISK_MAINTENANCE", portfolioPlanProposal: { planClass: "RISK_MAINTENANCE" }, proposals: [hold], verifiedProposalClaims: await W.verify([hold]), activationSnapshot: W.snapshot({ activationSnapshotId: "act_6", reservationAccountVersion: 1, positions: [{ symbol: "BBB", quantityUnits: "86", markMicros: "45000000", sector: "sw" }] }) });
    if (maint.status !== "COMMITTED" || maint.reservedNotionalMinor !== String(372294 * 2)) throw new Error(`maintenance ${JSON.stringify(maint).slice(0, 200)}`);
    const holdSet = W.docsIn(W.fake.COL.orderSets).map(dec).find((o) => o.symbol === "BBB" && o.purpose === "PROTECT");
    if (!holdSet || holdSet.legs.some((l) => l.role === "ENTRY") || holdSet.legs.find((l) => l.role === "STOP").quantityUnits !== "86") throw new Error("hold order set");
    const bbbPointer = W.docsIn(W.fake.COL.activeMandates).find((p) => p.symbol === "BBB");
    if (bbbPointer.desiredVersion !== 2 || bbbPointer.decision !== "HOLD") throw new Error("hold pointer did not advance the series");
    /* a high-impact event pauses only the unfilled entry: outbox CANCEL_UNFILLED_ENTRY, pointer PAUSED_EVIDENCE, mandate still active */
    const paused = await MD.pauseUnfilledEntry("AAA", "evt_1", { accountId: "paper-1", admin: W.fake });
    if (paused.paused !== true || !W.fake.docs.has(`${W.fake.COL.executionOutbox}/${paused.transitionId}`)) throw new Error("pause not written");
    const ap = W.docsIn(W.fake.COL.activeMandates).find((p) => p.symbol === "AAA");
    if (ap.status !== "PAUSED_EVIDENCE" || ap.lossBoundaryPriceMicros !== "37250000") throw new Error("pause touched protection or missed status");
    if ((await MD.hasActiveMandate("AAA", { accountId: "paper-1", admin: W.fake })) !== true || (await MD.hasActiveMandate("ZZZ", { accountId: "paper-1", admin: W.fake })) !== false) throw new Error("hasActiveMandate");
    return true;
  }));

  cases.push(fixture("emergency_risk_policy_only_freezes_cancels_reduces_or_covers_never_opens_a_long_and_is_inactive_until_approved", async () => {
    const ER = require("./_investorEmergencyRisk");
    const R = require("./_investorRisk");
    const P = require("./_investorPolicy");
    const crypto = require("crypto");
    const nowMs = Date.UTC(2026, 8, 4, 14);
    const content = { ...P.EMERGENCY_RISK_POLICY_TEMPLATE, status: "APPROVED", approvedBy: "owner", approvedAtMs: nowMs - 86400000, effectiveFrom: "2026-09-01" };
    delete content.policyHash;
    const stored = { ...content, policyHash: crypto.createHash("sha256").update(JSON.stringify(P.canonical(content))).digest("hex") };
    const active = P.activeEmergencyPolicy(stored);
    if (active.active !== true) throw new Error(`policy not active: ${active.reason}`);
    const portfolio = { navMinor: "10000000", settledCashMinor: "3000000", reservedMinor: "0",
      positions: [{ symbol: "BIG", quantityUnits: "400", markMicros: "50000000", lossBoundaryPriceMicros: "45000000", sector: "sw" }, { symbol: "SHRT", quantityUnits: "-5", markMicros: "10000000", sector: "sw" }, { symbol: "NAK", quantityUnits: "10", markMicros: "20000000", lossBoundaryPriceMicros: null, sector: "sw" }],
      workingOrders: [{ orderId: "o1", symbol: "NEW", side: "buy", remainingUnits: "20", limitPriceMicros: "30000000", sector: "sw" }] };
    /* persistence: a breach must hold for its declared seconds before it fires */
    const first = ER.evaluateTriggers({ policy: stored, metrics: { drawdownFromPeakBps: "700" }, persistence: {}, nowMs });
    if (first.fired.length !== 0 || first.pending.length !== 1) throw new Error("drawdown fired without persistence");
    const second = ER.evaluateTriggers({ policy: stored, metrics: { drawdownFromPeakBps: "700", shortQuantityUnits: "5", singleNameWeightBps: "2000" }, persistence: first.persistence, nowMs: nowMs + 61000 });
    if (!second.fired.some((f) => f.id === "drawdown") || !second.fired.some((f) => f.id === "accidental_short")) throw new Error(`fired ${JSON.stringify(second.fired.map((f) => f.id))}`);
    const plan = ER.planActions({ fired: [...second.fired, { id: "concentration_breach", action: "REDUCE_TO_LIMIT", metric: "singleNameWeightBps", value: "2000", threshold: "1200", op: "gt" }], portfolio, policy: stored, riskMandate: P.RISK_MANDATE, nowMs });
    const ops = plan.actions.map((a) => a.op);
    if (!ops.includes("FREEZE_EXPANSION") || !ops.includes("CANCEL_UNFILLED_ENTRY") || !ops.includes("BUY_TO_COVER_ACCIDENTAL_SHORT") || !ops.includes("REDUCE")) throw new Error(`ops ${ops}`);
    for (const a of plan.actions) { if (a.label !== "EMERGENCY_RISK") throw new Error("unlabelled action"); if (ER.FORBIDDEN.includes(a.op)) throw new Error(`forbidden op ${a.op}`); if (!stored.permittedOperations.includes(a.op)) throw new Error(`unpermitted op ${a.op}`); }
    const cover = plan.actions.find((a) => a.op === "BUY_TO_COVER_ACCIDENTAL_SHORT");
    if (cover.symbol !== "SHRT" || cover.quantityUnits !== "5" || cover.targetQuantityUnits !== "0") throw new Error("cover quantity is not exactly the short");
    /* concentration: 400 × $50 = $20,000 of $100,000 NAV against a 10% cap → remove exactly 200 shares, marketable limit, regular session */
    const reduce = plan.actions.find((a) => a.op === "REDUCE" && a.symbol === "BIG");
    if (!reduce || reduce.quantityUnits !== "200" || reduce.orderType !== "MARKETABLE_LIMIT" || reduce.session !== "REGULAR_ONLY" || reduce.ruleUsed !== "EXACT_BREACH_REMOVAL_WHOLE_SHARES") throw new Error(`reduce ${JSON.stringify(reduce)}`);
    const cancel = plan.actions.find((a) => a.op === "CANCEL_UNFILLED_ENTRY");
    if (!cancel || cancel.symbol !== "NEW" || cancel.quantityUnits !== "20") throw new Error("unfilled entry not cancelled");
    if (plan.forcesLaterSolReview !== true || plan.forcesReconciliation !== true) throw new Error("emergency action does not force review");
    /* without an owner-approved policy the template is inactive: freeze and alert only */
    const inactive = ER.planActions({ fired: second.fired, portfolio, policy: P.EMERGENCY_RISK_POLICY_TEMPLATE, riskMandate: P.RISK_MANDATE, nowMs, policyActive: false });
    if (inactive.actions.length !== 1 || inactive.actions[0].op !== "FREEZE_EXPANSION") throw new Error(`inactive policy acted: ${inactive.actions.map((a) => a.op)}`);
    /* a thesis-less position gets a protection plan that can never buy */
    const ep = ER.emergencyProtectionPlan({ accountId: "paper-1", symbol: "NAK", position: { quantityUnits: "10", markMicros: "20000000", entryPriceMicros: "19000000" }, reason: "inherited without thesis", nowMs });
    if (ep.permittedOperations.includes("BUY") || !ep.forbiddenOperations.includes("BUY") || ep.status !== "ACTION_REQUIRED" || ep.protectiveOrder.stopMicros !== "19500000" || ep.protectiveOrder.quantityUnits !== "10") throw new Error(`protection plan ${JSON.stringify(ep).slice(0, 200)}`);
    /* enforcement persists outbox transitions labelled EMERGENCY_RISK and freezes buys; never a Sol decision */
    const fake = fakeAdmin();
    const out = await ER.enforceBoundedPolicy({ accountId: "paper-1", observed: { shortQuantityUnits: "5" }, portfolio, control: { emergencyRiskPolicy: stored }, admin: fake, nowMs });
    if (out.policyActive !== true || !out.actions.some((a) => a.op === "BUY_TO_COVER_ACCIDENTAL_SHORT") || out.outbox.length !== 1) throw new Error(`enforce ${JSON.stringify(out).slice(0, 200)}`);
    const ob = fake.docs.get(`${fake.COL.executionOutbox}/${out.outbox[0]}`);
    if (!ob || ob.authority !== "EMERGENCY_RISK" || ob.kind !== "BUY_TO_COVER_ACCIDENTAL_SHORT" || ob.quantityUnits !== "5") throw new Error("outbox transition");
    const ctrl = fake.docs.get(`${fake.COL.control}/control`);
    if (!ctrl || ctrl.buyState !== "FROZEN" || ctrl.emergencyState !== "ENGAGED" || ctrl.emergencyForcesSolReview !== true) throw new Error("control not frozen");
    /* operational revalidation: stale broker truth freezes expansion as a hard breach; a clean book allows it */
    const stale = R.revalidateOperationalLimits({ portfolio: { navMinor: "10000000", settledCashMinor: "9000000", reservedMinor: "0", positions: [], workingOrders: [] }, brokerTruthAgeSeconds: 900 });
    if (stale.allowExpansion !== false || stale.hardBreach !== true || !stale.reasons.includes("STALE_BROKER_TRUTH")) throw new Error("stale truth allowed expansion");
    const clean = R.revalidateOperationalLimits({ portfolio: { navMinor: "10000000", settledCashMinor: "9000000", reservedMinor: "0", positions: [], workingOrders: [] }, brokerTruthAgeSeconds: 30 });
    if (clean.allowExpansion !== true || clean.hardBreach !== false) throw new Error(`clean book refused: ${clean.reasons}`);
    return true;
  }));

  /* ── Group 6 (commit 13): deterministic execution. The OHLC simulator is
     touch-before-close with the adverse convention; the outbox saga applies
     desired state through the bound adapter; fills are immutable, positions
     are rebuilt from them, the ledger balances, reservations release only
     when the entry is terminal, and protection attaches for exactly the
     owned quantity. No model code is loaded by the executor. */
  cases.push(fixture("ohlc_simulator_is_touch_before_close_takes_the_adverse_path_and_never_awards_a_favourable_same_bar_exit", () => {
    const X = require("./_investorExecution");
    const bar = (o, h, l, c, v = 100000) => ({ t: "2026-09-04T14:31:00.000Z", o, h, l, c, v });
    const entry = { role: "ENTRY", side: "buy", type: "LIMIT", priceMicros: "43250000", remainingUnits: "86" };
    /* marketable at the open fills at the open, never above the limit */
    let s = X.simulateLegOnBar({ leg: entry, bar: bar(43.0, 43.5, 42.9, 43.4) });
    if (!s.fill || s.fill.priceMicros !== "43000000" || s.fill.quantityUnits !== "86") throw new Error(`marketable ${JSON.stringify(s)}`);
    /* touched but unmarketable fills at the limit, capped by participation (10% of 500 shares = 50) */
    s = X.simulateLegOnBar({ leg: entry, bar: bar(43.6, 43.8, 43.2, 43.7, 500) });
    if (!s.fill || s.fill.priceMicros !== "43250000" || s.fill.quantityUnits !== "50") throw new Error(`queue ${JSON.stringify(s)}`);
    /* not touched: no fill; the close is irrelevant */
    s = X.simulateLegOnBar({ leg: entry, bar: bar(43.6, 43.9, 43.3, 43.26) });
    if (s.fill || s.touch) throw new Error("close-only fill");
    /* a halted bar fills nothing even when the level is inside its range */
    s = X.simulateLegOnBar({ leg: entry, bar: bar(43.0, 43.5, 42.9, 43.4, 0) });
    if (s.fill || s.reason !== "halted_no_volume") throw new Error("halt filled");
    const stop = { role: "STOP", side: "sell", type: "STOP", stopMicros: "37250000", remainingUnits: "86" };
    /* gapped through: fills at the (worse) open */
    s = X.simulateLegOnBar({ leg: stop, bar: bar(36.0, 36.5, 35.8, 36.2) });
    if (!s.fill || s.fill.priceMicros !== "36000000" || s.fill.quantityUnits !== "86") throw new Error(`gap ${JSON.stringify(s)}`);
    /* touched intrabar: fills at the stop less the modelled adverse print, never at the close */
    s = X.simulateLegOnBar({ leg: stop, bar: bar(38.0, 38.2, 37.1, 37.9) });
    if (!s.fill || BigInt(s.fill.priceMicros) >= 37250000n || BigInt(s.fill.priceMicros) < 37100000n) throw new Error(`stop print ${JSON.stringify(s)}`);
    if (X.simulateLegOnBar({ leg: stop, bar: bar(38.0, 38.2, 37.3, 37.26) }).fill) throw new Error("stop filled without a touch");
    const target = { role: "TARGET", side: "sell", type: "LIMIT", priceMicros: "49000000", remainingUnits: "86" };
    s = X.simulateLegOnBar({ leg: target, bar: bar(49.5, 49.8, 49.1, 49.3) });
    if (!s.fill || s.fill.priceMicros !== "49500000") throw new Error("gap-up target should fill at the open");
    s = X.simulateLegOnBar({ leg: target, bar: bar(48.0, 49.2, 47.9, 48.9) });
    if (!s.fill || s.fill.priceMicros !== "49000000") throw new Error("touched target");
    /* stop-limit may trigger and stay unfilled */
    const sl = { role: "STOP", side: "sell", type: "STOP_LIMIT", stopMicros: "37250000", priceMicros: "37000000", remainingUnits: "10" };
    s = X.simulateLegOnBar({ leg: sl, bar: bar(38.0, 38.1, 36.5, 36.8) });
    if (s.fill || s.triggered !== true || s.reason !== "stop_limit_triggered_unfilled") throw new Error(`stop-limit ${JSON.stringify(s)}`);
    /* a marketable limit obeys its collar against the reference */
    const ml = { role: "SELL", side: "sell", type: "MARKETABLE_LIMIT", collarBps: "75", remainingUnits: "10" };
    s = X.simulateLegOnBar({ leg: ml, bar: { ...bar(39.0, 39.2, 38.8, 39.1), prevClose: 40.0 } });
    if (s.fill || s.reason !== "outside_collar") throw new Error("collar ignored");
    s = X.simulateLegOnBar({ leg: ml, bar: { ...bar(39.8, 40.0, 39.7, 39.9), prevClose: 40.0 } });
    if (!s.fill || s.fill.priceMicros !== "39800000") throw new Error("marketable limit inside collar");
    /* collisions: entry + target → target suppressed; entry + stop → ambiguous adverse; target + stop → stop wins, ambiguous */
    const f = (p) => ({ touch: true, fill: { quantityUnits: "1", priceMicros: p } });
    let r = X.resolveBarCollisions({ entry: f("43000000"), target: f("49000000"), stop: null });
    if (r.target.fill !== null || r.target.suppressed !== true || r.ambiguous) throw new Error("favourable same-bar exit awarded");
    r = X.resolveBarCollisions({ entry: f("43000000"), target: null, stop: f("37000000") });
    if (!r.stop.fill || r.ambiguous !== true) throw new Error("adverse same-bar stop not taken");
    r = X.resolveBarCollisions({ entry: null, target: f("49000000"), stop: f("37000000") });
    if (r.target.fill !== null || !r.stop.fill || r.ambiguous !== true) throw new Error("target and stop collision not adverse");
    return true;
  }));

  cases.push(fixture("execution_saga_applies_desired_state_records_immutable_fills_protects_exactly_the_owned_quantity_and_keeps_the_ledger_balanced", async () => {
    const X = require("./_investorExecution");
    const B = require("./_investorBroker");
    const W = stagingWorld();
    const AAA = W.proposal("AAA");
    const verified = await W.verify([AAA]);
    const staged = await W.stage({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION" }, proposals: [AAA], verifiedProposalClaims: verified, activationSnapshot: W.snapshot() });
    if (staged.status !== "COMMITTED") throw new Error(`staging ${staged.status} ${staged.reason}`);
    const t0 = Date.UTC(2026, 9, 5, 13, 30);          // Monday 2026-10-05 09:30 ET, an authorized session in the example
    const adapter = B.createPaperAdapter({ admin: W.fake, now: () => t0 });
    const orderSetId = `os_${staged.mandateVersionIds[0]}`;
    /* the outbox transition applies the desired order set: entry WORKING, protection ARMED, pointer WORKING, applied pointer advanced */
    const applied = await X.applyOutbox({ admin: W.fake, adapter, accountId: "paper-1", nowMs: t0 });
    if (applied.applied !== 1) throw new Error(`apply ${JSON.stringify(applied)}`);
    let os = await X.readOrderSet(W.fake, orderSetId);
    if (os.status !== "WORKING" || !os.brokerGroupId || os.appliedMandateVersionId !== staged.mandateVersionIds[0]) throw new Error("order set not working");
    const st = (role) => os.legs.find((l) => l.role === role).status;
    if (st("ENTRY") !== "WORKING" || st("TARGET") !== "ARMED" || st("STOP") !== "ARMED") throw new Error(`legs ${os.legs.map((l) => `${l.role}:${l.status}`)}`);
    let pointer = W.fake.docs.get(`${W.fake.COL.activeMandates}/paper-1_AAA`);
    if (pointer.status !== "WORKING" || pointer.appliedVersionId !== staged.mandateVersionIds[0]) throw new Error(`pointer ${JSON.stringify(pointer).slice(0, 200)}`);
    /* re-applying is idempotent: no second broker acknowledgement */
    const again = await X.applyOutbox({ admin: W.fake, adapter, accountId: "paper-1", nowMs: t0 + 60000 });
    if (again.applied !== 0 || [...W.fake.docs.keys()].filter((k) => k.startsWith(`${W.fake.COL.brokerEvents}/`)).length !== 1) throw new Error("duplicate broker submission");
    const bar = (min, o, h, l, c, v) => ({ t: new Date(t0 + min * 60000).toISOString(), o, h, l, c, v });
    /* bar 1: the limit is touched but only 300 shares trade → 30 fill (10% participation); the remainder is cancelled and protection attaches for exactly 30 */
    let sim = await X.simulatePaperFills({ admin: W.fake, adapter, accountId: "paper-1", barsBySymbol: { AAA: [bar(1, 43.40, 43.60, 43.20, 43.50, 300)] }, nowMs: t0 + 120000 });
    if (sim.fills.length !== 1 || sim.fills[0].quantityUnits !== "30" || sim.fills[0].priceMicros !== "43250000") throw new Error(`partial ${JSON.stringify(sim)}`);
    if (sim.cancelledRemainders.length !== 1 || sim.cancelledRemainders[0].remainingUnits !== "56" || sim.protectionAttached[0].quantityUnits !== "30") throw new Error(`remainder ${JSON.stringify(sim)}`);
    os = await X.readOrderSet(W.fake, orderSetId);
    if (st("ENTRY") !== "CANCELLED" || st("TARGET") !== "WORKING" || st("STOP") !== "WORKING" || os.legs.find((l) => l.role === "STOP").quantityUnits !== "30") throw new Error(`protection legs ${os.legs.map((l) => `${l.role}:${l.status}:${l.quantityUnits}`)}`);
    pointer = W.fake.docs.get(`${W.fake.COL.activeMandates}/paper-1_AAA`);
    if (pointer.status !== "PROTECTED_RTH" || pointer.protectedQuantityUnits !== "30" || pointer.lossBoundaryPriceMicros !== "37250000") throw new Error(`pointer after fill ${JSON.stringify(pointer).slice(0, 300)}`);
    const pos = W.fake.docs.get(`${W.fake.COL.positions}/paper-1_AAA`);
    if (!pos || pos.quantityUnits !== "30" || pos.engineVersion !== "manager" || pos.decisionAuthority !== "SOL" || pos.protectionState !== "PROTECTED_RTH" || pos.costBasisMinor !== "129750") throw new Error(`position ${JSON.stringify(pos).slice(0, 300)}`);
    const res = W.fake.docs.get(`${W.fake.COL.reservationAccounts}/paper-1`);
    if (res.reservedNotionalMinor !== "0") throw new Error("reservation not released when the entry became terminal");
    let cons = await X.assertConservation("paper-1", { admin: W.fake });
    if (cons.pass !== true) throw new Error(`ledger ${JSON.stringify(cons)}`);
    const acct = W.fake.docs.get(`${W.fake.COL.accounts}/paper-1`);
    if (acct.balanceCents.cash !== 6000000 - 129750 || acct.balanceCents.positions !== 129750) throw new Error(`cash ${JSON.stringify(acct.balanceCents)}`);
    /* a duplicate bar replays nothing */
    sim = await X.simulatePaperFills({ admin: W.fake, adapter, accountId: "paper-1", barsBySymbol: { AAA: [bar(1, 43.40, 43.60, 43.20, 43.50, 300)] }, nowMs: t0 + 180000 });
    if (sim.fills.length !== 0) throw new Error("replayed bar produced a fill");
    /* bar 2: the target is touched (not the close): 30 sell at 49.00, the OCO sibling is cancelled, the position closes with realized P&L and a trade record */
    sim = await X.simulatePaperFills({ admin: W.fake, adapter, accountId: "paper-1", barsBySymbol: { AAA: [bar(5, 48.50, 49.10, 48.40, 48.70, 5000)] }, nowMs: t0 + 400000 });
    if (sim.fills.length !== 1 || sim.fills[0].role !== "TARGET" || sim.fills[0].priceMicros !== "49000000" || sim.closed.length !== 1) throw new Error(`exit ${JSON.stringify(sim)}`);
    os = await X.readOrderSet(W.fake, orderSetId);
    if (st("TARGET") !== "FILLED" || st("STOP") !== "CANCELLED" || os.status !== "CLOSED") throw new Error(`after exit ${os.legs.map((l) => `${l.role}:${l.status}`)} ${os.status}`);
    const pos2 = W.fake.docs.get(`${W.fake.COL.positions}/paper-1_AAA`);
    if (pos2.open !== false || pos2.quantityUnits !== "0" || pos2.realizedMinor !== String(30 * 4900 - 129750)) throw new Error(`closed position ${JSON.stringify(pos2).slice(0, 200)}`);
    const trades = [...W.fake.docs.entries()].filter(([k]) => k.startsWith(`${W.fake.COL.trades}/`)).map(([, v]) => v);
    if (trades.length !== 1 || trades[0].engineVersion !== "manager" || trades[0].realizedMinor !== String(30 * 4900 - 129750)) throw new Error("trade record");
    const fills = [...W.fake.docs.entries()].filter(([k]) => k.startsWith(`${W.fake.COL.fills}/`)).map(([, v]) => v);
    if (fills.length !== 2 || fills.some((f) => f.engineVersion !== "manager" || !f.bar)) throw new Error("fills not immutable events with bars");
    cons = await X.assertConservation("paper-1", { admin: W.fake });
    if (cons.pass !== true) throw new Error(`ledger after exit ${JSON.stringify(cons)}`);
    const acct2 = W.fake.docs.get(`${W.fake.COL.accounts}/paper-1`);
    if (acct2.balanceCents.cash !== 6000000 - 129750 + 147000 || acct2.balanceCents.positions !== 0 || acct2.balanceCents.realized_pl !== -(147000 - 129750)) throw new Error(`balances ${JSON.stringify(acct2.balanceCents)}`);
    if (W.fake.docs.get(`${W.fake.COL.activeMandates}/paper-1_AAA`).status !== "CLOSED") throw new Error("pointer not closed");
    /* the legacy lifecycle audit never judges manager records and refuses an identity-less one */
    const L = require("./_investorLedger");
    let code = null;
    try { L.assertManagerRecordIdentity({ symbol: "AAA" }); } catch (e) { code = e.code; }
    if (code !== "LEGACY_IDENTITY_REJECTED" || L.assertManagerRecordIdentity(pos2) !== true) throw new Error("identity check");
    return true;
  }));

  cases.push(fixture("executor_tick_expires_entries_freezes_on_operational_breach_and_never_loads_the_model_gateway", async () => {
    const X = require("./_investorExecution");
    const B = require("./_investorBroker");
    const fs = require("fs");
    /* the executor handler imports no model code */
    const src = fs.readFileSync(require.resolve("./investorExecution-background.js"), "utf8") + fs.readFileSync(require.resolve("./_investorExecution.js"), "utf8") + fs.readFileSync(require.resolve("./_investorBroker.js"), "utf8");
    if (/require\("\.\/_investorOpenai"\)/.test(src)) throw new Error("executor requires the model gateway");
    if (!/executor must not load the model gateway/.test(src)) throw new Error("load-time guard missing");
    const walk = eagerGraphReaches("./investorExecution-background.js", /_investorOpenai\.js$/);
    if (walk) throw new Error(`executor reaches the gateway eagerly via ${walk.join(" > ")}`);
    const W = stagingWorld();
    const AAA = W.proposal("AAA");
    const staged = await W.stage({ planClass: "EXPANSION", portfolioPlanProposal: { planClass: "EXPANSION" }, proposals: [AAA], verifiedProposalClaims: await W.verify([AAA]), activationSnapshot: W.snapshot() });
    if (staged.status !== "COMMITTED") throw new Error("staging");
    const t0 = Date.UTC(2026, 8, 3, 14);
    const adapter = B.createPaperAdapter({ admin: W.fake, now: () => t0 });
    await X.applyOutbox({ admin: W.fake, adapter, accountId: "paper-1", nowMs: t0 });
    /* after the last authorized session's close the unfilled entry expires and its reservation is released; a fresh tick cancels through the outbox */
    const late = Date.UTC(2026, 8, 5, 12);
    const tick = await X.tick({ admin: W.fake, adapter, accountId: "paper-1", control: { engineMode: "manager" }, nowMs: late, metrics: { brokerTruthAgeSeconds: 5 } });
    if (tick.expired.join(",") !== "AAA") throw new Error(`expiry ${JSON.stringify(tick.expired)}`);
    const pointer = W.fake.docs.get(`${W.fake.COL.activeMandates}/paper-1_AAA`);
    if (!["ENTRY_EXPIRED", "CANCELLED"].includes(pointer.status)) throw new Error(`pointer ${pointer.status}`);
    const os = await X.readOrderSet(W.fake, `os_${staged.mandateVersionIds[0]}`);
    if (os.legs.find((l) => l.role === "ENTRY").status !== "CANCELLED") throw new Error("entry not cancelled on expiry");
    if (W.fake.docs.get(`${W.fake.COL.reservationAccounts}/paper-1`).reservedNotionalMinor !== "0") throw new Error("expired entry kept its reservation");
    if (tick.conservation.pass !== true || tick.operational.allowExpansion !== true) throw new Error(`tick ${JSON.stringify(tick.operational)}`);
    /* stale broker truth is a hard breach: expansion freezes and the (inactive) emergency policy may only freeze */
    const tick2 = await X.tick({ admin: W.fake, adapter, accountId: "paper-1", control: { engineMode: "manager" }, nowMs: late + 60000, metrics: { brokerTruthAgeSeconds: 900 } });
    if (tick2.operational.allowExpansion !== false || tick2.operational.hardBreach !== true) throw new Error("stale truth allowed expansion");
    const ctrl = W.fake.docs.get(`${W.fake.COL.control}/control`);
    if (!ctrl || ctrl.buyState !== "FROZEN" || ctrl.freezeReason !== "STALE_BROKER_TRUTH") throw new Error(`control ${JSON.stringify(ctrl)}`);
    if (tick2.emergency && tick2.emergency.actions.some((a) => a.op !== "FREEZE_EXPANSION")) throw new Error("inactive emergency policy acted beyond freezing");
    /* the broker capability matrix is a record: configuration may only disable */
    if (B.selfCheck().pass !== true) throw new Error(`broker selfCheck ${JSON.stringify(B.selfCheck())}`);
    const live = B.createAlpacaAdapter({ env: {}, control: {} });
    let code = null;
    try { await live.getAccountSnapshot("paper-1"); } catch (e) { code = e.code; }
    if (code !== "LIVE_ADAPTER_DISABLED") throw new Error("live adapter reachable without the flag");
    if (B.adapterFor({ control: {}, env: {}, admin: W.fake }).adapter !== "paper") throw new Error("default adapter is not paper");
    return true;
  }));


  /* ═══ GROUP 7 — LEARNING, EVALS, ALERTS, POST-CLOSE, ARCHIVE (blueprint §16, §12.1; §21 commit 14) ═══ */

  /* Walk the EAGER require graph (top-level `const X = require("./…")` lines only —
     a lazy require inside a function is a runtime choice the caller controls) from
     a module and return the path to the first file matching `needle`, or null. */
  function eagerGraphReaches(entry, needle) {
    const fs = require("fs"), path = require("path");
    const edges = (file) => { let src = ""; try { src = fs.readFileSync(file, "utf8"); } catch { return []; }
      const out = []; for (const m of src.matchAll(/^(?:const|let|var)\s+[^=\n]+=\s*require\(\s*["'](\.\/[^"']+)["']\s*\)/gm)) out.push(m[1]); return out; };
    const start = require.resolve(entry);
    const seen = new Map([[start, [path.basename(start)]]]);
    const stack = [start];
    while (stack.length) {
      const file = stack.pop();
      for (const d of edges(file)) {
        let r; try { r = require.resolve(path.resolve(path.dirname(file), d)); } catch { continue; }
        if (seen.has(r)) continue;
        seen.set(r, [...seen.get(file), path.basename(r)]);
        if (needle.test(r)) return seen.get(r);
        stack.push(r);
      }
    }
    return null;
  }

  cases.push(fixture("non_model_handlers_never_reach_the_gateway_through_eager_imports", () => {
    const needle = /_investorOpenai\.js$/;
    for (const h of ["./investorExecution-background.js", "./investorPostclose-background.js", "./investorArchive-background.js"]) {
      const hit = eagerGraphReaches(h, needle);
      if (hit) throw new Error(`${h} reaches the gateway: ${hit.join(" > ")}`);
    }
    /* the walker itself sees edges: the ingest handler (Luna extraction) legitimately imports the gateway */
    if (!eagerGraphReaches("./investorIngest-background.js", needle)) throw new Error("the walker must find the ingest handler's gateway import");
    if (!eagerGraphReaches("./investorExecution-background.js", /_investorAdmin\.js$/)) throw new Error("the walker must follow the executor's own imports");
    return true;
  }));

  /* A daily series of `n` sessions after `startDate`, with closes stepping by `stepUsd`. */
  function dailySeries({ startDate = "2026-09-04", n = 70, startUsd = 100, stepUsd = 1, spread = 0.5 } = {}) {
    const out = [];
    let t = Date.parse(`${startDate}T00:00:00Z`), px = startUsd;
    for (let i = 0; i < n; i += 1) {
      const d = new Date(t);
      if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) { out.push({ date: d.toISOString().slice(0, 10), o: px, h: px + spread, l: px - spread, c: px, v: 1000000 }); px += stepUsd; }
      t += 86400000;
    }
    return out;
  }

  cases.push(fixture("forecast_record_freezes_the_declared_basis_and_resolves_by_the_point_in_time_clock", async () => {
    const L = require("./_investorLearning");
    const cutoffMs = Date.UTC(2026, 8, 4, 13);
    const proposal = { symbol: "AAA", decision: "BUY", action: { entry: { limitPriceMicros: "100000000" }, protection: { takeProfitPriceMicros: "115000000", lossBoundaryPriceMicros: "94000000" } },
      forecast: { horizonTradingDays: 20, basis: { returnConvention: "ARITHMETIC_PRICE_RETURN", corporateActionPolicy: "SPLIT_ADJUSTED_ONLY", conditionalOn: "OPPORTUNITY_FROM_REFERENCE_PRICE", estimatedRoundTripCostMicrosPerShare: "40000" },
        fillProbabilityByExpiryPpm: "700000", noFillOutcome: "NO_TRADE", outcomeBuckets: [{ label: "loss", probabilityPpm: "300000", returnBps: "-600" }, { label: "gain", probabilityPpm: "700000", returnBps: "1200" }], uncertaintyLevel: "MEDIUM" },
      invalidators: [{ predicateType: "guidance_cut", consequence: "EXIT" }] };
    const rec = L.outcomeRecordFor({ managerRunId: "run_1", tradingDate: "2026-09-04", decision: { symbol: "AAA", decision: "BUY", capitalRank: 1, reasonCode: null }, proposal, cutoffMs, benchmarkCloseMicros: "500000000", versions: { policyHash: "p1" } });
    if (rec.referencePriceMicros !== "100000000" || rec.referencePriceType !== "BUY_LIMIT") throw new Error("BUY reference must be the entry limit");
    if (rec.status !== "PENDING" || !rec.horizons.includes(20) || rec.returnConvention !== "ARITHMETIC_PRICE_RETURN") throw new Error("basis not frozen");
    /* a WATCH record references the cutoff close and has no target */
    const watch = L.outcomeRecordFor({ managerRunId: "run_1", tradingDate: "2026-09-04", decision: { symbol: "BBB", decision: "WATCH" }, referenceCloseMicros: "50000000", cutoffMs });
    if (watch.referencePriceType !== "CUTOFF_CLOSE" || watch.targetPriceMicros !== null) throw new Error("WATCH reference must be the cutoff close");
    /* resolution: bars strictly after the reference; 1d/5d/20d resolved once enough sessions exist, 60d still pending */
    const series = dailySeries({ startDate: "2026-09-04", n: 45, startUsd: 100, stepUsd: 1 });
    const bench = dailySeries({ startDate: "2026-09-04", n: 45, startUsd: 500, stepUsd: 0.5 });
    const r = L.resolveAgainstBars({ record: rec, series, benchmarkSeries: bench });
    if (!r.resolved.h1 || !r.resolved.h5 || !r.resolved.h20 || r.resolved.h60) throw new Error(`horizons ${JSON.stringify(Object.keys(r.resolved))}`);
    if (r.complete) throw new Error("60d horizon cannot be complete after 45 calendar days");
    /* first bar after 2026-09-04 is 2026-09-07 (the 5th and 6th are a weekend) at 101 → +100 bps */
    if (r.resolved.h1.returnBps !== "100") throw new Error(`h1 return ${r.resolved.h1.returnBps}`);
    if (!r.excursions || !r.excursions.targetTouchedOn) throw new Error("target 115 touched by a rising series must be recorded");
    if (r.excursions.boundaryTouchedOn) throw new Error("boundary never touched by a rising series");
    /* a bar dated on or before the reference day never counts */
    if (L.barsAfter(series, cutoffMs).some((b) => b.date <= "2026-09-04")) throw new Error("bars on the reference day leaked");
    return true;
  }));

  cases.push(fixture("counterfactual_and_anti_anchoring_rows_use_the_same_clock_and_costs", async () => {
    const L = require("./_investorLearning");
    const row = L.counterfactualRow({ managerRunId: "run_1", tradingDate: "2026-09-04", horizon: 20,
      selected: [{ symbol: "AAA", returnBps: "300" }, { symbol: "BBB", returnBps: "-100" }], unselected: [{ symbol: "CCC", returnBps: "500" }, { symbol: "DDD", returnBps: "100" }, { symbol: "EEE", returnBps: "0" }], benchmarkReturnBps: "50" });
    if (!row.counterfactualId || row.horizonTradingDays !== 20 || row.selectedCount !== 2 || row.unselectedCount !== 3) throw new Error("row identity");
    if (row.selectedMeanBps !== "100" || row.unselectedMeanBps !== "200") throw new Error(`means ${row.selectedMeanBps}/${row.unselectedMeanBps}`);
    if (row.selectedMinusUnselectedBps !== "-100" || row.selectedMinusBenchmarkBps !== "50") throw new Error(`lift ${row.selectedMinusUnselectedBps}/${row.selectedMinusBenchmarkBps}`);
    const sample = L.antiAnchoringSample({ symbols: ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"], tradingDate: "2026-09-04", heldSymbols: ["A"], ppm: 300000 });
    const again = L.antiAnchoringSample({ symbols: ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"], tradingDate: "2026-09-04", heldSymbols: ["A"], ppm: 300000 });
    if (JSON.stringify(sample) !== JSON.stringify(again)) throw new Error("the sample must be deterministic per date");
    if (sample.includes("A")) throw new Error("held names are never re-reviewed blind");
    const rec = L.reconcileAntiAnchoring({ prior: [{ symbol: "B", decision: "WATCH" }, { symbol: "C", decision: "BUY" }], independent: [{ symbol: "B", decision: "WATCH" }, { symbol: "C", decision: "ABSTAIN" }] });
    if (rec.sampled !== 2 || rec.agreementPpm !== "500000" || rec.disagreements.length !== 1 || rec.disagreements[0] !== "C") throw new Error(`reconcile ${JSON.stringify(rec)}`);
    return true;
  }));

  cases.push(fixture("freeze_is_idempotent_and_resolve_due_writes_forecasts_and_counterfactuals", async () => {
    const L = require("./_investorLearning");
    const fake = fakeAdmin();
    const cutoffMs = Date.UTC(2026, 8, 4, 13);
    const decisions = [{ symbol: "AAA", decision: "BUY", capitalRank: 1 }, { symbol: "BBB", decision: "WATCH" }, { symbol: "CCC", decision: "WATCH" }];
    const proposalsBySymbol = { AAA: { symbol: "AAA", decision: "BUY", action: { entry: { limitPriceMicros: "100000000" }, protection: { takeProfitPriceMicros: "120000000", lossBoundaryPriceMicros: "90000000" } } } };
    const closes = { AAA: "100000000", BBB: "100000000", CCC: "100000000" };
    const a = await L.freezeDecisionOutcomeRecords({ managerRunId: "run_1", tradingDate: "2026-09-04", cutoffMs, decisions, proposalsBySymbol, closeBySymbol: closes, benchmarkCloseMicros: "500000000", admin: fake });
    const b = await L.freezeDecisionOutcomeRecords({ managerRunId: "run_1", tradingDate: "2026-09-04", cutoffMs, decisions, proposalsBySymbol, closeBySymbol: closes, benchmarkCloseMicros: "500000000", admin: fake });
    if (a.written !== 3 || b.written !== 0 || b.skipped !== 3) throw new Error(`freeze ${JSON.stringify([a, b])}`);
    const up = dailySeries({ startDate: "2026-09-04", n: 12, startUsd: 100, stepUsd: 1 }), down = dailySeries({ startDate: "2026-09-04", n: 12, startUsd: 100, stepUsd: -1 });
    const series = { AAA: up, BBB: down, CCC: up, SPY: dailySeries({ startDate: "2026-09-04", n: 12, startUsd: 500, stepUsd: 0 }) };
    const out = await L.resolveDue({ admin: fake, seriesReader: async (s) => series[s] || [], benchmarkReader: async (s) => series[s] || [], nowMs: cutoffMs + 12 * 86400000 });
    if (out.examined !== 3 || out.advanced !== 3 || out.completed !== 0) throw new Error(`resolve ${JSON.stringify(out)}`);
    const fc = [...fake.docs.entries()].filter(([k]) => k.startsWith(`${fake.COL.forecasts}/`)).map(([, v]) => v);
    if (fc.some((f) => f.status !== "PENDING" || !f.resolved.h5)) throw new Error("h5 must be resolved and the record still pending");
    const cf = [...fake.docs.entries()].filter(([k]) => k.startsWith(`${fake.COL.counterfactuals}/`)).map(([, v]) => v);
    const h5 = cf.find((c) => c.horizonTradingDays === 5);
    if (!h5 || h5.selectedCount !== 1 || h5.unselectedCount !== 2) throw new Error(`counterfactual ${JSON.stringify(cf.map((c) => [c.horizonTradingDays, c.selectedCount, c.unselectedCount]))}`);
    if (BigInt(h5.selectedMinusUnselectedBps) <= 0n) throw new Error("the rising selected name must show positive lift against a mixed unselected set");
    /* a second pass with the same bars advances nothing */
    const again = await L.resolveDue({ admin: fake, seriesReader: async (s) => series[s] || [], benchmarkReader: async (s) => series[s] || [], nowMs: cutoffMs + 12 * 86400000 });
    if (again.advanced !== 0) throw new Error("no new bars, no advance");
    return true;
  }));

  cases.push(fixture("evals_grade_structure_not_price_and_promotion_needs_prospective_evidence_and_approval", async () => {
    const E = require("./_investorEvals");
    const cases2 = E.injectionCases();
    if (cases2.length < 2 || cases2.some((c) => c.kind !== "prompt_injection")) throw new Error("injection cases");
    const run = await E.runEvalSet({ cases: cases2, runner: async (c) => (c.tags.includes("tools") ? { ok: true, decision: "WATCH", toolCalls: [{ name: "submitOrder" }] } : { ok: true, decision: "BUY" }), label: "t" });
    if (run.promote !== false || run.passed !== 0 || run.cases !== 2) throw new Error(`eval run ${JSON.stringify({ promote: run.promote, passed: run.passed })}`);
    if (!run.results[0].problems.includes("forbidden_decision:BUY") || !run.results[1].problems.includes("disallowed_tool_call")) throw new Error(`problems ${JSON.stringify(run.results.map((r) => r.problems))}`);
    const st = E.stabilityScore({ runs: [{ decisionsBySymbol: { A: "BUY", B: "WATCH" } }, { decisionsBySymbol: { A: "BUY", B: "ABSTAIN" } }] });
    if (st.stablePpm !== "500000" || st.unstable[0].symbol !== "B") throw new Error(`stability ${JSON.stringify(st)}`);
    const pb = E.positionalBias({ rotations: [{ decisionsBySymbol: { A: "BUY", S: "WATCH" } }, { decisionsBySymbol: { A: "BUY", S: "BUY" } }], sentinels: [{ symbol: "S", expected: "WATCH" }] });
    if (!pb.biased || !pb.sentinelDrift[0].drift) throw new Error("a sentinel that flips with roster order is positional bias");
    if (!E.perturbationResponse({ baseline: { decision: "BUY" }, perturbed: { decision: "WATCH" } }).pass) throw new Error("perturbation");
    const gate0 = E.promotionGate({ challenger: { evalPassPpm: 990000, stablePpm: 990000 }, incumbent: { stablePpm: 980000 }, prospective: { sessions: 10 }, approval: null });
    if (gate0.promote || !gate0.reasons.includes("prospective_sessions_below_60") || !gate0.reasons.includes("explicit_version_approval_missing")) throw new Error(`gate0 ${JSON.stringify(gate0.reasons)}`);
    const gate1 = E.promotionGate({ challenger: { evalPassPpm: 990000, stablePpm: 990000 }, incumbent: { stablePpm: 980000 }, prospective: { sessions: 70, challengerMinusIncumbentNetBps: "5", downsideSafetyPass: true }, approval: { approved: true, by: "operator", versionId: "v2" } });
    if (!gate1.promote) throw new Error(`gate1 ${JSON.stringify(gate1.reasons)}`);
    const tl = E.trialLedgerSummary({ trials: [{ id: "a", inSampleScore: 3, outOfSampleScore: 1 }, { id: "b", inSampleScore: 2, outOfSampleScore: 3 }, { id: "c", inSampleScore: 1, outOfSampleScore: 2 }] });
    if (tl.trials !== 3 || tl.bestInSample !== "a" || tl.bestOutOfSample !== "b" || tl.note.indexOf("never a trade gate") < 0) throw new Error(`trials ${JSON.stringify(tl)}`);
    let code = null;
    try { E.buildEvalCase({ kind: "nope", input: {}, expected: {} }); } catch (e) { code = e.code; }
    if (code !== "EVAL_KIND_UNKNOWN") throw new Error("unknown kinds must be rejected");
    return true;
  }));

  cases.push(fixture("alerts_derive_from_conditions_with_stable_ids_and_resolve_only_when_the_condition_clears", async () => {
    const AL = require("./_investorAlerts");
    const fake = fakeAdmin();
    const nowMs = Date.UTC(2026, 8, 4, 21);
    const control = { buyState: "FROZEN", freezeReason: "operator", executorState: "PAUSED_SAFETY", executorPauseReason: "LEDGER_CONSERVATION_FAILED",
      lastManagerRun: { managerRunId: "run_1", status: "complete", noBuyReasons: [{ code: "COVERAGE_INCOMPLETE", missing: ["ZZZ"], completedCount: 303, eligibleCount: 304 }], maintenance: { deadlineMissed: true, actionRequired: [{ symbol: "AAA", reason: "no_mandate" }] } } };
    const portfolio = { positions: [{ symbol: "AAA", quantityUnits: "10", lossBoundaryPriceMicros: null, protectionState: "PROTECTION_PENDING" }, { symbol: "BBB", quantityUnits: "5", lossBoundaryPriceMicros: "1", protectionState: "PROTECTION_PENDING" }], activeMandates: [{ symbol: "AAA", status: "ACTION_REQUIRED" }] };
    const derived = AL.deriveConditions({ control, portfolio, nowMs });
    const ids = derived.map((c) => c.conditionId);
    for (const want of ["coverage_incomplete:run_1", "manager_missed_deadline:run_1", "action_required:AAA", "buys_frozen", "ledger_conservation_failed", "unprotected_position:AAA", "protection_pending:BBB"]) if (!ids.includes(want)) throw new Error(`missing ${want} in ${ids.join(",")}`);
    if (new Set(ids).size !== ids.length) throw new Error("condition ids must be unique per derivation");
    if (derived.some((c) => !["info", "warning", "critical"].includes(c.severity) || !c.action)) throw new Error("every alert carries a severity and an action");
    const first = await AL.upsertAlerts({ admin: fake, accountId: "paper-1", derived, nowMs });
    if (first.raised !== derived.length || first.resolved !== 0) throw new Error(`first ${JSON.stringify(first)}`);
    const ack = await AL.acknowledge({ admin: fake, conditionId: "buys_frozen", by: "operator", nowMs: nowMs + 1000 });
    if (!ack.acknowledged || !ack.stillActive) throw new Error("acknowledgement must not resolve");
    /* the same conditions again: nothing new, nothing resolved, occurrences grow */
    const second = await AL.upsertAlerts({ admin: fake, accountId: "paper-1", derived, nowMs: nowMs + 60000 });
    if (second.raised !== 0 || second.kept !== derived.length || second.resolved !== 0) throw new Error(`second ${JSON.stringify(second)}`);
    const frozen = fake.docs.get(`${fake.COL.alerts}/buys_frozen`);
    if (frozen.occurrences !== 2 || frozen.active !== true || frozen.acknowledgedBy !== "operator") throw new Error(`doc ${JSON.stringify(frozen)}`);
    /* the freeze clears: only that alert resolves, by condition_cleared */
    const cleared = AL.deriveConditions({ control: { ...control, buyState: "ACTIVE" }, portfolio, nowMs: nowMs + 120000 });
    const third = await AL.upsertAlerts({ admin: fake, accountId: "paper-1", derived: cleared, nowMs: nowMs + 120000 });
    if (third.resolved !== 1 || fake.docs.get(`${fake.COL.alerts}/buys_frozen`).active !== false || fake.docs.get(`${fake.COL.alerts}/buys_frozen`).resolvedBy !== "condition_cleared") throw new Error(`third ${JSON.stringify(third)}`);
    const active = await AL.listActive({ admin: fake, accountId: "paper-1" });
    if (active.length !== derived.length - 1 || active[0].severity !== "critical") throw new Error("active list sorted critical first");
    return true;
  }));

  cases.push(fixture("kpi_definitions_are_versioned_and_the_self_check_passes", async () => {
    const K = require("./_investorKpi");
    const r = K.selfCheck();
    if (!r || r.pass !== true) throw new Error(`kpi selfCheck ${JSON.stringify(r).slice(0, 400)}`);
    if (!K.KPI_DEFINITIONS || Object.keys(K.KPI_DEFINITIONS).length < 12) throw new Error("KPI registry too small");
    const row = K.dailyAggregate({ date: "2026-09-04", accountId: "paper-1", inputs: { nowMs: 1 } });
    if (!row || !row.kpiVersion || !Array.isArray(row.missing)) throw new Error("dailyAggregate must report what it could not compute");
    return true;
  }));

  cases.push(fixture("postclose_marks_records_nav_freezes_decisions_resolves_and_derives_alerts_without_a_model", async () => {
    const PC = require("./investorPostclose-background");
    if (Object.keys(require.cache).some((k) => /_investorOpenai\.js$/.test(k)) && !PC.runPostclose) throw new Error("gateway loaded");
    const fake = fakeAdmin();
    const tradingDate = "2026-09-04", nowMs = Date.UTC(2026, 8, 4, 21, 30);
    fake.docs.set(`${fake.COL.accounts}/paper-1`, { accountId: "paper-1", balanceCents: { cash: 5000000, reserved: 0 }, balanceRevision: 3, startingNavCents: 6000000 });
    fake.docs.set(`${fake.COL.positions}/paper-1_AAA`, { accountId: "paper-1", symbol: "AAA", open: true, qty: 100, quantityUnits: "100", entryPriceUsd: 95, costBasisCents: 950000, lastMarkUsd: 97, lossBoundaryPriceMicros: "90000000", protectionState: "PROTECTED_RTH", engineVersion: "manager" });
    fake.docs.set(`${fake.COL.control}/control`, { accountId: "paper-1", engineMode: "manager", buyState: "ACTIVE",
      lastManagerRun: { managerRunId: "run_1", status: "complete", tradingDate, cutoffMs: Date.UTC(2026, 8, 4, 13), policyHash: "p", universeHash: "u", universeVersion: "v", contextManifestHash: "c",
        coverage: { ok: true, completedCount: 304, eligibleCount: 304, duplicates: [] }, research: { completed: 12 }, elapsedMs: 1500000, activation: { status: "COMMITTED", mandateVersionIds: ["mv_1"] } } });
    const SC = require("./_investorStorageCodec");
    for (const [sym, decision] of [["AAA", "HOLD"], ["BBB", "BUY"], ["CCC", "WATCH"]]) fake.docs.set(`${fake.COL.managerDecisions}/run_1_${sym}`, SC.encode({ schemaVersion: "manager-decision.v1", managerRunId: "run_1", symbol: sym, decision, reasonCode: null, capitalRank: decision === "BUY" ? 1 : null, tradingDate, accountId: "paper-1" }));
    fake.docs.set(`${fake.COL.portfolioPlans}/plan_1`, { planId: "plan_1", managerRunId: "run_1", status: "COMMITTED", mandateVersionIds: ["mv_1"], proposal: { mandates: [{ symbol: "BBB", decision: "BUY", action: { entry: { limitPriceMicros: "50000000" }, protection: { takeProfitPriceMicros: "60000000", lossBoundaryPriceMicros: "46000000" } } }] } });
    fake.docs.set(`${fake.COL.activationEnvelopes}/mv_1`, { symbol: "BBB", mandateVersionId: "mv_1", authorizedQuantityUnits: "10", expectedTerminalPriceMicros: "58000000" });
    const series = { AAA: dailySeries({ startDate: "2026-08-20", n: 16, startUsd: 90, stepUsd: 1 }), BBB: dailySeries({ startDate: "2026-08-20", n: 16, startUsd: 45, stepUsd: 0.5 }), CCC: dailySeries({ startDate: "2026-08-20", n: 16, startUsd: 20, stepUsd: 0 }), SPY: dailySeries({ startDate: "2026-08-20", n: 16, startUsd: 500, stepUsd: 1 }) };
    const reader = async (s) => ({ series: series[s] || [] });
    const aaaClose = PC.finalBar(series.AAA, tradingDate);
    if (!aaaClose || aaaClose.date !== tradingDate || !aaaClose.exact) throw new Error(`final bar ${JSON.stringify(aaaClose)}`);
    const out = await PC.runPostclose({ accountId: "paper-1", tradingDate, admin: fake, seriesReader: reader, nowMs });
    if (!out.ok) throw new Error(`postclose errors ${JSON.stringify(out.errors)}`);
    if (out.steps.marks.marked !== 1 || fake.docs.get(`${fake.COL.positions}/paper-1_AAA`).lastMarkUsd !== aaaClose.c) throw new Error(`marks ${JSON.stringify(out.steps.marks)}`);
    const nav = fake.docs.get(`${fake.COL.navMarks}/paper-1_${tradingDate}`);
    if (!nav || !nav.finalMark || nav.finalMark.source !== "postclose" || BigInt(nav.finalMark.navMinor) !== 5000000n + BigInt(Math.round(aaaClose.c * 100)) * 100n) throw new Error(`nav ${JSON.stringify(nav)}`);
    if (out.steps.freeze.written !== 3 || out.steps.freeze.proposals !== 1 || out.steps.freeze.envelopes !== 1) throw new Error(`freeze ${JSON.stringify(out.steps.freeze)}`);
    const bbb = fake.docs.get(`${fake.COL.forecasts}/${require("./_investorLearning").forecastIdFor({ managerRunId: "run_1", symbol: "BBB" })}`);
    if (!bbb || bbb.referencePriceMicros !== "50000000" || bbb.referencePriceType !== "BUY_LIMIT" || bbb.benchmark.referenceCloseMicros == null) throw new Error(`BBB record ${JSON.stringify(bbb).slice(0, 300)}`);
    if (out.steps.resolve.examined !== 3 || out.steps.resolve.advanced !== 0) throw new Error(`nothing after the reference day can resolve on the same evening: ${JSON.stringify(out.steps.resolve)}`);
    const kpi = fake.docs.get(`${fake.COL.kpiDaily}/paper-1_${tradingDate}`);
    if (!kpi || !kpi.kpis || kpi.errors.length) throw new Error(`kpi row ${JSON.stringify(kpi && kpi.errors)}`);
    if (!kpi.kpis.universeCoverage || kpi.kpis.universeCoverage.ppm !== "1000000") throw new Error(`coverage kpi ${JSON.stringify(kpi.kpis.universeCoverage)}`);
    if (!kpi.kpis.investmentReturn || kpi.kpis.investmentReturn.observations !== 0 || kpi.kpis.investmentReturn.timeWeightedReturnBps !== null || kpi.kpis.investmentReturn.smallSampleWarning !== true) throw new Error(`a single NAV mark yields no return and must warn, not pretend: ${JSON.stringify(kpi.kpis.investmentReturn)}`);
    if (!kpi.kpis.unitEconomics || kpi.kpis.unitEconomics.costMinor !== "0" || !kpi.kpis.completionLatency || !kpi.kpis.integrity || kpi.kpis.integrity.pass !== true) throw new Error(`unit/latency/integrity ${JSON.stringify([kpi.kpis.unitEconomics, kpi.kpis.completionLatency, kpi.kpis.integrity])}`);
    if (!kpi.missing.includes("triggerCapture") || !kpi.missing.includes("calibration")) throw new Error("KPIs without inputs must be listed as missing, never fabricated");
    if (out.steps.alerts.active < 0 || !Array.isArray(out.steps.alerts.conditions)) throw new Error("alerts step");
    const ctrl = fake.docs.get(`${fake.COL.control}/control`);
    if (ctrl.lastPostcloseDate !== tradingDate || !ctrl.lastPostclose || ctrl.lastPostclose.ok !== true) throw new Error("control must record completion");
    /* the same evening again is idempotent for the frozen records */
    const again = await PC.runPostclose({ accountId: "paper-1", tradingDate, admin: fake, seriesReader: reader, nowMs: nowMs + 1000 });
    if (!again.ok || again.steps.freeze.written !== 0 || again.steps.freeze.skipped !== 3) throw new Error(`rerun ${JSON.stringify(again.steps.freeze)}`);
    /* a failing step withholds the completion date */
    const broken = fakeAdmin();
    broken.docs.set(`${broken.COL.control}/control`, { accountId: "paper-1", engineMode: "manager" });
    broken.docs.set(`${broken.COL.accounts}/paper-1`, { accountId: "paper-1", balanceCents: { cash: 1 } });
    const bad = await PC.runPostclose({ accountId: "paper-1", tradingDate, admin: broken, seriesReader: async () => { throw new Error("feed down"); }, nowMs, kpiInputs: null });
    if (bad.steps.freeze.skipped !== true || bad.steps.freeze.reason !== "no_manager_run") throw new Error(`no run → skip ${JSON.stringify(bad.steps.freeze)}`);
    return true;
  }));

  cases.push(fixture("archive_exports_the_day_bounded_writes_a_retention_manifest_and_deletes_nothing", async () => {
    const AR = require("./investorArchive-background");
    const CS = require("./_investorContentStore");
    const fake = fakeAdmin();
    const tradingDate = "2026-09-04";
    const { startMs, endMs } = AR.dayWindowMs(tradingDate);
    if (!(startMs < endMs) || endMs - startMs !== 86400000) throw new Error("day window");
    fake.docs.set(`${fake.COL.control}/control`, { accountId: "paper-1", engineMode: "manager" });
    fake.docs.set(`${fake.COL.brokerEvents}/be_1`, { brokerEventId: "be_1", accountId: "paper-1", atMs: startMs + 3600000, kind: "ACK" });
    fake.docs.set(`${fake.COL.brokerEvents}/be_2`, { brokerEventId: "be_2", accountId: "paper-1", atMs: startMs - 1, kind: "ACK" });          /* yesterday */
    fake.docs.set(`${fake.COL.brokerEvents}/be_3`, { brokerEventId: "be_3", accountId: "other", atMs: startMs + 5, kind: "ACK" });           /* another account */
    fake.docs.set(`${fake.COL.fills}/f_1`, { fillId: "f_1", accountId: "paper-1", atMs: startMs + 7200000, symbol: "AAA" });
    fake.docs.set(`${fake.COL.mandateEvents}/mv_1_0001`, { mandateVersionId: "mv_1", accountId: "paper-1", atMs: startMs + 100, sequence: 1 });
    fake.docs.set(`${fake.COL.jobs}/j_1`, { jobId: "j_1", finishedAtMs: startMs + 200, status: "complete" });
    fake.docs.set(`${fake.COL.modelRequests}/r_1`, { requestId: "r_1", updatedAtMs: startMs + 300 });
    fake.docs.set(`${fake.COL.evidenceDeltas}/d_1`, { symbol: "AAA", firstSeenAtMs: startMs + 400 });
    const ad = CS.memoryAdapter(), st = CS.memoryManifestStore();
    const store = { assertAvailable: () => true, putJson: (kind, value, opts) => CS.putJson(kind, value, { ...opts, adapter: ad, store: st }) };
    const before = fake.docs.size;
    const run = await AR.runArchive({ accountId: "paper-1", tradingDate, admin: fake, contentStore: store, nowMs: endMs + 3600000 });
    if (run.status !== "complete") throw new Error(`archive ${JSON.stringify(run.errors)}`);
    if (run.counts.brokerEvents.exported !== 1 || run.counts.fills.exported !== 1 || run.counts.mandateEvents.exported !== 1 || run.counts.jobs.exported !== 1 || run.counts.modelRequests.exported !== 1 || run.counts.evidenceDeltas.exported !== 1 || run.counts.executionOutbox.exported !== 0) throw new Error(`counts ${JSON.stringify(run.counts)}`);
    if (!run.export.exported || !run.export.contentHash || run.export.encoding !== "gzip" || run.export.records !== 6) throw new Error(`export ${JSON.stringify(run.export)}`);
    if (run.deleted !== 0 || fake.docs.size !== before + 1) throw new Error("the archive must add its run record (the control doc already exists) and delete nothing");
    if (!run.retention.fills || run.retention.fills.class !== "audit_chain" || run.retention.fills.purgeEligibleFrom !== null || run.retention.fills.purgeAllowedAfterExport) throw new Error("fills are audit chain: never purgeable");
    if (run.retention.brokerEvents.purgeEligibleFrom !== "2033-09-02" || !run.retention.brokerEvents.exportedInBundle) throw new Error(`retention ${JSON.stringify(run.retention.brokerEvents)}`);
    const stored = await CS.getJson(run.export.contentHash, { adapter: ad, store: st });
    const bundle = stored && stored.json ? stored.json : stored;
    if (!bundle || bundle.tradingDate !== tradingDate || bundle.collections.brokerEvents.length !== 1 || bundle.collections.brokerEvents[0].brokerEventId !== "be_1") throw new Error("stored bundle must round-trip through the content hash");
    const ctrl = fake.docs.get(`${fake.COL.control}/control`);
    if (ctrl.lastArchiveDate !== tradingDate || !ctrl.lastArchiveSummary || ctrl.lastArchiveSummary.status !== "complete") throw new Error("control must record the archive");
    /* the same day again: the bundle already exists, the run is idempotent */
    const again = await AR.runArchive({ accountId: "paper-1", tradingDate, admin: fake, contentStore: store, nowMs: endMs + 7200000 });
    if (again.export.contentHash !== run.export.contentHash) throw new Error("same day, same bytes, same hash");
    /* a content store that is not configured fails closed without touching the data */
    const unavailable = await AR.runArchive({ accountId: "paper-1", tradingDate, admin: fake, contentStore: { assertAvailable: () => { throw Object.assign(new Error("no bucket"), { code: "CONTENT_STORE_UNAVAILABLE" }); }, putJson: () => { throw new Error("must not be called"); } }, nowMs: endMs + 9000000 });
    if (unavailable.export.exported !== false || unavailable.export.reason !== "CONTENT_STORE_UNAVAILABLE") throw new Error(`unavailable ${JSON.stringify(unavailable.export)}`);
    return true;
  }));


  /* ═══ GROUP 8 — API v2, SESSIONS, FIVE-VIEW CONSOLE (blueprint §11, §14; §21 commit 15) ═══ */

  /** A seeded manager-mode world for the v2 API: control, account, a held protected position, a working entry, decisions, a run, alerts, deltas. */
  function apiWorld({ accountMode = "PAPER_AI", writerEpoch = 1, attested = true } = {}) {
    const fake = fakeAdmin();
    const C = fake.COL;
    const nowMs = Date.UTC(2026, 8, 4, 15, 30);
    const commit = process.env.COMMIT_REF || process.env.DEPLOY_ID || "local";
    const P = require("./_investorPolicy");
    const policy = P.loadActiveSync({});
    const run = { managerRunId: "run_premarket_manager_paper-1_2026-09-04", status: "complete", tradingDate: "2026-09-04", accountId: "paper-1", universeVersion: "v6", universeHash: "u".repeat(64), eligibleCount: 304, decisionCount: 304, byDecision: { WATCH: 300, BUY: 2, HOLD: 2 },
      buys: [{ symbol: "AAA", capitalRank: 1 }], contextManifestHash: "c".repeat(64), policyHash: policy.policyHash, cutoffMs: nowMs - 6 * 3600000, startedAtMs: nowMs - 5 * 3600000, completedAtMs: nowMs - 4 * 3600000, elapsedMs: 3600000,
      coverage: { ok: true, completedCount: 304, eligibleCount: 304, repaired: 0 }, maintenance: { actionable: 1, actionRequired: [], staged: null }, research: { completed: 3, failed: 0, deferred: 1, ranges: null }, activation: { status: "COMMITTED", planId: "plan_1", activationSnapshotId: "act_1", mandateVersionIds: ["mv_AAA_1"] }, noBuyReasons: [], costMinor: "420", stage: "complete" };
    fake.docs.set(`${C.control}/control`, { accountId: "paper-1", engineMode: writerEpoch > 0 ? "manager" : "legacy", accountMode, writerEpoch, controlVersion: 3, fixturesPass: attested, fixturesCommit: attested ? commit : "other", fixturesCount: 200, buyState: "OPEN", managerState: "ENABLED", emergencyState: "CLEAR", executorState: "MONITORING",
      lastManagerRunId: run.managerRunId, lastManagerRun: run, lastManagerRunDate: "2026-09-04", lastIngestPass: { finishedAtMs: nowMs - 8 * 3600000, companies: 304, freshness: { finishedBeforeFreeze: true } }, lastExecutionTick: { atMs: nowMs - 60000, reasons: [], conservation: { pass: true, discrepancies: [] }, outboxApplied: 1, fills: { fills: 0 } } });
    fake.docs.set(`${C.accounts}/paper-1`, { accountId: "paper-1", balanceCents: { cash: 4950000, reserved: 100000, positions: 950000, contributed_capital: -6000000 }, balanceRevision: 7, portfolioVersion: 7, writerEpoch, startingNavCents: 6000000 });
    fake.docs.set(`${C.ledger}/genesis_paper-1`, { txnId: "genesis_paper-1", accountId: "paper-1", kind: "capital_contribution", atMs: nowMs - 86400000 * 30, postedAt: new Date(nowMs - 86400000 * 30).toISOString(), legs: [{ account: "cash", amountCents: 6000000 }, { account: "contributed_capital", amountCents: -6000000 }] });
    /* the journal explains every balance: the BBB purchase and the AAA reservation */
    fake.docs.set(`${C.ledger}/buy_BBB`, { txnId: "buy_BBB", accountId: "paper-1", kind: "manager_fill", atMs: nowMs - 86400000, postedAt: new Date(nowMs - 86400000).toISOString(), legs: [{ account: "cash", amountCents: -950000 }, { account: "positions", amountCents: 950000 }], meta: { symbol: "BBB", fillId: "fill_BBB_1", orderSetId: "os_mv_BBB_1", mandateVersionId: "mv_BBB_1" } });
    fake.docs.set(`${C.ledger}/reserve_AAA`, { txnId: "reserve_AAA", accountId: "paper-1", kind: "reservation", atMs: nowMs - 3600000, postedAt: new Date(nowMs - 3600000).toISOString(), legs: [{ account: "cash", amountCents: -100000 }, { account: "reserved", amountCents: 100000 }], meta: { symbol: "AAA", mandateVersionId: "mv_AAA_1" } });
    /* BBB: held, protected by an applied mandate; AAA: a working entry with reservation */
    fake.docs.set(`${C.positions}/paper-1_BBB`, { accountId: "paper-1", symbol: "BBB", open: true, qty: 100, quantityUnits: "100", entryPriceUsd: 95, costBasisCents: 950000, costBasisMinor: "950000", lastMarkUsd: 97, lastMarkAt: new Date(nowMs).toISOString(), lossBoundaryPriceMicros: "90000000", takeProfitPriceMicros: "110000000", protectionState: "PROTECTED_RTH", protectionAcknowledged: true, engineVersion: "manager", mandateVersionId: "mv_BBB_1", positionLifecycleId: "plc_BBB", sector: "software" });
    fake.docs.set(`${C.activeMandates}/paper-1_BBB`, { accountId: "paper-1", symbol: "BBB", mandateSeriesId: "paper-1_BBB", desiredVersion: 1, desiredVersionId: "mv_BBB_1", appliedVersion: 1, appliedVersionId: "mv_BBB_1", status: "PROTECTED_RTH", entryState: "FILLED", protectionState: "PROTECTED_RTH", planClass: "EXPANSION", decision: "BUY", capitalRank: 1, expiresAtMs: nowMs + 20 * 86400000, lossBoundaryPriceMicros: "90000000", takeProfitPriceMicros: "110000000", thesisHealth: "INTACT", updatedAtMs: nowMs - 86400000 });
    fake.docs.set(`${C.orderSets}/os_mv_BBB_1`, { schemaVersion: "order-set.v1", orderSetId: "os_mv_BBB_1", accountId: "paper-1", symbol: "BBB", mandateVersionId: "mv_BBB_1", status: "WORKING", legIds: ["os_mv_BBB_1_TARGET", "os_mv_BBB_1_STOP"], reservationId: "res_BBB", createdAtMs: nowMs - 86400000, planId: "plan_0", brokerGroupId: "pg_bbb",
      legs: [{ legId: "os_mv_BBB_1_TARGET", role: "TARGET", side: "sell", type: "LIMIT", priceMicros: "110000000", quantityUnits: "100", remainingUnits: "100", status: "WORKING", timeInForce: "GTC", ocoGroup: "oco_BBB" }, { legId: "os_mv_BBB_1_STOP", role: "STOP", side: "sell", type: "STOP", stopMicros: "90000000", quantityUnits: "100", remainingUnits: "100", status: "WORKING", timeInForce: "GTC", ocoGroup: "oco_BBB" }] });
    fake.docs.set(`${C.orderLegs}/os_mv_BBB_1_TARGET`, { legId: "os_mv_BBB_1_TARGET", orderSetId: "os_mv_BBB_1", accountId: "paper-1", symbol: "BBB", role: "TARGET", side: "sell", type: "LIMIT", priceMicros: "110000000", quantityUnits: "100", remainingUnits: "100", filledUnits: "0", status: "WORKING", timeInForce: "GTC", ocoGroup: "oco_BBB", workingSinceMs: nowMs - 86400000 });
    fake.docs.set(`${C.orderLegs}/os_mv_BBB_1_STOP`, { legId: "os_mv_BBB_1_STOP", orderSetId: "os_mv_BBB_1", accountId: "paper-1", symbol: "BBB", role: "STOP", side: "sell", type: "STOP", stopMicros: "90000000", quantityUnits: "100", remainingUnits: "100", filledUnits: "0", status: "WORKING", timeInForce: "GTC", ocoGroup: "oco_BBB", workingSinceMs: nowMs - 86400000 });
    fake.docs.set(`${C.activationEnvelopes}/mv_BBB_1`, { symbol: "BBB", mandateVersionId: "mv_BBB_1", authorizedQuantityUnits: "100", proposedQuantityUnits: "120", reservedNotionalMinor: "950000", plannedLossAtBoundaryMinor: "50000", bindingGapStressLossMinor: "90000", bindingConstraint: "cashCapacity", validatedAtMs: nowMs - 86400000, portfolioPlanId: "plan_0", reservationId: "res_BBB" });
    fake.docs.set(`${C.activeMandates}/paper-1_AAA`, { accountId: "paper-1", symbol: "AAA", mandateSeriesId: "paper-1_AAA", desiredVersion: 1, desiredVersionId: "mv_AAA_1", appliedVersion: 1, appliedVersionId: "mv_AAA_1", status: "WORKING", entryState: "WORKING", planClass: "EXPANSION", decision: "BUY", capitalRank: 1, expiresAtMs: nowMs + 3 * 86400000, updatedAtMs: nowMs - 3600000 });
    fake.docs.set(`${C.orderSets}/os_mv_AAA_1`, { schemaVersion: "order-set.v1", orderSetId: "os_mv_AAA_1", accountId: "paper-1", symbol: "AAA", mandateVersionId: "mv_AAA_1", status: "WORKING", legIds: ["os_mv_AAA_1_ENTRY", "os_mv_AAA_1_TARGET", "os_mv_AAA_1_STOP"], reservationId: "res_AAA", createdAtMs: nowMs - 3600000, planId: "plan_1",
      legs: [{ legId: "os_mv_AAA_1_ENTRY", role: "ENTRY", side: "buy", type: "LIMIT", priceMicros: "50000000", quantityUnits: "20", remainingUnits: "20", status: "WORKING", timeInForce: "DAY", sessionDates: ["2026-09-04", "2026-09-05"] }, { legId: "os_mv_AAA_1_TARGET", role: "TARGET", side: "sell", type: "LIMIT", priceMicros: "60000000", quantityUnits: "20", remainingUnits: "20", status: "ARMED", activatesOn: "ENTRY_FILL" }, { legId: "os_mv_AAA_1_STOP", role: "STOP", side: "sell", type: "STOP", stopMicros: "46000000", quantityUnits: "20", remainingUnits: "20", status: "ARMED", activatesOn: "ENTRY_FILL" }] });
    for (const l of fake.docs.get(`${C.orderSets}/os_mv_AAA_1`).legs) fake.docs.set(`${C.orderLegs}/${l.legId}`, { ...l, orderSetId: "os_mv_AAA_1", accountId: "paper-1", symbol: "AAA", filledUnits: "0", workingSinceMs: nowMs - 3600000 });
    fake.docs.set(`${C.capitalReservations}/res_AAA`, { schemaVersion: "capital-reservation.v1", reservationId: "res_AAA", accountId: "paper-1", symbol: "AAA", mandateVersionId: "mv_AAA_1", status: "ACTIVE", reservedNotionalMinor: "100000", plannedLossMinor: "8000", stressLossMinor: "12000", createdAtMs: nowMs - 3600000 });
    fake.docs.set(`${C.reservationAccounts}/paper-1`, { accountId: "paper-1", version: 2, reservedNotionalMinor: "100000", reservedPlannedLossMinor: "8000", reservedStressLossMinor: "12000", committedPortfolioPlanId: "plan_1", updatedAtMs: nowMs - 3600000 });
    fake.docs.set(`${C.activationEnvelopes}/mv_AAA_1`, { symbol: "AAA", mandateVersionId: "mv_AAA_1", authorizedQuantityUnits: "20", proposedQuantityUnits: "20", reservedNotionalMinor: "100000", plannedLossAtBoundaryMinor: "8000", bindingGapStressLossMinor: "12000", bindingConstraint: "plannedLossCapacity", validatedAtMs: nowMs - 3600000, portfolioPlanId: "plan_1", reservationId: "res_AAA", proposalHash: "p".repeat(64) });
    fake.docs.set(`${C.portfolioPlans}/plan_1`, { planId: "plan_1", planClass: "EXPANSION", status: "COMMITTED", accountId: "paper-1", managerRunId: run.managerRunId, activationSnapshotId: "act_1", mandateVersionIds: ["mv_AAA_1"], symbols: ["AAA"], committedAtMs: nowMs - 3600000 });
    const SC = require("./_investorStorageCodec");
    const U = require("./_investorUniverse");
    const symbols = U.tradeTier.map((r) => r.symbol);
    symbols.forEach((sym, i) => fake.docs.set(`${C.managerDecisions}/${run.managerRunId}_${sym}`, SC.encode({ schemaVersion: "manager-decision.v1", managerRunId: run.managerRunId, symbol: sym, decision: i === 0 ? "BUY" : i === 1 ? "HOLD" : "WATCH", reasonCode: null, reason: i === 0 ? "attractive after-cost opportunity" : "no edge yet", capitalRank: i === 0 ? 1 : null, fundingState: i === 0 ? "FUNDED" : "NOT_APPLICABLE", reviewDirective: "REUSE_CURRENT", changedSincePrior: i < 3, held: i === 1, eligible: true, offRoster: false, source: i === 1 ? "holding_analysis" : "review", tradingDate: "2026-09-04", accountId: "paper-1", asOfMs: nowMs - 4 * 3600000 })));
    fake.docs.set(`${C.dossiers}/${symbols[0]}`, { symbol: symbols[0], currentVersionId: "dv_1", asOfMs: nowMs - 8 * 3600000, updatedAtMs: nowMs - 8 * 3600000, lastManagerReviewAtMs: nowMs - 4 * 3600000, standingView: { decision: "BUY", status: "BUY", asOfMs: nowMs - 4 * 3600000 }, pendingDeltaCount: 1 });
    fake.docs.set(`${C.evidenceDeltas}/delta_${symbols[0]}_1`, { deltaId: `delta_${symbols[0]}_1`, eventId: `delta_${symbols[0]}_1`, symbol: symbols[0], safetyClass: "routine", eventClass: "guidance_update", title: "Company raised full-year guidance", firstSeenAtMs: nowMs - 2 * 3600000, publishedAtMs: nowMs - 3 * 3600000, managerMateriality: "pending", verified: true, sourceId: "sec.latest", reasons: ["guidance"] });
    fake.docs.set(`${C.alerts}/buys_frozen`, { conditionId: "buys_frozen", code: "buys_frozen", severity: "warning", title: "New buys frozen", action: "see the freeze reason", detail: null, accountId: "paper-1", active: true, raisedAtMs: nowMs - 3600000, lastSeenAtMs: nowMs - 60000, acknowledgedAtMs: null, acknowledgedBy: null, resolvedAtMs: null, occurrences: 3 });
    fake.docs.set(`${C.jobs}/focused_research__AAA_2026-09-04`, { jobId: "focused_research__AAA_2026-09-04", task: "focused_research", status: "queued", priority: 300, enqueuedAtMs: nowMs - 600000, dueAtMs: nowMs - 600000, attempts: 0, payload: { symbol: "AAA", accountId: "paper-1" }, targetFunction: "investorManager-background" });
    fake.docs.set(`${C.navMarks}/paper-1_2026-09-03`, { accountId: "paper-1", date: "2026-09-03", finalMark: { navMinor: "5990000", source: "postclose", atMs: nowMs - 86400000 } });
    fake.docs.set(`${C.navMarks}/paper-1_2026-09-02`, { accountId: "paper-1", date: "2026-09-02", finalMark: { navMinor: "6000000", source: "postclose", atMs: nowMs - 2 * 86400000 } });
    fake.docs.set(`${C.costs}/openai_2026-09-04`, { spentMinor: 420, reservedMinor: 0, calls: 12, byRole: { manager: { model: "gpt-5.6-sol", calls: 4, spentMinor: 380, inputTokens: 900000, outputTokens: 40000 }, facts: { model: "gpt-5.6-luna", calls: 8, spentMinor: 40 } } });
    const fcId = require("./_investorLearning").forecastIdFor({ managerRunId: run.managerRunId, symbol: symbols[0] });
    fake.docs.set(`${C.forecasts}/${fcId}`, { forecastId: fcId, managerRunId: run.managerRunId, symbol: symbols[0], status: "PENDING", decision: "BUY", referencePriceMicros: "50000000", referencePriceType: "BUY_LIMIT", horizons: [1, 5, 20, 60], tradingDate: "2026-09-04" });
    const S = require("./_investorApiSchemas");
    const V2 = require("./_investorApiV2");
    const auth = (reauthed = false) => ({ ok: true, via: "cookie", subject: "operator", sessionId: "s_fixture", reauthed });
    let seq = 0;
    const read = (action, params = {}) => V2.dispatch({ body: { apiVersion: "investor.v2", requestId: `req_fixture_${++seq}`, action, params }, admin: fake, nowMs, authOverride: auth() });
    const mutate = (action, params = {}, { key = null, reason = "fixture reason", version = null, preview = null, absent = null, reauthed = false, nowMs: at = null } = {}) =>
      V2.dispatch({ body: { apiVersion: "investor.v2", requestId: `req_fixture_${++seq}`, action, params, idempotencyKey: key || `key_${action}_${++seq}_${"x".repeat(12)}`, csrfToken: "c".repeat(24), auditReason: reason, ...(version != null ? { expectedResourceVersion: String(version) } : {}), ...(preview ? { previewToken: preview } : {}), ...(absent ? { expectedAbsent: true } : {}) }, admin: fake, nowMs: at || nowMs, authOverride: auth(reauthed) });
    const ctrl = () => fake.docs.get(`${C.control}/control`);
    return { fake, C, nowMs, run, policy, symbols, S, V2, read, mutate, ctrl, commit };
  }

  cases.push(fixture("api_v2_contract_compiles_strictly_and_rejects_unknown_fields_oversized_pages_and_unsafe_numbers", () => {
    const S = require("./_investorApiSchemas");
    const c = S.compileAll();
    if (!c || c.count < 60) throw new Error(`compiled ${c && c.count}`);
    if (S.READ_ACTIONS.length !== 27 || S.MUTATION_ACTIONS.length !== 31) throw new Error(`actions ${S.READ_ACTIONS.length}/${S.MUTATION_ACTIONS.length}`);
    const req = (body) => S.validateRequest(body);
    const base = { apiVersion: "investor.v2", requestId: "req_0000000001" };
    if (!req({ ...base, action: "companies", params: { pageSize: 200, bucket: "eligible" } }).ok) throw new Error("valid read rejected");
    if (req({ ...base, action: "companies", params: { pageSize: 201 } }).ok) throw new Error("oversized page accepted");
    if (req({ ...base, action: "companies", params: { cursor: "short" } }).ok) throw new Error("malformed cursor accepted");
    if (req({ ...base, action: "companies", params: { nope: 1 } }).ok) throw new Error("unknown field accepted");
    if (req({ ...base, action: "requestSell", params: { positionId: "p", symbol: "AAA", quantityUnits: 10, quantityScale: 0, orderType: "LIMIT", timeInForce: "DAY", exchangeSession: "RTH", reason: "why", expectedPositionVersion: "1", expectedMandateVersion: null }, idempotencyKey: "k".repeat(20), csrfToken: "c".repeat(20), auditReason: "why" }).ok) throw new Error("a JSON number for a quantity must be rejected");
    if (req({ ...base, action: "requestSell", params: { positionId: "p", symbol: "AAA", quantityUnits: "012", quantityScale: 0, orderType: "LIMIT", limitPriceMicros: "1", timeInForce: "DAY", exchangeSession: "RTH", reason: "why", expectedPositionVersion: "1", expectedMandateVersion: null }, idempotencyKey: "k".repeat(20), csrfToken: "c".repeat(20), auditReason: "why" }).ok) throw new Error("a leading zero is not canonical");
    if (req({ apiVersion: "investor.v1", requestId: "req_0000000001", action: "companies" }).error.code !== "UNSUPPORTED_API_VERSION") throw new Error("v1 must not reach v2");
    if (req({ ...base, action: "freezeBuys", params: {}, idempotencyKey: "k".repeat(20), csrfToken: "c".repeat(20), auditReason: "why" }).ok) throw new Error("a versioned mutation needs expectedResourceVersion");
    if (req({ ...base, action: "companies", params: {}, idempotencyKey: "k".repeat(20) }).ok) throw new Error("a read must not carry mutation fields");
    if (!req({ ...base, action: "pauseMandate", params: { mandateSeriesId: "m", symbol: "AAA", pauseKind: "OPERATIONAL" }, idempotencyKey: "k".repeat(20), csrfToken: "c".repeat(20), auditReason: "why", expectedResourceVersion: "1" }).ok) throw new Error("valid mutation rejected");
    if (req({ ...base, action: "pauseMandate", params: { mandateSeriesId: "m", symbol: "AAA", pauseKind: "OPERATIONAL" }, idempotencyKey: "k".repeat(20), csrfToken: "c".repeat(20), expectedResourceVersion: "1" }).ok) throw new Error("a reason-required mutation without auditReason must be rejected");
    /* every §14.10 view model is compiled and every model schema passes the strict-subset walk */
    for (const name of ["ManagerDashboardView", "ControlStateView", "CompanyRowView", "HoldingRowView", "AuthorizationExecutionView", "AlertView", "SystemView", "PerformanceView", "SpendView"]) if (!S.VIEW_MODELS[name]) throw new Error(`view model ${name} missing`);
    if (!S.validateModelOutput("mandate-proposal.v1", require("./_investorPolicy").EXAMPLE_MANDATE_PROPOSAL).ok) throw new Error("the example mandate proposal must validate through Ajv");
    if (S.HTTP_STATUS.IDEMPOTENCY_KEY_REUSED !== 409 || S.HTTP_STATUS.ACTION_RETIRED !== 410 || S.HTTP_STATUS.REAUTH_REQUIRED !== 403 || S.HTTP_STATUS.SESSION_EXPIRED !== 401 || S.HTTP_STATUS.SEMANTIC_REJECTED !== 422 || S.HTTP_STATUS.BUDGET_EXHAUSTED !== 429) throw new Error("http mapping");
    if (!/^[a-f0-9]{64}$/.test(S.contractHash())) throw new Error("contract hash");
    return true;
  }));

  cases.push(fixture("api_v2_reads_answer_bounded_envelopes_and_typed_view_models_from_a_seeded_world", async () => {
    const W = apiWorld();
    const { S, read } = W;
    const check = (r, name = null) => { if (r.statusCode !== 200 || !r.body.ok) throw new Error(`${name || "read"} → ${r.statusCode} ${JSON.stringify(r.body.error).slice(0, 300)}`); const v = S.validateResponse(r.body); if (!v.ok) throw new Error(`envelope ${JSON.stringify(v.issues).slice(0, 300)}`); return r.body; };
    const view = (name, value) => { const v = S.validateView(name, value); if (!v.ok) throw new Error(`${name}: ${JSON.stringify(v.issues).slice(0, 400)}`); };
    const dash = check(await read("managerDashboard"), "managerDashboard");
    view("ManagerDashboardView", dash.data);
    if (dash.data.coverage.completedCount !== 304 || dash.data.coverage.eligibleCount !== 304) throw new Error("coverage must come from the run's frozen denominator");
    if (dash.data.account.accountMode !== "PAPER_AI" || dash.data.account.nav.amountMinor !== "6020000") throw new Error(`account ${JSON.stringify(dash.data.account)}`);
    if (dash.data.models.investment.snapshot !== "gpt-5.6-sol" || dash.data.models.investment.reasoningEffort !== "high") throw new Error("model routing");
    if (!dash.data.alerts.length || dash.data.alerts[0].conditionId !== "buys_frozen") throw new Error("alerts");
    if (dash.data.evidenceDeltas.length !== 1 || dash.data.workflow.length !== 6) throw new Error("deltas/workflow");
    if (JSON.stringify(dash.data).length > 200000) throw new Error("the dashboard must stay a summary");
    view("SpendView", dash.data.spend);
    if (dash.data.spend.actual.amountMinor !== "420" || dash.data.spend.dailyLimit.amountMinor === "0") throw new Error("spend");
    const cs = check(await read("controlState"), "controlState");
    view("ControlStateView", cs.data);
    if (cs.data.resourceVersion !== "3" || cs.data.mutationsEnabled !== true || cs.data.writerEpoch !== 1) throw new Error(`control ${JSON.stringify(cs.data).slice(0, 200)}`);
    const cap = (a) => cs.data.availableActions.find((c) => c.action === a);
    if (!cap("freezeBuys").enabled || cap("resumeBuys").enabled || !cap("runManagerReview").enabled || cap("activateAccountMode").enabled || !cap("deactivateAccountMode").enabled || !cap("emergencyStop").enabled || cap("resumeSystem").enabled) throw new Error(`capabilities ${JSON.stringify(cs.data.availableActions.map((c) => [c.action, c.enabled]))}`);
    if (!cap("resumeBuys").requiresReauth || cap("freezeBuys").requiresReauth || !cap("freezeBuys").requiresReason) throw new Error("capability flags must mirror the contract");
    /* companies: the complete book, paginated with a bound cursor */
    const p1 = check(await read("companies", { pageSize: 100, sort: "symbol" }), "companies");
    if (p1.data.collectionState !== "READY" || p1.data.items.length !== 100 || !p1.nextCursor || p1.data.totalCount < 304) throw new Error(`page1 ${p1.data.items.length}/${p1.data.totalCount}`);
    for (const row of p1.data.items.slice(0, 5)) view("CompanyRowView", row);
    if (!p1.data.buckets || !p1.data.buckets.byBucket || p1.data.buckets.byBucket.eligible !== 304) throw new Error(`buckets ${JSON.stringify(p1.data.buckets)}`);
    const p2 = check(await read("companies", { pageSize: 100, sort: "symbol", cursor: p1.nextCursor }), "companies page 2");
    if (p2.data.items.some((r) => p1.data.items.some((x) => x.symbol === r.symbol)) || p2.data.completedCount !== 200) throw new Error("pages must not overlap");
    const tampered = await read("companies", { pageSize: 100, sort: "symbol", cursor: p1.nextCursor.slice(0, -2) + "zz" });
    if (tampered.statusCode !== 400 || tampered.body.error.code !== "CURSOR_INVALID") throw new Error("a tampered cursor must be refused");
    const wrongSort = await read("companies", { pageSize: 100, sort: "decision", cursor: p1.nextCursor });
    if (wrongSort.statusCode !== 400) throw new Error("a cursor from another sort must be refused");
    const held = check(await read("companies", { held: true }), "companies held");
    if (held.data.items.length !== 1 || held.data.items[0].symbol !== "BBB" || held.data.items[0].researchState === undefined) throw new Error(`held ${JSON.stringify(held.data.items.map((r) => r.symbol))}`);
    const buy = check(await read("companies", { decision: "BUY" }), "companies BUY");
    if (buy.data.items.length !== 1 || buy.data.items[0].capitalRank !== 1 || buy.data.items[0].changed !== true) throw new Error(`buy ${JSON.stringify(buy.data.items[0]).slice(0, 300)}`);
    if (held.data.items[0].bucket !== "managedOffRoster" || held.data.items[0].mandateState !== "PROTECTED_RTH") throw new Error(`a held off-roster name stays managed: ${JSON.stringify(held.data.items[0]).slice(0, 300)}`);
    const excluded = check(await read("companies", { bucket: "excluded", pageSize: 5 }), "excluded");
    if (!excluded.data.items.length || excluded.data.items[0].exclusionReason == null || excluded.data.items[0].availableActions[0].enabled) throw new Error("excluded rows carry the reason and no research capability");
    /* portfolio: holdings, authorizations, working orders, queue, revisions */
    const pf = check(await read("portfolio"), "portfolio");
    if (pf.data.holdings.items.length !== 1) throw new Error("one holding");
    view("HoldingRowView", pf.data.holdings.items[0]);
    const h = pf.data.holdings.items[0];
    if (h.symbol !== "BBB" || h.quantity.quantityUnits !== "100" || h.mark.priceMicros !== "97000000" || h.averageCost.priceMicros !== "95000000" || h.lossBoundary.priceMicros !== "90000000" || h.protection.state !== "PROTECTED_RTH" || h.weightBps !== "1611") throw new Error(`holding ${JSON.stringify(h).slice(0, 400)}`);
    if (h.distanceToBoundaryBps !== "-722" || h.distanceToTargetBps !== "1340" || h.forwardDownside.amountMinor !== "70000") throw new Error(`distances ${h.distanceToBoundaryBps}/${h.distanceToTargetBps}/${h.forwardDownside.amountMinor}`);
    if (!h.availableActions.find((c) => c.action === "requestSell").enabled) throw new Error("a held position can be sold");
    if (pf.data.standingAuthorizations.items.length !== 2) throw new Error("two standing authorizations");
    for (const a of pf.data.standingAuthorizations.items) view("AuthorizationExecutionView", a);
    const aaa = pf.data.standingAuthorizations.items.find((a) => a.symbol === "AAA");
    if (aaa.state !== "WORKING" || aaa.entry.priceMicros !== "50000000" || aaa.authorizedQuantity.quantityUnits !== "20" || aaa.reservations.notional.amountMinor !== "100000" || aaa.remainingQuantity.quantityUnits !== "20") throw new Error(`AAA ${JSON.stringify(aaa).slice(0, 300)}`);
    if (pf.data.workingOrders.items.length !== 2) throw new Error("two working order sets");
    view("OrderSetView", pf.data.workingOrders.items[0]);
    /* the rest of the read surface answers and validates */
    const runs = check(await read("managerRuns"), "managerRuns"); view("ManagerRunView", runs.data.items[0]);
    if (runs.data.items[0].state !== "COMPLETE" || runs.data.items[0].coverage.completedCount !== 304) throw new Error("run view");
    const jobs = check(await read("jobs"), "jobs"); view("JobView", jobs.data.items[0]);
    const journal = check(await read("decisionJournal", { pageSize: 10 }), "journal"); view("DecisionJournalRowView", journal.data.items[0]);
    if (journal.data.totalCount !== 304 || journal.data.items[0].decision !== "BUY" || !journal.data.items[0].forecast) throw new Error(`journal ${journal.data.totalCount} ${journal.data.items[0].decision}`);
    const ev = check(await read("materialEvents"), "materialEvents"); view("MaterialEventView", ev.data.items[0]);
    const al = check(await read("alerts"), "alerts"); view("AlertView", al.data.items[0]);
    const sys = check(await read("systemHealth"), "systemHealth"); view("SystemView", sys.data);
    if (!sys.data.identities.fixtures.matchesBuild || sys.data.identities.roles.investment.model !== "gpt-5.6-sol" || sys.data.configuration.riskMandate.bounds == null) throw new Error("system identities");
    for (const k of ["sources", "dossiers", "models", "manager", "mandates", "reservations", "executor", "broker", "ledger", "calendar"]) { const hc = sys.data.health[k]; for (const f of ["state", "lastSuccessAt", "expectedBy", "ageSeconds", "backlog", "errorCode", "conditionId", "operatorAction"]) if (!(f in hc)) throw new Error(`health.${k}.${f} missing`); if (!S.ENUMS.ComponentHealth.includes(hc.state)) throw new Error(`health.${k}.state ${hc.state}`); }
    const perf = check(await read("performance", { range: "1M" }), "performance"); view("PerformanceView", perf.data);
    if (perf.data.returns.observations !== 1 || perf.data.returns.smallSampleWarning !== true || perf.data.costs.ai.amountMinor !== "420") throw new Error(`performance ${JSON.stringify(perf.data.returns)} ${perf.data.costs.ai.amountMinor}`);
    const an = check(await read("decisionAnalytics", { horizon: 20 }), "analytics"); view("DecisionAnalyticsView", an.data);
    const uni = check(await read("universe"), "universe");
    if (uni.data.snapshot.eligibleCount !== 304 || uni.data.members !== undefined) throw new Error("universe snapshot");
    const acct = check(await read("account"), "account");
    if (acct.data.nav.amountMinor !== "6020000") throw new Error("account nav");
    const os = check(await read("orderSets"), "orderSets");
    if (os.data.totalCount !== 2) throw new Error("order sets");
    const md = check(await read("mandates", { status: "active" }), "mandates");
    if (md.data.totalCount !== 2) throw new Error("mandates");
    const ex = check(await read("executionEvents"), "executionEvents");
    if (ex.data.collectionState !== "EMPTY") throw new Error("no events yet");
    const doss = check(await read("companyDossier", { symbol: W.symbols[0] }), "companyDossier"); view("CompanyDetailView", doss.data);
    if (doss.data.decision.decision !== "BUY" || doss.data.deltas.length !== 1 || doss.data.mandate !== null) throw new Error(`dossier ${JSON.stringify(doss.data.decision)} ${doss.data.deltas.length}`);
    const dossB = check(await read("companyDossier", { symbol: "BBB" }), "companyDossier BBB"); view("CompanyDetailView", dossB.data);
    if (!dossB.data.mandate || dossB.data.mandate.state !== "PROTECTED_RTH" || dossB.data.mandate.boundary.priceMicros !== "90000000" || dossB.data.mandate.authorizedQuantity.quantityUnits !== "100" || dossB.data.mandate.plannedLoss.amountMinor !== "50000") throw new Error(`BBB mandate ${JSON.stringify(dossB.data.mandate)}`);
    return true;
  }));

  cases.push(fixture("api_v2_mutations_claim_idempotency_before_side_effects_and_honour_versions_modes_and_reauth", async () => {
    const W = apiWorld();
    const { fake, C, mutate, ctrl, read } = W;
    /* observe mode: investment mutations are disabled; the release control itself, safety freezes and administration remain */
    const O = apiWorld({ accountMode: "OBSERVE", writerEpoch: 0 });
    const disabled = await O.mutate("runManagerReview", { reason: "OPERATOR" });
    if (disabled.statusCode !== 403 || disabled.body.error.code !== "MUTATIONS_DISABLED") throw new Error(`observe mode ${disabled.statusCode} ${JSON.stringify(disabled.body.error)}`);
    const frozenInObserve = await O.mutate("freezeBuys", {}, { version: 3 });
    if (frozenInObserve.statusCode !== 200) throw new Error(`freezeBuys must work in OBSERVE: ${JSON.stringify(frozenInObserve.body.error)}`);
    const badPreflight = await O.mutate("activateAccountMode", { targetMode: "PAPER_AI", policyHash: "a".repeat(64), universeHash: "b".repeat(64) }, { version: 4, reauthed: true });
    if (badPreflight.statusCode !== 422 || badPreflight.body.error.code !== "PREFLIGHT_FAILED" || !/policy_hash_mismatch/.test(badPreflight.body.error.message)) throw new Error(`preflight ${badPreflight.statusCode} ${JSON.stringify(badPreflight.body.error).slice(0, 200)}`);
    const uni = await O.read("universe");
    const good = await O.mutate("activateAccountMode", { targetMode: "PAPER_AI", policyHash: O.policy.policyHash, universeHash: uni.body.data.snapshot.universeHash }, { version: 4, reauthed: true });
    if (good.statusCode !== 200 || O.ctrl().accountMode !== "PAPER_AI" || O.ctrl().writerEpoch !== 1 || O.ctrl().engineMode !== "manager" || O.ctrl().controlVersion !== 5) throw new Error(`activation ${good.statusCode} ${JSON.stringify(good.body.error)} ${JSON.stringify(O.ctrl()).slice(0, 200)}`);
    if (!O.ctrl().lastTransition || O.ctrl().lastTransition.action !== "activateAccountMode") throw new Error("transition record");
    /* idempotency: same key + same content replays; same key + other content is refused; stale version conflicts */
    const key = "key_freeze_0000000001";
    const a = await mutate("freezeBuys", {}, { key, version: 3 });
    if (a.statusCode !== 200 || !a.body.mutationId || a.body.resourceVersion !== "4" || a.body.appliedState.buyState !== "FROZEN" || ctrl().buyState !== "FROZEN" || ctrl().freezeNewBuys !== true) throw new Error(`freeze ${a.statusCode} ${JSON.stringify(a.body).slice(0, 300)}`);
    const b = await mutate("freezeBuys", {}, { key, version: 3 });
    if (b.statusCode !== 200 || b.body.mutationId !== a.body.mutationId || b.body.replayed !== true || ctrl().controlVersion !== 4) throw new Error(`replay ${JSON.stringify(b.body).slice(0, 300)}`);
    const c = await mutate("freezeBuys", {}, { key, version: 3, reason: "a different reason" });
    if (c.statusCode !== 409 || c.body.error.code !== "IDEMPOTENCY_KEY_REUSED") throw new Error(`reuse ${c.statusCode} ${JSON.stringify(c.body.error)}`);
    const stale = await mutate("pauseManager", {}, { version: 3 });
    if (stale.statusCode !== 409 || stale.body.error.code !== "VERSION_CONFLICT" || stale.body.resourceVersion !== "4") throw new Error(`stale ${stale.statusCode} ${JSON.stringify(stale.body.error)}`);
    const audits = [...fake.docs.entries()].filter(([k]) => k.startsWith(`${C.audit}/`)).map(([, v]) => v);
    if (!audits.some((x) => x.action === "freezeBuys" && x.mutationId === a.body.mutationId)) throw new Error("every mutation leaves an audit link");
    /* resumeBuys is reauth-gated by the contract and blocked while the emergency state is not CLEAR */
    const cs = await read("controlState");
    if (cs.body.data.buyState.applied !== "FROZEN" || cs.body.data.availableActions.find((x) => x.action === "resumeBuys").enabled !== true) throw new Error("resumeBuys must be offered once frozen");
    const resumed = await mutate("resumeBuys", {}, { version: 4, reauthed: true });
    if (resumed.statusCode !== 200 || ctrl().buyState !== "OPEN" || ctrl().freezeNewBuys !== false) throw new Error(`resume ${resumed.statusCode} ${JSON.stringify(resumed.body.error)}`);
    /* the budget: an increase needs reauthentication, a decrease does not */
    const up = await mutate("setBudget", { dailyReservationMinor: "999999" }, { version: 5 });
    if (up.statusCode !== 403 || up.body.error.code !== "REAUTH_REQUIRED") throw new Error(`budget increase ${up.statusCode}`);
    const down = await mutate("setBudget", { dailyReservationMinor: "100" }, { version: 5 });
    if (down.statusCode !== 200 || ctrl().budget.dailyReservationMinor !== "100" || ctrl().budget.version !== 1) throw new Error(`budget decrease ${down.statusCode} ${JSON.stringify(down.body.error)}`);
    /* the risk mandate: bounded, versioned, an over-limit book freezes expansion instead of liquidating */
    const oob = await mutate("setRiskMandate", { overrides: { "weights.maxSingleNameWeightBps": "9999" } }, { version: 6, reauthed: true });
    if (oob.statusCode !== 422 || oob.body.error.code !== "RISK_BOUND_EXCEEDED") throw new Error(`bounds ${oob.statusCode} ${JSON.stringify(oob.body.error)}`);
    const tight = await mutate("setRiskMandate", { overrides: { "weights.maxSingleNameWeightBps": "1000" } }, { version: 6, reauthed: true });
    if (tight.statusCode !== 200 || tight.body.data.version !== 1 || !/^[a-f0-9]{64}$/.test(tight.body.data.hash) || tight.body.data.overLimit[0] !== "BBB" || ctrl().buyState !== "FROZEN" || fake.docs.get(`${C.positions}/paper-1_BBB`).open !== true) throw new Error(`risk ${tight.statusCode} ${JSON.stringify(tight.body).slice(0, 300)}`);
    /* alerts: acknowledgement is recorded and silences nothing */
    const ack = await mutate("acknowledgeAlert", { alertId: "buys_frozen", alertVersion: "3" });
    if (ack.statusCode !== 200 || ack.body.data.stillActive !== true || fake.docs.get(`${C.alerts}/buys_frozen`).active !== true || fake.docs.get(`${C.alerts}/buys_frozen`).acknowledgedBy !== "operator") throw new Error(`ack ${ack.statusCode} ${JSON.stringify(ack.body)}`);
    const ackStale = await mutate("acknowledgeAlert", { alertId: "buys_frozen", alertVersion: "2" });
    if (ackStale.statusCode !== 409) throw new Error("alert version conflict");
    /* research and review enqueue jobs with operator lineage; never activate */
    const rr = await mutate("requestResearch", { symbol: "CCC", directive: "RESEARCH_NOW" });
    if (rr.statusCode !== 200 || !rr.body.jobId || !fake.docs.get(`${C.jobs}/${rr.body.jobId}`) || fake.docs.get(`${C.jobs}/${rr.body.jobId}`).payload.reason !== "OPERATOR") throw new Error(`research ${rr.statusCode} ${JSON.stringify(rr.body.error)}`);
    const rm = await mutate("runManagerReview", { reason: "OPERATOR" });
    if (rm.statusCode !== 200 || !rm.body.jobId || !rm.body.data.evidenceCutoff || !fake.docs.get(`${C.jobs}/${rm.body.jobId}`).runId.includes("_op")) throw new Error(`review ${rm.statusCode} ${JSON.stringify(rm.body).slice(0, 300)}`);
    const rm2 = await mutate("runManagerReview", { reason: "OPERATOR" });
    if (rm2.statusCode !== 200 || rm2.body.jobId !== rm.body.jobId || rm2.body.data.duplicate !== true) throw new Error("the same slot deduplicates");
    const rev = await mutate("runFocusedRevision", { symbol: W.symbols[0], deltaId: `delta_${W.symbols[0]}_1` });
    if (rev.statusCode !== 200 || !rev.body.jobId) throw new Error(`revision ${rev.statusCode} ${JSON.stringify(rev.body.error)}`);
    /* pause / resume / cancel a mandate: typed pause, same-hash resume, cancel keeps protection */
    const pause = await mutate("pauseMandate", { mandateSeriesId: "paper-1_AAA", symbol: "AAA", pauseKind: "OPERATIONAL" }, { version: 1 });
    const outbox = () => [...fake.docs.entries()].filter(([k]) => k.startsWith(`${C.executionOutbox}/`)).map(([, v]) => v);
    if (pause.statusCode !== 200 || fake.docs.get(`${C.activeMandates}/paper-1_AAA`).status !== "PAUSED_OPERATIONAL" || !outbox().some((t) => t.kind === "CANCEL_UNFILLED_ENTRY" && t.authority === "OPERATOR" && t.symbol === "AAA")) throw new Error(`pause ${pause.statusCode} ${JSON.stringify(pause.body.error)}`);
    const wrongHash = await mutate("resumeMandate", { mandateSeriesId: "paper-1_AAA", symbol: "AAA", mandateHash: "0".repeat(64) }, { version: 1 });
    if (wrongHash.statusCode !== 409) throw new Error("resume needs the paused hash");
    const blockedByDelta = await mutate("resumeMandate", { mandateSeriesId: "paper-1_AAA", symbol: "AAA", mandateHash: require("./_investorPolicy").sha256("mv_AAA_1") }, { version: 1 });
    if (blockedByDelta.statusCode !== 409 && blockedByDelta.statusCode !== 200) throw new Error(`resume ${blockedByDelta.statusCode} ${JSON.stringify(blockedByDelta.body.error)}`);
    const cancelHeld = await mutate("cancelMandate", { mandateSeriesId: "paper-1_BBB", symbol: "BBB" }, { version: 1 });
    if (cancelHeld.statusCode !== 200 || cancelHeld.body.data.protectionRetained !== true || fake.docs.get(`${C.activeMandates}/paper-1_BBB`).status !== "PROTECTED_RTH" || fake.docs.get(`${C.activeMandates}/paper-1_BBB`).replacementRequired !== true) throw new Error(`cancel held ${cancelHeld.statusCode} ${JSON.stringify(cancelHeld.body).slice(0, 300)}`);
    if (fake.docs.get(`${C.orderLegs}/os_mv_BBB_1_STOP`).status !== "WORKING") throw new Error("cancelling a held mandate must never touch its protection");
    /* the account reset needs a one-use preview, the right versions, no exposure, and conservation */
    const pv = await mutate("previewPaperAccountReset", {});
    if (pv.statusCode !== 200 || !pv.body.data.previewToken || pv.body.data.canReset !== false || !pv.body.data.blockers.some((b) => /open_positions/.test(b))) throw new Error(`preview ${pv.statusCode} ${JSON.stringify(pv.body).slice(0, 300)}`);
    const reset = await mutate("resetPaperAccount", { accountVersion: pv.body.data.accountVersion, balanceHash: pv.body.data.balanceHash }, { preview: pv.body.data.previewToken, reauthed: true });
    if (reset.statusCode !== 409 || reset.body.error.code !== "STATE_CONFLICT") throw new Error(`reset with exposure ${reset.statusCode} ${JSON.stringify(reset.body.error)}`);
    /* emergency stop: expansion frozen, unfilled entries cancelled through the outbox, protection untouched, not a liquidation */
    const es = await mutate("emergencyStop", {});
    if (es.statusCode !== 200 || ctrl().emergencyState !== "ENGAGED" || ctrl().buyState !== "FROZEN" || ctrl().managerState !== "PAUSED") throw new Error(`emergency ${es.statusCode} ${JSON.stringify(es.body.error)}`);
    if (fake.docs.get(`${C.orderLegs}/os_mv_BBB_1_STOP`).status !== "WORKING" || fake.docs.get(`${C.positions}/paper-1_BBB`).open !== true) throw new Error("an emergency stop never cancels protection or liquidates");
    const rs = await mutate("resumeSystem", {}, { version: ctrl().controlVersion, reauthed: true });
    if (rs.statusCode !== 200 || !["CLEAR", "RECOVERING"].includes(ctrl().emergencyState) || ctrl().buyState !== "FROZEN") throw new Error(`resumeSystem ${rs.statusCode} ${JSON.stringify(rs.body.error)} ${ctrl().emergencyState}`);
    /* deactivation: back to OBSERVE with buys frozen and protection retained */
    const de = await mutate("deactivateAccountMode", { targetMode: "OBSERVE" }, { version: ctrl().controlVersion, reauthed: true });
    if (de.statusCode !== 200 || ctrl().accountMode !== "OBSERVE" || ctrl().buyState !== "FROZEN") throw new Error(`deactivate ${de.statusCode} ${JSON.stringify(de.body.error)}`);
    const after = await mutate("requestResearch", { symbol: "DDD" });
    if (after.statusCode !== 403 || after.body.error.code !== "MUTATIONS_DISABLED") throw new Error("OBSERVE disables investment mutations again");
    return true;
  }));

  cases.push(fixture("operator_sell_is_a_typed_outbox_transition_that_resizes_protection_and_cancel_restores_it", async () => {
    const W = apiWorld();
    const { fake, C, mutate, nowMs } = W;
    const X = require("./_investorExecution");
    const B = require("./_investorBroker");
    const adapter = B.createPaperAdapter({ admin: fake, now: () => nowMs });
    const bad = await mutate("requestSell", { positionId: "paper-1_BBB", symbol: "BBB", quantityUnits: "150", quantityScale: 0, orderType: "MARKETABLE_LIMIT", timeInForce: "DAY", exchangeSession: "RTH", collarBps: "100", reason: "trim", expectedPositionVersion: "7", expectedMandateVersion: "1" });
    if (bad.statusCode !== 422) throw new Error(`oversell must be refused: ${bad.statusCode} ${JSON.stringify(bad.body.error)}`);
    const staleV = await mutate("requestSell", { positionId: "paper-1_BBB", symbol: "BBB", quantityUnits: "25", quantityScale: 0, orderType: "MARKETABLE_LIMIT", timeInForce: "DAY", exchangeSession: "RTH", reason: "trim", expectedPositionVersion: "6", expectedMandateVersion: "1" });
    if (staleV.statusCode !== 409 || staleV.body.error.code !== "VERSION_CONFLICT") throw new Error(`stale position version ${staleV.statusCode}`);
    const sell = await mutate("requestSell", { positionId: "paper-1_BBB", symbol: "BBB", quantityUnits: "25", quantityScale: 0, orderType: "MARKETABLE_LIMIT", timeInForce: "DAY", exchangeSession: "RTH", collarBps: "100", reason: "trim a quarter", expectedPositionVersion: "7", expectedMandateVersion: "1" });
    if (sell.statusCode !== 200 || !sell.body.data.overrideId || !sell.body.data.orderSetId || sell.body.data.quantity.quantityUnits !== "25" || sell.body.data.thesisUnchanged !== true) throw new Error(`sell ${sell.statusCode} ${JSON.stringify(sell.body).slice(0, 300)}`);
    const orderSetId = sell.body.data.orderSetId;
    const os = fake.docs.get(`${C.orderSets}/${orderSetId}`);
    if (os.purpose !== "OPERATOR_SELL" || os.authority !== "OPERATOR" || os.legs[0].role !== "SELL" || os.legs[0].type !== "MARKETABLE_LIMIT") throw new Error("operator order set shape");
    const t = fake.docs.get(`${C.executionOutbox}/${orderSetId}:OPERATOR_SELL`);
    if (!t || t.kind !== "OPERATOR_SELL" || t.status !== "PENDING" || t.authority !== "OPERATOR") throw new Error("outbox transition");
    const dup = await mutate("requestSell", { positionId: "paper-1_BBB", symbol: "BBB", quantityUnits: "10", quantityScale: 0, orderType: "MARKETABLE_LIMIT", timeInForce: "DAY", exchangeSession: "RTH", reason: "again", expectedPositionVersion: "7", expectedMandateVersion: "1" });
    if (dup.statusCode !== 409 || !/PROTECTION_RESIZE_PENDING/.test(dup.body.error.message)) throw new Error("a second sell while one is working is refused");
    /* the executor applies it: protection legs resized to the 75 shares that stay owned, the sell submitted */
    const applied = await X.applyTransition({ admin: fake, adapter, transition: t, nowMs });
    if (!applied.applied || applied.protectionResize.length !== 2 || applied.protectionResize.some((r) => r.remainingUnits !== "75")) throw new Error(`apply ${JSON.stringify(applied)}`);
    if (fake.docs.get(`${C.orderLegs}/os_mv_BBB_1_STOP`).remainingUnits !== "75" || fake.docs.get(`${C.orderLegs}/os_mv_BBB_1_TARGET`).remainingUnits !== "75" || fake.docs.get(`${C.orderLegs}/${orderSetId}_SELL`).status !== "WORKING") throw new Error("protection resized and sell working");
    if (fake.docs.get(`${C.activeMandates}/paper-1_BBB`).symbolLock !== null || fake.docs.get(`${C.activeMandates}/paper-1_BBB`).protectedQuantityUnits !== "75") throw new Error("symbol lock released; protected quantity recorded");
    /* a marketable sell fills on the next bar and the ledger stays balanced */
    const fills = await X.simulatePaperFills({ admin: fake, adapter, accountId: "paper-1", barsBySymbol: { BBB: [{ t: new Date(nowMs + 60000).toISOString(), o: 97, h: 97.5, l: 96.5, c: 97.2, v: 500000, prevClose: 97 }] }, nowMs: nowMs + 60000 });
    if (!fills.fills.some((f) => f.symbol === "BBB" && f.role === "SELL" && f.quantityUnits === "25")) throw new Error(`sell fill ${JSON.stringify(fills.fills)}`);
    const pos = fake.docs.get(`${C.positions}/paper-1_BBB`);
    if (pos.quantityUnits !== "75" || pos.open !== true) throw new Error(`position after sell ${JSON.stringify(pos).slice(0, 200)}`);
    const cons = await X.assertConservation("paper-1", { admin: fake });
    if (!cons.pass) throw new Error(`conservation ${JSON.stringify(cons.discrepancies)}`);
    const late = await mutate("cancelSell", { overrideId: sell.body.data.overrideId, orderSetId }, { version: 1 });
    if (late.statusCode !== 409 || !/EXECUTION_BEGUN|FILLED|is /.test(late.body.error.message)) throw new Error(`cancel after fill ${late.statusCode} ${JSON.stringify(late.body.error)}`);
    /* a second sell: cancelled before the executor touches it, no resize; then one cancelled after submission restores protection */
    const s2 = await mutate("requestSell", { positionId: "paper-1_BBB", symbol: "BBB", quantityUnits: "10", quantityScale: 0, orderType: "LIMIT", limitPriceMicros: "99000000", timeInForce: "GTC", exchangeSession: "RTH", reason: "limit trim", expectedPositionVersion: String(fake.docs.get(`${C.accounts}/paper-1`).portfolioVersion || fake.docs.get(`${C.accounts}/paper-1`).balanceRevision), expectedMandateVersion: "1" });
    if (s2.statusCode !== 200) throw new Error(`second sell ${s2.statusCode} ${JSON.stringify(s2.body.error)}`);
    const c2 = await mutate("cancelSell", { overrideId: s2.body.data.overrideId, orderSetId: s2.body.data.orderSetId }, { version: 1 });
    if (c2.statusCode !== 200 || c2.body.data.state !== "CANCELLED" || fake.docs.get(`${C.executionOutbox}/${s2.body.data.orderSetId}:OPERATOR_SELL`).status !== "CANCELLED") throw new Error(`early cancel ${c2.statusCode} ${JSON.stringify(c2.body).slice(0, 200)}`);
    const s3 = await mutate("requestSell", { positionId: "paper-1_BBB", symbol: "BBB", quantityUnits: "10", quantityScale: 0, orderType: "LIMIT", limitPriceMicros: "99000000", timeInForce: "GTC", exchangeSession: "RTH", reason: "limit trim", expectedPositionVersion: String(fake.docs.get(`${C.accounts}/paper-1`).portfolioVersion || fake.docs.get(`${C.accounts}/paper-1`).balanceRevision), expectedMandateVersion: "1" });
    if (s3.statusCode !== 200) throw new Error(`third sell ${s3.statusCode} ${JSON.stringify(s3.body.error)}`);
    const t3 = fake.docs.get(`${C.executionOutbox}/${s3.body.data.orderSetId}:OPERATOR_SELL`);
    const ap3 = await X.applyTransition({ admin: fake, adapter, transition: t3, nowMs: nowMs + 120000 });
    if (!ap3.applied || fake.docs.get(`${C.orderLegs}/os_mv_BBB_1_STOP`).remainingUnits !== "65") throw new Error(`third apply ${JSON.stringify(ap3)}`);
    const c3 = await mutate("cancelSell", { overrideId: s3.body.data.overrideId, orderSetId: s3.body.data.orderSetId }, { version: 1 });
    if (c3.statusCode !== 200 || c3.body.data.state !== "CANCEL_PENDING") throw new Error(`cancel pending ${c3.statusCode} ${JSON.stringify(c3.body).slice(0, 200)}`);
    const tc = fake.docs.get(`${C.executionOutbox}/${c3.body.data.transitionId}`);
    const apc = await X.applyTransition({ admin: fake, adapter, transition: tc, nowMs: nowMs + 180000 });
    if (!apc.applied || fake.docs.get(`${C.orderLegs}/os_mv_BBB_1_STOP`).remainingUnits !== "75" || fake.docs.get(`${C.orderLegs}/${s3.body.data.orderSetId}_SELL`).status !== "CANCELLED") throw new Error(`restore ${JSON.stringify(apc)} stop=${fake.docs.get(`${C.orderLegs}/os_mv_BBB_1_STOP`).remainingUnits}`);
    return true;
  }));

  cases.push(fixture("sessions_are_server_state_with_httponly_cookie_rotating_csrf_reauth_and_origin_enforcement", async () => {
    const AUTH = require("./_investorAuth");
    const SESSION = require("./investorSession");
    const S = require("./_investorApiSchemas");
    const fake = fakeAdmin();
    const hadPass = process.env.INVESTOR_PASSCODE, hadSecret = process.env.INVESTOR_SESSION_SECRET;
    if (!AUTH.authSecrets().passcode) { process.env.INVESTOR_PASSCODE = "fixture-passcode-0123456789"; process.env.INVESTOR_SESSION_SECRET = "fixture-session-secret-0123456789abcdef0123456789"; }
    try {
      const passcode = AUTH.authSecrets().passcode;
      if (!passcode) return true; /* no secrets reachable in this process: nothing to attest */
      const nowMs = Date.UTC(2026, 8, 4, 15);
      const ev = (body, { cookie = null, origin = "https://investor.goldenspike.app", host = "investor.goldenspike.app", method = "POST", extra = {} } = {}) => ({ httpMethod: method, headers: { origin, host, "content-type": "application/json", ...(cookie ? { cookie } : {}), ...extra }, body: JSON.stringify(body) });
      const bad = await SESSION.handle(ev({ action: "create", passcode: "wrong-passcode-000000" }), { admin: fake, nowMs });
      if (bad.statusCode !== 401 || JSON.parse(bad.body).error.code !== "AUTH_MISSING" || bad.headers["Set-Cookie"]) throw new Error(`wrong passcode ${bad.statusCode}`);
      const foreign = await SESSION.handle(ev({ action: "create", passcode }, { origin: "https://evil.example" }), { admin: fake, nowMs });
      if (foreign.statusCode !== 403 || JSON.parse(foreign.body).error.code !== "ORIGIN_REJECTED") throw new Error(`origin ${foreign.statusCode}`);
      const crossSite = await SESSION.handle(ev({ action: "create", passcode }, { extra: { "sec-fetch-site": "cross-site" } }), { admin: fake, nowMs });
      if (crossSite.statusCode !== 403) throw new Error("cross-site fetch must be rejected");
      const created = await SESSION.handle(ev({ action: "create", passcode }), { admin: fake, nowMs });
      const cb = JSON.parse(created.body);
      if (created.statusCode !== 200 || !cb.ok || !cb.data.csrfToken || !cb.data.expiresAt || cb.payloadVersion !== S.PAYLOAD_VERSION) throw new Error(`create ${created.statusCode} ${created.body.slice(0, 200)}`);
      const setCookie = created.headers["Set-Cookie"];
      if (!/HttpOnly/.test(setCookie) || !/SameSite=Strict/.test(setCookie) || !/Secure/.test(setCookie) || !/^inv_session=/.test(setCookie)) throw new Error(`cookie ${setCookie}`);
      if (JSON.stringify(cb).includes(passcode) || /session=/.test(JSON.stringify(cb.data))) throw new Error("secrets never return in the body");
      const cookie = setCookie.split(";")[0];
      const sessions = [...fake.docs.entries()].filter(([k]) => k.startsWith(`${fake.COL.sessions}/`)).map(([, v]) => v);
      if (sessions.length !== 1 || sessions[0].csrfHash === cb.data.csrfToken) throw new Error("server session record with a hashed CSRF token");
      /* the v2 guard: cookie required, CSRF for mutations, reauth when demanded */
      const g1 = await AUTH.requireOperatorV2(ev({ apiVersion: "investor.v2" }), {}, { mutation: false, admin: fake, nowMs });
      if (g1.ok || g1.code !== "SESSION_EXPIRED") throw new Error("no cookie → 401 class");
      const g2 = await AUTH.requireOperatorV2(ev({}, { cookie }), { csrfToken: "nope" }, { mutation: true, admin: fake, nowMs });
      if (g2.ok || g2.code !== "CSRF_INVALID") throw new Error("bad csrf");
      const g3 = await AUTH.requireOperatorV2(ev({}, { cookie }), { csrfToken: cb.data.csrfToken }, { mutation: true, admin: fake, nowMs });
      if (!g3.ok || g3.reauthed !== false) throw new Error(`good csrf ${JSON.stringify(g3)}`);
      const g4 = await AUTH.requireOperatorV2(ev({}, { cookie }), { csrfToken: cb.data.csrfToken }, { mutation: true, reauth: true, admin: fake, nowMs });
      if (g4.ok || g4.code !== "REAUTH_REQUIRED") throw new Error("reauth demanded");
      const reauthBad = await SESSION.handle(ev({ action: "reauth", passcode: "wrong-passcode-000000", csrfToken: cb.data.csrfToken }, { cookie }), { admin: fake, nowMs });
      if (reauthBad.statusCode !== 401) throw new Error("reauth wrong passcode");
      const reauth = await SESSION.handle(ev({ action: "reauth", passcode, csrfToken: cb.data.csrfToken }, { cookie }), { admin: fake, nowMs });
      const rb = JSON.parse(reauth.body);
      if (reauth.statusCode !== 200 || !rb.data.reauthToken) throw new Error(`reauth ${reauth.statusCode} ${reauth.body.slice(0, 200)}`);
      const g5 = await AUTH.requireOperatorV2(ev({}, { cookie }), { csrfToken: cb.data.csrfToken, reauthToken: rb.data.reauthToken }, { mutation: true, reauth: true, admin: fake, nowMs });
      if (!g5.ok || g5.reauthed !== true) throw new Error("reauth token accepted");
      const g6 = await AUTH.requireOperatorV2(ev({}, { cookie }), { csrfToken: cb.data.csrfToken, reauthToken: rb.data.reauthToken }, { mutation: true, reauth: true, admin: fake, nowMs: nowMs + 8 * 60000 });
      if (g6.ok) throw new Error("a reauth token expires in minutes");
      /* refresh rotates the CSRF token; the old one stops working; the cookie stays the same */
      const refreshed = await SESSION.handle(ev({ action: "refresh" }, { cookie }), { admin: fake, nowMs: nowMs + 60000 });
      const fb = JSON.parse(refreshed.body);
      if (refreshed.statusCode !== 200 || fb.data.csrfToken === cb.data.csrfToken) throw new Error("csrf must rotate");
      const g7 = await AUTH.requireOperatorV2(ev({}, { cookie }), { csrfToken: cb.data.csrfToken }, { mutation: true, admin: fake, nowMs: nowMs + 60000 });
      if (g7.ok) throw new Error("the rotated-out csrf token must fail");
      /* revoke is server-side: the same cookie is dead immediately */
      const revoked = await SESSION.handle(ev({ action: "revoke" }, { cookie }), { admin: fake, nowMs: nowMs + 120000 });
      if (revoked.statusCode !== 200 || !/Max-Age=0/.test(revoked.headers["Set-Cookie"])) throw new Error("revoke clears the cookie");
      const g8 = await AUTH.requireOperatorV2(ev({}, { cookie }), {}, { admin: fake, nowMs: nowMs + 120000 });
      if (g8.ok || g8.code !== "SESSION_EXPIRED") throw new Error("revoked session must be dead");
      const expired = await SESSION.handle(ev({ action: "refresh" }, { cookie }), { admin: fake, nowMs: nowMs + 120000 });
      if (expired.statusCode !== 401) throw new Error("refresh after revoke is 401");
      /* the v1 compatibility guard accepts a live cookie too, so the legacy reads keep working during dual run */
      const created2 = await SESSION.handle(ev({ action: "create", passcode }), { admin: fake, nowMs });
      const cookie2 = created2.headers["Set-Cookie"].split(";")[0];
      const g9 = await AUTH.requireOperatorAsync(ev({}, { cookie: cookie2 }), {}, { admin: fake });
      if (!g9.ok || g9.via !== "cookie") throw new Error("v1 guard accepts the cookie");
      return true;
    } finally {
      if (hadPass === undefined) delete process.env.INVESTOR_PASSCODE; else process.env.INVESTOR_PASSCODE = hadPass;
      if (hadSecret === undefined) delete process.env.INVESTOR_SESSION_SECRET; else process.env.INVESTOR_SESSION_SECRET = hadSecret;
    }
  }));

  cases.push(fixture("v1_investment_mutations_retire_under_the_manager_engine_and_reads_route_by_exact_api_version", () => {
    const S = require("./_investorApiSchemas");
    const fs = require("fs");
    const src = fs.readFileSync(require.resolve("./investorApi.js"), "utf8");
    for (const a of ["approve", "reject", "kill", "resume", "activateSafety", "setControl", "recordRecallBenchmark", "setRegime", "setIntelligenceWatchlist", "setPaperLearning"]) if (!S.RETIRED_V1_MUTATIONS.includes(a)) throw new Error(`${a} must be retired`);
    if (S.V1_EMERGENCY_MAP.kill !== "emergencyStop" || S.V1_EMERGENCY_MAP.resume !== "resumeSystem") throw new Error("emergency compatibility is mapped explicitly");
    if (!/body\.apiVersion === SCHEMAS_V2\.API_VERSION/.test(src) || !/V2\.dispatch\(/.test(src)) throw new Error("v2 dispatch");
    if (!/apiVersion !== undefined && body\.apiVersion !== "investor\.v1"/.test(src)) throw new Error("only absent or investor.v1 reaches ACTIONS_V1");
    if (!/statusCode: 410|json\(event, 410/.test(src) || !/ACTION_RETIRED/.test(src) || !/recordV1Telemetry/.test(src)) throw new Error("410 retirement with telemetry");
    if (!/requireOperatorAsync/.test(src)) throw new Error("v1 accepts the cookie session");
    if (!/download=/.test(src) || !/openPayload\("download"/.test(src)) throw new Error("signed download");
    const V2 = require("./_investorApiV2");
    for (const a of S.READ_ACTIONS) if (typeof V2.READS[a] !== "function") throw new Error(`read ${a}`);
    for (const a of S.MUTATION_ACTIONS) if (typeof V2.MUTATIONS[a] !== "function") throw new Error(`mutation ${a}`);
    return true;
  }));

  /* ── the console (§14): one file, five views, stable ids, shared vectors, no inline handlers, CSP hashes ── */
  function consoleSource() {
    const fs = require("fs"), path = require("path");
    const candidates = [];
    for (const root of [__dirname, process.cwd()]) for (let up = 0; up <= 4; up += 1) candidates.push(path.join(root, ...Array(up).fill(".."), "investor.html"));
    const file = candidates.find((p) => { try { return fs.statSync(p).isFile(); } catch { return false; } });
    if (!file) throw new Error("investor.html not reachable from the bundle");
    const html = fs.readFileSync(file, "utf8");
    const script = html.slice(html.indexOf("<script>") + "<script>".length, html.lastIndexOf("</script>"));
    const style = html.slice(html.indexOf("<style>") + "<style>".length, html.indexOf("</style>"));
    return { file, html, script, style, root: path.dirname(file) };
  }
  cases.push(fixture("console_is_one_file_with_five_views_stable_ids_shared_vectors_and_no_inline_script_or_style", () => {
    const S = require("./_investorApiSchemas");
    const { html, script } = consoleSource();
    if ((html.match(/<script>/g) || []).length !== 1 || (html.match(/<\/script>/g) || []).length !== 1 || (html.match(/<style>/g) || []).length !== 1) throw new Error("exactly one script block and one style block");
    if (/<script[^>]+src=|<link[^>]+href=|src="https?:/.test(html)) throw new Error("no external resources");
    if (/\son[a-z]+="/i.test(html) || /javascript:/i.test(html)) throw new Error("no inline event handlers");
    if (/ style="/.test(html)) throw new Error("no style attributes (CSP hashes cover the one style block)");
    if (/innerHTML/.test(script) || /\beval\(|new Function\(/.test(script)) throw new Error("no innerHTML / eval");
    if (/sessionStorage\.setItem\(|localStorage\.setItem\(/.test(script) && /token|session|passcode|csrf/i.test(script.match(/(sessionStorage|localStorage)\.setItem\([^)]*\)/g).join(" "))) throw new Error("credentials never persist in browser storage");
    const ids = [...html.matchAll(/id="([^"]+)"/g)].map((m) => m[1]);
    const dup = ids.filter((x, i) => ids.indexOf(x) !== i);
    if (dup.length) throw new Error(`duplicate ids ${dup.slice(0, 5).join(",")}`);
    const need = ["criticalBanner", "alertCenter", "signOut", "managerStatus", "managerReviewSummary", "universeCoverage", "evidenceDelta", "holdingRevisions", "newDecisions", "materialEventInbox", "managerAuthorizationSummary", "managerWorkflow", "managerSpend", "managerRunDetail", "managerModelSummary",
      "companySearch", "companyCount", "companyPager", "companyFilters", "companyCoverageList", "researchQueue", "opportunityList", "watchList", "expiredPlans", "companyDrawer", "companyIdentity", "companyChart", "companyEvidenceDelta", "companyThesis", "companyValuation", "companyDecision", "companyPlan", "companyCatalysts", "companyRisks", "companySources", "companyDecisionHistory",
      "portfolioSummary", "holdingList", "standingAuthorizations", "workingOrders", "executionQueue", "executionTimeline", "planRevisionHistory", "performanceSummary", "forecastCalibration", "expectedVsRealized", "selectionLift", "thesisAccuracy", "missedOpportunityAudit", "decisionAttribution", "modelComparison",
      "systemAlerts", "sourceHealth", "sourceAdministration", "dossierHealth", "managerJobHealth", "modelHealth", "executorHealth", "brokerHealth", "marketConfig", "accountConfig", "accountModeActivation", "policyIdentity", "modelPolicyStatus", "ledgerSummary", "reconcileSystem", "costBreakdown", "budgetAdministration", "riskAdministration", "universeAdministration", "freezeUniverse", "resolveCiks", "corporateActionQueue", "auditExport",
      "pauseManager", "resumeManager", "reviewNow", "freezeBuys", "resumeBuys", "emergencyStop", "resumeSystem", "managerMode", "dailyBudget", "riskMandate", "modePill", "sessPill", "syncLab", "railState", "railProv", "refreshBtn", "gate", "gateForm", "pc", "gateErr", "app", "toast"];
    const missing = need.filter((i) => !ids.includes(i));
    if (missing.length) throw new Error(`missing ids ${missing.join(",")}`);
    for (const gone of ["renderMovers", "renderLiveConfig", "renderExploration", "renderLadder", "renderMemory", "renderRegime", "causeTag", "gateChips", "GATE_PLAIN", "exitPlanText", "patienceBadge", "EXIT_PLAIN", "eagerNow", "sizeNow", "holdNow", "sessionWhy", "tradeCard", "advToggle", "navAdv", "todayHistWhy", "v-approvals", "v-knobs", "v-candidates"]) if (new RegExp(`\\b${gone}\\b`).test(html)) throw new Error(`${gone} must not survive the rewrite`);
    const m = script.match(/var INVESTOR_TEST_VECTORS = (\{.*?\});\n/);
    if (!m) throw new Error("shared test vectors missing");
    if (JSON.stringify(JSON.parse(m[1])) !== JSON.stringify(S.TEST_VECTORS)) throw new Error("the console's test vectors must equal the server's");
    if (!/"investor\.v2"/.test(script) || !/apiVersion\s*:/.test(script) || !/credentials\s*:\s*"same-origin"/.test(script) || !/investorSession/.test(script)) throw new Error("v2 wire + same-origin credentials + session endpoint");
    if (!/idempotencyKey/.test(script) || !/csrfToken/.test(script) || !/expectedResourceVersion/.test(script) || !/reauthToken/.test(script)) throw new Error("mutation envelope fields");
    if (!/availableActions/.test(script) || !/data-action/.test(script) || !/data-resource-version/.test(script)) throw new Error("capabilities drive controls");
    if (!/payloadVersion/.test(script) || !/investor\.v2\.1/.test(script)) throw new Error("payloadVersion check");
    if (!/Investor AI — AI Fund Manager/.test(html)) throw new Error("product name");
    if (!/function selfTestVectors\(/.test(script) || !/function canonicalInt\(/.test(script) || !/BigInt\(/.test(script)) throw new Error("BigInt formatters with a self-test");
    return true;
  }));
  cases.push(fixture("console_csp_hashes_in_netlify_toml_match_the_inline_script_and_style_blocks", () => {
    const fs = require("fs"), path = require("path"), crypto = require("crypto");
    const { script, style, root } = consoleSource();
    const tomlPath = [path.join(root, "netlify.toml"), path.join(__dirname, "..", "..", "netlify.toml")].find((p) => { try { return fs.statSync(p).isFile(); } catch { return false; } });
    if (!tomlPath) return true; /* the checkout root is not reachable from this bundle; the deploy-time check runs where it is */
    const toml = fs.readFileSync(tomlPath, "utf8");
    const block = toml.slice(toml.indexOf('for = "/investor.html"'));
    const csp = (block.match(/Content-Security-Policy\s*=\s*"([^"]+)"/) || [])[1];
    if (!csp) throw new Error("no CSP for /investor.html");
    const h = (s) => `'sha256-${crypto.createHash("sha256").update(s, "utf8").digest("base64")}'`;
    if (!csp.includes(`script-src 'self' ${h(script)}`)) throw new Error("script hash mismatch — recompute with scripts/investor/csp-hashes");
    if (!csp.includes(`style-src 'self' ${h(style)}`)) throw new Error("style hash mismatch");
    for (const d of ["default-src 'self'", "connect-src 'self'", "object-src 'none'", "base-uri 'none'", "frame-ancestors 'none'", "form-action 'self'"]) if (!csp.includes(d)) throw new Error(`csp directive missing: ${d}`);
    if (/unsafe-inline|unsafe-eval/.test(csp)) throw new Error("no unsafe-* in the console CSP");
    if (!/Cache-Control = "max-age=0, no-cache, no-store, must-revalidate"/.test(block)) throw new Error("the console must revalidate on every load");
    return true;
  }));
  /* ── D-9: no credential material may be reachable from the deployed build.
     A tracked private key was found at netlify/functions/secrets/ (mode 644,
     PEM). Rotation at the provider is the control that matters; this check is
     what stops a reintroduced credential from deploying quietly: it runs in
     the same attestation set that controlAllowsEntry reads, so a key in the
     tree halts trading on that build instead of merely warning. It scans
     (1) every exported function of every investor module, (2) any secrets
     directory reachable from the bundle or the checkout, and (3) the
     repository's ignore rules when the checkout root is reachable. */
  cases.push(fixture("no_private_key_material_reachable_from_the_build", () => {
    const fs = require("fs"), path = require("path");
    const PEM = /-----BEGIN [A-Z ]*PRIVATE KEY-----/;
    const MARK = /PRIVATE KEY/;
    const BLOB = /[A-Za-z0-9+/=]{400,}/;
    const modules = { M, S, L, R, V, SH, C, U, B, I, T, AL, RS, strategy,
      K: require("./investorKick"), O: require("./_investorOpenai"),
      AU: require("./_investorAuth"), AD: require("./_investorAdmin"),
      XP: require("./_investorExitPolicy"), ST: require("./_investorStrike"),
      IS: require("./_investorIntelligenceSources"), E: require("./_investorEvidence") };
    const offenders = [];
    for (const [name, mod] of Object.entries(modules)) {
      for (const [k, v] of Object.entries(mod || {})) {
        const src = sourceOf(v);
        if (PEM.test(src) || MARK.test(src)) offenders.push(`${name}.${k}`);
      }
    }
    const roots = [__dirname, process.cwd(), path.join(process.cwd(), "netlify", "functions")];
    const dirs = new Set();
    for (const root of roots) {
      dirs.add(path.join(root, "secrets"));
      dirs.add(path.join(root, "gcp-secrets"));
      dirs.add(path.join(root, "gcp-secrets", "secrets"));
    }
    for (const dir of dirs) {
      let names = [];
      try { names = fs.readdirSync(dir); } catch { continue; }
      for (const n of names) {
        let text = "";
        try { text = fs.readFileSync(path.join(dir, n), "utf8").slice(0, 65536); } catch { continue; }
        if (PEM.test(text) || MARK.test(text) || BLOB.test(text)) offenders.push(path.join(dir, n));
        else offenders.push(`${path.join(dir, n)} (unexpected file in a secrets directory)`);
      }
    }
    /* Repository hygiene, only where the checkout root is visible (build box,
       local run). The bundle has no .gitignore and is not penalised for it. */
    const repoRoots = [path.join(__dirname, "..", ".."), process.cwd()];
    for (const root of repoRoots) {
      if (!fs.existsSync(path.join(root, "netlify.toml"))) continue;
      let ignore = "";
      try { ignore = fs.readFileSync(path.join(root, ".gitignore"), "utf8"); } catch {}
      if (!/^netlify\/functions\/secrets\/$/m.test(ignore)) offenders.push(`${root}/.gitignore lacks netlify/functions/secrets/`);
      if (!/^\*PrivateKey\*$/m.test(ignore)) offenders.push(`${root}/.gitignore lacks *PrivateKey*`);
      break;
    }
    if (offenders.length) throw new Error(`credential material reachable: ${offenders.slice(0, 6).join("; ")}`);
    return true;
  }));

  const pass = cases.every((c) => c.pass);
  /* The count is inside the hash: silently dropping a fixture must change
     the attestation, not merely shorten the list behind an unchanged one. */
  const SCHEMA = "runtime-fixtures-v42-fund-manager-build";
  const fixtureHash = digest({ schema: SCHEMA, count: cases.length, cases });
  return { schema: SCHEMA, pass, fixtureHash, passed: cases.filter((c) => c.pass).length,
    total: cases.length, cases };
}

/** Await every pending (asynchronous) fixture and recompute the attestation.
 *  The bootstrap and the health action use this; runFixtures() remains for
 *  callers that can only run synchronously and reports async cases as failed. */
async function runFixturesAsync() {
  const r = runFixtures();
  for (const c of r.cases) {
    if (!c.pending) continue;
    try {
      const detail = await c.promise;
      c.pass = detail !== false;
      c.detail = detail === true ? null : detail;
      delete c.error;
    } catch (e) {
      c.pass = false;
      c.error = String(e.message).slice(0, 200);
    }
    delete c.pending; delete c.promise;
  }
  const pass = r.cases.every((c) => c.pass);
  const fixtureHash = digest({ schema: r.schema, count: r.cases.length, cases: r.cases });
  return { ...r, pass, fixtureHash, passed: r.cases.filter((c) => c.pass).length };
}

module.exports = { canonical, digest, fixture, runFixtures, runFixturesAsync };
