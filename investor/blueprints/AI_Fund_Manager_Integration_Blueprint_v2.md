# Investor AI — AI Fund Manager Integration Blueprint v2

**Supersedes:** `AI_Fund_Manager_Implementation_Blueprint.md` (v1, 2026-09-03, 2,256 lines)
**Repository:** `Pakonieczny/GoKu_Shipping`
**Baseline commit reviewed:** `ea14b71` — strategy v18, bootstrap 25, roster v6 (304 names)
**Revision date:** 2026-09-04
**Deliverable type:** implementation plan. This document does not alter the running application and does not authorize live trading.

---

## 0. Status and relationship to v1

v1 is retained in full. This document does three things and nothing else:

1. **Adds a blocking Phase −1** in front of v1's Phase 0. Nine verified defects in the current plumbing are remediated before any AI Fund Manager work begins. Six of the nine appear nowhere in v1.
2. **Amends specific v1 sections** where v1 is factually wrong about the current system, or silent where it must not be. Each amendment names the v1 section and gives replacement text.
3. **Adds four invariants, six blocking test suites and four runtime hard gates** that v1 does not contain.

Every v1 section not named in §4 or §6 below carries forward **unchanged**. In particular v1's authority model (§3), mandate contract (§7), execution plane (§8), persistence model (§10) and sizing precedence (§15.1) are adopted as written — they are the strongest part of v1 and this revision does not touch them.

### Why Phase −1 exists

Six of the nine defects below are in the **plumbing**, not the strategy. They survive the replacement of the decider. If the AI Fund Manager is built on top of them:

- the model's mandates will be evaluated against prices that never existed (D-2),
- every model failure will be converted into a purchase (D-1),
- the evidence sweep the model depends on will remain ~300× over-subscribed (D-6),
- the pre-market Manager Meeting cannot be dispatched at all (D-8), and
- the coverage run will silently exhaust its budget on day one (D-8b).

The AI would then be scored on outcomes it did not produce. Phase −1 is not cleanup. It is the precondition for the measurement that v1's entire learning architecture (§16) depends on.

---

## 1. Verified defect register

Every row was verified by direct inspection at commit `ea14b71`. "v1 coverage" states whether v1 addresses it.

| ID | Defect | Evidence | v1 coverage | Severity |
|---|---|---|---|---|
| **D-1** | Every AI failure mode is converted into a BUY | `_investorOpenai.js:309,312,325,331,372,379,395,404,435,442,447` → `cause:"evidence_pending"`; `_investorSignal.js:640-649` `default:` returns `{trade:true, side:"long", relaxed:true}` | **absent** | Critical |
| **D-2** | Exits are decided on bar close, never on bar high/low | `_investorStrike.js:443` `strikeVerdict(last.c, plan)`; `_investorPositionGuard.js:373` `currentPrice: last.c`; `_investorExitPolicy.js` references `.h`/`.l` zero times | §1 item 8, §2 L77, §16.3 | Critical |
| **D-3** | The pre-registered strategy is structurally unable to place an order | `_investorSignal.js:683,690`; `_investorStrategy.js:230-231` (`requireCalibratedEdge:true`, `calibratedExpectedEdgeBps:null`); `_investorCalibration.js:33-40` requires 252+126+126+2×15 = **534** sessions | **absent** | Critical |
| **D-4** | The lane that actually traded runs with the declared risk controls disabled | `_investorStrategy.js:313` (`costMarginMultiple` 0.25 vs declared 2.0 at `:229`), `:329` (`minAdvUsd` $50M vs declared $300M at `:201`), `:478-480,483-484` (five exposure/loss breakers all at 100% of NAV) | **absent** | Critical |
| **D-5** | All 15 frozen shadow variants carry the exit defect v18 fixed | `_investorVariants.js` — `grep -c takeProfitPct` → **0**; all inherit `takeProfitPct:null` and the +3% arm / −4% giveback | **absent** | Major |
| **D-6** | Company-agnostic feeds are fetched once per company | `_investorIntelligenceSources.js:485` keys state `` `${sourceId}_${symbol}` ``; 10 of 25 sources are unambiguously global (`kind:"feed"`) → ~3,040 requests where ~10 suffice | **absent** | Major |
| **D-7** | `plainBlock()` is declared twice; the second silently wins | `investor.html:3710` and `:3744` | §14.5 L1313 | Moderate |
| **D-8** | No job can be dispatched before 09:45 ET | `investorKick.js:81` `PLAN_EARLIEST_MIN = 9*60+45`, enforced at `:101` | **absent** | Major |
| **D-8b** | AI budget ceilings would block the Manager Meeting silently, and the block routes to D-1 | `_investorOpenai.js:62-63` `DAILY_USD_CEILING` $5, `CYCLE_CALL_CEILING` 12; exhaustion returns `cause:"evidence_pending"` at `:331` | **absent** | Critical |
| **D-9** | A live private key is tracked in the repository | `netlify/functions/secrets/gcpPrivateKey.txt` — git-tracked, mode 644, 1,734 bytes, begins `-----BEGIN PRIVATE KEY-----`; no `.gitignore` exists | §18 | Critical |

**Note on D-4.** The relaxed values are inside the declared clamp (`_investorStrategy.js:52` permits `costMarginMultiple` down to 0.25; `:58` permits `minAdvUsd` down to $5e7). This is an operator-selectable floor, not a code defect. It is listed because every performance conclusion drawn from the exploratory lane was drawn at that floor, and v1 draws conclusions from that lane without saying so.

---

## 2. Corrections to v1 §2 — "What the repository does today"

v1 §2 (L46-79) describes the current system as a working deterministic residual-reversal desk whose weakness is that it lacks judgment. Three corrections are required, because they change what the AI is being compared *against*.

**Replace v1 §2's characterization of the strategy with:**

> The repository contains two strategy configurations, not one.
>
> The **pre-registered strict configuration** (`_investorStrategy.js:168-300`) carries the full research discipline — a 2.0× cost-margin hurdle, a $300M ADV floor, a t>3.0 significance hurdle, a 0.5 decay haircut, and a declared retirement condition at 200+ closed positions. It also declares `requireCalibratedEdge: true` with `calibratedExpectedEdgeBps: null`. `_investorSignal.js:683` therefore evaluates `calibratedPass` as `calibrationKnown && calibrated > 0`, which is false, and `:690` gates every proposal on it. `_investorCalibration.js:33-40` cannot return a verdict until 534 chronological sessions exist. **This configuration has never placed an order and cannot place one for approximately two years.** No performance conclusion of any kind may be drawn about it.
>
> The **exploratory paper configuration** (`_investorStrategy.js:308-335`, `paperLearningDefaults`) is what produced every trade in the record, including the 38 round trips cited in the v18 note. It runs at `costMarginMultiple` 0.25 (one eighth of the declared hurdle), `minAdvUsd` $50M (one sixth of the declared floor, against a slippage model that charges 12bp half-trip below $100M ADV), `positionScale` 3, and with all five exposure and loss breakers at 100% of NAV (`:478-484`).
>
> **Consequence for this project.** The comparison "AI Fund Manager versus the deterministic strategy" is not currently available. What exists is a comparison against a deliberately loosened paper configuration whose measured results are additionally corrupted by D-2. v1 §16.5's control arm "frozen v18 legacy control" must be restated (see §4.6 below).

**Replace v1's description of the evidence plane with:**

> The evidence plane is composed entirely of free public sources: GDELT discovery indexes, SEC EDGAR, Federal Register, USAspending, ten government RSS feeds, and NWS alerts (`_investorIntelligenceSources.js`). Market data is Alpaca. There is **no** fundamentals vendor, **no** consensus-estimate feed, **no** transcript source, and **no** confirmed earnings calendar — `investor/universe/v1.json` carries `earningsDates: []` with `earningsDatesSource: "operator_entry_required"` for every name, and `cikSource: "unresolved_pending_sec_map"` for a substantial minority. The only XBRL fact retrieved anywhere in the codebase is `dei:EntityCommonStockSharesOutstanding`.
>
> The dossier fields v1 §5.4 requires the manager to reason over — `revenueGrowthBps`, `fcfMarginBps`, `netDebtEbitdaMilli`, `forwardMultipleMilli` — have **zero sources in this repository**. v1 §22 question 3 correctly identifies vendor selection as open. This document elevates it from an open question to a **Phase 0 blocking dependency with a priced line item** (see §4.5).

---

## 3. Phase −1 — Plumbing remediation (blocking; precedes v1 Phase 0)

Nine work items. Each is independently deployable and independently testable. None requires an AI decision. Exit criterion for Phase −1: all nine acceptance tests green in CI, and thirty consecutive sessions recorded with no regression.

### P−1.1 — Fail-closed evidence classification (D-1, D-8b)

**Problem.** `_investorOpenai.js` collapses eleven distinct outcomes into the single token `evidence_pending`: missing API key, incomplete source coverage, lease contention, exhausted daily budget, unreachable model, HTTP error, unexpected tool call, unparseable output, schema-invalid cause, model abstention, and failed verbatim citation verification. `_investorSignal.js:640-649` treats that token as *absence of information* and — when `paperAbstainOnMissingInfo` is true, which it is in the exploratory lane — returns `{trade:true, side:"long", confidence:0.2, relaxed:true}`.

The result is that a rejected model output, a model that could not be reached, and an exhausted budget all become purchases at reduced size.

**Fix.** Split the single token into three causes with distinct semantics:

| Cause | Meaning | Permits new risk? |
|---|---|---|
| `evidence_not_yet_gathered` | The sweep has not reached this name. Absence of information. | Yes, at reduced size, only where `paperAbstainOnMissingInfo === true` |
| `evidence_insufficient` | The model was reached, returned validly, and reported it cannot determine cause from the supplied sources. A finding of insufficiency. | Yes, at reduced size, only where `paperAbstainOnMissingInfo === true` |
| `evidence_unavailable` | System failure or rejected output: no key, budget exhausted, lease busy, unreachable, HTTP error, unexpected tool call, unparseable, schema-invalid, abstained, or citation verification failed. **Not a finding.** | **Never** |

`_investorOpenai.js` maps `:309, :312, :325, :331, :372, :379, :395, :404, :435, :442, :447` to `evidence_unavailable`. Only the model's own returned enum value maps to `evidence_insufficient`.

`_investorSignal.js` `directionFromCause` gains an explicit case:

```js
case CAUSE.EVIDENCE_UNAVAILABLE:
  // A failure to obtain a verdict is not a verdict. This branch never
  // permits new risk, in any cohort, at any size, regardless of
  // paperAbstainOnMissingInfo. It is the fail-closed boundary.
  return { trade: false, side: null, confidence: 0,
           reason: "evidence unavailable — classification did not complete" };
```

and the `default:` branch is changed to return `{trade:false}` unconditionally, so that any cause added later fails closed rather than open.

**Acceptance.** A test that, for each of the eleven failure paths in `_investorOpenai.js`, asserts `directionFromCause(...).trade === false` with `paperAbstainOnMissingInfo: true` set. This is a blocking suite (§6.1).

**Relation to v1.** v1 §8 and §17.4 assert fail-closed behaviour throughout ("it does not coerce malformed text into an order", L1645). v1 never states that the current system does the opposite. Without P−1.1 the v1 executor would inherit an open failure path beneath its closed one.

### P−1.2 — Touch-based exit evaluation (D-2)

**Problem.** Bars carry `{o,h,l,c}` (`_investorMarket.js:751`) and are integrity-checked against their own high/low (`:959-961`), but the exit path reads only `c`. A one-minute bar can trade through a target or a stop and close back inside the band; the system records no event. This understates realized gains and hides realized losses simultaneously.

**Fix, in three parts.**

1. `_investorExitPolicy.js` — `evaluateExit` accepts `{ mark, barHigh, barLow, entry, peak }`. The hard stop and trailing stop test against `barLow`; the profit target and pullback target test against `barHigh`. `mark` is retained only for reporting `pnlPct`.
2. **Collision rule.** When a single bar's `[l,h]` range contains both the protective level and the target, and no finer-grained sequence is available, the **adverse** outcome is taken. Where the collision cannot be resolved and the position's status materially depends on it, the interval is recorded `UNSCORABLE` and excluded from performance statistics rather than resolved favourably. This adopts v1 §8.5 ("a touch is not a fill") as a hard rule rather than a paper-fidelity note.
3. `_investorStrike.js:443` — `strikeVerdict` receives `{ low: last.l, high: last.h, close: last.c }`. An arm level is struck on `low <= armBelowUsd`; the `gap_below_band` determination uses `low` likewise.

**Production answer.** Per v1 §1 item 8 and §8.4, the durable fix is to lodge the authorized protective and target orders with the broker as native limit/OCO/bracket orders, so the broker's engine watches continuously. Polling remains the reconciliation path, not the trigger path. P−1.2 makes the polling path honest in the interim and makes paper simulation faithful permanently.

**Acceptance.** Fixture: entry $100, target +2%, bar `{o:101, h:103, l:100.5, c:100.8}`. Pre-fix the position is held; post-fix it exits at the target. Mirror fixture on the stop side. Collision fixture asserts the adverse outcome. Blocking suite (§6.2).

### P−1.3 — Declare the calibration state (D-3)

**Problem.** The strict configuration is order-blocked for ~534 sessions and nothing in the system or the UI says so. v1 §16.5 proposes "frozen v18 legacy control" as an experimental arm without noting that the strict arm cannot trade.

**Fix.** No behavioural change to the gate — it is correct and should stay. Three disclosure changes:

1. `_investorSignal.js` returns a structured block reason `calibration_unavailable` carrying `{ sessionsAvailable, sessionsRequired, earliestEligibleSession }` rather than a generic failure.
2. The console surfaces this as a first-class state, not a silent no-op: *"the pre-registered strategy is not eligible to trade — N of 534 sessions recorded."*
3. `StrategyVersions` records, for every version, whether it was ever order-eligible. Any performance claim about a version that was never order-eligible is rejected at write time.

**Acceptance.** A test asserting that a strict-configuration signal evaluation returns `calibration_unavailable` with a finite `sessionsRequired`, and that the dashboard payload carries it. Blocking suite (§6.3).

### P−1.4 — Configuration parity and provenance (D-4)

**Problem.** Results were produced at `costMarginMultiple` 0.25, `minAdvUsd` $50M and five breakers at 100% of NAV, and are reported without those facts attached.

**Fix.** No parameter changes — the operator may keep whatever floor they choose. What changes is that the floor becomes part of the record:

1. Every closed position, every daily NAV row and every KPI row carries a `riskConfigHash` over the effective parameter set actually in force at the moment of the decision, plus an explicit `deviationsFromDeclared` array naming each parameter whose effective value differs from the frozen strategy's declared value, with both values.
2. The console renders a persistent banner whenever any breaker is at a disabling value or any hurdle is below its declared value: *"risk controls reduced — results are not comparable to the declared strategy."*
3. Any performance comparison across two periods with different `riskConfigHash` values is labelled non-comparable in the analytics view rather than being charted as one series.

**Acceptance.** Test asserting `deviationsFromDeclared` is non-empty for the current exploratory configuration and names `costMarginMultiple`, `minAdvUsd`, and the five breakers. Blocking suite (§6.4).

### P−1.5 — Variant parity (D-5)

**Problem.** The 15 frozen variants in `_investorVariants.js:31-152` (15 variants, ids A–Q) predate v18 and declare none of `takeProfitPct`, `trailingArmsAtPct`, `trailingStopPct`, `holdLosersThroughRankExit`, `requireSessionMove`. They inherit `takeProfitPct: null` and the +3%-arm/−4%-giveback trailing stop. The shadow harness is therefore running 15 copies of the defect v18 was created to fix, and any variant that "wins" wins under the old exit regime.

**Fix.** Two options; the operator picks one and the choice is recorded.

- **(a) Re-freeze.** Mint a new variant generation `v19-*` in which every variant inherits the v18 exit parameters, and retire the 15 pre-v18 variants with an explicit `retiredReason: "pre-v18 exit regime"`. Prior results are preserved and marked as belonging to the old regime.
- **(b) Declare.** Keep the 15 variants and add an explicit `exitRegime: "pre-v18"` field, and block any promotion of a pre-v18 variant into a post-v18 configuration.

(a) is recommended. Either way, **silently mixing regimes is closed**: `_investorVariants.js` gains a validator that rejects a variant whose exit parameters are undeclared.

**Acceptance.** Test asserting every registered variant declares a complete exit parameter set, or is explicitly tagged with a retired regime. Blocking suite (§6.5).

### P−1.6 — Source scope correction (D-6)

**Problem.** `readState(sourceId, symbol)` at `_investorIntelligenceSources.js:485` keys polling state as `` `${sourceId}_${symbol}` `` for every source. Ten of the twenty-five sources are `kind:"feed"`, and `nws.alerts` (`kind:"nws_alerts"`) is arguably an eleventh — DOL, NLRB, FTC, DOJ, FAA, NTSB press, NTSB investigations, defense contracts, NWS alerts and equivalents — and are not company-specific. Each is therefore fetched once per company in scope.

**Fix.**

1. Add `scope: "global" | "company"` to every source descriptor. All ten `kind:"feed"` sources are `global`; `nws.alerts` is classified during implementation (it is a geography-filtered endpoint, so it may be global-with-parameters rather than per-company); GDELT queries, `company.direct`, `federal.register` and `usaspending` are `company`.
2. `readState` keys global sources by `sourceId` alone.
3. A global source is fetched at most once per sweep, its items are recorded once, and **entity resolution fans the resulting items out to companies** rather than the fetch being repeated per company.
4. Add a source-budget assertion: a sweep that issues more than `(globalSources × 1) + (companySources × companiesInSweep)` requests fails the sweep and raises a typed alert rather than silently continuing.

**Effect.** For a 304-name pass the global-feed request count falls from ~3,040 to ~10.

**Acceptance.** Test asserting a two-company sweep issues exactly one request per global source. Blocking suite (§6.6).

**Relation to v1.** v1 §11.1 gives `ingest` its own continuous bounded queue and §5.5 defines evidence deltas. Neither is achievable at the current fetch multiplier; P−1.6 is the precondition for v1's ingest design.

### P−1.7 — Remove the duplicate UI declaration (D-7)

`plainBlock` is declared at `investor.html:3710` (plan-block reasons, backed by `PLAN_BLOCK_PLAIN`) and again at `:3744` (sell-state reasons). JavaScript hoisting keeps the second, so `PLAN_BLOCK_PLAIN` is dead and plan-block messages render from the wrong dictionary.

**Fix.** Rename to `plainPlanBlock` and `plainSellBlock`, update call sites, merge the dictionaries only if the merge is verified non-lossy.

**Acceptance.** A DOM test asserting a known plan-block code renders its `PLAN_BLOCK_PLAIN` text. Included in the UI suite (§6.7). This item is superseded by v1 §14 if and when the UI is rewritten; it is fixed now because the UI rewrite is deferred (§7).

### P−1.8 — Scheduler window for pre-market work (D-8)

**Problem.** `investorKick.js:81` sets `PLAN_EARLIEST_MIN = 9*60+45` and `:101` rejects any configured time outside `[09:45, 15:30]`. v1 §11.1 requires a `premarket_manager` task between the evidence cutoff and 09:15 ET. It cannot be dispatched.

**Fix.** The window becomes per-task rather than global:

| Task class | Window | Rationale |
|---|---|---|
| `scan` / opportunity (existing) | 09:45–15:30 ET | Unchanged. The current comment is correct: scans are meaningful only after the opening auction and need time before the close. |
| `premarket_manager`, `ingest`, `focused_research` | 04:00–09:15 ET | Model and evidence work; no order is placed. |
| `execute` | market hours + extended per broker | Unchanged path. |
| `postclose`, `archive` | after official close | Unchanged path. |

`normalizePlanTimes` is parameterized by task class rather than reading a module-level constant. The existing constant is retained as the `scan` window so current behaviour is bit-identical.

**Acceptance.** Test asserting `premarket_manager` is dispatchable at 08:30 ET and `scan` is not. Blocking suite (§6.8).

### P−1.9 — Credential remediation (D-9)

**Problem.** `netlify/functions/secrets/gcpPrivateKey.txt` is git-tracked, mode 644, 1,734 bytes, and is a genuine PEM private key. There is no `.gitignore` in the repository.

**Fix.** In this order:

1. Identify the principal and **rotate/revoke the key at the provider**. Treat it as compromised from the moment of first commit, not from discovery.
2. Inventory deployments and review access logs for the key's lifetime.
3. `git rm --cached` the file; add `.gitignore` covering `netlify/functions/secrets/`, `*.pem`, `*_rsa`, `*PrivateKey*`.
4. Purge from history where the repository's sharing model warrants it. Note that rotation, not purging, is the control that matters; purging without rotating is theatre.
5. Replace with a managed secret injected at runtime. No source file currently references `gcpPrivateKey`, so removal is behaviourally inert — verify this with a grep before and after.
6. Add secret scanning to CI (§6.9).

**Sequencing.** Ship alone, first, before any other change in Phase −1.

**Relation to v1.** v1 §18 states this correctly and is adopted verbatim. It is restated here only to place it in the executable sequence, and to add the observation that no code references the file — which makes step 5 free.

---

## 4. Amendments to v1 by section

### 4.1 — v1 §3 "Target authority model": add the fail-closed evidence boundary

v1's authority model says deterministic code "may reject or reduce an AI proposal, never improve it." Add, as a stated consequence:

> **The absence of an AI proposal is not a proposal.** Where the manager cannot be reached, cannot be afforded, returns unparseable output, or returns output whose citations fail verification, the system holds. It does not fall back to a prior regime, a cheaper model, a heuristic, or a reduced-size version of the proposal it did not receive. This is the fail-closed boundary named in D-1; the current system violates it and P−1.1 restores it.

### 4.2 — v1 §8.4/§8.5: promote the touch rule from paper fidelity to hard rule

v1 §8.5 states the touch/fill convention as a paper-fill-fidelity requirement. Promote it:

> The TOUCH / FILL / UNSCORABLE convention governs **every** exit evaluation in the system — live, paper, shadow, backtest and KPI — not only paper simulation. A protective or target level is evaluated against the bar's high and low. A bar whose range contains both levels with no finer sequence resolves adversely or is marked UNSCORABLE; it never resolves favourably. Any performance figure computed under a close-only convention is invalid and must be recomputed or discarded.

### 4.3 — v1 §9.4/§9.5: reconcile the operating budget with the enforced ceiling

v1 §9.4 provisions $4–$10 on a normal day and $10–$20 on a busy day. `_investorOpenai.js:62-63` enforces `DAILY_USD_CEILING` $5 and `CYCLE_CALL_CEILING` 12. v1 never reconciles the two, and exhaustion currently routes into D-1.

Amend §9.5 with:

> The enforced ceilings are configuration, not commentary. Before Phase 3, `INVESTOR_OPENAI_DAILY_USD` and `INVESTOR_OPENAI_CYCLE_CALLS` are raised to the provisioned figures and are treated as a **reservation**, not a cap on correctness: when the reservation is exhausted, the run abstains visibly under `evidence_unavailable` (P−1.1) and activates no new BUY mandate. It never degrades to a cheaper model for investment judgment (v1 §9.5 item 4, retained) and never falls back to v18 (item 5, retained), and — new — it never converts the exhaustion into a reduced-size trade.
>
> Additionally, v1 §9.3's table omits two terms that dominate variance. Both must be budgeted and instrumented before Phase 3:
> - **Reasoning tokens.** v1 L764 correctly states these are a subset of billed output rather than a separate charge. They are nonetheless the largest uncertain term: at 500 / 1,000 / 1,500 reasoning tokens per name, the 304-name coverage call alone costs approximately **$3.04 / $6.08 / $9.12**, the upper figure exceeding v1's entire normal-day envelope in a single call.
> - **Cache writes.** A first write is 1.25× ordinary input; only confirmed later reads receive the 0.1× rate. v1 §9.4 says not to assume a hit rate. The budget must therefore be provisioned at the **zero-hit-rate** figure and reduced only once `cache_write_tokens`, `cached_tokens` and realized savings are measured.
>
> The one-time 304-name dossier bootstrap remains the largest single bill in the project and is currently unbounded. It is given an explicit ceiling and a resumable checkpoint before it is run.

### 4.4 — v1 §11.1: scheduler windows

Amend the scheduler table to carry the per-task-class windows from P−1.8, and add:

> The existing `PLAN_EARLIEST_MIN` / `PLAN_LATEST_MIN` window applies to the `scan` task class only and is retained unchanged for it. Task classes that place no order — `ingest`, `premarket_manager`, `focused_research` — carry their own windows. A task class with no declared window is not dispatchable.

### 4.5 — v1 §22 question 3: promote the data vendor from open question to priced dependency

v1 §22 lists vendor selection as one of nine questions "to be answered by measured implementation." Given §2's correction — that four of the dossier's economic fields have no source at all — it is not a research question but a purchase.

> **Phase 0 blocking dependency.** Before Phase 2 begins, the following must be selected, priced, contracted, and rights-verified for automated use: point-in-time news with corrections, consensus estimates and revisions, transcripts or their absence justified, guidance history, a corporate-actions feed, and an exchange calendar. Until then, `revenueGrowthBps`, `fcfMarginBps`, `netDebtEbitdaMilli` and `forwardMultipleMilli` are **absent**, not null — the dossier schema must distinguish the two, and the manager must be shown their absence explicitly rather than being handed a null it may read as a value.
>
> A manager reasoning about expectation gaps from GDELT headlines (`full_text_allowed: false`), EDGAR filings and one XBRL field is not the system v1 describes. This dependency is stated with its annual cost before Phase 2 is authorized.
>
> The exchange calendar in particular is already a live defect: `_investorMarket.js:372-414` derives holidays by hand, including an Easter computus (`easterSunday()` at `:372`). v1 L167 requires an authoritative versioned calendar. Alpaca's `/v2/calendar` is already available to this account and should replace the hand-derived table in Phase −1 or Phase 0.

### 4.6 — v1 §16.5: restate the control arms

v1 §16.5 names "frozen v18 legacy control" as experimental arm 2. Per §2 above, that arm is ambiguous. Replace with:

> **Arm 2a — frozen exploratory control.** The v18 exploratory configuration as actually run, at its actual `riskConfigHash`, with its deviations from the declared strategy attached to every row (P−1.4), and with exits recomputed under the touch convention (P−1.2). This is the honest comparator and the only one with a track record.
>
> **Arm 2b — declared strict configuration, shadow only.** The pre-registered configuration with `requireCalibratedEdge` observed but not enforced, run in shadow so that the strategy the repository actually declares finally generates a record. It is labelled *shadow, non-eligible* and its results are never presented as the strict strategy's live performance.
>
> Arms 1 (AI Fund Manager), 3 (declared benchmark / equal weight) and 4 (matched random and factor baselines) carry forward from v1 unchanged.

### 4.7 — v1 §16.3 and §19: declare the acceptance numbers

v1 §16.3 and §17 specify metrics correctly — Deflated Sharpe, PBO, clustering by decision date, block bootstrap, effective sample size — and v1 §16.4 L1568 correctly warns that sixty sessions is an operational soak only. But **every threshold in v1 is written in the future tense** ("predeclared", "fixed before promotion") and no number appears anywhere in 2,256 lines.

This is the single most consequential omission in v1, because portfolio-return significance is unreachable on the owner's timescale: detecting a true annualized Sharpe of 0.5 at 80% power and 5% two-sided requires roughly 31 years of daily returns; Sharpe 1.0 roughly 8 years; Sharpe 2.0 roughly 2 years. The repository's own strict configuration already implies ~1,137 trading days before its own hurdle can be evaluated.

> **Amendment.** Promotion is gated on **forecast-quality** metrics, which resolve in months, not on portfolio return, which does not. Before Phase 3 begins, the following are written down as numbers and frozen:
>
> | Metric | Resolves in | Threshold |
> |---|---|---|
> | Matched selection lift vs. matched-random, clustered by decision date | weeks–months | *declare before starting* |
> | CRPS / quantile loss on the manager's own declared outcome buckets | weeks | *declare before starting* |
> | Calibration error on predeclared, dated, binary events | weeks | *declare before starting* |
> | Coverage completeness and citation-verification pass rate | days | 100% / *declare* |
> | Post-cost portfolio return vs. arms 2a, 3, 4 | years | reported, **not** a gate |
>
> A stop condition is declared alongside each start condition. The experiment must be able to fail, on a stated number, within a stated horizon. v1 §19's Phase 7 exit criterion — "enough evidence to decide the next experiment" — is replaced by these thresholds.

### 4.8 — v1 §19: a merit gate inside the delivered scope

v1's phase plan gates plumbing at every phase in scope. Its only investment-merit criterion (L1807) gates Phase 8, which L1753 places out of scope. The plan can therefore complete in full without ever testing whether the AI adds value.

> **Amendment — new Phase 3.5, blocking.** After Phase 3 (Sol in observation mode) and before Phase 4 (mandate and paper executor), the §4.7 forecast-quality thresholds are evaluated on accumulated shadow decisions. If they are not met, Phase 4 does not begin. The project either iterates within Phase 3 or stops. **No further build occurs on unmet merit.**

### 4.9 — v1 §14: defer the UI rewrite

v1 §14 specifies a 15-module rewrite of `investor.html`. It is a 100% rewrite of the only interface the operator has, and it delivers no evidence about the central question.

> **Amendment.** The UI rewrite moves to Phase 6 as v1 has it, but is explicitly **out of scope until Phase 3.5 passes**. Until then, the existing console is extended in place with: the calibration-state banner (P−1.3), the reduced-risk-controls banner (P−1.4), a manager-run panel, and the D-7 fix. v1 §14.5's six enumerated current UI defects are fixed in place rather than dissolved into a rewrite that may not happen.

### 4.10 — v1 §13: dispositions corrected

v1 §13 dispositions are adopted with three corrections:

| File | v1 disposition | Corrected disposition | Reason |
|---|---|---|---|
| `_investorLedger.js` | rewrite into the new execution plane | **Port intact.** Its lifecycle is not rewritten in the same window as introducing a broker route that does not yet exist. | Integer double-entry, four conservation equations, content-hash idempotency and an execution clock that forbids filling on a bar at or before the decision. This is the strongest code in the repository and it is load-bearing for every arm of §16.5. |
| `_investorVariants.js` | retire | **Retire only after P−1.5 resolves the regime mismatch,** so prior variant results are preserved with their regime attached rather than discarded silently. | D-5 |
| `_investorSignal.js` | retire (superseded by manager judgment) | **Retire the entry-selection path; retain `directionFromCause`'s fail-closed boundary** as the pattern for the manager's own unavailability handling. | D-1 |

---

## 5. New invariants

These are stated as invariants because each is violated by the current system and each must hold before and after the AI Fund Manager exists.

**I-1 — Fail-closed evidence.** A failure to obtain a verdict is never a verdict. No system failure, budget exhaustion, contention, parse failure, or citation-verification failure may result in new risk, in any cohort, at any size. *(D-1, D-8b)*

**I-2 — Touch before close.** Every protective and target level is evaluated against the bar's high and low. Same-bar collisions resolve adversely or are marked UNSCORABLE. No performance figure is computed under a close-only convention. *(D-2)*

**I-3 — Source scope.** A source is fetched at a cadence determined by its own scope, never multiplied by the roster. Entity resolution fans results out; it does not fan requests out. *(D-6)*

**I-4 — Configuration provenance.** Every result row carries the hash of the risk configuration actually in force at the decision, and an explicit list of deviations from the declared strategy. Results produced under different configurations are never charted as one series. *(D-3, D-4, D-5)*

---

## 6. New blocking test suites

Added to v1 §17.2. All are `node --test`, all run in CI, all block merge. The repository currently has no test script, no test framework and no lockfile; v1 §17.5 specifies the CI sequence and is adopted as written.

| ID | Suite | Asserts |
|---|---|---|
| **6.1** | `test:investor:failclosed` | All eleven `_investorOpenai.js` failure paths yield `trade === false` under `paperAbstainOnMissingInfo: true`. The `default:` branch of `directionFromCause` returns `trade:false`. |
| **6.2** | `test:investor:touch` | Target-touch, stop-touch and same-bar-collision fixtures. Asserts adverse resolution on collision and UNSCORABLE where unresolvable. Includes a regression fixture reproducing the pre-fix close-only behaviour. |
| **6.3** | `test:investor:calibration-state` | Strict configuration returns `calibration_unavailable` with finite `sessionsAvailable`/`sessionsRequired`; dashboard payload carries it. |
| **6.4** | `test:investor:config-provenance` | Every result row carries `riskConfigHash`; `deviationsFromDeclared` names `costMarginMultiple`, `minAdvUsd` and the five breakers under the current exploratory configuration. |
| **6.5** | `test:investor:variant-parity` | Every registered variant declares a complete exit parameter set or carries a retired-regime tag. |
| **6.6** | `test:investor:source-scope` | A two-company sweep issues exactly one request per global source; the sweep-budget assertion fires when exceeded. |
| **6.7** | `test:investor:ui` | No duplicate top-level function declarations in `investor.html`; known plan-block codes render from `PLAN_BLOCK_PLAIN`. |
| **6.8** | `test:investor:scheduler` | `premarket_manager` dispatchable at 08:30 ET; `scan` not dispatchable at 08:30 ET; `scan` window bit-identical to current behaviour. |
| **6.9** | `test:secrets` | No PEM header, no `PRIVATE KEY` marker, no high-entropy credential in tracked files. Fails the build, not a warning. |

**Correction to v1 §17.4.** v1's build attestation inherits `controlAllowsEntry`, which unlocks trading on `fixturesPass && fixturesCommit === commit` (`_investorLedger.js:481`, check at `:499`). `_investorSelftest.js` contains zero references to `_investorOpenai.js` or `OPENAI` — verified by grep; no runtime fixture exercises the model path at all. That is tolerable today, when the model is one enum among thirteen blocking gates; it is not tolerable once the model is the investment authority. Suites 6.1 and 6.2 are added to the attestation set before Phase 3.

---

## 7. Revised phase plan

| Phase | Content | Gate to exit |
|---|---|---|
| **−1** | P−1.1 … P−1.9 (this document §3) | All nine suites green; 30 sessions with no regression |
| **0** | v1 Phase 0 freeze and baseline, **plus** the priced data-vendor dependency (§4.5) and the exchange calendar | Vendor contracted and rights-verified, or Phase 2 explicitly deferred |
| **1** | v1 Phase 1 — contracts, policy, indexes, authority tests | v1 gate, plus I-1…I-4 encoded as tests |
| **2** | v1 Phase 2 — evidence and dossier plane | v1 gate, plus source-scope budget assertion holding at 304 names |
| **3** | v1 Phase 3 — Sol in observation mode, **on 30–40 names, not 304** | Ten or more complete shadow sessions captured |
| **3.5** | **New, blocking.** Forecast-quality evaluation against the §4.7 thresholds | Thresholds met. **If not met, the project stops or iterates in Phase 3. No further build.** |
| **4–8** | v1 Phases 4–8 unchanged | v1 gates, plus §4.7's reporting requirements |

**On the reduced Phase 3 roster.** v1 §6.3's complete-coverage contract — that no score, rank or shortlist may hide a company from the manager — is a good principle and carries forward for the production system. It is not required to *test* whether the manager has skill. A 30–40 name shadow at full dossier depth answers the §4.7 questions at roughly 2% of the 304-name cost, and every one of those metrics is a per-decision metric that does not require the full roster to resolve. The roster expands to 304 at Phase 4, once merit is established.

---

## 8. Traceability

| Defect | Phase −1 item | Invariant | Test suite | v1 amendment | Runtime gate |
|---|---|---|---|---|---|
| D-1 | P−1.1 | I-1 | 6.1 | §4.1, §4.10 | Added to attestation set |
| D-2 | P−1.2 | I-2 | 6.2 | §4.2 | Added to attestation set |
| D-3 | P−1.3 | I-4 | 6.3 | §4.6 | Dashboard state |
| D-4 | P−1.4 | I-4 | 6.4 | §4.6 | Persistent banner |
| D-5 | P−1.5 | I-4 | 6.5 | §4.10 | Variant validator |
| D-6 | P−1.6 | I-3 | 6.6 | §4.4 | Sweep-budget assertion |
| D-7 | P−1.7 | — | 6.7 | §4.9 | — |
| D-8 | P−1.8 | — | 6.8 | §4.4 | Task-class window |
| D-8b | P−1.1 | I-1 | 6.1 | §4.3 | Reservation, not cap |
| D-9 | P−1.9 | — | 6.9 | §18 (v1, verbatim) | CI secret scan |

---

## 9. What this revision does not change

Stated explicitly so that the scope of the amendment is unambiguous.

- **v1 §3, §7, §8, §10, §15.1 are adopted as written.** The three-record authority split (`MandateProposal` / `MandateServerBinding` / `ActivationEnvelope`, separate schemas and hashes, the joined read model never accepted as model output), the model process holding no broker credential and no order tool, the `lotFloor(min[…])` sizing precedence in which the model's quantity is one input among eight and the validator may reduce but never raise, and the ordering of RISK_MAINTENANCE ahead of EXPANSION as separately committed plan classes — these are the strongest content in v1 and are taken unchanged.
- **v1 §18's risk table is adopted as written**, including the prompt-injection, source-poisoning, hallucinated-citation, temporal-leakage and model-drift controls.
- **The decision to replace the deterministic strategy as the investment authority is not revisited here.** That is the owner's call and it has been made. This document's position is narrower: the plumbing beneath the decider must be correct before the decider is replaced, or the replacement cannot be evaluated.

---

## 10. Open items this revision does not close

1. The §4.7 thresholds are specified as *required*, not *chosen*. The owner supplies the numbers before Phase 3. This document deliberately does not pick them.
2. v1 §22's remaining questions 1, 2, 4, 5, 6, 8 and 9 stay open and are unaffected by this revision.
3. Whether `_investorLedger.js`'s conservation equations survive the introduction of a real broker adapter is untested until a live paper account exists. It is the highest-risk untested assumption in the ported code.
4. The one-time dossier bootstrap cost remains unbounded until §4.3's ceiling is set against measured documents.
