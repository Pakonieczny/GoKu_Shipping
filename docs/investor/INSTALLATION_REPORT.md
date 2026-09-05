# Investor AI — AI Fund Manager: Installation Report (blueprint v2)

Branch: `claude/investor-app-overhaul-jebmm7` in `Pakonieczny/GoKu_Shipping`.
Base: `ea14b71` (v18 legacy desk). Head: `7a8804f`. Sixteen commits, all pushed.
Deploy attestation: 200 fixtures, all green (`schema=runtime-fixtures-v42-fund-manager-build`, 1.7 s).

This document records what was installed, how, and why, in enough detail for
an independent reviewer (human or AI) to audit the work against the blueprint
"Investor AI — AI Fund Manager Implementation Blueprint (v2)". Section numbers
in brackets (e.g. §6.3) refer to that blueprint.

---

## 1. What the system is supposed to do

The repository's Investor subsystem was a deterministic residual-reversal
"volatility desk" (strategy v18, universe v6) with an AI layer that only
classified news. It has been rebuilt into an **AI fund manager**:

- **Sol (`gpt-5.6-sol`, high reasoning)** is the only component that makes
  investment selections, comparisons and mandate terms. Every trading day it
  reviews the **entire frozen eligible roster (304 names in universe v6)**,
  revises every holding, researches the names it chooses, and issues a final
  portfolio synthesis with a unique capital rank per BUY.
- **Luna (`gpt-5.6-luna`)** only extracts source-bound facts from documents
  and independently verifies claims. It can never choose, rank, size or
  suppress a company. Terra is forbidden for investment work.
- **Deterministic code** owns everything else: evidence collection and
  hashing, exact arithmetic (money as canonical integer strings), the risk
  envelope that may only refuse or shrink a proposal, execution and
  reconciliation, the ledger, learning/KPIs, and the operator console.
- **Nothing asks a model when a price is touched.** Targets, stops and time
  exits execute mechanically from an already-authorised mandate.
- **Every failure is fail-closed.** A model error, budget exhaustion, missing
  evidence or incomplete coverage produces `ABSTAIN` / no new BUY, never a
  trade. Emergency de-risking is a separately owner-approved, hashed policy
  that can freeze, cancel entries or reduce, and can never open exposure.

### 1.1 The daily process (New York time)

| Window | Job | Handler | What happens |
|---|---|---|---|
| overnight → 08:30 | `ingest` | `investorIngest-background` | One sweep: global feeds once, per-company SEC/issuer sources, Luna extraction, claim recording, dossier versions; high-impact deltas routed. |
| 08:30 (evidence freeze) | `premarket_manager` | `investorManager-background` | The Manager Meeting: freeze cutoff → frozen roster snapshot → full-roster review by Sol (single context or balanced blocks) → structural coverage repair (once) → holding maintenance (hard deadline 09:15) → focused research → final synthesis (soft deadline 09:20) → risk envelopes → all-or-none activation → one decision row per symbol. |
| 09:30–16:00 | `execute` (every minute) | `investorExecution-background` | Expire/pause entries, revalidate operational limits, apply desired order sets through the broker adapter, simulate paper fills on OHLC bars, attach protection for exactly the owned quantity, assert ledger conservation. No model code is loaded. |
| intraday | `event_revision`, `focused_research` | `investorManager-background` | A high-impact evidence delta pauses an unfilled entry and asks Sol for a focused revision; prior protection is never widened. |
| after 16:20 | `postclose` | `investorPostclose-background` | Final marks, NAV, frozen forecast/outcome records for every decision, point-in-time resolution, matched counterfactuals, KPI row, typed alerts. |
| after 16:20 | `archive`, `audit_export` | `investorArchive-background` | Bounded content-addressed exports with an explicit retention manifest; the audit chain is never deleted. |

`investorKick` (cron, every minute) is the only scheduler. It enqueues due
jobs, launches at most four per tick in the order execute → event →
continuation → manager → ingest → postclose, and hands each worker a
single-use HMAC nonce bound to the job.

### 1.2 Decision vocabulary and authority

`BUY`, `WATCH`, `IGNORE`, `HOLD`, `REDUCE`, `SELL`, `ABSTAIN` (§15). A BUY is
invalid without an evidence-backed bear case and invalidators. `ABSTAIN` is a
first-class safe result. Precedence: emergency stop/freeze → data/corporate-
action integrity → broker legality → hard portfolio risk → active AI mandate.
The size authority is `Q_authorized = lotFloor(min[Q_AI, cash, name, sector,
planned-loss, stressed-loss, ADV, overnight capacities])`; a material clamp
sends the plan back to Sol for re-synthesis rather than silently trading a
different size.

---

## 2. Expected and desired outcomes

### 2.1 Capital deployment expectations

- The manager may deploy capital only through **complete, validated mandates**
  (proposal → server binding → activation envelope → reservation → desired
  order set) staged against a fresh activation snapshot with all-or-none CAS.
- **Coverage is a hard gate**: if the frozen roster is not covered exactly
  (304/304), no new BUY activates that day. Holdings are still maintained.
- **Bounds (paper defaults, `_investorPolicy.RISK_MANDATE`)**: single name
  ≤ 10 % of NAV, sector ≤ 25 %, correlated cluster ≤ 20 %, gross/net ≤ 95 %,
  settled cash reserve ≥ 5 %, open-order notional ≤ 30 %, planned loss per
  position ≤ 1 % and ≤ 5 % aggregate, stressed loss ≤ 2.5 % / ≤ 10 %, daily
  loss freeze at 2 %, drawdown states at 6 % (freeze expansion) and 12 %
  (emergency review), order ≤ 1 % of ADV, position ≤ 5 % of ADV, ADV ≥ $50 M,
  spread ≤ 50 bps, at most 8 actionable BUYs per run, long-only whole-share
  common equity in the regular session only.
- **Every BUY carries a target, a loss boundary, authorised sessions
  (≤ 3) and a time exit**; protection is attached for the owned quantity
  before the position is considered protected; unprotected intervals are a
  measured integrity failure.
- **AI spend is reserved before it is spent** (default $10/day reservation,
  operator-versioned), and exhaustion abstains instead of trading.
- Deployment therefore scales with **evidence, coverage and risk capacity**,
  not with market activity. An idle day with no attractive after-cost
  opportunity, or a day with incomplete coverage, correctly deploys nothing
  and says why (`noBuyReasons`).

### 2.2 Return expectations and how they are judged

The blueprint's definition of done (§20) does not promise a return; it
requires that the AI book **demonstrate incremental after-cost, risk-adjusted
value** against the frozen v18 baseline and a declared benchmark (SPY) with
identical point-in-time data and costs **before live capital is considered**.
The system now measures exactly that. Targets are predeclared in
`_investorKpi.KPI_DEFINITIONS` (32 KPIs). The headline ones:

| Layer | KPI | Target |
|---|---|---|
| Portfolio | Investment return (time-weighted) | above benchmark |
| Portfolio | Excess return vs SPY | > 0 bps |
| Portfolio | Owner economic return (return minus operating cost drag) | > 0 bps |
| Portfolio | Sharpe / Sortino / Calmar | Sharpe ≥ 1 after one year |
| Portfolio | Max drawdown / CVaR95 | drawdown no worse than −20 % |
| Portfolio | Expectancy / payoff / profit factor | profit factor > 1.3 |
| Selection | Matched selection lift (selected vs matched unselected) | > 0 bps |
| Selection | Abstention quality | net ≥ 0 bps |
| Research | Calibration (ECE) / CRPS / pinball loss | ECE ≤ 5 %; declining |
| Evidence | Universe coverage | ≥ 95 % fresh dossiers |
| Evidence | Material claim support | 100 % |
| Execution | Trigger capture (touch / fill) | ≥ 99 % / ≥ 95 % |
| Execution | Slippage vs benchmark | ≤ 10 bps |
| Execution | Protection latency | p95 ≤ 5 s, unprotected intervals = 0 |
| Operations | Integrity (duplicate/orphan/over-quantity/unprotected) | 0 |
| Cost | Operating cost drag | ≤ 50 bps per year |
| Governance | Unexplained decision flips; regression recurrence | 0 |

"Strong returns" in this system means: positive matched selection lift and
positive excess return after every cost (spread, slippage, data, AI), with
drawdown and integrity inside the mandate, sustained prospectively across
enough sessions that the deflated Sharpe and the effective sample size are
meaningful. The learning layer scores every decision and every unselected
comparison without look-ahead; no model, prompt or policy change is promoted
without task-specific evals and prospective shadow evidence plus explicit
version approval.

---

## 3. Installation log (commit order per §21)

Every commit keeps the paper ledger balanced and its fixture set green;
a red fixture halts trading on the deployed build (`controlAllowsEntry`).
Verification was run locally after every commit with the same
`_investorSelftest.runFixturesAsync()` the deploy uses.

| # | Commit | Files | What was installed and how |
|---|---|---|---|
| 1 | `d0963d9` security | `.gitignore`, `_investorSelftest.js`, `secrets/gcpPrivateKey.txt` (removed) | Tracked private key removed from the tree, ignore rules added, and a fixture (D-9) that scans every exported function and any reachable secrets directory for key material — a reintroduced credential fails the attestation. |
| 2 | `8064846` fail-closed evidence, touch exits, source scope, windows | 19 files incl. `_investorOpenai`, `_investorSignal`, `_investorExitPolicy`, `_investorStrike`, `_investorIntelligenceSources`, `investorKick`, `investor.html` | Group-1 defect fixes D-1…D-8b: every gateway failure is `evidence_unavailable` and never a trade; exits fire on bar high/low with the adverse same-bar convention; calibration state declared; configuration provenance on every result row; variant exit-regime parity; global feeds fetched once per sweep; duplicate `plainBlock` removed; per-task scheduler windows so the 08:30 meeting can dispatch. |
| 3 | `58eb1ba` freeze legacy baseline | `_investorBootstrap`, `_investorAdmin`, `investor/*.json`, nested `netlify.toml` removed | v18/v6 frozen with full hashes and labelled legacy collections; the stale nested config deleted; hygiene fixture. |
| 4 | `f8b8713` policy and schemas | `_investorPolicy.js`, `_investorMoney.js` | Versioned inline policy: role models, cost rates, cutoffs, budget, risk mandate with bounds, emergency template, decision vocabulary, eleven strict model schemas with hashes; exact BigInt/rational money library with mandatory rounding modes. |
| 5 | `27c98ee` codecs and content store | `_investorStorageCodec.js`, `_investorContentStore.js`, `_investorAdmin.js` | Normalised document codec (money boundaries, content hashes), 40+ new collections and index declarations, content-addressed object store with manifests. |
| 6 | `39901fb` jobs, nonces, leases | `_investorJobs.js`, `investorKick.js`, `investorIngest-background.js`, `_investorAuth.js` | Task vocabulary, dedupe, run-scoped leases that survive the 15-minute cap, bound single-use worker nonces, dispatch plan with a 20-second budget; the overnight ingest worker. |
| 7 | `a099857` SEC facts and universe snapshots | `_investorFundamentals.js`, `_investorDataProviders.js`, `_investorUniverse.js` | Point-in-time XBRL company facts with lineage, provider-neutral data interface (no consensus vendor), frozen eligible snapshot with hash and exact coverage matching. |
| 8 | `2167e60` claims and one sweep | `_investorClaimVerifier.js`, `_investorEventRouter.js`, `_investorEvidence.js` | Guidance and earnings-date claims with supersession, independent persisted verification verdicts, evidence-delta classification and routing, the single overnight sweep. |
| 9 | `2904669` dossiers and workset | `_investorDossier.js`, `_investorWorkset.js` | Versioned sector-aware dossiers (common core + sector block), 300–500-token cards, deltas, freshness matrix, the managed workset (eligible ∪ held ∪ pending, off-roster names still managed). |
| 10 | `984bd62` gateway rewrite | `_investorOpenai.js` | Role-fixed gateway: background/async Sol calls with resume by request key, allowlisted bounded tools, minor-unit cost reservation, model-request records, strict structured outputs, typed failures. |
| 11 | `e6e4c09` the meeting | `_investorManager.js`, `_investorResearch.js`, `_investorResearchTools.js`, `_investorValuation.js`, `_investorPortfolio.js`, `_investorPortfolioRisk.js`, `investorManager-background.js` | Checkpointed stage machine (freeze→review→coverage→maintenance→research→synthesis→activation→persist), exact coverage contract, anti-goalpost rule for holdings, research pool with deferral, deterministic valuation and size authority, one decision row per symbol, typed no-BUY reasons. |
| 12 | `7ab7c98` mandates and envelopes | `_investorMandate.js`, `_investorEmergencyRisk.js`, `_investorRisk.js` | Three immutable records (proposal, binding, envelope), all-or-none CAS staging against a fresh activation snapshot, reservations, desired-state outbox, evidence pauses, bounded emergency policy enforcement. |
| 13 | `c23364b` execution | `_investorExecution.js`, `_investorBroker.js`, `investorExecution-background.js`, `_investorLedger.js` | OHLC simulator (touch before close, collisions, participation, halts), fills → legs → positions → balanced journal with conservation checks, paper and Alpaca adapters behind a capability matrix, executor tick with no model code. |
| 14 | `c8d40d4` learning and reporting | `_investorLearning.js`, `_investorKpi.js`, `_investorEvals.js`, `_investorAlerts.js`, `investorPostclose-background.js`, `investorArchive-background.js` | Frozen outcome records, point-in-time resolution, matched counterfactuals, 32 versioned KPIs, eval suite with a promotion gate that never self-promotes, typed alerts with stable ids, post-close and archive workers. |
| 15 | `b424259` API v2 and console | `_investorApiSchemas.js`, `_investorApiV2.js`, `investorSession.js`, `_investorAuth.js`, `investorApi.js`, `investor.html`, `netlify.toml`, `package.json`, `scripts/investor/csp-hashes.js` | Ajv-compiled wire contract (27 reads, 31 mutations), idempotent versioned mutations, cookie sessions with CSRF and reauth, origin enforcement, v1 retirement under the manager engine, the five-view console with hashed strict CSP. |
| 16 | `7a8804f` end-to-end review | `_investorManager.js`, `_investorExecution.js`, `_investorPolicy.js`, `_investorSelftest.js` | The chain fixture (meeting → activation → execution → post-close → console) and the two hand-off gaps it exposed: measured liquidity marks for every staging call, and a hold on exposure-adding transitions during any buy freeze. |

### 3.1 How each step was verified

- Local harness: the repository has no test toolchain, so the deploy
  attestation module itself was run under Node 22 with the runtime
  dependencies (`@google-cloud/firestore`, `@google-cloud/storage`, `ajv`,
  `ajv-formats`) resolved from a scratch `node_modules`. Command:
  `cd netlify/functions && node -e 'require("./_investorSelftest").runFixturesAsync().then(r => console.log(r.pass, r.passed + "/" + r.cases.length))'`.
- Fixtures use an in-memory Firestore (`fakeAdmin`) with equality, `in`,
  and range queries, transactions and field-value increments, so every
  persistence path is exercised without network access.
- Every module is admin-injectable (`{ admin }` or `deps`), which is what
  lets the attestation run the real code, not mocks, at deploy time.

---

## 4. Architecture map for reviewers

### 4.1 Backend modules (`netlify/functions`)

| Area | Modules |
|---|---|
| Policy, money, codecs | `_investorPolicy`, `_investorMoney`, `_investorStorageCodec`, `_investorContentStore`, `_investorAdmin` |
| Scheduling | `investorKick` (cron), `_investorJobs` |
| Evidence | `investorIngest-background`, `_investorIntelligence`, `_investorIntelligenceSources`, `_investorEvidence`, `_investorFundamentals`, `_investorDataProviders`, `_investorClaimVerifier`, `_investorEventRouter`, `_investorDossier`, `_investorUniverse`, `_investorWorkset` |
| Manager | `investorManager-background`, `_investorManager`, `_investorResearch`, `_investorResearchTools`, `_investorValuation`, `_investorOpenai` (gateway), `_investorPortfolio`, `_investorPortfolioRisk` |
| Mandates and risk | `_investorMandate`, `_investorRisk`, `_investorEmergencyRisk` |
| Execution | `investorExecution-background`, `_investorExecution`, `_investorBroker`, `_investorLedger`, `_investorMarket`, `_investorHistory` |
| Learning | `investorPostclose-background`, `investorArchive-background`, `_investorLearning`, `_investorKpi`, `_investorEvals`, `_investorAlerts`, `_investorNav` |
| Surface | `investorApi` (v1 + v2 routing), `_investorApiV2`, `_investorApiSchemas`, `investorSession`, `_investorAuth`, `investor.html` |
| Attestation | `_investorSelftest` (200 fixtures), `_investorLedger.controlAllowsEntry` |

### 4.2 Key invariants and where they are enforced

- No model code in the executor/post-close/archive: load-time guard in each
  handler plus a source-level eager-import walk in the fixture set.
- Coverage exactness (§6.3, D-12): `_investorManager.assertExactCoverageInput`
  / `validateCoverage`; incomplete coverage → `COVERAGE_INCOMPLETE`, no BUY.
- Envelope only shrinks (§6.6): `assertNoEnvelopeIncreasesProposal`; material
  clamp → `NEEDS_SOL_RESYNTHESIS`.
- All-or-none activation on a fresh snapshot: `stagePortfolioPlan` CAS.
- Protection never widened by a delta review (§6.7): `antiGoalpostViolations`.
- Ledger conservation after every tick: `assertConservation`; failure pauses
  the executor (`PAUSED_SAFETY`).
- Buy freeze holds exposure-adding transitions only: `applyOutbox({allowExpansion})`.
- Sessions are server state; mutations need CSRF, idempotency, version, and
  reauth where the contract says so: `_investorApiSchemas.ACTIONS_V2`,
  `_investorAuth.requireOperatorV2`, `_investorApiV2.claimMutation`.
- Console CSP hashes must match the single inline script/style block:
  fixture `console_csp_hashes_in_netlify_toml_match_the_inline_script_and_style_blocks`.

### 4.3 Operator console (`investor.html`)

Five routes — Manager, Companies, Portfolio, Decisions & Learning, System —
with the §14.2 component ids, one global control surface driven by the
server's `availableActions`, capability-driven per-row controls, BigInt
formatters validated against the shared test vectors, cookie + CSRF + reauth
wire, no inline handlers, no `innerHTML`, no browser-stored credentials, and
persistent critical banners.

---

## 5. Deploying and cutting over

1. Deploy the branch through the existing GitHub → Netlify connection.
   Netlify installs `package.json` dependencies (now including `ajv` and
   `ajv-formats`) and bundles functions with esbuild as before. No local npm
   step is needed.
2. The deploy attestation runs the 200 fixtures; `Control.fixturesPass`
   and `fixturesCommit` must match the build before any entry is allowed.
3. Sign in to the console (cookie session). The account starts in
   `OBSERVE`: reads work, investment mutations are disabled.
4. In System → Account mode activation, run `activateAccountMode` (reauth
   required). Preflight checks fixtures, policy hash, universe hash, the
   paper account and ledger conservation, then sets `PAPER_AI`, engine
   `manager`, and increments the writer epoch. From then on v1 investment
   mutations answer `410 ACTION_RETIRED`.
5. Set the daily AI budget and risk mandate overrides in System if the
   defaults are not wanted; both are versioned and audited.
6. Live trading is a separate step: the Alpaca adapter enables only under
   `LIMITED_LIVE` with `INVESTOR_LIVE_BROKER=alpaca`, and the paper spread
   assumption must be replaced by a measured quote first (§19.3).

Environment/secrets unchanged from before: operator passcode and session
secret (Firestore `authConfig` or env), OpenAI key, Alpaca keys via
`setMarketConfig`, `INVESTOR_CONTENT_BUCKET` for the content store.

---

## 6. Known limits and labelled assumptions

- Paper mode has no quoted spread; `riskMandate.liquidity.paperAssumedSpreadBps`
  (10 bps) is a labelled assumption applied as a hard input.
- Consensus estimates, transcripts and a commercial earnings calendar are
  intentionally absent (§5.6, closed); the forward view comes from issuer
  guidance and announced dates through the extraction path.
- Firestore composite indexes (§10.6) must be created through the operator's
  usual mechanism; queries in the API were written to need as few as possible
  and to fall back to bounded equality queries.
- The console was generated against the wire contract and verified
  structurally (ids, blocks, vectors, CSP, parse); it has not been exercised
  in a browser session inside this environment.
- Live operation still requires the owner's capital, loss tolerance,
  liquidity, tax/account constraints, jurisdiction and broker rules (§15.1).

---

## 7. Review checklist for an independent auditor

1. Run the attestation locally (Section 3.1) and confirm 200/200.
2. Read `end_to_end_meeting_activation_execution_postclose_and_console_agree_on_one_trading_day`
   in `_investorSelftest.js`; it is the narrative of one trading day through
   every hand-off.
3. Confirm the authority model (§3) in code: only `_investorOpenai.finalizePortfolio`
   / `researchCompany` / `reviewUniverse` produce decisions; `_investorMandate`
   and `_investorPortfolioRisk` only refuse or shrink; `_investorExecution`
   never reads a model.
4. Confirm fail-closed paths: gateway failures, budget exhaustion, incomplete
   coverage, unsupported claims, conservation failure, stale broker truth.
5. Confirm the KPI definitions and targets in `_investorKpi.KPI_DEFINITIONS`
   match Section 2.2 and that `investorPostclose-background` writes the daily
   row from measured inputs and lists what it could not compute.
6. Confirm the console contract: `_investorApiSchemas.ACTIONS_V2` and
   `VIEW_MODELS` versus the calls in `investor.html`.
