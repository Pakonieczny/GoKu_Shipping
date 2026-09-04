# Investor AI — AI Fund Manager Implementation Blueprint (v2)

**Repository reviewed:** `Pakonieczny/GoKu_Shipping`  
**Repository commit:** `ea14b71055df03f969e94fdf3d771ea054f748fa`  
**Current investor release:** strategy v18 / bootstrap 25  
**Design date:** 2026-09-03  
**Revision date:** 2026-09-04  
**Revision scope:** nine defects verified in the current plumbing are added as §2A and remediated by a new blocking Phase −1 (§19). Amendments applied inline to §2, §3, §8.5, §8.8 (new), §9.5, §11.1, §16.5, §17.2, §17.4 and §19. Every other section is v1 as written. Changed passages are marked **[v2]**.  
**Primary UI:** `investor.html`  
**Input record reviewed:** `Investor_AIChangeRecord.md` (577 lines)  
**Deliverable type:** implementation plan; this document does not alter the running application or authorize live trading.

## Executive decision

1. Replace the deterministic residual-reversal strategy as the investment authority with one **GPT-5.6 Sol, high-reasoning AI Fund Manager**. Ratios, price histories, filings, and news become evidence—not automatic buy/sell votes.
2. Keep deterministic software for exact arithmetic, validation, risk ceilings, idempotency, monitoring, accounting, and execution. It may reject or reduce an AI proposal, never improve it. The sole exception is a separately owner-approved, narrowly bounded emergency-risk policy that may reduce/flatten prohibited exposure or neutralize an accidental short—never seek alpha or open a long.
3. Give Sol a compact, current, source-linked record for **every member of the frozen eligible roster** every pre-market session—304 companies in the audited v6 roster. No Luna/Terra score, five-day rule, rank, or shortlist may hide a company from Sol.
4. Perform full research once, persist it, and update it from evidence deltas. Existing holdings receive a pre-market revision of their prior thesis and mandate—not a wasteful daily clean-room research job.
5. Deeply research only a new opportunity, a materially changed company, an earnings release, a stale thesis, or an explicit anti-anchoring audit. There is no fixed daily quota.
6. Every actionable AI decision must publish a complete, versioned standing mandate: action, executable limit/order terms, proposed size, profit target, loss boundary, holding horizon, separate entry/protection lifetimes, invalidators, and review triggers.
7. A deterministic executor continuously enforces those standing mandates at **$0 of AI-token cost**. It does not ask Sol again when the authorized price is reached.
8. Use broker-native limit/OCO/bracket protection when available. The current one-minute close-only guard can miss an intrabar target touch; paper simulation must use bar high/low with conservative collision handling.
9. Record every decision, forecast, revision, rejected name, execution, outcome, prompt, model, and source version. Learn through prospective evaluation and controlled challenger tests—not by letting recent P&L silently rewrite the strategy.
10. Rebuild `investor.html` around five clear views: **Manager, Companies, Portfolio, Decisions & Learning, System**. Remove the Advanced drawer, deterministic knobs, rank/gate explanations, duplicate controls, and approval ceremony.

> **Immediate security blocker:** the repository tracks `netlify/functions/secrets/gcpPrivateKey.txt`. Treat that key as compromised and rotate/remove it before any architecture deployment or live-broker work; Section 18 gives the exact response.

## 1. The objective—and the necessary correction

The requested commercial aim is to make the highest return in the shortest time. That phrase is understandable, but it is unsafe as a machine objective: a system can appear to optimize it by taking leverage, concentration, illiquidity, turnover, and ruin risk. FINRA specifically warns that a poorly designed agent reward can optimize decisions that harm investors ([FINRA, Observations on AI Agents](https://www.finra.org/media-center/blog/observations-on-ai-agents)).

The application mission should therefore be:

> **Maximize long-run, after-cost, risk-adjusted capital compounding by making evidence-grounded, time-aware investment decisions while bounding drawdown, concentration, liquidity, execution, data, and model risk.**

This still makes profit the purpose. It prevents the application from mistaking speed, win rate, trade count, or gross returns for durable wealth creation.

### Brutally honest limits

- There is no independent public evidence that a fully autonomous frontier LLM reliably produces net stock-market alpha across regimes. Some studies find return-relevant information in LLM news interpretation, but that is much narrower than proving an autonomous fund manager ([Lopez-Lira and Tang](https://arxiv.org/abs/2304.07619)).
- LLMs can reason over messy qualitative information, but financial benchmarks continue to expose retrieval, calculation, and analyst-judgment failures. FinanceBench found severe failure rates for an older GPT-4 retrieval setup; SECQUE finds analyst-insight tasks particularly difficult ([FinanceBench](https://arxiv.org/html/2311.11944v1), [SECQUE](https://aclanthology.org/2025.gem-1.16.pdf)). Those numbers must not be projected mechanically onto GPT-5.6 Sol, but they justify validation.
- Structured output guarantees shape, not truth. A valid JSON object can still contain a false fact, a bad assumption, or a poor investment decision.
- Stops are not guarantees. A stop order becomes a market order and may fill away from its trigger; a stop-limit can fail to fill ([Alpaca order documentation](https://docs.alpaca.markets/us/docs/orders-at-alpaca)).
- Historical LLM tests can leak knowledge from model training data. Prospective shadow and paper decisions are the primary proof, with historical replay used only as supporting evidence ([Glasserman and Lin](https://www.pm-research.com/content/iijjfds/6/1/25)).

The target is therefore not “trust AI instead of code.” It is **give investment judgment to intelligence and give exact enforcement to code**, then measure whether the combination actually beats the current system after all costs.

## 2. What the repository does today

The investor surface comprises 46 directly investor-named artifacts and roughly 35,000 lines, including a 334 KB single-file UI. Root `netlify.toml`, `package.json`, and a tracked stale alternative at `netlify/functions/netlify.toml` create configuration ambiguity if tooling runs from the nested directory, so the subsystem inventory covers 49 files. This review also found one critical cross-cutting credential file that must be remediated, making 50 existing-file dispositions below. Root `netlify.toml` schedules `investorKick` every minute. That dispatcher launches `cycle`, `guard`, `evidence`, or `archive` work in `investorCycle-background.js`.

**[v2] The repository contains two strategy configurations, not one.** v1 described one, and drew conclusions from the second without naming it. Both facts below change what the AI Fund Manager is being compared *against*.

The **pre-registered strict configuration** (`_investorStrategy.js:168-300`) carries the full research discipline — a 2.0× cost-margin hurdle, a $300M ADV floor, a t>3.0 significance hurdle, a 0.5 decay haircut, and a declared retirement condition at 200+ closed positions. It also declares `requireCalibratedEdge: true` with `calibratedExpectedEdgeBps: null`. `_investorSignal.js:683` therefore evaluates `calibratedPass` as `calibrationKnown && calibrated > 0`, which is false, and `:690` gates every proposal on it. `_investorCalibration.js:33-40` cannot return a verdict until 252 + 126 + 126 + 2×15 = **534 chronological sessions** exist. **This configuration has never placed an order and cannot place one for approximately two years.** No performance conclusion of any kind may be drawn about it.

The **exploratory paper configuration** (`_investorStrategy.js:308-335`, `paperLearningDefaults`) produced every trade in the record, including the 38 round trips cited in the v18 note. It runs at `costMarginMultiple` 0.25 (one eighth of the declared hurdle), `minAdvUsd` $50M (one sixth of the declared floor, against a slippage model charging 12bp half-trip below $100M ADV), `positionScale` 3, and with all five exposure and loss breakers at 100% of NAV (`:478-484`). These values sit inside the declared clamp (`:52`, `:58`), so this is an operator-selectable floor and not a code defect — but every performance conclusion drawn from this lane was drawn at that floor, and none of those conclusions currently carries that fact.

**[v2] The evidence plane is entirely free public sources.** GDELT discovery indexes, SEC EDGAR, Federal Register, USAspending, ten government RSS feeds, and NWS alerts (`_investorIntelligenceSources.js`, 25 sources). Market data is Alpaca. There is **no** fundamentals vendor, **no** consensus-estimate feed, **no** transcript source, and **no** confirmed earnings calendar — `investor/universe/v1.json` carries `earningsDates: []` with `earningsDatesSource: "operator_entry_required"` for every name, and `cikSource: "unresolved_pending_sec_map"` for a substantial minority. The only XBRL fact retrieved anywhere in the codebase is `dei:EntityCommonStockSharesOutstanding`. The dossier fields §5.4 requires the manager to reason over — `revenueGrowthBps`, `fcfMarginBps`, `netDebtEbitdaMilli`, `forwardMultipleMilli` — therefore have **zero sources in this repository**. The dossier schema must distinguish *absent* from *null*, and the manager must be shown absence explicitly rather than handed a null it may read as a value. §22 question 3 remains open.

The current investment pipeline is explicitly inverted for cost:

1. Pull bars for the trade universe.
2. Calculate factor residuals and five-session statistics.
3. Rank the cross-section and find threshold breaches.
4. Fetch evidence only for breaches.
5. Ask a model only when deterministic classification remains ambiguous.
6. Apply thirteen deterministic entry gates and deterministic sizing.
7. Apply fixed deterministic exits to holdings.

That is the architecture the user has rejected: the code chooses what is interesting before intelligence sees it, and the model is prohibited by `_investorOpenai.js` from choosing an order, price, or size.

### Evidence from the v18 change record

The attached change record is valuable engineering evidence, but it also demonstrates why the operating model must change:

- Nine linked defects prevented candidates, Firestore writes, promotion, pricing, reconciliation, or full roster coverage at different layers.
- The current selector looks back five completed sessions and then requires residual, rank, trend, liquidity, intelligence, evidence, cost, and other gates.
- Only about six companies per evidence sweep receive a fresh dossier; the record reports 34 stale daily histories.
- `_investorOpenai.js` is intentionally an evidence clerk: it caps normal input at 24,000 characters and research at 60,000, uses Luna/Terra roles, sets `store:false`, rejects tool calls, and expressly forbids browsing, return forecasting, pricing, sizing, or orders. The proposed Sol workflow cannot be bolted onto that call shape; the gateway and resumable job lifecycle must be rewritten.
- Exploratory-auto v12 added a fixed 2% take-profit only after 38 historical round trips produced a net loss of $19.89. It is not universal across the current code: the strict v18 baseline still declares `takeProfitPct: null`. The important defect is that neither branch derives a company-specific exit from an investment thesis.
- The change record explicitly states that no trade had yet been observed under v18 at its time of writing.

The record reports 141/141 raw and bundled fixtures. This review could not independently rerun that suite because the local checkout lacks `@google-cloud/firestore`; the fixture result is therefore an attachment claim, not a newly reproduced result.

### Current execution gap that directly answers the “momentary target” concern

`_investorPositionGuard.js` evaluates exits using the latest bar's `last.c`; `_investorStrike.js` likewise calls `strikeVerdict(last.c, plan)`. A one-minute bar can trade through a target and close back below it, so a close-only poll can miss the touch. The correct production answer is not “ask AI more often.” It is to lodge the authorized limit/protective order with the broker, whose order engine watches the market continuously. Alpaca documents bracket orders as an entry plus take-profit and stop-loss group, with the exit legs queued after the entry fills ([Alpaca bracket orders](https://docs.alpaca.markets/us/docs/orders-at-alpaca#bracket-orders)).

## 2A. Verified defect register **[v2]**

Nine defects verified by direct inspection at commit `ea14b71`. Six appear nowhere in v1. Six are in the **plumbing**, not the strategy, and therefore survive the replacement of the decider: built on top of them the manager would be scored on outcomes it did not produce. All nine are remediated in Phase −1 (§19).

| ID | Defect | Evidence | In v1? | Severity |
|---|---|---|---|---|
| **D-1** | Every AI failure mode is converted into a BUY | `_investorOpenai.js:309,312,325,331,372,379,395,404,435,442,447` all return `cause:"evidence_pending"`; `_investorSignal.js:640-649` `default:` reads that as absence of information and returns `{trade:true, side:"long", relaxed:true}` | no | Critical |
| **D-2** | Exits are decided on bar close, never on bar high/low | `_investorStrike.js:443` `strikeVerdict(last.c, plan)`; `_investorPositionGuard.js:373` `currentPrice: last.c`; `_investorExitPolicy.js` references `.h`/`.l` zero times | §1 item 8, §2, §16.3 | Critical |
| **D-3** | The pre-registered strategy cannot place an order | `_investorSignal.js:683,690`; `_investorStrategy.js:230-231`; `_investorCalibration.js:33-40` requires 534 sessions | no | Critical |
| **D-4** | The lane that traded ran with declared risk controls reduced | `_investorStrategy.js:313`, `:329`, `:478-480,483-484` | no | Critical |
| **D-5** | All 15 frozen shadow variants carry the exit defect v18 fixed | `_investorVariants.js:31-152` (ids A–Q) — `grep -c takeProfitPct` → **0** | no | Major |
| **D-6** | Company-agnostic feeds are fetched once per company | `_investorIntelligenceSources.js:485` keys state `` `${sourceId}_${symbol}` ``; 10 of 25 sources are unambiguously global (`kind:"feed"`) → ~3,040 requests where ~10 suffice | no | Major |
| **D-7** | `plainBlock()` declared twice; the second silently wins | `investor.html:3710` and `:3744` | §14.5 | Moderate |
| **D-8** | No job can be dispatched before 09:45 ET, so the pre-market Manager Meeting of §11.1 cannot run | `investorKick.js:81` `PLAN_EARLIEST_MIN = 9*60+45`, enforced at `:101` | no | Major |
| **D-8b** | AI budget ceilings block the Manager Meeting silently, and that block routes into D-1 | `_investorOpenai.js:62-63` — `DAILY_USD_CEILING` $5, `CYCLE_CALL_CEILING` 12; exhaustion returns `cause:"evidence_pending"` at `:331` | no | Critical |
| **D-9** | A live private key is tracked in the repository | `netlify/functions/secrets/gcpPrivateKey.txt` — git-tracked, mode 644, 1,734 bytes, begins `-----BEGIN PRIVATE KEY-----`; no `.gitignore` exists | §18 | Critical |

## 3. Target authority model

| Concern | Authority | What it may do | What it may not do |
|---|---|---|---|
| Raw collection | Deterministic adapters | Fetch allowlisted sources, timestamp, hash, deduplicate, version | Interpret investment merit |
| New-document extraction | GPT-5.6 Luna | Turn unstructured documents into source-linked candidate facts | Choose, suppress, buy, sell, size, or rank a company |
| Exact metrics and valuation arithmetic | Deterministic calculators | Calculate returns, cash flows, scenario math, exposure, liquidity, and costs from declared inputs | Declare that a metric means “buy” |
| Investment judgment | GPT-5.6 Sol, high reasoning | Compare all companies, interpret evidence, research, forecast, choose BUY/WATCH/IGNORE/HOLD/REDUCE/SELL/ABSTAIN, set priorities and mandates | Bypass evidence, risk policy, mandate schema, or execution isolation |
| Risk envelope | Deterministic risk service | Refuse, pause, or create a smaller **activation envelope** to comply with cash, loss, exposure, liquidity, concentration, or data-integrity limits | Mutate the AI proposal, increase size, substitute a symbol, improve priority, or create a normal order |
| Execution | Deterministic executor/broker | Submit, monitor, reconcile, cancel, and replace orders exactly authorized by an active mandate | Reinterpret news, move a target, extend an expiry, average down, or choose a trade |
| Emergency risk | Owner-approved `EmergencyRiskPolicy` enforced by code | Freeze expansion; cancel entries; reduce/flatten only on enumerated hard breach; buy-to-cover only an accidental short to zero | Claim an AI decision, select for return, rotate, average down, or create positive exposure |
| Learning and promotion | Versioned evaluation process | Score predictions/outcomes, compare challengers, propose a version promotion | Let recent P&L self-edit prompts or silently promote a winner |
| Operator | Authenticated human | Pause manager, freeze new buys, cancel a mandate, sell manually, emergency stop | Retroactively alter audit history |

### Fixed model routing

There should be no model deciding which model gets to make a decision. The routing policy is code and changes only through a versioned, evaluated release:

```js
const ROLE_MODELS = Object.freeze({
  facts: {
    model: "gpt-5.6-luna",
    authority: "extract_source_bound_facts_only"
  },
  manager: {
    model: "gpt-5.6-sol",
    reasoning: { effort: "high" },
    authority: "investment_decision_and_standing_mandate"
  }
});
```

Terra is removed from the investment path. A cheaper model may help prepare facts, but **no cheaper model is allowed to filter the eligible roster (304 names today) before Sol sees it**. This follows the useful planner/worker division described in OpenAI's [reasoning-model guidance](https://developers.openai.com/api/docs/guides/reasoning-best-practices), while preserving a single investment authority.

### Failure posture

- Luna failure: retain the raw document, mark extraction incomplete, and block any affected new authorization until resolved.
- Sol failure/refusal/invalid schema: no new or expanded mandate; retain the last still-valid holding protection, and show `ACTION REQUIRED`.
- Market/data staleness: freeze new buys; keep broker-native protective exits in force.
- Evidence conflict: Sol must return `ABSTAIN` or request more research; code may not resolve it by a ratio.
- Executor failure: do not call Sol. Reconcile broker state, retry idempotently, and escalate visibly.
- **[v2] Evidence-classification failure: the absence of a verdict is not a verdict.** Where the classifier cannot be reached, cannot be afforded, returns unparseable or schema-invalid output, or returns output whose citations fail verbatim verification, the system holds. It does not fall back to a prior regime, a cheaper model, a heuristic, or a reduced-size version of the proposal it did not receive. This is the fail-closed boundary; the current system violates it (D-1, D-8b) and Phase −1 item P−1.1 restores it.

## 4. Target system topology

```mermaid
flowchart TD
    A["Allowlisted filings, prices, news, events"] --> B["Point-in-time evidence and fact store"]
    B --> C["Persistent eligible-roster dossiers"]
    C --> D["Sol AI Fund Manager"]
    D --> E["Versioned standing mandates"]
    E --> F["Risk validator and deterministic executor"]
    F --> G["Paper broker now; live broker only after authorization"]
    G --> H["Ledger, outcomes, counterfactuals, KPIs"]
    H --> C
```

The feedback arrow means new evidence and measured outcomes enrich the next dossier. It does **not** mean P&L automatically rewrites prompts or authorizes risk.

### Trust boundaries

1. Source content is untrusted data. HTML, filings, and news cannot inject instructions into the manager prompt.
2. The model process has no broker credential and no order-submission tool.
3. A model response first becomes a proposed mandate. Only a separate deterministic validator can activate its immutable hash.
4. The executor accepts only an active, unexpired mandate hash and an idempotency key.
5. Every supersession creates a new version; no active plan is edited in place.

NIST recommends provenance, origin/modification tracking, production monitoring, and error/near-miss records for generative systems ([NIST AI 600-1](https://doi.org/10.6028/NIST.AI.600-1)). These are design requirements here, not optional compliance decoration.

## 5. Data and evidence plane

### 5.1 Where the initial information for 304 companies comes from

| Data lane | Initial/backfill source | Ongoing source | Purpose | Decision status |
|---|---|---|---|---|
| Identity and filings | SEC ticker/CIK mapping, submissions history, 10-K/10-Q/8-K/DEF 14A | SEC submissions and filing documents | Business, risks, management, capital allocation, legal disclosures | Primary evidence |
| Standard financial facts | SEC XBRL company facts and filing tables | New XBRL facts on each filing | Revenue, margins, cash flow, balance sheet, share count, segment history | Primary facts; deterministic reconciliation |
| Price/volume/corporate actions | Alpaca historical daily/intraday feed and existing history store | Alpaca market feed/calendar/actions | Price context, liquidity, volatility, execution, benchmark | Exact market evidence, never a buy vote |
| Cash/benchmark context | Official central-bank/Treasury series for account currency; corporate-action-adjusted benchmark prices from market provider | Versioned daily/calendar updates | Cash hurdle, risk-free series, SPY/sector/equal-weight comparisons | Exact dated inputs; missing series blocks affected KPI, not trades by guess |
| Company communications | SEC-linked official domains/IR pages | Press releases, guidance, presentations, calls where lawfully available | Management's explanation and forward statements | Issuer evidence; self-interested |
| Government/regulatory | Existing Federal Register, DOJ, FTC, DOL, NLRB, FDA, NTSB, FAA, EPA, FDIC, Fed, CISA, CPSC and related adapters | Source-specific polling | Legal, regulatory, contracts, trials, recalls, safety, sector events | Primary external evidence |
| Broad discovery | GDELT and specialist-publication metadata | Incremental discovery | Find potentially relevant reporting | Lead only until source is verified |
| Estimates and transcripts | **Not adequately supplied by the current repository** | Licensed point-in-time vendor recommended | Consensus revisions, guidance history, call transcripts, expectation gap | Required for a truly comprehensive manager; vendor bake-off first |
| Portfolio state | Existing paper account, orders, fills, positions, ledger, NAV | Real-time internal records | Cash, exposure, cost basis, mandate capacity | Exact internal state |

The SEC's APIs provide unauthenticated submissions and XBRL JSON, update throughout the day, and publish nightly bulk archives; SEC calls bulk archives the most efficient way to backfill large amounts of data ([SEC EDGAR API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)). Use the nightly `companyfacts.zip` and submissions archive for initial coverage, then real-time per-company endpoints for deltas. Respect the SEC's [fair-access guidance](https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data), including a declared user agent and the 10-request-per-second ceiling.

The ingest planner distinguishes provider-global feeds from issuer queries. A global regulatory/news/action feed is fetched **once per provider cursor**, content-addressed, then deterministically entity-resolved and fanned out to affected dossiers; it is never polled 304 times. Per-company work is limited to issuer-specific SEC submissions/filings, verified IR endpoints, or provider APIs whose query genuinely requires the issuer. Source-state records keep ETag/cursor/rate-limit/coverage and prove fetch-once behavior in tests.

The exchange calendar is ingested from an authoritative exchange or broker calendar and versioned into immutable session snapshots. The application does not generate holiday/half-day tables by hand. If the current or next required XNYS session, early close, or unscheduled closure is unresolved or disagrees across configured authorities, new entries fail closed while existing broker protection remains; fixtures cover DST, July/Thanksgiving/Christmas half days, national mourning/unscheduled closures, and provider correction.

The current public-source stack is not “everything about a company.” It is missing reliable point-in-time analyst estimates, broad licensed news, and many transcripts. Pretending otherwise would create false confidence. A provider decision is therefore a **Phase 0 architecture gate**, not an indefinite enhancement: define a provider-neutral schema, run a time-boxed bake-off on timeliness, corrections, universe coverage, historical point-in-time integrity, entitlement/retention rights, permission to process content through the selected model, and cost, then either procure a source or explicitly launch with those fields marked unavailable. Do not scrape paywalls or allow model memory to fill the gap.

#### Roster eligibility is also part of investment governance

The audited v6 file is a hand-authored thematic roster, not the investable US market. Sol is the sole subjective judge **inside** that roster; roster construction can still create garbage-in/garbage-out. Replace implicit membership with a versioned `UniverseSnapshot` that records every included and excluded security and the objective reason:

- supported jurisdiction/exchange and common-equity security type;
- broker tradability, short/long restrictions, fractional-share capability, and account eligibility;
- minimum rolling median dollar volume, maximum spread, price/data availability, and minimum trading history;
- IPO seasoning, halt, bankruptcy, acquisition, delisting, ticker/CUSIP/CIK changes, ADR and foreign-issuer treatment;
- explicit policy exclusions, with owner, rationale, effective date, and review date;
- held or working-order symbols outside the new roster, which remain in the **managed-position roster** until flat;
- a quarterly broad-market candidate refresh plus event-driven additions/removals, reviewed and versioned before use.

Each run freezes `universeVersion`, `universeHash`, `eligibleCount`, and membership as of the decision time. The Companies UI exposes an “Excluded” tab and reasons. Historical evaluation uses the membership that actually existed then, including later-delisted names; it never backfills today's survivors into the past.

Be explicit about the boundary: daily Sol review can find the best opportunity **among the 304 current mandate names**, not prove it found the best stock in the entire market. Manager v1 keeps all 304 unless an objective eligibility event removes one; poor recent attractiveness is not a removal reason. If market-wide discovery is desired later, add a separately budgeted quarterly `UNIVERSE_EXPANSION_REVIEW`: build an objective, fully disclosed common-equity pool, use Sol-only resumable shards plus global synthesis to propose additions, expose every inclusion/exclusion and reason, and version the owner-approved universe policy. A cheap model or five-day rank may not prefilter that expansion review either.

### 5.2 One-time dossier bootstrap

For every active symbol in `_investorUniverse.js` v6—the current 304-name runtime roster—not the stale 45-name `investor/universe/v1.json`:

1. Resolve ticker, CIK, legal name, former names, sector, SIC, and verified official domains.
2. Backfill daily market/benchmark data and primary filings for at least the available post-IPO history needed to cover a business cycle; five years is a storage/default floor, not a universal analytical rule, and cyclical businesses require longer history where available.
3. Deterministically normalize XBRL facts and reconcile period, units, amendments, splits, and duplicate contexts.
4. Use Luna to extract source-bound qualitative facts from unstructured portions: strategy, segments, management changes, stated risks, guidance, catalysts, and contradictions. Every fact carries a source span.
5. Build a compact `UniverseCard` and a fuller persistent `CompanyDossier`.
6. Calculate a content hash. Subsequent runs process only new or revised source versions.
7. Do **not** run 304 full Sol research jobs. Sol reviews 304 compact cards and chooses the small number worth full underwriting.

`_investorFundamentals.js` owns the numerical filing plane. It ingests SEC `companyfacts`/`frames`, normalizes taxonomy concept, unit, fiscal period, filing/accession, amendment, segment/context, and as-of availability, reconciles stock splits and share bases, and emits immutable `FinancialFactVersion` records. Tests cover amended filings, duplicate contexts, instant versus duration facts, fiscal-year changes, currency/unit conversion, restatements, and point-in-time queries. Benchmark returns, sector returns, risk-free/cash rates, and corporate actions receive the same source/version/as-of treatment.

Do not pass SEC bulk ZIPs through `_investorFetch.js`, whose audited default cap is 8 MiB, or assume they fit a 15-minute Netlify function. An explicit `scripts/investor/bootstrap-sec.js` streams bulk files into object storage in a controlled one-time/backfill job, creates immutable manifests, and queues bounded normalization batches. Incremental per-company API calls remain in the scheduled ingest worker. Netlify documents a non-configurable 15-minute background-function limit, so every production job must checkpoint and resume rather than assume a large backfill will finish in one invocation ([Netlify background functions](https://docs.netlify.com/build/functions/background-functions/)).

### 5.3 Separate facts, interpretations, and forecasts

Each dossier must maintain three visibly different layers in separate records:

- **Facts:** exact, source-linked observations with publication time, retrieval time, effective period, units, and hash.
- **Interpretations:** immutable `ResearchMemo`/`ManagerDecision` records containing the manager's explanation of why facts matter, with supporting and opposing claim IDs.
- **Forecasts:** immutable, horizon-bound probability distributions in `Forecast` records that can later be scored.

`DossierVersion` contains normalized facts and pointers to those AI-authored records; it does not embed an interpretation as a fact. This prevents an old AI opinion from re-entering tomorrow's prompt as if it were observed evidence.

### 5.4 Compact card shown to Sol for every company

Target 300–500 tokens per company, generated from structured state rather than free-form summaries. Use a common core plus a sector-specific block: banks receive capital/credit/deposit metrics; insurers underwriting/reserves; SaaS retention/unit economics; energy production/reserves/commodity sensitivity; REITs NOI/FFO/occupancy/debt maturity; biotech trial/regulatory/runway data. The historical window follows the business cycle and data availability rather than a universal five-year rule.

```json
{
  "symbol": "XYZ",
  "identity": {"name": "Example Corp", "sector": "industrials", "cik": "..."},
  "asOf": "2026-09-02T20:00:00Z",
  "price": {"currency": "USD", "closeMicros": "42100000", "returnBps": {"1d": "-120", "5d": "-480", "3m": "710", "1y": "1840"}},
  "relative": {"sector5dBps": "-90", "market5dBps": "20", "drawdownBps": "-840"},
  "fundamentals": {"revenueGrowthBps": "920", "fcfMarginBps": "1210", "netDebtEbitdaMilli": "1400"},
  "valuation": {"methodHints": ["ev_ebitda", "dcf"], "forwardMultipleMilli": "14300"},
  "expectations": {"revision30dBps": null, "coverage": "vendor_missing"},
  "changes": [{"eventId": "evt_...", "type": "8-K", "safetyClass": "high_impact", "managerMateriality": "pending"}],
  "standingView": {"status": "WATCH", "researchVersion": 3, "ageTradingDays": 7},
  "portfolio": {"held": false, "activeMandate": false},
  "dataQuality": {"complete": false, "missing": ["consensus_estimates"]}
}
```

The card contains signals that a human manager would inspect, but no composite “buy score.” Sol receives the raw components and an explicit missing-data record.

### 5.5 Evidence delta and materiality

`EvidenceDelta` is a typed difference between two dossier versions—not another model opinion. It includes added, revised, contradicted, and expired facts. Point-in-time inclusion uses the source's publication time when reliable and otherwise the system's immutable first-seen time; a later-corrected timestamp never makes evidence appear earlier than it was actually available.

| Change | Default treatment | Sol work |
|---|---|---|
| Prior close/ordinary price movement | Update card | Morning comparative review only |
| Routine low-impact news | Store delta | Morning review unless Sol requests research |
| 10-K/10-Q, earnings release, new guidance | Material | Full re-underwrite for a candidate/holding |
| 8-K, M&A, capital raise, restatement, auditor or CEO/CFO change | Material | Immediate focused revision; often full re-underwrite |
| Regulatory action, approval, recall, major litigation, cyber event | Material when entity match and source verified | Immediate focused revision |
| Material contract/customer/supplier/labor event | Material after source and economic exposure are established | Focused revision |
| Price/volume anomaly with no verified cause | Attention event only | Sol may request research; code cannot trade it |
| Target, stop, or entry price touched | Execution event | **No AI call**; execute active mandate |
| Mandate expiry | Authorization ends | Pause; next manager run must renew explicitly |
| Conflicting/stale required facts | Integrity event | ABSTAIN/freeze affected new authorization |

“Material” means plausibly capable of changing the thesis, valuation distribution, expected duration, invalidation condition, or portfolio risk—not merely that a keyword appeared. Code may attach objective event classes and high-recall safety flags, but it must not silently decide investment materiality. Every verified delta reaches the next Sol packet; high-impact classes also pause affected new entries pending Sol. Luna extraction is evaluated for recall and evidentiary support, with direct source excerpts for high-impact documents, sampled raw-document audits, and Sol escalation when extraction is uncertain.

## 6. The AI Fund Manager process

### 6.1 Daily cadence

| Time/trigger | Work | AI used | Scope | Result |
|---|---|---|---|---|
| Overnight | Fetch and normalize new filings, facts, news, events, prices | Luna only for new unstructured documents | Changed evidence across the eligible roster | Versioned fact deltas |
| Pre-market, holding target by 09:15 ET | One logical Manager Meeting | Sol high | Frozen eligible roster + managed positions + portfolio | Complete roster coverage, holding revisions, research questions and capital ordering |
| Same meeting, when research is requested | Focused underwriting and final portfolio synthesis | Sol high with managed retrieval/calculation tools | New/material/stale names plus current holdings | Final BUY/WATCH/IGNORE/HOLD/REDUCE/SELL/ABSTAIN choices and complete portfolio-feasible mandates |
| Market hours | Monitor/submit/reconcile active mandates and broker events | None | Active orders and holdings | Deterministic fills, OCO protection, audit events |
| Material event | Quarantine affected entry; focused revision | Sol high only when decision-relevant | Affected company/position | Superseded, paused, or unchanged mandate |
| Post-close | Mark NAV, attribute costs/outcomes, capture counterfactuals | None by default | Portfolio + all reviewed names | Daily journal and KPI observations |
| Weekly | Error/postmortem review and stale-dossier queue | Sol only for selected cases | Failures, missed opportunities, calibration | Proposed improvements; no auto-promotion |
| Every 20 trading days by default | Independent anti-anchoring refresh | Sol high | Current holdings and active watch theses | Clean-room re-underwrite |

Twenty trading days is a starting policy, not a discovered optimum. Earnings and other material events override it immediately. The application must measure whether 10, 20, or 30 days gives better decision quality per dollar before changing the policy.

The scheduler uses the NYSE calendar rather than fixed UTC closes. Recommended starting cutoffs are: freeze the initial evidence manifest at 08:30 ET; start the manager immediately; treat holding protection/revision as a 09:15 hard operational deadline; treat new-opportunity completion as a 09:20 soft deadline. A mandate not safely completed by then receives a later `validFrom`—possibly after the open or next session—rather than rushed reasoning. Between the frozen cutoff and the open, every new verified delta is appended to a late-evidence queue; a high-impact class pauses the affected entry immediately, and all deltas reach Sol at the next continuation/revision. Holding protection never expires merely because research is late.

### 6.2 One logical Manager Meeting—not redundant committees

The pre-market job is a versioned state machine that may require several API round trips for tool use, but it is one decision process and one audit trace. It has two ordered commit classes so opportunity research cannot delay risk maintenance:

```text
freeze universe/evidence/portfolio manifests
    -> load every eligible compact card + every held/pending symbol, including off-roster names
    -> Sol covers every frozen roster symbol exactly once
    -> revise existing holding mandates from prior thesis + new evidence
    -> validate/apply the Sol-authored holding maintenance change-set by 09:15, or retain prior acknowledged protection
    -> identify research-needed names and gather bounded retrieval/calculator results
    -> Sol compares the completed research set together and publishes final expansion decisions, unique capital ranks, and feasible reservations
    -> deterministic validator atomically activates the complete BUY basket against fresh post-maintenance broker truth
    -> deterministic executor watches them without further AI confirmation
```

There is no “light AI reviewer,” no Terra committee, no trigger-time AI judge, and no AI call every 15 or 30 minutes. Focused research is not a repeated clean-room review: it gathers the deeper evidence and calculations that the compact card cannot contain. The final portfolio synthesis occurs once inside that same logical Manager Meeting so fully researched names are compared together rather than activated in whichever order jobs finish. Its mandates remain standing until revision, expiry, or execution; touching a price never causes another model call.

#### Logical meeting versus physical API calls

“One meeting” is an audit and decision boundary, not a requirement to trust one fragile HTTP request. The runner first estimates the exact tokenized payload. If it is at or below a tested 220k-input guardrail, it may use one Sol background response. If it is larger, times out, or fails the long-context eval, partition the frozen roster deterministically into balanced blocks, use **Sol high for every block**, and run one Sol global synthesis over all per-name outputs, expanded research, holdings, and portfolio constraints. No block may omit a name and no cheaper model may rank or filter it.

Use the Responses API asynchronous background lifecycle with `store:false`, persist the response ID/status, and poll/resume from short Netlify jobs; OpenAI documents that background requests can use `store:false` while temporarily retaining response data to support polling ([OpenAI background mode](https://developers.openai.com/api/docs/guides/background)). Cap tool calls, input, output/reasoning, and wall time; detect truncation explicitly. Rotate or seeded-randomize company order, insert frozen sentinel cases, repeat selected frozen contexts, and compare the single-context and block+synthesis variants before release to detect positional bias and anchoring.

### 6.3 Coverage contract for the complete managed universe

Sol's universe response must include exactly one row for every symbol in the frozen eligible snapshot—304 at the audited commit. Do not overload workflow with investment judgment. Each row has both:

- `reviewDirective`: `RESEARCH_NOW`, `UPDATE_EXISTING`, `REUSE_CURRENT`, or `NONE`; and
- `provisionalDisposition`: `WATCH`, `IGNORE`, `ABSTAIN`, `HOLD_CANDIDATE`, `REDUCE_CANDIDATE`, or `SELL_CANDIDATE`.

The final persisted `ManagerDecision.decision` is one canonical value—`BUY`, `WATCH`, `IGNORE`, `HOLD`, `REDUCE`, `SELL`, or `ABSTAIN`—for every roster row. Workflow values never appear in the decision column. Every row requires a short plain-language reason, `changedSincePrior`, and a machine-readable reason code where relevant (`DATA_INCOMPLETE`, `EVIDENCE_CONFLICT`, `UNCERTAINTY`, or `MODEL_FAILURE`). Backend validation compares the response against `{universeVersion, universeHash, eligibleCount, symbols}` rather than a hard-coded `304`. Duplicate, missing, or unknown symbols invalidate the run; one repair request may supply only missing rows. The repaired rows are merged into an `effectiveCoverage` object and that object—not the original incomplete response—is used by research, final synthesis, persistence, and audit. If coverage is still incomplete, no new BUY mandate from that run is activated.

The compact-card phase cannot issue a new BUY mandate. It can request focused underwriting, provisionally classify a name as `WATCH`, `IGNORE`, or `ABSTAIN`, or analyze an existing holding against its prior full memo. Every first-time BUY requires source-complete focused research and the meeting's final cross-company portfolio synthesis.

Every research request carries a Sol-authored unique `researchPriority`, `completionClass` (`HOLDING_REQUIRED`, `BUY_REQUIRED`, `OPTIONAL_DISCOVERY`), and reason. The job runner uses a bounded concurrent pool (initial cap three), always launches the smallest unique priority first, checkpoints each result, and waits at a barrier before final synthesis. Budget/deadline deferral may remove only a suffix of that order after all `HOLDING_REQUIRED` work; response-array or job-completion order has no authority. A requested name whose research is incomplete cannot become BUY: it finishes as `WATCH` with `reasonCode:"RESEARCH_DEFERRED"`, or `ABSTAIN` if a held decision is unsafe/incomplete. The UI shows requested, completed, deferred, and failed priority ranges.

The management workset is `eligible snapshot ∪ held symbols ∪ symbols with working/pending broker state`. Off-roster, halted, acquired, or delisted holdings remain managed and are reported separately until flat. Held companies in the eligible roster also appear in coverage, but their detailed revision occurs only once inside the same Manager Meeting. `reviseHolding()` is reserved for a later material-event continuation; it is not a duplicate morning call. Manager v1 explicitly forbids scaling in: a symbol with any owned quantity cannot receive `BUY`, and `HOLD` cannot increase exposure. A future `INCREASE` action would require its own schema, aggregate-position protection, reservation, and evals; it must not be smuggled through `BUY`.

### 6.4 Opportunity routes

Sol must be prompted to search across multiple legitimate investment patterns so the old “five-day decline” assumption does not merely reappear in prose:

1. High-quality compounder at an acceptable price.
2. Fundamental inflection in revenue, margin, cash flow, or balance sheet.
3. Expectations gap: evidence diverges from market/consensus expectations.
4. Misunderstood event or temporary dislocation.
5. Credible catalyst with asymmetric payoff.
6. Asset value, sum-of-parts, or cash-flow value.
7. Special situation such as restructuring, spin, tender, or merger—with event-specific risk.
8. Portfolio replacement: a superior expected opportunity after costs and risk.

The manager can find momentum, quality, value, event, or contrarian opportunities. Five-day returns, z-scores, SMA trends, and ratios remain useful descriptors; none is an eligibility gate.

### 6.5 Focused underwriting checklist

For a selected company, Sol must answer:

1. What does the company do, how does it make cash, and what are its economic drivers?
2. What has changed, why now, and is the change already reflected in price?
3. What do primary filings, management claims, independent sources, and market behavior agree or disagree about?
4. What are the balance-sheet, dilution, liquidity, governance, legal, customer, supplier, and sector risks?
5. Which valuation framework fits this business, and why?
6. What bull/base/bear assumptions produce the value range? A deterministic tool performs the arithmetic.
7. What evidence would disprove the thesis?
8. What return distribution and horizon are expected after trading costs, and is the opportunity still worthwhile after its incremental AI/data operating cost?
9. Is this better than cash, the benchmark, current holdings, and other authorized opportunities?
10. What exact mandate should be issued—or why should the manager abstain?

The manager's research tools are read-only and source-bound:

| Tool | Owner | Returns |
|---|---|---|
| `getFilingFactsAsOf` | `_investorFundamentals.js` | Normalized fact versions and filing/accession lineage |
| `getSourceSpans` | `_investorEvidence.js` | Exact filing/news/transcript spans with publication/first-seen metadata |
| `searchDecisionData` | `_investorDataProviders.js` | Entitled point-in-time news, estimates and transcripts with correction/version IDs |
| `getMarketContextAsOf` | `_investorMarket.js`/`_investorHistory.js` | Price, volume, spread, benchmark, peer and corporate-action context—not a buy score |
| `runValuation` | `_investorValuation.js` | Exact scenario arithmetic from Sol-declared assumptions |
| `getPortfolioSnapshot` | `_investorPortfolio.js` | Positions, orders, reservations, cash, exposures and opportunity-cost set |
| `runPortfolioScenarios` | `_investorPortfolioRisk.js` | Read-only factor/sector/correlation exposures, liquidity, marginal risk, cash drag and binding gap/halt stress for proposed baskets |

There is no unrestricted browser and no order tool in model context. A missing entitled source is returned as missing, never filled from model memory.

### 6.6 Valuation without returning to dumb ratio rules

Sol chooses the appropriate method and assumptions; `_investorValuation.js` calculates the numbers. Supported tools should include:

- Discounted cash flow and reverse DCF.
- Comparable multiples and historical-band sensitivity.
- Sum-of-the-parts.
- Dividend/residual-income models for relevant financial businesses.
- Unit-economics and cohort models.
- Probability-weighted event trees for binary or special situations.

Ratios are inputs and cross-checks. The code verifies units, formulas, probability sums, and sensitivity tables; it does not conclude that “P/E below X means buy.”

Every forecast declares `forecastBasis`: reference quote type/value/time, currency, arithmetic or log-return convention, price-return or total-return treatment, corporate-action treatment, horizon and resolution calendar, conditional-on-entry-fill versus opportunity forecast, estimated round-trip costs deducted, probability of fill by expiry, and the no-fill/cash-drag outcome. Sol supplies scenario assumptions and probabilities; `_investorValuation.js` derives expected value, quantiles, and cost-adjusted return exactly once. Model-authored derived arithmetic is rejected rather than trusted. This makes a patient limit order comparable with cash, another opportunity, and a currently held position.

### 6.7 Holding revision: update, do not repeat

Each held-company input contains the last full thesis, prior forecast, active mandate, every revision since it, current position economics, and only the new evidence/price delta. Sol must return a company decision plus a workflow directive; these are separate fields:

- executable decision: `HOLD`, `REDUCE`, or `SELL`;
- safe non-executable decision: `ABSTAIN` with a machine-readable reason code;
- revision result: `UNCHANGED`, `REVISED`, or `PROTECTION_ONLY`;
- research directive: `NONE` or `FULL_REUNDERWRITE`.

Every held decision also supplies a unique `emergencyReductionRank`, `emergencyRankAsOf`, `emergencyRankExpiresAfterSession`, and short rationale. This is not a sell order; it tells the preauthorized emergency policy which thesis Sol would sacrifice first if a hard account breach later forces de-risking. If ranks are missing, expired, duplicated, or unsafe to use, the emergency fallback is explicitly non-investment: accidental short/uncovered exposure first, then the smallest liquid set with greatest binding stressed-loss relief per dollar. The audit labels which rule was used.

It may tighten protection or shorten duration from a delta review. It may **not** widen a loss boundary, extend a losing thesis, add to a loser, or increase maximum dollar risk without a full re-underwrite. That anti-goalpost rule prevents the model from rationalizing a deteriorating position.

### 6.8 Plan classes, capital allocation, and reservation rule

One logical Manager Meeting can publish two server-bound `PortfolioPlanProposal` classes:

1. `RISK_MAINTENANCE`: the Sol-authored HOLD/protection, REDUCE, and SELL change-set. It is validated and committed by the 09:15 hard target without waiting for unrelated candidate research. If incomplete/invalid, existing acknowledged protection remains and the affected holding becomes `ACTION_REQUIRED`; it never silently becomes HOLD.
2. `EXPANSION`: the uniquely ranked, jointly selected BUY basket after research and final cross-company synthesis. It is validated against a **new post-maintenance** activation snapshot and committed all-or-none. A failed BUY cannot suppress a SELL/reduction; an unresolved unsafe holding may, however, freeze expansion under risk policy.

Immediately before either commit, the server reconciles broker positions, open orders, settled cash, buying power, reservations and account state into a fresh immutable `activationSnapshotId`; the morning evidence/portfolio cutoff is never reused as execution truth. Sol receives deterministic `runPortfolioScenarios` output and makes the selection. Code may round downward to broker-valid whole shares or apply a smaller risk cap. A clamp is material if it rejects/zeros a proposal; changes action, rank, price, boundary, session, or basket membership; or has `deltaUnits >= 1 && (deltaPctOfProposal >= 5% || deltaNavBps >= 25)`. Boundary equality is material. Only a smaller whole-share rounding below both relative thresholds is silent. Any material clamp leaves that plan class uncommitted and returns to one Sol continuation; tiny-position fixtures cover 1-, 2-, and 3-share cases. Code may not silently create a portfolio Sol did not choose.

`_investorMandate.stagePortfolioPlan()` performs account-level compare-and-swap against `activationSnapshotId`, current portfolio version, `writerEpoch`, and reservation-account version. For each plan class, one Firestore transaction validates the complete class, rechecks aggregate cash/risk/exposure, and either:

1. writes the immutable portfolio-plan binding, every proposal/server binding/activation envelope, all reservations, desired order sets and outbox records, then advances one `committedPortfolioPlanId`; or
2. writes only a rejected-plan audit record and activates **none** of the basket.

The transaction is all-or-nothing; serial proposal staging against the same stale snapshot is forbidden. Keep the number of actionable BUYs below the transaction write ceiling. If later scale exceeds that ceiling, prepare immutable child documents first and atomically commit only a hash-complete plan pointer plus reservation aggregate; the executor reads children only through the committed pointer. `InvestorAI_ReservationAccounts` is the single aggregate ledger for reserved notional and planned/stress-risk capacity, preventing concurrent runs from double-spending.

Each validator-owned activation envelope records the AI `proposedQuantityUnits`, smaller-or-equal `authorizedQuantityUnits`, `clampReason`, `riskPolicyHash`, binding `activationSnapshotId`, reservation amounts, and portfolio-plan ID; the immutable AI proposal is never edited. All active BUY mandates fit and are fully reserved, so they can work simultaneously and no runtime race chooses companies. Lower-ranked ideas that do not fit are canonical `WATCH` decisions with `reasonCode:"UNFUNDED"` and a separate `fundingState:"UNFUNDED"`, never a new decision enum or executable order. Released cash never silently promotes one: the next scheduled or event-driven Sol portfolio synthesis must authorize it. This accepts that an unfunded touch may be missed in exchange for preventing FIFO timing or hidden deterministic logic from selecting a stock.

### 6.9 Exactly when AI is—and is not—called

| Situation | Decision path | New AI call at execution time? |
|---|---|---|
| New opportunity | Full-roster review → requested research/tools → final batch portfolio synthesis → validate/reserve/stage limit mandate | No; all judgment precedes the standing order |
| Morning holding update | Prior full thesis + overnight/premarket deltas + price/portfolio state → one morning Sol conclusion/mandate | No second “final check” |
| Target/stop/trailing/time-exit condition occurs | Broker/order simulator executes the already-authorized leg; executor reconciles | **No—$0 AI** |
| Unfilled entry authorization expires | Executor cancels entry/remainder and releases reservation; protection for filled shares remains | No |
| New verified high-impact evidence | Code safety-pauses affected unfilled entry; Sol performs a delta revision and requests full research only if needed | Yes, because the information changed |
| Ordinary evidence on a non-holding | Store and include in next frozen-roster meeting | No immediate call |
| Operator emergency/manual sell | Lock, reconcile, cancel/resize conflicting legs, execute override, audit | No AI permission required |

Thus “buying and selling” is not an ongoing token expense. The paid judgment is the scheduled or genuinely new-evidence decision that creates/revises the mandate; the deterministic executor only carries it out.

## 7. Standing mandate contract

An actionable mandate is both an investment explanation and a machine authorization. The exact discriminated JSON Schemas live in the repository and are supplied through OpenAI Structured Outputs, which enforces required keys and enums ([OpenAI Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs)). Semantic checks remain mandatory. `WATCH`, `IGNORE`, and `ABSTAIN` are immutable manager decisions but never executable mandates; `FULL_REUNDERWRITE` is a workflow directive, not a trade decision.

### 7.1 Required logical schema

Strict model output, server lineage, and risk authorization are three different records. Sol emits only `MandateProposal.v1`; application code allocates `MandateServerBinding.v1`; the validator creates `ActivationEnvelope.v1`. The UI may join them, but the OpenAI Structured Output schema must never ask the model to invent version IDs, hashes, reservation state, or validator results.

```json
{
  "schemaVersion": "mandate-proposal.v1",
  "symbol": "XYZ",
  "decision": "BUY",
  "opportunityRoute": "expectations_gap",
  "asOf": "2026-09-03T12:55:00Z",
  "thesis": {
    "summary": "Plain-language investment case",
    "whyNow": "What changed and why price is attractive now",
    "evidenceFor": ["claim_1", "claim_2"],
    "evidenceAgainst": ["claim_3"],
    "uncertainties": ["Consensus estimates unavailable"]
  },
  "forecast": {
    "basis": {
      "referencePriceMicros": "43250000",
      "referencePriceType": "BUY_LIMIT",
      "referenceTime": "2026-09-03T12:55:00Z",
      "currency": "USD",
      "returnConvention": "ARITHMETIC_TOTAL_RETURN",
      "conditionalOn": "ENTRY_FILLED_AT_REFERENCE_PRICE",
      "corporateActionPolicy": "SPLIT_AND_CASH_DISTRIBUTION_ADJUSTED",
      "horizonTradingDays": 40,
      "estimatedRoundTripCostMicrosPerShare": "40000"
    },
    "fillProbabilityByExpiryPpm": "650000",
    "noFillOutcome": "CASH_RETURN_OVER_AUTHORIZED_SESSIONS",
    "horizonTradingDays": 40,
    "outcomeBuckets": [
      {"id": "bear", "interval": "[-INF,-500bps)", "probabilityPpm": "250000", "terminalPriceMicros": "36000000"},
      {"id": "base", "interval": "[-500bps,2000bps)", "probabilityPpm": "500000", "terminalPriceMicros": "48000000"},
      {"id": "bull", "interval": "[2000bps,+INF]", "probabilityPpm": "250000", "terminalPriceMicros": "55000000"}
    ],
    "uncertaintyLevel": "MEDIUM"
  },
  "allocation": {
    "capitalRank": 3,
    "targetWeightBps": "600",
    "maxWeightBps": "700",
    "proposedQuantityUnits": "86",
    "quantityScale": 0,
    "maxCapitalMinor": "372000",
    "maxPlannedLossMinor": "52000",
    "currency": "USD"
  },
  "action": {
    "kind": "BUY",
    "entry": {
      "orderType": "LIMIT",
      "limitPriceMicros": "43250000",
      "timeInForce": "DAY",
      "regularSessionOnly": true,
      "exchangeCalendar": "XNYS",
      "validFrom": "2026-09-03T13:35:00Z",
      "authorizedSessionDates": ["2026-09-03", "2026-09-04"],
      "restageEachAuthorizedSession": true
    },
    "protection": {
      "takeProfitPriceMicros": "49000000",
      "lossBoundaryPriceMicros": "37250000",
      "lossOrderType": "STOP",
      "brokerTimeInForce": "GTC",
      "persistentAtBroker": true,
      "coverageSession": "REGULAR_ONLY",
      "timeExit": {
        "triggerAfterSessionDate": "2026-10-30",
        "submitAt": "NEXT_ELIGIBLE_REGULAR_SESSION_OPEN",
        "orderType": "MARKETABLE_LIMIT",
        "collarBps": "75",
        "onHaltOrNonfill": "KEEP_PROTECTION_AND_ALERT"
      },
      "lifecycle": "UNTIL_POSITION_FLAT_OR_REPLACED",
      "trailing": {
        "enabled": false,
        "activationPriceMicros": null,
        "offsetType": null,
        "offsetValueMicrosOrBps": null,
        "peakBasis": null
      }
    }
  },
  "invalidators": [
    {
      "predicateType": "FACT_EVENT",
      "eventTypes": ["GUIDANCE_WITHDRAWN"],
      "sourceClasses": ["SEC", "ISSUER_IR"],
      "consequence": "PAUSE_ENTRY_AND_QUEUE_REVISION",
      "humanText": "Company withdraws its current guidance"
    }
  ],
  "reviewTriggers": [
    {"type": "EVENT_CLASS", "values": ["EARNINGS", "NEW_8K", "CEO_OR_CFO_CHANGE", "MATERIAL_REGULATORY_EVENT"]},
    {"type": "RESEARCH_DUE_AFTER_SESSION", "value": "2026-10-01"}
  ],
  "sourceManifest": [
    {"claimId": "claim_1", "documentVersionId": "docv_1", "publishedAt": "2026-08-28T20:01:00Z", "retrievedAt": "2026-08-28T20:02:00Z"},
    {"claimId": "claim_2", "documentVersionId": "docv_2", "publishedAt": "2026-08-29T12:00:00Z", "retrievedAt": "2026-08-29T12:03:00Z"},
    {"claimId": "claim_3", "documentVersionId": "docv_3", "publishedAt": "2026-09-02T14:00:00Z", "retrievedAt": "2026-09-02T14:04:00Z"}
  ]
}
```

The server then binds the unmodified proposal to `{proposalId, proposalHash, portfolioPlanId, managerRunId, mandateSeriesId, version, mandateVersionId, previousVersionId, expectedActiveVersion, dossierVersionId, researchVersionId, model, reasoningEffort, promptHash, schemaHash, policyHash, contextManifestHash, sourceManifestHash}`. The validator separately writes `{status, activationSnapshotId, riskPolicyHash, authorizedQuantityUnits, reservedNotionalMinor, plannedLossAtBoundaryMinor, bindingGapStressLossMinor, clampReason}`. These records have separate schemas and hashes; the joined `MandateReadModel` is not accepted as model output.

For this example, `_investorValuation.js` derives the conditional expected terminal price as `(0.25×36)+(0.50×48)+(0.25×55)=$46.75`; gross expected return from $43.25 is 8.09%, and after the declared $0.04-per-share round-trip cost it is approximately 8.00%. It derives discrete quantiles under one documented inverse-CDF convention instead of accepting invented quantiles from the model. At the worst authorized fill, nominal boundary loss is `86 × ($43.25 − $37.25) = $516`; the $3.44 declared cost keeps planned loss below the $520 proposal ceiling. Gap/halt stress is not just displayed: its validator-derived loss must pass both per-name and aggregate binding capacity limits because a stop is not a promised fill.

There is deliberately no executable `minimumPriceMicros`. A buy limit at $43.25 can fill at any better price and therefore cannot enforce a [$39.00, $43.25] band. The research memo may display a preferred valuation zone, but the mandate exposes one executable `limitPriceMicros`. If Sol believes a sharp downward gap invalidates the thesis, it must express that as a typed event/quote review predicate and the system must avoid staging or pause before the session; no broker-native limit can guarantee cancellation before an instantaneous gap fill.

Manager v1 uses broker-enforced `DAY` entries with an explicit authorized-session list. After each close, the executor proves the old order is expired/cancelled; before the next authorized session it reconciles the broker and may restage the same hash. It never infers another day. Broker `GTD` may be enabled only where the adapter proves the exchange-date semantics. App-expired `GTC` is prohibited: an outage could otherwise leave a stale buy live past authorization. Holidays, early closes, unscheduled closures, and daylight-saving changes come from a versioned authoritative exchange/broker calendar, never hand-maintained UTC arithmetic.

When `trailing.enabled=true`, `activationPrice`, `offsetType` (`ABSOLUTE_USD` or `PERCENT`), positive `offsetValue`, and `peakBasis` (`LAST_TRADE` or broker-supported high-water mark) are required, along with whether a replacement preserves or resets the peak. The mandate holds the authorized rule; `OrderLeg` holds the observed peak and current stop. The Portfolio UI shows both and flags any difference from broker state.

#### Numeric, quantity, and time contract

No audited monetary, price, quantity, probability, rate, or return value at an investment, ledger, API, or UI boundary uses binary floating point. JSON carries those numeric values as canonical base-10 integer strings so they cannot be silently rounded. Bounded structural counts, enum ranks, and schema scales may remain validated safe JSON integers:

| Concept | Wire/storage representation | v1 rule |
|---|---|---|
| Security price | `priceMicros`, USD × 1,000,000, integer string | Convert to broker decimal exactly; reject off-tick values |
| Money | `amountMinor`, ISO-4217 minor units, integer string + `currency` | USD cents today; currency metadata is mandatory |
| Quantity | `quantityUnits`, integer string + `quantityScale` | Common-equity v1 requires `quantityScale=0`; no fractional shares |
| Probability | `probabilityPpm`, integer string | `[0,1000000]`; complete distributions sum exactly to 1,000,000 |
| Rate/weight/return | signed `basisPoints`, integer string | 100 bps = 1%; calculation keeps rational numerator/denominator until final rounding |
| Instant | RFC 3339 UTC string | event time and received time are separate |
| Exchange session | `calendarId` + `YYYY-MM-DD` | Never inferred from UTC date; calendar-version ID is retained |

`_investorMoney.js` parses these strings to `BigInt`, uses checked multiply/divide with explicit rounding modes, and supplies formatting only at the UI edge. Broker adapters prove lossless round trips for price, quantity, and fee fields. Firestore numeric queries use separately validated sortable encodings or bounded int64 mirrors; the canonical audit value remains the string. Schema tests cover sign, overflow, scale, currency, tick size, round-trip, and forbidden JSON-number input.

### 7.2 Discriminated decision and action variants

| Decision | Executable payload | Required semantics |
|---|---|---|
| `BUY` | `action.kind=BUY`, entry, protection, proposed quantity | Limit only initially; entry authorization is separate from held-share protection; the adapter must use a supported partial-fill algorithm |
| `HOLD` | `action.kind=HOLD_PROTECT`, protection for current quantity | No new entry or exposure; protection persists until flat/replaced |
| `REDUCE` | `action.kind=REDUCE`, exact whole shares, `LIMIT` or session-bound `MARKETABLE_LIMIT`, expiry, protection for remainder | Partial exit; use only an adapter transition proven safe for its capabilities |
| `SELL` | `action.kind=SELL`, exact owned quantity and session-bound `MARKETABLE_LIMIT` or `LIMIT`, collar/expiry/next-eligible-session rules | Closes exposure without the ambiguous fiction of `MARKET_NOW`; halt/nonfill behavior is explicit |
| `WATCH` | none | Research/price/event conditions may be recorded, but no order authorization |
| `IGNORE` | none | No present thesis; next roster review still covers the name |
| `ABSTAIN` | none plus `reasonCode` | Data conflict/incompleteness/uncertainty/model failure; never mapped to HOLD |

A revision of an unfilled BUY must explicitly return `KEEP`, `REVISE`, or `REVOKE`. `REVISE` creates the next immutable version; `REVOKE` cancels the entry reservation/order. A revision of a partially filled BUY separately addresses remaining-entry quantity and protection for already-filled quantity.

There is no `INCREASE` in v1. Any owned quantity makes `BUY` schema-invalid; the manager may HOLD, REDUCE, SELL, or ABSTAIN. This deliberately prevents accidental averaging-in through ambiguous action semantics.

If the thesis is complete and Sol is willing to own the company at price X, it should issue a funded BUY limit mandate now; that price touch executes without another review. `WATCH` is reserved for cases where price alone is not enough and evidence/uncertainty still requires a new judgment. In v1 its conditions are explanatory only: routine price movement waits for the next Manager Meeting and verified evidence uses the existing event router. There is no hidden intraday WATCH-condition engine and it can never buy.

### 7.3 Semantic activation checks

The activation pipeline must enforce all of the following before desired state is committed. Network/model work such as claim verification finishes first and produces immutable verdict IDs/hashes; `_investorMandate.stagePortfolioPlan()` performs only pure checks and Firestore reads/writes inside its retryable transaction:

- Schema and enum validity.
- Symbol is in the frozen eligible universe **or** is a managed held/pending symbol, and all referenced records belong to it.
- Decision time, publication times, and evidence times are not in the future.
- Every factual thesis premise resolves to a stored, verified source span. Before the transaction, `_investorClaimVerifier.js` proves span/document identity deterministically, then sends a separate source-bound structured verification job to Luna (`SUPPORTED`, `CONTRADICTED`, `INSUFFICIENT`, with exact span IDs); the Sol generation may not attest to itself. High-risk disputed premises require operator review or force ABSTAIN. Forecasts and thesis conclusions are labeled `INFERENCE` and linked to factual premises, not falsely called sourced facts. Verification version, model, tokens, latency, sampled human verdict, and disagreement are persisted and budgeted. The transaction verifies only those immutable IDs/hashes and current status.
- Required facts are current; unresolved conflicts are explicit.
- Outcome buckets are exhaustive and half-open/non-overlapping; canonical integer `probabilityPpm` values sum exactly to 1,000,000; derived quantiles are monotone under the declared convention; deterministic expected-value math matches the assumptions. Tolerance applies only to non-authoritative display conversion.
- The decision/action discriminator matches; non-executable decisions contain no order fields; BUY/HOLD/REDUCE/SELL carry every action-specific field.
- For a long BUY, target is above `limitPriceMicros` and loss boundary below it. Planned loss uses worst authorized fill, `authorizedQuantityUnits`, fees/slippage allowance, and tick rounding—not preferred or average price. Gap stress is calculated and shown separately.
- For an existing position, forward downside is measured from a fresh executable bid/mark to the loss boundary plus exit costs; average cost is shown separately only for inception P&L and tax/accounting. Current adjusted quantity, gap/overnight/halt stress, liquidity and corporate-action reconciliation are binding inputs.
- Price and quantity precision match the instrument and broker; rounding is always toward less exposure/aggression. Fractional support, session eligibility, time in force, and broker order-class support are explicit.
- Maximum planned dollar loss, binding stressed loss, gross exposure, concentration, settled cash, broker buying power, liquidity, and duplicate-position policies pass both per name and for the complete proposed basket.
- Risk creates a separate smaller-or-equal activation envelope and records why; it never overwrites the proposed quantity or mandate hash.
- Series ID, monotonic version, immutable version ID, expected active version, manager-run ID, dossier/research versions, prompt/schema/model/source-manifest/policy hashes are present.
- An active mandate with the same idempotency key cannot already exist.
- Validation occurs inside the account-level `stagePortfolioPlan` transaction described in Section 6.8. It writes an immutable committed desired plan and outbox events atomically with reservations; external broker application then follows the saga in Section 10.5.

If any check fails, store the proposed object and validation errors for audit, but never expose it to execution.

### 7.4 Typed invalidators and conservative routing

Natural-language thesis text is not executable. Predicates use allowlisted types such as `EVENT_CLASS`, `FACT_EVENT`, `METRIC_CROSSES`, `SOURCE_CORRECTION`, `DATA_STALE`, `PRICE_GAP`, and `DATE_REACHED`, with typed operands, source requirements, hysteresis where applicable, and a conservative consequence (`PAUSE_ENTRY_AND_QUEUE_REVISION`, `KEEP_PROTECTION_AND_QUEUE_REVISION`, or `FREEZE_EXPANSION`). Code performs only matching and safety action; Sol decides whether the evidence actually changes the investment thesis. Every delta still appears in the next manager context, and routing false-negative rate and review latency are measured.

## 8. Deterministic execution that does not call AI

### 8.1 Execution lifecycle

```mermaid
stateDiagram-v2
    [*] --> Proposed
    Proposed --> Validated: schema, evidence, risk pass
    Validated --> BrokerSyncPending: outbox claimed
    BrokerSyncPending --> Working: broker acknowledges desired legs
    Working --> PartiallyFilled: partial execution
    PartiallyFilled --> ProtectionPending: cancel remainder and reconcile
    ProtectionPending --> ProtectedRTH: OCO acknowledged
    ProtectionPending --> ActionRequired: protection SLA missed
    Working --> EntryFilled: entry complete
    Working --> ExitFilled: reduce/sell complete
    Working --> Paused: material event or integrity failure
    Working --> Expired: mandate time ends
    Working --> Cancelled: manager or operator cancels
    EntryFilled --> ProtectionPending: attach exact exits
    ExitFilled --> ProtectedRTH: position remains and exits resized
    ExitFilled --> Closed: position is flat
    ProtectedRTH --> OvernightGapExposed: regular session closes
    OvernightGapExposed --> ProtectedRTH: next regular session opens
    ProtectedRTH --> Closed: target, stop, time, or sell fills
    BrokerSyncPending --> Reconciling: timeout or ambiguous acknowledgement
    Reconciling --> Working: broker truth recovered
```

The executor should run every minute as a reconciliation fallback, but the broker should hold the actual limit/OCO/bracket orders. A one-minute serverless poll must not be the only thing capable of catching a transient price. The first live adapter is explicitly **polling/reconciliation based** because a persistent authenticated trading stream does not fit a short-lived Netlify function; broker-native orders catch price touches between polls. A later streaming or signed-webhook adapter needs a separate RFC and deduplicated event endpoint rather than being assumed here.

### 8.2 Broker-independent interface

```js
class BrokerAdapter {
  async getCapabilities(accountId) {}
  async getAccountSnapshot(accountId) {}
  async getSessionCalendar(calendarId, range) {}
  async submitOrderSet(desiredOrderSet, idempotencyKey) {}
  async replaceOrderSet(observedOrderSet, desiredOrderSet, idempotencyKey) {}
  async cancelOrderSet(orderSetId, reason) {}
  async getOrderSet(orderSetId) {}
  async listOpenOrders(accountId) {}
  async listPositions(accountId) {}
  async listFills(accountId, sinceCursor) {}
  async reconcile({ accountId, desiredState, observedState }) {}
}
```

Implement `PaperBrokerAdapter` first. `AlpacaBrokerAdapter` remains disabled until a separate live-trading authorization, credentials, account/jurisdiction review, and rollout gate. The current repository has Alpaca market-data credentials but explicitly has **no live broker integration**.

Every adapter publishes a versioned, startup-tested capability record; configuration may only disable capabilities, never claim unsupported ones. The validator blocks any mandate/order combination it cannot implement exactly:

| Capability | Paper v1 | Alpaca candidate adapter default | Consequence |
|---|---:|---:|---|
| DAY limit entry | Yes | Yes | Required initial entry primitive |
| Native OCO for an owned position | Yes | Yes, subject to account/order validation | Each sibling covers the same owned quantity |
| Bracket protects a partial entry fill immediately | Simulatable | No—documented exits activate after complete entry fill | Do not advertise a bracket as partial-fill protection |
| Trailing stop inside OCO/bracket | Simulatable only if policy enables | No | Reject target-plus-trailing combination; use a supported fixed stop or no live activation |
| Extended-hours bracket/OCO protection | Simulatable with explicit data | No initial support | Status is `PROTECTED_RTH`, never generically `PROTECTED` |
| Atomic group replacement | Yes | False until integration test proves it | Retain old protection or flatten; never perform an unsafe generic replace |
| Reduce-only equity exit | Yes | False until proven | Double-sell prevention cannot rely on it |
| Streaming fill notification | Deterministic simulator | Not in the Netlify polling v1 | Limited-live entry remains gated until detection/protection latency passes policy |

For the Alpaca candidate path, a native bracket cannot satisfy the stated first-partial-fill invariant by itself, trailing stops cannot be a bracket/OCO stop leg, and bracket orders are regular-session only. Alpaca also warns that both OCO exits can fill before cancellation in extreme markets and documents GTC/regular-session constraints ([Alpaca orders](https://docs.alpaca.markets/us/docs/orders-at-alpaca)). These are blocking semantic checks, not UI footnotes. A live rollout must re-verify the matrix against the broker API version and integration fixtures; unsupported combinations remain paper-only.

### 8.3 Order-set, leg, fill, and ledger contract

The current ledger assumes one active order per symbol, one fill per position lifecycle, and whole-position close; its lifecycle audit can classify a second fill as a duplicate. That is incompatible with partial fills, reductions, and OCO. Preserve integer-money double-entry journal primitives, but **rewrite** order lifecycle ownership around:

- `OrderSet`: account, symbol, mandate version, desired/applied version, purpose, status, broker group ID, reservation ID;
- `OrderLeg`: stable leg ID, role (`ENTRY`, `TARGET`, `STOP`, `REDUCE`, `SELL`), side, type, price/stop, original/filled/remaining quantity, TIF/session dates, broker ID;
- `Fill`: immutable broker/simulator event ID, leg ID, quantity, price, fee, event/received time, source cursor;
- `PositionLot/Aggregate`: quantity and cost reconstructed from fills, never assumed from one record;
- `BrokerEvent`: immutable normalized poll result with provider ID/hash and dedupe key;
- `CapitalReservation`: active/released notional and risk capacity linked to the mandate version.

`lifecycleAudit`, cash reservation, fill aggregation, position reduction, trade close, reconciliation, and mandate/order links must all be rewritten for multiple fills and multiple legs. `InvestorAI_PaperOrders`, `PaperFills`, `Positions`, and `Trades` gain `schemaVersion`, `engineVersion`, `decisionAuthority`, and migration provenance; a new executor must reject a legacy identity shape instead of guessing.

### 8.4 Order rules

- Entry: use the AI-authorized `limitPriceMicros`; never pay above it and never infer a lower price floor.
- Protection: after fill, confirm both target and stop/OCO siblings are acknowledged for the exact filled quantity before showing `PROTECTED_RTH`; outside their valid session show `OVERNIGHT_GAP_EXPOSED`. `PROTECTION_PENDING`, `PARTIALLY_PROTECTED`, and `ACTION_REQUIRED` are distinct incident states.
- Protection persistence: v1 requires broker-native GTC protective siblings that remain lodged across closes, holidays, application restarts, and mandate research deadlines until position flat or a safe replacement is acknowledged. If an adapter supports only DAY exits, live entry is rejected unless a separately proven pre-close flatten policy is active; the one-minute app may never become the only target/stop watcher.
- OCO quantity: each sibling's remaining quantity equals the protected owned quantity; their **sum normally equals twice** that quantity because either sibling may close it. Validate shared OCO group, sell side, reduce-only behavior where available, and mutual cancellation—never require sibling quantities to sum to the position.
- First partial entry fill: cancel the unfilled remainder, await a terminal cancel/reconcile result, recompute exact owned quantity, then attach a supported OCO for that quantity. Until acknowledgement the state is `PROTECTION_PENDING`, not protected. If the bounded safety deadline expires, use the preauthorized emergency flatten path when the market/session permits; otherwise freeze expansion and raise a critical overnight-risk incident. Limited live remains disabled unless measured notification-to-protection latency passes policy.
- Revision: apply only a newer desired mandate version. If the adapter supports tested atomic group replacement, use it. Otherwise keep the old plan unchanged and set `ACTION_REQUIRED`, or execute an explicitly authorized flatten; do not claim cancel-first and submit-first can simultaneously guarantee no unprotected interval and no oversell. REDUCE/SELL uses the same capability-specific state machine and reconciles every intervening fill.
- Entry expiry: cancel only the unfilled entry/remainder and release its capital reservation. It never cancels protection on shares already held. Research review due dates likewise do not switch protection off.
- Material evidence: pause/cancel an unfilled buy. Existing protective exits remain until a valid revision replaces them.
- Partial fill: aggregate fills and follow the exact cancel-remainder/attach-protection procedure above. Each OCO sibling may equal, but not exceed, protected quantity.
- Corporate action: quarantine price-based mandates until split/dividend basis is reconciled.
- Duplicate poll events/retries: derive idempotency keys from `mandateHash + action + version + leg`; also deduplicate provider event IDs and source cursors.
- Pause semantics: an operational pause may resume the same unexpired hash only after reconciliation and only if no evidence delta exists. An evidence/thesis pause requires a new Sol version. Pausing an unfilled entry cancels it; pausing a held mandate never removes protection.
- Manual exit/cancel: first lock the symbol, refresh broker truth, cancel or resize conflicting OCO legs, submit the override, and reconcile race outcomes. Preserve the original mandate and record `operatorOverride` rather than rewriting history.
- OCO race recovery: brokers warn that both exits can fill in fast markets. On detection, atomically mark the symbol `OVERSELL_INCIDENT`, cancel every remaining sell leg, reconcile fills, and invoke a narrowly preauthorized buy-to-cover for **only** the accidental short quantity using the emergency session/collar rule. It may restore zero but never create a long position. Persist the incident and require reconciliation before new exposure.

### 8.5 Paper fill fidelity

A bar touching an order level is a `TOUCH`, not automatically a `FILL`. The simulator implements side/order-specific rules: a buy limit marketable at the open fills no worse than the open/limit rule after spread and latency; an unmarketable touched limit is subject to queue position and capped volume participation; a sell stop-market gapped through fills at the open or a worse modeled eligible print; a stop-limit may trigger and remain unfilled; marketable limits obey their collar; halts forbid fills; and partials consume modeled eligible volume. If both target and stop could execute within one OHLC bar and no finer sequence exists, choose the adverse path for conservative performance or mark the observation `AMBIGUOUS`/exclude it from primary scoring—never choose the favorable path. The same rule applies when one bar first becomes entry-fillable and also crosses a target or stop: use finer eligible data when available; otherwise never award a favorable same-bar exit, and apply the predeclared adverse or unscorable convention. Store touch and fill separately with bar OHLC, quote/NBBO approximation, provider/provenance, latency, participation, trigger resolution, authorized level, simulated fill, and cost. Retain `_investorMarket.storeBars/readStoredBars`; missing or untrusted one-minute data makes the interval `UNSCORABLE`/`DEGRADED`, never a false pass for “no missed touch.”

**[v2] This convention is a hard rule for the whole system, not a paper-simulation rule.** It governs every exit evaluation — live, paper, shadow, backtest and KPI. Every protective and target level is evaluated against the bar's high and low. A bar whose range contains both levels with no finer sequence resolves adversely or is marked `UNSCORABLE`; it never resolves favourably. **Any performance figure computed under a close-only convention is invalid and must be recomputed or discarded.** The current system evaluates exits on `last.c` alone (D-2), which both erases gains that were touched and retreated and hides losses that were touched and recovered; Phase −1 item P−1.2 corrects it.

### 8.6 Desired-to-applied saga and core loop

Firestore cannot atomically change an external broker. The complete plan is committed atomically inside Firestore; each resulting mandate transition then uses a transactional outbox and explicit desired/observed state:

1. `stagePortfolioPlan` transactionally writes all immutable bindings/envelopes/reservations/desired `OrderSet`s/outbox events and one committed-plan pointer, or none;
2. worker claims the outbox event and reads fresh broker state;
3. submit/cancel/replace with idempotency key, preserving old protection while safe replacement is pending;
4. persist broker acknowledgement/event and set `appliedMandateVersion` only after all required legs are acknowledged;
5. on timeout/crash, enter `RECONCILING`, query broker truth, and resume from persisted state—never repeat blindly;
6. release old reservations/legs only after observed state proves it safe.

```js
for (const mandate of await mandates.active()) {
  if (mandate.entryExpired(exchangeCalendar)) await executor.cancelEntryRemainder(mandate);
  if (await evidence.hasHighImpactUnreviewedDelta(mandate.symbol)) {
    await executor.pauseNewEntry(mandate, "material_event_pending_review");
  }
  const safe = await risk.revalidateOperationalLimits(mandate);
  if (!safe.allowExpansion) await executor.freezeExpansion(mandate, safe.reason);
  if (safe.hardBreach) await emergencyRisk.enforceBoundedPolicy({ mandate, observed: safe });
  await executor.reconcileDesiredOrderSet(mandate, safe);
}
await broker.reconcile({ accountId, desiredState: await orderSets.desired(accountId), observedState: null });
await ledger.assertConservation(accountId);
```

This revalidation covers operational safety and current cash/exposure. It cannot decide that the company is no longer attractive; that requires a manager revision.

### 8.7 Emergency-only protection

Cutover, Sol outage, corrupt evidence, or an inherited position without a valid thesis uses a separate `EmergencyProtectionPlan`, never a fabricated AI mandate. It records issuer, position basis/quantity, hard loss ceiling, protective order terms, creator (`SYSTEM_POLICY` or operator), reason, creation/expiry/review times, permitted operations (`KEEP_PROTECTION`, `REDUCE`, `SELL`; never BUY), and replacement status. It is visible as `ACTION REQUIRED`, versioned, and can only be replaced by a valid Sol holding mandate or an operator liquidation.

Hard risk limits are enforceable even if Sol is unavailable. A separately versioned, owner-approved `EmergencyRiskPolicy` gives deterministic code narrow authority to:

1. freeze new exposure and cancel unfilled entries on stale broker truth, unresolved reconciliation, daily-loss/drawdown, buying-power, concentration, gross-exposure, stressed-loss, or protection-SLA breach;
2. neutralize an accidental short to zero;
3. reduce or flatten an unprotected position after the declared acknowledgement deadline; and
4. meet a broker margin/liquidation requirement or bring an account back under an exact hard legal/risk ceiling.

It cannot rotate into another company, average down, improve returns, or open a long. Each trigger specifies metric source, threshold, persistence/hysteresis, permitted session, maximum quantity, order type, price collar, halt/nonfill behavior, and deterministic reduction priority. Priority is: accidental short first; uncovered/margin-causing exposure second; then the smallest liquid sale set that removes the binding stress breach using frozen marginal-stress arithmetic and the latest Sol-authored emergency reduction ranks. Every action is labeled `EMERGENCY_RISK`, alerts the operator, and forces reconciliation and a later Sol review. This is the only deterministic authority that may originate a REDUCE/SELL outside an active AI mandate or explicit operator instruction.

Before any live mode, `_investorPortfolio.js` must distinguish settled cash, unsettled proceeds, regulatory/broker buying power, and withdrawable cash. The ledger must retain acquisition date and tax-lot IDs and either implement account-specific lot selection/wash-sale inputs or state prominently that every decision, mandate comparison, and reported return is pre-tax. Taxable-account replacement logic cannot silently treat pre-tax proceeds as equivalent economic value.

### 8.8 Standing invariants **[v2]**

Four invariants, each violated by the current system and each required to hold before and after the AI Fund Manager exists.

**I-1 — Fail-closed evidence.** A failure to obtain a verdict is never a verdict. No system failure, budget exhaustion, lease contention, parse failure or citation-verification failure may result in new risk, in any cohort, at any size. *(D-1, D-8b)*

**I-2 — Touch before close.** Every protective and target level is evaluated against the bar's high and low. Same-bar collisions resolve adversely or are marked `UNSCORABLE`. No performance figure is computed under a close-only convention. *(D-2)*

**I-3 — Source scope.** A source is fetched at a cadence set by its own scope, never multiplied by the roster. Entity resolution fans results out; it does not fan requests out. *(D-6)*

**I-4 — Configuration provenance.** Every result row carries the hash of the risk configuration actually in force at the decision, plus an explicit list of deviations from the declared strategy. Results produced under different configurations are never charted as one series. *(D-3, D-4, D-5)*

## 9. AI usage and daily cost model

### 9.1 What deserves the expensive model

Sol high reasoning is justified for ambiguous, comparative investment judgment: interpreting new evidence, reconciling contradictions, selecting opportunities across 304 companies, revising theses, choosing valuation assumptions, and issuing mandates. It is wasteful for HTML cleanup, arithmetic, quote parsing, stale-data checks, and order monitoring.

Luna is appropriate for source-bound extraction because that is a narrow, auditable task. Deterministic code must verify numbers and cited spans. The quality claim is not “Luna is always accurate”; it is that paying Sol to copy facts does not improve the architecture enough to justify the cost unless application-specific evals prove otherwise.

### 9.2 Current published token prices

As of this blueprint's date, OpenAI lists GPT-5.6 Sol at **$4.00 per million ordinary input tokens, $5.00 per million cache-write tokens, $0.40 per million cached-read tokens, and $20.00 per million output tokens**. Inputs above 272,000 tokens in one request are priced at 2× input and 1.5× output for the full request. The listed Sol price is promotional through at least November 21, 2026 ([GPT-5.6 Sol model page](https://developers.openai.com/api/docs/models/gpt-5.6-sol)). GPT-5.6 Luna is listed at **$0.20 / $0.25 / $0.02 / $1.20** per million ordinary-input/cache-write/cached-read/output tokens ([GPT-5.6 Luna model page](https://developers.openai.com/api/docs/models/gpt-5.6-luna)). GPT-5.6 cache writes cost 1.25× ordinary input and reads cost 0.1× ([prompt-caching guide](https://developers.openai.com/api/docs/guides/prompt-caching)). Pricing can change; the UI must read versioned configured rates rather than hardcode this paragraph.

Reasoning tokens are a subset of billed output usage, not a second charge to add to output. These are **API charges billed separately from any ChatGPT subscription**. Tool/search fees and third-party data costs are separate.

### 9.3 Budgeted daily workload

| Work | Typical uncached input | Typical billed output/reasoning | Estimated AI cost | Why it exists |
|---|---:|---:|---:|---|
| Luna evidence extraction + independent claim verification | 0.1–1.2M Luna tokens across only changed documents/claims | 20k–180k | about $0.05–$0.55 | Convert sources to cited facts and independently verify factual-premise support |
| Sol coverage + holding revision + final synthesis | 160k–300k aggregate across resumable sub-272k calls | 50k–150k | about $1.64–$4.50 | Full-roster comparison, holding revisions, final cross-name allocation; upper end allows cache writes |
| Focused research/re-underwriting tool loops | 0–300k Sol input total on a normal day | 0–150k | about $0–$4.50 | Only new/material/stale names; zero is normal; no fixed 5–10 quota |
| Material-event revision | 0–60k input | 0–30k | about $0–$0.90 | Only when an event cannot wait |
| Coverage repair / block-global synthesis contingency | 0–180k extra Sol input | 0–60k | about $0–$2.00 | Zero on a clean run; explicit P95/retry reserve, never hidden in 304-name cost |
| Anti-anchoring full refresh | 0–120k Sol input on due days | 0–60k | about $0–$1.75 | Staggered 20-session holdings/watch refresh; also included in focused-research daily ceiling |
| Weekly selected postmortems | 20k–80k Sol input on review day | 10k–40k | about $0.28–$1.15 | Offline evaluation only; report both day-of and per-session amortized cost |
| Price monitoring and execution | 0 | 0 | **$0** | Deterministic/broker execution of standing mandates |

The direct formula is:

\[
\text{AI cost} = \frac{4U_{Sol}+5W_{Sol}+0.4C_{Sol}+20O_{Sol}}{10^6}
+ \frac{0.2U_{Luna}+0.25W_{Luna}+0.02C_{Luna}+1.2O_{Luna}}{10^6}
+ \text{tool fees}
\]

where `U`, `W`, `C`, and `O` are ordinary input, cache-write input, cached-read input, and total billed output tokens. Compute `U` from usage without double-counting `W` or `C`; record reasoning tokens as an output subcategory for diagnostics only.

### 9.4 Recommended operating budget

- **Normal trading day:** provision **$4–$10** for model tokens; the lower end assumes few research jobs and useful cache hits.
- **Busy earnings/event day:** provision **$10–$20** for model tokens, with an explicit ceiling and a visible queue rather than silent model degradation.
- **Monitoring/buy/sell triggers:** **$0 AI**, because the manager already authorized them.
- **One-time bootstrap:** expect a higher variable bill while dossiers and initial holdings are underwritten; instrument it rather than pretending a precise figure before measuring the real documents.

These are engineering estimates, not quotes or total platform costs. Search/tool fees, a licensed data feed, Firestore/object storage, Netlify compute, broker fees, spreads, and slippage are reported separately. The application must record ordinary-input, cache-write, cached-read, total-output, reasoning-subset, repair/retry, repeated tool-loop context, tool-call, and per-run costs; report measured P50/P95 and ceiling breaches after at least ten shadow sessions. Five to ten genuinely deep jobs may exceed the “normal day” range and must be measured, not squeezed into a fictional token allowance. Report cost per Manager Meeting, roster name covered, focused-research job, actionable mandate, activated mandate, and fill-producing mandate; never make “cost per decision” the headline by counting 304 non-actions as 304 equivalent decisions.

Use explicit cache mode and place breakpoints only after genuinely reusable policy/schema/tool prefixes. Put changing company cards, timestamps, portfolio state, and evidence after the last breakpoint so one-use data does not incur a needless cache-write premium. A first write costs 1.25× ordinary input; only confirmed later reads receive the 0.1× rate. Do not assume a hit rate until `cache_write_tokens`, `cached_tokens`, reuse count, expiry, and realized savings prove it.

### 9.5 Cost-degradation policy

When the daily budget is near exhaustion:

1. Never cancel existing protective orders to save AI cost.
2. Finish the current holding revision before considering new opportunities.
3. Defer low-priority new research; mark it visibly.
4. Do not substitute Luna/Terra for Sol investment judgment.
5. Do not fall back to v18 residual buys.
6. If complete frozen-roster coverage cannot finish, activate no new buy mandate from that run.
7. **[v2]** Exhaustion never becomes a trade. When the reservation is spent the run abstains visibly under `evidence_unavailable` and activates no new BUY. It does not convert the shortfall into a reduced-size position (D-1, D-8b).

**[v2] Reconciling §9.4's provisioned budget with the ceilings the code actually enforces.** §9.4 provisions $4–$10 on a normal day and $10–$20 on a busy day. `_investorOpenai.js:62-63` enforces `DAILY_USD_CEILING` $5 and `CYCLE_CALL_CEILING` 12. The Manager Meeting would therefore block on day one, and today that block routes into the fail-open path of D-1. Before Phase 3, `INVESTOR_OPENAI_DAILY_USD` and `INVESTOR_OPENAI_CYCLE_CALLS` are raised to the provisioned figures and treated as a **reservation, not a cap on correctness**.

Two terms dominate the variance in §9.3's table and must be budgeted and instrumented before Phase 3:

- **Reasoning tokens.** L764 correctly states these are a subset of billed output rather than a separate charge. They remain the largest uncertain term: at 500 / 1,000 / 1,500 reasoning tokens per name, the 304-name coverage call alone costs approximately **$3.04 / $6.08 / $9.12**, the upper figure exceeding the entire normal-day envelope in a single call.
- **Cache writes.** A first write is 1.25× ordinary input; only confirmed later reads receive the 0.1× rate. §9.4 already says not to assume a hit rate — so the budget is provisioned at the **zero-hit-rate** figure and reduced only once `cache_write_tokens`, `cached_tokens` and realized savings are measured.

The one-time 304-name dossier bootstrap is the largest single bill in the project and is currently unbounded. It receives an explicit ceiling and a resumable checkpoint before it is run.

## 10. Persistence and audit model

### 10.1 Existing collections to retain

Retain and extend the useful point-in-time and accounting foundations:

- `InvestorAI_Control`
- `InvestorAI_Universe`
- `InvestorAI_SourceRegistry`
- `InvestorAI_SourceState`
- `InvestorAI_Jobs`
- `InvestorAI_Documents`
- `InvestorAI_DocumentVersions`
- `InvestorAI_Claims`
- `InvestorAI_Events`
- `InvestorAI_MarketLatest`
- `InvestorAI_MarketFiles`
- `InvestorAI_MarketDaily`
- `InvestorAI_PaperAccounts`
- `InvestorAI_PaperOrders`
- `InvestorAI_PaperFills`
- `InvestorAI_Positions`
- `InvestorAI_Trades`
- `InvestorAI_Ledger`
- `InvestorAI_Runs`
- `InvestorAI_Costs`
- `InvestorAI_Audit`
- `InvestorAI_Sessions`
- `InvestorAI_Invariants`
- `InvestorAI_ArchiveRuns`
- `InvestorAI_NavMarks`

Keep `InvestorAI_Intelligence` only as a compatibility pointer while migrating to dossiers. Preserve its historical documents.

After migration, `DocumentVersions` stores identity, point-in-time metadata, extraction state, and `contentManifestId`; it does not embed an arbitrarily large filing body. Existing small records remain readable through the codec/migration layer.

`InvestorAI_Runs` remains the operational job/cycle store (attempt, lease, checkpoint, retry, worker, status). `InvestorAI_ManagerRuns` below is the immutable investment-decision trace. They may reference each other by ID but must never duplicate or independently update the same state machine.

### 10.2 New collections

| Collection | Key | Purpose |
|---|---|---|
| `InvestorAI_Dossiers` | symbol | Small current pointer, freshness matrix, current version/hash |
| `InvestorAI_DossierVersions` | symbol + version/hash | Immutable normalized-fact IDs, content/source-manifest pointers, and memo/forecast pointers; no embedded AI opinion |
| `InvestorAI_FinancialFacts` | issuer + concept + period + as-of version | Normalized point-in-time XBRL/filing fact with unit/context/accession/amendment lineage |
| `InvestorAI_EvidenceDeltas` | symbol + from/to hashes | Added/revised/contradicted/expired facts, objective safety class, and later Sol materiality conclusion |
| `InvestorAI_ManagerRuns` | run ID | Small run trace/counters/status and content pointers for roster coverage, revisions, research, costs, and identity hashes |
| `InvestorAI_ManagerDecisions` | `{managerRunId}_{symbol}` | Exactly one immutable per-symbol disposition/revision pointer; 304 current rows do not share one oversized run document |
| `InvestorAI_ClaimVerifications` | claim + verifier version/hash | Independent source-span support/contradiction verdict, model/human audit and cost |
| `InvestorAI_ResearchMemos` | symbol + version | Immutable full underwriting and revision history |
| `InvestorAI_PortfolioPlans` | portfolio-plan ID | Immutable Sol `RISK_MAINTENANCE|EXPANSION` proposal, hashes, commit/reject state and fresh activation snapshot |
| `InvestorAI_ActivationSnapshots` | activation snapshot ID | Immutable broker-reconciled positions/orders/cash/buying-power/reservation/content hashes used for CAS activation |
| `InvestorAI_MandateProposals` | proposal ID/hash | Immutable strict Sol output only; no server/validator fields |
| `InvestorAI_Mandates` | immutable mandate version ID | `MandateServerBinding`: stable series/version/run/hash lineage and proposal/envelope references |
| `InvestorAI_ActivationEnvelopes` | mandate version ID | Validator status, authorized quantity, risk/reservation hashes, rejection/clamp reasons |
| `InvestorAI_ActiveMandates` | account + symbol | Desired and broker-applied version pointers with expected-version CAS |
| `InvestorAI_MandateEvents` | mandate + sequence | Validation, activation, trigger, broker, pause, fill, supersession audit stream |
| `InvestorAI_OrderSets` | order-set ID | Desired/applied mandate versions, broker group, lifecycle and reconciliation state |
| `InvestorAI_OrderLegs` | order-set + stable leg ID | Entry/target/stop/reduce/sell terms, quantities, provider IDs and state |
| `InvestorAI_BrokerEvents` | provider + account + event ID/hash | Immutable normalized poll observations and dedupe cursor |
| `InvestorAI_CapitalReservations` | account + mandate version | Worst-case notional/planned-risk reservation and release ledger |
| `InvestorAI_ReservationAccounts` | account | CAS-protected aggregate reserved notional/planned/stress risk and committed portfolio-plan version |
| `InvestorAI_ExecutionOutbox` | deterministic transition ID | Crash-resumable desired broker transition and claim state |
| `InvestorAI_WorkerNonces` | job + attempt + nonce ID | Expiring HMAC hash and atomic unused/consumed replay state |
| `InvestorAI_EmergencyPlans` | account + symbol + version | Protection-only authority during cutover/outage; cannot buy |
| `InvestorAI_EmergencyRiskPolicies` | account + policy version | Owner-approved exact hard triggers, reduction bounds/order, hash and effective dates |
| `InvestorAI_Alerts` | stable condition ID | Condition/resource version, severity, active/resolved state and acknowledgement history |
| `InvestorAI_Mutations` | actor + account + action + idempotency key | Canonical request hash, in-progress/terminal response, expiry, preview-token hash/consumption and audit lineage |
| `InvestorAI_CalendarSnapshots` | venue + version/range | Authoritative sessions, half days, closures, source and fetched/as-of identity |
| `InvestorAI_CorporateActions` | provider/action ID + version | Split/dividend/merger evidence, quarantine, adjustment preview, confirmation and reconciliation lineage |
| `InvestorAI_Forecasts` | forecast ID | Horizon, distribution, confidence, later outcome |
| `InvestorAI_Counterfactuals` | decision + horizon | Later return for selected and unselected companies using only post-decision data |
| `InvestorAI_Postmortems` | decision/trade ID | Error attribution, lessons, operator adjudication |
| `InvestorAI_Evals` | eval run ID | Prompt/model/schema challenger results and release gates |
| `InvestorAI_KpiDaily` | account + date | Portfolio, decision, evidence, execution, and cost KPI aggregates |
| `InvestorAI_ContentManifests` | content hash | Firestore metadata/pointer for raw filings, large packets, full outputs, and archives stored outside document fields |

### 10.3 Storage shape, size, and codec rules

Cloud Firestore limits one document to 1 MiB ([Firestore quotas](https://firebase.google.com/docs/firestore/quotas)). Therefore a manager run stores counters and hashes, never 304 decision rows; each row lives in `ManagerDecisions`. Large filings, research packets, raw model payloads, and full manifests go to a dedicated private `INVESTOR_CONTENT_BUCKET` through direct `@google-cloud/storage` use in `_investorContentStore.js`—not through an unrelated shared Firebase helper. Use least-privilege service identity, an `investor-ai/` prefix, encryption, content hashes, generation-match preconditions, and no public ACL. Firestore stores SHA-256, generation, byte length, MIME type, storage URI, compression, key version, created/as-of times, and chunk/index metadata. A fake in-memory adapter supports tests. If object storage is unavailable, fail ingest/backfill visibly—do not truncate evidence into a Firestore field.

The existing `_investorAdmin.firestoreSafe()` transforms an array directly nested inside another array into an index-keyed object. New code must not treat arbitrary Firestore reads as if JSON round-trips automatically. Add `_investorStorageCodec.js` with schema-specific `encode/decode`, one logical mandate/decision/fact per document, child documents or references for repeated complex records, byte-size checks before every write, and round-trip fixtures for every v1 schema. Retain the global guard as last-resort protection for legacy writers, but new schemas pass through the explicit codec and reject unexpected shapes.

### 10.4 Legacy collections

Freeze these read-only after cutover and label every record `engineVersion: "legacy-v18"` when projected through the API:

- `InvestorAI_Candidates`
- old `InvestorAI_Decisions`
- `InvestorAI_StrategyVersions`
- `InvestorAI_ShadowDays`, `ShadowOpen`, `ShadowClosed`, `ShadowAccounts`, `ShadowObservations`
- `InvestorAI_Calibration`
- `InvestorAI_SoakCycles`
- `InvestorAI_ScanSnapshots`
- `InvestorAI_EntryPlans`

Never reinterpret an old Candidate or EntryPlan as an AI authorization. Drain/cancel legacy pending proposals before enabling the new executor.

### 10.5 Mandate pointer and broker-application saga

The pre-market run first writes immutable Sol proposals, then reconciles a fresh `ActivationSnapshot`. A Firestore transaction can atomically commit **desired portfolio state**, but cannot atomically alter an external broker. Use the Section 6.8 account-level commit and Section 8.6 outbox saga:

1. Read portfolio/account/reservation versions, each series `desiredVersion`/`appliedVersion`/next sequence, and the fresh broker-reconciled activation snapshot.
2. Verify proposal prior hashes/expected pointers and the complete `PortfolioPlanProposal` hash.
3. In `stagePortfolioPlan`, validate every actionable member and aggregate constraint; allocate server IDs/versions; write immutable bindings, envelopes, reservations, desired order sets and deterministic outbox events.
4. Atomically advance `committedPortfolioPlanId`, reservation aggregate, and each desired pointer—or commit none. Old `appliedVersion` pointers and acknowledged protection remain until broker transition.
5. Workers reconcile/cancel/replace at the broker using idempotent leg IDs and the adapter-specific safe transition.
6. After required acknowledgements, transactionally advance each applied pointer, mark the prior version superseded-applied, and append broker events; plan application may be `PARTIAL_EXTERNAL` while reconciliation completes, never falsely atomic.

On crash or ambiguous acknowledgement, broker truth wins and the order set enters `RECONCILING`. No code may mark a plan applied merely because Firestore committed. Event document IDs include series, mandate version, and allocated sequence, so retries cannot fork lineage.

### 10.6 Required indexes and retention

The repository currently has no checked-in Firestore index definition even though composite queries are load-bearing. **[v2]** Declare and deploy these indexes through whatever mechanism the operator already uses for Firestore; this plan adds no `firebase.json`, `.firebaserc` or index-readiness script. At minimum index:

- active mandates by account/status/expiry/priority;
- manager runs by date/status;
- manager decisions by run/decision/symbol and symbol/as-of;
- portfolio plans by account/status/as-of; activation snapshots by account/portfolio version/as-of;
- claim verifications by claim/verdict/as-of;
- dossier versions by symbol/as-of;
- evidence deltas by symbol/objective safety class/created time and by Sol materiality/review time;
- mandate events by mandate/sequence and account/time;
- forecasts/counterfactuals by horizon/resolution time;
- document versions by symbol/published/as-of time.
- order sets by account/status and desired/applied version;
- order legs by order set/status/role; broker events by account/provider cursor;
- capital reservations by account/status/rank; reservation accounts by committed plan/version; outbox by status/next-attempt time;
- alerts by active/severity/updated time; calendar snapshots by venue/effective range/version; emergency policies by account/effective time.

Current-pointer documents stay small. Append a version only when its content hash changes. Export/archive raw high-volume event and bar data under an explicit retention policy; do not delete the audit chain needed to reproduce a decision.

### 10.7 Additive migration contract

`scripts/investor/migrate-v18-to-manager-v1.js` runs dry-run first, uses a deterministic `migrationId`, checkpoints collection/document cursors, records before/after hashes and counts, and is safe to resume. It adds—without erasing legacy meaning—`schemaVersion`, `engineVersion`, `decisionAuthority`, `migrationId`, and legacy source IDs to `Control`, `PaperOrders`, `PaperFills`, `Positions`, and `Trades`. `Control` gains separate account mode, manager state, executor state, freeze-buys, emergency-stop, active policy/hash, desired/applied schema epochs, and migration checkpoint.

Legacy orders/EntryPlans are never converted into AI mandates. They are inventoried, drained/cancelled, or represented read-only. The new ledger/executor rejects records missing a recognized v1 identity unless an explicit compatibility adapter handles them; migration verification compares cash, positions, fills, journal balance, order counts, and hashes before enabling a writer. Rollback disables new writers and restores read routing—it does not delete new audit records or resurrect old automatic entries.

## 11. Scheduler, jobs, and API contract

### 11.1 Scheduler

Keep `investorKick` as the sole one-minute cron dispatcher, but replace its task vocabulary and bind every worker nonce to the exact target function:

| Task | Handler | Owner module | Due rule / behavior |
|---|---|---|---|
| `ingest` | `investorIngest-background.js` | evidence, fundamentals, dossier | Continuous bounded source queue; fetch/version/normalize and create deltas; no investment decision |
| `premarket_manager` | `investorManager-background.js` | manager/jobs/OpenAI | Trading day after initial evidence cutoff; freeze manifests, cover full roster/holdings, manage async Sol response |
| `focused_research` | `investorManager-background.js` | research/valuation/jobs | Continuation inside the manager run or event revision; checkpoint every bounded tool batch |
| `portfolio_synthesis` | `investorManager-background.js` | manager/portfolio risk/OpenAI | After focused event research, compare against the current full decision/portfolio set; only this path may propose a new BUY |
| `event_revision` | `investorManager-background.js` | event router/manager | Deduplicated high-impact delta; entry already safety-paused; Sol determines investment meaning |
| `execute` | `investorExecution-background.js` | execution/broker/ledger | Every minute/retry; claim outbox, poll broker, reconcile; no model call |
| `postclose` | `investorPostclose-background.js` | NAV/learning/KPI | After official exchange close and final mark; attribution, forecast/counterfactual observations |
| `archive` | `investorArchive-background.js` | archive/content manifests | Nightly bounded export/retention manifests |

Each one-minute `investorKick` has a 20-second dispatch budget and launches at most four jobs: due `execute` work first; then critical event revisions; then existing async-response continuations; then pre-market manager; then ingest; then post-close/archive. At most one new heavy job per category launches per tick. Dispatch stops when the deadline is near, leaving durable queue state for the next minute. Research/ingest/model latency can therefore never starve order reconciliation.

`investorKick.dispatch()` writes `{jobId, task, targetFunction, attempt, payloadHash}`, creates a random nonce ID, stores only its HMAC/hash and `UNUSED` state with a short expiry under the attempt, and signs the exact binding. The worker transaction verifies signature, expiry, payload hash and handler identity **and atomically changes the nonce to `CONSUMED` while claiming that job attempt**. A retry receives a new attempt/nonce; a replay receives 409. Job leases, heartbeats, deterministic IDs, checkpoint cursors, and retry semantics remain.

Long OpenAI work is an async response polled by successive short manager jobs. OpenAI says `store:false` background data is temporarily stored for roughly ten minutes, so the application polls each due response every minute, treats two minutes without a successful poll as degraded, and uses an eight-minute terminal-output-persistence SLA ([OpenAI background mode](https://developers.openai.com/api/docs/guides/background)). Persist terminal output and usage immediately to the private content store and heartbeat `lastSuccessfulPollAt`. If a response expires, query persisted terminal content first; otherwise record `RESPONSE_EXPIRED_UNKNOWN`, reserve retry cost, and require an operator or idempotency-policy decision—never blindly resubmit an expensive Sol run. Store provider response ID, request hash, billing/usage seen, attempt lineage, and whether a retry might be double-billed.

**[v2] Dispatch windows are per task class.** `investorKick.js:81` sets `PLAN_EARLIEST_MIN = 9*60+45` and `:101` rejects any configured time outside `[09:45, 15:30]`. The `premarket_manager` task above therefore cannot be dispatched at all (D-8). `normalizePlanTimes` is parameterized by task class rather than reading one module-level constant:

| Task class | Window (ET) | Rationale |
|---|---|---|
| `scan` (existing opportunity scan) | 09:45–15:30 | Unchanged. The existing comment is correct: scans are meaningful only after the opening auction and need time before the close. Behaviour stays bit-identical. |
| `premarket_manager`, `ingest`, `focused_research` | 04:00–09:15 | Model and evidence work; no order is placed. |
| `execute` | market hours plus extended per broker | Unchanged path. |
| `postclose`, `archive` | after official close | Unchanged path. |

A task class with no declared window is not dispatchable.

### 11.2 API v2 reads

Continue using the authenticated POST surface initially to minimize infrastructure churn, but give it explicit v2 actions and bounded pagination:

| Action | Response |
|---|---|
| `managerDashboard` | portfolio summary, latest manager run, frozen-roster coverage, deltas, alerts, active mandates, spend |
| `controlState` | orthogonal account mode, manager, buy-freeze, emergency and executor requested/applied states plus server-authorized actions |
| `companies` | paginated eligible/excluded/managed-off-roster rows with decision/research/freshness/plain reason |
| `companyDossier` | facts, changes, thesis, valuation, forecast, sources, decision/mandate history |
| `portfolio` | positions, economics, thesis health, mandate levels, trigger distance |
| `mandates` | active/history with validation and execution state |
| `orderSets` | paginated desired/applied order sets, legs, fills, reservations, broker sync state and errors |
| `executionEvents` | paginated mandate/order/broker/ledger timeline with stable correlation IDs |
| `managerRuns` | trace/progress/coverage/cost and failure details |
| `jobs` | generic paginated async-job status/checkpoints/errors for manager, research, reconciliation and export |
| `decisionJournal` | BUY/WATCH/IGNORE/HOLD/REDUCE/SELL/ABSTAIN plus evidence and later outcomes |
| `decisionAnalytics` | calibration, selected-vs-unselected, missed opportunity, attribution |
| `performance` | NAV, benchmark, risk, trading/data/AI costs |
| `materialEvents` | unresolved/resolved event revision queue |
| `corporateActions` | quarantined/pending/rebased split and dividend reconciliation queue/history |
| `systemHealth` | source/dossier/manager/mandate/reservation/executor/broker health separately |
| `universe`, `sources` | canonical universe snapshot/exclusions and source/provider detail |
| `soakStatus` | manager/executor acceptance evidence and unscorable intervals |
| `auditExports` | bounded job status and, only when complete, `{manifestId,sha256,bytes,downloadUrl,downloadExpiresAt}`; short-lived same-origin/signed URL issuance is audited |
| `alerts` | persistent typed alerts, underlying condition state, acknowledgement and resolution history |
| `account`, `quotes`, `intraday`, `history`, `navSeries`, `ledger` | retained operational/detail reads |

### 11.3 API v2 mutations

| Action | Behavior |
|---|---|
| `pauseManager` / `resumeManager` | explicit future-job state transitions; pause never stops executor or removes existing protection |
| `runManagerReview` | browser sends only `{reason:"OPERATOR"}`; server stamps accepted/effective as-of and evidence cutoff, freezes context and deduplicates; `SCHEDULED_PREMARKET` is internal-only |
| `requestResearch` | operator-requested research that cannot directly activate without normal validation |
| `runFocusedRevision` | enqueue a specific evidence-delta review |
| `pauseMandate` / `resumeMandate` | typed pause; same-hash resume only for reconciled operational pause with no delta; thesis pause requires new Sol version |
| `cancelMandate` | revoke unfilled entry authorization; for held shares preserve protection via current/emergency plan and require explicit replacement or sell |
| `requestSell` | retain manual emergency/operator sell path |
| `cancelSell` | cancel only an unfilled manual sell after locking/reconciling/resizing protection |
| `freezeBuys` / `resumeBuys` | explicit requested/applied new-exposure state; resume requires reconciliation and server authorization |
| `emergencyStop` / `resumeSystem` | kill/recovery controls with reconciliation |
| `activateAccountMode` | reauthenticated hash-bound `OBSERVE → PAPER_AI` release after preflight; future live transition is a separate RFC/action |
| `deactivateAccountMode` | reauthenticated `PAPER_AI → OBSERVE`; freeze/cancel acknowledged unfilled entries, preserve held protection/executor reconciliation, never auto-unfreeze |
| `setBudget` | per-role daily ceilings and alert thresholds |
| `setRiskMandate` | bounded hard-risk policy; versioned/audited; a newly over-limit book freezes expansion and queues Sol/operator resolution rather than silently liquidating |
| `setEmergencyRiskPolicy` | reauthenticated owner approval of exact hard triggers/reduction bounds/order; cannot open long exposure and activates only after dry-run impact preview |
| `setMarketConfig` | retain provider/credential configuration |
| `confirmSplit` / `confirmCashDividend` | retain corporate-action reconciliation |
| `requestAuditExport` | enqueue bounded point-in-time audit export; return job ID, never large content inline |
| `createPaperAccount` / `previewPaperAccountReset` / `resetPaperAccount` | Manager v1 has exactly one active paper account; preview returns one-use `{previewToken,accountVersion,balanceHash,recordsAffected,expiresAt}`; reset requires it, reauthentication, no position/order/active mandate, and conservation proof |
| `reconcile` | enqueue broker/ledger reconciliation, never perform a long broker workflow inline |
| `freezeUniverse` | publish/freeze a versioned roster snapshot; never alter investment decisions |
| `resolveCiks` | admin-only bounded identity repair with source manifest and audit |
| `setIssuerDomains` | audited allowlisted issuer/IR-domain correction with expected source-registry version; cannot authorize a trade |
| `acknowledgeAlert` | record operator acknowledgement by alert ID/version; never clears the underlying safety condition or marks it resolved |

Retire from the normal operator API/UI: `learning`, `knobs`, `candidate`, per-trade `approve/reject`, `recordRecallBenchmark`, `setRegime`, `setIntelligenceWatchlist`, `setPaperLearning`, and arbitrary old `setControl` strategy tuning. Preserve compatibility reads until the new UI no longer calls them; each removed action returns a versioned `410 ACTION_RETIRED` only after call telemetry proves no new-UI consumer remains.

`emergencyStop` means freeze expansion, cancel unfilled entries, stop non-protective transitions, and reconcile while leaving acknowledged protective exits working. It is not “cancel every order” and does not liquidate the portfolio. Any future liquidate-all control must be a separately named, strongly confirmed operation. `resumeSystem` first reconciles broker/ledger/desired state and cannot revive an evidence-paused or expired entry.

`requestSell` requires `{positionId, symbol, quantityUnits, quantityScale:0, orderType, timeInForce, limitPriceMicros?, exchangeSession, collarBps?, reason, expectedPositionVersion, expectedMandateVersion}`. A UI “sell 25%” shortcut resolves against the displayed version to a downward-rounded whole `quantityUnits` preview; the mutation always sends the exact resolved quantity. Its accepted response includes `overrideId`, `orderSetId`, requested/applied state, `cancelableUntil`, and protection-resize status. The symbol lock + broker refresh + OCO cancel/resize/re-arm saga in Section 8 owns all fill/cancel races. `cancelSell` accepts the override/order-set/version, not just a symbol.

### 11.4 Dashboard payload rule

Do not repeat the current six-megabyte dashboard failure. `managerDashboard` contains summaries only; company details and histories are paginated/on-demand. Every collection response includes `asOf`, `nextCursor`, `payloadVersion`, and `partial=false/true` with a reason. The UI must never infer complete roster coverage from a progress animation. Company/journal/order/event searches are server-side; filters and sort are schema enums; opaque cursors bind to `snapshotId + universeVersion/managerRunId + filterHash + sort + lastKey`, so mid-run writes cannot skip/duplicate rows. Responses return total and per-state bucket counts for that frozen snapshot.

### 11.5 Wire/auth contract

All v2 reads use `{apiVersion:"investor.v2", requestId, action, params}`. Mutations additionally require `{idempotencyKey, csrfToken, auditReason?}` plus the action-specific concurrency field: `expectedResourceVersion`, `expectedAbsent:true`, or a one-use preview token. All responses use `{ok, requestId, payloadVersion, asOf, partial, partialReason, nextCursor, data, error}`; mutation responses add `{mutationId,acceptedAt,jobId?,resourceVersion,requestedState?,appliedState?}` only where meaningful. Errors are typed as `{code,message,severity,retryable,fieldIssues,correlationId}` with consistent HTTP mapping: 400 schema, 401 expired/no session, 403 origin/CSRF/role, 404 unknown resource, 409 version/state conflict, 422 semantic rejection, 429 budget/rate, 503 degraded dependency. JSON Schemas under `netlify/functions/investor-assets/schemas/api-v2/` reject unknown fields, oversized page sizes, malformed cursors, unsafe numbers, and unsupported transitions.

`investorApi.js` must dispatch `ACTIONS_V1` only when `apiVersion` is absent/`investor.v1` and `ACTIONS_V2` only for exact `investor.v2`; each registry has separate schemas. During observation, v2 mutations are disabled. At cutover, `InvestorAI_Control.writerEpoch` grants exactly one engine mutation authority per account and all v1 investment mutations return `410 ACTION_RETIRED`; emergency compatibility is explicitly mapped, not inferred. This prevents two UIs/writers acting on one account.

Before side effects, `_investorApiV2.js` transactionally claims `actorId + accountId + action + idempotencyKey` in `InvestorAI_Mutations` with the canonical request hash. The same key/hash returns the stored in-progress or terminal result; the same key with different content returns `409 IDEMPOTENCY_KEY_REUSED`; expiry never removes the underlying audit link. One-use reset/risk previews store a payload/account-version hash and change `UNUSED → CONSUMED` in the same transaction that accepts the mutation. This state is durable across Netlify cold starts and prevents duplicate sells or control transitions.

Add `investorSession.js` for `create`, `refresh`, and `revoke` session operations. Prefer a Secure, HttpOnly, SameSite=Strict cookie before any live mode; login/refresh returns expiry and a rotating CSRF token in the response body, sign-out revokes server state and clears the cookie, and 401 triggers the visible sign-in gate without replaying a mutation. A paper-only bearer compatibility path, if temporarily required, is same-origin, memory-only, short-lived, and never URL/local-storage state. State-changing actions require exact origin, CSRF, idempotency, audit reason where required, and optimistic version. `setMarketConfig`, `setRiskMandate`, `setEmergencyRiskPolicy`, `freezeUniverse`, `resolveCiks`, account reset, source-domain change, both account-mode transitions, `resumeBuys`, `resumeSystem`, and budget increases require reauthentication; `emergencyStop` remains immediate. Secrets are write-only and never returned. Background actions accept only atomically consumed single-use HMAC worker nonces. No browser payload contains broker/OpenAI/cloud credentials or direct mandate activation authority.

## 12. New module and asset map

### 12.1 New production files **[v2]**

**[v2] Scope correction.** v1 listed 96 files. Sixty-four of them were not application code: 36 JSON schema files, 5 prompt `.md` files, 3 test files plus a Playwright/emulator toolchain, `firebase.json`, `firestore.indexes.json`, a GitHub Actions workflow, three one-off migration scripts, a `.css` and a `.svg`, and 13 files splitting `investor.html` into ES modules.

That decomposition does not match how this application deploys. Netlify bundles `netlify/functions/**` with esbuild; the operator console is one self-contained `investor.html`; JSON schemas are already carried inline in code (`CLASSIFY_SCHEMA` in `_investorOpenai.js`); prompts are already carried inline in the same module; and the repository already has a working, deploy-gated test harness in `_investorSelftest.js` whose results block trading through `controlAllowsEntry` (`_investorLedger.js:499`). Introducing an npm/CI/emulator/Playwright toolchain would replace something that works with something weaker, since CI tests do not gate trading and these do.

**The file map is therefore Netlify function modules plus the existing console. Nothing else.**

| New file | Required exports/responsibility |
|---|---|
| `netlify/functions/_investorStorageCodec.js` | Schema-aware encode/decode, size limits, content references, round-trip assertions |
| `netlify/functions/_investorMoney.js` | BigInt parsing, checked rational arithmetic, explicit rounding, tick/lot/ISO-4217 conversion and sortable mirrors |
| `netlify/functions/_investorContentStore.js` | Content-addressed GCS put/get/verify and immutable Firestore manifest pointer |
| `netlify/functions/_investorFundamentals.js` | SEC XBRL/companyfacts/frames normalization, amendment/context/unit/period/share reconciliation |
| `netlify/functions/_investorDataProviders.js` | Provider-neutral point-in-time news/estimate/transcript interface, entitlement/retention metadata, correction lineage |
| `netlify/functions/_investorPolicy.js` | `loadActive`, `validateHashSet`, exchange/risk/model/cost policy identity |
| `netlify/functions/_investorJobs.js` | `claimOnce`, `enqueueOnce`, `prioritizeNextManagerPacket`, `checkpoint`, `heartbeat`, `complete`, `failClosed`; operational runs only |
| `netlify/functions/_investorPortfolio.js` | Point-in-time account/position/order/reservation snapshot and portfolio constraint inputs |
| `netlify/functions/_investorPortfolioRisk.js` | Pure read-only factor/sector/correlation/liquidity/marginal and gap/halt stress scenarios for Sol |
| `netlify/functions/_investorDossier.js` | `buildBaseline`, `buildDelta`, `compactCards`, `expandedHoldingDeltas`, `freshnessMatrix`, `current`, `persistVersion`, routine-delta record |
| `netlify/functions/_investorManager.js` | `claimManagerRun`, `freezeDecisionCutoff`, `runManagerMeeting`, `validateCoverage`, `persistDecisionRows`, `finalizePortfolio`, `complete/failClosed` |
| `netlify/functions/_investorResearch.js` | `latest`, `buildPacket`, `persistImmutable`, and bounded priority-ordered `ResearchPool`; managed research, suffix-only deferral, barrier, manifests/content pointers |
| `netlify/functions/_investorResearchTools.js` | Read-only tool schemas/dispatch; symbol/as-of and URL scope, argument/result/token/byte/call bounds, source-result hashes and audit |
| `netlify/functions/_investorClaimVerifier.js` | Span identity plus independent Luna structured support/contradiction verification and audit sampling |
| `netlify/functions/_investorValuation.js` | Pure DCF/reverse-DCF/multiples/SOTP/residual-income/unit/event-tree calculators |
| `netlify/functions/_investorMandate.js` | Validate/bind proposals and atomically `stagePortfolioPlan`; stable lineage, reservations, desired pointers/outbox, pause/revoke |
| `netlify/functions/_investorEventRouter.js` | Entity resolution, dedupe, objective event classes/high-recall safety pause, all-delta Sol queue; no investment materiality judgment |
| `netlify/functions/_investorExecution.js` | Order-set/leg translation, outbox saga, OHLC paper semantics, broker poll/sync, idempotency, reconciliation |
| `netlify/functions/_investorBroker.js` | Broker interface and paper adapter; live adapter feature-flagged off |
| `netlify/functions/_investorEmergencyRisk.js` | Enforce only hashed owner-approved hard triggers, freeze/cancel/reduce/flatten bounds and oversell neutralization; emit `EMERGENCY_RISK`, never AI decisions |
| `netlify/functions/_investorAlerts.js` | Derive/upsert stable alert conditions, acknowledge, and resolve only from underlying condition |
| `netlify/functions/_investorApiSchemas.js` | Pinned Ajv 2020 schema compilation/format validation; fail build/cold start closed |
| `netlify/functions/_investorApiV2.js` | `ACTIONS_V2`, request handlers and typed view-model presenters; no v1 fallthrough |
| `netlify/functions/_investorLearning.js` | Forecast resolution, counterfactuals, postmortems, version-aware retrieval memory |
| `netlify/functions/_investorKpi.js` | Pure KPI definitions/calculations and daily aggregation |
| `netlify/functions/_investorEvals.js` | Task-specific model/prompt/schema eval runner and promotion evidence |
| `netlify/functions/investorIngest-background.js` | Bounded evidence/fundamental/dossier ingest handler |
| `netlify/functions/investorManager-background.js` | Resumable async-response manager/research/event handler, separate from executor |
| `netlify/functions/investorExecution-background.js` | Outbox claim, broker polling, fills, protection and reconciliation handler; no AI import |
| `netlify/functions/investorPostclose-background.js` | Final marks, outcomes, counterfactual and KPI observations |
| `netlify/functions/investorArchive-background.js` | Bounded content/audit archive and retention-manifest handler |
| `netlify/functions/investorSession.js` | Create/refresh/revoke HttpOnly sessions, rotate CSRF, clear cookies, expiry/revocation and reauthentication |

**Schemas** stay inline in the module that owns them, exactly as `CLASSIFY_SCHEMA` does today. Every schema keeps a `schemaVersion` string and a stable hash so the mandate contract's `schemaHash` binding (§7) still works — a hash over an inline literal is identical to a hash over a file.

**Prompts** stay inline in `_investorOpenai.js`, which is already the sole model boundary (§12.2). `promptHash` is computed over the literal. If a prompt later needs to be edited without a code deploy, `netlify.toml` already bundles `netlify/functions/prompts/**` via `included_files` and that path can be reused — but nothing in this design requires it.

**Tests** are added as `fixture()` cases in the existing `_investorSelftest.js` (§17.2). No new test files, no test framework, no lockfile, no CI pipeline.

**UI** remains the single `investor.html`. §14's view architecture, component IDs, state definitions and defect list all still apply; they describe sections of one file rather than separate modules.

**Indexes** are declared and deployed by whatever mechanism the operator already uses for Firestore. No `firebase.json` or `firestore.indexes.json` is added by this plan.

### 12.2 Model gateway rewrite

`_investorOpenai.js` should remain the only OpenAI boundary and retain its useful spend reservation, cache lease, `store:false`, redaction, and verbatim citation checks. Rewrite its roles and expose only:

```js
extractFacts({ documentVersions, schemaVersion })          // Luna
verifyClaims({ claimPremises, immutableSourceSpans, schemaVersion }) // Luna; independent factual-support check
reviewUniverse({ cards, universeManifest, holdings, portfolio, policy }) // Sol high
repairCoverageStructure({ responseId, missing })                 // Sol continuation; missing rows only
researchCompany({ dossier, delta, portfolio, tools })      // Sol high
finalizePortfolio({ coverage, researchResults, holdings, portfolio, feasibleAlternatives, policy }) // Sol high; expansion basket
reviseEntry({ baseline, delta, state, portfolio })          // Sol high; unfilled/partial entry
reviseHolding({ baseline, delta, position, mandate })       // Sol high; later event only
finalizeEventRevision({ priorAssessment, expandedResearch, state, portfolio, scenarios }) // Sol high
writePostmortem({ frozenDecision, laterOutcome })           // Sol; offline only
```

Every request records request/response IDs, asynchronous status/checkpoint, model returned, reasoning effort, token categories, tool calls, prompt/schema/policy/context hashes, latency, refusal/truncation, repair/retry, and cost. Tool names, symbols, URLs, arguments, call count, response bytes, time, and tokens are server-side allowlisted and bounded; results are hashed and source content remains inert untrusted data. A refusal or incomplete response never becomes `HOLD` by default; it becomes `decision: "ABSTAIN"` with `reasonCode: "MODEL_FAILURE"` or another precise reason so the UI cannot confuse silence with judgment.

## 13. Exact current-file disposition

The action column means **production authority after migration**. Retired files remain available for historical replay until a later archival decision; they are not deleted in the first cutover.

| Existing file | Action | Exact target responsibility |
|---|---|---|
| `investor.html` | Rewrite | Thin semantic five-view shell and authentication gate; move CSS/JS out; retain charts through modules; delete deterministic UI logic |
| `investor/strategies/v1.json` | Retire | Stale v1 hypothesis; archive as legacy metadata, never runtime policy |
| `investor/universe/v1.json` | Retire/replace | Stale 45-trade/12-research roster; generate one canonical versioned 304 roster from `_investorUniverse.js` before later retiring the JS duplicate |
| `netlify/functions/_investorAdmin.js` | Retain/extend | Preserve Firestore namespace/global legacy nested-array guard; add collection constants and require explicit v1 storage codecs/byte checks for new records |
| `_investorAllocator.js` | Retire | Remove variant-test/leader allocation; AI priorities allocation, risk only clamps |
| `_investorAuth.js` | Retain/harden | **Enforce and reject** non-allowlisted Origin/Host (not merely set CORS); add HttpOnly session lookup, CSRF/revocation/reauth, atomically consumed HMAC worker nonces, redaction and isolated broker roles |
| `_investorBootstrap.js` | Rewrite | Bind readiness/safety epoch to manager policy, roster, prompt/schema/model hashes; backfill/check dossiers and active protection |
| `_investorCalibration.js` | Retire | Remove deterministic variant calibration/forward lock; new calibration lives in `_investorLearning.js`/`_investorEvals.js` |
| `_investorCorporateActions.js` | Retain/extend | Keep split quarantine and share/price rebasing; pause and rebase mandates transactionally |
| `_investorDecisionFeedback.js` | Rewrite | AI decision/outcome attribution, selected/unselected counterfactuals, error labels |
| `_investorDecisionManifest.js` | Rewrite | `hashAll`, `hashMandateProposal`, and replay validation for model/prompt/schema/policy/context/sources/research/mandate identity |
| `_investorEvidence.js` | Retain/extend | SEC/document versioning, as-of queries, claim spans; preclassification is evidence metadata only |
| `_investorExitPolicy.js` | Replace authority | Evaluate explicit AI mandate triggers and narrow emergency controls only; no rank/time/2% universal thesis |
| `_investorExplore.js` | Retire | Remove Thompson ordering, control cohorts, paper exploration cadence |
| `_investorFetch.js` | Retain/harden | Sole bounded public HTTP boundary, allowlists, SSRF/DNS/redirect/content/rate/hash controls, injection-safe ingestion; bulk SEC ZIPs use the offline streaming bootstrap instead |
| `_investorHistory.js` | Retain/extend | Point-in-time price/fundamental history and exact features; no buy/sell conclusions |
| `_investorIntelligence.js` | Rewrite | Build source-linked dossier/delta/event evidence; remove adverse-only sizing/decision authority |
| `_investorIntelligenceSources.js` | Retain/extend | Source registry/entity/domain routing; replace 24-watchlist/6-per-sweep starvation with fair dynamic-roster due queue |
| `_investorLadder.js` | Retire | Remove research→shadow→limited-auto deterministic ladder; rollout gates move to eval policy |
| `_investorLease.js` | Retain | Worker heartbeat and account-wide execution/entry mutex |
| `_investorLedger.js` | Rewrite lifecycle / retain journal | Preserve integer double-entry primitives; replace one-fill/whole-close lifecycle with order sets, legs, multiple fills, partial reductions, reservations, broker events and reconciliation |
| `_investorMarket.js` | Retain/extend | Market settings, authoritative versioned XNYS calendar/session snapshots, feed quality, `storeBars/readStoredBars`; add quote/order/touch and corporate-action-aware semantics; delete hand-generated holiday authority |
| `_investorMultiSession.js` | Demote | Keep five-session/sector-relative calculations only as optional dossier features; no admission rule |
| `_investorNav.js` | Retain | Marked NAV snapshots/series with mandate revision markers |
| `_investorOpenai.js` | Rewrite | Luna fact extraction + Sol manager/research/revision roles and strict schemas; remove Terra investment path |
| `_investorPatience.js` | Retire | Remove historical-bounce sleeve and underwater rank suppression |
| `_investorPlainReason.js` | Retire | Stored structured AI prose is data; escaped UI renderers inside `investor.html` own display, with no backend hard-coded strategy narrative |
| `_investorPositionGuard.js` | Replace/shim | During migration delegate to `_investorExecution`; then keep a compatibility wrapper or retire |
| `_investorResearchStats.js` | Retire | Remove old variant-gate authority; DSR/PBO/multiple-testing research metrics belong in offline `_investorEvals.js`, not daily KPI authority |
| `_investorRisk.js` | Retain/rewrite | Normal `buildActivationEnvelope` and operational `revalidate`; hard cash/exposure/drawdown/liquidity/concentration/tail bounds; reject or smaller envelope only; emergency order authority lives separately |
| `_investorSelftest.js` | **[v2] Retain and extend** | This file is the verification harness, not a subset of one (§17.5). It gains the new schema/plan/execution/authority invariants and the nine suites of §17.2 as additional `fixture()` cases. Nothing moves to a separate test directory. |
| `_investorShadow.js` | Archive/rewrite later | Preserve legacy benchmark replay; new shadow manager operates on mandates and forecasts |
| `_investorSignal.js` | Retire authority | Optional pure feature extraction only; remove residual/rank/gate decisions from runtime |
| `_investorSoak.js` | Rewrite | AI mandate lifecycle, failure-mode, reconciliation, target-touch and cost soak evidence |
| `_investorState.js` | Rewrite | Orthogonal versioned machines: `accountMode=OBSERVE|PAPER_AI|LIMITED_LIVE`; `managerState=ENABLED|PAUSED`; `buyState=OPEN|FROZEN`; `emergencyState=CLEAR|ENGAGED|RECOVERING`; `executorState=MONITORING|APPLYING|RECONCILING|PAUSED_SAFETY`; component health `HEALTHY|DEGRADED|DOWN|ACTION_REQUIRED`; every control has requested/applied state |
| `_investorStrategy.js` | Freeze/replace | Preserve v18 replay; runtime reads `fund-manager-v1.json`, not deterministic thresholds |
| `_investorStrike.js` | Retire | Do not adapt inert legacy EntryPlans; `_investorExecution.js` handles unified mandates |
| `_investorSufficiency.js` | Rewrite | Required-input/freshness/conflict attestations; no heuristic that sizes thin data |
| `_investorTemporal.js` | Retain/demote | Scheduled event/exposure/seasonality evidence for Sol; no independent liquidation/allocation authority |
| `_investorUniverse.js` | Migrate | Current actual v6 roster (342 declared, 38 excluded, 304 trade); emit canonical eligibility/exclusion snapshot and `freezeEligibleSnapshot`, then become loader only |
| `_investorVariants.js` | Retire | Remove fourteen deterministic policy arms from production |
| `_investorVisibleText.js` | Retain | Conservative visible-text extraction and hidden-subtree handling |
| `_investorWorkset.js` | Retain/rewrite | `buildManaged` for `frozen eligible ∪ held ∪ pending`, plus fair dossier queue, explicit exclusions and source transitions |
| `investorApi.js` | Rewrite/additive v2 | New bounded manager/dossier/mandate/journal/KPI API; preserve auth and operational compatibility reads |
| `investorCycle-background.js` | Split/retire | Keep legacy worker behind flag during dual run; move ingest, manager, execution, and post-close into bounded jobs |
| `investorKick.js` | Rewrite | Dispatch new task vocabulary and due queues; remain fast and model/ledger-free |
| `netlify.toml` | Update | Bundle only backend `netlify/functions/investor-assets/{policy,schemas,prompts}/**` plus canonical file-backed universe data; configure workers/cron; set version-safe UI asset caching and strict Investor CSP; never expose backend assets as static files |
| `package.json` | **[v2] Leave alone** | It is `etsy-app`, shared with unrelated subsystems, and its build script targets `cherry-viewer`. This plan adds no test framework, no Ajv/Playwright/axe dependency and no npm scripts. Netlify installs the existing dependencies at deploy; the investor system needs nothing further from it. Adding a lockfile remains worthwhile for reproducible deploys, but it is not part of this plan. |
| lockfile (new) | Add | Commit a reproducible npm lockfile generated by the supported Node/npm toolchain and enforce clean install in CI |
| `netlify/functions/netlify.toml` | Delete/deprecate explicitly | It is a tracked stale alternative, not necessarily simultaneous production config; remove ambiguity and ensure tooling selects root config |
| `netlify/functions/secrets/gcpPrivateKey.txt` | Remove and rotate credential | Tracked mode-644 private key is treated as compromised; never print it. Revoke/rotate, inventory deployments/logs, purge history as appropriate, move to least-privilege managed secrets, and block recurrence with secret scanning |

This table deliberately includes `investor.html`, both stale JSON files, all 43 Investor JS files, root build/dependency files, the nested configuration hazard, and the separately discovered credential blocker. The repository currently has no lockfile, and `package.json` uses ranges (including `@google-cloud/firestore: ^6.8.0`) plus one `latest`; it is not reproducibly pinned today.

The Investor response header policy is `default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'`, adding only a specifically documented provider origin if a same-origin backend proxy cannot serve a required resource. **[v2]** The console is one self-contained `investor.html` with no separate UI modules, stylesheet or icon sprite, so there are no versioned UI assets to cache. `investor.html` must revalidate on every load — `netlify.toml` already sets `Cache-Control: max-age=0, no-cache, no-store, must-revalidate` on it — so a stale shell cannot retain incompatible contracts. Because styles and scripts are inline, `script-src` and `style-src` require the corresponding `'unsafe-inline'` or, preferably, per-block hashes; adopt hashes. Function `included_files` continues to carry backend assets only.

## 14. UI overhaul

### 14.1 Five-view information architecture

Remove the `Advanced` drawer completely. The operator should never need to understand residual ranks, policy arms, promotion ladders, or which cheap model was allowed to see a company.

| View | Purpose | Required components |
|---|---|---|
| **Manager** | What the AI manager decided today and what is happening now | Account/risk hero; latest Manager Meeting; frozen-roster coverage; evidence changes; new/revised decisions; material-event inbox; authorization summary; workflow; AI spend; Manager On/Pause, Review Now, Freeze Buys, Emergency Stop |
| **Companies** | Complete transparent coverage book and opportunity workbench | Every eligible/excluded/managed-off-roster row; search; AI disposition; short reason; “what changed”; dossier/research freshness; opportunity route; active mandate; research queue/watch/expired tabs; full company drawer |
| **Portfolio** | Every holding and its exact current plan | Position economics; thesis health; current target/loss boundary/duration; distance to levels; next catalyst/review; plan version; revision history; working orders; execution timeline; manual pause/sell |
| **Decisions & Learning** | Prove whether the manager adds value | Immutable decision journal; NAV/performance; benchmark; forecast calibration; expected vs realized; selected vs unselected; missed opportunities; error attribution; model/prompt comparisons; after-cost results |
| **System** | Health, audit, budget, and safety—not strategy tinkering | Source/dossier/job/model/executor/broker health; ledger; costs; market config; Sol/Luna role-policy and prompt/schema hashes; risk limits; account; read-only global-control state with a link to Manager; source/universe/corporate-action administration; audit export |

### 14.2 Stable component IDs

Use predictable component IDs so backend/UI contract tests can assert that the operator sees every critical state:

- Global shell: `#criticalBanner`, `#alertCenter`, `#signOut`—present on every route.
- Manager: `#managerStatus`, `#managerReviewSummary`, `#universeCoverage`, `#evidenceDelta`, `#holdingRevisions`, `#newDecisions`, `#materialEventInbox`, `#managerAuthorizationSummary`, `#managerWorkflow`, `#managerSpend`, `#managerRunDetail`, `#managerModelSummary`.
- Companies: `#companySearch`, `#companyCount`, `#companyPager`, `#companyFilters`, `#companyCoverageList`, `#researchQueue`, `#opportunityList`, `#watchList`, `#expiredPlans`.
- Company drawer: `#companyDrawer`, `#companyIdentity`, `#companyChart`, `#companyEvidenceDelta`, `#companyThesis`, `#companyValuation`, `#companyDecision`, `#companyPlan`, `#companyCatalysts`, `#companyRisks`, `#companySources`, `#companyDecisionHistory`.
- Portfolio: `#portfolioSummary`, `#holdingList`, `#standingAuthorizations`, `#workingOrders`, `#executionQueue`, `#executionTimeline`, `#planRevisionHistory`.
- Learning: `#performanceSummary`, `#forecastCalibration`, `#expectedVsRealized`, `#selectionLift`, `#thesisAccuracy`, `#missedOpportunityAudit`, `#decisionAttribution`, `#modelComparison`.
- System: `#systemAlerts`, `#sourceHealth`, `#sourceAdministration`, `#dossierHealth`, `#managerJobHealth`, `#modelHealth`, `#executorHealth`, `#brokerHealth`, `#marketConfig`, `#accountConfig`, `#accountModeActivation`, `#policyIdentity`, `#modelPolicyStatus`, `#ledgerSummary`, `#reconcileSystem`, `#costBreakdown`, `#budgetAdministration`, `#riskAdministration`, `#universeAdministration`, `#freezeUniverse`, `#resolveCiks`, `#corporateActionQueue`, `#auditExport`.
- Global operating control groups, rendered only in Manager: `#pauseManager/#resumeManager`, `#reviewNow`, `#freezeBuys/#resumeBuys`, `#emergencyStop/#resumeSystem`. The server-provided `availableActions` determines which member of a pair is visible/enabled; the browser never inverts stale state. Status summaries: `#managerMode`, `#managerModelSummary`, `#dailyBudget`, `#riskMandate`. Model routing is read-only and changes only in a versioned release. Contextual position overrides stay beside the affected Portfolio row; reauthenticated provider/risk/budget/corporate-action administration stays in System. Neither duplicates the global controls.

Rows and cards use stable resource attributes and never duplicate DOM IDs. Repeated controls use server-returned `data-action`, `data-resource-id`, and `data-resource-version` for research requests, mandate pause/resume, sell/cancel, corporate-action confirmation, and alert acknowledgement; the browser never invents permission from status text.

The five routes are a release information-architecture hypothesis, not a sacred metric. Validate desktop/mobile wireframes with the operator before implementation. Manager above the fold answers only: **what changed, what Sol decided, what is executing, and what needs me**. Research detail, cost drill-down, histories, and source manifests use progressive disclosure; a panel with no current action collapses rather than becoming permanent dashboard noise.

### 14.3 Exact current-view migration

| Current `investor.html` area | Disposition |
|---|---|
| Sign-in gate (`#gate`, `#gateForm`, `#pc`, `#gateErr`) | Keep; rename “Volatility Desk” to “Investor AI — AI Fund Manager”; state paper/live mode and risk disclaimer |
| Today hero, holdings, history, NAV | Keep useful financial information; rebuild as Manager and Portfolio components |
| `#todayPendingCard` + `#todayPlansCard` | Merge duplicate panels into standing authorizations/execution timeline |
| `#todayDesk`, dynamic `#deskCtl`, Control Room controls | Replace three competing controllers with one four-control surface on Manager; System mirrors state read-only |
| `#opsConsole` | Rebuild as factual workflow: evidence → full roster reviewed → research/revisions → mandates → execution |
| `#todayWatchCard` | Replace residual leaders with manager attention/research queue |
| `#todayBlocksCard` | Replace gate tally with plain capital-deployment explanation |
| Companies (`#v-companies`) | Keep/rebuild for the complete dynamic roster plus exclusions/managed off-roster, with AI disposition/freshness/reason |
| Results + advanced Performance | Merge into Decisions & Learning |
| Briefing (`#v-briefing`) | Remove as a destination; migrate only spend/health/activity |
| Every ranked name (`#v-candidates`) | Remove; opportunities live under Companies |
| Position detail (`#v-positions`) | Merge into Portfolio |
| Approvals (`#v-approvals`) | Remove mandatory per-trade approval ceremony; optional operator overrides remain |
| Learning (`#v-learning`) | Replace deterministic variant ladder with forecast/outcome learning |
| Intelligence (`#v-intelligence`) | Replace 1–24-name watchlist with persistent eligible-roster dossiers |
| Settings explained (`#v-knobs`) | Remove deterministic knobs; show policy/risk/model/cost configuration |
| No-trade log (`#v-notrade`) | Replace with all-action Decision Journal |
| Ledger (`#v-ledger`) | Keep under System/Audit; link every row to mandate and execution |
| Sources (`#v-sources`) | Keep under System; show coverage/freshness/failure, not experiment holdouts |
| Toast | Keep for low-severity confirmation; critical failures become persistent banners |

### 14.4 Functions and language to remove

Delete the user-facing logic for:

- `renderMovers`, `renderLiveConfig`, `renderExploration`, `renderLadder`, `renderMemory`, and the manual `renderRegime` controls.
- `causeTag`, `gateChips`, the eight-step `derivation`, candidate pass/blocked/rank/z/strict presentation, and the fixed five-session `sessionWhy` selector.
- `GATE_PLAIN`, fixed `exitPlanText`, `patienceBadge`, and hard-coded `EXIT_PLAIN` claims.
- Eagerness/holding/size presets `EAGER`, `HOLD`, `SIZE`, `nearest`, `eagerNow`, `sizeNow`, and `holdNow`.
- Paper-learning controls, recall-benchmark controls, frozen-policy wizard, scan-frequency choices, and `setIntelligenceWatchlist` as an eligibility gate.
- Ranked/breach counters and “automation proof” ladders.

Metrics such as five-day change, z-score, volatility, moving averages, earnings distance, and valuation ratios can still appear inside a dossier as labeled evidence. They must not display as unexplained pass/fail chips.

Migrate the remaining controls one by one:

- `#modePill` becomes account mode (`Observe`, `Paper AI`, future `Limited Live`); `#sessPill` retains market session; `#syncLab` shows distinct freshness times; `#railState/#railProv` show manager/executor state and data provenance; `#refreshBtn` refreshes display only.
- Retire editable `#inpGuard`, `#selStrikeBars`, and `#btnStrike`; show executor cadence and data resolution read-only in System.
- Retire `#ceilSel/#btnCeil`, `#btnHold`, `#btnSafety`, and `#btnDry` as strategy controls. Replace them with versioned budget/risk status and explicit account mode.
- Map current `#btnFreezeEntries` to `#freezeBuys` and `#btnUnfreezeEntries` to `#resumeBuys`. Current `#btnFreeze` is Universe “Verify & freeze current” and moves to System `#freezeUniverse`. Move `#btnCiks` to automatic identity-health repair with System `#resolveCiks` as the admin fallback.
- Preserve verified issuer-domain correction currently exposed through `#intelDomains`, but move it to Source Administration; changing a domain is audited and cannot itself authorize a trade.
- Preserve useful approval-card facts—decision age/expiry, expected economics, planned loss, same-cycle alternatives, source hashes, validation and sufficiency—inside mandate detail while deleting approve/reject buttons.
- Delete `#todayHistWhy` and its deterministic narrative that profits do not matter.
- Preserve `requestSell` and pending `cancelSell`: Portfolio displays a confirmation with quantity/order type, then a cancellable pending state only until execution begins. A manual sell never silently edits the AI thesis.
- Add an explicit corporate-action queue for `confirmSplit` and `confirmCashDividend`, with source, affected position/mandate, proposed basis change, and confirmation history.

### 14.5 Current UI defects to close during the rewrite

- Three separate control surfaces can disagree.
- Pending Orders and Orders duplicate the same state.
- `#todayStats` is hidden and its renderer is unused.
- `tradeCard()` is unused.
- `plainBlock()` is declared twice, allowing the later declaration to override the earlier behavior.
- The operations console filters for a cycle object and then checks whether that object is an evidence job, making its research branch unreachable.
- The Companies badge `#nHeld` shows a holdings count rather than company count.
- `#csName` renders sector rather than company name.
- Results “Account value” renders explanatory text rather than the value/chart.
- Critical failures disappear in a 2.6-second toast.

Do not port these defects into separate JS/CSS files.

### 14.6 Charts

- Retain the NAV/equity chart; overlay deposits, buys, sells, target/stop fills, and mandate revisions.
- Retain company daily/intraday price, volume, and SMA overlays as evidence.
- Add executable AI limit price, non-executable preferred valuation zone (clearly labeled), profit target, planned loss boundary, mandate creation/revision, and catalyst markers.
- Remove the highlighted 12-bar “signal window” and any chart treatment that implies it is the decision rule.
- Every touch tooltip must show authorized level, observed bar high/low, trigger time resolution, actual/simulated fill, and slippage.

### 14.7 Icons and status language

Use one SVG sprite alongside visible text; decorative icons use `aria-hidden="true"`. Only standalone icon buttons receive an accessible name. Color is never the only signal.

| Meaning | Icon metaphor | Text label |
|---|---|---|
| Manager | Compass | Manager |
| Companies/research | Building with magnifier | Companies |
| Portfolio | Briefcase | Portfolio |
| Decisions/learning | Open book with check | Decisions & Learning |
| System/safety | Shield | System |
| Active plan | Target | Authorized |
| Material event | Bell/document | Review required |
| Execution | Paired arrows | Executing/Filled |
| BUY | Arrow into portfolio | BUY |
| SELL | Arrow out of portfolio | SELL |
| HOLD | Pause bars | HOLD |
| REDUCE | Arrow partially out | REDUCE |
| WATCH | Eye | WATCH |
| IGNORE | Muted circle | IGNORE |
| ABSTAIN | Shield-question | ABSTAIN—data/uncertainty |
| Pause/freeze | Pause bars/snowflake | Paused/Frozen |
| Reconcile | Circular arrows | Reconciling |
| Stale/degraded | Clock/warning triangle | Stale/Degraded |
| Protected/unprotected | Shield-check/shield-alert | Protected RTH/Unprotected |
| Partial fill | Half-filled circle | Partially filled |
| Profit/loss level | Target/down boundary | Target/Loss boundary |
| Duration | Calendar-clock | Authorized through |
| Evidence | Linked document | Source evidence |
| Cost | Receipt | Cost |
| Error | Octagon-alert | Action required |
| Utilities | Magnifier/filter/refresh/close/menu/download | Search/Filter/Refresh/Close/Menu/Export |

Replace mixed Unicode `◆`, `▶`, `✓`, `·`, `✗`, `⚠`, and single-letter marks. Use green only for authorized/healthy, amber for review, red for blocked/risk, and blue for information. Never use green/red alone to express P&L or action.

### 14.8 Canonical visible states

- AI decision: `BUY`, `WATCH`, `IGNORE`, `HOLD`, `REDUCE`, `SELL`, `ABSTAIN`.
- Research: `CURRENT`, `UPDATE_AVAILABLE`, `REVISION_RUNNING`, `FULL_RESEARCH_REQUIRED`, `STALE`, `INCOMPLETE`.
- Manager run: `QUEUED`, `RUNNING`, `PARTIAL`, `COMPLETE`, `FAILED`, `CANCELLED`.
- Research adds: `QUEUED`, `DEFERRED_BUDGET`, `FAILED`.
- Mandate/execution: `PROPOSED`, `REJECTED`, `VALIDATED`, `RESERVED`, `BROKER_SYNC_PENDING`, `WAITING_FOR_PRICE`, `WORKING`, `PARTIALLY_FILLED`, `PROTECTION_PENDING`, `PARTIALLY_PROTECTED`, `PROTECTED_RTH`, `OVERNIGHT_GAP_EXPOSED`, `FILLED`, `CANCEL_PENDING`, `ENTRY_EXPIRED`, `CANCELLED`, `PAUSED_OPERATIONAL`, `PAUSED_EVIDENCE`, `SUPERSEDED`, `RECONCILING`, `OVERSELL_INCIDENT`, `ACTION_REQUIRED`, `ERROR`, `UNPROTECTED`. `PROPOSED` and `REJECTED` are validator lifecycle states, never human approvals and never executable.
- Source/feed: `CURRENT`, `STALE`, `DEGRADED`, `DOWN`.
- Thesis health: `IMPROVING`, `INTACT`, `WEAKENED`, `BROKEN`, `UNKNOWN`.
- Corporate action: `QUARANTINED`, `AWAITING_CONFIRMATION`, `REBASED`.
- Orthogonal controls: `accountMode=OBSERVE|PAPER_AI|LIMITED_LIVE`; `managerState=ENABLED|PAUSED`; `buyState=OPEN|FROZEN`; `emergencyState=CLEAR|ENGAGED|RECOVERING`; `executorState=MONITORING|APPLYING|RECONCILING|PAUSED_SAFETY`.
- Component health: `HEALTHY`, `DEGRADED`, `DOWN`, `ACTION_REQUIRED`.

Each control exposes requested and applied states separately. Account mode, manager state, buy freeze, emergency state, executor state, component health, market session, and feed state are independent badges; `PAUSED` never ambiguously describes all of them. Internal underscore enums render as plain labels such as “Waiting for price.”

The UI must distinguish source freshness, dossier freshness, manager-review freshness, mandate freshness, and market-mark freshness. “Fresh” is not one global boolean.

### 14.9 Client behavior

- The browser may poll manager summaries every 30 seconds and active jobs every 2.5–5 seconds; quote display can retain its 5/15-second cadence.
- Browser polling is display-only. A closed tab cannot stop broker orders or server-side execution.
- Paginate company and journal data; fetch a full dossier only when opened.
- Persist filters in URL/session state, but never persist credentials or investment authority in local storage.
- API fetches use `credentials:"same-origin"`; the rotating CSRF token stays memory-only, sign-out calls session revoke, bearer tokens are never kept in `sessionStorage`, and a mutation is never automatically replayed after 401 or 409.
- Critical errors remain until acknowledged/resolved.
- Show actual completed counts using the run's frozen denominator—for example, “304/304 reviewed,” “6/8 holdings revised,” and “2 research jobs deferred”—not an animated estimate.
- Preserve the display-versus-executable-feed distinction on every quote: provider, feed, delay, and timestamp remain visible; displayed price is never described as the fill price.
- Company chart keeps stable range and volume/trend toggles. The drawer traps focus, closes on Escape/backdrop, restores trigger focus, supports a URL deep link, aborts stale requests when switching symbols, and becomes full-screen on small screens.
- Render model rationale and source snippets with `textContent` or a tightly constrained structured renderer—never raw model HTML/Markdown through `innerHTML`. Validate citation URLs and use `rel="noopener noreferrer"`.
- Explicitly handle 401/session expiry, abortable requests, loading/empty/stale/partial/failure states, `payloadVersion`, and cursor errors. An older or partial poll response can never overwrite a newer complete frozen-roster state.

### 14.10 Exact UI view-model contracts

The JSON Schemas in `netlify/functions/investor-assets/schemas/api-v2/` define these minimum fields; UI code may not infer them from legacy dashboard objects:

Canonical UI primitives are `Money{currency,amountMinor,minorScale}`, `Price{currency,priceMicros}`, `Quantity{quantityUnits,quantityScale}`, RFC-3339 UTC instants, and `SessionRef{calendarId,calendarVersion,sessionDate}`. Authoritative payloads are never preformatted strings. `_investorMoney.js` validates/calculates server values; the display helpers inside `investor.html` parse bounded canonical integers with `BigInt` and use `Intl.NumberFormat` only for display. Server and browser share JSON test vectors. Every mutable resource contains `{resourceId,resourceVersion,availableActions[]}`; each action capability is `{action,enabled,disabledReason,requiresReauth,requiresReason,confirmationKind}`. The UI never infers eligibility.

| View model | Required fields |
|---|---|
| `ManagerDashboardView` | singular account with NAV/settled cash/buying power/reserved/available `Money`; `models{investment{snapshot,reasoningEffort},extraction{snapshot,reasoningEffort}}`; policy/schema/prompt hashes; latest run/coverage/holding-review summaries; committed `portfolioPlanId` + `activationSnapshotId`; decision/research/event/authorization counts; spend, health and persistent alerts |
| `ControlStateView` | independent requested/applied account/manager/buy/emergency/executor states, `writerEpoch`, last transition/correlation, blocking conditions and server action capabilities |
| `ManagerRunView` | run/evidence/universe/portfolio IDs and hashes, started/cutoff/deadline/completed instants, physical call/repair/retry checkpoints, exact 304-dynamic coverage, per-state counts, missing rows, research jobs, final plan, costs and typed failure |
| `CompanyRowView` | symbol/name/sector, frozen eligibility/exclusion, canonical decision plus separate review directive, reason, changed flag, thesis/dossier/research/evidence freshness, data quality, held/pending flags, mandate state, capital rank and opportunity route |
| `CompanyDetailView` | identity history; common/sector facts with point-in-time IDs and canonical numeric primitives; deltas; sourced premises versus inference; thesis/opposition/uncertainty; valuation inputs/derived outputs; forecast basis/distribution/fill probability; decision/mandate/binding/envelope hashes; catalysts, invalidators and source manifests |
| `HoldingRowView` | position/decision/mandate/binding/envelope/order-set IDs and versions; symbol and `Quantity`; average-cost `Price` separately from executable mark `Price`; market-value/unrealized-P&L `Money`; weight bps; forward and stressed downside `Money`; thesis health; target/boundary `Price`; time-exit `SessionRef`; distance bps; protection coverage/state; emergency rank/expiry; off-roster/tax-treatment labels |
| `AuthorizationExecutionView` | proposal/server-binding/envelope/portfolio-plan/activation-snapshot IDs and hashes; desired/applied versions; proposed/authorized/filled/remaining `Quantity`; reservations `Money`; clamp/risk fields; entry/target/boundary `Price`; `validFrom`, TIF, authorized sessions, typed legs, fills, broker capability/coverage, sync, errors and override lineage |
| `ResearchJobView` | job/run/symbol, kind/directive/status, baseline/delta/dossier/source-manifest versions, requested/started/completed instants, tool budget/usage, claim-verification summary, output hash and typed failure |
| `OrderSetView` | order-set/plan/mandate/account/symbol IDs, desired/applied state, broker group/capability version, reservation, each typed leg with canonical prices/quantities/TIF/session/provider IDs, fills, coverage window, reconciliation cursor and error |
| `ExecutionEventView` | immutable event/correlation/account/symbol/plan/mandate/order/leg IDs, authority `AI_MANDATE|OPERATOR|EMERGENCY_RISK`, type/state, requested/observed values, event/received instants, broker source/dedupe ID and resulting ledger IDs |
| `JobView` | job/task/handler/attempt, state/priority/lease/checkpoint, created/due/heartbeat/completed instants, response polling SLA, progress numerator/denominator, cost, retry lineage and typed error |
| `MaterialEventView` | event ID/version, symbol/class/headline/source URL and manifest, published/first-seen instants, verification, affected mandate/order state, safety pause, manager job/review resolution and server capabilities—never browser trade inference |
| `CorporateActionView` | action ID/version/type/source/effective dates, old/new security and ratio/cash primitives, affected lots/mandates/orders, quarantine/rebase preview, confirmation/reconciliation history and capabilities |
| `AlertView` | stable alert/condition/resource ID/version, severity, active/resolved state, title/plain detail, first/last seen, acknowledgement actor/time, resolution evidence, correlation IDs and capabilities |
| `AuditExportView` | export job/version/snapshot/filter/status, requested/completed/expiry instants and, when complete, manifest ID/SHA-256/byte count/short-lived download URL |
| `DecisionJournalRowView` | run/symbol/decision/review directive, proposal/mandate/plan versions, short rationale, premise/inference/source hashes, forecast, alternatives, activation/result state, later outcomes and attribution authority |
| `DecisionAnalyticsView` | frozen cohort/benchmark definitions, horizon, dependence-aware method/effective sample size/confidence interval, calibration/selection/counterfactual/error metrics, exclusions and version IDs |
| `PerformanceView` | NAV and benchmark series IDs, total-return/cash/tax basis, deposits/withdrawals, realized/unrealized P&L and trading/AI/data costs as `Money`, return/drawdown/volatility/Sortino bps, exposure, stress and confidence intervals |
| `SystemView` | source/dossier/model/manager/mandate/reservation/executor/broker/ledger/calendar health; backlog/freshness; active identities/hashes; account/risk/budget/universe/source/corporate-action configuration summaries; audit state and capabilities |
| `ChartSeriesView` | series ID/version/as-of, currency/calendar/adjustment/provenance, resolution, points using RFC-3339 plus canonical prices/quantities, gaps, scorable state and mandate/event markers |
| `SpendView` | per task/model ordinary/cache-write/cached-read/total-output/reasoning-subset token-count strings; reserved/actual/tool/data `Money`; retries; daily limit/remaining/deferred work; P50/P95; meeting/covered-name/research/actionable/activated/fill-producing denominators |
| `HealthView` | each component has `state,lastSuccessAt,expectedBy,ageSeconds,backlog,errorCode,conditionId,operatorAction`; health never substitutes for the orthogonal control state |

Every list wraps `{collectionState: "LOADING|READY|EMPTY|PARTIAL|STALE|FAILED", asOf, items, completedCount, totalCount, nextCursor, error}`. `store.js` compares semantic resource version plus request sequence before committing. A partial page/run may merge known rows by stable key but cannot replace a complete newer snapshot. Citation URLs are allowlisted `https`, source titles/snippets are untrusted text, and the UI preserves current sign-out/session-expiry behavior.

## 15. Decision vocabulary and criteria

The manager must explain decisions in plain language, but the actions need stable semantics:

| Decision | Meaning |
|---|---|
| `BUY` | Evidence is sufficient; expected after-cost outcome is attractive relative to cash, benchmark, and alternatives; downside is understood; a complete mandate is issued |
| `WATCH` | Plausible thesis, but price, catalyst, confirmation, or evidence is not yet adequate; no executable authorization |
| `IGNORE` | No defensible edge, outside mandate, structurally unattractive, or clearly inferior opportunity; reason retained for counterfactual analysis |
| `HOLD` | Existing thesis and opportunity cost justify keeping the position under an explicit current exit plan |
| `REDUCE` | Keep part of the thesis but lower exposure for changed expected value, risk, liquidity, correlation, concentration, or opportunity cost; exact remaining protection is issued |
| `SELL` | Thesis invalidated, valuation realized, opportunity cost superior, risk changed, or time/catalyst case failed; issue executable disposition |
| `ABSTAIN` | Evidence is incomplete/conflicting, valuation cannot be bounded, model cannot answer reliably, or system integrity is degraded |

The model should not optimize a weighted score. It must make a comparative judgment and expose the reasoning. The following are required considerations, not deterministic gates:

- Evidence strength and independence.
- Business quality, competitive position, management, and capital allocation.
- Financial trajectory and balance-sheet survivability.
- Valuation range and what expectations price already embeds.
- Catalysts, time horizon, and path dependency.
- Bear case, permanent-capital-loss mechanisms, and thesis invalidators.
- Liquidity, volatility, crowding, correlations, and portfolio role.
- After-cost expected return versus cash, benchmark, current holdings, and other opportunities.
- Missing information and uncertainty.

A BUY without an evidence-backed bear case and invalidators is invalid. A SELL does not require waiting for the price target if new evidence breaks the thesis. A target touch already covered by an active mandate does not require a SELL call; it executes mechanically.

### 15.1 Hard-risk policy and sizing precedence

`netlify/functions/investor-assets/policy/fund-manager-v1.json` embeds a versioned `riskMandate` with currency/account NAV basis and explicit operator inputs: maximum single-name and sector weights, gross/net exposure, minimum settled-cash reserve, maximum planned and binding stressed loss per position/in aggregate, daily-loss/freeze thresholds, peak-to-trough drawdown states, maximum order/position share of rolling ADV, maximum spread, overnight exposure, correlated-cluster cap, gap/halt scenarios, and open-order notional. Manager v1 fixes whole shares and regular-session protected orders; fractional shares and extended-hours execution are not configurable. Paper defaults are labeled assumptions; live operation is blocked until the owner supplies capital, loss tolerance, liquidity needs, tax/account constraints, jurisdiction, and broker rules.

Manager v1 should be long-only common equity plus cash, with no leverage, options, short selling, margin expansion, or averaging down from a delta-only review. `SELL` can close only owned quantity. Any broader instrument set requires a new discriminated schema, risk model, market-data contract, simulator, broker capability test, and explicit policy release; it must not slip in through a prompt change.

For a long BUY, quantity authority is unambiguous:

\[
Q_{authorized}=\operatorname{lotFloor}\left(\min\left[
Q_{AI},
\frac{cashCapacity}{limitPrice+costPerShare},
\frac{nameCapacity}{limitPrice},
\frac{sectorCapacity}{limitPrice},
\frac{plannedLossCapacity}{limitPrice-lossBoundary+costPerShare},
\frac{stressLossCapacity}{limitPrice-gapHaltStressFill+stressCostPerShare},
ADVCapacity,
overnightCapacity
\right]\right)
\]

`Q_AI` is Sol's immutable `proposedQuantityUnits`; `targetWeightBps` explains intent and `maxWeightBps` is an AI ceiling, but neither silently overrides quantity. The validator may authorize the calculated smaller `authorizedQuantityUnits` or reject below the broker/instrument minimum. AI confidence does not scale size until prospective calibration proves that it should. Planned-risk aggregation includes held shares and working entries at worst authorized fills. Gap/overnight/halt stress is both displayed and a **binding** per-name/aggregate size constraint; it is not disguised as a guaranteed stop outcome. For an existing holding, forward risk starts at the fresh executable bid/mark, while average cost appears only in P&L/tax reporting.

Precedence is: emergency stop/freeze → data/corporate-action integrity → broker/account legality → hard portfolio risk → active AI mandate. Normal safety layers may cancel expansion or reduce authorization, never select a replacement company, raise exposure, widen a boundary, or cancel protection. Only the separately hashed `EmergencyRiskPolicy` may issue bounded `EMERGENCY_RISK` reductions/flattening under Section 8.7; those actions are never recorded or scored as Sol decisions.

## 16. Learning architecture and KPIs

### 16.1 What “learning” means

Learning is controlled memory and evaluation:

1. Freeze exactly what the manager saw and predicted.
2. Observe later market/company outcomes only after the forecast timestamp.
3. Compare selected companies with eligible unselected companies and declared benchmarks.
4. Attribute errors to evidence, retrieval, interpretation, valuation, sizing, timing, execution, or exogenous surprise.
5. Add validated failure cases to an evaluation set.
6. Test a challenger prompt/model/policy in shadow.
7. Promote only after prospective evidence and explicit version approval.
8. On a predeclared random sample of non-holdings, hide the prior WATCH/IGNORE conclusion during an isolated anti-anchoring review, then reconcile the independent result afterward.

It is **not** an agent editing its own prompt after a loss. That would chase noise and create an uncounted strategy search.

### 16.2 Required outcome record

For every BUY/WATCH/IGNORE/HOLD/REDUCE/SELL/ABSTAIN, store:

- prices and benchmark returns at 1, 5, 20, 60, and declared-horizon trading days;
- maximum favorable excursion (MFE) and maximum adverse excursion (MAE);
- whether entry, target, loss boundary, catalyst, invalidator, and time horizon occurred;
- expected vs realized after-cost return;
- portfolio impact, opportunity cost, and selected-vs-unselected result;
- thesis element resolution and source corrections;
- forecast outcome intervals/event definitions and frozen resolution procedure;
- forecast reference price/time/currency/return convention, total-return and corporate-action treatment, entry-fill probability, no-fill cash-drag outcome, horizon and deducted costs;
- model/prompt/schema/policy/context versions;
- AI, data, spread, slippage, and opportunity costs;
- operator overrides and whether scoring should separate manager quality from manual action.

### 16.3 KPI definitions

| Layer | KPI | Definition/use |
|---|---|---|
| Evidence | Universe coverage | Current cards / frozen `eligibleCount`; target 100% before new-buy activation, plus separate managed-off-roster coverage |
| Evidence | Material-claim support | Material claims whose cited spans exist **and entail/support the claim** / all material claims; executable target 100%, with contradiction/source-independence sampling |
| Evidence | Freshness | Age by required source lane, not one average |
| Evidence | Conflict rate | Unresolved contradictory facts / material facts |
| Extraction | Numeric reconciliation | Extracted financial values matching deterministic filing/XBRL calculation within tolerance |
| Extraction | High-impact recall/support | Expert-labeled high-impact facts retrieved and correctly source-supported; sampled raw documents expose omissions |
| Research | Reuse efficiency | Decisions using valid prior memo + delta / eligible revisions; paired with stale/missed-change rate so reuse cannot hide decay |
| Manager | Distribution score | CRPS and quantile loss for the forecast return distribution; log/Brier only for separately declared, mutually exclusive events with frozen resolution rules |
| Manager | Calibration | Observed frequency versus probability for those predeclared events/intervals; generic prose `confidence` is not scored as a probability |
| Manager | Forecast error | Quantile/median predicted minus realized return/price by declared horizon |
| Manager | Same-evidence consistency | Action stability under repeated frozen-context evals; disagreement is inspected, not hidden |
| Manager | Mandate validity/clamp rate | First-pass schema/semantic validity and fraction of proposals risk-clamped/rejected; original and envelope both retained |
| Selection | Matched selection lift | Later selected return minus point-in-time tradable controls matched on sector, beta, size, liquidity and volatility, plus factor/benchmark-adjusted result |
| Selection | Missed-opportunity rate | Tradable/risk-eligible unselected names exceeding a predeclared adjusted-return threshold, reviewed with the contemporaneous evidence context |
| Selection | Abstention quality | Error avoided by abstention versus opportunities unnecessarily missed |
| Portfolio | Investment return | Time-weighted portfolio return net of commissions, spread and slippage but before owner-paid AI/data/platform expenses |
| Portfolio | Owner economic return | Investment P&L less AI, data, platform and other operating expenses; report currency and NAV basis |
| Portfolio | Excess return | Net portfolio return minus predeclared SPY and equal-weight-universe benchmarks |
| Portfolio | Sharpe/Sortino/Calmar | Risk-adjusted returns; report windows and small-sample limits |
| Portfolio | Maximum drawdown/CVaR | Peak-to-trough loss and average loss beyond the 95th percentile |
| Portfolio | Expectancy/payoff/profit factor | Supporting diagnostics; never sole optimization target |
| Portfolio | Turnover/exposure/concentration | Cost and risk footprint of the manager |
| Execution | Trigger capture | Authorized target/stop touches detected / reconstructable touches, and eligible fills captured / reconstructable eligible fills, reported separately; a touch is not a fill |
| Execution | Slippage | Fill versus executable benchmark and versus authorized level |
| Execution | Protection latency | Entry fill to acknowledged correctly sized protective legs; P50/P95/max and any unprotected interval |
| Execution | Integrity | Duplicate, orphan, rejected, stale, over-quantity, or unprotected orders; target zero |
| Operations | Completion/latency | Manager run before market cutoff, event-review latency, executor reconciliation latency |
| Cost | Unit economics | Report cost per Manager Meeting, roster name covered, focused-research job, actionable mandate, activated mandate and fill-producing mandate separately; no single denominator is the headline |
| Cost | Operating cost drag | `(AI + data + platform cost) / average NAV × 10,000` basis points for the period |
| Cost | Cost per incremental basis point | AI + data cost / validated incremental net return versus control; only meaningful over adequate samples |
| Governance | Unexplained flip rate | Decision changes without new evidence or declared price/portfolio change |
| Governance | Regression recurrence | Previously identified error classes that recur after a released fix |

### 16.4 Portfolio formulas

At minimum, calculate and version:

\[
\text{Sharpe} = \sqrt{252}\frac{\overline{r_p-r_f}}{s(r_p-r_f)}, \quad
\text{Sortino} = \sqrt{252}\frac{\bar r_p-MAR}{\sqrt{\operatorname{mean}(\min(r_p-MAR,0)^2)}}
\]

\[
\text{Max Drawdown} = \min_t\left(\frac{NAV_t}{\max_{u\le t}NAV_u}-1\right), \quad
\text{Calmar} = \frac{\text{annualized return}}{|\text{Max Drawdown}|}
\]

Declare `MAR`, return sampling frequency, cash-flow treatment, benchmark, annualization, and missing-day policy. Report raw Sharpe and Deflated Sharpe together. The Deflated Sharpe adjusts for selection bias/multiple testing and non-normal returns, but cannot repair data leakage, omitted costs, or bad fills ([Bailey and López de Prado](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)). Track every model/prompt/policy trial so probability-of-backtest-overfitting analysis is not calculated only on winners ([Bailey et al.](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253)). These research statistics are offline evaluation outputs, not daily trade gates.

Daily rows across 304 repeated symbols and overlapping 20/60-day outcomes are not independent samples. Predeclare inference before looking at results: cluster by decision date and symbol, use a block bootstrap or HAC estimator consistent with the horizon/holding overlap, report effective sample size and confidence intervals, and use total-return benchmarks including cash yield and comparable benchmark trading costs. Sixty sessions is an operational soak only. Performance/noninferiority and downside-safety thresholds, power assumptions, missing/unscorable treatment, and regime coverage must be fixed before promotion.

### 16.5 Incremental AI value experiment

Run prospective self-financing books from the same timestamp, point-in-time universe, capital, constraints, and execution assumptions:

1. AI Fund Manager.
2. **[v2] Frozen exploratory control (arm 2a).** The v18 exploratory configuration as actually run, at its actual `riskConfigHash`, with its deviations from the declared strategy attached to every row, and with exits recomputed under the touch convention of §8.5. This is the honest comparator and the only arm with a track record. *"Frozen v18 legacy control" was ambiguous: per §2 the strict configuration has never traded, so the label named two different things.*
2b. **[v2] Declared strict configuration, shadow only (arm 2b).** The pre-registered configuration with `requireCalibratedEdge` observed but not enforced, run in shadow so the strategy the repository actually declares finally generates a record. Labelled *shadow, non-eligible*; its results are never presented as the strict strategy's live performance.
3. Simple declared benchmark/equal-weight control.
4. Matched random portfolios and transparent factor baselines.

Also sample unselected companies randomly each day for detailed outcome review. Preserve removed/delisted names and do not impute future membership. Historical model replay remains secondary because a modern model may know later outcomes from training; prospective decisions are the primary evidence. This design detects whether Sol merely writes persuasive stories around returns that beta, factors, or random selection would have achieved.

## 17. Testing and verification

OpenAI recommends task-specific evals, production-representative data, full logging, automated scoring where possible, and human calibration rather than “vibe-based” evaluation ([evaluation best practices](https://developers.openai.com/api/docs/guides/evaluation-best-practices)). Apply the same discipline to code and model behavior.

### 17.1 Retain/port current safety fixtures

Preserve tests for integer-balanced ledger accounting; strictly post-decision execution clocks; exchange calendar/holiday/close handling; consolidated-feed requirements; volatility/tail sizing; future-document rejection; visible-text extraction; split/dividend quarantine; chunk retries/missing-symbol reporting; dispatch CAS; leases/heartbeats; stale proposal rejection; pending exposure; order lifecycle/reconciliation; and NAV finalization.

Demote the residual/five-session/variant/Thompson/patience/strike/paper-relaxation/shadow-ladder tests to a clearly labeled legacy replay suite. Do not silently delete them: the v18 control must remain reproducible.

### 17.2 New blocking unit/contract suites

**[v2] These are `fixture()` cases added to the existing `_investorSelftest.js`, not new test files.** They run on the deployed commit and, through `fixturesPass`/`fixturesCommit`, halt trading when they fail (§17.5). Each keeps the file's existing shape: `cases.push(fixture("name", () => { ... return true }))`.

| Suite | Blocking cases |
|---|---|
| Authority boundary | No deterministic module originates a **normal investment** BUY/REDUCE/SELL, improves priority, increases size, or replaces a symbol; operator/emergency orders require their exact authority label, policy/actor hash and bounds; emergency code can only reduce/flatten or buy-to-cover an accidental short to zero; Sol cannot bypass hard risk |
| Storage/codec | Firestore byte limit, content-manifest hash/readback, nested-array round trip, one decision per doc, 304-row run size, corrupt/missing object, migration schema versions |
| **[v2] Fail-closed evidence** | Each of the eleven failure paths in `_investorOpenai.js` yields `trade === false` with `paperAbstainOnMissingInfo: true` set; the `default:` branch of `directionFromCause` returns `trade:false` so any cause added later fails closed *(D-1, D-8b; I-1)* |
| **[v2] Touch/fill** | Target-touch, stop-touch and same-bar-collision fixtures; adverse resolution on collision, `UNSCORABLE` where unresolvable; regression fixture reproducing the pre-fix close-only behaviour *(D-2; I-2)* |
| **[v2] Calibration state** | Strict configuration returns `calibration_unavailable` carrying finite `sessionsAvailable`/`sessionsRequired`/`earliestEligibleSession`; the dashboard payload carries it *(D-3; I-4)* |
| **[v2] Configuration provenance** | Every result row carries `riskConfigHash`; `deviationsFromDeclared` names `costMarginMultiple`, `minAdvUsd` and the five breakers under the current exploratory configuration *(D-4; I-4)* |
| **[v2] Variant parity** | Every registered variant declares a complete exit parameter set, or carries an explicit retired-regime tag; mixed regimes cannot be promoted *(D-5; I-4)* |
| **[v2] Source scope** | A two-company sweep issues exactly one request per global source; the sweep-budget assertion fires when the request count exceeds `(globalSources × 1) + (companySources × companiesInSweep)` *(D-6; I-3)* |
| **[v2] UI declarations** | Static check over the `investor.html` source text: no duplicate top-level function declaration, and the `PLAN_BLOCK_PLAIN` dictionary is reachable from its call site. Source-text assertions in the existing file's style — no browser, no Playwright *(D-7)* |
| **[v2] Scheduler windows** | `premarket_manager` dispatchable at 08:30 ET; `scan` not dispatchable at 08:30 ET; the `scan` window is bit-identical to current behaviour *(D-8)* |
| **[v2] Secrets** | No PEM header, no `PRIVATE KEY` marker and no high-entropy credential reachable in the bundle. Runs in the same harness, so a reintroduced credential halts trading rather than merely warning *(D-9)* |
| Fundamentals/dossier/as-of | XBRL taxonomy/unit/context/amendment/period/share/split reconciliation; source versions; publication-or-first-seen cutoff; no future evidence; unchanged hash avoids repeat extraction; fact/opinion separation |
| Complete coverage | Dynamic roster hash/count equality; duplicates/unknown/missing names; block resume/synthesis; order rotation/sentinels; held/pending off-roster names never lost; stale/delisted/unpriced treatment |
| Manager schema | Separate review directive/final decision, every action/workflow variant, final portfolio comparison, unique ranks, refusal/truncation/repair merge, invalid enum, missing source, probability/fill/no-fill errors, missing bear case/invalidator, context overflow |
| Claim support | Span/document identity; independent Luna supported/contradicted/insufficient verdict; generation cannot self-attest; immutable hash checked at activation; human sample agreement and budget attribution |
| Mandate/plan semantics | Proposal vs server binding vs envelope, stable lineage, canonical BigInt price/money/quantity/rate, derived forecast arithmetic, fresh activation snapshot, all-or-none basket reservation/CAS, material clamp resynthesis, no INCREASE, desired/applied supersession/revocation |
| Event routing | Deduplicated revision; routine delta inclusion; unfilled KEEP/REVISE/REVOKE; partial remainder + held protection; held and nonholding paths; full research followed by synthesis; unaffected protection retained; false-negative sample |
| OHLC execution | Exact equality, target/stop touches, marketable open, queue/volume, gap-through stop versus stop-limit nonfill, entry+exit and target+stop same-bar ambiguity, partials, stale/duplicate/out-of-order bars, session policy |
| Broker lifecycle | Capability rejection, bracket partial-fill gap, fixed versus trailing incompatibility, GTC protection across close/holiday/restart, session coverage, cancel-remainder/attach OCO, sibling quantities, double-fill neutralization, atomic/non-atomic replace downgrade, submit/cancel/reject/crash/reconcile, reservation release only after terminal broker truth |
| Calendar | Authoritative version ingestion, DST, holidays/half days/unscheduled closures/corrections, missing/disputed session fails new exposure closed |
| Failure behavior | Model/source/budget/Firestore/market/broker outage; no legacy fallback; old valid protection survives; visible degraded state |
| Cost/cache | Reservation races, token attribution, no double billing, cache invalidation on any relevant hash, ceiling behavior |
| Journal/KPI | Frozen context replay, later-only outcomes, MFE/MAE, benchmark alignment, manual override separation, no look-ahead |
| API/UI | Exact v2 schemas/auth/version conflicts, redaction, payload bounds, pagination, partial/out-of-order data, all critical states/actions visible, no old action called, safe rationale/citation rendering, accessibility |

### 17.3 Model eval set

Build a versioned benchmark from:

- known repository failures and disputed decisions;
- expert-authored filing questions across extraction, comparisons, ratios, risks, and analyst insight;
- bullish, bearish, unchanged, conflicting, stale, and insufficient-evidence cases;
- adversarial documents containing prompt-injection text;
- corporate actions, restatements, ticker changes, ambiguous entities, and duplicated news;
- portfolio conflicts where the individually attractive name is inferior after concentration/opportunity cost;
- repeated frozen inputs to measure stochastic action stability;
- roster-order rotations, block boundaries and sentinel companies to measure long-context positional bias;
- counterfactual perturbations where one material fact changes and the decision should respond.

Human-scored gold answers should judge evidence use, reasoning, uncertainty, valuation assumptions, mandate coherence, and action—not whether the stock happened to rise.

### 17.4 Runtime hard gates

Before activating any new buy mandate, require:

- `completedCount === eligibleCount` and exact symbol/hash equality for that frozen manager run;
- every held/pending off-roster symbol has current protection or an emergency plan;
- zero future-dated evidence;
- zero unsupported material mandate claims;
- all critical source/freshness attestations;
- valid schema and semantic checks;
- reconciled paper account/ledger;
- full worst-case cash/risk reservation and a unique AI capital rank;
- no current kill/freeze state;
- active protection for every existing position;
- desired/applied mandate and broker state either agree or are in a safe, visible reconciliation transition;
- exact prompt/model/schema/policy/context hashes.

An invalid result may receive one bounded schema/coverage repair. The system then abstains; it does not coerce malformed text into an order.

**[v2] Correction to the build attestation.** `controlAllowsEntry` (`_investorLedger.js:481`) unlocks all trading on `fixturesPass && fixturesCommit === commit` (`:499`). `_investorSelftest.js` contains zero references to `_investorOpenai.js` or `OPENAI` — no runtime fixture exercises the model path at all. That is tolerable today, when the model is one enum among thirteen blocking gates; it is not tolerable once the model is the investment authority. The **fail-closed evidence** and **touch/fill** suites of §17.2 are added to the attestation set before Phase 3.

### 17.5 How verification actually runs **[v2]**

**[v2] v1 specified `npm ci`, a lockfile, `node --test`, a pinned `firebase-tools` emulator, and a Playwright Chromium runtime driven by a GitHub Actions workflow. That is removed.** This application has no local npm workflow; npm runs only on Netlify's build box to install the packages `external_node_modules` keeps unbundled. Adding a parallel CI toolchain would duplicate a harness the repository already has, and would be weaker than it.

Verification runs where it already runs:

- `_investorSelftest.js` registers checks through `fixture(name, fn)` and executes them on the deployed commit.
- `_investorBootstrap.js` writes `fixturesPass` and `fixturesCommit` from that run.
- `_investorLedger.js:499` refuses entry unless `fixturesPass === true` **and** `fixturesCommit === commit`, and `investorApi.js:53` enforces the same.

So a failing check does not merely turn a pipeline red — **it halts trading on the current build.** That property is worth more than a CI badge and this plan preserves it rather than competing with it.

The nine suites of §17.2 are therefore nine additional `fixture()` cases in that file, and the two named in §17.4 join the set `controlAllowsEntry` already reads. No new file, no framework, no lockfile, no workflow.

**Index rollout** remains an explicit pre-cutover operation, performed by whatever mechanism the operator already uses to manage Firestore indexes. No writer-epoch flip occurs while an index is building or missing. This plan does not add `firebase.json` or a `.firebaserc`.

## 18. Security, governance, and operational risk

### Immediate credential stop condition

The repository tracks `netlify/functions/secrets/gcpPrivateKey.txt` as a mode-644, 1,734-byte private-key file. Its contents were not displayed in this review. Treat the credential as compromised now: identify the associated principal, revoke/rotate it, inventory deployments and relevant access logs, remove the file and purge Git history where appropriate, replace it with a least-privilege managed secret, and add pre-commit/CI secret scanning. No live-broker work should begin until this is closed and current Firestore/storage access is retested.

| Risk | Required control |
|---|---|
| Prompt injection in filings/news | Treat source text as inert quoted data; strip active markup; no broker tool in model context; server-side allowlist tool names/symbols/URLs/arguments, call/time/token/byte ceilings, result hashes, redirect/SSRF controls, and adversarial tests |
| Source poisoning/spoofing | Allowlisted adapters, DNS/redirect/SSRF protections, domain/entity resolution, hashes, source class/independence, corroboration |
| Hallucinated citation | Document/version/claim IDs must exist; exact span plus entailment/contradiction/source-independence checks; sampled human review; unsupported material claim blocks activation |
| Temporal leakage | Publication/retrieval/as-of timestamps; point-in-time queries; future evidence test; immutable context manifest |
| Model drift | Pin model snapshot where available; record returned model; shadow-evaluate changes; explicit promotion/rollback |
| Reward hacking/overtrading | Mission and KPIs emphasize after-cost risk-adjusted return; cap turnover/exposure; never optimize win rate/trade count alone |
| Research anchoring | Delta updates plus isolated clean-room re-underwrite after material events/staleness; sampled non-holdings hide prior disposition; no widening loser risk without full review |
| Backtest overfit | Count all trials, walk-forward/out-of-time tests, PBO/Deflated Sharpe, prospective shadow as primary evidence |
| Secret compromise | Separate operator/session, OpenAI, data, and future broker credentials with least privilege/KMS; model never receives secrets |
| Unauthorized order | Active mandate hash + validator signature + idempotency key; executor-only broker access |
| Broker/data outage | Working broker protection, fail closed on new exposure, reconcile, persistent banner, no AI workaround |
| Runaway spend | Per-role reservations/ceilings, concurrency caps, cost attribution, visible degradation policy |
| Untrusted UI output | No raw model HTML/Markdown injection; escaped structured renderer, validated HTTPS citations, CSP, `noopener noreferrer`, no secrets/authority in URL or local storage |

The current `_investorAuth.js` correctly describes a broad shared-service-account trust boundary. That is not adequate for real money. Create separate least-privilege identities and secrets before a live adapter is enabled.

FINRA's AI guidance is directly relevant as engineering practice even if this remains a personal paper-trading application: autonomy, auditability, domain knowledge, hallucination, privacy, and reward design remain real failure modes ([FINRA Regulatory Notice 24-09](https://www.finra.org/rules-guidance/notices/24-09)). “AI Fund Manager” is product language, not a legal status. Determine the actual owner jurisdiction, broker/account type, tax treatment, market-data rights, automated-order rules, advisory/recordkeeping obligations, and terminology with qualified counsel before live use or use for anyone else. If the owner/account is Canadian, explicitly review CIRO/CSA and the Canadian broker's automation terms rather than assuming US FINRA/SEC material is sufficient.

## 19. Phased implementation and cutover

This must be additive and reversible. A big-bang rewrite would recreate the same costly “fix the visible gate, discover the next gate” cycle documented in v18.

### Phase −1 — Plumbing remediation **[v2]** (blocking; precedes Phase 0)

Nine defects (§2A). Six are in the plumbing rather than the strategy and survive the replacement of the decider — built on top of them, the manager's mandates are evaluated against prices that never existed, every model failure becomes a purchase, the evidence sweep stays ~300× over-subscribed, the pre-market meeting cannot be dispatched, and the coverage run silently exhausts its budget. The AI would then be scored on outcomes it did not produce. This is not cleanup; it is the precondition for the measurement §16 depends on.

Each item is independently deployable and independently testable, and none requires an AI decision. **Exit:** all nine suites of §17.2 green in CI, and thirty consecutive sessions recorded with no regression.

**P−1.1 — Fail-closed evidence classification (D-1, D-8b).** `_investorOpenai.js` collapses eleven distinct outcomes into `evidence_pending`: missing API key, incomplete coverage, lease contention, exhausted budget, unreachable model, HTTP error, unexpected tool call, unparseable output, schema-invalid cause, model abstention, and failed citation verification. `_investorSignal.js:640-649` reads that token as *absence of information* and — where `paperAbstainOnMissingInfo` is true, as it is in the exploratory lane — returns `{trade:true, side:"long", confidence:0.2, relaxed:true}`. A rejected model output, an unreachable model, and an exhausted budget all become purchases. Split the token into three causes:

| Cause | Meaning | Permits new risk? |
|---|---|---|
| `evidence_not_yet_gathered` | The sweep has not reached this name. Absence of information. | Yes, reduced size, only where `paperAbstainOnMissingInfo === true` |
| `evidence_insufficient` | The model was reached, returned validly, and reported it cannot determine cause from the supplied sources. A finding of insufficiency. | Yes, reduced size, only where `paperAbstainOnMissingInfo === true` |
| `evidence_unavailable` | System failure or rejected output. **Not a finding.** | **Never**, in any cohort, at any size |

`_investorOpenai.js:309,312,325,331,372,379,395,404,435,442,447` map to `evidence_unavailable`; only the model's own returned enum maps to `evidence_insufficient`. `directionFromCause` gains an explicit `EVIDENCE_UNAVAILABLE` case returning `{trade:false}`, and its `default:` branch is changed to return `{trade:false}` unconditionally so any cause added later fails closed rather than open.

**P−1.2 — Touch-based exit evaluation (D-2).** Bars carry `{o,h,l,c}` (`_investorMarket.js:751`) and are integrity-checked against their own high/low (`:959-961`), but the exit path reads only `c`. Three parts: (a) `_investorExitPolicy.js` `evaluateExit` accepts `{mark, barHigh, barLow, entry, peak}` — stops and trailing stops test against `barLow`, profit and pullback targets against `barHigh`, and `mark` is retained only for reporting `pnlPct`; (b) the collision rule of §8.5 applies — adverse path, or `UNSCORABLE` where the position's status materially depends on an unresolvable sequence; (c) `_investorStrike.js:443` passes `{low: last.l, high: last.h, close: last.c}`, with arm levels struck on `low <= armBelowUsd` and `gap_below_band` determined from `low`. The durable production answer remains §1 item 8 and §8.4: lodge authorized protective and target orders with the broker so its engine watches continuously, leaving polling as the reconciliation path rather than the trigger path. P−1.2 makes polling honest in the interim and paper simulation faithful permanently.

**P−1.3 — Declare the calibration state (D-3).** No behavioural change: the gate is correct and stays. Disclosure only — (a) `_investorSignal.js` returns a structured `calibration_unavailable` carrying `{sessionsAvailable, sessionsRequired, earliestEligibleSession}`; (b) the console surfaces it as a first-class state rather than a silent no-op; (c) `StrategyVersions` records for every version whether it was ever order-eligible, and any performance claim about a version that never was is rejected at write time.

**P−1.4 — Configuration parity and provenance (D-4).** No parameter changes — the operator keeps whatever floor they choose. What changes is that the floor joins the record: (a) every closed position, NAV row and KPI row carries a `riskConfigHash` over the effective parameter set in force at the decision, plus a `deviationsFromDeclared` array naming each parameter differing from the frozen strategy's declared value, with both values; (b) a persistent banner whenever any breaker is at a disabling value or any hurdle is below its declared value; (c) any comparison across two periods with different `riskConfigHash` is labelled non-comparable rather than charted as one series.

**P−1.5 — Variant parity (D-5).** The 15 variants at `_investorVariants.js:31-152` predate v18 and declare none of `takeProfitPct`, `trailingArmsAtPct`, `trailingStopPct`, `holdLosersThroughRankExit`, `requireSessionMove`. Either (a) mint a `v19-*` generation inheriting the v18 exit parameters and retire the 15 with `retiredReason: "pre-v18 exit regime"`, preserving prior results as belonging to the old regime — recommended; or (b) keep them and add an explicit `exitRegime: "pre-v18"` field, blocking promotion of a pre-v18 variant into a post-v18 configuration. Either way `_investorVariants.js` gains a validator rejecting a variant whose exit parameters are undeclared, so regimes can no longer mix silently.

**P−1.6 — Source scope correction (D-6).** (a) Add `scope: "global" | "company"` to every source descriptor — all ten `kind:"feed"` sources are `global`, and `nws.alerts` is classified during implementation since it is a geography-filtered endpoint; GDELT queries, `company.direct`, `federal.register` and `usaspending` are `company`. (b) `readState` keys global sources by `sourceId` alone. (c) A global source is fetched at most once per sweep and **entity resolution fans its items out to companies** rather than the fetch being repeated. (d) A sweep issuing more than `(globalSources × 1) + (companySources × companiesInSweep)` requests fails and raises a typed alert rather than silently continuing. For a 304-name pass the global-feed request count falls from ~3,040 to ~10. This is the precondition for the bounded ingest queue of §11.1 and the evidence deltas of §5.5.

**P−1.7 — Remove the duplicate UI declaration (D-7).** `plainBlock` at `investor.html:3710` (plan-block reasons, backed by `PLAN_BLOCK_PLAIN`) and again at `:3744` (sell-state reasons). Hoisting keeps the second, so `PLAN_BLOCK_PLAIN` is dead and plan-block messages render from the wrong dictionary. Rename to `plainPlanBlock` and `plainSellBlock`, update call sites, and merge the dictionaries only if the merge is verified non-lossy. Superseded by §14 if and when the UI is rewritten; fixed now because it is live today.

**P−1.8 — Scheduler window for pre-market work (D-8).** Per the task-class window table in §11.1. The existing constant is retained as the `scan` window so current behaviour is bit-identical.

**P−1.9 — Credential remediation (D-9).** In this order: (1) identify the principal and **rotate/revoke at the provider**, treating the key as compromised from first commit rather than from discovery; (2) inventory deployments and review access logs for the key's lifetime; (3) `git rm --cached` and add a `.gitignore` covering `netlify/functions/secrets/`, `*.pem`, `*_rsa`, `*PrivateKey*`; (4) purge history where the repository's sharing model warrants it — noting that rotation, not purging, is the control that matters, and purging without rotating is theatre; (5) replace with a managed secret injected at runtime — no source file references `gcpPrivateKey`, verified by grep, so removal is behaviourally inert; (6) add the secret-scanning suite of §17.2 to CI. **Ship alone, first, before any other change in Phase −1.**

### Phase 0 — Freeze and baseline

**Files:** `netlify/functions/secrets/gcpPrivateKey.txt`, auth/config, `package.json` + new lockfile/CI, legacy strategy/universe modules, ledger, API, selftest.  
**Work:** rotate/remove the exposed key and add secret scanning first; tag commit `ea14b710...` as the legacy baseline; lock the supported dependency/runtime toolchain; export current control/positions/orders/ledger/strategy/universe hashes; document actual v18/v6 versus stale JSON; add `engineVersion` projection; freeze new legacy entries during migration drills; decide the point-in-time news/estimate/transcript launch source or explicitly accept missing coverage.  
**Exit:** credential incident closed; clean CI install is reproducible; legacy replay and account conservation reproduce; every open position/order is inventoried; data entitlements/known gaps are approved.

### Phase 1 — Contracts, policy, indexes, and authority tests

**[v2] Files:** `_investorAdmin.js`, `_investorStorageCodec.js`, `_investorMoney.js`, `_investorContentStore.js`, `_investorPolicy.js`, `_investorJobs.js`, `_investorApiSchemas.js`, `_investorBootstrap.js`, `_investorDecisionManifest.js`, `_investorSelftest.js`, `investorKick.js`, root `netlify.toml`. Policy and schemas are inline in the modules that own them (§12.1). Firestore indexes are deployed by the operator's existing mechanism.  
**Work:** define/compile discriminated schemas/states/hashes, canonical BigInt values, storage codecs/size boundaries, normalized collections/indexes, fixed role-model mapping, lineage/desired-applied/outbox contracts, atomically consumed worker nonces, execution-first dispatcher skeleton, and authority tests.  
**Exit:** schema/round-trip/size/emulator fixtures pass; malformed/unsupported/future/stale mandates, concurrent versions, replayed nonce and unsafe transitions are rejected; required test-environment indexes work and production rollout procedure can prove READY.

### Phase 2 — Evidence and complete-roster dossier plane

**Files:** `_investorOpenai.js` Luna boundary, `_investorClaimVerifier.js`, `_investorEvidence.js`, `_investorFetch.js`, `_investorFundamentals.js`, `_investorDataProviders.js`, `_investorIntelligenceSources.js`, `_investorIntelligence.js`, `_investorHistory.js`, `_investorTemporal.js`, `_investorWorkset.js`, `_investorDossier.js`, `_investorEventRouter.js`, `investorIngest-background.js`, `scripts/investor/bootstrap-sec.js`.  
**Work:** publish transparent canonical eligible/excluded snapshots; fetch global feeds once and fan out; stream SEC bulk content to object storage; ingest authoritative calendar and XBRL/benchmark/cash-rate/corporate-action facts; build cards/deltas; run Luna extraction and independent claim-verification recall/support checks with direct excerpts.  
**Exit:** every frozen eligible and managed off-roster symbol has a reproducible card or explicit `INCOMPLETE`; no symbol starves; facts/manifests replay point-in-time; large content never exceeds Firestore limits.

### Phase 3 — Sol Manager in observation mode

**Files:** `_investorOpenai.js` Sol roles, `_investorManager.js`, `_investorResearch.js`, `_investorResearchTools.js`, `_investorValuation.js`, `_investorPortfolio.js`, `_investorPortfolioRisk.js`, `_investorApiV2.js`, `_investorAuth.js`, `investorSession.js`, `_investorAlerts.js`, `investorManager-background.js`, ManagerRun/ManagerDecision persistence and read-only v2 APIs.  
**Work:** dynamic full-roster Manager Meeting, managed-position revision, async response recovery, fully bounded read-only research tools, focused research, deterministic portfolio scenarios, final Sol selection, separate directive/decision, repair merge, claim verification and exact cost accounting. Session-authenticated v2 reads expose every decision; mutations/order output remain disabled.  
**Exit:** ten or more complete shadow sessions can actually be captured and compared; every frozen-roster disposition is visible; positional-bias tests and costs measured; model eval baseline approved.

### Phase 4 — Mandate and paper executor

**Files:** `_investorMandate.js`, `_investorExecution.js`, `_investorBroker.js`, `_investorRisk.js`, `_investorEmergencyRisk.js`, `_investorLedger.js`, `_investorExitPolicy.js`, `_investorPositionGuard.js`, `_investorCorporateActions.js`, `_investorState.js`, `_investorAlerts.js`, `_investorSoak.js`, `investorExecution-background.js`, v2 mutation handlers.  
**Work:** fresh activation snapshots and atomic portfolio-plan/reservation CAS; desired/applied outbox saga; PaperBroker capability matrix; multi-fill ledger; exact limit/stop/OCO/session lifecycle; touch-versus-fill simulation; partial protection; safe/blocked replacement; oversell recovery; emergency-risk authority; typed controls; manual-sell/corporate-action races and reconciliation.  
**Exit:** zero duplicates/orphans/double-sells/unprotected positions in the blocking soak; zero missed **scorable** target-stop touches, with data gaps explicitly degraded/unscorable.

### Phase 5 — Learning and prospective controls

**Files:** `_investorLearning.js`, `_investorKpi.js`, `_investorEvals.js`, rewritten DecisionFeedback/Shadow/Soak, retired ResearchStats authority, Decisions & Learning API contracts.  
**Work:** forecasts with resolvable distributions, matched counterfactuals/random books/factor controls, anti-anchoring samples, postmortems, KPI definitions, challenger runs, v18/benchmark parallel books.  
**Exit:** outcome clock proves no look-ahead; all trials counted; model/prompt comparisons and owner/trading economics are reproducible.

### Phase 6 — UI/API cutover

**[v2] Files:** `investor.html` (single self-contained console — styles, markup and script inline, as today), `investorApi.js` v1/v2 registry, static headers in `netlify.toml`, and UI `fixture()` cases in `_investorSelftest.js`.  
**Work:** validate wireframes; build five-route progressive UI and exact component/state/icon/action mapping, canonical numeric formatting, safe AI text, pagination/auth/accessibility. Use a Netlify Deploy Preview of the rewritten `investor.html`: preview receives v2 reads while all v2 mutations are rejected and production stays v1. Cutover deploys/smoke-tests v2 first, then atomically flips `writerEpoch` so only one engine writes. Rollback freezes buys, restores the prior deploy/read routing, and never enables two writers.  
**Exit:** no old investment-control action is called; every active mandate traces from UI to evidence and broker state; every frozen-roster row has a reason; the four above-the-fold questions are answerable without opening System.

### Phase 7 — Paper AI operation

**Work:** freeze legacy entry writer; cancel/drain old proposals and inert EntryPlans; full Sol review for every open holding; apply complete new protection through the acknowledged desired/applied saga; enable `PAPER_AI`; run prospective sessions across ordinary and volatile conditions.  
**Exit:** safety gates remain perfect and the AI book has enough prospective evidence to decide the next experiment. At least 60 trading sessions is a reasonable **operational-soak** starting point, not proof of durable alpha. Performance promotion requires predeclared power/confidence criteria, sufficient independent decisions/forecast resolutions, and regime coverage rather than a calendar count alone.

### Phase 8 — Optional limited live operation

This phase is **out of scope for automatic execution of this plan**. It requires explicit user authorization, broker adapter implementation, legal/account review, separate secrets, cancel/replace/poll/reconnect tests (plus stream/webhook tests only if that adapter is separately chosen), and a predeclared capital-loss envelope. Start with a small fixed fraction of capital; promotion raises exposure, not model authority. Roll back to freeze-new-buys while preserving protective orders.

### Cutover sequence for open positions

1. Engage `freezeBuys`; leave exits operational.
2. Drain/reject legacy proposed orders and cancel old EntryPlans.
3. Reconcile ledger, positions, cash, and corporate actions.
4. Have Sol fully underwrite any holding without a current source-complete memo.
5. Generate one complete mandate per holding; invalid/incomplete holdings receive an explicit temporary emergency-only protection state and human review—not a fabricated AI thesis.
6. Commit desired plan pointers, run the broker-application saga, and confirm applied versions/protective legs before retiring old protection.
7. Disable legacy writers, enable new executor, and run reconciliation.
8. Switch UI/API reads; retain legacy data read-only.
9. Keep a one-action rollback that freezes buys and returns control to emergency protection, never to automatic residual buying.

## 20. Definition of done

The overhaul is not complete when Sol produces persuasive prose. It is complete only when all of these are true:

### Architecture

- Sol is the only component making investment selections, comparisons, and mandate terms.
- Emergency de-risking is a separately owner-approved, hashed, narrowly bounded authority and is never attributed to Sol.
- Every member of the frozen eligible snapshot—304 in audited v6—is explicitly covered in each completed pre-market run, and every held/pending off-roster symbol remains managed.
- No deterministic signal/rank/gate or cheap-model result can hide a company from Sol.
- No AI is called when an already-authorized price target, stop, or entry is touched.

### Evidence and decisions

- Every factual premise of an actionable mandate resolves to point-in-time source evidence and an independent persisted verification verdict; inferences remain labeled as such.
- Existing research is reused; daily work is delta-based; full reviews have explicit reasons.
- Every decision includes opposing evidence, uncertainty, valuation assumptions, horizon, invalidators, and portfolio comparison.
- ABSTAIN is a first-class safe result and never silently maps to HOLD or BUY.

### Execution and safety

- AI proposals, server bindings, envelopes and active mandates are separately immutable/versioned; risk-maintenance and expansion plan classes use fresh activation snapshots and all-or-none CAS, while external application uses an acknowledged saga.
- Normal risk only rejects or reduces an envelope; execution follows AI/operator authority. Emergency code acts only under an exact owner-approved policy and cannot open positive exposure.
- Broker capability checks make paper/live semantics match the UI, including RTH versus overnight coverage; unsupported combinations cannot activate.
- Intrabar touches versus fills, gaps, entry/exit collisions, OCO sibling/double-fill behavior, partial protection, restarts, calendars and corporate actions pass.
- Duplicate/orphan/over-quantity orders, unsupported mandates, future evidence, missed reconstructable **scorable** triggers, and `UNPROTECTED` intervals inside required coverage are zero in acceptance testing. Declared overnight gap exposure and data gaps remain visible and are never scored as success.

### UI

- Five validated primary routes, no Advanced drawer, no residual/gate/variant control panels; Manager uses progressive disclosure and answers the four primary operator questions above the fold.
- The operator can answer: what changed, what Sol decided, why, what price/size/duration it authorized, what could invalidate it, what is executing, and how it performed.
- Every critical error remains visible and actionable.
- Icons, words, color, keyboard behavior, screen-reader labels, and mobile layout are consistent.

### Learning and economics

- Every manager decision and unselected comparison is outcome-tracked without look-ahead.
- v18, benchmark, and AI books are evaluated with identical point-in-time data and costs.
- Actual daily model/tool/data cost is visible by task; deterministic execution shows $0 AI.
- No model/prompt/policy change reaches production without task-specific evals and prospective shadow evidence.
- The AI strategy demonstrates incremental **after-cost, risk-adjusted** value before live capital is considered.

## 21. Recommended implementation order by pull request

1. `security: rotate tracked GCP key and add secret scanning`
2. `investor: freeze legacy v18/v6 and lock reproducible dependencies`
3. `investor: add fund-manager policy and discriminated schemas`
4. `investor: add normalized storage codecs, collections, indexes, and content manifests`
5. `investor: ingest point-in-time SEC facts and transparent universe snapshots`
6. `investor: build sector-aware dossiers and complete managed workset`
7. `investor: rewrite OpenAI gateway for Luna facts and async Sol manager`
8. `investor: add full-roster meeting, holding revision, research, and final synthesis`
9. `investor: add mandate lineage, risk envelopes, reservations, and desired-state outbox`
10. `investor: rewrite ledger and OHLC PaperBroker order-set reconciliation`
11. `investor: add forecasts, matched counterfactuals, KPI and eval suite`
12. `investor: add exact v2 API and five-route UI`
13. `investor: dual-run legacy control and AI paper manager`
14. `investor: remove legacy production writers after cutover evidence`
15. `investor: optional live-broker RFC—no activation in this series`

Each pull request must be independently deployable behind a feature flag, include migrations and rollback, and keep the paper ledger balanced. Do not mix UI deletion, decision-authority migration, and live execution in one release.

## 22. Research basis and unresolved questions

### Evidence used for this design

- The [SEC EDGAR API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces) supports primary, real-time submissions/XBRL and efficient nightly bulk backfill.
- Retrieval grounding can improve factual specificity, but generic RAG evidence does not prove financial correctness ([Lewis et al., RAG](https://proceedings.neurips.cc/paper/2020/hash/6b493230205f780e1bc26945df7481e5-Abstract.html)).
- FinanceBench and SECQUE support separate testing of retrieval, calculation, comparison, risk, and analyst insight rather than treating “financial intelligence” as one score ([FinanceBench](https://arxiv.org/html/2311.11944v1), [SECQUE](https://aclanthology.org/2025.gem-1.16.pdf)).
- FinAR-Bench reports stronger extraction than indicator computation for tested LLMs, supporting deterministic arithmetic plus AI interpretation ([FinAR-Bench](https://arxiv.org/html/2506.07315v2)).
- Model-generated historical trading signals require strong leakage and multiple-testing controls ([Glasserman and Lin](https://www.pm-research.com/content/iijjfds/6/1/25), [Probability of Backtest Overfitting](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253)).
- NIST, FINRA, and bank model-risk guidance support provenance, monitoring, effective challenge, limitations, and outcome comparison as sound engineering governance ([NIST AI 600-1](https://doi.org/10.6028/NIST.AI.600-1), [FINRA 24-09](https://www.finra.org/rules-guidance/notices/24-09), [Federal Reserve SR 26-2](https://www.federalreserve.gov/supervisionreg/srletters/SR2602.pdf)). SR 26-2 formally targets covered banks and excludes generative/agentic AI from its scope; its validation practices are used here by analogy, not as a claim of applicability.
- OpenAI documents strict JSON schemas, task-specific evals, prompt caching, reasoning-model use on ambiguous dense information, and the current Sol/Luna prices ([Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs), [evaluation guidance](https://developers.openai.com/api/docs/guides/evaluation-best-practices), [prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching), [reasoning guidance](https://developers.openai.com/api/docs/guides/reasoning-best-practices)).
- OpenAI background mode supports asynchronous polling with `store:false`; Netlify background functions have a 15-minute ceiling, so long manager work must checkpoint across invocations ([OpenAI background mode](https://developers.openai.com/api/docs/guides/background), [Netlify background functions](https://docs.netlify.com/build/functions/background-functions/)).
- Firestore's 1 MiB document limit requires normalized per-symbol decisions and object-backed large content rather than one giant run/dossier document ([Firestore quotas](https://firebase.google.com/docs/firestore/quotas)).
- Alpaca's order documentation supports limit, stop, stop-limit, trailing, OCO/OTO/bracket lifecycle design, while explicitly noting stop fill uncertainty ([Alpaca orders](https://docs.alpaca.markets/us/docs/orders-at-alpaca)).

### Questions that must be answered by measured implementation—not opinion

1. What common/sector-card length preserves selection quality across the current 304-company eligible snapshot and later roster sizes?
2. How often does a company need full re-underwriting: 10, 20, or 30 trading days absent material events?
3. Which point-in-time news/estimates/transcript vendor has adequate coverage, latency, corrections, rights, and cost?
4. Does Sol high materially outperform medium reasoning for the application's frozen eval set and prospective decisions?
5. Does one guarded sub-220k context or Sol-only balanced blocks plus global synthesis perform better on frozen coverage, sentinel, order-rotation, stability, latency, and cost evals? Ship the measured winner with automatic resumable fallback; never allow a cheaper-model prefilter.
6. What research depth produces incremental decision quality before marginal tokens stop helping?
7. Does the AI book outperform v18 and declared benchmarks after spread, slippage, data, and AI costs across enough regimes?
8. What loss/exposure limits match the operator's capital, risk tolerance, tax situation, liquidity needs, account rules, and jurisdiction? These must be supplied before live operation.
9. Is the 304-name mandate intentionally the complete opportunity set, or should a later quarterly Sol-only expansion review cover the broader objectively tradable market and at what budget?

### Final recommendation

Build the dossier, mandate, execution, and evaluation foundations first; then give Sol authority in paper mode. The highest-leverage use of more intelligence is not checking prices more often. It is better source coverage, full-universe comparative judgment, company-appropriate underwriting, persistent memory, explicit disconfirmation, and disciplined measurement. The deterministic layer remains indispensable—but only as the faithful calculator, safety boundary, and executor of the AI Fund Manager's versioned decisions.

## 23. End-to-end implementation traceability

| Stage/trigger | Code owners | Durable output | API/UI hookup | Blocking handoff condition |
|---|---|---|---|---|
| Initial/backfill ingest | `bootstrap-sec.js`, `_investorContentStore`, `_investorFundamentals`, `_investorEvidence`, `_investorDataProviders` | Content manifests, document/fact versions, source state | `sources`, `systemHealth`; System source/dossier health | Hash/size/as-of/entitlement valid; no silent truncation |
| Incremental evidence | `investorIngest-background`, `_investorEventRouter`, `_investorDossier`, Luna extraction in `_investorOpenai` | Evidence delta, refreshed dossier pointer/card | Manager evidence changes; Company drawer sources | All verified deltas retained; high-impact entry pause; extraction support checks |
| Morning manager | `investorKick`, `_investorJobs`, `investorManager-background`, `_investorManager`, `_investorPortfolio` | Small ManagerRun + one ManagerDecision per symbol | `managerDashboard`, `managerRuns`, `companies`; Manager/Companies | Frozen roster exact, holdings managed, response complete, identity hashes present |
| Focused research/final synthesis | `_investorResearch`, `_investorValuation`, `_investorOpenai`, `_investorManager` | ResearchMemo, forecast, final decision, mandate proposal | `companyDossier`, `decisionJournal`; drawer/Learning | Required sources answered/missing, candidates compared together, unique capital ranks |
| Validation/risk/reservation | `_investorMandate`, `_investorRisk`, `_investorPolicy`, `_investorDecisionManifest` | Immutable mandate, activation envelope, reservation, desired pointer/outbox | `mandates`, Portfolio authorization detail | Worst-fill risk passes; no size increase; full reservation; CAS succeeds |
| Broker/paper application | `investorExecution-background`, `_investorExecution`, `_investorBroker`, `_investorLedger`, `_investorMarket` | OrderSet/Leg/Fill/BrokerEvent, applied pointer, balanced journal | `portfolio`, `reconcile`, `ledger`; Portfolio/System | Required legs acknowledged, desired/observed reconciled, filled quantity protected |
| Price target/stop/time event | Broker-native order or OHLC PaperBroker; executor reconciliation | Fill, position/trade update, mandate event | Working orders/execution timeline/alerts | Active applied hash only; idempotent event; **no AI call** |
| New high-impact evidence | `_investorEventRouter`, `_investorMandate`, `investorManager-background` | Pause event, event revision/full memo, superseding desired plan | `materialEvents`, `runFocusedRevision`; Manager/Company | Entry paused, old holding protection retained, Sol conclusion recorded |
| Post-close learning | `investorPostclose-background`, `_investorLearning`, `_investorKpi`, `_investorEvals`, `_investorNav` | NAV, outcomes, matched counterfactuals, KPI/eval rows | `performance`, `decisionAnalytics`; Decisions & Learning | Point-in-time outcome clock, identical cost assumptions, no auto-promotion |
| Operation/security | `_investorAuth`, `_investorState`, `_investorSelftest`, `investorApi`, root `netlify.toml` | Sessions, audit, health, migration/version state | All v2 auth/errors; persistent banners/System | Key rotated, role isolation, health explicit, rollback freezes buys and keeps protection |

## Appendix A — Current backend trace and exact API migration

### Current call graph at the audited commit

1. Root `netlify.toml` invokes `investorKick.handler` every minute.
2. `investorKick.control()` and `decide()` choose `cycle`, `guard`, `evidence`, or `archive`; `dispatch()` writes `InvestorAI_Jobs`, mints a one-use worker nonce, and invokes `investorCycle-background`.
3. `investorCycle-background.handler` validates nonce/job/leases and routes `guard` to `PositionGuard.runGuard`, `evidence` to `runEvidence`, `archive` to `runArchive`, and the default to `runCycle`.
4. `runCycle` bootstraps and loads the frozen strategy/universe/control; fetches the market panel; calculates History, MultiSession, and Signal features/ranks; evaluates holdings; retrieves evidence only for firing non-holdings; lets Luna classify ambiguous move evidence; applies Signal/Intelligence/Temporal gates; sizes through Risk; writes Ledger orders/fills; then updates Shadow/Allocator/Calibration/Feedback/Ladder/Soak/NAV.
5. `runEvidence` prioritizes positions, deterministic candidates, the operator watchlist, then research tier; it handles at most about six companies per sweep, resolves/polls sources, asks Terra to synthesize events, then applies deterministic Intelligence/Temporal policy.
6. `runGuard` loads positions, active legacy EntryPlans, and pending orders; fetches a small one-minute panel; applies corporate-action checks and deterministic ExitPolicy; passes only the latest close into exits/strikes; arms intents and fills/closes through Ledger.
7. `investorApi.handler` authenticates, selects an `ACTIONS` entry, bounds the response, redacts failures, and returns the control-room payload.

This trace is why the refactor must remove old modules from the authority path rather than merely place Sol after them.

### All current API actions

| Current action | Migration |
|---|---|
| `dashboard` | Compatibility read, then replace with `managerDashboard` |
| `learning` | Retire; replace with `decisionAnalytics` |
| `knobs` | Retire deterministic settings |
| `candidate` | Retire; replace with `companyDossier` |
| `performance` | Retain/rewrite with full KPI contract |
| `history` | Retain |
| `quotes` | Retain |
| `navSeries` | Retain |
| `intraday` | Retain |
| `approve` | Retire; AI mandates activate only through validation/risk/reservation, with no approval ceremony |
| `expireStaleOrders` | Fold into mandate executor/reconciliation |
| `reject` | Replace with `cancelMandate`/operator override |
| `kill` | Rename/retain as `emergencyStop` |
| `resume` | Rename/retain as `resumeSystem`, with reconciliation |
| `activateSafety` | Replace with reauthenticated `activateAccountMode`; no ambiguous safety toggle |
| `setControl` | Retire; replace only with named v2 state transitions (`pauseManager`, `freezeBuys`, `emergencyStop`, and explicit resumes) |
| `recordRecallBenchmark` | Remove from operator surface; retain as internal eval ingestion if useful |
| `setRegime` | Retire manual investment control; regime is evidence |
| `openAccount` | Split into single-account `createPaperAccount`, `previewPaperAccountReset`, and `resetPaperAccount`; no v1 account selection |
| `ledger` | Retain/extend with mandate links |
| `reconcile` | Retain/extend for broker adapter |
| `soakStatus` | Retain/rewrite for new executor/manager soak |
| `decisions` | Replace with `decisionJournal` |
| `sources` | Retain under `systemHealth`/detail |
| `intelligence` | Replace with `companyDossier`/`companies` |
| `setIntelligenceWatchlist` | Retire as eligibility gate; optional favorites are UI-only |
| `universe` | Retain/rewrite with canonical eligibility/version metadata |
| `requestSell` | Retain manual sell override |
| `cancelSell` | Retain while request is pending |
| `cancelPlan` | Replace with `cancelMandate` |
| `runCycleNow` | Replace with task-specific enqueue actions |
| `confirmSplit` | Retain/extend |
| `confirmCashDividend` | Retain/extend |
| `setPaperLearning` | Retire |
| `setMarketConfig` | Retain |
| `freezeUniverse` | Retain as roster-version operation, separate from investment judgment |
| `resolveCiks` | Move to automatic setup/health workflow; keep admin-only fallback |
| `health` | Replace/extend as `systemHealth` |

The current source has 38 action methods, not a small handful; additive v2 deployment is necessary to avoid breaking the 334 KB page in one release.

## Appendix B — Orchestration pseudocode

### Manager Meeting

```js
async function advanceManagerMeeting({ tradingDate, accountId, jobId }) {
  const job = await Jobs.claimOnce({ jobId, task: "premarket_manager" });
  const policy = await Policy.loadActive();
  const roster = await Universe.freezeEligibleSnapshot({ tradingDate, policy });
  const portfolio = await Portfolio.snapshot({ accountId, asOf: job.startedAt });
  const workset = Workset.buildManaged({ roster, positions: portfolio.positions, pending: portfolio.pending });
  const cutoff = await Manager.freezeDecisionCutoff({
    runStartedAt: job.startedAt,
    inclusionRule: "published_at_else_immutable_first_seen"
  });
  const cards = await Dossier.compactCards({ symbols: roster.symbols, cutoff });
  Manager.assertExactCoverageInput(cards, roster); // compares version/hash/count/symbols
  const holdingPackets = await Dossier.expandedHoldingDeltas({
    symbols: workset.managedPositionSymbols,
    cutoff
  });
  const contextManifestHash = DecisionManifest.hashAll({
    roster, cards, holdingPackets, portfolio, cutoff, policy
  });

  // OpenAI gateway chooses one guarded context or Sol-only blocks + Sol synthesis.
  // It persists responseId/status so this Netlify job can checkpoint and resume.
  let effectiveInitial = await OpenAI.reviewUniverse({
    cards, universeManifest: roster, holdings: holdingPackets, portfolio,
    policy, contextManifestHash
  });

  const coverage = Manager.validateCoverage(effectiveInitial.coverage, roster);
  if (!coverage.ok) {
    const repaired = await OpenAI.repairCoverageStructure({
      responseId: effectiveInitial.responseId, missing: coverage.missing
    });
    effectiveInitial = Manager.mergeStructuralCoverageRepair({
      original: effectiveInitial, repairedRows: repaired.coverage, expectedMissing: coverage.missing
    });
    if (!Manager.validateCoverage(effectiveInitial.coverage, roster).ok)
      return Manager.failClosed(job, "incomplete_universe_coverage");
  }

  const maintenanceProposal = Manager.buildRiskMaintenancePlan({
    holdingAnalysis: effectiveInitial.holdingAnalysis,
    holdings: holdingPackets,
    hardDeadline: job.holdingDeadline
  });
  const maintenanceClaims = await ClaimVerifier.verifyAndPersistBatch({
    proposals: maintenanceProposal.actionableMandates
  });
  const maintenanceSnapshot = await Portfolio.captureActivationSnapshotAfterBrokerReconcile({ accountId });
  const maintenanceStage = await Mandate.stagePortfolioPlan({
    planClass: "RISK_MAINTENANCE",
    portfolioPlanProposal: maintenanceProposal,
    proposals: maintenanceProposal.actionableMandates,
    verifiedProposalClaims: maintenanceClaims,
    activationSnapshot: maintenanceSnapshot, accountId, cutoff, policy
  });
  if (maintenanceStage.status !== "COMMITTED") {
    await Mandate.retainPriorAcknowledgedProtectionAndAlert({
      accountId, affectedSymbols: maintenanceProposal.symbols, reason: maintenanceStage.reason
    });
  }

  Manager.validateUniqueResearchPriority(effectiveInitial.researchRequests);
  const researchResults = await ResearchPool.run({
    requests: effectiveInitial.researchRequests,
    concurrency: Math.min(3, policy.maxConcurrentResearchJobs),
    orderBy: "researchPriority",
    mayDeferOnlySuffix: true,
    mustCompleteClasses: ["HOLDING_REQUIRED"],
    worker: async request => {
      const packet = await Research.buildPacket({
        symbol: request.symbol, cutoff, prior: await Research.latest(request.symbol),
        directive: request.reviewDirective
      });
      const result = await OpenAI.researchCompany({
        dossier: packet, delta: packet.delta, portfolio,
        tools: ResearchTools.allowlisted({
          evidence: Evidence.getSourceSpans,
          filings: Fundamentals.getFilingFactsAsOf,
          decisionData: DataProviders.searchDecisionData,
          market: Market.getMarketContextAsOf,
          valuation: Valuation.run,
          portfolio: Portfolio.getSnapshot,
          portfolioRisk: PortfolioRisk.runScenarios
        }, policy.toolPolicy)
      });
      const persisted = await Research.persistImmutable(result);
      await ClaimVerifier.verifyAndPersist({
        premises: persisted.factualPremises, sourceManifest: persisted.sourceManifest
      });
      await Jobs.checkpoint(job, { stage: "research", symbol: request.symbol });
      return persisted;
    }
  });
  Manager.forceNonExecutableForIncompleteResearch({
    coverage: effectiveInitial.coverage, researchRequests: effectiveInitial.researchRequests,
    completed: researchResults.completed, deferred: researchResults.deferred, failed: researchResults.failed
  });

  const expansionPortfolio = await Portfolio.capturePlanningSnapshotAfterMaintenanceReconcile({ accountId });
  const feasibleAlternatives = await PortfolioRisk.buildFeasibleAlternatives({
    researchResults: researchResults.completed, holdings: holdingPackets, portfolio: expansionPortfolio, policy
  }); // arithmetic/exposure scenarios only; it does not select companies
  let final = await OpenAI.finalizePortfolio({
    coverage: effectiveInitial.coverage,
    provisionalHoldingAnalysis: effectiveInitial.holdingAnalysis,
    researchResults: researchResults.completed,
    holdings: holdingPackets,
    portfolio: expansionPortfolio,
    feasibleAlternatives,
    policy
  });
  let decisions = Manager.composeFinalDecisionRows({ roster, initial: effectiveInitial, final });
  Manager.validateUniqueCapitalRanksAndFeasibility(decisions, portfolio, policy);

  const verifiedProposalClaims = await ClaimVerifier.verifyAndPersistBatch({
    proposals: final.expansionMandates
  }); // network/model work is never performed inside a retryable Firestore transaction
  let activation = await Portfolio.captureActivationSnapshotAfterBrokerReconcile({ accountId });
  let staged = await Mandate.stagePortfolioPlan({
    planClass: "EXPANSION",
    portfolioPlanProposal: final.portfolioPlan,
    proposals: final.expansionMandates,
    verifiedProposalClaims,
    activationSnapshot: activation,
    accountId, cutoff, policy
  }); // all actionable mandates/reservations commit together or none do

  if (staged.status === "NEEDS_SOL_RESYNTHESIS") {
    final = await OpenAI.finalizePortfolio({
      coverage: effectiveInitial.coverage, provisionalHoldingAnalysis: effectiveInitial.holdingAnalysis,
      researchResults: researchResults.completed, holdings: holdingPackets, portfolio: activation,
      feasibleAlternatives: staged.revisedFeasibleAlternatives, policy,
      priorProposal: final, reason: "MATERIAL_ACTIVATION_CLAMP"
    });
    decisions = Manager.composeFinalDecisionRows({ roster, initial: effectiveInitial, final });
    const reverification = await ClaimVerifier.verifyAndPersistBatch({ proposals: final.expansionMandates });
    activation = await Portfolio.captureActivationSnapshotAfterBrokerReconcile({ accountId });
    staged = await Mandate.stagePortfolioPlan({
      planClass: "EXPANSION",
      portfolioPlanProposal: final.portfolioPlan, proposals: final.expansionMandates,
      verifiedProposalClaims: reverification, activationSnapshot: activation,
      accountId, cutoff, policy
    });
  }
  if (staged.status !== "COMMITTED") return Manager.failClosed(job, staged.reason);

  await Manager.persistDecisionRows({
    job, roster, decisions,
    maintenancePortfolioPlanId: maintenanceStage.planId || null,
    expansionPortfolioPlanId: staged.planId
  });
  const rawOutputManifest = await ContentStore.putJson({
    initial: effectiveInitial, maintenanceProposal, maintenanceStage, final, staged
  });
  return Manager.complete(job, {
    universeVersion: roster.version,
    universeHash: roster.hash,
    eligibleCount: roster.symbols.length,
    decisionCount: decisions.length,
    contextManifestHash,
    activationSnapshotId: activation.id,
    maintenancePortfolioPlanId: maintenanceStage.planId || null,
    expansionPortfolioPlanId: staged.planId,
    rawOutputManifestId: rawOutputManifest.id
  });
}
```

`repairCoverage` is permitted to fill structural omissions only. It may not change already-returned dispositions or issue mandates; any substantive correction becomes a new, fully versioned manager run.

### Later material-event revision

```js
async function runEventRevision(job) {
  const policy = await Policy.loadActive();
  const state = await Portfolio.symbolStateAfterBrokerReconcile(job.accountId, job.symbol);
  const deltaPacket = await Research.buildPacket({
    symbol: job.symbol, cutoff: job.cutoff,
    prior: await Research.latest(job.symbol), directive: "DELTA_REVISION"
  });
  const portfolio = await Portfolio.snapshot({ accountId: job.accountId, asOf: job.cutoff });
  const tools = ResearchTools.allowlisted({
    evidence: Evidence.getSourceSpans, filings: Fundamentals.getFilingFactsAsOf,
    decisionData: DataProviders.searchDecisionData, market: Market.getMarketContextAsOf,
    valuation: Valuation.run, portfolio: Portfolio.getSnapshot,
    portfolioRisk: PortfolioRisk.runScenarios
  }, policy.toolPolicy);

  if (state.kind === "NONHOLDING_NO_ENTRY") {
    const memo = await OpenAI.researchCompany({ dossier: deltaPacket, delta: deltaPacket.delta, portfolio, tools });
    const persisted = await Research.persistImmutable(memo);
    await ClaimVerifier.verifyAndPersist({ premises: persisted.factualPremises, sourceManifest: persisted.sourceManifest });
    await Jobs.enqueueOnce({
      task: "portfolio_synthesis", dedupeId: `event-portfolio:${job.eventId}`,
      payload: { changedSymbol: job.symbol, morningRunId: job.morningRunId, researchVersionId: persisted.id }
    });
    return { state: state.kind, disposition: "RESEARCHED_AWAITING_PORTFOLIO_SYNTHESIS" };
    // A nonholding event can never create a standalone BUY.
  }

  let assessment = state.kind === "UNFILLED_ENTRY" || state.kind === "PARTIALLY_FILLED_ENTRY"
    ? await OpenAI.reviseEntry({ baseline: deltaPacket.prior, delta: deltaPacket.delta, state, portfolio })
    : await OpenAI.reviseHolding({
        baseline: deltaPacket.prior, delta: deltaPacket.delta,
        position: state.position, mandate: state.appliedMandate, portfolio
      });

  let expandedResearch = null;
  if (assessment.researchDirective === "FULL_REUNDERWRITE") {
    expandedResearch = await OpenAI.researchCompany({
      dossier: await Research.buildPacket({
        symbol: job.symbol, cutoff: job.cutoff, prior: deltaPacket.prior,
        directive: "FULL_REUNDERWRITE"
      }),
      delta: deltaPacket.delta, portfolio, tools
    });
    expandedResearch = await Research.persistImmutable(expandedResearch);
    await ClaimVerifier.verifyAndPersist({
      premises: expandedResearch.factualPremises, sourceManifest: expandedResearch.sourceManifest
    });
    assessment = await OpenAI.finalizeEventRevision({
      priorAssessment: assessment, expandedResearch, state, portfolio,
      scenarios: await PortfolioRisk.runScenarios({ portfolio, symbol: job.symbol, policy })
    }); // research output itself is never treated as the decision
  }

  if (state.kind === "UNFILLED_ENTRY" && assessment.entryRevision === "REVOKE")
    return Mandate.requestEntryCancelWithOutbox({ state, eventId: job.eventId });
    // Reservation remains binding through CANCEL_PENDING/RECONCILING and is released
    // only after broker truth proves the remainder terminal; intervening partials resize it.

  // KEEP after an evidence pause still means a new Sol-attested immutable version;
  // REVISE supersedes terms. For a partial fill, the result separately states
  // remainingEntry=KEEP|REVISE|REVOKE and heldDecision=HOLD|REDUCE|SELL|ABSTAIN;
  // acknowledged protection for owned shares is never removed by ABSTAIN.
  if (assessment.decision === "ABSTAIN" && state.kind === "UNFILLED_ENTRY")
    return Mandate.keepEvidencePausedAndCancelRemainder({ state, eventId: job.eventId });
  if (assessment.decision === "ABSTAIN" && state.ownedQuantityUnits !== "0")
    return Mandate.retainProtectionActionRequired({ state, assessment });

  const claimVerdicts = await ClaimVerifier.verifyAndPersistBatch({ proposals: [assessment.mandate] });
  const activation = await Portfolio.captureActivationSnapshotAfterBrokerReconcile({ accountId: job.accountId });
  return Mandate.stagePortfolioPlan({
    portfolioPlanProposal: Manager.singleRevisionPlan({ assessment, state }),
    proposals: [assessment.mandate], verifiedProposalClaims: claimVerdicts,
    activationSnapshot: activation, accountId: job.accountId, cutoff: job.cutoff, policy
  });
}
```

### Evidence-event routing

```js
async function routeEvidenceEvent(event) {
  const match = await EventRouter.resolveDeduplicateAndClassifyObjective(event);
  if (!match.isNewVerifiedDelta) return match;
  await Dossier.recordRoutineDelta(match); // all verified deltas reach next Sol context
  if (match.highRecallSafetyClass)
    await Mandate.pauseUnfilledEntry(match.symbol, match.eventId);
  if (match.highRecallSafetyClass)
    return Jobs.enqueueOnce({
      task: "event_revision",
      dedupeId: `${match.symbol}:${match.canonicalEventHash}`,
      payload: { symbol: match.symbol, eventId: match.eventId, cutoff: match.firstSeenAt }
    });
  if (match.hasActiveMandate)
    await Jobs.prioritizeNextManagerPacket({ symbol: match.symbol, deltaId: match.deltaId });
  return match; // routine evidence waits for the next meeting; code made no investment-materiality judgment
}
```

### Portfolio-plan activation invariant

```js
async function stagePortfolioPlan({ portfolioPlanProposal, proposals,
  verifiedProposalClaims, activationSnapshot, accountId, policy }) {
  // All source retrieval, Sol/Luna calls, claim verification, and broker polling
  // have completed before this function enters a retryable transaction.
  const planHash = DecisionManifest.hashPortfolioPlanProposal(portfolioPlanProposal, proposals);
  const envelopes = Risk.buildPortfolioActivationEnvelopes({
    portfolioPlanProposal, proposals, activationSnapshot, riskMandate: policy.riskMandate
  });
  if (envelopes.hasMaterialClamp)
    return { status: "NEEDS_SOL_RESYNTHESIS", revisedFeasibleAlternatives: envelopes.alternatives };
  if (!envelopes.allAllow)
    return Mandate.persistRejectedPlan({ portfolioPlanProposal, planHash, envelopes });

  return Firestore.runTransaction(async tx => {
    const live = await Mandate.readActivationCASState(tx, accountId);
    Mandate.assertSnapshotStillCurrent({
      live, activationSnapshot,
      expectedPortfolioVersion: activationSnapshot.portfolioVersion,
      expectedReservationVersion: activationSnapshot.reservationAccountVersion,
      expectedWriterEpoch: activationSnapshot.writerEpoch
    });
    Mandate.assertImmutableClaimVerdicts(tx, verifiedProposalClaims); // IDs/hashes only; no model call
    Mandate.assertAllOrNoneSemantics({ portfolioPlanProposal, proposals, envelopes });
    Mandate.assertNoEnvelopeIncreasesProposal({ proposals, envelopes });
    const bindings = Mandate.allocateServerBindings(tx, { accountId, proposals, planHash });
    Mandate.writeCommittedPlanChildrenAndOutbox(tx, {
      portfolioPlanProposal, proposals, bindings, envelopes, activationSnapshot, planHash,
      idempotencyKey: `${planHash}:${activationSnapshot.id}:DESIRED`
    });
    Mandate.advanceCommittedPlanAndReservationAggregate(tx, { accountId, planHash, bindings, envelopes });
    return { status: "COMMITTED", planId: bindings.portfolioPlanId };
  });
}
```

The transaction either advances the complete committed plan and every reservation/desired pointer or writes none of them. It does not claim the external broker has applied the plan. No function in these flows calls a residual selector when Sol is unavailable.

## Appendix C — Current collection inventory

`_investorAdmin.js` declares 38 top-level collection names. The migration must account for every one:

`Control`, `Universe`, `SourceRegistry`, `SourceState`, `Jobs`, `Documents`, `DocumentVersions`, `Claims`, `Events`, `Intelligence`, `MarketLatest`, `MarketFiles`, `MarketDaily`, `Candidates`, `Decisions`, `PaperAccounts`, `PaperOrders`, `PaperFills`, `Positions`, `Trades`, `Ledger`, `StrategyVersions`, `Runs`, `Costs`, `Audit`, `Sessions`, `ShadowDays`, `ShadowOpen`, `ShadowClosed`, `ShadowAccounts`, `ShadowObservations`, `Calibration`, `Invariants`, `SoakCycles`, `ScanSnapshots`, `ArchiveRuns`, `NavMarks`, and `EntryPlans`—each prefixed `InvestorAI_` at runtime.

There is also a `InvestorAI_Control/control/navHistory/{date}` subcollection. `SourceRegistry` and `MarketFiles` appear chiefly declared/reserved in the audited code; migration code must not assume every declared collection currently has documents.
