/*  netlify/functions/_investorUniverse.js
 *  ---------------------------------------------------------------------------
 *  Investor_AI — frozen universe configuration.
 *
 *  This is a .js module rather than a .json data file on purpose. Two builds
 *  failed because the config lived somewhere the bundler could not reach: once
 *  in a sibling investor/ folder that never made it into the repo, and once as
 *  a .json that was not copied alongside the code. A .js file in this folder
 *  travels with every other _investor* helper and esbuild resolves it with
 *  certainty, so that class of failure is gone.
 *
 *  EDIT THIS FILE to retune. It is read at runtime by investorCycle-background
 *  and investorApi. To run a different version, copy to _investorStrategy.v2.js,
 *  register it in Firestore, and switch versions from the control room — never
 *  edit an active version in place.
 * ---------------------------------------------------------------------------
 */

"use strict";

module.exports = {
  "version": "v2",
  "name": "Investor_AI trade tier \u2014 wide breadth for counterfactual learning",
  "createdAt": "2026-08-29",
  "supersedes": "v1 (45 names)",
  "rationale": "Widened from 45 to ~300 liquid US-listed common shares. Two reasons, both about learning speed. (1) Information ratio scales with the SQUARE ROOT of the number of independent bets, so breadth improves results directly \u2014 it is what Medallion actually exploits, winning on a 50.75% hit rate across enormous volume. (2) The shadow harness scores every variant against every ranked name each cycle, so more names means proportionally more counterfactual observations per day, and the ~625 independent days needed to tell a real edge from luck arrives far sooner. Average dollar volume is now MEASURED from the bars the system already fetches rather than hardcoded, because hardcoded volume goes stale within weeks and the liquidity gate is the main defence against cost drag.",
  "exclusions": {
    "binary_biopharma": "Unscheduled FDA/Phase 3 readouts gap 40%+ and no blackout calendar or stop can protect against them. Structurally incompatible with hundreds of position-exposures a year on an edge of tens of basis points.",
    "commodity_beta": "OXY DVN FANG SLB HAL FCX NUE AA CLF - track WTI/copper/steel. Systematic risk wearing a company name; no company-specific event for the evidence engine to find.",
    "rate_driven_financials": "JPM BAC GS MS SCHW BLK - flattest tails in the market but they trade on the curve, not on their own filings.",
    "flat_tail_defensives": "COST HD LOW - Home Depot's implied earnings move was +/-1.8%. Nothing to trade.",
    "managed_care": "UNH CVS CI - beat the options-implied move 88% of the time, the highest in a 29-stock study. Policy shocks are chronically underpriced AND unschedulable - the one gap type a blackout cannot fix.",
    "adrs": "Excluded by the plan's universe boundary until ADR ratio, depositary fee, withholding and termination treatment are implemented."
  },
  "deadTickers": {
    "X": "US Steel \u2014 delisted June 2025 (Nippon Steel)",
    "PARA": "Ticker recycled - now Banzai International, a ~$4M microcap. Paramount Skydance is PSKY.",
    "EA": "take-private closed Aug 2026",
    "MASI": "Danaher acquisition closed; delisted from Nasdaq 2026-06-10.",
    "KLAC_NOTE": "KLAC is live but executed a 10-for-1 split ~May 2026 - unadjusted history shows a spurious -90% gap.",
    "PXD": "Pioneer \u2014 acquired by ExxonMobil, closed 2024",
    "MRO": "Marathon Oil \u2014 acquired by ConocoPhillips, closed 2024",
    "WRK": "WestRock \u2014 merged into Smurfit Westrock (SW), 2024",
    "FISV": "Fiserv \u2014 ticker changed to FI",
    "SWN": "Southwestern \u2014 merged into Expand Energy (EXE), 2024",
    "GPS": "Gap \u2014 ticker changed to GAP"
  },
  "liquidityNote": "advUsd is computed per cycle from measured price x volume. The gate blocks anything under the floor in _investorStrategy.js. No volume figure is asserted in this file.",
  "tradeTier": [
    {
      "symbol": "NVDA",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMD",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "MU",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "INTC",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AVGO",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "QCOM",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TXN",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MRVL",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "ADI",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NXPI",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MCHP",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ON",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SWKS",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "QRVO",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MPWR",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LSCC",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ALAB",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CRDO",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMKR",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ONTO",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ENTG",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MKSI",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ACLS",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UCTT",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ICHR",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMAT",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "LRCX",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KLAC",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TER",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COHR",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WOLF",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SLAB",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SITM",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "POWI",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DIOD",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DELL",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HPQ",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HPE",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SMCI",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "WDC",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "STX",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NTAP",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PSTG",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ANET",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CSCO",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "JNPR",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VRT",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "NVT",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GEV",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ZBRA",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KEYS",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TDY",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TRMB",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FLEX",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "JBL",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SANM",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BHE",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PLXS",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FN",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MSFT",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ORCL",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "CRM",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ADBE",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "INTU",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NOW",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "WDAY",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SNOW",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PANW",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CRWD",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "ZS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "OKTA",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NET",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DDOG",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MDB",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TEAM",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HUBS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VEEV",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TYL",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PTC",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ANSS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CDNS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SNPS",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ADSK",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ROP",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FTNT",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GEN",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DOCU",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ZM",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TWLO",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BILL",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PCTY",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PAYC",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMZN",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GOOGL",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "META",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NFLX",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "DIS",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SPOT",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RBLX",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TTWO",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "APP",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TTD",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PINS",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SNAP",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RDDT",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UBER",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "LYFT",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DASH",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ABNB",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BKNG",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EXPE",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ETSY",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EBAY",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CHWY",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "W",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SHOP",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "JPM",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BAC",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WFC",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "C",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SCHW",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BLK",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BX",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KKR",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "APO",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ARES",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COIN",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "HOOD",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SOFI",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PYPL",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "V",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MA",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AXP",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COF",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DFS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SYF",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FIS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GPN",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ICE",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CME",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NDAQ",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SPGI",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MCO",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MSCI",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TROW",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BEN",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IVZ",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "STT",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NTRS",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UNH",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ELV",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CI",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CVS",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HUM",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CNC",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MCK",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COR",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CAH",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ABT",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BSX",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SYK",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MDT",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ISRG",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EW",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DXCM",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PODD",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ZBH",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BDX",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BAX",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RMD",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ALGN",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TMO",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DHR",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "A",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WAT",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MTD",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IQV",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CRL",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HCA",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UHS",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "THC",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DVA",
      "sector": "health",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LLY",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MRK",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PFE",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ABBV",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BMY",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AMGN",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GILD",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VRTX",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "REGN",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BIIB",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MRNA",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "INCY",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NBIX",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ALNY",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BMRN",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EXEL",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "UTHR",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "JAZZ",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HALO",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SRPT",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IONS",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RARE",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FOLD",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ITCI",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AXSM",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CORT",
      "sector": "pharma",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CAT",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DE",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HON",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GE",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RTX",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LMT",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NOC",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GD",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LHX",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HII",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BA",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TXT",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TDG",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HEI",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CW",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LDOS",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BAH",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CACI",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SAIC",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PSN",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KBR",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "J",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ACM",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EME",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PWR",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MAS",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BLDR",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "URI",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FAST",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "GWW",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ETN",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EMR",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ROK",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PH",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DOV",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IEX",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "XYL",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AME",
      "sector": "indus",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "XOM",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CVX",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COP",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "OXY",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DVN",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FANG",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EOG",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HES",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "APA",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SLB",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HAL",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BKR",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NOV",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FTI",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CHRD",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MTDR",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AR",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RRC",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CTRA",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LNG",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "OKE",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WMB",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KMI",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TRGP",
      "sector": "energy",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NEE",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DUK",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SO",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "D",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AEP",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EXC",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "XEL",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ED",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WEC",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ES",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PEG",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SRE",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PCG",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CEG",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VST",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NRG",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TLN",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AES",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ETR",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FE",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CNP",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CMS",
      "sector": "power",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "WMT",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "COST",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TGT",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "HD",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LOW",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DG",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DLTR",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "KR",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ROST",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TJX",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BURL",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ULTA",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DKS",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "BBY",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AZO",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ORLY",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AAP",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "TSCO",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NKE",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LULU",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DECK",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CROX",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SKX",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "RL",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PVH",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "VFC",
      "sector": "cons",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "FCX",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NUE",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "STLD",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CLF",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "AA",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MP",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LIN",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "APD",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "SHW",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ECL",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DD",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "DOW",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "LYB",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PPG",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "ALB",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CE",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "EMN",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "CF",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "MOS",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "NTR",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "IP",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    },
    {
      "symbol": "PKG",
      "sector": "mat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime"
    }
  ],
  "researchTier": [
    {
      "symbol": "AMD",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "MU",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "MRVL",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "AMAT",
      "sector": "semi",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "SMCI",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "VRT",
      "sector": "hw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "ORCL",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "NOW",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "CRWD",
      "sector": "sw",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "NFLX",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "UBER",
      "sector": "plat",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    },
    {
      "symbol": "COIN",
      "sector": "fin",
      "cik": null,
      "cikSource": "unresolved_pending_sec_map",
      "advUsdSource": "measured_at_runtime",
      "tier": "research"
    }
  ],
  "selfCorrection": "Bootstrap resolves every symbol against SEC's own ticker map and DROPS anything it cannot match, recording it under unresolvedDropped. A delisted or renamed ticker therefore removes itself on the next cycle rather than sitting in the roster producing stale data \u2014 which is what happened with X and EA before a fixture caught them."
};
