/*  netlify/functions/_investorPlainReason.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the plain-English reason vocabulary.
 *
 *  WHY A FIXED LIST RATHER THAN GENERATED TEXT
 *  ------------------------------------------------------------------------
 *  Every buy and every sell in this system is produced by a small, closed set
 *  of deterministic rules. There are five ways a position can be opened and
 *  ten ways it can be closed, and that is the whole space. So the reason a
 *  person reads should come from a fixed table keyed by those rules, not from
 *  a sentence assembled at render time and not from a model. Three things
 *  follow from that:
 *
 *    · it cannot drift — the same rule always produces the same words;
 *    · it cannot lie — a code with no entry falls back to a neutral label
 *      rather than inventing a rationale the engine never had;
 *    · it can be tested — a fixture pins every code to its text and to the
 *      five-word ceiling, so a future edit cannot quietly make the console
 *      verbose or wrong.
 *
 *  THE FIVE-WORD RULE. `text` is what appears in the table and is capped at
 *  five words on purpose: it has to be readable at a glance while scanning a
 *  column, by someone with no trading background. `detail` is the one-sentence
 *  expansion for a tooltip or an expander, and carries the nuance the short
 *  form has to drop. Nothing here decides anything; this module is read only
 *  by display paths.
 * ---------------------------------------------------------------------------
 */

"use strict";

const MAX_WORDS = 5;

/* ── WHY IT WAS BOUGHT ───────────────────────────────────────────────────
 * Keyed by the `cause` the evidence lane assigned at the decision, plus the
 * two structural cases: a control-cohort pick, which has no signal at all,
 * and the ordinary fallback for a decision that predates cause recording. */
const BUY_REASONS = Object.freeze({
  control: {
    text: "Random test buy",
    detail: "Taken deliberately at random, at half size, with no signal at all. "
      + "This is the control group the real strategy is measured against — it is "
      + "supposed to be unremarkable.",
    tone: "neutral",
  },
  heavy_selling: {
    text: "Heavy selling, no news",
    detail: "It fell further than the market and its sector explain, on unusually "
      + "heavy volume, and no filing or announcement was found that accounts for it. "
      + "Crowd-driven drops like this tend to partly reverse.",
    tone: "good",
  },
  quiet_drop: {
    text: "Fell hard, no news",
    detail: "It fell further than the market and its sector explain, and nothing in "
      + "the sources the desk watches explains why. The desk is being paid to take "
      + "the other side of that move.",
    tone: "good",
  },
  news_pending: {
    text: "Fell hard, news pending",
    detail: "It fell further than the market and its sector explain, but the evidence "
      + "sweep had not finished checking for a cause. Bought at reduced size because "
      + "the desk knew less than usual.",
    tone: "caution",
  },
  news_drop: {
    text: "Fell on company news",
    detail: "A real filing or announcement explains the fall. The desk does not "
      + "normally fade genuine news, so seeing this is unusual and worth a look.",
    tone: "caution",
  },
  peer_gap: {
    text: "Fell more than peers",
    detail: "It fell further than the market and its sector explain. The specific "
      + "cause was not recorded on this decision.",
    tone: "neutral",
  },
});

/* ── WHY IT WAS SOLD ─────────────────────────────────────────────────────
 * Keyed by `exitKind` from _investorExitPolicy. Every branch of that file
 * appears here; `open` and `exiting` cover a position that has not closed. */
const SELL_REASONS = Object.freeze({
  stop_loss: {
    text: "Hit its safety stop",
    detail: "It fell far enough below what was paid that the desk cut the loss "
      + "rather than hoping. This is the floor under every position, and it fires "
      + "at a loss by definition.",
    tone: "bad",
  },
  trailing_stop: {
    text: "Locked in part of gain",
    detail: "It had gained, then gave back too much from its best price. Selling "
      + "here banks part of a win instead of letting it round-trip back to nothing.",
    tone: "good",
  },
  take_profit: {
    text: "Reached its profit target",
    detail: "It reached the gain the strategy was aiming for.",
    tone: "good",
  },
  pullback_target: {
    text: "Reached its price target",
    detail: "It climbed back to the prior high the trade was aiming at.",
    tone: "good",
  },
  pullback_failed: {
    text: "The bounce failed",
    detail: "It gave back more of its recovery than the setup allows, so the price "
      + "level the trade was leaning on failed.",
    tone: "bad",
  },
  earnings_exit: {
    text: "Sold before earnings report",
    detail: "Results were due. Prices jump unpredictably around a report, so the "
      + "desk steps aside rather than gamble — whatever the position is worth then.",
    tone: "neutral",
  },
  intelligence_critical: {
    text: "Bad company news found",
    detail: "A corroborated, material adverse event about the company itself was "
      + "found, so the position was closed to reduce risk.",
    tone: "bad",
  },
  signal: {
    text: "Gap closed, edge gone",
    detail: "The reason it was bought no longer applies. It was bought because it "
      + "had fallen further than its sector and the market explain; that gap has now "
      + "closed — either because it recovered, or because everything else fell too. "
      + "In the second case there is nothing left to collect even though the price "
      + "is below what was paid.",
    tone: "neutral",
  },
  time: {
    text: "Time limit reached",
    detail: "The trade was given a fixed number of sessions to work. It did not, so "
      + "the money is released for a better idea instead of being tied up.",
    tone: "neutral",
  },
  manual: {
    text: "You sold it",
    detail: "You asked the desk to sell this holding. It is kept out of the "
      + "strategy's scorecard, because it is your judgement rather than its own.",
    tone: "neutral",
  },
  exiting: {
    text: "Selling now",
    detail: "An exit has been decided and is waiting for the first price the desk "
      + "could honestly trade at.",
    tone: "neutral",
  },
  patience_hold: {
    text: "Held to recover",
    detail: "This company's own history says it recovers from drops like this within "
      + "a few sessions, so it is in the patience sleeve: the usual 'the gap has closed' "
      + "sale is held off while it is below what was paid, for a limited number of "
      + "sessions and a limited share of the account. Its safety stop, its earnings "
      + "exit and every other protection are unchanged.",
    tone: "neutral",
  },
  open: {
    text: "Still held",
    detail: "The position is open and its exit rules are checked every minute the "
      + "market is open.",
    tone: "neutral",
  },
});

const CAUSE_TO_BUY = Object.freeze({
  cause_detected_fundamental: "news_drop",
  abnormal_activity_without_covered_fundamental_event: "heavy_selling",
  no_cause_detected_in_covered_sources: "quiet_drop",
  evidence_pending: "news_pending",
});

function entry(table, code, fallback) {
  const row = table[code] || table[fallback];
  return { code: table[code] ? code : fallback, ...row };
}

/** Why a position or closed trade was bought. Reads only recorded fields. */
function buyReason(row = {}) {
  const context = row.decisionContext || {};
  const code = row.cohortRole === "control" ? "control"
    : CAUSE_TO_BUY[row.cause] || "peer_gap";
  return { ...entry(BUY_REASONS, code, "peer_gap"),
    /* Bought by the strike tier at a level the deep scan armed earlier,
       rather than at the moment of the scan itself. Shown as a small tag
       beside the reason; it is a mechanism, not a rationale. */
    atPresetPrice: context.entryPath === "strike",
    armedAtUsd: context.strikeObservation ? context.strikeObservation.armBelowUsd : null };
}

/** Why a trade was sold, or the state of a position that is still open. */
function sellReason(row = {}) {
  if (row.open === true) {
    const intent = row.exitIntent;
    if (intent && Number(intent.decisionAtMs) > 0) {
      const pending = entry(SELL_REASONS, intent.kind, "exiting");
      return { ...pending, pending: true };
    }
    /* A patient position reads differently from an ordinary one: the reason it
       is still here is the sleeve, not the absence of a trigger. */
    if (row.patience && row.patience.granted === true) {
      return { ...entry(SELL_REASONS, "patience_hold", "open"), pending: false,
        patient: true, grantSessions: Number(row.patience.grantSessions) || null };
    }
    return { ...entry(SELL_REASONS, "open", "open"), pending: false };
  }
  const code = row.manualClose === true ? "manual"
    : row.exitKind || closeReasonToKind(row.closeReason || row.exitReason);
  return { ...entry(SELL_REASONS, code, "signal"), pending: false };
}

/* Older trades were written before `exitKind` existed and carry only the
   sentence the rule produced. Recover the kind from it so history predating
   the field still reads correctly rather than defaulting. */
function closeReasonToKind(reason) {
  const r = String(reason || "").toLowerCase();
  if (!r) return null;
  if (/manual_operator_sell|you asked/.test(r)) return "manual";
  if (/hard stop/.test(r)) return "stop_loss";
  if (/trailing stop/.test(r)) return "trailing_stop";
  if (/profit target/.test(r)) return "take_profit";
  if (/prior swing high|pullback target/.test(r)) return "pullback_target";
  if (/gave back .* up-leg|level failed/.test(r)) return "pullback_failed";
  if (/earnings in/.test(r)) return "earnings_exit";
  if (/material adverse|corroborated/.test(r)) return "intelligence_critical";
  if (/crossed exit/.test(r)) return "signal";
  if (/max hold/.test(r)) return "time";
  return null;
}

/** Holding time in words. `tradingDaysHeld` returns fractional SESSIONS, so
 *  0.2 is about eighty minutes of market time, not five hours of wall clock —
 *  reporting it as "0.2d" invited exactly that misreading. */
const SESSION_HOURS = 6.5;
function heldText(tradingDays) {
  /* Number(null) is 0 and Number.isFinite(0) is true, so an absent value used
     to render as "under a minute" — a confident statement about a duration
     nobody recorded. Absence is checked before the coercion. */
  if (tradingDays === null || tradingDays === undefined || tradingDays === "") return "—";
  const d = Number(tradingDays);
  if (!Number.isFinite(d) || d < 0) return "—";
  if (d === 0) return "under a minute";
  if (d < 1) {
    const minutes = Math.round(d * SESSION_HOURS * 60);
    if (minutes < 1) return "under a minute";
    if (minutes < 60) return `${minutes} min`;
    const hours = minutes / 60;
    return `${hours < 10 ? hours.toFixed(1) : Math.round(hours)} hrs`;
  }
  const sessions = d < 10 ? Number(d.toFixed(1)) : Math.round(d);
  return `${sessions} ${sessions === 1 ? "session" : "sessions"}`;
}

/** Every label, for the fixture and for any "what can this say?" view. */
function vocabulary() {
  return {
    buy: Object.entries(BUY_REASONS).map(([code, v]) => ({ code, ...v })),
    sell: Object.entries(SELL_REASONS).map(([code, v]) => ({ code, ...v })),
    maxWords: MAX_WORDS,
  };
}

module.exports = { BUY_REASONS, SELL_REASONS, CAUSE_TO_BUY, MAX_WORDS,
  buyReason, sellReason, closeReasonToKind, heldText, vocabulary };
