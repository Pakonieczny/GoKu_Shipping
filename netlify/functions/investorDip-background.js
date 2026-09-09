/*  netlify/functions/investorDip-background.js
 *
 *  The dip-reversal watch loop. Invoked by the kick's job dispatcher (never
 *  scheduled directly). Runs for up to ~13 minutes, polling real-time IEX
 *  prints every five seconds for the watched symbols and handing them to
 *  the deterministic detector. A fresh worker that finds a live loop exits
 *  at once, so one loop runs per account. Paper only; no model gateway.
 */
"use strict";
const preloadedModules = new Set(Object.keys(require.cache));
const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const JOBS = require("./_investorJobs");
const DIP = require("./_investorDipReversal");
const { redact } = require("./_investorAuth");
if (Object.keys(require.cache).some((k) => !preloadedModules.has(k) && /_investorOpenai\.js$/.test(k))) throw new Error("the dip lane must not load the model gateway");

const FN_NAME = "investorDip-background";
const TASK = "dip_watch";
const TICK_MS = 5000;
const LOOP_MS = 13 * 60 * 1000;
const LEASE_MS = 25000;

async function controlDoc() { const s = await A.col(A.COL.control).doc("control").get(); return s.exists ? s.data() : {}; }
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

exports.handler = async (event) => {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { return { statusCode: 400, body: JSON.stringify({ error: "invalid JSON" }) }; }
  const { jobId, task, nonce, payload = {} } = body;
  if (task !== TASK || !jobId) return { statusCode: 400, body: JSON.stringify({ error: "invalid job shape" }) };
  await AUTH.loadAuthSecrets();
  const claimed = await JOBS.claimOnce({ jobId, task: TASK, targetFunction: FN_NAME, token: nonce, payload });
  if (!claimed.claimed) return { statusCode: claimed.httpStatus || 409, body: JSON.stringify({ ok: false, reason: claimed.reason }) };
  const claim = claimed.claim;
  const D = A;
  const startedAt = Date.now();
  const summary = { ticks: 0, events: [], symbols: [], skipped: null, errors: 0 };
  try {
    await M.loadMarketSettings();
    const ctrl = await controlDoc();
    const accountId = String(payload.accountId || ctrl.accountId || "paper-1");
    const settings = DIP.normalizeSettings(ctrl.dip);
    const paperOk = ctrl.engineMode === "manager" && (ctrl.accountMode || ctrl.mode) === "PAPER_AI" && !ctrl.killSwitch && ctrl.executorState !== "PAUSED_SAFETY" && ctrl.executorEnabled !== false && ctrl.emergencyState !== "ENGAGED";
    const stateRef = D.col(D.COL.dipState).doc(accountId);
    /* one loop at a time: a fresh lease inside LEASE_MS means another worker is alive */
    const prior = await stateRef.get();
    const priorLoop = prior.exists ? Number(prior.data().loopAliveAtMs) || 0 : 0;
    if (Date.now() - priorLoop < LEASE_MS) { summary.skipped = "loop_alive"; await JOBS.complete(claim, summary); return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: "loop_alive" }) }; }
    if (!settings.enabled || !paperOk) {
      summary.skipped = !settings.enabled ? "disabled" : "paper_execution_blocked";
      await stateRef.set({ accountId, loopNote: summary.skipped, updatedAtMs: Date.now() }, { merge: true });
      await JOBS.complete(claim, summary); return { statusCode: 200, body: JSON.stringify({ ok: true, skipped: summary.skipped }) };
    }
    const state = await DIP.loadState(D, accountId);
    const wl = await DIP.watchlist(D, accountId, settings, { control: ctrl, state });
    const symbols = wl.symbols; state.excludedAiHeld = wl.excludedAiHeld;
    summary.symbols = symbols;
    const bars = await DIP.seedBars(symbols);
    for (const s of symbols) bars[s] = bars[s] || [];
    /* positions the lane already holds must still be watched even if the
       settings list changed underneath them */
    for (const s of Object.keys(state.open || {})) if (!bars[s]) bars[s] = [];
    let lastSave = 0, lastHeartbeat = Date.now();
    while (Date.now() - startedAt < LOOP_MS) {
      const nowMs = Date.now();
      const session = M.sessionState(new Date(nowMs));
      if (!session.open && !Object.keys(state.open || {}).length) { state.loopNote = "market closed"; break; }
      try {
        const prints = await DIP.latestTrades(Object.keys(bars));
        const events = await DIP.evaluate({ D, accountId, settings, state, bars, prints, session, nowMs });
        summary.ticks += 1;
        if (events.length) summary.events.push(...events.map((e) => ({ kind: e.kind, symbol: e.symbol, atMs: e.atMs, price: e.price, pnlUsd: e.pnlUsd })));
        state.loopNote = `watching ${Object.keys(bars).length} symbol(s) every ${TICK_MS / 1000}s`;
      } catch (e) {
        summary.errors += 1; state.loopNote = "error: " + String(e.message).slice(0, 120);
        console.error("dip tick failed", redact({ jobId, error: e.message }));
        if (summary.errors > 20) break;
      }
      state.loopAliveAtMs = Date.now();
      if (Date.now() - lastSave > 15000 || summary.events.length) { await DIP.saveState(D, { ...state, accountId, loopAliveAtMs: Date.now() }); lastSave = Date.now(); }
      else await stateRef.set({ loopAliveAtMs: Date.now(), loopNote: state.loopNote }, { merge: true });
      if (Date.now() - lastHeartbeat > 60000) { await JOBS.heartbeat(claim).catch(() => {}); lastHeartbeat = Date.now(); }
      const fresh = await controlDoc();
      if (!(fresh.dip && fresh.dip.enabled === true) || fresh.killSwitch || fresh.emergencyState === "ENGAGED" || fresh.executorState === "PAUSED_SAFETY") { state.loopNote = "stopped: lane disabled or execution paused"; break; }
      const spent = Date.now() - nowMs;
      if (spent < TICK_MS) await sleep(TICK_MS - spent);
    }
    await DIP.saveState(D, { ...state, accountId, loopAliveAtMs: 0, loopEndedAtMs: Date.now() });
    await JOBS.complete(claim, { ...summary, events: summary.events.slice(0, 40) });
    return { statusCode: 200, body: JSON.stringify({ ok: true, ticks: summary.ticks, events: summary.events.length }) };
  } catch (e) {
    console.error("investorDip loop failed", redact({ jobId, error: e.message, stack: (e.stack || "").slice(0, 400) }));
    await A.col(A.COL.dipState).doc(String(payload.accountId || "paper-1")).set({ loopAliveAtMs: 0, loopNote: "failed: " + String(e.message).slice(0, 160), updatedAtMs: Date.now() }, { merge: true }).catch(() => {});
    await JOBS.failClosed(claim, { code: e.code || "DIP_LOOP_FAILED", message: e.message, retryable: true }).catch(() => ({}));
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: String(e.message).slice(0, 200) }) };
  }
};
exports.FN_NAME = FN_NAME;
