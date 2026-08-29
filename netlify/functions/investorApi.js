/*  netlify/functions/investorApi.js  (v1.0)
 *  ---------------------------------------------------------------------------
 *  Investor_AI — the console API. Synchronous HTTP, NOT scheduled.
 *
 *  Split from investorKick.js because a scheduled Netlify function cannot be
 *  reached over HTTP: Netlify answers a direct request with a bare 403 and no
 *  body. Putting a console on a cron'd function makes it unreachable, which is
 *  precisely the failure the legacy Google Ads autopilot hit.
 *
 *  Every action requires a verified operator. CORS is an exact-origin
 *  allowlist and is treated as a browser mechanic, never as authorization.
 *  Nothing here fetches a URL supplied by the browser, and no action can
 *  reach a real broker because none exists.
 * ---------------------------------------------------------------------------
 */

"use strict";

const A = require("./_investorAdmin");
const AUTH = require("./_investorAuth");
const M = require("./_investorMarket");
const S = require("./_investorSignal");
const L = require("./_investorLedger");
const E = require("./_investorEvidence");
const O = require("./_investorOpenai");

const MAX_BODY = 64 * 1024;

async function ctrlDoc() {
  const s = await A.col(A.COL.control).doc("control").get();
  return s.exists ? s.data() : {};
}

function latestCycleId(rows) {
  const ids = [...new Set(rows.map((r) => r.cycleId).filter(Boolean))].sort();
  return ids[ids.length - 1] || null;
}

/* ── actions ───────────────────────────────────────────────────────────── */
const ACTIONS = {

  /* The single call the dashboard polls. One round trip, everything on it. */
  async dashboard() {
    const ctrl = await ctrlDoc();
    const accountId = ctrl.accountId || "paper-1";
    const session = M.sessionState(new Date());

    const [candSnap, ordSnap, posSnap, regSnap, costSnap, runSnap] = await Promise.all([
      A.col(A.COL.candidates).orderBy("updated_at", "desc").limit(80).get(),
      A.col(A.COL.orders).where("status", "in", ["proposed", "approved"]).limit(40).get(),
      A.col(A.COL.positions).where("open", "==", true).limit(40).get(),
      A.col(A.COL.control).doc("regime").get(),
      A.col(A.COL.costs).doc(`openai_${new Date().toISOString().slice(0, 10)}`).get(),
      A.col(A.COL.runs).orderBy("startedAt", "desc").limit(8).get(),
    ]);

    const candidates = [];
    candSnap.forEach((d) => candidates.push(d.data()));
    const cycleId = latestCycleId(candidates);
    const current = candidates.filter((c) => c.cycleId === cycleId);

    const orders = []; ordSnap.forEach((d) => orders.push(d.data()));
    const positions = []; posSnap.forEach((d) => positions.push(d.data()));
    const runs = []; runSnap.forEach((d) => {
      const r = d.data();
      runs.push({ jobId: r.jobId, kind: r.kind, status: r.status, breaches: r.breaches,
                  proposals: r.proposals, modelCalls: r.modelCalls, elapsedMs: r.elapsedMs,
                  startedAt: r.startedAt && r.startedAt.toDate ? r.startedAt.toDate().toISOString() : null });
    });

    let balances = null, cost = null;
    try { balances = await L.balances(accountId); } catch {}
    try { cost = await L.costMeter(accountId); } catch {}

    const reg = regSnap.exists ? regSnap.data() : {};
    const openai = costSnap.exists ? costSnap.data() : { usd: 0, calls: 0 };

    return {
      ok: true,
      now: new Date().toISOString(),
      session,
      control: {
        enabled: ctrl.enabled !== false,
        mode: ctrl.mode || "research",
        dryRun: ctrl.dryRun !== false,
        killSwitch: !!ctrl.killSwitch,
        accountId,
        strategyVersion: ctrl.strategyVersion || "v1",
        cycleSeconds: ctrl.cycleSeconds || 300,
        lastCycleFinishedAt: ctrl.lastCycleFinishedAt && ctrl.lastCycleFinishedAt.toDate
          ? ctrl.lastCycleFinishedAt.toDate().toISOString() : null,
        lastCycleSummary: ctrl.lastCycleSummary || null,
      },
      market: {
        provider: M.activeProvider().id,
        feedDelayMinutes: M.activeProvider().delayMinutes,
        maxGrade: M.activeProvider().maxGrade,
      },
      regime: {
        vix: reg.vix || null, vixMedian: reg.vixMedian || null,
        vixNorm: reg.vix && reg.vixMedian ? Number((reg.vix / reg.vixMedian).toFixed(2)) : null,
        cor3m: reg.cor3m || null,
        gate: S.dispersionGate(Number(reg.cor3m), {}),
        asOf: reg.asOf || null,
      },
      cycleId,
      candidates: current.sort((a, b) => (a.rank ?? 1) - (b.rank ?? 1)),
      breaches: current.filter((c) => c.breach),
      orders, positions,
      balances, cost,
      openaiToday: { usd: Number((openai.usd || 0).toFixed(4)), calls: openai.calls || 0,
                     ceiling: O.DAILY_USD_CEILING },
      runs,
    };
  },

  /* Full detail for one name — the per-candidate panel. */
  async candidate({ symbol, cycleId }) {
    if (!symbol) return { error: "symbol required" };
    let cid = cycleId;
    if (!cid) {
      const s = await A.col(A.COL.candidates).where("symbol", "==", symbol)
        .orderBy("updated_at", "desc").limit(1).get();
      if (s.empty) return { error: `no candidate record for ${symbol}` };
      cid = s.docs[0].data().cycleId;
    }
    const [cSnap, pSnap] = await Promise.all([
      A.col(A.COL.candidates).doc(`${cid}_${symbol}`).get(),
      A.col(A.COL.positions).doc(`${(await ctrlDoc()).accountId || "paper-1"}_${symbol}`).get(),
    ]);
    if (!cSnap.exists) return { error: `no candidate record for ${symbol} in ${cid}` };

    const session = M.sessionState(new Date());
    let bars = [];
    try { bars = await M.readBars(symbol, session.date); } catch {}
    if (!bars.length) { try { bars = await M.readRecentBars(symbol, 2); } catch {} }

    const docs = [];
    try {
      const dsnap = await A.col(A.COL.documents).where("symbol", "==", symbol)
        .orderBy("first_seen_at", "desc").limit(12).get();
      dsnap.forEach((d) => {
        const x = d.data();
        docs.push({
          documentId: x.documentId, form: x.form, title: x.title, link: x.link,
          accession: x.accession,
          source_published_at: x.source_published_at,
          first_seen_at: x.first_seen_at && x.first_seen_at.toDate ? x.first_seen_at.toDate().toISOString() : null,
        });
      });
    } catch {}

    return { ok: true, candidate: cSnap.data(), bars, documents: docs,
             position: pSnap.exists ? pSnap.data() : null };
  },

  async approve({ orderId, operator }) {
    if (!orderId) return { error: "orderId required" };
    const r = await L.approveOrder(orderId, operator || "operator");
    await A.col(A.COL.audit).add({ action: "approve", orderId, operator: operator || "operator",
      at: A.FV.serverTimestamp(), ...A.envelope({ created_by: "investorApi" }) });
    return r;
  },

  async reject({ orderId, reason, operator }) {
    if (!orderId) return { error: "orderId required" };
    const r = await L.rejectOrder(orderId, reason, operator || "operator");
    await A.col(A.COL.audit).add({ action: "reject", orderId, reason: reason || null,
      operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return r;
  },

  /* Kill switch cancels proposed and unfilled orders and blocks new decisions.
     It does NOT delete evidence, history, or the ledger. */
  async kill({ operator }) {
    await A.col(A.COL.control).doc("control").set({
      killSwitch: true, enabled: false, killedBy: operator || "operator",
      killedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    const snap = await A.col(A.COL.orders).where("status", "==", "proposed").limit(200).get();
    const b = A.batch();
    snap.forEach((d) => b.set(d.ref, { status: "cancelled", cancelReason: "kill switch" }, { merge: true }));
    await b.commit();
    await A.col(A.COL.audit).add({ action: "kill", cancelled: snap.size,
      operator: operator || "operator", at: A.FV.serverTimestamp(),
      ...A.envelope({ created_by: "investorApi" }) });
    return { killSwitch: true, cancelledProposals: snap.size };
  },

  async resume({ operator }) {
    await A.col(A.COL.control).doc("control").set({
      killSwitch: false, enabled: true, resumedBy: operator || "operator",
      resumedAt: A.FV.serverTimestamp(),
    }, { merge: true });
    return { killSwitch: false, enabled: true };
  },

  async setControl({ patch, operator }) {
    const ALLOW = ["mode", "dryRun", "cycleSeconds", "evidenceEverySeconds",
                   "afterHoursCycles", "accountId", "strategyVersion", "universeVersion"];
    const MODES = ["research", "approval", "shadow", "limited_auto"];
    const clean = {};
    for (const k of ALLOW) {
      if (patch && patch[k] !== undefined) {
        if (k === "mode" && !MODES.includes(patch[k])) continue;
        if (k === "cycleSeconds") { const n = Number(patch[k]); if (n >= 60 && n <= 3600) clean[k] = n; continue; }
        if (k === "evidenceEverySeconds") { const n = Number(patch[k]); if (n >= 300 && n <= 86400) clean[k] = n; continue; }
        if (k === "dryRun" || k === "afterHoursCycles") { clean[k] = !!patch[k]; continue; }
        clean[k] = patch[k];
      }
    }
    if (Object.keys(clean).length) {
      await A.col(A.COL.control).doc("control").set(clean, { merge: true });
      await A.col(A.COL.audit).add({ action: "setControl", patch: clean,
        operator: operator || "operator", at: A.FV.serverTimestamp(),
        ...A.envelope({ created_by: "investorApi" }) });
    }
    return { patched: clean };
  },

  /* Regime inputs. Entered by the operator or written by a future adapter —
     COR3M has no free programmatic feed, so a manual value beats a fabricated
     one, and a stale value degrades sizing rather than silently passing. */
  async setRegime({ vix, vixMedian, cor3m, operator }) {
    const patch = { asOf: new Date().toISOString(), setBy: operator || "operator" };
    if (isFinite(Number(vix))) patch.vix = Number(vix);
    if (isFinite(Number(vixMedian))) patch.vixMedian = Number(vixMedian);
    if (isFinite(Number(cor3m))) patch.cor3m = Number(cor3m);
    await A.col(A.COL.control).doc("regime").set(patch, { merge: true });
    return { regime: patch, gate: S.dispersionGate(patch.cor3m, {}) };
  },

  async openAccount({ accountId, startingNavUsd }) {
    const ctrl = await ctrlDoc();
    const id = accountId || ctrl.accountId || "paper-1";
    const r = await L.openAccount({
      accountId: id,
      startingNavUsd: Number(startingNavUsd) || 100000,
      strategyVersion: ctrl.strategyVersion || "v1",
    });
    await A.col(A.COL.control).doc("control").set({ accountId: id }, { merge: true });
    return r;
  },

  async ledger({ accountId, limit }) {
    const ctrl = await ctrlDoc();
    const id = accountId || ctrl.accountId || "paper-1";
    const snap = await A.col(A.COL.ledger).where("accountId", "==", id)
      .orderBy("postedAt", "desc").limit(Math.min(Number(limit) || 50, 200)).get();
    const rows = [];
    snap.forEach((d) => {
      const x = d.data();
      rows.push({ txnId: x.txnId, kind: x.kind, legs: x.legs, meta: x.meta,
                  postedAt: x.postedAt && x.postedAt.toDate ? x.postedAt.toDate().toISOString() : null });
    });
    return { ok: true, accountId: id, rows,
             balances: await L.balances(id), cost: await L.costMeter(id) };
  },

  /* No-trade reasons are the most valuable dataset this system produces. */
  async decisions({ limit, cycleId }) {
    let q = A.col(A.COL.decisions);
    q = cycleId ? q.where("cycleId", "==", cycleId) : q.orderBy("decisionAtMs", "desc");
    const snap = await q.limit(Math.min(Number(limit) || 60, 250)).get();
    const rows = []; snap.forEach((d) => rows.push(d.data()));
    const tally = {};
    for (const r of rows) for (const b of (r.noTradeReasons || [])) tally[b] = (tally[b] || 0) + 1;
    return { ok: true, rows, blockTally: tally };
  },

  async sources() {
    const snap = await A.col(A.COL.sourceState).limit(60).get();
    const state = {}; snap.forEach((d) => {
      const x = d.data();
      state[d.id] = {
        consecutiveFailures: x.consecutiveFailures || 0,
        lastError: x.lastError || null,
        lastSuccessAt: x.lastSuccessAt && x.lastSuccessAt.toDate ? x.lastSuccessAt.toDate().toISOString() : null,
      };
    });
    return { ok: true, registry: E.SOURCES, state };
  },

  async universe() {
    const ctrl = await ctrlDoc();
    const snap = await A.col(A.COL.universe).doc(ctrl.universeVersion || "v1").get();
    if (snap.exists) return { ok: true, universe: snap.data(), source: "firestore" };
    return { ok: true, universe: require("../../investor/universe/v1.json"), source: "repo" };
  },

  /* Freeze a universe version so additions can never be backdated. */
  async freezeUniverse({ operator }) {
    const u = require("../../investor/universe/v1.json");
    const ref = A.col(A.COL.universe).doc(u.version);
    const existing = await ref.get();
    if (existing.exists) return { ok: true, frozen: false, note: `${u.version} already frozen` };
    await ref.set({ ...u, frozenAt: A.FV.serverTimestamp(), frozenBy: operator || "operator",
      ...A.envelope({ created_by: "investorApi.freezeUniverse" }) });
    return { ok: true, frozen: true, version: u.version, tradeTier: (u.tradeTier || []).length };
  },

  /* Resolve tickers to CIKs from SEC's own authoritative map. A guessed CIK
     silently polls the WRONG filer, which is worse than no CIK at all — so the
     universe ships with them null and this fills them in from the source. */
  async resolveCiks({ operator }) {
    const { fetchPublic } = require("./_investorFetch");
    const r = await fetchPublic("https://www.sec.gov/files/company_tickers.json", {
      sourceId: "sec.tickers", accept: ["json"], timeoutMs: 20000,
    });
    if (!r.json) return { error: "could not parse SEC company_tickers.json" };
    const map = {};
    for (const v of Object.values(r.json)) {
      if (v && v.ticker) map[String(v.ticker).toUpperCase()] = String(v.cik_str).padStart(10, "0").replace(/^0+/, "");
    }
    const u = require("../../investor/universe/v1.json");
    const resolved = [], mismatched = [], missing = [];
    const apply = (row) => {
      const got = map[row.symbol];
      if (!got) { missing.push(row.symbol); return row; }
      if (row.cik && row.cik !== got) {
        mismatched.push({ symbol: row.symbol, had: row.cik, sec: got });
      }
      resolved.push({ symbol: row.symbol, cik: got });
      return { ...row, cik: got, cikSource: "sec_company_tickers" };
    };
    const next = {
      ...u,
      tradeTier: (u.tradeTier || []).map(apply),
      researchTier: (u.researchTier || []).map((r2) => {
        const got = map[r2.symbol];
        return got ? { ...r2, cik: got, cikSource: "sec_company_tickers" } : r2;
      }),
      cikResolvedAt: new Date().toISOString(),
    };
    await A.col(A.COL.universe).doc(u.version).set({
      ...next, frozenAt: A.FV.serverTimestamp(), frozenBy: operator || "operator",
      ...A.envelope({ created_by: "investorApi.resolveCiks" }),
    }, { merge: true });
    return { ok: true, resolved: resolved.length, missing, mismatched,
             note: mismatched.length ? "MISMATCHES FOUND — the manual CIK was wrong and has been corrected from SEC" : "all manual CIKs agreed with SEC" };
  },

  async health() {
    const p = M.activeProvider();
    return {
      ok: true,
      provider: { id: p.id, delayMinutes: p.delayMinutes, maxGrade: p.maxGrade,
                  degradedFrom: p.degradedFrom || null, reason: p.reason || null },
      env: {
        firebase: !!(process.env.FIREBASE_PRIVATE_KEY && process.env.FIREBASE_PROJECT_ID),
        openai: !!process.env.OPENAI_API_KEY,
        passcode: !!process.env.INVESTOR_PASSCODE,
        marketProvider: process.env.INVESTOR_MARKET_PROVIDER || "(unset — manual)",
      },
      session: M.sessionState(new Date()),
      openaiCeilingUsd: O.DAILY_USD_CEILING,
    };
  },
};

/* ── handler ───────────────────────────────────────────────────────────── */
exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers: AUTH.corsHeaders(event), body: "" };
  }
  if (event.httpMethod !== "POST") {
    return AUTH.json(event, 405, { error: "POST only" });
  }
  if ((event.body || "").length > MAX_BODY) {
    return AUTH.json(event, 413, { error: "request too large" });
  }

  let body = {};
  try { body = JSON.parse(event.body || "{}"); }
  catch { return AUTH.json(event, 400, { error: "invalid JSON body" }); }

  const guard = AUTH.requireOperator(event, body);
  if (!guard.ok) return guard.response;

  const action = String(body.action || "dashboard");
  const fn = ACTIONS[action];
  if (!fn) return AUTH.json(event, 400, { error: `unknown action "${action}"` });

  try {
    const out = await fn(body);
    const extra = guard.session ? { session: guard.session } : {};
    return AUTH.json(event, 200, { ...out, ...extra });
  } catch (e) {
    console.error("investorApi", action, AUTH.redact({ error: e.message, stack: (e.stack || "").slice(0, 300) }));
    return AUTH.json(event, 500, { error: String(e.message).slice(0, 200), action });
  }
};

exports.ACTIONS = ACTIONS;
