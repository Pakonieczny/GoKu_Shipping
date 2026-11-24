/* netlify/functions/_etsyStop.js — ETSYSTOP (Paul, 20 Nov 2026: "Once a 429 is reached the application must stop all work related to
   Etsy." … "a manual reset somewhere in the settings that will allow a user to reset all of the applications running on all computers to
   resume pulling from Etsy. I should be able to select this on my computer and then all the other computers should comply through the cloud.")

   ONE switch for the whole shop, kept in the cloud (Firestore EtsyApi_Config/stop). Every server call to Etsy goes through guard(): while the
   switch is on, the call never leaves for Etsy and is answered here with a 429 { error: "etsy_stopped" }. The first 429 Etsy itself sends
   (any function, any station, any computer) turns the switch on. It stays on until a person presses Resume Etsy (Brites Manager → Settings),
   which turns it off in the cloud; every function reads the switch again within CACHE_MS, so every computer and station follows.

   The switch is read once per CACHE_MS per warm function instance (one Firestore read), and a 429 writes it at once. When the cloud cannot be
   read, the last reading stands; a function that never read it lets the call through (a Firestore outage must not stop the shop by itself). */
"use strict";

const COLL = "EtsyApi_Config", DOC_ID = "stop", DOC = COLL + "/" + DOC_ID;
const CACHE_MS = 5000;
const ETSY_URL = /^https?:\/\/(openapi\.etsy\.com|api\.etsy\.com)(\/|$)/i;

let store = null;       // { get(): state|null, set(patch) } — Firestore unless a test sets its own
let cache = null;       // { at, state }

function firestoreStore() {
  const admin = require("./firebaseAdmin");
  const ref = admin.firestore().collection(COLL).doc(DOC_ID);
  return {
    async get() { const s = await ref.get(); return s.exists ? s.data() : null; },
    async set(patch) { await ref.set(patch, { merge: true }); }
  };
}
function storeOf() { if (!store) store = firestoreStore(); return store; }

const clean = st => {
  const s = st || {};
  return { stopped: s.stopped === true, since: +s.since || 0, fn: String(s.fn || ""), path: String(s.path || ""), detail: String(s.detail || ""),
    resumedAt: +s.resumedAt || 0, resumedBy: String(s.resumedBy || ""), rev: +s.rev || 0 };
};

/** The switch as the cloud has it (fresh: skip this instance's few-second copy). */
async function state(fresh = false) {
  if (!fresh && cache && Date.now() - cache.at < CACHE_MS) return cache.state;
  try { const st = clean(await storeOf().get()); cache = { at: Date.now(), state: st }; return st; }
  catch (e) { console.warn("[etsyStop] the switch could not be read:", e && e.message); return cache ? cache.state : clean(null); }
}

/** Turn the switch on (a 429 from Etsy). The first stop's time and cause are kept; later 429s only count. */
async function stop(info = {}) {
  const now = Date.now(), was = await state(true);
  const st = was.stopped ? was : clean({ stopped: true, since: now, fn: info.fn, path: info.path, detail: info.detail, rev: was.rev + 1 });
  cache = { at: now, state: st };
  if (was.stopped) return st;
  try { await storeOf().set({ stopped: true, since: now, fn: String(info.fn || "").slice(0, 80), path: String(info.path || "").slice(0, 160), detail: String(info.detail || "").slice(0, 300), rev: was.rev + 1, resumedAt: null, resumedBy: null }); }
  catch (e) { console.error("[etsyStop] the stop could not be saved to the cloud:", e && e.message); }
  console.warn(`[etsyStop] Etsy answered 429 (${info.fn || "?"} ${info.path || ""}); every Etsy call is stopped until Resume Etsy is pressed`);
  return st;
}

/** Resume Etsy (a person's press in Settings): every function lets calls through again once it reads the switch. */
async function resume(by) {
  const was = await state(true), now = Date.now();
  await storeOf().set({ stopped: false, resumedAt: now, resumedBy: String(by || "").slice(0, 80), rev: was.rev + 1 });
  cache = { at: now, state: clean({ ...was, stopped: false, resumedAt: now, resumedBy: by, rev: was.rev + 1 }) };
  return cache.state;
}

function stoppedResponse(st) {
  const { Response } = require("node-fetch");
  const when = st.since ? new Date(st.since).toISOString() : "";
  return new Response(JSON.stringify({ error: "etsy_stopped", stopped: true, since: st.since, message: `Etsy is stopped for the whole shop since ${when} (Etsy answered 429). Press Resume Etsy in Brites Manager → Settings to start again.` }),
    { status: 429, headers: { "content-type": "application/json", "x-etsy-stopped": "1" } });
}
/* ETSYGATE (Paul, 24 Nov 2026: "space out the burst calls. max 3 per sec"): on 23 Nov at 9:00:18 PM Etsy answered "Exceeded per second rate limit" to one call of several that met in one second, and the whole shop stood
   still until the next morning. Every function runs in processes of its own, so each kept its own pace and none saw the others. ONE gate for all of them (Firestore EtsyApi_Config/gate): a call takes the next free start time,
   GAP_MS after the one before it, whichever function, station or computer that was, and waits for it. The switch is read in the same transaction, so a call that waited behind a 429 never leaves. The last calls are kept there
   (when, which function, which address), so what came together can be read afterwards. When the cloud cannot be reached, the calls of this one process keep the same distance among themselves. */
const PER_SEC = 3, GAP_MS = Math.ceil(1000 / PER_SEC) + 6, GATE_DOC = COLL + "/gate", GATE_KEEP = 60, GATE_WAIT_MS = 6000;
const sleep = ms => new Promise(r => setTimeout(r, ms));
let gate = null, localNext = 0, line = Promise.resolve();
function firestoreGate() {
  const admin = require("./firebaseAdmin"), db = admin.firestore(), stopRef = db.doc(DOC), ref = db.doc(GATE_DOC);
  return { take: info => db.runTransaction(async tx => {
    const [s, g] = await tx.getAll(stopRef, ref), st = clean(s.exists ? s.data() : null); if (st.stopped) return { stopped: st };
    const d = (g.exists && g.data()) || {}, now = Date.now(), next = +d.next || 0, at = next > now && next < now + 600000 ? next : now;
    tx.set(ref, { next: at + GAP_MS, perSec: PER_SEC, recent: (Array.isArray(d.recent) ? d.recent : []).concat([{ t: at, fn: String(info.fn || "").slice(0, 60), path: String(info.path || "").slice(0, 120) }]).slice(-GATE_KEEP) });
    return { at };
  }, { maxAttempts: 10 }) };
}
const localGate = { take: async () => ({ at: 0 }) };
/** This call's turn: waits for its start time. → { stopped: state } when the switch went on meanwhile, else {} */
function turn(info) {
  const mine = line.then(async () => {
    let r = null;
    try { r = await Promise.race([(gate || (gate = store && store.fake ? localGate : firestoreGate())).take(info), sleep(GATE_WAIT_MS).then(() => null)]); if (!r) console.warn("[etsyStop] the gate did not answer: this process keeps the distance itself"); }
    catch (e) { console.warn("[etsyStop] the gate could not be read:", e && e.message); }
    if (r && r.stopped) return r;
    const at = Math.max(Date.now(), (r && r.at) || 0, localNext); localNext = at + GAP_MS; return { at };
  });
  line = mine.then(() => {}, () => {});
  return mine.then(async r => { if (r.stopped) { cache = { at: Date.now(), state: r.stopped }; return r; } const ms = r.at - Date.now(); if (ms > 0) { await sleep(ms); const st = await state(true); if (st.stopped) return { stopped: st }; } return {}; });   // (a call that waited looks at the switch once more: a 429 meanwhile keeps it here)
}
/** The last calls through the gate (read-only). */
async function recent() { try { const s = await require("./firebaseAdmin").firestore().doc(GATE_DOC).get(), d = s.exists ? s.data() || {} : {}; return { perSec: PER_SEC, next: +d.next || 0, recent: Array.isArray(d.recent) ? d.recent : [] }; } catch (_) { return { perSec: PER_SEC, next: 0, recent: [] }; } }
const pathOf = url => { try { return new URL(String(url)).pathname; } catch (_) { return ""; } };   // (never the query: it can hold ids, never keys)

/** A fetch that stops at the switch for Etsy addresses and passes every other address through untouched. */
function guard(fetchImpl, fn) {
  const guarded = async function etsyGuardedFetch(url, init) {
    const target = typeof url === "string" ? url : (url && url.url) || String(url);
    if (!ETSY_URL.test(target)) return fetchImpl(url, init);
    const st = await state();
    if (st.stopped) return stoppedResponse(st);
    const t = await turn({ fn, path: pathOf(target) });   // (ETSYGATE: at most 3 calls a second for the whole shop)
    if (t.stopped) return stoppedResponse(t.stopped);
    const res = await fetchImpl(url, init);
    if (res && res.status === 429) {
      let detail = "";
      try { detail = res.clone ? (await res.clone().text()).slice(0, 300) : ""; } catch (_) {}
      await stop({ fn, path: pathOf(target), detail });
    }
    return res;
  };
  return guarded;
}

/** A 429 this module answered (the switch), not Etsy. */
const isStopAnswer = res => !!(res && res.headers && res.headers.get && res.headers.get("x-etsy-stopped") === "1");

module.exports = { guard, state, stop, resume, recent, isStopAnswer, ETSY_URL, DOC, PER_SEC, GAP_MS,
  _setStore(s) { store = s; if (s) s.fake = true; cache = null; gate = null; localNext = 0; line = Promise.resolve(); }, _setGate(g) { gate = g; localNext = 0; line = Promise.resolve(); }, _clearCache() { cache = null; } };
