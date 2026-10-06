/*  charm-nest-laser-time.js — how long each cut sheet took the Laser person (Paul, 6 Oct 2026; plans/stations-round2/plan.md R7, api.md section LS1).
 *
 *  The Laser person marks a sheet completed in the Sorter's Library (op laserDone) only after it is laser cut AND every back engraving on it is done.
 *  Its time = the moment it is marked - START, START = the LATER of (this person's most recent sign-in as Laser, this person's previous sheet completion).
 *  So the first sheet after a sign-in counts from the sign-in; with a continuous sign-in each next sheet counts from the previous completion; per person;
 *  never earlier than the sign-in (an idle, 5 pm, midnight or manual sign-out and a new sign-in restarts the clock); no Laser sign-in (Admin, no role, Design)
 *  = no time at all, never invented.
 *
 *    LaserSheetTime.measure(by)     -> the `laserTime` the page sends with op laserDone: { v:1, role:"laser"|"", session, loginAgo, prevAgo|null, prevKnown, seconds|null, startedFrom }
 *                                      (ages in ms before the press, never clock times: a wrong clock on this computer cannot spoil the figure). Called once per
 *                                      press by charm-nest-1.html's api(); synchronous and cheap.
 *    LaserSheetTime.noted(res, sent, by)   the server accepted the press: this computer remembers it as the person's previous completion
 *    LaserSheetTime.forget(by)      a completion was taken back: what is remembered about that person's previous sheet is dropped and read again
 *    LaserSheetTime.prepare(by)     reads the person's newest standing completion from the server (op laserSheetLast), so a reload or ANOTHER computer gives the same
 *                                      previous sheet; kept in memory, shifted by the measured difference of the two clocks. Never blocks a press.
 *    LaserSheetTime.configure({ call, who, role, now })    tests and the page: where the reads go and where the sign-in comes from
 *
 *  The SERVER decides the stored figure (netlify/functions/_laserSheetTime.js, from Station_Sessions and the earlier completions) and keeps this page's figure beside
 *  it; when they differ the server's is kept. So everything here is a cross-check and a fallback, and it never throws into the page: every entry is wrapped.
 *  The sign-in comes from StationSession: who() (the person and `startAt`, the moment of the sign-in) and role() (LD1: "laser" for a Laser sign-in).
 *  Nothing is written to the page's screen. No Etsy call, no model call, no PIN: only the NAME is used. */
(function (root) {
  "use strict";
  if (root.LaserSheetTime) return;
  const KEY = "cn.laserSheetLast.v1", MAX_S = 24 * 3600, STALE_MS = 5 * 60000, WATCH_MS = 60000;
  const cfg = { call: null, who: null, role: null, now: null };
  const mem = new Map();          // person key -> { local: ms on THIS clock of the previous completion, fetched: ms (a server read), from: "press" | "server" | "stored" }
  const busy = new Set();
  const warn = (...a) => { try { console.warn("[LaserSheetTime]", ...a); } catch (_) {} };
  const nowMs = () => (typeof cfg.now === "function" ? cfg.now() : Date.now());

  /** One person however the login spelled the name (the server's rule: personKey in _laserSheetTime.js). */
  const cleanName = v => {
    let s = String(v == null ? "" : v).replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim().slice(0, 200);
    if ((s.match(/\p{Nd}/gu) || []).length >= 4) s = s.replace(/\p{Nd}+/gu, " ").replace(/\s+/g, " ").trim();      // a PIN that slipped into a name is dropped
    s = s.slice(0, 80); return /\p{L}/u.test(s) ? s : "";
  };
  const keyOf = v => cleanName(v).normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/['‘’`´]/g, "").replace(/[^\p{L}\p{N}]+/gu, " ").trim();

  /** The pure rule, on one clock: { startAt, startedFrom, seconds }. A tie counts as the sign-in; no sign-in is unknown. */
  function computeStart(at, loginAt, prevAt) {
    at = +at || 0; loginAt = +loginAt || 0; prevAt = +prevAt || 0;
    if (!(at > 0) || !(loginAt > 0)) return { startAt: null, startedFrom: "unknown", seconds: null };
    const fromPrev = prevAt > loginAt && prevAt <= at, startAt = fromPrev ? prevAt : Math.min(loginAt, at);
    return { startAt, startedFrom: fromPrev ? "previousSheet" : "login", seconds: Math.max(0, Math.min(MAX_S, Math.round((at - startAt) / 1000))) };
  }

  /* ── what is remembered (this tab, and this computer's storage as a fallback) ── */
  const store = () => { try { const v = JSON.parse(root.localStorage.getItem(KEY) || "null"); return v && typeof v === "object" ? v : {}; } catch (_) { return {}; } };
  const save = (key, local) => {
    try { const o = store(), keys = Object.keys(o); for (const k of keys.slice(0, Math.max(0, keys.length - 10))) delete o[k]; delete o[key]; o[key] = { local }; root.localStorage.setItem(KEY, JSON.stringify(o)); } catch (_) {}
  };
  function previous(key, loginAt) {
    const m = mem.get(key);
    if (m && m.local > loginAt) return { at: m.local, known: true };
    if (m && m.fetched >= loginAt) return { at: 0, known: true };                 // the server was asked after the sign-in and had none inside it
    if (m && m.local) return { at: 0, known: m.fetched >= loginAt };
    const s = store()[key];
    if (s && s.local > loginAt && s.local <= nowMs()) return { at: s.local, known: true };
    return { at: 0, known: false };
  }

  /* ── the sign-in, from StationSession ── */
  function session() {
    let w = null, role = "";
    try { w = typeof cfg.who === "function" ? cfg.who() : root.StationSession && typeof root.StationSession.who === "function" ? root.StationSession.who() : null; } catch (_) { w = null; }
    try { role = typeof cfg.role === "function" ? cfg.role() : root.StationSession && typeof root.StationSession.role === "function" ? root.StationSession.role() : ""; } catch (_) { role = ""; }
    role = String(role || (w && w.role) || (w && w.station === "laser" ? "laser" : "") || "").toLowerCase();
    return w && w.person ? { person: String(w.person), startAt: +w.startAt || 0, session: String(w.session || ""), role } : null;
  }

  /** The `laserTime` of one press, for `by` (the name that marks the sheet completed). */
  function measure(by) {
    const now = nowMs(), none = { v: 1, role: "", seconds: null, startedFrom: "unknown" };
    try {
      const key = keyOf(by), s = session();
      if (!key || !s || s.role !== "laser" || !(s.startAt > 0) || keyOf(s.person) !== key) return none;     // Admin, no role, Design, or a name that is not the signed-in one: no time
      const p = previous(key, s.startAt), r = computeStart(now, s.startAt, p.at);
      setTimeout(() => { try { if (!busy.has(key) && !(mem.get(key) && now - (mem.get(key).fetched || 0) < STALE_MS)) prepare(by); } catch (_) {} }, 0);   // (so the NEXT press knows what another computer did)
      return { v: 1, role: "laser", session: s.session, loginAgo: Math.max(0, now - s.startAt), prevAgo: p.at ? Math.max(0, now - p.at) : null, prevKnown: p.known, seconds: r.seconds, startedFrom: r.startedFrom, now };
    } catch (e) { warn("measure:", e); return none; }
  }
  /** The server accepted a press: it is this person's previous completion from now on. */
  function noted(res, sent, by) {
    try {
      const key = keyOf(by);
      if (!key || !res || !res.laserTime || !Array.isArray(res.laserTime.sheets) || !res.laserTime.sheets.length) return;
      const local = sent && +sent.now > 0 ? +sent.now : nowMs();
      mem.set(key, { local, fetched: (mem.get(key) || {}).fetched || 0, from: "press" }); save(key, local);
    } catch (e) { warn("noted:", e); }
  }
  function forget(by) {
    try { const key = keyOf(by); if (!key) return; mem.delete(key); const o = store(); delete o[key]; try { root.localStorage.setItem(KEY, JSON.stringify(o)); } catch (_) {} setTimeout(() => { try { prepare(by); } catch (_) {} }, 400); }
    catch (e) { warn("forget:", e); }
  }

  /* ── the server's answer about the previous completion (a read; shifted by the measured difference of the two clocks) ── */
  async function ask(body) {
    if (typeof cfg.call === "function") return cfg.call(body);
    const api = root.CN && root.CN.api;
    if (typeof api !== "function") return null;
    return api("charmNestLibrary", body, { quiet: true, silent: true });
  }
  async function prepare(by) {
    const key = keyOf(by); if (!key || busy.has(key)) return null;
    busy.add(key);
    try {
      const t0 = nowMs(), r = await ask({ op: "laserSheetLast", by: String(by) }), t1 = nowMs();
      if (!r || r.error || !(+r.now > 0)) return null;
      const mid = (t0 + t1) / 2, last = r.last && +r.last.at > 0 ? r.last : null;
      const local = last ? Math.round(mid - (+r.now - +last.at)) : 0, cur = mem.get(key) || {};
      // the server's time of it, put on THIS clock (its own "now" minus how long ago it was); a press this computer made is at least as new, then it stays
      const mine = cur.from === "press" && (cur.local || 0) >= local;
      mem.set(key, { local: mine ? cur.local : local, fetched: t1, from: mine ? "press" : "server" });
      return last ? { at: local, last } : { at: 0, last: null };
    } catch (e) { return null; }
    finally { busy.delete(key); }
  }
  // while a Laser person is signed in, what another computer did is read now and then (one small read every few minutes), so the next press already knows it
  let watching = null;
  function watch() {
    if (watching || typeof setInterval !== "function") return;
    watching = setInterval(() => {
      try { const s = session(); if (!s || s.role !== "laser") return; const key = keyOf(s.person), m = mem.get(key); if (!busy.has(key) && !(m && nowMs() - (m.fetched || 0) < STALE_MS)) prepare(s.person); } catch (_) {}
    }, WATCH_MS);
    if (watching && typeof watching.unref === "function") watching.unref();
  }
  watch();

  root.LaserSheetTime = {
    measure, noted, forget, prepare, computeStart, keyOf,
    configure(o) { Object.assign(cfg, o || {}); return root.LaserSheetTime; },
    /** for the tests and the console: what this tab remembers (a person's previous completion on this clock) */
    remembered(by) { const m = mem.get(keyOf(by)); return m ? Object.assign({}, m) : null; },
    reset() { mem.clear(); busy.clear(); try { root.localStorage.removeItem(KEY); } catch (_) {} }
  };
})(typeof window !== "undefined" ? window : globalThis);
