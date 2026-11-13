/* charm-nest-engrave-sync.js — THE ENGRAVING WORK OF EVERY LINE, SHARED BY EVERY COMPUTER (ENGRSYNC, Paul, 13 Nov 2026: "Everything must be on the cloud nothing can be on one
   computer"; the local-first design of 28 Oct: the cloud holds the truth, each computer keeps a copy, the changes feed says what moved).

   What there was: a person's engraving work (the Front / Back / Both / None choice, the words and their reading, the font, the place, turn and size, an approval not yet written as a
   back file) lived only in the workspace of the computer it was done on (charm-nest-bridge.js Session, IndexedDB). Another computer showed its own copy, made from the cloud lists and
   never worked on: other words, other choices, None where the first said Back.

   What this does (one mechanism, beside the page; nothing in the engraving code is changed):
     · SEND   every change of the page's workspace is told to Session.schedule; 1.5 s after the last one (6 s at most) each line whose engraving changed HERE is sent whole, as plain
              JSON (its setup `engravingOverride`, its `engrave` summary, and every job's own fields with its place: centre, turn, size), to charmNestLibrary engraveSyncPut. One
              document per line; the newest write wins. The cloud names the order in the changes feed (p:<order>).
     · TAKE   the changes feed (charm-nest-sync-worker.js follow, kinds 'p') names an order another computer wrote: its lines are read (engraveSyncGet) and put on this page's jobs:
              the fields as sent, and the words fitted again AT THE SAME PLACE (keepPlace: fitJob keeps a placement for the very words and font it was made with). A line this person
              changed here in the last 10 s is not overwritten (their own press wins until it is sent). Nothing that was read is paid for again (the reading travels with the line).
     · START  once the workspace is up, every line the cloud holds for this page's orders is taken; the computer that has the run open sends every line the cloud does not hold yet
              (once: the cloud then starts from the work as it is).
     · NET    every 10 minutes, and when the feed says 'p:*' or starts over, the lines written since the last one taken are read (one read per line that changed).
     · ART    a Charm Editor Save on another computer (op artPut, kept in the cloud already) is put on this page's piece when the feed names its order (CharmNestArtEdit.sync), not only
              at a reload or when the piece is opened in the editor.
   COST: a change: one write (and the feed's own). A computer: no read while nothing changed; one read per line another computer changed.

   window.CharmNestEngraveSync: state() · flush() · pull(orders?) · sigOf(line) */
(function (root) {
  "use strict";
  const DEBOUNCE = 1500, MAX_WAIT = 6000, OWN_HOLD = 10000, NET_MS = 600000, BATCH = 40, START_DELAY = 6000;
  const JOB_FIELDS = ["face", "slot", "rowKey", "groupKey", "linked", "pre", "state", "reason", "text", "lines", "lineInput", "lineMode", "splitIndex", "lineGap", "decision", "source", "quote",
    "confidence", "requests", "questions", "claudeReasoning", "ignored", "wordsSource", "claudeJob", "claude", "fontKey", "fontAsked", "fontName", "fontStyle", "faceFontWord", "fontLocked",
    "fontMapped", "fontListing", "fontSubstituted", "wantSize", "nudged", "turnUp", "upSystem", "orientVersion", "approvedBy", "approvedAt", "engravingSeals", "decidedAt", "decidedBy",
    "stagedFrom", "copyOverrides", "archived"];
  const FIT_STATES = new Set(["review", "approved", "written", "ready", "fitting"]);
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const fnv = s => { let h = 0x811c9dc5; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; } return h.toString(36); };
  const plain = v => { try { return v == null ? null : JSON.parse(JSON.stringify(v)); } catch (_) { return null; } };
  const E = () => root.Engrave || null, O = () => root.Orders || null, S = () => root.Session || null;
  const api = (body, opts) => root.CN && typeof root.CN.api === "function" ? root.CN.api("charmNestLibrary", body, Object.assign({ quiet: true }, opts || {})) : Promise.reject(new Error("no api"));
  const dev = () => safe(() => root.CharmNestMadeThumb.state().dev, "") || safe(() => root.localStorage.getItem("cn.madeThumb.dev"), "") || "";
  const who = () => safe(() => (typeof root.employeeName === "function" ? root.employeeName() : "") || (root.B && root.B.employee) || "", "");

  let lastInput = 0;   // (when a person last pressed or typed on this page: only a change a person made here, or the computer that has the run open, is sent; a pass in the background of another
                       //  computer (its own reading or preparing of lines it never worked on) is never sent over the work)
  const PERSON_MS = 20000;
  const st = { sent: 0, taken: 0, fails: 0, last: null, started: false, seeded: false, lastAt: 0, busy: false };
  const SIGS = new Map();      // line key -> the signature of what this page last sent or took (a change is one that differs)
  const LOCAL_AT = new Map();  // line key -> when this page last saw it change by itself (a press here)
  const TAKING = new Set();    // line keys being put on the page now (their changes are not sends)
  const WAITING = new Map();   // line key -> a cloud record whose line was not on this page yet
  async function retryWaiting() { if (!WAITING.size) return 0; const lines = linesNow(); let n = 0; for (const [k, l] of [...WAITING]) { if (!lines.has(k)) continue; WAITING.delete(k); if (await take(l, "keys")) n++; } if (n) { safe(() => E().render()); safe(() => O().render()); } return n; }
  const RAISED = new Set();    // line keys the cloud says are ones to engrave: kept raised here across every re-reading of the order (Orders.interpretAll reads the spec again)
  const raise = line => { if (line && line.spec && !line.spec.engraveCandidate) { line.spec.engraveCandidate = true; line.spec.engraveRaised = true; } };
  let timer = 0, firstAt = 0;

  const lineOf = r => (r && r.parentRow) || r;
  const linesNow = () => { const seen = new Map(); for (const r of safe(() => O().rows(), [])) { const l = lineOf(r); if (l && l.key && !seen.has(l.key) && l.state !== "gone") seen.set(l.key, l); } return seen; };
  const jobsOfLine = line => safe(() => E().jobsOf(line), []) || [];

  /** One job as plain JSON: its own fields and its place (what fitJob needs to put the same words in the same spot). */
  function jobRec(j) {
    const o = { key: j.key };
    for (const f of JOB_FIELDS) if (j[f] !== undefined) o[f] = plain(j[f]);
    const f = j.fit || j.writtenFit;
    if (f && !f.artOnly && Array.isArray(f.centre) && f.size > 0) {
      const v = j.view;
      o.place = { centre: f.centre.slice(), angle: f.angle || 0, size: f.size, lines: (Array.isArray(f.lines) && f.lines.length ? f.lines : j.lines || []).slice(), weight: f.weight || "Regular",
        lineGap: f.lineGap != null ? f.lineGap : j.lineGap, fittedMax: f.fittedMax || f.size, upAngle: v ? v.upAngle : f.upAngle, ...(v ? { cx: v.cx, cy: v.cy } : {}), fontKey: j.fontKey || j.fontId || null };
    }
    return o;
  }
  /** A line's whole engraving as plain JSON, or null when it has none. */
  function lineRec(line) {
    const jobs = jobsOfLine(line);
    if (!jobs.length && !line.engravingOverride) return null;
    return { v: 1, override: plain(line.engravingOverride || null), engrave: plain(line.engrave || null), raised: !!((line.spec && line.spec.engraveCandidate) || RAISED.has(line.key)), jobs: jobs.map(jobRec) };   // raised: (RAISEDSYNC, Paul 13 Nov 2026, order 4222483240 SEA_59109 on another computer: None and no grid where the run computer shows Back with its grid): the line is one to engrave (the run computer's reading raises it; a computer without the run never does)
  }
  /** The signature of what a line's engraving is now ('' when it has none): two computers with the same work have the same one. Places are rounded (a refit lands within 0.01 mm). */
  function sigOf(line) {
    const jobs = jobsOfLine(line); if (!jobs.length && !line.engravingOverride) return "";
    const r2 = n => (Number.isFinite(+n) ? Math.round(+n * 100) / 100 : n);
    // (no copies: one JSON of the fields as they are, the paid reading's long answer left out; the place rounded, as a refit lands within 0.01 mm)
    const parts = [line.engravingOverride || null, !!((line.spec && line.spec.engraveCandidate) || RAISED.has(line.key))];
    for (const j of jobs) { const o = { k: j.key }; for (const f of JOB_FIELDS) if (f !== "claude" && j[f] !== undefined) o[f] = j[f]; const f = j.fit || j.writtenFit; if (f && Array.isArray(f.centre)) o.p = [r2(f.centre[0]), r2(f.centre[1]), r2(f.angle || 0), r2(f.size)]; parts.push(o); }
    let txt = ""; try { txt = JSON.stringify(parts); } catch (_) { txt = String(Date.now()); }
    return fnv(txt);
  }

  /* ── SEND ── */
  function changed() {
    if (!st.started) return;
    const now = Date.now(); if (!firstAt) firstAt = now;
    clearTimeout(timer);
    timer = setTimeout(sweep, now - firstAt >= MAX_WAIT ? 0 : DEBOUNCE);
  }
  async function sweep() {
    timer = 0; firstAt = 0;
    try { await retryWaiting(); } catch (_) {}
    if (st.busy) { changed(); return; }
    const out = [];
    for (const [key, line] of linesNow()) {
      if (TAKING.has(key)) continue;
      const sig = sigOf(line), prev = SIGS.get(key);
      if (prev === undefined) { SIGS.set(key, sig); continue; }   // (first seen here: what it is now is the start, not a change; the start pass decides what is sent)
      if (sig === prev || !sig) continue;
      if (!(Date.now() - lastInput < PERSON_MS) && !(root.B && root.B.run)) { SIGS.set(key, sig); continue; }   // (changed by the page itself on a computer without the run: taken as the start, not sent)
      LOCAL_AT.set(key, Date.now());
      const rec = lineRec(line); if (rec) out.push({ key, rec, sig });
    }
    if (out.length) await send(out);
  }
  async function send(list) {
    st.busy = true;
    try {
      for (let i = 0; i < list.length; i += BATCH) {
        const chunk = list.slice(i, i + BATCH);
        const res = await api({ op: "engraveSyncPut", lines: chunk.map(x => ({ key: x.key, rec: x.rec, sig: x.sig })), by: who(), dev: dev() });
        for (const r of (res && res.lines) || []) { const x = chunk.find(c => c.key === r.key); if (!x) continue; if (r.at) { SIGS.set(x.key, x.sig); st.sent++; st.lastAt = Math.max(st.lastAt, r.at); } else st.fails++; }
      }
      st.last = { at: Date.now(), sent: list.length };
    } catch (e) { st.fails++; st.last = { at: Date.now(), error: String(e && e.message || e) }; setTimeout(changed, 15000); }
    finally { st.busy = false; }
  }

  /* ── TAKE ── */
  async function pull(orders, why) {
    const body = Array.isArray(orders) ? { op: "engraveSyncGet", orders: orders.slice(0, 600) } : { op: "engraveSyncGet", since: Math.max(0, st.lastAt - 5000) };
    let res; try { res = await api(body); } catch (e) { st.fails++; return 0; }
    const lines = (res && res.lines) || []; let n = 0;
    for (const l of lines) { st.lastAt = Math.max(st.lastAt, l.at || 0); if (await take(l, why)) n++; }
    if (n) { safe(() => E().render()); safe(() => O().render()); }
    return n;
  }
  /** One line the cloud holds put on this page, when it is another's newer work. -> true when it was taken. */
  async function take(l, why) {
    const all = linesNow(), line = all.get(l.key); if (!l.rec) return false;
    if (!line) { WAITING.set(l.key, l); return false; }   // (NEWLINES, Paul 13 Nov 2026: "What about future orders?"): the line has not reached this computer yet: taken when it does (retryWaiting)
    const mine = l.dev && l.dev === dev(), localAt = LOCAL_AT.get(l.key) || 0;
    if (Date.now() - localAt < OWN_HOLD) return false;   // (a press made here a moment ago: it is sent, and wins)
    const sigNow = sigOf(line); if (l.sig && sigNow === l.sig) { SIGS.set(l.key, sigNow); return false; }
    if (mine && why !== "start") return false;   // (this computer's own write coming back: already here)
    TAKING.add(l.key);
    try {
      const rec = l.rec;
      if (rec.override !== undefined) line.engravingOverride = plain(rec.override) || undefined;
      if (rec.override === null) delete line.engravingOverride;
      if (rec.engrave) line.engrave = Object.assign(line.engrave || {}, plain(rec.engrave));
      if (rec.raised) { RAISED.add(l.key); raise(line); }
      safe(() => E().ensureJobs(line));
      const items = safe(() => E().items(), null);
      for (const rj of rec.jobs || []) {
        const j = items && items.get(rj.key); if (!j) continue;
        for (const f of JOB_FIELDS) { if (rj[f] === undefined) delete j[f]; else j[f] = plain(rj[f]); }
        if (rj.place && FIT_STATES.has(rj.state) && (j.lineInput || j.lines || []).length) {
          // the same words, in the same font, at the same place: fitJob keeps a placement made for them (keepPlace), then the state it had there goes back on
          const keepState = rj.state, keepApproved = { approvedAt: rj.approvedAt, approvedBy: rj.approvedBy };
          j.keepPlace = plain(rj.place); if (j.keepPlace) j.keepPlace.fontKey = j.fontKey || j.keepPlace.fontKey;
          j.fit = null; j.view = null; j.mask = null; j.state = "ready";
          try { await E().fitJob(j); } catch (e) { console.warn("[engrave-sync] fit", l.key, e); }
          j.state = keepState; if (keepApproved.approvedAt !== undefined) Object.assign(j, keepApproved);
        } else if (!FIT_STATES.has(rj.state)) { j.fit = null; }
      }
      safe(() => root.CharmNestMadeThumb && root.CharmNestMadeThumb.changed("engrave-sync", (line.poolIds || []).slice()));
      safe(() => S().schedule());
      SIGS.set(l.key, sigOf(line));
      st.taken++;
      return true;
    } catch (e) { console.warn("[engrave-sync] take", l.key, e); return false; }
    finally { setTimeout(() => { TAKING.delete(l.key); SIGS.set(l.key, sigOf(all.get(l.key) || line)); }, 50); }
  }

  /* ── START ── */
  async function start() {
    if (st.started) return; st.started = true;
    const lines = linesNow(), orders = [...new Set([...lines.values()].map(l => String(safe(() => l.order.receiptId, "") || l.key.split("_")[0])).filter(Boolean))];
    let have = new Set(); const cloudRaised = new Map();
    try {
      for (let i = 0; i < orders.length; i += 600) {
        const res = await api({ op: "engraveSyncGet", orders: orders.slice(i, i + 600) });
        for (const l of (res && res.lines) || []) { have.add(l.key); cloudRaised.set(l.key, !!(l.rec && l.rec.raised)); st.lastAt = Math.max(st.lastAt, l.at || 0); await take(l, "start"); }
      }
    } catch (e) { st.fails++; }
    // what this page holds now is the start of what it sends; the computer with the run open seeds the cloud with every line it does not hold yet
    const seed = [];
    for (const [key, line] of linesNow()) {
      const sig = sigOf(line); SIGS.set(key, sig);
      if (sig && root.B && root.B.run && (!have.has(key) || (!!(line.spec && line.spec.engraveCandidate) && !cloudRaised.get(key)))) { const rec = lineRec(line); if (rec) seed.push({ key, rec, sig }); }   // (and a line the cloud holds without its raised mark: sent again with it)
    }
    if (seed.length) { st.seeded = true; await send(seed); }
    safe(() => S().schedule());
    safe(() => E().render()); safe(() => O().render());
  }

  /* ── hooks ── */
  function hook() {
    const s = S(); if (!s || s.__engraveSync) return !!s;
    const orig = s.schedule;
    if (typeof orig !== "function") return false;
    s.schedule = function () { try { changed(); } catch (_) {} return orig.apply(this, arguments); };
    s.__engraveSync = true;
    // (RAISEDSYNC: every re-reading of the orders keeps the lines the cloud raised as ones to engrave)
    const Od = O(); if (Od && typeof Od.interpretAll === "function" && !Od.__engraveSyncRaise) { const oi = Od.interpretAll; Od.interpretAll = function () { const r = oi.apply(this, arguments); try { if (RAISED.size) for (const [k, line] of linesNow()) if (RAISED.has(k)) raise(line); } catch (_) {} return r; }; Od.__engraveSyncRaise = true; }
    return true;
  }
  function follow() {
    const Y = root.CNSync; if (!Y || typeof Y.follow !== "function") return;
    Y.follow({ name: "engrave-sync", kinds: "p",
      active: () => true,
      wants: k => k === "p:*" || linesNow().size > 0,
      read: (why, keys) => {
        artSoon();   // (a Charm Editor Save on another computer: the piece's artwork edit is read and put on this page's charm, ArtEdit.sync)
        const named = (keys || []).filter(k => /^p:\d{4,30}$/.test(k)).map(k => k.slice(2));
        if (why === "keys" && named.length && !(keys || []).includes("p:*")) return pull(named, why);
        return pull(null, why);
      } });
  }
  let artT = 0;
  const artSoon = () => { if (artT) return; artT = setTimeout(() => { artT = 0; safe(() => { const A = root.CharmNestArtEdit; if (A && typeof A.sync === "function") Promise.resolve(A.sync()).then(() => { safe(() => E().render()); safe(() => O().render()); }).catch(() => {}); }); }, 1200); };
  function boot() {
    let tries = 0;
    const go = () => {
      tries++;
      const ready = !!(E() && O() && S() && root.CN && typeof root.CN.api === "function" && safe(() => O().rows().length > 0, false));
      if (!ready && tries < 120) return setTimeout(go, 1000);
      hook(); follow();
      for (const t of ["pointerdown", "keydown", "input"]) root.addEventListener(t, e => { if (!e || e.isTrusted !== false) lastInput = Date.now(); }, { capture: true, passive: true });
      setTimeout(() => { start().catch(e => console.warn("[engrave-sync] start", e)); }, START_DELAY);
      setInterval(() => { if (st.started && !(root.document && root.document.hidden)) pull(null, "net").catch(() => {}); }, NET_MS);
    };
    go();
  }
  root.CharmNestEngraveSync = { state: () => Object.assign({}, st, { lines: SIGS.size }), flush: sweep, pull, sigOf: l => sigOf(lineOf(l)), _take: take };
  if (root.document) { if (root.document.readyState === "loading") root.document.addEventListener("DOMContentLoaded", boot); else boot(); }
})(typeof self !== "undefined" ? self : this);
