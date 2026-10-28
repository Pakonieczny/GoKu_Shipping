/* charm-nest-sync-worker.js — ONE changes feed per computer (Paul, 28 Oct 2026: "every press and every window to feel like a native app, and fewer cloud
   calls"; plan: the local-first design, phase 2).

   What the page did before: every open tab ran up to five "anything new?" timers of its own (the Library's live read, the placement feed, the Review feed,
   the order window's timeline, the sheet window's follow), each every 2 to 4 s while a person worked there: 42 to 64 calls a minute for one tab, twice that
   for two, and 18 to 31 more for each sheet or order window open.

   What this does: one tab of this computer (the leader of its world) asks the cloud ONE small question, charmNestLibrary changes { since } (one document
   read: the Library's revision and its list of what was written, charmNestLibrary.js FEED), and tells every tab of this computer what moved. Each surface
   then reads only what it shows that moved, with the question it always asked (by revision): nothing moved, nothing is read.
     · the leader: a Web Lock per world ('cn-sync:production' | 'cn-sync:sandbox', as cn-poll.js elects one poller per computer), taken by a VISIBLE tab
       only: a tab hidden lets it go to the next visible one at once, a tab closed lets it go by itself, and with every tab hidden nothing is asked at all;
     · the pace (the pace the five timers kept, now for the computer): 3 s while somebody touched any tab of it in the last 2 minutes, 15 s for the next
       ten, then 60 s; at once when a tab is shown again or the network comes back; a failure waits 2, 4, 8 … 60 s;
     · the tabs hear it on BroadcastChannel 'cn-sync:<world>' (the sandbox and production never meet: their own lock, channel, database and revision
       document); each answer carries the count it was asked from, so a tab that missed one asks every surface again (as after a reset);
     · records this computer keeps (charm-nest-store.js: the Library's rows and sets) that moved are read again in one getMany at most every 30 s and
       written through the store, which tells the other tabs;
     · the open order windows of every tab are watched by their Order_Rev (a station's scan, a Team message, a cancel), at most 12;
     · every ten minutes while the computer is in use, every surface asks once anyway (verify): the net for a write nothing named.
   It never writes to the cloud. When the cloud does not know `changes` (an older deploy) or the browser has no BroadcastChannel, it stops and every surface
   keeps its own timer, as before this file existed.

   CNSync (window; module.exports in tests)
     start({ world, api, enabled })   api(fn, body, opts) the page's; enabled() false while the cloud is not answering (nothing is asked)
     follow({ name, kinds, wants(key), active(), since(), read(why, keys) }) → { live(), dirty(), resume(), stop() }
                                      a surface's subscription. kinds: the first letters of the names it reads ('s' sheets, 't' sets, 'r' runs, 'c' custom
                                      records, 'p' where an order's pieces are, 'x' cancels, 'o' timelines, 'm' designs, 'a' learned maps); wants(key): this
                                      name is one it shows; active(): it is on screen now (else it is read when it is: resume); since(): when its own last read
                                      began (a read that began after the leader's question has what it named: nothing more is asked); read(why, keys, at):
                                      why 'keys' | 'reset' | 'verify' | 'dirty', at: when the leader asked (what was written before it is named)
     live()                           the feed answers (here or in another tab): a surface lets its own timer go to its net
     quietSince(t, kinds)             nothing of these kinds moved since t, and the feed was heard all along (a list read at t is still what the cloud holds)
     watchOrder(rid) → off            an open order window's Order_Rev, watched by the leader
     once(name, everyMs, fn)          fn in one tab of this computer at most every everyMs (the Library's seal check)
     poke()                           ask now (a tab shown, the network back)
     state()                          { world, leader, live, n, polls, heard, keys, … } (the tests read it) */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(root);
  else root.CNSync = factory(root);
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const V = 1;
  const PACE = { active: 3000, quiet: 15000, idle: 60000, activeFor: 120000, quietFor: 600000, gap: 600, failMax: 60000, verifyEvery: 600000, liveFor: 150000, fetchEvery: 30000, watchFor: 180000, cursorFor: 1800000 };
  const KIND = /^[strcpxoma*]$/;
  const doc = root.document || null;
  const now = () => Date.now();
  const rand = () => Math.random().toString(36).slice(2, 10) + now().toString(36).slice(-4);
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  const visible = () => !doc || doc.visibilityState !== 'hidden';

  let cfg = { world: 'production', api: null, enabled: null };
  let started = false, dead = false, world = 'production', ch = null, me = rand();
  // the leader's
  let leader = false, cand = null, timer = 0, busy = false, again = false, fails = 0, lastStart = 0, lastOk = 0, verifyAt = 0, cg = null;
  const orderRevs = new Map();                 // rid → its Order_Rev as last answered
  const watching = new Map();                  // tab → { rids, at } (the open order windows of every tab)
  const stale = new Map();                     // 'kind\u0000id' → [kind, id]: records this computer keeps that moved (getMany, fetchEvery)
  let fetchT = 0, fetchAt = 0;
  // every tab's
  let n = null, heardAt = 0, firstAt = 0, lastInput = now(), actSent = 0;
  const lastKind = new Map();                  // kind → the leader's question time of the last answer that named it ('*': not known, '!': a reset)
  const subs = new Set();
  const mine = new Map();                      // rid → how many order windows of this tab watch it
  const stats = { polls: 0, answers: 0, keys: 0, resets: 0, heard: 0, reads: 0, fetches: 0, errors: 0, gaps: 0, leads: 0 };

  const enabled = () => started && !dead && safe(() => (cfg.enabled ? !!cfg.enabled() : true), false);
  const online = () => safe(() => !(root.navigator && root.navigator.onLine === false), true);
  const store = () => (root.CharmNestStore && typeof root.CharmNestStore.has === 'function' ? root.CharmNestStore : null);
  const ls = () => safe(() => root.localStorage, null);
  const cursorKey = () => 'cn.sync.cursor.' + world;

  /* ── the cursor: kept for this computer, so a new leader (or the page after a reload) goes on from where the last one stopped ── */
  function keepCursor() { const s = ls(); if (s && n != null) safe(() => s.setItem(cursorKey(), JSON.stringify({ n, at: now(), cg }))); }
  function keptCursor() {
    const s = ls(); const v = s ? safe(() => JSON.parse(s.getItem(cursorKey()) || 'null'), null) : null;
    return v && Number.isFinite(+v.n) && now() - (+v.at || 0) < PACE.cursorFor ? v : null;
  }

  /* ── what reaches the surfaces ── */
  function deliver(m) {
    const reset = !!m.reset, verify = !!m.verify, keys = new Set(Array.isArray(m.keys) ? m.keys.map(String) : []), at = +m.at || now();
    if (reset) { lastKind.set('!', at); stats.resets++; }
    for (const k of keys) { const kind = k === '*' ? '*' : k[0]; if (KIND.test(kind)) lastKind.set(kind, Math.max(lastKind.get(kind) || 0, at)); }
    stats.keys += keys.size;
    for (const s of [...subs]) s.take(keys, reset, verify, at);
  }
  /* A tab's own surfaces hear an answer: from the leader here, or by the channel. An answer asked from a count this tab did not reach (it missed one) is a
     reset for it: every surface asks its own question again. */
  function heard(m, local) {
    if (!m || m.v !== V) return;
    heardAt = now(); if (!firstAt) firstAt = heardAt;
    if (!local) stats.heard++;
    if (m.t === 'verify') { deliver({ verify: true, at: m.at }); return; }
    const gap = !m.reset && n != null && m.from != null && m.from !== n && m.n !== n;
    if (gap) stats.gaps++;
    if (m.n != null) n = m.n;
    if (m.cg != null) cg = m.cg;
    if (m.first && !gap) return;   // (the first answer of a fresh computer names nothing: what the tabs read at start is what the cloud holds)
    if (m.n === m.from && !m.reset && !gap && !(m.keys && m.keys.length)) return;
    deliver({ keys: m.keys, reset: m.reset || gap, at: m.at });
  }
  function post(m) { if (!ch) return; try { ch.postMessage(Object.assign({ v: V, from_: me }, m)); } catch (_) {} }
  function onMessage(ev) {
    const m = ev && ev.data; if (!m || m.v !== V || m.from_ === me || !started) return;
    if (m.t === 'feed' || m.t === 'verify') heard(m, false);
    else if (m.t === 'act') { lastInput = Math.max(lastInput, +m.at || 0); if (leader) wake(); }
    else if (m.t === 'watch') { watching.set(m.from_, { rids: (m.rids || []).map(String).slice(0, 12), at: now() }); }
    else if (m.t === 'hello' || m.t === 'poke') { if (leader) soon(); }
    else if (m.t === 'lead') { if (mine.size) post({ t: 'watch', rids: [...mine.keys()] }); }
    else if (m.t === 'dead') { stop(true); }
  }

  /* ── the leader's question ── */
  const pace = () => { const q = now() - lastInput; return q < PACE.activeFor ? PACE.active : q < PACE.quietFor ? PACE.quiet : PACE.idle; };
  function schedule(ms) {
    clearTimeout(timer); timer = 0;
    if (!leader) return;
    const wait = ms != null ? ms : fails ? Math.min(PACE.failMax, 2000 * 2 ** Math.min(fails - 1, 5)) : Math.max(PACE.gap, lastStart + pace() - now());
    timer = setTimeout(() => { timer = 0; ask(); }, Math.max(0, wait));
  }
  function soon() { if (!leader) return; if (busy) { again = true; return; } schedule(Math.max(0, lastStart + PACE.gap - now())); }
  function wake() { if (leader && !busy && timer && !fails) schedule(); }   // (input after a quiet spell: the next question at the active pace from the last one)
  function watched() {
    const out = new Map();
    for (const [rid] of mine) out.set(rid, 1);
    for (const [tab, w] of watching) { if (now() - w.at > PACE.watchFor) { watching.delete(tab); continue; } for (const rid of w.rids) out.set(rid, 1); }
    const body = {}; let k = 0;
    for (const rid of out.keys()) { if (k++ >= 12) break; body['o:' + rid] = orderRevs.get(rid) || ''; }
    for (const rid of [...orderRevs.keys()]) if (!out.has(rid)) orderRevs.delete(rid);
    return k ? body : null;
  }
  async function ask() {
    if (!leader) return;
    if (busy) { again = true; return; }
    if (!enabled() || !visible() || !online()) { evaluate(); return; }
    busy = true; lastStart = now(); stats.polls++;
    const at = lastStart, from = n, body = { op: 'getChanges', since: n };   // (op changes, by the name every read rule of the page knows: get…)
    if (cg != null) body.cg = cg;
    const w = watched(); if (w) body.watch = w;
    try {
      const r = await cfg.api('charmNestLibrary', body, { quiet: true, timeoutMs: 15000 });
      if (!leader) return;
      if (!r || r.error || !Number.isFinite(+r.n)) {
        const e = new Error((r && r.error) || 'no answer');
        if (r && /unknown op/i.test(String(r.error))) e.unknown = true;
        throw e;
      }
      fails = 0; lastOk = now(); stats.answers++;
      if (r.orders) for (const [rid, rev] of Object.entries(r.orders)) orderRevs.set(String(rid), String(rev || ''));
      const m = { t: 'feed', from, n: +r.n, keys: r.keys || [], reset: !!r.reset, first: from == null, cg: r.cg != null ? +r.cg : cg, at };
      heard(m, true); post(m); keepCursor();
      if (m.keys.length || m.reset) heldMoved(m.keys, m.reset);
      // (the net: every surface asks once anyway every ten minutes while the computer is in use)
      if (now() - verifyAt >= PACE.verifyEvery && now() - lastInput < PACE.quietFor) { verifyAt = now(); const v = { t: 'verify', at: now() }; if (firstAt && verifyAt - firstAt > 1000) { heard(v, true); post(v); } }
    } catch (e) {
      stats.errors++;
      if (e && (e.unknown || /unknown op/i.test(String(e.message)))) { post({ t: 'dead' }); stop(true); return; }   // (an older cloud: every surface keeps its own timer)
      fails++;
    } finally {
      busy = false;
      if (leader) { if (again) { again = false; schedule(Math.max(0, lastStart + PACE.gap - now())); } else schedule(); }
    }
  }

  /* ── the records this computer keeps (charm-nest-store.js) that moved: one getMany at most every fetchEvery, written through the store ── */
  function heldMoved(keys, reset) {
    const St = store(); if (!St || reset) return;
    for (const k of keys) {
      const kind = k[0] === 's' ? 'sheets' : k[0] === 't' ? 'sets' : null, id = k.slice(2);
      if (kind && id !== '*' && St.has(kind, id)) stale.set(kind + '\u0000' + id, [kind, id]);
    }
    if (stale.size && !fetchT) fetchT = setTimeout(fetchHeld, Math.max(0, fetchAt + PACE.fetchEvery - now()));
  }
  async function fetchHeld() {
    fetchT = 0;
    const St = store(); if (!St || !leader || !stale.size) return;
    const want = [...stale.values()].slice(0, 300); for (const [kind, id] of want) stale.delete(kind + '\u0000' + id);
    const key = ([kind, id]) => (kind === 'sheets' ? 's:' : 't:') + id, ifRevs = {};
    for (const x of want) { const r = St.rev(x[0], x[1]); if (r) ifRevs[key(x)] = r; }
    fetchAt = now(); stats.fetches++;
    try {
      const r = await cfg.api('charmNestLibrary', { op: 'getMany', keys: want.map(key), ifRevs }, { quiet: true, timeoutMs: 20000 });
      for (const [k, v] of Object.entries((r && r.recs) || {})) if (v && v.rec) St.put(k[0] === 's' ? 'sheets' : 'sets', k.slice(2), v.rec, v.rev, { world });
    } catch (_) { /* the surfaces ask by revision when they are shown: nothing is lost */ }
    if (stale.size && !fetchT) fetchT = setTimeout(fetchHeld, PACE.fetchEvery);
  }

  /* ── the election: one leader per computer and world, a visible tab ── */
  function becomeLeader() {
    if (leader) return;
    leader = true; stats.leads++; fails = 0; again = false; verifyAt = now();   // (a handover is not a reason for every surface to ask again)
    if (n == null) { const k = keptCursor(); if (k) { n = +k.n; if (k.cg != null && cg == null) cg = +k.cg; } }
    post({ t: 'lead' });
    ask();
  }
  function loseLeadership() {
    if (!leader) return;
    leader = false; clearTimeout(timer); timer = 0; clearTimeout(fetchT); fetchT = 0;
  }
  function join() {
    if (cand) return;
    const locks = safe(() => root.navigator && root.navigator.locks, null);
    if (!locks || typeof locks.request !== 'function') { cand = { solo: true, leave() {} }; becomeLeader(); return; }   // (no Web Locks: every visible tab asks for itself, one question each, still not five)
    const c = { held: false, release: null, ac: typeof root.AbortController === 'function' ? new root.AbortController() : null };
    c.leave = () => { if (c.held) { if (c.release) c.release(); } else if (c.ac) { try { c.ac.abort(); } catch (_) {} } };
    cand = c;
    try {
      const p = locks.request('cn-sync:' + world, c.ac ? { signal: c.ac.signal } : {}, () => {
        if (cand !== c || !wanted()) return undefined;   // (not wanted any more: the lock goes straight to the next tab)
        c.held = true;
        return new Promise(res => { c.release = () => { c.held = false; res(); }; becomeLeader(); });
      });
      if (p && p.then) p.then(() => { if (cand === c) { cand = null; loseLeadership(); evaluate(); } }, () => { if (cand === c) { cand = null; loseLeadership(); } });
    } catch (_) { cand = null; }
  }
  function leave() { if (!cand) return; const c = cand; cand = null; loseLeadership(); try { c.leave(); } catch (_) {} }
  const wanted = () => enabled() && visible() && online();
  function evaluate() { if (!started || dead) return; if (wanted()) join(); else leave(); }

  /* ── a surface's subscription ── */
  function follow(spec) {
    const kinds = new Set(String(spec.kinds || '').split('').filter(k => KIND.test(k)));
    let dirty = null, gone = false;
    const active = () => safe(() => (spec.active ? !!spec.active() : visible()), false);
    const run = (why, keys, at) => { stats.reads++; safe(() => spec.read(why, keys, at)); };
    const s = {
      take(keys, reset, verify, at) {
        if (gone) return;
        let hit = reset || verify || keys.has('*'), mine = [];
        if (!hit) for (const k of keys) { if (!kinds.has(k[0])) continue; if (k.endsWith(':*') || safe(() => (spec.wants ? !!spec.wants(k) : true), true)) { hit = true; mine.push(k); } }
        if (!hit) return;
        const why = reset ? 'reset' : verify ? 'verify' : 'keys';
        // (its own read began after the leader's question: it holds what that answer named)
        if (why === 'keys' && spec.since && safe(() => +spec.since() || 0, 0) > at) return;
        if (!active() || !visible()) { dirty = dirty || { why, keys: new Set(), at }; if (why === 'reset' || why === 'verify') dirty.why = why; dirty.at = Math.min(dirty.at, at); for (const k of mine) dirty.keys.add(k); return; }
        run(why, mine, at);
      },
      live: () => live(),
      dirty: () => !!dirty,
      resume() { if (gone || !dirty || !active() || !visible()) return false; const d = dirty; dirty = null; run(d.why === 'keys' ? 'dirty' : d.why, [...d.keys], d.at); return true; },
      stop() { gone = true; subs.delete(s); }
    };
    subs.add(s);
    return s;
  }
  const live = () => started && !dead && (leader ? now() - lastOk < PACE.liveFor : n != null && now() - heardAt < PACE.liveFor);
  /** Nothing of these kinds (or not known, or a reset) moved after t, and this tab heard the feed all along since before it. */
  function quietSince(t, kinds) {
    if (!live() || !firstAt || firstAt > t) return false;
    if ((lastKind.get('!') || 0) >= t || (lastKind.get('*') || 0) >= t) return false;
    for (const k of String(kinds || '')) if ((lastKind.get(k) || 0) >= t) return false;
    return true;
  }
  function watchOrder(rid) {
    rid = String(rid == null ? '' : rid).replace(/\D/g, ''); if (!rid) return () => {};
    mine.set(rid, (mine.get(rid) || 0) + 1); post({ t: 'watch', rids: [...mine.keys()] });
    let off = false;
    return () => { if (off) return; off = true; const c = (mine.get(rid) || 1) - 1; if (c > 0) mine.set(rid, c); else mine.delete(rid); post({ t: 'watch', rids: [...mine.keys()] }); };
  }
  /** fn in one tab of this computer at most every everyMs (a stamp all tabs read, and a lock while it runs). */
  async function once(name, everyMs, fn) {
    const key = 'cn.sync.once.' + world + '.' + name, s = ls();
    const last = () => (s ? +safe(() => s.getItem(key), 0) || 0 : 0);
    if (now() - last() < everyMs) return false;
    const go = async () => { if (now() - last() < everyMs) return false; if (s) safe(() => s.setItem(key, String(now()))); await fn(); return true; };
    const locks = safe(() => root.navigator && root.navigator.locks, null);
    if (!locks || typeof locks.request !== 'function') return go();
    try { return await locks.request('cn-sync-once:' + world + ':' + name, { ifAvailable: true }, lock => (lock ? go() : false)); } catch (_) { return go(); }
  }
  function poke() { if (!started || dead) return false; if (leader) soon(); else post({ t: 'poke' }); evaluate(); return true; }

  /* ── this tab ── */
  function onInput() {
    const t = now(); const was = lastInput; lastInput = t;
    if (leader) { if (t - was > PACE.activeFor) wake(); return; }
    if (t - actSent >= 5000) { actSent = t; post({ t: 'act', at: t }); }
  }
  function onVisible() {
    evaluate();
    if (!visible()) return;
    if (leader) soon(); else post({ t: 'hello', n });
    for (const s of [...subs]) s.resume();
  }
  function start(o) {
    if (started) return api;
    cfg = Object.assign(cfg, o || {});
    world = o && o.world === 'sandbox' ? 'sandbox' : 'production';
    if (typeof cfg.api !== 'function' || typeof root.BroadcastChannel !== 'function') { dead = true; return api; }   // (nothing to share it with: every surface keeps its own timer)
    started = true;
    try { ch = new root.BroadcastChannel('cn-sync:' + world); ch.onmessage = onMessage; } catch (_) { ch = null; dead = true; started = false; return api; }
    if (doc && doc.addEventListener) {
      doc.addEventListener('visibilitychange', onVisible);
      for (const ev of ['pointerdown', 'keydown', 'wheel', 'touchstart', 'pointermove']) doc.addEventListener(ev, onInput, { capture: true, passive: true });
    }
    if (root.addEventListener) {
      root.addEventListener('focus', onVisible);
      root.addEventListener('online', () => { fails = 0; onVisible(); });
      root.addEventListener('offline', evaluate);
      root.addEventListener('cn-cloud-back', () => { fails = 0; onVisible(); });
      root.addEventListener('pagehide', leave);
      root.addEventListener('pageshow', onVisible);
    }
    post({ t: 'hello', n });
    evaluate();
    // (the cloud's first answer and a sign-in come without an event of their own: a look every 2 s starts the feed once it may ask)
    let watchSent = 0;
    setInterval(() => { if (dead) return; evaluate(); if (mine.size && now() - watchSent > 30000) { watchSent = now(); post({ t: 'watch', rids: [...mine.keys()] }); } }, 2000);
    return api;
  }
  /** Stop for good on this page (an older cloud, or a test): every surface goes back to its own timer. */
  function stop(isDead) {
    if (isDead) dead = true;
    leave(); clearTimeout(fetchT); fetchT = 0;
  }
  function state() {
    return { world, started, dead, leader, live: live(), n, cg, pace: pace(), polls: stats.polls, answers: stats.answers, heard: stats.heard, keys: stats.keys, resets: stats.resets, reads: stats.reads,
      fetches: stats.fetches, errors: stats.errors, gaps: stats.gaps, leads: stats.leads, subs: subs.size, watched: Object.keys(watched() || {}), lastOk, heardAt, me };
  }
  const api = { start, follow, live, quietSince, watchOrder, once, poke, stop, state, PACE };
  return api;
});
