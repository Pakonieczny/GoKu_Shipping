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
       ten, then 60 s; 2 s while a surface that followed at 2 s is in use (the Review tab, a sheet window: nothing anybody looks at is slower than it was);
       at once when a tab is shown again or the network comes back; a failure waits 2, 4, 8 … 60 s;
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
     follow({ name, kinds, wild, wants(key), active(), since(), every(), read(why, keys) }) → { live(), dirty(), resume(), stop() }
                                      a surface's subscription. kinds: the first letters of the names it reads ('s' sheets, 't' sets, 'r' runs, 'c' custom
                                      records, 'p' where an order's pieces are, 'x' cancels, 'o' timelines, 'm' designs, 'a' learned maps); wild: the kinds whose
                                      '<kind>:*' (some of it, not known which) concerns it (all of its kinds unless said); every(): the pace it followed at on
                                      its own while it is in use (2000: the Review tab, a sheet window), 0 otherwise; wants(key): this
                                      name is one it shows; active(): it is on screen now (else it is read when it is: resume); since(): when its own last read
                                      began (a read that began after the leader's question has what it named: nothing more is asked); read(why, keys, at):
                                      why 'keys' | 'reset' | 'verify' | 'dirty' | 'lost' (the feed stopped answering: back to its own timer, read now),
                                      at: when the leader asked (what was written before it is named);
                                      clear(): whether something was named while it was not on screen, forgotten (the surface reads it its own way)
     live()                           the feed answers (here or in another tab): a surface lets its own timer go to its net
     quietSince(t, kinds)             nothing of these kinds moved since t, and the feed was heard all along (a list read at t is still what the cloud holds)
     watchOrder(rid) → off            an open order window's Order_Rev, watched by the leader
     once(name, everyMs, fn)          fn in one tab of this computer at most every everyMs (the Library's seal check)
     poke(fn?)                        ask now (a tab shown, the network back); fn({ keys, reset }): the first answer to a question asked after it
     state()                          { world, leader, live, n, polls, heard, keys, … } (the tests read it)

   THE OUTBOX'S DRAIN (Paul, 28 Oct 2026: "nothing he presses may wait for the cloud"; plan: phase 3). The presses this computer kept (charm-nest-store.js mutate)
   are sent from here, by ONE tab of the computer at a time (a Web Lock per world, 'cn-outbox:<world>', taken by whichever tab has something to send, shown or
   hidden: a press made just before the tab was put away still goes), oldest first, each with its mutation id, several timeline steps in one request. An answer
   leaves the outbox and tells the press (here and in every tab); a refusal in the cloud's own words too; no answer (offline, a timeout, the cloud down) is tried
   again after 1, 2, 4 … 60 s (a fifth either way), at once when the network comes back or a tab is shown. The page going away sends what is still waiting with
   keepalive (the next page sends it again, and the mid makes that a no-op). An answer says the feed's count it wrote (n) and the names it wrote under (keys):
   the feed's next answer that names only those is this computer's own write, already drawn, and no surface reads for it (follow-up reads per write: 0).
     drain()                          send now what waits (the store calls it on every press)
     own(keys, n)                     these names at this count are this computer's own write (every tab is told)
     drainState()                     { sent, acked, failed, retried, replayed, batches, waiting } (the tests read it) */
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
  let n = null, heardAt = 0, firstAt = 0, lastInput = now(), actSent = 0, hurry = 0, hurryAt = 0, hurrySent = 0;
  const lastKind = new Map();                  // kind → the leader's question time of the last answer that named it ('*': not known, '!': a reset)
  const subs = new Set();
  const onceAt = new Map();
  let pokes = [], failing = false;             // (failing: the leader's last question failed; every surface reads on its own until one is answered)                              // ({ at, fn }) poke(fn): told the first answer to a question asked after it                    // name → when a tab of this computer last ran it (once: told on the channel, never stored)
  const mine = new Map();                      // rid → how many order windows of this tab watch it
  const stats = { polls: 0, answers: 0, keys: 0, resets: 0, heard: 0, reads: 0, fetches: 0, errors: 0, gaps: 0, leads: 0 };

  const enabled = () => started && !dead && safe(() => (cfg.enabled ? !!cfg.enabled() : true), false);
  const online = () => safe(() => !(root.navigator && root.navigator.onLine === false), true);
  const store = () => (root.CharmNestStore && typeof root.CharmNestStore.has === 'function' ? root.CharmNestStore : null);
  const ls = () => safe(() => root.localStorage, null);
  const cursorKey = () => 'cn.sync.cursor.' + world;

  /* ── the cursor: kept for this computer, so a new leader (or the page after a reload) goes on from where the last one stopped ── */
  // (production's only: the sandbox leaves nothing behind in the browser (Paul, 10 Oct 2026), and a sandbox page after a reload begins from the count it is answered)
  function keepCursor() { const s = world === 'production' ? ls() : null; if (s && n != null) safe(() => s.setItem(cursorKey(), JSON.stringify({ n, at: now(), cg }))); }
  function keptCursor() {
    const s = world === 'production' ? ls() : null; const v = s ? safe(() => JSON.parse(s.getItem(cursorKey()) || 'null'), null) : null;
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
    heard0(m, local);
    if (m && m.v === V && m.t === 'feed' && pokes.length) {   // (poke(fn): the first answer to a question asked after it)
      const due = pokes.filter(p => (+m.at || 0) >= p.at - 50 || now() - p.at > 10000); pokes = pokes.filter(p => !due.includes(p));
      for (const p of due) if (now() - p.at <= 10000) safe(() => p.fn({ keys: (m.keys || []).map(String), reset: !!m.reset }));
    }
  }
  function heard0(m, local) {
    if (!m || m.v !== V) return;
    heardAt = now(); if (!firstAt) firstAt = heardAt;
    if (m.t === 'feed') failing = false;
    if (!local) stats.heard++;
    if (m.t === 'verify') { deliver({ verify: true, at: m.at }); return; }
    const gap = !m.reset && n != null && m.from != null && m.from !== n && m.n !== n;
    if (gap) stats.gaps++;
    if (m.n != null) n = m.n;
    if (m.cg != null) cg = m.cg;
    if (m.first && !gap) return;   // (the first answer of a fresh computer names nothing: what the tabs read at start is what the cloud holds)
    if (m.n === m.from && !m.reset && !gap && !(m.keys && m.keys.length)) return;
    // (OUTBOX: a name written last by this computer's own press, at the count its answer said, is drawn already: no surface reads for it)
    const keys = !m.reset && !gap && m.counts && Array.isArray(m.keys) ? m.keys.filter(k => !(ownN.has(k) && ownN.get(k).n === +m.counts[k])) : m.keys;
    if (keys && m.keys && keys.length < m.keys.length) { stats.ownSkipped = (stats.ownSkipped || 0) + m.keys.length - keys.length; if (!keys.length && !m.reset && !gap) return; }
    deliver({ keys, reset: m.reset || gap, at: m.at });
  }
  function post(m) { if (!ch) return; try { ch.postMessage(Object.assign({ v: V, from_: me }, m)); } catch (_) {} }
  function onMessage(ev) {
    const m = ev && ev.data; if (!m || m.v !== V || m.from_ === me || !started) return;
    if (m.t === 'feed' || m.t === 'verify') heard(m, false);
    else if (m.t === 'act') { lastInput = Math.max(lastInput, +m.at || 0); hurry = +m.hurry || 0; hurryAt = +m.at || now(); if (leader) wake(); }
    else if (m.t === 'watch') { watching.set(m.from_, { rids: (m.rids || []).map(String).slice(0, 12), at: now() }); }
    else if (m.t === 'hello' || m.t === 'poke') { if (leader) soon(); }
    else if (m.t === 'lead') { if (mine.size) post({ t: 'watch', rids: [...mine.keys()] }); }
    else if (m.t === 'dead') { stop(true); }
    else if (m.t === 'fail') { lost(); }
    else if (m.t === 'once') { onceAt.set(String(m.name), Math.max(onceAt.get(String(m.name)) || 0, +m.at || 0)); }
    else if (m.t === 'own') { ownHeard(m.keys, m.n); }
  }

  /* ── the leader's question ── */
  // (a surface in use that followed faster than 3 s on its own asks for its own pace while it is used: hurry, from any tab)
  const wantFast = () => { let ms = 0; for (const s of subs) { const v = s.every(); if (v > 0 && (!ms || v < ms)) ms = v; } return ms; };
  const pace = () => {
    const q = now() - lastInput; if (q >= PACE.activeFor) return q < PACE.quietFor ? PACE.quiet : PACE.idle;
    const own = leader ? wantFast() : 0, other = now() - hurryAt < PACE.activeFor ? hurry : 0, fast = [own, other].filter(v => v > 0);
    return fast.length ? Math.max(1000, Math.min(PACE.active, ...fast)) : PACE.active;
  };
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
      const m = { v: V, t: 'feed', from, n: +r.n, keys: r.keys || [], counts: r.counts || null, reset: !!r.reset, first: from == null, cg: r.cg != null ? +r.cg : cg, at };
      heard(m, true); post(m); keepCursor();
      if (m.keys.length || m.reset) heldMoved(m.keys, m.reset);
      // (the net: every surface asks once anyway every ten minutes while the computer is in use)
      if (now() - verifyAt >= PACE.verifyEvery && now() - lastInput < PACE.quietFor) { verifyAt = now(); const v = { v: V, t: 'verify', at: now() }; if (firstAt && verifyAt - firstAt > 1000) { heard(v, true); post(v); } }
    } catch (e) {
      stats.errors++;
      if (e && (e.unknown || /unknown op/i.test(String(e.message)))) { post({ t: 'dead' }); stop(true); return; }   // (an older cloud: every surface keeps its own timer)
      fails++;
      post({ t: 'fail', at: now() }); lost();   // (the question failed: every surface of every tab reads on its own, as before this file, until one is answered)
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
    const kinds = new Set(String(spec.kinds || '').split('').filter(k => KIND.test(k))), wild = new Set(String(spec.wild == null ? spec.kinds || '' : spec.wild).split(''));
    let dirty = null, gone = false;
    const active = () => safe(() => (spec.active ? !!spec.active() : visible()), false);
    const run = (why, keys, at) => { stats.reads++; safe(() => spec.read(why, keys, at)); };
    const s = {
      every: () => (gone || !spec.every ? 0 : safe(() => +spec.every() || 0, 0)),
      take(keys, reset, verify, at) {
        if (gone) return;
        let hit = reset || verify || keys.has('*'), mine = [];
        if (!hit) for (const k of keys) { if (!kinds.has(k[0])) continue; if (k.endsWith(':*') ? wild.has(k[0]) : safe(() => (spec.wants ? !!spec.wants(k) : true), true)) { hit = true; mine.push(k); } }
        if (!hit) return;
        const why = reset ? 'reset' : verify ? 'verify' : 'keys';
        // (its own read began after the leader's question: it holds what that answer named)
        if (why === 'keys' && spec.since && safe(() => +spec.since() || 0, 0) > at) return;
        if (!active() || !visible()) { dirty = dirty || { why, keys: new Set(), at }; if (why === 'reset' || why === 'verify') dirty.why = why; dirty.at = Math.min(dirty.at, at); for (const k of mine) dirty.keys.add(k); return; }
        run(why, mine, at);
      },
      live: () => live(),
      dirty: () => !!dirty,
      clear: () => { const d = !!dirty; dirty = null; return d; },
      lost: () => { if (gone) return; if (active() && visible()) run('lost', [], now()); else dirty = { why: 'reset', keys: new Set(), at: now() }; },
      resume() { if (gone || !dirty || !active() || !visible()) return false; const d = dirty; dirty = null; run(d.why === 'keys' ? 'dirty' : d.why, [...d.keys], d.at); return true; },
      stop() { gone = true; subs.delete(s); }
    };
    subs.add(s);
    return s;
  }
  const live = () => started && !dead && !failing && (leader ? now() - lastOk < PACE.liveFor : n != null && now() - heardAt < PACE.liveFor);
  /** The feed stopped answering (a failed question, an older cloud): each surface is told once (read 'lost'), so a timer it let go to its net comes back now. */
  function lost(isDead) {
    if (failing && !isDead) return;
    failing = true;
    for (const s of [...subs]) s.lost();
  }
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
  /** fn in one tab of this computer at most every everyMs (a lock while it runs, and the time it ran told to every tab). */
  async function once(name, everyMs, fn) {
    name = String(name);
    const last = () => onceAt.get(name) || 0;
    if (now() - last() < everyMs) return false;
    const go = async () => { if (now() - last() < everyMs) return false; const at = now(); onceAt.set(name, at); post({ t: 'once', name, at }); await fn(); return true; };
    const locks = safe(() => root.navigator && root.navigator.locks, null);
    if (!locks || typeof locks.request !== 'function') return go();
    try { return await locks.request('cn-sync-once:' + world + ':' + name, { ifAvailable: true }, lock => (lock ? go() : false)); } catch (_) { return go(); }
  }
  function poke(fn) {
    if (!started || dead) return false;
    if (typeof fn === 'function') { pokes.push({ at: now(), fn }); if (pokes.length > 20) pokes.shift(); }
    if (leader) soon(); else post({ t: 'poke' }); evaluate(); return true;
  }

  /* ── THE OUTBOX'S DRAIN (phase 3; see the header) ── */
  const ownN = new Map();                      // name → { n, at }: written last by this computer's own press at count n (5 minutes)
  function ownHeard(keys, n) { if (!Number.isFinite(+n)) return; const t = now(); for (const k of keys || []) { const had = ownN.get(String(k)); if (!had || had.n < +n) ownN.set(String(k), { n: +n, at: t }); } for (const [k, v] of ownN) if (t - v.at > 300000) ownN.delete(k); }
  function own(keys, n) { if (!Array.isArray(keys) || !keys.length || !Number.isFinite(+n)) return; ownHeard(keys, n); post({ t: 'own', keys, n: +n }); }
  const DR = { batch: 50, batchBytes: 48000, failMax: 60000, keepalive: 60000, tick: 15000 };
  const dstats = { sent: 0, acked: 0, failed: 0, retried: 0, replayed: 0, batches: 0, keepalive: 0 };
  let draining = false, drainAgain = false, drainT = 0, drainFails = 0, drainOn = false;
  const ostore = () => (root.CharmNestStore && typeof root.CharmNestStore.outbox === 'function' ? root.CharmNestStore : null);
  const sendable = e => e && !e.job && e.state !== 'failed' && e.op;
  function bodyOf(group) {
    const h = group[0];
    if (h.batch === 'timelineAdd') return { op: 'timelineAdd', events: group.flatMap(e => (e.args && Array.isArray(e.args.events) ? e.args.events : [])) };   // (the timeline's own wire format: its events carry their ids, kept once by the server)
    return Object.assign({}, h.args || {}, { op: h.op, mid: h.mid });
  }
  // (a refusal is the cloud's own answer in words: a 4xx with its JSON; a 409 "being written already", a 401, a 5xx, a timeout or no network is no answer yet)
  const refusedBy = e => !!e && !!e.answered && !(e.data && (e.data.inFlight || e.data.retry)) && e.status !== 401 && e.status !== 408 && e.status !== 429 && !(e.status >= 500);
  function drainSoon(ms) { if (!drainOn) return; clearTimeout(drainT); drainT = setTimeout(() => { drainT = 0; drain(); }, Math.max(0, ms || 0)); }
  async function drain() {
    const St = ostore(); if (!drainOn || !St || typeof cfg.api !== 'function') return false;
    if (draining) { drainAgain = true; return false; }
    if (!online()) return false;
    const mine = St.pending(sendable); if (!mine.length) { const disk = await St.outbox().catch(() => []); if (!disk.some(sendable)) return false; }
    draining = true;
    try {
      const locks = safe(() => root.navigator && root.navigator.locks, null), run = () => drainLoop(St);
      if (locks && typeof locks.request === 'function') await locks.request('cn-outbox:' + world, run); else await run();
    } catch (e) { console.warn('outbox: drain', e); }
    finally { draining = false; if (drainAgain) { drainAgain = false; drainSoon(0); } }
    return true;
  }
  async function drainLoop(St) {
    for (let round = 0; round < 1000; round++) {
      if (!online()) return;
      const list = (await St.outbox()).filter(sendable); if (!list.length) return;
      const t = now(), head = list[0];
      // (one computer's presses land in the order they were made: one waiting out its back-off holds the ones after it)
      if ((+head.nextAt || 0) > t) { drainSoon(+head.nextAt - t); return; }
      const group = [head];
      if (head.batch) { let bytes = JSON.stringify(head.args || {}).length; for (const x of list.slice(1)) { const nb = JSON.stringify(x.args || {}).length; if (x.batch !== head.batch || x.fn !== head.fn || (+x.nextAt || 0) > t || group.length >= DR.batch || bytes + nb > DR.batchBytes) break; group.push(x); bytes += nb; } }
      for (const e of group) { e.state = 'sending'; e.tries = (+e.tries || 0) + 1; e.sentAt = t; St.touch(e); }
      dstats.sent += group.length; if (group.length > 1) dstats.batches++;
      let r = null, err = null;
      try { r = await cfg.api(head.fn || 'charmNestLibrary', bodyOf(group), { quiet: true, timeoutMs: 30000, outbox: true }); } catch (x) { err = x; }
      if (!err && r && !r.error) {
        drainFails = 0;
        if (r.replayed) dstats.replayed++;
        for (const e of group) { await St.settle(e.k, { ok: true, answer: r }); dstats.acked++; }
        if (Array.isArray(r.keys) && Number.isFinite(+r.n)) own(r.keys, r.n);
        continue;
      }
      if (!err) err = Object.assign(new Error((r && r.error) || 'the cloud sent back no answer'), { answered: !!(r && r.error), data: r });
      if (refusedBy(err)) {
        for (const e of group) { await St.settle(e.k, { error: String(err.message || 'refused').replace(/\.$/, ''), refused: true, data: err.data || null }); dstats.failed++; }
        continue;
      }
      // no answer yet: tried again after a back-off (1, 2, 4 … 60 s, a fifth either way); the network back or a tab shown tries at once
      drainFails++; dstats.retried++;
      const wait = Math.min(DR.failMax, 1000 * 2 ** Math.min(drainFails - 1, 6)) * (0.8 + Math.random() * 0.4);
      for (const e of group) { e.state = 'pending'; e.nextAt = now() + wait; e.error = String((err && err.message) || '').slice(0, 200); St.touch(e); }
      drainSoon(wait);
      return;
    }
  }
  /** The page going away: what still waits goes with keepalive (64 KB at most in all, the browser's limit); it stays in the outbox, and the next page sends it again (the mid: written once). */
  function drainLeaving() {
    const St = ostore(); if (!drainOn || !St || typeof cfg.api !== 'function') return;
    let bytes = 0;
    for (const e of St.pending(sendable)) {
      const body = bodyOf([e]), n = JSON.stringify(body).length + 200; if (bytes + n > DR.keepalive) break; bytes += n;
      dstats.keepalive++;
      try { Promise.resolve(cfg.api(e.fn || 'charmNestLibrary', body, { quiet: true, keepalive: true, outbox: true })).catch(() => {}); } catch (_) {}
    }
  }
  function drainBack() { const St = ostore(); drainFails = 0; if (St) for (const e of St.pending(sendable)) if (+e.nextAt > now()) { e.nextAt = 0; St.touch(e); } drainSoon(0); }
  function drainStart() {
    if (drainOn) return; const St = ostore(); if (!St || typeof cfg.api !== 'function') return;
    drainOn = true;
    St.onMutation(ev => { if (ev && (ev.t === 'mut' || ev.t === 'load')) drainSoon(ev.t === 'mut' && ev.from === 'tab' ? 300 : 0); });   // (another tab's press: that tab drains it; this one looks a moment later, in case that tab is gone)
    if (root.addEventListener) { root.addEventListener('online', drainBack); root.addEventListener('cn-cloud-back', drainBack); root.addEventListener('pagehide', drainLeaving); }
    if (doc && doc.addEventListener) doc.addEventListener('visibilitychange', () => { if (visible()) drainBack(); });
    setInterval(() => { const S2 = ostore(); if (S2 && !draining && S2.pending(e => sendable(e) && !(+e.nextAt > now())).length) drain(); }, DR.tick);
    Promise.resolve(St.ready && St.ready()).then(() => drainSoon(0), () => drainSoon(0));
  }
  function drainState() { const St = ostore(); return Object.assign({ waiting: St ? St.pending(sendable).length : 0, jobs: St ? St.pending(e => !!e.job).length : 0, draining, fails: drainFails }, dstats); }

  /* ── this tab ── */
  function onInput() {
    const t = now(); const was = lastInput; lastInput = t;
    if (leader) { if (t - was > PACE.activeFor) wake(); return; }
    const h = wantFast();
    if (t - actSent >= 5000 || (h && t - hurrySent >= 5000)) { actSent = t; if (h) hurrySent = t; post({ t: 'act', at: t, hurry: h || 0 }); }
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
    try { drainStart(); } catch (e) { console.warn('outbox: drain start', e); }   // (OUTBOX: the presses kept on this computer are sent whether or not the feed runs)
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
    if (isDead && !dead) { dead = true; lost(true); }
    leave(); clearTimeout(fetchT); fetchT = 0;
  }
  function state() {
    return { world, started, dead, leader, live: live(), n, cg, pace: pace(), polls: stats.polls, answers: stats.answers, heard: stats.heard, keys: stats.keys, resets: stats.resets, reads: stats.reads,
      fetches: stats.fetches, errors: stats.errors, gaps: stats.gaps, leads: stats.leads, subs: subs.size, watched: Object.keys(watched() || {}), lastOk, heardAt, me, ownSkipped: stats.ownSkipped || 0 };
  }
  const api = { start, follow, live, quietSince, watchOrder, once, poke, stop, state, PACE, drain, own, drainState };
  return api;
});
