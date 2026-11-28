/* charm-nest-sheet-live.js — SHEETLIVE: ONE live copy of every sheet on this page (Paul, 27 Nov 2026: "I've been having too many problems with the sheets and the library showing wrong old
   information, not updating, not being able to click the manual button"; "nothing can be on any computer. Everything must be grabbed from the cloud").

   What there was: one open page held several copies of the same sheet, each read and refreshed its own way, and they drifted apart: the Library's list rows and cards (LaserReview's
   records, the lists' rows), the Nest cards' pages built by charm-nest-cloud-sheets.js, the sheet window's record (W.rec), the order window's Sheet tab (kept a minute), the memory store
   (charm-nest-store.js, 'sheets' and 'sheetFull') and the "Updating…" spinner's own map (CNSheetBusy). An answer read later could stand beside an older one on another screen, and Manual
   Control refused ("The window showed an older copy of this sheet") because the window had borrowed charm objects from a Nest-card copy rebuilt in the background (MCREBIND, 27 Nov).

   What this does: exactly ONE record per sheet id, in memory only (never written to the computer: no localStorage, no IndexedDB), the newest one this page has seen from any read:
     · a whole record (getSheet / getMany full: its charms and placements) when one was read, else the list row (listSheets / laserStatus / getMany); the whole one supersedes the row of the
       same version (a row of that version brings its readiness fields in: DERIVED), and NEWER ALWAYS WINS by the store's own rule (charm-nest-store.js newerOf, NEWERWINS: the document's
       revision, else updatedAt, previewAt, layoutRev, contentRev, previewRev), whatever order the answers land in;
     · every view draws from it (each behind its own switch, window.CN_SHEETLIVE in charm-nest-1.html, read at use: off, the view goes as it went before this file);
     · it reads ONLY the sheets a view on screen watches (watch / watchSet), and only when the changes feed names them (charm-nest-sync-worker.js follow, the push counter's nudge): s:<sheet>,
       t:<set> of a watched sheet's set, p: / x: / o:<order> of a watched sheet's orders (a view that asks for them: orders), '*', a reset, a verify. Each read is by revision (getSheet ifRev:
       one field read when nothing moved; getMany ifRevs for several at once); a burst of names is one read per sheet; one read in flight per sheet, a name heard meanwhile reads exactly once
       more after it, nothing is dropped while it is busy. It keeps NO timer of its own: no polling (a failed read is tried again at most `retries` times, 2 s, 4 s, 8 s, then left to the next name).
   It writes nothing to the cloud.

   CNSheetLive (window; module.exports in tests: { create, DERIVED })
     get(id) → record | null              the one shared object (callers must NOT change it: put a new object, or mine(), to change what is shown)
     full(id) → record | null             the record when it is a whole one (charms, placements), else null (a view that needs one watches with full: a newer row then reads it)
     rev(id) · has(id) · pools(id)        its revision (the record's update time "<s>.<ns>", what an ifRev read sends) · whether it is held · its pieces' rows when a read asked pieces
     put(id, rec, { rev, full, from, why }) → the record held afterwards (rec when taken; the newer one held when rec is older: NEWERWINS)
     putRows(rows, revs) → records        a list's rows put in (revs: { id | 's:'+id: rev }), each answered with the record held for it, in order
     listRows(rows, revs) → rows          the same, each answered as a list-shaped COPY (the row, a newer row, or the row with a newer whole record's row fields): what a list draws
     mine(id, fields, { rev })            a person's own just-saved write, shown at once: a new object, the record held with those fields; only the cloud's answer at or after that write
                                          (its rev, else one newer by its counters) replaces it, so an answer read before the write never brings the old state back
     gone(id)                             the sheet is no longer in the cloud (a read said so): told once; a copy of it seen again (an archived mark, a list) is not taken, a read of it is
     watch(id, view, { full, orders }) → unwatch()   reference counted per view; unwatch(id, view) too · watchSet(view, () => ids, opts) → off: a list of cards watched as one (the Nest cards)
     subscribe(id | '*', fn) → off        fn({ id, rec, prev, full, why, from, name }) after each change of the record held (why 'order' / 'set': a name of its orders or set, the record as it was)
     read(id, { pieces, whole }) → Promise<record | null>   read now (by revision; whole: in full), shared with a read on its way; null: the cloud no longer has it; rejects when it could not be
                                          read and nothing is held (else the record held) · refresh(id): the same, its answer not awaited
     names(keys, why) · wants(key)        the feed's names (the adapter hands them here) · whether a name concerns a sheet watched
     busy(id) → '' | 'nesting' | 'placing' | 'writing files'   CNSheetBusy's rule (charm-nest-1.html SHEETBUSY) on the record held: one answer for every view
     on(view) → bool                      the view's switch (window.CN_SHEETLIVE[view] === true), read at use
     live() → bool                        the changes feed answers (CNSync.live): a view's own net timer may wait
     state()                              counts (the tests read them) */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(root, true);
  else root.CNSheetLive = factory(root, false);
})(typeof self !== 'undefined' ? self : this, function (root, node) {
  'use strict';
  /* SHEETLIVE (27 Nov 2026): the fields a server answer works out from OTHER documents (the pieces' rows, the order lines, the set: readinessRecords), so two answers of the SAME version of the
     sheet record can differ in them; a list row of that version brings them into the whole record held, nothing else (every other field of the version is the same, and a row's shapes differ:
     outputs, backs, label, sources). Paul: "every screen shows exactly what the cloud has". */
  const DERIVED = ['engraving', 'orderReadiness', 'laser'];
  /* (the fields a list row and a whole record carry in the same shape: a list drawn from the one copy keeps its rows list-shaped (their outputs, backs, label, sources as a list row has them) and
     takes these from a newer whole record, as the Library's own patchRow always took its ROW_FIELDS) */
  const ROWSAFE = ['poolIds', 'orders', 'charmCount', 'placedCount', 'rejectCount', 'density', 'freePt2', 'updatedAt', 'setId', 'setSeq', 'sheetIndex', 'draft', 'solidIncluded', 'laserDoneAt', 'laserDoneBy', 'roseCutAt', 'archived',
    'status', 'saving', 'layoutRev', 'contentRev', 'previewRev', 'previewAt', 'releaseFull', 'draftOrder', 'engraving', 'orderReadiness', 'laser'];
  const picOf = r => (r && (typeof r.preview === 'string' ? r.preview : r.outputs && r.outputs.preview && r.outputs.preview.url)) || null;
  const KINDS = 'stpxo';   // (the feed's names it follows: sheets, sets, and an order's pieces, cancel and timeline)
  const sid = v => (v == null ? '' : String(v));
  const isFull = r => !!r && Array.isArray(r.charms) && Array.isArray(r.placements);
  const docRev = r => { const m = /^(\d+)\.(\d+)$/.exec(sid(r)); return m && +m[1] > 0 ? [+m[1], +m[2]] : null; };
  /** (the cloud's revisions, "<s>.<ns>") a before b; false when either cannot be read */
  const revBefore = (a, b) => { const x = docRev(a), y = docRev(b); return !!x && !!y && (x[0] < y[0] || (x[0] === y[0] && x[1] < y[1])); };
  const ordersOf = r => (r && Array.isArray(r.orders) ? r.orders.map(sid) : []);
  const safe = (f, d) => { try { const v = f(); return v === undefined ? d : v; } catch (_) { return d; } };
  // (NOSCROLLJUMP: two records with the same content; a record that cannot be compared is not the same)
  const sameRec = (a, b) => { if (a === b) return true; try { return JSON.stringify(a) === JSON.stringify(b); } catch (_) { return false; } };

  function create(o) {
    const C = Object.assign({ api: null, now: () => Date.now(), setTimeout: (f, ms) => setTimeout(f, ms), clearTimeout: t => clearTimeout(t), newerOf: null, busyWhy: null, switches: null, live: null,
      coalesceMs: 40, retryMs: 2000, retries: 3, batch: 100, cap: 5000, timeoutMs: 15000, log: null }, o || {});
    // NEWERWINS is the store's rule, never a copy of it (charm-nest-store.js newerOf); without one (a test that gives none), the same version is all it can tell: taken, as the store did before it
    const storeNewer = () => C.newerOf || safe(() => root.CharmNestStore && root.CharmNestStore.newerOf, null) || (node ? safe(() => require('./charm-nest-store.js').newerOf, null) : null);
    const newer = (a, ra, b, rb) => { const f = storeNewer(); return f ? safe(() => f(a, ra, b, rb), 0) : 0; };
    const E = new Map();        // id → { rec, rev, full, min: { rev, strict } | null, at }: THE record of each sheet (memory only)
    const R = new Map();        // id → { inflight, again, fails, waiters, next, pieces, retryT }: its read
    const PO = new Map();       // id → { rev, pools }: the pieces' rows a read with pieces brought (charm-nest-cloud-sheets.js rebuilds a sheet from them)
    const W = new Map();        // id → Map(view → { n, full, orders })
    const DYN = new Map();      // view → { ids: () => ids, opts }
    const SUBS = new Map();     // id | '*' → Set(fn)
    const GONE = new Map();     // id → the record it had when the cloud said it was gone: a copy of it seen again later (the store's archived mark, a list) never brings it back; a read of it does
    const Q = new Set(); let qT = 0;
    const st = { puts: 0, taken: 0, older: 0, echo: 0, merged: 0, mine: 0, reads: 0, many: 0, same: 0, gone: 0, fails: 0, retries: 0, names: 0, again: 0, told: 0 };
    const now = () => C.now();
    const log = (...a) => { if (C.log) safe(() => C.log(...a)); };

    /* ── the one record of each sheet ── */
    const get = id => { const e = E.get(sid(id)); return e ? e.rec : null; };
    const full = id => { const e = E.get(sid(id)); return e && e.full ? e.rec : null; };
    const rev = id => { const e = E.get(sid(id)); return e ? e.rev || '' : ''; };
    const has = id => E.has(sid(id));
    const pools = id => { const p = PO.get(sid(id)), e = E.get(sid(id)); return p && e && (!p.rev || !e.rev || p.rev === e.rev) ? p.pools : null; };
    function tell(id, rec, prev, why, from, name) {
      const ev = { id, rec, prev, full: !!(E.get(id) && E.get(id).full), why: why || 'put', from: from || '', name: name || '' };
      for (const key of [id, '*']) for (const fn of [...(SUBS.get(key) || [])]) { st.told++; try { fn(ev); } catch (e) { log('subscriber', e); } }
    }
    function keepRoom() {
      if (E.size <= C.cap) return; const w = watched();
      for (const id of [...E.keys()]) { if (E.size <= C.cap) break; if (!w.has(id)) { E.delete(id); PO.delete(id); } }   // (the oldest-seen first, never one a view watches)
    }
    // (a whole record of the same version takes in the derived fields a row of it brings: the same object when none differs)
    function withDerived(whole, row) {
      const pick = {}; let diff = false;
      for (const k of DERIVED) if (row[k] !== undefined && JSON.stringify(row[k]) !== JSON.stringify(whole[k])) { pick[k] = row[k]; diff = true; }
      return diff ? Object.assign({}, whole, pick) : whole;
    }
    /** Keep a record the cloud answered (or a view read): the record held afterwards. */
    function put(id, rec, opt) {
      opt = opt || {}; id = sid(id || (rec && (rec.id || rec.sheetId))); if (!id || !rec || typeof rec !== 'object') return get(id);
      st.puts++;
      const e = E.get(id), r = opt.rev == null ? '' : String(opt.rev), f = opt.full != null ? !!opt.full : isFull(rec);
      if (!e && GONE.has(id)) { const g = GONE.get(id); if (opt.from !== 'read' && !(g && newer(rec, '', g, '') > 0)) { st.older++; return null; } GONE.delete(id); }   // (gone: only the cloud's own read of it, or a newer record (put back), is taken)
      if (e && e.rec === rec) { if (r && !e.min && r !== e.rev && !revBefore(r, e.rev)) e.rev = r; return rec; }   // (the very object held, put again: its revision learned, nothing told)
      // (a person's own write is held (mine): only the cloud's answer at or after that write replaces it; one read before it never brings the old state back)
      if (e && e.min) { const ok = r && e.min.rev ? (e.min.strict ? revBefore(e.min.rev, r) : !revBefore(r, e.min.rev)) : newer(rec, '', e.rec, '') > 0; if (!ok) { st.older++; return e.rec; } }   // (no revision to go by: only one newer by its counters)
      const c = !e ? 1 : e.min ? 1 : newer(rec, r, e.rec, e.rev);
      if (c < 0) { st.older++; return e.rec; }   // (NEWERWINS: the record held is newer; it stays, and every view keeps drawing it)
      /* NOSCROLLJUMP (Paul, 28 Nov 2026: "Why does the library continuously scroll on my behalf when it's being updated? I told you I don't want any part of the system to get changed when things are
         being updated"): the SAME record again (the store's echo of what a view just put here, a list read of a version already held, a read that brought nothing new) is no change: the object held
         stays, its revision is learned, and NO view is told. Every such copy was taken as a new object and told to every view, about every 5 s on an open page, and the Library drew the card again each time. */
      if (e && !e.min && sameRec(rec, e.rec)) { if (r && (!e.rev || !revBefore(r, e.rev))) e.rev = r; st.echo++; return e.rec; }
      let next = rec, nf = f, nr = r;
      if (e && c === 0 && !e.min) {
        if (e.full && !f) { next = withDerived(e.rec, rec); nf = true; nr = e.rev || r; if (next === e.rec) return e.rec; st.merged++; }   // (the same version as a list row: the whole record stays, with its readiness)
        else if (!r) nr = e.rev;   // (the same version, its revision not said: the one held kept)
      }
      E.delete(id); E.set(id, { rec: next, rev: nr, full: nf, min: null, at: now() }); keepRoom(); st.taken++;
      tell(id, next, e ? e.rec : null, opt.why || 'put', opt.from);
      // (a newer list row overtook the whole record a view needs (the sheet window, the order window): the whole one is read now, by revision)
      if (e && e.full && !nf) { const w = watchOf(id); if (w && w.full) queue(id, 'full'); }
      return next;
    }
    /** A list's rows put in, each answered LIST-SHAPED: the row itself when it is the newest, a newer row held, or the given row with the row fields of a newer whole record (ROWSAFE, its
        picture's link): always a copy, so the list may mark its rows (a picture's time, a cut) and the one copy's record is never changed. */
    function listRows(rows, revs) {
      const given = Array.isArray(rows) ? rows : [], held = putRows(given, revs);
      return held.map((h, i) => { const x = given[i]; if (!h || typeof h !== 'object' || !x || typeof x !== 'object') return h; if (h === x || !isFull(h)) return Object.assign({}, h); const o = Object.assign({}, x); for (const k of ROWSAFE) if (h[k] !== undefined) o[k] = h[k]; const pv = picOf(h); if (pv) o.preview = pv; return o; });
    }
    function putRows(rows, revs) {
      const out = [];
      for (const x of Array.isArray(rows) ? rows : []) {
        const id = sid(x && (x.id || x.sheetId)); if (!id) { out.push(x); continue; }
        const rv = revs && typeof revs === 'object' ? (revs['s:' + id] != null ? revs['s:' + id] : revs[id]) : undefined;
        out.push(put(id, x, { rev: rv, full: isFull(x), from: 'list' }) || x);
      }
      return out;
    }
    /** A person's own write, answered: shown at once (a new object), and held until the cloud's own record of it (at or after rev) is read. */
    function mine(id, fields, opt) {
      opt = opt || {}; id = sid(id); const e = E.get(id); if (!e || !fields || typeof fields !== 'object') return e ? e.rec : null;
      const r = opt.rev == null ? '' : String(opt.rev);
      if (r && e.rev && revBefore(r, e.rev)) { st.older++; return e.rec; }   // (the record held is already newer than this write's answer: it holds it, or a later change)
      const next = Object.assign({}, e.rec, fields); st.mine++;
      // (min: what replaces it: the cloud's record at or after the write's own revision; with none known, one strictly newer than the version it was made on)
      E.delete(id); E.set(id, { rec: next, rev: '', full: e.full, min: r ? { rev: r, strict: false } : { rev: e.rev || '', strict: true }, at: now() });
      tell(id, next, e.rec, 'mine', opt.from || 'mine');
      return next;
    }
    function gone(id) { id = sid(id); const e = E.get(id); if (!e && GONE.has(id)) return; st.gone++; E.delete(id); PO.delete(id); GONE.set(id, e ? e.rec : null); if (GONE.size > 2000) GONE.delete(GONE.keys().next().value); if (e) tell(id, null, e.rec, 'gone'); }   // (told once)

    /* ── what the views on screen watch ── */
    function watch(id, view, opts) {
      id = sid(id); view = sid(view) || 'view'; if (!id) return () => {};
      if (!W.has(id)) W.set(id, new Map());
      const m = W.get(id), w = m.get(view) || { n: 0, full: false, orders: false };
      w.n++; if (opts && opts.full) w.full = true; if (opts && opts.orders) w.orders = true; m.set(view, w);
      let off = false; return () => { if (off) return; off = true; unwatch(id, view); };
    }
    function unwatch(id, view) {
      id = sid(id); const m = W.get(id); if (!m) return;
      if (view == null) { W.delete(id); return; }
      const w = m.get(sid(view)); if (!w) return; if (--w.n <= 0) m.delete(sid(view)); if (!m.size) W.delete(id);
    }
    function watchSet(view, ids, opts) { view = sid(view); if (typeof ids !== 'function') return () => {}; const d = { ids, opts: opts || {} }; DYN.set(view, d); return () => { if (DYN.get(view) === d) DYN.delete(view); }; }
    // (the sheets the views watch now, with what they ask: the windows' own watches and the card lists' sets, looked at once per answer of the feed)
    function watched() {
      const out = new Map(), add = (id, w) => { id = sid(id); if (!id) return; const h = out.get(id) || { full: false, orders: false }; out.set(id, { full: h.full || !!w.full, orders: h.orders || !!w.orders }); };
      for (const [id, m] of W) for (const w of m.values()) add(id, w);
      for (const d of DYN.values()) for (const id of safe(() => d.ids() || [], [])) add(id, d.opts);
      return out;
    }
    const watchOf = id => watched().get(sid(id)) || null;
    function wants(k) {
      k = sid(k); if (k === '*') return true; const kind = k[0], v = k.slice(2); if (!v) return false;
      const w = watched(); if (!w.size) return false;
      if (kind === 's') return v === '*' || w.has(v);
      if (v === '*') return false;   // (some set or order, not known which: the views' own reads, as before; '*' and s:* reach every watched sheet)
      if (kind === 't') { for (const id of w.keys()) { const r = get(id); if (r && sid(r.setId) === v) return true; } return false; }
      if ('pxo'.includes(kind)) { for (const [id, x] of w) if (x.orders && ordersOf(get(id)).includes(v)) return true; return false; }
      return false;
    }
    /** The feed named these (why 'keys' | 'wrote' | 'dirty'; 'reset' | 'verify' | 'lost' or no names: every watched sheet is looked at by its revision). */
    function names(keys, why) {
      const list = (Array.isArray(keys) ? keys : []).map(sid), w = watched(); st.names += list.length;
      const every = !list.length || why === 'reset' || why === 'verify' || why === 'lost' || list.includes('*') || list.includes('s:*');
      if (every) for (const [id, x] of w) { if (rev(id) || W.has(id)) queue(id, why || 'every'); else void x; }   // (a card of a list held with no revision is read when it is named, never on a wildcard: as charm-nest-cloud-sheets.js sweep)
      for (const k of list) {
        const kind = k[0], v = k.slice(2); if (!v || v === '*') continue;
        if (kind === 's') { if (w.has(v)) queue(v, 'name'); continue; }
        if (kind === 't') { for (const id of w.keys()) { const r = get(id); if (r && sid(r.setId) === v) { queue(id, 'set'); tell(id, r, r, 'set', '', k); } } continue; }
        if ('pxo'.includes(kind)) for (const [id, x] of w) { if (!x.orders) continue; const r = get(id); if (r && ordersOf(r).includes(v)) { queue(id, 'order'); tell(id, r, r, 'order', '', k); } }
      }
    }

    /* ── the reads: by revision, one in flight per sheet, a burst as one ── */
    const rs = id => { let r = R.get(id); if (!r) R.set(id, r = { inflight: false, again: false, fails: 0, waiters: [], pieces: false, whole: false, retryT: 0 }); return r; };
    function queue(id, why) {
      id = sid(id); if (!id) return; const r = rs(id);
      if (r.inflight) { if (!r.again) st.again++; r.again = true; return; }   // (named while its read is out: exactly one more read once it is back)
      Q.add(id); if (!qT) qT = C.setTimeout(flush, C.coalesceMs); void why;
    }
    function flush() {
      qT = 0; const ids = [...Q]; Q.clear(); if (!ids.length) return;
      if (typeof api() !== 'function') { for (const id of ids) settle(id, get(id)); return; }
      const probe = [], one = [];
      for (const id of ids) { const r = rs(id); if (r.inflight) { r.again = true; continue; } if (needWhole(id)) r.whole = true; if (rev(id) && !r.pieces && !r.whole) probe.push(id); else one.push(id); }
      if (probe.length === 1) one.push(probe.pop());
      for (const id of one) readOne(id);
      for (let i = 0; i < probe.length; i += C.batch) readMany(probe.slice(i, i + C.batch));
    }
    const api = () => (typeof C.api === 'function' ? C.api : null);
    // (a view that needs the whole record (full) while only a list row is held: read in full, never by the row's revision, which would answer "unchanged" and leave the row)
    const needWhole = id => { const w = watchOf(id); return !!(w && w.full && has(id) && !full(id)); };
    // (a waiter is { res, rej }: a read that failed answers the record held, or its error when none is held: a view tells "could not be read" from "no longer in the cloud")
    const answer = (ws, rec, err) => { for (const w of ws) safe(() => (err && rec == null ? w.rej(err) : w.res(rec))); };
    function settle(id, rec) { const r = rs(id), ws = r.waiters; r.waiters = []; answer(ws, rec, null); }
    function done(id, ok) {
      const r = rs(id); r.inflight = false;
      if (ok) r.fails = 0;
      else if (++r.fails <= C.retries && !r.retryT) { st.retries++; r.retryT = C.setTimeout(() => { r.retryT = 0; queue(id, 'retry'); }, C.retryMs * 2 ** (r.fails - 1)); }   // (bounded: then the next name reads it)
      if (r.again) { r.again = false; queue(id, 'again'); }
    }
    async function readOne(id) {
      const r = rs(id), ws = r.waiters; r.waiters = []; r.inflight = true; st.reads++;
      const body = { op: 'getSheet', id }; if (rev(id) && !r.whole) body.ifRev = rev(id); if (r.pieces) body.pieces = true; r.pieces = false; r.whole = false;
      let out = get(id), ok = false, err = null;
      try {
        const a = await api()('charmNestLibrary', body, { quiet: true, timeoutMs: C.timeoutMs });
        if (!a || a.error) throw new Error((a && a.error) || 'no answer');
        if (a.unchanged) { st.same++; out = get(id); }
        else if (!a.sheet) { gone(id); out = null; }
        else { out = put(id, a.sheet, { rev: a.rev, full: true, from: 'read', why: 'read' }); if (a.pools) PO.set(id, { rev: a.rev ? String(a.rev) : '', pools: a.pools }); }
        ok = true;
      } catch (e) { st.fails++; err = e || new Error('no answer'); out = get(id); log('read', id, e && e.message); }
      answer(ws, out, err);
      done(id, ok);
    }
    async function readMany(ids) {
      const ws = new Map(); for (const id of ids) { const r = rs(id); ws.set(id, r.waiters); r.waiters = []; r.inflight = true; }
      st.many++; st.reads += ids.length;
      const keys = ids.map(id => 's:' + id), ifRevs = {}; for (const id of ids) if (rev(id)) ifRevs['s:' + id] = rev(id);
      const res = new Map(ids.map(id => [id, undefined])); let ok = false, err = null;
      try {
        const a = await api()('charmNestLibrary', { op: 'getMany', keys, ifRevs, full: true }, { quiet: true, timeoutMs: C.timeoutMs });
        if (!a || a.error) throw new Error((a && a.error) || 'no answer');
        for (const [k, v] of Object.entries(a.recs || {})) { const id = k.slice(2); if (v && v.rec && res.has(id)) res.set(id, put(id, v.rec, { rev: v.rev, full: true, from: 'read', why: 'read' })); }
        for (const k of a.gone || []) { const id = sid(k).slice(2); if (res.has(id)) { gone(id); res.set(id, null); } }
        ok = true;
      } catch (e) { st.fails++; err = e || new Error('no answer'); log('read many', e && e.message); }
      for (const id of ids) { if (ok && res.get(id) === undefined) st.same++; const out = res.get(id) === undefined ? get(id) : res.get(id); answer(ws.get(id) || [], out, err); done(id, ok); }
    }
    /** Read now (by revision), shared with the read on its way: a read asked while one is out is the one after it. */
    function read(id, opt) {
      id = sid(id); if (!id) return Promise.resolve(null);
      const r = rs(id); if (opt && opt.pieces) r.pieces = true; if (opt && opt.whole) r.whole = true;
      return new Promise((res, rej) => { r.waiters.push({ res, rej }); if (r.inflight) { r.again = true; return; } Q.add(id); if (qT) { C.clearTimeout(qT); qT = 0; } flush(); });
    }
    const refresh = (id, opt) => { read(id, opt).catch(() => {}); };

    /* ── the views ── */
    function subscribe(id, fn) { id = id === '*' ? '*' : sid(id); if (!id || typeof fn !== 'function') return () => {}; if (!SUBS.has(id)) SUBS.set(id, new Set()); SUBS.get(id).add(fn); return () => { const s = SUBS.get(id); if (s) { s.delete(fn); if (!s.size) SUBS.delete(id); } }; }
    const busy = id => { const f = C.busyWhy || safe(() => root.CNSheetBusy && root.CNSheetBusy.why, null); return f ? safe(() => f(get(id)) || '', '') : ''; };
    const on = view => { const s = (typeof C.switches === 'function' ? safe(C.switches, null) : C.switches) || safe(() => root.CN_SHEETLIVE, null); return !!(s && s[view] === true); };
    const live = () => (typeof C.live === 'function' ? !!safe(C.live, false) : !!safe(() => root.CNSync && root.CNSync.live(), false));
    function state() { let fulls = 0; for (const e of E.values()) if (e.full) fulls++; return Object.assign({ held: E.size, whole: fulls, watched: [...watched().keys()], queued: Q.size, inflight: [...R].filter(([, r]) => r.inflight).map(([id]) => id), subs: SUBS.size }, st); }
    return { get, full, rev, has, pools, put, putRows, listRows, mine, gone, watch, unwatch, watchSet, watched: () => [...watched().keys()], subscribe, read, refresh, names, wants, busy, on, live, state, config: o2 => Object.assign(C, o2 || {}), KINDS, DERIVED };
  }

  if (node || !root || !root.document) return { create, DERIVED, KINDS, ROWSAFE };
  /* ── the page's adapter: the page's own api(), the changes feed, the store's records, the spinner ── */
  const L = create({ api: (fn, body, opts) => (root.CN && typeof root.CN.api === 'function' ? root.CN.api(fn, body, opts) : Promise.reject(new Error('the page is not ready'))), log: (...a) => safe(() => console.warn('[sheet-live]', ...a)) });
  // (every record the store keeps of a sheet (the feed's held reads, the Library's rows, a window's whole record) reaches the one copy too: newer wins there as well)
  safe(() => { const St = root.CharmNestStore; if (St && typeof St.on === 'function') St.on(ev => { if (ev && ev.rec && (ev.kind === 'sheets' || ev.kind === 'sheetFull')) L.put(ev.id, ev.rec, { rev: safe(() => St.rev(ev.kind, ev.id), ''), full: ev.kind === 'sheetFull', from: 'store' }); }); });
  // (the feed: only names of what a view on screen watches are taken; each is read by its revision. No timer of its own: the push's nudge and the feed's own pace are all)
  safe(() => { const Y = root.CNSync; if (Y && typeof Y.follow === 'function') Y.follow({ name: 'sheet-live', kinds: KINDS, wild: 's', active: () => true, wants: k => L.wants(k), read: (why, keys, at, nm) => L.names(keys && keys.length ? keys : ['*'].concat(nm || []), why) }); });   // (no keys: the answer carried '*', a reset or a verify, and its names ride as the fourth argument (ENGLIVE): every watched sheet by its revision, and those named)
  // (SHEETBUSY: the spinner reads the one record: it is told each change, charm-nest-1.html CNSheetBusy)
  L.subscribe('*', ev => { if (ev.rec && root.CNSheetBusy && typeof root.CNSheetBusy.see === 'function') safe(() => root.CNSheetBusy.see(ev.rec, ev.id)); });
  L.create = create;
  return L;
});
