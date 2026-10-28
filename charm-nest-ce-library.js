/* charm-nest-ce-library.js — THE DESIGN LIBRARY at the Charm Editor's right hand (Paul, 28 Oct 2026: "Just like we currently do in the charm editor on our website
 * this charm editor must have a repository of all opened, modified designed pieces, and this repository must be searchable, and you must integrated on the right hand
 * side in the same way we do it in the website, charm editor. Look at how it's done on a website because we've spent a lot of time making it really efficient and fast
 * and see if you can improve it if not, just copied the implementation as long as you find it efficient. Users must be able to pull up their previous work and this
 * repository must grow with every new update or change to any charm using the charm editor.").
 *
 * One panel in the editor's right-hand slot (CharmEditor.panels), three tabs:
 *   Designs        every design in the design library, its latest version, newest first (CharmNestDesigns, charm-nest-designs.js)
 *   Charm library  the master charms (CharmNestCeMasters, charm-nest-ce-masters.js)
 *   Recent         what was opened or placed from here on this computer, newest first (kept in this browser only: a convenience, never a record)
 * Search as you type over a local index (name, SKU, order number, employee, version, id; every word must be found, a leading # is ignored), five filters (Mine, Order,
 * No order, Today, Week), tiles or a list, a click opens a design (asking first whether to keep unsaved changes), a drag puts it on the charm being edited as ONE
 * group at the size it was made, the ⊕ puts it in the middle, and the clock shows a design's versions (click one to open it, drag one onto the charm).
 *
 * WHAT CAME FROM THE STUDIO (the drawing studio's dock, LIBRARY: J 19330-19420, 20700-20990, 21640-21760; S 2666-2730; C 4357-4482, 5543-5560, 6115-6136):
 *   · one source, derived and memoised: the list is DERIVED from the data the page already holds (the designs module's local copy), rebuilt only when its signature
 *     changes (J 20700 mkLibSig), never once per renderer; it costs no read of its own and no listener of its own
 *   · the tile: a square picture on the band colour with a two-line caption (name, then a small uppercase line), badges in ink at the corners, buttons that appear on
 *     hover (C 4410-4462); a waiting tile is paper, not a hole, so nothing moves when the picture arrives (J 20500 MK_THUMB_WAIT)
 *   · pictures asked for only when a tile is in view, a few lanes at a time, the nearest first, the answer posted straight into the one <img> that wanted it with no
 *     re-render (J 20540-20620); a short memory of the pictures already shown (J 20480 MK_SKETCH_CAP)
 *   · older work arrives by itself when the list is scrolled near its end (J 21661-21678: "a button that says 'there is more' underneath a list you were already
 *     scrolling is a button asking you to confirm what you just did"); a filter that would show nothing says so rather than vanishing (J 20907-20917)
 *   · one delegated listener for every tile (J 27066 "ONE LISTENER, NOT THREE HUNDRED"); press and drag to place with an 8 px slop, a ghost that follows the hand and
 *     a work area that lights up under it (J 21681-21760); on a touch screen a vertical start is the list scrolling, a sideways start is a drag
 * WHAT IS BETTER HERE (the studio's is built for a hundred and fifty designs; this one is for thousands):
 *   · a VIRTUAL list: only the tiles on screen (and a row either side) exist, about thirty elements whatever the count, recycled as the list scrolls. The studio built
 *     every tile in slices of eighteen a frame (J 20862 mkGridPaint): 3,000 designs were 170 frames of building and 3,000 elements to lay out, again on every keystroke
 *   · search narrows instead of starting over: each row carries one lower-case line made when the index is built (the studio lower-cased every title on every
 *     keystroke and sorted again, J 20832), and a query that only adds letters searches the last answer, not everything; the order is set once, at build time
 *   · a picture is asked for only after its tile has stayed in view for a moment (a fast scroll past a thousand designs asks for none of them), and is kept on this
 *     computer by the page's picture store (charm-nest-thumbs.js, IndexedDB) under the design's id and VERSION, which never change: a reload, a new tab or a bad
 *     connection shows it with no request at all
 *   · several words, each found anywhere (a name and an order number: "heart 4213"), and the filters an employee needs (Mine, Order, No order, Today, Week)
 *   · a list view beside the tiles (one design a row, three lines: more of them on screen when looking for an order or a person), and the keys: ↓ from the search into
 *     the list, the arrows through it, Enter opens, a letter goes back to the search, ⌘Z / ⌘S … still reach the editor
 * Measured (tests/charm-nest/ce-library.cjs, 3,000 designs): a keystroke 2 ms of work (most 7), a scroll 2 ms (most 5), ~14 elements in the list.
 *
 * READS (guarded: a module that is not on the page leaves its tab saying so):
 *   CharmNestDesigns   rows() (this computer's index, newest first: the list shows at once), list() (the same after asking the cloud only what was saved since), get(id,
 *                      version, { history }) -> the design whole (history: its versions, newest first), put(design), onChange(fn) ({ kind, row? }). A row: { id, name,
 *                      version, thumbUrl, widthMm, heightMm, orderId, customName, by, at, source, hash, orders, createdBy }
 *   CharmNestCeMasters search('', { limit, recent: false }) (or list() / index()): every master as { sku, name, thumbUrl, widthMm, heightMm, file }; onChange(fn) when the
 *                      library has come; open(sku) and openDesign(design): the editor in design mode, whose Save makes a version (theirs: the fonts, the save, the card
 *                      after it); faces?(sku) -> { front, back? } to place a master (else the same reading of a master the masters module does: Master, Pool.masterCharm,
 *                      its model.masterFace); current() (the design the editor holds, so its tile is marked)
 *   CharmEditor        panels (registered here), current.dirty(), current.end(keep), current.opts (the editor's read-outs: this file asks "keep your changes?" itself, then
 *                      closes the session as chosen), ctx (the extension points: addItems, commit, select, items, face, frame, pointOf, geo, model, say, stage)
 * OPENING: the session is closed as the person chose (kept: exactly as its Save, so whoever opened it gets its result; left: as Cancel, nothing asked again), then the
 * masters module opens the design or master in design mode. Without it, this opens the editor itself with the same fonts and opts.design = { id, version, …, keptBy:
 * 'library' }, and puts the version when it is saved (a master: a new design whose source is that master). */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === 'object' && module.exports) module.exports = api; else root.CharmNestCeLibrary = api;
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const doc = root && root.document ? root.document : null;
  const MM = 25.4 / 72, DAY = 86400000;
  const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const lc = s => String(s == null ? '' : s).toLowerCase();
  const now = () => Date.now();
  const perf = () => (root.performance && root.performance.now ? root.performance.now() : Date.now());

  /* ═══ 1 · the index: rows made once for each change of the data, searched on every keystroke ═══ */
  /** Milliseconds of a stamp in any of the shapes the page meets: a number, a Date, a Firestore Timestamp ({seconds} / {_seconds} / toMillis), an ISO string. */
  function msOf(v) {
    if (v == null || v === '') return 0;
    if (typeof v === 'number') return Number.isFinite(v) ? (v < 1e11 ? v * 1000 : v) : 0;
    if (v instanceof Date) return v.getTime() || 0;
    if (typeof v.toMillis === 'function') { try { return v.toMillis() || 0; } catch (_) { return 0; } }
    if (typeof v === 'object') { const s = v.seconds != null ? v.seconds : v._seconds; return s != null ? +s * 1000 + Math.round((+(v.nanoseconds != null ? v.nanoseconds : v._nanoseconds) || 0) / 1e6) : 0; }
    const t = Date.parse(v); return Number.isFinite(t) ? t : 0;
  }
  /** An order id that is no order: none, or the id an orderless custom piece is given (D4, 28 Oct 2026: CUSTOM-<yyyymmdd>-<n>). */
  const noOrder = o => !o || /^CUSTOM-/i.test(String(o));
  const initials = s => { const w = String(s || '').replace(/[^\p{L}\p{N} ]+/gu, ' ').trim().split(/\s+/).filter(Boolean); return ((w[0] || '?')[0] + (w[1] ? w[1][0] : (w[0] || '')[1] || '')).toUpperCase(); };

  function designRow(d) {
    const id = String(d.id), version = Math.max(1, Math.round(+d.version || 1)), orderId = d.orderId ? String(d.orderId) : '';
    const src = d.source && typeof d.source === 'object' ? d.source : {};
    const sku = String(src.sku || d.sku || ''), by = String(d.by || ''), customName = d.customName ? String(d.customName) : '', made = String(d.createdBy || '');
    const name = String(d.name || customName || '').trim() || 'Untitled design', orders = Array.isArray(d.orders) ? d.orders.filter(o => o && o !== orderId).join(' ') : '';
    return {
      key: 'd:' + id, kind: 'design', id, version, name, sku, orderId, customName, by, byLc: lc(by).trim(), madeLc: lc(made).trim(), at: msOf(d.at || d.updatedAt),
      custom: noOrder(orderId), thumbUrl: d.thumbUrl ? String(d.thumbUrl) : '', widthMm: +d.widthMm || 0, heightMm: +d.heightMm || 0, hash: d.hash || '', source: src,
      hay: lc([name, customName, sku, orderId, orders, by, made !== by ? made : '', src.file || '', 'v' + version, id].join(' ')), pic: 'd|' + id + '|' + version
    };
  }
  function masterRow(m) {
    const sku = String(m.sku || m.id || '').trim(), name = String(m.name || m.title || '').trim() || sku;
    return {
      key: 'm:' + sku, kind: 'master', id: sku, version: 0, name, sku, orderId: '', customName: '', by: '', byLc: '', madeLc: '', at: msOf(m.at), custom: false,
      thumbUrl: m.thumbUrl ? String(m.thumbUrl) : '', widthMm: +m.widthMm || 0, heightMm: +m.heightMm || 0, category: m.category ? String(m.category) : '',
      hay: lc([name, sku, m.category || '', m.file || ''].join(' ')), pic: m.picKey ? String(m.picKey) : m.thumbUrl ? 'ms|' + sku + '|' + m.thumbUrl : 'ms|' + sku
    };
  }
  /** The words of a query: lower case, split on spaces, a leading # dropped (an order number typed as "#4213"). */
  const tokens = q => lc(q).split(/\s+/).map(t => t.replace(/^#+/, '')).filter(Boolean);
  const startOfDay = t => { const d = new Date(t); d.setHours(0, 0, 0, 0); return d.getTime(); };
  const startOfWeek = t => { const d = new Date(startOfDay(t)); d.setDate(d.getDate() - ((d.getDay() + 6) % 7)); return d.getTime(); };   // (Monday)
  /**
   * The rows that answer a query, as indices into ix.rows in the index's own order (newest first for designs). f: { mine, order: '' | 'with' | 'none', when: '' |
   * 'today' | 'week' }. A query that only ADDS letters to the last one (same index, same filters) searches the last answer: every word of the new query contains a word
   * of the old, so nothing outside the old answer can match. Returns { list (Int32Array), and what the next call needs to narrow }.
   */
  function query(ix, q, f, me, t, prev) {
    f = f || {};
    const toks = tokens(q), qn = toks.join(' '), meLc = lc(me).trim();
    const t0 = f.when === 'today' ? startOfDay(t || now()) : f.when === 'week' ? startOfWeek(t || now()) : -Infinity;
    const fkey = [f.mine ? meLc || '\u0000' : '', f.order || '', f.when ? t0 : ''].join('|');
    const src = prev && prev.ix === ix && prev.fkey === fkey && qn.startsWith(prev.qn) ? prev.list : null;
    const rows = ix.rows, N = src ? src.length : rows.length, nt = toks.length, out = new Int32Array(N);
    let n = 0;
    outer: for (let j = 0; j < N; j++) {
      const i = src ? src[j] : j, r = rows[i];
      if (f.mine && (!meLc || (r.byLc !== meLc && r.madeLc !== meLc))) continue;
      if (f.order === 'with' && r.custom) continue;
      if (f.order === 'none' && !r.custom) continue;
      if (r.at < t0) continue;
      for (let k = 0; k < nt; k++) if (r.hay.indexOf(toks[k]) < 0) continue outer;
      out[n++] = i;
    }
    return { ix, fkey, qn, list: out.subarray(0, n), narrowed: !!src };
  }
  /** How many rows each filter would leave with the words typed (the chips say it before they are pressed, as the studio's menu items do, J 20907). */
  function counts(ix, q, me, t) {
    const toks = tokens(q), meLc = lc(me).trim(), d0 = startOfDay(t || now()), w0 = startOfWeek(t || now());
    const c = { all: 0, mine: 0, with: 0, none: 0, today: 0, week: 0 };
    outer: for (const r of ix.rows) {
      for (const k of toks) if (r.hay.indexOf(k) < 0) continue outer;
      c.all++; if (meLc && (r.byLc === meLc || r.madeLc === meLc)) c.mine++; if (r.custom) c.none++; else c.with++; if (r.at >= d0) c.today++; if (r.at >= w0) c.week++;
    }
    return c;
  }

  /* ═══ 2 · the sources: three tabs over the data the page already holds ═══ */
  const Designs = () => root.CharmNestDesigns || null;
  const Masters = () => root.CharmNestCeMasters || null;
  const summaries = r => (Array.isArray(r) ? r : r && Array.isArray(r.designs) ? r.designs : []);
  const watchers = new Set();
  const changed = which => { for (const fn of watchers) { try { fn(which); } catch (_) { /* a closed panel */ } } };

  /* designs: the design library's own index on this computer (shown at once), then what the cloud says was saved since; a page of older ones only if the module pages */
  const DS = { byId: new Map(), ix: { rows: [], sig: '' }, next: null, loading: null, loaded: false, error: '', sub: false, dirty: false };
  function designsMerge(list) {
    for (const d of list) { if (!d || d.id == null) continue; const r = designRow(d), had = DS.byId.get(r.id); if (!had || r.version > had.version || (r.version === had.version && r.at >= had.at)) DS.byId.set(r.id, r); }
    DS.dirty = true;
  }
  /** The module's index as it stands (no call): taken whole, so a design the module no longer holds (a sandbox reset) goes too. */
  function designsRows(D) { try { const rows = D.rows(); if (Array.isArray(rows)) { DS.byId.clear(); designsMerge(rows); } } catch (_) { /* as it was */ } }
  function designsIndex() {
    if (!DS.dirty) return DS.ix;
    const rows = [...DS.byId.values()].sort((a, b) => b.at - a.at || (a.id < b.id ? -1 : 1));
    DS.ix = { rows, sig: rows.length + '|' + (rows[0] ? rows[0].key + rows[0].version + rows[0].at : '') }; DS.dirty = false;
    return DS.ix;
  }
  function designsLoad(more, fresh) {
    const D = Designs(); if (!D || typeof D.list !== 'function') return null;
    if (!DS.sub && typeof D.onChange === 'function') {
      DS.sub = true;
      // (the module's copy changed: a save (its row comes with it), another tab, the cloud's answer. Coalesced, as the studio's sessionsViewsSoon, J 1556)
      let due = 0;
      try {
        D.onChange(x => {
          if (x && x.row) designsMerge([x.row]); else if (typeof D.rows === 'function') DS.reread = true; else { const list = summaries(x); if (list.length) designsMerge(list); else DS.reread = true; }
          if (due) return;
          due = setTimeout(() => { due = 0; if (DS.reread) { DS.reread = false; if (typeof D.rows === 'function') designsRows(D); else { DS.loaded = false; designsLoad(); return; } } changed('designs'); }, 60);
        });
      } catch (_) { /* no change feed: the list is as loaded */ }
    }
    if (!more && !DS.byId.size && typeof D.rows === 'function') { designsRows(D); if (DS.byId.size) changed('designs'); }
    if (DS.loading) return DS.loading;
    if (more && !DS.next) return null;
    if (!more && DS.loaded && !fresh) return null;
    DS.loading = Promise.resolve().then(() => D.list(more ? { cursor: DS.next } : {})).then(r => {
      designsMerge(summaries(r)); DS.next = r && !Array.isArray(r) && r.next ? r.next : null; DS.loaded = true; DS.error = '';
    }, e => { DS.error = (e && e.message) || String(e); DS.loaded = true; }).finally(() => { DS.loading = null; changed('designs'); });
    return DS.loading;
  }
  const designFull = new Map();                              // 'id|version' -> Promise<design>: a design asked for once (a drag starts asking before the drop)
  function designGet(id, version) {
    const k = id + '|' + (version || ''); let p = designFull.get(k);
    if (!p) {
      const D = Designs(); if (!D || typeof D.get !== 'function') return Promise.reject(new Error('The design library is not on this page.'));
      p = Promise.resolve().then(() => D.get(id, version || undefined)).then(r => { const d = r && r.design ? r.design : r; if (!d || !d.faces) throw new Error('That design has no drawing.'); return d; });
      p.catch(() => designFull.delete(k));
      designFull.set(k, p); if (designFull.size > 40) designFull.delete(designFull.keys().next().value);
    }
    return p;
  }

  /* masters: the master charms as the masters module lists them */
  const MS = { ix: { rows: [], sig: '' }, bySku: new Map(), loading: null, loaded: false, error: '', sub: false };
  function mastersLoad(force) {
    const M = Masters(); if (!M) return null;
    // (every master, A to Z, as the module holds them: its search with no words and no limit; nothing is asked of the cloud per keystroke, the panel searches its own copy)
    const fn = typeof M.list === 'function' ? () => M.list() : typeof M.index === 'function' ? () => M.index() : typeof M.search === 'function' ? () => M.search('', { limit: 1e7, recent: false }) : null;
    if (!fn) return null;
    if (!MS.sub && typeof M.onChange === 'function') { MS.sub = true; try { M.onChange(() => { MS.loaded = false; mastersLoad(true); }); } catch (_) { /* fine */ } }
    if (MS.loading) return MS.loading;
    if (MS.loaded && !force) return null;
    MS.loading = Promise.resolve().then(fn).then(list => {
      const rows = [], by = new Map();
      for (const m of Array.isArray(list) ? list : []) { if (!m) continue; const r = masterRow(m); if (!r.sku || by.has(r.sku)) continue; by.set(r.sku, r); rows.push(r); }
      rows.sort((a, b) => (a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : 0));
      MS.ix = { rows, sig: rows.length + '|' + (rows[0] ? rows[0].sku : '') }; MS.bySku = by; MS.loaded = true; MS.error = '';
    }, e => { MS.error = (e && e.message) || String(e); MS.loaded = true; }).finally(() => { MS.loading = null; changed('masters'); });
    return MS.loading;
  }

  /* recent: what was opened or placed from this panel on this computer (a convenience kept in this browser, never a record) */
  const RECENT_KEY = 'cn.ceLib.recent', RECENT_MAX = 60;
  let recentList = null, recentIx = { rows: [], sig: '' }, recentFor = '';
  function recentRead() {
    if (recentList) return recentList;
    try { const v = JSON.parse((root.localStorage && root.localStorage.getItem(RECENT_KEY)) || '[]'); recentList = Array.isArray(v) ? v.filter(x => x && x.k) : []; } catch (_) { recentList = []; }
    return recentList;
  }
  function recentPush(row, version, what) {
    const list = recentRead().filter(x => x.k !== row.key);
    list.unshift({ k: row.key, kind: row.kind, id: row.id, v: version || row.version || 0, n: row.name, o: row.orderId || '', at: now(), w: what || 'opened' });
    recentList = list.slice(0, RECENT_MAX); recentFor = '';
    try { root.localStorage && root.localStorage.setItem(RECENT_KEY, JSON.stringify(recentList)); } catch (_) { /* storage blocked: this visit only */ }
    changed('recent');
  }
  function recentIndex() {
    const list = recentRead(), sig = list.length + '|' + (list[0] ? list[0].k + list[0].at : '') + '|' + DS.ix.sig + '|' + MS.ix.sig;
    if (sig === recentFor) return recentIx;
    const rows = list.map(x => {
      const base = x.kind === 'master' ? MS.bySku.get(x.id) : DS.byId.get(x.id);
      const r = base ? Object.assign({}, base) : (x.kind === 'master' ? masterRow({ sku: x.id, name: x.n }) : designRow({ id: x.id, name: x.n, version: x.v, orderId: x.o }));
      r.at = x.at; r.recent = x.w; r.hay = r.hay + ' ' + lc(x.w);
      return r;
    });
    recentIx = { rows, sig }; recentFor = sig;
    return recentIx;
  }

  /* ═══ 3 · pictures: asked for only once a tile has stayed in view, kept by the page's picture store, posted into the one <img> that wants it ═══ */
  const SETTLE_MS = 90, LANES = 4, SHOWN_MAX = 900;
  const shown = new Map();                                   // picture key -> address already shown on this page (most recent last)
  const shownPut = (k, u) => { shown.delete(k); shown.set(k, u); while (shown.size > SHOWN_MAX) shown.delete(shown.keys().next().value); };
  const shownGet = k => { const u = shown.get(k); if (u) { shown.delete(k); shown.set(k, u); } return u || ''; };
  const stats = { asked: 0, kept: 0 };
  function picFor(row) {
    const T = root.CharmNestThumbs;
    if (row.thumbUrl) {
      stats.asked++;
      // (a stored picture is read once and kept on this computer under the design's id and version, which never change, so it is never asked for again)
      if (T && typeof T.picture === 'function' && typeof T.fetchDataUrl === 'function') return T.picture(row.pic, () => T.fetchDataUrl(row.thumbUrl)).then(u => u || row.thumbUrl, () => row.thumbUrl);
      return new Promise(res => { const im = new root.Image(); im.decoding = 'async'; im.onload = im.onerror = () => res(row.thumbUrl); im.src = row.thumbUrl; });
    }
    const M = Masters();
    if (row.kind === 'master' && M && typeof M.thumb === 'function') { stats.asked++; return Promise.resolve().then(() => M.thumb(row.sku)).then(u => (typeof u === 'string' ? u : ''), () => ''); }
    return Promise.resolve('');
  }

  /* ═══ 4 · the look: the editor's own tokens (the panel sits inside .ce-root), the studio's library tiles (C 4357-4482) ═══ */
  const SV = (d, extra) => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"${extra || ''}>${d}</svg>`;
  const IC = {
    lib: SV('<rect x="3.5" y="3.5" width="7" height="7" rx=".6"/><rect x="13.5" y="3.5" width="7" height="7" rx=".6"/><rect x="3.5" y="13.5" width="7" height="7" rx=".6"/><path d="M17 13.5v7M13.5 17h7"/>'),
    search: SV('<circle cx="10.5" cy="10.5" r="6.5"/><path d="m15.5 15.5 5 5"/>'),
    x: SV('<path d="M6 6l12 12M18 6 6 18"/>'),
    clock: SV('<circle cx="12" cy="12" r="8.5"/><path d="M12 7.5V12l3 2"/>'),
    place: SV('<rect x="3.5" y="3.5" width="17" height="17" rx="1"/><path d="M12 8v8M8 12h8"/>'),
    tiles: SV('<rect x="4" y="4" width="7" height="7"/><rect x="13" y="4" width="7" height="7"/><rect x="4" y="13" width="7" height="7"/><rect x="13" y="13" width="7" height="7"/>'),
    list: SV('<rect x="4" y="5" width="4" height="4"/><path d="M11 7h9"/><rect x="4" y="15" width="4" height="4"/><path d="M11 17h9"/>'),
    back: SV('<path d="M15 5 8 12l7 7"/>')
  };
  const CSS = `
.cel{ flex:1 1 auto; min-height:0; height:100%; display:flex; flex-direction:column; position:relative; background:#fff; color:var(--ink,#1c1d1d); }
.cel-tabs{ flex:none; display:flex; align-items:stretch; border-bottom:1px solid var(--hair); }
.cel-tab{ flex:1 1 auto; min-width:0; border:0; background:none; cursor:pointer; padding:11px 4px 10px; font-size:9.5px; font-weight:700; letter-spacing:.13em; text-transform:uppercase; color:var(--muted);
  border-bottom:2px solid transparent; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; transition:background var(--in), color var(--in); }
.cel-tab.is-on{ color:var(--ink); border-bottom-color:var(--gold); }
.cel-tab:not(.is-on):hover{ background:var(--hov); color:var(--ink); border-bottom-color:var(--gold-lt); }
.cel-tab b{ display:inline-block; margin-left:5px; padding:1px 4px; min-width:14px; background:var(--ink); color:#fff; font-size:8.5px; font-weight:700; letter-spacing:.03em; vertical-align:1px; }
.cel-tab.is-on b{ background:var(--gold); }
.cel-head{ flex:none; padding:9px 9px 7px; border-bottom:1px solid var(--hair); }
.cel-search{ position:relative; display:flex; }
.cel-search > svg{ position:absolute; left:9px; top:50%; transform:translateY(-50%); width:14px; height:14px; color:var(--muted); pointer-events:none; }
.cel-search input{ flex:1 1 auto; min-width:0; height:34px; border:1px solid var(--hair); background:#fff; padding:0 30px 0 30px; font-family:inherit; font-size:12.5px; color:var(--ink); outline:none; }
.cel-search input:focus{ border-color:var(--ink); }
.cel-search input::placeholder{ color:#9a9690; }
.cel-x{ position:absolute; right:3px; top:3px; width:28px; height:28px; border:0; background:none; cursor:pointer; color:var(--muted); display:inline-flex; align-items:center; justify-content:center; padding:0; }
.cel-x svg{ width:13px; height:13px; } .cel-x:hover{ background:var(--hov); color:var(--ink); }
.cel-bar{ display:flex; align-items:center; gap:4px; margin-top:7px; min-width:0; }
.cel-chips{ flex:1 1 auto; min-width:0; display:flex; gap:3px; overflow-x:auto; scrollbar-width:none; }
.cel-chips::-webkit-scrollbar{ display:none; }
.cel-chip{ flex:none; height:26px; padding:0 7px; border:1px solid var(--hair); background:#fff; cursor:pointer; font-size:8.5px; font-weight:700; letter-spacing:.09em; text-transform:uppercase; color:var(--muted);
  transition:border-color var(--in), background var(--in), color var(--in); }
.cel-chip:not(.is-on):not(:disabled):hover{ background:var(--hov); border-color:var(--hov-line); color:var(--ink); }
.cel-chip.is-on{ background:var(--ink); border-color:var(--ink); color:#fff; }
.cel-chip:disabled{ opacity:.38; cursor:default; }
.cel-view{ flex:none; width:26px; height:26px; border:1px solid var(--hair); background:#fff; cursor:pointer; color:var(--muted); display:inline-flex; align-items:center; justify-content:center; padding:0; }
.cel-view svg{ width:14px; height:14px; } .cel-view:hover{ background:var(--hov); color:var(--ink); }
.cel-count{ display:flex; justify-content:space-between; gap:8px; margin-top:6px; font-size:9px; letter-spacing:.11em; text-transform:uppercase; color:var(--muted); white-space:nowrap; min-height:12px; }
.cel-count span{ overflow:hidden; text-overflow:ellipsis; }
.cel-list{ flex:1 1 auto; min-height:0; overflow-y:auto; overflow-x:hidden; position:relative; outline:none; overscroll-behavior:contain; contain:strict; touch-action:pan-y; }
.cel-list:focus-visible{ box-shadow:inset 0 0 0 2px var(--gold); }
.cel-sizer{ width:1px; height:0; }
.cel-cell{ position:absolute; left:0; top:0; width:var(--cw,120px); height:var(--ch,160px); margin:0; background:#fff; cursor:pointer; overflow:hidden; contain:layout paint style;
  user-select:none; -webkit-user-select:none; transition:border-color var(--in), box-shadow var(--in), background var(--in); }
.cel-cell.is-free{ visibility:hidden; }
.cel-pic{ position:relative; display:block; background:var(--band); overflow:hidden; flex:none; }
.cel-pic img{ position:absolute; inset:6%; width:88%; height:88%; object-fit:contain; pointer-events:none; }
.cel-pic img:not([src]){ display:none; }
.cel-ph{ position:absolute; inset:0; display:flex; align-items:center; justify-content:center; font-style:normal; font-family:var(--serif); font-size:26px; color:#d9d2c2; letter-spacing:.04em; pointer-events:none; }
.cel-cell.is-pic .cel-ph{ display:none; }
.cel-tx{ display:block; min-width:0; }
.cel-nm{ display:block; font-size:11.5px; font-weight:600; color:var(--ink); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cel-nm{ line-height:16px; }
.cel-s1,.cel-s2{ display:block; font-size:8.5px; line-height:11px; letter-spacing:.09em; text-transform:uppercase; color:var(--muted); margin-top:2px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cel-s1.is-custom{ color:var(--gold); font-weight:700; }
.cel-v{ position:absolute; left:5px; top:5px; z-index:1; background:var(--ink); color:#fff; font-size:8px; font-weight:700; letter-spacing:.12em; padding:3px 5px; line-height:1; pointer-events:none; }
.cel-v:empty{ display:none; }
.cel-cell.is-cur .cel-v{ background:var(--gold); }
.cel-b{ position:absolute; right:5px; z-index:1; width:26px; height:26px; border:1px solid var(--hair); background:rgba(255,255,255,.95); cursor:pointer; color:var(--ink); display:inline-flex; align-items:center; justify-content:center; padding:0;
  opacity:0; transition:opacity var(--in), border-color var(--in); }
.cel-b svg{ width:14px; height:14px; } .cel-b:hover{ border-color:var(--ink); }
.cel-b--place{ top:5px; } .cel-b--hist{ top:35px; }
.cel-cell:hover .cel-b,.cel-cell.is-act .cel-b,.cel-b:focus-visible{ opacity:1; }
.cel-cell.is-nohist .cel-b--hist{ display:none; }
/* tiles: two to a row, as the studio's library (C 4404-4462) */
.cel.is-tiles .cel-cell{ display:flex; flex-direction:column; border:1px solid var(--hair); }
.cel.is-tiles .cel-cell .cel-pic{ width:100%; height:var(--pw,110px); }
.cel.is-tiles .cel-cell .cel-tx{ padding:5px 7px 6px; border-top:1px solid var(--hair); }
.cel.is-tiles .cel-cell:hover,.cel.is-tiles .cel-cell.is-act{ border-color:var(--ink); box-shadow:0 6px 18px rgba(28,29,29,.12); }
/* list: one to a row, a small picture and three lines (more of them on screen, for finding by order or by person) */
.cel.is-list .cel-cell{ display:flex; align-items:center; gap:9px; padding:0 66px 0 8px; border-bottom:1px solid var(--hair); }
.cel.is-list .cel-cell .cel-pic{ width:46px; height:46px; border:1px solid var(--hair); }
.cel.is-list .cel-cell .cel-ph{ font-size:15px; }
.cel.is-list .cel-cell .cel-v{ left:auto; right:8px; top:50%; transform:translateY(-50%); }
.cel.is-list .cel-cell .cel-b{ top:50%; transform:translateY(-50%); right:36px; width:24px; height:24px; } .cel.is-list .cel-cell .cel-b--place{ right:66px; display:none; }
.cel.is-list .cel-cell:hover,.cel.is-list .cel-cell.is-act{ background:var(--hov); }
.cel-cell.is-cur{ box-shadow:inset 3px 0 0 0 var(--gold); }
.cel-cell.is-lift{ opacity:.45; }
.cel-empty{ position:absolute; left:0; right:0; top:0; padding:26px 16px; text-align:center; font-size:12px; line-height:1.7; color:var(--muted); }
.cel-empty b{ color:var(--ink); }
.cel-more{ position:absolute; left:0; right:0; height:30px; display:flex; align-items:center; justify-content:center; font-size:9px; letter-spacing:.14em; text-transform:uppercase; color:var(--muted); }
/* the picture that follows the hand to the charm (C 4474-4481) */
.cel-ghost{ position:fixed; z-index:60; left:0; top:0; width:92px; height:92px; transform:translate(-50%,-50%); pointer-events:none; background:#fff; border:1px solid var(--ink); box-shadow:0 14px 34px rgba(28,29,29,.3);
  opacity:.88; display:flex; align-items:center; justify-content:center; padding:6px; }
.cel-ghost img{ max-width:100%; max-height:100%; object-fit:contain; }
.cel-ghost i{ font-style:normal; font-size:10px; font-weight:700; letter-spacing:.06em; text-align:center; color:var(--ink); }
.cel-ghost.is-over{ opacity:1; border:2px solid var(--gold); }
.ce-stage.cel-over{ outline:2px dashed var(--gold); outline-offset:-4px; }
/* a design's versions, over the list */
.cel-hist{ position:absolute; inset:0; z-index:5; display:flex; flex-direction:column; background:#fff; }
.cel-hist[hidden]{ display:none; }
.cel-hist__head{ flex:none; display:flex; align-items:center; gap:6px; padding:8px 9px; border-bottom:1px solid var(--hair); min-width:0; }
.cel-hist__back{ flex:none; width:30px; height:30px; border:1px solid var(--hair); background:#fff; cursor:pointer; color:var(--ink); display:inline-flex; align-items:center; justify-content:center; padding:0; }
.cel-hist__back svg{ width:15px; height:15px; } .cel-hist__back:hover{ background:var(--hov); }
.cel-hist__t{ min-width:0; flex:1 1 auto; }
.cel-hist__t b{ display:block; font-family:var(--serif); font-weight:500; font-size:18px; line-height:1.1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cel-hist__t span{ display:block; font-size:8.5px; letter-spacing:.12em; text-transform:uppercase; color:var(--muted); margin-top:3px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.cel-hist__list{ flex:1 1 auto; min-height:0; overflow-y:auto; padding:6px; }
.cel-vrow{ position:relative; display:flex; align-items:center; gap:9px; width:100%; min-height:56px; padding:4px 6px; margin:0 0 3px; border:1px solid var(--hair); background:#fff; cursor:pointer; text-align:left;
  font:inherit; color:var(--ink); user-select:none; -webkit-user-select:none; }
.cel-vrow:hover{ border-color:var(--ink); background:var(--hov); }
.cel-vrow.is-cur{ box-shadow:inset 3px 0 0 0 var(--gold); }
.cel-vrow .cel-pic{ width:44px; height:44px; border:1px solid var(--hair); }
.cel-vrow .cel-ph{ font-size:12px; }
.cel-vrow.is-pic .cel-ph{ display:none; }
.cel-vrow .cel-tx{ flex:1 1 auto; }
.cel-vrow .cel-nm{ font-size:12px; }
.cel-ask3 .ce-ask__card{ width:min(480px,100%); }
.cel-ask3 .ce-ask__b{ flex-wrap:wrap; row-gap:8px; }
@media (max-width:640px){ .cel-b{ opacity:1; } }
@media (prefers-reduced-motion: reduce){ .cel-cell,.cel-tab,.cel-chip,.cel-b{ transition:none; } }
`;
  function injectCss() {
    if (!doc || doc.getElementById('celCss')) return;
    const s = doc.createElement('style'); s.id = 'celCss'; s.textContent = CSS; (doc.head || doc.documentElement).appendChild(s);
  }
  let fmtD = null, fmtDY = null, fmtT = null;
  /** When, short: the time for today, the day and month this year, the year too before that (the studio's mkLibDate, J 20840, with the time added for today). */
  function when(t) {
    if (!t) return '';
    try {
      if (!fmtD) { fmtD = new Intl.DateTimeFormat(undefined, { day: 'numeric', month: 'short' }); fmtDY = new Intl.DateTimeFormat(undefined, { day: 'numeric', month: 'short', year: 'numeric' }); fmtT = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' }); }
      const n = now(); if (t >= startOfDay(n)) return fmtT.format(t);
      return new Date(t).getFullYear() === new Date(n).getFullYear() ? fmtD.format(t) : fmtDY.format(t);
    } catch (_) { return ''; }
  }
  const me = () => {
    try { if (root.CNEmployee && typeof root.CNEmployee.name === 'function') return String(root.CNEmployee.name() || '').trim(); } catch (_) { /* below */ }
    try { return typeof employeeName === 'function' ? String(employeeName() || '').trim() : ''; } catch (_) { return ''; }   // eslint-disable-line no-undef
  };
  const log = (kind, data) => { try { if (root.CNDesignLog && typeof root.CNDesignLog.event === 'function') root.CNDesignLog.event(kind, data); } catch (_) { /* the log never stops the work */ } };

  /* ═══ 5 · the panel ═══ */
  // what survives a design being opened (the workspace closes and a new one opens): the tab, the words, the filters, the view, where each tab was scrolled to
  const prefs = { tab: 'designs', q: '', f: { mine: false, order: '', when: '' }, view: 'tiles', top: {} };
  try { const v = root.localStorage && root.localStorage.getItem('cn.ceLib.view'); if (v === 'list' || v === 'tiles') prefs.view = v; } catch (_) { /* the default */ }
  let S = null;                                              // the mounted panel, one at a time (one editor at a time)

  function mount(ctx, el) {
    if (!doc) return;
    if (S) unmount();
    injectCss();
    const box = doc.createElement('div');
    box.className = 'cel is-' + prefs.view;
    box.innerHTML = `
<div class="cel-tabs" role="tablist" aria-label="Library">
  <button class="cel-tab" type="button" role="tab" data-cel="tab" data-t="designs" title="Every design, its latest version first">Designs<b data-n="designs">0</b></button>
  <button class="cel-tab" type="button" role="tab" data-cel="tab" data-t="masters" title="The master charms">Charm library<b data-n="masters">0</b></button>
  <button class="cel-tab" type="button" role="tab" data-cel="tab" data-t="recent" title="Opened or placed from here on this computer">Recent<b data-n="recent">0</b></button>
</div>
<div class="cel-head">
  <div class="cel-search">${IC.search}<input type="text" data-cel="q" placeholder="Name, SKU, order or person" autocomplete="off" spellcheck="false" aria-label="Search the library"><button class="cel-x" type="button" data-cel="clear" aria-label="Clear the search" title="Clear (Esc)" hidden>${IC.x}</button></div>
  <div class="cel-bar">
    <div class="cel-chips" role="group" aria-label="Filters">
      <button class="cel-chip" type="button" data-cel="f" data-k="mine" aria-pressed="false">Mine</button>
      <button class="cel-chip" type="button" data-cel="f" data-k="order" data-v="with" aria-pressed="false">Order</button>
      <button class="cel-chip" type="button" data-cel="f" data-k="order" data-v="none" aria-pressed="false">No order</button>
      <button class="cel-chip" type="button" data-cel="f" data-k="when" data-v="today" aria-pressed="false">Today</button>
      <button class="cel-chip" type="button" data-cel="f" data-k="when" data-v="week" aria-pressed="false">Week</button>
    </div>
    <button class="cel-view" type="button" data-cel="view"></button>
  </div>
  <div class="cel-count"><span data-count></span><span data-note></span></div>
</div>
<div class="cel-list" role="listbox" tabindex="0" aria-label="Designs"><div class="cel-sizer"></div></div>
<div class="cel-hist" hidden></div>`;
    const keep = { display: el.style.display, flexDirection: el.style.flexDirection, overflow: el.style.overflow };
    Object.assign(el.style, { display: 'flex', flexDirection: 'column', overflow: 'hidden' });
    el.appendChild(box);
    const $ = s => box.querySelector(s);
    const list = $('.cel-list'), sizer = $('.cel-sizer'), input = $('[data-cel="q"]'), hist = $('.cel-hist');
    S = {
      ctx, el, box, keep, list, sizer, input, hist, cells: [], live: new Map(), free: [], n: 0, res: null, prev: null, ix: null, rows: [], first: 0, last: -1,
      lay: { cols: 2, cw: 120, ch: 160, pw: 110, rowH: 168, gap: 8, pad: 8, w: 0, h: 0 }, act: -1, timer: 0, running: 0, drag: null, ro: null, work: { search: [], scroll: [] }, curKey: null
    };
    input.value = prefs.q;
    wire();
    measure();
    const onData = which => { if (!S) return; if (which === prefs.tab || which === 'designs' || which === 'masters') refresh(true); else if (which === 'recent') { counts3(); if (prefs.tab === 'recent') refresh(true); } };
    S.onData = onData; watchers.add(onData);
    // (every time the editor opens: the design library is asked what was saved since (it asks the cloud at most every 20 s, one read when nothing was), so another
    //  person's work is here too; the list shows this computer's copy meanwhile)
    designsLoad(false, true); mastersLoad();
    setTab(prefs.tab, true);
    if (prefs.top[prefs.tab]) list.scrollTop = prefs.top[prefs.tab];
    render(true);
  }
  function unmount() {
    if (!S) return;
    const s = S; S = null;
    prefs.top[prefs.tab] = s.list.scrollTop;
    watchers.delete(s.onData);
    clearTimeout(s.timer);
    if (s.ro) try { s.ro.disconnect(); } catch (_) { /* fine */ }
    endDrag(null, true, s);
    if (s.ask) s.ask.done('cancel');
    if (s.box.parentNode) s.box.parentNode.removeChild(s.box);
    Object.assign(s.el.style, s.keep);
  }

  /* ── what the list shows ── */
  function sourceIx(tab) { return tab === 'masters' ? MS.ix : tab === 'recent' ? recentIndex() : designsIndex(); }
  function counts3() {
    if (!S) return;
    const n = { designs: designsIndex().rows.length, masters: MS.ix.rows.length, recent: recentRead().length };
    for (const b of S.box.querySelectorAll('[data-n]')) b.textContent = fmtN(n[b.dataset.n]);
  }
  const fmtN = n => (n >= 10000 ? Math.round(n / 1000) + 'k' : String(n || 0));
  function setTab(t, quiet) {
    if (!S) return;
    if (!quiet) prefs.top[prefs.tab] = S.list.scrollTop;
    prefs.tab = t === 'masters' || t === 'recent' ? t : 'designs';
    for (const b of S.box.querySelectorAll('[data-cel="tab"]')) { const on = b.dataset.t === prefs.tab; b.classList.toggle('is-on', on); b.setAttribute('aria-selected', on ? 'true' : 'false'); }
    S.box.querySelector('.cel-chips').hidden = prefs.tab !== 'designs';
    S.list.setAttribute('aria-label', prefs.tab === 'masters' ? 'Master charms' : prefs.tab === 'recent' ? 'Recent' : 'Designs');
    S.prev = null; S.act = -1;
    if (prefs.tab === 'masters') mastersLoad(); else if (prefs.tab === 'designs') designsLoad();
    closeHist();
    refresh(false);
    if (!quiet) S.list.scrollTop = prefs.top[prefs.tab] || 0;
    render(true);
  }
  /** The words and filters run again over the tab's index; keepTop: the list stays where it is (new data arriving), else it goes back to the top (a new search). */
  function refresh(keepTop) {
    if (!S) return;
    const t0 = perf();
    const ix = sourceIx(prefs.tab), f = prefs.tab === 'designs' ? prefs.f : {};
    if (ix !== S.ix) S.prev = null;
    const anchor = keepTop && S.ix && S.res ? topKey() : null;
    S.ix = ix;
    S.res = query(ix, prefs.q, f, me(), now(), S.prev); S.prev = S.res;
    S.rows = ix.rows; S.n = S.res.list.length;
    S.input.parentNode.querySelector('.cel-x').hidden = !prefs.q;
    syncChips();
    counts3();
    sayCount();
    sizeList();
    if (anchor) { const at = indexOfKey(anchor.key); if (at >= 0) S.list.scrollTop = Math.max(0, rowTop(Math.floor(at / S.lay.cols)) - anchor.off); }
    else if (!keepTop) S.list.scrollTop = 0;
    render(true);
    S.lastWork = perf() - t0;
  }
  function topKey() {
    const i = Math.max(0, Math.floor((S.list.scrollTop - S.lay.pad) / S.lay.rowH) * S.lay.cols); if (!S.n || !S.res) return null;
    const r = S.rows[S.res.list[Math.min(i, S.n - 1)]]; return r ? { key: r.key, off: rowTop(Math.floor(i / S.lay.cols)) - S.list.scrollTop } : null;
  }
  function indexOfKey(k) { const L = S.res.list; for (let i = 0; i < L.length; i++) if (S.rows[L[i]].key === k) return i; return -1; }
  function syncChips() {
    const f = prefs.f, meName = me();
    for (const b of S.box.querySelectorAll('[data-cel="f"]')) {
      const k = b.dataset.k, on = k === 'mine' ? !!f.mine : f[k] === b.dataset.v;
      b.classList.toggle('is-on', on); b.setAttribute('aria-pressed', on ? 'true' : 'false');
      if (k === 'mine') { b.disabled = !meName; b.title = meName ? `Made by ${meName}` : 'Sign in to see your own designs'; }
    }
    if (prefs.tab === 'designs' && S.ix && S.ix.rows.length <= 20000) {
      // (how many each would leave, on the chip's own title: the question the chip is pressed to answer, J 20907; counted only while the list is a few thousand)
      const c = counts(S.ix, prefs.q, meName, now()), say = { mine: c.mine, with: c.with, none: c.none, today: c.today, week: c.week };
      const words = { with: 'with an order', none: 'with no order (custom pieces)', today: 'made or changed today', week: 'made or changed this week' };
      for (const b of S.box.querySelectorAll('[data-cel="f"]')) { const k = b.dataset.k === 'mine' ? 'mine' : b.dataset.v; if (k !== 'mine' || meName) b.title = `${say[k]} ${k === 'mine' ? `by ${meName}` : words[k]}`; }
    }
    const v = S.box.querySelector('[data-cel="view"]');
    v.innerHTML = prefs.view === 'tiles' ? IC.list : IC.tiles; v.title = prefs.view === 'tiles' ? 'Show as a list' : 'Show as tiles'; v.setAttribute('aria-label', v.title);
  }
  function sayCount() {
    const total = S.ix.rows.length, n = S.n, unit = prefs.tab === 'masters' ? 'charm' : 'design';
    const loading = prefs.tab === 'designs' ? DS.loading && !total : prefs.tab === 'masters' ? MS.loading && !total : false;
    S.box.querySelector('[data-count]').textContent = loading ? 'Loading…' : n === total ? `${total.toLocaleString()} ${unit}${total === 1 ? '' : 's'}` : `${n.toLocaleString()} of ${total.toLocaleString()}`;
    S.box.querySelector('[data-note]').textContent = prefs.tab === 'designs' && DS.next ? (DS.loading ? 'loading older…' : 'older below') : '';
  }

  /* ── the virtual list: rows of cols cells; only what is on screen (and a row either side) is in the page ── */
  const OVER = 1;
  function layoutFor(w) {
    const pad = prefs.view === 'tiles' ? 8 : 0, gap = prefs.view === 'tiles' ? 8 : 0;
    if (prefs.view === 'list') return { cols: 1, cw: Math.max(1, w), ch: 58, pw: 46, rowH: 58, gap, pad, w };
    // (the caption: a name line of 16 px and two small lines of 13, its padding and its rule: 54 px)
    const cols = w >= 430 ? 3 : 2, cw = Math.max(60, Math.floor((w - 2 * pad - (cols - 1) * gap) / cols)), pw = Math.round(cw * 0.86), cap = 54;
    return { cols, cw, ch: pw + cap, pw, rowH: pw + cap + gap, gap, pad, w };
  }
  function measure() {
    if (!S) return;
    const w = S.list.clientWidth, h = S.list.clientHeight;
    const L = layoutFor(w); L.h = h;
    const changedW = L.cw !== S.lay.cw || L.cols !== S.lay.cols || L.rowH !== S.lay.rowH;
    S.lay = L;
    S.box.style.setProperty('--cw', L.cw + 'px'); S.box.style.setProperty('--ch', L.ch + 'px'); S.box.style.setProperty('--pw', L.pw + 'px');
    return changedW;
  }
  const rowTop = r => S.lay.pad + r * S.lay.rowH;
  /** The design (or master) the editor has open, when it was opened from here: its row is marked. */
  function curKey() {
    if (S.curKey != null) return S.curKey;
    let o = null;
    try { o = (S.ctx && S.ctx.opts) || (root.CharmEditor && root.CharmEditor.current && root.CharmEditor.current.opts) || null; } catch (_) { o = null; }
    let d = o && o.design || null;
    if (!d) { try { const M = Masters(), c = M && typeof M.current === 'function' ? M.current() : null; if (c) d = { id: c.designId, sku: c.source && c.source.kind === 'master' ? c.source.sku : '' }; } catch (_) { d = null; } }
    return (S.curKey = d ? (d.id ? 'd:' + d.id : d.sku ? 'm:' + d.sku : '') : '');
  }
  function sizeList() {
    const rows = Math.ceil(S.n / S.lay.cols);
    S.sizer.style.height = (rows ? rowTop(rows) - S.lay.gap + S.lay.pad : 0) + (prefs.tab === 'designs' && DS.next ? 30 : 0) + 'px';
    let em = S.list.querySelector('.cel-empty');
    if (!S.n) {
      if (!em) { em = doc.createElement('div'); em.className = 'cel-empty'; S.list.appendChild(em); }
      em.innerHTML = emptyWords();
    } else if (em) em.remove();
    let mo = S.list.querySelector('.cel-more');
    if (S.n && prefs.tab === 'designs' && DS.next) { if (!mo) { mo = doc.createElement('div'); mo.className = 'cel-more'; S.list.appendChild(mo); } mo.style.top = (rowTop(Math.ceil(S.n / S.lay.cols)) - S.lay.gap + S.lay.pad) + 'px'; mo.textContent = DS.loading ? 'Loading older designs…' : 'Older designs load as you scroll'; }
    else if (mo) mo.remove();
  }
  function emptyWords() {
    const total = S.ix.rows.length;
    if (prefs.tab === 'designs') {
      if (!Designs()) return 'The design library is not on this page yet.';
      if (DS.error && !total) return `The design library could not be read: ${esc(DS.error)}`;
      if (!total) return DS.loading || !DS.loaded ? 'Loading the design library…' : 'Nothing saved yet.<br>Every charm saved in the editor lands here, <b>every version of it</b>, ready to open again.';
    } else if (prefs.tab === 'masters') {
      if (!Masters()) return 'The charm library is not on this page yet.';
      if (MS.error && !total) return `The charm library could not be read: ${esc(MS.error)}`;
      if (!total) return MS.loading || !MS.loaded ? 'Loading the charm library…' : 'No master charms.';
    } else if (!total) return 'Nothing yet.<br>Designs and charms you open or place from here are listed here, newest first.';
    return 'Nothing matches that.' + (prefs.tab === 'designs' && (prefs.f.mine || prefs.f.order || prefs.f.when) ? '<br>A filter is on: press it again to take it off.' : '');
  }
  function makeCell() {
    const c = doc.createElement('div');
    c.className = 'cel-cell is-free'; c.setAttribute('role', 'option');
    c.innerHTML = '<span class="cel-pic"><i class="cel-ph"></i><img alt="" decoding="async" draggable="false"></span><span class="cel-tx"><b class="cel-nm"></b><span class="cel-s1"></span><span class="cel-s2"></span></span>'
      + `<i class="cel-v"></i><button class="cel-b cel-b--place" type="button" data-cel="place" tabindex="-1" title="Put it in the middle of the charm (or drag it there)" aria-label="Put it on the charm">${IC.place}</button>`
      + `<button class="cel-b cel-b--hist" type="button" data-cel="hist" tabindex="-1" title="Versions" aria-label="Versions">${IC.clock}</button>`;
    c._img = c.querySelector('img'); c._ph = c.querySelector('.cel-ph'); c._nm = c.querySelector('.cel-nm'); c._s1 = c.querySelector('.cel-s1'); c._s2 = c.querySelector('.cel-s2'); c._v = c.querySelector('.cel-v');
    c._key = ''; c._pk = ''; c._i = -1;
    S.list.appendChild(c); S.cells.push(c);
    return c;
  }
  /** What a row says: its name, then the order (or "Custom · no order", D4) and the version, then who and when; a master its SKU and size. */
  function bind(c, i) {
    const r = S.rows[S.res.list[i]];
    c._i = i; c.dataset.i = String(i);
    if (c._key !== r.key || c._ver !== r.version || c._at !== r.at) {
      c._key = r.key; c._ver = r.version; c._at = r.at; c._row = r;
      c._nm.textContent = r.name;
      if (r.kind === 'master') {
        c._s1.textContent = r.name !== r.sku ? r.sku : r.category || 'Master charm'; c._s1.classList.remove('is-custom');
        c._s2.textContent = r.recent ? `${r.recent} ${when(r.at)}` : r.widthMm && r.heightMm ? `${+r.widthMm.toFixed(1)} × ${+r.heightMm.toFixed(1)} mm` : '';
        c._v.textContent = '';
      } else {
        c._s1.textContent = r.custom ? 'Custom · no order' : '#' + r.orderId; c._s1.classList.toggle('is-custom', r.custom);   // (D4: its own name, and the tag; its CUSTOM- id is found by the search)
        c._s2.textContent = r.recent ? `${r.recent} ${when(r.at)}` : [r.by, when(r.at)].filter(Boolean).join(' · ');
        c._v.textContent = 'v' + r.version;
      }
      c._ph.textContent = initials(r.name);
      c.title = `${r.name}${r.kind === 'master' ? ' · ' + r.sku : (r.custom ? ' · custom, no order' : ' · order ' + r.orderId) + ' · version ' + r.version + (r.by ? ' · ' + r.by : '')}\nClick to open · drag onto the charm to place it`;
      c.classList.toggle('is-nohist', r.kind !== 'design');
      c.classList.toggle('is-cur', r.key === curKey());
      const pk = r.pic;
      if (c._pk !== pk) {
        c._pk = pk; c._at0 = perf(); c._has = false;
        const hit = shownGet(pk) || (root.CharmNestThumbs && root.CharmNestThumbs.peek ? root.CharmNestThumbs.peek(pk) : '');
        if (hit) { c._img.src = hit; c._has = true; c.classList.add('is-pic'); }
        else { c._img.removeAttribute('src'); c.classList.remove('is-pic'); pics(); }
      }
    }
    c.classList.toggle('is-act', i === S.act);
    c.setAttribute('aria-selected', i === S.act ? 'true' : 'false');
  }
  function place(c, i) {
    const L = S.lay, r = Math.floor(i / L.cols), k = i % L.cols;
    const x = L.pad + k * (L.cw + L.gap), y = rowTop(r);
    if (c._x !== x || c._y !== y) { c._x = x; c._y = y; c.style.transform = `translate(${x}px,${y}px)`; }
  }
  /** The cells for the rows in view: a cell that left the view is reused for one that came in (its words and picture changed, nothing built). */
  function render(force) {
    if (!S || !S.res) return;
    const L = S.lay, top = S.list.scrollTop, h = L.h || S.list.clientHeight;
    const rows = Math.ceil(S.n / L.cols);
    const r0 = Math.max(0, Math.floor((top - L.pad) / L.rowH) - OVER), r1 = Math.min(rows - 1, Math.floor((top + h - L.pad) / L.rowH) + OVER);
    const a = r0 * L.cols, b = Math.min(S.n - 1, (r1 + 1) * L.cols - 1);
    if (!force && a === S.first && b === S.last) return;
    for (const [i, c] of S.live) if (force || i < a || i > b) { S.live.delete(i); S.free.push(c); }
    for (let i = a; i <= b; i++) {
      if (S.live.has(i)) continue;
      // (a freed cell that last showed this same row is taken first: its picture is already in it)
      let c = null; const want = S.rows[S.res.list[i]].key;
      for (let j = S.free.length - 1; j >= 0; j--) if (S.free[j]._key === want) { c = S.free[j]; S.free.splice(j, 1); break; }
      if (!c) c = S.free.pop() || makeCell();
      c.classList.remove('is-free'); bind(c, i); place(c, i); S.live.set(i, c);
    }
    for (const c of S.free) if (!c.classList.contains('is-free')) { c.classList.add('is-free'); c._i = -1; c.dataset.i = ''; }
    S.first = a; S.last = b;
    // older designs arrive by themselves when the end is near (J 21661): two screens of runway
    if (prefs.tab === 'designs' && DS.next && !DS.loading && (r1 + 1) * L.rowH + L.pad >= rows * L.rowH - 2 * h) designsLoad(true);
  }

  /* ── pictures for the cells in view ── */
  function pics() { if (S && !S.timer) S.timer = setTimeout(pump, SETTLE_MS); }
  function pump() {
    if (!S) return; S.timer = 0;
    const t = perf(), ready = []; let wait = 0;
    for (const c of S.live.values()) { if (!c._pk || c._has || c._asking === c._pk) continue; const age = t - c._at0; if (age >= SETTLE_MS) ready.push(c); else wait = Math.max(wait, SETTLE_MS - age); }
    if (ready.length) {
      const mid = S.list.scrollTop + S.list.clientHeight / 2;
      ready.sort((p, q) => Math.abs(p._y - mid) - Math.abs(q._y - mid));
      for (const c of ready) {
        if (S.running >= LANES) { wait = wait || 40; break; }
        const pk = c._pk, row = c._row, s = S;
        c._asking = pk; s.running++;
        picFor(row).then(u => {
          if (u) shownPut(pk, u);
          if (c._pk === pk && u) { c._img.src = u; c._has = true; }
          else if (c._pk === pk) c._has = true;   // (no picture: the initials stay)
        }).finally(() => { if (c._asking === pk) c._asking = ''; s.running--; if (S === s) pics(); });
      }
    }
    if (wait && !S.timer) S.timer = setTimeout(pump, wait + 5);
  }

  /* ── the keys: the editor keeps every key to itself (one listener on the window, in the capture phase); an element that carries __ceKey is handed its keys ── */
  function keyInput(e) {
    const k = e.key;
    if (k === 'Escape') { e.preventDefault(); if (S.input.value) { S.input.value = ''; onInput(); } else focusWin(); }
    else if (k === 'ArrowDown') { e.preventDefault(); if (S.n) { setAct(Math.max(0, S.act)); S.list.focus({ preventScroll: true }); } }
    else if (k === 'Enter') { e.preventDefault(); if (S.n) openRow(S.rows[S.res.list[Math.max(0, S.act)]]); }
  }
  function keyList(e) {
    const k = e.key, cols = S.lay.cols;
    const go = d => { e.preventDefault(); setAct(Math.max(0, Math.min(S.n - 1, (S.act < 0 ? 0 : S.act + d)))); };
    if (k === 'ArrowDown') go(cols); else if (k === 'ArrowUp') { if (S.act < cols) { e.preventDefault(); S.input.focus(); } else go(-cols); }
    else if (k === 'ArrowRight') go(1); else if (k === 'ArrowLeft') go(-1);
    else if (k === 'Home') { e.preventDefault(); setAct(0); } else if (k === 'End') { e.preventDefault(); setAct(S.n - 1); }
    else if (k === 'Enter') { e.preventDefault(); if (S.act >= 0) openRow(S.rows[S.res.list[S.act]]); }
    else if (k === 'Escape') { e.preventDefault(); S.input.focus(); }
    else if (k.length === 1 && !e.metaKey && !e.ctrlKey && !e.altKey) { S.input.focus(); }   // (typing goes to the search, the letter with it)
    else if ((e.metaKey || e.ctrlKey) && k.length === 1) { e.preventDefault(); passOn(e); }  // (⌘Z, ⌘S … are the editor's, wherever the focus is)
  }
  /** A key the library has no use for, handed to the editor as if pressed on its window (the editor keeps one listener for all keys, on the window). */
  function passOn(e) {
    const w = S.box.closest('.ce-win'); if (!w) return;
    focusWin();
    try { w.dispatchEvent(new root.KeyboardEvent('keydown', { key: e.key, code: e.code, ctrlKey: e.ctrlKey, metaKey: e.metaKey, shiftKey: e.shiftKey, altKey: e.altKey, bubbles: true, cancelable: true })); } catch (_) { /* fine */ }
  }
  function setAct(i) {
    S.act = i; if (i < 0) return render(true);
    const L = S.lay, y = rowTop(Math.floor(i / L.cols)), top = S.list.scrollTop, h = S.list.clientHeight;
    if (y < top) S.list.scrollTop = y - L.pad; else if (y + L.ch > top + h) S.list.scrollTop = y + L.ch - h + L.pad;
    render(true);
  }
  const focusWin = () => { try { const w = S.box.closest('.ce-win'); if (w) w.focus({ preventScroll: true }); } catch (_) { /* fine */ } };

  function onInput() {
    if (!S) return;
    const t0 = perf();
    prefs.q = S.input.value;
    S.act = -1;
    refresh(false);
    S.work.search.push(perf() - t0);
    // (a search that leaves less than a screen while older designs are still to come asks for the next page, once the typing has stopped)
    clearTimeout(S.moreT);
    if (prefs.tab === 'designs' && DS.next && S.n < 40) S.moreT = setTimeout(() => designsLoad(true), 400);
  }

  function wire() {
    const { box, list, input } = S;
    input.__ceKey = keyInput; list.__ceKey = keyList;
    input.addEventListener('input', onInput);
    box.addEventListener('click', e => {
      const b = e.target.closest('[data-cel]'); if (!b || !S) return;
      const a = b.dataset.cel;
      if (a === 'tab') setTab(b.dataset.t);
      else if (a === 'clear') { input.value = ''; onInput(); input.focus(); }
      else if (a === 'f') {
        const k = b.dataset.k, f = prefs.f;
        if (k === 'mine') f.mine = !f.mine; else f[k] = f[k] === b.dataset.v ? '' : b.dataset.v;
        S.prev = null; refresh(false);
      } else if (a === 'view') {
        prefs.view = prefs.view === 'tiles' ? 'list' : 'tiles';
        try { root.localStorage && root.localStorage.setItem('cn.ceLib.view', prefs.view); } catch (_) { /* this visit only */ }
        box.classList.toggle('is-tiles', prefs.view === 'tiles'); box.classList.toggle('is-list', prefs.view === 'list');
        const k = topKey(); measure(); syncChips(); sizeList();
        if (k) { const at = indexOfKey(k.key); if (at >= 0) list.scrollTop = Math.max(0, rowTop(Math.floor(at / S.lay.cols)) - S.lay.pad); }
        render(true);
      } else if (a === 'place' || a === 'hist') {
        const c = b.closest('.cel-cell'); const r = c && c._row; if (!r) return;
        if (a === 'place') placeRow(r, null, null); else openHist(r);
      }
    });
    list.addEventListener('scroll', () => {
      if (!S) return;
      const t0 = perf(); render(false); S.work.scroll.push(perf() - t0);
      if (S.work.scroll.length > 4000) S.work.scroll.splice(0, 2000);
    }, { passive: true });
    // (a picture that arrived after its cell was reused for another row is not shown there: the address is checked on load)
    list.addEventListener('load', e => { const c = e.target.closest && e.target.closest('.cel-cell'); if (c && e.target === c._img) c.classList.add('is-pic'); }, true);
    list.addEventListener('pointerdown', onDown);
    list.addEventListener('pointermove', onMove);
    list.addEventListener('pointerup', e => endDrag(e, false));
    list.addEventListener('pointercancel', e => endDrag(e, true));
    list.addEventListener('dblclick', e => e.preventDefault());
    if (root.ResizeObserver) { S.ro = new root.ResizeObserver(() => { if (!S) return; const k = S.lay.cols; if (measure() || k !== S.lay.cols) { sizeList(); render(true); } else { S.lay.h = S.list.clientHeight; render(false); } }); S.ro.observe(list); }
  }

  /* ── press, drag, drop (J 21681-21760): a press that never moves opens; one that travels lifts a ghost and puts the design down where it is let go ── */
  function onDown(e) {
    if (!S || e.button > 0) return;
    if (e.target.closest('.cel-b')) return;
    const c = e.target.closest('.cel-cell'); if (!c || !c._row) return;
    const touch = e.pointerType === 'touch';
    S.drag = { row: c._row, version: 0, cell: c, x0: e.clientX, y0: e.clientY, moved: false, ghost: null, id: e.pointerId, touch, scroll: false, over: false, host: S.list };
    if (!touch) { try { S.list.setPointerCapture(e.pointerId); } catch (_) { /* fine */ } }
  }
  function onMove(e) {
    const d = S && S.drag; if (!d || e.pointerId !== d.id) return;
    const dx = e.clientX - d.x0, dy = e.clientY - d.y0;
    if (!d.moved) {
      if (dx * dx + dy * dy < 64) return;
      // (a finger that starts up or down is the list scrolling; one that starts sideways is a drag, J 21700)
      if (d.touch && Math.abs(dy) >= Math.abs(dx)) { d.scroll = true; S.drag = null; return; }
      d.moved = true;
      try { d.host.setPointerCapture(e.pointerId); } catch (_) { /* fine */ }
      liftGhost(d);
      // the design is asked for now, so the drop finds it already here
      if (d.row.kind === 'design') designGet(d.row.id, d.version || d.row.version).catch(() => {}); else mastersFaces(d.row.sku).catch(() => {});
    }
    e.preventDefault();
    d.ghost.style.left = e.clientX + 'px'; d.ghost.style.top = e.clientY + 'px';
    const over = overStage(e.clientX, e.clientY);
    if (over !== d.over) { d.over = over; d.ghost.classList.toggle('is-over', over); const st = stageEl(); if (st) st.classList.toggle('cel-over', over); }
  }
  function liftGhost(d) {
    const g = doc.createElement('div'); g.className = 'cel-ghost';
    const u = shownGet(d.row.pic);
    g.innerHTML = u ? `<img alt="" src="${esc(u)}">` : `<i>${esc(d.row.name)}</i>`;
    (S.box.closest('.ce-root') || doc.body).appendChild(g); d.ghost = g;
    if (d.cell) d.cell.classList.add('is-lift');
  }
  function endDrag(e, cancelled, s) {
    s = s || S; const d = s && s.drag; if (!d || (e && e.pointerId !== d.id)) return;
    s.drag = null;
    if (d.ghost) d.ghost.remove();
    if (d.cell) d.cell.classList.remove('is-lift');
    const st = stageEl(); if (st) st.classList.remove('cel-over');
    if (cancelled || d.scroll || !e) return;
    if (!d.moved) { if (d.vrow) openRow(d.row, d.version); else { setAct(d.cell ? d.cell._i : -1); openRow(d.row); } return; }
    if (!overStage(e.clientX, e.clientY)) return;
    placeRow(d.row, d.version, [e.clientX, e.clientY]);
  }
  const stageEl = () => (S && (S.ctx && S.ctx.stage || (S.box.closest('.ce-root') || doc).querySelector('.ce-stage'))) || null;
  function overStage(x, y) {
    const st = stageEl(); if (!st) return false;
    const hit = doc.elementFromPoint(x, y);
    return !!(hit && st.contains(hit));
  }

  /* ═══ 6 · placing a design on the charm: ONE group, at the size it was made ═══ */
  /** A master's drawing as faces: the masters module's own (faces), else the same reading of it the masters module makes when it opens one (the entry, its first size,
   *  Pool.masterCharm's one copy per page, model.masterFace). */
  async function mastersFaces(sku) {
    const M = Masters();
    if (M && typeof M.faces === 'function') { const r = await M.faces(sku); const f = r && r.faces ? r.faces : r; if (!f || !(f.front || f.back)) throw new Error('that charm has no drawing'); return f; }
    const Ms = root.Master, Pl = root.Pool, AE = root.CharmNestArtEdit, Mo = M && M.model, EM = root.CharmEditor && root.CharmEditor.model;
    if (!Ms || !Pl || !AE || !Mo || typeof Mo.masterFace !== 'function' || !EM || typeof Pl.masterCharm !== 'function') throw new Error('the charm library cannot hand over a charm on this page');
    const e = (Ms.entryFor && Ms.entryFor(sku)) || (Ms.fetchEntry && await Ms.fetchEntry(sku));
    if (!e) throw new Error('it is not in the charm library');
    const sizes = Object.keys(e.sizes || {}).filter(k => e.sizes[k] && e.sizes[k].aiPath);
    const src = await Pl.masterCharm(e, sizes[0] || null), charm = src && src.charms && src.charms[0];
    if (!charm) throw new Error('its drawing could not be read');
    const front = Mo.masterFace(charm, AE, EM); if (front) delete front.skipped;
    if (!front || !front.items || !front.items.length) throw new Error('its drawing has no part the editor can hold');
    return { front };
  }
  const STRIP = ['bbox', 'matrix', 'pivot'];
  function strip(it) {
    const o = {}; for (const k of Object.keys(it)) if (!STRIP.includes(k) && k[0] !== '_') o[k] = it[k];
    if (it.kind === 'group') o.children = (it.children || []).map(strip);
    return o;
  }
  /**
   * The design's drawing for the face being edited (its back when the back is being edited and it has one, else its front), as one new group centred where it was let
   * go (the middle of the charm for the ⊕). Its size is the size it was made: nothing is scaled. When the charm already has its own cut line, the design's cut line is
   * left out (a second outline would cut the charm through) and the line over the work area says so.
   */
  async function placeRow(row, version, client) {
    const s = S; if (!s) return;
    const ctx = s.ctx, C = root.CharmEditor, model = (ctx && ctx.model) || (C && C.model);
    if (!ctx || typeof ctx.addItems !== 'function' || !model) { sayLine('This editor cannot take a design dropped on it.'); return; }
    let faces, name = row.name, v = version || row.version;
    try { faces = row.kind === 'master' ? await mastersFaces(row.sku) : (await designGet(row.id, v)).faces; }
    catch (err) { sayLine(`Not placed · ${(err && err.message) || err}`); return; }
    if (S !== s) return;
    const face = typeof ctx.face === 'function' ? ctx.face() : 'front';
    const fd = (face === 'back' && faces.back) || faces.front || faces.back;
    let kids = (fd && fd.items || []).filter(Boolean);
    const here = typeof ctx.items === 'function' ? ctx.items() : [];
    const hasCut = list => list.some(it => it.role === 'cut' || (it.kind === 'group' && hasCut(it.children || [])));
    let leftOut = 0;
    if (hasCut(here)) { const k2 = kids.filter(it => it.role !== 'cut'); leftOut = kids.length - k2.length; kids = k2; }
    if (!kids.length) { sayLine(`“${name}” has nothing to place on this side.`); return; }
    // its box as drawn (the editor's own geometry when it is given, so words are measured with the editor's font engine)
    let bb = null;
    const un = (a, b) => (!a ? b : !b ? a : [Math.min(a[0], b[0]), Math.min(a[1], b[1]), Math.max(a[2], b[2]), Math.max(a[3], b[3])]);
    for (const it of kids) {
      let b = null;
      try { b = ctx.geo && ctx.geo.drawn ? ctx.geo.drawn(it, model.I) : null; } catch (_) { b = null; }
      if (!b && Array.isArray(it.bbox) && it.bbox.length === 4) b = it.bbox;
      if (!b && it.subpaths && model.bboxOf) b = model.bboxOf(it.subpaths);
      bb = un(bb, b);
    }
    const fr = typeof ctx.frame === 'function' ? ctx.frame() : null;
    let at = client && typeof ctx.pointOf === 'function' ? ctx.pointOf(client[0], client[1]) : null;
    if (!at) at = fr ? [fr.x0 + fr.wPt / 2, fr.y0 + fr.hPt / 2] : [0, 0];
    const c = bb ? [(bb[0] + bb[2]) / 2, (bb[1] + bb[3]) / 2] : at;
    const roles = {}; kids.forEach(k => { roles[k.role] = (roles[k.role] || 0) + 1; });
    const role = Object.keys(roles).sort((p, q) => roles[q] - roles[p])[0] || 'art';
    const reid = model.reid || (x => x);
    const g = {
      kind: 'group', role, source: 'added', name: row.kind === 'master' ? name : `${name} · v${v}`, hidden: false, locked: false,
      tx: +(at[0] - c[0]).toFixed(6), ty: +(at[1] - c[1]).toFixed(6), sx: 1, sy: 1, rot: 0,
      children: kids.map(it => reid(strip(it))),
      // (where it came from, as plain values: they travel with the part into the design library and the laser files' records)
      ...(row.kind === 'master' ? { masterSku: row.sku } : { designId: row.id, designVersion: v })
    };
    let ids = null;
    try { ids = ctx.addItems([g]); if (typeof ctx.commit === 'function') ctx.commit(`Place ${name}`); }
    catch (err) { sayLine(`Not placed · ${(err && err.message) || err}`); return; }
    if (Array.isArray(ids) && ids.length && typeof ctx.select === 'function') { try { ctx.select(ids); } catch (_) { /* fine */ } }
    recentPush(row, v, 'placed');
    log('designPlace', { designId: row.kind === 'design' ? row.id : null, version: row.kind === 'design' ? v : null, sku: row.sku || null, face, by: me() });
    sayLine(`Placed “${name}”${row.kind === 'design' ? ` (v${v})` : ''} as one group${leftOut ? ' · its cut line was left out: this charm has its own' : ''}`);
  }
  function sayLine(t) {
    const ctx = S && S.ctx;
    if (ctx && typeof ctx.say === 'function') { try { ctx.say(t, 3200); return; } catch (_) { /* below */ } }
    if (S) S.box.querySelector('[data-note]').textContent = t;
  }

  /* ═══ 7 · opening a design: ask first whether to keep the changes, then the editor opens on it with the same fonts ═══ */
  function askKeep(title, name) {
    return new Promise(res => {
      const win = S.box.closest('.ce-win') || S.box;
      const box = doc.createElement('div'); box.className = 'ce-ask cel-ask3';
      box.innerHTML = `<div class="ce-ask__card" role="alertdialog" aria-modal="true" aria-label="Keep your changes?"><h3>Keep your changes?</h3>`
        + `<p>${title ? `“${esc(title)}” has` : 'This charm has'} changes that are not saved. Keep them (they are saved just as Save does) before “${esc(name)}” opens, or leave them.</p>`
        + `<div class="ce-ask__b"><button class="ce-btn ce-btn--ghost" type="button" data-k="cancel">Cancel</button><button class="ce-btn ce-btn--ghost" type="button" data-k="discard">Leave them</button>`
        + `<button class="ce-btn ce-btn--ink" type="button" data-k="keep">Keep and open</button></div></div>`;
      win.appendChild(box);
      const s = S;
      // (the keys go to the question at once: an Esc pressed the moment it appears is its Cancel)
      const kb = box.querySelector('[data-k="keep"]'); if (kb) kb.focus({ preventScroll: true });
      s.ask = { done: v => { if (!s.ask) return; s.ask = null; box.remove(); res(v); } };
      box.addEventListener('click', ev => { const b = ev.target.closest('[data-k]'); if (b) s.ask.done(b.dataset.k); else if (ev.target === box) s.ask.done('cancel'); });
      const keys = ev => { if (ev.key === 'Escape') { ev.preventDefault(); s.ask && s.ask.done('cancel'); } };
      for (const b of box.querySelectorAll('button')) b.__ceKey = keys;
    });
  }
  let opening = false;
  /**
   * Open a design (its latest version, or `version`) or a master. Asked first when the charm in the editor has changes; the session is then closed as chosen (kept:
   * exactly as its Save, so whoever opened it gets its result; left: as Cancel) and the masters module opens the design in design mode (its Save makes the next
   * version, its card offers Send to sheet). Without that module, the editor opens again here on the design with the fonts, the font engine and the locks it had, and
   * its Save puts a version of the design (a master: a new design whose source is that master).
   */
  async function openRow(row, version) {
    if (!S || opening || !row) return;
    const C = root.CharmEditor, cur = C && C.current;
    if (!cur || typeof C.open !== 'function') return;
    opening = true;
    const s = S;
    let next = null, design = null;
    try {
      const v = row.kind === 'design' ? version || row.version : 0;
      const title = (() => { try { const h = s.box.closest('.ce-win').querySelector('.ce-id h2'); return h ? h.textContent : ''; } catch (_) { return ''; } })();
      let keep = false;
      if (typeof cur.dirty === 'function' && cur.dirty()) {
        const a = await askKeep(title, row.kind === 'design' ? `${row.name} · v${v}` : row.name);
        if (a !== 'keep' && a !== 'discard') return;
        keep = a === 'keep';
      }
      const M = Masters();
      if (row.kind === 'master' && M && typeof M.open === 'function') {
        // (the masters module reads the master and opens it as a new design: the editor is closed first, as chosen, and the master handed over; it logs the open)
        if (!endSession(cur, keep)) return;
        recentPush(row, 0, 'opened');
        M.open(row.sku);
        return;
      }
      sayLine(`Opening “${row.name}”…`);
      let d;
      // (read before anything is closed: a design that cannot be read leaves the charm in the editor as it is)
      try { d = row.kind === 'design' ? await designGet(row.id, v) : { name: row.name, faces: await mastersFaces(row.sku), source: { kind: 'master', sku: row.sku } }; }
      catch (err) { sayLine(`Not opened · ${(err && err.message) || err}`); return; }
      if (S !== s) return;
      if (row.kind === 'design' && M && typeof M.openDesign === 'function') {
        if (!endSession(cur, keep)) return;
        recentPush(row, v, 'opened');
        M.openDesign(d);
        return;
      }
      const o = cur.opts || {};
      if (!endSession(cur, keep)) return;
      const faces = {}; for (const f of ['front', 'back']) if (d.faces[f]) faces[f] = d.faces[f];
      const orderId = d.orderId || row.orderId || '', customName = d.customName || row.customName || '';
      design = row.kind === 'design'
        ? { id: row.id, version: v, latest: row.version, name: d.name || row.name, orderId: orderId || null, customName: customName || null, source: d.source || row.source || null, keptBy: 'library' }
        : { id: null, version: 0, name: row.name, orderId: null, customName: null, source: { kind: 'master', sku: row.sku }, sku: row.sku, keptBy: 'library' };
      next = {
        fonts: o.fonts, textToPaths: o.textToPaths, lockRoles: o.lockRoles,
        title: design.name,
        subtitle: row.kind === 'design' ? `design · v${v}${v < row.version ? ` of ${row.version}` : ''} · ${noOrder(orderId) ? (customName ? `custom · ${customName}` : 'custom · no order') : 'order ' + orderId}` : `master · ${row.sku}`,
        faces, startFace: faces.front ? 'front' : 'back', design
      };
      recentPush(row, v, 'opened');
      log('designOpen', { designId: design.id, version: v || null, sku: row.kind === 'master' ? row.sku : (row.sku || null), orderId: orderId || null, from: 'library', kept: keep, by: me() });
    } finally { opening = false; }
    if (!next) return;
    // (the session opened here may itself open another design: this one's Save, if it comes, is still a version of THIS design)
    const res = await C.open(next);
    if (res) await keepVersion(design, res);
  }
  /** The open session closed as chosen: kept (exactly as its Save) or left (as Cancel, with nothing asked again). False when the editor cannot be closed from here. */
  function endSession(cur, keep) {
    if (typeof cur.end === 'function') { cur.end(!!keep); return true; }
    if (cur.ctx && typeof cur.ctx.close === 'function') { cur.ctx.close(!!keep); return true; }
    sayLine('This editor cannot open another design from here.');
    return false;
  }
  /** A session opened from here was saved: a new version of its design (D8, every save is a version), by the person signed in. */
  async function keepVersion(design, res) {
    const D = Designs();
    if (!D || typeof D.put !== 'function') return;
    const f = res.faces.front || res.faces.back, fr = f && f.frame;
    let by = me();
    if (!by) { try { if (root.CNEmployee && typeof root.CNEmployee.need === 'function') by = String((await root.CNEmployee.need('Kept with the design you saved.')) || '').trim(); } catch (_) { by = ''; } }
    const put = { name: design.name, faces: res.faces, widthMm: fr ? +(fr.wPt * MM).toFixed(3) : 0, heightMm: fr ? +(fr.hPt * MM).toFixed(3) : 0, source: design.source || { kind: 'new' }, by };
    if (design.id) put.id = design.id;
    if (design.orderId) put.orderId = design.orderId;
    if (design.customName) put.customName = design.customName;
    try {
      const r = await D.put(put);
      const id = (r && r.id) || design.id, version = r && r.version;
      if (id) { designsMerge([Object.assign({}, put, { id, version: version || (design.latest || 0) + 1, at: (r && r.at) || now(), hash: r && r.hash, faces: undefined, thumbUrl: (r && r.thumbUrl) || '' })]); changed('designs'); }
      recentPush({ key: 'd:' + id, kind: 'design', id, name: design.name, orderId: design.orderId || '', version }, version, 'saved');
      log('designSave', { designId: id, version: version || null, orderId: design.orderId || null, from: 'library', by });
    } catch (err) {
      try { if (typeof root.toast === 'function') root.toast(`The design was not saved: ${(err && err.message) || err}`, 'bad', 8000); } catch (_) { /* fine */ }
    }
  }

  /* ═══ 8 · a design's versions ═══ */
  async function versionsOf(row) {
    const D = Designs();
    let list = null;
    try { if (D && typeof D.versions === 'function') list = await D.versions(row.id); } catch (_) { list = null; }
    if (!Array.isArray(list) && D && typeof D.get === 'function') {
      try { const d = await D.get(row.id, undefined, { history: true }); list = d && (Array.isArray(d.history) ? d.history : Array.isArray(d.versions) ? d.versions : null); } catch (_) { list = null; }
    }
    if (!Array.isArray(list)) list = Array.from({ length: row.version }, (_, i) => ({ version: i + 1 }));
    return list.map(x => (typeof x === 'number' ? { version: x } : x)).filter(x => x && +x.version > 0).sort((a, b) => b.version - a.version);
  }
  async function openHist(row) {
    if (!S) return;
    const s = S, h = s.hist;
    h.innerHTML = `<div class="cel-hist__head"><button class="cel-hist__back" type="button" data-cel="back" title="Back to the library (Esc)" aria-label="Back">${IC.back}</button>`
      + `<div class="cel-hist__t"><b>${esc(row.name)}</b><span>${esc(row.custom ? 'Custom · ' + (row.orderId || 'no order') : 'Order ' + row.orderId)} · ${row.version} version${row.version === 1 ? '' : 's'}</span></div></div>`
      + '<div class="cel-hist__list" role="listbox" aria-label="Versions, newest first"><div class="cel-empty" style="position:static">Reading the versions…</div></div>';
    h.hidden = false; h._row = row;
    h.querySelector('[data-cel="back"]').__ceKey = e => { if (e.key === 'Escape') { e.preventDefault(); closeHist(); } };
    const vs = await versionsOf(row);
    if (S !== s || h._row !== row) return;
    const L = h.querySelector('.cel-hist__list');
    L.innerHTML = vs.map(x => {
      const n = +x.version, pk = 'd|' + row.id + '|' + n, u = shownGet(pk);
      return `<button class="cel-vrow${n === row.version ? ' is-cur' : ''}${u ? ' is-pic' : ''}" type="button" data-v="${n}" title="Open version ${n} · or drag it onto the charm">`
        + `<span class="cel-pic"><i class="cel-ph">v${n}</i><img alt="" decoding="async" draggable="false"${u ? ` src="${esc(u)}"` : ''}></span>`
        + `<span class="cel-tx"><b class="cel-nm">Version ${n}${n === row.version ? ' · latest' : ''}</b><span class="cel-s2">${esc([x.by, x.at ? when(msOf(x.at)) : ''].filter(Boolean).join(' · '))}</span></span></button>`;
    }).join('');
    L.__ceKey = e => { if (e.key === 'Escape') { e.preventDefault(); closeHist(); } };
    // the pictures of the versions that have one (a version's picture never changes, so it is kept under its own number)
    for (const x of vs) {
      const n = +x.version, pk = 'd|' + row.id + '|' + n; if (!x.thumbUrl || shownGet(pk)) continue;
      picFor({ thumbUrl: x.thumbUrl, pic: pk }).then(u => { if (!u) return; shownPut(pk, u); const b = L.querySelector(`[data-v="${n}"]`); if (b) { b.querySelector('img').src = u; b.classList.add('is-pic'); } });
    }
    L.addEventListener('pointerdown', e => {
      const b = e.target.closest('.cel-vrow'); if (!b || !S || e.button > 0) return;
      S.drag = { row, version: +b.dataset.v, cell: null, vrow: b, x0: e.clientX, y0: e.clientY, moved: false, ghost: null, id: e.pointerId, touch: e.pointerType === 'touch', scroll: false, over: false, host: L };
      if (e.pointerType !== 'touch') { try { L.setPointerCapture(e.pointerId); } catch (_) { /* fine */ } }
    });
    L.addEventListener('pointermove', onMove);
    L.addEventListener('pointerup', e => endDrag(e, false));
    L.addEventListener('pointercancel', e => endDrag(e, true));
    h.querySelector('[data-cel="back"]').addEventListener('click', closeHist);
    log('designVersions', { designId: row.id, versions: vs.length, by: me() });
  }
  function closeHist() { if (S && !S.hist.hidden) { S.hist.hidden = true; S.hist.innerHTML = ''; S.hist._row = null; } }

  /* ═══ 9 · registration: one panel in the editor's right-hand slot ═══ */
  // open beside the charm by itself on a wide screen (the studio's dock is "continuously docked beside the canvas", J 19372); on a narrower one the two columns would
  // take too much of the work area, so it waits for the head's Library button, and once it is opened there the editor remembers it open (ce.xpanel)
  const WIDE_PX = 1440;
  const panel = {
    id: 'library', title: 'Library', side: 'right', icon: IC.lib,
    get open() { return !!(root.innerWidth && root.innerWidth >= WIDE_PX); },
    mount, unmount
  };
  if (doc && !(typeof module === 'object' && module.exports)) {
    const CE = root.CharmEditor || (root.CharmEditor = {});
    const P = CE.panels || (CE.panels = []);
    if (!P.some(p => p && p.id === panel.id)) P.push(panel);
    // (nothing is read until an editor opens: a page that never opens one asks for nothing; the designs module answers from its own copy after that)
  }

  /* read-outs for tests: nothing here changes the work */
  const probe = {
    state: () => (S ? { tab: prefs.tab, q: prefs.q, f: Object.assign({}, prefs.f), view: prefs.view, n: S.n, total: S.ix ? S.ix.rows.length : 0, cells: S.cells.length, live: S.live.size,
      first: S.first, last: S.last, lay: Object.assign({}, S.lay), keys: S.res ? Array.from(S.res.list.slice(0, 50), i => S.rows[i].key) : [], narrowed: !!(S.res && S.res.narrowed) } : null),
    work: () => (S ? { search: S.work.search.slice(), scroll: S.work.scroll.slice() } : null),
    pics: () => Object.assign({ shown: shown.size }, stats),
    reload: () => { DS.loaded = false; MS.loaded = false; designsLoad(); mastersLoad(); },
    forget: () => { DS.byId.clear(); DS.dirty = true; DS.loaded = false; DS.next = null; DS.sub = false; MS.ix = { rows: [], sig: '' }; MS.bySku.clear(); MS.loaded = false; MS.sub = false; shown.clear(); designFull.clear(); recentList = null; recentFor = ''; stats.asked = 0; }
  };
  return { panel, query, counts, tokens, designRow, masterRow, msOf, noOrder, open: openRow, place: placeRow, probe, version: 1 };
});
