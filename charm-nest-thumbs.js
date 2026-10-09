/* Pictures that stay — the loading layer under every design thumbnail (Paul, 9 Oct 2026: "on every refresh, I have to go
   through the downloading of all the thumbnails ... I need fast loads while scrolling").

   What it did before: the Master tab drew each tile by downloading that design's .ai through the asset function (no-store,
   never cached: one function call and about 13 KB each), reading it and drawing it, two at a time, first come first served.
   Every tile that had scrolled past was queued behind the last, so after a fast scroll the tiles on screen waited at the back
   of hundreds ("Loading preview..." on every card), and a refresh started from nothing. The Library's charm tiles asked the
   same function for their stored PNG on every load (Cache-Control private, no-cache: one conditional call per picture).

   What this does, without changing how a card is drawn (the callers still draw it, this only decides when and whether):
     1 · a finished picture is kept on this computer (IndexedDB, with a small in-memory layer on top), under a key that names
         the design AND the drawing code that made it, so a refresh, a new tab or a bad connection shows what was seen with no
         request and no drawing, and a re-indexed design or a changed drawing never shows an old picture;
     2 · only what is on screen and a little way ahead is asked for, the nearest to the middle of the screen first, ahead in the
         scroll direction before behind; a tile that leaves that band before its turn is dropped from the queue, and a tile
         that is only passing is not asked for at all (it must stay in the band for a moment);
     3 · the store never grows without limit: oldest-used pictures go first beyond the caps, pictures of an older drawing
         code are removed, and nothing unused for 45 days is kept.
   It reads nothing from Firestore and calls no function of its own. No IndexedDB (a private window, blocked site data): the
   page still works, from the in-memory layer alone. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(root);
  else root.CharmNestThumbs = factory(root);
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const DB_NAME = 'cn-thumbs', STORE = 'pics', INDEX = 'idx', SCHEMA = 1;
  const LIMITS = { entries: 9000, bytes: 120 * 1024 * 1024, idleDays: 45, trimTo: 0.85 };
  const MEM_MAX = 1200;                       // pictures kept as data URLs for this page (about 16 KB each)
  const SETTLE_MS = 110;                      // a tile asked for only after it has stayed this long in the band (a fast scroll passes it by)
  const IDLE_MS = 220;                        // what is only AHEAD of the screen (not on it) is asked for once the scrolling has been still this long
  const day = 86400000;

  /* ── which drawing code made a picture ── */
  // The pictures are made by these files, and every one of them is bumped (?v=) when it changes, so their addresses are the
  // version of the drawing: a changed drawing gets new keys and the old pictures are never shown again.
  // (charm-nest-background.js carries the address of the compute worker that does the drawing, so it is part of it too.
  //  DRAW_REV: bump it by hand when the way a design is READ or drawn changes in a file that is not listed here, e.g. readMasterCharm in the bridge)
  const DRAWERS = /charm-nest-(?:pdf|geom|vector|pair|pair-thumb|background)\.js/, DRAW_REV = 1;
  let versionCache = null;
  function drawingVersion() {
    if (versionCache !== null) return versionCache;
    let s = 'v' + SCHEMA + '.' + DRAW_REV;
    try { s += [...root.document.querySelectorAll('script[src]')].map(e => e.getAttribute('src') || '').filter(u => DRAWERS.test(u)).sort().join('|'); } catch (_) { /* no document: the key is the schema alone */ }
    let h = 5381; for (let i = 0; i < s.length; i++) h = ((h * 33) ^ s.charCodeAt(i)) >>> 0;
    return (versionCache = h.toString(36));
  }
  /** A key for a design's drawn picture: its file, which version of it, and the drawing code. */
  const designKey = (path, hashOrNull, stampOrNull) => ['m', drawingVersion(), path, hashOrNull == null ? '' : hashOrNull, stampOrNull == null ? '' : stampOrNull].join('|');
  /** A key for a stored PNG that is not versioned by itself: good for a week, then asked for again. */
  const weeklyKey = id => ['w', String(Math.floor(Date.now() / (7 * day))), id].join('|');

  /* ── the in-memory layer ── */
  const mem = new Map();
  const remember = (key, url) => { mem.delete(key); mem.set(key, url); while (mem.size > MEM_MAX) mem.delete(mem.keys().next().value); };
  const peek = key => { const u = key ? mem.get(key) : null; if (u) { mem.delete(key); mem.set(key, u); } return u || ''; };

  /* ── the on-disk layer (IndexedDB; every call may answer "nothing" and the page goes on) ── */
  let dbp = null;
  function open() {
    if (dbp) return dbp;
    return (dbp = new Promise(res => {
      try {
        const idb = root.indexedDB; if (!idb) return res(null);
        const rq = idb.open(DB_NAME, 1);
        rq.onupgradeneeded = () => { const db = rq.result; db.createObjectStore(STORE, { keyPath: 'k' }); db.createObjectStore(INDEX, { keyPath: 'k' }); };
        rq.onsuccess = () => { const db = rq.result; db.onversionchange = () => { try { db.close(); } catch (_) {} dbp = null; }; res(db); };
        rq.onerror = rq.onblocked = () => res(null);
      } catch (_) { res(null); }
    }));
  }
  const tx = (db, stores, mode) => db.transaction(stores, mode);
  async function diskGet(key) {
    const db = await open(); if (!db) return null;
    return new Promise(res => {
      try { const rq = tx(db, STORE).objectStore(STORE).get(key); rq.onsuccess = () => res(rq.result || null); rq.onerror = () => res(null); } catch (_) { res(null); }
    });
  }
  async function diskPut(key, bytes, type, meta) {
    const db = await open(); if (!db) return false;
    return new Promise(res => {
      try {
        const t = tx(db, [STORE, INDEX], 'readwrite');
        t.objectStore(STORE).put({ k: key, b: bytes, t: type });
        t.objectStore(INDEX).put({ k: key, n: bytes.byteLength, at: Date.now(), g: meta && meta.g || '', v: meta && meta.v || '' });
        t.oncomplete = () => res(true); t.onerror = t.onabort = () => res(false);
      } catch (_) { res(false); }
    });
  }
  // a picture seen again is marked as used, in one write for all that were seen in the last few seconds
  const touched = new Set(); let touchTimer = 0;
  function touch(key) {
    if (!key) return; touched.add(key);
    if (touchTimer) return;
    touchTimer = setTimeout(async () => {
      touchTimer = 0; const keys = [...touched]; touched.clear();
      const db = await open(); if (!db) return;
      try {
        const t = tx(db, INDEX, 'readwrite'), s = t.objectStore(INDEX), now = Date.now();
        for (const k of keys) { const rq = s.get(k); rq.onsuccess = () => { const r = rq.result; if (r && now - r.at > 3600000) { r.at = now; s.put(r); } }; }
      } catch (_) { /* a missed mark only makes a picture look older than it is */ }
    }, 4000);
  }
  /** Keep the store small: drop what an older drawing made, what has not been used for 45 days, then the oldest-used beyond the caps. */
  let trimming = null;
  function trim(force) {
    if (trimming && !force) return trimming;
    return (trimming = (async () => {
      const db = await open(); if (!db) return { removed: 0 };
      const rows = await new Promise(res => { try { const rq = tx(db, INDEX).objectStore(INDEX).getAll(); rq.onsuccess = () => res(rq.result || []); rq.onerror = () => res([]); } catch (_) { res([]); } });
      const now = Date.now(), ver = drawingVersion(), drop = new Set();
      let live = [];
      for (const r of rows) { if ((r.g === 'm' && r.v && r.v !== ver) || (r.g === 'w' && r.v && r.v !== thisWeek()) || now - r.at > LIMITS.idleDays * day) drop.add(r.k); else live.push(r); }
      let bytes = live.reduce((a, r) => a + r.n, 0), count = live.length;
      if (count > LIMITS.entries || bytes > LIMITS.bytes) {
        live.sort((a, b) => a.at - b.at);
        const maxN = Math.floor(LIMITS.entries * LIMITS.trimTo), maxB = Math.floor(LIMITS.bytes * LIMITS.trimTo);
        for (const r of live) { if (count <= maxN && bytes <= maxB) break; drop.add(r.k); count--; bytes -= r.n; }
      }
      if (drop.size) await new Promise(res => { try { const t = tx(db, [STORE, INDEX], 'readwrite'); for (const k of drop) { t.objectStore(STORE).delete(k); t.objectStore(INDEX).delete(k); mem.delete(k); } t.oncomplete = t.onerror = t.onabort = () => res(); } catch (_) { res(); } });
      return { removed: drop.size, entries: count, bytes };
    })().finally(() => { setTimeout(() => { trimming = null; }, 60000); }));
  }
  let sinceTrim = 0, trimmedOnce = false;
  function maybeTrim() { if (!trimmedOnce) { trimmedOnce = true; setTimeout(() => trim().catch(() => {}), 6000); } else if (++sinceTrim >= 250) { sinceTrim = 0; trim().catch(() => {}); } }

  /* ── pictures as bytes on disk, as data URLs in the page ── */
  function toBytes(dataUrl) {
    const m = /^data:([^;,]*)(;base64)?,(.*)$/s.exec(dataUrl || ''); if (!m || !m[2]) return null;
    const bin = atob(m[3]), out = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return { bytes: out.buffer, type: m[1] || 'image/png' };
  }
  function toUrl(bytes, type) {
    const a = new Uint8Array(bytes); let s = ''; for (let i = 0; i < a.length; i += 8192) s += String.fromCharCode.apply(null, a.subarray(i, i + 8192));
    return 'data:' + (type || 'image/png') + ';base64,' + btoa(s);
  }
  const groupOf = key => (key && key[1] === '|' ? key[0] : '');
  const versionOf = key => (groupOf(key) === 'm' || groupOf(key) === 'w' ? key.split('|')[1] : '');
  const thisWeek = () => String(Math.floor(Date.now() / (7 * day)));

  /** What is already kept for this key: the page's memory, then the disk. '' when nothing is. */
  async function lookup(key) {
    if (!key) return '';
    if (!trimmedOnce) maybeTrim();
    const hit = peek(key); if (hit) return hit;
    const row = await diskGet(key).catch(() => null);
    if (!row || !row.b) return '';
    const url = toUrl(row.b, row.t); remember(key, url); touch(key); return url;
  }
  const making = new Map();
  /** Make the picture (one at a time per key, whoever asks), keep it, answer it. A failure is kept nowhere. */
  function make(key, produce) {
    if (key && making.has(key)) return making.get(key);
    const p = (async () => {
      const url = await produce();
      if (key && url && typeof url === 'string' && url.startsWith('data:')) {
        remember(key, url);
        try { const b = toBytes(url); if (b) diskPut(key, b.bytes, b.type, { g: groupOf(key), v: versionOf(key) }).then(ok => { if (ok) maybeTrim(); }); } catch (_) { /* kept in memory only */ }
      }
      return url;
    })().finally(() => { if (key) making.delete(key); });
    if (key) making.set(key, p);
    return p;
  }
  /** get-or-make, for a caller that has no grid to schedule. */
  async function picture(key, produce) { return (await lookup(key)) || make(key, produce); }

  /** A stored PNG (or any image) as a data URL, read once through the page's own cache. */
  async function fetchDataUrl(url, init) {
    const r = await fetch(url, Object.assign({ cache: 'default' }, init || {}));
    if (!r.ok) throw new Error('Could not read the picture (' + r.status + ').');
    const blob = await r.blob(); if (!blob.size) throw new Error('The picture was empty.');
    return new Promise((res, rej) => { const f = new FileReader(); f.onload = () => res(f.result); f.onerror = () => rej(f.error || new Error('Could not read the picture.')); f.readAsDataURL(blob); });
  }

  /* ── the loader: the pictures of one grid, nearest first, only near the screen ── */
  const scrollParent = el => { for (let p = el && el.parentElement; p; p = p.parentElement) { const o = root.getComputedStyle(p).overflowY; if ((o === 'auto' || o === 'scroll' || o === 'overlay') && p.scrollHeight > p.clientHeight + 1) return p; } return null; };
  /**
   * mount(grid, o): o.selector (the tiles waiting for a picture), o.keyOf(tile) (the key, '' for none), o.produce(tile) → Promise<data URL>
   * (the caller's own drawing or download), o.paint(tile, url), o.fail(tile, error); o.concurrency (6), o.margin ('70% 0px').
   * o.keep === false: the caller's produce keeps its own picture (it calls picture() itself under the same key); this only looks it up.
   * Returns { stop() }. A tile with a picture already in memory is painted before this returns (a redrawn grid does not blink).
   */
  function mount(grid, o) {
    const tiles = [...grid.querySelectorAll(o.selector)];
    const conc = o.concurrency || 6, st = { stopped: false, running: 0, looking: 0, timer: 0, top: 0, dir: 1, lastScroll: 0 };
    const waiting = new Map();                // tile → { since, key, state: 'new' | 'looking' | 'queued' | 'making' }
    const done = new WeakSet(); let io = null;
    const finish = (tile, url) => { waiting.delete(tile); done.add(tile); try { io && io.unobserve(tile); } catch (_) {} if (tile.isConnected) { try { o.paint(tile, url); } catch (_) { /* a caller's paint must not stop the loader */ } } };
    let rest = [];
    for (const t of tiles) { const hit = peek(o.keyOf(t)); if (hit) finish(t, hit); else rest.push(t); }
    const scroller = scrollParent(grid), vp = () => { if (scroller) { const r = scroller.getBoundingClientRect(); return { top: r.top, bottom: r.bottom }; } return { top: 0, bottom: root.innerHeight || 800 }; };
    const sc = scroller || root;
    const onScroll = () => { const y = scroller ? scroller.scrollTop : root.scrollY; if (y !== st.top) { st.dir = y > st.top ? 1 : -1; st.top = y; st.lastScroll = Date.now(); } };
    try { st.top = scroller ? scroller.scrollTop : root.scrollY; sc.addEventListener('scroll', onScroll, { passive: true }); } catch (_) { /* no scrolling to follow */ }
    const dist = tile => {                    // how far a tile is from being looked at: 0 on screen (reading order), more above or below it; behind the scroll counts double
      const v = vp(), r = tile.getBoundingClientRect();
      if (r.bottom >= v.top && r.top <= v.bottom) return ((r.top - v.top) / Math.max(1, v.bottom - v.top)) * 0.5 + r.left * 1e-6;   // (always under 1)
      const gap = r.bottom < v.top ? v.top - r.bottom : r.top - v.bottom, behind = (r.bottom < v.top) === (st.dir > 0);
      return 1 + gap * (behind ? 2 : 1);
    };
    const look = async () => {                // what is already kept is painted at once, whatever the queue is doing
      while (!st.stopped && st.looking < 8) {
        let next = null; for (const [t, w] of waiting) if (w.state === 'new') { next = t; break; }
        if (!next) return;
        const w = waiting.get(next); w.state = 'looking'; st.looking++;
        lookup(w.key).then(url => { if (url) finish(next, url); else if (waiting.get(next) === w) w.state = 'queued'; }, () => { if (waiting.get(next) === w) w.state = 'queued'; })
          .finally(() => { st.looking--; look(); pump(); });
      }
    };
    const pump = () => {
      if (st.stopped) return; clearTimeout(st.timer); st.timer = 0;
      if (st.running >= conc) return;
      const now = Date.now(); let wait = 0; const ready = [], still = now - st.lastScroll >= IDLE_MS;
      for (const [t, w] of waiting) { if (w.state !== 'queued') continue; const age = now - w.since; if (age >= SETTLE_MS) ready.push(t); else wait = Math.max(wait, SETTLE_MS - age); }
      if (ready.length) {
        const d = new Map(ready.map(t => [t, dist(t)])); ready.sort((a, b) => d.get(a) - d.get(b));
        for (const t of ready) {
          if (st.running >= conc) break;
          if (d.get(t) >= 1 && !still) { wait = Math.max(wait, IDLE_MS - (now - st.lastScroll)); continue; }   // ahead of the screen while it is still moving: later
          const w = waiting.get(t); if (!w || w.state !== 'queued') continue;
          w.state = 'making'; st.running++;
          (o.keep === false ? Promise.resolve().then(() => o.produce(t)) : make(w.key, () => o.produce(t))).then(url => { if (url) finish(t, url); else { waiting.delete(t); o.fail && o.fail(t, new Error('No picture')); } },
            err => { waiting.delete(t); if (t.isConnected && o.fail) { try { o.fail(t, err); } catch (_) {} } })
            .finally(() => { st.running--; pump(); });
        }
      }
      if (wait) st.timer = setTimeout(pump, wait + 5);
    };
    const enter = t => { if (!waiting.has(t) && !done.has(t)) waiting.set(t, { since: Date.now(), key: o.keyOf(t) || '', state: 'new' }); };
    const leave = t => { const w = waiting.get(t); if (w && (w.state === 'new' || w.state === 'queued' || w.state === 'looking')) waiting.delete(t); };
    if (typeof IntersectionObserver === 'function') {
      io = new IntersectionObserver(es => { for (const e of es) (e.isIntersecting ? enter : leave)(e.target); look(); pump(); }, { root: scroller, rootMargin: o.margin || '70% 0px' });
      for (const t of rest) io.observe(t);
    } else { for (const t of rest) enter(t); look(); }
    return { stop() { st.stopped = true; clearTimeout(st.timer); try { io && io.disconnect(); } catch (_) {} try { sc.removeEventListener('scroll', onScroll); } catch (_) {} waiting.clear(); }, pending: () => waiting.size };
  }

  /** Images that carry data-pic="<address>" (and data-pic-key): shown from the kept copy, else read once and kept. A picture that cannot be read goes to the address itself, as it did before. */
  function mountImages(host, o) {
    o = o || {};
    return mount(host, {
      selector: o.selector || 'img[data-pic]', concurrency: o.concurrency || 4,
      keyOf: img => img.dataset.picKey || '',
      produce: img => fetchDataUrl(img.dataset.pic),
      paint: (img, url) => { img.src = url; },
      fail: img => { if (img.dataset.pic) img.src = img.dataset.pic; }
    });
  }

  async function stats() {
    const db = await open(); if (!db) return { entries: 0, bytes: 0, mem: mem.size, disk: false };
    const rows = await new Promise(res => { try { const rq = tx(db, INDEX).objectStore(INDEX).getAll(); rq.onsuccess = () => res(rq.result || []); rq.onerror = () => res([]); } catch (_) { res([]); } });
    return { entries: rows.length, bytes: rows.reduce((a, r) => a + r.n, 0), mem: mem.size, disk: true };
  }
  async function clear() {
    mem.clear(); const db = await open(); if (!db) return;
    await new Promise(res => { try { const t = tx(db, [STORE, INDEX], 'readwrite'); t.objectStore(STORE).clear(); t.objectStore(INDEX).clear(); t.oncomplete = t.onerror = t.onabort = () => res(); } catch (_) { res(); } });
  }
  return { designKey, weeklyKey, drawingVersion, lookup, make, picture, peek, fetchDataUrl, mount, mountImages, trim, stats, clear, LIMITS, _resetVersion: () => { versionCache = null; }, _forget: () => mem.clear() };
});
