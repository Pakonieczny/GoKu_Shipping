/* charm-nest-store.js — the records kept on THIS computer (Paul, 28 Oct 2026: "as responsive as Photoshop … leverage local storage … keep the API
   calls to the bare minimum"; plan: the local-first design, phase 1).

   What the page did before: every sheet window waited for a whole getSheet (a full read, Storage link lookups of up to 1.5 s, the readiness worked out
   again), the Library read its lists in full (about 1.3 MB) whenever they were a minute old, and a reload began from nothing: the lists read again,
   every master design downloaded and parsed again.

   What this does: it keeps what the page has read, each record under the revision the cloud gave it (`rev`: the record's update time, which moves
   with every write to it, whoever made it), in IndexedDB on this computer, with the same records in memory. A surface draws from here at once and
   asks the cloud one small question: "has it changed since rev?" (charmNestLibrary.js LOCAL_READS: getSheet ifRev, listSheets / setList ifRev and
   since, getMany). Only what really changed is read again.
     · two databases of records, one per world: cn-local.production and cn-local.sandbox. They never mix (Paul, 10 Oct 2026: the sandbox leaves
       nothing behind): the world is fixed when the page starts and checked on every write, a write while the sandbox is being reset (or from a page
       another tab's reset made stale) is refused, a sandbox reset deletes cn-local.sandbox (charm-nest-sandbox-families.js names it), and a database
       made before the last reset is emptied when it is opened;
     · one shared database of content, cn-blobs: master designs as read and parsed, named by their file and version, never by a record of either side;
     · at start the lists the page keeps (the Library's) and their rows are read in one go (getAll and one transaction of gets), so the Library is drawn
       before any call; a whole sheet record is read when its window opens (a few milliseconds);
     · how long (Paul, 28 Oct 2026, decision 3: "45 days for EVERY kind ... customer words may stay on the computer for that long"): whatever was not
       used for 45 days is removed, and the oldest-used go first when the computer's storage runs short (over 70% of what the browser allows: down to
       60%, designs first, then whole sheet records), or when a write is refused for space (freed, tried once more, then left to memory);
     · navigator.storage.persist() is asked for (decision 5: the shop's Chrome keeps site data). Storage Buckets are for the outbox of phase 3, where
       unsent work must survive: everything here is a copy the cloud holds, so it stays in the default bucket, where a sandbox reset finds it;
     · no IndexedDB (a private window, blocked site data, a browser that throws on it): every call works on memory alone, and the page goes the way it
       went before this file existed.
   It calls no function and reads nothing from Firestore itself: the page asks, and gives this what the cloud answered.

   CharmNestStore (window; module.exports in tests)
     init({ world, worldNow, writable, epoch })   world 'production' | 'sandbox'; worldNow() the page's world at a write; writable() false while a write
                                                  must not happen (a reset under way, a stale tab); epoch() the reset epoch. → ready()
     ready() → Promise<{ disk, rows }>             the start read done (at once with no IndexedDB)
     get(kind, id) / rev(kind, id) / has(kind, id) memory, at once ('sheets' list rows, 'sheetFull' whole sheet records, 'sets', 'lists')
     load(kind, id) → Promise<record | null>       memory, else this computer's disk
     put(kind, id, record, rev, { world }) → bool  kept now in memory, on disk at the end of this task; refused (false) for another world or while not writable
     patch(kind, id, fields) → bool                fields merged into the record kept (its rev stays: the cloud's next answer says what it is)
     drop(kind, id)
     list(name) → { kind, ids, since, rev, at }    putList(name, { kind, ids, since, rev })    rows(name) → { rows, missing }
     ask(name) → { wantRev, since, ifRev? }        what a list read sends: since '0.0' (everything) unless this computer holds the list and every row of it
     take(name, kind, answer, field, idOf) → rows | null   a list's answer put together with what is held (unchanged, or only the rows written since),
                                                  kept for next time; null when it cannot be (the page reads it whole)
     shared(key, make) → Promise                   one read in flight per key: a caller asking the same while it is out gets its own copy of the answer
     blob(store, key) → Promise<value | null>      putBlob(store, key, value)   ('parsed' | 'bytes', in cn-blobs)
     on(fn) → off                                  fn({ kind, id, rec, from: 'here' | 'tab' }) after a put, a patch or a drop, here or in another tab
     sweep() · stats() · freeze() · discard()      discard: this page's own sandbox reset (memory emptied, the database deleted, nothing written again) */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(root);
  else root.CharmNestStore = factory(root);
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const SCHEMA = 1;                                      // bump when a store is added or a row's shape changes: what is kept is a copy, so an upgrade may drop it
  const KINDS = ['sheets', 'sheetFull', 'sets', 'lists'];
  const BLOBS = ['parsed', 'bytes'];
  const day = 86400000;
  const LIMITS = {
    keepDays: 45,                                        // Paul, 28 Oct 2026 (decision 3): every kind
    full: 600, sheets: 8000, sets: 4000, lists: 50,      // rows of each kind at most (the oldest-used beyond go)
    parsed: 2000, bytes: 800,                            // designs in cn-blobs at most
    memFull: 24, memBlobs: 80,                           // whole sheet records and designs held in memory (the rest are read from disk when asked)
    quotaHigh: 0.7, quotaTo: 0.6,                        // share of the browser's quota: above the first, trimmed to the second
    usedEveryMs: 6 * 3600000,                            // a use is written to disk at most this often per row (a 45-day horizon needs no finer)
    openMs: 4000                                         // a database that does not open by then is left alone: memory only
  };

  /* ── the page's world and its guard ── */
  let cfg = { world: 'production', worldNow: null, writable: null, epoch: null };
  let world = 'production', frozen = false, inited = false;
  const isWorld = w => w === 'production' || w === 'sandbox';
  const epochNow = () => { try { if (cfg.epoch) return String(cfg.epoch() || ''); } catch (_) { return ''; } try { return root.localStorage.getItem('cn.resetEpoch.' + world) || ''; } catch (_) { return ''; } };
  /** May this page write now? Its world unchanged since it started, nothing frozen, and the page's own guard (a reset under way, a stale tab). */
  function canWrite(w) {
    if (frozen) return false;
    if (w != null && w !== world) return false;
    try { if (cfg.worldNow && cfg.worldNow() !== world) return false; } catch (_) { return false; }
    try { if (cfg.writable && cfg.writable() === false) return false; } catch (_) { return false; }
    return true;
  }

  /* ── memory: what the page draws from (the disk below is only its copy across reloads) ── */
  const mem = Object.fromEntries(KINDS.map(k => [k, new Map()]));   // id → { rec, rev, saved, used, diskUsed }
  const blobMem = Object.fromEntries(BLOBS.map(k => [k, new Map()]));
  const lru = (map, key, max) => { const v = map.get(key); if (v === undefined) return v; map.delete(key); map.set(key, v); while (map.size > max) map.delete(map.keys().next().value); return v; };
  const listeners = new Set();
  const tell = ev => { for (const fn of [...listeners]) { try { fn(ev); } catch (e) { console.warn('local store: listener', e); } } };

  /* ── the disk (every call may answer "nothing", and the page goes on from memory) ── */
  const idbOf = () => { try { const f = root.indexedDB; return f && typeof f.open === 'function' ? f : null; } catch (_) { return null; } };
  const dbName = () => 'cn-local.' + world;
  const once = rq => new Promise(res => { try { rq.onsuccess = () => res(rq.result); rq.onerror = () => res(null); } catch (_) { res(null); } });
  const within = (p, ms) => Promise.race([p, new Promise(r => setTimeout(() => r(null), ms))]);
  let dbp = null, blobp = null, diskOn = false;
  const closed = new Set();   // (databases this page let go of: never opened again before it reloads)
  /* A database is opened at whatever version it has; one that lacks a store this code needs (new, or made by an older or another code) is opened once
     more at the next version to make it. What is kept is a copy of the cloud's: a store of another shape is made again, never migrated (meta.schema
     says which rows a database holds: boot empties one of another schema). */
  const shaped = (db, stores) => stores.concat(['uses']).every(s => db.objectStoreNames.contains(s));
  function openDb(name, stores) {
    const idb = idbOf(); if (!idb || closed.has(name)) return Promise.resolve(null);
    const attempt = version => new Promise(res => {
      try {
        const rq = version ? idb.open(name, version) : idb.open(name);
        rq.onupgradeneeded = () => {
          const db = rq.result;
          for (const s of [...db.objectStoreNames]) if (!stores.includes(s) && s !== 'uses') db.deleteObjectStore(s);
          for (const s of stores) if (!db.objectStoreNames.contains(s)) db.createObjectStore(s, { keyPath: 'k' });
          if (!db.objectStoreNames.contains('uses')) { const u = db.createObjectStore('uses', { keyPath: 'k' }); u.createIndex('used', 'used'); u.createIndex('kindUsed', ['kind', 'used']); }
        };
        rq.onsuccess = () => {
          const db = rq.result;
          if (!shaped(db, stores)) { const v = db.version; try { db.close(); } catch (_) {} return res(version ? null : attempt(v + 1)); }
          // another tab deletes this database (a sandbox reset) or a newer page upgrades it: this page lets go, and goes on in memory until it reloads
          db.onversionchange = () => { try { db.close(); } catch (_) {} closed.add(name); if (name === 'cn-blobs') blobp = null; else { dbp = null; diskOn = false; } };
          res(db);
        };
        rq.onerror = rq.onblocked = () => res(null);
      } catch (_) { res(null); }
    });
    return within(attempt(0), LIMITS.openMs);
  }
  const records = () => (dbp || (dbp = openDb(dbName(), KINDS.concat(['meta'])).then(db => { if (!db) dbp = null; return db; })));
  const blobsDb = () => (blobp || (blobp = openDb('cn-blobs', BLOBS).then(db => { if (!db) blobp = null; return db; })));
  const errName = e => (e && (e.name || (e.target && e.target.error && e.target.error.name))) || '';
  const isQuota = e => /QuotaExceeded/i.test(errName(e)) || /quota/i.test(String(e && e.message));

  /* ── writes: gathered in this task, written in one transaction at its end ── */
  const queue = new Map();   // 'kind\u0000id' → { kind, id, row | null (delete), use }
  let flushTimer = 0, flushing = Promise.resolve(), useTimer = 0;
  const qk = (kind, id) => kind + '\u0000' + id;
  function enqueue(kind, id, row, use) { queue.set(qk(kind, id), { kind, id, row, use: use || 0 }); if (!flushTimer) flushTimer = setTimeout(flush, 0); }
  function flush() {
    flushTimer = 0;
    if (!queue.size) return flushing;
    const batch = [...queue.values()]; queue.clear();
    return (flushing = flushing.then(() => writeBatch(batch, 0)).catch(e => console.warn('local store: write', e)));
  }
  async function writeBatch(batch, tries) {
    if (!canWrite()) return false;
    const db = await records(); if (!db) return false;
    const ok = await new Promise(res => {
      try {
        const stores = [...new Set(batch.map(b => b.kind))].concat(['uses']), t = db.transaction(stores, 'readwrite'), uses = t.objectStore('uses');
        for (const b of batch) {
          const s = t.objectStore(b.kind), uk = b.kind + '|' + b.id;
          if (b.row === null) { s.delete(b.id); uses.delete(uk); }
          else { if (b.row !== undefined) s.put(b.row); if (b.use) uses.put({ k: uk, kind: b.kind, id: b.id, used: b.use }); }
        }
        t.oncomplete = () => res(true);
        t.onerror = t.onabort = e => res(isQuota(t.error || e) ? 'quota' : false);
      } catch (e) { res(isQuota(e) ? 'quota' : false); }
    });
    if (ok === 'quota' && tries < 1) { await squeeze(true); return writeBatch(batch, tries + 1); }   // (room made, tried once more; then left to memory)
    if (ok === true) { diskOn = true; announce(batch); }
    return ok === true;
  }

  /* ── other tabs of this computer, same world: told what was written, they read it from disk ── */
  let channel = null;
  function announce(batch) {
    if (!channel) return;
    const items = batch.filter(b => b.row !== undefined).map(b => [b.kind, b.id, b.row ? b.row.rev || '' : null]);
    if (items.length) { try { channel.postMessage({ t: 'put', items }); } catch (_) {} }
  }
  async function heard(items) {
    const want = new Map();
    for (const [kind, id, rev] of items || []) {
      if (!mem[kind]) continue;
      if (rev === null) { if (mem[kind].delete(id)) tell({ kind, id, rec: null, from: 'tab' }); continue; }
      const had = mem[kind].get(id);
      if (had && had.rev === rev && rev) continue;
      if (kind === 'sheetFull') { mem[kind].delete(id); continue; }   // (read from disk when its window asks: a whole record is not read for nothing)
      if (!want.has(kind)) want.set(kind, []); want.get(kind).push(id);
    }
    const db = want.size ? await records() : null; if (!db) return;
    const got = await new Promise(res => {
      const out = [];
      try { const t = db.transaction([...want.keys()], 'readonly'); for (const [kind, ids] of want) for (const id of ids) { const rq = t.objectStore(kind).get(id); rq.onsuccess = () => { if (rq.result && rq.result.rec) out.push([kind, id, rq.result]); }; } t.oncomplete = t.onerror = t.onabort = () => res(out); } catch (_) { res(out); }
    });
    for (const [kind, id, row] of got) { mem[kind].set(id, { rec: row.rec, rev: row.rev || '', saved: row.saved || 0, used: Date.now(), diskUsed: Date.now() }); tell({ kind, id, rec: row.rec, from: 'tab' }); }
  }
  async function diskRow(kind, id) {
    const db = await records(); if (!db) return null;
    try { return await once(db.transaction(kind, 'readonly').objectStore(kind).get(id)); } catch (_) { return null; }
  }

  /* ── the start: the lists and their rows, read in one go ── */
  let readyP = null;
  function init(o) {
    if (inited) return readyP;
    inited = true;
    cfg = Object.assign(cfg, o || {});
    world = isWorld(cfg.world) ? cfg.world : 'production';
    try { channel = typeof root.BroadcastChannel === 'function' ? new root.BroadcastChannel('cn-store:' + world) : null; if (channel) channel.onmessage = e => { if (e && e.data && e.data.t === 'put') heard(e.data.items); }; } catch (_) { channel = null; }
    try { const st = root.navigator && root.navigator.storage; if (st && st.persisted) st.persisted().then(p => p || (st.persist && st.persist())).catch(() => {}); } catch (_) {}
    readyP = boot().catch(e => { console.warn('local store: start', e); return { disk: false, rows: 0 }; });
    readyP.then(() => { try { setTimeout(() => sweep().catch(() => {}), 9000); } catch (_) {} });
    try { root.addEventListener('pagehide', () => flush()); } catch (_) {}   // (what this task kept goes to disk before the page leaves, as far as the browser lets it)
    return readyP;
  }
  async function boot() {
    const db = await records(); if (!db) return { disk: false, rows: 0 };
    const t0 = Date.now(), ep = epochNow();
    const [meta, lists] = await new Promise(res => {
      try { const t = db.transaction(['meta', 'lists'], 'readonly'); Promise.all([once(t.objectStore('meta').get('meta')), once(t.objectStore('lists').getAll())]).then(res); } catch (_) { res([null, []]); }
    });
    // a database of another world (it cannot be, by its name) or made before the last sandbox reset: emptied before anything is read from it
    if (!meta || meta.world !== world || meta.schema !== SCHEMA || (meta.epoch || '') !== ep) {
      if (!canWrite()) return { disk: false, rows: 0 };
      await new Promise(res => { try { const t = db.transaction(KINDS.concat(['meta', 'uses']), 'readwrite'); for (const s of KINDS.concat(['uses'])) t.objectStore(s).clear(); t.objectStore('meta').put({ k: 'meta', world, schema: SCHEMA, epoch: ep, clientId: clientId(), at: Date.now() }); t.oncomplete = t.onerror = t.onabort = () => res(); } catch (_) { res(); } });
      diskOn = true;
      return { disk: true, rows: 0, fresh: true, ms: Date.now() - t0 };
    }
    // the lists' rows (the Library's sheets and sets) in one transaction of gets
    const want = new Map();
    for (const l of lists || []) {
      if (!l || !l.rec || !Array.isArray(l.rec.ids) || !mem[l.rec.kind]) continue;
      if (!mem.lists.has(l.k)) mem.lists.set(l.k, { rec: l.rec, rev: l.rev || '', saved: l.saved || 0, used: Date.now(), diskUsed: Date.now() });
      for (const id of l.rec.ids) { if (!want.has(l.rec.kind)) want.set(l.rec.kind, new Set()); want.get(l.rec.kind).add(String(id)); }
    }
    let n = 0;
    if (want.size) await new Promise(res => {
      try {
        const t = db.transaction([...want.keys()], 'readonly'), now = Date.now();
        for (const [kind, ids] of want) {
          const s = t.objectStore(kind);
          for (const id of ids) { const rq = s.get(id); rq.onsuccess = () => { const r = rq.result; if (r && r.rec && !mem[kind].has(id)) { mem[kind].set(id, { rec: r.rec, rev: r.rev || '', saved: r.saved || 0, used: now, diskUsed: 0 }); n++; } }; }
        }
        t.oncomplete = t.onerror = t.onabort = () => res();
      } catch (_) { res(); }
    });
    diskOn = true;
    return { disk: true, rows: n, ms: Date.now() - t0 };
  }
  function clientId() { try { const c = root.crypto; if (c && c.randomUUID) return c.randomUUID(); } catch (_) {} return Date.now().toString(36) + Math.random().toString(36).slice(2, 10); }

  /* ── reading and writing records ── */
  const known = kind => !!mem[kind];
  const sid = id => (id == null ? '' : String(id));
  function slot(kind, id) {
    const m = mem[kind]; if (!m) return null;
    const s = kind === 'sheetFull' ? lru(m, id, LIMITS.memFull) : m.get(id);
    if (s) { s.used = Date.now(); if (s.used - (s.diskUsed || 0) > LIMITS.usedEveryMs) markUse(kind, id, s); }
    return s || null;
  }
  // a use is written in the next batch, or by itself a little later (one small row in a store of its own: a whole record is never written again for it)
  function markUse(kind, id, s) {
    s.diskUsed = s.used;
    if (!canWrite()) return;
    const q = queue.get(qk(kind, id));
    if (q) { q.use = s.used; return; }
    queue.set(qk(kind, id), { kind, id, row: undefined, use: s.used });
    if (!useTimer) useTimer = setTimeout(() => { useTimer = 0; flush(); }, 30000);
  }
  const get = (kind, id) => { const s = slot(kind, sid(id)); return s ? s.rec : null; };
  const rev = (kind, id) => { const s = mem[kind] && mem[kind].get(sid(id)); return s ? s.rev : ''; };
  const has = (kind, id) => !!(mem[kind] && mem[kind].has(sid(id)));
  async function load(kind, id) {
    id = sid(id); if (!known(kind) || !id) return null;
    const s = slot(kind, id); if (s) return s.rec;
    const row = await diskRow(kind, id);
    if (!row || !row.rec) return null;
    if (mem[kind].has(id)) return mem[kind].get(id).rec;   // (put meanwhile: the newer one)
    const now = Date.now();
    mem[kind].set(id, { rec: row.rec, rev: row.rev || '', saved: row.saved || 0, used: now, diskUsed: 0 });
    if (kind === 'sheetFull') lru(mem[kind], id, LIMITS.memFull);
    markUse(kind, id, mem[kind].get(id));
    return row.rec;
  }
  /** Keep a record the cloud answered, under its revision. The object is kept as given and written at the end of this task (a caller that changes it
      in place later puts it again; one that only holds it again need not: get() hands the kept object back). */
  function put(kind, id, rec, r, opts) {
    id = sid(id); if (!known(kind) || !id || rec == null || typeof rec !== 'object') return false;
    if (!canWrite(opts && opts.world)) return false;
    const now = Date.now(), rv = r == null ? '' : String(r);
    mem[kind].set(id, { rec, rev: rv, saved: now, used: now, diskUsed: now });
    if (kind === 'sheetFull') lru(mem[kind], id, LIMITS.memFull);
    enqueue(kind, id, { k: id, rev: rv, saved: now, rec }, now);
    tell({ kind, id, rec, from: 'here' });
    return true;
  }
  /** Fields the page learned of a record (a seal, a step, a live answer's readiness): merged into the record kept, its revision unchanged. */
  function patch(kind, id, fields) {
    id = sid(id); const s = mem[kind] && mem[kind].get(id);
    if (!s || !fields || typeof fields !== 'object' || !canWrite()) return false;
    const rec = Object.assign({}, s.rec, fields), now = Date.now();
    Object.assign(s, { rec, saved: now, used: now, diskUsed: now });
    enqueue(kind, id, { k: id, rev: s.rev, saved: now, rec }, now);
    tell({ kind, id, rec, from: 'here' });
    return true;
  }
  function drop(kind, id) {
    id = sid(id); if (!known(kind) || !id || !canWrite()) return false;
    const had = mem[kind].delete(id);
    enqueue(kind, id, null);
    if (had) tell({ kind, id, rec: null, from: 'here' });
    return true;
  }
  /* a list: which rows, in which order, and what to ask the cloud with next time (since: its upTo; rev: the Library's revision) */
  const list = name => get('lists', name);
  function putList(name, l) {
    if (!l || !known(l.kind) || !Array.isArray(l.ids)) return false;
    const rec = { kind: l.kind, ids: l.ids.map(String), since: l.since || '', rev: l.rev || '', at: Date.now() };
    return put('lists', name, rec, rec.rev);
  }
  /** The rows of a list as this computer holds them; missing: how many it names that are not held (the page then reads it whole). */
  function rows(name) {
    const l = list(name); if (!l) return { rows: [], missing: 0, list: null };
    const out = []; let missing = 0;
    for (const id of l.ids) { const r = get(l.kind, id); if (r) out.push(r); else missing++; }
    return { rows: out, missing, list: l };
  }

  /** What a list read asks the cloud (charmNestLibrary.js LOCAL_READS): the list's revision and how far it was read, when this computer holds it whole;
      else everything once (since '0.0': every row, each with its revision, and how far that read saw). */
  function ask(name) {
    const r = rows(name), l = r.list;
    return l && !r.missing && l.since ? { wantRev: true, since: l.since, ...(l.rev ? { ifRev: l.rev } : {}) } : { wantRev: true, since: '0.0' };
  }
  /** A list's answer (rows under `field`, each named by idOf) put together with what is held, and kept for next time: unchanged → the held rows; ids → every row
      in order, the ones sent new, the rest as held; neither (an older server) → the rows sent, as they are. null when a row it names is held nowhere. */
  function take(name, kind, ans, field, idOf) {
    if (!ans || typeof ans !== 'object' || !known(kind)) return null;
    const l = list(name), sent = Array.isArray(ans[field]) ? ans[field] : [], revs = ans.revs && typeof ans.revs === 'object' ? ans.revs : {};
    if (ans.unchanged) { const r = rows(name); if (!l || r.missing) return null; if (ans.rev && ans.rev !== l.rev) putList(name, Object.assign({}, l, { rev: ans.rev })); return r.rows; }
    for (const x of sent) { const id = idOf(x); if (id != null) put(kind, id, x, revs[id] != null ? revs[id] : ''); }
    if (!Array.isArray(ans.ids)) { putList(name, { kind, ids: sent.map(idOf).filter(x => x != null), since: '', rev: '' }); return sent; }
    const out = [];
    for (const id of ans.ids) { const r = get(kind, id); if (!r) return null; out.push(r); }
    putList(name, { kind, ids: ans.ids, since: ans.upTo || '', rev: ans.rev || '' });
    return out;
  }
  /* the same read asked again while it is on its way waits for that one (charm-nest-1.html api()); each caller after the first gets a copy of the answer,
     so a page that changes what it was given changes only its own */
  const inflight = new Map();
  const copyOf = v => { try { return typeof root.structuredClone === 'function' ? root.structuredClone(v) : JSON.parse(JSON.stringify(v)); } catch (_) { return v; } };
  function shared(key, make) {
    const hit = inflight.get(key); if (hit) return hit.then(copyOf);
    const p = Promise.resolve().then(make);
    inflight.set(key, p);
    const out = () => { if (inflight.get(key) === p) inflight.delete(key); };
    p.then(out, out);
    return p;
  }

  /* ── content: designs as read and parsed (cn-blobs, shared by both worlds: named by the file and its version, never by a record) ── */
  async function blob(store, key) {
    key = sid(key); if (!blobMem[store] || !key) return null;
    const m = lru(blobMem[store], key, LIMITS.memBlobs); if (m !== undefined) return m;
    const db = await blobsDb(); if (!db) return null;
    let row = null; try { row = await once(db.transaction(store, 'readonly').objectStore(store).get(key)); } catch (_) { row = null; }
    if (!row || row.v === undefined) return null;
    blobMem[store].set(key, row.v); lru(blobMem[store], key, LIMITS.memBlobs);
    blobUse(store, key);
    return row.v;
  }
  const blobUsed = new Map();
  function blobUse(store, key) {
    const k = store + '|' + key, now = Date.now(); if (now - (blobUsed.get(k) || 0) < LIMITS.usedEveryMs) return;
    blobUsed.set(k, now);
    blobsDb().then(db => { if (!db || frozen) return; try { db.transaction('uses', 'readwrite').objectStore('uses').put({ k, kind: store, id: key, used: now }); } catch (_) {} });
  }
  /** Keep a design (any structured-clonable value: typed arrays and ArrayBuffers as they are). Never refused for the world: it names no record. */
  async function putBlob(store, key, value) {
    key = sid(key); if (!blobMem[store] || !key || value === undefined || frozen) return false;
    blobMem[store].set(key, value); lru(blobMem[store], key, LIMITS.memBlobs);
    const db = await blobsDb(); if (!db) return false;
    const write = () => new Promise(res => {
      try {
        const t = db.transaction([store, 'uses'], 'readwrite'), now = Date.now();
        t.objectStore(store).put({ k: key, v: value, saved: now }); t.objectStore('uses').put({ k: store + '|' + key, kind: store, id: key, used: now });
        blobUsed.set(store + '|' + key, now);
        t.oncomplete = () => res(true); t.onerror = t.onabort = e => res(isQuota(t.error || e) ? 'quota' : false);
      } catch (e) { res(isQuota(e) ? 'quota' : (console.warn('local store: a design not kept', e), false)); }
    });
    let ok = await write();
    if (ok === 'quota') { await squeeze(true); ok = await write(); }
    return ok === true;
  }

  /* ── what goes: unused for 45 days, the oldest-used beyond the counts, and the oldest-used while space is short ── */
  // the rows of one kind used before `before` (oldest first), at most `max` of them; with no `before`, the oldest `max`
  function oldest(db, kind, max, before) {
    return new Promise(res => {
      const out = [];
      try {
        const range = IDBKeyRange.bound([kind, 0], [kind, before == null ? Infinity : before], false, before != null);
        const rq = db.transaction('uses', 'readonly').objectStore('uses').index('kindUsed').openCursor(range);
        rq.onsuccess = () => { const c = rq.result; if (!c || out.length >= max) return res(out); out.push(c.value.id); c.continue(); };
        rq.onerror = () => res(out);
      } catch (_) { res(out); }
    });
  }
  function count(db, kind) {
    return new Promise(res => { try { const rq = db.transaction('uses', 'readonly').objectStore('uses').index('kindUsed').count(IDBKeyRange.bound([kind, 0], [kind, Infinity])); rq.onsuccess = () => res(rq.result || 0); rq.onerror = () => res(0); } catch (_) { res(0); } });
  }
  // (inMem: the page's memory loses them too, for what was not used in 45 days; what goes for room stays in memory while this page is open)
  function remove(db, kind, ids, inMem) {
    if (!ids.length) return Promise.resolve(0);
    return new Promise(res => {
      try {
        const t = db.transaction([kind, 'uses'], 'readwrite');
        for (const id of ids) { t.objectStore(kind).delete(id); t.objectStore('uses').delete(kind + '|' + id); if (inMem) inMem.delete(id); }
        t.oncomplete = () => res(ids.length); t.onerror = t.onabort = () => res(0);
      } catch (_) { res(0); }
    });
  }
  let sweeping = null;
  /** Remove what was not used for 45 days and what passes the counts, in both databases; then trim if the computer's storage is short. */
  function sweep(now) {
    if (sweeping) return sweeping;
    return (sweeping = (async () => {
      const t = now || Date.now(), before = t - LIMITS.keepDays * day, out = { removed: 0 };
      const db = canWrite() ? await records() : null;
      // (every row is written with its use, in the same transaction: a row without one cannot be left behind)
      if (db) for (const kind of KINDS) {
        out.removed += await remove(db, kind, await oldest(db, kind, 100000, before), mem[kind]);
        const cap = LIMITS[kind === 'sheetFull' ? 'full' : kind], n = await count(db, kind);
        if (n > cap) out.removed += await remove(db, kind, await oldest(db, kind, n - cap), null);
      }
      const bd = frozen ? null : await blobsDb();
      if (bd) for (const s of BLOBS) {
        out.removed += await remove(bd, s, await oldest(bd, s, 100000, before), blobMem[s]);
        const n = await count(bd, s); if (n > LIMITS[s]) out.removed += await remove(bd, s, await oldest(bd, s, n - LIMITS[s]), null);
      }
      out.squeezed = await squeeze(false);
      return out;
    })().finally(() => { sweeping = null; }));
  }
  /** Space: over 70% of the browser's quota (or a write refused for space: force), the oldest-used go, designs first, then whole sheet records, then
      the rest, until 60% (a fifth of a store at a time, the estimate read again after each). */
  async function squeeze(force) {
    let est = null; try { const st = root.navigator && root.navigator.storage; est = st && st.estimate ? await st.estimate() : null; } catch (_) { est = null; }
    const over = e => e && e.quota > 0 && e.usage / e.quota > (force ? LIMITS.quotaTo : LIMITS.quotaHigh);
    if (!force && !over(est)) return 0;
    let removed = 0;
    const order = [['b', 'parsed'], ['b', 'bytes'], ['r', 'sheetFull'], ['r', 'sheets'], ['r', 'sets']];
    for (const [where, kind] of order) {
      const db = where === 'b' ? await blobsDb() : await records(); if (!db) continue;
      for (let round = 0; round < 5; round++) {
        const n = await count(db, kind); if (!n) break;
        removed += await remove(db, kind, await oldest(db, kind, Math.max(1, Math.ceil(n / 5))), null);
        try { est = await root.navigator.storage.estimate(); } catch (_) { est = null; }
        if (!est || est.usage / est.quota <= LIMITS.quotaTo) return removed;
        if (force && round >= 1) return removed;   // (a refused write: room for it, not a clear-out)
      }
    }
    return removed;
  }

  /* ── a reset ── */
  /** Nothing is written again from this page (it reloads): a reset under way, or a tab stood down. */
  function freeze() { frozen = true; queue.clear(); clearTimeout(flushTimer); flushTimer = 0; clearTimeout(useTimer); useTimer = 0; }
  /** This page's own sandbox reset: memory emptied, the sandbox's database closed and deleted, nothing written again. Production's is never deleted here. */
  async function discard() {
    freeze();
    for (const k of KINDS) mem[k].clear();
    if (world !== 'sandbox') return false;
    try { const db = dbp ? await dbp : null; if (db) db.close(); } catch (_) {}
    closed.add(dbName()); dbp = null;
    const idb = idbOf(); if (!idb) return false;
    return new Promise(res => { try { const rq = idb.deleteDatabase(dbName()); rq.onsuccess = () => res(true); rq.onerror = () => res(false); rq.onblocked = () => res(true); } catch (_) { res(false); } });
  }
  async function stats() {
    const out = { world, disk: diskOn, frozen, mem: Object.fromEntries(KINDS.map(k => [k, mem[k].size])), rows: {}, blobs: {} };
    const db = await records(); if (db) for (const k of KINDS) out.rows[k] = await count(db, k);
    const bd = await blobsDb(); if (bd) for (const s of BLOBS) out.blobs[s] = await count(bd, s);
    try { const st = root.navigator && root.navigator.storage; if (st && st.persisted) out.persisted = await st.persisted(); } catch (_) {}
    return out;
  }
  return {
    init, ready: () => readyP || Promise.resolve({ disk: false, rows: 0 }), world: () => world, disk: () => diskOn,
    get, rev, has, load, put, patch, drop, list, putList, rows, ask, take, shared, blob, putBlob,
    on: fn => { listeners.add(fn); return () => listeners.delete(fn); },
    flush, sweep, squeeze, stats, freeze, discard, LIMITS, KINDS
  };
});
