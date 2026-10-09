/* The charm library kept on this computer (Paul, 9 Oct 2026: "on every refresh, I have to go through the downloading of all the
   thumbnails ... I need fast loads ... especially if you have weak Internet").

   What it did before: every refresh read the WHOLE charm index again (Master.load in charm-nest-bridge.js: about 6,960 Firestore
   documents in three parts, about 270 KB compressed and 3.2 MB read out for each, plus the master file records), although the page
   already knew how to skip that within one visit by comparing the index's signature (its count and the stamps of the newest
   write and the newest edit: charmNestLibrary.js masterIndexSig). The copy it compared with lived in memory only.

   What this does: keeps the last WHOLE answer (entries, files, and the signatures they were read with) in IndexedDB. A page that
   starts with none asks the cloud for the signatures only (masterListFiles with ifFilesSig: about a dozen reads, a few hundred
   bytes) and believes the copy only when they are unchanged. A changed signature, from any writer (a re-index, a person's facing
   or pair patch, a removal, a master file added or taken away), drops the copy and reads everything again, as before.
   It never shows what it has not checked: nothing here paints the copy before the cloud has agreed.
     · one record only, written whole in one transaction (what an older page code or a smaller store held goes with it);
     · the key names this page's code (the bridge and this file as addressed in the page, which are bumped with ?v= whenever they
       change, and the drawing code's revision, charm-nest-thumbs.js DRAW_REV), and the cloud's own shape of an entry is part of
       the signature, so a code change on either side never reuses an old record;
     · a record that does not read back as it was written (truncated, not JSON, a count that is off) is removed and ignored;
     · a record older than 30 days or larger than 48 MB is not kept.
   It calls no function and reads nothing from Firestore itself. No IndexedDB (a private window, blocked site data) or any other
   failure: read() answers null and the page reads everything, exactly as it did before this file existed. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(root);
  else root.CharmNestMasterCache = factory(root);
})(typeof self !== 'undefined' ? self : this, function (root) {
  'use strict';
  const DB_NAME = 'cn-master-list', STORE = 'copy', SCHEMA = 1;
  const LIST_REV = 1;                              // bump by hand when what is stored, or how a stored copy is read, changes in a way no script address shows
  const LIMITS = { chars: 48 * 1024 * 1024, idleDays: 30, waitMs: 4000 };
  const CODE = /charm-nest-(?:bridge|master-cache)\.js/;
  const day = 86400000;

  let revCache = null;
  /** Which code made and reads the copy: this page's bridge and this file (their addresses carry ?v=), and the drawing code's revision. */
  function revision() {
    if (revCache !== null) return revCache;
    let s = 'v' + SCHEMA + '.' + LIST_REV;
    try { s += [...root.document.querySelectorAll('script[src]')].map(e => e.getAttribute('src') || '').filter(u => CODE.test(u)).sort().join('|'); } catch (_) { /* no document: the schema alone */ }
    try { if (root.CharmNestThumbs && root.CharmNestThumbs.drawingVersion) s += '|' + root.CharmNestThumbs.drawingVersion(); } catch (_) { /* the module is optional */ }
    let h = 5381; for (let i = 0; i < s.length; i++) h = ((h * 33) ^ s.charCodeAt(i)) >>> 0;
    return (revCache = h.toString(36));
  }
  const keyNow = () => 'copy|' + revision();

  /* ── the store (every call may answer "nothing" and the page goes on) ── */
  let dbp = null;
  function open() {
    if (dbp) return dbp;
    return (dbp = new Promise(res => {
      try {
        const idb = root.indexedDB; if (!idb) return res(null);
        const rq = idb.open(DB_NAME, 1);
        rq.onupgradeneeded = () => { try { rq.result.createObjectStore(STORE, { keyPath: 'k' }); } catch (_) { /* opened below */ } };
        rq.onsuccess = () => { const db = rq.result; db.onversionchange = () => { try { db.close(); } catch (_) {} dbp = null; }; res(db); };
        rq.onerror = rq.onblocked = () => res(null);
      } catch (_) { res(null); }
    })).then(db => { if (!db) dbp = null; return db; });
  }
  const once = (rq, pick) => new Promise(res => { try { rq.onsuccess = () => res(pick ? pick(rq.result) : rq.result); rq.onerror = () => res(null); } catch (_) { res(null); } });
  const done = t => new Promise(res => { try { t.oncomplete = () => res(true); t.onerror = t.onabort = () => res(false); } catch (_) { res(false); } });
  const within = (p, ms) => Promise.race([p, new Promise(r => setTimeout(() => r(null), ms))]);

  /** A stored record as the page uses it, or null when it is not good for this page. */
  function decode(row) {
    try {
      if (!row || row.v !== SCHEMA || row.k !== keyNow() || row.rev !== revision()) return null;
      if (!(Date.now() - row.at < LIMITS.idleDays * day) || row.at > Date.now() + day) return null;
      if (typeof row.entriesJson !== 'string' || typeof row.filesJson !== 'string' || !row.index || typeof row.index !== 'object') return null;
      const entries = JSON.parse(row.entriesJson), files = JSON.parse(row.filesJson);
      if (!Array.isArray(entries) || !Array.isArray(files) || entries.length !== row.n) return null;
      for (const e of entries) if (!e || typeof e !== 'object' || typeof e.sku !== 'string' || !e.sku) return null;
      return { entries, index: row.index, files, filesSig: row.filesSig || null, at: row.at };
    } catch (_) { return null; }
  }

  /** The last whole copy kept for this page's code, or null (none, another code's, old, unreadable, no IndexedDB). It is a copy to CHECK, not to show. */
  async function read() {
    try {
      return await within((async () => {
        const db = await open(); if (!db) return null;
        const row = await once(db.transaction(STORE, 'readonly').objectStore(STORE).get(keyNow()));
        const out = decode(row);
        if (row && !out) forget().catch(() => {});   // (a record under this page's key that does not read back: removed, the next full read writes a good one)
        return out;
      })(), LIMITS.waitMs);
    } catch (_) { return null; }
  }

  /** Keep a whole answer: { index, entries, files, filesSig }. It is serialised NOW (the page goes on changing its own objects),
      written in one transaction that also removes every other record. Answers whether it was kept. */
  async function save(rec) {
    try {
      if (!rec || !rec.index || typeof rec.index !== 'object' || !Array.isArray(rec.entries) || !rec.entries.length) return false;
      const entriesJson = JSON.stringify(rec.entries), filesJson = JSON.stringify(Array.isArray(rec.files) ? rec.files : []);
      if (entriesJson.length + filesJson.length > LIMITS.chars) { await forget(); return false; }
      const row = { k: keyNow(), v: SCHEMA, rev: revision(), at: Date.now(), n: rec.entries.length, index: JSON.parse(JSON.stringify(rec.index)), filesSig: rec.filesSig || null, entriesJson, filesJson };
      const db = await open(); if (!db) return false;
      const t = db.transaction(STORE, 'readwrite'), s = t.objectStore(STORE);
      s.clear(); s.put(row);
      return await done(t);
    } catch (_) { return false; }
  }

  /** The master file records changed, the index did not: replace those two fields of the kept record, whole and untouched otherwise. */
  async function saveFiles(files, filesSig) {
    try {
      const db = await open(); if (!db) return false;
      const filesJson = JSON.stringify(Array.isArray(files) ? files : []);
      return await new Promise(res => {
        try {
          const t = db.transaction(STORE, 'readwrite'), s = t.objectStore(STORE), rq = s.get(keyNow());
          rq.onsuccess = () => {
            const row = rq.result;
            if (row && row.v === SCHEMA && row.rev === revision() && typeof row.entriesJson === 'string') { row.filesJson = filesJson; row.filesSig = filesSig || null; s.put(row); }
            else t.abort();                          // (no copy of this page's code to put them in)
          };
          t.oncomplete = () => res(true); t.onerror = t.onabort = () => res(false);
        } catch (_) { res(false); }
      });
    } catch (_) { return false; }
  }

  /** Remove whatever is kept. */
  async function forget() {
    try {
      const db = await open(); if (!db) return false;
      const t = db.transaction(STORE, 'readwrite'); t.objectStore(STORE).clear();
      return await done(t);
    } catch (_) { return false; }
  }
  async function stats() {
    try {
      const db = await open(); if (!db) return { kept: false, disk: false };
      const rows = await once(db.transaction(STORE, 'readonly').objectStore(STORE).getAll()) || [];
      return { kept: rows.length > 0, disk: true, records: rows.length, chars: rows.reduce((a, r) => a + ((r.entriesJson || '').length + (r.filesJson || '').length), 0), n: rows[0] ? rows[0].n : 0 };
    } catch (_) { return { kept: false, disk: false }; }
  }
  return { read, save, saveFiles, forget, stats, revision, LIMITS, _resetRevision: () => { revCache = null; dbp = null; } };
});
