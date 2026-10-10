/* charm-nest-sandbox-browser.js — what the sandbox leaves in THIS BROWSER, and the one engine that clears, counts and guards
   it (Paul, 10 Oct 2026: "None of these options actually fully delete all memory of the Sandbox testing ... ALL other data
   should be wiped").

   The list of stores is not here: it is the "browser" half of charm-nest-sandbox-families.js (CharmNestSandboxFamilies
   .browser(), one entry per store, plain data). This file only knows how to work each KIND of store, so a store added to
   that list is cleared by every reset, counted in Settings, held back from writing during a reset, and checked by the guard
   test (tests/charm-nest/sandbox-wipe-browser.cjs) with no other change:

     kind "localStorage" / "sessionStorage"  every key that matches `match` (a RegExp source) but the names in `keep`
     kind "sharedList"      a localStorage list that production shares; only its items carrying the sandbox's flag go
                            (lists: [{ key | prefix, flag: "sandbox" | "body.sandbox" }])
     kind "sharedMap"       a localStorage object that production shares; only the ids that start with idPrefix go
     kind "flagged"         a single localStorage object (a label handed to the printer) that goes when it carries the flag
     kind "setting"         fields of the page's settings (cn.settings) put back to their defaults ({ fields: { name: default } })
     kind "idb"             keys (and key prefixes) of one IndexedDB object store (the saved workspace)
     kind "idbDatabases"    whole IndexedDB databases whose name matches
     kind "cache"           Cache API caches whose name matches
     kind "memory"          what the page holds in memory: counted and cleared through hooks the page gives (setHooks)
     kind "station"         the Design Station's own storage (another origin: cleared by the station when the sorter asks)

   What a reset NEVER matches: any key without the sandbox's mark (the real side), the epoch (cn.resetEpoch.sandbox, which
   tells every tab a reset happened), the Hold mark and the last-reset note (they are the reset's own).

   sweepSync()  → how many entries it removed or cut (localStorage, sessionStorage, settings; never throws)
   sweep()      → { keys, parts, other, memory }: the same, and the IndexedDB parts, databases and caches too
   left()       → [{ key, label, n }] one row per store, READ ONLY: how many entries this browser holds of it now (n null: not
                  countable from this page)
   scrub(area, key, value) → null (the write must not happen) or the value to write: the guard a page puts in front of
                  localStorage.setItem / sessionStorage.setItem while a reset runs, so no timer or queue writes the old records back
   Nothing here touches the network, and nothing here is a secret. */
(function (root, factory) {
  const api = factory(root);
  if (typeof module === "object" && module.exports) module.exports = api; else root.CharmNestSandboxBrowser = api;
})(typeof self !== "undefined" ? self : globalThis, function (root) {
  "use strict";
  let hooks = {};
  const registry = () => root.CharmNestSandboxFamilies || (typeof require === "function" ? require("./charm-nest-sandbox-families.js") : null);
  const entries = () => { try { const r = registry(); return r ? r.browser() : []; } catch (_) { return []; } };
  const area = name => { try { return root[name] || null; } catch (_) { return null; } };
  const keysOf = st => { const out = []; try { for (let i = 0; i < st.length; i++) { const k = st.key(i); if (k != null) out.push(k); } } catch (_) {} return out; };
  const parse = v => { try { return JSON.parse(v); } catch (_) { return undefined; } };
  const isMap = v => !!v && typeof v === "object" && !Array.isArray(v);
  const flagOf = (item, flag) => !!item && String(flag).split(".").reduce((o, p) => (o == null ? o : o[p]), item) === true;
  const matcher = e => { try { return new RegExp(e.match); } catch (_) { return /(?!)/; } };
  const kept = (e, k) => (e.keep || []).indexOf(k) >= 0;
  const AREAS = { localStorage: "localStorage", sessionStorage: "sessionStorage" };

  /** The keys of a "localStorage"/"sessionStorage" entry that a reset removes. */
  const marked = (e, st) => { const re = matcher(e); return keysOf(st).filter(k => re.test(k) && !kept(e, k)); };
  /** The keys a "sharedList" entry watches: [{ key, flag }]. */
  const listKeys = (e, st) => {
    const all = keysOf(st), out = [];
    for (const l of e.lists || []) for (const k of l.key ? all.filter(x => x === l.key) : all.filter(x => x.indexOf(l.prefix) === 0)) out.push({ key: k, flag: l.flag });
    return out;
  };

  /* ── the per-kind work on the synchronous stores: count (read only), cut (write) and scrub (what may be written) ── */
  const KINDS = {
    localStorage: {
      count: e => { const st = area("localStorage"); return st ? marked(e, st).length : 0; },
      cut: e => { const st = area("localStorage"); let n = 0; if (st) for (const k of marked(e, st)) { try { st.removeItem(k); n++; } catch (_) {} } return n; },
      scrub: (e, a, k, v) => (a === "localStorage" && matcher(e).test(k) && !kept(e, k) ? null : v)
    },
    sessionStorage: {
      count: e => { const st = area("sessionStorage"); return st ? marked(e, st).length : 0; },
      cut: e => { const st = area("sessionStorage"); let n = 0; if (st) for (const k of marked(e, st)) { try { st.removeItem(k); n++; } catch (_) {} } return n; },
      scrub: (e, a, k, v) => (a === "sessionStorage" && matcher(e).test(k) && !kept(e, k) ? null : v)
    },
    sharedList: {
      count: e => { const st = area("localStorage"); let n = 0; if (st) for (const l of listKeys(e, st)) { const a = parse(st.getItem(l.key)); if (Array.isArray(a)) n += a.filter(x => flagOf(x, l.flag)).length; } return n; },
      cut: e => {
        const st = area("localStorage"); let n = 0; if (!st) return 0;
        for (const l of listKeys(e, st)) {
          const a = parse(st.getItem(l.key)); if (!Array.isArray(a)) continue;
          const rest = a.filter(x => !flagOf(x, l.flag)); if (rest.length === a.length) continue;
          n += a.length - rest.length; try { if (rest.length) st.setItem(l.key, JSON.stringify(rest)); else st.removeItem(l.key); } catch (_) {}
        }
        return n;
      },
      scrub: (e, a, k, v) => {
        if (a !== "localStorage") return v;
        const l = (e.lists || []).find(x => x.key ? x.key === k : k.indexOf(x.prefix) === 0); if (!l) return v;
        const arr = parse(v); if (!Array.isArray(arr)) return v;
        const rest = arr.filter(x => !flagOf(x, l.flag)); return rest.length === arr.length ? v : JSON.stringify(rest);
      }
    },
    sharedMap: {
      count: e => { const st = area("localStorage"); let n = 0; if (st) for (const m of e.maps || []) { const o = parse(st.getItem(m.key)); if (isMap(o)) n += Object.keys(o).filter(id => id.indexOf(m.idPrefix) === 0).length; } return n; },
      cut: e => {
        const st = area("localStorage"); let n = 0; if (!st) return 0;
        for (const m of e.maps || []) {
          const o = parse(st.getItem(m.key)); if (!isMap(o)) continue;
          const ids = Object.keys(o).filter(id => id.indexOf(m.idPrefix) === 0); if (!ids.length) continue;
          for (const id of ids) delete o[id]; n += ids.length;
          try { if (Object.keys(o).length) st.setItem(m.key, JSON.stringify(o)); else st.removeItem(m.key); } catch (_) {}
        }
        return n;
      },
      scrub: (e, a, k, v) => {
        if (a !== "localStorage") return v;
        const m = (e.maps || []).find(x => x.key === k); if (!m) return v;
        const o = parse(v); if (!isMap(o)) return v;
        const ids = Object.keys(o).filter(id => id.indexOf(m.idPrefix) === 0); if (!ids.length) return v;
        for (const id of ids) delete o[id]; return JSON.stringify(o);
      }
    },
    flagged: {
      count: e => { const st = area("localStorage"); let n = 0; if (st) for (const k of e.keys || []) if (flagOf(parse(st.getItem(k)), e.flag)) n++; return n; },
      cut: e => { const st = area("localStorage"); let n = 0; if (st) for (const k of e.keys || []) if (flagOf(parse(st.getItem(k)), e.flag)) { try { st.removeItem(k); n++; } catch (_) {} } return n; },
      scrub: (e, a, k, v) => (a === "localStorage" && (e.keys || []).indexOf(k) >= 0 && flagOf(parse(v), e.flag) ? null : v)
    },
    setting: {
      count: e => { const st = area("localStorage"), o = st ? parse(st.getItem(e.settingsKey)) : null; return isMap(o) ? Object.keys(e.fields || {}).filter(f => o[f] !== undefined && o[f] !== e.fields[f]).length : 0; },
      cut: e => {
        const st = area("localStorage"), o = st ? parse(st.getItem(e.settingsKey)) : null; if (!isMap(o)) return 0;
        const f = Object.keys(e.fields || {}).filter(x => o[x] !== undefined && o[x] !== e.fields[x]); if (!f.length) return 0;
        for (const x of f) o[x] = e.fields[x]; try { st.setItem(e.settingsKey, JSON.stringify(o)); } catch (_) { return 0; } return f.length;
      },
      scrub: (e, a, k, v) => {
        if (a !== "localStorage" || k !== e.settingsKey) return v;
        const o = parse(v); if (!isMap(o)) return v;
        let hit = false; for (const x of Object.keys(e.fields || {})) if (o[x] !== undefined && o[x] !== e.fields[x]) { o[x] = e.fields[x]; hit = true; }
        return hit ? JSON.stringify(o) : v;
      }
    }
  };

  /* ── IndexedDB (never created by a count: only a database that exists is opened) ── */
  const idbNames = async () => { try { const idb = root.indexedDB; if (!idb || typeof idb.databases !== "function") return null; return (await idb.databases()).map(d => d.name); } catch (_) { return null; } };
  const reqP = rq => new Promise((res, rej) => { rq.onsuccess = () => res(rq.result); rq.onerror = () => rej(rq.error); });
  const idbOpen = e => new Promise((resolve, reject) => {
    // the same schema the page's own Session creates, so whichever opens a fresh database first leaves it as the page expects
    const rq = root.indexedDB.open(e.db, e.version || 1);
    rq.onupgradeneeded = () => { try { if (!rq.result.objectStoreNames.contains(e.objectStore)) rq.result.createObjectStore(e.objectStore); } catch (_) {} };
    rq.onsuccess = () => resolve(rq.result); rq.onerror = () => reject(rq.error); rq.onblocked = () => reject(new Error("blocked"));
  });
  const idbDoomed = (e, keys) => keys.filter(k => typeof k === "string" && ((e.keys || []).indexOf(k) >= 0 || (e.prefixes || []).some(p => k.indexOf(p) === 0)));
  async function idbKeys(e) {
    if (!root.indexedDB) return { db: null, keys: [] };
    const names = await idbNames(); if (names && names.indexOf(e.db) < 0) return { db: null, keys: [] };
    const db = await idbOpen(e);
    try { if (!db.objectStoreNames.contains(e.objectStore)) return { db, keys: [] }; const store = db.transaction(e.objectStore, "readonly").objectStore(e.objectStore); if (typeof store.getAllKeys !== "function") return { db, keys: [] }; return { db, keys: idbDoomed(e, await reqP(store.getAllKeys())) }; }
    catch (err) { try { db.close(); } catch (_) {} throw err; }
  }
  async function idbCount(e) { let r; try { r = await idbKeys(e); } catch (_) { return null; } try { return r.keys.length; } finally { try { r.db && r.db.close(); } catch (_) {} } }
  async function idbCut(e) {
    let r; try { r = await idbKeys(e); } catch (_) { return 0; }
    try {
      if (!r.db || !r.keys.length) return 0;
      await new Promise((resolve, reject) => { const tx = r.db.transaction(e.objectStore, "readwrite"), st = tx.objectStore(e.objectStore); r.keys.forEach(k => st.delete(k)); tx.oncomplete = () => resolve(); tx.onerror = () => reject(tx.error); tx.onabort = () => reject(tx.error || new Error("interrupted")); });
      return r.keys.length;
    } catch (_) { return 0; } finally { try { r.db && r.db.close(); } catch (_) {} }
  }
  const dbDoomed = async e => { const names = await idbNames(), re = matcher(e); return (names || []).filter(n => typeof n === "string" && re.test(n) && !kept(e, n)); };
  async function dbCut(e) {
    let n = 0;
    for (const name of await dbDoomed(e)) { try { await new Promise(res => { const rq = root.indexedDB.deleteDatabase(name); rq.onsuccess = rq.onerror = rq.onblocked = () => res(); }); n++; } catch (_) {} }
    return n;
  }
  const cacheNames = async e => { try { if (!root.caches || typeof root.caches.keys !== "function") return []; const re = matcher(e); return (await root.caches.keys()).filter(n => re.test(n) && !kept(e, n)); } catch (_) { return []; } };
  async function cacheCut(e) { let n = 0; for (const name of await cacheNames(e)) { try { if (await root.caches.delete(name)) n++; } catch (_) {} } return n; }

  /* ── the public engine ── */
  function sweepSync() {
    let n = 0;
    for (const e of entries()) { const k = KINDS[e.kind]; if (!k) continue; try { n += k.cut(e); } catch (_) {} }
    return n;
  }
  async function sweep() {
    const out = { keys: sweepSync(), parts: 0, other: 0, memory: 0 };
    for (const e of entries()) {
      try {
        if (e.kind === "idb") out.parts += await idbCut(e);
        else if (e.kind === "idbDatabases") out.other += await dbCut(e);
        else if (e.kind === "cache") out.other += await cacheCut(e);
        else if (e.kind === "memory" && hooks.memoryClear) out.memory += (await hooks.memoryClear()) || 0;
      } catch (_) {}
    }
    return out;
  }
  async function left() {
    const rows = [];
    for (const e of entries()) {
      let n = null;
      try {
        const k = KINDS[e.kind];
        if (k) n = k.count(e);
        else if (e.kind === "idb") n = await idbCount(e);
        else if (e.kind === "idbDatabases") n = (await dbDoomed(e)).length;
        else if (e.kind === "cache") n = (await cacheNames(e)).length;
        else if (e.kind === "memory") n = hooks.memoryCount ? await hooks.memoryCount() : 0;
      } catch (_) { n = null; }
      rows.push({ key: e.key, label: e.label, n });
    }
    return rows;
  }
  function scrub(a, key, value) {
    let v = value;
    for (const e of entries()) {
      const k = KINDS[e.kind]; if (!k || !k.scrub) continue;
      if ((e.kind === "localStorage" || e.kind === "sessionStorage") && AREAS[e.kind] !== a) continue;
      try { v = k.scrub(e, a, String(key), v); } catch (_) {}
      if (v === null) return null;
    }
    return v;
  }
  /** Whether a localStorage / sessionStorage key is one of the sandbox's own marked keys (kept ones excluded). */
  function isMarked(a, key) {
    return entries().some(e => (e.kind === a) && matcher(e).test(String(key)) && !kept(e, String(key)));
  }
  return { sweepSync, sweep, left, scrub, isMarked, entries, setHooks: h => { hooks = Object.assign({}, h || {}); } };
});
