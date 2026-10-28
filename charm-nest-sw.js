/* charm-nest-sw.js — the page's own files and pictures kept on this computer: Charm Nest's service worker (local-first plan, Phase 4;
   Paul, 28 Oct 2026: "Every sheet that opens must be instantaneous ... This must work like a native application"). A reload asked the
   network for about 150 files of the page's own code (each a revalidation in production) before the first screen; with the worker
   the files come from this computer and a warm reload makes at most 3 static requests (tests/charm-nest/service-worker.cjs).

   ONE file, two roles:
     · the worker (registered by charm-nest-1.html as /charm-nest-sw.js, scope /): answers the requests below from Cache Storage;
     · the page (<script src="charm-nest-sw.js?v=…"> in charm-nest-1.html): CNServiceWorker — starts the worker once the page is up and
       idle (never in the way of the boot), hands it what the page already loaded, Settings' "Reload from cloud", the console's off/on.

   KEPT (cache-first: a hit never asks the network)
     · cn-static-<GEN>: a same-origin GET of the app's own files (charm-nest-*, vendor/, lib/, brites-progress.css) whose address names
       its version (?v=…). The version is the key: a new token is a new address, so a page is never handed old code for a new token,
       and the copy kept for the older token of the same file is dropped when the new one is kept.
     · cn-pics-<GEN>: a picture read whole through /.netlify/functions/charmNestAsset whose Storage address names its own version (v= of
       10–16 digits: the function answers such an address with the same bytes for ever, _charmNestAsset.js identify), outside the
       sandbox's files (charmnest/sandbox/): a sheet's picture is downloaded once per computer.
   NEVER KEPT (the network, exactly as without a worker)
     · a page (every navigation: HTML) and every other function call (the API: data belongs to the local record store,
       charm-nest-store.js); where the browser has static routing (Chrome 123+) these never even reach the worker;
     · anything that is not a GET, carries a Range, or names no version: the master designs (charmnest/master/<SKU>.ai|png are
       written again under the same download token, so their address does not name their bytes: the content-addressed blob store
       of the plan, keyed by charmHash, is their place), the engraving fonts (the page asks force-cache: the HTTP cache keeps them),
       emoji-sequences.json (asked no-cache on purpose: it names the emoji font's version), the workers' unversioned imports.
   No precache: the worker ships no list of files. What the page loaded before the worker was running is ADOPTED at idle: the page
   hands over the versioned addresses it used and the worker reads them from the HTTP cache (force-cache), so the next load is warm.
   Sandbox and production share one origin and one worker: the caches hold code, and production's pictures only, nothing of a world.
   LIMITS: each cache is an LRU (CAP below: bytes and entries; the least recently used go first). The index of what is kept and when
   it was last used is one JSON in cn-sw-meta-<GEN>.

   RETIRING IT (any one is enough)
     · every computer, from the page: window.CN_SW = "off" in charm-nest-1.html's <head> (the page itself is never cached, so the next
       load of every computer unregisters the worker and deletes its caches, by a few inline lines that do not need this file);
     · every computer, from the worker: RETIRED = true below (the browser checks this file at every load; the new copy deletes the
       caches and unregisters itself). A new GEN throws every kept file away (each is downloaded once more); VERSION names the copy;
     · one computer: ?sw=off in the address (kept as localStorage cn.sw = "off"; ?sw=on undoes it), or CNServiceWorker.off() in the console;
     · Settings → This computer's copy → Reload from cloud (CNServiceWorker.reloadFromCloud()): the kept files go, the page reloads.
   Automation (navigator.webdriver: the test suites) runs without the worker unless localStorage cn.sw = "on", so a test that routes
   the page's requests is never answered by a worker it did not ask for. */
(function (root) {
  "use strict";
  const VERSION = "2026-10-28.1";   // this copy of the worker (status, Settings)
  const RETIRED = false;            // true: the worker deletes its caches and unregisters itself on every computer (server-side retirement)
  const GEN = "v1";                 // the caches' generation: a new one throws away every kept file
  const STATIC = "cn-static-" + GEN, PICS = "cn-pics-" + GEN, META = "cn-sw-meta-" + GEN;
  const OURS = /^cn-(static|pics|sw-meta)-/;   // this worker's caches, of any generation (an older generation's are deleted when a new worker starts)
  const MB = 1048576, CAP = { [STATIC]: { bytes: 160 * MB, n: 2000 }, [PICS]: { bytes: 200 * MB, n: 5000 } };
  const APP = /^\/(charm-nest-[\w.-]+\.(js|mjs|css|png|jpg|svg|webp|json|wasm)|brites-progress\.css|vendor\/[\w.\/-]+|lib\/[\w.\/-]+)$/;
  const ASSET = "/.netlify/functions/charmNestAsset";
  const SCRIPT = /\/charm-nest-sw\.js(\?|$)/;

  /** What the worker does with a request: "static" or "pic" (kept, cache-first), "" (the network, untouched). Pure.
   *  o: { method, mode, range (a Range header is present), origin (the worker's own) } */
  function classify(href, o) {
    o = o || {};
    if ((o.method || "GET") !== "GET" || o.mode === "navigate" || o.range) return "";
    let u; try { u = new URL(href); } catch (_) { return ""; }
    if (o.origin && u.origin !== o.origin) return "";
    if (u.pathname === ASSET) {
      const p = picPath(u);
      return p && !/^charmnest\/sandbox\//.test(p) ? "pic" : "";
    }
    if (u.pathname.indexOf("/.netlify/") === 0 || !u.searchParams.get("v") || !APP.test(u.pathname)) return "";
    return "static";
  }
  /** The Storage path of a charmNestAsset address whose Storage address names its version (v= 10–16 digits), "" otherwise. */
  function picPath(u) {
    let inner; try { inner = new URL(u.searchParams.get("url") || ""); } catch (_) { return ""; }
    const m = /^\/v0\/b\/[^/]+\/o\/(.+)$/.exec(inner.pathname);
    if (inner.protocol !== "https:" || inner.hostname !== "firebasestorage.googleapis.com" || !m) return "";
    if (!/^\d{10,16}$/.test(inner.searchParams.get("v") || "")) return "";
    let p = ""; try { p = decodeURIComponent(m[1]); } catch (_) { return ""; }
    return /^charmnest\//.test(p) && p.split("/").indexOf("..") < 0 ? p : "";
  }
  /** The file an address is a version of (its older versions are dropped when a newer one is kept). */
  function fileOf(href) {
    try { const u = new URL(href); return u.pathname === ASSET ? "pic:" + picPath(u) : u.pathname; } catch (_) { return href; }
  }
  /** The addresses to drop so a cache keeps within its cap: the least recently used first, down to 90% of the cap. Pure.
   *  entries: [{ url, b: bytes, u: last used (ms) }] · cap: { bytes, n } */
  function evict(entries, cap) {
    let bytes = 0, n = entries.length;
    for (const e of entries) bytes += e.b || 0;
    if (bytes <= cap.bytes && n <= cap.n) return [];
    const out = [];
    for (const e of entries.slice().sort((a, b) => (a.u || 0) - (b.u || 0))) {
      if (bytes <= cap.bytes * 0.9 && n <= cap.n * 0.9) break;
      out.push(e.url); bytes -= e.b || 0; n--;
    }
    return out;
  }

  /* ═══ the worker ═══ */
  function worker(sw) {
    const origin = sw.location.origin, INDEX = origin + "/__cn-sw/index.json";
    // (static routing: a page and every function call but the pictures go straight to the network; the worker is not even started for them)
    const ROUTES = [
      { condition: { requestMode: "navigate" }, source: "network" },
      { condition: { urlPattern: { pathname: ASSET } }, source: "fetch-event" },
      { condition: { urlPattern: { pathname: "/.netlify/*" } }, source: "network" }
    ];
    let idx = null, saving = null, dirty = false, stopped = false;   // (stopped: switched off while pages it controls are still open: it keeps nothing more)
    /** url -> { url, c: cache, f: file, b: bytes, u: last used }: read once per life of the worker; rebuilt from the caches when missing */
    function index() {
      return idx || (idx = (async () => {
        const m = new Map();
        try {
          const r = await (await caches.open(META)).match(INDEX);
          const j = r ? await r.json() : null;
          if (j && j.gen === GEN && Array.isArray(j.e)) { for (const e of j.e) if (e && e.url) m.set(e.url, e); return m; }
          for (const c of [STATIC, PICS]) {
            if (!(await caches.has(c))) continue;
            const cache = await caches.open(c);
            for (const rq of await cache.keys()) { const r2 = await cache.match(rq); m.set(rq.url, { url: rq.url, c, f: fileOf(rq.url), b: +(r2 && r2.headers.get("content-length")) || 0, u: 0 }); }
          }
        } catch (_) {}
        return m;
      })());
    }
    /** The index written back, at most once every 2 s (the caller keeps the worker alive for it with waitUntil). */
    function save() {
      dirty = true;
      if (saving) return saving;
      return (saving = new Promise(r => setTimeout(r, 2000)).then(async () => {
        saving = null; if (!dirty || stopped) return; dirty = false;
        try { const m = await index(); await (await caches.open(META)).put(INDEX, new Response(JSON.stringify({ gen: GEN, v: VERSION, e: [...m.values()] }), { headers: { "Content-Type": "application/json" } })); } catch (_) {}
      }));
    }
    async function touch(url) { const m = await index(), e = m.get(url); if (e) { e.u = Date.now(); return save(); } }
    /** A response kept: the older versions of its file dropped, the cache held to its cap. */
    async function keep(name, url, res) {
      if (stopped) return;
      const cache = await caches.open(name), m = await index();
      const sized = res.headers.get("content-length") ? null : res.clone();
      await cache.put(url, res);
      const b = +res.headers.get("content-length") || (sized ? (await sized.blob()).size : 0), f = fileOf(url);
      const drop = [];
      for (const [u, e] of m) if (e.f === f && u !== url) drop.push(u);
      m.set(url, { url, c: name, f, b, u: Date.now() });
      for (const u of drop.concat(evict([...m.values()].filter(e => e.c === name && e.f !== f), CAP[name]))) {
        const e = m.get(u); m.delete(u); try { await (await caches.open(e && e.c || name)).delete(u); } catch (_) {}
      }
      return save();
    }
    const keepable = res => res && res.status === 200 && (res.type === "basic" || res.type === "default") && !/no-store/i.test(res.headers.get("cache-control") || "");
    async function serve(req, kind, ev) {
      const name = kind === "pic" ? PICS : STATIC;
      let hit = null;
      try { hit = await (await caches.open(name)).match(req.url, { ignoreVary: true }); } catch (_) { hit = null; }
      if (hit) { ev.waitUntil(touch(req.url).catch(() => {})); return hit; }
      const res = await fetch(req);
      if (keepable(res)) ev.waitUntil(keep(name, req.url, res.clone()).catch(() => {}));
      return res;
    }
    /** The versioned addresses a page loaded before the worker ran: read from the HTTP cache (force-cache) and kept, a few at a time. */
    async function adopt(urls) {
      const m = await index(), todo = [];
      for (const url of urls.slice(0, 600)) { const kind = typeof url === "string" ? classify(url, { origin }) : ""; if (kind && !m.has(url) && todo.indexOf(url) < 0) todo.push([url, kind]); }
      let n = 0;
      const run = async () => { for (let t; (t = todo.shift());) { try { const res = await fetch(t[0], { cache: "force-cache", credentials: "same-origin" }); if (keepable(res)) { await keep(t[1] === "pic" ? PICS : STATIC, t[0], res); n++; } } catch (_) {} } };
      await Promise.all([run(), run(), run(), run()]);
      return n;
    }
    async function clear() {
      let n = 0;
      for (const k of await caches.keys()) if (OURS.test(k) && (await caches.delete(k))) n++;
      idx = null; dirty = false;
      return n;
    }
    async function status() {
      const m = await index(), of = c => { let n = 0, bytes = 0; for (const e of m.values()) if (e.c === c) { n++; bytes += e.b || 0; } return { n, bytes }; };
      return { version: VERSION, gen: GEN, retired: RETIRED, static: of(STATIC), pics: of(PICS) };
    }

    sw.addEventListener("install", e => {
      sw.skipWaiting();
      if (!RETIRED && typeof e.addRoutes === "function") { try { e.waitUntil(Promise.resolve(e.addRoutes(ROUTES)).catch(() => {})); } catch (_) {} }
    });
    sw.addEventListener("activate", e => e.waitUntil((async () => {
      const mine = RETIRED ? [] : [STATIC, PICS, META];
      for (const k of await caches.keys()) if (OURS.test(k) && mine.indexOf(k) < 0) await caches.delete(k);
      if (RETIRED) { await sw.registration.unregister(); return; }   // (pages already open keep running; their next load is not controlled)
      await sw.clients.claim();
    })().catch(() => {})));
    sw.addEventListener("fetch", e => {
      if (RETIRED || stopped) return;
      const r = e.request, kind = classify(r.url, { method: r.method, mode: r.mode, range: r.headers.has("range"), origin });
      if (!kind) return;
      // (whatever goes wrong in here, the page gets the network's answer, as it would with no worker)
      e.respondWith(serve(r, kind, e).catch(() => fetch(r)));
    });
    sw.addEventListener("message", e => {
      const d = e.data || {}, port = e.ports && e.ports[0], reply = x => { try { if (port) port.postMessage(x); } catch (_) {} };
      if (d.op === "stop") stopped = true;   // (the page's off switch, before it unregisters the worker)
      const job = d.op === "status" ? status() : d.op === "clear" || d.op === "stop" ? clear().then(n => ({ cleared: n })) : d.op === "adopt" && Array.isArray(d.urls) ? adopt(d.urls).then(n => ({ adopted: n })) : null;
      if (job) e.waitUntil(job.then(reply, err => reply({ error: String(err && err.message || err) })));
    });
  }

  /* ═══ the page ═══ */
  function page(w) {
    if (w.CNServiceWorker) return;
    const nav = w.navigator, sw = nav && nav.serviceWorker, me = w.document.currentScript;
    const scriptUrl = (() => { try { return new URL(me && me.src || "charm-nest-sw.js", w.location.href).pathname; } catch (_) { return "/charm-nest-sw.js"; } })();
    const pref = () => { try { return w.localStorage.getItem("cn.sw") || ""; } catch (_) { return ""; } };
    const setPref = v => { try { if (v) w.localStorage.setItem("cn.sw", v); else w.localStorage.removeItem("cn.sw"); } catch (_) {} };
    /** Whether this page runs the worker, and why not. (The inline lines in charm-nest-1.html's <head> already applied ?sw= and the page's switch.) */
    function wanted() {
      if (!sw || !w.isSecureContext || !w.caches) return { on: false, why: "this browser has no service worker here" };
      if (w.CN_SW === "off") return { on: false, why: "switched off for every computer (charm-nest-1.html)", kill: true };
      if (pref() === "off") return { on: false, why: "switched off on this computer (?sw=on turns it back on)", kill: true };
      if (nav.webdriver && pref() !== "on") return { on: false, why: "automation: off unless localStorage cn.sw = \"on\"" };
      return { on: true, why: "" };
    }
    const ours = r => { const x = r && (r.active || r.waiting || r.installing); return !!(x && SCRIPT.test(x.scriptURL)); };
    async function registration() { try { return (await sw.getRegistrations()).find(ours) || null; } catch (_) { return null; } }
    function ask(target, msg, ms) {
      return new Promise(resolve => {
        if (!target) return resolve(null);
        const ch = new MessageChannel(), t = setTimeout(() => resolve(null), ms || 4000);
        ch.port1.onmessage = e => { clearTimeout(t); resolve(e.data); };
        try { target.postMessage(msg, [ch.port2]); } catch (_) { clearTimeout(t); resolve(null); }
      });
    }
    async function dropCaches() { let n = 0; try { for (const k of await w.caches.keys()) if (OURS.test(k) && (await w.caches.delete(k))) n++; } catch (_) {} return n; }
    /** Unregisters the worker and deletes its caches. -> { unregistered, caches } */
    async function stop() {
      let unregistered = false;
      try { for (const r of await sw.getRegistrations()) if (ours(r)) { if (r.active) await ask(r.active, { op: "stop" }, 2000); unregistered = (await r.unregister()) || unregistered; } } catch (_) {}
      return { unregistered, caches: await dropCaches() };
    }
    /** The versioned files this page loaded, handed to the worker to keep (it reads them from the HTTP cache). */
    function loaded() {
      const out = [];
      try { for (const e of w.performance.getEntriesByType("resource")) if (classify(e.name, { origin: w.location.origin })) out.push(e.name); } catch (_) {}
      return out;
    }
    const idle = (fn, ms) => (typeof w.requestIdleCallback === "function" ? w.requestIdleCallback(fn, { timeout: ms }) : setTimeout(fn, 200));
    let started = null;
    /** Registers the worker once the page has loaded and is idle; then hands it what the page loaded. -> Promise<registration | null> */
    function start() {
      if (started) return started;
      started = new Promise(resolve => {
        const go = () => idle(async () => {
          let reg = null;
          try { reg = await sw.register(scriptUrl, { scope: "/" }); } catch (e) { api.error = String(e && e.message || e); return resolve(null); }   // (no worker: the page goes on as it always did)
          resolve(reg);
          try {
            const ready = await Promise.race([sw.ready, new Promise(r => setTimeout(() => r(null), 15000))]);
            if (ready && ready.active) idle(async () => { api.adopted = await ask(ready.active, { op: "adopt", urls: loaded() }, 60000); }, 5000);
          } catch (_) {}
        }, 4000);
        if (w.document.readyState === "complete") go(); else w.addEventListener("load", go, { once: true });
      });
      return started;
    }
    async function status() {
      const want = wanted(), reg = sw ? await registration() : null, worker = reg && (reg.active || reg.waiting || reg.installing);
      const s = reg && reg.active ? await ask(reg.active, { op: "status" }, 3000) : null;
      return { on: want.on, why: want.why, registered: !!reg, controlled: !!(sw && sw.controller && SCRIPT.test(sw.controller.scriptURL)), state: worker ? worker.state : "", worker: s, error: api.error || "" };
    }
    /** Settings → Reload from cloud: the kept files go and the page is read again from the network. Other local copies may join in:
     *  window "cn:reloadfromcloud" (detail.wait(promise)) is dispatched first and waited for (at most 5 s). */
    async function reloadFromCloud(o) {
      const by = (() => { try { return typeof employeeName === "function" ? employeeName() || "" : ""; } catch (_) { return ""; } })();   // (the bridge's signed-in person)
      try { w.CNDesignLog && w.CNDesignLog.event("reloaded-from-cloud", { by }); } catch (_) {}
      const waits = [];
      try { w.dispatchEvent(new w.CustomEvent("cn:reloadfromcloud", { detail: { wait: p => waits.push(Promise.resolve(p).catch(() => {})) } })); } catch (_) {}
      const reg = sw ? await registration() : null;
      if (reg && reg.active) await ask(reg.active, { op: "clear" }, 3000);
      const caches = await dropCaches();
      await Promise.race([Promise.all(waits), new Promise(r => setTimeout(r, 5000))]);
      if (!(o && o.reload === false)) w.location.reload();
      return { caches };
    }
    const api = w.CNServiceWorker = {
      VERSION, GEN, classify, status, reloadFromCloud, start, error: "", adopted: null,
      /** this computer: the worker off (and its caches gone) until on() */
      off: () => { setPref("off"); return stop(); },
      /** this computer: the worker back on (also under automation) */
      on: () => { setPref("on"); started = null; return start(); }
    };
    const want = wanted();
    if (want.kill) stop(); else if (want.on) start();
    settingsRow(w, api);
  }
  /** Settings → "This computer's copy": what is kept, and Reload from cloud (the row is charm-nest-1.html's #stLocalCopy). */
  function settingsRow(w, api) {
    const doc = w.document, fmt = b => (b / MB).toFixed(1) + " MB";
    const paint = async host => {
      const say = host.querySelector("[data-sw-say]"); if (!say) return;
      const s = await api.status(), k = s.worker;
      say.textContent = k ? `Kept here: ${k.static.n} files of the page (${fmt(k.static.bytes)}) and ${k.pics.n} pictures (${fmt(k.pics.bytes)}) · worker ${k.version}` : s.on ? "The worker is starting: the page's files are kept from the next load." : `Not kept on this computer: ${s.why}.`;
    };
    const mount = () => {
      const host = doc.getElementById("stLocalCopy"); if (!host || host.dataset.sw) return;
      host.dataset.sw = "1";
      host.innerHTML = '<label>This computer\'s copy</label><button class="btn ghost sm" type="button" data-sw-reload title="delete the page\'s files and pictures kept on this computer and load the page again from the cloud">Reload from cloud</button><div class="help" data-sw-say role="status" aria-live="polite"></div><div class="help">The page\'s code and its sheet pictures are kept on this computer, by version, so a reload needs no download. Nothing of the orders or the sandbox is kept here.</div>';
      host.querySelector("[data-sw-reload]").addEventListener("click", () => { host.querySelector("[data-sw-say]").textContent = "Reloading from the cloud…"; api.reloadFromCloud(); });
      const dlg = doc.getElementById("dlgSettings");
      if (dlg && typeof w.MutationObserver === "function") new w.MutationObserver(() => { if (dlg.open) paint(host); }).observe(dlg, { attributes: true, attributeFilter: ["open"] });
    };
    if (doc.readyState === "loading") doc.addEventListener("DOMContentLoaded", mount, { once: true }); else mount();
  }

  const isWorker = typeof root.ServiceWorkerGlobalScope === "function" && root instanceof root.ServiceWorkerGlobalScope;
  if (isWorker) worker(root);
  else if (typeof root.document === "object" && root.document && typeof root.navigator === "object") page(root);
  if (typeof module === "object" && module.exports) module.exports = { VERSION, GEN, RETIRED, STATIC, PICS, META, CAP, classify, picPath, fileOf, evict };
})(typeof self !== "undefined" ? self : globalThis);
