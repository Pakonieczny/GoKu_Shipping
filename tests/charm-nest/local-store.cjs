/* LOCAL (Paul, 28 Oct 2026: "as responsive as Photoshop … leverage local storage … keep the API calls to the bare minimum"): the records kept on this
   computer (charm-nest-store.js) in a real headless Chromium (its IndexedDB is real), served by the fixture server. No function is called, nothing live.
     1 · kept across a reload: the Library's lists and their rows are in memory before any call (the start read, timed), a whole sheet record from disk
     2 · the worlds never mix: cn-local.production and cn-local.sandbox; a write for the other world, or while the page's guard says no, is refused
     3 · a sandbox reset: the page's own discard deletes cn-local.sandbox (production's stays); a database made before the last reset is emptied when opened
     4 · 45 days unused (Paul's decision, every kind): gone from disk and memory; the oldest-used go when space is short, designs first; a write refused
         for space is tried once more after room is made
     5 · another tab of the same world hears a write and reads it from disk
     6 · designs as read and parsed (cn-blobs): typed arrays kept as they are, shared by both worlds
     7 · no IndexedDB (missing, or a getter that throws), and in Node: every call works from memory
   node tests/charm-nest/local-store.cjs        PW_DIR=<playwright node_modules> CHROMIUM=<chrome> */
'use strict';
const path = require('path'), fs = require('fs'), assert = require('node:assert/strict');
const root = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const day = 86400000;

(async () => {
  /* ── 7 (Node) · no IndexedDB at all: memory alone ── */
  {
    delete require.cache[require.resolve(path.join(root, 'charm-nest-store.js'))];
    const St = require(path.join(root, 'charm-nest-store.js'));
    const r = await St.init({ world: 'production' });
    assert.deepEqual(r, { disk: false, rows: 0 });
    assert(St.put('sheets', 'a', { id: 'a', n: 1 }, '1.1') && St.get('sheets', 'a').n === 1 && St.rev('sheets', 'a') === '1.1');
    assert(St.putList('lib', { kind: 'sheets', ids: ['a', 'b'], since: '1.1', rev: 'r-1' }));
    assert.deepEqual(St.rows('lib').rows.map(x => x.id), ['a']); assert.equal(St.rows('lib').missing, 1);
    assert(await St.putBlob('parsed', 'k', { a: new Float32Array([1, 2]) }) === false && (await St.blob('parsed', 'k')).a[1] === 2, 'a design is kept in memory');
    assert(St.patch('sheets', 'a', { n: 2 }) && St.get('sheets', 'a').n === 2 && St.rev('sheets', 'a') === '1.1');
    assert(!St.put('sheets', 'x', { id: 'x' }, '1', { world: 'sandbox' }), 'another world is refused');
    await St.flush();
    console.log('7 · Node, no IndexedDB: put, get, lists, patch, designs from memory');
  }

  const { chromium } = require(path.join(pwDir, 'playwright-core'));
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const URL0 = srv.sorterOrigin + '/charm-nest-store.js';   // (a page of the site's own origin; the store is added to it as the sorter adds it)
  const ctx = await browser.newContext();
  async function page(opts = {}) {
    const p = await ctx.newPage();
    p.on('pageerror', e => console.log('  page error:', e.message));
    if (opts.noIdb) await p.addInitScript(how => { if (how === 'throw') Object.defineProperty(window, 'indexedDB', { get() { throw new Error('blocked'); }, configurable: true }); else Object.defineProperty(window, 'indexedDB', { value: undefined, configurable: true }); }, opts.noIdb);
    await p.goto(URL0); await p.addScriptTag({ url: '/charm-nest-store.js' });
    return p;
  }
  const boot = (p, o) => p.evaluate(async o => { if (o.guarded) o.writable = () => !window.__wiping; const t0 = performance.now(); const r = await CharmNestStore.init(o); return Object.assign({ t: performance.now() - t0 }, r); }, o);
  try {
    /* ── 1 · kept across a reload ── */
    let p = await page();
    let r = await boot(p, { world: 'production' });
    assert(r.disk && r.fresh, 'a new database: ' + JSON.stringify(r));
    await p.evaluate(() => {
      const S = CharmNestStore, ids = [];
      for (let i = 0; i < 300; i++) { const id = 'sheet-' + i; ids.push(id); S.put('sheets', id, { id, metal: i % 2 ? 'gold' : 'silver', orders: [String(4100000000 + i)], names: 'x'.repeat(3000) }, '1790000000.' + i); }
      S.putList('library:sheets', { kind: 'sheets', ids, since: '1790000000.299', rev: '1790000000.5-12' });
      S.put('sets', 'set-1', { setId: 'set-1', sheetIds: ids.slice(0, 3) }, '1790000001.1');
      S.putList('library:sets', { kind: 'sets', ids: ['set-1'], since: '1790000001.1', rev: '1790000000.5-12' });
      const charms = Array.from({ length: 120 }, (_, i) => ({ id: 'c' + i, outline: Array.from({ length: 80 }, (_, j) => [i, j]) }));
      S.put('sheetFull', 'sheet-7', { id: 'sheet-7', charms, placements: charms.map(c => ({ id: c.id, cxPt: 1, cyPt: 2 })) }, '1790000000.7');
      return S.flush();
    });
    await p.reload(); await p.addScriptTag({ url: '/charm-nest-store.js' });
    r = await boot(p, { world: 'production' });
    const back = await p.evaluate(async () => { const S = CharmNestStore, t0 = performance.now(), full = await S.load('sheetFull', 'sheet-7'), tFull = performance.now() - t0; return { lib: S.rows('library:sheets').rows.length, missing: S.rows('library:sheets').missing, list: S.list('library:sheets'), set: S.get('sets', 'set-1'), rev: S.rev('sheets', 'sheet-9'), memFull: S.get('sheetFull', 'sheet-7') ? 1 : 0, full: full && full.charms.length, tFull }; });
    assert(r.disk && r.rows === 301, 'the start read brings the lists\' rows: ' + JSON.stringify(r));
    assert(back.lib === 300 && back.missing === 0 && back.set.sheetIds.length === 3 && back.rev === '1790000000.9' && back.list.since === '1790000000.299' && back.list.rev === '1790000000.5-12');
    assert(back.full === 120 && back.memFull === 1, 'a whole sheet record read from disk, then held');
    console.log(`1 · after a reload: ${r.rows} rows of 2 lists in memory ${r.t.toFixed(1)} ms after the start (disk read ${r.ms} ms); a whole sheet record (120 charms) from disk in ${back.tFull.toFixed(1)} ms`);

    /* ── 2 · the worlds never mix ── */
    const sb = await page();
    await boot(sb, { world: 'sandbox', guarded: true });   // (the page's guard: false while a reset is under way)
    const w = await sb.evaluate(async () => {
      const S = CharmNestStore, out = {};
      out.prodSeen = !!S.get('sheets', 'sheet-1');
      out.own = S.put('sheets', 'sb-1', { id: 'sb-1' }, '1.1');
      out.other = S.put('sheets', 'sb-2', { id: 'sb-2' }, '1.1', { world: 'production' });
      window.__wiping = true; out.wiping = S.put('sheets', 'sb-3', { id: 'sb-3' }, '1.1'); out.patchWiping = S.patch('sheets', 'sb-1', { x: 1 }); window.__wiping = false;
      await S.flush();
      out.dbs = (await indexedDB.databases()).map(d => d.name).sort();
      return out;
    });
    assert.deepEqual(w, { prodSeen: false, own: true, other: false, wiping: false, patchWiping: false, dbs: ['cn-local.production', 'cn-local.sandbox'] });
    const prodHasSb = await p.evaluate(() => CharmNestStore.load('sheets', 'sb-1'));
    assert.equal(prodHasSb, null, 'production never sees a sandbox row');
    console.log('2 · two databases, one per world; a write for the other world, or during a reset, refused');

    /* ── 3 · a sandbox reset ── */
    // a database made before the last reset (another tab reset meanwhile: the epoch moved) is emptied when it is opened
    await sb.evaluate(() => localStorage.setItem('cn.resetEpoch.sandbox', 'epoch-2'));
    await sb.reload(); await sb.addScriptTag({ url: '/charm-nest-store.js' });
    r = await boot(sb, { world: 'sandbox' });
    assert(r.fresh && !(await sb.evaluate(() => CharmNestStore.load('sheets', 'sb-1'))), 'a database from before the reset was emptied');
    await sb.evaluate(() => { CharmNestStore.put('sheets', 'sb-4', { id: 'sb-4' }, '1.2'); return CharmNestStore.flush(); });
    const gone = await sb.evaluate(async () => { const ok = await CharmNestStore.discard(); return { ok, after: CharmNestStore.put('sheets', 'sb-5', { id: 'sb-5' }, '1'), dbs: (await indexedDB.databases()).map(d => d.name).sort() }; });
    assert.deepEqual(gone, { ok: true, after: false, dbs: ['cn-local.production'] }, 'the page\'s own reset deleted the sandbox\'s database alone, and nothing is written again');
    // the registry's sweep (another tab's reset) deletes it too, while a page holds it open: that page lets go
    const sb2 = await page(); await boot(sb2, { world: 'sandbox' });
    await sb2.evaluate(() => { CharmNestStore.put('sheets', 'sb-6', { id: 'sb-6' }, '1'); return CharmNestStore.flush(); });
    const sweeper = await page(); await sweeper.addScriptTag({ url: '/charm-nest-sandbox-families.js' }); await sweeper.addScriptTag({ url: '/charm-nest-sandbox-browser.js' });
    const left0 = await sweeper.evaluate(() => CharmNestSandboxBrowser.left());
    const swept = await sweeper.evaluate(() => CharmNestSandboxBrowser.sweep());
    const left1 = await sweeper.evaluate(() => CharmNestSandboxBrowser.left());
    const row = (rows, k) => rows.find(x => x.key === k).n;
    assert(row(left0, 'indexedDB:cn-local.sandbox') === 1 && row(left0, 'indexedDB:sandbox-databases') === 0 && swept.other >= 1 && row(left1, 'indexedDB:cn-local.sandbox') === 0, 'the registry counts and deletes cn-local.sandbox once: ' + JSON.stringify([left0, left1]));
    assert.deepEqual(await sweeper.evaluate(async () => (await indexedDB.databases()).map(d => d.name).sort()), ['cn-local.production']);
    assert.equal(await sb2.evaluate(async () => { CharmNestStore.put('sheets', 'sb-7', { id: 'sb-7' }, '1'); await CharmNestStore.flush(); return (await indexedDB.databases()).map(d => d.name).includes('cn-local.sandbox'); }), false, 'the page that held it open does not make it again');
    await Promise.all([sb, sb2, sweeper].map(x => x.close()));
    console.log('3 · a sandbox reset: the database deleted (the page\'s own, or the registry\'s sweep in another tab), production\'s kept; one older than the reset emptied');

    /* ── 4 · 45 days; space ── */
    const ev = await p.evaluate(async day => {
      const S = CharmNestStore;
      S.put('sheets', 'used-today', { id: 'used-today' }, '2.1'); await S.flush();
      const before = (await S.stats()).rows.sheets;
      const in44 = await S.sweep(Date.now() + 44 * day);
      const in46 = await S.sweep(Date.now() + 46 * day);
      return { before, in44: in44.removed, in46: in46.removed, after: (await S.stats()).rows, mem: S.get('sheets', 'sheet-1'), list: S.list('library:sheets') };
    }, day);
    assert(ev.before === 301 && ev.in44 === 0 && ev.in46 >= 303 && ev.after.sheets === 0 && ev.after.sheetFull === 0 && ev.mem === null && ev.list === null, 'nothing goes at 44 days, everything unused at 46: ' + JSON.stringify(ev));
    // space short: the oldest-used designs go first; a write refused for space is tried again once room is made
    const sq = await p.evaluate(async () => {
      const S = CharmNestStore, out = {};
      for (let i = 0; i < 10; i++) await S.putBlob('parsed', 'design-' + i, { bits: new Uint8Array(1000) });
      S.put('sheetFull', 'keep-me', { id: 'keep-me' }, '3.1'); await S.flush();
      const real = navigator.storage.estimate.bind(navigator.storage); let calls = 0;
      navigator.storage.estimate = async () => (++calls <= 1 ? { usage: 80, quota: 100 } : { usage: 50, quota: 100 });   // (80% at the start, 50% once a fifth of the designs went)
      out.removed = await S.squeeze(false); out.stats = await S.stats();
      navigator.storage.estimate = real;
      // one write refused for space
      const put = IDBObjectStore.prototype.put; let refused = 0;
      IDBObjectStore.prototype.put = function (v) { if (!refused && v && v.k === 'tight') { refused++; throw new DOMException('no room', 'QuotaExceededError'); } return put.apply(this, arguments); };
      S.put('sheets', 'tight', { id: 'tight' }, '4.1'); await S.flush();
      IDBObjectStore.prototype.put = put;
      out.refused = refused; out.tight = await (async () => { const db = await new Promise(r => { const q = indexedDB.open('cn-local.production'); q.onsuccess = () => r(q.result); }); const row = await new Promise(r => { const q = db.transaction('sheets').objectStore('sheets').get('tight'); q.onsuccess = () => r(q.result); }); db.close(); return !!row; })();
      return out;
    });
    assert(sq.removed === 2 && sq.stats.blobs.parsed === 8 && sq.stats.rows.sheetFull === 1, 'over 70%: the oldest-used designs went first, a fifth at a time, until 60%: ' + JSON.stringify(sq));
    assert(sq.refused === 1 && sq.tight, 'a write refused for space was written once room was made');
    console.log(`4 · unused 45 days: ${ev.in46} rows gone (none at 44); space short: ${sq.removed} oldest designs went first; a write refused for space written on the second try`);

    /* ── 5 · another tab hears it ── */
    const p2 = await page(); await boot(p2, { world: 'production' });
    await p.evaluate(() => { CharmNestStore.put('sheets', 'from-tab-1', { id: 'from-tab-1', v: 7 }, '5.1'); return CharmNestStore.flush(); });
    await p2.waitForFunction(() => CharmNestStore.get('sheets', 'from-tab-1'), null, { timeout: 3000 });
    assert.equal(await p2.evaluate(() => CharmNestStore.rev('sheets', 'from-tab-1')), '5.1');
    console.log('5 · another tab of the same world has the row a moment after it was written');

    /* ── 6 · designs ── */
    await p.evaluate(() => CharmNestStore.putBlob('parsed', 'charm-hash|rev-1', { outline: new Float64Array([1.5, 2.5]), bits: new Uint8Array([1, 0, 1]).buffer, name: 'Heart' }));
    const p3 = await page(); await boot(p3, { world: 'sandbox' });
    const bl = await p3.evaluate(async () => { const v = await CharmNestStore.blob('parsed', 'charm-hash|rev-1'); return v && { a: v.outline instanceof Float64Array && v.outline[1], b: v.bits instanceof ArrayBuffer && v.bits.byteLength, name: v.name }; });
    assert.deepEqual(bl, { a: 2.5, b: 3, name: 'Heart' }, 'a design kept by one world is read by the other, typed arrays as they were');
    console.log('6 · designs (cn-blobs): typed arrays and buffers kept as they are, shared by both worlds');

    /* ── 7 · no IndexedDB in the page ── */
    for (const how of ['missing', 'throw']) {
      const q = await page({ noIdb: how }); r = await boot(q, { world: 'production' });
      const m = await q.evaluate(async () => { const S = CharmNestStore; S.put('sheets', 'm', { id: 'm' }, '1'); S.putList('l', { kind: 'sheets', ids: ['m'] }); await S.putBlob('parsed', 'd', { x: 1 }); await S.flush(); return { row: S.get('sheets', 'm'), rows: S.rows('l').rows.length, blob: (await S.blob('parsed', 'd')).x, full: await S.load('sheetFull', 'nope'), swept: (await S.sweep()).removed }; });
      assert(!r.disk && m.row && m.rows === 1 && m.blob === 1 && m.full === null && m.swept === 0, how + ': ' + JSON.stringify([r, m]));
      await q.close();
    }
    console.log('7 · window.indexedDB missing, or throwing: every call answers from memory');
    console.log('local-store: OK');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
