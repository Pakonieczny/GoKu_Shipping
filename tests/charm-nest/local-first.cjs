/* LOCAL GATES (Paul, 28 Oct 2026: "as responsive as Photoshop … leverage local storage … keep the API calls to the bare minimum"; the local-first plan,
   phase 1). The real sorter page (charm-nest-1.html) in headless Chromium over the fixture server (bridge-server.cjs: the real charmNest* handlers on an
   in-memory Firestore), the Library at scale (tests/perf/scale-fixture.cjs: 6,000 historical orders, 80 sets in progress) and one sheet of real master
   drawings (COMET_9427 and SCHRODINGER CAT 1, tests/charm-nest/fixtures). Nothing live, no credential. Each gate prints its number:
     G1 · a sheet seen before, opened again after a reload with every function call held 2 s (context.route): its whole plate and its orders drawn
          in under 100 ms, no veil (the median of three reloads, each the sheet's first opening in its page, once the page is at rest)
     G2 · the reopen's getSheet {ifRev} answers unchanged in under 1 KB, at 1 Firestore read
     G3 · a warm reload paints the Library's cards within 500 ms of the page's scripts having run (calls held 2 s: only this computer's copy can)
     G4 · the Library visited again with nothing changed: under 20 KB
     G5 · after the reload, 0 master downloads and 0 parses for the designs the sheet uses
     G6 · window.indexedDB removed: the Library and the sheet window work as before, from the cloud, with no page error
   node tests/charm-nest/local-first.cjs        PW_DIR=<playwright node_modules> CHROMIUM=<chrome>   NOGATE=1: report only */
'use strict';
const path = require('path'), fs = require('fs'), os = require('os'), assert = require('node:assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const F = require('../perf/scale-fixture.cjs');
const { seedMaster } = require('./_engraveRealWorld.cjs');
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
const GATE = { reopenMs: 100, unchangedBytes: 1024, libraryMs: 500, revisitKB: 20, downloads: 0, parses: 0 };
const NOGATE = !!process.env.NOGATE;
const SHEET = 'gate-sheet', SET = 'set-gate-1', HOLD = 2000;
const fails = [], out = [];
const gate = (name, ok, text) => { out.push(`  ${ok ? '✓' : '✗'} ${name.padEnd(4)} ${text}`); if (!ok) fails.push(name + ': ' + text); };

/** One sheet of 24 pieces of two real master designs, in a set in progress (so the Library's Sets view shows it). */
function seedGateSheet(st) {
  const designs = [['COMET_9427', 'COMET_9427.ai'], ['SCHRODINGER CAT 1', 'SCHRODINGER_CAT_1.ai']].map(([sku, file]) => {
    const e = st.doc('Charm_Master_Index', sku), path = 'charmnest/master/' + file;
    return { sku, path, hash: e.charmHash, w: e.widthPt, h: e.heightPt, area: e.areaPt2, id: 'pool:' + path.replace(/[^\w]+/g, '_') };
  });
  const charms = [], placements = [], orders = [];
  for (let i = 0; i < 24; i++) {
    const d = designs[i % 2], rid = String(4199000000 + i), id = 'gc' + i;
    orders.push(rid);
    charms.push({ id, hash: d.hash, name: d.sku, sourceId: d.id, sourceName: d.sku + ' (master)', index: 0, qty: 1, order: rid, poolId: `${rid}_${rid}1_1`, custom: false, sku: d.sku, areaPt2: d.area, widthPt: d.w, heightPt: d.h });
    placements.push({ id, cxPt: 24 + (i % 8) * 34, cyPt: 26 + Math.floor(i / 8) * 46, angle: 0, scale: 1 });
  }
  const day = new Date().toISOString().slice(0, 10), now = Date.now();
  st.put('Charm_Nest_Sheets', SHEET, { id: SHEET, sheetId: SHEET, metal: 'gold', metalLabel: 'GF 14/20', day, setId: SET, setSeq: 41, sheetIndex: 1, runId: 'run-gate', status: 'complete', placedCount: 24, charmCount: 24, density: 0.62, freePt2: 900,
    verification: { ok: true }, stock: { wPt: 288, hPt: 144 }, folder: 'GF_Gate_Set-41_Sheet-1', fileBase: 'GF_Gate_Set-41_Sheet-1', orders, poolIds: charms.map(c => c.poolId), charms, placements,
    sources: designs.map(d => ({ id: d.id, name: d.sku + ' (master)', hash: d.hash, bytes: 0, path: d.path, url: null, pool: true, custom: false, sku: d.sku })), updatedAt: now, createdAt: now, current: true });
  st.put('Charm_Nest_Sets', SET, { setId: SET, seq: 41, day, runId: 'run-gate', sheetIds: [SHEET], materials: ['gold'], status: 'labelled', committedAt: now, updatedAt: now, current: true });
}

/* in the page from its first script: every call to the functions (op, bytes), a parse asked of the worker, a master file read */
function probes() {
  window.__net = []; window.__parses = 0; window.__masters = 0;
  const f = window.fetch;
  window.fetch = function (u, o) {
    const url = String(u && u.url || u), fn = /\/\.netlify\/functions\//.test(url) ? url.replace(/^.*\/functions\//, '').replace(/\?.*$/, '') : '';
    const body = o && typeof o.body === 'string' ? o.body : '', rec = fn ? { t: performance.now(), fn, op: (/"op":"(\w+)"/.exec(body) || [])[1] || '', body: body.slice(0, 300) } : null;
    if (rec) window.__net.push(rec);
    let plain = url; try { plain = decodeURIComponent(decodeURIComponent(url)); } catch (_) {}
    if (/charmnest\/master\/[^?#]*\.ai\b/i.test(plain)) window.__masters++;   // (a master design's file, however it is asked for: Storage, the asset door)
    const p = f.apply(this, arguments);
    if (rec) p.then(r => r.clone().arrayBuffer().then(b => { rec.bytes = b.byteLength; rec.done = performance.now(); }, () => {}), () => {});
    return p;
  };
  const post = Worker.prototype.postMessage;
  Worker.prototype.postMessage = function (m) { if (m && m.type === 'parse') window.__parses++; return post.apply(this, arguments); };
  // the first Library card on screen (a set card or a sheet card), timed at the frame that shows it
  window.__cardAt = 0;
  const seen = () => { if (window.__cardAt) return; const c = document.querySelector('#libBody .setCard, #libBody .libCard'); if (c && c.getClientRects().length) requestAnimationFrame(() => { if (!window.__cardAt) window.__cardAt = performance.now(); }); };
  new MutationObserver(seen).observe(document, { childList: true, subtree: true });
  document.addEventListener('DOMContentLoaded', () => { window.__scriptsAt = performance.now(); });
}

async function main() {
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser gates were not run'); return; }
  const srv = await start({ receipts: [] }), { st } = srv;
  const counts = F.seedHistory(st, { orders: 6000, current: 80 }); seedMaster(st, ['COMET_9427', 'SCHRODINGER CAT 1']); seedGateSheet(st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox'] });
  const world = async (o = {}) => {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(s => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; },
      { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' });
    if (o.noIdb) await context.addInitScript(() => { try { Object.defineProperty(window, 'indexedDB', { value: undefined, configurable: true }); } catch (_) {} });
    await context.addInitScript(probes);
    const page = await context.newPage(); page.setDefaultTimeout(60000);
    const errors = []; page.on('pageerror', e => errors.push(e.message));
    return { context, page, errors };
  };
  const ready = page => page.waitForFunction(() => window.CN && window.Sets && window.SheetWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
  // the sheet window holds the sheet whole: its record, every piece drawn from its design, its orders listed, no veil
  const drawn = id => { const W = SheetWin._W; return !!(W.dlg && W.dlg.open && W.id === id && W.rec && W.geom && W.el.veil.hidden && W.pieces.length === 24 && W.pieces.every(x => x.c) && W.el.orders.querySelector('li:not(.swNone)')); };
  try {
    console.log(`  fixture: ${counts.sets} sets, ${counts.sheets} sheets (80 sets in progress) · a sheet of 24 pieces from 2 master drawings · load ${os.loadavg().map(x => x.toFixed(2)).join(' ')}`);
    /* ── the first visit: the Library read, the sheet opened and drawn (what this computer then keeps) ── */
    const a = await world();
    await a.page.goto(`${srv.sorterOrigin}/charm-nest-1.html#library`, { waitUntil: 'load' });
    await ready(a.page);
    await a.page.waitForFunction(() => document.querySelector('#libBody .setCard'), null, { timeout: 60000 });
    const firstLib = await a.page.evaluate(() => window.__net.filter(x => /^(setList|listSheets)$/.test(x.op)).reduce((n, x) => n + (x.bytes || 0), 0));
    await a.page.evaluate(id => SheetWin.open(id), SHEET);
    await a.page.waitForFunction(drawn, SHEET, { timeout: 60000 });
    const firstOpen = await a.page.evaluate(() => ({ parses: window.__parses, masters: window.__masters, ops: window.__net.map(x => x.op || x.fn) }));
    if (process.env.DEBUG) console.log('  first visit calls:', firstOpen.ops.join(' '));
    await a.page.evaluate(() => SheetWin.close()); await a.page.waitForTimeout(1500);   // (what was kept reaches the disk)
    console.log(`  first visit: Library lists ${Math.round(firstLib / 1024)} KB; the sheet drawn with ${firstOpen.masters} master download(s) and ${firstOpen.parses} parse(s)`);

    /* ── the reload, every call held 2 s ── */
    await a.context.route(/\/\.netlify\/functions\//, async r => { await new Promise(res => setTimeout(res, HOLD)); try { await r.continue(); } catch (_) {} });
    // (G1 is taken after three reloads, its median: the machine is shared, and one number is the noise of whoever else runs; each reopen is the first opening of the sheet in its page)
    // (the main thread's own work beside each wall time, from Chrome's counters: about twenty agents share this machine, and a wall time alone mixes the page's work with their waiting)
    const cdp = await a.context.newCDPSession(a.page); await cdp.send('Performance.enable');
    const counters = async () => Object.fromEntries((await cdp.send('Performance.getMetrics')).metrics.map(x => [x.name, x.value]));
    const work = (m0, m1) => { const d = k => Math.round(Math.max(0, (m1[k] || 0) - ((m1[k] || 0) >= (m0[k] || 0) ? (m0[k] || 0) : 0)) * 1000); return { task: d('TaskDuration'), script: d('ScriptDuration'), style: d('RecalcStyleDuration'), layout: d('LayoutDuration') }; };
    const told = w => `main thread ${w.task} ms: script ${w.script}, style ${w.style}, layout ${w.layout}`;
    const reopens = [], reopenWork = [];
    for (let k = 0; k < 3; k++) {
      const r0 = await counters();
      await a.page.reload({ waitUntil: 'domcontentloaded' });
      await a.page.waitForFunction(() => window.__cardAt > 0 && window.SheetWin && window.CharmNestStore, null, { timeout: 30000 });
      const loadWork = work(r0, await counters());
      if (process.env.DEBUG) console.log('  reload', k, JSON.stringify(await a.page.evaluate(async () => ({ origin: Math.round(performance.timeOrigin), ready: await CharmNestStore.ready(), card: Math.round(window.__cardAt) }))));
      if (!k) {
        // (the scripts' start: the first of the page's scripts arrived; their end: DOMContentLoaded, every script run)
        const lib = await a.page.evaluate(() => { const first = performance.getEntriesByType('resource').find(e => /charm-nest-clock\.js/.test(e.name)); return { fromStart: window.__cardAt - (first ? first.responseEnd : 0), afterAll: window.__cardAt - (window.__scriptsAt || Infinity), cards: document.querySelectorAll('#libBody .setCard').length, calls: window.__net.filter(x => x.done && x.done < window.__cardAt).length }; });
        // (measured from the moment every script of the page has run, DOMContentLoaded: nothing can draw a card before the Library's own code is on the page; from the first script's arrival is printed beside it)
        gate('G3', lib.afterAll < GATE.libraryMs && lib.calls === 0, `warm reload: Library cards painted ${Math.round(lib.afterAll)} ms after the page's scripts ran (${Math.round(lib.fromStart)} ms after the first of them arrived; ${lib.cards} set cards, ${lib.calls} calls answered before; gate < ${GATE.libraryMs} ms); ${told(loadWork)} from the reload until the cards were seen`);
      }
      await a.page.waitForTimeout(HOLD + 2500);   // (the reload's own reads, held, come back and are drawn: the reopen is a person's click on a page at rest; the calls stay held)
      const o0 = await counters();
      const g1 = await a.page.evaluate(async ({ id, src }) => {
        const drawn = eval('(' + src + ')'), t0 = performance.now(); SheetWin.open(id);
        for (;;) { await new Promise(r => requestAnimationFrame(r)); if (drawn(id)) return { ms: performance.now() - t0 }; if (performance.now() - t0 > 15000) return { ms: Infinity }; }
      }, { id: SHEET, src: String(drawn) });
      reopenWork.push(work(o0, await counters()));
      reopens.push(Math.round(g1.ms));
      if (k < 2) { await a.page.evaluate(() => SheetWin.close()); await a.page.waitForTimeout(600); }
    }
    const g1ms = reopens.slice().sort((x, y) => x - y)[1], g1w = reopenWork[reopens.indexOf(g1ms)];
    gate('G1', g1ms < GATE.reopenMs, `a sheet seen before, reopened after a reload (calls held ${HOLD / 1000} s): whole plate and orders in ${g1ms} ms (median of ${reopens.join(', ')}; load ${os.loadavg()[0].toFixed(2)}), no veil (gate < ${GATE.reopenMs} ms); ${told(g1w)} (each: ${reopenWork.map(w => w.task).join(', ')} ms)`);
    await a.page.waitForTimeout(HOLD + 1500);   // (the reopen's question comes back)
    const after = await a.page.evaluate(() => ({ parses: window.__parses, masters: window.__masters, get: window.__net.filter(x => x.op === 'getSheet').map(x => ({ bytes: x.bytes, body: x.body })) }));
    gate('G5', after.masters === GATE.downloads && after.parses === GATE.parses, `after the reload: ${after.masters} master download(s), ${after.parses} parse(s) for the designs used (gate 0 and 0)`);
    const ask = after.get.find(x => /"ifRev"/.test(x.body));
    st.reads = 0; const probe = await fetch(srv.sorterOrigin + '/.netlify/functions/charmNestLibrary', { method: 'POST', headers: { 'content-type': 'application/json' }, body: ask ? ask.body.replace(/^.*?(\{"op":"getSheet".*\})$/, '$1') : '{}' }).then(r => r.text());
    gate('G2', !!ask && ask.bytes < GATE.unchangedBytes && /"unchanged":true/.test(probe) && st.reads === 1, ask ? `the reopen asked getSheet {ifRev}: ${ask.bytes} B, ${st.reads} Firestore read (gate < 1 KB, 1 read)` : 'the reopen never asked getSheet with ifRev');
    await a.page.evaluate(() => SheetWin.close());

    /* ── the Library visited again, nothing changed ── */
    await a.context.unroute(/\/\.netlify\/functions\//);
    await a.page.waitForTimeout(HOLD + 500);
    await a.page.evaluate(() => CN.setMode('orders')); await a.page.waitForTimeout(800);
    const n0 = await a.page.evaluate(() => window.__net.length);
    await a.page.evaluate(() => CN.setMode('library')); await a.page.waitForTimeout(2500);
    const rv = await a.page.evaluate(n0 => { const l = window.__net.slice(n0); return { kb: l.reduce((n, x) => n + (x.bytes || 0), 0) / 1024, ops: l.map(x => x.op || x.fn) }; }, n0);
    gate('G4', rv.kb < GATE.revisitKB, `the Library visited again, nothing changed: ${rv.kb.toFixed(1)} KB in ${rv.ops.length} call(s) [${rv.ops.join(' ')}] (gate < ${GATE.revisitKB} KB)`);
    if (a.errors.length) fails.push('page errors: ' + a.errors.slice(0, 3).join(' | '));
    await a.context.close();

    /* ── no IndexedDB ── */
    const b = await world({ noIdb: true });
    await b.page.goto(`${srv.sorterOrigin}/charm-nest-1.html#library`, { waitUntil: 'load' });
    await ready(b.page);
    await b.page.waitForFunction(() => document.querySelector('#libBody .setCard'), null, { timeout: 60000 });
    await b.page.evaluate(id => SheetWin.open(id), SHEET);
    const okNo = await b.page.waitForFunction(drawn, SHEET, { timeout: 60000 }).then(() => true, () => false);
    const noIdb = await b.page.evaluate(() => ({ idb: typeof window.indexedDB, disk: window.CharmNestStore ? CharmNestStore.disk() : null }));
    gate('G6', okNo && !b.errors.length && noIdb.idb === 'undefined' && noIdb.disk === false, `window.indexedDB removed: Library shown, sheet drawn ${okNo ? 'yes' : 'NO'}, page errors ${b.errors.length}${b.errors.length ? ' (' + b.errors.slice(0, 2).join(' | ') + ')' : ''}`);
    await b.context.close();
  } finally { await browser.close(); srv.close(); }
  console.log(out.join('\n'));
  if (fails.length && !NOGATE) { console.error('local-first gates FAILED:\n  ' + fails.join('\n  ')); process.exit(1); }
  console.log(fails.length ? 'local-first: report only (NOGATE)' : 'local-first: every gate met');
}
main().catch(e => { console.error(e); process.exit(1); });
