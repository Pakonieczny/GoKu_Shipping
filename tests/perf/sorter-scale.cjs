// SORTER AT SCALE (Paul, 26 Oct 2026: "I don't see anything for how fast things actually happen and the performance of the overall platform once it's being used at scale with tens of thousands of
// orders in live and historical"). The real Charm Nest page (charm-nest-1.html) in headless Chromium over tests/charm-nest/bridge-server.cjs, seeded by ./scale-fixture.cjs with a live run of open
// orders and the history behind it, timed with the page's own clock (performance.now, long tasks, rAF frames) and held to budgets:
//   first usable screen < 4 s · a tab switch or opening a window < 300 ms · a keystroke in a search < 100 ms · no frame over 50 ms while a list scrolls · the heap after GC does not grow with a repeated action.
// Requests and bytes each action costs are printed beside the times. No network but the loopback; no Etsy, no AI, no Firestore.
//   node tests/perf/sorter-scale.cjs              (default: 800 open orders, 6,000 historical, under 5 minutes)
//   SCALE=full node tests/perf/sorter-scale.cjs    (3,000 open orders, 30,000 historical: the production numbers)
//   PW_DIR=<playwright node_modules> CHROMIUM=<chrome>
'use strict';
const path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('../charm-nest/bridge-server.cjs');
const F = require('./scale-fixture.cjs');
const FULL = process.env.SCALE === 'full', OPEN = FULL ? 3000 : 800, HIST = FULL ? 30000 : 6000, INTAKE = 500, REPEAT = FULL ? 30 : 12;
const BUDGET = { first: 4000, tab: 300, win: 300, key: 100, frame: 50, intake: 3000, heapGrowthMB: 8 };

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  const hist = F.seedHistory(srv.st, { orders: HIST, current: FULL ? 200 : 80 });
  console.log(`  fixture: ${OPEN} open orders · ${HIST} historical orders in ${hist.sets} sets / ${hist.sheets} sheets · ${hist.cancelled} cancelled`);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome', args: ['--no-sandbox', '--enable-precise-memory-info', '--js-flags=--expose-gc'] });
  const rows = [], fails = [], errors = [];
  const rec = (name, value, budget, unit = 'ms', extra = '') => { const ok = budget == null || value <= budget; rows.push({ name, value, budget, unit, extra, ok }); if (!ok) fails.push(`${name}: ${Math.round(value)} ${unit} > ${budget} ${unit}`); console.log(`  ${ok ? '✓' : '✗'} ${name.padEnd(46)} ${String(Math.round(value)).padStart(7)} ${unit}${budget != null ? '  (budget ' + budget + ')' : ''}${extra ? '  · ' + extra : ''}`); };
  let bytes = 0, reqs = 0;
  const cost = () => { const r = reqs, b = bytes; return () => `${reqs - r} req, ${Math.round((bytes - b) / 1024)} KB`; };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(s => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator';
      // (the page's own long tasks, from the first script on)
      window.__long = []; try { new PerformanceObserver(l => { for (const e of l.getEntries()) window.__long.push(e.duration); }).observe({ type: 'longtask', buffered: true }); } catch (_) {} },
      { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' });
    const page = await context.newPage();
    page.setDefaultTimeout(60000);
    page.on('pageerror', e => errors.push(e.message));
    page.on('requestfinished', async q => { reqs++; try { const s = await q.sizes(); bytes += s.responseBodySize + s.responseHeadersSize; } catch (_) {} });
    const cdp = await context.newCDPSession(page);
    const heap = async () => { await cdp.send('HeapProfiler.collectGarbage'); await cdp.send('HeapProfiler.collectGarbage'); return (await cdp.send('Runtime.getHeapUsage')).usedSize / 1048576; };
    // the time from an action to the frame that shows it: the action's own work plus the style, layout and paint it causes
    // PROFILE=1: the CPU profile of every measured action, its heaviest functions by self time (to find what to fix)
    const prof = async (name, run) => { if (!process.env.PROFILE) return run(); await cdp.send('Profiler.enable'); await cdp.send('Profiler.start'); const v = await run(); const { profile } = await cdp.send('Profiler.stop');
      const self = new Map(), dt = profile.timeDeltas, byId = new Map(profile.nodes.map(n => [n.id, n])); profile.samples.forEach((id, i) => { const n = byId.get(id), f = n.callFrame, k = (f.functionName || '(anon)') + ' ' + f.url.split('/').pop() + ':' + (f.lineNumber + 1); self.set(k, (self.get(k) || 0) + (dt[i] || 0) / 1000); });
      console.log('    [' + name + '] ' + [...self].sort((a, b) => b[1] - a[1]).slice(0, 8).map(([k, t]) => k + ' ' + t.toFixed(0)).join(' | ')); return v; };
    const timed = (fn, arg) => page.evaluate(async ({ src, arg }) => { const f = eval(src); const t0 = performance.now(); await f(arg); await new Promise(r => requestAnimationFrame(() => setTimeout(r, 0))); return performance.now() - t0; }, { src: String(fn), arg });

    // ── 1 · first usable screen: navigation start to the page's cloud answered and the Nest tab drawn
    let c = cost();
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true && document.querySelector('#modeSeg button[data-mode="orders"]'));
    rec('first usable screen', await page.evaluate(() => performance.now()), BUDGET.first, 'ms', c());

    // ── 2 · the live run: OPEN open orders on the list (what a pull hands Orders), read once
    c = cost();
    const toRows = `(orders => orders.flatMap(o => o.lines.map(l => ({ key: CharmNestOrders.lineKey(o, l), order: o, line: l, arrivedAt: o.createTs * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }))))`;
    const seedT = await page.evaluate(async ({ orders, toRows }) => { const t0 = performance.now(); B.orders.rows = eval(toRows)(orders); B.orders.byKey = new Map(B.orders.rows.map(r => [r.key, r])); await Orders.interpretAll(); return performance.now() - t0; }, { orders: F.openOrders(OPEN), toRows });
    const lines = await page.evaluate(() => Orders.rows().length);
    rec(`read ${OPEN} open orders (${lines} lines)`, seedT, null, 'ms', c());

    // ── 3 · each top tab, from the Nest tab and back (the first visit draws it; the second is what a person sees all day)
    const tab = m => prof('tab ' + m, () => timed(async m => { document.querySelector(`#modeSeg button[data-mode="${m}"]`).click(); }, m));
    for (const m of ['orders', 'engrave', 'review', 'library', 'nest']) { c = cost(); const t1 = await tab(m); await page.waitForTimeout(300); rec(`tab → ${m} (first)`, t1, null, 'ms', c()); }
    for (const m of ['orders', 'engrave', 'review', 'library', 'nest']) { c = cost(); const t2 = await tab(m); await page.waitForTimeout(150); rec(`tab → ${m}`, t2, BUDGET.tab, 'ms', c()); }

    // ── 4 · the Orders list: drawn, scrolled (with Show more) without a long frame, searched a key at a time
    await tab('orders'); await page.waitForSelector('#ordBody [data-key]');
    rec('Orders rows drawn (paged)', await page.evaluate(() => document.querySelectorAll('#ordBody [data-key]').length), null, 'rows');
    const scroll = sel => page.evaluate(async sel => {
      const host = document.querySelector(sel), frames = []; let last = performance.now(), on = true;
      const tick = t => { frames.push(t - last); last = t; if (on) requestAnimationFrame(tick); }; requestAnimationFrame(tick);
      for (let i = 0; i < 40; i++) { host.scrollTop += 400; const more = host.querySelector('.libMore button, [data-more]'); if (more && i % 8 === 7) more.click(); await new Promise(r => setTimeout(r, 30)); }
      on = false; await new Promise(r => requestAnimationFrame(r)); frames.shift(); return Math.max(...frames);
    }, sel);
    c = cost(); rec('Orders scroll: longest frame', await prof('scroll orders', () => scroll('#ordBody')), BUDGET.frame, 'ms', c());
    const typing = (sel, text) => page.evaluate(async ({ sel, text }) => {
      const box = document.querySelector(sel); box.focus(); box.value = ''; const per = [];
      for (const ch of text) { const t0 = performance.now(); box.value += ch; box.dispatchEvent(new InputEvent('input', { bubbles: true, data: ch })); box.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: ch })); await new Promise(r => requestAnimationFrame(() => setTimeout(r, 0))); per.push(performance.now() - t0); }
      const t1 = performance.now(); await new Promise(r => setTimeout(r, 600)); return { worst: Math.max(...per), settle: performance.now() - t1 };
    }, { sel, text });
    c = cost(); let ty = await prof('type orders', () => typing('#ordQ', '4300000123')); rec('Orders search: worst keystroke', ty.worst, BUDGET.key, 'ms', c());
    const found = await page.evaluate(() => [...document.querySelectorAll('#ordBody [data-key]')].filter(n => n.offsetParent).length);
    rec('Orders search: rows for one order number', found, null, 'rows');
    await page.evaluate(() => { const b = document.querySelector('#ordQ'); b.value = ''; b.dispatchEvent(new InputEvent('input', { bubbles: true })); }); await page.waitForTimeout(700);

    // ── 5 · the order window: a new order and an old one (the oldest on the list), opened and closed
    const keys = await page.evaluate(() => { const r = Orders.rows().slice().sort((a, b) => a.order.createTs - b.order.createTs); return [r[r.length - 1].key, r[0].key]; });
    const winOpen = k => prof('window', () => timed(async k => { OrderWin.open(k); }, k));
    const winClose = () => page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.close(); });
    for (const [label, k] of [['new', keys[0]], ['old', keys[1]]]) { c = cost(); const t = await winOpen(k); await page.waitForTimeout(400); rec(`order window (${label} order) open`, t, BUDGET.win, 'ms', c()); await winClose(); await page.waitForTimeout(300); }

    // ── 6 · the Library: first view of the sets, scroll, search, a set opened
    c = cost(); const libT = await tab('library'); await page.waitForFunction(() => document.querySelector('#libBody .setCard, #libBody .libEmpty'), null, { timeout: 60000 });
    const libFull = await page.evaluate(() => performance.now());
    rec('Library tab', libT, BUDGET.tab, 'ms', c());
    rec('Library set cards', await page.evaluate(() => document.querySelectorAll('#libBody .setCard').length), null, 'cards', `${libFull | 0}`);
    c = cost(); rec('Library scroll: longest frame', await prof('scroll library', () => scroll('#libBody')), BUDGET.frame, 'ms', c());
    c = cost(); ty = await typing('#libSearch', '4100000'); rec('Library search: worst keystroke', ty.worst, BUDGET.key, 'ms', c());
    await page.evaluate(() => { const b = document.querySelector('#libSearch'); b.value = ''; b.dispatchEvent(new InputEvent('input', { bubbles: true })); }); await page.waitForTimeout(500);
    const setOpen = await timed(async () => { const card = document.querySelector('#libBody .setCard'); (card.querySelector('.setHead, header, .setTitle') || card).click(); });
    rec('Library: open a set', setOpen, BUDGET.win, 'ms');
    await page.keyboard.press('Escape'); await page.waitForTimeout(300);

    // ── 7 · the intake of a pull of 500 new orders onto the live list
    c = cost();
    const intake = await page.evaluate(async ({ orders, toRows }) => { const t0 = performance.now(); const add = eval(toRows)(orders); for (const r of add) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); } await Orders.interpretAll(); await new Promise(r => requestAnimationFrame(() => setTimeout(r, 0))); return performance.now() - t0; }, { orders: F.openOrders(INTAKE, 99).map((o, i) => Object.assign(o, { receiptId: String(4400000000 + i), orderNumber: String(4400000000 + i) })), toRows });
    rec(`intake of a pull of ${INTAKE} orders`, intake, BUDGET.intake, 'ms', c());

    // ── 8 · memory: an hour-like session of the same actions; the heap after GC must not climb with them
    const h0 = await heap();
    const round = async () => { for (const m of ['orders', 'review', 'library', 'engrave', 'nest']) await tab(m); await tab('orders'); await winOpen(keys[0]); await winClose(); await winOpen(keys[1]); await winClose(); };
    for (let i = 0; i < 3; i++) await round();             // (warm: caches and first drawings)
    const h1 = await heap();
    for (let i = 0; i < REPEAT; i++) await round();
    const h2 = await heap();
    rec('heap after GC, start', h0, null, 'MB');
    rec(`heap growth over ${REPEAT} rounds (after warm-up)`, h2 - h1, BUDGET.heapGrowthMB, 'MB', `${h1.toFixed(1)} → ${h2.toFixed(1)} MB`);
    const longest = await page.evaluate(() => Math.max(0, ...window.__long));
    rec('longest task over the session', longest, null, 'ms');
    rec('requests over the session', reqs, null, 'req', `${Math.round(bytes / 1048576)} MB`);
  } finally { await browser.close(); srv.close(); }
  if (errors.length) { console.log('  page errors:', errors.slice(0, 5)); fails.push(errors.length + ' page error(s)'); }
  if (fails.length) { console.log('\n  OVER BUDGET:\n   · ' + fails.join('\n   · ')); process.exitCode = 1; } else console.log('\n  every budget held');
}
main().catch(e => { console.error(e); process.exitCode = 1; });
