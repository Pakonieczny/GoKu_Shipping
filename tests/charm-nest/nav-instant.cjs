// NAV-INSTANT (Paul, 28 Oct 2026: "Every sheet that opens must be instantaneous the users needs the ability to move between Tabs, Lists and Modals, Nesting sheets, Order Details, Engraving etc...
// at near real-time speed. This must work like a native application … I absolutely wanna keep the API calls to the bare minimum"). The real sorter page (charm-nest-1.html) in headless Chromium over
// bridge-server.cjs, seeded by tests/perf/scale-fixture.cjs (800 open orders, 6,000 historical in sets of sheets), each navigation a person makes all day timed from the input to the frame that shows
// it (p50 / p95 of several), with the long tasks (> 50 ms) it caused and the calls it made to /.netlify/functions/* (the page's fetch is wrapped: op name and bytes). The Engraving steps run in the
// real-drawing world (_engraveRealWorld.cjs: real charms, real fit). Budgets (on a quiet machine, with margin): a tab switch p95 < 100 ms and NO cloud call when its data is fresh; the order window
// opened from memory p50 < 150 ms; its tabs, Back / Next < 100 ms; Engraving Next / Back p95 < 100 ms. The load average is printed with the numbers (other agents share the machine).
//   node tests/charm-nest/nav-instant.cjs                 NOBUDGET=1: report only · JSON=<file>: the rows as JSON · PW_DIR=<playwright node_modules> CHROMIUM=<chrome>
'use strict';
const path = require('path'), fs = require('fs'), os = require('os');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const F = require('../perf/scale-fixture.cjs');
const OPEN = 800, HIST = 6000;
// Paul's targets (TARGET) are printed beside each row; the budgets that fail the run (BUDGET) are what this branch holds today with margin on a loaded machine (load ~3): a regression guard
// until the order window reaches the targets (28 Oct 2026, round 2, load ~2.5: tab → orders p95 ~120 ms, review ~100 ms, order window open p50 ~190 ms, Next/Back p95 ~200 ms, Engraving Next p95 ~110 ms)
const TARGET = { tabP95: 100, tabCalls: 0, winOpenP50: 150, winTabP95: 100, winStepP95: 150, engStepP95: 100, sheetWinP95: 150 };
const BUDGET = { tabP95: 300, tabCalls: 3, libCalls: 3, libKB: 100, winOpenP50: 400, winTabP95: 400, winStepP95: 500, engStepP95: 250, sheetWinP95: 400 };
const NOBUDGET = !!process.env.NOBUDGET;
const CHROME = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1243/chrome-linux64/chrome';
const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node-tools/node_modules', '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));

const q = (a, p) => { if (!a.length) return 0; const s = a.slice().sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.max(0, Math.ceil(p * s.length) - 1))]; };
const rows = [], fails = [];
const TARGET_OF = { 'tab → orders': `p95 < ${TARGET.tabP95}, 0 calls`, 'tab → review': `p95 < ${TARGET.tabP95}, 0 calls`, 'tab → library': `p95 < ${TARGET.tabP95}`, 'order window open': `p50 < ${TARGET.winOpenP50}`, 'order window Next': `p95 < ${TARGET.winStepP95}`, 'order window Back': `p95 < ${TARGET.winStepP95}`, 'Engraving Next': `p95 < ${TARGET.engStepP95}`, 'Engraving Back': `p95 < ${TARGET.engStepP95}` };
function rec(name, times, extra, budget) {
  const p50 = q(times, .5), p95 = q(times, .95), row = Object.assign({ name, n: times.length, p50: Math.round(p50), p95: Math.round(p95), load: os.loadavg()[0].toFixed(2) }, extra);
  rows.push(row);
  const over = budget && !NOBUDGET ? Object.entries(budget).filter(([k, v]) => row[k] > v).map(([k, v]) => `${name}: ${k} ${row[k]} > ${v}`) : [];
  fails.push(...over);
  console.log(`  ${over.length ? '✗' : '✓'} ${name.padEnd(44)} p50 ${String(row.p50).padStart(5)} ms  p95 ${String(row.p95).padStart(5)} ms${row.work50 != null ? `  work ${row.work50}/${row.work95}` : ''}  long ${String(row.long).padStart(2)} (${row.longMs} ms)  calls ${String(row.calls).padStart(3)} ${row.kb} KB${row.ops ? '  [' + row.ops + ']' : ''}${budget ? '  budget ' + JSON.stringify(budget) : ''}${TARGET_OF[name.replace(/ \(.*$/, '')] ? '  target ' + TARGET_OF[name.replace(/ \(.*$/, '')] : ''}  · load ${row.load}`);
  return row;
}

// in the page from its first script: the long tasks (start, duration) and every call to the functions (start, op, bytes)
function probes() {
  window.__long = []; try { new PerformanceObserver(l => { for (const e of l.getEntries()) window.__long.push([e.startTime, e.duration]); }).observe({ type: 'longtask', buffered: true }); } catch (_) {}
  window.__net = []; const f = window.fetch;
  window.fetch = function (u, o) {
    const url = String(u && u.url || u), rec = /\/\.netlify\/functions\//.test(url) ? { t: performance.now(), fn: url.replace(/^.*\/functions\//, '').replace(/\?.*$/, ''), op: (o && typeof o.body === 'string' && (/"op":"(\w+)"/.exec(o.body) || [])[1]) || '', bytes: 0 } : null;
    if (rec) window.__net.push(rec);
    const p = f.apply(this, arguments);
    if (rec) p.then(r => r.clone().arrayBuffer().then(b => { rec.bytes = b.byteLength; }, () => {}), () => {});
    return p;
  };
}

async function world(opts) {
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { return null; }
  const srv = await start({ receipts: [] });
  F.seedHistory(srv.st, { orders: HIST, current: 80 });
  const browser = await chromium.launch({ executablePath: CHROME, args: ['--no-sandbox'] });
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await context.addInitScript(s => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; },
    { v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' });
  await context.addInitScript(probes);
  const page = await context.newPage(); page.setDefaultTimeout(60000);
  const errors = []; page.on('pageerror', e => errors.push(e.message));
  return { srv, browser, page, errors, close: async () => { await browser.close(); srv.close(); } };
}

/** One measured action, n times: the time from the input to the next frame, then a quiet wait (settle ms) in which its background reads and long tasks are still counted to it. */
async function measure(page, name, n, act, o = {}) {
  const times = [], work = [], ops = new Map(); let long = 0, longMs = 0, calls = 0, bytes = 0;
  for (let i = 0; i < n; i++) {
    if (o.before) await o.before(i);
    const from = await page.evaluate(() => [performance.now(), window.__net.length]);
    // (and the main thread's own work for it, the press's handler until it returns: a figure the machine's load moves far less than the time to the frame)
    const [t, wk] = await page.evaluate(async ({ src, i }) => { const f = eval("(" + src + ")"); const t0 = performance.now(); const p = f(i); const t1 = performance.now(); await p; await new Promise(r => requestAnimationFrame(() => setTimeout(r, 0))); return [performance.now() - t0, t1 - t0]; }, { src: String(act), i });
    times.push(t); work.push(wk);
    await page.waitForTimeout(o.settle == null ? 400 : o.settle);
    const got = await page.evaluate(([t0, n0]) => ({ net: window.__net.slice(n0).map(x => [x.op || x.fn, x.bytes]), long: window.__long.filter(([s]) => s >= t0).map(([, d]) => d) }), from);
    for (const [op, b] of got.net) { calls++; bytes += b; ops.set(op, (ops.get(op) || 0) + 1); }
    long += got.long.length; longMs = Math.max(longMs, Math.round(Math.max(0, ...got.long)));
    if (o.after) await o.after(i);
  }
  const perAct = x => +(x / n).toFixed(1);
  return rec(name, times, { work50: Math.round(q(work, .5)), work95: Math.round(q(work, .95)), long, longMs, calls: perAct(calls), kb: Math.round(bytes / 1024 / n), ops: [...ops].sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}×${perAct(v)}`).join(' ') }, o.budget);
}

async function scrollList(page, name, sel, o = {}) {
  const from = await page.evaluate(() => [performance.now(), window.__net.length]);
  const frame = await page.evaluate(async sel => {
    const host = document.querySelector(sel); if (!host) return -1; const frames = []; let last = performance.now(), on = true;
    const tick = t => { frames.push(t - last); last = t; if (on) requestAnimationFrame(tick); }; requestAnimationFrame(tick);
    for (let i = 0; i < 40; i++) { host.scrollTop += 400; const more = host.querySelector('.libMore button, [data-more]'); if (more && i % 8 === 7) more.click(); await new Promise(r => setTimeout(r, 30)); }
    on = false; await new Promise(r => requestAnimationFrame(r)); frames.shift(); host.scrollTop = 0; return Math.max(...frames);
  }, sel);
  await page.waitForTimeout(400);
  const got = await page.evaluate(([t0, n0]) => ({ net: window.__net.slice(n0).map(x => [x.op || x.fn, x.bytes]), long: window.__long.filter(([s]) => s >= t0).map(([, d]) => d) }), from);
  const ops = new Map(); for (const [op] of got.net) ops.set(op, (ops.get(op) || 0) + 1);
  return rec(name + ' (longest frame)', [frame], { long: got.long.length, longMs: Math.round(Math.max(0, ...got.long)), calls: got.net.length, kb: Math.round(got.net.reduce((a, [, b]) => a + b, 0) / 1024), ops: [...ops].map(([k, v]) => `${k}×${v}`).join(' ') }, o.budget);
}

async function scale() {
  const w = await world(); if (!w) { console.log('  – no playwright-core: the browser checks were not run'); return null; }
  const { page } = w;
  try {
    console.log(`  fixture: ${OPEN} open orders · ${HIST} historical · load ${os.loadavg().map(x => x.toFixed(2)).join(' ')}`);
    // ── first load
    await page.goto(`${w.srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && CN.S.cloud.ok === true && document.querySelector('#modeSeg button[data-mode="orders"]'));
    const t0 = await page.evaluate(() => performance.now());
    await page.waitForTimeout(1500);
    const first = await page.evaluate(t0 => ({ net: window.__net.filter(x => x.t <= t0 + 1500), long: window.__long.filter(([s]) => s <= t0 + 1500).map(([, d]) => d) }), t0);
    { const ops = new Map(); for (const x of first.net) ops.set(x.op || x.fn, (ops.get(x.op || x.fn) || 0) + 1);
      rec('first load (to usable screen)', [t0], { long: first.long.length, longMs: Math.round(Math.max(0, ...first.long)), calls: first.net.length, kb: Math.round(first.net.reduce((a, x) => a + x.bytes, 0) / 1024), ops: [...ops].sort((a, b) => b[1] - a[1]).slice(0, 8).map(([k, v]) => `${k}×${v}`).join(' ') }); }
    // ── the live run's open orders (what a pull hands Orders)
    const toRows = `(orders => orders.flatMap(o => o.lines.map(l => ({ key: CharmNestOrders.lineKey(o, l), order: o, line: l, arrivedAt: o.createTs * 1000, spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }))))`;
    await page.evaluate(async ({ orders, toRows }) => { B.orders.rows = eval(toRows)(orders); B.orders.byKey = new Map(B.orders.rows.map(r => [r.key, r])); await Orders.interpretAll(); }, { orders: F.openOrders(OPEN), toRows });
    const TABS = ['orders', 'engrave', 'review', 'library', 'nest'];
    // the first visit of each tab (draws it; what a person meets once a session)
    for (const m of TABS) await measure(page, `tab → ${m} (first visit)`, 1, new Function(`document.querySelector('#modeSeg button[data-mode="${m}"]').click()`), { settle: 1200 });
    await page.waitForTimeout(1500);
    // ── tab switches, all day: round after round, each tab from the one before
    const R = 8, tabTimes = Object.fromEntries(TABS.map(m => [m, { t: [], w: [], long: 0, longMs: 0, calls: 0, kb: 0, ops: new Map() }]));
    for (let r = 0; r < R; r++) for (const m of TABS) {
      const from = await page.evaluate(() => [performance.now(), window.__net.length]);
      const [t, wk] = await page.evaluate(async m => { const t0 = performance.now(); document.querySelector(`#modeSeg button[data-mode="${m}"]`).click(); const t1 = performance.now(); await new Promise(r => requestAnimationFrame(() => setTimeout(r, 0))); return [performance.now() - t0, t1 - t0]; }, m);
      await page.waitForTimeout(350);
      const got = await page.evaluate(([t0, n0]) => ({ net: window.__net.slice(n0).map(x => [x.op || x.fn, x.bytes]), long: window.__long.filter(([s]) => s >= t0).map(([, d]) => d) }), from);
      const T = tabTimes[m]; T.t.push(t); T.w.push(wk); T.long += got.long.length; T.longMs = Math.max(T.longMs, Math.round(Math.max(0, ...got.long)));
      for (const [op, b] of got.net) { T.calls++; T.kb += b / 1024; T.ops.set(op, (T.ops.get(op) || 0) + 1); }
    }
    // (the Library shown again within a minute reads by revision only: its whole read was 8 calls and 2.5 MB at this size)
    for (const m of TABS) { const T = tabTimes[m]; rec(`tab → ${m}`, T.t, { work50: Math.round(q(T.w, .5)), work95: Math.round(q(T.w, .95)), long: T.long, longMs: T.longMs, calls: +(T.calls / R).toFixed(1), kb: Math.round(T.kb / R), ops: [...T.ops].sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k}×${+(v / R).toFixed(1)}`).join(' ') }, m === 'library' ? { p95: BUDGET.tabP95, calls: BUDGET.libCalls, kb: BUDGET.libKB } : { p95: BUDGET.tabP95, calls: BUDGET.tabCalls }); }

    // ── scrolling each list
    await page.evaluate(() => CN.setMode('orders')); await page.waitForTimeout(500);
    await scrollList(page, 'scroll Orders list', '#ordBody');
    await page.evaluate(() => CN.setMode('review')); await page.waitForTimeout(500);
    await scrollList(page, 'scroll Review list', '#stage');
    await page.evaluate(() => CN.setMode('library')); await page.waitForTimeout(1500);
    await scrollList(page, 'scroll Library sets', '#stage');

    // ── the order window: opened from the Orders list, its tabs, Back / Next, closed
    await page.evaluate(() => CN.setMode('orders')); await page.waitForTimeout(600);
    const keys = await page.evaluate(() => [...document.querySelectorAll('#ordBody [data-key]')].map(n => n.dataset.key).slice(0, 12));
    const shut = () => page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.close(); });
    await measure(page, 'order window open (Overview)', 8, new Function('i', `OrderWin.open(${JSON.stringify(keys)}[i % ${keys.length}])`), { settle: 500, after: async () => { await shut(); await page.waitForTimeout(250); }, budget: { p50: BUDGET.winOpenP50 } });
    await page.evaluate(k => OrderWin.open(k), keys[0]); await page.waitForTimeout(800);
    const view = v => new Function(`document.querySelector('#orderWin .owTabsV [data-ow-view="${v}"]').click()`);
    await measure(page, 'order window → Timeline tab', 6, view('timeline'), { settle: 400, after: () => page.evaluate(() => document.querySelector('#orderWin .owTabsV [data-ow-view="info"]').click()).then(() => page.waitForTimeout(300)), budget: { p95: BUDGET.winTabP95 } });
    await measure(page, 'order window → Sheet tab', 6, view('sheet'), { settle: 400, after: () => page.evaluate(() => document.querySelector('#orderWin .owTabsV [data-ow-view="info"]').click()).then(() => page.waitForTimeout(300)), budget: { p95: BUDGET.winTabP95 } });
    await measure(page, 'order window → Overview tab', 6, view('info'), { settle: 400, before: () => page.evaluate(() => document.querySelector('#orderWin .owTabsV [data-ow-view="timeline"]').click()).then(() => page.waitForTimeout(300)), budget: { p95: BUDGET.winTabP95 } });
    await measure(page, 'order window Next', 10, () => document.getElementById('owNext').click(), { settle: 450, budget: { p95: BUDGET.winStepP95 } });
    await measure(page, 'order window Back', 10, () => document.getElementById('owPrev').click(), { settle: 450, budget: { p95: BUDGET.winStepP95 } });
    await shut(); await page.waitForTimeout(400);

    // ── search and filters
    const typing = (sel, text) => page.evaluate(async ({ sel, text }) => {
      const box = document.querySelector(sel); box.focus(); box.value = ''; const per = [];
      for (const ch of text) { const t0 = performance.now(); box.value += ch; box.dispatchEvent(new InputEvent('input', { bubbles: true, data: ch })); box.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: ch })); await new Promise(r => requestAnimationFrame(() => setTimeout(r, 0))); per.push(performance.now() - t0); }
      await new Promise(r => setTimeout(r, 700)); box.value = ''; box.dispatchEvent(new InputEvent('input', { bubbles: true })); return per;
    }, { sel, text });
    const typed = async (name, sel, text) => { const from = await page.evaluate(() => [performance.now(), window.__net.length]); const per = await typing(sel, text); await page.waitForTimeout(600);
      const got = await page.evaluate(([t0, n0]) => ({ net: window.__net.slice(n0).map(x => [x.op || x.fn, x.bytes]), long: window.__long.filter(([s]) => s >= t0).map(([, d]) => d) }), from);
      const ops = new Map(); for (const [op] of got.net) ops.set(op, (ops.get(op) || 0) + 1);
      rec(name, per, { long: got.long.length, longMs: Math.round(Math.max(0, ...got.long)), calls: got.net.length, kb: Math.round(got.net.reduce((a, [, b]) => a + b, 0) / 1024), ops: [...ops].map(([k, v]) => `${k}×${v}`).join(' ') }); };
    await typed('Orders search (per keystroke, 10 keys)', '#ordQ', '4300000123');
    const chip = v => new Function('i', `const v = ${JSON.stringify(v)}[i % ${v.length}]; document.querySelector('#ordersView .ordChip[data-f="metal"][data-v="' + v + '"]').click()`);
    await measure(page, 'Orders filter (metal chip)', 6, chip(['gold', 'silver', '']), { settle: 400 });
    await page.evaluate(() => CN.setMode('library')); await page.waitForTimeout(1200);
    await typed('Library search (per keystroke, 7 keys)', '#libSearch', '4100000');
    await page.waitForTimeout(800);
    await measure(page, 'Library filter (metal chip)', 6, new Function('i', `document.querySelector('#libView .fchip[data-m="' + ['gold', 'silver', 'all'][i % 3] + '"]').click()`), { settle: 600 });
    await page.waitForTimeout(800);

    // ── the sheet window from the Library, and sheet to sheet
    const sheetIds = await page.evaluate(() => (S => S.library.rows || [])(CN.S).filter(r => r.setId).slice(0, 8).map(r => r.id || r.sheetId));
    if (sheetIds.length >= 2 && await page.evaluate(() => !!window.SheetWin)) {
      const shutSheet = () => page.evaluate(() => { try { SheetWin.close(); } catch (_) {} });
      await measure(page, 'sheet window open (from Library)', 5, new Function('i', `return SheetWin.open(${JSON.stringify(sheetIds)}[i % ${sheetIds.length}])`), { settle: 600, after: async () => { await shutSheet(); await page.waitForTimeout(400); }, budget: { p95: BUDGET.sheetWinP95 } });
      await page.evaluate(id => SheetWin.open(id), sheetIds[0]); await page.waitForTimeout(800);
      await measure(page, 'sheet window → another sheet', 8, new Function('i', `return SheetWin.open(${JSON.stringify(sheetIds)}[(i + 1) % ${sheetIds.length}])`), { settle: 600, budget: { p95: BUDGET.sheetWinP95 } });
      await shutSheet();
    } else console.log('  – no sheets in the Library rows: the sheet window was not measured');
    if (w.errors.length) { console.log('  page errors:', w.errors.slice(0, 5)); fails.push(w.errors.length + ' page error(s)'); }
  } finally { await w.close(); }
}

async function engraving() {
  if (!process.env.PW_DIR && pwDir) process.env.PW_DIR = pwDir;   // (the engraving world finds playwright-core where this file found it)
  if (!process.env.CHROMIUM) process.env.CHROMIUM = CHROME;          // (and the browser this file runs: its own default is an older build)
  const say = m => { if (process.env.TRACE) console.log('    · ' + m); };
  const within = (what, p, ms = 120000) => { let t; return Promise.race([p, new Promise((_, no) => { t = setTimeout(() => no(new Error(`${what}: no answer in ${ms / 1000} s`)), ms); })]).finally(() => clearTimeout(t)); };
  const R = require('./_engraveRealWorld.cjs');
  say('starting the engraving world');
  const w = await within('starting the engraving world', R.world({ viewport: { width: 1440, height: 900 } })); if (!w) { console.log('  – the engraving world did not start'); return; }
  say('world up');
  const page = w.page;
  try {
    await page.evaluate(probes);
    say('building the engraving orders');
    const made = await within('building the engraving orders', R.build(w), 180000);
    say('built');
    const jobs = Object.values(made).flatMap(m => m.jobs.map(j => j.key));
    console.log(`  engraving world: ${jobs.length} placements · load ${os.loadavg().map(x => x.toFixed(2)).join(' ')}`);
    const openAt = k => new Function(`CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: ${JSON.stringify(k)} }); Engrave.render();`);
    await measure(page, 'Engraving: open a placement', 4, new Function('i', `const k = ${JSON.stringify(jobs)}[i % ${jobs.length}]; CN.setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: k }); Engrave.render();`), { settle: 600, before: () => page.evaluate(() => CN.setMode('orders')).then(() => page.waitForTimeout(300)) });
    // Next / Back: the card's own ‹ Back and Next › (the placement before or after it in the queue), as a person presses them: 20 presses, through every placement and round
    const step = d => new Function('i', `const b = document.querySelector('#engraveView .rh [data-a="${d > 0 ? 'next' : 'prev'}"]'); if (b) b.click();`);
    await page.evaluate(openAt(jobs[0])); await page.waitForTimeout(800);
    const focusOf = () => page.evaluate(() => (Engrave.view() || {}).focus || null);
    const f0 = await focusOf(); await page.evaluate(step(1)); await page.waitForTimeout(400); const f1 = await focusOf();
    console.log(`  engraving Next moves the card: ${f0 !== f1 ? 'yes' : 'NO'} (${f0} → ${f1})`); if (f0 === f1) fails.push('Engraving Next did not move to another placement');
    await measure(page, 'Engraving Next', 20, step(1), { settle: 250, budget: { p95: BUDGET.engStepP95 } });
    await measure(page, 'Engraving Back', 20, step(-1), { settle: 250, budget: { p95: BUDGET.engStepP95 } });
    await measure(page, 'Engraving Approve', 1, () => { const b = document.querySelector('#engraveView .egApproveButton[data-a="approve"], #engraveView [data-e="approve"]:not([disabled])'); if (b) b.click(); }, { settle: 1500 });
    if (w.errors && w.errors.length) console.log('  page errors (engraving world):', w.errors.slice(0, 5));
  } finally { await w.close(); }
}

/* The engraving phase runs in a process of its own: a second bridge-server started in this process after the scale world shares the functions' modules (Node's require cache) with
   the first one, whose servers are closed, and the engraving world's first build waited on them for good (28 Oct 2026: the run sat at 2 % CPU until it was killed). Each step of it
   also has a time limit, named when it is passed, so it can never wait in silence. */
function engravingApart() {
  const { spawnSync } = require('child_process'), out = path.join(os.tmpdir(), `nav-instant-eng-${process.pid}.json`);
  const r = spawnSync(process.execPath, [__filename], { stdio: 'inherit', env: Object.assign({}, process.env, { ONLY_ENGRAVE: '1', JSON: out }), timeout: 300000 });
  try { rows.push(...JSON.parse(fs.readFileSync(out, 'utf8'))); fs.unlinkSync(out); } catch (_) {}
  if (r.status !== 0) fails.push(`the engraving phase ${r.error ? 'did not finish: ' + r.error.message : 'ended with ' + (r.status == null ? r.signal : 'exit ' + r.status)}`);
}
async function main() {
  if (!process.env.ONLY_ENGRAVE) await scale();
  if (process.env.ONLY_ENGRAVE) await engraving().catch(e => { console.log('  engraving world failed:', e.message); fails.push('engraving world: ' + e.message); });
  else if (!process.env.SKIP_ENGRAVE) engravingApart();
  if (process.env.JSON) fs.writeFileSync(process.env.JSON, JSON.stringify(rows, null, 1));
  if (fails.length) { console.log('\n  OVER BUDGET:\n   · ' + fails.join('\n   · ')); process.exitCode = 1; } else console.log('\n  every budget held');
}
if (require.main === module) main().catch(e => { console.error(e); process.exitCode = 1; });
module.exports = { probes, measure };
