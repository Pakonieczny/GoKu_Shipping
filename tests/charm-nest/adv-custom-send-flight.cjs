// The Custom designs window's Send to Sheet (Paul, 28 Sep: animations "not exactly visible" and "jerky"): a design put
// on Gold and sent, the window goes back into its card and a copy of the designs flies to the Nest tab, which says "+1".
// An adversarial check saw long tasks of 98 and 55 ms during that flight. Measured here in a real Chromium, unthrottled,
// on the fake site (bridge-server.cjs), with requestAnimationFrame deltas, long tasks and every element.animate() call:
//   visible · the flying copy is on screen, moves, and a "+1" rises from the tab;
//   smooth  · from the window's close (it goes back into its card) until the copy lands: no long task over 50 ms, at
//             most one frame in ten over 34 ms (before: the whole list was redrawn again in the task that closed it);
//   cheap   · only transform, opacity and clip-path animated;
//   whole   · the line is sent and placed on a Gold sheet of the run under way, and the card says so.
// PROFILE=1 prints what ran in each long task (a CPU profile, by the page functions on the stack); TRACE=1 what the main
// thread did in each; BEFORE=<a checkout of the code before> runs that code instead, for a before/after comparison.
// The timing gate is hard only when the machine is quiet (load per core < 1.2).
//   node tests/charm-nest/adv-custom-send-flight.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), os = require('os');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const LAYOUT = /^(top|left|right|bottom|width|height|maxHeight|minHeight|maxWidth|minWidth|margin.*|padding.*|border.*Width|inset|flex.*|gap)$/;
const PAINT = /^(boxShadow|filter|backdropFilter|background.*|color|borderColor|outline.*)$/;
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const ORDERS = [{ receiptId: '4175423829', orderNumber: '4175423829', createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer 3829' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: '41754238291', listingId: '1800038291', sku: 'CUSTOM-N-001-665441', title: 'Custom Name Necklace, Personalized Gold Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] }];

function recorders() {
  const M = window.__M = { rec: false };
  const A = Element.prototype.animate;
  Element.prototype.animate = function (k, o) {
    if (M.rec) {
      const props = new Set(); for (const f of Array.isArray(k) ? k : k ? [k] : []) for (const p of Object.keys(f)) if (!['offset', 'easing', 'composite'].includes(p)) props.add(p);
      const cls = typeof this.className === 'string' ? this.className : (this.getAttribute && this.getAttribute('class')) || this.tagName;
      M.anims.push({ cls: String(cls).slice(0, 30), props: [...props], t: performance.now() - M.t0 });
    }
    return A.call(this, k, o);
  };
  try { new PerformanceObserver(l => { if (M.rec) for (const e of l.getEntries()) M.long.push([Math.round(e.startTime - M.t0), Math.round(e.duration)]); }).observe({ entryTypes: ['longtask'] }); } catch (_) {}
  const loop = t => { if (M.rec) { M.frames.push(t - M.t0); for (const [name, fn] of M.watch) { let v = null; try { v = fn(); } catch (_) {} (M.seen[name] = M.seen[name] || []).push([Math.round(t - M.t0), v]); } } requestAnimationFrame(loop); };
  requestAnimationFrame(loop);
  window.__start = () => Object.assign(M, { rec: true, frames: [], long: [], anims: [], watch: [], seen: {}, marks: [], t0: performance.now() });
  window.__mark = name => { if (M.rec) M.marks.push([name, Math.round(performance.now() - M.t0)]); };
  window.__stop = () => { M.rec = false; const d = []; for (let i = 2; i < M.frames.length; i++) d.push([Math.round(M.frames[i]), M.frames[i] - M.frames[i - 1]]); return { d, long: M.long, anims: M.anims, seen: M.seen, marks: M.marks, t0: M.t0 }; };
}
const judge = (r, from = 0, to = 1e9) => {
  const d = r.d.filter(([t]) => t >= from && t <= to);
  return { frames: d.length, worst: Math.round(Math.max(0, ...d.map(x => x[1]))), over34: d.filter(x => x[1] > 34).length, long: r.long.filter(([s, ms]) => ms > 50 && s + ms > from && s < to).map(x => x[1]) };
};

/** what ran during each long task: the profile's samples in it, by the outermost page function on the stack */
function blame(profile, longs) {
  const byId = new Map(profile.nodes.map(n => [n.id, n])), parent = new Map();
  for (const n of profile.nodes) for (const c of n.children || []) parent.set(c, n.id);
  const out = [];
  let t = profile.startTime; const times = profile.timeDeltas.map(d => (t += d));
  // the recording's 0 in the profile's clock: the first sample of __syncMark, a 4 ms spin run just after __start()
  const i0 = profile.samples.findIndex(id => byId.get(id).callFrame.functionName === '__syncMark'); if (i0 < 0) return [];
  const base = times[i0];
  for (const [s, ms] of longs) {
    const from = base + s * 1000, to = from + ms * 1000, tally = new Map(), self = new Map();
    profile.samples.forEach((id, i) => {
      if (times[i] < from || times[i] > to) return;
      const chain = []; for (let x = id; x != null; x = parent.get(x)) chain.push(byId.get(x).callFrame);
      const own = chain[0], ownKey = `${own.functionName || '(anon)'}@${path.basename(own.url || '')}:${own.lineNumber + 1}`;
      self.set(ownKey, (self.get(ownKey) || 0) + 1);
      const page = chain.filter(f => f.url && /charm-nest|order-timeline|brites/.test(f.url));
      const keys = [...new Set(page.slice(-6).reverse().map(f => `${f.functionName || '(anon)'}@${path.basename(f.url)}:${f.lineNumber + 1}`))].join(' > ') || '(native) ' + (own.functionName || '');
      tally.set(keys, (tally.get(keys) || 0) + 1);
    });
    out.push({ at: s, ms, stacks: [...tally].sort((a, b) => b[1] - a[1]).slice(0, 6), self: [...self].sort((a, b) => b[1] - a[1]).slice(0, 8) });
  }
  return out;
}

async function scene(browser, profile) {
  const { start } = require(path.join(process.env.BEFORE ? path.resolve(process.env.BEFORE) : here, 'tests/charm-nest/bridge-server.cjs'));
  const srv = await start({ receipts: [] });
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  try {
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    await context.addInitScript(recorders);
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(20000);
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, ORDERS);
    // a run under way (as in the shop, past its pool step): a line sent now goes onto its sheet at once, and the sheet
    // is drawn and nested while the copy flies
    await page.evaluate(() => { const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-flight`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; });
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const card = '#rvList .reviewListRow[data-rid="4175423829"]';
    await page.waitForSelector(card);
    await page.evaluate(({ sel, text }) => { const dt = new DataTransfer(); dt.items.add(new File([text], 'heart-a.dxf')); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(18) });
    await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
    await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
    // (as a person takes a moment over the window: the workspace's own save paces itself ten seconds apart, and one due
    // now is written when the send changes it, as in the shop)
    await page.waitForTimeout(+(process.env.WAIT_MS || 11000));
    // the flight marked where it starts and ends (Motion.fly is the page's own; wrapped, not changed)
    // (the workspace's save, written to IndexedDB, marked when it is written)
    await page.evaluate(() => { const put = IDBObjectStore.prototype.put; IDBObjectStore.prototype.put = function () { if (this.name === 'workspaces') __mark('save'); return put.apply(this, arguments); }; });
    await page.evaluate(() => { const c = Motion.dialogClose; Motion.dialogClose = function () { __mark('close'); return c.apply(this, arguments); }; });
    await page.evaluate(() => { const f = Motion.fly; Motion.fly = function (g) { __mark('fly'); __M.flyG = g; const p = f.apply(this, arguments); Promise.resolve(p).then(() => __mark('landed')); return p; }; });
    let cdp = null;
    let trace = null;
    if (process.env.TRACE) {
      trace = { cdp: await context.newCDPSession(page), events: [] };
      trace.cdp.on('Tracing.dataCollected', e => trace.events.push(...e.value));
      trace.done = new Promise(res => trace.cdp.on('Tracing.tracingComplete', res));
      await trace.cdp.send('Tracing.start', { transferMode: 'ReportEvents', traceConfig: { includedCategories: ['devtools.timeline', 'disabled-by-default-devtools.timeline', 'v8', 'blink.user_timing'] } });
    }
    if (profile) { cdp = await context.newCDPSession(page); await cdp.send('Profiler.enable'); await cdp.send('Profiler.setSamplingInterval', { interval: 500 }); await cdp.send('Profiler.start'); }
    const r = await page.evaluate(async () => {
      __start(); performance.mark('__t0'); (window.__syncMark = function __syncMark() { const t = performance.now(); while (performance.now() - t < 4); })();
      __M.watch.push(['copy', () => { const g = __M.flyG; if (!g || !g.isConnected) return null; const b = g.getBoundingClientRect(); return Math.round(b.left + b.top) + '|' + getComputedStyle(g).opacity; }],
        ['plus', () => [...document.querySelectorAll('.mPlus')].map(p => p.textContent).join(',') || null]);
      document.querySelector('#cuDlg [data-send]').click(); __mark('clicked');
      const t = performance.now(); while (!__M.marks.some(m => m[0] === 'landed') && performance.now() - t < 12000) await new Promise(res => setTimeout(res, 50));
      await new Promise(res => setTimeout(res, 400));
      const out = __stop(); out.origin = performance.timeOrigin; return out;
    });
    if (trace) {
      await trace.cdp.send('Tracing.end'); await trace.done;
      // the recording's 0: the user-timing mark made right after __start(); what the main thread did in each long task
      const m0 = trace.events.find(e => e.name === '__t0' && e.cat.includes('blink.user_timing'));
      const main = m0 ? trace.events.filter(e => e.pid === m0.pid && e.tid === m0.tid && e.ph === 'X' && e.dur) : [];
      for (const [s0, ms] of r.long.filter(([, ms]) => ms > +(process.env.LONG_MS || 50))) {
        const from = m0.ts + s0 * 1000, to = from + ms * 1000, tally = new Map();
        for (const e of main) if (e.ts >= from - 500 && e.ts < to && !['RunTask', 'ThreadControllerImpl::RunTask', 'RunMicrotasks'].includes(e.name) && e.dur > 1000) tally.set(e.name + (e.args && e.args.data && e.args.data.functionName ? ':' + e.args.data.functionName : e.args && e.args.data && e.args.data.url ? ':' + path.basename(e.args.data.url).slice(0, 30) + ':' + (e.args.data.lineNumber || '') : ''), (tally.get(e.name) || 0) + Math.round(e.dur / 100) / 10);
        console.log(`  trace · long task at ${s0} ms, ${ms} ms: ` + [...tally].sort((a, b) => b[1] - a[1]).slice(0, 14).map(([k, v]) => `${k} ${v}`).join(' · '));
      }
    }
    let prof = null;
    if (cdp) { prof = (await cdp.send('Profiler.stop')).profile; }
    const end = await page.evaluate(() => { const row = B.orders.byKey.get('4175423829_41754238291'); return { state: row.state, onGold: allSheets().filter(p => p.metal === 'gold').reduce((n, p) => n + p.charms.filter(c => c.custom).length, 0), sent: !!CustomSheet.sentOf(row), strip: !!document.querySelector('#rvList .reviewListRow[data-rid="4175423829"] .cuDesigns.sent'), dlg: document.querySelector('#cuDlg').open, ghosts: document.querySelectorAll('#motionLayer .mGhost').length }; });
    const fly = (r.marks.find(m => m[0] === 'fly') || [])[1], landed = (r.marks.find(m => m[0] === 'landed') || [])[1];
    let blamed = null;
    if (prof && fly != null) {
      // the long tasks, and the frames that took over 30 ms (a task under 50 ms is not reported as long, yet drops a frame)
      const longs = r.long.filter(([s, ms]) => ms > +(process.env.LONG_MS || 50)).concat(r.d.filter(([, v]) => v > 30).map(([t, v]) => [Math.round(t - v), Math.round(v)]));
      blamed = { longs, list: blame(prof, longs) };
    }
    return { r, fly, landed, end, errors, blamed };
  } finally { await context.close(); await srv.close(); }
}

(async () => {
  const load = os.loadavg()[0] / os.cpus().length, quiet = load < 1.2;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], check = (ok, what, hard = true) => { if (!ok && hard) fails.push(what); console.log((ok ? '  ok   ' : hard ? '  FAIL ' : '  warn ') + what); };
  let s;
  try { s = await scene(browser, !!process.env.PROFILE); } finally { await browser.close(); }
  const { r, fly, landed, end } = s;
  // the flights: the window going back into its card (from its close) and the copy going to the Nest tab, until it lands
  const close = (r.marks.find(m => m[0] === 'close') || [])[1];
  const during = fly != null && landed != null ? judge(r, close != null ? close : fly, landed) : null, before = judge(r, 0, close != null ? close : fly);
  const props = [...new Set(r.anims.filter(a => fly != null && a.t >= fly - 5).flatMap(a => a.props))];
  const layout = props.filter(p => LAYOUT.test(p)), paint = props.filter(p => PAINT.test(p));
  const copy = (r.seen.copy || []).map(x => x[1]).filter(Boolean), pos = new Set(copy.map(v => v.split('|')[0])), plus = (r.seen.plus || []).map(x => x[1]).filter(Boolean);
  console.log(`\n  load per core ${load.toFixed(2)} (${quiet ? 'quiet: timing gates are hard' : 'busy: timing gates only warn'})`);
  console.log(`  clicked at 0, window closed at ${close} ms, flight ${fly}–${landed} ms; all long tasks ${JSON.stringify(r.long)}; workspace saved at ${JSON.stringify(r.marks.filter(m => m[0] === 'save').map(m => m[1]))} ms`);
  if (during) console.log(`  during the flights (close → landed): worst ${during.worst} ms, >34 ms ${during.over34}/${during.frames}, long ${JSON.stringify(during.long)}; slow frames ${JSON.stringify(r.d.filter(([t, v]) => v > 30 && t >= (close ?? fly) && t <= landed).map(([t, v]) => [t, Math.round(v)]))}`);
  if (before) console.log(`  click → close:    worst ${before.worst} ms, long ${JSON.stringify(before.long)}`);
  if (s.blamed) for (const b of s.blamed.list) { console.log(`\n  long task at ${b.at} ms, ${b.ms} ms:`); for (const [k, n] of b.stacks) console.log(`    ${String(n).padStart(4)} ${k}`); console.log('    self: ' + b.self.map(([k, n]) => `${k} ${n}`).join(' · ')); }
  console.log('');
  check(fly != null && landed != null && landed - fly >= 1200, `the copy flies to the Nest tab (${fly}–${landed} ms)`);
  check(pos.size >= 10 && plus.some(p => /\+1/.test(p)), `the copy is seen moving (${pos.size} positions) and "+1" rises (${[...new Set(plus)].join(' ')})`);
  check(!layout.length && !paint.length, `only transform, opacity and clip-path animated in the flight (${props.join(',')})`);
  check(end.sent && end.strip && !end.dlg && !end.ghosts && end.state === 'pooled' && end.onGold === 1, `sent, placed on a Gold sheet, the card says so, the window closed, no copy left (${JSON.stringify(end)})`);
  if (during) check(!during.long.length && during.over34 <= during.frames / 10, `the window's way back and the flight are smooth (from the close to the landing): worst ${during.worst} ms, >34 ms ${during.over34}/${during.frames}, long ${JSON.stringify(during.long)}`, quiet);
  check(!s.errors.length, 'no page errors: ' + s.errors.join(' | '));
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
