// A simulated day of Etsy orders through the whole Charm Sorter process: the fake site (Etsy, Firestore, Storage,
// the model) on two origins, the sorter and the framed Design Station in Chromium on a shared simulated clock, the
// sorter's own 10-minute intake in Auto mode, and a person who approves engravings and accepts changed orders during
// working hours and records the Rose Gold cut in the evening. It takes one to two hours; the outcome is checked by
// analyze.cjs. PW_DIR names a directory holding playwright-core; CHROMIUM names the browser.
//   ORDERS=100 HOURS=24 SEED=7 FIXTURE_SCALE=0.8 node tests/charm-nest/day-sim/day.cjs [out.json]
//   node tests/charm-nest/day-sim/analyze.cjs out.json
// Production-size options (all off by default): BACKLOG=400 open orders already waiting at the first check (a cold pull,
// paced by the station under Etsy's own limits, the page's clock stepped so it takes seconds), TICK_MIN=60 minutes between
// checks (also the sorter's poll interval), START_HOUR=7 starts the day at that Toronto hour, CRASH_HOUR=13 closes the
// page without warning at the first check of that hour and opens it again, DEADLINE_S=190 stops the day at that many wall
// seconds and still writes the outcome, CRASH_AFTER_S=40 does the same crash that many wall seconds after the first pull
// (a backlog of 400 keeps the nester busy for hours: the crash comes in the middle of that work), RUN_MODE=manual leaves the
// run off so only the intake is exercised (no sheets: a cheap 24 hours of checks), ETSY_LIMITS_OFF=1 lifts Etsy's 60-a-minute and
// 150-in-ten-minutes pacing and the 250 ms between reads in the station's copy (a stepped clock cannot wait out 25 minutes of it in
// seconds without making the reads time out), so the first pull runs on the page's own clock, NO_SHOTS=1 skips the closing screenshots. Each check also records its own time, the
// page's busy time, memory, and what localStorage / IndexedDB hold, for analyze.cjs.
const fs = require('fs'), path = require('path'), os = require('os');
const realNow = Date.now.bind(Date);
let simOffset = 0;
Date.now = () => realNow() + simOffset;            // the fake server's clock follows the pages' clock
const { open, repo } = require('./harness.cjs');

const ORDERS = +process.env.ORDERS || 100, HOURS = +process.env.HOURS || 24, SEED = +process.env.SEED || 7, ONLY = process.env.ONLY || null;
const BACKLOG = +process.env.BACKLOG || 0, TICK_MIN = +process.env.TICK_MIN || 10, TICK = TICK_MIN * 60000;
const START_HOUR = process.env.START_HOUR ? +process.env.START_HOUR : null, CRASH_HOUR = process.env.CRASH_HOUR ? +process.env.CRASH_HOUR : -1;
const DEADLINE_S = +process.env.DEADLINE_S || 0, WALL0 = realNow(), CRASH_AFTER_S = +process.env.CRASH_AFTER_S || 0, RUN_MODE = process.env.RUN_MODE || 'auto', ETSY_LIMITS_OFF = !!process.env.ETSY_LIMITS_OFF;
const pastDeadline = (reserveS = 0) => !!DEADLINE_S && (realNow() - WALL0) / 1000 + reserveS > DEADLINE_S;
const OUT = path.resolve(process.argv[2] || path.join(os.tmpdir(), 'charm-sorter-day.json'));
const LOG = OUT.replace(/\.json$/, '.log');
fs.writeFileSync(LOG, '');
const log = (...a) => { const line = (DEADLINE_S ? `[${Math.round((realNow() - WALL0) / 1000)}s] ` : '') + a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '); fs.appendFileSync(LOG, line + '\n'); console.log(line); };

// ── the orders ──
function rng(seed) { let a = seed >>> 0; return () => { a = (a + 0x6D2B79F5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }
const R = rng(SEED);
const pick = (xs, ws) => { let r = R() * ws.reduce((a, b) => a + b, 0); for (let i = 0; i < xs.length; i++) { r -= ws[i]; if (r < 0) return xs[i]; } return xs[xs.length - 1]; };
const METAL_TEXT = { GF: '14k Gold Filled', SS: 'Sterling Silver', RG: '14k Rose Gold Filled', '14K': '14K Solid Gold', '10K': '10K Solid Gold', '??': 'Solid Gold' };
const NAMES = ['ANNA', 'LEO', 'MIA', 'NOAH', 'ZOE', 'ELI', 'IVY', 'MAX', 'AVA', 'SAM', 'LILY', 'JACK', 'EMMA', 'OWEN', 'NORA', 'LUCA'];
// Toronto hour weights for when orders are placed: quiet overnight, busiest late morning to evening
const HOUR_W = [2, 1, 1, 1, 1, 2, 3, 4, 6, 7, 8, 8, 8, 8, 8, 8, 8, 9, 9, 9, 8, 7, 5, 3];
function torontoHour(ms) { return +new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Toronto', hour: '2-digit', hourCycle: 'h23' }).format(new Date(ms)); }

function makeOrders(t0) {
  // arrival times: weighted by the Toronto hour, spread uniformly inside it
  const slots = []; for (let m = 0; m < HOURS * 60; m++) slots.push(t0 + m * 60000);
  const times = [];
  // the orders already open at the first check: placed over the three days before it, the newest last
  for (let k = 0; k < BACKLOG; k++) times.push(t0 - 3 * 86400000 + Math.floor((k + R()) * 3 * 86400000 / BACKLOG) - 1000);
  while (times.length < BACKLOG + ORDERS) { const t = slots[Math.floor(R() * slots.length)]; if (R() * 9 < HOUR_W[torontoHour(t)]) times.push(t + Math.floor(R() * 60000)); }
  times.sort((a, b) => a - b);
  const orders = [];
  times.forEach((t, i) => {
    const rid = 4200000001 + i, lines = pick([1, 2, 3], [70, 22, 8]);
    const orderMetal = ONLY || pick(['GF', 'SS', 'RG', '14K', '10K', '??'], [45, 34, 13, 4, 2, 2]);
    const txs = [];
    for (let j = 0; j < lines; j++) {
      const metal = ONLY || (R() < 0.72 ? orderMetal : pick(['GF', 'SS', 'RG'], [45, 40, 15]));
      const unknown = !ONLY && R() < 0.02;
      const skuN = 1 + Math.floor(R() * 20), sku = unknown ? `BR-NOPE-${String(i).padStart(2, '0')}` : `BR-TST-${String(skuN).padStart(2, '0')}`;
      const personal = R() < 0.25 ? pick(NAMES, NAMES.map(() => 1)) + (R() < 0.2 ? ' ' + (2000 + Math.floor(R() * 26)) : '') : null;
      const variations = [{ formatted_name: 'Metal', formatted_value: METAL_TEXT[metal] }];
      if (personal) variations.push({ formatted_name: 'Personalization', formatted_value: personal });
      txs.push({ transaction_id: Number(`${rid}${j + 1}`), listing_id: 1718000 + (unknown ? 900 + i : skuN), receipt_id: rid, sku, title: `${sku} charm`, quantity: pick([1, 2, 3], [85, 12, 3]), expected_ship_date: Math.floor(t / 1000) + 86400 * (3 + Math.floor(R() * 3)), variations, is_personalized: !!personal, _metal: metal, _personal: personal });
    }
    const msg = R() < 0.04 ? 'Could you please make sure it arrives before Friday?' : '';
    orders.push({ at: t, receipt: { receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: msg, update_timestamp: Math.floor(t / 1000), create_timestamp: Math.floor(t / 1000), status: 'Paid', is_shipped: false, transactions: txs } });
  });
  // events: two cancellations a few hours after the order, one personalization changed an hour later
  const events = [];
  const cands = orders.filter((o, i) => i > BACKLOG + 5 && i < orders.length - 10);
  for (let k = 0; k < 2 && cands.length; k++) { const o = cands.splice(Math.floor(R() * cands.length), 1)[0]; events.push({ at: o.at + (2 + Math.floor(R() * 4)) * 3600000, kind: 'cancel', rid: o.receipt.receipt_id }); }
  // an order of the first pull is cancelled while its sheet is filling
  if (BACKLOG) { const o = orders[Math.floor(BACKLOG * 0.8)]; events.push({ at: t0 + 3 * 3600000, kind: 'cancel', rid: o.receipt.receipt_id }); }
  const personal = orders.filter(o => o.receipt.transactions.some(t => t._personal) && o.at >= t0 && o.at < t0 + (HOURS - 3) * 3600000);
  if (personal.length) { const o = personal[Math.floor(R() * personal.length)]; events.push({ at: o.at + 3600000, kind: 'change', rid: o.receipt.receipt_id }); }
  events.sort((a, b) => a.at - b.at);
  return { orders, events };
}
const stripPrivate = r => Object.assign({}, r, { transactions: r.transactions.map(({ _metal, _personal, ...t }) => t) });

(async () => {
  if (START_HOUR != null) { let m = Math.ceil(realNow() / 60000) * 60000; while (torontoHour(m) !== START_HOUR || new Date(m).getUTCMinutes() !== 0) m += 60000; simOffset = m - realNow(); }
  const t0 = Date.now();
  const { orders, events } = makeOrders(t0);
  const lines = orders.flatMap(o => o.receipt.transactions);
  log('orders', ORDERS, 'lines', lines.length, 'pieces', lines.reduce((n, t) => n + t.quantity, 0), 'by metal', Object.entries(lines.reduce((m, t) => (m[t._metal] = (m[t._metal] || 0) + t.quantity, m), {})), 'personalized', lines.filter(t => t._personal).length, 'unknown SKU', lines.filter(t => /NOPE/.test(t.sku)).length, 'events', events.map(e => e.kind + ' ' + e.rid));
  const receipts = [];
  const H = await open({ receipts, clock: t0, etsyLimitsOff: ETSY_LIMITS_OFF, masters: [{ count: 20, scale: +process.env.FIXTURE_SCALE || 1 }], settings: { budgetS: +process.env.BUDGET || 30, runMode: 'manual', pullMode: 'all', pollOrders: 'on', pollMinutes: 10, heartbeatS: 5, heartbeatMiss: 3 } });
  log('setup done (browser, pages, master library)');
  let { page } = H; const { st, ctx } = H;
  const frame = () => page.frames().find(f => f.url().startsWith(H.stationOrigin));
  const advance = async ms => { simOffset += ms; await ctx.clock.fastForward(ms); };
  const ticks = [];
  const snapshot = () => page.evaluate(() => {
    const r = B.run, rows = Orders.rows();
    const by = {}; for (const x of rows) by[x.state] = (by[x.state] || 0) + 1;
    const sheets = allSheets().map(sh => ({ metal: sh.metal, status: sh.status, n: sh.placements.length, fill: sh.liveInfo ? Math.round(100 * (sh.liveInfo.placedPt2 || 0) / Math.max(1, sh.liveInfo.usablePt2 || 1)) : null, full: !!sh.releaseFull, set: sh.setId || null, fb: sh.fileBase || null, cut: !!sh.roseCutAt,
      charms: sh.charms.length, rejects: (sh.rejects || []).length, endedBy: sh.endedBy || null, hold: sh.runHold || null, fin: !!sh.intakeFinalized, append: !!sh.appendOnly, phase: sh.intakePhase || null, problem: sh.problem || null, ver: sh.verification ? !!sh.verification.ok : null, run: sh.runId ? sh.runId.slice(-6) : null, closed: window.LiveNest ? LiveNest.closed(sh) : null, density: sh.density != null ? Math.round(sh.density * 100) : null }));
    return { run: r && { id: r.runId, status: r.status, step: r.step, stoppedBy: r.stoppedBy || null, committed: (r.committed || []).length }, rows: by, sheets, engrave: [...Engrave.items().values()].reduce((m, j) => (m[j.state] = (m[j.state] || 0) + 1, m), {}), arrivals: window.Arrivals?.text?.() || '', etsy: DesignLink.state() && DesignLink.state().etsy && DesignLink.state().etsy.meter && { total: DesignLink.state().etsy.meter.total, alarm: DesignLink.state().etsy.meter.alarm, braked: DesignLink.state().etsy.meter.braked } };
  });
  async function idle(maxMs = 8 * 60000) {
    const s0 = realNow(); let calm = 0, s = null;
    while (realNow() - s0 < maxMs) {
      s = await page.evaluate(() => {
        const r = B.run, sheets = allSheets();
        const nesting = sheets.filter(sh => ['nesting', 'queued', 'finishing'].includes(sh.status) || sh._operationStarting || sh.guidancePending || sh.persisted && !sh.persistedDone).length;
        const ops = window.CharmNestOperations && CharmNestOperations.snapshot ? CharmNestOperations.snapshot() : null;
        const busyOps = ops ? (Array.isArray(ops) ? ops.length : (ops.active || []).length + (ops.queued || []).length) : 0;
        return { run: r && r.status, step: r && r.step, busy: !!(r && r.arrivalBusy), pending: !!Arrivals.state().pending, checking: /Checking/.test(window.Arrivals?.text?.() || ''), nesting, busyOps, engraving: [...Engrave.items().values()].filter(j => ['classify', 'fitting', 'ready'].includes(j.state) || j.backSaving).length };
      });
      const quiet = !s.checking && !s.busy && !s.nesting && s.run !== 'running' && !s.busyOps;
      calm = quiet ? calm + 1 : 0;
      if (calm >= 3) return s;
      if (pastDeadline()) return Object.assign({ timeout: true, deadline: true }, s);
      // a check that reads a cold backlog is paced by the station in the page's clock (60 reads a minute, 150 in ten): step that clock
      if (BACKLOG && s.checking) { await advance(1000); await page.waitForTimeout(15); } else await page.waitForTimeout(600);
    }
    return Object.assign({ timeout: true }, s);
  }
  const person = async () => page.evaluate(async () => {
    const out = [];
    for (const j of Engrave.items().values()) {
      try {
        if (j.state === 'words') { await Engrave.decideWords(j, { text: j.text, by: 'Tester' }); out.push('words ' + j.key); }
        else if (j.state === 'review') { await Engrave.approve(j, 'Tester'); out.push('approve ' + j.key + ' ' + j.state); }
      } catch (e) { out.push('error ' + j.key + ' ' + e.message); }
    }
    // a changed order waits in the Review tab until a person accepts the new words, as the banner asks
    for (const it of Review.items().filter(it => it.kind === 'orderChanged')) {
      try { const b = Review.card(it).querySelector('[data-a=accept]'); if (b) { b.click(); out.push('accept change ' + it.row.order.receiptId); } } catch (e) { out.push('error change ' + e.message); }
    }
    return out;
  });

  // ── what the page holds, for the scale checks ──
  let cdp = null;
  const cdpOpen = async () => { try { cdp = await ctx.newCDPSession(page); await cdp.send('Performance.enable'); } catch (e) { cdp = null; } };
  const cdpMetrics = async gc => { if (!cdp) return {}; try { if (gc) await cdp.send('HeapProfiler.collectGarbage'); const { metrics } = await cdp.send('Performance.getMetrics'); const m = Object.fromEntries(metrics.map(x => [x.name, x.value])); return { heapMB: +(m.JSHeapUsedSize / 1048576).toFixed(1), nodes: m.Nodes, listeners: m.JSEventListeners, taskS: +m.TaskDuration.toFixed(2), scriptS: +m.ScriptDuration.toFixed(2), layoutS: +m.LayoutDuration.toFixed(2), styleS: +m.RecalcStyleDuration.toFixed(2) }; } catch (e) { return { error: e.message }; } };
  const storageProbe = () => { let chars = 0; const big = []; for (let i = 0; i < localStorage.length; i++) { const k = localStorage.key(i), n = k.length + (localStorage.getItem(k) || '').length; chars += n; big.push([k.slice(0, 48), n]); } big.sort((a, b) => b[1] - a[1]); return navigator.storage.estimate().then(e => ({ lsKeys: localStorage.length, lsChars: chars, lsTop: big.slice(0, 4), usageMB: +((e.usage || 0) / 1048576).toFixed(2) })).catch(() => ({ lsKeys: localStorage.length, lsChars: chars, lsTop: big.slice(0, 4) })); };
  const idbProbe = async () => { const out = {}; const dbs = indexedDB.databases ? await indexedDB.databases() : []; for (const d of dbs) { try { const db = await new Promise((res, rej) => { const r = indexedDB.open(d.name); r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error); r.onblocked = () => rej(new Error('blocked')); }); const stores = {}; for (const n of [...db.objectStoreNames]) { const all = await new Promise((res, rej) => { const q = db.transaction(n, 'readonly').objectStore(n).getAll(); q.onsuccess = () => res(q.result); q.onerror = () => rej(q.error); }); let bytes = 0; for (const v of all) { try { bytes += JSON.stringify(v, (k, x) => x instanceof ArrayBuffer ? { b: x.byteLength } : ArrayBuffer.isView(x) ? { b: x.byteLength } : x).length; } catch (_) {} } stores[n] = { n: all.length, kb: Math.round(bytes / 1024) }; } db.close(); out[d.name] = stores; } catch (e) { out[d.name] = { error: String(e && e.message || e) }; } } return out; };
  const measure = async (withIdb) => {
    const m = await cdpMetrics(true);
    const sorter = await page.evaluate(storageProbe).catch(e => ({ error: e.message }));
    const fr = frame(), station = fr ? await fr.evaluate(storageProbe).catch(e => ({ error: e.message })) : null;
    const counts = await page.evaluate(() => ({ rows: Orders.rows().length, events: CN.AG.events.length, sheets: allSheets().length, pieces: allSheets().reduce((n, sh) => n + sh.charms.length, 0), engrave: Engrave.items().size })).catch(() => ({}));
    const out = Object.assign({}, m, counts, { sorter, station });
    if (withIdb) { out.idbSorter = await page.evaluate(idbProbe).catch(e => ({ error: e.message })); out.idbStation = fr ? await fr.evaluate(idbProbe).catch(e => ({ error: e.message })) : null; }
    return out;
  };
  // the time one check takes. After a long jump of the clock the page starts its check 10 s later (the wake guard), so the clock is
  // stepped a second at a time until the check has begun and ended
  async function checkDone() {
    const t = realNow(), probe = () => page.evaluate(() => ({ busy: /Checking/.test(Arrivals.text()), last: Arrivals.state().lastCheck, err: Arrivals.state().error })).catch(() => ({}));
    const s0 = await probe(); let c = s0, sawBusy = false, tBusy = 0, cpuA = null;
    for (let i = 0; i < 400 && !pastDeadline(); i++) {
      c = await probe(); if (c.busy && !sawBusy) { sawBusy = true; tBusy = realNow(); cpuA = await cdpMetrics(false); }
      if (!c.busy && (sawBusy || c.last > s0.last)) break;
      if (!sawBusy && i >= 14) break;
      await advance(1000); await page.waitForTimeout(15);
    }
    const cpuB = cpuA && await cdpMetrics(false), busyS = cpuA && cpuB && cpuA.taskS != null && cpuB.taskS >= cpuA.taskS ? +(cpuB.taskS - cpuA.taskS).toFixed(2) : null;
    return { checkMs: realNow() - (tBusy || t), busyS, checked: !!(c.last > s0.last), checkError: c.err || null };
  }
  // the biggest single document each collection holds now: Firestore refuses a document over 1 MiB, the stand-in does not
  const docSizes = () => Object.fromEntries(['Charm_Nest_Runs', 'Charm_Nest_Sheets', 'Charm_Nest_Sets', 'Charm_Pool', 'Charm_Pool_Back', 'Charm_Nest_Arrivals'].map(c => { const docs = st.docs; let mx = 0, id = '', n = 0; for (const [k, v] of docs) if (k.startsWith(c + '/')) { n++; const b = JSON.stringify(v).length; if (b > mx) { mx = b; id = k.slice(c.length + 1); } } return [c, { n, maxKB: Math.round(mx / 1024), id: id.slice(0, 40) }]; }));
  // the facts a crash must not lose: the page's own lines, placed pieces and engraving decisions, and what the cloud holds
  const facts = async () => {
    const pg = await page.evaluate(() => ({ rows: Orders.rows().map(r => r.key + '|' + r.state), placed: allSheets().flatMap(sh => sh.placements.map(p => { const c = sh.charms.find(x => x.id === p.id); return c && c.poolId; }).filter(Boolean)), engr: [...Engrave.items().values()].map(j => j.key + '|' + j.state), run: B.run && B.run.status }));
    return { page: pg, done: st.list('Design_Completed Orders').map(x => x._id), sheetPieces: st.list('Charm_Nest_Sheets').flatMap(sh => sh.poolIds || []), backs: st.list('Charm_Pool_Back').map(b => b.poolId), sets: st.list('Charm_Nest_Sets').map(x => x.setId + (x.committedAt ? ':committed' : '')) };
  };
  // the page dies without warning (renderer crash) and a person opens the sorter again in the same browser
  async function crashAndReopen() {
    const before = await facts();
    const nap = ms => new Promise(r => setTimeout(r, ms));
    try { const c = await ctx.newCDPSession(page); c.send('Page.crash').catch(() => {}); } catch (_) {}
    await nap(600); await Promise.race([page.close({ runBeforeUnload: false }).catch(() => {}), nap(5000)]);
    page = await ctx.newPage();
    page.on('pageerror', e => H.errors.push('sorter pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') H.errors.push('console: ' + m.text().slice(0, 300)); });
    await page.goto(`${H.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.RunCtl && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await cdpOpen();
    const back = await page.evaluate(() => ({ mode: CN.S.settings.runMode, run: B.run && { id: B.run.runId, status: B.run.status, step: B.run.step }, rows: Orders.rows().length }));
    return { before, back };
  }

  let crash = null;
  async function afterCrash() {
    // the page has taken its work up again by itself: what the cloud and the page held before the crash must still be held
    for (let i = 0; i < 60 && !pastDeadline(); i++) { const n = await page.evaluate(() => Orders.rows().length).catch(() => 0); if (n >= crash.before.page.rows.length) break; await page.waitForTimeout(500); }
    crash.after = await facts(); const b = crash.before, a = crash.after, has = new Set(a.page.rows.map(x => x.split('|')[0])), eng = new Map(a.page.engr.map(x => x.split('|')));
    const placedNow = new Set([...a.page.placed, ...a.sheetPieces]);
    crash.lost = { rows: b.page.rows.map(x => x.split('|')[0]).filter(k => !has.has(k)), done: b.done.filter(x => !a.done.includes(x)), pieces: b.sheetPieces.filter(x => !placedNow.has(x)), backs: b.backs.filter(x => !a.backs.includes(x)), sets: b.sets.filter(x => a.sets.includes(x) === false && !(x.endsWith(':committed') ? false : a.sets.includes(x + ':committed'))),
      engraving: b.page.engr.map(x => x.split('|')).filter(([k, v]) => ['approved', 'written'].includes(v) && !['approved', 'written'].includes(eng.get(k))).map(([k, v]) => k + ' ' + v + '→' + eng.get(k)) };
    log('  after the crash, lost:', JSON.stringify(Object.fromEntries(Object.entries(crash.lost).map(([k, v]) => [k, v.length]))), 'rows', b.page.rows.length, '→', a.page.rows.length);
  }
  // the first check of a backlog: the station reads every order under Etsy's limits in the page's clock, which is stepped here
  async function coldPull(expectLines) {
    const t = realNow(), s0 = Date.now(), errs = []; let last = -1, iters = 0, st = {}, step = 300, stoppedAt = 0, resumes = 0;
    if (RUN_MODE === 'manual') await page.evaluate(() => { Arrivals.check(); });   // the run is off: nothing else asks for the pull
    while (!pastDeadline() && iters < 8000) {
      iters++;
      const fr = frame(), r = fr ? await fr.evaluate(() => etsyMeter.total).catch(() => null) : null;
      const progressed = r != null && r !== last; if (r != null) last = r;
      st = await page.evaluate(() => ({ rows: Orders.rows().length, err: Arrivals.state().error, checking: /Checking/.test(Arrivals.text()), run: B.run && { status: B.run.status, step: B.run.step, stoppedBy: B.run.stoppedBy || null } })).catch(() => st);
      // the run's own first pull gives up after 20 minutes (the call's limit); a person presses Resume, as the banner asks
      if (st.run && st.run.status === 'stopped') {
        if (!stoppedAt) { stoppedAt = Date.now(); const m = 'run stopped: ' + st.run.stoppedBy; if (!errs.some(e => e.err === m)) { errs.push({ err: m, simMin: Math.round((Date.now() - s0) / 60000), wallS: Math.round((realNow() - t) / 1000) }); log('  first pull:', m, 'after', Math.round((Date.now() - s0) / 60000), 'simulated min'); } }
        else if (Date.now() - stoppedAt >= 30000) { resumes++; stoppedAt = 0; await page.evaluate(() => { RunCtl.resume && RunCtl.resume().catch(() => {}); }); log('  first pull: Resume pressed, simulated min', Math.round((Date.now() - s0) / 60000)); }
      } else stoppedAt = 0;
      if (st.err && !errs.some(e => e.err === st.err)) { errs.push({ err: st.err, simMin: Math.round((Date.now() - s0) / 60000), wallS: Math.round((realNow() - t) / 1000) }); log('  first pull: check failed after', Math.round((Date.now() - s0) / 60000), 'simulated min:', st.err); }
      if (st.rows >= expectLines && !st.checking) break;
      if (iters % 150 === 0) log(`  first pull: ${st.rows}/${expectLines} lines, ${last} Etsy reads, ${Math.round((Date.now() - s0) / 60000)} simulated min, ${Math.round((realNow() - t) / 1000)} s`);
      // a read takes a few timers and a network answer: short steps while reads go on, longer (under the ping's 4.5 s timeout) while the station waits for room
      if (ETSY_LIMITS_OFF) { await new Promise(r => setTimeout(r, 150)); continue; }   // no pacing to wait out: the page's clock runs by itself
      step = progressed ? 300 : Math.min(3000, step * 2); simOffset += step; await ctx.clock.runFor(step); await new Promise(r => setTimeout(r, 10));
    }
    return { wallMs: realNow() - t, simMin: +((Date.now() - s0) / 60000).toFixed(1), reads: last, rows: st.rows, failures: errs, resumes, iterations: iters };
  }
  // a person works the Engrave queue while the sheets are laid out
  async function workFor(seconds) { const until = realNow() + seconds * 1000; while (realNow() < until && !pastDeadline()) { await person().catch(() => {}); await page.waitForTimeout(2500); } }
  try {
    await cdpOpen();
    // the first pull: every open order is in the Etsy list before the sorter asks
    let oi = 0, ei = 0, tick = 0, cutDone = false;
    while (oi < orders.length && orders[oi].at <= Date.now()) { receipts.push(stripPrivate(orders[oi].receipt)); oi++; }
    if (oi) log('first pull waiting at Etsy:', oi, 'orders');
    const baseline = await measure(true);
    await page.evaluate(m => { CN.setMode('orders'); if (m === 'auto') RunCtl.setMode('auto'); }, RUN_MODE);
    if (BACKLOG) {
      const lines = orders.filter(o => o.at <= Date.now()).reduce((n, o) => n + o.receipt.transactions.length, 0);
      const pull = await coldPull(lines), mem0 = await measure(true), snap0 = await snapshot();
      log('first pull done:', JSON.stringify(pull));
      ticks.push({ tick: 0, at: Date.now(), hour: torontoHour(Date.now()), arrived: [], acted: [], idle: {}, snap: snap0, ms: pull.wallMs, check: { checkMs: pull.wallMs, checked: pull.rows >= lines, checkError: pull.failures.length ? pull.failures.map(f => f.err).join(' | ') : null, simMin: pull.simMin, reads: pull.reads }, docs: docSizes(), busyS: null, mem: mem0, crashNote: null });
      if (CRASH_AFTER_S) { await workFor(CRASH_AFTER_S); crash = await crashAndReopen(); log('  page crashed and reopened', JSON.stringify(crash.back)); await afterCrash(); }
    }
    if (RUN_MODE === 'auto') { if (BACKLOG && DEADLINE_S) await workFor(1e9); else await idle(); }   // (until the wall-clock limit, a person keeps approving)
    const end = t0 + HOURS * 3600000 + 2 * TICK;
    while (Date.now() < end && !pastDeadline(22)) {   // (a check and what follows it take up to 20 s; the outcome is written after the limit)
      const now = Date.now();
      // Etsy changes since the last check
      const arrived = [];
      while (oi < orders.length && orders[oi].at <= now) { receipts.push(stripPrivate(orders[oi].receipt)); arrived.push(orders[oi].receipt.receipt_id); oi++; }
      while (ei < events.length && events[ei].at <= now) {
        const e = events[ei++], i = receipts.findIndex(r => r.receipt_id === e.rid);
        if (i < 0) continue;
        if (e.kind === 'cancel') { receipts.splice(i, 1); log('  event cancel', e.rid); }
        if (e.kind === 'change') { const r = receipts[i]; const t = r.transactions.find(t => t.is_personalized); const v = t.variations.find(v => v.formatted_name === 'Personalization'); v.formatted_value = 'ROSE'; r.update_timestamp = Math.floor(now / 1000); log('  event change', e.rid, '→ ROSE'); }
      }
      const tReal = realNow(), cpu0 = await cdpMetrics(false);
      await advance(TICK); tick++;
      if (RUN_MODE === 'manual') await page.evaluate(() => { Arrivals.check(); });   // (what the "check now" chip does)
      const hour = torontoHour(Date.now());
      let crashNote = null;
      if (hour === CRASH_HOUR && !crash && !CRASH_AFTER_S) {
        await page.waitForTimeout(1500);   // the check and the nesting it started are under way
        crash = await crashAndReopen(); crashNote = 'page crashed and reopened at tick ' + tick;
        log('  ' + crashNote, JSON.stringify(crash.back));
      }
      const chk = await checkDone();
      const s1 = await idle();
      let acted = [];
      if (crash && !crash.after) await afterCrash();
      if (hour >= 7 && hour < 23) { acted = await person(); if (acted.length) await idle(); }
      // the person cuts the rose sheet in the evening once a set carried it, and records the cut
      if (hour === 21 && !cutDone) {
        const cut = await page.evaluate(async () => { const out = []; for (const sh of allSheets().filter(p => p.metal === 'rose' && p.rosePlan && !p.roseCutAt && p.setId && Sets.ofRun(p.runId).some(s => s.setId === p.setId && s.committedAt))) { try { await RoseStock.record(sh); out.push('cut ' + sh.fileBase + ' ' + !!sh.roseCutAt); } catch (e) { out.push('cut failed ' + sh.fileBase + ': ' + e.message); } } return out; });
        if (cut.length) { cutDone = true; acted.push(...cut); await idle(); }
      }
      const snap = await snapshot();
      const cpu1 = await cdpMetrics(false), mem = await measure(tick === 1 || tick % 8 === 0);
      ticks.push({ tick, at: Date.now(), hour, arrived, acted, idle: s1, snap, ms: realNow() - tReal, check: chk, docs: docSizes(), busyS: cpu1.taskS != null && cpu0.taskS != null && cpu1.taskS >= cpu0.taskS ? +(cpu1.taskS - cpu0.taskS).toFixed(2) : null, mem, crashNote });
      const sheetLine = snap.sheets.map(x => `${x.metal[0].toUpperCase()}${x.metal.slice(1, 3)}:${x.status[0]}${x.n}${x.charms !== x.n ? '(' + x.charms + ')' : ''}${x.fill != null ? '/' + x.fill + '%' : ''}${x.full ? 'F' : ''}${x.set ? '@' + x.set.split('-').pop() : ''}${x.closed ? ' CLOSED' + (x.hold ? '[hold:' + x.hold + ']' : '') + (x.fin ? '[fin]' : '') : ''}${x.rejects ? ' rej' + x.rejects + '/' + x.endedBy : ''}`).join(' ');
      log(`t${tick} ${String(hour).padStart(2, '0')}h ${Math.round((realNow() - tReal) / 1000)}s check=${Math.round(chk.checkMs)}ms${chk.checked ? '' : ' (none)'}${chk.checkError ? ' ERR ' + chk.checkError : ''} busy=${cpu1.taskS != null && cpu0.taskS != null ? (cpu1.taskS - cpu0.taskS).toFixed(1) : '?'}s heap=${mem.heapMB}MB ls=${mem.sorter && mem.sorter.lsChars}+${mem.station && mem.station.lsChars} +${arrived.length} run=${snap.run && snap.run.status}/${snap.run && snap.run.step}${snap.run && snap.run.stoppedBy ? ' STOP:' + snap.run.stoppedBy : ''} rows=${JSON.stringify(snap.rows)} eng=${JSON.stringify(snap.engrave)} ${acted.length ? 'acted=' + acted.length : ''} ${s1.timeout ? 'IDLE-TIMEOUT ' + JSON.stringify(s1) : ''}\n     ${sheetLine}\n     ${snap.arrivals}${snap.etsy && snap.etsy.alarm ? ' ETSY ALARM ' + JSON.stringify(snap.etsy.alarm) : ''}`);
      if (snap.run && snap.run.status === 'stopped') {
        const ag = await page.evaluate(() => CN.AG.events.slice(-15).map(e => e.kind + ' | ' + e.text));
        log('  RUN STOPPED', snap.run.stoppedBy, '\n   ' + ag.join('\n   '));
        // a person presses Resume, as the banner asks
        await page.evaluate(() => RunCtl.resume && RunCtl.resume().catch(() => {}));
      }
    }
    if (BACKLOG) { const snapE = await snapshot(), memE = await measure(true); ticks.push({ tick: tick + 1, at: Date.now(), hour: torontoHour(Date.now()), arrived: [], acted: [], idle: {}, snap: snapE, ms: 0, check: null, docs: docSizes(), busyS: null, mem: memE, crashNote: 'end of the run' }); }
    // ── what a person sees at the end of the day ──
    const shotDir = OUT.replace(/\.json$/, '-shots'); fs.mkdirSync(shotDir, { recursive: true });
    for (const mode of process.env.NO_SHOTS ? [] : ['orders', 'nest', 'engrave', 'review', 'library', 'design']) {
      try { await page.evaluate(m => CN.setMode(m), mode); await page.waitForTimeout(mode === 'library' ? 4000 : 1200); await page.screenshot({ path: path.join(shotDir, mode + '.png') }); await page.screenshot({ path: path.join(shotDir, mode + '-full.png'), fullPage: true }); } catch (e) { log('shot', mode, e.message); }
    }
    // ── the outcome ──
    const sorter = await page.evaluate(() => ({
      rows: Orders.rows().map(r => ({ rid: String(r.order.receiptId), key: r.key, state: r.state, reason: r.reason, material: r.material || (r.spec && r.spec.material), sku: r.spec && r.spec.designSku, qty: r.spec && r.spec.quantity, poolIds: r.poolIds, engrave: r.engrave && { state: r.engrave.state, text: r.engrave.text, needed: r.engrave.needed } })),
      run: B.run && { runId: B.run.runId, status: B.run.status, committed: B.run.committed, holds: B.run.holds, refused: B.run.refused },
      sheets: allSheets().map(sh => ({ sheetId: sh.sheetId, metal: sh.metal, status: sh.status, setId: sh.setId, fileBase: sh.fileBase, full: !!sh.releaseFull, draft: !!sh.draft, runId: sh.runId, placements: sh.placements.map(p => { const c = sh.charms.find(x => x.id === p.id); return c && c.poolId; }).filter(Boolean), roseCutAt: sh.roseCutAt || null, rosePlan: !!sh.rosePlan, lines: sh.rosePlan && sh.rosePlan.stages ? sh.rosePlan.stages.length : 0, textCharms: sh.charms.filter(c => sh.placements.some(p => p.id === c.id) && (c.members || []).some(m => m && m.kind === 'text')).map(c => c.sku || c.name) })),
      events: CN.AG.events.map(e => (e.kind || '') + ' | ' + e.text),
      arrivals: Arrivals.state(),
    }));
    const server = {};
    for (const c of ['Charm_Nest_Sets', 'Charm_Nest_Sheets', 'Charm_Pool', 'Charm_Pool_Back', 'Design_Completed Orders', 'Charm_Nest_Arrivals', 'Charm_Nest_Runs', 'Charm_Nest_Rose_Stock', 'Charm_Nest_Counters', 'Design_RealTime_Selected_Orders']) server[c] = st.list(c);
    fs.writeFileSync(OUT, JSON.stringify({ t0, orders: orders.map(o => ({ at: o.at, rid: String(o.receipt.receipt_id), lines: o.receipt.transactions.map(t => ({ tx: String(t.transaction_id), sku: t.sku, metal: t._metal, qty: t.quantity, personal: t._personal })) })), events, ticks, sorter, server, errors: H.errors, stationWarn: H.consoleLog.filter(l => /hydrate|brake|429|watchdog|timed out|did not answer/i.test(l)).slice(0, 12), baseline, crash: crash && { back: crash.back, lost: crash.lost || null, before: { rows: crash.before.page.rows.length, done: crash.before.done.length, pieces: crash.before.sheetPieces.length, backs: crash.before.backs.length, sets: crash.before.sets }, after: crash.after && { rows: crash.after.page.rows.length, done: crash.after.done.length } }, endedBy: pastDeadline() ? 'deadline' : 'day', wallS: Math.round((realNow() - WALL0) / 1000), config: { BACKLOG, ORDERS, HOURS, TICK_MIN, START_HOUR, CRASH_HOUR, DEADLINE_S, BUDGET: +process.env.BUDGET || 30 } }, null, 1));
    log('wrote', OUT, 'errors', H.errors.length);
  } finally { await H.close(); }
})().catch(e => { log('FAILED', e && e.stack || e); process.exit(1); });
