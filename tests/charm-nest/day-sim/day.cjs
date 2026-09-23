// A simulated day of Etsy orders through the whole Charm Sorter process: the fake site (Etsy, Firestore, Storage,
// the model) on two origins, the sorter and the framed Design Station in Chromium on a shared simulated clock, the
// sorter's own 10-minute intake in Auto mode, and a person who approves engravings and accepts changed orders during
// working hours and records the Rose Gold cut in the evening. It takes one to two hours; the outcome is checked by
// analyze.cjs. PW_DIR names a directory holding playwright-core; CHROMIUM names the browser.
//   ORDERS=100 HOURS=24 SEED=7 FIXTURE_SCALE=0.8 node tests/charm-nest/day-sim/day.cjs [out.json]
//   node tests/charm-nest/day-sim/analyze.cjs out.json
const fs = require('fs'), path = require('path'), os = require('os');
const realNow = Date.now.bind(Date);
let simOffset = 0;
Date.now = () => realNow() + simOffset;            // the fake server's clock follows the pages' clock
const { open, repo } = require('./harness.cjs');

const ORDERS = +process.env.ORDERS || 100, HOURS = +process.env.HOURS || 24, SEED = +process.env.SEED || 7, ONLY = process.env.ONLY || null;
const TICK = 10 * 60000;
const OUT = path.resolve(process.argv[2] || path.join(os.tmpdir(), 'charm-sorter-day.json'));
const LOG = OUT.replace(/\.json$/, '.log');
fs.writeFileSync(LOG, '');
const log = (...a) => { const line = a.map(x => typeof x === 'string' ? x : JSON.stringify(x)).join(' '); fs.appendFileSync(LOG, line + '\n'); console.log(line); };

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
  while (times.length < ORDERS) { const t = slots[Math.floor(R() * slots.length)]; if (R() * 9 < HOUR_W[torontoHour(t)]) times.push(t + Math.floor(R() * 60000)); }
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
  const cands = orders.filter((o, i) => i > 5 && i < orders.length - 10);
  for (let k = 0; k < 2 && cands.length; k++) { const o = cands.splice(Math.floor(R() * cands.length), 1)[0]; events.push({ at: o.at + (2 + Math.floor(R() * 4)) * 3600000, kind: 'cancel', rid: o.receipt.receipt_id }); }
  const personal = orders.filter(o => o.receipt.transactions.some(t => t._personal) && o.at < t0 + (HOURS - 3) * 3600000);
  if (personal.length) { const o = personal[Math.floor(R() * personal.length)]; events.push({ at: o.at + 3600000, kind: 'change', rid: o.receipt.receipt_id }); }
  events.sort((a, b) => a.at - b.at);
  return { orders, events };
}
const stripPrivate = r => Object.assign({}, r, { transactions: r.transactions.map(({ _metal, _personal, ...t }) => t) });

(async () => {
  const t0 = Date.now();
  const { orders, events } = makeOrders(t0);
  const lines = orders.flatMap(o => o.receipt.transactions);
  log('orders', ORDERS, 'lines', lines.length, 'pieces', lines.reduce((n, t) => n + t.quantity, 0), 'by metal', Object.entries(lines.reduce((m, t) => (m[t._metal] = (m[t._metal] || 0) + t.quantity, m), {})), 'personalized', lines.filter(t => t._personal).length, 'unknown SKU', lines.filter(t => /NOPE/.test(t.sku)).length, 'events', events.map(e => e.kind + ' ' + e.rid));
  const receipts = [];
  const H = await open({ receipts, clock: t0, masters: [{ count: 20, scale: +process.env.FIXTURE_SCALE || 1 }], settings: { budgetS: +process.env.BUDGET || 30, runMode: 'manual', pullMode: 'all', pollOrders: 'on', pollMinutes: 10, heartbeatS: 5, heartbeatMiss: 3 } });
  const { page, st, ctx } = H;
  const advance = async ms => { simOffset += ms; await ctx.clock.fastForward(ms); };
  const ticks = [];
  const snapshot = () => page.evaluate(() => {
    const r = B.run, rows = Orders.rows();
    const by = {}; for (const x of rows) by[x.state] = (by[x.state] || 0) + 1;
    const sheets = allSheets().map(sh => ({ metal: sh.metal, status: sh.status, n: sh.placements.length, fill: sh.liveInfo ? Math.round(100 * (sh.liveInfo.placedPt2 || 0) / Math.max(1, sh.liveInfo.usablePt2 || 1)) : null, full: !!sh.releaseFull, set: sh.setId || null, fb: sh.fileBase || null, cut: !!sh.roseCutAt,
      charms: sh.charms.length, rejects: (sh.rejects || []).length, endedBy: sh.endedBy || null, hold: sh.runHold || null, fin: !!sh.intakeFinalized, append: !!sh.appendOnly, phase: sh.intakePhase || null, problem: sh.problem || null, ver: sh.verification ? !!sh.verification.ok : null, run: sh.runId ? sh.runId.slice(-6) : null, closed: window.LiveNest ? LiveNest.closed(sh) : null, density: sh.density != null ? Math.round(sh.density * 100) : null }));
    return { run: r && { id: r.runId, status: r.status, step: r.step, stoppedBy: r.stoppedBy || null, committed: (r.committed || []).length }, rows: by, sheets, engrave: [...Engrave.items().values()].reduce((m, j) => (m[j.state] = (m[j.state] || 0) + 1, m), {}), arrivals: (document.getElementById('arrivalCounter') || {}).textContent || '', etsy: DesignLink.state() && DesignLink.state().etsy && DesignLink.state().etsy.meter && { total: DesignLink.state().etsy.meter.total, alarm: DesignLink.state().etsy.meter.alarm, braked: DesignLink.state().etsy.meter.braked } };
  });
  async function idle(maxMs = 8 * 60000) {
    const s0 = realNow(); let calm = 0, s = null;
    while (realNow() - s0 < maxMs) {
      s = await page.evaluate(() => {
        const r = B.run, sheets = allSheets();
        const nesting = sheets.filter(sh => ['nesting', 'queued', 'finishing'].includes(sh.status) || sh._operationStarting || sh.guidancePending || sh.persisted && !sh.persistedDone).length;
        const ops = window.CharmNestOperations && CharmNestOperations.snapshot ? CharmNestOperations.snapshot() : null;
        const busyOps = ops ? (Array.isArray(ops) ? ops.length : (ops.active || []).length + (ops.queued || []).length) : 0;
        return { run: r && r.status, step: r && r.step, busy: !!(r && r.arrivalBusy), pending: !!Arrivals.state().pending, checking: /Checking/.test((document.getElementById('arrivalCounter') || {}).textContent || ''), nesting, busyOps, engraving: [...Engrave.items().values()].filter(j => ['classify', 'fitting', 'ready'].includes(j.state) || j.backSaving).length };
      });
      const quiet = !s.checking && !s.busy && !s.nesting && s.run !== 'running' && !s.busyOps;
      calm = quiet ? calm + 1 : 0;
      if (calm >= 3) return s;
      await page.waitForTimeout(600);
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
  try {
    await page.evaluate(() => { CN.setMode('orders'); RunCtl.setMode('auto'); });
    await idle();
    const end = t0 + HOURS * 3600000 + 2 * TICK;
    let oi = 0, ei = 0, tick = 0, cutDone = false;
    while (Date.now() < end) {
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
      const tReal = realNow();
      await advance(TICK); tick++;
      const hour = torontoHour(Date.now());
      const s1 = await idle();
      let acted = [];
      if (hour >= 7 && hour < 23) { acted = await person(); if (acted.length) await idle(); }
      // the person cuts the rose sheet in the evening once a set carried it, and records the cut
      if (hour === 21 && !cutDone) {
        const cut = await page.evaluate(async () => { const out = []; for (const sh of allSheets().filter(p => p.metal === 'rose' && p.rosePlan && !p.roseCutAt && p.setId && Sets.ofRun(p.runId).some(s => s.setId === p.setId && s.committedAt))) { try { await RoseStock.record(sh); out.push('cut ' + sh.fileBase + ' ' + !!sh.roseCutAt); } catch (e) { out.push('cut failed ' + sh.fileBase + ': ' + e.message); } } return out; });
        if (cut.length) { cutDone = true; acted.push(...cut); await idle(); }
      }
      const snap = await snapshot();
      ticks.push({ tick, at: Date.now(), hour, arrived, acted, idle: s1, snap, ms: realNow() - tReal });
      const sheetLine = snap.sheets.map(x => `${x.metal[0].toUpperCase()}${x.metal.slice(1, 3)}:${x.status[0]}${x.n}${x.charms !== x.n ? '(' + x.charms + ')' : ''}${x.fill != null ? '/' + x.fill + '%' : ''}${x.full ? 'F' : ''}${x.set ? '@' + x.set.split('-').pop() : ''}${x.closed ? ' CLOSED' + (x.hold ? '[hold:' + x.hold + ']' : '') + (x.fin ? '[fin]' : '') : ''}${x.rejects ? ' rej' + x.rejects + '/' + x.endedBy : ''}`).join(' ');
      log(`t${tick} ${String(hour).padStart(2, '0')}h ${Math.round((realNow() - tReal) / 1000)}s +${arrived.length} run=${snap.run && snap.run.status}/${snap.run && snap.run.step}${snap.run && snap.run.stoppedBy ? ' STOP:' + snap.run.stoppedBy : ''} rows=${JSON.stringify(snap.rows)} eng=${JSON.stringify(snap.engrave)} ${acted.length ? 'acted=' + acted.length : ''} ${s1.timeout ? 'IDLE-TIMEOUT ' + JSON.stringify(s1) : ''}\n     ${sheetLine}\n     ${snap.arrivals}${snap.etsy && snap.etsy.alarm ? ' ETSY ALARM ' + JSON.stringify(snap.etsy.alarm) : ''}`);
      if (snap.run && snap.run.status === 'stopped') {
        const ag = await page.evaluate(() => CN.AG.events.slice(-15).map(e => e.kind + ' | ' + e.text));
        log('  RUN STOPPED', snap.run.stoppedBy, '\n   ' + ag.join('\n   '));
        // a person presses Resume, as the banner asks
        await page.evaluate(() => RunCtl.resume && RunCtl.resume().catch(() => {}));
      }
    }
    // ── what a person sees at the end of the day ──
    const shotDir = OUT.replace(/\.json$/, '-shots'); fs.mkdirSync(shotDir, { recursive: true });
    for (const mode of ['orders', 'nest', 'engrave', 'review', 'library', 'design']) {
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
    fs.writeFileSync(OUT, JSON.stringify({ t0, orders: orders.map(o => ({ at: o.at, rid: String(o.receipt.receipt_id), lines: o.receipt.transactions.map(t => ({ tx: String(t.transaction_id), sku: t.sku, metal: t._metal, qty: t.quantity, personal: t._personal })) })), events, ticks, sorter, server, errors: H.errors }, null, 1));
    log('wrote', OUT, 'errors', H.errors.length);
  } finally { await H.close(); }
})().catch(e => { log('FAILED', e && e.stack || e); process.exit(1); });
