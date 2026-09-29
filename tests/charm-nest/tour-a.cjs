// The Send to Sheet tour (charm-nest-tour.js, Paul 29 Sep 00:25): a custom order sent from its Review card is seen
// leaving the list for the Nest tab, landing on its sheet, and coming back home to the Review tab as it was left.
// Driven in a real Chromium on the fake site (bridge-server.cjs), with a small stand-in of part B's NestFocus:
//   1 · the designs window's Send to Sheet, with a run under way: the window goes back into its card, the tour switches
//       to the Nest tab, opens the Gold sheet through NestFocus.open(sheetId, { poolIds, caption }), lands the piece
//       (land), eases it back (close), and goes home: the Review tab, Custom Orders, the same scroll; nothing left over;
//       only transform, opacity and clip-path animated; about 3-5 s; frames measured once (a warning only);
//   2 · the card's own Send to Sheet with no run open: the design waits (NestFocus.waiting), and Esc mid-tour skips
//       to the end and goes home at once.
// Screenshots to SHOTS (default /mnt/project-files/plans/tour/a-*.png).
//   node tests/charm-nest/tour-a.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), fs = require('fs'), os = require('os');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/tour', TAG = process.env.NF === '0' ? 'fallback-' : process.env.STANDIN ? 'standin-' : 'real-';
const LAYOUT = /^(top|left|right|bottom|width|height|maxHeight|minHeight|maxWidth|minWidth|margin.*|padding.*|border.*Width|inset|flex.*|gap)$/;
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, n) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Gold Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
const ORDERS = [order('4175423829', 5), order('4175423830', 4)];

function recorders() {
  const M = window.__M = { rec: false };
  const A = Element.prototype.animate;
  Element.prototype.animate = function (k, o) {
    if (M.rec) {
      const props = new Set(); for (const f of Array.isArray(k) ? k : k ? [k] : []) for (const p of Object.keys(f)) if (!['offset', 'easing', 'composite'].includes(p)) props.add(p);
      const cls = typeof this.className === 'string' ? this.className : (this.getAttribute && this.getAttribute('class')) || this.tagName;
      M.anims.push({ cls: String(cls).slice(0, 30), props: [...props] });
    }
    return A.call(this, k, o);
  };
  try { new PerformanceObserver(l => { if (M.rec) for (const e of l.getEntries()) M.long.push([Math.round(e.startTime - M.t0), Math.round(e.duration)]); }).observe({ entryTypes: ['longtask'] }); } catch (_) {}
  const loop = t => { if (M.rec) { M.frames.push(t - M.t0); const m = window.CN && CN.S.mode; if (m !== M.mode) { M.modes.push([m, Math.round(t - M.t0)]); M.mode = m; } const c = document.querySelector('#tourLayer .tourCoin'); if (c) M.coin.add(Math.round(c.getBoundingClientRect().left) + ',' + Math.round(c.getBoundingClientRect().top)); } requestAnimationFrame(loop); };
  requestAnimationFrame(loop);
  window.__start = () => Object.assign(M, { rec: true, frames: [], long: [], anims: [], modes: [], mode: null, coin: new Set(), t0: performance.now() });
  window.__stop = () => { M.rec = false; const d = []; for (let i = 2; i < M.frames.length; i++) d.push(M.frames[i] - M.frames[i - 1]); return { d, long: M.long, anims: M.anims, modes: M.modes, coin: M.coin.size }; };
}
// part B's NestFocus, as its contract says, reduced to what the tour needs: the Gold card's canvas and a spot on it
function standIn() {
  const calls = window.__NF = [];
  window.NestFocus = {
    open(sheetId, o) {
      calls.push(['open', sheetId, (o.poolIds || []).slice(), o.caption]);
      const card = document.querySelector('.sheetCard[data-m="gold"]'); card.scrollIntoView({ block: 'center' });
      return new Promise(res => setTimeout(() => {
        const cv = card.querySelector('[data-r="canvas"]').getBoundingClientRect();
        res({ card, spots: o.poolIds.map((id, i) => ({ poolId: id, rect: { left: cv.left + cv.width * .3 + i * 40, top: cv.top + cv.height * .35, width: 52, height: 58 }, rot: 20, scale: 0 })),
          land: id => { calls.push(['land', id]); return new Promise(r => setTimeout(r, 100)); },
          close: () => { calls.push(['close']); return new Promise(r => setTimeout(r, 320)); } });
      }, 520));
    },
    waiting(metal) { calls.push(['waiting', metal]); const q = document.querySelector(`.sheetCard[data-m="${metal}"] .shHead`); const r = q.getBoundingClientRect(); return { rect: { left: r.left, top: r.top, width: r.width, height: r.height } }; }
  };
}

(async () => {
  const { start } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], check = (ok, what, hard = true) => { if (!ok && hard) fails.push(what); console.log((ok ? '  ok   ' : hard ? '  FAIL ' : '  warn ') + what); };
  const quiet = os.loadavg()[0] / os.cpus().length < 1.2;
  fs.mkdirSync(SHOTS, { recursive: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  try {
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    await context.addInitScript(recorders);
    // the page's own NestFocus (part B), its calls recorded; STANDIN=1: a small stand-in of its contract instead;
    // NF=0: none, the tour's own fallback (the card spotlit, the middle of its sheet)
    const NF = process.env.NF !== '0';
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(20000);
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.SendTour && CN.S.cloud.ok === true, null, { timeout: 60000 });
    if (!NF) await page.evaluate(() => { window.__NF = []; window.NestFocus = undefined; });
    else if (process.env.STANDIN) await page.evaluate(standIn);
    else await page.evaluate(() => {
      const calls = window.__NF = [], NFo = window.NestFocus; if (!NFo) throw new Error('no NestFocus on the page');
      const open = NFo.open, waiting = NFo.waiting;
      NFo.open = function (id, o) { calls.push(['open', id, (o.poolIds || []).slice(), o.caption]); return Promise.resolve(open.apply(this, arguments)).then(f => f && Object.assign({}, f, { land: pid => { calls.push(['land', pid]); return f.land(pid); }, close: () => { calls.push(['close']); return f.close(); } })); };
      NFo.waiting = function (m) { calls.push(['waiting', m]); return waiting.apply(this, arguments); };
    });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
    }, ORDERS);
    await page.evaluate(() => { const day = new Date().toISOString().slice(0, 10); B.run = { runId: `run-${day}-tour`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] }; });
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    const drop = async rid => {
      const card = `#rvList .reviewListRow[data-rid="${rid}"]`; await page.waitForSelector(card);
      await page.evaluate(({ sel, text }) => { const dt = new DataTransfer(); dt.items.add(new File([text], 'lion.dxf')); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card, text: DXF(18) });
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
      await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
      await page.waitForTimeout(600);
    };

    // ── 1 · from the designs window, with a run under way ──
    await drop('4175423829');
    const scroll0 = await page.evaluate(() => { const s = document.querySelector('#reviewView .egPane.scroll'); return s ? s.scrollTop : null; });
    await page.evaluate(() => { __start(); window.__t0 = performance.now(); document.querySelector('#cuDlg [data-send]').click(); });
    const shot = async (name, ms) => { await page.waitForTimeout(ms); await page.screenshot({ path: path.join(SHOTS, `a-${TAG}${name}.png`) }); };
    await page.waitForFunction(() => SendTour.playing(), null, { timeout: 8000, polling: 'raf' });
    const began = await page.evaluate(() => Math.round(performance.now() - __t0));
    await shot('1-lift', 700);
    await page.waitForFunction(() => CN.S.mode === 'nest', null, { timeout: 8000 });
    await shot('2-nest', 250);
    await page.waitForFunction(() => document.querySelector('#tourLayer .tourPiece'), null, { timeout: 8000 }).catch(() => {});
    await shot('3-landing', 380);
    await page.waitForFunction(() => !SendTour.playing(), null, { timeout: 15000 });
    const took = await page.evaluate(() => Math.round(performance.now() - __t0));
    await shot('4-home', 350);
    const r1 = await page.evaluate(() => __stop());
    const end1 = await page.evaluate(() => { const row = B.orders.byKey.get('4175423829_41754238291'); return { mode: CN.S.mode, chip: !!document.querySelector('#reviewView .egTab[data-k="customOrder"].on'), scroll: document.querySelector('#reviewView .egPane.scroll')?.scrollTop ?? null, state: row.state, gold: allSheets().filter(p => p.metal === 'gold').reduce((n, p) => n + p.charms.filter(c => c.custom).length, 0), left: document.querySelectorAll('#tourLayer > *').length, dlg: document.querySelector('#cuDlg').open, views: [...document.querySelectorAll('#sheets, #reviewView')].map(v => v.getAnimations().length), calls: window.__NF.slice(), poolIds: row.poolIds.slice(), sheetIds: allSheets().filter(p => p.charms.some(c => row.poolIds.includes(c.poolId))).map(p => p.sheetId || `${p.metal}:${p.page}`) }; });
    console.log(`  1 · home after ${took} ms; modes ${JSON.stringify(r1.modes)}; NestFocus ${JSON.stringify(end1.calls)}`);
    const opened = end1.calls.filter(c => c[0] === 'open'), landed = end1.calls.filter(c => c[0] === 'land').map(c => c[1]);
    check(r1.modes.some(m => m[0] === 'nest') && end1.mode === 'review' && end1.chip && end1.scroll === scroll0, `a real tab switch to Nest and back home to Review · Custom Orders at the same scroll (${JSON.stringify({ mode: end1.mode, chip: end1.chip, scroll: [scroll0, end1.scroll] })})`);
    if (NF) check(opened.length === 1 && JSON.stringify(opened[0][2]) === JSON.stringify(end1.poolIds) && (end1.sheetIds.includes(opened[0][1]) || opened[0][1] === 'gold') && /Sheet 1/.test(opened[0][3]), `its sheet opened once through NestFocus.open(sheetId, { poolIds, caption }) (${JSON.stringify(opened[0])})`);
    if (NF) check(landed.length === end1.poolIds.length && end1.poolIds.every(id => landed.includes(id)) && end1.calls.some(c => c[0] === 'close'), `each piece landed on it (land ${landed.length}/${end1.poolIds.length}) and the sheet eased back (close)`);
    check(end1.state === 'pooled' && end1.gold === 1 && !end1.dlg, `placed first, as before: pooled, 1 piece on Gold, the window closed (${end1.state}, ${end1.gold})`);
    check(r1.coin >= 20, `the coin is seen moving (${r1.coin} positions)`);
    // (the tour starts once the window is back in its card: CustomSheet.send passes that as its delay, 480 ms)
    const tour = took - began - 480;
    check(tour >= 3000 && tour <= 5200, `about 3-5 s for one sheet: ${tour} ms from the window back in its card to home (${took} ms from the press)`);
    check(!end1.left && !end1.views.some(Boolean), `nothing left over: no tour layer nodes, no view animation (${end1.left}, ${end1.views})`);
    const props = [...new Set(r1.anims.flatMap(a => a.props))], layout = props.filter(p => LAYOUT.test(p)), tourProps = [...new Set(r1.anims.filter(a => /^tour/.test(a.cls) || a.cls === 'mGhost tourCard').flatMap(a => a.props))];
    check(tourProps.every(p => ['transform', 'opacity', 'clipPath'].includes(p)) && !layout.length, `only transform, opacity and clip-path animated (tour: ${tourProps.join(',')}; all: ${props.join(',')})`);
    const worst = Math.round(Math.max(0, ...r1.d)), over = r1.d.filter(v => v > 34).length;
    check(over <= r1.d.length / 10, `frames: worst ${worst} ms, >34 ms ${over}/${r1.d.length}, long tasks ${JSON.stringify(r1.long.filter(l => l[1] > 50))}`, false);

    // ── 2 · the card's own button with no run open: it waits for the next run; Esc skips to the end ──
    await page.evaluate(() => { B.run = null; });
    await drop('4175423830');
    await page.click('#cuDlg [data-later]');
    await page.waitForFunction(() => !document.querySelector('#cuDlg').open);
    await page.waitForTimeout(400);
    await page.evaluate(() => { window.__NF.length = 0; __start(); window.__t0 = performance.now(); document.querySelector('#rvList .reviewListRow[data-rid="4175423830"] [data-cu-send]').click(); });
    await page.waitForFunction(() => CN.S.mode === 'nest', null, { timeout: 8000 });
    await shot('5-waiting', 700);
    const esc0 = await page.evaluate(() => performance.now());
    await page.keyboard.press('Escape');
    await page.waitForFunction(() => !SendTour.playing(), null, { timeout: 5000 });
    const escMs = await page.evaluate(t => Math.round(performance.now() - t), esc0);
    const end2 = await page.evaluate(() => { const row = B.orders.byKey.get('4175423830_41754238301'); return { mode: CN.S.mode, state: row.state, sent: !!CustomSheet.sentOf(row), calls: window.__NF.slice(), left: document.querySelectorAll('#tourLayer > *').length }; });
    __stopQuiet = await page.evaluate(() => __stop());
    console.log(`  2 · Esc → home in ${escMs} ms; NestFocus ${JSON.stringify(end2.calls)}`);
    check(end2.sent && end2.state !== 'pooled' && (!NF || end2.calls.some(c => c[0] === 'waiting' && c[1] === 'gold')) && !end2.calls.some(c => c[0] === 'open'), `no run open: it waits for the next run (NestFocus.waiting('gold'), no sheet opened; ${end2.state})`);
    check(end2.mode === 'review' && escMs < 1500 && !end2.left, `Esc skips to the end and goes home (${escMs} ms, ${end2.mode})`);
    check(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await context.close(); await browser.close(); await srv.close(); }
  console.log(`  (load per core ${(os.loadavg()[0] / os.cpus().length).toFixed(2)}${quiet ? '' : ', busy'})`);
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
var __stopQuiet;
