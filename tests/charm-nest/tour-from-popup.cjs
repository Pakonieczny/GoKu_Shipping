// Send to Sheet from a window (Paul, 29 Sep 01:30: "half the animation is not visible because it's blocked by the pop-up
// and it doesn't engage smoothly and it looks broken and disjointed"). The order window's Send to Sheet, pressed on each
// of its tabs (Overview, Timeline, Sheet): the window steps out of the way (it shrinks and fades toward its card, never
// closed), the design lifts off the button pressed, and the window comes back exactly as it was: the same order, tab and
// scroll, the text typed in it kept, no second window ever. A click (Timeline) or Esc (Sheet) mid-flight skips, and it
// comes back all the same. Then the other starts, which start from the card itself: a Custom Orders card's Send to
// Sheet, an Unknown SKU card's, and the designs window's (it goes back into its card first). On every frame of every
// flight the design (the coin, or a piece coming down) is hit-tested at its centre with document.elementFromPoint: it
// must be the thing on top, so nothing covers it at any moment. Only transform and opacity animate the window.
// Driven in a real Chromium on the fake site (bridge-server.cjs): nothing live is read or written.
// Screenshots (a frame strip of the Overview send) to SHOTS (default /mnt/project-files/plans/tour-popup).
//   node tests/charm-nest/tour-from-popup.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), fs = require('fs'), os = require('os');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/tour-popup';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = w => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, 20, 10, 0, 20, 20,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, 16, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const order = (rid, n, sku, title) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: rid + '1', listingId: '18000' + rid.slice(-5), sku: sku || 'CUSTOM-N-001-' + rid.slice(-6), title: title || 'Custom Name Necklace, Personalized Gold Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });
const OW = { info: '4175423801', timeline: '4175423802', sheet: '4175423803' }, CARD = '4175423805', DLG = '4175423806', UNKNOWN = '4178100001';
const ORDERS = [order(OW.info, 9), order(OW.timeline, 8), order(OW.sheet, 7), order(CARD, 6), order(DLG, 5), order(UNKNOWN, 4, 'ROSE_77', 'Rose Charm')];

// every frame while on: the design's centre hit-tested, the windows open, the frame's length
function watcher() {
  const W = window.__W = { on: false };
  const A = Element.prototype.animate;
  Element.prototype.animate = function (k, o) {
    if (W.on && this.id === 'orderWin') { const props = new Set(); for (const f of Array.isArray(k) ? k : [k]) for (const p of Object.keys(f || {})) if (!['offset', 'easing', 'composite'].includes(p)) props.add(p); W.props.push(...props); }
    return A.call(this, k, o);
  };
  const loop = () => {
    if (W.on) {
      const t = performance.now(); if (W.last) { W.d.push(t - W.last); W.dt.push([t - W.t0, t - W.last]); } W.last = t;
      for (const n of document.querySelectorAll('#tourLayer .tourPiece, #tourLayer .tourCoin')) {
        const cs = getComputedStyle(n); if (cs.visibility === 'hidden' || +cs.opacity < .05) continue;
        const b = n.getBoundingClientRect(), x = b.left + b.width / 2, y = b.top + b.height / 2;
        if (b.width < 2 || x < 1 || y < 1 || x > innerWidth - 1 || y > innerHeight - 1) continue;
        W.seen++; W.at.add(Math.round(x) + ',' + Math.round(y));
        const h = document.elementFromPoint(x, y);
        if (!h || !n.contains(h)) W.covered.push([Math.round(t - W.t0), n.className, h ? String(h.id || h.className || h.tagName).slice(0, 40) : null, document.querySelector('#tourLayer').parentNode.id || 'body']);
      }
      const open = [...document.querySelectorAll('dialog[open]')].filter(d => d.id !== 'tourTop').map(d => d.id || d.className);
      if (W.pop && (open.length !== 1 || open[0] !== 'orderWin')) W.extra.push([Math.round(t - W.t0), open.join(',')]);
      const g = document.querySelector('#tourLayer .tourCard'), c = document.querySelector('.mdGhost');
      if (g && c && getComputedStyle(g).visibility !== 'hidden' && +getComputedStyle(g).opacity > .05) { W.overCopy++; if (W.oc.length < 4) W.oc.push([Math.round(t - W.t0), g.getAnimations().map(a => a.playState).join('/'), getComputedStyle(g).opacity, performance.getEntriesByType('mark').filter(m => /^tour:/.test(m.name) && m.startTime > W.t0).map(m => m.name + '@' + Math.round(m.startTime - W.t0)).join(' ')]); }
    }
    requestAnimationFrame(loop);
  };
  requestAnimationFrame(loop);
  window.__on = pop => Object.assign(W, { on: true, pop, last: 0, d: [], dt: [], seen: 0, at: new Set(), covered: [], extra: [], overCopy: 0, oc: [], props: [], t0: performance.now() });
  window.__off = () => { W.on = false; const marks = {}; for (const m of performance.getEntriesByType('mark')) if (/^tour:/.test(m.name) && m.startTime > W.t0) marks[m.name.slice(5)] = m.startTime - W.t0; const at = (from, ms) => from == null ? null : W.dt.filter(([x]) => x > from && x <= from + ms).map(([, v]) => Math.round(v)); return { phases: { tuck: at(marks.tuck, 520), back: at(marks.back, 600) }, d: W.d, seen: W.seen, at: W.at.size, covered: W.covered, extra: W.extra, overCopy: W.overCopy, oc: W.oc, props: [...new Set(W.props)] }; };
  // the flying design is made hit-testable (only here) so elementFromPoint answers whether anything is over it
  addEventListener('DOMContentLoaded', () => { const s = document.createElement('style'); s.textContent = '#tourLayer .tourCoin,#tourLayer .tourPiece{pointer-events:auto!important}'; document.head.appendChild(s); });
}
// the order window as it stands: its order, tab, the scroll of everything scrolled in it, the text typed, how it is drawn
const STATE = () => {
  const d = document.getElementById('orderWin'), cs = getComputedStyle(d), scroll = {};
  for (const n of d.querySelectorAll('*')) if (n.scrollTop) { let k = String(n.id || n.className || n.tagName), i = 1; while (scroll[k + (i > 1 ? '#' + i : '')] != null) i++; scroll[k + (i > 1 ? '#' + i : '')] = n.scrollTop; }
  return { open: d.open, modal: d.matches(':modal'), key: OrderWin.key(), view: OrderWin.view(), scroll, note: document.getElementById('owNote').value, input: document.getElementById('owInput').value,
    drawn: { vis: cs.visibility, op: cs.opacity, tf: cs.transform, anims: d.getAnimations().filter(a => a.playState === 'running').length }, layer: (document.getElementById('tourLayer') || { parentNode: { id: 'none' } }).parentNode.id || 'body', top: !!document.querySelector('#tourTop[open]'), mode: CN.S.mode };
};

(async () => {
  const { start } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], check = (ok, what, hard = true) => { if (!ok && hard) fails.push(what); console.log((ok ? '  ok   ' : hard ? '  FAIL ' : '  warn ') + what); };
  const quiet = os.loadavg()[0] / os.cpus().length < 1.2;
  fs.mkdirSync(SHOTS, { recursive: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 660 } });
  try {
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    await context.addInitScript(watcher);
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(20000);
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.SendTour && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      const day = new Date().toISOString().slice(0, 10);
      B.run = { runId: `run-${day}-popup`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, ORDERS);
    const card = rid => `#rvList .reviewListRow[data-rid="${rid}"]`;
    const tab = k => page.click(`#reviewView .egTab[data-k="${k}"]`);
    // a design dropped on the order's card, put on Gold in the designs window
    const design = async (rid, name) => {
      await page.waitForSelector(card(rid));
      await page.evaluate(({ sel, text, name }) => { const dt = new DataTransfer(); dt.items.add(new File([text], name)); const n = document.querySelector(sel); n.scrollIntoView({ block: 'center' }); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); }, { sel: card(rid), text: DXF(18), name });
      await page.waitForFunction(() => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === 1, null, { timeout: 30000 });
      await page.click('#cuDlg .cuFile .cuM[data-m="gold"]');
      await page.waitForTimeout(500);
    };
    const later = async () => { await page.click('#cuDlg [data-later]'); await page.waitForFunction(() => !document.querySelector('#cuDlg').open); await page.waitForTimeout(450); };
    const sentOf = rid => page.evaluate(k => { const r = B.orders.byKey.get(k); return { sent: !!CustomSheet.sentOf(r), state: r.state }; }, `${rid}_${rid}1`);
    const done = async () => { await page.waitForFunction(() => !SendTour.playing(), null, { timeout: 25000, polling: 100 }); await page.waitForTimeout(250); };
    const report = (name, w) => {
      const worst = Math.round(Math.max(0, ...w.d)), over = w.d.filter(v => v > 34).length;
      console.log(`  ${name}: design hit-tested on ${w.seen} frames at ${w.at} places; covered ${w.covered.length}${w.covered.length ? ' ' + JSON.stringify(w.covered.slice(0, 6)) : ''}; frames worst ${worst} ms, >34 ms ${over}/${w.d.length}`);
      check(w.seen >= 20 && w.at >= 15, `${name}: the design is seen flying (${w.seen} frames, ${w.at} places)`);
      check(!w.covered.length, `${name}: nothing covers the design on any frame (${w.covered.length} covered)`);
      if (w.phases && (w.phases.tuck || w.phases.back)) console.log(`      frames while the window steps aside: ${JSON.stringify(w.phases.tuck)}; while it comes back: ${JSON.stringify(w.phases.back)}`);
      check(over <= w.d.length / 10, `${name}: smooth, worst frame ${worst} ms, >34 ms ${over}/${w.d.length}`, false);   // (measured once: a warning only)
    };

    // designs on every order first, each window closed with Send later
    await tab('customOrder');
    for (const rid of [OW.info, OW.timeline, OW.sheet, CARD, DLG]) { await design(rid, `lion-${rid.slice(-2)}.dxf`); await later(); }
    await tab('unmatchedSku');
    await design(UNKNOWN, 'rose.dxf'); await later();
    await tab('customOrder');

    // ── 1-3 · the order window's Send to Sheet on each of its tabs ──
    for (const [view, rid] of Object.entries(OW)) {
      await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), card(rid));
      await page.waitForTimeout(250);
      const RV = () => page.evaluate(() => { const e = document.querySelector('#reviewView .egPane.scroll'); return e ? [Math.round(e.scrollTop), Math.round(e.scrollHeight - e.clientHeight)] : [null, null]; });
      const rvScroll = await RV();
      await page.click(`${card(rid)} .purchaseSummary`);
      await page.waitForFunction(() => OrderWin.isOpen());
      await page.waitForTimeout(900);
      if (view !== 'info') { await page.click(`#orderWin [data-ow-view="${view}"]`); await page.waitForTimeout(900); }
      // text typed where it can be (the Team message box, and the order's note on the Overview); a scroll in the view
      const typed = `kept for ${rid}`;
      if (await page.isVisible('#owInput')) { await page.click('#owInput'); await page.keyboard.type(typed); } else await page.evaluate(v => { document.getElementById('owInput').value = v; }, typed);
      if (view === 'info') { await page.click('#owNote'); await page.keyboard.type('note ' + rid); }
      const scrolled = await page.evaluate(() => { const v = document.querySelector('#orderWin .owView:not([hidden])'), out = []; for (const n of [v, ...v.querySelectorAll('*')]) { const cs = getComputedStyle(n); if (/(auto|scroll)/.test(cs.overflowY) && n.scrollHeight > n.clientHeight + 30 && n.getClientRects().length) { n.scrollTop = Math.round((n.scrollHeight - n.clientHeight) / 2); out.push((n.id || n.className) + ':' + n.scrollTop); } } return out; });
      await page.waitForTimeout(900);   // (the note saves itself)
      const s0 = await page.evaluate(STATE), b = await page.evaluate(() => { const x = document.getElementById('owSendSheet'); return x && !x.hidden ? x.textContent : null; });
      check(b === 'Send to Sheet' && s0.open && s0.modal && s0.view === view && (scrolled.length > 0 || view === 'timeline'), `${view}: the order window is open on ${view}, scrolled (${scrolled.join(' ')}), with Send to Sheet in its header (${b})`);
      if (!b) { await page.click('#owClose'); continue; }
      await page.evaluate(() => __on(true));
      const t0 = Date.now();
      await page.click('#owSendSheet');
      const shots = view === 'info' ? [['1-pressed', 60], ['2-stepping-aside', 260], ['3-lifted', 700]] : [];
      for (const [n, ms] of shots) { await page.waitForTimeout(Math.max(0, ms - (Date.now() - t0))); await page.screenshot({ path: path.join(SHOTS, `${n}.png`) }); }
      await page.waitForFunction(() => CN.S.mode === 'nest', null, { timeout: 12000, polling: 'raf' });
      let how = 'played through';
      if (view === 'info') {
        await page.waitForFunction(() => document.querySelector('#tourLayer .tourPiece'), null, { timeout: 8000, polling: 'raf' }).catch(() => {});
        await page.waitForTimeout(250); await page.screenshot({ path: path.join(SHOTS, '4-landing.png') });
        await page.waitForFunction(() => performance.getEntriesByName('tour:back').length > (window.__backs || 0), null, { timeout: 15000, polling: 'raf' });
        await page.waitForTimeout(230); await page.screenshot({ path: path.join(SHOTS, '5-coming-back.png') });
      } else if (view === 'timeline') { await page.waitForTimeout(500); await page.mouse.click(720, 520); how = 'skipped by a click'; }
      else { await page.waitForTimeout(500); await page.keyboard.press('Escape'); how = 'skipped by Esc'; }
      await done();
      await page.evaluate(() => { window.__backs = performance.getEntriesByName('tour:back').length; });
      if (view === 'info') await page.screenshot({ path: path.join(SHOTS, '6-back-as-it-was.png') });
      const w = await page.evaluate(() => __off()), s1 = await page.evaluate(STATE), sent = await sentOf(rid);
      const rv1 = await RV();
      // the cards above it stay where they are: the scroll moves only by however much shorter the sent card left the
      // list (its Send to Sheet gone), and no further; a list that can no longer scroll that far sits at its foot
      const shrink = Math.max(0, rvScroll[1] - rv1[1]), rvSame = Math.abs(rv1[0] - rvScroll[0]) <= shrink + 2 || (rvScroll[0] > rv1[1] && Math.abs(rv1[0] - rv1[1]) <= 1);
      console.log(`  ${view} (${how}, ${Date.now() - t0} ms): before ${JSON.stringify(s0)}\n      after  ${JSON.stringify(s1)}`);
      report(`${view} (${how})`, w);
      check(!w.extra.length, `${view}: no second window at any moment (${JSON.stringify(w.extra.slice(0, 3))})`);
      check(w.props.every(p => ['transform', 'opacity'].includes(p)), `${view}: the window moves on transform and opacity only (${w.props.join(',')})`);
      check(sent.sent && sent.state === 'pooled', `${view}: sent and placed first (${JSON.stringify(sent)})`);
      check(s1.open && s1.modal && s1.key === s0.key && s1.view === s0.view, `${view}: the same window is back, on the same order and tab (${s1.key}, ${s1.view})`);
      check(JSON.stringify(s1.scroll) === JSON.stringify(s0.scroll), `${view}: every scroll in it where it was (${JSON.stringify(s1.scroll)})`);
      check(s1.input === typed && s1.note === s0.note, `${view}: the text typed is kept ("${s1.input}", note "${s1.note}")`);
      check(s1.drawn.vis === 'visible' && s1.drawn.op === '1' && s1.drawn.tf === 'none' && !s1.drawn.anims, `${view}: drawn whole again, nothing left on it (${JSON.stringify(s1.drawn)})`);
      check(s1.layer === 'body' && !s1.top && s1.mode === 'review' && rvSame, `${view}: the tour's layer back on the page, the Review tab under it as it was (${s1.layer}, ${s1.mode}, scroll ${rvScroll[0]} → ${rv1[0]}${rv1[0] !== rvScroll[0] ? `, the list ${shrink} px shorter without its Send to Sheet` : ''})`);
      check(await page.evaluate(() => { const x = document.getElementById('owSendSheet'); return !x || x.hidden; }), `${view}: its Send to Sheet is gone (the designs are on the sheets)`);
      await page.click('#owClose'); await page.waitForFunction(() => !document.querySelector('#orderWin').open); await page.waitForTimeout(500);
    }

    // ── 4-6 · from the card itself: a Custom Orders card, the designs window (back into its card first), an Unknown SKU card ──
    const fromCard = async (name, rid, press) => {
      await page.evaluate(sel => document.querySelector(sel).scrollIntoView({ block: 'center' }), card(rid));
      await page.waitForTimeout(300);
      if (name === 'the designs window') { await page.click(`${card(rid)} [data-cu-designs]`); await page.waitForFunction(() => document.querySelector('#cuDlg').open); await page.waitForTimeout(900); }
      await page.evaluate(() => __on(false));
      await page.click(press);
      await page.waitForFunction(() => SendTour.playing(), null, { timeout: 12000, polling: 'raf' });
      await done();
      const w = await page.evaluate(() => __off()), sent = await sentOf(rid), mode = await page.evaluate(() => CN.S.mode);
      report(name, w);
      check(!w.overCopy, `${name}: no lifted copy of the card over the window going back into it (${w.overCopy} frames${w.overCopy ? ' ' + JSON.stringify(w.oc) : ''})`);
      check(sent.sent && mode === 'review', `${name}: sent, and home in the Review tab (${JSON.stringify(sent)}, ${mode})`);
    };
    await fromCard('a Custom Orders card', CARD, `${card(CARD)} [data-cu-send]`);
    await fromCard('the designs window', DLG, '#cuDlg [data-send]');
    await tab('unmatchedSku');
    await fromCard('an Unknown SKU card', UNKNOWN, `${card(UNKNOWN)} [data-cu-send]`);
    check(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await context.close(); await browser.close(); await srv.close(); }
  console.log(`  (load per core ${(os.loadavg()[0] / os.cpus().length).toFixed(2)}${quiet ? '' : ', busy: frame timings only warn'})`);
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
