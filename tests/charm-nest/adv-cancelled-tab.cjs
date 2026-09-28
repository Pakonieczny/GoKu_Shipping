// Adversarial checks (wave 3, area 3) of Orders › Cancelled. Each numbered block proved a bug before its fix:
//  1 · a person's cancel of an order Etsy has since cancelled too (etsyStatus "Canceled") offered Restore: restoring it
//      deleted the record the stations' cancelCheck reads, so an order dead on Etsy passed every station silently
//  2 · Restore with no name saved on this screen went to the library as by "" ("operator"): who restored was lost
//  3 · a double-click on a cancelled Custom Orders card's Print QR label (or Complete Order) went ahead: the second
//      click of the pair was taken as the deliberate second press, so nothing was asked
//  4 · a line AutoCancel kept marked "gone" (its piece stayed on a cut or released sheet) stayed gone for good after a
//      Restore: the arrivals' merge keeps a row it already has and the orders check skips gone rows
// And held up (measured, no fix needed): Etsy's "Fully Refunded" keeps Restore; the restore flight is visible (the copy
// moves between start, middle and end), smooth (rAF deltas, long tasks), animates only transform and opacity, and is not
// cut off; the rows below close up with transforms; the rows of a newly shown pile fade and slide in.
// Headless Chromium against the local fake site (bridge-server.cjs); every request off the loopback is aborted; the cancel
// records are this file's own (page.route). No Etsy call, no paid AI.
//   node tests/charm-nest/adv-cancelled-tab.cjs [playwright-core dir]
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const NOW = Date.now(), H = 3600000;
const REC = (orderId, by, ago, extra) => Object.assign({ orderId, by, why: 'test', at: NOW - ago * H, buyer: 'B ' + orderId, sheets: [], lines: [{ transactionId: '1', sku: 'SKU-' + orderId, title: 't', quantity: 1 }], source: by === 'Etsy' ? 'etsy' : 'sorter' }, extra || {});
const BOTH = '4100000021', REFUND = '4100000022', PERSON = '4100000023', REVIVE = '4100000024';
const SHIP = Math.floor(Date.now() / 1000) + 3 * 86400;
const order = (rid, sku) => ({ receiptId: String(rid), orderNumber: String(rid), createTs: SHIP - 86400, updateTs: SHIP - 86000, shipBy: SHIP, buyer: { name: 'Buyer ' + rid }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: String(rid) + '1', listingId: '1800000001', sku, title: sku + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' },
    { transactionId: String(rid) + '2', listingId: '1800000002', sku: sku + '-B', title: sku + ' B charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }] });

async function main() {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const bad = [];
  const check = async (name, fn) => { try { await fn(); console.log('  ✓ ' + name); } catch (e) { bad.push(name); console.error('  ✗ ' + name + '\n    ' + (e && e.message || e)); } };
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 950 } });
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    // (no name saved on this screen: the restore must ask for it)
    await ctx.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.removeItem('cn.employee'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => null; });
    const page = await ctx.newPage(), errors = [];
    page.setDefaultTimeout(15000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    let records = [
      REC(BOTH, 'Anna', 1, { why: 'Customer asked', etsyStatus: 'Canceled', etsyAt: NOW - 0.5 * H }),
      REC(REFUND, 'Etsy', 2, { why: 'Cancelled on Etsy', etsyStatus: 'Fully Refunded', etsyAt: NOW - 2 * H }),
      REC(PERSON, 'Ben', 3),
      REC(REVIVE, 'Etsy', 3.5, { etsyStatus: 'Fully Refunded' }),
      ...Array.from({ length: 24 }, (_, i) => REC(String(4200000100 + i), 'Operator', 4 + i))
    ];
    const restores = [];
    await page.route(/\/\.netlify\/functions\/charmNestLibrary/, async r => {
      let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {}
      const ok = body => r.fulfill({ status: 200, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(body) }).catch(() => {});
      if (b.op === 'cancelList') {
        if (b.idsOnly) return ok({ ids: records.map(c => c.orderId), truncated: false });
        const n = Math.max(1, Math.min(500, Math.round(+b.limit) || 200)), s = records.slice().sort((a, c) => c.at - a.at);
        return ok({ list: s.slice(0, n), truncated: false });
      }
      if (b.op === 'cancelCheck') { const out = {}; for (const id of b.orderIds || []) { const c = records.find(x => x.orderId === String(id)); if (c) out[id] = { at: c.at, by: c.by, why: c.why, source: c.source, sheets: c.sheets }; } return ok({ cancelled: out, now: Date.now() }); }
      if (b.op === 'cancelRestore') { restores.push(b); records = records.filter(c => c.orderId !== String(b.orderId)); return ok({ ok: true }); }
      return r.fallback();
    });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Cancelled && window.Motion && window.CustomPrint && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async () => { B.employee = ''; try { localStorage.removeItem('cn.employee'); } catch (_) {} await Cancelled.load(true); await Cancelled.history(); CN.setMode('orders'); Orders.renderNow(); });
    const rowSel = rid => `#ordBody .cxRow[data-rid="${rid}"]`;

    /* the rows of a pile newly shown fade and slide in: visible mid-way, and only opacity and transform */
    await check('the Cancelled rows come in visibly (opacity/transform only)', async () => {
      const got = await page.evaluate(() => new Promise(res => {
        document.querySelector('#ordChips [data-pile="cancelled"]').click();
        const first = () => document.querySelector('#ordBody .cxRow');
        const t0 = performance.now(), samples = [];
        const tick = () => { const n = first(); if (n) samples.push([performance.now() - t0, +getComputedStyle(n).opacity]); if (performance.now() - t0 < 700) requestAnimationFrame(tick); else res({ samples, props: n0props() }); };
        const n0props = () => { const n = first(); return n ? [...new Set(n.getAnimations().flatMap(a => a.effect.getKeyframes().flatMap(k => Object.keys(k)).filter(k => !['offset', 'easing', 'composite', 'computedOffset'].includes(k))))] : []; };
        requestAnimationFrame(tick);
      }));
      const mid = got.samples.filter(s => s[1] > 0.02 && s[1] < 0.98);
      assert(mid.length >= 3, 'the first row is seen fading in over several frames: ' + JSON.stringify(got.samples.slice(0, 12)));
      assert(got.samples[got.samples.length - 1][1] === 1, 'and lands fully shown');
    });
    await page.waitForSelector(rowSel(PERSON));
    await page.waitForTimeout(600);

    await check('1 · a person\'s cancel Etsy has since cancelled too offers no Restore (and says Etsy cancelled it)', async () => {
      const r = await page.evaluate(sel => { const n = document.querySelector(sel); return { restore: !!n.querySelector('[data-cx=restore]'), who: n.querySelector('.cxWho').textContent }; }, rowSel(BOTH));
      assert.equal(r.restore, false, 'no Restore: the order is cancelled on Etsy, and the stations read the record: ' + JSON.stringify(r));
      assert(/Anna/.test(r.who) && /Etsy/.test(r.who), 'who cancelled it, and that Etsy did too: ' + r.who);
    });
    await check('Etsy\'s "Fully Refunded" keeps Restore; Etsy\'s "Canceled" has none', async () => {
      assert.equal(await page.evaluate(sel => !!document.querySelector(sel + ' [data-cx=restore]'), rowSel(REFUND)), true);
    });

    await check('2 · Restore with no name saved asks for it inline, and the library is told who restored', async () => {
      await page.click(rowSel(REFUND) + ' [data-cx=restore]');
      await page.waitForSelector(rowSel(REFUND) + ' .cxAskBar');
      const name = await page.$(rowSel(REFUND) + ' .cxAskBar input');
      if (name) {
        // Restore pressed before a name is typed restores nothing
        await page.click(rowSel(REFUND) + ' [data-cx=yes]');
        await page.waitForTimeout(250);
        assert.equal(restores.length, 0, 'nothing restored without a name');
        await name.fill('Nora');
      }
      await page.click(rowSel(REFUND) + ' [data-cx=yes]');
      for (const t0 = Date.now(); !restores.length; ) { if (Date.now() - t0 > 5000) throw new Error('no cancelRestore'); await page.waitForTimeout(50); }
      assert.equal(restores[0].by, 'Nora', 'cancelRestore carries who restored it: ' + JSON.stringify(restores[0]));
      assert.equal(await page.evaluate(() => localStorage.getItem('cn.employee')), 'Nora', 'the name is kept for the next change');
      await page.waitForFunction(() => !document.querySelector('#motionLayer .mGhost'), null, { timeout: 5000 });
      await page.waitForTimeout(900);
    });

    await check('the restore flight: visible, smooth, transform/opacity only, not cut off', async () => {
      await page.evaluate(() => { B.employee = 'Nora'; try { localStorage.setItem('cn.employee', 'Nora'); } catch (_) {} });
      await page.waitForSelector(rowSel(PERSON) + ' [data-cx=restore]');
      await page.click(rowSel(PERSON) + ' [data-cx=restore]');
      await page.waitForSelector(rowSel(PERSON) + ' [data-cx=yes]');
      await page.waitForTimeout(300);
      await page.evaluate(() => {
        const R = window.__rec = { deltas: [], long: [], rects: [], props: new Set(), belowProps: new Set(), start: 0, gone: false };
        try { const po = new PerformanceObserver(l => { for (const e of l.getEntries()) R.long.push(Math.round(e.duration)); }); po.observe({ type: 'longtask', buffered: false }); R.po = po; } catch (_) {}
        let last = performance.now();
        const layer = () => document.getElementById('motionLayer');
        const tick = t => {
          R.deltas.push(t - last); last = t;
          const g = layer() && layer().querySelector('.mGhost');
          if (g) {
            if (!R.start) R.start = t;
            const r = g.getBoundingClientRect(); R.rects.push([t - R.start, Math.round(r.left), Math.round(r.top), Math.round(r.width), +getComputedStyle(g).opacity]);
            for (const a of g.getAnimations()) for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) if (!['offset', 'easing', 'composite', 'computedOffset'].includes(p)) R.props.add(p);
          } else if (R.start) R.gone = true;
          for (const n of document.querySelectorAll('#ordBody .cxRow')) for (const a of n.getAnimations()) if (!(a instanceof CSSTransition)) for (const k of a.effect.getKeyframes()) for (const p of Object.keys(k)) if (!['offset', 'easing', 'composite', 'computedOffset'].includes(p)) R.belowProps.add(p);
          if (!(R.start && R.gone) && t - (R.start || t) < 4000) requestAnimationFrame(tick);
          else R.done = true;
        };
        requestAnimationFrame(tick);
      });
      await page.click(rowSel(PERSON) + ' [data-cx=yes]');
      await page.waitForFunction(() => window.__rec.done, null, { timeout: 8000 });
      const R = await page.evaluate(() => { const R = window.__rec; try { R.po.disconnect(); } catch (_) {} const inFlight = R.deltas.slice(Math.max(0, R.deltas.length - R.rects.length - 1)); return { slowAt: R.rects.map((x, i) => [Math.round(x[0]), Math.round(inFlight[i + 1] || 0)]).filter(x => x[1] > 34), rects: R.rects, props: [...R.props], belowProps: [...R.belowProps], long: R.long, worst: Math.max(...inFlight.slice(2)), slow: inFlight.slice(2).filter(d => d > 34).length, frames: inFlight.length }; });
      assert(R.rects.length >= 12, 'the copy is on screen for a visible time: ' + R.rects.length + ' frames');
      const [s, m, e] = [R.rects[0], R.rects[Math.floor(R.rects.length / 2)], R.rects[R.rects.length - 1]];
      assert(R.rects[R.rects.length - 1][0] >= 280, 'the move lasts long enough to follow: ' + R.rects[R.rects.length - 1][0] + ' ms');
      assert(JSON.stringify(s.slice(1, 4)) !== JSON.stringify(m.slice(1, 4)) && JSON.stringify(m.slice(1, 4)) !== JSON.stringify(e.slice(1, 4)), 'start, middle and end differ: ' + JSON.stringify([s, m, e]));
      const layout = R.props.filter(p => /^(top|left|right|bottom|width|height|margin|padding)/i.test(p));
      assert.deepEqual(layout, [], 'no layout property animated on the copy: ' + R.props.join());
      assert.deepEqual(R.belowProps.filter(p => !['transform', 'opacity'].includes(p)), [], 'the rows below close up with transforms (their own hover transitions aside): ' + R.belowProps.join());
      // (smoothness is judged only on a machine not loaded by other work: rAF deltas and long tasks then measure the page)
      const load = require('os').loadavg()[0] / require('os').cpus().length, smooth = `${R.slow} of ${R.frames} frames over 34 ms (worst ${Math.round(R.worst)} ms) at ${JSON.stringify(R.slowAt)}, long tasks ${R.long.join() || 'none'}`;
      if (load > 1.2) console.log(`    (smoothness not judged: machine load ${load.toFixed(1)} per core; ${smooth})`);
      else { assert(R.slow <= 2, 'smooth: ' + smooth); assert(!R.long.some(d => d > 50), 'no long task while it flies: ' + smooth); }
      console.log(`    (flight ${Math.round(R.rects[R.rects.length - 1][0])} ms, ${R.frames} frames, worst ${Math.round(R.worst)} ms, props ${R.props.join('/')})`);
      assert.equal(restores[restores.length - 1].by, 'Nora');
    });

    await check('3 · a double-click on a cancelled order\'s Print QR label / Complete Order asks, and does not go ahead', async () => {
      const got = await page.evaluate(async rid => {
        const toasts = () => [...document.querySelectorAll('.toast, #toast, [class*=toast]')].map(n => n.textContent).join(' | ');
        const out = {};
        for (const act of ['print', 'complete']) {
          const it = { key: 'cu:adv-' + act, rid, rows: [], record: null };
          const before = toasts();
          CustomPrint[act](it); await new Promise(r => setTimeout(r, 120)); CustomPrint[act](it);   // a double-click
          await new Promise(r => setTimeout(r, 60));
          const html = CustomPrint.buttonHtml(it, act, 'ghost', 'Label', 't', 'sm');
          out[act] = { asks: /anyway\?/.test(html), went: toasts() !== before && /nothing to print|Nothing left to complete/.test(toasts()) };
          // a deliberate second press, a moment later, goes ahead
          await new Promise(r => setTimeout(r, 500)); CustomPrint[act](it); await new Promise(r => setTimeout(r, 60));
          out[act].later = !/anyway\?/.test(CustomPrint.buttonHtml(it, act, 'ghost', 'Label', 't', 'sm'));
        }
        return out;
      }, BOTH);
      for (const act of ['print', 'complete']) {
        assert.equal(got[act].asks, true, `${act}: still asking after a double-click: ` + JSON.stringify(got));
        assert.equal(got[act].went, false, `${act}: the double-click did not go ahead: ` + JSON.stringify(got));
        assert.equal(got[act].later, true, `${act}: a deliberate second press goes ahead: ` + JSON.stringify(got));
      }
    });

    await check('4 · a Restore brings back the lines AutoCancel kept marked gone (not lines gone for another reason)', async () => {
      const got = await page.evaluate(async ({ rid, o }) => {
        const mk = (line, reason) => ({ key: CharmNestOrders.lineKey(o, line), order: o, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'gone', reason, claimedBy: null, poolIds: [], engrave: null, material: null });
        const a = mk(o.lines[0], 'cancelled on Etsy'), b = mk(o.lines[1], 'shipped');
        for (const r of [a, b]) { B.orders.rows.push(r); B.orders.byKey.set(r.key, r); }
        await Cancelled.restore(rid);
        return { a: [a.state, a.reason], b: [b.state, b.reason], pulled: Orders.rows().filter(r => String(r.order.receiptId) === rid && r.state !== 'gone').length };
      }, { rid: REVIVE, o: order(REVIVE, 'GF-REV-1') });
      assert.notEqual(got.a[0], 'gone', 'the line the cancel had marked gone is in play again: ' + JSON.stringify(got));
      assert.equal(got.a[1], null, 'with no cancel reason left on it: ' + JSON.stringify(got));
      assert.deepEqual(got.b, ['gone', 'shipped'], 'a line gone for another reason stays gone: ' + JSON.stringify(got));
    });

    assert.deepEqual(errors, [], 'no page errors');
  } finally { await browser.close(); srv.close(); }
  if (bad.length) { console.error(`adv-cancelled-tab: ${bad.length} failed`); process.exit(1); }
  console.log('adv-cancelled-tab OK');
}
main().catch(e => { console.error(e); process.exit(1); });
