// Adversarial (28 Sep, wave 3, area 8): the order view's Overview. The Team tab follows the order on screen while an order
// outside the pull is still read (it kept the thread of the order shown before, under the new order's number); a note
// saved just before the order's record is read again is not put back to the old words by that read answering late; a
// note written at another station shows on every line of the order as it opens; Next slides the next line in, and the
// close shrinks the view back into its row — both seen moving, and smooth.
// Headless Chromium against the fake site (bridge-server.cjs); every request that is not to the loopback is aborted.
//   node tests/charm-nest/adv-overview.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tids: ['41762088411', '41762088412'], sku: 'TINY_TAG' }, B2 = { rid: '4176200172', tids: ['41762001721'], sku: 'LEAF_CHARM' };
const C = { rid: '4175000123', tid: '41750001231', sku: 'ASTER_FLOWER' };   // an order outside the pull
const RUN = 'run-adv-ovw';
const order = (o, buyer, note) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: note || '', messages: [],
  lines: o.tids.map((tid, i) => ({ transactionId: tid, listingId: '1800000' + tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace ' + (i + 1), quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' })) });

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] });
  srv.st.put('Brites_Orders', A.rid, { 'Staff Note': 'Old note' });
  srv.st.put('Charm_Pool', `${C.rid}_${C.tid}_1`, { poolId: `${C.rid}_${C.tid}_1`, orderId: C.rid, transactionId: C.tid, lineKey: `${C.rid}_${C.tid}`, sku: C.sku, material: 'gold', copy: 1, quantity: 1, state: 'committed', runId: RUN, updatedAt: Date.now() });
  srv.st.put('Charm_Nest_Runs', RUN, { runId: RUN, day: '2026-09-20', status: 'complete', step: 'complete', updatedAt: Date.now() - 7 * DAY * 1000,
    lines: { [`${C.rid}_${C.tid}`]: { state: 'committed', poolIds: [`${C.rid}_${C.tid}_1`], sku: C.sku, material: 'gold', quantity: 1, orderId: C.rid, transactionId: C.tid, createTs: SHIP - 12 * DAY, arrivedAt: 0,
      snap: { title: 'Aster birth flower necklace', listingId: '1800004321', metalKey: 'gold', metalLabel: 'GF 14/20', orderNumber: C.rid, buyer: 'Janet Steptoe', shipBy: SHIP - 4 * DAY, isGift: false, vars: ['Metal␟14k Gold Filled'], pers: ['September'] } } } });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = []; const check = (ok, msg) => { console.log((ok ? '  ✓ ' : '  ✗ ') + msg); if (!ok) fails.push(msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      return r.abort();
    });
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, n: 0, engagements: [], active: null, conversation: null }) }));
    // the team's thread: the fake site has none, so it is answered here (A has one message, every other order none)
    await context.route(/\/\.netlify\/functions\/firebaseOrders\?messagesFor=/, r => {
      const ids = decodeURIComponent(/messagesFor=([^&]*)/.exec(r.request().url())[1]).split(',');
      const byOrder = {}; for (const id of ids) byOrder[id] = id === A.rid ? [{ id: 'mA1', senderName: 'Welding', text: 'A-TEAM-MESSAGE about the chain', at: Date.now() - 60000 }] : [];
      return r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ success: true, byOrder }) });
    });
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-adv-test')); localStorage.setItem('cn.mail.tab', JSON.stringify('team')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.CustomerMail && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, [order(A, 'Hannah Whitford', 'Old note'), order(B2, 'Ava Patel')]);
    const a1 = `${A.rid}_${A.tids[0]}`, a2 = `${A.rid}_${A.tids[1]}`;
    const noteIs = (v, ms = 3000) => page.waitForFunction(v => document.getElementById('owNote').value === v, v, { timeout: ms }).then(() => true, () => false);
    const serverNote = rid => (srv.st.doc('Brites_Orders', rid) || {})['Staff Note'];
    const until = async (f, ms = 6000) => { for (const t0 = Date.now(); !f(); ) { if (Date.now() - t0 > ms) return false; await new Promise(r => setTimeout(r, 50)); } return true; };
    const closed = () => page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 4000 });
    const settled = () => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 5000 });
    // frames and long tasks while something moves (rAF deltas; PerformanceObserver 'longtask')
    const watch = () => page.evaluate(() => { const w = window.__w = { d: [], lt: [], on: true }; let t = performance.now(); const f = n => { w.d.push(n - t); t = n; if (w.on) requestAnimationFrame(f); }; requestAnimationFrame(f); try { w.po = new PerformanceObserver(l => l.getEntries().forEach(e => w.lt.push(Math.round(e.duration)))); w.po.observe({ type: 'longtask' }); } catch (_) {} });
    // (frame times mean something only on a machine not busy with other work: on a loaded one they are reported, not judged)
    const busy = require('os').loadavg()[0] > require('os').cpus().length;
    const smooth = (w, what) => { const ok = !w.slow.length && !w.lt.some(x => x > 50), msg = `${what}: smooth (${w.n} frames, max ${w.max} ms, slow ${JSON.stringify(w.slow)}, long tasks ${JSON.stringify(w.lt)})`; if (ok || !busy) check(ok, msg); else console.log(`  – ${msg} — not judged: load ${require('os').loadavg()[0].toFixed(1)} on ${require('os').cpus().length} cores`); };
    const unwatch = () => page.evaluate(() => { const w = window.__w; w.on = false; try { w.po.disconnect(); } catch (_) {} const d = w.d.slice(1); return { n: d.length, max: Math.round(Math.max(0, ...d)), slow: d.filter(x => x > 34).map(Math.round), lt: w.lt }; });

    // 1 · a note written at another station shows on every line of the order as the view opens
    srv.st.put('Brites_Orders', A.rid, { 'Staff Note': 'Welding: chain swapped to 18 in' });
    await page.click(`#ordItems [data-key="${a1}"]`);
    await page.waitForFunction(() => OrderWin.isOpen());
    check(await noteIs('Welding: chain swapped to 18 in'), 'a note written at another station shows as the view opens');
    const both = await page.evaluate(ks => ks.map(k => B.orders.byKey.get(k).spec.staffNote), [a1, a2]);
    check(both.every(n => n === 'Welding: chain swapped to 18 in'), 'and is on every line of the order: ' + JSON.stringify(both));
    await settled();

    // 2 · Next slides the next line in: seen moving, smoothly (transform and opacity only)
    await page.evaluate(() => document.activeElement && document.activeElement.blur());
    await watch();
    const slide = await page.evaluate(async () => {
      const b = document.querySelector('#orderWin .owBody'); document.getElementById('owNext').click();
      const at = [], t0 = performance.now();
      let props = null;
      while (performance.now() - t0 < 900) { await new Promise(r => requestAnimationFrame(r)); const an = b.getAnimations(); if (an.length && !props) props = an.map(a => Object.keys(a.effect.getKeyframes()[0] || {}).filter(k => !['offset', 'easing', 'composite', 'computedOffset'].includes(k))); const cs = getComputedStyle(b); at.push([Math.round(performance.now() - t0), cs.transform, +cs.opacity]); if (!an.length && performance.now() - t0 > 100) break; }
      return { at, props: props || [] };
    });
    const sl = await unwatch();
    const mid = slide.at.filter(([t]) => t > 40 && t < 200);
    const end = slide.at[slide.at.length - 1];
    check(mid.length && mid.some(([, tr, op]) => tr !== 'none' || op < 1) && end[1] === 'none' && end[2] === 1 && end[0] >= 280, `Next to the order's other line: seen sliding in over ${end[0]} ms and lands: ` + JSON.stringify(mid.slice(0, 2)));
    check(slide.props.flat().every(k => k === 'transform' || k === 'opacity'), 'Next: only transform and opacity move: ' + JSON.stringify(slide.props));
    smooth(sl, 'Next');
    await page.waitForFunction(k => OrderWin.key() === k, a2);

    // 3 · a note saved just before the record is read again (typed, then Next to the order's other line): the read,
    //     taken before the save landed and answering after it, does not put the old words back
    await page.click('#owPrev');
    await page.waitForFunction(k => OrderWin.key() === k, a1);
    await page.waitForTimeout(400);
    let release = null; const gate = new Promise(r => { release = r; });
    const stale = JSON.stringify({ success: true, data: { 'Staff Note': 'Welding: chain swapped to 18 in' } });
    await page.route(/\/\.netlify\/functions\/firebaseOrders\?orderId=/, async r => { await gate; return r.fulfill({ status: 200, contentType: 'application/json', body: stale }); });
    await page.fill('#owNote', 'Rush: ships Monday');
    await page.click('#owNext');
    await page.waitForFunction(k => OrderWin.key() === k, a2);
    check(await until(() => serverNote(A.rid) === 'Rush: ships Monday'), 'the note is saved');
    await page.waitForTimeout(300);   // (the save's answer taken in)
    release(); await page.waitForTimeout(400);
    check(await noteIs('Rush: ships Monday', 1500), 'a read taken before the save and answering after it keeps the note just saved: ' + await page.inputValue('#owNote'));
    await page.unroute(/\/\.netlify\/functions\/firebaseOrders\?orderId=/);

    // 4 · the Team tab follows the order on screen: an order outside the pull opened in place (the header search) while
    //     it is still read shows its own thread, never the thread of the order shown before
    await page.waitForFunction(() => /A-TEAM-MESSAGE/.test(document.getElementById('owThread').textContent), null, { timeout: 5000 });
    let hold = null; const slow = new Promise(r => { hold = r; });
    await page.route(/\/\.netlify\/functions\/charmNestLibrary/, async r => { let b = {}; try { b = JSON.parse(r.request().postData() || '{}'); } catch (_) {} if (b.op === 'poolList' && String(b.orderId) === C.rid) await slow; return r.continue(); });
    await page.evaluate(rid => { OrderWin.openOrder(rid, { keepFrom: true, highlight: true }); }, C.rid);
    await page.waitForFunction(rid => OrderWin.key() === rid && !document.getElementById('owLoading').hidden, C.rid);
    await page.waitForTimeout(400);
    const team = await page.evaluate(() => ({ title: document.getElementById('owTitle').textContent, thread: document.getElementById('owThread').textContent.replace(/\s+/g, ' ').trim(), shown: !document.getElementById('owPaneTeam').hidden }));
    check(team.shown && !/A-TEAM-MESSAGE/.test(team.thread), `while order ${C.rid} is read, the Team tab shows its thread, not the last order's: "${team.thread.slice(0, 90)}" under "${team.title}"`);
    hold();
    await page.waitForFunction(() => document.getElementById('owLoading').hidden && /Janet Steptoe/.test(document.getElementById('owSub').textContent), null, { timeout: 15000 });
    check(!/A-TEAM-MESSAGE/.test(await page.evaluate(() => document.getElementById('owThread').textContent)), 'and once it is read');
    await page.unroute(/\/\.netlify\/functions\/charmNestLibrary/);
    await page.click('#owClose'); await closed();

    // 5 · the close: shrinks back into the row it was opened from, seen moving, smoothly, the row answering once it lands
    await page.click(`#ordItems [data-key="${a1}"]`);
    await page.waitForFunction(() => OrderWin.isOpen()); await settled(); await page.waitForTimeout(200);
    const row = await page.evaluate(k => { const r = document.querySelector(`#ordItems [data-key="${k}"]`).getBoundingClientRect(); return { top: Math.round(r.top), bottom: Math.round(r.bottom) }; }, a1);
    await watch();
    const shut = await page.evaluate(async () => {
      const d = document.getElementById('orderWin'); document.getElementById('owClose').click();
      const at = [], t0 = performance.now();
      while (d.open && performance.now() - t0 < 900) { await new Promise(r => requestAnimationFrame(r)); if (d.open) at.push([Math.round(performance.now() - t0), getComputedStyle(d).clipPath]); }
      return { at, open: d.open, ms: Math.round(performance.now() - t0) };
    });
    const cl = await unwatch();
    const tops = shut.at.map(([t, c]) => [t, +((/inset\(([\d.]+)px/.exec(c) || [])[1] || 0)]);
    const midC = tops.filter(([t]) => t > 100 && t < 330).map(([, v]) => v);
    check(!shut.open && shut.ms >= 300 && midC.length && midC.some(v => v > 0 && v < row.top - 2) && tops[tops.length - 1][1] > row.top * 0.8,
      `the close is seen shrinking into the row (row top ${row.top}; clip top over ${shut.ms} ms: ${tops.filter((_, i) => i % 3 === 0).map(x => x.join('ms:')).join(' ')})`);
    smooth(cl, 'the close');

    check(!errors.length, 'no page errors ' + JSON.stringify(errors));
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error(`${fails.length} check(s) failed`); process.exit(1); }
}
main().then(() => console.log('adv-overview OK')).catch(e => { console.error(e); process.exit(1); });
