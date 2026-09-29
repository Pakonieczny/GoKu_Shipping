// Adversarial (28 Sep, Paul: "a beautiful and seamless animation"): the order view must fly with its own look, not as a
// blank cream box whose content is drawn once it lands. Opened from an Orders row, from a charm of an order in the pull
// and from a charm of an order outside the pull (read from the records while it grows), each flight is stopped at its
// middle (every animation paused at 325 of its 650 ms) and looked at:
//  · the header (the order number, and the buyer and ship-by when they are known) is inside the view's visible clip,
//    fully opaque and not under the source's tint;
//  · an order still being read shows the Overview's skeleton blocks, inside the clip;
//  · the screenshot of the clip is not blank: it holds text (dark pixels), counted in the page from the PNG.
// Then the flight is let go and must land as it did (network data drawn only once it has landed: adv-orderview-smooth).
// The mid-flight picture of the charm outside the pull is saved to SHOT (default plans/open-order/d-mid.png).
// BASE=<git ref> runs the same flights on that ref's charm-nest-bridge.js and charm-nest-1.html and expects them to fail.
//   node tests/charm-nest/adv-orderview-flight.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, BASE=<ref>)
const path = require('path'), fs = require('fs'), assert = require('assert/strict'), { execFileSync } = require('child_process');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' };               // in the pull (Orders row)
const C = { rid: '4175000123', tid: '41750001231', sku: 'ASTER_FLOWER' };           // outside the pull (a charm on a sheet)
const S = { rid: '4177000001', tid: '41770000011', sku: 'TINY_TAG' };               // in the pull, opened from its charm
const RUN = 'run-adv-flight';
const SHOT = process.env.SHOT || '/mnt/project-files/plans/open-order/d-mid.png';
const line = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: ['Initial: H'], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

function seed(st) {
  st.put('Brites_Orders', C.rid, { 'Staff Note': 'C note' });
  st.put('Charm_Pool', `${C.rid}_${C.tid}_1`, { poolId: `${C.rid}_${C.tid}_1`, orderId: C.rid, transactionId: C.tid, lineKey: `${C.rid}_${C.tid}`, sku: C.sku, material: 'gold', copy: 1, quantity: 1, state: 'committed', runId: RUN, updatedAt: Date.now() });
  st.put('Charm_Nest_Runs', RUN, { runId: RUN, day: '2026-09-20', status: 'complete', step: 'complete', updatedAt: Date.now() - 7 * DAY * 1000,
    lines: { [`${C.rid}_${C.tid}`]: { state: 'committed', poolIds: [`${C.rid}_${C.tid}_1`], sku: C.sku, material: 'gold', quantity: 1, orderId: C.rid, transactionId: C.tid, createTs: SHIP - 12 * DAY, arrivedAt: 0,
      snap: { title: 'Aster birth flower necklace', listingId: '1800004321', metalKey: 'gold', metalLabel: 'GF 14/20', orderNumber: C.rid, buyer: 'Janet Steptoe', shipBy: SHIP - 4 * DAY, isGift: false, vars: ['Metal␟14k Gold Filled'], pers: ['September'] } } } });
}

async function pageOn(browser, srv, over) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
  if (over) for (const [file, body] of Object.entries(over)) await context.route(u => new URL(u.href).pathname === '/' + file, r => r.fulfill({ status: 200, contentType: file.endsWith('.html') ? 'text/html; charset=utf-8' : 'text/javascript', body }));
  await context.addInitScript(() => {
    try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-adv-test')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
    window.prompt = () => 'Test Operator';
  });
  const page = await context.newPage(), errors = [];
  page.setDefaultTimeout(20000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderSearch && CN.S.cloud.ok === true, null, { timeout: 60000 });
  await page.evaluate(async ({ orders }) => {
    await Orders.loadMaps(true);
    for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    Orders.interpretAll(); CN.setMode('orders'); Orders.render();
  }, { orders: [order(A.rid, 'Hannah Whitford', [line(A.tid, A.sku)]), order(S.rid, 'Mia Lund', [line(S.tid, S.sku)])] });
  // the cloud answers late, as it does live: every read lands in the middle of the grow or after it
  await page.route(/\/\.netlify\/functions\/(charmNestLibrary|firebaseOrders)/, async r => { await new Promise(res => setTimeout(res, 250)); return r.continue().catch(() => {}); });
  await page.waitForTimeout(800);
  return { page, errors, context };
}

/** Starts a flight, stops every animation at its middle, and reads what is on screen then. */
async function midFlight(page, open, shot) {
  await page.evaluate(() => {
    window.__mid = new Promise(res => {
      const t0 = performance.now();
      const loop = () => {
        const d = document.getElementById('orderWin'), grow = d && d.getAnimations().find(a => a.effect && a.effect.getKeyframes().some(k => k.clipPath));
        if (grow && grow.currentTime >= 325) { for (const a of document.getAnimations()) try { a.pause(); } catch (_) {} return res(true); }
        if (performance.now() - t0 > 3000) return res(false);
        requestAnimationFrame(loop);
      };
      requestAnimationFrame(loop);
    });
  });
  await open();
  assert(await page.evaluate(() => window.__mid), 'the view grew (its clip animation reached its middle)');
  const dom = await page.evaluate(() => {
    const d = document.getElementById('orderWin'), cs = getComputedStyle(d), M = new DOMMatrix(cs.transform === 'none' ? undefined : cs.transform);
    const ins = (/inset\(([^)]*)\)/.exec(cs.clipPath) || [, '0px'])[1].split('round')[0].trim().split(/\s+/).map(parseFloat);
    const [t, r = t, b = t, l = r] = ins, w = d.offsetWidth, h = d.offsetHeight;
    const p0 = M.transformPoint(new DOMPoint(l, t)), p1 = M.transformPoint(new DOMPoint(w - r, h - b));
    const clip = { left: p0.x, top: p0.y, right: p1.x, bottom: p1.y };
    const opa = n => { let o = 1; for (; n && n !== document.documentElement; n = n.parentElement) o *= +getComputedStyle(n).opacity; return o; };
    const tint = Math.max(0, ...[...d.querySelectorAll('.owTint')].map(n => +getComputedStyle(n).opacity));
    const seen = n => { if (!n || !n.getClientRects().length) return 0; const q = n.getBoundingClientRect(), iw = Math.max(0, Math.min(q.right, clip.right) - Math.max(q.left, clip.left)), ih = Math.max(0, Math.min(q.bottom, clip.bottom) - Math.max(q.top, clip.top)); return q.width * q.height ? iw * ih / (q.width * q.height) : 0; };
    const part = sel => { const n = d.querySelector(sel); return n ? { text: n.textContent.trim(), inClip: +seen(n).toFixed(2), opacity: +opa(n).toFixed(2) } : null; };
    const sk = [...d.querySelectorAll('.owVInfo .owSk')].filter(n => n.getClientRects().length);
    return { clip, tint: +tint.toFixed(2), title: part('#owTitle'), sub: part('#owSub'), rail: part('#owRail'), tabs: part('.owTabsV'),
      skel: sk.length, skelIn: sk.filter(n => seen(n) > .5 && opa(n) > .9).length, loading: !!(d.querySelector('#owLoading') && !d.querySelector('#owLoading').hidden) };
  });
  const png = await page.screenshot({ type: 'png' });
  if (shot) fs.writeFileSync(shot, png);
  // text in the clip: pixels much darker than the cream surface, counted in the page from the PNG
  dom.ink = await page.evaluate(async ({ b64, c }) => {
    const img = await createImageBitmap(await (await fetch('data:image/png;base64,' + b64)).blob());
    const cv = new OffscreenCanvas(img.width, img.height), g = cv.getContext('2d'); g.drawImage(img, 0, 0);
    const x0 = Math.max(0, Math.ceil(c.left)), y0 = Math.max(0, Math.ceil(c.top)), x1 = Math.min(img.width, Math.floor(c.right)), y1 = Math.min(img.height, Math.floor(c.bottom));
    if (x1 - x0 < 2 || y1 - y0 < 2) return 0;
    const px = g.getImageData(x0, y0, x1 - x0, y1 - y0).data; let n = 0;
    for (let i = 0; i < px.length; i += 4) if (0.3 * px[i] + 0.59 * px[i + 1] + 0.11 * px[i + 2] < 150) n++;
    return n;
  }, { b64: png.toString('base64'), c: dom.clip });
  await page.evaluate(() => { for (const a of document.getAnimations()) try { a.play(); } catch (_) {} });
  return dom;
}

async function flights(page) {
  const out = {};
  const settle = async () => { await page.waitForTimeout(1400); };
  const close = async () => { await page.evaluate(() => OrderWin.close()); await page.waitForTimeout(900); await page.evaluate(() => { CN.setMode('orders'); Orders.render(); }); await page.waitForTimeout(300); };
  // 1 · an Orders row
  const row = `.ocard[data-key="${A.rid}_${A.tid}"],.orderListRow[data-key="${A.rid}_${A.tid}"]`;
  await page.waitForSelector(row);
  out.row = await midFlight(page, () => page.click(row));
  await settle(); out.rowLanded = await page.evaluate(rid => OrderWin.isOpen() && document.getElementById('owTitle').textContent.includes(rid), A.rid);
  await close();
  // 2 · a charm of an order in the pull (a small square on the page, as the sheet window's charm is)
  const charm = await page.evaluate(k => { const n = document.querySelector(`[data-key="${k}"]`); const r = n.getBoundingClientRect(); return { left: r.left + 20, top: r.top + 10, width: 28, height: 28 }; }, `${S.rid}_${S.tid}`);
  out.charm = await midFlight(page, () => page.evaluate(({ rid, charm }) => { OrderWin.openOrder(rid, { from: charm }); }, { rid: S.rid, charm }));
  await settle(); await close();
  // 3 · a charm of an order outside the pull: its records are read while the view grows
  out.outside = await midFlight(page, () => page.evaluate(({ rid, charm }) => { OrderWin.openOrder(rid, { from: charm }); }, { rid: C.rid, charm }), SHOT);
  await page.waitForTimeout(2500);
  out.outsideLanded = await page.evaluate(rid => ({ open: OrderWin.isOpen(), title: document.getElementById('owTitle').textContent.includes(rid), skel: document.querySelectorAll('#orderWin .owVInfo .owSk').length, meta: document.getElementById('owMeta').textContent }), C.rid);
  await close();
  return out;
}

function check(R) {
  const fails = [];
  const need = (ok, what) => { if (!ok) fails.push(what); };
  for (const [k, rid, sub] of [['row', A.rid, 'Hannah Whitford'], ['charm', S.rid, 'Mia Lund'], ['outside', C.rid, null]]) {
    const m = R[k];
    need(m.title && m.title.text.includes(rid), `${k}: the header says order ${rid} mid-flight (${m.title && m.title.text})`);
    need(m.title && m.title.inClip >= .9 && m.title.opacity >= .9, `${k}: the order number is inside the flying view and opaque (in clip ${m.title && m.title.inClip}, opacity ${m.title && m.title.opacity})`);
    if (sub) need(m.sub && m.sub.text.includes(sub) && /ship by/.test(m.sub.text) && m.sub.inClip >= .9 && m.sub.opacity >= .9, `${k}: the buyer and ship-by show under it (${JSON.stringify(m.sub)})`);
    need(m.rail && m.rail.inClip > .5 && m.rail.opacity >= .9, `${k}: the rail's frame flies with it (${JSON.stringify(m.rail)})`);
    need(m.tabs && m.tabs.inClip > .5 && m.tabs.opacity >= .9, `${k}: the tabs fly with it (${JSON.stringify(m.tabs)})`);
    need(m.tint < .5, `${k}: the source's tint has let the view show through by mid-flight (${m.tint})`);
    need(m.ink >= 150, `${k}: the flying view is not blank: ${m.ink} dark pixel(s) of text in its clip`);
  }
  need(R.outside.loading || R.outside.skel > 0, 'outside: the view says the order is being read');
  need(R.outside.skel >= 4 && R.outside.skelIn >= 2, `outside: the Overview's skeleton blocks show where the records are still coming (${R.outside.skel} drawn, ${R.outside.skelIn} in the clip)`);
  return fails;
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const base = process.env.BASE || '';
  const over = base ? Object.fromEntries(['charm-nest-bridge.js', 'charm-nest-1.html'].map(f => [f, execFileSync('git', ['show', `${base}:${f}`], { cwd: root, maxBuffer: 64 << 20 }).toString('utf8')])) : null;
  const srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    if (over) {
      const p = await pageOn(browser, srv, over), R = await flights(p.page); await p.context.close();
      const f = check(R);
      console.log(`  before (${base}): ${f.length} check(s) fail`); for (const x of f) console.log('    ✗ ' + x);
      assert(f.length > 0, 'the old flight fails these checks');
    }
    const p = await pageOn(browser, srv, null), R = await flights(p.page); await p.context.close();
    for (const k of ['row', 'charm', 'outside']) console.log(`  ${k.padEnd(8)} mid-flight: ${JSON.stringify({ title: R[k].title, sub: R[k].sub && R[k].sub.text, tint: R[k].tint, ink: R[k].ink, skel: R[k].skel, skelIn: R[k].skelIn })}`);
    const f = check(R);
    assert.deepEqual(f, [], 'the view flies with its own look');
    assert(R.rowLanded, 'the row\'s order is open once it lands');
    assert(R.outsideLanded.open && R.outsideLanded.title && R.outsideLanded.skel === 0 && /Janet Steptoe/.test(R.outsideLanded.meta), `the order outside the pull lands with its records drawn and no skeleton left (${JSON.stringify(R.outsideLanded)})`);
    assert.deepEqual(p.errors, [], 'no page errors');
    console.log(`  ✓ header, rail, tabs and skeleton fly with the view; not blank mid-flight · picture: ${SHOT}`);
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('adv-orderview-flight: ok'), e => { console.error(e); process.exit(1); });
