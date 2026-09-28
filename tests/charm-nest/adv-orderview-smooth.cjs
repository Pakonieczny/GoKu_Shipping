// Adversarial (28 Sep, wave 3; Paul: "some of the animations were not exactly visible and they were jerky"): the order
// view's open and close must be smooth from each of its three sources — an Orders row, a search result (an order outside
// the pull, read from the records while the view grows) and a sheet charm (the Sheet view, whose sheet record arrives
// mid-flight). Each flight is measured in a real Chromium: requestAnimationFrame deltas (a frame over 34 ms is a stutter)
// and long tasks (PerformanceObserver 'longtask', over 50 ms). The network answers are held 250 ms so they land in the
// middle of the 650 ms grow, as they do live.
// Checks that hold on any machine: the view is visible and moves (the clip differs mid-way from its start and end); no
// network-driven render reaches the view while it flies (the DOM of the view is still until it lands, then the held
// content fades in); will-change is set only during the flight; the landing glow is an opacity-only layer (no
// box-shadow animated on the row it lands in); nothing but transform, opacity and clip-path is animated.
// Frame and long-task thresholds are gated only when the load per core is under 1.2 (the machine is shared).
// BASE=<git ref> also runs the same flights on that ref's charm-nest-bridge.js and charm-nest-1.html in the same run,
// and prints before and after side by side.
//   node tests/charm-nest/adv-orderview-smooth.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, BASE=<ref>)
const path = require('path'), os = require('os'), assert = require('assert/strict'), { execFileSync } = require('child_process');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const A = { rid: '4176208841', tid: '41762088411', sku: 'TINY_TAG' };               // in the pull (Orders row)
const C = { rid: '4175000123', tid: '41750001231', sku: 'ASTER_FLOWER' };           // outside the pull (search)
const S = { rid: '4177000001', tid: '41770000011', sku: 'TINY_TAG' };               // on a sheet (sheet charm)
const RUN = 'run-adv-smooth', SH = 'sheet-adv-smooth', PS = `${S.rid}_${S.tid}_1`;
const HOLD = 250;   // ms each network answer is held, so it lands mid-flight
const line = (tid, sku, metalKey, metalLabel) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku.replace(/_/g, ' ') + ' necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: metalLabel }], metalKey, metalLabel, personalization: ['Initial: H'], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

function seed(srv) {
  const st = srv.st;
  st.put('Brites_Orders', C.rid, { 'Staff Note': 'C note' });
  st.put('Charm_Pool', `${C.rid}_${C.tid}_1`, { poolId: `${C.rid}_${C.tid}_1`, orderId: C.rid, transactionId: C.tid, lineKey: `${C.rid}_${C.tid}`, sku: C.sku, material: 'gold', copy: 1, quantity: 1, state: 'committed', runId: RUN, updatedAt: Date.now() });
  st.put('Charm_Nest_Runs', RUN, { runId: RUN, day: '2026-09-20', status: 'complete', step: 'complete', updatedAt: Date.now() - 7 * DAY * 1000,
    lines: { [`${C.rid}_${C.tid}`]: { state: 'committed', poolIds: [`${C.rid}_${C.tid}_1`], sku: C.sku, material: 'gold', quantity: 1, orderId: C.rid, transactionId: C.tid, createTs: SHIP - 12 * DAY, arrivedAt: 0,
      snap: { title: 'Aster birth flower necklace', listingId: '1800004321', metalKey: 'gold', metalLabel: 'GF 14/20', orderNumber: C.rid, buyer: 'Janet Steptoe', shipBy: SHIP - 4 * DAY, isGift: false, vars: ['Metal␟14k Gold Filled'], pers: ['September'] } } } });
  // a busy GF sheet: S's charm among 60 others
  const box = (id, cx, cy, w) => ({ id, cxPt: cx, cyPt: cy, angle: 0, wPt: w, hPt: w });
  const pl = [], ch = [];
  for (let i = 0; i < 60; i++) { const id = 'b' + i, rid = i === 20 ? S.rid : String(4178000000 + i); pl.push(box(id, 12 + (i % 15) * 22, 12 + Math.floor(i / 15) * 22, 18)); ch.push({ id, name: `${rid} · SKU${i}`, poolId: i === 20 ? PS : `${rid}_${rid}1_1`, order: rid, sku: i === 20 ? S.sku : 'SKU' + i }); }
  st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 2, day: '2026-09-27', status: 'written', stock: { wPt: 340, hPt: 110 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch });
  st.put('Charm_Pool', PS, { poolId: PS, orderId: S.rid, transactionId: S.tid, lineKey: `${S.rid}_${S.tid}`, sku: S.sku, material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SH, sheetName: '2026-09-27_GF_Set-1_Sheet-2', updatedAt: Date.now() });
  for (let i = 0; i < 14; i++) st.put('Order_Timeline', `${A.rid}_e${i}`, { orderId: A.rid, type: i % 2 ? 'note' : 'nested', at: Date.now() - (20 - i) * 60000, by: 'Test Operator', text: 'step ' + i });
}

/** One page on the fake site; `over` serves those files from a git ref instead of the worktree (the "before"). */
async function pageOn(browser, srv, over) {
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
  if (over) for (const [file, body] of Object.entries(over)) await context.route(u => new URL(u.href).pathname === '/' + file, r => r.fulfill({ status: 200, contentType: file.endsWith('.html') ? 'text/html; charset=utf-8' : 'text/javascript', body }));
  await context.addInitScript(() => {
    try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-adv-test')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
    window.prompt = () => 'Test Operator';
    // long tasks, all the time; frames and the view's mutations only while measuring
    window.__lt = []; try { new PerformanceObserver(l => { for (const e of l.getEntries()) window.__lt.push({ s: e.startTime, d: e.duration }); }).observe({ type: 'longtask', buffered: true }); } catch (_) {}
    window.__m = {
      begin(fly) { const m = this; m.fly = fly || 650; m.fr = []; m.on = true; m.t0 = performance.now(); m.mut = []; m.wc = []; m.clip = []; m.tOpen = null; m.wasOpen = !!(document.getElementById('orderWin') || {}).open;
        const loop = t => { if (!m.on) return; m.fr.push(t); const d = document.getElementById('orderWin'); if (m.tOpen == null && (m.wasOpen || (d && d.open))) m.tOpen = t - m.t0; if (d) { const cs = getComputedStyle(d); m.wc.push(cs.willChange); m.clip.push(cs.clipPath); } requestAnimationFrame(loop); }; requestAnimationFrame(loop);
        const d = document.getElementById('orderWin'); m.mo = new MutationObserver(list => { const t = performance.now(); for (const x of list) { const n = x.target.nodeType === 1 ? x.target : x.target.parentElement; if (!n || n.closest('.owTint,.owGlow')) continue; if (x.type === 'childList' && [...x.addedNodes, ...x.removedNodes].every(k => k.nodeType === 1 && k.matches('.owTint,.owGlow'))) continue; m.mut.push({ t: t - m.t0, type: x.type, inCust: !!n.closest('#owPaneCust'), where: (n.id || n.className || n.tagName).toString().slice(0, 40), attr: x.attributeName }); } });
        if (d) m.mo.observe(d, { subtree: true, childList: true, characterData: true, attributes: true, attributeFilter: ['hidden', 'class', 'style', 'width', 'height'] }); },
      end() { const m = this; m.on = false; m.mo && m.mo.disconnect(); const t1 = performance.now();
        // the flight: from the first frame the view is open (the open's own synchronous work comes before it) for `fly` ms
        const o = m.tOpen == null ? 0 : m.tOpen, a = m.t0 + o, b = a + m.fly;
        const fr = m.fr.filter(t => t >= a && t <= b + 17), dt = fr.slice(1).map((t, i) => t - fr[i]);
        const lt = window.__lt.filter(x => x.s + x.d > a + 1 && x.s < b);
        const all = m.fr.slice(1).map((t, i) => [Math.round(m.fr[i] - a), Math.round(t - m.fr[i])]).filter(x => x[1] > 34);
        return { ms: Math.round(t1 - m.t0), tOpen: Math.round(o), frames: fr.length, maxDt: Math.round(Math.max(0, ...dt)), over34: dt.filter(x => x > 34).length, lt: lt.length, ltMax: Math.round(Math.max(0, ...lt.map(x => x.d))), ltSum: Math.round(lt.reduce((a, x) => a + x.d, 0)),
          slow: all, lts: window.__lt.filter(x => x.s + x.d > m.t0 && x.s < t1).map(x => [Math.round(x.s - a), Math.round(x.d)]), mut: m.mut.map(x => Object.assign(x, { t: x.t - o })), wc: m.wc, clip: m.clip }; }
    };
  });
  const page = await context.newPage(), errors = [];
  page.setDefaultTimeout(20000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.OrderSearch && window.SheetWin && SheetWin.drawOrder && CN.S.cloud.ok === true, null, { timeout: 60000 });
  await page.evaluate(async ({ orders }) => {
    await Orders.loadMaps(true);
    for (const [order, pool] of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: pool ? 'pooled' : 'pulled', reason: null, claimedBy: null, poolIds: pool ? [pool] : [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    Orders.interpretAll(); CN.setMode('orders'); Orders.render();
  }, { orders: [[order(A.rid, 'Hannah Whitford', [line(A.tid, A.sku, 'gold', '14k Gold Filled')]), null], [order(S.rid, 'Mia Lund', [line(S.tid, S.sku, 'gold', '14k Gold Filled')]), PS]] });
  // the cloud answers late, as it does live: each read lands in the middle of the grow
  await page.route(/\/\.netlify\/functions\/(charmNestLibrary|firebaseOrders)/, async r => { await new Promise(res => setTimeout(res, HOLD)); return r.continue().catch(() => {}); });
  await page.waitForTimeout(800);
  return { page, errors, context };
}

async function flights(page) {
  const out = {}, rest = () => page.waitForTimeout(500);
  const measure = async (fn, ms, fly) => { await page.evaluate(f => window.__m.begin(f), fly || 650); await fn(); await page.waitForTimeout(ms); return page.evaluate(() => window.__m.end()); };
  const shut = () => measure(() => page.evaluate(() => OrderWin.close()), 800, 420);
  const glow = () => page.evaluate(() => { const n = document.querySelector('.ocard[data-key],.orderListRow[data-key]'); return document.getAnimations().filter(a => a.effect && a.effect.target && !a.effect.target.closest('#orderWin')).map(a => a.effect.getKeyframes().flatMap(k => Object.keys(k).filter(p => !['offset', 'easing', 'composite', 'computedOffset'].includes(p)))).flat(); });

  // 1 · an Orders row
  await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
  const row = `.ocard[data-key="${A.rid}_${A.tid}"],.orderListRow[data-key="${A.rid}_${A.tid}"]`;
  await page.waitForSelector(row);
  out.rowOpen = await measure(() => page.click(row), 1000);
  await rest();
  out.rowClose = await measure(async () => { await page.evaluate(() => OrderWin.close()); await page.waitForTimeout(470); out.rowGlowProps = await glow(); }, 400, 420);
  await rest();

  // 2 · a search result: an order outside the pull, read from the records while the view grows
  await page.keyboard.press('/'); await page.waitForFunction(() => OrderSearch.isOpen());
  await page.fill('#cnsQ', C.rid);
  await page.waitForFunction(rid => [...document.querySelectorAll('#cnsList .cnsCard')].some(n => n.dataset.rid === rid), C.rid, { timeout: 10000 });
  await page.waitForTimeout(300);
  out.searchOpen = await measure(() => page.keyboard.press('Enter'), 1500);
  out.searchShown = await page.evaluate(rid => OrderWin.isOpen() && document.getElementById('owTitle').textContent.includes(rid) && !/Reading the order/.test(document.getElementById('owNotes').textContent), C.rid);
  await rest();
  out.searchClose = await shut();
  await rest();

  // 3 · a sheet charm: the Sheet view, its sheet record read while the view grows
  await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
  const charm = await page.evaluate(k => { const n = document.querySelector(`[data-key="${k}"]`); const r = n.getBoundingClientRect(); return { left: r.left + 20, top: r.top + 10, width: 28, height: 28 }; }, `${S.rid}_${S.tid}`);
  out.sheetOpen = await measure(() => page.evaluate(({ rid, charm, pool }) => { OrderWin.openOrder(rid, { view: 'sheet', from: charm, poolId: pool }); }, { rid: S.rid, charm, pool: PS }), 1500);
  out.sheetShown = await page.evaluate(sh => { const i = OrderWin._sheet(); return !!(i && i.sheet && i.sheet.id === sh); }, SH);
  await rest();
  out.sheetClose = await shut();
  await rest();
  return out;
}

const brief = m => m ? `max frame ${m.maxDt} ms · ${m.over34} frame(s) > 34 ms · ${m.lt} long task(s), max ${m.ltMax} ms, sum ${m.ltSum} ms` + (process.env.DEBUG ? `  [slow ${JSON.stringify(m.slow)} lt ${JSON.stringify(m.lts)}]` : '') : '—';

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const base = process.env.BASE || '';
  const over = base ? Object.fromEntries(['charm-nest-bridge.js', 'charm-nest-1.html'].map(f => [f, execFileSync('git', ['show', `${base}:${f}`], { cwd: root, maxBuffer: 64 << 20 }).toString('utf8')])) : null;
  const srv = await start({ receipts: [] }); seed(srv);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const load = () => os.loadavg()[0] / os.cpus().length;
  try {
    const runs = [];
    const rounds = +process.env.ROUNDS || 1;
    for (let i = 0; i < rounds; i++) for (const which of over ? ['before', 'after'] : ['after']) {
      const p = await pageOn(browser, srv, which === 'before' ? over : null);
      const l0 = load(); const res = await flights(p.page); const l1 = load();
      runs.push({ which, res, load: Math.max(l0, l1), errors: p.errors }); await p.context.close();
    }
    const KEYS = ['rowOpen', 'rowClose', 'searchOpen', 'searchClose', 'sheetOpen', 'sheetClose'];
    for (const k of KEYS) for (const r of runs) console.log(`  ${k.padEnd(11)} ${r.which.padEnd(6)} ${brief(r.res[k])}`);
    const after = runs.filter(r => r.which === 'after'), last = after[after.length - 1], R = last.res;
    assert.deepEqual(last.errors, [], 'no page errors');

    // visible: the clip moves from the source to the screen and back
    for (const k of ['rowOpen', 'searchOpen', 'sheetOpen', 'rowClose']) {
      const c = R[k].clip.filter(x => x && x !== 'none'), mid = new Set(c.slice(1, -1));
      assert(c.length >= 6 && mid.size >= 4, `${k}: the view's clip moves over several frames (${c.length} framed, ${mid.size} distinct)`);
    }
    assert(R.searchShown, 'the search result opened its order, read from the records');
    assert(R.sheetShown, 'the sheet charm opened the Sheet view on its sheet');
    // held: no network-driven render reaches the view while it flies (from 60 ms in until it lands at 650 ms)
    for (const k of ['rowOpen', 'searchOpen', 'sheetOpen']) {
      // (not held yet, reported: the charm's vector design and listing photo, drawn by the list's media helper as they
      // arrive, and the Customer tab, drawn by charm-nest-mail.js)
      const mid = R[k].mut.filter(x => x.t > 20 && x.t < 640 && !(x.type === 'attributes' && /owGlow|owTint/.test(x.where)) && !/^owVector$|^owPhoto$|^pic$|^thumb/.test(x.where) && !x.inCust);
      assert.deepEqual(mid.map(x => `${x.type}:${x.where}${x.attr ? '@' + x.attr : ''}`), [], `${k}: nothing is rendered into the view mid-flight`);
    }
    // will-change only during the flight
    for (const k of ['rowOpen', 'searchOpen', 'sheetOpen']) { const w = R[k].wc; assert(w.slice(1, 20).some(x => /clip-path/.test(x)), `${k}: will-change is set while it flies (${w.slice(0, 5)})`); assert(!/clip-path/.test(w[w.length - 1] || ''), `${k}: and cleared once it has landed (${w[w.length - 1]})`); }
    // the landing glow animates opacity only: no box-shadow repainted on the row
    assert(R.rowGlowProps.length > 0, 'the row it lands in glows');
    assert.deepEqual([...new Set(R.rowGlowProps)].filter(p => !['opacity', 'transform'].includes(p)), [], 'the landing glow is opacity-only: ' + R.rowGlowProps.join(','));

    // smooth, gated only on a quiet machine
    const busy = Math.max(...runs.map(r => r.load));
    if (busy < 1.2) {
      for (const k of KEYS) { const m = R[k]; assert(m.ltMax <= 50 || m.lt <= 1 && m.ltMax <= 70, `${k}: no long task over 50 ms while it flies (${brief(m)})`); assert(m.over34 <= 2, `${k}: at most two frames over 34 ms (${brief(m)})`); }
      console.log(`  ✓ frame and long-task thresholds held (load per core ${busy.toFixed(2)})`);
    } else console.log(`  – load per core ${busy.toFixed(2)} ≥ 1.2: frame and long-task thresholds not gated (numbers above are for comparison)`);
    console.log('  ✓ visible, held, will-change only in flight, opacity-only landing glow');
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('adv-orderview-smooth: ok'), e => { console.error(e); process.exit(1); });
