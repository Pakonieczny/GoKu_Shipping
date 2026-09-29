// The Send to Sheet tour with more than one sheet (charm-nest-tour.js, Paul 29 Sep 00:25: "if there's multiple sheets,
// then I should see one animation at a time distinctively … and then fully coming back to where the user started").
// Driven in a real Chromium on the fake site (bridge-server.cjs), with the page's own NestFocus, its calls recorded:
//   1 · one order, two designs on two metals (GF, SS): two legs, one after the other: GF opened, its piece landed,
//       then SS takes the light from it (Paul, 29 Sep 01:30: no flash of every card between two sheets), "2 of 2"
//       said, landed, closed; never two sheets spotlit at once; home to Review · Custom Orders at the same scroll;
//   2 · one design × 3 on one sheet: one leg, the three land one after another, "+3";
//   4 · a two-line order (two Etsy lines of one listing, one card), one line held: the placed legs play, then the held pieces wait and say why; home;
//   6 · a click in the middle of leg 2: everything lands at once, NestFocus closes, nothing hidden or dimmed, home.
// Screenshots to SHOTS (default /mnt/project-files/plans/tour/p2-*.png).
//   node tests/charm-nest/tour-p2.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), fs = require('fs');
const here = path.join(__dirname, '../..');
const pwDir = process.env.PW_DIR || path.join(here, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/tour';
const DG = (...kv) => { let t = ''; for (let i = 0; i < kv.length; i += 2) t += `${kv[i]}\n${kv[i + 1]}\n`; return t; };
const DXF = (w, h = 20) => DG(0, 'SECTION', 2, 'HEADER', 9, '$INSUNITS', 70, 4, 0, 'ENDSEC', 0, 'SECTION', 2, 'ENTITIES',
  0, 'LWPOLYLINE', 8, 'CUT', 90, 4, 70, 1, 10, 0, 20, 0, 10, w, 20, 0, 42, 0.4, 10, w, 20, h, 10, 0, 20, h,
  0, 'CIRCLE', 8, 'CUT', 10, w / 2, 20, h - 4, 40, 1.2, 0, 'ENDSEC', 0, 'EOF');
const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 2, 17) / 1000);
const line = (rid, i) => ({ transactionId: rid + i, listingId: '18000' + rid.slice(-5), sku: 'CUSTOM-N-001-' + rid.slice(-6), title: 'Custom Name Necklace, Personalized Gold Charm Necklace', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' });
const order = (rid, n, lines = 1) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - n * DAY, updateTs: SHIP - n * DAY + 60, shipBy: SHIP, buyer: { name: 'Buyer ' + rid.slice(-4) }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: Array.from({ length: lines }, (_, i) => line(rid, i + 1)) });
const R1 = '4175423841', R2 = '4175423842', R4 = '4175423844', R6 = '4175423846';
const ORDERS = [order(R1, 6), order(R2, 5), order(R4, 4, 2), order(R6, 3)];

// NestFocus's calls, and at each open how many sheets stand spotlit; the most ever spotlit at once, frame by frame
function recordNF() {
  const calls = window.__NF = [], NFo = window.NestFocus; if (!NFo) throw new Error('no NestFocus on the page');
  const open = NFo.open, waiting = NFo.waiting;
  const lit = () => document.querySelectorAll('.sheetCard.nfFocus').length;
  window.__maxLit = 0; const tick = () => { window.__maxLit = Math.max(window.__maxLit, lit()); requestAnimationFrame(tick); }; requestAnimationFrame(tick);
  NFo.open = function (id, o) {
    calls.push(['open', String(id), (o.poolIds || []).slice(), o.caption, lit(), performance.now()]);
    return Promise.resolve(open.apply(this, arguments)).then(f => f && Object.assign({}, f, {
      land: pid => { calls.push(['land', pid, lit()]); return f.land(pid); },
      close: () => { calls.push(['close', performance.now()]); return f.close(); } }));
  };
  NFo.waiting = function (m) { calls.push(['waiting', m]); return waiting.apply(this, arguments); };
  // the "+N" notes and the captions the tour shows
  window.__said = [];
  new MutationObserver(ms => { for (const m of ms) for (const n of m.addedNodes) if (n.nodeType === 1 && /tourPlus|tourCap|nfCap/.test(n.className)) setTimeout(() => window.__said.push(n.textContent), 0); }).observe(document.body, { childList: true, subtree: true });
}
const clean = () => ({ mode: CN.S.mode, chip: !!document.querySelector('#reviewView .egTab[data-k="customOrder"].on'), scroll: document.querySelector('#reviewView .egPane.scroll')?.scrollTop ?? null,
  left: document.querySelectorAll('#tourLayer > *').length, lit: document.querySelectorAll('.sheetCard.nfFocus, .nfCap, .nfGlow, .nfPiece, .nfRing').length,
  dim: ['nfDim', 'nfOn'].filter(c => document.getElementById('sheets').classList.contains(c)), hidden: allSheets().filter(p => p._tourHide && p._tourHide.size).length,
  nfOpen: NestFocus.isOpen(), views: [...document.querySelectorAll('#sheets, #reviewView')].map(v => v.getAnimations().filter(a => a.playState !== 'finished').length).reduce((a, b) => a + b, 0) });

(async () => {
  const { start } = require(path.join(here, 'tests/charm-nest/bridge-server.cjs'));
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], check = (ok, what) => { if (!ok) fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
  fs.mkdirSync(SHOTS, { recursive: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 950 } });
  try {
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.g/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = []; page.setDefaultTimeout(20000);
    page.on('pageerror', e => errors.push(String(e)));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.CustomSheet && window.SendTour && window.NestFocus && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(recordNF);
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('review'); Review.render();
      const day = new Date().toISOString().slice(0, 10);
      B.run = { runId: `run-${day}-tour`, day, setId: null, releasePolicy: 2, solidIncluded: {}, step: 'nest', status: 'running', mode: 'manual', startedAt: Date.now(), updatedAt: Date.now(), lines: {}, sheets: {}, holds: {}, errors: [], resumable: true, stoppedBy: null, fix: null, orders: [] };
    }, ORDERS);
    await page.click('#reviewView .egTab[data-k="customOrder"]');
    // designs dropped on a card, each on its metal, with its count of copies
    const drop = async (rid, designs) => {
      const card = `#rvList .reviewListRow[data-rid="${rid}"]`; await page.waitForSelector(card);
      await page.evaluate(({ sel, files }) => { const dt = new DataTransfer(); for (const f of files) dt.items.add(new File([f.text], f.name)); const n = document.querySelector(sel); for (const t of ['dragenter', 'dragover', 'drop']) n.dispatchEvent(new DragEvent(t, { bubbles: true, cancelable: true, dataTransfer: dt, clientX: 700, clientY: 400 })); },
        { sel: card, files: designs.map((d, i) => ({ name: d.name || `design-${i + 1}.dxf`, text: DXF(d.w || 18, d.h || 20) })) });
      await page.waitForFunction(n => document.querySelectorAll('#cuDlg[open] .cuFile .cuThumb img').length === n, designs.length, { timeout: 30000 });
      for (const [i, d] of designs.entries()) {
        await page.click(`#cuDlg .cuFile:nth-child(${i + 1}) .cuM[data-m="${d.metal}"]`);
        for (let q = 1; q < (d.qty || 1); q++) await page.click(`#cuDlg .cuFile:nth-child(${i + 1}) [data-q="1"]`);
      }
      await page.waitForTimeout(500);
    };
    const send = async () => { await page.evaluate(() => { window.__NF.length = 0; window.__said.length = 0; window.__maxLit = 0; window.__t0 = performance.now(); document.querySelector('#cuDlg [data-send]').click(); }); await page.waitForFunction(() => SendTour.playing(), null, { timeout: 8000, polling: 'raf' }); };
    const home = () => page.waitForFunction(() => !SendTour.playing(), null, { timeout: 25000 });
    const rowsOf = rid => page.evaluate(rid => B.orders.rows.filter(r => String(r.order.receiptId) === rid).map(r => ({ key: r.key, state: r.state, reason: r.reason, ids: (r.poolIds || []).slice(), sheets: (r.poolIds || []).map(id => { const p = allSheets().find(s => s.charms.some(c => c.poolId === id)); return p ? `${p.metal}:${p.page || 1}` : null; }) })), rid);
    const scrollOf = () => page.evaluate(() => document.querySelector('#reviewView .egPane.scroll')?.scrollTop ?? null);
    const nothingLeft = (e, what) => check(e.mode === 'review' && e.chip && !e.left && !e.lit && !e.dim.length && !e.hidden && !e.nfOpen && !e.views, `${what}: home to Review · Custom Orders, nothing left spotlit, dimmed, hidden or on the tour layer (${JSON.stringify(e)})`);
    // one sheet in the light at a time: each piece lands on a sheet standing alone in the light, and never are two lit
    const oneAtATime = (calls, most) => most <= 1 && calls.filter(c => c[0] === 'land').every(c => c[2] === 1);

    // ── 1 · one order, two designs on two metals ──
    await drop(R1, [{ metal: 'gold' }, { metal: 'silver' }]);
    const s1 = await scrollOf();
    await send();
    await page.waitForFunction(() => window.__NF.filter(c => c[0] === 'land').length >= 1, null, { timeout: 12000 });
    await page.waitForTimeout(250); await page.screenshot({ path: path.join(SHOTS, 'p2-1-leg1-landing.png') });
    await page.waitForFunction(() => window.__NF.filter(c => c[0] === 'land').length >= 2, null, { timeout: 12000 });
    await page.waitForTimeout(250); await page.screenshot({ path: path.join(SHOTS, 'p2-2-leg2-landing.png') });
    await home();
    const took1 = await page.evaluate(() => Math.round(performance.now() - __t0));
    await page.waitForTimeout(300); await page.screenshot({ path: path.join(SHOTS, 'p2-3-home.png') });
    const e1 = await page.evaluate(clean), c1 = await page.evaluate(() => window.__NF.slice()), r1 = await rowsOf(R1), m1 = await page.evaluate(() => window.__maxLit), said1 = await page.evaluate(() => window.__said.slice());
    console.log(`  1 · ${took1} ms; NestFocus ${JSON.stringify(c1.map(c => c.slice(0, 4)))}; rows ${JSON.stringify(r1)}`);
    const opens1 = c1.filter(c => c[0] === 'open');
    check(r1[0].state === 'pooled' && JSON.stringify(r1[0].sheets) === '["gold:1","silver:1"]', `placed first: one piece on GF, one on SS (${JSON.stringify(r1[0].sheets)})`);
    check(opens1.length === 2 && /^GF 14\/20 · Sheet 1/.test(opens1[0][3]) && /^SS · Sheet 1/.test(opens1[1][3]) && JSON.stringify(opens1[0][2]) === JSON.stringify([r1[0].ids[0]]) && JSON.stringify(opens1[1][2]) === JSON.stringify([r1[0].ids[1]]), `two legs, GF then SS, each with its own piece (${JSON.stringify(opens1.map(c => [c[1], c[2], c[3]]))})`);
    check(oneAtATime(c1, m1) && JSON.stringify(c1.map(c => c[0])) === '["open","land","open","land","close"]', `one sheet at a time: opened, landed, the next takes the light, landed, closed (${c1.map(c => c[0]).join(' ')}; most lit at once ${m1})`);
    check(said1.some(s => /^GF 14\/20 · Sheet 1 1 of 2/.test(s)) && said1.some(s => /^SS · Sheet 1 2 of 2/.test(s)) && said1.filter(s => /on Sheet 1 \+1/.test(s)).length === 2, `each sheet said, "1 of 2", "2 of 2", and what arrived on it (${JSON.stringify(said1)})`);
    check(took1 < 13500, `two sheets in ${took1} ms`);
    nothingLeft(e1, '1'); check(e1.scroll === s1, `the same scroll (${s1} → ${e1.scroll})`);

    // ── 2 · one design × 3 pieces on one sheet ──
    await drop(R2, [{ metal: 'gold', qty: 3 }]);
    await send(); await home();
    const c2 = await page.evaluate(() => window.__NF.slice()), said2 = await page.evaluate(() => window.__said.slice()), r2 = await rowsOf(R2);
    console.log(`  2 · NestFocus ${JSON.stringify(c2.map(c => c.slice(0, 4)))}; said ${JSON.stringify(said2)}`);
    const lands2 = c2.filter(c => c[0] === 'land');
    check(c2.filter(c => c[0] === 'open').length === 1 && lands2.length === 3 && r2[0].ids.every(id => lands2.some(l => l[1] === id)), `one leg, its three pieces landed one after another (${lands2.map(l => l[1]).join(', ')})`);
    check(said2.some(s => /on Sheet 1 \+3\b/.test(s)), `"+3" said (${JSON.stringify(said2.filter(s => /\+/.test(s)))})`);
    nothingLeft(await page.evaluate(clean), '2');

    // ── 4 · a two-line order, one line held: the placed legs play, then the held pieces wait and say why ──
    // (two lines: each design is cut twice by default, one copy for each line)
    await drop(R4, [{ metal: 'gold' }, { metal: 'silver' }]);
    const k42 = (await rowsOf(R4))[1].key;
    await page.evaluate(k => { const pa = Pool.poolAdd; window.__pa = pa; Pool.poolAdd = function (row) { if (row.key === k) return Promise.reject(new Error('its chain is out of stock')); return pa.apply(this, arguments); }; }, k42);
    await send(); await home();
    await page.evaluate(() => { Pool.poolAdd = window.__pa; });
    const c4 = await page.evaluate(() => window.__NF.slice()), said4 = await page.evaluate(() => window.__said.slice()), r4 = await rowsOf(R4);
    const note4 = await page.evaluate(() => [...document.querySelectorAll('.mNote')].map(n => n.textContent).join(' | '));
    console.log(`  4 · NestFocus ${JSON.stringify(c4.map(c => c.slice(0, 4)))}; said ${JSON.stringify(said4)}; rows ${JSON.stringify(r4)}; note ${note4}`);
    check(r4[0].state === 'pooled' && r4[1].state === 'held', `line 1 placed, line 2 held (${r4.map(r => r.state)})`);
    const seq4 = c4.map(c => c[0] + (c[0] === 'waiting' ? ':' + c[1] : '')).join(' ');
    check(JSON.stringify(r4[0].sheets) === '["gold:1","silver:1"]', `line 1's copies placed, one on each metal (${JSON.stringify(r4[0].sheets)})`);
    check(/^open land open land close waiting:gold waiting:silver$/.test(seq4), `the placed legs (GF, SS) play, then the held line's GF and SS pieces wait (${seq4})`);
    check(said4.filter(s => /held/.test(s) && /out of stock/.test(s)).length === 2 && /held.*out of stock/.test(note4), `each says why it waits, and so does the note home (${JSON.stringify(said4.filter(s => /held/.test(s)))})`);
    nothingLeft(await page.evaluate(clean), '4');

    // ── 6 · a click in the middle of leg 2: everything lands at once, and home ──
    await drop(R6, [{ metal: 'gold' }, { metal: 'silver', qty: 2 }]);
    await send();
    await page.waitForFunction(() => window.__NF.filter(c => c[0] === 'open').length >= 2, null, { timeout: 12000 });
    await page.waitForTimeout(700);
    const t6 = await page.evaluate(() => performance.now());
    await page.mouse.click(720, 480);
    await home();
    const ms6 = await page.evaluate(t => Math.round(performance.now() - t), t6);
    await page.waitForTimeout(700);   // (NestFocus's own easing back ends)
    const e6 = await page.evaluate(clean), c6 = await page.evaluate(() => window.__NF.slice()), r6 = await rowsOf(R6), m6 = await page.evaluate(() => window.__maxLit);
    console.log(`  6 · home ${ms6} ms after the click; NestFocus ${JSON.stringify(c6.map(c => c.slice(0, 3)))}`);
    check(r6[0].state === 'pooled' && ms6 < 1500, `skipped: home ${ms6} ms after the click`);
    // (skipped, the rest lands at once: the light has already let go, so a landing there stands on no lit sheet)
    check(m6 <= 1 && c6.filter(c => c[0] === 'close').length >= 1, `never two sheets lit, and the sheet open when skipped is closed (${c6.map(c => c[0]).join(' ')}; most lit at once ${m6})`);
    nothingLeft(e6, '6');
    check(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await context.close(); await browser.close(); await srv.close(); }
  if (fails.length) { console.log(`\n${fails.length} failed`); process.exit(1); }
  console.log('\nall passed');
})().catch(e => { console.error(e); process.exit(1); });
