// A counted-disc order ("ROSEGOLD - 2 Disc": n pieces of ONE group, one engraving job per disc) cycled one disc at a time in every card that is not the Engrave tab
// (Paul, 10 Oct 2026: "cycle through all the chosen discs one by one ... everywhere the engraving option is presented to the user, including all popup modals").
// The same component as the Engrave tab (charm-nest-piece-switch.js: the switch, the cursor), the same card as ever (CNEngravingSeals.panel), nothing restyled.
//   node tests/charm-nest/disc-modals.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>; SHOTS=<dir> to write screenshots)
// Headless Chromium on a fake site (bridge-server.cjs); every request that is not to the loopback is aborted; nothing is written anywhere.
//   1 · order window, Overview: ONE card for the disc shown + the Disc 1 | Disc 2 switch + Back/Next; each disc its own words, state, font, Approve, seal, Fix
//   2 · order window, Sheet tab: the same switch above the engraving panel; a disc on another sheet opens that sheet
//   3 · sheet window, piece view: the same switch; a press selects that disc's charm
//   4 · a pair, a single and a plain quantity-N line keep their cards exactly (no switch)
//   5 · text: the order's timeline lines, the global search, the set manifest, the station tag name the disc
const path = require('path'), fs = require('fs'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 11, 17) / 1000), SH = 'sheet-dm-1', SH2 = 'sheet-dm-2';
const SHOTS = process.env.SHOTS || '';
const D = { rid: '4175370240', tid: '41753702401', sku: 'INITIAL_DISC_4571', discs: 2 };      // two discs on one sheet: J and Q
const E = { rid: '4172791262', tid: '41727912621', sku: 'INITIAL_8391', discs: 3 };            // three discs, the third on another sheet: A, A, B
const S1 = { rid: '4180000099', tid: '41800000991', sku: 'FIREBIRD_2', discs: 0 };             // a single charm
const Q3 = { rid: '4180000123', tid: '41800001231', sku: 'FIREBIRD_2', discs: 0, qty: 3 };      // a plain quantity-3 line (three of one charm, no discs)
const pool = (o, n) => `${o.rid}_${o.tid}_${n}`, keyOf = o => `${o.rid}_${o.tid}`;
const line = o => ({ transactionId: o.tid, listingId: '18000' + o.tid.slice(-5), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' Necklace Personalized', quantity: o.qty || 1, expectedShipDate: SHIP,
  variations: o.discs ? [{ name: 'Metal', value: '14k Gold Filled' }, { name: 'Necklace options', value: `ROSEGOLD - ${o.discs} Disc` }, { name: 'Fonts', value: '16"/ Typewriter' }] : [{ name: 'Metal', value: '14k Gold Filled' }],
  metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: o.discs ? ['Tag 1: J, Tag 2: Q'] : [] });
const order = (o, buyer) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines: [line(o)] });

function seed(st) {
  const sheet = (id, n, charms) => ({ id, metal: 'gold', sheetIndex: n, setSeq: 1, day: '2026-10-10', status: 'written', fileBase: `GF_Oct.10.26_Set-1_Sheet-${n}`, folder: `GF_Oct.10.26_Set-1_Sheet-${n}`, stock: { wPt: 230, hPt: 90 }, orders: [...new Set(charms.map(c => c.order))],
    placements: charms.map((c, i) => ({ id: c.id, cxPt: 20 + i * 30, cyPt: 30, angle: 0, wPt: 22, hPt: 22 })), charms, backPool: [] });
  const ch = (id, o, n) => ({ id, name: `${o.rid} · ${o.sku}`, poolId: pool(o, n), order: o.rid, sku: o.sku });
  const on1 = [ch('c0', D, 1), ch('c1', D, 2), ch('c2', E, 1), ch('c3', E, 2), ch('c4', S1, 1)], on2 = [ch('c5', E, 3)];
  st.put('Charm_Nest_Sheets', SH, sheet(SH, 1, on1)); st.put('Charm_Nest_Sheets', SH2, sheet(SH2, 2, on2));
  const row = (o, n, sheetId) => ({ poolId: pool(o, n), orderId: o.rid, transactionId: o.tid, lineKey: keyOf(o), sku: o.sku, material: 'gold', copy: n, quantity: o.discs || 1, state: 'written', sheetId, sheetName: sheetId === SH ? 'GF_Oct.10.26_Set-1_Sheet-1' : 'GF_Oct.10.26_Set-1_Sheet-2', updatedAt: Date.now() });
  for (const [o, n, s] of [[D, 1, SH], [D, 2, SH], [E, 1, SH], [E, 2, SH], [E, 3, SH2], [S1, 1, SH]]) st.put('Charm_Pool', pool(o, n), row(o, n, s));
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
    });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('crash', () => console.error('PAGE CRASHED'));
    if (process.env.DM_LOG) page.on('console', m => console.log('  [console]', m.type(), m.text().slice(0, 300)));
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && window.Engrave && window.CNEngravingSeals && CN.S.cloud.ok === true, null, { timeout: 60000 });
    check(await page.evaluate(() => !!(window.CharmNestPieceSwitch && CharmNestPieceSwitch.html)), 'the page loads the shared piece switch (CharmNestPieceSwitch)');

    // ── the orders, one job per disc ─────────────────────────────────────────────────────────────────────────────────
    await page.evaluate(async ({ orders, SH }) => {
      await Orders.loadMaps(true);
      for (const [o, pools] of orders) for (const l of o.lines) { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'written', reason: null, claimedBy: null, poolIds: pools, engrave: null, material: 'gold', metal: 'gold' }; B.orders.rows.push(row); B.orders.byKey && B.orders.byKey.set(key, row); }
      Orders.interpretAll();
      window.__approves = []; window.__links = [];
      Engrave.renderBack = () => { const cv = document.createElement('canvas'); cv.width = 300; cv.height = 300; return cv; };
      Engrave.approve = async (job, by, button) => {
        if (job._approvalTask) return; job._approvalTask = true;
        try {
          if (job.state !== 'review') return;
          __approves.push({ key: job.key, by });
          await new Promise(r => setTimeout(r, 120));
          const at = Date.now(), seal = CNEngravingSeals.add(job, 'engraveApproved', by, at);
          job.stamping = true; try { await CNEngravingSeals.press(button, seal); } finally { job.stamping = false; }
          Object.assign(job, { state: 'approved', approvedBy: by, approvedAt: at });
          Object.assign(job.row.engrave || (job.row.engrave = {}), { needed: true, state: 'approved', approved: true, approvedBy: by, approvedAt: at, text: job.text, seals: job.engravingSeals });
        } finally { delete job._approvalTask; }
      };
      if (window.EngraveLink) EngraveLink.open = async a => { __links.push(a); return true; };
    }, { orders: [[order(D, 'Ariana Gugler'), [pool(D, 1), pool(D, 2)]], [order(E, 'Dana Wolf'), [pool(E, 1), pool(E, 2), pool(E, 3)]], [order(S1, 'Cari Moll'), [pool(S1, 1)]], [order(Q3, 'Lee Park'), [pool(Q3, 1), pool(Q3, 2), pool(Q3, 3)]]], SH });
    const setJobs = (o, defs) => page.evaluate(({ key, defs }) => {
      const row = B.orders.byKey.get(key), jobs = Engrave.ensureJobs(row), T = Date.now();
      defs.forEach(([slot, st, text, font]) => {
        const job = jobs.find(j => j.slot === slot) || jobs[0], done = st === 'approved';
        Object.assign(job, { state: st, text, lines: text ? text.split(' // ') : [], activityAt: T, approvedBy: done ? 'Paul' : null, approvedAt: done ? T - 3600e3 : null, backs: [], fit: st === 'review' || done ? {} : null, view: st === 'review' || done ? {} : null, verify: { geometry: { ok: true } } });
        if (font) job.font = font;
        if (done) job.engravingSeals = [{ id: 'old' + slot, how: 'engraveApproved', at: T - 3600e3, by: 'Paul' }];
        job.engraveRec = { needed: true, state: st, approved: done, text, approvedBy: job.approvedBy, approvedAt: job.approvedAt, seals: done ? job.engravingSeals : [] };
      });
      return jobs.map(j => j.key);
    }, { key: keyOf(o), defs });
    const dKeys = await setJobs(D, [['D1', 'review', 'J', { name: 'Typewriter', asked: 'Typewriter' }], ['D2', 'review', 'Q', { name: 'Typewriter', asked: 'Typewriter' }]]);
    const eKeys = await setJobs(E, [['D1', 'review', 'A'], ['D2', 'approved', 'A'], ['D3', 'words', 'B']]);
    const sKeys = await setJobs(S1, [[null, 'review', 'GOOD // LUCK']]);
    const qKeys = await setJobs(Q3, [[null, 'review', 'LOVE']]);
    check(dKeys.length === 2 && eKeys.length === 3 && dKeys[0] === `${keyOf(D)}#D1` && sKeys.length === 1 && !/#/.test(sKeys[0]), 'one job per disc for the two counted lines, one job for the single: ' + JSON.stringify([dKeys, eKeys, sKeys]));
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    await page.waitForSelector(`#ordItems [data-key="${keyOf(D)}"]`);

    const shot = async (name, sel) => { if (!SHOTS) return; fs.mkdirSync(SHOTS, { recursive: true }); try { const h = sel ? await page.$(sel) : null; if (h) await h.screenshot({ path: path.join(SHOTS, name) }); else await page.screenshot({ path: path.join(SHOTS, name) }); } catch (e) { console.log('  – shot ' + name + ' failed: ' + e.message); } };
    const OV = '#orderWin .owVInfo .owEng', TAB = '#owSheetPanel [data-engraving-panel]', WIN = '[data-r2=eng]';
    const waitCard = (sel, state, ms = 8000) => page.waitForFunction(({ sel, state }) => { const e = document.querySelector(sel + ' .swEng'); return e && (e.offsetParent || e.getClientRects().length) && (!state || e.dataset.state === state); }, { sel, state }, { timeout: ms }).then(() => true, () => false);
    const closeWin = async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 5000 }).catch(() => {}); await page.waitForTimeout(300); };
    // what the engraving area of a host shows, as a person reads it
    const area = sel => page.evaluate(sel => {
      const host = document.querySelector(sel); if (!host) return { host: false };
      const cards = [...host.querySelectorAll('.swEng')].filter(e => e.offsetParent || e.getClientRects().length);
      const sw = host.querySelector('.egEarSwitch'), tabs = sw ? [...sw.querySelectorAll('.egEarTab')] : [];
      const e = cards[0], b = e && e.querySelector('.egApproveButton');
      return { host: true, cards: cards.length, state: e && e.dataset.state, words: e ? ((e.querySelector('.words') || {}).textContent || '') : '', seals: e ? e.querySelectorAll('.seal').length : 0, button: b ? { text: b.textContent.trim(), disabled: b.disabled } : null,
        switchShown: !!(sw && (sw.offsetParent || sw.getClientRects().length)), tabs: tabs.map(t => ({ key: t.dataset.ear, on: t.getAttribute('aria-pressed') === 'true', text: t.textContent.replace(/\s+/g, ' ').trim() })),
        nav: [...host.querySelectorAll('[data-a="discPrev"],[data-a="discNext"]')].map(n => ({ a: n.dataset.a, disabled: n.disabled })), engrave: !!(e && e.querySelector('[data-e=engrave]')) };
    }, sel);
    const probe = { shot, OV, TAB, WIN, waitCard, closeWin, area };
    module.exports.__probe = probe;
    await require('./disc-modals-run.cjs')({ page, check, probe, srv, D, E, S1, pool, keyOf, dKeys, eKeys, sKeys, qKeys, Q3, errors });
  } finally { await browser.close(); srv.close(); }
  if (fails.length) { console.error('\n' + fails.length + ' failed:\n - ' + fails.join('\n - ')); process.exit(1); }
  console.log('disc-modals: ok');
}
main().catch(e => { console.error(e); process.exit(1); });
