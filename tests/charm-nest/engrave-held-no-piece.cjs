// HELDENGRAVE (Paul, 18 Nov 2026, order 4225100553 on Ana's computer: "it keeps on flashing the screen when I look at it ... this is not the high-quality image. The grade is gone and the options underneath
// the charm work area are also missing so this order is broken").  An order that is ON HOLD has no piece on any sheet (its pieces were taken off, or never placed): its engraving job still stands in Review
// (words read, even an approval seal on it) but it has NO charm to fit the words on.  Its card used to be half drawn: a small flat picture, no grid, no row of options, and the page kept drawing it again.
//   1  the card of such a job says plainly that the order is on hold and the engraving opens once it is placed on a sheet; it does not pretend to be a placement (no Approve button)
//   2  it keeps its options row (disabled, saying why) and is drawn ONCE: left alone for 5 s the card is not rebuilt (no flicker)
//   3  a job that DOES have its piece (the normal case) is untouched: the same card, Approve, the options row
// Real charm-nest-1.html in headless Chromium over the repo's fake site (bridge-server.cjs). Nothing live.
//   PW_DIR=... CHROMIUM=... node tests/charm-nest/engrave-held-no-piece.cjs      (and again with REAL_ROW=1: the leftover pool row, state abandoned, that order 4225100553 really has)
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHIP = Math.floor(Date.UTC(2026, 10, 18, 17) / 1000), RID = '4225100553', TX = '5283317236', KEY = `${RID}_${TX}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Custom heart charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['❤️'] }] };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('engrave-held-no-piece: playwright-core not found, skipped'); process.exit(0); } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  try {
    const context = await browser.newContext({ viewport: { width: 1280, height: 720 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', JSON.stringify({ name: 'Ana_M' })); } catch (_) {} });
    await context.addInitScript(r => { window.__realRow = r; }, process.env.REAL_ROW === '1');   // (REAL_ROW=1: the job names the pool row the cloud really holds, abandoned and held; default: no piece at all)
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Engrave && window.CharmNestEngravePanel && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // the order, ON HOLD, no piece anywhere; its engraving job in Review with the words and no fit (there is no charm to fit them on)
    await page.evaluate(async ({ order, KEY }) => {
      const line = order.lines[0];
      const row = { key: KEY, order, line, arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'held', hold: 'on hold: the design is not ready', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      row.spec.engraveCandidate = true;
      const j = Engrave.ensureJob(row);
      const REAL = !!window.__realRow;   // (the cloud's own picture of order 4225100553: its pool row stays, state abandoned, held by Ana_M, and the job names it)
      if (REAL) { const pid = KEY + '_1'; B.pool.rows.set(pid, { poolId: pid, orderId: row.order.receiptId, state: 'abandoned', sheetId: null, heldAt: Date.now() - 3600e3, heldBy: 'Ana_M', heldReason: 'on hold', custom: true }); }
      Object.assign(j, { state: 'review', text: '❤️', lines: ['❤️'], lineInput: ['❤️'], copies: REAL ? [KEY + '_1'] : [], pre: true, fit: null, view: null, mask: null, confidence: 0.85, source: 'personalization' });
      row.engrave = { needed: true, state: 'review', approved: false, text: j.text };
      window.__renders = 0; window.__stacks = []; const r0 = Engrave.render; Engrave.render = function () { __renders++; if (__stacks.length < 12) __stacks.push(new Error().stack.split('\n').slice(2, 7).map(x => x.trim().replace(/http:\/\/[^/]+\//, '')).join(' < ')); return r0.apply(this, arguments); };
    }, { order: ORDER, KEY });

    const open = () => page.evaluate(async KEY => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back') || Engrave.jobOf(row);
      setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
      await new Promise(r => setTimeout(r, 400));
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement]');
      const q = s => c ? c.querySelectorAll(s).length : -1;
      return { has: !!c, text: c ? c.innerText.replace(/\s+/g, ' ') : '', approve: q('[data-a=approve]'), approveOn: c ? [...c.querySelectorAll('[data-a=approve]')].filter(b => !b.disabled).length : -1, heldNotes: c ? [...c.querySelectorAll('.egHeldNote,.egApproveWhy,.why')].map(n => n.className + ':' + n.textContent.trim().slice(0, 40)) : [], ctl: q('.ctl'), centre: q('[data-a=centre]'), lines: q('[data-a=linecount]'), held: c ? [...c.querySelectorAll('*')].filter(n => !n.children.length && /on hold, so its piece is not on a sheet/.test(n.textContent)).length : -1, heldText: c ? ([...c.querySelectorAll('*')].filter(n => !n.children.length && /on hold, so its piece is not on a sheet/.test(n.textContent))[0] || { textContent: '' }).textContent.replace(/\s+/g, ' ').trim() : '',
        centreOff: c && c.querySelector('[data-a=centre]') ? c.querySelector('[data-a=centre]').disabled : null, canvases: q('canvas'), state: j.state, fit: !!j.fit };
    }, KEY);

    // ── 1, 2 · the held order's card ──
    const d = await open();
    console.log('   card:', JSON.stringify(d).slice(0, 700));
    check(d.has, 'the Engrave detail opens on the held order\'s job');
    check(d.held >= 1 && /on hold/i.test(d.heldText) && /sheet/i.test(d.heldText), 'it says plainly that the order is on hold and the engraving opens once it is placed on a sheet: "' + d.heldText + '"');
    check(d.approveOn === 0, 'and Approve cannot be pressed (there is nothing to place the words on): ' + JSON.stringify({ buttons: d.approve, pressable: d.approveOn }));
    check(d.held === 1, 'the sentence is said once, in the panel beside the words (never over the work area, where the Open in Charm Editor button sits): ' + d.held);
    check(d.ctl >= 1 && d.centre === 1 && d.lines === 1 && d.centreOff === true, 'the options row is there, disabled: ' + JSON.stringify({ ctl: d.ctl, centre: d.centre, lines: d.lines, off: d.centreOff }));
    if (shots) await page.screenshot({ path: path.join(shots, 'held-card.png') });
    await page.waitForTimeout(2500);   // (the page's own start-up work settles first)
    // is the card replaced, or its drawing redrawn, while nobody touches anything?
    const churn = await page.evaluate(() => new Promise(res => {
      const host = document.querySelector('#egQueue'); let adds = 0, muts = 0;
      const mo = new MutationObserver(list => { for (const m of list) { muts++; adds += m.addedNodes.length; } });
      mo.observe(host, { childList: true, subtree: true, attributes: true, characterData: true });
      const n0 = __renders;
      setTimeout(() => { mo.disconnect(); res({ renders: __renders - n0, muts, adds }); }, 5000);
    }));
    if (process.env.HELD_DEBUG) console.log((await page.evaluate(() => __stacks)).map((x, i) => '   [' + i + '] ' + x).join('\n'));
    check(churn.renders <= 1 && churn.adds <= 2, 'left alone for 5 s the card is not drawn again and again: ' + JSON.stringify(churn));

    // ── 3 · a job WITH its piece is as it was ──
    srv.st.put('Charm_Nest_Sheets', 'gold-su-1', { id: 'gold-su-1', metal: 'gold', charms: [{ id: 'g0', poolId: KEY + '_1', order: RID }], placements: [{ id: 'g0', cxPt: 40, cyPt: 40, angle: 0 }], poolIds: [KEY + '_1'], orders: [RID], sheetIndex: 1, fileBase: 'gold-su-1', runId: 'run-test-1' });
    const d3 = await page.evaluate(async KEY => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back') || Engrave.jobOf(row);
      row.state = 'pooled'; delete row.hold; row.poolIds = [KEY + '_1']; j.copies = [KEY + '_1']; j.text = 'Ana'; j.lines = ['Ana']; j.lineInput = ['Ana'];
      j.fit = { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }; j.view = {}; j.verify = { geometry: { ok: true } };
      setMode('engrave'); Engrave.restoreView({ tab: 'place', list: false, chosen: true, focus: j.key }); Engrave.render();
      await new Promise(r => setTimeout(r, 300));
      const c = document.querySelector('#egQueue .rvItem[data-kind=placement]'), q = s => c ? c.querySelectorAll(s).length : -1;
      return { has: !!c, held: q('.egHeldNote'), approve: q('[data-a=approve]'), centre: q('[data-a=centre]') };
    }, KEY);
    check(d3.has && d3.held === 0 && d3.approve === 1 && d3.centre === 1, 'a job with its piece on a sheet draws the card as before: ' + JSON.stringify(d3));
    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
  } finally { await browser.close(); srv.close && srv.close(); }
  if (fails.length) { console.log('engrave-held-no-piece: ' + fails.length + ' failed'); process.exit(1); }
  console.log('engrave-held-no-piece: ok');
})().catch(e => { console.error(e); process.exit(1); });
