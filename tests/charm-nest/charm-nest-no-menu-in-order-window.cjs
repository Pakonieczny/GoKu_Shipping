// ENGMODAL (engrave-ui-1012, Paul point 4, 12 Oct 2026): "this menu should not exist as part of the detailed orders modal popup, it should only be part of the detailed
// Engraving modal popup within the Engraving tab". The ENGRAVES options menu (None / Front only / Back only / Front + back, the words box, the font drop-down, Save / Cancel,
// the "[Edit]" button) is drawn by charm-nest-engrave-setup.js. This proves, in the REAL page (charm-nest-1.html in headless Chromium over the local fake site; offline: no Etsy,
// no paid reading, no live write), for an order that has engraving:
//   1  the order window (and its Sheet view) draws no ENGRAVES menu, no textarea, no font select, no Edit/Save/Cancel, while the engraving card itself is still there
//   2  the sheet window's engraving card draws none either
//   3  the Engrave tab's placement card still has its menu slot filled (the one place the menu lives)
//   4  source: no mount of the menu is left in order-engraving.js or sheetwin.js, and the order window's Sheet-tab panel in the bridge no longer creates the owEngSetup host
// Names are made up. Run: PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/charm-nest-no-menu-in-order-window.cjs
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');
const SHIP = Math.floor(Date.UTC(2026, 9, 14, 17) / 1000), RID = '4195966039', TX = '5009123456789', KEY = `${RID}_${TX}`;
const ORDER = { receiptId: RID, orderNumber: RID, createTs: SHIP - 5 * 86400, updateTs: SHIP - 5 * 86400 + 60, shipBy: SHIP, buyer: { name: 'Name One' }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
  lines: [{ transactionId: TX, listingId: '4458930445', sku: 'MINI_0445', title: 'Mini Tag Initial Charm Add On Charm Personalized Letter Charm', quantity: 1, expectedShipDate: SHIP, metalKey: 'gold', metalLabel: 'GF 14/20',
    variations: [{ name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font choice', value: 'Ace' }, { name: 'Metal Choice', value: 'Gold Filled' }], personalization: ['front - Name One\nback - Name Two'] }] };

// what would make a person see the menu: its own classes / data hooks, or any words box, font drop-down or its buttons
const HOOKS = '.egSetup, .owEngSetup, .swEngSetup, [data-su], [data-setup]';   // the menu's own classes and data hooks: none may be anywhere in the window
const MENU = HOOKS + ', textarea, select';   // inside the engraving card/section, any words box or drop-down would be the menu too (the window's message boxes are elsewhere)

(async () => {
  const fails = [];
  const check = (ok, msg) => { if (!ok) fails.push(msg); console.log((ok ? '  ok  ' : '  FAIL ') + msg); };

  // 4 · source (needs no browser)
  const src = f => fs.readFileSync(path.join(root, f), 'utf8');
  check(!/CharmNestEngraveSetup|CharmNestEngravePanel/.test(src('charm-nest-order-engraving.js')), 'order-engraving.js (the order window card) mounts no engraving menu');
  check(!/CharmNestEngraveSetup|CharmNestEngravePanel/.test(src('charm-nest-sheetwin.js')), 'sheetwin.js (the sheet window card) mounts no engraving menu');
  check(!/owEngSetup|swEngSetup/.test(src('charm-nest-bridge.js')), 'the bridge (order window Sheet-tab panel) no longer creates an owEngSetup host');

  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('no-menu-in-order-window: playwright-core not found, page checks skipped'); finish(); return; } }
  const srv = await start({ receipts: [] });
  const POOLID = KEY + '_1';
  srv.st.put('Charm_Nest_Sheets', 'gold-nm-1', { id: 'gold-nm-1', metal: 'gold', charms: [{ id: 'g0', poolId: POOLID, order: RID }], placements: [{ id: 'g0', cxPt: 40, cyPt: 40, angle: 0 }], poolIds: [POOLID], orders: [RID], sheetIndex: 1, fileBase: 'gold-nm-1', runId: 'run-test-1' });
  srv.st.put('Charm_Pool', POOLID, { poolId: POOLID, orderId: RID, sheetId: 'gold-nm-1', state: 'placed', material: 'gold' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 1 });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Engrave && window.OrderEngraving && window.CNEngravingSeals && window.CharmNestEngraveSetup && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // the order, read by the real intake (front + back), and its back job in review as Engrave holds it
    await page.evaluate(async ({ order, KEY }) => {
      const line = order.lines[0];
      const row = { key: KEY, order, line, arrivedAt: Date.now() - 3600e3, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [KEY + '_1'], engrave: null, material: null };
      B.orders.rows.push(row); B.orders.byKey.set(KEY, row);
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      row.spec.engraveCandidate = true;
      const j = Engrave.ensureJob(row); Object.assign(j, { state: 'review', text: 'Name Two', lines: ['Name Two'], fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } }, source: 'personalization', confidence: 0.9 });
      row.engrave = { needed: true, state: 'review', approved: false, text: j.text };
      Engrave.render = () => {};
    }, { order: ORDER, KEY });
    const control = await page.evaluate(KEY => { const r = B.orders.byKey.get(KEY); return { wants: !!CharmNestEngraveSetup.wants(r), jobs: Engrave.jobsOf(r).length }; }, KEY);
    check(control.wants && control.jobs >= 1, 'the order has engraving the menu would have shown for: ' + JSON.stringify(control));

    // 1 · the order window
    await page.evaluate(k => OrderWin.open(k), KEY);
    await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d && d.open; });
    await page.waitForFunction(() => { const h = document.getElementById('owEng'); return h && !h.hidden && h.children.length > 0; }, null, { timeout: 20000 }).catch(() => {});
    await page.waitForTimeout(600);
    const ow = await page.evaluate(({ sel, hooks }) => {
      const d = document.getElementById('orderWin'), h = document.getElementById('owEng'), sec = document.getElementById('owEngSec') || h;
      return { open: !!d && d.open, card: !!h && !h.hidden && h.children.length > 0, whole: d ? d.querySelectorAll(hooks).length : -1, menu: sec ? sec.querySelectorAll(sel).length : -1,
        label: d ? /(^|\s)engraves(\s|$)/i.test([...d.querySelectorAll('.egSuLbl, [data-engraves]')].map(x => x.textContent).join(' ')) : null,
        edit: sec ? [...sec.querySelectorAll('button')].filter(b => /^(edit|save|cancel)$/i.test(b.textContent.trim())).map(b => b.textContent.trim()) : [] };
    }, { sel: MENU, hooks: HOOKS });
    check(ow.open && ow.card, 'the order window is open and still draws the order\'s engraving card: ' + JSON.stringify({ open: ow.open, card: ow.card }));
    check(ow.whole === 0 && ow.menu === 0 && ow.label === false, 'the order window draws no ENGRAVES menu, no textarea, no font select in its engraving section: ' + JSON.stringify({ menuHooksInWindow: ow.whole, menuElementsInSection: ow.menu, engravesLabel: ow.label }));
    check(ow.edit.length === 0, 'and no Edit / Save / Cancel button in it: ' + JSON.stringify(ow.edit));

    // 1b · the order window's Sheet view (its panel carries the same engraving card)
    await page.evaluate(() => OrderWin.close && OrderWin.close()); await page.waitForTimeout(500);
    await page.evaluate(k => OrderWin.open(k, { view: 'sheet' }), KEY);
    await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d && d.open; });
    await page.waitForFunction(() => document.querySelector('#orderWin [data-engraving-panel] .swEng, #orderWin [data-engraving-panel] .owEngCard, #orderWin [data-engraving-panel] button'), null, { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(600);
    const sv = await page.evaluate(({ sel, hooks }) => { const d = document.getElementById('orderWin'), p = d && d.querySelector('[data-engraving-panel]'); return { panel: !!p, menu: p ? p.querySelectorAll(sel).length : -1, whole: d ? d.querySelectorAll(hooks).length : -1 }; }, { sel: MENU, hooks: HOOKS });
    check(sv.whole === 0 && (!sv.panel || sv.menu === 0), 'the order window\'s Sheet view draws no engraving menu' + (sv.panel ? '' : ' (its engraving panel is not drawn for this fixture: the window-wide hooks were checked)') + ': ' + JSON.stringify(sv));
    await page.evaluate(() => OrderWin.close && OrderWin.close()); await page.waitForTimeout(500);

    // 2 · the sheet window
    const sw = await page.evaluate(async ({ POOLID, sel }) => {
      try {
        await SheetWin.open('gold-nm-1', { select: POOLID });
        const until = async (f, ms) => { const t0 = Date.now(); while (Date.now() - t0 < ms) { const v = f(); if (v) return v; await new Promise(r => setTimeout(r, 100)); } return null; };
        const card = await until(() => document.querySelector('[data-r2=eng] .swEng'), 15000);
        await new Promise(r => setTimeout(r, 500));
        const host = document.querySelector('[data-r2=eng]');
        return { card: !!card, menu: host ? host.querySelectorAll(sel).length : -1 };
      } catch (e) { return { err: String(e && e.message || e) }; }
    }, { POOLID, sel: MENU });
    check(sw.card && sw.menu === 0, 'the sheet window draws its engraving card and no engraving menu: ' + JSON.stringify(sw));
    await page.evaluate(() => SheetWin.close && SheetWin.close()); await page.waitForTimeout(500);

    // 3 · the Engrave tab's placement card keeps the menu slot, filled (the one place the menu lives)
    const tab = await page.evaluate(KEY => {
      const row = B.orders.byKey.get(KEY), j = Engrave.jobOf(row, null, 'back');
      Object.assign(j, { state: 'review', approvedBy: null, approvedAt: null, decidedAt: null, decidedBy: null }); Object.assign(row.engrave, { state: 'review', approved: false, needed: true }); delete row.engravingOverride; CharmNestEngraveSetup.DRAFTS.clear();
      // (the Engrave tab builds its detail card with a context, placementCard(job, n, { counts, ears }); the Review tab's card is built without one and is NOT the Engrave detail: it has the slot but never fills it)
      const look = card => { const slot = card.querySelector('[data-engsetup]'); return { slot: !!slot, filled: !!slot && !slot.hidden && slot.children.length > 0, text: slot ? slot.textContent.replace(/\s+/g, ' ').trim().slice(0, 100) : '' }; };
      return { detail: look(Engrave.placementCard(j, 1, {})), review: look(Engrave.placementCard(j, 1)) };
    }, KEY).catch(e => ({ err: String(e.message || e) }));
    check(tab.detail && tab.detail.slot && tab.detail.filled, 'the Engrave tab\'s placement card mounts the menu in its own slot: ' + JSON.stringify(tab.detail || tab));
    check(tab.review && tab.review.slot && !tab.review.filled, 'and the Review tab\'s placement card (built without the Engrave detail\'s context) leaves that slot empty: ' + JSON.stringify(tab.review || tab));

    check(errors.length === 0, 'no page errors: ' + errors.join(' | ').slice(0, 300));
  } finally { await browser.close(); srv.close && srv.close(); }
  finish();

  function finish() {
    console.log(fails.length ? `no-menu-in-order-window: FAILED (${fails.length})` : 'no-menu-in-order-window: ok');
    process.exit(fails.length ? 1 : 0);
  }
})().catch(e => { console.error(e); process.exit(1); });
