// A small world for the order window's facts (OrderFacts): the real sorter page in headless Chromium over the fake site (bridge-server.cjs), with a handful of
// ordinary orders put into the Orders tab. Every buyer name here is made up. Used by order-facts-browser.cjs; it writes nothing anywhere real.
const path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

/** order specs: times are relative to NOW in the PAGE's own zone (hh:mm of a day offset), so "Today 11:02 AM" is what the page really says */
const FIXTURES = {
  single: { rid: '4195000011', sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', buyer: 'Alder Fixture', placed: { day: 0, h: 11, m: 2 }, ship: 6, variations: [{ name: 'Metal choice', value: 'Gold' }, { name: 'Chain length', value: '18 inches' }] },
  pairMatching: { rid: '4195000012', sku: 'MASKS HAPPY/SAD', title: 'Theatre Happy Sad Drama Mask Earrings available in 14k Gold Fill, Sterling Silver, 14k Rose Gold Fill or 14K Solid Gold Earrings', buyer: 'Birch Sample', placed: { day: 0, h: 11, m: 2 }, ship: 7, listing: '1737560072', variations: [{ name: 'Metal choice', value: 'Gold' }, { name: 'Style', value: 'Stud earrings' }] },
  mismatched: { rid: '4195000013', sku: 'MISMATCHED_7134', title: 'Mismatched mitten earrings, one of each', buyer: 'Cedar Example', placed: { day: -1, h: 16, m: 15 }, ship: 3, variations: [{ name: 'Metal choice', value: 'Silver' }, { name: 'Style', value: 'Stud earrings' }] },
  // an order of several charms: a necklace charm and an earring pair (two lines, three pieces), one buyer, one order
  multi: { rid: '4195000014', sku: 'COMEDY 89578', title: 'Comedy and tragedy necklace, one charm', buyer: 'Dune Placeholder', placed: { day: -3, h: 9, m: 40 }, ship: 1, variations: [{ name: 'Metal choice', value: 'Gold' }],
    more: [{ sku: 'MASKS HAPPY/SAD', title: 'Theatre Happy Sad Drama Mask Earrings', variations: [{ name: 'Metal choice', value: 'Gold' }, { name: 'Style', value: 'Stud earrings' }] }] },
  qty3: { rid: '4195000018', sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', buyer: 'Hazel Standin', placed: { day: -1, h: 18, m: 45 }, ship: 6, qty: 3, variations: [{ name: 'Metal choice', value: 'Gold' }] },
  engraved: { engrave: { needed: true, state: 'waiting' }, rid: '4195000015', sku: 'MINI_0445', title: 'Mini tag necklace, front and back engraved', buyer: 'Elm Dummy', placed: { day: 0, h: 8, m: 30 }, ship: 5, variations: [{ name: 'Metal choice', value: 'Gold' }, { name: 'Charm Type', value: 'Tag1 (front+back)' }, { name: 'Font', value: 'Ace' }], personalization: ['front - W / back - 1. 27. 24'] },
  late: { rid: '4195000016', sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', buyer: 'Fir Testcase', placed: { day: -9, h: 13, m: 5 }, ship: -3, variations: [{ name: 'Metal choice', value: 'Gold' }] },
  message: { rid: '4195000017', sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', buyer: 'Gum Specimen', placed: { day: -2, h: 10, m: 10 }, ship: 4, gift: true, buyerMessage: 'Hello! This is a surprise for my sister, could it arrive before her birthday on the 20th? Thank you so much.', giftMessage: 'Happy birthday, love from all of us.', variations: [{ name: 'Metal choice', value: 'Gold' }] }
};

/** the fixture as an Etsy-shaped order (times in seconds) */
function orderOf(fx, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(fx.ship, 17, 0), tid = fx.rid + '1';
  return { receiptId: fx.rid, orderNumber: fx.rid, createTs: at(fx.placed.day, fx.placed.h, fx.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: fx.buyer }, buyerMessage: fx.buyerMessage || '', isGift: !!fx.gift, giftMessage: fx.giftMessage || '', staffNote: '', messages: [],
    lines: [{ transactionId: tid, listingId: fx.listing || ('18' + fx.rid.slice(-6)), sku: fx.sku, title: fx.title, quantity: fx.qty || 1, expectedShipDate: shipBy, variations: fx.variations || [], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: fx.personalization || [], buyerMessage: fx.buyerMessage || '' }]
      .concat((fx.more || []).map((m, i) => ({ transactionId: fx.rid + (i + 2), listingId: '19' + fx.rid.slice(-6) + i, sku: m.sku, title: m.title, quantity: m.qty || 1, expectedShipDate: shipBy, variations: m.variations || [], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: [], buyerMessage: '' }))) };
}

async function world(opts = {}) {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { return null; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const context = await browser.newContext({ viewport: opts.viewport || { width: 1440, height: 900 }, reducedMotion: opts.reducedMotion || 'no-preference', deviceScaleFactor: opts.scale || 1, hasTouch: !!opts.touch, isMobile: false });
  await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
  await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
  const page = await context.newPage(), errors = [], warns = [];
  page.on('console', m => { if (m.type() === 'warning' && /^order view:/.test(m.text())) warns.push(m.text()); });   // (OrderWin's tryDo swallows an error with this warning: none may)
  page.setDefaultTimeout(30000);
  page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
  await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
  await page.waitForFunction(needFacts => window.CN && window.Orders && window.OrderWin && (window.OrderFacts || !needFacts) && CN.S.cloud.ok === true, opts.facts !== false, { timeout: 60000 });
  return { srv, browser, context, page, errors, warns, async close() { await browser.close(); await srv.close(); } };
}

/** put the fixtures into the Orders tab as pulled lines (the page builds each line's spec from them) */
async function seed(page, names) {
  await page.evaluate(async ({ fixtures, orderSrc }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
    await Orders.loadMaps(true);
    for (const fx of fixtures) {
      const order = orderOf(fx, now);
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: fx.engrave && line === order.lines[0] ? Object.assign({}, fx.engrave) : null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    }
    Orders.interpretAll(); CN.setMode('orders'); Orders.render();
  }, { fixtures: names.map(n => FIXTURES[n]), orderSrc: orderOf.toString() });
}
const keyOf = n => `${FIXTURES[n].rid}_${FIXTURES[n].rid}1`;
module.exports = { world, seed, FIXTURES, keyOf, orderOf, root };
