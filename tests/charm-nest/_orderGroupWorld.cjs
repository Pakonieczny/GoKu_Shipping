// ENGONE (engone-1013): a small world for the Orders tab's ONE-ORDER-ONE-ROW grouping. The real sorter page in headless Chromium over the fake site (_orderFactsWorld.cjs),
// with orders put into the Orders tab as pulled lines through the real intake. Every buyer and every word here is made up ("Name One"); nothing is written anywhere real.
//   const G = require('./_orderGroupWorld.cjs'); const w = await G.open({ width: 1700 }); await G.seed(w.page, ['single','twoPiece','pair','three']);
'use strict';
const Base = require('./_orderFactsWorld.cjs');

const V = (name, value) => ({ name, value });
const METAL = 'Metal Choice / Engraving Option';
/** An order is { rid, buyer, placed:{day,h,m}, ship (days), lines:[{ sku, title, qty, listing, variations, personalization }] }. Times are relative to NOW in the page's own zone. */
const ORDERS = {
  // one Etsy order, two lines (two metals) of the same design, the same made-up words: Paul's 13 Oct image
  twoPiece: { rid: '4196987621', buyer: 'Name One', placed: { day: 0, h: 16, m: 38 }, ship: 6, lines: [
    { sku: 'NEURON_37061', title: 'Neuron Necklace, Silver or Gold Engraved Charm', listing: '1890000111', variations: [V(METAL, 'Silver + Engrave'), V('Length', '18"')], personalization: ['Name One'] },
    { sku: 'NEURON_37061', title: 'Neuron Necklace, Silver or Gold Engraved Charm', listing: '1890000111', variations: [V(METAL, '14k Gold + Engrave'), V('Length', '16"')], personalization: ['Name One'] }] },
  // three lines of three different charms, different words on each
  three: { rid: '4196987633', buyer: 'Name Two', placed: { day: -1, h: 9, m: 5 }, ship: 3, lines: [
    { sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', listing: '1890000221', variations: [V('Metal choice', 'Gold'), V('Chain length', '18 inches')], personalization: [] },
    { sku: 'COMEDY 89578', title: 'Comedy and tragedy necklace, one charm', listing: '1890000222', variations: [V('Metal choice', 'Silver')], personalization: ['Name Two'] },
    { sku: 'MINI_0445', title: 'Mini tag necklace, front and back engraved', listing: '1890000223', qty: 2, variations: [V('Metal choice', 'Gold')], personalization: ['back - Name Two'] }] },
  // one line (unchanged: a row as it always was)
  single: { rid: '4196987644', buyer: 'Name Three', placed: { day: 0, h: 11, m: 2 }, ship: 6, lines: [
    { sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', listing: '1890000331', variations: [V('Metal choice', 'Gold'), V('Chain length', '18 inches')], personalization: [] }] },
  // an order that is an earring pair (ONE line, two pieces: Left and Right) plus a necklace line
  pair: { rid: '4196987655', buyer: 'Name Four', placed: { day: -2, h: 15, m: 20 }, ship: 2, lines: [
    { sku: 'MASKS HAPPY/SAD', title: 'Theatre Happy Sad Drama Mask Earrings', listing: '1737560072', variations: [V('Metal choice', 'Gold'), V('Style', 'Stud earrings')], personalization: [] },
    { sku: 'COMEDY 89578', title: 'Comedy and tragedy necklace, one charm', listing: '1890000442', variations: [V('Metal choice', 'Gold')], personalization: [] }] },
  // an earring pair line alone (a pair row: one line)
  pairOnly: { rid: '4196987666', buyer: 'Name Five', placed: { day: -2, h: 10, m: 0 }, ship: 4, lines: [
    { sku: 'MASKS HAPPY/SAD', title: 'Theatre Happy Sad Drama Mask Earrings', listing: '1737560072', variations: [V('Metal choice', 'Gold'), V('Style', 'Stud earrings')], personalization: [] }] },
  // a second two-line order (another buyer), for counts and filters
  twoMore: { rid: '4196987677', buyer: 'Name Six', placed: { day: -3, h: 8, m: 15 }, ship: 5, lines: [
    { sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', listing: '1890000331', variations: [V('Metal choice', 'Gold')], personalization: [] },
    { sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', listing: '1890000331', variations: [V('Metal choice', 'Silver')], personalization: [] }] }
};
const keyOf = (name, i = 0) => `${ORDERS[name].rid}_${ORDERS[name].rid}${i + 1}`;

/** the order as an Etsy-shaped order (times in seconds) */
function orderOf(o, now) {
  const at = (d, h, m) => { const t = new Date(now); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return Math.floor(t.getTime() / 1000); };
  const shipBy = at(o.ship, 17, 0);
  return { receiptId: o.rid, orderNumber: o.rid, createTs: at(o.placed.day, o.placed.h, o.placed.m), updateTs: at(0, 12, 0), shipBy, buyer: { name: o.buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [],
    lines: o.lines.map((l, i) => ({ transactionId: o.rid + (i + 1), listingId: l.listing, sku: l.sku, title: l.title, quantity: l.qty || 1, expectedShipDate: shipBy, variations: l.variations || [], metalKey: /silver/i.test(JSON.stringify(l.variations)) ? 'silver' : 'gold', metalLabel: /silver/i.test(JSON.stringify(l.variations)) ? 'SS' : 'GF 14/20', personalization: l.personalization || [], buyerMessage: '' })) };
}

async function open(opts = {}) {
  // (opts.real: the master library holds real charm drawings (_engraveRealWorld.cjs), so a design such as COMET_9427 draws its vector picture; for screenshots)
  const w = opts.real ? await require('./_engraveRealWorld.cjs').world({ viewport: { width: opts.width || 1700, height: opts.height || 900 } }) : await Base.world({ viewport: { width: opts.width || 1700, height: opts.height || 900 }, reducedMotion: opts.reducedMotion });
  if (!w) return null;
  if (opts.sandbox) {   // (the Sandbox workspace: the same page code over the sandbox's own collections, as _reviewSendWorld.cjs opens it)
    await w.page.evaluate(() => { const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); Object.assign(s, { sandbox: 'on', sandboxStream: 'off', pollOrders: 'off' }); localStorage.setItem('cn.settings', JSON.stringify(s)); localStorage.removeItem('cn.sandboxHold'); });
    await w.page.reload({ waitUntil: 'load' });
    await w.page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Review && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await w.page.waitForFunction(() => !window.Session || typeof Session.ready !== 'function' || Session.ready() === true || (Session.failure && Session.failure()), null, { timeout: 60000 });
    await w.page.evaluate(async () => { await Orders.loadMaps(true); try { localStorage.removeItem('cn.sandboxHold'); } catch (_) {} });
    if (!(await w.page.evaluate(() => !!WORKSPACE_SANDBOX))) throw new Error('the page is not in the sandbox');
  }
  w.foreign = []; w.page.on('request', r => { if (!/^(http:\/\/(127\.0\.0\.1|localhost)[:/]|data:|blob:)/.test(r.url()) && !/fonts\.(googleapis|gstatic)/.test(r.url())) w.foreign.push(r.url()); });
  return w;
}
/** Pulled lines through the real intake; the Orders tab shown. `names` = keys of ORDERS. */
async function seed(page, names) {
  await page.evaluate(async ({ orders, orderSrc }) => {
    const mk = eval('(' + orderSrc + ')'), now = Date.now();
    await Orders.loadMaps(true);
    for (const o of orders) {
      const order = mk(o, now);
      for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
    }
    Orders.interpretAll(); CN.setMode('orders'); Orders.render();
  }, { orders: names.map(n => ORDERS[n]), orderSrc: orderOf.toString() });
}
/** The Orders list as it is drawn now: each top-level entry of #ordItems with its order, its lines (pieces) and what it shows. */
const entries = page => page.evaluate(() => [...document.querySelectorAll('#ordItems > [data-rid]')].map(n => {
  const grouped = n.classList.contains('ordGroup'), pieces = grouped ? [...n.querySelectorAll('.ogPiece')] : [n];
  return { rid: n.dataset.rid, grouped, keys: pieces.map(p => p.dataset.key), nums: [...n.querySelectorAll('.onum')].length, buttons: n.querySelectorAll('.revSendBtn').length, text: n.innerText.replace(/\s+/g, ' ').trim() };
}));
module.exports = { open, seed, entries, ORDERS, keyOf, orderOf, Base };
