// Where the "Details" toggle sits in the order window's facts line, in the REAL page (headless Chromium over the fake site, bridge-server.cjs).
// Paul, 12 Oct 2026: "Relocate the Details dropdown menu button to always be on the first line to the right side of the SS sheet button."
//   node tests/charm-nest/order-details-position.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir keeps screenshots)
// Every buyer name is made up. Orders are the fixtures of _orderFactsWorld.cjs, and facts painted straight into the open window's own host (so the real CSS and
// the real window width apply); nothing real is read or written. Without playwright-core the browser checks are skipped (the DOM-order checks stay in order-facts.cjs).
// At 390, 768, 1280 and 1920 px, for an order with a metal chip (short and long), with a long buyer name, with no metal chip, late, with many later chips:
//   1 Details is the very next item after the metal chip (after the ship-by chip when there is no metal chip), on the same line, to its right, a few px away
//   2 when the strip is wide enough for the shortened first row, Details is on LINE 1 (the order number's line), the later chips (engraving ...) follow it or wrap below
//   3 nothing runs out of the strip or the window; the DOM order (the Tab and arrow order) is the visual order
//   4 a cut buyer name says its full name in its title when the pointer reaches it; a chip that fits has no title
//   5 Details still opens and closes the grid, from where it now sits
const path = require('path'), assert = require('assert/strict');
const W = require('./_orderFactsWorld.cjs');
const shots = process.env.CN_SHOTS || null;
let n = 0; const ok = (name, c) => { assert(c !== false, name); n++; console.log('  ✓ ' + name); };

async function open(page, name) {
  await page.evaluate(k => OrderWin.openOrder(k.split('_')[0]), W.keyOf(name));
  await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && document.querySelector('#owMeta .owfBar .owfMore') && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 15000 });
  await page.evaluate(() => document.querySelectorAll('#orderWin .owMain').forEach(m => { m.scrollTop = 1e6; }));
  await page.waitForTimeout(250);
}
const shut = async page => { await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d.open) d.dispatchEvent(new Event('cancel', { cancelable: true })); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); };

/** facts painted into the open window's own #owMeta, then the line measured, in ONE synchronous step (no repaint can come between) */
const paintAndMeasure = (page, c) => page.evaluate(c => {
  const later = c.later || [], now = Date.now();
  const f = {
    key: 'dp-' + c.id, loading: false, on: {}, cells: [{ label: 'Order', text: '4200898392' }], order: { id: '4200898392', num: '' }, buyer: { name: c.buyer, gift: false },
    purchased: { ts: now - 3600e3, label: 'Purchased' }, ship: { by: Math.floor(now / 1000) + c.ship * 86400, quiet: false }, metal: c.metal ? { label: c.metal, color: '#9aa3b5', none: false, choices: [] } : null,
    qty: later.includes('qty') ? { n: 1, pieces: 2, pair: true, mismatched: false, counted: null, notes: [] } : null,
    custom: later.includes('custom') ? { label: 'Custom: names', note: '' } : null,
    engraving: later.includes('engraving') ? { chip: 'Engraving', html: '<span>x</span>', approved: false } : null,
    message: later.includes('message') ? { buyer: 'hello', gift: '', unread: 1, bad: false } : null, listing: { id: '1', title: 't' }, state: null
  };
  OrderFacts.paint(document.getElementById('owMeta'), f);
  return measure();
  function measure() {
    const host = document.getElementById('owMeta'), bar = host.querySelector('.owfBar'), b = bar.getBoundingClientRect(), d = document.getElementById('orderWin').getBoundingClientRect();
    const items = [...bar.querySelectorAll('[data-owf-item]')].map(el => { const r = el.getBoundingClientRect(); return { id: el.dataset.owfChip || (el.classList.contains('owfMore') ? 'details' : '?'), top: r.top, left: r.left, right: r.right, w: r.width }; });
    return { items, bar: { l: b.left, r: b.right, w: b.width }, win: { l: d.left, r: d.right }, hscroll: host.scrollWidth > host.clientWidth + 1 };
  }
}, c);
const measureNow = page => page.evaluate(() => {
  const host = document.getElementById('owMeta'), bar = host.querySelector('.owfBar'), b = bar.getBoundingClientRect(), d = document.getElementById('orderWin').getBoundingClientRect();
  const items = [...bar.querySelectorAll('[data-owf-item]')].map(el => { const r = el.getBoundingClientRect(); return { id: el.dataset.owfChip || (el.classList.contains('owfMore') ? 'details' : '?'), top: r.top, left: r.left, right: r.right, w: r.width }; });
  return { items, bar: { l: b.left, r: b.right, w: b.width }, win: { l: d.left, r: d.right }, hscroll: host.scrollWidth > host.clientWidth + 1 };
});

/** the checks 1 to 3 for one measured line; `must` makes line 1 a must (the order that is Paul's own) */
function check(label, m, c, must) {
  const ids = m.items.map(i => i.id), at = ids.indexOf('details'), it = id => m.items.find(i => i.id === id);
  assert(at > 0, `${label}: Details is there: ${ids}`);
  const prev = m.items[at - 1], wantPrev = c.metal === null ? 'ship' : 'metal';
  assert.equal(prev.id, wantPrev, `${label}: the item before Details is the ${wantPrev} chip: ${ids}`);
  assert.deepEqual(ids.slice(0, at - 1), ['order', 'buyer', 'purchased', 'ship'].slice(0, wantPrev === 'ship' ? 3 : 4), `${label}: the order's own chips come first, in order`);
  const dt = it('details'), gap = dt.left - prev.right;
  assert(Math.abs(dt.top - prev.top) <= 1, `${label}: Details is on the line of the ${wantPrev} chip (${dt.top} vs ${prev.top})`);
  assert(gap >= -0.5 && gap <= 14, `${label}: Details is right of the ${wantPrev} chip and next to it (gap ${gap})`);
  // line 1: possible when the never-cut chips (order, time, metal, Details) and the two that give way, at their smallest, fit
  const gapsN = (m.items.filter(i => i.id !== 'details' && ['order', 'buyer', 'purchased', 'ship', 'metal'].includes(i.id)).length) * 6;   // (6 px between the row's items, Details included)
  const need = it('order').w + it('purchased').w + (it('metal') ? it('metal').w : 0) + dt.w + 64 + (it('ship') ? 88 : 0) + gapsN;
  const line1 = Math.abs(dt.top - it('order').top) <= 1;
  if (must || m.bar.w >= need + 4) assert(line1, `${label}: Details is on LINE 1 (strip ${Math.round(m.bar.w)} px, the shortened row needs ${Math.round(need)}): Details top ${dt.top}, order top ${it('order').top}`);
  for (const l of ids.slice(at + 1)) { const x = it(l); assert(x.top > dt.top + 1 || x.left >= dt.right - 0.5, `${label}: the later chip ${l} follows Details, on its line or below it`); }
  for (const x of m.items) assert(x.left >= m.bar.l - 1 && x.right <= m.bar.r + 1, `${label}: ${x.id} stays inside the strip (${Math.round(x.left)}..${Math.round(x.right)} in ${Math.round(m.bar.l)}..${Math.round(m.bar.r)})`);
  assert(!m.hscroll, `${label}: the facts never scroll sideways`);
  // the Tab / arrow order (the DOM order) is the visual order: reading by line, then left to right, gives the same list
  const read = m.items.slice().sort((a, b) => (Math.round(a.top / 12) - Math.round(b.top / 12)) || a.left - b.left).map(i => i.id);
  assert.deepEqual(read, ids, `${label}: the DOM (Tab and arrow) order is the visual order`);
  return { line1, rows: new Set(m.items.map(i => Math.round(i.top / 12))).size, need: Math.round(need) };
}

const CASES = [
  { id: 'paul', buyer: 'Alder Fixture', metal: 'SS', ship: 5, later: ['engraving'], must: true },           // the order of Paul's picture: five chips, SS, then Engraving
  { id: 'long', buyer: 'Bartholomew Fictional-Surname the Third of Nowhere in Particular', metal: 'GF 14/20', ship: 6, later: ['qty', 'engraving', 'message'] },
  { id: 'nometal', buyer: 'Birch Sample', metal: null, ship: 2, later: ['engraving'] },
  { id: 'late', buyer: 'Cedar Example', metal: 'SS', ship: -3, later: ['qty', 'custom', 'engraving', 'message'] }
];

async function main() {
  const w = await W.world({ facts: true, scale: shots ? 2 : 1 });
  if (!w) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { page } = w;
  try {
    await W.seed(page, ['single', 'engraved', 'pairMatching']);
    const seen = [];
    for (const width of [390, 768, 1280, 1920]) {
      await page.setViewportSize({ width, height: 900 }); await page.waitForTimeout(150);
      await open(page, 'single');
      for (const c of CASES) {
        const m = await paintAndMeasure(page, c), r = check(`${width}px ${c.id}`, m, c, c.must && width >= 768);
        seen.push(`${width}px ${c.id}: strip ${Math.round(m.bar.w)} px, ${r.rows} row(s), Details ${r.line1 ? 'on line 1' : 'beside the ' + (c.metal === null ? 'ship-by' : 'metal') + ' chip, line ' + (1 + Math.round((m.items.find(i => i.id === 'details').top - m.items[0].top) / 28))}`);
        if (shots && c.id === 'paul') await (await page.$('#owMeta')).screenshot({ path: path.join(shots, `DETAILSPOS-after-${width}.png`) });
      }
      // the real orders through the real bridge (metal GF 14/20, the engraving chip behind Details)
      await shut(page);
      for (const name of ['single', 'engraved', 'pairMatching']) {
        await open(page, name);
        const m = await measureNow(page); check(`${width}px real ${name}`, m, { metal: 'x' }, false);
        await shut(page);
      }
    }
    ok('1-3 at 390, 768, 1280 and 1920 px, for 4 made-up orders and 3 fixture orders: Details is the next item after the metal chip (after the ship-by chip with no metal chip), on its line and just right of it; on line 1 whenever the shortened first row fits; the later chips follow it; nothing runs out of the strip; Tab order is visual order', true);
    seen.forEach(s => console.log('    ' + s));

    // 4 · the cut buyer name says itself
    await page.setViewportSize({ width: 768, height: 900 }); await page.waitForTimeout(150);
    await open(page, 'single');
    await paintAndMeasure(page, CASES[1]);
    const cut = await page.evaluate(() => { const c = document.querySelector('#owMeta [data-owf-chip=buyer]'), t = c.querySelector('.owfTx'); return { cut: t.scrollWidth > t.clientWidth + 1, title: c.getAttribute('title'), full: t.textContent }; });
    assert(cut.cut, 'the long buyer name is cut with an ellipsis at 768 px'); assert.equal(cut.title, null, 'no title before the pointer is there');
    const buyerEl = await page.$('#owMeta [data-owf-chip=buyer]'); await buyerEl.scrollIntoViewIfNeeded(); const buyerBox = await buyerEl.boundingBox(); await page.mouse.move(buyerBox.x + buyerBox.width / 2, buyerBox.y + buyerBox.height / 2); await page.waitForTimeout(120);
    const t1 = await page.evaluate(() => ({ title: document.querySelector('#owMeta [data-owf-chip=buyer]').getAttribute('title'), order: document.querySelector('#owMeta [data-owf-chip=order]').getAttribute('title') }));
    assert.equal(t1.title, cut.full, 'the cut name is the chip\'s title'); assert.equal(t1.order, null, 'a chip that fits has no title');
    await page.mouse.move(2, 2); await page.keyboard.press('Escape');
    await page.setViewportSize({ width: 1920, height: 900 }); await page.waitForTimeout(150);
    await paintAndMeasure(page, CASES[0]);
    const bEl = await page.$('#owMeta [data-owf-chip=buyer]'); await bEl.scrollIntoViewIfNeeded(); const bb = await bEl.boundingBox(); await page.mouse.move(bb.x + bb.width / 2, bb.y + bb.height / 2); await page.waitForTimeout(120);
    assert.equal(await page.evaluate(() => document.querySelector('#owMeta [data-owf-chip=buyer]').getAttribute('title')), null, 'a buyer name that fits has no title');
    ok('4 a cut buyer name says its full name in its title once the pointer is on it; a chip that fits has none', true);
    await page.mouse.move(2, 2); await shut(page);

    // 5 · Details still works from where it sits, at a phone width too
    for (const width of [390, 1280]) {
      await page.setViewportSize({ width, height: 900 }); await page.waitForTimeout(150);
      await open(page, 'engraved');
      const before = await page.evaluate(() => document.querySelector('#owMeta .owfDetails').hidden);
      await (await page.$('#owMeta .owfMore')).scrollIntoViewIfNeeded(); await page.click('#owMeta .owfMore'); await page.waitForTimeout(260);
      const op = await page.evaluate(() => { const d = document.querySelector('#owMeta .owfDetails'); return { hidden: d.hidden, h: d.getBoundingClientRect().height, exp: document.querySelector('#owMeta .owfMore').getAttribute('aria-expanded') }; });
      assert(before && !op.hidden && op.h > 40 && op.exp === 'true', `${width}: Details opens the grid`);
      await page.click('#owMeta .owfMore'); await page.waitForTimeout(260);
      assert.equal(await page.evaluate(() => document.querySelector('#owMeta .owfDetails').hidden), true, `${width}: a second press closes it`);
      await shut(page);
    }
    ok('5 Details opens and closes the grid from its new place (1280 and 390 px)', true);
    assert.deepEqual(w.errors, [], 'no page errors'); assert.deepEqual(w.warns, [], 'no error swallowed by the order window');
    ok('6 no script error on the page throughout', true);
    console.log(`order-details-position: ${n} checks passed`);
  } finally { await w.close(); }
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
