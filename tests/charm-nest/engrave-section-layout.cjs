// The order window's Engraving section sits directly under the chip strip (Paul, 12 Oct 2026: "relocate the engravings section to be immediately below the menu", "sectioned off with a
// horizontal line ... the engraving title on the top left-hand corner ... the cards for each piece side-by-side ... at least three cards horizontally in a row ... without scrolling").
// The REAL page (charm-nest-1.html and its real stylesheet) in headless Chromium over the fake loopback site (tests/charm-nest/bridge-server.cjs); the real OrderEngraving draws a card for
// each engraved piece (Engrave jobs are made here by Engrave.ensureJob, the placement picture is a stand-in canvas). Every name is made up. Nothing real is read or written, no network.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/engrave-section-layout.cjs      (CHROMIUM=<chrome>, CN_SHOTS=<dir> keeps screenshots)
// What it proves (at 1280x720, 1024x768 and 1440x900, in the real stylesheet)
//   1 the section is in the centre column, right after the chip strip (#owMeta), at most 24 px below it; it is ruled off by a thin line; "Engraving" is its small title, at its top-left corner
//   2 an order of three pieces: three cards in ONE row (equal tops), side by side, no horizontal scroll anywhere; the first row starts above the fold with the order window not scrolled
//   3 one piece or two: the cards keep their third-of-the-row width, left-aligned (never stretched)
//   4 an order with nothing to engrave: no title, no line, no room
//   5 the old tall left-column card is gone (nothing of the section is in the picture column); the chip cards still hang over the section's cards
//   6 the section does not follow the piece switcher: picking a piece leaves every card where it is
// It does NOT prove the cards' own look (ENGCARD's) nor the strict whole-card-above-the-fold rule (ENGNOSCROLL's test): the card's bottom edge is printed, not asserted.
const path = require('path'), assert = require('assert/strict');
const W = require('./_orderFactsWorld.cjs');
const shots = process.env.CN_SHOTS || null;
let n = 0; const ok = (name, c) => { assert(c !== false, name); n++; console.log('  ✓ ' + name); };

/** what a drawn card looks like (the old card: .swEng inside .owEngCard; ENGCARD's: the .engCard itself) */
const CARD = '.swEng, .engCard, .owEngCard';
const PIECE = (sku, title) => ({ sku, title, variations: [{ name: 'Metal choice', value: 'Gold' }] });
const FX = {
  three: { rid: '4196000001', sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', buyer: 'Name One', placed: { day: 0, h: 11, m: 2 }, ship: 6, variations: [{ name: 'Metal choice', value: 'Gold' }], more: [PIECE('ASTER_FLOWER', 'Aster flower charm necklace'), PIECE('MINI_0445', 'Mini tag charm necklace')] },
  two: { rid: '4196000002', sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', buyer: 'Name Two', placed: { day: 0, h: 11, m: 5 }, ship: 6, variations: [{ name: 'Metal choice', value: 'Gold' }], more: [PIECE('ASTER_FLOWER', 'Aster flower charm necklace')] },
  one: { rid: '4196000003', sku: 'LEAF_CHARM', title: 'Pressed leaf charm necklace in 14k gold fill', buyer: 'Name Three', placed: { day: 0, h: 11, m: 9 }, ship: 6, variations: [{ name: 'Metal choice', value: 'Gold' }] },
  none: { rid: '4196000004', sku: 'PLAIN_TAG', title: 'Plain tag charm necklace in 14k gold fill', buyer: 'Name Four', placed: { day: 0, h: 11, m: 12 }, ship: 6, variations: [{ name: 'Metal choice', value: 'Gold' }] }
};
const ENGRAVED = ['three', 'two', 'one'];   // (the orders whose every line has a back engraving to approve)

async function seed(page) {
  await page.evaluate(async ({ fixtures, engraved, orderSrc }) => {
    const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
    await Orders.loadMaps(true);
    const art = text => { const c = document.createElement('canvas'); c.width = c.height = 300; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 300, 300); x.fillStyle = '#111'; x.font = '40px sans-serif'; x.textAlign = 'center'; x.fillText(text, 150, 150); return c; };
    Engrave.renderBack = job => art(job.text || '');
    const made = [];
    for (const [name, fx] of Object.entries(fixtures)) {
      const order = orderOf(fx, now);
      for (const line of order.lines) {
        const key = CharmNestOrders.lineKey(order, line);
        const row = { key, order, line, arrivedAt: now, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null };
        B.orders.rows.push(row); B.orders.byKey.set(key, row); if (engraved.includes(name)) made.push(key);
      }
    }
    Orders.interpretAll();
    let i = 0;
    for (const key of made) {
      const row = B.orders.byKey.get(key), words = 'Name ' + ['One', 'Two', 'Three', 'Four', 'Five', 'Six'][i++ % 6], j = Engrave.ensureJob(row);
      Object.assign(j, { state: 'review', text: words, lines: [words], fit: { size: 10, capMm: 2, centre: [0, 0], angle: 0, weight: 'Regular' }, view: {}, verify: { geometry: { ok: true } } });
      row.engrave = { needed: true, state: 'review', approved: false, text: words };
    }
    Engrave.render = () => {};   // (the Engraving tab itself is not under test)
    CN.setMode('orders'); Orders.render();
  }, { fixtures: FX, engraved: ENGRAVED, orderSrc: W.orderOf.toString() });
}
const idle = page => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 15000 });
async function open(page, name, cards) {
  await page.evaluate(rid => OrderWin.openOrder(rid), FX[name].rid);
  await page.waitForFunction(() => document.getElementById('orderWin').open && document.querySelector('#owMeta .owfBar'), null, { timeout: 15000 });
  if (cards) await page.waitForFunction(({ k, CARD }) => { const g = document.getElementById('owEng'), isCard = c => c.matches(CARD) || !!c.querySelector(CARD); return [...g.children].filter(c => !c.hidden && c.getClientRects().length && isCard(c)).length >= k && !g.querySelector('.owEngWait'); }, { k: cards, CARD }, { timeout: 15000 });
  await idle(page); await page.waitForTimeout(250);
}
const shut = async page => { await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d.open) d.dispatchEvent(new Event('cancel', { cancelable: true })); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); };
/** everything the checks read from the page, in one go (all in CSS px, window coordinates) */
const look = page => page.evaluate(() => {
  const box = e => { if (!e) return null; const r = e.getBoundingClientRect(); return { l: r.left, t: r.top, r: r.right, b: r.bottom, w: r.width, h: r.height }; };
  const sec = document.querySelector('#orderWin .engSec'), grid = document.getElementById('owEng'), meta = document.getElementById('owMeta'), hd = sec && sec.querySelector('.engSecHd'), main = document.querySelector('#orderWin .owMain');
  const cs = e => e && getComputedStyle(e);
  // a card slot = a direct child of the grid that holds a drawn card (a wait line is not one)
  const cells = grid ? [...grid.children].filter(c => !c.hidden && c.getClientRects().length && (c.matches('.swEng, .engCard, .owEngCard') || c.querySelector('.swEng, .engCard, .owEngCard'))).map(box) : [];
  return {
    vw: innerWidth, vh: innerHeight,
    secIn: !!sec && !!sec.closest('.owInfoMain'), secAfterMeta: !!sec && !!meta && meta.compareDocumentPosition(sec) === Node.DOCUMENT_POSITION_FOLLOWING && sec.parentElement === meta.parentElement,
    nextSibling: meta && meta.nextElementSibling === sec, sec: box(sec), meta: box(meta), grid: box(grid), hd: box(hd), hdText: hd && hd.textContent, hdDisplay: hd && cs(hd).display, hdFont: hd && parseFloat(cs(hd).fontSize),
    border: sec && cs(sec).borderTopWidth, borderStyle: sec && cs(sec).borderTopStyle, gridHidden: grid && grid.hidden, gridDisplay: grid && cs(grid).display, cols: grid && cs(grid).gridTemplateColumns.split(' ').length,
    cells, scrollTop: main && main.scrollTop, mainScrollW: main && main.scrollWidth, mainClientW: main && main.clientWidth, docScrollW: document.documentElement.scrollWidth,
    inPics: !!document.querySelector('#orderWin .owPics #owEng, #orderWin .owPics .swEng, #orderWin .owPics .owEngCard, #orderWin .owPics .engCard'), titles: [...document.querySelectorAll('#orderWin .engSecHd')].length,
    text: sec ? sec.innerText.replace(/\s+/g, ' ').trim() : ''
  };
});
const near = (a, b, e = 1.5) => Math.abs(a - b) <= e;

async function main() {
  const w = await W.world({ facts: true, viewport: { width: 1280, height: 720 }, scale: shots ? 2 : 1 });
  if (!w) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { page } = w;
  try {
    await page.waitForFunction(() => window.Engrave && window.OrderEngraving && window.CNEngravingSeals, null, { timeout: 30000 });
    await seed(page);
    const sizes = [{ width: 1280, height: 720 }, { width: 1024, height: 768 }, { width: 1440, height: 900 }];
    for (const vp of sizes) {
      const at = `${vp.width}x${vp.height}`;
      await page.setViewportSize(vp); await page.waitForTimeout(300);

      /* an order of three engraved pieces */
      await open(page, 'three', 3);
      let s = await look(page);
      assert(s.secIn && s.secAfterMeta, `${at}: the section is in the centre column after the chip strip`);
      assert(s.nextSibling, `${at}: it is the very next element after #owMeta`);
      const gap = s.sec.t - s.meta.b; assert(gap >= 0 && gap <= 24, `${at}: the section starts ${gap}px under the chip strip`);
      ok(`${at} · 1a the Engraving section starts ${Math.round(gap)} px under the chip strip, in the centre column (after #owMeta)`, true);
      assert(parseFloat(s.border) >= 1 && s.borderStyle === 'solid', `${at}: ruled off by a thin line`);
      assert.equal(s.hdText, 'Engraving'); assert(s.hdDisplay !== 'none'); assert(s.hdFont <= 12, `${at}: the title is small (${s.hdFont}px)`);
      assert(near(s.hd.l, s.sec.l, 1) && s.hd.t - s.sec.t <= 14, `${at}: the title is at the section's top-left corner`);
      assert(s.hd.t < s.cells[0].t, `${at}: the title is above the cards`);
      ok(`${at} · 1b a thin line (${s.border}), and "Engraving" small (${s.hdFont}px) at the top-left corner`, true);
      assert.equal(s.cells.length, 3, `${at}: three cards, got ${s.cells.length}`);
      assert(s.cells.every(c => near(c.t, s.cells[0].t, 1)), `${at}: the three cards share one row: ${s.cells.map(c => Math.round(c.t))}`);
      assert(s.cells[0].r <= s.cells[1].l + 1 && s.cells[1].r <= s.cells[2].l + 1, `${at}: side by side, none over another`);
      assert(s.cells.every(c => c.l >= s.sec.l - 1 && c.r <= s.sec.r + 1), `${at}: inside the section`);
      assert(s.cells.every(c => near(c.w, s.cells[0].w, 1.5)), `${at}: equal widths`);
      ok(`${at} · 2a three cards in ONE row, side by side, ${Math.round(s.cells[0].w)} px each (centre column ${Math.round(s.sec.w)} px)`, true);
      assert(s.mainScrollW <= s.mainClientW + 1 && s.docScrollW <= s.vw + 1, `${at}: nothing scrolls sideways (${s.mainScrollW}/${s.mainClientW}, doc ${s.docScrollW}/${s.vw})`);
      assert(s.cells.every(c => c.t >= 0 && c.t < s.vh - 60), `${at}: every card of the first row starts on the screen: tops ${s.cells.map(c => Math.round(c.t))} of ${s.vh}`);
      assert.equal(s.scrollTop, 0);
      const bottom = Math.max(...s.cells.map(c => c.b));
      ok(`${at} · 2b the first row starts at y=${Math.round(s.cells[0].t)} of ${s.vh} with the window not scrolled, no sideways scroll (card bottom ${Math.round(bottom)}px: ${bottom <= s.vh ? 'whole row visible' : 'the strict whole-card rule is the no-scroll test\'s'})`, true);
      assert(s.cols >= 3, `${at}: at least three columns`); ok(`${at} · 2c the grid has ${s.cols} columns`, true);
      assert(!s.inPics, `${at}: nothing of the section in the picture column`);
      ok(`${at} · 5a the old tall left-column card is gone: nothing engraving-like in the picture column`, true);
      if (shots) await page.screenshot({ path: path.join(shots, `ENGSEC-three-${at}.png`) });

      /* the section does not follow the piece switcher */
      const keys = await page.evaluate(() => [...document.querySelectorAll('#owPcSum .owPcRow[data-piece]')].map(r => r.dataset.piece));
      if (keys.length >= 3) {
        const before = (await look(page)).cells.map(c => Math.round(c.l)).join();
        await page.evaluate(k => OrderWin.selectPiece(k), keys[1]); await page.waitForTimeout(500); await idle(page);
        const after = await look(page);
        assert.equal(after.cells.length, 3, `${at}: still three cards after picking a piece`); assert.equal(after.cells.map(c => Math.round(c.l)).join(), before, `${at}: the cards stay where they were`);
        await page.evaluate(() => OrderWin.selectPiece(null)); await page.waitForTimeout(300);
        ok(`${at} · 6 picking one piece on the list leaves all three cards where they are`, true);
      }

      /* a chip's card hangs over the section's cards, not under them */
      if (vp.width === 1280) {
        await page.evaluate(() => { document.querySelector('#orderWin .owMain').scrollTop = 0; });
        await page.click('[data-owf-chip=order]'); await page.waitForTimeout(350);
        const pop = await page.evaluate(() => { const p = document.querySelector('#owMeta .owfPop'); if (!p || p.hidden) return null; const r = p.getBoundingClientRect(), g = document.getElementById('owEng').getBoundingClientRect(); const x = r.left + r.width / 2, y = Math.min(r.bottom - 6, Math.max(r.top + 6, g.top + 20)); const hit = document.elementFromPoint(x, y); return { overlaps: r.bottom > g.top + 8, onTop: !!hit && !!hit.closest('.owfPop') }; });
        assert(pop, 'the order chip opens its card'); assert(!pop.overlaps || pop.onTop, 'the chip card is above the section\'s cards where they overlap');
        ok(`${at} · 5b the chip's card ${pop.overlaps ? 'overlaps the first row and stays on top of it' : 'opens clear of the cards'}`, true);
        await page.keyboard.press('Escape'); await page.waitForTimeout(150);
      }

      /* two pieces, one piece: never stretched */
      await shut(page); await open(page, 'two', 2);
      s = await look(page);
      assert.equal(s.cells.length, 2); assert(s.cells.every(c => near(c.t, s.cells[0].t, 1)), `${at}: two cards share a row`);
      assert(s.cells.every(c => c.w <= s.grid.w / 3 + 1), `${at}: two cards, each at most a third of the row: ${s.cells.map(c => Math.round(c.w))} of ${Math.round(s.grid.w)}`);
      assert(near(s.cells[0].l, s.grid.l, 1), `${at}: left-aligned`);
      await shut(page); await open(page, 'one', 1);
      const o = await look(page);
      assert.equal(o.cells.length, 1); assert(o.cells[0].w <= o.grid.w / 3 + 1 && near(o.cells[0].l, o.grid.l, 1), `${at}: one card is a third of the row, left-aligned: ${Math.round(o.cells[0].w)} of ${Math.round(o.grid.w)}`);
      assert(near(o.cells[0].w, s.cells[0].w, 1.5), `${at}: a card is the same width alone, in two and in three`);
      ok(`${at} · 3 one and two cards keep their third-of-the-row width (${Math.round(o.cells[0].w)} px of ${Math.round(o.grid.w)}), left-aligned`, true);

      /* nothing to engrave: no title, no line, no room */
      await shut(page); await open(page, 'none', 0);
      s = await look(page);
      assert(s.gridHidden, `${at}: the grid is hidden`); assert.equal(s.hdDisplay, 'none', `${at}: the title is not shown`);
      assert(s.sec.h <= 1 && parseFloat(s.border) === 0, `${at}: the section takes no room and draws no line (${s.sec.h}px, border ${s.border})`);
      assert.equal(s.text, '', `${at}: no word of the section is on screen`);
      ok(`${at} · 4 an order with nothing to engrave shows no title, no line and takes no room`, true);
      await shut(page);
    }

    /* the wide window: more columns, the same rule (4 from 840px, 5 from 1060px of section) */
    await page.setViewportSize({ width: 1920, height: 1080 }); await page.waitForTimeout(300);
    await open(page, 'three', 3); const wide = await look(page);
    assert(wide.cols >= 4 && wide.cells.length === 3 && wide.cells.every(c => near(c.t, wide.cells[0].t, 1)), `1920: ${wide.cols} columns, the three cards in one row`);
    assert(wide.cells.every(c => c.w <= wide.grid.w / 4 + 1), '1920: no card is stretched past its track');
    ok(`1920x1080 · 2d the section is ${Math.round(wide.sec.w)} px wide: ${wide.cols} columns, cards ${Math.round(wide.cells[0].w)} px`, true);
    await shut(page);

    assert.deepEqual(w.errors, [], 'no page errors'); assert.deepEqual(w.warns, [], 'no error swallowed by the order window');
    ok('7 no script error on the page throughout', true);
    console.log(`engrave-section-layout: ${n} checks passed`);
  } finally { await w.close(); }
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
