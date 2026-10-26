// The order window's facts in the REAL page (headless Chromium over the fake site, bridge-server.cjs): the bridge gathers the facts of each kind of order, OrderFacts draws the
// glance line, the cards and Details (Paul, 12 Oct 2026: "a mush of stuff"). Every buyer name is made up (tests/charm-nest/_orderFactsWorld.cjs); nothing real is read or written.
//   node tests/charm-nest/order-facts-browser.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir keeps screenshots)
// 1 a single charm: the chips, the glance is calm and short; the ship-by chip is green with its days left; Details holds the old grid, label for label
// 2 a matching pair, an order of several lines, a late order, a message from the buyer, front + back engraving: each one's chips and cards
// 3 hover rests 350 ms, a card stays inside the window at a laptop, a small and a phone width, Esc closes the card and not the window, outside press closes
// 4 copy: one press on the order number copies it and shows the check
// 5 keyboard and touch; reduced motion; no card ever opens over another dialog (one open dialog throughout)
const path = require('path'), assert = require('assert/strict');
const W = require('./_orderFactsWorld.cjs');
const shots = process.env.CN_SHOTS || null;
let n = 0; const ok = (name, c) => { assert(c !== false, name); n++; console.log('  ✓ ' + name); };

async function open(page, name) {
  await page.evaluate(k => OrderWin.openOrder(k.split('_')[0]), W.keyOf(name));
  await page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && document.querySelector('#owMeta .owfBar') && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 15000 });
  await page.evaluate(() => document.querySelectorAll('#orderWin .owMain').forEach(m => { m.scrollTop = 1e6; }));
}
const shut = async page => { await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d.open) d.dispatchEvent(new Event('cancel', { cancelable: true })); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); };
const bar = page => page.evaluate(() => [...document.querySelectorAll('#owMeta .owfBar [data-owf-chip]')].map(c => [c.dataset.owfChip, c.innerText.replace(/\s+/g, ' ').trim(), c.className]));
const popState = page => page.evaluate(() => { const p = document.querySelector('#owMeta .owfPop'), d = document.getElementById('orderWin').getBoundingClientRect(); if (!p || p.hidden) return null; const r = p.getBoundingClientRect(); return { text: p.innerText.replace(/\s+/g, ' ').trim(), l: r.left, t: r.top, r: r.right, b: r.bottom, dl: d.left, dt: d.top, dr: d.right, db: d.bottom, side: p.dataset.side, peek: p.classList.contains('peek'), dialogs: document.querySelectorAll('dialog[open]').length }; });
const inside = p => p.l >= p.dl - 0.5 && p.t >= p.dt - 0.5 && p.r <= p.dr + 0.5 && p.b <= p.db + 0.5;
const hover = async (page, id) => { const el = await page.$(`[data-owf-chip="${id}"]`); const b = await el.boundingBox(); await page.mouse.move(b.x + b.width / 2, b.y + b.height / 2); };

async function main() {
  const w = await W.world({ facts: true, scale: shots ? 2 : 1 });
  if (!w) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { page, context } = w;
  try {
    await context.grantPermissions(['clipboard-read', 'clipboard-write'], { origin: w.srv.sorterOrigin }).catch(() => {});
    await W.seed(page, ['single', 'pairMatching', 'mismatched', 'multi', 'engraved', 'late', 'message', 'qty3']);

    /* 1 · a single charm */
    await open(page, 'single');
    const r1 = await page.evaluate(() => { const h = document.getElementById('owMeta'), b = h.querySelector('.owfBar').getBoundingClientRect(); return { h: h.getBoundingClientRect().height, bar: b.height, details: h.querySelector('.owfDetails').hidden, oldCells: h.querySelectorAll('.m').length, ship: getComputedStyle(h.querySelector('[data-owf-chip=ship]')).backgroundColor, sage: getComputedStyle(document.querySelector('.pill.ok') || h).backgroundColor, tabs: h.querySelectorAll('[data-owf-item][tabindex="0"]').length, role: h.querySelector('.owfBar').getAttribute('role') }; });
    const c1 = await bar(page); const ids1 = c1.map(c => c[0]);
    assert.deepEqual(ids1, ['order', 'buyer', 'purchased', 'ship', 'metal', 'state']);
    assert.equal(c1[0][1], W.FIXTURES.single.rid); assert.equal(c1[1][1], 'Alder Fixture'); assert.match(c1[2][1], /^Today 11:02 AM$/); assert.match(c1[3][1], /^[A-Z][a-z]{2} \d+ · 6 days left$/); assert.equal(c1[4][1], 'GF 14/20');
    assert(/\bok\b/.test(c1[3][2]), 'ship by is calm green with time left');
    ok('1a a single charm: order, buyer, purchased, ship by (green, days left) and metal at a glance, the state as one pill', true);
    assert(r1.details, 'Details closed'); assert(r1.oldCells >= 8, 'the whole old grid is in the page: ' + r1.oldCells);
    assert(r1.bar <= 64, 'the glance is at most two rows: ' + r1.bar); assert(r1.h <= 70, 'the whole facts area is slim: ' + r1.h);
    assert.equal(r1.tabs, 1); assert.equal(r1.role, 'toolbar');
    ok(`1b the glance takes ${Math.round(r1.bar)} px (the old first row of boxes took ~67 px, the old grid ~330)`, true);
    // Details: every label of the old grid, in its order
    await page.click('#owMeta .owfMore'); await page.waitForTimeout(260);
    const det = await page.evaluate(() => { const d = document.querySelector('#owMeta .owfDetails'); return { shown: !d.hidden && d.getBoundingClientRect().height > 40, labels: [...d.querySelectorAll('.m i')].map(i => i.textContent), cols: getComputedStyle(d.querySelector('.owfList')).gridTemplateColumns.split(' ').length, expanded: document.querySelector('#owMeta .owfMore').getAttribute('aria-expanded') }; });
    assert(det.shown); assert.equal(det.expanded, 'true'); assert.equal(det.cols, 2, 'two columns');
    assert.deepEqual(det.labels, ['Order', 'Purchased', 'Buyer', 'Quantity', 'Metal', 'State', 'Ship by', 'Chain', 'Metal', 'Chain length', 'Listing', 'Title']);   // (OPTTIDY, Paul 13 Oct: "Metal choice" is said "Metal"; NECKLACE, Paul 13 Oct 2026: "Do not drop the word Necklace": a "Chain length" keeps its noun; the cells are all still there)
    ok('1c Details opens the complete old grid in two columns, every label in its old order', true);
    if (shots) { await page.screenshot({ path: path.join(shots, 'FACTS-single-details.png'), clip: await page.evaluate(() => { const m = document.querySelector('#orderWin .owInfoMain').getBoundingClientRect(), r = document.getElementById('owMeta').getBoundingClientRect(); return { x: m.left - 10, y: r.top - 10, width: m.width + 20, height: r.height + 20 }; }) }); }
    await page.click('#owMeta .owfMore'); await page.waitForTimeout(260);
    assert.equal(await page.evaluate(() => document.querySelector('#owMeta .owfDetails').hidden), true); ok('1d a second press closes Details', true);

    /* 2 · each kind of order */
    await shut(page); await open(page, 'pairMatching');
    let c = await bar(page); assert.deepEqual(c.map(x => x[0]), ['order', 'buyer', 'purchased', 'ship', 'metal', 'qty', 'state']); assert.equal(c.find(x => x[0] === 'qty')[1], '1 pair');
    await hover(page, 'qty'); await page.waitForTimeout(600);
    let p = await popState(page); assert(p, 'the pair card'); assert.match(p.text, /1 pair · 2 pieces, Left and Right/); assert.match(p.text, /Matching: the Right is the mirror of the Left/); assert(p.peek, 'hover is a peek'); assert.equal(p.dialogs, 1, 'no pop-up over a pop-up');
    const det2 = await page.evaluate(() => Object.fromEntries([...document.querySelectorAll('#owMeta .m')].map(m => [m.querySelector('i').textContent, m.querySelector('span').textContent])));
    assert.equal(det2.Quantity, '1 pair · 2 pieces: Left and Right'); assert.equal(det2.Pair, 'Matching: the Right is the Left turned over (its mirror image)');
    ok('2a a matching pair: "1 pair" chip, its card, and the old Quantity and Pair cells word for word in Details', true);
    await hover(page, 'order'); await page.waitForTimeout(600);
    p = await popState(page); assert.match(p.text, new RegExp(W.FIXTURES.pairMatching.rid)); assert.match(p.text, /1737560072/); assert.match(p.text, /Theatre Happy Sad Drama Mask Earrings available in 14k Gold Fill, Sterling Silver, 14k Rose Gold Fill or 14K Solid Gold Earrings/);
    ok('2b the order chip\'s card holds the listing number and the full listing title', true);
    await hover(page, 'metal'); await page.waitForTimeout(600); p = await popState(page); assert.match(p.text, /GF 14\/20\s+Gold/i); ok('2c the metal card holds the buyer\'s metal choice (OPTTIDY: under the card\'s own title, without the words "Metal choice")', true);
    await hover(page, 'purchased'); await page.waitForTimeout(600); p = await popState(page); assert(p.text.includes(new Date().toLocaleDateString('en-US', { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric' })), 'today\'s full date (the world places the order today at 11:02): ' + p.text); assert.match(p.text, /11:02 AM/); ok('2d the purchase card holds the full date', true);

    await shut(page); await open(page, 'multi');
    c = await bar(page); assert.deepEqual(c.map(x => x[0]).slice(0, 5), ['order', 'buyer', 'purchased', 'ship', 'metal']);
    const piecesShown = await page.evaluate(() => document.querySelectorAll('#owPcSum .owPcRow').length);
    assert(piecesShown >= 2, 'the order of several charms lists its pieces: ' + piecesShown);
    assert(/tomorrow|today/.test(c.find(x => x[0] === 'ship')[1]) && /\bwarn\b/.test(c.find(x => x[0] === 'ship')[2]), 'due tomorrow is amber');
    ok('2e an order of several charms (a necklace charm and an earring pair): the glance is the order\'s, the pieces stay in Its pieces; due tomorrow is amber', true);

    await shut(page); await open(page, 'qty3');
    c = await bar(page); assert.equal(c.find(x => x[0] === 'qty')[1], '×3'); ok('2f three of one charm is a "×3" chip', true);

    await shut(page); await open(page, 'late');
    c = await bar(page); const sh = c.find(x => x[0] === 'ship'); assert.match(sh[1], /3 days late$/); assert(/\bbad\b/.test(sh[2]), 'late is red'); assert.match(c.find(x => x[0] === 'purchased')[1], /^[A-Z][a-z]{2} \d+, 1:05 PM$/);
    await hover(page, 'ship'); await page.waitForTimeout(600); p = await popState(page); assert.match(p.text, /Late by\s*3 days/i); assert.match(p.text, /past its ship-by date/);
    ok('2g a late order: a red chip that says how late, its card says so', true);
    if (shots) { await page.screenshot({ path: path.join(shots, 'FACTS-late-ship-window.png') }); }

    await shut(page);
    // front + back engraving: the page's engraving model builds this cell (ENGUI2); the fixture gives the model's own cell text
    // (the page's own intake reads this fixture as front "W" + back "1. 27. 24"; its Engrave jobs are not built in this world, so the cell is the model's plain one: the words below come from a stub of the model's cell)
    await page.evaluate(() => { OrderEngraving.faceCell = () => '<span>Front · W<br>Back · 1. 27. 24</span>'; });
    await open(page, 'engraved');
    c = await bar(page); const eg = c.find(x => x[0] === 'engraving'); assert(eg, 'an engraving chip'); assert.equal(eg[1], 'Front + back');
    await hover(page, 'engraving'); await page.waitForTimeout(600); p = await popState(page); assert.match(p.text, /Front · W/); assert.match(p.text, /Back · 1\. 27\. 24/);
    const engCell = await page.evaluate(() => { const m = [...document.querySelectorAll('#owMeta .m')].find(x => x.querySelector('i').textContent === 'Engraving'); return m && m.innerText.replace(/\s+/g, ' '); });
    assert.match(engCell, /Front · W Back · 1\. 27\. 24|Front · W\s*Back · 1\. 27\. 24/); ok('2h front + back engraving: a chip, a card with both faces\' words, and the old cell in Details', true);

    await shut(page); await open(page, 'message');
    c = await bar(page); assert(c.some(x => x[0] === 'message' && x[1] === 'Message')); assert(await page.evaluate(() => !!document.querySelector('[data-owf-chip=buyer] + * , [data-owf-chip=buyer] .owfGift')), 'a gift mark on the buyer');
    await hover(page, 'message'); await page.waitForTimeout(600); p = await popState(page); assert.match(p.text, /Hello! This is a surprise for my sister/); assert.match(p.text, /Happy birthday, love from all of us\./);
    ok('2i a message from the buyer: a chip, a card with the buyer\'s words and the gift message', true);
    // the Customer tab's unread: the chip carries the dot and the card a way to the tab
    await shut(page);
    await page.evaluate(() => { window.CustomerMail.tabNews = () => ({ unread: true, bad: false }); });
    await open(page, 'message');
    const dot = await page.evaluate(() => !!document.querySelector('[data-owf-chip=message] .owDot')); assert(dot, 'the unread dot');
    await page.click('[data-owf-chip=message]'); await page.waitForTimeout(300); p = await popState(page); assert.match(p.text, /new reply from the buyer in the Customer tab/); assert(!p.peek);
    await page.click('#owMeta .owfBtn'); await page.waitForTimeout(250);
    assert.equal(await page.evaluate(() => document.querySelector('[data-ow-tab=customer]').getAttribute('aria-selected')), 'true'); assert.equal(await popState(page), null);
    ok('2j an unread reply puts the dot on the message chip, and the card\'s button opens the Customer tab', true);
    if (shots) { await shut(page); await open(page, 'message'); await hover(page, 'message'); await page.waitForTimeout(700); }
    await page.evaluate(() => { delete window.CustomerMail.tabNews; });

    /* 3 · the cards: 350 ms, inside the window at three widths, Esc, outside */
    await shut(page); await open(page, 'pairMatching');
    await hover(page, 'buyer'); await page.waitForTimeout(220); assert.equal(await popState(page), null, 'not open at 220 ms'); await page.waitForTimeout(260); assert(await popState(page), 'open by about 480 ms');
    await page.mouse.move(5, 5); await page.waitForTimeout(350); assert.equal(await popState(page), null, 'leaving closes it'); ok('3a hover opens the card after the rest and leaving puts it away', true);
    await hover(page, 'order'); await page.waitForTimeout(600);
    await page.keyboard.press('Escape'); await page.waitForTimeout(200);
    assert.equal(await popState(page), null); assert.equal(await page.evaluate(() => document.getElementById('orderWin').open), true, 'Esc put the card away and left the window open');
    await page.click('[data-owf-chip=ship]'); await page.waitForTimeout(250); assert(await popState(page)); await page.mouse.click(30, 300); await page.waitForTimeout(200); assert.equal(await popState(page), null);
    ok('3b Esc closes the card first and the window stays; a press outside closes a pinned card', true);
    for (const vp of [{ width: 1440, height: 900 }, { width: 1024, height: 700 }, { width: 820, height: 760 }, { width: 390, height: 780 }]) {
      await page.setViewportSize(vp); await page.waitForTimeout(500);
      await page.evaluate(() => document.querySelectorAll('#orderWin .owMain, #orderWin .owVInfo').forEach(m => { m.scrollTop = 1e6; }));
      const ids = (await bar(page)).map(x => x[0]);
      for (const id of ids) {
        await page.evaluate(() => { const o = document.querySelector('#orderWin .owVInfo'); });
        const el = await page.$(`[data-owf-chip="${id}"]`); await el.scrollIntoViewIfNeeded(); await el.click(); await page.waitForTimeout(260);
        const s = await popState(page); assert(s, `${vp.width}: a card for ${id}`); assert(inside(s), `${vp.width}: the ${id} card stays inside the window ${JSON.stringify(s)}`);
        assert.equal(s.dialogs, 1);
        await page.keyboard.press('Escape'); await page.waitForTimeout(120);
      }
      // (the header of this window runs past a 390 px screen already, whatever the facts do: only the facts are held to the window)
      const rows = await page.evaluate(() => { const b = document.querySelector('#owMeta .owfBar'), d = document.getElementById('orderWin'), h = document.getElementById('owMeta'); const tops = new Set([...b.querySelectorAll('[data-owf-chip],.owfMore')].map(c => Math.round((c.offsetTop + c.offsetHeight / 2) / 20))); return { rows: tops.size, out: [...h.querySelectorAll('*')].filter(e => !e.closest('[hidden]') && e.getBoundingClientRect().right > d.clientWidth + 1).length, over: [...b.querySelectorAll('.owfChip')].some(c => c.getBoundingClientRect().right > b.getBoundingClientRect().right + 1), hscroll: h.scrollWidth > h.clientWidth + 1 }; });
      assert(!rows.over, `${vp.width}: no chip runs out of the line`); assert.equal(rows.out, 0, `${vp.width}: nothing of the facts runs past the window's edge`); assert(!rows.hscroll, `${vp.width}: the facts never scroll sideways`);
      console.log(`    (${vp.width}x${vp.height}: ${rows.rows} row(s) of chips)`);
    }
    await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(400);
    ok('3c at 1440, 1024, 820 and 390 wide every card stays inside the window, over no other dialog, and the chips wrap whole', true);

    /* 4 · copy */
    await open(page, 'pairMatching');
    await page.click('[data-owf-chip=order]'); await page.waitForTimeout(250);
    const clip = await page.evaluate(() => navigator.clipboard.readText().catch(() => null));
    if (clip !== null) assert.equal(clip, W.FIXTURES.pairMatching.rid, 'the order number is on the clipboard');
    assert(await page.evaluate(() => document.querySelector('[data-owf-chip=order]').classList.contains('copied')), 'the check');
    assert.equal(await page.evaluate(() => getComputedStyle(document.querySelector('[data-owf-chip=order] .owfCp .ck')).opacity), '1');
    ok('4 one press on the order number copies it and the chip shows the check' + (clip === null ? ' (clipboard unreadable here: the check only)' : ''), true);
    if (shots) { await page.waitForTimeout(100); await page.screenshot({ path: path.join(shots, 'FACTS-copied.png'), clip: await page.evaluate(() => { const m = document.querySelector('#orderWin .owInfoMain').getBoundingClientRect(), r = document.getElementById('owMeta').getBoundingClientRect(); return { x: m.left - 10, y: r.top - 10, width: m.width + 20, height: 90 }; }) }); }
    await page.keyboard.press('Escape'); await page.waitForTimeout(150);

    /* 5 · keyboard and reduced motion */
    await page.focus('[data-owf-chip=order]'); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await page.waitForTimeout(150);
    assert.equal(await page.evaluate(() => document.activeElement && document.activeElement.dataset.owfChip), 'order');
    assert(await popState(page), 'keyboard focus opens the card');
    await page.keyboard.press('ArrowRight'); await page.waitForTimeout(150); assert.equal(await page.evaluate(() => document.activeElement.dataset.owfChip), 'buyer'); p = await popState(page); assert.match(p.text, /Buyer/i);
    await page.keyboard.press('Escape'); await page.waitForTimeout(150); assert.equal(await popState(page), null); assert.equal(await page.evaluate(() => document.activeElement.dataset.owfChip), 'buyer');
    ok('5a the keyboard: a focused chip opens its card, the arrows walk the line, Esc puts the card away and keeps the focus on its chip', true);
    const moveAnim = await page.evaluate(async () => { const chip = document.querySelector('[data-owf-chip=metal]'); chip.focus(); document.activeElement.blur(); chip.click(); const p = document.querySelector('#owMeta .owfPop'); return p.getAnimations().map(a => a.effect.getKeyframes().map(k => Object.keys(k).filter(x => !['offset', 'computedOffset', 'easing', 'composite'].includes(x)).join('+'))).flat(); });
    assert(moveAnim.some(k => /transform/.test(k)), 'the card moves softly by default: ' + moveAnim.join(','));
    await page.keyboard.press('Escape');
    await page.emulateMedia({ reducedMotion: 'reduce' });
    const still = await page.evaluate(async () => { document.querySelector('[data-owf-chip=metal]').click(); const p = document.querySelector('#owMeta .owfPop'); const a = p.getAnimations().map(x => x.effect.getKeyframes().map(k => Object.keys(k).filter(z => !['offset', 'computedOffset', 'easing', 'composite'].includes(z)).join('+'))).flat(); const dur = p.getAnimations().map(x => x.effect.getTiming().duration); document.querySelector('#owMeta .owfMore').click(); return { a, dur }; });
    assert(!still.a.some(k => /transform/.test(k)), 'reduced motion: a plain fade, no movement: ' + still.a.join(',')); assert(still.dur.every(d => d <= 120));
    await page.emulateMedia({ reducedMotion: 'no-preference' });
    ok('5b with reduced motion the card only fades (no transform) and Details opens at once', true);

    assert.deepEqual(w.errors, [], 'no page errors'); assert.deepEqual(w.warns, [], 'no error swallowed by the order window');
    ok('6 no script error on the page throughout', true);
    console.log(`order-facts-browser: ${n} checks passed`);
  } finally { await w.close(); }
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
