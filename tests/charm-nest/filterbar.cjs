// FILTERBAR (Paul, 17 Oct 2026): "This much is much too crowded. Consolidate this entire menu into the smallest and most efficient footprint so it's easier to use and understand all of the options. Make it Beautiful and
// tastefully looking so it's a pleasure to use easy to click easy to understand and real-time reactions with spinner/loader when waiting."
// The Orders tab's bar: the REAL page over the repo's fake site, with 48 seeded orders of several metals, kinds and engraving (no Etsy, nothing live). What it proves:
//   1  the bar is one row at a wide window, never scrolls sideways at 1500 / 1024 / 760 / 400 / 320 px, and holds no <select> (the three selects, the date chips and both sort selects are gone)
//   2  Filters: one button with a count badge; its panel holds Metal and Engraved as chip groups (no Kind: Paul, 18 Oct 2026) with every answer the old selects offered (and their counts); Esc closes it and gives the focus back; Clear
//   3  a chip narrows the list at once: the live count ("12 of 48 orders"), the gold chip under the bar (one press removes it), the badge; Clear all
//   4  the date menu (all five ranges, the one in force named on its button) and the sort control (all five keys, one arrow reverses the order, keys and direction really reorder the list)
//   5  Cards | List (and S.settings.orderView), search with its ×, Esc in the box, Enter
//   6  the real-time feedback: a change shows aria-busy on the list, a spinner on the control that was touched and the new count BEFORE the list has been redrawn, and all of it ends
//   7  the Cancelled pile steps the open-lines controls aside and keeps the date menu and the direction
//   8  phone width: every target is at least 40 px, the Filters panel is a bottom sheet inside the window, nothing overflows
//   node tests/charm-nest/filterbar.cjs
'use strict';
const path = require('path');
const { world, seed, FIXTURES } = require('./_orderFactsWorld.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };

const METALS = ['Gold', 'Silver', 'Rose Gold', 'Gold', 'Silver', 'Gold'], FORMS = ['Necklace', 'Stud earrings', 'Huggie earrings', 'Bracelet', 'Necklace', 'Charm only'];
for (let i = 0; i < 40; i++) FIXTURES['x' + i] = { rid: String(4195100000 + i), sku: 'LEAF_CHARM', title: 'Pressed leaf charm ' + FORMS[i % 6].toLowerCase(), buyer: 'Buyer ' + i, placed: { day: -(i % 12), h: 9 + (i % 8), m: (i * 7) % 60 }, ship: 2 + (i % 9), variations: [{ name: 'Metal choice', value: METALS[i % 6] }, { name: 'Style', value: FORMS[i % 6] }], engrave: i % 3 === 0 ? { needed: true, state: 'waiting' } : undefined };

(async () => {
  const w = await world({ viewport: { width: 1500, height: 900 }, facts: false });
  if (!w) { console.log('SKIP: no browser (set PW_DIR and CHROMIUM)'); return; }
  const { page } = w;
  const sleep = ms => page.waitForTimeout(ms);
  try {
    await seed(page, Object.keys(FIXTURES).filter(k => k.startsWith('x')).concat(['single', 'pairMatching', 'mismatched', 'multi', 'qty3', 'engraved', 'late', 'message']));
    await page.evaluate(() => {
      const MS = ['gold', 'silver', 'rose', 'gold10k', 'gold14k', 'gold'], FS = ['necklace', 'earrings', 'huggie', 'bracelet', 'charm', 'keychain'];
      Orders.rows().forEach((r, i) => { r.material = MS[i % 6]; if (r.spec) r.spec.form = FS[(i >> 1) % 6]; });
      CNListActivity.set('orders', { range: 'all', direction: 'desc' }); Orders.view().sort = 'activity'; Orders.view().desc = false; Orders.render();
    });
    await sleep(600);
    const total = await page.evaluate(() => document.querySelector('#ordChips [data-pile=""] b').textContent);

    // ═══ 1 · one row, no selects, no sideways scroll ═══
    const shape = () => page.evaluate(() => { const b = document.getElementById('ordBar'); const tops = [...b.children].filter(c => c.getBoundingClientRect().height).map(c => Math.round(c.getBoundingClientRect().top)).sort((x, y) => x - y); let rows = tops.length ? 1 : 0; for (let i = 1; i < tops.length; i++) if (tops[i] - tops[i - 1] > 14) rows++; return { rows, h: Math.round(b.getBoundingClientRect().height), selects: b.querySelectorAll('select').length, page: document.documentElement.scrollWidth - document.documentElement.clientWidth, bar: b.scrollWidth - b.clientWidth, sandbox: !!b.querySelector('#sandboxBar') }; });
    let s = await shape();
    check(s.rows === 1 && s.selects === 0 && s.page <= 0 && s.bar <= 1, '1500 px: one row, no select, no sideways scroll: ' + JSON.stringify(s));
    check(s.h <= 44, 'and it is slim: ' + s.h + ' px tall (it was a 33 px row that scrolled sideways under 2000 px)');
    const ids = await page.evaluate(() => ['ordBar', 'ordQ', 'ordPull', 'ordChips', 'ordBody', 'ordSort', 'ordViewSeg', 'ordFiltersBtn', 'ordFiltersPop', 'ordCount', 'ordCharmTotal', 'ordActive'].filter(i => !document.getElementById(i)));
    check(ids.length === 0, 'the ids the page and the tests use are all there (ordBar ordQ ordPull ordChips ordBody ordSort ordViewSeg + the new ones): missing ' + ids.join());

    // ═══ 2 · the Filters panel ═══
    const fb = '#ordFiltersBtn', pop = '#ordFiltersPop';
    check(await page.locator(fb).getAttribute('aria-expanded') === 'false' && await page.locator(pop).isHidden(), 'Filters is closed to begin with');
    await page.click(fb); await page.waitForSelector(pop + ':not([hidden])');
    check(await page.locator(fb).getAttribute('aria-expanded') === 'true', 'a press opens it (aria-expanded)');
    const inPanel = await page.evaluate(() => { const p = document.getElementById('ordFiltersPop'); return { focus: p.contains(document.activeElement), groups: [...p.querySelectorAll('.ordFg h4')].map(h => h.textContent), metals: [...p.querySelectorAll('.ordChip[data-f="metal"]')].map(c => c.dataset.v + ':' + (c.querySelector('small') || {}).textContent), forms: [...p.querySelectorAll('.ordChip[data-f="form"]')].map(c => c.dataset.v + ':' + (c.querySelector('small') || {}).textContent), eng: [...p.querySelectorAll('.ordChip[data-f="eng"]')].map(c => c.dataset.v + ':' + (c.querySelector('small') || {}).textContent), done: p.querySelector('[data-done]').textContent, clearOff: p.querySelector('[data-clear]').disabled, inside: (r => r.left >= 0 && r.right <= innerWidth && r.bottom <= innerHeight)(p.getBoundingClientRect()) }; });
    check(inPanel.focus, 'the focus goes into the panel');
    check(inPanel.groups.join() === 'Metal,Engraved', 'the panel has Metal and Engraved, no Kind: ' + inPanel.groups);
    check(['gold', 'silver', 'rose', 'gold10k', 'gold14k'].every(m => inPanel.metals.some(x => x.startsWith(m + ':'))) && inPanel.metals[0].startsWith(':'), 'every metal the old select offered, with its count, and Any first: ' + inPanel.metals);
    check(inPanel.forms.length === 0, 'no Kind chips (Necklaces, Charms only, Huggies…): ' + inPanel.forms);
    const lines = await page.evaluate(() => Orders.rows().filter(r => r.state !== 'gone').length);
    check(inPanel.eng.length === 3 && inPanel.eng[1].startsWith('yes:') && inPanel.eng[2].startsWith('no:') && +inPanel.eng[1].split(':')[1] + +inPanel.eng[2].split(':')[1] === lines, 'Engraved / Not engraved split the tab\'s lines (' + lines + '), as the old select did: ' + inPanel.eng);
    check(inPanel.clearOff && /^Show \d+ orders$/.test(inPanel.done) && inPanel.inside, 'Clear is off while nothing narrows; the button says what it shows (' + inPanel.done + '); the panel is inside the window');
    await page.keyboard.press('Escape'); await sleep(100);
    check(await page.locator(pop).isHidden() && await page.evaluate(() => document.activeElement && document.activeElement.id) === 'ordFiltersBtn', 'Esc closes it and the focus is back on the Filters button');
    await page.focus(fb); await page.keyboard.press('Enter'); await page.waitForSelector(pop + ':not([hidden])');
    check(true, 'Enter on the focused button opens it too');
    await page.keyboard.press('ArrowRight'); await page.keyboard.press('Escape'); await sleep(80);

    // ═══ 3 · a chip narrows the list at once ═══
    await page.click(fb); await page.waitForSelector(pop + ':not([hidden])');
    await page.click(pop + ' .ordChip[data-f="metal"][data-v="silver"]');
    const quick = await page.evaluate(() => ({ count: document.getElementById('ordCount').textContent, on: document.getElementById('ordCount').hasAttribute('data-on'), busy: document.getElementById('ordBody').getAttribute('aria-busy'), spin: !!document.querySelector('#ordFiltersBtn .spin'), badge: document.querySelector('#ordFiltersBtn .cnBadge').textContent, chips: [...document.querySelectorAll('#ordActive .ordAx')].map(x => x.textContent.trim()), pressed: document.querySelector('#ordFiltersPop .ordChip[data-f="metal"][data-v="silver"]').getAttribute('aria-pressed'), shown: document.querySelectorAll('#ordItems > *').length }));
    check(/^\d+ of \d+ orders$/.test(quick.count) && quick.on && quick.count.startsWith(quick.count.split(' ')[0]) && +quick.count.split(' ')[0] < +total, 'at once: the live count says what is left: "' + quick.count + '"');
    check(quick.busy === 'true' && quick.spin, 'at once: the list is aria-busy and the Filters button shows the spinner (the list has not been redrawn yet)');
    check(quick.badge === '1' && quick.chips.length === 1 && /SS/.test(quick.chips[0]) && quick.pressed === 'true', 'at once: badge 1, the gold chip "' + quick.chips + '" under the bar, the chip pressed');
    await sleep(700);
    const settled = await page.evaluate(() => ({ busy: document.getElementById('ordBody').hasAttribute('aria-busy'), spin: document.querySelectorAll('#ordBar .spin').length, bar: document.getElementById('ordBar').hasAttribute('data-busy'), mats: [...new Set(Orders.view && Orders.walkRows().map(r => r.material))], n: new Set(Orders.walkRows().map(r => r.order.receiptId)).size, done: document.querySelector('#ordFiltersPop [data-done]').textContent }));
    check(!settled.busy && settled.spin === 0 && !settled.bar, 'then the busy state ends: no aria-busy, no spinner, no data-busy');
    check(settled.mats.join() === 'silver' && settled.done === 'Show ' + settled.n + ' orders', 'and the list is only Silver (' + settled.n + ' orders); the panel button says "' + settled.done + '"');
    await page.click(pop + ' .ordChip[data-f="eng"][data-v="yes"]'); await sleep(600);
    const three = await page.evaluate(() => ({ badge: document.querySelector('#ordFiltersBtn .cnBadge').textContent, chips: [...document.querySelectorAll('#ordActive .ordAx')].map(x => x.dataset.ax), all: !!document.querySelector('#ordActive [data-ax-all]'), on: document.getElementById('ordFiltersBtn').hasAttribute('data-on'), vis: Orders.walkRows().every(r => r.material === 'silver' && r.engrave && r.engrave.needed) }));
    check(three.badge === '2' && three.chips.join() === 'metal,eng' && three.all && three.on && three.vis, 'two filters: badge 2, two chips and Clear all, the Filters button gold, and every line shown passes both: ' + JSON.stringify(three));
    await page.keyboard.press('Escape'); await sleep(100);
    await page.click('#ordActive [data-ax="eng"]'); await sleep(500);
    const afterX = await page.evaluate(() => ({ chips: [...document.querySelectorAll('#ordActive .ordAx')].map(x => x.dataset.ax), eng: Orders.view().eng, badge: document.querySelector('#ordFiltersBtn .cnBadge').textContent, all: !!document.querySelector('#ordActive [data-ax-all]') }));
    check(afterX.chips.join() === 'metal' && afterX.eng === null && afterX.badge === '1' && !afterX.all, 'one press on a chip lets go of that filter only (one chip left, so no Clear all): ' + JSON.stringify(afterX));
    // (Clear all is there only for two or more: Engraved goes back on, then Clear all lets go of both)
    await page.click(fb); await page.click(pop + ' .ordChip[data-f="eng"][data-v="yes"]'); await sleep(500); await page.keyboard.press('Escape'); await sleep(100);
    await page.click('#ordActive [data-ax-all]'); await sleep(500);
    const cleared = await page.evaluate(() => ({ hidden: document.getElementById('ordActive').hidden, v: Orders.view(), count: document.getElementById('ordCount').hasAttribute('data-on'), badge: document.querySelector('#ordFiltersBtn .cnBadge').hidden, range: CNListActivity.state('orders').range }));
    check(cleared.hidden && !cleared.v.metal && !cleared.v.form && !cleared.v.eng && !cleared.count && cleared.badge && cleared.range === 'all', 'Clear all: no chip row, no badge, nothing narrowed');
    await page.click(fb); await page.click(pop + ' .ordChip[data-f="metal"][data-v="rose"]'); await sleep(500);
    check(await page.evaluate(() => !document.querySelector('#ordFiltersPop [data-clear]').disabled), 'Clear in the panel is on while a filter is');
    await page.click(pop + ' [data-clear]'); await sleep(500);
    check(await page.evaluate(() => !Orders.view().metal && document.querySelector('#ordFiltersBtn .cnBadge').hidden), 'and Clear lets go of them all');
    await page.click(pop + ' .ordChip[data-f="metal"][data-v="rose"]'); await sleep(300); await page.click(pop + ' .ordChip[data-f="metal"][data-v="rose"]'); await sleep(300);
    check(await page.evaluate(() => !Orders.view().metal), 'a chip that is on, pressed again, goes off');
    await page.keyboard.press('Escape'); await sleep(100);

    // ═══ 4 · the date menu and the sort control ═══
    const dateBtn = '#ordersView .cnDateBtn';
    check(/Any time/.test(await page.textContent(dateBtn)), 'the date control says "Any time" while no range is in force');
    await page.click(dateBtn); await page.waitForSelector('#ordersView .cnDate .cnMenu:not([hidden])');
    const dm = await page.evaluate(() => ({ items: [...document.querySelectorAll('#ordersView .cnDate [data-range]')].map(b => b.dataset.range + '=' + b.textContent.trim()), checked: document.querySelector('#ordersView .cnDate [aria-checked=true]').dataset.range, focus: document.activeElement.dataset.range }));
    check(dm.items.join() === 'all=Any time,today=Today,yesterday=Yesterday,7=Last 7 days,14=Last 14 days' && dm.checked === 'all' && dm.focus === 'all', 'the five ranges, the one in force ticked and focused: ' + dm.items);
    await page.keyboard.press('ArrowDown'); await page.keyboard.press('ArrowDown'); await page.keyboard.press('Enter'); await sleep(600);
    const yd = await page.evaluate(() => ({ range: CNListActivity.state('orders').range, label: document.querySelector('#ordersView .cnDateBtn').textContent.trim(), on: document.querySelector('#ordersView .cnDateBtn').hasAttribute('data-on'), chip: (document.querySelector('#ordActive [data-ax="range"]') || {}).textContent, focus: document.activeElement.className, closed: document.querySelector('#ordersView .cnDate .cnMenu').hidden }));
    check(yd.range === 'yesterday' && yd.label === 'Yesterday' && yd.on && /Yesterday/.test(yd.chip || '') && yd.closed && /cnDateBtn/.test(yd.focus), 'arrow keys + Enter pick Yesterday: the button names it in gold, a chip under the bar, the menu closed, the focus back: ' + JSON.stringify(yd));
    await page.click('#ordActive [data-ax="range"]'); await sleep(500);
    check(await page.evaluate(() => CNListActivity.state('orders').range === 'all'), 'the range chip lets go of the range');
    // sort
    await page.click('#ordSort'); await page.waitForSelector('#ordersView .cnSort .cnMenu:not([hidden])');
    const keys = await page.evaluate(() => [...document.querySelectorAll('#ordersView [data-sortkey]')].map(b => b.dataset.sortkey + '=' + b.textContent.trim() + ':' + b.getAttribute('aria-checked')));
    check(keys.join() === 'activity=Last activity:true,arrival=Order date:false,due=Ship-by date:false,order=Order number:false,state=State:false', 'the five sort keys: ' + keys);
    await page.click('#ordersView [data-sortkey="order"]'); await sleep(600);
    const order = () => page.evaluate(() => Orders.walkRows().map(r => String(r.order.receiptId)).filter((x, i, a) => a.indexOf(x) === i));
    const o1 = await order(), asc = o1.slice().sort();
    check(JSON.stringify(o1) === JSON.stringify(asc) && /Order number/.test(await page.textContent('#ordSort')), 'by Order number: lowest first, the button names the key');
    const dirBtn = '#ordersView .cnDir';
    check(await page.getAttribute(dirBtn, 'data-dir') === 'asc' && /Lowest number first/.test(await page.getAttribute(dirBtn, 'title')), 'and the arrow says ascending, in words, in its title');
    await page.click(dirBtn); await sleep(600);
    const o2 = await order();
    check(JSON.stringify(o2) === JSON.stringify(asc.slice().reverse()) && await page.getAttribute(dirBtn, 'data-dir') === 'desc' && await page.evaluate(() => Orders.view().desc === true), 'the arrow reverses it: highest first (OV.desc)');
    await page.click('#ordSort'); await page.click('#ordersView [data-sortkey="due"]'); await sleep(600);
    const due = await page.evaluate(() => Orders.walkRows().map(r => r.order.shipBy || 0));
    check(due.every((x, i) => !i || due[i - 1] <= x) && await page.evaluate(() => Orders.view().sort === 'due' && Orders.view().desc === false), 'choosing another key starts it in its own direction: Ship-by, soonest first');
    await page.click('#ordSort'); await page.click('#ordersView [data-sortkey="arrival"]'); await sleep(600);
    check(await page.evaluate(() => Orders.view().sort === 'arrival' && !!document.querySelector('#ordItems .ordDay')), 'Order date sorts by arrival and draws its day headings');
    await page.click('#ordSort'); await page.click('#ordersView [data-sortkey="state"]'); await sleep(500);
    await page.click('#ordSort'); await page.click('#ordersView [data-sortkey="activity"]'); await sleep(500);
    await page.click(dirBtn); await sleep(500);
    check(await page.evaluate(() => CNListActivity.state('orders').direction === 'asc' && Orders.view().sort === 'activity'), 'on Last activity the arrow is the shared direction (oldest first)');
    await page.click(dirBtn); await sleep(500);
    check(await page.evaluate(() => CNListActivity.state('orders').direction === 'desc'), 'and back to newest first');

    // ═══ 5 · view, search ═══
    await page.click('#ordViewSeg [data-view="cards"]'); await sleep(200);
    const cv = await page.evaluate(() => ({ cards: !!document.querySelector('#ordItems.ordCards'), saved: CN.S.settings.orderView, on: document.querySelector('#ordViewSeg .on').dataset.view, pressed: document.querySelector('#ordViewSeg [data-view=cards]').getAttribute('aria-pressed') }));
    check(cv.cards && cv.saved === 'cards' && cv.on === 'cards' && cv.pressed === 'true', 'Cards: drawn in the same breath, kept in Settings (orderView), the button pressed');
    await page.click('#ordViewSeg [data-view="list"]'); await sleep(200);
    check(await page.evaluate(() => !!document.querySelector('#ordItems.ordList') && CN.S.settings.orderView === 'list'), 'List likewise');
    await page.fill('#ordQ', '419510'); await sleep(40);
    const typing = await page.evaluate(() => ({ count: document.getElementById('ordCount').textContent, busy: document.getElementById('ordBody').getAttribute('aria-busy'), spin: !!document.querySelector('#ordFind .spin'), x: !document.getElementById('ordFindX').hidden, rows: document.querySelectorAll('#ordItems > *').length }));
    check(/^\d+ of \d+ orders$/.test(typing.count) && typing.busy === 'true' && typing.spin && typing.x, 'typing: the count is new, the box shows the spinner and its ×, the list is busy (40 ms in: "' + typing.count + '")');
    await sleep(700);
    const typed = await page.evaluate(() => ({ busy: document.getElementById('ordBody').hasAttribute('aria-busy'), spin: document.querySelectorAll('#ordBar .spin').length, n: new Set(Orders.walkRows().map(r => r.order.receiptId)).size, allMatch: Orders.walkRows().every(r => String(r.order.receiptId).includes('419510')) }));
    check(!typed.busy && typed.spin === 0 && typed.n > 0 && typed.allMatch, 'a moment later the list is the search\'s answer (' + typed.n + ' orders) and the busy state has ended');
    await page.keyboard.press('Escape'); await sleep(500);
    check(await page.evaluate(() => document.getElementById('ordQ').value === '' && Orders.view().q === '' && document.getElementById('ordFindX').hidden) && await page.evaluate(() => document.activeElement.id) === 'ordQ', 'Esc in the box clears it and keeps the focus in it');
    await page.fill('#ordQ', '4195100003'); await sleep(500); await page.click('#ordFindX'); await sleep(500);
    check(await page.evaluate(() => Orders.view().q === '' && document.activeElement.id === 'ordQ'), 'the × clears the search and puts the cursor back in the box');
    await page.type('#ordQ', '41951000'); await page.keyboard.press('Enter'); await sleep(250);
    check(await page.evaluate(() => Orders.walkRows().length > 0 && Orders.walkRows().every(r => String(r.order.receiptId).includes('41951000'))), 'Enter draws without waiting out the pause');
    await page.fill('#ordQ', ''); await sleep(500);

    // ═══ 7 · the Cancelled pile ═══
    await page.click('#ordChips [data-pile="cancelled"]'); await sleep(600);
    const cx = await page.evaluate(() => { const vis = sel => { const n = document.querySelector(sel); return !!n && n.getClientRects().length > 0; }; return { cls: document.getElementById('ordBar').className, filters: vis('#ordFiltersBtn'), view: vis('#ordViewSeg'), sortKey: vis('#ordSort'), date: vis('#ordersView .cnDateBtn'), dir: vis('#ordersView .cnDir'), dirText: document.querySelector('#ordersView .cnDir').textContent.trim(), ph: document.getElementById('ordQ').placeholder, sortEl: !!document.getElementById('ordSort') }; });
    check(/onCx/.test(cx.cls) && !cx.filters && !cx.view && !cx.sortKey && cx.sortEl && cx.date && cx.dir && /first/.test(cx.dirText) && /buyer/.test(cx.ph), 'Cancelled: Filters, Cards | List and the sort key step aside; the date menu and the direction stay (' + cx.dirText + '): ' + JSON.stringify(cx));
    await page.click('#ordChips [data-pile=""]'); await sleep(500);
    check(await page.evaluate(() => document.getElementById('ordFiltersBtn').getClientRects().length > 0 && document.getElementById('ordSort').getClientRects().length > 0), 'and Open Orders brings them back');
    // ═══ 9 · the other tabs get the same two small controls (the shared component: CNListActivity.mount) ═══
    for (const [mode, hasDate] of [['engrave', true], ['review', true], ['library', false], ['charms', true], ['master', true]]) {
      await page.evaluate(m => CN.setMode(m), mode); await sleep(900);
      const t = await page.evaluate(() => { const tl = [...document.querySelectorAll('.cnListTools')].find(x => x.getClientRects().length); if (!tl) return null; return { scope: tl.dataset.activityTools, selects: tl.querySelectorAll('select').length, date: !!tl.querySelector('.cnDateBtn'), chips: tl.querySelectorAll('.cnDatePresets').length, dir: !!tl.querySelector('.cnDir'), items: tl.querySelectorAll('[data-range]').length, w: Math.round(tl.getBoundingClientRect().width) }; });
      check(!!t && t.selects === 0 && t.chips === 0 && t.dir && t.date === hasDate && t.items === (hasDate ? 5 : 0), mode + ': the tools are ' + (hasDate ? 'one date menu and one sort button' : 'the one sort button (a list with no date range)') + ': ' + JSON.stringify(t));
      if (!t) continue;
      const before = await page.evaluate(sc => CNListActivity.state(sc).direction, t.scope);
      await page.click(`[data-activity-tools="${t.scope}"] .cnDir`); await sleep(500);
      const flipped = await page.evaluate(sc => ({ d: CNListActivity.state(sc).direction, b: document.querySelector(`[data-activity-tools="${sc}"] .cnDir`).dataset.dir, label: document.querySelector(`[data-activity-tools="${sc}"] .cnDir`).textContent.trim() }), t.scope);
      check(flipped.d !== before && flipped.b === flipped.d && /first/.test(flipped.label), mode + ': the button reverses the list (' + before + ' → ' + flipped.d + ', "' + flipped.label + '")');
      await page.click(`[data-activity-tools="${t.scope}"] .cnDir`); await sleep(400);
      if (hasDate) {
        await page.click(`[data-activity-tools="${t.scope}"] .cnDateBtn`); await page.click(`[data-activity-tools="${t.scope}"] [data-range="7"]`); await sleep(500);
        check(await page.evaluate(sc => CNListActivity.state(sc).range === '7' && /Last 7 days/.test(document.querySelector(`[data-activity-tools="${sc}"] .cnDateBtn`).textContent), t.scope), mode + ': the date menu sets the range and the button names it');
        await page.click(`[data-activity-tools="${t.scope}"] .cnDateBtn`); await page.click(`[data-activity-tools="${t.scope}"] [data-range="all"]`); await sleep(400);
      }
    }
    await page.evaluate(() => CN.setMode('orders')); await sleep(500);
    check(w.errors.length === 0, 'no page errors: ' + w.errors.join(' | '));
  } catch (e) { check(false, 'the test itself failed: ' + (e && e.stack || e)); }
  await w.close();

  // ═══ 1 + 8 · the widths, and a phone ═══
  for (const width of [1500, 1024, 760, 400, 320]) {
    const touch = width <= 760;
    const v = await world({ viewport: { width, height: 800 }, facts: false, touch });
    try {
      await seed(v.page, Object.keys(FIXTURES).filter(k => k.startsWith('x')).concat(['single', 'pairMatching', 'mismatched']));
      await v.page.evaluate(wd => { const MS = ['gold', 'silver', 'rose', 'gold10k', 'gold14k', 'gold'], FS = ['necklace', 'earrings', 'huggie', 'bracelet', 'charm', 'keychain']; Orders.rows().forEach((r, i) => { r.material = MS[i % 6]; if (r.spec) r.spec.form = FS[(i >> 1) % 6]; }); Orders.render(); if (wd < 1200 && !document.getElementById('app').classList.contains('railOff')) document.getElementById('btnRail').click(); }, width);
      await v.page.waitForTimeout(500);
      const m = await v.page.evaluate(() => { const b = document.getElementById('ordBar'), r = b.getBoundingClientRect(); const small = [...b.querySelectorAll('button:not([hidden]),.ordFind')].filter(n => n.getClientRects().length && !n.closest('[hidden]') && !n.closest('.cnPop')).map(n => ({ c: n.className || n.id, h: Math.round(n.getBoundingClientRect().height) })).filter(x => x.h < 40); const wide = [...b.querySelectorAll('*')].filter(n => n.getClientRects().length && n.getBoundingClientRect().right > document.documentElement.clientWidth + 1).map(n => n.className || n.tagName); return { page: document.documentElement.scrollWidth - document.documentElement.clientWidth, bar: b.scrollWidth - b.clientWidth, small, wide, h: Math.round(r.height) }; });
      check(m.page <= 0 && m.bar <= 1 && m.wide.length === 0, width + ' px: nothing overflows (page ' + m.page + ', bar ' + m.bar + ', beyond the edge ' + m.wide.join() + '), bar ' + m.h + ' px tall');
      if (touch) check(m.small.length === 0, width + ' px: every target is at least 40 px tall' + (m.small.length ? ': ' + JSON.stringify(m.small) : ''));
      // the panel
      await v.page.click('#ordFiltersBtn'); await v.page.waitForSelector('#ordFiltersPop:not([hidden])'); await v.page.waitForTimeout(300);
      const pr = await v.page.evaluate(() => { const p = document.getElementById('ordFiltersPop'), r = p.getBoundingClientRect(), chips = [...p.querySelectorAll('.ordChip,[data-clear],[data-done]')].map(n => n.getBoundingClientRect().height); return { l: Math.round(r.left), r: Math.round(r.right), t: Math.round(r.top), b: Math.round(r.bottom), vw: innerWidth, vh: innerHeight, sheet: p.classList.contains('cnSheet'), minChip: Math.round(Math.min(...chips)), scrim: !!document.querySelector('.cnScrim') }; });
      check(pr.l >= 0 && pr.r <= pr.vw && pr.t >= 0 && pr.b <= pr.vh, width + ' px: the Filters panel is inside the window: ' + JSON.stringify(pr));
      if (width <= 640) check(pr.sheet && pr.scrim, width + ' px: it is a bottom sheet with a scrim');
      if (touch) check(pr.minChip >= 40, width + ' px: its chips and buttons are at least 40 px: ' + pr.minChip);
      await v.page.keyboard.press('Escape'); await v.page.waitForTimeout(200);
      if (touch) {   // the date and sort menus on a phone: inside the window, items 44 px
        await v.page.click('#ordersView .cnDateBtn'); await v.page.waitForTimeout(250);
        const dm2 = await v.page.evaluate(() => { const p = document.querySelector('#ordersView .cnDate .cnMenu'), r = p.getBoundingClientRect(); return { inside: r.left >= 0 && r.right <= innerWidth && r.bottom <= innerHeight, item: Math.round(p.querySelector('[data-range]').getBoundingClientRect().height) }; });
        check(dm2.inside && dm2.item >= 40, width + ' px: the date menu is inside the window with ' + dm2.item + ' px items');
        await v.page.keyboard.press('Escape');
        // another tab's bar scrolls sideways on a phone: its menu must still open in full, over the list, not clipped by the bar
        await v.page.evaluate(() => CN.setMode('review')); await v.page.waitForTimeout(800);
        await v.page.click('#reviewView .cnDateBtn'); await v.page.waitForTimeout(300);
        const rv = await v.page.evaluate(() => { const p = document.querySelector('#reviewView .cnDate .cnMenu'), r = p.getBoundingClientRect(), hit = document.elementFromPoint(r.left + r.width / 2, r.top + r.height / 2); return { shown: !p.hidden && r.width > 100 && r.height > 100, inside: r.left >= 0 && r.right <= innerWidth && r.bottom <= innerHeight, onTop: !!hit && p.contains(hit) }; });
        check(rv.shown && rv.inside && rv.onTop, width + ' px: the Review tab\'s date menu opens in full inside the window and on top: ' + JSON.stringify(rv));
        await v.page.keyboard.press('Escape');
      }
      check(v.errors.length === 0, width + ' px: no page errors: ' + v.errors.join(' | '));
    } catch (e) { check(false, width + ' px: the test itself failed: ' + (e && e.stack || e)); }
    await v.close();
  }
  console.log(fails.length ? '\nFAIL: ' + fails.length + ' of ' + nCheck + '\n - ' + fails.join('\n - ') : '\nPASS: ' + nCheck + ' checks (FILTERBAR: the Orders bar)');
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error('FAIL:', e && e.stack || e); process.exit(1); });
