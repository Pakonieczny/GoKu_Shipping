// CMP1 (Paul, 9 Nov 2026): the Orders tab's head consolidated, and a Completed pile for shipped orders, each with its SHIPPED seal.
//   "consolidate with the Anytime and last activity under the filters, remove the option for list or card review ... Add a new tab called "Completed" where orders that have been marked as
//    shipped automatically get placed ... Each completed order must contain a shipped seal. Relocate the "design studio" button to the top right hand corner of the "Order" tab list view."
// The REAL page over the repo's fake site (_orderFactsWorld.cjs: offline, made-up orders, no Etsy, nothing live). What it proves:
//   1  the bar: Pull orders · Open Orders · Completed · Cancelled · charms … search · Filters · Design Studio; no Any time / Last activity / ↓ / Cards|List outside Filters; one slim row at 1400 px
//   2  Filters holds "Time" and "Sort by": a range and a sort chosen there narrow / order the list, the badge counts them, the gold chips under the bar show them and let go of them; Clear lets go
//   3  Design Studio is the last control of the head (its top right), the same node after redraws, gold, and still opens "Design a charm"
//   4  a saved "cards" view is ignored: the list is drawn as rows
//   5  an order Etsy shipped (the orders check found it gone, reason "shipped") leaves Open Orders and its count (the pile and the tab), and is listed under Completed with its count
//   6  an order with a recorded shipped event (a shipping station's scan, read through the timeline the page already reads) moves too; its seal names the person who shipped it
//   7  every Completed row has a SHIPPED seal (the Etsy-only one says "Etsy"); a rested / clicked seal grows (Seal.zoom); the order window of a completed order shows the seal
//   8  an order that ships while Completed is on screen is stamped (the wooden press on its seal)
//   9  a phone (390 px): the head wraps, nothing scrolls sideways, Design Studio still at the right end
//   PW_DIR=<dir holding playwright-core> CHROMIUM=<chrome> node tests/charm-nest/orders-completed.cjs
'use strict';
const { world, seed, FIXTURES } = require('./_orderFactsWorld.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg, ev) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg + (ok || !ev ? '' : '  ' + ev)); };

const head = page => page.evaluate(() => {
  const bar = document.getElementById('ordBar'), pop = document.getElementById('ordFiltersPop');
  const outside = sel => [...bar.querySelectorAll(sel)].filter(n => !pop.contains(n)).length;
  const tools = document.getElementById('ordTools'), ds = document.getElementById('ordDesignBtn');
  const vis = n => { const r = n.getBoundingClientRect(), cs = getComputedStyle(n); return r.width > 0 && r.height > 0 && cs.display !== 'none' && cs.visibility !== 'hidden'; };
  const controls = [...bar.querySelectorAll('button,label.ordFind')].filter(n => !pop.contains(n) && !n.closest('#ordActive') && !n.closest('.ordFind') && vis(n));
  const dsR = ds && ds.getBoundingClientRect(), right = Math.max(...controls.map(n => n.getBoundingClientRect().right));
  const mids = controls.map(n => { const r = n.getBoundingClientRect(); return (r.top + r.bottom) / 2; }).sort((a, b) => a - b), rows = mids.filter((m, i) => !i || m - mids[i - 1] > 12).length;
  return { dates: outside('.cnDateBtn,.cnDate,[data-range]'), sorts: outside('.cnSort,.cnKey,.cnDir,[data-sortkey]'), views: document.querySelectorAll('#ordViewSeg,#ordBar [data-view]').length, tools: bar.querySelectorAll('[data-activity-tools]').length,
    piles: [...document.querySelectorAll('#ordChips [data-pile]')].map(b => b.dataset.pile), dsLast: !!ds && tools.lastElementChild === ds, dsRight: !!dsR && dsR.right >= right - 0.5,
    order: controls.map(n => n.id || n.dataset.pile || n.className.split(' ')[0]).join(' · '), barH: Math.round(bar.getBoundingClientRect().height), rows,
    findW: Math.round(document.getElementById('ordFind').getBoundingClientRect().width), overflow: document.documentElement.scrollWidth > document.documentElement.clientWidth || bar.scrollWidth > bar.clientWidth + 1 };
});
const pile = (page, id) => page.evaluate(id => { const b = document.querySelector(`#ordChips [data-pile="${id}"] b`); return b ? +b.textContent : null; }, id);
const listed = page => page.evaluate(() => [...new Set([...document.querySelectorAll('#ordItems [data-rid]')].map(n => n.dataset.rid))]);
const filt = page => page.evaluate(() => { const b = document.getElementById('ordFiltersBtn'), badge = b.querySelector('.cnBadge'); return { badge: badge.hidden ? 0 : +badge.textContent, range: CNListActivity.state('orders').range, dir: CNListActivity.state('orders').direction, sort: Orders.view().sort, desc: Orders.view().desc,
  ax: [...document.querySelectorAll('#ordActive .ordAx')].map(x => x.dataset.ax), sections: [...document.querySelectorAll('#ordFiltersPop .ordFg h4')].map(h => h.textContent) }; });
const chip = (page, f, v) => page.evaluate(([f, v]) => { const c = [...document.querySelectorAll('#ordFiltersPop .ordChip')].find(x => x.dataset.f === f && x.dataset.v === v); if (c) c.click(); return !!c; }, [f, v]);
const cmpRows = page => page.evaluate(() => [...document.querySelectorAll('#ordBody .cmpRow')].map(n => ({ rid: n.dataset.rid, seal: !!n.querySelector('[data-testid="shipped-seal"] .seal svg'), by: (n.querySelector('.pgBy') || {}).textContent || '', aria: (n.querySelector('.seal') || { getAttribute: () => '' }).getAttribute('aria-label') || '' })));

(async () => {
  const w = await world({ viewport: { width: 1400, height: 900 }, facts: false });
  if (!w) { console.log('SKIP: no browser (set PW_DIR and CHROMIUM)'); return; }
  const { page } = w;
  try {
    await page.evaluate(() => { try { localStorage.removeItem('cn.completed.v1:production'); } catch (_) {} });
    await seed(page, ['single', 'pairMatching', 'engraved', 'late', 'message']);
    await page.waitForTimeout(600);

    /* 1 · the bar */
    const h = await head(page);
    check(h.dates === 0 && h.sorts === 0 && h.views === 0 && h.tools === 0, `1 no Any time, Last activity, ↓ or Cards|List in the bar outside Filters (date ${h.dates}, sort ${h.sorts}, view ${h.views}, tools ${h.tools})`, JSON.stringify(h));
    check(JSON.stringify(h.piles) === JSON.stringify(['', 'completed', 'cancelled']) && h.rows === 1 && h.barH <= 44 && h.findW <= 262 && h.findW >= 140,
      `1 the piles read Open Orders · Completed · Cancelled; the head is one slim row at 1400 px (${h.barH} px; search ${h.findW} px): ${h.order}`, JSON.stringify(h));

    /* 2 · Time and Sort by inside Filters */
    await page.click('#ordFiltersBtn'); await page.waitForTimeout(250);
    let f = await filt(page);
    check(f.sections.includes('Time') && f.sections.includes('Sort by') && f.badge === 0, `2 Filters holds "Time" and "Sort by" (${f.sections.join(', ')}); nothing applied, no badge`, JSON.stringify(f));
    const before = await listed(page);
    await chip(page, 'range', 'yesterday'); await page.waitForTimeout(500);
    f = await filt(page);
    check(f.range === 'yesterday' && f.badge === 1 && f.ax.includes('range'), `2 Time › Yesterday: the range is in force, the badge says ${f.badge}, a gold chip under the bar says it`, JSON.stringify(f));
    const inY = await listed(page);
    check(before.length === 5 && inY.length === 0, `2 the list is narrowed by the range (${before.length} → ${inY.length} orders: every made-up order had activity today)`, JSON.stringify({ before, inY }));
    await chip(page, 'range', '7'); await page.waitForTimeout(500);
    const in7 = await listed(page); f = await filt(page);
    check(f.range === '7' && in7.length === before.length, `2 Time › Last 7 days brings them back (${in7.length})`, JSON.stringify({ f, in7 }));
    await chip(page, 'sort', 'order'); await page.waitForTimeout(500);
    f = await filt(page);
    const byNum = await listed(page);
    check(f.sort === 'order' && f.badge === 2 && f.ax.includes('sort') && JSON.stringify(byNum) === JSON.stringify(byNum.slice().sort()), `2 Sort by › Order number orders the list by number (${byNum.join(', ')}); the badge says ${f.badge}`, JSON.stringify(f));
    const dirs = await page.evaluate(() => [...document.querySelectorAll('#ordFiltersPop .ordChip[data-f="dir"]')].map(c => c.textContent + (c.getAttribute('aria-pressed') === 'true' ? '*' : '')));
    await chip(page, 'dir', 'desc'); await page.waitForTimeout(500);
    const byNumDesc = await listed(page);
    check(JSON.stringify(byNumDesc) === JSON.stringify(byNum.slice().reverse()), `2 its direction is under it (${dirs.join(' / ')}): Highest number first reverses the list`, JSON.stringify({ byNumDesc, dirs }));
    await page.evaluate(() => document.querySelector('#ordActive .ordAx[data-ax="sort"]').click()); await page.waitForTimeout(500);
    f = await filt(page);
    check(f.sort === 'activity' && !f.desc && f.dir === 'desc' && f.badge === 1 && !f.ax.includes('sort'), `2 its chip under the bar puts the sort back to Last activity, newest first (badge ${f.badge})`, JSON.stringify(f));
    await page.evaluate(() => { if (!document.getElementById('ordFiltersPop').hidden) return; document.getElementById('ordFiltersBtn').click(); }); await page.waitForTimeout(200);
    await page.evaluate(() => document.querySelector('#ordFiltersPop [data-clear]').click()); await page.waitForTimeout(500);
    f = await filt(page);
    check(f.range === 'all' && f.badge === 0 && !f.ax.length, `2 Clear lets go of the range too (badge ${f.badge})`, JSON.stringify(f));
    await page.keyboard.press('Escape'); await page.waitForTimeout(200);

    /* 3 · Design Studio at the top right, the same node, still opens Design a charm */
    const n0 = await page.evaluateHandle(() => document.getElementById('ordDesignBtn'));
    await page.evaluate(() => { Orders.render(); Orders.renderNow(); Orders.showPile('cancelled'); }); await page.waitForTimeout(400);
    await page.evaluate(() => Orders.showPile('')); await page.waitForTimeout(400);
    const h3 = await head(page);
    const ds = await page.evaluate(n => { const b = document.getElementById('ordDesignBtn'); return { same: b === n, gold: /\bcnCeBtn\b/.test(b.className), on: !b.disabled, text: b.textContent.trim() }; }, n0);
    await page.evaluate(() => { window.__calls = []; window.__realM = window.CharmNestCeMasters; window.CharmNestCeMasters = { newDesign: o => window.__calls.push([o && o.from, o && o.back && o.back.label]) }; });
    await page.click('#ordDesignBtn'); await page.waitForTimeout(200);
    const calls = await page.evaluate(() => window.__calls);
    await page.evaluate(() => { window.CharmNestCeMasters = window.__realM; document.getElementById('ordDesignBtn').click(); }); await page.waitForTimeout(700);
    const modal = await page.evaluate(() => { const w = document.querySelector('.cem-win'); return w ? [...w.querySelectorAll('.cem-seg button')].map(x => x.textContent.trim()) : null; });
    await page.keyboard.press('Escape'); await page.waitForTimeout(400);
    check(h3.dsLast && h3.dsRight && ds.same && ds.gold && ds.on && ds.text === 'Design Studio', `3 "Design Studio" is the last control of the head, at its right end (the same gold node after redraws and a pile switch)`, JSON.stringify({ h3, ds }));
    check(JSON.stringify(calls) === JSON.stringify([['ordersDesignStudio', 'Back to Orders']]) && !!modal && modal.length >= 2, `3 pressed, it still opens "Design a charm" (${(modal || []).join(' / ')})`, JSON.stringify({ calls, modal }));

    /* 4 · a saved cards view is ignored */
    await page.evaluate(() => { CN.S.settings.orderView = 'cards'; Orders.view().view = 'cards'; Orders.renderNow(); }); await page.waitForTimeout(300);
    const cls = await page.evaluate(() => document.getElementById('ordItems').className);
    check(cls === 'ordList', `4 a saved "cards" view is ignored: the Orders tab is the list (${cls})`);

    /* 5 · an order Etsy shipped: out of Open Orders, into Completed */
    const open0 = await pile(page, ''), cmp0 = await pile(page, 'completed'), tab0 = await page.evaluate(() => +document.getElementById('tabOrdersN').textContent);
    const shipAt = Date.now() - 3 * 3600e3;
    await page.evaluate(([rid, at]) => { for (const r of Orders.rows()) if (String(r.order.receiptId) === rid) { r.state = 'gone'; r.reason = 'shipped'; r.shippedAt = at; } Orders.renderNow(); }, [FIXTURES.late.rid, shipAt]);
    await page.waitForTimeout(500);
    const open1 = await pile(page, ''), cmp1 = await pile(page, 'completed'), tab1 = await page.evaluate(() => +document.getElementById('tabOrdersN').textContent), l1 = await listed(page);
    check(open1 === open0 - 1 && tab1 === tab0 - 1 && cmp1 === cmp0 + 1 && !l1.includes(FIXTURES.late.rid), `5 the order Etsy shipped left Open Orders (${open0} → ${open1}; the tab ${tab0} → ${tab1}) and Completed counts it (${cmp0} → ${cmp1})`, JSON.stringify({ open0, open1, cmp0, cmp1, tab0, tab1, l1 }));

    /* 6 · a recorded shipped event (the timeline the page already reads) */
    await page.evaluate(rid => { OrderTimeline.record({ orderId: rid, type: 'shipped', by: 'Marco R.', station: 'shipping', id: 'ship-scan-test' }); }, FIXTURES.single.rid);
    await page.waitForTimeout(700);
    const open2 = await pile(page, ''), cmp2 = await pile(page, 'completed'), l2 = await listed(page);
    check(open2 === open1 - 1 && cmp2 === cmp1 + 1 && !l2.includes(FIXTURES.single.rid), `6 a shipping station's shipped event moves its order too (Open ${open1} → ${open2}, Completed ${cmp1} → ${cmp2})`, JSON.stringify({ open2, cmp2, l2 }));
    // (and an event only the timeline read knows: learned from OrderTimeline.get, which the order window and the search already call; nothing is read for it here)
    const learnedByRead = await page.evaluate(async rid => { const before = CNCompleted.has(rid); await OrderTimeline.get(rid).catch(() => null); await new Promise(r => setTimeout(r, 50)); return { before, after: CNCompleted.has(rid) }; }, FIXTURES.single.rid);
    check(learnedByRead.after, '6 the same fact read back through OrderTimeline.get is kept (the read the page already makes)', JSON.stringify(learnedByRead));

    /* 7 · the Completed list: every row has its SHIPPED seal */
    await page.click('#ordChips [data-pile="completed"]'); await page.waitForTimeout(3000);   // (the two orders that shipped while the page was open are stamped as they come in: the stamps end first)
    const rows = await cmpRows(page);
    const etsyRow = rows.find(r => r.rid === FIXTURES.late.rid), scanRow = rows.find(r => r.rid === FIXTURES.single.rid);
    check(rows.length === 2 && rows.every(r => r.seal), `7 Completed lists ${rows.length} orders, every one with its SHIPPED seal`, JSON.stringify(rows));
    check(!!etsyRow && /Etsy/.test(etsyRow.by) && /shipped/i.test(etsyRow.aria) && !!scanRow && /Marco R\./.test(scanRow.by), `7 the seal built from Etsy's shipped fact alone says "${etsyRow && etsyRow.by}"; the scanned one names its person ("${scanRow && scanRow.by}")`, JSON.stringify(rows));
    check(rows[0] && rows[0].rid === FIXTURES.single.rid, '7 newest shipped first', JSON.stringify(rows.map(r => r.rid)));
    const visual = await page.evaluate(() => { const s = document.querySelector('#ordBody .cmpRow .seal'), r = s.getBoundingClientRect(); return { w: Math.round(r.width), label: document.querySelector('#ordBody .cmpRow svg text') ? [...document.querySelector('#ordBody .cmpRow svg').querySelectorAll('text,textPath')].map(t => t.textContent).join(' ') : '' }; });
    await page.hover('#ordBody .cmpRow .seal'); await page.waitForTimeout(900);
    const zoomed = await page.evaluate(() => !!(Seal.zoom && Seal.zoom.current && Seal.zoom.current.closest('.cmpRow')));
    await page.mouse.move(5, 5); await page.waitForTimeout(400);
    check(visual.w >= 30 && /SHIPPED/.test(visual.label) && zoomed, `7 the seal is the existing SHIPPED face (${visual.w} px, "${visual.label.slice(0, 40)}…") and grows in place when rested on`, JSON.stringify({ visual, zoomed }));
    await page.evaluate(rid => { const n = [...document.querySelectorAll('#ordBody .cmpRow')].find(x => x.dataset.rid === rid); n.querySelector('.cmpId').click(); }, FIXTURES.late.rid);
    await page.waitForTimeout(1500);
    const win = await page.evaluate(() => { const d = document.getElementById('orderWin'), s = document.querySelector('#owShipSeal .seal'); return { open: !!(d && d.open), seal: !!s, aria: s ? s.getAttribute('aria-label') : '' }; });
    check(win.open && win.seal && /Etsy/.test(win.aria), `7 the order window of a completed order shows the same seal (${win.aria})`, JSON.stringify(win));
    await page.keyboard.press('Escape'); await page.waitForTimeout(700);

    /* 8 · shipped while Completed is on screen: the stamp comes down */
    await page.evaluate(() => { window.__pressed = 0; const p = Seal.press; Seal.press = function (s) { if (s && s.closest && s.closest('.cmpRow')) window.__pressed++; return p.apply(this, arguments); }; });
    await page.evaluate(rid => { for (const r of Orders.rows()) if (String(r.order.receiptId) === rid) { r.state = 'gone'; r.reason = 'shipped'; } Orders.renderNow(); }, FIXTURES.message.rid);
    await page.waitForTimeout(1800);
    const after8 = await page.evaluate(() => ({ pressed: window.__pressed, n: document.querySelectorAll('#ordBody .cmpRow').length, seals: document.querySelectorAll('#ordBody .cmpRow [data-testid="shipped-seal"] .seal').length }));
    check(after8.pressed >= 1 && after8.n === 3 && after8.seals === 3, `8 an order shipped while Completed is shown comes in stamped (${after8.pressed} press; ${after8.n} rows, ${after8.seals} seals)`, JSON.stringify(after8));

    check(!w.errors.length, 'no page errors: ' + w.errors.join(' | '));
  } catch (e) { fails.push(e.message); console.error(e); }
  finally { await w.close(); }

  /* 9 · a phone (a touch screen of 390 px, as filterbar.cjs opens one) */
  const p = await world({ viewport: { width: 390, height: 844 }, facts: false, touch: true });
  try {
    await seed(p.page, ['single', 'pairMatching', 'engraved']); await p.page.waitForTimeout(400);
    // (the rail folded away, as a phone has it: completed-seals-review-orders.cjs folds it the same way)
    await p.page.evaluate(() => { if (!document.getElementById('app').classList.contains('railOff')) document.getElementById('btnRail').click(); }); await p.page.waitForTimeout(700);
    const hp = await head(p.page);
    check(!hp.overflow && hp.dsLast && hp.dsRight && hp.findW >= 250, `9 at 390 px the head wraps cleanly (${hp.rows} rows, ${hp.barH} px; the search ${hp.findW} px), nothing scrolls sideways, Design Studio at the right end`, JSON.stringify(hp));
    check(!p.errors.length, '9 no page errors on the phone: ' + p.errors.join(' | '));
  } catch (e) { fails.push(e.message); console.error(e); }
  finally { await p.close(); }
  console.log(fails.length ? `\n${fails.length} OF ${nCheck} FAIL` : `\nALL ${nCheck} PASS`);
  process.exitCode = fails.length ? 1 : 0;
})();
