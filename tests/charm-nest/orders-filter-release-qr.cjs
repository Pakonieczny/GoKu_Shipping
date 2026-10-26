// RQFILTER (Paul, 19 Oct 2026): "You should also add a new sorting filter inside the existing Filter drop-down menu inside the Orders tab. It should show only the open 'Release Hold' and 'Print QR' Orders."
// The REAL page over the repo's fake site (no Etsy, nothing live), with seven made-up orders, each in the state its name says:
//   held     on hold: the Orders card's own "Release hold" button                          -> Release Hold
//   qr       an unknown SKU: an open Review card with Print QR label (its Release Hold is greyed) -> Print QR
//   both     ONE order of two lines: an unknown SKU (a Review card with Print QR label) and a line on hold -> Release Hold and Print QR (counted once)
//   marked   Send to Review pressed on its card (on a sheet): the Review card has a Release Hold that can be pressed, no Print QR -> Release Hold
//   released its hold was released (the large Release Hold seal is on its card)             -> nothing is open
//   printed  its QR label was printed (completed by hand, QR LABEL PRINTED)                -> nothing is open
//   plain    nothing to do                                                                  -> nothing is open
// What it proves:
//   1  the Filters panel has a "Needs action" group: Any, "Release Hold + Print QR", "Release Hold", "Print QR", each with its live count (orders: the count the list will show)
//   2  Release Hold shows exactly the open Release Hold orders; Print QR exactly the open Print QR orders; both chips on (or the combined chip) = either (OR), each order once
//   3  the badge counts the group once, the gold chip under the bar says what is on ("Release Hold", "Print QR", "Release Hold + Print QR") and removes it; Clear all and Clear let go of it; a chip pressed again goes off
//   4  the live count in the bar ("4 of 7 orders") and the list agree; no On hold pile (NOHOLDPILE); the Cancelled pile steps the filter aside and Open Orders brings it back
//   5  live: a real press of Release hold takes the order out of the filter at once, a label printed (the record the print press writes) likewise, and the chip counts follow
//   6  a group that could only do nothing is not drawn (while it is on, it is); the filter is part of the saved Orders view
//   7  it reads nothing and writes nothing: no request goes out while the filter is used
//   node tests/charm-nest/orders-filter-release-qr.cjs
'use strict';
const { world, seed, FIXTURES } = require('./_orderFactsWorld.cjs');
const fails = []; let nCheck = 0;
const check = (ok, msg) => { nCheck++; if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };

const mk = (rid, sku, buyer, h) => ({ rid, sku, title: 'RQ test charm ' + sku, buyer, placed: { day: 0, h, m: 5 }, ship: 5, variations: [{ name: 'Metal choice', value: 'Gold' }] });
const ID = { held: '4196000001', qr: '4196000002', both: '4196000003', marked: '4196000004', released: '4196000005', printed: '4196000006', plain: '4196000007' };
FIXTURES.rqHeld = mk(ID.held, 'RQ_HELD_ONE', 'Held Buyer', 8);
FIXTURES.rqQr = mk(ID.qr, 'RQ_QR_ONE', 'Qr Buyer', 9);
FIXTURES.rqBoth = Object.assign(mk(ID.both, 'RQ_BOTH_ONE', 'Both Buyer', 10), { more: [{ sku: 'RQ_BOTH_TWO', title: 'RQ test charm RQ_BOTH_TWO', variations: [{ name: 'Metal choice', value: 'Gold' }] }] });
FIXTURES.rqMark = mk(ID.marked, 'RQ_MARK_ONE', 'Marked Buyer', 11);
FIXTURES.rqRel = mk(ID.released, 'RQ_REL_ONE', 'Released Buyer', 12);
FIXTURES.rqPrn = mk(ID.printed, 'RQ_PRN_ONE', 'Printed Buyer', 13);
FIXTURES.rqPlain = mk(ID.plain, 'RQ_PLAIN_ONE', 'Plain Buyer', 14);
const NAMES = ['rqHeld', 'rqQr', 'rqBoth', 'rqMark', 'rqRel', 'rqPrn', 'rqPlain'];

(async () => {
  const w = await world({ viewport: { width: 1500, height: 900 }, facts: false });
  if (!w) { console.log('SKIP: no browser (set PW_DIR and CHROMIUM)'); return; }
  const { page } = w;
  const sleep = ms => page.waitForTimeout(ms);
  const fb = '#ordFiltersBtn', pop = '#ordFiltersPop', chipSel = v => `${pop} .ordChip[data-f="act"][data-v="${v}"]`;
  // (what the page asks of the fake site by itself, with nothing touched: the agent, the pieces' placement, the cancelled list, the Review tab's live read; the filter may ask for nothing beyond it)
  const BACKGROUND = new Set(['startAgent', 'getAgent', 'customReadGet', 'customSaveRepair', 'customSavePending', 'getOrderPieces', 'cancelList', 'customGet']);
  const requests = []; page.on('request', rq => { if (/^(data|blob):/.test(rq.url())) return; let op = ''; try { const b = JSON.parse(rq.postData() || '{}'); op = b.op || b.action || ''; } catch (_) {} requests.push({ method: rq.method(), path: rq.url().replace(/^https?:\/\/[^/]+/, ''), op }); });
  const rids = () => page.evaluate(() => [...new Set(Orders.walkRows().map(r => String(r.order.receiptId)))].sort());
  const ids = (...names) => names.map(n => ID[n]).sort();
  const sameSet = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const calm = () => page.evaluate(() => document.querySelectorAll('#toasts .toast').forEach(t => t.remove()));   // (a toast from an earlier press would cover the chips)
  const openPanel = async () => { await calm(); if (await page.locator(pop).isHidden()) { await page.click(fb); await page.waitForSelector(pop + ':not([hidden])'); } };
  const closePanel = async () => { if (await page.locator(pop).isVisible()) { await page.keyboard.press('Escape'); await sleep(120); } };
  const press = async v => { await openPanel(); await page.click(chipSel(v)); await sleep(550); };
  const chipsNow = () => page.evaluate(() => [...document.querySelectorAll('#ordActive .ordAx')].map(x => x.dataset.ax + '=' + x.textContent.trim()));
  const badge = () => page.evaluate(() => { const b = document.querySelector('#ordFiltersBtn .cnBadge'); return b.hidden ? '' : b.textContent; });
  const count = () => page.evaluate(() => document.getElementById('ordCount').textContent.trim());
  const pressed = () => page.evaluate(() => [...document.querySelectorAll('#ordFiltersPop .ordChip[data-f="act"]')].filter(c => c.getAttribute('aria-pressed') === 'true').map(c => c.dataset.v || 'any'));
  const clearAll = async () => { await page.evaluate(() => { const O = Orders.view(); O.act = null; O.q = ''; }); await page.evaluate(() => Orders.render()); await sleep(500); };
  try {
    await seed(page, NAMES);
    await sleep(700);
    // ── put each order in the state its name says (the rows are the page's own: only what happened to them is staged) ──
    await page.evaluate(ID => {
      const by = rid => Orders.rows().find(r => String(r.order.receiptId) === rid);
      const quiet = r => { r.problems = []; r.state = 'nested'; r.poolIds = ['pool-' + r.order.receiptId]; };   // (an order with nothing to ask: no Review card, on a sheet)
      for (const n of ['held', 'marked', 'released', 'printed', 'plain']) quiet(by(ID[n]));
      const h = by(ID.held); h.hold = 'Held by the test'; h.state = 'held'; h.poolIds = [];
      const b2 = Orders.rows().filter(r => String(r.order.receiptId) === ID.both)[1]; b2.hold = 'Held by the test'; b2.state = 'held';   // (the order's first line still asks its unknown-SKU question: a Review card; its second line is on hold)
      const rel = by(ID.released); rel.releaseHold = { at: Date.now() - 600000, by: 'Test Operator', id: 'rel-test' }; rel.frontAt = rel.releaseHold.at;
      const p = by(ID.printed), at = Date.now() - 900000;
      const rec = { key: p.key, receiptId: String(p.order.receiptId), transactionId: String(p.line.transactionId), sku: p.line.sku, title: p.line.title, category: 'Unknown SKU', kind: '', state: 'completed', how: 'print', completedAt: at, completedBy: 'Test Operator', printedAt: at, printedBy: 'Test Operator', prints: 1, lastPrintedAt: at, lastPrintedBy: 'Test Operator', hasLabel: true, stamps: [{ how: 'print', at, by: 'Test Operator' }] };
      B.maps.customDone = Object.assign({}, B.maps.customDone, { [p.key]: rec }); p.spec.customDone = rec;
      Review.syncOrderItems(); Orders.render();
    }, ID);
    await sleep(600);
    // the real Send to Review button on the "marked" order's card (it is on a sheet: the Review card it makes has Release Hold and, being on a sheet, no Print QR label)
    await page.click(`#ordItems [data-rid="${ID.marked}"] .revSendBtn`); await sleep(1500);
    const model = await page.evaluate(ID => { const o = {}; for (const [n, rid] of Object.entries(ID)) { o[n] = { held: false, card: null, print: false, release: '' }; for (const r of Orders.rows().filter(x => String(x.order.receiptId) === rid)) { const it = Review.pieceItemFor(r), m = o[n]; m.held = m.held || Orders.heldNow(r); if (it) { m.card = it.kind + (it.done ? ' done' : ''); m.print = m.print || !!Review.printable(it); m.release = (it.src ? ReviewRelease.of(it.src).how : '') || m.release; } } } return o; }, ID);
    console.log('  model: ' + JSON.stringify(model));
    check(model.held.held && !model.held.card && model.qr.card === 'reviewAct' && model.qr.print && model.qr.release === 'anyway' && model.both.held && model.both.print && model.both.card === 'reviewAct' && model.marked.card === 'reviewAct' && !model.marked.print && model.marked.release === 'mark'
      && !model.released.held && !model.released.card && model.printed.card === 'customOrder done' && !model.plain.held && !model.plain.card, 'the seven orders stand as staged: the buttons\' own rules say held / Print QR / both / Release Hold only / nothing / printed / nothing');

    // ═══ 1 · the panel ═══
    const before = requests.length;
    await page.click(fb); await page.waitForSelector(pop + ':not([hidden])');
    const g = await page.evaluate(() => { const p = document.getElementById('ordFiltersPop'), sec = [...p.querySelectorAll('.ordFg')].find(s => s.querySelector('h4').textContent === 'Needs action'); return sec ? { chips: [...sec.querySelectorAll('.ordChip')].map(c => (c.dataset.v || '') + ':' + c.firstChild.textContent + ':' + (c.querySelector('small') ? c.querySelector('small').textContent : '')), tips: [...sec.querySelectorAll('.ordChip[title]')].length, headings: [...p.querySelectorAll('.ordFg h4')].map(h => h.textContent) } : null; });
    check(!!g && g.chips.join('|') === ':Any:|both:Release Hold + Print QR:4|hold:Release Hold:4|qr:Print QR:2', 'the panel has "Needs action": Any, Release Hold + Print QR 4, Release Hold 4 (held, qr, both, marked: a Review card with an open question now releases anyway), Print QR 2 (qr, both): ' + (g && g.chips.join(' | ')));
    check(g && g.tips === 3 && (await pressed()).join() === 'any', 'each answer says what it is in its title; Any is the one pressed while nothing is on');
    await closePanel();

    // ═══ 2 · the chips ═══
    await press('hold');
    const hold = await rids();
    check(sameSet(hold, ids('held', 'qr', 'both', 'marked')), 'Release Hold: exactly the open Release Hold orders (on hold, in Review with an open question (releases anyway), on hold and in Review, sent to Review): ' + hold);
    check(await count() === '4 of 7 orders' && await badge() === '1' && (await chipsNow()).join() === 'act=Release Hold' && (await pressed()).join() === 'hold', 'the live count says "4 of 7 orders", the badge 1, one gold chip "Release Hold", the Release Hold chip pressed');
    check(await page.evaluate(() => document.querySelector('#ordActive .ordAx').getAttribute('aria-label')) === 'Remove the filter: Needs action, Release Hold', 'and the chip says what pressing it does (aria-label)');
    await press('qr');
    const either = await rids();
    check(sameSet(either, ids('held', 'qr', 'both', 'marked')) && (await chipsNow()).join() === 'act=Release Hold + Print QR', 'Print QR switched on as well = either (OR): held, qr, both, marked, each once: ' + either);
    check((await pressed()).sort().join() === 'both,hold,qr' && await badge() === '1' && await count() === '4 of 7 orders', 'both single chips and the combined chip are pressed together; the badge is still 1 (one group); "4 of 7 orders"');
    await press('hold');
    const qrOnly = await rids();
    check(sameSet(qrOnly, ids('qr', 'both')) && (await chipsNow()).join() === 'act=Print QR' && (await pressed()).join() === 'qr', 'Release Hold switched off: only Print QR is left, the open Print QR orders (qr, both): ' + qrOnly);
    await press('qr');
    check(sameSet(await rids(), ids('held', 'qr', 'both', 'marked', 'released', 'printed', 'plain')) && await badge() === '' && (await chipsNow()).length === 0 && (await pressed()).join() === 'any' && await count() === '7 orders', 'Print QR switched off too: neither on is no filter (all seven, no badge, no chip, Any pressed)');
    await press('both');
    check(sameSet(await rids(), ids('held', 'qr', 'both', 'marked')) && (await pressed()).sort().join() === 'both,hold,qr', 'the combined chip alone selects both at once: the same four');
    await press('both');
    check(sameSet(await rids(), ids('held', 'qr', 'both', 'marked', 'released', 'printed', 'plain')) && (await pressed()).join() === 'any', 'and pressed again it goes off');
    await press('qr'); await press('');   // (Any lets go)
    check(await page.evaluate(() => Orders.view().act) === null && await badge() === '', 'Any lets go of the group');
    // the orders that were acted on are in no answer
    await press('both');
    const acted = await rids();
    check(!acted.includes(ID.released) && !acted.includes(ID.printed) && !acted.includes(ID.plain), 'an order whose hold was released, one whose QR label was printed and a plain order are in neither chip');
    await closePanel();

    // ═══ 3 · the chip under the bar, Clear, Clear all ═══
    check((await chipsNow()).join() === 'act=Release Hold + Print QR', 'the gold chip under the bar names it: "Release Hold + Print QR"');
    await page.click('#ordActive [data-ax="act"]'); await sleep(500);
    check(await page.evaluate(() => Orders.view().act) === null && (await chipsNow()).length === 0 && sameSet(await rids(), ids('held', 'qr', 'both', 'marked', 'released', 'printed', 'plain')), 'one press on the chip lets go of the filter');
    await press('hold');
    await page.fill('#ordQ', 'RQ_'); await sleep(700);
    check(!!(await page.$('#ordActive [data-ax-all]')) && sameSet(await rids(), ids('held', 'qr', 'both', 'marked')), 'with the search as well there is Clear all, and the list is both narrowings');
    await page.click('#ordActive [data-ax-all]'); await sleep(600);
    check(await page.evaluate(() => Orders.view().act === null && Orders.view().q === '') && await badge() === '' && (await rids()).length === 7, 'Clear all lets go of the filter and the search');
    await press('qr');
    await openPanel(); await page.click(pop + ' [data-clear]'); await sleep(550);
    check(await page.evaluate(() => Orders.view().act) === null && await badge() === '' && (await rids()).length === 7, 'Clear, in the panel, lets go of it too');
    await closePanel();
    { const asked = requests.slice(before).filter(x => !(x.method === 'POST' && BACKGROUND.has(x.op)) && !(x.method === 'HEAD' && /charm-nest-1\.html$/.test(x.path)));   // (the page's own deploy-version check, a HEAD of itself, is background too)
      check(asked.length === 0, 'nothing was read or written for the filter: every request while it was used is one of the page\'s own background asks (' + requests.slice(before).length + ' of them): ' + (asked.map(x => x.method + ' ' + x.path + ' ' + x.op).join(' ; ') || 'no other request')); }

    // ═══ 4 · the piles ═══
    await press('hold'); await closePanel();
    // (NOHOLDPILE, Paul 20 Oct 2026: there is no On hold pile; the held orders stay in Open Orders under the filter)
    check(await page.evaluate(() => !document.querySelector('#ordChips [data-pile="hold"]') && [...document.querySelectorAll('#ordChips [data-pile]')].map(b => b.dataset.pile).join() === ',cancelled'), 'the piles are Open Orders and Cancelled: no On hold chip');
    check(sameSet(await rids(), ids('held', 'qr', 'both', 'marked')), 'the held orders stay in Open Orders, inside the Release Hold filter: ' + await rids());
    await page.click('#ordChips [data-pile="cancelled"]'); await sleep(700);
    check(await page.evaluate(() => { const vis = sel => { const n = document.querySelector(sel); return !!n && n.getClientRects().length > 0; }; return !vis('#ordFiltersBtn') && !vis('#ordActive .ordAx'); }), 'the Cancelled pile steps the Filters button and its chip aside');
    await page.click('#ordChips [data-pile=""]'); await sleep(700);
    check(await page.evaluate(() => Orders.view().act) === 'hold' && sameSet(await rids(), ids('held', 'qr', 'both', 'marked')) && (await chipsNow()).join() === 'act=Release Hold', 'and Open Orders brings the filter back as it was');

    // ═══ 5 · live ═══
    // the hold of the held order is released (what a Release hold press ends with: the hold is gone, the order goes on to its sheet and keeps the Release Hold seal), then the page draws the list as it does after any release
    await page.evaluate(ID => { const r = Orders.rows().find(x => String(x.order.receiptId) === ID.held); r.hold = null; r.state = 'nested'; r.poolIds = ['pool-held']; r.releaseHold = { at: Date.now(), by: 'Test Operator', id: 'rel-live' }; Review.syncOrderItems(); Orders.render(); }, ID);
    await sleep(800);
    check(sameSet(await rids(), ids('qr', 'both', 'marked')) && await count() === '3 of 7 orders', 'a hold released: the order leaves the Release Hold filter at once (the others stay: their Release Hold is live): ' + await rids());
    await openPanel();
    check((await page.evaluate(() => [...document.querySelectorAll('#ordFiltersPop .ordFg')].find(s => s.querySelector('h4').textContent === 'Needs action').textContent.replace(/\s+/g, ' '))).includes('Release Hold3'), 'and the chip counts follow: Release Hold 3');
    await closePanel();
    await press('hold'); await press('qr');   // Print QR only
    { const got = await rids(); check(sameSet(got, ids('qr', 'both')), 'Print QR alone: qr and both: ' + got + ' (act ' + await page.evaluate(() => Orders.view().act) + ')'); }
    // the QR label printed: the record the print press writes, then the page's own settle (Orders.render)
    await page.evaluate(ID => {
      const r = Orders.rows().find(x => String(x.order.receiptId) === ID.qr), at = Date.now();
      const rec = { key: r.key, receiptId: String(r.order.receiptId), transactionId: String(r.line.transactionId), sku: r.line.sku, title: r.line.title, category: 'Unknown SKU', kind: '', state: 'completed', how: 'print', completedAt: at, completedBy: 'Test Operator', printedAt: at, printedBy: 'Test Operator', prints: 1, lastPrintedAt: at, lastPrintedBy: 'Test Operator', hasLabel: true, stamps: [{ how: 'print', at, by: 'Test Operator' }] };
      B.maps.customDone = Object.assign({}, B.maps.customDone, { [r.key]: rec }); r.spec.customDone = rec; r.problems = []; Review.syncOrderItems(); Orders.render();   // (a completed line asks nothing any more: what the page's settle, Orders.interpretAll, makes of it)
    }, ID);
    await sleep(900);
    check(sameSet(await rids(), ids('both')) && await count() === '1 of 7 orders', 'a QR label printed: the order leaves the Print QR filter at once: ' + await rids() + ' (' + await count() + ')');
    await openPanel();
    const afterPrint = await page.evaluate(() => [...document.querySelectorAll('#ordFiltersPop .ordFg')].find(s => s.querySelector('h4').textContent === 'Needs action').textContent.replace(/\s+/g, ' '));
    check(/Release Hold \+ Print QR2/.test(afterPrint) && /Release Hold2/.test(afterPrint) && /Print QR1/.test(afterPrint), 'and the counts follow: ' + afterPrint);
    await closePanel();
    // everything acted on: the other two orders are finished the same ways (the second line's hold released, the first line's label printed, the Send to Review answered)
    await page.evaluate(ID => {
      const [l1, l2] = Orders.rows().filter(x => String(x.order.receiptId) === ID.both), at = Date.now();
      l2.hold = null; l2.state = 'nested'; l2.poolIds = ['pool-l2']; l2.problems = []; l1.problems = [];
      const rec = { key: l1.key, receiptId: ID.both, transactionId: String(l1.line.transactionId), sku: l1.line.sku, title: l1.line.title, category: 'Unknown SKU', kind: '', state: 'completed', how: 'print', completedAt: at, completedBy: 'Test Operator', printedAt: at, printedBy: 'Test Operator', prints: 1, lastPrintedAt: at, lastPrintedBy: 'Test Operator', hasLabel: true, stamps: [{ how: 'print', at, by: 'Test Operator' }] };
      B.maps.customDone = Object.assign({}, B.maps.customDone, { [l1.key]: rec }); l1.spec.customDone = rec; Review.syncOrderItems(); Orders.render();
    }, ID);
    await sleep(800);
    check((await rids()).length === 0 && await page.evaluate(() => /Nothing matches these filters/.test(document.getElementById('ordBody').textContent)), 'with its label printed the last Print QR order leaves, and the list says "Nothing matches these filters" (with its Show everything)');
    await openPanel();
    check((await pressed()).join() === 'qr' && await page.evaluate(() => !!document.querySelector('#ordFiltersPop .ordChip[data-f="act"][data-v="qr"]')), 'the filter that is on stays in the panel, so it can be switched off');
    await closePanel();
    await page.click('#ordClear'); await sleep(700);
    check(await page.evaluate(() => Orders.view().act) === null && (await rids()).length === 7, 'Show everything lets go of the filter');
    await page.evaluate(async ID => { await CharmNestReviewMark.resolve(ID.marked, { by: 'Test Operator', how: 'ok' }); Review.syncOrderItems(); Orders.render(); }, ID);
    await sleep(1200);

    // ═══ 6 · a group that could only do nothing, and the saved view ═══
    const quiet = await page.evaluate(() => ({ wrapHidden: document.getElementById('ordFilters').hidden, groups: [...document.querySelectorAll('#ordFiltersPop .ordFg h4')].map(h => h.textContent) }));
    check(!quiet.groups.includes('Needs action') && quiet.wrapHidden, 'nothing is open any more and nothing is on: the group is not drawn (it could only do nothing), and with no group left the Filters button is not drawn either: ' + JSON.stringify(quiet));
    await page.evaluate(() => { Orders.view().act = 'qr'; Orders.render(); }); await sleep(500);
    await openPanel();
    const kept = await page.evaluate(() => { const s = [...document.querySelectorAll('#ordFiltersPop .ordFg')].find(x => x.querySelector('h4').textContent === 'Needs action'); return s ? [...s.querySelectorAll('.ordChip')].map(c => (c.dataset.v || 'any') + ':' + c.getAttribute('aria-pressed')).join() : null; });
    check(kept === 'any:false,qr:true', 'but while one is on the group is drawn, with that chip on, so it can be switched off: ' + kept);
    await closePanel();
    check(await page.evaluate(() => JSON.stringify(Orders.view()).includes('"act":"qr"') && !!document.getElementById('ordActive').textContent.includes('Print QR')), 'the filter is part of the Orders view that is saved with the workspace (Orders.view().act), and its chip says "Print QR"');
    await clearAll();
    check(w.errors.length === 0, 'no page errors: ' + w.errors.join(' | '));
  } catch (e) { check(false, 'the test itself failed: ' + (e && e.stack || e)); }
  await w.close();

  // ═══ 6b · every line has an open action: the group could only do nothing ═══
  const v = await world({ viewport: { width: 1500, height: 900 }, facts: false });
  try {
    await seed(v.page, ['rqQr']);
    await v.page.waitForTimeout(700);
    await v.page.click('#ordFiltersBtn').catch(() => {});
    const only = await v.page.evaluate(() => ({ wrap: document.getElementById('ordFilters').hidden, groups: [...document.querySelectorAll('#ordFiltersPop .ordFg h4')].map(h => h.textContent) }));
    check(!only.groups.includes('Needs action'), 'one order, and it has an open Print QR: the filter would show everything, so the group is not drawn: ' + JSON.stringify(only));
    check(v.errors.length === 0, 'no page errors in the second page: ' + v.errors.join(' | '));
  } catch (e) { check(false, 'the second page failed: ' + (e && e.stack || e)); }
  await v.close();

  // ═══ 5b · a REAL press of Release hold on the card ═══
  const u = await world({ viewport: { width: 1500, height: 900 }, facts: false });
  try {
    await seed(u.page, ['rqHeld', 'rqPlain', 'rqQr']);
    await u.page.waitForTimeout(700);
    await u.page.evaluate(ID => { const by = rid => Orders.rows().find(r => String(r.order.receiptId) === rid); const p = by(ID.plain); p.problems = []; p.state = 'nested'; p.poolIds = ['pool-plain']; const h = by(ID.held); h.problems = []; h.state = 'held'; h.hold = 'Held by the test'; h.poolIds = []; Review.syncOrderItems(); Orders.render(); }, ID);
    await u.page.waitForTimeout(600);
    // the unknown-SKU order is sent to Review too (the mark API; its card button says "In Review" already, so it cannot be pressed): it now has two cards in Review, the unknown SKU's (its first, Release Hold greyed) and the mark's (Release Hold can be pressed)
    await u.page.evaluate(async rid => { await CharmNestReviewMark.mark(rid, { by: 'Test Operator' }); Review.syncOrderItems(); Orders.render(); }, ID.qr); await u.page.waitForTimeout(1500);
    await u.page.click('#ordFiltersBtn'); await u.page.waitForSelector('#ordFiltersPop:not([hidden])');
    await u.page.click('#ordFiltersPop .ordChip[data-f="act"][data-v="hold"]'); await u.page.waitForTimeout(600);
    await u.page.keyboard.press('Escape'); await u.page.waitForTimeout(150);
    const one = await u.page.evaluate(() => [...new Set(Orders.walkRows().map(r => String(r.order.receiptId)))]);
    check([...one].sort().join() === [ID.held, ID.qr].sort().join() && !!(await u.page.$(`#ordItems [data-rid="${ID.held}"] .relHold`)), 'the Release Hold filter holds the held order (its own Release hold button) and the unknown-SKU order whose SECOND card has a Release Hold to press: ' + one);
    await u.page.click(`#ordItems [data-rid="${ID.held}"] .relHold`); await u.page.waitForTimeout(2200);
    const gone = await u.page.evaluate(() => ({ rows: [...new Set(Orders.walkRows().map(r => String(r.order.receiptId)))], empty: /Nothing matches these filters/.test(document.getElementById('ordBody').textContent), held: Orders.rows().some(r => Orders.heldNow(r)), relBtn: document.querySelectorAll('#ordItems .relHold:not([data-gate])').length, cardRel: ReviewRelease.of(Review.pieceItemFor(Orders.rows().find(r => String(r.order.receiptId) === '4196000001')).src).how }));
    // (the real release makes the line up again from scratch: the released order is an unknown SKU now, so its Review card has a Release Hold that can be pressed (anyway): it is still a Release Hold order, but no longer a held one)
    check(!gone.held && !gone.relBtn && gone.cardRel === 'anyway' && gone.rows.join() === [ID.held, ID.qr, ID.plain].sort().join(), 'a REAL press of Release hold: nothing is on hold and its card has no Release hold button any more; the order is in the filter only through its Review card (Release Hold "anyway"), as the other unknown-SKU orders are: ' + JSON.stringify(gone));
    check(u.errors.length === 0, 'no page errors in the third page: ' + u.errors.join(' | '));
  } catch (e) { check(false, 'the third page failed: ' + (e && e.stack || e)); }
  await u.close();

  console.log(fails.length ? `\nFAIL: ${fails.length} of ${nCheck}\n - ` + fails.join('\n - ') : `\norders-filter-release-qr: ${nCheck} checks passed`);
  process.exit(fails.length ? 1 : 0);
})();
