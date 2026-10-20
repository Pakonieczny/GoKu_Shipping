// ONE ORDER, ONE ROW (ENGONE, 13 Oct 2026). Paul: an Etsy order of two lines (one Silver, one Gold, the same design and words) was drawn as TWO order rows in the Orders tab, each with
// its own number, time, personalisation, pictures and Send to Review: "This is the same order and should be shown as one order with 2 pieces".
// Offline only: the real sorter page in headless Chromium over the fake site (_orderGroupWorld.cjs), made-up buyers and words ("Name One"), no Etsy, no paid AI, nothing real written.
// The two APIs behind Send to Review (CharmNestReviewMark, CharmNestReviewHold) are STUBS here: this test proves the Orders tab, not the Review model.
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/order-group.cjs
// Proves: an order of two lines is ONE entry with its number, time, personalisation and Ship by once and each piece listed under it with what differs (options, state, Qty, its own pictures);
// ONE Send to Review for it (pressed: one mark call for the order, then "In Review"); a three-line order and a pair + necklace order group the same way; a single line and a pair line alone
// are drawn exactly as a row always was; the counts (Open Orders, the tab, charms) do not change; a press on each piece opens the order window on THAT piece (picked in "Its pieces"), a press
// on the top opens the order; filters, search and the On hold pile still work on a grouped order; the cards view groups too; Previous and Next walk orders, not lines; nothing overflows at 900 px.
const assert = require('assert/strict');
const G = require('./_orderGroupWorld.cjs');
const results = [];
let tag = '';
const ok = (name, cond, extra) => { assert.ok(cond, tag + name + (extra ? ' :: ' + extra : '')); results.push(tag + name); console.log('  ✓ ' + tag + name); };

async function run(mode) {
  const w = await G.open({ width: 2000, sandbox: mode === 'sandbox' });
  if (!w) { console.log('order-group: skipped (playwright-core not found; set PW_DIR)'); return false; }
  tag = mode === 'sandbox' ? '[sandbox] ' : '';
  const { page } = w, O = G.ORDERS, K = G.keyOf;
  const entry = async rid => (await G.entries(page)).find(e => e.rid === rid);
  const chips = () => page.evaluate(() => ({ open: document.querySelector('#ordChips [data-pile=""] b')?.textContent, hold: document.querySelector('#ordChips [data-pile="hold"] b')?.textContent || null, tab: document.getElementById('tabOrdersN')?.textContent, charms: document.getElementById('ordCharmTotal')?.textContent.replace(/\s+/g, ' ').trim() }));
  const closeWin = async () => { await page.evaluate(() => { const d = document.getElementById('orderWin'); if (d && d.open) d.close(); }); await page.waitForFunction(() => !document.getElementById('orderWin')?.open); };
  try {
    const names = ['twoPiece', 'three', 'single', 'pair', 'pairOnly', 'twoMore'];
    await G.seed(page, names);
    await page.waitForSelector('#ordItems [data-key]');
    // the stubs for Send to Review (what REVMARK and REVHOLD publish, same signatures)
    await page.evaluate(() => {
      const S = window.__rv = { marked: new Set(), calls: [], opens: [], listeners: [], gate: null };
      window.CharmNestReviewMark = {
        isMarked: rid => S.marked.has(String(rid)),
        mark: (rid, o) => { S.calls.push({ rid: String(rid), by: o && o.by }); return new Promise(res => { S.gate = () => { S.marked.add(String(rid)); res({ ok: true, rid: String(rid) }); }; }); },
        open: rid => { S.opens.push(String(rid)); return S.marked.has(String(rid)); },
        onChange: fn => { S.listeners.push(fn); return () => {}; }
      };
      window.CharmNestReviewHold = { canHold: () => ({ ok: true, sheets: [] }), sheetsHeldBy: () => [] };
      window.__nat = []; Review.items = () => __nat; Review.openOrders = () => new Map();   // (the seeded orders raise questions of their own: this keeps them out of "In Review")
      Orders.render();
    });
    await page.waitForFunction(() => document.querySelector('#ordItems .revSendBtn'));
    await page.waitForTimeout(300);
    const all = await G.entries(page);

    // 1. one order of two lines = ONE entry, the number once
    const two = await entry(O.twoPiece.rid);
    ok('1 an order of two lines (Silver + Gold) is ONE entry in the list, holding both pieces in order', all.filter(e => e.rid === O.twoPiece.rid).length === 1 && two.grouped && two.keys.length === 2 && two.keys[0] === K('twoPiece', 0) && two.keys[1] === K('twoPiece', 1), JSON.stringify(two.keys));
    const said = await page.evaluate(rid => { const n = document.querySelector(`#ordItems > [data-rid="${rid}"]`), t = n.innerText; const times = (t.match(/\d{1,2}:\d\d [ap]\.m\./g) || []).length; return { num: (t.match(new RegExp(rid, 'g')) || []).length, time: times, words: (t.match(/Name One/g) || []).length, ship: (t.match(/Ship by/g) || []).length, count: /2 pieces/.test(t), label: n.getAttribute('aria-label'), perso: (t.match(/Personalisation/gi) || []).length }; }, O.twoPiece.rid);
    ok('2 its order number, time, personalisation and Ship by are said once, and the card says "2 pieces"', said.num === 1 && said.time === 1 && said.words === 1 && said.perso === 1 && said.ship === 1 && said.count && /2 pieces/.test(said.label), JSON.stringify(said));
    const pieces = await page.evaluate(rid => [...document.querySelectorAll(`#ordItems > [data-rid="${rid}"] .ogPiece`)].map(p => ({ key: p.dataset.key, t: p.innerText.replace(/\s+/g, ' '), pics: p.querySelectorAll('[data-vector]').length + p.querySelectorAll('[data-listing]').length, qty: (p.querySelector('.rowFacts') || {}).textContent, st: (p.querySelector('.ost') || {}).textContent })), O.twoPiece.rid);
    ok('3 each piece shows what differs: its own options, state, Qty and its own two pictures', pieces.length === 2 && /Silver \+ Engrave/.test(pieces[0].t) && /18"/.test(pieces[0].t) && /14k Gold \+ Engrave/.test(pieces[1].t) && /16"/.test(pieces[1].t) && !/14k Gold/.test(pieces[0].t) && pieces.every(p => p.pics === 2 && /^Qty 1/.test(p.qty) && /pulled/i.test(p.st)), JSON.stringify(pieces));

    // 4. one Send to Review for the order
    ok('4 ONE Send to Review for the order (not one per piece), at the top, beside Ship by', two.buttons === 1 && (await page.evaluate(rid => { const n = document.querySelector(`#ordItems > [data-rid="${rid}"]`), b = n.querySelector('.revSendBtn'), head = n.querySelector('.ogHead'); return !!head && head.contains(b) && b.textContent === 'Send to Review'; }, O.twoPiece.rid)));
    await page.click(`#ordItems > [data-rid="${O.twoPiece.rid}"] .revSendBtn`);
    const busy = await page.evaluate(() => ({ calls: __rv.calls.slice() }));
    await page.evaluate(() => __rv.gate());
    await page.waitForFunction(rid => [...document.querySelectorAll(`#ordItems [data-rid="${rid}"] .revSendBtn`)].every(b => b.textContent === 'In Review'), O.twoPiece.rid);
    ok('4b pressed once: ONE mark call, for the order, and it then says "In Review" (the order window stayed shut)', busy.calls.length === 1 && busy.calls[0].rid === O.twoPiece.rid && !(await page.evaluate(() => document.getElementById('orderWin').open)), JSON.stringify(busy.calls));

    // 5. a three-line order, a pair + necklace order
    const three = await entry(O.three.rid), pairMix = await entry(O.pair.rid);
    const threeT = three.text;
    ok('5a a three-line order is one entry of three pieces ("4 pieces": the third line is Qty 2), its three charms each on their own piece', three.grouped && three.keys.length === 3 && three.buttons === 1 && three.nums === 1 && /4 pieces/.test(threeT) && /LEAF_CHARM/.test(threeT) && /COMEDY 89578/.test(threeT) && /MINI_0445/.test(threeT), threeT.slice(0, 200));
    ok('5b the words that differ from piece to piece stay on their pieces (no shared personalisation line)', (await page.evaluate(rid => { const n = document.querySelector(`#ordItems > [data-rid="${rid}"]`); return { head: n.querySelector('.ogHead').innerText.includes('Name Two'), pieces: [...n.querySelectorAll('.ogPiece')].map(p => /Name Two/.test(p.innerText)) }; }, O.three.rid)).pieces.join() === 'false,true,true');
    ok('5c a pair line plus a necklace line: one entry, "3 pieces" (Left + Right and one necklace); the pair keeps its "Left + Right"', pairMix.grouped && pairMix.keys.length === 2 && /3 pieces/.test(pairMix.text) && /Left \+ Right/.test(pairMix.text) && pairMix.buttons === 1, pairMix.text.slice(0, 220));

    // 6. single line and a pair row alone: exactly as a row always was
    const single = await entry(O.single.rid), pairOnly = await entry(O.pairOnly.rid);
    ok('6 an order of one line (and a pair line alone) is drawn as a row exactly as before: no group, its own key, one number, one button, Qty and Ship by together', !single.grouped && single.keys[0] === K('single', 0) && single.nums === 1 && single.buttons === 1 && /Qty 1 · Ship by/.test(single.text) && !pairOnly.grouped && /Left \+ Right/.test(pairOnly.text) && pairOnly.buttons === 1, single.text.slice(0, 200));

    // 7. counts unchanged: distinct orders, the tab, the charms (pieces)
    const c = await chips();
    ok('7 the counts are the orders and pieces they always were: Open Orders 6, the tab 6, 14 charms', c.open === '6' && c.tab === '6' && /^14 charms/.test(c.charms), JSON.stringify(c));

    // 8. a press on each piece opens the order window on THAT piece; the top opens the order
    const pick = async key => {
      await page.click(`#ordItems [data-key="${key}"] .rowFacts`);
      await page.waitForFunction(() => document.getElementById('orderWin')?.open);
      await page.waitForFunction(k => !!document.querySelector(`#orderWin .owPcRow[data-piece="${k}"]`), key, { timeout: 20000 });
      const st = await page.evaluate(() => ({ key: OrderWin.key(), sel: [...document.querySelectorAll('#orderWin .owPcRow.sel')].map(n => n.dataset.piece) }));
      await closeWin(); return st;
    };
    const p1 = await pick(K('twoPiece', 0)), p2 = await pick(K('twoPiece', 1)), p3 = await pick(K('three', 2));
    ok('8a pressing piece 1, piece 2 (and piece 3 of the three-line order) opens the order window with THAT piece picked in "Its pieces"', p1.key === K('twoPiece', 0) && p1.sel.join() === K('twoPiece', 0) && p2.key === K('twoPiece', 1) && p2.sel.join() === K('twoPiece', 1) && p3.key === K('three', 2) && p3.sel.join() === K('three', 2), JSON.stringify([p1, p2, p3]));
    await page.click(`#ordItems > [data-rid="${O.twoPiece.rid}"] .onum`);
    await page.waitForFunction(() => document.getElementById('orderWin')?.open);
    const top = await page.evaluate(() => ({ key: OrderWin.key(), sel: [...document.querySelectorAll('#orderWin .owPcRow.sel')].length }));
    ok('8b pressing the top of the card opens the order itself (no piece picked)', top.key === K('twoPiece', 0) && top.sel === 0, JSON.stringify(top));
    // 8c Previous and Next walk the pieces in the order the list draws them (the order's pieces together, then the next order)
    const lines = (await G.entries(page)).flatMap(e => e.keys), at = lines.indexOf(K('twoPiece', 0));
    const walk = await page.evaluate(() => document.getElementById('owPos')?.textContent);
    await page.click('#owNext'); await page.waitForFunction(k => OrderWin.key() === k, lines[at + 1], { timeout: 10000 });
    const second = await page.evaluate(() => document.getElementById('owPos')?.textContent);
    await page.click('#owNext'); await page.waitForFunction(k => OrderWin.key() === k, lines[at + 2], { timeout: 10000 });
    ok('8c Next walks the pieces as the list draws them: piece 1, piece 2 of the same order, then the next order\'s first piece', walk === `${at + 1} of ${lines.length}` && second === `${at + 2} of ${lines.length}` && lines[at + 1] === K('twoPiece', 1) && !lines[at + 2].startsWith(O.twoPiece.rid), JSON.stringify({ walk, second, at, lines }));
    await closeWin();

    // 9. the keyboard: Enter on a piece opens it
    await page.focus(`#ordItems [data-key="${K('twoPiece', 1)}"]`); await page.keyboard.press('Enter');
    await page.waitForFunction(() => document.getElementById('orderWin')?.open);
    await page.waitForFunction(k => !!document.querySelector(`#orderWin .owPcRow.sel[data-piece="${k}"]`), K('twoPiece', 1), { timeout: 20000 });
    ok('9 Enter on a focused piece opens the order window on it', (await page.evaluate(() => ({ key: OrderWin.key(), sel: [...document.querySelectorAll('#orderWin .owPcRow.sel')].map(n => n.dataset.piece) }))).sel.join() === K('twoPiece', 1));
    await closeWin();

    // 10. filters and search still work on a grouped order
    await page.evaluate(() => { document.getElementById('ordQ').value = ''; });
    await page.fill('#ordQ', O.twoPiece.rid); await page.waitForTimeout(300);
    const found = await G.entries(page);
    ok('10a search for the order number: the one grouped order', found.length === 1 && found[0].grouped && found[0].keys.length === 2, JSON.stringify(found.map(e => [e.rid, e.keys.length])));
    await page.fill('#ordQ', ''); await page.waitForTimeout(200);
    // (FILTERBAR, Paul 17 Oct 2026: the Metal select is the Metal chip group in the Filters panel, #ordFiltersBtn; Any = the select's empty answer)
    await page.click('#ordFiltersBtn'); await page.waitForSelector('#ordFiltersPop:not([hidden])');
    const metals = await page.evaluate(() => [...document.querySelectorAll('#ordFiltersPop .ordChip[data-f="metal"]')].map(o => o.dataset.v + '=' + o.textContent));
    await page.click('#ordFiltersPop .ordChip[data-f="metal"][data-v="silver"]'); await page.waitForTimeout(500);
    const silver = await G.entries(page), sTwo = silver.find(e => e.rid === O.twoPiece.rid);
    ok('10b the Silver filter shows the order\'s Silver piece (a filter leaves one piece: drawn as a row), never the Gold piece', !!sTwo && !sTwo.grouped && sTwo.keys.join() === K('twoPiece', 0) && !silver.some(e => e.keys.includes(K('twoPiece', 1))), JSON.stringify([metals, silver.map(e => [e.rid, e.keys])]));
    await page.click('#ordFiltersPop .ordChip[data-f="metal"][data-v=""]'); await page.waitForTimeout(500);
    await page.keyboard.press('Escape'); await page.waitForSelector('#ordFiltersPop', { state: 'hidden' });

    // 11. a held piece of a grouped order; one Release hold for the order (NOHOLDPILE, Paul 20 Oct 2026: no On hold pile, the order stays in Open Orders)
    await page.evaluate(k => { Orders.rows().find(r => r.key === k).hold = { by: 'Test Operator', at: Date.now(), reason: 'test' }; Orders.render(); }, K('twoPiece', 1));
    await page.waitForFunction(rid => document.querySelector(`#ordItems > [data-rid="${rid}"] .relHold:not([data-gate])`), O.twoPiece.rid);
    const heldAll = await page.evaluate(rid => ({ rel: document.querySelectorAll(`#ordItems > [data-rid="${rid}"] .relHold:not([data-gate])`).length, hold: !!document.querySelector('#ordChips [data-pile="hold"]'), open: document.querySelector('#ordChips [data-pile=""] b').textContent }), O.twoPiece.rid);
    ok('11a a held piece: ONE Release hold on the order card in Open Orders, no On hold chip, Open Orders unchanged', heldAll.rel === 1 && heldAll.hold === false && heldAll.open === '6', JSON.stringify(heldAll));
    await page.evaluate(k => { delete Orders.rows().find(r => r.key === k).hold; Orders.render(); }, K('twoPiece', 1));
    await page.waitForTimeout(300);

    // 12. the cards view groups too: one full-width card, its pieces side by side, one button
    await page.click('#ordViewSeg [data-view="cards"]'); await page.waitForSelector('#ordItems.ordCards .ordGroup');
    const cards = await page.evaluate(rid => { const n = document.querySelector(`#ordItems.ordCards > [data-rid="${rid}"]`), r = n.getBoundingClientRect(), host = document.getElementById('ordItems').getBoundingClientRect(), ps = [...n.querySelectorAll('.ogPiece')].map(p => p.getBoundingClientRect()); return { full: Math.abs(r.width - host.width) < 2, side: ps.length === 2 && Math.abs(ps[0].top - ps[1].top) < 2 && ps[1].left > ps[0].left, btn: n.querySelectorAll('.revSendBtn').length }; }, O.twoPiece.rid);
    ok('12 the cards view: the order is one full-width card, its two pieces side by side, one Send to Review', cards.full && cards.side && cards.btn === 1, JSON.stringify(cards));
    await page.click('#ordViewSeg [data-view="list"]');

    // 13. a narrow screen: nothing spills out of the card and the page does not scroll sideways
    await page.setViewportSize({ width: 900, height: 800 }); await page.waitForTimeout(300);
    const narrow = await page.evaluate(() => ({ spill: [...document.querySelectorAll('#ordItems .ordGroup *')].filter(n => { const g = n.closest('.ordGroup').getBoundingClientRect(), r = n.getBoundingClientRect(); return r.width > 0 && (r.right > g.right + 1 || r.left < g.left - 1); }).length, page: document.documentElement.scrollWidth <= innerWidth + 1 }));
    ok('13 at 900 px nothing of a grouped order spills out of its card and the page does not scroll sideways', narrow.spill === 0 && narrow.page, JSON.stringify(narrow));
    await page.setViewportSize({ width: 2000, height: 900 });

    ok('14 no script error on the page throughout, and nothing was asked of any address but the local fake', w.errors.length === 0 && w.foreign.length === 0, w.errors.concat(w.foreign).join(' | '));
  } finally { await w.close(); }
  return true;
}
(async () => {
  console.log('== production =='); if (!(await run('production'))) return;
  console.log('== sandbox =='); await run('sandbox');
  console.log(`order-group: ${results.length} checks passed`);
})().catch(e => { console.error('FAIL:', e && e.message || e); process.exit(1); });
