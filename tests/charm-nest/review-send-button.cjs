// Send to Review, the button on each row of the Orders tab (charm-nest-review-send.js). Offline only: the real sorter page in
// headless Chromium over the fake site (_orderFactsWorld.cjs), made-up buyers, no Etsy, no paid AI, nothing real written. The two
// APIs the button calls (CharmNestReviewMark, CharmNestReviewHold) are STUBS here: this test proves the button, the states, the
// words and "never a second add", not the Review model (REVMARK's tests) nor the sheet hold (REVHOLD's).
//   PW_DIR=/opt/node22/lib/node_modules/playwright/node_modules node tests/charm-nest/review-send-button.cjs
// Checks: no mark API = no button at all; with it exactly one button per row (list and cards), under "Qty · Ship by", left
// aligned with it, a normal small button; a press: disabled + aria-busy + small spinner + the same words while it works, the mark call
// made ONCE with the order and the signed-in name, the order window not opened; afterwards "In Review" on every line of that order;
// a second press only opens it (open called, mark not called again); an order already on a sheet gets ONE plain line naming the
// sheet that says it stays and the sheet is held; a piece that cannot be held gets one line and nothing is marked; a failed save
// says so and the button can be pressed again; another computer's change (the API's callback) repaints without a redraw; an
// order the system already put in Review shows "In Review" and opens that card; the keyboard presses it; nothing overflows at 900 px.
const path = require('path'), assert = require('assert/strict');
const { world, seed, FIXTURES } = require('./_orderFactsWorld.cjs');
const results = [];
const ok = (name, cond, extra) => { assert.ok(cond, name + (extra ? ' :: ' + extra : '')); results.push(name); console.log('  ✓ ' + name); };

(async () => {
  const w = await world({ viewport: { width: 2400, height: 900 } });   // (wide, as Paul's screen: the actions are the column at the right)
  if (!w) { console.log('review-send-button: skipped (playwright-core not found; set PW_DIR)'); return; }
  const { page } = w;
  try {
    const single = FIXTURES.single.rid, multi = FIXTURES.multi.rid, pair = FIXTURES.pairMatching.rid;
    await seed(page, ['single', 'multi', 'pairMatching']);
    await page.waitForSelector('#ordItems [data-key]');
    const rowsDrawn = () => page.evaluate(() => [...document.querySelectorAll('#ordItems > [data-rid]')].map(n => ({ key: n.dataset.key || n.dataset.first, rid: n.dataset.rid, btns: n.querySelectorAll('.revSendBtn').length })));   // (ONE ORDER, ONE ROW: an order of several lines is ONE entry of the list)

    // 1. no mark API on the page (the real one is taken away for this check): no button anywhere (the page is exactly as before)
    await page.evaluate(() => { window.__realMark = window.CharmNestReviewMark; delete window.CharmNestReviewMark; Orders.render(); });
    await page.waitForTimeout(200);
    let rows = await rowsDrawn();
    ok('1 no mark API: no row has the button', rows.length >= 3 && rows.every(r => r.btns === 0), JSON.stringify(rows));

    // the stubs (what REVMARK and REVHOLD publish, same signatures)
    await page.evaluate(() => {
      const S = window.__rv = { marked: new Set(), calls: [], opens: [], listeners: [], gate: null, fail: null, canHold: {}, held: {} };
      window.CharmNestReviewMark = {
        isMarked: rid => S.marked.has(String(rid)),
        mark: (rid, o) => { S.calls.push({ rid: String(rid), by: o && o.by }); return new Promise(res => { S.gate = () => { if (S.fail) { const e = S.fail; S.fail = null; return res({ ok: false, error: e }); } S.marked.add(String(rid)); res({ ok: true, rid: String(rid) }); }; }); },
        open: rid => { S.opens.push(String(rid)); return S.marked.has(String(rid)); },
        onChange: fn => { S.listeners.push(fn); return () => {}; }
      };
      window.CharmNestReviewHold = { canHold: rid => S.canHold[String(rid)] || { ok: true, sheets: [] }, sheetsHeldBy: rid => S.held[String(rid)] || [] };
      window.__nat = [];                                  // what the Review tab itself holds (the seeded orders raise questions of their own: this keeps them out)
      Review.items = () => __nat;
      window.__fire = (rid, kind) => { for (const f of S.listeners) f({ rid: String(rid), kind }); };
      Orders.render();
    });
    await page.waitForFunction(() => document.querySelector('#ordItems .revSendBtn'));
    rows = await rowsDrawn();
    ok('2 with the API: exactly one button on every row (one per ORDER)', rows.length >= 3 && rows.every(r => r.btns === 1), JSON.stringify(rows));

    // layout: the label, under Qty · Ship by, left aligned with it, a small normal button
    const lay = await page.evaluate(() => [...document.querySelectorAll('#ordItems > [data-rid]')].map(n => {
      const b = n.querySelector('.revSendBtn'), f = n.querySelector('.rowFacts'), br = b.getBoundingClientRect(), fr = f.getBoundingClientRect(), row = n.getBoundingClientRect(), grouped = n.classList.contains('ordGroup');
      // (a row: under "Qty · Ship by", left aligned with it; an order of several lines: in the top of its card, after "Ship by", the order's one button)
      return { text: b.textContent, place: grouped ? (n.querySelector('.ogHead').contains(b) && br.left >= fr.right - 1 && Math.abs((br.top + br.height / 2) - (fr.top + fr.height / 2)) < 30) : (br.top >= fr.bottom - 1 && Math.abs(br.left - fr.left) <= 1.5), h: br.height, w: br.width, inside: br.right <= row.right + 1 && br.left >= row.left - 1, type: b.type, cls: b.className };
    }));
    ok('3 the label is "Send to Review", under "Qty · Ship by" (an order of several lines: beside its Ship by at the top of its card), small, inside the row', lay.length >= 3 && lay.every(l => l.text === 'Send to Review' && l.place && l.h > 18 && l.h < 44 && l.w < 260 && l.inside && l.type === 'button' && /ghost/.test(l.cls)), JSON.stringify(lay));

    // 4. press: busy state, one call, signed-in name, the order window stays shut
    const sel = rid => `#ordItems > [data-rid="${rid}"] .revSendBtn`;
    await page.click(sel(single));
    const busy = await page.evaluate(rid => { const b = document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`); return { dis: b.disabled, busy: b.getAttribute('aria-busy'), spin: !!b.querySelector('.spin'), text: b.textContent, calls: __rv.calls.length, win: !!document.getElementById('orderWin')?.open }; }, single);
    ok('4a pressed: disabled, aria-busy, a small spinner and the same words', busy.dis && busy.busy === 'true' && busy.spin && busy.text === 'Send to Review', JSON.stringify(busy));
    ok('4b the mark call was made once, for this order, under the signed-in name; the order window did not open', busy.calls === 1 && !busy.win);
    const by = await page.evaluate(() => ({ call: __rv.calls[0], me: CNEmployee.name() }));
    ok('4c the name sent is the signed-in login', by.call.rid === single && by.call.by && by.call.by === by.me, JSON.stringify(by));
    const again = await page.evaluate(async rid => { const r = await ReviewSend.press(rid); return { r, calls: __rv.calls.length }; }, single);
    ok('4d a second press while it works adds nothing', again.r.error === 'busy' && again.calls === 1);
    await page.evaluate(() => __rv.gate());
    await page.waitForFunction(rid => { const b = document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`); return b && b.textContent === 'In Review' && !b.disabled; }, single);
    const done = await page.evaluate(rid => { const b = document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`); return { text: b.textContent, busy: b.getAttribute('aria-busy'), msgs: document.querySelectorAll(`#ordItems > [data-rid="${rid}"] .revSendMsg`).length, label: b.getAttribute('aria-label') }; }, single);
    ok('4e done: "In Review", not busy, no line (the order was on no sheet)', done.text === 'In Review' && !done.busy && done.msgs === 0 && /in Review/.test(done.label), JSON.stringify(done));

    // 5. pressing it again opens it in Review and adds nothing
    await page.click(sel(single));
    const reopen = await page.evaluate(() => ({ calls: __rv.calls.length, opens: __rv.opens.slice() }));
    ok('5 a press on "In Review" opens that order in Review; the mark call is not made again', reopen.calls === 1 && reopen.opens.length === 1 && reopen.opens[0] === single, JSON.stringify(reopen));

    // 6. an order of two lines (ONE card since 13 Oct): its one button marks the order
    await page.evaluate(() => { __rv.canHold = {}; });
    const multiRows = await page.evaluate(rid => ({ buttons: document.querySelectorAll(`#ordItems > [data-rid="${rid}"] .revSendBtn`).length, pieces: document.querySelectorAll(`#ordItems > [data-rid="${rid}"] .ogPiece`).length }), multi);
    const callsMulti = await page.evaluate(() => __rv.calls.length);
    await page.click(`#ordItems > [data-rid="${multi}"] .revSendBtn`);
    await page.evaluate(() => __rv.gate());
    await page.waitForFunction(rid => [...document.querySelectorAll(`#ordItems > [data-rid="${rid}"] .revSendBtn`)].every(b => b.textContent === 'In Review'), multi);
    ok('6 an order of several lines is ONE card with ONE button; it marks the order once and says "In Review"', multiRows.buttons === 1 && multiRows.pieces >= 2 && (await page.evaluate(() => __rv.calls.length)) === callsMulti + 1, JSON.stringify(multiRows));

    // 7. an order already on a sheet: ONE plain line naming the sheet
    await page.evaluate(rid => { __rv.canHold[rid] = { ok: true, sheets: [{ sheetId: 'sh1', name: 'SS Draft 2', setId: 'set-2026-10-12-1' }] }; __rv.held[rid] = [{ sheetId: 'sh1', name: 'SS Draft 2', setId: 'set-2026-10-12-1' }]; }, pair);
    await page.click(sel(pair));
    await page.evaluate(() => __rv.gate());
    await page.waitForSelector(`#ordItems > [data-rid="${pair}"] .revSendMsg`);
    const line = await page.evaluate(rid => [...document.querySelectorAll(`#ordItems > [data-rid="${rid}"] .revSendMsg`)].map(n => n.textContent), pair);
    ok('7 on a sheet: one plain line names the sheet, says it stays and that the sheet is held', line.length === 1 && /set-2026-10-12-1 · SS Draft 2/.test(line[0]) && /Stays on/.test(line[0]) && /held until this is resolved in Review/.test(line[0]) && line[0].length < 140, JSON.stringify(line));

    // 8. a piece that cannot be held: one line, nothing marked
    const cut = FIXTURES.late.rid;
    await seed(page, ['late']);
    await page.waitForSelector(`#ordItems > [data-rid="${cut}"] .revSendBtn`);
    await page.evaluate(rid => { __rv.canHold[rid] = { ok: false, why: 'This order has a piece that is already released, so it cannot be held.' }; }, cut);
    const before = await page.evaluate(() => __rv.calls.length);
    await page.click(sel(cut));
    const nope = await page.evaluate(rid => ({ calls: __rv.calls.length, msg: [...document.querySelectorAll(`#ordItems > [data-rid="${rid}"] .revSendMsg`)].map(n => n.textContent), text: document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`).textContent, dis: document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`).disabled }), cut);
    ok('8 a piece that cannot be held: one line says so, nothing is marked, the button stays "Send to Review"', nope.calls === before && nope.msg.length === 1 && /cannot be held/.test(nope.msg[0]) && nope.text === 'Send to Review' && !nope.dis, JSON.stringify(nope));

    // 9. a failed save: said plainly, the button can be pressed again
    await page.evaluate(rid => { __rv.canHold[rid] = { ok: true, sheets: [] }; __rv.fail = 'the save did not go through'; }, cut);
    await page.click(sel(cut));
    await page.evaluate(() => __rv.gate());
    await page.waitForFunction(rid => /Not sent/.test(document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendMsg`)?.textContent || ''), cut);
    const failed = await page.evaluate(rid => ({ text: document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`).textContent, dis: document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`).disabled, marked: __rv.marked.has(rid) }), cut);
    ok('9 a failed save: "Not sent", nothing marked, the button is back and enabled', failed.text === 'Send to Review' && !failed.dis && !failed.marked, JSON.stringify(failed));

    // 10. the keyboard
    await page.keyboard.press('Shift');
    await page.focus(sel(cut));
    const ring = await page.evaluate(rid => { const b = document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`), cs = getComputedStyle(b); return { fv: b.matches(':focus-visible'), w: parseFloat(cs.outlineWidth), st: cs.outlineStyle }; }, cut);
    ok('10a a focused button shows the app\'s focus ring', ring.fv && ring.st !== 'none' && ring.w >= 2, JSON.stringify(ring));
    const callsBefore = await page.evaluate(() => __rv.calls.length);
    await page.keyboard.press('Enter');
    await page.evaluate(() => __rv.gate());
    await page.waitForFunction(rid => document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`)?.textContent === 'In Review', cut);
    ok('10b Enter presses it, once, and the order window stays shut', (await page.evaluate(() => __rv.calls.length)) === callsBefore + 1 && !(await page.evaluate(() => document.getElementById('orderWin').open)));

    // 11. another computer's change (the API's callback) repaints without a redraw
    const keep = await page.evaluate(rid => { const n = document.querySelector(`#ordItems > [data-rid="${rid}"]`); window.__node = n; __rv.marked.delete(rid); __fire(rid, 'resolved'); return n.querySelector('.revSendBtn').textContent; }, cut);
    const same = await page.evaluate(rid => ({ text: __node.querySelector('.revSendBtn').textContent, sameNode: document.querySelector(`#ordItems > [data-rid="${rid}"]`) === __node }), cut);
    ok('11a resolved elsewhere: the button goes back to "Send to Review" on the same row, no redraw', same.text === 'Send to Review' && same.sameNode, keep);
    await page.evaluate(rid => { __rv.marked.add(rid); __fire(rid, 'marked'); }, cut);
    ok('11b marked elsewhere: it says "In Review" at once', (await page.evaluate(() => __node.querySelector('.revSendBtn').textContent)) === 'In Review');

    // 12. an order the system itself put in Review: "In Review", and a press opens that card
    await page.evaluate(rid => { __rv.marked.delete(rid); __fire(rid, 'resolved'); }, cut);
    const nat = await page.evaluate(rid => {
      const row = Orders.rows().find(r => String(r.order.receiptId) === rid);
      window.__shown = [];
      const was = Review.showCard; Review.showCard = (...a) => { __shown.push(a); };
      __nat.push({ kind: 'unmatchedSku', key: 'ord:test-natural', row, rows: [row], problem: { kind: 'unmatchedSku' }, why: 'x' });
      Orders.render();
      return row.key;
    }, cut);
    await page.waitForFunction(rid => document.querySelector(`#ordItems > [data-rid="${rid}"] .revSendBtn`)?.textContent === 'In Review', cut);
    const callsN = await page.evaluate(() => __rv.calls.length);
    await page.click(sel(cut));
    const shown = await page.evaluate(() => ({ shown: __shown.slice(), calls: __rv.calls.length }));
    ok('12 an order already in Review by itself: "In Review", a press opens that card and adds nothing', shown.calls === callsN && shown.shown.length === 1 && shown.shown[0][0] === 'ord:test-natural', JSON.stringify(shown));
    await page.evaluate(() => { __nat.length = 0; });

    // 13. the cards view carries the same button, once per card
    await page.click('#ordViewSeg [data-view="cards"]');
    await page.waitForSelector('#ordItems.ordCards .revSendBtn');
    const cards = await page.evaluate(() => [...document.querySelectorAll('#ordItems.ordCards > [data-rid]')].map(n => n.querySelectorAll('.revSendBtn').length));
    ok('13 the cards view: one button on every card (an order of several lines is one card)', cards.length >= 3 && cards.every(n => n === 1), JSON.stringify(cards));
    await page.click('#ordViewSeg [data-view="list"]');

    // 14. narrow screen (the sorter's own layout at 900 px: the actions are a bar under the row): nothing spills out of the row, the page does not scroll sideways
    await page.setViewportSize({ width: 900, height: 780 });
    await page.waitForTimeout(250);
    const narrow = await page.evaluate(() => ({ spill: [...document.querySelectorAll('#ordItems > [data-rid]')].filter(n => { const b = n.querySelector('.revSendBtn'), r = n.getBoundingClientRect(), br = b.getBoundingClientRect(); return br.right > r.right + 1 || br.left < r.left - 1; }).length, page: document.documentElement.scrollWidth <= innerWidth + 1 }));
    ok('14 at 900 px the button stays inside its row and the page does not scroll sideways', narrow.spill === 0 && narrow.page, JSON.stringify(narrow));

    // 15. the row still opens its order when pressed anywhere but the button
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(150);
    await page.click(`#ordItems > [data-rid="${single}"] .onum`);
    await page.waitForFunction(() => document.getElementById('orderWin')?.open);
    ok('15 pressing the order itself still opens the order window', true);

    ok('16 no script error on the page throughout', w.errors.length === 0, w.errors.join(' | '));
  } finally { await w.close(); }
  console.log(`review-send-button: ${results.length} checks passed`);
})().catch(e => { console.error('FAIL:', e && e.message || e); process.exit(1); });
