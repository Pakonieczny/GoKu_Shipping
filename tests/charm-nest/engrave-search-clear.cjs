// The Engrave tab's search box has a × that clears it (Paul, 18 Oct 2026: "Add a 'X' inside the search bar to clear the currently written search query"):
// the × is the Orders bar's own (.ordFindX), inside the box at its right, shown only while the box holds text. Pressing it empties the box, shows the whole list
// again at once (not after the typing pause) and leaves the focus in the box; Esc in the box does the same; an empty box leaves Esc alone. It is a button named
// "Clear search", reached with Tab, with a visible focus ring. The Orders bar's × is unchanged.
//   node tests/charm-nest/engrave-search-clear.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser check was not run'); return; }
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Engrave && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(() => {
      // three approved engravings: rows of the ONE list
      const sq = { kind: 'path', subpaths: [[['m', [0, 0]], ['l', [34, 0]], ['l', [34, 34]], ['l', [0, 34]], ['h']]], stroke: true, strokeRGB: [0, 0, 0], lwPt: .25, bbox: [0, 0, 34, 34] };
      const charm = { id: 'c', name: 'TAG', metal: 'gold', centerPt: [17, 17], widthPt: 34, heightPt: 34, areaPt2: 34 * 34, outline: sq, members: [sq], bbox: [0, 0, 34, 34], thumb: '' };
      const was = Pool.charmOf; Pool.charmOf = id => /^sc-/.test(id) ? Object.assign({ poolId: id }, charm) : was(id);
      for (const n of [1, 2, 3]) {
        const row = { key: 'sc' + n, order: { receiptId: '419521544' + n }, line: { listingId: '700' + n, sku: 'TAG_' + n, title: 'Tag', variations: [] }, spec: { designSku: 'TAG_' + n }, state: 'pooled', poolIds: ['sc-' + n], problems: [], arrivedAt: Date.now() - n };
        Engrave.items().set(row.key, { key: row.key, row, copies: row.poolIds, state: 'written', lines: ['S' + n], backs: [], approvedBy: 'Test Operator', approvedAt: Date.now() - 60000 });
      }
      CN.setMode('engrave'); Engrave.restoreView({ list: true, chosen: true }); Engrave.render();
    });
    const ROW = '#egQueue .egPlacementList .placementRow';
    await page.waitForFunction(ROW => document.querySelectorAll(ROW).length === 3, ROW);
    const state = () => page.evaluate(ROW => {
      const q = document.getElementById('egQ'), x = document.getElementById('egQX');
      return { value: q.value, x: !!x && !x.hidden, xShown: !!x && getComputedStyle(x).display !== 'none', rows: document.querySelectorAll(ROW).length, focus: document.activeElement && document.activeElement.id };
    }, ROW);

    // empty: no ×
    let s = await state();
    assert.equal(s.x || s.xShown, false, 'an empty box shows no ×: ' + JSON.stringify(s));
    const x0 = await page.evaluate(() => { const x = document.getElementById('egQX'); return { tag: x.tagName, type: x.type, label: x.getAttribute('aria-label'), cls: x.className, inBox: x.parentElement.contains(document.getElementById('egQ')) }; });
    assert.deepEqual(x0, { tag: 'BUTTON', type: 'button', label: 'Clear search', cls: 'ordFindX', inBox: true }, 'the × is a button named "Clear search", the Orders bar\'s own ×, in the box: ' + JSON.stringify(x0));

    // typing shows the × at the right inside the box, and the list narrows
    await page.click('#egQ'); await page.keyboard.type('4195215442');
    s = await state();
    assert.equal(s.x && s.xShown, true, 'typing shows the × at once: ' + JSON.stringify(s));
    await page.waitForFunction(ROW => document.querySelectorAll(ROW).length === 1, ROW);
    const geo = await page.evaluate(() => { const q = document.getElementById('egQ').getBoundingClientRect(), x = document.getElementById('egQX').getBoundingClientRect(), st = getComputedStyle(document.getElementById('egQ'));
      return { inside: x.left >= q.left && x.right <= q.right && x.top >= q.top && x.bottom <= q.bottom, right: q.right - x.right, half: x.left > q.left + q.width / 2, pad: parseFloat(st.paddingRight), w: x.width }; });
    assert(geo.inside && geo.half && geo.right <= 8 && geo.pad >= geo.w, 'the × sits inside the box at its right, and the text never runs under it: ' + JSON.stringify(geo));

    // the × clears: the box empties, the whole list is back at once (no wait for the typing pause), the focus stays in the box
    const click = await page.evaluate(ROW => { document.getElementById('egQX').click(); const q = document.getElementById('egQ'), x = document.getElementById('egQX');
      return { value: q.value, rows: document.querySelectorAll(ROW).length, focus: document.activeElement.id, x: !x.hidden }; }, ROW);
    assert.deepEqual({ value: click.value, rows: click.rows, focus: click.focus, x: click.x }, { value: '', rows: 3, focus: 'egQ', x: false }, 'the × empties the box, shows all three rows at once, hides itself and keeps the focus in the box: ' + JSON.stringify(click));
    // the same with a real pointer press (the × takes the focus on mousedown; it is given back to the box)
    await page.keyboard.type('4195215443'); await page.waitForFunction(ROW => document.querySelectorAll(ROW).length === 1, ROW);
    await page.click('#egQX');
    s = await state();
    assert.deepEqual([s.value, s.rows, s.focus, s.x], ['', 3, 'egQ', false], 'a pointer press on the × does the same: ' + JSON.stringify(s));
    await page.waitForTimeout(300);
    s = await state();
    assert.deepEqual([s.value, s.rows, s.focus], ['', 3, 'egQ'], 'and nothing late (the typing pause) brings the search back: ' + JSON.stringify(s));

    // keyboard: Tab from the box reaches the ×, which shows a focus ring; Enter presses it
    await page.keyboard.type('4195215441'); await page.keyboard.press('Tab');
    const kb = await page.evaluate(() => { const a = document.activeElement, cs = getComputedStyle(a); return { id: a.id, ring: cs.outlineStyle !== 'none' && parseFloat(cs.outlineWidth) >= 1 }; });
    assert.deepEqual(kb, { id: 'egQX', ring: true }, 'Tab reaches the × and its focus shows: ' + JSON.stringify(kb));
    await page.keyboard.press('Enter');
    s = await state();
    assert.deepEqual([s.value, s.rows, s.focus, s.x], ['', 3, 'egQ', false], 'Enter on the × clears too: ' + JSON.stringify(s));

    // Esc in the box clears it; an Esc on an empty box is left to whatever else listens
    await page.keyboard.type('sc_never_matches'); await page.waitForFunction(ROW => !document.querySelector(ROW), ROW);
    await page.evaluate(() => { window.__esc = 0; document.addEventListener('keydown', e => { if (e.key === 'Escape') window.__esc++; }); });
    await page.keyboard.press('Escape');
    s = await state();
    assert.deepEqual([s.value, s.rows, s.focus, s.x], ['', 3, 'egQ', false], 'Esc in the box empties it and shows the whole list at once: ' + JSON.stringify(s));
    assert.equal(await page.evaluate(() => window.__esc), 0, 'the Esc that cleared is not passed on');
    await page.keyboard.press('Escape');
    assert.equal(await page.evaluate(() => window.__esc), 1, 'an Esc on the empty box is passed on as before');

    // the empty list's own Clear search does the same
    await page.keyboard.type('sc_never_matches'); await page.waitForSelector('#egQueue [data-eg-clear]');
    await page.click('#egQueue [data-eg-clear]');
    s = await state();
    assert.deepEqual([s.value, s.rows, s.x], ['', 3, false], 'the empty list\'s Clear search empties the box and hides the ×: ' + JSON.stringify(s));

    // a search kept through a rebuild of the tab keeps its ×
    await page.fill('#egQ', '4195215442'); await page.waitForFunction(ROW => document.querySelectorAll(ROW).length === 1, ROW);
    await page.evaluate(() => { CN.setMode('orders'); CN.setMode('engrave'); Engrave.render(); });
    s = await state();
    assert.deepEqual([s.value, s.x], ['4195215442', true], 'the search kept through a redraw keeps its ×: ' + JSON.stringify(s));

    // the Orders bar's × is the same as before (24 px, in its box)
    await page.evaluate(() => CN.setMode('orders'));
    await page.waitForSelector('#ordQ');
    await page.fill('#ordQ', '4195'); await page.waitForTimeout(300);
    const ord = await page.evaluate(() => { const x = document.getElementById('ordFindX'), cs = getComputedStyle(x); return { shown: !x.hidden, w: cs.width, pos: cs.position }; });
    assert.deepEqual(ord, { shown: true, w: '24px', pos: 'static' }, 'the Orders bar\'s × is unchanged: ' + JSON.stringify(ord));

    assert.deepEqual(errors, [], 'no page errors');
    console.log('  ✓ the Engrave search box: typing shows the ×; the ×, Enter on it, Esc and the empty list\'s Clear search empty it, show the whole list at once and keep the focus; the Orders × is unchanged');
  } finally { await browser.close(); srv.close(); }
})().then(() => console.log('Engrave search clear OK')).catch(e => { console.error(e); process.exit(1); });
