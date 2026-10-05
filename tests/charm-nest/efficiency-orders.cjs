// The employee page's real-time order search list (charm-nest-efficiency-orders.js, EfficiencyOrders.mount). Fakes only: the harness answers
// the gated read function from tests/charm-nest/efficiency-orders-fixture.cjs (invented customers and orders, a fake passcode); every other
// request off the loopback is aborted, so nothing reaches the internet, Etsy or Firestore.
//   1 · the pure parts (working time text, status hint, the answer normalised, marked words)
//   2 · a list in the sorter page: first page, the row's facts, one tile per piece (+N), the QR (decoded back to the order), the in-place zoom,
//       the hover cards, a press opens the order (the host's onOpen, else the sorter's own)
//   3 · search as you type (debounced, marked, honest empty state), a stale request is cancelled, paging on scroll, filters and sort
//   4 · real time: a new order slides in at the top, a changed one updates in place, "N new" when scrolled, pause when hidden, errors with Retry
//   5 · 1440 / 900 / 390 px have no sideways scroll, reduced motion, nothing off the loopback, no console errors; unmount leaves nothing
//   6 · the contract: the list over E4's REAL personOrders handler (efficiency-orders-e4-backend.cjs: its invented shop, a fake passcode, a fake clock),
//       so the fields read, the server's order, the paging cursor, the search forms and the notes are the real ones, not only the fixture's
//   node tests/charm-nest/efficiency-orders.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<folder for screenshots>)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-orders-fixture.cjs');

/* ── 1 · the pure parts ── */
{
  const win = { document: { getElementById: () => null, addEventListener() {}, createElement: () => ({}) }, console };
  win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-efficiency-orders.js'), 'utf8'), win);
  const E = win.EfficiencyOrders, j = o => JSON.parse(JSON.stringify(o));
  assert.equal(E.fmtDur(252000), '4 m 12 s'); assert.equal(E.fmtDur(41000), '41 s'); assert.equal(E.fmtDur(3720000), '1 h 2 m'); assert.equal(E.fmtDur(240000), '4 m');
  assert.equal(E.fmtDur(500), '—', 'under a second is not printed as a time'); assert.equal(E.fmtDur(null), '—'); assert.equal(E.fmtDur(2 * 86400000 + 3 * 3600000), '2 d 3 h');
  const st = x => E.normOrder(Object.assign({ rid: '1', at: 5 }, x)).status.s;
  assert.equal(st({ completes: 1 }), 'completed'); assert.equal(st({}), 'handled'); assert.equal(st({ completes: 1, undone: 1 }), 'reopened'); assert.equal(st({ completes: 1, rejected: 1 }), 'issue');
  assert.equal(st({ completes: 1, issues: [{ kind: 'undone' }] }), 'reopened'); assert.equal(st({ completes: 1, issues: [{ kind: 'reprint' }, { kind: 'rescan' }] }), 'completed', 'a reprint or rescan is a signal, never a verdict');
  assert.equal(st({ completes: 1, undone: 1, issues: [{ kind: 'failed', label: 'Failed' }] }), 'issue', 'an issue outranks a reopening');
  for (const k of ['refused', 'cancelAlert', 'heldOrSkipped', 'lookupFailed']) assert.equal(st({ completes: 1, issues: [{ kind: k }] }), 'issue', k + ' is an issue (E4\'s kinds)');
  const M = j(E.norm({ ok: true, now: 9, total: 3, orders: [{ rid: 77 }, {}, null, { orderId: 'x1', number: '#55', pieces: [{ id: 'a', label: 'Heart', thumbUrl: 'javascript:alert(1)' }, { id: 'b', thumbUrl: 'https://i.example/x.jpg' }], piecesCount: 4, qr: { text: 'QR55' }, durationMs: 'bad' }] }));
  assert.equal(M.orders.length, 2, 'rows without an id are dropped'); assert.equal(M.orders[0].rid, '77'); assert.equal(M.orders[0].qr, '77', 'no qr: the receipt id, which is what the sticker carries');
  assert.equal(M.orders[0].piecesCount, 0); assert.equal(M.orders[0].durationMs, null); assert.deepEqual(M.orders[0].pieces, []); assert.equal(M.orders[0].next, undefined);
  const x = M.orders[1]; assert.equal(x.number, '55', 'the # is not doubled'); assert.equal(x.piecesCount, 4); assert.equal(x.pieces[0].thumbUrl, '', 'a javascript: address is not a picture'); assert.equal(x.pieces[1].thumbUrl, 'https://i.example/x.jpg'); assert.equal(x.durationMs, null); assert.equal(x.qr, 'QR55');
  assert.equal(M.next, ''); assert.equal(M.total, 3); assert.equal(j(E.norm(null)).orders.length, 0); assert.equal(j(E.norm({ next: 'o25', orders: [] })).next, 'o25');
  assert.equal(E.hl('Maya <b>Lindgren', ['maya', 'b']), '<mark class="efoMk">Maya</mark> &lt;b&gt;Lindgren', 'a word is marked, text is escaped, one letter is not marked');
  assert.equal(E.hl('Heart charm', ['ar', 'cha']), 'He<mark class="efoMk">ar</mark>t <mark class="efoMk">cha</mark>rm'); assert.equal(E.hl('abc', []), 'abc'); assert.deepEqual(j(E.words('  Oct  5 ')), ['oct', '5']);
  // the date text with what was typed marked: a weekday, a month, a day beside a month, a whole date, a whole month (the forms E4 reads)
  const M_ = t => `<mark class="efoMk">${t}</mark>`, day = { day: '2026-10-03' };   // (a Saturday)
  assert.equal(E.hlDate(day, 'Sat, Oct 3', []), 'Sat, Oct 3'); assert.equal(E.hlDate(day, 'Sat, Oct 3', ['oct', '3']), `Sat, ${M_('Oct')} ${M_('3')}`); assert.equal(E.hlDate(day, 'Sat, Oct 3', ['3']), 'Sat, Oct 3', 'a bare number says nothing about the date');
  assert.equal(E.hlDate(day, 'Sat, Oct 3', ['saturday']), `${M_('Sat')}, Oct 3`); assert.equal(E.hlDate(day, 'Sat, Oct 3', ['sat', 'oct']), `${M_('Sat')}, ${M_('Oct')} 3`); assert.equal(E.hlDate(day, 'Sat, Oct 3', ['monday']), 'Sat, Oct 3');
  for (const w of ['2026-10-03', '10/3', '2026-10', '10/3/2026']) assert.equal(E.hlDate(day, 'Sat, Oct 3', [w]), M_('Sat, Oct 3'), w + ' marks the whole date');
  assert.equal(E.hlDate(day, 'Sat, Oct 3', ['2026-11']), 'Sat, Oct 3'); assert.equal(E.hlDate(day, 'Sat, Oct 3, 2025', ['oct']), `Sat, ${M_('Oct')} 3, 2025`, 'a year is kept');
  console.log('  ✓ the pure parts: working time text, status hint, the answer normalised, marked words and dates');
}

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fx = F.make(), seen = { urls: [], bodies: [] };
  let real = null;   // section 6: { name, ask(body) → { status, text } } (E4's real handler) answers for that person
  // an order of 30 pieces whose receipt lists 12 (E4 lists at most 12 pieces and says the true count): 24 tiles, "+27", "and 6 more"
  { const o = fx.orders[10]; o.pieces = Array.from({ length: 12 }, (_, k) => ({ id: o.rid + '-' + (k + 1), label: 'Big set ' + (k + 1), sku: 'BIG-' + (k + 1), thumbUrl: F.pic(k, 'Big ' + (k + 1)) })); o.piecesCount = 30; o.parts = 30; }
  const shots = process.env.SHOTS; if (shots) fs.mkdirSync(shots, { recursive: true });
  const snap = async (page, name, clip) => { if (shots) await page.screenshot({ path: path.join(shots, name + '.png'), clip }); };
  const wire = async ctx => {
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url());
      seen.urls.push(u.href);
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') return route.abort();
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        if (b.op !== 'personOrders') return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: false, error: 'not this test' }) });
        if (real && b.name === real.name) { const r = await real.ask(b); return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: r.text }).catch(() => {}); }
        const r = fx.answer(b), ms = fx.delayFor(b);
        if (ms) await new Promise(x => setTimeout(x, ms));
        return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) }).catch(() => {});
      }
      return route.continue();
    });
    await ctx.addInitScript(k => {
      try { sessionStorage.setItem('cn.eff.key', k); localStorage.setItem('cn.employee', 'Tester'); } catch (_) {}
      window.__aborts = 0; const f = window.fetch; window.fetch = function (u, o) { if (String(u).includes('employeeEfficiency') && o && o.signal) o.signal.addEventListener('abort', () => { window.__aborts++; }); return f.apply(this, arguments); };
    }, F.KEY);
  };
  const track = page => { const errs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { if (m.type() === 'error' && !/503|401|Failed to load resource/.test(m.text())) errs.push('console: ' + m.text()); }); return errs; };
  const boot = async (page, w, h) => {
    await page.setViewportSize({ width: w, height: h || 900 });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.EfficiencyOrders && window.PieceMedia && window.Efficiency && window.Efficiency.api && document.readyState === 'complete', null, { timeout: 60000 });
    await page.addScriptTag({ url: `${srv.sorterOrigin}/lib/jsQR.js` });
  };
  /** A scroll box of its own (as a page of the console is), the list mounted in it. */
  const mount = (page, opts) => page.evaluate(o => {
    if (window.__h) window.__h.unmount();
    document.getElementById('efoTest')?.remove();
    const d = document.createElement('div'); d.id = 'efoTest'; d.style.cssText = 'position:fixed;inset:0;z-index:50;overflow:auto;background:#f3f0ea;padding:16px'; document.body.appendChild(d);
    const h = document.createElement('div'); h.id = 'efoHost'; h.style.cssText = 'max-width:1400px;margin:0 auto'; d.appendChild(h);
    window.__opened = null; window.__live = 0;
    new MutationObserver(ms => { for (const m of ms) if (m.target.classList && m.target.classList.contains('efoFresh')) window.__live++; }).observe(h, { subtree: true, attributes: true, attributeFilter: ['class'] });
    window.__ranges = [];
    const opts = Object.assign({ name: 'Giovanna', onOpen: rid => { window.__opened = rid; }, onRange: r => { window.__ranges.push(r); }, pollMs: 600 }, o);
    window.__h = EfficiencyOrders.mount(h, opts);
  }, opts || {});
  const rows = page => page.locator('.efoRow');
  const rids = page => page.evaluate(() => [...document.querySelectorAll('.efoRow')].map(r => r.dataset.rid));
  const calls = () => fx.state.calls.filter(c => c.op === 'personOrders');
  const hidden = (page, on) => page.evaluate(v => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => v ? 'hidden' : 'visible' }); document.dispatchEvent(new Event('visibilitychange')); }, on);
  const wait = ms => new Promise(r => setTimeout(r, ms));
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await wire(ctx);
    const page = await ctx.newPage(), errs = track(page);
    await boot(page, 1440);
    const outside0 = seen.urls.length;

    /* ── 2 · first page, the row, the tiles, the QR ── */
    fx.setDelay(() => 1200);
    await mount(page, { pollMs: 0 });
    const wt = await (await page.waitForFunction(() => { const w = document.querySelector('.efoState .efoWait'); return w ? { t: w.innerText, spin: w.querySelectorAll('.spin').length } : false; }, null, { timeout: 5000 })).jsonValue();
    assert.match(wt.t, /Loading orders/, 'a small labelled spinner while the first page is read'); assert.equal(wt.spin, 1);
    await page.waitForSelector('.efoRow'); fx.setDelay(null);
    assert.equal(await rows(page).count(), 25, 'one page of 25');
    assert.equal(calls()[0].limit, 25); assert.equal(calls()[0].name, 'Giovanna'); assert.equal(calls()[0].cursor, ''); assert.equal(calls()[0].q, '');
    assert.equal(await page.evaluate(() => document.body.innerText.includes('fixture-pass')), false, 'the passcode is never on the page');
    assert.match(await page.locator('.efoCount').innerText(), /^60 orders$/);
    assert.equal(await page.locator('.efoSearch input').getAttribute('placeholder'), 'Search orders, customers, pieces, stations or dates');
    const r0 = rows(page).nth(0), o0 = fx.orders[0];
    assert.equal((await r0.locator('.efoNum').innerText()).trim(), '#' + o0.number); assert.equal((await r0.locator('.efoCust').innerText()).trim(), o0.customer);
    assert.match(await r0.locator('.efoWhen').innerText(), /^[A-Z][a-z]{2}, [A-Z][a-z]{2} \d{1,2}\s+(?:·\s*)?\d{1,2}:\d{2} [AP]M$/m, 'date and time in New York');
    assert.equal(await r0.locator('.efoWhen').evaluate(e => e.getAttribute('datetime')), new Date(o0.at).toISOString());
    const nyClock = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit' }).format(new Date(o0.at));
    assert((await r0.locator('.efoWhen').innerText()).includes(nyClock), 'the time is New York time: ' + nyClock);
    assert.equal((await r0.locator('.efoChip').innerText()).trim(), 'Sorting'); assert.equal((await r0.locator('.efoDur b').innerText()).trim(), '4 m 12 s'); assert.equal((await r0.locator('.efoStat').innerText()).trim(), 'Completed');
    assert.match(await r0.locator('.efoStat').getAttribute('title'), /Completed here/);
    // statuses: reopened (index 2), issue (index 6: rejected), handled-or-completed per the fixture
    const stat = i => rows(page).nth(i).locator('.efoStat').innerText();
    assert.equal((await stat(2)).trim(), 'Reopened'); assert.equal((await stat(6)).trim(), 'Issue');
    // one tile per piece
    const tiles = i => rows(page).nth(i).locator('.efoTh');
    assert.equal(await tiles(0).count(), 1, 'a one-piece order: one tile'); assert.equal(await tiles(2).count(), 3, 'three pieces: three tiles');
    assert.equal(fx.orders[3].pieces.length, 7); assert.equal(await tiles(3).count(), 7, 'seven pieces: seven tiles (one per piece)');
    assert.equal(await rows(page).nth(3).locator('.efoTh:not(.over)').evaluateAll(l => l.filter(e => e.offsetParent).length), 3, 'only three are shown');
    assert.equal((await rows(page).nth(3).locator('.efoMore').innerText()).trim(), '+4');
    assert.deepEqual(await tiles(3).evaluateAll(l => l.map(e => e.dataset.n)), ['1', '2', '3', '4', '5', '6', '7'], 'each tile says which piece it is');
    assert.equal(await tiles(2).nth(1).locator('img').count(), 1, 'a stored picture is drawn'); assert.match(await tiles(2).nth(1).locator('img').getAttribute('src'), /^data:image\/svg/);
    const bare = fx.orders.findIndex(o => !o.info), bi = bare;   // an order whose receipt is not stored: calm placeholder, never a broken picture
    assert(bi >= 0 && bi < 25); assert.equal(await tiles(bi).count(), 1); assert.equal(await tiles(bi).first().evaluate(e => e.classList.contains('ph') && !e.querySelector('img') && !!e.querySelector('svg')), true, 'no picture stored: a calm placeholder');
    assert.equal(await rows(page).nth(bi).locator('.efoCust').count(), 0, 'no customer stored: nothing is invented');
    // the QR: one per row, and it reads back as the order's own text
    assert.equal(await page.locator('.efoRow .efoQr[data-qr]').count(), 25);
    const decode = (pg, i) => rows(pg).nth(i).locator('.efoQr img').evaluate(async im => { await im.decode(); const cv = document.createElement('canvas'), N = 320; cv.width = cv.height = N; const cx = cv.getContext('2d'); cx.imageSmoothingEnabled = false; cx.drawImage(im, 0, 0, N, N); const d = cx.getImageData(0, 0, N, N); const q = window.jsQR(d.data, N, N); return q ? q.data : null; });
    for (const i of [0, 3, 11, 24]) assert.equal(await decode(page, i), fx.orders[i].rid, `row ${i}: the QR reads back as the order`);
    assert.equal(await page.locator('.efoRow .efoQr').first().getAttribute('data-zoom-dot'), '1.9');
    await snap(page, '1-list-1440', { x: 0, y: 0, width: 1440, height: 900 });

    /* ── hover: zoom in place (never full screen), the cards ── */
    const th = tiles(2).nth(1), before = await th.boundingBox();
    await th.hover(); await page.waitForSelector('.efoZ.sealZoomed', { timeout: 3000 }); await wait(450);
    const zb = await th.boundingBox(); assert(zb.width > before.width * 1.6 && zb.width < before.width * 2.1, `grown in place: ${before.width} → ${zb.width}`);
    assert(Math.abs((zb.x + zb.width / 2) - (before.x + before.width / 2)) < before.width, 'it grows where it stands');
    assert(zb.width < 140, 'never a full-screen view'); assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 0);
    assert.equal(await page.evaluate(() => document.querySelectorAll('.efoZ.sealZoomed').length), 1);
    await snap(page, '2-thumbnail-zoom', { x: 0, y: 150, width: 900, height: 260 });
    assert.equal(await page.locator('.efoTip.on').count(), 0, 'the row card does not cover a zoom');
    await page.mouse.move(700, 12); await page.waitForFunction(() => !document.querySelector('.efoZ.sealZoomed'), null, { timeout: 3000 });
    const qr = rows(page).nth(1).locator('.efoQr'), qb = await qr.boundingBox();
    await qr.hover(); await page.waitForSelector('.efoZ.efoQr.sealZoomed', { timeout: 3000 }); await wait(450);
    const qz = await qr.boundingBox(); assert(qz.width > qb.width * 1.6, `the QR grows in place: ${qb.width} → ${qz.width}`);
    await snap(page, '3-qr-zoom', { x: 900, y: 80, width: 540, height: 300 });
    await page.mouse.move(700, 12); await page.waitForFunction(() => !document.querySelector('.efoZ.sealZoomed'), null, { timeout: 3000 });
    // the hover card of a row: the timing facts
    await rows(page).nth(4).locator('.efoMain').hover({ position: { x: 150, y: 10 } }); await page.waitForSelector('.efoTip.on', { timeout: 3000 });
    const tip = await page.locator('.efoTip').innerText();
    for (const w of ['#' + fx.orders[4].number, 'Worked', 'Start to finish', 'Scans', 'Completed', 'Labels printed', 'Pieces']) assert(tip.includes(w), 'the card says ' + w);
    assert(tip.includes('10 m 18 s'), 'the working time in words'); assert(tip.includes('logged activity, not effort'), 'what the number is');
    await snap(page, '4-row-card', { x: 0, y: 150, width: 900, height: 600 });
    await page.keyboard.press('Escape'); await page.waitForFunction(() => !document.querySelector('.efoTip.on'), null, { timeout: 2000 });
    const hb = await rows(page).nth(4).locator('.efoMain').boundingBox(); await page.mouse.move(hb.x + 160, hb.y + 12); await wait(500);
    assert.equal(await page.locator('.efoTip.on').count(), 0, 'Esc puts the card away and it stays away until the pointer leaves the row');
    await page.mouse.move(700, 12); await page.waitForFunction(() => !document.querySelector('.efoTip.on'), null, { timeout: 3000 });
    // "+N": hover shows every piece; a press opens them in place
    const more = rows(page).nth(3).locator('.efoMore'); await more.hover(); await page.waitForSelector('.efoTip.on .efoTipP', { timeout: 3000 });
    assert.equal(await page.locator('.efoTip .efoTipP > div').count(), 7, 'all seven pieces on hover');
    assert.match(await page.locator('.efoTip').innerText(), /Moon charm/);
    await snap(page, '5-all-pieces', { x: 0, y: 150, width: 900, height: 600 });
    await page.mouse.move(700, 12); await page.waitForFunction(() => !document.querySelector('.efoTip.on'), null, { timeout: 3000 });
    await more.click(); assert.equal(await rows(page).nth(3).locator('.efoTh').evaluateAll(l => l.filter(e => e.offsetParent).length), 7, 'a press shows all seven in place');
    assert.equal(await page.evaluate(() => window.__opened), null, 'the +N press does not open the order'); assert.equal((await more.innerText()).trim(), 'Less');
    await more.click(); assert.equal(await rows(page).nth(3).locator('.efoTh').evaluateAll(l => l.filter(e => e.offsetParent).length), 3);
    // a very large order: tiles stop at 24, "+27" counts the true 30, the card lists 24 and says how many more; a piece the server did not list is a calm placeholder
    assert.equal(await tiles(10).count(), 24, '30 pieces: 24 tiles'); assert.equal((await rows(page).nth(10).locator('.efoMore').innerText()).trim(), '+27');
    assert.equal(await tiles(10).nth(12).evaluate(e => e.classList.contains('ph') && !e.querySelector('img')), true, 'the 13th piece was not listed: placeholder, nothing invented');
    await rows(page).nth(10).locator('.efoMore').hover(); await page.waitForSelector('.efoTip.on .efoTipP', { timeout: 3000 });
    assert.equal(await page.locator('.efoTip .efoTipP > div').count(), 24); assert.match(await page.locator('.efoTip .efoTipW').innerText(), /and 6 more/); assert.match(await page.locator('.efoTip').innerText(), /Piece 13/);
    await page.mouse.move(700, 12); await page.waitForFunction(() => !document.querySelector('.efoTip.on'), null, { timeout: 3000 });
    // a press opens the order: the host's onOpen, a tile or the QR included; else the sorter's own function
    await rows(page).nth(4).locator('.efoNum').click(); assert.equal(await page.evaluate(() => window.__opened), fx.orders[4].rid);
    await tiles(7).first().click(); assert.equal(await page.evaluate(() => window.__opened), fx.orders[7].rid, 'a press on a tile opens the order');
    await rows(page).nth(8).locator('.efoQr').click(); assert.equal(await page.evaluate(() => window.__opened), fx.orders[8].rid, 'a press on the QR opens the order');
    await page.evaluate(() => { window.__sorterOpen = []; window.openOrderFrom = (b, rid) => { window.__sorterOpen.push(rid); return true; }; });
    await mount(page, { onOpen: undefined, pollMs: 0 }); await page.waitForSelector('.efoRow');
    await rows(page).nth(2).locator('.efoOpen').focus(); await page.keyboard.press('Enter');
    assert.deepEqual(await page.evaluate(() => window.__sorterOpen), [fx.orders[2].rid], 'with no onOpen, the sorter opens the order (Enter on the row works too)');
    assert.equal(await page.evaluate(() => document.querySelectorAll('dialog[open]').length), 0, 'this list opens no pop-up of its own');
    console.log('  ✓ first page, the row, one tile per piece (+N), the QR read back, in-place zoom, hover cards, a press opens the order');

    /* ── 3 · search as you type ── */
    await mount(page, { pollMs: 0 }); await page.waitForSelector('.efoRow');
    const n0 = calls().length, input = page.locator('.efoSearch input');
    assert.equal(await page.locator('.efoNote:visible').count(), 0, 'no note when the server has none');
    fx.state.notes = ['Customer names, SKUs and piece titles were searched in the newest 250 orders with a stored receipt.'];
    await input.click();
    // four keystrokes in one task (a fast typist): the debounce must turn them into one request, for the last text
    await page.evaluate(() => { const i = document.querySelector('.efoSearch input'); for (const v of ['m', 'ma', 'may', 'maya']) { i.value = v; i.dispatchEvent(new Event('input', { bubbles: true })); } });
    for (let i = 0; i < 400 && !calls().slice(n0).some(c => c.q === 'maya'); i++) await wait(25);
    await page.waitForFunction(() => !window.__h.state().loading && document.querySelectorAll('.efoRow .efoMk').length > 0, null, { timeout: 8000 });
    await wait(300);
    const typed = calls().slice(n0); assert.equal(typed.length, 1, 'not one request per key (debounced): ' + JSON.stringify(typed.map(c => c.q))); assert.equal(typed[0].q, 'maya'); assert.equal(typed[0].cursor, '');
    const want = fx.orders.filter(o => fx.hit(o, 'maya')); assert(want.length > 0);
    assert.deepEqual(await rids(page), want.slice(0, 25).map(o => o.rid), 'only the orders that match');
    assert.equal(await page.locator('.efoRow .efoCust').evaluateAll(l => l.every(e => /maya/i.test(e.textContent))), true);
    assert.equal(await page.locator('.efoRow .efoCust mark.efoMk').first().innerText(), 'Maya', 'the word typed is marked in the text');
    assert.match(await page.locator('.efoCount').innerText(), new RegExp(`^${want.length} of 60 orders$`));
    assert.equal(await page.locator('.efoNote:visible').count(), 1); assert.match(await page.locator('.efoNote').innerText(), /newest 250 orders with a stored receipt/); assert.equal(await page.locator('.efoNote span').count(), 1, "the server's note is shown once, quietly");
    await snap(page, '6-search-maya', { x: 0, y: 0, width: 1440, height: 560 });
    // a piece, a station and a date are searched too
    await page.evaluate(() => window.__h.setQuery('paw')); await page.waitForFunction(() => window.__h.state().query === 'paw' && !window.__h.state().loading);
    const paw = fx.orders.filter(o => fx.hit(o, 'paw')); assert.deepEqual(await rids(page), paw.slice(0, 25).map(o => o.rid), 'a piece name');
    assert.equal(await page.locator('.efoRow .efoSub mark.efoMk').first().innerText().then(t => t.toLowerCase()), 'paw', 'the piece name is marked');
    const ymd = F.ymd(fx.orders[0].at), [yy, mm, dd] = ymd.split('-').map(Number), mon = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'][mm - 1];
    await page.evaluate(q => window.__h.setQuery(q), `${mon} ${dd}`); await page.waitForFunction(q => window.__h.state().query === q && !window.__h.state().loading, `${mon} ${dd}`);
    const dated = fx.orders.filter(o => fx.hit(o, `${mon} ${dd}`)); assert.deepEqual(await rids(page), dated.slice(0, 25).map(o => o.rid), 'a date');
    assert.equal(await page.locator('.efoRow .efoWhen .d mark.efoMk').count() > 0, true, 'the date is marked in the row');
    assert.equal(await page.locator('.efoRow .efoWhen .d').first().innerHTML(), `${new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', weekday: 'short' }).format(new Date(fx.orders[0].at)).replace(/^(\w{3}).*/, '$1')}, <mark class="efoMk">${mon[0].toUpperCase() + mon.slice(1)}</mark> <mark class="efoMk">${dd}</mark>`, 'month and day marked, the weekday not');
    fx.state.notes = [];
    // nothing matches: honest, with a way back
    await input.fill('zzqx'); await page.waitForSelector('.efoState b'); assert.equal(await page.locator('.efoNote:visible').count(), 0, 'the note goes when the server stops sending it');
    await page.waitForFunction(() => /No orders match/.test(document.querySelector('.efoState').innerText));
    assert.equal(await rows(page).count(), 0, 'no fake rows'); assert.match(await page.locator('.efoState').innerText(), /zzqx/); await snap(page, '7-no-match', { x: 0, y: 0, width: 1440, height: 420 });
    await page.locator('.efoState [data-act="clear"]').click(); await page.waitForFunction(() => document.querySelectorAll('.efoRow').length === 25);
    assert.equal(await input.inputValue(), '', 'cleared'); assert.equal(await page.evaluate(() => window.__h.state().query), '');
    // the clear button and Esc
    await input.fill('theo'); await page.waitForFunction(() => window.__h.state().query === 'theo' && !window.__h.state().loading); await page.locator('.efoClear').click();
    await page.waitForFunction(() => window.__h.state().query === '' && !window.__h.state().loading); assert.equal(await input.inputValue(), '');
    // a stale request is cancelled: the answer to the first word comes late and must never be drawn
    const a0 = await page.evaluate(() => window.__aborts);
    fx.setDelay(b => (b.q === 'a' ? 2000 : 60));
    const s0 = calls().length; await input.click(); await page.keyboard.type('a');
    for (let i = 0; i < 200 && !calls().slice(s0).some(c => c.q === 'a'); i++) await wait(25);      // 'a' is asked (debounced) and still waiting
    assert(calls().slice(s0).some(c => c.q === 'a'), "'a' was asked");
    await page.keyboard.type('b'); await page.waitForFunction(() => window.__h.state().query === 'ab' && !window.__h.state().loading, null, { timeout: 6000 });
    await wait(2100);
    assert.equal(await page.evaluate(() => window.__h.state().query), 'ab'); assert.deepEqual(await rids(page), fx.orders.filter(o => fx.hit(o, 'ab')).slice(0, 25).map(o => o.rid), 'the late answer to "a" was not drawn');
    assert((await page.evaluate(() => window.__aborts)) > a0, 'the stale request was aborted, not only ignored'); fx.setDelay(null);
    // station chips, dates and sort: what E5's page drives through the same calls
    await input.fill(''); await page.waitForFunction(() => window.__h.state().query === '' && !window.__h.state().loading);
    await page.locator('.efoFc[data-st="welding"]').click(); await page.waitForFunction(() => window.__h.state().station === 'welding' && !window.__h.state().loading);
    assert.equal(calls().at(-1).station, 'welding'); assert.deepEqual(await rids(page), fx.orders.filter(o => o.stations.includes('welding')).slice(0, 25).map(o => o.rid));
    assert.equal(await page.locator('.efoFc[data-st="welding"]').getAttribute('aria-pressed'), 'true');
    await page.evaluate(d => window.__h.setRange({ from: d, to: d }), ymd); await page.waitForFunction(() => window.__h.state().from && !window.__h.state().loading);
    assert.equal(calls().at(-1).from, ymd); assert.equal(calls().at(-1).to, ymd); assert.equal(await page.locator('.efoDates input[name="efofrom"]').inputValue(), ymd, 'the date fields show the range the host set');
    assert.deepEqual(await page.evaluate(() => window.__ranges), [], 'a range the host set is not echoed back to it');
    await page.evaluate(() => window.__h.setRange({ from: '2001-01-01', to: '2001-01-02' })); await page.waitForFunction(() => /No orders match/.test(document.querySelector('.efoState')?.innerText || ''), null, { timeout: 4000 });
    assert.match(await page.locator('.efoState').innerText(), /No order was handled on these days/); assert.equal(await rows(page).count(), 0);
    await page.locator('input[name="efoto"]').fill('2001-01-05'); await page.waitForFunction(() => window.__ranges.length === 1);
    assert.deepEqual(await page.evaluate(() => window.__ranges[0]), { from: '2001-01-01', to: '2001-01-05' }, 'the host hears the list\'s own date fields');
    await page.locator('.efoDx').click(); await page.waitForFunction(() => !window.__h.state().from && !window.__h.state().loading); assert.equal(calls().at(-1).from, undefined);
    assert.equal(await page.evaluate(() => window.__ranges.at(-1)), null, 'Clear dates tells the host');
    await page.locator('.efoFc[data-st=""]').click(); await page.waitForFunction(() => !window.__h.state().station && !window.__h.state().loading);
    await page.evaluate(() => window.__h.setRange({ from: '2001-01-01', to: '2001-01-02' })); await page.waitForSelector('.efoState [data-act="clear"]'); await page.evaluate(() => { window.__ranges = []; });
    await page.locator('.efoState [data-act="clear"]').click(); await page.waitForFunction(() => document.querySelectorAll('.efoRow').length === 25 && !window.__h.state().from);
    assert.deepEqual(await page.evaluate(() => window.__ranges), [null], '"Clear search and filters" tells the host the range is gone');
    await page.locator('.efoSeg [data-sort="slowest"]').click();
    const durs = await page.evaluate(() => window.__h.state().rids.map(r => r)), ms = fx.orders.slice(0, 25).map(o => o.durationMs);
    const shown = await page.locator('.efoRow .efoDur b').allInnerTexts(); assert.equal(shown[0], '1 h 2 m', 'the slowest first'); assert(/loaded so far/.test(await page.locator('.efoNote').innerText()), 'it says it sorted what is loaded');
    await page.locator('.efoSeg [data-sort="fastest"]').click(); assert.equal((await page.locator('.efoRow .efoDur b').first().innerText()).trim(), '2 s', 'the fastest first'); await page.locator('.efoSeg [data-sort="newest"]').click();
    assert.deepEqual(await rids(page), fx.orders.slice(0, 25).map(o => o.rid), 'newest is the server order again');
    await snap(page, '8-filters', { x: 0, y: 0, width: 1440, height: 200 });
    // paging on scroll: the next page by cursor, a labelled spinner, then every order and an end line
    await mount(page, { pollMs: 0 }); await page.waitForSelector('.efoRow'); fx.setDelay(b => (b.cursor ? 1500 : 0));
    const p0 = calls().length;
    await page.evaluate(() => { const d = document.getElementById('efoTest'); d.scrollTop = d.scrollHeight; });
    const ft = await (await page.waitForFunction(() => { const f = document.querySelector('.efoFoot'); return f && f.querySelector('.spin') ? f.innerText : false; }, null, { timeout: 6000 })).jsonValue();
    assert.match(ft, /Loading more orders/, 'a labelled spinner while the next page is read');
    await page.waitForFunction(() => document.querySelectorAll('.efoRow').length === 50, null, { timeout: 8000 });
    assert.equal(calls()[p0].cursor, 'o25', 'the cursor the server gave is sent back'); assert.equal(calls()[p0].limit, 25);
    await page.evaluate(() => { const d = document.getElementById('efoTest'); d.scrollTop = d.scrollHeight; });
    await page.waitForFunction(() => document.querySelectorAll('.efoRow').length === 60, null, { timeout: 8000 }); fx.setDelay(null);
    await page.evaluate(() => { const d = document.getElementById('efoTest'); d.scrollTop = d.scrollHeight; }); await wait(500);
    assert.deepEqual(await rids(page), fx.orders.map(o => o.rid), 'every order once, in order'); assert.equal(calls().length, p0 + 2, 'no request after the end');
    assert.match(await page.locator('.efoFoot').innerText(), /All 60 orders shown/);
    console.log('  ✓ search as you type (debounced, marked, honest when nothing matches), stale request aborted, filters, sort, paging');

    /* ── 4 · real time ── */
    await mount(page, { pollMs: 500 }); await page.waitForSelector('.efoRow');
    const top0 = (await rids(page))[0], keep = await page.evaluate(() => { const e = document.querySelector('.efoRow'); e.__mark = 'same'; return e.dataset.rid; });
    assert.equal(await page.locator('.efoLive').getAttribute('data-s'), 'live'); assert.match(await page.locator('.efoLive').innerText(), /Live/);
    fx.add(1); const newRid = fx.orders[0].rid;
    await page.waitForFunction(r => document.querySelector('.efoRow')?.dataset.rid === r, newRid, { timeout: 5000 });
    assert.equal((await rids(page))[1], top0, 'the new order is at the top, the others follow'); assert.equal(await page.evaluate(() => window.__live) > 0, true, 'it arrived with the quiet slide and wash');
    assert.equal(await page.evaluate(k => [...document.querySelectorAll('.efoRow')].find(e => e.dataset.rid === k).__mark, keep), 'same', 'the rows already there were not rebuilt');
    assert.equal(await page.locator('.efoPill:visible').count(), 0, 'at the top: no pill, the order just arrives');
    // an order that changes (still being worked) is updated where it stands
    const el1 = fx.orders[2]; el1.durationMs = 777000; el1.steps[0].durationMs = 777000;
    await page.waitForFunction(() => document.querySelectorAll('.efoRow')[2].querySelector('.efoDur b').textContent === '12 m 57 s', null, { timeout: 5000 });
    assert.equal(await page.evaluate(() => document.querySelectorAll('.efoRow')[2].__mark), undefined); assert.equal((await rids(page)).length, 26);
    // an order worked again takes the top in the SERVER's order (the time of its last action), which is not the order of the first actions
    const t6 = fx.touch(6); await page.waitForFunction(r => document.querySelector('.efoRow')?.dataset.rid === r, t6.rid, { timeout: 5000 });
    assert.deepEqual(await rids(page), fx.orders.slice(0, 26).map(o => o.rid), "the server's order is kept; the list is not re-sorted by time"); assert(fx.orders[0].at < fx.orders[1].at, 'the touched order started earlier yet stays above');
    // scrolled away: new orders wait behind the "N new" pill, the list does not move under the reader
    await page.evaluate(() => { document.getElementById('efoTest').scrollTop = 900; }); await wait(250);
    const before2 = await rids(page), st0 = await page.evaluate(() => document.getElementById('efoTest').scrollTop);
    fx.add(2);
    await page.waitForSelector('.efoPill:visible', { timeout: 5000 }); assert.match(await page.locator('.efoPill').innerText(), /2 new/);
    assert.deepEqual(await rids(page), before2, 'nothing moved'); assert.equal(await page.evaluate(() => document.getElementById('efoTest').scrollTop), st0, 'the scroll place is kept');
    assert.equal(await page.evaluate(() => window.__h.state().buffered), 2);
    await snap(page, '9-new-pill', { x: 0, y: 0, width: 1440, height: 420 });
    await page.evaluate(() => document.querySelector('.efoPill').click()); await page.waitForFunction(() => document.querySelectorAll('.efoRow').length === 28, null, { timeout: 3000 });
    assert.deepEqual((await rids(page)).slice(0, 2), fx.orders.slice(0, 2).map(o => o.rid)); await page.waitForFunction(() => document.getElementById('efoTest').scrollTop < 40, null, { timeout: 4000 });
    assert.equal(await page.locator('.efoPill:visible').count(), 0);
    // hidden: no request at all; shown: it catches up at once
    await hidden(page, true); await wait(700); const h0 = calls().length; fx.add(1); await wait(1500);
    assert.equal(calls().length, h0, 'no request while the page is hidden'); assert.equal(await page.locator('.efoLive').getAttribute('data-s'), 'paused'); assert.match(await page.locator('.efoLive').innerText(), /Paused/);
    await hidden(page, false); await page.waitForFunction(r => document.querySelector('.efoRow')?.dataset.rid === r, fx.orders[0].rid, { timeout: 3000 });
    assert(calls().length > h0, 'caught up when shown'); assert.equal(await page.locator('.efoLive').getAttribute('data-s'), 'live');
    // a live check that fails keeps what is on screen and says so; it recovers
    fx.state.fail = 3; await page.waitForFunction(() => document.querySelector('.efoLive').dataset.s === 'slow', null, { timeout: 8000 });
    assert(await rows(page).count() >= 28, 'the rows stay'); assert.match(await page.locator('.efoLive').innerText(), /Reconnecting/);
    await page.waitForFunction(() => document.querySelector('.efoLive').dataset.s === 'live', null, { timeout: 15000 });
    // errors and empty states
    fx.state.fail = 1; await mount(page, { pollMs: 0 });
    await page.waitForSelector('.efoState [data-act="retry"]'); assert.match(await page.locator('.efoState').innerText(), /could not be read/); assert.equal(await rows(page).count(), 0, 'no fake rows on an error');
    await snap(page, '10-error', { x: 0, y: 0, width: 1440, height: 360 });
    await page.locator('.efoState [data-act="retry"]').click(); await page.waitForSelector('.efoRow'); assert.equal(await page.locator('.efoState:visible').count(), 0);
    // a passcode the server does not accept (the shell's error shape: status 401, auth) is asked once and then left alone
    await page.evaluate(() => {
      window.__h.unmount(); document.getElementById('efoTest')?.remove(); window.__n401 = 0;
      const d = document.createElement('div'); d.id = 'efoTest'; d.style.cssText = 'position:fixed;inset:0;z-index:50;overflow:auto;background:#f3f0ea;padding:16px'; document.body.appendChild(d);
      const h = document.createElement('div'); h.id = 'efoHost'; d.appendChild(h);
      window.__h = EfficiencyOrders.mount(h, { name: 'Giovanna', pollMs: 300, call: async () => { window.__n401++; throw Object.assign(new Error('That passcode was not accepted.'), { status: 401, auth: true }); } });
    });
    await page.waitForSelector('.efoState [data-act="retry"]', { timeout: 5000 }); assert.match(await page.locator('.efoState').innerText(), /passcode was not accepted/); await wait(1200);
    assert.equal(await page.evaluate(() => window.__n401), 1, 'a refused passcode is asked once, not again and again'); assert.equal(await page.locator('.efoLive').getAttribute('data-s'), 'paused'); assert.equal(await rows(page).count(), 0);
    await mount(page, { name: 'Nobody Yet', pollMs: 0 }); await page.waitForFunction(() => /not handled an order yet/.test(document.querySelector('.efoState')?.innerText || ''));
    assert.equal(await rows(page).count(), 0); assert.doesNotMatch(await page.locator('.efoState').innerText(), /No orders match/); await snap(page, '11-empty', { x: 0, y: 0, width: 1440, height: 360 });
    console.log('  ✓ real time: arrival at the top, change in place, "N new" when scrolled, pause when hidden, reconnect, errors with Retry, empty states');

    /* ── 5 · widths, unmount, console, reduced motion ── */
    for (const w of [900, 390]) {
      await page.setViewportSize({ width: w, height: 900 }); await mount(page, { pollMs: 0 }); await page.waitForSelector('.efoRow'); await wait(500);
      const fit = await page.evaluate(() => { const d = document.getElementById('efoTest'), h = document.getElementById('efoHost'); const lim = h.getBoundingClientRect().right + 1; return { sideways: d.scrollWidth > d.clientWidth + 1, over: [...document.querySelectorAll('.efoRow, .efoBar, .efoFilters, .efoSearch input')].filter(e => e.getBoundingClientRect().right > lim || e.getBoundingClientRect().left < h.getBoundingClientRect().left - 1).length, win: document.documentElement.scrollWidth > innerWidth + 1 }; });
      assert.deepEqual(fit, { sideways: false, over: 0, win: false }, `${w} px has no sideways scroll`);
      const lay = await page.evaluate(() => { const r = document.querySelectorAll('.efoRow')[2], q = s => r.querySelector(s).getBoundingClientRect(); return { thumbsBelowMain: q('.efoThumbsW').top >= q('.efoMain').bottom - 1, metaBelowMain: q('.efoMeta .efoWhen').top >= q('.efoMain').bottom - 1, qrRightOfMain: q('.efoQrW').left >= q('.efoMain').right - 1 }; });
      if (w === 390) assert.deepEqual(lay, { thumbsBelowMain: true, metaBelowMain: true, qrRightOfMain: true }, 'stacked on a phone'); else assert.equal(lay.metaBelowMain, true, 'two lines at 900');
      // the zoom of a tile at this width stays on screen
      const t2 = rows(page).nth(2).locator('.efoTh').nth(0); await t2.hover(); await page.waitForSelector('.efoZ.sealZoomed', { timeout: 3000 }); await wait(450);
      const zr = await t2.boundingBox(); assert(zr.x >= 0 && zr.x + zr.width <= w + 0.5 && zr.y >= 0, 'the zoom stays on screen at ' + w); await page.mouse.move(w - 4, 4); await page.waitForFunction(() => !document.querySelector('.efoZ.sealZoomed'), null, { timeout: 3000 });
      await snap(page, `12-fit-${w}`, { x: 0, y: 0, width: w, height: 900 });
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    const u0 = calls().length; await page.evaluate(() => { window.__h.unmount(); });
    assert.equal(await page.evaluate(() => document.getElementById('efoHost').children.length + document.querySelectorAll('.efoTip').length), 0, 'unmount empties the box and removes the card');
    await wait(1400); assert.equal(calls().length, u0, 'no request after unmount');
    await mount(page, { pollMs: 0 }); await page.waitForSelector('.efoRow'); await page.evaluate(() => { const h = document.getElementById('efoHost'); window.__h2 = EfficiencyOrders.mount(h, { name: 'Giovanna', pollMs: 0, onOpen() {} }); });
    await page.waitForSelector('.efoRow'); assert.equal(await page.locator('.efoSearch').count(), 1, 'mounting again into the same box replaces the old list');
    // a box taken out of the page without unmount() is let go after a while: its card and listeners do not stay
    await page.evaluate(() => { EfficiencyOrders.options.orphanMs = 400; window.__h3 = EfficiencyOrders.mount(document.getElementById('efoHost'), { name: 'Giovanna', pollMs: 300, onOpen() {} }); });
    await page.waitForSelector('.efoRow'); await page.locator('.efoRow').nth(1).locator('.efoMain').hover({ position: { x: 150, y: 10 } }); await page.waitForSelector('.efoTip.on', { timeout: 6000 });
    await page.evaluate(() => { document.getElementById('efoTest').remove(); }); await wait(1500);
    assert.equal(await page.evaluate(() => document.querySelectorAll('.efoTip').length), 0, 'a list removed from the page lets go of its card'); assert.equal(await page.evaluate(() => window.__h3.state().polling), false);
    await page.evaluate(() => { EfficiencyOrders.options.orphanMs = 20000; });
    const offLoop = seen.urls.slice(outside0).filter(u => { const h = new URL(u).hostname; return h !== '127.0.0.1' && h !== 'localhost'; });
    assert.deepEqual(offLoop, [], 'nothing left the loopback while the list ran: no Etsy, no internet');
    assert.deepEqual(errs, [], 'no page error, no console error');
    await ctx.close();

    const rm = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
    await wire(rm); const p2 = await rm.newPage(), errs2 = track(p2);
    await boot(p2, 1440); await mount(p2, { pollMs: 400 }); await p2.waitForSelector('.efoRow');
    fx.add(1); await p2.waitForFunction(r => document.querySelector('.efoRow')?.dataset.rid === r, fx.orders[0].rid, { timeout: 5000 });
    const motion = await p2.evaluate(() => { const r = document.querySelector('.efoRow'); const cs = getComputedStyle(r); const an = [...document.querySelectorAll('.efoRow, .efoPill, .efoMore, .efoFc')].filter(e => getComputedStyle(e).animationName !== 'none' || parseFloat(getComputedStyle(e).transitionDuration) > 0).length; return { an, cls: r.className, live: window.__live }; });
    assert.equal(motion.an, 0, 'no animation or transition under reduced motion'); assert.equal(motion.live, 0, 'the new row has no slide or wash'); assert(!/efoIn/.test(motion.cls));
    const tq = (await p2.locator('.efoRow').nth(2).locator('.efoTh').nth(0).boundingBox()); await p2.locator('.efoRow').nth(2).locator('.efoTh').nth(0).hover(); await wait(900);
    assert.equal((await p2.locator('.efoRow').nth(2).locator('.efoTh').nth(0).boundingBox()).width, tq.width, 'the platform leaves tiles at their size under reduced motion');
    assert.deepEqual(errs2, []); await rm.close();
    console.log('  ✓ 1440 / 900 / 390 px fit, unmount leaves nothing, nothing off the loopback, reduced motion, no console errors');

    /* ── 6 · the contract: the list over E4's REAL personOrders handler (its invented shop; the fake passcode; the pictures it stores are https
          addresses that this harness aborts, so every tile must fall back to the calm placeholder, and nothing may reach the internet) ── */
    {
      const { spawn } = require('child_process'), http = require('http');
      const WHO = 'Giovanna C.', ISSUE = new Set(['refused', 'rejected', 'cancelAlert', 'heldOrSkipped', 'failed', 'lookupFailed']);
      const wantStat = o => (o.rejected > 0 || o.errors > 0 || o.issues.some(i => ISSUE.has(i.kind)) ? 'Issue' : o.undone > 0 || o.issues.some(i => i.kind === 'undone') ? 'Reopened' : o.completes > 0 ? 'Completed' : 'Handled');
      const durText = ms => { if (ms < 1000) return '—'; const t = Math.round(ms / 1000); if (t < 60) return t + ' s'; const m = Math.floor(t / 60), r = t % 60; if (m < 60) return r ? `${m} m ${r} s` : m + ' m'; return `${Math.floor(m / 60)} h` + (m % 60 ? ` ${m % 60} m` : ''); };
      const be = spawn(process.execPath, [path.join(__dirname, 'efficiency-orders-e4-backend.cjs')], { env: Object.assign({}, process.env, { FAKE_PASS: F.KEY }), stdio: ['ignore', 'pipe', 'inherit'] });
      const rctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
      try {
        const port = await new Promise((res, rej) => { let b = ''; const t = setTimeout(() => rej(new Error('the E4 backend did not start')), 20000); be.stdout.on('data', c => { b += c; const m = /PORT (\d+)/.exec(b); if (m) { clearTimeout(t); res(+m[1]); } }); be.on('exit', c => rej(new Error('the E4 backend stopped: ' + c))); });
        const post = body => new Promise((res, rej) => { const rq = http.request({ host: '127.0.0.1', port, method: 'POST', headers: { 'content-type': 'application/json' } }, r => { let t = ''; r.on('data', c => { t += c; }); r.on('end', () => res({ status: r.statusCode, text: t })); }); rq.on('error', rej); rq.end(JSON.stringify(body)); });
        const log = [];
        real = { name: WHO, ask: b => { log.push(b); return post(b); } };
        const direct = async b => JSON.parse((await post(Object.assign({ key: F.KEY, op: 'personOrders', name: WHO, limit: 25 }, b))).text);
        await wire(rctx); const rp = await rctx.newPage(), errs3 = track(rp); await boot(rp, 1440); const off0 = seen.urls.length;
        const D0 = await direct({});
        assert(D0.ok && D0.found && D0.orders.length === 25 && D0.next === 'o25' && D0.total > 1000, 'the real handler answers a first page: ' + JSON.stringify([D0.ok, D0.found, D0.orders.length, D0.next, D0.total]));
        await mount(rp, { name: WHO, pollMs: 0 }); await rp.waitForSelector('.efoRow');
        assert.deepEqual(await rids(rp), D0.orders.map(o => o.rid), "the server's order, as sent"); assert.equal(log[0].name, WHO); assert.equal(log[0].q, ''); assert.equal(log[0].sort, undefined, 'no sort is asked while it is newest');
        assert.equal(+(await rp.locator('.efoCount').innerText()).replace(/\D/g, ''), D0.total, 'the total the server counted');
        const facts = await rp.evaluate(() => [...document.querySelectorAll('.efoRow')].map(r => ({ num: r.querySelector('.efoNum').textContent.trim(), cust: r.querySelector('.efoCust') ? r.querySelector('.efoCust').textContent.trim() : '', tiles: r.querySelectorAll('.efoTh').length, qr: r.querySelector('.efoQr').dataset.qr, dur: r.querySelector('.efoDur b').textContent.trim(), stat: r.querySelector('.efoStat').textContent.trim(), chip: r.querySelector('.efoChip').textContent.trim().toLowerCase(), when: r.querySelector('.efoWhen').getAttribute('datetime') })));
        D0.orders.forEach((o, i) => {
          const f = facts[i], at = `row ${i} (${o.rid})`;
          assert.equal(f.num, '#' + o.number, at); assert.equal(f.cust, o.customer, at + ' customer'); assert.equal(f.tiles, Math.max(1, Math.min(24, o.piecesCount)), at + ' one tile per piece'); assert.equal(f.qr, o.qr.text, at + ' qr');
          assert.equal(f.dur, durText(o.durationMs), at + ' working time'); assert.equal(f.stat, wantStat(o), at + ' status'); assert.equal(f.chip, o.station, at + ' station'); assert.equal(f.when, new Date(o.at).toISOString(), at + ' time');
        });
        assert(D0.orders.some(o => o.piecesCount > 1) && D0.orders.some(o => o.customer), 'the first page has multi-piece orders and customers');
        await wait(600); assert.equal(await rp.locator('.efoTh').evaluateAll(l => l.every(e => e.querySelector('img, svg'))), true, 'every tile shows a picture or the placeholder, never an empty square');
        assert.equal(await rp.locator('.efoTh img').count() === 0 || await rp.locator('.efoTh.ph').count() > 0, true);
        for (const i of [0, 7]) assert.equal(await decode(rp, i), D0.orders[i].rid, `real row ${i}: the QR reads back as the order`);
        // the search forms the server reads: a weekday, a month and day, a whole date, a month; each marked as typed, in the server's order
        const ask = async (q, extra) => { await rp.evaluate(v => window.__h.setQuery(v), q); await rp.waitForFunction(v => window.__h.state().query === v && !window.__h.state().loading, q); return direct(Object.assign({ q }, extra)); };
        const dates = () => rp.locator('.efoRow .efoWhen .d').evaluateAll(l => l.map(e => e.innerHTML));
        let D = await ask('friday'); assert(D.total > 0); assert.deepEqual(await rids(rp), D.orders.map(o => o.rid), "weekday: the server's rows");
        assert.equal((await dates()).every(h => /^<mark class="efoMk">Fri<\/mark>, \w{3} \d{1,2}$/.test(h)), true, 'the weekday is marked: ' + (await dates())[0]);
        D = await ask('oct 2'); assert.equal(D.orders[0].day, '2026-10-02'); assert.deepEqual(await rids(rp), D.orders.map(o => o.rid), 'month and day');
        assert.equal((await dates())[0], 'Fri, <mark class="efoMk">Oct</mark> <mark class="efoMk">2</mark>');
        D = await ask('10/2'); assert(D.total > 0); assert.deepEqual(await rids(rp), D.orders.map(o => o.rid), 'a whole date'); assert.equal((await dates()).every(h => h === '<mark class="efoMk">Fri, Oct 2</mark>'), true, 'the whole date is marked: ' + (await dates())[0]);
        D = await ask('oct'); assert(D.total > 0); assert.deepEqual(await rids(rp), D.orders.map(o => o.rid), 'a month'); assert.equal((await dates()).every(h => /<mark class="efoMk">Oct<\/mark>/.test(h)), true, 'the month is marked');
        // a customer: the server says how far that search reached, and the list shows its words quietly (or nothing, when it has none)
        const who = D0.orders.find(o => o.customer).customer, first = who.split(' ')[0].toLowerCase();
        D = await ask(first); assert(D.total > 0); assert.deepEqual(await rids(rp), D.orders.map(o => o.rid), 'a customer');
        assert.equal(await rp.locator('.efoRow .efoCust mark.efoMk').first().innerText().then(t => t.toLowerCase()), first, 'the name is marked');
        if (D.notes.length) { const nt = await rp.locator('.efoNote').innerText(); for (const n of D.notes) assert(nt.includes(n), "the server's note is shown: " + n); } else assert.equal(await rp.locator('.efoNote:visible').count(), 0);
        console.log('    (real shape: the customer search said ' + JSON.stringify({ searched: D.searched, notes: D.notes }) + ')');
        await rp.evaluate(() => window.__h.setQuery('')); await rp.waitForFunction(() => window.__h.state().query === '' && !window.__h.state().loading);
        // the cursor the server gives ('o25') is sent back; the pages follow each other with no repeat
        await mount(rp, { name: WHO, pollMs: 0 }); await rp.waitForSelector('.efoRow'); const lg0 = log.length;
        await rp.evaluate(() => { const d = document.getElementById('efoTest'); d.scrollTop = d.scrollHeight; });
        await rp.waitForFunction(() => document.querySelectorAll('.efoRow').length === 50, null, { timeout: 8000 });
        assert.equal(log.slice(lg0).find(b => b.cursor).cursor, 'o25'); const D1 = await direct({ cursor: 'o25' });
        assert.deepEqual(await rids(rp), D0.orders.concat(D1.orders).map(o => o.rid), 'two pages, in the server order, no repeat');
        // the live check over identical answers changes nothing on screen (no rebuild, no pill, still live)
        await mount(rp, { name: WHO, pollMs: 400 }); await rp.waitForSelector('.efoRow'); await rp.evaluate(() => { document.querySelector('.efoRow').__mark = 'same'; }); const lg1 = log.length; await wait(1800);
        assert(log.length - lg1 >= 2, 'it kept checking'); assert.deepEqual(await rids(rp), D0.orders.map(o => o.rid)); assert.equal(await rp.evaluate(() => document.querySelector('.efoRow').__mark), 'same', 'an unchanged answer rebuilds nothing');
        assert.equal(await rp.locator('.efoPill:visible').count(), 0); assert.equal(await rp.locator('.efoLive').getAttribute('data-s'), 'live');
        // what left the loopback: only the pictures the server stored (aborted here), nothing else
        const off = seen.urls.slice(off0).filter(u => { const h = new URL(u).hostname; return h !== '127.0.0.1' && h !== 'localhost'; });
        assert(off.length > 0 && off.every(u => u.startsWith('https://i.etsystatic.com/fake/')), 'only stored picture addresses were requested: ' + JSON.stringify([...new Set(off)].slice(0, 3)));
        assert.deepEqual(errs3, [], 'no page error, no console error');
      } finally { real = null; be.kill(); await rctx.close(); }
      console.log('  ✓ the contract over E4\'s real personOrders: fields, order, paging cursor, date / month / weekday / customer search, notes, steady live checks');
    }
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
