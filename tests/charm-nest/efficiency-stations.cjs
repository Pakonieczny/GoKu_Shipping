// The live stations board and the shared order card (charm-nest-efficiency-stations.js, window.EfficiencyStations).
// Fakes only: the harness answers the gated read function (op "live") from tests/charm-nest/efficiency-stations-fixture.cjs (invented people,
// invented orders, pictures drawn in the fixture, a fake passcode); every other request off the loopback is aborted, so nothing reaches the
// internet, Etsy or Firestore.
//   1 · the view model and the timer words (a missing field is empty, qr text falls back to the order number, 95 s is 01:35, 62 min is 1 h 2 m)
//   2 · a labelled spinner on first load, "locked" when there is no passcode, the request carries op live + key + mode and never a URL key
//   3 · every station as a row (state, people, counts, sparkline), one card per person, the order's QR (decoded: it IS the order number), the
//       order picture, ONE picture per piece of a 3-piece order (a broken one is a calm placeholder), no pieces row for a one-piece order
//   4 · the timer ticks once a second with no request; mm:ss, then h m
//   5 · hover: a resting pointer grows a picture or the QR in place (never full screen, kept in view, put back), the hover cards of a station
//       and of a person (only the facts the answer carries), a press opens the order (openOrderFrom) once
//   6 · live: a new order flies in (Motion's copy on the layer), a finished one says "Done in …" and lifts away, people arrive and leave,
//       the status line tells the truth (Reconnecting, then Live), data stays on screen through a failure, honest empty states
//   7 · hidden page, hidden tab: no request; shown again: reads at once
//   8 · reduced motion: nothing moves; 1440, 900 and 390 px have no sideways scroll; touch: a tap grows, a second tap opens; no console errors
//   node tests/charm-nest/efficiency-stations.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const F = require('./efficiency-stations-fixture.cjs');
const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const pwDir = process.env.PW_DIR || (fs.existsSync(path.join(root, 'node_modules/playwright-core')) ? path.join(root, 'node_modules') : '/opt/node22/lib/node_modules/playwright/node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  let fx = F.make(); const seen = { urls: [], aborted: 0 };
  const wire = async ctx => {
    await ctx.route(() => true, async route => {
      const u = new URL(route.request().url()); seen.urls.push(u.href);
      if (u.hostname !== '127.0.0.1' && u.hostname !== 'localhost') { seen.aborted++; return route.abort(); }
      if (u.pathname.endsWith('/employeeEfficiency')) {
        let b = {}; try { b = JSON.parse(route.request().postData() || '{}'); } catch (_) {}
        const r = fx.answer(b); if (fx.delay) await sleep(fx.delay);
        return route.fulfill({ status: r.status, contentType: 'application/json', headers: { 'Access-Control-Allow-Origin': '*' }, body: JSON.stringify(r.json) });
      }
      return route.continue();
    });
    await ctx.addInitScript(() => { window.confirm = () => true; window.alert = () => {}; });
  };
  const track = page => { const errs = [], logs = []; page.on('pageerror', e => errs.push(e.message)); page.on('console', m => { logs.push(m.text()); if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errs.push('console: ' + m.text()); }); return { errs, logs }; };
  const load = async (ctx, vw) => {
    const page = await ctx.newPage(), t = track(page);
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.EfficiencyStations && window.QRCode && window.Motion && window.OrderWin && document.readyState === 'complete', null, { timeout: 60000 });
    await page.addScriptTag({ path: path.join(root, 'lib/jsQR.js') });
    return Object.assign({ page }, t);
  };
  // the board in a view of its own in the sorter's stage (the Employee efficiency shell mounts it in its Stations tab; this test needs only the board)
  const mount = (page, o = {}) => page.evaluate(o => {
    document.querySelector('.app').classList.toggle('railOff', innerWidth < 700);
    for (const c of document.getElementById('stage').children) if (c.id !== 'esHost') c.classList.add('hidden');
    let d = document.getElementById('esHost'); if (!d) { d = document.createElement('div'); d.className = 'lib'; d.id = 'esHost'; document.getElementById('stage').appendChild(d); }
    if (window.__b) { window.__b.unmount(); window.__b = null; }
    d.classList.remove('hidden'); d.textContent = '';
    const E = EfficiencyStations; E.options.pollMs = o.pollMs || 600000; E.options.zoomDelay = 120; E.options.doneMs = 700; E.options.flyMs = 700;
    window.__mode = 'real'; window.__opened = []; window.openOrderFrom = (btn, rid) => { window.__opened.push([rid, !!btn.closest('dialog')]); return true; };
    window.__b = E.mount(d, { own: true, mode: () => window.__mode });
    return true;
  }, o);
  const V = '#esHost';
  const card = rid => `${V} .esCard[data-rid="${rid}"]`;
  const txt = async sel => (await (await fx_page().$(sel)).innerText()).replace(/\s+/g, ' ').trim();
  let cur = null; const fx_page = () => cur;
  const inBox = (r, b, tol = 1.5) => r.left >= b.left - tol && r.right <= b.right + tol && r.top >= b.top - tol && r.bottom <= b.bottom + tol;
  const rectOf = async (page, sel) => page.$eval(sel, e => { const r = e.getBoundingClientRect(); return { left: r.left, top: r.top, right: r.right, bottom: r.bottom, width: r.width, height: r.height }; });
  const calls = () => fx.state.calls.length;
  const forced = new Set();
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await wire(ctx);
    const P = await load(ctx), page = P.page; cur = page;

    /* ── 1 · the view model and the words ── */
    {
      const r = await page.evaluate(() => {
        const E = EfficiencyStations, j = o => JSON.parse(JSON.stringify(o));
        const M = j(E.norm({ ok: true, at: 5, stations: [
          { key: 'welding', label: 'Welding', people: ['Ana M.', { name: 'Bo', partsToday: 3, medianOrderMs: 90000 }], current: [{ person: 'Ana M.', rid: '3521000777', customer: 'Cy', scannedAt: 100, pieces: [{ id: 'x_1', label: 'A' }, { id: 'x_2' }] }, { person: 'Late', orderNumber: '12', qr: { text: 'T12' } }, { person: 'No id' }], counts: { partsToday: 4 }, spark: [1, 2, 3] },
          { key: 'ghost' }, { label: 'Only label', state: 'sleeping' }, null, {}],
          signedIn: [{ name: 'Bo', since: 7, lastSeenAt: 8 }, { name: '' }] }));
        const W = M.stations[0];
        return { n: M.stations.length, keys: M.stations.map(s => s.key), states: M.stations.map(s => s.state), cur: W.current.map(c => [c.rid, c.orderNumber, c.qr, c.pieces.length, c.person]), people: W.people.map(p => [p.name, p.since, p.parts, p.medianMs]), counts: W.counts, spark: W.spark, mode: M.mode,
          fmt: [E.fmt.since(95000), E.fmt.since(3720000), E.fmt.since(-5), E.fmt.since(0), E.fmt.since(3599000), E.fmt.words(252000), E.fmt.words(59000), E.fmt.words(3600000), E.fmt.words(4320000), E.fmt.initials('Paul K.'), E.fmt.initials('Ivy'), E.fmt.initials('Shelly_R')], empty: j(E.norm(null)).stations, sparkOnePoint: j(E.norm({ stations: [{ key: 'a', spark: [3] }] })).stations[0].spark };
      });
      assert.equal(r.n, 3, 'a station without a key or a label is dropped'); assert.deepEqual(r.keys, ['welding', 'ghost', 'only label']);
      assert.deepEqual(r.states, ['working', 'offline', 'offline'], 'a state is worked out when the server sent none or nonsense');
      assert.deepEqual(r.cur, [['3521000777', '3521000777', '3521000777', 2, 'Ana M.'], ['12', '12', 'T12', 0, 'Late']], 'the QR text is the order number unless the answer says otherwise; an order with no id is dropped');
      assert.deepEqual(r.people, [['Ana M.', null, null, null], ['Bo', 7, 3, 90000], ['Late', null, null, null]], 'people from the list and from the cards; sign-in time joined by name');
      assert.deepEqual(r.counts, { parts: 4, orders: null }, 'a count the answer lacks stays unknown (no zero invented)'); assert.deepEqual(r.spark, [1, 2, 3]); assert.equal(r.sparkOnePoint, null, 'one point is not a line');
      assert.deepEqual(r.fmt, ['01:35', '1 h 2 m', '00:00', '00:00', '59:59', '4 m 12 s', '59 s', '1 h', '1 h 12 m', 'PK', 'I', 'SR']); assert.deepEqual(r.empty, []);
      console.log('  ✓ view model: missing fields are empty not invented, states worked out, QR text, timer and duration words');
    }

    /* ── 2 · first load: a labelled spinner; locked without a passcode ── */
    fx.delay = 700;
    await mount(page);
    await page.waitForSelector(`${V} .esWait`);
    assert.equal(await txt(`${V} .esWait`), 'The console is locked. Enter the manager passcode to see the stations.', 'no passcode: said plainly, nothing is asked here');
    assert.equal(calls(), 0, 'no passcode: no request');
    assert.equal(await page.isVisible(`${V} .esList`), false);
    await page.evaluate(k => sessionStorage.setItem('cn.eff.key', k), F.KEY);
    await mount(page); await page.waitForSelector(`${V} .esWaitT`);
    assert.equal(await txt(`${V} .esWait`), 'Reading the stations…', 'a small labelled spinner while the first answer is on its way');
    assert(await page.isVisible(`${V} .esWait .esSpin`) && await page.isVisible(`${V} .esLive[data-s=load]`), 'the spinner and the status line wait');
    await page.waitForSelector(`${V} .esSt`); fx.delay = 0;
    await page.waitForFunction(() => !document.querySelector('#esHost .esWait') || document.querySelector('#esHost .esWait').hidden);
    assert.equal(await page.isVisible(`${V} .esWait`), false, 'the spinner goes when the answer lands');
    const first = fx.state.calls[fx.state.calls.length - 1];
    assert.deepEqual([first.op, first.key, first.sandbox], ['live', F.KEY, false], 'op live, the passcode in the body, the real stations');
    assert(seen.urls.every(u => !u.includes(F.KEY) && !/[?&]key=/.test(u)), 'the passcode is never in a URL');
    await page.evaluate(() => { window.__mode = 'sandbox'; }); await page.evaluate(() => __b.refresh());
    assert.equal(fx.state.calls[fx.state.calls.length - 1].sandbox, true, 'the Real | Sandbox choice goes with the request (sandbox:true)');
    await page.evaluate(() => { window.__mode = 'real'; });
    console.log('  ✓ locked says so with no request; first load shows a labelled spinner; op live carries key and mode, never in a URL');

    /* ── 3 · every station, every card ── */
    const stations = await page.$$eval(`${V} .esSt`, rs => rs.map(r => ({ key: r.dataset.key, state: r.dataset.state, name: r.querySelector('.esStName').textContent, people: [...r.querySelectorAll('.esPer')].map(p => p.textContent), cards: r.querySelectorAll('.esCard').length, idle: (r.querySelector('.esIdle') || {}).textContent || '' })));
    assert.deepEqual(stations.map(s => [s.key, s.state]), [['shipping', 'working'], ['assembly', 'working'], ['welding', 'working'], ['sorting', 'idle'], ['design', 'offline'], ['sorter', 'working'], ['inbox', 'idle']], 'every station, in the server\'s order, with its state');
    assert.deepEqual(stations.map(s => s.name), ['Shipping', 'Assembly', 'Welding', 'Sorting', 'Design', 'Sorter / Laser', 'Inbox']);
    assert.deepEqual(stations.map(s => s.cards), [1, 2, 1, 0, 0, 1, 0], 'one card per person at work; none on a station that is not processing');
    assert.deepEqual(stations[1].people, ['AMAnna M.', 'IRIvy R.'], 'the people on a station, with initials');
    assert(/^Idle · last event \d+:\d\d (AM|PM) \(1\d m ago\)$/.test(stations[3].idle), 'an idle station says so quietly with its last event time: ' + stations[3].idle);
    assert(/^Offline · nobody is signed in$/.test(stations[4].idle), stations[4].idle);
    assert.equal(await txt(`${V} .esSum`), '4 of 7 stations working · 7 people on');
    assert.equal(await page.isVisible(`${V} .esNone`), false, 'the empty line only when nothing is being processed');
    assert.equal(await page.$$eval(`${V} .esSt[data-state=working] .esLight`, ls => ls.every(l => getComputedStyle(l, '::after').animationName === 'esPulse')), true, 'a calm pulse on every light that is working');
    assert.equal(await page.$$eval(`${V} .esSt:not([data-state=working]) .esLight`, ls => ls.every(l => getComputedStyle(l, '::after').animationName === 'none')), true, 'and none on the others');
    assert.equal(await page.$$eval(`${V} .esSt[data-key=shipping] .esCnt b`, bs => bs.map(b => b.textContent).join(',')), '41,12', "today's counts");
    assert.equal(await page.$$eval(`${V} .esSt .esSpark`, s => s.length), 3, 'a sparkline for each station whose answer carries the last hour, none invented for the others');
    // the cards
    assert.equal(await page.$$eval(`${V} .esCard`, c => c.length), 5);
    const c1 = await page.$eval(card('3521000101'), c => ({ oid: c.querySelector('.esOid').textContent, cust: c.querySelector('.esCust').textContent, who: c.querySelector('.esWho').innerText.replace(/\s+/g, ' '), t: c.querySelector('.esT').textContent, pieces: c.querySelector('.esPieces') ? !c.querySelector('.esPieces').hidden : false }));
    assert.deepEqual([c1.oid, c1.cust], ['3521000101', 'Nora Fixture']); assert(/Michael V\./.test(c1.who) && !/Shipping/.test(c1.who), 'the person; the station is the row, not repeated on the card: ' + c1.who);
    assert.equal(c1.pieces, false, 'a one-piece order has no pieces row (its picture is the piece)');
    // the QR is real: decode it from the picture on the card
    const dec = await page.evaluate(rid => new Promise(res => {
      const im = document.querySelector(`#esHost .esCard[data-rid="${rid}"] .esQr img`); if (!im) return res(null);
      const go = () => { const c = document.createElement('canvas'); c.width = im.naturalWidth; c.height = im.naturalHeight; const g = c.getContext('2d'); g.drawImage(im, 0, 0); const d = g.getImageData(0, 0, c.width, c.height); const r = jsQR(d.data, c.width, c.height); res(r && r.data); };
      im.complete && im.naturalWidth ? go() : im.addEventListener('load', go);
    }), '3521000303');
    assert.equal(dec, '3521000303', 'the QR on the card decodes to the order\'s own text');
    assert(await page.$eval(`${card('3521000101')} .esQr img`, i => /^data:image\/png/.test(i.src)), 'drawn locally by the app\'s own generator (no network)');
    // pictures: the order's picture, one per piece, a broken one calm
    await page.waitForFunction(() => document.querySelectorAll('#esHost .esCard[data-rid="3521000303"] .esPcTh').length === 3 && [...document.querySelectorAll('#esHost .esCard[data-rid="3521000303"] .esPcTh')].every(b => b.dataset.state === 'ready' || b.dataset.state === 'none'));
    const pcs = await page.$$eval(`${card('3521000303')} .esPcTh`, bs => bs.map(b => ({ n: b.dataset.n, state: b.dataset.state, img: !!b.querySelector('img'), ph: !!b.querySelector('[data-ph]'), title: b.title })));
    assert.deepEqual(pcs.map(p => [p.n, p.state]), [['1', 'ready'], ['2', 'none'], ['3', 'ready']], 'ONE picture per piece of the 3-piece order; the broken one is a placeholder');
    assert.equal(pcs[1].ph, true); assert.equal(pcs[0].img && pcs[2].img, true);
    assert.equal(await page.$eval(`${card('3521000303')} .esPcL`, e => e.textContent), '3 pieces', '"pieces", never "lines"');
    assert.equal(await page.$$eval(`${card('3521000202')} .esPcTh`, b => b.length), 2, 'two pieces, two pictures');
    assert.equal(await page.$eval(`${card('3521000404')} .esTh`, b => [b.dataset.state, !!b.querySelector('[data-ph]')].join()), 'none,true', 'no picture for the order: a calm placeholder, not a broken image');
    assert.equal(await page.$eval(`${card('3521000303')} .esTh img`, i => i.naturalWidth > 0), true);
    assert.equal(await txt(`${card('3521000303')} .esNote`), 'Waiting on the second piece');
    assert(!/\blines?\b/i.test(await page.$eval(V, e => e.innerText)), 'the word "line" is never on the board');
    console.log('  ✓ every station as a row (state, people, counts, sparkline); one card per person; QR decodes to the order; order picture, one per piece, placeholders');

    /* ── 4 · the timer ticks by itself ── */
    const secs = s => { const m = /^(\d+):(\d\d)$/.exec(s); return m ? +m[1] * 60 + +m[2] : null; };
    const read = async rid => page.$eval(`${card(rid)} .esT`, e => e.textContent);
    const sc101 = fx.find('shipping').current[0].scannedAt, t1 = secs(await read('3521000101')), want = (Date.now() - sc101) / 1000, n1 = calls();
    await sleep(2300);
    const t2 = secs(await read('3521000101'));
    assert(Math.abs(t1 - want) <= 1.6, `since the scan (${want.toFixed(1)} s): ` + t1); assert(t2 - t1 >= 2 && t2 - t1 <= 3, `ticks once a second: ${t1} -> ${t2}`);
    assert.equal(calls(), n1, 'no request per tick');
    assert(/^1 h \d m$/.test(await read('3521000404')), 'an hour and more reads "1 h 2 m": ' + await read('3521000404'));
    assert(await page.$eval(`${card('3521000101')} .esT`, e => e.getAttribute('role') === 'timer' && /^Scanned at /.test(e.title)), 'a timer for readers, with the scan time on hover');
    console.log('  ✓ the timer ticks once a second with no request; mm:ss then h m');

    /* ── 5 · hover: zoom in place, hover cards, a press opens the order ── */
    const sel = `${card('3521000101')} .esTh`;
    const r0 = await rectOf(page, sel);
    await page.hover(sel); await sleep(40);
    assert.equal(await page.evaluate(() => !!EfficiencyStations.zoomed()), false, 'a pointer passing over does not zoom at once (a resting pointer does)');
    await sleep(520);
    const r1 = await rectOf(page, sel);
    assert(r1.width > r0.width * 1.9 && r1.width < 260, `the picture grows where it stands: ${r0.width} -> ${r1.width}`);
    assert(Math.abs((r1.left + r1.right) / 2 - (r0.left + r0.right) / 2) < 40, 'about its own place, not full screen');
    assert.equal(await page.$$eval('dialog[open]', d => d.length), 0, 'no dialog'); assert.equal(await page.$$eval('body > *', ns => ns.filter(n => getComputedStyle(n).position === 'fixed' && n.getBoundingClientRect().width > innerWidth * .8 && n.id !== 'motionLayer').length), 0, 'no full-screen layer');
    const stage = await rectOf(page, '#stage');
    assert(inBox(r1, { left: Math.max(0, stage.left), top: Math.max(0, stage.top), right: stage.right, bottom: stage.bottom }), 'in view, inside the stage');
    assert.equal(await page.$eval(sel, e => getComputedStyle(e, '::after').opacity), '1', 'lifted on a soft shadow');
    await page.mouse.move(5, 5); await sleep(450);
    const r2 = await rectOf(page, sel);
    assert(Math.abs(r2.width - r0.width) < 1, `and put back: ${r2.width}`); assert.equal(await page.evaluate(() => EfficiencyStations.zoomed()), null);
    // the QR grows the same way
    const qsel = `${card('3521000101')} .esQr`, q0 = await rectOf(page, qsel);
    await page.hover(qsel); await sleep(700);
    const q1 = await rectOf(page, qsel); assert(q1.width > q0.width * 2 && q1.width < 260, `the QR grows: ${q0.width} -> ${q1.width}`);
    await page.keyboard.press('Escape'); await sleep(450); assert(Math.abs((await rectOf(page, qsel)).width - q0.width) < 1, 'Esc puts it back');
    // a piece picture, and a picture at the very top of the scrolling stage is nudged into view
    const psel = `${card('3521000303')} .esPcTh >> nth=0`, p0 = await rectOf(page, psel);
    await page.hover(psel); await sleep(700); const p1 = await rectOf(page, psel); assert(p1.width > p0.width * 2.4, `a piece picture grows too: ${p0.width} -> ${p1.width}`);
    await page.mouse.move(5, 5); await sleep(400);
    await page.evaluate(() => { const s = document.getElementById('stage'), c = document.querySelector('#esHost .esCard[data-rid="3521000101"]'); s.scrollTop += c.getBoundingClientRect().top - s.getBoundingClientRect().top - 2; });
    await page.hover(sel); await sleep(700);
    const stage2 = await rectOf(page, '#stage'), r3 = await rectOf(page, sel);
    assert(r3.top >= stage2.top - 1 && r3.width > r0.width * 1.9, `nudged down inside the scrolling stage: top ${r3.top} vs ${stage2.top}`);
    await page.mouse.move(5, 5); await sleep(400); await page.evaluate(() => { document.getElementById('stage').scrollTop = 0; });
    // hover cards: a station, a person (only the facts the answer carries)
    await page.hover(`${V} .esSt[data-key=assembly] .esStId`); await page.waitForSelector('.esTip[data-on]');
    let tip = await page.$eval('.esTip', e => e.innerText.replace(/\s+/g, ' '));
    assert(/Assembly/i.test(tip) && /Working · 2 orders/i.test(tip) && /Parts today 52/i.test(tip) && /Orders today 17/i.test(tip) && /Last event/i.test(tip) && /Anna M\., Ivy R\./i.test(tip), 'station card: ' + tip);
    assert(/Pieces scanned or completed here today/i.test(tip) && /Logged activity only/i.test(tip), 'every number carries its plain definition');
    assert.equal(await page.$eval('.esTip', e => e.querySelectorAll('svg.esSpark').length), 1, 'the last-hour sparkline');
    await page.hover(`${V} .esPer[data-name="Anna M."]`); await sleep(60);
    tip = await page.$eval('.esTip', e => e.innerText.replace(/\s+/g, ' '));
    assert(/Anna M\./i.test(tip) && /Signed in since \d+:\d\d (AM|PM) · 4 h/i.test(tip) && /Parts today 33/i.test(tip) && /Orders today 9/i.test(tip) && /Median per order 5 m 12 s/i.test(tip) && /Longest idle 15 m/i.test(tip) && /on order 3521000202/i.test(tip), 'person card: ' + tip);
    assert(/The middle time from scan to done/i.test(tip) && /not effort/i.test(tip), 'definitions and the fairness line');
    await page.hover(`${V} .esPer[data-name="Ivy R."]`); await sleep(60);
    tip = await page.$eval('.esTip', e => e.innerText.replace(/\s+/g, ' '));
    assert(/Signed in since/i.test(tip) && !/Median/i.test(tip) && !/Parts today/i.test(tip) && !/Longest idle/i.test(tip), 'a person without those facts shows none (nothing invented): ' + tip);
    const tr = await rectOf(page, '.esTip'), vp = page.viewportSize(); assert(tr.left >= 0 && tr.right <= vp.width && tr.top >= 0 && tr.bottom <= vp.height, 'the hover card stays on screen');
    await page.hover(`${card('3521000202')} .esWho .esAv`); await sleep(60);
    assert(/Anna M\./i.test(await page.$eval('.esTip', e => e.innerText)) && /on order 3521000202/i.test(await page.$eval('.esTip', e => e.innerText)), 'the person on a card has the same hover card');
    await page.mouse.move(5, 5); await sleep(200); assert.equal(await page.$eval('.esTip', e => e.hasAttribute('data-on')), false, 'and it goes when the pointer leaves');
    // a press opens the order, once, the way other lists do
    await page.evaluate(() => { window.__opened = []; });
    await page.click(`${card('3521000202')} .esCust`); await page.mouse.move(5, 5);
    await page.click(`${card('3521000202')} .esOid`); await page.mouse.move(5, 5);
    await page.click(`${card('3521000202')} .esTh`); await page.mouse.move(5, 5); await sleep(300);
    const opened = await page.evaluate(() => window.__opened);
    assert.deepEqual(opened, [['3521000202', false], ['3521000202', false], ['3521000202', false]], 'a press on the card, the number or the picture opens that order once each (openOrderFrom, from outside any dialog)');
    console.log('  ✓ hover: pictures and QR grow in place and go back, kept in view; station and person cards with only the facts there are; a press opens the order once');

    /* ── 6 · live: arrivals, departures, the truth ── */
    await page.evaluate(() => { EfficiencyStations.options.pollMs = 600000; });
    await page.evaluate(() => document.querySelector('#esHost .esSt[data-key=sorting]').scrollIntoView({ block: 'center' }));
    fx.start('sorting', { rid: '3521000505', person: 'Dana S.', customer: 'Zed Fixture', pieces: [{ id: '3521000505_1', label: 'Piece 1', thumbUrl: F.pic(51, 'vector') }, { id: '3521000505_2', label: 'Piece 2', thumbUrl: F.pic(52, 'vector') }] });
    await page.evaluate(() => { window.__b.refresh(); window.__ghost = 0; const L = document.getElementById('motionLayer'); const t = setInterval(() => { if (document.querySelector('#motionLayer .mGhost')) window.__ghost++; }, 30); setTimeout(() => clearInterval(t), 1500); });
    await page.waitForSelector(card('3521000505'), { state: 'attached' });
    await sleep(260); assert(await page.evaluate(() => !!document.querySelector('#motionLayer .mGhost')), 'a new order flies in: Motion\'s copy is on the layer while it travels');
    await sleep(1100);
    assert(await page.evaluate(() => window.__ghost) > 3 && await page.evaluate(() => !document.querySelector('#motionLayer .mGhost')), 'it landed and the copy is gone');
    assert.equal(await page.$eval(`${V} .esSt[data-key=sorting]`, r => [r.dataset.state, r.querySelectorAll('.esCard').length, !!r.querySelector('.esIdle')].join()), 'working,1,false', 'the station is working now and its idle line has given way');
    assert.equal(await page.$eval(card('3521000505'), c => getComputedStyle(c).visibility), 'visible');
    assert.equal(await page.$$eval(`${card('3521000505')} .esPcTh`, b => b.length), 2);
    assert.equal(await txt(`${V} .esSum`), '5 of 7 stations working · 7 people on');
    // a finished order says how long it took and lifts away
    const sc303 = fx.find('welding').current[0].scannedAt; fx.finish('welding', 'Giovanna C.');
    await page.evaluate(() => __b.refresh()); await page.waitForSelector(`${card('3521000303')}.done`);
    const done = await txt(`${card('3521000303')} .esDn`), dm = /^Done in (?:(\d+) m)?(?: ?(\d+) s)?$/.exec(done), wantMs = Date.now() - sc303; assert(dm && Math.abs((+dm[1] || 0) * 60 + (+dm[2] || 0) - wantMs / 1000) <= 4, `a quiet note, the time from the scan (${(wantMs / 1000).toFixed(0)} s): ` + done);
    assert.equal(await page.$eval(`${card('3521000303')} .esT`, e => e.hidden), true, 'the timer stops'); assert.equal(await page.$(`${V} .esSt[data-key=welding] .esIdle`), null, 'the idle line waits until the card has gone');
    await page.waitForFunction(sel => !document.querySelector(sel), card('3521000303'), { timeout: 4000 });
    assert.equal(await page.$eval(`${V} .esSt[data-key=welding]`, r => [r.dataset.state, r.querySelectorAll('.esCard').length].join()), 'idle,0', 'it lifted away and the station is idle');
    assert(/^Idle · last event/.test(await txt(`${V} .esSt[data-key=welding] .esIdle`)));
    assert.equal(await page.$$eval(`${V} .esCnt`, c => c.length > 0), true);
    // people arrive and leave
    fx.signOut('Rae T.'); await page.evaluate(() => __b.refresh());
    await page.waitForFunction(() => !document.querySelector('#esHost .esSt[data-key=inbox] .esPer'), null, { timeout: 3000 });
    assert.equal(await page.$eval(`${V} .esSt[data-key=inbox]`, r => r.dataset.state), 'offline', 'the last person left: offline');
    assert.equal(await txt(`${V} .esSum`), '4 of 7 stations working · 6 people on', 'the summary follows (welding idle again)');
    // the status line tells the truth
    assert(/^Live · updated (just now|\d+s ago)$/.test(await txt(`${V} .esLiveT`)), 'Live · updated Ns ago: ' + await txt(`${V} .esLiveT`));
    await page.evaluate(() => { EfficiencyStations.options.pollMs = 200; EfficiencyStations.options.maxBackoffMs = 400; });
    fx.state.fail = 3; await page.evaluate(() => __b.refresh());
    await page.waitForFunction(() => /Reconnecting/.test(document.querySelector('#esHost .esLiveT').textContent), null, { timeout: 3000 });
    assert.equal(await page.$$eval(`${V} .esCard`, c => c.length), 5, 'what was on screen stays on screen through a failure');
    assert(/^Reconnecting · last update/.test(await txt(`${V} .esLiveT`)));
    await page.waitForFunction(() => /^Live/.test(document.querySelector('#esHost .esLiveT').textContent), null, { timeout: 6000 });
    await page.evaluate(() => { EfficiencyStations.options.pollMs = 600000; });
    await sleep(500);
    // honest empty states
    fx.clear(); await page.evaluate(() => __b.refresh()); await page.waitForFunction(() => !document.querySelector('#esHost .esCard'), null, { timeout: 5000 });
    assert.equal(await txt(`${V} .esNone`), 'No station is processing an order right now'); assert.equal(await page.isVisible(`${V} .esNone`), true);
    assert.equal(await page.$$eval(`${V} .esSt`, r => r.length), 7, 'the stations stay listed, each saying so quietly');
    assert.equal(await txt(`${V} .esSum`), '0 of 7 stations working · 6 people on');
    console.log('  ✓ live: a new order flies in, a finished one says "Done in …" and lifts away, people leave, Live / Reconnecting tell the truth, honest empty state');

    /* ── 7 · hidden: no request; shown again: reads at once ── */
    await page.evaluate(() => { EfficiencyStations.options.pollMs = 200; });
    await page.evaluate(() => __b.refresh()); await sleep(900);
    const live1 = calls(); await sleep(700); assert(calls() - live1 >= 2, 'polling while shown: ' + (calls() - live1));
    await page.evaluate(() => { Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => window.__vis || 'visible' }); window.__vis = 'hidden'; document.dispatchEvent(new Event('visibilitychange')); });
    await sleep(500); const h1 = calls(); await sleep(1300);
    assert.equal(calls(), h1, 'a hidden page asks for nothing');
    await page.evaluate(() => { window.__vis = 'visible'; document.dispatchEvent(new Event('visibilitychange')); });
    await sleep(500); assert(calls() > h1, 'shown again: it reads at once'); await sleep(600); assert(calls() - h1 >= 3, 'and keeps going');
    await page.evaluate(() => document.getElementById('esHost').classList.add('hidden')); await sleep(500);
    const t0 = calls(); await sleep(1300); assert.equal(calls(), t0, 'a tab that is left asks for nothing');
    await page.evaluate(() => document.getElementById('esHost').classList.remove('hidden')); await sleep(700); assert(calls() > t0, 'and when it is shown again it reads');
    await page.evaluate(() => { EfficiencyStations.options.pollMs = 600000; });
    await page.evaluate(() => __b.unmount()); await sleep(300); const u0 = calls(); await sleep(900); assert.equal(calls(), u0, 'unmounted: nothing is read'); assert.equal(await page.$$eval(V + ' .es', e => e.length), 0, 'and it is gone from the page');
    console.log('  ✓ no request while the page or the tab is hidden; reads at once when shown; unmount stops everything');

    /* ── 8 · fit, reduced motion, touch, errors ── */
    fx = F.make();
    for (const w of [1440, 900, 390]) {
      await page.setViewportSize({ width: w, height: 900 }); await mount(page); await page.waitForSelector(`${V} .esSt`, { timeout: 8000 }).catch(async e => { console.log('DEBUG', w, await page.evaluate(() => [document.getElementById('esHost').className, document.getElementById('esHost').innerText.slice(0, 200), document.visibilityState, EfficiencyStations.feed.calls, EfficiencyStations.feed.busy, EfficiencyStations.feed.fails])); throw e; }); await sleep(500);
      const m = await page.evaluate(() => { const s = document.getElementById('stage'), h = document.getElementById('esHost'); return [document.documentElement.scrollWidth - innerWidth, s.scrollWidth - s.clientWidth, h.scrollWidth - h.clientWidth, [...document.querySelectorAll('#esHost *')].filter(e => { const r = e.getBoundingClientRect(); return r.width && r.right > h.getBoundingClientRect().right + 1; }).length]; });
      assert.deepEqual(m, [0, 0, 0, 0], `${w} px: nothing sticks out sideways ${m}`);
    }
    console.log('  ✓ 1440, 900 and 390 px: no sideways scroll');
    assert.deepEqual(P.errs, [], 'no page errors, no console errors: ' + P.errs.join(' | '));
    assert(P.logs.every(l => !/Stations board/.test(l)), 'no warnings from the board');
    assert(P.logs.every(l => !l.includes(F.KEY)), 'the passcode is never in the console');
    assert.equal(seen.aborted, 0, 'nothing went to the internet'); assert(fx.state.calls.every(c => c.op === 'live'), 'only op live is ever asked');
    await ctx.close();

    // reduced motion: everything is still
    const rctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' }); await wire(rctx);
    const R = await load(rctx); const rp = R.page; cur = rp;
    await rp.evaluate(k => sessionStorage.setItem('cn.eff.key', k), F.KEY); fx = F.make(); fx.clear();
    await mount(rp); await rp.waitForSelector(`${V} .esSt`);
    fx.start('sorting', { rid: '3521000606', person: 'Dana S.', pieces: [{ id: 'a_1' }, { id: 'a_2' }] });
    await rp.evaluate(() => { window.__g = 0; const t = setInterval(() => { if (document.querySelector('#motionLayer .mGhost')) window.__g++; }, 20); setTimeout(() => clearInterval(t), 1200); __b.refresh(); });
    await rp.waitForSelector(card('3521000606')); await sleep(1300);
    assert.equal(await rp.evaluate(() => window.__g), 0, 'reduced motion: nothing flies');
    assert.equal(await rp.evaluate(() => document.getAnimations().filter(a => a.effect && a.effect.target && a.effect.target.closest && a.effect.target.closest('#esHost')).length), 0, 'reduced motion: not one animation on the board');
    const rs = `${card('3521000606')} .esTh`, w0 = (await rectOf(rp, rs)).width;
    await rp.hover(rs); await sleep(600); assert.equal(Math.round((await rectOf(rp, rs)).width), Math.round(w0), 'reduced motion: a picture does not grow (the ring is the sign)');
    assert.equal(await rp.evaluate(() => !!EfficiencyStations.zoomed()), true);
    fx.finish('sorting', 'Dana S.'); await rp.evaluate(() => __b.refresh()); await rp.waitForSelector(`${card('3521000606')}.done`); await rp.waitForFunction(sel => !document.querySelector(sel), card('3521000606'), { timeout: 3000 });
    assert.deepEqual(R.errs, [], 'no errors under reduced motion: ' + R.errs.join(' | '));
    await rctx.close();

    // touch: a tap grows a picture, a second tap opens the order
    fx = F.make();
    const tctx = await browser.newContext({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true, deviceScaleFactor: 2 }); await wire(tctx);
    const Tt = await load(tctx); const tp = Tt.page; cur = tp;
    await tp.evaluate(k => sessionStorage.setItem('cn.eff.key', k), F.KEY);
    await mount(tp); await tp.waitForSelector(`${V} .esSt`);
    const ts = `${card('3521000101')} .esTh`, tw0 = (await rectOf(tp, ts)).width;
    await tp.tap(ts); await sleep(500);
    assert.equal(await tp.evaluate(() => window.__opened.length), 0, 'touch: the first tap grows the picture, it does not open the order');
    assert((await rectOf(tp, ts)).width > tw0 * 1.8, 'touch: grown');
    const gr = await rectOf(tp, ts); await tp.touchscreen.tap((gr.left + gr.right) / 2, (gr.top + gr.bottom) / 2); await sleep(300);   // (a real finger, not a scroll into view first: a scroll puts the grown picture back)
    assert.deepEqual(await tp.evaluate(() => window.__opened.map(o => o[0])), ['3521000101'], 'touch: a tap on the grown picture opens the order');
    await tp.tap(`${V} .esSt[data-key=assembly] .esPer >> nth=0`); await sleep(150);
    assert(/Anna M\./.test(await tp.$eval('.esTip', e => e.innerText)), 'touch: a tap on a person shows the card');
    assert.deepEqual(Tt.errs, [], 'no errors on touch: ' + Tt.errs.join(' | '));
    await tctx.close();
    console.log('  ✓ reduced motion: nothing flies, nothing animates, no growing; touch: tap grows, second tap opens; no console errors');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
