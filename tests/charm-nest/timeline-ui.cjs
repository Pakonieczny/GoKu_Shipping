// Browser test of the order timeline component (charm-nest-timeline-ui.js) inside the sorter page, over the local
// stand-in for the site (bridge-server.cjs). OrderTimeline.get is stubbed with one order of 31 events over six days and a
// cancelled one, each with the `where` the server's whereOf gives; one more order goes through the real client and the
// stand-in's timelineAdd/timelineGet. Checks the Now line and the milestone rail, the lanes and their stamps, hover (the loupe), click
// (inline detail: before → after, reason, Open sheet, Around this step), filters, a live event arriving, the 20 s
// refresh (shortened here), focus(), an error with Retry, compact mode, reduced motion and destroy() leaving no timers.
//   node tests/charm-nest/timeline-ui.cjs [playwright-core dir]      (SHOTS=<dir> saves the three screenshots there)
const fs = require('fs'), path = require('path'), assert = require('assert');
const root = path.join(__dirname, '../..');
const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
const { chromium } = require(path.join(pwDir, 'playwright-core'));
const { start } = require('./bridge-server.cjs');
const SHOTS = process.env.SHOTS || '';

const MAIN = '4176208841', CX = '4175011873';
/** The fixture, built in the page so its days are relative to today. */
function fixture({ MAIN, CX }) {
  const day0 = new Date(); day0.setHours(0, 0, 0, 0); day0.setDate(day0.getDate() - 5);
  const T = (d, hm) => { const [h, m] = hm.split(':').map(Number); const t = new Date(day0); t.setDate(t.getDate() + d); t.setHours(h, m, 0, 0); return +t; };
  let n = 0;
  const ev = (orderId, d, hm, type, by, extra) => Object.assign({ id: `${orderId}~${type}~e${++n}`, orderId, at: T(d, hm), type, by, source: 'sorter', station: '', device: '', text: '' }, extra || {});
  const M = (d, hm, type, by, extra) => ev(MAIN, d, hm, type, by, extra);
  const main = [
    M(0, '14:39', 'arrived', 'Etsy', { source: 'etsy', text: 'Order arrived from Etsy' }),
    M(0, '14:40', 'pulled', '', { source: 'system', text: 'Pulled into the day’s run' }),
    M(0, '14:40', 'interpreted', '', { source: 'system', text: 'Read: 14K Tiny Initial Tag, back engraving' }),
    M(0, '14:41', 'placed', '', { source: 'system', sheet: '14K Sheet 3', sheetId: 'sh-k3', text: 'Tiny Initial Tag placed on 14K Sheet 3', data: { poolId: `${MAIN}_555_1`, position: 'row 2 · 18 mm' } }),
    M(0, '15:02', 'engraveNeeded', '', { source: 'system', text: 'Back engraving: Love, Mom · 2026' }),
    M(0, '16:05', 'engraveApproved', 'Giovanna C.', { text: 'Back engraving approved', data: { words: 'Love, Mom · 2026', height: '1.1 mm' } }),
    M(0, '16:07', 'held', 'Giovanna C.', { text: 'Held — waiting on the buyer', data: { reason: 'The note could read H or K. Held until the buyer confirms.' } }),
    M(0, '16:08', 'teamMessage', 'Giovanna C.', { text: 'Asked the buyer: is the initial H?' }),
    M(1, '08:52', 'customerMessage', 'Hannah W.', { source: 'etsy', text: '“Yes, H please! Thank you.”' }),
    M(1, '09:10', 'released', 'Giovanna C.', { text: 'Released — initial H confirmed' }),
    M(1, '09:22', 'decided', 'Paul', { text: '14K Solid Gold approved', data: { metal: '14K Solid Gold', value: '$118.00' } }),
    M(1, '09:31', 'sizeChanged', 'Paul', { sheet: '14K Sheet 3', sheetId: 'sh-k3', text: '14K Sheet 3 resized', data: { before: { wIn: 4, hIn: 6 }, after: { wIn: 6, hIn: 8 } } }),
    M(1, '09:33', 'moved', '', { source: 'system', sheet: '14K Sheet 4', sheetId: 'sh-k4', lineKey: `${MAIN}_555`, text: 'Moved to 14K Sheet 4', data: { fromSheet: '14K Sheet 3', toSheet: '14K Sheet 4', poolId: `${MAIN}_555_1` } }),
    M(1, '11:02', 'merged', 'Paul', { sheet: '14K Sheet 4', sheetId: 'sh-k4', text: '14K Sheet 3 merged into Sheet 4', data: { charmsBefore: 31, charmsAfter: 38 } }),
    M(1, '13:15', 'roseLine', 'Paul', { sheet: 'RG Sheet 7', sheetId: 'sh-rg7', text: 'RG green line approved' }),
    M(1, '15:40', 'included', '', { source: 'system', setId: 'set-212', text: 'Included in Set 212' }),
    M(1, '15:42', 'qrLabel', 'Paul', { setId: 'set-212', sheet: '14K Sheet 4', sheetId: 'sh-k4', text: 'QR label made for Set 212' }),
    M(1, '15:44', 'setCommitted', 'Paul', { setId: 'set-212', text: 'Set 212 committed' }),
    M(2, '10:05', 'laserDone', 'Marco R.', { station: 'laser', sheet: '14K Sheet 4', sheetId: 'sh-k4', text: '14K Sheet 4 laser cut', data: { machine: 'Fiber 2', minutes: 11 } }),
    M(2, '10:32', 'roseCut', 'Marco R.', { station: 'laser', sheet: 'RG Sheet 7', sheetId: 'sh-rg7', text: 'RG Sheet 7 cut on the green line' }),
    M(2, '13:10', 'scan', 'Ana P.', { source: 'station', station: 'sorting', device: 'sorting-1', text: 'Scanned at Sorting' }),
    M(2, '13:12', 'sorted', 'Ana P.', { source: 'station', station: 'sorting', device: 'sorting-1', text: 'Both pieces sorted to the order' }),
    M(2, '13:14', 'sealPrinted', 'Ana P.', { station: 'sorting', text: 'QR label printed', data: { n: 1 } }),
    M(3, '09:40', 'scan', 'Marco R.', { source: 'station', station: 'welding', device: 'weld-1', text: 'Scanned at Welding' }),
    M(3, '10:15', 'welded', 'Marco R.', { source: 'station', station: 'welding', device: 'weld-1', text: 'Jump rings closed' }),
    M(3, '10:16', 'note', 'Marco R.', { source: 'station', station: 'welding', text: 'Ring on the Aster re-welded, looks good.' }),
    M(3, '14:05', 'scan', 'Luisa T.', { source: 'station', station: 'assembly', device: 'assembly-2', text: 'Scanned at Assembly' }),
    M(3, '14:48', 'assembled', 'Luisa T.', { source: 'station', station: 'assembly', device: 'assembly-2', text: 'Assembled on 18″ cable chain' }),
    M(5, '09:05', 'scan', 'Dana K.', { source: 'station', station: 'shipping', device: 'shipping-1', text: 'Scanned at Shipping' }),
    M(5, '09:20', 'packed', 'Dana K.', { source: 'station', station: 'shipping', device: 'shipping-1', text: 'Packed in a gift box' }),
    M(5, '09:22', 'labelPrinted', 'Dana K.', { source: 'station', station: 'shipping', device: 'shipping-1', text: 'USPS label printed', data: { tracking: '9400 1112 0206 5512 3345 67' } })
  ];
  const C = (d, hm, type, by, extra) => ev(CX, d, hm, type, by, extra);
  const cx = [
    C(0, '10:12', 'arrived', 'Etsy', { source: 'etsy', text: 'Order arrived from Etsy' }),
    C(0, '10:13', 'placed', '', { source: 'system', sheet: 'RG Sheet 9', sheetId: 'sh-rg9', text: 'Placed on RG Sheet 9' }),
    C(0, '15:30', 'engraveApproved', 'Giovanna C.', { text: 'Back engraving approved' }),
    C(1, '09:05', 'teamMessage', 'Giovanna C.', { text: 'Buyer asked for a gift note.' }),
    C(2, '11:40', 'etsyCancelled', 'Etsy', { source: 'etsy', text: 'Cancelled on Etsy', data: { reason: 'Buyer requested', refund: '$42.00' } }),
    C(2, '11:40', 'removed', '', { source: 'system', sheet: 'RG Sheet 9', sheetId: 'sh-rg9', text: 'Taken off RG Sheet 9', data: { reason: 'Cancelled on Etsy — the sheet was not cut yet, so its room was freed', freed: '71 mm²' } }),
    C(3, '13:02', 'cancelAlert', 'Ana P.', { source: 'station', station: 'sorting', device: 'sorting-1', text: 'Scanned at Sorting — alert shown, Understood pressed' })
  ];
  return { [MAIN]: { events: main, cancelled: null, where: null }, [CX]: { events: cx, cancelled: { at: cx[4].at, by: 'Etsy', why: 'Buyer requested', source: 'etsy' }, where: null } };
}

(async () => {
  const srv = await start({ receipts: [] });
  const { sorterOrigin } = srv;
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  // nothing leaves the machine: every request that is not to the local stand-in is aborted
  await ctx.route(() => true, r => { const h = new URL(r.request().url()).hostname; return h === '127.0.0.1' || h === 'localhost' ? r.continue() : r.abort(); });
  const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
  await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
  await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
  await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && /timeline|OrderTimeline|tlUI/i.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const shot = async name => { if (SHOTS) { await page.waitForTimeout(250); await page.screenshot({ path: path.join(SHOTS, name + '.png') }); } };
  const ok = [];

  await page.goto(`${sorterOrigin}/charm-nest-1.html`);
  await page.waitForFunction(() => window.OrderTimelineUI && window.OrderTimeline && document.readyState === 'complete', null, { timeout: 60000 });
  // the shared client (its TYPES, record and onRecord are real), with get() answering from the fixture; an order not in
  // the fixture goes to the real get() (the stand-in's timelineGet)
  await page.evaluate(({ fx, MAIN, CX }) => {
    window.__fx = (new Function('return (' + fx + ')'))()({ MAIN, CX });
    window.__gets = 0; window.__fail = 0;
    const nativeWait = window.setTimeout.bind(window);   // the stub's own wait is not one of the component's timers
    const realGet = OrderTimeline.get;
    OrderTimeline.get = async id => { if (!window.__fx[id]) return realGet(id); window.__gets++; await new Promise(r => nativeWait(r, 120)); if (window.__fail > 0) { window.__fail--; throw new Error('HTTP 503'); } return JSON.parse(JSON.stringify(window.__fx[id])); };
    // every timer the component sets, so destroy() can be checked for leftovers
    window.__tlTimers = new Set();
    const st = window.setTimeout, ct = window.clearTimeout, si = window.setInterval, ci = window.clearInterval;
    const mine = () => /charm-nest-timeline-ui\.js/.test(new Error().stack || '');
    window.setTimeout = function (fn, ms, ...a) { let id; const m = mine(); const f = typeof fn === 'function' ? function () { window.__tlTimers.delete(id); return fn.apply(this, arguments); } : fn; id = st.call(window, f, ms, ...a); if (m) window.__tlTimers.add(id); return id; };
    window.clearTimeout = function (id) { window.__tlTimers.delete(id); return ct.call(window, id); };
    window.setInterval = function (fn, ms, ...a) { const id = si.call(window, fn, ms, ...a); if (mine()) window.__tlTimers.add(id); return id; };
    window.clearInterval = function (id) { window.__tlTimers.delete(id); return ci.call(window, id); };
    window.__host = el => { const h = document.createElement('div'); h.className = 'tlTestHost'; h.style.cssText = 'position:fixed;inset:0;z-index:2147480000;background:var(--card);display:flex;flex-direction:column'; el.style.cssText = 'flex:1 1 auto;min-height:0;display:flex;flex-direction:column'; h.appendChild(el); document.body.appendChild(h); return h; };
  }, { fx: fixture.toString(), MAIN, CX });
  // each answer carries the server's `where`, worked out by the server's own whereOf
  const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));
  const where = {};
  for (const [id, o] of Object.entries(await page.evaluate(() => window.__fx))) where[id] = Timeline.whereOf(o.events, o.cancelled);
  await page.evaluate(w => { for (const id in w) window.__fx[id].where = w[id]; }, where);
  assert.equal(where[MAIN].step, 6); assert.equal(where[CX].step, 2);

  // ── mount on a detached container: the wait line says what it waits for, then the order paints ──
  await page.evaluate(MAIN => {
    window.__el = document.createElement('div');
    window.__tl = OrderTimelineUI.mount(window.__el, { orderId: MAIN, live: true, pollMs: 1500, onSheet: (s, p) => { window.__sheet = [s, p]; } });
    window.__waitText = window.__el.querySelector('.tlMsg').textContent;
  }, MAIN);
  assert.match(await page.evaluate(() => window.__waitText), /Loading the timeline of order 4176208841/);
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 31, null, { timeout: 5000 });
  ok.push('mounts on a detached container: a spinner line says what it loads, then 31 stamps');
  await page.evaluate(() => window.__host(window.__el));
  await page.waitForTimeout(900);

  // ── Now and the rail ──
  let r = await page.evaluate(() => { const q = s => [...window.__el.querySelectorAll(s)]; return { now: q('.tlNowT')[0].textContent, done: q('.tlStop.d').map(n => n.dataset.stage), cur: q('.tlStop.c').map(n => n.dataset.stage), fut: q('.tlStop.f').map(n => n.dataset.stage), skip: q('.tlStop.s').length, fill: q('.tlFill')[0].style.transform, ghosts: q('.tlSt.ghost').length, nowLine: (q('.tlNowLine')[0] || {}).textContent, lanes: q('.tlLane span').map(s => s.textContent), days: q('.tlDay:not(.idle)').length, idle: q('.tlDay.idle').length }; });
  assert.deepEqual(r.done, ['arrived', 'sheet', 'approved', 'laser', 'sorted', 'welded', 'assembled'], 'passed steps stamped: ' + r.done);
  assert.deepEqual(r.cur, ['shipped'], 'the next step pulses');
  assert.deepEqual(r.fut, ['completed'], 'the step to come is an outline');
  assert.equal(r.now, where[MAIN].label); assert.equal(r.now, 'Packed');
  assert.match(r.fill, /scaleX\(0\.875\)/);
  assert.equal(r.ghosts, 2, 'the two milestones to come are dashed stamps on the lanes');
  assert.equal(r.nowLine, 'NOW · AT SHIPPING');
  assert.equal(r.days, 5); assert.equal(r.idle, 1, 'the idle day collapses');
  assert.match(r.lanes[3], /Ana P\./); assert.match(r.lanes[6], /Dana K\./);
  r = await page.evaluate(() => ({ sub: window.__el.querySelector('.tlNowS').textContent, rail: [...window.__el.querySelectorAll('.tlStop span')].map(s => s.textContent) }));
  assert.deepEqual(r.rail, where[MAIN].rail, 'the rail is the server\'s 9 steps');
  assert.match(r.sub, /Shipping/); assert.match(r.sub, /Dana K\./); assert.match(r.sub, /next: Shipped/); assert.match(r.sub, new RegExp('on ' + where[MAIN].sheet));
  await page.click('.tlNowS .tlOpenSheet');
  assert.deepEqual(await page.evaluate(() => window.__sheet), [where[MAIN].sheetId, null], 'Open sheet on the where\'s sheet (the last one cut)');
  ok.push('Now reads the server\'s where ("Packed", Shipping · Dana K., next: Shipped, on RG Sheet 7 + Open sheet); the 9-step rail: 7 stamped, Shipped pulses, Completed an outline; 5 day columns + 1 idle; NOW · AT SHIPPING');

  // ── hover a stamp: it lifts onto the loupe at 136px with its full face ──
  const welded = await page.$('.tlSt[data-key="welded~e25"]');
  const small = await welded.boundingBox();
  await welded.hover();
  await page.waitForTimeout(320);
  r = await page.evaluate(() => { const L = window.__el.querySelector('.tlLoupe'), b = L.getBoundingClientRect(), s = window.__el.querySelector('.tlSt[data-key="welded~e25"]'); return { disp: getComputedStyle(L).display, w: b.width, texts: [...L.querySelectorAll('text')].map(t => t.textContent).join(' | '), cap: (L.querySelector('.cap') || {}).textContent, lifted: s.classList.contains('lifted'), shadow: getComputedStyle(L).boxShadow }; });
  assert.equal(r.disp, 'block'); assert(r.w > 130 && small.width < 40, `the stamp grows from ${small.width}px to ${r.w}px`);
  assert.match(r.texts, /WELDED/); assert.match(r.texts, /MARCO R\./); assert.match(r.texts, /WELDING STATION/); assert.match(r.texts, /\d{1,2}:\d\d [AP]M/);
  assert.equal(r.cap, 'Jump rings closed'); assert(r.lifted && /rgba/.test(r.shadow));
  ok.push(`hover: the stamp grows ${Math.round(small.width)}px → ${Math.round(r.w)}px on the loupe with shadow, reading WELDED, the time, MARCO R. and the station`);
  await page.mouse.move(700, 880); await page.waitForTimeout(260);
  assert.equal(await page.evaluate(() => getComputedStyle(window.__el.querySelector('.tlLoupe')).display), 'none', 'the loupe goes away');

  // ── click: the detail opens inline (no dialog), with before → after, the sheet and Open sheet ──
  await page.click('.tlSt[data-key="moved~e13"]');
  await page.waitForTimeout(350);
  r = await page.evaluate(() => { const d = window.__el.querySelector('.tlDetail'); return { h: d.querySelector('h3').textContent, kind: d.querySelector('.tlLbl').textContent, ba: [...d.querySelectorAll('.tlBA .m span')].map(s => s.textContent), badge: d.querySelector('.tlBadge').textContent, facts: [...d.querySelectorAll('.tlMeta .m')].map(m => m.textContent).join(' | '), around: [...d.querySelectorAll('.tlArw')].length, cur: d.querySelector('.tlArw.cur b').textContent, sel: window.__el.querySelectorAll('.tlSt.sel').length, lane: window.__el.querySelector('.tlLane.on').dataset.lane, dialogs: document.querySelectorAll('dialog[open]').length }; });
  assert.equal(r.h, 'Moved to 14K Sheet 4'); assert.match(r.kind, /Moved sheet · step 13 of 31/);
  assert.deepEqual(r.ba, ['14K Sheet 3', '14K Sheet 4']);
  assert.match(r.badge, /Sheet & laser/i); assert.match(r.badge, /Automatic/);
  assert.match(r.facts, /Sheet14K Sheet 4/); assert.match(r.facts, /Line/); assert.equal(r.around, 5); assert.equal(r.cur, 'Moved to 14K Sheet 4');
  assert.equal(r.sel, 1); assert.equal(r.lane, 'sheet'); assert.equal(r.dialogs, 0, 'no pop-up');
  await page.click('.tlDetail .tlOpenSheet');
  assert.deepEqual(await page.evaluate(() => window.__sheet), ['sh-k4', `${MAIN}_555_1`]);
  ok.push('click: inline detail (step 13 of 31, before 14K Sheet 3 → after 14K Sheet 4, station badge, facts, 5 around); Open sheet calls onSheet(sh-k4, poolId); no dialog');
  await page.click('.tlSt[data-key="sizeChanged~e12"]'); await page.waitForTimeout(150);
  assert.deepEqual(await page.$$eval('.tlDetail .tlBA .m span', s => s.map(x => x.textContent)), ['4 × 6 in', '6 × 8 in'], 'size change before → after');
  await page.click('.tlSt[data-key="held~e7"]'); await page.waitForTimeout(150);
  assert.match(await page.$eval('.tlDetail .tlWhy', d => d.textContent), /Why it was held.*H or K/);
  // Earlier / Later and the arrow keys
  await page.click('.tlDetail [data-step="1"]'); await page.waitForTimeout(120);
  assert.equal(await page.$eval('.tlDetail h3', h => h.textContent), 'Asked the buyer: is the initial H?');
  await page.keyboard.press('ArrowLeft'); await page.waitForTimeout(120);
  assert.equal(await page.$eval('.tlDetail h3', h => h.textContent), 'Held — waiting on the buyer');
  await page.click('.tlDetail .tlArw:nth-of-type(1)'); await page.waitForTimeout(120);
  ok.push('size change shows 4 × 6 in → 6 × 8 in; a hold shows its reason; Earlier/Later, ← → and Around this step move the detail');

  // ── filters dim what does not match; nothing moves ──
  const pos0 = await page.$$eval('.tlSt[data-key]', s => s.map(b => b.style.left + b.style.top).join());
  await page.click('.tlChip[data-f="stn"]'); await page.waitForTimeout(300);
  r = await page.evaluate(() => ({ lit: window.__el.querySelectorAll('.tlSt[data-key]:not(.dim)').length, count: window.__el.querySelector('.tlChip[data-f="stn"] b').textContent, on: window.__el.querySelector('.tlChip.on').dataset.f, op: getComputedStyle(window.__el.querySelector('.tlSt[data-key="arrived~e1"]')).opacity }));
  assert.equal(String(r.lit), r.count); assert.equal(r.on, 'stn'); assert(+r.op < .2, 'non-matches dim to 13%: ' + r.op);
  assert.equal(await page.$$eval('.tlSt[data-key]', s => s.map(b => b.style.left + b.style.top).join()), pos0, 'nothing moves');
  const counts = await page.$$eval('.tlChip', c => c.map(x => x.textContent));
  ok.push(`filters: ${counts.join(' · ')}; Stations lights ${r.lit} stamps and dims the rest, nothing moves`);
  await page.click('.tlSt[data-key="welded~e25"]'); await page.waitForTimeout(350);
  await page.hover('.tlSt[data-key="sorted~e22"]'); await page.waitForTimeout(320);
  await shot('tlui-2-hover-filter');
  await page.mouse.move(700, 880);
  await page.click('.tlChip[data-f="hc"]'); await page.waitForTimeout(100);
  assert.equal(await page.$$eval('.tlSt[data-key]:not(.dim)', s => s.map(b => b.dataset.key.split('~')[0]).join()), 'held,released');
  await page.click('.tlChip[data-legend]'); await page.waitForTimeout(200);
  assert(await page.$$eval('.tlLegend figure', f => f.length) >= 45, 'the stamp legend shows every kind');
  await page.click('.tlChip[data-f="all"]'); await page.waitForTimeout(250);
  assert.equal(await page.$$eval('.tlSt.dim', s => s.length), 0);

  // ── live: a new event from this page drops in and the rail moves on ──
  await page.click('.tlSt[data-key="labelPrinted~e31"]'); await page.waitForTimeout(200);
  await page.evaluate(MAIN => OrderTimeline.record({ orderId: MAIN, type: 'shipped', by: 'Dana K.', station: 'shipping', text: 'Shipped — USPS acceptance scan', id: 'live-ship-1', data: { carrier: 'USPS' } }), MAIN);
  await page.waitForTimeout(120);
  r = await page.evaluate(() => { const q = s => [...window.__el.querySelectorAll(s)]; const s = q('.tlSt[data-key="shipped~live-ship-1"]')[0]; return { n: q('.tlSt[data-key]').length, anim: s ? s.getAnimations().length : -1, ring: q('.tlInkRing').length, cur: q('.tlStop.c').map(n => n.dataset.stage), done: q('.tlStop.d').length, now: q('.tlNowT')[0].textContent, h: q('.tlDetail h3')[0].textContent, ghosts: q('.tlSt.ghost').length, nowAnim: q('.tlNowLine')[0].getAnimations().length }; });
  assert.equal(r.n, 32); assert(r.anim > 0, 'the new stamp drops in'); assert.equal(r.ring, 1, 'an ink ring spreads');
  assert.deepEqual(r.cur, ['completed']); assert.equal(r.done, 8); assert.equal(r.now, 'Shipped', 'the page\'s own step moves Now ahead of the server\'s where');
  assert.equal(r.h, 'Shipped — USPS acceptance scan', 'the reader was on the latest step, so the detail follows the new one');
  assert.equal(r.ghosts, 1); assert(r.nowAnim > 0, 'the NOW line glides');
  await page.waitForTimeout(1300);
  await shot('tlui-1-live-timeline');
  // the refresh every pollMs while visible (20 s in the app) asks again and keeps the live step
  const g0 = await page.evaluate(() => window.__gets);
  await page.waitForFunction(g => window.__gets > g, g0, { timeout: 4000 });
  await page.waitForTimeout(250);
  assert.equal(await page.$$eval('.tlSt[data-key]', s => s.length), 32);
  assert.equal(await page.$$eval('.tlStop.c', s => s.map(n => n.dataset.stage).join()), 'completed', 'the server\'s older where does not take the rail back');
  ok.push('live: OrderTimeline.record drops the Shipped stamp in with an ink ring, NOW glides, the rail moves to Completed; the timed refresh asks again and keeps it');

  // ── focus(eventId) ──
  assert.equal(await page.evaluate(MAIN => window.__tl.focus(`${MAIN}~sorted~e22`), MAIN), true);
  await page.waitForTimeout(150);
  assert.equal(await page.$eval('.tlSt.sel', b => b.dataset.key), 'sorted~e22');
  assert.equal(await page.evaluate(() => window.__tl.focus('nope')), false);
  // a rail stamp opens its step
  await page.click('.tlStop[data-stage="laser"]'); await page.waitForTimeout(150);
  assert.equal(await page.$eval('.tlSt.sel', b => b.dataset.key), 'roseCut~e20', 'the rail opens the latest event of that step');
  ok.push('focus(eventId) selects and shows the event; a rail stamp opens its step');

  // ── destroy(): nothing left behind ──
  r = await page.evaluate(async () => { const before = window.__tlTimers.size; window.__tl.destroy(); const after = window.__tlTimers.size, g = window.__gets; await new Promise(r => setTimeout(r, 2200)); OrderTimeline.record({ orderId: '4176208841', type: 'note', text: 'after destroy', id: 'x1' }); return { before, after, gets: window.__gets - g, left: window.__el.children.length, host: document.querySelector('.tlTestHost').remove() }; });
  assert.equal(r.after, 0, 'destroy() leaves no timers'); assert.equal(r.gets, 0, 'no refresh after destroy'); assert.equal(r.left, 0);
  ok.push(`destroy(): ${r.before} timer(s) → 0, no refresh after, container empty, a later record is ignored`);

  // ── the cancelled order ──
  await page.evaluate(CX => { window.__el = document.createElement('div'); window.__host(window.__el); window.__tl = OrderTimelineUI.mount(window.__el, { orderId: CX, onSheet: () => {} }); }, CX);
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 7, null, { timeout: 5000 });
  await page.waitForTimeout(1300);
  r = await page.evaluate(() => { const q = s => [...window.__el.querySelectorAll(s)]; return { now: q('.tlNowT')[0].textContent, sub: q('.tlNowS')[0].textContent, stamp: q('.tlCxStamp text').map(t => t.textContent).join(' '), x: q('.tlStop.x').map(n => n.dataset.stage), gone: q('.tlStop.gone').length, line: (q('.tlNowLine.cx span')[0] || {}).textContent, hatch: q('.tlAfterCx').length, ghosts: q('.tlSt.ghost').length }; });
  assert.equal(r.now, 'Cancelled — do not proceed'); assert.match(r.sub, /Cancelled on Etsy/); assert.match(r.sub, /Buyer requested/);
  assert.match(r.stamp, /CANCELLED/); assert.match(r.stamp, /DO NOT PROCEED/); assert.match(r.stamp, /ON ETSY/);
  assert.deepEqual(r.x, ['laser'], 'a clay ✕ where it stopped (approved, never cut)'); assert.equal(r.gone, 5);
  assert.match(r.line, /^CANCELLED · /); assert.equal(r.hatch, 1); assert.equal(r.ghosts, 0);
  await page.click('.tlSt[data-key="removed~e37"]'); await page.waitForTimeout(350);
  assert.match(await page.$eval('.tlDetail .tlWhy', d => d.textContent), /Why it was taken off.*not cut yet/);
  await page.click('.tlSt[data-key="etsyCancelled~e36"]'); await page.waitForTimeout(400);
  assert.match(await page.$eval('.tlDetail .tlWhy', d => d.textContent), /Why it was cancelled.*Buyer requested/);
  await shot('tlui-3-cancelled');
  ok.push('cancelled: red CANCELLED · DO NOT PROCEED stamp across the rail, ✕ at Laser cut and later steps struck, clay line with hatching, no ghosts, reasons in the detail');
  await page.evaluate(() => { window.__tl.destroy(); document.querySelector('.tlTestHost').remove(); });

  // ── an error says so, with Retry; compact draws the rail only ──
  await page.evaluate(MAIN => { window.__fail = 1; window.__el = document.createElement('div'); window.__host(window.__el); window.__tl = OrderTimelineUI.mount(window.__el, { orderId: MAIN, live: false }); }, MAIN);
  await page.waitForSelector('.tlMsg.err .tlRetry', { timeout: 3000 });
  assert.match(await page.$eval('.tlMsg.err', m => m.textContent), /Couldn't load the timeline: HTTP 503/);
  await page.click('.tlMsg.err .tlRetry');
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 31, null, { timeout: 3000 });
  await page.evaluate(() => { window.__tl.destroy(); document.querySelector('.tlTestHost').remove(); });
  await page.evaluate(MAIN => { window.__el = document.createElement('div'); window.__host(window.__el); window.__tl = OrderTimelineUI.mount(window.__el, { orderId: MAIN, compact: true, live: false }); }, MAIN);
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlStop.d').length === 7, null, { timeout: 3000 });
  r = await page.evaluate(() => ({ grid: getComputedStyle(window.__el.querySelector('.tlGrid')).display, rail: window.__el.querySelector('.tlRail').getBoundingClientRect().height }));
  assert.equal(r.grid, 'none'); assert(r.rail > 30);
  await page.evaluate(() => { window.__tl.destroy(); document.querySelector('.tlTestHost').remove(); });
  ok.push('an error shows "Couldn\'t load the timeline: HTTP 503" with Retry, which loads it; compact draws the Now line and rail only');

  // ── the real client over the stand-in: record and send three steps, then timelineGet (with its where) paints them ──
  const REAL = '4170000001';
  await page.evaluate(REAL => {
    const t = Date.now() - 3 * 3600e3;
    OrderTimeline.record({ orderId: REAL, type: 'arrived', by: 'Etsy', text: 'Order arrived from Etsy', at: t, id: 'r1' });
    OrderTimeline.record({ orderId: REAL, type: 'placed', sheet: '14K Sheet 9', sheetId: 'sh-k9', text: 'Placed on 14K Sheet 9', at: t + 60e3, id: 'r2' });
    OrderTimeline.record({ orderId: REAL, type: 'laserDone', by: 'Marco R.', station: 'laser', sheet: '14K Sheet 9', sheetId: 'sh-k9', text: '14K Sheet 9 laser cut', at: t + 3600e3, id: 'r3' });
    OrderTimeline.flush();
  }, REAL);
  await page.waitForFunction(() => OrderTimeline.pending() === 0, null, { timeout: 8000 });
  await page.evaluate(REAL => { window.__el = document.createElement('div'); window.__host(window.__el); window.__tl = OrderTimelineUI.mount(window.__el, { orderId: REAL, live: false }); }, REAL);
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 3, null, { timeout: 5000 });
  r = await page.evaluate(() => { const q = s => [...window.__el.querySelectorAll(s)]; return { now: q('.tlNowT')[0].textContent, done: q('.tlStop.d').map(n => n.dataset.stage), cur: q('.tlStop.c').map(n => n.dataset.stage), approved: q('.tlStop[data-stage="approved"]')[0].title, line: q('.tlNowLine')[0].textContent, pend: q('.tlSt.pend').length }; });
  assert.deepEqual(r.done, ['arrived', 'sheet', 'approved', 'laser'], 'a step passed with no event of its own shows done: ' + r.done);
  assert.equal(r.approved, 'Approved: done'); assert.deepEqual(r.cur, ['sorted']);
  assert.equal(r.now, 'Cut on the laser'); assert.match(r.line, /^NOW · /); assert.equal(r.pend, 0, 'every step came back from the server');
  await page.evaluate(() => { window.__tl.destroy(); document.querySelector('.tlTestHost').remove(); });
  ok.push('the real OrderTimeline over the stand-in\'s timelineAdd/timelineGet: 3 recorded steps come back, Now "Cut on the laser" from where, rail 4 done (Approved without its own event), Sorted next');

  // ── reduced motion: no animations run ──
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.evaluate(MAIN => { window.__el = document.createElement('div'); window.__host(window.__el); window.__tl = OrderTimelineUI.mount(window.__el, { orderId: MAIN, live: false }); }, MAIN);
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 31, null, { timeout: 3000 });
  await page.hover('.tlSt[data-key="welded~e25"]'); await page.click('.tlSt[data-key="moved~e13"]');
  // what is left are the 1 ms stand-ins the reduced-motion rule leaves (instant); nothing lasts longer
  r = await page.evaluate(() => window.__el.getAnimations({ subtree: true }).filter(a => { const t = a.effect.getComputedTiming(); return !(t.duration <= 1 && t.iterations === 1); }).map(a => a.animationName || a.transitionProperty || 'script').join());
  assert.equal(await page.evaluate(() => getComputedStyle(window.__el.querySelector('.tlBusy')).display), 'none', 'no stray spinner');
  assert.equal(r, '', 'reduced motion: nothing animates: ' + r);
  await page.evaluate(() => { window.__tl.destroy(); document.querySelector('.tlTestHost').remove(); });
  ok.push('prefers-reduced-motion: the same states, without animations');

  assert.deepEqual(errors, [], 'no page errors: ' + errors.join('\n'));
  await browser.close(); srv.close();
  console.log('Timeline UI OK:\n - ' + ok.join('\n - '));
  process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
