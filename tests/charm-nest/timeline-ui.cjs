// Browser test of the order timeline component (charm-nest-timeline-ui.js) inside the sorter page, over the local
// stand-in for the site (bridge-server.cjs). OrderTimeline.get is stubbed with one order of 31 events over six days and a
// cancelled one, each with the `where` the server's whereOf gives; one more order goes through the real client and the
// stand-in's timelineAdd/timelineGet. Checks the Now line and the milestone rail, the lanes and their stamps, rest (the grown seal), click
// (inline detail: reason, Open sheet, Around this step), the noise that draws no seal, a live event arriving, the 20 s
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
    window.__tl = OrderTimelineUI.mount(window.__el, { orderId: MAIN, live: true, pollMs: 1500, onSheet: (s, p) => { window.__sheet = [s, p]; }, onEvents: l => { window.__evList = l; }, onNow: n => { window.__nowSaid = n; } });
    window.__waitText = window.__el.querySelector('.tlMsg').textContent;
  }, MAIN);
  assert.match(await page.evaluate(() => window.__waitText), /Loading the timeline of order 4176208841/);
  // only real milestones and what a person did are sealed (Paul, 28 Sep), and every QR label printed (29 Sep): 10 of 31
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 10, null, { timeout: 5000 });
  const r0 = await page.evaluate(() => ({ n: window.__evList.length, first: window.__evList[0], now: window.__nowSaid.text, next: window.__nowSaid.next, step: window.__nowSaid.step, drawn: [...window.__el.querySelectorAll('.tlSt[data-key]')].map(b => b.dataset.key.split('~')[0]) }));
  assert.equal(r0.n, 31); assert.equal(r0.first.type, 'arrived'); assert.equal(r0.first.id, `${MAIN}~arrived~e1`); assert.equal(r0.first.x, undefined, 'the host gets the records, not the drawing');
  assert.deepEqual(r0.drawn, ['arrived', 'placed', 'engraveApproved', 'held', 'released', 'laserDone', 'sorted', 'sealPrinted', 'welded', 'assembled'], 'seals: the milestones, what a person did and the label printed — ' + r0.drawn);
  assert.equal(r0.now, 'Packed'); assert.equal(r0.next, 'Shipped'); assert.equal(r0.step, 6);
  ok.push('mounts on a detached container: a spinner line says what it loads, then 10 seals (milestones, the hold/release and the QR label printed), while onEvents still hands the host all 31 records; onNow "Packed", next Shipped');
  await page.evaluate(() => window.__host(window.__el));
  await page.waitForTimeout(900);

  // ── Now and the rail ──
  let r = await page.evaluate(() => { const q = s => [...window.__el.querySelectorAll(s)]; return { now: q('.tlNowT')[0].textContent, done: q('.tlStop.d').map(n => n.dataset.stage), cur: q('.tlStop.c').map(n => n.dataset.stage), fut: q('.tlStop.f').map(n => n.dataset.stage), skip: q('.tlStop.s').length, fill: q('.tlFill')[0].style.transform, ghosts: q('.tlSt.ghost').length, nowLine: (q('.tlNowLine')[0] || {}).textContent, lanes: q('.tlLane span').map(s => s.textContent), days: q('.tlDay:not(.idle)').length, idle: q('.tlDay.idle').length }; });
  assert.deepEqual(r.done, ['arrived', 'sheet', 'engraved', 'laser', 'sorted', 'welded', 'assembled'], 'passed steps stamped: ' + r.done);
  assert.deepEqual(r.cur, ['shipped'], 'the next step pulses');
  assert.deepEqual(r.fut, [], 'Shipped is the last step (Etsy\'s completion folds into it)');
  assert.equal(r.now, where[MAIN].label); assert.equal(r.now, 'Packed');
  assert.match(r.fill, /scaleX\(1(\.0+)?\)/);
  assert.equal(r.ghosts, 1, 'the milestone to come is a dashed stamp on its lane');
  assert.equal(r.nowLine, 'NOW · AT SHIPPING');
  assert.equal(r.days, 4); assert.equal(r.idle, 0, 'the days that held only noise are gone');
  assert.match(r.lanes[3], /Ana P\./); assert.match(r.lanes[6], /Dana K\./);
  r = await page.evaluate(() => ({ sub: window.__el.querySelector('.tlNowS').textContent, rail: [...window.__el.querySelectorAll('.tlStop span')].map(s => s.textContent) }));
  assert.deepEqual(r.rail, where[MAIN].rail, 'the rail is the server\'s 8 steps');
  assert.deepEqual(r.rail, ['Order in', 'Nested', 'Engraved', 'Laser cut', 'Sorted', 'Welded', 'Assembled', 'Shipped']);
  // a piece's own steps: Welded only for a stud earring (the sorter's purchaseDetails reads its type)
  const sf = await page.evaluate(() => { const k = x => OrderTimelineUI.stagesFor(x).map(s => s.k).join(' '); return { neck: k({ title: 'Custom Name Necklace, Dainty' }), stud: k({ line: { title: 'Tiny Moon Stud Earrings', variations: [] }, spec: {} }), huggie: k({ title: 'Star Huggie Hoop Earrings' }), opt: k({ title: 'Zodiac Charm', variations: [{ name: 'Style', value: 'Stud earrings' }] }), order: k([{ title: 'Name Necklace' }, { title: 'Heart Stud Earrings' }]), none: k([{ title: 'Name Necklace' }, { form: 'huggie' }]), unread: k({ title: '', variations: [] }), all: k(null) }; });
  assert.equal(sf.neck, 'arrived sheet engraved laser sorted assembled shipped', 'a necklace is never welded');
  assert.equal(sf.stud, 'arrived sheet engraved laser sorted welded assembled shipped', 'a stud earring is welded');
  assert.equal(sf.huggie, sf.neck, 'a huggie is not a stud'); assert.equal(sf.opt, sf.stud, 'the Style option says stud');
  assert.equal(sf.order, sf.stud, 'an order with any stud earring shows Welded'); assert.equal(sf.none, sf.neck, 'an order with none does not');
  assert.equal(sf.unread, sf.stud, 'a piece not read yet does not rule welding out'); assert.equal(sf.all, sf.stud);
  // Engraved like Welded: only a piece with a back engraving takes it (its Engrave state, else its spec); unknown keeps it
  const se = await page.evaluate(() => {
    const k = x => OrderTimelineUI.stagesFor(x).map(s => s.k).join(' '), neck = { title: 'Name Necklace' }, R = (spec, engrave) => ({ line: neck, spec, engrave });
    const plain = R({ engraveCandidate: false }), words = R({ engraveCandidate: true }, { needed: true, state: 'words' });
    const ev = [{ type: 'arrived', at: 1, id: 'a' }, { type: 'engraveApproved', at: 2, id: 'b', lineKey: 'k1' }];
    return { plain: k(plain), none: k(R({ engraveCandidate: true }, { needed: false, state: 'none', approved: true })), skipped: k(R({ engraveCandidate: true }, { needed: false, state: 'skipped' })),
      noDesign: k(R({ noDesign: true, engraveCandidate: false })), words: k(words), reading: k(R({ engraveCandidate: true }, { needed: false, state: 'classify' })), cand: k(R({ engraveCandidate: true })),
      studPlain: k({ line: { title: 'Tiny Moon Stud Earrings', variations: [] }, spec: { engraveCandidate: false } }), piece: k(Object.assign({}, neck, { engrave: { state: 'none' }, engraveCandidate: true })),
      mix: k([plain, words]), allPlain: k([plain, R({ engraveCandidate: true }, { state: 'none' })]), etsy: k(neck),
      kept: OrderTimelineUI.summary(ev, [{ key: 'k1', tid: '1', qty: 1, line: Object.assign({}, neck, { engraveCandidate: false }) }]).each[0].steps.map(s => s.k).join(' ') };
  });
  const bare = 'arrived sheet laser sorted assembled shipped';
  assert.equal(se.plain, bare, 'no personalisation, message or note: no Engraved step'); assert.equal(se.none, bare, 'Engrave state none: no Engraved step');
  assert.equal(se.skipped, bare, 'cut plain: no Engraved step'); assert.equal(se.noDesign, bare); assert.equal(se.piece, bare, 'a piece line carrying state none');
  assert.equal(se.studPlain, 'arrived sheet laser sorted welded assembled shipped', 'a plain stud: Welded, no Engraved');
  for (const k of ['words', 'reading', 'cand', 'etsy']) assert.equal(se[k], sf.neck, k + ': a back engraving, or not known yet, keeps Engraved');
  assert.equal(se.mix, sf.neck, 'an order with any engraved piece shows Engraved'); assert.equal(se.allPlain, bare, 'an order of plain pieces does not');
  assert.equal(se.kept, sf.neck, 'an engraveApproved event keeps the step on a plain piece (what happened is always drawn)');
  assert.match(r.sub, /Shipping/); assert.match(r.sub, /Dana K\./); assert.match(r.sub, /next: Shipped/); assert.match(r.sub, new RegExp('on ' + where[MAIN].sheet));
  await page.click('.tlNowS .tlOpenSheet');
  assert.deepEqual(await page.evaluate(() => window.__sheet), [where[MAIN].sheetId, null], 'Open sheet on the where\'s sheet (the last one cut)');
  ok.push('Now reads the server\'s where ("Packed", Shipping · Dana K., next: Shipped, on RG Sheet 7 + Open sheet); the 8-step rail: 7 stamped, Shipped pulses; stagesFor: Welded only for stud earrings, Engraved only with a back engraving (unknown keeps it); 5 day columns + 1 idle; NOW · AT SHIPPING');

  // ── rest on a stamp (750 ms): its seal grows where it stands, by the shared adaptive curve (Seal.zoom); no loupe, no second seal,
  //    no dark caption; the step explainer card sits under the grown seal; the dot keeps the hover (Paul, 2026-10-02) ──
  const welded = await page.$('.tlSt[data-key="welded~e25"]');
  const small = await welded.boundingBox();
  await welded.hover();
  await page.waitForTimeout(1500);
  const zoomAt = () => page.evaluate(() => { const s = window.__el.querySelector('.tlSt[data-key="welded~e25"]'), d = s.getBoundingClientRect(), X = [...window.__el.querySelectorAll('.tlExp')].find(x => getComputedStyle(x).display === 'block'); return { grown: s.dataset.sealZoom || '', k: window.Seal.zoomScale(s.offsetWidth), w: d.width, top: d.top, bottom: d.bottom, cx: d.left + d.width / 2, left: d.left, right: d.right, copy: !!document.querySelector('.tlLoupe,.tlNowZoom,.sealLens'), capTop: X ? X.getBoundingClientRect().top : null, expText: X ? X.textContent : '', texts: [...s.querySelectorAll('text')].map(t => t.textContent).join(' | '), aria: s.getAttribute('aria-label') || '', hov: s.matches(':hover'), shadow: getComputedStyle(s).filter, titled: s.hasAttribute('title'), op: getComputedStyle(s).opacity }; });
  r = await zoomAt();
  assert(r.grown && !r.copy, 'the seal itself has grown, with no second seal: ' + JSON.stringify(r.grown)); assert(small.width < 60, 'a small dot: ' + small.width);
  assert(Math.abs(r.w / small.width - r.k) <= .25 * r.k, `the dot grows by the shared curve (×${r.k}): ${small.width.toFixed(1)}px → ${r.w.toFixed(1)}px`);
  assert(r.left >= 0 && r.right <= 1440 && r.top >= 0, 'and stays in the view');
  assert(+r.op > .9 && r.hov && !r.titled, 'the dot stays in sight, keeps the hover and has no tooltip');
  assert(r.capTop >= r.bottom - 1, 'the step explainer card sits under the grown seal, not over it (it replaced the dark caption): card ' + r.capTop + ', seal bottom ' + r.bottom);
  assert.match(r.texts, /WELDED/); assert.match(r.texts, /\d{1,2}:\d\d [AP]M/); assert.doesNotMatch(r.texts, /MARCO R\.|WELDING STATION|Signed by/, 'no signer on the face: it is for assistive text');
  assert.match(r.aria, /Marco R\./, 'the signer is the dot\'s accessible name'); assert.match(r.expText, /Welded/); assert(r.shadow !== 'none', 'lifted on a soft shadow: ' + r.shadow);
  // moving within the dot keeps the seal, still and in place
  for (const [dx, dy] of [[-.3, -.3], [.3, .25], [0, .35], [-.35, 0]]) { await page.mouse.move(small.x + small.width * (.5 + dx), small.y + small.height * (.5 + dy)); await page.waitForTimeout(40); }
  await page.waitForTimeout(160);
  const r2 = await zoomAt(); assert(r2.grown, 'moving within the dot keeps the seal grown'); assert(Math.abs(r2.top - r.top) < 1 && Math.abs(r2.cx - r.cx) < 1, 'and it does not move');
  ok.push(`rest: the seal grows ${Math.round(small.width)}px → ${Math.round(r.w)}px (×${r.k}) where it stands, with shadow, no loupe, copy or caption, the step card under it; the dot stays in sight and keeps the hover; the signer is its accessible name`);
  await page.mouse.move(700, 880); await page.waitForTimeout(500);
  assert.equal(await page.evaluate(() => document.querySelectorAll('[data-seal-zoom]').length + ([...window.__el.querySelectorAll('.tlExp')].filter(x => getComputedStyle(x).display === 'block').length)), 0, 'the seal and its card go back');

  // ── the step explainer (Paul, 28 Sep, point 5): a step to come, hovered, says below its dot what is still missing ──
  const futs = await page.$$eval('.tlStop:not(.d)', s => s.map(n => n.dataset.stage)), futK = futs[futs.length - 1];
  await page.hover(`.tlStop[data-stage="${futK}"] .tlSeal`); await page.waitForTimeout(1500);
  r = await page.evaluate(k => { const X = window.__el.querySelector('.tlExp'), b = X.getBoundingClientRect(), st = window.__el.querySelector(`.tlStop[data-stage="${k}"]`), d = st.querySelector('.tlSeal').getBoundingClientRect(); return { disp: getComputedStyle(X).display, text: X.textContent, need: X.querySelectorAll('.rq:not(.ok)').length, top: b.top, dot: d.bottom, title: st.getAttribute('title') }; }, futK);
  assert.equal(r.disp, 'block', 'the step card shows');
  assert(r.need > 0 && /Next|After|Waiting|Needs a person/.test(r.text), 'a step to come lists what is missing: ' + r.text);
  assert(r.top >= r.dot, `the card sits below the dot (${r.top} ≥ ${r.dot})`); assert.equal(r.title, null, 'no dark tooltip on the rail');
  const ghostK = await page.$eval('.tlSt.ghost[data-stage]', g => g.dataset.stage);
  await page.hover(`.tlSt.ghost[data-stage="${ghostK}"]`); await page.waitForTimeout(1500);
  assert(await page.evaluate(() => window.__el.querySelectorAll('.tlExp .rq:not(.ok)').length) > 0, 'a dashed stamp to come shows its missing lines');
  await shot('tlui-d-step-card');
  await page.click(`.tlStop[data-stage="${futK}"]`); await page.waitForTimeout(150);
  const pinned = await page.evaluate(() => ({ pin: !!window.__el.querySelector('.tlDetail .tlPin'), path: window.__el.querySelectorAll('.tlDetail .tlPath2 button').length, need: window.__el.querySelectorAll('.tlDetail .tlPin .rq:not(.ok)').length }));
  assert(pinned.pin && pinned.need > 0 && pinned.path >= 7, 'a click pins the step inline with the whole path: ' + JSON.stringify(pinned));
  await shot('tlui-d-pinned');
  await page.mouse.move(700, 880); await page.waitForTimeout(300);   // (a seal rested on has its own Escape first: it goes back, then the pin)
  await page.keyboard.press('Escape'); await page.waitForTimeout(100);
  assert.equal(await page.evaluate(() => !!window.__el.querySelector('.tlDetail .tlPin')), false, 'Escape unpins');
  await page.mouse.move(700, 880); await page.waitForTimeout(200);
  ok.push(`step explainer: hovering "${futK}" (to come) shows a card below its dot with ${r.need} missing line(s), no dark tooltip; a dashed stamp says the same; a click pins it inline with the path, Escape unpins`);

  // ── click: the detail opens inline (no dialog), with before → after, the sheet and Open sheet ──
  await page.click('.tlSt[data-key="placed~e4"]');
  await page.waitForTimeout(350);
  r = await page.evaluate(() => { const d = window.__el.querySelector('.tlDetail'); return { h: d.querySelector('h3').textContent, kind: d.querySelector('.tlLbl').textContent, ba: [...d.querySelectorAll('.tlBA .m span')].map(s => s.textContent), badge: d.querySelector('.tlBadge').textContent, facts: [...d.querySelectorAll('.tlMeta .m')].map(m => m.textContent).join(' | '), around: [...d.querySelectorAll('.tlArw')].length, cur: d.querySelector('.tlArw.cur b').textContent, sel: window.__el.querySelectorAll('.tlSt.sel').length, lane: window.__el.querySelector('.tlLane.on').dataset.lane, dialogs: document.querySelectorAll('dialog[open]').length }; });
  assert.equal(r.h, 'Tiny Initial Tag placed on 14K Sheet 3'); assert.match(r.kind, /milestone 2 of 10/);
  assert.match(r.badge, /Sheet & laser/i); assert.match(r.badge, /Automatic/);
  assert.match(r.facts, /Sheet14K Sheet 3/); assert.equal(r.around, 4); assert.equal(r.cur, 'Tiny Initial Tag placed on 14K Sheet 3');
  assert.equal(r.sel, 1); assert.equal(r.lane, 'sheet'); assert.equal(r.dialogs, 0, 'no pop-up');
  await page.click('.tlDetail .tlOpenSheet');
  assert.deepEqual(await page.evaluate(() => window.__sheet), ['sh-k3', `${MAIN}_555_1`]);
  ok.push('click: inline detail (milestone 2 of 10, station badge, facts, 4 around); Open sheet calls onSheet(sh-k3, poolId); no dialog');
  await page.click('.tlSt[data-key="held~e7"]'); await page.waitForTimeout(150);
  assert.match(await page.$eval('.tlDetail .tlWhy', d => d.textContent), /Why it was held.*H or K/);
  // Earlier / Later and the arrow keys
  await page.click('.tlDetail [data-step="1"]'); await page.waitForTimeout(120);
  assert.equal(await page.$eval('.tlDetail h3', h => h.textContent), 'Released — initial H confirmed', 'Later walks the seals, not the noise between them');
  await page.keyboard.press('ArrowLeft'); await page.waitForTimeout(120);
  assert.equal(await page.$eval('.tlDetail h3', h => h.textContent), 'Held — waiting on the buyer');
  await page.click('.tlDetail .tlArw:nth-of-type(1)'); await page.waitForTimeout(120);
  ok.push('a hold shows its reason; Earlier/Later, ← → and Around this step move the detail, seal to seal');

  // ── the noise draws no seal, and the filter chips are gone: one quiet "Stamps" chip is left ──
  const chips = await page.$$eval('.tlChip', c => c.map(x => x.textContent));
  assert.deepEqual(chips, ['Stamps'], 'the All · Milestones · Stations · Sheets · Holds & cancels · Messages chips are gone: ' + chips);
  const noise = await page.$$eval('.tlSt[data-key]', s => s.map(b => b.dataset.key.split('~')[0]).filter(t => ['pulled', 'interpreted', 'scan', 'moved', 'qrLabel', 'setCommitted', 'included', 'merged', 'note', 'teamMessage', 'customerMessage', 'packed', 'labelPrinted', 'decided', 'sizeChanged', 'roseLine', 'roseCut', 'engraveNeeded'].includes(t)));
  assert.deepEqual(noise, [], 'read, pulled, scanned, moved, QR label, set committed … draw no seal');
  assert.equal(await page.$$eval('.tlSt.dim', s => s.length), 0, 'nothing is dimmed any more');
  await page.click('.tlSt[data-key="welded~e25"]'); await page.waitForTimeout(350);
  await page.hover('.tlSt[data-key="sorted~e22"]'); await page.waitForTimeout(1500);
  await shot('tlui-2-hover-filter');
  await page.mouse.move(700, 880);
  await page.click('.tlChip[data-legend]'); await page.waitForTimeout(200);
  const leg = await page.$$eval('.tlLegend figure', f => f.length);
  assert(leg >= 14 && leg <= 22, 'the legend shows the seals that are drawn, not all 45 kinds: ' + leg);
  await page.click('.tlChip[data-legend]'); await page.waitForTimeout(250);
  ok.push(`the noise draws no seal; the filter chips are gone (one quiet "Stamps" chip left) and the legend is ${leg} seals, not 45`);

  // ── live: a new event from this page drops in and the rail moves on ──
  await page.click('.tlSt[data-key="assembled~e28"]'); await page.waitForTimeout(200);
  await page.evaluate(MAIN => OrderTimeline.record({ orderId: MAIN, type: 'shipped', by: 'Dana K.', station: 'shipping', text: 'Shipped — USPS acceptance scan', id: 'live-ship-1', data: { carrier: 'USPS' } }), MAIN);
  await page.waitForTimeout(120);
  r = await page.evaluate(() => { const q = s => [...window.__el.querySelectorAll(s)]; const s = q('.tlSt[data-key="shipped~live-ship-1"]')[0]; return { n: q('.tlSt[data-key]').length, anim: s ? s.getAnimations().length : -1, ring: q('.tlInkRing').length, cur: q('.tlStop.c').map(n => n.dataset.stage), done: q('.tlStop.d').length, now: q('.tlNowT')[0].textContent, h: q('.tlDetail h3')[0].textContent, ghosts: q('.tlSt.ghost').length, nowAnim: q('.tlNowLine')[0].getAnimations().length }; });
  assert.equal(r.n, 11); assert(r.anim > 0, 'the new stamp drops in'); assert.equal(r.ring, 1, 'an ink ring spreads');
  assert.deepEqual(r.cur, [], 'Shipped is the last step'); assert.equal(r.done, 8); assert.equal(r.now, 'Shipped', 'the page\'s own step moves Now ahead of the server\'s where');
  assert.equal(r.h, 'Shipped — USPS acceptance scan', 'the reader was on the latest step, so the detail follows the new one');
  assert.equal(r.ghosts, 0); assert(r.nowAnim > 0, 'the NOW line glides');
  await page.waitForTimeout(1300);
  await shot('tlui-1-live-timeline');
  // the refresh every pollMs while visible (20 s in the app) asks again and keeps the live step
  const g0 = await page.evaluate(() => window.__gets);
  await page.waitForFunction(g => window.__gets > g, g0, { timeout: 4000 });
  await page.waitForTimeout(250);
  assert.equal(await page.$$eval('.tlSt[data-key]', s => s.length), 11);
  assert.equal(await page.$$eval('.tlStop.d', s => s.length), 8, 'the server\'s older where does not take the rail back');
  ok.push('live: OrderTimeline.record drops the Shipped stamp in with an ink ring, NOW glides, the rail ends at Shipped; the timed refresh asks again and keeps it');

  // ── focus(eventId) ──
  assert.equal(await page.evaluate(MAIN => window.__tl.focus(`${MAIN}~sorted~e22`), MAIN), true);
  await page.waitForTimeout(150);
  assert.equal(await page.$eval('.tlSt.sel', b => b.dataset.key), 'sorted~e22');
  assert.equal(await page.evaluate(() => window.__tl.focus('nope')), false);
  assert.equal(await page.evaluate(() => window.__tl.focus(window.__evList.find(e => e.type === 'welded'))), true, 'focus takes an event as onEvents/onOpen hand it out');
  await page.waitForTimeout(150);
  assert.equal(await page.$eval('.tlSt.sel', b => b.dataset.key), 'welded~e25');
  // a rail stamp opens its step
  await page.click('.tlStop[data-stage="laser"]'); await page.waitForTimeout(150);
  assert.equal(await page.$eval('.tlSt.sel', b => b.dataset.key), 'laserDone~e19', "the rail opens that step's seal (its rose cut is noise, and draws none)");
  ok.push('focus(eventId) selects and shows the event; a rail stamp opens its step');

  // ── destroy(): nothing left behind ──
  r = await page.evaluate(async () => { const before = window.__tlTimers.size; window.__tl.destroy(); const after = window.__tlTimers.size, g = window.__gets; await new Promise(r => setTimeout(r, 2200)); OrderTimeline.record({ orderId: '4176208841', type: 'note', text: 'after destroy', id: 'x1' }); return { before, after, gets: window.__gets - g, left: window.__el.children.length, host: document.querySelector('.tlTestHost').remove() }; });
  assert.equal(r.after, 0, 'destroy() leaves no timers'); assert.equal(r.gets, 0, 'no refresh after destroy'); assert.equal(r.left, 0);
  ok.push(`destroy(): ${r.before} timer(s) → 0, no refresh after, container empty, a later record is ignored`);

  // ── the cancelled order ──
  await page.evaluate(CX => { window.__el = document.createElement('div'); window.__host(window.__el); window.__tl = OrderTimelineUI.mount(window.__el, { orderId: CX, onSheet: () => {} }); }, CX);
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 6, null, { timeout: 5000 });
  await page.waitForTimeout(1300);
  r = await page.evaluate(() => { const q = s => [...window.__el.querySelectorAll(s)]; return { now: q('.tlNowT')[0].textContent, sub: q('.tlNowS')[0].textContent, stamp: q('.tlCxStamp text').map(t => t.textContent).join(' '), x: q('.tlStop.x').map(n => n.dataset.stage), gone: q('.tlStop.gone').length, line: (q('.tlNowLine.cx span')[0] || {}).textContent, hatch: q('.tlAfterCx').length, ghosts: q('.tlSt.ghost').length }; });
  assert.equal(r.now, 'Cancelled — do not proceed'); assert.match(r.sub, /Cancelled on Etsy/); assert.match(r.sub, /Buyer requested/);
  assert.match(r.stamp, /CANCELLED/); assert.match(r.stamp, /DO NOT PROCEED/); assert.match(r.stamp, /ON ETSY/);
  assert.deepEqual(r.x, ['laser'], 'a clay ✕ where it stopped (engraved, never cut)'); assert.equal(r.gone, 4);
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
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 10, null, { timeout: 3000 });
  await page.evaluate(() => { window.__tl.destroy(); document.querySelector('.tlTestHost').remove(); });
  // compact, as the order view's header holds it: a 44px strip, the rail alone; a stamp hands its event to onOpen
  await page.evaluate(MAIN => {
    const h = document.createElement('div'); h.className = 'tlTestHost'; h.style.cssText = 'position:fixed;left:200px;top:4px;width:760px;height:44px;z-index:2147480000;background:var(--card);overflow:hidden';
    window.__el = document.createElement('div'); window.__el.style.height = '100%'; h.appendChild(window.__el); document.body.appendChild(h);
    window.__hostClick = null; h.addEventListener('click', e => { window.__hostClick = e.defaultPrevented; });
    window.__tl = OrderTimelineUI.mount(window.__el, { orderId: MAIN, compact: true, live: false, onOpen: ev => { window.__opened = ev; } });
    window.__waitText = window.__el.querySelector('.tlMsg').textContent;
  }, MAIN);
  assert.equal(await page.evaluate(() => window.__waitText), 'Loading the steps…');
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlStop.d').length === 7, null, { timeout: 3000 });
  r = await page.evaluate(() => { const q = s => window.__el.querySelector(s), b = q('.tlRail').getBoundingClientRect(), stops = [...window.__el.querySelectorAll('.tlStop')].map(n => n.getBoundingClientRect()); return { grid: getComputedStyle(q('.tlGrid')).display, now: getComputedStyle(q('.tlNow')).display, h: b.height, w: b.width, low: Math.max(...stops.map(s => s.bottom)), n: stops.length }; });
  assert.equal(r.grid, 'none'); assert.equal(r.now, 'none'); assert.equal(r.n, 8);
  assert(r.h <= 44 && r.low <= 48 && r.w > 700, `the rail fits the 44px strip: ${JSON.stringify(r)}`);
  await page.click('.tlStop[data-stage="laser"]');
  r = await page.evaluate(() => ({ opened: window.__opened, host: window.__hostClick }));
  assert.equal(r.opened && r.opened.id, `${MAIN}~roseCut~e20`); assert.equal(r.opened.orderId, MAIN); assert.equal(r.host, true, 'the host sees the click handled');
  await page.evaluate(() => { window.__tl.destroy(); document.querySelector('.tlTestHost').remove(); });
  ok.push('an error shows "Couldn\'t load the timeline: HTTP 503" with Retry, which loads it; compact fits a 44px header strip (rail alone, its own wait line) and a stamp calls onOpen(event)');

  // ── the order view's "Where it is now": the milestone it is at, what is holding it up in words, no row of seals ──
  r = await page.evaluate(({ MAIN, CX }) => {
    const card = document.createElement('div'); card.className = 'tlTestHost'; card.style.cssText = 'position:fixed;left:40px;top:120px;display:flex;gap:16px;align-items:center;padding:20px;background:var(--card);z-index:2147480000';
    document.body.appendChild(card);
    const evs = window.__fx[MAIN].events, last = evs[evs.length - 1];
    const a = OrderTimelineUI.nowStamps(evs, { ev: last }), a2 = OrderTimelineUI.nowStamps(evs, { ev: last });
    card.innerHTML = a.seal + '<div class="row">' + a.recent + '</div>'; OrderTimelineUI.wireNow(card, ev => { window.__opened = ev; });
    const minis = card.querySelectorAll('.tlMini').length, seal = [...card.querySelectorAll('.tlNowSeal text')].map(t => t.textContent).join(' '), w = card.querySelector('.tlNowSeal').getBoundingClientRect().width;
    // an order held, and one with a question nobody answered: the blocker, in plain words
    const hold = OrderTimelineUI.nowStamps(evs.slice(0, 8), {});
    const ask = OrderTimelineUI.nowStamps([evs[0], { orderId: MAIN, id: 'q1', type: 'needsDecision', at: evs[0].at + 6e4, by: 'paul', text: 'Unknown SKU: BLOOMING_20239 (HUGGIE): its huggie design is not in any master file' }], {});
    const cx = window.__fx[CX].events.find(e => e.type === 'etsyCancelled'), c = OrderTimelineUI.nowStamps(window.__fx[CX].events, { ev: cx, cancelled: cx });
    card.innerHTML = c.seal + '<div class="row">' + c.recent + '</div>'; OrderTimelineUI.wireNow(card, () => {});
    const cxEl = card.querySelector('.tlNowSeal.cx');
    return { same: a.seal === a2.seal && a.recent === a2.recent, minis, seal, w, recent: a.recent, blocker: a.blocker,
      hold: hold.blocker, holdRow: hold.recent, ask: ask.blocker, askSeal: /ARRIVED|ORDER/i.test(ask.seal), cxBlock: c.blocker,
      cx: [...cxEl.querySelectorAll('text')].map(t => t.textContent).join(' '), cw: cxEl.getBoundingClientRect().width, drop: cxEl.getAnimations().length };
  }, { MAIN, CX });
  assert.equal(r.minis, 0, 'no row of small stamps any more');
  assert.match(r.seal, /ASSEMBLED/); assert.match(r.seal, /LUISA T\./, 'the seal is the milestone it is at, not the last label printed');
  assert(r.w > 85 && r.w < 140, 'the milestone seal at 92px: ' + r.w);
  assert.equal(r.recent, ''); assert.equal(r.blocker, null, 'nothing is holding this one up');
  assert(r.same, 'the same order draws the same markup (the view skips an unchanged card)');
  assert.equal(r.hold.label, 'On hold'); assert.match(r.hold.text, /H or K/); assert.match(r.holdRow, /On hold/);
  assert.equal(r.ask.label, 'Needs a decision'); assert.match(r.ask.text, /Unknown SKU/); assert(r.askSeal, 'and its seal is the milestone, not a "?"');
  assert.equal(r.cxBlock, null);
  assert.match(r.cx, /CANCELLED ORDER/); assert.match(r.cx, /DO NOT PROCEED/); assert.equal(r.drop, 1, 'the cancel seal drops in');
  // explainOn: a rested seal says what its next step needs; a click opens that step on the Timeline
  await page.evaluate(MAIN => { const card = document.querySelector('.tlTestHost'), a = OrderTimelineUI.nowStamps(window.__fx[MAIN].events, {}); card.innerHTML = a.seal; OrderTimelineUI.explainOn(card, () => ({ events: window.__fx[MAIN].events }), st => { window.__pinned = st; }); }, MAIN);
  await page.mouse.move(0, 0); await page.hover('.tlTestHost .tlNowSeal'); await page.waitForTimeout(1500);
  r = await page.evaluate(() => { const X = [...document.querySelectorAll('.tlExp')].find(x => getComputedStyle(x).display === 'block'); return X ? { need: X.querySelectorAll('.rq:not(.ok)').length, top: X.getBoundingClientRect().top, seal: document.querySelector('.tlTestHost .tlNowSeal').getBoundingClientRect().bottom } : null; });
  assert(r && r.need > 0 && r.top >= r.seal, 'the Overview seal shows the next step\'s missing lines below it: ' + JSON.stringify(r));
  await page.click('.tlTestHost .tlNowSeal');
  assert.equal(await page.evaluate(() => window.__pinned && window.__pinned.stage), 'shipped', 'a click on the original seal asks for the next step on the Timeline');
  await page.evaluate(() => document.querySelector('.tlTestHost').remove());
  ok.push('nowStamps: one seal — the milestone it is at (ASSEMBLED · LUISA T.) — no row of stamps, the blocker in plain words ("On hold — H or K", "Needs a decision — Unknown SKU"), and the 118px CANCELLED ORDER · DO NOT PROCEED seal dropping in');

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
  await page.evaluate(REAL => { window.__el = document.createElement('div'); window.__host(window.__el); window.__tl = OrderTimelineUI.mount(window.__el, { orderId: REAL, live: false, stages: () => OrderTimelineUI.stagesFor([{ title: 'Custom Name Necklace' }]) }); }, REAL);
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 3, null, { timeout: 5000 });
  r = await page.evaluate(() => { const q = s => [...window.__el.querySelectorAll(s)]; return { now: q('.tlNowT')[0].textContent, done: q('.tlStop.d').map(n => n.dataset.stage), cur: q('.tlStop.c').map(n => n.dataset.stage), engraved: q('.tlStop[data-stage="engraved"]')[0].getAttribute('aria-label'), stops: q('.tlStop').map(n => n.dataset.stage).join(' '), line: q('.tlNowLine')[0].textContent, pend: q('.tlSt.pend').length }; });
  assert.deepEqual(r.done, ['arrived', 'sheet', 'engraved', 'laser'], 'a step passed with no event of its own shows done: ' + r.done);
  assert.equal(r.engraved, 'Engraved: done'); assert.deepEqual(r.cur, ['sorted']);
  assert.equal(r.stops, 'arrived sheet engraved laser sorted assembled shipped', 'a necklace order\'s rail has no Welded step');
  assert.equal(r.now, 'Cut on the laser'); assert.match(r.line, /^NOW · /); assert.equal(r.pend, 0, 'every step came back from the server');
  await page.evaluate(() => { window.__tl.destroy(); document.querySelector('.tlTestHost').remove(); });
  ok.push('the real OrderTimeline over the stand-in\'s timelineAdd/timelineGet: 3 recorded steps come back, Now "Cut on the laser" from where, rail 4 done (Engraved without its own event), Sorted next, no Welded for a necklace');

  // ── reduced motion: no animations run ──
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.evaluate(MAIN => { window.__el = document.createElement('div'); window.__host(window.__el); window.__tl = OrderTimelineUI.mount(window.__el, { orderId: MAIN, live: false }); }, MAIN);
  await page.waitForFunction(() => window.__el.querySelectorAll('.tlSt[data-key]').length === 10, null, { timeout: 3000 });
  await page.hover('.tlSt[data-key="welded~e25"]'); await page.click('.tlSt[data-key="placed~e4"]');
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
