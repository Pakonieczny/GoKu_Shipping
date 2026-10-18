// SHEETSTRIP (Paul, 18 Oct 2026: "Remove the '1336 mm free' and condense some of this UI so that everything easily fits on one line"): the sheet
// window's strip under the plate says no free area, says "11 to approve" / "3 approved" beside its dots (the whole words in the tooltip), and holds
// every count and both switches on ONE line at a 1280 px window; at a 1024 px window it fits too, or wraps whole items, never clipping one.
// Offline: headless Chromium over the fake site (bridge-server.cjs: the real charmNestLibrary ops over an in-memory store, nothing live). The sheet is the
// fullest strip there is: a pair, a half pair, a front + back piece, backs to approve and backs approved.
//   node tests/charm-nest/sheet-strip-one-line.cjs       (PW_DIR=<playwright-core's node_modules>, CHROMIUM=<chrome>; SHOT_DIR=<dir> saves pictures of the strip)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');

/* ── the source: the free area is gone from the strip ── */
const sw = fs.readFileSync(path.join(root, 'charm-nest-sheetwin.js'), 'utf8');
const i = sw.indexOf('  function renderStrip() {'), j = sw.indexOf('  function turnPlate(', i);
assert(i > 0 && j > i, 'renderStrip found');
assert.doesNotMatch(sw.slice(i, j), /freePt2|mm² free/, 'the strip no longer says the free area');
console.log('ok   source: renderStrip reads no freePt2 and says no "mm² free"');

(async () => {
  const pwDir = process.env.PW_DIR || [path.join(root, 'node_modules'), '/opt/node22/lib/node_modules/playwright/node_modules'].find(d => fs.existsSync(path.join(d, 'playwright-core')));
  let chromium; try { ({ chromium } = require(path.join(pwDir || '', 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const { start } = require('./bridge-server.cjs'), noNested = require('./_noNestedArrays.cjs');
  const SHEETS = 'Charm_Nest_Sheets', POOL = 'Charm_Pool', BACKS = 'Charm_Pool_Back', SETS = 'Charm_Nest_Sets';
  const HALF = '4300000101', PAIR = '4300000105', NECK = '4195966039', OTHER = '4300000104';
  const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
  const FILL = Array.from({ length: 12 }, (_, k) => [String(4300000110 + k), 10 + k, 1]);
  // GF Sheet 1: one ear of the mismatched pair HALF (its other ear is on GF Sheet 2), both ears of PAIR, the front + back necklace, twelve plain backs
  const S1 = [[HALF, 1, 1], [PAIR, 5, 1], [PAIR, 5, 2], [NECK, 2, 1], ...FILL], S2 = [[HALF, 1, 2], [OTHER, 4, 1]];
  const isPair = r => r === HALF || r === PAIR;
  const pairBits = (r, c) => isPair(r) ? { side: c === 1 ? 'L' : 'R', mirror: c === 2, bodyIndex: r === HALF ? c - 1 : 0, groupKey: `${r}:${5000000000 + (r === HALF ? 1 : 5)}` } : {};
  const srv = await start({ receipts: [] }), { st, sorterOrigin } = srv;
  const sheet = (id, n, list) => ({ id, metal: 'gold', setId: 'set-1', density: .72, freePt2: 1336 / (25.4 / 72) ** 2,
    charms: list.map(([r, t, c], k) => ({ id: id + k, poolId: pid(r, t, c), order: r, sku: 'TEST-' + t, name: `${r} · TEST-${t}`, ...pairBits(r, c) })),
    placements: list.map((x, k) => ({ id: id + k, cxPt: 25 + (k % 8) * 34, cyPt: 30 + Math.floor(k / 8) * 40, angle: 0, wPt: 22, hPt: 22 })), poolIds: list.map(([r, t, c]) => pid(r, t, c)),
    orders: [...new Set(list.map(x => x[0]))], sheetIndex: n, fileBase: `GF_2026-10-18_Set-1_Sheet-${n}`, runId: 'run-test-1', placedCount: list.length, charmCount: list.length, stock: { wPt: 300, hPt: 150 } });
  const rec = (sheetId, rid, tx, copy, face, words) => Object.assign({ poolId: pid(rid, tx, copy), sheetId, order: rid, sku: 'TEST-' + tx, copy, text: words, lines: [words], sizePt: 7, capMm: 1.7, weight: 'Regular', lineGap: .216, centre: [11, 11], angle: 0, upAngle: 90, approvedAt: Date.now() - 4000, approvedBy: 'Test Operator' }, face === 'front' ? { face: 'front' } : {});
  // approved: the necklace's front and back, both ears' fronts, the first three plain backs; every other engraving still to approve
  const recs = [rec('gold-open-1', NECK, 2, 1, 'front', 'W'), rec('gold-open-1', NECK, 2, 1, 'back', '1. 27. 24'), rec('gold-open-1', PAIR, 5, 1, 'front', 'A'), rec('gold-open-1', PAIR, 5, 2, 'front', 'A'),
    ...FILL.slice(0, 3).map(([r, t, c]) => rec('gold-open-1', r, t, c, 'back', 'Ada'))];
  const yes = { needed: true, state: 'written', approved: true }, no = { needed: true, state: 'written', approved: false, text: 'Bo' };
  const d1 = sheet('gold-open-1', 1, S1), d2 = sheet('gold-open-2', 2, S2);
  d1.backPool = recs; d1.engraving = {};
  for (const [r, t, c] of S1) {
    const p = pid(r, t, c), mine = recs.filter(q => q.poolId === p), front = r === NECK || r === PAIR;
    const dec = f => mine.some(q => (q.face === 'front') === (f === 'front')) ? yes : no;
    d1.engraving[p] = front ? Object.assign({}, dec('back'), { faces: { front: dec('front'), back: dec('back') } }) : dec('back');
  }
  for (const d of [d1, d2]) { noNested(d); st.put(SHEETS, d.id, d); }
  for (const r of recs) st.put(BACKS, r.face === 'front' ? r.poolId + '~front' : r.poolId, r);
  st.put(SETS, 'set-1', { id: 'set-1', seq: 1, sheetIds: ['gold-open-1', 'gold-open-2'], runId: 'run-test-1' });
  for (const [sid, list] of [['gold-open-1', S1], ['gold-open-2', S2]]) for (const [r, t, c] of list) st.put(POOL, pid(r, t, c), { poolId: pid(r, t, c), orderId: r, sheetId: sid, setId: 'set-1', state: 'placed', material: 'gold', lineKey: `${r}_${5000000000 + t}`, sku: 'TEST-' + t,
    ...(isPair(r) ? Object.assign(pairBits(r, c), { groupSize: 2, kind: r === HALF ? 'mismatched' : 'pair' }) : {}) });

  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  await ctx.route(url => !/^https?:\/\/(127\.0\.0\.1|localhost)[:/]/.test(url.href), r => {
    const u = r.request().url();
    if (/gstatic\.com\/firebasejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(u) ? '' : "const nope = () => { throw new Error('firebase stub'); }; export const getFirestore = nope, initializeApp = nope;" });
    if (/qrcodejs/.test(u)) return r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) });
    return r.abort();
  });
  await ctx.addInitScript(() => { try { if (!localStorage.getItem('cn.employee')) localStorage.setItem('cn.employee', 'Tester'); const s = JSON.parse(localStorage.getItem('cn.settings') || '{}'); s.pollOrders = 'off'; s.runMode = 'manual'; localStorage.setItem('cn.settings', JSON.stringify(s)); } catch (_) {} window.confirm = () => true; window.alert = () => {}; });
  const page = await ctx.newPage(), errors = [];
  page.on('pageerror', e => errors.push('page: ' + e.message));
  page.on('console', m => { if (m.type() === 'error' && !/firebase stub|Failed to load resource|ERR_FAILED|net::/.test(m.text())) errors.push('console: ' + m.text().slice(0, 300)); });
  const until = async (fn, ms = 20000, what = '') => { const t0 = Date.now(); for (;;) { const v = await fn(); if (v) return v; if (Date.now() - t0 > ms) throw new Error('timed out: ' + what); await new Promise(r => setTimeout(r, 100)); } };
  // the strip as laid out: its items (each box against the strip's own), the lines they sit on, and whether anything runs past its edge
  const measure = () => page.evaluate(() => {
    const s = document.querySelector('.swStrip'), b = s.getBoundingClientRect(), cs = getComputedStyle(s);
    const left = b.left + parseFloat(cs.paddingLeft), right = b.right - parseFloat(cs.paddingRight);
    const items = [...s.children].flatMap(e => e.classList.contains('swStripR') ? [...e.children] : [e]).map(e => { const r = e.getBoundingClientRect(); return { text: e.innerText.replace(/\s+/g, ' ').trim(), title: e.title || '', l: r.left, r: r.right, top: Math.round(r.top + r.height / 2), clipped: e.scrollWidth > e.clientWidth + 1 }; });
    const lines = items.map(x => x.top).sort((p, q) => p - q).filter((y, k, all) => !k || y - all[k - 1] > 6).length;   // (centres a few px apart are one line: a pill is taller than a word)
    return { width: Math.round(b.width), lines, items, overflow: s.scrollWidth > s.clientWidth + 1, outside: items.filter(x => x.l < left - 1 || x.r > right + 1).map(x => x.text), clipped: items.filter(x => x.clipped).map(x => x.text), text: s.innerText.replace(/\s+/g, ' ').trim() };
  });
  try {
    await page.goto(`${sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && CN.S.cloud.ok === true && window.SheetWin && window.OrderPieces && window.PiecePlacement && window.PiecePlacement.groups, null, { timeout: 60000 });
    await page.evaluate(([rows]) => {   // the lines the order rows carry (the pull): each pair is one line of quantity 1 that makes two pieces
      const pid = (rid, tx, copy) => `${rid}_${5000000000 + tx}_${copy}`;
      const row = (rid, tx, n, front) => ({ key: `${rid}_${5000000000 + tx}`, order: { receiptId: rid, orderNumber: rid, createTs: 1790000000, updateTs: 1790000000, shipBy: 1790500000, buyer: { name: 'Buyer' }, lines: [], messages: [] },
        line: { transactionId: String(5000000000 + tx), listingId: '', sku: 'TEST-' + tx, title: 'Test ' + tx, quantity: 1, variations: [], personalization: [] },
        spec: { designSku: 'TEST-' + tx, quantity: 1, material: 'gold', problems: [], ...(front ? { engraveCandidate: true, engraving: { v: 1, mode: 'both', front: { text: 'W' }, back: { text: 'B' }, source: 'labelled', confidence: 1 } } : {}) },
        problems: [], state: 'written', reason: null, poolIds: Array.from({ length: n }, (_, k) => pid(rid, tx, k + 1)), engrave: null, material: 'gold', arrivedAt: Date.now() - 7200000 });
      window.B.orders.rows = rows.map(a => row(...a)); window.B.orders.byKey = new Map(window.B.orders.rows.map(r => [r.key, r]));
    }, [[[HALF, 1, 2], [PAIR, 5, 2, true], [NECK, 2, 1, true], [OTHER, 4, 1], ...FILL.map(([r, t]) => [r, t, 1])]]);
    await page.evaluate(() => SheetWin.open('gold-open-1'));
    const strip = () => page.evaluate(() => (document.querySelector('.swStrip') || {}).innerText || '');
    await until(async () => /to approve/.test(await strip()) && /approved/.test((await strip()).replace(/to approve/g, '')) && /half pair/.test(await strip()) && /\bpair\b/.test((await strip()).replace(/half pair/g, '')), 30000, 'the strip with its pair, half pair and engraving counts');
    await page.waitForTimeout(900);   // (the window's own growth and the strip's fade in)

    let m = await measure();
    console.log(`     1280 px window: strip ${m.width} px, ${m.lines} line(s): ${m.text}`);
    assert.doesNotMatch(m.text, /mm²|free/i, 'no free area in the strip: ' + m.text);
    assert(!m.items.some(x => /free/i.test(x.text + x.title)), 'no item of the strip is the free area');
    assert.match(m.text, /16 charms/); assert.match(m.text, /1 pair\b/); assert.match(m.text, /1 half pair/); assert.match(m.text, /15 orders/); assert.match(m.text, /72% full/);
    const due = m.items.find(x => /to approve/.test(x.text)), done = m.items.find(x => /^\d+ approved$/.test(x.text));
    assert(due && /^\d+ to approve$/.test(due.text), 'the dot says "N to approve": ' + JSON.stringify(due));
    assert(done, 'the dot says "N approved": ' + JSON.stringify(m.items.map(x => x.text)));
    const nDue = +due.text.split(' ')[0], nDone = +done.text.split(' ')[0];
    assert.equal(nDone, 7, 'approved: the necklace front + back, both ears\' fronts, three plain backs: ' + done.text);
    assert.equal(due.title, `${nDue} engravings to approve`, 'the whole words in the tooltip'); assert.equal(done.title, `${nDone} engravings approved`);
    assert.equal(m.lines, 1, 'one line at a 1280 px window: ' + JSON.stringify(m.items.map(x => [x.text, x.top])));
    assert.equal(m.overflow, false, 'nothing runs past the strip'); assert.deepEqual(m.outside, [], 'every item inside the strip'); assert.deepEqual(m.clipped, [], 'no text cut off');
    console.log(`ok   1280 px: one line, no free area, "${due.text}" / "${done.text}" (tooltips "${due.title}" / "${done.title}")`);
    if (process.env.SHOT_DIR) await page.locator('.swStrip').screenshot({ path: path.join(process.env.SHOT_DIR, 'strip-1280.png') });

    // both switches still work, with their names
    const marks = () => page.evaluate(() => { const b = document.querySelector('.swStrip [data-r2=backs]'); return { text: b.innerText, pressed: b.getAttribute('aria-pressed'), title: b.title }; });
    let k = await marks(); assert.equal(k.text, 'Hide marks'); assert.equal(k.pressed, 'true'); assert(k.title, 'the marks switch says what it does');
    await page.click('.swStrip [data-r2=backs]'); k = await marks(); assert.equal(k.text, 'Show marks'); assert.equal(k.pressed, 'false');
    await page.click('.swStrip [data-r2=backs]'); k = await marks(); assert.equal(k.text, 'Hide marks');
    const face = () => page.evaluate(() => { const b = document.querySelector('.swStrip [data-r2=face]'); return { text: b.innerText, pressed: b.getAttribute('aria-pressed'), title: b.title }; });
    let f = await face(); assert.equal(f.text, 'Back · engraving'); assert.match(f.title, /Turn the sheet over/);
    await page.click('.swStrip [data-r2=face]');
    await until(async () => (await face()).text === 'Front', 15000, 'the plate on its back');
    f = await face(); assert.equal(f.pressed, 'true');
    // (while the backs are read the strip says so beside a spinner, and may take a second line for it; measured once they are in)
    await until(() => page.evaluate(() => !document.querySelector('.swStrip .spin')), 20000, 'the backs read');
    m = await measure();
    assert(m.items.some(x => /Back side, mirrored as the laser sees it/.test(x.title)), 'the side the plate shows, in the tooltip: ' + JSON.stringify(m.items));
    assert.equal(m.lines, 1, 'one line on the back too: ' + m.text); assert.deepEqual(m.clipped, []);
    console.log(`     back: ${m.text}`);
    await page.waitForTimeout(800);
    await page.click('.swStrip [data-r2=face]');
    await until(async () => (await face()).text === 'Back · engraving', 15000, 'the plate on its front');
    console.log('ok   Hide / Show marks and Front | Back · engraving switch and keep their names; one line on the back as well');

    // a 1024 px window: one line, or whole items on a second line; never one cut off or run past the edge
    await page.setViewportSize({ width: 1024, height: 768 }); await page.waitForTimeout(400);
    m = await measure();
    console.log(`     1024 px window: strip ${m.width} px, ${m.lines} line(s)`);
    assert(m.lines <= 2, 'at most two lines at 1024 px: ' + m.lines);
    assert.equal(m.overflow, false, 'nothing runs past the strip at 1024 px'); assert.deepEqual(m.outside, [], 'every item inside at 1024 px'); assert.deepEqual(m.clipped, []);
    console.log(`ok   1024 px: ${m.lines === 1 ? 'one line' : 'wraps whole items onto a second line'}, nothing cut off`);
    if (process.env.SHOT_DIR) await page.locator('.swStrip').screenshot({ path: path.join(process.env.SHOT_DIR, 'strip-1024.png') });
    if (process.env.SHOT_DIR) { await page.setViewportSize({ width: 1280, height: 800 }); await page.waitForTimeout(400); await page.screenshot({ path: path.join(process.env.SHOT_DIR, 'window-1280.png') }); }

    assert.deepEqual(errors, [], 'no page errors: ' + errors.join(' | '));
    console.log('\nbrowser checks passed');
  } finally { await browser.close(); srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
