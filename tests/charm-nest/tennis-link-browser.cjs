// TENNISLINK, part C · the page (real headless Chromium against the repo's fake site, which runs the REAL charmNestLibrary handler over an in-memory Firestore: zero network).
//   node tests/charm-nest/tennis-link-browser.cjs [playwright-core dir]      (PW_DIR / CHROMIUM as pair-rows-lists.cjs; skipped without; TL_SHOTS=<dir> saves two screenshots)
// The two real master designs: their index entries as read live on 10 Oct 2026 and their real .ai files, served to the page like Storage serves them.
// The saved link is written once through the page's own call (aliasPut, no sandbox flag = production's store) and then read by a production page and by a sandbox page.
'use strict';
const assert = require('assert'), fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
let n = 0; const ok = (c, m) => { assert(c, m); n++; };
const BALL = 'TENNIS BALL (HUGGIE)', RACKET = 'TENNIS RACKET (HUGGIE)', LID = '1744372161', RAW = 'HUGGIE HOOPS-TENNIS BALL/RACKET3';
const TITLE = 'Mismatched Tennis Ball and Raquet Huggie Hoops Charm, Handcrafted in Gold Vermeil, Silver, Solid 14k Gold | Handcrafted Sports Jewelry';
const SET7 = 'HUGGIE HOOPS-TENNIS SET 7', LID2 = '1744372162';   // (a listing with a SKU no words explain: what Paul's Review rows looked like before the names were read)

(async () => {
  const pwDir = process.env.PW_DIR || process.argv[2] || (fs.existsSync('/opt/node22/lib/node_modules/playwright/node_modules') ? '/opt/node22/lib/node_modules/playwright/node_modules' : path.join(root, 'node_modules'));
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  · tennis-link browser part: no playwright-core, not run'); return; }
  const chromePath = process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome';
  if (!fs.existsSync(chromePath)) { console.log('  · tennis-link browser part: no chrome, not run'); return; }
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] }), st = srv.st;
  // the master: the real index entries and the real files
  const masters = JSON.parse(fs.readFileSync(path.join(__dirname, 'fixtures/tennis-master-entries.json'), 'utf8')).entries;
  const fileOf = { [BALL]: 'TENNIS_BALL_HUGGIE_.ai', [RACKET]: 'TENNIS_RACKET_HUGGIE_.ai' };
  for (const e of masters) {
    st.put('Charm_Master_Index', e.sku, Object.assign({}, e));
    if (fileOf[e.sku]) st.blobs.set(e.aiPath, { buf: fs.readFileSync(path.join(__dirname, 'fixtures', fileOf[e.sku])), generation: 1, meta: { contentType: 'application/illustrator', metadata: { firebaseStorageDownloadTokens: 't' } } });
  }
  const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 12, 17) / 1000);
  const V = (name, value, ids) => Object.assign({ name, value }, ids ? { propertyId: ids[0], valueId: ids[1] } : {});
  const line = (tid, listingId, sku, title, vars, extra) => Object.assign({ transactionId: tid, listingId, sku, title, quantity: 1, expectedShipDate: SHIP, variations: vars }, extra || {});
  const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
  const A = '4171010675', B2 = '4173373368', C = '4171010999', D = '4171011000';
  const orders = [
    order(A, 'Silver 8.5', [line('5213678588', LID, 'Huggie Hoops-Tennis Ball/Racket3', TITLE, [V('METAL CHOICE', 'Silver', ['514', '55196991045']), V('HOOP SIZE', '8.5mm', ['513', '110638330075'])], { productId: '27738389707', metalKey: 'silver', metalLabel: 'Silver' })]),
    order(B2, '14k 11mm', [line('5215153110', LID, 'Huggie Hoops-Tennis Ball/Racket3', TITLE, [V('METAL CHOICE', '14k Solid Gold', ['514', '148615583071']), V('HOOP SIZE', '11mm', ['513', '65252824563'])], { productId: '28104491116', metalKey: '14k', metalLabel: '14K Gold' })]),
    order(C, 'A SKU no words explain', [line('5213670001', LID2, SET7, 'Sports Huggie Hoops Charm', [V('METAL CHOICE', 'Silver'), V('HOOP SIZE', '8.5mm')], { metalKey: 'silver', metalLabel: 'Silver' })]),
    order(D, 'A similar title, another listing', [line('5213670002', '1999000555', 'Huggie Hoops-Tennis Ball/Zebra', 'Mismatched Tennis Ball and Zebra Huggie Hoops Charm, Handcrafted', [V('METAL CHOICE', 'Silver'), V('HOOP SIZE', '8.5mm')], { metalKey: 'silver', metalLabel: 'Silver' })])
  ];
  const browser = await chromium.launch({ executablePath: chromePath, args: ['--no-sandbox'] });
  const errors = [], shots = process.env.TL_SHOTS || '';
  async function session(sandbox) {
    const context = await browser.newContext({ viewport: { width: 1500, height: 1500 } });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(sb => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify(Object.assign({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on', sandbox: 'off' }, sb ? { sandbox: 'on', sandboxStream: 'off' } : {}))); localStorage.setItem('cn.employee', 'Test Operator'); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; }, sandbox);
    const page = await context.newPage(); page.setDefaultTimeout(40000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.Review && window.OrderWin && window.OrderPieces && window.PieceMedia && CN.S.cloud.ok === true && B.master.loadedAt > 0 && !B.master.loading, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const o of orders) o.lines.forEach(l => { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); });
      Orders.interpretAll(); Review.syncOrderItems(); CN.setMode('orders'); Orders.render();
      window.__ink = async src => {   // the picture's ink clusters above the chip band, and the chips
        const im = new Image(); im.src = src; await im.decode(); const cv = document.createElement('canvas'); cv.width = im.width; cv.height = im.height; const c = cv.getContext('2d'); c.drawImage(im, 0, 0); const d = c.getImageData(0, 0, im.width, im.height).data;
        const ink = (x, y) => { const i = (y * im.width + x) * 4; return !(d[i] > 200 && d[i + 1] > 200 && d[i + 2] > 200); }, dark = (x, y) => { const i = (y * im.width + x) * 4; return d[i] < 70 && d[i + 1] < 70 && d[i + 2] < 70; };
        const band = Math.round(im.height * .16), H = im.height - band;
        const cl = (y0, y1, f) => { const cols = []; for (let x = 0; x < im.width; x++) { let h = false; for (let y = y0; y < y1 && !h; y++) h = f(x, y); cols.push(h); } const out = []; let s = -1; for (let x = 0; x <= im.width; x++) { if (cols[x] && s < 0) s = x; if (!cols[x] && s >= 0) { out.push([s, x - 1]); s = -1; } } return out; };
        return { w: im.width, h: im.height, bodies: cl(0, H, ink), chips: cl(H, im.height, dark) };
      };
    }, orders);
    return { page, context };
  }
  const rowState = (page, rid) => page.evaluate(rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid), s = r.spec, p = s && s.pair; return { designSku: s.designSku, source: p && p.source, members: p && (p.members || []).map(m => m.side + ':' + m.sku), problems: s.problems.map(x => x.kind + (x.pairSecond ? ':' + x.pairSecond.why : x.reason ? ':' + x.reason : '')), pieces: s.pieceCount, sides: p && (p.sides || []).join(''), notes: p && p.notes }; }, rid);
  const vectorsDrawn = async (page, sel) => { await page.waitForFunction(s => { const hs = [...document.querySelectorAll(s)]; return hs.length > 0 && hs.every(h => h.getAttribute('aria-busy') === 'false'); }, sel, { timeout: 60000 }); return page.evaluate(async s => Promise.all([...document.querySelectorAll(s)].map(async h => { const im = h.querySelector('img'), nd = h.closest('[data-rid]'); return { text: h.textContent.trim(), rid: nd ? nd.dataset.rid : '', pic: im && im.src ? await window.__ink(im.src) : null }; })), sel); };
  const two = (pic, what) => ok(pic && pic.bodies.length === 2 && pic.chips.length === 2, `${what}: BOTH designs side by side with a Left and a Right chip ${JSON.stringify(pic && pic.bodies)} ${JSON.stringify(pic && pic.chips)}`);
  try {
    /* 1 · production page: before the link, the two real lines are read by their words; a SKU no words explain waits in Review (what Paul saw) */
    const { page, context } = await session(false);
    const before = {}; for (const rid of [A, B2, C, D]) before[rid] = await rowState(page, rid);
    for (const rid of [A, B2]) ok(before[rid].source === 'skus' && before[rid].members.join() === `L:${BALL},R:${RACKET}`, `before the link, ${rid} is read from the words of its SKU (${before[rid].source})`);
    ok(before[C].source === null && before[C].problems.some(p => /^unmatchedSku/.test(p)) && before[C].designSku === SET7, 'the same listing under a SKU no words explain waits as an unknown SKU (as Paul\'s rows did): ' + JSON.stringify(before[C].problems));
    ok(before[D].source === null && before[D].problems.some(p => /^needsMapping:.*no single master design reads as “Zebra”/.test(p)), 'a similar title on another listing, with a design the master lacks, asks in one plain line: ' + JSON.stringify(before[D].problems));
    await page.evaluate(() => { Review.syncOrderItems(); CN.setMode('review'); Review.render(); });
    const reviewBefore = await page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(r => r.dataset.rid));
    ok(reviewBefore.includes(C) && reviewBefore.includes(D) && !reviewBefore.includes(A) && !reviewBefore.includes(B2), 'Review holds the line nobody can read and the Zebra line: ' + reviewBefore);
    if (shots) { await page.waitForTimeout(500); await page.screenshot({ path: path.join(shots, 'TENNISLINK-review-before.png') }); }
    /* 2 · the saved link, once, through the page's own call (no sandbox flag: production's store), for the REAL SKU and for the SKU no words explain */
    const put = (listingId, fromSku) => page.evaluate(async ({ listingId, fromSku, BALL, RACKET }) => CN.api('charmNestLibrary', { op: 'aliasPut', listingId, fromSku, pair: { L: BALL, R: RACKET }, by: 'TENNISLINK', title: 'Mismatched Tennis Ball and Raquet Huggie Hoops Charm' }), { listingId, fromSku, BALL, RACKET });
    const w1 = await put(LID, RAW), w2 = await put(LID2, SET7);
    ok(w1 && w1.ok === true && w2 && w2.ok === true, 'the real call saved the pair for the real listing and SKU and for the other listing: ' + JSON.stringify([w1, w2]));
    const calls = st.calls.filter(c => c.name === 'charmNestLibrary' && c.body.op === 'aliasPut');
    ok(calls.length === 2 && calls.every(c => !c.body.sandbox), 'both are production calls (no sandbox flag)');
    ok(![...st.docs.keys()].some(k => k.startsWith('Sandbox_Charm_Sku_Aliases/')), 'nothing was written to a sandbox copy');
    const rec = st.doc('Charm_Sku_Aliases', LID); ok(rec && Object.keys(rec.pairBySku).join('|') === RAW, 'production holds ONE record for the listing, under its SKU: ' + JSON.stringify(rec && Object.keys(rec.pairBySku)));
    const stored = JSON.stringify(rec.pairBySku[RAW]); ok(/"L":"TENNIS BALL \(HUGGIE\)"/.test(stored) && /"R":"TENNIS RACKET \(HUGGIE\)"/.test(stored), 'the stored pair for the real SKU: ' + stored);
    await page.evaluate(async () => { await Orders.loadMaps(true); Orders.interpretAll(); Review.syncOrderItems(); Orders.render(); });
    const after = {}; for (const rid of [A, B2, C, D]) after[rid] = await rowState(page, rid);
    for (const rid of [A, B2, C]) {
      ok(after[rid].source === 'alias' && after[rid].members.join() === `L:${BALL},R:${RACKET}`, `${rid}: now read from the saved link: Left ${BALL}, Right ${RACKET}`);
      ok(after[rid].problems.length === 0, `${rid}: no question: ${JSON.stringify(after[rid].problems)}`); ok(after[rid].pieces === 2 && after[rid].sides === 'LR', `${rid}: 2 pieces, a Left and a Right`);
      ok(after[rid].designSku === BALL, `${rid}: the line's design is the Left's`);
    }
    ok(after[D].source === null && after[D].problems.some(p => /Zebra/.test(p)), 'the similar title on another listing still asks (the link is this listing\'s and this SKU\'s only)');
    /* 3 · Review: the line the link resolves left Review; the Zebra line stays */
    await page.evaluate(() => { CN.setMode('review'); Review.render(); });
    const reviewAfter = await page.evaluate(() => [...document.querySelectorAll('#rvList .reviewListRow')].map(r => r.dataset.rid));
    ok(!reviewAfter.includes(C) && reviewAfter.includes(D), 'Review: the linked line is gone, the Zebra line stays: ' + reviewAfter);
    /* 4 · Orders tab: one row each, BOTH designs side by side, never "Unavailable · Retry" */
    await page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    const vecs = await vectorsDrawn(page, '#ordItems [data-vector]');
    ok(vecs.length === 4, 'four rows: ' + vecs.length);
    for (const v of vecs.filter(x => [A, B2, C].includes(x.rid))) { ok(!/Unavailable|No vector/.test(v.text), `${v.rid}: "Vector design" reads neither "Unavailable · Retry" nor "No vector available" (${v.text || 'a picture'})`); two(v.pic, `Orders row ${v.rid}`); }
    // a line nobody can read yet (its SKU names two designs, "Tennis Ball/Zebra", with a slash no master file can hold) says so; it is not a failed load to retry
    { const z = vecs.find(x => x.rid === D); ok(z && /No vector available/.test(z.text) && !/Unavailable/.test(z.text), 'the Zebra line, still unread, says "No vector available", not "Unavailable · Retry": ' + (z && z.text)); }
    ok(!st.calls.some(c => c.name === 'charmNestLibrary' && c.body.op === 'masterGet' && /\//.test(String(c.body.sku || ''))), 'the page never asks the library for a SKU with a slash in it (the library answers those with a 400)');
    const captions = await page.evaluate(() => [...document.querySelectorAll('#ordItems > [data-rid]')].map(nd => [nd.dataset.rid, (nd.querySelector('.comparePair figure:first-child figcaption') || {}).textContent, ((nd.querySelector('.rowFacts') || {}).textContent || '').replace(/\s+/g, ' ')]));
    for (const [rid, cap, facts] of captions.filter(x => [A, B2, C].includes(x[0]))) ok(/Vector design · Left \+ Right/.test(cap) && /Qty 1 · Left \+ Right/.test(facts), `${rid}: caption and facts say Left + Right: ${cap} | ${facts}`);
    ok(captions.filter(x => [A, B2].includes(x[0])).length === 2, 'one row per order: no duplicate row for either design');
    /* 5 · the two ears, one by one (the Engrave tab, the piece dots, the order window's pieces): each ear is its OWN design, never one design twice */
    const ears = await page.evaluate(async rid => { const r = B.orders.rows.find(x => x.order.receiptId === rid), get = async side => { const s = await PieceMedia.vectorThumb(r, { side, mirror: side === 'R', px: 160 }); return s ? await window.__ink(s) : null; }; const L = await get('L'), R = await get('R'); return { L, R, keyL: PieceMedia.vectorKey(r, { side: 'L' }), keyR: PieceMedia.vectorKey(r, { side: 'R', mirror: true }) }; }, A);
    ok(ears.L && ears.R && ears.L.bodies.length === 1 && ears.R.bodies.length === 1, 'an ear asked for by side is one body: ' + JSON.stringify([ears.L && ears.L.bodies, ears.R && ears.R.bodies]));
    ok(ears.keyL !== ears.keyR && /TENNIS BALL \(HUGGIE\)/.test(ears.keyL) && /TENNIS RACKET \(HUGGIE\)/.test(ears.keyR), 'the Left ear is the Tennis Ball and the Right ear the Tennis Racket (each its own picture key): ' + ears.keyL + ' / ' + ears.keyR);
    ok(ears.L.h !== ears.R.h || ears.L.w !== ears.R.w, 'the two ears are two different drawings');
    /* 6 · the order window: the pair picture, a Left row and a Right row */
    await page.evaluate(rid => { OrderWin.open(B.orders.rows.find(r => r.order.receiptId === rid).key, { view: 'info' }); }, A);
    await page.waitForFunction(() => { const v = document.getElementById('owVector'); return v && v.querySelector('img') && v.getAttribute('aria-busy') !== 'true'; }, null, { timeout: 30000 });
    const win = await page.evaluate(async () => { const im = document.querySelector('#owVector img'); return { pic: await window.__ink(im.src), rows: [...document.querySelectorAll('#owPcSum .owPcRow')].map(r => ({ side: r.dataset.side || '', text: r.textContent.replace(/\s+/g, ' ').trim().slice(0, 140) })) }; });
    two(win.pic, 'the order window header');
    ok(win.rows.filter(r => r.side).map(r => r.side).join() === 'L,R', 'its pieces list has the Left row and the Right row: ' + JSON.stringify(win.rows));
    await page.evaluate(() => OrderWin.close()); await page.waitForTimeout(200);
    if (shots) { await page.evaluate(() => { CN.setMode('orders'); Orders.render(); window.scrollTo(0, 0); }); await vectorsDrawn(page, '#ordItems [data-vector]'); await page.screenshot({ path: path.join(shots, 'TENNISLINK-orders-after.png') }); await page.evaluate(() => { CN.setMode('review'); Review.render(); }); await page.waitForTimeout(600); await page.screenshot({ path: path.join(shots, 'TENNISLINK-review-after.png') }); }
    await context.close();

    /* 7 · a SANDBOX page reads production's link (the sandbox keeps its own answers apart, and a sandbox wipe never touches production's) */
    const sb = await session(true);
    const sbA = await rowState(sb.page, A); ok(sbA.source === 'alias' && sbA.members.join() === `L:${BALL},R:${RACKET}`, 'a sandbox page read the pair production holds: ' + JSON.stringify(sbA));
    const sbC = await rowState(sb.page, C); ok(sbC.source === 'alias' && sbC.problems.length === 0, 'and the second SKU, with no question');
    const sbD = await rowState(sb.page, D); ok(sbD.source === null && sbD.problems.length > 0, 'and still asks about the similar title on another listing');
    await sb.page.evaluate(() => { CN.setMode('orders'); Orders.render(); });
    const sv = await vectorsDrawn(sb.page, '#ordItems [data-vector]');
    for (const v of sv.filter(x => [A, B2, C].includes(x.rid))) { ok(!/Unavailable|No vector/.test(v.text), `sandbox ${v.rid}: no "Unavailable · Retry"`); two(v.pic, `sandbox Orders row ${v.rid}`); }
    ok(![...st.docs.keys()].some(k => k.startsWith('Sandbox_Charm_Sku_Aliases/')), 'the sandbox page wrote nothing of its own');
    await sb.context.close();
    ok(!errors.length, 'no page errors: ' + errors.join(' | '));
  } finally { await browser.close(); srv.close(); }
  console.log(`  ✓ C · the page: both real designs drawn side by side in the Orders row and the order window, each ear its own design, Review clears, a sandbox page reads production's link (${n} checks)`);
})().catch(e => { console.error(e); process.exit(1); });
