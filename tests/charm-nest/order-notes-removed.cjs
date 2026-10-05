// The Order notes box is gone from every order detail modal (Paul, 5 Oct 2026: "Image #1 - Remove this UI from all order detail
// modals UI"): the textarea "ORDER NOTES - for whoever opens this order next . saved to the order by itself, every station and
// sorter sees them" with its "Leave a note on this order for the next person who opens it..." placeholder.
// The sorter's order window (charm-nest-1.html + charm-nest-bridge.js, headless Chromium against the fake site bridge-server.cjs,
// every request off the loopback aborted) at 1440, 900 and 390 px:
//   - no box, label, caption or placeholder anywhere in the window; the space closes up (the customer's words sit one normal
//     field gap above the details grid) and the window is no wider or taller than before;
//   - opening, repainting, Previous/Next, every tab, Esc and the x all work and nothing throws (no page error, no console error),
//     for an order of two pieces, another order, and an order the sorter holds no piece of;
//   - the window never writes a staff note and never asks the record for one; a note already in the record is left as it is;
//   - a build that still has the box (the markup back, or only its words) is caught by the very same checks (mutants).
// The Design Station's order pop-up (design.html, design-1.html, and the old assets/ copies) no longer has its Staff note box
// either: read from the pages' own source and, for design.html and design-1.html, in the pop-up opened on the fake station.
// The stored notes (Firestore "Staff Note", firebaseOrders staffNote / ?staffNotes / ?staffNotesFor, the station's notes.set)
// are untouched: only the boxes and their reading and writing in the pop-ups are gone.
//   node tests/charm-nest/order-notes-removed.cjs [playwright-core dir]   (CN_SHOTS=dir saves screenshots of the fixture)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 9, 17) / 1000);
const A = { rid: '4176208841', tids: ['41762088411', '41762088412'], sku: 'TINY_TAG' }, B2 = { rid: '4176200172', tids: ['41762001721'], sku: 'LEAF_CHARM' };
const GONE = '4170000999';   // an order the sorter holds no piece of
const OLD = 'Old note left before the box was removed';
const order = (o, buyer, note) => ({ receiptId: o.rid, orderNumber: o.rid, createTs: SHIP - 5 * DAY, updateTs: SHIP - 5 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: note || '', messages: [],
  lines: o.tids.map((tid, i) => ({ transactionId: tid, listingId: '1800000' + tid.slice(-3), sku: o.sku, title: o.sku.replace(/_/g, ' ') + ' necklace ' + (i + 1), quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: 'GF 14/20', personalization: ['Initial: H'], buyerMessage: '' })) });

const failures = [];
const check = (ok, msg) => { if (ok) console.log('  ok  ' + msg); else { failures.push(msg); console.log('  FAIL ' + msg); } };

const receiptOf = (rid, txs, day) => ({ receipt_id: rid, order_number: rid, name: 'Buyer ' + rid, country_iso: 'US', city: 'Austin', message_from_buyer: 'please engrave ANNA on the back', update_timestamp: day, create_timestamp: day - 3600, status: 'Paid', is_shipped: false, transactions: txs });
const txOf = (rid, i, sku, day) => ({ transaction_id: Number(`${rid}${i}`), listing_id: 1718000 + i, receipt_id: rid, sku, title: `${sku} charm`, quantity: 1, expected_ship_date: day + 86400 });

// the Design Station's order pop-up, opened the way a person opens it: Refresh, the order's row, its item
async function stationPart(chromium, shots) {
  const day = Math.floor(Date.now() / 1000), RID = '3521000001', STORED = 'A note left before the box was removed';
  for (const pageName of ['design-1.html', 'design.html']) {
    const srv = await start({ receipts: [receiptOf(3521000001, [txOf(3521000001, 1, 'BR-TST-01', day)], day), receiptOf(3521000002, [txOf(3521000002, 1, 'BR-TST-02', day)], day)] });
    srv.st.put('Brites_Orders', RID, { 'Staff Note': STORED });
    const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
    try {
      const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
      const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
      await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
      await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
      await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
      await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href) && !/gstatic\.com\/firebasejs|qrcodejs|fonts\./.test(u.href), r => r.abort());
      await ctx.addInitScript(({ station }) => {
        if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
        window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
      }, { station: srv.stationOrigin });
      const page = await ctx.newPage(), errors = [], writes = [], reads = [];
      page.on('pageerror', e => errors.push('page error: ' + e.message));
      page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errors.push('console error: ' + m.text().slice(0, 200)); });
      page.on('request', rq => { const u = rq.url(); if (rq.method() === 'POST' && /firebaseOrders/.test(u) && /staffNote/.test(rq.postData() || '')) writes.push(rq.postData()); if (rq.method() === 'GET' && /firebaseOrders\?(?:[^#]*&)?orderId=/.test(u)) reads.push(u); });
      await page.goto(`${srv.stationOrigin}/${pageName}`, { waitUntil: 'load' });
      await page.waitForFunction(() => document.getElementById('listingModal') && window.DesignStation, null, { timeout: 30000 });
      await page.waitForTimeout(5000);   // (the station signs in and wires its buttons first)
      for (let i = 0; i < 6 && !(await page.evaluate(rid => !!document.querySelector(`.orderRow[data-receipt='${rid}']`), RID)); i++) {
        await page.evaluate(() => { const b = [...document.querySelectorAll('#updateOrderListBtn,#stripRefreshBtn,#emptyLoadBtn')].find(x => x.getBoundingClientRect().width > 0); if (b) b.click(); });
        await page.waitForTimeout(6000);
      }
      await page.waitForFunction(rid => !!document.querySelector(`.orderRow[data-receipt='${rid}']`), RID, { timeout: 20000 });
      await page.waitForTimeout(2500);
      const flagged = await page.evaluate(rid => { const r = document.querySelector(`.orderRow[data-receipt='${rid}']`); return !!r && r.classList.contains('hasNote'); }, RID);
      await page.click(`.orderRow[data-receipt='${RID}']`);
      await page.waitForFunction(() => document.querySelectorAll('.tile').length > 0, null, { timeout: 20000 });
      await page.waitForTimeout(1200);
      await page.evaluate(() => { const t = document.querySelector('.tile'); (t.querySelector('.tileBody, .body, img') || t).click(); });
      await page.waitForFunction(() => document.getElementById('listingModal').open, null, { timeout: 15000 });
      await page.waitForTimeout(1200);
      const probe = () => page.evaluate(() => {
        const d = document.getElementById('listingModal'), n = document.getElementById('modalListingNotes'), m = document.getElementById('modalListingDetails'), r = x => x.getBoundingClientRect();
        return { open: d.open, box: !!document.getElementById('modalStaffNote'), words: /staff note|visible to every station|add a note for this order/i.test(d.innerText + ' ' + [...d.querySelectorAll('[placeholder]')].map(e => e.placeholder).join(' ')),
          areas: d.querySelectorAll('.lmGrid textarea').length, notes: !!n, details: !!m && m.children.length >= 4,
          gap: Math.round(r(m).top - r(n).bottom), fits: r(d.querySelector('.dlg')).bottom <= innerHeight + 1 && r(d.querySelector('.dlg')).right <= innerWidth + 1 };
      });
      const bad = (p, label) => {
        const f = [];
        if (!p.open) f.push(`${label}: the pop-up is not open`);
        if (p.box) f.push(`${label}: #modalStaffNote is in the pop-up`);
        if (p.words) f.push(`${label}: the words of the staff note box are in the pop-up`);
        if (p.areas !== 1) f.push(`${label}: the pop-up has ${p.areas} text boxes where the buyer's message is the only one`);
        if (!p.notes || !p.details) f.push(`${label}: the rest of the pop-up is not all there`);
        if (p.gap < 0 || p.gap > 16) f.push(`${label}: the space did not close up (${p.gap} px between the buyer's message and the details)`);
        if (!p.fits) f.push(`${label}: the pop-up does not fit the screen`);
        return f;
      };
      const p = await probe(), f = bad(p, pageName);
      if (shots) await page.screenshot({ path: path.join(shots, `station-${pageName.replace('.html', '')}-order-popup-no-note.png`) });
      check(f.length === 0, `${pageName}: the order pop-up has no staff note box; the buyer's message and the details follow each other (gap ${p.gap} px); it fits 1440 x 900` + (f.length ? ' · ' + f.join(' · ') : ''));
      check(flagged, `${pageName}: the order's row still carries its purple note flag (the stored note is still known to the list)`);
      // mutant: the old box put back is caught by the same checks
      await page.evaluate(() => { document.getElementById('modalListingNotes').insertAdjacentHTML('afterend', '<label class="fLabel">Staff note <span>— saved automatically, visible to every station</span></label><textarea class="inp" id="modalStaffNote" placeholder="Add a note for this order…"></textarea>'); });
      const mut = bad(await probe(), 'mutant');
      check(mut.length >= 3, `${pageName}: with the old box put back the same checks fail (${mut.length})`);
      await page.evaluate(() => { document.getElementById('modalStaffNote').previousElementSibling.remove(); document.getElementById('modalStaffNote').remove(); });
      check(bad(await probe(), 'restored').length === 0, `${pageName}: and pass again without it`);
      await page.evaluate(() => document.getElementById('lmClose').click());
      await page.waitForFunction(() => !document.getElementById('listingModal').open, null, { timeout: 5000 });
      await page.waitForTimeout(1000);
      check(writes.length === 0 && reads.length === 0, `${pageName}: the pop-up wrote no staff note and read none for a box` + (writes.length || reads.length ? ': ' + [...writes, ...reads].join(' | ') : ''));
      check((srv.st.doc('Brites_Orders', RID) || {})['Staff Note'] === STORED, `${pageName}: the note stored on the order is untouched`);
      check(errors.length === 0, `${pageName}: no page error and no console error` + (errors.length ? ': ' + errors.join(' | ') : ''));
    } finally { await browser.close(); srv.close(); }
  }
}

// the station's notes.set stays, framed by the sorter as in real use: the sorter's own engraving and material decisions add their
// line to the stored note through it, with no box anywhere to show it
async function linkPart(chromium) {
  const day = Math.floor(Date.now() / 1000), RID = '3521000001';
  const srv = await start({ receipts: [receiptOf(3521000001, [txOf(3521000001, 1, 'BR-TST-01', day)], day)] });
  srv.st.put('Brites_Orders', RID, { 'Staff Note': 'before' });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    const fbStub = "const nope = () => { throw new Error('firebase stub'); }; export const initializeApp = nope, getApp = nope, getStorage = nope, ref = nope, uploadBytesResumable = nope, getDownloadURL = nope, getAuth = nope, signInAnonymously = nope;";
    await ctx.route(/gstatic\.com\/firebasejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: /-compat\.js/.test(r.request().url()) ? '' : fbStub }));
    await ctx.route(/qrcodejs/, r => r.fulfill({ status: 200, contentType: 'text/javascript', headers: { 'Cross-Origin-Resource-Policy': 'cross-origin' }, body: fs.readFileSync(path.join(root, 'lib/qrcode.min.js')) }));
    await ctx.route(/fonts\.googleapis|fonts\.gstatic/, r => r.fulfill({ status: 200, contentType: 'text/css', body: '' }));
    await ctx.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href) && !/gstatic\.com\/firebasejs|qrcodejs|fonts\./.test(u.href), r => r.abort());
    await ctx.addInitScript(({ station, sorter }) => {
      if (location.origin === station) { localStorage.setItem('access_token', 'tok'); localStorage.setItem('refresh_token', 'ref'); localStorage.setItem('token_expires_at', String(Math.floor(Date.now() / 1000) + 7200)); localStorage.setItem('employee_name', 'Tester'); }
      if (location.origin === sorter) { localStorage.setItem('cn.employee', 'Tester'); }
      window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {};
    }, { station: srv.stationOrigin, sorter: srv.sorterOrigin });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push('page error: ' + e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.CN && window.CN.S && window.DesignLink && CN.S.cloud.ok !== null, null, { timeout: 60000 });
    await page.evaluate(station => { const s = CN.S.settings; s.dsOrigin = station; s.notify = 'off'; s.sound = 'off'; s.runMode = 'manual'; CN.saveSettings && CN.saveSettings(); }, srv.stationOrigin);
    await page.evaluate(() => CN.setMode('design'));
    await page.evaluate(() => DesignLink.ensure());
    const hello = await page.evaluate(() => DesignLink.state());
    check(hello && hello.counts && Array.isArray(hello.selection), 'the station answers the sorter as in real use (framed, handshake done)');
    const r = await page.evaluate(rid => DesignLink.call('notes.set', { receiptId: rid, text: 'before\nEngrave (Tester): ANNA' }), RID);
    check(r && r.receiptId === RID && r.hasNote === true, 'notes.set still answers: ' + JSON.stringify(r));
    check((srv.st.doc('Brites_Orders', RID) || {})['Staff Note'] === 'before\nEngrave (Tester): ANNA', 'and stores the note on the order, with no box anywhere to show it');
    check(errors.length === 0, 'no page error' + (errors.length ? ': ' + errors.join(' | ') : ''));
  } finally { await browser.close(); srv.close(); }
}

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  - no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || '';
  if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  srv.st.put('Brites_Orders', A.rid, { 'Staff Note': OLD });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    const outside = [];
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => {
      if (/fonts\.googleapis|fonts\.gstatic/.test(r.request().url())) return r.fulfill({ status: 200, contentType: 'text/css', body: '' });
      outside.push(r.request().url()); return r.abort();
    });
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ ok: true, n: 0, engagements: [], active: null, conversation: null }) }));
    await context.addInitScript(() => { try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-notes-test')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {} window.prompt = () => 'Test Operator'; });
    const page = await context.newPage(), errors = [], noteWrites = [], noteReads = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => errors.push('page error: ' + String(e.stack || e.message).split('\n').slice(0, 3).join(' | ')));
    // (an aborted request off the loopback is logged by the browser as a failed resource: that is the test's own wall, not the page)
    page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource/.test(m.text())) errors.push('console error: ' + m.text().slice(0, 240)); });
    page.on('request', rq => {
      const u = rq.url();
      if (rq.method() === 'POST' && /firebaseOrders/.test(u) && /staffNote/.test(rq.postData() || '')) noteWrites.push(rq.postData());
      if (rq.method() === 'GET' && /firebaseOrders\?(?:[^#]*&)?orderId=/.test(u)) noteReads.push(u);
    });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async orders => {
      await Orders.loadMaps(true);
      for (const order of orders) for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll(); CN.setMode('orders'); Orders.render();
    }, [order(A, 'Hannah Whitford', OLD), order(B2, 'Ava Patel')]);
    const a1 = `${A.rid}_${A.tids[0]}`, a2 = `${A.rid}_${A.tids[1]}`, b1 = `${B2.rid}_${B2.tids[0]}`;
    const closed = () => page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 4000 });
    const settled = () => page.waitForFunction(() => { const d = document.getElementById('orderWin'); return d.open && !d.getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity); }, null, { timeout: 5000 });
    const click = sel => page.evaluate(s => { const b = document.querySelector(s); if (!b) throw new Error('no ' + s); b.click(); }, sel);

    // what the order window shows, read as drawn
    const probe = () => page.evaluate(() => {
      const d = document.getElementById('orderWin'), r = d.getBoundingClientRect();
      const said = document.getElementById('owNotes'), fld = said && said.closest('.owField'), meta = document.getElementById('owMeta');
      return { open: d.open,
        box: !!document.getElementById('owNote'), label: !!d.querySelector('label[for=owNote]'), field: !!d.querySelector('.owNoteField'),
        areas: d.querySelectorAll('.owInfoMain textarea').length,
        words: /order notes|opens this order next|every station and sorter sees|note on this order|next person who opens/i.test(d.textContent + ' ' + [...d.querySelectorAll('[placeholder],[title],[aria-label]')].map(e => (e.getAttribute('placeholder') || '') + ' ' + (e.title || '') + ' ' + (e.getAttribute('aria-label') || '')).join(' ')),
        old: d.textContent.includes('Old note left before'),
        said: !!said && !!fld, details: !!meta && meta.children.length > 0, composer: !!document.getElementById('owInput'),
        gap: fld && meta ? Math.round(meta.getBoundingClientRect().top - fld.getBoundingClientRect().bottom) : null,
        w: Math.round(r.width), h: Math.round(r.height), iw: innerWidth, ih: innerHeight, sw: d.scrollWidth, cw: d.clientWidth };
    });
    // the checks a window must pass; each failure named (the mutants below must make this list non-empty)
    const bad = (p, label) => {
      const f = [];
      if (!p.open) f.push(`${label}: the order window is not open`);
      if (p.box) f.push(`${label}: #owNote is in the window`);
      if (p.label) f.push(`${label}: a label for the notes box is in the window`);
      if (p.field) f.push(`${label}: the notes field wrapper is in the window`);
      if (p.areas) f.push(`${label}: a text box sits in the Overview (${p.areas})`);
      if (p.words) f.push(`${label}: the words of the Order notes box are in the window`);
      if (p.old) f.push(`${label}: a note stored in the record is shown`);
      if (!p.said || !p.details || !p.composer) f.push(`${label}: the rest of the window is not all there (customer words ${p.said}, details ${p.details}, message box ${p.composer})`);
      if (p.gap == null || p.gap < 0 || p.gap > 14) f.push(`${label}: the space did not close up: ${p.gap} px between what the customer wrote and the details`);
      return f;
    };
    const VIEWS = [[1440, 900], [900, 700], [390, 844]], base = {};

    // 1 · each size: open an order of two pieces; repaint, walk, every tab, close and open again
    for (const [w, h] of VIEWS) {
      await page.setViewportSize({ width: w, height: h });
      await page.evaluate(() => { if (OrderWin.isOpen()) OrderWin.close(); });
      await page.waitForTimeout(250);
      await page.evaluate(k => OrderWin.open(k), a1);
      await page.waitForFunction(() => OrderWin.isOpen() && document.getElementById('owLoading').hidden);
      await settled(); await page.waitForTimeout(500);
      let p = await probe(); base[w] = p;
      console.log(`  .  ${w} px: window ${p.w} x ${p.h} in ${p.iw} x ${p.ih}, content ${p.sw} wide in ${p.cw}, gap ${p.gap}`);
      let f = bad(p, `${w}px, first piece`);
      if (w >= 900) { if (p.w > p.iw || p.h > p.ih) f.push(`${w}px: the window is bigger than the screen (${p.w}x${p.h} in ${p.iw}x${p.ih})`); }
      if (shots) {   // (focus on the Previous button scrolls a window that is wider than the screen: the picture starts from the left edge)
        await page.evaluate(() => { const d = document.getElementById('orderWin'); for (const n of [d, ...d.querySelectorAll('*')]) if (n.scrollLeft) n.scrollLeft = 0; window.scrollTo(0, 0); });
        await page.waitForTimeout(200); await page.screenshot({ path: path.join(shots, `order-window-no-notes-${w}.png`) });
      }
      await page.evaluate(() => { OrderWin.paint(); OrderWin.paint(); });
      await page.waitForTimeout(300);
      f.push(...bad(await probe(), `${w}px, repainted`));
      // Next and Previous, then every tab and back to the Overview
      await click('#owNext'); await page.waitForFunction(k => OrderWin.key() === k, a2); await page.waitForTimeout(600);
      f.push(...bad(await probe(), `${w}px, second piece`));
      await click('#owPrev'); await page.waitForFunction(k => OrderWin.key() === k, a1); await page.waitForTimeout(600);
      // (the Sheet tab is greyed for a piece that has no sheet yet, as it should be: it is opened through the window's own call)
      for (const v of ['timeline', 'sheet', 'info']) {
        if (v === 'sheet') await page.evaluate(() => OrderWin.setView('sheet')); else await click(`.owTabsV [data-ow-view="${v}"]`);
        await page.waitForFunction(v2 => OrderWin.view() === v2, v, { timeout: 8000 }).catch(async () => { throw new Error(`${w}px: the ${v} view did not open: ` + JSON.stringify(await page.evaluate(() => ({ view: OrderWin.view(), busy: !!(window.Seal && Seal.busy && Seal.busy()), tabs: [...document.querySelectorAll('.owTabsV [data-ow-view]')].map(b => b.dataset.owView + ':' + b.getAttribute('aria-selected')) })))); });
        await page.waitForTimeout(450);
      }
      f.push(...bad(await probe(), `${w}px, back on the Overview`));
      // the other order, by Next; then closed by Esc and by the x, and opened again
      await page.evaluate(k => OrderWin.open(k), b1); await page.waitForTimeout(500);
      f.push(...bad(await probe(), `${w}px, another order`));
      await page.evaluate(() => document.getElementById('orderWin').dispatchEvent(new Event('cancel', { cancelable: true })));
      await closed();
      await page.evaluate(k => OrderWin.open(k), a1); await page.waitForFunction(() => OrderWin.isOpen()); await page.waitForTimeout(500);
      await click('#owClose'); await closed();
      check(f.length === 0, `${w} x ${h}: no notes box; the space closes up (gap ${p.gap} px); open, repaint, Previous/Next, every tab, Esc and the x all work` + (f.length ? ' · ' + f.join(' · ') : ''));
    }
    // (the Overview's content width was the same with the box and without it, 1440 / 927 / 922 px at 1440 / 900 / 390 px when this was
    //  measured on the unchanged window: removing the box does not widen it. Other work on the window moves those widths, so they are only reported)
    console.log(`  .  content width at 1440 / 900 / 390 px: ${VIEWS.map(([w]) => base[w].sw).join(' / ')} (reported, not judged)`);

    // 2 · an order the sorter holds no piece of: read from the records, "no piece", no box, nothing thrown
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.evaluate(rid => { OrderWin.openOrder(rid, {}); }, GONE);
    await page.waitForFunction(() => OrderWin.isOpen() && document.getElementById('owLoading').hidden, null, { timeout: 20000 });
    await page.waitForTimeout(700);
    const gone = await page.evaluate(() => { const d = document.getElementById('orderWin'); return { box: !!document.getElementById('owNote'), words: /order notes|opens this order next|note on this order/i.test(d.textContent), said: document.getElementById('owNotes').textContent }; });
    check(!gone.box && !gone.words, `an order with no piece in the sorter: no notes box and none of its words ("${gone.said.slice(0, 60)}...")`);
    await click('#owClose'); await closed();

    // 3 · nothing was written to a note, nothing asked for one, the stored note is as it was, no Etsy call, nothing thrown
    await page.waitForTimeout(1000);
    check(noteWrites.length === 0, 'the order window wrote no staff note to the record' + (noteWrites.length ? ': ' + noteWrites.join(' | ') : ''));
    check(noteReads.length === 0, 'and read no staff note for a box' + (noteReads.length ? ': ' + noteReads.join(' | ') : ''));
    check((srv.st.doc('Brites_Orders', A.rid) || {})['Staff Note'] === OLD, 'the note already stored on the order is still stored, as it was');
    check(outside.filter(u => /etsy/i.test(u)).length === 0, 'no Etsy call');
    check(errors.length === 0, 'no page error and no console error' + (errors.length ? ': ' + errors.join(' | ') : ''));

    // 4 · mutants: a build that still has the box must fail the very same checks
    await page.evaluate(k => OrderWin.open(k), a1);
    await page.waitForFunction(() => OrderWin.isOpen()); await settled(); await page.waitForTimeout(400);
    check(bad(await probe(), 'control').length === 0, 'control: the checks pass on the real window');
    await page.evaluate(() => {   // mutant 1: the old markup, back where it was
      const meta = document.getElementById('owMeta');
      meta.insertAdjacentHTML('beforebegin', '<div class="owField owNoteField"><label class="fLabel" for="owNote">Order notes <span class="soft">— for whoever opens this order next · saved to the order by itself, every station and sorter sees them</span></label><textarea class="owIn" id="owNote" rows="3" placeholder="Leave a note on this order for the next person who opens it…"></textarea></div>');
    });
    const m1 = bad(await probe(), 'mutant 1');
    check(m1.length >= 4, `mutant 1, the box put back: caught by ${m1.length} checks (${m1.map(x => x.replace(/^mutant 1: /, '').slice(0, 40)).join('; ')})`);
    await page.evaluate(() => { document.querySelector('.owNoteField').remove(); });
    check(bad(await probe(), 'restored').length === 0, 'control again: the window without the mutant passes');
    await page.evaluate(() => {   // mutant 2: only its words (a caption), the space still closed up
      const said = document.getElementById('owNotes'); said.insertAdjacentHTML('afterend', '<span class="soft">Order notes — for whoever opens this order next</span>');
    });
    const m2 = bad(await probe(), 'mutant 2');
    check(m2.some(x => /words of the Order notes box/.test(x)), 'mutant 2, only its caption: caught by the words check');
    await page.evaluate(() => { const s = document.querySelector('#owNotes + .soft'); if (s) s.remove(); });
    await page.evaluate(() => {   // mutant 3: a textarea anywhere in the Overview, however named
      const t = document.createElement('textarea'); t.id = 'someNote'; document.getElementById('owMeta').insertAdjacentElement('beforebegin', t);
    });
    const m3 = bad(await probe(), 'mutant 3');
    check(m3.some(x => /text box sits in the Overview/.test(x)) && m3.some(x => /did not close up/.test(x)), 'mutant 3, any text box in the Overview: caught by the text box check and the space check');
    await click('#owClose'); await closed();
    check(errors.length === 0, 'no page error from the mutant runs either');
  } finally { await browser.close(); srv.close(); }

  // 5 · the source: no box, no markup, no reading or writing code for it, in the sorter or in any station page
  const read = f => fs.readFileSync(path.join(root, f), 'utf8');
  for (const f of ['charm-nest-1.html', 'charm-nest-bridge.js']) {
    const t = read(f);
    check(!/\bowNote\b|owNoteField|Leave a note on this order|Order notes\b|saveNote|paintNote|refreshNote|earlyNote/.test(t), `${f}: no notes box, no markup, no CSS and no note reading or writing code for it`);
  }
  for (const f of ['design.html', 'design-1.html', 'assets/design.html', 'assets/design-1.html']) {
    const t = read(f);
    check(!/modalStaffNote|Add a note for this order|visible to every station|placeholder="Staff Note"/.test(t), `${f}: no staff note box in the order pop-up, no markup, CSS or code for it`);
  }
  // and what stays: the stored notes and the ops and flags that serve them (read here from the source)
  const fo = read('netlify/functions/firebaseOrders.js'), d1 = read('design-1.html'), d0 = read('design.html');
  check(/staffNote\s+!==\s+undefined/.test(fo) && /staffNotesFor/.test(fo) && /staffNotes\s*===\s*"1"/.test(fo), 'firebaseOrders still stores a staffNote and answers ?staffNotes and ?staffNotesFor');
  check(/async "notes\.set"\(a\)/.test(d1) && /markStaffNoteOrders/.test(d1) && /markStaffNoteOrders/.test(d0), 'the station keeps notes.set and the purple note flag on its rows');

  // 6 · the Design Station's order pop-up (design.html and design-1.html) on the fake station: no staff note box, the rest as it was
  await stationPart(chromium, shots);

  // 7 · what stays: the station's notes.set, framed by the sorter
  await linkPart(chromium);

  if (failures.length) { console.log('\nFAILED: ' + failures.length); process.exit(1); }
  console.log('order-notes-removed OK');
})().catch(e => { console.error(e); process.exit(1); });
