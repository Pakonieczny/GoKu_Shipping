// SEALWIN (Paul 20 Oct 2026: "All of these seals must be visible in all places where a given button is relevant or information about a given button's status is relevant to the user. This includes the
// Detailed Order view modal."): the order window shows every seal the order has collected, beside its button when that button is shown in the window, else in the compact Seals row (#owSealRow).
//   what is proved (the REAL page over the offline fake site; every name is made up; the cancel and sheet seals are seeded as records in the release seal record's shape, on the order's Review document)
//     1  node: the painter is in the window's paint and listens for the seal events; the row is in the page; the cache token is bumped (-sw1)
//     2  an order with print + release + cancel + sheet seals and a Hold Order mark (ORDER HELD): the window shows all five in the Seals row, oldest first, each in its kind's ink, who and when on hover
//     3  a kind nobody listed (a seeded "engraveSent" record with its own label) is drawn too, with a neutral ink and its stored label
//     4  a seal whose button is shown in the window sits beside that button, not in the row
//     5  LIVE: a seal added while the window is open appears without a reload (the seal event, and a plain repaint), and its Undo takes it away
//     6  a cancelled order's window (all its lines gone, opened by its receipt id as Orders › Cancelled does) shows its seals
//   PW_DIR=<playwright node_modules> node tests/charm-nest/seals-window.cjs [PW_DIR]
'use strict';
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
if (process.argv[2] && !process.env.PW_DIR) process.env.PW_DIR = process.argv[2];
const read = f => fs.readFileSync(path.join(root, f), 'utf8');
const R = require('./_engraveRealWorld.cjs');
const Base = require('./_orderFactsWorld.cjs');
const fails = []; let passed = 0;
const check = (ok, what) => { if (ok) passed++; else fails.push(what); console.log((ok ? '  ok   ' : '  FAIL ') + what); };
const V = (name, value) => ({ name, value });
const fx = (rid, buyer) => ({ rid, sku: 'COMET_9427', listing: '18' + rid.slice(-6), title: 'Comet Necklace, Celestial Charm Necklace in 14k Gold Fill', buyer, placed: { day: 0, h: 9, m: 25 }, ship: 5, variations: [V('Metal choice', 'Gold')], personalization: [] });
const ALL = fx('4290000401', 'Name Forty'), LIVE = fx('4290000402', 'Name Fortyone'), GONE = fx('4290000403', 'Name Fortytwo');
const keyOf = f => `${f.rid}_${f.rid}1`;

{
  const bridge = read('charm-nest-bridge.js'), html = read('charm-nest-1.html');
  check(/tryDo\(paintWinSeals\)/.test(bridge) && /"cn-review-mark", "cn-order-seal"/.test(bridge) && /function winSealsOf\(/.test(bridge), 'source: paint() draws the seals; the window listens for the seal events');
  check(/id="owSealRow"/.test(html) && /charm-nest-bridge\.js\?v=[^"]*-sw1[-"]/.test(html), 'source: the Seals row is in the page; the cache token is bumped (-sw1)');
}

(async () => {
  const W = await R.world({ viewport: { width: 1440, height: 900 }, reducedMotion: 'reduce' });
  if (!W) { console.log('no playwright-core: the browser part is skipped'); return finish(); }
  const { page } = W, sleep = ms => page.waitForTimeout(ms);
  try {
    await page.evaluate(({ fixtures, orderSrc }) => {
      const orderOf = eval('(' + orderSrc + ')'), now = Date.now();
      for (const f of fixtures) { const order = orderOf(f, now); for (const line of order.lines) { const key = CharmNestOrders.lineKey(order, line); const row = { key, order, line, arrivedAt: Date.now(), spec: null, problems: [], state: 'pulled', reason: null, claimedBy: null, poolIds: [], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); } }
      Orders.interpretAll();
    }, { fixtures: [ALL, LIVE, GONE], orderSrc: Base.orderOf.toString() });
    const T = await page.evaluate(() => Date.now()), H = 3600e3;
    // seeds: a print seal on the piece's record, a Release Hold on the line, cancel + sheet records on the order's Review document (the release record's shape: { at, by, how, id })
    await page.evaluate(({ all, gone, allKey, goneKey, T, H }) => {
      const seal = (rid, how, at, by, extra) => Object.assign({ at, by, how, id: `${how}:${rid}:${at}` }, extra || {});
      B.maps.customKept = Object.assign({}, B.maps.customKept, {
        [allKey]: { key: allKey, receiptId: all, state: 'open', stamps: [{ how: 'print', at: T - 5 * H, by: 'Pat Printer', n: 1 }], prints: 1 },
        [goneKey]: { key: goneKey, receiptId: gone, state: 'open', stamps: [{ how: 'print', at: T - 6 * H, by: 'Pat Printer', n: 1 }], prints: 1 } });
      B.orders.byKey.get(allKey).releaseHold = { at: T - 4 * H, by: 'Cy Releaser', id: `releaseHold:${all}:${T - 4 * H}` };
      B.maps.reviewMarks = Object.assign({}, B.maps.reviewMarks, {
        [all + '_review']: { key: all + '_review', kind: 'reviewMark', category: 'Review', receiptId: all, review: { state: 'done', markedAt: T - 4.5 * H, markedBy: 'Hal Holder', doneAt: T - 4.2 * H, doneBy: 'Hal Holder', how: 'other' }, sheet: seal(all, 'sheet', T - 3 * H, 'Sam Sheeter'), cancel: seal(all, 'cancel', T - 2 * H, 'Cass Canceller'), engraveSent: { at: T - 1 * H, by: 'Una Known', label: 'Sent to engraver' } },
        [gone + '_review']: { key: gone + '_review', kind: 'reviewMark', category: 'Review', receiptId: gone, cancel: seal(gone, 'cancel', T - 1 * H, 'Cass Canceller') } });
      for (const r of B.orders.rows) if (String(r.order.receiptId) === gone) r.state = 'gone';
    }, { all: ALL.rid, gone: GONE.rid, allKey: keyOf(ALL), goneKey: keyOf(GONE), T, H });
    await page.evaluate(() => { CN.setMode('orders'); Orders.showPile('', ''); Orders.render(); });

    const closeWin = async () => { await page.evaluate(() => { const o = document.getElementById('orderWin'); if (o && o.open) document.getElementById('owClose').click(); }); await page.waitForFunction(() => !document.getElementById('orderWin').open, null, { timeout: 8000 }).catch(() => {}); await sleep(250); };
    const settle = async () => { await page.waitForSelector('#owMeta [data-owf-chip="order"]', { timeout: 20000 }); await sleep(900); };
    const rowSeals = () => page.evaluate(() => {
      const row = document.getElementById('owSealRow'), vis = !!row && !row.hidden && row.getBoundingClientRect().height > 10;
      return { vis, items: [...document.querySelectorAll('#owSealRow [data-ow-win-seal], #owSealRow .owWinSeal')].map(n => { const svg = n.querySelector('svg'), r = n.getBoundingClientRect(); return { kind: n.dataset.sealKind, at: +n.dataset.sealAt, title: n.title, family: svg && svg.dataset.sealFamily, text: svg ? (() => { try { return JSON.parse(svg.dataset.sealModel).action || ''; } catch (_) { return ''; } })() : '', w: Math.round(r.width) }; }) };
    });

    // ═══ 2 · all four ═══
    await page.evaluate(k => OrderWin.open(k), keyOf(ALL)); await settle();
    let s = await rowSeals();
    const kinds = s.items.map(i => i.kind);
    check(s.vis && ['print', 'release', 'sheet', 'cancel', 'hold'].every(k => kinds.includes(k)), `2 the Seals row shows print, release, sheet, cancel and ORDER HELD (the Hold Order mark) (${kinds.join(', ')})`);
    check(s.items.every((x, i, a) => !i || a[i - 1].at <= x.at), '2 oldest to newest');
    const by = k => s.items.find(i => i.kind === k) || {};
    check(by('release').family === 'release' && by('cancel').family === 'cancelled' && /ORDER CANCELLED/.test(by('cancel').text) && /SENT TO SHEET/.test(by('sheet').text) && by('hold').family === 'exceptions' && by('hold').text === 'ORDER HELD', `2 each in its kind's design (${s.items.map(i => i.kind + ':' + i.family).join(' ')})`);
    check(/Cass Canceller/.test(by('cancel').title) && /Sam Sheeter/.test(by('sheet').title) && /Cy Releaser/.test(by('release').title) && /Pat Printer/.test(by('print').title) && /Hal Holder/.test(by('hold').title), '2 who and when on hover (title)');
    check(s.items.every(i => i.w >= 24 && i.w <= 48), `2 compact (${s.items.map(i => i.w).join(',')} px)`);
    // ═══ 3 · an unlisted kind ═══
    check(by('engraveSent').family === 'prepared' && /SENT TO ENGRAVER/.test(by('engraveSent').text) && /Sent to engraver · by Una Known/.test(by('engraveSent').title), '3 an unlisted kind: neutral ink, its stored label');
    // ═══ 4 · beside its button ═══
    await page.evaluate(() => { const b = document.createElement('button'); b.type = 'button'; b.className = 'btn ghost xs cnCancelBtn'; b.id = 'testCancelBtn'; b.textContent = 'Cancel Order'; document.querySelector('#orderWin .owRight').prepend(b); OrderWin.paint(); });
    await sleep(300);
    const side = await page.evaluate(() => ({ beside: !!document.querySelector('#testCancelBtn + [data-ow-btn-seals] [data-seal-kind="cancel"]'), inRow: !!document.querySelector('#owSealRow [data-seal-kind="cancel"]') }));
    check(side.beside && !side.inRow, `4 a seal whose button is shown sits beside it (${JSON.stringify(side)})`);
    await page.evaluate(() => { document.getElementById('testCancelBtn').remove(); OrderWin.paint(); });
    await closeWin();

    // ═══ 5 · live ═══
    await page.evaluate(k => OrderWin.open(k), keyOf(LIVE)); await settle();
    s = await rowSeals(); check(!s.vis && !s.items.length, '5 an order with no seal: no Seals row');
    const put = (rid, field, v) => page.evaluate(({ rid, field, v }) => { const k = rid + '_review', was = B.maps.reviewMarks[k] || { key: k, kind: 'reviewMark', category: 'Review', receiptId: rid }, rec = Object.assign({}, was); if (v) rec[field] = v; else delete rec[field]; B.maps.reviewMarks = Object.assign({}, B.maps.reviewMarks, { [k]: rec }); }, { rid, field, v });
    await put(LIVE.rid, 'sheet', { at: T, by: 'Live Sheeter', how: 'sheet', id: 'sheet:' + LIVE.rid + ':' + T });
    await page.evaluate(rid => document.dispatchEvent(new CustomEvent('cn-order-seal', { detail: { rid } })), LIVE.rid); await sleep(200);
    s = await rowSeals(); check(s.vis && s.items.some(i => i.kind === 'sheet' && /Live Sheeter/.test(i.title)), '5 a seal stamped while the window is open appears at once (the seal event)');
    await put(LIVE.rid, 'cancel', { at: T + 1000, by: 'Live Canceller', how: 'cancel', id: 'cancel:' + LIVE.rid });
    await page.evaluate(() => OrderWin.paint()); await sleep(300);
    s = await rowSeals(); check(s.items.some(i => i.kind === 'cancel'), '5 ... and on a plain repaint (what every stamp already calls)');
    await put(LIVE.rid, 'sheet', null);
    await page.evaluate(rid => document.dispatchEvent(new CustomEvent('cn-review-mark', { detail: { rid, kind: 'synced' } })), LIVE.rid); await sleep(200);
    s = await rowSeals(); check(!s.items.some(i => i.kind === 'sheet') && s.items.some(i => i.kind === 'cancel'), '5 Undo takes the seal away live');
    await put(LIVE.rid, 'cancel', { at: T + 1000, by: 'Live Canceller', how: 'cancel', id: 'cancel:' + LIVE.rid, undoneAt: T + 2000, undoneBy: 'Live Canceller' });
    await page.evaluate(() => OrderWin.paint()); await sleep(300);
    s = await rowSeals(); check(!s.items.some(i => i.kind === 'cancel'), '5 a cancel marked undone (cancel.undoneAt, the Undo of Cancel Order) is not shown');
    await closeWin();

    // ═══ 6 · a cancelled order ═══
    await page.evaluate(rid => OrderWin.openOrder(rid), GONE.rid); await settle();
    s = await rowSeals();
    check(s.vis && s.items.some(i => i.kind === 'cancel') && s.items.some(i => i.kind === 'print'), `6 a cancelled order's window shows its seals (${s.items.map(i => i.kind).join(', ')})`);
    await closeWin();
    const errs = W.errors.filter(e => !/ResizeObserver/.test(e));
    check(errs.length === 0, `no page error (${errs.slice(0, 3).join(' | ')})`);
  } catch (e) {
    fails.push('the test stopped: ' + (e && e.stack || e));
    console.log('  FAIL the test stopped:', String(e && e.stack || e).split('\n').slice(0, 6).join('\n'));
  } finally { await W.close(); }
  finish();
})();

function finish() {
  console.log(fails.length ? `\n${fails.length} FAILED, ${passed} passed:\n- ${fails.join('\n- ')}` : `\nseals-window: ${passed} checks passed`);
  process.exitCode = fails.length ? 1 : 0;
}
