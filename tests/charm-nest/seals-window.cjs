// SEALWIN (Paul 20 Oct 2026: "All of these seals must be visible in all places where a given button is relevant or information about a given button's status is relevant to the user. This includes the
// Detailed Order view modal."): the order window showed every seal the order had collected, beside its button when that button was shown in the window, else in a compact Seals row (#owSealRow).
// Paul, 26 Oct 2026: "Remove this extra duplicate and unneeded 'Sent to Sheet' button + Seal and the second 'Seals' menu completely from the UI of the Detailed Order modal." The Seals row is GONE
// (charm-nest-1.html has no #owSealRow and no CSS for it; charm-nest-bridge.js paintWinSeals draws no row). A seal whose button IS shown in the window still sits beside that button; a seal whose button is
// NOT shown is not drawn in the window at all (it stays on the Orders / Review cards and in the Timeline tab).
//   what is proved now (the REAL page over the offline fake site; every name is made up; the cancel and sheet seals are seeded as records in the release seal record's shape, on the order's Review document)
//     1  node: the painter is in the window's paint and listens for the seal events; NO Seals row in the page (no #owSealRow element, no .owSealRow CSS) and paintWinSeals does not touch one;
//        the bridge's cache token is the current one (-ns26)
//     2  an order with print + release + cancel + sheet seals, a Hold Order mark (ORDER HELD) and a kind nobody listed (a seeded "engraveSent" record with its own label): the order still has every one
//        (OrderWin.seals(), the window's reader of the seals the cards share), but the window has no Seals row, no "Seals" header, and draws no seal away from a shown button of its kind: ORDER HELD
//        sits beside the window's own Hold Order button (.revSendBtn) in its kind's design, compact, who and when on hover; the seals whose button the window does not show (here release, sheet, cancel
//        and the unlisted kind) are not drawn in it at all
//     3  (dropped: it proved an unlisted kind was drawn in the Seals row with a neutral ink. An unlisted kind has no button, so it is never drawn in the window now; 2 checks the reader still lists it with
//        its stored label and that the window does not draw it)
//     4  a seal whose button is shown in the window sits beside that button (the test's own Cancel Order button, #testCancelBtn), in its kind's design, compact, who and when on hover, once; when the button
//        goes, the seal leaves the window with it (no row takes it)
//     5  LIVE, beside a shown button (the same injected Cancel Order button): a seal stamped while the window is open appears beside it without a reload (the seal event), its Undo takes it away live,
//        it appears again on a plain repaint, and a cancel marked undone is not shown; never a Seals row
//     6  a cancelled order's window (all its lines gone, opened by its receipt id as Orders › Cancelled does): no Seals row; the reader still has its cancel and print seals; its cancel seal sits beside a
//        shown Cancel Order button
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
  const fn = (src, start, end) => { const i = src.indexOf(start), j = src.indexOf(end, i + start.length); return i >= 0 && j > i ? src.slice(i, j) : ''; };
  const painter = fn(bridge, 'function paintWinSeals()', '/** Paint the window from the row it is showing. */');
  check(/tryDo\(paintWinSeals\)/.test(bridge) && /"cn-review-mark", "cn-order-seal"/.test(bridge) && /function winSealsOf\(/.test(bridge), 'source: paint() draws the seals; the window listens for the seal events');
  check(!/owSealRow|ow-seal-row/.test(html), 'source: no Seals row in the page (Paul, 26 Oct 2026): no #owSealRow element, no .owSealRow CSS');
  check(!!painter && /data-ow-btn-seals/.test(painter) && !/owSealRow|owSealRowHd/.test(painter), 'source: paintWinSeals still draws a seal beside its button and no longer touches a Seals row');
  check(/charm-nest-bridge\.js\?v=[^"]*-ns26[-"]/.test(html), 'source: the bridge\'s cache token is the current one (-ns26)');
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
    /** what the window draws of the order's seals now: whether any Seals row or "Seals" header is left, every drawn seal (beside a shown button or not), and what the reader still knows */
    const winSeals = () => page.evaluate(() => {
      const dlg = document.getElementById('orderWin');
      const shown = b => !!b && b.isConnected && !b.hidden && !b.closest('[hidden]') && getComputedStyle(b).display !== 'none' && b.getBoundingClientRect().width > 0;
      const header = [...dlg.querySelectorAll('*')].some(n => !n.children.length && n.textContent.trim() === 'Seals' && !n.closest('[data-view="timeline"]'));
      const drawn = [...dlg.querySelectorAll('.owWinSeal, [data-testid="ow-win-seal"]')].map(n => {
        const g = n.closest('[data-ow-btn-seals]'), b = g && g.previousElementSibling, svg = n.querySelector('svg'), r = n.getBoundingClientRect();
        return { kind: n.dataset.sealKind, at: +n.dataset.sealAt, title: n.title, family: svg && svg.dataset.sealFamily, text: svg ? (() => { try { return JSON.parse(svg.dataset.sealModel).action || ''; } catch (_) { return ''; } })() : '',
          w: Math.round(r.width), beside: !!g, btn: b ? (b.id || b.className) : '', btnShown: shown(b) };
      });
      // (the buttons of the kinds that are drawn beside one, as SEAL_KINDS names them; print and complete rest their seals on a piece's row, cuControls, and an unlisted kind has no button)
      const BTN = { release: '[data-rel-hold],[data-cu-release]', cancel: '.cnCancelBtn,[data-cancel-order]', sheet: '#owSendSheet,[data-cu-sheet-send]', hold: '.revSendBtn' };
      const btnShown = Object.fromEntries(Object.entries(BTN).map(([k, sel]) => [k, [...dlg.querySelectorAll(sel)].some(b => !b.closest('[data-ow-btn-seals]') && shown(b))]));
      return { row: !!document.getElementById('owSealRow') || !!document.querySelector('.owSealRow, .owSealRowHd, [data-testid="ow-seal-row"]'), header, drawn, btnShown,
        known: (OrderWin.seals() || []).map(s => ({ kind: s.kind, by: s.by, label: s.label })) };
    });
    const addCancelBtn = () => page.evaluate(() => { const b = document.createElement('button'); b.type = 'button'; b.className = 'btn ghost xs cnCancelBtn'; b.id = 'testCancelBtn'; b.textContent = 'Cancel Order'; document.querySelector('#orderWin .owRight').prepend(b); OrderWin.paint(); });
    const dropCancelBtn = () => page.evaluate(() => { const b = document.getElementById('testCancelBtn'); if (b) b.remove(); OrderWin.paint(); });
    const besideCancel = () => page.evaluate(() => [...document.querySelectorAll('#testCancelBtn + [data-ow-btn-seals] [data-seal-kind]')].map(n => ({ kind: n.dataset.sealKind, title: n.title })));

    // ═══ 2 · every kind on the order, none drawn away from its button, no Seals row ═══
    await page.evaluate(k => OrderWin.open(k), keyOf(ALL)); await settle();
    let s = await winSeals();
    const kinds = s.known.map(i => i.kind);
    // (RELEASED, Paul 27 Oct 2026: "updating the Hold Order seal to a Released Order Seal": this order was held (Hal Holder) and then released (Cy Releaser), so its ORDER HELD is what the release ended: it is
    //  no longer a seal of its own, the ORDER RELEASED seal carries it (held))
    check(['print', 'release', 'sheet', 'cancel', 'engraveSent'].every(k => kinds.includes(k)) && !kinds.includes('hold'), `2 the order still has print, release, sheet, cancel and the unlisted kind; its ORDER HELD, ended by the later release, is not a seal of its own (OrderWin.seals(): ${kinds.join(', ')})`);
    const relRec = await page.evaluate(() => (CNSealTravel.of(OrderWin.rid()) || []).find(x => x.kind === 'release') || null);   // (the one reader the window and the cards share)
    check(relRec && relRec.held && relRec.held.by === 'Hal Holder', '2 ... the ORDER RELEASED seal knows the hold it ended: ' + JSON.stringify(relRec && relRec.held));
    check((s.known.find(i => i.kind === 'engraveSent') || {}).label === 'Sent to engraver', '2 ... the unlisted kind with its stored label');
    check(!s.row && !s.header, `2 no Seals row: no #owSealRow in the page, no "Seals" header in the window (row=${s.row} header=${s.header})`);
    check(s.drawn.every(d => d.beside && d.btnShown), `2 no seal is drawn in the window away from a shown button of its kind (${JSON.stringify(s.drawn)})`);
    // (RELSTAMP / RELEASED, 27 Oct 2026: the ORDER RELEASED seal sits beside the window's own Hold Order button, the button that takes Release Hold's place once the order is released; no ORDER HELD is drawn)
    const ho = s.drawn.filter(d => d.kind === 'release');
    check(s.btnShown.hold && ho.length === 1 && ho[0].beside && /revSendBtn/.test(ho[0].btn) && ho[0].btnShown && !s.drawn.some(d => d.kind === 'hold'), `2 ORDER RELEASED sits beside the window's own Hold Order button, once, and no ORDER HELD is drawn (${JSON.stringify(s.drawn)})`);
    check(ho.length === 1 && ho[0].family === 'release' && ho[0].text === 'ORDER RELEASED' && /Order released · by Cy Releaser/.test(ho[0].title) && ho[0].w >= 24 && ho[0].w <= 48, `2 ... in its kind's design, who and when on hover, compact (${ho.map(d => d.family + ':' + d.text + ' ' + d.w + 'px ' + d.title).join(' ')})`);
    const unshown = s.known.map(i => i.kind).filter(k => k !== 'print' && k !== 'complete' && k !== 'release' && !s.btnShown[k]);   // (release: beside Hold Order, above)
    // (the order is held (ORDER HELD), so its window shows a Release Hold button and the release seal rests beside it, checked above; the rest have no button here)
    check(['sheet', 'cancel', 'engraveSent'].every(k => unshown.includes(k)) && unshown.every(k => !s.drawn.some(d => d.kind === k)), `2 the seals whose button the window does not show (${unshown.join(', ')}) are not drawn in it (drawn: ${s.drawn.map(d => d.kind).join(', ') || 'none'}; buttons shown: ${JSON.stringify(s.btnShown)})`);
    // ═══ 4 · beside its button ═══
    await addCancelBtn(); await sleep(300);
    s = await winSeals();
    const cx = s.drawn.filter(d => d.kind === 'cancel');
    check(cx.length === 1 && cx[0].beside && cx[0].btn === 'testCancelBtn' && cx[0].btnShown, `4 a seal whose button is shown sits beside it, once (${JSON.stringify(cx)})`);
    check(cx.length === 1 && cx[0].family === 'cancelled' && /ORDER CANCELLED/.test(cx[0].text), `4 ... in its kind's design (${cx.map(d => d.family + ':' + d.text).join(' ')})`);
    check(cx.length === 1 && /Order cancelled/.test(cx[0].title) && /by Cass Canceller/.test(cx[0].title), `4 ... who and when on hover (title: ${cx.map(d => d.title).join(' | ')})`);
    check(cx.length === 1 && cx[0].w >= 24 && cx[0].w <= 48, `4 ... compact (${cx.map(d => d.w).join(',')} px)`);
    check(!s.row && !s.header, '4 still no Seals row');
    await dropCancelBtn(); await sleep(300);
    s = await winSeals();
    check(!s.drawn.some(d => d.kind === 'cancel') && !s.row && s.drawn.every(d => d.beside && d.btnShown), `4 when its button goes, the seal leaves the window with it (no row takes it) (still drawn: ${s.drawn.map(d => d.kind + ' beside ' + d.btn).join(', ') || 'none'})`);
    await closeWin();

    // ═══ 5 · live, beside a shown button ═══
    await page.evaluate(k => OrderWin.open(k), keyOf(LIVE)); await settle();
    await addCancelBtn(); await sleep(300);
    s = await winSeals(); check(!s.row && !s.drawn.length && !(await besideCancel()).length, '5 an order with no seal: nothing beside its button, no Seals row');
    const put = (rid, field, v) => page.evaluate(({ rid, field, v }) => { const k = rid + '_review', was = B.maps.reviewMarks[k] || { key: k, kind: 'reviewMark', category: 'Review', receiptId: rid }, rec = Object.assign({}, was); if (v) rec[field] = v; else delete rec[field]; B.maps.reviewMarks = Object.assign({}, B.maps.reviewMarks, { [k]: rec }); }, { rid, field, v });
    await put(LIVE.rid, 'cancel', { at: T, by: 'Live Canceller', how: 'cancel', id: 'cancel:' + LIVE.rid + ':' + T });
    await page.evaluate(rid => document.dispatchEvent(new CustomEvent('cn-order-seal', { detail: { rid } })), LIVE.rid); await sleep(200);
    let bc = await besideCancel(); check(bc.length === 1 && bc[0].kind === 'cancel' && /Live Canceller/.test(bc[0].title), `5 a seal stamped while the window is open appears beside its button at once (the seal event) (${JSON.stringify(bc)})`);
    await put(LIVE.rid, 'cancel', null);
    await page.evaluate(rid => document.dispatchEvent(new CustomEvent('cn-review-mark', { detail: { rid, kind: 'synced' } })), LIVE.rid); await sleep(200);
    bc = await besideCancel(); check(!bc.length, `5 Undo takes the seal away live (${JSON.stringify(bc)})`);
    await put(LIVE.rid, 'cancel', { at: T + 1000, by: 'Live Recanceller', how: 'cancel', id: 'cancel:' + LIVE.rid + ':' + (T + 1000) });
    await page.evaluate(() => OrderWin.paint()); await sleep(300);
    bc = await besideCancel(); check(bc.length === 1 && /Live Recanceller/.test(bc[0].title), `5 ... and appears again on a plain repaint (what every stamp already calls) (${JSON.stringify(bc)})`);
    await put(LIVE.rid, 'cancel', { at: T + 1000, by: 'Live Recanceller', how: 'cancel', id: 'cancel:' + LIVE.rid + ':' + (T + 1000), undoneAt: T + 2000, undoneBy: 'Live Recanceller' });
    await page.evaluate(() => OrderWin.paint()); await sleep(300);
    bc = await besideCancel(); check(!bc.length, '5 a cancel marked undone (cancel.undoneAt, the Undo of Cancel Order) is not shown');
    s = await winSeals(); check(!s.row && !s.header, '5 never a Seals row');
    await dropCancelBtn();
    await closeWin();

    // ═══ 6 · a cancelled order ═══
    await page.evaluate(rid => OrderWin.openOrder(rid), GONE.rid); await settle();
    s = await winSeals();
    check(!s.row && !s.header, '6 a cancelled order\'s window has no Seals row');
    check(s.known.some(i => i.kind === 'cancel') && s.known.some(i => i.kind === 'print'), `6 ... the reader still has its seals (${s.known.map(i => i.kind).join(', ')})`);
    await addCancelBtn(); await sleep(300);
    bc = await besideCancel(); check(bc.length === 1 && bc[0].kind === 'cancel' && /Cass Canceller/.test(bc[0].title), `6 ... its cancel seal sits beside a shown Cancel Order button (${JSON.stringify(bc)})`);
    await dropCancelBtn();
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
