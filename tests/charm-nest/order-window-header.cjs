// The Order window's header, its milestone rail and the seals of its Engraving cards (HDRHEAD / HDRSEAL; Paul, 16 Oct 2026, order 4195059483, five pictures):
//   1. the seals on an Engraving card are twice as large (48 px, were 24) and are pressed across the card's outer top-right corner: partly on the card, partly beyond its outline (a real seal joins two sheets of paper);
//      several overlap a little, the newest on top; nothing clips them; they never cover the card's name or buttons; their zoom grows with them
//   2. the "NEEDS A DECISION" head card draws no seal (it was a duplicate of the Engraving card's)
//   3. the header has no line of buyer · pieces · ship by under the number and no state / metal chips; nothing else needed them, and what they said is told where it already was
//   4. the milestone rail fills the header between the number and the walk controls, its stamps 2x (40 px, were 20) and its names larger (were 8.5 px), still one row, aligned with the controls
//   5. the rail's stamp zoom grows with them (1.65 x of 40 = 66 px, twice the 33 px a 20 px stamp grew to)
// Headless Chromium against the local fake site (bridge-server.cjs), offline, fake buyers only. At 1440, 1024 and 400 px wide.
//   SHOTS=<dir> node tests/charm-nest/order-window-header.cjs [playwright-core dir]
//   HDR_MEASURE=1  records the numbers and pictures without asserting (how the "before" of this change was measured on main)
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

/* what main drew, measured with this very file's fixtures and browser before the change (CSS px) */
const OLD = { stamp: 20, label: 8.5, zoomed: 33, cardSeal: 24, cardSealZoomed: 37 };

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 25, 17) / 1000), T0 = Date.UTC(2026, 9, 16, 17, 0, 0), MIN = 60000;
const R1 = '4195059483', R2 = '4195059484', R3 = '4195059485';   // Mandy Bellamy (two seals, a question), Ann Seal (three seals, no question), Tess One (four cards: a second row)
const ln = (rid, n, sku, metal, label) => ({ transactionId: rid + n, listingId: '19037' + n + '9935', sku, title: sku.replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: label }], metalKey: metal, metalLabel: label, personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 9 * DAY, updateTs: SHIP - 9 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });
const ORDERS = [
  order(R1, 'Mandy Bellamy', [ln(R1, '1', 'CORGI_34728', 'silver', 'SS'), ln(R1, '2', 'PLAIN_TAG', 'silver', 'SS')]),
  order(R2, 'Ann Seal', [ln(R2, '1', 'CORGI_34728', 'silver', 'SS')]),
  order(R3, 'Tess One', [ln(R3, '1', 'AAA_ONE', 'gold', 'GF 14/20'), ln(R3, '2', 'BBB_TWO', 'gold', 'GF 14/20'), ln(R3, '3', 'CCC_THREE', 'gold', 'GF 14/20'), ln(R3, '4', 'DDD_FOUR', 'gold', 'GF 14/20')])];
const key = (rid, n) => `${rid}_${rid}${n}`;
const SEALS = { [key(R1, 1)]: [['Giovanna', 23], ['Paul', 24]], [key(R2, 1)]: [['Giovanna', 21], ['Paul', 22], ['Sam', 24]],
  [key(R3, 1)]: [['Paul', 23]], [key(R3, 2)]: [['Paul', 24]], [key(R3, 3)]: [['Paul', 25]], [key(R3, 4)]: [['Paul', 26], ['Sam', 27]] };
const WORDS = { [key(R1, 1)]: 'Test', [key(R2, 1)]: 'Test', [key(R3, 1)]: 'One', [key(R3, 2)]: 'Two', [key(R3, 3)]: 'Three', [key(R3, 4)]: 'Four' };

(async () => {
  const MEASURE = process.env.HDR_MEASURE === '1';
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { try { ({ chromium } = require('/opt/node22/lib/node_modules/playwright/node_modules/playwright-core')); } catch (__) { console.log('  - no playwright-core: the browser checks were not run'); return; } }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [], num = {};
  const check = (ok, msg) => { if (MEASURE) { console.log('  · ' + msg); return; } if (!ok) fails.push(msg); console.log((ok ? '  ✓ ' : '  ✗ ') + msg); };
  const near = (a, b, tol) => Math.abs(a - b) <= tol;
  try {
    const context = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2, timezoneId: 'America/Toronto' });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(30000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    page.on('console', m => { if (m.type() === 'error' && !/Failed to load resource|net::ERR/.test(m.text())) { errors.push('console: ' + m.text()); console.error('console error:', m.text().slice(0, 200)); } });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.Engrave && window.OrderEngraving && window.CNEngravingSeals && window.OrderTimeline && CN.S.cloud.ok === true, null, { timeout: 60000 });

    // ── the orders, their approved jobs (with their seals) and their timelines ──
    await page.evaluate(async ({ orders, SEALS, WORDS, R1, R2, R3, T0, MIN }) => {
      for (const o of orders) for (const line of o.lines) { const key = CharmNestOrders.lineKey(o, line); const row = { key, order: o, line, arrivedAt: T0, spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [key + '_1'], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      for (const o of orders) for (const line of o.lines) B.master.entries.set(line.sku, { sku: line.sku, updatedAt: 1 });   // (known designs: the only question is the one the fixture raises)
      Orders.interpretAll(); await Orders.loadMaps(true); Orders.interpretAll();
      const art = text => { const c = document.createElement('canvas'); c.width = c.height = 300; const x = c.getContext('2d'); x.fillStyle = '#fff'; x.fillRect(0, 0, 300, 300); x.fillStyle = '#111'; x.font = '40px sans-serif'; x.textAlign = 'center'; text.split('\n').forEach((t, i) => x.fillText(t, 150, 150 + i * 44)); return c; };
      Engrave.renderBack = job => art(job.text || '');
      const day = Date.UTC(2026, 9, 16, 18, 0, 0);   // 2:00 PM in Toronto
      window.__evs = {};
      const evs = rid => (window.__evs[rid] = window.__evs[rid] || [{ id: rid + '~arrived', orderId: rid, type: 'arrived', at: T0, by: 'Etsy', source: 'etsy' }, { id: rid + '~placed', orderId: rid, type: 'placed', at: T0 + 20 * MIN, by: 'Paul', source: 'sorter', sheet: 'SS Sheet 1', text: 'Placed on SS Sheet 1', data: {} }]);
      for (const [lk, list] of Object.entries(SEALS)) {
        const row = B.orders.byKey.get(lk), rid = String(row.order.receiptId), j = Engrave.ensureJob(row), text = WORDS[lk];
        Object.assign(j, { text, lines: [text], backs: [] }); row.engrave = { needed: true, state: 'approved', approved: true, text };
        for (const [by, minute] of list) {
          const at = day + (minute - 20) * MIN; CNEngravingSeals.add(j, 'engraveApproved', by, at); j.approvedBy = by; j.approvedAt = at; row.engrave.approvedBy = by; row.engrave.approvedAt = at;
          evs(rid).push({ id: rid + '~eng~' + lk + at, orderId: rid, type: 'engraveApproved', at, by, source: 'sorter', lineKey: lk, text: 'Back engraving approved', data: { poolId: lk + '_1' } });
        }
        CNEngravingSeals.keep(j); j.state = 'approved';
        j.backs = [{ poolId: lk + '_1', approvedAt: j.approvedAt, approvedBy: j.approvedBy, png: art(text).toDataURL() }];
      }
      // the question the order raised (Paul's picture 2): NEEDS A DECISION, "Words: Check the requested inscription"
      evs(R1).push({ id: R1 + '~nd', orderId: R1, type: 'needsDecision', at: day + 30 * MIN, by: 'system', source: 'sorter', lineKey: R1 + '_' + R1 + '2', text: 'Words: Check the requested inscription', data: { kind: 'words', why: 'Check the requested inscription' } });
      Engrave.render = () => {}; Engrave.approve = async () => {};
      const wait = window.setTimeout.bind(window);
      OrderTimeline.get = async rid => { await new Promise(r => wait(r, 15)); return JSON.parse(JSON.stringify({ id: rid, events: evs(rid), cancelled: null, where: null })); };
      CN.setMode('orders'); Orders.render();
    }, { orders: ORDERS, SEALS, WORDS, R1, R2, R3, T0, MIN });

    const settled = () => page.waitForFunction(() => !document.getElementById('orderWin').getAnimations({ subtree: true }).some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), null, { timeout: 15000 });
    const open = async k => {
      await page.evaluate(k => OrderWin.open(k), k);
      await page.waitForFunction(k => OrderWin.isOpen() && OrderWin.key() === k && document.querySelectorAll('#owRail .tlStop').length > 3 && document.querySelector('#owEng .engCard .engThumb'), k, { timeout: 20000 });
      await page.evaluate(() => { if (OrderWin.selectedPiece && OrderWin.selectedPiece()) OrderWin.selectPiece(null); });
      await page.waitForFunction(() => document.querySelectorAll('#owEng .engCard .egButtonSeal .seal').length > 0, null, { timeout: 15000 });
      await settled(); await page.mouse.move(5, 895); await page.waitForTimeout(500);
    };
    const close = async () => { await page.evaluate(() => OrderWin.close && OrderWin.close()); await page.waitForFunction(() => !OrderWin.isOpen(), null, { timeout: 8000 }).catch(() => {}); await page.waitForTimeout(300); };

    // what the page draws, in numbers (CSS px)
    const measure = () => page.evaluate(() => {
      const R = e => { if (!e) return null; const r = e.getBoundingClientRect(); return { l: r.left, t: r.top, r: r.right, b: r.bottom, w: r.width, h: r.height }; };
      const q = s => document.querySelector(s), qa = s => [...document.querySelectorAll(s)];
      const head = q('#orderWin .owHead'), title = q('#owTitle'), rail = q('#owRail'), right = q('#orderWin .owRight'), close = q('#owClose'), prev = q('#owPrev'), next = q('#owNext'), pos = q('#owPos');
      const stamps = qa('#owRail .tlStop .tlSeal').map(e => e.offsetWidth), labels = qa('#owRail .tlStop > span').map(e => parseFloat(getComputedStyle(e).fontSize));
      const stops = qa('#owRail .tlStop');
      const hs = head && head.getBoundingClientRect();
      const nameOf = el => { const r = document.createRange(); r.selectNodeContents(el); const b = r.getBoundingClientRect(); return { l: b.left, t: b.top, r: b.right, b: b.bottom }; };
      const cards = qa('#owEng .engCard').map(card => {
        const cr = card.getBoundingClientRect(), seals = [...card.querySelectorAll('.egButtonSeal .seal')].map(s => { const r = s.getBoundingClientRect(), cx = (r.left + r.right) / 2, cy = (r.top + r.bottom) / 2, R = s.offsetWidth / 2; const hit = (x, y) => { const e = document.elementFromPoint(x, y); return !!e && (e === s || s.contains(e)); }; return { l: r.left, t: r.top, r: r.right, b: r.bottom, w: s.offsetWidth, h: s.offsetHeight, outsideHit: hit(cx, cy - .8 * R), outsideRightHit: hit(cx + .8 * R, cy), insideHit: hit(cx, cy + .8 * R), label: s.getAttribute('aria-label') || '' }; });
        const name = card.querySelector('.engName'), btns = [...card.querySelectorAll('button.egOpen, button.egApproveButton')].map(R), thumbs = [...card.querySelectorAll('.engThumb')].map(R);
        return { key: card.dataset.key, state: card.dataset.state, card: { l: cr.left, t: cr.top, r: cr.right, b: cr.bottom, w: cr.width }, seals, name: name ? nameOf(name) : null, nameBox: R(name), nameClipped: name ? name.scrollWidth > name.clientWidth + 1 : false, btns, thumbs, count: (card.querySelector('.egButtonSeal') || {}).dataset && card.querySelector('.egButtonSeal').dataset.sealCount };
      });
      const now = q('#owNowCard');
      return {
        vw: innerWidth, docW: document.scrollingElement.scrollWidth, headW: head ? head.scrollWidth : 0, headCW: head ? head.clientWidth : 0, headH: hs ? hs.height : 0, boxW: q('#orderWin .owBox') ? q('#orderWin .owBox').scrollWidth : 0, boxCW: q('#orderWin .owBox') ? q('#orderWin .owBox').clientWidth : 0,
        dlgSW: q('#orderWin').scrollWidth, dlgCW: q('#orderWin').clientWidth,
        ids: { owSub: !!q('#owSub'), owNow: !!q('#owNow'), owMetal: !!q('#owMetal'), pill: !!q('#orderWin .owHead .pill, #orderWin .owHead .owNow'), subClass: !!q('#orderWin .owSub') },
        headText: head ? head.innerText.replace(/\s+/g, ' ').trim() : '', title: title ? title.textContent.trim() : '', labelledby: q('#orderWin').getAttribute('aria-labelledby'),
        titleBox: R(title), railBox: R(rail), rightBox: R(right), closeBox: R(close), prevBox: R(prev), nextBox: R(next), posBox: R(pos), headBox: R(head), railDisplay: rail ? getComputedStyle(rail).display : '',
        stamps, labels, stopBoxes: stops.map(R), stopSw: stops.map(s => s.scrollWidth - s.clientWidth),
        railLabelClipped: stops.some(s => { const sp = s.querySelector(':scope > span'); return sp && sp.scrollWidth > sp.clientWidth + 1; }),
        trackH: (() => { const t = q('#owRail .tlTrack'); return t ? t.getBoundingClientRect().height : 0; })(),
        meta: q('#owMeta') ? q('#owMeta').innerText.replace(/\s+/g, ' ').trim() : '', pcSum: q('#owPcSum') ? q('#owPcSum').innerText.replace(/\s+/g, ' ').trim() : '',
        now: now && !now.hidden ? { k: (now.querySelector('.k') || {}).textContent || '', t: (now.querySelector('.t') || {}).textContent || '', seals: now.querySelectorAll('.tlNowSeal').length, sealed: now.classList.contains('sealed'), cols: getComputedStyle(now).gridTemplateColumns, h: now.getBoundingClientRect().height, tx: R(now.querySelector('.tx')), box: R(now) } : null,
        cards, grid: R(q('#owEng')), gridGap: q('#owEng') ? getComputedStyle(q('#owEng')).rowGap : '', mainScroll: (() => { const m = q('#orderWin .owMain'), v = q('#orderWin .owVInfo'); return m ? { sw: Math.max(m.scrollWidth, v ? v.scrollWidth : 0), cw: Math.max(m.clientWidth, v ? v.clientWidth : 0) } : null; })(),
        seal: { grid: q('#owEng') ? getComputedStyle(q('#owEng')).overflowX : '' }
      };
    });
    const zoomOf = async (sel, shot) => {   // hover a seal for its delay: the size it grows to (CSS px, from its own box: Seal.zoom scales the seal)
      const el = page.locator(sel).first(); await el.scrollIntoViewIfNeeded().catch(() => {});
      const base = await el.evaluate(e => e.getBoundingClientRect().width), box = await el.boundingBox();
      await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2); await page.waitForTimeout(900);
      const grown = await el.evaluate(e => ({ w: e.getBoundingClientRect().width, zoomed: e.classList.contains('sealZoomed'), k: e.dataset.sealZoom || '' }));
      if (shot && shots) { const g = await el.boundingBox(); const x = Math.max(0, g.x - 90), y = Math.max(0, g.y - 50); await page.screenshot({ path: path.join(shots, shot), clip: { x, y, width: Math.min(300, 1440 - x), height: 200 } }); }
      await page.mouse.move(5, 895); await page.waitForTimeout(500);
      return { base, grown };
    };

    // ════ 1440 px: Mandy Bellamy's order (Paul's pictures): two seals, a question, two pieces ════
    await open(`${R1}_${R1}1`);
    let m = await measure();
    num.m1440 = m;
    if (shots) { await page.screenshot({ path: path.join(shots, 'w1440-order.png') }); await page.locator('#orderWin .owHead').screenshot({ path: path.join(shots, 'w1440-head.png') }); await page.locator('#owEngSec').screenshot({ path: path.join(shots, 'w1440-engraving.png') }).catch(() => {}); if (await page.$('#owNowCard:not([hidden])')) await page.locator('#owNowCard').screenshot({ path: path.join(shots, 'w1440-banner.png') }); }
    // — 3 · the header: the number, the rail, the walk controls; no subtitle, no chips
    check(m.title === 'Order ' + R1 && !m.ids.owSub && !m.ids.subClass && !m.ids.owNow && !m.ids.owMetal && !m.ids.pill, 'the header has no subtitle line and no state or metal chip (no #owSub, #owNow, #owMetal, no pill): ' + JSON.stringify(m.ids));
    check(!/Mandy|piece|ship by|approved|\bSS\b/i.test(m.headText), 'the header says nothing but the number (and the walk controls): "' + m.headText + '"');
    check(m.labelledby === 'owTitle' && m.title === 'Order ' + R1, 'the dialog is named by its title: aria-labelledby="' + m.labelledby + '" → "' + m.title + '"');
    check(/Mandy Bellamy/.test(m.meta) && /Oct 25/.test(m.meta), 'the buyer and the ship-by date are in the Overview chip strip: "' + m.meta.slice(0, 160) + '"');
    check(/2 pieces|Its pieces|Piece/i.test(m.pcSum + ' ' + m.meta) || m.pcSum.length > 0, 'the pieces are listed on the Overview ("' + m.pcSum.slice(0, 80) + '")');
    // — 4 · the rail
    const st = m.stamps; num.stamp1440 = st[2]; num.label1440 = m.labels[0];
    check(st.length >= 5 && st.every(w => near(w, st[2], 2.5)) && st[2] >= 2 * OLD.stamp - 2 && st[2] <= 2.6 * OLD.stamp, `the rail's stamps are twice as large: ${st.map(w => w.toFixed(1)).join(', ')} px (were ${OLD.stamp})`);
    check(m.labels.every(f => f >= 11) && m.labels[0] / OLD.label >= 1.3, `its names are larger: ${m.labels[0].toFixed(1)} px (were ${OLD.label})`);
    const tb = m.titleBox, rb = m.railBox, rr = m.rightBox, hd = m.headBox;
    check(rb.l - tb.r >= 20 && rb.l - tb.r <= 40 && rr.l - rb.r >= 20 && rr.l - rb.r <= 40, `the rail fills the header from the number to the controls, with 28 px of air each side: ${(rb.l - tb.r).toFixed(1)} / ${(rr.l - rb.r).toFixed(1)} px; ${rb.w.toFixed(0)} px wide (was at most 860)`);
    check(rb.w > 860, 'the rail is wider than the 860 px it was held to: ' + rb.w.toFixed(0));
    const cy = r => (r.t + r.b) / 2;
    check(near(cy(rb), cy(hd), 2) && near(cy(m.closeBox), cy(hd), 2) && near(cy(m.prevBox), cy(hd), 2) && near(cy(tb), cy(hd), 3), `the rail, the number, Previous, Next and Close share the header's centre line: ${[rb, tb, m.prevBox, m.closeBox].map(r => cy(r).toFixed(1)).join(', ')} (header ${cy(hd).toFixed(1)})`);
    check(m.stopBoxes.every(b => b.t >= rb.t - 1 && b.b <= rb.b + 1) && m.stopBoxes.every((b, i, a) => !i || near(b.t, a[0].t, 1)), 'one row: every step is inside the rail, on one line');
    check(!m.railLabelClipped, 'no step name is cut off at 1440');
    check(m.trackH >= 2.5, 'the track is thicker: ' + m.trackH + ' px');
    check(m.headH >= 76 && m.headH <= 90, 'the header is ' + m.headH + ' px high (was 52)');
    // — 5 · the rail's zoom
    const zr = await zoomOf('#owRail .tlStop .tlSeal:not(.pending)', 'w1440-zoom-rail.png'); num.railZoom = zr;
    check(zr.grown.zoomed && zr.grown.w >= 1.9 * OLD.zoomed - 4 && zr.grown.w <= 72.5, `a rail stamp zooms to ${zr.grown.w.toFixed(1)} px from ${zr.base.toFixed(1)} (was ${OLD.zoomed}; twice is ${2 * OLD.zoomed}), ×${zr.grown.k}`);
    // — 1 · the seals of Mandy's Engraving card
    const c1 = m.cards.find(c => c.key === key(R1, 1)); num.cardSeals = c1 && c1.seals.length;
    check(c1 && c1.seals.length === 2 && c1.seals.every(s => near(s.w, 2 * OLD.cardSeal, 1)), `the card's two seals are 2x: ${c1 && c1.seals.map(s => s.w.toFixed(1)).join(', ')} px (were ${OLD.cardSeal})`);
    const cr = c1.card, s0 = c1.seals[0], s1 = c1.seals[1];
    check(c1.seals.every(s => s.t < cr.t - 8 && s.b > cr.t + 8), `each seal straddles the card's top border: ${c1.seals.map(s => (cr.t - s.t).toFixed(0) + ' px above / ' + (s.b - cr.t).toFixed(0) + ' px on the card').join(', ')}`);
    check(s1.r > cr.r + 2 && s1.l < cr.r - 20, `the newest crosses the card's right border too: it reaches ${(s1.r - cr.r).toFixed(1)} px past it, ${(cr.r - s1.l).toFixed(0)} px on the card`);
    check(s0.r > s1.l + 4 && s0.l < s1.l - 10 && s1.l - s0.l >= 20, `the two overlap a little and are in the order they were pressed (oldest left, newest at the corner): ${(s0.r - s1.l).toFixed(0)} px of overlap, step ${(s1.l - s0.l).toFixed(0)} px`);
    check(c1.seals.every(s => s.outsideHit) && c1.seals.every(s => s.insideHit || s === s0) && s1.outsideRightHit, 'nothing clips them: the part above the card (and the part past its right edge) takes the pointer (elementFromPoint)');
    check(c1.seals[0].label.includes('Giovanna') && c1.seals[1].label.includes('Paul') && /2:03 PM/.test(c1.seals[0].label) && /2:04 PM/.test(c1.seals[1].label), 'the order and the times are kept (their names say): ' + c1.seals.map(s => s.label).join(' | '));
    const hitsOverlap = await page.evaluate(() => { const c = document.querySelector('#owEng .engCard'), ss = [...c.querySelectorAll('.egButtonSeal .seal')], a = ss[0].getBoundingClientRect(), b = ss[1].getBoundingClientRect(), x = (b.left + a.right) / 2, y = (b.top + b.bottom) / 2, e = document.elementFromPoint(x, y); return { newestOnTop: !!e && ss[1].contains(e), z: ss.map(s => getComputedStyle(s).zIndex) }; });
    check(hitsOverlap.newestOnTop, 'where they overlap the newest is on top');
    const hit = (a, b) => !(a.r <= b.l || b.r <= a.l || a.b <= b.t || b.b <= a.t);
    const covers = c1.seals.flatMap(s => [c1.nameBox && hit(s, { l: c1.nameBox.l, t: c1.nameBox.t, r: c1.nameBox.r, b: c1.nameBox.b }) ? 'the name' : '', ...c1.btns.map(b => hit(s, { l: b.l, t: b.t, r: b.r, b: b.b }) ? 'a button' : ''), ...c1.thumbs.map(b => hit(s, { l: b.l, t: b.t, r: b.r, b: b.b }) ? 'a picture' : '')]).filter(Boolean);
    check(covers.length === 0, 'no seal covers the card\'s name, its buttons or its picture' + (covers.length ? ': ' + covers.join(', ') : ''));
    check(m.cards.every(c => c.seals.every(s => s.outsideHit)), 'every card of the window passes the same hit test');
    const zs = await zoomOf(`#owEng .engCard[data-key="${key(R1, 1)}"] .egButtonSeal .seal:last-child`, 'w1440-zoom-seal.png'); num.sealZoom = zs;
    check(zs.grown.zoomed && zs.grown.w >= 1.9 * OLD.cardSealZoomed - 5 && zs.grown.w <= 80.5, `a card seal zooms to ${zs.grown.w.toFixed(1)} px from ${zs.base.toFixed(1)} (was ${OLD.cardSealZoomed}; twice is ${2 * OLD.cardSealZoomed}), ×${zs.grown.k}`);
    // — 2 · the banner
    check(m.now && /Needs a decision/i.test(m.now.k) && /Check the requested inscription/.test(m.now.t), 'the banner says NEEDS A DECISION and the words: "' + (m.now && m.now.k) + ' / ' + (m.now && m.now.t) + '"');
    check(m.now && m.now.seals === 0 && !m.now.sealed && !/px px/.test(m.now.cols) && m.now.cols.split(' ').length === 1, 'the banner draws no seal and its text takes the whole card: ' + (m.now && m.now.cols));
    check(m.now && near(m.now.h, 79, 1), 'the banner keeps its 79 px height: ' + (m.now && m.now.h));
    // the same order with no question: its head card keeps its seal
    await close(); await open(`${R2}_${R2}1`);
    const m2 = await measure(); num.m2 = m2;
    check(m2.now && !/Needs a decision/i.test(m2.now.k) && m2.now.seals === 1 && m2.now.sealed, 'an order with no question keeps the seal in its head card: "' + (m2.now && m2.now.k) + '", ' + (m2.now && m2.now.seals) + ' seal');
    const c2 = m2.cards.find(c => c.key === key(R2, 1));
    check(c2 && c2.seals.length === 3 && c2.seals.every(s => near(s.w, 48, 1) && s.outsideHit && s.t < c2.card.t - 8 && s.b > c2.card.t + 8), `three seals: 48 px each, all on the border, none clipped: ${c2 && c2.seals.map(s => s.l.toFixed(0)).join(', ')}`);
    check(c2 && c2.seals[1].l > c2.seals[0].l + 12 && c2.seals[2].l > c2.seals[1].l + 12 && c2.seals[0].l >= (c2.nameBox ? c2.nameBox.r : 0) - 1, 'with three the seals close up but stay in order and clear of the name');
    if (shots) await page.locator('#owEngSec').screenshot({ path: path.join(shots, 'w1440-three-seals.png') }).catch(() => {});
    await close();
    // a second row of cards: the seal of a card in it hangs in the gap, not on the card above
    await page.setViewportSize({ width: 1440, height: 1500 }); await page.waitForTimeout(300);
    await open(`${R3}_${R3}1`);
    await page.evaluate(() => { OrderWin.selectPiece && OrderWin.selectPiece(null); });
    await page.waitForFunction(() => document.querySelectorAll('#owEng .engCard').length === 4, null, { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(600);
    const m3 = await measure(); num.m3 = m3;
    const row2 = m3.cards.filter(c => c.card.t > Math.min(...m3.cards.map(x => x.card.t)) + 40);
    const above = m3.cards.filter(c => !row2.includes(c));
    check(m3.cards.length === 4 && row2.length >= 1, 'four cards: a second row: ' + m3.cards.map(c => c.card.t.toFixed(0)).join(', '));
    const hitCards = row2.flatMap(c => c.seals.flatMap(s => above.filter(a => a.card.l < s.r && a.card.r > s.l && a.card.b > s.t && a.card.t < s.b).map(a => a.key)));
    check(hitCards.length === 0, 'a seal on a card of the second row does not reach the card above it (the row gap is ' + m3.gridGap + ')');
    check(m3.cards.every(c => c.seals.every(s => s.outsideHit)), 'none of the four cards\' seals is clipped');
    if (shots) { await page.screenshot({ path: path.join(shots, 'w1440-two-rows.png') }); }
    await close(); await page.setViewportSize({ width: 1440, height: 900 }); await page.waitForTimeout(300);

    // ════ 1024 px and 400 px ════
    for (const W of [1024, 400]) {
      await page.setViewportSize({ width: W, height: W === 400 ? 860 : 800 });
      await page.waitForTimeout(400);
      await open(`${R1}_${R1}1`);
      await page.evaluate(() => document.getElementById('owEngSec').scrollIntoView({ block: 'center' })); await page.waitForTimeout(400);
      const n = await measure(); num['m' + W] = n;
      if (shots) { await page.screenshot({ path: path.join(shots, `w${W}-order.png`) }); await page.locator('#orderWin .owHead').screenshot({ path: path.join(shots, `w${W}-head.png`) }); await page.locator('#owEngSec').screenshot({ path: path.join(shots, `w${W}-engraving.png`) }).catch(() => {}); if (await page.$('#owNowCard:not([hidden])')) await page.locator('#owNowCard').screenshot({ path: path.join(shots, `w${W}-banner.png`) }); }
      const k1 = n.cards.find(c => c.key === key(R1, 1));
      check(n.docW <= n.vw && n.headW <= n.headCW + 1 && n.dlgSW <= n.dlgCW + 1, `${W} px: the page and the header do not scroll sideways: doc ${n.docW}/${n.vw}, header ${n.headW}/${n.headCW}, window ${n.dlgSW}/${n.dlgCW}`);
      check(!n.ids.owSub && !n.ids.owNow && !n.ids.owMetal && n.title === 'Order ' + R1, `${W} px: no subtitle, no chips, the number stays`);
      if (n.railDisplay !== 'none') {
        check(n.stamps.length >= 5 && n.stamps.every(w => w >= 30) && n.stopBoxes.every(b => b.t >= n.railBox.t - 1 && b.b <= n.railBox.b + 1) && n.stopBoxes.every((b, i, a) => !i || near(b.t, a[0].t, 1)), `${W} px: the rail is one row with stamps of ${n.stamps[0].toFixed(1)} px (was ${OLD.stamp})`);
        check(n.railBox.l >= n.titleBox.r && n.railBox.r <= n.rightBox.l, `${W} px: the rail sits between the number and the controls`);
        check(!n.railLabelClipped, `${W} px: no step name is cut off`);
      } else console.log(`  · ${W} px: the rail is not drawn (as before; under 900 px the header is the number and the controls)`);
      check(k1 && k1.seals.length === 2 && k1.seals.every(s => s.w >= 39 && s.t < k1.card.t - 6 && s.b > k1.card.t + 6 && s.outsideHit), `${W} px: the card's seals (${k1 && k1.seals.map(s => s.w.toFixed(0)).join(', ')} px) still straddle its border and are not clipped`);
      check(k1 && k1.seals.every(s => s.r <= n.vw && s.l >= 0) && n.mainScroll && n.mainScroll.sw <= n.mainScroll.cw + 1, `${W} px: they stay inside the screen and make nothing scroll sideways: main ${n.mainScroll && n.mainScroll.sw}/${n.mainScroll && n.mainScroll.cw}`);
      check(n.now && n.now.seals === 0 && /Needs a decision/i.test(n.now.k), `${W} px: the banner draws no seal`);
      await close();
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    check(errors.length === 0, 'no console or page errors: ' + JSON.stringify(errors.slice(0, 3)));
    if (MEASURE) console.log('NUMBERS ' + JSON.stringify({ stamp1440: num.stamp1440, label1440: num.label1440, railZoom: num.railZoom, sealZoom: num.sealZoom, cardSeals: num.cardSeals, headH: num.m1440.headH, railW: num.m1440.railBox && num.m1440.railBox.w, w400: { headW: num.m400.headW, headCW: num.m400.headCW, docW: num.m400.docW, dlgSW: num.m400.dlgSW, dlgCW: num.m400.dlgCW, railDisplay: num.m400.railDisplay }, w1024: { stamp: num.m1024.stamps[0], label: num.m1024.labels[0], headW: num.m1024.headW, headCW: num.m1024.headCW } }));
  } finally { await browser.close(); await srv.close && srv.close(); }
  if (fails.length) { console.log('\n' + fails.length + ' check(s) failed'); process.exit(1); }
  console.log('\n  order-window-header: ok'); process.exit(0);
})().catch(e => { console.error(e); process.exit(1); });
