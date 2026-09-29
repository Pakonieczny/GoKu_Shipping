// The sheet window's "Open order" (Paul, 28 Sep 23:38): the clicked charm's order panel has a small ghost button on the
// order's heading row (no new bar, no height) that hands the window over to the order view — never one window over
// another. Checked in headless Chromium on the fake site (bridge-server.cjs), a sheet window grown out of a card:
//   · the button sits on the number's row and adds no height; Enter on it opens the order view on its Overview, on this
//     charm's line, grown out of the order panel (its clip starts on the panel), while the window's plate and header fall
//     back and fade underneath (transform and opacity only) and its backdrop hands over;
//   · during the flight nothing in the window is drawn again and no mark loop runs; frames and long tasks are measured
//     (gated only on a quiet machine);
//   · Esc in the order view shrinks it back into the panel as the window comes up in step, as it was: the same charm, the
//     same scroll, a Hold / Cancel choice with its typed note still open, the keyboard back on the button; nothing of
//     either flight is left;
//   · after Previous / Next or another order in the view, it still goes back into the panel;
//   · the chain goes on: the window then closes back into the card it grew from.
// Screenshots (one mid-transition) go to SHOTS (default /mnt/project-files/plans/open-order) when that folder exists.
//   node tests/charm-nest/sheetwin-open-order.cjs   (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const path = require('path'), fs = require('fs'), os = require('os'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const DAY = 86400, SHIP = Math.floor(Date.UTC(2026, 9, 5, 17) / 1000);
const S = { rid: '4172131078', tid: '41721310781', sku: 'FIREBIRD_2' };
const T = { rid: '4172131099', tid: '41721310991', sku: 'TINY_TAG' };
const SH = 'sheet-open-order', PS = `${S.rid}_${S.tid}_1`, PT = `${T.rid}_${T.tid}_1`;
const SHOTS = process.env.SHOTS || '/mnt/project-files/plans/open-order';
const line = (tid, sku) => ({ transactionId: tid, listingId: '18000' + tid.slice(-5), sku, title: sku.replace(/_/g, ' ') + ' charm', quantity: 1, expectedShipDate: SHIP, variations: [{ name: 'Metal', value: '14k Gold Filled' }], metalKey: 'gold', metalLabel: '14k Gold Filled', personalization: [], buyerMessage: '' });
const order = (rid, buyer, lines) => ({ receiptId: rid, orderNumber: rid, createTs: SHIP - 7 * DAY, updateTs: SHIP - 7 * DAY + 60, shipBy: SHIP, buyer: { name: buyer }, buyerMessage: '', isGift: false, giftMessage: '', staffNote: '', messages: [], lines });

function seed(st) {
  const pl = [], ch = [];
  for (let i = 0; i < 40; i++) {
    const id = 'b' + i, rid = i === 12 ? S.rid : i === 13 ? T.rid : String(4178100000 + i);
    pl.push({ id, cxPt: 14 + (i % 10) * 26, cyPt: 14 + Math.floor(i / 10) * 26, angle: 0, wPt: 20, hPt: 20 });
    ch.push({ id, name: `${rid} · SKU${i}`, poolId: i === 12 ? PS : i === 13 ? PT : `${rid}_${rid}1_1`, order: rid, sku: i === 12 ? S.sku : i === 13 ? T.sku : 'SKU' + i });
  }
  st.put('Charm_Nest_Sheets', SH, { id: SH, metal: 'gold', sheetIndex: 1, setSeq: 1, day: '2026-09-28', status: 'written', fileBase: 'GF_Sep.28.26_Set-1_Sheet-1', folder: 'GF_Sep.28.26_Set-1_Sheet-1', stock: { wPt: 270, hPt: 110 }, orders: [...new Set(ch.map(c => c.order))], placements: pl, charms: ch, poolIds: ch.map(c => c.poolId) });
  for (const [x, p] of [[S, PS], [T, PT]]) st.put('Charm_Pool', p, { poolId: p, orderId: x.rid, transactionId: x.tid, lineKey: `${x.rid}_${x.tid}`, sku: x.sku, material: 'gold', copy: 1, quantity: 1, state: 'written', sheetId: SH, sheetName: 'GF_Sep.28.26_Set-1_Sheet-1', updatedAt: Date.now() });
}

async function main() {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const srv = await start({ receipts: [] }); seed(srv.st);
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const load = () => os.loadavg()[0] / os.cpus().length;
  try {
    const context = await browser.newContext({ viewport: { width: 1500, height: 900 } });
    // nothing leaves the machine
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.route(/\/\.netlify\/functions\/etsyMailOrderLink/, r => r.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ engagements: [], active: null, conversation: null, ok: true, n: 0 }) }));
    await context.addInitScript(() => {
      try { if (!sessionStorage.getItem('__seeded')) { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); localStorage.setItem('cn.mail.station', JSON.stringify('k-oo-test')); sessionStorage.setItem('__seeded', '1'); } } catch (_) {}
      window.prompt = () => 'Test Operator';
      window.__lt = []; try { new PerformanceObserver(l => { for (const e of l.getEntries()) window.__lt.push({ s: e.startTime, d: e.duration }); }).observe({ type: 'longtask', buffered: true }); } catch (_) {}
      // frames, long tasks and what changes inside the sheet window while it flies
      window.__m = {
        begin() { const m = this, gen = m.gen = (m.gen || 0) + 1; m.fr = []; m.on = true; m.t0 = performance.now(); m.mut = []; m.raf = []; m.op = []; m.clip = []; m.kt = Infinity;
          addEventListener('keydown', () => { m.kt = performance.now(); }, { capture: true, once: true });
          const sw = SheetWin._W, ow = document.getElementById('orderWin');
          const loop = t => { if (!m.on || m.gen !== gen) return; m.fr.push(t); m.raf.push(sw.raf); m.op.push(+getComputedStyle(sw.dlg).opacity); m.clip.push(getComputedStyle(ow).clipPath); requestAnimationFrame(loop); }; requestAnimationFrame(loop);
          // (what the click itself does before the first frame is not the flight; the messages, not rendered while the
          // window is away, are drawn by the mail modules as the order view reads the same threads)
          m.mo = new MutationObserver(list => { if (!m.fr.some(t => t > m.kt)) return; for (const x of list) { const n = x.target.nodeType === 1 ? x.target : x.target.parentElement; const box = n.closest && n.closest('[data-r2=msgs]'); if (box && getComputedStyle(box).contentVisibility === 'hidden') continue; if (x.type === 'attributes' && x.attributeName === 'style' && (n === sw.dlg || n === sw.el.stage || n === sw.el.headBar)) continue; if (x.type === 'attributes' && x.attributeName === 'class' && n === sw.dlg) continue; m.mut.push({ t: Math.round(performance.now() - m.t0), type: x.type, where: (n.getAttribute && (n.getAttribute('data-r') || n.getAttribute('data-r2')) || n.className || n.tagName).toString().slice(0, 40), attr: x.attributeName }); } });
          m.mo.observe(sw.dlg, { subtree: true, childList: true, characterData: true, attributes: true }); },
        end(fly) { const m = this; m.on = false; m.mo.disconnect();
          const a = m.fr[0] || m.t0, b = a + fly, fr = m.fr.filter(t => t <= b + 17), dt = fr.slice(1).map((t, i) => t - fr[i]);
          const lt = window.__lt.filter(x => x.s + x.d > a + 1 && x.s < b);
          return { dts: dt.map(Math.round), frames: fr.length, maxDt: Math.round(Math.max(0, ...dt)), over34: dt.filter(x => x > 34).length, lt: lt.length, ltMax: Math.round(Math.max(0, ...lt.map(x => x.d))), mut: m.mut.filter(x => x.t < fly), raf: m.raf.slice(0, Math.max(1, fr.length)), op: m.op, clip: m.clip }; }
      };
    });
    const page = await context.newPage(), errors = [];
    page.setDefaultTimeout(20000);
    page.on('pageerror', e => { errors.push(e.message); console.error('page error:', String(e.stack || e.message).split('\n').slice(0, 4).join(' | ')); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.CN && window.Orders && window.OrderWin && window.SheetWin && CN.S.cloud.ok === true, null, { timeout: 60000 });
    await page.evaluate(async ({ orders }) => {
      await Orders.loadMaps(true);
      for (const [o, pool] of orders) for (const l of o.lines) { const key = CharmNestOrders.lineKey(o, l); const row = { key, order: o, line: l, arrivedAt: Date.now(), spec: null, problems: [], state: 'pooled', reason: null, claimedBy: null, poolIds: [pool], engrave: null, material: null }; B.orders.rows.push(row); B.orders.byKey.set(key, row); }
      Orders.interpretAll();
      // a card on the page for the window to grow out of and go back into
      const card = document.createElement('div'); card.id = '__card'; Object.assign(card.style, { position: 'fixed', left: '40px', top: '120px', width: '360px', height: '150px', background: '#f6f1e6', borderRadius: '12px', zIndex: 1 }); document.body.appendChild(card);
    }, { orders: [[order(S.rid, 'Cari Moll', [line(S.tid, S.sku)]), PS], [order(T.rid, 'Mia Lund', [line(T.tid, T.sku)]), PT]] });
    await page.waitForTimeout(600);

    // the window, grown out of the card, lands on the charm
    await page.evaluate(({ sh, ps }) => { const c = document.getElementById('__card'); SheetWin.open(sh, { select: ps, origin: { tint: '', stock: { wPt: 270, hPt: 110 }, rects: () => { const r = c.getBoundingClientRect(); return { box: r, sheet: r }; } } }); }, { sh: SH, ps: PS });
    assert(await page.evaluate(() => SheetWin._W.dlg.classList.contains('swGrow')), 'the window grows out of the card');
    await page.waitForFunction(ps => { const W = SheetWin._W; return W.sel && W.sel.poolId === ps && W.view === 'piece' && !W.flip && !W.flying; }, PS);
    await page.waitForTimeout(1500);
    const btn = await page.evaluate(() => {
      const W = SheetWin._W, d = W.el.detail, b = d.querySelector('.swOrderHead .swOrdTop [data-r2=openOrd]'), rid = d.querySelector('.swOrdTop .rid');
      if (!b) return null;
      const hb = b.getBoundingClientRect(), hr = rid.getBoundingClientRect(), top = d.querySelector('.swOrdTop').getBoundingClientRect();
      return { text: b.textContent.trim(), cls: b.className, icon: !!b.querySelector('svg'), bh: hb.height, rh: hr.height, th: top.height, sameRow: Math.abs((hb.top + hb.bottom) / 2 - (hr.top + hr.bottom) / 2) < 3 };
    });
    assert(btn, 'the order panel has an Open order button on its heading row');
    assert(btn.text === 'Open order' && /\bbtn\b/.test(btn.cls) && /\bghost\b/.test(btn.cls) && btn.icon, 'a small ghost button, "Open order" with its icon: ' + JSON.stringify(btn));
    assert(btn.sameRow && btn.bh <= btn.rh && Math.abs(btn.th - btn.rh) < 1, 'on the number\'s row, adding no height: ' + JSON.stringify(btn));

    // state to come back to: a Hold / Cancel choice open with a note typed, the panel scrolled
    await page.click('.swOffBtn');
    await page.click('[data-then=cancel]');
    await page.fill('.swOff .swNote', 'keep me');
    const before = await page.evaluate(() => { const W = SheetWin._W, s = W.el.pieceScroll; s.scrollTop = Math.min(60, s.scrollHeight - s.clientHeight); return { scroll: s.scrollTop, sel: W.sel.poolId, face: W.face, side: W.el.side.getBoundingClientRect().toJSON() }; });

    // Enter on the button: the order view grows out of the panel
    await page.waitForTimeout(400);
    // (focused first: focusing scrolls the button into view; the panel is scrolled after, as the keyboard leaves it)
    await page.focus('[data-r2=openOrd]');
    before.scroll = await page.evaluate(() => { const s = SheetWin._W.el.pieceScroll; s.scrollTop = Math.min(40, s.scrollHeight - s.clientHeight); return s.scrollTop; });
    assert(before.scroll > 0, 'the panel is scrolled');
    const l0 = load();
    await page.evaluate(() => window.__m.begin());
    await page.keyboard.press('Enter');
    const first = await page.evaluate(() => {
      const ow = document.getElementById('orderWin'), sw = SheetWin._W, kf = a => a.effect.getKeyframes();
      const clipA = ow.getAnimations().find(a => kf(a).some(k => k.clipPath));
      const mine = [sw.dlg, sw.el.stage, sw.el.headBar].flatMap(n => n.getAnimations().filter(a => !a.animationName).map(a => ({ n: n === sw.dlg ? 'dlg' : n.dataset.r, ms: a.effect.getTiming().duration, props: [...new Set(kf(a).flatMap(k => Object.keys(k).filter(p => !['offset', 'easing', 'composite', 'computedOffset'].includes(p))))] })));
      return { open: OrderWin.isOpen(), clip0: clipA ? kf(clipA)[0].clipPath : '', growMs: clipA ? clipA.effect.getTiming().duration : 0, mine, away: sw.dlg.classList.contains('swAway'), swOpen: sw.dlg.open };
    });
    const insets = r => [r.top, 1500 - r.right, 900 - r.bottom, r.left].map(v => Math.round(Math.max(0, v)));
    assert(first.open && first.swOpen && first.away, 'the order view opens and the window stays open underneath, out of sight: ' + JSON.stringify(first));
    const c0 = (first.clip0.match(/-?[\d.]+px/g) || []).slice(0, 4).map(v => Math.round(parseFloat(v)));
    assert.deepEqual(c0, insets(before.side), 'the order view grows out of the order panel: ' + first.clip0);
    assert(first.growMs === 650, 'over the view\'s 650 ms');
    assert(first.mine.length === 2 && first.mine.every(a => a.ms === 650 && a.n !== 'dlg'), 'the plate and header fall back in the same 650 ms: ' + JSON.stringify(first.mine));
    assert.deepEqual([...new Set(first.mine.flatMap(a => a.props))].sort(), ['opacity', 'transform'], 'only opacity and transform move in the window');
    // one frame mid-way, held, for the eye
    await page.waitForTimeout(240);
    const mid = await page.evaluate(() => { const sw = SheetWin._W; return { op: +getComputedStyle(sw.el.stage).opacity, stage: getComputedStyle(sw.el.stage).transform, side: getComputedStyle(sw.el.side).transform, clip: getComputedStyle(document.getElementById('orderWin')).clipPath }; });
    assert(mid.op > 0 && mid.op < 1 && mid.stage !== 'none' && mid.side === 'none' && /inset/.test(mid.clip), 'mid-way: the window fades and falls back, the panel itself still, the view growing: ' + JSON.stringify(mid));
    await page.waitForTimeout(700);
    const flyOpen = await page.evaluate(() => window.__m.end(650));
    const l1 = load();

    const landed = await page.evaluate(ps => { const sw = SheetWin._W; return { view: OrderWin.view(), key: OrderWin.key(), op: +getComputedStyle(sw.dlg).opacity, side: sw.el.side.getBoundingClientRect().toJSON(), willChange: [sw.dlg, sw.el.stage, sw.el.headBar].map(n => n.style.willChange).join(''), sel: sw.sel && sw.sel.poolId === ps }; }, PS);
    assert(landed.view === 'info' && landed.key === `${S.rid}_${S.tid}`, 'on its Overview, on this charm\'s line: ' + JSON.stringify(landed));
    assert(landed.op === 0 && landed.sel && !landed.willChange, 'the window is out of sight, its charm still chosen, nothing left promoted: ' + JSON.stringify(landed));
    assert.deepEqual(flyOpen.mut.map(x => `${x.type}:${x.where}${x.attr ? '@' + x.attr : ''}`), [], 'nothing in the window is drawn again while it flies');
    assert(flyOpen.raf.every(r => !r), 'no mark loop runs on the plate while it flies');

    // Esc: the view shrinks back into the panel as the window comes up in step
    await page.evaluate(() => window.__m.begin());
    await page.keyboard.press('Escape');
    await page.waitForTimeout(120);
    const back = await page.evaluate(() => { const sw = SheetWin._W, ow = document.getElementById('orderWin'), kf = a => a.effect.getKeyframes(); const a = ow.getAnimations().find(x => kf(x).some(k => k.clipPath)); return { end: a ? kf(a).slice(-1)[0].clipPath : '', ms: a ? a.effect.getTiming().duration : 0, ret: sw.dlg.classList.contains('swReturn') && getComputedStyle(sw.dlg).opacity === '1', mine: [sw.el.stage, sw.el.headBar].flatMap(n => n.getAnimations().map(x => x.effect.getTiming().duration)) }; });
    await page.waitForTimeout(700);
    const flyBack = await page.evaluate(() => window.__m.end(420));
    const c1 = (back.end.match(/-?[\d.]+px/g) || []).slice(0, 4).map(v => Math.round(parseFloat(v)));
    assert.deepEqual(c1, insets(before.side), 'Esc sends the view back into the panel: ' + back.end);
    assert(back.ret && back.mine.includes(420) && back.ms === 420, 'the window comes up in step with it (420 ms): ' + JSON.stringify(back));
    const after = await page.evaluate(() => {
      const sw = SheetWin._W, d = sw.dlg, E = sw.el, off = E.detail.querySelector('.swOff');
      return { owOpen: OrderWin.isOpen(), open: d.open, cls: d.className, op: getComputedStyle(d).opacity, anims: [d, E.stage, E.headBar].map(n => n.getAnimations().filter(a => !a.animationName).length), tf: [E.stage, E.headBar].map(n => getComputedStyle(n).transform + '|' + n.style.transformOrigin),
        sel: sw.sel && sw.sel.poolId, view: sw.view, face: sw.face, scroll: E.pieceScroll.scrollTop, note: off && off.querySelector('.swNote').value, then: off && off.querySelector('[data-then=cancel]').getAttribute('aria-checked'), focus: document.activeElement && document.activeElement.dataset.r2 };
    });
    assert(!after.owOpen && after.open && !/swAway|swReturn/.test(after.cls) && after.op === '1', 'the order view is gone and the window is back: ' + JSON.stringify(after));
    assert.deepEqual(after.anims, [0, 0, 0], 'nothing of either flight is left');
    assert(after.tf.every(t => t === 'none|'), 'the plate and header are in place: ' + after.tf);
    assert(after.sel === before.sel && after.view === 'piece' && after.face === before.face && after.scroll === before.scroll, 'the same charm, the same side, the same scroll: ' + JSON.stringify({ before, after }));
    assert(after.note === 'keep me' && after.then === 'true', 'the Hold / Cancel choice and its note are still there: ' + JSON.stringify(after));
    assert.equal(after.focus, 'openOrd', 'the keyboard is back on the button');
    assert.deepEqual(flyBack.mut.filter(x => !/^(swOff|swNote)/.test(x.where)).map(x => `${x.type}:${x.where}`), [], 'nothing in the window is drawn again as it comes back');

    // another order in the view: it still goes back into the panel
    await page.click('[data-r2=openOrd]'); await page.waitForTimeout(900);
    await page.evaluate(rid => OrderWin.openOrder(rid, { keepFrom: true }), T.rid); await page.waitForTimeout(500);
    const other = await page.evaluate(() => OrderWin.key());
    // (not awaited: the close resolves once the view has gone)
    await page.evaluate(() => { OrderWin.close(); }); await page.waitForTimeout(100);
    const back2 = await page.evaluate(() => { const ow = document.getElementById('orderWin'), kf = a => a.effect.getKeyframes(); const a = ow.getAnimations().find(x => kf(x).some(k => k.clipPath)); return a ? kf(a).slice(-1)[0].clipPath : ''; });
    await page.waitForTimeout(700);
    assert.equal(other, `${T.rid}_${T.tid}`, 'the view moved to another order');
    assert.deepEqual((back2.match(/-?[\d.]+px/g) || []).slice(0, 4).map(v => Math.round(parseFloat(v))), insets(before.side), 'and still goes back into the panel: ' + back2);
    assert(await page.evaluate(() => SheetWin._W.dlg.open && getComputedStyle(SheetWin._W.dlg).opacity === '1' && !OrderWin.isOpen()), 'the window is back');

    // screenshots: the window, then one frame of the hand-off held mid-way
    if (fs.existsSync(SHOTS)) {
      await page.screenshot({ path: path.join(SHOTS, 'a-sheet-window.png') });
      await page.click('[data-r2=openOrd]');
      await page.evaluate(() => { for (const a of document.getAnimations()) { a.pause(); a.currentTime = 260; } });
      await page.waitForTimeout(80);
      await page.screenshot({ path: path.join(SHOTS, 'a-mid-transition.png') });
      await page.evaluate(() => { for (const a of document.getAnimations()) a.play(); });
      await page.waitForTimeout(900);
      await page.screenshot({ path: path.join(SHOTS, 'a-order-view.png') });
      await page.keyboard.press('Escape'); await page.waitForTimeout(800);
    }

    // the same grow of the order view out of the panel with nothing handed over (the window left as it is under it): what
    // the view's own flight costs on this machine, printed beside the hand-off's
    await page.evaluate(() => window.__m.begin());
    await page.evaluate(rid => { OrderWin.openOrder(rid, { from: SheetWin._W.el.side, view: 'info' }); }, S.rid);
    await page.waitForTimeout(900);
    const flyBare = await page.evaluate(() => window.__m.end(650));
    await page.evaluate(() => { OrderWin.close(); }); await page.waitForTimeout(700);

    // the chain goes on: the window closes back into the card it grew from
    await page.evaluate(() => { SheetWin.close(); }); await page.waitForTimeout(60);
    const home = await page.evaluate(() => SheetWin._W.el.plate.getAnimations().some(a => a.effect.getKeyframes().some(k => k.transform && k.transform !== 'none')));
    await page.waitForTimeout(900);
    assert(home, 'then the window flies back into its card');
    assert(await page.evaluate(() => !SheetWin.isOpen() && !OrderWin.isOpen()), 'and closes');
    assert.deepEqual(errors, [], 'no page errors');

    const brief = m => `max frame ${m.maxDt} ms · ${m.over34} frame(s) > 34 ms · ${m.lt} long task(s), max ${m.ltMax} ms` + (process.env.DEBUG ? `  [${m.dts.join(' ')}]` : '');
    console.log(`  hand-off ${brief(flyOpen)}\n  back     ${brief(flyBack)}\n  view alone (no hand-off) ${brief(flyBare)}`);
    const busy = Math.max(l0, l1);
    // (gated where the order view's own flight is smooth: in a software-drawn headless Chromium it is not, by itself)
    if (busy < 1.2 && flyBare.over34 <= 2) { for (const m of [flyOpen, flyBack]) { assert(m.over34 <= 2, 'at most two frames over 34 ms: ' + brief(m)); assert(m.ltMax <= 50, 'no long task over 50 ms while it flies: ' + brief(m)); } console.log(`  ✓ frames and long tasks held (load per core ${busy.toFixed(2)})`); }
    else console.log(`  – load per core ${busy.toFixed(2)}, the order view alone ${flyBare.over34} frame(s) > 34 ms: frame thresholds not gated (numbers above are for comparison)`);
  } finally { await browser.close(); srv.close(); }
}
main().then(() => console.log('sheetwin-open-order: ok'), e => { console.error(e); process.exit(1); });
