// The seals of a piece's milestones (window.PieceSeals, charm-nest-piece-seals.js; Paul, 5 Oct 2026, point 5: "If a given step requires a seal, then you can
// show an unfinished version of that seal ... visible but slightly transparent, so it's easy to understand that it's still missing; and on a solid dot, if a
// seal was present, it would be shown in the hover expanded view"). Runs in headless Chromium on the page itself (charm-nest-1.html over the fake site,
// bridge-server.cjs), with a fixture card standing where the dots' hover card will stand; nothing is written anywhere.
//  · a DONE step with a recorded seal shows the real seal (the timeline's own face, the same .seal element Seal.zoom grows) with its person, place and date/time,
//    never a ghost; a step recorded twice keeps its one seal, as the Timeline draws it; the Complete Order and QR label seals ('complete', 'print') are the card's own, one seal and "+N" for every press and reprint
//  · a NOT-DONE step that has a seal shows the unfinished seal: the same outline, words and icon as the real one, dashed, about half transparent, NO date, time or
//    person anywhere (text, aria-label), not a .seal, no tab stop, never zoomed by a rest, a click or the keyboard; it is not shown for a step that will never happen
//    (cancelled, completed by hand) and not at all for a step that has no seal in the app (a scan, packed) or a done step the timeline holds no event of
//  · Reopen and Undo never take a real seal away, and a real seal beats a ghost whatever `done` says
//  · a rest zooms the real seal in place, a click zooms it at once, a second click puts it back; reduced motion keeps all of it, with nothing animating on the ghost
//  · 1440, 900 and 390 px: the card's seal and line fit the card and the screen, the zoomed seal stays in view
//  · three mutants (a ghost shown for a done step, a ghost drawn at full strength, a ghost that is zoomable) are caught by the very same checks
//   SHOTS=<dir> node tests/charm-nest/piece-seals-ghost.cjs [playwright-core dir]
const fs = require('fs'), path = require('path');
const root = path.join(__dirname, '../..');
const { start } = require('./bridge-server.cjs');

const RID = '4171711853', KA = `${RID}_41717118531`, KB = `${RID}_41717118532`, KC = `${RID}_41717118533`;
const H = 36e5, T0 = Date.now() - 4 * 24 * H;
const ev = (type, h, x) => Object.assign({ id: `${RID}~${type}~${h}`, orderId: RID, type, at: T0 + h * H, by: 'Paul', source: 'sorter' }, x || {});
// piece A: nested, laser cut on its sheet, sorted twice (the second scan at the same station; the station names the line here, so piece B has not been sorted)   piece B: nested on another sheet, not cut   piece C: completed by hand
const EVENTS = [
  ev('arrived', 0, { source: 'etsy', by: 'Etsy' }),
  ev('placed', 5, { lineKey: KA, sheetId: 'shA', sheet: 'GF Sheet 1' }), ev('placed', 6, { lineKey: KB, sheetId: 'shB', sheet: 'RG Sheet 1' }), ev('placed', 6.5, { lineKey: KC, sheetId: 'shA', sheet: 'GF Sheet 1' }),
  ev('laserDone', 30, { sheetId: 'shA', sheet: 'GF Sheet 1', by: 'Marco R.', station: 'laser' }),
  ev('sorted', 40, { lineKey: KA, by: 'Ana P.', station: 'sorting', device: 'sort-1' }), ev('sorted', 44, { lineKey: KA, by: 'Ana P.', station: 'sorting', device: 'sort-1' }),
  ev('scan', 41, { by: 'Ana P.', station: 'welding' }),
  ev('sealCompleted', 50, { lineKey: KC, data: { how: 'button' }, text: 'Custom order completed' }),
  ev('sealPrinted', 51, { lineKey: KC, data: { how: 'print', prints: 1 }, text: 'Custom QR label printed' })];
const PIECES = [KA, KB, KC].map((key, i) => ({ key, tid: key.split('_')[1], qty: 1, pools: [`${key}_1`], sheets: [['shA'], ['shB'], ['shA']][i], line: { sku: 'S' + i }, name: 'P' + i }));
// a piece's custom record: three QR prints and a Complete Order press, then reopened (state open): every seal stays
const REC = { key: KC, state: 'open', how: 'button', completedAt: T0 + 50 * H, completedBy: 'Paul', prints: 3,
  stamps: [{ how: 'print', at: T0 + 49 * H, by: 'Paul' }, { how: 'button', at: T0 + 50 * H, by: 'Paul' }, { how: 'print', at: T0 + 51 * H, by: 'Rita' }, { how: 'print', at: T0 + 52 * H, by: 'Paul' }] };

(async () => {
  const pwDir = process.argv[2] || process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: not run'); return; }
  const shots = process.env.SHOTS || ''; if (shots) fs.mkdirSync(shots, { recursive: true });
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const fails = [];
  let quiet = false;
  const check = (ok, msg) => { if (!ok) fails.push(msg); if (!quiet) console.log((ok ? '  ✓ ' : '  ✗ ') + msg); return ok; };
  const open = async (w, h, reduced) => {
    const context = await browser.newContext({ viewport: { width: w, height: h }, deviceScaleFactor: shots ? 2 : 1, reducedMotion: reduced ? 'reduce' : 'no-preference' });
    await context.route(u => !/^http:\/\/(127\.0\.0\.1|localhost)[:/]/.test(u.href), r => /fonts\.googleapis|fonts\.gstatic/.test(r.request().url()) ? r.fulfill({ status: 200, contentType: 'text/css', body: '' }) : r.abort());
    await context.addInitScript(() => { try { localStorage.setItem('cn.settings', JSON.stringify({ v: 26, dsOrigin: 'http://127.0.0.1:9', runMode: 'manual', sound: 'off', notify: 'off', review: 'on' })); localStorage.setItem('cn.employee', 'Test Operator'); } catch (_) {} });
    const page = await context.newPage(); page.setDefaultTimeout(30000);
    const errors = []; page.on('pageerror', e => { errors.push(e.message); console.error('page error:', e.message); });
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`, { waitUntil: 'load' });
    await page.waitForFunction(() => window.PieceSeals && window.Seal && window.OrderTimelineUI && window.Motion, null, { timeout: 60000 });
    await page.evaluate(({ EVENTS, PIECES, REC }) => { window.__fx = { EVENTS, PIECES, REC };
      // a card like the dots' hover card: 236 px wide, a head, a slot for the seal
      window.__card = (title, ret, x, y) => { const d = document.createElement('div'); d.className = 'pgCardFx'; d.style.cssText = `position:fixed;left:${x}px;top:${y}px;z-index:50;box-sizing:border-box;width:min(236px,calc(100vw - 16px));padding:8px 11px 9px;border-radius:11px;background:#fffefb;border:1px solid #e4ddd0;font:11px/1.4 system-ui,sans-serif;color:#5b554c;box-shadow:0 10px 26px rgba(30,26,20,.13)`;
        d.innerHTML = `<div><b style="font:600 12px system-ui;color:#1c1a17">${title}</b></div><p style="margin:3px 0 6px">What this step says</p><div class="slot" style="min-height:48px"></div>`; document.body.appendChild(d); if (ret) d.querySelector('.slot').appendChild(ret); return d; };
      window.__ctx = (i, extra) => Object.assign({ events: __fx.EVENTS, piece: __fx.PIECES[i], pieces: __fx.PIECES }, extra || {});
    }, { EVENTS, PIECES, REC });
    return { context, page, errors };
  };
  const strip = p => p.evaluate(() => document.querySelectorAll('.pgCardFx').forEach(n => n.remove()));
  /** What a rendered step is, read off the page: the first element's facts. */
  const facts = (p, step, i, opts, extra) => p.evaluate(([step, i, opts, extra]) => {
    const el = PieceSeals.render(step, __ctx(i, extra), opts); if (!el) return null;
    const card = __card(step, el, 20, 20), seals = [...el.querySelectorAll('.seal')], g = el.querySelector('.pgGhost'), svg = (seals[0] || g || el).querySelector('svg');
    const model = svg ? JSON.parse(svg.getAttribute('data-seal-model')) : null, text = svg ? [...svg.querySelectorAll('text')].map(t => t.textContent).join(' ') : '';
    const op = svg ? [...svg.querySelectorAll('g[opacity]')].reduce((n, x) => n * parseFloat(x.getAttribute('opacity')), 1) : 1;
    let anc = 1; for (let n = (g || seals[0] || el); n && n !== card; n = n.parentElement) anc *= parseFloat(getComputedStyle(n).opacity);
    const r = (seals[0] || g || el).getBoundingClientRect(), cr = card.getBoundingClientRect(), wr = el.getBoundingClientRect();
    const out = { state: el.dataset.pgState, step: el.dataset.pgStep, count: +el.dataset.pgCount, seals: seals.length, ghosts: el.querySelectorAll('.pgGhost').length, plus: (el.querySelector('.sealPlus') || {}).textContent || '', caption: (el.querySelector('.pgBy') || {}).textContent || '',
      family: model && model.family, action: model && model.action, date: model && model.date, time: model && model.time, ghostModel: !!(model && model.ghost), text, opacity: op * anc, dashed: !!(svg && svg.querySelector('[data-seal-outline][stroke-dasharray]')),
      aria: (seals[0] || g || el).getAttribute('aria-label') || '', tab: (seals[0] || g || el).tabIndex, isSeal: (seals[0] || g || el).classList.contains('seal'), outline: svg && svg.querySelector('[data-seal-outline]') ? svg.querySelector('[data-seal-outline]').getAttribute('d') : '',
      fit: wr.left >= cr.left - .5 && wr.right <= cr.right + .5 && r.left >= 0 && r.right <= innerWidth && r.top >= 0 && r.bottom <= innerHeight, scroll: card.scrollWidth <= card.clientWidth + 1, w: r.width };
    card.remove(); return out;
  }, [step, i, opts || {}, extra || null]);

  /** The whole set of checks of the module on one page; `m` names the mutant it is run against (its checks are silent: the failures are what is counted). */
  async function suite(page) {
    const A = i => (step, o, x) => facts(page, step, i, o, x);
    const a = A(0), b = A(1), c = A(2);
    // ═══ 1 · a done step shows its real seal ═══
    const laser = await a('laser', { done: true });
    check(laser && laser.state === 'done' && laser.seals === 1 && laser.ghosts === 0 && laser.family === 'laser' && laser.action === 'LASER CUT', "a done step with a seal: Laser cut shows its real seal (the laser family's LASER CUT), no ghost");
    check(laser && !!laser.date && !!laser.time && /\d/.test(laser.date) && /\d/.test(laser.time) && !laser.ghostModel && laser.opacity > .9 && !laser.dashed, '…with its date and time on its face, drawn at full strength, not dashed');
    check(laser && /Marco R\./.test(laser.aria) && /GF Sheet 1/.test(laser.aria) && /\d{4}/.test(laser.aria) && /Marco R\./.test(laser.caption) && /GF Sheet 1/.test(laser.caption), "…its person (Marco R.) and place (GF Sheet 1) said in its label and in the line beside it, and its date in the label");
    check(laser && laser.isSeal && laser.tab === 0, '…it is a .seal with a tab stop: the one Seal.zoom grows');
    const sorted = await a('sorted', { done: true });
    check(sorted && sorted.seals === 1 && sorted.plus === '' && sorted.count === 2 && sorted.ghosts === 0, 'two sorting scans of one step are ONE seal (the timeline draws one per step and piece), both recorded');
    check(sorted && /Ana P\./.test(sorted.caption) && /Sorting/.test(sorted.caption) && /Ana P\./.test(sorted.aria) && /Sorting/.test(sorted.aria), '…who and where (Ana P., Sorting) are in its line and its label');
    const arrived = await a('arrived', { done: true });
    check(arrived && arrived.seals === 1 && arrived.family === 'received' && arrived.action === 'ORDER RECEIVED' && /Etsy/.test(arrived.aria), 'Order in: the ORDER RECEIVED seal, from Etsy');
    const nested = await b('sheet', { done: true });
    check(nested && nested.seals === 1 && nested.family === 'prepared' && /RG Sheet 1/.test(nested.aria), "Nested: piece B's own ON SHEET seal (its sheet, not piece A's)");
    const nocap = await a('laser', { done: true, caption: false });
    check(nocap && nocap.seals === 1 && nocap.caption === '', 'caption:false draws the seal alone');
    // the ghost's way of saying "a seal is there": the same outline, words, icon as the real one
    // ═══ 2 · a not-done step that has a seal shows the unfinished seal ═══
    const gl = await b('laser', { done: false });
    check(gl && gl.state === 'missing' && gl.ghosts === 1 && gl.seals === 0 && gl.count === 0, "a not-done step that has a seal: piece B's Laser cut shows the unfinished seal, no real seal");
    check(gl && laser && gl.family === laser.family && gl.action === laser.action && gl.outline === laser.outline, '…the same kind as the real one: the same family, words and outline');
    check(gl && gl.ghostModel && gl.dashed, '…dashed, and marked unfinished in its own model');
    check(gl && gl.opacity >= .25 && gl.opacity <= .6, `…slightly transparent (${gl ? gl.opacity.toFixed(2) : '–'} strength, between .25 and .6)`);
    check(gl && !/\d/.test(gl.text) && !/NOT RECORDED/i.test(gl.text) && !gl.date && !gl.time && !/\d/.test(gl.aria) && gl.caption === '', '…no date, no time and no person anywhere on it (its words are the seal\'s own, its label says "not stamped yet")');
    check(gl && !gl.isSeal && gl.tab < 0 && /not stamped yet/.test(gl.aria), '…not a .seal and not a tab stop');
    for (const [k, fam] of [['arrived', 'received'], ['sheet', 'prepared'], ['engraved', 'engraving'], ['laser', 'laser'], ['sorted', 'finishing'], ['welded', 'finishing'], ['assembled', 'finishing'], ['shipped', 'fulfilment']]) {
      const g = await page.evaluate(([k]) => { const e = PieceSeals.ghost(k, 36); if (!e) return null; const m = JSON.parse(e.querySelector('svg').getAttribute('data-seal-model')); return { family: m.family, ghost: !!m.ghost, seal: e.classList.contains('seal') }; }, [k]);
      check(g && g.family === fam && g.ghost && !g.seal, `ghost('${k}') draws the ${fam} seal, unfinished`);
    }
    const not = await page.evaluate(() => ({ none: PieceSeals.ghost('scan'), pack: PieceSeals.ghost('packed'), foo: PieceSeals.ghost('nonsense'), has: ['arrived', 'sheet', 'engraved', 'laser', 'sorted', 'welded', 'assembled', 'shipped', 'complete', 'print'].map(k => PieceSeals.has(k)), hasNot: ['scan', 'packed', 'labelPrinted', 'held', 'nonsense', ''].map(k => PieceSeals.has(k)) }));
    check(!not.none && !not.pack && !not.foo && not.has.every(Boolean) && not.hasNot.every(x => !x), 'a step that has no seal in the app (a scan, packed, a held note, a made-up name) has none: ghost() and render() give nothing, has() says no');
    // ═══ 3 · where no seal is shown ═══
    const nil = await page.evaluate(() => ({
      scan: PieceSeals.render('scan', __ctx(0), { done: false }), packed: PieceSeals.render('packed', __ctx(0), { done: true }),
      noEvent: PieceSeals.render('shipped', __ctx(0), { done: true }), noTimeline: PieceSeals.render('sorted', { piece: __fx.PIECES[0] }, { done: false }), noTimeline2: PieceSeals.render('sorted', __ctx(0, { events: null }), { done: false }),
      cancelled: PieceSeals.render('assembled', __ctx(1, { events: __fx.EVENTS.concat([{ id: 'x1', orderId: '1', type: 'cancelled', at: Date.now() - 36e5, by: 'Paul', source: 'sorter' }]), cancelled: { at: Date.now() - 36e5, by: 'Paul' } }), { done: false }),
      byHand: PieceSeals.render('assembled', __ctx(2), { done: false }), handSorted: PieceSeals.render('sorted', __ctx(2), { done: false }),
      noRec: PieceSeals.render('print', __ctx(0), { done: false }), noPrint: PieceSeals.render('complete', __ctx(1), { done: false })
    }));
    for (const [k, why] of [['scan', 'a step with no seal in the app'], ['packed', 'a step with no seal in the app, even when "done"'], ['noEvent', 'a done step the timeline holds no event of (nothing is invented)'], ['noTimeline', 'the timeline not read yet (no events given)'], ['noTimeline2', 'events that are not an array'],
      ['cancelled', 'a step a cancelled order will never take'], ['byHand', 'a step a piece completed by hand will never take'], ['noRec', "a Print QR label nobody pressed (not a step: no ghost)"], ['noPrint', "a Complete Order nobody pressed (not a step: no ghost)"]]) check(nil[k] === null, `nothing is shown for ${why}`);
    // ═══ 4 · the pieces completed by hand: their own two seals, "+N" for the reprints; Reopen takes nothing away ═══
    const comp = await c('complete', { done: true }, { rec: REC });
    check(comp && comp.seals === 1 && comp.ghosts === 0 && comp.action === 'ORDER COMPLETE' && comp.family === 'fulfilment', 'Complete Order: the card\'s own ORDER COMPLETE seal');
    check(comp && /^\+/.test(comp.plus) === false && comp.count >= 1, '…one Complete Order press, no "+N"');
    const prt = await c('print', { done: true }, { rec: REC });
    check(prt && prt.seals === 1 && prt.action === 'QR LABEL PRINTED' && prt.family === 'prepared' && prt.plus === '+2' && prt.count === 3, 'QR label printed three times: ONE seal and "+2" (every reprint adds its own)');
    check(comp && prt && comp.state === 'done' && prt.state === 'done', "…the record is open (reopened): its seals are all still there, Reopen and Undo take none away");
    const keep = await a('sorted', { done: false, size: 40 });
    check(keep && keep.state === 'done' && keep.seals === 1 && keep.ghosts === 0, 'a real seal beats a ghost: Sorted has its seal, so "not done" still shows the real seal and no ghost');
    const reopened = await page.evaluate(() => { const evs = __fx.EVENTS.concat([{ id: 'ro', orderId: '1', type: 'note', at: Date.now() - 36e5, by: 'Paul', lineKey: __fx.PIECES[2].key, data: { reopened: 'reopen' } }]); const e = PieceSeals.render('complete', __ctx(2, { events: evs }), { done: false }); return !!e && e.querySelectorAll('.seal').length === 1; });
    check(reopened, '…and a Reopen in the timeline after the press: the seal is still there (the timeline alone is enough)');
    // ═══ 4b · what the order window holds: OrderTimelineUI.summary's own item works as the ctx, and its D is left as it was ═══
    const viaSummary = await page.evaluate(() => {
      const sum = OrderTimelineUI.summary(__fx.EVENTS, __fx.PIECES, null), rows = [], keys = ['arrived', 'sheet', 'engraved', 'laser', 'sorted', 'welded', 'assembled', 'shipped'];
      for (let i = 0; i < 3; i++) for (const k of keys) {
        const x = sum.each[i], a = PieceSeals.render(k, Object.assign({}, x, { pieces: __fx.PIECES }), {}), b = PieceSeals.render(k, __ctx(i), {});
        rows.push([i, k, a ? a.dataset.pgState : null, b ? b.dataset.pgState : null]);
      }
      return { rows, railKept: sum.each.every(x => x.D.rail.every(r => typeof r.k === 'string')) };
    });
    check(viaSummary.rows.every(r => r[2] === r[3]) && viaSummary.railKept, 'summary().each[i] works as the ctx and gives the same as the plain ctx for every step of every piece; its D is not changed');
    check(viaSummary.rows.filter(r => r[0] === 0 && ['arrived', 'sheet', 'laser', 'sorted'].includes(r[1])).every(r => r[2] === 'done') && viaSummary.rows.find(r => r[0] === 1 && r[1] === 'laser')[2] === 'missing', "…piece A's first four steps are done (real seals), piece B's Laser cut is missing (a ghost)");
    // ═══ 5 · the card data ═══
    const inf = await page.evaluate(() => ({ b: PieceSeals.info('laser', __ctx(1)), a: PieceSeals.info('laser', __ctx(0)), none: PieceSeals.info('scan', __ctx(0)) }));
    check(inf.b && inf.b.state !== 'done' && inf.b.ghost === true && inf.b.seals.length === 0 && inf.b.need.length > 0, 'info(): the missing step says what is missing and that a ghost goes with it');
    check(inf.a && inf.a.state === 'done' && inf.a.ghost === false && inf.a.seals.length === 1 && inf.a.seals[0].person === 'Marco R.' && inf.a.seals[0].sheet === 'GF Sheet 1' && inf.a.seals[0].at > 0, 'info(): the done step says who, where and when from the same event as the seal');
    check(inf.none === null, 'info(): none for a step with no seal');
  }

  const sealAt = (p, sel) => p.evaluate(sel => { const s = document.querySelector(sel); if (!s) return null; const r = s.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2, w: r.width }; }, sel);
  const zoomK = p => p.evaluate(() => { const z = document.querySelector('[data-seal-zoom]'); return z ? +z.dataset.sealZoom : 0; });
  const rest = p => p.evaluate(() => Seal.zoom.DELAY);

  try {
    // ═══ the checks, on the page at 1440 ═══
    const first = await open(1440, 900, false), page = first.page;
    console.log('▸ the module at 1440');
    await suite(page);

    // ═══ zoom: the real seal grows in place, the ghost never does ═══
    console.log('▸ zoom');
    const DELAY = await rest(page);
    await page.evaluate(() => { __card('Laser cut · Done', PieceSeals.render('laser', __ctx(0), { done: true }), 300, 300); __card('Laser cut · To come', PieceSeals.render('laser', __ctx(1), { done: false }), 620, 300); });
    const realSel = '.pgCardFx .pgSeals[data-pg-state="done"] .seal', ghostSel = '.pgCardFx .pgGhost';
    const r1 = await sealAt(page, realSel), g1 = await sealAt(page, ghostSel);
    await page.mouse.move(5, 880); await page.mouse.move(g1.x, g1.y); await page.waitForTimeout(DELAY + 300);
    check(await zoomK(page) === 0, `the ghost: resting on it for ${DELAY + 300} ms zooms nothing`);
    await page.mouse.click(g1.x, g1.y); await page.waitForTimeout(450);
    check(await zoomK(page) === 0, 'the ghost: a click zooms nothing');
    await page.mouse.move(5, 880);
    check(await page.evaluate(() => { const g = document.querySelector('.pgGhost'); g.focus(); return g.tabIndex < 0 && document.activeElement !== g && !g.matches('[tabindex]'); }), 'the ghost: not a tab stop, the keyboard never lands on it');
    await page.mouse.move(5, 880); await page.waitForTimeout(400);
    // the real seal: a rest zooms it, in place; a click zooms it at once, a second puts it back
    await page.mouse.move(r1.x, r1.y); await page.waitForTimeout(DELAY - 150);
    check(await zoomK(page) === 0, 'the real seal: not yet zoomed before the rest is over');
    await page.waitForTimeout(500);
    const kRest = await zoomK(page);
    check(kRest > 1, `the real seal: a rest of ${DELAY} ms grows it in place (x${kRest})`);
    const box = await page.evaluate(sel => { const s = document.querySelector(sel), r = s.getBoundingClientRect(); return { cx: r.left + r.width / 2, cy: r.top + r.height / 2, w: r.width }; }, realSel);
    check(Math.abs(box.cx - r1.x) < 14 && Math.abs(box.cy - r1.y) < 14 && box.w > r1.w * 1.1, '…from where it stands (no copy, no card of its own)');
    await page.mouse.move(5, 880); await page.waitForTimeout(500);
    await page.mouse.click(r1.x, r1.y); await page.waitForTimeout(450);
    check(await zoomK(page) > 1, 'the real seal: a click zooms it at once');
    await page.mouse.click(r1.x, r1.y); await page.waitForTimeout(450);
    check(await zoomK(page) === 0, '…a second click puts it back');
    await page.mouse.move(5, 880); await page.waitForTimeout(300);
    // the keyboard: Tab onto the real seal grows it at once, Esc puts it back
    await page.focus(realSel); await page.keyboard.press('Shift+Tab'); await page.keyboard.press('Tab'); await page.waitForTimeout(450);
    check(await zoomK(page) > 1, 'the real seal: Tab onto it zooms it at once');
    await page.keyboard.press('Escape'); await page.waitForTimeout(450);
    check(await zoomK(page) === 0, '…and Esc puts it back'); await page.evaluate(() => document.activeElement && document.activeElement.blur());
    await strip(page);

    // ═══ screenshots of the fixture: a real seal in a card, a ghost seal in a card, 1440 ═══
    if (shots) {
      await page.evaluate(() => { __card('Laser cut · Done', PieceSeals.render('laser', __ctx(0), { done: true, size: 44 }), 60, 60); __card('Laser cut · Still to come', PieceSeals.render('laser', __ctx(1), { done: false, size: 44 }), 340, 60);
        __card('Sorted · Done', PieceSeals.render('sorted', __ctx(0), { done: true, size: 44 }), 60, 190); __card('Sorted · Still to come', PieceSeals.render('sorted', __ctx(1), { done: false, size: 44 }), 340, 190);
        __card('QR label printed · 3 prints', PieceSeals.render('print', __ctx(2), { done: true, size: 44 }, { rec: __fx.REC }) || PieceSeals.render('print', __ctx(2, { rec: __fx.REC }), { done: true, size: 44 }), 60, 320);
        __card('Order complete', PieceSeals.render('complete', __ctx(2, { rec: __fx.REC }), { done: true, size: 44 }), 340, 320); });
      await page.screenshot({ path: path.join(shots, '1-cards-1440.png'), clip: { x: 40, y: 40, width: 620, height: 460 } });
      await strip(page);
      // the ghost beside the real one, large, for the eye
      await page.evaluate(() => { const row = document.createElement('div'); row.className = 'pgCardFx'; row.style.cssText = 'position:fixed;left:60px;top:60px;z-index:50;display:flex;gap:40px;padding:24px 32px;background:#fffefb;border:1px solid #e4ddd0;border-radius:12px;align-items:center';
        for (const [k, i, d] of [['arrived', 0, true], ['sheet', 0, true], ['laser', 0, true], ['sorted', 0, true], ['shipped', 0, false]]) { const e = PieceSeals.render(k, __ctx(i), { done: d, size: 72, caption: false }) || PieceSeals.ghost(k, 72); row.appendChild(e); }
        document.body.appendChild(row); });
      await page.screenshot({ path: path.join(shots, '2-seals-big-1440.png'), clip: { x: 40, y: 40, width: 560, height: 160 } });
      await page.evaluate(() => { document.querySelector('.pgCardFx').remove(); const row = document.createElement('div'); row.className = 'pgCardFx'; row.style.cssText = 'position:fixed;left:60px;top:60px;z-index:50;display:flex;gap:40px;padding:24px 32px;background:#fffefb;border:1px solid #e4ddd0;border-radius:12px;align-items:center';
        for (const k of ['arrived', 'sheet', 'engraved', 'laser', 'sorted', 'welded', 'assembled', 'shipped']) row.appendChild(PieceSeals.ghost(k, 56)); document.body.appendChild(row); });
      await page.screenshot({ path: path.join(shots, '3-ghosts-all-steps-1440.png'), clip: { x: 40, y: 40, width: 860, height: 150 } });
      await strip(page);
    }

    // ═══ the three mutants: the same checks must fail on each ═══
    console.log('▸ mutants (each must be caught)');
    const mutate = async (name, patch) => {
      const m = await open(1440, 900, false);
      await m.page.evaluate(patch);
      const before = fails.length; quiet = true;
      try { await suite(m.page); } catch (e) { fails.push('mutant crashed: ' + e.message); }
      const caught = fails.length - before; fails.length = before; quiet = false;
      check(caught > 0, `mutant "${name}" is caught (${caught} check${caught === 1 ? '' : 's'} failed)`);
      await m.context.close();
    };
    await mutate('a ghost for a done step', () => { const real = PieceSeals.render; PieceSeals.render = (k, c, o) => { const x = real(k, c, o); return x && x.dataset.pgState === 'done' ? PieceSeals.ghost(k, (o && o.size) || 40) : x; }; });
    await mutate('a ghost drawn at full strength', () => { const f = Seal.face; Seal.face = (m, o) => f(m, o).replace(/ opacity="\.55"/g, ''); });
    await mutate('a ghost that is a zoomable .seal', () => { const g = PieceSeals.ghost; PieceSeals.ghost = (k, s) => { const e = g(k, s); if (e) { e.classList.add('seal'); e.tabIndex = 0; } return e; }; const r = PieceSeals.render; PieceSeals.render = (k, c, o) => { const x = r(k, c, o); const e = x && x.querySelector('.pgGhost'); if (e) { e.classList.add('seal'); e.tabIndex = 0; } return x; }; });
    await mutate('a ghost with the date and time written on it', () => { const f = Seal.face; Seal.face = (m, o) => f(Object.assign({}, m, { ghost: false, date: '05 OCT 2026', time: '12:41 PM' }), Object.assign({}, o, { ghost: false })).replace('<g fill', '<g opacity=".55" fill'); });

    // ═══ the widths: 900 and 390, and reduced motion at 1440 ═══
    for (const [w, h] of [[900, 820], [390, 780]]) {
      console.log(`▸ ${w} px`);
      const m = await open(w, h, false), p = m.page;
      for (const [step, i, o, extra, label] of [['laser', 0, { done: true, size: 44 }, null, 'a real seal'], ['sorted', 0, { done: true, size: 44 }, null, 'a real seal of a twice-recorded step'], ['laser', 1, { done: false, size: 44 }, null, 'a ghost'], ['print', 2, { done: true, size: 44 }, { rec: REC }, 'a seal and "+2"']]) {
        const f = await facts(p, step, i, o, extra);
        check(f && f.fit && f.scroll, `${w}: ${label} fits its 236 px card and the screen`);
      }
      // a card against the right edge of the narrowest screen, the real seal zoomed: it stays in view
      await p.evaluate(() => __card('Laser cut · Done', PieceSeals.render('laser', __ctx(0), { done: true, size: 44 }), Math.max(8, innerWidth - 8 - Math.min(236, innerWidth - 16)), 120));
      const s = await sealAt(p, '.pgCardFx .pgSeals .seal'); await p.mouse.click(s.x, s.y); await p.waitForTimeout(450);
      const vis = await p.evaluate(() => { const z = document.querySelector('[data-seal-zoom]'); if (!z) return null; const r = z.getBoundingClientRect(); return { in: r.left >= 0 && r.right <= innerWidth && r.top >= 0 && r.bottom <= innerHeight, w: r.width }; });
      check(vis && vis.in && vis.w > 44, `${w}: the zoomed real seal stays inside the screen (${vis ? Math.round(vis.w) : '–'} px)`);
      await p.mouse.click(5, h - 5); await p.waitForTimeout(300);
      if (shots) {
        await strip(p);
        await p.evaluate(() => { __card('Laser cut · Done', PieceSeals.render('laser', __ctx(0), { done: true, size: 44 }), 8, 20); __card('Laser cut · Still to come', PieceSeals.render('laser', __ctx(1), { done: false, size: 44 }), 8, 150); });
        await p.screenshot({ path: path.join(shots, `4-cards-${w}.png`), clip: { x: 0, y: 0, width: Math.min(w, 400), height: 290 } });
      }
      check(m.errors.length === 0, `${w}: no page errors`);
      await m.context.close();
    }
    {
      console.log('▸ reduced motion');
      const m = await open(1440, 900, true), p = m.page;
      await p.evaluate(() => { __card('Laser cut · Done', PieceSeals.render('laser', __ctx(0), { done: true }), 300, 300); __card('Laser cut · To come', PieceSeals.render('laser', __ctx(1), { done: false }), 620, 300); });
      const anim = await p.evaluate(() => { const g = document.querySelector('.pgGhost'), cs = getComputedStyle(g); return { anims: g.getAnimations({ subtree: true }).length, tr: cs.transitionDuration, an: cs.animationName }; });
      check(anim.anims === 0 && /^0s(, 0s)*$/.test(anim.tr) && anim.an === 'none', 'reduced motion: nothing animates or transitions on the ghost');
      const r = await sealAt(p, '.pgCardFx .pgSeals[data-pg-state="done"] .seal'), g = await sealAt(p, '.pgCardFx .pgGhost');
      await p.mouse.click(g.x, g.y); await p.waitForTimeout(400);
      check(await zoomK(p) === 0, 'reduced motion: the ghost still does not zoom');
      await p.mouse.click(r.x, r.y); await p.waitForTimeout(400);
      check(await zoomK(p) > 1, 'reduced motion: a click still zooms the real seal');
      check(m.errors.length === 0, 'reduced motion: no page errors');
      await m.context.close();
    }
    check(first.errors.length === 0, 'no page errors at 1440');
    await first.context.close();
  } catch (e) { fails.push('crash: ' + (e.stack || e.message)); console.error(e); }
  await browser.close(); srv.close && srv.close();
  console.log(fails.length ? `\n${fails.length} failed:\n - ` + fails.join('\n - ') : '\nall passed');
  process.exit(fails.length ? 1 : 0);
})();
