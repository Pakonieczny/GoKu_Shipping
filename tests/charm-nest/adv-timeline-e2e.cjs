// End-to-end check of the rebuilt order timeline (Paul, 28 Sep 21:18, points 1-6, and 21:26: "make sure it's 100%"),
// in the real sorter page with the fake site (bridge-server.cjs), every other request aborted:
//  1 · the server's rail, its keys and whereOf agree with the page's copy;
//  2 · a single necklace: nothing drawn before the order came in (a read recorded two days early sits at the arrival),
//      seals only for milestones, in time order, no Welded, a dashed step for each step to come;
//  3 · every rail step, done or to come, and every dashed stamp shows its explainer card under the dot; a done step's
//      seal zooms ABOVE its dot, inside the view, and the card stays while the pointer moves onto it;
//  4 · three pieces, one a stud earring and one engraved: "all" says the next step is the one the order waits on (never a
//      step every piece that takes it has passed), and each piece shows only its own steps and stamps;
//  5 · cancelled then restored, removed and held then released: the person's seals are there, the rail is not cancelled;
//  6 · 2000+ recorded steps draw in time, and the hover animations run on transform/opacity only, frames under 34 ms.
//   node tests/charm-nest/adv-timeline-e2e.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, CN_SHOTS=dir)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

// ── 1 · the server's rail and the page's copy ──
{
  const win = { document: { getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } } };
  win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'), win);
  const UI = win.OrderTimelineUI;
  assert.deepEqual(JSON.parse(JSON.stringify(UI.STAGES.map(s => s.l))), Timeline.RAIL, 'the rail\'s names');
  assert.deepEqual(JSON.parse(JSON.stringify(UI.STAGES.map(s => s.k))), Timeline.RAIL_KEYS, 'the rail\'s keys');
  // a history per rail step: the page's step and the server's are the same
  const T0 = Date.UTC(2026, 8, 20, 12);
  const H = [['arrived'], ['arrived', 'placed'], ['arrived', 'placed', 'engraveApproved'], ['arrived', 'placed', 'laserDone'], ['arrived', 'placed', 'laserDone', 'sorted'],
    ['arrived', 'placed', 'laserDone', 'sorted', 'welded'], ['arrived', 'placed', 'laserDone', 'sorted', 'assembled'], ['arrived', 'placed', 'laserDone', 'sorted', 'assembled', 'packed', 'shipped', 'etsyCompleted'],
    ['arrived', 'placed', 'cancelled'], ['arrived', 'placed', 'cancelled', 'cancelRestored'], ['arrived', 'held', 'released', 'placed', 'removed']];
  for (const h of H) {
    const evs = h.map((type, i) => ({ id: 'o~' + type + '~' + i, type, at: T0 + i * 36e5, by: 'Paul', sheetId: type === 'placed' ? 'sh1' : '', sheet: type === 'placed' ? 'GF Sheet 1' : '' }));
    const srv = Timeline.whereOf(evs.map(e => ({ ...e })), null), cli = UI.derive(evs.map(e => ({ ...e })), null).W;
    for (const f of ['stage', 'label', 'text', 'step', 'cancelled', 'since', 'at', 'by']) assert.deepEqual(cli[f], srv[f], `${f} for ${h.join(',')}`);
  }
  console.log('  ✓ 1 · the rail (names, keys) and whereOf agree with the server for every step, a cancel, a restore, a hold and a removal');
}

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
  const shots = process.env.CN_SHOTS || null;
  const { start } = require('./bridge-server.cjs');
  const srv = await start({ receipts: [] });
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  try {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
    await ctx.route(() => true, r => { const h = new URL(r.request().url()).hostname; return h === '127.0.0.1' || h === 'localhost' ? r.continue() : r.abort(); });
    await ctx.addInitScript(() => { try { localStorage.setItem('cn.employee', 'Tester'); } catch (_) {} window.confirm = () => true; window.prompt = () => 'Tester'; window.alert = () => {}; });
    const page = await ctx.newPage(), errors = [];
    page.on('pageerror', e => errors.push(e.message));
    await page.goto(`${srv.sorterOrigin}/charm-nest-1.html`);
    await page.waitForFunction(() => window.OrderTimelineUI && window.OrderTimeline && document.readyState === 'complete', null, { timeout: 60000 });
    await page.evaluate(() => {
      window.__fx = {};
      const wait = window.setTimeout.bind(window);
      OrderTimeline.get = async id => { await new Promise(r => wait(r, 20)); return JSON.parse(JSON.stringify(window.__fx[id])); };
      window.__mount = (id, extra) => {
        if (window.__tl) { window.__tl.destroy(); document.querySelectorAll('.tlTestHost').forEach(x => x.remove()); }
        const h = document.createElement('div'); h.className = 'tlTestHost'; h.style.cssText = 'position:fixed;inset:0;z-index:2147480000;background:var(--card);display:flex;flex-direction:column';
        window.__el = document.createElement('div'); window.__el.style.cssText = 'flex:1 1 auto;min-height:0;display:flex;flex-direction:column;height:100%'; h.appendChild(window.__el); document.body.appendChild(h);
        window.__tl = OrderTimelineUI.mount(window.__el, Object.assign({ orderId: id, live: true, pollMs: 600000 }, extra || {}));
      };
    });
    // the server's answer, as get() makes it: its chronology (nothing before the arrival), time order, and where
    const setFx = (id, events, cancelled) => {
      const evs = Timeline.chronology(events.map(e => Object.assign({ orderId: id }, e))).sort(Timeline.byTime);
      return page.evaluate(a => { window.__fx[a.id] = a; }, { id, events: evs, cancelled: cancelled || null, where: Timeline.whereOf(evs, cancelled || null, { record: true }) });
    };
    const settle = () => page.waitForFunction(() => window.__el.querySelector('.tlStops[data-keys]') && !window.__el.querySelector('.tlMsg:not([hidden])') && !document.getAnimations().some(a => a.playState === 'running' && a.effect && a.effect.getTiming().iterations !== Infinity), null, { timeout: 8000 });
    const H = 36e5, D = 24 * H, T0 = Date.now() - 6 * D;
    const ev = (o, type, h, x) => Object.assign({ id: `${o}~${type}~${h}${x && x.lineKey ? '~' + x.lineKey : ''}`, type, at: T0 + h * H, by: 'Paul', source: 'sorter' }, x || {});
    const state = () => page.evaluate(() => {
      const el = window.__el, seals = [...el.querySelectorAll('.tlSt[data-key]')];
      return { rail: [...el.querySelectorAll('.tlStop')].map(b => b.dataset.stage + ':' + b.className.replace('tlStop ', '').trim()), now: el.querySelector('.tlNowT').textContent, sub: el.querySelector('.tlNowS').textContent,
        seals: seals.map(b => ({ k: b.dataset.key.split('~')[0], x: parseFloat(b.style.left) })), ghosts: [...el.querySelectorAll('.tlSt.ghost')].map(b => b.dataset.stage) };
    });

    // ── 2 · a single necklace ──
    const N = '4200000001', neck = { title: 'Initial necklace', variations: [{ name: 'Style', value: 'Necklace' }], engrave: { state: 'none' } };
    await setFx(N, [ev(N, 'interpreted', -40, { by: 'system' }), ev(N, 'arrived', 0, { source: 'etsy', by: 'Etsy' }), ev(N, 'arrived', 2, { id: `${N}~arrived~seen`, by: 'system' }),
      ev(N, 'pooled', 3), ev(N, 'placed', 5, { sheetId: 'shA', sheet: 'GF Sheet 1' }), ev(N, 'qrLabel', 6, { sheetId: 'shA' }), ev(N, 'setCommitted', 7), ev(N, 'laserDone', 30, { sheetId: 'shA', sheet: 'GF Sheet 1' }),
      ev(N, 'scan', 50, { station: 'sorting' }), ev(N, 'sorted', 51, { station: 'sorting', by: 'Ana' }), ev(N, 'sorted', 52, { station: 'sorting', by: 'Ana', id: `${N}~sorted~again` })]);
    await page.evaluate(({ id, line }) => window.__mount(id, { stages: () => OrderTimelineUI.stagesFor(line) }), { id: N, line: neck });
    await settle();
    let s = await state();
    assert.deepEqual(s.rail.map(x => x.split(':')[0]), ['arrived', 'sheet', 'laser', 'sorted', 'assembled', 'shipped'], 'a necklace with no engraving: no Engraved, no Welded: ' + s.rail);
    assert.deepEqual(s.rail.map(x => x.split(':')[1]), ['d', 'd', 'd', 'd', 'c', 'f'], 'done to Sorted, Assembled next: ' + s.rail);
    assert.deepEqual(s.seals.map(x => x.k), ['arrived', 'placed', 'laserDone', 'sorted'], 'one seal per milestone, the noise and the double arrival left out: ' + s.seals.map(x => x.k));
    assert(s.seals.every((x, i, a) => !i || x.x > a[i - 1].x), 'the seals in time order');
    assert.deepEqual(s.ghosts, ['assembled', 'shipped'], 'a dashed stamp per step to come, in process order');
    const early = await page.evaluate(() => { const x = window.__fx['4200000001'].events; const a = x.find(e => e.type === 'arrived'); return x.filter(e => e.at < a.at).length; });
    assert.equal(early, 0, 'nothing is before the arrival');
    console.log('  ✓ 2 · a necklace: 4 seals in order from the arrival (the early read and the second arrival not drawn), no Welded or Engraved, Assembled next, 2 dashed steps to come');

    // ── 3 · the explainer card on every step, a done step's seal grown in place with the card clear of it, the card kept under the pointer ──
    const cards = [];
    for (const sel of ['.tlStop[data-stage]', '.tlSt.ghost[data-stage]', '.tlSt[data-key]']) {
      const n = await page.locator(sel).count();
      for (let i = 0; i < n; i++) {
        await page.mouse.move(5, 895); await page.waitForTimeout(200);
        const b = page.locator(sel).nth(i); await b.hover(); await page.waitForTimeout(1050);   // (500 ms of rest, then the card and the grown seal)
        const r = await page.evaluate(sel => {
          const el = window.__el, exp = el.querySelector('.tlExp'), e = exp.getBoundingClientRect(), hov = [...el.querySelectorAll(sel)].find(x => x.matches(':hover'));
          const sealEl = hov && (hov.querySelector('.tlSeal') || hov), dot = sealEl && sealEl.getBoundingClientRect(), base = hov && hov.getBoundingClientRect();
          return { sel, stage: hov && (hov.dataset.stage || hov.dataset.key.split('~')[0]), card: getComputedStyle(exp).display === 'block' && +getComputedStyle(exp).opacity > .9, head: exp.querySelector('.xh') ? exp.querySelector('.xh').textContent : '',
            lines: exp.querySelectorAll('.rq').length, below: dot ? e.top >= dot.bottom - 1 || e.left >= dot.right || e.right <= dot.left : false,
            grown: !!(sealEl && sealEl.dataset.sealZoom) && dot.width >= Math.min(sealEl.offsetWidth * 1.15, 72) - .5, copy: !!document.querySelector('.tlLoupe,.tlNowZoom,.sealLens'), apart: !!dot && (dot.bottom <= e.top + 1 || dot.top >= e.bottom - 1 || dot.right <= e.left + 1 || dot.left >= e.right - 1),
            inView: !!dot && dot.left >= 0 && dot.right <= innerWidth && dot.top >= 0, done: hov && (hov.classList.contains('d') || !!hov.dataset.key) };
        }, sel);
        cards.push(r);
      }
    }
    const bad = cards.filter(r => !r.card || !r.lines || !r.head || !r.below || r.copy || (r.done && !(r.grown && r.apart && r.inView)));
    if (process.env.E2E_DEBUG) console.log(JSON.stringify(cards));
    assert.deepEqual(bad, [], 'every step shows its card under the dot, and a done one its seal grown in place, clear of the card, in the view, with no second seal');
    // the pointer moves from the dot onto its card: only the seal itself owns the hover (Paul), so the card and the grown seal go; a fresh rest brings them back
    await page.mouse.move(5, 895); await page.waitForTimeout(200);
    const stop = page.locator('.tlStop[data-stage="assembled"]'); await stop.hover(); await page.waitForTimeout(1050);
    const cb = await page.evaluate(() => { const r = window.__el.querySelector('.tlExp').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + 14, b: r.bottom }; });
    await page.mouse.move(cb.x, cb.y, { steps: 6 }); await page.waitForTimeout(400);
    const gone = await page.evaluate(() => { const x = window.__el.querySelector('.tlExp'); return getComputedStyle(x).display === 'none' && !document.querySelector('[data-seal-zoom]'); });
    assert(gone, 'the card cannot keep the hover alive: moving onto it puts the card and the grown seal away');
    await page.mouse.move(5, 895); await page.waitForTimeout(500);
    assert.equal(await page.evaluate(() => getComputedStyle(window.__el.querySelector('.tlExp')).display), 'none', 'and it stays away when the pointer leaves');
    if (shots) { await page.locator('.tlStop[data-stage="sorted"]').hover(); await page.waitForTimeout(400); await page.screenshot({ path: path.join(shots, 'e2e-necklace-hover.png') }); }
    console.log(`  ✓ 3 · ${cards.length} steps hovered (rail, dashed, seals): each shows its card under the dot, a done one its seal grown in place and clear of the card, in the view; moving onto the card puts both away`);

    // ── 4 · three pieces: a necklace, an engraved necklace, a stud earring ──
    const M = '4200000002', kA = `${M}_1`, kB = `${M}_2`, kC = `${M}_3`;
    const pieces = [
      { key: kA, tid: '1', qty: 1, pools: [kA + '_1'], sheets: ['shM'], line: { title: 'Initial necklace', variations: [], engrave: { state: 'none' } } },
      { key: kB, tid: '2', qty: 1, pools: [kB + '_1'], sheets: ['shM'], line: { title: 'Heart necklace', variations: [], engrave: { state: 'ready', needed: true } } },
      { key: kC, tid: '3', qty: 1, pools: [kC + '_1'], sheets: ['shM'], line: { title: 'Star stud earrings', variations: [{ name: 'Style', value: 'Stud earrings' }], engrave: { state: 'none' } } }];
    const all = [ev(M, 'arrived', 0, { source: 'etsy', by: 'Etsy' })];
    for (const [k, h] of [[kA, 0], [kB, 1], [kC, 2]]) all.push(ev(M, 'placed', 4 + h, { lineKey: k, sheetId: 'shM', sheet: 'GF Sheet 2' }));
    all.push(ev(M, 'engraveApproved', 9, { lineKey: kB }));
    all.push(ev(M, 'laserDone', 30, { sheetId: 'shM', sheet: 'GF Sheet 2' }));
    for (const [k, h] of [[kA, 0], [kB, 1], [kC, 2]]) all.push(ev(M, 'sorted', 50 + h, { lineKey: k, station: 'sorting' }));
    all.push(ev(M, 'welded', 60, { lineKey: kC, station: 'welding' }));
    await setFx(M, all);
    await page.evaluate(({ id, pieces }) => window.__mount(id, { pieces, stages: () => OrderTimelineUI.stagesFor(pieces.map(p => p.line)) }), { id: M, pieces });
    await settle();
    s = await state();
    assert.deepEqual(s.rail.map(x => x.split(':')[0]), ['arrived', 'sheet', 'engraved', 'laser', 'sorted', 'welded', 'assembled', 'shipped'], 'all pieces: the steps any piece takes');
    const cur = s.rail.filter(x => / c/.test(' ' + x.split(':')[1])).map(x => x.split(':')[0]);
    assert.deepEqual(cur, ['assembled'], 'all pieces sorted and the stud welded: the order waits on Assembled, not on a step already done: ' + s.rail);
    assert(!/next: Welded/.test(s.sub), 'the Now line agrees: ' + s.sub);
    const q = await page.evaluate(() => { const r = window.__el.querySelector('.tlStop[data-stage="welded"]'); return r.getAttribute('aria-label'); });
    assert.match(q, /done/i, 'Welded reads done (1 of 1 piece that takes it): ' + q);
    const per = {};
    for (const p of pieces) {
      await page.evaluate(k => window.__tl.setPieces(null, k, 1), p.key); await settle();
      const t = await state(); per[p.key] = t;
    }
    assert.deepEqual(per[kA].rail.map(x => x.split(':')[0]), ['arrived', 'sheet', 'laser', 'sorted', 'assembled', 'shipped'], 'the plain necklace: ' + per[kA].rail);
    assert.deepEqual(per[kB].rail.map(x => x.split(':')[0]), ['arrived', 'sheet', 'engraved', 'laser', 'sorted', 'assembled', 'shipped'], 'the engraved necklace: ' + per[kB].rail);
    assert.deepEqual(per[kC].rail.map(x => x.split(':')[0]), ['arrived', 'sheet', 'laser', 'sorted', 'welded', 'assembled', 'shipped'], 'the stud: ' + per[kC].rail);
    assert(!per[kA].seals.some(x => x.k === 'welded' || x.k === 'engraveApproved'), 'the plain necklace shows no other piece\'s weld or engraving');
    assert(per[kC].seals.some(x => x.k === 'welded') && !per[kC].seals.some(x => x.k === 'engraveApproved'), 'the stud shows its weld, not B\'s engraving');
    assert.equal(per[kA].seals.filter(x => x.k === 'sorted').length, 1, 'one Sorted seal: its own');
    if (shots) { await page.evaluate(() => window.__tl.setPieces(null, null, -1)); await settle(); await page.locator('.tlStop[data-stage="assembled"]').hover(); await page.waitForTimeout(400); await page.screenshot({ path: path.join(shots, 'e2e-3pieces-all.png') }); await page.mouse.move(5, 895); }
    console.log('  ✓ 4 · three pieces: all of them wait on Assembled (Welded done by the one stud); each piece shows only its own steps and seals');

    // ── 5 · cancelled then restored; removed, held, released ──
    const X = '4200000003';
    await setFx(X, [ev(X, 'arrived', 0, { source: 'etsy', by: 'Etsy' }), ev(X, 'placed', 3, { sheetId: 'shX', sheet: 'SS Sheet 1' }), ev(X, 'cancelled', 5, { text: 'Buyer asked' }), ev(X, 'cancelRestored', 8),
      ev(X, 'removed', 9, { sheetId: 'shX', data: { reason: 'hold for buyer' } }), ev(X, 'held', 9.5, { data: { reason: 'Waiting on the buyer' } }), ev(X, 'released', 20), ev(X, 'placed', 22, { id: `${X}~placed~2`, sheetId: 'shY', sheet: 'SS Sheet 2' })]);
    await page.evaluate(({ id, line }) => window.__mount(id, { stages: () => OrderTimelineUI.stagesFor(line) }), { id: X, line: neck });
    await settle();
    s = await state();
    for (const k of ['cancelled', 'cancelRestored', 'removed', 'held', 'released']) assert(s.seals.some(x => x.k === k), k + ' has its seal');
    assert(!s.rail.some(x => /\bx\b/.test(x.split(':')[1])) && !/Cancelled/.test(s.now), 'restored: not cancelled: ' + s.now);
    assert(s.seals.every((x, i, a) => !i || x.x > a[i - 1].x), 'in time order');
    console.log('  ✓ 5 · cancelled then restored, taken off, held and let go: each a seal in time order; the order is not shown cancelled; ' + s.now);

    // ── 6 · 2000+ steps; the motion ──
    const L = '4200000004', big = [ev(L, 'arrived', 0, { source: 'etsy', by: 'Etsy' })];
    for (let i = 0; i < 2100; i++) big.push(ev(L, ['scan', 'moved', 'interpreted', 'note'][i % 4], 1 + i * .05, { id: `${L}~x~${i}`, lineKey: `${L}_${i % 20}` }));
    for (let i = 0; i < 20; i++) big.push(ev(L, 'placed', 2 + i, { lineKey: `${L}_${i}`, sheetId: 'shL', sheet: 'GF Sheet 9' }));
    await setFx(L, big);
    const t0 = Date.now();
    await page.evaluate(id => window.__mount(id), L); await settle();
    const took = Date.now() - t0;
    s = await state();
    assert(s.seals.length === 21 && s.seals[0].k === 'arrived', '2121 records: 21 seals, the arrival first: ' + s.seals.length);
    assert(took < 6000, 'drawn in ' + took + ' ms');
    // hover motion: what animates, and the frames (counted in the page while the driver hovers a step and leaves it)
    const hoverFrames = async () => {
      await page.mouse.move(5, 895); await page.waitForTimeout(300);
      const box = await page.locator('.tlStop[data-stage="sheet"]').boundingBox();
      const frames = page.evaluate(async () => { const gaps = [], props = new Set(); let last = 0, on = true; const tick = t => { if (last) gaps.push(t - last); last = t; for (const a of document.getAnimations()) for (const k of (a.effect && a.effect.getKeyframes ? a.effect.getKeyframes() : [])) for (const p of Object.keys(k)) if (!['offset', 'easing', 'composite', 'computedOffset'].includes(p)) props.add(p); if (on) requestAnimationFrame(tick); }; requestAnimationFrame(tick); await new Promise(r => setTimeout(r, 900)); on = false; return { props: [...props], worst: Math.max(...gaps.slice(1)), n: gaps.length }; });
      await page.waitForTimeout(60);
      await page.mouse.move(box.x + box.width / 2, box.y + 10, { steps: 3 }); await page.waitForTimeout(350);
      await page.mouse.move(5, 895, { steps: 3 });
      return frames;
    };
    const fBig = await hoverFrames();
    await page.evaluate(id => window.__mount(id), N); await settle();
    const f = await hoverFrames();
    assert([...f.props, ...fBig.props].every(p => ['transform', 'opacity', 'clipPath'].includes(p)), 'only transform, opacity and clip-path animate: ' + f.props + ' ' + fBig.props);
    assert(f.worst < 34, `the worst frame while the seal and card come and go: ${f.worst.toFixed(1)} ms`);
    console.log(`  ✓ 6 · 2121 records draw 21 seals in ${took} ms; hovering animates ${f.props.join(', ')} only; worst frame ${f.worst.toFixed(1)} ms (necklace), ${fBig.worst.toFixed(1)} ms (2121 records)`);

    await page.evaluate(() => { window.__tl.destroy(); document.querySelectorAll('.tlTestHost').forEach(x => x.remove()); });
    assert.deepEqual(errors, [], 'no page errors');
    console.log('adv-timeline-e2e OK');
  } finally { await browser.close(); if (srv.close) await srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
