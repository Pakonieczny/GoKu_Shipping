// Adversarial checks of the order timeline component (charm-nest-timeline-ui.js) and its header rail:
//  1 · the rail logic stays in step with the server's whereOf (random histories, unknown types, the record rule);
//  2 · a live step redraws 100 events inside a frame (< 16 ms) and keeps the stamps already drawn;
//  3 · a restore whose cancelRestored event was not written: the server's where (the record) says not cancelled;
//  4 · the header rail's loupe goes when the hovered ✕ turns back into a step (a restore arrives while hovering);
//  5 · ← → from an "Around this step" row keep the keyboard there, and from a stamp the focus follows the step;
//  6 · the CANCELLED stamp follows the cancel (a person's, then Etsy's); unknown and odd types still draw.
//   node tests/charm-nest/adv-timeline-ui.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>)
const fs = require('fs'), path = require('path'), vm = require('vm'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const Timeline = require(path.join(root, 'netlify/functions/_orderTimeline.js'));

// ── 1 · client whereOf/derive against the server's whereOf ──
{
  const win = { document: { getElementById: () => null, createElement: () => ({}), head: { appendChild() {} } } };
  win.window = win; vm.createContext(win);
  vm.runInContext(fs.readFileSync(path.join(root, 'charm-nest-timeline-ui.js'), 'utf8'), win);
  const UI = win.OrderTimelineUI, types = [...Timeline.TYPES, 'bogus', 'constructor'];
  let seed = 11; const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff, pick = a => a[Math.floor(rnd() * a.length)];
  const F = ['stage', 'label', 'text', 'sheet', 'sheetId', 'setId', 'station', 'device', 'by', 'at', 'since', 'designed', 'cancelled', 'step'];
  for (let it = 0; it < 4000; it++) {
    const evs = [];
    for (let i = Math.floor(rnd() * 12); i > 0; i--) {
      const e = { type: pick(types), at: 1e12 + Math.floor(rnd() * 20) * 1000, by: pick(['', 'Paul', 'system', 'Etsy']), station: pick(['', 'sorting', 'welding']), device: pick(['', 'd1']) };
      if (rnd() < .5) { const s = pick(['A', 'B']); e.sheetId = 'sh-' + s; e.sheet = 'Sheet ' + s; }
      if (e.type === 'note' && rnd() < .5) e.data = { stamp: 'DESIGNED :)' };
      evs.push(e);
    }
    const cx = rnd() < .15 ? { at: 1e12 + 30000, by: pick(['Etsy', 'Paul']) } : null, copy = () => evs.map(e => ({ ...e }));
    const srv = Timeline.whereOf(copy(), cx), cli = UI.derive(copy(), cx).W;
    for (const f of F) assert.deepEqual(cli[f], srv[f], `whereOf.${f} differs from the server's for ${JSON.stringify(evs.map(e => e.type))}`);
    const rec = Timeline.whereOf(copy(), cx, { record: true });
    assert.equal(!!UI.derive(copy(), cx, rec).cancelled, rec.cancelled, `the rail's cancel differs from the server's record rule for ${JSON.stringify(evs.map(e => e.type))}`);
  }
  console.log('  ✓ 1 · 4000 random histories: whereOf field for field as the server\'s (unknown types too), cancel as the record says');
}

(async () => {
  const pwDir = process.env.PW_DIR || path.join(root, 'node_modules');
  let chromium; try { ({ chromium } = require(path.join(pwDir, 'playwright-core'))); } catch (_) { console.log('  – no playwright-core: the browser checks were not run'); return; }
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
      const TY = ['arrived', 'pulled', 'interpreted', 'placed', 'engraveApproved', 'moved', 'teamMessage', 'laserDone', 'scan', 'sorted', 'welded', 'assembled', 'packed', 'note'];
      window.__gen = (id, n, span) => Array.from({ length: n }, (_, i) => { const type = TY[Math.min(TY.length - 1, Math.floor(i * TY.length / n))]; return { id: `${id}~${type}~g${i}`, orderId: id, type, at: Math.round(Date.now() - span - 36e5 + i * span / Math.max(1, n - 1)), by: ['Paul', 'Ana P.', ''][i % 3], source: 'sorter', station: type === 'sorted' ? 'sorting' : '', text: `${type} ${i}` }; });
      window.__mount = (id, extra, box) => {
        if (window.__tl) { window.__tl.destroy(); document.querySelectorAll('.tlTestHost').forEach(x => x.remove()); }
        const h = document.createElement('div'); h.className = 'tlTestHost'; h.style.cssText = box || 'position:fixed;inset:0;z-index:2147480000;background:var(--card);display:flex;flex-direction:column';
        window.__el = document.createElement('div'); window.__el.style.cssText = 'flex:1 1 auto;min-height:0;display:flex;flex-direction:column;height:100%'; h.appendChild(window.__el); document.body.appendChild(h);
        window.__tl = OrderTimelineUI.mount(window.__el, Object.assign({ orderId: id, live: true, pollMs: 600000 }, extra || {}));
      };
    });
    const setFx = (id, events, cancelled) => page.evaluate(a => { window.__fx[a.id] = a; }, { id, events, cancelled: cancelled || null, where: Timeline.whereOf(events, cancelled || null, { record: true }) });
    const painted = n => page.waitForFunction(n => window.__el.querySelectorAll('.tlSt[data-key], .tlStop.d').length >= n, n, { timeout: 5000 });
    const q = (sel, fn) => page.evaluate(({ sel, fn }) => (new Function('els', 'return (' + fn + ')(els)'))([...window.__el.querySelectorAll(sel)]), { sel, fn: fn.toString() });

    // ── 2 · a live step over 100 events: under a frame, the drawn stamps kept ──
    const P = '4100000100', evs100 = await page.evaluate(id => window.__gen(id, 100, 5 * 864e5), P);
    await setFx(P, evs100); await page.evaluate(id => window.__mount(id), P); await painted(100); await page.waitForTimeout(800);
    // (three rounds of 7, the best median: a busy machine's hiccup is not the component's cost)
    const perf = await page.evaluate(async id => {
      const first = window.__el.querySelector('.tlSt[data-key]'), med = [];
      for (let round = 0, n = 0; round < 3; round++) {
        const ms = [];
        for (let i = 0; i < 7; i++, n++) { const t0 = performance.now(); OrderTimeline.record({ orderId: id, type: 'note', text: 'live ' + n, id: 'perf' + n }); ms.push(performance.now() - t0); await new Promise(r => requestAnimationFrame(() => setTimeout(r, 30))); }
        med.push(ms.sort((a, b) => a - b)[3]);
      }
      return { median: Math.min(...med), kept: first.isConnected && window.__el.querySelector('.tlSt[data-key]') === first, n: window.__el.querySelectorAll('.tlSt[data-key]').length, order: [...window.__el.querySelectorAll('.tlSt[data-key]')].map(b => +b.style.left.replace('px', '')).every((x, i, a) => !i || x >= a[i - 1]) };
    }, P);
    assert.equal(perf.n, 121); assert(perf.kept, 'the stamps already drawn are kept'); assert(perf.order, 'the stamps stay in time order in the page');
    assert(perf.median < 16, `a live redraw of 100 events takes ${perf.median.toFixed(1)} ms (budget 16)`);
    console.log(`  ✓ 2 · a live step over 100 events: ${perf.median.toFixed(1)} ms (median of 7, best of three rounds), the drawn stamps kept and in order`);

    // ── 3 · a restore whose cancelRestored event was not written ──
    const R = '4100000200', base = await page.evaluate(id => window.__gen(id, 12, 3 * 864e5), R);
    const cxEv = { id: `${R}~cancelled~c1`, orderId: R, type: 'cancelled', at: Date.now() - 6e4, by: 'Paul', text: 'Cancelled by Paul' };
    await setFx(R, base.concat([cxEv]), null);
    await page.evaluate(id => window.__mount(id), R); await painted(13); await page.waitForTimeout(300);
    let r = await page.evaluate(() => ({ now: window.__el.querySelector('.tlNowT').textContent, x: window.__el.querySelectorAll('.tlStop.x').length, stamp: window.__el.querySelectorAll('.tlCxStamp').length }));
    assert.doesNotMatch(r.now, /Cancelled/, 'the record is gone: not cancelled, as the server says'); assert.equal(r.x, 0); assert.equal(r.stamp, 0);
    // a cancel recorded on this page counts at once (before the server has it)
    await page.evaluate(id => OrderTimeline.record({ orderId: id, type: 'cancelled', by: 'Paul', text: 'Cancelled again', id: 'c2' }), R); await page.waitForTimeout(200);
    r = await page.evaluate(() => ({ now: window.__el.querySelector('.tlNowT').textContent, x: window.__el.querySelectorAll('.tlStop.x').length }));
    assert.match(r.now, /Cancelled/); assert.equal(r.x, 1);
    console.log('  ✓ 3 · a cancel with no record left (restore without its event) is not shown as cancelled; a live cancel still is');

    // ── 6 · the CANCELLED stamp follows the cancel; odd types draw ──
    const E = '4100000300', ev6 = base.map(e => Object.assign({}, e, { id: e.id.replace(R, E), orderId: E }));
    await setFx(E, ev6, { at: Date.now() - 5e5, by: 'Paul', why: 'Asked' });
    await page.evaluate(id => window.__mount(id), E); await painted(12); await page.waitForTimeout(1300);
    const t1 = await page.evaluate(() => window.__el.querySelector('.tlCxStamp').textContent);
    await setFx(E, ev6, { at: Date.now() - 2e5, by: 'Etsy', why: 'Buyer requested', source: 'etsy' });
    await page.evaluate(() => window.__tl.refresh()); await page.waitForTimeout(200);
    const t2 = await page.evaluate(() => [...window.__el.querySelectorAll('.tlCxStamp')].map(x => x.textContent));
    assert.match(t1, /BY PAUL/); assert.equal(t2.length, 1); assert.match(t2[0], /ON ETSY/, 'the stamp says who cancelled it now');
    const O = '4100000400';
    await page.evaluate(id => { window.__fx[id] = { events: ['bogus', 'arrived', 'constructor', 'toString', 'placed'].map((type, i) => ({ id: `${id}~${type}~o${i}`, type, at: Date.now() - (5 - i) * 36e5 })), cancelled: null, where: null }; window.__mount(id); }, O);
    await painted(5);
    r = await page.evaluate(() => { for (const b of window.__el.querySelectorAll('.tlSt[data-key]')) b.click(); for (const c of window.__el.querySelectorAll('.tlChip')) c.click(); return { n: window.__el.querySelectorAll('.tlSt[data-key]').length, now: window.__el.querySelector('.tlNowT').textContent }; });
    assert.equal(r.n, 5); assert.equal(r.now, 'On a sheet');
    console.log('  ✓ 6 · the CANCELLED stamp goes from BY PAUL to ON ETSY with the record; unknown and prototype-named types draw and click');

    // ── 5 · keyboard ──
    await page.evaluate(id => window.__mount(id), R); await painted(13); await page.waitForTimeout(500);
    r = await page.evaluate(() => {
      const el = window.__el, lbl = () => el.querySelector('.tlDetail .tlLbl').textContent.replace(/ ·.*/, '') + '#' + el.querySelector('.tlDetail .tlLbl').textContent.match(/step (\d+)/)[1];
      const key = k => document.activeElement.dispatchEvent(new KeyboardEvent('keydown', { key: k, bubbles: true, cancelable: true }));
      el.querySelector('.tlSt[data-key]').click(); el.querySelector('.tlArw').focus();
      const out = [lbl()]; for (let i = 0; i < 3; i++) { key('ArrowRight'); out.push(lbl() + (document.activeElement.classList.contains('tlArw') ? '' : '!lost')); }
      const st = el.querySelectorAll('.tlSt[data-key]')[6]; st.click(); st.focus(); key('ArrowLeft'); key('ArrowLeft');
      out.push(lbl() + '@' + (document.activeElement.dataset.key || document.activeElement.tagName));
      return out;
    });
    assert.deepEqual(r.slice(0, 4).map(x => x.split('#')[1]), ['1', '2', '3', '4'], 'three → from an Around row move three steps: ' + r.join(' '));
    assert(!r.slice(1, 4).some(x => x.endsWith('!lost')), 'the keyboard stays on the Around list');
    assert.match(r[4], /#5@/); assert.equal(r[4].split('@')[1], await page.evaluate(() => [...window.__el.querySelectorAll('.tlSt[data-key]')][4].dataset.key), 'the focus follows the step on the lanes');
    console.log('  ✓ 5 · ← → : from an Around row three steps in a row, from a stamp the focus follows');

    // ── 4 · header rail: hovering the ✕ while the restore arrives ──
    const C = '4100000500', ev4 = base.map(e => Object.assign({}, e, { id: e.id.replace(R, C), orderId: C })), cx4 = Object.assign({}, cxEv, { id: `${C}~cancelled~c1`, orderId: C });
    await setFx(C, ev4.concat([cx4]), { at: cx4.at, by: 'Paul', why: 'Asked' });
    await page.evaluate(id => window.__mount(id, { compact: true }, 'position:fixed;left:200px;top:100px;width:720px;height:52px;z-index:2147480000;background:var(--card);display:flex'), C);
    await page.waitForSelector('.tlUI.compact .tlStop.x'); await page.waitForTimeout(900);
    await page.hover('.tlUI.compact .tlStop.x'); await page.waitForTimeout(300);
    assert.equal(await page.evaluate(() => getComputedStyle(window.__el.querySelector('.tlLoupe')).display), 'block');
    await setFx(C, ev4.concat([cx4, { id: `${C}~cancelRestored~r`, orderId: C, type: 'cancelRestored', at: Date.now(), by: 'Paul' }]), null);
    await page.evaluate(() => window.__tl.refresh()); await page.waitForTimeout(300);
    await page.mouse.move(700, 600); await page.waitForTimeout(300);
    r = await page.evaluate(() => ({ loupe: getComputedStyle(window.__el.querySelector('.tlLoupe')).display, x: window.__el.querySelectorAll('.tlStop.x').length, lifted: window.__el.querySelectorAll('.lifted').length }));
    assert.equal(r.x, 0); assert.equal(r.loupe, 'none', 'the loupe does not stay over the page'); assert.equal(r.lifted, 0);
    console.log('  ✓ 4 · header rail: the ✕ turned back into a step under the pointer, and its loupe went with it');

    await page.evaluate(() => { window.__tl.destroy(); document.querySelectorAll('.tlTestHost').forEach(x => x.remove()); });
    assert.deepEqual(errors, []);
    console.log('adv-timeline-ui OK');
  } finally { await browser.close(); if (srv.close) await srv.close(); }
})().catch(e => { console.error(e); process.exit(1); });
