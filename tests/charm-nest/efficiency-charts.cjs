// The Employee efficiency charts library (charm-nest-efficiency-charts.js -> window.EfficiencyCharts), on the gallery fixture
// (tests/charm-nest/efficiency-charts-gallery.html: invented data, no network, no backend).
//   1 · the helpers: durations, h m, per hour, percent, clock, change, nice ticks, countUp; only transform / opacity / stroke-dashoffset
//       are animated by the style sheet
//   2 · every chart draws from sample data (line, area, bars grouped / stacked, stacked area, hour heat map, calendar, sparkline, donut)
//   3 · hover opens ONE card with the right numbers (value, previous period and change, share, state), at the real pointer
//   4 · update() morphs (mid-frame differs from both ends, the end equals a fresh chart's drawing) and keeps the card and the nodes
//   5 · gaps, closed days, empty and loading states; a day click / key press calls onDay with the day
//   6 · keyboard (arrows, Home, End, Esc, Enter) and touch (tap, scrub, tap away)
//   7 · widths 390 / 900 / 1440: no sideways scroll, fewer ticks on the phone, the card stays inside the chart
//   8 · reduced motion: the drawing is final at once and nothing animates
//   9 · only transform / opacity / stroke-dashoffset animate at run time; frame time while a range morphs; no console errors
//   node tests/charm-nest/efficiency-charts.cjs     (PW_DIR=<playwright node_modules>, CHROMIUM=<chrome>, SHOTS=<dir for screenshots>)
const fs = require('fs'), path = require('path'), assert = require('assert/strict');
const root = path.join(__dirname, '../..');
const GALLERY = 'file://' + path.join(__dirname, 'efficiency-charts-gallery.html');
const ok = m => console.log('  ✓ ' + m);

(async () => {
  let chromium;
  for (const c of [process.env.PW_DIR && path.join(process.env.PW_DIR, 'playwright-core'), process.env.PW_DIR && path.join(process.env.PW_DIR, 'playwright'), '/opt/node22/lib/node_modules/playwright', 'playwright']) {
    if (!c) continue; try { ({ chromium } = require(c)); break; } catch (_) {}
  }
  if (!chromium) { console.log('  – no playwright: the browser checks were not run'); return; }
  const browser = await chromium.launch({ executablePath: process.env.CHROMIUM || '/opt/pw-browsers/chromium-1194/chrome-linux/chrome', args: ['--no-sandbox'] });
  const errs = [];
  const open = async (opts, extra) => {
    const ctx = await browser.newContext(Object.assign({ viewport: { width: 1440, height: 900 } }, opts || {}));
    const page = await ctx.newPage();
    page.on('pageerror', e => errs.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') errs.push('console: ' + m.text()); });
    if (extra) await extra(page);
    await page.goto(GALLERY);
    await page.waitForFunction(() => window.GAL && document.querySelectorAll('.efc').length >= 15);
    await page.waitForTimeout(1300);
    return { ctx, page };
  };
  const box = async (page, sel) => { await page.locator(sel).scrollIntoViewIfNeeded(); await page.waitForTimeout(60); return page.locator(sel).boundingBox(); };
  const tipText = (page, id) => page.evaluate(i => { const t = document.querySelector('#' + i + ' .efc-tip'); return t && t.hasAttribute('data-open') ? { open: true, title: (t.querySelector('.efc-tip-t') || {}).textContent || '', big: (t.querySelector('.efc-tip-v b') || {}).textContent || '', unit: (t.querySelector('.efc-tip-v span') || {}).textContent || '', rows: [...t.querySelectorAll('.efc-tip-r')].map(r => ({ k: r.dataset.k, v: r.querySelector('b').textContent, d: (r.querySelector('.efc-delta') || {}).textContent || '' })), foot: (t.querySelector('.efc-tip-f') || {}).textContent || '', text: t.innerText } : { open: false }; }, id);
  const hoverAt = async (page, id, fx, fy) => {
    const b = await box(page, '#' + id + ' .efc-svg');
    const x = b.x + b.width * fx, y = b.y + b.height * (fy == null ? .5 : fy);
    await page.mouse.move(Math.max(1, x - 14), y); await page.mouse.move(x, y, { steps: 3 }); await page.waitForTimeout(260);
    return { x, y };
  };
  const away = async page => { await page.mouse.move(2, 2); await page.waitForTimeout(200); };
  const shots = async (page, w) => {
    if (!process.env.SHOTS) return; fs.mkdirSync(process.env.SHOTS, { recursive: true });
    for (const id of ['g-line', 'g-multi', 'g-area', 'g-hours', 'g-grouped', 'g-stackbars', 'g-stacked', 'g-heat', 'g-strip', 'g-calmonth', 'g-calyear', 'g-donut']) { const card = page.locator('#' + id).locator('xpath=ancestor::div[contains(@class,"card")][1]'); await card.scrollIntoViewIfNeeded(); await page.waitForTimeout(80); await card.screenshot({ path: path.join(process.env.SHOTS, `chart-${id.slice(2)}-${w}.png`) }); }
  };

  try {
    /* ── 1 · helpers ── */
    const { page } = await open();
    const H = await page.evaluate(() => {
      const f = EfficiencyCharts.fmt, t = EfficiencyCharts.niceTicks;
      const el = document.createElement('b'); document.body.appendChild(el); EfficiencyCharts.countUp(el, 0, 321); const v = el.dataset.v;
      return { dur: [f.duration(252000), f.duration(12000), f.duration(3900000), f.duration(90000), f.duration(172800000), f.duration(null)], hm: [f.hm(3900000), f.hm(2700000), f.hm(21600000)], ph: [f.perHour(328), f.perHour(4.25), f.perHour(null)], pct: [f.percent(72), f.percent(7.5), f.percent(null)], clock: [f.clock(485), f.clock(0), f.clock(13 * 60 + 5)], delta: [f.delta(36, 30), f.delta(5, 10), f.delta(4, 4), f.delta(4, 0), f.delta(null, 4)], unit: [f.byUnit('pieces')(1), f.byUnit('pieces')(34), f.byUnit('pieces/hour')(328), f.byUnit('seconds')(252), f.byUnit('percent')(55), f.byUnit('pieces', true)(1234)], ticks: [t(87, 4, 'pieces').ticks, t(0, 4, 'pieces').ticks, t(3, 4, 'orders').ticks, t(95, 4, 'percent').ticks, t(900, 4, 'seconds').ticks.map(x => f.duration(x * 1000))], counted: v, color: [EfficiencyCharts.color('welding'), EfficiencyCharts.color('welding') === EfficiencyCharts.color('Welding')] };
    });
    assert.deepEqual(H.dur, ['4 m 12 s', '12 s', '1 h 5 m', '1 m 30 s', '2 d', '—']);
    assert.deepEqual(H.hm, ['1 h 5 m', '45 m', '6 h']); assert.deepEqual(H.ph, ['328 per hour', '4.3 per hour', '—']); assert.deepEqual(H.pct, ['72%', '7.5%', '—']);
    assert.deepEqual(H.clock, ['8:05 AM', '12:00 AM', '1:05 PM']);
    assert.equal(H.delta[0].text, '+20%'); assert.equal(H.delta[0].dir, 'up'); assert.equal(H.delta[1].text, '−50%'); assert.equal(H.delta[1].dir, 'down'); assert.equal(H.delta[2].dir, 'flat'); assert.equal(H.delta[3].text, '+4'); assert.equal(H.delta[4].dir, null);
    assert.deepEqual(H.unit, ['1 piece', '34 pieces', '328 per hour', '4 m 12 s', '55%', '1,234']);
    assert.deepEqual(H.ticks[0], [0, 25, 50, 75, 100]); assert.equal(H.ticks[1].length > 1, true); assert.deepEqual(H.ticks[2], [0, 1, 2, 3, 4].slice(0, H.ticks[2].length), 'whole numbers for orders'); assert(H.ticks[2].every(Number.isInteger));
    assert.deepEqual(H.ticks[3], [0, 25, 50, 75, 100]); assert(H.ticks[4].includes('5 m') || H.ticks[4].includes('4 m') || H.ticks[4].includes('3 m'), 'time ticks on minutes: ' + H.ticks[4]);
    assert.equal(H.counted, '321', 'countUp carries its value at once'); assert.equal(H.color[1], true);
    ok('helpers: durations, h m, per hour, percent, clock, change, units, nice ticks, countUp, station colours');
    // the style sheet animates nothing but transform, opacity and stroke-dashoffset
    const css = await page.evaluate(() => document.getElementById('efcStyle').textContent);
    const ALLOWED = new Set(['opacity', 'transform', 'stroke-dashoffset']);
    for (const m of css.matchAll(/transition:([^;}]+)/g)) for (const part of m[1].split(/,(?![^(]*\))/)) { const prop = part.trim().split(/[\s!]+/)[0]; if (prop === 'none') continue; assert(ALLOWED.has(prop), 'transition on ' + prop + ' is not allowed'); }
    for (const m of css.matchAll(/@keyframes\s+(\w+)\s*\{([\s\S]*?\}\s*)\}/g)) for (const d of m[2].matchAll(/([a-z-]+)\s*:/g)) assert(ALLOWED.has(d[1]), 'keyframes ' + m[1] + ' animates ' + d[1]);
    assert(!/transition:[^;}]*\b(width|height|top|left|margin|padding|fill|stroke(?!-dashoffset)|d)\b/.test(css));
    ok('the style sheet animates only transform, opacity and stroke-dashoffset');

    /* ── 2 · every chart draws ── */
    const R = await page.evaluate(() => {
      const q = (id, s) => document.querySelectorAll('#' + id + ' ' + s).length;
      const nonEmpty = (id, s) => [...document.querySelectorAll('#' + id + ' ' + s)].filter(p => (p.getAttribute('d') || '').length > 10).length;
      const st = id => document.querySelector('#' + id + ' .efc').dataset.state;
      return { line: [nonEmpty('g-line', 'path.efc-line'), q('g-line', 'path.efc-prev'), q('g-line', 'path.efc-band'), st('g-line')], multi: [nonEmpty('g-multi', 'path.efc-line'), q('g-multi', 'path.efc-line')], area: [nonEmpty('g-area', 'path.efc-area'), nonEmpty('g-area', 'path.efc-line')], hours: [nonEmpty('g-hours', 'path.efc-bar'), q('g-hours', 'path.efc-bar.cur')], grouped: [nonEmpty('g-grouped', 'path.efc-bar')], stackbars: [nonEmpty('g-stackbars', 'path.efc-bar')], stacked: [nonEmpty('g-stacked', 'path.efc-fill'), q('g-stacked', 'path.efc-fill')], heat: [q('g-heat', 'rect.efc-cell'), q('g-strip', 'rect.efc-cell')], cal: [q('g-calmonth', '.efc-day'), q('g-calyear', '.efc-day'), q('g-calyear', '.efc-mt')], donut: [nonEmpty('g-donut', 'path.efc-arc'), q('g-donut', '.efc-dli')], spark: [1, 2, 3, 4].map(i => nonEmpty('g-spark' + i, 'path.efc-line')), legend: [...document.querySelectorAll('#g-multi .efc-legend .efc-lgi')].map(x => x.textContent), legend1: [...document.querySelectorAll('#g-line .efc-legend .efc-lgi')].map(x => x.textContent) };
    });
    assert(R.line[0] >= 1 && R.line[1] === 1 && R.line[2] === 1 && R.line[3] === 'ready', 'line: line, previous period dashed, band'); assert.equal(R.multi[1], 2); assert(R.multi[0] === 2);
    assert(R.area[0] >= 1 && R.area[1] >= 1); assert(R.hours[0] >= 4 && R.hours[1] === 1, 'bars with the current bar in gold'); assert(R.grouped[0] > 20); assert(R.stackbars[0] > 50); assert.equal(R.stacked[1], 5); assert(R.stacked[0] === 5);
    assert.equal(R.heat[0], 10 * 16, 'ten days of hours 5 to 20'); assert.equal(R.heat[1], 16); assert.equal(R.cal[0], 30); assert(R.cal[1] >= 300 && R.cal[2] === 12, 'a year of months: ' + R.cal);
    assert.equal(R.donut[0], 5); assert.equal(R.donut[1], 5); assert(R.spark.every(v => v >= 1), 'sparklines: ' + R.spark);
    assert.deepEqual(R.legend, ['Pieces', 'Orders'], 'two series: a legend'); assert.deepEqual(R.legend1, ['Previous period', 'Typical (median to p90)'], 'one series: the legend names the dashed line and the band');
    ok('every chart draws from the sample data (line + previous + band, area, bars + gold bar, stacked, heat map, calendars, sparklines, donut + legend)');

    /* ── 3 · hover shows ONE card with the right numbers ── */
    const D = await page.evaluate(() => { const d = GAL.data; return { x: d.x, parts: d.parts, prev: d.prevParts, orders: d.orders, stations: d.station.map(s => ({ label: s.label, values: s.values })), rate: d.rate }; });
    const dayTxt = x => { const t = new Date(Date.parse(x + 'T12:00:00Z')); return t.toLocaleDateString('en-US', { timeZone: 'UTC', weekday: 'short' }) + ', ' + t.toLocaleDateString('en-US', { timeZone: 'UTC', month: 'short', day: 'numeric' }); };
    const idxOfTitle = t => D.x.findIndex(x => dayTxt(x) === t);
    const gi = n => Math.round(n).toLocaleString('en-US');
    // line: scan until a day with a value, a previous value and a typical range is under the pointer
    let T = null, li = -1;
    for (const fx of [.62, .66, .7, .55, .5, .45]) { await hoverAt(page, 'g-line', fx); T = await tipText(page, 'g-line'); li = idxOfTitle(T.title); if (T.open && li >= 0 && D.parts[li] != null && D.prev[li] != null) break; }
    assert(T.open && li >= 0 && D.parts[li] != null, 'the card opens on a day with numbers');
    assert.equal(T.big, gi(D.parts[li])); assert.equal(T.unit, D.parts[li] === 1 ? 'piece' : 'pieces');
    const prevRow = T.rows.find(r => r.k === 'Previous period'); assert(prevRow, 'previous period row'); assert.equal(prevRow.v, gi(D.prev[li]) + (D.prev[li] === 1 ? ' piece' : ' pieces'));
    const dl = await page.evaluate(([a, b]) => EfficiencyCharts.fmt.delta(a, b).text, [D.parts[li], D.prev[li]]); assert.equal(prevRow.d, dl, 'the change against the previous period');
    assert(T.rows.some(r => r.k.startsWith('Typical')), 'the band row'); assert(/signed in/.test(T.foot), 'the definition line');
    assert.equal(await page.locator('.efc-tip[data-open]').count(), 1, 'exactly ONE card on the page');
    const guide = await page.evaluate(() => { const g = document.querySelector('#g-line .efc-guide'); return g.classList.contains('show') ? g.style.transform : ''; }); assert(/translate\(/.test(guide), 'the crosshair follows');
    ok('line: the card shows the day, the value, the previous period with its change, the band and the definition (ONE card; crosshair)');
    // grouped bars, stacked bars, stacked area, heat map, strip, calendar, donut, sparkline
    await away(page);
    let gT; for (const fx of [.6, .64, .56, .5]) { await hoverAt(page, 'g-grouped', fx); gT = await tipText(page, 'g-grouped'); const gi2 = idxOfTitle(gT.title); if (gT.open && gi2 >= 0 && D.parts[gi2] != null) { assert.equal(gT.rows.find(r => r.k === 'Pieces').v, gi(D.parts[gi2])); assert.equal(gT.rows.find(r => r.k === 'Orders').v, gi(D.orders[gi2])); break; } }
    assert(gT.open && gT.rows.length >= 2, 'grouped bars: both series in the card (a series of pieces is shown by the number)');
    await away(page);
    let sT; for (const fx of [.6, .64, .56, .5]) { await hoverAt(page, 'g-stackbars', fx); sT = await tipText(page, 'g-stackbars'); const k = idxOfTitle(sT.title); if (sT.open && k >= 0 && D.stations[0].values[k] != null) { const total = D.stations.reduce((a, s) => a + (s.values[k] || 0), 0); assert.equal(sT.rows.find(r => r.k === 'Total').v, gi(total) + ' pieces'); assert.equal(sT.rows.find(r => r.k === 'Welding').v, gi(D.stations[0].values[k]) + ' pieces'); break; } }
    assert(sT.rows.some(r => r.k === 'Total'), 'stacked bars: the total');
    await away(page);
    const stk = await hoverAt(page, 'g-stacked', .62); const stT = await tipText(page, 'g-stacked'); assert(stT.open && (stT.rows.length === 0 || stT.rows.some(r => r.k === 'Total')) && stT.title.length > 3, 'stacked area card'); void stk;
    await away(page);
    await hoverAt(page, 'g-heat', .25, .3); const hT = await tipText(page, 'g-heat'); assert(hT.open && /AM|PM/.test(hT.title) && /Share of the day/.test(hT.text), 'heat map card: the day, the hour, the share of the day');
    await away(page);
    await hoverAt(page, 'g-strip', .3, .3); const sp = await tipText(page, 'g-strip'); assert(sp.open && sp.rows.some(r => r.k === 'Share of the total'), 'strip card');
    await away(page);
    const cellCenter = async day => page.evaluate(d => { const r = document.querySelector('.efc-day[data-day="' + d + '"] .bg').getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; }, day);
    await box(page, '#g-calmonth .efc-svg'); let c = await cellCenter('2026-10-02'); await page.mouse.move(c.x - 9, c.y); await page.mouse.move(c.x, c.y, { steps: 2 }); await page.waitForTimeout(260);
    const cT = await tipText(page, 'g-calmonth'); assert.equal(cT.title, 'Fri, Oct 2'); assert.equal(cT.big, 'Worked'); assert(cT.rows.some(r => r.k === 'Signed in' && /h/.test(r.v)) && cT.rows.some(r => r.k === 'First in' && r.v === '8:05 AM'), 'the calendar card: state, signed in, first in');
    const d2 = await page.evaluate(() => GAL.model['2026-10-02']); assert.equal(cT.rows.find(r => r.k === 'Pieces').v, gi(d2.parts));
    await page.evaluate(() => document.querySelector('#g-calmonth .efc-svg').dispatchEvent(new PointerEvent('pointerleave', { pointerType: 'mouse' })));
    c = await cellCenter('2026-10-03'); await page.mouse.move(c.x, c.y); await page.waitForTimeout(260); const cl = await tipText(page, 'g-calmonth'); assert.equal(cl.big, 'Closed', 'a closed day says so');
    await away(page);
    await box(page, '#g-donut .efc-svg'); const dc = await page.evaluate(() => { const svg = document.querySelector('#g-donut svg'), ci = svg.querySelector('circle'), b = svg.getBoundingClientRect(), th = -Math.PI / 4, r = +ci.getAttribute('r'); return { x: b.left + +ci.getAttribute('cx') + r * Math.cos(th), y: b.top + +ci.getAttribute('cy') + r * Math.sin(th) }; });
    await page.mouse.move(dc.x - 4, dc.y + 4); await page.mouse.move(dc.x, dc.y, { steps: 2 }); await page.waitForTimeout(260);
    const dT = await tipText(page, 'g-donut'); const dSl = await page.evaluate(() => { const t = document.querySelectorAll('#g-donut .efc-dli'); return [...t].map(r => [r.querySelector('em').textContent, r.querySelector('b').textContent, r.querySelector('small').textContent]); });
    assert.equal(dT.title, dSl[0][0]); assert.equal(dT.big + ' ' + dT.unit, dSl[0][1] + ' pieces'); assert.equal(dT.rows.find(r => r.k === 'Share').v.replace('.0', ''), dSl[0][2].replace('.0', ''), 'donut card = the largest slice');
    assert.equal(await page.evaluate(() => document.querySelector('#g-donut .efc-ctr-l').textContent), dSl[0][0] + ' · ' + dSl[0][2].replace('.0', ''), 'the centre says it too');
    await away(page);
    await hoverAt(page, 'g-spark1', .5, .5); const spT = await tipText(page, 'g-spark1'); assert(spT.open && /\d/.test(spT.big), 'sparkline card');
    await away(page);
    assert.equal(await page.locator('.efc-tip[data-open]').count(), 0, 'every card closes when the pointer leaves');
    ok('hover cards with the right numbers: grouped and stacked bars (total), stacked area, heat map, strip, calendar (state, signed in, closed), donut (and its centre), sparkline');

    /* ── 4 · update() morphs, keeps the card and the nodes ── */
    const lineEls = () => page.evaluate(() => { const s = document.querySelector('#g-line svg'), l = s.querySelector('path.efc-line'); return { svg: !!window.__svg0 && window.__svg0 === s, line: !!window.__line0 && window.__line0 === l, d: l.getAttribute('d') }; });
    await page.evaluate(() => { window.__svg0 = document.querySelector('#g-line svg'); window.__line0 = document.querySelector('#g-line svg path.efc-line'); window.__opens = 0; const t = document.querySelector('#g-line .efc-tip'); new MutationObserver(ms => { for (const m of ms) if (m.attributeName === 'data-open' && !t.hasAttribute('data-open')) window.__opens++; }).observe(t, { attributes: true }); });
    // hover today (the right edge), tick, and the card stays with the new number
    const b = await box(page, '#g-line .efc-svg'); await page.mouse.move(b.x + b.width - 30, b.y + 100); await page.mouse.move(b.x + b.width - 8, b.y + 100, { steps: 3 }); await page.waitForTimeout(260);
    const before = await tipText(page, 'g-line'); assert(before.open && before.title === 'Mon, Oct 5' && before.big === '29', 'today: 29 pieces, was ' + JSON.stringify(before.big));
    const d0 = (await lineEls()).d;
    await page.evaluate(() => GAL.tick());     await page.waitForTimeout(110); const mid = await lineEls(); await page.waitForTimeout(900); const end = await lineEls();
    const after = await tipText(page, 'g-line');
    assert(after.open && after.title === 'Mon, Oct 5' && after.big === '32', 'the card stays on today and shows the new 32, not ' + after.big); assert.equal(await page.evaluate(() => window.__opens), 0, 'the card never closed during the update');
    assert(end.svg && end.line, 'the same svg and line nodes were morphed, not rebuilt'); assert(mid.d !== d0 && mid.d !== end.d, 'mid-frame differs from both ends');
    await page.evaluate(() => { const host = document.createElement('div'); host.id = 'scratch'; host.style.width = document.getElementById('g-line').clientWidth + 'px'; document.body.appendChild(host); const g = GAL.data; const c = EfficiencyCharts.line(host, { unit: 'pieces', height: 240, seriesLabel: 'Pieces' }); c.update({ x: g.x, series: [{ key: 'parts', label: 'Pieces', values: g.parts, prev: g.prevParts }], band: { lo: g.band.lo, hi: g.band.hi, label: 'Typical' }, closed: g.x.map(d => GAL.model[d].state === 'closed') }); window.__fresh = c; });
    await page.waitForTimeout(1200);
    const fresh = await page.evaluate(() => document.querySelector('#scratch path.efc-line').getAttribute('d')); assert.equal(end.d, fresh, 'after the morph the drawing equals a fresh chart of the same data'); await page.evaluate(() => { window.__fresh.destroy(); document.getElementById('scratch').remove(); });
    await away(page);
    ok('update(): the card stays on today with the new number and never closes; the same nodes morph; mid-frame differs from both ends; the end equals a fresh drawing');
    // a new range morphs too (and the day buckets of a year are weeks)
    await page.evaluate(() => GAL.setRange('week')); await page.waitForTimeout(1100);
    let wk = await page.evaluate(() => ({ n: document.querySelectorAll('#g-line path.efc-line').length, kind: GAL.data.kind, bars: [...document.querySelectorAll('#g-grouped path.efc-bar')].filter(p => p.getAttribute('d')).length }));
    assert.equal(wk.kind, 'day'); assert(wk.bars >= 4 && wk.bars <= 14, 'a week: about 7 days of bars: ' + wk.bars);
    await page.evaluate(() => GAL.setRange('year')); await page.waitForTimeout(1100);
    wk = await page.evaluate(() => ({ kind: GAL.data.kind, n: GAL.data.x.length, xl: document.querySelectorAll('#g-line .efc-xl:not([display])').length })); assert.equal(wk.kind, 'week'); assert(wk.n >= 52 && wk.n <= 54); assert(wk.xl >= 4 && wk.xl <= 30 && wk.xl < wk.n, 'week labels thinned: ' + wk.xl);
    await page.evaluate(() => GAL.setRange('day')); await page.waitForTimeout(1100);
    const dayR = await page.evaluate(() => ({ kind: GAL.data.kind, n: GAL.data.x.length, xl: [...document.querySelectorAll('#g-line .efc-xl:not([display])')].map(t => t.textContent) })); assert.equal(dayR.n, 24); assert(dayR.xl.includes('12a') && dayR.xl.includes('8a') && dayR.xl.length >= 4 && dayR.xl.length <= 24, 'hour labels: ' + dayR.xl);
    await page.evaluate(() => GAL.setRange('month')); await page.waitForTimeout(1100);
    ok('range switches (day, week, month, year) redraw: hour, day and week axes with thinned labels');

    /* ── 5 · gaps, closed days, empty, loading ── */
    const G = await page.evaluate(async () => {
      const host = document.createElement('div'); host.id = 'gaps'; host.style.width = '600px'; document.body.appendChild(host);
      const c = EfficiencyCharts.line(host, { unit: 'pieces', height: 200 }); const x = ['2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05'];
      c.update({ x, series: [{ key: 'p', label: 'Pieces', values: [5, null, 7, 8, 6] }] });
      await new Promise(r => setTimeout(r, 900));
      const runs = d => (d.match(/M/g) || []).length; const d1 = host.querySelector('path.efc-line').getAttribute('d');
      c.show(1); const t1 = host.querySelector('.efc-tip-v b').textContent;
      c.update({ x, series: [{ key: 'p', label: 'Pieces', values: [5, null, 7, 8, 6] }], closed: [false, true, false, false, false] }); await new Promise(r => setTimeout(r, 900));
      const d2 = host.querySelector('path.efc-line').getAttribute('d'); c.show(1); const t2 = host.querySelector('.efc-tip-v b').textContent;
      c.update({ x, series: [{ key: 'p', label: 'Pieces', values: [0, 0, 0, 0, 0] }] }); await new Promise(r => setTimeout(r, 200));
      const empty = { text: host.querySelector('.efc-empty') && host.querySelector('.efc-empty').textContent, state: host.querySelector('.efc').dataset.state, y: [...host.querySelectorAll('.efc-tick')].length };
      c.update({ x: [], series: [] }, { emptyText: 'No issues in this range' }); await new Promise(r => setTimeout(r, 100)); const custom = host.querySelector('.efc-empty').textContent;
      c.update({ x, series: [{ key: 'p', label: 'Pieces', values: [3, 4, 5, 6, 7] }] }); await new Promise(r => setTimeout(r, 900));
      c.update({ loading: true }); await new Promise(r => setTimeout(r, 450));
      const loading = { attr: host.querySelector('.efc').hasAttribute('data-loading'), label: host.querySelector('.efc-wait') && host.querySelector('.efc-wait').textContent, role: host.querySelector('.efc-wait') && host.querySelector('.efc-wait').getAttribute('role'), kept: host.querySelector('path.efc-line').getAttribute('d').length > 20, op: getComputedStyle(host.querySelector('.efc-body')).opacity };
      c.update({ x, series: [{ key: 'p', label: 'Pieces', values: [3, 4, 5, 6, 8] }] }); await new Promise(r => setTimeout(r, 500));
      const done = { attr: host.querySelector('.efc').hasAttribute('data-loading'), wait: !!host.querySelector('.efc-wait') };
      c.destroy(); const gone = !document.getElementById('gaps').children.length;
      return { r1: runs(d1), t1, r2: runs(d2), t2, empty, custom, loading, done, gone };
    });
    assert.equal(G.r1, 2, 'a day with nothing logged is a gap: two runs'); assert.equal(G.t1, 'Nothing logged'); assert.equal(G.r2, 1, 'a closed day is bridged'); assert.equal(G.t2, 'Closed');
    assert.equal(G.empty.text, 'No activity in this range'); assert.equal(G.empty.state, 'empty'); assert.equal(G.custom, 'No issues in this range');
    assert(G.loading.attr && G.loading.label === 'Loading' && G.loading.role === 'status' && G.loading.kept && +G.loading.op < .6, 'loading: a small labelled spinner, the drawing held dimmed ' + JSON.stringify(G.loading));
    assert(!G.done.attr && !G.done.wait, 'new data clears the spinner'); assert(G.gone, 'destroy() removes the chart');
    const states = await page.evaluate(() => ({ empty: document.querySelector('#g-empty .efc-empty') && document.querySelector('#g-empty .efc-empty').textContent, wait: document.querySelector('#g-loading .efc-wait') && document.querySelector('#g-loading .efc-wait').textContent, h: document.querySelector('#g-loading .efc-body').offsetHeight }));
    assert.equal(states.empty, 'No activity in this range'); assert.equal(states.wait, 'Loading pieces'); assert(states.h >= 100, 'the loading state keeps the chart\'s height (no layout jump)');
    ok('gaps for nothing logged, closed days bridged, calm empty state (custom text too), labelled spinner with the drawing held, destroy');
    // a day click calls onDay with the day; the selected day moves
    c = await cellCenter('2026-10-02'); await page.evaluate(() => { window.__picked = null; }); await box(page, '#g-calmonth .efc-svg'); c = await cellCenter('2026-10-01'); await page.mouse.click(c.x, c.y); await page.waitForTimeout(200);
    assert.equal(await page.evaluate(() => window.__picked), '2026-10-01'); assert.equal(await page.evaluate(() => document.querySelector('#g-calmonth .efc-day.sel').dataset.day), '2026-10-01', 'the selected day follows');
    c = await cellCenter('2026-10-20'); await page.evaluate(() => { window.__picked = 'x'; }); await page.mouse.click(c.x, c.y); await page.waitForTimeout(100); assert.equal(await page.evaluate(() => window.__picked), 'x', 'a day still to come is not clickable');
    await away(page);
    ok('calendar: a click calls onDay(day) and moves the selection; future days do nothing');

    /* ── 5b · the other ways to call it: shorthands, units, labels, rows, share, folding, strips, one month, a quiet sparkline ── */
    const V = await page.evaluate(async () => {
      const mk = (id, w) => { const h = document.createElement('div'); h.id = id; h.style.width = (w || 560) + 'px'; document.body.appendChild(h); return h; };
      const wait = ms => new Promise(r => setTimeout(r, ms));
      const tipOf = h => { const t = h.querySelector('.efc-tip'); return t && t.hasAttribute('data-open') ? { title: (t.querySelector('.efc-tip-t') || {}).textContent || '', big: (t.querySelector('.efc-tip-v b') || {}).textContent || '', rows: [...t.querySelectorAll('.efc-tip-r')].map(r => [r.dataset.k, (r.querySelector('b') || {}).textContent || '']), foot: (t.querySelector('.efc-tip-f') || {}).textContent || '', text: t.innerText } : null; };
      const out = {};
      // points shorthand + seconds unit
      let h = mk('v1'); let c = EfficiencyCharts.line(h, { unit: 'seconds', height: 180 });
      c.update({ points: [{ x: '2026-10-01', y: 252, prev: 200 }, { x: '2026-10-02', y: 4000, prev: 300 }, { x: '2026-10-03', y: 310 }] }); await wait(900);
      c.show(0); out.pts0 = tipOf(h); c.show(1); out.pts1 = tipOf(h); c.destroy();
      // category labels + label()/tipRows()/def/onPoint
      h = mk('v2'); let picked = null;
      c = EfficiencyCharts.bars(h, { unit: 'pieces', height: 180, label: p => 'Cell ' + p.x, tipRows: p => [['Orders', String(p.values.p * 2)]], def: 'Pieces finished.', onPoint: p => { picked = p.x; } });
      c.update({ x: ['Mon', 'Tue', 'Wed'], series: [{ key: 'p', label: 'Pieces', values: [3, 8, 5] }] }); await wait(900);
      c.show(1); out.cat = tipOf(h); out.catLabels = [...h.querySelectorAll('.efc-xl:not([display])')].map(t => t.textContent);
      h.querySelector('.efc-svg').focus(); await wait(60); h.querySelector('.efc-svg').dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); out.picked = picked; c.destroy();
      // stacked share
      h = mk('v3'); c = EfficiencyCharts.stacked(h, { height: 200, share: true });
      const x3 = ['2026-10-01', '2026-10-02', '2026-10-03']; c.update({ x: x3, series: [{ key: 'welding', label: 'Welding', values: [6, 2, 5] }, { key: 'assembly', label: 'Assembly', values: [2, 2, 5] }] }); await wait(900);
      c.show(0); out.share = tipOf(h); out.shareTicks = [...h.querySelectorAll('.efc-yaxis .efc-tick')].map(t => t.textContent); c.destroy();
      // stacked as columns
      h = mk('v4'); c = EfficiencyCharts.stacked(h, { height: 200, mode: 'bars', unit: 'pieces' });
      c.update({ x: x3, series: [{ key: 'welding', label: 'Welding', values: [6, 2, 5] }, { key: 'assembly', label: 'Assembly', values: [2, 2, 5] }] }); await wait(900);
      out.stackBars = h.querySelectorAll('path.efc-bar').length; c.show(0); out.stackBarsTip = tipOf(h); c.destroy();
      // donut folding
      h = mk('v5', 360); c = EfficiencyCharts.donut(h, { max: 4, unit: 'pieces' });
      c.update({ slices: [{ key: 'a', label: 'A', value: 9 }, { key: 'b', label: 'B', value: 6 }, { key: 'c', label: 'C', value: 3 }, { key: 'd', label: 'D', value: 2 }, { key: 'e', label: 'E', value: 1 }] }); await wait(900);
      out.donutArcs = h.querySelectorAll('path.efc-arc').length; out.donutRows = [...h.querySelectorAll('.efc-dli em')].map(e => e.textContent); out.donutCentre = (h.querySelector('.efc-ctr-v') || {}).textContent; c.destroy();
      // heat from days
      h = mk('v6', 640); c = EfficiencyCharts.hourHeatmap(h, { hourFrom: 6, hourTo: 18 });
      const hrs = d => Array.from({ length: 24 }, (_, i) => (i >= 8 && i <= 15 ? d + i : 0));
      c.update({ days: [{ day: '2026-10-01', hours: hrs(1) }, { day: '2026-10-02', hours: hrs(2) }] }); await wait(900);
      out.heatCells = h.querySelectorAll('rect.efc-cell').length; c.destroy();
      // one month as mini grids
      h = mk('v7', 560); c = EfficiencyCharts.calendarHeat(h, { layout: 'months' });
      const days = []; for (let d = 1; d <= 31; d++) days.push({ day: '2026-10-' + String(d).padStart(2, '0'), state: d <= 5 ? (d % 7 === 4 || d % 7 === 5 ? 'closed' : 'worked') : 'future' });
      c.update({ days, today: '2026-10-05' }); await wait(700);
      out.monthName = [...h.querySelectorAll('.efc-mt')].map(t => t.textContent); out.monthCells = h.querySelectorAll('.efc-day').length; c.destroy();
      // quiet sparkline
      h = mk('v8', 160); c = EfficiencyCharts.sparkline(h, { yMin: 0, hover: false });
      c.update({ values: [4, 6, 5, 9, 7] }); await wait(700);
      const svg = h.querySelector('svg'); out.spark = { h: Math.round(svg.getBoundingClientRect().height), tab: svg.getAttribute('tabindex'), dot: h.querySelectorAll('.efc-dot, circle').length, line: !!h.querySelector('path.efc-line') }; c.destroy();
      for (const id of ['v1', 'v2', 'v3', 'v4', 'v5', 'v6', 'v7', 'v8']) { const e = document.getElementById(id); if (e) e.remove(); }
      return out;
    });
    assert(V.pts0 && V.pts0.big === '4 m 12 s', 'points shorthand, seconds unit: ' + JSON.stringify(V.pts0 && V.pts0.big)); assert.equal(V.pts1.big, '1 h 7 m');
    assert(V.pts0.rows.some(r => r[0] === 'Previous period' && r[1] === '3 m 20 s'), 'previous period in time: ' + JSON.stringify(V.pts0.rows));
    assert.equal(V.cat.title, 'Cell Tue'); assert(V.cat.rows.some(r => r[0] === 'Orders' && r[1] === '16'), 'tipRows'); assert.equal(V.cat.foot, 'Pieces finished.'); assert.deepEqual(V.catLabels, ['Mon', 'Tue', 'Wed'], 'category labels as given'); assert.equal(V.picked, 'Tue', 'Enter calls onPoint with the point the card shows');
    assert(/%/.test(V.share.big) || V.share.rows.some(r => /%/.test(r[1])), 'share: percent in the card ' + JSON.stringify(V.share)); assert(V.shareTicks.some(t => /100/.test(t)), '100% tick: ' + V.shareTicks);
    assert.equal(V.stackBars, 6, 'stacked columns: 2 series x 3 days'); assert(V.stackBarsTip && V.stackBarsTip.rows.some(r => r[0] === 'Total'), 'stacked columns card has the total');
    assert.equal(V.donutArcs, 4, 'max 4: three slices and Other'); assert(V.donutRows.includes('Other'), 'Other: ' + V.donutRows); assert.equal(V.donutCentre.replace(/[^\d]/g, ''), '21', 'the centre is the whole');
    assert.equal(V.heatCells, 2 * 13, 'days shape: 2 days x hours 6..18'); assert.deepEqual(V.monthName, ['October 2026']); assert.equal(V.monthCells, 31);
    assert(V.spark.h >= 24 && V.spark.h <= 32 && V.spark.tab === '-1' && V.spark.line, 'quiet sparkline: ' + JSON.stringify(V.spark));
    ok('other ways to call it: points shorthand with time unit, category labels, label / tipRows / def / onPoint, share, stacked columns, donut folding into Other, heat from days, one month as a mini grid, quiet sparkline');

    /* ── 6 · keyboard ── */
    await page.evaluate(() => { document.querySelector('#g-line .efc-svg').focus(); }); await page.waitForTimeout(250);
    let k = await tipText(page, 'g-line'); assert(k.open && k.title === 'Mon, Oct 5', 'focus opens the card on the newest day: ' + k.title);
    await page.keyboard.press('ArrowLeft'); await page.waitForTimeout(150); k = await tipText(page, 'g-line'); assert.equal(k.title, 'Sun, Oct 4'); assert.equal(k.big, 'Closed');
    await page.keyboard.press('Home'); await page.waitForTimeout(150); k = await tipText(page, 'g-line'); assert.equal(k.title, dayTxt(await page.evaluate(() => GAL.data.x[0])));
    await page.keyboard.press('End'); await page.waitForTimeout(150); k = await tipText(page, 'g-line'); assert.equal(k.title, 'Mon, Oct 5');
    await page.keyboard.press('Escape'); await page.waitForTimeout(150); assert.equal((await tipText(page, 'g-line')).open, false, 'Esc closes the card');
    await page.evaluate(() => { document.querySelector('#g-grouped .efc-svg').focus(); }); await page.waitForTimeout(200); k = await tipText(page, 'g-grouped'); assert(k.open, 'bars: focus opens the card'); await page.keyboard.press('ArrowLeft'); await page.waitForTimeout(120); const k2 = await tipText(page, 'g-grouped'); assert.notEqual(k2.title, k.title);
    await page.evaluate(() => { document.querySelector('#g-calmonth .efc-svg').focus(); window.__picked = null; }); await page.waitForTimeout(200); k = await tipText(page, 'g-calmonth'); assert.equal(k.title, 'Mon, Oct 5', 'calendar: focus opens today');
    await page.keyboard.press('ArrowLeft'); await page.keyboard.press('ArrowLeft'); await page.keyboard.press('ArrowLeft'); await page.waitForTimeout(150); k = await tipText(page, 'g-calmonth'); assert.equal(k.title, 'Fri, Oct 2'); await page.keyboard.press('Enter'); await page.waitForTimeout(100); assert.equal(await page.evaluate(() => window.__picked), '2026-10-02', 'Enter opens the day');
    await page.keyboard.press('ArrowDown'); await page.waitForTimeout(100); assert.equal((await tipText(page, 'g-calmonth')).title, 'Fri, Oct 9', 'Down is one week');
    await page.evaluate(() => { document.querySelector('#g-donut .efc-svg').focus(); }); await page.waitForTimeout(200); k = await tipText(page, 'g-donut'); assert.equal(k.title, 'Welding'); await page.keyboard.press('ArrowRight'); await page.waitForTimeout(100); assert.equal((await tipText(page, 'g-donut')).title, 'Assembly');
    await page.evaluate(() => document.activeElement && document.activeElement.blur()); await page.waitForTimeout(150);
    ok('keyboard: focus opens the card, arrows / Home / End walk it, Esc closes, Enter opens a day (calendar), donut slices');

    /* ── 9 (part) · no layout properties, only transform / opacity / stroke-dashoffset, and frame time, while a range morphs ── */
    const SCAN = await page.evaluate(async () => {
      // which properties do running animations and transitions touch? (scanned every 40 ms while charts morph, hover, load and tick)
      const allowed = new Set(['opacity', 'transform', 'strokeDashoffset', 'stroke-dashoffset', 'offset', 'easing', 'composite', 'computedOffset']);
      const bad = new Set(), seen = new Set();
      const poll = () => { for (const a of document.getAnimations()) { const e = a.effect; if (!e || !e.getKeyframes) continue; if (a.transitionProperty) { seen.add(a.transitionProperty); if (!allowed.has(a.transitionProperty)) bad.add(a.transitionProperty); } for (const kf of e.getKeyframes()) for (const p of Object.keys(kf)) { seen.add(p); if (!allowed.has(p)) bad.add(p); } } };
      const iv = setInterval(poll, 40);
      GAL.setRange('quarter'); await new Promise(r => setTimeout(r, 300)); document.querySelector('#g-line .efc-svg').dispatchEvent(new PointerEvent('pointerleave', { pointerType: 'mouse' }));
      await new Promise(r => setTimeout(r, 700)); GAL.tick(); await new Promise(r => setTimeout(r, 700)); GAL.charts.donut.setLoading(true); await new Promise(r => setTimeout(r, 400)); GAL.apply('month'); await new Promise(r => setTimeout(r, 900));
      clearInterval(iv); return { bad: [...bad], seen: [...seen] };
    });
    assert.deepEqual(SCAN.bad, [], 'animated properties other than transform / opacity / stroke-dashoffset: ' + SCAN.bad + ' (seen: ' + SCAN.seen + ')');
    const MT = await page.evaluate(async () => {
      // frame time while the charts morph (no polling in this run; one warm-up round first: the engine compiles its code on first use).
      // The test machine is shared, so the wall-clock gap between frames is only reported (and its median checked); the library's own
      // cost is measured: the time each animation-frame callback spends in the chart code. The frame in which the new data is handed to
      // all 18 charts at once (the burst) is counted apart.
      GAL.setRange('quarter'); await new Promise(r => setTimeout(r, 900)); GAL.setRange('month'); await new Promise(r => setTimeout(r, 900));
      const per = [], js = []; for (const k of Object.keys(GAL.charts)) { const c = GAL.charts[k]; if (!c) continue; const u = c.update; c.update = function () { const t = performance.now(); const r = u.apply(this, arguments); per.push(performance.now() - t); return r; }; }
      const orig = window.requestAnimationFrame; window.requestAnimationFrame = f => orig.call(window, t => { const s = performance.now(); f(t); if (!f.__skip) js.push(performance.now() - s); });
      let frames = [], last = performance.now(), run = true, trig = 0; const bursts = [];
      const tickf = t => { if (t - trig > 120) frames.push(t - last); last = t; if (run) requestAnimationFrame(tickf); }; tickf.__skip = true;
      requestAnimationFrame(tickf);
      const go = f => { trig = performance.now() + 20; const t = performance.now(); f(); bursts.push(performance.now() - t); };
      go(() => GAL.setRange('quarter')); await new Promise(r => setTimeout(r, 900)); go(() => GAL.tick()); await new Promise(r => setTimeout(r, 700)); go(() => GAL.apply('month')); await new Promise(r => setTimeout(r, 900));
      run = false; window.requestAnimationFrame = orig; frames.sort((a, b) => a - b); js.sort((a, b) => a - b);
      const q = (a, f) => a[Math.min(a.length - 1, Math.floor(a.length * f))];
      return { n: frames.length, p50: q(frames, .5), p95: q(frames, .95), max: frames[frames.length - 1], jsN: js.length, jsP50: q(js, .5), jsP95: q(js, .95), jsMax: js[js.length - 1], burst: Math.round(Math.min(...bursts)), perMed: Math.round(q(per.slice().sort((a, b) => a - b), .5) * 10) / 10 };
    });
    assert(MT.p50 <= 34, 'median frame while charts morph: ' + MT.p50); assert(MT.jsP95 <= 12 && MT.jsMax <= 80, 'chart code per frame: p50 ' + MT.jsP50 + ' p95 ' + MT.jsP95 + ' max ' + MT.jsMax + ' ms'); assert(MT.perMed < 8 && MT.burst < 120, 'one update() of one chart (median) ' + MT.perMed + ' ms, all 18 together (best of 3) ' + MT.burst + ' ms');
    ok(`only ${SCAN.seen.filter(p => !['offset', 'easing', 'composite', 'computedOffset'].includes(p)).sort().join(' / ') || 'transform / opacity'} animate at run time; morph frames: wall gap p50 ${MT.p50.toFixed(1)} / p95 ${MT.p95.toFixed(1)} / max ${MT.max.toFixed(1)} ms on a shared machine, chart code per frame p50 ${MT.jsP50.toFixed(1)} / p95 ${MT.jsP95.toFixed(1)} / max ${MT.jsMax.toFixed(1)} ms (${MT.jsN} frames); one update() ${MT.perMed} ms median, all 18 charts at once ${MT.burst} ms (best of 3)`);

    // brush to zoom: dragging across the line chart draws a selection and calls onRange with two days
    await page.evaluate(async () => { const host = document.createElement('div'); host.id = 'brush'; host.style.width = '700px'; document.body.appendChild(host); window.__range = []; const c = EfficiencyCharts.line(host, { unit: 'pieces', height: 200, onRange: (a, b) => window.__range.push([a, b]) }); const g = GAL.data; c.update({ x: g.x, series: [{ key: 'p', label: 'Pieces', values: g.parts }] }); await new Promise(r => setTimeout(r, 900)); });
    const bb = await box(page, '#brush .efc-hit'); await page.mouse.move(bb.x + bb.width * .3, bb.y + 60); await page.mouse.down(); await page.mouse.move(bb.x + bb.width * .6, bb.y + 60, { steps: 6 });
    const brushShown = await page.evaluate(() => !!document.querySelector('#brush .efc-brush')); await page.mouse.up(); await page.waitForTimeout(100);
    const rng = await page.evaluate(() => window.__range);
    assert(brushShown, 'a selection rectangle while dragging'); assert.equal(rng.length, 1); assert(/^\d{4}-\d{2}-\d{2}$/.test(rng[0][0]) && rng[0][0] < rng[0][1], 'onRange(fromDay, toDay): ' + rng[0]);
    await page.evaluate(() => document.getElementById('brush').remove());
    ok('drag across a line chart draws a selection and ends with onRange(fromDay, toDay)');
    await page.context().close();

    /* ── 7 · widths: 900 and 390 (touch) ── */
    const widths = {};
    for (const w of [900, 390]) {
      const o = await open(w === 390 ? { viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true, deviceScaleFactor: 2 } : { viewport: { width: w, height: 900 } });
      const r = await o.page.evaluate(() => ({ sw: document.documentElement.scrollWidth, cw: document.documentElement.clientWidth, wide: [...document.querySelectorAll('.efc-svg')].filter(s => s.getBoundingClientRect().right > s.closest('.card').getBoundingClientRect().right + 1).length, yt: document.querySelectorAll('#g-line .efc-tk').length, xl: document.querySelectorAll('#g-line .efc-xl:not([display])').length, lineH: document.querySelector('#g-line svg').getBoundingClientRect().height, hitH: document.querySelector('#g-line .efc-hit').getBoundingClientRect().height, calCell: document.querySelector('#g-calmonth .efc-day .bg').getBoundingClientRect().width, yearCols: new Set([...document.querySelectorAll('#g-calyear .efc-mt')].map(t => Math.round(+t.getAttribute('x')))).size }));
      widths[w] = r; assert(r.sw <= r.cw + 1, `${w}px: no sideways scroll (${r.sw} > ${r.cw})`); assert.equal(r.wide, 0, `${w}px: every chart fits its card`);
      // the card stays inside the chart at both edges
      for (const edge of [.02, .98]) { const bx = await box(o.page, '#g-line .efc-hit'); const x = bx.x + bx.width * edge, y = bx.y + 40; if (w === 390) { await o.page.evaluate(([xx, yy]) => { const h = document.querySelector('#g-line .efc-hit'); const ev = t => new PointerEvent(t, { pointerType: 'touch', pointerId: 7, clientX: xx, clientY: yy, bubbles: true, isPrimary: true }); h.dispatchEvent(ev('pointerdown')); h.dispatchEvent(ev('pointerup')); }, [x, y]); } else { await o.page.mouse.move(x - 5, y); await o.page.mouse.move(x, y, { steps: 2 }); } await o.page.waitForTimeout(250);
        const inside = await o.page.evaluate(() => { const t = document.querySelector('#g-line .efc-tip').getBoundingClientRect(), c = document.querySelector('#g-line .efc').getBoundingClientRect(); return { open: document.querySelector('#g-line .efc-tip').hasAttribute('data-open'), l: t.left - c.left, r: c.right - t.right, tt: t.top - c.top }; });
        assert(inside.open && inside.l >= -1 && inside.r >= -1 && inside.tt >= -1, `${w}px: the card stays inside the chart at the ${edge < .5 ? 'left' : 'right'} edge ${JSON.stringify(inside)}`); await away(o.page); await o.page.evaluate(() => document.dispatchEvent(new PointerEvent('pointerdown', { pointerType: 'touch', bubbles: true }))); }
      if (w === 390) {
        // touch: a tap opens the card, a scrub moves it, tapping away closes it; the second tap on a point is a click
        const bx = await box(o.page, '#g-hours .efc-hit'); await o.page.touchscreen.tap(bx.x + bx.width * .36, bx.y + 40); await o.page.waitForTimeout(250);
        let t1 = await tipText(o.page, 'g-hours'); assert(t1.open, 'a tap opens the card'); const ttl = t1.title;
        await o.page.evaluate(([x, y, x2]) => { const h = document.querySelector('#g-line .efc-hit'); const ev = (t, xx) => new PointerEvent(t, { pointerType: 'touch', pointerId: 9, clientX: xx, clientY: y, bubbles: true, isPrimary: true }); h.dispatchEvent(ev('pointerdown', x)); window.__t1 = document.querySelector('#g-line .efc-tip-t').textContent; h.dispatchEvent(ev('pointermove', x2)); window.__t2 = document.querySelector('#g-line .efc-tip-t').textContent; h.dispatchEvent(ev('pointerup', x2)); }, await (async () => { const b = await box(o.page, '#g-line .efc-hit'); return [b.x + b.width * .3, b.y + 60, b.x + b.width * .8]; })());
        const sc = await o.page.evaluate(() => [window.__t1, window.__t2, document.querySelector('#g-line .efc-tip').hasAttribute('data-open')]); assert(sc[0] !== sc[1] && sc[2], 'scrubbing moves the card and it stays after the finger lifts: ' + sc);
        assert(r.hitH >= 150, 'the hit area is the whole chart height on the phone: ' + r.hitH);
        await o.page.evaluate(() => document.body.dispatchEvent(new PointerEvent('pointerdown', { pointerType: 'touch', bubbles: true }))); await o.page.waitForTimeout(150); assert.equal((await tipText(o.page, 'g-line')).open, false, 'tapping away closes the card'); void ttl;
      }
      if (w === 390) await shots(o.page, w);
      await o.ctx.close();
    }
    assert(widths[390].yt < widths[900].yt || widths[390].yt <= 4, `fewer y ticks on the phone: ${widths[390].yt} vs ${widths[900].yt}`); assert(widths[390].xl <= widths[900].xl, `fewer x labels on the phone: ${widths[390].xl} vs ${widths[900].xl}`);
    assert(widths[390].yearCols <= 2 && widths[900].yearCols >= 2, `the year calendar wraps to ${widths[390].yearCols} columns on the phone, ${widths[900].yearCols} at 900`);
    ok(`900 and 390 px: no sideways scroll, every chart fits its card, fewer ticks on the phone (${widths[390].yt}/${widths[390].xl} vs ${widths[900].yt}/${widths[900].xl}), the card stays inside the chart, touch taps / scrubs / taps away, the whole chart height is the hit area`);

    /* ── 8 · reduced motion ── */
    const rm = await open({ reducedMotion: 'reduce' });
    const RM = await rm.page.evaluate(async () => {
      const el = document.createElement('b'); document.body.appendChild(el); EfficiencyCharts.countUp(el, 0, 500); const text = el.textContent;
      GAL.setRange('quarter'); const d1 = document.querySelector('#g-line path.efc-line').getAttribute('d');
      const host = document.createElement('div'); host.style.width = document.getElementById('g-line').clientWidth + 'px'; document.body.appendChild(host); const g = GAL.data; const c = EfficiencyCharts.line(host, { unit: 'pieces', height: 240, seriesLabel: 'Pieces' }); c.update({ x: g.x, series: [{ key: 'parts', label: 'Pieces', values: g.parts, prev: g.prevParts }], band: { lo: g.band.lo, hi: g.band.hi, label: 'Typical' }, closed: g.x.map(d => GAL.model[d].state === 'closed') });
      const d2 = host.querySelector('path.efc-line').getAttribute('d');
      const anim = document.getAnimations().length; const dash = getComputedStyle(document.querySelector('#g-line path.efc-line')).strokeDashoffset;
      return { text, same: d1 === d2, anim, dash, tr: getComputedStyle(document.querySelector('.efc-tip')).transitionDuration };
    });
    assert.equal(RM.text, '500', 'countUp is instant'); assert(RM.same, 'with reduced motion the drawing is final at once (no tween)'); assert.equal(RM.anim, 0, 'no animation running'); assert(/^0s(, 0s)*$/.test(RM.tr), 'no transitions: ' + RM.tr); assert(/^0(px)?$/.test(RM.dash), 'the line is not left half-drawn: ' + RM.dash);
    await rm.ctx.close();
    ok('reduced motion: counts, drawings and the card are final at once; no animation, no transition');

    /* ── words: pieces never lines; no console errors ── */
    const o3 = await open(); const vis = await o3.page.evaluate(() => { const out = []; document.querySelectorAll('.efc').forEach(e => out.push(e.innerText)); return out.join(' | '); });
    for (const id of ['g-line', 'g-multi', 'g-grouped', 'g-stackbars', 'g-heat']) { const f = id === 'g-line' ? .6 : .55; await hoverAt(o3.page, id, f); const t = await tipText(o3.page, id); assert(!/\blines?\b/i.test(t.text || ''), 'a card says lines: ' + t.text); await away(o3.page); }
    assert(!/\blines?\b/i.test(vis), 'visible chart text says "lines": ' + (vis.match(/.{20}\blines?\b.{20}/i) || [])[0]);
    await shots(o3.page, 1440);
    await o3.ctx.close();
    assert.deepEqual(errs, [], 'console or page errors: ' + errs.join(' // '));
    ok('"pieces", never "lines", in every visible word; no console or page errors in the whole run');
  } catch (e) {
    console.error('  ✗ ' + (e && e.message || e)); if (errs.length) console.error('  errors seen: ' + errs.join(' // '));
    process.exitCode = 1;
  } finally { await browser.close(); }
})();
